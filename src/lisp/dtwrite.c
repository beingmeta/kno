/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/dtypeio.h"

#include <zlib.h>
#include <errno.h>

#ifndef KNO_DEBUG_DTYPEIO
#define KNO_DEBUG_DTYPEIO 0
#endif

int kno_use_dtblock = KNO_USE_DTBLOCK;

unsigned int kno_check_dtsize = 1;

int (*kno_dtype_error)
(struct KNO_OUTBUF *,lispval x,u8_string details) = NULL;

u8_condition kno_InconsistentDTypeSize=_("Inconsistent DTYPE size");

static lispval error_symbol;

struct  KNO_DTYPE_PACKAGE {
  kno_packet_unpacker packetfns[64];
  kno_vector_unpacker vectorfns[64];};
extern struct KNO_DTYPE_PACKAGE *kno_dtype_packages[64];

static ssize_t write_hashtable(struct KNO_OUTBUF *out,struct KNO_HASHTABLE *v);
static ssize_t write_hashset(struct KNO_OUTBUF *out,struct KNO_HASHSET *v);
static ssize_t write_slotmap(struct KNO_OUTBUF *out,struct KNO_SLOTMAP *v);
static ssize_t write_schemap(struct KNO_OUTBUF *out,struct KNO_SCHEMAP *v);
static ssize_t write_mystery(struct KNO_OUTBUF *out,struct KNO_MYSTERY_DTYPE *v);

static u8_byte _dbg_outbuf[KNO_DEBUG_OUTBUF_SIZE];

static ssize_t try_dtype_output(ssize_t *len,struct KNO_OUTBUF *out,lispval x)
{
  ssize_t olen = out->bufwrite-out->buffer;
  ssize_t dlen = kno_write_dtype(out,x);
  if (dlen<0)
    return dlen;
  else if ((kno_check_dtsize) && (out->buf_flushfn == NULL) &&
           ((olen+dlen) != (out->bufwrite-out->buffer)))
    /* If you're writing straight to memory, check dtype size argument */
    u8_log(LOG_WARN,kno_InconsistentDTypeSize,
           "Expecting %lld off=%lld, buffer is %lld != %lld=%lld+%lld for %s",
           dlen,olen,(out->bufwrite-out->buffer),
           (olen+dlen),olen,dlen,
           kno_lisp2buf(x,KNO_DEBUG_OUTBUF_SIZE,_dbg_outbuf));
  *len = *len+dlen;
  return dlen;
}
#define kno_output_dtype(len,out,x)                              \
  if (RARELY(KNO_ISREADING(out))) return kno_isreadbuf(out); \
  else if (try_dtype_output(&len,out,x)<0) return -1; else {}

static ssize_t write_opaque(struct KNO_OUTBUF *out,lispval x)
{
  u8_string srep = kno_lisp2string(x);
  int slen = strlen(srep);
  kno_output_byte(out,dt_compound);
  kno_output_byte(out,dt_symbol);
  kno_output_4bytes(out,7);
  kno_output_bytes(out,"%opaque",7);
  kno_output_byte(out,dt_string);
  kno_output_4bytes(out,slen);  /* 18 bytes up to here */
  kno_output_bytes(out,srep,slen);
  u8_free(srep);
  return 18+slen;
}

static int opaque_unparser(u8_output out,lispval val,kno_typeinfo info)
{
  struct KNO_COMPOUND *compound = (kno_compound) val;
  if ( (compound->compound_length > 0) &&
       (KNO_STRINGP(compound->compound_0)) ) {
    lispval srep = compound->compound_0;
    u8_string str = KNO_CSTRING(srep);
    u8_puts(out,"#<<");
    if (str[0] != '#')
      u8_putn(out,str,KNO_STRLEN(srep));
    else if (strncmp(str,"#<<",3)==0)
      u8_putn(out,str,(KNO_STRLEN(srep)-2));
    else if (strncmp(str,"#<",2)==0)
      u8_putn(out,str+1,KNO_STRLEN(srep)-1);
    else u8_putn(out,str,KNO_STRING_LENGTH(srep));
    u8_puts(out,">>");
    return 1;}
  else return 0;
}

static ssize_t write_choice_dtype(kno_outbuf out,kno_choice ch)
{
  lispval _natsorted[17], *natsorted=_natsorted;
  int n_choices = KNO_XCHOICE_SIZE(ch);
  ssize_t dtype_len =0;
  const lispval *data; int i = 0;
  if  ((out->buf_flags)&(KNO_NATSORT_VALUES)) {
    natsorted = kno_natsort_choice(ch,_natsorted,17);
    data = (const lispval *)natsorted;}
  else data = KNO_XCHOICE_DATA(ch);
  if (n_choices < 256)
    if ((out->buf_flags)&(KNO_USE_DTYPEV2)) {
      dtype_len = 2;
      kno_output_byte(out,dt_tiny_choice);
      kno_output_byte(out,n_choices);}
    else {
      dtype_len = 3;
      kno_output_byte(out,dt_kno_package);
      kno_output_byte(out,dt_small_choice);
      kno_output_byte(out,n_choices);}
  else {
    dtype_len = 6;
    kno_output_byte(out,dt_kno_package);
    kno_output_byte(out,dt_choice);
    kno_output_4bytes(out,n_choices);}
  while (i < n_choices) {
    kno_output_dtype(dtype_len,out,data[i]); i++;}
  if (natsorted!=_natsorted) u8_free(natsorted);
  return dtype_len;
}

static ssize_t output_symbol_bytes(struct KNO_OUTBUF *out,
                                   const unsigned char *bytes,
                                   size_t len)
{
  int dtflags = out->buf_flags;
  if ( ( (dtflags) & KNO_FIX_DTSYMS) || ( kno_dtype_fixcase ) ) {
    u8_string scan = bytes;
    int c = u8_sgetc(&scan), hascase = 0, fix = ! (u8_isupper(c));
    U8_STATIC_OUTPUT(upper,len*2);
    while (c >= 0) {
      if (fix)
        u8_putc(&upper,u8_toupper(c));
      else if (u8_isupper(c)) {
        if (hascase < 0) {
          /* Mixed case, leave it */
          fix=0; break;}
        else {
          u8_putc(&upper,c);
          hascase=1;}}
      else if (u8_islower(c)) {
        if (hascase > 0) {
          /* Mixed case, leave it */
          fix=0; break;}
        else {
          u8_putc(&upper,u8_toupper(c));
          hascase=-11;}}
      else u8_putc(&upper,c);
      c = u8_sgetc(&scan);}
    if (fix) {
      kno_output_bytes(out,u8_outstring(&upper),u8_outlen(&upper));}
    else {kno_output_bytes(out,bytes,len);}}
  else {kno_output_bytes(out,bytes,len);}
  return len;
}

KNO_EXPORT ssize_t kno_write_dtype(struct KNO_OUTBUF *out,lispval x)
{
  if (RARELY(KNO_ISREADING(out)))
    return kno_isreadbuf(out);
  else switch (KNO_PTR_MANIFEST_TYPE(x)) {
    case kno_oid_ptr_type: { /* output OID */
      KNO_OID addr = KNO_OID_ADDR(x);
      if ((KNO_OID_HI(addr))==0) {
        lispval val = kno_zero_pool_value(x);
        if (!(VOIDP(val))) {
          int rv = kno_write_dtype(out,val);
          kno_decref(val);
          return rv;}}
      kno_output_byte(out,dt_oid);
      kno_output_4bytes(out,KNO_OID_HI(addr));
      kno_output_4bytes(out,KNO_OID_LO(addr));
      return 9;}
    case kno_fixnum_ptr_type: { /* output fixnum */
      long long val = FIX2INT(x);
      if ((val<=INT_MAX) && (val>=INT_MIN)) {
        kno_output_byte(out,dt_fixnum);
        kno_output_4bytes(out,val);
        return 5;}
      else {
        kno_write_byte(out,dt_numeric_package);
        kno_write_byte(out,dt_small_bigint);
        kno_write_byte(out,9);
        if (val<0) {
          kno_write_byte(out,1);
          val = -val;}
        else kno_write_byte(out,0);
        kno_write_8bytes(out,val);
        return 3+1+8;}}
    case kno_immediate_ptr_type: { /* output constant */
      kno_lisp_type itype = KNO_IMMEDIATE_TYPE(x);
      int data = KNO_GET_IMMEDIATE(x,itype), retval = 0;
      if (itype == kno_symbol_type) { /* output symbol */
        lispval name = kno_symbol_names[data];
        struct KNO_STRING *s = kno_consptr(struct KNO_STRING *,name,kno_string_type);
        const unsigned char *bytes = s->str_bytes;
        int len = s->str_bytelen, dtflags = out->buf_flags;
        if (((dtflags)&(KNO_USE_DTYPEV2)) && (len<256)) {
          {kno_output_byte(out,dt_tiny_symbol);}
          {kno_output_byte(out,len);}
          {output_symbol_bytes(out,bytes,len);}
          return len+2;}
        else {
          {kno_output_byte(out,dt_symbol);}
          {kno_output_4bytes(out,len);}
          {output_symbol_bytes(out,bytes,len);}
          return len+5;}}
      else if (itype == kno_character_type) { /* Output unicode character */
        kno_output_byte(out,dt_character_package);
        if (data<128) {
          kno_output_byte(out,dt_ascii_char);
          kno_output_byte(out,1); kno_output_byte(out,data);
          return 4;}
        else {
          kno_output_byte(out,dt_unicode_char);
          if (data<0x100) {
            kno_output_byte(out,1);
            kno_output_byte(out,data);
            return 4;}
          else if (data<0x10000) {
            kno_output_byte(out,2);
            kno_output_byte(out,((data>>8)&0xFF));
            kno_output_byte(out,(data&0xFF));
            return 5;}
          else if (data<0x1000000) {
            kno_output_byte(out,3);
            kno_output_byte(out,((data>>16)&0xFF));
            kno_output_byte(out,((data>>8)&0xFF));
            kno_output_byte(out,(data&0xFF));
            return 6;}
          else {
            kno_output_byte(out,3); kno_output_4bytes(out,data);
            return 7;}}
        return 5;}
      else if (itype == kno_constant_type)
        switch (data) {
        case 0: kno_output_byte(out,dt_void); return 1;
        case 1:
          kno_output_byte(out,dt_boolean);
          kno_output_byte(out,0);
          return 2;
        case 2:
          kno_output_byte(out,dt_boolean);
          kno_output_byte(out,1);
          return 2;
        case 3:
          if ((out->buf_flags)&(KNO_USE_DTYPEV2)) {
            kno_output_byte(out,dt_empty_choice);
            return 1;}
          else {
            kno_output_byte(out,dt_kno_package);
            kno_output_byte(out,dt_small_choice);
            kno_output_byte(out,0);
            return 3;}
        case 4: kno_output_byte(out,dt_empty_list); return 1;
        case 20: {
          kno_output_byte(out,dt_default_value);
          return 1;}
        default:
          if ((out->buf_flags)&(KNO_WRITE_OPAQUE))
            return write_opaque(out,x);
          else {
            u8_log(LOG_CRIT,"InvalidConstantDType",
                   "The constant '%q' doesn't have a DType representation",
                   x);
            return -1;}}
      else if ((KNO_VALID_TYPECODEP(itype)) && (kno_dtype_writers[itype]))
        return kno_dtype_writers[itype](out,x);
      else if ((out->buf_flags)&(KNO_WRITE_OPAQUE))
        return write_opaque(out,x);
      else if ((kno_dtype_error) &&
               (retval = kno_dtype_error(out,x,"no handler")))
        return retval;
      else return KNO_ERR(-1,kno_NoMethod,_("Can't write DTYPE"),NULL,x);
      break;}
    case kno_cons_ptr_type: {/* output cons */
      struct KNO_CONS *cons = KNO_CONS_DATA(x);
      int ctype = KNO_CONS_TYPE(cons);
      switch (ctype) {
      case kno_string_type: {
        struct KNO_STRING *s = (struct KNO_STRING *) cons;
        int len = s->str_bytelen;
        if (((out->buf_flags)&(KNO_USE_DTYPEV2)) && (len<256)) {
          kno_output_byte(out,dt_tiny_string);
          kno_output_byte(out,len);
          kno_output_bytes(out,s->str_bytes,len);
          return 2+len;}
        else {
          kno_output_byte(out,dt_string);
          kno_output_4bytes(out,len);
          kno_output_bytes(out,s->str_bytes,len);
          return 5+len;}}
      case kno_packet_type: {
        struct KNO_STRING *s = (struct KNO_STRING *) cons;
        kno_output_byte(out,dt_packet);
        kno_output_4bytes(out,s->str_bytelen);
        kno_output_bytes(out,s->str_bytes,s->str_bytelen);
        return 5+s->str_bytelen;}
      case kno_secret_type: {
        struct KNO_STRING *s = (struct KNO_STRING *) cons;
        const unsigned char *data = s->str_bytes;
        unsigned int len = s->str_bytelen, sz = 0;
        kno_output_byte(out,dt_character_package);
        if (len<256) {
          kno_output_byte(out,dt_short_secret_packet);
          kno_output_byte(out,len);
          sz = 3;}
        else {
          kno_output_byte(out,dt_secret_packet);
          kno_output_4bytes(out,len);
          sz = 6;}
        kno_output_bytes(out,data,len);
        return sz+len;}
      case kno_pair_type: {
        lispval scan = x;
        ssize_t len = 0;
        while (1) {
          struct KNO_PAIR *p = (struct KNO_PAIR *) scan;
          lispval cdr = p->cdr;
          {kno_output_byte(out,dt_pair); len++;}
          {kno_output_dtype(len,out,p->car);}
          if (PAIRP(cdr)) scan = cdr;
          else {
            kno_output_dtype(len,out,cdr);
            return len;}}
        return len;}
      case kno_rational_type:  case kno_complex_type: {
        lispval car, cdr;
        ssize_t len = 1;
        if (ctype == kno_rational_type) {
          _kno_unpack_rational((lispval)cons,&car,&cdr);
          kno_output_byte(out,dt_rational);}
        else {
          _kno_unpack_complex((lispval)cons,&car,&cdr);
          kno_output_byte(out,dt_complex);}
        kno_output_dtype(len,out,car);
        kno_output_dtype(len,out,cdr);
        return len;}
      case kno_vector_type: {
        struct KNO_VECTOR *v = (struct KNO_VECTOR *) cons;
        int i = 0, length = v->vec_length;
        ssize_t dtype_len = 5;
        kno_output_byte(out,dt_vector);
        kno_output_4bytes(out,length);
        while (i < length) {
          kno_output_dtype(dtype_len,out,v->vec_elts[i]); i++;}
        return dtype_len;}
      case kno_choice_type:
        return write_choice_dtype(out,(kno_choice)cons);
      case kno_qchoice_type: {
        struct KNO_QCHOICE *qv = (struct KNO_QCHOICE *) cons;
        kno_output_byte(out,dt_kno_package);
        if (EMPTYP(qv->qchoiceval)) {
          kno_output_byte(out,dt_small_qchoice);
          kno_output_byte(out,0);
          return 3;}
        else {
          struct KNO_CHOICE *v = (struct KNO_CHOICE *) (qv->qchoiceval);
          const lispval *data = KNO_XCHOICE_DATA(v);
          int i = 0, len = KNO_XCHOICE_SIZE(v);
          ssize_t dtype_len;
          if (len < 256) {
            dtype_len = 3;
            kno_output_byte(out,dt_small_qchoice);
            kno_output_byte(out,len);}
          else {
            dtype_len = 6;
            kno_output_byte(out,dt_qchoice);
            kno_output_4bytes(out,len);}
          while (i < len) {
            kno_output_dtype(dtype_len,out,data[i]); i++;}
          return dtype_len;}}
      case kno_hashset_type:
        return write_hashset(out,(struct KNO_HASHSET *) cons);
      case kno_slotmap_type:
        return write_slotmap(out,(struct KNO_SLOTMAP *) cons);
      case kno_schemap_type:
        return write_schemap(out,(struct KNO_SCHEMAP *) cons);
      case kno_hashtable_type:
        return write_hashtable(out,(struct KNO_HASHTABLE *) cons);
      case kno_mystery_type:
        return write_mystery(out,(struct KNO_MYSTERY_DTYPE *) cons);
      default: {
        kno_lisp_type ctype = KNO_CONS_TYPE(cons);
        if ((KNO_VALID_TYPECODEP(ctype)) && (kno_dtype_writers[ctype]))
          return kno_dtype_writers[ctype](out,x);
        else if ((out->buf_flags)&(KNO_WRITE_OPAQUE))
          return write_opaque(out,x);
        else if (kno_dtype_error)
          return kno_dtype_error(out,x,"no handler");
        else return KNO_ERR(-1,kno_NoMethod,_("Can't write DTYPE"),NULL,x);}
      }}
    default:
      return -1;
    }
}

static ssize_t write_mystery(struct KNO_OUTBUF *out,struct KNO_MYSTERY_DTYPE *v)
{
  ssize_t dtype_size = 2;
  int size = v->myst_dtsize;
  int vectorp = (v->myst_dtcode)&0x80;
  kno_output_byte(out,v->myst_dtpackage);
  if (size>256) {
    kno_output_byte(out,v->myst_dtcode|0x40);
    kno_output_4bytes(out,size);
    dtype_size = dtype_size+4;}
  else {
    kno_output_byte(out,v->myst_dtcode&(~0x40));
    kno_output_byte(out,size);
    dtype_size = dtype_size+1;}
  if (vectorp) {
    lispval *elts = v->mystery_payload.elts, *limit = elts+size;
    while (elts<limit) {
      kno_output_dtype(dtype_size,out,*elts); elts++;}
    return dtype_size;}
  else {
    kno_output_bytes(out,v->mystery_payload.bytes,size);
    return dtype_size+size;}
}

static ssize_t write_slotmap(struct KNO_OUTBUF *out,struct KNO_SLOTMAP *v)
{
  ssize_t dtype_len;
  kno_read_lock_table(v);
  {
    struct KNO_KEYVAL *keyvals = v->sm_keyvals;
    int i = 0, kvsize = KNO_XSLOTMAP_NUSED(v), len = kvsize*2;
    kno_output_byte(out,dt_kno_package);
    if (len < 256) {
      dtype_len = 3;
      kno_output_byte(out,dt_small_slotmap);
      kno_output_byte(out,len);}
    else {
      dtype_len = 6;
      kno_output_byte(out,dt_slotmap);
      kno_output_4bytes(out,len);}
    while (i < kvsize) {
      if ((try_dtype_output(&dtype_len,out,keyvals[i].kv_key)>=0) &&
          (try_dtype_output(&dtype_len,out,keyvals[i].kv_val)>=0)) {
        i++;}
      else {
        kno_unlock_table(v);
        return -1;}}}
  kno_unlock_table(v);
  return dtype_len;
}

static ssize_t write_schemap(struct KNO_OUTBUF *out,struct KNO_SCHEMAP *v)
{
  ssize_t dtype_len;
  kno_read_lock_table(v);
  {
    lispval *schema = v->table_schema, *values = v->table_values;
    int i = 0, schemasize = KNO_XSCHEMAP_SIZE(v), len = schemasize*2;
    kno_output_byte(out,dt_kno_package);
    if (len < 256) {
      dtype_len = 3;
      kno_output_byte(out,dt_small_slotmap);
      kno_output_byte(out,len);}
    else {
      dtype_len = 6;
      kno_output_byte(out,dt_slotmap);
      kno_output_4bytes(out,len);}
    while (i < schemasize) {
      if ((try_dtype_output(&dtype_len,out,schema[i])>=0) &&
          (try_dtype_output(&dtype_len,out,values[i])>=0))
        i++;
      else {
        kno_unlock_table(v);
        return -1;}}}
  kno_unlock_table(v);
  return dtype_len;
}

static ssize_t write_hashtable(struct KNO_OUTBUF *out,struct KNO_HASHTABLE *v)
{
  ssize_t dtype_len;
  kno_read_lock_table(v);
  {
    int size = v->table_n_keys;
    struct KNO_HASH_BUCKET **scan = v->ht_buckets, **limit = scan+v->ht_n_buckets;
    kno_output_byte(out,dt_kno_package);
    if (size < 128) {
      dtype_len = 3;
      kno_output_byte(out,dt_small_hashtable);
      kno_output_byte(out,size*2);}
    else {
      dtype_len = 6;
      kno_output_byte(out,dt_hashtable);
      kno_output_4bytes(out,size*2);}
    scan = v->ht_buckets; limit = scan+v->ht_n_buckets;
    while (scan < limit)
      if (*scan) {
        struct KNO_HASH_BUCKET *he = *scan++;
        struct KNO_KEYVAL *kscan = &(he->kv_val0);
        struct KNO_KEYVAL *klimit = kscan+he->bucket_len;
        while (kscan < klimit) {
          if (try_dtype_output(&dtype_len,out,kscan->kv_key)<0) {
            kno_unlock_table(v);
            return -1;}
          if (try_dtype_output(&dtype_len,out,kscan->kv_val)<0) {
            kno_unlock_table(v);
            return -1;}
          kscan++;}}
      else scan++;}
  kno_unlock_table(v);
  return dtype_len;
}

static ssize_t write_hashset(struct KNO_OUTBUF *out,struct KNO_HASHSET *v)
{
  ssize_t dtype_len;
  kno_read_lock_table(v); {
    int size = v->hs_n_elts;
    lispval *scan = v->hs_buckets, *limit = scan+v->hs_n_buckets;
    kno_output_byte(out,dt_kno_package);
    if (size < 128) {
      dtype_len = 3;
      kno_output_byte(out,dt_small_hashset);
      kno_output_byte(out,size);}
    else {
      dtype_len = 6;
      kno_output_byte(out,dt_hashset);
      kno_output_4bytes(out,size);}
    scan = v->hs_buckets; limit = scan+v->hs_n_buckets;
    while (scan < limit) {
      lispval contents = *scan++;
      if (KNO_EMPTYP(contents)) continue;
      else if (KNO_AMBIGP(contents)) {
        KNO_DO_CHOICES(elt,contents) {
          if (try_dtype_output(&dtype_len,out,elt)<0) {
            KNO_STOP_DO_CHOICES;
            kno_unlock_table(v);
            return -1;}}}
      else if (try_dtype_output(&dtype_len,out,contents)<0) {
        kno_unlock_table(v);
        return -1;}
      else continue;}}
  kno_unlock_table(v);
  return dtype_len;
}

#define newpos(pos,ptr,lim) ((((ptr)+pos) <= lim) ? (pos) : (-1))

/* Reading and writing compressed dtypes */

/* This writes a non frame value with compression. */
KNO_EXPORT int kno_zwrite_dtype(struct KNO_OUTBUF *s,lispval x)
{
  KNO_DECL_OUTBUF(out,2000);
  unsigned char *zbytes, zbuf[2000];
  ssize_t dt_len, zlen = 2000, size=0;
  int rv=-1;
  if (kno_write_dtype(&out,x)<0) {
    kno_close_outbuf(&out);
    return KNO_ERROR;}
  else dt_len=out.bufwrite-out.buffer;
  zbytes = kno_zlib_compress(out.buffer,dt_len,zbuf,&zlen,-1);
  if (zbytes) {
    rv=kno_write_byte(s,dt_ztype);
    if (rv>0) {size += rv; rv=kno_write_varint(s,zlen);}
    if (rv>0) {size += rv; rv=kno_write_bytes(s,zbytes,zlen);}}
  if (zbytes != zbuf) u8_free(zbytes);
  kno_close_outbuf(&out);
  if (rv<0)
    return rv;
  else return size;
}

KNO_EXPORT int kno_zwrite_dtypes(struct KNO_OUTBUF *s,lispval x)
{
  KNO_DECL_OUTBUF(out,2000);
  int retval = 0;
  size_t size=0;
  if (CHOICEP(x)) {
    DO_CHOICES(v,x) {
      retval = kno_write_dtype(&out,v);
      if (retval<0) {KNO_STOP_DO_CHOICES; break;}}}
  else if (VECTORP(x)) {
    int i = 0, len = VEC_LEN(x);
    lispval *data = VEC_DATA(x);
    while (i<len) {
      retval = kno_write_dtype(&out,data[i]); i++;
      if (retval<0) break;}}
  else retval = kno_write_dtype(&out,x);
  if (retval>=0) {
    ssize_t outlen = out.bufwrite - out.buffer, zlen = outlen;
    unsigned char zbuf[outlen+100];
    unsigned char *zbytes =
      kno_zlib_compress(out.buffer,outlen,zbuf,&zlen,-1);
    if (zbytes) {
      retval = kno_write_byte(s,dt_ztype);
      if (retval>=0) {
        size += retval;
        retval=kno_write_varint(s,zlen);}
      if (retval>=0) {
        size += retval;
        retval=kno_write_bytes(s,zbytes,zlen);}}
    if ( (zbytes) && (zbytes != zbuf) ) u8_free(zbytes);}
  kno_close_outbuf(&out);
  if ( retval < 0 )
    return retval;
  else return size;
}

/* Custom compounds */

static ssize_t dtype_compound(struct KNO_OUTBUF *out,lispval x)
{
  struct KNO_COMPOUND *xc = kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type);
  int n_bytes = 1;
  kno_write_byte(out,dt_compound);
  n_bytes = n_bytes+kno_write_dtype(out,xc->typetag);
  if ( (xc->compound_length) == 1 )
    n_bytes = n_bytes+kno_write_dtype(out,xc->compound_0);
  else {
    int i = 0, n = xc->compound_length; lispval *data = &(xc->compound_0);
    kno_write_byte(out,dt_vector);
    kno_write_4bytes(out,xc->compound_length);
    n_bytes = n_bytes+5;
    while (i<n) {
      int written = kno_write_dtype(out,data[i]);
      if (written<0) return written;
      else n_bytes = n_bytes+written;
      i++;}}
  return n_bytes;
}

/* File initialization */

KNO_EXPORT void kno_init_dtwrite_c()
{
  u8_register_source_file(_FILEINFO);

  kno_dtype_writers[kno_compound_type]=dtype_compound;

  error_symbol = kno_intern("%error");

  kno_set_unparsefn(kno_intern("%opaque"),opaque_unparser);

  kno_register_config
    ("USEDTBLOCK",_("Use the DTBLOCK dtype code when appropriate"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_use_dtblock);

}

