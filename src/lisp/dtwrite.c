/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/dtypeio.h"

#include <zlib.h>
#include <errno.h>

#ifndef FD_DEBUG_DTYPEIO
#define FD_DEBUG_DTYPEIO 0
#endif

int fd_use_dtblock = FD_USE_DTBLOCK;

unsigned int fd_check_dtsize = 1;

int (*fd_dtype_error)
     (struct FD_OUTBUF *,lispval x,u8_string details) = NULL;

u8_condition fd_InconsistentDTypeSize=_("Inconsistent DTYPE size");

static lispval error_symbol;

struct  FD_DTYPE_PACKAGE {
  fd_packet_unpacker packetfns[64];
  fd_vector_unpacker vectorfns[64];};
extern struct FD_DTYPE_PACKAGE *fd_dtype_packages[64];

static ssize_t write_hashtable(struct FD_OUTBUF *out,struct FD_HASHTABLE *v);
static ssize_t write_hashset(struct FD_OUTBUF *out,struct FD_HASHSET *v);
static ssize_t write_slotmap(struct FD_OUTBUF *out,struct FD_SLOTMAP *v);
static ssize_t write_schemap(struct FD_OUTBUF *out,struct FD_SCHEMAP *v);
static ssize_t write_mystery(struct FD_OUTBUF *out,struct FD_MYSTERY_DTYPE *v);

static u8_byte _dbg_outbuf[FD_DEBUG_OUTBUF_SIZE];

static ssize_t try_dtype_output(ssize_t *len,struct FD_OUTBUF *out,lispval x)
{
  ssize_t olen = out->bufwrite-out->buffer;
  ssize_t dlen = fd_write_dtype(out,x);
  if (dlen<0)
    return dlen;
  else if ((fd_check_dtsize) && (out->buf_flushfn == NULL) &&
           ((olen+dlen) != (out->bufwrite-out->buffer)))
    /* If you're writing straight to memory, check dtype size argument */
    u8_log(LOG_WARN,fd_InconsistentDTypeSize,
           "Expecting %lld off=%lld, buffer is %lld != %lld=%lld+%lld for %s",
           dlen,olen,(out->bufwrite-out->buffer),
           (olen+dlen),olen,dlen,
           fd_lisp2buf(x,FD_DEBUG_OUTBUF_SIZE,_dbg_outbuf));
  *len = *len+dlen;
  return dlen;
}
#define fd_output_dtype(len,out,x) \
  if (PRED_FALSE(FD_ISREADING(out))) return fd_isreadbuf(out); \
  else if (try_dtype_output(&len,out,x)<0) return -1; else {}

static ssize_t write_opaque(struct FD_OUTBUF *out,lispval x)
{
  u8_string srep = fd_lisp2string(x);
  int slen = strlen(srep);
  fd_output_byte(out,dt_compound);
  fd_output_byte(out,dt_symbol);
  fd_output_4bytes(out,7);
  fd_output_bytes(out,"%OPAQUE",7);
  fd_output_byte(out,dt_string);
  fd_output_4bytes(out,slen);  /* 18 bytes up to here */
  fd_output_bytes(out,srep,slen);
  u8_free(srep);
  return 18+slen;
}

static int opaque_unparser(u8_output out,lispval val,
                               fd_compound_typeinfo info)
{
  struct FD_COMPOUND *compound = (fd_compound) val;
  if ( (compound->compound_length > 0) &&
       (FD_STRINGP(compound->compound_0)) ) {
    lispval srep = compound->compound_0;
    u8_string str = FD_CSTRING(srep);
    u8_puts(out,"#<<");
    if (str[0] != '#')
      u8_putn(out,str,FD_STRLEN(srep));
    else if (strncmp(str,"#<<",3)==0)
      u8_putn(out,str,(FD_STRLEN(srep)-2));
    else if (strncmp(str,"#<",2)==0)
      u8_putn(out,str+1,FD_STRLEN(srep)-1);
    else u8_putn(out,str,FD_STRING_LENGTH(srep));
    u8_puts(out,">>");
    return 1;}
  else return 0;
}

static ssize_t write_choice_dtype(fd_outbuf out,fd_choice ch)
{
  lispval _natsorted[17], *natsorted=_natsorted;
  int n_choices = FD_XCHOICE_SIZE(ch);
  ssize_t dtype_len =0;
  const lispval *data; int i = 0;
  if  ((out->buf_flags)&(FD_NATSORT_VALUES)) {
    natsorted = fd_natsort_choice(ch,_natsorted,17);
    data = (const lispval *)natsorted;}
  else data = FD_XCHOICE_DATA(ch);
  if (n_choices < 256)
    if ((out->buf_flags)&(FD_USE_DTYPEV2)) {
      dtype_len = 2;
      fd_output_byte(out,dt_tiny_choice);
      fd_output_byte(out,n_choices);}
    else {
      dtype_len = 3;
      fd_output_byte(out,dt_framerd_package);
      fd_output_byte(out,dt_small_choice);
      fd_output_byte(out,n_choices);}
  else {
    dtype_len = 6;
    fd_output_byte(out,dt_framerd_package);
    fd_output_byte(out,dt_choice);
    fd_output_4bytes(out,n_choices);}
  while (i < n_choices) {
    fd_output_dtype(dtype_len,out,data[i]); i++;}
  if (natsorted!=_natsorted) u8_free(natsorted);
  return dtype_len;
}

FD_EXPORT ssize_t fd_write_dtype(struct FD_OUTBUF *out,lispval x)
{
  if (PRED_FALSE(FD_ISREADING(out)))
    return fd_isreadbuf(out);
  else switch (FD_PTR_MANIFEST_TYPE(x)) {
    case fd_oid_ptr_type: { /* output OID */
      FD_OID addr = FD_OID_ADDR(x);
      if ((FD_OID_HI(addr))==0) {
        lispval val = fd_zero_pool_value(x);
        if (!(VOIDP(val))) {
          int rv = fd_write_dtype(out,val);
          fd_decref(val);
          return rv;}}
      fd_output_byte(out,dt_oid);
      fd_output_4bytes(out,FD_OID_HI(addr));
      fd_output_4bytes(out,FD_OID_LO(addr));
      return 9;}
    case fd_fixnum_ptr_type: { /* output fixnum */
      long long val = FIX2INT(x);
      if ((val<=INT_MAX) && (val>=INT_MIN)) {
        fd_output_byte(out,dt_fixnum);
        fd_output_4bytes(out,val);
        return 5;}
      else {
        fd_write_byte(out,dt_numeric_package);
        fd_write_byte(out,dt_small_bigint);
        fd_write_byte(out,9);
        if (val<0) {
          fd_write_byte(out,1);
          val = -val;}
        else fd_write_byte(out,0);
        fd_write_8bytes(out,val);
        return 3+1+8;}}
    case fd_immediate_ptr_type: { /* output constant */
      fd_ptr_type itype = FD_IMMEDIATE_TYPE(x);
      int data = FD_GET_IMMEDIATE(x,itype), retval = 0;
      if (itype == fd_symbol_type) { /* output symbol */
        lispval name = fd_symbol_names[data];
        struct FD_STRING *s = fd_consptr(struct FD_STRING *,name,fd_string_type);
        int len = s->str_bytelen;
        if (((out->buf_flags)&(FD_USE_DTYPEV2)) && (len<256)) {
          {fd_output_byte(out,dt_tiny_symbol);}
          {fd_output_byte(out,len);}
          {fd_output_bytes(out,s->str_bytes,len);}
          return len+2;}
        else {
          {fd_output_byte(out,dt_symbol);}
          {fd_output_4bytes(out,len);}
          {fd_output_bytes(out,s->str_bytes,len);}
          return len+5;}}
      else if (itype == fd_character_type) { /* Output unicode character */
        fd_output_byte(out,dt_character_package);
        if (data<128) {
          fd_output_byte(out,dt_ascii_char);
          fd_output_byte(out,1); fd_output_byte(out,data);
          return 4;}
        else {
          fd_output_byte(out,dt_unicode_char);
          if (data<0x100) {
            fd_output_byte(out,1);
            fd_output_byte(out,data);
            return 4;}
          else if (data<0x10000) {
            fd_output_byte(out,2);
            fd_output_byte(out,((data>>8)&0xFF));
            fd_output_byte(out,(data&0xFF));
            return 5;}
          else if (data<0x1000000) {
            fd_output_byte(out,3);
            fd_output_byte(out,((data>>16)&0xFF));
            fd_output_byte(out,((data>>8)&0xFF));
            fd_output_byte(out,(data&0xFF));
            return 6;}
          else {
            fd_output_byte(out,3); fd_output_4bytes(out,data);
            return 7;}}
        return 5;}
      else if (itype == fd_constant_type)
        switch (data) {
        case 0: fd_output_byte(out,dt_void); return 1;
        case 1:
          fd_output_byte(out,dt_boolean);
          fd_output_byte(out,0);
          return 2;
        case 2:
          fd_output_byte(out,dt_boolean);
          fd_output_byte(out,1);
          return 2;
        case 3:
          if ((out->buf_flags)&(FD_USE_DTYPEV2)) {
            fd_output_byte(out,dt_empty_choice);
            return 1;}
          else {
            fd_output_byte(out,dt_framerd_package);
            fd_output_byte(out,dt_small_choice);
            fd_output_byte(out,0);
            return 3;}
        case 4: fd_output_byte(out,dt_empty_list); return 1;
        default:
          if ((out->buf_flags)&(FD_WRITE_OPAQUE))
            return write_opaque(out,x);
          else {
            u8_log(LOG_CRIT,"InvalidConstantDType",
                   "The constant '%q' doesn't have a DType representation",
                   x);
            return -1;}}
      else if ((FD_VALID_TYPECODEP(itype)) && (fd_dtype_writers[itype]))
        return fd_dtype_writers[itype](out,x);
      else if ((out->buf_flags)&(FD_WRITE_OPAQUE))
        return write_opaque(out,x);
      else if ((fd_dtype_error) &&
               (retval = fd_dtype_error(out,x,"no handler")))
        return retval;
      else {
        fd_seterr(fd_NoMethod,_("Can't write DTYPE"),NULL,x);
        return -1;}
      break;}
    case fd_cons_ptr_type: {/* output cons */
      struct FD_CONS *cons = FD_CONS_DATA(x);
      int ctype = FD_CONS_TYPE(cons);
      switch (ctype) {
      case fd_string_type: {
        struct FD_STRING *s = (struct FD_STRING *) cons;
        int len = s->str_bytelen;
        if (((out->buf_flags)&(FD_USE_DTYPEV2)) && (len<256)) {
          fd_output_byte(out,dt_tiny_string);
          fd_output_byte(out,len);
          fd_output_bytes(out,s->str_bytes,len);
          return 2+len;}
        else {
          fd_output_byte(out,dt_string);
          fd_output_4bytes(out,len);
          fd_output_bytes(out,s->str_bytes,len);
          return 5+len;}}
      case fd_packet_type: {
        struct FD_STRING *s = (struct FD_STRING *) cons;
        fd_output_byte(out,dt_packet);
        fd_output_4bytes(out,s->str_bytelen);
        fd_output_bytes(out,s->str_bytes,s->str_bytelen);
        return 5+s->str_bytelen;}
      case fd_secret_type: {
        struct FD_STRING *s = (struct FD_STRING *) cons;
        const unsigned char *data = s->str_bytes;
        unsigned int len = s->str_bytelen, sz = 0;
        fd_output_byte(out,dt_character_package);
        if (len<256) {
          fd_output_byte(out,dt_short_secret_packet);
          fd_output_byte(out,len);
          sz = 3;}
        else {
          fd_output_byte(out,dt_secret_packet);
          fd_output_4bytes(out,len);
          sz = 6;}
        fd_output_bytes(out,data,len);
        return sz+len;}
      case fd_pair_type: {
        lispval scan = x;
        ssize_t len = 0;
        while (1) {
          struct FD_PAIR *p = (struct FD_PAIR *) scan;
          lispval cdr = p->cdr;
          {fd_output_byte(out,dt_pair); len++;}
          {fd_output_dtype(len,out,p->car);}
          if (PAIRP(cdr)) scan = cdr;
          else {
            fd_output_dtype(len,out,cdr);
            return len;}}
        return len;}
      case fd_rational_type:  case fd_complex_type: {
        lispval car, cdr;
        ssize_t len = 1;
        if (ctype == fd_rational_type) {
          _fd_unpack_rational((lispval)cons,&car,&cdr);
          fd_output_byte(out,dt_rational);}
        else {
          _fd_unpack_complex((lispval)cons,&car,&cdr);
          fd_output_byte(out,dt_complex);}
        fd_output_dtype(len,out,car);
        fd_output_dtype(len,out,cdr);
        return len;}
      case fd_vector_type: {
        struct FD_VECTOR *v = (struct FD_VECTOR *) cons;
        int i = 0, length = v->vec_length;
        ssize_t dtype_len = 5;
        fd_output_byte(out,dt_vector);
        fd_output_4bytes(out,length);
        while (i < length) {
          fd_output_dtype(dtype_len,out,v->vec_elts[i]); i++;}
        return dtype_len;}
      case fd_choice_type:
        return write_choice_dtype(out,(fd_choice)cons);
      case fd_qchoice_type: {
        struct FD_QCHOICE *qv = (struct FD_QCHOICE *) cons;
        fd_output_byte(out,dt_framerd_package);
        if (EMPTYP(qv->qchoiceval)) {
          fd_output_byte(out,dt_small_qchoice);
          fd_output_byte(out,0);
          return 3;}
        else {
          struct FD_CHOICE *v = (struct FD_CHOICE *) (qv->qchoiceval);
          const lispval *data = FD_XCHOICE_DATA(v);
          int i = 0, len = FD_XCHOICE_SIZE(v);
          ssize_t dtype_len;
          if (len < 256) {
            dtype_len = 3;
            fd_output_byte(out,dt_small_qchoice);
            fd_output_byte(out,len);}
          else {
            dtype_len = 6;
            fd_output_byte(out,dt_qchoice);
            fd_output_4bytes(out,len);}
          while (i < len) {
            fd_output_dtype(dtype_len,out,data[i]); i++;}
          return dtype_len;}}
      case fd_hashset_type:
        return write_hashset(out,(struct FD_HASHSET *) cons);
      case fd_slotmap_type:
        return write_slotmap(out,(struct FD_SLOTMAP *) cons);
      case fd_schemap_type:
        return write_schemap(out,(struct FD_SCHEMAP *) cons);
      case fd_hashtable_type:
        return write_hashtable(out,(struct FD_HASHTABLE *) cons);
      case fd_mystery_type:
        return write_mystery(out,(struct FD_MYSTERY_DTYPE *) cons);
      default: {
        fd_ptr_type ctype = FD_CONS_TYPE(cons);
        if ((FD_VALID_TYPECODEP(ctype)) && (fd_dtype_writers[ctype]))
          return fd_dtype_writers[ctype](out,x);
        else if ((out->buf_flags)&(FD_WRITE_OPAQUE))
          return write_opaque(out,x);
        else if (fd_dtype_error)
          return fd_dtype_error(out,x,"no handler");
        else {
          fd_seterr(fd_NoMethod,_("Can't write DTYPE"),NULL,x);
          return -1;}}
      }}
    default:
      return -1;
    }
}

static ssize_t write_mystery(struct FD_OUTBUF *out,struct FD_MYSTERY_DTYPE *v)
{
  ssize_t dtype_size = 2;
  int size = v->myst_dtsize;
  int vectorp = (v->myst_dtcode)&0x80;
  fd_output_byte(out,v->myst_dtpackage);
  if (size>256) {
    fd_output_byte(out,v->myst_dtcode|0x40);
    fd_output_4bytes(out,size);
    dtype_size = dtype_size+4;}
  else {
    fd_output_byte(out,v->myst_dtcode&(~0x40));
    fd_output_byte(out,size);
    dtype_size = dtype_size+1;}
  if (vectorp) {
    lispval *elts = v->mystery_payload.elts, *limit = elts+size;
    while (elts<limit) {
      fd_output_dtype(dtype_size,out,*elts); elts++;}
    return dtype_size;}
  else {
    fd_output_bytes(out,v->mystery_payload.bytes,size);
    return dtype_size+size;}
}

static ssize_t write_slotmap(struct FD_OUTBUF *out,struct FD_SLOTMAP *v)
{
  ssize_t dtype_len;
  fd_read_lock_table(v);
  {
    struct FD_KEYVAL *keyvals = v->sm_keyvals;
    int i = 0, kvsize = FD_XSLOTMAP_NUSED(v), len = kvsize*2;
    fd_output_byte(out,dt_framerd_package);
    if (len < 256) {
      dtype_len = 3;
      fd_output_byte(out,dt_small_slotmap);
      fd_output_byte(out,len);}
    else {
      dtype_len = 6;
      fd_output_byte(out,dt_slotmap);
      fd_output_4bytes(out,len);}
    while (i < kvsize) {
      if ((try_dtype_output(&dtype_len,out,keyvals[i].kv_key)>=0) &&
          (try_dtype_output(&dtype_len,out,keyvals[i].kv_val)>=0)) {
        i++;}
      else {
        fd_unlock_table(v);
        return -1;}}}
  fd_unlock_table(v);
  return dtype_len;
}

static ssize_t write_schemap(struct FD_OUTBUF *out,struct FD_SCHEMAP *v)
{
  ssize_t dtype_len;
  fd_read_lock_table(v);
  {
    lispval *schema = v->table_schema, *values = v->schema_values;
    int i = 0, schemasize = FD_XSCHEMAP_SIZE(v), len = schemasize*2;
    fd_output_byte(out,dt_framerd_package);
    if (len < 256) {
      dtype_len = 3;
      fd_output_byte(out,dt_small_slotmap);
      fd_output_byte(out,len);}
    else {
      dtype_len = 6;
      fd_output_byte(out,dt_slotmap);
      fd_output_4bytes(out,len);}
    while (i < schemasize) {
      if ((try_dtype_output(&dtype_len,out,schema[i])>=0) &&
          (try_dtype_output(&dtype_len,out,values[i])>=0))
        i++;
      else {
        fd_unlock_table(v);
        return -1;}}}
  fd_unlock_table(v);
  return dtype_len;
}

static ssize_t write_hashtable(struct FD_OUTBUF *out,struct FD_HASHTABLE *v)
{
  ssize_t dtype_len;
  fd_read_lock_table(v);
  {
    int size = v->table_n_keys;
    struct FD_HASH_BUCKET **scan = v->ht_buckets, **limit = scan+v->ht_n_buckets;
    fd_output_byte(out,dt_framerd_package);
    if (size < 128) {
      dtype_len = 3;
      fd_output_byte(out,dt_small_hashtable);
      fd_output_byte(out,size*2);}
    else {
      dtype_len = 6;
      fd_output_byte(out,dt_hashtable);
      fd_output_4bytes(out,size*2);}
    scan = v->ht_buckets; limit = scan+v->ht_n_buckets;
    while (scan < limit)
      if (*scan) {
        struct FD_HASH_BUCKET *he = *scan++;
        struct FD_KEYVAL *kscan = &(he->kv_val0);
        struct FD_KEYVAL *klimit = kscan+he->bucket_len;
        while (kscan < klimit) {
          if (try_dtype_output(&dtype_len,out,kscan->kv_key)<0) {
            fd_unlock_table(v);
            return -1;}
          if (try_dtype_output(&dtype_len,out,kscan->kv_val)<0) {
            fd_unlock_table(v);
            return -1;}
          kscan++;}}
      else scan++;}
  fd_unlock_table(v);
  return dtype_len;
}

static ssize_t write_hashset(struct FD_OUTBUF *out,struct FD_HASHSET *v)
{
  ssize_t dtype_len;
  fd_read_lock_table(v); {
    int size = v->hs_n_elts;
    lispval *scan = v->hs_buckets, *limit = scan+v->hs_n_buckets;
    fd_output_byte(out,dt_framerd_package);
    if (size < 128) {
      dtype_len = 3;
      fd_output_byte(out,dt_small_hashset);
      fd_output_byte(out,size);}
    else {
      dtype_len = 6;
      fd_output_byte(out,dt_hashset);
      fd_output_4bytes(out,size);}
    scan = v->hs_buckets; limit = scan+v->hs_n_buckets;
    while (scan < limit) {
      lispval contents = *scan++;
      if (FD_EMPTYP(contents)) continue;
      else if (FD_AMBIGP(contents)) {
        FD_DO_CHOICES(elt,contents) {
          if (try_dtype_output(&dtype_len,out,elt)<0) {
            FD_STOP_DO_CHOICES;
            fd_unlock_table(v);
            return -1;}}}
      else if (try_dtype_output(&dtype_len,out,contents)<0) {
        fd_unlock_table(v);
        return -1;}
      else continue;}}
  fd_unlock_table(v);
  return dtype_len;
}

#define newpos(pos,ptr,lim) ((((ptr)+pos) <= lim) ? (pos) : (-1))

/* Reading and writing compressed dtypes */

/* This writes a non frame value with compression. */
FD_EXPORT int fd_zwrite_dtype(struct FD_OUTBUF *s,lispval x)
{
  FD_DECL_OUTBUF(out,2000);
  unsigned char *zbytes, zbuf[2000];
  ssize_t dt_len, zlen = 2000, size=0;
  int rv=-1;
  if (fd_write_dtype(&out,x)<0) {
    fd_close_outbuf(&out);
    return FD_ERROR;}
  else dt_len=out.bufwrite-out.buffer;
  zbytes = fd_zlib_compress(out.buffer,dt_len,zbuf,&zlen,-1);
  if (zbytes) {
    rv=fd_write_byte(s,dt_ztype);
    if (rv>0) {size += rv; rv=fd_write_zint(s,zlen);}
    if (rv>0) {size += rv; rv=fd_write_bytes(s,zbytes,zlen);}}
  if (zbytes != zbuf) u8_free(zbytes);
  fd_close_outbuf(&out);
  if (rv<0)
    return rv;
  else return size;
}

FD_EXPORT int fd_zwrite_dtypes(struct FD_OUTBUF *s,lispval x)
{
  FD_DECL_OUTBUF(out,2000);
  int retval = 0;
  size_t size=0;
  if (CHOICEP(x)) {
    DO_CHOICES(v,x) {
      retval = fd_write_dtype(&out,v);
      if (retval<0) {FD_STOP_DO_CHOICES; break;}}}
  else if (VECTORP(x)) {
    int i = 0, len = VEC_LEN(x);
    lispval *data = VEC_DATA(x);
    while (i<len) {
      retval = fd_write_dtype(&out,data[i]); i++;
      if (retval<0) break;}}
  else retval = fd_write_dtype(&out,x);
  if (retval>=0) {
    ssize_t outlen = out.bufwrite - out.buffer, zlen = outlen;
    unsigned char zbuf[outlen+100];
    unsigned char *zbytes =
      fd_zlib_compress(out.buffer,outlen,zbuf,&zlen,-1);
    if (zbytes) {
      retval = fd_write_byte(s,dt_ztype);
      if (retval>=0) {
        size += retval;
        retval=fd_write_zint(s,zlen);}
      if (retval>=0) {
        size += retval;
        retval=fd_write_bytes(s,zbytes,zlen);}}
    if ( (zbytes) && (zbytes != zbuf) ) u8_free(zbytes);}
  fd_close_outbuf(&out);
  if ( retval < 0 )
    return retval;
  else return size;
}

/* Custom compounds */

static ssize_t dtype_compound(struct FD_OUTBUF *out,lispval x)
{
  struct FD_COMPOUND *xc = fd_consptr(struct FD_COMPOUND *,x,fd_compound_type);
  int n_bytes = 1;
  fd_write_byte(out,dt_compound);
  n_bytes = n_bytes+fd_write_dtype(out,xc->compound_typetag);
  if ( (xc->compound_length) == 1 )
    n_bytes = n_bytes+fd_write_dtype(out,xc->compound_0);
  else {
    int i = 0, n = xc->compound_length; lispval *data = &(xc->compound_0);
    fd_write_byte(out,dt_vector);
    fd_write_4bytes(out,xc->compound_length);
    n_bytes = n_bytes+5;
    while (i<n) {
      int written = fd_write_dtype(out,data[i]);
      if (written<0) return written;
      else n_bytes = n_bytes+written;
      i++;}}
  return n_bytes;
}

/* File initialization */

FD_EXPORT void fd_init_dtwrite_c()
{
  u8_register_source_file(_FILEINFO);

  fd_dtype_writers[fd_compound_type]=dtype_compound;

  error_symbol = fd_intern("%ERROR");

  fd_compound_unparser("%OPAQUE",opaque_unparser);

  fd_register_config
    ("USEDTBLOCK",_("Use the DTBLOCK dtype code when appropriate"),
     fd_boolconfig_get,fd_boolconfig_set,&fd_use_dtblock);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
