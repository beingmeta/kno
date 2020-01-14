/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1
#define KNO_INLINE_XTYPE_REFS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/xtypes.h"

#include <libu8/u8elapsed.h>

#include <zlib.h>
#include <errno.h>

#ifndef KNO_DEBUG_XTYPEIO
#define KNO_DEBUG_XTYPEIO 0
#endif

static unsigned char *do_zuncompress
(const unsigned char *bytes,size_t n_bytes,
 ssize_t *dbytes,unsigned char *init_dbuf);


static lispval objid_symbol, mime_symbol, encrypted_symbol,
  compressed_symbol, error_symbol, mime_symbol, knopaque_symbol;

static lispval rational_xtype_tag, complex_xtype_tag,
  timestamp_xtype_tag, zcompress_xtype_tag;

kno_xtype_fn kno_xtype_writers[KNO_TYPE_MAX];

lispval restore_bigint(ssize_t n_digits,unsigned char *bytes,int negp);

/* Object restoration */

static lispval restore_tagged(lispval tag,lispval data,xtype_refs refs);
static lispval restore_compressed(lispval tag,lispval data,xtype_refs refs);
static ssize_t write_opaque
(kno_outbuf out, lispval x,xtype_refs refs);
static ssize_t write_slotmap
(kno_outbuf out,struct KNO_SLOTMAP *map,xtype_refs refs);
static ssize_t write_schemap
(kno_outbuf out,struct KNO_SCHEMAP *map,xtype_refs refs);



/* Utility functions */

KNO_FASTOP ssize_t xt_read_varint(kno_inbuf in)
{
  kno_8bytes result = 0; int probe;
  while ((probe = (kno_read_byte(in))))
    if (probe<0) return -1;
    else if (probe&0x80) result = result<<7|(probe&0x7F);
    else break;
  return result<<7|probe;
}

static lispval unexpected_eod()
{
  return kno_err1(kno_UnexpectedEOD);
}

/* XREF objects */

KNO_EXPORT int kno_init_xrefs(xtype_refs refs,
			      int n_refs,int refs_len,int flags,
			      lispval *elts,
			      kno_hashtable lookup)
{
  refs->xt_n_refs = n_refs;
  refs->xt_refs_len = refs_len;
  refs->xt_refs_flags = flags;
  refs->xt_refs = elts;
  if (lookup)
    refs->xt_lookup = lookup;
  else {
    struct KNO_HASHTABLE *table =
      (kno_hashtable) kno_make_hashtable(NULL,n_refs);
    if (table == NULL) return -1;
    lispval *vals = u8_malloc(n_refs*sizeof(lispval));
    if (vals == NULL) {
      kno_recycle_hashtable(table);
      return -1;}
    int i = 0, n = n_refs; while (i < n) {
      vals[i] = KNO_INT(i);
      i++;}
    int rv = kno_hashtable_iter(table,kno_table_store,n_refs,elts,vals);
    u8_free(vals);
    if (rv<0) {
      kno_recycle_hashtable(table);
      return -1;}
    refs->xt_lookup = table;}
  return n_refs;
}

KNO_EXPORT ssize_t kno_add_xtype_ref(lispval x,xtype_refs refs)
{
  if ( (KNO_OIDP(x)) || (KNO_SYMBOLP(x)) ) {
    lispval v = kno_hashtable_get(refs->xt_lookup,x,KNO_VOID);
    if (KNO_FIXNUMP(v))
      return KNO_FIX2INT(v);
    if ( (refs->xt_refs_flags) & (XTYPE_REFS_READ_ONLY) )
      return -1;
    size_t ref = refs->xt_n_refs++;
    if (ref >= refs->xt_refs_len) {
      int delta = refs->xt_refs_len;
      if (delta > XTYPE_REFS_DELTA_MAX)
	delta=XTYPE_REFS_DELTA_MAX;
      ssize_t new_size = refs->xt_refs_len+delta;
      lispval *refvals = refs->xt_refs, *new_refs =
	(new_size>XTYPE_REFS_MAX) ? (NULL) :
	(u8_realloc(refvals,sizeof(lispval)*new_size));
      if (refvals == NULL) {
	refs->xt_refs_flags |= XTYPE_REFS_READ_ONLY;
	return -1;}
      else {
	refs->xt_refs = new_refs;
	refs->xt_refs_len = new_size;}}
    refs->xt_refs[ref]=x;
    return ref;}
  else return -1;
}

KNO_EXPORT ssize_t _kno_xtype_ref(lispval x,xtype_refs refs,int add)
{
  return kno_xtype_ref(x,refs,add);
}

/* Writing XTYPEs */

static ssize_t write_xtype(kno_outbuf out,lispval x,xtype_refs refs)
{
  unsigned int flags = out->buf_flags;
  if (OIDP(x)) {
    if ( (refs) && (! ( (flags) & (XTYPE_NO_XTREFS) ) ) ) {
      lispval base = kno_oid_ptr_type | ((x)&_KNO_OID_BUCKET_MASK);
      ssize_t xtref = kno_xtype_ref(base,refs,-1);
      if (xtref >= 0) {
	if (kno_write_byte(out,xt_offref) > 0) {
	  long long offset = KNO_OID_BASE_OFFSET(x);
	  ssize_t base_len = kno_write_varint(out,xtref);
	  if (base_len<0) return base_len;
	  ssize_t off_len = kno_write_varint(out,offset);
	  if (off_len > 0)
	    return 1+base_len+off_len;
	  else return off_len;}}}
    KNO_OID addr = KNO_OID_ADDR(x);
    kno_write_byte(out,xt_oid);
    kno_write_4bytes(out,KNO_OID_HI(addr));
    kno_write_4bytes(out,KNO_OID_LO(addr));
    return 9;}
  else if (SYMBOLP(x)) {
    if ( (refs) && (! ( (flags) & (XTYPE_NO_XTREFS) ) ) ) {
      ssize_t xtref = kno_xtype_ref(x,refs,-1);
      if (xtref >= 0) {
	kno_write_byte(out,xt_absref);
	return 1+kno_write_varint(out,xtref);}}
    u8_string pname = KNO_SYMBOL_NAME(x);
    ssize_t len = strlen(pname);
    int rv = kno_write_byte(out,xt_symbol);
    rv = kno_write_varint(out,len);
    if (KNO_EXPECT_FALSE(rv<0))
      return rv;
    return kno_write_bytes(out,pname,len);}
  else if (KNO_CHARACTERP(x)) {
    int code = KNO_CHAR2CODE(x);
    kno_write_byte(out,xt_character);
    ssize_t code_len = kno_write_varint(out,code);
    if (code_len < 0) return code_len;
    else return 1+code_len;}
  else if (KNO_IMMEDIATEP(x)) {
    int rv = -1;
    switch (x) {
    case KNO_FALSE:
      rv=kno_write_byte(out,xt_false); break;
    case KNO_TRUE:
      rv=kno_write_byte(out,xt_true); break;
    case KNO_EMPTY_CHOICE:
      rv=kno_write_byte(out,xt_empty_choice); break;
    case KNO_EMPTY_LIST:
      rv=kno_write_byte(out,xt_empty_list); break;
    case KNO_DEFAULT:
      rv=kno_write_byte(out,xt_default); break;
    case KNO_VOID:
      rv=kno_write_byte(out,xt_void); break;
    default:
      if (flags&XTYPE_WRITE_OPAQUE)
	return write_opaque(out,x,refs);
      else {
	kno_seterr("XType/BadImmediate","xt_write_dtype",NULL,x);
	return -1;}}
    return rv;}
  else if (KNO_FIXNUMP(x)) {
    ssize_t intval = KNO_FIX2INT(x);
    if (intval<0) {
      kno_write_byte(out,xt_negint);
      return 1+kno_write_varint(out,(-intval));}
    kno_write_byte(out,xt_posint);
    return 1+kno_write_varint(out,intval);}
  else NO_ELSE;
  kno_lisp_type ctype = KNO_CONSPTR_TYPE(x);
  switch (ctype) {
  case kno_string_type: case kno_secret_type: case kno_packet_type: {
    struct KNO_STRING *s = (kno_string) x;
    ssize_t len = s->str_bytelen;
    int xt_code = -1;
    switch (ctype) {
    case kno_string_type:
      xt_code=xt_utf8; break;
    case kno_packet_type:
      xt_code=xt_packet; break;
    case kno_secret_type:
      xt_code=xt_secret; break;
    default: {/* Never reached */}}
    kno_write_byte(out,xt_code);
    return 1+kno_write_varint(out,len)+
      kno_write_bytes(out,s->str_bytes,len);}
  case kno_choice_type: case kno_vector_type: {
    int xt_code; ssize_t n_elts; lispval *elts;
    if (ctype == kno_choice_type) {
      xt_code=xt_choice;
      n_elts = KNO_CHOICE_SIZE(x);
      elts = (lispval *) KNO_CHOICE_DATA(x);}
    else {
      xt_code=xt_vector;
      n_elts = KNO_VECTOR_LENGTH(x);
      elts = KNO_VECTOR_ELTS(x);}
    kno_write_byte(out,xt_code);
    ssize_t n_written = 1+kno_write_varint(out,n_elts);
    ssize_t i = 0; while (i<n_elts) {
      /* Check for output buffer overflow */
      lispval elt = elts[i];
      ssize_t rv = write_xtype(out,elt,refs);
      if (rv>0) n_written+=rv;
      i++;}
    return n_written;}
  case kno_pair_type: {
    struct KNO_PAIR *pair = (kno_pair) x;
    kno_write_byte(out,xt_pair);
    ssize_t car_bytes = write_xtype(out,pair->car,refs);
    if (car_bytes<0) return car_bytes;
    /* Check for output buffer overflow */
    ssize_t cdr_bytes = write_xtype(out,pair->cdr,refs);
    if (cdr_bytes<0) return cdr_bytes;
    /* Check for output buffer overflow */
    return 1+car_bytes+cdr_bytes;}
  case kno_slotmap_type:
    return write_slotmap(out,(kno_slotmap)x,refs);
  case kno_schemap_type:
    return write_schemap(out,(kno_schemap)x,refs);
  default:
    if (kno_xtype_writers[ctype])
      return kno_xtype_writers[ctype](out,x,refs);
    else if (flags&XTYPE_WRITE_OPAQUE)
      return write_opaque(out,x,refs);
    else {
      kno_seterr("CantWriteXType","write_xtype",NULL,x);
      return -1;}}
}

KNO_EXPORT ssize_t kno_write_xtype(kno_outbuf out,lispval x,xtype_refs refs)
{
  return write_xtype(out,x,refs);
}

/* Reading XTYPEs */

static lispval read_xtype(kno_inbuf in,xtype_refs refs)
{
  int byte = kno_read_byte(in);
  xt_type_code xt_code = (xt_type_code) byte;
  switch (xt_code) {
  case xt_true: return KNO_TRUE;
  case xt_false: return KNO_FALSE;
  case xt_empty_choice: return KNO_EMPTY;
  case xt_empty_list: return KNO_NIL;
  case xt_default: return KNO_DEFAULT;
  case xt_void: return KNO_VOID;

  case xt_rational: return rational_xtype_tag;
  case xt_complex: return complex_xtype_tag;
  case xt_timestamp: return timestamp_xtype_tag;
  case xt_zcompress: return zcompress_xtype_tag;

  case xt_absref: {
    ssize_t off = xt_read_varint(in);
    if ( (refs) && (off < refs->xt_n_refs) )
      return refs->xt_refs[off];
    else if (refs == NULL)
      return kno_err("No xtype refs declared","read_xtype",NULL,VOID);
    else return kno_err("No xtype ref out of range","read_xtype",NULL,VOID);}
  case xt_offref: {
    if (PRED_FALSE(refs==NULL))
      return kno_err("No xtype refs declared","read_xtype",NULL,VOID);
    lispval base = KNO_VOID;
    ssize_t base_off = xt_read_varint(in);
    if (PRED_FALSE(base_off<0))
      return kno_err("InvalidBaseOff","read_xtype",NULL,VOID);
    else if (PRED_FALSE(base_off < refs->xt_n_refs))
      return kno_err("xtype base ref out of range","read_xtype",NULL,VOID);
    else base = refs->xt_refs[base_off];
    ssize_t oid_off  = xt_read_varint(in);
    if (oid_off<0) return kno_err("InvalidBaseOff","read_xtype",NULL,VOID);
    if (!(OIDP(base))) return kno_err("BadBaseRef","read_xtype",NULL,base);
    int baseid = KNO_OID_BASE_ID(base);
    return KNO_CONSTRUCT_OID(baseid,oid_off);}

  case xt_double: {
    char bytes[8];
    double flonum, *f;
    int rv = kno_read_bytes(bytes,in,8);
    if (rv<0) return unexpected_eod();
#if WORDS_BIGENDIAN
    f = (double *)&bytes;
#else
    char reversed[8]; int i = 0; while (i<8) {
      reversed[i] = bytes[7-i]; i++;}
    f = (double *) &reversed;
#endif
    flonum = *f;
    return _kno_make_double(flonum);}

  case xt_oid: {
    long long hival=kno_read_4bytes(in), loval;
    if (PRED_FALSE(hival<0))
      return unexpected_eod();
    else loval=kno_read_4bytes(in);
    if (PRED_FALSE(loval<0))
      return unexpected_eod();
    else {
      KNO_OID addr = KNO_NULL_OID_INIT;
      KNO_SET_OID_HI(addr,hival);
      KNO_SET_OID_LO(addr,loval);
      return kno_make_oid(addr);}}
  case xt_objid: {
    lispval packet = kno_init_packet(NULL,12,NULL);
    unsigned char *data = (unsigned char *) (KNO_PACKET_DATA(packet));
    int rv = kno_read_bytes(data,in,12);
    if (rv<0) return KNO_ERROR;
    else return kno_init_compound(NULL,objid_symbol,0,1,packet);}
  case xt_uuid: {
    struct KNO_UUID *uuid = u8_alloc(struct KNO_UUID);
    KNO_INIT_CONS(uuid,kno_uuid_type);
    int rv = kno_read_bytes(uuid->uuid16,in,16);
    if (rv<0) return KNO_ERROR;
    else return LISP_CONS(uuid);}

  case xt_posint: case xt_negint: case xt_character: {
    long long ival = xt_read_varint(in);
    if (ival<0) return KNO_ERROR;
    else switch (xt_code) {
      case xt_posint:
	return KNO_INT(ival);
      case xt_negint:
	ival = -ival;
	return KNO_INT(ival);
      default:
	return KNO_CODE2CHAR(ival);}}

  case xt_utf8: case xt_packet: case xt_secret: {
    ssize_t len = xt_read_varint(in);
    int cons_type = (xt_code == xt_utf8) ? (kno_string_type) :
      (xt_code == xt_packet) ? (kno_packet_type) :
      (kno_secret_type);
    struct KNO_STRING *s = u8_malloc(sizeof(KNO_STRING)+(len+1));
    u8_byte *buf = ((u8_byte*)s)+sizeof(struct KNO_STRING);
    KNO_INIT_CONS(s,cons_type);
    s->str_freebytes = 0;
    s->str_bytelen   = len;
    s->str_bytes     = buf;
    int rv = kno_read_bytes(buf,in,len);
    if (rv<0) {
      u8_free(s);
      return KNO_ERROR;}
    buf[len]=0;
    return LISP_CONS(s);}
  case xt_symbol: {
    ssize_t len = xt_read_varint(in);
    if (len<0) return KNO_ERROR;
    u8_byte pname[len+1];
    int rv = kno_read_bytes(pname,in,len);
    if (rv<0) return KNO_ERROR;
    pname[len]='\0';
    return kno_intern(pname);}

  case xt_block: {
    ssize_t len = xt_read_varint(in);
    if (len<0) return KNO_ERROR;
    unsigned char *buf = u8_malloc(len);
    int rv = kno_read_bytes(buf,in,len);
    if (rv<0) return KNO_ERROR;}
  case xt_posbig: case xt_negbig: {
    ssize_t n_digits = xt_read_varint(in);
    unsigned char _bytes[128], *bytes=_bytes;
    if (n_digits > 128) bytes=u8_malloc(n_digits);
    int rv = kno_read_bytes(bytes,in,n_digits);
    if (rv<0) {
      if (bytes != _bytes) u8_free(bytes);
      return KNO_ERROR;}
    lispval result =
      restore_bigint(n_digits,bytes,(xt_code == xt_negbig));
    if (bytes != _bytes) u8_free(bytes);
    if (KNO_BIGINTP(result))
      return result;
    else return KNO_ERROR;}
  case xt_choice: {
    ssize_t n_elts = xt_read_varint(in);
    struct KNO_CHOICE *ch = kno_alloc_choice(n_elts);
    lispval *elts = (lispval *) KNO_XCHOICE_ELTS(ch);
    KNO_INIT_CONS(ch,kno_choice_type);
    ch->choice_size=n_elts;
    int atomicp = 1; ssize_t i = 0; while (i < n_elts) {
      lispval elt = read_xtype(in,refs);
      if (KNO_ABORTED(elt)) {
	int j = 0; while (j<i) {
	  lispval gc_elt = elts[j]; kno_decref(gc_elt); j++;}
	return elt;}
      if ( (atomicp) && (KNO_CONSP(elt)) ) atomicp=0;
      elts[i]=elt;
      i++;}
    return kno_init_choice(ch,n_elts,elts,
			   ((atomicp)?(KNO_CHOICE_ISATOMIC):
			    (KNO_CHOICE_ISCONSES)) |
			   KNO_CHOICE_DOSORT|KNO_CHOICE_COMPRESS|
			   KNO_CHOICE_REALLOC);}
  case xt_vector: {
    ssize_t n_elts = xt_read_varint(in);
    lispval vec = kno_make_vector(n_elts,NULL);
    lispval *elts = KNO_VECTOR_ELTS(vec);
    int atomicp = 1; ssize_t i = 0; while (i < n_elts) {
      lispval elt = read_xtype(in,refs);
      if (KNO_ABORTED(elt)) {
	int j = 0; while (j<i) {
	  lispval gc_elt = elts[j]; kno_decref(gc_elt); j++;}
	return elt;}
      if ( (atomicp) && (KNO_CONSP(elt)) ) atomicp=0;
      elts[i]=elt;
      i++;}
    return vec;}
  case xt_table: {
    ssize_t n_elts = xt_read_varint(in), n_slots = n_elts/2;
    if (n_elts%2)
      return kno_err("CorruptXTable","read_xtype",NULL,VOID);
    lispval map = kno_make_slotmap(n_slots,n_slots,NULL);
    struct KNO_KEYVAL *kv = KNO_SLOTMAP_KEYVALS(map);
    ssize_t i = 0; while (i < n_slots) {
      lispval key = read_xtype(in,refs), val = read_xtype(in,refs);
      kv[i].kv_key = key; kv[i].kv_val = val;
      i++;}
    return map;}

  case xt_pair: case xt_tagged:
  case xt_mimeobj: case xt_compressed:
  case xt_encrypted: {
    lispval car = read_xtype(in,refs);
    if (ABORTED(car)) return car;
    lispval cdr = read_xtype(in,refs);
    if (ABORTED(cdr)) {
      kno_decref(car);
      return cdr;}
    if (xt_code == xt_pair)
      return kno_init_pair(NULL,car,cdr);
    else if (xt_code == xt_tagged)
      return restore_tagged(car,cdr,refs);
    else if (xt_code == xt_mimeobj)
      return kno_init_compound
	(NULL,mime_symbol,KNO_COMPOUND_USEREF,2,car,cdr);
    else if (xt_code == xt_compressed)
      return restore_compressed(car,cdr,refs);
    else if (xt_code == xt_encrypted)
      return kno_init_compound
	(NULL,encrypted_symbol,KNO_COMPOUND_USEREF,2,car,cdr);
    else return KNO_VOID;}
  default:
    return kno_err("BadXType","kno_read_xtype",NULL,VOID);}
}

KNO_EXPORT lispval kno_read_xtype(kno_inbuf in,xtype_refs refs)
{
  return read_xtype(in,refs);
}

/* Object dump handlers */

static ssize_t write_slotmap(kno_outbuf out,struct KNO_SLOTMAP *map,
			     xtype_refs refs)
{
  ssize_t xtype_len = 1;
  kno_read_lock_table(map);
  {
    struct KNO_KEYVAL *keyvals = map->sm_keyvals;
    int i = 0, kvsize = KNO_XSLOTMAP_NUSED(map), len = kvsize*2;
    kno_output_byte(out,xt_table);
    xtype_len += kno_write_varint(out,len);
    while (i < kvsize) {
      lispval key = keyvals[i].kv_key;
      lispval val = keyvals[i].kv_val;
      ssize_t key_len = write_xtype(out,key,refs);
      if (key_len<0) { xtype_len=-1; break; }
      else {
	xtype_len += key_len;
	ssize_t value_len = write_xtype(out,val,refs);
	if (value_len<0) { xtype_len=-1; break; }
	else xtype_len += value_len;
	i++;}}
    kno_unlock_table(map);
    return xtype_len;}
}

static ssize_t write_schemap(kno_outbuf out,
			     struct KNO_SCHEMAP *map,
			     xtype_refs refs)
{
  ssize_t xtype_len = 1;
  kno_read_lock_table(map);
  {
    lispval *schema = map->table_schema, *values = map->schema_values;
    int i = 0, schemasize = KNO_XSCHEMAP_SIZE(map), len = schemasize*2;
    kno_output_byte(out,xt_table);
    xtype_len += kno_write_varint(out,len);
    while (i < schemasize) {
      lispval key = schema[i], val = values[i];
      ssize_t key_len = write_xtype(out,key,refs);
      if (key_len<0) { xtype_len=-1; break; }
      else {
	xtype_len += key_len;
	ssize_t value_len = write_xtype(out,val,refs);
	if (key_len<0) { xtype_len=-1; break; }
	else xtype_len += value_len;
	i++;}}
    kno_unlock_table(map);
    return xtype_len;}
}

static ssize_t write_opaque(kno_outbuf out, lispval x,xtype_refs refs)
{
  U8_STATIC_OUTPUT(unparse,128);
  kno_unparse(unparseout,x);
  ssize_t sz = 1;
  kno_write_byte(out,xt_tagged);
  ssize_t rv = write_xtype(out,knopaque_symbol,refs);
  if (rv<0) {
    u8_close_output(unparseout);
    return rv;}
  else sz += rv;
  kno_write_byte(out,xt_utf8); sz++;
  ssize_t len = u8_outlen(unparseout);
  sz += kno_write_varint(out,len);
  sz += kno_write_bytes(out,u8_outstring(unparseout),len);
  u8_close_output(unparseout);
  return sz;
}

/* Restore handlers */

static lispval restore_tagged(lispval tag,lispval data,xtype_refs refs)
{
  if (tag == rational_xtype_tag) {
    if (PAIRP(data))
      return _kno_make_rational(KNO_CAR(data),KNO_CDR(data));}
  else if (tag == complex_xtype_tag) {
    if (PAIRP(data))
      return _kno_make_complex(KNO_CAR(data),KNO_CDR(data));}
  else if (tag == timestamp_xtype_tag) {
    if (FIXNUMP(data))
      return kno_time2timestamp((time_t)(KNO_FIX2INT(data)));}
  else if (tag == zcompress_xtype_tag) {
    if (PACKETP(data)) {
      ssize_t compressed_len = KNO_PACKET_LENGTH(data);
      const unsigned char *bytes = KNO_PACKET_DATA(data);
      ssize_t uncompressed_len = 0;
      unsigned char *uncompressed =
	do_zuncompress(bytes,compressed_len,
		       &uncompressed_len,NULL);
      if (uncompressed) {
	struct KNO_INBUF inflated = { 0 };
	KNO_INIT_BYTE_INPUT(&inflated,uncompressed,uncompressed_len);
	lispval value = kno_read_xtype(&inflated,refs);
        u8_big_free(uncompressed);
        return value;}
      else return kno_err("UncompressFailed","xt_zread",NULL,VOID);}}
  else NO_ELSE;
  struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
  if ((e) && (e->type_restorefn)) {
    lispval result = e->type_restorefn(tag,data,e);
    return result;}
  else {
    int flags = 0;
    if (e) {
      if (e->type_isopaque)
	flags |= KNO_COMPOUND_OPAQUE;
      if (e->type_ismutable)
	flags |= KNO_COMPOUND_MUTABLE;
      if (e->type_issequence)
	flags |= KNO_COMPOUND_SEQUENCE;
      if (e->type_istable)
	flags |= KNO_COMPOUND_TABLE;}
    if (KNO_VECTORP(data))
      return kno_init_compound_from_elts(NULL,tag,flags,
					 KNO_VECTOR_LENGTH(data),
					 KNO_VECTOR_ELTS(data));
    else return kno_init_compound(NULL,tag,flags,1,data);}
}

static lispval restore_compressed(lispval tag,lispval data,xtype_refs refs)
{
  if (tag == zcompress_xtype_tag) {
    if (PACKETP(data)) {
      ssize_t compressed_len = KNO_PACKET_LENGTH(data);
      const unsigned char *bytes = KNO_PACKET_DATA(data);
      ssize_t uncompressed_len = 0;
      unsigned char *uncompressed =
	do_zuncompress(bytes,compressed_len,
		       &uncompressed_len,NULL);
      if (uncompressed) {
	struct KNO_INBUF inflated = { 0 };
	KNO_INIT_BYTE_INPUT(&inflated,uncompressed,uncompressed_len);
	lispval value = kno_read_xtype(&inflated,refs);
        u8_big_free(uncompressed);
        return value;}
      else return kno_err("UncompressFailed","xt_zread",NULL,VOID);}}
  else return kno_init_compound
	 (NULL,compressed_symbol,KNO_COMPOUND_USEREF,2,tag,data);
}

/* Uncompressing */

static unsigned char *do_zuncompress
(const unsigned char *bytes,size_t n_bytes,
 ssize_t *dbytes,unsigned char *init_dbuf)
{
  u8_condition error = NULL; int zerror;
  unsigned long csize = n_bytes, dsize, dsize_max;
  Bytef *cbuf = (Bytef *)bytes, *dbuf;
  if (init_dbuf == NULL) {
    dsize = dsize_max = csize*4;
    dbuf = u8_big_alloc(dsize_max);}
  else {
    dbuf = init_dbuf;
    dsize = dsize_max = *dbytes;}
  while ((zerror = uncompress(dbuf,&dsize,cbuf,csize)) < Z_OK)
    if (zerror == Z_MEM_ERROR) {
      error=_("ZLIB ran out of memory"); break;}
    else if (zerror == Z_BUF_ERROR) {
      /* We don't use realloc because there's not point in copying
	 the data and we hope the overhead of free/malloc beats
	 realloc when we're doubling the buffer. */
      if (dbuf!=init_dbuf) u8_big_free(dbuf);
      dbuf = u8_big_alloc(dsize_max*2);
      if (dbuf == NULL) {
	error=_("pool value uncompress ran out of memory");
	break;}
      dsize = dsize_max = dsize_max*2;}
    else if (zerror == Z_DATA_ERROR) {
      error=_("ZLIB uncompress data error"); break;}
    else {
      error=_("Bad ZLIB return code"); break;}
  if (error == NULL) {
    *dbytes = dsize;
    return dbuf;}
  else {
    if (dbuf != init_dbuf) u8_big_free(dbuf);
    return KNO_ERR2(NULL,error,"do_zuncompress");}
}

/* File initialization */

KNO_EXPORT void kno_init_xtypes_c()
{
  u8_register_source_file(_FILEINFO);

  objid_symbol = kno_intern("_objid");
  mime_symbol = kno_intern("%mime");
  encrypted_symbol = kno_intern("%encrypted");
  compressed_symbol = kno_intern("%compressed");
  error_symbol = kno_intern("%error");
  knopaque_symbol = kno_intern("_knopaque");

  int i = 0; while (i < KNO_TYPE_MAX) {
    kno_xtype_writers[i++]=NULL;};

  rational_xtype_tag  = kno_register_constant("rational_xttag");
  complex_xtype_tag   = kno_register_constant("complex_xttag");
  timestamp_xtype_tag = kno_register_constant("timestamp_xttag");
  zcompress_xtype_tag = kno_register_constant("zcompress_xttag");

}
