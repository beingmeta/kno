/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1
#define KNO_INLINE_XTYPE_REFS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/numbers.h"
#include "kno/xtypes.h"

#include <libu8/u8elapsed.h>
#include <libu8/u8printf.h>

#include <zlib.h>

#if HAVE_SNAPPYC_H
#include <snappy-c.h>
#endif
#if HAVE_ZSTD_H
#include <zstd.h>
#endif

#include <errno.h>

#ifndef KNO_DEBUG_XTYPEIO
#define KNO_DEBUG_XTYPEIO 0
#endif

static unsigned char *xtype_zlib_uncompress
(const unsigned char *bytes,size_t n_bytes,
 ssize_t *dbytes,unsigned char *init_dbuf);
static unsigned char *xtype_zstd_uncompress
(ssize_t *destlen,const unsigned char *source,size_t source_len,void *state);
static unsigned char *xtype_snappy_uncompress
(ssize_t *destlen,const unsigned char *source,size_t source_len);

static ssize_t write_xtype(kno_outbuf out,lispval x,xtype_refs refs);

lispval kno_xtrefs_typetag;

static lispval objid_symbol, mime_symbol, encrypted_symbol,
  compressed_symbol, error_symbol, mime_symbol, knopaque_symbol,
  xrefs_symbol;

kno_xtype_fn kno_xtype_writers[KNO_TYPE_MAX];

lispval restore_bigint(ssize_t n_digits,unsigned char *bytes,int negp);

#define nobytes(in,nbytes) (RARELY(!(kno_request_bytes(in,nbytes))))
#define havebytes(in,nbytes) (USUALLY(kno_request_bytes(in,nbytes)))

/* Object restoration */

static lispval read_tagged(lispval tag,kno_inbuf in,xtype_refs refs,int);
static lispval restore_tagged(lispval tag,lispval data,xtype_refs refs,int);
static lispval restore_compressed(lispval tag,lispval data,xtype_refs refs);

static lispval read_bigtable(lispval tag,kno_inbuf in,xtype_refs refs);
static lispval read_bigset(lispval tag,kno_inbuf in,xtype_refs refs);


static ssize_t write_opaque
(kno_outbuf out, lispval x,xtype_refs refs);
static ssize_t write_slotmap(kno_outbuf out,struct KNO_SLOTMAP *map,xtype_refs refs);
static ssize_t write_schemap(kno_outbuf out,struct KNO_SCHEMAP *map,xtype_refs refs);
static ssize_t write_hashtable(kno_outbuf out,struct KNO_HASHTABLE *hashtable,xtype_refs refs);
static ssize_t write_hashset(kno_outbuf out,struct KNO_HASHSET *hashset,xtype_refs refs);

static ssize_t write_compound(kno_outbuf out,lispval tag,
			      struct KNO_COMPOUND *cvec,
			      xtype_refs refs);

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

KNO_EXPORT int kno_init_xrefs(xtype_refs refs,int flags,
			      int zero_refs,int n_refs,int refs_len,int refs_max,
			      lispval *elts,
			      kno_hashtable lookup)
{
  refs->xt_refs_zero = zero_refs;
  refs->xt_refs_count = n_refs;
  refs->xt_refs_len = refs_len;
  if (refs_max>0) {
    if (refs_max<refs_len) refs_max = refs_len;}
  else if (refs_max == 0)
    refs_max = refs_len;
  else refs->xt_refs_max = -1;
  if (flags<0)
    refs->xt_refs_flags = XTYPE_REFS_DEFAULT;
  else refs->xt_refs_flags = flags;
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

/* Recycle method for wrapped xtype refs */
static void recycle_xtype_refs(void *ptr)
{
  struct XTYPE_REFS *refs = (xtype_refs) ptr;
  kno_recycle_xrefs(refs);
  u8_free(refs);
}

KNO_EXPORT ssize_t kno_add_xtype_ref(lispval x,xtype_refs refs)
{
  if ( (KNO_OIDP(x)) || (KNO_SYMBOLP(x)) ) {
    lispval v = kno_hashtable_get(refs->xt_lookup,x,KNO_VOID);
    if (KNO_FIXNUMP(v))
      return KNO_FIX2INT(v);
    if ( (refs->xt_refs_flags) & (XTYPE_REFS_READ_ONLY) )
      return -1;
    size_t ref = refs->xt_n_refs;
    if (ref >= refs->xt_refs_len) {
      if ( (refs->xt_refs_max>=0) &&
	   (refs->xt_refs_len >= refs->xt_refs_max) ) {
	refs->xt_refs_flags |= XTYPE_REFS_READ_ONLY;
	return -1;}
      ssize_t delta = refs->xt_refs_len;
      if (delta > XTYPE_REFS_DELTA_MAX)
	delta=XTYPE_REFS_DELTA_MAX;
      ssize_t new_size = refs->xt_refs_len+delta;
      lispval *refvals = refs->xt_refs,
	*new_refs = u8_realloc(refvals,sizeof(lispval)*new_size);
      if (new_refs == NULL) {
	refs->xt_refs_flags |= XTYPE_REFS_READ_ONLY;
	return -1;}
      else {
	refs->xt_refs = new_refs;
	refs->xt_refs_len = new_size;}}
    int rv = kno_hashtable_add(refs->xt_lookup,x,KNO_INT(ref));
    if (rv>0) {
      refs->xt_refs_flags |= XTYPE_REFS_CHANGED;
      refs->xt_refs[ref]=x;
      refs->xt_n_refs++;
      return ref;}
    else return -1;}
  else return -1;
}

KNO_EXPORT ssize_t _kno_xtype_ref(lispval x,xtype_refs refs,int add)
{
  return kno_xtype_ref(x,refs,add);
}

KNO_EXPORT void kno_recycle_xrefs(xtype_refs refs)
{
  if ( (refs->xt_lookup != NULL) &&
       ( ( (refs->xt_refs_flags) & (XTYPE_REFS_EXT_ELTS) ) == 0) ) {
    kno_recycle_hashtable(refs->xt_lookup);
    u8_free(refs->xt_lookup);}
  if ( ( (refs->xt_refs_flags) & (XTYPE_REFS_EXT_ELTS) ) == 0) {
    if ( ( (refs->xt_refs_flags) & (XTYPE_REFS_CONS_ELTS) ) ) {
      lispval *elts = refs->xt_refs;
      int i = 0, len = refs->xt_n_refs; while (i<len) {
	lispval elt = elts[i++]; kno_decref(elt);}}
    u8_free(refs->xt_refs);}
  memset(refs,0,sizeof(struct XTYPE_REFS));
}

/* Writing sorted choices */

static ssize_t write_choice_xtype(kno_outbuf out,kno_choice ch,xtype_refs refs)
{
  int n_choices = KNO_XCHOICE_SIZE(ch);
  lispval _natsorted[17];
  lispval *natsorted = kno_natsort_choice(ch,_natsorted,17);
  const lispval *data = natsorted;
  int i = 0; ssize_t xtype_len = 0;
  int rv = kno_write_byte(out,xt_choice);
  if (rv<0) return rv;
  else rv = kno_write_varint(out,n_choices);
  if (rv<0) return rv;
  else xtype_len = rv+1;
  while (i < n_choices) {
    lispval elt = data[i];
    ssize_t elt_size = write_xtype(out,elt,refs);
    if (elt_size<0) return elt_size;
    xtype_len += elt_size;
    i++;}
  if (natsorted!=_natsorted) u8_free(natsorted);
  return xtype_len;
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
	  ssize_t off_len = kno_write_varint(out,offset);
	  if (off_len<0) return off_len;
	  ssize_t base_len = kno_write_varint(out,xtref);
	  if (base_len > 0)
	    return 1+base_len+off_len;
	  else return base_len;}}}
    KNO_OID addr = KNO_OID_ADDR(x);
    kno_write_byte(out,xt_oid);
    kno_write_4bytes(out,KNO_OID_HI(addr));
    kno_write_4bytes(out,KNO_OID_LO(addr));
    return 9;}
  else if (KNO_FIXNUMP(x)) {
    ssize_t intval = KNO_FIX2INT(x);
    if (intval<0) {
      kno_write_byte(out,xt_negint);
      return 1+kno_write_varint(out,(-intval));}
    kno_write_byte(out,xt_posint);
    return 1+kno_write_varint(out,intval);}
  else if (KNO_IMMEDIATEP(x)) {
    if (SYMBOLP(x)) {
      if ( (refs) && (! ( (flags) & (XTYPE_NO_XTREFS) ) ) ) {
	ssize_t xtref = kno_xtype_ref(x,refs,-1);
	if (xtref >= 0) {
	  kno_write_byte(out,xt_absref);
	  return 1+kno_write_varint(out,xtref);}}
      u8_string pname = KNO_SYMBOL_NAME(x);
      ssize_t len = strlen(pname);
      int rv = kno_write_byte(out,xt_symbol);
      rv = kno_write_varint(out,len);
      if (KNO_RARELY(rv<0))
	return rv;
      return 1+rv+kno_write_bytes(out,pname,len);}
    else if (KNO_CHARACTERP(x)) {
      int code = KNO_CHAR2CODE(x);
      kno_write_byte(out,xt_character);
      ssize_t code_len = kno_write_varint(out,code);
      if (code_len < 0) return code_len;
      else return 1+code_len;}
    else NO_ELSE;
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
    case KNO_VOID: case KNO_QVOID:
      rv=kno_write_byte(out,xt_void); break;
    default:
      if (x==kno_rational_xtag)
	rv=kno_write_byte(out,xt_rational);
      else if (x==kno_complex_xtag)
	rv=kno_write_byte(out,xt_complex);
      else if (x==kno_timestamp_xtag)
	rv=kno_write_byte(out,xt_timestamp);
      else if (x==kno_regex_xtag)
	rv=kno_write_byte(out,xt_regex);
      else if (x==kno_qchoice_xtag)
	rv=kno_write_byte(out,xt_qchoice);
      else if (x==kno_bigtable_xtag)
	rv=kno_write_byte(out,xt_bigtable);
      else if (x==kno_bigset_xtag)
	rv=kno_write_byte(out,xt_bigset);
      else if (x==kno_zlib_xtag)
	rv=kno_write_byte(out,xt_zlib);
      else if (x==kno_zstd_xtag)
	rv=kno_write_byte(out,xt_zstd);
      else if (x==kno_snappy_xtag)
	rv=kno_write_byte(out,xt_snappy);
      else {
	/* For immediates we check if they're in the table as possible xrefs.
	   This is handy for immediate types like opcodes which can be serialized. */
	if ( (refs) && (! ( (flags) & (XTYPE_NO_XTREFS) ) ) ) {
	  ssize_t xtref = kno_xtype_ref(x,refs,-1);
	  if (xtref >= 0) {
	    kno_write_byte(out,xt_absref);
	    return 1+kno_write_varint(out,xtref);}}
	kno_lisp_type typecode = KNO_IMMEDIATE_TYPE(x);
	if (kno_xtype_writers[typecode])
	  return kno_xtype_writers[typecode](out,x,refs);
	struct KNO_TYPEINFO *typeinfo = kno_probe_typeinfo(KNO_CTYPE(typecode));
	if ( (typeinfo) && (typeinfo->type_dumpfn) ) {
	  lispval dumped = typeinfo->type_dumpfn(x,typeinfo);
	  if (!(KNO_ABORTED(dumped))) {
	    ssize_t rv = kno_write_tagged_xtype
	      (out,typeinfo->type_usetag,dumped,refs);
	    kno_decref(dumped);
	    return rv;}
	  else {
	    u8_log(LOGWARN,"xtype/dumpfn_failed",
		   "Couldn't render %q data for %q",
		   typeinfo->typetag,x);
	    kno_clear_errors(1);}}
	if (flags&XTYPE_WRITE_OPAQUE)
	  return write_opaque(out,x,refs);
	else {
	  kno_seterr("XType/BadImmediate","xt_write_dtype",NULL,x);
	  return -1;}}}
    return rv;}
  else NO_ELSE;
  kno_lisp_type ctype = KNO_CONS_TYPEOF(x);
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
  case kno_choice_type:
    return write_choice_xtype(out,(kno_choice)x,refs);
  case kno_vector_type: {
    ssize_t n_elts = KNO_VECTOR_LENGTH(x);
    lispval *elts = KNO_VECTOR_ELTS(x);
    kno_write_byte(out,xt_vector);
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
  case kno_hashtable_type:
    return write_hashtable(out,(kno_hashtable)x,refs);
  case kno_hashset_type:
    return write_hashset(out,(kno_hashset)x,refs);
  case kno_compound_type: case kno_rawptr_type: {
    lispval typetag = (ctype==kno_compound_type) ? (KNO_COMPOUND_TAG(x)) :
      (KNO_RAWPTR_TAG(x));
    struct KNO_TYPEINFO *typeinfo = kno_probe_typeinfo(typetag);
    if ( (typeinfo) && (typeinfo->type_dumpfn) ) {
      lispval dumped = typeinfo->type_dumpfn(x,typeinfo);
      lispval usetag = typeinfo->type_usetag;
      if (KNO_VOIDP(usetag)) usetag=typetag;
      if (!(KNO_ABORTED(dumped))) {
	ssize_t rv = kno_write_tagged_xtype(out,usetag,dumped,refs);
	kno_decref(dumped);
	return rv;}
      else {
	u8_log(LOGWARN,"xtype/dumpfn_failed",
	       "Couldn't render %q data for %q",
	       typeinfo->typetag,x);
	kno_clear_errors(1);}}
    if (ctype == kno_compound_type)
      return write_compound(out,typetag,(kno_compound)x,refs);
    else if (flags&XTYPE_WRITE_OPAQUE)
      return write_opaque(out,x,refs);
    else {
      kno_seterr("XType/BadImmediate","xt_write_dtype",NULL,x);
      return -1;}}
  case kno_qchoice_type: {
    struct KNO_QCHOICE *qv = (kno_qchoice) x;
    kno_write_byte(out,xt_tagged);
    kno_write_byte(out,xt_qchoice);
    ssize_t content_len = write_xtype(out,qv->qchoiceval,refs);
    if (content_len<0) return content_len;
    else return 2+content_len;}
  case kno_prechoice_type: {
    lispval norm = kno_make_simple_choice(x);
    ssize_t rv = write_xtype(out,norm,refs);
    kno_decref(norm);
    return rv;}
  default:
    if (kno_xtype_writers[ctype])
      return kno_xtype_writers[ctype](out,x,refs);
    else if (flags&XTYPE_WRITE_OPAQUE)
      return write_opaque(out,x,refs);
    else {
      kno_seterr("CantWriteXType","write_xtype",
		 kno_type2name(ctype),x);
      return -1;}}
}

KNO_EXPORT ssize_t kno_write_xtype(kno_outbuf out,lispval x,xtype_refs refs)
{
  return write_xtype(out,x,refs);
}

KNO_EXPORT unsigned char *kno_encode_xtype(lispval x,ssize_t *sz,xtype_refs refs)
{
  KNO_DECL_OUTBUF(out,1000);
  ssize_t n_bytes = write_xtype(&out,x,refs);
  *sz = n_bytes;
  if (n_bytes<0) {
    kno_close_outbuf(&out);
    return NULL;}
  unsigned char *bytes = u8_malloc(n_bytes);
  memcpy(bytes,out.buffer,n_bytes);
  kno_close_outbuf(&out);
  return bytes;
}

KNO_EXPORT ssize_t kno_embed_xtype(kno_outbuf out,lispval x,xtype_refs refs)
{
  if ( (refs == NULL) || (refs->xt_n_refs == 0) )
    return write_xtype(out,x,refs);
  kno_write_byte(out,xt_refcoded);
  kno_write_byte(out,xt_vector);
  int i=0, n_refs = refs->xt_n_refs;
  ssize_t result_len = 2;
  ssize_t rv = kno_write_varint(out,n_refs);
  if (rv<=0) return -1;
  else result_len += rv;
  lispval *elts = refs->xt_refs;
  while (i < n_refs) {
    lispval ref = elts[i];
    ssize_t rv = write_xtype(out,ref,NULL);
    if (rv<=0) return -1;
    else result_len += rv;
    i++;}
  rv = write_xtype(out,x,refs);
  if (rv<=0) return -1;
  else return result_len+rv;
}

/* Reading XTYPEs */

static lispval read_xtype(kno_inbuf in,xtype_refs refs)
{
  unsigned int flags = in->buf_flags;
  if (RARELY(KNO_ISWRITING(in)))
    return kno_lisp_iswritebuf(in);
  else if (USUALLY(havebytes(in,1))) {
    int byte = kno_read_byte(in);
    xt_type_code xt_code = (xt_type_code) byte;
    switch (xt_code) {
    case xt_true: return KNO_TRUE;
    case xt_false: return KNO_FALSE;
    case xt_empty_choice: return KNO_EMPTY;
    case xt_empty_list: return KNO_NIL;
    case xt_default: return KNO_DEFAULT;
    case xt_void: return KNO_QVOID;

    case xt_rational: return kno_rational_xtag;
    case xt_complex: return kno_complex_xtag;
    case xt_timestamp: return kno_timestamp_xtag;
    case xt_regex: return kno_regex_xtag;
    case xt_qchoice: return kno_qchoice_xtag;
    case xt_bigtable: return kno_bigtable_xtag;
    case xt_bigset: return kno_bigset_xtag;
    case xt_zlib: return kno_zlib_xtag;
    case xt_zstd: return kno_zstd_xtag;
    case xt_snappy: return kno_snappy_xtag;

    case xt_absref: {
      ssize_t off = xt_read_varint(in);
      if ( (refs) && (off < refs->xt_n_refs) )
	return refs->xt_refs[off];
      else if (refs == NULL)
	return kno_err("No xtype refs declared","read_xtype",NULL,VOID);
      else return kno_err("No xtype ref out of range","read_xtype",NULL,VOID);}
    case xt_offref: {
      if (RARELY(refs==NULL))
	return kno_err("No xtype refs declared","read_xtype",NULL,VOID);
      ssize_t oid_off  = xt_read_varint(in);
      if (oid_off<0) return kno_err("InvalidRefOff","read_xtype",NULL,VOID);
      lispval base = KNO_VOID;
      ssize_t base_off = xt_read_varint(in);
      if (RARELY(base_off<0))
	return kno_err("InvalidBaseOff","read_xtype",NULL,VOID);
      else if (base_off < refs->xt_n_refs)
	base = refs->xt_refs[base_off];
      else return kno_err("xtype base ref out of range","read_xtype",NULL,VOID);
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
      if (RARELY(hival<0))
	return unexpected_eod();
      else loval=kno_read_4bytes(in);
      if (RARELY(loval<0))
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
      /* NUL-terminate it, even if it's a packet, since it's not counted
	 in the length. */
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
      lispval vec = kno_make_vector(n_elts,NULL), err = KNO_VOID;
      lispval *elts = KNO_VECTOR_ELTS(vec);
      int atomicp = 1; ssize_t i = 0; while (i < n_elts) {
	lispval elt = read_xtype(in,refs);
	if (ABORTED(elt)) { err=elt; break;}
	if ( (atomicp) && (KNO_CONSP(elt)) ) atomicp=0;
	elts[i]=elt;
	i++;}
      if (ABORTED(err)) {
	i--; while (i>=0) {
	    lispval gc_elt = elts[i];
	    kno_decref(gc_elt);
	    i--;}
	return err;}
      else return vec;}
    case xt_table: {
      ssize_t n_elts = xt_read_varint(in), n_slots = n_elts/2;
      if (n_elts%2)
	return kno_err("CorruptXTypeTable","read_xtype",NULL,VOID);
      lispval map = kno_make_slotmap(n_slots,n_slots,NULL), err = KNO_VOID;
      struct KNO_KEYVAL *kv = KNO_SLOTMAP_KEYVALS(map);
      ssize_t i = 0; while (i < n_slots) {
	lispval key = read_xtype(in,refs);
	if (ABORTED(key)) {err = key; break;}
	lispval val = read_xtype(in,refs);
	if (ABORTED(val)) {
	  kno_decref(key);
	  err = val;
	  break;}
	kv[i].kv_key = key; kv[i].kv_val = val;
	i++;}
      if (ABORTED(err)) {
	i--; while (i>=0) {
	  lispval key = kv[i].kv_key;
	  lispval val = kv[i].kv_val;
	  kno_decref(key);
	  kno_decref(val);
	  i--;}
	return map;}
      else return map;}
    case xt_compound: {
      ssize_t n_elts = xt_read_varint(in);
      if (n_elts < 4) return kno_err2("CorruptXTypeCompound","read_xtype");
      lispval tag = read_xtype(in,refs);
      lispval head_len = read_xtype(in,refs);
      lispval schema = read_xtype(in,refs);
      lispval table = read_xtype(in,refs);
      int sequencep = (KNO_FIXNUMP(head_len));
      int tablep = (KNO_TABLEP(table));
      int seqoff = (sequencep) ? (KNO_FIX2INT(head_len)) : (-1);
      int compound_flags =
	((sequencep) ?
	 ( (KNO_COMPOUND_SEQUENCE) | (seqoff<<8) ) :
	 (0) );
      kno_decref(schema); /* This is for future expansion */
      int n = n_elts-4;
      if (tablep) n++;
      lispval result =
	kno_init_compound_from_elts(NULL,tag,compound_flags,n,NULL);
      if (KNO_ABORTED(result)) {
	return KNO_ERROR;}
      struct KNO_COMPOUND *cvec = (kno_compound) result;
      if (tablep) {
	cvec->compound_annotated=1;
	if (sequencep) cvec->compound_seqoff++;}
      lispval *write = KNO_COMPOUND_ELTS(result);
      lispval *limit = write+n;
      if (tablep) *write++=table;
      while (write<limit) {
	lispval elt = read_xtype(in,refs);
	if (ABORTED(elt)) {
	  kno_decref(result);
	  result=elt;
	  break;}
	else *write++=elt;}
      return result;}
    case xt_tagged: {
      lispval tag = read_xtype(in,refs);
      if (ABORTED(tag)) return tag;
      return read_tagged(tag,in,refs,flags);}

    case xt_pair:
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
      else if (xt_code == xt_mimeobj)
	return kno_init_compound
	  (NULL,mime_symbol,KNO_COMPOUND_USEREF,2,car,cdr);
      else if (xt_code == xt_compressed) {
	lispval inflated = restore_compressed(car,cdr,refs);
	kno_decref(car);
	kno_decref(cdr);
	return inflated;}
      else if (xt_code == xt_encrypted)
	return kno_init_compound
	  (NULL,encrypted_symbol,KNO_COMPOUND_USEREF,2,car,cdr);
      else return KNO_VOID;}

    case xt_refcoded: {
      lispval refvec = read_xtype(in,refs);
      if (ABORTED(refvec)) return refvec;
      else if (!(KNO_VECTORP(refvec))) {
	kno_seterr("XTypeError","read_xtype(xt_refcoded)",NULL,refvec);
	kno_decref(refvec);
	return KNO_ERROR;}
      struct XTYPE_REFS xrefs;
      int n_refs = KNO_VECTOR_LENGTH(refvec);
      kno_init_xrefs(&xrefs,
		     (XTYPE_REFS_EXT_ELTS|XTYPE_REFS_READ_ONLY),
		     -1,n_refs,n_refs,n_refs,
		     KNO_VECTOR_ELTS(refvec),
		     NULL);
      lispval decoded = read_xtype(in,&xrefs);
      kno_recycle_xrefs(&xrefs);
      return decoded;}

    default:
      return kno_err("BadXType","kno_read_xtype",NULL,VOID);}
  }
  else return KNO_EOD;
}

KNO_EXPORT lispval kno_read_xtype(kno_inbuf in,xtype_refs refs)
{
  return read_xtype(in,refs);
}

KNO_EXPORT lispval kno_decode_xtype(unsigned char *bytes,size_t len,xtype_refs xrefs)
{
  KNO_DECL_INBUF(in,bytes,len,0);
  return read_xtype(&in,xrefs);
}

static lispval read_tagged(lispval tag,kno_inbuf in,xtype_refs refs,int flags)
{
  if ( tag == kno_bigtable_xtag )
    return read_bigtable(tag,in,refs);
  else if ( tag == kno_bigset_xtag )
    return read_bigset(tag,in,refs);
  else {
    lispval data = read_xtype(in,refs);
    if (ABORTED(data)) {
      kno_decref(tag);
      return data;}
    lispval restored = restore_tagged(tag,data,refs,flags);
    kno_decref(tag);
    kno_decref(data);
    return restored;}
}

static lispval read_bigtable(lispval tag,kno_inbuf in,xtype_refs refs)
{
  int code = kno_read_byte(in);
  if (code != xt_table)
    return kno_err("BadXType","read_tagged(bigtable)",NULL,KNO_VOID);
  ssize_t n_elts = kno_read_varint(in);
  if ( (n_elts<0) || (n_elts%2) )
    return kno_err("BadXType","read_tagged(bigtable/n_keys)",NULL,KNO_VOID);
  ssize_t n_keys = n_elts/2;
  struct KNO_KEYVAL *keyvals = u8_alloc_n(n_keys,struct KNO_KEYVAL);
  ssize_t i = 0; while (i<n_keys) {
    lispval key = read_xtype(in,refs);
    if (KNO_ABORTED(key)) break;
    lispval value = read_xtype(in,refs);
    if (KNO_ABORTED(value)) { kno_decref(key); break; }
    keyvals[i].kv_key = key;
    keyvals[i].kv_val = value;
    i++;}
  if (i == n_keys) {
    lispval table = kno_initialize_hashtable(NULL,keyvals,n_keys);
    u8_free(keyvals);
    return table;}
  else {
    ssize_t j = 0; while (j<i) {
      kno_decref(keyvals[j].kv_key);
      kno_decref(keyvals[j].kv_val);
      j++;}
    u8_free(keyvals);
    return KNO_ERROR_VALUE;}
}

static lispval read_bigset(lispval tag,kno_inbuf in,xtype_refs refs)
{
  int code = kno_read_byte(in);
  if (code != xt_vector)
    return kno_err("BadXType","read_tagged(bigtable)",NULL,KNO_VOID);
  ssize_t i = 0, n_elts = kno_read_varint(in);
  struct KNO_HASHSET *h = u8_alloc(struct KNO_HASHSET);
  kno_init_hashset(h,n_elts*2,KNO_MALLOCD_CONS);
  lispval result = (lispval) h;
  while (i<n_elts) {
    lispval elt = read_xtype(in,refs);
    if (KNO_ABORTED(elt)) {
      kno_decref(result);
      result = elt;
      break;}
    kno_hashset_add(h,elt);
    kno_decref(elt);}
  return result;
}

static int skip_bytes(kno_inbuf in,size_t n_bytes)
{
  if (kno_request_bytes(in,n_bytes)) {
    in->bufread = in->bufread+n_bytes;
    return 1;}
  else return -1;
}

static int validate_xtype(kno_inbuf in,xtype_refs refs)
{
  if (RARELY(KNO_ISWRITING(in))) {
    kno_lisp_iswritebuf(in);
    return -1;}
  else if (USUALLY(havebytes(in,1))) {
    int byte = kno_read_byte(in);
    xt_type_code xt_code = (xt_type_code) byte;
    switch (xt_code) {
    case xt_true: case xt_false: case xt_empty_choice:
    case xt_empty_list: case xt_default: case xt_void:
      return 1;
    case xt_rational: case xt_complex: case xt_timestamp:
    case xt_regex: case xt_qchoice: case xt_bigtable:
    case xt_zlib: case xt_zstd: case xt_snappy:
      return 1;
    case xt_absref: {
      ssize_t off = xt_read_varint(in);
      if (refs == NULL)
	return 1;
      else if (off < refs->xt_n_refs)
	return 1;
      else {
	u8_byte intbuf[32]; 
	kno_seterr("No xtype ref out of range","read_xtype",
		   u8_bprintf(intbuf,"%llud",(unsigned long long)off),
		   VOID);
	return -1;}}
    case xt_offref: {
      ssize_t oid_off  = xt_read_varint(in);
      if (oid_off<0) {
	kno_seterr("InvalidRefOff","read_xtype",NULL,VOID);
	return -1;}
      ssize_t base_off = xt_read_varint(in);
      if (RARELY(base_off<0)) {
	kno_err("InvalidBaseOff","read_xtype",NULL,VOID);
	return -1;}
      else if ( (refs == NULL) || (base_off < refs->xt_n_refs) )
	return 1;
      else {
	kno_err("xtype base ref out of range","read_xtype",NULL,VOID);
	return -1;}}

    case xt_double: case xt_oid: {
      char bytes[8];
      int rv = kno_read_bytes(bytes,in,8);
      if (rv<0) goto early_eod;
      return 1;}

    case xt_objid: {
      int rv = skip_bytes(in,12);
      if (rv<0) goto early_eod;
      else return 1;}
    case xt_uuid: {
      int rv = skip_bytes(in,16);
      if (rv<0) goto early_eod;
      else return 1;}
    case xt_posint: case xt_negint: case xt_character: {
      long long ival = xt_read_varint(in);
      if (ival<0) goto early_eod;
      else return 1;}
    case xt_utf8: case xt_packet: case xt_secret:
    case xt_symbol: case xt_posbig: case xt_negbig: {
      ssize_t len = xt_read_varint(in);
      if (len<0) goto early_eod;
      int rv = skip_bytes(in,len);
      if (rv<0) goto early_eod;
      else return 1;}
    case xt_block: {
      ssize_t len = xt_read_varint(in);
      if (len<0) goto early_eod;
      int rv = kno_request_bytes(in,len);
      if (rv<0) goto early_eod;
      const unsigned char *read=in->bufread;
      rv = validate_xtype(in,refs);
      if ( (rv>=0) && ( (in->bufread-read) != len) ) {
	kno_seterr("BadXTypeBlock","skip_xtype",NULL,VOID);
	rv = -1;}
      return rv;}

    case xt_choice: case xt_vector: case xt_table: {
      ssize_t n_elts = xt_read_varint(in);
      if (n_elts < 0) return -1;
      int i = 0; while (i<n_elts) {
	int rv = validate_xtype(in,refs);
	if (rv<0) return -1;
	else i++;}
      return 1;}

    case xt_pair: case xt_tagged:
    case xt_mimeobj: case xt_compressed:
    case xt_encrypted: {
      int rv = validate_xtype(in,refs);
      if (rv<0) return -1;
      else rv = validate_xtype(in,refs);
      if (rv<0) return -1;
      else return 1;}

    case xt_refcoded: {
      lispval refvec = read_xtype(in,refs);
      if (ABORTED(refvec)) return refvec;
      else if (!(KNO_VECTORP(refvec))) {
	kno_seterr("XTypeError","read_xtype(xt_refcoded)",NULL,refvec);
	kno_decref(refvec);
	return KNO_ERROR;}
      struct XTYPE_REFS xrefs;
      int n_refs = KNO_VECTOR_LENGTH(refvec);
      kno_init_xrefs(&xrefs,
		     (XTYPE_REFS_EXT_ELTS|XTYPE_REFS_READ_ONLY),
		     -1,n_refs,n_refs,n_refs,
		     KNO_VECTOR_ELTS(refvec),
		     NULL);
      int rv = validate_xtype(in,&xrefs);
      kno_recycle_xrefs(&xrefs);
      return rv;}

    default:
      return kno_err("BadXType","kno_read_xtype",NULL,VOID);}
  }
  else return -1;
 early_eod:
  u8_seterr(kno_UnexpectedEOD,"validate_xtype",NULL);
  return -1;
}

KNO_EXPORT int kno_validate_xtype(kno_inbuf in,xtype_refs refs)
{
  return validate_xtype(in,refs);
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
    lispval *schema = map->table_schema, *values = map->table_values;
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

static ssize_t write_hashtable(kno_outbuf out,struct KNO_HASHTABLE *hashtable,
			       xtype_refs refs)
{
  int i = 0, n_keys = -1;
  struct KNO_KEYVAL *keyvals = kno_hashtable_keyvals(hashtable,&n_keys,1);
  if (RARELY(n_keys < 0)) return -1;
  int len = n_keys*2;
  kno_output_byte(out,xt_tagged); kno_output_byte(out,xt_bigtable);
  kno_output_byte(out,xt_table);
  ssize_t rv = kno_write_varint(out,len);
  if (rv>0) {
    ssize_t xtype_len = 3+rv;
    while (i < n_keys) {
      lispval key = keyvals[i].kv_key;
      lispval val = keyvals[i].kv_val;
      ssize_t key_len = write_xtype(out,key,refs);
      if (key_len<0) { xtype_len=-1; break; }
      else xtype_len += key_len;
      ssize_t value_len = write_xtype(out,val,refs);
      if (value_len<0) { xtype_len=-1; break; }
      xtype_len += value_len;
      i++;}
    i = 0; while (i < n_keys) {
      kno_decref(keyvals[i].kv_key);
      kno_decref(keyvals[i].kv_val);
      i++;}
    u8_free(keyvals);
    return xtype_len;}
  else return rv;
}

static ssize_t write_hashset(kno_outbuf out,struct KNO_HASHSET *hashtable,
			     xtype_refs refs)
{
  ssize_t i = 0, n_elts = -1;
  lispval *elts = kno_hashset_vec(hashtable,&n_elts);
  if (RARELY(n_elts < 0)) return -1;
  kno_output_byte(out,xt_tagged); kno_output_byte(out,xt_bigset);
  kno_output_byte(out,xt_vector);
  ssize_t rv = kno_write_varint(out,n_elts);
  if (rv>0) {
    ssize_t xtype_len = 3+rv;
    while (i < n_elts) {
      lispval elt = elts[i];
      ssize_t elt_len = write_xtype(out,elt,refs);
      if (elt_len<0) { xtype_len=-1; break; }
      else xtype_len += elt_len;
      i++;}
    kno_decref_elts(elts,n_elts);
    u8_free(elts);
    return xtype_len;}
  else return rv;
}

KNO_EXPORT ssize_t kno_write_tagged_xtype
(kno_outbuf out,lispval tag,lispval data,xtype_refs refs)
{
  ssize_t size = 1;
  kno_output_byte(out,xt_tagged);
  ssize_t rv = kno_write_xtype(out,tag,refs);
  if (rv<0) return rv; else size += rv;
  rv = kno_write_xtype(out,data,refs);
  if (rv<0) return rv;
  else return size+rv;
}


static ssize_t write_compound
(kno_outbuf out,lispval tag,struct KNO_COMPOUND *cvec,xtype_refs refs)
{
  if (tag == KNOSYM_XTYPE) {
    if ( (cvec->compound_length>=1) &&
	 (KNO_PACKETP(KNO_COMPOUND_VREF(cvec,0))) ) {
      lispval packet = KNO_COMPOUND_VREF(cvec,0);
      ssize_t packet_len = KNO_PACKET_LENGTH(packet);
      const unsigned char *packet_data = KNO_PACKET_DATA(packet);
      if (cvec->compound_length==1) {
	kno_output_bytes(out,packet_data,packet_len);
	return packet_len;}
      else {
	lispval xrefs = KNO_COMPOUND_VREF(cvec,1);
	if (KNO_FALSEP(xrefs)) {
	  kno_output_bytes(out,packet_data,packet_len);
	  return packet_len;}
	else if (KNO_RAW_TYPEP(xrefs,kno_xtrefs_typetag)) {
	  xtype_refs cxrefs = KNO_RAWPTR_VALUE(xrefs);
	  if ( (refs == cxrefs) || (refs->xt_lookup == cxrefs->xt_lookup) ) {
	    kno_output_bytes(out,packet_data,packet_len);
	    return packet_len;}
	  else {/* This should probably unpack the object and repack it */}}}}}
  ssize_t bytes = 0;
  int n_elts = cvec->compound_length;
  lispval *elts = &(cvec->compound_0), *limit = elts + n_elts;
  int tablep = (cvec->compound_annotated != 0);
  int write_elts = n_elts + 4 - tablep;
  ssize_t rv = kno_write_byte(out,xt_compound);
  if (rv<0) return rv; else bytes += rv;
  rv = kno_write_varint(out,write_elts);
  if (rv<0) return rv; else bytes += rv;
  rv = write_xtype(out,tag,refs);
  if (rv<0) return rv; else bytes += rv;
  if (cvec->compound_seqoff>=0) {
    rv=kno_write_byte(out,xt_posint);
    if (rv==1) {
      bytes++; rv = kno_write_varint(out,cvec->compound_seqoff);}
    else return -1;}
  else rv = write_xtype(out,KNO_FALSE,NULL);
  if (rv<0) return rv; else bytes += rv;
  /* This is a space for compounds which have attached schema */
  rv = write_xtype(out,KNO_FALSE,NULL);
  if (rv<0) return rv; else bytes += rv;
  if (tablep)
    rv = write_xtype(out,*elts++,refs);
  else rv = write_xtype(out,KNO_FALSE,NULL);
  if (rv<0) return rv; else bytes += rv;
  while (elts<limit) {
    rv = write_xtype(out,*elts++,refs);
    if (rv<0) return rv; else bytes +=rv;}
  return bytes;
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

static lispval restore_tagged(lispval tag,lispval data,xtype_refs refs,int flags)
{
  if (tag == kno_timestamp_xtag) {
    if (FIXNUMP(data))
      return kno_time2timestamp((time_t)(KNO_FIX2INT(data)));}
  else if (tag == kno_qchoice_xtag)
      return kno_make_qchoice(data);
  else if (tag == kno_rational_xtag) {
    if (PAIRP(data))
      return kno_make_rational(KNO_CAR(data),KNO_CDR(data));}
  else if (tag == kno_complex_xtag) {
    if (PAIRP(data))
      return kno_make_complex(KNO_CAR(data),KNO_CDR(data));}
  else NO_ELSE;
  struct KNO_TYPEINFO *e = (flags&XTYPE_SKIP_RESTORE) ? (NULL) :
    (kno_use_typeinfo(tag));
  if ((e) && (e->type_restorefn)) {
    lispval result = e->type_restorefn(tag,data,e);
    return result;}
  else if ((e) && (kno_default_restorefn)) {
    lispval result = kno_default_restorefn(tag,data,e);
    return result;}
  else if (e) {
    int flags = KNO_COMPOUND_INCREF;
    if (e) {
      if (e->type_isopaque)
	flags |= KNO_COMPOUND_OPAQUE;
      if (e->type_ismutable)
	flags |= KNO_COMPOUND_MUTABLE;
      if (e->type_issequence)
	flags |= KNO_COMPOUND_SEQUENCE;
      if (e->type_istable)
	flags |= KNO_COMPOUND_TABLE;
      if ( (e->type_issequence) && (e->type_istable) )
	flags |= (1<<8);}
    if (KNO_VECTORP(data))
      return kno_init_compound_from_elts(NULL,tag,flags,
					 KNO_VECTOR_LENGTH(data),
					 KNO_VECTOR_ELTS(data));
    else return kno_init_compound(NULL,tag,flags,1,data);}
  else if (KNO_VECTORP(data))
    return kno_init_compound_from_elts(NULL,tag,flags,
				       KNO_VECTOR_LENGTH(data),
				       KNO_VECTOR_ELTS(data));
  else return kno_init_compound(NULL,tag,flags,1,data);
}

static lispval restore_compressed(lispval tag,lispval data,xtype_refs refs)
{
  if (!(PACKETP(data)))
    return kno_init_compound
      (NULL,compressed_symbol,KNO_COMPOUND_USEREF,2,tag,data);
  else {
    ssize_t compressed_len = KNO_PACKET_LENGTH(data);
    const unsigned char *bytes = KNO_PACKET_DATA(data);
    unsigned char *uncompressed = NULL;
    ssize_t uncompressed_len = 0;
    if (tag == kno_zlib_xtag)
      uncompressed = xtype_zlib_uncompress
	(bytes,compressed_len,&uncompressed_len,NULL);
    else if (tag == kno_zstd_xtag)
      uncompressed = xtype_zstd_uncompress
	(&uncompressed_len,bytes,compressed_len,NULL);
    else if (tag == kno_snappy_xtag)
      uncompressed = xtype_snappy_uncompress
	(&uncompressed_len,bytes,compressed_len);
    else return kno_init_compound
	   (NULL,compressed_symbol,KNO_COMPOUND_USEREF,2,tag,data);
    if (uncompressed) {
      struct KNO_INBUF inflated = { 0 };
      KNO_INIT_BYTE_INPUT(&inflated,uncompressed,uncompressed_len);
      lispval value = read_xtype(&inflated,refs);
      u8_big_free(uncompressed);
      return value;}
    else return kno_err("UncompressFailed","xt_zread",NULL,VOID);}
  return kno_init_compound
    (NULL,compressed_symbol,KNO_COMPOUND_USEREF,2,tag,data);
}

/* Uncompressing */

static unsigned char *xtype_zlib_uncompress
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

static unsigned char *xtype_snappy_uncompress
(ssize_t *destlen,const unsigned char *source,size_t source_len)
{
#if HAVE_SNAPPYC_H
  size_t uncompressed_size;
  snappy_status size_rv =
    snappy_uncompressed_length(source,source_len,&uncompressed_size);
  if (size_rv == SNAPPY_OK) {
    unsigned char *uncompressed = u8_big_alloc(uncompressed_size);
    snappy_status uncompress_rv=
      snappy_uncompress(source,source_len,uncompressed,&uncompressed_size);
    if (uncompress_rv == SNAPPY_OK) {
      *destlen = uncompressed_size;
      return uncompressed;}
    else {
      u8_big_free(uncompressed);
      u8_seterr("SnappyUncompressFailed","xtype_snappy_uncompress",NULL);}
      return NULL;}
  else {
    u8_seterr("SnappyUncompressFailed","xtype_snappy_uncompress",NULL);
    return NULL;}
#else
  kno_seterr(_("NoSnappySupport"),"xtype_snappy_uncompress",NULL,VOID);
  return NULL;
#endif
}

#if HAVE_ZSTD_H
#define zstd_error(code) (u8_fromlibc((char *)ZSTD_getErrorName(code)))
#endif

static unsigned char *xtype_zstd_uncompress
(ssize_t *destlen,const unsigned char *source,size_t source_len,void *state)
{
#if HAVE_ZSTD_H
#if HAVE_ZSTD_GETFRAMECONTENTSIZE
  size_t alloc_size = ZSTD_getFrameContentSize(source,source_len);
  if (RARELY(alloc_size == ZSTD_CONTENTSIZE_UNKNOWN)) {
    u8_seterr("UnknownContentSize","xtype_zstd_uncompress",
	      zstd_error(alloc_size));
    return NULL;}
  else if (RARELY(alloc_size == ZSTD_CONTENTSIZE_ERROR)) {
    u8_seterr("ZSTD_ContentSizeError","xtype_zstd_uncompress",
	      zstd_error(alloc_size));
    return NULL;}
#else
  size_t alloc_size = ZSTD_getDecompressedSize(source,source_len);
#endif
  unsigned char *uncompressed = u8_big_alloc(alloc_size);
  size_t uncompressed_size =
    ZSTD_decompress(uncompressed,alloc_size,source,source_len);
  if (ZSTD_isError(uncompressed_size)) {
    u8_seterr("ZSTD_UncompressError","xtype_zstd_uncompress",
	      zstd_error(uncompressed_size));
    u8_big_free(uncompressed);
    return NULL;}
  else {
    if ( (uncompressed_size*2) < alloc_size) {
      unsigned char *new_data = u8_big_realloc(uncompressed,uncompressed_size);
      if (new_data) uncompressed = new_data;}
    *destlen = uncompressed_size;
    return uncompressed;}
#else
    kno_seterr(_("NoZSTDSupport"),"xtype_zstd_uncompress",NULL,VOID);
    return NULL;
#endif
}

/* Getting xrefs from lisp objects */

KNO_EXPORT lispval kno_wrap_xrefs(struct XTYPE_REFS *refs)
{
  return kno_wrap_pointer((void *)refs,sizeof(struct XTYPE_REFS),
			  recycle_xtype_refs,
			  kno_xtrefs_typetag,
			  NULL);
}

KNO_EXPORT lispval kno_copy_xrefs(struct XTYPE_REFS *refs)
{
  struct XTYPE_REFS *copy = u8_alloc(struct XTYPE_REFS);
  unsigned int copy_flags = XTYPE_REFS_READ_ONLY;
  memset(copy,0,sizeof(struct XTYPE_REFS));
  int n_refs = refs->xt_n_refs;
  lispval *copycodes = u8_alloc_n(n_refs,lispval);
  if (refs->xt_refs_flags & XTYPE_REFS_CONS_ELTS) {
    lispval *codes = refs->xt_refs;
    int i = 0; while (i<n_refs) {
      lispval ref = codes[i]; kno_incref(ref);
      copycodes[i]=ref;
      i++;}
    copy_flags |= XTYPE_REFS_CONS_ELTS;}
  else memcpy(copycodes,refs->xt_refs,sizeof(lispval)*n_refs);
  copy->xt_refs_flags = copy_flags;
  copy->xt_refs_max=copy->xt_refs_len=n_refs;
  copy->xt_refs=copycodes;
  if (refs->xt_lookup) {
    kno_hashtable lookup = refs->xt_lookup;
    if (KNO_MALLOCD_CONSP(lookup)) {
      kno_incref(((lispval)lookup));
      copy->xt_lookup=lookup;}
    else {
      kno_hashtable lookup_copy = kno_copy_hashtable(NULL,lookup,1);
      if (!(lookup_copy)) {
	if (refs->xt_refs_flags & XTYPE_REFS_CONS_ELTS)
	  kno_decref_elts(copycodes,n_refs);
	u8_free(copycodes);
	u8_free(copy);
	return KNO_ERROR;}
      else copy->xt_lookup=lookup_copy;}}
  return kno_wrap_pointer((void *)copy,sizeof(struct XTYPE_REFS),
			  recycle_xtype_refs,
			  kno_xtrefs_typetag,
			  NULL);
}

KNO_EXPORT lispval kno_getxrefs(lispval arg)
{
  int free_arg = 0;
  if ( (KNO_FALSEP(arg)) || (KNO_VOIDP(arg)) || (KNO_DEFAULTP(arg)) )
    return KNO_FALSE;
  if (KNO_TABLEP(arg)) {
    arg = kno_getopt(arg,xrefs_symbol,KNO_VOID);
    if ( (KNO_FALSEP(arg)) || (KNO_VOIDP(arg)) || (KNO_DEFAULTP(arg)) )
      return KNO_FALSE;
    free_arg = 1;}
  if (KNO_RAW_TYPEP(arg,kno_xtrefs_typetag)) {
    if (free_arg)
      return arg;
    else return kno_incref(arg);}

  /* negative is an error, zero is uncreated, >0 is created */

  const lispval *elts = NULL; int n_elts = 0;
  if (KNO_PRECHOICEP(arg)) {
    if (free_arg)
      arg = kno_simplify_choice(arg);
    else {
      arg=kno_make_simple_choice(arg);
      free_arg=1;}}
  if (KNO_VECTORP(arg)) {
    elts =   KNO_VECTOR_ELTS(arg);
    n_elts = KNO_VECTOR_LENGTH(arg);}
  else if (KNO_CHOICEP(arg)) {
    elts =   KNO_CHOICE_ELTS(arg);
    n_elts = KNO_CHOICE_SIZE(arg);}
  else if ( (KNO_SYMBOLP(arg)) || (KNO_OIDP(arg)) ) {
    elts = &arg; n_elts = 1;}
  else return kno_err("InvalidXRefValue","kno_getxrefs",NULL,arg);

  if ( (elts) && (n_elts) ) {
    lispval *copy = u8_alloc_n(n_elts,lispval);
    int i = 0; while (i < n_elts) {
      lispval elt = elts[i];
      if ( (OIDP(elt)) || (SYMBOLP(elt)) ) {
	copy[i] = elt;
	kno_incref(elt);
	i++;}
      else {
	kno_seterr("InvalidXRef","kno_getxrefs",NULL,elt);
	u8_free(copy);
	if (free_arg) kno_decref(arg);
	return KNO_ERROR;}}
    if (free_arg) kno_decref(arg);
    struct XTYPE_REFS *refs = u8_alloc(struct XTYPE_REFS);
    int init_rv =
      kno_init_xrefs(refs,XTYPE_REFS_READ_ONLY,-1,n_elts,n_elts,-1,
		     copy,NULL);
    if (init_rv<0) {
      u8_free(copy);
      u8_free(refs);
      kno_seterr("XRefInitFailed","kno_getxrefs",NULL,VOID);
      return KNO_ERROR;}
    else return kno_wrap_xrefs(refs);}
  if (free_arg) kno_decref(arg);
  return KNO_FALSE;
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

  xrefs_symbol = kno_intern("xrefs");

  kno_xtrefs_typetag = kno_intern("%xtype-refs");

  int i = 0; while (i < KNO_TYPE_MAX) {
    kno_xtype_writers[i++]=NULL;};

}
