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

KNO_EXPORT ssize_t kno_add_xtype_ref(lispval x,xtype_refs refs)
{
  if ( (KNO_OIDP(x)) || (KNO_SYMBOLP(x)) ) {
    lispval v = fd_hashtable_get(&(refs->xt_lookup),x,KNO_VOID);
    if (KNO_FIXNUMP(v))
      return KNO_FIX2INT(v);
    if ( (xt->xt_refs_info) && (XTYPE_REFS_READ_ONLY) )
      return -1;
    size_t ref = refs->xt_n_refs++;
    if (ref >= xt->xt_refs_len) {
      int delta = xt->xt_refs_len;
      if (delta > XTYPE_REFS_DELTA_MAX) delta=XTYPE_REFS_DELTA_MAX;
      ssize_t new_size = xt->xt_refs_len+delta;
      lispval *refs = xt->xt_refs, *new_refs = 
	(new_size>XTYPE_REFS_MAX) ? (NULL) :
	(u8_realloc(refs,sizeof(lispval)*new_size));
      if (refs == NULL) {
	xt->xt_refs_info |= XTYPE_REFS_READ_ONLY;
	return -1;}
      else {
	xt->xt_refs = new_refs;
	xt->xt_refs_len = new_size;}}
    xt->xt_refs[ref]=x;
    return ref;}
  else return -1;
}
  

static ssize_t write_xtype(kno_outbuf out,lispval x,xtype_refs refs)
{
  unsigned int flags = out->buf_flags;
  if (OIDP(x)) {
    if ( (flags) & (XTYPE_USE_XTREFS) ) {
      lispval base = KNO_OID_TYPE | (x&_KNO_OID_BUCKET_MASK);
      ssize_t xtref = kno_xtype_ref(base,refs,(flags&XTYPE_ADD_SYMREFS));
      if (xtref >= 0) {
	if (kno_write_byte(out,xt_offref) > 0) {
	  int offset = KNO_OID_BASE_OFFSET(x);
	  ssize_t base_len = kno_write_varint(out,xtref);
	  ssize_t off_len = kno_write_varint(out,offset);
	  if (off_len > 0) return 1+base_len+off_len;}}}
    OID addr = KNO_OID_ADDR(x);
    kno_write_byte(out,xt_oid);
    kno_write_4bytes(out,KNO_OID_HI(addr));
    kno_write_4bytes(out,KNO_OID_LO(addr));
    return 9;}
  else if (SYMBOLP(x)) {
    if ( (flags) & (XTYPE_USE_XTREFS) ) {
      ssize_t xtref = kno_xtype_ref(x,refs,(flags&XTYPE_ADD_SYMREFS));
      if (xtref >= 0) {
	kno_write_byte(out,xt_absref);
	return 1+kno_write_varint(out,xtref);}}
    u8_string pname = KNO_SYMBOL_NAME(x);
    ssize_t len = strlen(pname);
    kno_write_byte(out,xt_symbol);
    if (KNO_EXPECT_FALSE(rv<0)) return -1;
    rv = kno_write_varint(out,len);
    if (KNO_EXPECT_FALSE(rv<0)) return -1;
    return kno_write_bytes(out,pname,len);}
  else if (KNO_IMMEDIATEP(x)) {
    int rv = -1;
    switch (x) {
    case KNO_FALSE:
      kno_write_byte(out,xt_false); break;
    case KNO_TRUE:
      kno_write_byte(out,xt_true); break;
    case KNO_EMPTY_CHOICE:
      kno_write_byte(out,xt_empty_choice); break;
    case KNO_EMPTY_LIST:
      kno_write_byte(out,xt_empty_list); break;
    case KNO_DEFAULT:
      kno_write_byte(out,xt_default); break;
    case KNO_VOID:
      kno_write_byte(out,xt_void); break;
    default:
      if (flags&XTYPE_WRITE_OPAQUE)
	return write_opaque(out,x);
      else {
	kno_seterr("XType/BadImmediate","xt_write_dtype",NULL,x);
	return -1;}}
    return 1;}
  else if (KNO_FIXNUMP(x)) {
    ssize_t intval = KNO_FIX2INT(x);
    if (intval<0) {
      kno_write_byte(out,xt_negint);
      return 1+kno_write_varint(out,(-intval));}
    kno_write_byte(out,xt_posint);
    return 1+kno_write_varint(out,intval);}
  else NO_ELSE;
  kno_ptr_type ctype = KNO_CONS_TYPE(x);
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
    case kno_packet_type: 
      xt_code=xt_packet; break;}
    kno_write_byte(out,xt_code);
    return 1+kno_write_varint(out,len)+
      kno_write_bytes(out,s->str_bytes,len);}
  case kno_choice_type: case kno_vector_type: {
    int xt_code; ssize_t n_elts; lispval *elts;
    if (ctype == kno_choice_type) {
      xt_code=xt_choice;
      n_elts = KNO_CHOICE_SIZE(x);
      elts = KNO_CHOICE_DATA(x);}
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
      if (rv>0) written+=rv;
      i++;}
    return n_written;}
  case kno_pair_type: case kno_rational_type: case kno_complex_type: {
    struct KNO_PAIR *pair = (kno_pair) x;
    kno_write_byte(out,xt_pair);
    ssize_t car_bytes = write_xtype(out,x->car,refs);
    if (car_bytes<0) return car_bytes;
    /* Check for output buffer overflow */
    ssize_t cdr_bytes = write_xtype(out,x->cdr,refs);
    if (cdr_bytes<0) return cdr_bytes;
    /* Check for output buffer overflow */
    return 1+car_bytes+cdr_bytes;}
  default:
    if (flags&XTYPE_WRITE_OPAQUE)
      return write_opaque(out,x);
    else {
      kno_seterr("CantWriteXType","write_xtype",NULL,x);
      return -1;}}
}

KNO_EXPORT ssize_t kno_write_xtype(kno_outbuf out,lispval x,xtype_refs refs)
{
  return write_xtype(out,x,refs);
}


/* File initialization */

KNO_EXPORT void kno_init_xtwrite_c()
{
  u8_register_source_file(_FILEINFO);
}
