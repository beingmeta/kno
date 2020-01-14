/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_XTYPES_H
#define KNO_XTYPES_H 1
#ifndef KNO_XTYPES_H_INFO
#define KNO_XTYPES_H_INFO "include/kno/xtypes.h"
#endif

#include "bufio.h"

#ifndef KNO_INLINE_XTYPE_REFS
#define KNO_INLINE_XTYPE_REFS 0
#endif

/* DTYPE constants */

#define XT_SCALAR(n)   ((n)+0x80)
#define XT_FIXVAL(n)   ((n)+0x88)
#define XT_STRING(n)   ((n)+0x90)
#define XT_PAIR(n)     ((n)+0x98)
#define XT_VECTOR(n)   ((n)+0xA0)
#define XT_XREF(n)     ((n)+0xA8)
#define XT_TYPE(n)     ((n)+0xB0)

typedef enum XT_TYPE_CODE {
  /* One-byte codes (format: <code> ) */
  xt_invalid      = XT_SCALAR(0x00),
  xt_true         = XT_SCALAR(0x01),
  xt_false        = XT_SCALAR(0x02),
  xt_empty_choice = XT_SCALAR(0x03),
  xt_empty_list   = XT_SCALAR(0x04),
  xt_default      = XT_SCALAR(0x05),
  xt_void         = XT_SCALAR(0x06),

  xt_double       = XT_FIXVAL(0x00),
  xt_oid          = XT_FIXVAL(0x01),
  xt_objid        = XT_FIXVAL(0x02),
  xt_uuid         = XT_FIXVAL(0x03),
  xt_posint       = XT_FIXVAL(0x04),
  xt_negint       = XT_FIXVAL(0x05),
  xt_character    = XT_FIXVAL(0x06),

  xt_utf8         = XT_STRING(0x00),
  xt_packet       = XT_STRING(0x01),
  xt_secret       = XT_STRING(0x02),
  xt_symbol       = XT_STRING(0x03),
  xt_posbig       = XT_STRING(0x04),
  xt_negbig       = XT_STRING(0x05),
  xt_block        = XT_STRING(0x06),

  xt_pair         = XT_PAIR(0x00),
  xt_tagged       = XT_PAIR(0x01),
  xt_mimeobj      = XT_PAIR(0x02),
  xt_compressed   = XT_PAIR(0x03),
  xt_encrypted    = XT_PAIR(0x04),

  xt_choice       = XT_VECTOR(0x00),
  xt_vector       = XT_VECTOR(0x01),
  xt_table        = XT_VECTOR(0x02),

  xt_absref       = XT_XREF(0x00),
  xt_offref       = XT_XREF(0x01),
  xt_absdef       = XT_XREF(0x02),

  xt_rational     = XT_TYPE(0x00),
  xt_complex      = XT_TYPE(0x01),
  xt_timestamp    = XT_TYPE(0x02)

} xt_type_code;

typedef struct XTYPE_REFS {
  int xt_refs_flags;
  size_t xt_n_refs;
  size_t xt_refs_len;
  lispval *xt_refs;
  struct KNO_HASHTABLE *xt_lookup;} XTYPE_REFS;
typedef struct XTYPE_REFS *xtype_refs;

#define XTYPE_REFS_DELTA_MAX 4096
#define XTYPE_REFS_MAX 16384

#define XTYPE_REFS_READ_ONLY 1
#define XTYPE_REFS_ADD_OIDS  2
#define XTYPE_REFS_ADD_SYMS  4
#define XTYPE_REFS_CHANGED   8

typedef ssize_t (*kno_xtype_fn)(struct KNO_OUTBUF *,lispval,xtype_refs);
KNO_EXPORT kno_xtype_fn kno_xtype_writers[KNO_TYPE_MAX];

/* XTYPE flags */

#define XTYPE_FLAGS_BASE      (KNO_BUFIO_MAX_FLAG)
#define XTYPE_WRITE_OPAQUE    (XTYPE_FLAGS_BASE)
#define XTYPE_NATSORT_VALUES  (XTYPE_FLAGS_BASE << 1)
#define XTYPE_NO_XTREFS       (XTYPE_FLAGS_BASE << 2)

KNO_EXPORT ssize_t kno_add_xtype_ref(lispval x,xtype_refs refs);
KNO_EXPORT ssize_t _kno_xtype_ref(lispval x,xtype_refs refs,int add);

#if KNO_INLINE_XTYPE_REFS
static ssize_t kno_xtype_ref(lispval x,xtype_refs refs,int add)
{
  if ((KNO_OIDP(x)) || (KNO_SYMBOLP(x))) {
    if ( (add==0) && (refs->xt_n_refs<=0) )
      return -1;
    lispval v = kno_hashtable_get(refs->xt_lookup,x,KNO_VOID);
    if (KNO_FIXNUMP(v))
      return KNO_FIX2INT(v);
    else if (KNO_VOIDP(v)) {
      if (add == 0) return -1;
      else if ( (add == 1) ||
		( (KNO_OIDP(x)) ?
		  ((refs->xt_refs_flags)&(XTYPE_REFS_ADD_OIDS)) :
		  ((refs->xt_refs_flags)&(XTYPE_REFS_ADD_SYMS)) ) ) {}
      else return -1;}
    else {
      u8_log(LOG_ERR,"BadXTypeRef","For %q=%q",x,v);
      kno_decref(v);
      if (!(add))
	return -1;}
    return kno_add_xtype_ref(x,refs);}
  else return -1;
}
#else
#define kno_xtype_ref _kno_xtype_ref
#endif

/* The top level functions */

KNO_EXPORT ssize_t kno_write_xtype(kno_outbuf out,lispval x,xtype_refs refs);
KNO_EXPORT ssize_t kno_validate_xtype(struct KNO_INBUF *in);
KNO_EXPORT lispval kno_read_xtype(kno_inbuf in,xtype_refs refs);

KNO_EXPORT int kno_init_xrefs(xtype_refs refs,
			      int n_refs,int refs_len,int flags,
			      lispval *elts,
			      kno_hashtable lookup);

/* Returning error codes */

#if KNO_DEBUG_XTYPES
#define xtype_return_errcode(x) ((lispval)(_kno_return_errcode(x)))
#else
#define xtype_return_errcode(x) ((lispval)(x))
#endif

/* Arithmetic stubs */

KNO_EXPORT lispval(*_kno_make_rational)(lispval car,lispval cdr);
KNO_EXPORT void(*_kno_unpack_rational)(lispval,lispval *,lispval *);
KNO_EXPORT lispval(*_kno_make_complex)(lispval car,lispval cdr);
KNO_EXPORT void(*_kno_unpack_complex)(lispval,lispval *,lispval *);
KNO_EXPORT lispval(*_kno_make_double)(double);

#endif /* KNO_XTYPES_H */

