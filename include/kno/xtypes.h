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

/* DTYPE constants */

typedef enum xt_type_code {
  /* One-byte codes (format: <code> ) */
  xt_invalid = 0x00,
  xt_true = 0x01,
  xt_false = 0x02,
  xt_empty_choice = 0x03,
  xt_empty_list = 0x04,
  xt_default = 0x05,
  xt_void = 0x06,
  /* Scalars */
  xt_pos_int = 0x20, /* + <varint> */
  xt_neg_int = 0x21, /* + <varint> */
  xt_pos_big = 0x22, /* + <varint(len)> <byte>+ */
  xt_neg_big = 0x23, /* + <varint(len)> <byte>+ */
  xt_float = 0x24, /* + 4bytes */
  xt_double = 0x25, /* + 8bytes */
  xt_character = 0x26, /* + <varint> */
  xt_oid = 0x27, /* 8 bytes */
  xt_objid = 0x28, /* 12 bytes */
  xt_uuid = 0x29, /* 16 bytes */
  /* Pair types (format: <code> <xtype> <xtype>) */
  xt_pair = 0x30,
  xt_rational = 0x31,
  xt_complex = 0x32,
  xt_tagged = 0x33,
  xt_compressed = 0x34,
  xt_encrypted = 0x35,
  xt_mime = 0x36,
  /* Packet types (format: <code> <len> <byte>*) */
  xt_utf8 = 0x40,
  xt_packet = 0x41,
  xt_secret = 0x42,
  xt_symbol = 0x42,
  xt_tagged_packet = 0x43,
  /* Repeated types (format: <code> <len> <xtype>*) */
  xt_vector = 0x48,
  xt_choice = 0x49,
  xt_table = 0x4a,
  xt_tagged_vector = 0x4b,
  /* Type codes, used in combination with xt_tagged and
     xt_vector/xt_packet types */
  xt_type_short16_vec = 0x50,
  xt_type_int32_vec = 0x51,
  xt_type_long64_vec = 0x52,
  xt_type_float_vec = 0x53,
  xt_type_double_vec = 0x54,
  xt_type_hashset = 0x55,
  xt_type_hashtable = 0x56,
  /* Refs */
  xt_ref = 0x60, /* + <varint> */
  xt_pool_ref = 0x61 /* + <varint> <xtype(base)> */
} xt_type_code;

typedef struct XTYPE_REFS {
  size_t xt_nrefs;
  size_t xt_refslen;
  lispval *xt_refs;
  struct KNO_HASHTABLE xt_lookup;} XTYPE_REFS;
typedef struct XTYPE_REFS *xtype_refs;

/* XTYPE flags */

#define XTYPE_FLAGS_BASE      (KNO_DTYPEIO_FLAGS << 1)
#define XTYPE_WRITE_OPAQUE    (XTYPE_FLAGS_BASE)
#define XTYPE_NATSORT_VALUES  (XTYPE_FLAGS_BASE << 1)
#define XTYPE_USE_XTREFS      (XTYPE_FLAGS_BASE << 2)
#define XTYPE_ADD_XTREFS      (XTYPE_FLAGS_BASE << 3)

/* The top level functions */

KNO_EXPORT ssize_t kno_write_xtype(kno_outbuf out,lispval x,xtype_refs refs);
KNO_EXPORT ssize_t kno_validate_xtype(struct KNO_INBUF *in);
KNO_EXPORT lispval kno_read_xtype(kno_inbuf in,xtype_refs refs);

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

/* Compounds */

typedef struct KNO_COMPOUND_CONSTRUCTOR {
  lispval (*make)(lispval tag,lispval data);
  struct KNO_COMPOUND_CONSTRUCTOR *next;} KNO_COMPOUND_CONSTRUCTOR;
typedef struct KNO_COMPOUND_CONSTRUCTOR *kno_compound_constructor;

#endif /* KNO_XTYPES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
