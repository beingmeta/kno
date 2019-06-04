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

KNO_EXPORT unsigned int kno_check_dtsize;

#define KNO_XTYPES_FLAGS      (KNO_BUFIO_MAX_FLAG)
#define KNO_IS_XTYPE          (KNO_XTYPES_FLAGS << 0)
#define KNO_WRITE_OPAQUE      (KNO_XTYPES_FLAGS << 1)
#define KNO_NATSORT_VALUES    (KNO_XTYPES_FLAGS << 2)

/* DTYPE constants */

typedef enum xt_type_code {
  /* One-byte codes */
  xt_invalid = 0x00,
  xt_true = 0x01,
  xt_false = 0x02,
  xt_empty_choice = 0x03,
  xt_empty_list = 0x04,
  xt_default = 0x05,
  xt_void = 0x06,
  xt_qempty = 0x07,
  /* Scalars */
  xt_fixnum_b = 0x10,
  xt_fixnum_bb = 0x11,
  xt_fixnum_bbb = 0x12,
  xt_fixnum_bbbb = 0x13,
  xt_posint_v = 0x14,
  xt_negint_v = 0x15,
  xt_flonum_bbbb = 0x16,
  xt_flonum_bbbbbbbb = 0x17,
  xt_character_b = 0x18,
  xt_character_bb = 0x19,
  xt_character_bbbb = 0x1a,
  xt_character_v = 0x1b,
  /* Refs */
  xt_ref_b = 0x20,
  xt_ref_bb = 0x21,
  xt_ref_bbb = 0x22,
  xt_ref_v = 0x23,
  /* String types */
  xt_utf8_b = 0x30,
  xt_utf8_bb = 0x3,
  xt_utf8_bbbb = 0x32,
  xt_utf8_v = 0x33,
  xt_packet_b = 0x34,
  xt_packet_bb = 0x35,
  xt_packet_bbbb = 0x36,
  xt_packet_v = 0x37,
  xt_secret_b = 0x38,
  xt_secret_bb = 0x39,
  xt_secret_bbbb = 0x3a,
  xt_secret_v = 0x3b,
  xt_u8symbol_b = 0x38,
  xt_u8symbol_bb = 0x39,
  xt_u8symbol_bbbb = 0x3a,
  xt_u8symbol_v = 0x3b,
  /* Pair types */
  xt_pair = 0x40,
  xt_rational = 0x41,
  xt_complex = 0x42,
  xt_tagged = 0x43,
  xt_compressed = 0x44,
  xt_encrypted = 0x45,
  xt_media = 0x46,
  /* Repeated types */
  xt_vector_b = 0x50,
  xt_vector_bb = 0x51,
  xt_vector_bbb = 0x52,
  xt_vector_v = 0x53,
  xt_choice_b = 0x54,
  xt_choice_bb = 0x55,
  xt_choice_bbb = 0x56,
  xt_choice_v = 0x57,
  xt_table_b = 0x58,
  xt_table_bb = 0x59,
  xt_table_bbb = 0x5a,
  xt_table_v = 0x5b,
  /* OID refs */
  xt_oid = 0x60, /* 8 bytes */
  xt_poolref_b = 0x64, /* has form off (bytes) + base (xtype) */
  xt_poolref_bb = 0x65, /* ... */
  xt_poolref_bbb = 0x66, /* ... */
  xt_poolrev_bbbb = 0x67, /* ... */
  /* Type codes */
  xt_type_short16_vec = 0x70,
  xt_type_int32_vec = 0x71,
  xt_type_long64_vec = 0x72,
  xt_type_float_vec = 0x73,
  xt_type_double_vec = 0x74,
  xt_type_hashset = 0x75,
  xt_type_hashtable = 0x76
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

KNO_EXPORT ssize_t kno_write_xtype
(struct KNO_OUTBUF *out,lispval x,struct XTYPE_REFS *);
KNO_EXPORT ssize_t kno_validate_xtype
(struct KNO_INBUF *in);
KNO_EXPORT lispval kno_read_xtype
(struct KNO_INBUF *in,lispval *refs,struct XTYPE_REFS *);

/* Returning error codes */

#if KNO_DEBUG_XTYPES
#define kno_return_errcode(x) ((lispval)(_kno_return_errcode(x)))
#else
#define kno_return_errcode(x) ((lispval)(x))
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
