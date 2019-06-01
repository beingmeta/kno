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

#define KNO_XTYPES_FLAGS     (KNO_BUFIO_MAX_FLAG)
#define KNO_WRITE_OPAQUE      (KNO_XTYPES_FLAGS << 0)
#define KNO_NATSORT_VALUES    (KNO_XTYPES_FLAGS << 1)

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
  /* String types */
  xt_utf8_b = 0x10,
  xt_utf8_bb = 0x1,
  xt_utf8_bbbb = 0x12,
  xt_utf8_bbbbbbbb = 0x13,
  xt_packet_b = 0x14,
  xt_packet_bb = 0x15,
  xt_packet_bbbb = 0x16,
  xt_packet_bbbbbbbb = 0x17,
  xt_secret_b = 0x18,
  xt_secret_bb = 0x19,
  xt_secret_bbbb = 0x1a,
  xt_secret_bbbbbbbb = 0x1b,
  xt_u8symbol_b = 0x18,
  xt_u8symbol_bb = 0x19,
  xt_u8symbol_bbbb = 0x1a,
  xt_u8symbol_bbbbbbbb = 0x1b,
  /* Scalars */
  xt_fixnum_b = 0x20,
  xt_fixnum_bb = 0x21,
  xt_fixnum_bbbb = 0x22,
  xt_fixnum_bbbbbbbb = 0x23,
  xt_bigint_b = 0x24,
  xt_bigint_bb = 0x25,
  xt_bigint_bbbb = 0x26,
  xt_bigint_bbbbbbbb = 0x27,
  xt_flonum_bbbb = 0x28,
  xt_flonum_bbbbbbbb = 0x29,
  xt_character_b = 0x2c,
  xt_character_bb = 0x2d,
  xt_character_bbbb = 0x2e,
  xt_character_bbbbbbbb = 0x2f,
  /* Pair types */
  xt_pair = 0x30,
  xt_rational = 0x31,
  xt_complex = 0x32,
  xt_coord = 0x33,
  xt_tagged = 0x34,
  xt_compressed = 0x35,
  xt_encrypted = 0x36,
  /* Repeated types */
  xt_vector_b = 0x40,
  xt_vector_bb = 0x41,
  xt_vector_bbbb = 0x42,
  xt_vector_bbbbbbbb = 0x43,
  xt_choice_b = 0x40,
  xt_choice_bb = 0x41,
  xt_choice_bbbb = 0x42,
  xt_choice_bbbbbbbb = 0x43,
  /* xt_tables */
  xt_table_b = 0x50,
  xt_table_bb = 0x51,
  xt_table_bbbb = 0x52,
  xt_table_bbbbbbbb = 0x53,
  /* OIDs */
  xt_oid = 0x60, /* 8 bytes */
  xt_oid_x_bb = 0x61, /* */
  xt_oid_x_bbbb = 0x62, /* */
  xt_oid_x_ bbbbbbbb = 0x63, /* */
  /* Refs */
  xt_ref_b = 0x60,
  xt_ref_bb = 0x61,
  xt_ref_bbb = 0x62,
  xt_ref_bbbb = 0x63,
  xt_ref_bbbbb = 0x64,
  xt_ref_bbbbbb = 0x65,
  xt_ref_bbbbbbb = 0x66,
  xt_ref_bbbbbbbb = 0x67,
  /* Type codes */
  xt_type_short16_vec = 0x70,
  xt_type_int32_vec = 0x71,
  xt_type_long64_vec = 0x72,
  xt_type_float_vec = 0x73,
  xt_type_double_vec = 0x74,
  xt_type_hashset = 0x75,
  xt_type_hashtable = 0x76
} dt_type_code;

/* The top level functions */

KNO_EXPORT ssize_t kno_write_xtype(struct KNO_OUTBUF *out,lispval x);
KNO_EXPORT ssize_t kno_validate_xtype(struct KNO_INBUF *in);
KNO_EXPORT lispval kno_read_xtype(struct KNO_INBUF *in,lispval *refs);

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
