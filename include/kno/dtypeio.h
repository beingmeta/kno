/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_DTYPEIO_H
#define KNO_DTYPEIO_H 1
#ifndef KNO_DTYPEIO_H_INFO
#define KNO_DTYPEIO_H_INFO "include/kno/dtypeio.h"
#endif

#include "bufio.h"

KNO_EXPORT unsigned int kno_check_dtsize;

#define KNO_DTYPEIO_FLAGS     (KNO_BUFIO_MAX_FLAG)
#define KNO_IS_NOT_DTYPEIO    (KNO_DTYPEIO_FLAGS << 0)
#define KNO_WRITE_OPAQUE      (KNO_DTYPEIO_FLAGS << 1)
#define KNO_NATSORT_VALUES    (KNO_DTYPEIO_FLAGS << 2)
#define KNO_USE_DTYPEV2       (KNO_DTYPEIO_FLAGS << 3)
#define KNO_FIX_DTSYMS        (KNO_DTYPEIO_FLAGS << 4)

/* DTYPE constants */

typedef enum dt_type_code {
  dt_invalid = 0x00,
  dt_empty_list = 0x01,
  dt_boolean = 0x02,
  dt_fixnum = 0x03,
  dt_flonum = 0x04,
  dt_packet = 0x05,
  dt_string = 0x06,
  dt_symbol = 0x07,
  dt_pair = 0x08,
  dt_vector = 0x09,
  dt_void = 0x0a,
  dt_compound = 0x0b,
  dt_error = 0x0c,
  dt_exception = 0x0d,
  dt_oid = 0x0e,
  dt_zstring = 0x0f,
  /* These are all for DTYPE protocol V2 */
  dt_tiny_symbol = 0x10,
  dt_tiny_string = 0x11,
  dt_tiny_choice = 0x12,
  dt_empty_choice = 0x13,
  dt_block = 0x14,
  dt_default_value = 0x15,

  dt_character_package = 0x40,
  dt_numeric_package = 0x41,
  dt_kno_package = 0x42,

  dt_ztype = 0x70

} dt_type_code;

typedef unsigned char dt_subcode;

enum dt_numeric_subcodes {
  dt_small_bigint = 0x00, dt_bigint = 0x40, dt_double = 0x01,
  dt_rational = 0x81, dt_complex = 0x82,
  dt_short_int_vector = 0x03, dt_int_vector = 0x43,
  dt_short_short_vector = 0x04, dt_short_vector = 0x44,
  dt_short_float_vector = 0x05, dt_float_vector = 0x45,
  dt_short_double_vector = 0x06, dt_double_vector = 0x46,
  dt_short_long_vector = 0x07, dt_long_vector = 0x47};
enum dt_character_subcodes
     { dt_ascii_char = 0x00, dt_unicode_char = 0x01,
       dt_unicode_string = 0x42, dt_unicode_short_string = 0x02,
       dt_unicode_symbol = 0x43, dt_unicode_short_symbol = 0x03,
       dt_unicode_zstring = 0x44, dt_unicode_short_zstring = 0x04,
       dt_secret_packet = 0x45, dt_short_secret_packet = 0x05};
enum dt_kno_subtypes
  { dt_choice = 0xC0, dt_small_choice = 0x80,
    dt_slotmap = 0xC1, dt_small_slotmap = 0x81,
    dt_hashtable = 0xC2, dt_small_hashtable = 0x82,
    dt_qchoice = 0xC3, dt_small_qchoice = 0x83,
    dt_hashset = 0xC4, dt_small_hashset = 0x84,
    dt_schemap = 0xC5, dt_small_schemap = 0x85,};

KNO_EXPORT int kno_use_dtblock;
KNO_EXPORT int kno_dtype_fixcase;


/* Arithmetic stubs */

KNO_EXPORT lispval(*_kno_make_rational)(lispval car,lispval cdr);
KNO_EXPORT void(*_kno_unpack_rational)(lispval,lispval *,lispval *);
KNO_EXPORT lispval(*_kno_make_complex)(lispval car,lispval cdr);
KNO_EXPORT void(*_kno_unpack_complex)(lispval,lispval *,lispval *);
KNO_EXPORT lispval(*_kno_make_double)(double);

/* Packing and unpacking */

typedef lispval(*kno_packet_unpacker)(ssize_t len,unsigned char *packet);
typedef lispval(*kno_vector_unpacker)(ssize_t len,lispval *vector);

KNO_EXPORT int kno_register_vector_unpacker
  (unsigned int code,unsigned int subcode,kno_vector_unpacker f);

KNO_EXPORT int kno_register_packet_unpacker
  (unsigned int code,unsigned int subcode,kno_packet_unpacker f);


/* The top level functions */

KNO_EXPORT ssize_t kno_write_dtype(struct KNO_OUTBUF *out,lispval x);
KNO_EXPORT ssize_t kno_validate_dtype(struct KNO_INBUF *in);
KNO_EXPORT lispval kno_read_dtype(struct KNO_INBUF *in);

/* Returning error codes */

#if KNO_DEBUG_DTYPEIO
#define kno_return_errcode(x) ((lispval)(_kno_return_errcode(x)))
#else
#define kno_return_errcode(x) ((lispval)(x))
#endif

typedef struct KNO_COMPOUND_CONSTRUCTOR {
  lispval (*make)(lispval tag,lispval data);
  struct KNO_COMPOUND_CONSTRUCTOR *next;} KNO_COMPOUND_CONSTRUCTOR;
typedef struct KNO_COMPOUND_CONSTRUCTOR *kno_compound_constructor;

#endif /* KNO_DTYPEIO_H */

