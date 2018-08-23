/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DTYPEIO_H
#define FRAMERD_DTYPEIO_H 1
#ifndef FRAMERD_DTYPEIO_H_INFO
#define FRAMERD_DTYPEIO_H_INFO "include/framerd/dtypeio.h"
#endif

#include "bufio.h"

FD_EXPORT unsigned int fd_check_dtsize;

#define FD_DTYPEIO_FLAGS     (FD_BUFIO_MAX_FLAG)
#define FD_WRITE_OPAQUE      (FD_DTYPEIO_FLAGS << 0)
#define FD_NATSORT_VALUES    (FD_DTYPEIO_FLAGS << 1)
#define FD_USE_DTYPEV2       (FD_DTYPEIO_FLAGS << 2)

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

  dt_character_package = 0x40,
  dt_numeric_package = 0x41,
  dt_framerd_package = 0x42,

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
enum dt_framerd_subtypes
  { dt_choice = 0xC0, dt_small_choice = 0x80,
    dt_slotmap = 0xC1, dt_small_slotmap = 0x81,
    dt_hashtable = 0xC2, dt_small_hashtable = 0x82,
    dt_qchoice = 0xC3, dt_small_qchoice = 0x83,
    dt_hashset = 0xC4, dt_small_hashset = 0x84};

FD_EXPORT int fd_use_dtblock;

/* Arithmetic stubs */

FD_EXPORT lispval(*_fd_make_rational)(lispval car,lispval cdr);
FD_EXPORT void(*_fd_unpack_rational)(lispval,lispval *,lispval *);
FD_EXPORT lispval(*_fd_make_complex)(lispval car,lispval cdr);
FD_EXPORT void(*_fd_unpack_complex)(lispval,lispval *,lispval *);
FD_EXPORT lispval(*_fd_make_double)(double);

/* Packing and unpacking */

typedef lispval(*fd_packet_unpacker)(ssize_t len,unsigned char *packet);
typedef lispval(*fd_vector_unpacker)(ssize_t len,lispval *vector);

FD_EXPORT int fd_register_vector_unpacker
  (unsigned int code,unsigned int subcode,fd_vector_unpacker f);

FD_EXPORT int fd_register_packet_unpacker
  (unsigned int code,unsigned int subcode,fd_packet_unpacker f);


/* The top level functions */

FD_EXPORT ssize_t fd_write_dtype(struct FD_OUTBUF *out,lispval x);
FD_EXPORT ssize_t fd_validate_dtype(struct FD_INBUF *in);
FD_EXPORT lispval fd_read_dtype(struct FD_INBUF *in);

/* Returning error codes */

#if FD_DEBUG_DTYPEIO
#define fd_return_errcode(x) ((lispval)(_fd_return_errcode(x)))
#else
#define fd_return_errcode(x) ((lispval)(x))
#endif

typedef struct FD_COMPOUND_CONSTRUCTOR {
  lispval (*make)(lispval tag,lispval data);
  struct FD_COMPOUND_CONSTRUCTOR *next;} FD_COMPOUND_CONSTRUCTOR;
typedef struct FD_COMPOUND_CONSTRUCTOR *fd_compound_constructor;

#endif /* FRAMERD_DTYPEIO_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
