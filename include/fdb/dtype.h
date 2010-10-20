/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2010 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

/* Dealing with DTYPE (dynamically typed) objects */

#ifndef FDB_DTYPE_H
#define FDB_DTYPE_H 1
#define FDB_DTYPE_H_VERSION "$Id$"

#include "common.h"
#include "ptr.h"
#include "cons.h"

FD_EXPORT fd_exception fd_NoMethod;

FD_EXPORT double fd_load_start;

FD_EXPORT int fd_init_dtypelib(void) FD_LIBINIT_FN;

FD_EXPORT fdtype fd_parse_expr(struct U8_INPUT *);
FD_EXPORT fdtype fd_parser(struct U8_INPUT *);
FD_EXPORT fdtype fd_parse(u8_string string);
FD_EXPORT fdtype fd_parse_arg(u8_string string);
FD_EXPORT u8_string fd_unparse_arg(fdtype obj);
FD_EXPORT int fd_skip_whitespace(u8_input s);
FD_EXPORT int fd_read_escape(u8_input in);

FD_EXPORT int fd_unparse(u8_output,fdtype);
FD_EXPORT u8_string fd_dtype2string(fdtype x);

FD_EXPORT fdtype fd_string2number(u8_string string,int base);
FD_EXPORT int fd_output_number(u8_output output,fdtype num,int base);

FD_EXPORT int fd_unparse_maxchars, fd_unparse_maxelts;

FD_EXPORT int fd_register_record_tag
  (fdtype symbol,fdtype (*recreate)(int n,fdtype *v));

FD_EXPORT int fd_parse_pointers;
FD_EXPORT fdtype (*_fd_parse_number)(u8_string,int);

FD_EXPORT int (*fd_unparse_error)(U8_OUTPUT *,fdtype x,u8_string details);
FD_EXPORT int (*fd_dtype_error)(struct FD_BYTE_OUTPUT *,fdtype x,u8_string details);
FD_EXPORT void fd_set_oid_parser(fdtype (*parsefn)(u8_string start,int len));
typedef int (*fd_hashfn)(fdtype x,unsigned int (*hf)(fdtype));
FD_EXPORT fdtype fd_parse_oid_addr(u8_string string,int len);
FD_EXPORT fd_hashfn fd_hashfns[];

#endif /* ndef FDB_DTYPE_H */

