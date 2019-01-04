/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_PORTS_H
#define FRAMERD_PORTS_H 1
#ifndef FRAMERD_PORTS_H_INFO
#define FRAMERD_PORTS_H_INFO "include/framerd/ports.h"
#endif

#include <libu8/libu8io.h>

#define FD_PORTP(x) (FD_TYPEP((x),fd_port_type))

typedef struct FD_PORT {
  FD_CONS_HEADER;
  u8_string port_id;
  u8_input port_input;
  u8_output port_output;
  lispval port_lisprefs;} FD_PORT;
typedef struct FD_PORT *fd_port;

FD_EXPORT u8_condition fd_UnknownEncoding;

FD_EXPORT void fd_print_backtrace(U8_OUTPUT *out,u8_exception ex,int width);
FD_EXPORT void fd_log_backtrace(u8_exception ex,int loglevel,
                                u8_condition label,int width);
FD_EXPORT void fd_summarize_backtrace(U8_OUTPUT *out,u8_exception ex);

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
