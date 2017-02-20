/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_PORTS_H
#define FRAMERD_PORTS_H 1
#ifndef FRAMERD_PORTS_H_INFO
#define FRAMERD_PORTS_H_INFO "include/framerd/ports.h"
#endif

#include <libu8/libu8io.h>

FD_EXPORT fd_ptr_type fd_port_type;

#define FD_PORTP(x) (FD_TYPEP((x),fd_port_type))

typedef struct FD_PORT {
  FD_CONS_HEADER;
  u8_string fd_portid;
  u8_input fd_inport;
  u8_output fd_outport;} FD_PORT;
typedef struct FD_PORT *fd_port;

typedef struct FD_BYTEPORT {
  FD_CONS_HEADER;
  int fd_owns_socket;
  struct FD_BYTESTREAM *dt_stream;} FD_BYTEPORT;
typedef struct FD_BYTEPORT *fd_byteport;

FD_EXPORT fd_ptr_type fd_byteport_type;

FD_EXPORT fd_exception fd_UnknownEncoding;

FD_EXPORT fdtype fd_printout(fdtype,fd_lispenv);
FD_EXPORT fdtype fd_printout_to(U8_OUTPUT *,fdtype,fd_lispenv);

FD_EXPORT int fd_pprint
  (u8_output out,fdtype x,
   u8_string prefix,int indent,int col,int maxcol,int initial);
typedef int (*fd_pprintfn)(u8_output,fdtype,u8_string,int,int,int,int,void *);
FD_EXPORT int fd_xpprint
  (u8_output out,fdtype x,
   u8_string prefix,int indent,int col,int maxcol,int initial,
   fd_pprintfn fn,void *data);

FD_EXPORT void fd_print_backtrace(U8_OUTPUT *out,u8_exception ex,int width);
FD_EXPORT void fd_log_backtrace(u8_exception ex,int loglevel,
				u8_condition label,int width);
FD_EXPORT void fd_summarize_backtrace(U8_OUTPUT *out,u8_exception ex);

FD_EXPORT void fd_init_schemeio(void) FD_LIBINIT_FN;

#endif
