/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2005-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_PORTS_H
#define KNO_PORTS_H 1
#ifndef KNO_PORTS_H_INFO
#define KNO_PORTS_H_INFO "include/kno/ports.h"
#endif

#include <libu8/libu8io.h>

#define KNO_PORTP(x) (KNO_TYPEP((x),kno_port_type))

typedef struct KNO_PORT {
  KNO_CONS_HEADER;
  u8_string port_id;
  u8_input port_input;
  u8_output port_output;
  lispval port_lisprefs;} KNO_PORT;
typedef struct KNO_PORT *kno_port;

KNO_EXPORT u8_condition kno_UnknownEncoding;

KNO_EXPORT void kno_print_backtrace(U8_OUTPUT *out,u8_exception ex,int width);
KNO_EXPORT void kno_log_backtrace(u8_exception ex,int loglevel,
                                u8_condition label,int width);
KNO_EXPORT void kno_summarize_backtrace(U8_OUTPUT *out,u8_exception ex);

KNO_EXPORT lispval kno_make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id);

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
