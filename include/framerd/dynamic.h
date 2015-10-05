/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/


#ifndef FRAMERD_DYNAMIC_H
#define FRAMERD_DYNAMIC_H
#ifndef FRAMERD_DYNAMIC_H_INFO
#define FRAMERD_DYNAMIC_H_INFO "include/framerd/dynamic.h"
#endif

#include <setjmp.h>

static const int in_dynamic_context=0;
static const int in_dynamic_exit=0;

#define fd_return(x) \
  if (in_dynamic_context) fd_throw(x,_fd_return_marker,NULL); \
  else return x
#define fd_break(x) \
  if (in_dynamic_context) fd_throw(x,_dc_tag,NULL); \
  else break;

struct FD_DYNAMIC_CONTEXT {
  u8_string tag; jmp_buf jb;
  fd_exception ex; u8_string report; fdtype irritant;
  struct FD_DYNAMIC_CONTEXT *outer;};

#define FD_START_DYNAMIC_CONTEXT    \
  {struct FD_DYNAMIC_CONTEXT _dc;   \
   const int _in_dynamic_context=1; \
   const char _dc_tag="dctag";      \
   _dc.tag=_dc_tag; _dc.ex=NULL; _dc.report=NULL; _dc.irritant=FD_VOID; \
   if (setjmp(&_dc.jb) == 0) { \
     fd_push_dynamic_context(&_dc);

#define FD_EXIT_DYNAMIC_CONTEXT      \
     fd_pop_dynamic_context(&_dc);}  \
   else {                            \
     const int _in_dynamic_exit=1;
#define FD_END_DYNAMIC_CONTEXT                    \
     if ((_dc.ex == NULL) || (_dc.ex == _dc_tag)) \
       fd_pop_dynamic_context(&_dc);              \
     else fd_reraise(&_dc);}}

#endif /* #ifndef FRAMERD_DYNAMIC_H */

