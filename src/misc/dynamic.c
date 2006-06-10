/* -*- Mode: C; -*- */

/* Copyright (C) 2004, 2005 beingmeta, inc.
   This file is part of EFramerD and is copyright and a valuable
   trade secret of beingmeta, inc.
*/

static char versionid[] = "$Id$";

#include "stdio.h"
#include "libu8/u8.h"
#include "libu8/printf.h"
#include "eframerd/lisp.h"
#include "eframerd/dynamic.h"

fd_exception
  fd_NoDynamicContext="Attempting to pop a non-existent dynamic context",
  fd_UnhandledException=_("No handlers for exception");


void empty_context()
{
  fd_warn_fn(fd_NoDynamicContext,NULL);
  exit(1);
}

void unhandled_exception(fd_exception ex,u8_string details,fd_lisp irritant)
{
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT(&out,256);
  if (details)
    u8_printf(&out,_("Unhandled exception %m (%s): %q"),ex,details,irritant);
  else u8_printf(&out,_("Unhandled exception %m: %q"),ex,irritant);  
  fd_warn_fn(fd_UnhandledException,out.bytes); u8_free(out.bytes);
  exit(1);
}

#if FD_THREADS_ENABLED
fd_threadkey fd_dc_key;
FD_EXPORT struct FD_DYNAMIC_CONTEXT *fd_get_dynamic_context()
{
  return fd_threadkey_get(fd_dc_key);
}
FD_EXPORT void fd_push_dynamic_context(struct FD_DYNAMIC_CONTEXT *newdc)
{
  struct FD_DYNAMIC_CONTEXT *dc=fd_threadkey_get(fd_dc_key);
  newdc->outer=dc;
  fd_threadkey_set(fd_dc_key,newdc);
}
FD_EXPORT void fd_pop_dynamic_context(struct FD_DYNAMIC_CONTEXT *newdc)
{
  struct FD_DYNAMIC_CONTEXT *dc=fd_threadkey_get(fd_dc_key);
  if (dc) empty_context();
  else {
    newdc=dc->outer; fd_threadkey_set(fd_dc_key,newdc);}
}
#else
struct FD_DYNAMIC_CONTEXT *fd_dynamic_context=NULL;
FD_EXPORT struct FD_DYNAMIC_CONTEXT *fd_get_dynamic_context()
{
  return fd_dynamic_context;
}
FD_EXPORT void fd_push_dynamic_context(struct FD_DYNAMIC_CONTEXT *newdc)
{
  newdc->outer=fd_dynamic_context;
  fd_dynamic_context=newdc;
}
FD_EXPORT void fd_pop_dynamic_context(struct FD_DYNAMIC_CONTEXT *newdc)
{
  if (fd_dynamic_context == NULL) empty_context();
  else fd_dynamic_context=fd_dynamic_context->outer;
}
#endif

FD_EXPORT void fd_throw(fd_lisp irritant,fd_exception ex,u8_string details)
{
  struct FD_DYNAMIC_CONTEXT *dc=fd_get_dynamic_context();
  if ((dc == NULL) || (dc->ex))
    unhandled_exception(ex,details,fd_incref(irritant));
  else {
    dc->ex=ex; dc->report=details; dc->irritant=fd_incref(irritant);;}
}

FD_EXPORT void fd_raise(fd_exception ex,u8_string details)
{
  struct FD_DYNAMIC_CONTEXT *dc=fd_get_dynamic_context();
  if ((dc == NULL) || (dc->ex))
    unhandled_exception(ex,details,FD_VOID);
  else {
    dc->ex=ex; dc->report=details; dc->irritant=FD_VOID;}
}

FD_EXPORT void fd_reraise(struct FD_DYNAMIC_CONTEXT *dc)
{
  if (dc->outer == NULL)
    unhandled_exception(dc->ex,dc->report,dc->irritant);
  else {
    dc->outer->ex=dc->ex; dc->outer->report=dc->report;
    dc->outer->irritant=dc->irritant;
    longjmp(dc->outer->jb,1);}
}

FD_EXPORT int fd_init_errbase()
{
  /* errbase version numbers are primes in the range 200-300
     minor versions are indicated by powers of the major version. */
#if FD_THREADS_ENABLED
  fd_new_threadkey(&fd_dc_key,NULL);
#endif
  return 223;
}


