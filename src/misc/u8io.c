/* -*- Mode: C; -*- */

/* Copyright (C) 2004, 2005 beingmeta, inc.
   This file is part of EFramerD and is copyright and a valuable
   trade secret of beingmeta, inc.
*/

static char versionid[] = "$Id: u8io.c,v 1.9 2005/02/15 03:03:40 haase Exp $";

#include <libu8/u8.h>
#include <libu8/u8io.h>
#include <libu8/u8text.h>
#include <libu8/printf.h>
#include <libu8/netfns.h>
#include <eframerd/lisp.h>

FD_EXPORT void fd_notify_fn(u8_condition s,u8_string details)
{
  if (details)
    u8_fprintf(stdout,"[%*t: %m: (%s)]\n",s,details);
  else u8_fprintf(stdout,"[%*t: %m]\n",s);
}

FD_EXPORT void fd_warn_fn(u8_condition s,u8_string details)
{
  if (details)
    u8_fprintf(stderr,"[%*t: %m: (%s)]\n",s,details);
  else u8_fprintf(stderr,"[%*t: %m]\n",s);
}

static int makeupper(int c)
{
  return u8_toupper(c);
}

/* U8_PRINTF extensions */

static u8_string lisp_printf_handler
  (u8_output s,char *cmd,u8_byte *buf,int bufsiz,va_list *args)
{
  fd_unparse(s,va_arg(*args,fd_lisp));
  return NULL;
}

static int better_unparse_exception(struct U8_OUTPUT *out,fd_lisp x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    FD_GET_CONS(x,fd_exception_type,struct FD_EXCEPTION_OBJECT *);
  if ((xo->details) && (!(FD_VOIDP(xo->irritant))))
    u8_printf(out,"#<!%s: %q %s>",xo->ex,xo->irritant,xo->details);
  else if (xo->details)
    u8_printf(out,"#<!%s: %s>",xo->ex,xo->details);
  else if (FD_VOIDP(xo->irritant))
    u8_printf(out,"#<!%s>",xo->ex);
  else u8_printf(out,"#<!%s: %q>",xo->ex,xo->irritant);
}

static int iobase_version=0;

FD_EXPORT int fd_init_iobase()
{
  /* iobase version numbers are primes in the range 100-200
     minor versions are indicated by powers of the major version. */
  int u8version;
  if (iobase_version) return iobase_version;
  u8version=u8_initialize();
  iobase_version=103*u8version;
  fd_unparsers[fd_exception_type]=better_unparse_exception;
  u8_printf_handlers['q']=lisp_printf_handler;
  fd_set_session_id(u8_sessionid());
  return iobase_version;
}

