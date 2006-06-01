/* -*- Mode: C; -*- */

/* Copyright (C) 2004, 2005 beingmeta, inc.
   This file is part of EFramerD and is copyright and a valuable
   trade secret of beingmeta, inc.
*/

static char versionid[] = "$Id: stdio.c,v 1.6 2005/03/05 21:07:39 haase Exp $";

#include <eframerd/lisp.h>
#include <stdio.h>

FD_EXPORT void notify_fn(u8_condition c,u8_string details)
{
  if (details)
    fprintf(stderr,_("[Notice: %s (%s)]\n"),c,details);
  else fprintf(stderr,_("[Notice: %s]\n"),c);
}

FD_EXPORT void warn_fn(u8_condition c,u8_string details)
{
  if (details)
    fprintf(stderr,_("[Warning: %s (%s)]\n"),c,details);
  else fprintf(stderr,_("[Warning: %s]\n"),c);
}

FD_EXPORT void raise_fn(u8_condition c,u8_string details)
{
  if (details)
    fprintf(stderr,_("[Warning: %s (%s)]\n"),c,details);
  else fprintf(stderr,_("[Warning: %s]\n"),c);
  exit(1);
}

FD_EXPORT int fd_init_iobase()
{
  /* iobase version numbers are primes in the range 100-200
     minor versions are indicated by powers of the major version. */
  u8_set_notify_handler(notify_fn);
  u8_set_warning_handler(warn_fn);
  u8_set_exception_handler(raise_fn);
  return 101;
}
