/* -*- Mode: C; -*- */

/* Copyright (C) 2004, 2005 beingmeta, inc.
   This file is part of EFramerD and is copyright and a valuable
   trade secret of beingmeta, inc.
*/

static char versionid[] = "$Id$";

#include <eframerd/lisp.h>
#include <stdio.h>

FD_EXPORT void fd_raise(fd_exception msg,u8_string details)
{
  if (details)
    fprintf(stderr,_("Uh oh: %s (%s)\n"),msg,details);
  else fprintf(stderr,_("Uh oh: %s\n"),msg);
  exit(1);
}

FD_EXPORT void fd_throw(fd_lisp irritant,fd_exception msg,u8_string details)
{
  if (FD_VOIDP(irritant))
    if (details)
      fprintf(stderr,_("Uh oh: %s (%s)\n"),msg,details);
    else fprintf(stderr,_("Uh oh: %s\n"),msg);
  else {
    struct U8_OUTPUT out;
    fd_ptr_type ctype=FD_PTR_TYPE(irritant);
    if (details)
      fprintf(stderr,_("Uh oh, %s (%s) #!%lx"),msg,details,irritant);
    else fprintf(stderr,_("Uh oh, %s #!%lx"),msg,irritant);    
    fflush(stderr);
    if ((ctype<=fd_symbol_type) ||
	((ctype>=fd_string_type) && (ctype<fd_mystery_type)) ||
	((ctype<FD_TYPE_MAX) && (fd_unparsers[ctype]))) {
      U8_INIT_OUTPUT(&out,1024); 
      fd_unparse(&out,irritant);
      fprintf(stderr,": %s\n",out.bytes); fflush(stderr);}
    else fprintf(stderr,": #!%lx",irritant);}
  exit(1);
}

FD_EXPORT int fd_init_errbase()
{
  /* errbase version numbers are primes in the range 200-300
     minor versions are indicated by powers of the major version. */
  return 211;
}

