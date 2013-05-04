/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/eval.h"

#include "libu8/libu8.h"
#include "libu8/u8stdio.h"

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

FD_EXPORT void fd_init_texttools(void);
FD_EXPORT void fd_init_fdweb(void);

int main(int argc,char **argv)
{
  int fd_version=fd_init_fdscheme();
  fd_lispenv env;
  fdtype expr, value;
  if (fd_version<0) exit(1);
  fd_init_fdscheme(); fd_init_schemeio();
  fd_init_texttools(); fd_init_fdweb();
  u8_init_chardata_c();
  env=fd_working_environment();
  expr=fd_parse(argv[1]);
  value=fd_eval(expr,env);
  u8_fprintf(stderr,_("Value of %q is %q\n"),expr,value);
  fd_decref(value); fd_decref(expr); fd_decref((fdtype)env);
  return 0;
}
