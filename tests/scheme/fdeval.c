/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/eval.h"

#include "libu8/libu8.h"

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

int main(int argc,char **argv)
{
  int fd_version=fd_init_fdscheme();
  fd_lispenv env=fd_working_environment();
  fdtype expr, value;
  fd_init_fdscheme(); fd_init_schemeio();
  fd_init_texttools(); fd_init_fdweb();
  u8_init_chardata_c();
  expr=fd_parse(argv[1]);
  value=fd_eval(expr,env);
  u8_fprintf(stderr,_("Value of %q is %q\n"),expr,value);
  fd_decref(value); fd_decref(expr); fd_decref((fdtype)env);
}


/* The CVS log for this file
   $Log: fdeval.c,v $
   Revision 1.13  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.12  2005/12/20 18:03:58  haase
   Made test executables include all libraries statically

   Revision 1.11  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.10  2005/05/04 09:11:44  haase
   Moved FD_INIT_SCHEME_BUILTINS into individual executables in order to dance around an OS X dependency problem

   Revision 1.9  2005/03/06 02:03:06  haase
   Fixed include statements for system header files

   Revision 1.8  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.7  2005/03/05 19:38:39  haase
   Added setlocale call

   Revision 1.6  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
