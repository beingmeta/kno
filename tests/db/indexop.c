/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"
#include "libu8/libu8.h"
#include "fdb/dtype.h"
#include "fdb/fddb.h"
#include "fdb/indices.h"

int main(int argc,char **argv)
{
  int fd_version=fd_init_dbfile();
  fd_index ix=fd_open_index(argv[1]);
  fdtype key=fd_parse(argv[2]);
  if (argc == 3) {
    fdtype value=fd_index_get(ix,key);
    u8_fprintf(stderr,_("The key %q is associated with %d values\n"),
	       key,FD_CHOICE_SIZE(value));
    {FD_DO_CHOICES(each,value)
       u8_fprintf(stderr,"\t%q\n",each);}
    fd_decref(value); value=FD_VOID;
  }
  else if (argc == 4) {
    fdtype value;
    if ((argv[3][0] == '+') || (argv[3][0] == '-'))
      value=fd_parse(argv[3]+1);
    else value=fd_parse(argv[3]);
    if (argv[3][0] == '-') fd_index_drop(ix,key,value);
    else fd_index_add(ix,key,value);
    fd_decref(value); value=FD_VOID;
    fd_index_commit(ix);}
  fd_decref(key); key=FD_VOID;
  fd_index_swapout(ix);
  fd_index_close(ix);
}


/* The CVS log for this file
   $Log: indexop.c,v $
   Revision 1.11  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.10  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.9  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.8  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
