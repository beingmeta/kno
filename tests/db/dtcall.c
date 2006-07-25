/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/pools.h"

#include <libu8/libu8.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

int main(int argc,char **argv)
{
  u8_connection socket;
  int fd_version=fd_init_dbfile(), i=0;
  struct FD_DTYPE_STREAM ds;
  fdtype expr=FD_EMPTY_LIST, result;
  socket=u8_connect(argv[1]);
  if (socket<0) {
    u8_fprintf(stderr,"Couldn't open socket to %s\n",argv[1]);
    exit(1);}
  fd_init_dtype_stream(&ds,socket,1024,NULL,NULL);
  i=argc-1; while (i>1) expr=fd_make_pair(fd_parse(argv[i--]),expr);
  u8_fprintf(stderr,_("Sending: %q\n"),expr);
  fd_dtswrite_dtype(&ds,expr); 
  result=fd_dtsread_dtype(&ds);
  u8_fprintf(stderr,_("Result is: %q\n"),result);
  fd_decref(expr); fd_decref(result);
  fd_dtsclose(&ds,1);
  return 0;
}


/* The CVS log for this file
   $Log: dtcall.c,v $
   Revision 1.12  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.11  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.10  2005/03/24 17:41:31  haase
   Added newline to error message

   Revision 1.9  2005/03/24 17:25:06  haase
   Catch socket error in dtcall

   Revision 1.8  2005/03/24 00:30:01  haase
   Fix return value from dtcall test

   Revision 1.7  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.6  2005/02/11 20:09:35  haase
   Added explicit call to fd_init_dbfile to ensure that the library was linked in.

   Revision 1.5  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
