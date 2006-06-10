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
#include "libu8/u8.h"
#include "fdb/dtype.h"
#include "fdb/pools.h"
#include "fdb/indices.h"

int main(int argc,char **argv)
{
  int fd_version=fd_init_dbfile();
  fd_index ix=fd_open_index(argv[1]);
  fdtype keys=FD_EMPTY_CHOICE; 
  int i=2;
  if (ix==NULL) {
    fprintf(stderr,_("Can't open index %s\n"),argv[1]);
    return 0;}
  while (i<argc) {
    fdtype key=fd_parse(argv[i]);
    FD_ADD_TO_CHOICE(keys,key); i++;}
  if (argc>2) fd_index_prefetch(ix,keys);
  {FD_DO_CHOICES(key,keys) {
      fdtype value=fd_index_get(ix,key); 
      u8_fprintf(stderr,_("Value of %q is %q\n"),key,value);
      fd_decref(value);}}
  fd_decref(keys);
  fd_index_swapout(ix);
  fd_index_close(ix);
  return 0;
}


/* The CVS log for this file
   $Log: index-get.c,v $
   Revision 1.9  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.8  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.7  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.6  2005/03/28 19:20:24  haase
   Fixes to test targets, etc

   Revision 1.5  2005/02/14 01:29:35  haase
   Increased code coverage of tests

   Revision 1.4  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
