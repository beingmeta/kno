/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: pool-get.c,v 1.16 2006/01/26 14:44:33 haase Exp $";

#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"
#include "libu8/u8.h"
#include "fdb/dtype.h"
#include "fdb/pools.h"

int main(int argc,char **argv)
{
  int fd_version=fd_init_dbfile();
  fd_pool p=fd_use_pool(argv[1]);
  fdtype oids=FD_EMPTY_CHOICE; 
  int i=2;
  if (p==NULL) {
    fprintf(stderr,_("Can't open pool %s\n"),argv[1]);
    return 0;}
  while (i<argc) {
    fdtype oid=fd_parse(argv[i]);
    FD_ADD_TO_CHOICE(oids,oid); i++;}
  if (argc>2) fd_pool_prefetch(p,oids);
  {FD_DO_CHOICES(oid,oids) {
      fdtype value=fd_oid_value(oid); 
      u8_fprintf(stderr,_("Value of %q is %q\n"),oid,value);
      fd_decref(value);}}
  fd_decref(oids);
  fd_pool_swapout(p);
  fd_pool_close(p);
  return 0;
}


/* The CVS log for this file
   $Log: pool-get.c,v $
   Revision 1.16  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.15  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.14  2005/03/28 19:20:24  haase
   Fixes to test targets, etc

   Revision 1.13  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.12  2005/02/14 01:29:35  haase
   Increased code coverage of tests

   Revision 1.11  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
