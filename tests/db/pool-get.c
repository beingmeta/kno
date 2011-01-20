/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2011 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/fddb.h"
#include "fdb/dbfile.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>
#include <libu8/u8netfns.h>

#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"

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
