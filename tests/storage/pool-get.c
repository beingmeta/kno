/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/drivers.h"

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
  int fd_version = fd_init_drivers();
  fd_pool p = ((fd_version>0)?(fd_use_pool(argv[1],0,FD_VOID)):(NULL));
  lispval oids = FD_EMPTY_CHOICE;
  int i = 2;
  if (p == NULL) {
    fprintf(stderr,_("Can't open pool %s\n"),argv[1]);
    return 0;}
  while (i<argc) {
    lispval oid = fd_parse(argv[i]);
    FD_ADD_TO_CHOICE(oids,oid); i++;}
  if (argc>2) fd_pool_prefetch(p,oids);
  {FD_DO_CHOICES(oid,oids) {
      lispval value = fd_oid_value(oid);
      u8_fprintf(stderr,_("Value of %q is %q\n"),oid,value);
      fd_decref(value);}}
  fd_decref(oids);
  fd_pool_swapout(p,FD_VOID);
  fd_pool_close(p);
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
