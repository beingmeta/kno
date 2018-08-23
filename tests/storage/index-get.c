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

#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"

int main(int argc,char **argv)
{
  int fd_version = fd_init_drivers();
  fd_index ix = ((fd_version>0)?(fd_get_index(argv[1],0,FD_VOID)):(NULL));
  lispval keys = FD_EMPTY_CHOICE;
  int i = 2;
  if (ix == NULL) {
    fprintf(stderr,_("Can't open index %s\n"),argv[1]);
    return 0;}
  while (i<argc) {
    lispval key = fd_parse(argv[i]);
    FD_ADD_TO_CHOICE(keys,key); i++;}
  if (argc>2) fd_index_prefetch(ix,keys);
  {FD_DO_CHOICES(key,keys) {
      lispval value = fd_index_get(ix,key);
      u8_fprintf(stderr,_("Value of %q is %q\n"),key,value);
      fd_decref(value);}}
  fd_decref(keys);
  fd_index_swapout(ix,FD_VOID);
  fd_index_close(ix);
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
