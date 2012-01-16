/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/dbfile.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"

int main(int argc,char **argv)
{
  int fd_version=fd_init_dbfile();
  fd_index ix=((fd_version>0)?(fd_open_index(argv[1])):(NULL));
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
