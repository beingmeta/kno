/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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
  fd_index ix; lispval key;
  if (fd_version<0) {
    u8_fprintf(stderr,_("Unable to initialize FramerD\n"));
    exit(1);}
  else if (argc<3) {
    u8_fprintf(stderr,_("Too few (<3) args\n"));
    exit(1);}
  else {
    ix = fd_get_index(argv[1],0,FD_VOID);
    key = fd_parse(argv[2]);}
  if (argc == 3) {
    lispval value = fd_index_get(ix,key);
    u8_fprintf(stderr,_("The key %q is associated with %d values\n"),
               key,FD_CHOICE_SIZE(value));
    {FD_DO_CHOICES(each,value)
       u8_fprintf(stderr,"\t%q\n",each);}
    fd_decref(value); value = FD_VOID;}
  else if (argc == 4) {
    lispval value;
    if ((argv[3][0] == '+') || (argv[3][0] == '-'))
      value = fd_parse(argv[3]+1);
    else value = fd_parse(argv[3]);
    if (argv[3][0] == '-') fd_index_drop(ix,key,value);
    else fd_index_add(ix,key,value);
    fd_decref(value); value = FD_VOID;
    fd_commit_index(ix);}
  fd_decref(key); key = FD_VOID;
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
