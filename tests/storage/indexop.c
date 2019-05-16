/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/storage.h"
#include "kno/drivers.h"

#include <libu8/libu8.h>
#include <libu8/u8stdio.h>

#include "strings.h"
#include "stdlib.h"
#include "stdio.h"
#include "sys/time.h"
#include "time.h"

int main(int argc,char **argv)
{
  int kno_version = kno_init_drivers();
  kno_index ix; lispval key;
  if (kno_version<0) {
    u8_fprintf(stderr,_("Unable to initialize Kno\n"));
    exit(1);}
  else if (argc<3) {
    u8_fprintf(stderr,_("Too few (<3) args\n"));
    exit(1);}
  else {
    ix = kno_get_index(argv[1],0,KNO_VOID);
    key = kno_parse(argv[2]);}
  if (argc == 3) {
    lispval value = kno_index_get(ix,key);
    u8_fprintf(stderr,_("The key %q is associated with %d values\n"),
               key,KNO_CHOICE_SIZE(value));
    {KNO_DO_CHOICES(each,value)
       u8_fprintf(stderr,"\t%q\n",each);}
    kno_decref(value); value = KNO_VOID;}
  else if (argc == 4) {
    lispval value;
    if ((argv[3][0] == '+') || (argv[3][0] == '-'))
      value = kno_parse(argv[3]+1);
    else value = kno_parse(argv[3]);
    if (argv[3][0] == '-') kno_index_drop(ix,key,value);
    else kno_index_add(ix,key,value);
    kno_decref(value); value = KNO_VOID;
    kno_commit_index(ix);}
  kno_decref(key); key = KNO_VOID;
  kno_index_swapout(ix,KNO_VOID);
  kno_index_close(ix);
  return 0;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
