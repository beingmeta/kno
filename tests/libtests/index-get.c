/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#include "kno/knosource.h"
#include "kno/lisp.h"
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
  kno_index ix = ((kno_version>0)?(kno_get_index(argv[1],0,KNO_VOID)):(NULL));
  lispval keys = KNO_EMPTY_CHOICE;
  int i = 2;
  if (ix == NULL) {
    fprintf(stderr,_("Can't open index %s\n"),argv[1]);
    return 0;}
  while (i<argc) {
    lispval key = kno_parse(argv[i]);
    KNO_ADD_TO_CHOICE(keys,key); i++;}
  if (argc>2) kno_index_prefetch(ix,keys);
  {KNO_DO_CHOICES(key,keys) {
      lispval value = kno_index_get(ix,key);
      u8_fprintf(stderr,_("Value of %q is %q\n"),key,value);
      kno_decref(value);}}
  kno_decref(keys);
  kno_index_swapout(ix,KNO_VOID);
  kno_index_close(ix);
  return 0;
}

