/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/drivers.h"

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
  int kno_version = kno_init_drivers();
  kno_pool p = ((kno_version>0)?(kno_use_pool(argv[1],0,KNO_VOID)):(NULL));
  lispval oids = KNO_EMPTY_CHOICE;
  int i = 2;
  if (p == NULL) {
    fprintf(stderr,_("Can't open pool %s\n"),argv[1]);
    return 0;}
  while (i<argc) {
    lispval oid = kno_parse(argv[i]);
    KNO_ADD_TO_CHOICE(oids,oid); i++;}
  if (argc>2) kno_pool_prefetch(p,oids);
  {KNO_DO_CHOICES(oid,oids) {
      lispval value = kno_oid_value(oid);
      u8_fprintf(stderr,_("Value of %q is %q\n"),oid,value);
      kno_decref(value);}}
  kno_decref(oids);
  kno_pool_swapout(p,KNO_VOID);
  kno_pool_close(p);
  return 0;
}

