/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/cons.h"
#include "kno/compounds.h"

#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>

#include <stdarg.h>

static struct KNO_HASHTABLE typeinfo;

KNO_EXPORT lispval kno_probe_typeinfo(lispval tag)
{
  return kno_hashtable_get(&typeinfo,tag,KNO_FALSE);
}

KNO_EXPORT lispval kno_use_typeinfo(lispval tag)
{
  lispval exists = kno_hashtable_get(&typeinfo,tag,KNO_VOID);
  if (KNO_VOIDP(exists)) {
    struct KNO_TYPEINFO *info = u8_alloc(struct KNO_TYPEINFO);
    KNO_INIT_FRESH_CONS(info,kno_typeinfo_type);
    info->typetag = tag; kno_incref(tag);
    info->type_props = kno_make_slotmap(2,0,NULL);
    info->type_handlers = kno_make_slotmap(2,0,NULL);
    int rv = kno_hashtable_op(&typeinfo,kno_table_init,tag,((lispval)info));
    if (rv > 0)
      return (lispval) info;
    else {
      kno_decref(typeinfo->typetag);
      kno_decref(typeinfo->type_props);
      kno_decref(typeinfo->type_handlers);
      u8_free(typeinfo);
      if (rv < 0)
	return KNO_ERROR;
      else return kno_hashtable_get(&typeinfo,tag,KNO_FALSE);}}
  else return exists;
}

/* Init methods */

void kno_init_typeinfo_c()
{
  u8_register_source_file(_FILEINFO);

  kno_init_hashtable(&typeinfo,231,NULL);
}

