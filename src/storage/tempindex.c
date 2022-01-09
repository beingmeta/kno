/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_BUFIO (!(KNO_AVOID_INLINE))
#define KNO_INLINE_CHOICES (!(KNO_AVOID_INLINE))
#define KNO_FAST_CHOICE_CONTAINSP (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

static struct KNO_INDEX_HANDLER tempindex_handler;

/* The in-memory index */

static lispval tempindex_fetch(kno_index ix,lispval key)
{
  return KNO_EMPTY_CHOICE;
}

static int tempindex_fetchsize(kno_index ix,lispval key)
{
  return 0;
}

static lispval *tempindex_fetchn(kno_index ix,int n,const lispval *keys)
{
  lispval *values = u8_big_alloc_n(n,lispval);
  int i = 0; while (i<n) {values[i++] = KNO_EMPTY;}
  return values;
}

static lispval *tempindex_fetchkeys(kno_index ix,int *n)
{
  *n = 0;
  return NULL;
}

static struct KNO_KEY_SIZE *tempindex_fetchinfo(kno_index ix,kno_choice filter,int *n)
{
  *n = 0;
  return NULL;
}

static unsigned int tempindex_init_n_slots = 24593;

static kno_index open_tempindex(u8_string name,kno_storage_flags flags,
                               lispval opts)
{
  struct KNO_TEMPINDEX *tempindex = u8_alloc(struct KNO_TEMPINDEX);
  lispval metadata = kno_getopt(opts,KNOSYM_METADATA,KNO_VOID);
  lispval size = kno_getopt(opts,KNOSYM_SIZE,KNO_VOID);
  kno_init_index((kno_index)tempindex,&tempindex_handler,
                name,name,name,
                flags|KNO_STORAGE_NOSWAP,
                metadata,
                opts);
  if (KNO_FIXNUMP(size)) {
    int n_slots = ((KNO_FIX2INT(size)) > 0) ?
      (kno_get_hashtable_size(size)) :
      (-(KNO_FIX2INT(size)));
    kno_resize_hashtable(&(tempindex->index_adds),n_slots);}
  else kno_resize_hashtable(&(tempindex->index_adds),tempindex_init_n_slots);
  kno_register_index((kno_index)tempindex);
  kno_decref(metadata);
  return (kno_index)tempindex;
}

static lispval tempindex_ctl(kno_index ix,lispval op,int n,kno_argvec args)
{
  struct KNO_TEMPINDEX *mix = (struct KNO_TEMPINDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return kno_err("BadIndexOpCall","hashindex_ctl",
                  mix->indexid,VOID);
  else if (op == kno_cachelevel_op)
    return KNO_INT(0);
  else if (op == kno_capacity_op)
    return EMPTY;
  else if (op == kno_swapout_op)
    return KNO_FALSE;
  else if (op == kno_load_op)
    return KNO_INT(ix->index_adds.table_n_keys);
  else if (op == kno_keycount_op)
    return KNO_INT(ix->index_adds.table_n_keys);
  else return kno_default_indexctl(ix,op,n,args);
}

/* The predicate for testing these */

KNO_EXPORT int kno_tempindexp(kno_index ix)
{
  return ( (ix) && (ix->index_handler == &tempindex_handler) );
}

KNO_EXPORT kno_index kno_make_tempindex
(u8_string name,kno_storage_flags flags,lispval opts)
{
  return open_tempindex(name,flags,opts);
}

static lispval register_symbol = KNO_VOID;

static kno_index create_tempindex(u8_string spec,void *data,
                                 kno_storage_flags flags,lispval opts)
{
  lispval registered = kno_getopt(opts,register_symbol,KNO_VOID);
  if (KNO_VOIDP(registered))
    flags |= KNO_STORAGE_UNREGISTERED;
  return open_tempindex(spec,flags,opts);
}

/* Initializing the driver module */

static struct KNO_INDEX_HANDLER tempindex_handler={
  "tempindex", 1, sizeof(struct KNO_TEMPINDEX), 14, NULL,
  NULL, /* close */
  NULL, /* commit */
  tempindex_fetch, /* fetch */
  tempindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  tempindex_fetchn, /* fetchn */
  tempindex_fetchkeys, /* fetchkeys */
  tempindex_fetchinfo, /* fetchinfo */
  NULL, /* batchadd */
  create_tempindex, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  tempindex_ctl  /* indexctl */
};

KNO_EXPORT void kno_init_tempindex_c()
{
  kno_register_index_type("tempindex",
                         &tempindex_handler,
                         open_tempindex,
                         NULL,
                         NULL);
  register_symbol = kno_intern("register");

  kno_register_config("TEMPINDEX:SIZE","default size for tempindex caches",
                     kno_intconfig_get,kno_intconfig_set,
                     &tempindex_init_n_slots);

  u8_register_source_file(_FILEINFO);
}

