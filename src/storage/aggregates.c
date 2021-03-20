/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_BUFIO 1
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8memlist.h>
#include <libu8/u8printf.h>

static struct KNO_INDEX_HANDLER aggregate_index_handler;

static lispval aggregate_fetch(kno_index ix,lispval key)
{
  struct KNO_AGGREGATE_INDEX *aix = (struct KNO_AGGREGATE_INDEX *)ix;
  lispval combined = EMPTY;
  int i = 0, lim;
  kno_lock_index(aix);
  lim = aix->ax_n_indexes;
  while (i < lim) {
    kno_index each = aix->ax_indexes[i++];
    lispval value;
    if (each->index_cache_level<0) {
      each->index_cache_level = kno_default_cache_level;
      kno_index_setcache(each,kno_default_cache_level);}
    if (kno_hashtable_probe(&(each->index_cache),key))
      value = kno_hashtable_get(&(each->index_cache),key,EMPTY);
    else if ((each->index_adds.table_n_keys) ||
             (each->index_drops.table_n_keys) ||
             (each->index_stores.table_n_keys))
      value = kno_index_get(each,key);
    else value = each->index_handler->fetch(each,key);
    if (KNO_ABORTP(value)) {
      kno_decref(combined);
      kno_unlock_index(aix);
      return value;}
    else {CHOICE_ADD(combined,value);}}
  kno_unlock_index(aix);
  return combined;
}

static int aggregate_prefetch(kno_index ix,lispval keys)
{
  int n_fetches = 0, i = 0, lim, n = KNO_CHOICE_SIZE(keys);
  struct KNO_AGGREGATE_INDEX *aix = (struct KNO_AGGREGATE_INDEX *)ix;
  lispval *keyv = u8_big_alloc_n(n,lispval);
  lispval *valuev = u8_big_alloc_n(n,lispval);
  DO_CHOICES(key,keys)
    if (!(kno_hashtable_probe(&(aix->index_cache),key))) {
      keyv[n_fetches]=key;
      valuev[n_fetches]=EMPTY;
      n_fetches++;}
  if (n_fetches==0) {
    u8_big_free(keyv);
    u8_big_free(valuev);
    return 0;}
  kno_lock_index(aix);
  lim = aix->ax_n_indexes;
  while (i < lim) {
    int j = 0; kno_index each = aix->ax_indexes[i];
    lispval *values=each->index_handler->fetchn(each,n_fetches,keyv);
    if (values == NULL) {
      u8_big_free(keyv);
      u8_big_free(valuev);
      kno_unlock_index(aix);
      return -1;}
    while (j<n_fetches) {
      CHOICE_ADD(valuev[j],values[j]); j++;}
    u8_big_free(values);
    i++;}
  kno_unlock_index(aix);
  i = 0; while (i<n_fetches) {
           if (PRECHOICEP(valuev[i])) {
             valuev[i]=kno_simplify_choice(valuev[i]);}
           i++;}
  /* The operation kno_table_add_empty_noref will create an entry even
     if the value is the empty choice. */
  kno_hashtable_iter(&(aix->index_cache),kno_table_add_empty_noref,
                    n_fetches,keyv,valuev);
  u8_big_free(keyv);
  u8_big_free(valuev);
  return n_fetches;
}

static lispval *aggregate_fetchn(kno_index ix,int n,const lispval *keys)
{
  int n_fetches = 0, i = 0, lim;
  struct KNO_AGGREGATE_INDEX *aix = (struct KNO_AGGREGATE_INDEX *)ix;
  lispval *keyv = u8_big_alloc_n(n,lispval);
  lispval *valuev = u8_big_alloc_n(n,lispval);
  unsigned int *posmap = u8_big_alloc_n(n,unsigned int);
  const lispval *scan = keys, *limit = keys+n;
  while (scan<limit) {
    int off = scan-keys; lispval key = *scan++;
    if (!(kno_hashtable_probe(&(aix->index_cache),key))) {
      keyv[n_fetches]=key;
      valuev[n_fetches]=EMPTY;
      posmap[n_fetches]=off;
      n_fetches++;}
    else valuev[scan-keys]=kno_hashtable_get(&(aix->index_cache),key,EMPTY);}
  if (n_fetches==0) {
    u8_big_free(keyv);
    u8_big_free(posmap);
    return valuev;}
  kno_lock_index(aix);
  lim = aix->ax_n_indexes;
  while (i < lim) {
    int j = 0; kno_index each = aix->ax_indexes[i];
    lispval *values=each->index_handler->fetchn(each,n_fetches,keyv);
    if (values == NULL) {
      u8_big_free(keyv);
      u8_big_free(posmap);
      u8_big_free(valuev);
      kno_unlock_index(aix);
      return NULL;}
    while (j<n_fetches) {
      CHOICE_ADD(valuev[posmap[j]],values[j]);
      j++;}
    u8_big_free(values);
    i++;}
  kno_unlock_index(aix);
  i = 0; while (i<n_fetches) {
           if (PRECHOICEP(valuev[i])) {
             valuev[i]=kno_simplify_choice(valuev[i]);}
           i++;}
  /* The operation kno_table_add_empty_noref will create an entry even
     if the value is the empty choice. */
  u8_big_free(keyv);
  u8_big_free(posmap);
  return valuev;
}

static lispval *aggregate_fetchkeys(kno_index ix,int *n)
{
  struct KNO_AGGREGATE_INDEX *aix = (struct KNO_AGGREGATE_INDEX *)ix;
  lispval combined = EMPTY;
  int i = 0, lim;
  kno_lock_index(aix);
  lim = aix->ax_n_indexes;
  while (i < lim) {
    kno_index each = aix->ax_indexes[i++];
    lispval keys = kno_index_keys(each);
    if (KNO_ABORTP(keys)) {
      kno_decref(combined);
      kno_unlock_index(aix);
      kno_interr(keys);
      return NULL;}
    else {CHOICE_ADD(combined,keys);}}
  kno_unlock_index(aix);
  {
    lispval simple = kno_simplify_choice(combined);
    int n_elts = KNO_CHOICE_SIZE(simple);
    lispval *results = ((n>0) ? (u8_big_alloc_n(n_elts,lispval)) : (NULL));
    if (n_elts==0) {
      *n = 0;
      return results;}
    else if (n_elts==1) {
      results[0]=simple;
      *n = 1;
      return results;}
    else if (KNO_CONS_REFCOUNT((kno_cons)simple)==1) {
      int j=0;
      DO_CHOICES(key,simple) {
        results[j]=key;
        j++;}
      kno_free_choice((kno_choice)simple);
      *n = j;
      return results;}
    else {
      int j=0;
      DO_CHOICES(key,simple) {
        results[j]=kno_incref(key);
        j++;}
      kno_decref(simple);
      *n = j;
      return results;}
  }
}

DEF_KNOSYM(canonical);

KNO_EXPORT kno_aggregate_index kno_make_aggregate_index
(lispval opts,int n_allocd,int n,kno_index *indexes)
{
  struct KNO_AGGREGATE_INDEX *aix = u8_alloc(struct KNO_AGGREGATE_INDEX);
  lispval metadata = kno_getopt(opts,KNOSYM_METADATA,KNO_VOID);
  kno_storage_flags flags =
    kno_get_dbflags(opts,KNO_STORAGE_ISINDEX|KNO_STORAGE_READ_ONLY);
  lispval idval = kno_getopt(opts,KNOSYM_LABEL,KNO_VOID);
  u8_string id = (KNO_STRINGP(idval)) ? (KNO_CSTRING(idval)) :
    (U8S("aggregate+0"));
  kno_init_index((kno_index)aix,&aggregate_index_handler,
		 id,NULL,NULL,flags,metadata,opts);
  if (n_allocd < n) n_allocd = n;
  u8_init_mutex(&(aix->index_lock));
  aix->ax_n_allocd = n_allocd;
  aix->ax_n_indexes = 0;
  aix->ax_indexes = u8_alloc_n(n_allocd,kno_index);
  aix->ax_oldvecs = NULL;
  int i = 0; while (i < n) {
    kno_index add = indexes[i++];
    if (add) kno_add_to_aggregate_index(aix,add);}
  kno_register_index((kno_index)aix);
  kno_decref(idval);
  return aix;
}

static int aggregateidp(u8_string id)
{
  if (id==NULL) return 1;
  u8_string plus = strrchr(id,'+');
  if (plus==NULL) return 0;
  else if (plus[1]=='\0') return 0;
  else {
    u8_string scan = plus+1;
    while (isdigit(*scan)) scan++;
    if (*scan=='\0') return 1;
    else return 0;}
}

KNO_EXPORT int kno_add_to_aggregate_index(kno_aggregate_index aix,kno_index add)
{
  if (aix->index_handler == &aggregate_index_handler) {
    int i = 0, n = aix->ax_n_indexes;
    while (i < n)
      if (aix->ax_indexes[i] == add)
        return 0;
      else i++;
    if (add == ((kno_index)aix))
      return 0;
    kno_lock_index(aix);
    if ( (aix->ax_n_allocd == 0) || (aix->ax_indexes == NULL) ) {
      int size = 4;
      kno_index *new = u8_realloc_n(aix->ax_indexes,size,kno_index);
      if (new) {
        aix->ax_indexes = new;
        aix->ax_n_allocd = size;
        aix->ax_n_indexes = 0;}
      else {
        kno_unlock_index(aix);
        u8_seterr("CouldntGrowIndex","kno_add_to_aggregate_index",
                  u8_strdup(aix->indexid));
        return -1;}}
    else if (aix->ax_n_indexes >= aix->ax_n_allocd) {
      int allocd = aix->ax_n_allocd;
      int reallocd = allocd * 2;
      kno_index *new = u8_realloc_n(aix->ax_indexes,reallocd,kno_index);
      if (new) {
        /* We keep the old array value around because it might still
           be accessed by other threads. But we keep it in a list to avoid
           potentially leak errors. A kludge, but it's not going to be a big
           leak. */
        aix->ax_oldvecs = u8_cons_list(aix->ax_indexes,aix->ax_oldvecs,0);
        aix->ax_indexes = new;
        aix->ax_n_allocd = reallocd;}
      else {
        kno_unlock_index(aix);
        u8_seterr("CouldntGrowIndex","kno_add_to_aggregate_index",
                  u8_strdup(aix->indexid));
        return -1;}}
    else NO_ELSE;

    aix->ax_indexes[aix->ax_n_indexes++]=add;

    if (add->index_serialno<0) {
      lispval alix = (lispval)add;
      kno_incref(alix);}

    /* If the new index doesn't cache or it has a keyslot that's different
       from the aggregate, don't cache the aggregate index */
    if (add->index_cache_level == 0)
      aix->index_cache_level = 0;
    else {
      lispval keyslot = add->index_keyslot;
      if ( (KNO_OIDP(keyslot)) || (KNO_SYMBOLP(keyslot)) ) {
        if ( keyslot != aix->index_keyslot ) {
          aix->index_cache_level = 0;}}}

    u8_string old_source = aix->index_source;

    u8_string old_id = aix->indexid;
    if (aggregateidp(old_id)) {
      aix->indexid = u8_mkstring("%s+%d",
				 aix->ax_indexes[0]->indexid,
				 (aix->ax_n_indexes-1));
      if (old_id) u8_free(old_id);}

    struct U8_OUTPUT sourceout;
    U8_INIT_OUTPUT(&sourceout,128);
    int j = 0; while (j < aix->ax_n_indexes) {
      kno_index each = aix->ax_indexes[j];
      if (each->index_source)
	u8_puts(&sourceout,each->index_source);
      else if (each->indexid)
	u8_puts(&sourceout,each->indexid);
      else NO_ELSE;
      u8_putc(&sourceout,'\n');
      j++;}
    aix->index_source=sourceout.u8_outbuf;
    if (old_source) u8_free(old_source);

    /* Invalidate the cache now that there's a new source */
    kno_reset_hashtable(&(aix->index_cache),-1,1);
    kno_unlock_index(aix);
    return 1;}
  else return kno_reterr(kno_TypeError,("aggregate_index"),NULL,
                        kno_index2lisp((kno_index)aix));
}

/* CTL */

static lispval partitions_symbol;

static lispval aggregate_ctl(kno_index ix,lispval op,int n,kno_argvec args)
{
  struct KNO_AGGREGATE_INDEX *cx = (struct KNO_AGGREGATE_INDEX *)ix;
  if ( ((n>0)&&(args == NULL)) || (n<0) )
    return kno_err("BadIndexOpCall","hashindex_ctl",
                  cx->indexid,VOID);
  else if (op == partitions_symbol) {
    if (n == 0) {
      lispval result = EMPTY;
      kno_index *indexes = cx->ax_indexes;
      int i = 0; while (i<cx->ax_n_indexes) {
        kno_index ix = indexes[i++];
        lispval lix = kno_index2lisp(ix);
        kno_incref(lix);
        CHOICE_ADD(result,lix);}
      return result;}
    else if (n == 1) {
      kno_index front = kno_lisp2index(args[0]);
      int rv = kno_add_to_aggregate_index(cx,front);
      if (rv<0) return KNO_ERROR;
      else return KNO_INT(cx->ax_n_indexes);}
    else return kno_err(kno_TooManyArgs,"aggregate_index_ctl/paritions",
                       cx->indexid,VOID);}
  else return kno_default_indexctl(ix,op,n,args);
}

static void recycle_aggregate_index(kno_index ix)
{
  if (ix->index_serialno>=0) return;
  struct KNO_AGGREGATE_INDEX *agg = (struct KNO_AGGREGATE_INDEX *) ix;
  kno_index *indexes = agg->ax_indexes;
  int i = 0, n = agg->ax_n_indexes;
  while (i < n) {
    kno_index each = indexes[i++];
    if (each->index_serialno<0) {
      lispval lix = (lispval) each;
      kno_decref(lix);}}
  u8_free(agg->ax_indexes);
  agg->ax_indexes = NULL;
  agg->ax_n_indexes = agg->ax_n_allocd = 0;
  if (agg->ax_oldvecs) u8_free_list(agg->ax_oldvecs);
}

static struct KNO_INDEX_HANDLER aggregate_index_handler={
  "aggregateindex", 1, sizeof(struct KNO_AGGREGATE_INDEX), 14,
  NULL, /* close */
  NULL, /* commit */
  aggregate_fetch, /* fetch */
  NULL, /* fetchsize */
  aggregate_prefetch, /* prefetch */
  aggregate_fetchn, /* fetchn */
  aggregate_fetchkeys, /* fetchkeys */
  NULL, /* fetchinfo */
  NULL, /* batchadd */
  NULL, /* create */
  NULL, /* walk */
  recycle_aggregate_index, /* recycle */
  aggregate_ctl /* indexctl */
};
struct KNO_INDEX_HANDLER *kno_aggregate_index_handler=&aggregate_index_handler;

KNO_EXPORT int kno_aggregate_indexp(kno_index ix)
{
  return ( (ix) && (ix->index_handler == &aggregate_index_handler) );
}

KNO_EXPORT void kno_init_aggregates_c()
{
  u8_register_source_file(_FILEINFO);
  partitions_symbol = kno_intern("partitions");

  kno_init_index(((kno_index)kno_default_background),
                &aggregate_index_handler,
                "background",NULL,NULL,
                KNO_STORAGE_ISINDEX|KNO_STORAGE_READ_ONLY,
                KNO_FALSE,KNO_FALSE);
  kno_default_background->ax_n_allocd = 64;
  kno_default_background->ax_n_indexes = 0;
  kno_default_background->ax_indexes = u8_alloc_n(64,kno_index);
  kno_default_background->ax_oldvecs = NULL;
  kno_register_index(((kno_index)kno_default_background));
}

