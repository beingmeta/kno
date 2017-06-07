/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_INDEXES 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"
#include "framerd/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>

fd_exception fd_EphemeralIndex=_("ephemeral index");
fd_exception fd_ReadOnlyIndex=_("read-only index");
fd_exception fd_NoFileIndexes=_("file indexes are not supported");
fd_exception fd_NotAFileIndex=_("not a file index");
fd_exception fd_BadIndexSpec=_("bad index specification");
fd_exception fd_IndexCommitError=_("can't save changes to index");

fdtype fd_index_hashop, fd_index_slotsop;

u8_condition fd_IndexCommit=_("Index/Commit");
static u8_condition ipeval_ixfetch="IXFETCH";

int fd_index_cache_init  = FD_INDEX_CACHE_INIT;
int fd_index_edits_init  = FD_INDEX_EDITS_INIT;
int fd_index_adds_init   = FD_INDEX_ADDS_INIT;

fd_index fd_primary_indexes[FD_N_PRIMARY_INDEXES];
int fd_n_primary_indexes = 0;
fd_index *fd_secondary_indexes = NULL;
int fd_n_secondary_indexes = 0;

static fd_index *consed_indexes = NULL;
ssize_t n_consed_indexes = 0, consed_indexes_len=0;
u8_mutex consed_indexes_lock;

static int add_consed_index(fd_index ix)
{
  u8_lock_mutex(&consed_indexes_lock);
  fd_index *scan=consed_indexes, *limit=scan+n_consed_indexes;
  while (scan<limit) {
    if ( *scan == ix ) {
      u8_unlock_mutex(&consed_indexes_lock);
      return 0;}
    else scan++;}
  if (n_consed_indexes>=consed_indexes_len) {
    ssize_t new_len=consed_indexes_len+64;
    fd_index *newvec=u8_realloc(consed_indexes,new_len*sizeof(fd_index));
    if (newvec) {
      consed_indexes=newvec;
      consed_indexes_len=new_len;}
    else {
      u8_seterr(fd_MallocFailed,"add_consed_index",u8dup(ix->indexid));
      u8_unlock_mutex(&consed_indexes_lock);
      return -1;}}
  consed_indexes[n_consed_indexes++]=ix;
  u8_unlock_mutex(&consed_indexes_lock);
  return 1;
}

static int drop_consed_index(fd_index ix)
{
  u8_lock_mutex(&consed_indexes_lock);
  fd_index *scan=consed_indexes, *limit=scan+n_consed_indexes;
  while (scan<limit) {
    if ( *scan == ix ) {
      size_t to_shift=(limit-scan)-1;
      memmove(scan,scan+1,to_shift);
      n_consed_indexes--;
      u8_unlock_mutex(&consed_indexes_lock);
      return 1;}
    else scan++;}
  u8_unlock_mutex(&consed_indexes_lock);
  return 0;
}

struct FD_COMPOUND_INDEX *fd_background = NULL;
static u8_mutex background_lock;

#if FD_GLOBAL_IPEVAL
static fdtype *index_delays;
#elif FD_USE__THREAD
static __thread fdtype *index_delays;
#elif FD_USE_TLS
static u8_tld_key index_delays_key;
#else
static fdtype *index_delays;
#endif

#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
#define get_index_delays() \
  ((fdtype *)u8_tld_get(index_delays_key))
FD_EXPORT void fd_init_index_delays()
{
  fdtype *delays = (fdtype *)u8_tld_get(index_delays_key);
  if (delays) return;
  else {
    int i = 0;
    delays = u8_alloc_n(FD_N_INDEX_DELAYS,fdtype);
    while (i<FD_N_INDEX_DELAYS) delays[i++]=EMPTY;
    u8_tld_set(index_delays_key,delays);}

}
#else
#define get_index_delays() (index_delays)
FD_EXPORT void fd_init_index_delays()
{
  if (index_delays) return;
  else {
    int i = 0;
    index_delays = u8_alloc_n(FD_N_INDEX_DELAYS,fdtype);
    while (i<FD_N_INDEX_DELAYS) index_delays[i++]=EMPTY;}

}
#endif

FD_EXPORT fdtype *fd_get_index_delays() { return get_index_delays(); }

static fdtype set_symbol, drop_symbol;

static u8_mutex indexes_lock;

/* Index ops */

FD_EXPORT fdtype fd_index_ctl(fd_index x,fdtype indexop,int n,fdtype *args)
{
  struct FD_INDEX_HANDLER *h = x->index_handler;
  if (h->indexctl)
    return h->indexctl(x,indexop,n,args);
  else return FD_FALSE;
}

/* Index cache levels */

/* a cache_level<0 indicates no caching has been done */

FD_EXPORT void fd_index_setcache(fd_index ix,int level)
{
  fdtype intarg = FD_INT(level);
  fdtype result = fd_index_ctl(ix,fd_cachelevel_op,1,&intarg);
  if (FD_ABORTP(result)) {fd_clear_errors(1);}
  fd_decref(result);
  ix->index_cache_level = level;
}

static void init_cache_level(fd_index ix)
{
  if (PRED_FALSE(ix->index_cache_level<0)) {
    fd_index_setcache(ix,fd_default_cache_level);}
}


/* The index registry */

FD_EXPORT void fd_register_index(fd_index ix)
{
  if (ix->index_flags&FD_STORAGE_UNREGISTERED)
    return;
  else if (ix->index_serialno<0) {
    u8_lock_mutex(&indexes_lock);
    if (ix->index_serialno>=0) { /* Handle race condition */
      u8_unlock_mutex(&indexes_lock); return;}
    if (fd_n_primary_indexes<FD_N_PRIMARY_INDEXES) {
      ix->index_serialno = fd_n_primary_indexes;
      fd_primary_indexes[fd_n_primary_indexes++]=ix;}
    else {
      if (fd_secondary_indexes)
        fd_secondary_indexes = u8_realloc_n
          (fd_secondary_indexes,fd_n_secondary_indexes+1,fd_index);
      else fd_secondary_indexes = u8_alloc_n(1,fd_index);
      ix->index_serialno = fd_n_secondary_indexes+FD_N_PRIMARY_INDEXES;
      fd_secondary_indexes[fd_n_secondary_indexes++]=ix;}
    u8_unlock_mutex(&indexes_lock);}
}

FD_EXPORT fdtype fd_index2lisp(fd_index ix)
{
  if (ix == NULL)
    return FD_ERROR;
  else if (ix->index_serialno>=0)
    return FDTYPE_IMMEDIATE(fd_index_type,ix->index_serialno);
  else return fd_incref((fdtype)ix);
}
FD_EXPORT fd_index fd_lisp2index(fdtype lix)
{
  if (FD_ABORTP(lix)) return NULL;
  else if (FD_TYPEP(lix,fd_index_type)) {
    int serial = FD_GET_IMMEDIATE(lix,fd_index_type);
    if (serial<FD_N_PRIMARY_INDEXES) return fd_primary_indexes[serial];
    else return fd_secondary_indexes[serial-FD_N_PRIMARY_INDEXES];}
  else if (FD_TYPEP(lix,fd_consed_index_type))
    return (fd_index) lix;
  else {
    fd_seterr(fd_TypeError,_("not an index"),NULL,lix);
    return NULL;}
}

/* Finding indexes by ids/sources/etc */

FD_EXPORT fd_index fd_find_index(u8_string spec)
{
  fd_index ix=fd_find_index_by_source(spec);
  if (ix) return ix;
  else if ((ix=fd_find_index_by_id(spec)))
    return ix;
  /* TODO: Add generic method which uses the index matcher
     methods to find indexes */
  else return ix;
}
FD_EXPORT u8_string fd_locate_index(u8_string spec)
{
  fd_index ix=fd_find_index_by_source(spec);
  if (ix) return u8_strdup(ix->index_source);
  else if ((ix=fd_find_index_by_id(spec)))
    return u8_strdup(ix->index_source);
  /* TODO: Add generic method which uses the index matcher
     methods to find indexes */
  else return NULL;
}

static int match_index_source(fd_index ix,u8_string source)
{
  return ((source)&&(ix->index_source)&&
          (strcmp(ix->index_source,source) == 0));
}

static int match_index_id(fd_index ix,u8_string id)
{
  return ((id)&&(ix->indexid)&&
          (strcmp(ix->indexid,id) == 0));
}

FD_EXPORT fd_index fd_find_index_by_id(u8_string id)
{
  int i = 0;
  if (id == NULL) return NULL;
  else while (i<fd_n_primary_indexes)
         if (match_index_id(fd_primary_indexes[i],id))
           return fd_primary_indexes[i];
         else i++;
  if (fd_secondary_indexes == NULL) return NULL;
  i = 0; while (i<fd_n_secondary_indexes)
           if (match_index_id(fd_secondary_indexes[i],id))
             return fd_secondary_indexes[i];
           else i++;
  return NULL;
}
FD_EXPORT fd_index fd_find_index_by_source(u8_string source)
{
  int i = 0;
  if (source == NULL) return NULL;
  else while (i<fd_n_primary_indexes)
         if (match_index_source(fd_primary_indexes[i],source))
           return fd_primary_indexes[i];
         else i++;
  if (fd_secondary_indexes == NULL) return NULL;
  i = 0; while (i<fd_n_secondary_indexes)
           if (match_index_source(fd_secondary_indexes[i],source))
             return fd_secondary_indexes[i];
           else i++;
  return NULL;
}

FD_EXPORT fd_index fd_get_index
(u8_string spec,fd_storage_flags flags,fdtype opts)
{
  if (strchr(spec,';')) {
    fd_index ix = NULL;
    u8_byte *copy = u8_strdup(spec);
    u8_byte *start = copy, *brk = strchr(start,';');
    while (brk) {
      if (ix == NULL) {
        *brk='\0'; ix = fd_open_index(start,flags,opts);
        if (ix) {brk = NULL; start = NULL;}
        else {
          start = brk+1;
          brk = strchr(start,';');}}}
    if (ix) return ix;
    else if ((start)&&(*start)) {
      int start_off = start-copy;
      u8_free(copy);
      return fd_open_index(spec+start_off,flags,opts);}
    else return NULL;}
  else return fd_open_index(spec,flags,opts);
}

/* Background indexes */

FD_EXPORT int fd_add_to_background(fd_index ix)
{
  if (ix == NULL) return 0;
  if (ix->index_serialno<0) {
    fdtype lix = (fdtype)ix;
    fd_incref(lix);}
  u8_lock_mutex(&background_lock);
  ix->index_flags = ix->index_flags|FD_INDEX_IN_BACKGROUND;
  if (fd_background)
    fd_add_to_compound_index(fd_background,ix);
  else {
    fd_index *indexes = u8_alloc_n(1,fd_index);
    indexes[0]=ix;
    fd_background=
      (struct FD_COMPOUND_INDEX *)fd_make_compound_index(1,indexes);
    u8_string old_id=fd_background->indexid;
    fd_background->indexid=u8_strdup("background");
    if (old_id) u8_free(old_id);}
  u8_unlock_mutex(&background_lock);
  return 1;
}

FD_EXPORT fd_index fd_use_index(u8_string spec,fd_storage_flags flags,fdtype opts)
{
  flags&=~FD_STORAGE_UNREGISTERED;
  if (strchr(spec,';')) {
    fd_index ix = NULL;
    u8_byte *copy = u8_strdup(spec);
    u8_byte *start = copy, *end = strchr(start,';');
    *end='\0'; while (start) {
      ix = fd_get_index(start,flags,opts);
      if (ix == NULL) {
        u8_free(copy); return NULL;}
      else fd_add_to_background(ix);
      if ((end) && (end[1])) {
        start = end+1; end = strchr(start,';');
        if (end) *end='\0';}
      else start = NULL;}
    u8_free(copy);
    return ix;}
  else {
    fd_index ix = fd_get_index(spec,flags,opts);
    if (ix) fd_add_to_background(ix);
    return ix;}
}

/* Core functions */

static void delay_index_fetch(fd_index ix,fdtype keys);

FD_EXPORT fdtype fd_index_fetch(fd_index ix,fdtype key)
{
  fdtype v;
  fd_hashtable cache = &(ix->index_cache);
  fd_hashtable adds = &(ix->index_adds);
  fd_hashtable edits = &(ix->index_edits);
  init_cache_level(ix);
  if (ix->index_edits.table_n_keys) {
    fdtype set_key = fd_make_pair(set_symbol,key);
    fdtype set_value = fd_hashtable_get(edits,set_key,VOID);
    fd_decref(set_key);
    if (!(VOIDP(set_value))) {
      if (ix->index_cache_level>0) fd_hashtable_store(cache,key,set_value);
      return set_value;}
    else {
      fdtype drop_key = fd_make_pair(drop_symbol,key);
      fdtype added = fd_hashtable_get(adds,key,EMPTY);
      fdtype dropped = fd_hashtable_get(edits,drop_key,EMPTY);
      fd_decref(drop_key);
      if (ix->index_handler->fetch)
        v = ix->index_handler->fetch(ix,key);
      else v = EMPTY;
      if (EMPTYP(dropped)) {
        CHOICE_ADD(v,added);}
      else {
        fdtype newv;
        CHOICE_ADD(v,added);
        newv = fd_difference(v,dropped);
        fd_decref(v); fd_decref(dropped);
        v = newv;}}}
  else if (ix->index_adds.table_n_keys) {
    fdtype added = fd_hashtable_get(&(ix->index_adds),key,EMPTY);
    if (ix->index_handler->fetch)
      v = ix->index_handler->fetch(ix,key);
    else v = EMPTY;
    CHOICE_ADD(v,added);}
  else if (ix->index_handler->fetch) {
    if ((fd_ipeval_status())&&
        ((ix->index_handler->prefetch)||
         (ix->index_handler->fetchn))) {
      delay_index_fetch(ix,key);
      v = EMPTY;}
    else v = ix->index_handler->fetch(ix,key);}
  else v = EMPTY;
  if (ix->index_cache_level>0) fd_hashtable_store(&(ix->index_cache),key,v);
  return v;
}

static void delay_index_fetch(fd_index ix,fdtype keys)
{
  struct FD_HASHTABLE *cache = &(ix->index_cache); int delay_count = 0;
  fdtype *delays = get_index_delays(), *delayp = &(delays[ix->index_serialno]);
  DO_CHOICES(key,keys) {
    if (fd_hashtable_probe(cache,key)) {}
    else {
      fd_incref(key);
      CHOICE_ADD((*delayp),key);
      delay_count++;}}
  if (delay_count) (void)fd_ipeval_delay(delay_count);
}

FD_EXPORT void fd_delay_index_fetch(fd_index ix,fdtype keys)
{
  delay_index_fetch(ix,keys);
}

FD_EXPORT int fd_index_prefetch(fd_index ix,fdtype keys)
{
  FDTC *fdtc = ((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  fdtype *keyvec = NULL, *values = NULL;
  fdtype lix = ((ix->index_serialno>=0)?
              (FDTYPE_IMMEDIATE(fd_index_type,ix->index_serialno)):
              ((fdtype)ix));
  int free_keys = 0, n_fetched = 0, cachelevel = 0;
  if (ix == NULL) return -1;
  else init_cache_level(ix);
  cachelevel = ix->index_cache_level;
  if (ix->index_handler->prefetch != NULL)
    if (fd_ipeval_status()) {
      delay_index_fetch(ix,keys);
      return 0;}
    else return ix->index_handler->prefetch(ix,keys);
  else if (ix->index_handler->fetchn == NULL) {
    if (fd_ipeval_status()) {
      delay_index_fetch(ix,keys);
      return 0;}
    else {
      DO_CHOICES(key,keys)
        if (!(fd_hashtable_probe(&(ix->index_cache),key))) {
          fdtype v = fd_index_fetch(ix,key);
          if (FD_ABORTP(v)) return fd_interr(v);
          n_fetched++;
          if (cachelevel>0) fd_hashtable_store(&(ix->index_cache),key,v);
          if (fdtc) fd_hashtable_store(&(fdtc->indexes),fd_make_pair(lix,key),v);
          fd_decref(v);}
      return n_fetched;}}
  if (PRECHOICEP(keys)) {keys = fd_make_simple_choice(keys); free_keys = 1;}
  if (fd_ipeval_status()) delay_index_fetch(ix,keys);
  else if (!(CHOICEP(keys))) {
    if (!(fd_hashtable_probe(&(ix->index_cache),keys))) {
      fdtype v = fd_index_fetch(ix,keys);
      if (FD_ABORTP(v)) return fd_interr(v);
      n_fetched = 1;
      if (cachelevel>0) fd_hashtable_store(&(ix->index_cache),keys,v);
      if (fdtc) fd_hashtable_store(&(fdtc->indexes),fd_make_pair(lix,keys),v);
      fd_decref(v);}}
  else {
    fdtype *write = NULL, singlekey = VOID;
    /* We first iterate over the keys to pick those that need to be fetched.
       If only one needs to be fetched, we will end up with write still NULL
       but with singlekey bound to that key. */
    struct FD_HASHTABLE *cache = &(ix->index_cache), *edits = &(ix->index_edits);
    if (edits->table_n_keys) {
      /* If there are any edits, we need to integrate them into whatever we
         prefetch. */
      DO_CHOICES(key,keys) {
        fdtype set_key = fd_make_pair(set_symbol,key);
        if (!((fd_hashtable_probe(cache,key)) ||
              (fd_hashtable_probe(edits,set_key)))) {
          if (write) *write++=key;
          else if (VOIDP(singlekey)) singlekey = key;
          else {
            write = keyvec = u8_alloc_n(FD_CHOICE_SIZE(keys),fdtype);
            write[0]=singlekey; write[1]=key; write = write+2;}}
        fd_decref(set_key);}}
    else {
      DO_CHOICES(key,keys)
        if (!(fd_hashtable_probe(cache,key))) {
          if (write) *write++=key;
          else if (VOIDP(singlekey)) singlekey = key;
          else {
            write = keyvec = u8_alloc_n(FD_CHOICE_SIZE(keys),fdtype);
            write[0]=singlekey; write[1]=key; write = write+2;}}}
    if (write == NULL)
      if (VOIDP(singlekey)) {}
      else {
        fdtype v = fd_index_fetch(ix,singlekey); n_fetched = 1;
        if (cachelevel>0) fd_hashtable_store(&(ix->index_cache),singlekey,v);
        if (fdtc) fd_hashtable_store
                    (&(fdtc->indexes),fd_make_pair(lix,singlekey),v);
        fd_decref(v);}
    else if (write == keyvec) n_fetched = 0;
    else {
      unsigned int i = 0, n = write-keyvec;
      n_fetched = n;
      values = ix->index_handler->fetchn(ix,n,keyvec);
      if (values == NULL) n_fetched = -1;
      else if (ix->index_edits.table_n_keys) /* When there are drops or sets */
        while (i < n) {
          fdtype key = keyvec[i];
          fdtype drop_key = fd_make_pair(drop_symbol,key);
          fdtype adds = fd_hashtable_get(&(ix->index_adds),key,EMPTY);
          fdtype drops=
            fd_hashtable_get(&(ix->index_edits),drop_key,EMPTY);
          fd_decref(drop_key);
          if (EMPTYP(drops))
            if (EMPTYP(adds)) {
              if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],values[i]);
              if (fdtc) fd_hashtable_store
                          (&(fdtc->indexes),fd_make_pair(lix,keyvec[i]),
                           values[i]);
              fd_decref(values[i]);}
            else {
              fdtype simple;
              CHOICE_ADD(values[i],adds);
              simple = fd_simplify_choice(values[i]);
              if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],simple);
              if (fdtc) fd_hashtable_store
                          (&(fdtc->indexes),fd_make_pair(lix,keyvec[i]),
                           simple);
              fd_decref(simple);}
          else {
            fdtype oldv = values[i], newv;
            CHOICE_ADD(oldv,adds);
            newv = fd_difference(oldv,drops);
            newv = fd_simplify_choice(newv);
            if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],newv);
            if (fdtc) fd_hashtable_store
                        (&(fdtc->indexes),fd_make_pair(lix,keyvec[i]),
                         newv);
            fd_decref(oldv); fd_decref(newv);}
          fd_decref(drops); i++;}
      else if (ix->index_adds.table_n_keys)  /* When there are just adds */
        while (i < n) {
          fdtype key = keyvec[i];
          fdtype adds = fd_hashtable_get(&(ix->index_adds),key,EMPTY);
          if (EMPTYP(adds)) {
            if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],values[i]);
            if (fdtc) fd_hashtable_store
                        (&(fdtc->indexes),fd_make_pair(lix,keyvec[i]),
                         values[i]);
            fd_decref(values[i]);}
          else {
            CHOICE_ADD(values[i],adds);
            values[i]=fd_simplify_choice(values[i]);
            if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],values[i]);
            if (fdtc) fd_hashtable_store
                        (&(fdtc->indexes),fd_make_pair(lix,keyvec[i]),
                         values[i]);
            fd_decref(values[i]);}
          i++;}
      else {
        /* When we're just storing. */
        if (fdtc) {
          int k = 0; while (k<n) {
            fd_hashtable_store(&(fdtc->indexes),fd_make_pair(lix,keyvec[k]),
                               values[k]);
            k++;}}
        if (cachelevel>0)
          fd_hashtable_iter(cache,fd_table_store_noref,n,keyvec,values);
        /* This is no longer needed with fd_table_store_noref */
        /* while (i < n) {fd_decref(values[i]); i++;} */
      }}}
  if (keyvec) {
    u8_free(keyvec);
    if (values) u8_free(values);}
  if (free_keys) fd_decref(keys);
  return n_fetched;
}

static int add_key_fn(fdtype key,fdtype val,void *data)
{
  fdtype **write = (fdtype **)data;
  *((*write)++) = fd_incref(key);
  /* Means keep going */
  return 0;
}

static int edit_key_fn(fdtype key,fdtype val,void *data)
{
  fdtype **write = (fdtype **)data;
  if (PAIRP(key)) {
    struct FD_PAIR *pair = (struct FD_PAIR *)key;
    if (pair->car == set_symbol) {
      fdtype real_key = pair->cdr;
      fd_incref(real_key);
      *((*write)++) = real_key;}}
  /* Means keep going */
  return 0;
}

FD_EXPORT fdtype fd_index_keys(fd_index ix)
{
  if (ix->index_handler->fetchkeys) {
    int n_fetched = 0;
    fdtype *fetched = ix->index_handler->fetchkeys(ix,&n_fetched);
    if ((n_fetched==0) && (ix->index_edits.table_n_keys == 0) &&
        (ix->index_adds.table_n_keys == 0))
      return EMPTY;
    else if ((n_fetched==0) && (ix->index_edits.table_n_keys == 0))
      return fd_hashtable_keys(&(ix->index_adds));
    else if ((n_fetched==0) && (ix->index_adds.table_n_keys == 0))
      return fd_hashtable_keys(&(ix->index_edits));
    else {
      fd_choice result; int n_total;
      fdtype *write_start, *write_at;
      n_total = n_fetched+ix->index_adds.table_n_keys+
        ix->index_edits.table_n_keys;
      result = fd_alloc_choice(n_total);
      if (n_fetched)
        memcpy(&(result->choice_0),fetched,sizeof(fdtype)*n_fetched);
      write_start = &(result->choice_0); write_at = write_start+n_fetched;
      if (ix->index_adds.table_n_keys)
        fd_for_hashtable(&(ix->index_adds),add_key_fn,&write_at,1);
      if (ix->index_edits.table_n_keys)
        fd_for_hashtable(&(ix->index_edits),edit_key_fn,&write_at,1);
      u8_free(fetched);
      return fd_init_choice(result,write_at-write_start,NULL,
                            FD_CHOICE_DOSORT|FD_CHOICE_REALLOC);}}
  else return fd_err(fd_NoMethod,"fd_index_keys",NULL,fd_index2lisp(ix));
}

static int copy_value_sizes(fdtype key,fdtype value,void *vptr)
{
  struct FD_HASHTABLE *sizes = (struct FD_HASHTABLE *)vptr;
  int value_size = ((VOIDP(value)) ? (0) : FD_CHOICE_SIZE(value));
  fd_hashtable_store(sizes,key,FD_INT(value_size));
  return 0;
}

FD_EXPORT fdtype fd_index_keysizes(fd_index ix,fdtype for_keys)
{
  if (ix->index_handler->fetchinfo) {
    fdtype keys=for_keys;
    int n_fetched = 0, decref_keys;
    struct FD_CHOICE *filter=NULL;
    if (PRECHOICEP(for_keys)) {
      keys=fd_make_simple_choice(for_keys);
      decref_keys=1;}
    if (EMPTYP(keys))
      return EMPTY;
    else if (!(CHOICEP(keys))) {
      fdtype v = fd_index_get(ix,keys);
      unsigned int size = FD_CHOICE_SIZE(v);
      fdtype result = fd_init_pair
        ( NULL, fd_incref(keys), FD_INT(size) );
      if (decref_keys) fd_decref(keys);
      fd_decref(v);
      return result;}
    struct FD_KEY_SIZE *fetched =
      ix->index_handler->fetchinfo(ix,(fd_choice)keys,&n_fetched);
    if ((n_fetched==0) && (ix->index_adds.table_n_keys==0))
      return EMPTY;
    else if ((ix->index_adds.table_n_keys)==0) {
      fd_choice result = fd_alloc_choice(n_fetched);
      fdtype *write = &(result->choice_0);
      int i = 0; while (i<n_fetched) {
        fdtype key = fetched[i].keysizekey, pair;
        unsigned int n_values = fetched[i].keysizenvals;
        pair = fd_conspair(fd_incref(key),FD_INT(n_values));
        *write++=pair; i++;}
      u8_free(fetched);
      return fd_init_choice(result,n_fetched,NULL,
                            FD_CHOICE_DOSORT|FD_CHOICE_REALLOC);}
    else {
      struct FD_HASHTABLE added_sizes;
      fd_choice result; int i = 0, n_total; fdtype *write;
      /* Get the sizes for added keys. */
      FD_INIT_STATIC_CONS(&added_sizes,fd_hashtable_type);
      fd_make_hashtable(&added_sizes,ix->index_adds.ht_n_buckets);
      fd_for_hashtable(&(ix->index_adds),copy_value_sizes,&added_sizes,1);
      n_total = n_fetched+ix->index_adds.table_n_keys;
      result = fd_alloc_choice(n_total); write = &(result->choice_0);
      while (i<n_fetched) {
        fdtype key = fetched[i].keysizekey, pair;
        unsigned int n_values = fetched[i].keysizenvals;
        fdtype added = fd_hashtable_get(&added_sizes,key,FD_INT(0));
        n_values = n_values+fd_getint(added); fd_decref(added);
        pair = fd_conspair(fd_incref(key),FD_INT(n_values));
        *write++=pair; i++;}
      fd_recycle_hashtable(&added_sizes); u8_free(fetched);
      return fd_init_choice(result,n_total,NULL,
                            FD_CHOICE_DOSORT|FD_CHOICE_REALLOC);}}
  else return fd_err(fd_NoMethod,"fd_index_keys",NULL,fd_index2lisp(ix));
}

FD_EXPORT fdtype fd_index_sizes(fd_index ix)
{
  return fd_index_keysizes(ix,VOID);
}

FD_EXPORT fdtype _fd_index_get(fd_index ix,fdtype key)
{
  fdtype cached; struct FD_PAIR tempkey;
  FDTC *fdtc = fd_threadcache;
  if (fdtc) {
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.car = fd_index2lisp(ix); tempkey.cdr = key;
    cached = fd_hashtable_get(&(fdtc->indexes),(fdtype)&tempkey,VOID);
    if (!(VOIDP(cached))) return cached;}
  if (ix->index_cache_level==0) cached = VOID;
  else if ((PAIRP(key)) && (!(VOIDP(ix->index_covers_slotids))) &&
      (!(atomic_choice_containsp(FD_CAR(key),ix->index_covers_slotids))))
    return EMPTY;
  else cached = fd_hashtable_get(&(ix->index_cache),key,VOID);
  if (VOIDP(cached)) cached = fd_index_fetch(ix,key);
#if FD_USE_THREADCACHE
  if (fdtc) {
    fd_hashtable_store(&(fdtc->indexes),(fdtype)&tempkey,cached);}
#endif
  return cached;
}
static fdtype table_indexget(fdtype ixarg,fdtype key,fdtype dflt)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix) {
    fdtype v = fd_index_get(ix,key);
    if (EMPTYP(v)) return fd_incref(dflt);
    else return v;}
  else return FD_ERROR;
}


static void extend_slotids(fd_index ix,const fdtype *keys,int n)
{
  fdtype slotids = ix->index_covers_slotids;
  int i = 0; while (i<n) {
    fdtype key = keys[i++], slotid;
    if (PAIRP(key)) slotid = FD_CAR(key); else continue;
    if ((OIDP(slotid)) || (SYMBOLP(slotid))) {
      if (atomic_choice_containsp(slotid,slotids)) continue;
      else {fd_decref(slotids); ix->index_covers_slotids = VOID;}}}
}

FD_EXPORT int _fd_index_add(fd_index ix,fdtype key,fdtype value)
{
  FDTC *fdtc = fd_threadcache;
  fd_hashtable adds = &(ix->index_adds), cache = &(ix->index_cache);
  if (U8_BITP(ix->index_flags,FD_STORAGE_READ_ONLY)) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_add",u8_strdup(ix->indexid),VOID);
    return -1;}
  else init_cache_level(ix);
  if (EMPTYP(value)) return 0;
  else if ((CHOICEP(key)) || (PRECHOICEP(key))) {
    fdtype keys = fd_make_simple_choice(key);
    const fdtype *keyv = FD_CHOICE_DATA(keys);
    unsigned int n = FD_CHOICE_SIZE(keys);
    fd_hashtable_iterkeys(adds,fd_table_add,n,keyv,value);
    if (!(VOIDP(ix->index_covers_slotids))) extend_slotids(ix,keyv,n);
    if ((FD_WRITETHROUGH_THREADCACHE)&&(fdtc)) {
      DO_CHOICES(k,key) {
        struct FD_PAIR tempkey;
        FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
        tempkey.car = fd_index2lisp(ix); tempkey.cdr = k;
        if (fd_hashtable_probe(&(fdtc->indexes),(fdtype)&tempkey)) {
          fd_hashtable_add(&(fdtc->indexes),(fdtype)&tempkey,value);}}}

    if (ix->index_cache_level>0)
      fd_hashtable_iterkeys(cache,fd_table_add_if_present,n,keyv,value);
    fd_decref(keys);}
  else {
    fd_hashtable_add(adds,key,value);
    fd_hashtable_op(cache,fd_table_add_if_present,key,value);}

  if ((fdtc)&&(FD_WRITETHROUGH_THREADCACHE)) {
    DO_CHOICES(k,key) {
      struct FD_PAIR tempkey;
      FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
      tempkey.car = fd_index2lisp(ix); tempkey.cdr = k;
      if (fd_hashtable_probe(&(fdtc->indexes),(fdtype)&tempkey)) {
        fd_hashtable_add(&(fdtc->indexes),(fdtype)&tempkey,value);}}}

  if ( (ix->index_flags&FD_INDEX_IN_BACKGROUND) &&
       (fd_background->index_cache.table_n_keys) && 
         (fd_background->index_cache.table_n_keys) ) {
    fd_hashtable bgcache = &(fd_background->index_cache);
    if (CHOICEP(key)) {
      const fdtype *keys = FD_CHOICE_DATA(key);
      unsigned int n = FD_CHOICE_SIZE(key);
      fd_hashtable_iterkeys(bgcache,fd_table_replace,n,keys,VOID);}
    else fd_hashtable_op(bgcache,fd_table_replace,key,VOID);}

  if (!(VOIDP(ix->index_covers_slotids))) extend_slotids(ix,&key,1);

  return 1;
}

static int table_indexadd(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix) return fd_index_add(ix,key,value);
  else return -1;
}

FD_EXPORT int fd_index_drop(fd_index ix,fdtype key,fdtype value)
{
  fd_hashtable cache = &(ix->index_cache);
  fd_hashtable edits = &(ix->index_edits);
  fd_hashtable adds = &(ix->index_adds);
  fd_hashtable bg_cache = &(fd_background->index_cache);
  if (U8_BITP(ix->index_flags,FD_STORAGE_READ_ONLY)) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_add",
              u8_strdup(ix->indexid),VOID);
    return -1;}
  else init_cache_level(ix);
  if ((CHOICEP(key)) || (PRECHOICEP(key))) {
    DO_CHOICES(eachkey,key) {
      fdtype drop_key = fd_make_pair(drop_symbol,eachkey);
      fdtype set_key = fd_make_pair(set_symbol,eachkey);
      fd_hashtable_add(edits,drop_key,value);
      fd_hashtable_drop(edits,set_key,value);
      fd_hashtable_drop(adds,eachkey,value);
      if (ix->index_cache_level>0)
        fd_hashtable_drop(cache,eachkey,value);
      fd_decref(set_key); 
      fd_decref(drop_key);}}
  else {
    fdtype drop_key = fd_make_pair(drop_symbol,key);
    fdtype set_key = fd_make_pair(set_symbol,key);
    fd_hashtable_add(edits,drop_key,value);
    fd_hashtable_drop(edits,set_key,value);
    fd_hashtable_drop(adds,key,value);
    if (ix->index_cache_level>0)
      fd_hashtable_drop(cache,key,value);
    fd_decref(set_key);
    fd_decref(drop_key);}
  if ((ix->index_flags&FD_INDEX_IN_BACKGROUND) &&
      (fd_background->index_cache.table_n_keys)) {
    if (CHOICEP(key)) {
      const fdtype *keys = FD_CHOICE_DATA(key);
      unsigned int n = FD_CHOICE_SIZE(key);
      fd_hashtable_iterkeys
        (bg_cache,fd_table_replace,n,keys,VOID);}
    else fd_hashtable_op(bg_cache,fd_table_replace,key,VOID);}
  return 1;
}
static int table_indexdrop(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix = fd_indexptr(ixarg);
  if (!(ix)) return -1;
  else if (VOIDP(value))
    return fd_index_store(ix,key,EMPTY);
  else return fd_index_drop(ix,key,value);
}

FD_EXPORT int fd_index_store(fd_index ix,fdtype key,fdtype value)
{
  fd_hashtable cache = &(ix->index_cache);
  fd_hashtable edits = &(ix->index_edits);
  fd_hashtable adds = &(ix->index_adds);
  fd_hashtable bg_cache = &(fd_background->index_cache);
  if (U8_BITP(ix->index_flags,FD_STORAGE_READ_ONLY)) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_store",
              u8_strdup(ix->indexid),VOID);
    return -1;}
  else init_cache_level(ix);
  if ((CHOICEP(key))||(PRECHOICEP(key))) {
    DO_CHOICES(eachkey,key) {
      fdtype set_key = fd_make_pair(set_symbol,eachkey);
      fdtype drop_key = fd_make_pair(drop_symbol,eachkey);
      fd_hashtable_store(edits,set_key,value);
      fd_hashtable_drop(edits,drop_key,VOID);
      if (ix->index_cache_level>0) {
        fd_hashtable_store(cache,eachkey,value);
        fd_hashtable_drop(adds,eachkey,VOID);}
      fd_decref(set_key); 
      fd_decref(drop_key);}}
  else {
    fdtype set_key = fd_make_pair(set_symbol,key);
    fdtype drop_key = fd_make_pair(drop_symbol,key);
    fd_hashtable_store(edits,set_key,value);
    fd_hashtable_drop(edits,drop_key,VOID);
    if (ix->index_cache_level>0) {
      fd_hashtable_store(cache,key,value);
      fd_hashtable_drop(adds,key,VOID);}
    fd_decref(set_key);
    fd_decref(drop_key);}
  if ((ix->index_flags&FD_INDEX_IN_BACKGROUND) &&
      (fd_background->index_cache.table_n_keys)) {
    if (CHOICEP(key)) {
      const fdtype *keys = FD_CHOICE_DATA(key);
      unsigned int n = FD_CHOICE_SIZE(key);
      fd_hashtable_iterkeys(bg_cache,fd_table_replace,n,keys,VOID);}
    else fd_hashtable_op(bg_cache,fd_table_replace,key,VOID);}
  return 1;
}

static int table_indexstore(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix) return fd_index_store(ix,key,value);
  else return -1;
}

static int merge_kv_into_adds(struct FD_KEYVAL *kv,void *data)
{
  struct FD_HASHTABLE *adds = (fd_hashtable) data;
  fd_hashtable_op_nolock(adds,fd_table_add,kv->kv_key,kv->kv_val);
  return 0;
}

FD_EXPORT int fd_index_merge(fd_index ix,fd_hashtable table)
{
  /* Ignoring this for now */
  fd_hashtable adds = &(ix->index_adds);
  if (U8_BITP(ix->index_flags,FD_STORAGE_READ_ONLY)) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_store",
              u8_strdup(ix->indexid),VOID);
    return -1;}
  else init_cache_level(ix);
  fd_write_lock_table(adds);
  fd_read_lock_table(table);
  fd_for_hashtable_kv(table,merge_kv_into_adds,(void *)adds,0);
  fd_unlock_table(table);
  fd_unlock_table(adds);
  return 1;
}

FD_EXPORT int fd_batch_add(fd_index ix,fdtype table)
{
  if (U8_BITP(ix->index_flags,FD_STORAGE_READ_ONLY)) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_store",
              u8_strdup(ix->indexid),VOID);
    return -1;}
  else init_cache_level(ix);
  if (HASHTABLEP(table))
    return fd_index_merge(ix,(fd_hashtable)table);
  else if (TABLEP(table)) {
    fd_hashtable adds = &(ix->index_adds);
    fdtype allkeys = fd_getkeys(table);
    int i = 0, n_keys = FD_CHOICE_SIZE(allkeys), atomic = 1;
    fdtype *keys = u8_alloc_n(n_keys,fdtype);
    fdtype *values = u8_alloc_n(n_keys,fdtype);
    DO_CHOICES(key,allkeys) {
      fdtype v = fd_get(table,key,VOID);
      if (!(VOIDP(v))) {
        if (CONSP(v)) atomic = 0;
        keys[i]=key; values[i]=v; i++;}}
    fd_hashtable_iter(adds,fd_table_add,i,keys,values);
    if (!(atomic)) for (int j = 0;j<i;j++) { fd_decref(values[j]); }
    u8_free(keys);
    u8_free(values);
    fd_decref(allkeys);
    return i;}
  else {
    fd_seterr(fd_TypeError,"fd_batch_add","Not a table",table);
    return -1;}
}

static fdtype table_indexkeys(fdtype ixarg)
{
  fd_index ix = fd_indexptr(ixarg);
  if (ix) return fd_index_keys(ix);
  else return fd_type_error(_("index"),"table_index_keys",ixarg);
}

FD_EXPORT int fd_index_commit(fd_index ix)
{
  if (ix == NULL) return -1;
  else init_cache_level(ix);
  if ((ix->index_adds.ht_n_buckets) || (ix->index_edits.ht_n_buckets)) {
    int n_edits = ix->index_edits.table_n_keys;
    int n_adds = ix->index_adds.table_n_keys;
    int n_keys = n_edits+n_adds, retval = 0;
    if (n_keys==0) return 0;
    u8_log(fd_storage_loglevel+1,fd_IndexCommit,
           "####### Saving %d updates to %s",n_keys,ix->indexid);
    double start_time = u8_elapsed_time();
    if (ix->index_cache_level<0) {
      fd_index_setcache(ix,fd_default_cache_level);}
    retval = ix->index_handler->commit(ix);
    if (retval<0)
      u8_log(LOG_CRIT,fd_IndexCommitError,
             _("!!!!!!! Error saving %d keys to %s after %f secs"),
             n_keys,ix->indexid,u8_elapsed_time()-start_time);
    else if (retval>0)
      u8_log(fd_storage_loglevel,fd_IndexCommit,
             _("####### Saved %d updated keys to %s in %f secs"),
             retval,ix->indexid,u8_elapsed_time()-start_time);
    else {}
    if (retval<0)
      u8_seterr(fd_IndexCommitError,"fd_index_commit",
                u8_strdup(ix->indexid));
    return retval;}
  else return 0;
}

FD_EXPORT void fd_index_swapout(fd_index ix,fdtype keys)
{
  struct FD_HASHTABLE *cache = &(ix->index_cache);
  if (((ix->index_flags)&FD_INDEX_NOSWAP) || (cache->table_n_keys==0))
    return;
  else if (VOIDP(keys)) {
    if ((ix->index_flags)&(FD_STORAGE_KEEP_CACHESIZE))
      fd_reset_hashtable(&(ix->index_cache),-1,1);
    else fd_reset_hashtable(&(ix->index_cache),0,1);}
  else if (CHOICEP(keys)) {
    struct FD_CHOICE *ch = (fd_choice)keys;
    fd_hashtable_iterkeys(cache,fd_table_replace,ch->choice_size,
                          FD_XCHOICE_DATA(ch),VOID);
    fd_devoid_hashtable(cache,0);
    return;}
  else if (PRECHOICEP(keys)) {
    fdtype simplified = fd_make_simple_choice(keys);
    fd_index_swapout(ix,simplified);
    fd_decref(simplified);}
  else {
    fd_hashtable_store(&(ix->index_cache),keys,VOID);}
}

FD_EXPORT void fd_index_close(fd_index ix)
{
  if ((ix) && (ix->index_handler) && (ix->index_handler->close))
    ix->index_handler->close(ix);
}

/* Common init function */

FD_EXPORT void fd_init_index(fd_index ix,
                             struct FD_INDEX_HANDLER *h,
                             u8_string id,u8_string src,
                             fd_storage_flags flags)
{
  U8_SETBITS(flags,FD_STORAGE_ISINDEX);
  if (U8_BITP(flags,FD_STORAGE_UNREGISTERED)) {
    FD_INIT_CONS(ix,fd_consed_index_type);
    add_consed_index(ix);}
  else {FD_INIT_STATIC_CONS(ix,fd_consed_index_type);}
  if (U8_BITP(flags,FD_STORAGE_READ_ONLY)) {
    U8_SETBITS(flags,FD_STORAGE_READ_ONLY); };
  ix->index_serialno = -1; ix->index_cache_level = -1; ix->index_flags = flags;
  FD_INIT_STATIC_CONS(&(ix->index_cache),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(ix->index_adds),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(ix->index_edits),fd_hashtable_type);
  fd_make_hashtable(&(ix->index_cache),fd_index_cache_init);
  fd_make_hashtable(&(ix->index_adds),0);
  fd_make_hashtable(&(ix->index_edits),0);
  ix->index_handler = h;
  /* This was what was specified */
  ix->indexid = u8_strdup(id);
  /* Don't copy this one */
  ix->index_source = src;
  ix->index_covers_slotids = VOID;
}

FD_EXPORT void fd_reset_index_tables
  (fd_index ix,ssize_t csize,ssize_t esize,ssize_t asize)
{
  int readonly = U8_BITP(ix->index_flags,FD_STORAGE_READ_ONLY);
  fd_hashtable cache = &(ix->index_cache);
  fd_hashtable edits = &(ix->index_edits);
  fd_hashtable adds = &(ix->index_adds);
  fd_reset_hashtable(cache,((csize==0)?(fd_index_cache_init):(csize)),1);
  if (edits->table_n_keys==0) {
    ssize_t level = (readonly)?(0):(esize==0)?(fd_index_edits_init):(esize);
    fd_reset_hashtable(edits,level,1);}
  if (adds->table_n_keys==0) {
    ssize_t level = (readonly)?(0):(asize==0)?(fd_index_adds_init):(asize);
    fd_reset_hashtable(adds,level,1);}
}


static void display_index(u8_output out,fd_index ix,fdtype lix)
{
  u8_byte numbuf[32], edits[64];
  u8_string tag = (CONSP(lix)) ? ("CONSINDEX") : ("INDEX");
  u8_string type = ((ix->index_handler) && (ix->index_handler->name)) ?
    (ix->index_handler->name) : ((u8_string)("notype"));
  if (ix->index_edits.table_n_keys) {
    strcpy(edits,"~");
    strcat(edits,u8_itoa10(ix->index_edits.table_n_keys,numbuf));}
  else strcpy(edits,"");
  if (ix->index_source)
    u8_printf(out,_("#<%s %s (%s) cx=%d+%d%s #!%lx \"%s\">"),
              tag,type,ix->indexid,
              ix->index_cache.table_n_keys,
              ix->index_adds.table_n_keys,
              edits,
              lix,ix->index_source);
  else u8_printf(out,_("#<%s %s (%s) cx=%d+%d~%d #!%lx>"),
                 tag,ix->indexid,type,
                 lix);
}
static int unparse_index(u8_output out,fdtype x)
{
  fd_index ix = fd_indexptr(x);
  if (ix == NULL) return 0;
  display_index(out,ix,x);
  return 1;
}

static int unparse_consed_index(u8_output out,fdtype x)
{
  fd_index ix = (fd_index)(x);
  if (ix == NULL) return 0;
  if (ix == NULL) return 0;
  display_index(out,ix,x);
  return 1;
}

static fdtype index_parsefn(int n,fdtype *args,fd_compound_typeinfo e)
{
  fd_index ix = NULL;
  if (n<2) return VOID;
  else if (STRINGP(args[2]))
    ix = fd_get_index(FD_STRING_DATA(args[2]),0,VOID);
  if (ix) return fd_index2lisp(ix);
  else return fd_err(fd_CantParseRecord,"index_parsefn",NULL,VOID);
}

/* Operations over all indexes */

FD_EXPORT int fd_for_indexes(int (*fcn)(fd_index ix,void *),void *data)
{
  if (n_consed_indexes) {
    u8_lock_mutex(&consed_indexes_lock);
    int j = 0; while (j<n_consed_indexes) {
      fd_index ix = consed_indexes[j++];
      int retval = fcn(ix,data);
      if (retval) u8_unlock_mutex(&consed_indexes_lock);
      if (retval<0) return retval;
      else if (retval) break;
      else j++;}}
  int i = 0; while (i < fd_n_primary_indexes) {
    int retval = fcn(fd_primary_indexes[i],data);
    if (retval<0) return retval;
    else if (retval) break;
    else i++;}
  if (i>=fd_n_primary_indexes) return i;
  i = 0; while (i < fd_n_secondary_indexes) {
    int retval = fcn(fd_secondary_indexes[i],data);
    if (retval<0) return retval;
    else if (retval) break;
    else i++;}
  return i;
}

static int swapout_index_handler(fd_index ix,void *data)
{
  fd_index_swapout(ix,VOID);
  return 0;
}


FD_EXPORT void fd_swapout_indexes()
{
  fd_for_indexes(swapout_index_handler,NULL);
}

static int close_index_handler(fd_index ix,void *data)
{
  fd_index_close(ix);
  return 0;
}

FD_EXPORT void fd_close_indexes()
{
  fd_for_indexes(close_index_handler,NULL);
}

static int commit_index_handler(fd_index ix,void *data)
{
  int *count = (int *) data;
  int retval = fd_index_commit(ix);
  if (retval<0)
    return retval;
  else *count += retval;
  return 0;
}

static int commit_index_noerr_handler(fd_index ix,void *data)
{
  int *count = (int *) data;
  int retval = fd_index_commit(ix);
  if (retval<0) {
    u8_log(LOG_CRIT,"INDEX_COMMIT_FAIL","Error committing %s",ix->indexid);
    fd_clear_errors(1);
    *count=-1;}
  else if ( *count >= 0 )
    *count += retval;
  else {}
  return 0;
}

FD_EXPORT int fd_commit_indexes()
{
  int count=0;
  int rv=fd_for_indexes(commit_index_handler,&count);
  if (rv<0) return rv;
  else return count;
}

FD_EXPORT int fd_commit_indexes_noerr()
{
  int count=0;
  int rv=fd_for_indexes(commit_index_handler,&count);
  if (rv<0) return rv;
  else return count;
}

static int accumulate_cachecount(fd_index ix,void *ptr)
{
  int *count = (int *)ptr;
  *count = *count+ix->index_cache.table_n_keys;
  return 0;
}

FD_EXPORT
long fd_index_cache_load()
{
  int result = 0, retval;
  retval = fd_for_indexes(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(fd_index ix,void *ptr)
{
  fdtype *vals = (fdtype *)ptr;
  fdtype keys = fd_hashtable_keys(&(ix->index_cache));
  CHOICE_ADD(*vals,keys);
  return 0;
}

FD_EXPORT
fdtype fd_cached_keys(fd_index ix)
{
  if (ix == NULL) {
    int retval; fdtype result = EMPTY;
    retval = fd_for_indexes(accumulate_cached,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return FD_ERROR;}
    else return result;}
  else return fd_hashtable_keys(&(ix->index_cache));
}

/* IPEVAL delay execution */

FD_EXPORT int fd_execute_index_delays(fd_index ix,void *data)
{
  fdtype *delays = get_index_delays();
  fdtype todo = delays[ix->index_serialno];
  if (EMPTYP(todo)) return 0;
  else {
    int retval = -1;
    /* u8_lock_mutex(&(fd_ipeval_lock)); */
    todo = delays[ix->index_serialno];
    delays[ix->index_serialno]=EMPTY;
    /* u8_unlock_mutex(&(fd_ipeval_lock)); */
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval>1)
      u8_log(LOG_NOTICE,ipeval_ixfetch,"Fetching %d keys from %s: %q",
             FD_CHOICE_SIZE(todo),ix->indexid,todo);
    else
#endif
    retval = fd_index_prefetch(ix,todo);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      u8_log(LOG_NOTICE,ipeval_ixfetch,"Fetched %d keys from %s",
             FD_CHOICE_SIZE(todo),ix->indexid);
#endif
    if (retval<0) return retval;
    else return 0;}
}

static void recycle_consed_index(struct FD_RAW_CONS *c)
{
  struct FD_INDEX *ix = (struct FD_INDEX *)c;
  struct FD_INDEX_HANDLER *handler = ix->index_handler;
  drop_consed_index(ix);
  if (handler->recycle) handler->recycle(ix);
  fd_recycle_hashtable(&(ix->index_cache));
  fd_recycle_hashtable(&(ix->index_adds));
  fd_recycle_hashtable(&(ix->index_edits));
  u8_free(ix->indexid);
  u8_free(ix->index_source);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

static fdtype copy_consed_index(fdtype x,int deep)
{
  /* Where might this get us into trouble when not really copying the pool? */
  fd_index ix = (fd_index)x;
  if (ix->index_serialno>=0)
    return fd_index2lisp(ix);
  else return x;
}

/* Initialize */

fd_ptr_type fd_consed_index_type;

static int check_index(fdtype x)
{
  int serial = FD_GET_IMMEDIATE(x,fd_index_type);
  if (serial<0) return 0;
  if (serial<FD_N_PRIMARY_INDEXES)
    if (fd_primary_indexes[serial]) return 1;
    else return 0;
  else if (fd_secondary_indexes) {
    int second_off=serial-FD_N_PRIMARY_INDEXES;
    if ( (second_off<0) || (second_off  > fd_n_secondary_indexes) )
      return 0;
    else return (fd_secondary_indexes[second_off]!=NULL);}
  else return 0;
}

FD_EXPORT fd_index _fd_indexptr(fdtype x)
{
  return fd_indexptr(x);
}

/* Initializations */

FD_EXPORT void fd_init_indexes_c()
{
  u8_register_source_file(_FILEINFO);

  fd_type_names[fd_index_type]=_("index");
  fd_immediate_checkfns[fd_index_type]=check_index;

  fd_consed_index_type = fd_register_cons_type("raw index");
  fd_type_names[fd_consed_index_type]=_("raw index");

  u8_init_mutex(&consed_indexes_lock);
  consed_indexes = u8_malloc(64*sizeof(fd_index));
  consed_indexes_len=65;

  fd_index_hashop=fd_intern("HASH");
  fd_index_slotsop=fd_intern("SLOTIDS");
  
  {
    struct FD_COMPOUND_TYPEINFO *e =
      fd_register_compound(fd_intern("INDEX"),NULL,NULL);
    e->fd_compound_parser = index_parsefn;}

  fd_tablefns[fd_index_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_index_type]->get = table_indexget;
  fd_tablefns[fd_index_type]->add = table_indexadd;
  fd_tablefns[fd_index_type]->drop = table_indexdrop;
  fd_tablefns[fd_index_type]->store = table_indexstore;
  fd_tablefns[fd_index_type]->test = NULL;
  fd_tablefns[fd_index_type]->keys = table_indexkeys;
  fd_tablefns[fd_index_type]->getsize = NULL;

  fd_tablefns[fd_consed_index_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_consed_index_type]->get = table_indexget;
  fd_tablefns[fd_consed_index_type]->add = table_indexadd;
  fd_tablefns[fd_consed_index_type]->drop = table_indexdrop;
  fd_tablefns[fd_consed_index_type]->store = table_indexstore;
  fd_tablefns[fd_consed_index_type]->test = NULL;
  fd_tablefns[fd_consed_index_type]->keys = table_indexkeys;
  fd_tablefns[fd_consed_index_type]->getsize = NULL;

  fd_recyclers[fd_consed_index_type]=recycle_consed_index;
  fd_unparsers[fd_consed_index_type]=unparse_consed_index;
  fd_copiers[fd_consed_index_type]=copy_consed_index;

  set_symbol = fd_make_symbol("SET",3);
  drop_symbol = fd_make_symbol("DROP",4);
  fd_unparsers[fd_index_type]=unparse_index;

  u8_init_mutex(&indexes_lock);
  u8_init_mutex(&background_lock);

#if (FD_USE_TLS)
  u8_new_threadkey(&index_delays_key,NULL);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
