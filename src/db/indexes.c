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
#include "framerd/fdb.h"
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

u8_condition fd_IndexCommit=_("Index/Commit");
static u8_condition ipeval_ixfetch="IXFETCH";

fd_index (*fd_file_index_type)(u8_string,fdb_flags)=NULL;

int fd_index_cache_init  = FD_INDEX_CACHE_INIT;
int fd_index_edits_init  = FD_INDEX_EDITS_INIT;
int fd_index_adds_init   = FD_INDEX_ADDS_INIT;

fd_index fd_primary_indexes[FD_N_PRIMARY_INDEXES], *fd_secondary_indexes=NULL;
int fd_n_primary_indexes=0, fd_n_secondary_indexes=0;

struct FD_COMPOUND_INDEX *fd_background=NULL;
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
  fdtype *delays=(fdtype *)u8_tld_get(index_delays_key);
  if (delays) return;
  else {
    int i=0;
    delays=u8_alloc_n(FD_N_INDEX_DELAYS,fdtype);
    while (i<FD_N_INDEX_DELAYS) delays[i++]=FD_EMPTY_CHOICE;
    u8_tld_set(index_delays_key,delays);}

}
#else
#define get_index_delays() (index_delays)
FD_EXPORT void fd_init_index_delays()
{
  if (index_delays) return;
  else {
    int i=0;
    index_delays=u8_alloc_n(FD_N_INDEX_DELAYS,fdtype);
    while (i<FD_N_INDEX_DELAYS) index_delays[i++]=FD_EMPTY_CHOICE;}

}
#endif

FD_EXPORT fdtype *fd_get_index_delays() { return get_index_delays(); }

static fdtype set_symbol, drop_symbol;

static u8_mutex indexes_lock;

/* Index cache levels */

/* a cache_level<0 indicates no caching has been done */

FD_EXPORT void fd_index_setcache(fd_index ix,int level)
{
  if (ix->index_handler->setcache) ix->index_handler->setcache(ix,level);
  ix->index_cache_level=level;
}

static void init_cache_level(fd_index ix)
{
  if (FD_EXPECT_FALSE(ix->index_cache_level<0)) {
    ix->index_cache_level=fd_default_cache_level;
    if (ix->index_handler->setcache)
      ix->index_handler->setcache(ix,fd_default_cache_level);}
}


/* The index registry */

FD_EXPORT void fd_register_index(fd_index ix)
{
  if (ix->index_flags&FDB_UNREGISTERED)
    return;
  else if (ix->index_serialno<0) {
    u8_lock_mutex(&indexes_lock);
    if (ix->index_serialno>=0) { /* Handle race condition */
      u8_unlock_mutex(&indexes_lock); return;}
    if (fd_n_primary_indexes<FD_N_PRIMARY_INDEXES) {
      ix->index_serialno=fd_n_primary_indexes;
      fd_primary_indexes[fd_n_primary_indexes++]=ix;}
    else {
      if (fd_secondary_indexes)
        fd_secondary_indexes=u8_realloc_n
          (fd_secondary_indexes,fd_n_secondary_indexes+1,fd_index);
      else fd_secondary_indexes=u8_alloc_n(1,fd_index);
      ix->index_serialno=fd_n_secondary_indexes+FD_N_PRIMARY_INDEXES;
      fd_secondary_indexes[fd_n_secondary_indexes++]=ix;}
    u8_unlock_mutex(&indexes_lock);}
}

FD_EXPORT fdtype fd_index2lisp(fd_index ix)
{
  if (ix==NULL)
    return FD_ERROR_VALUE;
  else if (ix->index_serialno>=0)
    return FDTYPE_IMMEDIATE(fd_index_type,ix->index_serialno);
  else return fd_incref((fdtype)ix);
}
FD_EXPORT fd_index fd_lisp2index(fdtype lix)
{
  if (FD_ABORTP(lix)) return NULL;
  else if (FD_TYPEP(lix,fd_index_type)) {
    int serial=FD_GET_IMMEDIATE(lix,fd_index_type);
    if (serial<FD_N_PRIMARY_INDEXES) return fd_primary_indexes[serial];
    else return fd_secondary_indexes[serial-FD_N_PRIMARY_INDEXES];}
  else if (FD_TYPEP(lix,fd_raw_index_type))
    return (fd_index) lix;
  else if (FD_STRINGP(lix))
    return fd_get_index(FD_STRDATA(lix),0);
  else {
    fd_seterr(fd_TypeError,_("not an index"),NULL,lix);
    return NULL;}
}

FD_EXPORT fd_index fd_find_index_by_cid(u8_string cid)
{
  int i=0;
  if (cid == NULL) return NULL;
  else while (i<fd_n_primary_indexes)
    if (strcmp(cid,fd_primary_indexes[i]->index_idstring)==0)
      return fd_primary_indexes[i];
    else i++;
  if (fd_secondary_indexes == NULL) return NULL;
  i=0; while (i<fd_n_secondary_indexes)
    if (strcmp(cid,fd_secondary_indexes[i]->index_idstring)==0)
      return fd_secondary_indexes[i];
    else i++;
  return NULL;
}

FD_EXPORT fd_index fd_get_index(u8_string spec,fdb_flags flags)
{
  if (strchr(spec,';')) {
    fd_index ix=NULL;
    u8_byte *copy=u8_strdup(spec), *start=copy, *brk=strchr(start,';');
    while (brk) {
      if (ix==NULL) {
        *brk='\0'; ix=fd_get_index(start,flags);
        if (ix) {brk=NULL; start=NULL;}
        else {
          start=brk+1;
          brk=strchr(start,';');}}}
    if (ix) return ix;
    else if ((start)&&(*start)) {
      int start_off=start-copy;
      u8_free(copy);
      return fd_get_index(spec+start_off,flags);}
    else return NULL;}
  else {
    fd_index known=fd_find_index_by_cid(spec);
    if (known) return known;
    else return fd_open_index(spec,flags);}
}

/* Background indexes */

FD_EXPORT int fd_add_to_background(fd_index ix)
{
  if (ix==NULL) return 0;
  if (ix->index_serialno<0) {
    fdtype lix=(fdtype)ix; fd_incref(lix);
    fd_seterr(fd_TypeError,"fd_add_to_background","static index",lix);
    return -1;}
  u8_lock_mutex(&background_lock);
  ix->index_flags=ix->index_flags|FD_INDEX_IN_BACKGROUND;
  if (fd_background)
    fd_add_to_compound_index(fd_background,ix);
  else {
    fd_index *indexes=u8_alloc_n(1,fd_index);
    indexes[0]=ix;
    fd_background=
      (struct FD_COMPOUND_INDEX *)fd_make_compound_index(1,indexes);}
  u8_unlock_mutex(&background_lock);
  return 1;
}

FD_EXPORT fd_index fd_use_index(u8_string spec,fdb_flags flags)
{
  if (strchr(spec,';')) {
    fd_index ix=NULL;
    u8_byte *copy=u8_strdup(spec);
    u8_byte *start=copy, *end=strchr(start,';');
    *end='\0'; while (start) {
      ix=fd_get_index(start,flags);
      if (ix==NULL) {
        u8_free(copy); return NULL;}
      else fd_add_to_background(ix);
      if ((end) && (end[1])) {
        start=end+1; end=strchr(start,';');
        if (end) *end='\0';}
      else start=NULL;}
    u8_free(copy);
    return ix;}
  else {
    fd_index ix=fd_get_index(spec,flags);
    if (ix) fd_add_to_background(ix);
    return ix;}
}

/* Core functions */

static void delay_index_fetch(fd_index ix,fdtype keys);

FD_EXPORT fdtype fd_index_fetch(fd_index ix,fdtype key)
{
  fdtype v;
  fd_hashtable cache=&(ix->index_cache);
  fd_hashtable adds=&(ix->index_adds);
  fd_hashtable edits=&(ix->index_edits);
  init_cache_level(ix);
  if (ix->index_edits.table_n_keys) {
    fdtype set_key=fd_make_pair(set_symbol,key);
    fdtype drop_key=fd_make_pair(drop_symbol,key);
    fdtype set_value=fd_hashtable_get(edits,set_key,FD_VOID);
    if (!(FD_VOIDP(set_value))) {
      if (ix->index_cache_level>0) fd_hashtable_store(cache,key,set_value);
      fd_decref(drop_key); fd_decref(set_key);
      return set_value;}
    else {
      fdtype added=fd_hashtable_get(adds,key,FD_EMPTY_CHOICE);
      fdtype dropped=fd_hashtable_get(edits,drop_key,FD_EMPTY_CHOICE);
      if (ix->index_handler->fetch)
        v=ix->index_handler->fetch(ix,key);
      else v=FD_EMPTY_CHOICE;
      if (FD_EMPTY_CHOICEP(dropped)) {
        FD_ADD_TO_CHOICE(v,added);}
      else {
        fdtype newv;
        FD_ADD_TO_CHOICE(v,added);
        newv=fd_difference(v,dropped);
        fd_decref(v); fd_decref(dropped);
        v=newv;}}}
  else if (ix->index_adds.table_n_keys) {
    fdtype added=fd_hashtable_get(&(ix->index_adds),key,FD_EMPTY_CHOICE);
    if (ix->index_handler->fetch)
      v=ix->index_handler->fetch(ix,key);
    else v=FD_EMPTY_CHOICE;
    FD_ADD_TO_CHOICE(v,added);}
  else if (ix->index_handler->fetch) {
    if ((fd_ipeval_status())&&
        ((ix->index_handler->prefetch)||
         (ix->index_handler->fetchn))) {
      delay_index_fetch(ix,key);
      v=FD_EMPTY_CHOICE;}
    else v=ix->index_handler->fetch(ix,key);}
  else v=FD_EMPTY_CHOICE;
  if (ix->index_cache_level>0) fd_hashtable_store(&(ix->index_cache),key,v);
  return v;
}

static void delay_index_fetch(fd_index ix,fdtype keys)
{
  struct FD_HASHTABLE *cache=&(ix->index_cache); int delay_count=0;
  fdtype *delays=get_index_delays(), *delayp=&(delays[ix->index_serialno]);
  FD_DO_CHOICES(key,keys) {
    if (fd_hashtable_probe(cache,key)) {}
    else {
      fd_incref(key);
      FD_ADD_TO_CHOICE((*delayp),key);
      delay_count++;}}
  if (delay_count) (void)fd_ipeval_delay(delay_count);
}

FD_EXPORT void fd_delay_index_fetch(fd_index ix,fdtype keys)
{
  delay_index_fetch(ix,keys);
}

FD_EXPORT int fd_index_prefetch(fd_index ix,fdtype keys)
{
  FDTC *fdtc=((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  fdtype *keyvec=NULL, *values=NULL;
  fdtype lix=((ix->index_serialno>=0)?
              (FDTYPE_IMMEDIATE(fd_index_type,ix->index_serialno)):
              ((fdtype)ix));
  int free_keys=0, n_fetched=0, cachelevel=0;
  if (ix == NULL) return -1;
  else init_cache_level(ix);
  cachelevel=ix->index_cache_level;
  if (ix->index_handler->prefetch != NULL)
    if (fd_ipeval_status()) {
      delay_index_fetch(ix,keys);
      return 0;}
    else return ix->index_handler->prefetch(ix,keys);
  else if ((ix->index_handler->fetchn==NULL)||(!((ix->index_flags)&(FDB_BATCHABLE)))) {
    if (fd_ipeval_status()) {
      delay_index_fetch(ix,keys);
      return 0;}
    else {
      FD_DO_CHOICES(key,keys)
        if (!(fd_hashtable_probe(&(ix->index_cache),key))) {
          fdtype v=fd_index_fetch(ix,key);
          if (FD_ABORTP(v)) return fd_interr(v);
          n_fetched++;
          if (cachelevel>0) fd_hashtable_store(&(ix->index_cache),key,v);
          if (fdtc) fd_hashtable_store(&(fdtc->indexes),fd_make_pair(lix,key),v);
          fd_decref(v);}
      return n_fetched;}}
  if (FD_ACHOICEP(keys)) {keys=fd_make_simple_choice(keys); free_keys=1;}
  if (fd_ipeval_status()) delay_index_fetch(ix,keys);
  else if (!(FD_CHOICEP(keys))) {
    if (!(fd_hashtable_probe(&(ix->index_cache),keys))) {
      fdtype v=fd_index_fetch(ix,keys);
      if (FD_ABORTP(v)) return fd_interr(v);
      n_fetched=1;
      if (cachelevel>0) fd_hashtable_store(&(ix->index_cache),keys,v);
      if (fdtc) fd_hashtable_store(&(fdtc->indexes),fd_make_pair(lix,keys),v);
      fd_decref(v);}}
  else {
    fdtype *write=NULL, singlekey=FD_VOID;
    /* We first iterate over the keys to pick those that need to be fetched.
       If only one needs to be fetched, we will end up with write still NULL
       but with singlekey bound to that key. */
    struct FD_HASHTABLE *cache=&(ix->index_cache), *edits=&(ix->index_edits);
    if (edits->table_n_keys) {
      /* If there are any edits, we need to integrate them into whatever we
         prefetch. */
      FD_DO_CHOICES(key,keys) {
        fdtype set_key=fd_make_pair(set_symbol,key);
        if (!((fd_hashtable_probe(cache,key)) ||
              (fd_hashtable_probe(edits,set_key)))) {
          if (write) *write++=key;
          else if (FD_VOIDP(singlekey)) singlekey=key;
          else {
            write=keyvec=u8_alloc_n(FD_CHOICE_SIZE(keys),fdtype);
            write[0]=singlekey; write[1]=key; write=write+2;}}
        fd_decref(set_key);}}
    else {
      FD_DO_CHOICES(key,keys)
        if (!(fd_hashtable_probe(cache,key))) {
          if (write) *write++=key;
          else if (FD_VOIDP(singlekey)) singlekey=key;
          else {
            write=keyvec=u8_alloc_n(FD_CHOICE_SIZE(keys),fdtype);
            write[0]=singlekey; write[1]=key; write=write+2;}}}
    if (write==NULL)
      if (FD_VOIDP(singlekey)) {}
      else {
        fdtype v=fd_index_fetch(ix,singlekey); n_fetched=1;
        if (cachelevel>0) fd_hashtable_store(&(ix->index_cache),singlekey,v);
        if (fdtc) fd_hashtable_store
                    (&(fdtc->indexes),fd_make_pair(lix,singlekey),v);
        fd_decref(v);}
    else if (write==keyvec) n_fetched=0;
    else {
      unsigned int i=0, n=write-keyvec;
      n_fetched=n;
      values=ix->index_handler->fetchn(ix,n,keyvec);
      if (values==NULL) n_fetched=-1;
      else if (ix->index_edits.table_n_keys) /* When there are drops or sets */
        while (i < n) {
          fdtype key=keyvec[i];
          fdtype drop_key=fd_make_pair(drop_symbol,key);
          fdtype adds=fd_hashtable_get(&(ix->index_adds),key,FD_EMPTY_CHOICE);
          fdtype drops=
            fd_hashtable_get(&(ix->index_edits),drop_key,FD_EMPTY_CHOICE);
          if (FD_EMPTY_CHOICEP(drops))
            if (FD_EMPTY_CHOICEP(adds)) {
              if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],values[i]);
              if (fdtc) fd_hashtable_store
                          (&(fdtc->indexes),fd_make_pair(lix,keyvec[i]),
                           values[i]);
              fd_decref(values[i]);}
            else {
              fdtype simple;
              FD_ADD_TO_CHOICE(values[i],adds);
              simple=fd_simplify_choice(values[i]);
              if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],simple);
              if (fdtc) fd_hashtable_store
                          (&(fdtc->indexes),fd_make_pair(lix,keyvec[i]),
                           simple);
              fd_decref(simple);}
          else {
            fdtype oldv=values[i], newv;
            FD_ADD_TO_CHOICE(oldv,adds);
            newv=fd_difference(oldv,drops);
            newv=fd_simplify_choice(newv);
            if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],newv);
            if (fdtc) fd_hashtable_store
                        (&(fdtc->indexes),fd_make_pair(lix,keyvec[i]),
                         newv);
            fd_decref(oldv); fd_decref(newv);}
          fd_decref(drops); i++;}
      else if (ix->index_adds.table_n_keys)  /* When there are just adds */
        while (i < n) {
          fdtype key=keyvec[i];
          fdtype adds=fd_hashtable_get(&(ix->index_adds),key,FD_EMPTY_CHOICE);
          if (FD_EMPTY_CHOICEP(adds)) {
            if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],values[i]);
            if (fdtc) fd_hashtable_store
                        (&(fdtc->indexes),fd_make_pair(lix,keyvec[i]),
                         values[i]);
            fd_decref(values[i]);}
          else {
            FD_ADD_TO_CHOICE(values[i],adds);
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
          int k=0; while (k<n) {
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
  fdtype **write=(fdtype **)data;
  *((*write)++)=fd_incref(key);
  /* Means keep going */
  return 0;
}

static int edit_key_fn(fdtype key,fdtype val,void *data)
{
  fdtype **write=(fdtype **)data;
  if (FD_PAIRP(key)) {
    struct FD_PAIR *pair=(struct FD_PAIR *)key;
    if (pair->fd_car==set_symbol) {
      *((*write)++)=pair->fd_cdr; fd_incref(pair->fd_cdr);}}
  /* Means keep going */
  return 0;
}

FD_EXPORT fdtype fd_index_keys(fd_index ix)
{
  if (ix->index_handler->fetchkeys) {
    int n_fetched=0;
    fdtype *fetched=ix->index_handler->fetchkeys(ix,&n_fetched);
    if ((n_fetched==0) && (ix->index_edits.table_n_keys == 0) &&
        (ix->index_adds.table_n_keys == 0))
      return FD_EMPTY_CHOICE;
    else if ((n_fetched==0) && (ix->index_edits.table_n_keys == 0))
      return fd_hashtable_keys(&(ix->index_adds));
    else if ((n_fetched==0) && (ix->index_adds.table_n_keys == 0))
      return fd_hashtable_keys(&(ix->index_edits));
    else {
      fd_choice result; int n_total;
      fdtype *write_start, *write_at;
      n_total=n_fetched+ix->index_adds.table_n_keys+
        ix->index_edits.table_n_keys;
      result=fd_alloc_choice(n_total);
      if (n_fetched)
        memcpy(&(result->choice_0),fetched,sizeof(fdtype)*n_fetched);
      write_start=&(result->choice_0); write_at=write_start+n_fetched;
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
  struct FD_HASHTABLE *sizes=(struct FD_HASHTABLE *)vptr;
  int value_size=((FD_VOIDP(value)) ? (0) : FD_CHOICE_SIZE(value));
  fd_hashtable_store(sizes,key,FD_INT(value_size));
  return 0;
}

FD_EXPORT fdtype fd_index_sizes(fd_index ix)
{
  if (ix->index_handler->fetchsizes) {
    int n_fetched=0;
    struct FD_KEY_SIZE *fetched=ix->index_handler->fetchsizes(ix,&n_fetched);
    if ((n_fetched==0) && (ix->index_adds.table_n_keys==0))
      return FD_EMPTY_CHOICE;
    else if ((ix->index_adds.table_n_keys)==0) {
      fd_choice result=fd_alloc_choice(n_fetched);
      fdtype *write=&(result->choice_0);
      int i=0; while (i<n_fetched) {
        fdtype key=fetched[i].keysizekey, pair;
        unsigned int n_values=fetched[i].keysizenvals;
        pair=fd_conspair(fd_incref(key),FD_INT(n_values));
        *write++=pair; i++;}
      u8_free(fetched);
      return fd_init_choice(result,n_fetched,NULL,
                            FD_CHOICE_DOSORT|FD_CHOICE_REALLOC);}
    else {
      struct FD_HASHTABLE added_sizes;
      fd_choice result; int i=0, n_total; fdtype *write;
      /* Get the sizes for added keys. */
      FD_INIT_STATIC_CONS(&added_sizes,fd_hashtable_type);
      fd_make_hashtable(&added_sizes,ix->index_adds.ht_n_buckets);
      fd_for_hashtable(&(ix->index_adds),copy_value_sizes,&added_sizes,1);
      n_total=n_fetched+ix->index_adds.table_n_keys;
      result=fd_alloc_choice(n_total); write=&(result->choice_0);
      while (i<n_fetched) {
        fdtype key=fetched[i].keysizekey, pair;
        unsigned int n_values=fetched[i].keysizenvals;
        fdtype added=fd_hashtable_get(&added_sizes,key,FD_INT(0));
        n_values=n_values+fd_getint(added); fd_decref(added);
        pair=fd_conspair(fd_incref(key),FD_INT(n_values));
        *write++=pair; i++;}
      fd_recycle_hashtable(&added_sizes); u8_free(fetched);
      return fd_init_choice(result,n_total,NULL,
                            FD_CHOICE_DOSORT|FD_CHOICE_REALLOC);}}
  else return fd_err(fd_NoMethod,"fd_index_keys",NULL,fd_index2lisp(ix));
}

FD_EXPORT fdtype _fd_index_get(fd_index ix,fdtype key)
{
  fdtype cached; struct FD_PAIR tempkey;
  FDTC *fdtc=fd_threadcache;
  if (fdtc) {
    FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
    tempkey.fd_car=fd_index2lisp(ix); tempkey.fd_cdr=key;
    cached=fd_hashtable_get(&(fdtc->indexes),(fdtype)&tempkey,FD_VOID);
    if (!(FD_VOIDP(cached))) return cached;}
  if (ix->index_cache_level==0) cached=FD_VOID;
  else if ((FD_PAIRP(key)) && (!(FD_VOIDP(ix->index_covers_slotids))) &&
      (!(atomic_choice_containsp(FD_CAR(key),ix->index_covers_slotids))))
    return FD_EMPTY_CHOICE;
  else cached=fd_hashtable_get(&(ix->index_cache),key,FD_VOID);
  if (FD_VOIDP(cached)) cached=fd_index_fetch(ix,key);
#if FD_USE_THREADCACHE
  if (fdtc) {
    fd_hashtable_store(&(fdtc->indexes),(fdtype)&tempkey,cached);}
#endif
  return cached;
}
static fdtype table_indexget(fdtype ixarg,fdtype key,fdtype dflt)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix) {
    fdtype v=fd_index_get(ix,key);
    if (FD_EMPTY_CHOICEP(v)) return fd_incref(dflt);
    else return v;}
  else return FD_ERROR_VALUE;
}


static void extend_slotids(fd_index ix,const fdtype *keys,int n)
{
  fdtype slotids=ix->index_covers_slotids;
  int i=0; while (i<n) {
    fdtype key=keys[i++], slotid;
    if (FD_PAIRP(key)) slotid=FD_CAR(key); else continue;
    if ((FD_OIDP(slotid)) || (FD_SYMBOLP(slotid))) {
      if (atomic_choice_containsp(slotid,slotids)) continue;
      else {fd_decref(slotids); ix->index_covers_slotids=FD_VOID;}}}
}

FD_EXPORT int _fd_index_add(fd_index ix,fdtype key,fdtype value)
{
  FDTC *fdtc=fd_threadcache;
  fd_hashtable adds=&(ix->index_adds), cache=&(ix->index_cache);
  if (U8_BITP(ix->index_flags,FDB_READ_ONLY)) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_add",u8_strdup(ix->index_idstring),FD_VOID);
    return -1;}
  else init_cache_level(ix);
  if (FD_EMPTY_CHOICEP(value)) return 0;
  else if ((FD_CHOICEP(key)) || (FD_ACHOICEP(key))) {
    fdtype keys=fd_make_simple_choice(key);
    const fdtype *keyv=FD_CHOICE_DATA(keys);
    unsigned int n=FD_CHOICE_SIZE(keys);
    fd_hashtable_iterkeys(adds,fd_table_add,n,keyv,value);
    if (!(FD_VOIDP(ix->index_covers_slotids))) extend_slotids(ix,keyv,n);
    if ((FD_WRITETHROUGH_THREADCACHE)&&(fdtc)) {
      FD_DO_CHOICES(k,key) {
        struct FD_PAIR tempkey;
        FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
        tempkey.fd_car=fd_index2lisp(ix); tempkey.fd_cdr=k;
        if (fd_hashtable_probe(&(fdtc->indexes),(fdtype)&tempkey)) {
          fd_hashtable_add(&(fdtc->indexes),(fdtype)&tempkey,value);}}}

    if (ix->index_cache_level>0)
      fd_hashtable_iterkeys(cache,fd_table_add_if_present,n,keyv,value);
    fd_decref(keys);}
  else {
    fd_hashtable_add(adds,key,value);
    fd_hashtable_op(cache,fd_table_add_if_present,key,value);}

  if ((fdtc)&&(FD_WRITETHROUGH_THREADCACHE)) {
    FD_DO_CHOICES(k,key) {
      struct FD_PAIR tempkey;
      FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
      tempkey.fd_car=fd_index2lisp(ix); tempkey.fd_cdr=k;
      if (fd_hashtable_probe(&(fdtc->indexes),(fdtype)&tempkey)) {
        fd_hashtable_add(&(fdtc->indexes),(fdtype)&tempkey,value);}}}

  if ( (ix->index_flags&FD_INDEX_IN_BACKGROUND) &&
       (fd_background->index_cache.table_n_keys) && 
         (fd_background->index_cache.table_n_keys) ) {
    fd_hashtable bgcache=&(fd_background->index_cache);
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key);
      fd_hashtable_iterkeys(bgcache,fd_table_replace,n,keys,FD_VOID);}
    else fd_hashtable_op(bgcache,fd_table_replace,key,FD_VOID);}

  if (!(FD_VOIDP(ix->index_covers_slotids))) extend_slotids(ix,&key,1);

  return 1;
}

static int table_indexadd(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix) return fd_index_add(ix,key,value);
  else return -1;
}

FD_EXPORT int fd_index_drop(fd_index ix,fdtype key,fdtype value)
{
  fd_hashtable cache=&(ix->index_cache);
  fd_hashtable edits=&(ix->index_edits);
  fd_hashtable adds=&(ix->index_adds);
  fd_hashtable bg_cache=&(fd_background->index_cache);
  if (U8_BITP(ix->index_flags,FDB_READ_ONLY)) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_add",
              u8_strdup(ix->index_idstring),FD_VOID);
    return -1;}
  else init_cache_level(ix);
  if (FD_CHOICEP(key)) {
    FD_DO_CHOICES(eachkey,key) {
      fdtype drop_key=fd_make_pair(drop_symbol,eachkey);
      fdtype set_key=fd_make_pair(set_symbol,eachkey);
      fd_hashtable_add(edits,drop_key,value);
      fd_hashtable_drop(edits,set_key,value);
      fd_hashtable_drop(adds,eachkey,value);
      if (ix->index_cache_level>0)
        fd_hashtable_drop(cache,eachkey,value);
      fd_decref(set_key); fd_decref(drop_key);}}
  else {
    fdtype drop_key=fd_make_pair(drop_symbol,key);
    fdtype set_key=fd_make_pair(set_symbol,key);
    fd_hashtable_add(edits,drop_key,value);
    fd_hashtable_drop(edits,set_key,value);
    fd_hashtable_drop(adds,key,value);
    if (ix->index_cache_level>0)
      fd_hashtable_drop(cache,key,value);
    fd_decref(set_key); fd_decref(drop_key);}
  if ((ix->index_flags&FD_INDEX_IN_BACKGROUND) &&
      (fd_background->index_cache.table_n_keys)) {
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key);
      fd_hashtable_iterkeys
        (bg_cache,fd_table_replace,n,keys,FD_VOID);}
    else fd_hashtable_op(bg_cache,fd_table_replace,key,FD_VOID);}
  return 1;
}
static int table_indexdrop(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix) return fd_index_drop(ix,key,value);
  else return -1;
}

FD_EXPORT int fd_index_store(fd_index ix,fdtype key,fdtype value)
{
  fd_hashtable cache=&(ix->index_cache);
  fd_hashtable edits=&(ix->index_edits);
  fd_hashtable adds=&(ix->index_adds);
  fd_hashtable bg_cache=&(fd_background->index_cache);
  if (U8_BITP(ix->index_flags,FDB_READ_ONLY)) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_store",
              u8_strdup(ix->index_idstring),FD_VOID);
    return -1;}
  else init_cache_level(ix);
  if (FD_CHOICEP(key)) {
    FD_DO_CHOICES(eachkey,key) {
      fdtype set_key=fd_make_pair(set_symbol,eachkey);
      fdtype drop_key=fd_make_pair(drop_symbol,eachkey);
      fd_hashtable_store(edits,set_key,value);
      fd_hashtable_drop(edits,drop_key,FD_VOID);
      if (ix->index_cache_level>0) {
        fd_hashtable_store(cache,eachkey,value);
        fd_hashtable_drop(adds,eachkey,FD_VOID);}
      fd_decref(set_key); fd_decref(drop_key);}}
  else {
    fdtype set_key=fd_make_pair(set_symbol,key);
    fdtype drop_key=fd_make_pair(drop_symbol,key);
    fd_hashtable_store(edits,set_key,value);
    fd_hashtable_drop(edits,drop_key,FD_EMPTY_CHOICE);
    if (ix->index_cache_level>0) {
      fd_hashtable_store(cache,key,value);
      fd_hashtable_drop(adds,key,FD_VOID);}
    fd_decref(set_key); fd_decref(drop_key);}
  if ((ix->index_flags&FD_INDEX_IN_BACKGROUND) &&
      (fd_background->index_cache.table_n_keys)) {
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key);
      fd_hashtable_iterkeys(bg_cache,fd_table_replace,n,keys,FD_VOID);}
    else fd_hashtable_op(bg_cache,fd_table_replace,key,FD_VOID);}
  return 1;
}

static int table_indexstore(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix) return fd_index_store(ix,key,value);
  else return -1;
}

static int merge_kv_into_adds(struct FD_KEYVAL *kv,void *data)
{
  struct FD_HASHTABLE *adds=(fd_hashtable) data;
  fd_hashtable_op_nolock(adds,fd_table_add,kv->kv_key,kv->kv_val);
  return 0;
}

FD_EXPORT int fd_index_merge(fd_index ix,fd_hashtable table)
{
  /* Ignoring this for now */
  fd_hashtable adds=&(ix->index_adds);
  if (U8_BITP(ix->index_flags,FDB_READ_ONLY)) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_store",
              u8_strdup(ix->index_idstring),FD_VOID);
    return -1;}
  else init_cache_level(ix);
  fd_write_lock_table(adds);
  fd_read_lock_table(table);
  fd_for_hashtable_kv(table,merge_kv_into_adds,(void *)adds,0);
  fd_unlock_table(table);
  fd_unlock_table(adds);
  return 1;
}

static fdtype table_indexkeys(fdtype ixarg)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix) return fd_index_keys(ix);
  else return fd_type_error(_("index"),"table_index_keys",ixarg);
}

FD_EXPORT int fd_index_commit(fd_index ix)
{
  if (ix==NULL) return -1;
  else init_cache_level(ix);
  if ((ix->index_adds.ht_n_buckets) || (ix->index_edits.ht_n_buckets)) {
    int n_keys=ix->index_adds.table_n_keys+ix->index_edits.table_n_keys, retval=0;
    if (n_keys==0) return 0;
    u8_log(fdb_loglevel,fd_IndexCommit,
           "####### Saving %d updates to %s",n_keys,ix->index_idstring);
    double start_time=u8_elapsed_time();
    if (ix->index_cache_level<0) {
      ix->index_cache_level=fd_default_cache_level;
      if (ix->index_handler->setcache)
        ix->index_handler->setcache(ix,fd_default_cache_level);}
    retval=ix->index_handler->commit(ix);
    if (retval<0)
      u8_log(LOG_CRIT,fd_IndexCommitError,
             _("!!!!!!! Error saving %d keys to %s after %f secs"),
             n_keys,ix->index_idstring,u8_elapsed_time()-start_time);
    else if (retval>0)
      u8_log(fdb_loglevel,fd_IndexCommit,
             _("####### Saved %d updated keys to %s in %f secs"),
             retval,ix->index_idstring,u8_elapsed_time()-start_time);
    else {}
    if (retval<0)
      u8_seterr(fd_IndexCommitError,"fd_index_commit",u8_strdup(ix->index_idstring));
    return retval;}
  else return 0;
}

FD_EXPORT void fd_index_swapout(fd_index ix)
{
  if ((((ix->index_flags)&FD_INDEX_NOSWAP)==0) && (ix->index_cache.table_n_keys)) {
    if ((ix->index_flags)&(FDB_STICKY_CACHESIZE))
      fd_reset_hashtable(&(ix->index_cache),-1,1);
    else fd_reset_hashtable(&(ix->index_cache),0,1);}
}

FD_EXPORT void fd_index_close(fd_index ix)
{
  if ((ix) && (ix->index_handler) && (ix->index_handler->close))
    ix->index_handler->close(ix);
}

/* Common init function */

FD_EXPORT void fd_init_index
  (fd_index ix,struct FD_INDEX_HANDLER *h,u8_string source,fdb_flags flags)
{
  U8_SETBITS(flags,FDB_ISINDEX);
  if (U8_BITP(flags,FDB_ISCONSED)) {
    FD_INIT_CONS(ix,fd_raw_index_type);}
  else {FD_INIT_STATIC_CONS(ix,fd_raw_index_type);}
  if (U8_BITP(flags,FDB_READ_ONLY)) { U8_SETBITS(flags,FDB_READ_ONLY); };
  if (h->fetchn) { U8_SETBITS(flags,FDB_BATCHABLE); };
  ix->index_serialno=-1; ix->index_cache_level=-1; ix->index_flags=flags;
  FD_INIT_STATIC_CONS(&(ix->index_cache),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(ix->index_adds),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(ix->index_edits),fd_hashtable_type);
  fd_make_hashtable(&(ix->index_cache),fd_index_cache_init);
  fd_make_hashtable(&(ix->index_adds),0);
  fd_make_hashtable(&(ix->index_edits),0);
  ix->index_handler=h;
  ix->index_idstring=u8_strdup(source);
  ix->index_source=u8_strdup(source);
  ix->index_xinfo=NULL;
  ix->index_covers_slotids=FD_VOID;
}

FD_EXPORT void fd_reset_index_tables
  (fd_index ix,ssize_t csize,ssize_t esize,ssize_t asize)
{
  int readonly=U8_BITP(ix->index_flags,FDB_READ_ONLY);
  fd_hashtable cache=&(ix->index_cache);
  fd_hashtable edits=&(ix->index_edits);
  fd_hashtable adds=&(ix->index_adds);
  fd_reset_hashtable(cache,((csize==0)?(fd_index_cache_init):(csize)),1);
  if (edits->table_n_keys==0) {
    ssize_t level=(readonly)?(0):(esize==0)?(fd_index_edits_init):(esize);
    fd_reset_hashtable(edits,level,1);}
  if (adds->table_n_keys==0) {
    ssize_t level=(readonly)?(0):(asize==0)?(fd_index_adds_init):(asize);
    fd_reset_hashtable(adds,level,1);}
}

static int unparse_index(u8_output out,fdtype x)
{
  fd_index ix=fd_indexptr(x); u8_string type;
  if (ix==NULL) return 0;
  if ((ix->index_handler) && (ix->index_handler->name)) type=ix->index_handler->name;
  else type="unrecognized";
  if ((ix->index_xinfo) && (strcmp(ix->index_source,ix->index_xinfo)))
    u8_printf(out,_("#<INDEX %s 0x%lx \"%s|%s\">"),
              type,x,ix->index_source,ix->index_xinfo);
  else u8_printf(out,_("#<INDEX %s 0x%lx \"%s\">"),type,x,ix->index_source);
  return 1;
}

static int unparse_raw_index(u8_output out,fdtype x)
{
  fd_index ix=(fd_index)(x); u8_string type;
  if (ix==NULL) return 0;
  if ((ix->index_handler) && (ix->index_handler->name)) type=ix->index_handler->name;
  else type="unrecognized";
  if ((ix->index_xinfo) && (strcmp(ix->index_source,ix->index_xinfo)))
    u8_printf(out,_("#<INDEX %s 0x%lx \"%s|%s\">"),
              type,x,ix->index_source,ix->index_xinfo);
  else u8_printf(out,_("#<INDEX %s 0x%lx \"%s\">"),type,x,ix->index_source);
  return 1;
}

static fdtype index_parsefn(int n,fdtype *args,fd_compound_typeinfo e)
{
  fd_index ix=NULL;
  if (n<2) return FD_VOID;
  else if (FD_STRINGP(args[2]))
    ix=fd_get_index(FD_STRING_DATA(args[2]),0);
  if (ix) return fd_index2lisp(ix);
  else return fd_err(fd_CantParseRecord,"index_parsefn",NULL,FD_VOID);
}

/* Operations over all indexes */

FD_EXPORT void fd_swapout_indexes()
{
  int i=0; while (i < fd_n_primary_indexes) {
    fd_index_swapout(fd_primary_indexes[i]); i++;}
  i=0; while (i < fd_n_secondary_indexes) {
    fd_index_swapout(fd_secondary_indexes[i]); i++;}
}

FD_EXPORT void fd_close_indexes()
{
  int i=0; while (i < fd_n_primary_indexes) {
    fd_index_close(fd_primary_indexes[i]); i++;}
  i=0; while (i < fd_n_secondary_indexes) {
    fd_index_close(fd_secondary_indexes[i]); i++;}
}

FD_EXPORT int fd_commit_indexes()
{
  int count=0, i=0; while (i < fd_n_primary_indexes) {
    int retval=fd_index_commit(fd_primary_indexes[i]);
    if (retval<0) return retval;
    else count=count+retval;
    i++;}
  i=0; while (i < fd_n_secondary_indexes) {
    int retval=fd_index_commit(fd_secondary_indexes[i]);
    if (retval<0) return retval;
    else count=count+retval;
    i++;}
  return count;
}

FD_EXPORT int fd_commit_indexes_noerr()
{
  int count=0;
  int i=0; while (i < fd_n_primary_indexes) {
    int retval=fd_index_commit(fd_primary_indexes[i]);
    if (retval<0) {
      u8_log(LOG_CRIT,"INDEX_COMMIT_FAIL","Error committing %s",
              fd_primary_indexes[i]->index_idstring);
      fd_clear_errors(1);
      count=-1;}
    if (count>=0) count=count+retval;
    i++;}
  i=0; while (i < fd_n_secondary_indexes) {
    int retval=fd_index_commit(fd_secondary_indexes[i]);
    if (retval<0) {
      u8_log(LOG_CRIT,"INDEX_COMMIT_FAIL","Error committing %s",
              fd_secondary_indexes[i]->index_idstring);
      fd_clear_errors(1);
      count=-1;}
    if (count>=0) count=count+retval;
    i++;}
  return count;
}

FD_EXPORT int fd_for_indexes(int (*fcn)(fd_index ix,void *),void *data)
{
  int i=0; while (i < fd_n_primary_indexes) {
    int retval=fcn(fd_primary_indexes[i],data);
    if (retval<0) return retval;
    else if (retval) break;
    else i++;}
  if (i>=fd_n_primary_indexes) return i;
  i=0; while (i < fd_n_secondary_indexes) {
    int retval=fcn(fd_secondary_indexes[i],data);
    if (retval<0) return retval;
    else if (retval) break;
    else i++;}
  return i;
}

static int accumulate_cachecount(fd_index ix,void *ptr)
{
  int *count=(int *)ptr;
  *count=*count+ix->index_cache.table_n_keys;
  return 0;
}

FD_EXPORT
long fd_index_cache_load()
{
  int result=0, retval;
  retval=fd_for_indexes(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(fd_index ix,void *ptr)
{
  fdtype *vals=(fdtype *)ptr;
  fdtype keys=fd_hashtable_keys(&(ix->index_cache));
  FD_ADD_TO_CHOICE(*vals,keys);
  return 0;
}

FD_EXPORT
fdtype fd_cached_keys(fd_index ix)
{
  if (ix==NULL) {
    int retval; fdtype result=FD_EMPTY_CHOICE;
    retval=fd_for_indexes(accumulate_cached,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return FD_ERROR_VALUE;}
    else return result;}
  else return fd_hashtable_keys(&(ix->index_cache));
}

/* IPEVAL delay execution */

FD_EXPORT int fd_execute_index_delays(fd_index ix,void *data)
{
  fdtype *delays=get_index_delays();
  fdtype todo=delays[ix->index_serialno];
  if (FD_EMPTY_CHOICEP(todo)) return 0;
  else {
    int retval=-1;
    /* u8_lock_mutex(&(fd_ipeval_lock)); */
    todo=delays[ix->index_serialno];
    delays[ix->index_serialno]=FD_EMPTY_CHOICE;
    /* u8_unlock_mutex(&(fd_ipeval_lock)); */
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval>1)
      u8_log(LOG_NOTICE,ipeval_ixfetch,"Fetching %d keys from %s: %q",
             FD_CHOICE_SIZE(todo),ix->index_idstring,todo);
    else
#endif
    retval=fd_index_prefetch(ix,todo);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      u8_log(LOG_NOTICE,ipeval_ixfetch,"Fetched %d keys from %s",
             FD_CHOICE_SIZE(todo),ix->index_idstring);
#endif
    if (retval<0) return retval;
    else return 0;}
}

static void recycle_raw_index(struct FD_RAW_CONS *c)
{
  struct FD_INDEX *ix=(struct FD_INDEX *)c;
  if (ix->index_handler==&fd_extindex_handler) {
    struct FD_EXTINDEX *ei=(struct FD_EXTINDEX *)c;
    fd_decref(ei->fetchfn);
    fd_decref(ei->commitfn);
    fd_decref(ei->state);}
  fd_recycle_hashtable(&(ix->index_cache));
  fd_recycle_hashtable(&(ix->index_adds));
  fd_recycle_hashtable(&(ix->index_edits));
  u8_free(ix->index_idstring);
  u8_free(ix->index_source);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

static fdtype copy_raw_index(fdtype x,int deep)
{
  /* Where might this get us into trouble when not really copying the pool? */
  fd_index ix=(fd_index)x;
  if (ix->index_serialno>=0)
    return fd_index2lisp(ix);
  else return x;
}

/* Initialize */

fd_ptr_type fd_index_type, fd_raw_index_type;

static int check_index(fdtype x)
{
  int serial=FD_GET_IMMEDIATE(x,fd_index_type);
  if (serial<0) return 0;
  if (serial<FD_N_PRIMARY_INDEXES)
    if (fd_primary_indexes[serial]) return 1;
    else return 0;
  else return (fd_secondary_indexes[serial-FD_N_PRIMARY_INDEXES]!=NULL);
}

FD_EXPORT fd_index _fd_indexptr(fdtype x)
{
  return fd_indexptr(x);
}

/* Initializations */

FD_EXPORT void fd_init_indexes_c()
{
  u8_register_source_file(_FILEINFO);

  fd_index_type=fd_register_immediate_type("index",check_index);
  fd_raw_index_type=fd_register_cons_type("raw index");

  fd_type_names[fd_index_type]=_("index");
  fd_type_names[fd_raw_index_type]=_("raw index");

  {
    struct FD_COMPOUND_TYPEINFO *e=fd_register_compound(fd_intern("INDEX"),NULL,NULL);
    e->fd_compound_parser=index_parsefn;}

  fd_tablefns[fd_index_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_index_type]->get=table_indexget;
  fd_tablefns[fd_index_type]->add=table_indexadd;
  fd_tablefns[fd_index_type]->drop=table_indexdrop;
  fd_tablefns[fd_index_type]->store=table_indexstore;
  fd_tablefns[fd_index_type]->test=NULL;
  fd_tablefns[fd_index_type]->keys=table_indexkeys;
  fd_tablefns[fd_index_type]->getsize=NULL;

  fd_tablefns[fd_raw_index_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_raw_index_type]->get=table_indexget;
  fd_tablefns[fd_raw_index_type]->add=table_indexadd;
  fd_tablefns[fd_raw_index_type]->drop=table_indexdrop;
  fd_tablefns[fd_raw_index_type]->store=table_indexstore;
  fd_tablefns[fd_raw_index_type]->test=NULL;
  fd_tablefns[fd_raw_index_type]->keys=table_indexkeys;
  fd_tablefns[fd_raw_index_type]->getsize=NULL;

  fd_recyclers[fd_raw_index_type]=recycle_raw_index;
  fd_unparsers[fd_raw_index_type]=unparse_raw_index;
  fd_copiers[fd_raw_index_type]=copy_raw_index;

  set_symbol=fd_make_symbol("SET",3);
  drop_symbol=fd_make_symbol("DROP",4);
  fd_unparsers[fd_index_type]=unparse_index;

  u8_init_mutex(&indexes_lock);
  u8_init_mutex(&background_lock);

#if (FD_USE_TLS)
  u8_new_threadkey(&index_delays_key,NULL);
#endif

#if FD_CALLTRACK_ENABLED
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("KEYS",1);
    cts->enabled=1; cts->intfcn=fd_index_cache_load;}
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
