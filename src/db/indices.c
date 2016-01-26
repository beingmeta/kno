/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_INDICES 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/apply.h"

#include <libu8/libu8.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>

fd_exception fd_EphemeralIndex=_("ephemeral index");
fd_exception fd_ReadOnlyIndex=_("read-only index");
fd_exception fd_NoFileIndices=_("file indices are not supported");
fd_exception fd_NotAFileIndex=_("not a file index");
fd_exception fd_BadIndexSpec=_("bad index specification");
fd_exception fd_IndexCommitError=_("can't save changes to index");
static u8_condition ipeval_ixfetch="IXFETCH";

fd_index (*fd_file_index_opener)(u8_string,int)=NULL;

fd_index fd_primary_indices[FD_N_PRIMARY_INDICES], *fd_secondary_indices=NULL;
int fd_n_primary_indices=0, fd_n_secondary_indices=0;

#if FD_THREADS_ENABLED
static u8_mutex background_lock;
#endif

struct FD_COMPOUND_INDEX *fd_background=NULL;

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

#if FD_THREADS_ENABLED
static u8_mutex indices_lock;
#endif

/* Index cache levels */

/* a cache_level<0 indicates no caching has been done */

FD_EXPORT void fd_index_setcache(fd_index ix,int level)
{
  if (ix->handler->setcache) ix->handler->setcache(ix,level);
  ix->cache_level=level;
  ix->flags=ix->flags|FD_EXPLICIT_SETCACHE;
}

static void init_cache_level(fd_index ix)
{
  if (FD_EXPECT_FALSE(ix->cache_level<0)) {
    ix->cache_level=fd_default_cache_level;
    if (ix->handler->setcache)
      ix->handler->setcache(ix,fd_default_cache_level);}
}


/* The index registry */

FD_EXPORT void fd_register_index(fd_index ix)
{
  if (ix->serialno<0) {
    fd_lock_mutex(&indices_lock);
    if (ix->serialno>=0) { /* Handle race condition */
      fd_unlock_mutex(&indices_lock); return;}
    if (fd_n_primary_indices<FD_N_PRIMARY_INDICES) {
      ix->serialno=fd_n_primary_indices;
      fd_primary_indices[fd_n_primary_indices++]=ix;}
    else {
      if (fd_secondary_indices)
        fd_secondary_indices=u8_realloc_n
          (fd_secondary_indices,fd_n_secondary_indices+1,fd_index);
      else fd_secondary_indices=u8_alloc_n(1,fd_index);
      ix->serialno=fd_n_secondary_indices+FD_N_PRIMARY_INDICES;
      fd_secondary_indices[fd_n_secondary_indices++]=ix;}
    fd_unlock_mutex(&indices_lock);}
}

FD_EXPORT fdtype fd_index2lisp(fd_index ix)
{
  if (ix->serialno>=0)
    return FDTYPE_IMMEDIATE(fd_index_type,ix->serialno);
  else return fd_incref((fdtype)ix);
}
FD_EXPORT fd_index fd_lisp2index(fdtype lix)
{
  if (FD_PTR_TYPEP(lix,fd_index_type)) {
    int serial=FD_GET_IMMEDIATE(lix,fd_index_type);
    if (serial<FD_N_PRIMARY_INDICES) return fd_primary_indices[serial];
    else return fd_secondary_indices[serial-FD_N_PRIMARY_INDICES];}
  else if (FD_PTR_TYPEP(lix,fd_raw_index_type))
    return (fd_index) lix;
  else if (FD_STRINGP(lix))
    return fd_open_index(FD_STRDATA(lix));
  else {
    fd_seterr(fd_TypeError,_("not an index"),NULL,lix);
    return NULL;}
}

FD_EXPORT fd_index fd_find_index_by_cid(u8_string cid)
{
  int i=0;
  if (cid == NULL) return NULL;
  else while (i<fd_n_primary_indices)
    if (strcmp(cid,fd_primary_indices[i]->cid)==0)
      return fd_primary_indices[i];
    else i++;
  if (fd_secondary_indices == NULL) return NULL;
  i=0; while (i<fd_n_secondary_indices)
    if (strcmp(cid,fd_secondary_indices[i]->cid)==0)
      return fd_secondary_indices[i];
    else i++;
  return NULL;
}

FD_EXPORT fd_index fd_open_index_x(u8_string spec,int consed)
{
  if (strchr(spec,';')) {
    fd_seterr(fd_BadIndexSpec,"fd_open_index",u8_strdup(spec),FD_VOID);
    return NULL;}
  else if ((strchr(spec,'@'))||(strchr(spec,':'))) {
    fd_index known=fd_find_index_by_cid(spec);
    if (known) return known;
    else {
      u8_byte buf[64]; fdtype xname;
      u8_byte *at=strchr(spec,'@');
      if (!(at))
        return fd_open_network_index(spec,spec,FD_VOID);
      else if ((strchr(at+1,'@'))||(strchr(at+1,':'))) {
        if (at-spec>63) return NULL;
        strncpy(buf,spec,at-spec); buf[at-spec]='\0';
        xname=fd_parse(buf);
        if (consed)
          return fd_open_network_index_x(spec,at+1,xname,consed);
        else return fd_open_network_index(spec,at+1,xname);}
      else if (consed)
        return fd_open_network_index_x(spec,spec,FD_VOID,consed);
      else return fd_open_network_index(spec,spec,FD_VOID);}}
  else if (fd_file_index_opener)
    return fd_file_index_opener(spec,consed);
  else {
    fd_seterr3(fd_NoFileIndices,"fd_open_index",u8_strdup(spec));
    return NULL;}
}

FD_EXPORT fd_index fd_open_index(u8_string spec)
{
  return fd_open_index_x(spec,0);
}

/* Background indices */

FD_EXPORT int fd_add_to_background(fd_index ix)
{
  if (ix==NULL) return 0;
  if (ix->serialno<0) {
    fdtype lix=(fdtype)ix; fd_incref(lix);
    fd_seterr(fd_TypeError,"fd_add_to_background","static index",lix);
    return -1;}
  fd_lock_mutex(&background_lock);
  ix->flags=ix->flags|FD_INDEX_IN_BACKGROUND;
  if (fd_background)
    fd_add_to_compound_index(fd_background,ix);
  else {
    fd_index *indices=u8_alloc_n(1,fd_index);
    indices[0]=ix;
    fd_background=
      (struct FD_COMPOUND_INDEX *)fd_make_compound_index(1,indices);}
  fd_unlock_mutex(&background_lock);
  return 1;
}

FD_EXPORT fd_index fd_use_index(u8_string spec)
{
  if (strchr(spec,';')) {
    fd_index ix=NULL;
    u8_byte *copy=u8_strdup(spec);
    u8_byte *start=copy, *end=strchr(start,';');
    *end='\0'; while (start) {
      ix=fd_open_index(start);
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
    fd_index ix=fd_open_index(spec);
    if (ix) fd_add_to_background(ix);
    return ix;}
}

/* Core functions */

static void delay_index_fetch(fd_index ix,fdtype keys);

FD_EXPORT fdtype fd_index_fetch(fd_index ix,fdtype key)
{
  fdtype v;
  init_cache_level(ix);
  if (ix->edits.n_keys) {
    fdtype set_key=fd_make_pair(set_symbol,key);
    fdtype drop_key=fd_make_pair(drop_symbol,key);
    fdtype set_value=fd_hashtable_get(&(ix->edits),set_key,FD_VOID);
    if (!(FD_VOIDP(set_value))) {
      if (ix->cache_level>0) fd_hashtable_store(&(ix->cache),key,set_value);
      fd_decref(drop_key); fd_decref(set_key);
      return set_value;}
    else {
      fdtype adds=fd_hashtable_get(&(ix->adds),key,FD_EMPTY_CHOICE);
      fdtype drops=fd_hashtable_get(&(ix->edits),drop_key,FD_EMPTY_CHOICE);
      if (ix->handler->fetch)
        v=ix->handler->fetch(ix,key);
      else v=FD_EMPTY_CHOICE;
      if (FD_EMPTY_CHOICEP(drops)) {
        FD_ADD_TO_CHOICE(v,adds);}
      else {
        fdtype newv;
        FD_ADD_TO_CHOICE(v,adds);
        newv=fd_difference(v,drops);
        fd_decref(v); fd_decref(drops);
        v=newv;}}}
  else if (ix->adds.n_keys) {
    fdtype adds=fd_hashtable_get(&(ix->adds),key,FD_EMPTY_CHOICE);
    v=ix->handler->fetch(ix,key);
    FD_ADD_TO_CHOICE(v,adds);}
  else if (ix->handler->fetch) {
    if ((fd_ipeval_status())&&
        ((ix->handler->prefetch)||
         (ix->handler->fetchn))) {
      delay_index_fetch(ix,key);
      v=FD_EMPTY_CHOICE;}
    else v=ix->handler->fetch(ix,key);}
  else v=FD_EMPTY_CHOICE;
  if (ix->cache_level>0) fd_hashtable_store(&(ix->cache),key,v);
  return v;
}

static void delay_index_fetch(fd_index ix,fdtype keys)
{
  struct FD_HASHTABLE *cache=&(ix->cache); int delay_count=0;
  fdtype *delays=get_index_delays(), *delayp=&(delays[ix->serialno]);
  FD_DO_CHOICES(key,keys) {
    if (fd_hashtable_probe(cache,key)) {}
    else {
      fd_incref(key);
      FD_ADD_TO_CHOICE((*delayp),key);
      delay_count++;}}
  if (delay_count) fd_ipeval_delay(delay_count);
}

FD_EXPORT void fd_delay_index_fetch(fd_index ix,fdtype keys)
{
  delay_index_fetch(ix,keys);
}

FD_EXPORT int fd_index_prefetch(fd_index ix,fdtype keys)
{
  FDTC *fdtc=((FD_USE_THREADCACHE)?(fd_threadcache):(NULL));
  fdtype *keyvec=NULL, *values=NULL;
  fdtype lix=((ix->serialno>=0)?
              (FDTYPE_IMMEDIATE(fd_index_type,ix->serialno)):
              ((fdtype)ix));
  int free_keys=0, n_fetched=0, cachelevel=0;
  if (ix == NULL) return -1;
  else init_cache_level(ix);
  cachelevel=ix->cache_level;
  if (ix->handler->prefetch != NULL)
    if (fd_ipeval_status()) {
      delay_index_fetch(ix,keys);
      return 0;}
    else return ix->handler->prefetch(ix,keys);
  else if ((ix->handler->fetchn==NULL)||(!((ix->flags)&(FD_INDEX_BATCHABLE)))) {
    if (fd_ipeval_status()) {
      delay_index_fetch(ix,keys);
      return 0;}
    else {
      FD_DO_CHOICES(key,keys)
        if (!(fd_hashtable_probe(&(ix->cache),key))) {
          fdtype v=fd_index_fetch(ix,key);
          if (FD_ABORTP(v)) return fd_interr(v);
          n_fetched++;
          if (cachelevel>0) fd_hashtable_store(&(ix->cache),key,v);
          if (fdtc) fd_hashtable_store(&(fdtc->indices),fd_make_pair(lix,key),v);
          fd_decref(v);}
      return n_fetched;}}
  if (FD_ACHOICEP(keys)) {keys=fd_make_simple_choice(keys); free_keys=1;}
  if (fd_ipeval_status()) delay_index_fetch(ix,keys);
  else if (!(FD_CHOICEP(keys))) {
    if (!(fd_hashtable_probe(&(ix->cache),keys))) {
      fdtype v=fd_index_fetch(ix,keys);
      if (FD_ABORTP(v)) return fd_interr(v);
      n_fetched=1;
      if (cachelevel>0) fd_hashtable_store(&(ix->cache),keys,v);
      if (fdtc) fd_hashtable_store(&(fdtc->indices),fd_make_pair(lix,keys),v);
      fd_decref(v);}}
  else {
    fdtype *write=NULL, singlekey=FD_VOID;
    /* We first iterate over the keys to pick those that need to be fetched.
       If only one needs to be fetched, we will end up with write still NULL
       but with singlekey bound to that key. */
    struct FD_HASHTABLE *cache=&(ix->cache), *edits=&(ix->edits);
    if (edits->n_keys) {
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
        if (cachelevel>0) fd_hashtable_store(&(ix->cache),singlekey,v);
        if (fdtc) fd_hashtable_store
                    (&(fdtc->indices),fd_make_pair(lix,singlekey),v);
        fd_decref(v);}
    else if (write==keyvec) n_fetched=0;
    else {
      unsigned int i=0, n=write-keyvec;
      n_fetched=n;
      values=ix->handler->fetchn(ix,n,keyvec);
      if (values==NULL) n_fetched=-1;
      else if (ix->edits.n_keys) /* When there are drops or sets */
        while (i < n) {
          fdtype key=keyvec[i];
          fdtype drop_key=fd_make_pair(drop_symbol,key);
          fdtype adds=fd_hashtable_get(&(ix->adds),key,FD_EMPTY_CHOICE);
          fdtype drops=
            fd_hashtable_get(&(ix->edits),drop_key,FD_EMPTY_CHOICE);
          if (FD_EMPTY_CHOICEP(drops))
            if (FD_EMPTY_CHOICEP(adds)) {
              if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],values[i]);
              if (fdtc) fd_hashtable_store
                          (&(fdtc->indices),fd_make_pair(lix,keyvec[i]),
                           values[i]);
              fd_decref(values[i]);}
            else {
              fdtype simple;
              FD_ADD_TO_CHOICE(values[i],adds);
              simple=fd_simplify_choice(values[i]);
              if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],simple);
              if (fdtc) fd_hashtable_store
                          (&(fdtc->indices),fd_make_pair(lix,keyvec[i]),
                           simple);
              fd_decref(simple);}
          else {
            fdtype oldv=values[i], newv;
            FD_ADD_TO_CHOICE(oldv,adds);
            newv=fd_difference(oldv,drops);
            newv=fd_simplify_choice(newv);
            if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],newv);
            if (fdtc) fd_hashtable_store
                        (&(fdtc->indices),fd_make_pair(lix,keyvec[i]),
                         newv);
            fd_decref(oldv); fd_decref(newv);}
          fd_decref(drops); i++;}
      else if (ix->adds.n_keys)  /* When there are just adds */
        while (i < n) {
          fdtype key=keyvec[i];
          fdtype adds=fd_hashtable_get(&(ix->adds),key,FD_EMPTY_CHOICE);
          if (FD_EMPTY_CHOICEP(adds)) {
            if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],values[i]);
            if (fdtc) fd_hashtable_store
                        (&(fdtc->indices),fd_make_pair(lix,keyvec[i]),
                         values[i]);
            fd_decref(values[i]);}
          else {
            FD_ADD_TO_CHOICE(values[i],adds);
            values[i]=fd_simplify_choice(values[i]);
            if (cachelevel>0) fd_hashtable_store(cache,keyvec[i],values[i]);
            if (fdtc) fd_hashtable_store
                        (&(fdtc->indices),fd_make_pair(lix,keyvec[i]),
                         values[i]);
            fd_decref(values[i]);}
          i++;}
      else {
        /* When we're just storing. */
        if (fdtc) {
          int k=0; while (k<n) {
            fd_hashtable_store(&(fdtc->indices),fd_make_pair(lix,keyvec[k]),
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
  *(*write++)=fd_incref(key);
  /* Means keep going */
  return 0;
}

FD_EXPORT fdtype fd_index_keys(fd_index ix)
{
  if (ix->handler->fetchkeys) {
    int n_fetched=0, n_total; fd_choice result;
    fdtype *fetched=ix->handler->fetchkeys(ix,&n_fetched);
    fdtype *write_start, *write_at;
    if (n_fetched==0) return fd_hashtable_keys(&(ix->adds));
    n_total=n_fetched+ix->adds.n_keys;
    result=fd_alloc_choice(n_total);
    memcpy(&(result->elt0),fetched,sizeof(fdtype)*n_fetched);
    write_start=&(result->elt0); write_at=write_start+n_fetched;
    u8_free(fetched);
    if (ix->adds.n_keys)
      fd_for_hashtable(&(ix->adds),add_key_fn,&write_at,1);
    return fd_init_choice(result,write_at-write_start,NULL,
                          FD_CHOICE_DOSORT|FD_CHOICE_REALLOC);}
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
  if (ix->handler->fetchsizes) {
    int n_fetched=0;
    struct FD_KEY_SIZE *fetched=ix->handler->fetchsizes(ix,&n_fetched);
    if ((n_fetched==0) && (ix->adds.n_keys)) return FD_EMPTY_CHOICE;
    else if ((ix->adds.n_keys)==0) {
      fd_choice result=fd_alloc_choice(n_fetched);
      fdtype *write=&(result->elt0);
      int i=0; while (i<n_fetched) {
        fdtype key=fetched[i].key, pair;
        unsigned int n_values=fetched[i].n_values;
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
      fd_make_hashtable(&added_sizes,ix->adds.n_slots);
      fd_for_hashtable(&(ix->adds),copy_value_sizes,&added_sizes,1);
      n_total=n_fetched+ix->adds.n_keys;
      result=fd_alloc_choice(n_total); write=&(result->elt0);
      while (i<n_fetched) {
        fdtype key=fetched[i].key, pair;
        unsigned int n_values=fetched[i].n_values;
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
    tempkey.car=fd_index2lisp(ix); tempkey.cdr=key;
    cached=fd_hashtable_get(&(fdtc->indices),(fdtype)&tempkey,FD_VOID);
    if (!(FD_VOIDP(cached))) return cached;}
  if (ix->cache_level==0) cached=FD_VOID;
  else if ((FD_PAIRP(key)) && (!(FD_VOIDP(ix->has_slotids))) &&
      (!(atomic_choice_containsp(FD_CAR(key),ix->has_slotids))))
    return FD_EMPTY_CHOICE;
  else cached=fd_hashtable_get(&(ix->cache),key,FD_VOID);
  if (FD_VOIDP(cached)) cached=fd_index_fetch(ix,key);
#if FD_USE_THREADCACHE
  if (fdtc) {
    fd_hashtable_store(&(fdtc->indices),(fdtype)&tempkey,cached);}
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
  fdtype slotids=ix->has_slotids;
  int i=0; while (i<n) {
    fdtype key=keys[i++], slotid;
    if (FD_PAIRP(key)) slotid=FD_CAR(key); else continue;
    if ((FD_OIDP(slotid)) || (FD_SYMBOLP(slotid))) {
      if (atomic_choice_containsp(slotid,slotids)) continue;
      else {fd_decref(slotids); ix->has_slotids=FD_VOID;}}}
}

FD_EXPORT int _fd_index_add(fd_index ix,fdtype key,fdtype value)
{
  FDTC *fdtc=fd_threadcache;
  if (ix->read_only) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_add",u8_strdup(ix->cid),FD_VOID);
    return -1;}
  else init_cache_level(ix);
  if (FD_EMPTY_CHOICEP(value)) {}
  else if (FD_CHOICEP(key)) {
    const fdtype *keys=FD_CHOICE_DATA(key);
    unsigned int n=FD_CHOICE_SIZE(key);
    if ((FD_WRITETHROUGH_THREADCACHE)&&(fdtc)) {
      FD_DO_CHOICES(k,key) {
        struct FD_PAIR tempkey;
        FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
        tempkey.car=fd_index2lisp(ix); tempkey.cdr=k;
        if (fd_hashtable_probe(&(fdtc->indices),(fdtype)&tempkey)) {
          fd_hashtable_add(&(fdtc->indices),(fdtype)&tempkey,value);}}}
    fd_hashtable_iterkeys(&(ix->adds),fd_table_add,n,keys,value);
    if (!(FD_VOIDP(ix->has_slotids))) extend_slotids(ix,keys,n);
    if (ix->cache_level>0)
      fd_hashtable_iterkeys(&(ix->cache),fd_table_add_if_present,n,keys,value);}
  else {
    if ((FD_WRITETHROUGH_THREADCACHE)&&(fdtc)) {
      struct FD_PAIR tempkey;
      FD_INIT_STATIC_CONS(&tempkey,fd_pair_type);
      tempkey.car=fd_index2lisp(ix); tempkey.cdr=key;
      if (fd_hashtable_probe(&(fdtc->indices),(fdtype)&tempkey)) {
        fd_hashtable_add(&(fdtc->indices),(fdtype)&tempkey,value);}}
    fd_hashtable_add(&(ix->adds),key,value);
    if (ix->cache_level>0)
      fd_hashtable_op(&(ix->cache),fd_table_add_if_present,key,value);}
  if ((ix->flags&FD_INDEX_IN_BACKGROUND) &&
      (fd_background->cache.n_keys)) {
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key);
      fd_hashtable_iterkeys
        (&(fd_background->cache),fd_table_replace,n,keys,FD_VOID);}
    else fd_hashtable_op(&(fd_background->cache),fd_table_replace,key,FD_VOID);}
  if (!(FD_VOIDP(ix->has_slotids))) extend_slotids(ix,&key,1);
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
  if (ix->read_only) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_add",u8_strdup(ix->cid),FD_VOID);
    return -1;}
  else init_cache_level(ix);
  if (FD_CHOICEP(key)) {
    FD_DO_CHOICES(eachkey,key) {
      fdtype drop_key=fd_make_pair(drop_symbol,eachkey);
      fdtype set_key=fd_make_pair(set_symbol,eachkey);
      fd_hashtable_add(&(ix->edits),drop_key,value);
      fd_hashtable_drop(&(ix->edits),set_key,value);
      fd_hashtable_drop(&(ix->adds),eachkey,value);
      if (ix->cache_level>0)
        fd_hashtable_drop(&(ix->cache),eachkey,value);
      fd_decref(set_key); fd_decref(drop_key);}}
  else {
    fdtype drop_key=fd_make_pair(drop_symbol,key);
    fdtype set_key=fd_make_pair(set_symbol,key);
    fd_hashtable_add(&(ix->edits),drop_key,value);
    fd_hashtable_drop(&(ix->edits),set_key,value);
    fd_hashtable_drop(&(ix->adds),key,value);
    if (ix->cache_level>0)
      fd_hashtable_drop(&(ix->cache),key,value);
    fd_decref(set_key); fd_decref(drop_key);}
  if ((ix->flags&FD_INDEX_IN_BACKGROUND) &&
      (fd_background->cache.n_keys)) {
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key);
      fd_hashtable_iterkeys
        (&(fd_background->cache),fd_table_replace,n,keys,FD_VOID);}
    else fd_hashtable_op(&(fd_background->cache),fd_table_replace,key,FD_VOID);}
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
  if (ix->read_only) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_store",u8_strdup(ix->cid),FD_VOID);
    return -1;}
  else init_cache_level(ix);
  if (FD_CHOICEP(key)) {
    FD_DO_CHOICES(eachkey,key) {
      fdtype set_key=fd_make_pair(set_symbol,eachkey);
      fdtype drop_key=fd_make_pair(drop_symbol,eachkey);
      fd_hashtable_store(&(ix->edits),set_key,value);
      fd_hashtable_op(&(ix->edits),fd_table_replace,drop_key,FD_EMPTY_CHOICE);
      if (ix->cache_level>0) {
        fd_hashtable_store(&(ix->cache),eachkey,value);
        fd_hashtable_op(&(ix->adds),fd_table_replace,eachkey,FD_VOID);}
      fd_decref(set_key); fd_decref(drop_key);}}
  else {
    fdtype set_key=fd_make_pair(set_symbol,key);
    fdtype drop_key=fd_make_pair(drop_symbol,key);
    fd_hashtable_store(&(ix->edits),set_key,value);
    fd_hashtable_op(&(ix->edits),fd_table_replace,drop_key,FD_EMPTY_CHOICE);
    if (ix->cache_level>0) {
      fd_hashtable_store(&(ix->cache),key,value);
      fd_hashtable_op(&(ix->adds),fd_table_replace,key,FD_VOID);}
    fd_decref(set_key); fd_decref(drop_key);}
  if ((ix->flags&FD_INDEX_IN_BACKGROUND) &&
      (fd_background->cache.n_keys)) {
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key);
      fd_hashtable_iterkeys
        (&(fd_background->cache),fd_table_replace,n,keys,FD_VOID);}
    else fd_hashtable_op(&(fd_background->cache),fd_table_replace,key,FD_VOID);}
  return 1;
}
static int table_indexstore(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix=fd_indexptr(ixarg);
  if (ix) return fd_index_store(ix,key,value);
  else return -1;
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
  if ((ix->adds.n_slots) || (ix->edits.n_slots)) {
    int n_keys=ix->adds.n_keys+ix->edits.n_keys, retval=0;
    double start_time=u8_elapsed_time();
    if (ix->cache_level<0) {
      ix->cache_level=fd_default_cache_level;
      if (ix->handler->setcache)
        ix->handler->setcache(ix,fd_default_cache_level);}
    retval=ix->handler->commit(ix);
    if (retval<0)
      u8_log(LOG_CRIT,fd_Commitment,
             _("Error saving %d keys to %s after %f secs"),
             n_keys,ix->cid,u8_elapsed_time()-start_time);
    else if (retval>0)
      u8_log(fddb_loglevel,fd_Commitment,
             _("Saved %d keys to %s in %f secs"),
             retval,ix->cid,u8_elapsed_time()-start_time);
    else {}
    if (retval<0)
      u8_seterr(fd_IndexCommitError,"fd_index_commit",u8_strdup(ix->cid));
    return retval;}
  else return 0;
}

FD_EXPORT void fd_index_swapout(fd_index ix)
{
  if ((((ix->flags)&FD_INDEX_NOSWAP)==0) && (ix->cache.n_keys)) {
    if ((ix->flags)&(FD_STICKY_CACHESIZE))
      fd_reset_hashtable(&(ix->cache),-1,1);
    else fd_reset_hashtable(&(ix->cache),0,1);}
}

FD_EXPORT void fd_index_close(fd_index ix)
{
  if ((ix) && (ix->handler) && (ix->handler->close))
    ix->handler->close(ix);
}

/* Common init function */

FD_EXPORT void fd_init_index
  (fd_index ix,struct FD_INDEX_HANDLER *h,u8_string source,int consed)
{
  if (consed) {FD_INIT_CONS(ix,fd_raw_index_type);}
  else {FD_INIT_STATIC_CONS(ix,fd_raw_index_type);}
  ix->serialno=-1; ix->cache_level=-1; ix->read_only=1;
  ix->flags=((h->fetchn)?(FD_INDEX_BATCHABLE):(0));
  FD_INIT_STATIC_CONS(&(ix->cache),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(ix->adds),fd_hashtable_type);
  FD_INIT_STATIC_CONS(&(ix->edits),fd_hashtable_type);
  fd_make_hashtable(&(ix->cache),0);
  fd_make_hashtable(&(ix->adds),0);
  fd_make_hashtable(&(ix->edits),0);
  ix->handler=h;
  ix->cid=u8_strdup(source);
  ix->source=u8_strdup(source);
  ix->xid=NULL;
  ix->has_slotids=FD_VOID;
}

static int unparse_index(u8_output out,fdtype x)
{
  fd_index ix=fd_indexptr(x); u8_string type;
  if (ix==NULL) return 0;
  if ((ix->handler) && (ix->handler->name)) type=ix->handler->name;
  else type="unrecognized";
  if ((ix->xid) && (strcmp(ix->source,ix->xid)))
    u8_printf(out,_("#<INDEX %s 0x%lx \"%s|%s\">"),
              type,x,ix->source,ix->xid);
  else u8_printf(out,_("#<INDEX %s 0x%lx \"%s\">"),type,x,ix->source);
  return 1;
}

static int unparse_raw_index(u8_output out,fdtype x)
{
  fd_index ix=(fd_index)(x); u8_string type;
  if (ix==NULL) return 0;
  if ((ix->handler) && (ix->handler->name)) type=ix->handler->name;
  else type="unrecognized";
  if ((ix->xid) && (strcmp(ix->source,ix->xid)))
    u8_printf(out,_("#<INDEX %s 0x%lx \"%s|%s\">"),
              type,x,ix->source,ix->xid);
  else u8_printf(out,_("#<INDEX %s 0x%lx \"%s\">"),type,x,ix->source);
  return 1;
}

static fdtype index_parsefn(int n,fdtype *args,fd_compound_entry e)
{
  fd_index ix=NULL;
  if (n<2) return FD_VOID;
  else if (FD_STRINGP(args[2]))
    ix=fd_open_index(FD_STRING_DATA(args[2]));
  if (ix) return fd_index2lisp(ix);
  else return fd_err(fd_CantParseRecord,"index_parsefn",NULL,FD_VOID);
}

/* Operations over all indices */

FD_EXPORT void fd_swapout_indices()
{
  int i=0; while (i < fd_n_primary_indices) {
    fd_index_swapout(fd_primary_indices[i]); i++;}
  i=0; while (i < fd_n_secondary_indices) {
    fd_index_swapout(fd_secondary_indices[i]); i++;}
}

FD_EXPORT void fd_close_indices()
{
  int i=0; while (i < fd_n_primary_indices) {
    fd_index_close(fd_primary_indices[i]); i++;}
  i=0; while (i < fd_n_secondary_indices) {
    fd_index_close(fd_secondary_indices[i]); i++;}
}

FD_EXPORT int fd_commit_indices()
{
  int count=0, i=0; while (i < fd_n_primary_indices) {
    int retval=fd_index_commit(fd_primary_indices[i]);
    if (retval<0) return retval;
    else count=count+retval;
    i++;}
  i=0; while (i < fd_n_secondary_indices) {
    int retval=fd_index_commit(fd_secondary_indices[i]);
    if (retval<0) return retval;
    else count=count+retval;
    i++;}
  return count;
}

FD_EXPORT int fd_commit_indices_noerr()
{
  int count=0;
  int i=0; while (i < fd_n_primary_indices) {
    int retval=fd_index_commit(fd_primary_indices[i]);
    if (retval<0) {
      u8_log(LOG_CRIT,"INDEX_COMMIT_FAIL","Error committing %s",
              fd_primary_indices[i]->cid);
      fd_clear_errors(1);
      count=-1;}
    if (count>=0) count=count+retval;
    i++;}
  i=0; while (i < fd_n_secondary_indices) {
    int retval=fd_index_commit(fd_secondary_indices[i]);
    if (retval<0) {
      u8_log(LOG_CRIT,"INDEX_COMMIT_FAIL","Error committing %s",
              fd_secondary_indices[i]->cid);
      fd_clear_errors(1);
      count=-1;}
    if (count>=0) count=count+retval;
    i++;}
  return count;
}

FD_EXPORT int fd_for_indices(int (*fcn)(fd_index ix,void *),void *data)
{
  int i=0; while (i < fd_n_primary_indices) {
    int retval=fcn(fd_primary_indices[i],data);
    if (retval<0) return retval;
    else if (retval) break;
    else i++;}
  if (i>=fd_n_primary_indices) return i;
  i=0; while (i < fd_n_secondary_indices) {
    int retval=fcn(fd_secondary_indices[i],data);
    if (retval<0) return retval;
    else if (retval) break;
    else i++;}
  return i;
}

static int accumulate_cachecount(fd_index ix,void *ptr)
{
  int *count=(int *)ptr;
  *count=*count+ix->cache.n_keys;
  return 0;
}

FD_EXPORT
long fd_index_cache_load()
{
  int result=0, retval;
  retval=fd_for_indices(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(fd_index ix,void *ptr)
{
  fdtype *vals=(fdtype *)ptr;
  fdtype keys=fd_hashtable_keys(&(ix->cache));
  FD_ADD_TO_CHOICE(*vals,keys);
  return 0;
}

FD_EXPORT
fdtype fd_cached_keys(fd_index ix)
{
  if (ix==NULL) {
    int retval; fdtype result=FD_EMPTY_CHOICE;
    retval=fd_for_indices(accumulate_cached,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return FD_ERROR_VALUE;}
    else return result;}
  else return fd_hashtable_keys(&(ix->cache));
}

/* IPEVAL delay execution */

FD_EXPORT int fd_execute_index_delays(fd_index ix,void *data)
{
  fdtype *delays=get_index_delays();
  fdtype todo=delays[ix->serialno];
  if (FD_EMPTY_CHOICEP(todo)) return 0;
  else {
    int retval=-1;
    /* fd_lock_mutex(&(fd_ipeval_lock)); */
    todo=delays[ix->serialno];
    delays[ix->serialno]=FD_EMPTY_CHOICE;
    /* fd_unlock_mutex(&(fd_ipeval_lock)); */
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval>1)
      u8_log(LOG_NOTICE,ipeval_ixfetch,"Fetching %d keys from %s: %q",
             FD_CHOICE_SIZE(todo),ix->cid,todo);
    else
#endif
    retval=fd_index_prefetch(ix,todo);
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval)
      u8_log(LOG_NOTICE,ipeval_ixfetch,"Fetched %d keys from %s",
             FD_CHOICE_SIZE(todo),ix->cid);
#endif
    if (retval<0) return retval;
    else return 0;}
}

/* The in-memory index */

static fdtype *memindex_fetchn(fd_index ix,int n,fdtype *keys)
{
  fdtype *results=u8_alloc_n(n,fdtype);
  int i=0; while (i<n) {
    results[i]=fd_hashtable_get(&(ix->cache),keys[i],FD_EMPTY_CHOICE);
    i++;}
  return results;
}

static fdtype *memindex_fetchkeys(fd_index ix,int *n)
{
  fdtype keys=fd_hashtable_keys(&(ix->cache));
  int n_elts=FD_CHOICE_SIZE(keys);
  fdtype *result=u8_alloc_n(n_elts,fdtype);
  int j=0;
  FD_DO_CHOICES(key,keys) {result[j++]=key;}
  *n=n_elts;
  return result;
}

static int memindex_fetchsizes_helper(fdtype key,fdtype value,void *ptr)
{
  struct FD_KEY_SIZE **key_size_ptr=(struct FD_KEY_SIZE **)ptr;
  (*key_size_ptr)->key=fd_incref(key);
  (*key_size_ptr)->n_values=FD_CHOICE_SIZE(value);
  *key_size_ptr=(*key_size_ptr)+1;
  return 0;
}

static struct FD_KEY_SIZE *memindex_fetchsizes(fd_index ix,int *n)
{
  if (ix->cache.n_keys) {
    struct FD_KEY_SIZE *sizes, *write; int n_keys;
    n_keys=ix->cache.n_keys;
    sizes=u8_alloc_n(n_keys,FD_KEY_SIZE); write=&(sizes[0]);
    fd_for_hashtable(&(ix->cache),memindex_fetchsizes_helper,
                     (void *)write,1);
    *n=n_keys;
    return sizes;}
  else {
    *n=0; return NULL;}
}

static int memindex_commit(fd_index ix)
{
  struct FD_MEM_INDEX *mix=(struct FD_MEM_INDEX *)ix;
  if ((mix->source) && (mix->commitfn))
    return (mix->commitfn)(mix,mix->source);
  else {
    fd_seterr(fd_EphemeralIndex,"memindex_commit",u8_strdup(ix->cid),FD_VOID);
    return -1;}
}

static struct FD_INDEX_HANDLER memindex_handler={
  "memindex", 1, sizeof(struct FD_MEM_INDEX), 12,
  NULL, /* close */
  memindex_commit, /* commit */
  NULL, /* setcache */
  NULL, /* setbuf */
  NULL, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  memindex_fetchn, /* fetchn */
  memindex_fetchkeys, /* fetchkeys */
  memindex_fetchsizes, /* fetchsizes */
  NULL, /* metadata */
  NULL /* sync */
};

FD_EXPORT
fd_index fd_make_mem_index(int consed)
{
  struct FD_MEM_INDEX *mix=u8_alloc(struct FD_MEM_INDEX);
  FD_INIT_STRUCT(mix,struct FD_MEM_INDEX);
  fd_init_index((fd_index)mix,&memindex_handler,"ephemeral",consed);
  mix->cache_level=1; mix->read_only=0; mix->flags=FD_INDEX_NOSWAP;
  fd_register_index((fd_index)mix);
  return (fd_index)mix;
}

/* FETCH indices */

FD_EXPORT
fd_index fd_make_extindex
  (u8_string name,fdtype fetchfn,fdtype commitfn,fdtype state,int reg)
{
  struct FD_EXTINDEX *fetchix=u8_alloc(struct FD_EXTINDEX);
  FD_INIT_STRUCT(fetchix,struct FD_EXTINDEX);
  fd_init_index((fd_index)fetchix,&fd_extindex_handler,name,(!(reg)));
  fetchix->cache_level=1;
  fetchix->read_only=(FD_VOIDP(commitfn));
  fetchix->fetchfn=fd_incref(fetchfn);
  fetchix->commitfn=fd_incref(commitfn);
  fetchix->state=fd_incref(state);
  if (reg) fd_register_index((fd_index)fetchix);
  else fetchix->serialno=-1;
  return (fd_index)fetchix;
}

static fdtype extindex_fetch(fd_index p,fdtype oid)
{
  struct FD_EXTINDEX *xp=(fd_extindex)p;
  fdtype state=xp->state, fetchfn=xp->fetchfn, value;
  struct FD_FUNCTION *fptr=((FD_FUNCTIONP(fetchfn))?
                            ((struct FD_FUNCTION *)fetchfn):
                            (NULL));
  if ((FD_VOIDP(state))||(FD_FALSEP(state))||
      ((fptr)&&(fptr->arity==1)))
    value=fd_apply(fetchfn,1,&oid);
  else {
    fdtype args[2]; args[0]=oid; args[1]=state;
    value=fd_apply(fetchfn,2,args);}
  return value;
}

/* This assumes that the FETCH function handles a vector intelligently,
   and just calls it on the vector of OIDs and extracts the data from
   the returned vector.  */
static fdtype *extindex_fetchn(fd_index p,int n,fdtype *keys)
{
  struct FD_EXTINDEX *xp=(fd_extindex)p;
  struct FD_VECTOR vstruct; fdtype vecarg;
  fdtype state=xp->state, fetchfn=xp->fetchfn, value=FD_VOID;
  struct FD_FUNCTION *fptr=((FD_FUNCTIONP(fetchfn))?
                            ((struct FD_FUNCTION *)fetchfn):
                            (NULL));
  if (!((p->flags)&(FD_INDEX_BATCHABLE))) return NULL;
  FD_INIT_STATIC_CONS(&vstruct,fd_vector_type);
  vstruct.length=n; vstruct.data=keys; vstruct.freedata=0;
  vecarg=FDTYPE_CONS(&vstruct);
  if ((FD_VOIDP(state))||(FD_FALSEP(state))||
      ((fptr)&&(fptr->arity==1)))
    value=fd_apply(xp->fetchfn,1,&vecarg);
  else {
    fdtype args[2]; args[0]=vecarg; args[1]=state;
    value=fd_apply(xp->fetchfn,2,args);}
  if (FD_ABORTP(value)) return NULL;
  else if (FD_VECTORP(value)) {
    struct FD_VECTOR *vstruct=(struct FD_VECTOR *)value;
    fdtype *results=u8_alloc_n(n,fdtype);
    memcpy(results,vstruct->data,sizeof(fdtype)*n);
    /* Free the CONS itself (and maybe data), to avoid DECREF/INCREF
       of values. */
    if (vstruct->freedata) u8_free(vstruct->data);
    u8_free((struct FD_CONS *)value);
    return results;}
  else {
    fdtype *values=u8_alloc_n(n,fdtype);
    if ((FD_VOIDP(state))||(FD_FALSEP(state))||
        ((fptr)&&(fptr->arity==1))) {
      int i=0; while (i<n) {
        fdtype key=keys[i];
        fdtype value=fd_apply(fetchfn,1,&key);
        if (FD_ABORTP(value)) {
          int j=0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}
    else {
      fdtype args[2]; int i=0; args[1]=state;
      i=0; while (i<n) {
        fdtype key=keys[i], value;
        args[0]=key; value=fd_apply(fetchfn,2,args);
        if (FD_ABORTP(value)) {
          int j=0; while (j<i) {fd_decref(values[j]); j++;}
          u8_free(values);
          return NULL;}
        else values[i++]=value;}
      return values;}}
}

static int extindex_commit(fd_index ix)
{
  struct FD_EXTINDEX *exi=(fd_extindex)ix;
  fdtype *adds, *drops, *stores;
  int n_adds=0, n_drops=0, n_stores=0;
  fd_write_lock_struct(&(exi->adds));
  fd_write_lock_struct(&(exi->edits));
  if (exi->edits.n_keys) {
    drops=u8_alloc_n(exi->edits.n_keys,fdtype);
    stores=u8_alloc_n(exi->edits.n_keys,fdtype);}
  else {drops=NULL; stores=NULL;}
  if (exi->adds.n_keys)
    adds=u8_alloc_n(exi->adds.n_keys,fdtype);
  else adds=NULL;
  if (exi->edits.n_keys) {
    int n_edits;
    struct FD_KEYVAL *kvals=fd_hashtable_keyvals(&(exi->edits),&n_edits,0);
    struct FD_KEYVAL *scan=kvals, *limit=kvals+n_edits;
    while (scan<limit) {
      fdtype key=scan->key;
      if (FD_PAIRP(key)) {
        fdtype kind=FD_CAR(key), realkey=FD_CDR(key), value=scan->value;
        fdtype assoc=fd_conspair(realkey,value);
        if (FD_EQ(kind,set_symbol)) {
          stores[n_stores++]=assoc; fd_incref(realkey);}
        else if (FD_EQ(kind,drop_symbol)) {
          drops[n_drops++]=assoc; fd_incref(realkey);}
        else u8_raise(_("Bad edit key in index"),"fd_extindex_commit",NULL);}
      else u8_raise(_("Bad edit key in index"),"fd_extindex_commit",NULL);
      scan++;}
    scan=kvals; while (scan<kvals) {
      fd_decref(scan->key);
      /* Not neccessary because we used the pointer above. */
      /* fd_decref(scan->value); */
      scan++;}}
  if (exi->adds.n_keys) {
    int add_len;
    struct FD_KEYVAL *kvals=fd_hashtable_keyvals(&(exi->adds),&add_len,0);
    /* Note that we don't have to decref these kvals because
       their pointers will become in the assocs in the adds vector. */
    struct FD_KEYVAL *scan=kvals, *limit=kvals+add_len;
    while (scan<limit) {
      fdtype key=scan->key, value=scan->value;
      fdtype assoc=fd_conspair(key,value);
      adds[n_adds++]=assoc;
      scan++;}}
  {
    fdtype avec=fd_init_vector(NULL,n_adds,adds);
    fdtype dvec=fd_init_vector(NULL,n_drops,drops);
    fdtype svec=fd_init_vector(NULL,n_stores,stores);
    fdtype argv[4], result=FD_VOID;
    argv[0]=avec;
    argv[1]=dvec;
    argv[2]=svec;
    argv[3]=exi->state;
    result=fd_apply(exi->commitfn,((FD_VOIDP(exi->state))?(3):(4)),argv);
    fd_decref(argv[0]); fd_decref(argv[1]); fd_decref(argv[2]);
    fd_reset_hashtable(&(exi->adds),67,0);
    fd_reset_hashtable(&(exi->edits),67,0);
    fd_rw_unlock_struct(&(exi->adds));
    fd_rw_unlock_struct(&(exi->edits));
    if (FD_ABORTP(result)) return -1;
    else {fd_decref(result); return 1;}
  }
}

struct FD_INDEX_HANDLER fd_extindex_handler={
  "extindexhandler", 1, sizeof(struct FD_EXTINDEX), 4,
  NULL, /* close */
  extindex_commit, /* commit */
  NULL, /* setcache */
  NULL, /* setbuf */
  extindex_fetch, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  extindex_fetchn, /* fetchn */
  NULL, /* fetchkeys */
  NULL, /* fetchsizes */
  NULL /* sync */
};

static void recycle_raw_index(struct FD_CONS *c)
{
  struct FD_INDEX *ix=(struct FD_INDEX *)c;
  if (ix->handler==&fd_extindex_handler) {
    struct FD_EXTINDEX *ei=(struct FD_EXTINDEX *)c;
    fd_decref(ei->fetchfn);
    fd_decref(ei->commitfn);
    fd_decref(ei->state);}
  fd_recycle_hashtable(&(ix->cache));
  fd_recycle_hashtable(&(ix->adds));
  fd_recycle_hashtable(&(ix->edits));
  u8_free(ix->cid);
  u8_free(ix->source);
  u8_free(ix);
}

/* Initialize */

fd_ptr_type fd_index_type, fd_raw_index_type;

static int check_index(fdtype x)
{
  int serial=FD_GET_IMMEDIATE(x,fd_index_type);
  if (serial<0) return 0;
  if (serial<FD_N_PRIMARY_INDICES)
    if (fd_primary_indices[serial]) return 1;
    else return 0;
  else return (fd_secondary_indices[serial-FD_N_PRIMARY_INDICES]!=NULL);
}

FD_EXPORT fd_index _fd_indexptr(fdtype x)
{
  return fd_indexptr(x);
}

/* Initializations */

FD_EXPORT void fd_init_indices_c()
{
  u8_register_source_file(_FILEINFO);

  fd_index_type=fd_register_immediate_type("index",check_index);
  fd_raw_index_type=fd_register_cons_type("raw index");

  {
    struct FD_COMPOUND_ENTRY *e=fd_register_compound(fd_intern("INDEX"),NULL,NULL);
    e->parser=index_parsefn;}

  fd_tablefns[fd_index_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_index_type]->get=table_indexget;
  fd_tablefns[fd_index_type]->add=table_indexadd;
  fd_tablefns[fd_index_type]->drop=table_indexdrop;
  fd_tablefns[fd_index_type]->store=table_indexstore;
  fd_tablefns[fd_index_type]->test=NULL;
  fd_tablefns[fd_index_type]->keys=table_indexkeys;
  fd_tablefns[fd_index_type]->getsize=NULL;

  fd_tablefns[fd_raw_index_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_raw_index_type]->get=table_indexget;
  fd_tablefns[fd_raw_index_type]->add=table_indexadd;
  fd_tablefns[fd_raw_index_type]->drop=table_indexdrop;
  fd_tablefns[fd_raw_index_type]->store=table_indexstore;
  fd_tablefns[fd_raw_index_type]->test=NULL;
  fd_tablefns[fd_raw_index_type]->keys=table_indexkeys;
  fd_tablefns[fd_raw_index_type]->getsize=NULL;

  fd_recyclers[fd_raw_index_type]=recycle_raw_index;
  fd_unparsers[fd_raw_index_type]=unparse_raw_index;

  set_symbol=fd_make_symbol("SET",3);
  drop_symbol=fd_make_symbol("DROP",4);
  fd_unparsers[fd_index_type]=unparse_index;
#if FD_THREADS_ENABLED
  fd_init_mutex(&indices_lock);
  fd_init_mutex(&background_lock);
#endif
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
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
