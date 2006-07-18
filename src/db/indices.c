/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_INDICES 1
#define FD_INLINE_IPEVAL 1

#include "fdb/dtype.h"
#include "fdb/tables.h"
#include "fdb/indices.h"

#include <libu8/libu8.h>
#include <libu8/filefns.h>

fd_exception fd_EphemeralIndex=_("ephemeral index");
fd_exception fd_ReadOnlyIndex=_("read-only index");
fd_exception fd_NoFileIndices=_("file indices are not supported");
fd_exception fd_NotAFileIndex=_("not a file index");
fd_exception fd_BadIndexSpec=_("bad index specification");
static u8_condition ipeval_ixfetch="IXFETCH";

fd_index (*fd_file_index_opener)(u8_string)=NULL;

fd_index fd_primary_indices[FD_N_PRIMARY_INDICES], *fd_secondary_indices=NULL;
int fd_n_primary_indices=0, fd_n_secondary_indices=0;

#if FD_THREADS_ENABLED
static u8_mutex background_lock;
#endif

struct FD_COMPOUND_INDEX *fd_background=NULL;

#if FD_GLOBAL_IPEVAL
static fdtype *index_delays;
#elif FD_USE_TLS
static u8_tld_key index_delays_key;
#elif FD_USE__THREAD
static __thread fdtype *index_delays;
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
    delays=u8_malloc(sizeof(fdtype)*FD_N_INDEX_DELAYS);
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
    index_delays=u8_malloc(sizeof(fdtype)*FD_N_INDEX_DELAYS);
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
  if (ix->handler->setcache)
    ix->handler->setcache(ix,level);
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
    u8_lock_mutex(&indices_lock);
    if (ix->serialno>=0) { /* Handle race condition */
      u8_unlock_mutex(&indices_lock); return;}
    if (fd_n_primary_indices<FD_N_PRIMARY_INDICES) {
      ix->serialno=fd_n_primary_indices;
      fd_primary_indices[fd_n_primary_indices++]=ix;}
    else {
      if (fd_secondary_indices) 
	fd_secondary_indices=u8_realloc
	  (fd_secondary_indices,sizeof(fd_index)*(fd_n_secondary_indices+1));
      else fd_secondary_indices=u8_malloc(sizeof(fd_index));
      ix->serialno=fd_n_secondary_indices+FD_N_PRIMARY_INDICES;
      fd_secondary_indices[fd_n_secondary_indices++]=ix;}
    u8_unlock_mutex(&indices_lock);}
}

FD_EXPORT fdtype fd_index2lisp(fd_index ix)
{
  return FDTYPE_IMMEDIATE(fd_index_type,ix->serialno);
}
FD_EXPORT fd_index fd_lisp2index(fdtype lix)
{
  if (FD_PRIM_TYPEP(lix,fd_index_type)) {
    int serial=FD_GET_IMMEDIATE(lix,fd_index_type);
    if (serial<FD_N_PRIMARY_INDICES) return fd_primary_indices[serial];
    else return fd_secondary_indices[serial-FD_N_PRIMARY_INDICES];}
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

FD_EXPORT fd_index fd_open_index(u8_string spec)
{
  if (strchr(spec,';')) {
    fd_seterr(fd_BadIndexSpec,"fd_open_index",u8_strdup(spec),FD_VOID);
    return NULL;}
  else if (strchr(spec,'@')) {
    fd_index known=fd_find_index_by_cid(spec);
    u8_byte *at=strchr(spec,'@');
    if (known) return known;
    else if (strchr(at+1,'@')) {
      u8_byte buf[64]; fdtype xname;
      if (at-spec>63) return NULL;
      strncpy(buf,spec,at-spec); buf[at-spec]='\0';
      xname=fd_parse(buf);
      return fd_open_network_index(at+1,xname);}
    else return fd_open_network_index(spec,FD_VOID);}
  else if (fd_file_index_opener)
    return fd_file_index_opener(spec);
  else {
    fd_seterr3(fd_NoFileIndices,"fd_open_index",u8_strdup(spec));
    return NULL;}
}

/* Background indices */

FD_EXPORT int fd_add_to_background(fd_index ix)
{
  if (ix==NULL) return 0;
  u8_lock_mutex(&background_lock);
  ix->flags=ix->flags|FD_INDEX_IN_BACKGROUND;
  if (fd_background) 
    fd_add_to_compound_index(fd_background,ix);
  else {
    fd_index *indices=u8_malloc(sizeof(fd_index));
    indices[0]=ix;
    fd_background=
      (struct FD_COMPOUND_INDEX *)fd_make_compound_index(1,indices);}
  u8_unlock_mutex(&background_lock);
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

FD_EXPORT fdtype fd_index_fetch(fd_index ix,fdtype key)
{
  fdtype v;
  init_cache_level(ix);
  if (ix->edits.n_keys) {
    fdtype value;
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
  else if (ix->handler->fetch)
    v=ix->handler->fetch(ix,key);
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
      FD_ADD_TO_CHOICE((*delayp),fd_incref(key));
      delay_count++;}}
  if (delay_count) fd_ipeval_delay(delay_count);
}

FD_EXPORT void fd_delay_index_fetch(fd_index ix,fdtype keys)
{
  delay_index_fetch(ix,keys);
}

FD_EXPORT int fd_index_prefetch(fd_index ix,fdtype keys)
{
  fdtype *keyvec=NULL, *values; int free_keys=0, n_fetched=0;
  if (ix == NULL) return -1;
  else init_cache_level(ix);
  if (ix->handler->prefetch != NULL)
    if (fd_ipeval_status()) {
      delay_index_fetch(ix,keys);
      return 0;}
    else return ix->handler->prefetch(ix,keys);
  else if (ix->handler->fetchn==NULL)
    if (fd_ipeval_status()) {
      delay_index_fetch(ix,keys);
      return 0;}
    else {
      FD_DO_CHOICES(key,keys)
	if (!(fd_hashtable_probe(&(ix->cache),key))) {
	  fdtype v=fd_index_fetch(ix,key);
	  if (FD_ABORTP(v)) return fd_interr(v);
	  n_fetched++;
	  fd_hashtable_store(&(ix->cache),key,v);
	  fd_decref(v);}
      return n_fetched;}
  if (ix->cache_level==0) return 0;
  if (FD_ACHOICEP(keys)) {keys=fd_make_simple_choice(keys); free_keys=1;}
  if (fd_ipeval_status()) delay_index_fetch(ix,keys);
  else if (!(FD_CHOICEP(keys))) {
    if (!(fd_hashtable_probe(&(ix->cache),keys))) {
      fdtype v=fd_index_fetch(ix,keys);
      if (FD_ABORTP(v)) return fd_interr(v);
      n_fetched=1;
      fd_hashtable_store(&(ix->cache),keys,v);
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
	      (fd_hashtable_probe(edits,set_key))))
	  if (write) *write++=key;
	  else if (FD_VOIDP(singlekey)) singlekey=key;
	  else {
	    write=keyvec=u8_malloc(sizeof(fdtype)*FD_CHOICE_SIZE(keys));
	    write[0]=singlekey; write[1]=key; write=write+2;}
	fd_decref(set_key);}}
    else {
      FD_DO_CHOICES(key,keys) 
	if (!(fd_hashtable_probe(cache,key)))
	  if (write) *write++=key;
	  else if (FD_VOIDP(singlekey)) singlekey=key;
	  else {
	    write=keyvec=u8_malloc(sizeof(fdtype)*FD_CHOICE_SIZE(keys));
	    write[0]=singlekey; write[1]=key; write=write+2;}}
    if (write==NULL)
      if (FD_VOIDP(singlekey)) {}
      else {
	fdtype v=fd_index_fetch(ix,singlekey); n_fetched=1;
	fd_hashtable_store(&(ix->cache),singlekey,v);
	fd_decref(v);}
    else if (write==keyvec) n_fetched=0;
    else {
      unsigned int i=0, n=write-keyvec;
      n_fetched=n;
      values=ix->handler->fetchn(ix,n,keyvec);
      if (values==NULL) n_fetched=-1;
      else if (ix->edits.n_keys) /* When there are drops or sets */
	while (i < n) {
	  fdtype key=keyvec[i], value=values[i];
	  fdtype drop_key=fd_make_pair(drop_symbol,key);
	  fdtype adds=fd_hashtable_get(&(ix->adds),key,FD_EMPTY_CHOICE);
	  fdtype drops=
	    fd_hashtable_get(&(ix->edits),drop_key,FD_EMPTY_CHOICE);
	  if (FD_EMPTY_CHOICEP(drops))
	    if (FD_EMPTY_CHOICEP(adds)) {
	      fd_hashtable_store(cache,keyvec[i],values[i]);
	      fd_decref(values[i]);}
	    else {
	      fdtype simple;
	      FD_ADD_TO_CHOICE(values[i],adds);
	      simple=fd_simplify_choice(values[i]);
	      fd_hashtable_store(cache,keyvec[i],simple);
	      fd_decref(simple);}
	  else {
	    fdtype oldv=values[i], newv;
	    FD_ADD_TO_CHOICE(oldv,adds);
	    newv=fd_difference(oldv,drops);
	    newv=fd_simplify_choice(newv);
	    fd_hashtable_store(cache,keyvec[i],newv);
	    fd_decref(oldv); fd_decref(newv);}
	  fd_decref(drops); i++;}
      else if (ix->adds.n_keys)  /* When there are just adds */
	while (i < n) {
	  fdtype key=keyvec[i];
	  fdtype value=values[i];
	  fdtype adds=fd_hashtable_get(&(ix->adds),key,FD_EMPTY_CHOICE);
	  if (FD_EMPTY_CHOICEP(adds)) {
	    fd_hashtable_store(cache,keyvec[i],values[i]);
	    fd_decref(values[i]);}
	  else {
	    FD_ADD_TO_CHOICE(values[i],adds);
	    values[i]=fd_simplify_choice(values[i]);
	    fd_hashtable_store(cache,keyvec[i],values[i]);
	    fd_decref(values[i]);}
	  i++;}
      else {
	/* When we're just storing. */
	fd_hashtable_iter(cache,fd_table_store_noref,n,keyvec,values);
	/* This is no longer needed with fd_table_store_noref */
	/* while (i < n) {fd_decref(values[i]); i++;} */
      }}}
  if (keyvec) {
    u8_free(keyvec); u8_free(values);}
  if (free_keys) fd_decref(keys);
  return n_fetched;
}

FD_EXPORT fdtype fd_index_keys(fd_index ix)
{
  if (ix->handler->fetchkeys) {
    fdtype current=ix->handler->fetchkeys(ix);
    if (ix->adds.n_keys) {
      fdtype added=fd_hashtable_keys(&(ix->adds));
      FD_ADD_TO_CHOICE(current,added);}
    return current;}
  else return fd_err(fd_NoMethod,"fd_index_keys",NULL,fd_index2lisp(ix));
}
static fdtype table_getkeys(fdtype ixarg)
{
  fd_index ix=fd_lisp2index(ixarg);
  if (ix) return fd_index_keys(ix);
  else return fd_erreify();
}

FD_EXPORT fdtype fd_index_sizes(fd_index ix)
{
  if (ix->handler->fetchsizes) 
    return ix->handler->fetchsizes(ix);
  else return fd_err(fd_NoMethod,"fd_index_sizes",NULL,fd_index2lisp(ix));
}
FD_EXPORT fdtype _fd_index_get(fd_index ix,fdtype key)
{
  fdtype cached=fd_hashtable_get(&(ix->cache),key,FD_VOID);
  if (FD_VOIDP(cached))
    if (fd_ipeval_status(1)) {
      delay_index_fetch(ix,key);
      return FD_EMPTY_CHOICE;}
    else return fd_index_fetch(ix,key);
  else return cached;
}
static fdtype table_indexget(fdtype ixarg,fdtype key,fdtype dflt)
{
  fd_index ix=fd_lisp2index(ixarg);
  if (ix) {
    fdtype v=fd_index_get(ix,key);
    if (FD_EMPTY_CHOICEP(v)) return fd_incref(dflt);
    else return v;}
  else return fd_erreify();
}


FD_EXPORT int _fd_index_add(fd_index ix,fdtype key,fdtype value)
{
  if (ix->read_only) {
    fd_seterr(fd_ReadOnlyIndex,"_fd_index_add",u8_strdup(ix->cid),FD_VOID);
    return -1;}
  else init_cache_level(ix);
  if (FD_EMPTY_CHOICEP(value)) {}
  else if (FD_CHOICEP(key)) {
    const fdtype *keys=FD_CHOICE_DATA(key);
    unsigned int n=FD_CHOICE_SIZE(key), retval;
    fd_hashtable_iterkeys(&(ix->adds),fd_table_add,n,keys,value);
    if (ix->cache_level>0)
      fd_hashtable_iterkeys(&(ix->cache),fd_table_add_if_present,n,keys,value);}
  else {
    int retval;
    fd_hashtable_add(&(ix->adds),key,value);
    if (ix->cache_level>0)
      fd_hashtable_op(&(ix->cache),fd_table_add_if_present,key,value);}
  if ((ix->flags&FD_INDEX_IN_BACKGROUND) &&
      (fd_background->cache.n_keys))
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key), retval;
      fd_hashtable_iterkeys
	(&(fd_background->cache),fd_table_replace,n,keys,FD_VOID);}
    else fd_hashtable_op(&(fd_background->cache),fd_table_replace,key,FD_VOID);
  return 1;
}
static int table_indexadd(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix=fd_lisp2index(ixarg); 
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
      (fd_background->cache.n_keys))
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key), retval;
      fd_hashtable_iterkeys(&(fd_background->cache),fd_table_replace,n,keys,FD_VOID);}
    else fd_hashtable_op(&(fd_background->cache),fd_table_replace,key,FD_VOID);
  return 1;
}
static int table_indexdrop(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix=fd_lisp2index(ixarg); 
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
      (fd_background->cache.n_keys))
    if (FD_CHOICEP(key)) {
      const fdtype *keys=FD_CHOICE_DATA(key);
      unsigned int n=FD_CHOICE_SIZE(key), retval;
      fd_hashtable_iterkeys(&(fd_background->cache),fd_table_replace,n,keys,FD_VOID);}
    else fd_hashtable_op(&(fd_background->cache),fd_table_replace,key,FD_VOID);
  return 1;
}
static int table_indexstore(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix=fd_lisp2index(ixarg); 
  if (ix) return fd_index_store(ix,key,value);
  else return -1;
}

static fdtype table_indexkeys(fdtype ixarg)
{
  fd_index ix=fd_lisp2index(ixarg); 
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
      u8_notify(fd_Commitment,_("Error saving %d keys to %s after %f secs"),
		n_keys,ix->cid,u8_elapsed_time()-start_time);
    else if (retval>0)
      u8_notify(fd_Commitment,_("Saved %d keys to %s in %f secs"),
		retval,ix->cid,u8_elapsed_time()-start_time);
    else {}
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
  (fd_index ix,struct FD_INDEX_HANDLER *h,u8_string source)
{
  ix->serialno=-1; ix->cache_level=-1; ix->read_only=1; ix->flags=0;
  fd_make_hashtable(&(ix->cache),0,NULL);
  fd_make_hashtable(&(ix->adds),0,NULL);
  fd_make_hashtable(&(ix->edits),0,NULL); 
  ix->handler=h;
  ix->cid=u8_strdup(source);
  ix->source=u8_strdup(source);
  ix->xid=NULL;
}

static int unparse_index(u8_output out,fdtype x)
{
  fd_index ix=fd_lisp2index(x);
  if (ix==NULL) return 0;
  if (ix->xid)
    u8_printf(out,_("#<INDEX 0x%lx \"%s|%s\">"),
	      x,ix->source,ix->xid);
  else u8_printf(out,_("#<INDEX 0x%lx \"%s\">"),x,ix->source);
  return 1;
}

static fdtype index_parsefn(FD_MEMORY_POOL_TYPE *p,int n,fdtype *args)
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
      u8_warn("INDEX_COMMIT_FAIL","Error committing %s",
	      fd_primary_indices[i]->cid);
      fd_clear_errors(1);
      count=-1;}
    if (count>=0) count=count+retval;
    i++;}
  i=0; while (i < fd_n_secondary_indices) {
    int retval=fd_index_commit(fd_secondary_indices[i]);
    if (retval<0) {
      u8_warn("INDEX_COMMIT_FAIL","Error committing %s",
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
int fd_cachecount_indices()
{
  int result=0, retval;
  retval=fd_for_indices(accumulate_cachecount,(void *)&result);
  if (retval<0) return retval;
  else return result;
}

static int accumulate_cached(fd_index ix,void *ptr)
{
  fdtype *vals=(fdtype *)ptr;
  FD_ADD_TO_CHOICE(*vals,fd_hashtable_keys(&(ix->cache)));
  return 0;
}

FD_EXPORT
fdtype fd_cached_keys(fd_index ix)
{
  if (ix==NULL) {
    int retval; fdtype result=FD_EMPTY_CHOICE;
    fd_for_indices(accumulate_cached,(void *)&result);
    if (retval<0) {
      fd_decref(result);
      return fd_erreify();}
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
    /* u8_lock_mutex(&(fd_ipeval_lock)); */
    todo=delays[ix->serialno];
    delays[ix->serialno]=FD_EMPTY_CHOICE;
    /* u8_unlock_mutex(&(fd_ipeval_lock)); */
#if FD_TRACE_IPEVAL
    if (fd_trace_ipeval>1)
      u8_notify(ipeval_ixfetch,"Fetching %d keys from %s: %q",
		FD_CHOICE_SIZE(todo),ix->cid,todo);
    else if (fd_trace_ipeval)
      u8_notify(ipeval_ixfetch,"Fetching %d keys from %s",
		FD_CHOICE_SIZE(todo),ix->cid);
#endif
    return fd_index_prefetch(ix,todo);}
}

/* Config methods */

static fdtype config_get_indices(fdtype var,void *data)
{
  fdtype results=FD_EMPTY_CHOICE;
  int i=0; while (i < fd_n_primary_indices) {
    fdtype lindex=fd_index2lisp(fd_primary_indices[i]);
    FD_ADD_TO_CHOICE(results,lindex);
    i++;}
  if (i>=fd_n_primary_indices) return results;
  i=0; while (i < fd_n_secondary_indices) {
    fdtype lindex=fd_index2lisp(fd_secondary_indices[i]);
    FD_ADD_TO_CHOICE(results,lindex);
    i++;}
  return results;
}
static int config_use_index(fdtype var,fdtype spec,void *data)
{
  if (FD_STRINGP(spec))
    if (fd_use_index(FD_STRDATA(spec))) return 1;
    else return -1;
  else {
    fd_seterr(fd_TypeError,"config_use_index",NULL,fd_incref(spec));
    return -1;}
}

/* The in-memory index */

static fdtype *memindex_fetchn(fd_index ix,int n,fdtype *keys)
{
  fdtype *results=u8_malloc(sizeof(fdtype)*n);
  int i=0; while (i<n) {
    results[i]=fd_hashtable_get(&(ix->cache),keys[i],FD_EMPTY_CHOICE);
    i++;}
  return results;
}

static fdtype memindex_fetchkeys(fd_index ix)
{
  return fd_hashtable_keys(&(ix->cache));
}

static int memindex_fetchsizes_helper(fdtype key,fdtype value,void *ptr)
{
  fdtype *results=(fdtype *)ptr;
  fdtype entry=fd_init_pair(NULL,fd_incref(key),FD_INT2DTYPE(FD_CHOICE_SIZE(value)));
  FD_ADD_TO_CHOICE(*results,entry);
  return 0;
}

static fdtype memindex_fetchsizes(fd_index ix)
{
  fdtype results=FD_EMPTY_CHOICE;
  fd_for_hashtable(&(ix->cache),memindex_fetchsizes_helper,(void *)&results,1);
  return results;
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
fd_index fd_make_mem_index()
{
  struct FD_MEM_INDEX *mix=u8_malloc(sizeof(struct FD_MEM_INDEX));
  fd_init_index((fd_index)mix,&memindex_handler,"ephemeral");
  mix->cache_level=1; mix->read_only=0; mix->flags=FD_INDEX_NOSWAP;
  fd_register_index((fd_index)mix);
  return (fd_index)mix;
}

/* Initialize */

fd_ptr_type fd_index_type;

static int check_index(fdtype x)
{
  int serial=FD_GET_IMMEDIATE(x,fd_index_type); 
  if (serial<0) return 0;
  if (serial<FD_N_PRIMARY_INDICES)
    if (fd_primary_indices[serial]) return 1;
    else return 0;
  else return (fd_secondary_indices[serial-FD_N_PRIMARY_INDICES]!=NULL);
}

FD_EXPORT fd_init_indices_c()
{
  fd_register_source_file(versionid);

  fd_index_type=fd_register_immediate_type("index",check_index);
  
  {
    struct FD_COMPOUND_ENTRY *e=fd_register_compound(fd_intern("INDEX"));
    e->parser=index_parsefn;}

  fd_register_config("INDICES",config_get_indices,config_use_index,NULL);

  fd_tablefns[fd_index_type]=u8_malloc_type(struct FD_TABLEFNS);
  fd_tablefns[fd_index_type]->get=table_indexget;
  fd_tablefns[fd_index_type]->add=table_indexadd;
  fd_tablefns[fd_index_type]->drop=table_indexdrop;
  fd_tablefns[fd_index_type]->store=table_indexstore;
  fd_tablefns[fd_index_type]->test=NULL;
  fd_tablefns[fd_index_type]->keys=table_indexkeys;
  fd_tablefns[fd_index_type]->getsize=NULL;
  set_symbol=fd_make_symbol("SET",3);
  drop_symbol=fd_make_symbol("DROP",4);
  fd_unparsers[fd_index_type]=unparse_index;
#if FD_THREADS_ENABLED
  u8_init_mutex(&indices_lock);
  u8_init_mutex(&background_lock);
#endif
#if ((FD_USE_TLS) && (!(FD_GLOBAL_IPEVAL)))
  u8_new_threadkey(&index_delays_key,NULL);
#endif
}


/* The CVS log for this file
   $Log: indices.c,v $
   Revision 1.96  2006/02/07 16:07:17  haase
   Initialize offsets cache (when neccessary) before prefetching

   Revision 1.95  2006/01/31 13:47:23  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.94  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.93  2006/01/16 17:58:07  haase
   Fixes to empty choice cases for indices and better error handling

   Revision 1.92  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.91  2005/12/28 23:03:29  haase
   Made choices be direct blocks of elements, including various fixes, simplifications, and more detailed documentation.

   Revision 1.90  2005/12/26 20:14:26  haase
   Further fixes to type reorganization

   Revision 1.89  2005/11/29 17:53:25  haase
   Catch file index overflows and 0/1 cases of index getkeys and getsizes

   Revision 1.88  2005/11/08 15:22:29  haase
   Made (COMMIT) ignore (but warn) on individual errors for now

   Revision 1.87  2005/09/04 20:24:09  haase
   Added flag to avoid swapping memindices out into oblivion

   Revision 1.86  2005/08/10 06:34:08  haase
   Changed module name to fdb, moving header file as well

   Revision 1.85  2005/08/07 21:04:53  haase
   Made index changes to background components invalidate entries in the background cache.

   Revision 1.84  2005/08/04 23:24:49  haase
   Don't report zero keys saved

   Revision 1.83  2005/07/14 01:42:50  haase
   Added memindices which are completely in-memory tables

   Revision 1.82  2005/07/13 22:11:57  haase
   Fixed index modification to do the right thing with an uncached index

   Revision 1.81  2005/07/12 21:12:18  haase
   Whitespace changes

   Revision 1.80  2005/06/22 23:19:45  haase
   Fixed repeated time statements in commitment reports

   Revision 1.79  2005/06/20 02:06:42  haase
   Various GC related fixes

   Revision 1.78  2005/06/05 23:15:22  haase
   Fixed bug in CONFIG INDICES return

   Revision 1.77  2005/06/05 03:42:28  haase
   Index simplifications and bug fixes for store/add combinations

   Revision 1.76  2005/06/04 18:42:41  haase
   Added getkeys method for indices

   Revision 1.75  2005/06/04 15:02:50  haase
   Fix index commit reporting error

   Revision 1.74  2005/06/04 12:44:09  haase
   Fixed error catching for prefetches

   Revision 1.73  2005/05/29 22:38:47  haase
   Simplified db layer fd_use_pool and fd_use_index

   Revision 1.72  2005/05/26 12:41:11  haase
   More return value error handling fixes

   Revision 1.71  2005/05/26 11:03:21  haase
   Fixed some bugs with read-only pools and indices

   Revision 1.70  2005/05/23 00:53:24  haase
   Fixes to header ordering to get off_t consistently defined

   Revision 1.69  2005/05/18 19:25:19  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.68  2005/05/17 22:09:24  haase
   Added commit reports

   Revision 1.67  2005/04/25 13:11:34  haase
   Added cache size counting

   Revision 1.66  2005/04/24 01:46:48  haase
   move delay pointer entry out of loop

   Revision 1.65  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.64  2005/04/09 22:35:37  haase
   Make pool and index unparsing decline for invalid values

   Revision 1.63  2005/03/30 15:30:00  haase
   Made calls to new seterr do appropriate strdups

   Revision 1.62  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.61  2005/03/29 01:51:24  haase
   Added U8_MUTEX_DECL and used it

   Revision 1.60  2005/03/26 19:09:13  haase
   Made index and pool commits set the cache level

   Revision 1.59  2005/03/26 18:31:41  haase
   Various configuration fixes

   Revision 1.58  2005/03/26 04:46:58  haase
   Added fd_index_sizes

   Revision 1.57  2005/03/23 21:47:29  haase
   Added return values for use pool/index primitives and added CONFIG interface to pool and index usage

   Revision 1.56  2005/03/06 18:28:21  haase
   Fixed parser to return errors

   Revision 1.55  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.54  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.53  2005/03/03 17:58:15  haase
   Moved stdio dependencies out of fddb and reorganized make structure

   Revision 1.52  2005/03/01 23:13:54  haase
   Fixes to index commitment implementation

   Revision 1.51  2005/03/01 19:41:30  haase
   Fixed table methods for indices to do lisp2index conversion

   Revision 1.50  2005/02/27 03:01:59  haase
   Made fd_lisp2index call fd_seterr when it fails

   Revision 1.49  2005/02/26 21:37:26  haase
   Made index operations possibly return error codes

   Revision 1.48  2005/02/24 19:12:46  haase
   Fixes to handling index arguments which are strings specifiying index sources

   Revision 1.47  2005/02/23 22:49:27  haase
   Created generalized compound registry and made compound dtypes and #< reading use it

   Revision 1.46  2005/02/22 21:31:48  haase
   Add immediate checkers for pools and indices

   Revision 1.45  2005/02/21 22:13:08  haase
   Added readable record printing

   Revision 1.44  2005/02/19 16:25:02  haase
   Replaced fd_parse with fd_intern

   Revision 1.43  2005/02/15 22:56:21  haase
   Fixed bug introduced with index cleanups

   Revision 1.42  2005/02/15 15:17:40  haase
   Added automatic handling of .pool and .index files

   Revision 1.41  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.40  2005/02/11 04:46:01  haase
   Made prefetching use _noref hashtable ops

   Revision 1.39  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
