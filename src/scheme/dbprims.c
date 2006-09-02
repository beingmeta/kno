/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/frames.h"
#include "fdb/methods.h"
#include "fdb/sequences.h"

static fdtype pools_symbol, indices_symbol, id_symbol;

/* Finding frames, etc. */

static fdtype find_frames_lexpr(int n,fdtype *args)
{
  return fd_finder(args[0],n-1,args+1);
}

static void hashtable_index_frame(fdtype ix,fdtype frames,fdtype slotids,fdtype values)
{
  if (FD_VOIDP(values)) {
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
	fdtype values=((FD_OIDP(frame)) ? (fd_frame_get(frame,slotid)) : (fd_get(frame,slotid,FD_EMPTY_CHOICE)));
	FD_DO_CHOICES(value,values) {
	  fdtype key=fd_init_pair(NULL,fd_incref(slotid),fd_incref(value));
	  fd_add(ix,key,frame);
	  fd_decref(key);}
	fd_decref(values);}}}
  else {
    FD_DO_CHOICES(slotid,slotids) {
      FD_DO_CHOICES(value,values) {
	fdtype key=fd_init_pair(NULL,fd_incref(slotid),fd_incref(value));
	fd_add(ix,key,frames);
	fd_decref(key);}}}
}

static fdtype index_frame_prim
  (fdtype indices,fdtype frames,fdtype slotids,fdtype values)
{
  if (FD_CHOICEP(indices)) {
    FD_DO_CHOICES(index,indices)
      if (FD_HASHTABLEP(index))
	hashtable_index_frame(index,frames,slotids,values);
      else {
	fd_index ix=fd_lisp2index(index);
	if (ix==NULL) return fd_erreify();
	else if (fd_index_frame(ix,frames,slotids,values)<0)
	  return fd_erreify();}}
  else if (FD_HASHTABLEP(indices)) {
    hashtable_index_frame(indices,frames,slotids,values);
    return FD_VOID;}
  else {
    fd_index ix=fd_lisp2index(indices);
    if (ix==NULL) return fd_erreify();
    else if (fd_index_frame(ix,frames,slotids,values)<0)
      return fd_erreify();}
  return FD_VOID;
}

/* Pool and index functions */

static fdtype poolp(fdtype arg)
{
  if (FD_POOLP(arg)) return FD_TRUE; else return FD_FALSE;
}

static fdtype indexp(fdtype arg)
{
  if (FD_INDEXP(arg)) return FD_TRUE; else return FD_FALSE;
}

static fdtype getpool(fdtype arg)
{
  fd_pool p=NULL;
  if (FD_POOLP(arg)) return fd_incref(arg);
  else if (FD_STRINGP(arg)) 
    p=fd_name2pool(FD_STRDATA(arg));
  else if (FD_OIDP(arg)) p=fd_oid2pool(arg);
  if (p) return fd_pool2lisp(p);
  else return FD_EMPTY_CHOICE;
}

static fdtype use_pool(fdtype arg1,fdtype arg2)
{
  if (FD_POOLP(arg1)) return fd_incref(arg1);
  else if (!(FD_STRINGP(arg1)))
    return fd_type_error(_("string"),"use_pool",arg1);
  else if (FD_VOIDP(arg2)) {
    fdtype results=FD_EMPTY_CHOICE;
    u8_byte *copy=u8_strdup(FD_STRDATA(arg1));
    u8_byte *start=copy, *end=strchr(start,';');
    if (end) *end='\0'; while (start) {
      if (strchr(start,'@')) {
	fdtype temp;
	if (fd_use_pool(start)==NULL) {
	  fd_decref(results); u8_free(copy);
	  return fd_erreify();}
	else temp=fd_find_pools_by_cid(start);
	FD_ADD_TO_CHOICE(results,temp);}
      else {
	fd_pool p=fd_use_pool(start);
	if (p==NULL) {
	  fd_decref(results); u8_free(copy);
	  return fd_erreify();}
	else {
	  fdtype pval=fd_pool2lisp(p);
	  FD_ADD_TO_CHOICE(results,pval);}}
      if ((end) && (end[1])) {
	start=end+1; end=strchr(start,';');
	if (end) *end='\0';}
      else start=NULL;}
    u8_free(copy);
    return results;}
  else if (!(FD_STRINGP(arg2)))
    return fd_type_error(_("string"),"use_pool",arg2);
  else {
    fd_pool p=fd_name2pool(FD_STRDATA(arg2));
    if (p==NULL) p=fd_use_pool(FD_STRDATA(arg1));
    if (p) return fd_pool2lisp(p);
    else return fd_erreify();}
}

static fdtype use_index(fdtype arg)
{
  fd_index ix=NULL;
  if (FD_INDEXP(arg)) {
    ix=fd_lisp2index(arg);
    fd_add_to_background(ix);
    return fd_incref(arg);}
  else if (FD_STRINGP(arg))
    if (strchr(FD_STRDATA(arg),';')) {
      /* We explicitly handle ; separated arguments here, so that
	 we can return the choice of indices. */
      fdtype results=FD_EMPTY_CHOICE;
      u8_byte *copy=u8_strdup(FD_STRDATA(arg));
      u8_byte *start=copy, *end=strchr(start,';');
      *end='\0'; while (start) {
	fd_index ix=fd_use_index(start);
	if (ix==NULL) {
	  u8_free(copy);
	  fd_decref(results);
	  return fd_erreify();}
	else {
	  fdtype ixv=fd_index2lisp(ix);
	  FD_ADD_TO_CHOICE(results,ixv);}
	if ((end) && (end[1])) {
	  start=end+1; end=strchr(start,';');
	  if (end) *end='\0';}
	else start=NULL;}
      u8_free(copy);
      return results;}
    else ix=fd_use_index(FD_STRDATA(arg));
  else return fd_type_error(_("index spec"),"use_index",arg);
  if (ix) return fd_index2lisp(ix);
  else return fd_erreify();
}

static fdtype open_index(fdtype arg)
{
  fd_index ix=NULL;
  if (FD_STRINGP(arg))
    if (strchr(FD_STRDATA(arg),';')) {
      /* We explicitly handle ; separated arguments here, so that
	 we can return the choice of indices. */
      fdtype results=FD_EMPTY_CHOICE;
      u8_byte *copy=u8_strdup(FD_STRDATA(arg));
      u8_byte *start=copy, *end=strchr(start,';');
      *end='\0'; while (start) {
	fd_index ix=fd_open_index(start);
	if (ix==NULL) {
	  u8_free(copy);
	  fd_decref(results);
	  return fd_erreify();}
	else {
	  fdtype ixv=fd_index2lisp(ix);
	  FD_ADD_TO_CHOICE(results,ixv);}
	if ((end) && (end[1])) {
	  start=end+1; end=strchr(start,';');
	  if (end) *end='\0';}
	else start=NULL;}
      u8_free(copy);
      return results;}
    else ix=fd_open_index(FD_STRDATA(arg));
  else if (FD_INDEXP(arg)) ix=fd_lisp2index(arg);
  else fd_seterr(fd_TypeError,"use_index",NULL,fd_incref(arg));
  if (ix) return fd_index2lisp(ix);
  else return fd_erreify();
}
static fdtype oidvalue(fdtype arg)
{
  return fd_oid_value(arg);
}
static fdtype setoidvalue(fdtype o,fdtype v)
{
  int retval;
  if (FD_SLOTMAPP(v)) {
    v=fd_deep_copy(v); 
    FD_SLOTMAP_MARK_MODIFIED(v);}
  else if (FD_SCHEMAPP(v)) {
    v=fd_deep_copy(v); 
    FD_SCHEMAP_MARK_MODIFIED(v);}
  else if (FD_HASHTABLEP(v)) {
    v=fd_deep_copy(v); 
    FD_HASHTABLE_MARK_MODIFIED(v);}
  else v=fd_incref(v);
  retval=fd_set_oid_value(o,v);
  fd_decref(v);
  if (retval<0) return fd_erreify();
  else return FD_VOID;
}

static fdtype lockoid(fdtype o,fdtype soft)
{
  int retval=fd_lock_oid(o);
  if (retval<0)
    if (FD_TRUEP(soft)) {
      fd_poperr(NULL,NULL,NULL,NULL);
      return FD_FALSE;}
    else return fd_erreify();
  else return FD_INT2DTYPE(retval);
}

static fdtype lockoids(fdtype oids)
{
  int retval=fd_lock_oids(oids);
  if (retval<0)
    return fd_erreify();
  else return FD_INT2DTYPE(retval);
}

static fdtype make_compound_index(int n,fdtype *args)
{
  fd_index *sources=u8_malloc(sizeof(fd_index)*8);
  int n_sources=0, max_sources=8;
  int i=0; while (i<n) {
    FD_DO_CHOICES(source,args[i]) {
      fd_index ix=NULL;
      if (FD_STRINGP(source)) ix=fd_open_index(fd_strdata(source));
      else if (FD_INDEXP(source)) ix=fd_lisp2index(source);
      else if (FD_SYMBOLP(source)) {
	fdtype val=fd_config_get(FD_SYMBOL_NAME(source));
	if (FD_STRINGP(val)) ix=fd_open_index(fd_strdata(val));
	else if (FD_INDEXP(val)) ix=fd_lisp2index(source);}
      if (ix) {
	if (n_sources>=max_sources) {
	  sources=u8_realloc(sources,sizeof(fd_index)*(max_sources+8));
	  max_sources=max_sources+8;}
	sources[n_sources++]=ix;}
      else {
	u8_free(sources);
	return fd_type_error("index","make_compound_index",source);}}
    i++;}
  return fd_index2lisp(fd_make_compound_index(n_sources,sources));
}

static fdtype add_to_compound_index(fdtype lcx,fdtype aix)
{
  if (FD_INDEXP(lcx)) {
    fd_index ix=fd_lisp2index(lcx);
    if (fd_add_to_compound_index((struct FD_COMPOUND_INDEX *)ix,
				 fd_lisp2index(aix))<0)
      return fd_erreify();
    else return FD_VOID;}
  else return fd_type_error(_("index"),"add_to_compound_index",lcx);
}

/* Adding adjuncts */

static fdtype adjunct_symbol;

static fdtype use_adjunct(fdtype index_arg,fdtype slotid,fdtype pool_arg)
{
  fd_index ix=fd_lisp2index(index_arg);
  if (ix==NULL) return fd_erreify();
  if (FD_VOIDP(slotid)) slotid=fd_index_get(ix,adjunct_symbol);
  if ((FD_SYMBOLP(slotid)) || (FD_OIDP(slotid)))
    if (FD_VOIDP(pool_arg))
      if (fd_set_adjunct(ix,slotid,NULL)<0) return fd_erreify();
      else return FD_VOID;
    else {
      fd_pool p=fd_lisp2pool(pool_arg);
      if (p==NULL) return fd_erreify();
      else if (fd_set_adjunct(ix,slotid,p)<0) return fd_erreify();
      else return FD_VOID;}
  else return fd_type_error(_("slotid"),"use_adjunct",slotid);
}

/* Cache calls */

static struct FD_HASHTABLE fcn_caches;

static struct FD_HASHTABLE *get_fcn_cache(fdtype fcn,int create)
{
  fdtype cache=fd_hashtable_get(&fcn_caches,fcn,FD_VOID);
  if (FD_VOIDP(cache)) {
    cache=fd_make_hashtable(NULL,512,NULL);
    fd_hashtable_store(&fcn_caches,fcn,cache);
    return FD_GET_CONS(cache,fd_hashtable_type,struct FD_HASHTABLE *);}
  else return FD_GET_CONS(cache,fd_hashtable_type,struct FD_HASHTABLE *);
}

static fdtype cachecall(int n,fdtype *args)
{
  fdtype fcn=args[0], vec, cached;
  struct FD_HASHTABLE *cache=get_fcn_cache(fcn,1);
  struct FD_VECTOR vecstruct;
  vecstruct.consbits=fd_vector_type;
  vecstruct.length=n-1;
  vecstruct.data=((n==1) ? (NULL) : (args+1));
  vec=FDTYPE_CONS(&vecstruct);
  cached=fd_hashtable_get(cache,vec,FD_VOID);
  if (FD_VOIDP(cached)) {
    int state=fd_ipeval_status();
    fdtype result=fd_dapply((struct FD_FUNCTION *)fcn,n-1,args+1);
    if (FD_EXCEPTIONP(result)) {
      fd_decref((fdtype)cache);
      return result;}
    else if (fd_ipeval_status()==state) {
      fdtype *datavec=((n-1) ? (u8_malloc(sizeof(fdtype)*(n-1))) : (NULL));
      fdtype key=fd_init_vector(NULL,n-1,datavec);
      int i=0, lim=n-1; while (i<lim) {
	datavec[i]=fd_incref(args[i+1]); i++;}
      fd_hashtable_store(cache,key,result);
      fd_decref(key);}
    fd_decref((fdtype)cache);
    return result;}
  else {
    fd_decref((fdtype)cache);
    return cached;}
}

FD_EXPORT void fd_clear_callcache()
{
  fd_reset_hashtable(&fcn_caches,128,1);
}

static fdtype clear_callcache(fdtype arg)
{
  if (FD_VOIDP(arg)) fd_reset_hashtable(&fcn_caches,128,1);
  else fd_hashtable_store(&fcn_caches,arg,FD_VOID);
  return FD_VOID;
}


/* DB control functions */

static fdtype swapout_lexpr(int n,fdtype *args)
{
  if (n == 0) {
    fd_swapout_indices();
    fd_swapout_pools();
    return FD_VOID;}
  else if (n == 1) {
    fdtype arg=args[0];
    if (FD_OIDP(arg)) fd_swapout_oid(arg);
    else if (FD_PRIM_TYPEP(arg,fd_index_type))
      fd_index_swapout(fd_lisp2index(arg));
    else if (FD_PRIM_TYPEP(arg,fd_pool_type))
      fd_pool_swapout(fd_lisp2pool(arg));
    else return fd_type_error(_("pool or index"),"swapout_lexpr",arg);
    return FD_VOID;}
  else return fd_err(fd_TooManyArgs,"swapout",NULL,FD_VOID);
}

static fdtype commit_lexpr(int n,fdtype *args)
{
  if (n == 0) {
    if (fd_commit_indices()<0)
      return fd_erreify();
    if (fd_commit_pools()<0)
      return fd_erreify();
    return FD_VOID;}
  else if (n == 1) {
    fdtype arg=args[0]; int retval=0;
    if (FD_PRIM_TYPEP(arg,fd_index_type))
      retval=fd_index_commit(fd_lisp2index(arg));
    else if (FD_PRIM_TYPEP(arg,fd_pool_type))
      retval=fd_pool_commit_all(fd_lisp2pool(arg),1);
    else return fd_type_error(_("pool or index"),"commit_lexpr",arg);
    if (retval<0) return fd_erreify();
    else return FD_VOID;}
  else return fd_err(fd_TooManyArgs,"commit",NULL,FD_VOID);
}

static fdtype clear_slotcache(fdtype arg)
{
  if (FD_VOIDP(arg)) fd_clear_slotcaches();
  else fd_clear_slotcache(arg);
  return FD_VOID;
}

static fdtype clearcaches()
{
  fd_clear_slotcaches();
  fd_clear_callcache();
  fd_swapout_indices();
  fd_swapout_pools();
  return FD_VOID;
}

static fd_pool arg2pool(fdtype arg)
{
  if (FD_POOLP(arg)) return fd_lisp2pool(arg);
  else if (FD_STRINGP(arg)) {
    fd_pool p=fd_name2pool(FD_STRDATA(arg));
    if (p) return p;
    else return fd_use_pool(FD_STRDATA(arg));}
  else if (FD_SYMBOLP(arg)) {
    fdtype v=fd_config_get(FD_SYMBOL_NAME(arg));
    if (FD_STRINGP(v)) return fd_use_pool(FD_STRDATA(v));
    else return NULL;}
  else return NULL;
}

static fdtype pool_load(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_load",arg);
  else {
    int load=fd_pool_load(p);
    if (load>=0) return FD_INT2DTYPE(load);
    else return fd_erreify();}
}

static fdtype pool_capacity(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_capacity",arg);
  else return FD_INT2DTYPE(p->capacity);
}

static fdtype pool_base(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_base",arg);
  else return fd_make_oid(p->base);
}

static fdtype pool_elts(fdtype arg,fdtype start,fdtype count)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_elts",arg);
  else {
    int i=0, lim=fd_pool_load(p);
    fdtype result=FD_EMPTY_CHOICE;
    FD_OID base=p->base, scan=base;
    if (lim<0) return fd_erreify();
    if (FD_VOIDP(start)) {}
    else if (FD_FIXNUMP(start))
      if (FD_FIX2INT(start)<0)
	return fd_type_error(_("pool offset"),"pool_elts",start);
      else i=FD_FIX2INT(start);
    else if (FD_OIDP(start)) i=FD_OID_DIFFERENCE(FD_OID_ADDR(start),base);
    else return fd_type_error(_("pool offset"),"pool_elts",start);
    if (FD_VOIDP(count)) {}
    else if (FD_FIXNUMP(count)) {
      int count_arg=FD_FIX2INT(count);
      if (count_arg<0)
	return fd_type_error(_("pool offset"),"pool_elts",count);
      else if (i+count_arg<lim) lim=i+count_arg;}
    else if (FD_OIDP(start)) {
      int lim_arg=FD_OID_DIFFERENCE(FD_OID_ADDR(count),base);
      if (lim_arg<lim) lim=lim_arg;}
    else return fd_type_error(_("pool offset"),"pool_elts",count);
    while (i<lim) {
      fdtype each=fd_make_oid(FD_OID_PLUS(base,i));
      FD_ADD_TO_CHOICE(result,each); i++;}
    return result;}
}

static fdtype pool_label(fdtype arg,fdtype use_source)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_label",arg);
  else if (p->label)
    return fdtype_string(p->label);
  else if (FD_FALSEP(use_source)) return FD_FALSE;
  else if (p->source)
    return fdtype_string(p->label);
  else return FD_FALSE;
}

static fdtype pool_close_prim(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_close",arg);
  else {
    fd_pool_close(p);
    return FD_VOID;}
}

static fdtype oid_range(fdtype start,fdtype end)
{
  int i=0, lim=fd_getint(end); 
  fdtype result=FD_EMPTY_CHOICE;
  FD_OID base=FD_OID_ADDR(start), scan=base;
  if (lim<0) return fd_erreify();
  else while (i<lim) {
    fdtype each=fd_make_oid(FD_OID_PLUS(base,i));
    FD_ADD_TO_CHOICE(result,each); i++;}
  return result;
}

static fdtype oid_vector(fdtype start,fdtype end)
{
  int i=0, lim=fd_getint(end); 
  if (lim<0) return fd_erreify();
  else {
    fdtype result=fd_init_vector(NULL,lim,NULL);
    fdtype *data=FD_VECTOR_DATA(result);
    FD_OID base=FD_OID_ADDR(start), scan=base; 
    while (i<lim) {
      fdtype each=fd_make_oid(FD_OID_PLUS(base,i));
      data[i++]=each;}
    return result;}
}

static fdtype random_oid(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"random_oid",arg);
  else {
    int load=fd_pool_load(p);
    if (load) {
      FD_OID base=p->base; int i=u8_random(load);
      return fd_make_oid(FD_OID_PLUS(base,i));}
    return FD_EMPTY_CHOICE;}
}

static fdtype pool_vec(fdtype arg)
{
  fd_pool p=arg2pool(arg);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"pool_vec",arg);
  else {
    int i=0, lim=fd_pool_load(p);
    fdtype result=fd_init_vector(NULL,lim,NULL);
    FD_OID base=p->base, scan=base;
    if (lim<0) return fd_erreify();
    else while (i<lim) {
      fdtype each=fd_make_oid(FD_OID_PLUS(base,i));
      FD_VECTOR_SET(result,i,each); i++;}
    return result;}
}

static fdtype cachecount(fdtype arg)
{
  fd_pool p=NULL; fd_index ix=NULL;
  if (FD_VOIDP(arg)) {
    int count=fd_cachecount_pools()+fd_cachecount_indices();
    return FD_INT2DTYPE(count);}
  else if (FD_EQ(arg,pools_symbol)) {
    int count=fd_cachecount_pools();
    return FD_INT2DTYPE(count);}
  else if (FD_EQ(arg,indices_symbol)) {
    int count=fd_cachecount_indices();
    return FD_INT2DTYPE(count);}
  else if (p=(fd_lisp2pool(arg))) {
    int count=p->cache.n_keys;
    return FD_INT2DTYPE(count);}
  else if (ix=(fd_lisp2index(arg))) {
    int count=ix->cache.n_keys;
    return FD_INT2DTYPE(count);}
  else return fd_type_error(_("pool or index"),"cachecount",arg);
}

/* OID functions */

static fdtype oidhi(fdtype x)
{
  FD_OID addr=FD_OID_ADDR(x);
  return FD_INT2DTYPE(FD_OID_HI(addr));
}

static fdtype oidlo(fdtype x)
{
  FD_OID addr=FD_OID_ADDR(x);
  return FD_INT2DTYPE(FD_OID_LO(addr));
}

static fdtype oidp(fdtype x)
{
  if (FD_OIDP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype oidpool(fdtype x)
{
  fd_pool p=fd_oid2pool(x);
  if (p==NULL) return FD_EMPTY_CHOICE;
  else return fd_pool2lisp(p);
}

static fdtype inpoolp(fdtype x,fdtype pool_arg)
{
  fd_pool p=fd_lisp2pool(pool_arg);
  fd_pool op=fd_oid2pool(x);
  if (p == op) return FD_TRUE;
  else return FD_FALSE;
}

/* Prefetching functions */

static fdtype prefetch_oids(fdtype oids)
{
  if (fd_prefetch_oids(oids)>=0) return FD_TRUE;
  else return fd_erreify();
}

static fdtype prefetch_keys(fdtype arg1,fdtype arg2)
{
  if (FD_VOIDP(arg2)) {
    if (fd_bg_prefetch(arg1)<0)
      return fd_erreify();
    else return FD_VOID;}
  else {
    FD_DO_CHOICES(arg,arg1) {
      if (FD_INDEXP(arg)) {
	fd_index ix=fd_lisp2index(arg);
	if (fd_index_prefetch(ix,arg2)<0) {
	  FD_STOP_DO_CHOICES;
	  return fd_erreify();}}
      else return fd_type_error(_("index"),"prefetch_keys",arg);}
    return FD_VOID;}
}

/* Getting cached OIDs */

static fdtype cached_oids(fdtype pool)
{
  if ((FD_VOIDP(pool)) || (FD_TRUEP(pool)))
    return fd_cached_oids(NULL);
  else {
    fd_pool p=fd_lisp2pool(pool);
    if (p) return fd_cached_oids(p);
    else return fd_type_error(_("pool"),"cached_oids",pool);}
}

static fdtype cached_keys(fdtype index)
{
  if ((FD_VOIDP(index)) || (FD_TRUEP(index)))
    return fd_cached_keys(NULL);
  else {
    fd_index ix=fd_lisp2index(index);
    if (ix) return fd_cached_keys(ix);
    else return fd_type_error(_("index"),"cached_keys",index);}
}

/* Frame get functions */

static fdtype fget(fdtype frames,fdtype slotids)
{
  int all_adjuncts=1;
  if (FD_CHOICEP(slotids)) {
    FD_DO_CHOICES(slotid,slotids) {
      int adjunctp=0;
      FD_DO_CHOICES(adjslotid,fd_adjunct_slotids) {
	if (FD_EQ(slotid,adjslotid)) {adjunctp=1; break;}}
      if (adjunctp==0) {all_adjuncts=0; break;}}}
  else {
    int adjunctp=0;
    FD_DO_CHOICES(adjslotid,fd_adjunct_slotids) {
      if (FD_EQ(slotids,adjslotid)) {adjunctp=1; break;}}
    if (adjunctp==0) all_adjuncts=0;}
  if ((fd_prefetch) && (fd_ipeval_status()==0) &&
      (FD_CHOICEP(frames)) && (all_adjuncts==0))
    fd_prefetch_oids(frames);
  {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(frame,frames)
      if (FD_OIDP(frame)) {
	FD_DO_CHOICES(slotid,slotids) {
	  fdtype v=fd_frame_get(frame,slotid);
	  if (FD_ABORTP(v)) {
	    fd_decref(results); return v;}
	  else {FD_ADD_TO_CHOICE(results,v);}}}
      else {
	FD_DO_CHOICES(slotid,slotids) {
	  fdtype v=fd_get(frame,slotid,FD_EMPTY_CHOICE);
	  if (FD_ABORTP(v)) {
	    fd_decref(results); return v;}
	  else {FD_ADD_TO_CHOICE(results,v);}}}
    return fd_simplify_choice(results);}
}

static fdtype fassert(fdtype frames,fdtype slotids,fdtype values)
{
  if (FD_EMPTY_CHOICEP(values)) return FD_VOID;
  else {
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
	FD_DO_CHOICES(value,values) {
	  if (fd_frame_add(frame,slotid,value)<0)
	    return fd_erreify();}}}
    return FD_VOID;}
}
static fdtype fretract(fdtype frames,fdtype slotids,fdtype values)
{
  if (FD_EMPTY_CHOICEP(values)) return FD_VOID;
  else {
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
	if (FD_VOIDP(values)) {
	  fdtype values=fd_frame_get(frame,slotid);
	  FD_DO_CHOICES(value,values) {
	    if (fd_frame_drop(frame,slotid,value)<0) {
	      fd_decref(values);
	      return fd_erreify();}}
	  fd_decref(values);}
	else {
	  FD_DO_CHOICES(value,values) {
	    if (fd_frame_drop(frame,slotid,value)<0)
	      return fd_erreify();}}}}
    return FD_VOID;}
}

static fdtype ftest(fdtype frames,fdtype slotids,fdtype values)
{
  FD_DO_CHOICES(frame,frames) {
    FD_DO_CHOICES(slotid,slotids) {
      FD_DO_CHOICES(value,values)
	if (FD_OIDP(frame)) {
	  int result=fd_frame_test(frame,slotid,value);
	  if (result<0) return fd_erreify();
	  else if (result) return FD_TRUE;}
	else {
	  int result=fd_test(frame,slotid,value);
	  if (result<0) return fd_erreify();
	  else if (result) return FD_TRUE;}}}
  return FD_FALSE;
}

static fdtype testp(int n,fdtype *args)
{
  fdtype frames=args[0], slotids=args[1], testfns=args[2];
  if ((FD_EMPTY_CHOICEP(frames)) || (FD_EMPTY_CHOICEP(slotids)))
    return FD_FALSE;
  else {
    FD_DO_CHOICES(frame,frames) {
      FD_DO_CHOICES(slotid,slotids) {
	fdtype values=fget(frame,slotid);
	FD_DO_CHOICES(testfn,testfns)
	  if (FD_APPLICABLEP(testfn)) {
	    fdtype test_result=FD_FALSE;
	    args[2]=values;
	    test_result=fd_apply((fd_function)testfn,n-2,args+2);
	    args[2]=testfns;
	    if (FD_ABORTP(test_result)) {
	      fd_decref(values); return test_result;}
	    else if (FD_TRUEP(test_result)) {
	      fd_decref(values); fd_decref(test_result);
	      return FD_TRUE;}
	    else {}}
	  else if ((FD_SYMBOLP(testfn)) || (FD_OIDP(testfn))) {
	    fdtype test_result;
	    args[1]=values; args[2]=testfn;
	    test_result=testp(n-1,args+1);
	    args[1]=slotids; args[2]=testfns;
	    if (FD_ABORTP(test_result)) {
	      fd_decref(values); return test_result;}
	    else if (FD_TRUEP(test_result)) {
	      fd_decref(values); return test_result;}
	    else fd_decref(test_result);}
	  else if (FD_TABLEP(testfn)) {
	    FD_DO_CHOICES(value,values) {
	      fdtype mapsto=fd_get(testfn,value,FD_VOID);
	      if (FD_ABORTP(mapsto)) {
		fd_decref(values);
		return mapsto;}
	      else if (FD_VOIDP(mapsto)) {}
	      else if (n>3)
		if (fd_overlapp(mapsto,args[3])) {
		  fd_decref(mapsto); fd_decref(values);
		  return FD_TRUE;}
		else {fd_decref(mapsto);}
	      else {
		fd_decref(mapsto); fd_decref(values);
		return FD_TRUE;}}}
	  else {
	    fd_decref(values);
	    return fd_type_error(_("value test"),"testp",testfn);}
	fd_decref(values);}}
    return FD_FALSE;}
}

/* Index operations */

static fdtype indexget(fdtype ixarg,fdtype key)
{
  fd_index ix=fd_lisp2index(ixarg);
  if (ix==NULL) return fd_erreify();
  else return fd_index_get(ix,key);
}

static fdtype indexadd(fdtype ixarg,fdtype key,fdtype values)
{
  fd_index ix=fd_lisp2index(ixarg);
  if (ix==NULL) return fd_erreify();
  fd_index_add(ix,key,values);
  return FD_VOID;
}

static fdtype indexset(fdtype ixarg,fdtype key,fdtype values)
{
  fd_index ix=fd_lisp2index(ixarg);
  if (ix==NULL) return fd_erreify();
  fd_index_store(ix,key,values);
  return FD_VOID;
}

static fdtype indexdecache(fdtype ixarg,fdtype key,fdtype value)
{
  fd_index ix=fd_lisp2index(ixarg);
  if (ix==NULL) return fd_erreify();
  if (FD_VOIDP(value))
    fd_hashtable_op(&(ix->cache),fd_table_replace,key,FD_VOID);
  else {
    fdtype keypair=fd_init_pair(NULL,fd_incref(key),fd_incref(value));
    fd_hashtable_op(&(ix->cache),fd_table_replace,keypair,FD_VOID);
    fd_decref(keypair);}
  return FD_VOID;
}

static fdtype bgdecache(fdtype key,fdtype value)
{
  fd_index ix=(fd_index)fd_background;
  if (ix==NULL) return fd_erreify();
  if (FD_VOIDP(value))
    fd_hashtable_op(&(ix->cache),fd_table_replace,key,FD_VOID);
  else {
    fdtype keypair=fd_init_pair(NULL,fd_incref(key),fd_incref(value));
    fd_hashtable_op(&(ix->cache),fd_table_replace,keypair,FD_VOID);
    fd_decref(keypair);}
  return FD_VOID;
}

static fdtype indexkeys(fdtype ixarg)
{
  fd_index ix=fd_lisp2index(ixarg);
  if (ix==NULL) return fd_erreify();
  return fd_index_keys(ix);
}

static fdtype indexsizes(fdtype ixarg)
{
  fd_index ix=fd_lisp2index(ixarg);
  if (ix==NULL) return fd_erreify();
  return fd_index_sizes(ix);
}

/* Other operations */

static int dotest(fdtype f,fdtype pred,fdtype val,int noinfer)
{
  if ((FD_OIDP(f)) && ((FD_SYMBOLP(pred)) || (FD_OIDP(pred))))
    if (noinfer)
      return fd_test(f,pred,val);
    else return fd_frame_test(f,pred,val);
  else if ((FD_TABLEP(f)) && ((FD_SYMBOLP(pred)) || (FD_OIDP(pred))))
    return fd_test(f,pred,val);
  else if (FD_TABLEP(pred))
    return fd_test(pred,f,val);
  else if (FD_APPLICABLEP(pred)) {
    fdtype rail[2], result;
    rail[0]=f; rail[1]=val; result=fd_apply((fd_function)pred,2,rail);
    if (FD_ABORTP(result))
      return fd_interr(result);
    else if ((FD_FALSEP(result)) || (FD_EMPTY_CHOICEP(result)))
      return 0;
    else {
      fd_decref(result);
      return 1;}}
  else return fd_reterr(fd_TypeError,"dotest",u8_strdup("test predicate"),
			pred);
}

static int binary_test(fdtype candidate,fdtype test,int noinfer)
{
  if ((FD_OIDP(candidate)) && ((FD_SYMBOLP(test)) || (FD_OIDP(test))))
    if (noinfer)
      return fd_test(candidate,test,FD_VOID);
    else return fd_frame_test(candidate,test,FD_VOID);
  else if (FD_PRIM_TYPEP(test,fd_hashset_type)) 
    if (fd_hashset_get((fd_hashset)test,candidate))
      return 1;
    else return 0;
  else if ((FD_OIDP(test)) || (FD_SYMBOLP(test)))
    return fd_test(candidate,test,FD_VOID);
  else if (FD_APPLICABLEP(test)) {
    fdtype v=fd_apply((fd_function)test,1,&candidate);
    if (FD_ABORTP(v)) return fd_interr(v);
    else if ((FD_FALSEP(v)) || (FD_EMPTY_CHOICEP(v)) || (FD_VOIDP(v))) return 0;
    else {fd_decref(v); return 1;}}
  else if (FD_POOLP(test))
    if (FD_OIDP(candidate)) {
      fd_pool p=fd_lisp2pool(test);
      if (fd_oid2pool(candidate)==p) return 1;
      else return 0;}
    else return 0;
  else if (FD_TABLEP(test))
    return fd_test(candidate,test,FD_VOID);
  else return fd_type_error(_("test object"),"binary_pick",test);
}

static fdtype binary_pick(fdtype candidates,fdtype test,int);

static fdtype pick_helper(int n,fdtype *args,int noinfer)
{
  fdtype start=args[0], candidates, next;
  int i=1;
  if (n==2) return binary_pick(args[0],args[1],noinfer);
  if ((n%2)==0)
    return fd_err(fd_SyntaxError,"wrong number of args to PICK",
		  NULL,FD_VOID);
  if ((fd_prefetch) && (fd_ipeval_status()>0)) prefetch_oids(start);
  candidates=fd_incref(start); next=FD_EMPTY_CHOICE;
  while (i<n) {
    FD_DO_CHOICES(candidate,candidates) {
      int testval=0;
      FD_DO_CHOICES(slotid,args[i]) {
	if (testval=dotest(candidate,slotid,args[i+1],noinfer)) {
	  FD_STOP_DO_CHOICES; break;}}
      if (testval<0) {
	fd_decref(candidates); fd_decref(next);
	return fd_erreify();}
      else if (testval) {
	FD_ADD_TO_CHOICE(next,fd_incref(candidate));}}
    fd_decref(candidates);
    candidates=next;
    next=FD_EMPTY_CHOICE;
    i=i+2;}
  return candidates;
}

static fdtype pick_lexpr(int n,fdtype *args)
{
  return pick_helper(n,args,0);
}

static fdtype prim_pick_lexpr(int n,fdtype *args)
{
  return pick_helper(n,args,1);
}

static fdtype binary_pick(fdtype candidates,fdtype test,int noinfer)
{
  fdtype results=FD_EMPTY_CHOICE;
  if (FD_EMPTY_CHOICEP(test)) return results;
  else {
    FD_DO_CHOICES(candidate,candidates)
      if (FD_CHOICEP(test)) {
	int hit=0;
	FD_DO_CHOICES(p,test)
	  if (binary_test(candidate,p,noinfer)) {
	    hit=1; FD_STOP_DO_CHOICES; break;}
	  else {}
	if (hit)
	  FD_ADD_TO_CHOICE(results,fd_incref(candidate));}
      else if (binary_test(candidate,test,noinfer)) {
	FD_ADD_TO_CHOICE(results,fd_incref(candidate));}
      else {}
    return results;}
}
  
static fdtype binary_reject(fdtype candidates,fdtype test,int);

static fdtype reject_helper(int n,fdtype *args,int noinfer)
{
  fdtype start=args[0], candidates;
  fdtype rejects=FD_EMPTY_CHOICE, next;
  int i=1;
  if (n==2) return binary_reject(args[0],args[1],noinfer);
  if ((n%2)==0)
    return fd_err(fd_SyntaxError,"wrong number of args to REJECT",
		  NULL,FD_VOID);
  if ((fd_prefetch) && (fd_ipeval_status()>0)) prefetch_oids(start);
  candidates=fd_incref(start);
  while (i<n) {
    FD_DO_CHOICES(candidate,candidates) {
      int testval=0;
      FD_DO_CHOICES(slotid,args[i]) {
	if ((testval=dotest(candidate,slotid,args[i+1],noinfer))) {
	  FD_STOP_DO_CHOICES; break;}}
      if (testval<0) {
	fd_decref(candidates); fd_decref(rejects);
	return fd_erreify();}
      else if (testval) {
	FD_ADD_TO_CHOICE(rejects,fd_incref(candidate));}}
    next=fd_difference(candidates,rejects);
    fd_decref(candidates); candidates=next;
    fd_decref(rejects); rejects=FD_EMPTY_CHOICE;
    next=FD_EMPTY_CHOICE; 
    i=i+2;}
  return candidates;
}

static fdtype reject_lexpr(int n,fdtype *args)
{
  return reject_helper(n,args,0);
}

static fdtype prim_reject_lexpr(int n,fdtype *args)
{
  return reject_helper(n,args,1);
}

static fdtype binary_reject(fdtype candidates,fdtype test,int noinfer)
{
  fdtype results=FD_EMPTY_CHOICE;
  if (FD_EMPTY_CHOICEP(test)) return fd_incref(candidates);
  else {
    FD_DO_CHOICES(candidate,candidates)
      if (FD_CHOICEP(test)) {
	int hit=0;
	FD_DO_CHOICES(p,test)
	  if (binary_test(candidate,p,noinfer)) {
	    hit=1; FD_STOP_DO_CHOICES; break;}
	  else {}
	if (hit) {}
	else {FD_ADD_TO_CHOICE(results,fd_incref(candidate));}}
      else if (binary_test(candidate,test,noinfer)) {}
      else {
	FD_ADD_TO_CHOICE(results,fd_incref(candidate));}
    return results;}
}

/* Kleene* operations */

static fdtype getstar(fdtype frames,fdtype slotids)
{
  fdtype results=fd_incref(frames);
  FD_DO_CHOICES(slotid,slotids) {
    fdtype all=fd_inherit_values(frames,slotid,slotid);
    if (FD_ABORTP(all)) {
      fd_decref(results);
      return all;}
    else {FD_ADD_TO_CHOICE(results,all);}}
  return fd_simplify_choice(results);
}

static fdtype inherit_prim(fdtype slotids,fdtype frames,fdtype through)
{
  fdtype results=fd_incref(frames);
  FD_DO_CHOICES(through_id,through) {
    fdtype all=fd_inherit_values(frames,slotids,through_id);
    FD_ADD_TO_CHOICE(results,all);}
  return fd_simplify_choice(results);
}

static fdtype pathp(fdtype frames,fdtype slotids,fdtype values)
{
  if (fd_pathp(frames,slotids,values)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype getbasis(fdtype frames,fdtype lattice)
{
  return fd_simplify_choice(fd_get_basis(frames,lattice));
}

/* Frame creation */

static fdtype allocate_oids(fdtype pool,fdtype howmany)
{
  fd_pool p=arg2pool(pool);
  if (p==NULL)
    return fd_type_error(_("pool spec"),"allocate_oids",pool);
  if (FD_VOIDP(howmany))
    return fd_pool_alloc(p,1);
  else if (FD_FIXNUMP(howmany))
    return fd_pool_alloc(p,FD_INT2DTYPE(howmany));
  else return fd_type_error(_("fixnum"),"allocate_oids",howmany);
}

static fdtype frame_create_lexpr(int n,fdtype *args)
{
  if (n%2==0)
    return fd_err(fd_SyntaxError,"FRAME-CREATE",NULL,FD_VOID);
  else if (FD_FALSEP(args[0])) {
    fdtype slotmap=fd_init_slotmap(NULL,0,NULL,NULL);
    int i=1; while (i<n) {
      fdtype values=args[i+1];
      FD_DO_CHOICES(slotid,args[i])
	fd_add(slotmap,slotid,args[i+1]);
      i=i+2;}
    return slotmap;}
  else {
    fd_pool p=arg2pool(args[0]); fdtype oid, slotmap; int i=1;
    if (p==NULL)
      return fd_type_error(_("pool spec"),"frame_create_lexpr",args[0]);
    oid=fd_pool_alloc(p,1);
    if (FD_EXCEPTIONP(oid)) return oid;
    slotmap=fd_init_slotmap(NULL,0,NULL,NULL);
    if (fd_set_oid_value(oid,slotmap)<0) {
      fd_decref(slotmap);
      return fd_erreify();}
    while (i<n) {
      fdtype values=args[i+1];
      FD_DO_CHOICES(slotid,args[i])
	fd_frame_add(oid,slotid,args[i+1]);
      i=i+2;}
    fd_decref(slotmap);
    return oid;}
}

static fdtype seq2frame_prim
  (fdtype poolspec,fdtype values,fdtype schema,fdtype dflt)
{
  if (!(FD_SEQUENCEP(schema)))
    return fd_type_error(_("sequence"),"seq2frame_prim",schema);
  else if (!(FD_SEQUENCEP(values)))
    return fd_type_error(_("sequence"),"seq2frame_prim",values);
  else {
    fdtype frame;
    int i=0, slen=fd_seq_length(schema), vlen=fd_seq_length(values);
    if (FD_FALSEP(poolspec))
      frame=fd_init_slotmap(NULL,0,NULL,NULL);
    else if (FD_OIDP(poolspec)) frame=poolspec;
    else {
      fdtype oid, slotmap;
      fd_pool p=arg2pool(poolspec);
      if (p==NULL)
	return fd_type_error(_("pool spec"),"seq2frame_prim",poolspec);
      oid=fd_pool_alloc(p,1);
      if (FD_EXCEPTIONP(oid)) return oid;
      slotmap=fd_init_slotmap(NULL,0,NULL,NULL);
      if (fd_set_oid_value(oid,slotmap)<0) {
	fd_decref(slotmap);
	return fd_erreify();}
      else {
	fd_decref(slotmap); frame=oid;}}
    while ((i<slen) && (i<vlen)) {
      fdtype slotid=fd_seq_elt(schema,i);
      fdtype value=fd_seq_elt(values,i);
      int retval;
      if (FD_OIDP(frame))
	retval=fd_frame_add(frame,slotid,value);
      else retval=fd_add(frame,slotid,value);
      if (retval<0) {
	fd_decref(slotid); fd_decref(value);
	if (FD_FALSEP(poolspec)) fd_decref(frame);
	return fd_erreify();}
      fd_decref(slotid); fd_decref(value);
      i++;}
    if (!(FD_VOIDP(dflt)))
      while (i<slen) {
	fdtype slotid=fd_seq_elt(schema,i);
	int retval;
	if (FD_OIDP(frame))
	  retval=fd_frame_add(frame,slotid,dflt);
	else retval=fd_add(frame,slotid,dflt);
	if (retval<0) {
	  fd_decref(slotid);
	  if (FD_FALSEP(poolspec)) fd_decref(frame);
	  return fd_erreify();}
	fd_decref(slotid); i++;}
    return frame;}
}

/* OID operations */

static fdtype oid_plus_prim(fdtype oid,fdtype increment)
{
  FD_OID base=FD_OID_ADDR(oid), next;
  int delta=fd_getint(increment);
  next=FD_OID_PLUS(base,delta);
  return fd_make_oid(next);
}

static fdtype oid_offset_prim(fdtype oidarg,fdtype against)
{
  FD_OID oid=FD_OID_ADDR(oidarg), base; unsigned int cap=-1;
  if (FD_OIDP(against)) {
    base=FD_OID_ADDR(against);}
  else if (FD_POOLP(against)) {
    fd_pool p=fd_lisp2pool(against);
    if (p) {base=p->base; cap=p->capacity;}
    else return fd_erreify();}
  else if ((FD_VOIDP(against)) || (FD_FALSEP(against))) {
    fd_pool p=fd_oid2pool(oidarg);
    if (p) {base=p->base; cap=p->capacity;}
    else return FD_INT2DTYPE((FD_OID_LO(oid))%0x100000);}
  else return fd_type_error(_("offset base"),"oid_offset_prim",against);
  if ((FD_OID_HI(oid))==(FD_OID_HI(base))) {
    int diff=(FD_OID_LO(oid))-(FD_OID_LO(base));
    if ((diff>=0) && ((cap<0) || (diff<cap)))
      return FD_INT2DTYPE(diff);
    else return FD_FALSE;}
  else return FD_FALSE;
}

#ifdef FD_OID_BASE_ID
static fdtype oid_ptrdata_prim(fdtype oid)
{
  return fd_init_pair(NULL,
		      FD_INT2DTYPE(FD_OID_BASE_ID(oid)),
		      FD_INT2DTYPE(FD_OID_BASE_OFFSET(oid)));
}
#endif

static fdtype make_oid_prim(fdtype high,fdtype low)
{
  unsigned int hi, lo; FD_OID addr;
  if (FD_FIXNUMP(high))
    if ((FD_FIX2INT(high))<0)
      return fd_type_error("uint32","make_oid_prim",high);
    else hi=FD_FIX2INT(high);
  else if (FD_BIGINTP(high)) {
    hi=fd_bigint2uint((struct FD_BIGINT *)high);
    if (hi==0) return fd_type_error("uint32","make_oid_prim",high);}
  else return fd_type_error("uint32","make_oid_prim",high);
  if (FD_FIXNUMP(low))
    if ((FD_FIX2INT(low))<0)
      return fd_type_error("uint32","make_oid_prim",low);
    else lo=FD_FIX2INT(low);
  else if (FD_BIGINTP(low)) {
    lo=fd_bigint2uint((struct FD_BIGINT *)low);
    if (lo==0) return fd_type_error("uint32","make_oid_prim",low);}
  else return fd_type_error("uint32","make_oid_prim",low);
  FD_SET_OID_HI(addr,hi);
  FD_SET_OID_LO(addr,lo);
  return fd_make_oid(addr);
}

static fdtype oid2string_prim(fdtype oid)
{
  FD_OID addr=FD_OID_ADDR(oid);
  u8_string s=u8_mkstring("@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));
  return fd_init_string(NULL,-1,s);
}

/* sumframe function */

static fdtype sumframe_prim(fdtype frames,fdtype slotids)
{
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(frame,frames) {
    fdtype slotmap=fd_init_slotmap(NULL,0,NULL,NULL);
    FD_DO_CHOICES(slotid,slotids) {
      fdtype value=fd_get(frame,slotid,FD_EMPTY_CHOICE);
      if (FD_ABORTP(value)) {
	fd_decref(results); return value;}
      else if (fd_add(slotmap,slotid,value)<0) {
	fd_decref(results); fd_decref(value);
	return fd_erreify();}
      fd_decref(value);}
    if (FD_OIDP(frame)) {
      FD_OID addr=FD_OID_ADDR(frame);
      u8_string s=u8_mkstring("@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));
      fdtype idstring=fd_init_string(NULL,-1,s);
      if (fd_add(slotmap,id_symbol,idstring)<0) {
	fd_decref(results); fd_decref(idstring);
	return fd_erreify();}
      else fd_decref(idstring);}
    FD_ADD_TO_CHOICE(results,slotmap);}
  return results;
}

/* Graph walking functions */

static fdtype applyfn(fdtype fn,fdtype node)
{
  if ((FD_OIDP(fn)) || (FD_SYMBOLP(fn)))
    return fd_frame_get(node,fn);
  else if (FD_CHOICEP(fn)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(f,fn) {
      fdtype v=applyfn(fn,node);
      if (FD_ABORTP(v)) {
	fd_decref(results);
	return v;}
      else {FD_ADD_TO_CHOICE(results,v);}}}
  else if (FD_APPLICABLEP(fn))
    return fd_apply((fd_function)fn,1,&node);
  else if (FD_TABLEP(fn))
    return fd_get(fn,node,FD_EMPTY_CHOICE);
  else return FD_EMPTY_CHOICE;
}

static int walkgraph(fdtype fn,fdtype state,fdtype arcs,
		      struct FD_HASHSET *seen,fdtype *results)
{
  fdtype next_state=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(node,state)
    if (!(fd_hashset_get(seen,node))) {
      fdtype output, expand;
      fd_hashset_add(seen,node);
      /* Apply the function */
      output=applyfn(fn,node);
      if (FD_ABORTP(output)) return fd_interr(output);
      else if (results) {FD_ADD_TO_CHOICE(*results,output);}
      else fd_decref(output);
      /* Do the expansion */
      expand=applyfn(arcs,node);
      if (FD_ABORTP(expand)) return fd_interr(expand);
      else {
	FD_DO_CHOICES(next,expand)
	  if (!(fd_hashset_get(seen,next))) {
	    FD_ADD_TO_CHOICE(next_state,next);}}
      fd_decref(expand);}
  fd_decref(state);
  if (!(FD_EMPTY_CHOICEP(next_state)))
    return walkgraph(fn,next_state,arcs,seen,results);
  else return 1;
}

static fdtype forgraph(fdtype fcn,fdtype roots,fdtype arcs)
{
  struct FD_HASHSET hashset; int retval;
  if ((FD_OIDP(arcs)) || (FD_SYMBOLP(arcs)) ||
      (FD_APPLICABLEP(arcs)) || (FD_TABLEP(arcs))) {}
  else if (FD_CHOICEP(arcs)) {
    FD_DO_CHOICES(each,arcs)
      if (!((FD_OIDP(each)) || (FD_SYMBOLP(each)) ||
	    (FD_APPLICABLEP(each)) || (FD_TABLEP(each))))
	return fd_type_error("mapfn","mapgraph",each);}
  else return fd_type_error("mapfn","mapgraph",arcs);
  fd_init_hashset(&hashset,1024);
  fd_incref(roots);
  retval=walkgraph(fcn,roots,arcs,&hashset,NULL);
  if (retval<0) {
    fd_decref((fdtype)&hashset);
    return fd_erreify();}
  else {
    fd_decref((fdtype)&hashset);
    return FD_VOID;}
}

static fdtype mapgraph(fdtype fcn,fdtype roots,fdtype arcs)
{
  fdtype results=FD_EMPTY_CHOICE; int retval; struct FD_HASHSET hashset; 
  if ((FD_OIDP(fcn)) || (FD_SYMBOLP(fcn)) ||
      (FD_APPLICABLEP(fcn)) || (FD_TABLEP(fcn))) {}
  else if (FD_CHOICEP(fcn)) {
    FD_DO_CHOICES(each,fcn)
      if (!((FD_OIDP(each)) || (FD_SYMBOLP(each)) ||
	    (FD_APPLICABLEP(each)) || (FD_TABLEP(each))))
	return fd_type_error("mapfn","mapgraph",each);}
  else return fd_type_error("mapfn","mapgraph",fcn);
  if ((FD_OIDP(arcs)) || (FD_SYMBOLP(arcs)) ||
      (FD_APPLICABLEP(arcs)) || (FD_TABLEP(arcs))) {}
  else if (FD_CHOICEP(arcs)) {
    FD_DO_CHOICES(each,arcs)
      if (!((FD_OIDP(each)) || (FD_SYMBOLP(each)) ||
	    (FD_APPLICABLEP(each)) || (FD_TABLEP(each))))
	return fd_type_error("mapfn","mapgraph",each);}
  else return fd_type_error("mapfn","mapgraph",arcs);
  fd_init_hashset(&hashset,1024); fd_incref(roots);
  retval=walkgraph(fcn,roots,arcs,&hashset,&results);
  if (retval<0) {
    fd_decref((fdtype)&hashset);
    fd_decref(results);
    return fd_erreify();}
  else {
    fd_decref((fdtype)&hashset);
    return results;}
}

/* Initializing */

FD_EXPORT void fd_init_dbfns_c()
{
  fd_register_source_file(versionid);

  fd_make_hashtable(&fcn_caches,128,NULL);

  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim2("GET",fget,2)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim3("TEST",ftest,2)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprimn("TESTP",testp,3)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("ASSERT!",fassert,3)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("RETRACT!",fretract,2)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("GETSLOTS",fd_getkeys,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("SUMFRAME",sumframe_prim,2)));

  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim2("GET*",getstar,2)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim3("PATH?",pathp,3)));
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprim3("INHERIT",inherit_prim,3)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("GET-BASIS",getbasis,2)));

  fd_idefn(fd_scheme_module,fd_make_cprimn("CACHECALL",cachecall,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1("CLEAR-CALLCACHE!",clear_callcache,0));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("OID-VALUE",oidvalue,1,fd_oid_type,FD_VOID));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("SET-OID-VALUE!",setoidvalue,2,
			   fd_oid_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("LOCK-OID!",lockoid,2,
			   fd_oid_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("LOCK-OIDS!",lockoids,1)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL?",poolp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("INDEX?",indexp,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("NAME->POOL",getpool,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("GETPOOL",getpool,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("POOL-LABEL",pool_label,1,
					    -1,FD_VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-BASE",pool_base,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-CAPACITY",pool_capacity,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-LOAD",pool_load,1));
  fd_idefn(fd_scheme_module,fd_make_cprim3("POOL-ELTS",pool_elts,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("OID-RANGE",oid_range,2,
			   fd_oid_type,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("OID-VECTOR",oid_vector,2,
			   fd_oid_type,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("RANDOM-OID",random_oid,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POOL-VECTOR",pool_vec,1));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("OID-HI",oidhi,1,fd_oid_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("OID-LO",oidlo,1,fd_oid_type,FD_VOID));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("OID?",oidp,1));
  fd_idefn(fd_xscheme_module,
	   fd_make_cprim1x("OID-POOL",oidpool,1,fd_oid_type,FD_VOID));
  fd_idefn(fd_xscheme_module,
	   fd_make_cprim2x("IN-POOL?",inpoolp,2,
			   fd_oid_type,FD_VOID,-1,FD_VOID));

  fd_idefn(fd_xscheme_module,fd_make_cprim2("USE-POOL",use_pool,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("USE-INDEX",use_index,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("OPEN-INDEX",open_index,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("CACHECOUNT",cachecount,0));

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim
	   (fd_make_cprimn("FRAME-CREATE",frame_create_lexpr,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim4("SEQ->FRAME",seq2frame_prim,3)));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2("ALLOCATE-OIDS",allocate_oids,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("OID-PLUS",oid_plus_prim,1,
			   fd_oid_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(1)));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("OID-OFFSET",oid_offset_prim,1,
			   fd_oid_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MAKE-OID",make_oid_prim,2));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("OID->STRING",oid2string_prim,1,
			   fd_oid_type,FD_VOID));
#ifdef FD_OID_BASE_ID
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("OID-PTRDATA",oid_ptrdata_prim,1,
			   fd_oid_type,FD_VOID));
#endif


  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("??",fd_bgfinder,2)));
  fd_idefn(fd_xscheme_module,
	   fd_make_ndprim(fd_make_cprimn("FIND-FRAMES",find_frames_lexpr,3)));

  fd_idefn(fd_xscheme_module,fd_make_cprimn("SWAPOUT",swapout_lexpr,0));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("COMMIT",commit_lexpr,0));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("POOL-CLOSE",pool_close_prim,1));
  fd_idefn(fd_xscheme_module,
	   fd_make_cprim1("CLEAR-SLOTCACHE!",clear_slotcache,0));
  fd_idefn(fd_xscheme_module,
	   fd_make_cprim0("CLEARCACHES",clearcaches,0));
  
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("PREFETCH-OIDS!",prefetch_oids,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("PREFETCH-KEYS!",prefetch_keys,1)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("CACHED-OIDS",cached_oids,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CACHED-KEYS",cached_keys,0));

  fd_idefn(fd_xscheme_module,
	   fd_make_ndprim
	   (fd_make_cprimn("MAKE-COMPOUND-INDEX",make_compound_index,1)));

  fd_idefn(fd_xscheme_module,
	   fd_make_cprim2("ADD-TO-COMPOUND-INDEX!",add_to_compound_index,2));

  fd_idefn(fd_xscheme_module,
	   fd_make_ndprim(fd_make_cprim4("INDEX-FRAME",index_frame_prim,3)));
  fd_idefn(fd_xscheme_module,fd_make_cprim3("INDEX-SET!",indexset,3));
  fd_idefn(fd_xscheme_module,fd_make_cprim3("INDEX-ADD!",indexadd,3));
  fd_idefn(fd_xscheme_module,fd_make_cprim2("INDEX-GET",indexget,2));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-KEYS",indexkeys,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("INDEX-SIZES",indexsizes,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim3("INDEX-DECACHE",indexdecache,2));
  fd_idefn(fd_xscheme_module,fd_make_cprim2("BGDECACHE",bgdecache,1));
  fd_idefn(fd_xscheme_module,
	   fd_make_ndprim(fd_make_cprimn("PICK",pick_lexpr,3)));
  fd_idefn(fd_xscheme_module,
	   fd_make_ndprim(fd_make_cprimn("REJECT",reject_lexpr,3)));

  fd_idefn(fd_xscheme_module,
	   fd_make_ndprim(fd_make_cprimn("%PICK",prim_pick_lexpr,3)));
  fd_idefn(fd_xscheme_module,
	   fd_make_ndprim(fd_make_cprimn("%REJECT",prim_reject_lexpr,3)));

  fd_idefn(fd_xscheme_module,
	   fd_make_ndprim(fd_make_cprim3("MAPGRAPH",mapgraph,3)));
  fd_idefn(fd_xscheme_module,
	   fd_make_ndprim(fd_make_cprim3("FORGRAPH",mapgraph,3)));

  fd_idefn(fd_xscheme_module,fd_make_cprim3("USE-ADJUNCT",use_adjunct,1));

  id_symbol=fd_intern("%ID");
  adjunct_symbol=fd_intern("%ADJUNCT");
  pools_symbol=fd_intern("POOLS");
  indices_symbol=fd_intern("INDICES");

}


/* The CVS log for this file
   $Log: dbfns.c,v $
   Revision 1.111  2006/01/27 21:31:29  haase
   Fixed leak in binary pick/reject

   Revision 1.110  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.109  2006/01/20 13:05:27  haase
   Fixed leak in setoidvalue

   Revision 1.108  2006/01/18 17:28:22  haase
   Added OID-PTRDATA primitive

   Revision 1.107  2006/01/18 14:37:11  haase
   Added OID-RANGE and OID-VECTOR primitives

   Revision 1.106  2006/01/16 22:17:08  haase
   Added SUMFRAME

   Revision 1.105  2006/01/16 22:02:50  haase
   Added SEQ->FRAME

   Revision 1.104  2006/01/16 17:58:07  haase
   Fixes to empty choice cases for indices and better error handling

   Revision 1.103  2006/01/07 23:12:46  haase
   Moved framerd object dtype handling into the main fd_read_dtype core, which led to substantial performanc improvements

   Revision 1.102  2006/01/07 19:08:00  haase
   Made MAKE-OID handle bigint args

   Revision 1.101  2006/01/05 18:20:33  haase
   Added missing return keyword

   Revision 1.100  2006/01/02 22:40:22  haase
   Various prefetching fixes

   Revision 1.99  2006/01/01 19:51:15  haase
   Added FORGRAPH and MAPGRAPH primitives which handle circularities

   Revision 1.98  2006/01/01 00:02:44  haase
   Added missing header file to dbfns.c

   Revision 1.97  2005/12/20 19:05:53  haase
   Switched to u8_random and added RANDOMSEED config variable

   Revision 1.96  2005/12/02 22:09:25  haase
   Fixed handling of binary pick and reject with slotids

   Revision 1.95  2005/11/29 17:53:25  haase
   Catch file index overflows and 0/1 cases of index getkeys and getsizes

   Revision 1.94  2005/11/16 23:19:39  haase
   Made hashtable indices work for unspecified value argument

   Revision 1.93  2005/11/16 18:34:37  haase
   Made hashtables work for index-frame and find-frames

   Revision 1.92  2005/11/08 15:22:29  haase
   Made (COMMIT) ignore (but warn) on individual errors for now

   Revision 1.91  2005/11/03 20:46:10  haase
   Made PICK and REJECT handle non-frame table candidates

   Revision 1.90  2005/10/25 16:07:11  haase
   Added background decache primitive

   Revision 1.89  2005/10/13 16:02:59  haase
   Added INDEX-DECACHE

   Revision 1.88  2005/08/19 21:51:51  haase
   Extended PICK and REJECT to take hashtables and functions as predicates

   Revision 1.87  2005/08/18 17:03:09  haase
   Fixed some incremental change bugs in file indices

   Revision 1.86  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.85  2005/08/08 00:47:50  haase
   Added return value to function cache clearing

   Revision 1.84  2005/08/02 22:35:14  haase
   Fixed GC error in recursive descent primitives

   Revision 1.83  2005/07/30 17:22:23  haase
   SET-OID-VALUE with schemaps and hashtables now copy and mark modified

   Revision 1.82  2005/07/30 16:15:36  haase
   Added INHERIT prim

   Revision 1.81  2005/07/25 19:38:18  haase
   Added LOCK-OID

   Revision 1.80  2005/07/23 21:13:03  haase
   Added OID->STRING to get an OID address directly

   Revision 1.79  2005/07/16 15:59:22  haase
   Added POOL? and INDEX? primitives

   Revision 1.78  2005/07/14 02:18:07  haase
   Added TESTP primitive

   Revision 1.77  2005/07/13 23:37:37  haase
   More extensions to adjunct handling

   Revision 1.76  2005/07/13 23:07:45  haase
   Added global adjuncts and LISP access to adjunct declaration

   Revision 1.75  2005/07/13 22:22:50  haase
   Added semantics for fd_test when the value argument is VOID, which just tests for any values

   Revision 1.74  2005/07/13 21:39:31  haase
   XSLOTMAP/XSCHEMAP renaming

   Revision 1.73  2005/07/12 21:38:47  haase
   Fixed bug which made the primitive OPEN-INDEX always call USE-INDEX which made for a busy background

   Revision 1.72  2005/07/08 20:18:04  haase
   Fixed TEST primitive to work on non-frames

   Revision 1.71  2005/07/01 00:25:17  haase
   Fixed prefetch-keys! case with a non-deterministic index arg

   Revision 1.70  2005/07/01 00:24:07  haase
   Fixed prefetch-keys! case with a non-deterministic index arg

   Revision 1.69  2005/07/01 00:21:00  haase
   Fixed prefetch-keys! case with a non-deterministic index arg

   Revision 1.68  2005/05/30 00:03:54  haase
   Fixes to pool declaration, allowing the USE-POOL primitive to return multiple pools correctly when given a ; spearated list or a pool server which provides multiple pools

   Revision 1.67  2005/05/29 22:38:47  haase
   Simplified db layer fd_use_pool and fd_use_index

   Revision 1.66  2005/05/29 18:26:35  haase
   Defined version of 'pick' and 'reject' with a single argument which can be a function, hashset, table, or slotid and treats non-empty values as true

   Revision 1.65  2005/05/27 20:39:53  haase
   Added OID swapout

   Revision 1.64  2005/05/26 20:48:42  haase
   Made USE-POOL return a pool, rather than void, again

   Revision 1.63  2005/05/26 11:03:00  haase
   Made set-oid-value! check for an error return value

   Revision 1.62  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.61  2005/05/17 18:40:04  haase
   Added OID? primitive

   Revision 1.60  2005/05/12 22:41:32  haase
   Added in-pool?

   Revision 1.59  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.58  2005/05/09 20:04:19  haase
   Move dtype hash functions into dbfile and made libfdscheme independent of libfddbfile

   Revision 1.57  2005/05/06 16:13:08  haase
   Made USE-POOL/USE-INDEX return void on string arguments and accept separated pool lists

   Revision 1.56  2005/04/26 00:06:22  haase
   Made name->pool return the empty choice when it fails to identify a pool

   Revision 1.55  2005/04/25 13:11:34  haase
   Added cache size counting

   Revision 1.54  2005/04/24 02:00:53  haase
   Don't bother prefetching under ipeval

   Revision 1.53  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.52  2005/04/14 01:32:41  haase
   Fix bug in open_index/use_index changes

   Revision 1.51  2005/04/14 01:25:22  haase
   Made use-pool/index functions take opened pools and indices as arguments

   Revision 1.50  2005/04/13 15:00:09  haase
   Added OID utility functions

   Revision 1.49  2005/04/12 20:41:15  haase
   Fixed type definition for set-oid-value

   Revision 1.48  2005/04/10 17:24:03  haase
   Fixed error in hashtable copier

   Revision 1.47  2005/04/09 22:44:15  haase
   Added REJECT primitive

   Revision 1.46  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.45  2005/03/28 19:18:45  haase
   Added prefetching configuration variable PREFETCH

   Revision 1.44  2005/03/26 18:31:41  haase
   Various configuration fixes

   Revision 1.43  2005/03/26 04:47:47  haase
   Added index-set and index-sizes primitives

   Revision 1.42  2005/03/06 00:07:45  haase
   Dropped exclamation from index-frame

   Revision 1.41  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.40  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.39  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.38  2005/03/02 15:52:24  haase
   Remove cache level setting primitive and simplified pool ops to use fd_lisp2pool

   Revision 1.37  2005/03/01 19:38:35  haase
   Define GETSLOTS alias for GETKEYS

   Revision 1.36  2005/03/01 02:12:03  haase
   Defined set-oid-value primitive

   Revision 1.35  2005/02/27 03:03:45  haase
   Added index-frame primitive and simplified other index primitives to use expanded fd_lisp2index

   Revision 1.34  2005/02/25 20:13:03  haase
   Add special cases and comprehensive retract

   Revision 1.33  2005/02/24 19:12:46  haase
   Fixes to handling index arguments which are strings specifiying index sources

   Revision 1.32  2005/02/19 21:23:10  haase
   Added FIND-FRAMES back

   Revision 1.31  2005/02/19 16:23:16  haase
   Added ALLOCATE-OIDS, FRAME-CREATE, MAKE-OID, and OID-PLUS

   Revision 1.30  2005/02/11 03:50:12  haase
   Added some eval branch predictions

   Revision 1.29  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
