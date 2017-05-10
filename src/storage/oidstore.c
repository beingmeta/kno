/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_TABLES 1
#define FD_INLINE_POOLS 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/apply.h"

#include <stdarg.h>

fd_exception fd_BadAdjunct=_("Bad adjunct table"),
  fd_AdjunctError=_("Pool adjunct error");

static int anonymous_oiderr(u8_string op,fdtype f)
{
  fd_seterr(fd_AnonymousOID,op,NULL,f);
  return -1;
}

/* Adjunct functions */

fdtype fd_adjunct_slotids;

static fdtype padjslotid;

static int n_global_adjuncts = 0, max_global_adjuncts = 0;
static struct FD_ADJUNCT *global_adjuncts = NULL;

FD_FASTOP fd_adjunct get_adjunct(fd_pool p,fdtype slotid)
{
  if (p) {
    struct FD_ADJUNCT *scan = p->pool_adjuncts;
    struct FD_ADJUNCT *limit = scan+p->pool_n_adjuncts;
    while (scan<limit)
      if (scan->slotid == slotid) {
        if (FD_FALSEP(scan->table))
          return NULL;
        else return scan;}
      else scan++;
    return NULL;}
  if (global_adjuncts) {
    struct FD_ADJUNCT *scan = global_adjuncts;
    struct FD_ADJUNCT *limit = scan+n_global_adjuncts;
    while (scan<limit)
      if (scan->slotid == slotid)
        return scan;
      else scan++;
    return NULL;}
  else return NULL;
}

FD_EXPORT fd_adjunct fd_get_adjunct(fd_pool p,fdtype slotid)
{
  return get_adjunct(p,slotid);
}

FD_EXPORT int fd_adjunctp(fd_pool p,fdtype slotid)
{
  return (get_adjunct(p,slotid)!=NULL);
}

FD_EXPORT int fd_set_adjunct(fd_pool p,fdtype slotid,fdtype adjtable)
{
  fd_adjunct adj = get_adjunct(p,slotid);
  if (!(adj)) {
    FD_ADD_TO_CHOICE(fd_adjunct_slotids,slotid);
    fd_adjunct_slotids = fd_simplify_choice(fd_adjunct_slotids);}
  if (adj) {
    fd_incref(adjtable); fd_decref(adj->table);
    adj->table = adjtable;
    return 0;}
  else {
    struct FD_ADJUNCT *adjuncts; int n, max;
    if (p) {
      adjuncts = p->pool_adjuncts;
      n = p->pool_n_adjuncts;
      max = p->pool_adjuncts_len;}
    else {
      adjuncts = global_adjuncts;
      n = n_global_adjuncts;
      max = max_global_adjuncts;}
    if (n>=max) {
      int new_max = ((max) ? (max*2) : (8));
      struct FD_ADJUNCT *newadj=
        u8_realloc(adjuncts,sizeof(struct FD_ADJUNCT)*new_max);
      if (p) {
        adjuncts = p->pool_adjuncts = newadj; p->pool_adjuncts_len = new_max;}
      else {
        adjuncts = global_adjuncts = newadj; max_global_adjuncts = new_max;}}
    adjuncts[n].pool = p; adjuncts[n].slotid = slotid;
    adjuncts[n].table = adjtable; fd_incref(adjtable);
    adj = &(adjuncts[n]);
    if (p) p->pool_n_adjuncts++; else n_global_adjuncts++;
    return 1;}
}

FD_EXPORT int fd_set_adjuncts(fd_pool p,fdtype adjuncts)
{
  int n_adjuncts=0;
  FD_DO_CHOICES(spec,adjuncts) {
    if (FD_INDEXP(spec)) {
      fdtype slotid=fd_get(spec,padjslotid,FD_VOID);
      if ((FD_SYMBOLP(slotid))||(FD_OIDP(slotid))) {
        fd_set_adjunct(p,slotid,spec);
        n_adjuncts++;}
      else {
        fd_seterr(fd_BadAdjunct,"fd_set_adjunct/noslotid",
                  u8_strdup(p->poolid),
                  fd_incref(spec));
        FD_STOP_DO_CHOICES;
        return -1;}}
    else if (FD_POOLP(spec)) {
      if (p->pool_label) {
        fd_set_adjunct(p,fd_intern(p->pool_label),spec);
        n_adjuncts++;}
      else {
        fd_seterr(fd_BadAdjunct,"fd_set_adjunct/noslotid",
                  u8_strdup(p->poolid),
                  fd_incref(spec));
        FD_STOP_DO_CHOICES;
        return -1;}}
    else {
      fdtype slotids=fd_getkeys(spec);
      FD_DO_CHOICES(slotid,slotids) {
        fdtype adjunct=fd_get(spec,slotid,FD_VOID);
        if (n_adjuncts<0) {
          fd_decref(adjunct);
          continue;}
        else if (FD_VOIDP(adjunct)) {}
        else if (FD_STRINGP(adjunct)) {
          fd_index ix=fd_get_index(FD_STRDATA(adjunct),-1,FD_FALSE);
          if (ix) {
            fd_decref(adjunct);
            adjunct=fd_index2lisp(ix);}
          else {
            fd_pool p=fd_get_pool(FD_STRDATA(adjunct),
                                  FD_STORAGE_ISPOOL|
                                  FD_POOL_ADJUNCT|
                                  FD_POOL_SPARSE,
                                  FD_FALSE);
            if (p) {
              fd_decref(adjunct);
              adjunct=fd_pool2lisp(p);}
            else {
              fd_seterr(fd_BadAdjunct,"fd_set_adjunct",
                        u8_strdup(p->poolid),
                        fd_make_pair(slotid,adjunct));
              fd_decref(adjunct);
              FD_STOP_DO_CHOICES;
              n_adjuncts=-1;}}}
        else if (FD_POOLP(adjunct)) {}
        else if (FD_INDEXP(adjunct)) {}
        else if (FD_TABLEP(adjunct)) {}
        else {
          fd_seterr(fd_BadAdjunct,"fd_set_adjunct",
                    u8_strdup(p->poolid),
                    fd_make_pair(slotid,adjunct));
          fd_decref(adjunct);
          n_adjuncts=-1;}
        fd_set_adjunct(p,slotid,adjunct);
        n_adjuncts++;}
      fd_decref(slotids);
      if (n_adjuncts<0) {
        FD_STOP_DO_CHOICES;
        return -1;}}}
  return n_adjuncts;
}

FD_EXPORT fdtype fd_get_adjuncts(fd_pool p)
{
  int i=0, n_adjuncts=p->pool_n_adjuncts;
  struct FD_ADJUNCT *adjuncts=p->pool_adjuncts;
  fdtype result=fd_make_slotmap(n_adjuncts,0,NULL);
  while (i < n_adjuncts) {
    struct FD_ADJUNCT *adj=&adjuncts[i++];
    fd_store(result,adj->slotid,adj->table);}
  return result;
}

/* Fetching from adjuncts */

static fd_index l2x(fdtype lix)
{
  int serial = FD_GET_IMMEDIATE(lix,fd_index_type);
  if (serial<FD_N_PRIMARY_INDEXES) return fd_primary_indexes[serial];
  else return fd_secondary_indexes[serial-FD_N_PRIMARY_INDEXES];
}

static fd_pool l2p(fdtype lp)
{
  int serial = FD_GET_IMMEDIATE(lp,fd_pool_type);
  if (serial<fd_n_pools)
    return fd_pools_by_serialno[serial];
  else {
    char buf[32];
    sprintf(buf,"serial = 0x%x",serial);
    fd_seterr3(fd_InvalidPoolPtr,"fd_lisp2pool",buf);
    return NULL;}
}

static fdtype adjunct_fetch(fd_adjunct adj,fdtype frame,fdtype dflt)
{
  fdtype store = adj->table;
  return
    ((FD_HASHTABLEP(store)) ?
     (fd_hashtable_get((fd_hashtable)store,frame,FD_VOID)) :
     (FD_INDEXP(store)) ? (fd_index_get(l2x(store),frame)) :
     (FD_POOLP(store)) ? (fd_pool_get(l2p(store),frame)) :
     (FD_TYPEP(store,fd_consed_index_type)) ?
     (fd_index_get(((fd_index)store),frame)) :
     (fd_get(store,frame,FD_VOID)));
}

static int adjunct_add(fd_adjunct adj,fdtype frame,fdtype value)
{
  return fd_add(adj->table,frame,value);
}

static int adjunct_drop(fd_adjunct adj,fdtype frame,fdtype value)
{
  return fd_drop(adj->table,frame,value);
}

static int adjunct_store(fd_adjunct adj,fdtype frame,fdtype value)
{
  return fd_store(adj->table,frame,value);
}

static int adjunct_test(fd_adjunct adj,fdtype frame,fdtype value)
{
  return fd_test(adj->table,frame,value);
}

/* Table operations on OIDs */

/* OIDs provide an intermediate data layer.  In general, getting a
   slotid from an OID gets the OID's value and then gets the slotid
   from that value (whatever type of object it is).  The exception to
   this case is that certain pools can declare that certain keys
   (slotids) are stored in external index.  These indexes, called
   adjunct indexes, allow another layer of description. */
FD_EXPORT fdtype fd_oid_get(fdtype f,fdtype slotid,fdtype dflt)
{
  fd_pool p = fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))
    if (fd_ignore_anonymous_oids) {
      fd_adjunct adj = get_adjunct(p,slotid);
      if (adj)
        return adjunct_fetch(adj,f,dflt);
      else return fd_incref(dflt);}
    else return fd_err(fd_AnonymousOID,NULL,NULL,f);
  else {
    fd_adjunct adj = get_adjunct(p,slotid); fdtype smap; int free_smap = 0;
    if (adj)
      return adjunct_fetch(adj,f,dflt);
    else {smap = fd_fetch_oid(p,f); free_smap = 1;}
    if (FD_ABORTP(smap))
      return smap;
    else if (FD_SLOTMAPP(smap)) {
      fdtype value = fd_slotmap_get((fd_slotmap)smap,slotid,dflt);
      if (free_smap) fd_decref(smap);
      return value;}
    else if (FD_SCHEMAPP(smap)) {
      fdtype value = fd_schemap_get((fd_schemap)smap,slotid,dflt);
      if (free_smap) fd_decref(smap);
      return value;}
    else if (FD_TABLEP(smap)) {
      fdtype value = fd_get(smap,slotid,dflt);
      if (free_smap) fd_decref(smap);
      return value;}
    else {
      if (free_smap) fd_decref(smap);
      return fd_incref(dflt);}}
}

FD_EXPORT int fd_oid_add(fdtype f,fdtype slotid,fdtype value)
{
  fd_pool p = fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL)) {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_add(adj,f,value);
    else return anonymous_oiderr("fd_oid_add",f);}
  else {
    fdtype smap; int retval;
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_add(adj,f,value);
    else smap = fd_locked_oid_value(p,f);
    if (FD_ABORTP(smap))
      return fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      retval = fd_slotmap_add(FD_XSLOTMAP(smap),slotid,value);
    else if (FD_SCHEMAPP(smap))
      retval = fd_schemap_add(FD_XSCHEMAP(smap),slotid,value);
    else retval = fd_add(smap,slotid,value);
    fd_decref(smap);
    return retval;}
}

FD_EXPORT int fd_oid_store(fdtype f,fdtype slotid,fdtype value)
{
  fd_pool p = fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL)) {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_store(adj,f,value);
    else return anonymous_oiderr("fd_oid_store",f);}
  else {
    fdtype smap; int retval;
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_store(adj,f,value);
    else smap = fd_locked_oid_value(p,f);
    if (FD_ABORTP(smap))
      return fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      if (FD_EMPTY_CHOICEP(value))
        retval = fd_slotmap_delete(FD_XSLOTMAP(smap),slotid);
      else retval = fd_slotmap_store(FD_XSLOTMAP(smap),slotid,value);
    else if (FD_SCHEMAPP(smap))
      retval = fd_schemap_store(FD_XSCHEMAP(smap),slotid,value);
    else retval = fd_store(smap,slotid,value);
    fd_decref(smap);
    return retval;}
}

FD_EXPORT int fd_oid_delete(fdtype f,fdtype slotid)
{
  fd_pool p = fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL)) {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_store(adj,f,FD_EMPTY_CHOICE);
    else return anonymous_oiderr("fd_oid_delete",f);}
  else {
    fdtype smap; int retval;
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_store(adj,f,FD_EMPTY_CHOICE);
    else smap = fd_locked_oid_value(p,f);
    if (FD_ABORTP(smap))
      return fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      retval = fd_slotmap_delete(FD_XSLOTMAP(smap),slotid);
    else if (FD_SCHEMAPP(smap))
      retval = fd_schemap_store(FD_XSCHEMAP(smap),slotid,FD_EMPTY_CHOICE);
    else retval = fd_store(smap,slotid,FD_EMPTY_CHOICE);
    fd_decref(smap);
    return retval;}
}

FD_EXPORT int fd_oid_drop(fdtype f,fdtype slotid,fdtype value)
{
  fd_pool p = fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))  {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_drop(adj,f,FD_EMPTY_CHOICE);
    else return anonymous_oiderr("fd_oid_drop",f);}
  else {
    fdtype smap; int retval;
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_drop(adj,f,value);
    else smap = fd_locked_oid_value(p,f);
    if (FD_ABORTP(smap))
      return fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      retval = fd_slotmap_drop(FD_XSLOTMAP(smap),slotid,value);
    else if (FD_SCHEMAPP(smap))
      retval = fd_schemap_drop(FD_XSCHEMAP(smap),slotid,value);
    else retval = fd_drop(smap,slotid,value);
    fd_decref(smap);
    return retval;}
}

FD_EXPORT int fd_oid_test(fdtype f,fdtype slotid,fdtype value)
{
  fd_pool p = fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))  {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_test(adj,f,value);
    else return anonymous_oiderr("fd_oid_test",f);}
  else if (FD_VOIDP(value)) {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_test(adj,f,value);
    else {
      fdtype v = fd_oid_get(f,slotid,FD_VOID);
      if (FD_VOIDP(v)) return 0;
      else {fd_decref(v); return 1;}}}
  else {
    fdtype smap; int retval;
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_test(adj,f,value);
    else smap = fd_fetch_oid(p,f);
    if (FD_ABORTP(smap))
      retval = fd_interr(smap);
    else if (FD_SLOTMAPP(smap))
      retval = fd_slotmap_test((fd_slotmap)smap,slotid,value);
    else if (FD_SCHEMAPP(smap))
      retval = fd_schemap_test((fd_schemap)smap,slotid,value);
    else if (FD_TABLEP(smap))
      retval = fd_test(smap,slotid,value);
    else {
      fd_decref(smap);
      return 0;}
    fd_decref(smap);
    return retval;}
}

FD_EXPORT fdtype fd_oid_keys(fdtype f)
{
  fd_pool p = fd_oid2pool(f);
  if (FD_EXPECT_FALSE(p == NULL))
    return fd_anonymous_oid("fd_oid_keys",f);
  else {
    fdtype smap = fd_fetch_oid(p,f);
    if (FD_ABORTP(smap)) return smap;
    else if (FD_TABLEP(smap)) {
      fdtype result = fd_getkeys(smap);
      fd_decref(smap);
      return result;}
    else return FD_EMPTY_CHOICE;}
}

/* Table operations on choices */

static fdtype choice_get(fdtype arg,fdtype slotid,fdtype dflt)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(each,arg) {
    fdtype v;
    if (FD_OIDP(each))
      v = fd_oid_get(each,slotid,FD_EMPTY_CHOICE);
    else v = fd_get(each,slotid,FD_EMPTY_CHOICE);
    FD_ADD_TO_CHOICE(results,v);}
  if (FD_EMPTY_CHOICEP(results))
    return fd_incref(dflt);
  else return fd_simplify_choice(results);
}

static int choice_add(fdtype arg,fdtype slotid,fdtype value)
{
  if (FD_EMPTY_CHOICEP(value)) return 0;
  else {
    FD_DO_CHOICES(each,arg)
      if (fd_add(each,slotid,value)<0) return -1;
    return 1;}
}

static int choice_store(fdtype arg,fdtype slotid,fdtype value)
{
  FD_DO_CHOICES(each,arg)
    if (fd_store(each,slotid,value)<0) return -1;
  return 1;
}

static int choice_drop(fdtype arg,fdtype slotid,fdtype value)
{
  FD_DO_CHOICES(each,arg)
    if (fd_drop(each,slotid,value)<0) return -1;
  return 1;
}

static int choice_test(fdtype arg,fdtype slotid,fdtype value)
{
  int result = 0;
  FD_DO_CHOICES(each,arg) {
    if ((result = (fd_test(each,slotid,value))))
      return result;}
  return 0;
}

static fdtype choice_keys(fdtype arg)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(each,arg) {
    fdtype keys = fd_getkeys(each);
    if (FD_ABORTP(keys)) {
      fd_decref(results); return keys;}
    else {FD_ADD_TO_CHOICE(results,keys);}}
  return results;
}


/* Getting paths */

/* Get Path functions */

FD_EXPORT fdtype fd_getpath(fdtype start,int n,fdtype *path,int infer,int accumulate)
{
  fdtype results = FD_EMPTY_CHOICE;
  fdtype scan = start; int i = 0;
  if (n==0) return fd_incref(start);
  while (i<n) {
    fdtype pred = path[i], newscan = FD_EMPTY_CHOICE;
    FD_DO_CHOICES(s,scan) {
      fdtype newval;
      if ((FD_OIDP(pred))||(FD_SYMBOLP(pred)))
        if (infer) newval = fd_frame_get(s,pred);
        else newval = fd_get(scan,pred,FD_EMPTY_CHOICE);
      else if (FD_TABLEP(pred))
        newval = fd_get(pred,s,FD_EMPTY_CHOICE);
      else if (FD_HASHSETP(pred))
        if (fd_hashset_get((fd_hashset)pred,s)) newval = s;
        else newval = FD_EMPTY_CHOICE;
      else if (FD_APPLICABLEP(pred))
        newval = fd_apply(pred,1,&s);
      else if ((FD_PAIRP(pred))&&(FD_APPLICABLEP(FD_CAR(pred)))) {
        fdtype fcn = FD_CAR(pred);
        fdtype args = FD_CDR(pred), argv[7]; int j = 1;
        argv[0]=s;
        if (FD_PAIRP(args))
          while (FD_PAIRP(args)) {
            argv[j++]=FD_CAR(args); args = FD_CDR(args);}
        else argv[j++]=args;
        if (j>7)
          newval = fd_err(fd_RangeError,"fd_getpath",
                        "too many elements in compound path",FD_VOID);
        else newval = fd_apply(j,fcn,argv);}
      else newval = fd_err(fd_TypeError,"fd_getpath",
                         "invalid path element",FD_VOID);
      if (FD_ABORTP(newval)) {
        fd_decref(scan); fd_decref(newscan);
        return newval;}
      else {FD_ADD_TO_CHOICE(newscan,newval);}}
    if (i>0) {
      if (accumulate) {FD_ADD_TO_CHOICE(results,scan);}
      else {fd_decref(scan);}}
    scan = newscan; i++;}
  if (accumulate) {FD_ADD_TO_CHOICE(results,scan);}
  if (accumulate) return results;
  else return scan;
}

/* Initialization */

FD_EXPORT void fd_init_oidobj_c()
{
  u8_register_source_file(_FILEINFO);

  fd_adjunct_slotids = FD_EMPTY_CHOICE;

  padjslotid=fd_intern("%ADJUNCT_SLOTID");

  /* Table functions for OIDs */
  fd_tablefns[fd_oid_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_oid_type]->get = fd_oid_get;
  fd_tablefns[fd_oid_type]->add = fd_oid_add;
  fd_tablefns[fd_oid_type]->drop = fd_oid_drop;
  fd_tablefns[fd_oid_type]->store = fd_oid_store;
  fd_tablefns[fd_oid_type]->test = fd_oid_test;
  fd_tablefns[fd_oid_type]->keys = fd_oid_keys;
  fd_tablefns[fd_oid_type]->getsize = NULL;

  /* Table functions for CHOICEs */
  fd_tablefns[fd_choice_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_choice_type]->get = choice_get;
  fd_tablefns[fd_choice_type]->add = choice_add;
  fd_tablefns[fd_choice_type]->drop = choice_drop;
  fd_tablefns[fd_choice_type]->store = choice_store;
  fd_tablefns[fd_choice_type]->test = choice_test;
  fd_tablefns[fd_choice_type]->keys = choice_keys;
  fd_tablefns[fd_choice_type]->getsize = NULL;

  /* Table functions for CHOICEs */
  fd_tablefns[fd_prechoice_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_prechoice_type]->get = choice_get;
  fd_tablefns[fd_prechoice_type]->add = choice_add;
  fd_tablefns[fd_prechoice_type]->drop = choice_drop;
  fd_tablefns[fd_prechoice_type]->store = choice_store;
  fd_tablefns[fd_prechoice_type]->test = choice_test;
  fd_tablefns[fd_prechoice_type]->keys = choice_keys;
  fd_tablefns[fd_prechoice_type]->getsize = NULL;

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/