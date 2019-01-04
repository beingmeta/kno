/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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
#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>

#include <stdarg.h>

u8_condition fd_BadAdjunct=_("Bad adjunct table"),
  fd_AdjunctError=_("Pool adjunct error"),
  fd_BadOIDValue=_("Bad OID store value");

/* TODO: Fix finish/modify semantics

   There's a problem with using OIDs after they've been finished.

   Suppose thread A gets a locked OID, and (eventually) does something
   to it's value (e.g. storing a value on its table). In between when
   it gets the locked OID and operates on it, a thread B decides that
   it's finished (though it's obviously not). It finishes it, setting
   it to readonly, so that when thread A gets around to doing its
   modification, the value (table) is now readonly. Which is an error.

   Possible fixes:
   1. Put locks around setting the readonly flag and have thread A lock
   the OID while it's working its way to modifying it. This requires
   using something other than fd_store or fd_slotmap_store to do the
   modification since they lock the OIDs themselves.
   2. Get rid of OID finishing and come up with another way to mark OIDs
   that are (probably) ready to be committed.
   3. Something I haven't thought of yet.

   (2) is probably the best option because it was something of a
   kludge in the first place, since it's using a flag designed for one
   purpose (being readonly) for another purpose (being (probably)
   ready to commit). A milder version of (2) would be to have a
   separate flag (table_finished) for when the value is ready to go
   committed. That might be the best course.

   The one thing going for (1) is if there are other contexts where
   the window between getting the locked OID and working on it's value
   was problematic. It wouldn't be a readonly problem if we separate
   table_finished and table_readonly. What are some other contexts?

   What happens if we have the finished_bit set and the OID is
   committed in the window between getting the locked OID and
   operating on it? It gets unlocked and the actual value will be
   freed after the change is made, so we lose whatever modification
   was done. That's not right.

   Suppose we don't swap out OIDs which were modified after they were
   finished. That would fix it. How do we do that? More flags? We
   don't want to clear the modified flag automatically after a
   save. Suppose we clear the modified flag after we start saving (and
   set it back if the save fails). Then, if anyone modifies it after that,
   we won't swap it out.


*/

/* Adjunct functions */

lispval fd_adjunct_slotids;

static lispval padjslotid;

static int n_global_adjuncts = 0, max_global_adjuncts = 0;
static struct FD_ADJUNCT *global_adjuncts = NULL;

FD_FASTOP fd_adjunct get_adjunct(fd_pool p,lispval slotid)
{
  if (p) {
    struct FD_ADJUNCT *scan = p->pool_adjuncts;
    struct FD_ADJUNCT *limit = scan+p->pool_n_adjuncts;
    while (scan<limit)
      if (scan->slotid == slotid) {
        if (FALSEP(scan->table))
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

FD_EXPORT fd_adjunct fd_get_adjunct(fd_pool p,lispval slotid)
{
  return get_adjunct(p,slotid);
}

FD_EXPORT int fd_adjunctp(fd_pool p,lispval slotid)
{
  return (get_adjunct(p,slotid)!=NULL);
}

FD_EXPORT int fd_set_adjunct(fd_pool p,lispval slotid,lispval adjtable)
{
  fd_adjunct adj = get_adjunct(p,slotid);
  if (!(adj)) {
    CHOICE_ADD(fd_adjunct_slotids,slotid);
    fd_adjunct_slotids = fd_simplify_choice(fd_adjunct_slotids);}
  if (adj) {
    lispval old = adj->table;
    fd_incref(adjtable);
    adj->table = adjtable;
    fd_decref(old);
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
    adjuncts[n].table = adjtable;
    fd_incref(adjtable);
    adj = &(adjuncts[n]);
    if (p)
      p->pool_n_adjuncts++;
    else n_global_adjuncts++;
    return 1;}
}

FD_EXPORT int fd_set_adjuncts(fd_pool p,lispval adjuncts)
{
  int n_adjuncts=0;
  DO_CHOICES(spec,adjuncts) {
    if (FD_INDEXP(spec)) {
      lispval slotid=fd_get(spec,padjslotid,VOID);
      if ((SYMBOLP(slotid))||(OIDP(slotid))) {
        fd_set_adjunct(p,slotid,spec);
        n_adjuncts++;}
      else {
        fd_seterr(fd_BadAdjunct,"fd_set_adjunct/noslotid",
                  p->poolid,fd_incref(spec));
        FD_STOP_DO_CHOICES;
        return -1;}}
    else if (FD_POOLP(spec)) {
      if (p->pool_label) {
        fd_set_adjunct(p,fd_intern(p->pool_label),spec);
        n_adjuncts++;}
      else {
        fd_seterr(fd_BadAdjunct,"fd_set_adjunct/noslotid",
                  p->poolid,fd_incref(spec));
        FD_STOP_DO_CHOICES;
        return -1;}}
    else {
      lispval slotids=fd_getkeys(spec);
      DO_CHOICES(slotid,slotids) {
        lispval adjunct=fd_get(spec,slotid,VOID);
        if (n_adjuncts<0) {
          fd_decref(adjunct);
          continue;}
        else if (VOIDP(adjunct)) {}
        else if (STRINGP(adjunct)) {
          fd_index ix=fd_get_index(CSTRING(adjunct),-1,FD_FALSE);
          if (ix) {
            fd_decref(adjunct);
            adjunct=fd_index_ref(ix);}
          else {
            fd_pool adjpool=
              fd_get_pool(CSTRING(adjunct),
                          FD_STORAGE_ISPOOL|FD_POOL_ADJUNCT,
                          FD_FALSE);
            if (adjpool) {
              fd_decref(adjunct);
              adjunct=fd_pool2lisp(adjpool);}
            else {
              fd_seterr(fd_BadAdjunct,"fd_set_adjunct",
                        p->poolid,fd_make_pair(slotid,adjunct));
              fd_decref(adjunct);
              FD_STOP_DO_CHOICES;
              n_adjuncts=-1;
              break;}}}
        else if (FD_POOLP(adjunct)) {}
        else if (FD_INDEXP(adjunct)) {}
        else {
          fd_seterr(fd_BadAdjunct,"fd_set_adjunct",
                    p->poolid,fd_make_pair(slotid,adjunct));
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

FD_EXPORT lispval fd_get_adjuncts(fd_pool p)
{
  int i=0, n_adjuncts=p->pool_n_adjuncts;
  struct FD_ADJUNCT *adjuncts=p->pool_adjuncts;
  lispval result=fd_make_slotmap(n_adjuncts,0,NULL);
  while (i < n_adjuncts) {
    struct FD_ADJUNCT *adj=&adjuncts[i++];
    fd_store(result,adj->slotid,adj->table);}
  return result;
}

/* Fetching from adjuncts */

static fd_index l2x(lispval lix)
{
  if (FD_ETERNAL_INDEXP(lix)) {
    int serial = FD_GET_IMMEDIATE(lix,fd_index_type);
    if (serial<FD_N_PRIMARY_INDEXES)
      return fd_primary_indexes[serial];
    else return fd_lisp2index(lix);}
  else if (FD_TYPEP(lix,fd_consed_index_type))
    return (fd_index) lix;
  else return NULL;
}

static fd_pool l2p(lispval lp)
{
  if (FD_ETERNAL_POOLP(lp)) {
    int serial = FD_GET_IMMEDIATE(lp,fd_pool_type);
    if (serial<fd_n_pools)
      return fd_pools_by_serialno[serial];
    else {
      char buf[64];
      fd_seterr3(fd_InvalidPoolPtr,"fd_lisp2pool",
                 u8_sprintf(buf,64,"serial = 0x%x",serial));
      return NULL;}}
  else if (FD_CONSED_POOLP(lp))
    return (fd_pool) lp;
  else return NULL;
}

static lispval adjunct_fetch(fd_adjunct adj,lispval frame,lispval dflt)
{
  lispval store = adj->table;
  return
    ((HASHTABLEP(store)) ?
     (fd_hashtable_get((fd_hashtable)store,frame,FD_EMPTY)) :
     (FD_INDEXP(store)) ? (fd_index_get(l2x(store),frame)) :
     (FD_POOLP(store)) ? (fd_pool_get(l2p(store),frame)) :
     (TYPEP(store,fd_consed_index_type)) ?
     (fd_index_get(((fd_index)store),frame)) :
     (fd_get(store,frame,VOID)));
}

static int adjunct_add(fd_adjunct adj,lispval frame,lispval value)
{
  return fd_add(adj->table,frame,value);
}

static int adjunct_drop(fd_adjunct adj,lispval frame,lispval value)
{
  return fd_drop(adj->table,frame,value);
}

static int adjunct_store(fd_adjunct adj,lispval frame,lispval value)
{
  return fd_store(adj->table,frame,value);
}

static int adjunct_test(fd_adjunct adj,lispval frame,lispval value)
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
FD_EXPORT lispval fd_oid_get(lispval f,lispval slotid,lispval dflt)
{
  fd_pool p = fd_oid2pool(f);
  if (PRED_FALSE(p == NULL)) {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_fetch(adj,f,dflt);}
  fd_adjunct adj = get_adjunct(p,slotid); lispval smap; int free_smap = 0;
  if (adj)
    return adjunct_fetch(adj,f,dflt);
  else {smap = fd_fetch_oid(p,f); free_smap = 1;}
  if (FD_ABORTP(smap))
    return smap;
  else if (SLOTMAPP(smap)) {
    lispval value = fd_slotmap_get((fd_slotmap)smap,slotid,dflt);
    if (free_smap) fd_decref(smap);
    return value;}
  else if (SCHEMAPP(smap)) {
    lispval value = fd_schemap_get((fd_schemap)smap,slotid,dflt);
    if (free_smap) fd_decref(smap);
    return value;}
  else if (TABLEP(smap)) {
    lispval value = fd_get(smap,slotid,dflt);
    if (free_smap) fd_decref(smap);
    return value;}
  else {
    if (free_smap) fd_decref(smap);
    return fd_incref(dflt);}
}

FD_EXPORT int fd_oid_add(lispval f,lispval slotid,lispval value)
{
  fd_pool p = fd_oid2pool(f);
  if (PRED_FALSE(p == NULL)) {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_add(adj,f,value);
    else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      fd_seterr(fd_AnonymousOID,"fd_oid_add",details,f);
      return -1;}}
  lispval smap; int retval;
  fd_adjunct adj = get_adjunct(p,slotid);
  if (adj) return adjunct_add(adj,f,value);
  else smap = fd_locked_oid_value(p,f);
  if (FD_ABORTP(smap))
    return fd_interr(smap);
  else if (SLOTMAPP(smap))
    retval = fd_slotmap_add(FD_XSLOTMAP(smap),slotid,value);
  else if (SCHEMAPP(smap))
    retval = fd_schemap_add(FD_XSCHEMAP(smap),slotid,value);
  else if (FD_TABLEP(smap))
    retval = fd_add(smap,slotid,value);
  else {
    FD_OID addr = FD_OID_ADDR(f);
    u8_byte _details[200];
    u8_string details=
      u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                 FD_OID_HI(addr),FD_OID_LO(addr),
                 p->poolid);
    fd_seterr(fd_BadOIDValue,"fd_oid_add",details,smap);
    retval=-1;}
  fd_decref(smap);
  return retval;
}

FD_EXPORT int fd_oid_store(lispval f,lispval slotid,lispval value)
{
  fd_pool p = fd_oid2pool(f);
  if (PRED_FALSE(p == NULL)) {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_store(adj,f,value);
    else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      fd_seterr(fd_AnonymousOID,"fd_oid_store",details,f);
      return -1;}}
  lispval smap; int retval;
  fd_adjunct adj = get_adjunct(p,slotid);
  if (adj) return adjunct_store(adj,f,value);
  else smap = fd_locked_oid_value(p,f);
  if (FD_ABORTP(smap))
    return fd_interr(smap);
  else if (SLOTMAPP(smap))
    if (EMPTYP(value))
      retval = fd_slotmap_delete(FD_XSLOTMAP(smap),slotid);
    else retval = fd_slotmap_store(FD_XSLOTMAP(smap),slotid,value);
  else if (SCHEMAPP(smap))
    retval = fd_schemap_store(FD_XSCHEMAP(smap),slotid,value);
  else if (FD_TABLEP(smap))
    retval = fd_store(smap,slotid,value);
  else {
    FD_OID addr = FD_OID_ADDR(f);
    u8_byte _details[200];
    u8_string details=
      u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                 FD_OID_HI(addr),FD_OID_LO(addr),
                 p->poolid);
    fd_seterr(fd_BadOIDValue,"fd_oid_store",details,smap);
    retval=-1;}
  fd_decref(smap);
  return retval;
}

FD_EXPORT int fd_oid_delete(lispval f,lispval slotid)
{
  fd_pool p = fd_oid2pool(f);
  if (PRED_FALSE(p == NULL)) {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_store(adj,f,EMPTY);else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      fd_seterr(fd_AnonymousOID,"fd_oid_delete",details,f);
      return -1;}}
  lispval smap; int retval;
  fd_adjunct adj = get_adjunct(p,slotid);
  if (adj) return adjunct_store(adj,f,EMPTY);
  else smap = fd_locked_oid_value(p,f);
  if (FD_ABORTP(smap))
    return fd_interr(smap);
  else if (SLOTMAPP(smap))
    retval = fd_slotmap_delete(FD_XSLOTMAP(smap),slotid);
  else if (SCHEMAPP(smap))
    retval = fd_schemap_store(FD_XSCHEMAP(smap),slotid,EMPTY);
  else if (FD_TABLEP(smap))
    retval = fd_store(smap,slotid,EMPTY);
  else {
    FD_OID addr = FD_OID_ADDR(f);
    u8_byte _details[200];
    u8_string details=
      u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                 FD_OID_HI(addr),FD_OID_LO(addr),
                 p->poolid);
    fd_seterr(fd_BadOIDValue,"fd_oid_delete",details,smap);
    retval=-1;}  fd_decref(smap);
  return retval;
}

FD_EXPORT int fd_oid_drop(lispval f,lispval slotid,lispval value)
{
  fd_pool p = fd_oid2pool(f);
  if (PRED_FALSE(p == NULL))  {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_drop(adj,f,EMPTY);
    else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      fd_seterr(fd_AnonymousOID,"fd_oid_drop",details,f);
      return -1;}}
  lispval smap; int retval;
  fd_adjunct adj = get_adjunct(p,slotid);
  if (adj) return adjunct_drop(adj,f,value);
  else smap = fd_locked_oid_value(p,f);
  if (FD_ABORTP(smap))
    return fd_interr(smap);
  else if (SLOTMAPP(smap))
    retval = fd_slotmap_drop(FD_XSLOTMAP(smap),slotid,value);
  else if (SCHEMAPP(smap))
    retval = fd_schemap_drop(FD_XSCHEMAP(smap),slotid,value);
  else if (FD_TABLEP(smap))
    retval = fd_drop(smap,slotid,value);
  else {
    FD_OID addr = FD_OID_ADDR(f);
    u8_byte _details[200];
    u8_string details=
      u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                 FD_OID_HI(addr),FD_OID_LO(addr),
                 p->poolid);
    fd_seterr(fd_BadOIDValue,"fd_oid_drop",details,smap);
    retval=-1;}
  fd_decref(smap);
  return retval;
}

FD_EXPORT int fd_oid_test(lispval f,lispval slotid,lispval value)
{
  fd_pool p = fd_oid2pool(f);
  if (PRED_FALSE(p == NULL))  {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_test(adj,f,value);
    else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      fd_seterr(fd_AnonymousOID,"fd_oid_drop",details,f);
      return -1;}}
  if (VOIDP(value)) {
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_test(adj,f,value);
    else {
      lispval v = fd_oid_get(f,slotid,VOID);
      if (VOIDP(v))
        return 0;
      else if (FD_ABORTP(v))
        return fd_interr(v);
      else {
        fd_decref(v);
        return 1;}}}
  else {
    lispval smap; int retval;
    fd_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_test(adj,f,value);
    else smap = fd_fetch_oid(p,f);
    if (FD_ABORTP(smap))
      retval = fd_interr(smap);
    else if (SLOTMAPP(smap))
      retval = fd_slotmap_test((fd_slotmap)smap,slotid,value);
    else if (SCHEMAPP(smap))
      retval = fd_schemap_test((fd_schemap)smap,slotid,value);
    else if (TABLEP(smap))
      retval = fd_test(smap,slotid,value);
    else if (FD_EMPTYP(smap))
      return 0;
    else {
      FD_OID addr = FD_OID_ADDR(f);
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                   FD_OID_HI(addr),FD_OID_LO(addr),
                   p->poolid);
      fd_seterr(fd_BadOIDValue,"fd_oid_test",details,smap);
      retval=-1;}
    fd_decref(smap);
    return retval;}
}

FD_EXPORT lispval fd_oid_keys(lispval f)
{
  fd_pool p = fd_oid2pool(f);
  if (PRED_FALSE(p == NULL))
    return EMPTY;
  else {
    lispval smap = fd_fetch_oid(p,f);
    if (FD_ABORTP(smap)) return smap;
    else if (TABLEP(smap)) {
      lispval result = fd_getkeys(smap);
      fd_decref(smap);
      return result;}
    else return EMPTY;}
}

/* Table operations on choices */

static lispval choice_get(lispval arg,lispval slotid,lispval dflt)
{
  lispval results = EMPTY;
  DO_CHOICES(each,arg) {
    lispval v;
    if (OIDP(each))
      v = fd_oid_get(each,slotid,EMPTY);
    else v = fd_get(each,slotid,EMPTY);
    CHOICE_ADD(results,v);}
  if (EMPTYP(results))
    return fd_incref(dflt);
  else return fd_simplify_choice(results);
}

static int choice_add(lispval arg,lispval slotid,lispval value)
{
  if (EMPTYP(value)) return 0;
  else {
    DO_CHOICES(each,arg)
      if (fd_add(each,slotid,value)<0) {
        FD_STOP_DO_CHOICES;
        return -1;}
    return 1;}
}

static int choice_store(lispval arg,lispval slotid,lispval value)
{
  DO_CHOICES(each,arg)
    if (fd_store(each,slotid,value)<0) {
      FD_STOP_DO_CHOICES;
      return -1;}
  return 1;
}

static int choice_drop(lispval arg,lispval slotid,lispval value)
{
  DO_CHOICES(each,arg)
    if (fd_drop(each,slotid,value)<0) {
      FD_STOP_DO_CHOICES;
      return -1;}
  return 1;
}

static int choice_test(lispval arg,lispval slotid,lispval value)
{
  int result = 0;
  DO_CHOICES(each,arg) {
    if ((result = (fd_test(each,slotid,value)))) {
      FD_STOP_DO_CHOICES;
      return result;}}
  return 0;
}

static lispval choice_keys(lispval arg)
{
  lispval results = EMPTY;
  DO_CHOICES(each,arg) {
    lispval keys = fd_getkeys(each);
    if (FD_ABORTP(keys)) {
      fd_decref(results);
      FD_STOP_DO_CHOICES;
      return keys;}
    else {CHOICE_ADD(results,keys);}}
  return results;
}


/* Getting paths */

/* Get Path functions */

FD_EXPORT lispval fd_getpath(lispval start,int n,lispval *path,int infer,int accumulate)
{
  lispval results = EMPTY;
  lispval scan = start; int i = 0;
  if (n==0) return fd_incref(start);
  while (i<n) {
    lispval pred = path[i], newscan = EMPTY;
    DO_CHOICES(s,scan) {
      lispval newval;
      if ((OIDP(pred))||(SYMBOLP(pred)))
        if (infer) newval = fd_frame_get(s,pred);
        else newval = fd_get(scan,pred,EMPTY);
      else if (TABLEP(pred))
        newval = fd_get(pred,s,EMPTY);
      else if (FD_HASHSETP(pred))
        if (fd_hashset_get((fd_hashset)pred,s)) newval = s;
        else newval = EMPTY;
      else if (FD_APPLICABLEP(pred))
        newval = fd_apply(pred,1,&s);
      else if ((PAIRP(pred))&&(FD_APPLICABLEP(FD_CAR(pred)))) {
        lispval fcn = FD_CAR(pred);
        lispval args = FD_CDR(pred), argv[7]; int j = 1;
        argv[0]=s;
        if (PAIRP(args))
          while (PAIRP(args)) {
            argv[j++]=FD_CAR(args); args = FD_CDR(args);}
        else argv[j++]=args;
        if (j>7)
          newval = fd_err(fd_RangeError,"fd_getpath",
                          "too many elements in compound path",VOID);
        else newval = fd_apply(j,fcn,argv);}
      else newval = fd_err(fd_TypeError,"fd_getpath",
                           "invalid path element",VOID);
      if (FD_ABORTP(newval)) {
        fd_decref(scan);
        fd_decref(newscan);
        FD_STOP_DO_CHOICES;
        return newval;}
      else {CHOICE_ADD(newscan,newval);}}
    if (i>0) {
      if (accumulate) {CHOICE_ADD(results,scan);}
      else {fd_decref(scan);}}
    scan = newscan; i++;}
  if (accumulate) {CHOICE_ADD(results,scan);}
  if (accumulate) return results;
  else return scan;
}

/* Initialization */

FD_EXPORT void fd_init_oidobj_c()
{
  u8_register_source_file(_FILEINFO);

  fd_adjunct_slotids = EMPTY;

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
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
