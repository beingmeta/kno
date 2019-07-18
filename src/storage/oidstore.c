/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_TABLES KNO_DO_INLINE
#define KNO_INLINE_POOLS KNO_DO_INLINE
#define KNO_INLINE_CHOICES KNO_DO_INLINE
#define KNO_INLINE_IPEVAL KNO_DO_INLINE
#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/apply.h"

#include <libu8/u8printf.h>

#include <stdarg.h>

u8_condition kno_BadAdjunct=_("Bad adjunct table"),
  kno_AdjunctError=_("Pool adjunct error"),
  kno_BadOIDValue=_("Bad OID store value");

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
   using something other than kno_store or kno_slotmap_store to do the
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

lispval kno_adjunct_slotids;

static lispval padjslotid;

static int n_global_adjuncts = 0, max_global_adjuncts = 0;
static struct KNO_ADJUNCT *global_adjuncts = NULL;

KNO_FASTOP kno_adjunct get_adjunct(kno_pool p,lispval slotid)
{
  if (p) {
    struct KNO_ADJUNCT *scan = p->pool_adjuncts;
    struct KNO_ADJUNCT *limit = scan+p->pool_n_adjuncts;
    while (scan<limit)
      if (scan->slotid == slotid) {
        if (FALSEP(scan->table))
          return NULL;
        else return scan;}
      else scan++;
    return NULL;}
  if (global_adjuncts) {
    struct KNO_ADJUNCT *scan = global_adjuncts;
    struct KNO_ADJUNCT *limit = scan+n_global_adjuncts;
    while (scan<limit)
      if (scan->slotid == slotid)
        return scan;
      else scan++;
    return NULL;}
  else return NULL;
}

KNO_EXPORT kno_adjunct kno_get_adjunct(kno_pool p,lispval slotid)
{
  return get_adjunct(p,slotid);
}

KNO_EXPORT int kno_adjunctp(kno_pool p,lispval slotid)
{
  return (get_adjunct(p,slotid)!=NULL);
}

KNO_EXPORT int kno_set_adjunct(kno_pool p,lispval slotid,lispval adjtable)
{
  kno_adjunct adj = get_adjunct(p,slotid);
  if (!(adj)) {
    CHOICE_ADD(kno_adjunct_slotids,slotid);
    kno_adjunct_slotids = kno_simplify_choice(kno_adjunct_slotids);}
  if (adj) {
    lispval old = adj->table;
    kno_incref(adjtable);
    adj->table = adjtable;
    kno_decref(old);
    return 0;}
  else {
    struct KNO_ADJUNCT *adjuncts; int n, max;
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
      struct KNO_ADJUNCT *newadj=
        u8_realloc(adjuncts,sizeof(struct KNO_ADJUNCT)*new_max);
      if (p) {
        adjuncts = p->pool_adjuncts = newadj; p->pool_adjuncts_len = new_max;}
      else {
        adjuncts = global_adjuncts = newadj; max_global_adjuncts = new_max;}}
    adjuncts[n].pool = p; adjuncts[n].slotid = slotid;
    adjuncts[n].table = adjtable;
    kno_incref(adjtable);
    adj = &(adjuncts[n]);
    if (p)
      p->pool_n_adjuncts++;
    else n_global_adjuncts++;
    return 1;}
}

KNO_EXPORT int kno_set_adjuncts(kno_pool p,lispval adjuncts)
{
  int n_adjuncts=0;
  DO_CHOICES(spec,adjuncts) {
    if (KNO_INDEXP(spec)) {
      lispval slotid=kno_get(spec,padjslotid,VOID);
      if ((SYMBOLP(slotid))||(OIDP(slotid))) {
        kno_set_adjunct(p,slotid,spec);
        n_adjuncts++;}
      else {
        kno_seterr(kno_BadAdjunct,"kno_set_adjunct/noslotid",
                  p->poolid,kno_incref(spec));
        KNO_STOP_DO_CHOICES;
        return -1;}}
    else if (KNO_POOLP(spec)) {
      if (p->pool_label) {
        kno_set_adjunct(p,kno_intern(p->pool_label),spec);
        n_adjuncts++;}
      else {
        kno_seterr(kno_BadAdjunct,"kno_set_adjunct/noslotid",
                  p->poolid,kno_incref(spec));
        KNO_STOP_DO_CHOICES;
        return -1;}}
    else {
      lispval slotids=kno_getkeys(spec);
      DO_CHOICES(slotid,slotids) {
        lispval adjunct=kno_get(spec,slotid,VOID);
        if (n_adjuncts<0) {
          kno_decref(adjunct);
          continue;}
        else if (VOIDP(adjunct)) {}
        else if (STRINGP(adjunct)) {
          kno_index ix=kno_get_index(CSTRING(adjunct),-1,KNO_FALSE);
          if (ix) {
            kno_decref(adjunct);
            adjunct=kno_index_ref(ix);}
          else {
            kno_pool adjpool=
              kno_get_pool(CSTRING(adjunct),
                          KNO_STORAGE_ISPOOL|KNO_POOL_ADJUNCT,
                          KNO_FALSE);
            if (adjpool) {
              kno_decref(adjunct);
              adjunct=kno_pool2lisp(adjpool);}
            else {
              kno_seterr(kno_BadAdjunct,"kno_set_adjunct",
                        p->poolid,kno_make_pair(slotid,adjunct));
              kno_decref(adjunct);
              KNO_STOP_DO_CHOICES;
              n_adjuncts=-1;
              break;}}}
        else if (KNO_POOLP(adjunct)) {}
        else if (KNO_INDEXP(adjunct)) {}
        else {
          kno_seterr(kno_BadAdjunct,"kno_set_adjunct",
                    p->poolid,kno_make_pair(slotid,adjunct));
          kno_decref(adjunct);
          n_adjuncts=-1;}
        kno_set_adjunct(p,slotid,adjunct);
        n_adjuncts++;}
      kno_decref(slotids);
      if (n_adjuncts<0) {
        KNO_STOP_DO_CHOICES;
        return -1;}}}
  return n_adjuncts;
}

KNO_EXPORT lispval kno_get_adjuncts(kno_pool p)
{
  int i=0, n_adjuncts=p->pool_n_adjuncts;
  struct KNO_ADJUNCT *adjuncts=p->pool_adjuncts;
  lispval result=kno_make_slotmap(n_adjuncts,0,NULL);
  while (i < n_adjuncts) {
    struct KNO_ADJUNCT *adj=&adjuncts[i++];
    kno_store(result,adj->slotid,adj->table);}
  return result;
}

/* Fetching from adjuncts */

static kno_index l2x(lispval lix)
{
  if (KNO_ETERNAL_INDEXP(lix)) {
    int serial = KNO_GET_IMMEDIATE(lix,kno_index_type);
    if (serial<KNO_N_PRIMARY_INDEXES)
      return kno_primary_indexes[serial];
    else return kno_lisp2index(lix);}
  else if (KNO_TYPEP(lix,kno_consed_index_type))
    return (kno_index) lix;
  else return NULL;
}

static kno_pool l2p(lispval lp)
{
  if (KNO_ETERNAL_POOLP(lp)) {
    int serial = KNO_GET_IMMEDIATE(lp,kno_pool_type);
    if (serial<kno_n_pools)
      return kno_pools_by_serialno[serial];
    else {
      char buf[64];
      kno_seterr3(kno_InvalidPoolPtr,"kno_lisp2pool",
                 u8_sprintf(buf,64,"serial = 0x%x",serial));
      return NULL;}}
  else if (KNO_CONSED_POOLP(lp))
    return (kno_pool) lp;
  else return NULL;
}

static lispval adjunct_fetch(kno_adjunct adj,lispval frame,lispval dflt)
{
  lispval store = adj->table;
  return
    ((HASHTABLEP(store)) ?
     (kno_hashtable_get((kno_hashtable)store,frame,KNO_EMPTY)) :
     (KNO_INDEXP(store)) ? (kno_index_get(l2x(store),frame)) :
     (KNO_POOLP(store)) ? (kno_pool_get(l2p(store),frame)) :
     (TYPEP(store,kno_consed_index_type)) ?
     (kno_index_get(((kno_index)store),frame)) :
     (kno_get(store,frame,VOID)));
}

static int adjunct_add(kno_adjunct adj,lispval frame,lispval value)
{
  return kno_add(adj->table,frame,value);
}

static int adjunct_drop(kno_adjunct adj,lispval frame,lispval value)
{
  return kno_drop(adj->table,frame,value);
}

static int adjunct_store(kno_adjunct adj,lispval frame,lispval value)
{
  return kno_store(adj->table,frame,value);
}

static int adjunct_test(kno_adjunct adj,lispval frame,lispval value)
{
  return kno_test(adj->table,frame,value);
}

/* Table operations on OIDs */

/* OIDs provide an intermediate data layer.  In general, getting a
   slotid from an OID gets the OID's value and then gets the slotid
   from that value (whatever type of object it is).  The exception to
   this case is that certain pools can declare that certain keys
   (slotids) are stored in external index.  These indexes, called
   adjunct indexes, allow another layer of description. */
KNO_EXPORT lispval kno_oid_get(lispval f,lispval slotid,lispval dflt)
{
  kno_pool p = kno_oid2pool(f);
  if (PRED_FALSE(p == NULL)) {
    kno_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_fetch(adj,f,dflt);}
  kno_adjunct adj = get_adjunct(p,slotid); lispval smap; int free_smap = 0;
  if (adj)
    return adjunct_fetch(adj,f,dflt);
  else {smap = kno_fetch_oid(p,f); free_smap = 1;}
  if (KNO_ABORTP(smap))
    return smap;
  else if (SLOTMAPP(smap)) {
    lispval value = kno_slotmap_get((kno_slotmap)smap,slotid,dflt);
    if (free_smap) kno_decref(smap);
    return value;}
  else if (SCHEMAPP(smap)) {
    lispval value = kno_schemap_get((kno_schemap)smap,slotid,dflt);
    if (free_smap) kno_decref(smap);
    return value;}
  else if (TABLEP(smap)) {
    lispval value = kno_get(smap,slotid,dflt);
    if (free_smap) kno_decref(smap);
    return value;}
  else {
    if (free_smap) kno_decref(smap);
    return kno_incref(dflt);}
}

KNO_EXPORT int kno_oid_add(lispval f,lispval slotid,lispval value)
{
  kno_pool p = kno_oid2pool(f);
  if (PRED_FALSE(p == NULL)) {
    kno_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_add(adj,f,value);
    else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      kno_seterr(kno_AnonymousOID,"kno_oid_add",details,f);
      return -1;}}
  lispval smap; int retval;
  kno_adjunct adj = get_adjunct(p,slotid);
  if (adj) return adjunct_add(adj,f,value);
  else smap = kno_locked_oid_value(p,f);
  if (KNO_ABORTP(smap))
    return kno_interr(smap);
  else if (SLOTMAPP(smap))
    retval = kno_slotmap_add(KNO_XSLOTMAP(smap),slotid,value);
  else if (SCHEMAPP(smap))
    retval = kno_schemap_add(KNO_XSCHEMAP(smap),slotid,value);
  else if (KNO_TABLEP(smap))
    retval = kno_add(smap,slotid,value);
  else {
    KNO_OID addr = KNO_OID_ADDR(f);
    u8_byte _details[200];
    u8_string details=
      u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                 KNO_OID_HI(addr),KNO_OID_LO(addr),
                 p->poolid);
    kno_seterr(kno_BadOIDValue,"kno_oid_add",details,smap);
    retval=-1;}
  kno_decref(smap);
  return retval;
}

KNO_EXPORT int kno_oid_store(lispval f,lispval slotid,lispval value)
{
  kno_pool p = kno_oid2pool(f);
  if (PRED_FALSE(p == NULL)) {
    kno_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_store(adj,f,value);
    else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      kno_seterr(kno_AnonymousOID,"kno_oid_store",details,f);
      return -1;}}
  lispval smap; int retval;
  kno_adjunct adj = get_adjunct(p,slotid);
  if (adj) return adjunct_store(adj,f,value);
  else smap = kno_locked_oid_value(p,f);
  if (KNO_ABORTP(smap))
    return kno_interr(smap);
  else if (SLOTMAPP(smap))
    if (EMPTYP(value))
      retval = kno_slotmap_delete(KNO_XSLOTMAP(smap),slotid);
    else retval = kno_slotmap_store(KNO_XSLOTMAP(smap),slotid,value);
  else if (SCHEMAPP(smap))
    retval = kno_schemap_store(KNO_XSCHEMAP(smap),slotid,value);
  else if (KNO_TABLEP(smap))
    retval = kno_store(smap,slotid,value);
  else {
    KNO_OID addr = KNO_OID_ADDR(f);
    u8_byte _details[200];
    u8_string details=
      u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                 KNO_OID_HI(addr),KNO_OID_LO(addr),
                 p->poolid);
    kno_seterr(kno_BadOIDValue,"kno_oid_store",details,smap);
    retval=-1;}
  kno_decref(smap);
  return retval;
}

KNO_EXPORT int kno_oid_delete(lispval f,lispval slotid)
{
  kno_pool p = kno_oid2pool(f);
  if (PRED_FALSE(p == NULL)) {
    kno_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_store(adj,f,EMPTY);else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      kno_seterr(kno_AnonymousOID,"kno_oid_delete",details,f);
      return -1;}}
  lispval smap; int retval;
  kno_adjunct adj = get_adjunct(p,slotid);
  if (adj) return adjunct_store(adj,f,EMPTY);
  else smap = kno_locked_oid_value(p,f);
  if (KNO_ABORTP(smap))
    return kno_interr(smap);
  else if (SLOTMAPP(smap))
    retval = kno_slotmap_delete(KNO_XSLOTMAP(smap),slotid);
  else if (SCHEMAPP(smap))
    retval = kno_schemap_store(KNO_XSCHEMAP(smap),slotid,EMPTY);
  else if (KNO_TABLEP(smap))
    retval = kno_store(smap,slotid,EMPTY);
  else {
    KNO_OID addr = KNO_OID_ADDR(f);
    u8_byte _details[200];
    u8_string details=
      u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                 KNO_OID_HI(addr),KNO_OID_LO(addr),
                 p->poolid);
    kno_seterr(kno_BadOIDValue,"kno_oid_delete",details,smap);
    retval=-1;}  kno_decref(smap);
  return retval;
}

KNO_EXPORT int kno_oid_drop(lispval f,lispval slotid,lispval value)
{
  kno_pool p = kno_oid2pool(f);
  if (PRED_FALSE(p == NULL))  {
    kno_adjunct adj = get_adjunct(p,slotid);
    if (adj)
      return adjunct_drop(adj,f,EMPTY);
    else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      kno_seterr(kno_AnonymousOID,"kno_oid_drop",details,f);
      return -1;}}
  lispval smap; int retval;
  kno_adjunct adj = get_adjunct(p,slotid);
  if (adj) return adjunct_drop(adj,f,value);
  else smap = kno_locked_oid_value(p,f);
  if (KNO_ABORTP(smap))
    return kno_interr(smap);
  else if (SLOTMAPP(smap))
    retval = kno_slotmap_drop(KNO_XSLOTMAP(smap),slotid,value);
  else if (SCHEMAPP(smap))
    retval = kno_schemap_drop(KNO_XSCHEMAP(smap),slotid,value);
  else if (KNO_TABLEP(smap))
    retval = kno_drop(smap,slotid,value);
  else {
    KNO_OID addr = KNO_OID_ADDR(f);
    u8_byte _details[200];
    u8_string details=
      u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                 KNO_OID_HI(addr),KNO_OID_LO(addr),
                 p->poolid);
    kno_seterr(kno_BadOIDValue,"kno_oid_drop",details,smap);
    retval=-1;}
  kno_decref(smap);
  return retval;
}

KNO_EXPORT int kno_oid_test(lispval f,lispval slotid,lispval value)
{
  kno_pool p = kno_oid2pool(f);
  if (PRED_FALSE(p == NULL))  {
    kno_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_test(adj,f,value);
    else {
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"%q",slotid);
      kno_seterr(kno_AnonymousOID,"kno_oid_drop",details,f);
      return -1;}}
  if (VOIDP(value)) {
    kno_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_test(adj,f,value);
    else {
      lispval v = kno_oid_get(f,slotid,VOID);
      if (VOIDP(v))
        return 0;
      else if (KNO_ABORTP(v))
        return kno_interr(v);
      else {
        kno_decref(v);
        return 1;}}}
  else {
    lispval smap; int retval;
    kno_adjunct adj = get_adjunct(p,slotid);
    if (adj) return adjunct_test(adj,f,value);
    else smap = kno_fetch_oid(p,f);
    if (KNO_ABORTP(smap))
      retval = kno_interr(smap);
    else if (SLOTMAPP(smap))
      retval = kno_slotmap_test((kno_slotmap)smap,slotid,value);
    else if (SCHEMAPP(smap))
      retval = kno_schemap_test((kno_schemap)smap,slotid,value);
    else if (TABLEP(smap))
      retval = kno_test(smap,slotid,value);
    else if (KNO_EMPTYP(smap))
      return 0;
    else {
      KNO_OID addr = KNO_OID_ADDR(f);
      u8_byte _details[200];
      u8_string details=
        u8_sprintf(_details,sizeof(_details),"@%lx/%lx(%s)",
                   KNO_OID_HI(addr),KNO_OID_LO(addr),
                   p->poolid);
      kno_seterr(kno_BadOIDValue,"kno_oid_test",details,smap);
      retval=-1;}
    kno_decref(smap);
    return retval;}
}

KNO_EXPORT lispval kno_oid_keys(lispval f)
{
  kno_pool p = kno_oid2pool(f);
  if (PRED_FALSE(p == NULL))
    return EMPTY;
  else {
    lispval smap = kno_fetch_oid(p,f);
    if (KNO_ABORTP(smap)) return smap;
    else if (TABLEP(smap)) {
      lispval result = kno_getkeys(smap);
      kno_decref(smap);
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
      v = kno_oid_get(each,slotid,EMPTY);
    else v = kno_get(each,slotid,EMPTY);
    CHOICE_ADD(results,v);}
  if (EMPTYP(results))
    return kno_incref(dflt);
  else return kno_simplify_choice(results);
}

static int choice_add(lispval arg,lispval slotid,lispval value)
{
  if (EMPTYP(value)) return 0;
  else {
    DO_CHOICES(each,arg)
      if (kno_add(each,slotid,value)<0) {
        KNO_STOP_DO_CHOICES;
        return -1;}
    return 1;}
}

static int choice_store(lispval arg,lispval slotid,lispval value)
{
  DO_CHOICES(each,arg)
    if (kno_store(each,slotid,value)<0) {
      KNO_STOP_DO_CHOICES;
      return -1;}
  return 1;
}

static int choice_drop(lispval arg,lispval slotid,lispval value)
{
  DO_CHOICES(each,arg)
    if (kno_drop(each,slotid,value)<0) {
      KNO_STOP_DO_CHOICES;
      return -1;}
  return 1;
}

static int choice_test(lispval arg,lispval slotid,lispval value)
{
  int result = 0;
  DO_CHOICES(each,arg) {
    if ((result = (kno_test(each,slotid,value)))) {
      KNO_STOP_DO_CHOICES;
      return result;}}
  return 0;
}

static lispval choice_keys(lispval arg)
{
  lispval results = EMPTY;
  DO_CHOICES(each,arg) {
    lispval keys = kno_getkeys(each);
    if (KNO_ABORTP(keys)) {
      kno_decref(results);
      KNO_STOP_DO_CHOICES;
      return keys;}
    else {CHOICE_ADD(results,keys);}}
  return results;
}


/* Getting paths */

/* Get Path functions */

KNO_EXPORT lispval kno_getpath(lispval start,int n,lispval *path,int infer,int accumulate)
{
  lispval results = EMPTY;
  lispval scan = start; int i = 0;
  if (n==0) return kno_incref(start);
  while (i<n) {
    lispval pred = path[i], newscan = EMPTY;
    DO_CHOICES(s,scan) {
      lispval newval;
      if ((OIDP(pred))||(SYMBOLP(pred)))
        if (infer) newval = kno_frame_get(s,pred);
        else newval = kno_get(scan,pred,EMPTY);
      else if (TABLEP(pred))
        newval = kno_get(pred,s,EMPTY);
      else if (KNO_HASHSETP(pred))
        if (kno_hashset_get((kno_hashset)pred,s)) newval = s;
        else newval = EMPTY;
      else if (KNO_APPLICABLEP(pred))
        newval = kno_apply(pred,1,&s);
      else if ((PAIRP(pred))&&(KNO_APPLICABLEP(KNO_CAR(pred)))) {
        lispval fcn = KNO_CAR(pred);
        lispval args = KNO_CDR(pred), argv[7]; int j = 1;
        argv[0]=s;
        if (PAIRP(args))
          while (PAIRP(args)) {
            argv[j++]=KNO_CAR(args); args = KNO_CDR(args);}
        else argv[j++]=args;
        if (j>7)
          newval = kno_err(kno_RangeError,"kno_getpath",
                          "too many elements in compound path",VOID);
        else newval = kno_apply(j,fcn,argv);}
      else newval = kno_err(kno_TypeError,"kno_getpath",
                           "invalid path element",VOID);
      if (KNO_ABORTP(newval)) {
        kno_decref(scan);
        kno_decref(newscan);
        KNO_STOP_DO_CHOICES;
        return newval;}
      else {CHOICE_ADD(newscan,newval);}}
    if (i>0) {
      if (accumulate) {CHOICE_ADD(results,scan);}
      else {kno_decref(scan);}}
    scan = newscan; i++;}
  if (accumulate) {CHOICE_ADD(results,scan);}
  if (accumulate) return results;
  else return scan;
}

/* Initialization */

KNO_EXPORT void kno_init_oidobj_c()
{
  u8_register_source_file(_FILEINFO);

  kno_adjunct_slotids = EMPTY;

  padjslotid=kno_intern("%adjunct_slotid");

  /* Table functions for OIDs */
  kno_tablefns[kno_oid_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_oid_type]->get = kno_oid_get;
  kno_tablefns[kno_oid_type]->add = kno_oid_add;
  kno_tablefns[kno_oid_type]->drop = kno_oid_drop;
  kno_tablefns[kno_oid_type]->store = kno_oid_store;
  kno_tablefns[kno_oid_type]->test = kno_oid_test;
  kno_tablefns[kno_oid_type]->keys = kno_oid_keys;
  kno_tablefns[kno_oid_type]->getsize = NULL;

  /* Table functions for CHOICEs */
  kno_tablefns[kno_choice_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_choice_type]->get = choice_get;
  kno_tablefns[kno_choice_type]->add = choice_add;
  kno_tablefns[kno_choice_type]->drop = choice_drop;
  kno_tablefns[kno_choice_type]->store = choice_store;
  kno_tablefns[kno_choice_type]->test = choice_test;
  kno_tablefns[kno_choice_type]->keys = choice_keys;
  kno_tablefns[kno_choice_type]->getsize = NULL;

  /* Table functions for CHOICEs */
  kno_tablefns[kno_prechoice_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_prechoice_type]->get = choice_get;
  kno_tablefns[kno_prechoice_type]->add = choice_add;
  kno_tablefns[kno_prechoice_type]->drop = choice_drop;
  kno_tablefns[kno_prechoice_type]->store = choice_store;
  kno_tablefns[kno_prechoice_type]->test = choice_test;
  kno_tablefns[kno_prechoice_type]->keys = choice_keys;
  kno_tablefns[kno_prechoice_type]->getsize = NULL;

}

