/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/cons.h"

/* Walks the object tree given a walker function. If the walker
   function returns -1, it immediately aborts, if the function return
   1, it declines to descend into the structure. */

kno_walk_fn kno_walkers[KNO_TYPE_MAX];

static int cons_walk(kno_walker walker,int constype,
                     lispval obj,void *walkdata,
                     kno_walk_flags flags,
                     int depth);

KNO_FASTOP int fast_walk(kno_walker walker,lispval obj,
                         void *walkdata,kno_walk_flags flags,
                         int depth)
{
  if (depth<=0)
    return 0;
  else if (CONSP(obj)) {
    int constype = KNO_LISP_TYPE(obj);
    switch (constype) {
    case kno_pair_type: case kno_vector_type:
    case kno_choice_type: case kno_prechoice_type: case kno_qchoice_type:
    case kno_slotmap_type: case kno_schemap_type:
    case kno_hashtable_type: case kno_hashset_type: {
      int rv = walker(obj,walkdata);
      if (rv<=0) return rv;
      else return cons_walk(walker,constype,obj,walkdata,flags,depth);}
    default:
      if (kno_walkers[constype]) {
        int rv = walker(obj,walkdata);
        if (rv<0) return rv;
        else if (rv>0)
          return cons_walk(walker,constype,obj,walkdata,flags,depth);
        else return 0;}
      else if ( (flags==0) || (flags&(KNO_WALK_ALL|KNO_WALK_TERMINALS)) )
        return walker(obj,walkdata);
      else return 0;}}
  else if ((flags)&(KNO_WALK_ALL))
    return walker(obj,walkdata);
  else if (KNO_CONSTANTP(obj)) {
    if ((flags)&(KNO_WALK_CONSTANTS))
      return walker(obj,walkdata);
    else return 0;}
  else if ((flags)&(KNO_WALK_TERMINALS))
    return walker(obj,walkdata);
  else return 0;
}

KNO_EXPORT
/* kno_walk:
    Arguments: two dtype pointers
    Returns: 1, 0, or -1 (an int)
  Walks the object tree, calling walker on nodes encountered. */
int kno_walk(kno_walker walker,lispval obj,void *walkdata,
            kno_walk_flags flags,int depth)
{
  return fast_walk(walker,obj,walkdata,flags,depth);
}

static int cons_walk(kno_walker walker,int constype,
                     lispval obj,void *walkdata,
                     kno_walk_flags flags,
                     int depth)
{
  if (depth<=0) return 0;
  else switch (constype) {
    case kno_pair_type: {
      lispval scan = obj;
      while (PAIRP(scan)) {
        struct KNO_PAIR *pair = (kno_pair) scan;
        int rv = walker(scan,walkdata);
        if (rv>0) {
          fast_walk(walker,pair->car,walkdata,flags,depth-1);
          scan = pair->cdr;}
        else return rv;}
      return fast_walk(walker,scan,walkdata,flags,depth-1);}
    case kno_vector_type: {
      int i = 0, len = VEC_LEN(obj), rv = 0;
      lispval *elts = VEC_DATA(obj);
      while (i<len) {
        rv = fast_walk(walker,elts[i],walkdata,flags,depth-1);
        if (rv<0) return rv;
        i++;}
      return len;}
    case kno_slotmap_type: {
      struct KNO_SLOTMAP *slotmap = KNO_XSLOTMAP(obj);
      kno_read_lock_table(slotmap);
      int i = 0, len = KNO_SLOTMAP_NSLOTS(obj);
      struct KNO_KEYVAL *keyvals = slotmap->sm_keyvals;
      while (i<len) {
        if ((fast_walk(walker,keyvals[i].kv_key,walkdata,flags,depth-1)<0)||
            (fast_walk(walker,keyvals[i].kv_val,walkdata,flags,depth-1)<0)) {
          kno_unlock_table(slotmap);
          return -1;}
        else i++;}
      kno_unlock_table(slotmap);
      return len;}
    case kno_schemap_type: {
      struct KNO_SCHEMAP *schemap = KNO_XSCHEMAP(obj);
      kno_read_lock_table(schemap);
      int i = 0, len = KNO_SCHEMAP_SIZE(obj);
      lispval *slotids = schemap->table_schema;
      lispval *slotvals = schemap->schema_values;
      while (i<len) {
        if ((fast_walk(walker,slotids[i],walkdata,flags,depth-1)<0) ||
            (fast_walk(walker,slotvals[i],walkdata,flags,depth-1)<0) ) {
          kno_unlock_table(schemap);
          return -1;}
        else i++;}
      kno_unlock_table(schemap);
      return len;}
    case kno_choice_type: {
      struct KNO_CHOICE *ch = (struct KNO_CHOICE *)obj;
      const lispval *data = KNO_XCHOICE_DATA(ch);
      int i = 0, len = KNO_XCHOICE_SIZE(ch);
      while (i<len) {
        lispval e = data[i++];
        if (fast_walk(walker,e,walkdata,flags,depth-1)<0)
          return -1;}
      return len;}
    case kno_prechoice_type: {
      struct KNO_PRECHOICE *ach = (struct KNO_PRECHOICE *)obj;
      lispval *data = ach->prechoice_data, *end = ach->prechoice_write;
      while (data<end) {
        lispval e = *data++;
        if (fast_walk(walker,e,walkdata,flags,depth-1)<0)
          return -1;}
      return end-data;}
    case kno_qchoice_type: {
      struct KNO_QCHOICE *qc = (struct KNO_QCHOICE *)obj;
      return fast_walk(walker,qc->qchoiceval,walkdata,flags,depth-1);}
    case kno_hashtable_type: {
      struct KNO_HASHTABLE *ht = (struct KNO_HASHTABLE *)obj;
      int i = 0, n_buckets, n_keys; struct KNO_HASH_BUCKET **buckets;
      kno_read_lock_table(ht);
      n_buckets = ht->ht_n_buckets;
      buckets = ht->ht_buckets;
      n_keys = ht->table_n_keys;
      while (i<n_buckets) {
        if (buckets[i]) {
          struct KNO_HASH_BUCKET *hashentry = buckets[i++];
          int j = 0, n_keyvals = hashentry->bucket_len;
          struct KNO_KEYVAL *keyvals = &(hashentry->kv_val0);
          while (j<n_keyvals) {
            if ((fast_walk(walker,keyvals[j].kv_key,walkdata,flags,depth-1)<0) ||
                (fast_walk(walker,keyvals[j].kv_val,walkdata,flags,depth-1)<0) ) {
              kno_unlock_table(ht);
              return -1;}
            else j++;}}
        else i++;}
      kno_unlock_table(ht);
      return n_keys;}
    case kno_hashset_type: {
      struct KNO_HASHSET *hs = (struct KNO_HASHSET *)obj;
      kno_read_lock_table(hs); {
        int i = 0, n_slots = hs->hs_n_buckets, n_elts = hs->hs_n_elts;
        lispval *slots = hs->hs_buckets;
        while (i<n_slots) {
          if (slots[i]) {
            if (fast_walk(walker,slots[i],walkdata,flags,depth-1)<0) {
              kno_unlock_table(hs);
              return -1;}}
          i++;}
        kno_unlock_table(hs);
        return n_elts;}}
    default:
      if (kno_walkers[constype])
        return kno_walkers[constype](walker,obj,walkdata,flags,depth-1);
      return 0;
    }
}

static int walk_compound(kno_walker walker,lispval x,
                          void *walkdata,
                          kno_walk_flags flags,
                          int depth)
{
  if (depth<=0) return 0;
  struct KNO_COMPOUND *c = kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type);
  int i = 0, len = c->compound_length;
  lispval *data = &(c->compound_0);
  walker(c->compound_typetag,walkdata);
  while (i<len) {
    if (fast_walk(walker,data[i],walkdata,flags,depth-1)<0)
      return -1;
    i++;}
  return len;
}

void kno_init_walk_c()
{
  kno_walkers[kno_compound_type]=walk_compound;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
