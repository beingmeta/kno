/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/cons.h"

/* Walks the object tree given a walker function. If the walker
   function returns -1, it immediately aborts, if the function return
   1, it declines to descend into the structure. */

fd_walk_fn fd_walkers[FD_TYPE_MAX];

static int cons_walk(fd_walker walker,int constype,
                     lispval obj,void *walkdata,
                     fd_walk_flags flags,
                     int depth);

FD_FASTOP int fast_walk(fd_walker walker,lispval obj,
                         void *walkdata,fd_walk_flags flags,
                         int depth)
{
  if (depth<=0)
    return 0;
  else if (CONSP(obj)) {
    int constype = FD_PTR_TYPE(obj);
    switch (constype) {
    case fd_pair_type: case fd_vector_type: case fd_code_type:
    case fd_choice_type: case fd_prechoice_type: case fd_qchoice_type:
    case fd_slotmap_type: case fd_schemap_type:
    case fd_hashtable_type: case fd_hashset_type: {
      int rv = walker(obj,walkdata);
      if (rv<=0) return rv;
      else return cons_walk(walker,constype,obj,walkdata,flags,depth);}
    default:
      if (fd_walkers[constype]) {
        int rv = walker(obj,walkdata);
        if (rv<0) return rv;
        else if (rv>0)
          return cons_walk(walker,constype,obj,walkdata,flags,depth);
        else return 0;}
      else if ( (flags==0) || (flags&(FD_WALK_ALL|FD_WALK_TERMINALS)) )
        return walker(obj,walkdata);
      else return 0;}}
  else if ((flags)&(FD_WALK_ALL))
    return walker(obj,walkdata);
  else if (FD_CONSTANTP(obj)) {
    if ((flags)&(FD_WALK_CONSTANTS))
      return walker(obj,walkdata);
    else return 0;}
  else if ((flags)&(FD_WALK_TERMINALS))
    return walker(obj,walkdata);
  else return 0;
}

FD_EXPORT
/* fd_walk:
    Arguments: two dtype pointers
    Returns: 1, 0, or -1 (an int)
  Walks the object tree, calling walker on nodes encountered. */
int fd_walk(fd_walker walker,lispval obj,void *walkdata,
            fd_walk_flags flags,int depth)
{
  return fast_walk(walker,obj,walkdata,flags,depth);
}

static int cons_walk(fd_walker walker,int constype,
                     lispval obj,void *walkdata,
                     fd_walk_flags flags,
                     int depth)
{
  if (depth<=0) return 0;
  else switch (constype) {
    case fd_pair_type: {
      lispval scan = obj;
      while (PAIRP(scan)) {
        struct FD_PAIR *pair = (fd_pair) scan;
        int rv = walker(scan,walkdata);
        if (rv>0) {
          fast_walk(walker,pair->car,walkdata,flags,depth-1);
          scan = pair->cdr;}
        else return rv;}
      return fast_walk(walker,scan,walkdata,flags,depth-1);}
    case fd_code_type: case fd_vector_type: {
      int i = 0, len = VEC_LEN(obj), rv = 0;
      lispval *elts = VEC_DATA(obj);
      while (i<len) {
        rv = fast_walk(walker,elts[i],walkdata,flags,depth-1);
        if (rv<0) return rv;
        i++;}
      return len;}
    case fd_slotmap_type: {
      struct FD_SLOTMAP *slotmap = FD_XSLOTMAP(obj);
      fd_read_lock_table(slotmap);
      int i = 0, len = FD_SLOTMAP_NSLOTS(obj);
      struct FD_KEYVAL *keyvals = slotmap->sm_keyvals;
      while (i<len) {
        if ((fast_walk(walker,keyvals[i].kv_key,walkdata,flags,depth-1)<0)||
            (fast_walk(walker,keyvals[i].kv_val,walkdata,flags,depth-1)<0)) {
          fd_unlock_table(slotmap);
          return -1;}
        else i++;}
      fd_unlock_table(slotmap);
      return len;}
    case fd_schemap_type: {
      struct FD_SCHEMAP *schemap = FD_XSCHEMAP(obj);
      fd_read_lock_table(schemap);
      int i = 0, len = FD_SCHEMAP_SIZE(obj);
      lispval *slotids = schemap->table_schema;
      lispval *slotvals = schemap->schema_values;
      while (i<len) {
        if ((fast_walk(walker,slotids[i],walkdata,flags,depth-1)<0) ||
            (fast_walk(walker,slotvals[i],walkdata,flags,depth-1)<0) ) {
          fd_unlock_table(schemap);
          return -1;}
        else i++;}
      fd_unlock_table(schemap);
      return len;}
    case fd_choice_type: {
      struct FD_CHOICE *ch = (struct FD_CHOICE *)obj;
      const lispval *data = FD_XCHOICE_DATA(ch);
      int i = 0, len = FD_XCHOICE_SIZE(ch);
      while (i<len) {
        lispval e = data[i++];
        if (fast_walk(walker,e,walkdata,flags,depth-1)<0)
          return -1;}
      return len;}
    case fd_prechoice_type: {
      struct FD_PRECHOICE *ach = (struct FD_PRECHOICE *)obj;
      lispval *data = ach->prechoice_data, *end = ach->prechoice_write;
      while (data<end) {
        lispval e = *data++;
        if (fast_walk(walker,e,walkdata,flags,depth-1)<0)
          return -1;}
      return end-data;}
    case fd_qchoice_type: {
      struct FD_QCHOICE *qc = (struct FD_QCHOICE *)obj;
      return fast_walk(walker,qc->qchoiceval,walkdata,flags,depth-1);}
    case fd_hashtable_type: {
      struct FD_HASHTABLE *ht = (struct FD_HASHTABLE *)obj;
      int i = 0, n_buckets, n_keys; struct FD_HASH_BUCKET **buckets;
      fd_read_lock_table(ht);
      n_buckets = ht->ht_n_buckets;
      buckets = ht->ht_buckets;
      n_keys = ht->table_n_keys;
      while (i<n_buckets) {
        if (buckets[i]) {
          struct FD_HASH_BUCKET *hashentry = buckets[i++];
          int j = 0, n_keyvals = hashentry->bucket_len;
          struct FD_KEYVAL *keyvals = &(hashentry->kv_val0);
          while (j<n_keyvals) {
            if ((fast_walk(walker,keyvals[j].kv_key,walkdata,flags,depth-1)<0) ||
                (fast_walk(walker,keyvals[j].kv_val,walkdata,flags,depth-1)<0) ) {
              fd_unlock_table(ht);
              return -1;}
            else j++;}}
        else i++;}
      fd_unlock_table(ht);
      return n_keys;}
    case fd_hashset_type: {
      struct FD_HASHSET *hs = (struct FD_HASHSET *)obj;
      fd_read_lock_table(hs); {
        int i = 0, n_slots = hs->hs_n_buckets, n_elts = hs->hs_n_elts;
        lispval *slots = hs->hs_buckets;
        while (i<n_slots) {
          if (slots[i]) {
            if (fast_walk(walker,slots[i],walkdata,flags,depth-1)<0) {
              fd_unlock_table(hs);
              return -1;}}
          i++;}
        fd_unlock_table(hs);
        return n_elts;}}
    default:
      if (fd_walkers[constype])
        return fd_walkers[constype](walker,obj,walkdata,flags,depth-1);
      return 0;
    }
}

static int walk_compound(fd_walker walker,lispval x,
                          void *walkdata,
                          fd_walk_flags flags,
                          int depth)
{
  if (depth<=0) return 0;
  struct FD_COMPOUND *c = fd_consptr(struct FD_COMPOUND *,x,fd_compound_type);
  int i = 0, len = c->compound_length;
  lispval *data = &(c->compound_0);
  walker(c->compound_typetag,walkdata);
  while (i<len) {
    if (fast_walk(walker,data[i],walkdata,flags,depth-1)<0)
      return -1;
    i++;}
  return len;
}

void fd_init_walk_c()
{
  fd_walkers[fd_compound_type]=walk_compound;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
