/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/cprims.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

FDPRIM("TABLE?",tablep,1,FD_NEEDS_1_ARG,
       "`(TABLE? *obj*)` returns #t if *obj* is a table, #f otherwise.")
  (lispval arg)
{
  if (TABLEP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

FDPRIM("HASKEYS?",haskeysp,1,FD_NEEDS_1_ARG,
       "`(HASKEYS? *obj*)` returns #t if *obj* is a non-empty table, "
       "#f otherwise.")
  (lispval arg)
{
  if (TABLEP(arg)) {
    fd_ptr_type argtype = FD_PTR_TYPE(arg);
    if ((fd_tablefns[argtype])->keys)
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

FDPRIM("SLOTMAP?",slotmapp,1,FD_NEEDS_1_ARG,
       "`(SLOTMAP? *obj*)` returns #t if *obj* is a slotmap, #f otherwise.")
  (lispval x)
{
  if (SLOTMAPP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

FDPRIM("SCHEMAP?",schemapp,1,FD_NEEDS_1_ARG,
       "`(SCHEMAP? *obj*)` returns #t if *obj* is a slotmap, #f otherwise.")
  (lispval x)
{
  if (SCHEMAPP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

FDPRIM("HASHTABLE?",hashtablep,1,FD_NEEDS_1_ARG,
       "`(HASHTABLE? *obj*)` returns #t if *obj* is a slotmap, #f otherwise.")
  (lispval x)
{
  if (HASHTABLEP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

FDPRIM1("MAKE-HASHSET",make_hashset,FD_NEEDS_NO_ARGS,
       "`(MAKE-HASHSET [*n_buckets*])` returns a hashset. "
        "*n_buckets*, if provided indicates the number of buckets",
        fd_fixnum_type,FD_VOID)
  (lispval arg)
{
  struct FD_HASHSET *h = u8_alloc(struct FD_HASHSET);
  if (VOIDP(arg))
    fd_init_hashset(h,17,FD_MALLOCD_CONS);
  else if (FD_UINTP(arg))
    fd_init_hashset(h,FIX2INT(arg),FD_MALLOCD_CONS);
  else return fd_type_error("uint","make_hashset",arg);
  FD_INIT_CONS(h,fd_hashset_type);
  return LISP_CONS(h);
}

FDPRIM1("MAKE-HASHTABLE",make_hashtable,FD_NEEDS_NO_ARGS,
       "`(MAKE-HASHTABLE [*n_buckets*])` returns a hashset. "
        "*n_buckets*, if provided indicates the number of buckets",
        fd_fixnum_type,FD_VOID)
  (lispval size)
{
  if (FD_UINTP(size))
    return fd_make_hashtable(NULL,FIX2INT(size));
  else return fd_make_hashtable(NULL,0);
}

FDPRIM1("PICK-HASHTABLE-SIZE", pick_hashtable_size,FD_NEEDS_1_ARG,
        "`(PICK-HASHTABLE-SIZE *count*)` picks a good hashtable size "
        "for a table of *count* elements.",
        fd_fixnum_type,FD_VOID)
  (lispval count_arg)
{
  if (!(FD_UINTP(count_arg)))
    return fd_type_error("uint","pick_hashtable_size",count_arg);
  int count = FIX2INT(count_arg);
  int size = fd_get_hashtable_size(count);
  return FD_INT(size);
}

FDPRIM2("RESET-HASHTABLE!",reset_hashtable,FD_NEEDS_1_ARG,
        "`(RESET-HASHTABLE! *table* [*slots*)` resets a hashtable, removing "
        "all of its values",
        fd_hashtable_type,FD_VOID,fd_fixnum_type,FD_VOID)
  (lispval table,lispval n_slots)
{
  if (!(FD_UINTP(n_slots)))
    return fd_type_error("uint","reset_hashtable",n_slots);
  fd_reset_hashtable((fd_hashtable)table,FIX2INT(n_slots),1);
  return VOID;
}

FDPRIM1("STATIC-HASHTABLE",static_hashtable,FD_NEEDS_1_ARG,
        "`(STATIC-HASHTABLE *table* )` declares all keys and values "
        "in *table* to be static (no GC) and disables locking for the table. "
        "This may improve performance on objects in the table",
        fd_hashtable_type,FD_VOID)
  (lispval table)
{
  struct FD_HASHTABLE *ht = (fd_hashtable)table;
  fd_write_lock_table(ht);
  fd_static_hashtable(ht,-1);
  ht->table_uselock = 0;
  fd_unlock_table(ht);
  return fd_incref(table);
}

FDPRIM1("UNSAFE-HASHTABLE",unsafe_hashtable,FD_NEEDS_1_ARG,
        "`(UNSAFE-TABLE *table* )` disables locking for *table*. "
        "This may improve performance.",
        fd_hashtable_type,FD_VOID)
  (lispval table)
{
  struct FD_HASHTABLE *ht = (fd_hashtable)table;
  fd_write_lock_table(ht);
  ht->table_uselock = 0;
  fd_unlock_table(ht);
  return fd_incref(table);
}

FDPRIM1("RESAFE-HASHTABLE",resafe_hashtable,FD_NEEDS_1_ARG,
        "`(RESAFE-TABLE *table* )` re-enables locking for *table* "
        "disabled by `UNSAFE-TABLE`.",
        fd_hashtable_type,FD_VOID)
  (lispval table)
{
  struct FD_HASHTABLE *ht = (fd_hashtable)table;
  fd_write_lock_table(ht);
  ht->table_uselock = 1;
  fd_unlock_table(ht);
  return fd_incref(table);
}

FDPRIM1("HASH-LISP",hash_lisp_prim,FD_NEEDS_1_ARG,
        "`(HASH_LISP *object* )` returns an integer hash value "
        "for *object*.",
        -1,FD_VOID)
  (lispval x)
{
  int val = fd_hash_lisp(x);
  return FD_INT(val);
}

FDPRIM("%GET",lispget,3,FD_NEEDS_2_ARGS|FD_NDCALL,
       "`(%GET *table* *key* [*default*])` returns the value "
       "of *key* in *table* or *default* if *table* does not contain "
       "*key*. *default* defaults to the empty choice {}."
       "Note that this does no inference, use GET to enable inference.")
  (lispval table,lispval key,lispval dflt)
{
  if (VOIDP(dflt))
    return fd_get(table,key,EMPTY);
  else return fd_get(table,key,dflt);
}

FDPRIM("ADD!",lispadd,3,FD_NEEDS_3_ARGS|FD_NDCALL,
       "`(ADD! *table* *key* *value*)` adds *value* to "
       "the associations of *key* in *table*. "
       "Note that this does no inference, use ASSERT! to enable inference.")
  (lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return VOID;
  else if (EMPTYP(key)) return VOID;
  else if (fd_add(table,key,val)<0)
    return FD_ERROR;
  else return VOID;
}
FDPRIM("DROP!",lispdrop,3,FD_NEEDS_2_ARGS|FD_NDCALL,
        "`(DROP! *table* *key* [*values*])` removes *values* "
        "from *key* of *table*. If *values* is not provided, "
        "all values associated with *key* are removed. "
        "Note that this does no inference, use RETRACT! to enable inference.")
  (lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return VOID;
  if (EMPTYP(key)) return VOID;
  else if (fd_drop(table,key,val)<0) return FD_ERROR;
  else return VOID;
}
FDPRIM("STORE!",lispstore,3,FD_NEEDS_ALL_ARGS|FD_NDCALL,
       "`(STORE! *table* *key* *value*)` stores *value* in "
       "*table* under *key*, removing all existing values. If "
       "*value* is a choice, the entire choice is stored under "
       "*key*. Note that this does no inference.")
  (lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return VOID;
  else if (EMPTYP(key)) return VOID;
  else if (QCHOICEP(val)) {
    struct FD_QCHOICE *qch = FD_XQCHOICE(val);
    if (fd_store(table,key,qch->qchoiceval)<0)
      return FD_ERROR;
    else return VOID;}
  else if (fd_store(table,key,val)<0)
    return FD_ERROR;
  else return VOID;
}
FDPRIM("%TEST",lisptest,3,FD_NEEDS_2_ARGS|FD_NDCALL,
       "`(%TEST *tables* *keys* [*values*])` returns true if "
       "any of *values* is stored undery any of *keys* in "
       "any of *tables*. If *values* is not provided, returns true "
       "if any values are stored under any of *keys* in any of *tables*. "
       "Note that this does no inference.")
  (lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return FD_FALSE;
  else if (EMPTYP(key)) return FD_FALSE;
  else if (EMPTYP(val)) return FD_FALSE;
  else {
    int retval = fd_test(table,key,val);
    if (retval<0) return FD_ERROR;
    else if (retval) return FD_TRUE;
    else return FD_FALSE;}
}

FDPRIM("GETIF",lispgetif,3,FD_NEEDS_2_ARGS|FD_NDCALL,
       "`(GETIF *table* *key* [*default*])` returns *key* if *table* is false, "
       "or returns the value of *key* in *table* or *default* if *table* does "
       "not contain *key*. Note that this does no inference.")
  (lispval table,lispval key,lispval dflt)
{
  if (FALSEP(table))
    return fd_incref(key);
  else if (VOIDP(dflt))
    return fd_get(table,key,EMPTY);
  else return fd_get(table,key,dflt);
}

FDPRIM("TRYGET",lisptryget,3,FD_NEEDS_2_ARGS|FD_NDCALL,
       "`(TRYGET *table* *key* [*default*])` returns *key* if *table* is false, "
       "or returns the value of *key* in *table* or *default* if *table* does "
       "not contain *key*. Note that this does no inference.")
  (lispval table,lispval key,lispval dflt)
{
  if ((FALSEP(table)) || (EMPTYP(table)))
    if (VOIDP(dflt))
      return fd_incref(key);
    else return fd_incref(dflt);
  else if (CHOICEP(table)) {
    lispval results = EMPTY;
    DO_CHOICES(etable,table) {
      DO_CHOICES(ekey,key) {
        lispval v = ((VOIDP(dflt)) ? (fd_get(etable,ekey,ekey)) :
                  (fd_get(etable,ekey,dflt)));
        CHOICE_ADD(results,v);}}
    if (EMPTYP(results))
      if (VOIDP(dflt))
        return fd_incref(key);
      else return fd_incref(dflt);
    else return results;}
  else if (CHOICEP(key)) {
    lispval results = EMPTY;
    DO_CHOICES(ekey,key) {
      lispval v = ((VOIDP(dflt)) ? (fd_get(table,ekey,ekey)) :
                (fd_get(table,ekey,dflt)));
      CHOICE_ADD(results,v);}
    if (EMPTYP(results))
      if (VOIDP(dflt))
        return fd_incref(key);
      else return fd_incref(dflt);
    else return results;}
  else if (VOIDP(dflt))
    return fd_get(table,key,key);
  else return fd_get(table,key,dflt);
}

FDPRIM("PICK-KEYS",lisp_pick_keys,2,FD_NEEDS_1_ARG,
       "`(PICK-KEYS *table* [*count*])` returns *count* keys "
       "from *table* or all of the keys if they're less than *count*.")
  (lispval table,lispval howmany_arg)
{
  if (!(TABLEP(table)))
    return fd_type_error(_("table"),"lisp_pick_key",table);
  else if (!(FD_UINTP(howmany_arg)))
    return fd_type_error(_("uint"),"lisp_pick_key",howmany_arg);
  else {
    lispval x = fd_getkeys(table);
    lispval normal = fd_make_simple_choice(x);
    int n = FD_CHOICE_SIZE(normal), howmany = FIX2INT(howmany_arg);
    if (!(CHOICEP(normal))) return normal;
    if (n<=howmany) return normal;
    else if (howmany==1) {
      int i = u8_random(n);
      const lispval *data = FD_CHOICE_DATA(normal);
      lispval result = data[i];
      fd_incref(result); fd_decref(normal);
      return result;}
    else if (n) {
      struct FD_HASHSET h;
      const lispval *data = FD_CHOICE_DATA(normal);
      int j = 0; fd_init_hashset(&h,n*3,FD_STACK_CONS);
      while (j<howmany) {
        int i = u8_random(n);
        if (fd_hashset_mod(&h,data[i],1)<0) {
          fd_recycle_hashset(&h);
          fd_decref(normal);
          fd_decref(x);
          return FD_ERROR;}
        else j++;}
      fd_decref(normal);
      return fd_hashset_elts(&h,1);}
    else return EMPTY;}
}

/* Converting schemaps to slotmaps */

static lispval schemap2slotmap_prim(lispval in)
{
  struct FD_SCHEMAP *schemap = (fd_schemap) in;
  lispval *schema = schemap->table_schema;
  lispval *values = schemap->schema_values;
  int size = schemap->schema_length;
  struct FD_KEYVAL kv[size];
  int i = 0; while (i < size) {
    lispval key = schema[i]; fd_incref(key);
    lispval val = values[i]; fd_incref(val);
    kv[i].kv_key = key;
    kv[i].kv_val = val;
    i++;}
  return fd_make_slotmap(size,size,kv);
}

/* Support for some iterated operations */

typedef lispval (*reduceop)(lispval,lispval);

/* Various table operations */

static lispval hashtable_increment(lispval table,lispval keys,lispval increment)
{
  if (EMPTYP(increment)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (VOIDP(increment)) increment = FD_INT(1);
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_increment,n_elts,elts,increment)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_increment,keys,increment)<0)
      return FD_ERROR;
    else return VOID;
  else return fd_type_error("table","hashtable_increment",table);
}

static lispval table_increment(lispval table,lispval keys,lispval increment)
{
  if (EMPTYP(increment)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (VOIDP(increment)) increment = FD_INT(1);
  else if (!(NUMBERP(increment)))
    return fd_type_error("number","table_increment",increment);
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_increment,n_elts,elts,increment)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_increment,keys,
                             increment)<0)
      return FD_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = fd_get(table,key,VOID);
      if (VOIDP(cur))
        fd_store(table,key,increment);
      else if ((FIXNUMP(cur)) && (FIXNUMP(increment))) {
        long long sum = FIX2INT(cur)+FIX2INT(increment);
        lispval lsum = FD_INT(sum);
        fd_store(table,key,lsum);
        fd_decref(lsum);}
      else if (NUMBERP(cur)) {
        lispval lsum = fd_plus(cur,increment);
        fd_store(table,key,lsum);
        fd_decref(lsum); fd_decref(cur);}
      else return fd_type_error("number","table_increment",cur);}
    return VOID;}
  else return fd_type_error("table","table_increment",table);
}

static lispval hashtable_increment_existing
  (lispval table,lispval key,lispval increment)
{
  if (EMPTYP(increment)) return VOID;
  else if (EMPTYP(key)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (VOIDP(increment)) increment = FD_INT(1);
  if (HASHTABLEP(table))
    if (CHOICEP(key)) {
      lispval keys = fd_make_simple_choice(key);
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys(FD_XHASHTABLE(table),
                                fd_table_increment_if_present,
                                n_elts,elts,increment)<0) {
        fd_decref(keys); return FD_ERROR;}
      else {fd_decref(keys); return VOID;}}
    else if (EMPTYP(key))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),
                             fd_table_increment_if_present,
                             key,increment)<0)
      return FD_ERROR;
    else return VOID;
  else return fd_type_error("table","hashtable_increment_existing",table);
}

static lispval table_increment_existing
                (lispval table,lispval keys,lispval increment)
{
  if (EMPTYP(increment)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (VOIDP(increment)) increment = FD_INT(1);
  else if (!(NUMBERP(increment)))
    return fd_type_error("number","table_increment_existing",increment);
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_increment,n_elts,elts,increment)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_increment,keys,
                             increment)<0)
      return FD_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = fd_get(table,key,VOID);
      if (VOIDP(cur)) {}
      else if ((FIXNUMP(cur)) && (FIXNUMP(increment))) {
        long long sum = FIX2INT(cur)+FIX2INT(increment);
        lispval lsum = FD_INT(sum);
        fd_store(table,key,lsum);
        fd_decref(lsum);}
      else if (NUMBERP(cur)) {
        lispval lsum = fd_plus(cur,increment);
        fd_store(table,key,lsum);
        fd_decref(lsum); fd_decref(cur);}
      else return fd_type_error("number","table_increment_existing",cur);}
    return VOID;}
  else return fd_type_error("table","table_increment_existing",table);
}

static lispval hashtable_multiply(lispval table,lispval key,lispval factor)
{
  if (EMPTYP(factor)) return VOID;
  else if (EMPTYP(key)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (VOIDP(factor)) factor = FD_INT(2);
  if (HASHTABLEP(table))
    if (CHOICEP(key)) {
      lispval keys = fd_make_simple_choice(key);
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys(FD_XHASHTABLE(table),
                                fd_table_multiply,
                                n_elts,elts,factor)<0) {
        fd_decref(keys); return FD_ERROR;}
      else {fd_decref(keys); return VOID;}}
    else if (EMPTYP(key))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),
                             fd_table_multiply,
                             key,factor)<0)
      return FD_ERROR;
    else return VOID;
  else return fd_type_error("table","hashtable_multiply",table);
}

static lispval table_multiply(lispval table,lispval keys,lispval factor)
{
  if (EMPTYP(factor)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (VOIDP(factor)) factor = FD_INT(1);
  else if (!(NUMBERP(factor)))
    return fd_type_error("number","table_multiply",factor);
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_multiply,n_elts,elts,factor)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_multiply,keys,factor)<0)
      return FD_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = fd_get(table,key,VOID);
      if (VOIDP(cur))
        fd_store(table,key,factor);
      else if (NUMBERP(cur)) {
        lispval lsum = fd_multiply(cur,factor);
        fd_store(table,key,lsum);
        fd_decref(lsum); fd_decref(cur);}
      else return fd_type_error("number","table_multiply",cur);}
    return VOID;}
  else return fd_type_error("table","table_multiply",table);
}

static lispval hashtable_multiply_existing
  (lispval table,lispval key,lispval factor)
{
  if (EMPTYP(factor)) return VOID;
  else if (EMPTYP(key)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (VOIDP(factor)) factor = FD_INT(2);
  if (HASHTABLEP(table))
    if (CHOICEP(key)) {
      lispval keys = fd_make_simple_choice(key);
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys(FD_XHASHTABLE(table),
                                fd_table_multiply_if_present,
                                n_elts,elts,factor)<0) {
        fd_decref(keys); return FD_ERROR;}
      else {fd_decref(keys); return VOID;}}
    else if (EMPTYP(key))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),
                             fd_table_multiply_if_present,
                             key,factor)<0)
      return FD_ERROR;
    else return VOID;
  else return fd_type_error("table","hashtable_multiply_existing",table);
}

static lispval table_multiply_existing(lispval table,lispval keys,lispval factor)
{
  if (EMPTYP(factor)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (VOIDP(factor)) factor = FD_INT(1);
  else if (!(NUMBERP(factor)))
    return fd_type_error("number","table_multiply_existing",factor);
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_multiply_if_present,n_elts,elts,factor)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_multiply_if_present,keys,factor)<0)
      return FD_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = fd_get(table,key,VOID);
      if (VOIDP(cur)) {}
      else if (NUMBERP(cur)) {
        lispval lsum = fd_multiply(cur,factor);
        fd_store(table,key,lsum);
        fd_decref(lsum); fd_decref(cur);}
      else return fd_type_error("number","table_multiply_existing",cur);}
    return VOID;}
  else return fd_type_error("table","table_multiply_existing",table);
}

/* Table MAXIMIZE
   Stores a value in a table if the current value is either empty or
   less than the new value. */

static lispval table_maximize(lispval table,lispval keys,lispval maxval)
{
  if (EMPTYP(maxval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (!(NUMBERP(maxval)))
    return fd_type_error("number","table_maximize",maxval);
  else if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_maximize,n_elts,elts,maxval)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_maximize,keys,maxval)<0)
      return FD_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = fd_get(table,key,VOID);
      if (VOIDP(cur))
        fd_store(table,key,maxval);
      else if (NUMBERP(cur)) {
        if (fd_numcompare(maxval,cur)>0) {
          fd_store(table,key,maxval);
          fd_decref(cur);}
        else {fd_decref(cur);}}
      else return fd_type_error("number","table_maximize",cur);}
    return VOID;}
  else return fd_type_error("table","table_maximize",table);
}

static lispval table_maximize_existing(lispval table,lispval keys,lispval maxval)
{
  if (EMPTYP(maxval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (!(NUMBERP(maxval)))
    return fd_type_error("number","table_maximize_existing",maxval);
  else if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_maximize_if_present,n_elts,elts,maxval)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_maximize_if_present,keys,maxval)<0)
      return FD_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = fd_get(table,key,VOID);
      if (VOIDP(cur)) {}
      else if (NUMBERP(cur)) {
        if (fd_numcompare(maxval,cur)>0) {
          fd_store(table,key,maxval);
          fd_decref(cur);}
        else {fd_decref(cur);}}
      else return fd_type_error("number","table_maximize",cur);}
    return VOID;}
  else return fd_type_error("table","table_maximize",table);
}

static lispval hashtable_maximize(lispval table,lispval keys,lispval maxval)
{
  if (EMPTYP(maxval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_maximize,n_elts,elts,maxval)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_maximize,keys,maxval)<0)
      return FD_ERROR;
    else return VOID;
  else return fd_type_error("table","hashtable_maximize",table);
}

static lispval hashtable_maximize_existing(lispval table,lispval keys,lispval maxval)
{
  if (EMPTYP(maxval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_maximize_if_present,n_elts,elts,maxval)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_maximize_if_present,keys,maxval)<0)
      return FD_ERROR;
    else return VOID;
  else return fd_type_error("table","hashtable_maximize_existing",table);
}

/* Table MINIMIZE
   Stores a value in a table if the current value is either empty or
   less than the new value. */

static lispval table_minimize(lispval table,lispval keys,lispval minval)
{
  if (EMPTYP(minval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (!(NUMBERP(minval)))
    return fd_type_error("number","table_minimize",minval);
  else if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_minimize,n_elts,elts,minval)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_minimize,keys,minval)<0)
      return FD_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = fd_get(table,key,VOID);
      if (VOIDP(cur))
        fd_store(table,key,minval);
      else if (NUMBERP(cur)) {
        if (fd_numcompare(minval,cur)<0) {
          fd_store(table,key,minval);
          fd_decref(cur);}
        else {fd_decref(cur);}}
      else return fd_type_error("number","table_minimize",cur);}
    return VOID;}
  else return fd_type_error("table","table_minimize",table);
}

static lispval table_minimize_existing(lispval table,lispval keys,lispval minval)
{
  if (EMPTYP(minval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (!(NUMBERP(minval)))
    return fd_type_error("number","table_minimize_existing",minval);
  else if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_minimize_if_present,n_elts,elts,minval)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_minimize_if_present,keys,minval)<0)
      return FD_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = fd_get(table,key,VOID);
      if (VOIDP(cur)) {}
      else if (NUMBERP(cur)) {
        if (fd_numcompare(minval,cur)<0) {
          fd_store(table,key,minval);
          fd_decref(cur);}
        else {fd_decref(cur);}}
      else return fd_type_error("number","table_minimize_existing",cur);}
    return VOID;}
  else return fd_type_error("table","table_minimize",table);
}

static lispval hashtable_minimize(lispval table,lispval keys,lispval minval)
{
  if (EMPTYP(minval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_minimize,n_elts,elts,minval)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_minimize,keys,minval)<0)
      return FD_ERROR;
    else return VOID;
  else return fd_type_error("table","hashtable_minimize",table);
}


static lispval hashtable_minimize_existing(lispval table,lispval keys,lispval minval)
{
  if (EMPTYP(minval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = FD_CHOICE_DATA(keys);
      int n_elts = FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_minimize_if_present,n_elts,elts,minval)<0) {
        return FD_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_minimize_if_present,keys,minval)<0)
      return FD_ERROR;
    else return VOID;
  else return fd_type_error("table","hashtable_minimize_existing",table);
}

/* Getting max values out of tables, especially hashtables. */

static lispval hashtable_max(lispval table,lispval scope)
{
  if (EMPTYP(scope))
    return EMPTY;
  else return fd_hashtable_max(FD_XHASHTABLE(table),scope,NULL);
}

static lispval hashtable_skim(lispval table,lispval threshold,lispval scope)
{
  return fd_hashtable_skim(FD_XHASHTABLE(table),threshold,scope);
}

static lispval hashtable_buckets(lispval table)
{
  fd_hashtable h = fd_consptr(fd_hashtable,table,fd_hashtable_type);
  return FD_INT(h->ht_n_buckets);
}

FDPRIM("TABLE-SIZE",table_size,1,FD_NEEDS_1_ARG,
       "`(TABLE-SIZE *table*)` returns the number of keys in *table*")
  (lispval table)
{
  int size = fd_getsize(table);
  if (size<0) return FD_ERROR;
  else return FD_INT(size);
}

FDPRIM("TABLE-WRITABLE?",table_writablep,1,FD_NEEDS_1_ARG,
       "(TABLE-WRITABLE? *table*) returns true if *table* "
       "can be modified")
  (lispval table)
{
  int read_only = fd_readonlyp(table);
  if (read_only == 0)
    return FD_TRUE;
  else if (read_only > 0)
    return FD_FALSE;
  else return FD_ERROR;
}

FDPRIM("TABLE-WRITABLE!",table_set_writable,2,FD_NEEDS_1_ARG,
       "(TABLE-WRITABLE! *table* *flag*) sets the read-only status "
       "of *table*. With no *flag*, it defaults to making the table "
       "writable.")
  (lispval table,lispval flag_arg)
{
  int flag = ((FALSEP(flag_arg))||(FD_ZEROP(flag_arg)))?(1):
    (VOIDP(flag_arg))?(0):(1);
  int retval = fd_set_readonly(table,flag);
  if (retval == 0)
    return FD_FALSE;
  else if (retval > 0)
    return FD_TRUE;
  else return FD_ERROR;
}

FDPRIM("TABLE-MODIFIED?",table_modifiedp,1,FD_NEEDS_1_ARG,
       "`(TABLE-MODIFIED? *table*)` returns true if table has been"
       "modified since it was created or the last call to `TABLE-MODIFIED!`")
  (lispval table)
{
  int ismod = fd_modifiedp(table);
  if (ismod == 0)
    return FD_FALSE;
  else if (ismod > 0)
    return FD_TRUE;
  else return FD_ERROR;
}

FDPRIM("TABLE-MODIFIED!",table_set_modified,2,FD_NEEDS_1_ARG,
       "`(TABLE-MODIFIED! *table* [*flag*] )` sets or clears the modified "
       "flag of *table*. *flag* defaults to true.")
  (lispval table,lispval flag_arg)
{
  int flag = ((FALSEP(flag_arg))||(FD_ZEROP(flag_arg)))?(0):
    (VOIDP(flag_arg))?(0):(1);
  int retval = fd_set_modified(table,flag);
  if (retval == 0)
    return FD_FALSE;
  else if (retval > 0)
    return FD_TRUE;
  else return FD_ERROR;
}

/* Max/Min etc */

static lispval table_max(lispval tables,lispval scope)
{
  if (EMPTYP(scope)) return scope;
  else {
    lispval results = EMPTY;
    DO_CHOICES(table,tables)
      if (TABLEP(table)) {
        lispval result = fd_table_max(table,scope,NULL);
        CHOICE_ADD(results,result);}
      else {
        fd_decref(results);
        return fd_type_error(_("table"),"table_max",table);}
    return results;}
}

static lispval table_maxval(lispval tables,lispval scope)
{
  if (EMPTYP(scope)) return scope;
  else {
    lispval results = EMPTY;
    DO_CHOICES(table,tables)
      if (TABLEP(table)) {
        lispval maxval = EMPTY;
        lispval result = fd_table_max(table,scope,&maxval);
        CHOICE_ADD(results,maxval);
        fd_decref(result);}
      else {
        fd_decref(results);
        return fd_type_error(_("table"),"table_maxval",table);}
    return results;}
}

static lispval table_skim(lispval tables,lispval maxval,lispval scope)
{
  if (EMPTYP(scope)) return scope;
  else {
    lispval results = EMPTY;
    DO_CHOICES(table,tables)
      if (TABLEP(table)) {
        lispval result = fd_table_skim(table,maxval,scope);
        if (FD_ABORTP(result)) {
          fd_decref(results);
          return result;}
        else {CHOICE_ADD(results,result);}}
      else {
        fd_decref(results);
        return fd_type_error(_("table"),"table_skim",table);}
    return results;}
}

static lispval table_map_size(lispval table)
{
  if (TYPEP(table,fd_hashtable_type)) {
    struct FD_HASHTABLE *ht = (struct FD_HASHTABLE *) table;
    long long n_values = fd_hashtable_map_size(ht);
    return FD_INT(n_values);}
  else if (TYPEP(table,fd_hashset_type)) {
    struct FD_HASHSET *hs = (struct FD_HASHSET *) table;
    return FD_INT(hs->hs_n_elts);}
  else if (TABLEP(table)) {
    lispval keys = fd_getkeys(table);
    long long count = 0;
    DO_CHOICES(key,keys) {
      lispval v = fd_get(table,key,VOID);
      if (!(VOIDP(v))) {
        int size = FD_CHOICE_SIZE(v);
        count += size;}
      fd_decref(v);}
    fd_decref(keys);
    return FD_INT(count);}
  else return fd_type_error(_("table"),"table_map_size",table);
}

/* Mapping into tables */

static lispval map2table(lispval keys,lispval fn,lispval hashp)
{
  int n_keys = FD_CHOICE_SIZE(keys);
  lispval table;
  if (FALSEP(hashp)) table = fd_empty_slotmap();
  else if (FD_TRUEP(hashp)) table = fd_make_hashtable(NULL,n_keys*2);
  else if (FIXNUMP(hashp))
    if (n_keys>(FIX2INT(hashp))) table = fd_make_hashtable(NULL,n_keys*2);
    else table = fd_empty_slotmap();
  else if (n_keys>8) table = fd_make_hashtable(NULL,n_keys*2);
  else table = fd_empty_slotmap();
  if ((SYMBOLP(fn)) || (OIDP(fn))) {
    DO_CHOICES(k,keys) {
      lispval v = ((OIDP(k)) ? (fd_frame_get(k,fn)) : (fd_get(k,fn,EMPTY)));
      fd_add(table,k,v);
      fd_decref(v);}}
  else if (FD_APPLICABLEP(fn)) {
    DO_CHOICES(k,keys) {
      lispval v = fd_apply(fn,1,&k);
      fd_add(table,k,v);
      fd_decref(v);}}
  else if (TABLEP(fn)) {
    DO_CHOICES(k,keys) {
      lispval v = fd_get(fn,k,EMPTY);
      fd_add(table,k,v);
      fd_decref(v);}}
  else {
    fd_decref(table);
    return fd_type_error("map","map2table",fn);}
  return table;
}

/* Hashset operations */

FDPRIM("HASHSET?",hashsetp,1,FD_NEEDS_1_ARG,
       "`(HASHSET? *obj*)` returns true if *obj* is a hashset.")
  (lispval x)
{
  if (TYPEP(x,fd_hashset_type))
    return FD_TRUE;
  else return FD_FALSE;
}

FDPRIM("HASHSET-ADD!",hashset_add,2,FD_NEEDS_2_ARGS,
       "`(HASHSET-ADD! *hashset* *keys*)` adds *keys* to *hashset*(s). "
       "Returns the number of new values added.")
  (lispval hs,lispval key)
{
  if (FD_EMPTYP(hs))
    return FD_EMPTY;
  else if ( (CHOICEP(hs)) || (PRECHOICEP(hs)) ) {
    lispval results = EMPTY;
    DO_CHOICES(h,hs) {
      if (!(FD_TYPEP(hs,fd_hashset_type))) {
        FD_STOP_DO_CHOICES;
        fd_decref(results);
        return fd_type_error("hashset","hashset_add",h);}
      lispval value = hashset_add(h,key);
      CHOICE_ADD(results,value);}
    return results;}
  else if (!(FD_TYPEP(hs,fd_hashset_type)))
    return fd_type_error("hashset","hashset_add",hs);
  else {
    int retval = fd_hashset_add((fd_hashset)hs,key);
    if (retval<0) return FD_ERROR;
    else if (retval) return FD_INT(retval);
    else return FD_FALSE;}
}

FDPRIM("HASHSET+",hashset_plus,2,FD_NEEDS_2_ARGS,
       "`(HASHSET+ *hashset* *keys*)` adds *keys* to *hashset*")
  (lispval hs,lispval values)
{
  fd_hashset_add((fd_hashset)hs,values);
  fd_incref(hs);
  return  hs;
}

FDPRIM("hashset_drop",hashset_drop,2,FD_NEEDS_2_ARGS|FD_NDCALL,
       "`(HASHSET-DROP! *hashset* *keys*)` removes *key* from *hashset*.")
  (lispval hs,lispval key)
{
  int retval = fd_hashset_drop((fd_hashset)hs,key);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

FDPRIM("hashset_get",hashset_get,2,FD_NEEDS_2_ARGS|FD_NDCALL,
       "`(HASHSET-GET *hashset* *key*)` returns true if "
       "*key* is in *hashsets*")
  (lispval hs,lispval key)
{
  int retval = fd_hashset_get((fd_hashset)hs,key);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

FDPRIM("hashset_test",hashset_test,2,FD_NEEDS_2_ARGS|FD_NDCALL,
       "`(HASHSET-TEST *hashset* *keys*)` returns true if "
       "any *keys* are in any of *hashsets*")
  (lispval hs,lispval keys)
{
  DO_CHOICES(hset,hs) {
    if (!(TYPEP(hset,fd_hashset_type)))
      return fd_type_error(_("hashset"),"hashsettest",hset);}
  if ( (FD_CHOICEP(keys)) ) {
    DO_CHOICES(hset,hs) {
      DO_CHOICES(key,keys) {
        if (fd_hashset_get((fd_hashset)hs,key)) {
          FD_STOP_DO_CHOICES;
          return FD_TRUE;}}}
    return FD_FALSE;}
  else if (FD_CHOICEP(hs)) {
    DO_CHOICES(hset,hs) {
      if (fd_hashset_get((fd_hashset)hs,keys)) {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else if (fd_hashset_get((fd_hashset)hs,keys))
    return FD_TRUE;
  else return FD_FALSE;
}

FDPRIM("hashset_elts",hashset_elts,2,FD_NEEDS_1_ARG,
       "Returns the elements of a hashset.\n"
       "With a non-false second argument, resets "
       "the hashset (removing all values)")
  (lispval hs,lispval clean)
{
  if (FALSEP(clean))
    return fd_hashset_elts((fd_hashset)hs,0);
  else return fd_hashset_elts((fd_hashset)hs,1);
}

FDPRIM("reset_hashset",reset_hashset,1,FD_NEEDS_1_ARG,
       "`(reset_hashset *hashset*)` Remove all of the elements of *hashset*.")
  (lispval hs)
{
  int rv=fd_reset_hashset((fd_hashset)hs);
  if (rv<0)
    return FD_ERROR;
  else if (rv)
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval choices2hashset(int n,lispval *args)
{
  struct FD_HASHSET *h = u8_alloc(struct FD_HASHSET);
  int size = 0; int i = 0; while (i<n) {
    size += FD_CHOICE_SIZE(args[i]); i++;}
  fd_init_hashset(h,((size<17) ? (17) : (size*3)),FD_MALLOCD_CONS);
  i=0; while (i<n) {
    fd_hashset_add(h,args[i]); i++;}
  return LISP_CONS(h);
}

static lispval hashset_intern(lispval hs,lispval key)
{
  return fd_hashset_intern((fd_hashset)hs,key,1);
}

static lispval hashset_probe(lispval hs,lispval key)
{
  return fd_hashset_intern((fd_hashset)hs,key,0);
}

/* Sorting slotmaps */

static lispval sort_slotmap(lispval slotmap)
{
  if (fd_sort_slotmap(slotmap,1)<0)
    return FD_ERROR;
  else return fd_incref(slotmap);
}

/* Merging hashtables */

static int merge_kv_into_table(struct FD_KEYVAL *kv,void *data)
{
  struct FD_HASHTABLE *ht = (fd_hashtable) data;
  fd_hashtable_op_nolock(ht,fd_table_add,kv->kv_key,kv->kv_val);
  return 0;
}

FD_EXPORT lispval hashtable_merge(lispval dest,lispval src)
{
  /* Ignoring this for now */
  fd_hashtable into = (fd_hashtable) dest;
  fd_hashtable from = (fd_hashtable) src;
  fd_write_lock_table(into);
  fd_read_lock_table(from);
  fd_for_hashtable_kv(from,merge_kv_into_table,(void *)into,0);
  fd_unlock_table(from);
  fd_unlock_table(into);
  return fd_incref(dest);
}

/* Initialization code */

FD_EXPORT void fd_init_tableprims_c()
{
  u8_register_source_file(_FILEINFO);

  DEFPRIM(tablep,1,fd_scheme_module);
  DEFPRIM(haskeysp,1,fd_scheme_module);
  DEFPRIM(slotmapp,1,fd_scheme_module);
  DEFPRIM(schemapp,1,fd_scheme_module);
  DEFPRIM(hashtablep,1,fd_scheme_module);
  DEFPRIM(make_hashset,1,fd_scheme_module);
  DEFPRIM(make_hashtable,1,fd_scheme_module);

  DEFPRIM(hash_lisp_prim,1,fd_scheme_module);

  DEFPRIM(static_hashtable,1,fd_scheme_module);
  DEFPRIM(unsafe_hashtable,1,fd_scheme_module);
  DEFPRIM(resafe_hashtable,1,fd_scheme_module);

  DEFPRIM(pick_hashtable_size,1,fd_scheme_module);
  DEFPRIM(reset_hashtable,2,fd_scheme_module);

  fd_idefn1(fd_scheme_module,"SCHEMAP->SLOTMAP",schemap2slotmap_prim,1,
            "Converts a schemap into a slotmap",
            fd_schemap_type,FD_VOID);

  /* Note that GET and TEST are actually DB functions which do inference */
  DEFPRIM(lispget,3,fd_scheme_module);
  DEFPRIM(lisptest,3,fd_scheme_module);
  DEFPRIM(lispadd,3,fd_scheme_module);
  DEFPRIM(lispdrop,3,fd_scheme_module);
  DEFPRIM(lispstore,3,fd_scheme_module);
  DEFPRIM(lispgetif,3,fd_scheme_module);
  DEFPRIM(lisptryget,3,fd_scheme_module);

  fd_idefn1(fd_scheme_module,"GETKEYS",fd_getkeys,1,
            "`(GETKEYS *table*)` returns all the keys in *table*.",
            -1,FD_VOID);
  fd_idefn1(fd_scheme_module,"GETVALUES",fd_getvalues,1,
            "`(GETVALUES *table*)` returns all the values associated with "
            "all of the keys in *table*.",
            -1,FD_VOID);
  fd_idefn1(fd_scheme_module,"GETASSOCS",fd_getassocs,1,
            "`(GETASSOCS *table*)` returns (key . values) pairs for all "
            "of the keys in *table*.",
            -1,FD_VOID);

  DEFPRIM(lisp_pick_keys,2,fd_scheme_module);

  DEFPRIM(table_size,1,fd_scheme_module);

  DEFPRIM(table_writablep,1,fd_scheme_module);
  DEFPRIM(table_set_writable,2,fd_scheme_module);

  DEFPRIM(table_modifiedp,1,fd_scheme_module);
  DEFPRIM(table_set_modified,2,fd_scheme_module);

  fd_defalias(fd_scheme_module,"WRITABLE?","TABLE-WRITABLE?");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("TABLE-MAX",table_max,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("TABLE-MAXVAL",table_maxval,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-SKIM",table_skim,2)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("TABLE-MAP-SIZE",table_map_size,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1
           ("PLIST->TABLE",fd_plist_to_slotmap,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1
           ("ALIST->TABLE",fd_alist_to_slotmap,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1
           ("BLIST->TABLE",fd_blist_to_slotmap,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1
           ("BINDINGS->TABLE",fd_blist_to_slotmap,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1x
           ("SORT-SLOTMAP",sort_slotmap,1,fd_slotmap_type,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-INCREMENT!",
                                         table_increment,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-INCREMENT-EXISTING!",
                                         table_increment_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MULTIPLY!",
                                         table_multiply,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MULTIPLY-EXISTING!",
                                         table_multiply_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MAXIMIZE!",
                                         table_maximize,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MAXIMIZE-EXISTING!",
                                         table_maximize_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MINIMIZE!",
                                         table_minimize,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MINIMIZE-EXISTING!",
                                         table_minimize_existing,2)));


  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-INCREMENT!",
                                         hashtable_increment,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-INCREMENT-EXISTING!",
                                         hashtable_increment_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MULTIPLY!",
                                         hashtable_multiply,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MULTIPLY-EXISTING!",
                                         hashtable_multiply_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MAXIMIZE!",
                                         hashtable_maximize,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MAXIMIZE-EXISTING!",
                                         hashtable_maximize_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MINIMIZE!",
                                         hashtable_minimize,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MINIMIZE-EXISTING!",
                                         hashtable_minimize_existing,2)));


  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("HASHTABLE-MAX",hashtable_max,1,
                           fd_hashtable_type,VOID,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("HASHTABLE-SKIM",hashtable_skim,1,
                           fd_hashtable_type,VOID,-1,VOID,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("HASHTABLE-BUCKETS",hashtable_buckets,1,
                           fd_hashtable_type,VOID));
  fd_idefn2(fd_scheme_module,"HASHTABLE/MERGE",hashtable_merge,2,
            "`(HASHTABLE/MERGE *dest* *src*)`\n"
            "Merges one hashtable into another",
            fd_hashtable_type,VOID,fd_hashtable_type,VOID);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("MAP->TABLE",map2table,2)));


  fd_idefnN(fd_scheme_module,"CHOICE->HASHSET",choices2hashset,FD_NDCALL,
            "Returns a hashset containing all of *elts*.");

  DEFPRIM(hashsetp,1,fd_scheme_module);
  DEFPRIM(hashset_get,2,fd_scheme_module);
  DEFPRIM(hashset_test,2,fd_scheme_module);
  DEFPRIM(hashset_add,2,fd_scheme_module);
  DEFPRIM(hashset_drop,2,fd_scheme_module);

  fd_idefn2(fd_scheme_module,"HASHSET/PROBE",hashset_probe,2,
            "(HASHSET/PROBE *hashset* *key*)\n"
            "If *key* is in *hashset*, returns the exact key (pointer) "
            "stored in *hashset*, otherwise returns {}",
            fd_hashset_type,FD_VOID,-1,FD_VOID);
  fd_idefn2(fd_scheme_module,"HASHSET/INTERN",hashset_intern,2,
            "(HASHSET/INTERN *hashset* *key*)\n"
            "If *key* is in *hashset*, returns the exact key (pointer) "
            "stored in *hashset*, otherwise adds *key* to the hashset "
            "and returns it.",
            fd_hashset_type,FD_VOID,-1,FD_VOID);

  DEFPRIM(hashset_plus,2,fd_scheme_module);
  DEFPRIM(hashset_elts,2,fd_scheme_module);
  DEFPRIM(reset_hashset,1,fd_scheme_module);


}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
