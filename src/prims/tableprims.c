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

DEFPRIM("TABLE?",tablep,MAX_ARGS(1),
        "`(TABLE? *obj*)` returns #t if *obj* is a table, #f otherwise.")
  (lispval arg)
{
  if (TABLEP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

DEFPRIM("HASKEYS?",haskeysp,MAX_ARGS(1),
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

DEFPRIM("SLOTMAP?",slotmapp,MAX_ARGS(1),
        "`(SLOTMAP? *obj*)` returns #t if *obj* is a slotmap, #f otherwise.")
  (lispval x)
{
  if (SLOTMAPP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

DEFPRIM("SCHEMAP?",schemapp,MAX_ARGS(1),
        "`(SCHEMAP? *obj*)` returns #t if *obj* is a slotmap, #f otherwise.")
  (lispval x)
{
  if (SCHEMAPP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

DEFPRIM("HASHTABLE?",hashtablep,MAX_ARGS(1),
        "`(HASHTABLE? *obj*)` returns #t if *obj* is a slotmap, #f otherwise.")
  (lispval x)
{
  if (HASHTABLEP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

DEFPRIM1("MAKE-HASHSET",make_hashset,MIN_ARGS(0),
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

DEFPRIM1("MAKE-HASHTABLE",make_hashtable,MIN_ARGS(0),
         "`(MAKE-HASHTABLE [*n_buckets*])` returns a hashset. "
         "*n_buckets*, if provided indicates the number of buckets",
         fd_fixnum_type,FD_VOID)
  (lispval size)
{
  if (FD_UINTP(size))
    return fd_make_hashtable(NULL,FIX2INT(size));
  else return fd_make_hashtable(NULL,0);
}

DEFPRIM1("PICK-HASHTABLE-SIZE", pick_hashtable_size,MAX_ARGS(1),
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

DEFPRIM2("RESET-HASHTABLE!",reset_hashtable,MIN_ARGS(1),
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

DEFPRIM1("STATIC-HASHTABLE",static_hashtable,MAX_ARGS(1),
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

DEFPRIM1("UNSAFE-HASHTABLE",unsafe_hashtable,MAX_ARGS(1),
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

DEFPRIM1("RESAFE-HASHTABLE",resafe_hashtable,MAX_ARGS(1),
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

DEFPRIM1("HASH-LISP",hash_lisp_prim,0,
         "`(HASH_LISP *object* )` returns an integer hash value "
         "for *object*.",
         -1,FD_VOID)
  (lispval x)
{
  int val = fd_hash_lisp(x);
  return FD_INT(val);
}

DEFPRIM("%GET",table_get,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
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

DEFPRIM("ADD!",table_add,MAX_ARGS(3)|NDCALL,
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
DEFPRIM("DROP!",table_drop,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
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
DEFPRIM("STORE!",table_store,MAX_ARGS(3)|NDCALL,
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
DEFPRIM("%TEST",table_test,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
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


DEFPRIM_DECL("GETKEYS",fd_getkeys,MAX_ARGS(1),
             "`(GETKEYS *table*)` returns all the keys in *table*.");
DEFPRIM_DECL("GETVALUES",fd_getvalues,MAX_ARGS(1),
             "`(GETVALUES *table*)` returns all the values associated with "
             "all of the keys in *table*.");
DEFPRIM_DECL("GETASSOCS",fd_getassocs,MAX_ARGS(1),
             "`(GETASSOCS *table*)` returns (key . values) pairs for all "
             "of the keys in *table*.");

/* Converting schemaps to slotmaps */

DEFPRIM1("SCHEMAP->SLOTMAP",schemap2slotmap_prim,MAX_ARGS(1),
         "`(SCHEMAP->SLOTMAP *schemap*)` converts a schemap to a slotmap.",
         fd_schemap_type,FD_VOID)
  (lispval in)
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

DEFPRIM("TABLE-INCREMENT!",
        table_increment,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(TABLE-INCREMENT! *table* *key* [*delta*])` "
        "adds *delta* (default to 1) to the current value of *key* in "
        "*table* (which defaults to 0).")
  (lispval table,lispval keys,lispval increment)
{
  if (EMPTYP(increment)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (VOIDP(increment))
    increment = FD_INT(1);
  else if (!(NUMBERP(increment)))
    return fd_type_error("number","table_increment",increment);
  else NO_ELSE;
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

DEFPRIM("TABLE-INCREMENT-EXISTING!",
        table_increment_existing,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(TABLE-INCREMENT-EXISTING! *table* *key* [*delta*])` "
        "adds *delta* (default to 1) to the current value of *key* in "
        "*table*, doing nothing if *key* is not in *table*.")
  (lispval table,lispval keys,lispval increment)
{
  if (EMPTYP(increment)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (VOIDP(increment))
    increment = FD_INT(1);
  else if (!(NUMBERP(increment)))
    return fd_type_error("number","table_increment_existing",increment);
  else NO_ELSE;
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

DEFPRIM("TABLE-MULTIPLY!",
        table_multiply,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MULTIPLY! *table* *key* *factor*)` "
        "multiplies the current value of *key* in *table* by *factor*, "
        "defaulting the value of *key* to 1.")
  (lispval table,lispval keys,lispval factor)
{
  if (EMPTYP(factor)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (VOIDP(factor))
    factor = FD_INT(1);
  else if (!(NUMBERP(factor)))
    return fd_type_error("number","table_multiply",factor);
  else NO_ELSE;
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

DEFPRIM("TABLE-MULTIPLY-EXISTING!",
        table_multiply_existing,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MULTIPLY-EXISTING! *table* *key* *factor*)` "
        "multiplies the current value of *key* in *table* by *factor*, "
        "doing nothing if *key* is not currently defined in *table*.")
  (lispval table,lispval keys,lispval factor)
{
  if (EMPTYP(factor)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (VOIDP(factor))
    factor = FD_INT(1);
  else if (!(NUMBERP(factor)))
    return fd_type_error("number","table_multiply_existing",factor);
  else NO_ELSE;
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

DEFPRIM("TABLE-MAXIMIZE!",
        table_maximize,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MAXIMIZE! *table* *key* *value*)` "
        "stores *value* under  *key* in *table* if it is larger "
        "than the current value or if there is no current value.")
  (lispval table,lispval keys,lispval maxval)
{
  if (EMPTYP(maxval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (!(NUMBERP(maxval)))
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

DEFPRIM("TABLE-MAXIMIZE-EXISTING!",
        table_maximize_existing,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MAXIMIZE-EXISTING! *table* *key* *value*)` "
        "stores *value* under  *key* in *table* if it is larger "
        "than the current value, doing nothing if *key* is not in *table*")
  (lispval table,lispval keys,lispval maxval)
{
  if (EMPTYP(maxval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (!(NUMBERP(maxval)))
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

/* Table MINIMIZE
   Stores a value in a table if the current value is either empty or
   less than the new value. */

DEFPRIM("TABLE-MINIMIZE!",
        table_minimize,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MINIMIZE! *table* *key* *value*)` "
        "stores *value* under  *key* in *table* if it is smaller "
        "than the current value or if there is no current value.")
  (lispval table,lispval keys,lispval minval)
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

DEFPRIM("TABLE-MINIMIZE-EXISTING!",
        table_minimize_existing,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MINIMIZE-EXISTING! *table* *key* *value*)` "
        "stores *value* under  *key* in *table* if it is smaller "
        "than the current value, doing nothing if *key* is not in *table*")
  (lispval table,lispval keys,lispval minval)
{
  if (EMPTYP(minval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (!(NUMBERP(minval)))
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

/* Generic functions */

DEFPRIM("TABLE-SIZE",table_size,MAX_ARGS(1),
        "`(TABLE-SIZE *table*)` returns the number of keys in *table*")
  (lispval table)
{
  int size = fd_getsize(table);
  if (size<0) return FD_ERROR;
  else return FD_INT(size);
}

DEFPRIM("TABLE-WRITABLE?",table_writablep,MAX_ARGS(1),
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

DEFPRIM("TABLE-WRITABLE!",table_set_writable,MAX_ARGS(2)|MIN_ARGS(1),
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

DEFPRIM("TABLE-MODIFIED?",table_modifiedp,MAX_ARGS(1),
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

DEFPRIM("TABLE-MODIFIED!",table_set_modified,MAX_ARGS(2)|MIN_ARGS(1),
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

/* Getting max values out of tables, especially hashtables. */

DEFPRIM("TABLE-MAX",table_max,MAX_ARGS(2)|MIN_ARGS(1)|NDCALL,
        "`(TABLE-MAX *table* [*scope*])` returns the key(s) "
        "in *table* with the largest numeric values. If *scope* "
        "is provided, limit the operation to the keys in *scope*.")
  (lispval tables,lispval scope)
{
  if (EMPTYP(scope))
    return scope;
  else if (FD_HASHTABLEP(tables))
    return fd_hashtable_max(FD_XHASHTABLE(tables),scope,NULL);
  else {
    lispval results = EMPTY;
    DO_CHOICES(table,tables)
      if (FD_HASHTABLEP(table)) {
        lispval result = fd_hashtable_max((fd_hashtable)table,scope,NULL);
        CHOICE_ADD(results,result);}
      else if (TABLEP(table)) {
        lispval result = fd_table_max(table,scope,NULL);
        CHOICE_ADD(results,result);}
      else {
        fd_decref(results);
        return fd_type_error(_("table"),"table_max",table);}
    return results;}
}

DEFPRIM("TABLE-MAXVAL",table_maxval,MAX_ARGS(2)|MIN_ARGS(1)|NDCALL,
        "`(TABLE-MAXVAL *table* [*scope*])` returns the value "
        "in *table* with the largest numeric magnitude. If *scope* "
        "is provided, limit the operation to the values associated "
        "with keys in *scope*.")
  (lispval tables,lispval scope)
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

DEFPRIM("TABLE-SKIM",table_skim,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(TABLE-SKIM *table* *threshold* [*scope*])` returns the key(s) "
        "in *table* associated with numeric values larger than *threhsold*. "
        "If *scope* is provided, limit the operation to the keys in *scope*.")
  (lispval tables,lispval maxval,lispval scope)
{
  if (EMPTYP(scope))
    return scope;
  else if (EMPTYP(maxval))
    return maxval;
  else if (FD_HASHTABLEP(tables))
    fd_hashtable_skim(FD_XHASHTABLE(tables),maxval,scope);
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

/* Table utility functions */

DEFPRIM("MAP->TABLE",map2table,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(MAP->TABLE *keys* *fcn* [*hashp*])` returns "
        "a table store the results of applying *fn* "
        "to *keys*. The type of table is controlled by *hashp*:"
        "* without *hashp*, &gt; 8 keys generates a hashtable, otherwise a slotmap;\n"
        "* if *hashp* is #t, a hashtable is always generated;\n"
        "* if *hashp* is #f, a slotmap is always generated;\n"
        "* if *hashp* is a fixnum, it is the threshold for using a hashtable.")
  (lispval keys,lispval fn,lispval hashp)
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

DEFPRIM1("TABLE-MAP-SIZE",table_map_size,MAX_ARGS(1),
         "`(TABLE-MAP-SIZE *table*)` returns the number key/value "
         "associates in *table*.",
         fd_table_type,FD_VOID)
  (lispval table)
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

/* Hashset operations */

DEFPRIM("HASHSET?",hashsetp,MAX_ARGS(1),
        "`(HASHSET? *obj*)` returns true if *obj* is a hashset.")
  (lispval x)
{
  if (TYPEP(x,fd_hashset_type))
    return FD_TRUE;
  else return FD_FALSE;
}

DEFPRIM("HASHSET-ADD!",hashset_add,MAX_ARGS(2)|MIN_ARGS(2)|NDCALL,
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
    if (retval<0)
      return FD_ERROR;
    else if (retval)
      return FD_INT(retval);
    else return FD_FALSE;}
}

DEFPRIM("HASHSET+",hashset_plus,MAX_ARGS(2)|MIN_ARGS(2),
        "`(HASHSET+ *hashset* *keys*)` adds *keys* to *hashset*")
  (lispval hs,lispval values)
{
  fd_hashset_add((fd_hashset)hs,values);
  fd_incref(hs);
  return  hs;
}

DEFPRIM("HASHSET-DROP!",hashset_drop,MAX_ARGS(2)|MIN_ARGS(2),
        "`(HASHSET-DROP! *hashset* *keys*)` removes *key* from *hashset*.")
  (lispval hs,lispval key)
{
  int retval = fd_hashset_drop((fd_hashset)hs,key);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

DEFPRIM("HASHSET-GET",hashset_get,MAX_ARGS(2)|MIN_ARGS(2),
        "`(HASHSET-GET *hashset* *key*)` returns true if "
        "*key* is in *hashsets*")
  (lispval hs,lispval key)
{
  int retval = fd_hashset_get((fd_hashset)hs,key);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

DEFPRIM("HASHSET-TEST",hashset_test,
        MAX_ARGS(2)|MIN_ARGS(2)|NDCALL,
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

DEFPRIM("HASHSET-ELTS",hashset_elts,MAX_ARGS(2)|MIN_ARGS(1),
        "Returns the elements of a hashset.\n"
        "With a non-false second argument, resets "
        "the hashset (removing all values).")
  (lispval hs,lispval clean)
{
  if (FALSEP(clean))
    return fd_hashset_elts((fd_hashset)hs,0);
  else return fd_hashset_elts((fd_hashset)hs,1);
}

DEFPRIM("RESET-HASHSET!",reset_hashset,MAX_ARGS(1),
        "`(RESET_HASHSET! *hashset*)` removes all of the elements of *hashset*.")
  (lispval hs)
{
  int rv=fd_reset_hashset((fd_hashset)hs);
  if (rv<0)
    return FD_ERROR;
  else if (rv)
    return FD_TRUE;
  else return FD_FALSE;
}

DEFPRIM("CHOICE->HASHSET",choices2hashset,FD_N_ARGS|MIN_ARGS(0),
        "`(CHOICE->HASHSET choices...)` returns a hashset combining "
        "mutiple choices.")
  (int n,lispval *args)
{
  struct FD_HASHSET *h = u8_alloc(struct FD_HASHSET);
  int size = 0; int i = 0; while (i<n) {
    size += FD_CHOICE_SIZE(args[i]); i++;}
  fd_init_hashset(h,((size<17) ? (17) : (size*3)),FD_MALLOCD_CONS);
  i=0; while (i<n) {
    fd_hashset_add(h,args[i]); i++;}
  return LISP_CONS(h);
}

DEFPRIM2("HASHSET/INTERN",hashset_intern,MIN_ARGS(2),
         "(HASHSET/INTERN *hashset* *key*)\n"
         "If *key* is in *hashset*, returns the exact key (pointer) "
         "stored in *hashset*, otherwise adds *key* to the hashset "
         "and returns it.",
         fd_hashset_type,FD_VOID,-1,FD_VOID)
  (lispval hs,lispval key)
{
  return fd_hashset_intern((fd_hashset)hs,key,1);
}

DEFPRIM2("HASHSET/PROBE",hashset_probe,MIN_ARGS(2),
         "(HASHSET/PROBE *hashset* *key*)\n"
         "If *key* is in *hashset*, returns the exact key (pointer) "
         "stored in *hashset*, otherwise returns {}",
         fd_hashset_type,FD_VOID,-1,FD_VOID)
  (lispval hs,lispval key)
{
  return fd_hashset_intern((fd_hashset)hs,key,0);
}

/* Sorting slotmaps */

DEFPRIM1("SORT-SLOTMAP",sort_slotmap,MAX_ARGS(1),
         "`(SORT-SLOTMAP *slotmap*)` sorts the keys in *slotmap* "
         "for improved performance",
         fd_slotmap_type,FD_VOID)
  (lispval slotmap)
{
  if (fd_sort_slotmap(slotmap,1)<0)
    return FD_ERROR;
  else return fd_incref(slotmap);
}

/* Merging hashtables */

DEFPRIM1("HASHTABLE-BUCKETS",hashtable_buckets,MAX_ARGS(1),
         "`(HASHTABLE-BUCKETS *hashtable*)` returns the number of buckets "
         "allocated for *hashtable*.",
         fd_hashtable_type,FD_VOID)
  (lispval table)
{
  fd_hashtable h = fd_consptr(fd_hashtable,table,fd_hashtable_type);
  return FD_INT(h->ht_n_buckets);
}

static int merge_kv_into_table(struct FD_KEYVAL *kv,void *data)
{
  struct FD_HASHTABLE *ht = (fd_hashtable) data;
  fd_hashtable_op_nolock(ht,fd_table_add,kv->kv_key,kv->kv_val);
  return 0;
}

DEFPRIM2("HASHTABLE/MERGE",hashtable_merge,MIN_ARGS(2),
         "`(HASHTABLE/MERGE *dest* *src*)`\n"
         "Merges one hashtable into another",
         fd_hashtable_type,VOID,fd_hashtable_type,VOID)
  (lispval dest,lispval src)
{
  fd_hashtable into = (fd_hashtable) dest;
  fd_hashtable from = (fd_hashtable) src;
  fd_write_lock_table(into);
  fd_read_lock_table(from);
  fd_for_hashtable_kv(from,merge_kv_into_table,(void *)into,0);
  fd_unlock_table(from);
  fd_unlock_table(into);
  return fd_incref(dest);
}

DEFPRIM_DECL("PLIST->TABLE",fd_plist_to_slotmap,MAX_ARGS(1),
             "`(PLIST->TABLE *plist*)` returns a slotmap from a plist "
             "(property list) of the form "
             "(key1 value1 key2 value2 ... )");
DEFPRIM_DECL("ALIST->TABLE",fd_alist_to_slotmap,MAX_ARGS(1),
             "`(ALIST->TABLE *list*)` returns a slotmap from an alist "
             "(association list) of the form "
             "((key1 . value1) (key2 . value2) ... )");
DEFPRIM_DECL("BLIST->TABLE",fd_blist_to_slotmap,MAX_ARGS(1),
             "`(BLIST->TABLE *list*)` returns a slotmap from a blist "
             "(binding list) of the form "
             "((key1  value1) (key2 value2) ... )");

/* Initialization code */

FD_EXPORT void fd_init_tableprims_c()
{
  u8_register_source_file(_FILEINFO);

  DECL_PRIM(tablep,1,fd_scheme_module);
  DECL_PRIM(haskeysp,1,fd_scheme_module);
  DECL_PRIM(slotmapp,1,fd_scheme_module);
  DECL_PRIM(schemapp,1,fd_scheme_module);
  DECL_PRIM(hashtablep,1,fd_scheme_module);
  DECL_PRIM(make_hashset,1,fd_scheme_module);
  DECL_PRIM(make_hashtable,1,fd_scheme_module);

  DECL_PRIM(hash_lisp_prim,1,fd_scheme_module);

  DECL_PRIM(static_hashtable,1,fd_scheme_module);
  DECL_PRIM(unsafe_hashtable,1,fd_scheme_module);
  DECL_PRIM(resafe_hashtable,1,fd_scheme_module);

  DECL_PRIM(pick_hashtable_size,1,fd_scheme_module);
  DECL_PRIM(reset_hashtable,2,fd_scheme_module);

  DECL_PRIM(schemap2slotmap_prim,1,fd_scheme_module);

  /* Note that GET and TEST are actually DB functions which do inference */
  DECL_PRIM(table_get,3,fd_scheme_module);
  DECL_PRIM(table_test,3,fd_scheme_module);
  DECL_PRIM(table_add,3,fd_scheme_module);
  DECL_PRIM(table_drop,3,fd_scheme_module);
  DECL_PRIM(table_store,3,fd_scheme_module);

  DECL_PRIM(fd_getkeys,1,fd_scheme_module);
  DECL_PRIM(fd_getvalues,1,fd_scheme_module);
  DECL_PRIM(fd_getassocs,1,fd_scheme_module);

  DECL_PRIM(table_size,1,fd_scheme_module);

  DECL_PRIM(table_writablep,1,fd_scheme_module);
  DECL_PRIM(table_set_writable,2,fd_scheme_module);

  DECL_PRIM(table_modifiedp,1,fd_scheme_module);
  DECL_PRIM(table_set_modified,2,fd_scheme_module);

  DECL_ALIAS("WRITABLE?",table_writablep,fd_scheme_module);

  DECL_PRIM(table_max,2,fd_scheme_module);
  DECL_PRIM(table_maxval,2,fd_scheme_module);
  DECL_PRIM(table_skim,3,fd_scheme_module);

  DECL_PRIM(map2table,3,fd_scheme_module);
  DECL_PRIM(table_map_size,1,fd_scheme_module);

  DECL_PRIM(table_increment,3,fd_scheme_module);
  DECL_PRIM(table_increment_existing,3,fd_scheme_module);
  DECL_PRIM(table_multiply,3,fd_scheme_module);
  DECL_PRIM(table_multiply_existing,3,fd_scheme_module);
  DECL_PRIM(table_maximize,3,fd_scheme_module);
  DECL_PRIM(table_maximize_existing,3,fd_scheme_module);
  DECL_PRIM(table_minimize,3,fd_scheme_module);
  DECL_PRIM(table_minimize_existing,3,fd_scheme_module);

  DECL_ALIAS("HASHTABLE-MAX",table_max,fd_scheme_module);
  DECL_ALIAS("HASHTABLE-MAXVAL",table_maxval,fd_scheme_module);
  DECL_ALIAS("HASHTABLE-SKIM",table_skim,fd_scheme_module);

  DECL_ALIAS("HASHTABLE-INCREMENT!",table_increment,fd_scheme_module);
  DECL_ALIAS("HASHTABLE-INCREMENT-EXISTING!",table_increment_existing,
             fd_scheme_module);
  DECL_ALIAS("HASHTABLE-MULTIPLY!",table_multiply,fd_scheme_module);
  DECL_ALIAS("HASHTABLE-MULTIPLY-EXISTING!",table_multiply_existing,
             fd_scheme_module);
  DECL_ALIAS("HASHTABLE-MAXIMIZE!",table_maximize,fd_scheme_module);
  DECL_ALIAS("HASHTABLE-MAXIMIZE-EXISTING!",table_maximize_existing,
             fd_scheme_module);
  DECL_ALIAS("HASHTABLE-MINIMIZE!",table_minimize,fd_scheme_module);
  DECL_ALIAS("HASHTABLE-MINIMIZE-EXISTING!",table_minimize_existing,
             fd_scheme_module);

  DECL_PRIM(hashtable_buckets,1,fd_scheme_module);
  DECL_PRIM(hashtable_merge,2,fd_scheme_module);

  /* Hashset primitives */

  DECL_PRIM_N(choices2hashset,fd_scheme_module);

  DECL_PRIM(hashsetp,1,fd_scheme_module);
  DECL_PRIM(hashset_get,2,fd_scheme_module);
  DECL_PRIM(hashset_test,2,fd_scheme_module);
  DECL_PRIM(hashset_add,2,fd_scheme_module);
  DECL_PRIM(hashset_drop,2,fd_scheme_module);

  DECL_PRIM(hashset_probe,2,fd_scheme_module);
  DECL_PRIM(hashset_intern,2,fd_scheme_module);

  DECL_PRIM(hashset_plus,2,fd_scheme_module);
  DECL_PRIM(hashset_elts,2,fd_scheme_module);
  DECL_PRIM(reset_hashset,1,fd_scheme_module);

  DECL_PRIM(sort_slotmap,1,fd_scheme_module);

  /* Various list to table conversion functions */

  DECL_PRIM(fd_plist_to_slotmap,1,fd_scheme_module);
  DECL_PRIM(fd_alist_to_slotmap,1,fd_scheme_module);
  DECL_PRIM(fd_blist_to_slotmap,1,fd_scheme_module);
  DECL_ALIAS("BINDINGS->SLOTMAP",fd_blist_to_slotmap,fd_scheme_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
