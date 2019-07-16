/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_CHOICES (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_TABLES (!(KNO_AVOID_CHOICES))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/cprims.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include <kno/cprims.h>


KNO_DCLPRIM("TABLE?",tablep,MAX_ARGS(1),
        "`(TABLE? *obj*)` returns #t if *obj* is a table, #f otherwise.")
static lispval tablep(lispval arg)
{
  if (TABLEP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM("HASKEYS?",haskeysp,MAX_ARGS(1),
        "`(HASKEYS? *obj*)` returns #t if *obj* is a non-empty table, "
        "#f otherwise.")
static lispval haskeysp(lispval arg)
{
  if (TABLEP(arg)) {
    kno_lisp_type argtype = KNO_LISP_TYPE(arg);
    if ((kno_tablefns[argtype])->keys)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

KNO_DCLPRIM("SLOTMAP?",slotmapp,MAX_ARGS(1),
        "`(SLOTMAP? *obj*)` returns #t if *obj* is a slotmap, #f otherwise.")
static lispval slotmapp(lispval x)
{
  if (SLOTMAPP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM("SCHEMAP?",schemapp,MAX_ARGS(1),
        "`(SCHEMAP? *obj*)` returns #t if *obj* is a slotmap, #f otherwise.")
static lispval schemapp(lispval x)
{
  if (SCHEMAPP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM("HASHTABLE?",hashtablep,MAX_ARGS(1),
        "`(HASHTABLE? *obj*)` returns #t if *obj* is a slotmap, #f otherwise.")
static lispval hashtablep(lispval x)
{
  if (HASHTABLEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM1("MAKE-HASHSET",make_hashset,MIN_ARGS(0),
         "`(MAKE-HASHSET [*n_buckets*])` returns a hashset. "
         "*n_buckets*, if provided indicates the number of buckets",
         kno_fixnum_type,KNO_VOID)
static lispval make_hashset(lispval arg)
{
  struct KNO_HASHSET *h = u8_alloc(struct KNO_HASHSET);
  if (VOIDP(arg))
    kno_init_hashset(h,17,KNO_MALLOCD_CONS);
  else if (KNO_UINTP(arg))
    kno_init_hashset(h,FIX2INT(arg),KNO_MALLOCD_CONS);
  else return kno_type_error("uint","make_hashset",arg);
  KNO_INIT_CONS(h,kno_hashset_type);
  return LISP_CONS(h);
}

KNO_DCLPRIM1("MAKE-HASHTABLE",make_hashtable,MIN_ARGS(0),
         "`(MAKE-HASHTABLE [*n_buckets*])` returns a hashset. "
         "*n_buckets*, if provided indicates the number of buckets",
         kno_fixnum_type,KNO_VOID)
static lispval make_hashtable(lispval size)
{
  if (KNO_UINTP(size))
    return kno_make_hashtable(NULL,FIX2INT(size));
  else return kno_make_hashtable(NULL,0);
}

KNO_DCLPRIM1("PICK-HASHTABLE-SIZE", pick_hashtable_size,MAX_ARGS(1),
         "`(PICK-HASHTABLE-SIZE *count*)` picks a good hashtable size "
         "for a table of *count* elements.",
         kno_fixnum_type,KNO_VOID)
static lispval pick_hashtable_size(lispval count_arg)
{
  if (!(KNO_UINTP(count_arg)))
    return kno_type_error("uint","pick_hashtable_size",count_arg);
  int count = FIX2INT(count_arg);
  int size = kno_get_hashtable_size(count);
  return KNO_INT(size);
}

KNO_DCLPRIM2("RESET-HASHTABLE!",reset_hashtable,MIN_ARGS(1),
         "`(RESET-HASHTABLE! *table* [*slots*)` resets a hashtable, removing "
         "all of its values",
         kno_hashtable_type,KNO_VOID,kno_fixnum_type,KNO_VOID)
static lispval reset_hashtable(lispval table,lispval n_slots)
{
  if (!(KNO_UINTP(n_slots)))
    return kno_type_error("uint","reset_hashtable",n_slots);
  kno_reset_hashtable((kno_hashtable)table,FIX2INT(n_slots),1);
  return VOID;
}

KNO_DCLPRIM1("STATIC-HASHTABLE",static_hashtable,MAX_ARGS(1),
         "`(STATIC-HASHTABLE *table* )` declares all keys and values "
         "in *table* to be static (no GC) and disables locking for the table. "
         "This may improve performance on objects in the table",
         kno_hashtable_type,KNO_VOID)
static lispval static_hashtable(lispval table)
{
  struct KNO_HASHTABLE *ht = (kno_hashtable)table;
  kno_write_lock_table(ht);
  kno_static_hashtable(ht,-1);
  ht->table_uselock = 0;
  kno_unlock_table(ht);
  return kno_incref(table);
}

KNO_DCLPRIM1("UNSAFE-HASHTABLE",unsafe_hashtable,MAX_ARGS(1),
         "`(UNSAFE-TABLE *table* )` disables locking for *table*. "
         "This may improve performance.",
         kno_hashtable_type,KNO_VOID)
static lispval unsafe_hashtable(lispval table)
{
  struct KNO_HASHTABLE *ht = (kno_hashtable)table;
  kno_write_lock_table(ht);
  ht->table_uselock = 0;
  kno_unlock_table(ht);
  return kno_incref(table);
}

KNO_DCLPRIM1("RESAFE-HASHTABLE",resafe_hashtable,MAX_ARGS(1),
         "`(RESAFE-TABLE *table* )` re-enables locking for *table* "
         "disabled by `UNSAFE-TABLE`.",
         kno_hashtable_type,KNO_VOID)
static lispval resafe_hashtable(lispval table)
{
  struct KNO_HASHTABLE *ht = (kno_hashtable)table;
  kno_write_lock_table(ht);
  ht->table_uselock = 1;
  kno_unlock_table(ht);
  return kno_incref(table);
}

KNO_DCLPRIM1("HASH-LISP",hash_lisp_prim,0,
         "`(HASH_LISP *object* )` returns an integer hash value "
         "for *object*.",
         -1,KNO_VOID)
static lispval hash_lisp_prim(lispval x)
{
  int val = kno_hash_lisp(x);
  return KNO_INT(val);
}

KNO_DCLPRIM("%GET",table_get,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(%GET *table* *key* [*default*])` returns the value "
        "of *key* in *table* or *default* if *table* does not contain "
        "*key*. *default* defaults to the empty choice {}."
        "Note that this does no inference, use GET to enable inference.")
static lispval table_get(lispval table,lispval key,lispval dflt)
{
  if (VOIDP(dflt))
    return kno_get(table,key,EMPTY);
  else return kno_get(table,key,dflt);
}

KNO_DCLPRIM("ADD!",table_add,MAX_ARGS(3)|NDCALL,
        "`(ADD! *table* *key* *value*)` adds *value* to "
        "the associations of *key* in *table*. "
        "Note that this does no inference, use ASSERT! to enable inference.")
static lispval table_add(lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return VOID;
  else if (EMPTYP(key)) return VOID;
  else if (kno_add(table,key,val)<0)
    return KNO_ERROR;
  else return VOID;
}
KNO_DCLPRIM("DROP!",table_drop,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(DROP! *table* *key* [*values*])` removes *values* "
        "from *key* of *table*. If *values* is not provided, "
        "all values associated with *key* are removed. "
        "Note that this does no inference, use RETRACT! to enable inference.")
static lispval table_drop(lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return VOID;
  if (EMPTYP(key)) return VOID;
  else if (kno_drop(table,key,val)<0) return KNO_ERROR;
  else return VOID;
}
KNO_DCLPRIM("STORE!",table_store,MAX_ARGS(3)|NDCALL,
        "`(STORE! *table* *key* *value*)` stores *value* in "
        "*table* under *key*, removing all existing values. If "
        "*value* is a choice, the entire choice is stored under "
        "*key*. Note that this does no inference.")
static lispval table_store(lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return VOID;
  else if (EMPTYP(key)) return VOID;
  else if (QCHOICEP(val)) {
    struct KNO_QCHOICE *qch = KNO_XQCHOICE(val);
    if (kno_store(table,key,qch->qchoiceval)<0)
      return KNO_ERROR;
    else return VOID;}
  else if (kno_store(table,key,val)<0)
    return KNO_ERROR;
  else return VOID;
}
KNO_DCLPRIM("%TEST",table_test,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(%TEST *tables* *keys* [*values*])` returns true if "
        "any of *values* is stored undery any of *keys* in "
        "any of *tables*. If *values* is not provided, returns true "
        "if any values are stored under any of *keys* in any of *tables*. "
        "Note that this does no inference.")
static lispval table_test(lispval table,lispval key,lispval val)
{
  if (EMPTYP(table)) return KNO_FALSE;
  else if (EMPTYP(key)) return KNO_FALSE;
  else if (EMPTYP(val)) return KNO_FALSE;
  else {
    int retval = kno_test(table,key,val);
    if (retval<0) return KNO_ERROR;
    else if (retval) return KNO_TRUE;
    else return KNO_FALSE;}
}


DEFPRIM_DECL("GETKEYS",kno_getkeys,MAX_ARGS(1),
             "`(GETKEYS *table*)` returns all the keys in *table*.");
DEFPRIM_DECL("GETVALUES",kno_getvalues,MAX_ARGS(1),
             "`(GETVALUES *table*)` returns all the values associated with "
             "all of the keys in *table*.");
DEFPRIM_DECL("GETASSOCS",kno_getassocs,MAX_ARGS(1),
             "`(GETASSOCS *table*)` returns (key . values) pairs for all "
             "of the keys in *table*.");

/* Converting schemaps to slotmaps */

KNO_DCLPRIM1("SCHEMAP->SLOTMAP",schemap2slotmap_prim,MAX_ARGS(1),
         "`(SCHEMAP->SLOTMAP *schemap*)` converts a schemap to a slotmap.",
         kno_schemap_type,KNO_VOID)
static lispval schemap2slotmap_prim(lispval in)
{
  struct KNO_SCHEMAP *schemap = (kno_schemap) in;
  lispval *schema = schemap->table_schema;
  lispval *values = schemap->schema_values;
  int size = schemap->schema_length;
  struct KNO_KEYVAL kv[size];
  int i = 0; while (i < size) {
    lispval key = schema[i]; kno_incref(key);
    lispval val = values[i]; kno_incref(val);
    kv[i].kv_key = key;
    kv[i].kv_val = val;
    i++;}
  return kno_make_slotmap(size,size,kv);
}

KNO_DCLPRIM1("SLOTMAP->SCHEMAP",slotmap2schemap_prim,MAX_ARGS(1),
         "`(SLOTMAP->SCHEMAP *slotmap*)` converts a schemap to a slotmap.",
         kno_schemap_type,KNO_VOID)
static lispval slotmap2schemap_prim(lispval map)
{
  struct KNO_SLOTMAP *slotmap = (kno_slotmap) map;
  lispval result = kno_init_schemap(NULL,slotmap->n_slots,slotmap->sm_keyvals);
  struct KNO_SCHEMAP *schemap = (kno_schemap) result;
  kno_incref_vec(schemap->table_schema,schemap->schema_length);
  kno_incref_vec(schemap->schema_values,schemap->schema_length);
  return result;
}

KNO_DCLPRIM1("->SCHEMAP",table2schemap_prim,MAX_ARGS(1)|NDCALL,
         "`(->SCHEMAP *assocs*)` converts a slotmap to a schemap.",
         -1,KNO_VOID)
static lispval table2schemap_prim(lispval tbl)
{
  if (KNO_TYPEP(tbl,kno_schemap_type))
    return kno_incref(tbl);
  else if (KNO_TYPEP(tbl,kno_slotmap_type))
    return slotmap2schemap_prim(tbl);
  lispval assocs = kno_getassocs(tbl);
  lispval keys = KNO_EMPTY;
  {KNO_DO_CHOICES(assoc,assocs) {
      if (KNO_PAIRP(assoc)) {
        lispval key = KNO_CAR(assoc); kno_incref(key);
        KNO_ADD_TO_CHOICE(keys,key);}}}
  int n = KNO_CHOICE_SIZE(keys);
  lispval *schema = u8_alloc_n(n,lispval);
  lispval *values = u8_alloc_n(n,lispval);
  {int i = 0; KNO_DO_CHOICES(key,keys) {
      lispval val = kno_get(assocs,key,KNO_VOID);
      if (KNO_VOIDP(val))
        values[i] = KNO_EMPTY;
      else values[i] = val;
      schema[i] = kno_incref(key);
      i++;}}
  kno_decref(assocs);
  kno_decref(keys);
  struct KNO_SCHEMAP *schemap = u8_alloc(struct KNO_SCHEMAP);
  return kno_make_schemap(schemap,n,0,schema,values);
}

/* Support for some iterated operations */

typedef lispval (*reduceop)(lispval,lispval);

/* Various table operations */

KNO_DCLPRIM("TABLE-INCREMENT!",
        table_increment,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(TABLE-INCREMENT! *table* *key* [*delta*])` "
        "adds *delta* (default to 1) to the current value of *key* in "
        "*table* (which defaults to 0).")
static lispval table_increment(lispval table,lispval keys,lispval increment)
{
  if (EMPTYP(increment)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (VOIDP(increment))
    increment = KNO_INT(1);
  else if (!(NUMBERP(increment)))
    return kno_type_error("number","table_increment",increment);
  else NO_ELSE;
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = KNO_CHOICE_DATA(keys);
      int n_elts = KNO_CHOICE_SIZE(keys);
      if (kno_hashtable_iterkeys
          (KNO_XHASHTABLE(table),kno_table_increment,n_elts,elts,increment)<0) {
        return KNO_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (kno_hashtable_op(KNO_XHASHTABLE(table),kno_table_increment,keys,
                             increment)<0)
      return KNO_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = kno_get(table,key,VOID);
      if (VOIDP(cur))
        kno_store(table,key,increment);
      else if ((FIXNUMP(cur)) && (FIXNUMP(increment))) {
        long long sum = FIX2INT(cur)+FIX2INT(increment);
        lispval lsum = KNO_INT(sum);
        kno_store(table,key,lsum);
        kno_decref(lsum);}
      else if (NUMBERP(cur)) {
        lispval lsum = kno_plus(cur,increment);
        kno_store(table,key,lsum);
        kno_decref(lsum); kno_decref(cur);}
      else return kno_type_error("number","table_increment",cur);}
    return VOID;}
  else return kno_type_error("table","table_increment",table);
}

KNO_DCLPRIM("TABLE-INCREMENT-EXISTING!",
        table_increment_existing,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(TABLE-INCREMENT-EXISTING! *table* *key* [*delta*])` "
        "adds *delta* (default to 1) to the current value of *key* in "
        "*table*, doing nothing if *key* is not in *table*.")
static lispval table_increment_existing(lispval table,lispval keys,lispval increment)
{
  if (EMPTYP(increment)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (VOIDP(increment))
    increment = KNO_INT(1);
  else if (!(NUMBERP(increment)))
    return kno_type_error("number","table_increment_existing",increment);
  else NO_ELSE;
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = KNO_CHOICE_DATA(keys);
      int n_elts = KNO_CHOICE_SIZE(keys);
      if (kno_hashtable_iterkeys
          (KNO_XHASHTABLE(table),kno_table_increment,n_elts,elts,increment)<0) {
        return KNO_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (kno_hashtable_op(KNO_XHASHTABLE(table),kno_table_increment,keys,
                             increment)<0)
      return KNO_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = kno_get(table,key,VOID);
      if (VOIDP(cur)) {}
      else if ((FIXNUMP(cur)) && (FIXNUMP(increment))) {
        long long sum = FIX2INT(cur)+FIX2INT(increment);
        lispval lsum = KNO_INT(sum);
        kno_store(table,key,lsum);
        kno_decref(lsum);}
      else if (NUMBERP(cur)) {
        lispval lsum = kno_plus(cur,increment);
        kno_store(table,key,lsum);
        kno_decref(lsum); kno_decref(cur);}
      else return kno_type_error("number","table_increment_existing",cur);}
    return VOID;}
  else return kno_type_error("table","table_increment_existing",table);
}

KNO_DCLPRIM("TABLE-MULTIPLY!",
        table_multiply,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MULTIPLY! *table* *key* *factor*)` "
        "multiplies the current value of *key* in *table* by *factor*, "
        "defaulting the value of *key* to 1.")
static lispval table_multiply(lispval table,lispval keys,lispval factor)
{
  if (EMPTYP(factor)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (VOIDP(factor))
    factor = KNO_INT(1);
  else if (!(NUMBERP(factor)))
    return kno_type_error("number","table_multiply",factor);
  else NO_ELSE;
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = KNO_CHOICE_DATA(keys);
      int n_elts = KNO_CHOICE_SIZE(keys);
      if (kno_hashtable_iterkeys
          (KNO_XHASHTABLE(table),kno_table_multiply,n_elts,elts,factor)<0) {
        return KNO_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (kno_hashtable_op(KNO_XHASHTABLE(table),kno_table_multiply,keys,factor)<0)
      return KNO_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = kno_get(table,key,VOID);
      if (VOIDP(cur))
        kno_store(table,key,factor);
      else if (NUMBERP(cur)) {
        lispval lsum = kno_multiply(cur,factor);
        kno_store(table,key,lsum);
        kno_decref(lsum); kno_decref(cur);}
      else return kno_type_error("number","table_multiply",cur);}
    return VOID;}
  else return kno_type_error("table","table_multiply",table);
}

KNO_DCLPRIM("TABLE-MULTIPLY-EXISTING!",
        table_multiply_existing,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MULTIPLY-EXISTING! *table* *key* *factor*)` "
        "multiplies the current value of *key* in *table* by *factor*, "
        "doing nothing if *key* is not currently defined in *table*.")
static lispval table_multiply_existing(lispval table,lispval keys,lispval factor)
{
  if (EMPTYP(factor)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (VOIDP(factor))
    factor = KNO_INT(1);
  else if (!(NUMBERP(factor)))
    return kno_type_error("number","table_multiply_existing",factor);
  else NO_ELSE;
  if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = KNO_CHOICE_DATA(keys);
      int n_elts = KNO_CHOICE_SIZE(keys);
      if (kno_hashtable_iterkeys
          (KNO_XHASHTABLE(table),kno_table_multiply_if_present,n_elts,elts,factor)<0) {
        return KNO_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (kno_hashtable_op(KNO_XHASHTABLE(table),kno_table_multiply_if_present,keys,factor)<0)
      return KNO_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = kno_get(table,key,VOID);
      if (VOIDP(cur)) {}
      else if (NUMBERP(cur)) {
        lispval lsum = kno_multiply(cur,factor);
        kno_store(table,key,lsum);
        kno_decref(lsum); kno_decref(cur);}
      else return kno_type_error("number","table_multiply_existing",cur);}
    return VOID;}
  else return kno_type_error("table","table_multiply_existing",table);
}

/* Table MAXIMIZE
   Stores a value in a table if the current value is either empty or
   less than the new value. */

KNO_DCLPRIM("TABLE-MAXIMIZE!",
        table_maximize,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MAXIMIZE! *table* *key* *value*)` "
        "stores *value* under  *key* in *table* if it is larger "
        "than the current value or if there is no current value.")
static lispval table_maximize(lispval table,lispval keys,lispval maxval)
{
  if (EMPTYP(maxval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (!(NUMBERP(maxval)))
    return kno_type_error("number","table_maximize",maxval);
  else if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = KNO_CHOICE_DATA(keys);
      int n_elts = KNO_CHOICE_SIZE(keys);
      if (kno_hashtable_iterkeys
          (KNO_XHASHTABLE(table),kno_table_maximize,n_elts,elts,maxval)<0) {
        return KNO_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (kno_hashtable_op(KNO_XHASHTABLE(table),kno_table_maximize,keys,maxval)<0)
      return KNO_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = kno_get(table,key,VOID);
      if (VOIDP(cur))
        kno_store(table,key,maxval);
      else if (NUMBERP(cur)) {
        if (kno_numcompare(maxval,cur)>0) {
          kno_store(table,key,maxval);
          kno_decref(cur);}
        else {kno_decref(cur);}}
      else return kno_type_error("number","table_maximize",cur);}
    return VOID;}
  else return kno_type_error("table","table_maximize",table);
}

KNO_DCLPRIM("TABLE-MAXIMIZE-EXISTING!",
        table_maximize_existing,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MAXIMIZE-EXISTING! *table* *key* *value*)` "
        "stores *value* under  *key* in *table* if it is larger "
        "than the current value, doing nothing if *key* is not in *table*")
static lispval table_maximize_existing(lispval table,lispval keys,lispval maxval)
{
  if (EMPTYP(maxval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (!(NUMBERP(maxval)))
    return kno_type_error("number","table_maximize_existing",maxval);
  else if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = KNO_CHOICE_DATA(keys);
      int n_elts = KNO_CHOICE_SIZE(keys);
      if (kno_hashtable_iterkeys
          (KNO_XHASHTABLE(table),kno_table_maximize_if_present,n_elts,elts,maxval)<0) {
        return KNO_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (kno_hashtable_op(KNO_XHASHTABLE(table),kno_table_maximize_if_present,keys,maxval)<0)
      return KNO_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = kno_get(table,key,VOID);
      if (VOIDP(cur)) {}
      else if (NUMBERP(cur)) {
        if (kno_numcompare(maxval,cur)>0) {
          kno_store(table,key,maxval);
          kno_decref(cur);}
        else {kno_decref(cur);}}
      else return kno_type_error("number","table_maximize",cur);}
    return VOID;}
  else return kno_type_error("table","table_maximize",table);
}

/* Table MINIMIZE
   Stores a value in a table if the current value is either empty or
   less than the new value. */

KNO_DCLPRIM("TABLE-MINIMIZE!",
        table_minimize,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MINIMIZE! *table* *key* *value*)` "
        "stores *value* under  *key* in *table* if it is smaller "
        "than the current value or if there is no current value.")
static lispval table_minimize(lispval table,lispval keys,lispval minval)
{
  if (EMPTYP(minval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else {}
  if (!(NUMBERP(minval)))
    return kno_type_error("number","table_minimize",minval);
  else if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = KNO_CHOICE_DATA(keys);
      int n_elts = KNO_CHOICE_SIZE(keys);
      if (kno_hashtable_iterkeys
          (KNO_XHASHTABLE(table),kno_table_minimize,n_elts,elts,minval)<0) {
        return KNO_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (kno_hashtable_op(KNO_XHASHTABLE(table),kno_table_minimize,keys,minval)<0)
      return KNO_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = kno_get(table,key,VOID);
      if (VOIDP(cur))
        kno_store(table,key,minval);
      else if (NUMBERP(cur)) {
        if (kno_numcompare(minval,cur)<0) {
          kno_store(table,key,minval);
          kno_decref(cur);}
        else {kno_decref(cur);}}
      else return kno_type_error("number","table_minimize",cur);}
    return VOID;}
  else return kno_type_error("table","table_minimize",table);
}

KNO_DCLPRIM("TABLE-MINIMIZE-EXISTING!",
        table_minimize_existing,MAX_ARGS(3)|NDCALL,
        "`(TABLE-MINIMIZE-EXISTING! *table* *key* *value*)` "
        "stores *value* under  *key* in *table* if it is smaller "
        "than the current value, doing nothing if *key* is not in *table*")
static lispval table_minimize_existing(lispval table,lispval keys,lispval minval)
{
  if (EMPTYP(minval)) return VOID;
  else if (EMPTYP(keys)) return VOID;
  else if (EMPTYP(table)) return VOID;
  else if (!(NUMBERP(minval)))
    return kno_type_error("number","table_minimize_existing",minval);
  else if (HASHTABLEP(table))
    if (CHOICEP(keys)) {
      const lispval *elts = KNO_CHOICE_DATA(keys);
      int n_elts = KNO_CHOICE_SIZE(keys);
      if (kno_hashtable_iterkeys
          (KNO_XHASHTABLE(table),kno_table_minimize_if_present,n_elts,elts,minval)<0) {
        return KNO_ERROR;}
      else return VOID;}
    else if (EMPTYP(keys))
      return VOID;
    else if (kno_hashtable_op(KNO_XHASHTABLE(table),kno_table_minimize_if_present,keys,minval)<0)
      return KNO_ERROR;
    else return VOID;
  else if (TABLEP(table)) {
    DO_CHOICES(key,keys) {
      lispval cur = kno_get(table,key,VOID);
      if (VOIDP(cur)) {}
      else if (NUMBERP(cur)) {
        if (kno_numcompare(minval,cur)<0) {
          kno_store(table,key,minval);
          kno_decref(cur);}
        else {kno_decref(cur);}}
      else return kno_type_error("number","table_minimize_existing",cur);}
    return VOID;}
  else return kno_type_error("table","table_minimize",table);
}

/* Generic functions */

KNO_DCLPRIM("TABLE-SIZE",table_size,MAX_ARGS(1),
        "`(TABLE-SIZE *table*)` returns the number of keys in *table*")
static lispval table_size(lispval table)
{
  int size = kno_getsize(table);
  if (size<0) return KNO_ERROR;
  else return KNO_INT(size);
}

KNO_DCLPRIM("TABLE-WRITABLE?",table_writablep,MAX_ARGS(1),
        "(TABLE-WRITABLE? *table*) returns true if *table* "
        "can be modified")
static lispval table_writablep(lispval table)
{
  int read_only = kno_readonlyp(table);
  if (read_only == 0)
    return KNO_TRUE;
  else if (read_only > 0)
    return KNO_FALSE;
  else return KNO_ERROR;
}

KNO_DCLPRIM("TABLE-WRITABLE!",table_set_writable,MAX_ARGS(2)|MIN_ARGS(1),
        "(TABLE-WRITABLE! *table* *flag*) sets the read-only status "
        "of *table*. With no *flag*, it defaults to making the table "
        "writable.")
static lispval table_set_writable(lispval table,lispval flag_arg)
{
  int flag = ((FALSEP(flag_arg))||(KNO_ZEROP(flag_arg)))?(1):
    (VOIDP(flag_arg))?(0):(1);
  int retval = kno_set_readonly(table,flag);
  if (retval == 0)
    return KNO_FALSE;
  else if (retval > 0)
    return KNO_TRUE;
  else return KNO_ERROR;
}

KNO_DCLPRIM("TABLE-MODIFIED?",table_modifiedp,MAX_ARGS(1),
        "`(TABLE-MODIFIED? *table*)` returns true if table has been"
        "modified since it was created or the last call to `TABLE-MODIFIED!`")
static lispval table_modifiedp(lispval table)
{
  int ismod = kno_modifiedp(table);
  if (ismod == 0)
    return KNO_FALSE;
  else if (ismod > 0)
    return KNO_TRUE;
  else return KNO_ERROR;
}

KNO_DCLPRIM("TABLE-MODIFIED!",table_set_modified,MAX_ARGS(2)|MIN_ARGS(1),
        "`(TABLE-MODIFIED! *table* [*flag*] )` sets or clears the modified "
        "flag of *table*. *flag* defaults to true.")
static lispval table_set_modified(lispval table,lispval flag_arg)
{
  int flag = ((FALSEP(flag_arg))||(KNO_ZEROP(flag_arg)))?(0):
    (VOIDP(flag_arg))?(0):(1);
  int retval = kno_set_modified(table,flag);
  if (retval == 0)
    return KNO_FALSE;
  else if (retval > 0)
    return KNO_TRUE;
  else return KNO_ERROR;
}

/* Max/Min etc */

/* Getting max values out of tables, especially hashtables. */

KNO_DCLPRIM("TABLE-MAX",table_max,MAX_ARGS(2)|MIN_ARGS(1)|NDCALL,
        "`(TABLE-MAX *table* [*scope*])` returns the key(s) "
        "in *table* with the largest numeric values. If *scope* "
        "is provided, limit the operation to the keys in *scope*.")
static lispval table_max(lispval tables,lispval scope)
{
  if (EMPTYP(scope))
    return scope;
  else if (KNO_HASHTABLEP(tables))
    return kno_hashtable_max(KNO_XHASHTABLE(tables),scope,NULL);
  else {
    lispval results = EMPTY;
    DO_CHOICES(table,tables)
      if (KNO_HASHTABLEP(table)) {
        lispval result = kno_hashtable_max((kno_hashtable)table,scope,NULL);
        CHOICE_ADD(results,result);}
      else if (TABLEP(table)) {
        lispval result = kno_table_max(table,scope,NULL);
        CHOICE_ADD(results,result);}
      else {
        kno_decref(results);
        return kno_type_error(_("table"),"table_max",table);}
    return results;}
}

KNO_DCLPRIM("TABLE-MAXVAL",table_maxval,MAX_ARGS(2)|MIN_ARGS(1)|NDCALL,
        "`(TABLE-MAXVAL *table* [*scope*])` returns the value "
        "in *table* with the largest numeric magnitude. If *scope* "
        "is provided, limit the operation to the values associated "
        "with keys in *scope*.")
static lispval table_maxval(lispval tables,lispval scope)
{
  if (EMPTYP(scope)) return scope;
  else {
    lispval results = EMPTY;
    DO_CHOICES(table,tables)
      if (TABLEP(table)) {
        lispval maxval = EMPTY;
        lispval result = kno_table_max(table,scope,&maxval);
        CHOICE_ADD(results,maxval);
        kno_decref(result);}
      else {
        kno_decref(results);
        return kno_type_error(_("table"),"table_maxval",table);}
    return results;}
}

KNO_DCLPRIM("TABLE-SKIM",table_skim,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(TABLE-SKIM *table* *threshold* [*scope*])` returns the key(s) "
        "in *table* associated with numeric values larger than *threhsold*. "
        "If *scope* is provided, limit the operation to the keys in *scope*.")
static lispval table_skim(lispval tables,lispval maxval,lispval scope)
{
  if (EMPTYP(scope))
    return scope;
  else if (EMPTYP(maxval))
    return maxval;
  else if (KNO_HASHTABLEP(tables))
    return kno_hashtable_skim(KNO_XHASHTABLE(tables),maxval,scope);
  else {
    lispval results = EMPTY;
    DO_CHOICES(table,tables)
      if (TABLEP(table)) {
        lispval result = (KNO_HASHTABLEP(table)) ?
          (kno_hashtable_skim((kno_hashtable)table,maxval,scope)) :
          (kno_table_skim(table,maxval,scope));
        if (KNO_ABORTP(result)) {
          kno_decref(results);
          return result;}
        else {CHOICE_ADD(results,result);}}
      else {
        kno_decref(results);
        return kno_type_error(_("table"),"table_skim",table);}
    return results;}
}

/* Table utility functions */

KNO_DCLPRIM("MAP->TABLE",map2table,MAX_ARGS(3)|MIN_ARGS(2)|NDCALL,
        "`(MAP->TABLE *keys* *fcn* [*hashp*])` returns "
        "a table store the results of applying *fn* "
        "to *keys*. The type of table is controlled by *hashp*:"
        "* without *hashp*, &gt; 8 keys generates a hashtable, otherwise a slotmap;\n"
        "* if *hashp* is #t, a hashtable is always generated;\n"
        "* if *hashp* is #f, a slotmap is always generated;\n"
        "* if *hashp* is a fixnum, it is the threshold for using a hashtable.")
static lispval map2table(lispval keys,lispval fn,lispval hashp)
{
  int n_keys = KNO_CHOICE_SIZE(keys);
  lispval table;
  if (FALSEP(hashp)) table = kno_empty_slotmap();
  else if (KNO_TRUEP(hashp)) table = kno_make_hashtable(NULL,n_keys*2);
  else if (FIXNUMP(hashp))
    if (n_keys>(FIX2INT(hashp))) table = kno_make_hashtable(NULL,n_keys*2);
    else table = kno_empty_slotmap();
  else if (n_keys>8) table = kno_make_hashtable(NULL,n_keys*2);
  else table = kno_empty_slotmap();
  if ((SYMBOLP(fn)) || (OIDP(fn))) {
    DO_CHOICES(k,keys) {
      lispval v = ((OIDP(k)) ? (kno_frame_get(k,fn)) : (kno_get(k,fn,EMPTY)));
      kno_add(table,k,v);
      kno_decref(v);}}
  else if (KNO_APPLICABLEP(fn)) {
    DO_CHOICES(k,keys) {
      lispval v = kno_apply(fn,1,&k);
      kno_add(table,k,v);
      kno_decref(v);}}
  else if (TABLEP(fn)) {
    DO_CHOICES(k,keys) {
      lispval v = kno_get(fn,k,EMPTY);
      kno_add(table,k,v);
      kno_decref(v);}}
  else {
    kno_decref(table);
    return kno_type_error("map","map2table",fn);}
  return table;
}

KNO_DCLPRIM1("TABLE-MAP-SIZE",table_map_size,MAX_ARGS(1),
         "`(TABLE-MAP-SIZE *table*)` returns the number key/value "
         "associates in *table*.",
         kno_table_type,KNO_VOID)
static lispval table_map_size(lispval table)
{
  if (TYPEP(table,kno_hashtable_type)) {
    struct KNO_HASHTABLE *ht = (struct KNO_HASHTABLE *) table;
    long long n_values = kno_hashtable_map_size(ht);
    return KNO_INT(n_values);}
  else if (TYPEP(table,kno_hashset_type)) {
    struct KNO_HASHSET *hs = (struct KNO_HASHSET *) table;
    return KNO_INT(hs->hs_n_elts);}
  else if (TABLEP(table)) {
    lispval keys = kno_getkeys(table);
    long long count = 0;
    DO_CHOICES(key,keys) {
      lispval v = kno_get(table,key,VOID);
      if (!(VOIDP(v))) {
        int size = KNO_CHOICE_SIZE(v);
        count += size;}
      kno_decref(v);}
    kno_decref(keys);
    return KNO_INT(count);}
  else return kno_type_error(_("table"),"table_map_size",table);
}

/* Hashset operations */

KNO_DCLPRIM("HASHSET?",hashsetp,MAX_ARGS(1),
        "`(HASHSET? *obj*)` returns true if *obj* is a hashset.")
static lispval hashsetp(lispval x)
{
  if (TYPEP(x,kno_hashset_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM("HASHSET-ADD!",hashset_add,MAX_ARGS(2)|MIN_ARGS(2)|NDCALL,
        "`(HASHSET-ADD! *hashset* *keys*)` adds *keys* to *hashset*(s). "
        "Returns the number of new values added.")
static lispval hashset_add(lispval hs,lispval key)
{
  if (KNO_EMPTYP(hs))
    return KNO_EMPTY;
  else if (CHOICEP(hs)) {
    lispval results = EMPTY;
    DO_CHOICES(h,hs) {
      if (!(KNO_TYPEP(hs,kno_hashset_type))) {
        KNO_STOP_DO_CHOICES;
        kno_decref(results);
        return kno_type_error("hashset","hashset_add",h);}
      lispval value = hashset_add(h,key);
      CHOICE_ADD(results,value);}
    return results;}
  else if (!(KNO_TYPEP(hs,kno_hashset_type)))
    return kno_type_error("hashset","hashset_add",hs);
  else {
    int retval = kno_hashset_add((kno_hashset)hs,key);
    if (retval<0)
      return KNO_ERROR;
    else if (retval)
      return KNO_INT(retval);
    else return KNO_FALSE;}
}

KNO_DCLPRIM("HASHSET+",hashset_plus,MAX_ARGS(2)|MIN_ARGS(2),
        "`(HASHSET+ *hashset* *keys*)` adds *keys* to *hashset*")
static lispval hashset_plus(lispval hs,lispval values)
{
  kno_hashset_add((kno_hashset)hs,values);
  kno_incref(hs);
  return  hs;
}

KNO_DCLPRIM("HASHSET-DROP!",hashset_drop,MAX_ARGS(2)|MIN_ARGS(2),
        "`(HASHSET-DROP! *hashset* *keys*)` removes *key* from *hashset*.")
static lispval hashset_drop(lispval hs,lispval key)
{
  int retval = kno_hashset_drop((kno_hashset)hs,key);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM("HASHSET-GET",hashset_get,MAX_ARGS(2)|MIN_ARGS(2),
        "`(HASHSET-GET *hashset* *key*)` returns true if "
        "*key* is in *hashsets*")
static lispval hashset_get(lispval hs,lispval key)
{
  int retval = kno_hashset_get((kno_hashset)hs,key);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM("HASHSET-TEST",hashset_test,
        MAX_ARGS(2)|MIN_ARGS(2)|NDCALL,
        "`(HASHSET-TEST *hashset* *keys*)` returns true if "
        "any *keys* are in any of *hashsets*")
static lispval hashset_test(lispval hs,lispval keys)
{
  DO_CHOICES(hset,hs) {
    if (!(TYPEP(hset,kno_hashset_type)))
      return kno_type_error(_("hashset"),"hashsettest",hset);}
  if ( (KNO_CHOICEP(keys)) ) {
    DO_CHOICES(hset,hs) {
      DO_CHOICES(key,keys) {
        if (kno_hashset_get((kno_hashset)hs,key)) {
          KNO_STOP_DO_CHOICES;
          return KNO_TRUE;}}}
    return KNO_FALSE;}
  else if (KNO_CHOICEP(hs)) {
    DO_CHOICES(hset,hs) {
      if (kno_hashset_get((kno_hashset)hs,keys)) {
        KNO_STOP_DO_CHOICES;
        return KNO_TRUE;}}
    return KNO_FALSE;}
  else if (kno_hashset_get((kno_hashset)hs,keys))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM("HASHSET-ELTS",hashset_elts,MAX_ARGS(2)|MIN_ARGS(1),
        "Returns the elements of a hashset.\n"
        "With a non-false second argument, resets "
        "the hashset (removing all values).")
static lispval hashset_elts(lispval hs,lispval clean)
{
  if (FALSEP(clean))
    return kno_hashset_elts((kno_hashset)hs,0);
  else return kno_hashset_elts((kno_hashset)hs,1);
}

KNO_DCLPRIM("RESET-HASHSET!",reset_hashset,MAX_ARGS(1),
        "`(RESET_HASHSET! *hashset*)` removes all of the elements of *hashset*.")
static lispval reset_hashset(lispval hs)
{
  int rv=kno_reset_hashset((kno_hashset)hs);
  if (rv<0)
    return KNO_ERROR;
  else if (rv)
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM("CHOICE->HASHSET",choices2hashset,KNO_N_ARGS|MIN_ARGS(0)|NDCALL,
        "`(CHOICE->HASHSET choices...)` returns a hashset combining "
        "mutiple choices.")
static lispval choices2hashset(int n,lispval *args)
{
  struct KNO_HASHSET *h = u8_alloc(struct KNO_HASHSET);
  int size = 0; int i = 0; while (i<n) {
    size += KNO_CHOICE_SIZE(args[i]); i++;}
  kno_init_hashset(h,((size<17) ? (17) : (size*3)),KNO_MALLOCD_CONS);
  i=0; while (i<n) {
    kno_hashset_add(h,args[i]); i++;}
  return LISP_CONS(h);
}

KNO_DCLPRIM2("HASHSET/INTERN",hashset_intern,MIN_ARGS(2),
         "(HASHSET/INTERN *hashset* *key*)\n"
         "If *key* is in *hashset*, returns the exact key (pointer) "
         "stored in *hashset*, otherwise adds *key* to the hashset "
         "and returns it.",
         kno_hashset_type,KNO_VOID,-1,KNO_VOID)
static lispval hashset_intern(lispval hs,lispval key)
{
  return kno_hashset_intern((kno_hashset)hs,key,1);
}

KNO_DCLPRIM2("HASHSET/PROBE",hashset_probe,MIN_ARGS(2),
         "(HASHSET/PROBE *hashset* *key*)\n"
         "If *key* is in *hashset*, returns the exact key (pointer) "
         "stored in *hashset*, otherwise returns {}",
         kno_hashset_type,KNO_VOID,-1,KNO_VOID)
static lispval hashset_probe(lispval hs,lispval key)
{
  return kno_hashset_intern((kno_hashset)hs,key,0);
}

/* Sorting slotmaps */

KNO_DCLPRIM1("SORT-SLOTMAP",sort_slotmap,MAX_ARGS(1),
         "`(SORT-SLOTMAP *slotmap*)` sorts the keys in *slotmap* "
         "for improved performance",
         kno_slotmap_type,KNO_VOID)
static lispval sort_slotmap(lispval slotmap)
{
  if (kno_sort_slotmap(slotmap,1)<0)
    return KNO_ERROR;
  else return kno_incref(slotmap);
}

/* Merging hashtables */

KNO_DCLPRIM1("HASHTABLE-BUCKETS",hashtable_buckets,MAX_ARGS(1),
         "`(HASHTABLE-BUCKETS *hashtable*)` returns the number of buckets "
         "allocated for *hashtable*.",
         kno_hashtable_type,KNO_VOID)
static lispval hashtable_buckets(lispval table)
{
  kno_hashtable h = kno_consptr(kno_hashtable,table,kno_hashtable_type);
  return KNO_INT(h->ht_n_buckets);
}

static int merge_kv_into_table(struct KNO_KEYVAL *kv,void *data)
{
  struct KNO_HASHTABLE *ht = (kno_hashtable) data;
  kno_hashtable_op_nolock(ht,kno_table_add,kv->kv_key,kv->kv_val);
  return 0;
}

KNO_DCLPRIM2("HASHTABLE/MERGE",hashtable_merge,MIN_ARGS(2),
         "`(HASHTABLE/MERGE *dest* *src*)`\n"
         "Merges one hashtable into another",
         kno_hashtable_type,VOID,kno_hashtable_type,VOID)
static lispval hashtable_merge(lispval dest,lispval src)
{
  kno_hashtable into = (kno_hashtable) dest;
  kno_hashtable from = (kno_hashtable) src;
  kno_write_lock_table(into);
  kno_read_lock_table(from);
  kno_for_hashtable_kv(from,merge_kv_into_table,(void *)into,0);
  kno_unlock_table(from);
  kno_unlock_table(into);
  return kno_incref(dest);
}

DEFPRIM_DECL("PLIST->TABLE",kno_plist_to_slotmap,MAX_ARGS(1),
             "`(PLIST->TABLE *plist*)` returns a slotmap from a plist "
             "(property list) of the form "
             "(key1 value1 key2 value2 ... )");
DEFPRIM_DECL("ALIST->TABLE",kno_alist_to_slotmap,MAX_ARGS(1),
             "`(ALIST->TABLE *list*)` returns a slotmap from an alist "
             "(association list) of the form "
             "((key1 . value1) (key2 . value2) ... )");
DEFPRIM_DECL("BLIST->TABLE",kno_blist_to_slotmap,MAX_ARGS(1),
             "`(BLIST->TABLE *list*)` returns a slotmap from a blist "
             "(binding list) of the form "
             "((key1  value1) (key2 value2) ... )");

/* Initialization code */

KNO_EXPORT void kno_init_tableprims_c()
{
  u8_register_source_file(_FILEINFO);

  DECL_PRIM(tablep,1,kno_scheme_module);
  DECL_PRIM(haskeysp,1,kno_scheme_module);
  DECL_PRIM(slotmapp,1,kno_scheme_module);
  DECL_PRIM(schemapp,1,kno_scheme_module);
  DECL_PRIM(hashtablep,1,kno_scheme_module);
  DECL_PRIM(make_hashset,1,kno_scheme_module);
  DECL_PRIM(make_hashtable,1,kno_scheme_module);

  DECL_PRIM(hash_lisp_prim,1,kno_scheme_module);

  DECL_PRIM(static_hashtable,1,kno_scheme_module);
  DECL_PRIM(unsafe_hashtable,1,kno_scheme_module);
  DECL_PRIM(resafe_hashtable,1,kno_scheme_module);

  DECL_PRIM(pick_hashtable_size,1,kno_scheme_module);
  DECL_PRIM(reset_hashtable,2,kno_scheme_module);

  DECL_PRIM(schemap2slotmap_prim,1,kno_scheme_module);
  DECL_PRIM(slotmap2schemap_prim,1,kno_scheme_module);
  DECL_PRIM(table2schemap_prim,1,kno_scheme_module);

  /* Note that GET and TEST are actually DB functions which do inference */
  DECL_PRIM(table_get,3,kno_scheme_module);
  DECL_PRIM(table_test,3,kno_scheme_module);
  DECL_PRIM(table_add,3,kno_scheme_module);
  DECL_PRIM(table_drop,3,kno_scheme_module);
  DECL_PRIM(table_store,3,kno_scheme_module);

  DECL_PRIM(kno_getkeys,1,kno_scheme_module);
  DECL_PRIM(kno_getvalues,1,kno_scheme_module);
  DECL_PRIM(kno_getassocs,1,kno_scheme_module);

  DECL_PRIM(table_size,1,kno_scheme_module);

  DECL_PRIM(table_writablep,1,kno_scheme_module);
  DECL_PRIM(table_set_writable,2,kno_scheme_module);

  DECL_PRIM(table_modifiedp,1,kno_scheme_module);
  DECL_PRIM(table_set_modified,2,kno_scheme_module);

  DECL_ALIAS("WRITABLE?",table_writablep,kno_scheme_module);

  DECL_PRIM(table_max,2,kno_scheme_module);
  DECL_PRIM(table_maxval,2,kno_scheme_module);
  DECL_PRIM(table_skim,3,kno_scheme_module);

  DECL_PRIM(map2table,3,kno_scheme_module);
  DECL_PRIM(table_map_size,1,kno_scheme_module);

  DECL_PRIM(table_increment,3,kno_scheme_module);
  DECL_PRIM(table_increment_existing,3,kno_scheme_module);
  DECL_PRIM(table_multiply,3,kno_scheme_module);
  DECL_PRIM(table_multiply_existing,3,kno_scheme_module);
  DECL_PRIM(table_maximize,3,kno_scheme_module);
  DECL_PRIM(table_maximize_existing,3,kno_scheme_module);
  DECL_PRIM(table_minimize,3,kno_scheme_module);
  DECL_PRIM(table_minimize_existing,3,kno_scheme_module);

  DECL_ALIAS("HASHTABLE-MAX",table_max,kno_scheme_module);
  DECL_ALIAS("HASHTABLE-MAXVAL",table_maxval,kno_scheme_module);
  DECL_ALIAS("HASHTABLE-SKIM",table_skim,kno_scheme_module);

  DECL_ALIAS("HASHTABLE-INCREMENT!",table_increment,kno_scheme_module);
  DECL_ALIAS("HASHTABLE-INCREMENT-EXISTING!",table_increment_existing,
             kno_scheme_module);
  DECL_ALIAS("HASHTABLE-MULTIPLY!",table_multiply,kno_scheme_module);
  DECL_ALIAS("HASHTABLE-MULTIPLY-EXISTING!",table_multiply_existing,
             kno_scheme_module);
  DECL_ALIAS("HASHTABLE-MAXIMIZE!",table_maximize,kno_scheme_module);
  DECL_ALIAS("HASHTABLE-MAXIMIZE-EXISTING!",table_maximize_existing,
             kno_scheme_module);
  DECL_ALIAS("HASHTABLE-MINIMIZE!",table_minimize,kno_scheme_module);
  DECL_ALIAS("HASHTABLE-MINIMIZE-EXISTING!",table_minimize_existing,
             kno_scheme_module);

  DECL_PRIM(hashtable_buckets,1,kno_scheme_module);
  DECL_PRIM(hashtable_merge,2,kno_scheme_module);

  /* Hashset primitives */

  DECL_PRIM_N(choices2hashset,kno_scheme_module);

  DECL_PRIM(hashsetp,1,kno_scheme_module);
  DECL_PRIM(hashset_get,2,kno_scheme_module);
  DECL_PRIM(hashset_test,2,kno_scheme_module);
  DECL_PRIM(hashset_add,2,kno_scheme_module);
  DECL_PRIM(hashset_drop,2,kno_scheme_module);

  DECL_PRIM(hashset_probe,2,kno_scheme_module);
  DECL_PRIM(hashset_intern,2,kno_scheme_module);

  DECL_PRIM(hashset_plus,2,kno_scheme_module);
  DECL_PRIM(hashset_elts,2,kno_scheme_module);
  DECL_PRIM(reset_hashset,1,kno_scheme_module);

  DECL_PRIM(sort_slotmap,1,kno_scheme_module);

  /* Various list to table conversion functions */

  DECL_PRIM(kno_plist_to_slotmap,1,kno_scheme_module);
  DECL_PRIM(kno_alist_to_slotmap,1,kno_scheme_module);
  DECL_PRIM(kno_blist_to_slotmap,1,kno_scheme_module);
  DECL_ALIAS("BINDINGS->SLOTMAP",kno_blist_to_slotmap,kno_scheme_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/


static void init_cprims(){
}
