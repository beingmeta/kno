/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES   (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES    (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/cprims.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"

DEFC_PRIM("haskeys?",haskeysp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *obj* is a non-empty table, #f "
	  "otherwise.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval haskeysp(lispval arg)
{
  if (TABLEP(arg)) {
    kno_lisp_type argtype = KNO_TYPEOF(arg);
    if ((kno_tablefns[argtype])->keys)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("slotmap?",slotmapp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *obj* is a slotmap, #f otherwise.",
	  {"x",kno_any_type,KNO_VOID})
static lispval slotmapp(lispval x)
{
  if (SLOTMAPP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("schemap?",schemapp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *obj* is a slotmap, #f otherwise.",
	  {"x",kno_any_type,KNO_VOID})
static lispval schemapp(lispval x)
{
  if (SCHEMAPP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("hashtable?",hashtablep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *obj* is a slotmap, #f otherwise.",
	  {"x",kno_any_type,KNO_VOID})
static lispval hashtablep(lispval x)
{
  if (HASHTABLEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("make-hashset",make_hashset,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "returns a hashset. *n_buckets*, if provided "
	  "indicates the number of buckets",
	  {"arg",kno_fixnum_type,KNO_VOID})
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

DEFC_PRIM("make-hashtable",make_hashtable,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "returns a hashtable. *n_buckets*, if provided "
	  "indicates the number of buckets",
	  {"size",kno_fixnum_type,KNO_VOID})
static lispval make_hashtable(lispval size)
{
  if (KNO_UINTP(size))
    return kno_make_hashtable(NULL,FIX2INT(size));
  else return kno_make_hashtable(NULL,0);
}

DEFC_PRIM("make-eq-hashtable",make_eq_hashtable,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	  "returns an `EQ` hashtable. *n_buckets*, if provided "
	  "indicates the number of buckets",
	  {"size",kno_fixnum_type,KNO_VOID})
static lispval make_eq_hashtable(lispval size)
{
  if (KNO_UINTP(size))
    return kno_make_eq_hashtable(NULL,FIX2INT(size));
  else return kno_make_eq_hashtable(NULL,0);
}

DEFC_PRIM("pick-hashtable-size",pick_hashtable_size,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "picks a good hashtable size for a table of "
	  "*count* elements.",
	  {"count_arg",kno_fixnum_type,KNO_VOID})
static lispval pick_hashtable_size(lispval count_arg)
{
  if (!(KNO_UINTP(count_arg)))
    return kno_type_error("uint","pick_hashtable_size",count_arg);
  int count = FIX2INT(count_arg);
  int size = kno_get_hashtable_size(count);
  return KNO_INT(size);
}

DEFC_PRIM("reset-hashtable!",reset_hashtable,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "resets a hashtable, removing all of its values",
	  {"table",kno_hashtable_type,KNO_VOID},
	  {"n_slots",kno_fixnum_type,KNO_VOID})
static lispval reset_hashtable(lispval table,lispval n_slots)
{
  if (!(KNO_UINTP(n_slots)))
    return kno_type_error("uint","reset_hashtable",n_slots);
  kno_reset_hashtable((kno_hashtable)table,FIX2INT(n_slots),1);
  return VOID;
}

DEFC_PRIM("static-hashtable",static_hashtable,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "declares all keys and values in *table* to be "
	  "static (no GC) and disables locking for the "
	  "table. This may improve performance on objects in "
	  "the table",
	  {"table",kno_hashtable_type,KNO_VOID})
static lispval static_hashtable(lispval table)
{
  struct KNO_HASHTABLE *ht = (kno_hashtable)table;
  kno_write_lock_table(ht);
  kno_static_hashtable(ht,-1);
  KNO_XTABLE_SET_USELOCK(ht,0);
  kno_unlock_table(ht);
  return kno_incref(table);
}

DEFC_PRIM("readonly-hashtable!",readonly_hashtable_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "makes *hashtable* be read-only and disables "
	  "locking for improved performance.",
	  {"table",kno_hashtable_type,KNO_VOID},
	  {"flag",kno_any_type,KNO_TRUE})
static lispval readonly_hashtable_prim(lispval table,lispval flag)
{
  int rv = kno_hashtable_set_readonly((kno_hashtable)table,KNO_TRUEP(flag));
  if (rv<0) return KNO_ERROR;
  else return kno_incref(table);
}

DEFC_PRIM("unsafe-hashtable",unsafe_hashtable,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "disables locking for *table*. This may improve "
	  "performance.",
	  {"table",kno_hashtable_type,KNO_VOID})
static lispval unsafe_hashtable(lispval table)
{
  struct KNO_HASHTABLE *ht = (kno_hashtable)table;
  kno_write_lock_table(ht);
  KNO_XTABLE_SET_USELOCK(ht,0);
  kno_unlock_table(ht);
  return kno_incref(table);
}

DEFC_PRIM("resafe-hashtable",resafe_hashtable,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "re-enables locking for *table* disabled by "
	  "`UNSAFE-TABLE`.",
	  {"table",kno_hashtable_type,KNO_VOID})
static lispval resafe_hashtable(lispval table)
{
  struct KNO_HASHTABLE *ht = (kno_hashtable)table;
  kno_write_lock_table(ht);
  KNO_XTABLE_SET_USELOCK(ht,1);
  kno_unlock_table(ht);
  return kno_incref(table);
}

DEFC_PRIM("hash-lisp",hash_lisp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns an integer hash value for *object*.",
	  {"x",kno_any_type,KNO_VOID})
static lispval hash_lisp_prim(lispval x)
{
  int val = kno_hash_lisp(x);
  return KNO_INT(val);
}

/* Converting schemaps to slotmaps */

DEFC_PRIM("schemap->slotmap",schemap2slotmap_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "converts a schemap to a slotmap.",
	  {"in",kno_schemap_type,KNO_VOID})
static lispval schemap2slotmap_prim(lispval in)
{
  return kno_schemap2slotmap(in);
}

DEFC_PRIM("slotmap->schemap",slotmap2schemap_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "converts a schemap to a slotmap.",
	  {"map",kno_schemap_type,KNO_VOID})
static lispval slotmap2schemap_prim(lispval map)
{
  struct KNO_SLOTMAP *slotmap = (kno_slotmap) map;
  lispval result = kno_init_schemap(NULL,slotmap->n_slots,slotmap->sm_keyvals);
  struct KNO_SCHEMAP *schemap = (kno_schemap) result;
  kno_incref_elts(schemap->table_schema,schemap->schema_length);
  kno_incref_elts(schemap->table_values,schemap->schema_length);
  return result;
}

DEFC_PRIM("->schemap",table2schemap_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "converts a slotmap to a schemap.",
	  {"tbl",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-increment!",table_increment,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "adds *delta* (default to 1) to the current value "
	  "of *key* in *table* (which defaults to 0).",
	  {"table",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID},
	  {"increment",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-increment-existing!",table_increment_existing,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "adds *delta* (default to 1) to the current value "
	  "of *key* in *table*, doing nothing if *key* is "
	  "not in *table*.",
	  {"table",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID},
	  {"increment",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-multiply!",table_multiply,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "multiplies the current value of *key* in *table* "
	  "by *factor*, defaulting the value of *key* to 1.",
	  {"table",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID},
	  {"factor",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-multiply-existing!",table_multiply_existing,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "multiplies the current value of *key* in *table* "
	  "by *factor*, doing nothing if *key* is not "
	  "currently defined in *table*.",
	  {"table",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID},
	  {"factor",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-maximize!",table_maximize,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "stores *value* under  *key* in *table* if it is "
	  "larger than the current value or if there is no "
	  "current value.",
	  {"table",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID},
	  {"maxval",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-maximize-existing!",table_maximize_existing,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "stores *value* under  *key* in *table* if it is "
	  "larger than the current value, doing nothing if "
	  "*key* is not in *table*",
	  {"table",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID},
	  {"maxval",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-minimize!",table_minimize,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "stores *value* under  *key* in *table* if it is "
	  "smaller than the current value or if there is no "
	  "current value.",
	  {"table",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID},
	  {"minval",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-minimize-existing!",table_minimize_existing,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	  "stores *value* under  *key* in *table* if it is "
	  "smaller than the current value, doing nothing if "
	  "*key* is not in *table*",
	  {"table",kno_any_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID},
	  {"minval",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-size",table_size,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of keys in *table*",
	  {"table",kno_any_type,KNO_VOID})
static lispval table_size(lispval table)
{
  int size = kno_getsize(table);
  if (size<0) return KNO_ERROR;
  else return KNO_INT(size);
}

DEFC_PRIM("table-writable?",table_writablep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "(TABLE-WRITABLE? *table*) "
	  "returns true if *table* can be modified",
	  {"table",kno_any_type,KNO_VOID})
static lispval table_writablep(lispval table)
{
  int read_only = kno_readonlyp(table);
  if (read_only == 0)
    return KNO_TRUE;
  else if (read_only > 0)
    return KNO_FALSE;
  else return KNO_ERROR;
}

DEFC_PRIM("table-writable!",table_set_writable,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "(TABLE-WRITABLE! *table* *flag*) "
	  "sets the read-only status of *table*. With no "
	  "*flag*, it defaults to making the table writable.",
	  {"table",kno_any_type,KNO_VOID},
	  {"flag_arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-modified?",table_modifiedp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if table has beenmodified since it "
	  "was created or the last call to `TABLE-MODIFIED!`",
	  {"table",kno_any_type,KNO_VOID})
static lispval table_modifiedp(lispval table)
{
  int ismod = kno_modifiedp(table);
  if (ismod == 0)
    return KNO_FALSE;
  else if (ismod > 0)
    return KNO_TRUE;
  else return KNO_ERROR;
}

DEFC_PRIM("table-modified!",table_set_modified,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "sets or clears the modified flag of *table*. "
	  "*flag* defaults to true.",
	  {"table",kno_any_type,KNO_VOID},
	  {"flag_arg",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-max",table_max,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "returns the key(s) in *table* with the largest "
	  "numeric values. If *scope* is provided, limit the "
	  "operation to the keys in *scope*.",
	  {"tables",kno_any_type,KNO_VOID},
	  {"scope",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-maxval",table_maxval,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "returns the value in *table* with the largest "
	  "numeric magnitude. If *scope* is provided, limit "
	  "the operation to the values associated with keys "
	  "in *scope*.",
	  {"tables",kno_any_type,KNO_VOID},
	  {"scope",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-skim",table_skim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "returns the key(s) in *table* associated with "
	  "numeric values larger than *threhsold*. If "
	  "*scope* is provided, limit the operation to the "
	  "keys in *scope*.",
	  {"tables",kno_any_type,KNO_VOID},
	  {"maxval",kno_any_type,KNO_VOID},
	  {"scope",kno_any_type,KNO_VOID})
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

DEFC_PRIM("map->table",map2table,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "returns a table store the results of applying "
	  "*fn* to *keys*. The type of table is controlled "
	  "by *hashp*:* without *hashp*, &gt; 8 keys "
	  "generates a hashtable, otherwise a slotmap;\n* if "
	  "*hashp* is #t, a hashtable is always generated;\n* "
	  "if *hashp* is #f, a slotmap is always generated;\n"
	  "* if *hashp* is a fixnum, it is the threshold for "
	  "using a hashtable.",
	  {"keys",kno_any_type,KNO_VOID},
	  {"fn",kno_any_type,KNO_VOID},
	  {"hashp",kno_any_type,KNO_VOID})
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

DEFC_PRIM("table-map-size",table_map_size,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number key/value associates in "
	  "*table*.",
	  {"table",kno_table_type,KNO_VOID})
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

DEFC_PRIM("hashset?",hashsetp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *obj* is a hashset.",
	  {"x",kno_any_type,KNO_VOID})
static lispval hashsetp(lispval x)
{
  if (TYPEP(x,kno_hashset_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("hashset-add!",hashset_add,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "adds *keys* to *hashset*(s). Returns the number "
	  "of new values added.",
	  {"hs",kno_hashset_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID})
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

DEFC_PRIM("hashset+",hashset_plus,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "adds *keys* to *hashset*",
	  {"hs",kno_hashset_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID})
static lispval hashset_plus(lispval hs,lispval values)
{
  kno_hashset_add((kno_hashset)hs,values);
  kno_incref(hs);
  return  hs;
}

DEFC_PRIM("hashset-drop!",hashset_drop,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "removes *key* from *hashset*.",
	  {"hs",kno_hashset_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID})
static lispval hashset_drop(lispval hs,lispval key)
{
  int retval = kno_hashset_drop((kno_hashset)hs,key);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("hashset-get",hashset_get,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "returns true if *key* is in *hashsets*",
	  {"hs",kno_hashset_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID})
static lispval hashset_get(lispval hs,lispval key)
{
  int retval = kno_hashset_get((kno_hashset)hs,key);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("hashset-test",hashset_test,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "returns true if any *keys* are in any of "
	  "*hashsets*",
	  {"hs",kno_hashset_type,KNO_VOID},
	  {"keys",kno_any_type,KNO_VOID})
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

DEFC_PRIM("hashset-elts",hashset_elts,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns the elements of a hashset.\nWith a "
	  "non-false second argument, resets the hashset "
	  "(removing all values).",
	  {"hs",kno_hashset_type,KNO_VOID},
	  {"clean",kno_any_type,KNO_VOID})
static lispval hashset_elts(lispval hs,lispval clean)
{
  if (FALSEP(clean))
    return kno_hashset_elts((kno_hashset)hs,0);
  else return kno_hashset_elts((kno_hashset)hs,1);
}

DEFC_PRIM("hashset->vector",hashset2vector_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a vector made up of the elements of *hashset*",
	  {"hs",kno_hashset_type,KNO_VOID})
static lispval hashset2vector_prim(lispval hs)
{
  ssize_t len = -1;
  lispval *elts = kno_hashset_vec((kno_hashset)hs,&len);
  if (elts==NULL) return KNO_ERROR;
  else return kno_init_vector(NULL,len,elts);
}

DEFC_PRIM("reset-hashset!",reset_hashset,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "removes all of the elements of *hashset*.",
	  {"hs",kno_hashset_type,KNO_VOID})
static lispval reset_hashset(lispval hs)
{
  int rv=kno_reset_hashset((kno_hashset)hs);
  if (rv<0)
    return KNO_ERROR;
  else if (rv)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIMN("choice->hashset",choices2hashset,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
	   "returns a hashset combining mutiple choices.")
static lispval choices2hashset(int n,kno_argvec args)
{
  struct KNO_HASHSET *h = u8_alloc(struct KNO_HASHSET);
  int size = 0; int i = 0; while (i<n) {
    size += KNO_CHOICE_SIZE(args[i]); i++;}
  kno_init_hashset(h,((size<17) ? (17) : (size*3)),KNO_MALLOCD_CONS);
  i=0; while (i<n) {
    kno_hashset_add(h,args[i]); i++;}
  return LISP_CONS(h);
}

DEFC_PRIM("hashset/intern",hashset_intern,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "(HASHSET/INTERN *hashset* *key*)\n"
	  "If *key* is in *hashset*, returns the exact key "
	  "(pointer) stored in *hashset*, otherwise adds "
	  "*key* to the hashset and returns it.",
	  {"hs",kno_hashset_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID})
static lispval hashset_intern(lispval hs,lispval key)
{
  return kno_hashset_intern((kno_hashset)hs,key,1);
}

DEFC_PRIM("hashset/probe",hashset_probe,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "(HASHSET/PROBE *hashset* *key*)\n"
	  "If *key* is in *hashset*, returns the exact key "
	  "(pointer) stored in *hashset*, otherwise returns "
	  "{}",
	  {"hs",kno_hashset_type,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID})
static lispval hashset_probe(lispval hs,lispval key)
{
  return kno_hashset_intern((kno_hashset)hs,key,0);
}

DEFC_PRIM("hashset/merge!",hashset_merge,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Merges the hashet *src* into *dest*, returns "
	  "the number of of new values added.",
	  {"dest",kno_hashset_type,KNO_VOID},
	  {"src",kno_hashset_type,KNO_VOID})
static lispval hashset_merge(lispval dest,lispval src)
{
  ssize_t n_added = kno_hashset_merge((kno_hashset)dest,(kno_hashset)src);
  if (n_added<0) return KNO_ERROR;
  else return KNO_INT(n_added);
}

/* Sorting slotmaps */

DEFC_PRIM("sort-slotmap",sort_slotmap,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "sorts the keys in *slotmap* for improved "
	  "performance",
	  {"slotmap",kno_slotmap_type,KNO_VOID})
static lispval sort_slotmap(lispval slotmap)
{
  if (kno_sort_slotmap(slotmap,1)<0)
    return KNO_ERROR;
  else return kno_incref(slotmap);
}

/* Merging hashtables */

DEFC_PRIM("hashtable-buckets",hashtable_buckets,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of buckets allocated for "
	  "*hashtable*.",
	  {"table",kno_hashtable_type,KNO_VOID})
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

DEFC_PRIM("hashtable/merge",hashtable_merge,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "\n"
	  "Merges one hashtable into another",
	  {"dest",kno_hashtable_type,KNO_VOID},
	  {"src",kno_hashtable_type,KNO_VOID})
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

/* Declare some prims from elsewhere */

DEFC_PRIM("plist->table",kno_plist_to_slotmap,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a slotmap from a plist (property list) of "
	  "the form (key1 value1 key2 value2 ... )",
	  {"plist",kno_any_type,KNO_VOID});

DEFC_PRIM("alist->table",kno_alist_to_slotmap,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a slotmap from an alist (association "
	  "list) of the form ((key1 . value1) (key2 . "
	  "value2) ... )",
	  {"alist",kno_any_type,KNO_VOID});

DEFC_PRIM("blist->table",kno_blist_to_slotmap,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a slotmap from a blist (binding list) of "
	  "the form ((key1  value1) (key2 value2) ... )",
	  {"blist",kno_any_type,KNO_VOID});

/* Initialization code */

KNO_EXPORT void kno_init_tableprims_c()
{

  u8_register_source_file(_FILEINFO);

  link_local_cprims();


}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("HASKEYS?",haskeysp,1,kno_scheme_module);
  KNO_LINK_CPRIM("SLOTMAP?",slotmapp,1,kno_scheme_module);
  KNO_LINK_CPRIM("SCHEMAP?",schemapp,1,kno_scheme_module);
  KNO_LINK_CPRIM("HASHTABLE?",hashtablep,1,kno_scheme_module);
  KNO_LINK_CPRIM("MAKE-HASHSET",make_hashset,1,kno_scheme_module);
  KNO_LINK_CPRIM("MAKE-HASHTABLE",make_hashtable,1,kno_scheme_module);
  KNO_LINK_CPRIM("MAKE-EQ-HASHTABLE",make_eq_hashtable,1,kno_scheme_module);
  KNO_LINK_CPRIM("PICK-HASHTABLE-SIZE", pick_hashtable_size,1,kno_scheme_module);
  KNO_LINK_CPRIM("RESET-HASHTABLE!",reset_hashtable,2,kno_scheme_module);
  KNO_LINK_CPRIM("STATIC-HASHTABLE",static_hashtable,1,kno_scheme_module);
  KNO_LINK_CPRIM("UNSAFE-HASHTABLE",unsafe_hashtable,1,kno_scheme_module);
  KNO_LINK_CPRIM("READONLY-HASHTABLE!",readonly_hashtable_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("RESAFE-HASHTABLE",resafe_hashtable,1,kno_scheme_module);
  KNO_LINK_CPRIM("HASH-LISP",hash_lisp_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("SCHEMAP->SLOTMAP",schemap2slotmap_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("SLOTMAP->SCHEMAP",slotmap2schemap_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("->SCHEMAP",table2schemap_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-INCREMENT!",table_increment,3,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-INCREMENT-EXISTING!",table_increment_existing,3,
                kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MULTIPLY!",table_multiply,3,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MULTIPLY-EXISTING!",table_multiply_existing,3,
                kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MAXIMIZE!",table_maximize,3,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MAXIMIZE-EXISTING!",table_maximize_existing,3,
                kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MINIMIZE!",table_minimize,3,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MINIMIZE-EXISTING!",table_minimize_existing,3,
                kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-SIZE",table_size,1,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-WRITABLE?",table_writablep,1,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-WRITABLE!",table_set_writable,2,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MODIFIED?",table_modifiedp,1,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MODIFIED!",table_set_modified,2,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MAX",table_max,2,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MAXVAL",table_maxval,2,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-SKIM",table_skim,3,kno_scheme_module);
  KNO_LINK_CPRIM("MAP->TABLE",map2table,3,kno_scheme_module);
  KNO_LINK_CPRIM("TABLE-MAP-SIZE",table_map_size,1,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET?",hashsetp,1,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET-ADD!",hashset_add,2,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET+",hashset_plus,2,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET-DROP!",hashset_drop,2,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET-GET",hashset_get,2,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET-TEST",hashset_test,2,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET->VECTOR",hashset2vector_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET-ELTS",hashset_elts,2,kno_scheme_module);
  KNO_LINK_CPRIM("RESET-HASHSET!",reset_hashset,1,kno_scheme_module);

  KNO_LINK_CPRIMN("CHOICE->HASHSET",choices2hashset,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET/INTERN",hashset_intern,2,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET/PROBE",hashset_probe,2,kno_scheme_module);
  KNO_LINK_CPRIM("HASHSET/MERGE!",hashset_merge,2,kno_scheme_module);
  KNO_LINK_CPRIM("SORT-SLOTMAP",sort_slotmap,1,kno_scheme_module);
  KNO_LINK_CPRIM("HASHTABLE-BUCKETS",hashtable_buckets,1,kno_scheme_module);
  KNO_LINK_CPRIM("HASHTABLE/MERGE",hashtable_merge,2,kno_scheme_module);

  KNO_LINK_CPRIM("PLIST->TABLE",kno_plist_to_slotmap,1,kno_scheme_module);
  KNO_LINK_CPRIM("ALIST->TABLE",kno_alist_to_slotmap,1,kno_scheme_module);
  KNO_LINK_CPRIM("BLIST->TABLE",kno_blist_to_slotmap,1,kno_scheme_module);

  KNO_LINK_ALIAS("HASHTABLE-MAX",table_max,kno_scheme_module);
  KNO_LINK_ALIAS("HASHTABLE-MAXVAL",table_maxval,kno_scheme_module);
  KNO_LINK_ALIAS("HASHTABLE-SKIM",table_skim,kno_scheme_module);

  KNO_LINK_ALIAS("HASHTABLE-INCREMENT!",table_increment,kno_scheme_module);
  KNO_LINK_ALIAS("HASHTABLE-INCREMENT-EXISTING!",table_increment_existing,
                 kno_scheme_module);
  KNO_LINK_ALIAS("HASHTABLE-MULTIPLY!",table_multiply,kno_scheme_module);
  KNO_LINK_ALIAS("HASHTABLE-MULTIPLY-EXISTING!",table_multiply_existing,
                 kno_scheme_module);
  KNO_LINK_ALIAS("HASHTABLE-MAXIMIZE!",table_maximize,kno_scheme_module);
  KNO_LINK_ALIAS("HASHTABLE-MAXIMIZE-EXISTING!",table_maximize_existing,
                 kno_scheme_module);
  KNO_LINK_ALIAS("HASHTABLE-MINIMIZE!",table_minimize,kno_scheme_module);
  KNO_LINK_ALIAS("HASHTABLE-MINIMIZE-EXISTING!",table_minimize_existing,
                 kno_scheme_module);
  KNO_LINK_ALIAS("BINDINGS->SLOTMAP",kno_blist_to_slotmap,kno_scheme_module);
}
