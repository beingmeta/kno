/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_TABLES_H
#define KNO_TABLES_H 1
#ifndef KNO_TABLES_H_INFO
#define KNO_TABLES_H_INFO "include/kno/tables.h"
#endif

KNO_EXPORT u8_condition kno_NoSuchKey;

typedef enum KNO_TABLEOP {
  kno_table_store = 0, kno_table_add = 1, kno_table_drop = 2, kno_table_default = 3,
  kno_table_increment = 4, kno_table_multiply = 5, kno_table_push = 6,
  kno_table_replace = 7, kno_table_replace_novoid = 8,
  kno_table_add_if_present = 9,
  kno_table_increment_if_present = 10,
  kno_table_multiply_if_present = 11,
  kno_table_store_noref = 12, kno_table_add_noref = 13,
  kno_table_test = 14, kno_table_haskey = 15,
  kno_table_add_empty = 16, kno_table_add_empty_noref = 17,
  kno_table_maximize = 18, kno_table_maximize_if_present = 19,
  kno_table_minimize = 20, kno_table_minimize_if_present = 21}
 kno_tableop;

typedef lispval (*kno_table_get_fn)(lispval,lispval,lispval);
typedef int (*kno_table_test_fn)(lispval,lispval,lispval);
typedef int (*kno_table_add_fn)(lispval,lispval,lispval);
typedef int (*kno_table_drop_fn)(lispval,lispval,lispval);
typedef int (*kno_table_store_fn)(lispval,lispval,lispval);
typedef int (*kno_table_getsize_fn)(lispval);
typedef int (*kno_table_modified_fn)(lispval,int);
typedef int (*kno_table_readonly_fn)(lispval,int);
typedef int (*kno_table_tablep_fn)(lispval);
typedef lispval (*kno_table_keys_fn)(lispval);

struct KNO_TABLEFNS {
  lispval (*get)(lispval obj,lispval kno_key,lispval dflt);
  int (*store)(lispval obj,lispval kno_key,lispval value);
  int (*add)(lispval obj,lispval kno_key,lispval value);
  int (*drop)(lispval obj,lispval kno_key,lispval value);
  int (*test)(lispval obj,lispval kno_key,lispval value);
  int (*readonly)(lispval obj,int op);
  int (*modified)(lispval obj,int op);
  int (*finished)(lispval obj,int op);
  int (*getsize)(lispval obj);
  lispval (*keys)(lispval obj);
  struct KNO_KEYVAL (*keyvals)(lispval obj,int *);
  int (*tablep)(lispval obj);
};

KNO_EXPORT struct KNO_TABLEFNS *kno_tablefns[];

#define kno_read_lock_table(p)  (u8_read_lock(&((p)->table_rwlock)))
#define kno_write_lock_table(p) (u8_write_lock(&((p)->table_rwlock)))
#define kno_unlock_table(p)  (u8_rw_unlock(&((p)->table_rwlock)))

KNO_EXPORT lispval kno_get(lispval obj,lispval key,lispval dflt);
KNO_EXPORT int kno_test(lispval obj,lispval key,lispval value);
KNO_EXPORT int kno_store(lispval obj,lispval key,lispval value);
KNO_EXPORT int kno_add(lispval obj,lispval key,lispval value);
KNO_EXPORT int kno_drop(lispval obj,lispval key,lispval value);

KNO_EXPORT int kno_readonlyp(lispval arg);
KNO_EXPORT int kno_modifiedp(lispval arg);
KNO_EXPORT int kno_finishedp(lispval arg);
KNO_EXPORT int kno_set_readonly(lispval arg,int val);
KNO_EXPORT int kno_set_modified(lispval arg,int val);
KNO_EXPORT int kno_set_finished(lispval arg,int val);

KNO_EXPORT int kno_getsize(lispval arg);
KNO_EXPORT lispval kno_getkeys(lispval arg);
KNO_EXPORT lispval kno_getvalues(lispval arg);
KNO_EXPORT lispval kno_getassocs(lispval arg);

/* Operations based on the ordering of key values. */
KNO_EXPORT lispval kno_table_max(lispval table,lispval scope,lispval *maxval);
KNO_EXPORT lispval kno_table_skim(lispval table,lispval maxval,lispval scope);

KNO_EXPORT void kno_display_table(u8_output out,lispval table,lispval keys);

KNO_EXPORT int _KNO_TABLEP(lispval x);

#if KNO_EXTREME_PROFILING
#define KNO_TABLEP _KNO_TABLEP
#else
#define KNO_TABLEP(x)                                                  \
  ( (KNO_OIDP(x)) ? (1) :                                              \
    (KNO_CONSP(x)) ?                                                   \
    ( ( ( KNO_CONSPTR_TYPE(x) >= kno_slotmap_type) &&                  \
        ( KNO_CONSPTR_TYPE(x) <= kno_hashset_type) ) ||                 \
      ( (kno_tablefns[KNO_CONSPTR_TYPE(x)] != NULL ) &&                 \
        ( (kno_tablefns[KNO_CONSPTR_TYPE(x)]->tablep == NULL ) ||       \
          (kno_tablefns[KNO_CONSPTR_TYPE(x)]->tablep(x)) ) ) ) :        \
    (KNO_IMMEDIATEP(x)) ?                                               \
    ( (kno_tablefns[KNO_IMMEDIATE_TYPE(x)] != NULL ) &&                 \
      ( (kno_tablefns[KNO_IMMEDIATE_TYPE(x)]->tablep == NULL ) ||       \
        (kno_tablefns[KNO_IMMEDIATE_TYPE(x)]->tablep(x)) ) ) :          \
    (0))
#endif
/* #define KNO_TABLEP(x) ( ((kno_tablefns[KNO_LISP_TYPE(x)])!=NULL)  */

#define KNO_INIT_SMAP_SIZE 7
#define KNO_INIT_HASH_SIZE 73

KNO_EXPORT int   kno_init_smap_size;
KNO_EXPORT int   kno_init_hash_size;

/* Slotmaps */

typedef struct KNO_KEYVAL {
  lispval kv_key, kv_val;} KNO_KEYVAL;
typedef struct KNO_KEYVAL *kno_keyval;
typedef struct KNO_KEYVAL *kno_keyvals;

typedef struct KNO_CONST_KEYVAL {
  const lispval kv_key, kv_val;} *kno_const_keyval;
typedef struct KNO_CONST_KEYVAL *kno_const_keyvals;

#define KNO_KEYVAL_LEN (sizeof(struct KNO_KEYVAL))

typedef int (*kv_valfn)(lispval,lispval,void *);
typedef int (*kno_kvfn)(struct KNO_KEYVAL *,void *);

typedef struct KNO_SLOTMAP {
  KNO_CONS_HEADER;
  int n_slots;
  int n_allocd;
  unsigned int table_readonly:1;
  unsigned int table_modified:1;
  unsigned int table_finished:1;
  unsigned int table_uselock:1;
  unsigned int sm_free_keyvals:1;
  unsigned int sm_sort_keyvals:1;
  struct KNO_KEYVAL *sm_keyvals;
  U8_RWLOCK_DECL(table_rwlock);} KNO_SLOTMAP;
typedef struct KNO_SLOTMAP *kno_slotmap;

#define KNO_SLOTMAP_LEN (sizeof(struct KNO_SLOTMAP))

#define KNO_SLOTMAPP(x) (KNO_TYPEP(x,kno_slotmap_type))
#define KNO_XSLOTMAP(x) \
  (kno_consptr(struct KNO_SLOTMAP *,x,kno_slotmap_type))
#define KNO_XSLOTMAP_SLOTS(sm) (sm->n_slots)
#define KNO_XSLOTMAP_NUSED(sm) (sm->n_slots)
#define KNO_XSLOTMAP_NALLOCATED(sm) (sm->n_allocd)
#define KNO_XSLOTMAP_KEYVALS(sm) ((sm)->sm_keyvals)
#define KNO_XSLOTMAP_USELOCKP(sm) (sm->table_uselock)
#define KNO_XSLOTMAP_READONLYP(sm) (sm->table_readonly)
#define KNO_XSLOTMAP_MODIFIEDP(sm) (sm->table_modified)
#define KNO_XSLOTMAP_FINISHEDP(sm) (sm->table_finished)

#define KNO_XSLOTMAP_SET_READONLY(sm) (sm)->table_readonly = 1
#define KNO_XSLOTMAP_CLEAR_READONLY(sm) (sm)->table_readonly = 0
#define KNO_XSLOTMAP_MARK_MODIFIED(sm) (sm)->table_modified = 1
#define KNO_XSLOTMAP_CLEAR_MODIFIED(sm) (sm)->table_modified = 0
#define KNO_XSLOTMAP_MARK_FINISHED(sm) (sm)->table_finished = 1
#define KNO_XSLOTMAP_CLEAR_FINISHED(sm) (sm)->table_finished = 0
#define KNO_XSLOTMAP_SET_NSLOTS(sm,sz) (sm)->n_slots = sz
#define KNO_XSLOTMAP_SET_NALLOCATED(sm,sz) (sm)->n_allocd = sz

#define KNO_SLOTMAP_NSLOTS(x) (KNO_XSLOTMAP_NUSED(KNO_XSLOTMAP(x)))
#define KNO_SLOTMAP_NUSED(x) (KNO_XSLOTMAP_NUSED(KNO_XSLOTMAP(x)))
#define KNO_SLOTMAP_READONLYP(x) (KNO_XSLOTMAP_READONLYP(KNO_XSLOTMAP(x)))
#define KNO_SLOTMAP_MODIFIEDP(x) (KNO_XSLOTMAP_MODIFIEDP(KNO_XSLOTMAP(x)))
#define KNO_SLOTMAP_FINISHEDP(x) (KNO_XSLOTMAP_FINISHEDP(KNO_XSLOTMAP(x)))
#define KNO_SLOTMAP_USELOCKP(x) (KNO_XSLOTMAP_USELOCKP(KNO_XSLOTMAP(x)))
#define KNO_SLOTMAP_SET_READONLY(x) \
  KNO_XSLOTMAP_SET_READONLY(KNO_XSLOTMAP(x))
#define KNO_SLOTMAP_CLEAR_READONLY(x) \
  KNO_XSLOTMAP_CLEAR_READONLY(KNO_XSLOTMAP(x))
#define KNO_SLOTMAP_MARK_MODIFIED(x) \
  KNO_XSLOTMAP_MARK_MODIFIED(KNO_XSLOTMAP(x))
#define KNO_SLOTMAP_CLEAR_MODIFIED(x) \
  KNO_XSLOTMAP_CLEAR_MODIFIED(KNO_XSLOTMAP(x))
#define KNO_SLOTMAP_MARK_FINISHED(x) \
  KNO_XSLOTMAP_MARK_FINISHED(KNO_XSLOTMAP(x))
#define KNO_SLOTMAP_CLEAR_FINISHED(x) \
  KNO_XSLOTMAP_CLEAR_FINISHED(KNO_XSLOTMAP(x))

#define KNO_SLOTMAP_KEYVALS(sm) (KNO_XSLOTMAP_KEYVALS(KNO_XSLOTMAP(sm)))

#define kno_slotmap_size(x) (KNO_XSLOTMAP_NUSED(KNO_XSLOTMAP(x)))
#define kno_slotmap_modifiedp(x) (KNO_XSLOTMAP_MODIFIEDP(KNO_XSLOTMAP(x)))

KNO_EXPORT lispval kno_init_slotmap
  (struct KNO_SLOTMAP *ptr,int len,struct KNO_KEYVAL *data);
KNO_EXPORT int kno_slotmap_store
  (struct KNO_SLOTMAP *sm,lispval key,lispval value);
KNO_EXPORT int kno_slotmap_add
  (struct KNO_SLOTMAP *sm,lispval key,lispval value);
KNO_EXPORT int kno_slotmap_drop
  (struct KNO_SLOTMAP *sm,lispval key,lispval value);
KNO_EXPORT int kno_slotmap_delete
  (struct KNO_SLOTMAP *sm,lispval key);
KNO_EXPORT lispval kno_slotmap_keys(struct KNO_SLOTMAP *sm);
KNO_EXPORT lispval kno_slotmap_assocs(struct KNO_SLOTMAP *sm);
KNO_EXPORT int kno_sort_slotmap(lispval slotmap,int sorted);

#define kno_empty_slotmap() (kno_init_slotmap(NULL,0,NULL))

KNO_EXPORT lispval kno_make_slotmap(int space,int len,struct KNO_KEYVAL *data);
KNO_EXPORT void kno_reset_slotmap(struct KNO_SLOTMAP *ptr);
KNO_EXPORT int kno_copy_slotmap(struct KNO_SLOTMAP *src,struct KNO_SLOTMAP *dest);

KNO_EXPORT lispval kno_slotmap_max
  (struct KNO_SLOTMAP *sm,lispval scope,lispval *maxvalp);

KNO_EXPORT struct KNO_KEYVAL *kno_keyvec_insert
(lispval key,struct KNO_KEYVAL **kvp,
 int *sizep,int *spacep,int limit,
 int freedata);
KNO_EXPORT struct KNO_KEYVAL *_kno_keyvec_get
   (lispval key,struct KNO_KEYVAL *keyvals,int size);

KNO_EXPORT struct KNO_KEYVAL *kno_sortvec_insert
(lispval key,struct KNO_KEYVAL **kvp,
 int *sizep,int *spacep,int limit,
 int freedata);
KNO_EXPORT struct KNO_KEYVAL *_kno_sortvec_get
   (lispval key,struct KNO_KEYVAL *keyvals,int size);


#if KNO_INLINE_TABLES
static struct KNO_KEYVAL *kno_keyvec_get
   (lispval key,struct KNO_KEYVAL *keyvals,int size)
{
  const struct KNO_KEYVAL *scan = keyvals, *limit = scan+size;
  if (KNO_ATOMICP(key))
    while (scan<limit)
      if (scan->kv_key == key)
	return (struct KNO_KEYVAL *)scan;
      else scan++;
  else while (scan<limit)
	 if (LISP_EQUAL(scan->kv_key,key))
	   return (struct KNO_KEYVAL *) scan;
	 else scan++;
  return NULL;
}
static U8_MAYBE_UNUSED struct KNO_KEYVAL *kno_sortvec_get
   (lispval key,struct KNO_KEYVAL *keyvals,int size)
{
  if (size<4)
    return kno_keyvec_get(key,keyvals,size);
  else {
    int found = 0;
    const struct KNO_KEYVAL
      *bottom = keyvals, *limit = bottom+size, *top = limit-1, *middle;
    if (KNO_ATOMICP(key))
      while (top>=bottom) {
	middle = bottom+(top-bottom)/2;
	if (middle>=limit) break;
	else if (key == middle->kv_key) {found = 1; break;}
	else if (KNO_CONSP(middle->kv_key)) top = middle-1;
	else if (key<middle->kv_key) top = middle-1;
	else bottom = middle+1;}
    else while (top>=bottom) {
      int comparison;
      middle = bottom+(top-bottom)/2;
      if (middle>=limit) break;
      comparison = cons_compare(key,middle->kv_key);
      if (comparison==0) {found = 1; break;}
      else if (comparison<0) top = middle-1;
      else bottom = middle+1;}
    if (found)
      return (struct KNO_KEYVAL *) middle;
    else return NULL;}
}
#else
#define kno_keyvec_get _kno_keyvec_get
#define kno_sortvec_get _kno_sortvec_get
#endif

KNO_EXPORT lispval _kno_slotmap_get
  (struct KNO_SLOTMAP *sm,lispval key,lispval dflt);
KNO_EXPORT lispval _kno_slotmap_test
  (struct KNO_SLOTMAP *sm,lispval key,lispval val);
KNO_EXPORT void kno_free_slotmap(struct KNO_SLOTMAP *sm);
KNO_EXPORT void kno_free_keyvals(struct KNO_KEYVAL *kvals,int n_kvals);

#if KNO_INLINE_TABLES
static U8_MAYBE_UNUSED lispval kno_slotmap_get
  (struct KNO_SLOTMAP *sm,lispval key,lispval dflt)
{
  unsigned int unlock = 0;
  struct KNO_KEYVAL *result; int size;
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_slotmap_type);
  if ((KNO_XSLOTMAP_USELOCKP(sm))&&
      (!(KNO_XSLOTMAP_READONLYP(sm)))) {
    u8_read_lock(&sm->table_rwlock);
    unlock = 1;}
  size = KNO_XSLOTMAP_NUSED(sm);
  result = kno_keyvec_get(key,sm->sm_keyvals,size);
  if (result) {
    lispval v = result->kv_val;
    if (KNO_PRECHOICEP(v))
      v = kno_make_simple_choice(v);
    else if (KNO_CONSP(v))
      v = kno_incref(v);
    else NO_ELSE;
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return v;}
  else {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return kno_incref(dflt);}
}
static U8_MAYBE_UNUSED lispval kno_slotmap_test
  (struct KNO_SLOTMAP *sm,lispval key,lispval val)
{
  unsigned int unlock = 0;
  struct KNO_KEYVAL *result; int size;
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_slotmap_type);
  if ((KNO_ABORTP(val))) return kno_interr(val);
  if ((KNO_ABORTP(key))) return kno_interr(key);
  if ((KNO_XSLOTMAP_USELOCKP(sm))&&
      (!(KNO_XSLOTMAP_READONLYP(sm)))) {
    u8_read_lock(&sm->table_rwlock); unlock = 1;}
  size = KNO_XSLOTMAP_NUSED(sm);
  result = kno_keyvec_get(key,sm->sm_keyvals,size);
  if (result) {
    lispval current = result->kv_val; int cmp;
    if (KNO_VOIDP(val)) cmp = 1;
    else if (KNO_EQ(val,current)) cmp = 1;
    else if ((KNO_CHOICEP(val)) || (KNO_PRECHOICEP(val)) ||
	     (KNO_CHOICEP(current)) || (KNO_PRECHOICEP(current)))
      cmp = kno_overlapp(val,current);
    else if (KNO_EQUAL(val,current)) cmp = 1;
    else cmp = 0;
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return cmp;}
  else {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return 0;}
}
#else
#define kno_slotmap_get _kno_slotmap_get
#define kno_slotmap_test _kno_slotmap_test
#endif

KNO_EXPORT lispval kno_plist_to_slotmap(lispval plist);
KNO_EXPORT lispval kno_alist_to_slotmap(lispval alist);
KNO_EXPORT lispval kno_blist_to_slotmap(lispval binding_list);

/* Schemamaps */

typedef struct KNO_SCHEMAP {
  KNO_CONS_HEADER;
  int schema_length;
  unsigned int table_readonly:1;
  unsigned int table_modified:1;
  unsigned int table_finished:1;
  unsigned int table_uselock:1;
  unsigned int schemap_sorted:1;
  unsigned int schemap_onstack:1;
  unsigned int schemap_tagged:1;
  unsigned int schemap_shared:1;
  unsigned int schemap_stackvals:1;
  unsigned int schemap_stackvec:1;
  lispval *table_schema, *schema_values;
  lispval schemap_template;
  U8_RWLOCK_DECL(table_rwlock);} KNO_SCHEMAP;

#define KNO_SCHEMAP_SORTED 1
#define KNO_SCHEMAP_PRIVATE 2
#define KNO_SCHEMAP_MODIFIED 4
#define KNO_SCHEMAP_STACK_SCHEMA 8
#define KNO_SCHEMAP_READONLY 16
/* This disallows the addition or removal of fields */
#define KNO_SCHEMAP_FIXED 32
/* Tagged schemaps keep an integer ID in their n+1st element */
#define KNO_SCHEMAP_TAGGED 64
#define KNO_SCHEMAP_INLINE 128
#define KNO_SCHEMAP_COPY_SCHEMA 256

#define KNO_SCHEMAP_LEN (sizeof(struct KNO_SCHEMAP))

typedef struct KNO_SCHEMAP *kno_schemap;

#define KNO_SCHEMAPP(x) (KNO_TYPEP(x,kno_schemap_type))
#define KNO_XSCHEMAP(x) (kno_consptr(struct KNO_SCHEMAP *,x,kno_schemap_type))
#define KNO_XSCHEMAP_SIZE(sm) ((sm)->schema_length)
#define KNO_XSCHEMAP_SORTEDP(sm) ((sm)->schemap_sorted)
#define KNO_XSCHEMAP_READONLYP(sm) ((sm)->table_readonly)
#define KNO_XSCHEMAP_USELOCKP(sm) ((sm)->table_uselock)
#define KNO_XSCHEMAP_SET_READONLY(sm) (sm)->table_readonly = 1
#define KNO_XSCHEMAP_CLEAR_READONLY(sm) (sm)->table_readonly = 0
#define KNO_XSCHEMAP_MODIFIEDP(sm) ((sm)->table_modified)
#define KNO_XSCHEMAP_MARK_MODIFIED(sm) (sm)->table_modified = 1
#define KNO_XSCHEMAP_CLEAR_MODIFIED(sm) (sm)->table_modified = 0
#define KNO_XSCHEMAP_FINISHEDP(sm) ((sm)->table_finished)
#define KNO_XSCHEMAP_MARK_FINISHED(sm) (sm)->table_finished = 1
#define KNO_XSCHEMAP_CLEAR_FINISHED(sm) (sm)->table_finished = 0

#define KNO_SCHEMAP_SIZE(x) (KNO_XSCHEMAP_SIZE(KNO_XSCHEMAP(x)))
#define KNO_SCHEMAP_SORTEDP(x) (KNO_XSCHEMAP_SORTEDP(KNO_XSCHEMAP(x)))
#define KNO_SCHEMAP_READONLYP(x) (KNO_XSCHEMAP_READONLYP(KNO_XSCHEMAP(x)))
#define KNO_SCHEMAP_MODIFIEDP(x) (KNO_XSCHEMAP_MODIFIEDP(KNO_XSCHEMAP(x)))
#define KNO_SCHEMAP_FINISHEDP(x) (KNO_XSCHEMAP_FINISHEDP(KNO_XSCHEMAP(x)))

#define KNO_SCHEMAP_SET_READONLY(x) \
  KNO_XSCHEMAP_SET_READONLY(KNO_XSCHEMAP(x))
#define KNO_SCHEMAP_CLEAR_READONLY(x) \
  KNO_XSCHEMAP_CLEAR_READONLY(KNO_XSCHEMAP(x))
#define KNO_SCHEMAP_MARK_MODIFIED(x) \
  KNO_XSCHEMAP_MARK_MODIFIED(KNO_XSCHEMAP(x))
#define KNO_SCHEMAP_CLEAR_MODIFIED(x) \
  KNO_XSCHEMAP_CLEAR_MODIFIED(KNO_XSCHEMAP(x))
#define KNO_SCHEMAP_MARK_FINISHED(x) \
  KNO_XSCHEMAP_MARK_FINISHED(KNO_XSCHEMAP(x))
#define KNO_SCHEMAP_CLEAR_FINISHED(x) \
  KNO_XSCHEMAP_CLEAR_FINISHED(KNO_XSCHEMAP(x))

KNO_EXPORT lispval kno_make_schemap
  (struct KNO_SCHEMAP *ptr,int n_slots,int flags,
   lispval *schema,lispval *values);
KNO_EXPORT lispval kno_init_schemap
  (struct KNO_SCHEMAP *ptr,int n_keyvals,
   struct KNO_KEYVAL *init);
KNO_EXPORT void kno_reset_schemap(struct KNO_SCHEMAP *ptr);

KNO_EXPORT lispval _kno_schemap_get
  (struct KNO_SCHEMAP *sm,lispval key,lispval dflt);
KNO_EXPORT lispval _kno_schemap_test
  (struct KNO_SCHEMAP *sm,lispval key,lispval val);
KNO_EXPORT int kno_schemap_store
  (struct KNO_SCHEMAP *sm,lispval key,lispval value);
KNO_EXPORT int kno_schemap_add
  (struct KNO_SCHEMAP *sm,lispval key,lispval value);
KNO_EXPORT int kno_schemap_drop
  (struct KNO_SCHEMAP *sm,lispval key,lispval value);
KNO_EXPORT lispval kno_schemap_keys(struct KNO_SCHEMAP *ht);
KNO_EXPORT lispval kno_schemap_assocs(struct KNO_SCHEMAP *ht);
KNO_EXPORT lispval *kno_register_schema(int n,lispval *v);
KNO_EXPORT void kno_sort_schema(int n,lispval *v);

#if KNO_INLINE_TABLES
static U8_MAYBE_UNUSED int _kno_get_slotno
  (lispval key,lispval *schema,int size,int sorted)
{
  if ((sorted) && (size>7)) {
    const lispval *bottom = schema, *middle = bottom+size/2;
    const lispval *hard_top = bottom+size, *top = hard_top;
    while (top>bottom) {
      if (key== *middle) return middle-schema;
      else if (key<*middle) {
	top = middle-1; middle = bottom+(top-bottom)/2;}
      else {
	bottom = middle+1; middle = bottom+(top-bottom)/2;}}
    if ((middle) && (middle<hard_top) && (key== *middle))
      return middle-schema;
    else return -1;}
  else {
    const lispval *scan = schema, *limit = schema+size;
    while (scan<limit)
      if (KNO_EQ(key,*scan)) return scan-schema;
      else scan++;
    return -1;}
}
static U8_MAYBE_UNUSED lispval kno_schemap_get
  (struct KNO_SCHEMAP *sm,lispval key,lispval dflt)
{
  int unlock = 0;
  int size, slotno, sorted;
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_schemap_type);
  if ( (!(KNO_XSCHEMAP_READONLYP(sm))) &&
       (KNO_XSCHEMAP_USELOCKP(sm))) {
    u8_read_lock(&(sm->table_rwlock));
    unlock = 1;}
  size = KNO_XSCHEMAP_SIZE(sm);
  sorted = KNO_XSCHEMAP_SORTEDP(sm);
  slotno=_kno_get_slotno(key,sm->table_schema,size,sorted);
  if (slotno>=0) {
    lispval v = sm->schema_values[slotno];
    if (KNO_PRECHOICEP(v))
      v = kno_make_simple_choice(v);
    else if (KNO_CONSP(v))
      v = kno_incref(v);
    else NO_ELSE;
    if (unlock) u8_rw_unlock(&(sm->table_rwlock));
    return v;}
  else {
    if (unlock) u8_rw_unlock(&(sm->table_rwlock));
    return kno_incref(dflt);}
}
static U8_MAYBE_UNUSED lispval kno_schemap_test
  (struct KNO_SCHEMAP *sm,lispval key,lispval val)
{
  int unlock = 0, size, slotno;
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_schemap_type);
  if ((KNO_ABORTP(val)))
    return kno_interr(val);
  if ( (!(KNO_XSCHEMAP_READONLYP(sm))) &&
       (KNO_XSCHEMAP_USELOCKP(sm)) ) {
    u8_read_lock(&(sm->table_rwlock));
    unlock = 1;}
  size = KNO_XSCHEMAP_SIZE(sm);
  slotno=_kno_get_slotno(key,sm->table_schema,size,sm->schemap_sorted);
  if (slotno>=0) {
    lispval current = sm->schema_values[slotno]; int cmp;
    if (KNO_VOIDP(val)) cmp = 1;
    else if (KNO_EQ(val,current)) cmp = 1;
    else if ((KNO_CHOICEP(val)) || (KNO_PRECHOICEP(val)) ||
	     (KNO_CHOICEP(current)) || (KNO_PRECHOICEP(current)))
      cmp = kno_overlapp(val,current);
    else if (KNO_EQUAL(val,current)) cmp = 1;
    else cmp = 0;
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return cmp;}
  else {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return 0;}
}
#else
#define kno_schemap_get _kno_schemap_get
#define kno_schemap_test _kno_schemap_test
#endif

/* Hashtables */

#define KNO_HASH_BIGTHRESH 0x4000000

KNO_EXPORT unsigned int kno_hash_bigthresh;

typedef struct KNO_HASH_BUCKET {
  int bucket_len;
  struct KNO_KEYVAL kv_val0;} KNO_HASH_BUCKET;
typedef struct KNO_HASH_BUCKET *kno_hash_bucket;

typedef struct KNO_HASHTABLE {
  KNO_CONS_HEADER;
  unsigned int table_n_keys;
  unsigned int table_readonly:1;
  unsigned int table_modified:1;
  unsigned int table_finished:1;
  unsigned int table_uselock:1;

  unsigned int ht_big_buckets:1;
  unsigned int ht_n_buckets;
  double table_load_factor;
  struct KNO_HASH_BUCKET **ht_buckets;
  U8_RWLOCK_DECL(table_rwlock);} KNO_HASHTABLE;
typedef struct KNO_HASHTABLE *kno_hashtable;

#define KNO_HASHTABLE_LEN (sizeof(struct KNO_HASHTABLE))

#define KNO_HASHTABLEP(x) (KNO_TYPEP(x,kno_hashtable_type))
#define KNO_XHASHTABLE(x) \
  kno_consptr(struct KNO_HASHTABLE *,x,kno_hashtable_type)

#define KNO_HASHTABLE_NBUCKETS(x) \
  ((KNO_XHASHTABLE(x))->ht_n_buckets)
#define KNO_HASHTABLE_NKEYS(x) \
  ((KNO_XHASHTABLE(x))->table_n_keys)
#define KNO_HASHTABLE_READONLYP(x) \
  ((KNO_XHASHTABLE(x))->table_readonly)
#define KNO_HASHTABLE_SET_READONLY(x) \
  ((KNO_XHASHTABLE(x))->table_readonly) = 1
#define KNO_HASHTABLE_CLEAR_READONLY(x) \
  ((KNO_XHASHTABLE(x))->table_readonly) = 0
#define KNO_HASHTABLE_MODIFIEDP(x) \
  ((KNO_XHASHTABLE(x))->table_modified)
#define KNO_HASHTABLE_MARK_MODIFIED(x) \
  ((KNO_XHASHTABLE(x))->table_modified) = 1
#define KNO_HASHTABLE_CLEAR_MODIFIED(x) \
  ((KNO_XHASHTABLE(x))->table_modified) = 0
#define KNO_HASHTABLE_FINISHEDP(x) \
  ((KNO_XHASHTABLE(x))->table_finished)
#define KNO_HASHTABLE_MARK_FINISHED(x) \
  ((KNO_XHASHTABLE(x))->table_finished) = 1
#define KNO_HASHTABLE_CLEAR_FINISHED(x) \
  ((KNO_XHASHTABLE(x))->table_finished) = 0

#define KNO_XHASHTABLE_NBUCKETS(x)          ((x)->ht_n_buckets)
#define KNO_XHASHTABLE_NKEYS(x)           ((x)->table_n_keys)
#define KNO_XHASHTABLE_READONLYP(x)      ((x)->table_readonly)
#define KNO_XHASHTABLE_SET_READONLY(x)   ((x)->table_readonly) = 1
#define KNO_XHASHTABLE_CLEAR_READONLY(x) ((x)->table_readonly) = 0
#define KNO_XHASHTABLE_MODIFIEDP(x)      ((x)->table_modified)
#define KNO_XHASHTABLE_MARK_MODIFIED(x)  ((x)->table_modified) = 1
#define KNO_XHASHTABLE_CLEAR_MODIFIED(x) ((x)->table_modified) = 0
#define KNO_XHASHTABLE_FINISHEDP(x)      ((x)->table_finished)
#define KNO_XHASHTABLE_MARK_FINISHED(x)  ((x)->table_finished) = 1
#define KNO_XHASHTABLE_CLEAR_FINISHED(x) ((x)->table_finished) = 0

KNO_EXPORT unsigned int kno_get_hashtable_size(unsigned int min);
KNO_EXPORT unsigned int kno_hash_bytes(u8_string string,int len);
KNO_EXPORT unsigned int kno_hash_lisp(lispval x);

KNO_EXPORT lispval kno_make_hashtable(kno_hashtable ptr,int n_slots);
KNO_EXPORT lispval kno_init_hashtable(kno_hashtable ptr,int n_keyvals,
                                    struct KNO_KEYVAL *init);
KNO_EXPORT lispval kno_initialize_hashtable(kno_hashtable ptr,
                                          struct KNO_KEYVAL *init,
                                          int n_keyvals);
KNO_EXPORT int kno_reset_hashtable(kno_hashtable ht,int n_slots,int lock);
KNO_EXPORT struct KNO_KEYVAL *kno_hashvec_get(lispval,struct KNO_HASH_BUCKET **,int);
KNO_EXPORT int kno_fast_reset_hashtable
(kno_hashtable,int,int,struct KNO_HASH_BUCKET ***,int *,int *);
KNO_EXPORT int kno_swap_hashtable
(struct KNO_HASHTABLE *src,struct KNO_HASHTABLE *dest,
 int n_keys,int locked);

KNO_EXPORT int kno_remove_deadwood
(struct KNO_HASHTABLE *ptr,
 int (*testfn)(lispval,lispval,void *),
 void *testdata);
KNO_EXPORT int kno_devoid_hashtable(kno_hashtable ht,int locked);
KNO_EXPORT int kno_static_hashtable(struct KNO_HASHTABLE *ptr,int);

KNO_EXPORT struct KNO_HASHTABLE *
kno_copy_hashtable(KNO_HASHTABLE *dest,KNO_HASHTABLE *src,int locksrc);

KNO_EXPORT int kno_hashtable_op
   (kno_hashtable ht,kno_tableop op,lispval key,lispval value);
KNO_EXPORT int kno_hashtable_op_nolock
   (kno_hashtable ht,kno_tableop op,lispval key,lispval value);
KNO_EXPORT int kno_hashtable_iter
   (kno_hashtable ht,kno_tableop op,int n,
    const lispval *keys,const lispval *values);
KNO_EXPORT int kno_hashtable_iter_kv
(struct KNO_HASHTABLE *ht,kno_tableop op,
 struct KNO_CONST_KEYVAL *kvals,int n,
 int lock);
KNO_EXPORT int kno_hashtable_iterkeys
   (kno_hashtable ht,kno_tableop op,int n,
    const lispval *keys,lispval value);
KNO_EXPORT int kno_hashtable_itervalues
   (kno_hashtable ht,kno_tableop op,int n,
    lispval key,const lispval *values);

KNO_EXPORT lispval kno_hashtable_get
   (kno_hashtable ht,lispval key,lispval dflt);
KNO_EXPORT lispval kno_hashtable_get_nolock
   (struct KNO_HASHTABLE *ht,lispval key,lispval dflt);
KNO_EXPORT lispval kno_hashtable_get_noref
   (struct KNO_HASHTABLE *ht,lispval key,lispval dflt);
KNO_EXPORT lispval kno_hashtable_get_nolockref
   (struct KNO_HASHTABLE *ht,lispval key,lispval dflt);
KNO_EXPORT int kno_hashtable_store(kno_hashtable ht,lispval key,lispval value);
KNO_EXPORT int kno_hashtable_add(kno_hashtable ht,lispval key,lispval value);
KNO_EXPORT int kno_hashtable_drop(kno_hashtable ht,lispval key,lispval value);

KNO_EXPORT int kno_hashtable_probe(kno_hashtable ht,lispval key);
KNO_EXPORT int kno_hashtable_probe_novoid(kno_hashtable ht,lispval key);

KNO_EXPORT lispval kno_hashtable_keys(kno_hashtable ht);
KNO_EXPORT struct KNO_KEYVAL *kno_hashtable_keyvals
  (kno_hashtable ht,int *sizep,int lock);
KNO_EXPORT int kno_for_hashtable
  (kno_hashtable ht,kv_valfn f,void *data,int lock);
KNO_EXPORT int kno_for_hashtable_kv
  (struct KNO_HASHTABLE *ht,kno_kvfn f,void *data,int lock);
KNO_EXPORT lispval kno_hashtable_assocs(kno_hashtable ht);

KNO_EXPORT lispval kno_hashtable_max(kno_hashtable,lispval,lispval *);
KNO_EXPORT lispval kno_hashtable_skim(kno_hashtable,lispval,lispval);

KNO_EXPORT lispval kno_slotmap_max(kno_slotmap,lispval,lispval *);
KNO_EXPORT lispval kno_slotmap_skim(kno_slotmap,lispval,lispval);

KNO_EXPORT long long kno_hashtable_map_size(struct KNO_HASHTABLE *h);

KNO_EXPORT int kno_resize_hashtable(kno_hashtable ptr,int n_slots);
KNO_EXPORT int kno_hashtable_stats
  (kno_hashtable ptr,
   int *n_slots,int *n_keys,
   int *n_buckets,int *n_collisions,int *max_bucket,
   int *n_vals,int *max_vals);

KNO_EXPORT void kno_hash_quality
  (unsigned int *hashv,int n_keys,int n_slots,
   unsigned int *buf,unsigned int bufsiz,
   unsigned int *nbucketsp,unsigned int *maxbucketp,
   unsigned int *ncollisionsp);

KNO_EXPORT int kno_recycle_hashtable(struct KNO_HASHTABLE *h);
KNO_EXPORT int kno_free_buckets(struct KNO_HASH_BUCKET **slots,
                              int slots_to_free,
                              int isbig);

KNO_EXPORT void kno_free_keyvals(struct KNO_KEYVAL *keyvals,int n_keyvals);

KNO_EXPORT int kno_hashtable_set_readonly(KNO_HASHTABLE *ht,int readonly);

/* Hashsets */

typedef struct KNO_HASHSET {
  KNO_CONS_HEADER;
  int hs_n_elts, hs_n_buckets;
  char hs_modified;
  char table_uselock;
  double hs_load_factor;
  lispval *hs_buckets;
  U8_RWLOCK_DECL(table_rwlock);} KNO_HASHSET;
typedef struct KNO_HASHSET *kno_hashset;

#define KNO_HASHSET_LEN (sizeof(struct KNO_HASHSET))

#define KNO_HASHSETP(x) (KNO_TYPEP(x,kno_hashset_type))
#define KNO_XHASHSET(x) \
  kno_consptr(struct KNO_HASHSET *,x,kno_hashset_type)

KNO_EXPORT int kno_hashset_get(kno_hashset h,lispval key);
KNO_EXPORT int kno_hashset_mod(kno_hashset h,lispval key,int add);
KNO_EXPORT void kno_hashset_add_raw(kno_hashset h,lispval key);
KNO_EXPORT int kno_hashset_add(kno_hashset h,lispval keys);
#define kno_hashset_drop(h,key) kno_hashset_mod(h,key,0)
KNO_EXPORT lispval kno_hashset_intern(kno_hashset h,lispval key,int add);

KNO_EXPORT lispval kno_hashset_elts(kno_hashset h,int clean);

KNO_EXPORT void kno_init_hashset(kno_hashset h,int n,int stack_cons);
KNO_EXPORT lispval kno_make_hashset(void);
KNO_EXPORT ssize_t kno_grow_hashset(kno_hashset h,ssize_t size);
KNO_EXPORT lispval kno_copy_hashset(KNO_HASHSET *nptr,KNO_HASHSET *ptr);
KNO_EXPORT int kno_recycle_hashset(struct KNO_HASHSET *h);
KNO_EXPORT int kno_reset_hashset(kno_hashset);

#endif /* KNO_TABLES_H */

