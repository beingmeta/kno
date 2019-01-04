/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_TABLES_H
#define FRAMERD_TABLES_H 1
#ifndef FRAMERD_TABLES_H_INFO
#define FRAMERD_TABLES_H_INFO "include/framerd/tables.h"
#endif

FD_EXPORT u8_condition fd_NoSuchKey;

typedef enum FD_TABLEOP {
  fd_table_store = 0, fd_table_add = 1, fd_table_drop = 2, fd_table_default = 3,
  fd_table_increment = 4, fd_table_multiply = 5, fd_table_push = 6,
  fd_table_replace = 7, fd_table_replace_novoid = 8,
  fd_table_add_if_present = 9,
  fd_table_increment_if_present = 10,
  fd_table_multiply_if_present = 11,
  fd_table_store_noref = 12, fd_table_add_noref = 13,
  fd_table_test = 14, fd_table_haskey = 15,
  fd_table_add_empty = 16, fd_table_add_empty_noref = 17,
  fd_table_maximize = 18, fd_table_maximize_if_present = 19,
  fd_table_minimize = 20, fd_table_minimize_if_present = 21}
 fd_tableop;

typedef lispval (*fd_table_get_fn)(lispval,lispval,lispval);
typedef int (*fd_table_test_fn)(lispval,lispval,lispval);
typedef int (*fd_table_add_fn)(lispval,lispval,lispval);
typedef int (*fd_table_drop_fn)(lispval,lispval,lispval);
typedef int (*fd_table_store_fn)(lispval,lispval,lispval);
typedef int (*fd_table_getsize_fn)(lispval);
typedef int (*fd_table_modified_fn)(lispval,int);
typedef int (*fd_table_readonly_fn)(lispval,int);
typedef lispval (*fd_table_keys_fn)(lispval);

struct FD_TABLEFNS {
  lispval (*get)(lispval obj,lispval fd_key,lispval dflt);
  int (*store)(lispval obj,lispval fd_key,lispval value);
  int (*add)(lispval obj,lispval fd_key,lispval value);
  int (*drop)(lispval obj,lispval fd_key,lispval value);
  int (*test)(lispval obj,lispval fd_key,lispval value);
  int (*readonly)(lispval obj,int op);
  int (*modified)(lispval obj,int op);
  int (*finished)(lispval obj,int op);
  int (*getsize)(lispval obj);
  lispval (*keys)(lispval obj);
  struct FD_KEYVAL (*keyvals)(lispval obj,int *);
};

FD_EXPORT struct FD_TABLEFNS *fd_tablefns[];

#define fd_read_lock_table(p)  (u8_read_lock(&((p)->table_rwlock)))
#define fd_write_lock_table(p) (u8_write_lock(&((p)->table_rwlock)))
#define fd_unlock_table(p)  (u8_rw_unlock(&((p)->table_rwlock)))

FD_EXPORT lispval fd_get(lispval obj,lispval key,lispval dflt);
FD_EXPORT int fd_test(lispval obj,lispval key,lispval value);
FD_EXPORT int fd_store(lispval obj,lispval key,lispval value);
FD_EXPORT int fd_add(lispval obj,lispval key,lispval value);
FD_EXPORT int fd_drop(lispval obj,lispval key,lispval value);

FD_EXPORT int fd_readonlyp(lispval arg);
FD_EXPORT int fd_modifiedp(lispval arg);
FD_EXPORT int fd_finishedp(lispval arg);
FD_EXPORT int fd_set_readonly(lispval arg,int val);
FD_EXPORT int fd_set_modified(lispval arg,int val);
FD_EXPORT int fd_set_finished(lispval arg,int val);

FD_EXPORT int fd_getsize(lispval arg);
FD_EXPORT lispval fd_getkeys(lispval arg);
FD_EXPORT lispval fd_getvalues(lispval arg);
FD_EXPORT lispval fd_getassocs(lispval arg);

/* Operations based on the ordering of key values. */
FD_EXPORT lispval fd_table_max(lispval table,lispval scope,lispval *maxval);
FD_EXPORT lispval fd_table_skim(lispval table,lispval maxval,lispval scope);

FD_EXPORT void fd_display_table(u8_output out,lispval table,lispval keys);

#define FD_TABLEP(x) ((fd_tablefns[FD_PTR_TYPE(x)])!=NULL)

#define FD_INIT_SMAP_SIZE 7
#define FD_INIT_HASH_SIZE 73

FD_EXPORT int   fd_init_smap_size;
FD_EXPORT int   fd_init_hash_size;

/* Slotmaps */

typedef struct FD_KEYVAL {
  lispval kv_key, kv_val;} FD_KEYVAL;
typedef struct FD_KEYVAL *fd_keyval;
typedef struct FD_KEYVAL *fd_keyvals;

typedef struct FD_CONST_KEYVAL {
  const lispval kv_key, kv_val;} *fd_const_keyval;
typedef struct FD_CONST_KEYVAL *fd_const_keyvals;

#define FD_KEYVAL_LEN (sizeof(struct FD_KEYVAL))

typedef int (*kv_valfn)(lispval,lispval,void *);
typedef int (*fd_kvfn)(struct FD_KEYVAL *,void *);

typedef struct FD_SLOTMAP {
  FD_CONS_HEADER;
  int n_slots;
  int n_allocd;
  unsigned int table_readonly:1;
  unsigned int table_modified:1;
  unsigned int table_finished:1;
  unsigned int table_uselock:1;
  unsigned int sm_free_keyvals:1;
  unsigned int sm_sort_keyvals:1;
  struct FD_KEYVAL *sm_keyvals;
  U8_RWLOCK_DECL(table_rwlock);} FD_SLOTMAP;
typedef struct FD_SLOTMAP *fd_slotmap;

#define FD_SLOTMAP_LEN (sizeof(struct FD_SLOTMAP))

#define FD_SLOTMAPP(x) (FD_TYPEP(x,fd_slotmap_type))
#define FD_XSLOTMAP(x) \
  (fd_consptr(struct FD_SLOTMAP *,x,fd_slotmap_type))
#define FD_XSLOTMAP_SLOTS(sm) (sm->n_slots)
#define FD_XSLOTMAP_NUSED(sm) (sm->n_slots)
#define FD_XSLOTMAP_NALLOCATED(sm) (sm->n_allocd)
#define FD_XSLOTMAP_KEYVALS(sm) ((sm)->sm_keyvals)
#define FD_XSLOTMAP_USELOCKP(sm) (sm->table_uselock)
#define FD_XSLOTMAP_READONLYP(sm) (sm->table_readonly)
#define FD_XSLOTMAP_MODIFIEDP(sm) (sm->table_modified)
#define FD_XSLOTMAP_FINISHEDP(sm) (sm->table_finished)

#define FD_XSLOTMAP_SET_READONLY(sm) (sm)->table_readonly = 1
#define FD_XSLOTMAP_CLEAR_READONLY(sm) (sm)->table_readonly = 0
#define FD_XSLOTMAP_MARK_MODIFIED(sm) (sm)->table_modified = 1
#define FD_XSLOTMAP_CLEAR_MODIFIED(sm) (sm)->table_modified = 0
#define FD_XSLOTMAP_MARK_FINISHED(sm) (sm)->table_finished = 1
#define FD_XSLOTMAP_CLEAR_FINISHED(sm) (sm)->table_finished = 0
#define FD_XSLOTMAP_SET_NSLOTS(sm,sz) (sm)->n_slots = sz
#define FD_XSLOTMAP_SET_NALLOCATED(sm,sz) (sm)->n_allocd = sz

#define FD_SLOTMAP_NSLOTS(x) (FD_XSLOTMAP_NUSED(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_NUSED(x) (FD_XSLOTMAP_NUSED(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_READONLYP(x) (FD_XSLOTMAP_READONLYP(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_MODIFIEDP(x) (FD_XSLOTMAP_MODIFIEDP(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_FINISHEDP(x) (FD_XSLOTMAP_FINISHEDP(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_USELOCKP(x) (FD_XSLOTMAP_USELOCKP(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_SET_READONLY(x) \
  FD_XSLOTMAP_SET_READONLY(FD_XSLOTMAP(x))
#define FD_SLOTMAP_CLEAR_READONLY(x) \
  FD_XSLOTMAP_CLEAR_READONLY(FD_XSLOTMAP(x))
#define FD_SLOTMAP_MARK_MODIFIED(x) \
  FD_XSLOTMAP_MARK_MODIFIED(FD_XSLOTMAP(x))
#define FD_SLOTMAP_CLEAR_MODIFIED(x) \
  FD_XSLOTMAP_CLEAR_MODIFIED(FD_XSLOTMAP(x))
#define FD_SLOTMAP_MARK_FINISHED(x) \
  FD_XSLOTMAP_MARK_FINISHED(FD_XSLOTMAP(x))
#define FD_SLOTMAP_CLEAR_FINISHED(x) \
  FD_XSLOTMAP_CLEAR_FINISHED(FD_XSLOTMAP(x))

#define FD_SLOTMAP_KEYVALS(sm) (FD_XSLOTMAP_KEYVALS(FD_XSLOTMAP(sm)))

#define fd_slotmap_size(x) (FD_XSLOTMAP_NUSED(FD_XSLOTMAP(x)))
#define fd_slotmap_modifiedp(x) (FD_XSLOTMAP_MODIFIEDP(FD_XSLOTMAP(x)))

FD_EXPORT lispval fd_init_slotmap
  (struct FD_SLOTMAP *ptr,int len,struct FD_KEYVAL *data);
FD_EXPORT int fd_slotmap_store
  (struct FD_SLOTMAP *sm,lispval key,lispval value);
FD_EXPORT int fd_slotmap_add
  (struct FD_SLOTMAP *sm,lispval key,lispval value);
FD_EXPORT int fd_slotmap_drop
  (struct FD_SLOTMAP *sm,lispval key,lispval value);
FD_EXPORT int fd_slotmap_delete
  (struct FD_SLOTMAP *sm,lispval key);
FD_EXPORT lispval fd_slotmap_keys(struct FD_SLOTMAP *sm);
FD_EXPORT int fd_sort_slotmap(lispval slotmap,int sorted);

#define fd_empty_slotmap() (fd_init_slotmap(NULL,0,NULL))

FD_EXPORT lispval fd_make_slotmap(int space,int len,struct FD_KEYVAL *data);
FD_EXPORT void fd_reset_slotmap(struct FD_SLOTMAP *ptr);
FD_EXPORT int fd_copy_slotmap(struct FD_SLOTMAP *src,struct FD_SLOTMAP *dest);

FD_EXPORT lispval fd_slotmap_max
  (struct FD_SLOTMAP *sm,lispval scope,lispval *maxvalp);

FD_EXPORT struct FD_KEYVAL *fd_keyvec_insert
(lispval key,struct FD_KEYVAL **kvp,
 int *sizep,int *spacep,int limit,
 int freedata);
FD_EXPORT struct FD_KEYVAL *_fd_keyvec_get
   (lispval key,struct FD_KEYVAL *keyvals,int size);

FD_EXPORT struct FD_KEYVAL *fd_sortvec_insert
(lispval key,struct FD_KEYVAL **kvp,
 int *sizep,int *spacep,int limit,
 int freedata);
FD_EXPORT struct FD_KEYVAL *_fd_sortvec_get
   (lispval key,struct FD_KEYVAL *keyvals,int size);


#if FD_INLINE_TABLES
static struct FD_KEYVAL *fd_keyvec_get
   (lispval key,struct FD_KEYVAL *keyvals,int size)
{
  const struct FD_KEYVAL *scan = keyvals, *limit = scan+size;
  if (FD_ATOMICP(key))
    while (scan<limit)
      if (scan->kv_key == key)
	return (struct FD_KEYVAL *)scan;
      else scan++;
  else while (scan<limit)
	 if (LISP_EQUAL(scan->kv_key,key))
	   return (struct FD_KEYVAL *) scan;
	 else scan++;
  return NULL;
}
static U8_MAYBE_UNUSED struct FD_KEYVAL *fd_sortvec_get
   (lispval key,struct FD_KEYVAL *keyvals,int size)
{
  if (size<4)
    return fd_keyvec_get(key,keyvals,size);
  else {
    int found = 0;
    const struct FD_KEYVAL
      *bottom = keyvals, *limit = bottom+size, *top = limit-1, *middle;
    if (FD_ATOMICP(key))
      while (top>=bottom) {
	middle = bottom+(top-bottom)/2;
	if (middle>=limit) break;
	else if (key == middle->kv_key) {found = 1; break;}
	else if (FD_CONSP(middle->kv_key)) top = middle-1;
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
      return (struct FD_KEYVAL *) middle;
    else return NULL;}
}
#else
#define fd_keyvec_get _fd_keyvec_get
#define fd_sortvec_get _fd_sortvec_get
#endif

FD_EXPORT lispval _fd_slotmap_get
  (struct FD_SLOTMAP *sm,lispval key,lispval dflt);
FD_EXPORT lispval _fd_slotmap_test
  (struct FD_SLOTMAP *sm,lispval key,lispval val);
FD_EXPORT void fd_free_slotmap(struct FD_SLOTMAP *sm);
FD_EXPORT void fd_free_keyvals(struct FD_KEYVAL *kvals,int n_kvals);

#if FD_INLINE_TABLES
static U8_MAYBE_UNUSED lispval fd_slotmap_get
  (struct FD_SLOTMAP *sm,lispval key,lispval dflt)
{
  unsigned int unlock = 0;
  struct FD_KEYVAL *result; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if ((FD_XSLOTMAP_USELOCKP(sm))&&
      (!(FD_XSLOTMAP_READONLYP(sm)))) {
    u8_read_lock(&sm->table_rwlock); unlock = 1;}
  size = FD_XSLOTMAP_NUSED(sm);
  result = fd_keyvec_get(key,sm->sm_keyvals,size);
  if (result) {
    lispval v = result->kv_val;
    if (FD_PRECHOICEP(v))
      v = fd_make_simple_choice(v);
    else if (FD_CONSP(v))
      v = fd_incref(v);
    else NO_ELSE;
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return v;}
  else {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return fd_incref(dflt);}
}
static U8_MAYBE_UNUSED lispval fd_slotmap_test
  (struct FD_SLOTMAP *sm,lispval key,lispval val)
{
  unsigned int unlock = 0;
  struct FD_KEYVAL *result; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if ((FD_ABORTP(val))) return fd_interr(val);
  if ((FD_ABORTP(key))) return fd_interr(key);
  if ((FD_XSLOTMAP_USELOCKP(sm))&&
      (!(FD_XSLOTMAP_READONLYP(sm)))) {
    u8_read_lock(&sm->table_rwlock); unlock = 1;}
  size = FD_XSLOTMAP_NUSED(sm);
  result = fd_keyvec_get(key,sm->sm_keyvals,size);
  if (result) {
    lispval current = result->kv_val; int cmp;
    if (FD_VOIDP(val)) cmp = 1;
    else if (FD_EQ(val,current)) cmp = 1;
    else if ((FD_CHOICEP(val)) || (FD_PRECHOICEP(val)) ||
	     (FD_CHOICEP(current)) || (FD_PRECHOICEP(current)))
      cmp = fd_overlapp(val,current);
    else if (FD_EQUAL(val,current)) cmp = 1;
    else cmp = 0;
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return cmp;}
  else {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return 0;}
}
#else
#define fd_slotmap_get _fd_slotmap_get
#define fd_slotmap_test _fd_slotmap_test
#endif

FD_EXPORT lispval fd_plist_to_slotmap(lispval plist);
FD_EXPORT lispval fd_alist_to_slotmap(lispval alist);
FD_EXPORT lispval fd_blist_to_slotmap(lispval binding_list);

/* Schemamaps */

typedef struct FD_SCHEMAP {
  FD_CONS_HEADER;
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
  U8_RWLOCK_DECL(table_rwlock);} FD_SCHEMAP;

#define FD_SCHEMAP_SORTED 1
#define FD_SCHEMAP_PRIVATE 2
#define FD_SCHEMAP_MODIFIED 4
#define FD_SCHEMAP_STACK_SCHEMA 8
#define FD_SCHEMAP_READONLY 16
/* This disallows the addition or removal of fields */
#define FD_SCHEMAP_FIXED 32
/* Tagged schemaps keep an integer ID in their n+1st element */
#define FD_SCHEMAP_TAGGED 64
#define FD_SCHEMAP_INLINE 128
#define FD_SCHEMAP_COPY_SCHEMA 256

#define FD_SCHEMAP_LEN (sizeof(struct FD_SCHEMAP))

typedef struct FD_SCHEMAP *fd_schemap;

#define FD_SCHEMAPP(x) (FD_TYPEP(x,fd_schemap_type))
#define FD_XSCHEMAP(x) (fd_consptr(struct FD_SCHEMAP *,x,fd_schemap_type))
#define FD_XSCHEMAP_SIZE(sm) ((sm)->schema_length)
#define FD_XSCHEMAP_SORTEDP(sm) ((sm)->schemap_sorted)
#define FD_XSCHEMAP_READONLYP(sm) ((sm)->table_readonly)
#define FD_XSCHEMAP_USELOCKP(sm) ((sm)->table_uselock)
#define FD_XSCHEMAP_SET_READONLY(sm) (sm)->table_readonly = 1
#define FD_XSCHEMAP_CLEAR_READONLY(sm) (sm)->table_readonly = 0
#define FD_XSCHEMAP_MODIFIEDP(sm) ((sm)->table_modified)
#define FD_XSCHEMAP_MARK_MODIFIED(sm) (sm)->table_modified = 1
#define FD_XSCHEMAP_CLEAR_MODIFIED(sm) (sm)->table_modified = 0
#define FD_XSCHEMAP_FINISHEDP(sm) ((sm)->table_finished)
#define FD_XSCHEMAP_MARK_FINISHED(sm) (sm)->table_finished = 1
#define FD_XSCHEMAP_CLEAR_FINISHED(sm) (sm)->table_finished = 0

#define FD_SCHEMAP_SIZE(x) (FD_XSCHEMAP_SIZE(FD_XSCHEMAP(x)))
#define FD_SCHEMAP_SORTEDP(x) (FD_XSCHEMAP_SORTEDP(FD_XSCHEMAP(x)))
#define FD_SCHEMAP_READONLYP(x) (FD_XSCHEMAP_READONLYP(FD_XSCHEMAP(x)))
#define FD_SCHEMAP_MODIFIEDP(x) (FD_XSCHEMAP_MODIFIEDP(FD_XSCHEMAP(x)))
#define FD_SCHEMAP_FINISHEDP(x) (FD_XSCHEMAP_FINISHEDP(FD_XSCHEMAP(x)))

#define FD_SCHEMAP_SET_READONLY(x) \
  FD_XSCHEMAP_SET_READONLY(FD_XSCHEMAP(x))
#define FD_SCHEMAP_CLEAR_READONLY(x) \
  FD_XSCHEMAP_CLEAR_READONLY(FD_XSCHEMAP(x))
#define FD_SCHEMAP_MARK_MODIFIED(x) \
  FD_XSCHEMAP_MARK_MODIFIED(FD_XSCHEMAP(x))
#define FD_SCHEMAP_CLEAR_MODIFIED(x) \
  FD_XSCHEMAP_CLEAR_MODIFIED(FD_XSCHEMAP(x))
#define FD_SCHEMAP_MARK_FINISHED(x) \
  FD_XSCHEMAP_MARK_FINISHED(FD_XSCHEMAP(x))
#define FD_SCHEMAP_CLEAR_FINISHED(x) \
  FD_XSCHEMAP_CLEAR_FINISHED(FD_XSCHEMAP(x))

FD_EXPORT lispval fd_make_schemap
  (struct FD_SCHEMAP *ptr,int n_slots,int flags,
   lispval *schema,lispval *values);
FD_EXPORT lispval fd_init_schemap
  (struct FD_SCHEMAP *ptr,int n_keyvals,
   struct FD_KEYVAL *init);
FD_EXPORT void fd_reset_schemap(struct FD_SCHEMAP *ptr);

FD_EXPORT lispval _fd_schemap_get
  (struct FD_SCHEMAP *sm,lispval key,lispval dflt);
FD_EXPORT lispval _fd_schemap_test
  (struct FD_SCHEMAP *sm,lispval key,lispval val);
FD_EXPORT int fd_schemap_store
  (struct FD_SCHEMAP *sm,lispval key,lispval value);
FD_EXPORT int fd_schemap_add
  (struct FD_SCHEMAP *sm,lispval key,lispval value);
FD_EXPORT int fd_schemap_drop
  (struct FD_SCHEMAP *sm,lispval key,lispval value);
FD_EXPORT lispval fd_schemap_keys(struct FD_SCHEMAP *ht);
FD_EXPORT lispval *fd_register_schema(int n,lispval *v);
FD_EXPORT void fd_sort_schema(int n,lispval *v);

#if FD_INLINE_TABLES
static U8_MAYBE_UNUSED int _fd_get_slotno
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
      if (FD_EQ(key,*scan)) return scan-schema;
      else scan++;
    return -1;}
}
static U8_MAYBE_UNUSED lispval fd_schemap_get
  (struct FD_SCHEMAP *sm,lispval key,lispval dflt)
{
  int unlock = 0;
  int size, slotno, sorted;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_schemap_type);
  if ( (!(FD_XSCHEMAP_READONLYP(sm))) &&
       (FD_XSCHEMAP_USELOCKP(sm))) {
    u8_read_lock(&(sm->table_rwlock));
    unlock = 1;}
  size = FD_XSCHEMAP_SIZE(sm);
  sorted = FD_XSCHEMAP_SORTEDP(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sorted);
  if (slotno>=0) {
    lispval v = sm->schema_values[slotno];
    if (FD_PRECHOICEP(v))
      v = fd_make_simple_choice(v);
    else if (FD_CONSP(v))
      v = fd_incref(v);
    else NO_ELSE;
    if (unlock) u8_rw_unlock(&(sm->table_rwlock));
    return v;}
  else {
    if (unlock) u8_rw_unlock(&(sm->table_rwlock));
    return fd_incref(dflt);}
}
static U8_MAYBE_UNUSED lispval fd_schemap_test
  (struct FD_SCHEMAP *sm,lispval key,lispval val)
{
  int unlock = 0, size, slotno;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_schemap_type);
  if ((FD_ABORTP(val)))
    return fd_interr(val);
  if ( (!(FD_XSCHEMAP_READONLYP(sm))) &&
       (FD_XSCHEMAP_USELOCKP(sm)) ) {
    u8_read_lock(&(sm->table_rwlock));
    unlock = 1;}
  size = FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sm->schemap_sorted);
  if (slotno>=0) {
    lispval current = sm->schema_values[slotno]; int cmp;
    if (FD_VOIDP(val)) cmp = 1;
    else if (FD_EQ(val,current)) cmp = 1;
    else if ((FD_CHOICEP(val)) || (FD_PRECHOICEP(val)) ||
	     (FD_CHOICEP(current)) || (FD_PRECHOICEP(current)))
      cmp = fd_overlapp(val,current);
    else if (FD_EQUAL(val,current)) cmp = 1;
    else cmp = 0;
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return cmp;}
  else {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return 0;}
}
#else
#define fd_schemap_get _fd_schemap_get
#define fd_schemap_test _fd_schemap_test
#endif

/* Hashtables */

#define FD_HASH_BIGTHRESH 0x4000000

FD_EXPORT unsigned int fd_hash_bigthresh;

typedef struct FD_HASH_BUCKET {
  int bucket_len;
  struct FD_KEYVAL kv_val0;} FD_HASH_BUCKET;
typedef struct FD_HASH_BUCKET *fd_hash_bucket;

typedef struct FD_HASHTABLE {
  FD_CONS_HEADER;
  unsigned int table_n_keys;
  unsigned int table_readonly:1;
  unsigned int table_modified:1;
  unsigned int table_finished:1;
  unsigned int table_uselock:1;

  unsigned int ht_big_buckets:1;
  unsigned int ht_n_buckets;
  double table_load_factor;
  struct FD_HASH_BUCKET **ht_buckets;
  U8_RWLOCK_DECL(table_rwlock);} FD_HASHTABLE;
typedef struct FD_HASHTABLE *fd_hashtable;

#define FD_HASHTABLE_LEN (sizeof(struct FD_HASHTABLE))

#define FD_HASHTABLEP(x) (FD_TYPEP(x,fd_hashtable_type))
#define FD_XHASHTABLE(x) \
  fd_consptr(struct FD_HASHTABLE *,x,fd_hashtable_type)

#define FD_HASHTABLE_NBUCKETS(x) \
  ((FD_XHASHTABLE(x))->ht_n_buckets)
#define FD_HASHTABLE_NKEYS(x) \
  ((FD_XHASHTABLE(x))->table_n_keys)
#define FD_HASHTABLE_READONLYP(x) \
  ((FD_XHASHTABLE(x))->table_readonly)
#define FD_HASHTABLE_SET_READONLY(x) \
  ((FD_XHASHTABLE(x))->table_readonly) = 1
#define FD_HASHTABLE_CLEAR_READONLY(x) \
  ((FD_XHASHTABLE(x))->table_readonly) = 0
#define FD_HASHTABLE_MODIFIEDP(x) \
  ((FD_XHASHTABLE(x))->table_modified)
#define FD_HASHTABLE_MARK_MODIFIED(x) \
  ((FD_XHASHTABLE(x))->table_modified) = 1
#define FD_HASHTABLE_CLEAR_MODIFIED(x) \
  ((FD_XHASHTABLE(x))->table_modified) = 0
#define FD_HASHTABLE_FINISHEDP(x) \
  ((FD_XHASHTABLE(x))->table_finished)
#define FD_HASHTABLE_MARK_FINISHED(x) \
  ((FD_XHASHTABLE(x))->table_finished) = 1
#define FD_HASHTABLE_CLEAR_FINISHED(x) \
  ((FD_XHASHTABLE(x))->table_finished) = 0

#define FD_XHASHTABLE_NBUCKETS(x)          ((x)->ht_n_buckets)
#define FD_XHASHTABLE_NKEYS(x)           ((x)->table_n_keys)
#define FD_XHASHTABLE_READONLYP(x)      ((x)->table_readonly)
#define FD_XHASHTABLE_SET_READONLY(x)   ((x)->table_readonly) = 1
#define FD_XHASHTABLE_CLEAR_READONLY(x) ((x)->table_readonly) = 0
#define FD_XHASHTABLE_MODIFIEDP(x)      ((x)->table_modified)
#define FD_XHASHTABLE_MARK_MODIFIED(x)  ((x)->table_modified) = 1
#define FD_XHASHTABLE_CLEAR_MODIFIED(x) ((x)->table_modified) = 0
#define FD_XHASHTABLE_FINISHEDP(x)      ((x)->table_finished)
#define FD_XHASHTABLE_MARK_FINISHED(x)  ((x)->table_finished) = 1
#define FD_XHASHTABLE_CLEAR_FINISHED(x) ((x)->table_finished) = 0

FD_EXPORT unsigned int fd_get_hashtable_size(unsigned int min);
FD_EXPORT unsigned int fd_hash_bytes(u8_string string,int len);
FD_EXPORT unsigned int fd_hash_lisp(lispval x);

FD_EXPORT lispval fd_make_hashtable(fd_hashtable ptr,int n_slots);
FD_EXPORT lispval fd_init_hashtable(fd_hashtable ptr,int n_keyvals,
                                    struct FD_KEYVAL *init);
FD_EXPORT lispval fd_initialize_hashtable(fd_hashtable ptr,
                                          struct FD_KEYVAL *init,
                                          int n_keyvals);
FD_EXPORT int fd_reset_hashtable(fd_hashtable ht,int n_slots,int lock);
FD_EXPORT struct FD_KEYVAL *fd_hashvec_get(lispval,struct FD_HASH_BUCKET **,int);
FD_EXPORT int fd_fast_reset_hashtable
(fd_hashtable,int,int,struct FD_HASH_BUCKET ***,int *,int *);
FD_EXPORT int fd_swap_hashtable
(struct FD_HASHTABLE *src,struct FD_HASHTABLE *dest,
 int n_keys,int locked);

FD_EXPORT int fd_remove_deadwood
(struct FD_HASHTABLE *ptr,
 int (*testfn)(lispval,lispval,void *),
 void *testdata);
FD_EXPORT int fd_devoid_hashtable(fd_hashtable ht,int locked);
FD_EXPORT int fd_static_hashtable(struct FD_HASHTABLE *ptr,int);

FD_EXPORT struct FD_HASHTABLE *
fd_copy_hashtable(FD_HASHTABLE *dest,FD_HASHTABLE *src,int locksrc);

FD_EXPORT int fd_hashtable_op
   (fd_hashtable ht,fd_tableop op,lispval key,lispval value);
FD_EXPORT int fd_hashtable_op_nolock
   (fd_hashtable ht,fd_tableop op,lispval key,lispval value);
FD_EXPORT int fd_hashtable_iter
   (fd_hashtable ht,fd_tableop op,int n,
    const lispval *keys,const lispval *values);
FD_EXPORT int fd_hashtable_iter_kv
(struct FD_HASHTABLE *ht,fd_tableop op,
 struct FD_CONST_KEYVAL *kvals,int n,
 int lock);
FD_EXPORT int fd_hashtable_iterkeys
   (fd_hashtable ht,fd_tableop op,int n,
    const lispval *keys,lispval value);
FD_EXPORT int fd_hashtable_itervalues
   (fd_hashtable ht,fd_tableop op,int n,
    lispval key,const lispval *values);

FD_EXPORT lispval fd_hashtable_get
   (fd_hashtable ht,lispval key,lispval dflt);
FD_EXPORT lispval fd_hashtable_get_nolock
   (struct FD_HASHTABLE *ht,lispval key,lispval dflt);
FD_EXPORT lispval fd_hashtable_get_noref
   (struct FD_HASHTABLE *ht,lispval key,lispval dflt);
FD_EXPORT lispval fd_hashtable_get_nolockref
   (struct FD_HASHTABLE *ht,lispval key,lispval dflt);
FD_EXPORT int fd_hashtable_store(fd_hashtable ht,lispval key,lispval value);
FD_EXPORT int fd_hashtable_add(fd_hashtable ht,lispval key,lispval value);
FD_EXPORT int fd_hashtable_drop(fd_hashtable ht,lispval key,lispval value);

FD_EXPORT int fd_hashtable_probe(fd_hashtable ht,lispval key);
FD_EXPORT int fd_hashtable_probe_novoid(fd_hashtable ht,lispval key);

FD_EXPORT lispval fd_hashtable_keys(fd_hashtable ht);
FD_EXPORT struct FD_KEYVAL *fd_hashtable_keyvals
  (fd_hashtable ht,int *sizep,int lock);
FD_EXPORT int fd_for_hashtable
  (fd_hashtable ht,kv_valfn f,void *data,int lock);
FD_EXPORT int fd_for_hashtable_kv
  (struct FD_HASHTABLE *ht,fd_kvfn f,void *data,int lock);
FD_EXPORT lispval fd_hashtable_assocs(fd_hashtable ht);

FD_EXPORT lispval fd_hashtable_max(fd_hashtable,lispval,lispval *);
FD_EXPORT lispval fd_hashtable_skim(fd_hashtable,lispval,lispval);

FD_EXPORT lispval fd_slotmap_max(fd_slotmap,lispval,lispval *);
FD_EXPORT lispval fd_slotmap_skim(fd_slotmap,lispval,lispval);

FD_EXPORT long long fd_hashtable_map_size(struct FD_HASHTABLE *h);

FD_EXPORT int fd_resize_hashtable(fd_hashtable ptr,int n_slots);
FD_EXPORT int fd_hashtable_stats
  (fd_hashtable ptr,
   int *n_slots,int *n_keys,
   int *n_buckets,int *n_collisions,int *max_bucket,
   int *n_vals,int *max_vals);

FD_EXPORT void fd_hash_quality
  (unsigned int *hashv,int n_keys,int n_slots,
   unsigned int *buf,unsigned int bufsiz,
   unsigned int *nbucketsp,unsigned int *maxbucketp,
   unsigned int *ncollisionsp);

FD_EXPORT int fd_recycle_hashtable(struct FD_HASHTABLE *h);
FD_EXPORT int fd_free_buckets(struct FD_HASH_BUCKET **slots,
                              int slots_to_free,
                              int isbig);

FD_EXPORT void fd_free_keyvals(struct FD_KEYVAL *keyvals,int n_keyvals);

FD_EXPORT int fd_hashtable_set_readonly(FD_HASHTABLE *ht,int readonly);

/* Hashsets */

typedef struct FD_HASHSET {
  FD_CONS_HEADER;
  int hs_n_elts, hs_n_buckets;
  char hs_modified;
  char table_uselock;
  double hs_load_factor;
  lispval *hs_buckets;
  U8_RWLOCK_DECL(table_rwlock);} FD_HASHSET;
typedef struct FD_HASHSET *fd_hashset;

#define FD_HASHSET_LEN (sizeof(struct FD_HASHSET))

#define FD_HASHSETP(x) (FD_TYPEP(x,fd_hashset_type))
#define FD_XHASHSET(x) \
  fd_consptr(struct FD_HASHSET *,x,fd_hashset_type)

FD_EXPORT int fd_hashset_get(fd_hashset h,lispval key);
FD_EXPORT int fd_hashset_mod(fd_hashset h,lispval key,int add);
FD_EXPORT void fd_hashset_add_raw(fd_hashset h,lispval key);
FD_EXPORT int fd_hashset_add(fd_hashset h,lispval keys);
#define fd_hashset_drop(h,key) fd_hashset_mod(h,key,0)
FD_EXPORT lispval fd_hashset_intern(fd_hashset h,lispval key,int add);

FD_EXPORT lispval fd_hashset_elts(fd_hashset h,int clean);

FD_EXPORT void fd_init_hashset(fd_hashset h,int n,int stack_cons);
FD_EXPORT lispval fd_make_hashset(void);
FD_EXPORT ssize_t fd_grow_hashset(fd_hashset h,ssize_t size);
FD_EXPORT lispval fd_copy_hashset(FD_HASHSET *nptr,FD_HASHSET *ptr);
FD_EXPORT int fd_recycle_hashset(struct FD_HASHSET *h);
FD_EXPORT int fd_reset_hashset(fd_hashset);

#endif /* FRAMERD_TABLES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
