/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_TABLES_H
#define FRAMERD_TABLES_H 1
#ifndef FRAMERD_TABLES_H_INFO
#define FRAMERD_TABLES_H_INFO "include/framerd/tables.h"
#endif

FD_EXPORT fd_exception fd_NoSuchKey;

typedef enum FD_TABLEOP {
  fd_table_store=0, fd_table_add=1, fd_table_drop=2, fd_table_default=3,
  fd_table_increment=4, fd_table_multiply=5, fd_table_push=6,
  fd_table_replace=7, fd_table_replace_novoid=8,
  fd_table_add_if_present=9,
  fd_table_increment_if_present=10,
  fd_table_multiply_if_present=11,
  fd_table_store_noref=12, fd_table_add_noref=13,
  fd_table_test=14,
  fd_table_add_empty=15, fd_table_add_empty_noref=16,
  fd_table_maximize=17, fd_table_maximize_if_present=18,
  fd_table_minimize=19, fd_table_minimize_if_present=20}
 fd_tableop;

typedef fdtype (*fd_table_get_fn)(fdtype,fdtype,fdtype);
typedef int (*fd_table_test_fn)(fdtype,fdtype,fdtype);
typedef int (*fd_table_add_fn)(fdtype,fdtype,fdtype);
typedef int (*fd_table_drop_fn)(fdtype,fdtype,fdtype);
typedef int (*fd_table_store_fn)(fdtype,fdtype,fdtype);
typedef int (*fd_table_getsize_fn)(fdtype);
typedef int (*fd_table_modified_fn)(fdtype,int);
typedef fdtype (*fd_table_keys_fn)(fdtype);

struct FD_TABLEFNS {
  fdtype (*get)(fdtype obj,fdtype fd_key,fdtype dflt);
  int (*store)(fdtype obj,fdtype fd_key,fdtype value);
  int (*add)(fdtype obj,fdtype fd_key,fdtype value);
  int (*drop)(fdtype obj,fdtype fd_key,fdtype value);
  int (*test)(fdtype obj,fdtype fd_key,fdtype value);
  int (*readonly)(fdtype obj,int op);
  int (*modified)(fdtype obj,int op);
  int (*getsize)(fdtype obj);
  fdtype (*keys)(fdtype obj);
};

FD_EXPORT struct FD_TABLEFNS *fd_tablefns[];

#if FD_THREADS_ENABLED
#define fd_read_lock_table(p)  (u8_read_lock(&((p)->table_rwlock)))
#define fd_write_lock_table(p) (u8_write_lock(&((p)->table_rwlock)))
#define fd_unlock_table(p)  (u8_rw_unlock(&((p)->table_rwlock)))
#else
#define fd_read_lock_table(p)
#define fd_write_lock_table(p)
#define fd_unlock_table(p)
#endif

FD_EXPORT fdtype fd_get(fdtype obj,fdtype key,fdtype dflt);
FD_EXPORT int fd_test(fdtype obj,fdtype key,fdtype value);
FD_EXPORT int fd_store(fdtype obj,fdtype key,fdtype value);
FD_EXPORT int fd_add(fdtype obj,fdtype key,fdtype value);
FD_EXPORT int fd_drop(fdtype obj,fdtype key,fdtype value);
FD_EXPORT int fd_getsize(fdtype arg);
FD_EXPORT int fd_modifiedp(fdtype arg);
FD_EXPORT int fd_set_modified(fdtype arg,int val);
FD_EXPORT int fd_set_readonly(fdtype arg,int val);
FD_EXPORT fdtype fd_getkeys(fdtype arg);
FD_EXPORT fdtype fd_getvalues(fdtype arg);
FD_EXPORT fdtype fd_getassocs(fdtype arg);
FD_EXPORT void fd_display_table(u8_output out,fdtype table,fdtype keys);
FD_EXPORT fdtype fd_table_max(fdtype table,fdtype scope,fdtype *maxval);
FD_EXPORT fdtype fd_table_skim(fdtype table,fdtype maxval,fdtype scope);


#define FD_TABLEP(x) ((fd_tablefns[FD_PTR_TYPE(x)])!=NULL)

#define FD_INIT_SMAP_SIZE 7
#define FD_INIT_HASH_SIZE 73

FD_EXPORT short fd_init_smap_size;
FD_EXPORT int   fd_init_hash_size;

/* Slotmaps */

struct FD_KEYVAL {
  fdtype fd_kvkey, fd_keyval;};

typedef int (*fd_keyvalfn)(fdtype,fdtype,void *);
typedef int (*fd_kvfn)(struct FD_KEYVAL *,void *);

typedef struct FD_SLOTMAP {
  FD_CONS_HEADER;
  unsigned int table_size:12;
  unsigned int table_freespace:12;
  unsigned int table_readonly:1;
  unsigned int table_modified:1;
  unsigned int table_uselock:1;
  unsigned int sm_free_keyvals:1;
  struct FD_KEYVAL *sm_keyvals;
  U8_RWLOCK_DECL(table_rwlock);} FD_SLOTMAP;
typedef struct FD_SLOTMAP *fd_slotmap;

#define FD_SLOTMAPP(x) (FD_TYPEP(x,fd_slotmap_type))
#define FD_XSLOTMAP(x) \
  (fd_consptr(struct FD_SLOTMAP *,x,fd_slotmap_type))
#define FD_XSLOTMAP_SIZE(sm) (sm->table_size)
#define FD_XSLOTMAP_SPACE(sm) (sm->table_freespace)
#define FD_XSLOTMAP_USELOCKP(sm) (sm->table_uselock)
#define FD_XSLOTMAP_MODIFIEDP(sm) (sm->table_modified)
#define FD_XSLOTMAP_READONLYP(sm) (sm->table_readonly)
#define FD_XSLOTMAP_SET_READONLY(sm) (sm)->table_readonly=1
#define FD_XSLOTMAP_CLEAR_READONLY(sm) (sm)->table_readonly=0
#define FD_XSLOTMAP_MARK_MODIFIED(sm) (sm)->table_modified=1
#define FD_XSLOTMAP_CLEAR_MODIFIED(sm) (sm)->table_modified=0
#define FD_XSLOTMAP_SET_SIZE(sm,sz) (sm)->table_size=sz
#define FD_XSLOTMAP_SET_SPACE(sm,sz) (sm)->table_freespace=sz

#define FD_SLOTMAP_SIZE(x) (FD_XSLOTMAP_SIZE(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_MODIFIEDP(x) (FD_XSLOTMAP_MODIFIEDP(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_READONLYP(x) (FD_XSLOTMAP_READONLYP(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_USELOCKP(x) (FD_XSLOTMAP_USELOCKP(FD_XSLOTMAP(x)))
#define FD_SLOTMAP_SET_READONLY(x) \
  FD_XSLOTMAP_SET_READONLY(FD_XSLOTMAP(x))
#define FD_SLOTMAP_CLEAR_READONLY(x) \
  FD_XSLOTMAP_CLEAR_READONLY(FD_XSLOTMAP(x))
#define FD_SLOTMAP_MARK_MODIFIED(x) \
  FD_XSLOTMAP_MARK_MODIFIED(FD_XSLOTMAP(x))
#define FD_SLOTMAP_CLEAR_MODIFIED(x) \
  FD_XSLOTMAP_CLEAR_MODIFIED(FD_XSLOTMAP(x))

#define fd_slotmap_size(x) (FD_XSLOTMAP_SIZE(FD_XSLOTMAP(x)))
#define fd_slotmap_modifiedp(x) (FD_XSLOTMAP_MODIFIEDP(FD_XSLOTMAP(x)))

FD_EXPORT fdtype fd_init_slotmap
  (struct FD_SLOTMAP *ptr,int len,struct FD_KEYVAL *data);
FD_EXPORT int fd_slotmap_store
  (struct FD_SLOTMAP *sm,fdtype key,fdtype value);
FD_EXPORT int fd_slotmap_add
  (struct FD_SLOTMAP *sm,fdtype key,fdtype value);
FD_EXPORT int fd_slotmap_drop
  (struct FD_SLOTMAP *sm,fdtype key,fdtype value);
FD_EXPORT int fd_slotmap_delete
  (struct FD_SLOTMAP *sm,fdtype key);
FD_EXPORT fdtype fd_slotmap_keys(struct FD_SLOTMAP *sm);

#define fd_empty_slotmap() (fd_init_slotmap(NULL,0,NULL))

FD_EXPORT fdtype fd_make_slotmap(int space,int len,struct FD_KEYVAL *data);

FD_EXPORT fdtype fd_slotmap_max
  (struct FD_SLOTMAP *sm,fdtype scope,fdtype *maxvalp);

FD_EXPORT struct FD_KEYVAL *fd_sortvec_insert
  (fdtype key,struct FD_KEYVAL **kvp,int *sizep,int *spacep,int freedata);

#if FD_INLINE_TABLES
static struct FD_KEYVAL *fd_sortvec_get
   (fdtype fd_key,struct FD_KEYVAL *keyvals,int size)
{
  if (size<4) {
    const struct FD_KEYVAL *scan=keyvals, *limit=scan+size;
    if (FD_ATOMICP(fd_key))
      while (scan<limit)
        if (scan->fd_kvkey==fd_key)
          return (struct FD_KEYVAL *)scan;
        else scan++;
    else while (scan<limit)
      if (FDTYPE_EQUAL(scan->fd_kvkey,fd_key))
        return (struct FD_KEYVAL *) scan;
      else scan++;
    return NULL;}
  else {
    int found=0;
    const struct FD_KEYVAL
      *bottom=keyvals, *limit=bottom+size, *top=limit-1, *middle;
    if (FD_ATOMICP(fd_key))
      while (top>=bottom) {
        middle=bottom+(top-bottom)/2;
        if (middle>=limit) break;
        else if (fd_key==middle->fd_kvkey) {found=1; break;}
        else if (FD_CONSP(middle->fd_kvkey)) top=middle-1;
        else if (fd_key<middle->fd_kvkey) top=middle-1;
        else bottom=middle+1;}
    else while (top>=bottom) {
      int comparison;
      middle=bottom+(top-bottom)/2;
      if (middle>=limit) break;
      comparison=cons_compare(fd_key,middle->fd_kvkey);
      if (comparison==0) {found=1; break;}
      else if (comparison<0) top=middle-1;
      else bottom=middle+1;}
    if (found)
      return (struct FD_KEYVAL *) middle;
    else return NULL;}
}
#else
FD_EXPORT struct FD_KEYVAL *_fd_sortvec_get
   (fdtype key,struct FD_KEYVAL *keyvals,int size);
#define fd_sortvec_get _fd_sortvec_get
#endif

FD_EXPORT fdtype _fd_slotmap_get
  (struct FD_SLOTMAP *sm,fdtype key,fdtype dflt);
FD_EXPORT fdtype _fd_slotmap_test
  (struct FD_SLOTMAP *sm,fdtype key,fdtype val);

#if FD_INLINE_TABLES
static U8_MAYBE_UNUSED fdtype fd_slotmap_get
  (struct FD_SLOTMAP *sm,fdtype key,fdtype dflt)
{
  unsigned int unlock=0;
  struct FD_KEYVAL *result; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if ((FD_XSLOTMAP_USELOCKP(sm))&&
      (!(FD_XSLOTMAP_READONLYP(sm)))) {
    fd_read_lock(&sm->table_rwlock); unlock=1;}
  size=FD_XSLOTMAP_SIZE(sm);
  result=fd_sortvec_get(key,sm->sm_keyvals,size);
  if (result) {
    fdtype v=fd_incref(result->fd_keyval);
    if (unlock) fd_rw_unlock(&sm->table_rwlock);
    return v;}
  else {
    if (unlock) fd_rw_unlock(&sm->table_rwlock);
    return fd_incref(dflt);}
}
static U8_MAYBE_UNUSED fdtype fd_slotmap_test
  (struct FD_SLOTMAP *sm,fdtype key,fdtype val)
{
  unsigned int unlock=0;
  struct FD_KEYVAL *result; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if ((FD_ABORTP(val))) return fd_interr(val);
  if ((FD_ABORTP(key))) return fd_interr(key);
  if ((FD_XSLOTMAP_USELOCKP(sm))&&
      (!(FD_XSLOTMAP_READONLYP(sm)))) {
    fd_read_lock(&sm->table_rwlock); unlock=1;}
  size=FD_XSLOTMAP_SIZE(sm);
  result=fd_sortvec_get(key,sm->sm_keyvals,size);
  if (result) {
    fdtype current=result->fd_keyval; int cmp;
    if (FD_VOIDP(val)) cmp=1;
    else if (FD_EQ(val,current)) cmp=1;
    else if ((FD_CHOICEP(val)) || (FD_ACHOICEP(val)) ||
             (FD_CHOICEP(current)) || (FD_ACHOICEP(current)))
      cmp=fd_overlapp(val,current);
    else if (FD_EQUAL(val,current)) cmp=1;
    else cmp=0;
    if (unlock) fd_rw_unlock(&sm->table_rwlock);
    return cmp;}
  else {
    if (unlock) fd_rw_unlock(&sm->table_rwlock);
    return 0;}
}
#else
#define fd_slotmap_get _fd_slotmap_get
#define fd_slotmap_test _fd_slotmap_test
#endif

FD_EXPORT fdtype fd_plist_to_slotmap(fdtype plist);
FD_EXPORT fdtype fd_alist_to_slotmap(fdtype alist);
FD_EXPORT fdtype fd_blist_to_slotmap(fdtype binding_list);

/* Schemamaps */

typedef struct FD_SCHEMAP {
  FD_CONS_HEADER; 
  short table_size;
  int schemap_sorted:1, schemap_onstack:1, schemap_tagged:1;
  int table_readonly:1, schemap_shared:1, table_modified:1;
  fdtype *table_schema, *schema_values;
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

typedef struct FD_SCHEMAP *fd_schemap;

#define FD_SCHEMAPP(x) (FD_TYPEP(x,fd_schemap_type))
#define FD_XSCHEMAP(x) (fd_consptr(struct FD_SCHEMAP *,x,fd_schemap_type))
#define FD_XSCHEMAP_SIZE(sm) ((sm)->table_size)
#define FD_XSCHEMAP_SORTEDP(sm) ((sm)->schemap_sorted)
#define FD_XSCHEMAP_READONLYP(sm) ((sm)->table_readonly)
#define FD_XSCHEMAP_MODIFIEDP(sm) ((sm)->table_modified)
#define FD_XSCHEMAP_MARK_MODIFIED(sm) (sm)->table_modified=1
#define FD_XSCHEMAP_CLEAR_MODIFIED(sm) (sm)->table_modified=0
#define FD_XSCHEMAP_SET_READONLY(sm) (sm)->table_readonly=1
#define FD_XSCHEMAP_CLEAR_READONLY(sm) (sm)->table_readonly=0

#define FD_SCHEMAP_SIZE(x) (FD_XSCHEMAP_SIZE(FD_XSCHEMAP(x)))
#define FD_SCHEMAP_SORTEDP(x) (FD_XSCHEMAP_SORTEDP(FD_XSCHEMAP(x)))
#define FD_SCHEMAP_READONLYP(x) (FD_XSCHEMAP_READONLYP(FD_XSCHEMAP(x)))
#define FD_SCHEMAP_MODIFIEDP(x) (FD_XSCHEMAP_MODIFIEDP(FD_XSCHEMAP(x)))

#define FD_SCHEMAP_MARK_MODIFIED(x) \
  FD_XSCHEMAP_MARK_MODIFIED(FD_XSCHEMAP(x))
#define FD_SCHEMAP_CLEAR_MODIFIED(x) \
  FD_XSCHEMAP_CLEAR_MODIFIED(FD_XSCHEMAP(x))
#define FD_SCHEMAP_SET_READONLY(x) \
  FD_XSCHEMAP_SET_READONLY(FD_XSCHEMAP(x))
#define FD_SCHEMAP_CLEAR_READONLY(x) \
  FD_XSCHEMAP_CLEAR_READONLY(FD_XSCHEMAP(x))

FD_EXPORT fdtype fd_make_schemap
  (struct FD_SCHEMAP *ptr,short n_slots,short flags,
   fdtype *schema,fdtype *values);
FD_EXPORT fdtype fd_init_schemap
  (struct FD_SCHEMAP *ptr,short n_keyvals,
   struct FD_KEYVAL *init);

FD_EXPORT fdtype _fd_schemap_get
  (struct FD_SCHEMAP *sm,fdtype key,fdtype dflt);
FD_EXPORT fdtype _fd_schemap_test
  (struct FD_SCHEMAP *sm,fdtype key,fdtype val);
FD_EXPORT int fd_schemap_store
  (struct FD_SCHEMAP *sm,fdtype key,fdtype value);
FD_EXPORT int fd_schemap_add
  (struct FD_SCHEMAP *sm,fdtype key,fdtype value);
FD_EXPORT int fd_schemap_drop
  (struct FD_SCHEMAP *sm,fdtype key,fdtype value);
FD_EXPORT fdtype fd_schemap_keys(struct FD_SCHEMAP *ht);
FD_EXPORT fdtype *fd_register_schema(int n,fdtype *v);
FD_EXPORT void fd_sort_schema(int n,fdtype *v);

#if FD_INLINE_TABLES
static U8_MAYBE_UNUSED int _fd_get_slotno
  (fdtype key,fdtype *schema,int size,int sorted)
{
  if ((sorted) && (size>7)) {
    const fdtype *bottom=schema, *middle=bottom+size/2;
    const fdtype *hard_top=bottom+size, *top=hard_top;
    while (top>bottom) {
      if (key==*middle) return middle-schema;
      else if (key<*middle) {
        top=middle-1; middle=bottom+(top-bottom)/2;}
      else {
        bottom=middle+1; middle=bottom+(top-bottom)/2;}}
    if ((middle) && (middle<hard_top) && (key==*middle))
      return middle-schema;
    else return -1;}
  else {
    const fdtype *scan=schema, *limit=schema+size;
    while (scan<limit)
      if (FD_EQ(key,*scan)) return scan-schema;
      else scan++;
    return -1;}
}
static U8_MAYBE_UNUSED fdtype fd_schemap_get
  (struct FD_SCHEMAP *sm,fdtype key,fdtype dflt)
{
  int unlock=0;
  int size, slotno, sorted;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_schemap_type);
  if (!(FD_XSCHEMAP_READONLYP(sm))) {
    fd_read_lock(&(sm->table_rwlock)); unlock=1;}
  size=FD_XSCHEMAP_SIZE(sm);
  sorted=FD_XSCHEMAP_SORTEDP(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sorted);
  if (slotno>=0) {
    fdtype v=fd_incref(sm->schema_values[slotno]);
    if (unlock) fd_rw_unlock(&(sm->table_rwlock));
    return v;}
  else {
    if (unlock) fd_rw_unlock(&(sm->table_rwlock));
    return fd_incref(dflt);}
}
static U8_MAYBE_UNUSED fdtype fd_schemap_test
  (struct FD_SCHEMAP *sm,fdtype key,fdtype val)
{
  int unlock=0, size, slotno;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_schemap_type);
  if ((FD_ABORTP(val)))
    return fd_interr(val);
  if (!(FD_XSCHEMAP_READONLYP(sm))) {
    fd_read_lock(&(sm->table_rwlock)); unlock=1;}
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sm->schemap_sorted);
  if (slotno>=0) {
    fdtype current=sm->schema_values[slotno]; int cmp;
    if (FD_VOIDP(val)) cmp=1;
    else if (FD_EQ(val,current)) cmp=1;
    else if ((FD_CHOICEP(val)) || (FD_ACHOICEP(val)) ||
             (FD_CHOICEP(current)) || (FD_ACHOICEP(current)))
      cmp=fd_overlapp(val,current);
    else if (FD_EQUAL(val,current)) cmp=1;
    else cmp=0;
    if (unlock) fd_rw_unlock(&sm->table_rwlock);
    return cmp;}
  else {
    if (unlock) fd_rw_unlock(&sm->table_rwlock);
    return 0;}
}
#else
#define fd_schemap_get _fd_schemap_get
#define fd_schemap_test _fd_schemap_test
#endif

/* Hashtables */

typedef struct FD_HASH_BUCKET {
  int fd_n_entries;
  struct FD_KEYVAL fd_keyval0;} FD_HASH_BUCKET;
typedef struct FD_HASH_BUCKET *fd_hash_bucket;

typedef struct FD_HASHTABLE {
  FD_CONS_HEADER;
  unsigned int ht_n_buckets, table_n_keys, table_load_factor;
  unsigned int table_readonly:1, table_modified:1, table_uselock:1;
  struct FD_HASH_BUCKET **fd_buckets;
  U8_RWLOCK_DECL(table_rwlock);} FD_HASHTABLE;
typedef struct FD_HASHTABLE *fd_hashtable;

#define FD_HASHTABLEP(x) (FD_TYPEP(x,fd_hashtable_type))
#define FD_XHASHTABLE(x) \
  fd_consptr(struct FD_HASHTABLE *,x,fd_hashtable_type)

#define FD_HASHTABLE_SLOTS(x) \
  ((FD_XHASHTABLE(x))->ht_n_buckets)
#define FD_HASHTABLE_SIZE(x) \
  ((FD_XHASHTABLE(x))->table_n_keys)
#define FD_HASHTABLE_READONLYP(x) \
  ((FD_XHASHTABLE(x))->table_readonly)
#define FD_HASHTABLE_MODIFIEDP(x) \
  ((FD_XHASHTABLE(x))->table_modified)
#define FD_HASHTABLE_MARK_MODIFIED(x) \
  ((FD_XHASHTABLE(x))->table_modified)=1
#define FD_HASHTABLE_CLEAR_MODIFIED(x) \
  ((FD_XHASHTABLE(x))->table_modified)=0
#define FD_HASHTABLE_SET_READONLY(x) \
  ((FD_XHASHTABLE(x))->table_readonly)=1
#define FD_HASHTABLE_CLEAR_READONLY(x) \
  ((FD_XHASHTABLE(x))->table_readonly)=0

#define FD_XHASHTABLE_SLOTS(x)          ((x)->ht_n_buckets)
#define FD_XHASHTABLE_SIZE(x)           ((x)->table_n_keys)
#define FD_XHASHTABLE_READONLYP(x)      ((x)->table_readonly)
#define FD_XHASHTABLE_MODIFIEDP(x)      ((x)->table_modified)
#define FD_XHASHTABLE_MARK_MODIFIED(x)  ((x)->table_modified)=1
#define FD_XHASHTABLE_CLEAR_MODIFIED(x) ((x)->table_modified)=0
#define FD_XHASHTABLE_SET_READONLY(x)   ((x)->table_readonly)=1
#define FD_XHASHTABLE_CLEAR_READONLY(x) ((x)->table_readonly)=0

FD_EXPORT unsigned int fd_get_hashtable_size(unsigned int min);
FD_EXPORT unsigned int fd_hash_string(u8_string string,int len);
FD_EXPORT unsigned int fd_hash_lisp(fdtype x);

FD_EXPORT fdtype fd_make_hashtable(fd_hashtable ptr,int n_slots);
FD_EXPORT fdtype fd_init_hashtable
   (fd_hashtable ptr,int n_keyvals,struct FD_KEYVAL *init);
FD_EXPORT int fd_reset_hashtable(fd_hashtable ht,int n_slots,int lock);
FD_EXPORT struct FD_KEYVAL *fd_hashvec_get(fdtype,struct FD_HASH_BUCKET **,int);
FD_EXPORT int fd_fast_reset_hashtable(fd_hashtable,int,int,struct FD_HASH_BUCKET ***,int *);
FD_EXPORT int fd_remove_deadwood_x(struct FD_HASHTABLE *ptr,
                                   int (*testfn)(fdtype,fdtype,void *),
                                   void *testdata);
FD_EXPORT int fd_remove_deadwood(struct FD_HASHTABLE *ptr);
FD_EXPORT int fd_devoid_hashtable_x(fd_hashtable ht,int locked);
FD_EXPORT int fd_devoid_hashtable(fd_hashtable ht);
FD_EXPORT int fd_static_hashtable(struct FD_HASHTABLE *ptr,int);

FD_EXPORT fdtype fd_copy_hashtable(FD_HASHTABLE *nptr,FD_HASHTABLE *ptr);

FD_EXPORT int fd_hashtable_op
   (fd_hashtable ht,fd_tableop op,fdtype key,fdtype value);
FD_EXPORT int fd_hashtable_op_nolock
   (fd_hashtable ht,fd_tableop op,fdtype key,fdtype value);
FD_EXPORT int fd_hashtable_iter
   (fd_hashtable ht,fd_tableop op,int n,
    const fdtype *keys,const fdtype *values);
FD_EXPORT int fd_hashtable_iterkeys
   (fd_hashtable ht,fd_tableop op,int n,
    const fdtype *keys,fdtype value);
FD_EXPORT int fd_hashtable_itervalues
   (fd_hashtable ht,fd_tableop op,int n,
    fdtype key,const fdtype *values);

FD_EXPORT fdtype fd_hashtable_get
   (fd_hashtable ht,fdtype key,fdtype dflt);
FD_EXPORT fdtype fd_hashtable_get_nolock
   (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt);
FD_EXPORT fdtype fd_hashtable_get_noref
   (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt);
FD_EXPORT fdtype fd_hashtable_get_nolockref
   (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt);
FD_EXPORT int fd_hashtable_store(fd_hashtable ht,fdtype key,fdtype value);
FD_EXPORT int fd_hashtable_add(fd_hashtable ht,fdtype key,fdtype value);
FD_EXPORT int fd_hashtable_drop(fd_hashtable ht,fdtype key,fdtype value);

FD_EXPORT int fd_hashtable_probe(fd_hashtable ht,fdtype key);
FD_EXPORT int fd_hashtable_probe_novoid(fd_hashtable ht,fdtype key);

FD_EXPORT fdtype fd_hashtable_keys(fd_hashtable ht);
FD_EXPORT struct FD_KEYVAL *fd_hashtable_keyvals
  (fd_hashtable ht,int *sizep,int lock);
FD_EXPORT int fd_for_hashtable
  (fd_hashtable ht,fd_keyvalfn f,void *data,int lock);
FD_EXPORT int fd_for_hashtable_kv
  (struct FD_HASHTABLE *ht,fd_kvfn f,void *data,int lock);

FD_EXPORT fdtype fd_hashtable_max(fd_hashtable,fdtype,fdtype *);
FD_EXPORT fdtype fd_hashtable_skim(fd_hashtable,fdtype,fdtype);

FD_EXPORT fdtype fd_slotmap_max(fd_slotmap,fdtype,fdtype *);
FD_EXPORT fdtype fd_slotmap_skim(fd_slotmap,fdtype,fdtype);

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
FD_EXPORT int fd_free_hashvec(struct FD_HASH_BUCKET **slots,int slots_to_free);

FD_EXPORT int fd_hashtable_set_readonly(FD_HASHTABLE *ht,int readonly);

/* Hashsets */

typedef struct FD_HASHSET {
  FD_CONS_HEADER;
  int hs_n_elts, hs_n_slots, hs_load_factor, hs_allatomic, hs_modified;
  fdtype *hs_slots;
  U8_MUTEX_DECL(hs_lock);} FD_HASHSET;
typedef struct FD_HASHSET *fd_hashset;

#define FD_HASHSETP(x) (FD_TYPEP(x,fd_hashset_type))
#define FD_XHASHSET(x) \
  fd_consptr(struct FD_HASHSET *,x,fd_hashset_type)

FD_EXPORT int fd_hashset_get(fd_hashset h,fdtype key);
FD_EXPORT int fd_hashset_mod(fd_hashset h,fdtype key,int add);
FD_EXPORT int fd_hashset_add_raw(fd_hashset h,fdtype key);
FD_EXPORT int fd_hashset_add(fd_hashset h,fdtype keys);
#define fd_hashset_drop(h,key) fd_hashset_mod(h,key,0)
FD_EXPORT fdtype fd_hashset_elts(fd_hashset h,int clean);

FD_EXPORT void fd_init_hashset(fd_hashset h,int n,int stack_cons);
FD_EXPORT fdtype fd_make_hashset(void);
FD_EXPORT ssize_t fd_grow_hashset(fd_hashset h,size_t size);
FD_EXPORT fdtype fd_copy_hashset(FD_HASHSET *nptr,FD_HASHSET *ptr);
FD_EXPORT int fd_recycle_hashset(struct FD_HASHSET *h);

#endif /* FRAMERD_TABLES_H */
