/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/hash.h"
#include "framerd/tables.h"
#include "framerd/numbers.h"

#include <libu8/u8printf.h>

u8_condition fd_NoSuchKey=_("No such key");
u8_condition fd_ReadOnlyTable=_("Read-Only table");
u8_condition fd_ReadOnlyHashtable=_("Read-Only hashtable");
static u8_string NotATable=_("Not a table");
static u8_string CantDrop=_("Table doesn't support drop");
static u8_string CantTest=_("Table doesn't support test");
static u8_string CantGetKeys=_("Table doesn't support getkeys");
static u8_string CantCheckModified=_("Can't check for modification status");
static u8_string CantSetModified=_("Can't set modification status");
static u8_string CantCheckReadOnly=_("Can't check for readonly status");
static u8_string CantSetReadOnly=_("Can't set readonly status");
static u8_string CantCheckFinished=_("Can't check for finished/completed status");
static u8_string CantSetFinished=_("Can't set finish/completed status");
static u8_string BadHashtableMethod=_("Invalid hashtable method");

#define DEBUGGING 0

static int resize_hashtable(struct FD_HASHTABLE *ptr,int n_slots,int need_lock);

#if DEBUGGING
#include <stdio.h>
#endif

#include <math.h>
#include <limits.h>

#define flip_word(x) \
  (((x>>24)&0xff) | ((x>>8)&0xff00) | ((x&0xff00)<<8) | ((x&0xff)<<24))
#define compute_offset(hash,size) (hash%size)

FD_FASTOP int numcompare(lispval x,lispval y)
{
  if ((FIXNUMP(x)) && (FIXNUMP(y)))
    if ((FIX2INT(x))>(FIX2INT(y))) return 1;
    else if ((FIX2INT(x))<(FIX2INT(y))) return -1;
    else return 0;
  else if ((FD_FLONUMP(x)) && (FD_FLONUMP(y)))
    if ((FD_FLONUM(x))>(FD_FLONUM(y))) return 1;
    else if ((FD_FLONUM(x))<(FD_FLONUM(y))) return -1;
    else return 0;
  else return fd_numcompare(x,y);
}

int   fd_init_smap_size = FD_INIT_SMAP_SIZE;
int   fd_init_hash_size = FD_INIT_HASH_SIZE;

unsigned int fd_hash_bigthresh = FD_HASH_BIGTHRESH;

/* Debugging tools */

#if DEBUGGING
static lispval look_for_key=VOID;

static void note_key(lispval key,struct FD_HASHTABLE *h)
{
  fprintf(stderr,_("Noticed %s on %lx\n"),fd_lisp2string(key),h);
}

#define KEY_CHECK(key,ht) \
  if (LISP_EQUAL(key,look_for_key)) note_key(key,ht)
#else
#define KEY_CHECK(key,ht)
#endif

/* Keyvecs */

FD_EXPORT
struct FD_KEYVAL *_fd_keyvec_get
   (lispval fd_key,struct FD_KEYVAL *keyvals,int size)
{
  const struct FD_KEYVAL *scan=keyvals, *limit=scan+size;
  if (ATOMICP(fd_key))
    while (scan<limit)
      if (scan->kv_key==fd_key)
        return (struct FD_KEYVAL *)scan;
      else scan++;
  else while (scan<limit)
         if (LISP_EQUAL(scan->kv_key,fd_key))
           return (struct FD_KEYVAL *) scan;
         else scan++;
  return NULL;
}

FD_EXPORT
struct FD_KEYVAL *fd_keyvec_insert
 (lispval key,struct FD_KEYVAL **keyvalp,
  int *sizep,int *spacep,int max_space,
  int freedata)
{
  int size=*sizep;
  int space= (spacep) ? (*spacep) : (0);
  struct FD_KEYVAL *keyvals=*keyvalp;
  const struct FD_KEYVAL *scan=keyvals, *limit=scan+size;
  if (keyvals) {
    if (ATOMICP(key))
      while (scan<limit)
        if (scan->kv_key==key)
          return (struct FD_KEYVAL *)scan;
        else scan++;
    else while (scan<limit)
           if (LISP_EQUAL(scan->kv_key,key))
             return (struct FD_KEYVAL *) scan;
           else scan++;}
  if (size<space)  {
    keyvals[size].kv_key=fd_getref(key);
    keyvals[size].kv_val=EMPTY;
    *sizep=size+1;
    return &(keyvals[size]);}
  else if (space < max_space) {
    size_t new_space = (spacep) ? (space+4) : (space+1);
    struct FD_KEYVAL *nkeyvals= ((keyvals) && (freedata)) ?
      (u8_realloc_n(keyvals,new_space,struct FD_KEYVAL)) :
      (u8_alloc_n(new_space,struct FD_KEYVAL));
    if ((keyvals) && (!(freedata)))
      memcpy(nkeyvals,keyvals,(size)*FD_KEYVAL_LEN);
    if (nkeyvals != keyvals)
      *keyvalp=nkeyvals;
    nkeyvals[size].kv_key=fd_getref(key);
    nkeyvals[size].kv_val=EMPTY;
    if (spacep) *spacep=new_space;
    *sizep=size+1;
    return &(nkeyvals[size]);}
  else return NULL;
}

/* Sort map */

FD_FASTOP void swap_keyvals(struct FD_KEYVAL *a,struct FD_KEYVAL *b)
{
  struct FD_KEYVAL tmp; tmp=*a; *a=*b; *b=tmp;
}

static void atomic_sort_keyvals(struct FD_KEYVAL *v,int n)
{
  unsigned i, j, ln, rn;
  while (n > 1) {
    swap_keyvals(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (v[j].kv_key > v[0].kv_key);
      do ++i; while (i < j && (v[i].kv_key<v[0].kv_key));
      if (i >= j) break; else {}
      swap_keyvals(&v[i], &v[j]);}
    swap_keyvals(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      atomic_sort_keyvals(v, ln); v += j; n = rn;}
    else {atomic_sort_keyvals(v + j, rn); n = ln;}}
}

static void cons_sort_keyvals(struct FD_KEYVAL *v,int n)
{
  unsigned i, j, ln, rn;
  while (n > 1) {
    swap_keyvals(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (cons_compare(v[j].kv_key,v[0].kv_key)>0);
      do ++i; while (i < j && ((cons_compare(v[i].kv_key,v[0].kv_key))<0));
      if (i >= j) break; else {}
      swap_keyvals(&v[i], &v[j]);}
    swap_keyvals(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      cons_sort_keyvals(v, ln); v += j; n = rn;}
    else {cons_sort_keyvals(v + j, rn); n = ln;}}
}

static void sort_keyvals(struct FD_KEYVAL *v,int n)
{
  int i=0; while (i<n)
    if (ATOMICP(v[i].kv_key)) i++;
    else {
      cons_sort_keyvals(v,n);
      return;}
  atomic_sort_keyvals(v,n);
}

FD_EXPORT struct FD_KEYVAL *_fd_sortvec_get
   (lispval key,struct FD_KEYVAL *keyvals,int size)
{
  return fd_sortvec_get(key,keyvals,size);
}

FD_EXPORT struct FD_KEYVAL *fd_sortvec_insert
  (lispval key,
   struct FD_KEYVAL **kvp,
   int *sizep,int *spacep,int max_space,
   int freedata)
{
  struct FD_KEYVAL *keyvals=*kvp;
  int size=*sizep, space=((spacep)?(*spacep):(0)), dir=0;
  struct FD_KEYVAL *bottom=keyvals, *top=bottom+size-1;
  struct FD_KEYVAL *limit=bottom+size, *middle=bottom+size/2;
  if (keyvals == NULL) {
    *kvp=keyvals=u8_alloc(struct FD_KEYVAL);
    memset(keyvals,0,FD_KEYVAL_LEN);
    if (keyvals==NULL) return NULL;
    keyvals->kv_key=fd_getref(key);
    keyvals->kv_val=EMPTY;
    if (sizep) *sizep=1;
    if (spacep) *spacep=1;
    return keyvals;}
  else if (size == 0) {
    middle = keyvals; dir=-1;}
  else if (ATOMICP(key))
    while (top>=bottom) {
      middle=bottom+(top-bottom)/2;
      if (middle>=limit) break;
      else if (key==middle->kv_key) {
        dir=0; break;}
      else if (CONSP(middle->kv_key)) {
        top=middle-1; dir = -1;}
      else if (key<middle->kv_key) {
        top=middle-1; dir = -1;}
      else {
        bottom=middle+1;
        dir = 1;}}
  else while (top>=bottom) {
      middle=bottom+(top-bottom)/2;
      if (middle>=limit) break;
      int comparison = cons_compare(key,middle->kv_key);
      if (comparison==0) {
        dir=0; break;}
      else if (comparison<0) {
        top=middle-1; dir = -1;}
      else {
        bottom=middle+1;
        dir = 1;}}
  if (dir == 0)
    return middle;
  if (size >= space) {
    /* Make sure there's space */
    int mpos=(middle-keyvals);
    int new_space = space+1;
    struct FD_KEYVAL *new_keyvals=
      ( (freedata) ? (u8_realloc_n(keyvals,new_space,struct FD_KEYVAL)) :
        (u8_extalloc(keyvals,(new_space*FD_KEYVAL_LEN),(space*FD_KEYVAL_LEN))));
    if ( new_keyvals == NULL )
      return NULL;
    keyvals = *kvp = new_keyvals;
    space   = *spacep = new_space;
    bottom = new_keyvals;
    middle = new_keyvals+mpos;
    top = new_keyvals+size;}
  /* Now, insert the value */
  struct FD_KEYVAL *insert_point = (dir<0) ? (middle) : (middle+1);
  int ipos = insert_point-keyvals;
  size_t bytes_to_move = (size-ipos)*FD_KEYVAL_LEN;
  memmove(insert_point+1,insert_point,bytes_to_move);
  insert_point->kv_key=fd_getref(key);
  insert_point->kv_val=EMPTY;
  *sizep=size+1;
  return insert_point;
}

static int slotmap_fail(struct FD_SLOTMAP *sm,u8_context caller)
{
  char dbuf[64];
  int size = sm->n_slots, space = sm->n_allocd;
  if (space >= SHRT_MAX)
    fd_seterr("SlotmapOverflow",caller,
              u8_sprintf(dbuf,64,"%d:%d>%d",size,space,SHRT_MAX),
              (lispval)sm);
  else fd_seterr("SlotmapInsertFail",caller,
                 u8_sprintf(dbuf,64,"%d:%d",size,space),
                 (lispval)sm);
  return -1;
}

FD_EXPORT lispval _fd_slotmap_get
  (struct FD_SLOTMAP *sm,lispval key,lispval dflt)
{
  return fd_slotmap_get(sm,key,dflt);
}

FD_EXPORT lispval _fd_slotmap_test(struct FD_SLOTMAP *sm,lispval key,lispval val)
{
  return fd_slotmap_test(sm,key,val);
}

FD_EXPORT int fd_slotmap_store(struct FD_SLOTMAP *sm,lispval key,lispval value)
{
  int unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if (FD_TROUBLEP(key))
    return fd_interr(key);
  else if ((FD_TROUBLEP(value)))
    return fd_interr(value);
  else if (FD_EMPTYP(key))
    return 0;
  else if (sm->table_readonly) {
    fd_seterr(fd_ReadOnlyTable,"fd_slotmap_store",NULL,key);
    return -1;}
  else if (sm->table_uselock) {
    u8_write_lock(&sm->table_rwlock);
    unlock=1;}
  else NO_ELSE;
  {
    int rv=0;
    int cur_nslots=FD_XSLOTMAP_NUSED(sm), nslots=cur_nslots;
    int cur_allocd=FD_XSLOTMAP_NALLOCATED(sm), allocd=cur_allocd;
    struct FD_KEYVAL *cur_keyvals=sm->sm_keyvals;;
    struct FD_KEYVAL *result=
      (sm->sm_sort_keyvals) ?
      (fd_sortvec_insert(key,&(sm->sm_keyvals),
                         &nslots,&allocd,SHRT_MAX,
                         sm->sm_free_keyvals)) :
      (fd_keyvec_insert(key,&(sm->sm_keyvals),
                        &nslots,&allocd,SHRT_MAX,
                        sm->sm_free_keyvals));
    if (PRED_FALSE(result==NULL)) {
      if (unlock) u8_rw_unlock(&(sm->table_rwlock));
      return slotmap_fail(sm,"fd_slotmap_store");}
    if (sm->sm_keyvals!=cur_keyvals) sm->sm_free_keyvals=1;
    fd_decref(result->kv_val);
    result->kv_val=fd_incref(value);
    FD_XSLOTMAP_MARK_MODIFIED(sm);
    if (cur_allocd != allocd) { FD_XSLOTMAP_SET_NALLOCATED(sm,allocd); }
    if (cur_nslots != nslots) {
      FD_XSLOTMAP_SET_NSLOTS(sm,nslots);
      rv=1;}
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return rv;}
}

FD_EXPORT int fd_slotmap_add(struct FD_SLOTMAP *sm,lispval key,lispval value)
{
  int unlock=0, retval=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if (FD_TROUBLEP(key))
    return 0;
  else if (FD_TROUBLEP(value))
    return fd_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (EMPTYP(value))
    return 0;
  else if (sm->table_readonly) {
    fd_seterr(fd_ReadOnlyTable,"fd_slotmap_store",NULL,key);
    return -1;}
  else if (sm->table_uselock) {
    u8_write_lock(&sm->table_rwlock);
    unlock=1;}
  else NO_ELSE;
  {
    int cur_size=FD_XSLOTMAP_NUSED(sm), size=cur_size;
    int cur_space=FD_XSLOTMAP_NALLOCATED(sm), space=cur_space;
    struct FD_KEYVAL *cur_keyvals=sm->sm_keyvals;
    struct FD_KEYVAL *result=
      (sm->sm_sort_keyvals) ?
      (fd_sortvec_insert(key,&(sm->sm_keyvals),
                         &size,&space,SHRT_MAX,
                         sm->sm_free_keyvals)) :
      (fd_keyvec_insert(key,&(sm->sm_keyvals),
                        &size,&space,SHRT_MAX,
                        sm->sm_free_keyvals));
    if (PRED_FALSE(result==NULL)) {
      if (unlock) u8_rw_unlock(&sm->table_rwlock);
      return slotmap_fail(sm,"fd_slotmap_add");}
    /* If this allocated a new keyvals structure, it needs to be
       freed.  (sm_free_kevyvals==0) when the keyvals are allocated at
       the end of the slotmap structure itself. */
    if (sm->sm_keyvals!=cur_keyvals)
      sm->sm_free_keyvals=1;
    fd_incref(value);
    if ( (result->kv_val == VOID) || (result->kv_val == FD_UNBOUND) )
      result->kv_val=value;
    else {CHOICE_ADD(result->kv_val,value);}
    FD_XSLOTMAP_MARK_MODIFIED(sm);
    if (cur_space != space) {
      FD_XSLOTMAP_SET_NALLOCATED(sm,space); }
    if (cur_size  != size) {
      FD_XSLOTMAP_SET_NSLOTS(sm,size);
      retval=1;}}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return retval;
}

FD_EXPORT int fd_slotmap_drop(struct FD_SLOTMAP *sm,lispval key,lispval value)
{
  struct FD_KEYVAL *result; int size, unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if ((FD_TROUBLEP(key)))
    return fd_interr(key);
  else if ((FD_TROUBLEP(value)))
    return fd_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (sm->table_readonly) {
    fd_seterr(fd_ReadOnlyTable,"fd_slotmap_store",NULL,key);
    return -1;}
  else if (sm->table_uselock) {
    u8_write_lock(&sm->table_rwlock);
    unlock=1;}
  else NO_ELSE;
  size=FD_XSLOTMAP_NUSED(sm);
  result=fd_keyvec_get(key,sm->sm_keyvals,size);
  if (result) {
    lispval newval=((VOIDP(value)) ? (EMPTY) :
                   (fd_difference(result->kv_val,value)));
    if ( (newval == result->kv_val) &&
         (!(EMPTYP(newval))) ) {
      /* This is the case where, for example, value isn't on the slot.
         But we incref'd newvalue/result->kv_val, so we decref it.
         However, if the slot is already empty (for whatever reason),
         dropping the slot actually removes it from the slotmap. */
      fd_decref(newval);}
    else {
      FD_XSLOTMAP_MARK_MODIFIED(sm);
      if (EMPTYP(newval)) {
        int entries_to_move=(size-(result-sm->sm_keyvals))-1;
        fd_decref(result->kv_key); fd_decref(result->kv_val);
        memmove(result,result+1,entries_to_move*FD_KEYVAL_LEN);
        FD_XSLOTMAP_SET_NSLOTS(sm,size-1);}
      else {
        fd_decref(result->kv_val);
        result->kv_val=newval;}}}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  if (result)
    return 1;
  else return 0;
}

FD_EXPORT int fd_slotmap_delete(struct FD_SLOTMAP *sm,lispval key)
{
  struct FD_KEYVAL *result; int size, unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if ((FD_TROUBLEP(key)))
    return fd_interr(key);
  else if (EMPTYP(key))
    return 0;
  else if (sm->table_readonly) {
    fd_seterr(fd_ReadOnlyTable,"fd_slotmap_store",NULL,key);
    return -1;}
  else if (sm->table_uselock) {
    u8_write_lock(&sm->table_rwlock);
    unlock=1;}
  else NO_ELSE;
  size=FD_XSLOTMAP_NUSED(sm);
  result=fd_keyvec_get(key,sm->sm_keyvals,size);
  if (result) {
    int entries_to_move=(size-(result-sm->sm_keyvals))-1;
    fd_decref(result->kv_key); fd_decref(result->kv_val);
    memmove(result,result+1,entries_to_move*FD_KEYVAL_LEN);
    FD_XSLOTMAP_MARK_MODIFIED(sm);
    FD_XSLOTMAP_SET_NSLOTS(sm,size-1);}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  if (result) return 1; else return 0;
}

static int slotmap_getsize(struct FD_SLOTMAP *ptr)
{
  FD_CHECK_TYPE_RET(ptr,fd_slotmap_type);
  return FD_XSLOTMAP_NUSED(ptr);
}

static int slotmap_modified(struct FD_SLOTMAP *ptr,int flag)
{
  FD_CHECK_TYPE_RET(ptr,fd_slotmap_type);
  int modified=FD_XSLOTMAP_MODIFIEDP(ptr);
  if (flag<0)
    return modified;
  else if (flag) {
    FD_XSLOTMAP_MARK_MODIFIED(ptr);
    return modified;}
  else {
    FD_XSLOTMAP_CLEAR_MODIFIED(ptr);
    return modified;}
}

static int slotmap_readonly(struct FD_SLOTMAP *ptr,int flag)
{
  FD_CHECK_TYPE_RET(ptr,fd_slotmap_type);
  int readonly=FD_XSLOTMAP_READONLYP(ptr);
  if (flag<0)
    return readonly;
  else if (flag) {
    FD_XSLOTMAP_SET_READONLY(ptr);
    return readonly;}
  else {
    FD_XSLOTMAP_CLEAR_READONLY(ptr);
    return readonly;}
}

FD_EXPORT lispval fd_slotmap_keys(struct FD_SLOTMAP *sm)
{
  struct FD_KEYVAL *scan, *limit; int unlock=0;
  struct FD_CHOICE *result;
  lispval *write; int size, atomic=1;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (sm->table_uselock) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm);
  scan=sm->sm_keyvals;
  limit=scan+size;
  if (size==0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return EMPTY;}
  else if (size==1) {
    lispval key=fd_incref(scan->kv_key);
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return key;}
  /* Otherwise, copy the keys into a choice vector. */
  result=fd_alloc_choice(size);
  write=(lispval *)FD_XCHOICE_DATA(result);
  while (scan < limit) {
    lispval key=(scan++)->kv_key;
    if (!(FD_VOIDP(key))) {
      if (CONSP(key)) {fd_incref(key); atomic=0;}
      *write++=key;}}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return fd_init_choice(result,size,NULL,
                        ((FD_CHOICE_REALLOC)|(FD_CHOICE_DOSORT)|
                         ((atomic)?(FD_CHOICE_ISATOMIC):
                          (FD_CHOICE_ISCONSES))));
}

FD_EXPORT lispval fd_slotmap_values(struct FD_SLOTMAP *sm)
{
  int unlock=0;
  struct FD_PRECHOICE *prechoice; lispval results;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (FD_XSLOTMAP_NUSED(sm) == 0) return FD_EMPTY;
  if (sm->table_uselock) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  int size=FD_XSLOTMAP_NUSED(sm);
  if (size == 0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return EMPTY;}
  struct FD_KEYVAL *scan = sm->sm_keyvals, *limit = scan+size;
  if (size==1) {
    lispval value=fd_incref(scan->kv_val);
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return value;}
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_prechoice(NULL,7*(size),0);
  prechoice=FD_XPRECHOICE(results);
  while (scan < limit) {
    lispval value=(scan++)->kv_val;
    if (CONSP(value)) {fd_incref(value);}
    _prechoice_add(prechoice,value);}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_simplify_choice(results);
}

FD_EXPORT lispval fd_slotmap_assocs(struct FD_SLOTMAP *sm)
{
  struct FD_KEYVAL *scan, *limit; int unlock=0;
  struct FD_PRECHOICE *prechoice; lispval results;
  int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (sm->table_uselock) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm);
  scan=sm->sm_keyvals;
  limit=scan+size;
  if (size==0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return EMPTY;}
  else if (size==1) {
    lispval key=scan->kv_key, value=scan->kv_val;
    fd_incref(key); fd_incref(value);
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return fd_init_pair(NULL,key,value);}
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_prechoice(NULL,7*(size),0);
  prechoice=FD_XPRECHOICE(results);
  while (scan < limit) {
    lispval key=scan->kv_key, value=scan->kv_val;
    lispval assoc=fd_init_pair(NULL,key,value);
    fd_incref(key); fd_incref(value); scan++;
    _prechoice_add(prechoice,assoc);}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return fd_simplify_choice(results);
}

FD_EXPORT lispval fd_slotmap_max
  (struct FD_SLOTMAP *sm,lispval scope,lispval *maxvalp)
{
  lispval maxval=VOID, result=EMPTY;
  struct FD_KEYVAL *scan, *limit;
  int size, unlock=0;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (EMPTYP(scope)) return result;
  if (sm->table_uselock) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm);
  scan=sm->sm_keyvals;
  limit=scan+size;
  while (scan<limit) {
    if ((VOIDP(scope)) || (fd_overlapp(scan->kv_key,scope))) {
      if (EMPTYP(scan->kv_val)) {}
      else if (NUMBERP(scan->kv_val)) {
        if (VOIDP(maxval)) {
          result=fd_incref(scan->kv_key);
          maxval=fd_incref(scan->kv_val);}
        else {
          int cmp=numcompare(scan->kv_val,maxval);
          if (cmp>0) {
            fd_decref(result); fd_decref(maxval);
            result=fd_incref(scan->kv_key);
            maxval=fd_incref(scan->kv_val);}
          else if (cmp==0) {
            fd_incref(scan->kv_key);
            CHOICE_ADD(result,scan->kv_key);}}}}
    scan++;}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  if ((maxvalp) && (NUMBERP(maxval)))
    *maxvalp=fd_incref(maxval);
  return result;
}

FD_EXPORT lispval fd_slotmap_skim(struct FD_SLOTMAP *sm,lispval maxval,
                                 lispval scope)
{
  lispval result=EMPTY; int unlock=0;
  struct FD_KEYVAL *scan, *limit; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (EMPTYP(scope))
    return result;
  if (sm->table_uselock) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm);
  scan=sm->sm_keyvals;
  limit=scan+size;
  while (scan<limit) {
    if ((VOIDP(scope)) || (fd_overlapp(scan->kv_key,scope)))
      if (NUMBERP(scan->kv_val)) {
        int cmp=numcompare(scan->kv_val,maxval);
        if (cmp>=0) {
          fd_incref(scan->kv_key);
          CHOICE_ADD(result,scan->kv_key);}}
    scan++;}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return result;
}

FD_EXPORT lispval fd_init_slotmap
  (struct FD_SLOTMAP *ptr,
   int len,struct FD_KEYVAL *data)
{
  if (ptr == NULL) {
    ptr=u8_alloc(struct FD_SLOTMAP);
    FD_INIT_FRESH_CONS(ptr,fd_slotmap_type);}
  else {
    FD_SET_CONS_TYPE(ptr,fd_slotmap_type);}
  ptr->n_allocd=len;
  if (data) {
    ptr->sm_keyvals=data;
    ptr->n_slots=len;}
  else if (len) {
    ptr->sm_keyvals=u8_alloc_n(len,struct FD_KEYVAL);
    ptr->n_slots=0;}
  else {
    ptr->n_slots=0;
    ptr->sm_keyvals=NULL;}
  ptr->table_modified=ptr->table_readonly=0;
  ptr->table_uselock=1;
  ptr->sm_free_keyvals=1;
  ptr->sm_sort_keyvals=0;
  u8_init_rwlock(&(ptr->table_rwlock));
  return LISP_CONS(ptr);
}

FD_EXPORT lispval fd_make_slotmap(int space,int len,struct FD_KEYVAL *data)
{
  struct FD_SLOTMAP *ptr=
    u8_malloc((FD_SLOTMAP_LEN)+
              (space*(FD_KEYVAL_LEN)));
  struct FD_KEYVAL *kv=
    ((struct FD_KEYVAL *)(((unsigned char *)ptr)+FD_SLOTMAP_LEN));
  int i=0;
  FD_INIT_STRUCT(ptr,struct FD_SLOTMAP);
  FD_INIT_CONS(ptr,fd_slotmap_type);
  ptr->n_allocd=space; ptr->n_slots=len;
  if (data) while (i<len) {
      lispval key=data[i].kv_key, val=data[i].kv_val;
      kv[i].kv_key=fd_not_static(key);
      kv[i].kv_val=fd_not_static(val);
      i++;}
  while (i<space) {
    kv[i].kv_key=VOID;
    kv[i].kv_val=VOID;
    i++;}
  ptr->sm_keyvals=kv;
  ptr->table_modified=ptr->table_readonly=0;
  ptr->table_uselock=1;
  ptr->sm_free_keyvals=0; ptr->sm_sort_keyvals=0;
  u8_init_rwlock(&(ptr->table_rwlock));
  return LISP_CONS(ptr);
}

FD_EXPORT void fd_reset_slotmap(struct FD_SLOTMAP *ptr)
{
  int unlock = 0;
  if (ptr->table_uselock) {
    u8_write_lock(&(ptr->table_rwlock));
    unlock=1;}
  struct FD_KEYVAL *scan = FD_XSLOTMAP_KEYVALS(ptr);
  struct FD_KEYVAL *limit = scan + FD_XSLOTMAP_NUSED(ptr);
  while (scan<limit) {
    lispval key = scan->kv_key;
    lispval val = scan->kv_val;
    fd_decref(key); fd_decref(val);
    scan->kv_key=VOID;
    scan->kv_val=VOID;
    scan++;}
  ptr->n_slots=0;
  if (unlock) u8_rw_unlock(&(ptr->table_rwlock));
}

static lispval copy_slotmap(lispval smap,int flags)
{
  struct FD_SLOTMAP *cur=fd_consptr(fd_slotmap,smap,fd_slotmap_type);
  struct FD_SLOTMAP *fresh; int unlock=0;
  if (!(cur->sm_free_keyvals)) {
    /* If the original doesn't 'own' its keyvals, it's probably block
       allocated (with the keyvals in memory right after the slotmap
       struct. In this case, we assume we want to give the copy the
       same properties. */
    lispval copy;
    struct FD_SLOTMAP *consed;
    struct FD_KEYVAL *kvals;
    int i=0, len;
    copy=fd_make_slotmap(cur->n_allocd,cur->n_slots,cur->sm_keyvals);
    consed=(struct FD_SLOTMAP *)copy;
    kvals=consed->sm_keyvals; len=consed->n_slots;
    if (cur->table_uselock) {
      fd_read_lock_table(cur);
      unlock=1;}
    while (i<len) {
      lispval key=kvals[i].kv_key, val=kvals[i].kv_val;
      if ((flags&FD_FULL_COPY)||(FD_STATICP(key)))
        kvals[i].kv_key=fd_copier(key,flags);
      else fd_incref(key);
      if ((flags&FD_FULL_COPY)||(FD_STATICP(val)))
        kvals[i].kv_val=fd_copier(val,flags);
      else fd_incref(val);
      i++;}
    if (unlock) fd_unlock_table(cur);
    return copy;}
  else fresh=u8_alloc(struct FD_SLOTMAP);
  FD_INIT_STRUCT(fresh,struct FD_SLOTMAP);
  FD_INIT_CONS(fresh,fd_slotmap_type);
  if (cur->table_uselock) {
    fd_read_lock_table(cur);
    unlock=1;}
  fresh->table_modified=0;
  fresh->table_readonly=0;
  fresh->table_uselock=1;
  fresh->sm_free_keyvals=1;
  fresh->sm_sort_keyvals=0;
  if (FD_XSLOTMAP_NUSED(cur)) {
    int n=FD_XSLOTMAP_NUSED(cur);
    struct FD_KEYVAL *read=cur->sm_keyvals, *read_limit=read+n;
    struct FD_KEYVAL *write=u8_alloc_n(n,struct FD_KEYVAL);
    fresh->n_allocd=fresh->n_slots=n;
    fresh->sm_keyvals=write;
    memset(write,0,n*FD_KEYVAL_LEN);
    while (read<read_limit) {
      lispval key=read->kv_key, val=read->kv_val; read++;
      if (CONSP(key)) {
        if ((flags&FD_FULL_COPY)||(FD_STATICP(key)))
          write->kv_key=fd_copier(key,flags);
        else write->kv_key=fd_incref(key);}
      else write->kv_key=key;
      if (CONSP(val))
        if (PRECHOICEP(val))
          write->kv_val=fd_make_simple_choice(val);
        else if ((flags&FD_FULL_COPY)||(FD_STATICP(val)))
          write->kv_val=fd_copier(val,flags);
        else write->kv_val=fd_incref(val);
      else write->kv_val=val;
      write++;}}
  else {
    fresh->n_allocd=fresh->n_slots=0;
    fresh->sm_keyvals=NULL;}
  if (unlock) fd_unlock_table(cur);
  u8_init_rwlock(&(fresh->table_rwlock));
  return LISP_CONS(fresh);
}

FD_EXPORT int fd_copy_slotmap(struct FD_SLOTMAP *src,
                              struct FD_SLOTMAP *dest)
{
  if (dest->sm_keyvals) {
    if (dest->n_slots) {
      struct FD_KEYVAL *scan = dest->sm_keyvals;
      struct FD_KEYVAL *limit = scan + dest->n_slots;
      while (scan<limit) {
        lispval key = scan->kv_key;
        lispval val = scan->kv_val;
        fd_decref(key);
        fd_decref(val);
        scan->kv_key=VOID;
        scan->kv_val=VOID;
        scan++;}}
    if (dest->sm_free_keyvals) u8_free(dest->sm_keyvals);
    dest->sm_keyvals=NULL;
    u8_destroy_rwlock(&(dest->table_rwlock));}
  dest->n_slots = src->n_slots;
  dest->n_allocd = src->n_allocd;
  dest->sm_keyvals = u8_alloc_n(src->n_allocd,struct FD_KEYVAL);
  dest->table_readonly = src->table_readonly;
  dest->table_modified = src->table_modified;
  dest->table_finished = src->table_finished;
  dest->sm_sort_keyvals = src->sm_sort_keyvals;
  dest->sm_free_keyvals = 1;
  dest->table_uselock = 1;
  u8_init_rwlock(&(dest->table_rwlock));
  struct FD_KEYVAL *read = src->sm_keyvals;
  struct FD_KEYVAL *limit = read + src->n_slots;
  struct FD_KEYVAL *write = dest->sm_keyvals;
  while (read < limit) {
    write->kv_key = read->kv_key; fd_incref(write->kv_key);
    write->kv_val = read->kv_val; fd_incref(write->kv_val);
    write++; read++;}
  return dest->n_slots;
}

FD_EXPORT void fd_free_keyvals(struct FD_KEYVAL *kvals,int n_kvals)
{
  int i=0; while (i<n_kvals) {
    lispval key = kvals[i].kv_key;
    lispval val = kvals[i].kv_val;
    fd_decref(key);
    fd_decref(val);
    i++;}
}

FD_EXPORT void fd_free_slotmap(struct FD_SLOTMAP *c)
{
  struct FD_SLOTMAP *sm=(struct FD_SLOTMAP *)c;
  int unlock=0;
  if (sm->table_uselock) {
    fd_write_lock_table(sm);
    unlock=1;}
  int slotmap_size=FD_XSLOTMAP_NUSED(sm);
  const struct FD_KEYVAL *scan=sm->sm_keyvals;
  const struct FD_KEYVAL *limit=sm->sm_keyvals+slotmap_size;
  while (scan < limit) {
    fd_decref(scan->kv_key);
    fd_decref(scan->kv_val);
    scan++;}
  if (sm->sm_free_keyvals) u8_free(sm->sm_keyvals);
  if (unlock) fd_unlock_table(sm);
  u8_destroy_rwlock(&(sm->table_rwlock));
  memset(sm,0,FD_SLOTMAP_LEN);
}


static void recycle_slotmap(struct FD_RAW_CONS *c)
{
  struct FD_SLOTMAP *sm=(struct FD_SLOTMAP *)c;
  fd_free_slotmap(sm);
  u8_free(sm);
}

static int unparse_slotmap(u8_output out,lispval x)
{
  int unlock = 0;
  struct FD_SLOTMAP *sm=FD_XSLOTMAP(x);
  if (sm->table_uselock) {
    fd_read_lock_table(sm);
    unlock=1;}
  {
    int slotmap_size=FD_XSLOTMAP_NUSED(sm);
    const struct FD_KEYVAL *scan=sm->sm_keyvals;
    const struct FD_KEYVAL *limit=sm->sm_keyvals+slotmap_size;
    u8_puts(out,"#[");
    if (scan<limit) {
      fd_unparse(out,scan->kv_key); u8_putc(out,' ');
      fd_unparse(out,scan->kv_val);
      scan++;}
    while (scan< limit) {
      u8_putc(out,' ');
      fd_unparse(out,scan->kv_key); u8_putc(out,' ');
      fd_unparse(out,scan->kv_val);
      scan++;}
    u8_puts(out,"]");
  }
  if (unlock) fd_unlock_table(sm);
  return 1;
}
static int compare_slotmaps(lispval x,lispval y,fd_compare_flags flags)
{
  int result=0; int unlockx=0, unlocky=0;
  struct FD_SLOTMAP *smx=(struct FD_SLOTMAP *)x;
  struct FD_SLOTMAP *smy=(struct FD_SLOTMAP *)y;
  int compare_lengths=(!(flags&FD_COMPARE_RECURSIVE));
  int compare_slots=(flags&FD_COMPARE_SLOTS);
  if (smx->table_uselock) {fd_read_lock_table(smx); unlockx=1;}
  if (smy->table_uselock) {fd_read_lock_table(smy); unlocky=1;}
  int xsize=FD_XSLOTMAP_NUSED(smx), ysize=FD_XSLOTMAP_NUSED(smy);
  {
    if ((compare_lengths) && (xsize>ysize)) result=1;
    else if ((compare_lengths) && (xsize<ysize)) result=-1;
    else if (!(compare_slots)) {
      const struct FD_KEYVAL *xkeyvals=smx->sm_keyvals;
      const struct FD_KEYVAL *ykeyvals=smy->sm_keyvals;
      int i=0; while (i < xsize) {
        int cmp=LISP_COMPARE(xkeyvals[i].kv_key,ykeyvals[i].kv_key,flags);
        if (cmp) {result=cmp; break;}
        else i++;}
      if (result==0) {
        i=0; while (i < xsize) {
          int cmp=FD_QCOMPARE(xkeyvals[i].kv_val,ykeyvals[i].kv_val);
          if (cmp) {result=cmp; break;}
          else i++;}}}
    else {
      struct FD_KEYVAL _xkvbuf[17], *xkvbuf=_xkvbuf;
      struct FD_KEYVAL _ykvbuf[17], *ykvbuf=_ykvbuf;
      int i=0, limit=(ysize>xsize) ? (xsize) : (ysize);
      if (xsize>17) xkvbuf=u8_alloc_n(xsize,struct FD_KEYVAL);
      if (ysize>17) ykvbuf=u8_alloc_n(ysize,struct FD_KEYVAL);
      memcpy(xkvbuf,FD_XSLOTMAP_KEYVALS(smx),
             FD_KEYVAL_LEN*xsize);
      memcpy(ykvbuf,FD_XSLOTMAP_KEYVALS(smy),
             FD_KEYVAL_LEN*ysize);
      sort_keyvals(xkvbuf,xsize);
      sort_keyvals(ykvbuf,ysize);
      while (i<limit) {
        lispval xkey=xkvbuf[i].kv_key, ykey=ykvbuf[i].kv_key;
        int key_cmp=LISP_COMPARE(xkey,ykey,flags);
        if (key_cmp) {
          result=key_cmp;
          break;}
        else {
          lispval xval=xkvbuf[i].kv_val, yval=ykvbuf[i].kv_val;
          int val_cmp=LISP_COMPARE(xval,yval,flags);
          if (val_cmp) {
            result=val_cmp;
            break;}
          else i++;}}
      if (xkvbuf!=_xkvbuf) u8_free(xkvbuf);
      if (ykvbuf!=_ykvbuf) u8_free(ykvbuf);}}
  if (unlockx) fd_unlock_table(smx);
  if (unlocky) fd_unlock_table(smy);
  return result;
}


/* Lists into slotmaps */

static int build_table(lispval table,lispval key,lispval value)
{
  if (EMPTYP(value)) {
    if (fd_test(table,key,VOID))
      return fd_add(table,key,value);
    else return fd_store(table,key,value);}
  else return fd_add(table,key,value);
}

FD_EXPORT lispval fd_plist_to_slotmap(lispval plist)
{
  if (!(PAIRP(plist)))
    return fd_type_error(_("plist"),"fd_plist_to_slotmap",plist);
  else {
    lispval scan=plist, result=fd_init_slotmap(NULL,0,NULL);
    while ((PAIRP(scan))&&(PAIRP(FD_CDR(scan)))) {
      lispval key=FD_CAR(scan), value=FD_CADR(scan);
      build_table(result,key,value);
      scan=FD_CDR(scan); scan=FD_CDR(scan);}
    if (!(NILP(scan))) {
      fd_decref(result);
      return fd_type_error(_("plist"),"fd_plist_to_slotmap",plist);}
    else return result;}
}

FD_EXPORT int fd_sort_slotmap(lispval slotmap,int sorted)
{
  if (!(TYPEP(slotmap,fd_slotmap_type)))
    return fd_reterr(fd_TypeError,"fd_sort_slotmap",
                     u8_strdup("slotmap"),fd_incref(slotmap));
  else {
    struct FD_SLOTMAP *sm=(fd_slotmap)slotmap;
    if (!(sorted)) {
      if (sm->sm_sort_keyvals) {
        sm->sm_sort_keyvals=0;
        return 1;}
      else return 0;}
    else if (sm->sm_sort_keyvals)
      return 0;
    else if (sm->n_slots==0) {
      sm->sm_sort_keyvals=1;
      return 1;}
    else {
      struct FD_KEYVAL *keyvals=sm->sm_keyvals;
      sort_keyvals(keyvals,sm->n_slots);
      sm->sm_sort_keyvals=1;
      return 1;}}
}

FD_EXPORT lispval fd_alist_to_slotmap(lispval alist)
{
  if (!(PAIRP(alist)))
    return fd_type_error(_("alist"),"fd_alist_to_slotmap",alist);
  else {
    lispval result=fd_init_slotmap(NULL,0,NULL);
    FD_DOLIST(assoc,alist) {
      if (!(PAIRP(assoc))) {
        fd_decref(result);
        return fd_type_error(_("alist"),"fd_alist_to_slotmap",alist);}
      else build_table(result,FD_CAR(assoc),FD_CDR(assoc));}
    return result;}
}

FD_EXPORT lispval fd_blist_to_slotmap(lispval blist)
{
  if (!(PAIRP(blist)))
    return fd_type_error(_("binding list"),"fd_blist_to_slotmap",blist);
  else {
    lispval result=fd_init_slotmap(NULL,0,NULL);
    FD_DOLIST(binding,blist) {
      if (!(PAIRP(binding))) {
        fd_decref(result);
        return fd_type_error(_("binding list"),"fd_blist_to_slotmap",blist);}
      else {
        lispval key=FD_CAR(binding), scan=FD_CDR(binding);
        if (NILP(scan))
          build_table(result,key,EMPTY);
        else while (PAIRP(scan)) {
          build_table(result,key,FD_CAR(scan));
          scan=FD_CDR(scan);}
        if (!(NILP(scan))) {
          fd_decref(result);
          return fd_type_error
            (_("binding list"),"fd_blist_to_slotmap",blist);}}}
    return result;}
}

/* Schema maps */

FD_EXPORT lispval fd_make_schemap
  (struct FD_SCHEMAP *ptr,int size,int flags,
   lispval *schema,lispval *values)
{
  int i=0, stackvec = 0;
  lispval *vec = values;
  if (ptr == NULL) {
    if (flags&FD_SCHEMAP_INLINE) {
      void *consed = u8_malloc(sizeof(struct FD_SCHEMAP)+(sizeof(lispval)*size));
      ptr = (struct FD_SCHEMAP *) consed;
      vec = (lispval *) (consed + sizeof(struct FD_SCHEMAP));
      stackvec = 1;}
    else ptr=u8_alloc(struct FD_SCHEMAP);}
  if (vec == NULL) vec = u8_alloc_n(size,lispval);
  FD_INIT_STRUCT(ptr,struct FD_SCHEMAP);
  FD_INIT_CONS(ptr,fd_schemap_type);
  ptr->schemap_template=FD_VOID;
  ptr->schema_length=size;
  if (flags&FD_SCHEMAP_PRIVATE) {
    ptr->schemap_shared=0;
    ptr->table_schema=schema;}
  else if ( (flags) & (FD_SCHEMAP_COPY_SCHEMA) ) {
    lispval *copied = u8_alloc_n(size,lispval);
    memcpy(copied,schema,sizeof(lispval)*size);
    fd_incref_vec(copied,size),
    ptr->table_schema=copied;
    ptr->schemap_shared=0;}
  else {
    ptr->table_schema=schema;
    ptr->schemap_shared=1;}
  if (flags&FD_SCHEMAP_SORTED) ptr->schemap_sorted=1;
  if (flags&FD_SCHEMAP_READONLY) ptr->table_readonly=1;
  if (flags&FD_SCHEMAP_MODIFIED) ptr->table_modified=1;
  ptr->schema_values=vec;
  ptr->schemap_stackvec = stackvec;
  if (vec == values) {}
  else if (values)
    memcpy(vec,values,sizeof(lispval)*size);
  else while (i<size) vec[i++]=VOID;
  u8_init_rwlock(&(ptr->table_rwlock));
  ptr->table_uselock=1;
  return LISP_CONS(ptr);
}

FD_FASTOP void lispv_swap(lispval *a,lispval *b)
{
  lispval t;
  t = *a;
  *a = *b;
  *b = t;
}

FD_EXPORT void fd_sort_schema(int n,lispval *v)
{
  unsigned i, j, ln, rn;
  while (n > 1) {
    lispv_swap(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (v[j] > v[0]);
      do ++i; while (i < j && v[i] < v[0]);
      if (i >= j) break; else {}
      lispv_swap(&v[i], &v[j]);}
    lispv_swap(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      fd_sort_schema(ln, v); v += j; n = rn;}
    else {fd_sort_schema(rn,v + j); n = ln;}}
}

FD_EXPORT lispval *fd_register_schema(int n,lispval *schema)
{
  fd_sort_schema(n,schema);
  return schema;
}

FD_EXPORT void fd_reset_schemap(struct FD_SCHEMAP *ptr)
{
  lispval *scan = ptr->schema_values;
  lispval *limit = scan+ptr->schema_length;
  while (scan < limit) {
    lispval value = *scan;
    *scan++ = FD_VOID;
    fd_decref(value);}
}

static lispval copy_schemap(lispval schemap,int flags)
{
  int unlock = 0;
  struct FD_SCHEMAP *ptr=
    fd_consptr(struct FD_SCHEMAP *,schemap,fd_schemap_type);
  int i=0, size=FD_XSCHEMAP_SIZE(ptr);
  struct FD_SCHEMAP *nptr =
    u8_malloc(sizeof(struct FD_SCHEMAP)+(size*sizeof(lispval)));
  lispval *ovalues=ptr->schema_values;
  lispval *values= (lispval *)(((void *)nptr)+sizeof(struct FD_SCHEMAP));
  lispval *schema=ptr->table_schema, *nschema=NULL;
  if (FD_XSCHEMAP_USELOCKP(ptr)) {
    fd_read_lock_table(ptr);
    unlock=1;}
  FD_INIT_STRUCT(nptr,struct FD_SCHEMAP);
  FD_INIT_CONS(nptr,fd_schemap_type);
  nptr->schemap_template=FD_VOID;
  if (ptr->schemap_onstack)
    nptr->table_schema=nschema=u8_alloc_n(size,lispval);
  else nptr->table_schema=schema;
  if ( (nptr->table_schema != schema) )
    while (i < size) {
      lispval val=ovalues[i];
      if (CONSP(val))
        if ((flags&FD_FULL_COPY)||(FD_STATICP(val)))
          values[i]=fd_copier(val,flags);
        else if (PRECHOICEP(val))
          values[i]=fd_make_simple_choice(val);
        else values[i]=fd_incref(val);
      else values[i]=val;
      nschema[i]=schema[i];
      i++;}
  else if (flags) while (i<size) {
      lispval val=ovalues[i];
      if (CONSP(val))
        if (PRECHOICEP(val))
          values[i]=fd_make_simple_choice(val);
        else if ((flags&FD_FULL_COPY)||(FD_STATICP(val)))
          values[i]=fd_copier(val,flags);
        else values[i]=fd_incref(val);
      else values[i]=val;
      i++;}
  else while (i < size) {
      values[i]=fd_incref(ovalues[i]);
      i++;}
  nptr->schema_values=values;
  nptr->schema_length=size;
  if  ( ptr->table_schema == nptr->table_schema ) {
    ptr->schemap_shared=nptr->schemap_shared=1;}
  if (unlock) fd_unlock_table(ptr);
  nptr->schemap_stackvec=1;
  nptr->table_uselock=1;
  u8_init_rwlock(&(nptr->table_rwlock));
  return LISP_CONS(nptr);
}

FD_EXPORT lispval fd_init_schemap
  (struct FD_SCHEMAP *ptr, int size, struct FD_KEYVAL *init)
{
  int i=0, stackvec = 0;
  lispval *new_schema, *new_vals;
  if (ptr == NULL) {
    ptr=u8_malloc(sizeof(struct FD_SCHEMAP)+(size*sizeof(lispval)));
    FD_INIT_FRESH_CONS(ptr,fd_schemap_type);
    new_vals=(lispval *)(((void *)ptr)+sizeof(struct FD_SCHEMAP));
    stackvec=1;}
  else {
    FD_SET_CONS_TYPE(ptr,fd_schemap_type);
    new_vals = u8_alloc_n(size,lispval);}
  new_schema=u8_alloc_n(size,lispval);
  ptr->schemap_template=FD_VOID;
  ptr->schema_values=new_vals;
  ptr->schema_length=size;
  ptr->schemap_sorted=0;
  sort_keyvals(init,size);
  while (i<size) {
    new_schema[i]=init[i].kv_key;
    new_vals[i]=init[i].kv_val;
    i++;}
  /* ptr->table_schema=fd_register_schema(size,new_schema); */
  ptr->table_schema = new_schema;
  if (ptr->table_schema != new_schema) {
    ptr->schemap_shared=1;
    u8_free(new_schema);}
  if (stackvec) ptr->schemap_stackvec = 1;
  u8_init_rwlock(&(ptr->table_rwlock));
  ptr->table_uselock=1;
  return LISP_CONS(ptr);
}

FD_EXPORT lispval _fd_schemap_get
  (struct FD_SCHEMAP *sm,lispval key,lispval dflt)
{
  return fd_schemap_get(sm,key,dflt);
}

FD_EXPORT lispval _fd_schemap_test
  (struct FD_SCHEMAP *sm,lispval key,lispval val)
{
  return fd_schemap_test(sm,key,val);
}


FD_EXPORT int fd_schemap_store
   (struct FD_SCHEMAP *sm,lispval key,lispval value)
{
  int slotno, size, unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_schemap_type);
  if ((FD_TROUBLEP(key)))
    return fd_interr(key);
  else if ((FD_TROUBLEP(value)))
    return fd_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (sm->table_readonly) {
    fd_seterr(fd_ReadOnlyTable,"fd_schemap_store",NULL,key);
    return -1;}
  else if (FD_XSCHEMAP_USELOCKP(sm)) {
    fd_write_lock_table(sm);
    unlock=1;}
  else NO_ELSE;
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sm->schemap_sorted);
  if (slotno>=0) {
    if (sm->schemap_stackvals) {
      fd_incref_vec(sm->schema_values,size);
      sm->schemap_stackvals = 0;}
    fd_decref(sm->schema_values[slotno]);
    sm->schema_values[slotno]=fd_incref(value);
    FD_XSCHEMAP_MARK_MODIFIED(sm);
    if (unlock) fd_unlock_table(sm);
    return 1;}
  else {
    if (unlock) fd_unlock_table(sm);
    fd_seterr(fd_NoSuchKey,"fd_schemap_store",NULL,key);
    return -1;}
}

FD_EXPORT int fd_schemap_add
  (struct FD_SCHEMAP *sm,lispval key,lispval value)
{
  int slotno, size, unlock = 0;
  FD_CHECK_TYPE_RET(sm,fd_schemap_type);
  if ((FD_TROUBLEP(key)))
    return fd_interr(key);
  else if ((FD_TROUBLEP(value)))
    return fd_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (EMPTYP(value))
    return 0;
  else if ((FD_TROUBLEP(value)))
    return fd_interr(value);
  else if (sm->table_readonly) {
    fd_seterr(fd_ReadOnlyTable,"fd_schemap_add",NULL,key);
    return -1;}
  else if (FD_XSCHEMAP_USELOCKP(sm)) {
    fd_write_lock_table(sm);
    unlock=1;}
  else NO_ELSE;
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sm->schemap_sorted);
  if (slotno>=0) {
    fd_incref(value);
    if (sm->schemap_stackvals) {
      fd_incref_vec(sm->schema_values,size);
      sm->schemap_stackvals = 0;}
    CHOICE_ADD(sm->schema_values[slotno],value);
    FD_XSCHEMAP_MARK_MODIFIED(sm);
    if (unlock) fd_unlock_table(sm);
    return 1;}
  else {
    if (unlock) fd_unlock_table(sm);
    fd_seterr(fd_NoSuchKey,"fd_schemap_add",NULL,key);
    return -1;}
}

FD_EXPORT int fd_schemap_drop
  (struct FD_SCHEMAP *sm,lispval key,lispval value)
{
  int slotno, size, unlock = 0;
  FD_CHECK_TYPE_RET(sm,fd_schemap_type);
  if ((FD_TROUBLEP(key)))
    return fd_interr(key);
  else if ((FD_TROUBLEP(value)))
    return fd_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (EMPTYP(value))
    return 0;
  else if ((FD_TROUBLEP(value)))
    return fd_interr(value);
  else if (sm->table_readonly) {
    fd_seterr(fd_ReadOnlyTable,"fd_schemap_drop",NULL,key);
    return -1;}
  else if (FD_XSCHEMAP_USELOCKP(sm)) {
    fd_write_lock_table(sm);
    unlock=1;}
  else NO_ELSE;
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sm->schemap_sorted);
  if (slotno>=0) {
    if (sm->schemap_stackvals) {
      fd_incref_vec(sm->schema_values,size);
      sm->schemap_stackvals = 0;}
    lispval oldval=sm->schema_values[slotno];
    lispval newval=((VOIDP(value)) ? (EMPTY) :
                   (fd_difference(oldval,value)));
    if (newval == oldval)
      fd_decref(newval);
    else {
      FD_XSCHEMAP_MARK_MODIFIED(sm);
      fd_decref(oldval);
      sm->schema_values[slotno]=newval;}
    if (unlock) fd_unlock_table(sm);
    return 1;}
  else {
    if (unlock) fd_unlock_table(sm);
    return 0;}
}

static int schemap_getsize(struct FD_SCHEMAP *ptr)
{
  FD_CHECK_TYPE_RET(ptr,fd_schemap_type);
  return FD_XSCHEMAP_SIZE(ptr);
}

static int schemap_modified(struct FD_SCHEMAP *ptr,int flag)
{
  FD_CHECK_TYPE_RET(ptr,fd_schemap_type);
  int modified=FD_XSCHEMAP_MODIFIEDP(ptr);
  if (flag<0)
    return modified;
  else if (flag) {
    ptr->table_modified=1;
    return modified;}
  else {
    ptr->table_modified=0;
    return modified;}
}

static int schemap_readonly(struct FD_SCHEMAP *ptr,int flag)
{
  FD_CHECK_TYPE_RET(ptr,fd_schemap_type);
  int readonly=FD_XSCHEMAP_READONLYP(ptr);
  if (flag<0)
    return readonly;
  else if (flag) {
    ptr->table_readonly=1;
    return readonly;}
  else {
    ptr->table_readonly=0;
    return readonly;}
}

FD_EXPORT lispval fd_schemap_keys(struct FD_SCHEMAP *sm)
{
  FD_CHECK_TYPE_RETDTYPE(sm,fd_schemap_type);
  {
    int size=FD_XSCHEMAP_SIZE(sm);
    if (size==0)
      return EMPTY;
    else if (size==1)
      return fd_incref(sm->table_schema[0]);
    else {
      struct FD_CHOICE *ch=fd_alloc_choice(size);
      memcpy((lispval *)FD_XCHOICE_DATA(ch),sm->table_schema,
             LISPVEC_BYTELEN(size));
      if (sm->schemap_sorted)
        return fd_init_choice(ch,size,sm->table_schema,0);
      else return fd_init_choice(ch,size,sm->table_schema,
                                 FD_CHOICE_INCREF|FD_CHOICE_DOSORT);}}
}

static void recycle_schemap(struct FD_RAW_CONS *c)
{
  int unlock = 0;
  struct FD_SCHEMAP *sm=(struct FD_SCHEMAP *)c;
  if (FD_XSCHEMAP_USELOCKP(sm)) {
    fd_write_lock_table(sm);
    unlock = 1;}
  else {}
  lispval template = sm->schemap_template;
  if ( (template) && (FD_CONSP(template)) ) {
    fd_decref(template);
    sm->schemap_template=FD_VOID;}
  int schemap_size=FD_XSCHEMAP_SIZE(sm);
  int stack_vals = sm->schemap_stackvals;
  if ( (sm->schema_values) &&  (PRED_TRUE (! stack_vals ) ) ) {
    lispval *scan=sm->schema_values;
    lispval *limit=sm->schema_values+schemap_size;
    while (scan < limit) {fd_decref(*scan); scan++;}
    if (! (sm->schemap_stackvec) )
      u8_free(sm->schema_values);}
  if ((sm->table_schema) && (!(sm->schemap_shared)))
    u8_free(sm->table_schema);
  if (unlock) fd_unlock_table(sm);
  u8_destroy_rwlock(&(sm->table_rwlock));
  memset(sm,0,FD_SCHEMAP_LEN);
  u8_free(sm);
}
static int unparse_schemap(u8_output out,lispval x)
{
  struct FD_SCHEMAP *sm=FD_XSCHEMAP(x);
  fd_read_lock_table(sm);
  {
    int i=0, schemap_size=FD_XSCHEMAP_SIZE(sm);
    lispval *schema=sm->table_schema, *values=sm->schema_values;
    u8_puts(out,"[");
    if (i<schemap_size) {
      fd_unparse(out,schema[i]); u8_putc(out,' ');
      fd_unparse(out,values[i]); i++;}
    while (i<schemap_size) {
      u8_putc(out,' ');
      fd_unparse(out,schema[i]); u8_putc(out,' ');
      fd_unparse(out,values[i]); i++;}
    u8_puts(out,"]");
  }
  fd_unlock_table(sm);
  return 1;
}
static int compare_schemaps(lispval x,lispval y,fd_compare_flags flags)
{
  int result=0;
  struct FD_SCHEMAP *smx=(struct FD_SCHEMAP *)x;
  struct FD_SCHEMAP *smy=(struct FD_SCHEMAP *)y;
  fd_read_lock_table(smx); fd_read_lock_table(smy);
  {
    int xsize=FD_XSCHEMAP_SIZE(smx), ysize=FD_XSCHEMAP_SIZE(smy);
    if (xsize>ysize) result=1;
    else if (xsize<ysize) result=-1;
    else {
      lispval *xschema=smx->table_schema, *yschema=smy->table_schema;
      lispval *xvalues=smx->schema_values, *yvalues=smy->schema_values;
      int i=0; while (i < xsize) {
        int cmp=FD_QCOMPARE(xschema[i],yschema[i]);
        if (cmp) {result=cmp; break;}
        else i++;}
      if (result==0) {
        i=0; while (i < xsize) {
          int cmp=FD_QCOMPARE(xvalues[i],yvalues[i]);
          if (cmp) {result=cmp; break;}
          else i++;}}}}
  fd_unlock_table(smx); fd_unlock_table(smy);
  return result;
}

/* Hash functions */

/* This is the point at which we resize hashtables. */
#define hashtable_needs_resizep(ht)\
   ((ht->table_n_keys*ht->table_load_factor)>(ht->ht_n_buckets))
/* This is the target size for resizing. */
#define hashtable_resize_target(ht) \
   ((ht->table_n_keys*ht->table_load_factor))

#define hashset_needs_resizep(hs)\
   ((hs->hs_n_elts*hs->hs_load_factor)>(hs->hs_n_buckets))
/* This is the target size for resizing. */
#define hashset_resize_target(hs) \
   (hs->hs_n_elts*hs->hs_load_factor)

static double default_hashtable_loading=1.2;
static double default_hashset_loading=1.7;

/* These are all the higher of prime pairs around powers of 2.  They are
    used to select good hashtable sizes. */
/* static unsigned int hashtable_sizes[]=
 {19, 43, 73, 139, 271, 523, 1033, 2083, 4129, 8221, 16453,
  32803, 65539, 131113, 262153, 524353, 1048891, 2097259,
  4194583,
  8388619, 16777291, 32000911, 64000819, 128000629,
  256001719, 0}; */

/* These are the new numbers, based on interweb research */
static unsigned int hashtable_sizes[]={
  53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593, 49157,
  98317, 196613, 393241, 786433, 1572869, 3145739, 6291469,
  12582917, 25165843, 50331653, 100663319, 201326611, 402653189,
  805306457, 1610612741};

/* This finds a hash table size which is larger than min */
FD_EXPORT unsigned int fd_get_hashtable_size(unsigned int min)
{
  unsigned int i=0;
  while (hashtable_sizes[i])
    if (hashtable_sizes[i] > min) return hashtable_sizes[i];
    else i++;
  return min;
}

#define FIXNUM_MULTIPLIER 220000073 
#define OID_MULTIPLIER 240000083
#define CONSTANT_MULTIPLIER 250000073
#define SYMBOL_MULTIPLIER 260000093
static unsigned int type_multipliers[]=
  {200000093,210000067,FIXNUM_MULTIPLIER,OID_MULTIPLIER,
   CONSTANT_MULTIPLIER,SYMBOL_MULTIPLIER};
#define N_TYPE_MULTIPLIERS 6

FD_FASTOP unsigned int mult_hash_bytes
  (const unsigned char *start,int len)
{
  unsigned int h=0;
  unsigned *istart=(unsigned int *)start, asint=0;
  const unsigned int *scan=istart, *limit=istart+len/4;
  const unsigned char *tail=start+(len/4)*4;
  while (scan<limit) h=((127*h)+*scan++)%MYSTERIOUS_MODULUS;
  switch (len%4) {
  case 0: asint=1; break;
  case 1: asint=tail[0]; break;
  case 2: asint=tail[0]|(tail[1]<<8); break;
  case 3: asint=tail[0]|(tail[1]<<8)|(tail[2]<<16); break;}
  h=((127*h)+asint)%MYSTERIOUS_MODULUS;
  return h;
}

FD_EXPORT unsigned int fd_hash_bytes(u8_string string,int len)
{
  return mult_hash_bytes(string,len);
}

/* Hashing dtype pointers */

static unsigned int hash_elts(lispval *x,unsigned int n);

static unsigned int hash_lisp(lispval x)
{
  enum FD_PTR_TYPE ptr_type = FD_PTR_MANIFEST_TYPE(x);
  if (ptr_type == fd_cons_type) {
    struct FD_CONS *cons = (struct FD_CONS *)x;
    ptr_type = FD_CONS_TYPE(cons);
    switch (ptr_type) {
    case fd_string_type: {
      struct FD_STRING *s = (fd_string) cons;
      return mult_hash_bytes(s->str_bytes,s->str_bytelen);}
    case fd_packet_type: case fd_secret_type: {
      struct FD_STRING *s = (fd_string) cons;
      return mult_hash_bytes(s->str_bytes,s->str_bytelen);}
    case fd_pair_type: {
      struct FD_PAIR *pair = (fd_pair) cons;
      lispval car=pair->car, cdr=pair->cdr;
      unsigned int hcar=hash_lisp(car), hcdr=hash_lisp(cdr);
      return hash_mult(hcar,hcdr);}
    case fd_vector_type: {
      struct FD_VECTOR *v = (fd_vector) cons;
      return hash_elts(v->vec_elts,v->vec_length);}
    case fd_compound_type: {
      struct FD_COMPOUND *c = (fd_compound) cons;
      if (c->compound_isopaque) {
        int ctype = FD_PTR_TYPE(x);
        if ( (ctype>0) && (ctype<N_TYPE_MULTIPLIERS) )
          return hash_mult(x,type_multipliers[ctype]);
        else return hash_mult(x,MYSTERIOUS_MULTIPLIER);}
      else return hash_mult
             (hash_lisp(c->compound_typetag),
              hash_elts(&(c->compound_0),c->compound_length));}
    case fd_slotmap_type: {
      struct FD_SLOTMAP *sm = (fd_slotmap) cons;
      lispval *kv=(lispval *)sm->sm_keyvals;
      return hash_elts(kv,sm->n_slots*2);}
    case fd_choice_type: {
      struct FD_CHOICE *ch = (fd_choice) cons;
      int size=FD_XCHOICE_SIZE(ch);
      return hash_elts((lispval *)(FD_XCHOICE_DATA(ch)),size);}
    case fd_prechoice_type: {
      lispval simple=fd_make_simple_choice(x);
      int hash=hash_lisp(simple);
      fd_decref(simple);
      return hash;}
    case fd_qchoice_type: {
      struct FD_QCHOICE *ch = (fd_qchoice) cons;
      return hash_lisp(ch->qchoiceval);}
    default: {
      if ((ptr_type<FD_TYPE_MAX) && (fd_hashfns[ptr_type]))
        return fd_hashfns[ptr_type](x,fd_hash_lisp);
      else return hash_mult(x,MYSTERIOUS_MULTIPLIER);}}}
  else if (ptr_type == fd_oid_type)
    return hash_mult(x,OID_MULTIPLIER);
  else if (ptr_type == fd_fixnum_type)
    return hash_mult(x,FIXNUM_MULTIPLIER);
  else {
   ptr_type = FD_IMMEDIATE_TYPE(x);
   if (ptr_type == fd_constant_type)
     return hash_mult(x,CONSTANT_MULTIPLIER);
   else if (ptr_type == fd_symbol_type)
     return hash_mult(x,SYMBOL_MULTIPLIER);
   else if ((ptr_type>fd_symbol_type) && (ptr_type<N_TYPE_MULTIPLIERS))
      return hash_mult(x,type_multipliers[ptr_type]);
    else return hash_mult(x,MYSTERIOUS_MULTIPLIER);}
}

FD_EXPORT unsigned int fd_hash_lisp(lispval x)
{
  return hash_lisp(x);
}

/* hash_elts: (static)
     arguments: a pointer to a vector of dtype pointers and a size
     Returns: combines the elements' hashes into a single hash
*/
static unsigned int hash_elts(lispval *x,unsigned int n)
{
  lispval *limit=x+n; int sum=0;
  while (x < limit) {
    unsigned int h=hash_lisp(*x);
    sum=hash_combine(sum,h); sum=sum%(MYSTERIOUS_MODULUS); x++;}
  return sum;
}

/* Hashvecs */

static struct FD_KEYVAL *hashvec_get
(lispval key,struct FD_HASH_BUCKET **slots,int n_slots,
 struct FD_HASH_BUCKET ***bucket_loc)
{
  unsigned int hash=fd_hash_lisp(key), offset=compute_offset(hash,n_slots);
  struct FD_HASH_BUCKET *he=slots[offset];
  if (bucket_loc) *bucket_loc = &(slots[offset]);
  if (he == NULL)
    return NULL;
  else if (PRED_TRUE(he->bucket_len == 1))
    if (LISP_EQUAL(key,he->kv_val0.kv_key))
      return &(he->kv_val0);
    else return NULL;
  else return fd_sortvec_get(key,&(he->kv_val0),he->bucket_len);
}

FD_FASTOP struct FD_KEYVAL *hash_bucket_insert
(lispval key,struct FD_HASH_BUCKET **hep)
{
  struct FD_HASH_BUCKET *he=*hep;
  struct FD_KEYVAL *keyvals=&(he->kv_val0);
  int size = he->bucket_len, found = 0;
  struct FD_KEYVAL *bottom=keyvals, *top=bottom+(size-1);
  struct FD_KEYVAL *middle=bottom+(top-bottom)/2;;
  while (top>=bottom) { /* (!(top<bottom)) */
    middle=bottom+(top-bottom)/2;
    if (LISP_EQUAL(key,middle->kv_key)) {found=1; break;}
    else if (cons_compare(key,middle->kv_key)<0) top=middle-1;
    else bottom=middle+1;}
  if (found)
    return middle;
  else {
    int mpos=(middle-keyvals), dir=(bottom>middle), ipos=mpos+dir;
    struct FD_KEYVAL *insert_point;
    struct FD_HASH_BUCKET *new_hashentry=
      /* We don't need to use size+1 here because FD_HASH_BUCKET includes
         one value. */
      u8_realloc(he,(sizeof(struct FD_HASH_BUCKET)+
                     (size)*FD_KEYVAL_LEN));
    memset((((unsigned char *)new_hashentry)+
            (sizeof(struct FD_HASH_BUCKET))+
            ((size-1)*FD_KEYVAL_LEN)),
           0,FD_KEYVAL_LEN);
    *hep=new_hashentry;
    new_hashentry->bucket_len++;
    insert_point=&(new_hashentry->kv_val0)+ipos;
    memmove(insert_point+1,insert_point,
            FD_KEYVAL_LEN*(size-ipos));
    if (CONSP(key)) {
      if (FD_STATICP(key))
        insert_point->kv_key=fd_copy(key);
      else insert_point->kv_key=fd_incref(key);}
    else insert_point->kv_key=key;
    insert_point->kv_val=EMPTY;
    return insert_point;}
}

FD_FASTOP struct FD_HASH_BUCKET *hash_bucket_remove
(struct FD_HASH_BUCKET *bucket,struct FD_KEYVAL *slot)
{
  int len = bucket->bucket_len;
  struct FD_KEYVAL *base = &(bucket->kv_val0);
  if ( (len == 1) && (slot == base) ) {
    u8_free(bucket);
    return NULL;}
  else {
    int offset = slot - base;
    size_t new_size = sizeof(struct FD_HASH_BUCKET)+(FD_KEYVAL_LEN*(len-2));
    assert( ( offset >= 0 ) && (offset < len ) );
    memmove(slot,slot+1,FD_KEYVAL_LEN*(len-(offset+1)));
    struct FD_HASH_BUCKET *new_bucket=u8_realloc(bucket,new_size);
    new_bucket->bucket_len--;
    return new_bucket;}
}

FD_FASTOP struct FD_KEYVAL *hashvec_insert
  (lispval key,struct FD_HASH_BUCKET **slots,int n_slots,int *n_keys)
{
  unsigned int hash=fd_hash_lisp(key), offset=compute_offset(hash,n_slots);
  struct FD_HASH_BUCKET *he=slots[offset];
  if (he == NULL) {
    he=u8_alloc(struct FD_HASH_BUCKET);
    FD_INIT_STRUCT(he,struct FD_HASH_BUCKET);
    he->bucket_len=1;
    he->kv_val0.kv_key=fd_getref(key);
    he->kv_val0.kv_val=EMPTY;
    slots[offset]=he;
    if (n_keys) (*n_keys)++;
    return &(he->kv_val0);}
  else if (he->bucket_len == 1) {
    if (LISP_EQUAL(key,he->kv_val0.kv_key))
      return &(he->kv_val0);
    else {
      if (n_keys) (*n_keys)++;
      return hash_bucket_insert(key,&slots[offset]);}}
  else {
    int size=he->bucket_len;
    struct FD_KEYVAL *kv=hash_bucket_insert(key,&slots[offset]);
    if ((n_keys) && (slots[offset]->bucket_len > size))
      (*n_keys)++;
    return kv;}
}

FD_EXPORT struct FD_KEYVAL *fd_hashvec_get
  (lispval key,struct FD_HASH_BUCKET **slots,int n_slots)
{
  return hashvec_get(key,slots,n_slots,NULL);
}

FD_EXPORT struct FD_KEYVAL *fd_hash_bucket_insert
(lispval key,struct FD_HASH_BUCKET **hep)
{
  return hash_bucket_insert(key,hep);
}

FD_EXPORT struct FD_KEYVAL *fd_hashvec_insert
  (lispval key,struct FD_HASH_BUCKET **slots,int n_slots,int *n_keys)
{
  return hashvec_insert(key,slots,n_slots,n_keys);
}

/* Hashtables */

/* Optimizing PRECHOICEs in hashtables

   An PRECHOICE in a hashtable should always be a unique value, since
   adding to a key in the table shouldn't effect anything else,
   especially whatever PRECHOICE was passed in.

   We implement this as follows:

   When storing, if we have a PRECHOICE, we always copy it and set
     uselock to zero in the copy.

   When adding, we don't bother locking and just add straight away.

   When getting, we normalize the results and return that.

   When dropping, we normalize and store that.
*/

FD_EXPORT lispval fd_hashtable_get
  (struct FD_HASHTABLE *ht,lispval key,lispval dflt)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (EMPTYP(key))
    return EMPTY;
  else if (ht->table_n_keys == 0)
    return fd_incref(dflt);
  else if (ht->table_uselock) {
    fd_read_lock_table(ht);
    unlock=1; }
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    /* Race condition, if someone dropped the sole key while we were
       busy locking the table. */
    if (unlock) fd_unlock_table(ht);
    return fd_incref(dflt);}
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    lispval rv=fd_incref(result->kv_val);
    if (VOIDP(rv)) {
      if (unlock) fd_unlock_table(ht);
      return fd_incref(dflt);}
    else if (PRECHOICEP(rv)) {
      if (unlock) fd_unlock_table(ht); fd_decref(rv);
      if (unlock) fd_write_lock_table(ht);
      result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
      if (result) {
        rv = result->kv_val;
        if (FD_PRECHOICEP(rv)) {
          lispval norm = fd_make_simple_choice(result->kv_val);
          if (unlock) fd_unlock_table(ht);
          return norm;}
        else return fd_incref(rv);}
      else return FD_EMPTY;}
    else {
      if (unlock) fd_unlock_table(ht);
      return rv;}}
  else {
    if (unlock) fd_unlock_table(ht);
    return fd_incref(dflt);}
}

FD_EXPORT lispval fd_hashtable_get_nolock
  (struct FD_HASHTABLE *ht,lispval key,lispval dflt)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (FD_EMPTYP(key))
    return FD_EMPTY;
  else if (ht->table_n_keys == 0)
    return fd_incref(dflt);
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    lispval rv=fd_incref(result->kv_val);
    if (FD_VOIDP(rv))
      return fd_incref(dflt);
    else if (PRECHOICEP(rv)) {
      lispval norm = fd_make_simple_choice(rv);
      fd_decref(rv);
      return norm;}
    else return rv;}
  else {
    return fd_incref(dflt);}
}

FD_EXPORT lispval fd_hashtable_get_noref
  (struct FD_HASHTABLE *ht,lispval key,lispval dflt)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (FD_EMPTYP(key))
    return FD_EMPTY;
  else if (ht->table_n_keys == 0)
    return dflt;
  else if (ht->table_uselock) {
    fd_read_lock_table(ht);
    unlock=1; }
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    /* Race condition */
    if (unlock) fd_unlock_table(ht);
    return dflt;}
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    lispval rv = fd_incref(result->kv_val);
    if (PRECHOICEP(rv)) {
      lispval norm = fd_make_simple_choice(rv);
      fd_decref(rv);
      if (unlock) fd_unlock_table(ht);
      return norm;}
    else {
      if (unlock) fd_unlock_table(ht);
      if (FD_VOIDP(rv))
        return fd_incref(dflt);
      else return rv;}}
  else {
    if (unlock) fd_unlock_table(ht);
    return dflt;}
}

FD_EXPORT lispval fd_hashtable_get_nolockref
  (struct FD_HASHTABLE *ht,lispval key,lispval dflt)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (FD_EMPTYP(key))
    return FD_EMPTY;
  else if (ht->table_n_keys == 0)
    return dflt;
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    lispval rv=result->kv_val;
    if (VOIDP(rv)) return dflt;
    else if (PRECHOICEP(rv))
      return fd_make_simple_choice(rv);
    else return rv;}
  else return dflt;
}

FD_EXPORT int fd_hashtable_probe(struct FD_HASHTABLE *ht,lispval key)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (FD_EMPTYP(key))
    return FD_EMPTY;
  else if (ht->table_n_keys == 0)
    return 0;
  else if (ht->table_uselock) {
    fd_read_lock_table(ht);
    unlock=1; }
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    if (unlock) fd_unlock_table(ht);
    return 0;}
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    if (unlock) fd_unlock_table(ht);
    return 1;}
  else {
    if (unlock) u8_rw_unlock((&(ht->table_rwlock)));
    return 0;}
}

static int hashtable_test(struct FD_HASHTABLE *ht,lispval key,lispval val)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (FD_EMPTYP(key))
    return FD_EMPTY;
  else if (ht->table_n_keys == 0)
    return 0;
  else if (ht->table_uselock) {
    fd_read_lock_table(ht);
    unlock=1;}
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    /* Avoid race condition */
    if (unlock) fd_unlock_table(ht);
    return 0;}
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    lispval current=result->kv_val; int cmp;
    if (VOIDP(val)) cmp=(!(VOIDP(current)));
    /* This used to return 0 when val is VOID and the value is
       EMPTY, but that's not consistent with the other table
       test functions. */
    else if (FD_EQ(val,current)) cmp=1;
    else if ((CHOICEP(val)) || (PRECHOICEP(val)) ||
             (CHOICEP(current)) || (PRECHOICEP(current)))
      cmp=fd_overlapp(val,current);
    else if (FD_EQUAL(val,current))
      cmp=1;
    else cmp=0;
    if (unlock) fd_unlock_table(ht);
    return cmp;}
  else {
    if (unlock) fd_unlock_table(ht);
    return 0;}
}

FD_EXPORT int fd_hashtable_probe_novoid(struct FD_HASHTABLE *ht,lispval key)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (FD_EMPTYP(key))
    return FD_EMPTY;
  else if (ht->table_n_keys == 0)
    return 0;
  else if (ht->table_uselock) {
    fd_read_lock_table(ht);
    unlock=1;}
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    if (unlock) fd_unlock_table(ht);
    return 0;}
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if ((result) && (!(VOIDP(result->kv_val)))) {
    if (unlock) fd_unlock_table(ht);
    return 1;}
  else {
    if (unlock) fd_unlock_table(ht);
    return 0;}
}

static struct FD_HASH_BUCKET **get_buckets(struct FD_HASHTABLE *ht,int n)
{
  struct FD_HASH_BUCKET **buckets = NULL;
  size_t buckets_size = n*sizeof(fd_hash_bucket);
  int use_bigalloc = ( buckets_size > fd_hash_bigthresh );
  if (use_bigalloc) {
    ht->ht_big_buckets=1;
    buckets = u8_big_alloc(buckets_size);}
  else {
    ht->ht_big_buckets=0;
    buckets = u8_malloc(buckets_size);}
  memset(buckets,0,buckets_size);
  return buckets;
}

static void setup_hashtable(struct FD_HASHTABLE *ptr,int n_buckets)
{
  if (n_buckets < 0) n_buckets=fd_get_hashtable_size(-n_buckets);
  ptr->ht_n_buckets=n_buckets;
  ptr->table_n_keys=0;
  ptr->table_modified=0;

  ptr->ht_buckets = get_buckets(ptr,n_buckets);
}

FD_EXPORT int fd_hashtable_store(fd_hashtable ht,lispval key,lispval value)
{
  struct FD_KEYVAL *result;
  int n_keys = 0, added = 0, unlock = 0;
  lispval newv, oldv;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if ((FD_TROUBLEP(value)))
    return fd_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (FD_TROUBLEP(key))
    return fd_interr(key);
  else if (ht->table_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_store",NULL,key);
    return -1;}
  else if (ht->table_uselock) {
    fd_write_lock_table(ht);
    unlock = 1;}
  else {}
  if (ht->ht_n_buckets == 0)
    setup_hashtable(ht,fd_init_hash_size);
  n_keys=ht->table_n_keys;
  result=hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys));
  if ( (ht->table_n_keys) > n_keys ) added=1; else added=0;
  ht->table_modified=1;
  oldv=result->kv_val;
  if (PRECHOICEP(value))
    /* Copy prechoices */
    newv=fd_make_simple_choice(value);
  else newv=fd_incref(value);
  if (FD_ABORTP(newv)) {
    if (unlock) fd_unlock_table(ht);
    return fd_interr(newv);}
  else {
    result->kv_val=newv;
    fd_decref(oldv);}
  if (unlock) fd_unlock_table(ht);
  if (PRED_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

static int add_to_hashtable(fd_hashtable ht,lispval key,lispval value)
{
  struct FD_KEYVAL *result;
  int added=0, n_keys=ht->table_n_keys;
  if (FD_TROUBLEP(value))
    return fd_interr(value);
  else if (ht->table_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_add",NULL,key);
    return -1;}
  else if (PRED_FALSE(EMPTYP(value)))
    return 0;
  else if (PRED_FALSE(FD_TROUBLEP(value)))
    return fd_interr(value);
  else fd_incref(value);
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->ht_n_buckets == 0) setup_hashtable(ht,fd_init_hash_size);
  n_keys=ht->table_n_keys;
  result=hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,
                        &(ht->table_n_keys));
  if (ht->table_n_keys>n_keys) added=1;
  ht->table_modified=1;
  if (VOIDP(result->kv_val))
    result->kv_val=value;
  else {CHOICE_ADD(result->kv_val,value);}
  /* If the value is an prechoice, it doesn't need to be locked
     because it will be protected by the hashtable's lock.  However
     this requires that we always normalize the choice inside of the
     lock (and before we return it).  */
  if ( (result) && (PRECHOICEP(result->kv_val)) ) {
    struct FD_PRECHOICE *ch=FD_XPRECHOICE(result->kv_val);
    if (ch->prechoice_uselock) ch->prechoice_uselock=0;}
  return added;
}

static int check_hashtable_size(fd_hashtable ht,ssize_t delta)
{
  size_t n_keys=ht->table_n_keys, n_buckets=ht->ht_n_buckets;
  size_t need_keys=(delta<0) ?
    (n_keys+fd_init_hash_size+3) :
    (n_keys+delta+fd_init_hash_size);
  if (n_buckets==0) {
    setup_hashtable(ht,fd_get_hashtable_size(need_keys));
    return ht->ht_n_buckets;}
  else if (need_keys>n_buckets) {
    size_t new_size=fd_get_hashtable_size(need_keys*2);
    resize_hashtable(ht,new_size,0);
    return ht->ht_n_buckets;}
  else return 0;
}

FD_EXPORT int fd_hashtable_add(fd_hashtable ht,lispval key,lispval value)
{
  int added=0, unlock = 0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (EMPTYP(key))
    return 0;
  else if (ht->table_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_add",NULL,key);
    return -1;}
  else if (PRED_FALSE(EMPTYP(value)))
    return 0;
  else if ((FD_TROUBLEP(value)))
    return fd_interr(value);
  else if ((FD_TROUBLEP(key)))
    return fd_interr(key);
  else if (ht->table_uselock) {
    fd_write_lock_table(ht);
    unlock=1;}
  else {}
  if (!(CONSP(key)))
    check_hashtable_size(ht,3);
  else if (CHOICEP(key))
    check_hashtable_size(ht,FD_CHOICE_SIZE(key));
  else if (PRECHOICEP(key))
    check_hashtable_size(ht,FD_PRECHOICE_SIZE(key));
  else check_hashtable_size(ht,3);
  /* These calls unlock the hashtable */
  if ( (CHOICEP(key)) || (PRECHOICEP(key)) ) {
    DO_CHOICES(eachkey,key) {
      added+=add_to_hashtable(ht,key,value);}}
  else added=add_to_hashtable(ht,key,value);
  if (unlock) fd_unlock_table(ht);
  return added;
}

FD_EXPORT int fd_hashtable_drop
(struct FD_HASHTABLE *ht,lispval key,lispval value)
{
  int unlock = 0;
  struct FD_HASH_BUCKET **bucket_loc;
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (EMPTYP(key))
    return 0;
  else if (FD_TROUBLEP(key))
    return fd_interr(key);
  else if (FD_TROUBLEP(value))
    return fd_interr(value);
  else if (ht->table_n_keys == 0)
    return 0;
  else if (ht->table_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_drop",NULL,key);
    return -1;}
  else if (ht->table_uselock) {
    fd_write_lock_table(ht);
    unlock=1;}
  else NO_ELSE;
  result=hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,&bucket_loc);
  if (result) {
    lispval cur = result->kv_val, new = FD_VOID;
    int remove_key = (EMPTYP(cur)) || (VOIDP(value)), changed=1;
    if (remove_key)
      new = FD_EMPTY;
    else {
      new = fd_difference(cur,value);
      if (EMPTYP(new)) remove_key=1;
      else changed = ( new != cur );}
    if (new != cur) fd_decref(cur);
    if (remove_key) {
      lispval key = result->kv_key;
      struct FD_HASH_BUCKET *bucket = *bucket_loc;
      *bucket_loc = hash_bucket_remove(bucket,result);
      ht->table_n_keys--;
      fd_decref(key);}
    else result->kv_val = new;
    if (changed) ht->table_modified=1;
    fd_unlock_table(ht);
    return 1;}
  if (unlock) fd_unlock_table(ht);
  return 0;
}

static lispval restore_hashtable(lispval tag,lispval alist,
                                 fd_compound_typeinfo e)
{
  lispval *keys, *vals; int n=0; struct FD_HASHTABLE *new;
  if (PAIRP(alist)) {
    int max=64;
    keys=u8_alloc_n(max,lispval);
    vals=u8_alloc_n(max,lispval);
    {FD_DOLIST(elt,alist) {
      if (n>=max) {
        keys=u8_realloc_n(keys,max*2,lispval);
        vals=u8_realloc_n(vals,max*2,lispval);
        max=max*2;}
      keys[n]=FD_CAR(elt); vals[n]=FD_CDR(elt); n++;}}}
  else return fd_err(fd_DTypeError,"restore_hashtable",NULL,alist);
  new=(struct FD_HASHTABLE *)fd_make_hashtable(NULL,n*2);
  fd_hashtable_iter(new,fd_table_add,n,keys,vals);
  u8_free(keys); u8_free(vals);
  new->table_modified=0;
  return LISP_CONS(new);
}

/* Evaluting hashtables */

FD_EXPORT void fd_hash_quality
  (unsigned int *hashv,int n_keys,int n_buckets,
   unsigned int *buf,unsigned int bufsiz,
   unsigned int *nbucketsp,unsigned int *maxbucketp,
   unsigned int *ncollisionsp)
{
  int i, mallocd=0;
  int n_collisions=0, max_bucket=0;
  if ((buf) && (n_buckets<bufsiz))
    memset(buf,0,sizeof(unsigned int)*n_buckets);
  else {
    buf=u8_alloc_n(n_buckets,unsigned int); mallocd=1;
    memset(buf,0,sizeof(unsigned int)*n_buckets);}
  i=0; while (i<n_keys)
    if (hashv[i]) {
      int offset=compute_offset(hashv[i],n_buckets);
      buf[offset]++;
      i++;}
    else i++;
  i=0; while (i<n_buckets)
    if (buf[i]) {
      n_buckets++;
      if (buf[i]>1) n_collisions++;
      if (buf[i]>max_bucket) max_bucket=buf[i];
      i++;}
    else i++;
  if (mallocd) u8_free(buf);
  if (nbucketsp) *nbucketsp=n_buckets;
  if (maxbucketp) *maxbucketp=max_bucket;
  if (ncollisionsp) *ncollisionsp=n_collisions;
}

/* A general operations function for hashtables */

#define TESTOP(x) ( ((x) == fd_table_test) || ((x) == fd_table_haskey) )

static int do_hashtable_op(struct FD_HASHTABLE *ht,fd_tableop op,
                           lispval key,lispval value)
{
  struct FD_KEYVAL *result;
  struct FD_HASH_BUCKET **bucket_loc=NULL;
  int added=0;
  if (EMPTYP(key))
    return 0;
  else if (FD_TROUBLEP(key))
    return fd_interr(key);
  else if (FD_TROUBLEP(value))
    return fd_interr(value);
  else NO_ELSE;
  if ( (ht->table_readonly) && ( ! (TESTOP(op) ) ) ) {
    fd_seterr2(fd_ReadOnlyHashtable,"do_hashtable_op");
    return -1;}
  switch (op) {
  case fd_table_replace: case fd_table_replace_novoid: case fd_table_drop:
  case fd_table_add_if_present: case fd_table_test: case fd_table_haskey:
  case fd_table_increment_if_present: case fd_table_multiply_if_present:
  case fd_table_maximize_if_present: case fd_table_minimize_if_present: {
    /* These are operations which can be resolved immediately if the key
       does not exist in the table.  It doesn't bother setting up the hashtable
       if it doesn't have to. */
    if (ht->table_n_keys == 0) return 0;
    result=hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,&bucket_loc);
    if (result == NULL)
      return 0;
    else if ( op == fd_table_haskey )
      return 1;
    else { added=1; break; }}

  case fd_table_add: case fd_table_add_noref:
    if ((EMPTYP(value)) &&
        ((op == fd_table_add) ||
         (op == fd_table_add_noref) ||
         (op == fd_table_add_if_present) ||
         (op == fd_table_test) ||
         (op == fd_table_drop)))
      return 0;

  default:
    if (ht->ht_n_buckets == 0) setup_hashtable(ht,fd_init_hash_size);
    result=hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,
                          &(ht->table_n_keys));
  } /* switch (op) */

  if ((result==NULL) &&
      ((op == fd_table_drop)                ||
       (op == fd_table_test)                ||
       (op == fd_table_replace)             ||
       (op == fd_table_add_if_present)      ||
       (op == fd_table_multiply_if_present) ||
       (op == fd_table_maximize_if_present) ||
       (op == fd_table_minimize_if_present) ||
       (op == fd_table_increment_if_present)))
    return 0;
  switch (op) {
  case fd_table_replace_novoid:
    if (VOIDP(result->kv_val)) return 0;
  case fd_table_store: case fd_table_replace: {
    lispval newv=fd_incref(value);
    fd_decref(result->kv_val); result->kv_val=newv;
    break;}
  case fd_table_store_noref:
    fd_decref(result->kv_val);
    result->kv_val=value;
    break;
  case fd_table_add_if_present:
    if (VOIDP(result->kv_val)) break;
  case fd_table_add: case fd_table_add_empty:
    fd_incref(value);
    if (VOIDP(result->kv_val)) result->kv_val=value;
    else {CHOICE_ADD(result->kv_val,value);}
    break;
  case fd_table_add_noref: case fd_table_add_empty_noref:
    if (VOIDP(result->kv_val))
      result->kv_val=value;
    else {CHOICE_ADD(result->kv_val,value);}
    break;
  case fd_table_drop: {
    int drop_key = 0;
    lispval cur = result->kv_val, new;
    if (FD_VOIDP(value)) {
      drop_key = 1; new=EMPTY;}
    else if (FD_EMPTYP(cur)) {
        drop_key = 1; new=EMPTY;}
    else {
      new=fd_difference(cur,value);
      if (EMPTYP(new)) drop_key=1;}
    if (new != cur) fd_decref(cur);
    if (drop_key) {
      lispval key = result->kv_key;
      struct FD_HASH_BUCKET *bucket = *bucket_loc;
      *bucket_loc = hash_bucket_remove(bucket,result);
      fd_decref(key);
      ht->table_n_keys--;
      result = NULL;}
    else result->kv_val = new;
    break;}
  case fd_table_test:
    if ((CHOICEP(result->kv_val)) ||
        (PRECHOICEP(result->kv_val)) ||
        (CHOICEP(value)) ||
        (PRECHOICEP(value)))
      return fd_overlapp(value,result->kv_val);
    else if (LISP_EQUAL(value,result->kv_val))
      return 1;
    else return 0;
  case fd_table_default:
    if ((EMPTYP(result->kv_val)) ||
        (VOIDP(result->kv_val))) {
      result->kv_val = fd_incref(value);}
    break;
  case fd_table_increment_if_present:
    if (VOIDP(result->kv_val)) break;
  case fd_table_increment:
    if ((EMPTYP(result->kv_val)) ||
        (VOIDP(result->kv_val))) {
      result->kv_val=fd_incref(value);}
    else if (!(NUMBERP(result->kv_val))) {
      fd_seterr(fd_TypeError,"fd_table_increment","number",result->kv_val);
      return -1;}
    else {
      lispval current=result->kv_val;
      DO_CHOICES(v,value)
        if ((FIXNUMP(current)) && (FIXNUMP(v))) {
          long long cval=FIX2INT(current);
          long long delta=FIX2INT(v);
          result->kv_val=FD_INT(cval+delta);}
        else if ((FD_FLONUMP(current)) &&
                 (FD_CONS_REFCOUNT(((fd_cons)current))<2) &&
                 ((FIXNUMP(v)) || (FD_FLONUMP(v)))) {
          struct FD_FLONUM *dbl=(fd_flonum)current;
          if (FIXNUMP(v))
            dbl->floval=dbl->floval+FIX2INT(v);
          else dbl->floval=dbl->floval+FD_FLONUM(v);}
        else if (NUMBERP(v)) {
          lispval newnum=fd_plus(current,v);
          if (newnum != current) {
            fd_decref(current);
            result->kv_val=newnum;}}
        else {
          fd_seterr(fd_TypeError,"fd_table_increment","number",v);
          return -1;}}
    break;
  case fd_table_multiply_if_present:
    if (VOIDP(result->kv_val)) break;
  case fd_table_multiply:
    if ((VOIDP(result->kv_val))||(EMPTYP(result->kv_val)))  {
      result->kv_val=fd_incref(value);}
    else if (!(NUMBERP(result->kv_val))) {
      fd_seterr(fd_TypeError,"fd_table_multiply","number",result->kv_val);
      return -1;}
    else {
      lispval current=result->kv_val;
      DO_CHOICES(v,value)
        if ((FD_INTP(current)) && (FD_INTP(v))) {
          long long cval=FIX2INT(current);
          long long factor=FIX2INT(v);
          result->kv_val=FD_INT(cval*factor);}
        else if ((FD_FLONUMP(current)) &&
                 (FD_CONS_REFCOUNT(((fd_cons)current))<2) &&
                 ((FIXNUMP(v)) || (FD_FLONUMP(v)))) {
          struct FD_FLONUM *dbl=(fd_flonum)current;
          if (FIXNUMP(v))
            dbl->floval=dbl->floval*FIX2INT(v);
          else dbl->floval=dbl->floval*FD_FLONUM(v);}
        else if (NUMBERP(v)) {
          lispval newnum=fd_multiply(current,v);
          if (newnum != current) {
            fd_decref(current);
            result->kv_val=newnum;}}
        else {
          fd_seterr(fd_TypeError,"table_multiply_op","number",v);
          return -1;}}
    break;
  case fd_table_maximize_if_present:
    if (VOIDP(result->kv_val)) break;
  case fd_table_maximize:
    if ((EMPTYP(result->kv_val)) ||
        (VOIDP(result->kv_val))) {
      result->kv_val=fd_incref(value);}
    else if (!(NUMBERP(result->kv_val))) {
      fd_seterr(fd_TypeError,"table_maximize_op","number",result->kv_val);
      return -1;}
    else {
      lispval current=result->kv_val;
      if ((NUMBERP(current)) && (NUMBERP(value))) {
        if (fd_numcompare(value,current)>0) {
          result->kv_val=fd_incref(value);
          fd_decref(current);}}
      else {
        fd_seterr(fd_TypeError,"table_maximize_op","number",value);
        return -1;}}
    break;
  case fd_table_minimize_if_present:
    if (VOIDP(result->kv_val)) break;
  case fd_table_minimize:
    if ((EMPTYP(result->kv_val))||(VOIDP(result->kv_val))) {
      result->kv_val=fd_incref(value);}
    else if (!(NUMBERP(result->kv_val))) {
      fd_seterr(fd_TypeError,"table_maximize_op","number",result->kv_val);
      return -1;}
    else {
      lispval current=result->kv_val;
      if ((NUMBERP(current)) && (NUMBERP(value))) {
        if (fd_numcompare(value,current)<0) {
          result->kv_val=fd_incref(value);
          fd_decref(current);}}
      else {
        fd_seterr(fd_TypeError,"table_maximize_op","number",value);
        return -1;}}
    break;
  case fd_table_push:
    if ((VOIDP(result->kv_val)) || (EMPTYP(result->kv_val))) {
      result->kv_val=fd_make_pair(value,NIL);}
    else if (PAIRP(result->kv_val)) {
      result->kv_val=fd_conspair(fd_incref(value),result->kv_val);}
    else {
      lispval tail=fd_conspair(result->kv_val,NIL);
      result->kv_val=fd_conspair(fd_incref(value),tail);}
    break;
  default: {
    u8_byte buf[64];
    added=-1;
    fd_seterr3(BadHashtableMethod,"do_hashtable_op",
               u8_sprintf(buf,64,"0x%x",op));
    break;}
  }
  ht->table_modified=1;
  if ( (result) && (PRECHOICEP(result->kv_val)) ) {
    struct FD_PRECHOICE *ch=FD_XPRECHOICE(result->kv_val);
    if (ch->prechoice_uselock) {
      u8_destroy_mutex(&(ch->prechoice_lock));
      ch->prechoice_uselock=0;}}
  return added;
}

FD_EXPORT int fd_hashtable_op
   (struct FD_HASHTABLE *ht,fd_tableop op,lispval key,lispval value)
{
  int added=0, unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (EMPTYP(key))
    return 0;
  else if (FD_TROUBLEP(key))
    return fd_interr(key);
  else if (FD_TROUBLEP(value))
    return fd_interr(value);
  else if (ht->table_uselock) {
    fd_write_lock_table(ht);
    unlock=1;}
  else NO_ELSE;
  added=do_hashtable_op(ht,op,key,value);
  if (unlock) fd_unlock_table(ht);
  if (PRED_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_op_nolock
   (struct FD_HASHTABLE *ht,fd_tableop op,lispval key,lispval value)
{
  int added;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (EMPTYP(key))
    return 0;
  else if (FD_TROUBLEP(key))
    return fd_interr(key);
  else if (FD_TROUBLEP(value))
    return fd_interr(value);
  else NO_ELSE;
  added=do_hashtable_op(ht,op,key,value);
  if (PRED_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    resize_hashtable(ht,new_size,0);}
  return added;
}

FD_EXPORT int fd_hashtable_iter
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    const lispval *keys,const lispval *values)
{
  int i=0, added=0, unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_uselock) {
    fd_write_lock_table(ht);
    unlock=1;}
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0) {
      added=do_hashtable_op(ht,op,keys[i],values[i]);
      if ( (added) && (TESTOP(op)) ) break;}
    else do_hashtable_op(ht,op,keys[i],values[i]);
    i++;}
  if (unlock) fd_unlock_table(ht);
  if ( (added) && (!(TESTOP(op))) &&
       (PRED_FALSE(hashtable_needs_resizep(ht)))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_iter_kv
(struct FD_HASHTABLE *ht,fd_tableop op,
 struct FD_CONST_KEYVAL *kvals,int n,
 int lock)
{
  int i=0, added=0, unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if ( (lock) && (ht->table_uselock) ) {
    fd_write_lock_table(ht);
    unlock=1;}
  while (i < n) {
    lispval key = kvals[i].kv_key;
    lispval val = kvals[i].kv_val;
    if (added==0) {
      added=do_hashtable_op(ht,op,key,val);
      if ( (added) && (TESTOP(op)) ) break;}
    else do_hashtable_op(ht,op,key,val);
    i++;}
  if (unlock) fd_unlock_table(ht);
  if ( (added) && (!(TESTOP(op))) &&
       (PRED_FALSE(hashtable_needs_resizep(ht)))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    resize_hashtable(ht,new_size,lock);}
  return added;
}

FD_EXPORT int fd_hashtable_iterkeys
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    const lispval *keys,lispval value)
{
  int i=0, added=0; int unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_uselock) {
    fd_write_lock_table(ht);
    unlock=1;}
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0) {
      added=do_hashtable_op(ht,op,keys[i],value);
      if ( (added) && (TESTOP(op)) )
        break;}
    else do_hashtable_op(ht,op,keys[i],value);
    if (added<0) break;
    i++;}
  if (unlock) fd_unlock_table(ht);
  if ( (added>0) && (!(TESTOP(op))) &&
       (PRED_FALSE(hashtable_needs_resizep(ht)))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_itervals
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    lispval key,const lispval *values)
{
  int i=0, added=0; int unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_uselock) {
    fd_write_lock_table(ht);
    unlock=1;}
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0) {
      added=do_hashtable_op(ht,op,key,values[i]);
      if ( (added) && ( (op == fd_table_haskey) || (op == fd_table_test) ) )
        break;}
    else do_hashtable_op(ht,op,key,values[i]);
    i++;}
  if (unlock) fd_unlock_table(ht);
  if ( (added) && (!(TESTOP(op))) &&
       (PRED_FALSE(hashtable_needs_resizep(ht)))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

static int hashtable_getsize(struct FD_HASHTABLE *ptr)
{
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  return ptr->table_n_keys;
}

static int hashtable_modified(struct FD_HASHTABLE *ptr,int flag)
{
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  int modified=ptr->table_modified;
  if (flag<0)
    return modified;
  else if (flag) {
    ptr->table_modified=1;
    return modified;}
  else {
    ptr->table_modified=0;
    return modified;}
}

static int hashtable_readonly(struct FD_HASHTABLE *ptr,int flag)
{
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  int readonly=ptr->table_readonly;
  if (flag<0)
    return readonly;
  else if (flag) {
    ptr->table_readonly=1;
    return readonly;}
  else {
    ptr->table_readonly=0;
    return readonly;}
}

FD_EXPORT lispval fd_hashtable_keys(struct FD_HASHTABLE *ptr)
{
  lispval result=EMPTY; int unlock=0;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  if (ptr->table_uselock) {u8_read_lock(&ptr->table_rwlock); unlock=1;}
  {
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          if (VOIDP(kvscan->kv_val)) {kvscan++;continue;}
          fd_incref(kvscan->kv_key);
          CHOICE_ADD(result,kvscan->kv_key);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  return fd_simplify_choice(result);
}

FD_EXPORT lispval fd_hashtable_values(struct FD_HASHTABLE *ptr)
{
  int unlock=0;
  struct FD_PRECHOICE *prechoice; lispval results;
  int size = ptr->table_n_keys;
  if (size == 0) return FD_EMPTY;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  if (ptr->table_uselock) {u8_read_lock(&ptr->table_rwlock); unlock=1;}
  size=ptr->table_n_keys;
  if (size == 0) {
    /* Race condition */
    if (unlock) u8_rw_unlock(&ptr->table_rwlock);
    return FD_EMPTY;}
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_prechoice(NULL,17*(size),0);
  prechoice=FD_XPRECHOICE(results);
  {
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets;
    struct FD_HASH_BUCKET **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          lispval value=kvscan->kv_val;
          if ((VOIDP(value))||(EMPTYP(value))) {
            kvscan++; continue;}
          fd_incref(value);
          _prechoice_add(prechoice,value);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  return fd_simplify_choice(results);
}

FD_EXPORT lispval fd_hashtable_assocs(struct FD_HASHTABLE *ptr)
{
  int unlock=0;
  struct FD_PRECHOICE *prechoice; lispval results;
  int size;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  if (ptr->table_uselock) {
    u8_read_lock(&ptr->table_rwlock);
    unlock=1;}
  size=ptr->table_n_keys;
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_prechoice(NULL,17*(size),0);
  prechoice=FD_XPRECHOICE(results);
  {
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets;
    struct FD_HASH_BUCKET **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          lispval key=kvscan->kv_key, value=kvscan->kv_val, assoc;
          if (VOIDP(value)) {
            kvscan++; continue;}
          fd_incref(key); fd_incref(value);
          assoc=fd_init_pair(NULL,key,value);
          _prechoice_add(prechoice,assoc);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  return fd_simplify_choice(results);
}

static int free_buckets(struct FD_HASH_BUCKET **buckets,int len,int big)
{
  if ((buckets) && (len)) {
    struct FD_HASH_BUCKET **scan=buckets, **lim=scan+len;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e = *scan;
        int n_entries=e->bucket_len;
        struct FD_CONST_KEYVAL *kvscan =
          ((struct FD_CONST_KEYVAL *) &(e->kv_val0));
        struct FD_CONST_KEYVAL *kvlimit = kvscan + n_entries;
        while (kvscan<kvlimit) {
          fd_decref(kvscan->kv_key);
          fd_decref(kvscan->kv_val);
          kvscan++;}
        *scan++=NULL;
        u8_free(e);}
      else scan++;}
  if (buckets) {
    if (big)
      u8_big_free(buckets);
    else u8_free(buckets);}
  return 0;
}

FD_EXPORT int fd_free_buckets(struct FD_HASH_BUCKET **buckets,
                              int buckets_to_free,
                              int isbig)
{
  return free_buckets(buckets,buckets_to_free,isbig);
}

FD_EXPORT int fd_reset_hashtable
(struct FD_HASHTABLE *ht,int n_buckets,int lock)
{
  struct FD_HASH_BUCKET **buckets;
  int buckets_to_free=0, unlock = 0, big=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (n_buckets<0)
    n_buckets=ht->ht_n_buckets;
  if ((lock) && (ht->table_uselock)) {
    fd_write_lock_table(ht);
    unlock=1;}
  /* Grab the buckets and their length. We'll free them after we've reset
     the table and released its lock. */
  big=ht->ht_big_buckets;
  buckets=ht->ht_buckets;
  buckets_to_free=ht->ht_n_buckets;
  /* Now initialize the structure.  */
  if (n_buckets == 0) {
    ht->ht_n_buckets=ht->table_n_keys=0;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets=NULL;}
  else {
    struct FD_HASH_BUCKET **bucketvec;
    ht->table_n_keys=0;
    ht->ht_n_buckets=n_buckets;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets = bucketvec = get_buckets(ht,n_buckets);}
  /* Free the lock, letting other processes use this hashtable. */
  if (unlock) fd_unlock_table(ht);
  /* Now, free the old data... */
  free_buckets(buckets,buckets_to_free,big);
  return n_buckets;
}

/* This resets a hashtable and passes out the internal data to be
   freed separately.  The idea is to hold onto the hashtable's lock
   for as little time as possible. */
FD_EXPORT int fd_fast_reset_hashtable
  (struct FD_HASHTABLE *ht,int n_buckets,int lock,
   struct FD_HASH_BUCKET ***bucketsptr,
   int *buckets_to_free,int *bigbuckets)
{
  int unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (bucketsptr==NULL)
    return fd_reset_hashtable(ht,n_buckets,lock);
  if (n_buckets<0) n_buckets=ht->ht_n_buckets;
  if ((lock) && (ht->table_uselock)) {
    fd_write_lock_table(ht);
    unlock=1;}
  /* Grab the buckets and their length. We'll free them after we've reset
     the table and released its lock. */
  *bucketsptr=ht->ht_buckets;
  *bigbuckets=ht->ht_big_buckets;
  *buckets_to_free=ht->ht_n_buckets;
  /* Now initialize the structure.  */
  if (n_buckets == 0) {
    ht->ht_n_buckets=ht->table_n_keys=0;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets=NULL;}
  else {
    ht->table_n_keys=0;
    ht->ht_n_buckets=n_buckets;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets = get_buckets(ht,n_buckets);}
  /* Free the lock, letting other processes use this hashtable. */
  if (unlock) fd_unlock_table(ht);
  return n_buckets;
}


FD_EXPORT int fd_swap_hashtable(struct FD_HASHTABLE *src,
                                struct FD_HASHTABLE *dest,
                                int n_keys,int locked)
{
#define COPYFIELD(field) dest->field=src->field

  if (n_keys<0) n_keys=src->table_n_keys;

  int n_buckets=fd_get_hashtable_size(n_keys), unlock=0;

  FD_CHECK_TYPE_RET(src,fd_hashtable_type);

  if (!(locked)) {
    if (src->table_uselock) {
      u8_write_lock(&(src->table_rwlock));
      unlock=1;}}

  memset(dest,0,FD_HASHTABLE_LEN);

  FD_SET_CONS_TYPE(dest,fd_hashtable_type);

  COPYFIELD(table_n_keys);
  COPYFIELD(table_load_factor);
  COPYFIELD(ht_n_buckets);
  COPYFIELD(ht_buckets);
  COPYFIELD(ht_big_buckets);

  dest->table_uselock=1;
  u8_init_rwlock(&(dest->table_rwlock));

  /* Now, reset the source table */

  src->ht_n_buckets=n_buckets;
  src->ht_buckets = get_buckets(dest,n_buckets);
  src->table_n_keys=0;
  src->table_modified=0;
  src->table_readonly=0;

  if (unlock) u8_rw_unlock(&(src->table_rwlock));

#undef COPYFIELD
  return 1;
}

FD_EXPORT int fd_static_hashtable(struct FD_HASHTABLE *ptr,int type)
{
  int n_conversions=0, unlock=0;
  fd_ptr_type keeptype=(fd_ptr_type) type;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if (ptr->table_uselock) {
    u8_write_lock(&ptr->table_rwlock);
    unlock=1;}
  {
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets;
    struct FD_HASH_BUCKET **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan;
        int n_keyvals=e->bucket_len;
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+n_keyvals;
        while (kvscan<kvlimit) {
          if ((CONSP(kvscan->kv_val)) &&
              ((type<0) || (TYPEP(kvscan->kv_val,keeptype)))) {
            lispval value=kvscan->kv_val;
            if (!(FD_STATICP(value))) {
              lispval static_value=fd_static_copy(value);
              if (static_value==value) {
                fd_decref(static_value);
                static_value=fd_register_fcnid(kvscan->kv_val);}
              else {
                kvscan->kv_val=static_value;
                fd_decref(value);
                n_conversions++;}}}
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  return n_conversions;
}

FD_EXPORT lispval fd_make_hashtable(struct FD_HASHTABLE *ptr,int n_buckets)
{
  if (ptr == NULL) {
    ptr=u8_alloc(struct FD_HASHTABLE);
    FD_INIT_FRESH_CONS(ptr,fd_hashtable_type);
    u8_init_rwlock(&(ptr->table_rwlock));}

  if (n_buckets == 0) {

    ptr->ht_n_buckets=ptr->table_n_keys=0;
    ptr->table_readonly=ptr->table_modified=0;
    ptr->table_uselock=1;
    ptr->table_load_factor=default_hashtable_loading;
    ptr->ht_buckets=NULL;
    return LISP_CONS(ptr);}
  else {
    if (n_buckets < 0) n_buckets=-n_buckets;
    else n_buckets=fd_get_hashtable_size(n_buckets);

    ptr->table_readonly=ptr->table_modified=0;
    ptr->table_n_keys=0;
    ptr->table_uselock=1;
    ptr->ht_n_buckets=n_buckets;
    ptr->table_load_factor=default_hashtable_loading;
    ptr->ht_buckets=get_buckets(ptr,n_buckets);

    return LISP_CONS(ptr);}
}

/* Note that this does not incref the values passed to it. */
FD_EXPORT lispval fd_init_hashtable(struct FD_HASHTABLE *ptr,int init_keys,
                                    struct FD_KEYVAL *inits)
{
  int n_buckets=fd_get_hashtable_size(init_keys);
  struct FD_HASH_BUCKET **buckets;

  if (ptr == NULL) {
    ptr=u8_alloc(struct FD_HASHTABLE);
    FD_INIT_FRESH_CONS(ptr,fd_hashtable_type);}
  else {FD_SET_CONS_TYPE(ptr,fd_hashtable_type);}

  ptr->table_n_keys=0;
  ptr->ht_n_buckets=n_buckets;
  ptr->ht_buckets=buckets=get_buckets(ptr,n_buckets);

  ptr->table_load_factor=default_hashtable_loading;
  ptr->table_modified=0;
  ptr->table_readonly=0;
  ptr->table_uselock=1;

  if (inits) {
    int i=0; while (i<init_keys) {
      struct FD_KEYVAL *ki=&(inits[i]);
      struct FD_KEYVAL *hv=hashvec_insert
        (ki->kv_key,buckets,n_buckets,&(ptr->table_n_keys));
      fd_incref(hv->kv_val);
      i++;}}

  ptr->table_uselock=1;
  u8_init_rwlock(&(ptr->table_rwlock));

  return LISP_CONS(ptr);
}

/* Note that this does not incref the values passed to it. */
FD_EXPORT lispval fd_initialize_hashtable(struct FD_HASHTABLE *ptr,
                                          struct FD_KEYVAL *inits,
                                          int init_keys)
{
  int n_buckets=fd_get_hashtable_size(init_keys);
  struct FD_HASH_BUCKET **buckets;

  if (ptr == NULL) {
    ptr=u8_alloc(struct FD_HASHTABLE);
    FD_INIT_FRESH_CONS(ptr,fd_hashtable_type);}
  else {FD_SET_CONS_TYPE(ptr,fd_hashtable_type);}

  ptr->table_n_keys=0;
  ptr->ht_n_buckets=n_buckets;
  ptr->ht_buckets=buckets=get_buckets(ptr,n_buckets);

  ptr->table_load_factor=default_hashtable_loading;
  ptr->table_modified=0;
  ptr->table_readonly=0;
  ptr->table_uselock=1;

  if (inits) {
    int i=0; while (i<init_keys) {
      struct FD_KEYVAL *ki=&(inits[i]);
      struct FD_KEYVAL *hv=
        hashvec_insert(ki->kv_key,buckets,n_buckets,&(ptr->table_n_keys));
      hv->kv_val = ki->kv_val;
      fd_decref(ki->kv_key);
      ki->kv_val=ki->kv_key=VOID;
      i++;}}

  ptr->table_uselock=1;
  u8_init_rwlock(&(ptr->table_rwlock));

  return LISP_CONS(ptr);
}

static int resize_hashtable(struct FD_HASHTABLE *ptr,int n_buckets,
                            int need_lock)
{
  int unlock=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if ( (need_lock) && (ptr->table_uselock) ) {
    fd_write_lock_table(ptr);
    unlock=1; }
  int big_buckets = ptr->ht_big_buckets;

  struct FD_HASH_BUCKET **new_buckets=get_buckets(ptr,n_buckets);
  struct FD_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
  struct FD_HASH_BUCKET **nscan=new_buckets, **nlim=nscan+n_buckets;
  while (nscan<nlim) *nscan++=NULL;
  while (scan < lim)
    if (*scan) {
      struct FD_HASH_BUCKET *e=*scan++;
      int bucket_len=e->bucket_len;
      struct FD_KEYVAL *kvscan=&(e->kv_val0);
      struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
      while (kvscan<kvlimit) {
        struct FD_KEYVAL *nkv =
          hashvec_insert(kvscan->kv_key,new_buckets,n_buckets,NULL);
        nkv->kv_val=kvscan->kv_val; kvscan->kv_val=VOID;
        fd_decref(kvscan->kv_key); kvscan++;}
      u8_free(e);}
    else scan++;

  if (big_buckets)
    u8_big_free(ptr->ht_buckets);
  else u8_free(ptr->ht_buckets);

  ptr->ht_n_buckets=n_buckets;
  ptr->ht_buckets=new_buckets;

  if (unlock) fd_unlock_table(ptr);

  return n_buckets;
}

FD_EXPORT int fd_resize_hashtable(struct FD_HASHTABLE *ptr,int n_buckets)
{
  return resize_hashtable(ptr,n_buckets,1);
}


/* VOIDs all values which only have one incoming pointer (from the table
   itself).  This is helpful in conjunction with fd_devoid_hashtable, which
   will then reduce the table to remove such entries. */
FD_EXPORT int fd_remove_deadwood(struct FD_HASHTABLE *ptr,
                                 int (*testfn)(lispval,lispval,void *),
                                 void *testdata)
{
  struct FD_HASH_BUCKET **scan, **lim;
  int n_buckets=ptr->ht_n_buckets, n_keys=ptr->table_n_keys; int unlock=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if ((n_buckets == 0) || (n_keys == 0)) return 0;
  if (ptr->table_uselock) {
    fd_write_lock_table(ptr);
    unlock=1;}
  while (1) {
    int n_cleared=0;
    scan=ptr->ht_buckets; lim=scan+ptr->ht_n_buckets;
    while (scan < lim) {
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan++;
        int bucket_len=e->bucket_len;
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
        if ((testfn)&&(testfn(kvscan->kv_key,VOID,testdata))) {}
        else while (kvscan<kvlimit) {
            lispval val=kvscan->kv_val;
            if (CONSP(val)) {
              struct FD_CONS *cval=(struct FD_CONS *)val;
              if (PRECHOICEP(val))
                cval=(struct FD_CONS *)
                  (val=kvscan->kv_val=fd_simplify_choice(val));
              if (FD_CONS_REFCOUNT(cval)==1) {
                kvscan->kv_val=VOID;
                fd_decref(val);
                n_cleared++;}}
            /* ??? In the future, this should probably scan CHOICES
               to remove deadwood as well.  */
            kvscan++;}}
      else scan++;}
    if (n_cleared)
      n_cleared=0;
    else break;}
  if (unlock) fd_unlock_table(ptr);
  return n_buckets;
}

FD_EXPORT int fd_devoid_hashtable(struct FD_HASHTABLE *ptr,int locked)
{
  int n_keys=ptr->table_n_keys;
  int n_buckets=ptr->ht_n_buckets;
  int unlock=0;

  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if ((n_buckets == 0) || (n_keys == 0)) return 0;
  if ((locked<0)?(ptr->table_uselock):(!(locked))) {
    fd_write_lock_table(ptr);
    /* Avoid race condition */
    n_keys=ptr->table_n_keys;
    n_buckets=ptr->ht_n_buckets,
      unlock=1;}

  struct FD_HASH_BUCKET **new_buckets=get_buckets(ptr,n_buckets);
  struct FD_HASH_BUCKET **scan=ptr->ht_buckets;
  struct FD_HASH_BUCKET **lim=scan+ptr->ht_n_buckets;
  int remaining_keys=0, big_buckets = ptr->ht_big_buckets;

  if (new_buckets==NULL) {
    if (unlock) fd_unlock_table(ptr);
    return -1;}
  while (scan < lim)
    if (*scan) {
      struct FD_HASH_BUCKET *e=*scan++;
      int bucket_len=e->bucket_len;
      struct FD_KEYVAL *kvscan=&(e->kv_val0);
      struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
      while (kvscan<kvlimit)
        if (VOIDP(kvscan->kv_val)) {
          fd_decref(kvscan->kv_key);
          kvscan++;}
        else {
          struct FD_KEYVAL *nkv =
            hashvec_insert(kvscan->kv_key,new_buckets,n_buckets,NULL);
          nkv->kv_val=kvscan->kv_val;
          kvscan->kv_val=VOID;
          fd_decref(kvscan->kv_key);
          remaining_keys++;
          kvscan++;}
      u8_free(e);}
    else scan++;

  if (big_buckets)
    u8_big_free(ptr->ht_buckets);
  else u8_free(ptr->ht_buckets);

  ptr->ht_n_buckets=n_buckets;
  ptr->ht_buckets=new_buckets;
  ptr->table_n_keys=remaining_keys;

  if (unlock) fd_unlock_table(ptr);
  return n_buckets;
}

FD_EXPORT int fd_hashtable_stats
  (struct FD_HASHTABLE *ptr,
   int *n_bucketsp,int *n_keysp,int *n_filledp,int *n_collisionsp,
   int *max_bucketp,int *n_valsp,int *max_valsp)
{
  int n_buckets=ptr->ht_n_buckets, n_keys=0, unlock=0;
  int n_filled=0, max_bucket=0, n_collisions=0;
  int n_vals=0, max_vals=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if (ptr->table_uselock) { fd_read_lock_table(ptr); unlock=1;}

  struct FD_HASH_BUCKET **scan=ptr->ht_buckets;
  struct FD_HASH_BUCKET **lim=scan+ptr->ht_n_buckets;
  while (scan < lim)
    if (*scan) {
      struct FD_HASH_BUCKET *e=*scan;
      int bucket_len=e->bucket_len;
      n_filled++;
      n_keys=n_keys+bucket_len;
      if (bucket_len>max_bucket)
        max_bucket=bucket_len;
      if (bucket_len>1)
        n_collisions++;
      if ((n_valsp) || (max_valsp)) {
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          int valcount;
          lispval val=kvscan->kv_val;
          if (CHOICEP(val))
            valcount=FD_CHOICE_SIZE(val);
          else if (PRECHOICEP(val))
            valcount=FD_PRECHOICE_SIZE(val);
          else valcount=1;
          n_vals=n_vals+valcount;
          if (valcount>max_vals)
            max_vals=valcount;
          kvscan++;}}
      scan++;}
    else scan++;

  if (n_filledp) *n_filledp=n_filled;
  if (n_keysp) *n_keysp=n_keys;
  if (n_bucketsp) *n_bucketsp=n_buckets;
  if (n_collisionsp) *n_collisionsp=n_collisions;
  if (max_bucketp) *max_bucketp=max_bucket;
  if (n_valsp) *n_valsp=n_vals;
  if (max_valsp) *max_valsp=max_vals;
  if (unlock) fd_unlock_table(ptr);
  return n_keys;
}

static void copy_keyval(struct FD_KEYVAL *dest,struct FD_KEYVAL *src)
{
  lispval key = src->kv_key, val = src->kv_val;
  if (FD_CONSP(key)) {
    if (FD_STATIC_CONSP(key))
      key = fd_deep_copy(key);
    else fd_incref(key);}
  if (FD_CONSP(val)) {
    if (FD_PRECHOICEP(val))
      val = fd_make_simple_choice(val);
    else if (FD_STATIC_CONSP(val))
      val = fd_deep_copy(val);
    else fd_incref(val);}
  dest->kv_key = key;
  dest->kv_val = val;
}

FD_EXPORT struct FD_HASHTABLE *
fd_copy_hashtable(FD_HASHTABLE *dest_arg,
                  FD_HASHTABLE *src,
                  int locksrc)
{
  struct FD_HASHTABLE *dest;
  int n_keys=0, n_buckets=0, unlock=0, copied_keys=0;
  struct FD_HASH_BUCKET **buckets, **read, **write, **read_limit;
  if (dest_arg==NULL)
    dest=u8_alloc(struct FD_HASHTABLE);
  else dest=dest_arg;
  if ( (locksrc) && (src->table_uselock) ) {
    fd_read_lock_table(src);
    unlock=1;}
  if (dest_arg) {
    FD_INIT_STATIC_CONS(dest,fd_hashtable_type);}
  else {FD_INIT_CONS(dest,fd_hashtable_type);}
  dest->table_modified=0;
  dest->table_readonly=0;
  dest->table_uselock=1;
  dest->ht_n_buckets=n_buckets=src->ht_n_buckets;
  dest->table_n_keys=n_keys=src->table_n_keys;
  read=buckets=src->ht_buckets;
  read_limit=read+n_buckets;
  dest->table_load_factor=src->table_load_factor;

  write=dest->ht_buckets=get_buckets(dest,n_buckets);

  while ( (read<read_limit) && (copied_keys < n_keys) )  {
    if (*read==NULL) {read++; write++;}
    else {
      struct FD_KEYVAL *kvread, *kvwrite, *kvlimit;
      struct FD_HASH_BUCKET *he=*read++, *newhe;
      int n=he->bucket_len;
      *write++=newhe=(struct FD_HASH_BUCKET *)
        u8_malloc(sizeof(struct FD_HASH_BUCKET)+
                  (n-1)*FD_KEYVAL_LEN);
      kvread  = &(he->kv_val0);
      kvwrite = &(newhe->kv_val0);
      newhe->bucket_len=n; kvlimit=kvread+n;
      while (kvread<kvlimit) {
        copy_keyval(kvwrite,kvread);
        kvread++;
        kvwrite++;
        copied_keys++;}}}

  if (unlock) fd_unlock_table(src);

  u8_init_rwlock(&(dest->table_rwlock));

  return dest;
}

static lispval copy_hashtable(lispval table,int deep)
{
  return (lispval) fd_copy_hashtable
    (NULL,fd_consptr(fd_hashtable,table,fd_hashtable_type),1);
}

static int unparse_hashtable(u8_output out,lispval x)
{
  struct FD_HASHTABLE *ht=FD_XHASHTABLE(x);
  u8_printf(out,"#<HASHTABLE %d/%d #!0x%llx>",
            ht->table_n_keys,ht->ht_n_buckets,
            (FD_LONGVAL( x)));
  return 1;
}

/* This makes a hashtable readonly and disables it's mutex.  The
   advantage of this is that it avoids lock contention but, of course,
   the table is read only.
 */

FD_EXPORT int fd_hashtable_set_readonly(FD_HASHTABLE *ht,int readonly)
{
  if (readonly) {
    if ((ht->table_readonly) && (ht->table_uselock==0))
      return 0;
    else if ((!(ht->table_readonly)) && (ht->table_uselock==0)) {
      fd_seterr2("Can't make non-locking hashtable readonly",
                "fd_hashtable_set_readonly");
      return -1;}
    fd_read_lock_table(ht);
    ht->table_readonly=1;
    ht->table_uselock=0;
    return 1;}
  else {
    if (ht->table_uselock) return 0;
    ht->table_uselock=1;
    ht->table_readonly=0;
    fd_unlock_table(ht);
    return 1;}
}

FD_EXPORT int fd_recycle_hashtable(struct FD_HASHTABLE *c)
{
  struct FD_HASHTABLE *ht=(struct FD_HASHTABLE *)c; int unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_uselock) {
    fd_write_lock_table(ht);
    unlock=1;}
  if (ht->ht_n_buckets==0) {
    if (unlock) fd_unlock_table(ht);
    u8_destroy_rwlock(&(ht->table_rwlock));
    return 0;}
  if (ht->ht_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->ht_buckets;
    struct FD_HASH_BUCKET **lim=scan+ht->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          fd_decref(kvscan->kv_key);
          fd_decref(kvscan->kv_val);
          kvscan++;}
        u8_free(*scan);
        *scan++=NULL;}
      else scan++;
    if (ht->ht_big_buckets)
      u8_big_free(ht->ht_buckets);
    else u8_free(ht->ht_buckets);}
  ht->ht_buckets=NULL;
  ht->ht_n_buckets=0;
  ht->table_n_keys=0;
  if (unlock) fd_unlock_table(ht);
  u8_destroy_rwlock(&(ht->table_rwlock));
  memset(ht,0,FD_HASHTABLE_LEN);
  return 0;
}

static void recycle_hashtable(struct FD_RAW_CONS *c)
{
  fd_recycle_hashtable((struct FD_HASHTABLE *)c);
  u8_free(c);
}

FD_EXPORT struct FD_KEYVAL *fd_hashtable_keyvals
   (struct FD_HASHTABLE *ht,int *sizep,int lock)
{
  struct FD_KEYVAL *results, *rscan; int unlock=0;
  if ((FD_CONS_TYPE(ht)) != fd_hashtable_type) {
    fd_seterr(fd_TypeError,"hashtable",NULL,(lispval)ht);
    return NULL;}
  if (ht->table_n_keys == 0) {
    *sizep=0;
    return NULL;}
  if ( (lock) && (ht->table_uselock) ) {
      fd_read_lock_table(ht);
      unlock=1;}
  if (ht->ht_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->ht_buckets;
    struct FD_HASH_BUCKET **lim=scan+ht->ht_n_buckets;
    rscan=results=u8_alloc_n(ht->table_n_keys,struct FD_KEYVAL);
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          copy_keyval(rscan,kvscan);
          kvscan++;
          rscan++;}
        scan++;}
      else scan++;
    *sizep=ht->table_n_keys;}
  else {*sizep=0; results=NULL;}
  if (unlock) fd_unlock_table(ht);
  return results;
}

FD_EXPORT int fd_for_hashtable
  (struct FD_HASHTABLE *ht,kv_valfn f,void *data,int lock)
{
  int unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return 0;
  if ((lock)&&(ht->table_uselock)) {fd_read_lock_table(ht); unlock=1;}
  if (ht->ht_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->ht_buckets;
    struct FD_HASH_BUCKET **lim=scan+ht->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan;
        int n_entries=e->bucket_len;
        const struct FD_KEYVAL *kvscan=&(e->kv_val0);
        const struct FD_KEYVAL *kvlimit=kvscan+n_entries;
        while (kvscan<kvlimit) {
          if (f(kvscan->kv_key,kvscan->kv_val,data)) {
            if (lock) fd_unlock_table(ht);
            return ht->ht_n_buckets;}
          else kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) fd_unlock_table(ht);
  return ht->ht_n_buckets;
}

FD_EXPORT int fd_for_hashtable_kv
  (struct FD_HASHTABLE *ht,fd_kvfn f,void *data,int lock)
{
  int unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return 0;
  if ((lock)&&(ht->table_uselock)) {
    fd_write_lock_table(ht);
    unlock=1;}
  if (ht->ht_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->ht_buckets, **lim=scan+ht->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan;
        int n_keyvals=e->bucket_len;
        struct FD_KEYVAL *kvscan=&(e->kv_val0);
        struct FD_KEYVAL *kvlimit=kvscan+n_keyvals;
        while (kvscan<kvlimit) {
          if (f(kvscan,data)) {
            if (unlock) fd_unlock_table(ht);
            return ht->ht_n_buckets;}
          else kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) fd_unlock_table(ht);
  return ht->ht_n_buckets;
}

/* Hashsets */

FD_EXPORT void fd_init_hashset(struct FD_HASHSET *hashset,int size,
                               int stack_cons)
{
  lispval *slots;
  int i=0, n_slots=fd_get_hashtable_size(size);
  if (stack_cons) {
    FD_INIT_STATIC_CONS(hashset,fd_hashset_type);}
  else {FD_INIT_CONS(hashset,fd_hashset_type);}
  hashset->hs_n_buckets=n_slots;
  hashset->hs_n_elts=0;
  hashset->hs_modified=0;
  hashset->hs_load_factor=default_hashset_loading;
  hashset->hs_buckets=slots=u8_alloc_n(n_slots,lispval);
  while (i < n_slots) slots[i++]=FD_EMPTY;
  u8_init_rwlock(&(hashset->table_rwlock));
  hashset->table_uselock = 1;
  return;
}

FD_EXPORT lispval fd_make_hashset()
{
  struct FD_HASHSET *h=u8_alloc(struct FD_HASHSET);
  fd_init_hashset(h,fd_init_hash_size,FD_MALLOCD_CONS);
  FD_INIT_CONS(h,fd_hashset_type);
  return LISP_CONS(h);
}

/* This does a simple binary search of a sorted choice vector,
   looking for a particular element. Once more, we separate out the
   atomic case because it just requires pointer comparisons.  */
static int choice_containsp(lispval x,struct FD_CHOICE *choice)
{
  int size = FD_XCHOICE_SIZE(choice);
  const lispval *bottom = FD_XCHOICE_DATA(choice), *top = bottom+(size-1);
  if (ATOMICP(x)) {
    while (top>=bottom) {
      const lispval *middle = bottom+(top-bottom)/2;
      if (x == *middle) return 1;
      else if (CONSP(*middle)) top = middle-1;
      else if (x < *middle) top = middle-1;
      else bottom = middle+1;}
    return 0;}
  else {
    while (top>=bottom) {
        const lispval *middle = bottom+(top-bottom)/2;
        int comparison = cons_compare(x,*middle);
        if (comparison == 0) return 1;
        else if (comparison<0) top = middle-1;
        else bottom = middle+1;}
      return 0;}
}

FD_EXPORT int fd_hashset_get(struct FD_HASHSET *h,lispval key)
{
  if (h->hs_n_elts==0) return 0;
  int hash = fd_hash_lisp(key), unlock=0;
  if (h->table_uselock) {
    fd_read_lock_table(h);
    unlock=1;}
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets, bucket = hash%n_slots;
  lispval contents = slots[bucket];
  int rv=0;
  if (FD_EMPTY_CHOICEP(contents)) rv=0;
  else if (contents==key) rv=1;
  else if (!(FD_AMBIGP(contents)))
    rv=FD_EQUALP(contents,key);
  else if (FD_CHOICEP(contents))
    rv=choice_containsp(key,(fd_choice)contents);
  else {
    if (unlock) {
      fd_unlock_table(h);
      fd_write_lock_table(h);}
    if (h->hs_n_buckets!=n_slots) {
      n_slots=h->hs_n_buckets;
      bucket = hash%n_slots;
      slots = h->hs_buckets;}
    contents = slots[bucket];
    if (FD_PRECHOICEP(contents))
      slots[bucket]=contents=fd_simplify_choice(contents);
    if (FD_EMPTYP(contents)) rv=0;
    else if (contents==key) rv=1;
    else if (FD_CHOICEP(contents))
      rv=choice_containsp(key,(fd_choice)contents);
    else rv=FD_EQUALP(contents,key);}
  if (unlock) fd_unlock_table(h);
  return rv;
}

static lispval hashset_probe(struct FD_HASHSET *h,lispval key)
{
  if (h->hs_n_elts==0)
    return FD_EMPTY;
  int hash = fd_hash_lisp(key), unlock=0;
  if (h->table_uselock) {
    fd_read_lock_table(h);
    unlock=1;}
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets, bucket = hash%n_slots;
  lispval contents = slots[bucket];
  lispval result=FD_EMPTY;
  if (FD_EMPTY_CHOICEP(contents)) {}
  else if (contents==key)
    result=contents;
  else if (!(FD_AMBIGP(contents))) {
    if (FD_EQUALP(contents,key))
      result=contents;}
  else {
    FD_DO_CHOICES(entry,contents) {
      if ( (entry==key) || (FD_EQUALP(entry,key)) ) {
        result=entry; FD_STOP_DO_CHOICES; break;}}}
  if (unlock) fd_unlock_table(h);
  return fd_incref(result);
}

static lispval hashset_intern(struct FD_HASHSET *h,lispval key)
{
  int hash = fd_hash_lisp(key), unlock = 0;
  if (h->table_uselock) {
    fd_read_lock_table(h);
    unlock=1;}
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets, bucket = hash%n_slots;
  lispval contents = slots[bucket];
  lispval result=FD_EMPTY;
  if (FD_EMPTY_CHOICEP(contents)) {}
  else if (contents==key)
    result=contents;
  else if (!(FD_AMBIGP(contents))) {
    if (FD_EQUALP(contents,key))
      result=contents;}
  else {
    FD_DO_CHOICES(entry,contents) {
      if ( (entry==key) || (FD_EQUALP(entry,key)) ) {
        result=entry; FD_STOP_DO_CHOICES; break;}}}
  if (FD_EMPTYP(result)) {
    fd_incref(key);
    FD_ADD_TO_CHOICE(contents,key);
    slots[bucket]=contents;
    result=key;}
  if (unlock) fd_unlock_table(h);
  return fd_incref(result);
}

FD_EXPORT lispval fd_hashset_intern(struct FD_HASHSET *h,lispval key,int add)
{
  if (add) {
    lispval existing = hashset_probe(h,key);
    if (FD_EMPTYP(existing)) {
      lispval added = hashset_intern(h,key);
      return added;}
    else return existing;}
  else return hashset_probe(h,key);
}

FD_EXPORT lispval fd_hashset_elts(struct FD_HASHSET *h,int clean)
{
  /* A clean value of -1 indicates that the hashset will be reset
     entirely (freed) if mallocd. */
  FD_CHECK_TYPE_RETDTYPE(h,fd_hashset_type);
  if (h->hs_n_elts==0) {
    if (clean<0) {
      if (FD_MALLOCD_CONSP(h))
        fd_decref((lispval)h);
      else {
        u8_free(h->hs_buckets);
        h->hs_n_buckets=h->hs_n_elts=0;}}
    return EMPTY;}
  else {
    int unlock = 0;
    if (h->table_uselock) {
      if (clean)
        fd_write_lock_table(h);
      else fd_read_lock_table(h);
      unlock=1;}
    lispval results=EMPTY;
    lispval *scan=h->hs_buckets, *limit=scan+h->hs_n_buckets;
    while (scan<limit) {
      lispval v = *scan;
      if ((EMPTYP(v)) || (VOIDP(v)) || (FD_NULLP(v))) {
        *scan++=EMPTY; continue;}
      if (FD_PRECHOICEP(v)) {
        if (clean) {
          lispval norm = fd_simplify_choice(v);
          FD_ADD_TO_CHOICE(results,norm);
          *scan=EMPTY;}
        else {
          lispval norm = fd_make_simple_choice(v);
          FD_ADD_TO_CHOICE(results,norm);}}
      else {
        FD_ADD_TO_CHOICE(results,v);
        if (clean) *scan=FD_EMPTY;
        else fd_incref(v);}
      scan++;}
    if (unlock) fd_unlock_table(h);
    if (clean<0) {
      if (FD_MALLOCD_CONSP(h)) {
        fd_decref((lispval)h);}
      else {
        u8_free(h->hs_buckets);
        h->hs_n_buckets=h->hs_n_elts=0;}}
    return fd_simplify_choice(results);}
}

FD_EXPORT int fd_reset_hashset(struct FD_HASHSET *h)
{
  FD_CHECK_TYPE_RETDTYPE(h,fd_hashset_type);
  if (h->hs_n_elts==0)
    return 0;
  else {
    int unlock=0;
    if (h->table_uselock) {
      fd_write_lock_table(h);
      unlock=1;}
    int n_elts=h->hs_n_elts;
    lispval *scan=h->hs_buckets, *limit=scan+h->hs_n_buckets;
    while (scan<limit) {
      lispval v=*scan;
      *scan=FD_EMPTY_CHOICE;
      fd_decref(v);
      scan++;}
    h->hs_n_elts=0;
    if (unlock) fd_unlock_table(h);
    return n_elts;}
}

static int hashset_getsize(struct FD_HASHSET *h)
{
  FD_CHECK_TYPE_RETDTYPE(h,fd_hashset_type);
  return h->hs_n_elts;
}

static int hashset_modified(struct FD_HASHSET *ptr,int flag)
{
  FD_CHECK_TYPE_RET(ptr,fd_hashset_type);
  int modified=ptr->hs_modified;
  if (flag<0)
    return modified;
  else if (flag) {
    ptr->hs_modified=1;
    return modified;}
  else {
    ptr->hs_modified=0;
    return modified;}
}

static lispval hashset_elts(struct FD_HASHSET *h)
{
  return fd_hashset_elts(h,0);
}

static void hashset_add_raw(struct FD_HASHSET *h,lispval key)
{
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets;
  int hash = fd_hash_lisp(key), bucket = hash%n_slots;
  lispval contents = slots[bucket];
  FD_ADD_TO_CHOICE(contents,key);
  slots[bucket] = contents;
}

static size_t grow_hashset(struct FD_HASHSET *h,ssize_t target)
{
  int i=0, lim=h->hs_n_buckets;
  size_t new_size= (target<=0) ?
    (fd_get_hashtable_size(hashset_resize_target(h))) :
    (target);
  lispval *slots=h->hs_buckets;
  lispval *newslots=u8_alloc_n(new_size,lispval);
  while (i<new_size) newslots[i++]=FD_EMPTY_CHOICE;
  h->hs_buckets=newslots; h->hs_n_buckets=new_size;
  i=0; while (i < lim) {
    lispval content=slots[i];
    if ((EMPTYP(content)) || (VOIDP(content)) || (FD_NULLP(content))) {
      slots[i++]=FD_EMPTY; continue;}
    else if (FD_AMBIGP(content)) {
      FD_DO_CHOICES(val,content) {
        hashset_add_raw(h,val);
        fd_incref(val);}
      fd_decref(content);}
    else hashset_add_raw(h,content);
    slots[i++]=FD_EMPTY;}
  u8_free(slots);
  return new_size;
}

FD_EXPORT ssize_t fd_grow_hashset(struct FD_HASHSET *h,ssize_t target)
{
  int unlock = 0;
  if (h->table_uselock) {
    fd_write_lock_table(h);
    unlock=1;}
  size_t rv=grow_hashset(h,target);
  if (unlock) fd_unlock_table(h);
  return rv;
}

static int hashset_test_add(struct FD_HASHSET *h,lispval key)
{
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets;
  int hash = fd_hash_lisp(key), bucket = hash%n_slots;
  lispval contents = slots[bucket];
  if (FD_EMPTYP(contents)) {
    fd_incref(key);
    slots[bucket] = key;
    return 1;}
  else if (contents == key)
    return 0;
  else if (FD_CHOICEP(contents)) {
    if (choice_containsp(key,(fd_choice)contents))
      return 0;
    else {
      fd_incref(key);
      FD_ADD_TO_CHOICE(slots[bucket],key);
      return 1;}}
  else if (FD_PRECHOICEP(contents)) {
    if (fd_choice_containsp(contents,key))
      return 0;
    else {
      fd_incref(key);
      FD_ADD_TO_CHOICE(slots[bucket],key);
      return 1;}}
  else if (FD_EQUALP(contents,key))
    return 0;
  else {
    fd_incref(key);
    FD_ADD_TO_CHOICE(slots[bucket],key);
    return 1;}
}

static int hashset_test_drop(struct FD_HASHSET *h,lispval key)
{
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets;
  int hash = fd_hash_lisp(key), bucket = hash%n_slots;
  lispval contents = slots[bucket];
  if (FD_EMPTYP(contents))
    return 0;
  else if (contents == key) {
    slots[bucket]=FD_EMPTY;
    fd_decref(key);
    return 1;}
  else if (FD_CHOICEP(contents)) {
    if (!(choice_containsp(key,(fd_choice)contents)))
      return 1;}
  else if (FD_PRECHOICEP(contents)) {
    if (!(fd_choice_containsp(contents,key)))
      return 0;}
  else if (FD_EQUALP(contents,key)) {
    slots[bucket]=FD_EMPTY;
    fd_decref(key);
    return 1;}
  else {}
  lispval new_contents = fd_difference(contents,key);
  slots[bucket]=new_contents;
  fd_decref(contents);
  return 1;
}

static int hashset_mod(struct FD_HASHSET *h,lispval key,int add)
{
  int rv=0;
  if (add) {
    rv = hashset_test_add(h,key);
    if (rv) {
      int n_elts=h->hs_n_elts++;
      if ( (h->hs_n_buckets) < (n_elts*h->hs_load_factor) )
        grow_hashset(h,-1);}}
  else {
    rv = hashset_test_drop(h,key);
    if (rv) h->hs_n_elts--;}
  return rv;
}

FD_EXPORT int fd_hashset_mod(struct FD_HASHSET *h,lispval key,int add)
{
  int rv=0, unlock=0;
  if (h->table_uselock) {
    fd_write_lock_table(h);
    unlock = 1;}
  rv=hashset_mod(h,key,add);
  if (unlock) fd_unlock_table(h);
  return rv;
}

/* This adds without locking or incref. */
FD_EXPORT void fd_hashset_add_raw(struct FD_HASHSET *h,lispval key)
{
  hashset_add_raw(h,key);
}

FD_EXPORT int fd_hashset_add(struct FD_HASHSET *h,lispval keys)
{
  if ((CHOICEP(keys))||(PRECHOICEP(keys))) {
    int n_vals=FD_CHOICE_SIZE(keys), unlock=0;
    size_t need_size=ceil((n_vals+h->hs_n_elts)*h->hs_load_factor), n_adds=0;
    if (need_size>h->hs_n_buckets) fd_grow_hashset(h,need_size);
    if (h->table_uselock) {
      fd_write_lock_table(h);
      unlock=1;}
    {DO_CHOICES(key,keys) {
        if (hashset_mod(h,key,1)) n_adds++;}}
    if (unlock) fd_unlock_table(h);
    return n_adds;}
  else if (EMPTYP(keys))
    return 0;
  else return fd_hashset_mod(h,keys,1);
}

FD_EXPORT int fd_recycle_hashset(struct FD_HASHSET *h)
{
  int unlock = 0;
  if (h->table_uselock) {
    fd_write_lock_table(h);
    unlock = 1;}
  lispval *scan=h->hs_buckets, *lim=scan+h->hs_n_buckets;
  while (scan<lim) {
    lispval v = *scan;
    fd_decref(v);
    *scan++=FD_EMPTY;}
  u8_free(h->hs_buckets);
  if (unlock) fd_unlock_table(h);
  u8_destroy_rwlock(&(h->table_rwlock));
  memset(h,0,sizeof(struct FD_HASHSET));
  return 1;
}

static void recycle_hashset(struct FD_RAW_CONS *c)
{
  fd_recycle_hashset((struct FD_HASHSET *)c);
  u8_free(c);
}

static lispval copy_hashset(lispval table,int deep)
{
  struct FD_HASHSET *ptr=fd_consptr(fd_hashset,table,fd_hashset_type);
  return fd_copy_hashset(NULL,ptr);
}

FD_EXPORT lispval fd_copy_hashset(struct FD_HASHSET *hnew,struct FD_HASHSET *h)
{
  int unlock = 0;
  if (h->table_uselock) {
    fd_read_lock_table(h);
    unlock = 1;}
  int n_slots = h->hs_n_buckets;
  lispval *read = h->hs_buckets, *readlim = read+n_slots;
  if (hnew==NULL) hnew=u8_alloc(struct FD_HASHSET);
  FD_INIT_CONS(hnew,fd_hashset_type);
  hnew->hs_buckets=u8_alloc_n(h->hs_n_buckets,lispval);
  lispval *write=hnew->hs_buckets;
  while (read<readlim) {
    lispval v=*read++;
    if (FD_EMPTYP(v))
      *write++=FD_EMPTY;
    else if (FD_PRECHOICEP(v))
      *write++=fd_make_simple_choice(v);
    else *write++=fd_incref(v);}
  hnew->hs_n_elts=h->hs_n_elts;
  hnew->hs_load_factor=h->hs_load_factor;
  hnew->hs_modified=0;
  if (unlock) fd_unlock_table(h);
  hnew->table_uselock=1;
  u8_init_rwlock((&hnew->table_rwlock));
  return (lispval) hnew;
}

static int unparse_hashset(u8_output out,lispval x)
{
  struct FD_HASHSET *hs=((struct FD_HASHSET *)x);
  u8_printf(out,"#<HASHSET%s %d/%d>",
            ((hs->hs_modified)?("(m)"):("")),
            hs->hs_n_elts,hs->hs_n_buckets);
  return 1;
}

static lispval hashset_get(lispval x,lispval key)
{
  struct FD_HASHSET *h=fd_consptr(struct FD_HASHSET *,x,fd_hashset_type);
  if (fd_hashset_get(h,key))
    return FD_TRUE;
  else return FD_FALSE;
}
static int hashset_store(lispval x,lispval key,lispval val)
{
  struct FD_HASHSET *h=fd_consptr(struct FD_HASHSET *,x,fd_hashset_type);
  if (FD_TRUEP(val))
    return fd_hashset_mod(h,key,1);
  else if (FALSEP(val))
    return fd_hashset_mod(h,key,0);
  else {
    fd_seterr(fd_RangeError,_("value is not a boolean"),NULL,val);
    return -1;}
}

/* Generic table functions */

struct FD_TABLEFNS *fd_tablefns[FD_TYPE_MAX];

#define CHECKPTR(arg,cxt)             \
  if (PRED_FALSE((!(FD_CHECK_PTR(arg))))) \
    _fd_bad_pointer(arg,cxt); else {}

static int bad_table_call(lispval arg,fd_ptr_type type,void *handler,
                          u8_context cxt)
{
  if (handler)
    return 0;
  else if (PRED_FALSE(fd_tablefns[type]==NULL)) {
    fd_seterr(NotATable,cxt,NULL,arg);
    return 1;}
  else {
    fd_seterr(fd_NoMethod,cxt,NULL,arg);
    return 1;}
}

#define BAD_TABLEP(arg,type,meth,cxt) \
  (bad_table_call                                                       \
   (arg,type,                                                           \
    ((fd_tablefns[type]) ? ((fd_tablefns[type])->meth) : (NULL)),       \
    cxt))
#define NOT_TABLEP(arg,type,cxt) \
  ( (fd_tablefns[type] == NULL) && (bad_table_call(arg,type,NULL,cxt)) )

FD_EXPORT lispval fd_get(lispval arg,lispval key,lispval dflt)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_get/table");
  CHECKPTR(key,"fd_get/key");
  CHECKPTR(key,"fd_get/dflt");
  if ((EMPTYP(arg))||(EMPTYP(key)))
    return EMPTY;
  else if (FD_UNAMBIGP(key)) {
    if (BAD_TABLEP(arg,argtype,get,"fd_get"))
      return FD_ERROR;
    else return (fd_tablefns[argtype]->get)(arg,key,dflt);}
  else if (BAD_TABLEP(arg,argtype,get,"fd_get"))
    return FD_ERROR;
  else {
    lispval results=EMPTY;
    lispval (*getfn)(lispval,lispval,lispval)=fd_tablefns[argtype]->get;
    DO_CHOICES(each,key) {
      lispval values=getfn(arg,each,EMPTY);
      if (FD_ABORTP(values)) {
        fd_decref(results); return values;}
      CHOICE_ADD(results,values);}
    if (EMPTYP(results)) return fd_incref(dflt);
    else return results;}
}

FD_EXPORT int fd_store(lispval arg,lispval key,lispval value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_store/table");
  CHECKPTR(key,"fd_store/key");
  CHECKPTR(value,"fd_store/value");
  if ((EMPTYP(arg))||(EMPTYP(key)))
    return 0;
  else if (FD_UNAMBIGP(key)) {
    if (BAD_TABLEP(arg,argtype,store,"fd_store"))
      return -1;
    else return (fd_tablefns[argtype]->store)(arg,key,value);}
  else if (BAD_TABLEP(arg,argtype,store,"fd_store"))
    return -1;
  else {
    int (*storefn)(lispval,lispval,lispval)=fd_tablefns[argtype]->store;
    DO_CHOICES(each,key) {
      int retval=storefn(arg,each,value);
      if (retval<0) return retval;}
    return 1;}
}

FD_EXPORT int fd_add(lispval arg,lispval key,lispval value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_add/table");
  CHECKPTR(key,"fd_add/key");
  CHECKPTR(value,"fd_add/value");
  if (PRED_FALSE((EMPTYP(arg))||(EMPTYP(key))))
    return 0;
  else if (FD_UNAMBIGP(key)) {
    if (NOT_TABLEP(arg,argtype,"fd_add"))
      return -1;
    else if (fd_tablefns[argtype]->add)
      return (fd_tablefns[argtype]->add)(arg,key,value);
    else if ((fd_tablefns[argtype]->store) &&
             (fd_tablefns[argtype]->get)) {
      int (*storefn)(lispval,lispval,lispval)=fd_tablefns[argtype]->store;
      lispval (*getfn)(lispval,lispval,lispval)=fd_tablefns[argtype]->get;
      DO_CHOICES(each,key) {
        int store_rv=0;
        lispval values=getfn(arg,each,EMPTY);
        lispval to_store;
        if (FD_ABORTP(values)) {
          FD_STOP_DO_CHOICES;
          return -1;}
        fd_incref(value);
        CHOICE_ADD(values,value);
        to_store=fd_make_simple_choice(values);
        store_rv=storefn(arg,each,to_store);
        fd_decref(values);
        fd_decref(to_store);
        if (store_rv<0) {
          FD_STOP_DO_CHOICES;
          return -1;}}
      return 1;}
    else return fd_err(fd_NoMethod,"fd_add",NULL,arg);}
  else if (BAD_TABLEP(arg,argtype,add,"fd_add"))
    return -1;
  else  {
    int (*addfn)(lispval,lispval,lispval)=fd_tablefns[argtype]->add;
    DO_CHOICES(each,key) {
      int retval=addfn(arg,each,value);
      if (retval<0) return retval;}
    return 1;}
}

FD_EXPORT int fd_drop(lispval arg,lispval key,lispval value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_drop/table");
  CHECKPTR(key,"fd_drop/key");
  CHECKPTR(value,"fd_drop/value");
  if ( (EMPTYP(arg)) || (EMPTYP(key)) )
    return 0;
  if (FD_VALID_TYPECODEP(argtype))
    if (PRED_TRUE(fd_tablefns[argtype]!=NULL))
      if (PRED_TRUE(fd_tablefns[argtype]->drop!=NULL))
        if (PRED_FALSE((EMPTYP(value)) ||
                            (EMPTYP(key))))
          return 0;
        else if (CHOICEP(key)) {
          int (*dropfn)(lispval,lispval,lispval)=fd_tablefns[argtype]->drop;
          DO_CHOICES(each,key) {
            int retval=dropfn(arg,each,value);
            if (retval<0) return retval;}
          return 1;}
        else return (fd_tablefns[argtype]->drop)(arg,key,value);
      else if ((fd_tablefns[argtype]->store) &&
               (fd_tablefns[argtype]->get))
        if (PRED_FALSE((EMPTYP(value))||(EMPTYP(key))))
          return 0;
        else if (VOIDP(value)) {
          int retval;
          int (*storefn)(lispval,lispval,lispval)=fd_tablefns[argtype]->store;
          DO_CHOICES(each,key) {
            retval=storefn(arg,each,EMPTY);
            if (retval<0) return retval;}
          return 1;}
        else {
          int (*storefn)(lispval,lispval,lispval)=fd_tablefns[argtype]->store;
          lispval (*getfn)(lispval,lispval,lispval)=fd_tablefns[argtype]->get;
          DO_CHOICES(each,key) {
            lispval values=getfn(arg,each,EMPTY);
            lispval nvalues;
            int retval;
            if (FD_ABORTP(values)) return values;
            else nvalues=fd_difference(values,value);
            retval=storefn(arg,each,nvalues);
            fd_decref(values); fd_decref(nvalues);
            if (retval<0) return retval;}
          return 1;}
      else return fd_reterr(fd_NoMethod,CantDrop,NULL,arg);
    else return fd_reterr(NotATable,"fd_drop",NULL,arg);
  else return fd_reterr(fd_BadPtr,"fd_drop",NULL,arg);
}

FD_EXPORT int fd_test(lispval arg,lispval key,lispval value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_test/table");
  CHECKPTR(key,"fd_test/key");
  CHECKPTR(value,"fd_test/value");
  if (PRED_FALSE((EMPTYP(arg))||(EMPTYP(key))))
    return 0;
  if ((EMPTYP(arg))||(EMPTYP(key)))
    return 0;
  else if (NOT_TABLEP(arg,argtype,"fd_test"))
    return -1;
  else if (PRED_TRUE(fd_tablefns[argtype]->test!=NULL))
    if (CHOICEP(key)) {
      int (*testfn)(lispval,lispval,lispval)=fd_tablefns[argtype]->test;
      DO_CHOICES(each,key)
        if (testfn(arg,each,value)) return 1;
      return 0;}
    else return (fd_tablefns[argtype]->test)(arg,key,value);
  else if (fd_tablefns[argtype]->get) {
    lispval (*getfn)(lispval,lispval,lispval)=fd_tablefns[argtype]->get;
    DO_CHOICES(each,key) {
      lispval values=getfn(arg,each,EMPTY);
      if (VOIDP(value))
        if (EMPTYP(values))
          return 0;
        else {
          fd_decref(values);
          return 1;}
      else if (EMPTYP(values)) {}
      else if (FD_EQ(value,values)) {
        fd_decref(values); return 1;}
      else if (fd_overlapp(value,values)) {
        fd_decref(values); return 1;}
      else fd_decref(values);}
    return 0;}
  else return fd_reterr(fd_NoMethod,CantTest,NULL,arg);
}

FD_EXPORT int fd_getsize(lispval arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_getsize/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->getsize)
      return (fd_tablefns[argtype]->getsize)(arg);
    else if (fd_tablefns[argtype]->keys) {
      lispval values=(fd_tablefns[argtype]->keys)(arg);
      if (FD_ABORTP(values))
        return fd_interr(values);
      else {
        int size=FD_CHOICE_SIZE(values);
        fd_decref(values);
        return size;}}
    else return fd_err(fd_NoMethod,CantGetKeys,NULL,arg);
  else return fd_err(NotATable,"fd_getkeys",NULL,arg);
}

FD_EXPORT int fd_modifiedp(lispval arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_modifiedp/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->modified)
      return (fd_tablefns[argtype]->modified)(arg,-1);
    else return fd_err(fd_NoMethod,CantCheckModified,NULL,arg);
  else return fd_err(NotATable,"fd_modifiedp",NULL,arg);
}

FD_EXPORT int fd_set_modified(lispval arg,int flag)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_set_modified/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->modified)
      return (fd_tablefns[argtype]->modified)(arg,flag);
    else return fd_err(fd_NoMethod,CantSetModified,NULL,arg);
  else return fd_err(NotATable,"fd_set_modified",NULL,arg);
}

FD_EXPORT int fd_readonlyp(lispval arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_readonlyp/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->readonly)
      return (fd_tablefns[argtype]->readonly)(arg,-1);
    else return fd_err(fd_NoMethod,CantCheckReadOnly,NULL,arg);
  else return fd_err(NotATable,"fd_readonlyp",NULL,arg);
}

FD_EXPORT int fd_set_readonly(lispval arg,int flag)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_set_readonly/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->readonly)
      return (fd_tablefns[argtype]->readonly)(arg,flag);
    else return fd_err(fd_NoMethod,CantSetReadOnly,NULL,arg);
  else return fd_err(NotATable,"fd_set_readonly",NULL,arg);
}

FD_EXPORT int fd_finishedp(lispval arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_finishedp/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->finished)
      return (fd_tablefns[argtype]->finished)(arg,-1);
    else return fd_err(fd_NoMethod,CantCheckFinished,NULL,arg);
  else return fd_err(NotATable,"fd_finishedp",NULL,arg);
}

FD_EXPORT int fd_set_finished(lispval arg,int flag)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_set_finished/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->finished)
      return (fd_tablefns[argtype]->finished)(arg,flag);
    else return fd_err(fd_NoMethod,CantSetFinished,NULL,arg);
  else return fd_err(NotATable,"fd_set_finished",NULL,arg);
}

FD_EXPORT lispval fd_getkeys(lispval arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_getkeys/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->keys)
      return (fd_tablefns[argtype]->keys)(arg);
    else return fd_err(fd_NoMethod,CantGetKeys,NULL,arg);
  else return fd_err(NotATable,"fd_getkeys",NULL,arg);
}

FD_EXPORT lispval fd_getvalues(lispval arg)
{
  CHECKPTR(arg,"fd_getvalues/table");
  /* Eventually, these might be fd_tablefns fields */
  if (TYPEP(arg,fd_hashtable_type))
    return fd_hashtable_values(FD_XHASHTABLE(arg));
  else if (TYPEP(arg,fd_slotmap_type))
    return fd_slotmap_values(FD_XSLOTMAP(arg));
  else if (CHOICEP(arg)) {
    lispval results=EMPTY;
    DO_CHOICES(table,arg) {
      CHOICE_ADD(results,fd_getvalues(table));}
    return results;}
  else if (PAIRP(arg))
    return fd_refcdr(arg);
  else if (!(TABLEP(arg)))
    return fd_err(NotATable,"fd_getvalues",NULL,arg);
  else {
    lispval results=EMPTY, keys=fd_getkeys(arg);
    DO_CHOICES(key,keys) {
      lispval values=fd_get(arg,key,VOID);
      if (!((VOIDP(values))||(EMPTYP(values)))) {
        CHOICE_ADD(results,values);}}
    fd_decref(keys);
    return results;}
}

FD_EXPORT lispval fd_getassocs(lispval arg)
{
  CHECKPTR(arg,"fd_getassocs/table");
  /* Eventually, these might be fd_tablefns fields */
  if (TYPEP(arg,fd_hashtable_type))
    return fd_hashtable_assocs(FD_XHASHTABLE(arg));
  else if (TYPEP(arg,fd_slotmap_type))
    return fd_slotmap_assocs(FD_XSLOTMAP(arg));
  else if (CHOICEP(arg)) {
    lispval results=EMPTY;
    DO_CHOICES(table,arg) {
      CHOICE_ADD(results,fd_getassocs(table));}
    return results;}
  else if (PAIRP(arg))
    return fd_incref(arg);
  else if (!(TABLEP(arg)))
    return fd_err(NotATable,"fd_getvalues",NULL,arg);
  else {
    lispval results=EMPTY, keys=fd_getkeys(arg);
    DO_CHOICES(key,keys) {
      lispval values=fd_get(arg,key,VOID);
      if (!(VOIDP(values))) {
        lispval assoc=fd_init_pair(NULL,key,values);
        fd_incref(key); fd_incref(values);
        CHOICE_ADD(results,assoc);}}
    fd_decref(keys);
    return results;}
}

/* Operations over tables */

struct FD_HASHMAX {lispval max, scope, keys;};

static int hashmaxfn(lispval key,lispval value,void *hmaxp)
{
  struct FD_HASHMAX *hashmax=hmaxp;
  if ((VOIDP(hashmax->scope)) || (fd_choice_containsp(key,hashmax->scope)))
    if (EMPTYP(value)) {}
    else if (NUMBERP(value))
      if (VOIDP(hashmax->max)) {
        hashmax->max=fd_incref(value); hashmax->keys=fd_incref(key);}
      else {
        int cmp=numcompare(value,hashmax->max);
        if (cmp>0) {
          fd_decref(hashmax->keys); fd_decref(hashmax->max);
          hashmax->keys=fd_incref(key); hashmax->max=fd_incref(value);}
        else if (cmp==0) {
          fd_incref(key);
          CHOICE_ADD(hashmax->keys,key);}}
    else {}
  else {}
  return 0;
}

FD_EXPORT
lispval fd_hashtable_max(struct FD_HASHTABLE *h,lispval scope,lispval *maxvalp)
{
  if (EMPTYP(scope)) return EMPTY;
  else {
    struct FD_HASHMAX hmax;
    hmax.keys=EMPTY; hmax.scope=scope; hmax.max=VOID;
    fd_for_hashtable(h,hashmaxfn,&hmax,1);
    if ((maxvalp) && (NUMBERP(hmax.max)))
      *maxvalp=hmax.max;
    else fd_decref(hmax.max);
    return hmax.keys;}
}

static int hashskimfn_noscope(lispval key,lispval value,void *hmaxp)
{
  struct FD_HASHMAX *hashmax=hmaxp;
  if (NUMBERP(value)) {
    int cmp=numcompare(value,hashmax->max);
    if (cmp>=0) {
      fd_incref(key);
      CHOICE_ADD(hashmax->keys,key);}}
  return 0;
}

static int hashskimfn(lispval key,lispval value,void *hmaxp)
{
  struct FD_HASHMAX *hashmax=hmaxp;
  if ((VOIDP(hashmax->scope)) || (fd_choice_containsp(key,hashmax->scope)))
    if (NUMBERP(value)) {
      int cmp=numcompare(value,hashmax->max);
      if (cmp>=0) {
        fd_incref(key);
        CHOICE_ADD(hashmax->keys,key);}}
  return 0;
}

FD_EXPORT
lispval fd_hashtable_skim(struct FD_HASHTABLE *h,lispval maxval,lispval scope)
{
  if (EMPTYP(scope))
    return EMPTY;
  else {
    struct FD_HASHMAX hmax;
    hmax.keys=EMPTY; hmax.scope=scope; hmax.max=maxval;
    if (FD_VOIDP(scope))
      fd_for_hashtable(h,hashskimfn_noscope,&hmax,1);
    else fd_for_hashtable(h,hashskimfn,&hmax,1);
    return hmax.keys;}
}

static int hashcountfn(lispval key,lispval value,void *vcountp)
{
  long long *countp = (long long *) vcountp;
  int n_values = FD_CHOICE_SIZE(value);
  *countp += n_values;
  return 0;
}

FD_EXPORT
long long fd_hashtable_map_size(struct FD_HASHTABLE *h)
{
  long long count=0;
  fd_for_hashtable(h,hashcountfn,&count,1);
  return count;
}


/* Using pairs as tables functions */

static lispval pairget(lispval pair,lispval key,lispval dflt)
{
  if (FD_EQUAL(FD_CAR(pair),key)) return fd_incref(FD_CDR(pair));
  else return EMPTY;
}
static int pairtest(lispval pair,lispval key,lispval val)
{
  if (FD_EQUAL(FD_CAR(pair),key))
    if (VOIDP(val))
      if (EMPTYP(FD_CDR(pair))) return 0;
      else return 1;
    else if (FD_EQUAL(FD_CDR(pair),val)) return 1;
    else return 0;
  else return 0;
}

static int pairgetsize(lispval pair)
{
  return 1;
}

static lispval pairkeys(lispval pair)
{
  return fd_incref(FD_CAR(pair));
}

/* Describing tables */

FD_EXPORT void fd_display_table(u8_output out,lispval table,lispval keysarg)
{
  U8_OUTPUT *tmp=u8_open_output_string(1024);
  lispval keys=
    ((VOIDP(keysarg)) ? (fd_getkeys(table)) : (fd_incref(keysarg)));
  DO_CHOICES(key,keys) {
    lispval values=fd_get(table,key,EMPTY);
    tmp->u8_write=tmp->u8_outbuf; *(tmp->u8_outbuf)='\0';
    u8_printf(tmp,"   %q:   %q\n",key,values);
    if (u8_strlen(tmp->u8_outbuf)<80) u8_puts(out,tmp->u8_outbuf);
    else {
      u8_printf(out,"   %q:\n",key);
      {DO_CHOICES(value,values) u8_printf(out,"      %q\n",value);}}
    fd_decref(values);}
  fd_decref(keys);
  u8_close((u8_stream)tmp);
}

/* Table max functions */

FD_EXPORT lispval fd_table_max(lispval table,lispval scope,lispval *maxvalp)
{
  if (EMPTYP(scope)) return EMPTY;
  else if (HASHTABLEP(table))
    return fd_hashtable_max((fd_hashtable)table,scope,maxvalp);
  else if (SLOTMAPP(table))
    return fd_slotmap_max((fd_slotmap)table,scope,maxvalp);
  else {
    lispval keys=fd_getkeys(table);
    lispval maxval=VOID, results=EMPTY;
    {DO_CHOICES(key,keys)
       if ((VOIDP(scope)) || (fd_overlapp(key,scope))) {
         lispval val=fd_get(table,key,VOID);
         if ((EMPTYP(val)) || (VOIDP(val))) {}
         else if (NUMBERP(val)) {
           if (VOIDP(maxval)) {
             maxval=fd_incref(val); results=fd_incref(key);}
           else {
             int cmp=numcompare(val,maxval);
             if (cmp>0) {
               fd_decref(results); results=fd_incref(key);
               fd_decref(maxval); maxval=fd_incref(val);}
             else if (cmp==0) {
               fd_incref(key);
               CHOICE_ADD(results,key);}}}
         else {}
         fd_decref(val);}}
    fd_decref(keys);
    if ((maxvalp) && (NUMBERP(maxval))) *maxvalp=maxval;
    else fd_decref(maxval);
    return results;}
}

FD_EXPORT lispval fd_table_skim(lispval table,lispval maxval,lispval scope)
{
  if (EMPTYP(scope))
    return EMPTY;
  else if (EMPTYP(maxval)) {
    /* It's not clear what the right behavior is here. */
    /* It could also all of the keys or the subset of them in *scope*.*/
    return EMPTY;}
  /*
  else if (EMPTYP(maxval)) {
    lispval keys = fd_getkeys(table);
    if (FD_VOIDP(scope))
      return keys;
    else if (EMPTYP(keys))
      return EMPTY;
    else {
      lispval args[2] = { keys, scope };
      lispval scoped = fd_intersection(args,2);
      fd_decref(keys);
      return scoped;}}
  */
  else if (!(FD_NUMBERP(maxval)))
    return fd_type_error("number","fd_table_skim/maxval",maxval);
  else if (HASHTABLEP(table))
    return fd_hashtable_skim((fd_hashtable)table,maxval,scope);
  else if (SLOTMAPP(table))
    return fd_slotmap_skim((fd_slotmap)table,maxval,scope);
  else {
    lispval keys=fd_getkeys(table);
    lispval results=EMPTY;
    {DO_CHOICES(key,keys)
       if ((VOIDP(scope)) || (fd_overlapp(key,scope))) {
         lispval val=fd_get(table,key,VOID);
         if (NUMBERP(val)) {
           int cmp=fd_numcompare(val,maxval);
           if (cmp>=0) {
             fd_incref(key);
             CHOICE_ADD(results,key);}
           fd_decref(val);}
         else fd_decref(val);}}
    fd_decref(keys);
    return results;}
}

/* Initializations */

void fd_init_tables_c()
{
  int i=0; while (i<FD_TYPE_MAX) fd_tablefns[i++]=NULL;

  u8_register_source_file(_FILEINFO);

  /* SLOTMAP */
  fd_recyclers[fd_slotmap_type]   = recycle_slotmap;
  fd_unparsers[fd_slotmap_type]   = unparse_slotmap;
  fd_copiers[fd_slotmap_type]     = copy_slotmap;
  fd_comparators[fd_slotmap_type] =compare_slotmaps;

  /* SCHEMAP */
  fd_recyclers[fd_schemap_type]   = recycle_schemap;
  fd_unparsers[fd_schemap_type]   = unparse_schemap;
  fd_copiers[fd_schemap_type]     = copy_schemap;
  fd_comparators[fd_schemap_type] = compare_schemaps;

  /* HASHTABLE */
  fd_recyclers[fd_hashtable_type] = recycle_hashtable;
  fd_unparsers[fd_hashtable_type] = unparse_hashtable;
  fd_copiers[fd_hashtable_type]   = copy_hashtable;

  /* HASHSET */
  fd_recyclers[fd_hashset_type]   = recycle_hashset;
  fd_unparsers[fd_hashset_type]   = unparse_hashset;
  fd_copiers[fd_hashset_type]     = copy_hashset;

  memset(fd_tablefns,0,sizeof(fd_tablefns));

  /* HASHTABLE table functions */
  fd_tablefns[fd_hashtable_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_hashtable_type]->get=(fd_table_get_fn)fd_hashtable_get;
  fd_tablefns[fd_hashtable_type]->add=(fd_table_add_fn)fd_hashtable_add;
  fd_tablefns[fd_hashtable_type]->drop=(fd_table_drop_fn)fd_hashtable_drop;
  fd_tablefns[fd_hashtable_type]->store=(fd_table_store_fn)fd_hashtable_store;
  fd_tablefns[fd_hashtable_type]->test=(fd_table_test_fn)hashtable_test;
  fd_tablefns[fd_hashtable_type]->getsize=
    (fd_table_getsize_fn)hashtable_getsize;
  fd_tablefns[fd_hashtable_type]->keys=(fd_table_keys_fn)fd_hashtable_keys;
  fd_tablefns[fd_hashtable_type]->modified=
    (fd_table_modified_fn)hashtable_modified;
  fd_tablefns[fd_hashtable_type]->readonly=
    (fd_table_readonly_fn)hashtable_readonly;

  /* SLOTMAP table functions */
  fd_tablefns[fd_slotmap_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_slotmap_type]->get=(fd_table_get_fn)fd_slotmap_get;
  fd_tablefns[fd_slotmap_type]->add=(fd_table_add_fn)fd_slotmap_add;
  fd_tablefns[fd_slotmap_type]->drop=(fd_table_drop_fn)fd_slotmap_drop;
  fd_tablefns[fd_slotmap_type]->store=(fd_table_store_fn)fd_slotmap_store;
  fd_tablefns[fd_slotmap_type]->test=(fd_table_test_fn)fd_slotmap_test;
  fd_tablefns[fd_slotmap_type]->getsize=(fd_table_getsize_fn)slotmap_getsize;
  fd_tablefns[fd_slotmap_type]->keys=(fd_table_keys_fn)fd_slotmap_keys;
  fd_tablefns[fd_slotmap_type]->modified=
    (fd_table_modified_fn)slotmap_modified;
  fd_tablefns[fd_slotmap_type]->readonly=
    (fd_table_readonly_fn)slotmap_readonly;

  /* SCHEMAP table functions */
  fd_tablefns[fd_schemap_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_schemap_type]->get=(fd_table_get_fn)fd_schemap_get;
  fd_tablefns[fd_schemap_type]->add=(fd_table_add_fn)fd_schemap_add;
  fd_tablefns[fd_schemap_type]->drop=(fd_table_drop_fn)fd_schemap_drop;
  fd_tablefns[fd_schemap_type]->store=(fd_table_store_fn)fd_schemap_store;
  fd_tablefns[fd_schemap_type]->test=(fd_table_test_fn)fd_schemap_test;
  fd_tablefns[fd_schemap_type]->getsize=(fd_table_getsize_fn)schemap_getsize;
  fd_tablefns[fd_schemap_type]->keys=(fd_table_keys_fn)fd_schemap_keys;
  fd_tablefns[fd_schemap_type]->modified=
    (fd_table_modified_fn)schemap_modified;
  fd_tablefns[fd_schemap_type]->readonly=
    (fd_table_readonly_fn)schemap_readonly;

  /* HASHSET table functions */
  fd_tablefns[fd_hashset_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_hashset_type]->get=(fd_table_get_fn)hashset_get;
  fd_tablefns[fd_hashset_type]->add=(fd_table_add_fn)hashset_store;
  /* This is a no-op because you can't drop a value from a hashset.
     That would just set its value to false. */
  fd_tablefns[fd_hashset_type]->drop=(fd_table_drop_fn)NULL;
  fd_tablefns[fd_hashset_type]->store=(fd_table_store_fn)hashset_store;
  /* This is a no-op because every key has a T/F value in the hashet. */
  fd_tablefns[fd_hashset_type]->test=NULL;
  fd_tablefns[fd_hashset_type]->getsize=(fd_table_getsize_fn)hashset_getsize;
  fd_tablefns[fd_hashset_type]->keys=(fd_table_keys_fn)hashset_elts;
  fd_tablefns[fd_hashset_type]->modified=
    (fd_table_modified_fn)hashset_modified;

  /* HASHSET table functions */
  fd_tablefns[fd_pair_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_pair_type]->get=pairget;
  fd_tablefns[fd_pair_type]->test=pairtest;
  fd_tablefns[fd_pair_type]->keys=pairkeys;
  fd_tablefns[fd_pair_type]->getsize=(fd_table_getsize_fn)pairgetsize;

  /* Table functions for
       OIDS
       CHOICES
      are foud in xtable.c */
  {
    struct FD_COMPOUND_TYPEINFO *e=
      fd_register_compound(fd_intern("HASHTABLE"),NULL,NULL);
    e->compound_restorefn=restore_hashtable;
  }
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
