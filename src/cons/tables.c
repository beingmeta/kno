/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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
#include "framerd/hash.h"
#include "framerd/tables.h"
#include "framerd/numbers.h"

#include <libu8/u8printf.h>

/* For sprintf */
#include <stdio.h>

fd_exception fd_NoSuchKey=_("No such key");
fd_exception fd_ReadOnlyHashtable=_("Read-Only hashtable");
static fd_exception HashsetOverflow=_("Hashset Overflow");
static u8_string NotATable=_("Not a table");
static u8_string CantDrop=_("Table doesn't support drop");
static u8_string CantTest=_("Table doesn't support test");
static u8_string CantGetKeys=_("Table doesn't support getkeys");
static u8_string CantCheckModified=_("Can't check for modification status");
static u8_string CantSetModified=_("Can't set modficiation status");
static u8_string BadHashtableMethod=_("Invalid hashtable method");

#define DEBUGGING 0

static int resize_hashtable(struct FD_HASHTABLE *ptr,int n_slots,int need_lock);

#if DEBUGGING
#include <stdio.h>
#endif

#define flip_word(x) \
  (((x>>24)&0xff) | ((x>>8)&0xff00) | ((x&0xff00)<<8) | ((x&0xff)<<24))
#define compute_offset(hash,size) (hash%size)
/* #define compute_offset(hash,size) hash_multr(hash,2654435769U,size) */

static int numcompare(fdtype x,fdtype y)
{
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(y)))
    if ((FD_FIX2INT(x))>(FD_FIX2INT(y))) return 1;
    else if ((FD_FIX2INT(x))<(FD_FIX2INT(y))) return -1;
    else return 0;
  else if ((FD_FLONUMP(x)) && (FD_FLONUMP(y)))
    if ((FD_FLONUM(x))>(FD_FLONUM(y))) return 1;
    else if ((FD_FLONUM(x))<(FD_FLONUM(y))) return -1;
    else return 0;
  else return fd_numcompare(x,y);
}

short fd_init_smap_size = FD_INIT_SMAP_SIZE;
int   fd_init_hash_size = FD_INIT_HASH_SIZE;


/* Debugging tools */

#if DEBUGGING
static fdtype look_for_key=FD_VOID;

static void note_key(fdtype key,struct FD_HASHTABLE *h)
{
  fprintf(stderr,_("Noticed %s on %lx\n"),fd_dtype2string(key),h);
}

#define KEY_CHECK(key,ht) \
  if (FDTYPE_EQUAL(key,look_for_key)) note_key(key,ht)
#else
#define KEY_CHECK(key,ht)
#endif

/* Keyvecs */

FD_EXPORT
struct FD_KEYVAL *_fd_keyvec_get
   (fdtype fd_key,struct FD_KEYVAL *keyvals,int size)
{
  const struct FD_KEYVAL *scan=keyvals, *limit=scan+size;
  if (FD_ATOMICP(fd_key))
    while (scan<limit)
      if (scan->kv_key==fd_key)
	return (struct FD_KEYVAL *)scan;
      else scan++;
  else while (scan<limit)
	 if (FDTYPE_EQUAL(scan->kv_key,fd_key))
	   return (struct FD_KEYVAL *) scan;
	 else scan++;
  return NULL;
}

static struct FD_KEYVAL *fd_keyvec_insert
 (fdtype key,struct FD_KEYVAL **keyvalp,int *sizep,int *spacep,
  int freedata)
{
  int size=*sizep;
  int space= (spacep) ? (*spacep) : (0);
  struct FD_KEYVAL *keyvals=*keyvalp;
  const struct FD_KEYVAL *scan=keyvals, *limit=scan+size;
  if (keyvals) {
    if (FD_ATOMICP(key))
      while (scan<limit)
        if (scan->kv_key==key)
          return (struct FD_KEYVAL *)scan;
        else scan++;
    else while (scan<limit)
           if (FDTYPE_EQUAL(scan->kv_key,key))
             return (struct FD_KEYVAL *) scan;
           else scan++;}
  if (size<space)  {
    keyvals[size].kv_key=key;
    keyvals[size].kv_val=FD_EMPTY_CHOICE;
    fd_incref(key);
    *sizep=size+1;
    return &(keyvals[size]);}
  else {
    size_t new_space = (spacep) ? (space+4) : (space+1);
    struct FD_KEYVAL *nkeyvals= ((keyvals) && (freedata)) ?
      (u8_realloc_n(keyvals,new_space,struct FD_KEYVAL)) :
      (u8_alloc_n(new_space,struct FD_KEYVAL));
    if ((keyvals) && (!(freedata)))
      memcpy(nkeyvals,keyvals,(size)*sizeof(struct FD_KEYVAL));
    if (nkeyvals != keyvals)
      *keyvalp=nkeyvals;
    nkeyvals[size].kv_key=key;
    nkeyvals[size].kv_val=FD_EMPTY_CHOICE;
    fd_incref(key);
    if (spacep) *spacep=new_space;
    *sizep=size+1;
    return &(nkeyvals[size]);}
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
    if (FD_ATOMICP(v[i].kv_key)) i++;
    else {
      cons_sort_keyvals(v,n);
      return;}
  atomic_sort_keyvals(v,n);
}

FD_EXPORT struct FD_KEYVAL *_fd_sortvec_get
   (fdtype key,struct FD_KEYVAL *keyvals,int size)
{
  return fd_sortvec_get(key,keyvals,size);
}

FD_EXPORT struct FD_KEYVAL *fd_sortvec_insert
  (fdtype key,struct FD_KEYVAL **kvp,int *sizep,int *spacep,int freedata)
{
  struct FD_KEYVAL *keyvals=*kvp;
  int size=*sizep, space=((spacep)?(*spacep):(0)), found=0;
  struct FD_KEYVAL *bottom=keyvals, *top=bottom+size-1;
  struct FD_KEYVAL *limit=bottom+size, *middle=bottom+size/2;
  if (keyvals == NULL) {
    *kvp=keyvals=u8_alloc(struct FD_KEYVAL);
    memset(keyvals,0,sizeof(struct FD_KEYVAL));
    if (keyvals==NULL) return NULL;
    keyvals->kv_key=fd_incref(key);
    keyvals->kv_val=FD_EMPTY_CHOICE;
    if (sizep) *sizep=1;
    if (spacep) *spacep=1;
    return keyvals;}
  if (FD_ATOMICP(key))
    while (top>=bottom) {
      middle=bottom+(top-bottom)/2;
      if (middle>=limit) break;
      else if (key==middle->kv_key) {found=1; break;}
      else if (FD_CONSP(middle->kv_key)) top=middle-1;
      else if (key<middle->kv_key) top=middle-1;
      else bottom=middle+1;}
  else while (top>=bottom) {
    int comparison;
    middle=bottom+(top-bottom)/2;
    if (middle>=limit) break;
    comparison=cons_compare(key,middle->kv_key);
    if (comparison==0) {found=1; break;}
    else if (comparison<0) top=middle-1;
    else bottom=middle+1;}
  if (found) return middle;
  else if (size+1<space) {
    struct FD_KEYVAL *insert_point=&(keyvals[size+1]);
    *sizep=size+1;
    insert_point->kv_key=fd_incref(key);
    insert_point->kv_val=FD_EMPTY_CHOICE;
    sort_keyvals(keyvals,size+1);
    return insert_point;}
  else {
    int mpos=(middle-keyvals), dir=(bottom>middle), ipos=mpos+dir;
    struct FD_KEYVAL *insert_point;
    struct FD_KEYVAL *new_keyvals=
      ((freedata)?(u8_realloc_n(keyvals,size+1,struct FD_KEYVAL)):
       (u8_extalloc(keyvals,((size+1)*sizeof(struct FD_KEYVAL)),
                    (size*sizeof(struct FD_KEYVAL)))));
    if (new_keyvals==NULL) return NULL;
    *kvp=new_keyvals; *sizep=size+1; *spacep=size+1;
    insert_point=new_keyvals+ipos;
    memmove(insert_point+1,insert_point,
            sizeof(struct FD_KEYVAL)*(size-ipos));
    insert_point->kv_key=fd_incref(key);
    insert_point->kv_val=FD_EMPTY_CHOICE;
    return insert_point;}
}

FD_EXPORT fdtype _fd_slotmap_get
  (struct FD_SLOTMAP *sm,fdtype key,fdtype dflt)
{
  return fd_slotmap_get(sm,key,dflt);
}

FD_EXPORT fdtype _fd_slotmap_test(struct FD_SLOTMAP *sm,fdtype key,fdtype val)
{
  return fd_slotmap_test(sm,key,val);
}

FD_EXPORT int fd_slotmap_store(struct FD_SLOTMAP *sm,fdtype key,fdtype value)
{
  int unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if ((FD_ABORTP(value)))
    return fd_interr(value);
  if (sm->table_uselock) { u8_write_lock(&sm->table_rwlock); unlock=1;}
  {
    int rv=0;
    int cur_nslots=FD_XSLOTMAP_NUSED(sm), nslots=cur_nslots;
    int cur_allocd=FD_XSLOTMAP_NALLOCATED(sm), allocd=cur_allocd;
    struct FD_KEYVAL *cur_keyvals=sm->sm_keyvals;;
    struct FD_KEYVAL *result=
      (sm->sm_sort_keyvals) ?
      (fd_sortvec_insert(key,&(sm->sm_keyvals),&nslots,&allocd,
                         sm->sm_free_keyvals)) :
      (fd_keyvec_insert(key,&(sm->sm_keyvals),&nslots,&allocd,
                        sm->sm_free_keyvals));
    if (FD_EXPECT_FALSE(result==NULL)) {
      if (unlock) u8_rw_unlock(&(sm->table_rwlock));
      fd_seterr(fd_MallocFailed,"fd_slotmap_store",NULL,FD_VOID);
      return -1;}
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

FD_EXPORT int fd_slotmap_add(struct FD_SLOTMAP *sm,fdtype key,fdtype value)
{
  int unlock=0, retval=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if (FD_EMPTY_CHOICEP(value)) return 0;
  else if ((FD_ABORTP(value)))
    return fd_interr(value);
  if (sm->table_uselock) { u8_write_lock(&sm->table_rwlock); unlock=1;}
  {
    int cur_size=FD_XSLOTMAP_NUSED(sm), size=cur_size;
    int cur_space=FD_XSLOTMAP_NALLOCATED(sm), space=cur_space;
    struct FD_KEYVAL *cur_keyvals=sm->sm_keyvals;
    struct FD_KEYVAL *result=
      (sm->sm_sort_keyvals) ?
      (fd_sortvec_insert(key,&(sm->sm_keyvals),&size,&space,
                         sm->sm_free_keyvals)) :
      (fd_keyvec_insert(key,&(sm->sm_keyvals),&size,&space,
                        sm->sm_free_keyvals));
    if (FD_EXPECT_FALSE(result==NULL)) {
      if (unlock) u8_rw_unlock(&sm->table_rwlock);
      fd_seterr(fd_MallocFailed,"fd_slotmap_add",NULL,FD_VOID);
      return -1;}
    /* If this allocated a new keyvals structure, it needs to be
       freed.  (sm_free_kevyvals==0) when the keyvals are allocated at
       the end of the slotmap structure itself. */
    if (sm->sm_keyvals!=cur_keyvals) sm->sm_free_keyvals=1;
    fd_incref(value);
    FD_ADD_TO_CHOICE(result->kv_val,value);
    FD_XSLOTMAP_MARK_MODIFIED(sm);
    if (cur_space != space) {
      FD_XSLOTMAP_SET_NALLOCATED(sm,space); }
    if (cur_size  != size) {
      FD_XSLOTMAP_SET_NSLOTS(sm,size);
      retval=1;}}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return retval;
}

FD_EXPORT int fd_slotmap_drop(struct FD_SLOTMAP *sm,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int size, unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if ((FD_ABORTP(value)))
    return fd_interr(value);
  if (sm->table_uselock) { u8_write_lock(&sm->table_rwlock); unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm);
  result=fd_keyvec_get(key,sm->sm_keyvals,size);
  if (result) {
    fdtype newval=((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) :
                   (fd_difference(result->kv_val,value)));
    if ( (newval == result->kv_val) &&
         (!(FD_EMPTY_CHOICEP(newval))) ) {
      /* This is the case where, for example, value isn't on the slot.
         But we incref'd newvalue/result->kv_val, so we decref it.
         However, if the slot is already empty (for whatever reason),
         dropping the slot actually removes it from the slotmap. */
      fd_decref(newval);}
    else {
      FD_XSLOTMAP_MARK_MODIFIED(sm);
      if (FD_EMPTY_CHOICEP(newval)) {
        int entries_to_move=(size-(result-sm->sm_keyvals))-1;
        fd_decref(result->kv_key); fd_decref(result->kv_val);
        memmove(result,result+1,entries_to_move*sizeof(struct FD_KEYVAL));
        FD_XSLOTMAP_SET_NSLOTS(sm,size-1);}
      else {
        fd_decref(result->kv_val);
        result->kv_val=newval;}}}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  if (result)
    return 1;
  else return 0;
}

FD_EXPORT int fd_slotmap_delete(struct FD_SLOTMAP *sm,fdtype key)
{
  struct FD_KEYVAL *result; int size, unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if (sm->table_uselock) { u8_write_lock(&sm->table_rwlock); unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm);
  result=fd_keyvec_get(key,sm->sm_keyvals,size);
  if (result) {
    int entries_to_move=(size-(result-sm->sm_keyvals))-1;
    fd_decref(result->kv_key); fd_decref(result->kv_val);
    memmove(result,result+1,entries_to_move*sizeof(struct FD_KEYVAL));
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

FD_EXPORT fdtype fd_slotmap_keys(struct FD_SLOTMAP *sm)
{
  struct FD_KEYVAL *scan, *limit; int unlock=0;
  struct FD_CHOICE *result;
  fdtype *write; int size, atomic=1;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (sm->table_uselock) { u8_read_lock(&sm->table_rwlock); unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm); scan=sm->sm_keyvals; limit=scan+size;
  if (size==0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return FD_EMPTY_CHOICE;}
  else if (size==1) {
    fdtype key=fd_incref(scan->kv_key);
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return key;}
  /* Otherwise, copy the keys into a choice vector. */
  result=fd_alloc_choice(size);
  write=(fdtype *)FD_XCHOICE_DATA(result);
  while (scan < limit) {
    fdtype key=(scan++)->kv_key;
    if (FD_CONSP(key)) {fd_incref(key); atomic=0;}
    *write++=key;}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return fd_init_choice(result,size,NULL,
                        ((FD_CHOICE_REALLOC)|(FD_CHOICE_DOSORT)|
                         ((atomic)?(FD_CHOICE_ISATOMIC):
                          (FD_CHOICE_ISCONSES))));
}

FD_EXPORT fdtype fd_slotmap_values(struct FD_SLOTMAP *sm)
{
  struct FD_KEYVAL *scan, *limit; int unlock=0;
  struct FD_ACHOICE *achoice; fdtype results;
  int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (sm->table_uselock) { u8_read_lock(&sm->table_rwlock); unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm); scan=sm->sm_keyvals; limit=scan+size;
  if (size==0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return FD_EMPTY_CHOICE;}
  else if (size==1) {
    fdtype value=fd_incref(scan->kv_val);
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return value;}
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_achoice(NULL,7*(size),0);
  achoice=FD_XACHOICE(results);
  while (scan < limit) {
    fdtype value=(scan++)->kv_val;
    if (FD_CONSP(value)) {fd_incref(value);}
    _achoice_add(achoice,value);}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_simplify_choice(results);
}

FD_EXPORT fdtype fd_slotmap_assocs(struct FD_SLOTMAP *sm)
{
  struct FD_KEYVAL *scan, *limit; int unlock=0;
  struct FD_ACHOICE *achoice; fdtype results;
  int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (sm->table_uselock) { u8_read_lock(&sm->table_rwlock); unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm); scan=sm->sm_keyvals; limit=scan+size;
  if (size==0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return FD_EMPTY_CHOICE;}
  else if (size==1) {
    fdtype key=scan->kv_key, value=scan->kv_val;
    fd_incref(key); fd_incref(value);
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return fd_init_pair(NULL,key,value);}
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_achoice(NULL,7*(size),0);
  achoice=FD_XACHOICE(results);
  while (scan < limit) {
    fdtype key=scan->kv_key, value=scan->kv_val;
    fdtype assoc=fd_init_pair(NULL,key,value);
    fd_incref(key); fd_incref(value); scan++;
    _achoice_add(achoice,assoc);}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return fd_simplify_choice(results);
}

FD_EXPORT fdtype fd_slotmap_max
  (struct FD_SLOTMAP *sm,fdtype scope,fdtype *maxvalp)
{
  fdtype maxval=FD_VOID, result=FD_EMPTY_CHOICE;
  struct FD_KEYVAL *scan, *limit; int size, unlock=0;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (FD_EMPTY_CHOICEP(scope)) return result;
  if (sm->table_uselock) { u8_read_lock(&sm->table_rwlock); unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm); scan=sm->sm_keyvals; limit=scan+size;
  while (scan<limit) {
    if ((FD_VOIDP(scope)) || (fd_overlapp(scan->kv_key,scope))) {
      if (FD_EMPTY_CHOICEP(scan->kv_val)) {}
      else if (FD_NUMBERP(scan->kv_val)) {
        if (FD_VOIDP(maxval)) {
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
            FD_ADD_TO_CHOICE(result,scan->kv_key);}}}}
    scan++;}
  if (unlock)
    u8_rw_unlock(&sm->table_rwlock);
  if ((maxvalp) && (FD_NUMBERP(maxval)))
    *maxvalp=fd_incref(maxval);
  return result;
}

FD_EXPORT fdtype fd_slotmap_skim(struct FD_SLOTMAP *sm,fdtype maxval,
                                 fdtype scope)
{
  fdtype result=FD_EMPTY_CHOICE; int unlock=0;
  struct FD_KEYVAL *scan, *limit; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (FD_EMPTY_CHOICEP(scope)) return result;
  if (sm->table_uselock) { u8_read_lock(&sm->table_rwlock); unlock=1;}
  size=FD_XSLOTMAP_NUSED(sm); scan=sm->sm_keyvals; limit=scan+size;
  while (scan<limit) {
    if ((FD_VOIDP(scope)) || (fd_overlapp(scan->kv_key,scope)))
      if (FD_NUMBERP(scan->kv_val)) {
        int cmp=numcompare(scan->kv_val,maxval);
        if (cmp>=0) {
          fd_incref(scan->kv_key);
          FD_ADD_TO_CHOICE(result,scan->kv_key);}}
    scan++;}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return result;
}

FD_EXPORT fdtype fd_init_slotmap
  (struct FD_SLOTMAP *ptr,
   int len,struct FD_KEYVAL *data)
{
  if (ptr == NULL) ptr=u8_alloc(struct FD_SLOTMAP);
  FD_INIT_STRUCT(ptr,struct FD_SLOTMAP);
  FD_INIT_CONS(ptr,fd_slotmap_type);
  ptr->n_allocd=len;
  if (data) {
    ptr->sm_keyvals=data;
    ptr->n_slots=len;}
  else if (len) {
    ptr->sm_keyvals=u8_zalloc_n(len,struct FD_KEYVAL);
    ptr->n_slots=0;}
  else {
    ptr->n_slots=0;
    ptr->sm_keyvals=NULL;}
  ptr->table_modified=ptr->table_readonly=0;
  ptr->table_uselock=1;
  ptr->sm_free_keyvals=1; ptr->sm_sort_keyvals=0;
  u8_init_rwlock(&(ptr->table_rwlock));
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_make_slotmap(int space,int len,struct FD_KEYVAL *data)
{
  struct FD_SLOTMAP *ptr=
    u8_malloc((sizeof(struct FD_SLOTMAP))+
              (space*(sizeof(struct FD_KEYVAL))));
  struct FD_KEYVAL *kv=
    ((struct FD_KEYVAL *)(((unsigned char *)ptr)+sizeof(struct FD_SLOTMAP)));
  int i=0;
  FD_INIT_STRUCT(ptr,struct FD_SLOTMAP);
  FD_INIT_CONS(ptr,fd_slotmap_type);
  ptr->n_allocd=space; ptr->n_slots=len;
  if (data) while (i<len) {
      kv[i].kv_key=data[i].kv_key;
      kv[i].kv_val=data[i].kv_val;
      i++;}
  while (i<space) {
    kv[i].kv_key=FD_VOID;
    kv[i].kv_val=FD_VOID;
    i++;}
  ptr->sm_keyvals=kv;
  ptr->table_modified=ptr->table_readonly=0;
  ptr->table_uselock=1;
  ptr->sm_free_keyvals=0; ptr->sm_sort_keyvals=0;
  u8_init_rwlock(&(ptr->table_rwlock));
  return FDTYPE_CONS(ptr);
}

static fdtype copy_slotmap(fdtype smap,int flags)
{
  struct FD_SLOTMAP *cur=fd_consptr(fd_slotmap,smap,fd_slotmap_type);
  struct FD_SLOTMAP *fresh; int unlock=0;
  if (!(cur->sm_free_keyvals)) {
    fdtype copy; struct FD_SLOTMAP *consed; struct FD_KEYVAL *kvals;
    int i=0, len;
    copy=fd_make_slotmap(cur->n_allocd,cur->n_slots,cur->sm_keyvals);
    consed=(struct FD_SLOTMAP *)copy;
    kvals=consed->sm_keyvals; len=consed->n_slots;
    if (cur->table_uselock) {fd_read_lock_table(cur); unlock=1;}
    while (i<len) {
      fdtype key=kvals[i].kv_key, val=kvals[i].kv_val;
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
    memset(write,0,n*sizeof(struct FD_KEYVAL));
    while (read<read_limit) {
      fdtype key=read->kv_key, val=read->kv_val; read++;
      if (FD_CONSP(key)) write->kv_key=fd_copy(key);
      else write->kv_key=key;
      if (FD_CONSP(val))
        if (FD_ACHOICEP(val))
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
  return FDTYPE_CONS(fresh);
}

static void recycle_slotmap(struct FD_RAW_CONS *c)
{
  struct FD_SLOTMAP *sm=(struct FD_SLOTMAP *)c;
  fd_write_lock_table(sm);
  {
    int slotmap_size=FD_XSLOTMAP_NUSED(sm);
    const struct FD_KEYVAL *scan=sm->sm_keyvals;
    const struct FD_KEYVAL *limit=sm->sm_keyvals+slotmap_size;
    while (scan < limit) {
      fd_decref(scan->kv_key);
      fd_decref(scan->kv_val);
      scan++;}
    if (sm->sm_free_keyvals) u8_free(sm->sm_keyvals);
    fd_unlock_table(sm);
    u8_destroy_rwlock(&(sm->table_rwlock));
    u8_free(sm);
  }
}
static int unparse_slotmap(u8_output out,fdtype x)
{
  struct FD_SLOTMAP *sm=FD_XSLOTMAP(x);
  fd_read_lock_table(sm);
  {
    int slotmap_size=FD_XSLOTMAP_NUSED(sm);
    const struct FD_KEYVAL *scan=sm->sm_keyvals;
    const struct FD_KEYVAL *limit=sm->sm_keyvals+slotmap_size;
    u8_puts(out,"#[");
    if (scan<limit) {
      fd_unparse(out,scan->kv_key); u8_putc(out,' ');
      fd_unparse(out,scan->kv_val); scan++;}
    while (scan< limit) {
      u8_putc(out,' ');
      fd_unparse(out,scan->kv_key); u8_putc(out,' ');
      fd_unparse(out,scan->kv_val); scan++;}
    u8_puts(out,"]");
  }
  fd_unlock_table(sm);
  return 1;
}
static int compare_slotmaps(fdtype x,fdtype y,fd_compare_flags flags)
{
  int result=0; int unlockx=0, unlocky=0;
  struct FD_SLOTMAP *smx=(struct FD_SLOTMAP *)x;
  struct FD_SLOTMAP *smy=(struct FD_SLOTMAP *)y;
  int compare_lengths=(!(flags&FD_COMPARE_ELTS));
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
        int cmp=FDTYPE_COMPARE(xkeyvals[i].kv_key,ykeyvals[i].kv_key,flags);
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
             sizeof(struct FD_KEYVAL)*xsize);
      memcpy(ykvbuf,FD_XSLOTMAP_KEYVALS(smy),
             sizeof(struct FD_KEYVAL)*ysize);
      sort_keyvals(xkvbuf,xsize);
      sort_keyvals(ykvbuf,ysize);
      while (i<limit) {
        fdtype xkey=xkvbuf[i].kv_key, ykey=ykvbuf[i].kv_key;
        int key_cmp=FDTYPE_COMPARE(xkey,ykey,flags);
        if (key_cmp) {
          result=key_cmp;
          break;}
        else {
          fdtype xval=xkvbuf[i].kv_val, yval=ykvbuf[i].kv_val;
          int val_cmp=FDTYPE_COMPARE(xval,yval,flags);
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

static int build_table(fdtype table,fdtype key,fdtype value)
{
  if (FD_EMPTY_CHOICEP(value)) {
    if (fd_test(table,key,FD_VOID))
      return fd_add(table,key,value);
    else return fd_store(table,key,value);}
  else return fd_add(table,key,value);
}

FD_EXPORT fdtype fd_plist_to_slotmap(fdtype plist)
{
  if (!(FD_PAIRP(plist)))
    return fd_type_error(_("plist"),"fd_plist_to_slotmap",plist);
  else {
    fdtype scan=plist, result=fd_init_slotmap(NULL,0,NULL);
    while ((FD_PAIRP(scan))&&(FD_PAIRP(FD_CDR(scan)))) {
      fdtype key=FD_CAR(scan), value=FD_CADR(scan);
      build_table(result,key,value);
      scan=FD_CDR(scan); scan=FD_CDR(scan);}
    if (!(FD_EMPTY_LISTP(scan))) {
      fd_decref(result);
      return fd_type_error(_("plist"),"fd_plist_to_slotmap",plist);}
    else return result;}
}

FD_EXPORT int fd_sort_slotmap(fdtype slotmap,int sorted)
{
  if (!(FD_TYPEP(slotmap,fd_slotmap_type)))
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

FD_EXPORT fdtype fd_alist_to_slotmap(fdtype alist)
{
  if (!(FD_PAIRP(alist)))
    return fd_type_error(_("alist"),"fd_alist_to_slotmap",alist);
  else {
    fdtype result=fd_init_slotmap(NULL,0,NULL);
    FD_DOLIST(assoc,alist) {
      if (!(FD_PAIRP(assoc))) {
        fd_decref(result);
        return fd_type_error(_("alist"),"fd_alist_to_slotmap",alist);}
      else build_table(result,FD_CAR(assoc),FD_CDR(assoc));}
    return result;}
}

FD_EXPORT fdtype fd_blist_to_slotmap(fdtype blist)
{
  if (!(FD_PAIRP(blist)))
    return fd_type_error(_("binding list"),"fd_blist_to_slotmap",blist);
  else {
    fdtype result=fd_init_slotmap(NULL,0,NULL);
    FD_DOLIST(binding,blist) {
      if (!(FD_PAIRP(binding))) {
        fd_decref(result);
        return fd_type_error(_("binding list"),"fd_blist_to_slotmap",blist);}
      else {
        fdtype key=FD_CAR(binding), scan=FD_CDR(binding);
        if (FD_EMPTY_LISTP(scan)) 
          build_table(result,key,FD_EMPTY_CHOICE);
        else while (FD_PAIRP(scan)) {
          build_table(result,key,FD_CAR(scan));
          scan=FD_CDR(scan);}
        if (!(FD_EMPTY_LISTP(scan))) {
          fd_decref(result);
          return fd_type_error
            (_("binding list"),"fd_blist_to_slotmap",blist);}}}
    return result;}
}

/* Schema maps */

FD_EXPORT fdtype fd_make_schemap
  (struct FD_SCHEMAP *ptr,short size,short flags,
   fdtype *schema,fdtype *values)
{
  int i=0;
  if (ptr == NULL) ptr=u8_alloc(struct FD_SCHEMAP);
  FD_INIT_STRUCT(ptr,struct FD_SCHEMAP);
  FD_INIT_CONS(ptr,fd_schemap_type); ptr->table_schema=schema;
  ptr->schema_length=size; ptr->table_schema=schema;
  if (flags&FD_SCHEMAP_SORTED) ptr->schemap_sorted=1;
  if (!(flags&FD_SCHEMAP_PRIVATE)) ptr->schemap_shared=0;
  if (flags&FD_SCHEMAP_READONLY) ptr->table_readonly=1;
  if (flags&FD_SCHEMAP_MODIFIED) ptr->table_modified=1;
  if (values) ptr->schema_values=values;
  else {
    ptr->schema_values=values=u8_alloc_n(size,fdtype);
    while (i<size) values[i++]=FD_VOID;}
  u8_init_rwlock(&(ptr->table_rwlock));
  return FDTYPE_CONS(ptr);
}

FD_FASTOP void lispv_swap(fdtype *a,fdtype *b)
{
  fdtype t;
  t = *a;
  *a = *b;
  *b = t;
}

FD_EXPORT void fd_sort_schema(int n,fdtype *v)
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

FD_EXPORT fdtype *fd_register_schema(int n,fdtype *schema)
{
  fd_sort_schema(n,schema);
  return schema;
}

static fdtype copy_schemap(fdtype schemap,int flags)
{
  struct FD_SCHEMAP *ptr=
    fd_consptr(struct FD_SCHEMAP *,schemap,fd_schemap_type);
  struct FD_SCHEMAP *nptr=u8_alloc(struct FD_SCHEMAP);
  int i=0, size=FD_XSCHEMAP_SIZE(ptr);
  fdtype *ovalues=ptr->schema_values;
  fdtype *values=((size==0) ? (NULL) : (u8_alloc_n(size,fdtype)));
  fdtype *schema=ptr->table_schema, *nschema=NULL;
  FD_INIT_STRUCT(nptr,struct FD_SCHEMAP);
  FD_INIT_CONS(nptr,fd_schemap_type);
  if (ptr->schemap_onstack)
    nptr->table_schema=nschema=u8_alloc_n(size,fdtype);
  else nptr->table_schema=schema;
  if ( (nptr->table_schema != schema) )
    while (i < size) {
      fdtype val=ovalues[i];
      if (FD_CONSP(val))
        if (FD_ACHOICEP(val))
          values[i]=fd_make_simple_choice(val);
        else if ((flags&FD_FULL_COPY)||(FD_STATICP(val)))
          values[i]=fd_copier(val,flags);
        else values[i]=fd_incref(val);
      else values[i]=val;
      nschema[i]=schema[i];
      i++;}
  else if (flags) {
    fdtype val=ovalues[i];
    if (FD_CONSP(val))
      if (FD_ACHOICEP(val))
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
  u8_init_rwlock(&(nptr->table_rwlock));
  return FDTYPE_CONS(nptr);
}

FD_EXPORT fdtype fd_init_schemap
  (struct FD_SCHEMAP *ptr, short size, struct FD_KEYVAL *init)
{
  int i=0; fdtype *news, *newv;
  if (ptr == NULL) ptr=u8_alloc(struct FD_SCHEMAP);
  FD_INIT_STRUCT(ptr,struct FD_SCHEMAP);
  FD_INIT_CONS(ptr,fd_schemap_type);
  news=u8_alloc_n(size,fdtype);
  ptr->schema_values=newv=u8_alloc_n(size,fdtype);
  ptr->schema_length=size; ptr->schemap_sorted=1;
  sort_keyvals(init,size);
  while (i<size) {
    news[i]=init[i].kv_key; newv[i]=init[i].kv_val; i++;}
  ptr->table_schema=fd_register_schema(size,news);
  if (ptr->table_schema != news) {
    ptr->schemap_shared=1;
    u8_free(news);}
  u8_free(init);
  u8_init_rwlock(&(ptr->table_rwlock));
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype _fd_schemap_get
  (struct FD_SCHEMAP *sm,fdtype key,fdtype dflt)
{
  return fd_schemap_get(sm,key,dflt);
}

FD_EXPORT fdtype _fd_schemap_test
  (struct FD_SCHEMAP *sm,fdtype key,fdtype val)
{
  return fd_schemap_test(sm,key,val);
}


FD_EXPORT int fd_schemap_store
   (struct FD_SCHEMAP *sm,fdtype key,fdtype value)
{
  int slotno, size;
  FD_CHECK_TYPE_RET(sm,fd_schemap_type);
  if ((FD_ABORTP(value)))
    return fd_interr(value);
  fd_write_lock_table(sm);
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sm->schemap_sorted);
  if (slotno>=0) {
    fd_decref(sm->schema_values[slotno]);
    sm->schema_values[slotno]=fd_incref(value);
    FD_XSCHEMAP_MARK_MODIFIED(sm);
    fd_unlock_table(sm);
    return 1;}
  else {
    fd_unlock_table(sm);
    fd_seterr(fd_NoSuchKey,"fd_schemap_store",NULL,key);
    return -1;}
}

FD_EXPORT int fd_schemap_add
  (struct FD_SCHEMAP *sm,fdtype key,fdtype value)
{
  int slotno, size;
  FD_CHECK_TYPE_RET(sm,fd_schemap_type);
  if (FD_EMPTY_CHOICEP(value)) return 0;
  else if ((FD_ABORTP(value)))
    return fd_interr(value);
  fd_write_lock_table(sm);
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sm->schemap_sorted);
  if (slotno>=0) {
    fd_incref(value);
    FD_ADD_TO_CHOICE(sm->schema_values[slotno],value);
    FD_XSCHEMAP_MARK_MODIFIED(sm);
    fd_unlock_table(sm);
    return 1;}
  else {
    fd_unlock_table(sm);
    fd_seterr(fd_NoSuchKey,"fd_schemap_add",NULL,key);
    return -1;}
}

FD_EXPORT int fd_schemap_drop
  (struct FD_SCHEMAP *sm,fdtype key,fdtype value)
{
  int slotno, size;
  FD_CHECK_TYPE_RET(sm,fd_schemap_type);
  if ((FD_ABORTP(value)))
    return fd_interr(value);
  fd_write_lock_table(sm);
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->table_schema,size,sm->schemap_sorted);
  if (slotno>=0) {
    fdtype oldval=sm->schema_values[slotno];
    fdtype newval=((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) :
                   (fd_difference(oldval,value)));
    if (newval == oldval) fd_decref(newval);
    else {
      FD_XSCHEMAP_MARK_MODIFIED(sm);
      fd_decref(oldval); sm->schema_values[slotno]=newval;}
    fd_unlock_table(sm);
    return 1;}
  else {
    fd_unlock_table(sm);
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

FD_EXPORT fdtype fd_schemap_keys(struct FD_SCHEMAP *sm)
{
  FD_CHECK_TYPE_RETDTYPE(sm,fd_schemap_type);
  {
    int size=FD_XSCHEMAP_SIZE(sm);
    if (size==0) return FD_EMPTY_CHOICE;
    else if (size==1) return fd_incref(sm->table_schema[0]);
    else {
      struct FD_CHOICE *ch=fd_alloc_choice(size);
      memcpy((fdtype *)FD_XCHOICE_DATA(ch),sm->table_schema,sizeof(fdtype)*size);
      if (sm->schemap_sorted)
        return fd_init_choice(ch,size,sm->table_schema,0);
      else return fd_init_choice(ch,size,sm->table_schema,
                                 FD_CHOICE_INCREF|FD_CHOICE_DOSORT);}}
}

static void recycle_schemap(struct FD_RAW_CONS *c)
{
  struct FD_SCHEMAP *sm=(struct FD_SCHEMAP *)c;
  fd_write_lock_table(sm);
  {
    int schemap_size=FD_XSCHEMAP_SIZE(sm);
    fdtype *scan=sm->schema_values, *limit=sm->schema_values+schemap_size;
    while (scan < limit) {fd_decref(*scan); scan++;}
    if ((sm->table_schema) && (!(sm->schemap_shared)))
      u8_free(sm->table_schema);
    if (sm->schema_values) u8_free(sm->schema_values);
    fd_unlock_table(sm);
    u8_destroy_rwlock(&(sm->table_rwlock));
    u8_free(sm);
  }
}
static int unparse_schemap(u8_output out,fdtype x)
{
  struct FD_SCHEMAP *sm=FD_XSCHEMAP(x);
  fd_read_lock_table(sm);
  {
    int i=0, schemap_size=FD_XSCHEMAP_SIZE(sm);
    fdtype *schema=sm->table_schema, *values=sm->schema_values;
    u8_puts(out,"#[");
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
static int compare_schemaps(fdtype x,fdtype y,fd_compare_flags flags)
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
      fdtype *xschema=smx->table_schema, *yschema=smy->table_schema;
      fdtype *xvalues=smx->schema_values, *yvalues=smy->schema_values;
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
   ((hs->hs_n_elts*hs->hs_load_factor)>(hs->hs_n_slots))
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

static unsigned int type_multipliers[]=
  {200000093,210000067,220000073,230000059,240000083,
   250000073,260000093};
#define N_TYPE_MULTIPLIERS 6

FD_FASTOP unsigned int mult_hash_string
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

FD_EXPORT unsigned int fd_hash_string(u8_string string,int len)
{
  return mult_hash_string(string,len);
}

/* Hashing dtype pointers */

static unsigned int hash_elts(fdtype *x,unsigned int n);

static unsigned int hash_lisp(fdtype x)
{
  if (FD_CONSP(x))
    switch (FD_PTR_TYPE(x)) {
    case fd_string_type: {
      struct FD_STRING *s=
        fd_consptr(struct FD_STRING *,x,fd_string_type);
      return mult_hash_string(s->fd_bytes,s->fd_bytelen);}
    case fd_packet_type: case fd_secret_type: {
      struct FD_STRING *s=(struct FD_STRING *)x;
      return mult_hash_string(s->fd_bytes,s->fd_bytelen);}
    case fd_pair_type: {
      fdtype car=FD_CAR(x), cdr=FD_CDR(x);
      unsigned int hcar=fd_hash_lisp(car), hcdr=fd_hash_lisp(cdr);
      return hash_mult(hcar,hcdr);}
    case fd_vector_type: {
      struct FD_VECTOR *v=
        fd_consptr(struct FD_VECTOR *,x,fd_vector_type);
      return hash_elts(v->fdvec_elts,v->fdvec_length);}
    case fd_compound_type: {
      struct FD_COMPOUND *c=
        fd_consptr(struct FD_COMPOUND *,x,fd_compound_type);
      if (c->compound_isopaque) {
        int ctype=FD_PTR_TYPE(x);
        if ((ctype>0) && (ctype<N_TYPE_MULTIPLIERS))
          return hash_mult(x,type_multipliers[ctype]);
        else return hash_mult(x,MYSTERIOUS_MULTIPLIER);}
      else return hash_mult
             (hash_lisp(c->compound_typetag),
              hash_elts(&(c->compound_0),c->fd_n_elts));}
    case fd_slotmap_type: {
      struct FD_SLOTMAP *sm=
        fd_consptr(struct FD_SLOTMAP *,x,fd_slotmap_type);
      fdtype *kv=(fdtype *)sm->sm_keyvals;
      return hash_elts(kv,sm->n_slots*2);}
    case fd_choice_type: {
      struct FD_CHOICE *ch=
        fd_consptr(struct FD_CHOICE *,x,fd_choice_type);
      int size=FD_XCHOICE_SIZE(ch);
      return hash_elts((fdtype *)(FD_XCHOICE_DATA(ch)),size);}
    case fd_achoice_type: {
      fdtype simple=fd_make_simple_choice(x);
      int hash=hash_lisp(simple);
      fd_decref(simple);
      return hash;}
    case fd_qchoice_type: {
      struct FD_QCHOICE *ch=
        fd_consptr(struct FD_QCHOICE *,x,fd_qchoice_type);
      return hash_lisp(ch->qchoiceval);}
    default: {
      int ctype=FD_PTR_TYPE(x);
      if ((ctype<FD_TYPE_MAX) && (fd_hashfns[ctype]))
        return fd_hashfns[ctype](x,fd_hash_lisp);
      else return hash_mult(x,MYSTERIOUS_MULTIPLIER);}}
  else {
    int ctype=FD_PTR_TYPE(x);
    if ((ctype>0) && (ctype<N_TYPE_MULTIPLIERS))
      return hash_mult(x,type_multipliers[ctype]);
    else return hash_mult(x,MYSTERIOUS_MULTIPLIER);}
}

FD_EXPORT unsigned int fd_hash_lisp(fdtype x)
{
  return hash_lisp(x);
}

/* hash_elts: (static)
     arguments: a pointer to a vector of dtype pointers and a size
     Returns: combines the elements' hashes into a single hash
*/
static unsigned int hash_elts(fdtype *x,unsigned int n)
{
  fdtype *limit=x+n; int sum=0;
  while (x < limit) {
    unsigned int h=fd_hash_lisp(*x);
    sum=hash_combine(sum,h); sum=sum%(MYSTERIOUS_MODULUS); x++;}
  return sum;
}

/* Hashvecs */

FD_EXPORT struct FD_KEYVAL *fd_hashvec_get
  (fdtype key,struct FD_HASH_BUCKET **slots,int n_slots)
{
  unsigned int hash=fd_hash_lisp(key), offset=compute_offset(hash,n_slots);
  struct FD_HASH_BUCKET *he=slots[offset];
  if (he == NULL) return NULL;
  else if (FD_EXPECT_TRUE(he->fd_n_entries == 1))
    if (FDTYPE_EQUAL(key,he->kv_val0.kv_key))
      return &(he->kv_val0);
    else return NULL;
  else return fd_sortvec_get(key,&(he->kv_val0),he->fd_n_entries);
}

FD_EXPORT struct FD_KEYVAL *fd_hash_bucket_insert
  (fdtype key,struct FD_HASH_BUCKET **hep)
{
  struct FD_HASH_BUCKET *he=*hep; int found=0;
  struct FD_KEYVAL *keyvals=&(he->kv_val0); int size=he->fd_n_entries;
  struct FD_KEYVAL *bottom=keyvals, *top=bottom+(size-1);
  struct FD_KEYVAL *middle=bottom+(top-bottom)/2;;
  while (top>=bottom) { /* (!(top<bottom)) */
    middle=bottom+(top-bottom)/2;
    if (FDTYPE_EQUAL(key,middle->kv_key)) {found=1; break;}
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
                     (size)*sizeof(struct FD_KEYVAL)));
    memset((((unsigned char *)new_hashentry)+
            (sizeof(struct FD_HASH_BUCKET))+
            ((size-1)*sizeof(struct FD_KEYVAL))),
           0,sizeof(struct FD_KEYVAL));
    *hep=new_hashentry; new_hashentry->fd_n_entries++;
    insert_point=&(new_hashentry->kv_val0)+ipos;
    memmove(insert_point+1,insert_point,
            sizeof(struct FD_KEYVAL)*(size-ipos));
    insert_point->kv_key=fd_incref(key);
    insert_point->kv_val=FD_EMPTY_CHOICE;
    return insert_point;}
}

FD_EXPORT struct FD_KEYVAL *fd_hashvec_insert
  (fdtype key,struct FD_HASH_BUCKET **slots,int n_slots,int *n_keys)
{
  unsigned int hash=fd_hash_lisp(key), offset=compute_offset(hash,n_slots);
  struct FD_HASH_BUCKET *he=slots[offset];
  if (he == NULL) {
    he=u8_zalloc(struct FD_HASH_BUCKET);
    FD_INIT_STRUCT(he,struct FD_HASH_BUCKET);
    he->fd_n_entries=1;
    he->kv_val0.kv_key=fd_incref(key);
    he->kv_val0.kv_val=FD_EMPTY_CHOICE;
    slots[offset]=he; if (n_keys) (*n_keys)++;
    return &(he->kv_val0);}
  else if (he->fd_n_entries == 1) {
    if (FDTYPE_EQUAL(key,he->kv_val0.kv_key))
      return &(he->kv_val0);
    else {
      if (n_keys) (*n_keys)++;
      return fd_hash_bucket_insert(key,&slots[offset]);}}
  else {
    int size=he->fd_n_entries;
    struct FD_KEYVAL *kv=fd_hash_bucket_insert(key,&slots[offset]);
    if ((n_keys) && (slots[offset]->fd_n_entries > size)) (*n_keys)++;
    return kv;}
}

/* Hashtables */

/* Optimizing ACHOICEs in hashtables

   An ACHOICE in a hashtable should always be a unique value, since
   adding to a key in the table shouldn't effect anything else,
   especially whatever ACHOICE was passed in.

   We implement this as follows:

   When storing, if we have an ACHOICE, we always copy it and set
     uselock to zero in the copy.

   When adding, we don't bother locking and just add straight away.

   When getting, we normalize the results and return that.

   When dropping, we normalize and store that.
*/

FD_EXPORT fdtype fd_hashtable_get
  (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return fd_incref(dflt);
  if (ht->table_uselock) { 
    fd_read_lock_table(ht); 
    unlock=1; }
  if (ht->table_n_keys == 0) {
    if (unlock) fd_unlock_table(ht);
    return fd_incref(dflt);}
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    fdtype rv=result->kv_val;
    if (FD_VOIDP(rv)) {
      if (unlock) fd_unlock_table(ht);
      return fd_incref(dflt);}
    else if (FD_ACHOICEP(rv)) {
      fdtype simple=fd_make_simple_choice(rv);
      if (unlock) fd_unlock_table(ht);
      return simple;}
    else {
      if (unlock) fd_unlock_table(ht);
      return fd_incref(rv);}}
  else {
    if (unlock) fd_unlock_table(ht);
    return fd_incref(dflt);}
}

FD_EXPORT fdtype fd_hashtable_get_nolock
  (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return fd_incref(dflt);
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    fdtype rv=result->kv_val;
    fdtype v=((FD_VOIDP(rv))?(fd_incref(dflt),dflt):
              (FD_ACHOICEP(rv))?
              (fd_make_simple_choice(rv)) :
              (fd_incref(rv)));
    return v;}
  else {
    return fd_incref(dflt);}
}

FD_EXPORT fdtype fd_hashtable_get_noref
  (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return dflt;
  if (ht->table_uselock) { fd_read_lock_table(ht); unlock=1; }
  if (ht->table_n_keys == 0) {
    if (unlock) fd_unlock_table(ht);
    return dflt;}
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    fdtype rv=result->kv_val;
    if (FD_VOIDP(rv)) {
      if (unlock) fd_unlock_table(ht);
      return dflt;}
    else if (FD_ACHOICEP(rv)) {
      rv=result->kv_val=fd_simplify_choice(rv);
      if (unlock) fd_unlock_table(ht);
      return rv;}
    else {
      if (unlock) fd_unlock_table(ht);
      return rv;}}
  else {
    if (unlock) fd_unlock_table(ht);
    return dflt;}
}

FD_EXPORT fdtype fd_hashtable_get_nolockref
  (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return dflt;
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    fdtype rv=result->kv_val;
    if (FD_VOIDP(rv)) return dflt;
    else if (FD_ACHOICEP(rv)) {
      result->kv_val=fd_simplify_choice(rv);
      return result->kv_val;}
    else return rv;}
  else return dflt;
}

FD_EXPORT int fd_hashtable_probe(struct FD_HASHTABLE *ht,fdtype key)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return 0;
  if (ht->table_uselock) { fd_read_lock_table(ht); unlock=1; }
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

static int hashtable_test(struct FD_HASHTABLE *ht,fdtype key,fdtype val)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return 0;
  if (ht->table_uselock) {fd_read_lock_table(ht); unlock=1;}
  if (ht->table_n_keys == 0) {
    if (unlock) fd_unlock_table(ht);
    return 0;}
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    fdtype current=result->kv_val; int cmp;
    if (FD_VOIDP(val)) cmp=(!(FD_VOIDP(current)));
    /* This used to return 0 if the value was the empty choice, but that's not
       consistent with the other table test functions and got Scheme's WHEREFROM
       into trouble. */
    /* if (FD_EMPTY_CHOICEP(current)) cmp=0; else cmp=1; */
    else if (FD_EQ(val,current)) cmp=1;
    else if ((FD_CHOICEP(val)) || (FD_ACHOICEP(val)) ||
             (FD_CHOICEP(current)) || (FD_ACHOICEP(current)))
      cmp=fd_overlapp(val,current);
    else if (FD_EQUAL(val,current)) cmp=1;
    else cmp=0;
    if (unlock) fd_unlock_table(ht);
    return cmp;}
  else {
    if (unlock) u8_rw_unlock((&(ht->table_rwlock)));
    return 0;}
}

/* ?? */
FD_EXPORT int fd_hashtable_probe_novoid(struct FD_HASHTABLE *ht,fdtype key)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return 0;
  if (ht->table_uselock) {fd_read_lock_table(ht); unlock=1;}
  if (ht->table_n_keys == 0) {
    if (unlock) fd_unlock_table(ht);
    return 0;}
  else result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if ((result) && (!(FD_VOIDP(result->kv_val)))) {
    if (unlock) fd_unlock_table(ht);
    return 1;}
  else {
    if (unlock) u8_rw_unlock((&(ht->table_rwlock)));
    return 0;}
}

static void setup_hashtable(struct FD_HASHTABLE *ptr,int n_slots)
{
  struct FD_HASH_BUCKET **slots;
  if (n_slots < 0) n_slots=fd_get_hashtable_size(-n_slots);
  ptr->ht_n_buckets=n_slots; ptr->table_n_keys=0; ptr->table_modified=0;
  ptr->ht_buckets=slots=
    u8_zalloc_n(n_slots,struct FD_HASH_BUCKET *);
}

FD_EXPORT int fd_hashtable_store(fd_hashtable ht,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int n_keys, added;
  fdtype newv, oldv;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if ((FD_ABORTP(value))) return fd_interr(value);
  if (ht->table_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_store",NULL,key);
    return -1;}
  fd_write_lock_table(ht);
  if (ht->ht_n_buckets == 0) setup_hashtable(ht,fd_init_hash_size);
  n_keys=ht->table_n_keys;
  result=fd_hashvec_insert
    (key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys));
  if ( (ht->table_n_keys) > n_keys ) added=1; else added=0;
  ht->table_modified=1; oldv=result->kv_val;
  if (FD_ACHOICEP(value))
    /* Copy achoices */
    newv=fd_make_simple_choice(value);
  else newv=fd_incref(value);
  if (FD_ABORTP(newv)) {
    fd_unlock_table(ht);
    return fd_interr(newv);}
  else {
    result->kv_val=newv; fd_decref(oldv);}
  fd_unlock_table(ht);
  if (FD_EXPECT_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_slots < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

static int add_to_hashtable(fd_hashtable ht,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result;
  int added=0, n_keys=ht->table_n_keys;
  if (FD_ABORTP(value)) {
    fd_unlock_table(ht);
    return fd_interr(value);}
  else if (ht->table_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_add",NULL,key);
    return -1;}
  else if (FD_EXPECT_FALSE(FD_EMPTY_CHOICEP(value)))
    return 0;
  else if (FD_EXPECT_FALSE(FD_ABORTP(value)))
    return fd_interr(value);
  else fd_incref(value);
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->ht_n_buckets == 0) setup_hashtable(ht,fd_init_hash_size);
  n_keys=ht->table_n_keys;
  result=fd_hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys));
  if (ht->table_n_keys>n_keys) added=1; 
  ht->table_modified=1;
  if (FD_VOIDP(result->kv_val))
    result->kv_val=value;
  else {FD_ADD_TO_CHOICE(result->kv_val,value);}
  /* If the value is an achoice, it doesn't need to be locked because
     it will be protected by the hashtable's lock.  However this requires
     that we always normalize the choice when we return it.  */
  if (FD_ACHOICEP(result->kv_val)) {
    struct FD_ACHOICE *ch=FD_XACHOICE(result->kv_val);
    if (ch->achoice_uselock) ch->achoice_uselock=0;}
  fd_unlock_table(ht);
  return added;
}

static int check_hashtable_size(fd_hashtable ht,ssize_t delta)
{
  size_t n_keys=ht->table_n_keys, n_slots=ht->ht_n_buckets; 
  size_t need_keys=(delta<0) ? 
    (n_keys+fd_init_hash_size+3) : 
    (n_keys+delta+fd_init_hash_size);
  if (n_slots==0) {
    setup_hashtable(ht,fd_get_hashtable_size(need_keys));
    return ht->ht_n_buckets;}
  else if (need_keys>n_slots) {
    size_t new_size=fd_get_hashtable_size(need_keys*2);
    resize_hashtable(ht,new_size,0);
    return ht->ht_n_buckets;}
  else return 0;
}

FD_EXPORT int fd_hashtable_add(fd_hashtable ht,fdtype key,fdtype value)
{
  int added=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_add",NULL,key);
    return -1;}
  if (FD_EXPECT_FALSE(FD_EMPTY_CHOICEP(value)))
    return 0;
  else if ((FD_ABORTP(value)))
    return fd_interr(value);
  fd_write_lock_table(ht);
  if (!(FD_CONSP(key)))
    check_hashtable_size(ht,3);
  else if (FD_CHOICEP(key))
    check_hashtable_size(ht,FD_CHOICE_SIZE(key));
  else if (FD_ACHOICEP(key))
    check_hashtable_size(ht,FD_ACHOICE_SIZE(key));
  else check_hashtable_size(ht,3);
  /* These calls unlock the hashtable */
  if ( (FD_CHOICEP(key)) || (FD_ACHOICEP(key)) ) {
    FD_DO_CHOICES(eachkey,key) {
      added+=add_to_hashtable(ht,key,value);}}
  else added=add_to_hashtable(ht,key,value);
  return added;
}

FD_EXPORT int fd_hashtable_drop
  (struct FD_HASHTABLE *ht,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_n_keys == 0) return 0;
  if (ht->table_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_drop",NULL,key);
    return -1;}
  fd_write_lock_table(ht);
  result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
  if (result) {
    fdtype newval=
      ((FD_VOIDP(value)) ? (FD_VOID) :
       (fd_difference(result->kv_val,value)));
    fd_decref(result->kv_val);
    result->kv_val=newval;
    ht->table_modified=1;
    fd_unlock_table(ht);
    return 1;}
  fd_unlock_table(ht);
  return 0;
}

static fdtype restore_hashtable(fdtype tag,fdtype alist,fd_compound_typeinfo e)
{
  fdtype *keys, *vals; int n=0; struct FD_HASHTABLE *new;
  if (FD_PAIRP(alist)) {
    int max=64;
    keys=u8_alloc_n(max,fdtype);
    vals=u8_alloc_n(max,fdtype);
    {FD_DOLIST(elt,alist) {
      if (n>=max) {
        keys=u8_realloc_n(keys,max*2,fdtype);
        vals=u8_realloc_n(vals,max*2,fdtype);
        max=max*2;}
      keys[n]=FD_CAR(elt); vals[n]=FD_CDR(elt); n++;}}}
  else return fd_err(fd_DTypeError,"restore_hashtable",NULL,alist);
  new=(struct FD_HASHTABLE *)fd_make_hashtable(NULL,n*2);
  fd_hashtable_iter(new,fd_table_add,n,keys,vals);
  u8_free(keys); u8_free(vals);
  new->table_modified=0;
  return FDTYPE_CONS(new);
}

/* Evaluting hashtables */

FD_EXPORT void fd_hash_quality
  (unsigned int *hashv,int n_keys,int n_slots,
   unsigned int *buf,unsigned int bufsiz,
   unsigned int *nbucketsp,unsigned int *maxbucketp,
   unsigned int *ncollisionsp)
{
  int i, mallocd=0;
  int n_buckets=0, n_collisions=0, max_bucket=0;
  if ((buf) && (n_slots<bufsiz))
    memset(buf,0,sizeof(unsigned int)*n_slots);
  else {
    buf=u8_alloc_n(n_slots,unsigned int); mallocd=1;
    memset(buf,0,sizeof(unsigned int)*n_slots);}
  i=0; while (i<n_keys)
    if (hashv[i]) {
      int offset=compute_offset(hashv[i],n_slots);
      buf[offset]++;
      i++;}
    else i++;
  i=0; while (i<n_slots)
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

static int do_hashtable_op
  (struct FD_HASHTABLE *ht,fd_tableop op,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int added=0, was_achoice=0;
  if (FD_EMPTY_CHOICEP(key)) return 0;
  if ((ht->table_readonly) && (op!=fd_table_test)) {
    fd_seterr(fd_ReadOnlyHashtable,"do_hashtable_op",NULL,FD_VOID);
    return -1;}
  switch (op) {
  case fd_table_replace: case fd_table_replace_novoid: case fd_table_drop:
  case fd_table_add_if_present: case fd_table_test:
  case fd_table_increment_if_present: case fd_table_multiply_if_present:
  case fd_table_maximize_if_present: case fd_table_minimize_if_present: {
    /* These are operations which can be resolved immediately if the key
       does not exist in the table.  It doesn't bother setting up the hashtable
       if it doesn't have to. */
    if (ht->table_n_keys == 0) return 0;
    result=fd_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets);
    if (result==NULL) return 0; 
    else { added=1; break; }}
  case fd_table_add: case fd_table_add_noref:
    if ((FD_EMPTY_CHOICEP(value)) &&
        ((op == fd_table_add) ||
         (op == fd_table_add_noref) ||
         (op == fd_table_add_if_present) ||
         (op == fd_table_test) ||
         (op == fd_table_drop)))
      return 0;
  default:
    if (ht->ht_n_buckets == 0) setup_hashtable(ht,fd_init_hash_size);
    result=fd_hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,
                             &(ht->table_n_keys));}
  if ((!(result))&&
      ((op==fd_table_drop)||
       (op==fd_table_test)||
       (op==fd_table_replace)||
       (op==fd_table_add_if_present)||
       (op==fd_table_multiply_if_present)||
       (op==fd_table_maximize_if_present)||
       (op==fd_table_minimize_if_present)||
       (op==fd_table_increment_if_present)))
    return 0;
  if ((result)&&(FD_ACHOICEP(result->kv_val))) was_achoice=1;
  switch (op) {
  case fd_table_replace_novoid:
    if (FD_VOIDP(result->kv_val)) return 0;
  case fd_table_store: case fd_table_replace: {
    fdtype newv=fd_incref(value);
    fd_decref(result->kv_val); result->kv_val=newv;
    break;}
  case fd_table_store_noref:
    fd_decref(result->kv_val);
    result->kv_val=value; 
    break;
  case fd_table_add_if_present:
    if (FD_VOIDP(result->kv_val)) break;
  case fd_table_add: case fd_table_add_empty:
    fd_incref(value);
    if (FD_VOIDP(result->kv_val)) result->kv_val=value;
    else {FD_ADD_TO_CHOICE(result->kv_val,value);}
    break;
  case fd_table_add_noref: case fd_table_add_empty_noref:
    if (FD_VOIDP(result->kv_val)) result->kv_val=value;
    else {FD_ADD_TO_CHOICE(result->kv_val,value);}
    break;
  case fd_table_drop: 
    if ((FD_VOIDP(value))||(fd_overlapp(value,result->kv_val))) {
      fdtype newval=((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) : 
                     (fd_difference(result->kv_val,value)));
      fd_decref(result->kv_val); 
      result->kv_val=newval;
      break;}
    else return 0;
  case fd_table_test:
    if ((FD_CHOICEP(result->kv_val)) || 
        (FD_ACHOICEP(result->kv_val)) ||
        (FD_CHOICEP(value)) || 
        (FD_ACHOICEP(value)))
      return fd_overlapp(value,result->kv_val);
    else if (FDTYPE_EQUAL(value,result->kv_val))
      return 1;
    else return 0;
  case fd_table_default:
    if ((FD_EMPTY_CHOICEP(result->kv_val)) ||
        (FD_VOIDP(result->kv_val))) {
      result->kv_val=fd_incref(value);}
    break;
  case fd_table_increment_if_present:
    if (FD_VOIDP(result->kv_val)) break;
  case fd_table_increment:
    if ((FD_EMPTY_CHOICEP(result->kv_val)) ||
        (FD_VOIDP(result->kv_val))) {
      result->kv_val=fd_incref(value);}
    else if (!(FD_NUMBERP(result->kv_val))) {
      fd_seterr(fd_TypeError,"fd_table_increment",
                u8_strdup("number"),result->kv_val);
      return -1;}
    else {
      fdtype current=result->kv_val;
      FD_DO_CHOICES(v,value)
        if ((FD_FIXNUMP(current)) && (FD_FIXNUMP(v))) {
          long long cval=FD_FIX2INT(current);
          long long delta=FD_FIX2INT(v);
          result->kv_val=FD_INT(cval+delta);}
        else if ((FD_FLONUMP(current)) &&
                 (FD_CONS_REFCOUNT(((fd_cons)current))<2) &&
                 ((FD_FIXNUMP(v)) || (FD_FLONUMP(v)))) {
          struct FD_FLONUM *dbl=(fd_flonum)current;
          if (FD_FIXNUMP(v))
            dbl->floval=dbl->floval+FD_FIX2INT(v);
          else dbl->floval=dbl->floval+FD_FLONUM(v);}
        else if (FD_NUMBERP(v)) {
          fdtype newnum=fd_plus(current,v);
          if (newnum != current) {
            fd_decref(current);
            result->kv_val=newnum;}}
        else {
          fd_seterr(fd_TypeError,"fd_table_increment",
                    u8_strdup("number"),v);
          return -1;}}
    break;
  case fd_table_multiply_if_present:
    if (FD_VOIDP(result->kv_val)) break;
  case fd_table_multiply:
    if ((FD_VOIDP(result->kv_val))||(FD_EMPTY_CHOICEP(result->kv_val)))  {
      result->kv_val=fd_incref(value);}
    else if (!(FD_NUMBERP(result->kv_val))) {
      fd_seterr(fd_TypeError,"fd_table_multiply",
                u8_strdup("number"),result->kv_val);
      return -1;}
    else {
      fdtype current=result->kv_val;
      FD_DO_CHOICES(v,value)
        if ((FD_INTP(current)) && (FD_INTP(v))) {
          long long cval=FD_FIX2INT(current);
          long long factor=FD_FIX2INT(v);
          result->kv_val=FD_INT(cval*factor);}
        else if ((FD_FLONUMP(current)) &&
                 (FD_CONS_REFCOUNT(((fd_cons)current))<2) &&
                 ((FD_FIXNUMP(v)) || (FD_FLONUMP(v)))) {
          struct FD_FLONUM *dbl=(fd_flonum)current;
          if (FD_FIXNUMP(v))
            dbl->floval=dbl->floval*FD_FIX2INT(v);
          else dbl->floval=dbl->floval*FD_FLONUM(v);}
        else if (FD_NUMBERP(v)) {
          fdtype newnum=fd_multiply(current,v);
          if (newnum != current) {
            fd_decref(current);
            result->kv_val=newnum;}}
        else {
          fd_seterr(fd_TypeError,"table_multiply_op",
                    u8_strdup("number"),v);
          return -1;}}
    break;
  case fd_table_maximize_if_present:
    if (FD_VOIDP(result->kv_val)) break;
  case fd_table_maximize:
    if ((FD_EMPTY_CHOICEP(result->kv_val)) ||
        (FD_VOIDP(result->kv_val))) {
      result->kv_val=fd_incref(value);}
    else if (!(FD_NUMBERP(result->kv_val))) {
      fd_seterr(fd_TypeError,"table_maximize_op",
                u8_strdup("number"),result->kv_val);
      return -1;}
    else {
      fdtype current=result->kv_val;
      if ((FD_NUMBERP(current)) && (FD_NUMBERP(value))) {
        if (fd_numcompare(value,current)>0) {
          result->kv_val=fd_incref(value);
          fd_decref(current);}}
      else {
        fd_seterr(fd_TypeError,"table_maximize_op",
                  u8_strdup("number"),value);
        return -1;}}
    break;
  case fd_table_minimize_if_present:
    if (FD_VOIDP(result->kv_val)) break;
  case fd_table_minimize:
    if ((FD_EMPTY_CHOICEP(result->kv_val))||(FD_VOIDP(result->kv_val))) {
      result->kv_val=fd_incref(value);}
    else if (!(FD_NUMBERP(result->kv_val))) {
      fd_seterr(fd_TypeError,"table_maximize_op",
                u8_strdup("number"),result->kv_val);
      return -1;}
    else {
      fdtype current=result->kv_val;
      if ((FD_NUMBERP(current)) && (FD_NUMBERP(value))) {
        if (fd_numcompare(value,current)<0) {
          result->kv_val=fd_incref(value);
          fd_decref(current);}}
      else {
        fd_seterr(fd_TypeError,"table_maximize_op",
                  u8_strdup("number"),value);
        return -1;}}
    break;
  case fd_table_push:
    if ((FD_VOIDP(result->kv_val)) || (FD_EMPTY_CHOICEP(result->kv_val))) {
      result->kv_val=fd_make_pair(value,FD_EMPTY_LIST);}
    else if (FD_PAIRP(result->kv_val)) {
      result->kv_val=fd_conspair(fd_incref(value),result->kv_val);}
    else {
      fdtype tail=fd_conspair(result->kv_val,FD_EMPTY_LIST);
      result->kv_val=fd_conspair(fd_incref(value),tail);}
    break;
  default:
    added=-1;
    fd_seterr3(BadHashtableMethod,"do_hashtable_op",u8_mkstring("0x%x",op));
    break;
  }
  ht->table_modified=1;
  if ((was_achoice==0) && (FD_ACHOICEP(result->kv_val))) {
    /* If we didn't have an achoice before and we do now, that means
       a new achoice was created with a mutex and everything.  We can
       safely destroy it and set the choice to not use locking, since
       the value will be protected by the hashtable's lock. */
    struct FD_ACHOICE *ch=FD_XACHOICE(result->kv_val);
    if (ch->achoice_uselock) ch->achoice_uselock=0;}
  return added;
}

FD_EXPORT int fd_hashtable_op
   (struct FD_HASHTABLE *ht,fd_tableop op,fdtype key,fdtype value)
{
  int added;
  if (FD_EMPTY_CHOICEP(key)) return 0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  fd_write_lock_table(ht);
  added=do_hashtable_op(ht,op,key,value);
  fd_unlock_table(ht);
  if (FD_EXPECT_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_slots < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_op_nolock
   (struct FD_HASHTABLE *ht,fd_tableop op,fdtype key,fdtype value)
{
  int added;
  if (FD_EMPTY_CHOICEP(key)) return 0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  added=do_hashtable_op(ht,op,key,value);
  if (FD_EXPECT_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_slots < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    resize_hashtable(ht,new_size,0);}
  return added;
}

FD_EXPORT int fd_hashtable_iter
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    const fdtype *keys,const fdtype *values)
{
  int i=0, added=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  fd_write_lock_table(ht);
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0)
      added=do_hashtable_op(ht,op,keys[i],values[i]);
    else do_hashtable_op(ht,op,keys[i],values[i]);
    i++;}
  fd_unlock_table(ht);
  if (FD_EXPECT_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_slots < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_iterkeys
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    const fdtype *keys,fdtype value)
{
  int i=0, added=0; int unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_uselock) { fd_write_lock_table(ht); unlock=1;}
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0)
      added=do_hashtable_op(ht,op,keys[i],value);
    else do_hashtable_op(ht,op,keys[i],value);
    if (added<0) {
      if (unlock) fd_unlock_table(ht);
      return added;}
    i++;}
  if (unlock) fd_unlock_table(ht);
  if (FD_EXPECT_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_slots < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_itervals
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    fdtype key,const fdtype *values)
{
  int i=0, added=0; int unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->table_uselock) { fd_write_lock_table(ht); unlock=1;}
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0)
      added=do_hashtable_op(ht,op,key,values[i]);
    else do_hashtable_op(ht,op,key,values[i]);
    i++;}
  if (unlock) fd_unlock_table(ht);
  if (FD_EXPECT_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_slots < loading/4;
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

FD_EXPORT fdtype fd_hashtable_keys(struct FD_HASHTABLE *ptr)
{
  fdtype result=FD_EMPTY_CHOICE; int unlock=0;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  if (ptr->table_uselock) {u8_read_lock(&ptr->table_rwlock); unlock=1;}
  {
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          if (FD_VOIDP(kvscan->kv_val)) {kvscan++;continue;}
          fd_incref(kvscan->kv_key);
          FD_ADD_TO_CHOICE(result,kvscan->kv_key);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  return fd_simplify_choice(result);
}

FD_EXPORT fdtype fd_hashtable_values(struct FD_HASHTABLE *ptr)
{
  int unlock=0;
  struct FD_ACHOICE *achoice; fdtype results;
  int size;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  if (ptr->table_uselock) {u8_read_lock(&ptr->table_rwlock); unlock=1;}
  size=ptr->table_n_keys;
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_achoice(NULL,17*(size),0);
  achoice=FD_XACHOICE(results);
  {
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          fdtype value=kvscan->kv_val;
          if ((FD_VOIDP(value))||(FD_EMPTY_CHOICEP(value))) {
            kvscan++; continue;}
          fd_incref(value);
          _achoice_add(achoice,value);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_simplify_choice(results);
}

FD_EXPORT fdtype fd_hashtable_assocs(struct FD_HASHTABLE *ptr)
{
  int unlock=0;
  struct FD_ACHOICE *achoice; fdtype results;
  int size;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  if (ptr->table_uselock) {u8_read_lock(&ptr->table_rwlock); unlock=1;}
  size=ptr->table_n_keys;
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_achoice(NULL,17*(size),0);
  achoice=FD_XACHOICE(results);
  {
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          fdtype key=kvscan->kv_key, value=kvscan->kv_val, assoc;
          if (FD_VOIDP(value)) {
            kvscan++; continue;}
          fd_incref(key); fd_incref(value);
          assoc=fd_init_pair(NULL,key,value);
          _achoice_add(achoice,assoc);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_simplify_choice(results);
}

static int free_buckets
(struct FD_HASH_BUCKET **slots,int slots_to_free)
{
  if ((slots) && (slots_to_free)) {
    struct FD_HASH_BUCKET **scan=slots, **lim=scan+slots_to_free;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+n_entries;
        while (kvscan<kvlimit) {
          fd_decref(kvscan->kv_key);
          fd_decref(kvscan->kv_val);
          kvscan++;}
        *scan++=NULL;
        u8_free(e);}
      else scan++;}
  if (slots) u8_free(slots);
  return 0;
}

FD_EXPORT int fd_free_buckets(struct FD_HASH_BUCKET **slots,int slots_to_free)
{
  return free_buckets(slots,slots_to_free);
}

FD_EXPORT int fd_reset_hashtable(struct FD_HASHTABLE *ht,int n_slots,int lock)
{
  struct FD_HASH_BUCKET **slots; int slots_to_free=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (n_slots<0) n_slots=ht->ht_n_buckets;
  if ((lock) && (ht->table_uselock)) fd_write_lock_table(ht);
  /* Grab the slots and their length. We'll free them after we've reset
     the table and released its lock. */
  slots=ht->ht_buckets; slots_to_free=ht->ht_n_buckets;
  /* Now initialize the structure.  */
  if (n_slots == 0) {
    ht->ht_n_buckets=ht->table_n_keys=0;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets=NULL;}
  else {
    int i=0; struct FD_HASH_BUCKET **slotvec;
    ht->table_n_keys=0;
    ht->ht_n_buckets=n_slots;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets=slotvec=u8_alloc_n(n_slots,struct FD_HASH_BUCKET *);
    while (i < n_slots) slotvec[i++]=NULL;}
  /* Free the lock, letting other processes use this hashtable. */
  if ((lock) && (ht->table_uselock)) fd_unlock_table(ht);
  /* Now, free the old data... */
  free_buckets(slots,slots_to_free);
  return n_slots;
}

/* This resets a hashtable and passes out the internal data to be
   freed separately.  The idea is to hold onto the hashtable's lock
   for as little time as possible. */
FD_EXPORT int fd_fast_reset_hashtable
  (struct FD_HASHTABLE *ht,int n_slots,int lock,
   struct FD_HASH_BUCKET ***slotsptr,int *slots_to_free)
{
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (slotsptr==NULL) return fd_reset_hashtable(ht,n_slots,lock);
  if (n_slots<0) n_slots=ht->ht_n_buckets;
  if ((lock) && (ht->table_uselock)) fd_write_lock_table(ht);
  /* Grab the slots and their length. We'll free them after we've reset
     the table and released its lock. */
  *slotsptr=ht->ht_buckets; *slots_to_free=ht->ht_n_buckets;
  /* Now initialize the structure.  */
  if (n_slots == 0) {
    ht->ht_n_buckets=ht->table_n_keys=0;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets=NULL;}
  else {
    int i=0; struct FD_HASH_BUCKET **slotvec;
    ht->table_n_keys=0;
    ht->ht_n_buckets=n_slots;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets=slotvec=u8_alloc_n(n_slots,struct FD_HASH_BUCKET *);
    while (i < n_slots) slotvec[i++]=NULL;}
  /* Free the lock, letting other processes use this hashtable. */
  if ((lock) && (ht->table_uselock)) fd_unlock_table(ht);
  return n_slots;
}


FD_EXPORT int fd_swap_hashtable(struct FD_HASHTABLE *src,
                                struct FD_HASHTABLE *dest,
                                int n_keys,int locked)
{
#define COPYFIELD(src,dest,field) dest->field=src->field
  int n_slots=fd_get_hashtable_size(n_keys), unlock=0;
  struct FD_HASH_BUCKET **slots;
  FD_CHECK_TYPE_RET(src,fd_hashtable_type);
  if (!(locked)) {
    if (src->table_uselock) {
      u8_write_lock(&(src->table_rwlock));
      unlock=1;}}
  memset(dest,0,sizeof(struct FD_HASHTABLE));
  FD_SET_CONS_TYPE(dest,fd_hashtable_type);
  COPYFIELD(src,dest,table_n_keys);
  COPYFIELD(src,dest,table_load_factor);
  COPYFIELD(src,dest,ht_n_buckets);
  COPYFIELD(src,dest,ht_buckets);
  dest->table_uselock=1;
  u8_init_rwlock(&(dest->table_rwlock));
  src->ht_n_buckets=n_slots; src->table_n_keys=0;
  src->table_modified=0; src->table_readonly=0;
  src->ht_buckets=slots=u8_zalloc_n(n_slots,struct FD_HASH_BUCKET *);
  if (unlock) u8_rw_unlock(&(src->table_rwlock));
#undef COPYFIELD
  return 1;
}

FD_EXPORT int fd_static_hashtable(struct FD_HASHTABLE *ptr,int type)
{
  int n_conversions=0;
  fd_ptr_type keeptype=(fd_ptr_type) type;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  u8_write_lock(&ptr->table_rwlock);
  {
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+n_keyvals;
        while (kvscan<kvlimit) {
          if ((FD_CONSP(kvscan->kv_val)) &&
              ((type<0) || (FD_TYPEP(kvscan->kv_val,keeptype)))) {
            fdtype value=kvscan->kv_val;
            fdtype static_value=fd_static_copy(value);
            if (static_value==value) {
              fd_decref(static_value);
              static_value=fd_register_fcnid(kvscan->kv_val);}
            kvscan->kv_val=static_value;
            fd_decref(kvscan->kv_val);
            n_conversions++;}
          kvscan++;}
        scan++;}
      else scan++;}
  u8_rw_unlock(&ptr->table_rwlock);
  return n_conversions;
}

FD_EXPORT fdtype fd_make_hashtable(struct FD_HASHTABLE *ptr,int n_slots)
{
  if (ptr == NULL) {
    ptr=u8_alloc(struct FD_HASHTABLE);
    FD_INIT_FRESH_CONS(ptr,fd_hashtable_type);
    u8_init_rwlock(&(ptr->table_rwlock));}

  if (n_slots == 0) {

    ptr->ht_n_buckets=ptr->table_n_keys=0;
    ptr->table_readonly=ptr->table_modified=0; ptr->table_uselock=1;
    ptr->table_load_factor=default_hashtable_loading;
    ptr->ht_buckets=NULL;
    return FDTYPE_CONS(ptr);}
  else {
    int i=0; struct FD_HASH_BUCKET **slots;

    if (n_slots < 0) n_slots=-n_slots;
    else n_slots=fd_get_hashtable_size(n_slots);
    ptr->table_readonly=ptr->table_modified=0; ptr->table_uselock=1;
    ptr->ht_n_buckets=n_slots; ptr->table_n_keys=0;
    ptr->table_load_factor=default_hashtable_loading;
    ptr->ht_buckets=slots=u8_zalloc_n(n_slots,struct FD_HASH_BUCKET *);
    while (i < n_slots) slots[i++]=NULL;
    return FDTYPE_CONS(ptr);}
}

/* Note that this does not incref the values passed to it. */
FD_EXPORT fdtype fd_init_hashtable(struct FD_HASHTABLE *ptr,
                                   int init_keys,
                                   struct FD_KEYVAL *inits)
{
  int n_slots=fd_get_hashtable_size(init_keys);
  struct FD_HASH_BUCKET **slots;
  if (ptr == NULL) {
    ptr=u8_alloc(struct FD_HASHTABLE);
    FD_INIT_FRESH_CONS(ptr,fd_hashtable_type);}
  else {FD_SET_CONS_TYPE(ptr,fd_hashtable_type);}
  ptr->ht_n_buckets=n_slots; ptr->table_n_keys=0;
  ptr->table_load_factor=default_hashtable_loading;
  ptr->table_modified=0; ptr->table_readonly=0; ptr->table_uselock=1;
  ptr->ht_buckets=slots=u8_zalloc_n(n_slots,struct FD_HASH_BUCKET *);
  memset(slots,0,sizeof(struct FD_HASH_BUCKET *)*n_slots);
  if (inits) {
    int i=0; while (i<init_keys) {
      struct FD_KEYVAL *ki=&(inits[i]);
      struct FD_KEYVAL *hv=
        fd_hashvec_insert(ki->kv_key,slots,n_slots,&(ptr->table_n_keys));
      hv->kv_val=fd_incref(ki->kv_val);
      i++;}}

  ptr->table_uselock=1;
  u8_init_rwlock(&(ptr->table_rwlock));

  return FDTYPE_CONS(ptr);
}

static int resize_hashtable(struct FD_HASHTABLE *ptr,int n_slots,int need_lock)
{
  int unlock=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if ( (need_lock) && (ptr->table_uselock) ) {
    fd_write_lock_table(ptr);
    unlock=1; }
  {
    struct FD_HASH_BUCKET **new_slots=u8_zalloc_n(n_slots,fd_hash_bucket);
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
    struct FD_HASH_BUCKET **nscan=new_slots, **nlim=nscan+n_slots;
    while (nscan<nlim) *nscan++=NULL;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan++; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          struct FD_KEYVAL *nkv=fd_hashvec_insert
            (kvscan->kv_key,new_slots,n_slots,NULL);
          nkv->kv_val=kvscan->kv_val; kvscan->kv_val=FD_VOID;
          fd_decref(kvscan->kv_key); kvscan++;}
        u8_free(e);}
      else scan++;
    u8_free(ptr->ht_buckets);
    ptr->ht_n_buckets=n_slots; ptr->ht_buckets=new_slots;}
  if (unlock) fd_unlock_table(ptr);
  return n_slots;
}

FD_EXPORT int fd_resize_hashtable(struct FD_HASHTABLE *ptr,int n_slots)
{
  return resize_hashtable(ptr,n_slots,1);
}


/* VOIDs all values which only have one incoming pointer (from the table
   itself).  This is helpful in conjunction with fd_devoid_hashtable, which
   will then reduce the table to remove such entries. */
FD_EXPORT int fd_remove_deadwood(struct FD_HASHTABLE *ptr,
                                 int (*testfn)(fdtype,fdtype,void *),
                                 void *testdata)
{
  struct FD_HASH_BUCKET **scan, **lim;
  int n_slots=ptr->ht_n_buckets, n_keys=ptr->table_n_keys; int unlock=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if ((n_slots == 0) || (n_keys == 0)) return 0;
  if (ptr->table_uselock) { fd_write_lock_table(ptr); unlock=1;}
  while (1) {
    int n_cleared=0;
    scan=ptr->ht_buckets; lim=scan+ptr->ht_n_buckets;
    while (scan < lim) {
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan++; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+fd_n_entries;
        if ((testfn)&&(testfn(kvscan->kv_key,FD_VOID,testdata))) {}
        else while (kvscan<kvlimit) {
            fdtype val=kvscan->kv_val;
            if (FD_CONSP(val)) {
              struct FD_CONS *cval=(struct FD_CONS *)val;
              if (FD_ACHOICEP(val))
                cval=(struct FD_CONS *)
                  (val=kvscan->kv_val=fd_simplify_choice(val));
              if (FD_CONS_REFCOUNT(cval)==1) {
                n_cleared++; kvscan->kv_val=FD_VOID; fd_decref(val);}}
            /* ??? In the future, this should probably scan CHOICES
               to remove deadwood as well.  */
            kvscan++;}}
      else scan++;}
    if (n_cleared) n_cleared=0; else break;}
  if (unlock) fd_unlock_table(ptr);
  return n_slots;
}

FD_EXPORT int fd_devoid_hashtable(struct FD_HASHTABLE *ptr,int locked)
{
  int n_slots=ptr->ht_n_buckets, n_keys=ptr->table_n_keys; int unlock=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if ((n_slots == 0) || (n_keys == 0)) return 0;
  if ((locked<0)?(ptr->table_uselock):(!(locked))) { 
    fd_write_lock_table(ptr); 
    unlock=1;}
  {
    struct FD_HASH_BUCKET **new_slots=u8_zalloc_n(n_slots,fd_hash_bucket);
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
    struct FD_HASH_BUCKET **nscan=new_slots, **nlim=nscan+n_slots;
    int remaining_keys=0;
    if (new_slots==NULL) {
      if (unlock) fd_unlock_table(ptr);
      return -1;}
    while (nscan<nlim) *nscan++=NULL;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan++; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit)
          if (FD_VOIDP(kvscan->kv_val)) {
            fd_decref(kvscan->kv_key); kvscan++;}
          else {
            struct FD_KEYVAL *nkv=fd_hashvec_insert
              (kvscan->kv_key,new_slots,n_slots,NULL);
            nkv->kv_val=kvscan->kv_val; kvscan->kv_val=FD_VOID;
            fd_decref(kvscan->kv_key); 
            remaining_keys++;
            kvscan++;}
        u8_free(e);}
      else scan++;
    u8_free(ptr->ht_buckets);
    ptr->ht_n_buckets=n_slots; ptr->ht_buckets=new_slots;
    ptr->table_n_keys=remaining_keys;}
  if (unlock) fd_unlock_table(ptr);
  return n_slots;
}

FD_EXPORT int fd_hashtable_stats
  (struct FD_HASHTABLE *ptr,
   int *n_slotsp,int *n_keysp,int *n_bucketsp,int *n_collisionsp,
   int *max_bucketp,int *n_valsp,int *max_valsp)
{
  int n_slots=ptr->ht_n_buckets, n_keys=0, unlock=0;
  int n_buckets=0, max_bucket=0, n_collisions=0;
  int n_vals=0, max_vals=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if (ptr->table_uselock) { fd_read_lock_table(ptr); unlock=1;}
  {
    struct FD_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        n_buckets++; n_keys=n_keys+fd_n_entries;
        if (fd_n_entries>max_bucket) max_bucket=fd_n_entries;
        if (fd_n_entries>1)
          n_collisions++;
        if ((n_valsp) || (max_valsp)) {
          struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+fd_n_entries;
          while (kvscan<kvlimit) {
            fdtype val=kvscan->kv_val; int valcount;
            if (FD_CHOICEP(val)) valcount=FD_CHOICE_SIZE(val);
            else if (FD_ACHOICEP(val)) valcount=FD_ACHOICE_SIZE(val);
            else valcount=1;
            n_vals=n_vals+valcount;
            if (valcount>max_vals) max_vals=valcount;
            kvscan++;}}
        scan++;}
      else scan++;
  }
  if (n_slotsp) *n_slotsp=n_slots;
  if (n_keysp) *n_keysp=n_keys;
  if (n_bucketsp) *n_bucketsp=n_buckets;
  if (n_collisionsp) *n_collisionsp=n_collisions;
  if (max_bucketp) *max_bucketp=max_bucket;
  if (n_valsp) *n_valsp=n_vals;
  if (max_valsp) *max_valsp=max_vals;
  if (unlock) fd_unlock_table(ptr);
  return n_keys;
}

FD_EXPORT fdtype fd_copy_hashtable(FD_HASHTABLE *nptr,FD_HASHTABLE *ptr)
{
  int n_slots=0, unlock=0;
  struct FD_HASH_BUCKET **slots, **read, **write, **read_limit;
  if (nptr==NULL) nptr=u8_alloc(struct FD_HASHTABLE);
  if (ptr->table_uselock) { fd_read_lock_table(ptr); unlock=1;}
  FD_INIT_CONS(nptr,fd_hashtable_type);
  nptr->table_modified=0; nptr->table_readonly=0; nptr->table_uselock=1;
  nptr->ht_n_buckets=n_slots=ptr->ht_n_buckets;;
  nptr->table_n_keys=ptr->table_n_keys;
  read=slots=ptr->ht_buckets;
  read_limit=read+n_slots;
  nptr->table_load_factor=ptr->table_load_factor;
  write=nptr->ht_buckets=u8_alloc_n(n_slots,fd_hash_bucket);
  memset(write,0,sizeof(struct FD_HASH_BUCKET *)*n_slots);
  while (read<read_limit)
    if (*read==NULL) {read++; write++;}
    else {
      struct FD_KEYVAL *kvread, *kvwrite, *kvlimit;
      struct FD_HASH_BUCKET *he=*read++, *newhe; int n=he->fd_n_entries;
      *write++=newhe=(struct FD_HASH_BUCKET *)
        u8_mallocz(sizeof(struct FD_HASH_BUCKET)+(n-1)*sizeof(struct FD_KEYVAL));
      kvread=&(he->kv_val0); kvwrite=&(newhe->kv_val0);
      newhe->fd_n_entries=n; kvlimit=kvread+n;
      while (kvread<kvlimit) {
        fdtype key=kvread->kv_key, val=kvread->kv_val; kvread++;
        if (FD_CONSP(key)) kvwrite->kv_key=fd_copy(key);
        else kvwrite->kv_key=key;
        if (FD_CONSP(val))
          if (FD_ACHOICEP(val))
            kvwrite->kv_val=fd_make_simple_choice(val);
          else if (FD_STATICP(val))
            kvwrite->kv_val=fd_copy(val);
          else {fd_incref(val); kvwrite->kv_val=val;}
        else kvwrite->kv_val=val;
        kvwrite++;}}
  if (unlock) fd_unlock_table(ptr);

  u8_init_rwlock(&(nptr->table_rwlock));

  return FDTYPE_CONS(nptr);
}

static fdtype copy_hashtable(fdtype table,int deep)
{
  struct FD_HASHTABLE *ptr=fd_consptr(fd_hashtable,table,fd_hashtable_type);
  struct FD_HASHTABLE *nptr=u8_alloc(struct FD_HASHTABLE);
  return fd_copy_hashtable(nptr,ptr);
}

static fdtype copy_hashset(fdtype table,int deep)
{
  struct FD_HASHSET *ptr=fd_consptr(fd_hashset,table,fd_hashset_type);
  struct FD_HASHSET *nptr=u8_alloc(struct FD_HASHSET);
  return fd_copy_hashset(nptr,ptr);
}

static int unparse_hashtable(u8_output out,fdtype x)
{
  struct FD_HASHTABLE *ht=FD_XHASHTABLE(x); char buf[128];
  sprintf(buf,"#<HASHTABLE %d/%d>",ht->table_n_keys,ht->ht_n_buckets);
  u8_puts(out,buf);
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
      fd_seterr("Can't lock modifiable which isn't currently locking",
                "fd_lock_hashtable",NULL,FD_VOID);
      return -1;}
    fd_read_lock_table(ht);
    ht->table_readonly=1; ht->table_uselock=0;
    return 1;}
  else {
    if (ht->table_uselock) return 0;
    ht->table_uselock=1; ht->table_readonly=0;
    fd_unlock_table(ht);
    return 1;}
}

FD_EXPORT int fd_recycle_hashtable(struct FD_HASHTABLE *c)
{
  struct FD_HASHTABLE *ht=(struct FD_HASHTABLE *)c;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  fd_write_lock_table(ht);
  if (ht->ht_n_buckets==0) {
    fd_unlock_table(ht);
    u8_destroy_rwlock(&(ht->table_rwlock));
    if (!(FD_STATIC_CONSP(ht))) u8_free(ht);
    return 0;}
  if (ht->ht_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->ht_buckets, **lim=scan+ht->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          fd_decref(kvscan->kv_key);
          fd_decref(kvscan->kv_val);
          kvscan++;}
        u8_free(*scan);
        *scan++=NULL;}
      else scan++;
    u8_free(ht->ht_buckets);}
  ht->ht_buckets=NULL; ht->ht_n_buckets=0; ht->table_n_keys=0;
  fd_unlock_table(ht);
  u8_destroy_rwlock(&(ht->table_rwlock));
  if (!(FD_STATIC_CONSP(ht))) u8_free(ht);
  return 0;
}

FD_EXPORT struct FD_KEYVAL *fd_hashtable_keyvals
   (struct FD_HASHTABLE *ht,int *sizep,int lock)
{
  struct FD_KEYVAL *results, *rscan; int unlock=0;
  if ((FD_CONS_TYPE(ht)) != fd_hashtable_type) {
    fd_seterr(fd_TypeError,"hashtable",NULL,(fdtype)ht);
    return NULL;}
  if (ht->table_n_keys == 0) {*sizep=0; return NULL;}
  if ((lock)&&(ht->table_uselock)) {
      fd_read_lock_table(ht);
      unlock=1;}
  if (ht->ht_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->ht_buckets, **lim=scan+ht->ht_n_buckets;
    rscan=results=u8_alloc_n(ht->table_n_keys,struct FD_KEYVAL);
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          rscan->kv_key=fd_incref(kvscan->kv_key);
          rscan->kv_val=fd_incref(kvscan->kv_val);
          rscan++; kvscan++;}
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
    struct FD_HASH_BUCKET **scan=ht->ht_buckets, **lim=scan+ht->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; 
        int n_entries=e->fd_n_entries;
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
  if ((lock)&&(ht->table_uselock)) {fd_write_lock_table(ht); unlock=1;}
  if (ht->ht_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->ht_buckets, **lim=scan+ht->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int n_keyvals=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->kv_val0), *kvlimit=kvscan+n_keyvals;
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
  fdtype *slots;
  int i=0, n_slots=fd_get_hashtable_size(size);
  if (stack_cons) {
    FD_INIT_STATIC_CONS(hashset,fd_hashset_type);}
  else {FD_INIT_CONS(hashset,fd_hashset_type);}
  hashset->hs_n_slots=n_slots; hashset->hs_n_elts=0; hashset->hs_modified=0;
  hashset->hs_allatomic=1; hashset->hs_load_factor=default_hashset_loading;
  hashset->hs_slots=slots=u8_alloc_n(n_slots,fdtype);
  while (i < n_slots) slots[i++]=0;
  u8_init_rwlock(&(hashset->table_rwlock));
  return;
}

FD_EXPORT fdtype fd_make_hashset()
{
  struct FD_HASHSET *h=u8_alloc(struct FD_HASHSET);
  fd_init_hashset(h,fd_init_hash_size,FD_MALLOCD_CONS);
  FD_INIT_CONS(h,fd_hashset_type);
  return FDTYPE_CONS(h);
}

static int hashset_get_slot(fdtype key,const fdtype *slots,int n)
{
  int hash=fd_hash_lisp(key), probe=hash%n, n_probes=0;
  while (n_probes<512)
    if (slots[probe]==0) return probe;
    else if (FDTYPE_EQUAL(key,slots[probe]))
      return probe;
    else {
      probe++; n_probes++;
      if (probe >= n) probe=0;}
  if (n_probes>512) return -1;
  else return probe;
}

FD_EXPORT int fd_hashset_get(struct FD_HASHSET *h,fdtype key)
{
  int probe, exists; fdtype *slots;
  if (h->hs_n_elts==0) return 0;
  fd_read_lock_table(h);
  slots=h->hs_slots;
  probe=hashset_get_slot(key,(const fdtype *)slots,h->hs_n_slots);
  if (probe>=0) {
    if (slots[probe]) exists=1; else exists=0;}
  fd_unlock_table(h);
  if (probe<0) {
    fd_seterr(HashsetOverflow,"fd_hashset_get",NULL,(fdtype)h);
    return -1;}
  else return exists;
}

FD_EXPORT fdtype fd_hashset_elts(struct FD_HASHSET *h,int clean)
{
  FD_CHECK_TYPE_RETDTYPE(h,fd_hashset_type);
  if (h->hs_n_elts==0) return FD_EMPTY_CHOICE;
  else {
    fd_read_lock_table(h);
    if (h->hs_n_elts==1) {
      fdtype *scan=h->hs_slots, *limit=scan+h->hs_n_slots;
      while (scan<limit)
        if (*scan)
          if (clean) {
            fdtype v=*scan;
            u8_free(h->hs_slots);
            fd_unlock_table(h);
            if (FD_VOIDP(v))
              return FD_EMPTY_CHOICE;
            else return v;}
          else {
            fdtype v=fd_incref(*scan);
            fd_unlock_table(h);
            if (FD_VOIDP(v))
              return FD_EMPTY_CHOICE;
            else return v;}
        else scan++;
      fd_unlock_table(h);
      return FD_VOID;}
    else {
      int n=h->hs_n_elts, atomicp=1;
      struct FD_CHOICE *new_choice=fd_alloc_choice(n);
      const fdtype *scan=h->hs_slots, *limit=scan+h->hs_n_slots;
      fdtype *base=(fdtype *)(FD_XCHOICE_DATA(new_choice));
      fdtype *write=base, *writelim=base+n;
      if (clean)
        while ((scan<limit) && (write<writelim))
          if (*scan) {
            fdtype v=*scan++;
            if (FD_CONSP(v)) atomicp=0;
            if (!(FD_VOIDP(v))) *write++=v;}
          else scan++;
      else while ((scan<limit) && (write<writelim)) {
        fdtype v=*scan++;
        if ((v) && (!(FD_VOIDP(v)))) {
          if (atomicp) {
            if (FD_CONSP(v)) {atomicp=0; fd_incref(v);}}
          else fd_incref(v);
          *write++=v;}}
      if (clean) {
        if (FD_MALLOCD_CONSP(h)) fd_decref((fdtype)h);
        else {
          u8_free(h->hs_slots); h->hs_n_slots=h->hs_n_elts=0;}}
      fd_unlock_table(h);
      return fd_init_choice(new_choice,write-base,base,
                            (FD_CHOICE_DOSORT|
                             ((atomicp)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES))|
                             FD_CHOICE_REALLOC));}}
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

static fdtype hashsetelts(struct FD_HASHSET *h)
{
  return fd_hashset_elts(h,0);
}

static int grow_hashset(struct FD_HASHSET *h)
{
  int i=0, lim=h->hs_n_slots;
  int new_size=fd_get_hashtable_size(hashset_resize_target(h));
  const fdtype *slots=h->hs_slots;
  fdtype *newslots=u8_zalloc_n(new_size,fdtype);
  while (i<new_size) newslots[i++]=FD_NULL;
  i=0; while (i < lim) {
    if (slots[i]==0) i++;
    else if (FD_VOIDP(slots[i])) i++;
    else {
      int off=hashset_get_slot(slots[i],newslots,new_size);
      if (off<0) {
        u8_free(newslots);
        return -1;}
      newslots[off]=slots[i]; i++;}}
  u8_free(h->hs_slots); h->hs_slots=newslots; h->hs_n_slots=new_size;
  return 1;
}

FD_EXPORT ssize_t fd_grow_hashset(struct FD_HASHSET *h,size_t target)
{
  int new_size=fd_get_hashtable_size(target);
  fd_write_lock_table(h); {
    int i=0, lim=h->hs_n_slots;
    const fdtype *slots=h->hs_slots;
    fdtype *newslots=u8_zalloc_n(new_size,fdtype);
    while (i<new_size) newslots[i++]=FD_NULL;
    i=0; while (i < lim)
           if (slots[i]==0) i++;
           else if (FD_VOIDP(slots[i])) i++;
           else {
             int off=hashset_get_slot(slots[i],newslots,new_size);
             if (off<0) {
               u8_free(newslots);
               fd_unlock_table(h);
               return -1;}
             newslots[off]=slots[i]; i++;}
    u8_free(h->hs_slots);
    h->hs_slots=newslots;
    h->hs_n_slots=new_size;
    fd_unlock_table(h);}
  return new_size;
}

FD_EXPORT int fd_hashset_mod(struct FD_HASHSET *h,fdtype key,int add)
{
  int probe; fdtype *slots;
  fd_write_lock_table(h);
  slots=h->hs_slots;
  probe=hashset_get_slot(key,h->hs_slots,h->hs_n_slots);
  if (probe < 0) {
    fd_unlock_table(h);
    fd_seterr(HashsetOverflow,"fd_hashset_mod",NULL,(fdtype)h);
    return -1;}
  else if (FD_NULLP(slots[probe]))
    if (add) {
      slots[probe]=fd_incref(key); 
      h->hs_n_elts++; h->hs_modified=1;
      if (FD_CONSP(key)) h->hs_allatomic=0;
      if (hashset_needs_resizep(h))
        grow_hashset(h);
      fd_unlock_table(h);
      return 1;}
    else {
      fd_unlock_table(h);
      return 0;}
  else if (add) {
    fd_unlock_table(h);
    return 0;}
  else {
    fd_decref(slots[probe]); slots[probe]=FD_VOID;
    h->hs_n_elts++; h->hs_modified=1;
    fd_unlock_table(h);
    return 1;}
}

/* This adds without locking or incref. */
FD_EXPORT int fd_hashset_add_raw(struct FD_HASHSET *h,fdtype key)
{
  int probe; fdtype *slots;
  slots=h->hs_slots;
  probe=hashset_get_slot(key,h->hs_slots,h->hs_n_slots);
  if (probe < 0) {
    fd_seterr(HashsetOverflow,"fd_hashset_mod",NULL,(fdtype)h);
    return -1;}
  else if (FD_NULLP(slots[probe])) {
    slots[probe]=key; h->hs_n_elts++; h->hs_modified=1;
    if (FD_CONSP(key)) h->hs_allatomic=0;
    if (FD_EXPECT_FALSE(hashset_needs_resizep(h)))
      grow_hashset(h);
    return 1;}
  else return 0;
}

/* This adds without locking or incref. */
FD_EXPORT int fd_hashset_add(struct FD_HASHSET *h,fdtype keys)
{
  if ((FD_CHOICEP(keys))||(FD_ACHOICEP(keys))) {
    int n_vals=FD_CHOICE_SIZE(keys);
    size_t need_size=n_vals*3+h->hs_n_elts, n_adds=0;
    if (need_size>h->hs_n_slots) fd_grow_hashset(h,need_size);
    fd_write_lock_table(h); {
      fdtype *slots=h->hs_slots; int n_slots=h->hs_n_slots;
      {FD_DO_CHOICES(key,keys) {
          int probe=hashset_get_slot(key,slots,n_slots);
          if (probe < 0) {
            fd_seterr(HashsetOverflow,"fd_hashset_add",NULL,(fdtype)h);
            FD_STOP_DO_CHOICES;
            fd_unlock_table(h);
            return -1;}
          else if ((FD_NULLP(slots[probe]))||(FD_VOIDP(slots[probe]))) {
            slots[probe]=key; fd_incref(key);
            h->hs_modified=1; h->hs_n_elts++; n_adds++;
            if (FD_CONSP(key)) h->hs_allatomic=0;
            if (FD_EXPECT_FALSE(hashset_needs_resizep(h))) {
              grow_hashset(h);
              slots=h->hs_slots;
              n_slots=h->hs_n_slots;}}
          else {}}}
      fd_unlock_table(h);
      return n_adds;}}
  else if (FD_EMPTY_CHOICEP(keys)) return 0;
  else return fd_hashset_mod(h,keys,1);
}

FD_EXPORT int fd_recycle_hashset(struct FD_HASHSET *h)
{
  fd_write_lock_table(h);
  if (h->hs_allatomic==0) {
    fdtype *scan=h->hs_slots, *lim=scan+h->hs_n_slots;
    while (scan<lim)
      if (FD_NULLP(*scan)) scan++;
      else {
        fdtype v=*scan++; fd_decref(v);}}
  u8_free(h->hs_slots);
  fd_unlock_table(h);
  u8_destroy_rwlock(&(h->table_rwlock));
  if (!(FD_STATIC_CONSP(h))) u8_free(h);
  return 1;
}

FD_EXPORT fdtype fd_copy_hashset(struct FD_HASHSET *hnew,struct FD_HASHSET *h)
{

  fdtype *newslots, *write, *read, *lim;
  if (hnew==NULL) hnew=u8_alloc(struct FD_HASHSET);
  fd_read_lock_table(h);
  read=h->hs_slots; lim=read+h->hs_n_slots;
  write=newslots=u8_alloc_n(h->hs_n_slots,fdtype);
  if (h->hs_allatomic)
    while (read<lim) *write++=*read++;
  else while (read<lim) {
      fdtype v=*read++; if (v) fd_incref(v); *write++=v;}
  FD_INIT_CONS(hnew,fd_hashset_type);
  hnew->hs_n_slots=h->hs_n_slots; hnew->hs_n_elts=h->hs_n_elts;
  hnew->hs_slots=newslots; hnew->hs_allatomic=h->hs_allatomic;
  hnew->hs_load_factor=h->hs_load_factor; hnew->hs_modified=0;
  fd_unlock_table(h);
  u8_init_rwlock((&hnew->table_rwlock));
  return (fdtype) hnew;
}

static int unparse_hashset(u8_output out,fdtype x)
{
  struct FD_HASHSET *hs=((struct FD_HASHSET *)x); char buf[128];
  sprintf(buf,"#<HASHSET%s %d/%d>",
          ((hs->hs_modified)?("(m)"):("")),
          hs->hs_n_elts,hs->hs_n_slots);
  u8_puts(out,buf);
  return 1;
}

static fdtype hashsetget(fdtype x,fdtype key)
{
  struct FD_HASHSET *h=fd_consptr(struct FD_HASHSET *,x,fd_hashset_type);
  if (fd_hashset_get(h,key)) return FD_TRUE;
  else return FD_FALSE;
}
static int hashsetstore(fdtype x,fdtype key,fdtype val)
{
  struct FD_HASHSET *h=fd_consptr(struct FD_HASHSET *,x,fd_hashset_type);
  if (FD_TRUEP(val)) return fd_hashset_mod(h,key,1);
  else if (FD_FALSEP(val)) return fd_hashset_mod(h,key,0);
  else {
    fd_seterr(fd_RangeError,_("value is not a boolean"),NULL,val);
    return -1;}
}

/* Generic table functions */

struct FD_TABLEFNS *fd_tablefns[FD_TYPE_MAX];

#define CHECKPTR(arg,cxt)             \
  if (FD_EXPECT_FALSE((!(FD_CHECK_PTR(arg))))) \
    _fd_bad_pointer(arg,cxt); else {}

#define NOT_TABLEP(arg,argtype,cxt)                     \
  (FD_EXPECT_FALSE((fd_tablefns[argtype]==NULL))) ?     \
  (fd_seterr(NotATable,cxt,NULL,arg), 1 ) :             \
  (0)

#define BAD_TABLEP(arg,type,meth,cxt)                   \
  (FD_EXPECT_FALSE((fd_tablefns[type]==NULL))) ?        \
  (fd_seterr(NotATable,cxt,NULL,arg), 1 ) :             \
  (FD_EXPECT_FALSE(fd_tablefns[type]->meth==NULL)) ?    \
  (fd_seterr(fd_NoMethod,cxt,NULL,arg), 1 ) :           \
  (0)

FD_EXPORT fdtype fd_get(fdtype arg,fdtype key,fdtype dflt)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_get/table");
  CHECKPTR(key,"fd_get/key");
  CHECKPTR(key,"fd_get/dflt");
  if ((FD_EMPTY_CHOICEP(arg))||(FD_EMPTY_CHOICEP(key)))
    return FD_EMPTY_CHOICE;
  else if (FD_UNAMBIGP(key)) {
    if (BAD_TABLEP(arg,argtype,get,"fd_get"))
      return FD_ERROR_VALUE;
    else return (fd_tablefns[argtype]->get)(arg,key,dflt);}
  else if (BAD_TABLEP(arg,argtype,get,"fd_get"))
    return FD_ERROR_VALUE;
  else {
    fdtype results=FD_EMPTY_CHOICE;
    fdtype (*getfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->get;
    FD_DO_CHOICES(each,key) {
      fdtype values=getfn(arg,each,FD_EMPTY_CHOICE);
      if (FD_ABORTP(values)) {
        fd_decref(results); return values;}
      FD_ADD_TO_CHOICE(results,values);}
    if (FD_EMPTY_CHOICEP(results)) return fd_incref(dflt);
    else return results;}
}

FD_EXPORT int fd_store(fdtype arg,fdtype key,fdtype value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_store/table");
  CHECKPTR(key,"fd_store/key");
  CHECKPTR(value,"fd_store/value");
  if ((FD_EMPTY_CHOICEP(arg))||(FD_EMPTY_CHOICEP(key)))
    return 0;
  else if (FD_UNAMBIGP(key)) {
    if (BAD_TABLEP(arg,argtype,store,"fd_store"))
      return -1;
    else return (fd_tablefns[argtype]->store)(arg,key,value);}
  else if (BAD_TABLEP(arg,argtype,store,"fd_store"))
    return -1;
  else {
    int (*storefn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->store;
    FD_DO_CHOICES(each,key) {
      int retval=storefn(arg,each,value);
      if (retval<0) return retval;}
    return 1;}
}

FD_EXPORT int fd_add(fdtype arg,fdtype key,fdtype value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_add/table");
  CHECKPTR(key,"fd_add/key");
  CHECKPTR(value,"fd_add/value");
  if (FD_EXPECT_FALSE((FD_EMPTY_CHOICEP(arg))||(FD_EMPTY_CHOICEP(key))))
    return 0;
  else if (FD_UNAMBIGP(key)) {
    if (NOT_TABLEP(arg,argtype,"fd_add"))
      return -1;
    else if (fd_tablefns[argtype]->add)
      return (fd_tablefns[argtype]->add)(arg,key,value);
    else if ((fd_tablefns[argtype]->store) &&
             (fd_tablefns[argtype]->get)) {
      int (*storefn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->store;
      fdtype (*getfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->get;
      FD_DO_CHOICES(each,key) {
        int store_rv=0;
        fdtype values=getfn(arg,each,FD_EMPTY_CHOICE);
        fdtype to_store;
        if (FD_ABORTP(values)) {
          FD_STOP_DO_CHOICES;
          return -1;}
        fd_incref(value);
        FD_ADD_TO_CHOICE(values,value);
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
    int (*addfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->add;
    FD_DO_CHOICES(each,key) {
      int retval=addfn(arg,each,value);
      if (retval<0) return retval;}
    return 1;}
}

FD_EXPORT int fd_drop(fdtype arg,fdtype key,fdtype value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_drop/table");
  CHECKPTR(key,"fd_drop/key");
  CHECKPTR(value,"fd_drop/value");
  if (FD_EMPTY_CHOICEP(arg)) return 0;
  if (FD_VALID_TYPECODEP(argtype))
    if (FD_EXPECT_TRUE(fd_tablefns[argtype]!=NULL))
      if (FD_EXPECT_TRUE(fd_tablefns[argtype]->drop!=NULL))
        if (FD_EXPECT_FALSE((FD_EMPTY_CHOICEP(value)) ||
                            (FD_EMPTY_CHOICEP(key))))
          return 0;
        else if (FD_CHOICEP(key)) {
          int (*dropfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->drop;
          FD_DO_CHOICES(each,key) {
            int retval=dropfn(arg,each,value);
            if (retval<0) return retval;}
          return 1;}
        else return (fd_tablefns[argtype]->drop)(arg,key,value);
      else if ((fd_tablefns[argtype]->store) &&
               (fd_tablefns[argtype]->get))
        if (FD_EXPECT_FALSE((FD_EMPTY_CHOICEP(value))||(FD_EMPTY_CHOICEP(key))))
          return 0;
        else if (FD_VOIDP(value)) {
          int retval;
          int (*storefn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->store;
          FD_DO_CHOICES(each,key) {
            retval=storefn(arg,each,FD_EMPTY_CHOICE);
            if (retval<0) return retval;}
          return 1;}
        else {
          int (*storefn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->store;
          fdtype (*getfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->get;
          FD_DO_CHOICES(each,key) {
            fdtype values=getfn(arg,each,FD_EMPTY_CHOICE);
            fdtype nvalues;
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

FD_EXPORT int fd_test(fdtype arg,fdtype key,fdtype value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_test/table"); CHECKPTR(key,"fd_test/key"); CHECKPTR(value,"fd_test/value");
  if (FD_EXPECT_FALSE((FD_EMPTY_CHOICEP(arg))||(FD_EMPTY_CHOICEP(key))))
    return 0;
  if ((FD_EMPTY_CHOICEP(arg))||(FD_EMPTY_CHOICEP(key)))
    return 0;
  else if (NOT_TABLEP(arg,argtype,"fd_test"))
    return -1;
  else if (FD_EXPECT_TRUE(fd_tablefns[argtype]->test!=NULL))
    if (FD_CHOICEP(key)) {
      int (*testfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->test;
      FD_DO_CHOICES(each,key)
        if (testfn(arg,each,value)) return 1;
      return 0;}
    else return (fd_tablefns[argtype]->test)(arg,key,value);
  else if (fd_tablefns[argtype]->get) {
    fdtype (*getfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->get;
    FD_DO_CHOICES(each,key) {
      fdtype values=getfn(arg,key,FD_EMPTY_CHOICE);
      if (FD_VOIDP(value))
        if (FD_EMPTY_CHOICEP(values))
          return 0;
        else {fd_decref(values); return 1;}
      else if (FD_EMPTY_CHOICEP(values)) {}
      else if (FD_EQ(value,values)) {
        fd_decref(values); return 1;}
      else if (fd_overlapp(value,values)) {
        fd_decref(values); return 1;}
      else fd_decref(values);}
    return 0;}
  else return fd_reterr(fd_NoMethod,CantTest,NULL,arg);
}

FD_EXPORT int fd_getsize(fdtype arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_getsize/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->getsize)
      return (fd_tablefns[argtype]->getsize)(arg);
    else if (fd_tablefns[argtype]->keys) {
      fdtype values=(fd_tablefns[argtype]->keys)(arg);
      if (FD_ABORTP(values))
        return fd_interr(values);
      else {
        int size=FD_CHOICE_SIZE(values);
        fd_decref(values);
        return size;}}
    else return fd_err(fd_NoMethod,CantGetKeys,NULL,arg);
  else return fd_err(NotATable,"fd_getkeys",NULL,arg);
}

FD_EXPORT int fd_modifiedp(fdtype arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_modifiedp/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->modified)
      return (fd_tablefns[argtype]->modified)(arg,-1);
    else return fd_err(fd_NoMethod,CantCheckModified,NULL,arg);
  else return fd_err(NotATable,"fd_modifiedp",NULL,arg);
}

FD_EXPORT int fd_set_modified(fdtype arg,int flag)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_set_modified/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->modified)
      return (fd_tablefns[argtype]->modified)(arg,flag);
    else return fd_err(fd_NoMethod,CantSetModified,NULL,arg);
  else return fd_err(NotATable,"fd_set_modified",NULL,arg);
}

FD_EXPORT int fd_readonlyp(fdtype arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_readonlyp/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->readonly)
      return (fd_tablefns[argtype]->readonly)(arg,-1);
    else return fd_err(fd_NoMethod,CantCheckModified,NULL,arg);
  else return fd_err(NotATable,"fd_readonlyp",NULL,arg);
}

FD_EXPORT int fd_set_readonly(fdtype arg,int flag)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_set_readonly/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->modified)
      return (fd_tablefns[argtype]->modified)(arg,flag);
    else return fd_err(fd_NoMethod,CantSetModified,NULL,arg);
  else return fd_err(NotATable,"fd_set_readonly",NULL,arg);
}

FD_EXPORT fdtype fd_getkeys(fdtype arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  CHECKPTR(arg,"fd_getkeys/table");
  if (fd_tablefns[argtype])
    if (fd_tablefns[argtype]->keys)
      return (fd_tablefns[argtype]->keys)(arg);
    else return fd_err(fd_NoMethod,CantGetKeys,NULL,arg);
  else return fd_err(NotATable,"fd_getkeys",NULL,arg);
}

FD_EXPORT fdtype fd_getvalues(fdtype arg)
{
  CHECKPTR(arg,"fd_getvalues/table");
  /* Eventually, these might be fd_tablefns fields */
  if (FD_TYPEP(arg,fd_hashtable_type)) 
    return fd_hashtable_values(FD_XHASHTABLE(arg));
  else if (FD_TYPEP(arg,fd_slotmap_type))
    return fd_slotmap_values(FD_XSLOTMAP(arg));
  else if (FD_CHOICEP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(table,arg) {
      FD_ADD_TO_CHOICE(results,fd_getvalues(table));}
    return results;}
  else if (FD_PAIRP(arg))
    return fd_refcdr(arg);
  else if (!(FD_TABLEP(arg)))
    return fd_err(NotATable,"fd_getvalues",NULL,arg);
  else {
    fdtype results=FD_EMPTY_CHOICE, keys=fd_getkeys(arg);
    FD_DO_CHOICES(key,keys) {
      fdtype values=fd_get(arg,key,FD_VOID);
      if (!((FD_VOIDP(values))||(FD_EMPTY_CHOICEP(values)))) {
        FD_ADD_TO_CHOICE(results,values);}}
    fd_decref(keys);
    return results;}
}

FD_EXPORT fdtype fd_getassocs(fdtype arg)
{
  CHECKPTR(arg,"fd_getassocs/table");
  /* Eventually, these might be fd_tablefns fields */
  if (FD_TYPEP(arg,fd_hashtable_type)) 
    return fd_hashtable_assocs(FD_XHASHTABLE(arg));
  else if (FD_TYPEP(arg,fd_slotmap_type))
    return fd_slotmap_assocs(FD_XSLOTMAP(arg));
  else if (FD_CHOICEP(arg)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(table,arg) {
      FD_ADD_TO_CHOICE(results,fd_getassocs(table));}
    return results;}
  else if (FD_PAIRP(arg))
    return fd_incref(arg);
  else if (!(FD_TABLEP(arg)))
    return fd_err(NotATable,"fd_getvalues",NULL,arg);
  else {
    fdtype results=FD_EMPTY_CHOICE, keys=fd_getkeys(arg);
    FD_DO_CHOICES(key,keys) {
      fdtype values=fd_get(arg,key,FD_VOID);
      if (!(FD_VOIDP(values))) {
        fdtype assoc=fd_init_pair(NULL,key,values);
        fd_incref(key); fd_incref(values);
        FD_ADD_TO_CHOICE(results,assoc);}}
    fd_decref(keys);
    return results;}
}

/* Operations over tables */

struct FD_HASHMAX {fdtype max, scope, keys;};

static int hashmaxfn(fdtype key,fdtype value,void *hmaxp)
{
  struct FD_HASHMAX *hashmax=hmaxp;
  if ((FD_VOIDP(hashmax->scope)) || (fd_choice_containsp(key,hashmax->scope)))
    if (FD_EMPTY_CHOICEP(value)) {}
    else if (FD_NUMBERP(value))
      if (FD_VOIDP(hashmax->max)) {
        hashmax->max=fd_incref(value); hashmax->keys=fd_incref(key);}
      else {
        int cmp=numcompare(value,hashmax->max);
        if (cmp>0) {
          fd_decref(hashmax->keys); fd_decref(hashmax->max);
          hashmax->keys=fd_incref(key); hashmax->max=fd_incref(value);}
        else if (cmp==0) {
          fd_incref(key);
          FD_ADD_TO_CHOICE(hashmax->keys,key);}}
    else {}
  else {}
  return 0;
}

FD_EXPORT
fdtype fd_hashtable_max(struct FD_HASHTABLE *h,fdtype scope,fdtype *maxvalp)
{
  if (FD_EMPTY_CHOICEP(scope)) return FD_EMPTY_CHOICE;
  else {
    struct FD_HASHMAX hmax;
    hmax.keys=FD_EMPTY_CHOICE; hmax.scope=scope; hmax.max=FD_VOID;
    fd_for_hashtable(h,hashmaxfn,&hmax,1);
    if ((maxvalp) && (FD_NUMBERP(hmax.max)))
      *maxvalp=hmax.max;
    else fd_decref(hmax.max);
    return hmax.keys;}
}

static int hashskimfn(fdtype key,fdtype value,void *hmaxp)
{
  struct FD_HASHMAX *hashmax=hmaxp;
  if ((FD_VOIDP(hashmax->scope)) || (fd_choice_containsp(key,hashmax->scope)))
    if (FD_NUMBERP(value)) {
      int cmp=numcompare(value,hashmax->max);
      if (cmp>=0) {
        fd_incref(key);
        FD_ADD_TO_CHOICE(hashmax->keys,key);}}
  return 0;
}

FD_EXPORT
fdtype fd_hashtable_skim(struct FD_HASHTABLE *h,fdtype maxval,fdtype scope)
{
  if (FD_EMPTY_CHOICEP(scope)) return FD_EMPTY_CHOICE;
  else {
    struct FD_HASHMAX hmax;
    hmax.keys=FD_EMPTY_CHOICE; hmax.scope=scope; hmax.max=maxval;
    fd_for_hashtable(h,hashskimfn,&hmax,1);
    return hmax.keys;}
}

static int hashcountfn(fdtype key,fdtype value,void *vcountp)
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

static fdtype pairget(fdtype pair,fdtype key,fdtype dflt)
{
  if (FD_EQUAL(FD_CAR(pair),key)) return fd_incref(FD_CDR(pair));
  else return FD_EMPTY_CHOICE;
}
static int pairtest(fdtype pair,fdtype key,fdtype val)
{
  if (FD_EQUAL(FD_CAR(pair),key))
    if (FD_VOIDP(val))
      if (FD_EMPTY_CHOICEP(FD_CDR(pair))) return 0;
      else return 1;
    else if (FD_EQUAL(FD_CDR(pair),val)) return 1;
    else return 0;
  else return 0;
}

static int pairgetsize(fdtype pair)
{
  return 1;
}

static fdtype pairkeys(fdtype pair)
{
  return fd_incref(FD_CAR(pair));
}

/* Describing tables */

FD_EXPORT void fd_display_table(u8_output out,fdtype table,fdtype keysarg)
{
  U8_OUTPUT *tmp=u8_open_output_string(1024);
  fdtype keys=
    ((FD_VOIDP(keysarg)) ? (fd_getkeys(table)) : (fd_incref(keysarg)));
  FD_DO_CHOICES(key,keys) {
    fdtype values=fd_get(table,key,FD_EMPTY_CHOICE);
    tmp->u8_write=tmp->u8_outbuf; *(tmp->u8_outbuf)='\0';
    u8_printf(tmp,"   %q:   %q\n",key,values);
    if (u8_strlen(tmp->u8_outbuf)<80) u8_puts(out,tmp->u8_outbuf);
    else {
      u8_printf(out,"   %q:\n",key);
      {FD_DO_CHOICES(value,values) u8_printf(out,"      %q\n",value);}}
    fd_decref(values);}
  fd_decref(keys);
  u8_close((u8_stream)tmp);
}

/* Table max functions */

FD_EXPORT fdtype fd_table_max(fdtype table,fdtype scope,fdtype *maxvalp)
{
  if (FD_EMPTY_CHOICEP(scope)) return FD_EMPTY_CHOICE;
  else if (FD_HASHTABLEP(table))
    return fd_hashtable_max((fd_hashtable)table,scope,maxvalp);
  else if (FD_SLOTMAPP(table))
    return fd_slotmap_max((fd_slotmap)table,scope,maxvalp);
  else {
    fdtype keys=fd_getkeys(table);
    fdtype maxval=FD_VOID, results=FD_EMPTY_CHOICE;
    {FD_DO_CHOICES(key,keys)
       if ((FD_VOIDP(scope)) || (fd_overlapp(key,scope))) {
         fdtype val=fd_get(table,key,FD_VOID);
         if ((FD_EMPTY_CHOICEP(val)) || (FD_VOIDP(val))) {}
         else if (FD_NUMBERP(val)) {
           if (FD_VOIDP(maxval)) {
             maxval=fd_incref(val); results=fd_incref(key);}
           else {
             int cmp=numcompare(val,maxval);
             if (cmp>0) {
               fd_decref(results); results=fd_incref(key);
               fd_decref(maxval); maxval=fd_incref(val);}
             else if (cmp==0) {
               fd_incref(key);
               FD_ADD_TO_CHOICE(results,key);}}}
         else {}
         fd_decref(val);}}
    fd_decref(keys);
    if ((maxvalp) && (FD_NUMBERP(maxval))) *maxvalp=maxval;
    else fd_decref(maxval);
    return results;}
}

FD_EXPORT fdtype fd_table_skim(fdtype table,fdtype maxval,fdtype scope)
{
  if (FD_EMPTY_CHOICEP(scope)) return FD_EMPTY_CHOICE;
  else if (FD_HASHTABLEP(table))
    return fd_hashtable_skim((fd_hashtable)table,maxval,scope);
  else if (FD_SLOTMAPP(table))
    return fd_slotmap_skim((fd_slotmap)table,maxval,scope);
  else {
    fdtype keys=fd_getkeys(table);
    fdtype results=FD_EMPTY_CHOICE;
    {FD_DO_CHOICES(key,keys)
       if ((FD_VOIDP(scope)) || (fd_overlapp(key,scope))) {
         fdtype val=fd_get(table,key,FD_VOID);
         if (FD_NUMBERP(val)) {
           int cmp=fd_numcompare(val,maxval);
           if (cmp>=0) {
             fd_incref(key);
             FD_ADD_TO_CHOICE(results,key);}
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
  fd_recyclers[fd_slotmap_type]=recycle_slotmap;
  fd_unparsers[fd_slotmap_type]=unparse_slotmap;
  fd_copiers[fd_slotmap_type]=copy_slotmap;
  fd_comparators[fd_slotmap_type]=compare_slotmaps;

  /* SCHEMAP */
  fd_recyclers[fd_schemap_type]=recycle_schemap;
  fd_unparsers[fd_schemap_type]=unparse_schemap;
  fd_copiers[fd_schemap_type]=copy_schemap;
  fd_comparators[fd_schemap_type]=compare_schemaps;

  /* HASHTABLE */
  fd_recyclers[fd_hashtable_type]=(fd_recycle_fn)fd_recycle_hashtable;
  fd_unparsers[fd_hashtable_type]=unparse_hashtable;
  fd_copiers[fd_hashtable_type]=copy_hashtable;

  /* HASHSET */
  fd_recyclers[fd_hashset_type]=(fd_recycle_fn)fd_recycle_hashset;
  fd_unparsers[fd_hashset_type]=unparse_hashset;
  fd_copiers[fd_hashset_type]=copy_hashset;

  memset(fd_tablefns,0,sizeof(fd_tablefns));

  /* HASHTABLE table functions */
  fd_tablefns[fd_hashtable_type]=u8_zalloc(struct FD_TABLEFNS);
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

  /* SLOTMAP table functions */
  fd_tablefns[fd_slotmap_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_slotmap_type]->get=(fd_table_get_fn)fd_slotmap_get;
  fd_tablefns[fd_slotmap_type]->add=(fd_table_add_fn)fd_slotmap_add;
  fd_tablefns[fd_slotmap_type]->drop=(fd_table_drop_fn)fd_slotmap_drop;
  fd_tablefns[fd_slotmap_type]->store=(fd_table_store_fn)fd_slotmap_store;
  fd_tablefns[fd_slotmap_type]->test=(fd_table_test_fn)fd_slotmap_test;
  fd_tablefns[fd_slotmap_type]->getsize=(fd_table_getsize_fn)slotmap_getsize;
  fd_tablefns[fd_slotmap_type]->keys=(fd_table_keys_fn)fd_slotmap_keys;
  fd_tablefns[fd_slotmap_type]->modified=
    (fd_table_modified_fn)slotmap_modified;

  /* SCHEMAP table functions */
  fd_tablefns[fd_schemap_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_schemap_type]->get=(fd_table_get_fn)fd_schemap_get;
  fd_tablefns[fd_schemap_type]->add=(fd_table_add_fn)fd_schemap_add;
  fd_tablefns[fd_schemap_type]->drop=(fd_table_drop_fn)fd_schemap_drop;
  fd_tablefns[fd_schemap_type]->store=(fd_table_store_fn)fd_schemap_store;
  fd_tablefns[fd_schemap_type]->test=(fd_table_test_fn)fd_schemap_test;
  fd_tablefns[fd_schemap_type]->getsize=(fd_table_getsize_fn)schemap_getsize;
  fd_tablefns[fd_schemap_type]->keys=(fd_table_keys_fn)fd_schemap_keys;
  fd_tablefns[fd_schemap_type]->modified=
    (fd_table_modified_fn)schemap_modified;

  /* HASHSET table functions */
  fd_tablefns[fd_hashset_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_hashset_type]->get=(fd_table_get_fn)hashsetget;
  fd_tablefns[fd_hashset_type]->add=(fd_table_add_fn)hashsetstore;
  /* This is a no-op because you can't drop a value from a hashset.
     That would just set its value to false. */
  fd_tablefns[fd_hashset_type]->drop=(fd_table_drop_fn)NULL;
  fd_tablefns[fd_hashset_type]->store=(fd_table_store_fn)hashsetstore;
  /* This is a no-op because every key has a T/F value in the hashet. */
  fd_tablefns[fd_hashset_type]->test=NULL;
  fd_tablefns[fd_hashset_type]->getsize=(fd_table_getsize_fn)hashset_getsize;
  fd_tablefns[fd_hashset_type]->keys=(fd_table_keys_fn)hashsetelts;
  fd_tablefns[fd_hashset_type]->modified=
    (fd_table_modified_fn)hashset_modified;

  /* HASHSET table functions */
  fd_tablefns[fd_pair_type]=u8_zalloc(struct FD_TABLEFNS);
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
    e->fd_compound_restorefn=restore_hashtable;
  }
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
