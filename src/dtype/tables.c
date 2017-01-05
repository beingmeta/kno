/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
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
static u8_string CantGet=_("Table doesn't support get");
static u8_string CantSet=_("Table doesn't support set");
static u8_string CantAdd=_("Table doesn't support add");
static u8_string CantDrop=_("Table doesn't support drop");
static u8_string CantTest=_("Table doesn't support test");
static u8_string CantGetKeys=_("Table doesn't support getkeys");
static u8_string CantCheckModified=_("Can't check for modification status");
static u8_string CantSetModified=_("Can't set modficiation status");
static u8_string BadHashtableMethod=_("Invalid hashtable method");

#define DEBUGGING 0

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
      do --j; while (v[j].fd_kvkey > v[0].fd_kvkey);
      do ++i; while (i < j && (v[i].fd_kvkey<v[0].fd_kvkey));
      if (i >= j) break; swap_keyvals(&v[i], &v[j]);}
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
      do --j; while (cons_compare(v[j].fd_kvkey,v[0].fd_kvkey)>0);
      do ++i; while (i < j && ((cons_compare(v[i].fd_kvkey,v[0].fd_kvkey))<0));
      if (i >= j) break; swap_keyvals(&v[i], &v[j]);}
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
    if (FD_ATOMICP(v[i].fd_kvkey)) i++;
    else return cons_sort_keyvals(v,n);
  return atomic_sort_keyvals(v,n);
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
  int size=*sizep, space=((spacep)?(0):(*spacep)), found=0;
  struct FD_KEYVAL *bottom=keyvals, *top=bottom+size-1;
  struct FD_KEYVAL *limit=bottom+size, *middle=bottom+size/2;
  if (keyvals == NULL) {
    *kvp=keyvals=u8_alloc(struct FD_KEYVAL);
    memset(keyvals,0,sizeof(struct FD_KEYVAL));
    if (keyvals==NULL) return NULL;
    keyvals->fd_kvkey=fd_incref(key);
    keyvals->fd_keyval=FD_EMPTY_CHOICE;
    if (sizep) *sizep=1;
    if (spacep) *spacep=1;
    return keyvals;}
  if (FD_ATOMICP(key))
    while (top>=bottom) {
      middle=bottom+(top-bottom)/2;
      if (middle>=limit) break;
      else if (key==middle->fd_kvkey) {found=1; break;}
      else if (FD_CONSP(middle->fd_kvkey)) top=middle-1;
      else if (key<middle->fd_kvkey) top=middle-1;
      else bottom=middle+1;}
  else while (top>=bottom) {
    int comparison;
    middle=bottom+(top-bottom)/2;
    if (middle>=limit) break;
    comparison=cons_compare(key,middle->fd_kvkey);
    if (comparison==0) {found=1; break;}
    else if (comparison<0) top=middle-1;
    else bottom=middle+1;}
  if (found) return middle;
  else if (size+1<space) {
    struct FD_KEYVAL *insert_point=kvp[size+1];
    *sizep=size+1;
    insert_point->fd_kvkey=fd_incref(key);
    insert_point->fd_keyval=FD_EMPTY_CHOICE;
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
    insert_point->fd_kvkey=fd_incref(key);
    insert_point->fd_keyval=FD_EMPTY_CHOICE;
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
  struct FD_KEYVAL *result, *okeyvals;
  int osize, size, ospace, space, unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if ((FD_ABORTP(value)))
    return fd_interr(value);
  if (sm->fd_uselock) { fd_write_lock(&sm->fd_rwlock); unlock=1;}
  size=osize=FD_XSLOTMAP_SIZE(sm);
  space=ospace=FD_XSLOTMAP_SPACE(sm);
  okeyvals=sm->fd_keyvals;
  result=fd_sortvec_insert(key,&(sm->fd_keyvals),&size,&space,sm->fd_free_keyvals);
  if (FD_EXPECT_FALSE(result==NULL)) {
    if (unlock) fd_rw_unlock(&(sm->fd_rwlock));
    fd_seterr(fd_MallocFailed,"fd_slotmap_store",NULL,FD_VOID);
    return -1;}
  if (sm->fd_keyvals!=okeyvals) sm->fd_free_keyvals=1;
  fd_decref(result->fd_keyval); result->fd_keyval=fd_incref(value);
  FD_XSLOTMAP_MARK_MODIFIED(sm);
  if (ospace != space) { FD_XSLOTMAP_SET_SPACE(sm,space); }
  if (osize != size) {
    FD_XSLOTMAP_SET_SIZE(sm,size);
    if (unlock) fd_rw_unlock(&sm->fd_rwlock);
    return 1;}
  if (unlock) fd_rw_unlock(&sm->fd_rwlock);
  return 0;
}

FD_EXPORT int fd_slotmap_add(struct FD_SLOTMAP *sm,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result, *okeyvals;
  int osize, size, ospace, space, unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if (FD_EMPTY_CHOICEP(value)) return 0;
  else if ((FD_ABORTP(value)))
    return fd_interr(value);
  if (sm->fd_uselock) { fd_write_lock(&sm->fd_rwlock); unlock=1;}
  size=osize=FD_XSLOTMAP_SIZE(sm);
  space=ospace=FD_XSLOTMAP_SPACE(sm);
  okeyvals=sm->fd_keyvals;
  result=fd_sortvec_insert(key,&(sm->fd_keyvals),&size,&space,sm->fd_free_keyvals);
  if (FD_EXPECT_FALSE(result==NULL)) {
    if (unlock) fd_rw_unlock(&sm->fd_rwlock);
    fd_seterr(fd_MallocFailed,"fd_slotmap_add",NULL,FD_VOID);
    return -1;}
  if (sm->fd_keyvals!=okeyvals) sm->fd_free_keyvals=1;
  fd_incref(value);
  FD_ADD_TO_CHOICE(result->fd_keyval,value);
  FD_XSLOTMAP_MARK_MODIFIED(sm);
  if (ospace != space) { FD_XSLOTMAP_SET_SPACE(sm,space); }
  if (osize != size) {
    FD_XSLOTMAP_SET_SIZE(sm,size);
    if (unlock) fd_rw_unlock(&sm->fd_rwlock);
    return 1;}
  if (unlock) fd_rw_unlock(&sm->fd_rwlock);
  return 0;
}

FD_EXPORT int fd_slotmap_drop(struct FD_SLOTMAP *sm,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int size, unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if ((FD_ABORTP(value)))
    return fd_interr(value);
  if (sm->fd_uselock) { fd_write_lock(&sm->fd_rwlock); unlock=1;}
  size=FD_XSLOTMAP_SIZE(sm);
  result=fd_sortvec_get(key,sm->fd_keyvals,size);
  if (result) {
    fdtype newval=((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) :
                   (fd_difference(result->fd_keyval,value)));
    if ((newval == result->fd_keyval)&&(!(FD_EMPTY_CHOICEP(newval)))) {
      /* This is the case where, for example, value isn't on the slot.
         But we incref'd newvalue/result->fd_keyval, so we decref it.
         However, if the slot is already empty (for whatever reason),
         dropping the slot actually removes it from the slotmap. */
      fd_decref(newval);}
    else {
      FD_XSLOTMAP_MARK_MODIFIED(sm);
      if (FD_EMPTY_CHOICEP(newval)) {
        int entries_to_move=(size-(result-sm->fd_keyvals))-1;
        fd_decref(result->fd_kvkey); fd_decref(result->fd_keyval);
        memmove(result,result+1,entries_to_move*sizeof(struct FD_KEYVAL));
        FD_XSLOTMAP_SET_SIZE(sm,size-1);}
      else {
        fd_decref(result->fd_keyval); result->fd_keyval=newval;}}}
  if (unlock) fd_rw_unlock(&sm->fd_rwlock);
  if (result) return 1; else return 0;
}

FD_EXPORT int fd_slotmap_delete(struct FD_SLOTMAP *sm,fdtype key)
{
  struct FD_KEYVAL *result; int size, unlock=0;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  if (sm->fd_uselock) { fd_write_lock(&sm->fd_rwlock); unlock=1;}
  size=FD_XSLOTMAP_SIZE(sm);
  result=fd_sortvec_get(key,sm->fd_keyvals,size);
  if (result) {
    int entries_to_move=(size-(result-sm->fd_keyvals))-1;
    fd_decref(result->fd_kvkey); fd_decref(result->fd_keyval);
    memmove(result,result+1,entries_to_move*sizeof(struct FD_KEYVAL));
    FD_XSLOTMAP_MARK_MODIFIED(sm);
    FD_XSLOTMAP_SET_SIZE(sm,size-1);}
  if (unlock) fd_rw_unlock(&sm->fd_rwlock);
  if (result) return 1; else return 0;
}

static int slotmap_getsize(struct FD_SLOTMAP *ptr)
{
  FD_CHECK_TYPE_RET(ptr,fd_slotmap_type);
  return FD_XSLOTMAP_SIZE(ptr);
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
  if (sm->fd_uselock) { fd_read_lock(&sm->fd_rwlock); unlock=1;}
  size=FD_XSLOTMAP_SIZE(sm); scan=sm->fd_keyvals; limit=scan+size;
  if (size==0) {
    if (unlock) fd_rw_unlock(&sm->fd_rwlock);
    return FD_EMPTY_CHOICE;}
  else if (size==1) {
    fdtype key=fd_incref(scan->fd_kvkey);
    if (unlock) fd_rw_unlock(&sm->fd_rwlock);
    return key;}
  /* Otherwise, copy the keys into a choice vector. */
  result=fd_alloc_choice(size);
  write=(fdtype *)FD_XCHOICE_DATA(result);
  while (scan < limit) {
    fdtype key=(scan++)->fd_kvkey;
    if (FD_CONSP(key)) {fd_incref(key); atomic=0;}
    *write++=key;}
  if (unlock) fd_rw_unlock(&sm->fd_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_init_choice(result,size,NULL,
                        ((FD_CHOICE_REALLOC)|
                         ((atomic)?(FD_CHOICE_ISATOMIC):
                          (FD_CHOICE_ISCONSES))));
}

FD_EXPORT fdtype fd_slotmap_values(struct FD_SLOTMAP *sm)
{
  struct FD_KEYVAL *scan, *limit; int unlock=0;
  struct FD_ACHOICE *achoice; fdtype results;
  int size, atomic=1;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (sm->fd_uselock) { fd_read_lock(&sm->fd_rwlock); unlock=1;}
  size=FD_XSLOTMAP_SIZE(sm); scan=sm->fd_keyvals; limit=scan+size;
  if (size==0) {
    if (unlock) fd_rw_unlock(&sm->fd_rwlock);
    return FD_EMPTY_CHOICE;}
  else if (size==1) {
    fdtype value=fd_incref(scan->fd_keyval);
    if (unlock) fd_rw_unlock(&sm->fd_rwlock);
    return value;}
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_achoice(NULL,7*(size),0);
  achoice=FD_XACHOICE(results);
  while (scan < limit) {
    fdtype value=(scan++)->fd_keyval;
    if (FD_CONSP(value)) {fd_incref(value); atomic=0;}
    _achoice_add(achoice,value);}
  if (unlock) fd_rw_unlock(&sm->fd_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_simplify_choice(results);
}

FD_EXPORT fdtype fd_slotmap_assocs(struct FD_SLOTMAP *sm)
{
  struct FD_KEYVAL *scan, *limit; int unlock=0;
  struct FD_ACHOICE *achoice; fdtype results;
  int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (sm->fd_uselock) { fd_read_lock(&sm->fd_rwlock); unlock=1;}
  size=FD_XSLOTMAP_SIZE(sm); scan=sm->fd_keyvals; limit=scan+size;
  if (size==0) {
    if (unlock) fd_rw_unlock(&sm->fd_rwlock);
    return FD_EMPTY_CHOICE;}
  else if (size==1) {
    fdtype key=scan->fd_kvkey, value=scan->fd_keyval;
    fd_incref(key); fd_incref(value);
    if (unlock) fd_rw_unlock(&sm->fd_rwlock);
    return fd_init_pair(NULL,key,value);}
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_achoice(NULL,7*(size),0);
  achoice=FD_XACHOICE(results);
  while (scan < limit) {
    fdtype key=scan->fd_kvkey, value=scan->fd_keyval;
    fdtype assoc=fd_init_pair(NULL,key,value);
    fd_incref(key); fd_incref(value); scan++;
    _achoice_add(achoice,assoc);}
  if (unlock) fd_rw_unlock(&sm->fd_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_simplify_choice(results);
}

FD_EXPORT fdtype fd_slotmap_max
  (struct FD_SLOTMAP *sm,fdtype scope,fdtype *maxvalp)
{
  fdtype maxval=FD_VOID, result=FD_EMPTY_CHOICE;
  struct FD_KEYVAL *scan, *limit; int size, unlock=0;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (FD_EMPTY_CHOICEP(scope)) return result;
  if (sm->fd_uselock) { fd_read_lock(&sm->fd_rwlock); unlock=1;}
  size=FD_XSLOTMAP_SIZE(sm); scan=sm->fd_keyvals; limit=scan+size;
  while (scan<limit) {
    if ((FD_VOIDP(scope)) || (fd_overlapp(scan->fd_kvkey,scope))) {
      if (FD_EMPTY_CHOICEP(scan->fd_keyval)) {}
      else if (FD_NUMBERP(scan->fd_keyval)) {
        if (FD_VOIDP(maxval)) {
          result=fd_incref(scan->fd_kvkey); maxval=fd_incref(scan->fd_keyval);}
        else {
          int cmp=numcompare(scan->fd_keyval,maxval);
          if (cmp>0) {
            fd_decref(result); fd_decref(maxval);
            result=fd_incref(scan->fd_kvkey);
            maxval=fd_incref(scan->fd_keyval);}
          else if (cmp==0) {
            fd_incref(scan->fd_kvkey);
            FD_ADD_TO_CHOICE(result,scan->fd_kvkey);}}}}
    scan++;}
  if (unlock) fd_rw_unlock(&sm->fd_rwlock);
  if ((maxvalp) && (FD_NUMBERP(maxval))) *maxvalp=fd_incref(maxval);
  return result;
}

FD_EXPORT fdtype fd_slotmap_skim(struct FD_SLOTMAP *sm,fdtype maxval,fdtype scope)
{
  fdtype result=FD_EMPTY_CHOICE; int unlock=0;
  struct FD_KEYVAL *scan, *limit; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (FD_EMPTY_CHOICEP(scope)) return result;
  if (sm->fd_uselock) { fd_read_lock(&sm->fd_rwlock); unlock=1;}
  size=FD_XSLOTMAP_SIZE(sm); scan=sm->fd_keyvals; limit=scan+size;
  while (scan<limit) {
    if ((FD_VOIDP(scope)) || (fd_overlapp(scan->fd_kvkey,scope)))
      if (FD_NUMBERP(scan->fd_keyval)) {
        int cmp=numcompare(scan->fd_keyval,maxval);
        if (cmp>=0) {
          fd_incref(scan->fd_kvkey);
          FD_ADD_TO_CHOICE(result,scan->fd_kvkey);}}
    scan++;}
  if (unlock) fd_rw_unlock(&sm->fd_rwlock);
  return result;
}

FD_EXPORT fdtype fd_init_slotmap
  (struct FD_SLOTMAP *ptr,
   int len,struct FD_KEYVAL *data)
{
  if (ptr == NULL) ptr=u8_alloc(struct FD_SLOTMAP);
  FD_INIT_STRUCT(ptr,struct FD_SLOTMAP);
  FD_INIT_CONS(ptr,fd_slotmap_type);
  ptr->fd_table_size=len;
  if (data) {
    sort_keyvals(data,len);
    ptr->fd_keyvals=data;
    ptr->fd_table_size=len;}
  else if (len) {
    ptr->fd_keyvals=u8_zalloc_n(len,struct FD_KEYVAL);
    ptr->fd_table_size=0;}
  else {
    ptr->fd_table_size=0;
    ptr->fd_keyvals=NULL;}
  ptr->fd_modified=ptr->fd_readonly=0;
  ptr->fd_uselock=1;
  ptr->fd_free_keyvals=1;
#if FD_THREADS_ENABLED
  fd_init_rwlock(&(ptr->fd_rwlock));
#endif
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
  ptr->fd_table_free=space; ptr->fd_table_size=len;
  while (i<len) {
    kv[i].fd_kvkey=data[i].fd_kvkey;
    kv[i].fd_keyval=data[i].fd_keyval;
    i++;}
  while (i<space) {kv[i].fd_kvkey=FD_VOID; kv[i].fd_keyval=FD_VOID; i++;}
  ptr->fd_keyvals=kv;
  sort_keyvals(kv,len);
  ptr->fd_modified=ptr->fd_readonly=0;
  ptr->fd_uselock=1;
  ptr->fd_free_keyvals=0;
#if FD_THREADS_ENABLED
  fd_init_rwlock(&(ptr->fd_rwlock));
#endif
  return FDTYPE_CONS(ptr);
}

static fdtype copy_slotmap(fdtype smap,int flags)
{
  struct FD_SLOTMAP *cur=FD_GET_CONS(smap,fd_slotmap_type,fd_slotmap);
  struct FD_SLOTMAP *fresh; int unlock=0;
  if (!(cur->fd_free_keyvals)) {
    fdtype copy; struct FD_SLOTMAP *consed; struct FD_KEYVAL *kvals;
    int i=0, len;
    copy=fd_make_slotmap(cur->fd_table_free,cur->fd_table_size,cur->fd_keyvals);
    consed=(struct FD_SLOTMAP *)copy;
    kvals=consed->fd_keyvals; len=consed->fd_table_size;
    if (cur->fd_uselock) {fd_read_lock_struct(cur); unlock=1;}
    while (i<len) {
      fdtype key=kvals[i].fd_kvkey, val=kvals[i].fd_keyval;
      if ((flags&FD_FULL_COPY)||(FD_STACK_CONSED(key)))
        kvals[i].fd_kvkey=fd_copier(key,flags);
      else fd_incref(key);
      if ((flags&FD_FULL_COPY)||(FD_STACK_CONSED(val)))
        kvals[i].fd_keyval=fd_copier(val,flags);
      else fd_incref(val);
      i++;}
    if (unlock) fd_rw_unlock_struct(cur);
    return copy;}
  else fresh=u8_alloc(struct FD_SLOTMAP);
  FD_INIT_STRUCT(fresh,struct FD_SLOTMAP);
  FD_INIT_CONS(fresh,fd_slotmap_type);
  if (cur->fd_uselock) {fd_read_lock_struct(cur); unlock=1;}
  fresh->fd_modified=fresh->fd_readonly=0;
  fresh->fd_uselock=1; fresh->fd_free_keyvals=1;
  if (FD_XSLOTMAP_SIZE(cur)) {
    int n=FD_XSLOTMAP_SIZE(cur);
    struct FD_KEYVAL *read=cur->fd_keyvals, *read_limit=read+n;
    struct FD_KEYVAL *write=u8_alloc_n(n,struct FD_KEYVAL);
    fresh->fd_table_free=fresh->fd_table_size=n; fresh->fd_keyvals=write;
    memset(write,0,n*sizeof(struct FD_KEYVAL));
    while (read<read_limit) {
      fdtype key=read->fd_kvkey, val=read->fd_keyval; read++;
      if (FD_CONSP(key)) write->fd_kvkey=fd_copy(key);
      else write->fd_kvkey=key;
      if (FD_CONSP(val))
        if (FD_ACHOICEP(val))
          write->fd_keyval=fd_make_simple_choice(val);
        else if ((flags&FD_FULL_COPY)||(FD_STACK_CONSED(val)))
          write->fd_keyval=fd_copier(val,flags);
        else write->fd_keyval=fd_incref(val);
      else write->fd_keyval=val;
      write++;}}
  else {
    fresh->fd_table_free=fresh->fd_table_size=0; fresh->fd_keyvals=NULL;}
  if (unlock) fd_rw_unlock_struct(cur);
#if FD_THREADS_ENABLED
  fd_init_rwlock(&(fresh->fd_rwlock));
#endif
  return FDTYPE_CONS(fresh);
}

static void recycle_slotmap(struct FD_CONS *c)
{
  struct FD_SLOTMAP *sm=(struct FD_SLOTMAP *)c;
  fd_write_lock_struct(sm);
  {
    int slotmap_size=FD_XSLOTMAP_SIZE(sm);
    const struct FD_KEYVAL *scan=sm->fd_keyvals;
    const struct FD_KEYVAL *limit=sm->fd_keyvals+slotmap_size;
    while (scan < limit) {
      fd_decref(scan->fd_kvkey); fd_decref(scan->fd_keyval); scan++;}
    if (sm->fd_free_keyvals) u8_free(sm->fd_keyvals);
    fd_rw_unlock_struct(sm);
    fd_destroy_rwlock(&(sm->fd_rwlock));
    u8_free(sm);
  }
}
static int unparse_slotmap(u8_output out,fdtype x)
{
  struct FD_SLOTMAP *sm=FD_XSLOTMAP(x);
  fd_read_lock_struct(sm);
  {
    int slotmap_size=FD_XSLOTMAP_SIZE(sm);
    const struct FD_KEYVAL *scan=sm->fd_keyvals;
    const struct FD_KEYVAL *limit=sm->fd_keyvals+slotmap_size;
    u8_puts(out,"#[");
    if (scan<limit) {
      fd_unparse(out,scan->fd_kvkey); u8_putc(out,' ');
      fd_unparse(out,scan->fd_keyval); scan++;}
    while (scan< limit) {
      u8_putc(out,' ');
      fd_unparse(out,scan->fd_kvkey); u8_putc(out,' ');
      fd_unparse(out,scan->fd_keyval); scan++;}
    u8_puts(out,"]");
  }
  fd_rw_unlock_struct(sm);
  return 1;
}
static int compare_slotmaps(fdtype x,fdtype y,int quick)
{
  int result=0; int unlockx=0, unlocky=0;
  struct FD_SLOTMAP *smx=(struct FD_SLOTMAP *)x;
  struct FD_SLOTMAP *smy=(struct FD_SLOTMAP *)y;
  if (smx->fd_uselock) {fd_read_lock_struct(smx); unlockx=1;}
  if (smy->fd_uselock) {fd_read_lock_struct(smy); unlocky=1;}
  {
    int xsize=FD_XSLOTMAP_SIZE(smx), ysize=FD_XSLOTMAP_SIZE(smy);
    if (xsize>ysize) result=1;
    else if (xsize<ysize) result=-1;
    else {
      const struct FD_KEYVAL *xkeyvals=smx->fd_keyvals, *ykeyvals=smy->fd_keyvals;
      int i=0; while (i < xsize) {
        int cmp=FD_QCOMPARE(xkeyvals[i].fd_kvkey,ykeyvals[i].fd_kvkey);
        if (cmp) {result=cmp; break;}
        else i++;}
      if (result==0) {
        i=0; while (i < xsize) {
        int cmp=FD_QCOMPARE(xkeyvals[i].fd_keyval,ykeyvals[i].fd_keyval);
        if (cmp) {result=cmp; break;}
        else i++;}}}
  }
  if (unlockx) fd_rw_unlock_struct(smx);
  if (unlocky) fd_rw_unlock_struct(smy);
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
  FD_INIT_CONS(ptr,fd_schemap_type); ptr->fd_schema=schema;
  ptr->fd_table_size=size; ptr->fd_schema=schema;
  if (flags&FD_SCHEMAP_SORTED) ptr->fd_sorted=1;
  if (flags&FD_SCHEMAP_PRIVATE) ptr->fd_shared_schema=1;
  if (flags&FD_SCHEMAP_READONLY) ptr->fd_readonly=1;
  if (flags&FD_SCHEMAP_MODIFIED) ptr->fd_modified=1;
  if (values) ptr->fd_values=values;
  else {
    ptr->fd_values=values=u8_alloc_n(size,fdtype);
    while (i<size) values[i++]=FD_VOID;}
#if FD_THREADS_ENABLED
  fd_init_rwlock(&(ptr->fd_rwlock));
#endif
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
      if (i >= j) break; lispv_swap(&v[i], &v[j]);}
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
    FD_GET_CONS(schemap,fd_schemap_type,struct FD_SCHEMAP *);
  struct FD_SCHEMAP *nptr=u8_alloc(struct FD_SCHEMAP);
  int i=0, size=FD_XSCHEMAP_SIZE(ptr);
  fdtype *ovalues=ptr->fd_values, *values=((size==0) ? (NULL) : (u8_alloc_n(size,fdtype)));
  fdtype *schema=ptr->fd_schema, *nschema=NULL;
  FD_INIT_STRUCT(nptr,struct FD_SCHEMAP);
  FD_INIT_CONS(nptr,fd_schemap_type);
  if (ptr->fd_stack_schema)
    nptr->fd_schema=nschema=u8_alloc_n(size,fdtype);
  else nptr->fd_schema=schema;
  if ( (ptr->fd_stack_schema) || (!(ptr->fd_shared_schema)) )
    while (i < size) {
      fdtype val=ovalues[i];
      nschema[i]=schema[i];
      if (FD_CONSP(val))
        if (FD_ACHOICEP(val))
          values[i]=fd_make_simple_choice(val);
        else if ((flags&FD_FULL_COPY)||(FD_STACK_CONSED(val)))
          values[i]=fd_copier(val,flags);
        else values[i]=fd_incref(val);
      else values[i]=val;
      i++;}
  else if (flags) while (i < size) {
      values[i]=fd_copier(ovalues[i],flags); i++;}
  else while (i < size) {
      values[i]=fd_incref(ovalues[i]); i++;}
  nptr->fd_values=values;
  nptr->fd_table_size=size;
  if ( (ptr->fd_stack_schema) || (!(ptr->fd_shared_schema)) )
    nptr->fd_shared_schema=1;
  fd_init_rwlock(&(nptr->fd_rwlock));
  return FDTYPE_CONS(nptr);
}

FD_EXPORT fdtype fd_init_schemap
  (struct FD_SCHEMAP *ptr,short size, struct FD_KEYVAL *init)
{
  int i=0; fdtype *news, *newv;
  if (ptr == NULL) ptr=u8_alloc(struct FD_SCHEMAP);
  FD_INIT_STRUCT(ptr,struct FD_SCHEMAP);
  FD_INIT_CONS(ptr,fd_schemap_type);
  news=u8_alloc_n(size,fdtype);
  ptr->fd_values=newv=u8_alloc_n(size,fdtype);
  ptr->fd_table_size=size; ptr->fd_sorted=1;
  sort_keyvals(init,size);
  while (i<size) {
    news[i]=init[i].fd_kvkey; newv[i]=init[i].fd_keyval; i++;}
  ptr->fd_schema=fd_register_schema(size,news);
  if (ptr->fd_schema != news) {
    ptr->fd_shared_schema=1;
    u8_free(news);}
  u8_free(init);
#if FD_THREADS_ENABLED
  fd_init_rwlock(&(ptr->fd_rwlock));
#endif
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
  fd_write_lock_struct(sm);
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->fd_schema,size,sm->fd_sorted);
  if (slotno>=0) {
    fd_decref(sm->fd_values[slotno]);
    sm->fd_values[slotno]=fd_incref(value);
    FD_XSCHEMAP_MARK_MODIFIED(sm);
    fd_rw_unlock_struct(sm);
    return 1;}
  else {
    fd_rw_unlock_struct(sm);
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
  fd_write_lock_struct(sm);
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->fd_schema,size,sm->fd_sorted);
  if (slotno>=0) {
    fd_incref(value);
    FD_ADD_TO_CHOICE(sm->fd_values[slotno],value);
    FD_XSCHEMAP_MARK_MODIFIED(sm);
    fd_rw_unlock_struct(sm);
    return 1;}
  else {
    fd_rw_unlock_struct(sm);
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
  fd_write_lock_struct(sm);
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->fd_schema,size,sm->fd_sorted);
  if (slotno>=0) {
    fdtype oldval=sm->fd_values[slotno];
    fdtype newval=((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) : (fd_difference(oldval,value)));
    if (newval == oldval) fd_decref(newval);
    else {
      FD_XSCHEMAP_MARK_MODIFIED(sm);
      fd_decref(oldval); sm->fd_values[slotno]=newval;}
    fd_rw_unlock_struct(sm);
    return 1;}
  else {
    fd_rw_unlock_struct(sm);
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
    ptr->fd_modified=1;
    return modified;}
  else {
    ptr->fd_modified=0;
    return modified;}
}

FD_EXPORT fdtype fd_schemap_keys(struct FD_SCHEMAP *sm)
{
  FD_CHECK_TYPE_RETDTYPE(sm,fd_schemap_type);
  {
    int size=FD_XSCHEMAP_SIZE(sm);
    if (size==0) return FD_EMPTY_CHOICE;
    else if (size==1) return fd_incref(sm->fd_schema[0]);
    else {
      struct FD_CHOICE *ch=fd_alloc_choice(size);
      memcpy((fdtype *)FD_XCHOICE_DATA(ch),sm->fd_schema,sizeof(fdtype)*size);
      if (sm->fd_sorted)
        return fd_init_choice(ch,size,sm->fd_schema,0);
      else return fd_init_choice(ch,size,sm->fd_schema,
                                 FD_CHOICE_INCREF|FD_CHOICE_DOSORT);}}
}

static void recycle_schemap(struct FD_CONS *c)
{
  struct FD_SCHEMAP *sm=(struct FD_SCHEMAP *)c;
  fd_write_lock_struct(sm);
  {
    int schemap_size=FD_XSCHEMAP_SIZE(sm);
    fdtype *scan=sm->fd_values, *limit=sm->fd_values+schemap_size;
    while (scan < limit) {fd_decref(*scan); scan++;}
    if ((sm->fd_schema) && (!(sm->fd_shared_schema)))
      u8_free(sm->fd_schema);
    if (sm->fd_values) u8_free(sm->fd_values);
    fd_rw_unlock_struct(sm);
    fd_destroy_rwlock(&(sm->fd_rwlock));
    u8_free(sm);
  }
}
static int unparse_schemap(u8_output out,fdtype x)
{
  struct FD_SCHEMAP *sm=FD_XSCHEMAP(x);
  fd_read_lock_struct(sm);
  {
    int i=0, schemap_size=FD_XSCHEMAP_SIZE(sm);
    fdtype *schema=sm->fd_schema, *values=sm->fd_values;
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
  fd_rw_unlock_struct(sm);
  return 1;
}
static int compare_schemaps(fdtype x,fdtype y,int quick)
{
  int result=0;
  struct FD_SCHEMAP *smx=(struct FD_SCHEMAP *)x;
  struct FD_SCHEMAP *smy=(struct FD_SCHEMAP *)y;
  fd_read_lock_struct(smx); fd_read_lock_struct(smy);
  {
    int xsize=FD_XSCHEMAP_SIZE(smx), ysize=FD_XSCHEMAP_SIZE(smy);
    if (xsize>ysize) result=1;
    else if (xsize<ysize) result=-1;
    else {
      fdtype *xschema=smx->fd_schema, *yschema=smy->fd_schema;
      fdtype *xvalues=smx->fd_values, *yvalues=smy->fd_values;
      int i=0; while (i < xsize) {
        int cmp=FD_QCOMPARE(xschema[i],yschema[i]);
        if (cmp) {result=cmp; break;}
        else i++;}
      if (result==0) {
        i=0; while (i < xsize) {
          int cmp=FD_QCOMPARE(xvalues[i],yvalues[i]);
          if (cmp) {result=cmp; break;}
          else i++;}}}}
  fd_rw_unlock_struct(smx); fd_rw_unlock_struct(smy);
  return result;
}

/* Hash functions */

/* This is the point at which we resize hashtables. */
#define hashtable_needs_resizep(ht)\
   ((ht->fd_n_keys*ht->fd_load_factor)>(ht->fd_n_buckets*4))
/* This is the target size for resizing. */
#define hashtable_resize_target(ht) \
   ((ht->fd_n_keys*ht->fd_load_factor)/2)

#define hashset_needs_resizep(hs)\
   ((hs->fd_n_keys*hs->fd_load_factor)>(hs->fd_n_slots*4))
/* This is the target size for resizing. */
#define hashset_resize_target(hs) \
   ((hs->fd_n_keys*hs->fd_load_factor)/2)

static int default_hashtable_loading=8;
static int default_hashset_loading=8;

/* These are all the higher of prime pairs around powers of 2.  They are
    used to select good hashtable sizes. */
static unsigned int hashtable_sizes[]=
 {19, 43, 73, 139, 271, 523, 1033, 2083, 4129, 8221, 16453,
  32803, 65539, 131113, 262153, 524353, 1048891, 2097259,
  4194583,
  8388619, 16777291, 32000911, 64000819, 128000629,
  256001719, 0};

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
        FD_GET_CONS(x,fd_string_type,struct FD_STRING *);
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
        FD_GET_CONS(x,fd_vector_type,struct FD_VECTOR *);
      return hash_elts(v->fd_vecelts,v->fd_veclen);}
    case fd_compound_type: {
      struct FD_COMPOUND *c=
        FD_GET_CONS(x,fd_compound_type,struct FD_COMPOUND *);
      if (c->fd_isopaque) {
        int ctype=FD_PTR_TYPE(x);
        if ((ctype>0) && (ctype<N_TYPE_MULTIPLIERS))
          return hash_mult(x,type_multipliers[ctype]);
        else return hash_mult(x,MYSTERIOUS_MULTIPLIER);}
      else return hash_mult
             (hash_lisp(c->fd_typetag),
              hash_elts(&(c->fd_elt0),c->fd_n_elts));}
    case fd_slotmap_type: {
      struct FD_SLOTMAP *sm=
        FD_GET_CONS(x,fd_slotmap_type,struct FD_SLOTMAP *);
      fdtype *kv=(fdtype *)sm->fd_keyvals;
      return hash_elts(kv,sm->fd_table_size*2);}
    case fd_choice_type: {
      struct FD_CHOICE *ch=
        FD_GET_CONS(x,fd_choice_type,struct FD_CHOICE *);
      int size=FD_XCHOICE_SIZE(ch);
      return hash_elts((fdtype *)(FD_XCHOICE_DATA(ch)),size);}
    case fd_achoice_type: {
      fdtype simple=fd_make_simple_choice(x);
      int hash=hash_lisp(simple);
      fd_decref(simple);
      return hash;}
    case fd_qchoice_type: {
      struct FD_QCHOICE *ch=
        FD_GET_CONS(x,fd_qchoice_type,struct FD_QCHOICE *);
      return hash_lisp(ch->fd_choiceval);}
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
    if (FDTYPE_EQUAL(key,he->fd_keyval0.fd_kvkey))
      return &(he->fd_keyval0);
    else return NULL;
  else return fd_sortvec_get(key,&(he->fd_keyval0),he->fd_n_entries);
}

FD_EXPORT struct FD_KEYVAL *fd_hash_bucket_insert
  (fdtype key,struct FD_HASH_BUCKET **hep)
{
  struct FD_HASH_BUCKET *he=*hep; int found=0;
  struct FD_KEYVAL *keyvals=&(he->fd_keyval0); int size=he->fd_n_entries;
  struct FD_KEYVAL *bottom=keyvals, *top=bottom+(size-1);
  struct FD_KEYVAL *middle=bottom+(top-bottom)/2;;
  while (top>=bottom) { /* (!(top<bottom)) */
    middle=bottom+(top-bottom)/2;
    if (FDTYPE_EQUAL(key,middle->fd_kvkey)) {found=1; break;}
    else if (cons_compare(key,middle->fd_kvkey)<0) top=middle-1;
    else bottom=middle+1;}
  if (found)
    return middle;
  else {
    int mpos=(middle-keyvals), dir=(bottom>middle), ipos=mpos+dir;
    struct FD_KEYVAL *insert_point;
    struct FD_HASH_BUCKET *new_hashentry=
      /* We don't need to use size+1 here because FD_HASH_BUCKET includes
         one value. */
      u8_realloc(he,
                 sizeof(struct FD_HASH_BUCKET)+
                 (size)*sizeof(struct FD_KEYVAL));
    memset((((unsigned char *)new_hashentry)+
            (sizeof(struct FD_HASH_BUCKET))+
            ((size-1)*sizeof(struct FD_KEYVAL))),
           0,sizeof(struct FD_KEYVAL));
    *hep=new_hashentry; new_hashentry->fd_n_entries++;
    insert_point=&(new_hashentry->fd_keyval0)+ipos;
    memmove(insert_point+1,insert_point,
            sizeof(struct FD_KEYVAL)*(size-ipos));
    insert_point->fd_kvkey=fd_incref(key);
    insert_point->fd_keyval=FD_EMPTY_CHOICE;
    return insert_point;}
}

FD_EXPORT struct FD_KEYVAL *fd_hashvec_insert
  (fdtype key,struct FD_HASH_BUCKET **slots,int n_slots,int *n_keys)
{
  unsigned int hash=fd_hash_lisp(key), offset=compute_offset(hash,n_slots);
  struct FD_HASH_BUCKET *he=slots[offset];
  if (he == NULL) {
    he=u8_alloc(struct FD_HASH_BUCKET);
    FD_INIT_STRUCT(he,struct FD_HASH_BUCKET);
    he->fd_n_entries=1; he->fd_keyval0.fd_kvkey=fd_incref(key);
    he->fd_keyval0.fd_keyval=FD_EMPTY_CHOICE;
    slots[offset]=he; if (n_keys) (*n_keys)++;
    return &(he->fd_keyval0);}
  else if (he->fd_n_entries == 1)
    if (FDTYPE_EQUAL(key,he->fd_keyval0.fd_kvkey))
      return &(he->fd_keyval0);
    else {
      if (n_keys) (*n_keys)++;
      return fd_hash_bucket_insert(key,&slots[offset]);}
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
  if (ht->fd_n_keys == 0) return fd_incref(dflt);
  if (ht->fd_uselock) { fd_read_lock_struct(ht); unlock=1; }
  if (ht->fd_n_keys == 0) {
    if (unlock) fd_rw_unlock_struct(ht);
    return fd_incref(dflt);}
  else result=fd_hashvec_get(key,ht->fd_buckets,ht->fd_n_buckets);
  if (result) {
    fdtype rv=result->fd_keyval;
    if (FD_VOIDP(rv)) {
      if (unlock) fd_rw_unlock_struct(ht);
      return fd_incref(dflt);}
    else if (FD_ACHOICEP(rv)) {
      fdtype simple=fd_make_simple_choice(rv);
      if (unlock) fd_rw_unlock_struct(ht);
      return simple;}
    else {
      if (unlock) fd_rw_unlock_struct(ht);
      return fd_incref(rv);}}
  else {
    if (unlock) fd_rw_unlock_struct(ht);
    return fd_incref(dflt);}
}

FD_EXPORT fdtype fd_hashtable_get_nolock
  (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (ht->fd_n_keys == 0) return fd_incref(dflt);
  else result=fd_hashvec_get(key,ht->fd_buckets,ht->fd_n_buckets);
  if (result) {
    fdtype rv=result->fd_keyval;
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
  if (ht->fd_n_keys == 0) return dflt;
  if (ht->fd_uselock) { fd_read_lock_struct(ht); unlock=1; }
  if (ht->fd_n_keys == 0) {
    if (unlock) fd_rw_unlock_struct(ht);
    return dflt;}
  else result=fd_hashvec_get(key,ht->fd_buckets,ht->fd_n_buckets);
  if (result) {
    fdtype rv=result->fd_keyval;
    if (FD_VOIDP(rv)) {
      if (unlock) fd_rw_unlock_struct(ht);
      return dflt;}
    else if (FD_ACHOICEP(rv)) {
      rv=result->fd_keyval=fd_simplify_choice(rv);
      if (unlock) fd_rw_unlock_struct(ht);
      return rv;}
    else {
      if (unlock) fd_rw_unlock_struct(ht);
      return rv;}}
  else {
    if (unlock) fd_rw_unlock_struct(ht);
    return dflt;}
}

FD_EXPORT fdtype fd_hashtable_get_nolockref
  (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (ht->fd_n_keys == 0) return dflt;
  else result=fd_hashvec_get(key,ht->fd_buckets,ht->fd_n_buckets);
  if (result) {
    fdtype rv=result->fd_keyval;
    if (FD_VOIDP(rv)) return dflt;
    else if (FD_ACHOICEP(rv)) {
      result->fd_keyval=fd_simplify_choice(rv);
      return result->fd_keyval;}
    else return rv;}
  else return dflt;
}

FD_EXPORT int fd_hashtable_probe(struct FD_HASHTABLE *ht,fdtype key)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->fd_n_keys == 0) return 0;
  if (ht->fd_uselock) { fd_read_lock_struct(ht); unlock=1; }
  if (ht->fd_n_keys == 0) {
    if (unlock) fd_rw_unlock_struct(ht);
    return 0;}
  else result=fd_hashvec_get(key,ht->fd_buckets,ht->fd_n_buckets);
  if (result) {
    if (unlock) fd_rw_unlock_struct(ht);
    return 1;}
  else {
    if (unlock) fd_rw_unlock((&(ht->fd_rwlock)));
    return 0;}
}

static int hashtable_test(struct FD_HASHTABLE *ht,fdtype key,fdtype val)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->fd_n_keys == 0) return 0;
  if (ht->fd_uselock) {fd_read_lock_struct(ht); unlock=1;}
  if (ht->fd_n_keys == 0) {
    if (unlock) fd_rw_unlock_struct(ht);
    return 0;}
  else result=fd_hashvec_get(key,ht->fd_buckets,ht->fd_n_buckets);
  if (result) {
    fdtype current=result->fd_keyval; int cmp;
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
    if (unlock) fd_rw_unlock_struct(ht);
    return cmp;}
  else {
    if (unlock) fd_rw_unlock((&(ht->fd_rwlock)));
    return 0;}
}

/* ?? */
FD_EXPORT int fd_hashtable_probe_novoid(struct FD_HASHTABLE *ht,fdtype key)
{
  struct FD_KEYVAL *result; int unlock=0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->fd_n_keys == 0) return 0;
  if (ht->fd_uselock) {fd_read_lock_struct(ht); unlock=1;}
  if (ht->fd_n_keys == 0) {
    if (unlock) fd_rw_unlock_struct(ht);
    return 0;}
  else result=fd_hashvec_get(key,ht->fd_buckets,ht->fd_n_buckets);
  if ((result) && (!(FD_VOIDP(result->fd_keyval)))) {
    if (unlock) fd_rw_unlock_struct(ht);
    return 1;}
  else {
    if (unlock) fd_rw_unlock((&(ht->fd_rwlock)));
    return 0;}
}

static void setup_hashtable(struct FD_HASHTABLE *ptr,int n_slots)
{
  struct FD_HASH_BUCKET **slots; int i=0;
  if (n_slots < 0) n_slots=fd_get_hashtable_size(-n_slots);
  ptr->fd_n_buckets=n_slots; ptr->fd_n_keys=0; ptr->fd_modified=0;
  ptr->fd_buckets=slots=
    u8_alloc_n(n_slots,struct FD_HASH_BUCKET *);
  while (i < n_slots) slots[i++]=NULL;
}

FD_EXPORT int fd_hashtable_store(fd_hashtable ht,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int n_keys, added;
  fdtype newv, oldv;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if ((FD_ABORTP(value))) return fd_interr(value);
  if (ht->fd_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_store",NULL,key);
    return -1;}
  fd_write_lock_struct(ht);
  if (ht->fd_n_buckets == 0) setup_hashtable(ht,17);
  n_keys=ht->fd_n_keys;
  result=fd_hashvec_insert
    (key,ht->fd_buckets,ht->fd_n_buckets,&(ht->fd_n_keys));
  if (ht->fd_n_keys>n_keys) added=1; else added=0;
  ht->fd_modified=1; oldv=result->fd_keyval;
  if (FD_ACHOICEP(value))
    /* Copy achoices */
    newv=fd_make_simple_choice(value);
  else newv=fd_incref(value);
  if (FD_ABORTP(newv)) {
    fd_rw_unlock_struct(ht);
    return fd_interr(newv);}
  else {
    result->fd_keyval=newv; fd_decref(oldv);}
  fd_rw_unlock_struct(ht);
  if (FD_EXPECT_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_slots < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_add(fd_hashtable ht,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int n_keys, added; fdtype newv;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->fd_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_add",NULL,key);
    return -1;}
  if (FD_EXPECT_FALSE(FD_EMPTY_CHOICEP(value)))
    return 0;
  else if ((FD_ABORTP(value)))
    return fd_interr(value);
  fd_write_lock_struct(ht);
  if (ht->fd_n_buckets == 0) setup_hashtable(ht,17);
  n_keys=ht->fd_n_keys;
  result=fd_hashvec_insert
    (key,ht->fd_buckets,ht->fd_n_buckets,&(ht->fd_n_keys));
  ht->fd_modified=1; if (ht->fd_n_keys>n_keys) added=1; else added=0;
  newv=fd_incref(value);
  if (FD_ABORTP(newv)) {
    fd_rw_unlock_struct(ht);
    return fd_interr(newv);}
  else if (FD_VOIDP(result->fd_keyval))
    result->fd_keyval=newv;
  else {FD_ADD_TO_CHOICE(result->fd_keyval,newv);}
  /* If the value is an achoice, it doesn't need to be locked because
     it will be protected by the hashtable's lock.  However this requires
     that we always normalize the choice when we return it.  */
  if (FD_ACHOICEP(result->fd_keyval)) {
    struct FD_ACHOICE *ch=FD_XACHOICE(result->fd_keyval);
    if (ch->fd_uselock) ch->fd_uselock=0;}
  fd_rw_unlock_struct(ht);
  if (FD_EXPECT_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_slots < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_drop
  (struct FD_HASHTABLE *ht,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->fd_n_keys == 0) return 0;
  if (ht->fd_readonly) {
    fd_seterr(fd_ReadOnlyHashtable,"fd_hashtable_drop",NULL,key);
    return -1;}
  fd_write_lock_struct(ht);
  result=fd_hashvec_get(key,ht->fd_buckets,ht->fd_n_buckets);
  if (result) {
    fdtype newval=
      ((FD_VOIDP(value)) ? (FD_VOID) :
       (fd_difference(result->fd_keyval,value)));
    fd_decref(result->fd_keyval);
    result->fd_keyval=newval;
    ht->fd_modified=1;
    fd_rw_unlock_struct(ht);
    return 1;}
  fd_rw_unlock_struct(ht);
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
  new->fd_modified=0;
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
  if ((ht->fd_readonly) && (op!=fd_table_test)) {
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
    if (ht->fd_n_keys == 0) return 0;
    result=fd_hashvec_get(key,ht->fd_buckets,ht->fd_n_buckets);
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
    if (ht->fd_n_buckets == 0) setup_hashtable(ht,17);
    result=fd_hashvec_insert(key,ht->fd_buckets,ht->fd_n_buckets,&(ht->fd_n_keys));}
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
  if ((result)&&(FD_ACHOICEP(result->fd_keyval))) was_achoice=1;
  switch (op) {
  case fd_table_replace_novoid:
    if (FD_VOIDP(result->fd_keyval)) return 0;
  case fd_table_store: case fd_table_replace: {
    fdtype newv=fd_incref(value);
    fd_decref(result->fd_keyval); result->fd_keyval=newv;
    break;}
  case fd_table_store_noref:
    fd_decref(result->fd_keyval); 
    result->fd_keyval=value; 
    break;
  case fd_table_add_if_present:
    if (FD_VOIDP(result->fd_keyval)) break;
  case fd_table_add: case fd_table_add_empty:
    fd_incref(value);
    if (FD_VOIDP(result->fd_keyval)) result->fd_keyval=value;
    else {FD_ADD_TO_CHOICE(result->fd_keyval,value);}
    break;
  case fd_table_add_noref: case fd_table_add_empty_noref:
    if (FD_VOIDP(result->fd_keyval)) result->fd_keyval=value;
    else {FD_ADD_TO_CHOICE(result->fd_keyval,value);}
    break;
  case fd_table_drop: 
    if ((FD_VOIDP(value))||(fd_overlapp(value,result->fd_keyval))) {
      fdtype newval=((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) : 
                     (fd_difference(result->fd_keyval,value)));
      fd_decref(result->fd_keyval); 
      result->fd_keyval=newval;
      break;}
    else return 0;
  case fd_table_test:
    if ((FD_CHOICEP(result->fd_keyval)) || 
        (FD_ACHOICEP(result->fd_keyval)) ||
        (FD_CHOICEP(value)) || 
        (FD_ACHOICEP(value)))
      return fd_overlapp(value,result->fd_keyval);
    else if (FDTYPE_EQUAL(value,result->fd_keyval))
      return 1;
    else return 0;
  case fd_table_default:
    if ((FD_EMPTY_CHOICEP(result->fd_keyval)) ||
        (FD_VOIDP(result->fd_keyval))) {
      result->fd_keyval=fd_incref(value);}
    break;
  case fd_table_increment_if_present:
    if (FD_VOIDP(result->fd_keyval)) break;
  case fd_table_increment:
    if ((FD_EMPTY_CHOICEP(result->fd_keyval)) ||
        (FD_VOIDP(result->fd_keyval))) {
      result->fd_keyval=fd_incref(value);}
    else if (!(FD_NUMBERP(result->fd_keyval))) {
      fd_seterr(fd_TypeError,"fd_table_increment",
                u8_strdup("number"),result->fd_keyval);
      return -1;}
    else {
      fdtype current=result->fd_keyval;
      FD_DO_CHOICES(v,value)
        if ((FD_FIXNUMP(current)) && (FD_FIXNUMP(v))) {
          int cval=FD_FIX2INT(current);
          int delta=FD_FIX2INT(v);
          result->fd_keyval=FD_INT(cval+delta);}
        else if ((FD_FLONUMP(current)) &&
                 (FD_CONS_REFCOUNT(((fd_cons)current))<2) &&
                 ((FD_FIXNUMP(v)) || (FD_FLONUMP(v)))) {
          struct FD_FLONUM *dbl=(fd_flonum)current;
          if (FD_FIXNUMP(v))
            dbl->fd_dblval=dbl->fd_dblval+FD_FIX2INT(v);
          else dbl->fd_dblval=dbl->fd_dblval+FD_FLONUM(v);}
        else if (FD_NUMBERP(v)) {
          fdtype newnum=fd_plus(current,v);
          if (newnum != current) {
            fd_decref(current); 
            result->fd_keyval=newnum;}}
        else {
          fd_seterr(fd_TypeError,"fd_table_increment",
                    u8_strdup("number"),v);
          return -1;}}
    break;
  case fd_table_multiply_if_present:
    if (FD_VOIDP(result->fd_keyval)) break;
  case fd_table_multiply:
    if ((FD_VOIDP(result->fd_keyval))||(FD_EMPTY_CHOICEP(result->fd_keyval)))  {
      result->fd_keyval=fd_incref(value);}
    else if (!(FD_NUMBERP(result->fd_keyval))) {
      fd_seterr(fd_TypeError,"fd_table_multiply",
                u8_strdup("number"),result->fd_keyval);
      return -1;}
    else {
      fdtype current=result->fd_keyval;
      FD_DO_CHOICES(v,value)
        if ((FD_FIXNUMP(current)) && (FD_FIXNUMP(v))) {
          int cval=FD_FIX2INT(current);
          int factor=FD_FIX2INT(v);
          result->fd_keyval=FD_INT(cval*factor);}
        else if ((FD_FLONUMP(current)) &&
                 (FD_CONS_REFCOUNT(((fd_cons)current))<2) &&
                 ((FD_FIXNUMP(v)) || (FD_FLONUMP(v)))) {
          struct FD_FLONUM *dbl=(fd_flonum)current;
          if (FD_FIXNUMP(v))
            dbl->fd_dblval=dbl->fd_dblval*FD_FIX2INT(v);
          else dbl->fd_dblval=dbl->fd_dblval*FD_FLONUM(v);}
        else if (FD_NUMBERP(v)) {
          fdtype newnum=fd_multiply(current,v);
          if (newnum != current) {
            fd_decref(current); 
            result->fd_keyval=newnum;}}
        else {
          fd_seterr(fd_TypeError,"table_multiply_op",
                    u8_strdup("number"),v);
          return -1;}}
    break;
  case fd_table_maximize_if_present:
    if (FD_VOIDP(result->fd_keyval)) break;
  case fd_table_maximize:
    if ((FD_EMPTY_CHOICEP(result->fd_keyval)) ||
        (FD_VOIDP(result->fd_keyval))) {
      result->fd_keyval=fd_incref(value);}
    else if (!(FD_NUMBERP(result->fd_keyval))) {
      fd_seterr(fd_TypeError,"table_maximize_op",
                u8_strdup("number"),result->fd_keyval);
      return -1;}
    else {
      fdtype current=result->fd_keyval;
      if ((FD_NUMBERP(current)) && (FD_NUMBERP(value))) {
        if (fd_numcompare(value,current)>0) {
          result->fd_keyval=fd_incref(value);
          fd_decref(current);}}
      else {
        fd_seterr(fd_TypeError,"table_maximize_op",
                  u8_strdup("number"),value);
        return -1;}}
    break;
  case fd_table_minimize_if_present:
    if (FD_VOIDP(result->fd_keyval)) break;
  case fd_table_minimize:
    if ((FD_EMPTY_CHOICEP(result->fd_keyval))||(FD_VOIDP(result->fd_keyval))) {
      result->fd_keyval=fd_incref(value);}
    else if (!(FD_NUMBERP(result->fd_keyval))) {
      fd_seterr(fd_TypeError,"table_maximize_op",
                u8_strdup("number"),result->fd_keyval);
      return -1;}
    else {
      fdtype current=result->fd_keyval;
      if ((FD_NUMBERP(current)) && (FD_NUMBERP(value))) {
        if (fd_numcompare(value,current)<0) {
          result->fd_keyval=fd_incref(value);
          fd_decref(current);}}
      else {
        fd_seterr(fd_TypeError,"table_maximize_op",
                  u8_strdup("number"),value);
        return -1;}}
    break;
  case fd_table_push:
    if ((FD_VOIDP(result->fd_keyval)) || (FD_EMPTY_CHOICEP(result->fd_keyval))) {
      result->fd_keyval=fd_make_pair(value,FD_EMPTY_LIST);}
    else if (FD_PAIRP(result->fd_keyval)) {
      result->fd_keyval=fd_conspair(fd_incref(value),result->fd_keyval);}
    else {
      fdtype tail=fd_conspair(result->fd_keyval,FD_EMPTY_LIST);
      result->fd_keyval=fd_conspair(fd_incref(value),tail);}
    break;
  default:
    added=-1;
    fd_seterr3(BadHashtableMethod,"do_hashtable_op",u8_mkstring("0x%x",op));
    break;
  }
  ht->fd_modified=1;
  if ((was_achoice==0) && (FD_ACHOICEP(result->fd_keyval))) {
    /* If we didn't have an achoice before and we do now, that means
       a new achoice was created with a mutex and everything.  We can
       safely destroy it and set the choice to not use locking, since
       the value will be protected by the hashtable's lock. */
    struct FD_ACHOICE *ch=FD_XACHOICE(result->fd_keyval);
    if (ch->fd_uselock) ch->fd_uselock=0;}
  return added;
}

FD_EXPORT int fd_hashtable_op
   (struct FD_HASHTABLE *ht,fd_tableop op,fdtype key,fdtype value)
{
  int added;
  if (FD_EMPTY_CHOICEP(key)) return 0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  fd_write_lock_struct(ht);
  added=do_hashtable_op(ht,op,key,value);
  fd_rw_unlock_struct(ht);
  if (FD_EXPECT_FALSE(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_slots < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=fd_get_hashtable_size(hashtable_resize_target(ht));
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_iter
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    const fdtype *keys,const fdtype *values)
{
  int i=0, added=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  fd_write_lock_struct(ht);
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0)
      added=do_hashtable_op(ht,op,keys[i],values[i]);
    else do_hashtable_op(ht,op,keys[i],values[i]);
    i++;}
  fd_rw_unlock_struct(ht);
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
  if (ht->fd_uselock) { fd_write_lock_struct(ht); unlock=1;}
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0)
      added=do_hashtable_op(ht,op,keys[i],value);
    else do_hashtable_op(ht,op,keys[i],value);
    if (added<0) {
      if (unlock) fd_rw_unlock_struct(ht);
      return added;}
    i++;}
  if (unlock) fd_rw_unlock_struct(ht);
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
  if (ht->fd_uselock) { fd_write_lock_struct(ht); unlock=1;}
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0)
      added=do_hashtable_op(ht,op,key,values[i]);
    else do_hashtable_op(ht,op,key,values[i]);
    i++;}
  if (unlock) fd_rw_unlock_struct(ht);
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
  return ptr->fd_n_keys;
}

static int hashtable_modified(struct FD_HASHTABLE *ptr,int flag)
{
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  int modified=ptr->fd_modified;
  if (flag<0)
    return modified;
  else if (flag) {
    ptr->fd_modified=1;
    return modified;}
  else {
    ptr->fd_modified=0;
    return modified;}
}

FD_EXPORT fdtype fd_hashtable_keys(struct FD_HASHTABLE *ptr)
{
  fdtype result=FD_EMPTY_CHOICE; int unlock=0;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  if (ptr->fd_uselock) {fd_read_lock(&ptr->fd_rwlock); unlock=1;}
  {
    struct FD_HASH_BUCKET **scan=ptr->fd_buckets, **lim=scan+ptr->fd_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          if (FD_VOIDP(kvscan->fd_keyval)) {kvscan++;continue;}
          fd_incref(kvscan->fd_kvkey);
          FD_ADD_TO_CHOICE(result,kvscan->fd_kvkey);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) fd_rw_unlock(&ptr->fd_rwlock);
  return fd_simplify_choice(result);
}

FD_EXPORT fdtype fd_hashtable_values(struct FD_HASHTABLE *ptr)
{
  struct FD_KEYVAL *scan, *limit; int unlock=0;
  struct FD_ACHOICE *achoice; fdtype results;
  int size, atomic=1;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  if (ptr->fd_uselock) {fd_read_lock(&ptr->fd_rwlock); unlock=1;}
  size=ptr->fd_n_keys;
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_achoice(NULL,17*(size),0);
  achoice=FD_XACHOICE(results);
  {
    struct FD_HASH_BUCKET **scan=ptr->fd_buckets, **lim=scan+ptr->fd_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          fdtype value=kvscan->fd_keyval;
          if ((FD_VOIDP(value))||(FD_EMPTY_CHOICEP(value))) {
            kvscan++; continue;}
          fd_incref(value);
          _achoice_add(achoice,value);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) fd_rw_unlock(&ptr->fd_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_simplify_choice(results);
}

FD_EXPORT fdtype fd_hashtable_assocs(struct FD_HASHTABLE *ptr)
{
  struct FD_KEYVAL *scan, *limit; int unlock=0;
  struct FD_ACHOICE *achoice; fdtype results;
  int size;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  if (ptr->fd_uselock) {fd_read_lock(&ptr->fd_rwlock); unlock=1;}
  size=ptr->fd_n_keys;
  /* Otherwise, copy the keys into a choice vector. */
  results=fd_init_achoice(NULL,17*(size),0);
  achoice=FD_XACHOICE(results);
  {
    struct FD_HASH_BUCKET **scan=ptr->fd_buckets, **lim=scan+ptr->fd_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          fdtype key=kvscan->fd_kvkey, value=kvscan->fd_keyval, assoc;
          if (FD_VOIDP(value)) {
            kvscan++; continue;}
          fd_incref(key); fd_incref(value);
          assoc=fd_init_pair(NULL,key,value);
          _achoice_add(achoice,assoc);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) fd_rw_unlock(&ptr->fd_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_simplify_choice(results);
}

static int free_hashvec
(struct FD_HASH_BUCKET **slots,int slots_to_free)
{
  if ((slots) && (slots_to_free)) {
    struct FD_HASH_BUCKET **scan=slots, **lim=scan+slots_to_free;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          fd_decref(kvscan->fd_kvkey);
          fd_decref(kvscan->fd_keyval);
          kvscan++;}
        u8_free(*scan);
        *scan++=NULL;}
      else scan++;}
  return 0;
}

FD_EXPORT int fd_free_hashvec(struct FD_HASH_BUCKET **slots,int slots_to_free)
{
  return free_hashvec(slots,slots_to_free);
}

FD_EXPORT int fd_reset_hashtable(struct FD_HASHTABLE *ht,int n_slots,int lock)
{
  struct FD_HASH_BUCKET **slots; int slots_to_free=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (n_slots<0) n_slots=ht->fd_n_buckets;
  if ((lock) && (ht->fd_uselock)) fd_write_lock_struct(ht);
  /* Grab the slots and their length. We'll free them after we've reset
     the table and released its lock. */
  slots=ht->fd_buckets; slots_to_free=ht->fd_n_buckets;
  /* Now initialize the structure.  */
  if (n_slots == 0) {
    ht->fd_n_buckets=ht->fd_n_keys=0; ht->fd_load_factor=default_hashtable_loading;
    ht->fd_buckets=NULL;}
  else {
    int i=0; struct FD_HASH_BUCKET **slotvec;
    ht->fd_n_buckets=n_slots; ht->fd_n_keys=0; ht->fd_load_factor=default_hashtable_loading;
    ht->fd_buckets=slotvec=u8_alloc_n(n_slots,struct FD_HASH_BUCKET *);
    while (i < n_slots) slotvec[i++]=NULL;}
  /* Free the lock, letting other processes use this hashtable. */
  if ((lock) && (ht->fd_uselock)) fd_rw_unlock_struct(ht);
  /* Now, free the old data... */
  if (slots_to_free) {
    free_hashvec(slots,slots_to_free);
    u8_free(slots);}
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
  if (n_slots<0) n_slots=ht->fd_n_buckets;
  if ((lock) && (ht->fd_uselock)) fd_write_lock_struct(ht);
  /* Grab the slots and their length. We'll free them after we've reset
     the table and released its lock. */
  *slotsptr=ht->fd_buckets; *slots_to_free=ht->fd_n_buckets;
  /* Now initialize the structure.  */
  if (n_slots == 0) {
    ht->fd_n_buckets=ht->fd_n_keys=0; ht->fd_load_factor=default_hashtable_loading;
    ht->fd_buckets=NULL;}
  else {
    int i=0; struct FD_HASH_BUCKET **slotvec;
    ht->fd_n_buckets=n_slots; ht->fd_n_keys=0; ht->fd_load_factor=default_hashtable_loading;
    ht->fd_buckets=slotvec=u8_alloc_n(n_slots,struct FD_HASH_BUCKET *);
    while (i < n_slots) slotvec[i++]=NULL;}
  /* Free the lock, letting other processes use this hashtable. */
  if ((lock) && (ht->fd_uselock)) fd_rw_unlock_struct(ht);
  return n_slots;
}

FD_EXPORT int fd_persist_hashtable(struct FD_HASHTABLE *ptr,int type)
{
  int n_conversions=0; fd_ptr_type keeptype=(fd_ptr_type)type;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  fd_write_lock(&ptr->fd_rwlock);
  {
    struct FD_HASH_BUCKET **scan=ptr->fd_buckets, **lim=scan+ptr->fd_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          if ((FD_CONSP(kvscan->fd_keyval)) &&
              ((type<0) || (FD_PTR_TYPEP(kvscan->fd_keyval,keeptype)))) {
            fdtype ppval=fd_pptr_register(kvscan->fd_keyval);
            fd_decref(kvscan->fd_keyval); n_conversions++;
            kvscan->fd_keyval=ppval;}
          kvscan++;}
        scan++;}
      else scan++;}
  fd_rw_unlock(&ptr->fd_rwlock);
  return n_conversions;
}

FD_EXPORT fdtype fd_make_hashtable(struct FD_HASHTABLE *ptr,int n_slots)
{
  if (n_slots == 0) {
    if (ptr == NULL) {
      ptr=u8_alloc(struct FD_HASHTABLE);
      FD_INIT_FRESH_CONS(ptr,fd_hashtable_type);}
#if FD_THREADS_ENABLED
    fd_init_rwlock(&(ptr->fd_rwlock));
#endif
    ptr->fd_n_buckets=ptr->fd_n_keys=0;
    ptr->fd_readonly=ptr->fd_modified=0; ptr->fd_uselock=1;
    ptr->fd_load_factor=default_hashtable_loading;
    ptr->fd_buckets=NULL;
    return FDTYPE_CONS(ptr);}
  else {
    int i=0; struct FD_HASH_BUCKET **slots;
    if (ptr == NULL) {
      ptr=u8_alloc(struct FD_HASHTABLE);
      FD_INIT_FRESH_CONS(ptr,fd_hashtable_type);}
#if FD_THREADS_ENABLED
    fd_init_rwlock(&(ptr->fd_rwlock));
#endif
    if (n_slots < 0) n_slots=-n_slots;
    else n_slots=fd_get_hashtable_size(n_slots);
    ptr->fd_readonly=ptr->fd_modified=0; ptr->fd_uselock=1;
    ptr->fd_n_buckets=n_slots; ptr->fd_n_keys=0;
    ptr->fd_load_factor=default_hashtable_loading;
    ptr->fd_buckets=slots=u8_alloc_n(n_slots,struct FD_HASH_BUCKET *);
    while (i < n_slots) slots[i++]=NULL;
    return FDTYPE_CONS(ptr);}
}

/* Note that this does not incref the values passed to it. */
FD_EXPORT fdtype fd_init_hashtable(struct FD_HASHTABLE *ptr,int fd_n_entries,
                                    struct FD_KEYVAL *inits)
{
  int i=0, n_slots=fd_get_hashtable_size(fd_n_entries*2), n_keys=0;
  struct FD_HASH_BUCKET **slots;
  if (ptr == NULL) {
    ptr=u8_alloc(struct FD_HASHTABLE);
    FD_INIT_FRESH_CONS(ptr,fd_hashtable_type);}
  ptr->fd_n_buckets=n_slots; ptr->fd_n_keys=fd_n_entries;
  ptr->fd_load_factor=default_hashtable_loading;
  ptr->fd_modified=0; ptr->fd_readonly=0; ptr->fd_uselock=1;
  ptr->fd_buckets=slots=u8_alloc_n(n_slots,struct FD_HASH_BUCKET *);
  memset(slots,0,sizeof(struct FD_HASH_BUCKET *)*n_slots);
  i=0; while (i<fd_n_entries) {
    struct FD_KEYVAL *ki=&(inits[i]);
    struct FD_KEYVAL *hv=fd_hashvec_insert(ki->fd_kvkey,slots,n_slots,&n_keys);
    hv->fd_keyval=fd_incref(ki->fd_keyval); i++;}
#if FD_THREADS_ENABLED
  ptr->fd_uselock=1;
  fd_init_rwlock(&(ptr->fd_rwlock));
#else
  ptr->fd_uselock=1;
#endif
  return FDTYPE_CONS(ptr);
}

FD_EXPORT int fd_resize_hashtable(struct FD_HASHTABLE *ptr,int n_slots)
{
  int unlock=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if (ptr->fd_uselock) { fd_write_lock_struct(ptr); unlock=1; }
  {
    struct FD_HASH_BUCKET **new_slots=u8_alloc_n(n_slots,fd_hash_bucket);
    struct FD_HASH_BUCKET **scan=ptr->fd_buckets, **lim=scan+ptr->fd_n_buckets;
    struct FD_HASH_BUCKET **nscan=new_slots, **nlim=nscan+n_slots;
    while (nscan<nlim) *nscan++=NULL;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan++; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          struct FD_KEYVAL *nkv=fd_hashvec_insert(kvscan->fd_kvkey,new_slots,n_slots,NULL);
          nkv->fd_keyval=kvscan->fd_keyval; kvscan->fd_keyval=FD_VOID;
          fd_decref(kvscan->fd_kvkey); kvscan++;}
        u8_free(e);}
      else scan++;
    u8_free(ptr->fd_buckets);
    ptr->fd_n_buckets=n_slots; ptr->fd_buckets=new_slots;}
  if (unlock) fd_rw_unlock_struct(ptr);
  return n_slots;
}

/* VOIDs all values which only have one incoming pointer (from the table
   itself).  This is helpful in conjunction with fd_devoid_hashtable, which
   will then reduce the table to remove such entries. */
FD_EXPORT int fd_remove_deadwood_x(struct FD_HASHTABLE *ptr,
                                   int (*testfn)(fdtype,fdtype,void *),
                                   void *testdata)
{
  struct FD_HASH_BUCKET **scan, **lim;
  int n_slots=ptr->fd_n_buckets, n_keys=ptr->fd_n_keys; int unlock=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if ((n_slots == 0) || (n_keys == 0)) return 0;
  if (ptr->fd_uselock) { fd_write_lock_struct(ptr); unlock=1;}
  while (1) {
    int n_cleared=0;
    scan=ptr->fd_buckets; lim=scan+ptr->fd_n_buckets;
    while (scan < lim) {
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan++; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        if ((testfn)&&(testfn(kvscan->fd_kvkey,FD_VOID,testdata))) {}
        else while (kvscan<kvlimit) {
            fdtype val=kvscan->fd_keyval;
            if (FD_CONSP(val)) {
              struct FD_CONS *cval=(struct FD_CONS *)val;
              if (FD_ACHOICEP(val))
                cval=(struct FD_CONS *)
                  (val=kvscan->fd_keyval=fd_simplify_choice(val));
              if (FD_CONS_REFCOUNT(cval)==1) {
                n_cleared++; kvscan->fd_keyval=FD_VOID; fd_decref(val);}}
            /* ??? In the future, this should probably scan CHOICES
               to remove deadwood as well.  */
            kvscan++;}}
      else scan++;}
    if (n_cleared) n_cleared=0; else break;}
  if (unlock) fd_rw_unlock_struct(ptr);
  return n_slots;
}
FD_EXPORT int fd_remove_deadwood(struct FD_HASHTABLE *ptr)
{
  return fd_remove_deadwood_x(ptr,NULL,NULL);
}


FD_EXPORT int fd_devoid_hashtable(struct FD_HASHTABLE *ptr)
{
  int n_slots=ptr->fd_n_buckets, n_keys=ptr->fd_n_keys; int unlock=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if ((n_slots == 0) || (n_keys == 0)) return 0;
  if (ptr->fd_uselock) { fd_write_lock_struct(ptr); unlock=1;}
  {
    struct FD_HASH_BUCKET **new_slots=u8_alloc_n(n_slots,fd_hash_bucket);
    struct FD_HASH_BUCKET **scan=ptr->fd_buckets, **lim=scan+ptr->fd_n_buckets;
    struct FD_HASH_BUCKET **nscan=new_slots, **nlim=nscan+n_slots;
    int remaining_keys=0;
    if (new_slots==NULL) {
      if (unlock) fd_rw_unlock_struct(ptr);
      return -1;}
    while (nscan<nlim) *nscan++=NULL;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan++; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit)
          if (FD_VOIDP(kvscan->fd_keyval)) {
            fd_decref(kvscan->fd_kvkey); kvscan++;}
          else {
            struct FD_KEYVAL *nkv=fd_hashvec_insert(kvscan->fd_kvkey,new_slots,n_slots,NULL);
            nkv->fd_keyval=kvscan->fd_keyval; kvscan->fd_keyval=FD_VOID;
            fd_decref(kvscan->fd_kvkey); kvscan++; remaining_keys++;}
        u8_free(e);}
      else scan++;
    u8_free(ptr->fd_buckets);
    ptr->fd_n_buckets=n_slots; ptr->fd_buckets=new_slots;
    ptr->fd_n_keys=remaining_keys;}
  if (unlock) fd_rw_unlock_struct(ptr);
  return n_slots;
}

FD_EXPORT int fd_hashtable_stats
  (struct FD_HASHTABLE *ptr,
   int *n_slotsp,int *n_keysp,int *n_bucketsp,int *n_collisionsp,
   int *max_bucketp,int *n_valsp,int *max_valsp)
{
  int n_slots=ptr->fd_n_buckets, n_keys=0, unlock=0;
  int n_buckets=0, max_bucket=0, n_collisions=0;
  int n_vals=0, max_vals=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if (ptr->fd_uselock) { fd_read_lock_struct(ptr); unlock=1;}
  {
    struct FD_HASH_BUCKET **scan=ptr->fd_buckets, **lim=scan+ptr->fd_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        n_buckets++; n_keys=n_keys+fd_n_entries;
        if (fd_n_entries>max_bucket) max_bucket=fd_n_entries;
        if (fd_n_entries>1)
          n_collisions++;
        if ((n_valsp) || (max_valsp)) {
          struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
          while (kvscan<kvlimit) {
            fdtype val=kvscan->fd_keyval; int valcount;
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
  if (unlock) fd_rw_unlock_struct(ptr);
  return n_keys;
}

FD_EXPORT fdtype fd_copy_hashtable(FD_HASHTABLE *nptr,FD_HASHTABLE *ptr)
{
  int n_slots=0, unlock=0;
  struct FD_HASH_BUCKET **slots, **read, **write, **read_limit;
  if (nptr==NULL) nptr=u8_alloc(struct FD_HASHTABLE);
  if (ptr->fd_uselock) { fd_read_lock_struct(ptr); unlock=1;}
  FD_INIT_CONS(nptr,fd_hashtable_type);
  nptr->fd_modified=0; nptr->fd_readonly=0; nptr->fd_uselock=1;
  nptr->fd_n_buckets=n_slots=ptr->fd_n_buckets;;
  nptr->fd_n_keys=ptr->fd_n_keys;
  read=slots=ptr->fd_buckets;
  read_limit=read+n_slots;
  nptr->fd_load_factor=ptr->fd_load_factor;
  write=nptr->fd_buckets=u8_alloc_n(n_slots,fd_hash_bucket);
  memset(write,0,sizeof(struct FD_HASH_BUCKET *)*n_slots);
  while (read<read_limit)
    if (*read==NULL) {read++; write++;}
    else {
      struct FD_KEYVAL *kvread, *kvwrite, *kvlimit;
      struct FD_HASH_BUCKET *he=*read++, *newhe; int n=he->fd_n_entries;
      *write++=newhe=(struct FD_HASH_BUCKET *)
        u8_malloc(sizeof(struct FD_HASH_BUCKET)+(n-1)*sizeof(struct FD_KEYVAL));
      kvread=&(he->fd_keyval0); kvwrite=&(newhe->fd_keyval0);
      newhe->fd_n_entries=n; kvlimit=kvread+n;
      while (kvread<kvlimit) {
        fdtype key=kvread->fd_kvkey, val=kvread->fd_keyval; kvread++;
        if (FD_CONSP(key)) kvwrite->fd_kvkey=fd_copy(key);
        else kvwrite->fd_kvkey=key;
        if (FD_CONSP(val))
          if (FD_ACHOICEP(val))
            kvwrite->fd_keyval=fd_make_simple_choice(val);
          else if (FD_STACK_CONSED(val))
            kvwrite->fd_keyval=fd_copy(val);
          else {fd_incref(val); kvwrite->fd_keyval=val;}
        else kvwrite->fd_keyval=val;
        kvwrite++;}}
  if (unlock) fd_rw_unlock_struct(ptr);
#if FD_THREADS_ENABLED
  fd_init_rwlock(&(nptr->fd_rwlock));
#endif
  return FDTYPE_CONS(nptr);
}

static fdtype copy_hashtable(fdtype table,int deep)
{
  struct FD_HASHTABLE *ptr=FD_GET_CONS(table,fd_hashtable_type,fd_hashtable);
  struct FD_HASHTABLE *nptr=u8_alloc(struct FD_HASHTABLE);
  return fd_copy_hashtable(nptr,ptr);
}

static fdtype copy_hashset(fdtype table,int deep)
{
  struct FD_HASHSET *ptr=FD_GET_CONS(table,fd_hashset_type,fd_hashset);
  struct FD_HASHSET *nptr=u8_alloc(struct FD_HASHSET);
  return fd_copy_hashset(nptr,ptr);
}

static int unparse_hashtable(u8_output out,fdtype x)
{
  struct FD_HASHTABLE *ht=FD_XHASHTABLE(x); char buf[128];
  sprintf(buf,"#<HASHTABLE %d/%d>",ht->fd_n_keys,ht->fd_n_buckets);
  u8_puts(out,buf);
  return 1;
}

FD_EXPORT int fd_recycle_hashtable(struct FD_HASHTABLE *c)
{
  struct FD_HASHTABLE *ht=(struct FD_HASHTABLE *)c;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  fd_write_lock_struct(ht);
  if (ht->fd_n_buckets==0) {
    fd_rw_unlock_struct(ht);
    fd_destroy_rwlock(&(ht->fd_rwlock));
    if (!(FD_STACK_CONSP(ht))) u8_free(ht);
    return 0;}
  if (ht->fd_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->fd_buckets, **lim=scan+ht->fd_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          fd_decref(kvscan->fd_kvkey);
          fd_decref(kvscan->fd_keyval);
          kvscan++;}
        u8_free(*scan);
        *scan++=NULL;}
      else scan++;
    u8_free(ht->fd_buckets);}
  ht->fd_buckets=NULL; ht->fd_n_buckets=0; ht->fd_n_keys=0;
  fd_rw_unlock_struct(ht);
  fd_destroy_rwlock(&(ht->fd_rwlock));
  if (!(FD_STACK_CONSP(ht))) u8_free(ht);
  return 0;
}

FD_EXPORT struct FD_KEYVAL *fd_hashtable_keyvals
   (struct FD_HASHTABLE *ht,int *sizep,int lock)
{
  struct FD_KEYVAL *results, *rscan; int unlock=0;
  if ((FD_CONS_TYPE(ht)) != fd_hashtable_type) {
    fd_seterr(fd_TypeError,"hashtable",NULL,(fdtype)ht);
    return NULL;}
  if (ht->fd_n_keys == 0) {*sizep=0; return NULL;}
  if ((lock)&&(ht->fd_uselock)) {
      fd_read_lock_struct(ht);
      unlock=1;}
  if (ht->fd_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->fd_buckets, **lim=scan+ht->fd_n_buckets;
    rscan=results=u8_alloc_n(ht->fd_n_keys,struct FD_KEYVAL);
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; int fd_n_entries=e->fd_n_entries;
        struct FD_KEYVAL *kvscan=&(e->fd_keyval0), *kvlimit=kvscan+fd_n_entries;
        while (kvscan<kvlimit) {
          rscan->fd_kvkey=fd_incref(kvscan->fd_kvkey);
          rscan->fd_keyval=fd_incref(kvscan->fd_keyval);
          rscan++; kvscan++;}
        scan++;}
      else scan++;
    *sizep=ht->fd_n_keys;}
  else {*sizep=0; results=NULL;}
  if (unlock) fd_rw_unlock_struct(ht);
  return results;
}

FD_EXPORT int fd_for_hashtable
  (struct FD_HASHTABLE *ht,fd_keyvalfn f,void *data,int lock)
{
  int unlock=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->fd_n_keys == 0) return 0;
  if ((lock)&&(ht->fd_uselock)) {fd_read_lock_struct(ht); unlock=1;}
  if (ht->fd_n_buckets) {
    struct FD_HASH_BUCKET **scan=ht->fd_buckets, **lim=scan+ht->fd_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct FD_HASH_BUCKET *e=*scan; 
        int n_entries=e->fd_n_entries;
        const struct FD_KEYVAL *kvscan=&(e->fd_keyval0);
        const struct FD_KEYVAL *kvlimit=kvscan+n_entries;
        while (kvscan<kvlimit) {
          if (f(kvscan->fd_kvkey,kvscan->fd_keyval,data)) {
            if (lock) fd_rw_unlock_struct(ht);
            return ht->fd_n_buckets;}
          else kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) fd_rw_unlock_struct(ht);
  return ht->fd_n_buckets;
}

/* Hashsets */

FD_EXPORT void fd_init_hashset(struct FD_HASHSET *hashset,int size,int stack_cons)
{
  fdtype *slots;
  int i=0, n_slots=fd_get_hashtable_size(size);
  if (stack_cons) {
    FD_INIT_STATIC_CONS(hashset,fd_hashset_type);}
  else {FD_INIT_CONS(hashset,fd_hashset_type);}
  hashset->fd_n_slots=n_slots; hashset->fd_n_keys=0; hashset->fd_modified=0;
  hashset->fd_allatomic=1; hashset->fd_load_factor=default_hashset_loading;
  hashset->fd_hashslots=slots=u8_alloc_n(n_slots,fdtype);
  while (i < n_slots) slots[i++]=0;
  fd_init_mutex(&(hashset->fd_lock));
  return;
}

FD_EXPORT fdtype fd_make_hashset()
{
  struct FD_HASHSET *h=u8_alloc(struct FD_HASHSET);
  fd_init_hashset(h,17,FD_MALLOCD_CONS);
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
  if (h->fd_n_keys==0) return 0;
  fd_lock_struct(h);
  slots=h->fd_hashslots;
  probe=hashset_get_slot(key,(const fdtype *)slots,h->fd_n_slots);
  if (probe>=0) {
    if (slots[probe]) exists=1; else exists=0;}
  fd_unlock_struct(h);
  if (probe<0) {
    fd_seterr(HashsetOverflow,"fd_hashset_get",NULL,(fdtype)h);
    return -1;}
  else return exists;
}

FD_EXPORT fdtype fd_hashset_elts(struct FD_HASHSET *h,int clean)
{
  FD_CHECK_TYPE_RETDTYPE(h,fd_hashset_type);
  if (h->fd_n_keys==0) return FD_EMPTY_CHOICE;
  else {
    fd_lock_struct(h);
    if (h->fd_n_keys==1) {
      fdtype *scan=h->fd_hashslots, *limit=scan+h->fd_n_slots;
      while (scan<limit)
        if (*scan)
          if (clean) {
            fdtype v=*scan;
            u8_free(h->fd_hashslots);
            fd_unlock_struct(h);
            if (FD_VOIDP(v))
              return FD_EMPTY_CHOICE;
            else return v;}
          else {
            fdtype v=fd_incref(*scan);
            fd_unlock_struct(h);
            if (FD_VOIDP(v))
              return FD_EMPTY_CHOICE;
            else return v;}
        else scan++;
      fd_unlock_struct(h);
      return FD_VOID;}
    else {
      int n=h->fd_n_keys, atomicp=1;
      struct FD_CHOICE *new_choice=fd_alloc_choice(n);
      const fdtype *scan=h->fd_hashslots, *limit=scan+h->fd_n_slots;
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
          u8_free(h->fd_hashslots); h->fd_n_slots=h->fd_n_keys=0;}}
      fd_unlock_struct(h);
      return fd_init_choice(new_choice,write-base,base,
                            (FD_CHOICE_DOSORT|
                             ((atomicp)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES))|
                             FD_CHOICE_REALLOC));}}
}

static int hashset_getsize(struct FD_HASHSET *h)
{
  FD_CHECK_TYPE_RETDTYPE(h,fd_hashset_type);
  return h->fd_n_keys;
}

static int hashset_modified(struct FD_HASHSET *ptr,int flag)
{
  FD_CHECK_TYPE_RET(ptr,fd_hashset_type);
  int modified=ptr->fd_modified;
  if (flag<0)
    return modified;
  else if (flag) {
    ptr->fd_modified=1;
    return modified;}
  else {
    ptr->fd_modified=0;
    return modified;}
}

static fdtype hashsetelts(struct FD_HASHSET *h)
{
  return fd_hashset_elts(h,0);
}

static int grow_hashset(struct FD_HASHSET *h)
{
  int i=0, lim=h->fd_n_slots;
  int new_size=fd_get_hashtable_size(hashset_resize_target(h));
  const fdtype *slots=h->fd_hashslots;
  fdtype *newslots=u8_alloc_n(new_size,fdtype);
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
  u8_free(h->fd_hashslots); h->fd_hashslots=newslots; h->fd_n_slots=new_size;
  return 1;
}

FD_EXPORT ssize_t fd_grow_hashset(struct FD_HASHSET *h,size_t target)
{
  int new_size=fd_get_hashtable_size(target);
  u8_lock_mutex(&(h->fd_lock)); {
    int i=0, lim=h->fd_n_slots;
    const fdtype *slots=h->fd_hashslots;
    fdtype *newslots=u8_alloc_n(new_size,fdtype);
    while (i<new_size) newslots[i++]=FD_NULL;
    i=0; while (i < lim)
           if (slots[i]==0) i++;
           else if (FD_VOIDP(slots[i])) i++;
           else {
             int off=hashset_get_slot(slots[i],newslots,new_size);
             if (off<0) {
               u8_free(newslots);
               u8_unlock_mutex(&(h->fd_lock));
               return -1;}
             newslots[off]=slots[i]; i++;}
    u8_free(h->fd_hashslots); h->fd_hashslots=newslots; h->fd_n_slots=new_size;
    u8_unlock_mutex(&(h->fd_lock));}
  return new_size;
}

FD_EXPORT int fd_hashset_mod(struct FD_HASHSET *h,fdtype key,int add)
{
  int probe; fdtype *slots;
  fd_lock_struct(h);
  slots=h->fd_hashslots;
  probe=hashset_get_slot(key,h->fd_hashslots,h->fd_n_slots);
  if (probe < 0) {
    fd_unlock_struct(h);
    fd_seterr(HashsetOverflow,"fd_hashset_mod",NULL,(fdtype)h);
    return -1;}
  else if (FD_NULLP(slots[probe]))
    if (add) {
      slots[probe]=fd_incref(key); 
      h->fd_n_keys++; h->fd_modified=1;
      if (FD_CONSP(key)) h->fd_allatomic=0;
      if (hashset_needs_resizep(h))
        grow_hashset(h);
      fd_unlock_struct(h);
      return 1;}
    else {
      fd_unlock_struct(h);
      return 0;}
  else if (add) {
    fd_unlock_struct(h);
    return 0;}
  else {
    fd_decref(slots[probe]); slots[probe]=FD_VOID;
    h->fd_n_keys++; h->fd_modified=1;
    fd_unlock_struct(h);
    return 1;}
}

/* This adds without locking or incref. */
FD_EXPORT int fd_hashset_add_raw(struct FD_HASHSET *h,fdtype key)
{
  int probe; fdtype *slots;
  slots=h->fd_hashslots;
  probe=hashset_get_slot(key,h->fd_hashslots,h->fd_n_slots);
  if (probe < 0) {
    fd_seterr(HashsetOverflow,"fd_hashset_mod",NULL,(fdtype)h);
    return -1;}
  else if (FD_NULLP(slots[probe])) {
    slots[probe]=key; h->fd_n_keys++; h->fd_modified=1;
    if (FD_CONSP(key)) h->fd_allatomic=0;
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
    size_t need_size=n_vals*3+h->fd_n_keys, n_adds=0;
    if (need_size>h->fd_n_slots) fd_grow_hashset(h,need_size);
    u8_lock_mutex(&(h->fd_lock)); {
      fdtype *slots=h->fd_hashslots; int n_slots=h->fd_n_slots;
      {FD_DO_CHOICES(key,keys) {
          int probe=hashset_get_slot(key,slots,n_slots);
          if (probe < 0) {
            fd_seterr(HashsetOverflow,"fd_hashset_add",NULL,(fdtype)h);
            FD_STOP_DO_CHOICES;
            u8_unlock_mutex(&(h->fd_lock));
            return -1;}
          else if ((FD_NULLP(slots[probe]))||(FD_VOIDP(slots[probe]))) {
            slots[probe]=key; fd_incref(key);
            h->fd_modified=1; h->fd_n_keys++; n_adds++;
            if (FD_CONSP(key)) h->fd_allatomic=0;
            if (FD_EXPECT_FALSE(hashset_needs_resizep(h))) {
              grow_hashset(h);
              slots=h->fd_hashslots;
              n_slots=h->fd_n_slots;}}
          else {}}}
      u8_unlock_mutex(&(h->fd_lock));
      return n_adds;}}
  else if (FD_EMPTY_CHOICEP(keys)) return 0;
  else return fd_hashset_mod(h,keys,1);
}

FD_EXPORT int fd_recycle_hashset(struct FD_HASHSET *h)
{
  fd_lock_struct(h);
  if (h->fd_allatomic==0) {
    fdtype *scan=h->fd_hashslots, *lim=scan+h->fd_n_slots;
    while (scan<lim)
      if (FD_NULLP(*scan)) scan++;
      else {
        fdtype v=*scan++; fd_decref(v);}}
  u8_free(h->fd_hashslots);
  fd_unlock_struct(h);
  fd_destroy_mutex(&(h->fd_lock));
  if (!(FD_STACK_CONSP(h))) u8_free(h);
  return 1;
}

FD_EXPORT fdtype fd_copy_hashset(struct FD_HASHSET *hnew,struct FD_HASHSET *h)
{

  fdtype *newslots, *write, *read, *lim;
  if (hnew==NULL) hnew=u8_alloc(struct FD_HASHSET);
  fd_lock_struct(h);
  read=h->fd_hashslots; lim=read+h->fd_n_slots;
  write=newslots=u8_alloc_n(h->fd_n_slots,fdtype);
  if (h->fd_allatomic)
    while (read<lim) *write++=*read++;
  else while (read<lim) {
      fdtype v=*read++; if (v) fd_incref(v); *write++=v;}
  FD_INIT_CONS(hnew,fd_hashset_type);
  hnew->fd_n_slots=h->fd_n_slots; hnew->fd_n_keys=h->fd_n_keys;
  hnew->fd_hashslots=newslots; hnew->fd_allatomic=h->fd_allatomic;
  hnew->fd_load_factor=h->fd_load_factor; hnew->fd_modified=0;
  fd_unlock_struct(h);
  fd_init_mutex((&hnew->fd_lock));
  return (fdtype) hnew;
}

static int unparse_hashset(u8_output out,fdtype x)
{
  struct FD_HASHSET *hs=((struct FD_HASHSET *)x); char buf[128];
  sprintf(buf,"#<HASHSET%s %d/%d>",
          ((hs->fd_modified)?("(m)"):("")),
          hs->fd_n_keys,hs->fd_n_slots);
  u8_puts(out,buf);
  return 1;
}

static fdtype hashsetget(fdtype x,fdtype key)
{
  struct FD_HASHSET *h=FD_GET_CONS(x,fd_hashset_type,struct FD_HASHSET *);
  if (fd_hashset_get(h,key)) return FD_TRUE;
  else return FD_FALSE;
}
static int hashsetstore(fdtype x,fdtype key,fdtype val)
{
  struct FD_HASHSET *h=FD_GET_CONS(x,fd_hashset_type,struct FD_HASHSET *);
  if (FD_TRUEP(val)) return fd_hashset_mod(h,key,1);
  else if (FD_FALSEP(val)) return fd_hashset_mod(h,key,0);
  else {
    fd_seterr(fd_RangeError,_("value is not a boolean"),NULL,val);
    return -1;}
}

/* Generic table functions */

struct FD_TABLEFNS *fd_tablefns[FD_TYPE_MAX];

FD_EXPORT fdtype fd_get(fdtype arg,fdtype key,fdtype dflt)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (FD_EMPTY_CHOICEP(arg)) return arg;
  else if (FD_VALID_TYPEP(argtype))
    if (FD_EXPECT_TRUE(fd_tablefns[argtype]!=NULL))
      if (FD_EXPECT_TRUE(fd_tablefns[argtype]->get!=NULL))
        if (FD_CHOICEP(key)) {
          fdtype results=FD_EMPTY_CHOICE;
          fdtype (*getfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->get;
          FD_DO_CHOICES(each,key) {
            fdtype values=getfn(arg,each,FD_EMPTY_CHOICE);
            if (FD_ABORTP(values)) {
              fd_decref(results); return values;}
            FD_ADD_TO_CHOICE(results,values);}
          if (FD_EMPTY_CHOICEP(results)) return fd_incref(dflt);
          else return results;}
        else return (fd_tablefns[argtype]->get)(arg,key,dflt);
      else return fd_err(fd_NoMethod,CantGet,NULL,arg);
    else return fd_err(NotATable,"fd_get",NULL,arg);
  else return fd_err(fd_BadPtr,"fd_get",NULL,arg);
}

FD_EXPORT int fd_store(fdtype arg,fdtype key,fdtype value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (FD_VALID_TYPEP(argtype))
    if (FD_EXPECT_TRUE(fd_tablefns[argtype]!=NULL))
      if (FD_EXPECT_TRUE(fd_tablefns[argtype]->store!=NULL))
        if (FD_CHOICEP(key)) {
          int (*storefn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->store;
          FD_DO_CHOICES(each,key) {
            int retval=storefn(arg,each,value);
            if (retval<0) return retval;}
          return 1;}
        else return (fd_tablefns[argtype]->store)(arg,key,value);
      else return fd_reterr(fd_NoMethod,CantSet,NULL,arg);
    else return fd_reterr(NotATable,"fd_store",NULL,arg);
  else return fd_reterr(fd_BadPtr,"fd_store",NULL,arg);
}

FD_EXPORT int fd_add(fdtype arg,fdtype key,fdtype value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (FD_VALID_TYPEP(argtype))
    if (FD_EXPECT_TRUE(fd_tablefns[argtype]!=NULL))
      if (FD_EXPECT_TRUE(fd_tablefns[argtype]->add!=NULL))
        if (FD_EXPECT_FALSE((FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_CHOICEP(key))))
          return 0;
        else if (FD_CHOICEP(key)) {
          int (*addfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->add;
          FD_DO_CHOICES(each,key) {
            int retval=addfn(arg,each,value);
            if (retval<0) return retval;}
          return 1;}
        else return (fd_tablefns[argtype]->add)(arg,key,value);
      else if ((fd_tablefns[argtype]->store) &&
               (fd_tablefns[argtype]->get))
        if (FD_EXPECT_FALSE((FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_CHOICEP(key))))
          return 0;
        else {
          int (*storefn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->store;
          fdtype (*getfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->get;
          FD_DO_CHOICES(each,key) {
            fdtype values=getfn(arg,each,FD_EMPTY_CHOICE);
            fdtype svalues;
            fd_incref(value);
            FD_ADD_TO_CHOICE(values,value);
            svalues=fd_make_simple_choice(values);
            storefn(arg,each,svalues);
            fd_decref(values); fd_decref(svalues);}
          return 1;}
      else return fd_reterr(fd_NoMethod,CantAdd,NULL,arg);
    else return fd_reterr(NotATable,"fd_add",NULL,arg);
  else return fd_reterr(fd_BadPtr,"fd_add",NULL,arg);
}

FD_EXPORT int fd_drop(fdtype arg,fdtype key,fdtype value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (FD_EMPTY_CHOICEP(arg)) return 0;
  if (FD_VALID_TYPEP(argtype))
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
        if (FD_EXPECT_FALSE
            ((FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_CHOICEP(key))))
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
  if (FD_EMPTY_CHOICEP(arg)) return 0;
  if (FD_VALID_TYPEP(argtype))
    if (FD_EXPECT_TRUE(fd_tablefns[argtype]!=NULL))
      if (FD_EXPECT_TRUE(fd_tablefns[argtype]->test!=NULL))
        if (FD_EXPECT_FALSE
            ((FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_CHOICEP(key))))
          return 0;
        else if (FD_CHOICEP(key)) {
          int (*testfn)(fdtype,fdtype,fdtype)=fd_tablefns[argtype]->test;
          FD_DO_CHOICES(each,key)
            if (testfn(arg,each,value)) return 1;
          return 0;}
        else return (fd_tablefns[argtype]->test)(arg,key,value);
      else if (fd_tablefns[argtype]->get)
        if (FD_EXPECT_FALSE
            ((FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_CHOICEP(key))))
          return 0;
        else {
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
    else return fd_reterr(NotATable,"fd_test",NULL,arg);
  else return fd_reterr(fd_BadPtr,"fd_test",NULL,arg);
}

FD_EXPORT int fd_getsize(fdtype arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (FD_VALID_TYPEP(argtype))
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
  else return fd_err(fd_BadPtr,"fd_getkeys",NULL,arg);
}

FD_EXPORT int fd_modifiedp(fdtype arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (FD_VALID_TYPEP(argtype))
    if (fd_tablefns[argtype])
      if (fd_tablefns[argtype]->modified)
        return (fd_tablefns[argtype]->modified)(arg,-1);
      else return fd_err(fd_NoMethod,CantCheckModified,NULL,arg);
    else return fd_err(NotATable,"fd_modifiedp",NULL,arg);
  else return fd_err(fd_BadPtr,"fd_modifiedp",NULL,arg);
}

FD_EXPORT int fd_set_modified(fdtype arg,int flag)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (FD_VALID_TYPEP(argtype))
    if (fd_tablefns[argtype])
      if (fd_tablefns[argtype]->modified)
        return (fd_tablefns[argtype]->modified)(arg,flag);
      else return fd_err(fd_NoMethod,CantSetModified,NULL,arg);
    else return fd_err(NotATable,"fd_modifiedp",NULL,arg);
  else return fd_err(fd_BadPtr,"fd_modifiedp",NULL,arg);
}

FD_EXPORT fdtype fd_getkeys(fdtype arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (FD_VALID_TYPEP(argtype))
    if (fd_tablefns[argtype])
      if (fd_tablefns[argtype]->keys)
        return (fd_tablefns[argtype]->keys)(arg);
      else return fd_err(fd_NoMethod,CantGetKeys,NULL,arg);
    else return fd_err(NotATable,"fd_getkeys",NULL,arg);
  else return fd_err(fd_BadPtr,"fd_getkeys",NULL,arg);
}

FD_EXPORT fdtype fd_getvalues(fdtype arg)
{
  /* Eventually, these might be fd_tablefns fields */
  if (FD_PRIM_TYPEP(arg,fd_hashtable_type)) 
    return fd_hashtable_values(FD_XHASHTABLE(arg));
  else if (FD_PRIM_TYPEP(arg,fd_slotmap_type))
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
  /* Eventually, these might be fd_tablefns fields */
  if (FD_PRIM_TYPEP(arg,fd_hashtable_type)) 
    return fd_hashtable_assocs(FD_XHASHTABLE(arg));
  else if (FD_PRIM_TYPEP(arg,fd_slotmap_type))
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
  fd_tablefns[fd_hashtable_type]->getsize=(fd_table_getsize_fn)hashtable_getsize;
  fd_tablefns[fd_hashtable_type]->keys=(fd_table_keys_fn)fd_hashtable_keys;
  fd_tablefns[fd_hashtable_type]->modified=(fd_table_modified_fn)hashtable_modified;

  /* SLOTMAP table functions */
  fd_tablefns[fd_slotmap_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_slotmap_type]->get=(fd_table_get_fn)fd_slotmap_get;
  fd_tablefns[fd_slotmap_type]->add=(fd_table_add_fn)fd_slotmap_add;
  fd_tablefns[fd_slotmap_type]->drop=(fd_table_drop_fn)fd_slotmap_drop;
  fd_tablefns[fd_slotmap_type]->store=(fd_table_store_fn)fd_slotmap_store;
  fd_tablefns[fd_slotmap_type]->test=(fd_table_test_fn)fd_slotmap_test;
  fd_tablefns[fd_slotmap_type]->getsize=(fd_table_getsize_fn)slotmap_getsize;
  fd_tablefns[fd_slotmap_type]->keys=(fd_table_keys_fn)fd_slotmap_keys;
  fd_tablefns[fd_slotmap_type]->modified=(fd_table_modified_fn)slotmap_modified;

  /* SCHEMAP table functions */
  fd_tablefns[fd_schemap_type]=u8_zalloc(struct FD_TABLEFNS);
  fd_tablefns[fd_schemap_type]->get=(fd_table_get_fn)fd_schemap_get;
  fd_tablefns[fd_schemap_type]->add=(fd_table_add_fn)fd_schemap_add;
  fd_tablefns[fd_schemap_type]->drop=(fd_table_drop_fn)fd_schemap_drop;
  fd_tablefns[fd_schemap_type]->store=(fd_table_store_fn)fd_schemap_store;
  fd_tablefns[fd_schemap_type]->test=(fd_table_test_fn)fd_schemap_test;
  fd_tablefns[fd_schemap_type]->getsize=(fd_table_getsize_fn)schemap_getsize;
  fd_tablefns[fd_schemap_type]->keys=(fd_table_keys_fn)fd_schemap_keys;
  fd_tablefns[fd_schemap_type]->modified=(fd_table_modified_fn)schemap_modified;

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
  fd_tablefns[fd_hashset_type]->modified=(fd_table_modified_fn)hashset_modified;

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
    e->fd_compound_restorefn=restore_hashtable;}

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
