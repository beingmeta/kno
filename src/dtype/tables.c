/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1

#include "fdb/dtype.h"
#include "fdb/tables.h"

fd_exception fd_NoSuchKey=_("No such key");
static fd_exception HashsetOverflow=_("Hashset Overflow");
static u8_string NotATable=_("Not a table");
static u8_string CantGet=_("Table doesn't support get");
static u8_string CantSet=_("Table doesn't support set");
static u8_string CantAdd=_("Table doesn't support add");
static u8_string CantDrop=_("Table doesn't support drop");
static u8_string CantTest=_("Table doesn't support test");
static u8_string CantGetKeys=_("Table doesn't support getkeys");
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

static void sort_keyvals(struct FD_KEYVAL *v,int n)
{
  unsigned i, j, ln, rn;
  while (n > 1) {
    swap_keyvals(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (v[j].key > v[0].key);
      do ++i; while (i < j && (v[i].key<v[0].key));
      if (i >= j) break; swap_keyvals(&v[i], &v[j]);}
    swap_keyvals(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      sort_keyvals(v, ln); v += j; n = rn;}
    else {sort_keyvals(v + j, rn); n = ln;}}
}

#if (!FD_INLINE_CHOICES)
static int cons_compare(fdtype x,fdtype y)
{
  if (FD_ATOMICP(x))
    if (FD_ATOMICP(y))
      if (x < y) return -1;
      else if (x == y)
	return 0;
      else return 1;
    else return -1;
  else if (FD_ATOMICP(y))
    return 1;
  else return fdtype_compare(x,y,1);
}
#endif

static void cons_sort_keyvals(struct FD_KEYVAL *v,int n)
{
  unsigned i, j, ln, rn;
  while (n > 1) {
    swap_keyvals(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (cons_compare(v[j].key,v[0].key)>0);
      do ++i; while (i < j && ((cons_compare(v[j].key,v[0].key))<0));
      if (i >= j) break; swap_keyvals(&v[i], &v[j]);}
    swap_keyvals(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      cons_sort_keyvals(v, ln); v += j; n = rn;}
    else {cons_sort_keyvals(v + j, rn); n = ln;}}
}

FD_EXPORT struct FD_KEYVAL *_fd_sortvec_get
   (fdtype key,struct FD_KEYVAL *keyvals,int size)
{
  return fd_sortvec_get(key,keyvals,size);
}

FD_EXPORT struct FD_KEYVAL *fd_sortvec_insert
  (fdtype key,struct FD_KEYVAL **kvp,int *sizep)
{
  struct FD_KEYVAL *keyvals=*kvp; int size=*sizep, found=0;
  struct FD_KEYVAL *bottom=keyvals, *top=bottom+size-1;
  struct FD_KEYVAL *limit=bottom+size, *middle=bottom+size/2;
  if (keyvals == NULL) {
    *kvp=keyvals=u8_malloc(sizeof(struct FD_KEYVAL));
    if (keyvals==NULL) return NULL;
    keyvals->key=fd_incref(key);
    keyvals->value=FD_EMPTY_CHOICE;
    if (sizep) *sizep=1;
    return keyvals;}
  if (FD_ATOMICP(key))
    while (top>=bottom) {
      middle=bottom+(top-bottom)/2;
      if (middle>=limit) break;
      else if (key==middle->key) {found=1; break;}
      else if (FD_CONSP(middle->key)) top=middle-1;
      else if (key<middle->key) top=middle-1;
      else bottom=middle+1;}
  else while (top>=bottom) {
    int comparison;
    middle=bottom+(top-bottom)/2;
    if (middle>=limit) break;
    comparison=cons_compare(key,middle->key);
    if (comparison==0) {found=1; break;}
    else if (comparison<0) top=middle-1;
    else bottom=middle+1;}
  if (found) return middle;
  else {
    int mpos=(middle-keyvals), dir=(bottom>middle), ipos=mpos+dir;
    struct FD_KEYVAL *insert_point;
    struct FD_KEYVAL *new_keyvals=
      u8_realloc(keyvals,sizeof(struct FD_KEYVAL)*(size+1));
    if (new_keyvals==NULL) return NULL;
    *kvp=new_keyvals; *sizep=size+1; insert_point=new_keyvals+ipos;
    memmove(insert_point+1,insert_point,
	    sizeof(struct FD_KEYVAL)*(size-ipos));
    insert_point->key=fd_incref(key);
    insert_point->value=FD_EMPTY_CHOICE;
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

FD_EXPORT int fd_slotmap_set(struct FD_SLOTMAP *sm,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int osize, size;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  u8_lock_mutex(&sm->lock);
  size=osize=FD_XSLOTMAP_SIZE(sm);
  result=fd_sortvec_insert(key,&(sm->keyvals),&size);
  if (FD_EXPECT_FALSE(result==NULL)) {
    u8_lock_mutex(&sm->lock);
    fd_seterr(fd_MallocFailed,"fd_slotmap_set",NULL,FD_VOID);
    return -1;}
  fd_decref(result->value); result->value=fd_incref(value);
  FD_XSLOTMAP_MARK_MODIFIED(sm);
  if (osize != size) {
    FD_XSLOTMAP_SET_SIZE(sm,size);
    u8_unlock_mutex(&sm->lock);
    return 1;}
  u8_unlock_mutex(&sm->lock);
  return 0;
}

FD_EXPORT int fd_slotmap_add(struct FD_SLOTMAP *sm,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int size, osize;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  u8_lock_mutex(&sm->lock);
  size=osize=FD_XSLOTMAP_SIZE(sm);
  result=fd_sortvec_insert(key,&(sm->keyvals),&size);
  if (FD_EXPECT_FALSE(result==NULL)) {
    u8_lock_mutex(&sm->lock);
    fd_seterr(fd_MallocFailed,"fd_slotmap_add",NULL,FD_VOID);
    return -1;}
  FD_ADD_TO_CHOICE(result->value,fd_incref(value)); 
  FD_XSLOTMAP_MARK_MODIFIED(sm);
  if (osize != size) {
    FD_XSLOTMAP_SET_SIZE(sm,size);
    u8_unlock_mutex(&sm->lock);
    return 1;}
  u8_unlock_mutex(&sm->lock);
  return 0;
}

FD_EXPORT int fd_slotmap_drop(struct FD_SLOTMAP *sm,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int size;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  u8_lock_mutex(&sm->lock);
  size=FD_XSLOTMAP_SIZE(sm);
  result=fd_sortvec_get(key,sm->keyvals,size);
  if (result) {
    fdtype newval=((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) : (fd_difference(result->value,value)));
    if (newval == result->value) {fd_decref(newval);}
    else {
      FD_XSLOTMAP_MARK_MODIFIED(sm);
      if (FD_EMPTY_CHOICEP(newval)) {
	int entries_to_move=(size-(result-sm->keyvals))-1;
	fd_decref(result->key);
	memmove(result,result+1,entries_to_move*sizeof(struct FD_KEYVAL));
	FD_XSLOTMAP_SET_SIZE(sm,size-1);}
      else {
	fd_decref(result->value); result->value=newval;}}}
  u8_unlock_mutex(&sm->lock);
  if (result) return 1; else return 0;
}

FD_EXPORT int fd_slotmap_delete(struct FD_SLOTMAP *sm,fdtype key)
{
  struct FD_KEYVAL *result; int size;
  FD_CHECK_TYPE_RET(sm,fd_slotmap_type);
  u8_lock_mutex(&sm->lock);
  size=FD_XSLOTMAP_SIZE(sm);
  result=fd_sortvec_get(key,sm->keyvals,size);
  if (result) {
    int entries_to_move=(size-(result-sm->keyvals))-1;
    fd_decref(result->key); fd_decref(result->value);
    memmove(result,result+1,entries_to_move*sizeof(struct FD_KEYVAL));
    FD_XSLOTMAP_MARK_MODIFIED(sm);
    FD_XSLOTMAP_SET_SIZE(sm,size-1);}
  u8_unlock_mutex(&sm->lock);
  if (result) return 1; else return 0;
}

static int slotmap_getsize(struct FD_SLOTMAP *ptr)
{
  FD_CHECK_TYPE_RET(ptr,fd_slotmap_type);
  return FD_XSLOTMAP_SIZE(ptr);
}

FD_EXPORT fdtype fd_slotmap_keys(struct FD_SLOTMAP *sm)
{
  struct FD_KEYVAL *scan, *limit;
  struct FD_CHOICE *result;
  fdtype *write; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  u8_lock_mutex(&sm->lock);
  size=FD_XSLOTMAP_SIZE(sm); scan=sm->keyvals; limit=scan+size;
  if (size==0) {
    u8_unlock_mutex(&sm->lock);
    return FD_EMPTY_CHOICE;}
  else if (size==1) {
    fdtype key=fd_incref(scan->key);
    u8_unlock_mutex(&sm->lock);
    return key;}
  /* Otherwise, copy the keys into a choice vector. */
  result=fd_alloc_choice(size);
  write=(fdtype *)FD_XCHOICE_DATA(result);
  while (scan < limit) *write++=(scan++)->key;
  u8_unlock_mutex(&sm->lock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return fd_init_choice(result,size,NULL,FD_CHOICE_ISATOMIC|FD_CHOICE_REALLOC);
}

FD_EXPORT fdtype fd_slotmap_max
  (struct FD_SLOTMAP *sm,fdtype scope,fdtype *maxvalp)
{
  fdtype maxval=FD_VOID, result=FD_EMPTY_CHOICE;
  struct FD_KEYVAL *scan, *limit; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (FD_EMPTY_CHOICEP(scope)) return result;
  u8_lock_mutex(&sm->lock);
  size=FD_XSLOTMAP_SIZE(sm); scan=sm->keyvals; limit=scan+size;
  while (scan<limit) {
    if ((FD_VOIDP(scope)) || (fd_overlapp(scan->key,scope))) {
      if (FD_EMPTY_CHOICEP(scan->value)) {}
      else if (FD_NUMBERP(scan->value)) {
	if (FD_VOIDP(maxval)) {
	  result=fd_incref(scan->key); maxval=fd_incref(scan->value);}
	else {
	  int cmp=numcompare(scan->value,maxval);
	  if (cmp>0) {
	    fd_decref(result); fd_decref(maxval);
	    result=fd_incref(scan->key);
	    maxval=fd_incref(scan->value);}
	  else if (cmp==0) {
	    FD_ADD_TO_CHOICE(result,fd_incref(scan->key));}}}}
    scan++;}
  u8_unlock_mutex(&sm->lock);
  if ((maxvalp) && (FD_NUMBERP(maxval))) *maxvalp=fd_incref(maxval);
  return result;
}

FD_EXPORT fdtype fd_slotmap_skim(struct FD_SLOTMAP *sm,fdtype maxval,fdtype scope)
{
  fdtype result=FD_EMPTY_CHOICE;
  struct FD_KEYVAL *scan, *limit; int size;
  FD_CHECK_TYPE_RETDTYPE(sm,fd_slotmap_type);
  if (FD_EMPTY_CHOICEP(scope)) return result;
  u8_lock_mutex(&sm->lock);
  size=FD_XSLOTMAP_SIZE(sm); scan=sm->keyvals; limit=scan+size;
  while (scan<limit) {
    if ((FD_VOIDP(scope)) || (fd_overlapp(scan->key,scope)))
      if (FD_NUMBERP(scan->value)) {
	int cmp=numcompare(scan->value,maxval);
	if (cmp>=0) {FD_ADD_TO_CHOICE(result,fd_incref(scan->key));}}
    scan++;}
  u8_unlock_mutex(&sm->lock);
  return result;
}

FD_EXPORT fdtype fd_init_slotmap
  (struct FD_SLOTMAP *ptr,
   int len,struct FD_KEYVAL *data,
   FD_MEMORY_POOL_TYPE *mpool)
{
  if (ptr == NULL) ptr=u8_malloc(sizeof(struct FD_SLOTMAP));
  FD_INIT_CONS(ptr,fd_slotmap_type);
  sort_keyvals(data,len);
  ptr->size=len; ptr->keyvals=data; ptr->mpool=mpool;
#if FD_THREADS_ENABLED
  u8_init_mutex(&(ptr->lock));
#endif
  return FDTYPE_CONS(ptr);
}

static fdtype copy_slotmap(fdtype smap)
{
  struct FD_SLOTMAP *cur=FD_GET_CONS(smap,fd_slotmap_type,fd_slotmap);
  struct FD_SLOTMAP *fresh=u8_malloc(sizeof(struct FD_SLOTMAP));
  u8_lock_mutex(&(cur->lock));
  {
    int n=FD_XSLOTMAP_SIZE(cur);
    struct FD_KEYVAL *read=cur->keyvals, *read_limit=read+n;
    struct FD_KEYVAL *write;
    if (n) write=u8_malloc(sizeof(struct FD_KEYVAL)*n);
    else write=NULL;
    FD_INIT_CONS(fresh,fd_slotmap_type);
    fresh->size=n; fresh->mpool=NULL; fresh->keyvals=write;
    while (read<read_limit) {
      fdtype key=read->key, val=read->value; read++;
      if (FD_CONSP(key)) write->key=fd_copy(key);
      else write->key=key;
      if (FD_CONSP(val))
	if (FD_ACHOICEP(val))
	  write->value=fd_make_simple_choice(val);
	else write->value=fd_copy(val);
      else write->value=val;
      write++;}
    u8_unlock_mutex(&(cur->lock));}
#if FD_THREADS_ENABLED
  u8_init_mutex(&(fresh->lock));
#endif
  return FDTYPE_CONS(fresh);
}

static void recycle_slotmap(struct FD_CONS *c)
{
  struct FD_SLOTMAP *sm=(struct FD_SLOTMAP *)c;
  u8_lock_mutex(&(sm->lock));
  {
    int slotmap_size=FD_XSLOTMAP_SIZE(sm);
    const struct FD_KEYVAL *scan=sm->keyvals, *limit=sm->keyvals+slotmap_size;
    FD_MEMORY_POOL_TYPE *mpool=sm->mpool;
    while (scan < limit) {
      fd_decref(scan->key); fd_decref(scan->value); scan++;}
    u8_pfree_x(mpool,sm->keyvals,sizeof(struct FD_KEYVAL)*size);
    u8_unlock_mutex(&(sm->lock));
    u8_destroy_mutex(&(sm->lock));
    u8_pfree_x(mpool,sm,sizeof(struct FD_SLOTMAP));    
  }
}
static int unparse_slotmap(u8_output out,fdtype x)
{
  struct FD_SLOTMAP *sm=FD_XSLOTMAP(x);
  u8_lock_mutex(&(sm->lock));
  {
    int slotmap_size=FD_XSLOTMAP_SIZE(sm);
    struct FD_KEYVAL *scan=sm->keyvals, *limit=sm->keyvals+slotmap_size;
    u8_puts(out,"#[");
    if (scan<limit) {
      fd_unparse(out,scan->key); u8_sputc(out,' ');
      fd_unparse(out,scan->value); scan++;}
    while (scan< limit) {
      u8_sputc(out,' ');
      fd_unparse(out,scan->key); u8_sputc(out,' ');
      fd_unparse(out,scan->value); scan++;}
    u8_puts(out,"]");
  }
  u8_unlock_mutex(&(sm->lock));
  return 1;
}
static int compare_slotmaps(fdtype x,fdtype y,int quick)
{
  int result=0;
  struct FD_SLOTMAP *smx=(struct FD_SLOTMAP *)x;
  struct FD_SLOTMAP *smy=(struct FD_SLOTMAP *)y;
  u8_lock_mutex(&(smx->lock)); u8_lock_mutex(&(smy->lock));
  {
    int xsize=FD_XSLOTMAP_SIZE(smx), ysize=FD_XSLOTMAP_SIZE(smy);
    if (xsize>ysize) result=1;
    else if (xsize<ysize) result=-1;
    else {
      const struct FD_KEYVAL *xkeyvals=smx->keyvals, *ykeyvals=smy->keyvals;
      int i=0; while (i < xsize) {
	int cmp=FD_QCOMPARE(xkeyvals[i].key,ykeyvals[i].key);
	if (cmp) {result=cmp; break;}
	else i++;}
      if (result==0) {
	i=0; while (i < xsize) {
	int cmp=FD_QCOMPARE(xkeyvals[i].value,ykeyvals[i].value);
	if (cmp) {result=cmp; break;}
	else i++;}}}
  }
  u8_unlock_mutex(&(smx->lock)); u8_unlock_mutex(&(smy->lock));
  return result;
}

/* Schema maps */

FD_EXPORT fdtype fd_make_schemap
(struct FD_SCHEMAP *ptr,short size,short flags,
   fdtype *schema,fdtype *values,
   FD_MEMORY_POOL_TYPE *mpool)
{
  int i=0; 
  if (ptr == NULL) ptr=u8_malloc(sizeof(struct FD_SCHEMAP));
  FD_INIT_CONS(ptr,fd_schemap_type); ptr->schema=schema;
  ptr->size=size; ptr->flags=flags; ptr->schema=schema; ptr->mpool=mpool;
  if (values) ptr->values=values;
  else {
    ptr->values=values=u8_pmalloc(mpool,sizeof(fdtype)*size);
    while (i<size) values[i++]=FD_VOID;}
#if FD_THREADS_ENABLED
  u8_init_mutex(&(ptr->lock));
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

static fdtype copy_schemap(fdtype schemap)
{
  struct FD_SCHEMAP *ptr=
    FD_GET_CONS(schemap,fd_schemap_type,struct FD_SCHEMAP *);
  struct FD_SCHEMAP *nptr=u8_malloc_type(struct FD_SCHEMAP);
  int i=0, size=FD_XSCHEMAP_SIZE(ptr);
  fdtype *ovalues=ptr->values, *values=((size==0) ? (NULL) : (u8_malloc(sizeof(fdtype)*size)));
  fdtype *schema=ptr->schema, *nschema;
  FD_INIT_CONS(nptr,fd_schemap_type);
  if (ptr->flags&FD_SCHEMAP_STACK_SCHEMA)
    nptr->schema=nschema=u8_malloc(sizeof(fdtype)*size);
  else nptr->schema=schema;
  if (ptr->flags&FD_SCHEMAP_STACK_SCHEMA)
    while (i < size) {
      fdtype val=ovalues[i];
      nschema[i]=schema[i];
      if (FD_CONSP(val))
	if (FD_ACHOICEP(val))
	  values[i]=fd_make_simple_choice(val);
	else values[i]=fd_copy(val);
      else values[i]=val;
      i++;}
  else while (i < size) {
    values[i]=fd_incref(ovalues[i]); i++;}
  nptr->values=values;
  nptr->size=size;
  if (ptr->flags&FD_SCHEMAP_STACK_SCHEMA)
    nptr->flags=(ptr->flags-FD_SCHEMAP_STACK_SCHEMA)|FD_SCHEMAP_PRIVATE;
  else nptr->flags=ptr->flags;
  u8_init_mutex(&(nptr->lock));
  return FDTYPE_CONS(nptr);
}

FD_EXPORT fdtype fd_init_schemap
  (struct FD_SCHEMAP *ptr,short size,
   struct FD_KEYVAL *init,FD_MEMORY_POOL_TYPE *mpool)
{
  int i=0; fdtype *news, *newv;
  if (ptr == NULL) ptr=u8_malloc(sizeof(struct FD_SCHEMAP));
  FD_INIT_CONS(ptr,fd_schemap_type);
  news=u8_pmalloc(mpool,sizeof(fdtype)*size);
  ptr->values=newv=u8_pmalloc(mpool,sizeof(fdtype)*size);
  ptr->size=size; ptr->mpool=mpool; ptr->flags=FD_SCHEMAP_SORTED;
  sort_keyvals(init,size);
  while (i<size) {
    news[i]=init[i].key; newv[i]=init[i].value; i++;}
  ptr->schema=fd_register_schema(size,news);
  if (ptr->schema != news) u8_pfree_x(mpool,news,sizeof(fdtype)*size);
  else ptr->flags=ptr->flags|FD_SCHEMAP_PRIVATE;
  u8_pfree_x(mpool,init,sizeof(struct FD_KEYVAL)*size);
#if FD_THREADS_ENABLED
  u8_init_mutex(&(ptr->lock));
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


FD_EXPORT int fd_schemap_set
   (struct FD_SCHEMAP *sm,fdtype key,fdtype value)
{
  int slotno, size;
  FD_CHECK_TYPE_RET(sm,fd_schemap_type);
  u8_lock_mutex(&(sm->lock));
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->schema,size,sm->flags&FD_SCHEMAP_SORTED);
  if (slotno>=0) {
    fd_decref(sm->values[slotno]);
    sm->values[slotno]=fd_incref(value);
    FD_XSCHEMAP_MARK_MODIFIED(sm);
    u8_unlock_mutex(&(sm->lock));
    return 1;}
  else {
    u8_unlock_mutex(&(sm->lock));
    fd_seterr(fd_NoSuchKey,"fd_schemap_set",NULL,key);
    return -1;}
}

FD_EXPORT int fd_schemap_add
  (struct FD_SCHEMAP *sm,fdtype key,fdtype value)
{
  int slotno, size;
  FD_CHECK_TYPE_RET(sm,fd_schemap_type);
  u8_lock_mutex(&(sm->lock));
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->schema,size,sm->flags&FD_SCHEMAP_SORTED);
  if (slotno>=0) {
    FD_ADD_TO_CHOICE(sm->values[slotno],fd_incref(value));
    FD_XSCHEMAP_MARK_MODIFIED(sm);
    u8_unlock_mutex(&(sm->lock));
    return 1;}
  else {
    u8_unlock_mutex(&(sm->lock));
    fd_seterr(fd_NoSuchKey,"fd_schemap_add",NULL,key);}
}
	      
FD_EXPORT int fd_schemap_drop
  (struct FD_SCHEMAP *sm,fdtype key,fdtype value)
{
  int slotno, size;
  FD_CHECK_TYPE_RET(sm,fd_schemap_type);
  u8_lock_mutex(&(sm->lock));
  size=FD_XSCHEMAP_SIZE(sm);
  slotno=_fd_get_slotno(key,sm->schema,size,sm->flags&FD_SCHEMAP_SORTED);
  if (slotno>=0) {
    fdtype oldval=sm->values[slotno];
    fdtype newval=((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) : (fd_difference(oldval,value)));
    if (newval == oldval) fd_decref(newval);
    else {
      FD_XSCHEMAP_MARK_MODIFIED(sm);
      fd_decref(oldval); sm->values[slotno]=newval;}
    u8_unlock_mutex(&(sm->lock));
    return 1;}
  else {
    u8_unlock_mutex(&(sm->lock));
    return 0;}
}

static int schemap_getsize(struct FD_SCHEMAP *ptr)
{
  FD_CHECK_TYPE_RET(ptr,fd_schemap_type);
  return FD_XSCHEMAP_SIZE(ptr);
}

FD_EXPORT fdtype fd_schemap_keys(struct FD_SCHEMAP *sm)
{
  FD_CHECK_TYPE_RETDTYPE(sm,fd_schemap_type);
  {
    int size=FD_XSCHEMAP_SIZE(sm);
    if (size==0) return FD_EMPTY_CHOICE;
    else if (size==1) return fd_incref(sm->schema[0]);
    else {
      struct FD_CHOICE *ch=fd_alloc_choice(size);
      memcpy((fdtype *)FD_XCHOICE_DATA(ch),sm->schema,sizeof(fdtype)*size);
      if ((sm->flags)&FD_SCHEMAP_SORTED)
	return fd_init_choice(ch,size,sm->schema,0);
      else return fd_init_choice(ch,size,sm->schema,FD_CHOICE_DOSORT);}}
}

static void recycle_schemap(struct FD_CONS *c)
{
  struct FD_SCHEMAP *sm=(struct FD_SCHEMAP *)c;
  u8_lock_mutex(&(sm->lock));
  {
    int schemap_size=FD_XSCHEMAP_SIZE(sm);
    fdtype *scan=sm->values, *limit=sm->values+schemap_size;
    FD_MEMORY_POOL_TYPE *mpool=sm->mpool;
    while (scan < limit) {fd_decref(*scan); scan++;}
    if (((sm->flags)&(FD_SCHEMAP_PRIVATE))&&(sm->schema))
      u8_pfree_x(mpool,sm->schema,sizeof(fdtype)*size);
    if (sm->values) u8_pfree_x(mpool,sm->values,sizeof(fdtype)*size);
    u8_unlock_mutex(&(sm->lock));
    u8_destroy_mutex(&(sm->lock));
    u8_pfree_x(mpool,sm,sizeof(struct FD_SCHEMAP));    
  }
}
static int unparse_schemap(u8_output out,fdtype x)
{
  struct FD_SCHEMAP *sm=FD_XSCHEMAP(x);
  u8_lock_mutex(&(sm->lock));
  {
    int i=0, schemap_size=FD_XSCHEMAP_SIZE(sm);
    fdtype *schema=sm->schema, *values=sm->values;
    u8_puts(out,"#[");
    if (i<schemap_size) {
      fd_unparse(out,schema[i]); u8_sputc(out,' ');
      fd_unparse(out,values[i]); i++;}
    while (i<schemap_size) {
      u8_sputc(out,' ');
      fd_unparse(out,schema[i]); u8_sputc(out,' ');
      fd_unparse(out,values[i]); i++;}
    u8_puts(out,"]");
  }
  u8_unlock_mutex(&(sm->lock));
  return 1;
}
static int compare_schemaps(fdtype x,fdtype y,int quick)
{
  int result=0;
  struct FD_SCHEMAP *smx=(struct FD_SCHEMAP *)x;
  struct FD_SCHEMAP *smy=(struct FD_SCHEMAP *)y;
  u8_lock_mutex(&(smx->lock)); u8_lock_mutex(&(smy->lock));
  {
    int xsize=FD_XSCHEMAP_SIZE(smx), ysize=FD_XSCHEMAP_SIZE(smy);
    if (xsize>ysize) result=1;
    else if (xsize<ysize) result=-1;
    else {
      fdtype *xschema=smx->schema, *yschema=smy->schema;
      fdtype *xvalues=smx->values, *yvalues=smy->values;
      int i=0; while (i < xsize) {
	int cmp=FD_QCOMPARE(xschema[i],yschema[i]);
	if (cmp) {result=cmp; break;}
	else i++;}
      if (result==0) {
	i=0; while (i < xsize) {
	  int cmp=FD_QCOMPARE(xvalues[i],yvalues[i]);
	  if (cmp) {result=cmp; break;}
	  else i++;}}}}
  u8_unlock_mutex(&(smx->lock)); u8_unlock_mutex(&(smy->lock));
  return result;
}
 
/* Hash functions */

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
#define MYSTERIOUS_MULTIPLIER 2654435769U /*  */
#define MYSTERIOUS_MODULUS 256001281 /*  */


#if (SIZEOF_LONG_LONG == 8)
static unsigned int hash_mult(unsigned int x,unsigned int y)
{
  return ((x*y)%(MYSTERIOUS_MODULUS));
}
#else
static unsigned int hash_mult(unsigned int x,unsigned int y)
{
  if (x == 1) return y;
  else if (y == 1) return x;
  else if ((x == 0) || (y == 0)) return 0;
  else {
    unsigned int a=(x>>16), b=(x&0xFFFF); 
    unsigned int c=(y>>16), d=(y&0xFFFF); 
    unsigned int bd=b*d, ad=a*d, bc=b*c, ac=a*c;
    unsigned int hi=ac, lo=(bd&0xFFFF), tmp, carry, i;
    tmp=(bd>>16)+(ad&0xFFFF)+(bc&0xFFFF);
    lo=lo+((tmp&0xFFFF)<<16); carry=(tmp>>16);
    hi=hi+carry+(ad>>16)+(bc>>16);
    i=0; while (i++ < 4) {
      hi=((hi<<8)|(lo>>24))%(MYSTERIOUS_MODULUS); lo=lo<<8;}
    return hi;}
}
#endif


#if (SIZEOF_LONG_LONG == 8)
static unsigned int hash_multr(unsigned int x,unsigned int y,unsigned int r)
{
  return ((x*y)%(r));
}
#else
static unsigned int hash_multr(unsigned int x,unsigned int y,unsigned int r)
{
  if (x == 1) return y;
  else if (y == 1) return x;
  if ((x == 0) || (y == 0)) return 0;
  else {
    unsigned int a=(x>>16), b=(x&0xFFFF); 
    unsigned int c=(y>>16), d=(y&0xFFFF); 
    unsigned int bd=b*d, ad=a*d, bc=b*c, ac=a*c;
    unsigned int hi=ac, lo=(bd&0xFFFF), tmp, carry, i;
    tmp=(bd>>16)+(ad&0xFFFF)+(bc&0xFFFF);
    lo=lo+((tmp&0xFFFF)<<16); carry=(tmp>>16);
    hi=hi+carry+(ad>>16)+(bc>>16);
    i=0; while (i++ < 4) {
      hi=((hi<<8)|(lo>>24))%(r); lo=lo<<8;}
    return hi;}
}
#endif

static unsigned int hash_combine(unsigned int x,unsigned int y)
{
  if ((x == 0) && (y == 0)) return MYSTERIOUS_MODULUS+2;
  else if ((x == 0) || (y == 0))
    return x+y;
  else return hash_mult(x,y);
}

FD_FASTOP unsigned int mult_hash_string(unsigned char *start,int len)
{
  unsigned int h=0;
  unsigned *istart=(unsigned int *)start, asint;
  const unsigned int *scan=istart, *limit=istart+len/4;
  unsigned char *tail=start+(len/4)*4;
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
      struct FD_STRING *s=FD_GET_CONS(x,fd_string_type,struct FD_STRING *);
      return mult_hash_string(s->bytes,s->length);}
    case fd_packet_type: {
      struct FD_STRING *s=FD_GET_CONS(x,fd_packet_type,struct FD_STRING *);
      return mult_hash_string(s->bytes,s->length);}
    case fd_pair_type: {
      fdtype car=FD_CAR(x), cdr=FD_CDR(x);
      unsigned int hcar=fd_hash_lisp(car), hcdr=fd_hash_lisp(cdr);
      return hash_mult(hcar,hcdr);}
    case fd_vector_type: {
      struct FD_VECTOR *v=FD_GET_CONS(x,fd_vector_type,struct FD_VECTOR *);
      return hash_elts(v->data,v->length);}
    case fd_slotmap_type: {
      struct FD_SLOTMAP *sm=FD_GET_CONS(x,fd_slotmap_type,struct FD_SLOTMAP *);
      fdtype *kv=(fdtype *)sm->keyvals;
      return hash_elts(kv,sm->size*2);}
    case fd_choice_type: {
      struct FD_CHOICE *ch=FD_GET_CONS(x,fd_choice_type,struct FD_CHOICE *);
      int size=FD_XCHOICE_SIZE(ch);
      return hash_elts((fdtype *)(FD_XCHOICE_DATA(ch)),size);}
    case fd_achoice_type: {
      fdtype simple=fd_make_simple_choice(x);
      int hash=hash_lisp(simple);
      fd_decref(simple);
      return hash;}
    case fd_qchoice_type: {
      struct FD_QCHOICE *ch=FD_GET_CONS(x,fd_qchoice_type,struct FD_QCHOICE *);
      return hash_lisp(ch->choice);}
    default: {
      int ctype=FD_PTR_TYPE(x);
      if ((ctype<FD_TYPE_MAX) && (fd_hashfns[ctype]))
	return fd_hashfns[ctype](x,fd_hash_lisp);
      else return hash_mult(x,MYSTERIOUS_MULTIPLIER);}}
  else {
    int ctype=FD_PTR_TYPE(x); int ptype=x&3;
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
  (fdtype key,struct FD_HASHENTRY **slots,int n_slots)
{
  unsigned int hash=fd_hash_lisp(key), offset=compute_offset(hash,n_slots);
  struct FD_HASHENTRY *he=slots[offset];
  if (he == NULL) return NULL;
  else if (FD_EXPECT_TRUE(he->n_keyvals == 1))
    if (FDTYPE_EQUAL(key,he->keyval0.key))
      return &(he->keyval0);
    else return NULL;
  else return fd_sortvec_get(key,&(he->keyval0),he->n_keyvals);
}

FD_EXPORT struct FD_KEYVAL *fd_hashentry_insert
   (fdtype key,struct FD_HASHENTRY **hep,FD_MEMORY_POOL_TYPE *mpool)
{
  struct FD_HASHENTRY *he=*hep; int found=0;
  struct FD_KEYVAL *keyvals=&(he->keyval0); int size=he->n_keyvals;
  struct FD_KEYVAL *bottom=keyvals, *top=bottom+(size-1), *middle;
  while (top>=bottom) { /* (!(top<bottom)) */
    middle=bottom+(top-bottom)/2;
    if (FDTYPE_EQUAL(key,middle->key)) {found=1; break;}
    else if (cons_compare(key,middle->key)<0) top=middle-1; 
    else bottom=middle+1;}
  if (found)
    return middle;
  else {
    int mpos=(middle-keyvals), dir=(bottom>middle), ipos=mpos+dir;
    struct FD_KEYVAL *insert_point;
    struct FD_HASHENTRY *new_hashentry=
      /* We don't need to use size+1 here because FD_HASHENTRY includes
	 one value. */
      u8_prealloc(mpool,he,
		  sizeof(struct FD_HASHENTRY)+
		  (size)*sizeof(struct FD_KEYVAL));
    *hep=new_hashentry; new_hashentry->n_keyvals++;
    insert_point=&(new_hashentry->keyval0)+ipos;
    memmove(insert_point+1,insert_point,
	    sizeof(struct FD_KEYVAL)*(size-ipos));
    insert_point->key=fd_incref(key);
    insert_point->value=FD_EMPTY_CHOICE;
    return insert_point;}
}

FD_EXPORT struct FD_KEYVAL *fd_hashvec_insert
  (fdtype key,struct FD_HASHENTRY **slots,int n_slots,
   int *n_keys,FD_MEMORY_POOL_TYPE *mpool)
{
  unsigned int hash=fd_hash_lisp(key), offset=compute_offset(hash,n_slots);
  struct FD_HASHENTRY *he=slots[offset]; 
  if (he == NULL) {
    he=u8_pmalloc(mpool,sizeof(struct FD_HASHENTRY));
    he->n_keyvals=1; he->keyval0.key=fd_incref(key);
    he->keyval0.value=FD_EMPTY_CHOICE;
    slots[offset]=he; if (n_keys) (*n_keys)++;
    return &(he->keyval0);}
  else if (he->n_keyvals == 1)
    if (FDTYPE_EQUAL(key,he->keyval0.key))
      return &(he->keyval0);
    else {
      if (n_keys) (*n_keys)++;
      return fd_hashentry_insert(key,&slots[offset],mpool);}
  else {
    int size=he->n_keyvals;
    struct FD_KEYVAL *kv=fd_hashentry_insert(key,&slots[offset],mpool);
    if ((n_keys) && (slots[offset]->n_keyvals > size)) (*n_keys)++;
    return kv;}
}

/* Hashtables */

FD_EXPORT fdtype fd_hashtable_get
  (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (ht->n_keys == 0) return dflt;
  u8_lock_mutex(&(ht->lock));
  if (ht->n_keys == 0) {
    u8_unlock_mutex(&(ht->lock)); return dflt;}
  else result=fd_hashvec_get(key,ht->slots,ht->n_slots);
  if (result) {
    fdtype rv=result->value;
    fdtype v=((FD_ACHOICEP(rv)) ?
	       (fd_make_simple_choice(rv)) :
	       (fd_incref(rv)));
    u8_unlock_mutex(&(ht->lock));
    return v;}
  else {
    u8_unlock_mutex((&(ht->lock)));
    return fd_incref(dflt);}
}

FD_EXPORT fdtype fd_hashtable_get_nolock
  (struct FD_HASHTABLE *ht,fdtype key,fdtype dflt)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RETDTYPE(ht,fd_hashtable_type);
  if (ht->n_keys == 0) return dflt;
  if (ht->n_keys == 0) return dflt;
  else result=fd_hashvec_get(key,ht->slots,ht->n_slots);
  if (result) {
    fdtype rv=result->value;
    fdtype v=((FD_ACHOICEP(rv)) ?
	       (fd_make_simple_choice(rv)) :
	       (fd_incref(rv)));
    return v;}
  else {
    return fd_incref(dflt);}
}

FD_EXPORT int fd_hashtable_probe(struct FD_HASHTABLE *ht,fdtype key)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->n_keys == 0) return 0;
  u8_lock_mutex(&(ht->lock));
  if (ht->n_keys == 0) {
    u8_unlock_mutex(&(ht->lock)); return 0;}
  else result=fd_hashvec_get(key,ht->slots,ht->n_slots);
  if (result) {
    u8_unlock_mutex(&(ht->lock));
    return 1;}
  else {
    u8_unlock_mutex((&(ht->lock)));
    return 0;}
}

static int hashtable_test(struct FD_HASHTABLE *ht,fdtype key,fdtype val)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->n_keys == 0) return 0;
  u8_lock_mutex(&(ht->lock));
  if (ht->n_keys == 0) {
    u8_unlock_mutex(&(ht->lock)); return 0;}
  else result=fd_hashvec_get(key,ht->slots,ht->n_slots);
  if (result) {
    fdtype current=result->value; int cmp;
    if (FD_VOIDP(val))
      if (FD_EMPTY_CHOICEP(current)) cmp=0; else cmp=1;
    else if (FD_EQ(val,current)) cmp=1;
    else if ((FD_CHOICEP(val)) || (FD_ACHOICEP(val)) ||
	     (FD_CHOICEP(current)) || (FD_ACHOICEP(current)))
      cmp=fd_overlapp(val,current);
    else if (FD_EQUAL(val,current)) cmp=1;
    else cmp=0;
    u8_unlock_mutex(&(ht->lock));
    return cmp;}
  else {
    u8_unlock_mutex((&(ht->lock)));
    return 0;}
}

/* ?? */
FD_EXPORT int fd_hashtable_probe_novoid(struct FD_HASHTABLE *ht,fdtype key)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->n_keys == 0) return 0;
  u8_lock_mutex(&(ht->lock));
  if (ht->n_keys == 0) {
    u8_unlock_mutex(&(ht->lock)); return 0;}
  else result=fd_hashvec_get(key,ht->slots,ht->n_slots);
  if ((result) && (!(FD_VOIDP(result->value)))) {
    u8_unlock_mutex(&(ht->lock));
    return 1;}
  else {
    u8_unlock_mutex((&(ht->lock)));
    return 0;}
}

static void setup_hashtable(struct FD_HASHTABLE *ptr,int n_slots)
{
  struct FD_HASHENTRY **slots; int i=0, guess_slots=0;
  if (n_slots < 0) n_slots=fd_get_hashtable_size(-n_slots);
  ptr->n_slots=n_slots; ptr->n_keys=0; ptr->modified=0;
  ptr->slots=slots=
    u8_pmalloc(ptr->mpool,sizeof(struct FD_HASHENTRY *)*n_slots);
  while (i < n_slots) slots[i++]=NULL;
}

FD_EXPORT int fd_hashtable_set(fd_hashtable ht,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int n_keys, added;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  u8_lock_mutex(&(ht->lock));
  if (ht->n_slots == 0) setup_hashtable(ht,17);
  n_keys=ht->n_keys;
  result=fd_hashvec_insert
    (key,ht->slots,ht->n_slots,&(ht->n_keys),ht->mpool);
  if (ht->n_keys>n_keys) added=1; else added=0;
  fd_decref(result->value); result->value=fd_incref(value);
  ht->modified=1;
  u8_unlock_mutex(&(ht->lock));
  if (FD_EXCEPTIONP(result->value)) 
    return fd_interr(result->value);
  if (FD_EXPECT_FALSE((ht->n_keys*ht->loading)>ht->n_slots*100)) {
    int new_size=
      fd_get_hashtable_size((ht->n_keys*ht->loading)/50);
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_add(fd_hashtable ht,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int n_keys, added; fdtype newv;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  u8_lock_mutex(&(ht->lock));
  if (ht->n_slots == 0) setup_hashtable(ht,17);
  n_keys=ht->n_keys;
  result=fd_hashvec_insert
    (key,ht->slots,ht->n_slots,&(ht->n_keys),ht->mpool);
  ht->modified=1; if (ht->n_keys>n_keys) added=1; else added=0;
  newv=fd_incref(value);
  if (FD_EXCEPTIONP(newv)) {
    u8_unlock_mutex(&(ht->lock));
    return fd_interr(newv);}
  else {FD_ADD_TO_CHOICE(result->value,newv);}
#if 1
  /* If the value is an achoice, it doesn't need to be locked
     because it will be protected by the hashtable's lock. */
  if (FD_ACHOICEP(result->value)) {
    struct FD_ACHOICE *ch=FD_XACHOICE(result->value);
    if (ch->uselock) {
      u8_destroy_mutex(&(ch->lock)); ch->uselock=0;}}
#endif
  u8_unlock_mutex(&(ht->lock));
  if (FD_EXPECT_FALSE((ht->n_keys*ht->loading)>ht->n_slots*100)) {
    int new_size=
      fd_get_hashtable_size((ht->n_keys*ht->loading)/50);
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_drop
  (struct FD_HASHTABLE *ht,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->n_keys == 0) return 0;
  u8_lock_mutex(&(ht->lock));
  result=fd_hashvec_get(key,ht->slots,ht->n_slots);
  if (result) {
    fdtype newval=
      ((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) : (fd_difference(result->value,value)));
    fd_decref(result->value);
    result->value=newval;
    ht->modified=1;
    u8_unlock_mutex(&(ht->lock));
    return 1;}
  u8_unlock_mutex(&(ht->lock));
  return 0;
}

static fdtype restore_hashtable
  (FD_MEMORY_POOL_TYPE *mp,fdtype tag,fdtype alist)
{
  fdtype *keys, *vals; int n=0; struct FD_HASHTABLE *new;
  if (FD_PAIRP(alist)) {
    int max=64;
    keys=u8_malloc(sizeof(fdtype)*max);
    vals=u8_malloc(sizeof(fdtype)*max);
    {FD_DOLIST(elt,alist) {
      if (n>=max) {
	keys=u8_realloc(keys,sizeof(fdtype)*max*2);
	vals=u8_realloc(vals,sizeof(fdtype)*max*2);
	max=max*2;}
      keys[n]=FD_CAR(elt); vals[n]=FD_CDR(elt); n++;}}}
  else return fd_err(fd_DTypeError,"restore_hashtable",NULL,alist);
  new=(struct FD_HASHTABLE *)fd_make_hashtable(NULL,n*2,mp);
  fd_hashtable_iter(new,fd_table_add,n,keys,vals);
  u8_free(keys); u8_free(vals);
  return FDTYPE_CONS(new);
}

/* Evaluting hashtables */

FD_EXPORT void fd_hash_quality
  (unsigned int *hashv,int n_keys,int n_slots,
   unsigned int *buf,unsigned int bufsiz,
   unsigned int *nbucketsp,unsigned int *maxbucketp,
   unsigned int *ncollisionsp)
{
  int i, mallocd=0, sum=0;
  int n_buckets=0, n_collisions=0, max_bucket=0;
  if ((buf) && (n_slots<bufsiz))
    memset(buf,0,sizeof(unsigned int)*n_slots);
  else {
    buf=u8_malloc(sizeof(unsigned int)*n_slots); mallocd=1;
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
  if (nbucketsp) *nbucketsp=n_buckets;
  if (maxbucketp) *maxbucketp=max_bucket;
  if (ncollisionsp) *ncollisionsp=n_collisions;
}

/* A general operations function for hashtables */

static int do_hashtable_op
  (struct FD_HASHTABLE *ht,fd_tableop op,fdtype key,fdtype value)
{
  struct FD_KEYVAL *result; int achoicep=0, added=0;
  if (FD_EMPTY_CHOICEP(key)) return 0;
  switch (op) {
  case fd_table_replace: case fd_table_replace_novoid: case fd_table_drop:
  case fd_table_add_if_present: case fd_table_test:
  case fd_table_increment_if_present: case fd_table_multiply_if_present:
    /* These are operations which can be resolved immediately if the key
       does not exist in the table.  It doesn't bother setting up the hashtable
       if it doesn't have to. */
    if (ht->n_keys == 0) return 0;
    result=fd_hashvec_get(key,ht->slots,ht->n_slots);
    if (result==NULL) return 0; else {added=1; break;}
  case fd_table_add: case fd_table_add_noref:
    if ((FD_EMPTY_CHOICEP(value)) &&
	((op == fd_table_add) ||
	 (op == fd_table_add_noref) ||
	 (op == fd_table_add_if_present) ||
	 (op == fd_table_test) ||
	 (op == fd_table_drop)))
      return 0;
  default:
    if (ht->n_slots == 0) setup_hashtable(ht,17);
    result=fd_hashvec_insert
      (key,ht->slots,ht->n_slots,&(ht->n_keys),ht->mpool);}
  if (op != fd_table_replace)
    if (FD_EXPECT_FALSE((FD_VOIDP(value)) || (FD_EMPTY_CHOICEP(value))))
      return 0;
  /* We see if there was an achoice created because we can safely
     disable its mutex since it is within the hashtable. */
  achoicep=FD_ACHOICEP(result->value);
  switch (op) {
  case fd_table_replace_novoid:
    if (FD_VOIDP(result->value)) return 0;
  case fd_table_set: case fd_table_replace:
    fd_decref(result->value); result->value=fd_incref(value); break;
  case fd_table_set_noref:
    fd_decref(result->value); result->value=value; break;
  case fd_table_add: case fd_table_add_empty: case fd_table_add_if_present:
    FD_ADD_TO_CHOICE(result->value,fd_incref(value)); break;
  case fd_table_add_noref: case fd_table_add_empty_noref:
    FD_ADD_TO_CHOICE(result->value,value); break;
  case fd_table_drop: {
    fdtype newval=((FD_VOIDP(value)) ? (FD_EMPTY_CHOICE) : (fd_difference(result->value,value)));
    fd_decref(result->value); result->value=fd_incref(newval);
    break;}
  case fd_table_test:
    if ((FD_CHOICEP(result->value)) || (FD_ACHOICEP(result->value)) ||
	(FD_CHOICEP(value)) || (FD_ACHOICEP(value)))
      return fd_overlapp(value,result->value);
    else if (FDTYPE_EQUAL(value,result->value))
      return 1;
    else return 0;
  case fd_table_default:
    if (FD_EMPTY_CHOICEP(result->value)) result->value=fd_incref(value);
    break;
  case fd_table_increment: case fd_table_increment_if_present:
    if (FD_EMPTY_CHOICEP(result->value)) result->value=fd_incref(value);
    else {
      fdtype current=result->value;
      FD_DO_CHOICES(v,value)
	if ((FD_FIXNUMP(current)) && (FD_FIXNUMP(v))) {
	  int cval=FD_FIX2INT(current);
	  int delta=FD_FIX2INT(v);
	  result->value=FD_INT2DTYPE(cval+delta);}
	else if ((FD_FLONUMP(current)) &&
		 (FD_CONS_REFCOUNT(((fd_cons)current))<2) &&
		 ((FD_FIXNUMP(v)) || (FD_FLONUMP(v)))) {
	  struct FD_DOUBLE *dbl=(fd_double)current;
	  if (FD_FIXNUMP(v))
	    dbl->flonum=dbl->flonum+FD_FIX2INT(v);
	  else dbl->flonum=dbl->flonum+FD_FLONUM(v);}
	else {
	  fdtype newnum=fd_plus(current,v);
	  if (newnum != current) {
	    fd_decref(current); result->value=newnum;}}}
    break;
  case fd_table_multiply: case fd_table_multiply_if_present:
    if (FD_EMPTY_CHOICEP(result->value)) result->value=fd_incref(value);
    else {
      fdtype current=result->value;
      FD_DO_CHOICES(v,value)
	if ((FD_FIXNUMP(current)) && (FD_FIXNUMP(v))) {
	  int cval=FD_FIX2INT(current);
	  int factor=FD_FIX2INT(v);
	  result->value=FD_INT2DTYPE(cval*factor);}
	else if ((FD_FLONUMP(current)) &&
		 (FD_CONS_REFCOUNT(((fd_cons)current))<2) &&
		 ((FD_FIXNUMP(v)) || (FD_FLONUMP(v)))) {
	  struct FD_DOUBLE *dbl=(fd_double)current;
	  if (FD_FIXNUMP(v))
	    dbl->flonum=dbl->flonum*FD_FIX2INT(v);
	  else dbl->flonum=dbl->flonum*FD_FLONUM(v);}
	else {
	  fdtype newnum=fd_multiply(current,v);
	  if (newnum != current) {
	    fd_decref(current); result->value=newnum;}}}
    break;
  case fd_table_push:
    if ((FD_VOIDP(result->value)) || (FD_EMPTY_CHOICEP(result->value)))
      result->value=fd_make_pair(value,FD_EMPTY_LIST);
    else if (FD_PAIRP(result->value)) 
      result->value=fd_init_pair(NULL,fd_incref(value),result->value);
    else {
      fdtype tail=fd_init_pair(NULL,result->value,FD_EMPTY_LIST);
      result->value=fd_init_pair(NULL,fd_incref(value),tail);}
    break;
  default:
    added=-1;
    fd_seterr3(BadHashtableMethod,"do_hashtable_op",u8_mkstring("0x%x",op));
    break;
  }
  ht->modified=1;
  if ((achoicep==0) && (FD_ACHOICEP(result->value))) {
    /* If we didn't have an achoice before and we do now, that means
       a new achoice was created with a mutex and everything.  We can
       safely destroy it and set the choice to not use locking, since 
       the value will be protected by the hashtable's lock. */
    struct FD_ACHOICE *ch=FD_XACHOICE(result->value);
    if (ch->uselock) {
      u8_destroy_mutex(&(ch->lock)); ch->uselock=0;}}
  return added;
}

FD_EXPORT int fd_hashtable_op
   (struct FD_HASHTABLE *ht,fd_tableop op,fdtype key,fdtype value)
{
  int added;
  if (FD_EMPTY_CHOICEP(key)) return 0;
  KEY_CHECK(key,ht); FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  u8_lock_mutex(&(ht->lock));
  added=do_hashtable_op(ht,op,key,value);
  u8_unlock_mutex(&(ht->lock));  
  if (FD_EXPECT_FALSE((ht->n_keys*ht->loading)>ht->n_slots*100)) {
    int new_size=
      fd_get_hashtable_size((ht->n_keys*ht->loading)/50);
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_iter
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    const fdtype *keys,const fdtype *values)
{
  int i=0, added=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  u8_lock_mutex(&(ht->lock));
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0)
      added=do_hashtable_op(ht,op,keys[i],values[i]);
    else do_hashtable_op(ht,op,keys[i],values[i]);
    i++;}
  u8_unlock_mutex(&(ht->lock));  
  if (FD_EXPECT_FALSE((ht->n_keys*ht->loading)>ht->n_slots*100)) {
    int new_size=
      fd_get_hashtable_size((ht->n_keys*ht->loading)/50);
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_iterkeys
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    const fdtype *keys,fdtype value)
{
  int i=0, added=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  u8_lock_mutex(&(ht->lock));
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0)
      added=do_hashtable_op(ht,op,keys[i],value);
    else do_hashtable_op(ht,op,keys[i],value);
    if (added<0) {
      u8_unlock_mutex(&(ht->lock));  
      return added;}
    i++;}
  u8_unlock_mutex(&(ht->lock));  
  if (FD_EXPECT_FALSE((ht->n_keys*ht->loading)>ht->n_slots*100)) {
    int new_size=
      fd_get_hashtable_size((ht->n_keys*ht->loading)/50);
    fd_resize_hashtable(ht,new_size);}
  return added;
}

FD_EXPORT int fd_hashtable_itervals
   (struct FD_HASHTABLE *ht,fd_tableop op,int n,
    fdtype key,const fdtype *values)
{
  int i=0, added=0;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  u8_lock_mutex(&(ht->lock));
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0)
      added=do_hashtable_op(ht,op,key,values[i]);
    else do_hashtable_op(ht,op,key,values[i]);
    i++;}
  u8_unlock_mutex(&(ht->lock));  
  if (FD_EXPECT_FALSE((ht->n_keys*ht->loading)>ht->n_slots*100)) {
    int new_size=
      fd_get_hashtable_size((ht->n_keys*ht->loading)/50);
    fd_resize_hashtable(ht,new_size);}
  return added;
}

static int hashtable_getsize(struct FD_HASHTABLE *ptr)
{
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  return ptr->n_keys;
}

FD_EXPORT fdtype fd_hashtable_keys(struct FD_HASHTABLE *ptr)
{
  fdtype result=FD_EMPTY_CHOICE;
  FD_CHECK_TYPE_RETDTYPE(ptr,fd_hashtable_type);
  u8_lock_mutex(&ptr->lock);
  {
    struct FD_HASHENTRY **scan=ptr->slots, **lim=scan+ptr->n_slots;
    while (scan < lim)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
	struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	while (kvscan<kvlimit) {
	  FD_ADD_TO_CHOICE(result,fd_incref(kvscan->key));
	  kvscan++;}
	scan++;}
      else scan++;}
  u8_unlock_mutex(&ptr->lock);
  return fd_simplify_choice(result);
}

FD_EXPORT int fd_reset_hashtable(struct FD_HASHTABLE *ht,int n_slots,int lock)
{
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (n_slots<0) n_slots=ht->n_slots;
  if (lock) u8_lock_mutex(&(ht->lock));
  /* First, recycle its components. */
  if (ht->n_slots) {
    struct FD_HASHENTRY **scan=ht->slots, **lim=scan+ht->n_slots;
    while (scan < lim)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
	struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	while (kvscan<kvlimit) {
	  fd_decref(kvscan->key); fd_decref(kvscan->value);
	  kvscan++;}
	u8_pfree_x(ht->mpool,*scan,
		   sizeof(struct FD_HASHENTRY)+
		   sizeof(struct FD_KEYVAL)*(n_keyvals-1));
	*scan++=NULL;}
      else scan++;
    u8_pfree_x(ht->mpool,ht->slots,sizeof(struct FD_HASHENTRY *)*ht->n_slots);}
  /* Now reinitialize it. */
  if (n_slots == 0) {
    ht->n_slots=ht->n_keys=0; ht->loading=200;
    ht->slots=NULL;}
  else {
    int i=0; struct FD_HASHENTRY **slots;
    ht->n_slots=n_slots; ht->n_keys=0; ht->loading=200; 
    ht->slots=slots=u8_pmalloc(mpool,sizeof(struct FD_HASHENTRY *)*n_slots);
    while (i < n_slots) slots[i++]=NULL;}
  if (lock) u8_unlock_mutex(&(ht->lock));
  return n_slots;
}

FD_EXPORT fdtype fd_make_hashtable
   (struct FD_HASHTABLE *ptr,int n_slots,FD_MEMORY_POOL_TYPE *mpool)
{
  if (n_slots == 0) {
    if (ptr == NULL) {
      ptr=u8_pmalloc(mpool,sizeof(struct FD_HASHTABLE));
      FD_INIT_CONS(ptr,fd_hashtable_type);}
    else {
      FD_SET_CONS_TYPE(ptr,fd_hashtable_type);}
#if FD_THREADS_ENABLED
    u8_init_mutex(&(ptr->lock));
#endif
    ptr->n_slots=ptr->n_keys=0; ptr->loading=200; ptr->modified=0;
    ptr->mpool=mpool; ptr->slots=NULL;
    return FDTYPE_CONS(ptr);}
  else {
    int i=0; struct FD_HASHENTRY **slots;
    if (ptr == NULL) {
      ptr=u8_pmalloc(mpool,sizeof(struct FD_HASHTABLE));
      FD_INIT_CONS(ptr,fd_hashtable_type);}
    else {FD_SET_CONS_TYPE(ptr,fd_hashtable_type);}
#if FD_THREADS_ENABLED
    u8_init_mutex(&(ptr->lock));
#endif
    if (n_slots < 0) n_slots=-n_slots;
    else n_slots=fd_get_hashtable_size(n_slots);
    ptr->modified=0;
    ptr->n_slots=n_slots; ptr->n_keys=0; ptr->loading=200; ptr->mpool=mpool;
    ptr->slots=slots=u8_pmalloc(mpool,sizeof(struct FD_HASHENTRY *)*n_slots);
    while (i < n_slots) slots[i++]=NULL;
    return FDTYPE_CONS(ptr);}
}

/* Note that this does not incref the values passed to it. */
FD_EXPORT fdtype fd_init_hashtable(struct FD_HASHTABLE *ptr,int n_keyvals,
				    struct FD_KEYVAL *inits,
				    FD_MEMORY_POOL_TYPE *mpool)
{
  int i=0, n_slots=fd_get_hashtable_size(n_keyvals*2), n_keys=0;
  struct FD_HASHENTRY **slots;
  if (ptr == NULL) ptr=u8_pmalloc(mpool,sizeof(struct FD_HASHTABLE));
  FD_INIT_CONS(ptr,fd_hashtable_type);
  ptr->n_slots=n_slots; ptr->mpool=mpool;
  ptr->n_keys=n_keyvals; ptr->loading=200; ptr->modified=0;
  ptr->slots=slots=u8_pmalloc(mpool,sizeof(struct FD_HASHENTRY *)*n_slots);
  memset(slots,0,sizeof(struct FD_HASHENTRY *)*n_slots);
  i=0; while (i<n_keyvals) {
    struct FD_KEYVAL *ki=&(inits[i]);
    struct FD_KEYVAL *hv=
      fd_hashvec_insert(ki->key,slots,n_slots,&n_keys,mpool);
    hv->value=fd_incref(ki->value); i++;}
#if FD_THREADS_ENABLED
  u8_init_mutex(&(ptr->lock));
#endif
  return FDTYPE_CONS(ptr);
}

FD_EXPORT int fd_resize_hashtable(struct FD_HASHTABLE *ptr,int n_slots)
{
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  u8_lock_mutex(&(ptr->lock));
  {
    struct FD_HASHENTRY **new_slots=
      u8_pmalloc(ptr->mpool,sizeof(struct FD_HASH_ENTRY *)*n_slots);
    struct FD_HASHENTRY **scan=ptr->slots, **lim=scan+ptr->n_slots;
    struct FD_HASHENTRY **nscan=new_slots, **nlim=nscan+n_slots;
    while (nscan<nlim) *nscan++=NULL;
    while (scan < lim)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan++; int n_keyvals=e->n_keyvals;
	struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	while (kvscan<kvlimit) {
	  struct FD_KEYVAL *nkv=
	    fd_hashvec_insert(kvscan->key,new_slots,n_slots,NULL,ptr->mpool);
	  nkv->value=kvscan->value; kvscan->value=FD_VOID;
	  fd_decref(kvscan->key); kvscan++;}
	u8_pfree_x(ptr->mpool,e,
		   sizeof(struct FD_HASHENTRY)+
		   (sizeof(struct FD_KEYVAL)*(e->n_keyvals-1)));}
      else scan++;
    u8_pfree_x(ptr->mpool,ptr->slots,
	       sizeof(struct FD_HASH_ENTRY *)*(ptr->n_slots));
    ptr->n_slots=n_slots; ptr->slots=new_slots;}
  u8_unlock_mutex(&(ptr->lock));
  return n_slots;
}

FD_EXPORT int fd_devoid_hashtable(struct FD_HASHTABLE *ptr)
{
  int n_slots=ptr->n_slots, n_keys=ptr->n_keys;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  if ((n_slots == 0) || (n_keys == 0)) return 0;
  u8_lock_mutex(&(ptr->lock));
  {
    struct FD_HASHENTRY **new_slots=
      u8_pmalloc(ptr->mpool,sizeof(struct FD_HASH_ENTRY *)*n_slots);
    struct FD_HASHENTRY **scan=ptr->slots, **lim=scan+ptr->n_slots;
    struct FD_HASHENTRY **nscan=new_slots, **nlim=nscan+n_slots;
    while (nscan<nlim) *nscan++=NULL;
    while (scan < lim)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan++; int n_keyvals=e->n_keyvals;
	struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	while (kvscan<kvlimit)
	  if (FD_VOIDP(kvscan->value)) {
	    fd_decref(kvscan->key); kvscan++;}
	  else {
	    struct FD_KEYVAL *nkv=
	      fd_hashvec_insert(kvscan->key,new_slots,n_slots,NULL,ptr->mpool);
	    nkv->value=kvscan->value; kvscan->value=FD_VOID;
	    fd_decref(kvscan->key); kvscan++;}
	u8_pfree_x(ptr->mpool,e,
		   sizeof(struct FD_HASHENTRY)+
		   (sizeof(struct FD_KEYVAL)*(e->n_keyvals-1)));}
      else scan++;
    u8_pfree_x(ptr->mpool,ptr->slots,
	       sizeof(struct FD_HASH_ENTRY *)*(ptr->n_slots));
    ptr->n_slots=n_slots; ptr->slots=new_slots;}
  u8_unlock_mutex(&(ptr->lock));
  return n_slots;
}

FD_EXPORT int fd_hashtable_stats
  (struct FD_HASHTABLE *ptr,
   int *n_slotsp,int *n_keysp,int *n_bucketsp,int *n_collisionsp,
   int *max_bucketp,int *n_valsp,int *max_valsp)
{
  int n_slots=ptr->n_slots, n_keys=0;
  int n_buckets=0, max_bucket=0, n_collisions=0;
  int n_vals=0, max_vals=0;
  FD_CHECK_TYPE_RET(ptr,fd_hashtable_type);
  u8_lock_mutex(&(ptr->lock));
  {
    struct FD_HASHENTRY **scan=ptr->slots, **lim=scan+ptr->n_slots;
    while (scan < lim)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
	n_buckets++; n_keys=n_keys+n_keyvals;
	if (n_keyvals>max_bucket) max_bucket=n_keyvals;
	if (n_keyvals>1) 
	  n_collisions++;
	if ((n_valsp) || (max_valsp)) {
	  struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	  while (kvscan<kvlimit) {
	    fdtype val=kvscan->value; int valcount;
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
  u8_unlock_mutex(&(ptr->lock));
  return n_keys;
}

static fdtype copy_hashtable(fdtype table)
{
  struct FD_HASHTABLE *ptr=FD_GET_CONS(table,fd_hashtable_type,fd_hashtable);
  struct FD_HASHTABLE *nptr=u8_malloc(sizeof(struct FD_HASHTABLE));
  struct FD_HASHENTRY **slots, **nslots, **read, **write, **read_limit;
  int n_slots=ptr->n_slots;
  FD_INIT_CONS(nptr,fd_hashtable_type);
  nptr->n_slots=n_slots;
  nptr->mpool=NULL;
  nptr->modified=0;
  nptr->n_keys=ptr->n_keys;
  read=slots=ptr->slots;
  read_limit=read+n_slots;
  nptr->loading=ptr->loading;
  write=nptr->slots=u8_malloc(n_slots*sizeof(struct FD_HASHENTRY *));
  memset(write,0,sizeof(struct FD_HASHENTRY *)*n_slots);
  while (read<read_limit)
    if (*read==NULL) {read++; write++;}
    else {
      struct FD_KEYVAL *kvread, *kvwrite, *kvlimit;
      struct FD_HASHENTRY *he=*read++, *newhe; int n=he->n_keyvals;
      *write++=newhe=u8_malloc(sizeof(struct FD_HASHENTRY)+(n-1)*sizeof(struct FD_KEYVAL));
      kvread=&(he->keyval0); kvwrite=&(newhe->keyval0); 
      newhe->n_keyvals=n; kvlimit=kvread+n;
      while (kvread<kvlimit) {
	fdtype key=kvread->key, val=kvread->value; kvread++;
	if (FD_CONSP(key)) kvwrite->key=fd_copy(key);
	else kvwrite->key=key;
	if (FD_CONSP(val))
	  if (FD_ACHOICEP(val))
	    kvwrite->value=fd_make_simple_choice(val);
	  else kvwrite->value=fd_copy(val);
	else kvwrite->value=val;
	kvwrite++;}}
#if FD_THREADS_ENABLED
  u8_init_mutex(&(nptr->lock));
#endif
  return FDTYPE_CONS(nptr);
}

static int unparse_hashtable(u8_output out,fdtype x)
{
  struct FD_HASHTABLE *ht=FD_XHASHTABLE(x); char buf[128];
  sprintf(buf,"#<HASHTABLE %d/%d>",ht->n_keys,ht->n_slots);
  u8_puts(out,buf);
  return 1;
}

FD_EXPORT int fd_recycle_hashtable(struct FD_HASHTABLE *c)
{
  struct FD_HASHTABLE *ht=(struct FD_HASHTABLE *)c;
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  u8_lock_mutex(&(ht->lock));
  if (ht->n_slots) {
    struct FD_HASHENTRY **scan=ht->slots, **lim=scan+ht->n_slots;
    while (scan < lim)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
	struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	while (kvscan<kvlimit) {
	  fd_decref(kvscan->key);
	  fd_decref(kvscan->value);
	  kvscan++;}
	u8_pfree_x(ht->mpool,*scan,
		   sizeof(struct FD_HASHENTRY)+
		   sizeof(struct FD_KEYVAL)*(n_keyvals-1));
	*scan++=NULL;}
      else scan++;
    u8_pfree_x(ht->mpool,ht->slots,sizeof(struct FD_HASHENTRY *)*ht->n_slots);}
  u8_unlock_mutex(&(ht->lock));
  u8_destroy_mutex(&(ht->lock));
  if (!(FD_STACK_CONSP(ht)))
    u8_pfree_x(ht->mpool,ht,sizeof(struct FD_HASHTABLE));
  return 0;
}

FD_EXPORT struct FD_KEYVAL *fd_hashtable_keyvals
   (struct FD_HASHTABLE *ht,int *sizep,int lock)
{
  struct FD_KEYVAL *results, *rscan;
  if ((FD_CONS_TYPE(ht)) != fd_hashtable_type) {
    fd_seterr(fd_TypeError,"hashtable",NULL,(fdtype)ht);
    return NULL;}
  if (ht->n_keys == 0) {*sizep=0; return NULL;}
  if (lock) u8_lock_mutex(&(ht->lock));
  if (ht->n_slots) {
    struct FD_HASHENTRY **scan=ht->slots, **lim=scan+ht->n_slots;
    rscan=results=u8_malloc(sizeof(struct FD_KEYVAL)*ht->n_keys);
    while (scan < lim)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
	struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	while (kvscan<kvlimit) {
	  rscan->key=fd_incref(kvscan->key);
	  rscan->value=fd_incref(kvscan->value);
	  kvscan++; scan++;}}
      else scan++;
    *sizep=ht->n_keys;}
  else {*sizep=0; results=NULL;}
  if (lock) u8_unlock_mutex(&(ht->lock));
  return results;
}

FD_EXPORT int fd_for_hashtable
  (struct FD_HASHTABLE *ht,fd_keyvalfn f,void *data,int lock)
{
  FD_CHECK_TYPE_RET(ht,fd_hashtable_type);
  if (ht->n_keys == 0) return 0;
  if (lock) u8_lock_mutex(&(ht->lock));
  if (ht->n_slots) {
    struct FD_HASHENTRY **scan=ht->slots, **lim=scan+ht->n_slots;
    while (scan < lim)
      if (*scan) {
	struct FD_HASHENTRY *e=*scan; int n_keyvals=e->n_keyvals;
	struct FD_KEYVAL *kvscan=&(e->keyval0), *kvlimit=kvscan+n_keyvals;
	while (kvscan<kvlimit) {
	  if (f(kvscan->key,kvscan->value,data)) {
	    if (lock) u8_unlock_mutex(&(ht->lock));
	    return;}
	  else kvscan++;}
	scan++;}
      else scan++;}
  if (lock) u8_unlock_mutex(&(ht->lock));
  return ht->n_slots;
}

/* Hashsets */

FD_EXPORT void fd_init_hashset(struct FD_HASHSET *hashset,int size)
{
  fdtype *slots;
  int i=0, n_slots=fd_get_hashtable_size(size);
  FD_INIT_CONS(hashset,fd_hashset_type);
  hashset->n_slots=n_slots; hashset->n_keys=0;
  hashset->atomicp=1; hashset->loading=200;
  hashset->slots=slots=u8_malloc(sizeof(fdtype)*n_slots);
  while (i < n_slots) slots[i++]=0;
  u8_init_mutex(&(hashset->lock));
  return;
}

FD_EXPORT fdtype fd_make_hashset()
{
  struct FD_HASHSET *h=u8_malloc_type(struct FD_HASHSET);
  fd_init_hashset(h,17);
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
  if (h->n_keys==0) return 0;
  u8_lock_mutex(&(h->lock));
  slots=h->slots;
  probe=hashset_get_slot(key,(const fdtype *)slots,h->n_slots);
  if (probe>=0)
    if (slots[probe]) exists=1; else exists=0;
  u8_unlock_mutex(&(h->lock));
  if (probe<0) {
    fd_seterr(HashsetOverflow,"fd_hashset_get",NULL,(fdtype)h);
    return -1;}
  else return exists;
}

FD_EXPORT fdtype fd_hashset_elts(struct FD_HASHSET *h,int clean)
{
  FD_CHECK_TYPE_RETDTYPE(h,fd_hashset_type);
  if (h->n_keys==0) return FD_EMPTY_CHOICE;
  else {
    u8_lock_mutex(&(h->lock));
    if (h->n_keys==1) {
      fdtype *scan=h->slots, *limit=scan+h->n_slots;
      while (scan<limit)
	if (*scan)
	  if (clean) {
	    fdtype v=*scan;
	    u8_free(h->slots);
	    return v;}
	  else {
	    u8_unlock_mutex(&(h->lock));
	    return fd_incref(*scan);}
	else scan++;}
    else {
      int n=h->n_keys, atomicp=1; 
      struct FD_CHOICE *new_choice=fd_alloc_choice(n);
      const fdtype *scan=h->slots, *limit=scan+h->n_slots;
      fdtype *base=(fdtype *)(FD_XCHOICE_DATA(new_choice));
      fdtype *write=base, *writelim=base+n;
      if (clean)
	while ((scan<limit) && (write<writelim))
	  if (*scan) *write++=*scan++; else scan++;
      else while ((scan<limit) && (write<writelim)) {
	fdtype v=*scan++;
	if (v) {
	  if (atomicp) {
	    if (FD_CONSP(v)) {atomicp=0; fd_incref(v);}}
	  else fd_incref(v);
	  *write++=v;}}
      if (clean) u8_free(h->slots);
      u8_unlock_mutex(&(h->lock));
      return fd_init_choice(new_choice,write-base,base,
			    (FD_CHOICE_DOSORT|
			     ((atomicp)?(FD_CHOICE_ISATOMIC):(FD_CHOICE_ISCONSES))|
			     FD_CHOICE_REALLOC));}}

}
static fdtype hashset_getsize(struct FD_HASHSET *h)
{
  FD_CHECK_TYPE_RETDTYPE(h,fd_hashset_type);
  return h->n_keys;
}

static fdtype hashsetelts(struct FD_HASHSET *h)
{
  return fd_hashset_elts(h,0);
}

static int grow_hashset(struct FD_HASHSET *h)
{
  int i=0, lim=h->n_slots, new_size=fd_get_hashtable_size(h->n_keys*3);
  const fdtype *slots=h->slots;
  fdtype *newslots=u8_malloc(sizeof(fdtype)*new_size);
  while (i<new_size) newslots[i++]=FD_NULL;
  i=0; while (i < lim)
	 if (slots[i]==0) i++;
	 else if (FD_VOIDP(slots[i])) i++;
	 else {
	   int off=hashset_get_slot(slots[i],newslots,new_size);
	   newslots[off]=slots[i]; i++;}
  u8_free(h->slots); h->slots=newslots; h->n_slots=new_size;
  return 1;
}

FD_EXPORT int fd_hashset_mod(struct FD_HASHSET *h,fdtype key,int add)
{
  int probe; fdtype *slots;
  u8_lock_mutex(&(h->lock));
  slots=h->slots;
  probe=hashset_get_slot(key,h->slots,h->n_slots);
  if (probe < 0) {
    u8_unlock_mutex(&(h->lock));
    fd_seterr(HashsetOverflow,"fd_hashset_mod",NULL,(fdtype)h);
    return -1;}
  else if (FD_NULLP(slots[probe]))
    if (add) {
      slots[probe]=fd_incref(key); h->n_keys++;
      if (FD_CONSP(key)) h->atomicp=0;
      if (FD_EXPECT_FALSE(h->n_keys*h->loading>h->n_slots*100))
	grow_hashset(h);
      u8_unlock_mutex(&(h->lock));
      return 1;}
    else {
      u8_unlock_mutex(&(h->lock));
      return 0;}
  else if (add) {
    u8_unlock_mutex(&(h->lock));
    return 0;}
  else {
    fd_decref(slots[probe]); slots[probe]=FD_VOID;
    u8_unlock_mutex(&(h->lock));
    return 1;}
}

/* This adds without locking or incref. */
FD_EXPORT int fd_hashset_init_add(struct FD_HASHSET *h,fdtype key)
{
  int probe; fdtype *slots;
  slots=h->slots;
  probe=hashset_get_slot(key,h->slots,h->n_slots);
  if (probe < 0) {
    fd_seterr(HashsetOverflow,"fd_hashset_mod",NULL,(fdtype)h);
    return -1;}
  else if (FD_NULLP(slots[probe])) {
    slots[probe]=key; h->n_keys++;
    if (FD_CONSP(key)) h->atomicp=0;
    if (FD_EXPECT_FALSE(h->n_keys*h->loading>h->n_slots*100))
      grow_hashset(h);
    return 1;}
}

FD_EXPORT int fd_recycle_hashset(struct FD_HASHSET *h)
{
  u8_lock_mutex(&(h->lock));
  if (h->atomicp==0) {
    fdtype *scan=h->slots, *lim=scan+h->n_slots;
    while (scan<lim)
      if (FD_NULLP(*scan)) scan++;
      else {
	fdtype v=*scan++; fd_decref(v);}}
  u8_free(h->slots);
  u8_unlock_mutex(&(h->lock)); 
  u8_destroy_mutex(&(h->lock));
  return 1;
}

static int unparse_hashset(u8_output out,fdtype x)
{
  struct FD_HASHSET *ht=((struct FD_HASHSET *)x); char buf[128];
  sprintf(buf,"#<HASHSET %d/%d>",ht->n_keys,ht->n_slots);
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
  if (FD_TRUEP(val)) fd_hashset_mod(h,key,1);
  else if (FD_FALSEP(val)) fd_hashset_mod(h,key,0);
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
  else if (FD_EXPECT_TRUE(argtype<FD_TYPE_MAX))
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
	  if (FD_EMPTY_CHOICEP(results)) return dflt;
	  else return results;}
	else return (fd_tablefns[argtype]->get)(arg,key,dflt);
      else return fd_err(fd_NoMethod,CantGet,NULL,arg);
    else return fd_err(NotATable,"fd_get",NULL,arg);
  else return fd_err(fd_BadPtr,"fd_get",NULL,arg);
}

FD_EXPORT int fd_store(fdtype arg,fdtype key,fdtype value)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (FD_EXPECT_TRUE(argtype<FD_TYPE_MAX))
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
  if (FD_EXPECT_TRUE(argtype<FD_TYPE_MAX))
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
	    FD_ADD_TO_CHOICE(values,fd_incref(value));
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
  if (FD_EXPECT_TRUE(argtype<FD_TYPE_MAX))
    if (FD_EXPECT_TRUE(fd_tablefns[argtype]!=NULL))
      if (FD_EXPECT_TRUE(fd_tablefns[argtype]->drop!=NULL))
	if (FD_EXPECT_FALSE((FD_EMPTY_CHOICEP(value)) || (FD_EMPTY_CHOICEP(key))))
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
  if (FD_EXPECT_TRUE(argtype<FD_TYPE_MAX))
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
      else return fd_err(fd_NoMethod,CantTest,NULL,arg);
    else return fd_err(NotATable,"fd_test",NULL,arg);
  else return fd_err(fd_BadPtr,"fd_test",NULL,arg);
}

FD_EXPORT int fd_getsize(fdtype arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (argtype<FD_TYPE_MAX)
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

FD_EXPORT fdtype fd_getkeys(fdtype arg)
{
  fd_ptr_type argtype=FD_PTR_TYPE(arg);
  if (argtype<FD_TYPE_MAX)
    if (fd_tablefns[argtype])
      if (fd_tablefns[argtype]->keys)
	return (fd_tablefns[argtype]->keys)(arg);
      else return fd_err(fd_NoMethod,CantGetKeys,NULL,arg);
    else return fd_err(NotATable,"fd_getkeys",NULL,arg);
  else return fd_err(fd_BadPtr,"fd_getkeys",NULL,arg);
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
	else if (cmp==0) {FD_ADD_TO_CHOICE(hashmax->keys,fd_incref(key));}}
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
      if (cmp>=0) {FD_ADD_TO_CHOICE(hashmax->keys,fd_incref(key));}}
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

static fdtype pairgetsize(fdtype pair)
{
  return FD_INT2DTYPE(1);
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
    tmp->point=tmp->bytes; *(tmp->bytes)='\0';
    u8_printf(tmp,"   %q:   %q\n",key,values);
    if (u8_strlen(tmp->bytes)<80) u8_puts(out,tmp->bytes);
    else {
      u8_printf(out,"   %q:\n",key);
      {FD_DO_CHOICES(value,values) u8_printf(out,"      %q\n",value);}}
    fd_decref(values);}
  fd_decref(keys);
  u8_close(tmp);
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
	       FD_ADD_TO_CHOICE(results,fd_incref(key));}}}
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

  fd_register_source_file(versionid);

  fd_recyclers[fd_slotmap_type]=recycle_slotmap;
  fd_unparsers[fd_slotmap_type]=unparse_slotmap;
  fd_recyclers[fd_schemap_type]=recycle_schemap;
  fd_unparsers[fd_schemap_type]=unparse_schemap;
  fd_recyclers[fd_hashtable_type]=(fd_recycle_fn)fd_recycle_hashtable;
  fd_unparsers[fd_hashtable_type]=unparse_hashtable;
  fd_recyclers[fd_hashset_type]=(fd_recycle_fn)fd_recycle_hashset;
  fd_unparsers[fd_hashset_type]=unparse_hashset;

  fd_copiers[fd_schemap_type]=copy_schemap;
  fd_copiers[fd_slotmap_type]=copy_slotmap;
  fd_copiers[fd_hashtable_type]=copy_hashtable;

  fd_comparators[fd_slotmap_type]=compare_slotmaps;
  fd_comparators[fd_schemap_type]=compare_schemaps;

  fd_tablefns[fd_hashtable_type]=u8_malloc_type(struct FD_TABLEFNS);
  fd_tablefns[fd_hashtable_type]->get=(fd_table_get_fn)fd_hashtable_get;
  fd_tablefns[fd_hashtable_type]->add=(fd_table_add_fn)fd_hashtable_add;
  fd_tablefns[fd_hashtable_type]->drop=(fd_table_drop_fn)fd_hashtable_drop;
  fd_tablefns[fd_hashtable_type]->store=(fd_table_store_fn)fd_hashtable_set;
  fd_tablefns[fd_hashtable_type]->test=(fd_table_test_fn)hashtable_test;
  fd_tablefns[fd_hashtable_type]->getsize=(fd_table_getsize_fn)hashtable_getsize;
  fd_tablefns[fd_hashtable_type]->keys=(fd_table_keys_fn)fd_hashtable_keys;

  fd_tablefns[fd_slotmap_type]=u8_malloc_type(struct FD_TABLEFNS);
  fd_tablefns[fd_slotmap_type]->get=(fd_table_get_fn)fd_slotmap_get;
  fd_tablefns[fd_slotmap_type]->add=(fd_table_add_fn)fd_slotmap_add;
  fd_tablefns[fd_slotmap_type]->drop=(fd_table_drop_fn)fd_slotmap_drop;
  fd_tablefns[fd_slotmap_type]->store=(fd_table_store_fn)fd_slotmap_set;
  fd_tablefns[fd_slotmap_type]->test=(fd_table_test_fn)fd_slotmap_test;
  fd_tablefns[fd_slotmap_type]->getsize=(fd_table_getsize_fn)slotmap_getsize;
  fd_tablefns[fd_slotmap_type]->keys=(fd_table_keys_fn)fd_slotmap_keys;

  fd_tablefns[fd_schemap_type]=u8_malloc_type(struct FD_TABLEFNS);
  fd_tablefns[fd_schemap_type]->get=(fd_table_get_fn)fd_schemap_get;
  fd_tablefns[fd_schemap_type]->add=(fd_table_add_fn)fd_schemap_add;
  fd_tablefns[fd_schemap_type]->drop=(fd_table_drop_fn)fd_schemap_drop;
  fd_tablefns[fd_schemap_type]->store=(fd_table_store_fn)fd_schemap_set;
  fd_tablefns[fd_schemap_type]->test=(fd_table_test_fn)fd_schemap_test;
  fd_tablefns[fd_schemap_type]->getsize=(fd_table_getsize_fn)schemap_getsize;
  fd_tablefns[fd_schemap_type]->keys=(fd_table_keys_fn)fd_schemap_keys;

  fd_tablefns[fd_hashset_type]=u8_malloc_type(struct FD_TABLEFNS);
  fd_tablefns[fd_hashset_type]->get=(fd_table_get_fn)hashsetget;
  fd_tablefns[fd_hashset_type]->add=(fd_table_add_fn)hashsetstore;
  fd_tablefns[fd_hashset_type]->drop=(fd_table_drop_fn)NULL;
  fd_tablefns[fd_hashset_type]->store=(fd_table_store_fn)hashsetstore;
  fd_tablefns[fd_hashset_type]->test=NULL;
  fd_tablefns[fd_hashset_type]->getsize=(fd_table_getsize_fn)hashset_getsize;
  fd_tablefns[fd_hashset_type]->keys=(fd_table_keys_fn)hashsetelts;

  fd_tablefns[fd_pair_type]=u8_malloc_type(struct FD_TABLEFNS);
  fd_tablefns[fd_pair_type]->get=pairget;
  fd_tablefns[fd_pair_type]->test=pairtest;
  fd_tablefns[fd_pair_type]->keys=pairkeys;
  fd_tablefns[fd_pair_type]->getsize=(fd_table_getsize_fn)pairgetsize;

  {
    struct FD_COMPOUND_ENTRY *e=fd_register_compound(fd_intern("HASHTABLE"));
    e->restore=restore_hashtable;}

}


/* The CVS log for this file
   $Log: tables.c,v $
   Revision 1.132  2006/02/07 03:15:25  haase
   Fixed some locking errors

   Revision 1.131  2006/02/03 16:50:19  haase
   Fixed tableop function for table_test

   Revision 1.130  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.129  2006/01/17 19:05:53  haase
   Fixed fencepost error in slotmap drop reallocation

   Revision 1.128  2006/01/16 05:10:36  haase
   Fixed bug in sortvec_insert adding slots to empty frames

   Revision 1.127  2006/01/09 01:25:07  haase
   Added const decls

   Revision 1.126  2006/01/09 00:13:33  haase
   Added const declarations to hashset functions

   Revision 1.125  2006/01/08 22:39:11  haase
   Added more const declarations

   Revision 1.124  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.123  2006/01/07 23:12:46  haase
   Moved framerd object dtype handling into the main fd_read_dtype core, which led to substantial performanc improvements

   Revision 1.122  2006/01/07 03:43:16  haase
   Fixes to choice mergesort implementation

   Revision 1.121  2006/01/03 02:48:38  haase
   Added hashset reading/writing

   Revision 1.120  2006/01/02 22:40:12  haase
   Added fd_table_add_empty ops

   Revision 1.119  2006/01/02 19:11:16  haase
   Fixed bug in hashset_elts introduced with block choices

   Revision 1.118  2005/12/28 23:03:29  haase
   Made choices be direct blocks of elements, including various fixes, simplifications, and more detailed documentation.

   Revision 1.117  2005/12/26 21:15:02  haase
   Added fd_getsize and TABLE-SIZE

   Revision 1.116  2005/12/26 20:14:26  haase
   Further fixes to type reorganization

   Revision 1.115  2005/12/26 18:19:44  haase
   Reorganized and documented lisp pointers and conses

   Revision 1.114  2005/12/19 19:23:56  haase
   Added more hashing functions

   Revision 1.113  2005/12/19 18:47:32  haase
   Added choice hashing

   Revision 1.112  2005/12/13 19:21:44  haase
   Added nolock version of fd_hashtable_get

   Revision 1.111  2005/11/05 15:40:15  haase
   Added a dependency maintenance system for cache slot gets and tests

   Revision 1.110  2005/09/04 19:50:45  haase
   Added DROP with 2 args (means drop all values)

   Revision 1.109  2005/08/25 21:47:00  haase
   Fixed bug with MAXVAL

   Revision 1.108  2005/08/25 20:34:21  haase
   Added generic table skim functions

   Revision 1.107  2005/08/18 19:56:26  haase
   Fixed empty and singleton cases for slotmap/schemap getkeys methods

   Revision 1.106  2005/08/16 17:11:53  haase
   Added test methods for most tables, made eval inline table operations, and made probes for binding use test rather than get.

   Revision 1.105  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.104  2005/07/31 22:02:13  haase
   Added slotmap and generic max procedures

   Revision 1.103  2005/07/26 21:23:01  haase
   Made hashtable ops be noops on empty choices

   Revision 1.102  2005/07/26 15:09:02  haase
   Further implementation of modified flag for hashtables

   Revision 1.101  2005/07/25 19:39:04  haase
   Added a modified flag to hashtables

   Revision 1.100  2005/07/13 23:07:45  haase
   Added global adjuncts and LISP access to adjunct declaration

   Revision 1.99  2005/07/13 22:22:50  haase
   Added semantics for fd_test when the value argument is VOID, which just tests for any values

   Revision 1.98  2005/07/13 21:59:05  haase
   Added availability of inline table operations

   Revision 1.97  2005/07/13 21:39:31  haase
   XSLOTMAP/XSCHEMAP renaming

   Revision 1.96  2005/07/13 21:28:14  haase
   Renamed FD_OPTIMIZE_CHOICES to FD_INLINE_CHOICES

   Revision 1.95  2005/07/13 21:24:05  haase
   Added readonly schemaps and slotmaps and corresponding optimizations

   Revision 1.94  2005/07/11 17:12:34  haase
   Fixed a thread race condition in hashtable access

   Revision 1.93  2005/07/04 15:47:49  haase
   Fixed stupid bug in slotmap copying

   Revision 1.92  2005/06/20 02:06:42  haase
   Various GC related fixes

   Revision 1.91  2005/06/19 02:38:48  haase
   Made fd_table_add and fd_table_drop hashtable ops be no-ops with an empty choice as a value

   Revision 1.90  2005/06/15 13:00:01  haase
   Fixed wraparound error for hashsets

   Revision 1.89  2005/06/08 13:26:42  haase
   Moved stripped down tagger into texttools

   Revision 1.88  2005/06/04 16:52:06  haase
   Added MORPHRULE

   Revision 1.87  2005/05/30 18:04:43  haase
   Removed legacy numeric plugin layer

   Revision 1.86  2005/05/30 17:49:59  haase
   Distinguished FD_QCOMPARE and FDTYPE_COMPARE

   Revision 1.85  2005/05/29 18:24:49  haase
   Added use of pairs as simple tables (CARS are the key)

   Revision 1.84  2005/05/26 11:02:26  haase
   Made slotmaps not use sign to indicate modification

   Revision 1.83  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.82  2005/05/14 15:54:33  haase
   Fixed bugs with printing, freeing, and parsing zero length schemaps

   Revision 1.81  2005/05/12 21:39:35  haase
   Made hashtable-max take a scope to select against

   Revision 1.80  2005/05/12 21:16:57  haase
   Added hashtable-max and hashtable-skim primitives in C and Scheme

   Revision 1.79  2005/05/09 20:02:20  haase
   Have fd_get handle an empty choice as a first argument

   Revision 1.78  2005/04/30 02:46:56  haase
   Lock slotmaps when copying

   Revision 1.77  2005/04/26 16:56:30  haase
   Fixed hashtable-increment bug based on lisp/fixnum punning

   Revision 1.76  2005/04/24 22:47:33  haase
   Added fd_display_table

   Revision 1.75  2005/04/16 18:04:41  haase
   Added missing mutex unlock to fd_hashset_elts

   Revision 1.74  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.73  2005/04/14 21:45:19  haase
   Removed fd_table_ops and replaced with appropriate calls to arithmetic functions

   Revision 1.72  2005/04/14 00:27:51  haase
   Fix and declare slotmap copier

   Revision 1.71  2005/04/12 01:15:45  haase
   Added fd_overlapp and its applications

   Revision 1.70  2005/04/11 22:43:26  haase
   Made generic table ops return a consistent value

   Revision 1.69  2005/04/10 18:32:35  haase
   Capture table op no-ops

   Revision 1.68  2005/04/10 17:24:03  haase
   Fixed error in hashtable copier

   Revision 1.67  2005/04/10 01:11:08  haase
   Added slotmap and hashtable copiers

   Revision 1.66  2005/04/09 22:43:42  haase
   Made generic table ops handle non-deterministic keys correctly

   Revision 1.65  2005/04/09 19:42:26  haase
   Fixed bug with hashsets to elts and made table primitives handle non-determinism in their key argument

   Revision 1.64  2005/04/07 19:17:16  haase
   Give schemaps flags of which one replaces ->sorted, and don't normally deep copy schemap schemas

   Revision 1.63  2005/04/04 22:18:55  haase
   Minor changes to help optimize table access

   Revision 1.62  2005/04/02 20:33:05  haase
   Fix to hashable_op fd_table_default

   Revision 1.61  2005/04/01 18:47:28  haase
   Added more branch predictions

   Revision 1.60  2005/04/01 15:11:11  haase
   Adapted the string hash algorithm and added some branch expectations

   Revision 1.59  2005/03/30 15:30:00  haase
   Made calls to new seterr do appropriate strdups

   Revision 1.58  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.57  2005/03/28 19:11:20  haase
   Don't bother devoiding empty hashtables

   Revision 1.56  2005/03/28 19:09:56  haase
   Don't bother devoiding uninitialized tables

   Revision 1.55  2005/03/14 05:49:31  haase
   Updated comments and internal documentation

   Revision 1.54  2005/03/08 04:19:45  haase
   Fixed schema copying bug

   Revision 1.53  2005/03/07 14:09:53  haase
   Fixed old, empty iteration and defined fd_devoid_hashtable

   Revision 1.52  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.51  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.50  2005/03/01 19:44:27  haase
   Fixed some error messages

   Revision 1.49  2005/02/26 22:31:41  haase
   Remodularized choice and oid add into xtables.c

   Revision 1.48  2005/02/26 21:36:48  haase
   Added fd_slotmap_delete and fd_table_replace_novoid operations

   Revision 1.47  2005/02/25 20:13:22  haase
   Fixed incref and decref references for double evaluation

   Revision 1.46  2005/02/23 22:49:27  haase
   Created generalized compound registry and made compound dtypes and #< reading use it

   Revision 1.45  2005/02/19 16:26:27  haase
   Init slotmap values to empty rather than void

   Revision 1.44  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.43  2005/02/11 04:45:29  haase
   Added _noref hashtable ops and put in some branch predictions

   Revision 1.42  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
