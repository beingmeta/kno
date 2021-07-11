/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES KNO_DO_INLINE
#define KNO_INLINE_TABLES 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/hash.h"
#include "kno/tables.h"
#include "kno/numbers.h"

#include <libu8/u8printf.h>

u8_condition kno_NoSuchKey=_("No such key");
u8_condition kno_ReadOnlyTable=_("Read-Only table");
u8_condition kno_ReadOnlyHashtable=_("Read-Only hashtable");
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

static int resize_hashtable(struct KNO_HASHTABLE *ptr,int n_slots,int need_lock);

#if DEBUGGING
#include <stdio.h>
#endif

#include <math.h>
#include <limits.h>

#define flip_word(x)                                                    \
  (((x>>24)&0xff) | ((x>>8)&0xff00) | ((x&0xff00)<<8) | ((x&0xff)<<24))
#define compute_offset(hash,size) (hash%size)

KNO_FASTOP int numcompare(lispval x,lispval y)
{
  if ((FIXNUMP(x)) && (FIXNUMP(y)))
    if ((FIX2INT(x))>(FIX2INT(y))) return 1;
    else if ((FIX2INT(x))<(FIX2INT(y))) return -1;
    else return 0;
  else if ((KNO_FLONUMP(x)) && (KNO_FLONUMP(y)))
    if ((KNO_FLONUM(x))>(KNO_FLONUM(y))) return 1;
    else if ((KNO_FLONUM(x))<(KNO_FLONUM(y))) return -1;
    else return 0;
  else return kno_numcompare(x,y);
}

int   kno_init_smap_size = KNO_INIT_SMAP_SIZE;
int   kno_init_hash_size = KNO_INIT_HASH_SIZE;

unsigned int kno_hash_bigthresh = KNO_HASH_BIGTHRESH;

/* Debugging tools */

#if DEBUGGING
static lispval look_for_key=VOID;

static void note_key(lispval key,struct KNO_HASHTABLE *h)
{
  fprintf(stderr,_("Noticed %s on %lx\n"),kno_lisp2string(key),h);
}

#define KEY_CHECK(key,ht)                               \
  if (LISP_EQUAL(key,look_for_key)) note_key(key,ht)
#else
#define KEY_CHECK(key,ht)
#endif

/* Keyvecs */

KNO_EXPORT
struct KNO_KEYVAL *_kno_keyvals_get
(lispval kno_key,struct KNO_KEYVAL *keyvals,int size)
{
  return kno_keyvals_get(kno_key,keyvals,size);
}

/* Temporary references */
KNO_EXPORT
struct KNO_KEYVAL *_kno_keyvec_get
(lispval kno_key,struct KNO_KEYVAL *keyvals,int size)
{
  return _kno_keyvals_get(kno_key,keyvals,size);
}

KNO_EXPORT
struct KNO_KEYVAL *kno_keyvals_insert
(lispval key,struct KNO_KEYVAL **keyvalp,
 int *sizep,int *spacep,int max_space,
 int freedata)
{
  int size=*sizep;
  int space= (spacep) ? (*spacep) : (0);
  struct KNO_KEYVAL *keyvals=*keyvalp;
  const struct KNO_KEYVAL *scan=keyvals, *limit=scan+size;
  if (keyvals) {
    if (ATOMICP(key))
      while (scan<limit)
        if (scan->kv_key==key)
          return (struct KNO_KEYVAL *)scan;
        else scan++;
    else while (scan<limit)
           if (LISP_EQUAL(scan->kv_key,key))
             return (struct KNO_KEYVAL *) scan;
           else scan++;}
  if (size<space)  {
    keyvals[size].kv_key=kno_getref(key);
    keyvals[size].kv_val=EMPTY;
    *sizep=size+1;
    return &(keyvals[size]);}
  else if (space < max_space) {
    size_t new_space = (spacep) ? (space+4) : (space+1);
    struct KNO_KEYVAL *nkeyvals= ((keyvals) && (freedata)) ?
      (u8_realloc_n(keyvals,new_space,struct KNO_KEYVAL)) :
      (u8_alloc_n(new_space,struct KNO_KEYVAL));
    if ((keyvals) && (!(freedata)))
      memcpy(nkeyvals,keyvals,(size)*KNO_KEYVAL_LEN);
    if (nkeyvals != keyvals)
      *keyvalp=nkeyvals;
    nkeyvals[size].kv_key=kno_getref(key);
    nkeyvals[size].kv_val=EMPTY;
    if (spacep) *spacep=new_space;
    *sizep=size+1;
    return &(nkeyvals[size]);}
  else return NULL;
}

/* Sort map */

KNO_FASTOP void swap_keyvals(struct KNO_KEYVAL *a,struct KNO_KEYVAL *b)
{
  struct KNO_KEYVAL tmp; tmp=*a; *a=*b; *b=tmp;
}

static void atomic_sort_keyvals(struct KNO_KEYVAL *v,int n)
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

static void cons_sort_keyvals(struct KNO_KEYVAL *v,int n)
{
  unsigned i, j, ln, rn;
  while (n > 1) {
    swap_keyvals(&v[0], &v[n/2]);
    for (i = 0, j = n; ; ) {
      do --j; while (__kno_cons_compare(v[j].kv_key,v[0].kv_key)>0);
      do ++i; while (i < j && ((__kno_cons_compare(v[i].kv_key,v[0].kv_key))<0));
      if (i >= j) break; else {}
      swap_keyvals(&v[i], &v[j]);}
    swap_keyvals(&v[j], &v[0]);
    ln = j;
    rn = n - ++j;
    if (ln < rn) {
      cons_sort_keyvals(v, ln); v += j; n = rn;}
    else {cons_sort_keyvals(v + j, rn); n = ln;}}
}

static void sort_keyvals(struct KNO_KEYVAL *v,int n)
{
  int i=0; while (i<n)
             if (ATOMICP(v[i].kv_key)) i++;
             else {
               cons_sort_keyvals(v,n);
               return;}
  atomic_sort_keyvals(v,n);
}

KNO_EXPORT struct KNO_KEYVAL *_kno_sortvec_get
(lispval key,struct KNO_KEYVAL *keyvals,int size)
{
  return __kno_sortvec_get(key,keyvals,size);
}

KNO_EXPORT struct KNO_KEYVAL *kno_sortvec_insert
(lispval key,
 struct KNO_KEYVAL **kvp,
 int *sizep,int *spacep,int max_space,
 int freedata)
{
  struct KNO_KEYVAL *keyvals=*kvp;
  int size=*sizep, space=((spacep)?(*spacep):(0)), dir=0;
  struct KNO_KEYVAL *bottom=keyvals, *top=bottom+size-1;
  struct KNO_KEYVAL *limit=bottom+size, *middle=bottom+size/2;
  if (keyvals == NULL) {
    *kvp=keyvals=u8_alloc(struct KNO_KEYVAL);
    memset(keyvals,0,KNO_KEYVAL_LEN);
    if (keyvals==NULL) return NULL;
    keyvals->kv_key=kno_getref(key);
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
      int comparison = __kno_cons_compare(key,middle->kv_key);
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
    struct KNO_KEYVAL *new_keyvals=
      ( (freedata) ? (u8_realloc_n(keyvals,new_space,struct KNO_KEYVAL)) :
        (u8_extalloc(keyvals,(new_space*KNO_KEYVAL_LEN),(space*KNO_KEYVAL_LEN))));
    if ( new_keyvals == NULL )
      return NULL;
    keyvals = *kvp = new_keyvals;
    space   = *spacep = new_space;
    bottom = new_keyvals;
    middle = new_keyvals+mpos;
    top = new_keyvals+size;}
  /* Now, insert the value */
  struct KNO_KEYVAL *insert_point = (dir<0) ? (middle) : (middle+1);
  int ipos = insert_point-keyvals;
  size_t bytes_to_move = (size-ipos)*KNO_KEYVAL_LEN;
  memmove(insert_point+1,insert_point,bytes_to_move);
  insert_point->kv_key=kno_getref(key);
  insert_point->kv_val=EMPTY;
  *sizep=size+1;
  return insert_point;
}

static int slotmap_fail(struct KNO_SLOTMAP *sm,u8_context caller)
{
  char dbuf[64];
  int size = sm->n_slots, space = sm->n_allocd;
  if (space >= SHRT_MAX)
    return KNO_ERR(-1,"SlotmapOverflow",caller,
                   u8_sprintf(dbuf,64,"%d:%d>%d",size,space,SHRT_MAX),
                   (lispval)sm);
  else return KNO_ERR(-1,"SlotmapInsertFail",caller,
                      u8_sprintf(dbuf,64,"%d:%d",size,space),
                      (lispval)sm);
}

KNO_EXPORT lispval _kno_slotmap_get
(struct KNO_SLOTMAP *sm,lispval key,lispval dflt)
{
  return __kno_slotmap_get(sm,key,dflt);
}

KNO_EXPORT lispval _kno_slotmap_test(struct KNO_SLOTMAP *sm,lispval key,lispval val)
{
  return __kno_slotmap_test(sm,key,val);
}

KNO_EXPORT int kno_slotmap_store(struct KNO_SLOTMAP *sm,lispval key,lispval value)
{
  int unlock=0;
  KNO_CHECK_TYPE_RET(sm,kno_slotmap_type);
  if (KNO_TROUBLEP(key))
    return kno_interr(key);
  else if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (KNO_EMPTYP(key))
    return 0;
  else if (KNO_XTABLE_READONLYP(sm))
    return KNO_ERR(-1,kno_ReadOnlyTable,"kno_slotmap_store",NULL,key);
  else if (KNO_XTABLE_USELOCKP(sm)) {
    u8_write_lock(&sm->table_rwlock);
    unlock=1;}
  else NO_ELSE;
  {
    int rv=0;
    int cur_nslots=KNO_XSLOTMAP_NUSED(sm), nslots=cur_nslots;
    int cur_allocd=KNO_XSLOTMAP_NALLOCATED(sm), allocd=cur_allocd;
    struct KNO_KEYVAL *cur_keyvals=sm->sm_keyvals;;
    struct KNO_KEYVAL *result=
      (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_SORT_KEYVALS)) ?
      (kno_sortvec_insert(key,&(sm->sm_keyvals),
                          &nslots,&allocd,SHRT_MAX,
			  (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_FREE_KEYVALS)))) :
      (kno_keyvals_insert(key,&(sm->sm_keyvals),
                         &nslots,&allocd,SHRT_MAX,
			 (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_FREE_KEYVALS))));
    if (RARELY(result==NULL)) {
      if (unlock) u8_rw_unlock(&(sm->table_rwlock));
      return slotmap_fail(sm,"kno_slotmap_store");}
    if (sm->sm_keyvals!=cur_keyvals) {
      KNO_XTABLE_SET_BIT(sm,KNO_SLOTMAP_FREE_KEYVALS,1);}
    kno_decref(result->kv_val);
    result->kv_val=kno_incref(value);
    KNO_XTABLE_SET_MODIFIED(sm,1);
    if (cur_allocd != allocd) { KNO_XSLOTMAP_SET_NALLOCATED(sm,allocd); }
    if (cur_nslots != nslots) {
      KNO_XSLOTMAP_SET_NSLOTS(sm,nslots);
      rv=1;}
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return rv;}
}

KNO_EXPORT int kno_slotmap_add(struct KNO_SLOTMAP *sm,lispval key,lispval value)
{
  int unlock=0, retval=0;
  KNO_CHECK_TYPE_RET(sm,kno_slotmap_type);
  if (KNO_TROUBLEP(key))
    return 0;
  else if (KNO_TROUBLEP(value))
    return kno_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (EMPTYP(value))
    return 0;
  else if (KNO_XTABLE_READONLYP(sm))
    return KNO_ERR(-1,kno_ReadOnlyTable,"kno_slotmap_store",NULL,key);
  else if (KNO_XTABLE_USELOCKP(sm)) {
    u8_write_lock(&sm->table_rwlock);
    unlock=1;}
  else NO_ELSE;
  {
    int cur_size=KNO_XSLOTMAP_NUSED(sm), size=cur_size;
    int cur_space=KNO_XSLOTMAP_NALLOCATED(sm), space=cur_space;
    struct KNO_KEYVAL *cur_keyvals=sm->sm_keyvals;
    struct KNO_KEYVAL *result=
      (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_SORT_KEYVALS)) ?
      (kno_sortvec_insert(key,&(sm->sm_keyvals),
                          &size,&space,SHRT_MAX,
			  (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_FREE_KEYVALS)))) :
      (kno_keyvals_insert(key,&(sm->sm_keyvals),
                         &size,&space,SHRT_MAX,
                         (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_FREE_KEYVALS))));
    if (RARELY(result==NULL)) {
      if (unlock) u8_rw_unlock(&sm->table_rwlock);
      return slotmap_fail(sm,"kno_slotmap_add");}
    /* If this allocated a new keyvals structure, it needs to be
       freed.  (sm_free_kevyvals==0) when the keyvals are allocated at
       the end of the slotmap structure itself. */
    if (sm->sm_keyvals!=cur_keyvals) {
      KNO_XTABLE_SET_BIT(sm,KNO_SLOTMAP_FREE_KEYVALS,1);}
    kno_incref(value);
    if ( (result->kv_val == VOID) || (result->kv_val == KNO_UNBOUND) )
      result->kv_val=value;
    else {CHOICE_ADD(result->kv_val,value);}
    KNO_XTABLE_SET_MODIFIED(sm,1);
    if (cur_space != space) {
      KNO_XSLOTMAP_SET_NALLOCATED(sm,space); }
    if (cur_size  != size) {
      KNO_XSLOTMAP_SET_NSLOTS(sm,size);
      retval=1;}}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return retval;
}

KNO_EXPORT int kno_slotmap_drop(struct KNO_SLOTMAP *sm,lispval key,lispval value)
{
  struct KNO_KEYVAL *result; int size, unlock=0;
  KNO_CHECK_TYPE_RET(sm,kno_slotmap_type);
  if ((KNO_TROUBLEP(key)))
    return kno_interr(key);
  else if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (KNO_XTABLE_READONLYP(sm))
    return KNO_ERR(-1,kno_ReadOnlyTable,"kno_slotmap_store",NULL,key);
  else if (KNO_XTABLE_USELOCKP(sm)) {
    u8_write_lock(&sm->table_rwlock);
    unlock=1;}
  else NO_ELSE;
  size=KNO_XSLOTMAP_NUSED(sm);
  result=kno_keyvals_get(key,sm->sm_keyvals,size);
  if (result) {
    lispval newval=((VOIDP(value)) ? (EMPTY) :
                    (kno_difference(result->kv_val,value)));
    if ( (newval == result->kv_val) &&
         (!(EMPTYP(newval))) ) {
      /* This is the case where, for example, value isn't on the slot.
         But we incref'd newvalue/result->kv_val, so we decref it.
         However, if the slot is already empty (for whatever reason),
         dropping the slot actually removes it from the slotmap. */
      kno_decref(newval);}
    else {
      KNO_XTABLE_SET_MODIFIED(sm,1);
      if (EMPTYP(newval)) {
        int entries_to_move=(size-(result-sm->sm_keyvals))-1;
        kno_decref(result->kv_key); kno_decref(result->kv_val);
        memmove(result,result+1,entries_to_move*KNO_KEYVAL_LEN);
        KNO_XSLOTMAP_SET_NSLOTS(sm,size-1);}
      else {
        kno_decref(result->kv_val);
        result->kv_val=newval;}}}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  if (result)
    return 1;
  else return 0;
}

KNO_EXPORT int kno_slotmap_delete(struct KNO_SLOTMAP *sm,lispval key)
{
  struct KNO_KEYVAL *result; int size, unlock=0;
  KNO_CHECK_TYPE_RET(sm,kno_slotmap_type);
  if ((KNO_TROUBLEP(key)))
    return kno_interr(key);
  else if (EMPTYP(key))
    return 0;
  else if (KNO_XTABLE_READONLYP(sm))
    return KNO_ERR(-1,kno_ReadOnlyTable,"kno_slotmap_store",NULL,key);
  else if (KNO_XTABLE_USELOCKP(sm)) {
    u8_write_lock(&sm->table_rwlock);
    unlock=1;}
  else NO_ELSE;
  size=KNO_XSLOTMAP_NUSED(sm);
  result=kno_keyvals_get(key,sm->sm_keyvals,size);
  if (result) {
    int entries_to_move=(size-(result-sm->sm_keyvals))-1;
    kno_decref(result->kv_key); kno_decref(result->kv_val);
    memmove(result,result+1,entries_to_move*KNO_KEYVAL_LEN);
    KNO_XTABLE_SET_MODIFIED(sm,1);
    KNO_XSLOTMAP_SET_NSLOTS(sm,size-1);}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  if (result) return 1; else return 0;
}

static int slotmap_getsize(struct KNO_SLOTMAP *ptr)
{
  KNO_CHECK_TYPE_RET(ptr,kno_slotmap_type);
  return KNO_XSLOTMAP_NUSED(ptr);
}

static int slotmap_modified(struct KNO_SLOTMAP *ptr,int flag)
{
  KNO_CHECK_TYPE_RET(ptr,kno_slotmap_type);
  int modified=KNO_XTABLE_MODIFIEDP(ptr);
  if (flag<0)
    return modified;
  else if (flag) {
    KNO_XTABLE_SET_MODIFIED(ptr,1);
    return modified;}
  else {
    KNO_XTABLE_SET_MODIFIED(ptr,0);
    return modified;}
}

static int slotmap_readonly(struct KNO_SLOTMAP *ptr,int flag)
{
  KNO_CHECK_TYPE_RET(ptr,kno_slotmap_type);
  int readonly=KNO_XTABLE_READONLYP(ptr);
  if (flag<0)
    return readonly;
  else if (flag) {
    KNO_XTABLE_SET_READONLY(ptr,1);
    return readonly;}
  else {
    KNO_XTABLE_SET_READONLY(ptr,0);
    return readonly;}
}

KNO_EXPORT lispval kno_slotmap_keys(struct KNO_SLOTMAP *sm)
{
  struct KNO_KEYVAL *scan, *limit; int unlock=0;
  struct KNO_CHOICE *result;
  lispval *write; int size, atomic=1;
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_slotmap_type);
  if (KNO_XTABLE_USELOCKP(sm)) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  size=KNO_XSLOTMAP_NUSED(sm);
  scan=sm->sm_keyvals;
  limit=scan+size;
  if (size==0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return EMPTY;}
  else if (size==1) {
    lispval key=kno_incref(scan->kv_key);
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return key;}
  /* Otherwise, copy the keys into a choice vector. */
  result=kno_alloc_choice(size);
  write=(lispval *)KNO_XCHOICE_DATA(result);
  while (scan < limit) {
    lispval key=(scan++)->kv_key;
    if (!(KNO_VOIDP(key))) {
      if (CONSP(key)) {kno_incref(key); atomic=0;}
      *write++=key;}}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return kno_init_choice(result,size,NULL,
                         ((KNO_CHOICE_REALLOC)|(KNO_CHOICE_DOSORT)|
                          ((atomic)?(KNO_CHOICE_ISATOMIC):
                           (KNO_CHOICE_ISCONSES))));
}

KNO_EXPORT lispval kno_slotmap_values(struct KNO_SLOTMAP *sm)
{
  int unlock=0;
  struct KNO_PRECHOICE *prechoice; lispval results;
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_slotmap_type);
  if (KNO_XSLOTMAP_NUSED(sm) == 0) return KNO_EMPTY;
  if (KNO_XTABLE_USELOCKP(sm)) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  int size=KNO_XSLOTMAP_NUSED(sm);
  if (size == 0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return EMPTY;}
  struct KNO_KEYVAL *scan = sm->sm_keyvals, *limit = scan+size;
  if (size==1) {
    lispval value=kno_incref(scan->kv_val);
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return value;}
  /* Otherwise, copy the keys into a choice vector. */
  results=kno_init_prechoice(NULL,7*(size),0);
  prechoice=KNO_XPRECHOICE(results);
  while (scan < limit) {
    lispval value=(scan++)->kv_val;
    if (CONSP(value)) {kno_incref(value);}
    kno_prechoice_add(prechoice,value);}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  /* Note that we can assume that the choice is sorted because the keys are. */
  return kno_simplify_choice(results);
}

KNO_EXPORT lispval *kno_slotmap_keyvec_n(struct KNO_SLOTMAP *sm,int *lenp)
{
  int unlock = 0;
  if (!((KNO_CONS_TYPEOF(sm))==kno_slotmap_type)) {
    kno_seterr(kno_TypeError,"kno_slotmap_keyvec_n",NULL,(lispval)sm);
    *lenp = -1; return NULL;}
  if (KNO_XTABLE_USELOCKP(sm)) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  int n_keys = KNO_XSLOTMAP_NUSED(sm), i = 0;
  if (n_keys == 0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    *lenp=0; return NULL;}
  else {
    struct KNO_KEYVAL *scan = sm->sm_keyvals, *limit=scan+n_keys;
    lispval *keys = u8_alloc_n(n_keys,lispval);
    while (scan < limit) {
      lispval key=(scan++)->kv_key;
      if (!(KNO_VOIDP(key))) {
	kno_incref(key);
	keys[i] = key;}}
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    *lenp = i;
    return keys;}
}

KNO_EXPORT lispval kno_slotmap_assocs(struct KNO_SLOTMAP *sm)
{
  struct KNO_KEYVAL *scan, *limit; int unlock=0;
  struct KNO_PRECHOICE *prechoice; lispval results;
  int size;
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_slotmap_type);
  if (KNO_XTABLE_USELOCKP(sm)) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  size=KNO_XSLOTMAP_NUSED(sm);
  scan=sm->sm_keyvals;
  limit=scan+size;
  if (size==0) {
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return EMPTY;}
  else if (size==1) {
    lispval key=scan->kv_key, value=scan->kv_val;
    kno_incref(key); kno_incref(value);
    if (unlock) u8_rw_unlock(&sm->table_rwlock);
    return kno_init_pair(NULL,key,value);}
  /* Otherwise, copy the keys into a choice vector. */
  results=kno_init_prechoice(NULL,7*(size),0);
  prechoice=KNO_XPRECHOICE(results);
  while (scan < limit) {
    lispval key=scan->kv_key, value=scan->kv_val;
    lispval assoc=kno_init_pair(NULL,key,value);
    kno_incref(key); kno_incref(value); scan++;
    kno_prechoice_add(prechoice,assoc);}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return kno_simplify_choice(results);
}

KNO_EXPORT lispval kno_slotmap_max
(struct KNO_SLOTMAP *sm,lispval scope,lispval *maxvalp)
{
  lispval maxval=VOID, result=EMPTY;
  struct KNO_KEYVAL *scan, *limit;
  int size, unlock=0;
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_slotmap_type);
  if (EMPTYP(scope)) return result;
  if (KNO_XTABLE_USELOCKP(sm)) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  size=KNO_XSLOTMAP_NUSED(sm);
  scan=sm->sm_keyvals;
  limit=scan+size;
  while (scan<limit) {
    if ((VOIDP(scope)) || (kno_overlapp(scan->kv_key,scope))) {
      if (EMPTYP(scan->kv_val)) {}
      else if (NUMBERP(scan->kv_val)) {
        if (VOIDP(maxval)) {
          result=kno_incref(scan->kv_key);
          maxval=kno_incref(scan->kv_val);}
        else {
          int cmp=numcompare(scan->kv_val,maxval);
          if (cmp>0) {
            kno_decref(result); kno_decref(maxval);
            result=kno_incref(scan->kv_key);
            maxval=kno_incref(scan->kv_val);}
          else if (cmp==0) {
            kno_incref(scan->kv_key);
            CHOICE_ADD(result,scan->kv_key);}}}}
    scan++;}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  if ((maxvalp) && (NUMBERP(maxval)))
    *maxvalp=kno_incref(maxval);
  return result;
}

KNO_EXPORT lispval kno_slotmap_skim(struct KNO_SLOTMAP *sm,lispval maxval,
                                    lispval scope)
{
  lispval result=EMPTY; int unlock=0;
  struct KNO_KEYVAL *scan, *limit; int size;
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_slotmap_type);
  if (EMPTYP(scope))
    return result;
  if (KNO_XTABLE_USELOCKP(sm)) {
    u8_read_lock(&sm->table_rwlock);
    unlock=1;}
  size=KNO_XSLOTMAP_NUSED(sm);
  scan=sm->sm_keyvals;
  limit=scan+size;
  while (scan<limit) {
    if ((VOIDP(scope)) || (kno_overlapp(scan->kv_key,scope)))
      if (NUMBERP(scan->kv_val)) {
        int cmp=numcompare(scan->kv_val,maxval);
        if (cmp>=0) {
          kno_incref(scan->kv_key);
          CHOICE_ADD(result,scan->kv_key);}}
    scan++;}
  if (unlock) u8_rw_unlock(&sm->table_rwlock);
  return result;
}

KNO_EXPORT lispval kno_init_slotmap
(struct KNO_SLOTMAP *ptr,
 int len,struct KNO_KEYVAL *data)
{
  if (ptr == NULL) {
    ptr=u8_alloc(struct KNO_SLOTMAP);
    KNO_INIT_FRESH_CONS(ptr,kno_slotmap_type);}
  else {
    KNO_SET_CONS_TYPE(ptr,kno_slotmap_type);}
  ptr->n_allocd=len;
  if (data) {
    ptr->sm_keyvals=data;
    ptr->n_slots=len;}
  else if (len) {
    ptr->sm_keyvals=u8_alloc_n(len,struct KNO_KEYVAL);
    ptr->n_slots=0;}
  else {
    ptr->n_slots=0;
    ptr->sm_keyvals=NULL;}
  ptr->table_bits = KNO_TABLE_USELOCKS|KNO_SLOTMAP_FREE_KEYVALS;
  u8_init_rwlock(&(ptr->table_rwlock));
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval kno_make_slotmap(int space,int len,struct KNO_KEYVAL *data)
{
  struct KNO_SLOTMAP *ptr=
    u8_malloc((KNO_SLOTMAP_LEN)+
              (space*(KNO_KEYVAL_LEN)));
  struct KNO_KEYVAL *kv=
    ((struct KNO_KEYVAL *)(((unsigned char *)ptr)+KNO_SLOTMAP_LEN));
  int i=0;
  KNO_INIT_STRUCT(ptr,struct KNO_SLOTMAP);
  KNO_INIT_CONS(ptr,kno_slotmap_type);
  ptr->n_allocd=space; ptr->n_slots=len;
  if (data) while (i<len) {
      lispval key=data[i].kv_key, val=data[i].kv_val;
      kv[i].kv_key=kno_not_static(key);
      kv[i].kv_val=kno_not_static(val);
      i++;}
  while (i<space) {
    kv[i].kv_key=VOID;
    kv[i].kv_val=VOID;
    i++;}
  ptr->sm_keyvals=kv;
  ptr->table_bits = KNO_TABLE_USELOCKS;
  u8_init_rwlock(&(ptr->table_rwlock));
  return LISP_CONS(ptr);
}

KNO_EXPORT void kno_reset_slotmap(struct KNO_SLOTMAP *ptr)
{
  int unlock = 0;
  if (KNO_XTABLE_USELOCKP(ptr)) {
    u8_write_lock(&(ptr->table_rwlock));
    unlock=1;}
  struct KNO_KEYVAL *scan = KNO_XSLOTMAP_KEYVALS(ptr);
  struct KNO_KEYVAL *limit = scan + KNO_XSLOTMAP_NUSED(ptr);
  while (scan<limit) {
    lispval key = scan->kv_key;
    lispval val = scan->kv_val;
    kno_decref(key); kno_decref(val);
    scan->kv_key=VOID;
    scan->kv_val=VOID;
    scan++;}
  ptr->n_slots=0;
  if (unlock) u8_rw_unlock(&(ptr->table_rwlock));
}

static lispval copy_slotmap(lispval smap,int flags)
{
  struct KNO_SLOTMAP *cur=kno_consptr(kno_slotmap,smap,kno_slotmap_type);
  struct KNO_SLOTMAP *fresh; int unlock=0;
  if (!(KNO_XTABLE_BITP(cur,KNO_SLOTMAP_FREE_KEYVALS))) {
    /* If the original doesn't 'own' its keyvals, it's probably block
       allocated (with the keyvals in memory right after the slotmap
       struct. In this case, we assume we want to give the copy the
       same properties. */
    lispval copy;
    struct KNO_SLOTMAP *consed;
    struct KNO_KEYVAL *kvals;
    int i=0, len;
    copy=kno_make_slotmap(cur->n_allocd,cur->n_slots,cur->sm_keyvals);
    consed=(struct KNO_SLOTMAP *)copy;
    kvals=consed->sm_keyvals; len=consed->n_slots;
    if (KNO_XTABLE_USELOCKP(cur)) {
      kno_read_lock_table(cur);
      unlock=1;}
    while (i<len) {
      lispval key=kvals[i].kv_key, val=kvals[i].kv_val;
      if ((flags&KNO_FULL_COPY)||(KNO_STATICP(key)))
        kvals[i].kv_key=kno_copier(key,flags);
      else kno_incref(key);
      if ((flags&KNO_FULL_COPY)||(KNO_STATICP(val)))
        kvals[i].kv_val=kno_copier(val,flags);
      else kno_incref(val);
      i++;}
    if (unlock) kno_unlock_table(cur);
    return copy;}
  else fresh=u8_alloc(struct KNO_SLOTMAP);
  KNO_INIT_STRUCT(fresh,struct KNO_SLOTMAP);
  KNO_INIT_CONS(fresh,kno_slotmap_type);
  if (KNO_XTABLE_USELOCKP(cur)) {
    kno_read_lock_table(cur);
    unlock=1;}
  int bits = KNO_TABLE_USELOCKS;
  if (KNO_XSLOTMAP_NUSED(cur)) {
    int n=KNO_XSLOTMAP_NUSED(cur);
    struct KNO_KEYVAL *read=cur->sm_keyvals, *read_limit=read+n;
    struct KNO_KEYVAL *write=u8_alloc_n(n,struct KNO_KEYVAL);
    bits |= KNO_SLOTMAP_FREE_KEYVALS;
    fresh->n_allocd=fresh->n_slots=n;
    fresh->sm_keyvals=write;
    memset(write,0,n*KNO_KEYVAL_LEN);
    while (read<read_limit) {
      lispval key=read->kv_key, val=read->kv_val; read++;
      if (CONSP(key)) {
        if ((flags&KNO_FULL_COPY)||(KNO_STATICP(key)))
          write->kv_key=kno_copier(key,flags);
        else write->kv_key=kno_incref(key);}
      else write->kv_key=key;
      if (CONSP(val))
        if (PRECHOICEP(val))
          write->kv_val=kno_make_simple_choice(val);
        else if ((flags&KNO_FULL_COPY)||(KNO_STATICP(val)))
          write->kv_val=kno_copier(val,flags);
        else write->kv_val=kno_incref(val);
      else write->kv_val=val;
      write++;}}
  else {
    fresh->n_allocd=fresh->n_slots=0;
    fresh->sm_keyvals=NULL;}
  fresh->table_bits = bits;
  if (unlock) kno_unlock_table(cur);
  u8_init_rwlock(&(fresh->table_rwlock));
  return LISP_CONS(fresh);
}

KNO_EXPORT int kno_copy_slotmap(struct KNO_SLOTMAP *src,
                                struct KNO_SLOTMAP *dest)
{
  if (dest->sm_keyvals) {
    if (dest->n_slots) {
      struct KNO_KEYVAL *scan = dest->sm_keyvals;
      struct KNO_KEYVAL *limit = scan + dest->n_slots;
      while (scan<limit) {
        lispval key = scan->kv_key;
        lispval val = scan->kv_val;
        kno_decref(key);
        kno_decref(val);
        scan->kv_key=VOID;
        scan->kv_val=VOID;
        scan++;}}
    if (KNO_XTABLE_BITP(dest,KNO_SLOTMAP_FREE_KEYVALS))
      u8_free(dest->sm_keyvals);
    dest->sm_keyvals=NULL;
    u8_destroy_rwlock(&(dest->table_rwlock));}
  dest->n_slots = src->n_slots;
  dest->n_allocd = src->n_allocd;
  dest->sm_keyvals = u8_alloc_n(src->n_allocd,struct KNO_KEYVAL);
  dest->table_bits = src->table_bits |
    KNO_SLOTMAP_FREE_KEYVALS |
    KNO_TABLE_USELOCKS;
  u8_init_rwlock(&(dest->table_rwlock));
  struct KNO_KEYVAL *read = src->sm_keyvals;
  struct KNO_KEYVAL *limit = read + src->n_slots;
  struct KNO_KEYVAL *write = dest->sm_keyvals;
  while (read < limit) {
    write->kv_key = read->kv_key; kno_incref(write->kv_key);
    write->kv_val = read->kv_val; kno_incref(write->kv_val);
    write++; read++;}
  return dest->n_slots;
}

KNO_EXPORT void kno_free_keyvals(struct KNO_KEYVAL *kvals,int n_kvals)
{
  int i=0; while (i<n_kvals) {
    lispval key = kvals[i].kv_key;
    lispval val = kvals[i].kv_val;
    kno_decref(key);
    kno_decref(val);
    i++;}
}

KNO_EXPORT void kno_free_slotmap(struct KNO_SLOTMAP *c)
{
  struct KNO_SLOTMAP *sm=(struct KNO_SLOTMAP *)c;
  int unlock=0;
  if (KNO_XTABLE_USELOCKP(sm)) {
    kno_write_lock_table(sm);
    unlock=1;}
  int slotmap_size=KNO_XSLOTMAP_NUSED(sm);
  const struct KNO_KEYVAL *scan=sm->sm_keyvals;
  const struct KNO_KEYVAL *limit=sm->sm_keyvals+slotmap_size;
  while (scan < limit) {
    kno_decref(scan->kv_key);
    kno_decref(scan->kv_val);
    scan++;}
  if (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_FREE_KEYVALS))
    u8_free(sm->sm_keyvals);
  if (unlock) kno_unlock_table(sm);
  u8_destroy_rwlock(&(sm->table_rwlock));
  memset(sm,0,KNO_SLOTMAP_LEN);
}


static void recycle_slotmap(struct KNO_RAW_CONS *c)
{
  struct KNO_SLOTMAP *sm=(struct KNO_SLOTMAP *)c;
  kno_free_slotmap(sm);
  u8_free(sm);
}

static int unparse_slotmap(u8_output out,lispval x)
{
  int unlock = 0;
  struct KNO_SLOTMAP *sm=KNO_XSLOTMAP(x);
  if (KNO_XTABLE_USELOCKP(sm)) {
    kno_read_lock_table(sm);
    unlock=1;}
  {
    int slotmap_size=KNO_XSLOTMAP_NUSED(sm);
    const struct KNO_KEYVAL *scan=sm->sm_keyvals;
    const struct KNO_KEYVAL *limit=sm->sm_keyvals+slotmap_size;
    u8_puts(out,"#[");
    if (scan<limit) {
      kno_unparse(out,scan->kv_key); u8_putc(out,' ');
      kno_unparse(out,scan->kv_val);
      scan++;}
    while (scan< limit) {
      u8_putc(out,' ');
      kno_unparse(out,scan->kv_key); u8_putc(out,' ');
      kno_unparse(out,scan->kv_val);
      scan++;}
    u8_puts(out,"]");
  }
  if (unlock) kno_unlock_table(sm);
  return 1;
}
static int compare_slotmaps(lispval x,lispval y,kno_compare_flags flags)
{
  int result=0; int unlockx=0, unlocky=0;
  struct KNO_SLOTMAP *smx=(struct KNO_SLOTMAP *)x;
  struct KNO_SLOTMAP *smy=(struct KNO_SLOTMAP *)y;
  int compare_lengths=(!(flags&KNO_COMPARE_RECURSIVE));
  int compare_slots=(flags&KNO_COMPARE_SLOTS);
  if (KNO_XTABLE_USELOCKP(smx)) {kno_read_lock_table(smx); unlockx=1;}
  if (KNO_XTABLE_USELOCKP(smy)) {kno_read_lock_table(smy); unlocky=1;}
  int xsize=KNO_XSLOTMAP_NUSED(smx), ysize=KNO_XSLOTMAP_NUSED(smy);
  {
    if ((compare_lengths) && (xsize>ysize)) result=1;
    else if ((compare_lengths) && (xsize<ysize)) result=-1;
    else if (!(compare_slots)) {
      const struct KNO_KEYVAL *xkeyvals=smx->sm_keyvals;
      const struct KNO_KEYVAL *ykeyvals=smy->sm_keyvals;
      int i=0; while (i < xsize) {
        int cmp=LISP_COMPARE(xkeyvals[i].kv_key,ykeyvals[i].kv_key,flags);
        if (cmp) {result=cmp; break;}
        else i++;}
      if (result==0) {
        i=0; while (i < xsize) {
          int cmp=KNO_QCOMPARE(xkeyvals[i].kv_val,ykeyvals[i].kv_val);
          if (cmp) {result=cmp; break;}
          else i++;}}}
    else {
      struct KNO_KEYVAL _xkvbuf[17], *xkvbuf=_xkvbuf;
      struct KNO_KEYVAL _ykvbuf[17], *ykvbuf=_ykvbuf;
      int i=0, limit=(ysize>xsize) ? (xsize) : (ysize);
      if (xsize>17) xkvbuf=u8_alloc_n(xsize,struct KNO_KEYVAL);
      if (ysize>17) ykvbuf=u8_alloc_n(ysize,struct KNO_KEYVAL);
      memcpy(xkvbuf,KNO_XSLOTMAP_KEYVALS(smx),
             KNO_KEYVAL_LEN*xsize);
      memcpy(ykvbuf,KNO_XSLOTMAP_KEYVALS(smy),
             KNO_KEYVAL_LEN*ysize);
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
  if (unlockx) kno_unlock_table(smx);
  if (unlocky) kno_unlock_table(smy);
  return result;
}


/* Lists into slotmaps */

static int build_table(lispval table,lispval key,lispval value)
{
  if (EMPTYP(value)) {
    if (kno_test(table,key,VOID))
      return kno_add(table,key,value);
    else return kno_store(table,key,value);}
  else return kno_add(table,key,value);
}

KNO_EXPORT lispval kno_plist_to_slotmap(lispval plist)
{
  if (!(PAIRP(plist)))
    return kno_type_error(_("plist"),"kno_plist_to_slotmap",plist);
  else {
    lispval scan=plist, result=kno_init_slotmap(NULL,0,NULL);
    while ((PAIRP(scan))&&(PAIRP(KNO_CDR(scan)))) {
      lispval key=KNO_CAR(scan), value=KNO_CADR(scan);
      build_table(result,key,value);
      scan=KNO_CDR(scan); scan=KNO_CDR(scan);}
    if (!(NILP(scan))) {
      kno_decref(result);
      return kno_type_error(_("plist"),"kno_plist_to_slotmap",plist);}
    else return result;}
}

KNO_EXPORT int kno_sort_slotmap(lispval slotmap,int sorted)
{
  if (!(TYPEP(slotmap,kno_slotmap_type)))
    return kno_reterr(kno_TypeError,"kno_sort_slotmap",
                      u8_strdup("slotmap"),kno_incref(slotmap));
  else {
    struct KNO_SLOTMAP *sm=(kno_slotmap)slotmap;
    if (!(sorted)) {
      if (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_SORT_KEYVALS)) {
	KNO_XTABLE_SET_BIT(sm,KNO_SLOTMAP_SORT_KEYVALS,0);
        return 1;}
      else return 0;}
    else if (KNO_XTABLE_BITP(sm,KNO_SLOTMAP_SORT_KEYVALS))
      return 0;
    else if (sm->n_slots==0) {
      KNO_XTABLE_SET_BIT(sm,KNO_SLOTMAP_SORT_KEYVALS,1);
      return 1;}
    else {
      struct KNO_KEYVAL *keyvals=sm->sm_keyvals;
      sort_keyvals(keyvals,sm->n_slots);
      KNO_XTABLE_SET_BIT(sm,KNO_SLOTMAP_SORT_KEYVALS,1);
      return 1;}}
}

KNO_EXPORT lispval kno_alist_to_slotmap(lispval alist)
{
  if (!(PAIRP(alist)))
    return kno_type_error(_("alist"),"kno_alist_to_slotmap",alist);
  else {
    lispval result=kno_init_slotmap(NULL,0,NULL);
    KNO_DOLIST(assoc,alist) {
      if (!(PAIRP(assoc))) {
        kno_decref(result);
        return kno_type_error(_("alist"),"kno_alist_to_slotmap",alist);}
      else build_table(result,KNO_CAR(assoc),KNO_CDR(assoc));}
    return result;}
}

KNO_EXPORT lispval kno_blist_to_slotmap(lispval blist)
{
  if (!(PAIRP(blist)))
    return kno_type_error(_("binding list"),"kno_blist_to_slotmap",blist);
  else {
    lispval result=kno_init_slotmap(NULL,0,NULL);
    KNO_DOLIST(binding,blist) {
      if (!(PAIRP(binding))) {
        kno_decref(result);
        return kno_type_error(_("binding list"),"kno_blist_to_slotmap",blist);}
      else {
        lispval key=KNO_CAR(binding), scan=KNO_CDR(binding);
        if (NILP(scan))
          build_table(result,key,EMPTY);
        else while (PAIRP(scan)) {
            build_table(result,key,KNO_CAR(scan));
            scan=KNO_CDR(scan);}
        if (!(NILP(scan))) {
          kno_decref(result);
          return kno_type_error
            (_("binding list"),"kno_blist_to_slotmap",blist);}}}
    return result;}
}

/* Schema maps */

KNO_EXPORT lispval kno_make_schemap
(struct KNO_SCHEMAP *ptr,int size,int flags,
 lispval *schema,lispval *values)
{
  int i=0, stackvec = 0;
  lispval *vec = values;
  if (ptr == NULL) {
    if (flags&KNO_SCHEMAP_STATIC_VALUES) {
      void *consed = u8_malloc(sizeof(struct KNO_SCHEMAP)+(sizeof(lispval)*size));
      ptr = (struct KNO_SCHEMAP *) consed;
      vec = (lispval *) (consed + sizeof(struct KNO_SCHEMAP));
      stackvec = 1;}
    else ptr=u8_alloc(struct KNO_SCHEMAP);}
  if (vec == NULL) vec = u8_alloc_n(size,lispval);
  KNO_INIT_STRUCT(ptr,struct KNO_SCHEMAP);
  KNO_INIT_CONS(ptr,kno_schemap_type);
  ptr->schemap_template=KNO_VOID;
  ptr->schema_length=size;
  ptr->table_bits = flags;
  if (flags&KNO_SCHEMAP_PRIVATE) {
    KNO_XTABLE_SET_BIT(ptr,KNO_SCHEMAP_PRIVATE,1);
    ptr->table_schema=schema;}
  else if ( (flags) & (KNO_SCHEMAP_COPY_SCHEMA) ) {
    lispval *copied = u8_alloc_n(size,lispval);
    memcpy(copied,schema,sizeof(lispval)*size);
    kno_incref_elts(copied,size), ptr->table_schema=copied;
    KNO_XTABLE_SET_BIT(ptr,KNO_SCHEMAP_PRIVATE,1);}
  else {
    ptr->table_schema=schema;
    KNO_XTABLE_SET_BIT(ptr,KNO_SCHEMAP_PRIVATE,0);}
  ptr->table_values=vec;
  KNO_XTABLE_SET_BIT(ptr,KNO_SCHEMAP_STATIC_VALUES,stackvec);
  if (vec == values) {}
  else if (values)
    memcpy(vec,values,sizeof(lispval)*size);
  else while (i<size) vec[i++]=VOID;
  u8_init_rwlock(&(ptr->table_rwlock));
  KNO_XTABLE_SET_USELOCK(ptr,1);
  return LISP_CONS(ptr);
}

KNO_FASTOP void lispv_swap(lispval *a,lispval *b)
{
  lispval t;
  t = *a;
  *a = *b;
  *b = t;
}

KNO_EXPORT void kno_sort_schema(int n,lispval *v)
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
      kno_sort_schema(ln, v); v += j; n = rn;}
    else {kno_sort_schema(rn,v + j); n = ln;}}
}

KNO_EXPORT lispval *kno_register_schema(int n,lispval *schema)
{
  kno_sort_schema(n,schema);
  return schema;
}

KNO_EXPORT void kno_reset_schemap(struct KNO_SCHEMAP *ptr)
{
  lispval *scan = ptr->table_values;
  lispval *limit = scan+ptr->schema_length;
  while (scan < limit) {
    lispval value = *scan;
    *scan++ = KNO_VOID;
    kno_decref(value);}
}

static lispval copy_schemap(lispval schemap,int flags)
{
  int unlock = 0;
  struct KNO_SCHEMAP *ptr=
    kno_consptr(struct KNO_SCHEMAP *,schemap,kno_schemap_type);
  int i=0, size=KNO_XSCHEMAP_SIZE(ptr);
  struct KNO_SCHEMAP *nptr =
    u8_malloc(sizeof(struct KNO_SCHEMAP)+(size*sizeof(lispval)));
  lispval *ovalues=ptr->table_values;
  lispval *values= (lispval *)(((void *)nptr)+sizeof(struct KNO_SCHEMAP));
  lispval *schema=ptr->table_schema, *nschema=NULL;
  int bits = KNO_TABLE_USELOCKS | KNO_SCHEMAP_STATIC_VALUES;
  if (KNO_XTABLE_USELOCKP(ptr)) {
    kno_read_lock_table(ptr);
    unlock=1;}
  KNO_INIT_STRUCT(nptr,struct KNO_SCHEMAP);
  KNO_INIT_CONS(nptr,kno_schemap_type);
  nptr->schemap_template=KNO_VOID;
  if (KNO_XTABLE_BITP(ptr,KNO_SCHEMAP_STATIC_SCHEMA)) {
    nptr->table_schema=nschema=u8_alloc_n(size,lispval);
    bits |= KNO_SCHEMAP_PRIVATE;}
  else nptr->table_schema=schema;
  if ( (nptr->table_schema != schema) )
    while (i < size) {
      lispval val=ovalues[i];
      if (CONSP(val))
        if ((flags&KNO_FULL_COPY)||(KNO_STATICP(val)))
          values[i]=kno_copier(val,flags);
        else if (PRECHOICEP(val))
          values[i]=kno_make_simple_choice(val);
        else values[i]=kno_incref(val);
      else values[i]=val;
      nschema[i]=schema[i];
      i++;}
  else if (flags) while (i<size) {
      lispval val=ovalues[i];
      if (CONSP(val))
        if (PRECHOICEP(val))
          values[i]=kno_make_simple_choice(val);
        else if ((flags&KNO_FULL_COPY)||(KNO_STATICP(val)))
          values[i]=kno_copier(val,flags);
        else values[i]=kno_incref(val);
      else values[i]=val;
      i++;}
  else while (i < size) {
      values[i]=kno_incref(ovalues[i]);
      i++;}
  nptr->table_values=values;
  nptr->schema_length=size;
  nptr->table_bits = bits;
  if (unlock) kno_unlock_table(ptr);
  u8_init_rwlock(&(nptr->table_rwlock));
  return LISP_CONS(nptr);
}

KNO_EXPORT lispval kno_init_schemap
(struct KNO_SCHEMAP *ptr, int size, struct KNO_KEYVAL *init)
{
  int i=0, stackvec = 0;
  lispval *new_schema, *new_vals;
  if (ptr == NULL) {
    ptr=u8_malloc(sizeof(struct KNO_SCHEMAP)+(size*sizeof(lispval)));
    KNO_INIT_FRESH_CONS(ptr,kno_schemap_type);
    new_vals=(lispval *)(((void *)ptr)+sizeof(struct KNO_SCHEMAP));
    stackvec=1;}
  else {
    KNO_SET_CONS_TYPE(ptr,kno_schemap_type);
    new_vals = u8_alloc_n(size,lispval);}
  new_schema=u8_alloc_n(size,lispval);
  ptr->schemap_template=KNO_VOID;
  ptr->table_values=new_vals;
  ptr->schema_length=size;
  ptr->table_bits = KNO_TABLE_USELOCKS;
  sort_keyvals(init,size);
  while (i<size) {
    new_schema[i]=init[i].kv_key;
    new_vals[i]=init[i].kv_val;
    i++;}
  /* ptr->table_schema=kno_register_schema(size,new_schema); */
  ptr->table_schema = new_schema;
  if (ptr->table_schema != new_schema) {
    u8_free(new_schema);}
  else {KNO_XTABLE_SET_BIT(ptr,KNO_SCHEMAP_PRIVATE,1);}
  if (stackvec) ptr->table_bits |= KNO_SCHEMAP_STATIC_VALUES;
  u8_init_rwlock(&(ptr->table_rwlock));
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval _kno_schemap_get
(struct KNO_SCHEMAP *sm,lispval key,lispval dflt)
{
  return __kno_schemap_get(sm,key,dflt);
}

KNO_EXPORT lispval _kno_schemap_test
(struct KNO_SCHEMAP *sm,lispval key,lispval val)
{
  return __kno_schemap_test(sm,key,val);
}


KNO_EXPORT int kno_schemap_store
(struct KNO_SCHEMAP *sm,lispval key,lispval value)
{
  int slotno, size, unlock=0;
  KNO_CHECK_TYPE_RET(sm,kno_schemap_type);
  if ((KNO_TROUBLEP(key)))
    return kno_interr(key);
  else if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (KNO_XTABLE_READONLYP(sm))
    return KNO_ERR(-1,kno_ReadOnlyTable,"kno_schemap_store",NULL,key);
  else if (KNO_XTABLE_USELOCKP(sm)) {
    kno_write_lock_table(sm);
    unlock=1;}
  else NO_ELSE;
  size=KNO_XSCHEMAP_SIZE(sm);
  slotno=__kno_get_slotno(key,sm->table_schema,size,
			  (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_SORTED)));
  if (slotno>=0) {
    if (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_STACK_VALUES)) {
      kno_incref_elts(sm->table_values,size);
      KNO_XTABLE_SET_BIT(sm,KNO_SCHEMAP_STACK_VALUES,0);}
    kno_decref(sm->table_values[slotno]);
    sm->table_values[slotno]=kno_incref(value);
    KNO_XTABLE_SET_MODIFIED(sm,1);
    if (unlock) kno_unlock_table(sm);
    return 1;}
  else {
    if (unlock) kno_unlock_table(sm);
    return KNO_ERR(-1,kno_NoSuchKey,"kno_schemap_store",NULL,key);}
}

KNO_EXPORT int kno_schemap_add
(struct KNO_SCHEMAP *sm,lispval key,lispval value)
{
  int slotno, size, unlock = 0;
  KNO_CHECK_TYPE_RET(sm,kno_schemap_type);
  if ((KNO_TROUBLEP(key)))
    return kno_interr(key);
  else if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (EMPTYP(value))
    return 0;
  else if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (KNO_XTABLE_READONLYP(sm))
    return KNO_ERR(-1,kno_ReadOnlyTable,"kno_schemap_add",NULL,key);
  else if (KNO_XTABLE_USELOCKP(sm)) {
    kno_write_lock_table(sm);
    unlock=1;}
  else NO_ELSE;
  size=KNO_XSCHEMAP_SIZE(sm);
  slotno=__kno_get_slotno(key,sm->table_schema,size,
			  (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_SORTED)));
  if (slotno>=0) {
    kno_incref(value);
    if (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_STACK_VALUES)) {
      kno_incref_elts(sm->table_values,size);
      KNO_XTABLE_SET_BIT(sm,KNO_SCHEMAP_STACK_VALUES,0);}
    CHOICE_ADD(sm->table_values[slotno],value);
    KNO_XTABLE_SET_MODIFIED(sm,1);
    if (unlock) kno_unlock_table(sm);
    return 1;}
  else {
    if (unlock) kno_unlock_table(sm);
    return KNO_ERR(-1,kno_NoSuchKey,"kno_schemap_add",NULL,key);}
}

KNO_EXPORT int kno_schemap_drop
(struct KNO_SCHEMAP *sm,lispval key,lispval value)
{
  int slotno, size, unlock = 0;
  KNO_CHECK_TYPE_RET(sm,kno_schemap_type);
  if ((KNO_TROUBLEP(key)))
    return kno_interr(key);
  else if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (EMPTYP(value))
    return 0;
  else if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (KNO_XTABLE_READONLYP(sm))
    return KNO_ERR(-1,kno_ReadOnlyTable,"kno_schemap_drop",NULL,key);
  else if (KNO_XTABLE_USELOCKP(sm)) {
    kno_write_lock_table(sm);
    unlock=1;}
  else NO_ELSE;
  size=KNO_XSCHEMAP_SIZE(sm);
  slotno=__kno_get_slotno(key,sm->table_schema,size,
			  (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_SORTED)));
  if (slotno>=0) {
    if (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_STACK_VALUES)) {
      kno_incref_elts(sm->table_values,size);
      KNO_XTABLE_SET_BIT(sm,KNO_SCHEMAP_STACK_VALUES,0);}
    lispval oldval=sm->table_values[slotno];
    lispval newval=((VOIDP(value)) ? (EMPTY) :
                    (kno_difference(oldval,value)));
    if (newval == oldval)
      kno_decref(newval);
    else {
      KNO_XTABLE_SET_MODIFIED(sm,1);
      kno_decref(oldval);
      sm->table_values[slotno]=newval;}
    if (unlock) kno_unlock_table(sm);
    return 1;}
  else {
    if (unlock) kno_unlock_table(sm);
    return 0;}
}

static int schemap_getsize(struct KNO_SCHEMAP *ptr)
{
  KNO_CHECK_TYPE_RET(ptr,kno_schemap_type);
  return KNO_XSCHEMAP_SIZE(ptr);
}

static int schemap_modified(struct KNO_SCHEMAP *ptr,int flag)
{
  KNO_CHECK_TYPE_RET(ptr,kno_schemap_type);
  int modified=KNO_XTABLE_MODIFIEDP(ptr);
  if (flag<0)
    return modified;
  else if (flag) {
    KNO_XTABLE_SET_MODIFIED(ptr,1);
    return modified;}
  else {
    KNO_XTABLE_SET_MODIFIED(ptr,0);
    return modified;}
}

static int schemap_readonly(struct KNO_SCHEMAP *ptr,int flag)
{
  KNO_CHECK_TYPE_RET(ptr,kno_schemap_type);
  int readonly=KNO_XTABLE_READONLYP(ptr);
  if (flag<0)
    return readonly;
  else if (flag) {
    KNO_XTABLE_SET_BIT(ptr,KNO_TABLE_READONLY,1);
    KNO_XTABLE_SET_BIT(ptr,KNO_TABLE_USELOCKS,0);
    return readonly;}
  else {
    KNO_XTABLE_SET_BIT(ptr,KNO_TABLE_READONLY,0);
    KNO_XTABLE_SET_BIT(ptr,KNO_TABLE_USELOCKS,1);
    return readonly;}
}

KNO_EXPORT lispval kno_schemap_keys(struct KNO_SCHEMAP *sm)
{
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_schemap_type);
  {
    int size=KNO_XSCHEMAP_SIZE(sm);
    if (size==0)
      return EMPTY;
    else if (size==1)
      return kno_incref(sm->table_schema[0]);
    else {
      struct KNO_CHOICE *ch=kno_alloc_choice(size);
      memcpy((lispval *)KNO_XCHOICE_DATA(ch),sm->table_schema,
             LISPVEC_BYTELEN(size));
      if (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_SORTED))
        return kno_init_choice(ch,size,sm->table_schema,0);
      else return kno_init_choice(ch,size,sm->table_schema,
                                  KNO_CHOICE_INCREF|KNO_CHOICE_DOSORT);}}
}

KNO_EXPORT lispval kno_schemap_assocs(struct KNO_SCHEMAP *sm)
{
  KNO_CHECK_TYPE_RETDTYPE(sm,kno_schemap_type);
  {
    int size=KNO_XSCHEMAP_SIZE(sm), unlock = 0;
    if (size==0)
      return EMPTY;
    if (KNO_XTABLE_USELOCKP(sm)) {
      u8_read_lock(&(sm->table_rwlock));
      unlock = 1;}
    /* It *might* have changed */
    size=KNO_XSCHEMAP_SIZE(sm);
    if (size==0)
      return EMPTY;
    if (size==1) {
      lispval result = kno_init_pair(NULL,
                                     kno_incref(sm->table_schema[0]),
                                     kno_incref(sm->table_values[0]));
      if (unlock) u8_rw_unlock(&(sm->table_rwlock));
      return result;}
    else {
      struct KNO_CHOICE *ch=kno_alloc_choice(size);
      lispval *write = (lispval *) KNO_XCHOICE_ELTS(ch);
      int i = 0; while (i<size) {
        lispval assoc =  kno_init_pair
          (NULL,kno_incref(sm->table_schema[i]),
           kno_incref(sm->table_values[i]));
        *write++ = assoc;
        i++;}
      if (unlock) u8_rw_unlock(&(sm->table_rwlock));
      return kno_init_choice(ch,size,sm->table_schema,
                             KNO_CHOICE_INCREF|KNO_CHOICE_DOSORT);}
  }
}

KNO_EXPORT lispval *kno_schemap_keyvec_n(struct KNO_SCHEMAP *sm,int *len)
{
  /* int unlock = 0; ?? */
  if (!((KNO_CONS_TYPEOF(sm))==kno_schemap_type)) {
    kno_seterr(kno_TypeError,"kno_schemap_keyvec_n","schemap",(lispval)sm);
    *len = -1; return NULL;}
  int size=KNO_XSCHEMAP_SIZE(sm);
  if (size==0) {*len = 0; return NULL;}
  lispval *keys = u8_alloc_n(size,lispval);
  memcpy(keys,sm->table_schema,LISPVEC_BYTELEN(size));
  kno_incref_elts(keys,size);
  *len = size;
  return keys;
}

static void recycle_schemap(struct KNO_RAW_CONS *c)
{
  int unlock = 0;
  struct KNO_SCHEMAP *sm=(struct KNO_SCHEMAP *)c;
  if (KNO_XTABLE_USELOCKP(sm)) {
    kno_write_lock_table(sm);
    unlock = 1;}
  else {}
  lispval template = sm->schemap_template;
  if ( (template) && (KNO_CONSP(template)) ) {
    kno_decref(template);
    sm->schemap_template=KNO_VOID;}
  int schemap_size=KNO_XSCHEMAP_SIZE(sm);
  int stack_vals = (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_STACK_VALUES));
  if ( (sm->table_values) && (stack_vals == 0) ) {
    lispval *scan=sm->table_values;
    lispval *limit=sm->table_values+schemap_size;
    while (scan < limit) {kno_decref(*scan); scan++;}}
  if (! (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_STATIC_VALUES)) )
    u8_free(sm->table_values);
  if ((sm->table_schema) && (KNO_XTABLE_BITP(sm,KNO_SCHEMAP_PRIVATE)))
    u8_free(sm->table_schema);
  if (unlock) kno_unlock_table(sm);
  u8_destroy_rwlock(&(sm->table_rwlock));
  memset(sm,0,KNO_SCHEMAP_LEN);
  u8_free(sm);
}
static int unparse_schemap(u8_output out,lispval x)
{
  struct KNO_SCHEMAP *sm=KNO_XSCHEMAP(x);
  kno_read_lock_table(sm);
  {
    int i=0, schemap_size=KNO_XSCHEMAP_SIZE(sm);
    lispval *schema=sm->table_schema, *values=sm->table_values;
    u8_puts(out,"[");
    if (i<schemap_size) {
      kno_unparse(out,schema[i]); u8_putc(out,' ');
      kno_unparse(out,values[i]); i++;}
    while (i<schemap_size) {
      u8_putc(out,' ');
      kno_unparse(out,schema[i]); u8_putc(out,' ');
      kno_unparse(out,values[i]); i++;}
    u8_puts(out,"]");
  }
  kno_unlock_table(sm);
  return 1;
}
static int compare_schemaps(lispval x,lispval y,kno_compare_flags flags)
{
  int result=0;
  struct KNO_SCHEMAP *smx=(struct KNO_SCHEMAP *)x;
  struct KNO_SCHEMAP *smy=(struct KNO_SCHEMAP *)y;
  kno_read_lock_table(smx); kno_read_lock_table(smy);
  {
    int xsize=KNO_XSCHEMAP_SIZE(smx), ysize=KNO_XSCHEMAP_SIZE(smy);
    if (xsize>ysize) result=1;
    else if (xsize<ysize) result=-1;
    else {
      lispval *xschema=smx->table_schema, *yschema=smy->table_schema;
      lispval *xvalues=smx->table_values, *yvalues=smy->table_values;
      int i=0; while (i < xsize) {
        int cmp=KNO_QCOMPARE(xschema[i],yschema[i]);
        if (cmp) {result=cmp; break;}
        else i++;}
      if (result==0) {
        i=0; while (i < xsize) {
          int cmp=KNO_QCOMPARE(xvalues[i],yvalues[i]);
          if (cmp) {result=cmp; break;}
          else i++;}}}}
  kno_unlock_table(smx); kno_unlock_table(smy);
  return result;
}

KNO_EXPORT lispval kno_schemap2slotmap(lispval schemap)
{
  struct KNO_SCHEMAP *sm = KNO_GET_CONS(schemap,kno_schemap_type,kno_schemap);
  if (sm==NULL) return KNO_ERROR;
  int i = 0, n = sm->schema_length;
  lispval result = kno_make_slotmap(n,n,NULL);
  if (KNO_ABORTED(result)) return result;
  lispval *schema = sm->table_schema, *values = sm->table_values;
  struct KNO_KEYVAL *keyvals = KNO_SLOTMAP_KEYVALS(result);
  while (i<n) {
    keyvals[i].kv_key=kno_incref(schema[i]);
    keyvals[i].kv_val=kno_incref(values[i]);
    i++;}
  return result;
}

/* Hash functions */

/* This is the point at which we resize hashtables. */
#define hashtable_needs_resizep(ht)                                     \
  ((ht->table_n_keys*ht->table_load_factor)>(ht->ht_n_buckets))
/* This is the target size for resizing. */
#define hashtable_resize_target(ht)             \
  ((ht->table_n_keys*ht->table_load_factor))

#define hashset_needs_resizep(hs)                               \
  ((hs->hs_n_elts*hs->hs_load_factor)>(hs->hs_n_buckets))
/* This is the target size for resizing. */
#define hashset_resize_target(hs)               \
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
KNO_EXPORT unsigned int kno_get_hashtable_size(unsigned int min)
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

#define CONS_MULTIPLIER1 310000023
#define CONS_MULTIPLIER2 420000017

KNO_FASTOP unsigned int mult_hash_bytes
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

KNO_EXPORT unsigned int kno_hash_bytes(u8_string string,int len)
{
  return mult_hash_bytes(string,len);
}

/* Hashing lisp pointers */

static unsigned int hash_elts(lispval *x,unsigned int n);
static unsigned int hash_cons(struct KNO_CONS *cons);

static unsigned int hash_lisp(lispval x)
{
  kno_lisp_type lisp_type = KNO_PTR_MANIFEST_TYPE(x);
  switch (lisp_type) {
  case kno_oid_type:
    return hash_mult(x,OID_MULTIPLIER);
  case kno_fixnum_type:
    return hash_mult(x,FIXNUM_MULTIPLIER);
  case kno_cons_type:
    return hash_cons((kno_cons)x);
  default: {
    kno_lisp_type immediate_type = KNO_IMMEDIATE_TYPE(x);
    switch (immediate_type) {
    case kno_constant_type:
      return hash_mult(x,CONSTANT_MULTIPLIER);
    case kno_symbol_type:
      return hash_mult(x,SYMBOL_MULTIPLIER);
    default:
      if ((immediate_type>kno_symbol_type) &&
	  (immediate_type<N_TYPE_MULTIPLIERS))
	return hash_mult(x,type_multipliers[lisp_type]);
      else return hash_mult(x,MYSTERIOUS_MULTIPLIER);
    }}}
}

static unsigned int hash_cons_ptr(lispval x,unsigned int mult)
{
  kno_ptrval bits = (kno_ptrval) x;
  bits = bits>>2;
#if SIZEOF_VOID_P > 4
  unsigned int l = (unsigned int)(bits&0xFFFFFFFF);
  unsigned int f = hash_mult(l,mult);
  return hash_mult(f,CONS_MULTIPLIER2);
#else
  return hash_mult(x,mult);
#endif
}

static unsigned int hash_ptr(lispval x)
{
  kno_lisp_type lisp_type = KNO_PTR_MANIFEST_TYPE(x);
  switch (lisp_type) {
  case kno_oid_type:
    return hash_mult(x,OID_MULTIPLIER);
  case kno_fixnum_type:
    return hash_mult(x,FIXNUM_MULTIPLIER);
  case kno_cons_type:
    return hash_cons_ptr(x,CONS_MULTIPLIER1);
  default: {
    kno_lisp_type immediate_type = KNO_IMMEDIATE_TYPE(x);
    switch (immediate_type) {
    case kno_constant_type:
      return hash_mult(x,CONSTANT_MULTIPLIER);
    case kno_symbol_type:
      return hash_mult(x,SYMBOL_MULTIPLIER);
    default:
      if ((immediate_type>kno_symbol_type) &&
	  (immediate_type<N_TYPE_MULTIPLIERS))
	return hash_mult(x,type_multipliers[lisp_type]);
      else return hash_mult(x,MYSTERIOUS_MULTIPLIER);
    }}}
}


static unsigned int hash_cons(struct KNO_CONS *cons)
{
  if (RARELY(cons == NULL))
    kno_raise(kno_NullPtr,"hash_lisp",NULL,KNO_VOID);
  kno_lisp_type lisp_type = KNO_CONS_TYPEOF(cons);
  switch (lisp_type) {
  case kno_string_type: {
    struct KNO_STRING *s = (kno_string) cons;
    return mult_hash_bytes(s->str_bytes,s->str_bytelen);}
  case kno_packet_type: case kno_secret_type: {
    struct KNO_STRING *s = (kno_string) cons;
    return mult_hash_bytes(s->str_bytes,s->str_bytelen);}
  case kno_pair_type: {
    struct KNO_PAIR *pair = (kno_pair) cons;
    lispval car=pair->car, cdr=pair->cdr;
    unsigned int hcar=hash_lisp(car), hcdr=hash_lisp(cdr);
    return hash_mult(hcar,hcdr);}
  case kno_vector_type: {
    struct KNO_VECTOR *v = (kno_vector) cons;
    return hash_elts(v->vec_elts,v->vec_length);}
  case kno_compound_type: {
    struct KNO_COMPOUND *c = (kno_compound) cons;
    if (c->compound_isopaque) {
      int ctype = KNO_CONS_TYPEOF(c);
      if ( (ctype>0) && (ctype<N_TYPE_MULTIPLIERS) )
	return hash_cons_ptr((lispval)cons,type_multipliers[ctype]);
      else return hash_cons_ptr((lispval)cons,MYSTERIOUS_MULTIPLIER);}
    else return hash_mult
	   (hash_lisp(c->typetag),
	    hash_elts(&(c->compound_0),c->compound_length));}
  case kno_slotmap_type: {
    struct KNO_SLOTMAP *sm = (kno_slotmap) cons;
    lispval *kv=(lispval *)sm->sm_keyvals;
    return hash_elts(kv,sm->n_slots*2);}
  case kno_choice_type: {
    struct KNO_CHOICE *ch = (kno_choice) cons;
    int size=KNO_XCHOICE_SIZE(ch);
    return hash_elts((lispval *)(KNO_XCHOICE_DATA(ch)),size);}
  case kno_prechoice_type: {
    lispval simple=kno_make_simple_choice((lispval)cons);
    int hash=hash_lisp(simple);
    kno_decref(simple);
    return hash;}
  case kno_qchoice_type: {
    struct KNO_QCHOICE *ch = (kno_qchoice) cons;
    return hash_lisp(ch->qchoiceval);}
  default: {
    if ((lisp_type<KNO_TYPE_MAX) && (kno_hashfns[lisp_type]))
      return kno_hashfns[lisp_type]((lispval)cons,kno_hash_lisp);
    else return hash_mult((lispval)cons,MYSTERIOUS_MULTIPLIER);}}
}

KNO_EXPORT unsigned int kno_hash_lisp(lispval x)
{
  return hash_lisp(x);
}

/* hash_elts: (static)
   arguments: a pointer to a vector of lisp pointers and a size
   Returns: combines the elements' hashes into a single hash
*/
static unsigned int hash_elts(lispval *x,unsigned int n)
{
  lispval *limit=x+n; int sum=0;
  while (x < limit) {
    unsigned int h=hash_lisp(*x);
    sum=hash_combine(sum,h);
    sum=sum%(MYSTERIOUS_MODULUS);
    x++;}
  return sum;
}

/* Hashvecs */

static struct KNO_KEYVAL *hashvec_get
(lispval key,struct KNO_HASH_BUCKET **slots,int n_slots,
 struct KNO_HASH_BUCKET ***bucket_loc)
{
  unsigned int hash=hash_lisp(key), offset=compute_offset(hash,n_slots);
  struct KNO_HASH_BUCKET *he=slots[offset];
  if (bucket_loc) *bucket_loc = &(slots[offset]);
  if (he == NULL)
    return NULL;
  else if (USUALLY(he->bucket_len == 1))
    if (LISP_EQUAL(key,he->kv_val0.kv_key))
      return &(he->kv_val0);
    else return NULL;
  else return kno_sortvec_get(key,&(he->kv_val0),he->bucket_len);
}

static struct KNO_KEYVAL *hash_bucket_insert
(lispval key,struct KNO_HASH_BUCKET **hep);

static struct KNO_KEYVAL *hashvec_insert
(lispval key,struct KNO_HASH_BUCKET **slots,int n_slots,int *n_keys)
{
  unsigned int hash=hash_lisp(key), offset=compute_offset(hash,n_slots);
  struct KNO_HASH_BUCKET *he=slots[offset];
  if (he == NULL) {
    he=u8_alloc(struct KNO_HASH_BUCKET);
    KNO_INIT_STRUCT(he,struct KNO_HASH_BUCKET);
    he->bucket_len=1;
    he->kv_val0.kv_key=kno_getref(key);
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
    struct KNO_KEYVAL *kv=hash_bucket_insert(key,&slots[offset]);
    if ((n_keys) && (slots[offset]->bucket_len > size))
      (*n_keys)++;
    return kv;}
}

static struct KNO_KEYVAL *hash_bucket_insert
(lispval key,struct KNO_HASH_BUCKET **hep)
{
  struct KNO_HASH_BUCKET *he=*hep;
  struct KNO_KEYVAL *keyvals=&(he->kv_val0);
  int size = he->bucket_len, found = 0;
  struct KNO_KEYVAL *bottom=keyvals, *top=bottom+(size-1);
  struct KNO_KEYVAL *middle=bottom+(top-bottom)/2;;
  while (top>=bottom) { /* (!(top<bottom)) */
    middle=bottom+(top-bottom)/2;
    if (LISP_EQUAL(key,middle->kv_key)) {found=1; break;}
    else if (__kno_cons_compare(key,middle->kv_key)<0) top=middle-1;
    else bottom=middle+1;}
  if (found)
    return middle;
  else {
    int mpos=(middle-keyvals), dir=(bottom>middle), ipos=mpos+dir;
    struct KNO_KEYVAL *keyvals, *insert_point;
    struct KNO_HASH_BUCKET *new_hashentry=
      /* We don't need to use size+1 here because KNO_HASH_BUCKET includes
         one value. */
      u8_realloc(he,(sizeof(struct KNO_HASH_BUCKET)+
                     (size)*KNO_KEYVAL_LEN));
    keyvals = &(new_hashentry->kv_val0);
    *hep=new_hashentry;
    new_hashentry->bucket_len++;
    insert_point=keyvals+ipos;
    memmove(insert_point+1,insert_point,(sizeof(struct KNO_KEYVAL))*(size-ipos));
    if (CONSP(key)) {
      if (KNO_STATICP(key))
        insert_point->kv_key=kno_copy(key);
      else insert_point->kv_key=kno_incref(key);}
    else insert_point->kv_key=key;
    insert_point->kv_val=EMPTY;
    return insert_point;}
}

KNO_FASTOP struct KNO_HASH_BUCKET *hash_bucket_remove
(struct KNO_HASH_BUCKET *bucket,struct KNO_KEYVAL *slot)
{
  int len = bucket->bucket_len;
  struct KNO_KEYVAL *base = &(bucket->kv_val0);
  if ( (len == 1) && (slot == base) ) {
    u8_free(bucket);
    return NULL;}
  else {
    int offset = slot - base;
    size_t new_size = sizeof(struct KNO_HASH_BUCKET)+(KNO_KEYVAL_LEN*(len-2));
    assert( ( offset >= 0 ) && (offset < len ) );
    memmove(slot,slot+1,KNO_KEYVAL_LEN*(len-(offset+1)));
    struct KNO_HASH_BUCKET *new_bucket=u8_realloc(bucket,new_size);
    new_bucket->bucket_len--;
    return new_bucket;}
}

KNO_EXPORT struct KNO_KEYVAL *kno_hashvec_get
(lispval key,struct KNO_HASH_BUCKET **slots,int n_slots)
{
  return hashvec_get(key,slots,n_slots,NULL);
}

KNO_EXPORT struct KNO_KEYVAL *kno_hash_bucket_insert
(lispval key,struct KNO_HASH_BUCKET **hep)
{
  return hash_bucket_insert(key,hep);
}

KNO_EXPORT struct KNO_KEYVAL *kno_hashvec_insert
(lispval key,struct KNO_HASH_BUCKET **slots,int n_slots,int *n_keys)
{
  return hashvec_insert(key,slots,n_slots,n_keys);
}


/* EQ Hashtable OPS */

static struct KNO_KEYVAL *eq_sortvec_get
(lispval key,struct KNO_KEYVAL *keyvals,int size)
{
  if (size<4)
    return __kno_keyvals_get(key,keyvals,size);
  else {
    int found = 0;
    const struct KNO_KEYVAL
      *bottom = keyvals, *limit = bottom+size, *top = limit-1, *middle;
    while (top>=bottom) {
      middle = bottom+(top-bottom)/2;
      if (middle>=limit) break;
      if (key<middle->kv_key) top = middle-1;
      else if (key<middle->kv_key)
	bottom = middle+1;
      else {found = 1; break;}}
    if (found)
      return (struct KNO_KEYVAL *) middle;
    else return NULL;}
}

static struct KNO_KEYVAL *eq_hashvec_get
(lispval key,struct KNO_HASH_BUCKET **slots,int n_slots,
 struct KNO_HASH_BUCKET ***bucket_loc)
{
  unsigned int hash=hash_ptr(key), offset=compute_offset(hash,n_slots);
  struct KNO_HASH_BUCKET *he=slots[offset];
  if (bucket_loc) *bucket_loc = &(slots[offset]);
  if (he == NULL)
    return NULL;
  else if (USUALLY(he->bucket_len == 1))
    if (LISP_EQUAL(key,he->kv_val0.kv_key))
      return &(he->kv_val0);
    else return NULL;
  else return eq_sortvec_get(key,&(he->kv_val0),he->bucket_len);
}

static struct KNO_KEYVAL *eq_hash_bucket_insert
(lispval key,struct KNO_HASH_BUCKET **hep)
{
  struct KNO_HASH_BUCKET *he=*hep;
  struct KNO_KEYVAL *keyvals=&(he->kv_val0);
  int size = he->bucket_len, found = 0;
  struct KNO_KEYVAL *bottom=keyvals, *top=bottom+(size-1);
  struct KNO_KEYVAL *middle=bottom+(top-bottom)/2;;
  while (top>=bottom) { /* (!(top<bottom)) */
    middle=bottom+(top-bottom)/2;
    if (key<middle->kv_key) top=middle-1;
    else if (key>middle->kv_key) bottom=middle+1;
    else {found=1; break;}}
  if (found)
    return middle;
  else {
    int mpos=(middle-keyvals), dir=(bottom>middle), ipos=mpos+dir;
    struct KNO_KEYVAL *keyvals, *insert_point;
    struct KNO_HASH_BUCKET *new_hashentry=
      /* We don't need to use size+1 here because KNO_HASH_BUCKET includes
         one value. */
      u8_realloc(he,(sizeof(struct KNO_HASH_BUCKET)+
                     (size)*KNO_KEYVAL_LEN));
    keyvals = &(new_hashentry->kv_val0);
    *hep=new_hashentry;
    new_hashentry->bucket_len++;
    insert_point=keyvals+ipos;
    memmove(insert_point+1,insert_point,(sizeof(struct KNO_KEYVAL))*(size-ipos));
    if (CONSP(key)) {
      if (KNO_STATICP(key))
        insert_point->kv_key=kno_copy(key);
      else insert_point->kv_key=kno_incref(key);}
    else insert_point->kv_key=key;
    insert_point->kv_val=EMPTY;
    return insert_point;}
}

static struct KNO_KEYVAL *eq_hashvec_insert
(lispval key,struct KNO_HASH_BUCKET **slots,int n_slots,int *n_keys)
{
  unsigned int hash=hash_ptr(key), offset=compute_offset(hash,n_slots);
  struct KNO_HASH_BUCKET *he=slots[offset];
  if (he == NULL) {
    he=u8_alloc(struct KNO_HASH_BUCKET);
    KNO_INIT_STRUCT(he,struct KNO_HASH_BUCKET);
    he->bucket_len=1;
    he->kv_val0.kv_key=kno_getref(key);
    he->kv_val0.kv_val=EMPTY;
    slots[offset]=he;
    if (n_keys) (*n_keys)++;
    return &(he->kv_val0);}
  else if (he->bucket_len == 1) {
    if (key == he->kv_val0.kv_key)
      return &(he->kv_val0);
    else {
      if (n_keys) (*n_keys)++;
      return eq_hash_bucket_insert(key,&slots[offset]);}}
  else {
    int size=he->bucket_len;
    struct KNO_KEYVAL *kv=eq_hash_bucket_insert(key,&slots[offset]);
    if ((n_keys) && (slots[offset]->bucket_len > size))
      (*n_keys)++;
    return kv;}
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

KNO_EXPORT lispval kno_hashtable_get
(struct KNO_HASHTABLE *ht,lispval key,lispval dflt)
{
  int unlock=0;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RETDTYPE(ht,kno_hashtable_type);
  if (EMPTYP(key))
    return EMPTY;
  else if (ht->table_n_keys == 0)
    return kno_incref(dflt);
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_read_lock_table(ht);
    unlock=1; }
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    /* Race condition, if someone dropped the sole key while we were
       busy locking the table. */
    if (unlock) kno_unlock_table(ht);
    return kno_incref(dflt);}
  else NO_ELSE;
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL)) :
    (hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL));
  if (result) {
    lispval values=kno_incref(result->kv_val);
    if (VOIDP(values)) {
      if (unlock) kno_unlock_table(ht);
      return kno_incref(dflt);}
    else if (PRECHOICEP(values)) {
      if (unlock) {
	kno_unlock_table(ht);
	kno_decref(values);
	values=KNO_VOID;
	kno_write_lock_table(ht);}
      if (eq_cmp)
	result = eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL);
      else result = hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL);
      if (result) {
        values = result->kv_val;
        if (KNO_PRECHOICEP(values)) {
          lispval norm = kno_make_simple_choice(values);
	  values = norm;}
	else kno_incref(values);}
      else values= KNO_EMPTY;
      if (unlock) kno_unlock_table(ht);
      return values;}
    else {
      if (unlock) kno_unlock_table(ht);
      return values;}}
  else {
    if (unlock) kno_unlock_table(ht);
    return kno_incref(dflt);}
}

KNO_EXPORT lispval kno_hashtable_get_nolock
(struct KNO_HASHTABLE *ht,lispval key,lispval dflt)
{
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RETDTYPE(ht,kno_hashtable_type);
  if (KNO_EMPTYP(key))
    return KNO_EMPTY;
  else if (ht->table_n_keys == 0)
    return kno_incref(dflt);
  else NO_ELSE;
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL)) :
    (hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL));
  if (result) {
    lispval rv=kno_incref(result->kv_val);
    if (KNO_VOIDP(rv))
      return kno_incref(dflt);
    else if (PRECHOICEP(rv)) {
      lispval norm = kno_make_simple_choice(rv);
      kno_decref(rv);
      return norm;}
    else return rv;}
  else {
    return kno_incref(dflt);}
}

KNO_EXPORT lispval kno_hashtable_get_noref
(struct KNO_HASHTABLE *ht,lispval key,lispval dflt)
{
  int unlock=0;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RETDTYPE(ht,kno_hashtable_type);
  if (KNO_EMPTYP(key))
    return KNO_EMPTY;
  else if (ht->table_n_keys == 0)
    return dflt;
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_read_lock_table(ht);
    unlock=1; }
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    /* Race condition */
    if (unlock) kno_unlock_table(ht);
    return dflt;}
  else NO_ELSE;
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL)) :
    (hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL));
  if (result) {
    lispval rv = kno_incref(result->kv_val);
    if (PRECHOICEP(rv)) {
      lispval norm = kno_make_simple_choice(rv);
      kno_decref(rv);
      if (unlock) kno_unlock_table(ht);
      return norm;}
    else {
      if (unlock) kno_unlock_table(ht);
      if (KNO_VOIDP(rv))
        return kno_incref(dflt);
      else return rv;}}
  else {
    if (unlock) kno_unlock_table(ht);
    return dflt;}
}

KNO_EXPORT lispval kno_hashtable_get_nolockref
(struct KNO_HASHTABLE *ht,lispval key,lispval dflt)
{
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RETDTYPE(ht,kno_hashtable_type);
  if (KNO_EMPTYP(key))
    return KNO_EMPTY;
  else if (ht->table_n_keys == 0)
    return dflt;
  else NO_ELSE;
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL)) :
    (hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL));
  if (result) {
    lispval rv=result->kv_val;
    if (VOIDP(rv)) return dflt;
    else if (PRECHOICEP(rv))
      return kno_make_simple_choice(rv);
    else return rv;}
  else return dflt;
}

KNO_EXPORT int kno_hashtable_probe(struct KNO_HASHTABLE *ht,lispval key)
{
  int unlock=0;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (KNO_EMPTYP(key))
    return KNO_EMPTY;
  else if (ht->table_n_keys == 0)
    return 0;
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_read_lock_table(ht);
    unlock=1; }
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    if (unlock) kno_unlock_table(ht);
    return 0;}
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL)) :
    (hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL));
  if (result) {
    if (unlock) kno_unlock_table(ht);
    return 1;}
  else {
    if (unlock) u8_rw_unlock((&(ht->table_rwlock)));
    return 0;}
}

static int hashtable_test(struct KNO_HASHTABLE *ht,lispval key,lispval val)
{
  int unlock=0;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (KNO_EMPTYP(key))
    return KNO_EMPTY;
  else if (ht->table_n_keys == 0)
    return 0;
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_read_lock_table(ht);
    unlock=1;}
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    /* Avoid race condition */
    if (unlock) kno_unlock_table(ht);
    return 0;}
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL)) :
    (hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL));
  if (result) {
    lispval current=result->kv_val; int cmp;
    if (VOIDP(val)) cmp=(!(VOIDP(current)));
    /* This used to return 0 when val is VOID and the value is
       EMPTY, but that's not consistent with the other table
       test functions. */
    else if (KNO_EQ(val,current)) cmp=1;
    else if ((CHOICEP(val)) || (PRECHOICEP(val)) ||
	     (CHOICEP(current)) || (PRECHOICEP(current)))
      cmp=kno_overlapp(val,current);
    else if (KNO_EQUAL(val,current))
      cmp=1;
    else cmp=0;
    if (unlock) kno_unlock_table(ht);
    return cmp;}
  else {
    if (unlock) kno_unlock_table(ht);
    return 0;}
}

KNO_EXPORT int kno_hashtable_test(struct KNO_HASHTABLE *ht,lispval key,lispval val)
{
  return hashtable_test(ht,key,val);
}

KNO_EXPORT int kno_hashtable_probe_novoid(struct KNO_HASHTABLE *ht,lispval key)
{
  int unlock=0;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (KNO_EMPTYP(key))
    return KNO_EMPTY;
  else if (ht->table_n_keys == 0)
    return 0;
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_read_lock_table(ht);
    unlock=1;}
  else NO_ELSE;
  if (ht->table_n_keys == 0) {
    if (unlock) kno_unlock_table(ht);
    return 0;}
  else NO_ELSE;
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL)) :
    (hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,NULL));
  if ((result) && (!(VOIDP(result->kv_val)))) {
    if (unlock) kno_unlock_table(ht);
    return 1;}
  else {
    if (unlock) kno_unlock_table(ht);
    return 0;}
}

static struct KNO_HASH_BUCKET **new_hash_buckets(struct KNO_HASHTABLE *ht,int n)
{
  size_t buckets_size = n*sizeof(kno_hash_bucket);
  int use_bigalloc = ( buckets_size > kno_hash_bigthresh );
  struct KNO_HASH_BUCKET **buckets = (use_bigalloc) ?
    (u8_big_alloc(buckets_size)) : (u8_malloc(buckets_size));
  KNO_XTABLE_SET_BIT(ht,KNO_HASHTABLE_BIG_BUCKETS,use_bigalloc);
  memset(buckets,0,buckets_size);
  ht->ht_buckets   = buckets;
  ht->ht_n_buckets = n;
  return buckets;
}

static void setup_hashtable(struct KNO_HASHTABLE *ptr,int n_buckets)
{
  if (n_buckets < 0) n_buckets=kno_get_hashtable_size(-n_buckets);
  ptr->ht_n_buckets=n_buckets;
  ptr->table_n_keys=0;
  KNO_XTABLE_SET_MODIFIED(ptr,0);
  new_hash_buckets(ptr,n_buckets);
}

KNO_EXPORT int kno_hashtable_store(kno_hashtable ht,lispval key,lispval value)
{
  int n_keys = 0, added = 0, unlock = 0;
  lispval newv, oldv;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (KNO_TROUBLEP(key))
    return kno_interr(key);
  else if (KNO_XTABLE_READONLYP(ht))
    return KNO_ERR(-1,kno_ReadOnlyHashtable,"kno_hashtable_store",NULL,key);
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
    unlock = 1;}
  else {}
  if (ht->ht_n_buckets == 0)
    setup_hashtable(ht,kno_init_hash_size);
  n_keys=ht->table_n_keys;
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys))) :
    (hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys)));
  if ( (ht->table_n_keys) > n_keys ) added=1; else added=0;
  KNO_XTABLE_SET_MODIFIED(ht,1);
  oldv=result->kv_val;
  if (PRECHOICEP(value))
    /* Copy prechoices */
    newv=kno_make_simple_choice(value);
  else newv=kno_incref(value);
  if (KNO_ABORTP(newv)) {
    if (unlock) kno_unlock_table(ht);
    return kno_interr(newv);}
  else {
    result->kv_val=newv;
    kno_decref(oldv);}
  if (unlock) kno_unlock_table(ht);
  if (RARELY(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=kno_get_hashtable_size(hashtable_resize_target(ht));
    kno_resize_hashtable(ht,new_size);}
  return added;
}

/* This stores the value if the current value is some version of empty. 
 Empty means VOID, EMPTY, LOCKHOLDER, etc.*/
KNO_EXPORT int kno_hashtable_init_value(kno_hashtable ht,lispval key,lispval value)
{
  int n_keys = 0, added = 0, unlock = 0;
  lispval newv, oldv;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (KNO_TROUBLEP(key))
    return kno_interr(key);
  else if (KNO_XTABLE_READONLYP(ht))
    return KNO_ERR(-1,kno_ReadOnlyHashtable,"kno_hashtable_store",NULL,key);
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
    unlock = 1;}
  else {}
  if (ht->ht_n_buckets == 0)
    setup_hashtable(ht,kno_init_hash_size);
  n_keys=ht->table_n_keys;
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys))) :
    (hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys)));
  if ( (ht->table_n_keys) > n_keys ) added=1; else added=0;
  KNO_XTABLE_SET_MODIFIED(ht,1);
  oldv=result->kv_val;
  if (PRECHOICEP(value))
    /* Copy prechoices */
    newv=kno_make_simple_choice(value);
  else newv=kno_incref(value);
  if (KNO_ABORTP(newv)) {
    if (unlock) kno_unlock_table(ht);
    return kno_interr(newv);}
  else if ( (added) || (KNO_EMPTYP(oldv)) || (KNO_VOIDP(oldv)) ||
	    (KNO_DEFAULTP(oldv)) || (oldv == KNO_LOCKHOLDER) ) {
    result->kv_val=newv;
    added = 1;}
  else {
    kno_decref(newv);
    added = 0;}
  if (unlock) kno_unlock_table(ht);
  if (RARELY(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=kno_get_hashtable_size(hashtable_resize_target(ht));
    kno_resize_hashtable(ht,new_size);}
  return added;
}

KNO_EXPORT int kno_hashtable_replace(kno_hashtable ht,lispval key,
				     lispval value,lispval old)
{
  int n_keys = 0, added = 0, unlock = 0;
  lispval newv, oldv;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if (EMPTYP(key))
    return 0;
  else if (KNO_TROUBLEP(key))
    return kno_interr(key);
  else if (KNO_XTABLE_READONLYP(ht))
    return KNO_ERR(-1,kno_ReadOnlyHashtable,"kno_hashtable_store",NULL,key);
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
    unlock = 1;}
  else {}
  if (ht->ht_n_buckets == 0)
    setup_hashtable(ht,kno_init_hash_size);
  n_keys=ht->table_n_keys;
  int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys))) :
    (hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys)));
  if ( (ht->table_n_keys) > n_keys ) added=1; else added=0;
  KNO_XTABLE_SET_MODIFIED(ht,1);
  oldv=result->kv_val;
  if (PRECHOICEP(value))
    /* Copy prechoices */
    newv=kno_make_simple_choice(value);
  else newv=kno_incref(value);
  if (KNO_ABORTP(newv)) {
    if (unlock) kno_unlock_table(ht);
    return kno_interr(newv);}
  else if ( (oldv == old) ||
	    ( (KNO_VOIDP(old)) && (oldv == KNO_EMPTY) ) ) {
    result->kv_val=newv;
    kno_decref(oldv);
    added = 1;}
  else {
    kno_decref(newv);
    added = 0;}
  if (unlock) kno_unlock_table(ht);
  if (RARELY(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=kno_get_hashtable_size(hashtable_resize_target(ht));
    kno_resize_hashtable(ht,new_size);}
  return added;
}

static int add_to_hashtable(kno_hashtable ht,lispval key,lispval value)
{
  int added=0, n_keys=ht->table_n_keys;
  if (KNO_TROUBLEP(value))
    return kno_interr(value);
  else if (KNO_XTABLE_READONLYP(ht))
    return KNO_ERR(-1,kno_ReadOnlyHashtable,"kno_hashtable_add",NULL,key);
  else if (RARELY(EMPTYP(value)))
    return 0;
  else if (RARELY(KNO_TROUBLEP(value)))
    return kno_interr(value);
  else kno_incref(value);
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (ht->ht_n_buckets == 0) setup_hashtable(ht,kno_init_hash_size);
  n_keys=ht->table_n_keys;
 int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys))) :
    (hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,&(ht->table_n_keys)));
  if (ht->table_n_keys>n_keys) added=1;
  KNO_XTABLE_SET_MODIFIED(ht,1);
  if (VOIDP(result->kv_val))
    result->kv_val=value;
  else {CHOICE_ADD(result->kv_val,value);}
  /* If the value is an prechoice, it doesn't need to be locked
     because it will be protected by the hashtable's lock.  However
     this requires that we always normalize the choice inside of the
     lock (and before we return it).  */
  if ( (result) && (PRECHOICEP(result->kv_val)) ) {
    struct KNO_PRECHOICE *ch=KNO_XPRECHOICE(result->kv_val);
    if (ch->prechoice_uselock) ch->prechoice_uselock=0;}
  return added;
}

static int check_hashtable_size(kno_hashtable ht,ssize_t delta)
{
  size_t n_keys=ht->table_n_keys, n_buckets=ht->ht_n_buckets;
  size_t need_keys=(delta<0) ?
    (n_keys+kno_init_hash_size+3) :
    (n_keys+delta+kno_init_hash_size);
  if (n_buckets==0) {
    setup_hashtable(ht,kno_get_hashtable_size(need_keys));
    return ht->ht_n_buckets;}
  else if (need_keys>n_buckets) {
    size_t new_size=kno_get_hashtable_size(need_keys*2);
    resize_hashtable(ht,new_size,0);
    return ht->ht_n_buckets;}
  else return 0;
}

KNO_EXPORT int kno_hashtable_add(kno_hashtable ht,lispval key,lispval value)
{
  int added=0, unlock = 0;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (EMPTYP(key))
    return 0;
  else if (KNO_XTABLE_READONLYP(ht))
    return KNO_ERR(-1,kno_ReadOnlyHashtable,"kno_hashtable_add",NULL,key);
  else if (RARELY(EMPTYP(value)))
    return 0;
  else if ((KNO_TROUBLEP(value)))
    return kno_interr(value);
  else if ((KNO_TROUBLEP(key)))
    return kno_interr(key);
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
    unlock=1;}
  else {}
  if (!(CONSP(key)))
    check_hashtable_size(ht,3);
  else if (CHOICEP(key))
    check_hashtable_size(ht,KNO_CHOICE_SIZE(key));
  else if (PRECHOICEP(key))
    check_hashtable_size(ht,KNO_PRECHOICE_SIZE(key));
  else check_hashtable_size(ht,3);
  /* These calls unlock the hashtable */
  if ( (CHOICEP(key)) || (PRECHOICEP(key)) ) {
    DO_CHOICES(eachkey,key) {
      added+=add_to_hashtable(ht,key,value);}}
  else added=add_to_hashtable(ht,key,value);
  if (unlock) kno_unlock_table(ht);
  return added;
}

KNO_EXPORT int kno_hashtable_drop
(struct KNO_HASHTABLE *ht,lispval key,lispval value)
{
  int unlock = 0;
  struct KNO_HASH_BUCKET **bucket_loc;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (EMPTYP(key))
    return 0;
  else if (KNO_TROUBLEP(key))
    return kno_interr(key);
  else if (KNO_TROUBLEP(value))
    return kno_interr(value);
  else if (ht->table_n_keys == 0)
    return 0;
  else if (KNO_XTABLE_READONLYP(ht))
    return KNO_ERR(-1,kno_ReadOnlyHashtable,"kno_hashtable_drop",NULL,key);
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
    unlock=1;}
  else NO_ELSE;
 int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
  struct KNO_KEYVAL *result = (eq_cmp) ?
    (eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,&bucket_loc)) :
    (hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,&bucket_loc));
  if (result) {
    lispval cur = result->kv_val, new = KNO_VOID;
    int remove_key = (EMPTYP(cur)) || (VOIDP(value)), changed=1;
    if (remove_key)
      new = KNO_EMPTY;
    else {
      new = kno_difference(cur,value);
      if (EMPTYP(new)) remove_key=1;
      else changed = ( new != cur );}
    if (new != cur) kno_decref(cur);
    if (remove_key) {
      lispval key = result->kv_key;
      struct KNO_HASH_BUCKET *bucket = *bucket_loc;
      *bucket_loc = hash_bucket_remove(bucket,result);
      ht->table_n_keys--;
      kno_decref(key);}
    else result->kv_val = new;
    if (changed) {KNO_XTABLE_SET_MODIFIED(ht,1);}
    kno_unlock_table(ht);
    return 1;}
  if (unlock) kno_unlock_table(ht);
  return 0;
}

static lispval restore_hashtable(lispval tag,lispval alist,
                                 kno_typeinfo e)
{
  lispval *keys, *vals; int n=0; struct KNO_HASHTABLE *new;
  if (PAIRP(alist)) {
    int max=64;
    keys=u8_alloc_n(max,lispval);
    vals=u8_alloc_n(max,lispval);
    {KNO_DOLIST(elt,alist) {
        if (n>=max) {
          keys=u8_realloc_n(keys,max*2,lispval);
          vals=u8_realloc_n(vals,max*2,lispval);
          max=max*2;}
        keys[n]=KNO_CAR(elt); vals[n]=KNO_CDR(elt); n++;}}}
  else return kno_err(kno_DTypeError,"restore_hashtable",NULL,alist);
  new=(struct KNO_HASHTABLE *)kno_make_hashtable(NULL,n*2);
  kno_hashtable_iter(new,kno_table_add,n,keys,vals);
  u8_free(keys); u8_free(vals);
  KNO_XTABLE_SET_MODIFIED(new,0);
  return LISP_CONS(new);
}

/* Evaluting hashtables */

KNO_EXPORT void kno_hash_quality
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

#define TESTOP(x) ( ((x) == kno_table_test) || ((x) == kno_table_haskey) )

static int do_hashtable_op(struct KNO_HASHTABLE *ht,kno_tableop op,
                           lispval key,lispval value)
{
  struct KNO_KEYVAL *result;
  struct KNO_HASH_BUCKET **bucket_loc=NULL;
  int added=0;
  if (EMPTYP(key))
    return 0;
  else if (KNO_TROUBLEP(key))
    return kno_interr(key);
  else if (KNO_TROUBLEP(value))
    return kno_interr(value);
  else NO_ELSE;
  if ( (KNO_XTABLE_READONLYP(ht)) && ( ! (TESTOP(op) ) ) )
    return KNO_ERR2(-1,kno_ReadOnlyHashtable,"do_hashtable_op");
 int eq_cmp = (ht->table_bits)&(KNO_HASHTABLE_COMPARE_EQ);
 switch (op) {
  case kno_table_replace: case kno_table_replace_novoid: case kno_table_drop:
  case kno_table_add_if_present: case kno_table_test: case kno_table_haskey:
  case kno_table_increment_if_present: case kno_table_multiply_if_present:
  case kno_table_maximize_if_present: case kno_table_minimize_if_present: {
    /* These are operations which can be resolved immediately if the key
       does not exist in the table.  It doesn't bother setting up the hashtable
       if it doesn't have to. */
    if (ht->table_n_keys == 0)
      return 0;
    result = (eq_cmp) ?
      (eq_hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,&bucket_loc)) :
      (hashvec_get(key,ht->ht_buckets,ht->ht_n_buckets,&bucket_loc));
    if (result == NULL)
      return 0;
    else if ( op == kno_table_haskey )
      return 1;
    else { added=1; break; }}

  case kno_table_add: case kno_table_add_noref:
    if ((EMPTYP(value)) &&
        ((op == kno_table_add) ||
         (op == kno_table_add_noref) ||
         (op == kno_table_add_if_present) ||
         (op == kno_table_test) ||
         (op == kno_table_drop)))
      return 0;

  default:
    if (ht->ht_n_buckets == 0) setup_hashtable(ht,kno_init_hash_size);
    result = (eq_cmp) ?
      (eq_hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,
			 &(ht->table_n_keys))) :
      (hashvec_insert(key,ht->ht_buckets,ht->ht_n_buckets,
		      &(ht->table_n_keys)));
    added = 1;
  } /* switch (op) */

  if ((result==NULL) &&
      ((op == kno_table_drop)                ||
       (op == kno_table_test)                ||
       (op == kno_table_replace)             ||
       (op == kno_table_add_if_present)      ||
       (op == kno_table_multiply_if_present) ||
       (op == kno_table_maximize_if_present) ||
       (op == kno_table_minimize_if_present) ||
       (op == kno_table_increment_if_present)))
    return 0;
  lispval curval = result->kv_val;
  if ( (op == kno_table_init) &&
       (! ( (KNO_EMPTYP(curval)) || (KNO_VOIDP(curval)) ||
	    (KNO_NULLP(curval)) || (KNO_LOCKHOLDER == curval) ) ) )
    return 0;
  else switch (op) {
  case kno_table_replace_novoid:
    if (VOIDP(curval)) return 0;
  case kno_table_store: case kno_table_replace: case kno_table_init: {
    lispval newv=kno_incref(value);
    result->kv_val=newv;
    kno_decref(curval);
    break;}
  case kno_table_store_noref:
    result->kv_val=value;
    kno_decref(curval);
    break;
  case kno_table_add_if_present:
    if (VOIDP(curval)) break;
  case kno_table_add: case kno_table_add_empty:
    kno_incref(value);
    if (VOIDP(curval))
      result->kv_val=value;
    else {CHOICE_ADD(result->kv_val,value);}
    break;
  case kno_table_add_noref: case kno_table_add_empty_noref:
    if (VOIDP(curval))
      result->kv_val=value;
    else {CHOICE_ADD(result->kv_val,value);}
    break;
  case kno_table_drop: {
    int drop_key = 0;
    lispval newval;
    if (KNO_VOIDP(value)) {
      drop_key = 1;
      newval=EMPTY;}
    else if (KNO_EMPTYP(curval)) {
      drop_key = 1;
      newval=EMPTY;}
    else {
      newval=kno_difference(curval,value);
      if (EMPTYP(newval)) drop_key=1;}
    if (newval != curval) kno_decref(curval);
    if (drop_key) {
      lispval key = result->kv_key;
      struct KNO_HASH_BUCKET *bucket = *bucket_loc;
      *bucket_loc = hash_bucket_remove(bucket,result);
      kno_decref(key);
      ht->table_n_keys--;
      result = NULL;}
    else result->kv_val = newval;
    break;}
  case kno_table_test:
    if ((CHOICEP(curval)) ||
        (PRECHOICEP(curval)) ||
        (CHOICEP(value)) ||
        (PRECHOICEP(value)))
      return kno_overlapp(value,curval);
    else if (LISP_EQUAL(value,curval))
      return 1;
    else return 0;
  case kno_table_default:
    if ((EMPTYP(curval)) || (VOIDP(curval))) {
      result->kv_val = kno_incref(value);}
    else added=0;
    break;
  case kno_table_increment_if_present:
    if (VOIDP(curval)) break;
  case kno_table_increment:
    if ((EMPTYP(curval)) || (VOIDP(curval))) {
      result->kv_val=kno_incref(value);}
    else if (!(NUMBERP(curval)))
      return KNO_ERR(-1,kno_TypeError,"kno_table_increment","number",
                     curval);
    else {
      DO_CHOICES(v,value)
        if ((FIXNUMP(curval)) && (FIXNUMP(v))) {
          long long cval=FIX2INT(curval);
          long long delta=FIX2INT(v);
          result->kv_val=KNO_INT(cval+delta);}
        else if ((KNO_FLONUMP(curval)) &&
                 (KNO_CONS_REFCOUNT(((kno_cons)curval))<2) &&
                 ((FIXNUMP(v)) || (KNO_FLONUMP(v)))) {
          struct KNO_FLONUM *dbl = (kno_flonum) curval;
          if (FIXNUMP(v))
            dbl->floval=dbl->floval+FIX2INT(v);
          else dbl->floval=dbl->floval+KNO_FLONUM(v);}
        else if (NUMBERP(v)) {
          lispval newnum=kno_plus(curval,v);
          if (newnum != curval) {
            kno_decref(curval);
            result->kv_val=newnum;}}
        else return KNO_ERR(-1,kno_TypeError,"kno_table_increment","number",v);}
    break;
  case kno_table_multiply_if_present:
    if (VOIDP(curval)) break;
  case kno_table_multiply:
    if ( (VOIDP(curval)) || (EMPTYP(curval)) )  {
      result->kv_val=kno_incref(value);}
    else if (!(NUMBERP(curval)))
      return KNO_ERR(-1,kno_TypeError,"kno_table_multiply","number",
                     curval);
    else {
      DO_CHOICES(v,value)
        if ((KNO_INTP(curval)) && (KNO_INTP(v))) {
          long long cval=FIX2INT(curval);
          long long factor=FIX2INT(v);
          result->kv_val=KNO_INT(cval*factor);}
        else if ((KNO_FLONUMP(curval)) &&
                 (KNO_CONS_REFCOUNT(((kno_cons)curval))<2) &&
                 ((FIXNUMP(v)) || (KNO_FLONUMP(v)))) {
          struct KNO_FLONUM *dbl=(kno_flonum)curval;
          if (FIXNUMP(v))
            dbl->floval=dbl->floval*FIX2INT(v);
          else dbl->floval=dbl->floval*KNO_FLONUM(v);}
        else if (NUMBERP(v)) {
          lispval newnum=kno_multiply(curval,v);
          if (newnum != curval) {
            kno_decref(curval);
            result->kv_val=newnum;}}
        else return KNO_ERR(-1,kno_TypeError,"table_multiply_op","number",v);}
    break;
  case kno_table_maximize_if_present:
    if (VOIDP(curval)) break;
  case kno_table_maximize:
    if ((EMPTYP(curval)) || (VOIDP(curval))) {
      result->kv_val=kno_incref(value);}
    else if (!(NUMBERP(curval)))
      return KNO_ERR(-1,kno_TypeError,"table_maximize_op","number",curval);
    else {
      if ((NUMBERP(curval)) && (NUMBERP(value))) {
        if (kno_numcompare(value,curval)>0) {
          result->kv_val=kno_incref(value);
          kno_decref(curval);}}
      else return KNO_ERR(-1,kno_TypeError,"table_maximize_op","number",value);}
    break;
  case kno_table_minimize_if_present:
    if (VOIDP(curval)) break;
  case kno_table_minimize:
    if ( (EMPTYP(curval)) || (VOIDP(curval)) ) {
      result->kv_val=kno_incref(value);}
    else if (!(NUMBERP(curval)))
      return KNO_ERR(-1,kno_TypeError,"table_maximize_op","number",curval);
    else {
      if ((NUMBERP(curval)) && (NUMBERP(value))) {
        if (kno_numcompare(value,curval)<0) {
          result->kv_val=kno_incref(value);
          kno_decref(curval);}}
      else return KNO_ERR(-1,kno_TypeError,"table_maximize_op","number",value);}
    break;
  case kno_table_push:
    if ((VOIDP(curval)) || (EMPTYP(curval))) {
      result->kv_val=kno_make_pair(value,NIL);}
    else if (PAIRP(curval)) {
      result->kv_val=kno_conspair(kno_incref(value),curval);}
    else {
      lispval tail=kno_conspair(curval,NIL);
      result->kv_val=kno_conspair(kno_incref(value),tail);}
    break;
  default: {
    u8_byte buf[64];
    added=-1;
    kno_seterr3(BadHashtableMethod,"do_hashtable_op",
                u8_sprintf(buf,64,"0x%x",op));
    break;}
  }
  if (added) {KNO_XTABLE_SET_MODIFIED(ht,1);}
  if ( (added) && (result) && (PRECHOICEP(result->kv_val)) ) {
    struct KNO_PRECHOICE *ch=KNO_XPRECHOICE(result->kv_val);
    if (ch->prechoice_uselock) {
      u8_destroy_mutex(&(ch->prechoice_lock));
      ch->prechoice_uselock=0;}}
  return added;
}

KNO_EXPORT int kno_hashtable_op
(struct KNO_HASHTABLE *ht,kno_tableop op,lispval key,lispval value)
{
  int added=0, unlock=0;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (EMPTYP(key))
    return 0;
  else if (KNO_TROUBLEP(key))
    return kno_interr(key);
  else if (KNO_TROUBLEP(value))
    return kno_interr(value);
  else if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
    unlock=1;}
  else NO_ELSE;
  added=do_hashtable_op(ht,op,key,value);
  if (unlock) kno_unlock_table(ht);
  if (RARELY(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=kno_get_hashtable_size(hashtable_resize_target(ht));
    kno_resize_hashtable(ht,new_size);}
  return added;
}

KNO_EXPORT int kno_hashtable_op_nolock
(struct KNO_HASHTABLE *ht,kno_tableop op,lispval key,lispval value)
{
  int added;
  KEY_CHECK(key,ht); KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (EMPTYP(key))
    return 0;
  else if (KNO_TROUBLEP(key))
    return kno_interr(key);
  else if (KNO_TROUBLEP(value))
    return kno_interr(value);
  else NO_ELSE;
  added=do_hashtable_op(ht,op,key,value);
  if (RARELY(hashtable_needs_resizep(ht))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=kno_get_hashtable_size(hashtable_resize_target(ht));
    resize_hashtable(ht,new_size,0);}
  return added;
}

KNO_EXPORT int kno_hashtable_iter
(struct KNO_HASHTABLE *ht,kno_tableop op,int n,
 const lispval *keys,const lispval *values)
{
  int i=0, added=0, unlock=0;
  KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
    unlock=1;}
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0) {
      added=do_hashtable_op(ht,op,keys[i],values[i]);
      if ( (added) && (TESTOP(op)) ) break;}
    else do_hashtable_op(ht,op,keys[i],values[i]);
    i++;}
  if (unlock) kno_unlock_table(ht);
  if ( (added) && (!(TESTOP(op))) &&
       (RARELY(hashtable_needs_resizep(ht)))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=kno_get_hashtable_size(hashtable_resize_target(ht));
    kno_resize_hashtable(ht,new_size);}
  return added;
}

KNO_EXPORT int kno_hashtable_iter_kv
(struct KNO_HASHTABLE *ht,kno_tableop op,
 struct KNO_CONST_KEYVAL *kvals,int n,
 int lock)
{
  int i=0, added=0, unlock=0;
  KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if ( (lock) && (KNO_XTABLE_USELOCKP(ht)) ) {
    kno_write_lock_table(ht);
    unlock=1;}
  while (i < n) {
    lispval key = kvals[i].kv_key;
    lispval val = kvals[i].kv_val;
    if (added==0) {
      added=do_hashtable_op(ht,op,key,val);
      if ( (added) && (TESTOP(op)) ) break;}
    else do_hashtable_op(ht,op,key,val);
    i++;}
  if (unlock) kno_unlock_table(ht);
  if ( (added) && (!(TESTOP(op))) &&
       (RARELY(hashtable_needs_resizep(ht)))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=kno_get_hashtable_size(hashtable_resize_target(ht));
    resize_hashtable(ht,new_size,lock);}
  return added;
}

KNO_EXPORT int kno_hashtable_iterkeys
(struct KNO_HASHTABLE *ht,kno_tableop op,int n,
 const lispval *keys,lispval value)
{
  int i=0, added=0; int unlock=0;
  KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
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
  if (unlock) kno_unlock_table(ht);
  if ( (added>0) && (!(TESTOP(op))) &&
       (RARELY(hashtable_needs_resizep(ht)))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=kno_get_hashtable_size(hashtable_resize_target(ht));
    kno_resize_hashtable(ht,new_size);}
  return added;
}

KNO_EXPORT int kno_hashtable_itervals
(struct KNO_HASHTABLE *ht,kno_tableop op,int n,
 lispval key,const lispval *values)
{
  int i=0, added=0; int unlock=0;
  KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
    unlock=1;}
  while (i < n) {
    KEY_CHECK(key,ht);
    if (added==0) {
      added=do_hashtable_op(ht,op,key,values[i]);
      if ( (added) && ( (op == kno_table_haskey) || (op == kno_table_test) ) )
        break;}
    else do_hashtable_op(ht,op,key,values[i]);
    i++;}
  if (unlock) kno_unlock_table(ht);
  if ( (added) && (!(TESTOP(op))) &&
       (RARELY(hashtable_needs_resizep(ht)))) {
    /* We resize when n_keys/n_buckets < loading/4;
       at this point, the new size is > loading/2 (a bigger number). */
    int new_size=kno_get_hashtable_size(hashtable_resize_target(ht));
    kno_resize_hashtable(ht,new_size);}
  return added;
}

static int hashtable_getsize(struct KNO_HASHTABLE *ptr)
{
  KNO_CHECK_TYPE_RET(ptr,kno_hashtable_type);
  return ptr->table_n_keys;
}

static int hashtable_modified(struct KNO_HASHTABLE *ptr,int flag)
{
  KNO_CHECK_TYPE_RET(ptr,kno_hashtable_type);
  int modified=KNO_XTABLE_MODIFIEDP(ptr);
  if (flag<0)
    return modified;
  else if (flag) {
    KNO_XTABLE_SET_MODIFIED(ptr,1);
    return modified;}
  else {
    KNO_XTABLE_SET_MODIFIED(ptr,0);
    return modified;}
}

static int hashtable_readonly(struct KNO_HASHTABLE *ptr,int flag)
{
  KNO_CHECK_TYPE_RET(ptr,kno_hashtable_type);
  int readonly=KNO_XTABLE_READONLYP(ptr);
  if (flag<0)
    return readonly;
  else if (flag) {
    KNO_XTABLE_SET_BIT(ptr,KNO_TABLE_READONLY,1);
    KNO_XTABLE_SET_BIT(ptr,KNO_TABLE_USELOCKS,0);
    return readonly;}
  else {
    KNO_XTABLE_SET_BIT(ptr,KNO_TABLE_READONLY,0);
    KNO_XTABLE_SET_BIT(ptr,KNO_TABLE_USELOCKS,1);
    return readonly;}
}

KNO_EXPORT lispval kno_hashtable_keys(struct KNO_HASHTABLE *ptr)
{
  lispval result=EMPTY; int unlock=0;
  KNO_CHECK_TYPE_RETDTYPE(ptr,kno_hashtable_type);
  if (KNO_XTABLE_USELOCKP(ptr)) {u8_read_lock(&ptr->table_rwlock); unlock=1;}
  {
    struct KNO_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct KNO_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          if (VOIDP(kvscan->kv_val)) {kvscan++;continue;}
          kno_incref(kvscan->kv_key);
          CHOICE_ADD(result,kvscan->kv_key);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  return kno_simplify_choice(result);
}

KNO_EXPORT lispval *kno_hashtable_keyvec_n(struct KNO_HASHTABLE *ptr,int *len)
{
  if (!((KNO_CONS_TYPEOF(ptr))==kno_hashtable_type)) {
    kno_seterr(kno_TypeError,"kno_hashtable_keyvec_n","hashtable",(lispval)ptr);
    *len = -1; return NULL;}
  int unlock=0;
  if (KNO_XTABLE_USELOCKP(ptr)) {u8_read_lock(&ptr->table_rwlock); unlock=1;}
  unsigned int n_keys = ptr->table_n_keys, i = 0;
  lispval *keys = u8_alloc_n(n_keys,lispval);
  struct KNO_HASH_BUCKET **scan=ptr->ht_buckets, **lim=scan+ptr->ht_n_buckets;
  while (scan < lim)
    if (*scan) {
      struct KNO_HASH_BUCKET *e=*scan;
      int bucket_len=e->bucket_len;
      struct KNO_KEYVAL *kvscan=&(e->kv_val0);
      struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
      while (kvscan<kvlimit) {
	if (VOIDP(kvscan->kv_val)) {kvscan++;continue;}
	lispval key = kvscan->kv_key;
	kno_incref(key);
	keys[i++]=key;
	kvscan++;}
      scan++;}
    else scan++;
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  *len = i;
  return keys;
}

KNO_EXPORT lispval kno_hashtable_values(struct KNO_HASHTABLE *ptr)
{
  int unlock=0;
  struct KNO_PRECHOICE *prechoice; lispval results;
  int size = ptr->table_n_keys;
  if (size == 0) return KNO_EMPTY;
  KNO_CHECK_TYPE_RETDTYPE(ptr,kno_hashtable_type);
  if (KNO_XTABLE_USELOCKP(ptr)) {u8_read_lock(&ptr->table_rwlock); unlock=1;}
  size=ptr->table_n_keys;
  if (size == 0) {
    /* Race condition */
    if (unlock) u8_rw_unlock(&ptr->table_rwlock);
    return KNO_EMPTY;}
  /* Otherwise, copy the keys into a choice vector. */
  results=kno_init_prechoice(NULL,17*(size),0);
  prechoice=KNO_XPRECHOICE(results);
  {
    struct KNO_HASH_BUCKET **scan=ptr->ht_buckets;
    struct KNO_HASH_BUCKET **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct KNO_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          lispval value=kvscan->kv_val;
          if ((VOIDP(value))||(EMPTYP(value))) {
            kvscan++; continue;}
          kno_incref(value);
          kno_prechoice_add(prechoice,value);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  return kno_simplify_choice(results);
}

KNO_EXPORT lispval kno_hashtable_assocs(struct KNO_HASHTABLE *ptr)
{
  int unlock=0;
  struct KNO_PRECHOICE *prechoice; lispval results;
  int size;
  KNO_CHECK_TYPE_RETDTYPE(ptr,kno_hashtable_type);
  if (KNO_XTABLE_USELOCKP(ptr)) {
    u8_read_lock(&ptr->table_rwlock);
    unlock=1;}
  size=ptr->table_n_keys;
  /* Otherwise, copy the keys into a choice vector. */
  results=kno_init_prechoice(NULL,17*(size),0);
  prechoice=KNO_XPRECHOICE(results);
  {
    struct KNO_HASH_BUCKET **scan=ptr->ht_buckets;
    struct KNO_HASH_BUCKET **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct KNO_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          lispval key=kvscan->kv_key, value=kvscan->kv_val, assoc;
          if (VOIDP(value)) {
            kvscan++; continue;}
          kno_incref(key); kno_incref(value);
          assoc=kno_init_pair(NULL,key,value);
          kno_prechoice_add(prechoice,assoc);
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  return kno_simplify_choice(results);
}

static int free_buckets(struct KNO_HASH_BUCKET **buckets,int len,int big)
{
  if ((buckets) && (len)) {
    struct KNO_HASH_BUCKET **scan=buckets, **lim=scan+len;
    while (scan < lim)
      if (*scan) {
        struct KNO_HASH_BUCKET *e = *scan;
        int n_entries=e->bucket_len;
        struct KNO_CONST_KEYVAL *kvscan =
          ((struct KNO_CONST_KEYVAL *) &(e->kv_val0));
        struct KNO_CONST_KEYVAL *kvlimit = kvscan + n_entries;
        while (kvscan<kvlimit) {
          kno_decref(kvscan->kv_key);
          kno_decref(kvscan->kv_val);
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

KNO_EXPORT int kno_free_buckets(struct KNO_HASH_BUCKET **buckets,
                                int buckets_to_free,
                                int isbig)
{
  return free_buckets(buckets,buckets_to_free,isbig);
}

KNO_EXPORT int kno_reset_hashtable
(struct KNO_HASHTABLE *ht,int n_buckets,int lock)
{
  int unlock = 0;
  KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (n_buckets<0)
    n_buckets=ht->ht_n_buckets;
  if ((lock) && (KNO_XTABLE_USELOCKP(ht))) {
    kno_write_lock_table(ht);
    unlock=1;}
  /* Grab the buckets and their length. We'll free them after we've reset
     the table and released its lock. */
  int old_len = ht->ht_n_buckets;
  int old_big = KNO_XTABLE_BITP(ht,KNO_HASHTABLE_BIG_BUCKETS);
  struct KNO_HASH_BUCKET **old_buckets = ht->ht_buckets;
  /* Now initialize the structure.  */
  if (n_buckets == 0) {
    ht->ht_n_buckets=ht->table_n_keys=0;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets=NULL;}
  else {
    ht->table_n_keys = 0;
    ht->table_load_factor = default_hashtable_loading;
    new_hash_buckets(ht,n_buckets);}
  /* Free the lock, letting other threads use this hashtable. */
  if (unlock) kno_unlock_table(ht);
  /* Now, free the old data... */
  free_buckets(old_buckets,old_len,old_big);
  return n_buckets;
}

/* This resets a hashtable and passes out the internal data to be
   freed separately.  The idea is to hold onto the hashtable's lock
   for as little time as possible. */
KNO_EXPORT int kno_fast_reset_hashtable
(struct KNO_HASHTABLE *ht,int n_buckets,int lock,
 struct KNO_HASH_BUCKET ***bucketsptr,
 int *buckets_to_free,int *bigbuckets)
{
  int unlock=0;
  KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (bucketsptr==NULL)
    return kno_reset_hashtable(ht,n_buckets,lock);
  if (n_buckets<0) n_buckets = ht->ht_n_buckets;
  if ((lock) && (KNO_XTABLE_USELOCKP(ht))) {
    kno_write_lock_table(ht);
    unlock=1;}
  /* Grab the buckets and their length. We'll free them after we've reset
     the table and released its lock. */
  *bucketsptr=ht->ht_buckets;
  *bigbuckets=KNO_XTABLE_BITP(ht,KNO_HASHTABLE_BIG_BUCKETS);
  *buckets_to_free=ht->ht_n_buckets;
  /* Now initialize the structure.  */
  if (n_buckets == 0) {
    ht->ht_n_buckets=ht->table_n_keys=0;
    ht->table_load_factor=default_hashtable_loading;
    ht->ht_buckets=NULL;}
  else {
    ht->table_n_keys=0;
    ht->table_load_factor=default_hashtable_loading;
    new_hash_buckets(ht,n_buckets);}
  /* Free the lock, letting other processes use this hashtable. */
  if (unlock) kno_unlock_table(ht);
  return n_buckets;
}


KNO_EXPORT int kno_swap_hashtable(struct KNO_HASHTABLE *src,
                                  struct KNO_HASHTABLE *dest,
                                  int n_keys,int locked)
{
#define COPYFIELD(field) dest->field=src->field

  if (n_keys<0) n_keys=src->table_n_keys;

  int n_buckets=kno_get_hashtable_size(n_keys), unlock=0;

  KNO_CHECK_TYPE_RET(src,kno_hashtable_type);

  if (!(locked)) {
    if (KNO_XTABLE_USELOCKP(src)) {
      u8_write_lock(&(src->table_rwlock));
      unlock=1;}}

  memset(dest,0,KNO_HASHTABLE_LEN);

  KNO_SET_CONS_TYPE(dest,kno_hashtable_type);

  COPYFIELD(table_n_keys);
  COPYFIELD(table_load_factor);
  COPYFIELD(ht_n_buckets);
  COPYFIELD(ht_buckets);
  KNO_XTABLE_SET_BIT(dest,KNO_HASHTABLE_BIG_BUCKETS,
		     (KNO_XTABLE_BITP(src,KNO_HASHTABLE_BIG_BUCKETS)));
  
  KNO_XTABLE_SET_USELOCK(dest,1);
  u8_init_rwlock(&(dest->table_rwlock));

  /* Now, reset the source table */

  new_hash_buckets(dest,n_buckets);
  src->table_n_keys=0;
  KNO_XTABLE_SET_BIT(src,(KNO_TABLE_MODIFIED|KNO_TABLE_READONLY),0);

  if (unlock) u8_rw_unlock(&(src->table_rwlock));

#undef COPYFIELD
  return 1;
}

KNO_EXPORT int kno_static_hashtable(struct KNO_HASHTABLE *ptr,int type)
{
  int n_conversions=0, unlock=0;
  kno_lisp_type keeptype=(kno_lisp_type) type;
  KNO_CHECK_TYPE_RET(ptr,kno_hashtable_type);
  if (KNO_XTABLE_USELOCKP(ptr)) {
    u8_write_lock(&ptr->table_rwlock);
    unlock=1;}
  {
    struct KNO_HASH_BUCKET **scan=ptr->ht_buckets;
    struct KNO_HASH_BUCKET **lim=scan+ptr->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct KNO_HASH_BUCKET *e=*scan;
        int n_keyvals=e->bucket_len;
        struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+n_keyvals;
        while (kvscan<kvlimit) {
          if ((CONSP(kvscan->kv_val)) &&
              ((type<0) || (TYPEP(kvscan->kv_val,keeptype)))) {
            lispval value=kvscan->kv_val;
            if (!(KNO_STATICP(value))) {
              lispval static_value=kno_static_copy(value);
              if (static_value==value) {
                kno_decref(static_value);
                static_value=kno_register_fcnid(kvscan->kv_val);}
              else {
                kvscan->kv_val=static_value;
                kno_decref(value);
                n_conversions++;}}}
          kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) u8_rw_unlock(&ptr->table_rwlock);
  return n_conversions;
}

KNO_EXPORT lispval kno_make_hashtable(struct KNO_HASHTABLE *ptr,int n_buckets)
{
  if (ptr == NULL) {
    ptr=u8_alloc(struct KNO_HASHTABLE);
    KNO_INIT_FRESH_CONS(ptr,kno_hashtable_type);
    u8_init_rwlock(&(ptr->table_rwlock));}

  if (n_buckets == 0) {
    ptr->ht_n_buckets=ptr->table_n_keys=0;
    ptr->table_bits = KNO_TABLE_USELOCKS;
    ptr->table_load_factor=default_hashtable_loading;
    ptr->ht_buckets=NULL;
    return LISP_CONS(ptr);}
  else {
    if (n_buckets < 0) n_buckets=-n_buckets;
    else n_buckets=kno_get_hashtable_size(n_buckets);

    ptr->table_bits = KNO_TABLE_USELOCKS;
    ptr->table_n_keys=0;
    ptr->table_load_factor=default_hashtable_loading;
    new_hash_buckets(ptr,n_buckets);

    return LISP_CONS(ptr);}
}

KNO_EXPORT lispval kno_make_eq_hashtable(struct KNO_HASHTABLE *ptr,
					 int n_buckets)
{
  if (ptr == NULL) {
    ptr=u8_alloc(struct KNO_HASHTABLE);
    KNO_INIT_FRESH_CONS(ptr,kno_hashtable_type);
    u8_init_rwlock(&(ptr->table_rwlock));}

  ptr->table_bits = KNO_TABLE_USELOCKS | KNO_HASHTABLE_COMPARE_EQ;
  ptr->table_load_factor=default_hashtable_loading;
  ptr->table_n_keys=0;

  if (n_buckets == 0) {
    ptr->ht_n_buckets=0;
    ptr->ht_buckets=NULL;
    return LISP_CONS(ptr);}
  else {
    if (n_buckets < 0)
      n_buckets=-n_buckets;
    else n_buckets=kno_get_hashtable_size(n_buckets);
    new_hash_buckets(ptr,n_buckets);

    return LISP_CONS(ptr);}
}

/* Note that this does not incref the values passed to it. */
KNO_EXPORT lispval kno_init_hashtable(struct KNO_HASHTABLE *ptr,int init_keys,
                                      struct KNO_KEYVAL *inits)
{
  int n_buckets=kno_get_hashtable_size(init_keys);

  if (ptr == NULL) {
    ptr=u8_alloc(struct KNO_HASHTABLE);
    KNO_INIT_FRESH_CONS(ptr,kno_hashtable_type);}
  else {KNO_SET_CONS_TYPE(ptr,kno_hashtable_type);}

  ptr->table_load_factor=default_hashtable_loading;
  ptr->table_bits = KNO_TABLE_USELOCKS;
  ptr->table_n_keys=0;

  struct KNO_HASH_BUCKET **buckets = new_hash_buckets(ptr,n_buckets);

  if (inits) {
    int i=0; while (i<init_keys) {
      struct KNO_KEYVAL *ki=&(inits[i]);
      struct KNO_KEYVAL *hv=hashvec_insert
        (ki->kv_key,buckets,n_buckets,&(ptr->table_n_keys));
      kno_incref(hv->kv_val);
      i++;}}

  KNO_XTABLE_SET_USELOCK(ptr,1);
  u8_init_rwlock(&(ptr->table_rwlock));

  return LISP_CONS(ptr);
}

/* Note that this does not incref the values passed to it. */
KNO_EXPORT lispval kno_initialize_hashtable(struct KNO_HASHTABLE *ptr,
                                            struct KNO_KEYVAL *inits,
                                            int init_keys)
{
  int n_buckets=kno_get_hashtable_size(init_keys);

  if (ptr == NULL) {
    ptr=u8_alloc(struct KNO_HASHTABLE);
    KNO_INIT_FRESH_CONS(ptr,kno_hashtable_type);}
  else {KNO_SET_CONS_TYPE(ptr,kno_hashtable_type);}

  ptr->table_load_factor=default_hashtable_loading;
  ptr->table_bits = KNO_TABLE_USELOCKS;

  struct KNO_HASH_BUCKET **buckets = new_hash_buckets(ptr,n_buckets);
  ptr->table_n_keys=0;

  if (inits) {
    int i=0; while (i<init_keys) {
      struct KNO_KEYVAL *ki=&(inits[i]);
      struct KNO_KEYVAL *hv=
        hashvec_insert(ki->kv_key,buckets,n_buckets,&(ptr->table_n_keys));
      hv->kv_val = ki->kv_val;
      kno_decref(ki->kv_key);
      ki->kv_val=ki->kv_key=VOID;
      i++;}}

  KNO_XTABLE_SET_USELOCK(ptr,1);
  u8_init_rwlock(&(ptr->table_rwlock));

  return LISP_CONS(ptr);
}

static int resize_hashtable(struct KNO_HASHTABLE *ptr,int n_buckets,
                            int need_lock)
{
  int unlock=0;
  KNO_CHECK_TYPE_RET(ptr,kno_hashtable_type);
  if ( (need_lock) && (KNO_XTABLE_USELOCKP(ptr)) ) {
    kno_write_lock_table(ptr);
    unlock=1; }

  struct KNO_HASH_BUCKET **old_buckets = ptr->ht_buckets;
  int old_len = ptr->ht_n_buckets;
  int old_big = KNO_XTABLE_BITP(ptr,KNO_HASHTABLE_BIG_BUCKETS);
  struct KNO_HASH_BUCKET **scan=old_buckets, **lim=scan+old_len;
  struct KNO_HASH_BUCKET **new_buckets=new_hash_buckets(ptr,n_buckets);
  while (scan < lim) {
    if (*scan) {
      struct KNO_HASH_BUCKET *e=*scan++;
      int bucket_len=e->bucket_len;
      struct KNO_KEYVAL *kvscan=&(e->kv_val0);
      struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
      while (kvscan<kvlimit) {
        struct KNO_KEYVAL *nkv =
          hashvec_insert(kvscan->kv_key,new_buckets,n_buckets,NULL);
        nkv->kv_val=kvscan->kv_val; kvscan->kv_val=VOID;
        kno_decref(kvscan->kv_key); kvscan++;}
      u8_free(e);}
    else scan++;}

  if (old_big)
    u8_big_free(old_buckets);
  else u8_free(old_buckets);

  if (unlock) kno_unlock_table(ptr);

  return n_buckets;
}

KNO_EXPORT int kno_resize_hashtable(struct KNO_HASHTABLE *ptr,int n_buckets)
{
  return resize_hashtable(ptr,n_buckets,1);
}


/* VOIDs all values which only have one incoming pointer (from the table
   itself).  This is helpful in conjunction with kno_devoid_hashtable, which
   will then reduce the table to remove such entries. */
KNO_EXPORT int kno_remove_deadwood(struct KNO_HASHTABLE *ptr,
                                   int (*testfn)(lispval,lispval,void *),
                                   void *testdata)
{
  struct KNO_HASH_BUCKET **scan, **lim;
  int n_buckets=ptr->ht_n_buckets, n_keys=ptr->table_n_keys; int unlock=0;
  KNO_CHECK_TYPE_RET(ptr,kno_hashtable_type);
  if ((n_buckets == 0) || (n_keys == 0)) return 0;
  if (KNO_XTABLE_USELOCKP(ptr)) {
    kno_write_lock_table(ptr);
    unlock=1;}
  while (1) {
    int n_cleared=0;
    scan=ptr->ht_buckets; lim=scan+ptr->ht_n_buckets;
    while (scan < lim) {
      if (*scan) {
        struct KNO_HASH_BUCKET *e=*scan++;
        int bucket_len=e->bucket_len;
        struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
        if ((testfn)&&(testfn(kvscan->kv_key,VOID,testdata))) {}
        else while (kvscan<kvlimit) {
            lispval val=kvscan->kv_val;
            if (CONSP(val)) {
              struct KNO_CONS *cval=(struct KNO_CONS *)val;
              if (PRECHOICEP(val))
                cval=(struct KNO_CONS *)
                  (val=kvscan->kv_val=kno_simplify_choice(val));
              if (KNO_CONS_REFCOUNT(cval)==1) {
                kvscan->kv_val=VOID;
                kno_decref(val);
                n_cleared++;}}
            /* ??? In the future, this should probably scan CHOICES
               to remove deadwood as well.  */
            kvscan++;}}
      else scan++;}
    if (n_cleared)
      n_cleared=0;
    else break;}
  if (unlock) kno_unlock_table(ptr);
  return n_buckets;
}

KNO_EXPORT int kno_devoid_hashtable(struct KNO_HASHTABLE *ptr,int locked)
{
  int n_keys=ptr->table_n_keys;
  int n_buckets=ptr->ht_n_buckets;
  int unlock=0;

  KNO_CHECK_TYPE_RET(ptr,kno_hashtable_type);
  if ((n_buckets == 0) || (n_keys == 0)) return 0;
  if ((locked<0)?(KNO_XTABLE_USELOCKP(ptr)):(!(locked))) {
    kno_write_lock_table(ptr);
    /* Avoid race condition */
    n_keys=ptr->table_n_keys;
    n_buckets=ptr->ht_n_buckets,
      unlock=1;}

  struct KNO_HASH_BUCKET **old_buckets = ptr->ht_buckets;
  int old_len = ptr->ht_n_buckets;
  int old_big = KNO_XTABLE_BITP(ptr,KNO_HASHTABLE_BIG_BUCKETS);
  struct KNO_HASH_BUCKET **scan=old_buckets, **lim=scan+old_len;
  struct KNO_HASH_BUCKET **new_buckets=new_hash_buckets(ptr,n_buckets);
  int removed_keys=0;

  if (new_buckets==NULL) {
    if (unlock) kno_unlock_table(ptr);
    return -1;}
  while (scan < lim)
    if (*scan) {
      struct KNO_HASH_BUCKET *e=*scan++;
      int bucket_len=e->bucket_len;
      struct KNO_KEYVAL *kvscan=&(e->kv_val0);
      struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
      while (kvscan<kvlimit)
        if (VOIDP(kvscan->kv_val)) {
          kno_decref(kvscan->kv_key);
	  removed_keys++;
          kvscan++;}
        else {
          struct KNO_KEYVAL *nkv =
            hashvec_insert(kvscan->kv_key,new_buckets,n_buckets,NULL);
          nkv->kv_val=kvscan->kv_val;
          kvscan->kv_val=VOID;
          kno_decref(kvscan->kv_key);
	  kvscan++;}
      u8_free(e);}
    else scan++;

  if (old_big)
    u8_big_free(old_buckets);
  else u8_free(old_buckets);

  ptr->table_n_keys -= removed_keys;

  if (unlock) kno_unlock_table(ptr);
  return n_buckets;
}

KNO_EXPORT int kno_hashtable_stats
(struct KNO_HASHTABLE *ptr,
 int *n_bucketsp,int *n_keysp,int *n_filledp,int *n_collisionsp,
 int *max_bucketp,int *n_valsp,int *max_valsp)
{
  int n_buckets=ptr->ht_n_buckets, n_keys=0, unlock=0;
  int n_filled=0, max_bucket=0, n_collisions=0;
  int n_vals=0, max_vals=0;
  KNO_CHECK_TYPE_RET(ptr,kno_hashtable_type);
  if (KNO_XTABLE_USELOCKP(ptr)) { kno_read_lock_table(ptr); unlock=1;}

  struct KNO_HASH_BUCKET **scan=ptr->ht_buckets;
  struct KNO_HASH_BUCKET **lim=scan+ptr->ht_n_buckets;
  while (scan < lim)
    if (*scan) {
      struct KNO_HASH_BUCKET *e=*scan;
      int bucket_len=e->bucket_len;
      n_filled++;
      n_keys=n_keys+bucket_len;
      if (bucket_len>max_bucket)
        max_bucket=bucket_len;
      if (bucket_len>1)
        n_collisions++;
      if ((n_valsp) || (max_valsp)) {
        struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          int valcount;
          lispval val=kvscan->kv_val;
          if (CHOICEP(val))
            valcount=KNO_CHOICE_SIZE(val);
          else if (PRECHOICEP(val))
            valcount=KNO_PRECHOICE_SIZE(val);
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
  if (unlock) kno_unlock_table(ptr);
  return n_keys;
}

static void copy_keyval(struct KNO_KEYVAL *dest,struct KNO_KEYVAL *src)
{
  lispval key = src->kv_key, val = src->kv_val;
  if (KNO_CONSP(key)) {
    if (KNO_STATIC_CONSP(key))
      key = kno_deep_copy(key);
    else kno_incref(key);}
  if (KNO_CONSP(val)) {
    if (KNO_PRECHOICEP(val))
      val = kno_make_simple_choice(val);
    else if (KNO_STATIC_CONSP(val))
      val = kno_deep_copy(val);
    else kno_incref(val);}
  dest->kv_key = key;
  dest->kv_val = val;
}

KNO_EXPORT struct KNO_HASHTABLE *
kno_copy_hashtable(KNO_HASHTABLE *dest_arg,
                   KNO_HASHTABLE *src,
                   int locksrc)
{
  struct KNO_HASHTABLE *dest;
  int n_keys=0, n_buckets=0, unlock=0, copied_keys=0;
  struct KNO_HASH_BUCKET **buckets, **read, **write, **read_limit;
  if (dest_arg==NULL)
    dest=u8_alloc(struct KNO_HASHTABLE);
  else dest=dest_arg;
  if ( (locksrc) && (KNO_XTABLE_USELOCKP(src)) ) {
    kno_read_lock_table(src);
    unlock=1;}
  if (dest_arg) {
    KNO_INIT_STATIC_CONS(dest,kno_hashtable_type);}
  else {KNO_INIT_CONS(dest,kno_hashtable_type);}
  dest->table_bits = KNO_TABLE_USELOCKS;
  dest->ht_n_buckets=n_buckets=src->ht_n_buckets;
  dest->table_n_keys=n_keys=src->table_n_keys;
  read=buckets=src->ht_buckets;
  read_limit=read+n_buckets;
  dest->table_load_factor=src->table_load_factor;

  write=new_hash_buckets(dest,n_buckets);

  while ( (read<read_limit) && (copied_keys < n_keys) )  {
    if (*read==NULL) {read++; write++;}
    else {
      struct KNO_KEYVAL *kvread, *kvwrite, *kvlimit;
      struct KNO_HASH_BUCKET *he=*read++, *newhe;
      int n=he->bucket_len;
      *write++=newhe=(struct KNO_HASH_BUCKET *)
        u8_malloc(sizeof(struct KNO_HASH_BUCKET)+
                  (n-1)*KNO_KEYVAL_LEN);
      kvread  = &(he->kv_val0);
      kvwrite = &(newhe->kv_val0);
      newhe->bucket_len=n; kvlimit=kvread+n;
      while (kvread<kvlimit) {
        copy_keyval(kvwrite,kvread);
        kvread++;
        kvwrite++;
        copied_keys++;}}}

  if (unlock) kno_unlock_table(src);

  u8_init_rwlock(&(dest->table_rwlock));

  return dest;
}

static lispval copy_hashtable(lispval table,int deep)
{
  return (lispval) kno_copy_hashtable
    (NULL,kno_consptr(kno_hashtable,table,kno_hashtable_type),1);
}

static int unparse_hashtable(u8_output out,lispval x)
{
  struct KNO_HASHTABLE *ht=KNO_XHASHTABLE(x);
  u8_printf(out,"#<HASHTABLE %d/%d #!%p>",ht->table_n_keys,ht->ht_n_buckets,x);
  return 1;
}

/* This makes a hashtable readonly and disables it's mutex.  The
   advantage of this is that it avoids lock contention but, of course,
   the table is read only.
*/

KNO_EXPORT int kno_hashtable_set_readonly(KNO_HASHTABLE *ht,int readonly)
{
  if (readonly) {
    if ( (KNO_XTABLE_READONLYP(ht)) && (!(KNO_XTABLE_USELOCKP(ht))) )
      return 0;
    else if ((!(KNO_XTABLE_READONLYP(ht))) && (!(KNO_XTABLE_USELOCKP(ht))))
      return KNO_ERR2(-1,"Can't make non-locking hashtable readonly",
                      "kno_hashtable_set_readonly");
    kno_read_lock_table(ht);
    KNO_XTABLE_SET_READONLY(ht,1);
    KNO_XTABLE_SET_USELOCK(ht,0);
    return 1;}
  else {
    if (KNO_XTABLE_READONLYP(ht)) return 0;
    if (KNO_XTABLE_USELOCKP(ht)) return 0;
    KNO_XTABLE_SET_READONLY(ht,0);
    KNO_XTABLE_SET_USELOCK(ht,1);
    kno_unlock_table(ht);
    return 1;}
}

KNO_EXPORT int kno_recycle_hashtable(struct KNO_HASHTABLE *c)
{
  struct KNO_HASHTABLE *ht=(struct KNO_HASHTABLE *)c; int unlock=0;
  KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (KNO_XTABLE_USELOCKP(ht)) {
    kno_write_lock_table(ht);
    unlock=1;}
  if (ht->ht_n_buckets==0) {
    if (unlock) kno_unlock_table(ht);
    u8_destroy_rwlock(&(ht->table_rwlock));
    return 0;}
  if (ht->ht_n_buckets) {
    struct KNO_HASH_BUCKET **scan=ht->ht_buckets;
    struct KNO_HASH_BUCKET **lim=scan+ht->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct KNO_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          kno_decref(kvscan->kv_key);
          kno_decref(kvscan->kv_val);
          kvscan++;}
        u8_free(*scan);
        *scan++=NULL;}
      else scan++;
    if (KNO_XTABLE_BITP(ht,KNO_HASHTABLE_BIG_BUCKETS))
      u8_big_free(ht->ht_buckets);
    else u8_free(ht->ht_buckets);}
  ht->ht_buckets=NULL;
  ht->ht_n_buckets=0;
  ht->table_n_keys=0;
  if (unlock) kno_unlock_table(ht);
  u8_destroy_rwlock(&(ht->table_rwlock));
  memset(ht,0,KNO_HASHTABLE_LEN);
  return 0;
}

static void recycle_hashtable(struct KNO_RAW_CONS *c)
{
  kno_recycle_hashtable((struct KNO_HASHTABLE *)c);
  u8_free(c);
}

KNO_EXPORT struct KNO_KEYVAL *kno_hashtable_keyvals
(struct KNO_HASHTABLE *ht,int *sizep,int lock)
{
  struct KNO_KEYVAL *results, *rscan; int unlock=0;
  if ((KNO_CONS_TYPEOF(ht)) != kno_hashtable_type)
    return KNO_ERR(NULL,kno_TypeError,"hashtable",NULL,(lispval)ht);
  if (ht->table_n_keys == 0) {
    *sizep=0;
    return NULL;}
  if ( (lock) && (KNO_XTABLE_USELOCKP(ht)) ) {
    kno_read_lock_table(ht);
    unlock=1;}
  if (ht->ht_n_buckets) {
    struct KNO_HASH_BUCKET **scan=ht->ht_buckets;
    struct KNO_HASH_BUCKET **lim=scan+ht->ht_n_buckets;
    rscan=results=u8_alloc_n(ht->table_n_keys,struct KNO_KEYVAL);
    while (scan < lim)
      if (*scan) {
        struct KNO_HASH_BUCKET *e=*scan;
        int bucket_len=e->bucket_len;
        struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+bucket_len;
        while (kvscan<kvlimit) {
          copy_keyval(rscan,kvscan);
          kvscan++;
          rscan++;}
        scan++;}
      else scan++;
    *sizep=ht->table_n_keys;}
  else {*sizep=0; results=NULL;}
  if (unlock) kno_unlock_table(ht);
  return results;
}

KNO_EXPORT int kno_for_hashtable
(struct KNO_HASHTABLE *ht,kv_valfn f,void *data,int lock)
{
  int unlock=0;
  KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (ht->table_n_keys == 0) return 0;
  if ((lock)&&(KNO_XTABLE_USELOCKP(ht))) {kno_read_lock_table(ht); unlock=1;}
  if (ht->ht_n_buckets) {
    struct KNO_HASH_BUCKET **scan=ht->ht_buckets;
    struct KNO_HASH_BUCKET **lim=scan+ht->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
        struct KNO_HASH_BUCKET *e=*scan;
        int n_entries=e->bucket_len;
        const struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        const struct KNO_KEYVAL *kvlimit=kvscan+n_entries;
        while (kvscan<kvlimit) {
          if (f(kvscan->kv_key,kvscan->kv_val,data)) {
            if (lock) kno_unlock_table(ht);
            return ht->ht_n_buckets;}
          else kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) kno_unlock_table(ht);
  return ht->ht_n_buckets;
}

KNO_EXPORT int kno_for_hashtable_kv
(struct KNO_HASHTABLE *ht,kno_kvfn f,void *data,int lock)
{
  int unlock=0;
  KNO_CHECK_TYPE_RET(ht,kno_hashtable_type);
  if (ht->table_n_keys == 0) return 0;
  if ((lock)&&(KNO_XTABLE_USELOCKP(ht))) {
    kno_write_lock_table(ht);
    unlock=1;}
  if (ht->ht_n_buckets) {
    struct KNO_HASH_BUCKET **scan=ht->ht_buckets, **lim=scan+ht->ht_n_buckets;
    while (scan < lim)
      if (*scan) {
	struct KNO_HASH_BUCKET *e=*scan;
	int n_keyvals=e->bucket_len;
	struct KNO_KEYVAL *kvscan=&(e->kv_val0);
        struct KNO_KEYVAL *kvlimit=kvscan+n_keyvals;
        while (kvscan<kvlimit) {
	  if (PRECHOICEP(kvscan->kv_val)) {
	    struct KNO_KEYVAL kv;
	    kv.kv_key = kvscan->kv_key;
	    kv.kv_val = kno_normalize_choice(kvscan->kv_val,0);
	    int rv = f(&kv,data);
	    kno_decref(kv.kv_val);
	    if (rv) {
	      if (unlock) kno_unlock_table(ht);
	      return ht->ht_n_buckets;}
	    else kvscan++;}
	  else if (f(kvscan,data)) {
            if (unlock) kno_unlock_table(ht);
            return ht->ht_n_buckets;}
          else kvscan++;}
        scan++;}
      else scan++;}
  if (unlock) kno_unlock_table(ht);
  return ht->ht_n_buckets;
}

/* Hashsets */

KNO_EXPORT void kno_init_hashset(struct KNO_HASHSET *hashset,int size,
                                 int stack_cons)
{
  lispval *slots;
  int i=0, n_slots=kno_get_hashtable_size(size);
  if (stack_cons) {
    KNO_INIT_STATIC_CONS(hashset,kno_hashset_type);}
  else {KNO_INIT_CONS(hashset,kno_hashset_type);}
  hashset->hs_n_buckets=n_slots;
  hashset->table_bits = KNO_TABLE_USELOCKS;
  hashset->hs_load_factor=default_hashset_loading;
  hashset->hs_buckets=slots=u8_alloc_n(n_slots,lispval);
  while (i < n_slots) slots[i++]=KNO_EMPTY;
  u8_init_rwlock(&(hashset->table_rwlock));
  return;
}

KNO_EXPORT lispval kno_make_hashset()
{
  struct KNO_HASHSET *h=u8_alloc(struct KNO_HASHSET);
  kno_init_hashset(h,kno_init_hash_size,KNO_MALLOCD_CONS);
  KNO_INIT_CONS(h,kno_hashset_type);
  return LISP_CONS(h);
}

/* This does a simple binary search of a sorted choice vector,
   looking for a particular element. Once more, we separate out the
   atomic case because it just requires pointer comparisons.  */
static int choice_containsp(lispval x,struct KNO_CHOICE *choice)
{
  int size = KNO_XCHOICE_SIZE(choice);
  const lispval *bottom = KNO_XCHOICE_DATA(choice), *top = bottom+(size-1);
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
      int comparison = __kno_cons_compare(x,*middle);
      if (comparison == 0) return 1;
      else if (comparison<0) top = middle-1;
      else bottom = middle+1;}
    return 0;}
}

KNO_EXPORT int kno_hashset_get(struct KNO_HASHSET *h,lispval key)
{
  if (h->hs_n_elts==0) return 0;
  int hash = kno_hash_lisp(key), unlock=0;
  if (KNO_XTABLE_USELOCKP(h)) {
    kno_read_lock_table(h);
    unlock=1;}
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets, bucket = hash%n_slots;
  lispval contents = slots[bucket];
  int rv=0;
  if (KNO_EMPTY_CHOICEP(contents)) rv=0;
  else if (contents==key) rv=1;
  else if (!(KNO_AMBIGP(contents)))
    rv=KNO_EQUALP(contents,key);
  else if (KNO_CHOICEP(contents))
    rv=choice_containsp(key,(kno_choice)contents);
  else {
    if (unlock) {
      kno_unlock_table(h);
      kno_write_lock_table(h);}
    if (h->hs_n_buckets!=n_slots) {
      n_slots=h->hs_n_buckets;
      bucket = hash%n_slots;
      slots = h->hs_buckets;}
    contents = slots[bucket];
    if (KNO_PRECHOICEP(contents))
      slots[bucket]=contents=kno_simplify_choice(contents);
    if (KNO_EMPTYP(contents)) rv=0;
    else if (contents==key) rv=1;
    else if (KNO_CHOICEP(contents))
      rv=choice_containsp(key,(kno_choice)contents);
    else rv=KNO_EQUALP(contents,key);}
  if (unlock) kno_unlock_table(h);
  return rv;
}

static lispval hashset_probe(struct KNO_HASHSET *h,lispval key)
{
  if (h->hs_n_elts==0)
    return KNO_EMPTY;
  int hash = kno_hash_lisp(key), unlock=0;
  if (KNO_XTABLE_USELOCKP(h)) {
    kno_read_lock_table(h);
    unlock=1;}
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets, bucket = hash%n_slots;
  lispval contents = slots[bucket];
  lispval result=KNO_EMPTY;
  if (KNO_EMPTY_CHOICEP(contents)) {}
  else if (contents==key)
    result=contents;
  else if (!(KNO_AMBIGP(contents))) {
    if (KNO_EQUALP(contents,key))
      result=contents;}
  else {
    KNO_DO_CHOICES(entry,contents) {
      if ( (entry==key) || (KNO_EQUALP(entry,key)) ) {
        result=entry; KNO_STOP_DO_CHOICES; break;}}}
  if (unlock) kno_unlock_table(h);
  return kno_incref(result);
}

static lispval hashset_intern(struct KNO_HASHSET *h,lispval key)
{
  int hash = kno_hash_lisp(key), unlock = 0;
  if (KNO_XTABLE_USELOCKP(h)) {
    kno_read_lock_table(h);
    unlock=1;}
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets, bucket = hash%n_slots;
  lispval contents = slots[bucket];
  lispval result=KNO_EMPTY;
  if (KNO_EMPTY_CHOICEP(contents)) {}
  else if (contents==key)
    result=contents;
  else if (!(KNO_AMBIGP(contents))) {
    if (KNO_EQUALP(contents,key))
      result=contents;}
  else {
    KNO_DO_CHOICES(entry,contents) {
      if ( (entry==key) || (KNO_EQUALP(entry,key)) ) {
        result=entry; KNO_STOP_DO_CHOICES; break;}}}
  if (KNO_EMPTYP(result)) {
    kno_incref(key);
    KNO_ADD_TO_CHOICE(contents,key);
    slots[bucket]=contents;
    result=key;}
  if (unlock) kno_unlock_table(h);
  return kno_incref(result);
}

KNO_EXPORT lispval kno_hashset_intern(struct KNO_HASHSET *h,lispval key,int add)
{
  if (add) {
    lispval existing = hashset_probe(h,key);
    if (KNO_EMPTYP(existing)) {
      lispval added = hashset_intern(h,key);
      return added;}
    else return existing;}
  else return hashset_probe(h,key);
}

KNO_EXPORT lispval kno_hashset_elts(struct KNO_HASHSET *h,int clean)
{
  /* A clean value of -1 indicates that the hashset will be reset
     entirely (freed) if mallocd. */
  KNO_CHECK_TYPE_RETDTYPE(h,kno_hashset_type);
  if (h->hs_n_elts==0) {
    if (clean<0) {
      if (KNO_MALLOCD_CONSP(h))
        kno_decref((lispval)h);
      else {
        u8_free(h->hs_buckets);
        h->hs_n_buckets=h->hs_n_elts=0;}}
    return EMPTY;}
  else {
    int unlock = 0;
    if (KNO_XTABLE_USELOCKP(h)) {
      if (clean)
        kno_write_lock_table(h);
      else kno_read_lock_table(h);
      unlock=1;}
    lispval results=EMPTY;
    lispval *scan=h->hs_buckets, *limit=scan+h->hs_n_buckets;
    while (scan<limit) {
      lispval v = *scan;
      if ((EMPTYP(v)) || (VOIDP(v)) || (KNO_NULLP(v))) {
        *scan++=EMPTY; continue;}
      if (KNO_PRECHOICEP(v)) {
        if (clean) {
          lispval norm = kno_simplify_choice(v);
          KNO_ADD_TO_CHOICE(results,norm);
          *scan=EMPTY;}
        else {
          lispval norm = kno_make_simple_choice(v);
          KNO_ADD_TO_CHOICE(results,norm);}}
      else {
        KNO_ADD_TO_CHOICE(results,v);
        if (clean) *scan=KNO_EMPTY;
        else kno_incref(v);}
      scan++;}
    if (unlock) kno_unlock_table(h);
    if (clean<0) {
      if (KNO_MALLOCD_CONSP(h)) {
        kno_decref((lispval)h);}
      else {
        u8_free(h->hs_buckets);
        h->hs_n_buckets=h->hs_n_elts=0;}}
    return kno_simplify_choice(results);}
}

KNO_EXPORT lispval *kno_hashset_vec(struct KNO_HASHSET *h,ssize_t *outlen)
{
  /* A clean value of -1 indicates that the hashset will be reset
     entirely (freed) if mallocd. */
  KNO_CHECK_TYPE_RETVAL(h,kno_hashset_type,NULL);
  if (h->hs_n_elts==0) {
    *outlen=0;
    return NULL;}
  else {
    int unlock = 0;
    if (KNO_XTABLE_USELOCKP(h)) {
      kno_read_lock_table(h);
      unlock=1;}
    ssize_t n_elts = h->hs_n_elts;
    lispval *results = u8_alloc_n(n_elts,lispval);
    lispval *write = results, *write_limit = results+n_elts;
    lispval *scan=h->hs_buckets, *limit=scan+h->hs_n_buckets;
    while (scan<limit) {
      lispval v = *scan;
      if (EMPTYP(v)) continue;
      else if ((VOIDP(v)) || (KNO_NULLP(v))) {
        *scan++=EMPTY; continue;}
      else if ( (KNO_CHOICEP(v)) || (KNO_PRECHOICEP(v)) ) {
	lispval choice = (KNO_PRECHOICEP(v)) ? (kno_make_simple_choice(v)) : (v);
	ssize_t size = KNO_CHOICE_SIZE(choice);
	if ((write+size)>write_limit) goto corrupt_hashset;
	if (KNO_ATOMIC_CHOICEP(choice)) {
	  const lispval *elts = KNO_CHOICE_ELTS(choice);
	  kno_lspcpy(write,elts,size);
	  write += size;}
	else {
	  KNO_DO_CHOICES(elt,choice) {
	    kno_incref(elt);
	    *write++=elt;}}
	if (choice != v) kno_decref(choice);}
      else {
	if (write>write_limit) goto corrupt_hashset;
	kno_incref(v); *write++=v;}
      scan++;}
    if (unlock) kno_unlock_table(h);
    *outlen = write-results;
    return results;
  corrupt_hashset:
    kno_decref_elts(results,write-results);
    u8_free(results);
    kno_unlock_table(h);
    lispval irritant = (KNO_MALLOCD_CONSP(h)) ? ((lispval) h) : (KNO_VOID);
    kno_seterr("CorruptHashSet","kno_hashset_vec",NULL,irritant);
    *outlen=-1;
    return NULL;}
}

KNO_EXPORT int kno_reset_hashset(struct KNO_HASHSET *h)
{
  KNO_CHECK_TYPE_RETDTYPE(h,kno_hashset_type);
  if (h->hs_n_elts==0)
    return 0;
  else {
    int unlock=0;
    if (KNO_XTABLE_USELOCKP(h)) {
      kno_write_lock_table(h);
      unlock=1;}
    int n_elts=h->hs_n_elts;
    lispval *scan=h->hs_buckets, *limit=scan+h->hs_n_buckets;
    while (scan<limit) {
      lispval v=*scan;
      *scan=KNO_EMPTY_CHOICE;
      kno_decref(v);
      scan++;}
    h->hs_n_elts=0;
    if (unlock) kno_unlock_table(h);
    return n_elts;}
}

static int hashset_getsize(struct KNO_HASHSET *h)
{
  KNO_CHECK_TYPE_RETDTYPE(h,kno_hashset_type);
  return h->hs_n_elts;
}

static int hashset_modified(struct KNO_HASHSET *ptr,int flag)
{
  KNO_CHECK_TYPE_RET(ptr,kno_hashset_type);
  int modified = KNO_XTABLE_MODIFIEDP(ptr);
  if (flag<0)
    return modified;
  else if (flag) {
    KNO_XTABLE_SET_MODIFIED(ptr,1);
    return modified;}
  else {
    KNO_XTABLE_SET_MODIFIED(ptr,0);
    return modified;}
}

static lispval hashset_elts(struct KNO_HASHSET *h)
{
  return kno_hashset_elts(h,0);
}

static void hashset_add_raw(struct KNO_HASHSET *h,lispval key)
{
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets;
  int hash = kno_hash_lisp(key), bucket = hash%n_slots;
  lispval contents = slots[bucket];
  KNO_ADD_TO_CHOICE(contents,key);
  slots[bucket] = contents;
}

static size_t grow_hashset(struct KNO_HASHSET *h,ssize_t target)
{
  int i=0, lim=h->hs_n_buckets;
  size_t new_size= (target<=0) ?
    (kno_get_hashtable_size(hashset_resize_target(h))) :
    (target);
  lispval *slots=h->hs_buckets;
  lispval *newslots=u8_alloc_n(new_size,lispval);
  while (i<new_size) newslots[i++]=KNO_EMPTY_CHOICE;
  h->hs_buckets=newslots; h->hs_n_buckets=new_size;
  i=0; while (i < lim) {
    lispval content=slots[i];
    if ((EMPTYP(content)) || (VOIDP(content)) || (KNO_NULLP(content))) {
      slots[i++]=KNO_EMPTY; continue;}
    else if (KNO_AMBIGP(content)) {
      KNO_DO_CHOICES(val,content) {
        hashset_add_raw(h,val);
        kno_incref(val);}
      kno_decref(content);}
    else hashset_add_raw(h,content);
    slots[i++]=KNO_EMPTY;}
  u8_free(slots);
  return new_size;
}

KNO_EXPORT ssize_t kno_grow_hashset(struct KNO_HASHSET *h,ssize_t target)
{
  int unlock = 0;
  if (KNO_XTABLE_USELOCKP(h)) {
    kno_write_lock_table(h);
    unlock=1;}
  size_t rv=grow_hashset(h,target);
  if (unlock) kno_unlock_table(h);
  return rv;
}

static int hashset_test_add(struct KNO_HASHSET *h,lispval key)
{
  if ( (VOIDP(key)) || (EMPTYP(key)) ) return 0;
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets;
  int hash = kno_hash_lisp(key), bucket = hash%n_slots;
  lispval contents = slots[bucket];
  if (KNO_EMPTYP(contents)) {
    kno_incref(key);
    slots[bucket] = key;
    return 1;}
  else if (contents == key)
    return 0;
  else if (KNO_CHOICEP(contents)) {
    if (choice_containsp(key,(kno_choice)contents))
      return 0;
    else {
      kno_incref(key);
      KNO_ADD_TO_CHOICE(slots[bucket],key);
      return 1;}}
  else if (KNO_PRECHOICEP(contents)) {
    if (kno_choice_containsp(contents,key))
      return 0;
    else {
      kno_incref(key);
      KNO_ADD_TO_CHOICE(slots[bucket],key);
      return 1;}}
  else if (KNO_EQUALP(contents,key))
    return 0;
  else {
    kno_incref(key);
    KNO_ADD_TO_CHOICE(slots[bucket],key);
    return 1;}
}

static int hashset_test_drop(struct KNO_HASHSET *h,lispval key)
{
  lispval *slots = h->hs_buckets;
  int n_slots = h->hs_n_buckets;
  int hash = kno_hash_lisp(key), bucket = hash%n_slots;
  lispval contents = slots[bucket];
  if (KNO_EMPTYP(contents))
    return 0;
  else if (contents == key) {
    slots[bucket]=KNO_EMPTY;
    kno_decref(key);
    return 1;}
  else if (KNO_CHOICEP(contents)) {
    if (!(choice_containsp(key,(kno_choice)contents)))
      return 1;}
  else if (KNO_PRECHOICEP(contents)) {
    if (!(kno_choice_containsp(contents,key)))
      return 0;}
  else if (KNO_EQUALP(contents,key)) {
    slots[bucket]=KNO_EMPTY;
    kno_decref(key);
    return 1;}
  else {}
  lispval new_contents = kno_difference(contents,key);
  slots[bucket]=new_contents;
  kno_decref(contents);
  return 1;
}

static int hashset_mod(struct KNO_HASHSET *h,lispval key,int add)
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

KNO_EXPORT int kno_hashset_mod(struct KNO_HASHSET *h,lispval key,int add)
{
  int rv=0, unlock=0;
  if (KNO_XTABLE_USELOCKP(h)) {
    kno_write_lock_table(h);
    unlock = 1;}
  rv=hashset_mod(h,key,add);
  if (unlock) kno_unlock_table(h);
  return rv;
}

/* This adds without locking or incref. */
KNO_EXPORT void kno_hashset_add_raw(struct KNO_HASHSET *h,lispval key)
{
  hashset_add_raw(h,key);
}

KNO_EXPORT int kno_hashset_add(struct KNO_HASHSET *h,lispval keys)
{
  if ((CHOICEP(keys))||(PRECHOICEP(keys))) {
    int n_vals=KNO_CHOICE_SIZE(keys), unlock=0;
    size_t need_size=ceil((n_vals+h->hs_n_elts)*h->hs_load_factor), n_adds=0;
    if (need_size>h->hs_n_buckets) kno_grow_hashset(h,need_size);
    if (KNO_XTABLE_USELOCKP(h)) {
      kno_write_lock_table(h);
      unlock=1;}
    {DO_CHOICES(key,keys) {
        if (hashset_mod(h,key,1)) n_adds++;}}
    if (unlock) kno_unlock_table(h);
    return n_adds;}
  else if (EMPTYP(keys))
    return 0;
  else return kno_hashset_mod(h,keys,1);
}

KNO_EXPORT ssize_t kno_hashset_merge(struct KNO_HASHSET *h,struct KNO_HASHSET *src)
{
  int lock_src = KNO_XTABLE_USELOCKP(src), lock_dest=KNO_XTABLE_USELOCKP(h);
  if (lock_src) kno_read_lock_table(src);
  if (lock_dest) kno_write_lock_table(h);
  ssize_t cur_vals = h->hs_n_elts, add_vals = src->hs_n_elts, n_adds = 0;
  size_t need_size=ceil((cur_vals+add_vals)*h->hs_load_factor);
  if (need_size>h->hs_n_buckets) grow_hashset(h,need_size);
  lispval *scan=src->hs_buckets, *lim=scan+src->hs_n_buckets;
  while (scan<lim) {
    lispval v = *scan++;
    if (KNO_CONSP(v)) {
      if (KNO_CHOICEP(v)) {
	KNO_ITER_CHOICES(vscan,vlimit,v);
	while (vscan<vlimit) {
	  lispval v = *vscan++;
	  if (hashset_test_add(h,v)) n_adds++;}}
      else if (KNO_PRECHOICEP(v)) {
	lispval norm = kno_make_simple_choice(v);
	KNO_ITER_CHOICES(vscan,vlimit,norm);
	while (vscan<vlimit) {
	  lispval v = *vscan++;
	  if (hashset_test_add(h,v)) n_adds++;}
	kno_decref(norm);}
      else if (hashset_test_add(h,v))
	n_adds++;
      else NO_ELSE;}
    else if ( (KNO_VOIDP(v)) || (KNO_EMPTYP(v)) ) {}
    else if (hashset_test_add(h,v)) n_adds++;
    else NO_ELSE;}
  if (lock_src) kno_unlock_table(src);
  if (lock_dest) kno_unlock_table(h);
  return n_adds;
}

KNO_EXPORT int kno_recycle_hashset(struct KNO_HASHSET *h)
{
  int unlock = 0;
  if (KNO_XTABLE_USELOCKP(h)) {
    kno_write_lock_table(h);
    unlock = 1;}
  lispval *scan=h->hs_buckets, *lim=scan+h->hs_n_buckets;
  while (scan<lim) {
    lispval v = *scan;
    kno_decref(v);
    *scan++=KNO_EMPTY;}
  u8_free(h->hs_buckets);
  if (unlock) kno_unlock_table(h);
  u8_destroy_rwlock(&(h->table_rwlock));
  memset(h,0,sizeof(struct KNO_HASHSET));
  return 1;
}

static void recycle_hashset(struct KNO_RAW_CONS *c)
{
  kno_recycle_hashset((struct KNO_HASHSET *)c);
  u8_free(c);
}

static lispval copy_hashset(lispval table,int deep)
{
  struct KNO_HASHSET *ptr=kno_consptr(kno_hashset,table,kno_hashset_type);
  return kno_copy_hashset(NULL,ptr);
}

KNO_EXPORT lispval kno_copy_hashset(struct KNO_HASHSET *hnew,struct KNO_HASHSET *h)
{
  int unlock = 0;
  if (KNO_XTABLE_USELOCKP(h)) {
    kno_read_lock_table(h);
    unlock = 1;}
  int n_slots = h->hs_n_buckets;
  lispval *read = h->hs_buckets, *readlim = read+n_slots;
  if (hnew==NULL) hnew=u8_alloc(struct KNO_HASHSET);
  KNO_INIT_CONS(hnew,kno_hashset_type);
  hnew->hs_buckets=u8_alloc_n(h->hs_n_buckets,lispval);
  lispval *write=hnew->hs_buckets;
  while (read<readlim) {
    lispval v=*read++;
    if (KNO_EMPTYP(v))
      *write++=KNO_EMPTY;
    else if (KNO_PRECHOICEP(v))
      *write++=kno_make_simple_choice(v);
    else *write++=kno_incref(v);}
  hnew->hs_n_elts=h->hs_n_elts;
  hnew->hs_load_factor=h->hs_load_factor;
  hnew->table_bits = KNO_TABLE_USELOCKS;
  if (unlock) kno_unlock_table(h);
  u8_init_rwlock((&hnew->table_rwlock));
  return (lispval) hnew;
}

static int unparse_hashset(u8_output out,lispval x)
{
  struct KNO_HASHSET *hs=((struct KNO_HASHSET *)x);
  u8_printf(out,"#<HASHSET%s %d/%d>",
	    ((KNO_XTABLE_MODIFIEDP(hs))?("(m)"):("")),
            hs->hs_n_elts,hs->hs_n_buckets);
  return 1;
}

static lispval hashset_get(lispval x,lispval key)
{
  struct KNO_HASHSET *h=kno_consptr(struct KNO_HASHSET *,x,kno_hashset_type);
  if (kno_hashset_get(h,key))
    return KNO_TRUE;
  else return KNO_FALSE;
}
static int hashset_store(lispval x,lispval key,lispval val)
{
  struct KNO_HASHSET *h=kno_consptr(struct KNO_HASHSET *,x,kno_hashset_type);
  if (KNO_TRUEP(val))
    return kno_hashset_mod(h,key,1);
  else if (FALSEP(val))
    return kno_hashset_mod(h,key,0);
  else return KNO_ERR(-1,kno_RangeError,_("value is not a boolean"),NULL,val);
}

/* Annotated get functions */

static lispval init_annotations(struct KNO_ANNOTATED *astruct)
{
  lispval annotations = astruct->annotations;
  if ( (annotations!=KNO_NULL) &&
       (KNO_CONSP(annotations)) &&
       (KNO_XXCONS_TYPEP((annotations),kno_coretable_type)) )
    return annotations;
  lispval consed = kno_make_slotmap(2,0,NULL);
  KNO_LOCK_PTR(astruct);
  annotations = astruct->annotations;
  if ( (annotations != KNO_NULL) &&
       (KNO_CONSP(annotations)) &&
       (KNO_XXCONS_TYPEP(annotations,kno_coretable_type)) ) {
    KNO_UNLOCK_PTR(astruct);
    kno_decref(consed);
    return annotations;}
  else {
    astruct->annotations=consed;
    KNO_UNLOCK_PTR(astruct);
    return consed;}
}

static lispval annotated_get(lispval x,lispval key,lispval dflt)
{
  struct KNO_ANNOTATED *a = (kno_annotated) x;
  if ( (a->annotations == KNO_NULL) || (a->annotations == KNO_EMPTY) )
    return KNO_EMPTY;
  else return kno_get(a->annotations,key,dflt);
}

static int annotated_test(lispval x,lispval key,lispval val)
{
  struct KNO_ANNOTATED *a = (kno_annotated) x;
  if ( (a->annotations == KNO_NULL) || (a->annotations == KNO_EMPTY) )
    return 0;
  lispval annotations = a->annotations;
  if ( (KNO_CONSP(annotations)) &&
       (KNO_XXCONS_TYPEP(annotations,kno_coretable_type)) )
    return kno_test(annotations,key,val);
  else return 0;
}

static int annotated_store(lispval x,lispval key,lispval val)
{
  lispval annotations = init_annotations((kno_annotated)x);
  return kno_store(annotations,key,val);
}

static int annotated_add(lispval x,lispval key,lispval val)
{
  lispval annotations = init_annotations((kno_annotated)x);
  return kno_add(annotations,key,val);
}

static int annotated_drop(lispval x,lispval key,lispval val)
{
  lispval annotations = init_annotations((kno_annotated)x);
  return kno_drop(annotations,key,val);
}

static lispval annotated_getkeys(lispval x)
{
  struct KNO_ANNOTATED *a = (kno_annotated) x;
  if ( (a->annotations == KNO_NULL) || (a->annotations == KNO_EMPTY) )
    return KNO_EMPTY;
  lispval annotations = a->annotations;
  if ( (KNO_CONSP(annotations)) &&
       (KNO_XXCONS_TYPEP((annotations),kno_coretable_type)) )
    return kno_getkeys(annotations);
  else return KNO_EMPTY;
}

static struct KNO_TABLEFNS annotated_tablefns =
  {
   annotated_get, /* get */
   annotated_store,/* store */
   annotated_add, /* add */
   annotated_drop, /* drop */
   annotated_test, /* test */
   NULL, /* readonly */
   NULL, /* modified */
   NULL, /* finished */
   NULL, /*getsize */
   annotated_getkeys, /* getkeys */
   NULL, /* keyvec_n */
   NULL, /* keyvals */
   NULL /* tablep */};

struct KNO_TABLEFNS *kno_annotated_tablefns = &annotated_tablefns;

/* Generic table functions */

#define CHECKPTR(arg,cxt)                  \
  if (RARELY((!(KNO_CHECK_PTR(arg))))) \
    _kno_bad_pointer(arg,cxt); else {}

static int bad_table_call(lispval arg,kno_lisp_type type,void *handler,
                          u8_context cxt)
{
  if (handler)
    return 0;
  else if (RARELY(kno_tablefns[type]==NULL))
    return KNO_ERR(-1,NotATable,cxt,NULL,arg);
  else if ( (kno_tablefns[type]->tablep) &&
            ((kno_tablefns[type]->tablep)(arg)) )
    return KNO_ERR(1,NotATable,cxt,NULL,arg);
  else return KNO_ERR(-1,kno_NoMethod,cxt,NULL,arg);
}

#define BAD_TABLEP(arg,type,meth,cxt)                                   \
  (bad_table_call                                                       \
   (arg,type,                                                           \
    ((kno_tablefns[type]) ? ((kno_tablefns[type])->meth) : (NULL)),     \
    cxt))
#define NOT_TABLEP(arg,type,cxt)                                        \
  ( (kno_tablefns[type] == NULL) && (bad_table_call(arg,type,NULL,cxt)) )

KNO_EXPORT lispval kno_get(lispval arg,lispval key,lispval dflt)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_get/table");
  CHECKPTR(key,"kno_get/key");
  CHECKPTR(key,"kno_get/dflt");
  if ((EMPTYP(arg))||(EMPTYP(key)))
    return EMPTY;
  else if (KNO_UNAMBIGP(key)) {
    if (BAD_TABLEP(arg,argtype,get,"kno_get"))
      return KNO_ERROR;
    else return (kno_tablefns[argtype]->get)(arg,key,dflt);}
  else if (BAD_TABLEP(arg,argtype,get,"kno_get"))
    return KNO_ERROR;
  else {
    lispval results=EMPTY;
    lispval (*getfn)(lispval,lispval,lispval)=kno_tablefns[argtype]->get;
    DO_CHOICES(each,key) {
      lispval values=getfn(arg,each,EMPTY);
      if (KNO_ABORTP(values)) {
        kno_decref(results); return values;}
      CHOICE_ADD(results,values);}
    if (EMPTYP(results)) return kno_incref(dflt);
    else return results;}
}

KNO_EXPORT int kno_store(lispval arg,lispval key,lispval value)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_store/table");
  CHECKPTR(key,"kno_store/key");
  CHECKPTR(value,"kno_store/value");
  if ((EMPTYP(arg))||(EMPTYP(key)))
    return 0;
  else if (KNO_UNAMBIGP(key)) {
    if (BAD_TABLEP(arg,argtype,store,"kno_store"))
      return -1;
    else return (kno_tablefns[argtype]->store)(arg,key,value);}
  else if (BAD_TABLEP(arg,argtype,store,"kno_store"))
    return -1;
  else {
    int (*storefn)(lispval,lispval,lispval)=kno_tablefns[argtype]->store;
    DO_CHOICES(each,key) {
      int retval=storefn(arg,each,value);
      if (retval<0) return retval;}
    return 1;}
}

KNO_EXPORT int kno_add(lispval arg,lispval key,lispval value)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_add/table");
  CHECKPTR(key,"kno_add/key");
  CHECKPTR(value,"kno_add/value");
  if (RARELY((EMPTYP(arg))||(EMPTYP(key))))
    return 0;
  else if (KNO_UNAMBIGP(key)) {
    if (NOT_TABLEP(arg,argtype,"kno_add"))
      return -1;
    else if (kno_tablefns[argtype]->add)
      return (kno_tablefns[argtype]->add)(arg,key,value);
    else if ((kno_tablefns[argtype]->store) &&
             (kno_tablefns[argtype]->get)) {
      int (*storefn)(lispval,lispval,lispval)=kno_tablefns[argtype]->store;
      lispval (*getfn)(lispval,lispval,lispval)=kno_tablefns[argtype]->get;
      DO_CHOICES(each,key) {
        int store_rv=0;
        lispval values=getfn(arg,each,EMPTY);
        lispval to_store;
        if (KNO_ABORTP(values)) {
          KNO_STOP_DO_CHOICES;
          return -1;}
        kno_incref(value);
        CHOICE_ADD(values,value);
        to_store=kno_make_simple_choice(values);
        store_rv=storefn(arg,each,to_store);
        kno_decref(values);
        kno_decref(to_store);
        if (store_rv<0) {
          KNO_STOP_DO_CHOICES;
          return -1;}}
      return 1;}
    else return kno_err(kno_NoMethod,"kno_add",NULL,arg);}
  else if (BAD_TABLEP(arg,argtype,add,"kno_add"))
    return -1;
  else  {
    int (*addfn)(lispval,lispval,lispval)=kno_tablefns[argtype]->add;
    DO_CHOICES(each,key) {
      int retval=addfn(arg,each,value);
      if (retval<0) return retval;}
    return 1;}
}

KNO_EXPORT int kno_drop(lispval arg,lispval key,lispval value)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_drop/table");
  CHECKPTR(key,"kno_drop/key");
  CHECKPTR(value,"kno_drop/value");
  if ( (EMPTYP(arg)) || (EMPTYP(key)) )
    return 0;
  if (KNO_VALID_TYPECODEP(argtype))
    if (USUALLY(kno_tablefns[argtype]!=NULL))
      if (USUALLY(kno_tablefns[argtype]->drop!=NULL))
        if (RARELY((EMPTYP(value)) ||
                       (EMPTYP(key))))
          return 0;
        else if (CHOICEP(key)) {
          int (*dropfn)(lispval,lispval,lispval)=kno_tablefns[argtype]->drop;
          DO_CHOICES(each,key) {
            int retval=dropfn(arg,each,value);
            if (retval<0) return retval;}
          return 1;}
        else return (kno_tablefns[argtype]->drop)(arg,key,value);
      else if ((kno_tablefns[argtype]->store) &&
               (kno_tablefns[argtype]->get))
        if (RARELY((EMPTYP(value))||(EMPTYP(key))))
          return 0;
        else if (VOIDP(value)) {
          int retval;
          int (*storefn)(lispval,lispval,lispval)=kno_tablefns[argtype]->store;
          DO_CHOICES(each,key) {
            retval=storefn(arg,each,EMPTY);
            if (retval<0) return retval;}
          return 1;}
        else {
          int (*storefn)(lispval,lispval,lispval)=kno_tablefns[argtype]->store;
          lispval (*getfn)(lispval,lispval,lispval)=kno_tablefns[argtype]->get;
          DO_CHOICES(each,key) {
            lispval values=getfn(arg,each,EMPTY);
            lispval nvalues;
            int retval;
            if (KNO_ABORTP(values)) return values;
            else nvalues=kno_difference(values,value);
            retval=storefn(arg,each,nvalues);
            kno_decref(values); kno_decref(nvalues);
            if (retval<0) return retval;}
          return 1;}
      else return kno_reterr(kno_NoMethod,CantDrop,NULL,arg);
    else return kno_reterr(NotATable,"kno_drop",NULL,arg);
  else return kno_reterr(kno_BadPtr,"kno_drop",NULL,arg);
}

KNO_EXPORT int kno_test(lispval arg,lispval key,lispval value)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_test/table");
  CHECKPTR(key,"kno_test/key");
  CHECKPTR(value,"kno_test/value");
  if (RARELY((EMPTYP(arg))||(EMPTYP(key))))
    return 0;
  if ((EMPTYP(arg))||(EMPTYP(key)))
    return 0;
  else if (NOT_TABLEP(arg,argtype,"kno_test"))
    return -1;
  else if (USUALLY(kno_tablefns[argtype]->test!=NULL))
    if (CHOICEP(key)) {
      int (*testfn)(lispval,lispval,lispval)=kno_tablefns[argtype]->test;
      DO_CHOICES(each,key)
        if (testfn(arg,each,value)) return 1;
      return 0;}
    else return (kno_tablefns[argtype]->test)(arg,key,value);
  else if (kno_tablefns[argtype]->get) {
    lispval (*getfn)(lispval,lispval,lispval)=kno_tablefns[argtype]->get;
    DO_CHOICES(each,key) {
      lispval values=getfn(arg,each,EMPTY);
      if (VOIDP(value))
        if (EMPTYP(values))
          return 0;
        else {
          kno_decref(values);
          return 1;}
      else if (EMPTYP(values)) {}
      else if (KNO_EQ(value,values)) {
        kno_decref(values); return 1;}
      else if (kno_overlapp(value,values)) {
        kno_decref(values); return 1;}
      else kno_decref(values);}
    return 0;}
  else return kno_reterr(kno_NoMethod,CantTest,NULL,arg);
}

KNO_EXPORT int kno_getsize(lispval arg)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_getsize/table");
  if (kno_tablefns[argtype])
    if (kno_tablefns[argtype]->getsize)
      return (kno_tablefns[argtype]->getsize)(arg);
    else if (kno_tablefns[argtype]->keys) {
      lispval values=(kno_tablefns[argtype]->keys)(arg);
      if (KNO_ABORTP(values))
        return kno_interr(values);
      else {
        int size=KNO_CHOICE_SIZE(values);
        kno_decref(values);
        return size;}}
    else return kno_err(kno_NoMethod,CantGetKeys,NULL,arg);
  else return kno_err(NotATable,"kno_getkeys",NULL,arg);
}

KNO_EXPORT int kno_modifiedp(lispval arg)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_modifiedp/table");
  if (kno_tablefns[argtype])
    if (kno_tablefns[argtype]->modified)
      return (kno_tablefns[argtype]->modified)(arg,-1);
    else return kno_err(kno_NoMethod,CantCheckModified,NULL,arg);
  else return kno_err(NotATable,"kno_modifiedp",NULL,arg);
}

KNO_EXPORT int kno_set_modified(lispval arg,int flag)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_set_modified/table");
  if (kno_tablefns[argtype])
    if (kno_tablefns[argtype]->modified)
      return (kno_tablefns[argtype]->modified)(arg,flag);
    else return kno_err(kno_NoMethod,CantSetModified,NULL,arg);
  else return kno_err(NotATable,"kno_set_modified",NULL,arg);
}

KNO_EXPORT int kno_readonlyp(lispval arg)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_readonlyp/table");
  if (kno_tablefns[argtype])
    if (kno_tablefns[argtype]->readonly)
      return (kno_tablefns[argtype]->readonly)(arg,-1);
    else return kno_err(kno_NoMethod,CantCheckReadOnly,NULL,arg);
  else return kno_err(NotATable,"kno_readonlyp",NULL,arg);
}

KNO_EXPORT int kno_set_readonly(lispval arg,int flag)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_set_readonly/table");
  if (kno_tablefns[argtype])
    if (kno_tablefns[argtype]->readonly)
      return (kno_tablefns[argtype]->readonly)(arg,flag);
    else return kno_err(kno_NoMethod,CantSetReadOnly,NULL,arg);
  else return kno_err(NotATable,"kno_set_readonly",NULL,arg);
}

KNO_EXPORT int kno_finishedp(lispval arg)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_finishedp/table");
  if (kno_tablefns[argtype])
    if (kno_tablefns[argtype]->finished)
      return (kno_tablefns[argtype]->finished)(arg,-1);
    else return kno_err(kno_NoMethod,CantCheckFinished,NULL,arg);
  else return kno_err(NotATable,"kno_finishedp",NULL,arg);
}

KNO_EXPORT int kno_set_finished(lispval arg,int flag)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_set_finished/table");
  if (kno_tablefns[argtype])
    if (kno_tablefns[argtype]->finished)
      return (kno_tablefns[argtype]->finished)(arg,flag);
    else return kno_err(kno_NoMethod,CantSetFinished,NULL,arg);
  else return kno_err(NotATable,"kno_set_finished",NULL,arg);
}

KNO_EXPORT lispval *kno_getkeyvec_n(lispval arg,int *len)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_getkeys/table");
  if (kno_tablefns[argtype])
    if (kno_tablefns[argtype]->keyvec_n)
      return (kno_tablefns[argtype]->keyvec_n)(arg,len);
    else if (kno_tablefns[argtype]->keys) {
      lispval keys = (kno_tablefns[argtype]->keys)(arg);
      if (KNO_EMPTYP(keys)) {
	*len = 0; return NULL;}
      else if (KNO_CHOICEP(keys)) {
	int n_keys = KNO_CHOICE_SIZE(keys);
	lispval *keyvec = u8_alloc_n(n_keys,lispval);
	memcpy(keyvec,KNO_CHOICE_ELTS(keys),n_keys*sizeof(lispval));
	*len = n_keys;
	kno_free_choice((kno_choice)keys);
	return keyvec;}
      else {
	lispval *keyvec = u8_alloc_n(1,lispval);
	keyvec[0] = keys;
	*len = 1;
	return keyvec;}}
    else {
      kno_seterr(kno_NoMethod,CantGetKeys,NULL,arg);
      *len = -1;
      return NULL;}
  else {
    kno_seterr(NotATable,"kno_getkeys",NULL,arg);
    *len = -1;
    return NULL;}
}

KNO_EXPORT lispval kno_getkeys(lispval arg)
{
  kno_lisp_type argtype=KNO_TYPEOF(arg);
  CHECKPTR(arg,"kno_getkeys/table");
  if (kno_tablefns[argtype])
    if (kno_tablefns[argtype]->keys)
      return (kno_tablefns[argtype]->keys)(arg);
    else return kno_err(kno_NoMethod,CantGetKeys,NULL,arg);
  else return kno_err(NotATable,"kno_getkeys",NULL,arg);
}

KNO_EXPORT lispval kno_getvalues(lispval arg)
{
  CHECKPTR(arg,"kno_getvalues/table");
  /* Eventually, these might be kno_tablefns fields */
  if (TYPEP(arg,kno_hashtable_type))
    return kno_hashtable_values(KNO_XHASHTABLE(arg));
  else if (TYPEP(arg,kno_slotmap_type))
    return kno_slotmap_values(KNO_XSLOTMAP(arg));
  else if (CHOICEP(arg)) {
    lispval results=EMPTY;
    DO_CHOICES(table,arg) {
      CHOICE_ADD(results,kno_getvalues(table));}
    return results;}
  else if (PAIRP(arg))
    return kno_refcdr(arg);
  else if (!(TABLEP(arg)))
    return kno_err(NotATable,"kno_getvalues",NULL,arg);
  else {
    lispval results=EMPTY, keys=kno_getkeys(arg);
    DO_CHOICES(key,keys) {
      lispval values=kno_get(arg,key,VOID);
      if (!((VOIDP(values))||(EMPTYP(values)))) {
        CHOICE_ADD(results,values);}}
    kno_decref(keys);
    return results;}
}

KNO_EXPORT lispval kno_getassocs(lispval arg)
{
  CHECKPTR(arg,"kno_getassocs/table");
  /* Eventually, these might be kno_tablefns fields */
  if (TYPEP(arg,kno_hashtable_type))
    return kno_hashtable_assocs(KNO_XHASHTABLE(arg));
  else if (TYPEP(arg,kno_slotmap_type))
    return kno_slotmap_assocs(KNO_XSLOTMAP(arg));
  else if (TYPEP(arg,kno_schemap_type))
    return kno_schemap_assocs(KNO_XSCHEMAP(arg));
  else if (CHOICEP(arg)) {
    lispval results=EMPTY;
    DO_CHOICES(table,arg) {
      CHOICE_ADD(results,kno_getassocs(table));}
    return results;}
  else if (PAIRP(arg))
    return kno_incref(arg);
  else if (!(TABLEP(arg)))
    return kno_err(NotATable,"kno_getvalues",NULL,arg);
  else {
    lispval results=EMPTY, keys=kno_getkeys(arg);
    DO_CHOICES(key,keys) {
      lispval values=kno_get(arg,key,VOID);
      if (!(VOIDP(values))) {
        lispval assoc=kno_init_pair(NULL,key,values);
        kno_incref(key);
	kno_incref(values);
        CHOICE_ADD(results,assoc);}}
    kno_decref(keys);
    return results;}
}

/* Operations over tables */

struct KNO_HASHMAX {lispval max, scope, keys;};

static int hashmaxfn(lispval key,lispval value,void *hmaxp)
{
  struct KNO_HASHMAX *hashmax=hmaxp;
  if ((VOIDP(hashmax->scope)) || (kno_choice_containsp(key,hashmax->scope)))
    if (EMPTYP(value)) {}
    else if (NUMBERP(value))
      if (VOIDP(hashmax->max)) {
        hashmax->max=kno_incref(value); hashmax->keys=kno_incref(key);}
      else {
        int cmp=numcompare(value,hashmax->max);
        if (cmp>0) {
          kno_decref(hashmax->keys); kno_decref(hashmax->max);
          hashmax->keys=kno_incref(key); hashmax->max=kno_incref(value);}
        else if (cmp==0) {
          kno_incref(key);
          CHOICE_ADD(hashmax->keys,key);}}
    else {}
  else {}
  return 0;
}

KNO_EXPORT
lispval kno_hashtable_max(struct KNO_HASHTABLE *h,lispval scope,lispval *maxvalp)
{
  if (EMPTYP(scope)) return EMPTY;
  else {
    struct KNO_HASHMAX hmax;
    hmax.keys=EMPTY; hmax.scope=scope; hmax.max=VOID;
    kno_for_hashtable(h,hashmaxfn,&hmax,1);
    if ((maxvalp) && (NUMBERP(hmax.max)))
      *maxvalp=hmax.max;
    else kno_decref(hmax.max);
    return hmax.keys;}
}

static int hashskimfn_noscope(lispval key,lispval value,void *hmaxp)
{
  struct KNO_HASHMAX *hashmax=hmaxp;
  if (NUMBERP(value)) {
    int cmp=numcompare(value,hashmax->max);
    if (cmp>=0) {
      kno_incref(key);
      CHOICE_ADD(hashmax->keys,key);}}
  return 0;
}

static int hashskimfn(lispval key,lispval value,void *hmaxp)
{
  struct KNO_HASHMAX *hashmax=hmaxp;
  if ((VOIDP(hashmax->scope)) || (kno_choice_containsp(key,hashmax->scope)))
    if (NUMBERP(value)) {
      int cmp=numcompare(value,hashmax->max);
      if (cmp>=0) {
        kno_incref(key);
        CHOICE_ADD(hashmax->keys,key);}}
  return 0;
}

KNO_EXPORT
lispval kno_hashtable_skim(struct KNO_HASHTABLE *h,lispval maxval,lispval scope)
{
  if (EMPTYP(scope))
    return EMPTY;
  else {
    struct KNO_HASHMAX hmax;
    hmax.keys=EMPTY; hmax.scope=scope; hmax.max=maxval;
    if (KNO_VOIDP(scope))
      kno_for_hashtable(h,hashskimfn_noscope,&hmax,1);
    else kno_for_hashtable(h,hashskimfn,&hmax,1);
    return hmax.keys;}
}

static int hashcountfn(lispval key,lispval value,void *vcountp)
{
  long long *countp = (long long *) vcountp;
  int n_values = KNO_CHOICE_SIZE(value);
  *countp += n_values;
  return 0;
}

KNO_EXPORT
long long kno_hashtable_map_size(struct KNO_HASHTABLE *h)
{
  long long count=0;
  kno_for_hashtable(h,hashcountfn,&count,1);
  return count;
}


/* Using pairs as tables functions */

static lispval pairget(lispval pair,lispval key,lispval dflt)
{
  if (KNO_EQUAL(KNO_CAR(pair),key)) return kno_incref(KNO_CDR(pair));
  else return EMPTY;
}
static int pairtest(lispval pair,lispval key,lispval val)
{
  if (KNO_EQUAL(KNO_CAR(pair),key))
    if (VOIDP(val))
      if (EMPTYP(KNO_CDR(pair))) return 0;
      else return 1;
    else if (KNO_EQUAL(KNO_CDR(pair),val)) return 1;
    else return 0;
  else return 0;
}

static int pairgetsize(lispval pair)
{
  return 1;
}

static lispval pairkeys(lispval pair)
{
  return kno_incref(KNO_CAR(pair));
}

/* Describing tables */

KNO_EXPORT void kno_display_table(u8_output out,lispval table,lispval keysarg)
{
  U8_OUTPUT *tmp=u8_open_output_string(1024);
  lispval keys=
    ((VOIDP(keysarg)) ? (kno_getkeys(table)) : (kno_incref(keysarg)));
  DO_CHOICES(key,keys) {
    lispval values=kno_get(table,key,EMPTY);
    tmp->u8_write=tmp->u8_outbuf; *(tmp->u8_outbuf)='\0';
    u8_printf(tmp,"   %q:   %q\n",key,values);
    if (u8_strlen(tmp->u8_outbuf)<80) u8_puts(out,tmp->u8_outbuf);
    else {
      u8_printf(out,"   %q:\n",key);
      {DO_CHOICES(value,values) u8_printf(out,"      %q\n",value);}}
    kno_decref(values);}
  kno_decref(keys);
  u8_close((u8_stream)tmp);
}

/* Table max functions */

KNO_EXPORT lispval kno_table_max(lispval table,lispval scope,lispval *maxvalp)
{
  if (EMPTYP(scope)) return EMPTY;
  else if (HASHTABLEP(table))
    return kno_hashtable_max((kno_hashtable)table,scope,maxvalp);
  else if (SLOTMAPP(table))
    return kno_slotmap_max((kno_slotmap)table,scope,maxvalp);
  else {
    lispval keys=kno_getkeys(table);
    lispval maxval=VOID, results=EMPTY;
    {DO_CHOICES(key,keys)
        if ((VOIDP(scope)) || (kno_overlapp(key,scope))) {
          lispval val=kno_get(table,key,VOID);
          if ((EMPTYP(val)) || (VOIDP(val))) {}
          else if (NUMBERP(val)) {
            if (VOIDP(maxval)) {
              maxval=kno_incref(val); results=kno_incref(key);}
            else {
              int cmp=numcompare(val,maxval);
              if (cmp>0) {
                kno_decref(results); results=kno_incref(key);
                kno_decref(maxval); maxval=kno_incref(val);}
              else if (cmp==0) {
                kno_incref(key);
                CHOICE_ADD(results,key);}}}
          else {}
          kno_decref(val);}}
    kno_decref(keys);
    if ((maxvalp) && (NUMBERP(maxval))) *maxvalp=maxval;
    else kno_decref(maxval);
    return results;}
}

KNO_EXPORT lispval kno_table_skim(lispval table,lispval maxval,lispval scope)
{
  if (EMPTYP(scope))
    return EMPTY;
  else if (EMPTYP(maxval)) {
    /* It's not clear what the right behavior is here. */
    /* It could also all of the keys or the subset of them in *scope*.*/
    return EMPTY;}
  else if (!(KNO_NUMBERP(maxval)))
    return kno_type_error("number","kno_table_skim/maxval",maxval);
  else if (HASHTABLEP(table))
    return kno_hashtable_skim((kno_hashtable)table,maxval,scope);
  else if (SLOTMAPP(table))
    return kno_slotmap_skim((kno_slotmap)table,maxval,scope);
  else {
    lispval keys=kno_getkeys(table);
    lispval results=EMPTY;
    {DO_CHOICES(key,keys)
        if ((VOIDP(scope)) || (kno_overlapp(key,scope))) {
          lispval val=kno_get(table,key,VOID);
          if (NUMBERP(val)) {
            int cmp=kno_numcompare(val,maxval);
            if (cmp>=0) {
              kno_incref(key);
              CHOICE_ADD(results,key);}
            kno_decref(val);}
          else kno_decref(val);}}
    kno_decref(keys);
    return results;}
}

/* Initializations */

void kno_init_tables_c()
{
  u8_register_source_file(_FILEINFO);

  /* SLOTMAP */
  kno_recyclers[kno_slotmap_type]   = recycle_slotmap;
  kno_unparsers[kno_slotmap_type]   = unparse_slotmap;
  kno_copiers[kno_slotmap_type]     = copy_slotmap;
  kno_comparators[kno_slotmap_type] = compare_slotmaps;

  /* SCHEMAP */
  kno_recyclers[kno_schemap_type]   = recycle_schemap;
  kno_unparsers[kno_schemap_type]   = unparse_schemap;
  kno_copiers[kno_schemap_type]     = copy_schemap;
  kno_comparators[kno_schemap_type] = compare_schemaps;

  /* HASHTABLE */
  kno_recyclers[kno_hashtable_type] = recycle_hashtable;
  kno_unparsers[kno_hashtable_type] = unparse_hashtable;
  kno_copiers[kno_hashtable_type]   = copy_hashtable;

  /* HASHSET */
  kno_recyclers[kno_hashset_type]   = recycle_hashset;
  kno_unparsers[kno_hashset_type]   = unparse_hashset;
  kno_copiers[kno_hashset_type]     = copy_hashset;

  /* HASHTABLE table functions */
  kno_tablefns[kno_hashtable_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_hashtable_type]->get=(kno_table_get_fn)kno_hashtable_get;
  kno_tablefns[kno_hashtable_type]->add=(kno_table_add_fn)kno_hashtable_add;
  kno_tablefns[kno_hashtable_type]->drop=(kno_table_drop_fn)kno_hashtable_drop;
  kno_tablefns[kno_hashtable_type]->store=(kno_table_store_fn)kno_hashtable_store;
  kno_tablefns[kno_hashtable_type]->test=(kno_table_test_fn)hashtable_test;
  kno_tablefns[kno_hashtable_type]->getsize=(kno_table_getsize_fn)hashtable_getsize;
  kno_tablefns[kno_hashtable_type]->keys=(kno_table_keys_fn)kno_hashtable_keys;
  kno_tablefns[kno_hashtable_type]->keyvec_n=(kno_table_keyvec_fn)kno_hashtable_keyvec_n;
  kno_tablefns[kno_hashtable_type]->modified=(kno_table_modified_fn)hashtable_modified;
  kno_tablefns[kno_hashtable_type]->readonly=(kno_table_readonly_fn)hashtable_readonly;

  /* SLOTMAP table functions */
  kno_tablefns[kno_slotmap_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_slotmap_type]->get=(kno_table_get_fn)kno_slotmap_get;
  kno_tablefns[kno_slotmap_type]->add=(kno_table_add_fn)kno_slotmap_add;
  kno_tablefns[kno_slotmap_type]->drop=(kno_table_drop_fn)kno_slotmap_drop;
  kno_tablefns[kno_slotmap_type]->store=(kno_table_store_fn)kno_slotmap_store;
  kno_tablefns[kno_slotmap_type]->test=(kno_table_test_fn)kno_slotmap_test;
  kno_tablefns[kno_slotmap_type]->getsize=(kno_table_getsize_fn)slotmap_getsize;
  kno_tablefns[kno_slotmap_type]->keys=(kno_table_keys_fn)kno_slotmap_keys;
  kno_tablefns[kno_slotmap_type]->keyvec_n=(kno_table_keyvec_fn)kno_slotmap_keyvec_n;
  kno_tablefns[kno_slotmap_type]->modified=(kno_table_modified_fn)slotmap_modified;
  kno_tablefns[kno_slotmap_type]->readonly=(kno_table_readonly_fn)slotmap_readonly;

  /* SCHEMAP table functions */
  kno_tablefns[kno_schemap_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_schemap_type]->get=(kno_table_get_fn)kno_schemap_get;
  kno_tablefns[kno_schemap_type]->add=(kno_table_add_fn)kno_schemap_add;
  kno_tablefns[kno_schemap_type]->drop=(kno_table_drop_fn)kno_schemap_drop;
  kno_tablefns[kno_schemap_type]->store=(kno_table_store_fn)kno_schemap_store;
  kno_tablefns[kno_schemap_type]->test=(kno_table_test_fn)kno_schemap_test;
  kno_tablefns[kno_schemap_type]->getsize=(kno_table_getsize_fn)schemap_getsize;
  kno_tablefns[kno_schemap_type]->keys=(kno_table_keys_fn)kno_schemap_keys;
  kno_tablefns[kno_schemap_type]->keyvec_n=(kno_table_keyvec_fn)kno_schemap_keyvec_n;
  kno_tablefns[kno_schemap_type]->modified=(kno_table_modified_fn)schemap_modified;
  kno_tablefns[kno_schemap_type]->readonly=(kno_table_readonly_fn)schemap_readonly;

  /* HASHSET table functions */
  kno_tablefns[kno_hashset_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_hashset_type]->get=(kno_table_get_fn)hashset_get;
  kno_tablefns[kno_hashset_type]->add=(kno_table_add_fn)hashset_store;
  /* This is a no-op because you can't drop a value from a hashset.
     That would just set its value to false. */
  kno_tablefns[kno_hashset_type]->drop=(kno_table_drop_fn)NULL;
  kno_tablefns[kno_hashset_type]->store=(kno_table_store_fn)hashset_store;
  /* This is a no-op because every key has a T/F value in the hashet. */
  kno_tablefns[kno_hashset_type]->test=NULL;
  kno_tablefns[kno_hashset_type]->getsize=(kno_table_getsize_fn)hashset_getsize;
  kno_tablefns[kno_hashset_type]->keys=(kno_table_keys_fn)hashset_elts;
  kno_tablefns[kno_hashset_type]->keyvec_n=(kno_table_keyvec_fn)kno_hashset_vec;
  kno_tablefns[kno_hashset_type]->modified=(kno_table_modified_fn)hashset_modified;

  /* PAIR table functions */
  kno_tablefns[kno_pair_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_pair_type]->get=pairget;
  kno_tablefns[kno_pair_type]->test=pairtest;
  kno_tablefns[kno_pair_type]->keys=pairkeys;
  kno_tablefns[kno_pair_type]->getsize=(kno_table_getsize_fn)pairgetsize;
  kno_tablefns[kno_pair_type]->tablep=NULL;

  /* Table functions for
     OIDS
     CHOICES
     are foud in xtable.c */
  {
    struct KNO_TYPEINFO *e = kno_use_typeinfo(kno_intern("hashtable"));
    e->type_restorefn=restore_hashtable;
  }
}

