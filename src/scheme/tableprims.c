/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

static fdtype tablep(fdtype arg)
{
  if (FD_TABLEP(arg)) return FD_TRUE; else return FD_FALSE;
}

static fdtype haskeysp(fdtype arg)
{
  if (FD_TABLEP(arg)) {
    fd_ptr_type argtype=FD_PTR_TYPE(arg);
    if ((fd_tablefns[argtype])->keys)
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static fdtype slotmapp(fdtype x)
{
  if (FD_SLOTMAPP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype schemapp(fdtype x)
{
  if (FD_SCHEMAPP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype hashtablep(fdtype x)
{
  if (FD_HASHTABLEP(x)) return FD_TRUE;
  else return FD_FALSE;
}

FD_EXPORT fdtype make_hashset(fdtype arg)
{
  struct FD_HASHSET *h=u8_alloc(struct FD_HASHSET);
  if (FD_VOIDP(arg))
    fd_init_hashset(h,17,FD_MALLOCD_CONS);
  else fd_init_hashset(h,FD_FIX2INT(arg),FD_MALLOCD_CONS);
  FD_INIT_CONS(h,fd_hashset_type);
  return FDTYPE_CONS(h);
}

static fdtype make_hashtable(fdtype size)
{
  if (FD_FIXNUMP(size))
    return fd_make_hashtable(NULL,FD_FIX2INT(size));
  else return fd_make_hashtable(NULL,0);
}

static fdtype pick_hashtable_size(fdtype count_arg)
{
  int count=FD_FIX2INT(count_arg);
  int size=fd_get_hashtable_size(count);
  return FD_INT(size);
}

static fdtype reset_hashtable(fdtype table,fdtype n_slots)
{
  fd_reset_hashtable((fd_hashtable)table,FD_FIX2INT(n_slots),1);
  return FD_VOID;
}

static fdtype static_hashtable(fdtype table)
{
  struct FD_HASHTABLE *ht=(fd_hashtable)table;
  fd_write_lock_struct(ht);
  ht->fd_uselock=0;
  fd_rw_unlock_struct(ht);
  return fd_incref(table);
}

static fdtype hash_lisp_prim(fdtype x)
{
  int val=fd_hash_lisp(x);
  return FD_INT(val);
}

static fdtype lispget(fdtype table,fdtype key,fdtype dflt)
{
  if (FD_VOIDP(dflt))
    return fd_get(table,key,FD_EMPTY_CHOICE);
  else return fd_get(table,key,dflt);
}

static fdtype lispgetif(fdtype table,fdtype key,fdtype dflt)
{
  if (FD_FALSEP(table)) return fd_incref(key);
  else if (FD_VOIDP(dflt))
    return fd_get(table,key,FD_EMPTY_CHOICE);
  else return fd_get(table,key,dflt);
}

static fdtype lisptryget(fdtype table,fdtype key,fdtype dflt)
{
  if ((FD_FALSEP(table)) || (FD_EMPTY_CHOICEP(table)))
    if (FD_VOIDP(dflt))
      return fd_incref(key);
    else return fd_incref(dflt);
  else if (FD_CHOICEP(table)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(etable,table) {
      FD_DO_CHOICES(ekey,key) {
        fdtype v=((FD_VOIDP(dflt)) ? (fd_get(etable,ekey,ekey)) :
                  (fd_get(etable,ekey,dflt)));
        FD_ADD_TO_CHOICE(results,v);}}
    if (FD_EMPTY_CHOICEP(results))
      if (FD_VOIDP(dflt))
        return fd_incref(key);
      else return fd_incref(dflt);
    else return results;}
  else if (FD_CHOICEP(key)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(ekey,key) {
      fdtype v=((FD_VOIDP(dflt)) ? (fd_get(table,ekey,ekey)) :
                (fd_get(table,ekey,dflt)));
      FD_ADD_TO_CHOICE(results,v);}
    if (FD_EMPTY_CHOICEP(results))
      if (FD_VOIDP(dflt))
        return fd_incref(key);
      else return fd_incref(dflt);
    else return results;}
  else if (FD_VOIDP(dflt))
    return fd_get(table,key,key);
  else return fd_get(table,key,dflt);
}

static fdtype lispadd(fdtype table,fdtype key,fdtype val)
{
  if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(key)) return FD_VOID;
  else if (fd_add(table,key,val)<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}
static fdtype lispdrop(fdtype table,fdtype key,fdtype val)
{
  if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  if (FD_EMPTY_CHOICEP(key)) return FD_VOID;
  else if (fd_drop(table,key,val)<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}
static fdtype lispstore(fdtype table,fdtype key,fdtype val)
{
  if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(key)) return FD_VOID;
  else if (FD_QCHOICEP(val)) {
    struct FD_QCHOICE *qch=FD_XQCHOICE(val);
    if (fd_store(table,key,qch->fd_choiceval)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;}
  else if (fd_store(table,key,val)<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}
static fdtype lisptest(fdtype table,fdtype key,fdtype val)
{
  if (FD_EMPTY_CHOICEP(table)) return FD_FALSE;
  else if (FD_EMPTY_CHOICEP(key)) return FD_FALSE;
  else if (FD_EMPTY_CHOICEP(val)) return FD_FALSE;
  else {
    int retval=fd_test(table,key,val);
    if (retval<0) return FD_ERROR_VALUE;
    else if (retval) return FD_TRUE;
    else return FD_FALSE;}
}

static fdtype lisp_pick_keys(fdtype table,fdtype howmany_arg)
{
  if (!(FD_TABLEP(table)))
    return fd_type_error(_("table"),"lisp_pick_key",table);
  else {
    fdtype x=fd_getkeys(table);
    fdtype normal=fd_make_simple_choice(x);
    int n=FD_CHOICE_SIZE(normal), howmany=FD_FIX2INT(howmany_arg);
    if (!(FD_CHOICEP(normal))) return normal;
    if (n<=howmany) return normal;
    else if (howmany==1) {
      int i=u8_random(n);
      const fdtype *data=FD_CHOICE_DATA(normal);
      fdtype result=data[i];
      fd_incref(result); fd_decref(normal);
      return result;}
    else if (n) {
      struct FD_HASHSET h;
      const fdtype *data=FD_CHOICE_DATA(normal);
      int j=0; fd_init_hashset(&h,n*3,FD_STACK_CONS);
      while (j<howmany) {
        int i=u8_random(n);
        if (fd_hashset_mod(&h,data[i],1)) j++;}
      fd_decref(normal);
      return fd_hashset_elts(&h,1);}
    else return FD_EMPTY_CHOICE;}
}

/* Support for some iterated operations */

typedef fdtype (*reduceop)(fdtype,fdtype);

/* Various table operations */

static fdtype hashtable_increment(fdtype table,fdtype keys,fdtype increment)
{
  if (FD_EMPTY_CHOICEP(increment)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_VOIDP(increment)) increment=FD_INT(1);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_increment,n_elts,elts,increment)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_increment,keys,increment)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else return fd_type_error("table","hashtable_increment",table);
}

static fdtype table_increment(fdtype table,fdtype keys,fdtype increment)
{
  if (FD_EMPTY_CHOICEP(increment)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_VOIDP(increment)) increment=FD_INT(1);
  else if (!(FD_NUMBERP(increment)))
    return fd_type_error("number","table_increment",increment);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_increment,n_elts,elts,increment)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_increment,keys,increment)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur))
        fd_store(table,key,increment);
      else if ((FD_FIXNUMP(cur)) && (FD_FIXNUMP(increment))) {
        int sum=FD_FIX2INT(cur)+FD_FIX2INT(increment);
        fdtype lsum=FD_INT(sum);
        fd_store(table,key,lsum);
        fd_decref(lsum);}
      else if (FD_NUMBERP(cur)) {
        fdtype lsum=fd_plus(cur,increment);
        fd_store(table,key,lsum);
        fd_decref(lsum); fd_decref(cur);}
      else return fd_type_error("number","table_increment",cur);}
    return FD_VOID;}
  else return fd_type_error("table","table_increment",table);
}

static fdtype hashtable_increment_existing
  (fdtype table,fdtype key,fdtype increment)
{
  if (FD_EMPTY_CHOICEP(increment)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(key)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_VOIDP(increment)) increment=FD_INT(1);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(key)) {
      fdtype keys=fd_make_simple_choice(key);
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys(FD_XHASHTABLE(table),
                                fd_table_increment_if_present,
                                n_elts,elts,increment)<0) {
        fd_decref(keys); return FD_ERROR_VALUE;}
      else {fd_decref(keys); return FD_VOID;}}
    else if (FD_EMPTY_CHOICEP(key))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),
                             fd_table_increment_if_present,
                             key,increment)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else return fd_type_error("table","hashtable_increment_existing",table);
}

static fdtype table_increment_existing
                (fdtype table,fdtype keys,fdtype increment)
{
  if (FD_EMPTY_CHOICEP(increment)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_VOIDP(increment)) increment=FD_INT(1);
  else if (!(FD_NUMBERP(increment)))
    return fd_type_error("number","table_increment_existing",increment);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_increment,n_elts,elts,increment)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_increment,keys,increment)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur)) {}
      else if ((FD_FIXNUMP(cur)) && (FD_FIXNUMP(increment))) {
        int sum=FD_FIX2INT(cur)+FD_FIX2INT(increment);
        fdtype lsum=FD_INT(sum);
        fd_store(table,key,lsum);
        fd_decref(lsum);}
      else if (FD_NUMBERP(cur)) {
        fdtype lsum=fd_plus(cur,increment);
        fd_store(table,key,lsum);
        fd_decref(lsum); fd_decref(cur);}
      else return fd_type_error("number","table_increment_existing",cur);}
    return FD_VOID;}
  else return fd_type_error("table","table_increment_existing",table);
}

static fdtype hashtable_multiply(fdtype table,fdtype key,fdtype factor)
{
  if (FD_EMPTY_CHOICEP(factor)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(key)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_VOIDP(factor)) factor=FD_INT(2);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(key)) {
      fdtype keys=fd_make_simple_choice(key);
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys(FD_XHASHTABLE(table),
                                fd_table_multiply,
                                n_elts,elts,factor)<0) {
        fd_decref(keys); return FD_ERROR_VALUE;}
      else {fd_decref(keys); return FD_VOID;}}
    else if (FD_EMPTY_CHOICEP(key))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),
                             fd_table_multiply,
                             key,factor)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else return fd_type_error("table","hashtable_multiply",table);
}

static fdtype table_multiply(fdtype table,fdtype keys,fdtype factor)
{
  if (FD_EMPTY_CHOICEP(factor)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_VOIDP(factor)) factor=FD_INT(1);
  else if (!(FD_NUMBERP(factor)))
    return fd_type_error("number","table_multiply",factor);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_multiply,n_elts,elts,factor)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_multiply,keys,factor)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur))
        fd_store(table,key,factor);
      else if (FD_NUMBERP(cur)) {
        fdtype lsum=fd_multiply(cur,factor);
        fd_store(table,key,lsum);
        fd_decref(lsum); fd_decref(cur);}
      else return fd_type_error("number","table_multiply",cur);}
    return FD_VOID;}
  else return fd_type_error("table","table_multiply",table);
}

static fdtype hashtable_multiply_existing
  (fdtype table,fdtype key,fdtype factor)
{
  if (FD_EMPTY_CHOICEP(factor)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(key)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_VOIDP(factor)) factor=FD_INT(2);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(key)) {
      fdtype keys=fd_make_simple_choice(key);
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys(FD_XHASHTABLE(table),
                                fd_table_multiply_if_present,
                                n_elts,elts,factor)<0) {
        fd_decref(keys); return FD_ERROR_VALUE;}
      else {fd_decref(keys); return FD_VOID;}}
    else if (FD_EMPTY_CHOICEP(key))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),
                             fd_table_multiply_if_present,
                             key,factor)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else return fd_type_error("table","hashtable_multiply_existing",table);
}

static fdtype table_multiply_existing(fdtype table,fdtype keys,fdtype factor)
{
  if (FD_EMPTY_CHOICEP(factor)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_VOIDP(factor)) factor=FD_INT(1);
  else if (!(FD_NUMBERP(factor)))
    return fd_type_error("number","table_multiply_existing",factor);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_multiply_if_present,n_elts,elts,factor)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_multiply_if_present,keys,factor)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur)) {}
      else if (FD_NUMBERP(cur)) {
        fdtype lsum=fd_multiply(cur,factor);
        fd_store(table,key,lsum);
        fd_decref(lsum); fd_decref(cur);}
      else return fd_type_error("number","table_multiply_existing",cur);}
    return FD_VOID;}
  else return fd_type_error("table","table_multiply_existing",table);
}

/* Table MAXIMIZE
   Stores a value in a table if the current value is either empty or
   less than the new value. */

static fdtype table_maximize(fdtype table,fdtype keys,fdtype maxval)
{
  if (FD_EMPTY_CHOICEP(maxval)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (!(FD_NUMBERP(maxval)))
    return fd_type_error("number","table_maximize",maxval);
  else if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_maximize,n_elts,elts,maxval)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_maximize,keys,maxval)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur))
        fd_store(table,key,maxval);
      else if (FD_NUMBERP(cur)) {
        if (fd_numcompare(maxval,cur)>0) {
          fd_store(table,key,maxval);
          fd_decref(cur);}
        else {fd_decref(cur);}}
      else return fd_type_error("number","table_maximize",cur);}
    return FD_VOID;}
  else return fd_type_error("table","table_maximize",table);
}

static fdtype table_maximize_existing(fdtype table,fdtype keys,fdtype maxval)
{
  if (FD_EMPTY_CHOICEP(maxval)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (!(FD_NUMBERP(maxval)))
    return fd_type_error("number","table_maximize_existing",maxval);
  else if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_maximize_if_present,n_elts,elts,maxval)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_maximize_if_present,keys,maxval)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur)) {}
      else if (FD_NUMBERP(cur)) {
        if (fd_numcompare(maxval,cur)>0) {
          fd_store(table,key,maxval);
          fd_decref(cur);}
        else {fd_decref(cur);}}
      else return fd_type_error("number","table_maximize",cur);}
    return FD_VOID;}
  else return fd_type_error("table","table_maximize",table);
}

static fdtype hashtable_maximize(fdtype table,fdtype keys,fdtype maxval)
{
  if (FD_EMPTY_CHOICEP(maxval)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_maximize,n_elts,elts,maxval)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_maximize,keys,maxval)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else return fd_type_error("table","hashtable_maximize",table);
}

static fdtype hashtable_maximize_existing(fdtype table,fdtype keys,fdtype maxval)
{
  if (FD_EMPTY_CHOICEP(maxval)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_maximize_if_present,n_elts,elts,maxval)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_maximize_if_present,keys,maxval)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else return fd_type_error("table","hashtable_maximize_existing",table);
}

/* Table MINIMIZE
   Stores a value in a table if the current value is either empty or
   less than the new value. */

static fdtype table_minimize(fdtype table,fdtype keys,fdtype minval)
{
  if (FD_EMPTY_CHOICEP(minval)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (!(FD_NUMBERP(minval)))
    return fd_type_error("number","table_minimize",minval);
  else if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_minimize,n_elts,elts,minval)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_minimize,keys,minval)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur))
        fd_store(table,key,minval);
      else if (FD_NUMBERP(cur)) {
        if (fd_numcompare(minval,cur)<0) {
          fd_store(table,key,minval);
          fd_decref(cur);}
        else {fd_decref(cur);}}
      else return fd_type_error("number","table_minimize",cur);}
    return FD_VOID;}
  else return fd_type_error("table","table_minimize",table);
}

static fdtype table_minimize_existing(fdtype table,fdtype keys,fdtype minval)
{
  if (FD_EMPTY_CHOICEP(minval)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (!(FD_NUMBERP(minval)))
    return fd_type_error("number","table_minimize_existing",minval);
  else if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_minimize_if_present,n_elts,elts,minval)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_minimize_if_present,keys,minval)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur)) {}
      else if (FD_NUMBERP(cur)) {
        if (fd_numcompare(minval,cur)<0) {
          fd_store(table,key,minval);
          fd_decref(cur);}
        else {fd_decref(cur);}}
      else return fd_type_error("number","table_minimize_existing",cur);}
    return FD_VOID;}
  else return fd_type_error("table","table_minimize",table);
}

static fdtype hashtable_minimize(fdtype table,fdtype keys,fdtype minval)
{
  if (FD_EMPTY_CHOICEP(minval)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_minimize,n_elts,elts,minval)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_minimize,keys,minval)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else return fd_type_error("table","hashtable_minimize",table);
}


static fdtype hashtable_minimize_existing(fdtype table,fdtype keys,fdtype minval)
{
  if (FD_EMPTY_CHOICEP(minval)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(keys)) return FD_VOID;
  else if (FD_EMPTY_CHOICEP(table)) return FD_VOID;
  else {}
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
          (FD_XHASHTABLE(table),fd_table_minimize_if_present,n_elts,elts,minval)<0) {
        return FD_ERROR_VALUE;}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_minimize_if_present,keys,minval)<0)
      return FD_ERROR_VALUE;
    else return FD_VOID;
  else return fd_type_error("table","hashtable_minimize_existing",table);
}

/* Getting max values out of tables, especially hashtables. */

static fdtype hashtable_max(fdtype table,fdtype scope)
{
  if (FD_EMPTY_CHOICEP(scope))
    return FD_EMPTY_CHOICE;
  else return fd_hashtable_max(FD_XHASHTABLE(table),scope,NULL);
}

static fdtype hashtable_skim(fdtype table,fdtype threshold,fdtype scope)
{
  return fd_hashtable_skim(FD_XHASHTABLE(table),threshold,scope);
}

static fdtype hashtable_buckets(fdtype table)
{
  fd_hashtable h=FD_GET_CONS(table,fd_hashtable_type,fd_hashtable);
  return FD_INT(h->fd_n_buckets);
}

static fdtype table_size(fdtype table)
{
  int size=fd_getsize(table);
  if (size<0) return FD_ERROR_VALUE;
  else return FD_INT(size);
}

static fdtype table_modifiedp(fdtype table)
{
  int ismod=fd_modifiedp(table);
  if (ismod == 0)
    return FD_FALSE;
  else if (ismod > 0)
    return FD_TRUE;
  else return FD_ERROR_VALUE;
}

static fdtype table_set_modified(fdtype table,fdtype flag_arg)
{
  int flag=((FD_FALSEP(flag_arg))||(FD_ZEROP(flag_arg)))?(0):(1);
  int retval=fd_set_modified(table,flag);
  if (retval == 0)
    return FD_FALSE;
  else if (retval > 0)
    return FD_TRUE;
  else return FD_ERROR_VALUE;
}

static fdtype table_max(fdtype tables,fdtype scope)
{
  if (FD_EMPTY_CHOICEP(scope)) return scope;
  else {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(table,tables)
      if (FD_TABLEP(table)) {
        fdtype result=fd_table_max(table,scope,NULL);
        FD_ADD_TO_CHOICE(results,result);}
      else {
        fd_decref(results);
        return fd_type_error(_("table"),"table_max",table);}
    return results;}
}

static fdtype table_maxval(fdtype tables,fdtype scope)
{
  if (FD_EMPTY_CHOICEP(scope)) return scope;
  else {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(table,tables)
      if (FD_TABLEP(table)) {
        fdtype maxval=FD_EMPTY_CHOICE;
        fdtype result=fd_table_max(table,scope,&maxval);
        FD_ADD_TO_CHOICE(results,maxval);
        fd_decref(result);}
      else {
        fd_decref(results);
        return fd_type_error(_("table"),"table_maxval",table);}
    return results;}
}

static fdtype table_skim(fdtype tables,fdtype maxval,fdtype scope)
{
  if (FD_EMPTY_CHOICEP(scope)) return scope;
  else {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(table,tables)
      if (FD_TABLEP(table)) {
        fdtype result=fd_table_skim(table,maxval,scope);
        FD_ADD_TO_CHOICE(results,result);}
      else {
        fd_decref(results);
        return fd_type_error(_("table"),"table_skim",table);}
    return results;}
}

static fdtype table_map_size(fdtype table)
{
  if (FD_PRIM_TYPEP(table,fd_hashtable_type)) {
    struct FD_HASHTABLE *ht = (struct FD_HASHTABLE *) table;
    long long n_values=fd_hashtable_map_size(ht);
    return FD_INT(n_values);}
  else if (FD_PRIM_TYPEP(table,fd_hashset_type)) {
    struct FD_HASHSET *hs = (struct FD_HASHSET *) table;
    return FD_INT(hs->fd_n_keys);}
  else if (FD_TABLEP(table)) {
    fdtype keys=fd_getkeys(table);
    long long count=0;
    FD_DO_CHOICES(key,keys) {
      fdtype v=fd_get(table,key,FD_VOID);
      if (!(FD_VOIDP(v))) {
        int size=FD_CHOICE_SIZE(v);
        count += size;}
      fd_decref(v);}
    fd_decref(keys);
    return FD_INT(count);}
  else return fd_type_error(_("table"),"table_map_size",table);
}

/* Mapping into tables */

static fdtype map2table(fdtype keys,fdtype fn,fdtype hashp)
{
  int n_keys=FD_CHOICE_SIZE(keys);
  fdtype table;
  if (FD_FALSEP(hashp)) table=fd_empty_slotmap();
  else if (FD_TRUEP(hashp)) table=fd_make_hashtable(NULL,n_keys*2);
  else if (FD_FIXNUMP(hashp))
    if (n_keys>(FD_FIX2INT(hashp))) table=fd_make_hashtable(NULL,n_keys*2);
    else table=fd_empty_slotmap();
  else if (n_keys>8) table=fd_make_hashtable(NULL,n_keys*2);
  else table=fd_empty_slotmap();
  if ((FD_SYMBOLP(fn)) || (FD_OIDP(fn))) {
    FD_DO_CHOICES(k,keys) {
      fdtype v=((FD_OIDP(k)) ? (fd_frame_get(k,fn)) : (fd_get(k,fn,FD_EMPTY_CHOICE)));
      fd_add(table,k,v);
      fd_decref(v);}}
  else if (FD_APPLICABLEP(fn)) {
    FD_DO_CHOICES(k,keys) {
      fdtype v=fd_apply(fn,1,&k);
      fd_add(table,k,v);
      fd_decref(v);}}
  else if (FD_TABLEP(fn)) {
    FD_DO_CHOICES(k,keys) {
      fdtype v=fd_get(fn,k,FD_EMPTY_CHOICE);
      fd_add(table,k,v);
      fd_decref(v);}}
  else {
    fd_decref(table);
    return fd_type_error("map","map2table",fn);}
  return table;
}

/* Hashset operations */

static fdtype hashsetp(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_hashset_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype hashsetget(fdtype hs,fdtype key)
{
  int retval=fd_hashset_get((fd_hashset)hs,key);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype hashsetadd(fdtype hs,fdtype key)
{
  if ((FD_CHOICEP(hs))||(FD_ACHOICEP(hs))) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(h,hs) {
      fdtype value=hashsetadd(h,key);
      FD_ADD_TO_CHOICE(results,value);}
    return results;}
  else {
    int retval=fd_hashset_add((fd_hashset)hs,key);
    if (retval<0) return FD_ERROR_VALUE;
    else if (retval) return FD_INT(retval);
    else return FD_FALSE;}
}

static fdtype hashsetplus(fdtype hs,fdtype values)
{
  fd_hashset_add((fd_hashset)hs,values);
  fd_incref(hs);
  return  hs;
}

static fdtype hashsetdrop(fdtype hs,fdtype key)
{
  int retval=fd_hashset_drop((fd_hashset)hs,key);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype hashsettest(fdtype hs,fdtype key)
{
  FD_DO_CHOICES(hset,hs) {
    if (!(FD_PRIM_TYPEP(hset,fd_hashset_type)))
      return fd_type_error(_("hashset"),"hashsettest",hset);
    else if (fd_hashset_get((fd_hashset)hs,key)) {
      FD_STOP_DO_CHOICES;
      return FD_TRUE;}}
  return FD_FALSE;
}

static fdtype hashsetelts(fdtype hs)
{
  return fd_hashset_elts((fd_hashset)hs,0);
}

static fdtype choice2hashset(fdtype arg)
{
  struct FD_HASHSET *h=u8_alloc(struct FD_HASHSET);
  int size=3*FD_CHOICE_SIZE(arg);
  fd_init_hashset(h,((size<17) ? (17) : (size)),FD_MALLOCD_CONS);
  fd_hashset_add(h,arg);
  return FDTYPE_CONS(h);
}

/* Initialization code */

FD_EXPORT void fd_init_tablefns_c()
{
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_xscheme_module,fd_make_cprim1("TABLE?",tablep,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("HASKEYS?",haskeysp,1));

  fd_idefn(fd_xscheme_module,fd_make_cprim1("SLOTMAP?",slotmapp,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("SCHEMAP?",schemapp,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("HASHTABLE?",hashtablep,1));

  fd_idefn(fd_xscheme_module,
           fd_make_cprim1x("MAKE-HASHSET",make_hashset,0,
                           fd_fixnum_type,FD_VOID));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("MAKE-HASHTABLE",make_hashtable,0));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("STATIC-HASHTABLE",static_hashtable,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("PICK-HASHTABLE-SIZE",pick_hashtable_size,1,
                           fd_fixnum_type,FD_VOID));


  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("RESET-HASHTABLE!",reset_hashtable,1,
                           fd_hashtable_type,FD_VOID,
                           fd_fixnum_type,FD_INT(-1)));
  /* Note that GET and TEST are actually DB functions which do inference */
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("%GET",lispget,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("%TEST",lisptest,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("GETIF",lispgetif,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TRYGET",lisptryget,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("ADD!",lispadd,3)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("DROP!",lispdrop,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("STORE!",lispstore,3)));
  fd_idefn(fd_scheme_module,fd_make_cprim1("GETKEYS",fd_getkeys,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("GETVALUES",fd_getvalues,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("GETASSOCS",fd_getassocs,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("PICK-KEYS",lisp_pick_keys,1,
                                            -1,FD_VOID,fd_fixnum_type,FD_INT(1)));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TABLE-SIZE",table_size,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TABLE-MODIFIED?",table_modifiedp,1));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("TABLE-MAX",table_max,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("TABLE-MAXVAL",table_maxval,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-SKIM",table_skim,2)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("TABLE-MAP-SIZE",table_map_size,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1
           ("PLIST->TABLE",fd_plist_to_slotmap,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1
           ("ALIST->TABLE",fd_alist_to_slotmap,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1
           ("BLIST->TABLE",fd_blist_to_slotmap,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1
           ("BINDINGS->TABLE",fd_blist_to_slotmap,1));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-INCREMENT!",
                                         table_increment,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-INCREMENT-EXISTING!",
                                         table_increment_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MULTIPLY!",
                                         table_multiply,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MULTIPLY-EXISTING!",
                                         table_multiply_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MAXIMIZE!",
                                         table_maximize,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MAXIMIZE-EXISTING!",
                                         table_maximize_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MINIMIZE!",
                                         table_minimize,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("TABLE-MINIMIZE-EXISTING!",
                                         table_minimize_existing,2)));


  fd_idefn(fd_scheme_module,fd_make_cprim1("HASH-LISP",hash_lisp_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-INCREMENT!",
                                         hashtable_increment,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-INCREMENT-EXISTING!",
                                         hashtable_increment_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MULTIPLY!",
                                         hashtable_multiply,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MULTIPLY-EXISTING!",
                                         hashtable_multiply_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MAXIMIZE!",
                                         hashtable_maximize,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MAXIMIZE-EXISTING!",
                                         hashtable_maximize_existing,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MINIMIZE!",
                                         hashtable_minimize,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("HASHTABLE-MINIMIZE-EXISTING!",
                                         hashtable_minimize_existing,2)));


  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("HASHTABLE-MAX",hashtable_max,1,
                           fd_hashtable_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("HASHTABLE-SKIM",hashtable_skim,1,
                           fd_hashtable_type,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("HASHTABLE-BUCKETS",hashtable_buckets,1,
                           fd_hashtable_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3("MAP->TABLE",map2table,2)));


  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprim1("CHOICE->HASHSET",choice2hashset,1)));
  fd_idefn(fd_scheme_module,fd_make_cprim1("HASHSET?",hashsetp,1));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("HASHSET-TEST",hashsettest,2)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("HASHSET-GET",hashsetget,2,
                           fd_hashset_type,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2x("HASHSET-ADD!",hashsetadd,2,
                                          fd_hashset_type,FD_VOID,
                                          -1,FD_VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("HASHSET-DROP!",hashsetdrop,2,
                           fd_hashset_type,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2x("HASHSET+",hashsetplus,2,
                                          fd_hashset_type,FD_VOID,
                                          -1,FD_VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("HASHSET-ELTS",hashsetelts,1,
                           fd_hashset_type,FD_VOID));


}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
