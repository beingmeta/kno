/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/frames.h"
#include "fdb/numbers.h"

static fdtype tablep(fdtype arg)
{
  if (FD_TABLEP(arg)) return FD_TRUE; else return FD_FALSE;
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
  return FD_INT2DTYPE(size);
}

static fdtype reset_hashtable(fdtype table,fdtype n_slots)
{
  fd_reset_hashtable((fd_hashtable)table,FD_FIX2INT(n_slots),1);
  return FD_VOID;
}

static fdtype static_hashtable(fdtype table)
{
  struct FD_HASHTABLE *ht=(fd_hashtable)table;
  fd_lock_mutex(&(ht->lock));
  ht->modified=-1;
  fd_unlock_mutex(&(ht->lock));
  return fd_incref(table);
}

static fdtype hash_lisp_prim(fdtype x)
{
  int val=fd_hash_lisp(x);
  return FD_INT2DTYPE(val);
}

static fdtype lispget(fdtype f,fdtype slotid)
{
  return fd_get(f,slotid,FD_EMPTY_CHOICE);
}

static fdtype lispadd(fdtype f,fdtype slotid,fdtype val)
{
  if (FD_EMPTY_CHOICEP(f)) return FD_VOID;
  else if (fd_add(f,slotid,val)<0) return fd_erreify();
  else return FD_VOID;
}
static fdtype lispdrop(fdtype f,fdtype slotid,fdtype val)
{
  if (FD_EMPTY_CHOICEP(f)) return FD_VOID;
  if (FD_EMPTY_CHOICEP(slotid)) return FD_VOID;
  else if (fd_drop(f,slotid,val)<0) return fd_erreify();
  else return FD_VOID;
}
static fdtype lispstore(fdtype f,fdtype slotid,fdtype val)
{
  if (FD_EMPTY_CHOICEP(f)) return FD_VOID;
  else if (fd_store(f,slotid,val)<0) return fd_erreify();
  else return FD_VOID;
}
static fdtype lisptest(fdtype f,fdtype slotid,fdtype val)
{
  if (FD_EMPTY_CHOICEP(f)) return FD_FALSE;
  else if (FD_EMPTY_CHOICEP(slotid)) return FD_FALSE;
  else if (FD_EMPTY_CHOICEP(val)) return FD_FALSE;
  else {
    int retval=fd_test(f,slotid,val);
    if (retval<0) return fd_erreify();
    else if (retval) return FD_TRUE;
    else return FD_FALSE;}
}

/* Various table operations */

static fdtype hashtable_increment(fdtype table,fdtype keys,fdtype increment)
{
  if (FD_VOIDP(increment)) increment=FD_INT2DTYPE(1);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
	  (FD_XHASHTABLE(table),fd_table_increment,n_elts,elts,increment)<0) {
	return fd_erreify();}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_increment,keys,increment)<0)
      return fd_erreify();
    else return FD_VOID;
  else return fd_type_error("table","hashtable_increment",table);
}

static fdtype table_increment(fdtype table,fdtype keys,fdtype increment)
{
  if (FD_VOIDP(increment)) increment=FD_INT2DTYPE(1);
  else if (!(FD_NUMBERP(increment)))
    return fd_type_error("number","table_increment",increment);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
	  (FD_XHASHTABLE(table),fd_table_increment,n_elts,elts,increment)<0) {
	return fd_erreify();}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_increment,keys,increment)<0)
      return fd_erreify();
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur))
	fd_store(table,key,increment);
      else if ((FD_FIXNUMP(cur)) && (FD_FIXNUMP(increment))) {
	int sum=FD_FIX2INT(cur)+FD_FIX2INT(increment);
	fdtype lsum=FD_INT2DTYPE(sum);
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
  if (FD_VOIDP(increment)) increment=FD_INT2DTYPE(1);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(key)) {
      fdtype keys=fd_make_simple_choice(key);
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys(FD_XHASHTABLE(table),
				fd_table_increment_if_present,
				n_elts,elts,increment)<0) {
	fd_decref(keys); return fd_erreify();}
      else {fd_decref(keys); return FD_VOID;}}
    else if (FD_EMPTY_CHOICEP(key))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),
			     fd_table_increment_if_present,
			     key,increment)<0)
      return fd_erreify();
    else return FD_VOID;
  else return fd_type_error("table","hashtable_increment_existing",table);
}

static fdtype table_increment_existing(fdtype table,fdtype keys,fdtype increment)
{
  if (FD_VOIDP(increment)) increment=FD_INT2DTYPE(1);
  else if (!(FD_NUMBERP(increment)))
    return fd_type_error("number","table_increment_existing",increment);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
	  (FD_XHASHTABLE(table),fd_table_increment,n_elts,elts,increment)<0) {
	return fd_erreify();}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_increment,keys,increment)<0)
      return fd_erreify();
    else return FD_VOID;
  else if (FD_TABLEP(table)) {
    FD_DO_CHOICES(key,keys) {
      fdtype cur=fd_get(table,key,FD_VOID);
      if (FD_VOIDP(cur)) {}
      else if ((FD_FIXNUMP(cur)) && (FD_FIXNUMP(increment))) {
	int sum=FD_FIX2INT(cur)+FD_FIX2INT(increment);
	fdtype lsum=FD_INT2DTYPE(sum);
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
  if (FD_VOIDP(factor)) factor=FD_INT2DTYPE(2);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(key)) {
      fdtype keys=fd_make_simple_choice(key);
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys(FD_XHASHTABLE(table),
				fd_table_multiply,
				n_elts,elts,factor)<0) {
	fd_decref(keys); return fd_erreify();}
      else {fd_decref(keys); return FD_VOID;}}
    else if (FD_EMPTY_CHOICEP(key))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),
			     fd_table_multiply,
			     key,factor)<0)
      return fd_erreify();
    else return FD_VOID;
  else return fd_type_error("table","hashtable_multiply",table);
}

static fdtype table_multiply(fdtype table,fdtype keys,fdtype factor)
{
  if (FD_VOIDP(factor)) factor=FD_INT2DTYPE(1);
  else if (!(FD_NUMBERP(factor)))
    return fd_type_error("number","table_multiply",factor);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
	  (FD_XHASHTABLE(table),fd_table_multiply,n_elts,elts,factor)<0) {
	return fd_erreify();}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_multiply,keys,factor)<0)
      return fd_erreify();
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
  if (FD_VOIDP(factor)) factor=FD_INT2DTYPE(2);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(key)) {
      fdtype keys=fd_make_simple_choice(key);
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys(FD_XHASHTABLE(table),
				fd_table_multiply_if_present,
				n_elts,elts,factor)<0) {
	fd_decref(keys); return fd_erreify();}
      else {fd_decref(keys); return FD_VOID;}}
    else if (FD_EMPTY_CHOICEP(key))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),
			     fd_table_multiply_if_present,
			     key,factor)<0)
      return fd_erreify();
    else return FD_VOID;
  else return fd_type_error("table","hashtable_multiply_existing",table);
}

static fdtype table_multiply_existing(fdtype table,fdtype keys,fdtype factor)
{
  if (FD_VOIDP(factor)) factor=FD_INT2DTYPE(1);
  else if (!(FD_NUMBERP(factor)))
    return fd_type_error("number","table_multiply_existing",factor);
  if (FD_HASHTABLEP(table))
    if (FD_CHOICEP(keys)) {
      const fdtype *elts=FD_CHOICE_DATA(keys);
      int n_elts=FD_CHOICE_SIZE(keys);
      if (fd_hashtable_iterkeys
	  (FD_XHASHTABLE(table),fd_table_multiply_if_present,n_elts,elts,factor)<0) {
	return fd_erreify();}
      else return FD_VOID;}
    else if (FD_EMPTY_CHOICEP(keys))
      return FD_VOID;
    else if (fd_hashtable_op(FD_XHASHTABLE(table),fd_table_multiply_if_present,keys,factor)<0)
      return fd_erreify();
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
  return FD_INT2DTYPE(h->n_slots);
}

static fdtype table_size(fdtype table)
{
  int size=fd_getsize(table);
  if (size<0) return fd_erreify();
  else return FD_INT2DTYPE(size);
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
  
/* Mapping into tables */

static fdtype map2table(fdtype keys,fdtype fn,fdtype hashp)
{
  int n_keys=FD_CHOICE_SIZE(keys);
  fdtype table;
  if (FD_FALSEP(hashp)) table=fd_init_slotmap(NULL,0,NULL);
  else if (FD_TRUEP(hashp)) table=fd_make_hashtable(NULL,n_keys*2);
  else if (FD_FIXNUMP(hashp))
    if (n_keys>(FD_FIX2INT(hashp))) table=fd_make_hashtable(NULL,n_keys*2);
    else table=fd_init_slotmap(NULL,0,NULL);
  else if (n_keys>8) table=fd_make_hashtable(NULL,n_keys*2);
  else table=fd_init_slotmap(NULL,0,NULL);
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

static fdtype hashsetget(fdtype hs,fdtype key)
{
  int retval=fd_hashset_get((fd_hashset)hs,key);
  if (retval<0) return fd_erreify();
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype hashsetadd(fdtype hs,fdtype key)
{
  int retval=fd_hashset_add((fd_hashset)hs,key);
  if (retval<0) return fd_erreify();
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype hashsetdrop(fdtype hs,fdtype key)
{
  int retval=fd_hashset_drop((fd_hashset)hs,key);
  if (retval<0) return fd_erreify();
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype hashsetelts(fdtype hs)
{
  return fd_hashset_elts((fd_hashset)hs,0);
}

static fdtype choice2hashset(fdtype arg)
{
  struct FD_HASHSET *h=u8_malloc(sizeof(struct FD_HASHSET));
  int size=3*FD_CHOICE_SIZE(arg);
  fd_init_hashset(h,((size<17) ? (17) : (size)));
  {FD_DO_CHOICES(elt,arg) fd_hashset_add(h,elt);}
  return FDTYPE_CONS(h);
}

/* Initialization code */

FD_EXPORT void fd_init_tablefns_c()
{
  fd_register_source_file(versionid);

  fd_idefn(fd_xscheme_module,fd_make_cprim1("TABLE?",tablep,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("SLOTMAP?",slotmapp,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("SCHEMAP?",schemapp,1));

  fd_idefn(fd_xscheme_module,fd_make_cprim0("MAKE-HASHSET",fd_make_hashset,0));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("MAKE-HASHTABLE",make_hashtable,0));
  fd_idefn(fd_xscheme_module,fd_make_cprim1("STATIC-HASHTABLE",static_hashtable,1));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("PICK-HASHTABLE-SIZE",pick_hashtable_size,1,
			   fd_fixnum_type,FD_VOID));


  fd_idefn(fd_xscheme_module,
	   fd_make_cprim2x("RESET-HASHTABLE!",reset_hashtable,1,
			   fd_hashtable_type,FD_VOID,
			   fd_fixnum_type,FD_INT2DTYPE(-1)));
  /* Note that GET and TEST are actually DB functions which do inference */
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("%GET",lispget,2)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("%TEST",lisptest,2)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("ADD!",lispadd,3)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("DROP!",lispdrop,2)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("STORE!",lispstore,3)));
  fd_idefn(fd_scheme_module,fd_make_cprim1("GETKEYS",fd_getkeys,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TABLE-SIZE",table_size,1));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("TABLE-MAX",table_max,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("TABLE-MAXVAL",table_maxval,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("TABLE-SKIM",table_skim,2)));


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
	   fd_make_cprim2x("HASHTABLE-MAX",hashtable_max,1,
			   fd_hashtable_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("HASHTABLE-MAX",hashtable_max,1,
			   fd_hashtable_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("HASHTABLE-BUCKETS",hashtable_buckets,1,
			   fd_hashtable_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim3("MAP->TABLE",map2table,2)));


  fd_idefn(fd_scheme_module,
	   fd_make_ndprim
	   (fd_make_cprim1("CHOICE->HASHSET",choice2hashset,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("HASHSET-GET",hashsetget,2,
			   fd_hashset_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("HASHSET-ADD!",hashsetadd,2,
			   fd_hashset_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("HASHSET-DROP!",hashsetdrop,2,
			   fd_hashset_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("HASHSET-ELTS",hashsetelts,1,
			   fd_hashset_type,FD_VOID));


}


/* The CVS log for this file
   $Log: tablefns.c,v $
   Revision 1.30  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.29  2005/12/26 21:15:02  haase
   Added fd_getsize and TABLE-SIZE

   Revision 1.28  2005/12/17 21:51:30  haase
   Added HASH-LISP primitive

   Revision 1.27  2005/09/04 19:50:45  haase
   Added DROP with 2 args (means drop all values)

   Revision 1.26  2005/08/25 20:34:34  haase
   Added generic table skim functions

   Revision 1.25  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.24  2005/08/01 01:08:11  haase
   Fixed name of TABLE-MAX and TABLE-MAXVAL

   Revision 1.23  2005/07/31 22:02:13  haase
   Added slotmap and generic max procedures

   Revision 1.22  2005/07/26 21:23:33  haase
   Caught empty choice calls to hashtable increment/multiply and added hashtable-multiply

   Revision 1.21  2005/06/28 17:38:39  haase
   Added ENVIRONMENT? and SYMBOL-BOUND? primitives

   Revision 1.20  2005/06/21 00:01:00  haase
   Added earlier gcc compatability

   Revision 1.19  2005/06/15 02:37:17  haase
   Added CHOICE->HASHSET

   Revision 1.18  2005/06/06 15:46:28  haase
   Make SCHEME table primitives prune (rather than err) when the first argument is the empty choice

   Revision 1.17  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.16  2005/05/16 18:50:57  haase
   Added hashtable-multiply! primitive

   Revision 1.15  2005/05/12 21:39:35  haase
   Made hashtable-max take a scope to select against

   Revision 1.14  2005/05/12 21:16:57  haase
   Added hashtable-max and hashtable-skim primitives in C and Scheme

   Revision 1.13  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.12  2005/04/11 00:39:23  haase
   Added hashset primitives

   Revision 1.11  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.10  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.9  2005/03/01 19:38:16  haase
   Made SCHEME tableops pass in choice args

   Revision 1.8  2005/02/13 23:55:41  haase
   whitespace changes

   Revision 1.7  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
