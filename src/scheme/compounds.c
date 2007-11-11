/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#include "fdb/dtype.h"
#include "fdb/support.h"
#include "fdb/eval.h"
#include "fdb/ports.h"

static fdtype compoundp(fdtype x,fdtype tag)
{
  if (FD_VOIDP(tag))
    if (FD_COMPOUNDP(x)) return FD_TRUE;
    else return FD_FALSE;
  else if (FD_COMPOUNDP(x))
    if (FD_COMPOUND_TAG(x)==tag)
      return FD_TRUE;
    else return FD_FALSE;
  else return FD_FALSE;
}

static fdtype compound_tag(fdtype x)
{
  return fd_incref(FD_COMPOUND_TAG(x));
}

static fdtype compound_length(fdtype x)
{
  struct FD_COMPOUND *compound=(struct FD_COMPOUND *)x;
  return FD_SHORT2DTYPE(compound->n_elts);
}

static fdtype compound_ref(fdtype x,fdtype offset,fdtype tag)
{
  struct FD_COMPOUND *compound=(struct FD_COMPOUND *)x;
  int off=FD_FIX2INT(offset), len=compound->n_elts;
  if (compound->mutable) fd_lock_struct(compound);
  if (((compound->tag==tag) || (FD_VOIDP(tag))) && (off<len)) {
    fdtype value=*((&(compound->elt0))+off);
    fd_incref(value);
    if (compound->mutable) fd_unlock_struct(compound);
    return value;}
  /* Unlock and figure out the details of the error */
  if (compound->mutable) fd_unlock_struct(compound);
  if ((compound->tag!=tag) && (!(FD_VOIDP(tag)))) {
    u8_string type_string=fd_dtype2string(tag);
    fd_seterr(fd_TypeError,"compound_ref",type_string,x);
    return FD_ERROR_VALUE;}
  else if (!(FD_VOIDP(tag))) {
    u8_string type_string=fd_dtype2string(tag);
    fd_seterr(fd_RangeError,"compound_ref",type_string,off);
    return FD_ERROR_VALUE;}
  else {
    fd_seterr(fd_RangeError,"compound_ref",NULL,off);
    return FD_ERROR_VALUE;}
}

static fdtype compound_set(fdtype x,fdtype offset,fdtype value,fdtype tag)
{
  struct FD_COMPOUND *compound=(struct FD_COMPOUND *)x;
  int off=FD_FIX2INT(offset), len=compound->n_elts;
  if ((compound->mutable) &&
      ((compound->tag==tag) || (FD_VOIDP(tag))) &&
      (off<len)) {
    fdtype *valuep=((&(compound->elt0))+off), old_value;
    fd_lock_struct(compound);
    old_value=*valuep;
    fd_incref(value);
    *valuep=value;
    fd_decref(old_value);
    fd_unlock_struct(compound);
    return FD_VOID;}
  /* Unlock and figure out the details of the error */
  fd_unlock_struct(compound);
  if (compound->mutable==0) {
    fd_seterr(_("Immutable record"),"set_compound",NULL,x);
    return FD_ERROR_VALUE;}
  else if ((compound->tag!=tag) && (!(FD_VOIDP(tag)))) {
    u8_string type_string=fd_dtype2string(tag);
    fd_seterr(fd_TypeError,"compound_ref",type_string,x);
    return FD_ERROR_VALUE;}
  else if (!(FD_VOIDP(tag))) {
    u8_string type_string=fd_dtype2string(tag);
    fd_seterr(fd_RangeError,"compound_ref",type_string,off);
    return FD_ERROR_VALUE;}
  else {
    fd_seterr(fd_RangeError,"compound_ref",NULL,off);
    return FD_ERROR_VALUE;}
}

static fdtype make_compound(int n,fdtype *args)
{
  struct FD_COMPOUND *compound=u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*sizeof(fdtype)));
  int i=1; fdtype *write=&(compound->elt0);
  FD_INIT_CONS(compound,fd_compound_type);
  compound->tag=fd_incref(args[0]); compound->n_elts=n-1; compound->mutable=0;
  while (i<n) {
    fd_incref(args[i]); *write++=args[i]; i++;}
  return FDTYPE_CONS(compound);
}

static fdtype make_mutable_compound(int n,fdtype *args)
{
  struct FD_COMPOUND *compound=u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*sizeof(fdtype)));
  int i=1; fdtype *write=&(compound->elt0);
  FD_INIT_CONS(compound,fd_compound_type);
  compound->tag=fd_incref(args[0]); compound->n_elts=n-1; compound->mutable=1;
  fd_init_mutex(&(compound->lock));
  while (i<n) {
    fd_incref(args[i]); *write++=args[i]; i++;}
  return FDTYPE_CONS(compound);
}

static fdtype vector2compound(fdtype vector,fdtype tag,fdtype mutable)
{
  int i=0, n=FD_VECTOR_LENGTH(vector);
  struct FD_COMPOUND *compound=u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*sizeof(fdtype)));
  fdtype *write=&(compound->elt0);
  FD_INIT_CONS(compound,fd_compound_type);
  compound->tag=fd_incref(tag); compound->n_elts=n;
  if (FD_FALSEP(mutable)) compound->mutable=0;
  else {
    compound->mutable=1;
    fd_init_mutex(&(compound->lock));}
  while (i<n) {
    fdtype elt=FD_VECTOR_REF(vector,i);
    fd_incref(elt);
    *write++=elt; i++;}
  return FDTYPE_CONS(compound);
}

FD_EXPORT void fd_init_compounds_c()
{
  fd_register_source_file(versionid);

  fd_idefn(fd_scheme_module,
	   fd_make_cprim2("COMPOUND-TYPE?",compoundp,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("COMPOUND-TAG",compound_tag,1,
			   fd_compound_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("COMPOUND-LENGTH",compound_tag,1,
			   fd_compound_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim3x("COMPOUND-REF",compound_ref,2,
			   fd_compound_type,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim4x("COMPOUND-SET!",compound_set,3,
			   fd_compound_type,FD_VOID,-1,FD_VOID,
			   -1,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("MAKE-COMPOUND",make_compound,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("MAKE-MUTABLE-COMPOUND",make_mutable_compound,1)));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim3x("VECTOR->COMPOUND",vector2compound,2,
			   fd_vector_type,FD_VOID,-1,FD_VOID,
			   -1,FD_FALSE));
}
