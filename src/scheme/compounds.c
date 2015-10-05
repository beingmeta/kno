/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/eval.h"
#include "framerd/ports.h"

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
  return FD_BYTE2DTYPE(compound->n_elts);
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
  if (FD_EMPTY_CHOICEP(x)) return FD_EMPTY_CHOICE;
  else if (FD_CHOICEP(x)) {
    FD_DO_CHOICES(eachx,x)
      if (FD_COMPOUNDP(eachx)) {
        fdtype result=compound_set(eachx,offset,value,tag);
        if (FD_ABORTP(result)) {
          FD_STOP_DO_CHOICES;
          return result;}
        else fd_decref(result);}
      else return fd_type_error("compound","compound_set",eachx);
    return FD_VOID;}
  else {
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
}

static fdtype make_compound(int n,fdtype *args)
{
  struct FD_COMPOUND *compound=
    u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*sizeof(fdtype)));
  int i=1; fdtype *write=&(compound->elt0);
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->tag=fd_incref(args[0]);
  compound->n_elts=n-1; compound->mutable=0; compound->opaque=0;
  while (i<n) {
    fd_incref(args[i]); *write++=args[i]; i++;}
  return FDTYPE_CONS(compound);
}

static fdtype make_opaque_compound(int n,fdtype *args)
{
  struct FD_COMPOUND *compound=
    u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*sizeof(fdtype)));
  int i=1; fdtype *write=&(compound->elt0);
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->tag=fd_incref(args[0]);
  compound->n_elts=n-1; compound->mutable=0;
  compound->opaque=1;
  while (i<n) {
    fd_incref(args[i]); *write++=args[i]; i++;}
  return FDTYPE_CONS(compound);
}

static fdtype make_mutable_compound(int n,fdtype *args)
{
  struct FD_COMPOUND *compound=
    u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*sizeof(fdtype)));
  int i=1; fdtype *write=&(compound->elt0);
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->tag=fd_incref(args[0]); compound->n_elts=n-1; compound->mutable=1;
  fd_init_mutex(&(compound->lock));
  while (i<n) {
    fd_incref(args[i]); *write++=args[i]; i++;}
  return FDTYPE_CONS(compound);
}

static fdtype make_opaque_mutable_compound(int n,fdtype *args)
{
  struct FD_COMPOUND *compound=
    u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*sizeof(fdtype)));
  int i=1; fdtype *write=&(compound->elt0);
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->tag=fd_incref(args[0]);
  compound->n_elts=n-1; compound->mutable=1;
  compound->opaque=1;
  fd_init_mutex(&(compound->lock));
  while (i<n) {
    fd_incref(args[i]); *write++=args[i]; i++;}
  return FDTYPE_CONS(compound);
}

static fdtype vector2compound(fdtype vector,fdtype tag,fdtype mutable,fdtype opaque)
{
  int i=0, n=FD_VECTOR_LENGTH(vector);
  struct FD_COMPOUND *compound=
    u8_malloc(sizeof(struct FD_COMPOUND)+((n-1)*sizeof(fdtype)));
  fdtype *write=&(compound->elt0);
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->tag=fd_incref(tag); compound->n_elts=n;
  if (FD_FALSEP(mutable)) compound->mutable=0;
  else {
    compound->mutable=1;
    fd_init_mutex(&(compound->lock));}
  if (FD_FALSEP(opaque)) compound->opaque=0;
  else {
    compound->opaque=1;}
  while (i<n) {
    fdtype elt=FD_VECTOR_REF(vector,i);
    fd_incref(elt);
    *write++=elt; i++;}
  return FDTYPE_CONS(compound);
}

/* Setting various compound properties */

static fdtype consfn_symbol, stringfn_symbol, tag_symbol;

static fdtype compound_corelen_prim(fdtype tag)
{
  struct FD_COMPOUND_ENTRY *e=fd_lookup_compound(tag);
  if (e) {
    if (e->core_slots<0) return FD_FALSE;
    else return FD_INT(e->core_slots);}
  else return FD_EMPTY_CHOICE;
}

static fdtype compound_set_corelen_prim(fdtype tag,fdtype slots_arg)
{
  int core_slots=FD_FIX2INT(slots_arg);
  fd_declare_compound(tag,FD_VOID,core_slots);
  return fd_incref(tag);
}

static fdtype tag_slotdata(fdtype tag)
{
  struct FD_COMPOUND_ENTRY *e=fd_lookup_compound(tag);
  if ((e)&&(FD_SLOTMAPP(e->data)))
    return fd_incref(e->data);
  else if ((e)&&(!(FD_VOIDP(e->data))))
    return fd_type_error("slotmap","tag_slotdata",e->data);
  else {
    struct FD_KEYVAL *keyvals=u8_alloc_n(1,struct FD_KEYVAL);
    fdtype slotmap=FD_VOID, *slotdata=&slotmap;
    keyvals[0].key=tag_symbol; keyvals[0].key=fd_incref(tag);
    slotmap=fd_init_slotmap(NULL,1,keyvals);
    fd_register_compound(tag,slotdata,NULL);
    return slotmap;}
}

static fdtype compound_metadata_prim(fdtype compound,fdtype field)
{
  fdtype tag=FD_COMPOUND_TAG(compound);
  fdtype slotmap=tag_slotdata(tag);
  fdtype result;
  if (FD_VOIDP(field)) result=fd_deep_copy(slotmap);
  else result=fd_get(slotmap,tag,FD_EMPTY_CHOICE);
  fd_decref(slotmap);
  return result;
}

static fdtype cons_compound(int n,fdtype *args,fd_compound_entry e)
{
  if (e->data) {
    fdtype method=fd_get(e->data,consfn_symbol,FD_VOID);
    if (FD_VOIDP(method)) return FD_VOID;
    else {
      fdtype result=fd_apply(method,n,args);
      fd_decref(method);
      return result;}}
  else {
    int i=0; while (i<n) {fdtype elt=args[i++]; fd_incref(elt);}
    return fd_init_compound_from_elts(NULL,e->tag,1,n,args);}
}

static int stringify_compound(u8_output out,fdtype compound,fd_compound_entry e)
{
  if (e->data) {
    fdtype method=fd_get(e->data,stringfn_symbol,FD_VOID);
    if (FD_VOIDP(method)) return 0;
    else {
      fdtype result=fd_apply(method,1,&compound);
      fd_decref(method);
      if (FD_STRINGP(result)) {
        u8_putn(out,FD_STRDATA(result),FD_STRLEN(result));
        fd_decref(result);
        return 1;}
      else {fd_decref(result); return 0;}}}
  else return 0;
}

static fdtype compound_set_consfn_prim(fdtype tag,fdtype consfn)
{
  if ((FD_SYMBOLP(tag))||(FD_OIDP(tag)))
    if (FD_FALSEP(consfn)) {
      fdtype slotmap=tag_slotdata(tag);
      struct FD_COMPOUND_ENTRY *e=fd_lookup_compound(tag);
      fd_drop(slotmap,consfn_symbol,FD_VOID);
      fd_decref(slotmap);
      e->parser=NULL;
      return FD_VOID;}
    else if (FD_APPLICABLEP(consfn)) {
      fdtype slotmap=tag_slotdata(tag);
      struct FD_COMPOUND_ENTRY *e=fd_lookup_compound(tag);
      fd_store(slotmap,consfn_symbol,consfn);
      fd_decref(slotmap);
      e->parser=cons_compound;
      return FD_VOID;}
    else return fd_type_error("applicable","set_compound_consfn_prim",tag);
  else return fd_type_error("compound tag","set_compound_consfn_prim",tag);
}


static fdtype compound_set_stringfn_prim(fdtype tag,fdtype stringfn)
{
  if ((FD_SYMBOLP(tag))||(FD_OIDP(tag)))
    if (FD_FALSEP(stringfn)) {
      fdtype slotmap=tag_slotdata(tag);
      struct FD_COMPOUND_ENTRY *e=fd_lookup_compound(tag);
      fd_drop(slotmap,stringfn_symbol,FD_VOID);
      fd_decref(slotmap);
      e->unparser=NULL;
      return FD_VOID;}
    else if (FD_APPLICABLEP(stringfn)) {
      fdtype slotmap=tag_slotdata(tag);
      struct FD_COMPOUND_ENTRY *e=fd_lookup_compound(tag);
      fd_store(slotmap,stringfn_symbol,stringfn);
      fd_decref(slotmap);
      e->unparser=stringify_compound;
      return FD_VOID;}
    else return fd_type_error("applicable","set_compound_stringfn_prim",tag);
  else return fd_type_error("compound tag","set_compound_stringfn_prim",tag);
}

/* Initializing common functions */

FD_EXPORT void fd_init_compounds_c()
{
  u8_register_source_file(_FILEINFO);

  consfn_symbol=fd_intern("CONS");
  stringfn_symbol=fd_intern("STRINGIFY");
  tag_symbol=fd_intern("TAG");

  fd_idefn(fd_scheme_module,
           fd_make_cprim2("COMPOUND-TYPE?",compoundp,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND-TAG",compound_tag,1,
                           fd_compound_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND-LENGTH",compound_length,1,
                           fd_compound_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("COMPOUND-REF",compound_ref,2,
                           fd_compound_type,FD_VOID,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprim4x("COMPOUND-SET!",compound_set,3,
                            -1,FD_VOID,-1,FD_VOID,
                            -1,FD_VOID,-1,FD_VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MAKE-COMPOUND",make_compound,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MAKE-MUTABLE-COMPOUND",make_mutable_compound,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MAKE-OPAQUE-COMPOUND",make_opaque_compound,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MAKE-OPAQUE-MUTABLE-COMPOUND",make_opaque_mutable_compound,1)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("VECTOR->COMPOUND",vector2compound,2,
                           fd_vector_type,FD_VOID,-1,FD_VOID,
                           -1,FD_FALSE,-1,FD_FALSE));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("COMPOUND-METATDATA",compound_metadata_prim,1,
                           fd_compound_type,FD_VOID,
                           fd_symbol_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND-CORELEN",compound_corelen_prim,1,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("COMPOUND-SET-CORELEN!",
                           compound_set_corelen_prim,2,
                           -1,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("COMPOUND-SET-CONSFN!",
                           compound_set_consfn_prim,2,
                           -1,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("COMPOUND-SET-STRINGFN!",
                           compound_set_stringfn_prim,2,
                           -1,FD_VOID,-1,FD_VOID));
}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
