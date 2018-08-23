/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/support.h"
#include "framerd/eval.h"
#include "framerd/ports.h"

static lispval compoundp(lispval x,lispval tag)
{
  if (VOIDP(tag))
    if (FD_COMPOUNDP(x))
      return fd_incref(x);
    else return FD_FALSE;
  else if (FD_COMPOUNDP(x))
    if (FD_AMBIGP(tag)) {
      if (fd_choice_containsp(FD_COMPOUND_TAG(x),tag))
        return fd_incref(x);
      else return FD_FALSE;}
    else if (FD_COMPOUND_TAG(x) == tag)
      return fd_incref(x);
    else return FD_FALSE;
  else return FD_FALSE;
}

static lispval compound_tag(lispval x)
{
  return fd_incref(FD_COMPOUND_TAG(x));
}

static lispval compound_length(lispval x)
{
  struct FD_COMPOUND *compound = (struct FD_COMPOUND *)x;
  return FD_BYTE2DTYPE(compound->compound_length);
}

static lispval compound_mutablep(lispval x)
{
  struct FD_COMPOUND *compound = (struct FD_COMPOUND *)x;
  if (compound->compound_ismutable)
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval compound_opaquep(lispval x)
{
  struct FD_COMPOUND *compound = (struct FD_COMPOUND *)x;
  if (compound->compound_isopaque)
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval compound_ref(lispval x,lispval offset,lispval tag)
{
  struct FD_COMPOUND *compound = (struct FD_COMPOUND *)x;
  if (!(FD_UINTP(offset)))
    return fd_type_error("unsigned int","compound_ref",offset);
  unsigned int off = FIX2INT(offset), len = compound->compound_length;
  if (compound->compound_ismutable) u8_lock_mutex(&(compound->compound_lock));
  if (((compound->compound_typetag == tag) || (VOIDP(tag))) && (off<len)) {
    lispval value = *((&(compound->compound_0))+off);
    fd_incref(value);
    if (compound->compound_ismutable) u8_unlock_mutex(&(compound->compound_lock));
    return value;}
  /* Unlock and figure out the details of the error */
  if (compound->compound_ismutable) u8_unlock_mutex(&(compound->compound_lock));
  if ((compound->compound_typetag!=tag) && (!(VOIDP(tag)))) {
    u8_string type_string = fd_lisp2string(tag);
    fd_seterr(fd_TypeError,"compound_ref",type_string,x);
    return FD_ERROR;}
  else if (!(VOIDP(tag))) {
    u8_string type_string = fd_lisp2string(tag);
    fd_seterr(fd_RangeError,"compound_ref",type_string,off);
    return FD_ERROR;}
  else {
    fd_seterr(fd_RangeError,"compound_ref",NULL,off);
    return FD_ERROR;}
}

static lispval unpack_compound(lispval x,lispval tag)
{
  struct FD_COMPOUND *compound = (struct FD_COMPOUND *)x;
  if ((!(VOIDP(tag)))&&(compound->compound_typetag!=tag)) {
    u8_string type_string = fd_lisp2string(tag);
    fd_seterr(fd_TypeError,"compound_ref",type_string,x);
    return FD_ERROR;}
  else {
    int len = compound->compound_length;
    lispval *elts = &(compound->compound_0), result = VOID;
    if (compound->compound_ismutable)
      u8_lock_mutex(&(compound->compound_lock));
    {
      lispval *scan = elts, *lim = elts+len; while (scan<lim) {
        lispval v = *scan++; fd_incref(v);}}
    result = fd_init_pair(NULL,fd_incref(compound->compound_typetag),
                        fd_make_vector(len,elts));
    if (compound->compound_ismutable) u8_unlock_mutex(&(compound->compound_lock));
    return result;}
}

static lispval compound_set(lispval x,lispval offset,lispval value,lispval tag)
{
  if (EMPTYP(x)) return EMPTY;
  else if (CHOICEP(x)) {
    DO_CHOICES(eachx,x)
      if (FD_COMPOUNDP(eachx)) {
        lispval result = compound_set(eachx,offset,value,tag);
        if (FD_ABORTP(result)) {
          FD_STOP_DO_CHOICES;
          return result;}
        else fd_decref(result);}
      else return fd_type_error("compound","compound_set",eachx);
    return VOID;}
  else {
    struct FD_COMPOUND *compound = (struct FD_COMPOUND *)x;
    if (!(FD_UINTP(offset)))
      return fd_type_error("unsigned int","compound_ref",offset);
    unsigned int off = FIX2INT(offset), len = compound->compound_length;
    if ((compound->compound_ismutable) &&
        ((compound->compound_typetag == tag) || (VOIDP(tag))) &&
        (off<len)) {
      lispval *valuep = ((&(compound->compound_0))+off), old_value;
      u8_lock_mutex(&(compound->compound_lock));
      old_value = *valuep;
      fd_incref(value);
      *valuep = value;
      fd_decref(old_value);
      u8_unlock_mutex(&(compound->compound_lock));
      return VOID;}
    /* Unlock and figure out the details of the error */
    u8_unlock_mutex(&(compound->compound_lock));
    if (compound->compound_ismutable==0) {
      fd_seterr(_("Immutable record"),"set_compound",NULL,x);
      return FD_ERROR;}
    else if ((compound->compound_typetag!=tag) && (!(VOIDP(tag)))) {
      u8_string type_string = fd_lisp2string(tag);
      fd_seterr(fd_TypeError,"compound_ref",type_string,x);
      return FD_ERROR;}
    else if (!(VOIDP(tag))) {
      u8_string type_string = fd_lisp2string(tag);
      fd_seterr(fd_RangeError,"compound_ref",type_string,off);
      return FD_ERROR;}
    else {
      fd_seterr(fd_RangeError,"compound_ref",NULL,off);
      return FD_ERROR;}
  }
}

static lispval compound_modify(lispval x,lispval offset,
                               lispval modifier,lispval value,
                               lispval tag)
{
  if (EMPTYP(x)) return EMPTY;
  else if (CHOICEP(x)) {
    DO_CHOICES(eachx,x)
      if (FD_COMPOUNDP(eachx)) {
        lispval result = compound_modify(eachx,offset,modifier,value,tag);
        if (FD_ABORTP(result)) {
          FD_STOP_DO_CHOICES;
          return result;}
        else fd_decref(result);}
      else return fd_type_error("compound","compound_set",eachx);
    return VOID;}
  else {
    struct FD_COMPOUND *compound = (struct FD_COMPOUND *)x;
    if (!(FD_UINTP(offset)))
      return fd_type_error("unsigned int","compound_ref",offset);
    unsigned int off = FIX2INT(offset), len = compound->compound_length;
    if ((compound->compound_ismutable) &&
        ((compound->compound_typetag == tag) || (VOIDP(tag))) &&
        (off<len)) {
      lispval *valuep = ((&(compound->compound_0))+off), old_value, new_value;
      u8_lock_mutex(&(compound->compound_lock));
      old_value = *valuep;
      if (FD_APPLICABLEP(modifier)) {
        if (FD_VOIDP(value))
          new_value = fd_apply(modifier,1,&old_value);
        else {
          lispval args[2] = { old_value, value };
          new_value = fd_apply(modifier,2,args);}}
      else if (modifier == FDSYM_ADD) {
        new_value = old_value; fd_incref(value);
        CHOICE_ADD(new_value,value);}
      else if (modifier == FDSYM_DROP)
        new_value = fd_difference(old_value,value);
      else if (modifier == FDSYM_STORE)
        new_value = fd_incref(value);
      else if (modifier == FDSYM_PLUS)
        new_value = fd_plus(old_value,value);
      else if (modifier == FDSYM_MINUS)
        new_value = fd_subtract(old_value,value);
      else new_value = fd_err("BadCompoundModifier","compound_modify",NULL,
                              modifier);
      if (FD_ABORTP(new_value)) {
        u8_unlock_mutex(&(compound->compound_lock));
        return new_value;}
      *valuep = new_value;
      fd_decref(old_value);
      u8_unlock_mutex(&(compound->compound_lock));
      return VOID;}
    /* Unlock and figure out the details of the error */
    u8_unlock_mutex(&(compound->compound_lock));
    if (compound->compound_ismutable==0) {
      fd_seterr(_("Immutable record"),"set_compound",NULL,x);
      return FD_ERROR;}
    else if ((compound->compound_typetag!=tag) && (!(VOIDP(tag)))) {
      u8_string type_string = fd_lisp2string(tag);
      fd_seterr(fd_TypeError,"compound_ref",type_string,x);
      return FD_ERROR;}
    else if (!(VOIDP(tag))) {
      u8_string type_string = fd_lisp2string(tag);
      fd_seterr(fd_RangeError,"compound_ref",type_string,off);
      return FD_ERROR;}
    else {
      fd_seterr(fd_RangeError,"compound_ref",NULL,off);
      return FD_ERROR;}
  }
}

static lispval make_compound(int n,lispval *args)
{
  struct FD_COMPOUND *compound=
    u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->compound_typetag = fd_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 0;
  compound->compound_isopaque = 0;
  compound->compound_off = -1;
  while (i<n) {
    fd_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

static lispval make_opaque_compound(int n,lispval *args)
{
  struct FD_COMPOUND *compound=
    u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->compound_typetag = fd_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 0;
  compound->compound_isopaque = 1;
  compound->compound_off = -1;
  while (i<n) {
    fd_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

static lispval make_mutable_compound(int n,lispval *args)
{
  struct FD_COMPOUND *compound=
    u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->compound_typetag = fd_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 1;
  compound->compound_off = -1;
  u8_init_mutex(&(compound->compound_lock));
  while (i<n) {
    fd_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

static lispval make_opaque_mutable_compound(int n,lispval *args)
{
  struct FD_COMPOUND *compound=
    u8_malloc(sizeof(struct FD_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->compound_typetag = fd_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 1;
  compound->compound_isopaque = 1;
  compound->compound_off = -1;
  u8_init_mutex(&(compound->compound_lock));
  while (i<n) {
    fd_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

static lispval seq2compound(lispval seq,lispval tag,
                            lispval mutable,lispval opaque,
                            lispval offset)
{
  lispval *read; int len;
  if (FD_VECTORP(seq)) {
    len = VEC_LEN(seq);
    read = FD_VECTOR_ELTS(seq);}
  else read = fd_seq_elts(seq,&len);
  if ( (read == NULL) && (len < 0) )
    return FD_ERROR_VALUE;
  int extra_elts = (len) ? (len-1) : (0);
  struct FD_COMPOUND *compound =
    u8_malloc(sizeof(struct FD_COMPOUND)+((extra_elts)*LISPVAL_LEN));
  FD_INIT_FRESH_CONS(compound,fd_compound_type);
  compound->compound_typetag = fd_incref(tag);
  compound->compound_length = len;
  if ( (len==0) || (FALSEP(mutable)) )
    compound->compound_ismutable = 0;
  else {
    compound->compound_ismutable = 1;
    u8_init_mutex(&(compound->compound_lock));}
  if (FALSEP(opaque)) {
    compound->compound_isopaque = 0;
    compound->compound_off = 0;}
  else {
    compound->compound_isopaque = 1;
    compound->compound_off = -1;}
  lispval *write = &(compound->compound_0);
  int i = 0, n = len; while (i<n) {
    lispval elt = read[i++];
    fd_incref(elt);
    *write++=elt;}
  if (FD_FALSEP(offset))
    compound->compound_off = -1;
  else {
    if (FD_FIXNUMP(offset)) {
      long long off = FD_FIX2INT(offset);
      if ( (off>=0) && (off<128) && (off < len) )
        compound->compound_off = off;
      else {
        fd_seterr("BadCompoundVectorOffset","vector2compound",NULL,offset);
        return FD_ERROR_VALUE;}}
    else compound->compound_off = 0;}
  if (FD_VECTORP(seq))
    return LISP_CONS(compound);
  else {
    fd_decref_vec(read,len);
    u8_free(read);
    return LISP_CONS(compound);}
}

/* Setting various compound properties */

static lispval consfn_symbol, stringfn_symbol, tag_symbol;

static lispval compound_corelen_prim(lispval tag)
{
  struct FD_COMPOUND_TYPEINFO *e = fd_lookup_compound(tag);
  if (e) {
    if (e->compound_corelen<0) return FD_FALSE;
    else return FD_INT(e->compound_corelen);}
  else return EMPTY;
}

static lispval compound_set_corelen_prim(lispval tag,lispval slots_arg)
{
  if (!(FD_UINTP(slots_arg)))
    return fd_type_error("unsigned int",
                         "compound_set_corelen_prim",
                         slots_arg);
  unsigned int core_slots = FIX2INT(slots_arg);
  fd_declare_compound(tag,VOID,core_slots);
  return fd_incref(tag);
}

static lispval tag_slotdata(lispval tag)
{
  struct FD_COMPOUND_TYPEINFO *e = fd_lookup_compound(tag);
  if ((e)&&(SLOTMAPP(e->compound_metadata)))
    return fd_incref(e->compound_metadata);
  else if ((e)&&(!(VOIDP(e->compound_metadata))))
    return fd_type_error("slotmap","tag_slotdata",e->compound_metadata);
  else {
    struct FD_KEYVAL *keyvals = u8_alloc_n(1,struct FD_KEYVAL);
    lispval slotmap = VOID, *slotdata = &slotmap;
    keyvals[0].kv_key = tag_symbol; keyvals[0].kv_key = fd_incref(tag);
    slotmap = fd_init_slotmap(NULL,1,keyvals);
    fd_register_compound(tag,slotdata,NULL);
    return slotmap;}
}

static lispval compound_metadata_prim(lispval compound,lispval field)
{
  lispval tag = FD_COMPOUND_TAG(compound);
  lispval slotmap = tag_slotdata(tag);
  lispval result;
  if (VOIDP(field)) result = fd_deep_copy(slotmap);
  else result = fd_get(slotmap,tag,EMPTY);
  fd_decref(slotmap);
  return result;
}

static lispval cons_compound(int n,lispval *args,fd_compound_typeinfo e)
{
  if (e->compound_metadata) {
    lispval method = fd_get(e->compound_metadata,FDSYM_CONS,VOID);
    if (VOIDP(method)) return VOID;
    else {
      lispval result = fd_apply(method,n,args);
      fd_decref(method);
      return result;}}
  else {
    return fd_init_compound_from_elts(NULL,e->compound_typetag,
                                      FD_COMPOUND_INCREF,
                                      n,args);}
}

static int stringify_compound(u8_output out,lispval compound,fd_compound_typeinfo e)
{
  if (e->compound_metadata) {
    lispval method = fd_get(e->compound_metadata,stringfn_symbol,VOID);
    if (VOIDP(method)) return 0;
    else {
      lispval result = fd_apply(method,1,&compound);
      fd_decref(method);
      if (STRINGP(result)) {
        u8_putn(out,CSTRING(result),STRLEN(result));
        fd_decref(result);
        return 1;}
      else {fd_decref(result); return 0;}}}
  else return 0;
}

static lispval compound_set_consfn_prim(lispval tag,lispval consfn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(consfn)) {
      lispval slotmap = tag_slotdata(tag);
      struct FD_COMPOUND_TYPEINFO *e = fd_lookup_compound(tag);
      fd_drop(slotmap,FDSYM_CONS,VOID);
      fd_decref(slotmap);
      e->compound_parser = NULL;
      return VOID;}
    else if (FD_APPLICABLEP(consfn)) {
      lispval slotmap = tag_slotdata(tag);
      struct FD_COMPOUND_TYPEINFO *e = fd_lookup_compound(tag);
      fd_store(slotmap,FDSYM_CONS,consfn);
      fd_decref(slotmap);
      e->compound_parser = cons_compound;
      return VOID;}
    else return fd_type_error("applicable","set_compound_consfn_prim",tag);
  else return fd_type_error("compound tag","set_compound_consfn_prim",tag);
}


static lispval compound_set_stringfn_prim(lispval tag,lispval stringfn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(stringfn)) {
      lispval slotmap = tag_slotdata(tag);
      struct FD_COMPOUND_TYPEINFO *e = fd_lookup_compound(tag);
      fd_drop(slotmap,stringfn_symbol,VOID);
      fd_decref(slotmap);
      e->compound_unparser = NULL;
      return VOID;}
    else if (FD_APPLICABLEP(stringfn)) {
      lispval slotmap = tag_slotdata(tag);
      struct FD_COMPOUND_TYPEINFO *e = fd_lookup_compound(tag);
      fd_store(slotmap,stringfn_symbol,stringfn);
      fd_decref(slotmap);
      e->compound_unparser = stringify_compound;
      return VOID;}
    else return fd_type_error("applicable","set_compound_stringfn_prim",tag);
  else return fd_type_error("compound tag","set_compound_stringfn_prim",tag);
}

/* Initializing common functions */

FD_EXPORT void fd_init_compoundfns_c()
{
  u8_register_source_file(_FILEINFO);

  consfn_symbol = fd_intern("CONS");
  stringfn_symbol = fd_intern("STRINGIFY");

  fd_idefn2(fd_scheme_module,"COMPOUND?",compoundp,1,
            "`(COMPOUND? *arg* [*tags*])` returns *arg* if it is "
            "a compound and (when specified) if it's typetag "
            "is any of *tags*.",
            -1,FD_VOID,-1,FD_VOID);
  fd_defalias(fd_scheme_module,"COMPOUND-TYPE?","COMPOUND?");
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND-TAG",compound_tag,1,
                           fd_compound_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND-LENGTH",compound_length,1,
                           fd_compound_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND-MUTABLE?",compound_mutablep,1,
                           fd_compound_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND-OPAQUE?",compound_opaquep,1,
                           fd_compound_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("COMPOUND-REF",compound_ref,2,
                           fd_compound_type,VOID,-1,VOID,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprim4x("COMPOUND-SET!",compound_set,3,
                            -1,VOID,-1,VOID,
                            -1,VOID,-1,VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprim5x("COMPOUND-MODIFY!",compound_modify,4,
                            -1,VOID,-1,VOID,
                            -1,VOID,-1,VOID,
                            -1,VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MAKE-COMPOUND",make_compound,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MAKE-MUTABLE-COMPOUND",
                                         make_mutable_compound,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MAKE-OPAQUE-COMPOUND",
                                         make_opaque_compound,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("MAKE-OPAQUE-MUTABLE-COMPOUND",
                                         make_opaque_mutable_compound,1)));
  fd_idefn5(fd_scheme_module,"SEQUENCE->COMPOUND",seq2compound,2,
            "`(SEQUENCE->COMPOUND *seq* *tag* *mutable* *opaque* *reserve*)` "
            "creates a compound object out of *seq*, tagged with *tag*. "
            "If *reserve* is #f, the returned compound won't be a sequence",
            -1,VOID,-1,VOID,
            -1,FD_FALSE,-1,FD_FALSE,-1,FD_TRUE);
  fd_idefn5(fd_scheme_module,"VECTOR->COMPOUND",seq2compound,2,
            "`(VECTOR->COMPOUND *vec* *tag* *mutable* *opaque* *reserve*)` "
            "creates a compound object out of *vec*, tagged with *tag*. "
            "If *reserve* is #f, the returned compound won't be a sequence",
            fd_vector_type,VOID,-1,VOID,
            -1,FD_FALSE,-1,FD_FALSE,-1,FD_TRUE);
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("UNPACK-COMPOUND",unpack_compound,1,
                           fd_compound_type,VOID,-1,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("COMPOUND-METATDATA",compound_metadata_prim,1,
                           fd_compound_type,VOID,
                           fd_symbol_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND-CORELEN",compound_corelen_prim,1,
                           -1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("COMPOUND-SET-CORELEN!",
                           compound_set_corelen_prim,2,
                           -1,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("COMPOUND-SET-CONSFN!",
                           compound_set_consfn_prim,2,
                           -1,VOID,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("COMPOUND-SET-STRINGFN!",
                           compound_set_stringfn_prim,2,
                           -1,VOID,-1,VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
