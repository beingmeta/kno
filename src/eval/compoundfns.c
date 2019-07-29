/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/support.h"
#include "kno/eval.h"
#include "kno/ports.h"

#include "kno/cprims.h"

static lispval choice_prim, choice_fcnid, push_prim, push_fcnid;
static lispval plus_prim, plus_fcnid, minus_prim, minus_fcnid, minus_prim;
static lispval plusone_prim, plusone_fcnid, minusone_prim, minusone_fcnid;
static lispval difference_prim, difference_fcnid;

DEFPRIM2("compound?",compoundp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
         "`(COMPOUND? *obj* *tag*)` "
         "returns #f if *obj* is a compound and (when *tag* "
         "is provided) has the typetag *tag*.",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval compoundp(lispval x,lispval tag)
{
  if (KNO_COMPOUNDP(x)) {
    if (KNO_VOIDP(tag))
      return KNO_TRUE;
    else if (KNO_COMPOUND_TYPEP(x,tag))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

/* TODO: This might be faster if it were non deterministic */
DEFPRIM2("pick-compounds",pick_compounds,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
         "`(pick-compounds *arg* [*tags*])` "
         "returns *arg* if it is a compound and (when "
         "specified) if it's typetag is any of *tags*.",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval pick_compounds(lispval candidates,lispval tags)
{
  if (VOIDP(tags)) {
    lispval result = KNO_EMPTY; int changes = 0;
    {KNO_DO_CHOICES(candidate,candidates)
        if (KNO_COMPOUNDP(candidate)) {
          kno_incref(candidate);
          KNO_ADD_TO_CHOICE(result,candidate);}
        else changes = 1;}
    if (changes == 0) {
      kno_decref(result);
      return kno_incref(candidates);}
    else return kno_simplify_choice(result);}
  else {
    lispval result = KNO_EMPTY; int changes = 0;
    {KNO_DO_CHOICES(candidate,candidates) {
        if (KNO_COMPOUNDP(candidate)) {
          if (kno_overlapp(KNO_COMPOUND_TAG(candidate),tags)) {
            kno_incref(candidate);
            KNO_ADD_TO_CHOICE(result,candidate);
            changes = 1;}
          else changes = 1;}
        else changes = 1;}}
    if (changes == 0) {
      kno_decref(result);
      return kno_incref(candidates);}
    else return kno_simplify_choice(result);}
}

DEFPRIM1("compound-tag",compound_tag,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(COMPOUND-TAG *arg0*)` **undocumented**",
         kno_compound_type,KNO_VOID);
static lispval compound_tag(lispval x)
{
  return kno_incref(KNO_COMPOUND_TAG(x));
}

DEFPRIM1("compound-length",compound_length,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(COMPOUND-LENGTH *arg0*)` **undocumented**",
         kno_compound_type,KNO_VOID);
static lispval compound_length(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  return KNO_BYTE2LISP(compound->compound_length);
}

DEFPRIM1("compound-mutable?",compound_mutablep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(COMPOUND-MUTABLE? *arg0*)` **undocumented**",
         kno_compound_type,KNO_VOID);
static lispval compound_mutablep(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if (compound->compound_ismutable)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("compound-opaque?",compound_opaquep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
         "`(COMPOUND-OPAQUE? *arg0*)` **undocumented**",
         kno_compound_type,KNO_VOID);
static lispval compound_opaquep(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if (compound->compound_isopaque)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM3("compound-ref",compound_ref,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
         "`(COMPOUND-REF *arg0* *arg1* [*arg2*])` **undocumented**",
         kno_compound_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID);
static lispval compound_ref(lispval x,lispval offset,lispval tag)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if (!(KNO_UINTP(offset)))
    return kno_type_error("unsigned int","compound_ref",offset);
  unsigned int off = FIX2INT(offset), len = compound->compound_length;
  if (compound->compound_ismutable)
    u8_read_lock(&(compound->compound_rwlock));
  if (((compound->typetag == tag) || (VOIDP(tag))) && (off<len)) {
    lispval value = *((&(compound->compound_0))+off);
    kno_incref(value);
    if (compound->compound_ismutable)
      u8_rw_unlock(&(compound->compound_rwlock));
    return kno_simplify_choice(value);}
  /* Unlock and figure out the details of the error */
  if (compound->compound_ismutable)
    u8_rw_unlock(&(compound->compound_rwlock));
  if ((compound->typetag!=tag) && (!(VOIDP(tag)))) {
    u8_string type_string = kno_lisp2string(tag);
    kno_seterr(kno_TypeError,"compound_ref",type_string,x);
    u8_free(type_string);
    return KNO_ERROR;}
  else if (!(VOIDP(tag))) {
    u8_string type_string = kno_lisp2string(tag);
    kno_seterr(kno_RangeError,"compound_ref",type_string,off);
    u8_free(type_string);
    return KNO_ERROR;}
  else {
    kno_seterr(kno_RangeError,"compound_ref",NULL,off);
    return KNO_ERROR;}
}

DEFPRIM2("unpack-compound",unpack_compound,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
         "`(UNPACK-COMPOUND *arg0* [*arg1*])` **undocumented**",
         kno_compound_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval unpack_compound(lispval x,lispval tag)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if ((!(VOIDP(tag)))&&(compound->typetag!=tag)) {
    u8_string type_string = kno_lisp2string(tag);
    kno_seterr(kno_TypeError,"compound_ref",type_string,x);
    u8_free(type_string);
    return KNO_ERROR;}
  else {
    int len = compound->compound_length;
    lispval *elts = &(compound->compound_0), result = VOID;
    if (compound->compound_ismutable)
      u8_read_lock(&(compound->compound_rwlock));
    {
      lispval *scan = elts, *lim = elts+len; while (scan<lim) {
        lispval v = *scan++; kno_incref(v);}}
    result = kno_init_pair(NULL,kno_incref(compound->typetag),
                           kno_make_vector(len,elts));
    if (compound->compound_ismutable)
      u8_rw_unlock(&(compound->compound_rwlock));
    return result;}
}

DEFPRIM4("compound-set!",compound_set,
	 KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3)|KNO_NDCALL,
         "`(COMPOUND-SET! *arg0* *arg1* *arg2* [*arg3*])` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval compound_set(lispval x,lispval offset,lispval value,lispval tag)
{
  if (EMPTYP(x)) return EMPTY;
  else if (CHOICEP(x)) {
    DO_CHOICES(eachx,x)
      if (KNO_COMPOUNDP(eachx)) {
        lispval result = compound_set(eachx,offset,value,tag);
        if (KNO_ABORTP(result)) {
          KNO_STOP_DO_CHOICES;
          return result;}
        else kno_decref(result);}
      else return kno_type_error("compound","compound_set",eachx);
    return VOID;}
  else {
    struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
    if (!(KNO_UINTP(offset)))
      return kno_type_error("unsigned int","compound_ref",offset);
    unsigned int off = FIX2INT(offset), len = compound->compound_length;
    if ((compound->compound_ismutable) &&
	((compound->typetag == tag) || (VOIDP(tag))) &&
	(off<len)) {
      lispval *valuep = ((&(compound->compound_0))+off), old_value;
      u8_write_lock(&(compound->compound_rwlock));
      old_value = *valuep;
      kno_incref(value);
      *valuep = value;
      kno_decref(old_value);
      u8_rw_unlock(&(compound->compound_rwlock));
      return VOID;}
    /* Unlock and figure out the details of the error */
    if (compound->compound_ismutable==0) {
      kno_seterr(_("Immutable record"),"set_compound",NULL,x);
      return KNO_ERROR;}
    else if ((compound->typetag!=tag) && (!(VOIDP(tag)))) {
      u8_string type_string = kno_lisp2string(tag);
      kno_seterr(kno_TypeError,"compound_ref",type_string,x);
      u8_free(type_string);
      return KNO_ERROR;}
    else if (!(VOIDP(tag))) {
      u8_string type_string = kno_lisp2string(tag);
      kno_seterr(kno_RangeError,"compound_ref",type_string,off);
      u8_free(type_string);
      return KNO_ERROR;}
    else {
      kno_seterr(kno_RangeError,"compound_ref",NULL,off);
      return KNO_ERROR;}
  }
}

static lispval apply_modifier(lispval modifier,lispval old_value,lispval value)
{
  if ( (modifier == KNOSYM_ADD) || 
       (modifier == choice_fcnid) ||
       (modifier == choice_prim) ) {
    lispval new_value = old_value;
    kno_incref(value);
    CHOICE_ADD(new_value,value);
    if ( (new_value == old_value) ||
	 (KNO_PRECHOICEP(old_value)) )
      return kno_incref(new_value);
    else return new_value;}
  else if ( (modifier == KNOSYM_DROP) ||
	    (modifier == difference_fcnid) ||
	    (modifier == difference_prim) )
    return kno_difference(old_value,value);
  else if ( (modifier == KNOSYM_PLUS) ||
	    (modifier == plus_fcnid) ||
	    (modifier == plus_prim) )
    return kno_plus(old_value,value);
  else if  ( (modifier == KNOSYM_MINUS) ||
	     (modifier == minus_fcnid) ||
	     (modifier == minus_prim) )
    return kno_subtract(old_value,value);
  else if (modifier == KNOSYM_STORE)
    return kno_incref(value);
  else {
    if (KNO_FCNIDP(modifier)) modifier = kno_fcnid_ref(modifier);
    if (KNO_FUNCTIONP(modifier)) {
      kno_function fcn = (kno_function) modifier;
      if (fcn->fcn_arity==1)
	return kno_apply(modifier,1,&old_value);
      else {
	lispval args[2] = { old_value, value };
	return kno_apply(modifier,2,args);}}
    else if ( (KNO_VOIDP(value)) || (KNO_DEFAULTP(value)) )
      return kno_apply(modifier,1,&old_value);
    else if (KNO_APPLICABLEP(modifier)) {
      lispval args[2] = { old_value, value };
      return kno_apply(modifier,2,args);}
    else return kno_err("BadCompoundModifier","compound_modify",NULL,
			modifier);}
}

DEFPRIM5
("compound-modify!",compound_modify,
 KNO_MAX_ARGS(5)|KNO_MIN_ARGS(4)|KNO_NDCALL,
 "`(compound-modify! *compound* *tag* *eltno* *modfn* *modval*)` "
 "**undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval compound_modify(lispval x,lispval tag,lispval offset,
                               lispval modifier,lispval value)
{
  if (EMPTYP(x))
    return EMPTY;
  else if (CHOICEP(x)) {
    DO_CHOICES(eachx,x)
      if (KNO_COMPOUNDP(eachx)) {
        lispval result = compound_modify(eachx,tag,offset,modifier,value);
        if (KNO_ABORTP(result)) {
          KNO_STOP_DO_CHOICES;
          return result;}
        else kno_decref(result);}
      else return kno_type_error("compound","compound_set",eachx);
    return VOID;}
  else {
    struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
    if (!(KNO_UINTP(offset)))
      return kno_type_error("unsigned int","compound_ref",offset);
    unsigned int off = FIX2INT(offset), len = compound->compound_length;
    if ((compound->compound_ismutable) &&
        ((compound->typetag == tag) || 
	 (VOIDP(tag)) || (FALSEP(tag)) || 
	 (DEFAULTP(tag)) ) &&
        (off<len)) {
      lispval *valuep = ((&(compound->compound_0))+off);
      lispval old_value, new_value;
      u8_write_lock(&(compound->compound_rwlock));
      old_value = *valuep;
      new_value = apply_modifier(modifier,old_value,value);
      if (KNO_ABORTP(new_value)) {
        u8_rw_unlock(&(compound->compound_rwlock));
        return new_value;}
      *valuep = new_value;
      kno_decref(old_value);
      u8_rw_unlock(&(compound->compound_rwlock));
      return VOID;}
    /* Unlock and figure out the details of the error */
    if (compound->compound_ismutable==0) {
      kno_seterr(_("Immutable record"),"set_compound",NULL,x);
      return KNO_ERROR;}
    else if ((compound->typetag!=tag) && (!(VOIDP(tag)))) {
      u8_string type_string = kno_lisp2string(tag);
      kno_seterr(kno_TypeError,"compound_ref",type_string,x);
      u8_free(type_string);
      return KNO_ERROR;}
    else if (!(VOIDP(tag))) {
      u8_string type_string = kno_lisp2string(tag);
      kno_seterr(kno_RangeError,"compound_ref",type_string,off);
      u8_free(type_string);
      return KNO_ERROR;}
    else {
      kno_seterr(kno_RangeError,"compound_ref",NULL,off);
      return KNO_ERROR;}
  }
}

DEFPRIM("make-compound",make_compound,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
        "`(MAKE-COMPOUND *arg0* *args...*)` **undocumented**");
static lispval make_compound(int n,kno_argvec args)
{
  struct KNO_COMPOUND *compound=
    u8_zmalloc(sizeof(struct KNO_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  KNO_INIT_CONS(compound,kno_compound_type);
  compound->typetag = kno_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 0;
  compound->compound_isopaque = 0;
  compound->compound_seqoff = -1;
  while (i<n) {
    kno_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

DEFPRIM("make-opaque-compound",make_opaque_compound,
	KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
        "`(MAKE-OPAQUE-COMPOUND *arg0* *args...*)` **undocumented**");
static lispval make_opaque_compound(int n,kno_argvec args)
{
  struct KNO_COMPOUND *compound=
    u8_zmalloc(sizeof(struct KNO_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  KNO_INIT_CONS(compound,kno_compound_type);
  compound->typetag = kno_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 0;
  compound->compound_isopaque = 1;
  compound->compound_seqoff = -1;
  while (i<n) {
    kno_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

DEFPRIM("make-mutable-compound",make_mutable_compound,
	KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
        "`(MAKE-MUTABLE-COMPOUND *arg0* *args...*)` **undocumented**");
static lispval make_mutable_compound(int n,kno_argvec args)
{
  struct KNO_COMPOUND *compound=
    u8_zmalloc(sizeof(struct KNO_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  KNO_INIT_CONS(compound,kno_compound_type);
  compound->typetag = kno_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 1;
  compound->compound_seqoff = -1;
  u8_init_rwlock(&(compound->compound_rwlock));
  while (i<n) {
    kno_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

DEFPRIM("make-opaque-mutable-compound",make_opaque_mutable_compound,
	KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
        "`(MAKE-OPAQUE-MUTABLE-COMPOUND *arg0* *args...*)` **undocumented**");
static lispval make_opaque_mutable_compound(int n,kno_argvec args)
{
  struct KNO_COMPOUND *compound=
    u8_zmalloc(sizeof(struct KNO_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  KNO_INIT_CONS(compound,kno_compound_type);
  compound->typetag = kno_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 1;
  compound->compound_isopaque = 1;
  compound->compound_seqoff = -1;
  u8_init_rwlock(&(compound->compound_rwlock));
  while (i<n) {
    kno_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

DEFPRIM5("sequence->compound",seq2compound,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
         "`(SEQUENCE->COMPOUND *seq* *tag* *mutable* *opaque* *reserve*)` "
         "creates a compound object out of *seq*, tagged "
         "with *tag*. If *reserve* is #f, the returned "
         "compound won't be a sequence",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_FALSE,kno_any_type,KNO_FALSE,
         kno_any_type,KNO_TRUE);
static lispval seq2compound(lispval seq,lispval tag,
                            lispval mutable,lispval opaque,
                            lispval offset)
{
  lispval *read; int len;
  if (KNO_VECTORP(seq)) {
    len = VEC_LEN(seq);
    read = KNO_VECTOR_ELTS(seq);}
  else read = kno_seq_elts(seq,&len);
  if ( (read == NULL) && (len < 0) )
    return KNO_ERROR_VALUE;
  int extra_elts = (len) ? (len-1) : (0);
  struct KNO_COMPOUND *compound =
    u8_zmalloc(sizeof(struct KNO_COMPOUND)+((extra_elts)*LISPVAL_LEN));
  KNO_INIT_CONS(compound,kno_compound_type);
  compound->typetag = kno_incref(tag);
  compound->compound_length = len;
  if ( (len==0) || (FALSEP(mutable)) )
    compound->compound_ismutable = 0;
  else {
    compound->compound_ismutable = 1;
    u8_init_rwlock(&(compound->compound_rwlock));}
  if (FALSEP(opaque)) {
    compound->compound_isopaque = 0;
    compound->compound_seqoff = 0;}
  else {
    compound->compound_isopaque = 1;
    compound->compound_seqoff = -1;}
  lispval *write = &(compound->compound_0);
  int i = 0, n = len; while (i<n) {
    lispval elt = read[i++];
    kno_incref(elt);
    *write++=elt;}
  if (KNO_FALSEP(offset))
    compound->compound_seqoff = -1;
  else if ( (KNO_VOIDP(offset)) || (KNO_DEFAULTP(offset)) ||
	    (KNO_TRUEP(offset)) )
    compound->compound_seqoff = 0;
  else if (KNO_FIXNUMP(offset)) {
    long long off = KNO_FIX2INT(offset);
    if ( (off>=0) && (off<128) && (off < len) )
      compound->compound_seqoff = off;
    else {
      kno_seterr("BadCompoundVectorOffset","vector2compound",NULL,offset);
      kno_decref_vec(&(compound->compound_0),len);
      u8_free(compound);
      return KNO_ERROR_VALUE;}}
  else {
    kno_seterr("BadCompoundVectorOffset","vector2compound",NULL,offset);
    kno_decref_vec(&(compound->compound_0),len);
    u8_free(compound);
    return KNO_ERROR_VALUE;}
  if (KNO_VECTORP(seq))
    return LISP_CONS(compound);
  else {
    kno_decref_vec(read,len);
    u8_free(read);
    return LISP_CONS(compound);}
}

/* Setting various compound properties */

static lispval consfn_symbol, stringfn_symbol, tag_symbol;


/* Initializing common functions */

KNO_EXPORT void kno_init_compoundfns_c()
{
  u8_register_source_file(_FILEINFO);

  tag_symbol = kno_intern("tag");
  consfn_symbol = kno_intern("cons");
  stringfn_symbol = kno_intern("stringify");

  lispval scheme = kno_scheme_module; 

  choice_prim = kno_get(scheme,kno_intern("choice"),KNO_VOID);
  choice_fcnid  = kno_register_fcnid(choice_prim);

  push_prim = kno_get(scheme,kno_intern("push"),KNO_VOID);
  push_fcnid  = kno_register_fcnid(push_prim);

  plus_prim = kno_get(scheme,kno_intern("+"),KNO_VOID);
  plus_fcnid  = kno_register_fcnid(plus_prim);

  minus_prim = kno_get(scheme,kno_intern("-"),KNO_VOID);
  minus_fcnid  = kno_register_fcnid(minus_prim);

  minusone_prim = kno_get(scheme,kno_intern("-1+"),KNO_VOID);
  minusone_fcnid  = kno_register_fcnid(minusone_prim);

  plusone_prim = kno_get(scheme,kno_intern("1+"),KNO_VOID);
  plusone_fcnid  = kno_register_fcnid(plusone_prim);

  link_local_cprims();
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("compound?",compoundp,2,scheme_module);
  KNO_LINK_PRIM("sequence->compound",seq2compound,5,scheme_module);
  KNO_LINK_VARARGS("make-opaque-mutable-compound",make_opaque_mutable_compound,scheme_module);
  KNO_LINK_VARARGS("make-mutable-compound",make_mutable_compound,scheme_module);
  KNO_LINK_VARARGS("make-opaque-compound",make_opaque_compound,scheme_module);
  KNO_LINK_VARARGS("make-compound",make_compound,scheme_module);
  KNO_LINK_PRIM("compound-modify!",compound_modify,5,scheme_module);
  KNO_LINK_PRIM("compound-set!",compound_set,4,scheme_module);
  KNO_LINK_PRIM("unpack-compound",unpack_compound,2,scheme_module);
  KNO_LINK_PRIM("compound-ref",compound_ref,3,scheme_module);
  KNO_LINK_PRIM("compound-opaque?",compound_opaquep,1,scheme_module);
  KNO_LINK_PRIM("compound-mutable?",compound_mutablep,1,scheme_module);
  KNO_LINK_PRIM("compound-length",compound_length,1,scheme_module);
  KNO_LINK_PRIM("compound-tag",compound_tag,1,scheme_module);
  KNO_LINK_PRIM("pick-compounds",pick_compounds,2,scheme_module);

  KNO_LINK_ALIAS("compound-type?",compoundp,scheme_module);
  KNO_LINK_ALIAS("vector->compound",seq2compound,scheme_module);
}

