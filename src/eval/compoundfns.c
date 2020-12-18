/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
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

DEFCPRIM("compound?",compoundp,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "returns #f if *obj* is a compound and (when *tag* "
	 "is provided) has the typetag *tag*.",
	 {"x",kno_any_type,KNO_VOID},
	 {"tag",kno_any_type,KNO_VOID})
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

DEFCPRIM("pick-compounds",pick_compounds,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "returns *arg* if it is a compound and (when "
	 "specified) if it's typetag is any of *tags*.",
	 {"candidates",kno_any_type,KNO_VOID},
	 {"tags",kno_any_type,KNO_VOID})
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

DEFCPRIM("compound-tag",compound_tag,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"x",kno_compound_type,KNO_VOID})
static lispval compound_tag(lispval x)
{
  return kno_incref(KNO_COMPOUND_TAG(x));
}

DEFCPRIM("compound-annotations",compound_annotations,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns a compound's annotations object, a table, "
	 "which is the first element of compound's declared "
	 "as annotated.",
	 {"x",kno_compound_type,KNO_VOID})
static lispval compound_annotations(lispval x)
{
  struct KNO_COMPOUND *co = (kno_compound) x;
  if (co->compound_istable) {
    if (KNO_TABLEP(co->compound_0))
      return kno_incref(co->compound_0);
    else return KNO_EMPTY;}
  else return KNO_EMPTY;
}

DEFCPRIM("compound-length",compound_length,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"x",kno_compound_type,KNO_VOID})
static lispval compound_length(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  return KNO_BYTE2LISP(compound->compound_length);
}

DEFCPRIM("compound-mutable?",compound_mutablep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"x",kno_compound_type,KNO_VOID})
static lispval compound_mutablep(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if (compound->compound_ismutable)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("compound-opaque?",compound_opaquep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"x",kno_compound_type,KNO_VOID})
static lispval compound_opaquep(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if (compound->compound_isopaque)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("compound-ref",compound_ref,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "**undocumented**",
	 {"x",kno_compound_type,KNO_VOID},
	 {"offset",kno_any_type,KNO_VOID},
	 {"tag",kno_any_type,KNO_VOID})
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

DEFCPRIM("unpack-compound",unpack_compound,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"x",kno_compound_type,KNO_VOID},
	 {"tag",kno_any_type,KNO_VOID})
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

DEFCPRIM("compound-set!",compound_set,
	 KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID},
	 {"offset",kno_any_type,KNO_VOID},
	 {"value",kno_any_type,KNO_VOID},
	 {"tag",kno_any_type,KNO_VOID})
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
    kno_incref(old_value); kno_incref(value);
    CHOICE_ADD(old_value,value);
    return old_value;}
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

DEFCPRIM("compound-modify!",compound_modify,
	 KNO_MAX_ARGS(5)|KNO_MIN_ARGS(4)|KNO_NDCALL,
	 "Modifies a field of *compound* atomically, "
	 "replacing it with (*modfn* *curval* *modval*) "
	 "while holding any locks on the compound.",
	 {"x",kno_any_type,KNO_VOID},
	 {"tag",kno_any_type,KNO_VOID},
	 {"offset",kno_any_type,KNO_VOID},
	 {"modifier",kno_any_type,KNO_VOID},
	 {"value",kno_any_type,KNO_VOID})
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

DEFCPRIMN("make-compound",make_compound,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "creates a simple compound object with type *tag* "
	  "and elements *elts*")
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

DEFCPRIMN("make-xcompound",make_xcompound,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(5)|KNO_NDCALL,
	  "creates a (possibly) complex compound object with "
	  "type *tag*. If *annotate* is a table, it is used "
	  "as the annotations for the compound; if it is #t, "
	  "an empty slotmap is created. If *seqoff* is not "
	  "false, the new compound will also support the "
	  "sequence protocol, with the sequence elements "
	  "starting at *seqoff* (if its a positive fixnum).")
static lispval make_xcompound(int n,kno_argvec args)
{
  lispval typetag = kno_incref(args[0]);
  int is_mutable  = (!(KNO_FALSEP(args[1])));
  int is_opaque   =  (!(KNO_FALSEP(args[2])));
  lispval arg3    = args[3], arg4=args[4];
  lispval annotations = (KNO_TABLEP(arg3)) ? (kno_incref(arg3)) :
    (!(KNO_FALSEP(arg3))) ? (kno_make_slotmap(3,0,NULL)) :
    (KNO_FALSE);
  int tablep      = (!(KNO_FALSEP(annotations))), seqoff = -1;
  if (KNO_FALSEP(arg4)) seqoff=-1;
  else if (KNO_TRUEP(arg4)) seqoff=0;
  else if ( (KNO_FIXNUMP(arg4)) && (KNO_FIX2INT(arg4)>=0) &&
	    (KNO_FIX2INT(arg4) < 128 ))
    seqoff = KNO_FIX2INT(arg4);
  else {
    u8_byte buf[128];
    return kno_err("InvalidCompoundSequenceOffset","make_xcompound",
		   u8_bprintf(buf,"%q",args[0]),
		   arg4);}
  int data_i = 5, data_len = n-5;
  int compound_len = data_len + tablep;
  struct KNO_COMPOUND *compound=
    /* KNO_COMPOUND contains the first element */
    u8_zmalloc(sizeof(struct KNO_COMPOUND)+((compound_len-1)*LISPVAL_LEN));
  lispval *write = (&(compound->compound_0));
  KNO_INIT_CONS(compound,kno_compound_type);
  compound->compound_length    = compound_len;
  compound->typetag            = typetag;
  compound->compound_ismutable = is_mutable;
  compound->compound_isopaque  = is_opaque;
  compound->compound_istable   = tablep;
  compound->compound_seqoff    = seqoff;
  if (tablep) *write++ = annotations;
  while (data_i<n) {
    lispval arg = args[data_i++]; kno_incref(arg);
    *write++=arg;}
  return LISP_CONS(compound);
}

DEFCPRIM("sequence->compound",seq2compound,
	 KNO_MAX_ARGS(6)|KNO_MIN_ARGS(2),
	 "creates a compound object out of *seq*, tagged "
	 "with *tag*. The first *reserve* elements of *seq* "
	 "will be part of the compound but not part of the "
	 "sequence; if *seqoff* is not #f, the returned "
	 "compound will also be a sequence, starting at the "
	 "*seqoff* element (if a fixnum in range).",
	 {"seq",kno_any_type,KNO_VOID},
	 {"tag",kno_any_type,KNO_VOID},
	 {"mutable",kno_any_type,KNO_FALSE},
	 {"opaque",kno_any_type,KNO_FALSE},
	 {"offset",kno_any_type,KNO_TRUE},
	 {"annotated",kno_any_type,KNO_FALSE})
static lispval seq2compound(lispval seq,lispval tag,
                            lispval mutable,lispval opaque,
                            lispval offset,lispval annotated)
{
  int len, seqoff = -1, istable = (!(KNO_FALSEP(annotated)));
  lispval *read;
  if (KNO_VECTORP(seq)) {
    len = VEC_LEN(seq);
    read = KNO_VECTOR_ELTS(seq);}
  else read = kno_seq_elts(seq,&len);
  if ( (read == NULL) && (len < 0) )
    return KNO_ERROR_VALUE;
  if ( (KNO_VOIDP(offset)) || (KNO_DEFAULTP(offset)) || (KNO_TRUEP(offset)) )
    seqoff = 0;
  else if (KNO_FIXNUMP(offset)) {
    long long off = KNO_FIX2INT(offset);
    if ( (off>=0) && (off<128) && (off < len) )
      seqoff = off + istable;
    else return kno_err("BadCompoundVectorOffset","vector2compound",
			NULL,offset);}
  else if (KNO_FALSEP(offset)) {}
  else return kno_err("BadCompoundVectorOffset","vector2compound",NULL,offset);
  lispval annotations = (KNO_TABLEP(annotated)) ? (kno_incref(annotated)) :
    (KNO_TRUEP(annotated)) ? (kno_make_slotmap(3,0,NULL)) :
    (KNO_VOID);
  int tablep = (!(KNO_VOIDP(annotations)));
  ssize_t compound_len = len + tablep;
  struct KNO_COMPOUND *compound =
    u8_zmalloc(sizeof(struct KNO_COMPOUND)+((compound_len-1)*LISPVAL_LEN));
  KNO_INIT_CONS(compound,kno_compound_type);
  compound->typetag = kno_incref(tag);
  compound->compound_length = compound_len;
  compound->compound_seqoff = seqoff;
  compound->compound_istable = tablep;
  if ( (len==0) || (FALSEP(mutable)) )
    compound->compound_ismutable = 0;
  else {
    compound->compound_ismutable = 1;
    u8_init_rwlock(&(compound->compound_rwlock));}
  compound->compound_isopaque = (!(FALSEP(opaque)));
  lispval *write = &(compound->compound_0);
  if (istable) *write++=annotations;
  int i = 0; while (i<len) {
    lispval elt = read[i++];
    kno_incref(elt);
    *write++=elt;}
  if (KNO_VECTORP(seq))
    return LISP_CONS(compound);
  else {
    kno_decref_vec(read,len);
    u8_free(read);
    return LISP_CONS(compound);}
}

DEFCPRIMN("make-opaque-compound",make_opaque_compound,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**")
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

DEFCPRIMN("make-mutable-compound",make_mutable_compound,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**")
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

DEFCPRIMN("make-opaque-mutable-compound",make_opaque_mutable_compound,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "**undocumented**")
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

  KNO_LINK_CPRIM("compound?",compoundp,2,scheme_module);
  KNO_LINK_CPRIM("sequence->compound",seq2compound,6,scheme_module);
  KNO_LINK_CVARARGS("make-opaque-mutable-compound",make_opaque_mutable_compound,scheme_module);
  KNO_LINK_CVARARGS("make-mutable-compound",make_mutable_compound,scheme_module);
  KNO_LINK_CVARARGS("make-opaque-compound",make_opaque_compound,scheme_module);
  KNO_LINK_CVARARGS("make-compound",make_compound,scheme_module);
  KNO_LINK_CVARARGS("make-xcompound",make_xcompound,scheme_module);
  KNO_LINK_CPRIM("compound-modify!",compound_modify,5,scheme_module);
  KNO_LINK_CPRIM("compound-set!",compound_set,4,scheme_module);
  KNO_LINK_CPRIM("unpack-compound",unpack_compound,2,scheme_module);
  KNO_LINK_CPRIM("compound-ref",compound_ref,3,scheme_module);
  KNO_LINK_CPRIM("compound-opaque?",compound_opaquep,1,scheme_module);
  KNO_LINK_CPRIM("compound-mutable?",compound_mutablep,1,scheme_module);
  KNO_LINK_CPRIM("compound-length",compound_length,1,scheme_module);
  KNO_LINK_CPRIM("compound-tag",compound_tag,1,scheme_module);
  KNO_LINK_CPRIM("compound-annotations",compound_annotations,1,scheme_module);
  KNO_LINK_CPRIM("pick-compounds",pick_compounds,2,scheme_module);

  KNO_LINK_ALIAS("compound-type?",compoundp,scheme_module);
  KNO_LINK_ALIAS("vector->compound",seq2compound,scheme_module);
}

