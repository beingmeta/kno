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

DEFPRIM2("COMPOUND?",compoundp,MIN_ARGS(1),
         "`(COMPOUND? *obj* *tag*)` returns #f if *obj* is a compound and "
         "(when *tag* is provided) has the typetag *tag*.",
         -1,KNO_VOID,-1,KNO_VOID)
  (lispval x,lispval tag)
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
static lispval pick_compounds(lispval candidates,lispval tags)
{
  if (VOIDP(tags)) {
    lispval result = KNO_EMPTY; int changes = 0;
    {KNO_DO_CHOICES(candidate,candidates)
        if (KNO_COMPOUNDP(candidate)) {
          kno_incref(candidate);
          KNO_ADD_TO_CHOICE(result,candidate);
          changes = 1;}}
    if (changes == 0) {
      kno_decref(result);
      return kno_incref(candidates);}
    else return kno_simplify_choice(result);}
  else {
    lispval result = KNO_EMPTY; int changes = 0;
    {KNO_DO_CHOICES(candidate,candidates)
        if ( (KNO_COMPOUNDP(candidate)) &&
             (kno_overlapp(KNO_COMPOUND_TAG(candidate),tags)) ) {
          kno_incref(candidate);
          KNO_ADD_TO_CHOICE(result,candidate);
          changes = 1;}}
    if (changes == 0) {
      kno_decref(result);
      return kno_incref(candidates);}
    else return kno_simplify_choice(result);}
}

static lispval compound_tag(lispval x)
{
  return kno_incref(KNO_COMPOUND_TAG(x));
}

static lispval compound_length(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  return KNO_BYTE2DTYPE(compound->compound_length);
}

static lispval compound_mutablep(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if (compound->compound_ismutable)
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval compound_opaquep(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if (compound->compound_isopaque)
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval compound_ref(lispval x,lispval offset,lispval tag)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if (!(KNO_UINTP(offset)))
    return kno_type_error("unsigned int","compound_ref",offset);
  unsigned int off = FIX2INT(offset), len = compound->compound_length;
  if (compound->compound_ismutable) u8_lock_mutex(&(compound->compound_lock));
  if (((compound->compound_typetag == tag) || (VOIDP(tag))) && (off<len)) {
    lispval value = *((&(compound->compound_0))+off);
    kno_incref(value);
    if (compound->compound_ismutable) u8_unlock_mutex(&(compound->compound_lock));
    return value;}
  /* Unlock and figure out the details of the error */
  if (compound->compound_ismutable) u8_unlock_mutex(&(compound->compound_lock));
  if ((compound->compound_typetag!=tag) && (!(VOIDP(tag)))) {
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

static lispval unpack_compound(lispval x,lispval tag)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  if ((!(VOIDP(tag)))&&(compound->compound_typetag!=tag)) {
    u8_string type_string = kno_lisp2string(tag);
    kno_seterr(kno_TypeError,"compound_ref",type_string,x);
    return KNO_ERROR;}
  else {
    int len = compound->compound_length;
    lispval *elts = &(compound->compound_0), result = VOID;
    if (compound->compound_ismutable)
      u8_lock_mutex(&(compound->compound_lock));
    {
      lispval *scan = elts, *lim = elts+len; while (scan<lim) {
        lispval v = *scan++; kno_incref(v);}}
    result = kno_init_pair(NULL,kno_incref(compound->compound_typetag),
                        kno_make_vector(len,elts));
    if (compound->compound_ismutable) u8_unlock_mutex(&(compound->compound_lock));
    return result;}
}

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
        ((compound->compound_typetag == tag) || (VOIDP(tag))) &&
        (off<len)) {
      lispval *valuep = ((&(compound->compound_0))+off), old_value;
      u8_lock_mutex(&(compound->compound_lock));
      old_value = *valuep;
      kno_incref(value);
      *valuep = value;
      kno_decref(old_value);
      u8_unlock_mutex(&(compound->compound_lock));
      return VOID;}
    /* Unlock and figure out the details of the error */
    u8_unlock_mutex(&(compound->compound_lock));
    if (compound->compound_ismutable==0) {
      kno_seterr(_("Immutable record"),"set_compound",NULL,x);
      return KNO_ERROR;}
    else if ((compound->compound_typetag!=tag) && (!(VOIDP(tag)))) {
      u8_string type_string = kno_lisp2string(tag);
      kno_seterr(kno_TypeError,"compound_ref",type_string,x);
      return KNO_ERROR;}
    else if (!(VOIDP(tag))) {
      u8_string type_string = kno_lisp2string(tag);
      kno_seterr(kno_RangeError,"compound_ref",type_string,off);
      return KNO_ERROR;}
    else {
      kno_seterr(kno_RangeError,"compound_ref",NULL,off);
      return KNO_ERROR;}
  }
}

static lispval compound_modify(lispval x,lispval offset,
                               lispval modifier,lispval value,
                               lispval tag)
{
  if (EMPTYP(x)) return EMPTY;
  else if (CHOICEP(x)) {
    DO_CHOICES(eachx,x)
      if (KNO_COMPOUNDP(eachx)) {
        lispval result = compound_modify(eachx,offset,modifier,value,tag);
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
        ((compound->compound_typetag == tag) || (VOIDP(tag))) &&
        (off<len)) {
      lispval *valuep = ((&(compound->compound_0))+off), old_value, new_value;
      u8_lock_mutex(&(compound->compound_lock));
      old_value = *valuep;
      if (KNO_APPLICABLEP(modifier)) {
        if (KNO_VOIDP(value))
          new_value = kno_apply(modifier,1,&old_value);
        else {
          lispval args[2] = { old_value, value };
          new_value = kno_apply(modifier,2,args);}}
      else if (modifier == KNOSYM_ADD) {
        new_value = old_value; kno_incref(value);
        CHOICE_ADD(new_value,value);}
      else if (modifier == KNOSYM_DROP)
        new_value = kno_difference(old_value,value);
      else if (modifier == KNOSYM_STORE)
        new_value = kno_incref(value);
      else if (modifier == KNOSYM_PLUS)
        new_value = kno_plus(old_value,value);
      else if (modifier == KNOSYM_MINUS)
        new_value = kno_subtract(old_value,value);
      else new_value = kno_err("BadCompoundModifier","compound_modify",NULL,
                              modifier);
      if (KNO_ABORTP(new_value)) {
        u8_unlock_mutex(&(compound->compound_lock));
        return new_value;}
      *valuep = new_value;
      kno_decref(old_value);
      u8_unlock_mutex(&(compound->compound_lock));
      return VOID;}
    /* Unlock and figure out the details of the error */
    u8_unlock_mutex(&(compound->compound_lock));
    if (compound->compound_ismutable==0) {
      kno_seterr(_("Immutable record"),"set_compound",NULL,x);
      return KNO_ERROR;}
    else if ((compound->compound_typetag!=tag) && (!(VOIDP(tag)))) {
      u8_string type_string = kno_lisp2string(tag);
      kno_seterr(kno_TypeError,"compound_ref",type_string,x);
      return KNO_ERROR;}
    else if (!(VOIDP(tag))) {
      u8_string type_string = kno_lisp2string(tag);
      kno_seterr(kno_RangeError,"compound_ref",type_string,off);
      return KNO_ERROR;}
    else {
      kno_seterr(kno_RangeError,"compound_ref",NULL,off);
      return KNO_ERROR;}
  }
}

static lispval make_compound(int n,lispval *args)
{
  struct KNO_COMPOUND *compound=
    u8_malloc(sizeof(struct KNO_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  KNO_INIT_FRESH_CONS(compound,kno_compound_type);
  compound->compound_typetag = kno_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 0;
  compound->compound_isopaque = 0;
  compound->compound_off = -1;
  while (i<n) {
    kno_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

static lispval make_opaque_compound(int n,lispval *args)
{
  struct KNO_COMPOUND *compound=
    u8_malloc(sizeof(struct KNO_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  KNO_INIT_FRESH_CONS(compound,kno_compound_type);
  compound->compound_typetag = kno_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 0;
  compound->compound_isopaque = 1;
  compound->compound_off = -1;
  while (i<n) {
    kno_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

static lispval make_mutable_compound(int n,lispval *args)
{
  struct KNO_COMPOUND *compound=
    u8_malloc(sizeof(struct KNO_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  KNO_INIT_FRESH_CONS(compound,kno_compound_type);
  compound->compound_typetag = kno_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 1;
  compound->compound_off = -1;
  u8_init_mutex(&(compound->compound_lock));
  while (i<n) {
    kno_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

static lispval make_opaque_mutable_compound(int n,lispval *args)
{
  struct KNO_COMPOUND *compound=
    u8_malloc(sizeof(struct KNO_COMPOUND)+((n-2)*LISPVAL_LEN));
  int i = 1; lispval *write = &(compound->compound_0);
  KNO_INIT_FRESH_CONS(compound,kno_compound_type);
  compound->compound_typetag = kno_incref(args[0]);
  compound->compound_length = n-1;
  compound->compound_ismutable = 1;
  compound->compound_isopaque = 1;
  compound->compound_off = -1;
  u8_init_mutex(&(compound->compound_lock));
  while (i<n) {
    kno_incref(args[i]);
    *write++=args[i];
    i++;}
  return LISP_CONS(compound);
}

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
    u8_malloc(sizeof(struct KNO_COMPOUND)+((extra_elts)*LISPVAL_LEN));
  KNO_INIT_FRESH_CONS(compound,kno_compound_type);
  compound->compound_typetag = kno_incref(tag);
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
    kno_incref(elt);
    *write++=elt;}
  if (KNO_FALSEP(offset))
    compound->compound_off = -1;
  else {
    if (KNO_FIXNUMP(offset)) {
      long long off = KNO_FIX2INT(offset);
      if ( (off>=0) && (off<128) && (off < len) )
        compound->compound_off = off;
      else {
        kno_seterr("BadCompoundVectorOffset","vector2compound",NULL,offset);
        return KNO_ERROR_VALUE;}}
    else compound->compound_off = 0;}
  if (KNO_VECTORP(seq))
    return LISP_CONS(compound);
  else {
    kno_decref_vec(read,len);
    u8_free(read);
    return LISP_CONS(compound);}
}

/* Setting various compound properties */

static lispval consfn_symbol, stringfn_symbol, tag_symbol;

static lispval compound_corelen_prim(lispval tag)
{
  struct KNO_COMPOUND_TYPEINFO *e = kno_lookup_compound(tag);
  if (e) {
    if (e->compound_corelen<0) return KNO_FALSE;
    else return KNO_INT(e->compound_corelen);}
  else return EMPTY;
}

static lispval compound_set_corelen_prim(lispval tag,lispval slots_arg)
{
  if (!(KNO_UINTP(slots_arg)))
    return kno_type_error("unsigned int",
                         "compound_set_corelen_prim",
                         slots_arg);
  unsigned int core_slots = FIX2INT(slots_arg);
  kno_declare_compound(tag,VOID,core_slots);
  return kno_incref(tag);
}

static lispval tag_slotdata(lispval tag)
{
  struct KNO_COMPOUND_TYPEINFO *e = kno_lookup_compound(tag);
  if ((e)&&(SLOTMAPP(e->compound_metadata)))
    return kno_incref(e->compound_metadata);
  else if ((e)&&(!(VOIDP(e->compound_metadata))))
    return kno_type_error("slotmap","tag_slotdata",e->compound_metadata);
  else {
    struct KNO_KEYVAL *keyvals = u8_alloc_n(1,struct KNO_KEYVAL);
    lispval slotmap = VOID, *slotdata = &slotmap;
    keyvals[0].kv_key = tag_symbol; keyvals[0].kv_key = kno_incref(tag);
    slotmap = kno_init_slotmap(NULL,1,keyvals);
    kno_register_compound(tag,slotdata,NULL);
    return slotmap;}
}

static lispval compound_metadata_prim(lispval compound,lispval field)
{
  lispval tag = KNO_COMPOUND_TAG(compound);
  lispval slotmap = tag_slotdata(tag);
  lispval result;
  if (VOIDP(field)) result = kno_deep_copy(slotmap);
  else result = kno_get(slotmap,tag,EMPTY);
  kno_decref(slotmap);
  return result;
}

static lispval cons_compound(int n,lispval *args,kno_compound_typeinfo e)
{
  if (e->compound_metadata) {
    lispval method = kno_get(e->compound_metadata,KNOSYM_CONS,VOID);
    if (VOIDP(method)) return VOID;
    else {
      lispval result = kno_apply(method,n,args);
      kno_decref(method);
      return result;}}
  else {
    return kno_init_compound_from_elts(NULL,e->compound_typetag,
                                      KNO_COMPOUND_INCREF,
                                      n,args);}
}

static int stringify_compound(u8_output out,lispval compound,kno_compound_typeinfo e)
{
  if (e->compound_metadata) {
    lispval method = kno_get(e->compound_metadata,stringfn_symbol,VOID);
    if (VOIDP(method)) return 0;
    else {
      lispval result = kno_apply(method,1,&compound);
      kno_decref(method);
      if (STRINGP(result)) {
        u8_putn(out,CSTRING(result),STRLEN(result));
        kno_decref(result);
        return 1;}
      else {kno_decref(result); return 0;}}}
  else return 0;
}

static lispval compound_set_consfn_prim(lispval tag,lispval consfn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(consfn)) {
      lispval slotmap = tag_slotdata(tag);
      struct KNO_COMPOUND_TYPEINFO *e = kno_lookup_compound(tag);
      kno_drop(slotmap,KNOSYM_CONS,VOID);
      kno_decref(slotmap);
      e->compound_parser = NULL;
      return VOID;}
    else if (KNO_APPLICABLEP(consfn)) {
      lispval slotmap = tag_slotdata(tag);
      struct KNO_COMPOUND_TYPEINFO *e = kno_lookup_compound(tag);
      kno_store(slotmap,KNOSYM_CONS,consfn);
      kno_decref(slotmap);
      e->compound_parser = cons_compound;
      return VOID;}
    else return kno_type_error("applicable","set_compound_consfn_prim",tag);
  else return kno_type_error("compound tag","set_compound_consfn_prim",tag);
}


static lispval compound_set_stringfn_prim(lispval tag,lispval stringfn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(stringfn)) {
      lispval slotmap = tag_slotdata(tag);
      struct KNO_COMPOUND_TYPEINFO *e = kno_lookup_compound(tag);
      kno_drop(slotmap,stringfn_symbol,VOID);
      kno_decref(slotmap);
      e->compound_unparser = NULL;
      return VOID;}
    else if (KNO_APPLICABLEP(stringfn)) {
      lispval slotmap = tag_slotdata(tag);
      struct KNO_COMPOUND_TYPEINFO *e = kno_lookup_compound(tag);
      kno_store(slotmap,stringfn_symbol,stringfn);
      kno_decref(slotmap);
      e->compound_unparser = stringify_compound;
      return VOID;}
    else return kno_type_error("applicable","set_compound_stringfn_prim",tag);
  else return kno_type_error("compound tag","set_compound_stringfn_prim",tag);
}

/* Initializing common functions */

KNO_EXPORT void kno_init_compoundfns_c()
{
  u8_register_source_file(_FILEINFO);

  consfn_symbol = kno_intern("cons");
  stringfn_symbol = kno_intern("stringify");

  DECL_PRIM(compoundp,2,kno_scheme_module);
  kno_defalias(kno_scheme_module,"COMPOUND-TYPE?","COMPOUND?");
  kno_idefn2(kno_scheme_module,"PICK-COMPOUND",pick_compounds,1,
             "`(PICK-COMPOUND *arg* [*tags*])` returns *arg* if it is "
             "a compound and (when specified) if it's typetag "
             "is any of *tags*.",
             -1,KNO_VOID,-1,KNO_VOID);
  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("COMPOUND-TAG",compound_tag,1,
                           kno_compound_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("COMPOUND-LENGTH",compound_length,1,
                           kno_compound_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("COMPOUND-MUTABLE?",compound_mutablep,1,
                           kno_compound_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("COMPOUND-OPAQUE?",compound_opaquep,1,
                           kno_compound_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim3x("COMPOUND-REF",compound_ref,2,
                           kno_compound_type,VOID,-1,VOID,-1,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim
           (kno_make_cprim4x("COMPOUND-SET!",compound_set,3,
                            -1,VOID,-1,VOID,
                            -1,VOID,-1,VOID)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim
           (kno_make_cprim5x("COMPOUND-MODIFY!",compound_modify,4,
                            -1,VOID,-1,VOID,
                            -1,VOID,-1,VOID,
                            -1,VOID)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("MAKE-COMPOUND",make_compound,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("MAKE-MUTABLE-COMPOUND",
                                         make_mutable_compound,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("MAKE-OPAQUE-COMPOUND",
                                         make_opaque_compound,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprimn("MAKE-OPAQUE-MUTABLE-COMPOUND",
                                         make_opaque_mutable_compound,1)));
  kno_idefn5(kno_scheme_module,"SEQUENCE->COMPOUND",seq2compound,2,
            "`(SEQUENCE->COMPOUND *seq* *tag* *mutable* *opaque* *reserve*)` "
            "creates a compound object out of *seq*, tagged with *tag*. "
            "If *reserve* is #f, the returned compound won't be a sequence",
            -1,VOID,-1,VOID,
            -1,KNO_FALSE,-1,KNO_FALSE,-1,KNO_TRUE);
  kno_idefn5(kno_scheme_module,"VECTOR->COMPOUND",seq2compound,2,
            "`(VECTOR->COMPOUND *vec* *tag* *mutable* *opaque* *reserve*)` "
            "creates a compound object out of *vec*, tagged with *tag*. "
            "If *reserve* is #f, the returned compound won't be a sequence",
            kno_vector_type,VOID,-1,VOID,
            -1,KNO_FALSE,-1,KNO_FALSE,-1,KNO_TRUE);
  kno_idefn(kno_scheme_module,
           kno_make_cprim2x("UNPACK-COMPOUND",unpack_compound,1,
                           kno_compound_type,VOID,-1,VOID));

  kno_idefn(kno_scheme_module,
           kno_make_cprim2x("COMPOUND-METATDATA",compound_metadata_prim,1,
                           kno_compound_type,VOID,
                           kno_symbol_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("COMPOUND-CORELEN",compound_corelen_prim,1,
                           -1,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim2x("COMPOUND-SET-CORELEN!",
                           compound_set_corelen_prim,2,
                           -1,VOID,kno_fixnum_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim2x("COMPOUND-SET-CONSFN!",
                           compound_set_consfn_prim,2,
                           -1,VOID,-1,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim2x("COMPOUND-SET-STRINGFN!",
                           compound_set_stringfn_prim,2,
                           -1,VOID,-1,VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
