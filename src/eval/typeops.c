/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_OBJTYPE 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/support.h"
#include "kno/eval.h"
#include "kno/ports.h"

#include "kno/cprims.h"

#include <errno.h>
#include <math.h>

static lispval choice_prim, choice_fcnid, push_prim, push_fcnid;
static lispval plus_prim, plus_fcnid, minus_prim, minus_fcnid, minus_prim;
static lispval plusone_prim, plusone_fcnid, minusone_prim, minusone_fcnid;
static lispval difference_prim, difference_fcnid;

DEF_KNOSYM(consfn); DEF_KNOSYM(stringfn);
DEF_KNOSYM(dumpfn); DEF_KNOSYM(restorefn);
DEF_KNOSYM(compound); DEF_KNOSYM(annotated); DEF_KNOSYM(sequence);
DEF_KNOSYM(mutable); DEF_KNOSYM(opaque);

/* Direct type predicates */

DEFCPRIM("regex?",regexp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a regular expression object",
	 {"x",kno_any_type,KNO_VOID})
static lispval regexp(lispval x)
{
  if (KNO_REGEXP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("packet?",packetp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a packet (a fixed length byte vector)",
	 {"x",kno_any_type,KNO_VOID})
static lispval packetp(lispval x)
{
  if (PACKETP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("secret?",secretp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a secret value, i.e. a fixed length byte "
	 "vector whose data is not to be displayed",
	 {"x",kno_any_type,KNO_VOID})
static lispval secretp(lispval x)
{
  if (TYPEP(x,kno_secret_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("symbol?",symbolp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a symbol",
	 {"x",kno_any_type,KNO_VOID})
static lispval symbolp(lispval x)
{
  if (SYMBOLP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("pair?",pairp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a LISP pair",
	 {"x",kno_any_type,KNO_VOID})
static lispval pairp(lispval x)
{
  if (PAIRP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("list?",listp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a LISP pair or NIL ()",
	 {"x",kno_any_type,KNO_VOID})
static lispval listp(lispval x)
{
  if (NILP(x)) return KNO_TRUE;
  else if (PAIRP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("proper-list?",proper_listp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a proper list, either NIL () "
	 "or the head of a chain of pairs ending in NIL ().",
	 {"x",kno_any_type,KNO_VOID})
static lispval proper_listp(lispval x)
{
  if (NILP(x)) return KNO_TRUE;
  else if (PAIRP(x)) {
    lispval scan = x;
    while (PAIRP(scan)) scan = KNO_CDR(scan);
    if (NILP(scan)) return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFCPRIM("vector?",vectorp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a LISP vector",
	 {"x",kno_any_type,KNO_VOID})
static lispval vectorp(lispval x)
{
  if (VECTORP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("number?",numberp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a number",
	 {"x",kno_any_type,KNO_VOID})
static lispval numberp(lispval x)
{
  if (NUMBERP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("flonum?",flonump,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a flonum (a real inexact value)",
	 {"x",kno_any_type,KNO_VOID})
static lispval flonump(lispval x)
{
  if (KNO_FLONUMP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("nan?",isnanp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is *not* a number, including invalid flonums",
	 {"x",kno_any_type,KNO_VOID})
static lispval isnanp(lispval x)
{
  if (KNO_FLONUMP(x)) {
    double d = KNO_FLONUM(x);
    if (fpclassify(d) != FP_NORMAL)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (NUMBERP(x))
    return KNO_FALSE;
  else return KNO_TRUE;
}

DEFCPRIM("immediate?",immediatep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a LISP immediate pointer. "
	 "Immediate pointers do not reference allocated memory and exclude "
	 "fixnums (small(ish) integers) and OIDs",
	 {"x",kno_any_type,KNO_VOID})
static lispval immediatep(lispval x)
{
  if (KNO_IMMEDIATEP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("consed?",consp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a 'cons', a pointer to allocated memory",
	 {"x",kno_any_type,KNO_VOID})
static lispval consp(lispval x)
{
  if (CONSP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("static?",staticp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a 'static' cons, a pointer to "
	 "allocated memory which is not subject to automatic recycling.",
	 {"x",kno_any_type,KNO_VOID})
static lispval staticp(lispval x)
{
  if (KNO_STATICP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("character?",characterp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a character object",
	 {"x",kno_any_type,KNO_VOID})
static lispval characterp(lispval x)
{
  if (KNO_CHARACTERP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("ctype?",ctypep_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a primitive CTYPE, declared by "
	 "either KNO itself or a loaded module.",
	 {"x",kno_any_type,KNO_VOID})
static lispval ctypep_prim(lispval x)
{
  if (KNO_TYPEP(x,kno_ctype_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("exception?",exceptionp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is an exception object describing "
	 "an unusual condition which happened previously. Exceptions "
	 "are generated during error handling.",
	 {"x",kno_any_type,KNO_VOID})
static lispval exceptionp(lispval x)
{
  if (KNO_EXCEPTIONP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("applicable?",applicablep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is applicable",
	 {"x",kno_any_type,KNO_VOID})
static lispval applicablep(lispval x)
{
  if (KNO_APPLICABLEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("fcnid?",fcnidp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a function ID, an immediate value "
	 "which refers to a function",
	 {"x",kno_any_type,KNO_VOID})
static lispval fcnidp(lispval x)
{
  if (KNO_FCNIDP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("boolean?",booleanp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a truth token (#t or #f currently)",
	 {"x",kno_any_type,KNO_VOID})
static lispval booleanp(lispval x)
{
  if ((KNO_TRUEP(x)) || (FALSEP(x)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("true?",truep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is not #f or {} (the failure value)",
	 {"x",kno_any_type,KNO_VOID})
static lispval truep(lispval x)
{
  if (FALSEP(x))
    return KNO_FALSE;
  else return KNO_TRUE;
}

DEFCPRIM("false?",falsep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is #f or {} (the failure value)",
	 {"x",kno_any_type,KNO_VOID})
static lispval falsep(lispval x)
{
  if (FALSEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("string?",stringp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a string",
	 {"x",kno_any_type,KNO_VOID})
static lispval stringp(lispval x)
{
  if (STRINGP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("valid-utf8?",valid_utf8p,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is a valid UTF-8 string. If *x "
	 "is a string, this should always be true though "
	 "it might not be true for strings of questional provenance. "
	 "if *x* is a packet, it returns true if the packet is a "
	 "well-formed UTF-8 representation.",
	 {"x",kno_any_type,KNO_VOID})
static lispval valid_utf8p(lispval x)
{
  if ( (STRINGP(x)) || (KNO_TYPEP(x,kno_packet_type)) ) {
    int rv = u8_validp(KNO_CSTRING(x));
    if (rv)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFCPRIM("empty-list?",nullp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *x* is the empty list.",
	 {"x",kno_any_type,KNO_VOID})
static lispval nullp(lispval x)
{
  if (NILP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("slotid?",slotidp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if *arg* is an OID or a symbol (a slotid)",
	 {"arg",kno_any_type,KNO_VOID})
static lispval slotidp(lispval arg)
{
  if ((OIDP(arg)) || (SYMBOLP(arg))) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("typeof",typeof_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the name of the basic C type of *x*",
	 {"x",kno_any_type,KNO_VOID})
static lispval typeof_prim(lispval x)
{
  kno_lisp_type t = KNO_PRIM_TYPE(x);
  if (kno_type_names[t])
    return kno_mkstring(kno_type_names[t]);
  else return kno_mkstring("unknown");
}

DEFCPRIM("tagged?",taggedp_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Returns false if *x* is not a compound or "
	 "a wrapped pointer (a raw type). If *tag* is provided, "
	 "`tagged?` returns false if the typetag of *x* "
	 "is not *tag*",
	 {"x",kno_any_type,KNO_VOID},
	 {"tag",kno_any_type,KNO_VOID})
static lispval taggedp_prim(lispval x,lispval tag)
{
  kno_lisp_type typecode = KNO_PRIM_TYPE(x);
  if (typecode == kno_compound_type)
    if (VOIDP(tag))
      return KNO_TRUE;
    else if (tag == KNO_COMPOUND_TAG(x))
      return KNO_TRUE;
    else return KNO_FALSE;
  else if (typecode == kno_rawptr_type) {
    kno_rawptr raw = (kno_rawptr) x;
    if (VOIDP(tag))
      return KNO_TRUE;
    else if (tag == raw->typetag)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFCPRIM("hastype?",hastypep_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "Returns true if any of *items* has any of *types*. "
	 "This *will not* signal an error for non-type values in *types*.",
	 {"items",kno_any_type,KNO_VOID},
	 {"types",kno_any_type,KNO_VOID})
static lispval hastypep_prim(lispval items,lispval types)
{
  if (KNO_EMPTYP(types))
    return KNO_FALSE;
  else if (KNO_EMPTYP(items))
    return KNO_FALSE;
  else if (KNO_CHOICEP(items)) {
    if (KNO_CHOICEP(types)) {
      KNO_DO_CHOICES(item,items) {
	KNO_DO_CHOICES(typeval,types) {
	  if (KNO_CHECKTYPE(item,typeval))
	    return KNO_TRUE;}}
      return KNO_FALSE;}
    else {
      KNO_DO_CHOICES(item,items) {
	if (KNO_CHECKTYPE(item,types))
	  return KNO_TRUE;}
      return KNO_FALSE;}}
  else if (KNO_CHOICEP(types)) {
    KNO_DO_CHOICES(typeval,types) {
      if (KNO_CHECKTYPE(items,typeval))
	return KNO_TRUE;}
    return KNO_FALSE;}
  else if (KNO_CHECKTYPE(items,types))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("only-hastype?",only_hastypep_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "Returns true if all of *items* have any of the types *types*. "
	 "This *will not* signal an error for non-type values in *types*.",
	 {"items",kno_any_type,KNO_VOID},
	 {"types",kno_type_type,KNO_VOID})
static lispval only_hastypep_prim(lispval items,lispval types)
{
  if (KNO_EMPTYP(types))
    return KNO_FALSE;
  else if (KNO_EMPTYP(items))
    return KNO_FALSE;
  else if (KNO_CHOICEP(items)) {
    if (KNO_CHOICEP(types)) {
      KNO_DO_CHOICES(item,items) {
	int matched = 0;
	KNO_DO_CHOICES(typeval,types) {
	  if (KNO_CHECKTYPE(item,typeval)) {
	    matched=0; KNO_STOP_DO_CHOICES; break;}}
	if (!(matched)) return KNO_FALSE;}
      return KNO_TRUE;}
    else {
      KNO_DO_CHOICES(item,items) {
	if (!(KNO_CHECKTYPE(item,types)))
	  return KNO_FALSE;}
      return KNO_TRUE;}}
  else if (KNO_CHOICEP(types)) {
    KNO_DO_CHOICES(typeval,types) {
      if (!(KNO_CHECKTYPE(items,typeval)))
	return KNO_FALSE;}
    return KNO_TRUE;}
  else if (KNO_CHECKTYPE(items,types))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Rawptr ops */

DEFCPRIM("rawptr?",rawptrp_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "returns true if *obj* is a raw pointer object "
	 "with a typetag of *type* (if provided).",
	 {"x",kno_any_type,KNO_VOID},
	 {"tag",kno_any_type,KNO_VOID})
static lispval rawptrp_prim(lispval x,lispval tag)
{
  if (! (KNO_TYPEP(x,kno_rawptr_type)) )
    return KNO_FALSE;
  if ( (KNO_VOIDP(tag)) || (KNO_DEFAULTP(tag)) )
    return KNO_TRUE;
  struct KNO_RAWPTR *obj = (kno_rawptr) x;
  if (obj->typetag == tag)
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("rawptr/type",rawptr_type_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "returns the type tag for *obj* if it is a raw "
	 "pointer object.",
	 {"x",kno_rawptr_type,KNO_VOID})
static lispval rawptr_type_prim(lispval x)
{
  struct KNO_RAWPTR *obj = (kno_rawptr) x;
  return kno_incref(obj->typetag);
}

DEFCPRIM("rawptr/id",rawptr_id_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "returns the idstring for the raw pointer *rawptr* if "
	 "it is a raw pointer object.",
	 {"rawptr",kno_rawptr_type,KNO_VOID})
static lispval rawptr_id_prim(lispval rawptr)
{
  struct KNO_RAWPTR *obj = (kno_rawptr) rawptr;
  if (obj->idstring)
    return knostring(obj->idstring);
  else return KNO_FALSE;
}

DEFCPRIM("rawptr/notes",rawptr_notes_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the notes object for the raw pointer "
	 "*rawptr* if it is a raw pointer object.",
	 {"rawptr",kno_rawptr_type,KNO_VOID})
static lispval rawptr_notes_prim(lispval rawptr)
{
  struct KNO_RAWPTR *obj = (kno_rawptr) rawptr;
  return kno_incref(obj->raw_annotations);
}

DEFCPRIM("rawptr/id!",rawptr_setid_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Sets the idstring of the raw pointer *rawptr*, "
	 "returning it",
	 {"rawptr",kno_rawptr_type,KNO_VOID},
	 {"id",kno_string_type,KNO_VOID})
static lispval rawptr_setid_prim(lispval rawptr,lispval id)
{
  struct KNO_RAWPTR *obj = (kno_rawptr) rawptr;
  u8_string oldid = obj->idstring;
  obj->idstring=u8_strdup(KNO_CSTRING(id));
  u8_free(oldid);
  return kno_incref(rawptr);
}

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
	 "Returns the typetag of a compound object",
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
	 "Returns the number of elements in the compound object *x*",
	 {"x",kno_compound_type,KNO_VOID})
static lispval compound_length(lispval x)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)x;
  return KNO_BYTE2LISP(compound->compound_length);
}

DEFCPRIM("compound-mutable?",compound_mutablep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns true if the compound *x* is mutable, i.e. if individual "
	 "elements in the object can be changed. Note that this enables "
	 "an object-specific lock whihch can slow multi-threaded access.",
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
	 "Returns true if the compound object *x* is opaque.",
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
	 "Returns the *offset* element of the compound *x*. This signals an error "
	 "if *tag* is provided and doesn't match the tagtype of *x*",
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
	 "Returns a pair of the typetag of *x* and "
	 "a vector of its elements.",
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
	 "Sets the *offset* element of the compound *x* to *value*. "
	 "This signals an error if *tag* is provided and doesn't match the "
	 "tagtype of *x*",
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
	  "Creates a (possibly) complex compound object with "
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
	  "`(make-opaque-compound *tag* *elts...*)` creates an opaque "
	  "compound object with typetag *tag*. and comprised "
	  "of the specified elements.")
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
	  "`(make-mutable-compound *tag* *elts...*)` creates a mutable "
	  "compound object with typetag *tag*. and comprised "
	  "of the specified elements.")
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
	  "`(make-opsque-mutable-compound *tag* *elts...*)` creates an opaque mutable "
	  "compound object with typetag *tag*. and comprised "
	  "of the specified elements.")
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

/* Getting typeinfo */

static lispval typeinfo_helper(lispval type,int probe)
{
  if (KNO_TYPEP(type,kno_typeinfo_type))
    return kno_incref(type);
  else if ( (KNO_OIDP(type)) || (KNO_SYMBOLP(type)) ) {
    struct KNO_TYPEINFO *info = (probe) ? (kno_probe_typeinfo(type)) :
      (kno_use_typeinfo(type));
    if (info) return (lispval) info;
    else return KNO_FALSE;}
  else if (KNO_TAGGEDP(type)) {
    struct KNO_TAGGED *tagged = (kno_tagged) type;
    if (tagged->typeinfo)
      return kno_incref((lispval)tagged->typeinfo);
    lispval tag = tagged->typetag;
    struct KNO_TYPEINFO *info = (probe) ? (kno_probe_typeinfo(tag)) :
      (kno_use_typeinfo(tag));
    if (info) return (lispval) info;
    else return KNO_FALSE;}
  else if (KNO_TYPEP(type,kno_ctype_type)) {
    struct KNO_TYPEINFO *info = (probe) ? (kno_probe_typeinfo(type)) :
      (kno_use_typeinfo(type));
    if (info) return (lispval) info;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFCPRIM("kno/type",type_cprim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "returns the typeinfo object for a type object or an object's type. "
	 "This creates the typeinfo object if it is not currently defined.",
	 {"obj",kno_any_type,KNO_VOID})
static lispval type_cprim(lispval type)
{
  return typeinfo_helper(type,0);
}

DEFCPRIM("kno/typeinfo/probe",typeinfo_probe_cprim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "returns the typeinfo object for a type object or an object's type. "
	 "This does not create typeinfo for unregistered types.",
	 {"obj",kno_any_type,KNO_VOID})
static lispval typeinfo_probe_cprim(lispval type)
{
  return typeinfo_helper(type,1);
}

/* Typed function dispatch */

KNO_DEFCPRIMNx("kno/dispatch",dispatch_cprim,
	       KNO_VAR_ARGS|KNO_MIN_ARGS(2)|KNO_NDCALL,
	       "Applies a type specific method *method* to *object* "
	       "with additional args.",2,
	       {"object",kno_any_type,KNO_VOID},
	       {"method",kno_slotid_type,KNO_VOID})
static lispval dispatch_cprim(int n,kno_argvec args)
{
  lispval objects = args[0], methods = args[1];
  if (KNO_CHOICEP(methods)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(object,objects) {
      KNO_DO_CHOICES(method,methods) {
	lispval result = kno_dispatch(object,method,n-2,args+2);
	if (KNO_ABORTED(result)) {
	  KNO_STOP_DO_CHOICES;
	  kno_decref(results);
	  return result;}
	else {KNO_ADD_TO_CHOICE(results,result);}}}
    return results;}
  else if (KNO_CHOICEP(objects)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(object,objects) {
      lispval result = kno_dispatch(object,methods,n-2,args+2);
      if (KNO_ABORTED(result)) {
	KNO_STOP_DO_CHOICES;
	kno_decref(results);
	return result;}
      else {KNO_ADD_TO_CHOICE(results,result);}}
    return results;}
  return kno_dispatch(args[0],args[1],n-2,args+2);
}

KNO_DEFCPRIM("kno/handles?",handlesp_cprim,
	     KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	     "Returns true if *object* handles *message*",
	     {"object",kno_any_type,KNO_VOID},
	     {"message",kno_slotid_type,KNO_VOID})
static lispval handlesp_cprim(lispval object,lispval message)
{
  struct KNO_TYPEINFO *typeinfo = kno_objtype(object);
  if (typeinfo == NULL)
    return KNO_FALSE;
  else {
    lispval handler = kno_get(typeinfo->type_props,message,KNO_VOID);
    if (KNO_APPLICABLEP(handler)) {
      kno_decref(handler);
      return KNO_TRUE;}
    else return KNO_FALSE;}
}

KNO_DEFCPRIM("kno/set-handler!",set_handler_cprim,
	     KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	     "Sets a handler",
	     {"type",kno_type_type,KNO_VOID},
	     {"handler",kno_applicable_type,KNO_VOID},
	     {"message",kno_slotid_type,KNO_VOID})
static lispval set_handler_cprim(lispval type,lispval message,lispval handler)
{
  struct KNO_TYPEINFO *typeinfo = kno_use_typeinfo(type);
  if (typeinfo == NULL) {
    kno_seterr("BadTypeArg","set_handler_cprim",NULL,type);
    return KNO_ERROR;}
  else if (KNO_VOIDP(message)) {
    if (KNO_FUNCTIONP(handler)) {
      kno_function f = (kno_function) handler;
      if (f->fcn_name) message = kno_intern(f->fcn_name);}
    if (KNO_VOIDP(message))
      return kno_err("NoMessageName","set_handler_cprim",NULL,handler);}
  int rv = kno_store(typeinfo->type_props,message,handler);
  if (rv<0) return KNO_ERROR;
  else return kno_incref(handler);
}

/* Vestiges from earlier type systems */

/* Type/method operations */

DEFCPRIM("type-set-consfn!",type_set_consfn_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Sets the function used to create an instance "
	 "of types tagged *tag*. This is mainly used by "
	 "the parser for expressions of the form\n"
	 "  #%(tag data...)",
	 {"tag",kno_any_type,KNO_VOID},
	 {"consfn",kno_any_type,KNO_VOID})
static lispval type_set_consfn_prim(lispval tag,lispval consfn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(consfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_drop(e->type_props,KNOSYM(consfn),VOID);
      return VOID;}
    else if (KNO_APPLICABLEP(consfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_store(e->type_props,KNOSYM(consfn),consfn);
      return VOID;}
    else return kno_type_error("applicable","set_compound_consfn_prim",tag);
  else return kno_type_error("compound tag","set_compound_consfn_prim",tag);
}

DEFCPRIM("type-set-restorefn!",type_set_restorefn_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Sets the function for 'restoring' objects saved by a corresponding "
	 "dumpfn",
	 {"tag",kno_any_type,KNO_VOID},
	 {"restorefn",kno_any_type,KNO_VOID})
static lispval type_set_restorefn_prim(lispval tag,lispval restorefn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(restorefn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_drop(e->type_props,KNOSYM(restorefn),VOID);
      return VOID;}
    else if (KNO_APPLICABLEP(restorefn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_store(e->type_props,KNOSYM(restorefn),restorefn);
      return VOID;}
    else return kno_type_error("applicable","set_compound_restorefn_prim",tag);
  else return kno_type_error("compound tag","set_compound_restorefn_prim",tag);
}

DEFCPRIM("type-set-dumpfn!",type_set_dumpfn_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Sets the function for 'dumping' objects of type *tag*.",
	 {"tag",kno_any_type,KNO_VOID},
	 {"dumpfn",kno_any_type,KNO_VOID})
static lispval type_set_dumpfn_prim(lispval tag,lispval dumpfn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(dumpfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_drop(e->type_props,KNOSYM(dumpfn),VOID);
      return VOID;}
    else if (KNO_APPLICABLEP(dumpfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_store(e->type_props,KNOSYM(dumpfn),dumpfn);
      return VOID;}
    else return kno_type_error("applicable","set_compound_dumpfn_prim",tag);
  else return kno_type_error("compound tag","set_compound_dumpfn_prim",tag);
}

DEFCPRIM("type-set-stringfn!",type_set_stringfn_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Sets the function for unparsing objects of type *tag*. *stringfn* "
	 "should return a string which is emitted as the printed representation "
	 "of the object tagged with *tag*. If *stringfn* is #f, any specified "
	 "stringfn is removed.",
	 {"tag",kno_any_type,KNO_VOID},
	 {"stringfn",kno_any_type,KNO_VOID})
static lispval type_set_stringfn_prim(lispval tag,lispval stringfn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(stringfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_drop(e->type_props,stringfn_symbol,VOID);
      return VOID;}
    else if (KNO_APPLICABLEP(stringfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_store(e->type_props,stringfn_symbol,stringfn);
      return VOID;}
    else return kno_type_error("applicable","set_type_stringfn_prim",tag);
  else return kno_type_error("type tag","set_type_stringfn_prim",tag);
}

DEFCPRIM("type-props",type_props_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "accesses the metadata associated with the typetag "
	 "assigned to *compound*. If *field* is specified, "
	 "that particular metadata field is returned. "
	 "Otherwise the entire metadata object (a slotmap) "
	 "is copied and returned.",
	 {"arg",kno_any_type,KNO_VOID},
	 {"field",kno_slotid_type,KNO_VOID})
static lispval type_props_prim(lispval arg,lispval field)
{
  struct KNO_TYPEINFO *e =
    (KNO_TAGGEDP(arg)) ? (kno_objtype(arg)) : (kno_use_typeinfo(arg));;
  if (VOIDP(field))
    return kno_deep_copy(e->type_props);
  else return kno_get(e->type_props,field,EMPTY);
}

static lispval opaque_symbol, mutable_symbol, sequence_symbol;

DEFCPRIM("type-set!",type_set_prim,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3)|KNO_NDCALL,
	 "stores *value* in *field* of the properties "
	 "associated with the type of tag *tag*.",
	 {"arg",kno_any_type,KNO_VOID},
	 {"field",kno_slotid_type,KNO_VOID},
	 {"value",kno_any_type,KNO_VOID})
static lispval type_set_prim(lispval arg,lispval field,lispval value)
{
  struct KNO_TYPEINFO *e =
    (KNO_TAGGEDP(arg)) ? (kno_objtype(arg)) : (kno_use_typeinfo(arg));;
  int rv = kno_store(e->type_props,field,value);
  if (rv < 0)
    return KNO_ERROR;
  if (field == KNOSYM(compound) ) {
    e->type_isopaque = (kno_overlapp(value,KNOSYM(opaque)));
    e->type_ismutable = (kno_overlapp(value,KNOSYM(mutable)));
    e->type_istable = (kno_overlapp(value,KNOSYM(annotated)));
    e->type_issequence = (kno_overlapp(value,KNOSYM(sequence)));}
  if (rv)
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Setting various compound properties */

static lispval consfn_symbol, stringfn_symbol, tag_symbol;

/* Initializing common functions */

KNO_EXPORT void kno_init_typeops_c()
{
  u8_register_source_file(_FILEINFO);

  tag_symbol = kno_intern("tag");

  KNOSYM(stringfn);
  KNOSYM(consfn);
  KNOSYM(compound);
  KNOSYM(dumpfn);
  KNOSYM(restorefn);
  KNOSYM(annotated);
  KNOSYM(sequence);
  KNOSYM(mutable);
  KNOSYM(opaque);

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

  KNO_LINK_CPRIM("tagged?",taggedp_prim,2,scheme_module);
  KNO_LINK_CPRIM("hastype?",hastypep_prim,2,scheme_module);
  KNO_LINK_CPRIM("typeof",typeof_prim,1,scheme_module);
  KNO_LINK_CPRIM("only-type?",only_hastypep_prim,2,scheme_module);
  KNO_LINK_CPRIM("false?",falsep,1,scheme_module);
  KNO_LINK_CPRIM("true?",truep,1,scheme_module);
  KNO_LINK_CPRIM("boolean?",booleanp,1,scheme_module);
  KNO_LINK_CPRIM("fcnid?",fcnidp,1,scheme_module);
  KNO_LINK_CPRIM("applicable?",applicablep,1,scheme_module);
  KNO_LINK_CPRIM("exception?",exceptionp,1,scheme_module);
  KNO_LINK_CPRIM("character?",characterp,1,scheme_module);
  KNO_LINK_CPRIM("ctype?",ctypep_prim,1,scheme_module);
  KNO_LINK_CPRIM("static?",staticp,1,scheme_module);
  KNO_LINK_CPRIM("consed?",consp,1,scheme_module);
  KNO_LINK_CPRIM("immediate?",immediatep,1,scheme_module);
  KNO_LINK_CPRIM("nan?",isnanp,1,scheme_module);
  KNO_LINK_CPRIM("flonum?",flonump,1,scheme_module);
  KNO_LINK_CPRIM("number?",numberp,1,scheme_module);
  KNO_LINK_CPRIM("vector?",vectorp,1,scheme_module);
  KNO_LINK_CPRIM("slotid?",slotidp,1,kno_db_module);
  KNO_LINK_CPRIM("proper-list?",proper_listp,1,scheme_module);
  KNO_LINK_CPRIM("list?",listp,1,scheme_module);
  KNO_LINK_CPRIM("pair?",pairp,1,scheme_module);
  KNO_LINK_CPRIM("symbol?",symbolp,1,scheme_module);
  KNO_LINK_CPRIM("secret?",secretp,1,scheme_module);
  KNO_LINK_CPRIM("packet?",packetp,1,scheme_module);
  KNO_LINK_CPRIM("regex?",regexp,1,scheme_module);
  KNO_LINK_CPRIM("valid-utf8?",valid_utf8p,1,scheme_module);
  KNO_LINK_CPRIM("string?",stringp,1,scheme_module);
  KNO_LINK_CPRIM("empty-list?",nullp,1,kno_scheme_module);
  KNO_LINK_ALIAS("null?",nullp,kno_scheme_module);
  KNO_LINK_ALIAS("nil?",nullp,kno_scheme_module);
  KNO_LINK_ALIAS("cons?",consp,scheme_module);
  KNO_LINK_ALIAS("char?",characterp,scheme_module);
  KNO_LINK_ALIAS("error?",exceptionp,scheme_module);
  KNO_LINK_ALIAS("ctype?",ctypep_prim,scheme_module);
  KNO_LINK_ALIAS("typep",hastypep_prim,scheme_module);

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

  KNO_LINK_CPRIM("kno/type",type_cprim,1,kno_scheme_module);
  KNO_LINK_CPRIM("kno/typeinfo/probe",typeinfo_probe_cprim,1,kno_scheme_module);
  KNO_LINK_ALIAS("kno/typeinfo",type_cprim,kno_scheme_module);

  KNO_LINK_CPRIM("kno/handles?",handlesp_cprim,2,kno_scheme_module);
  KNO_LINK_ALIAS("handles?",handlesp_cprim,scheme_module);

  KNO_LINK_CPRIM("kno/set-handler!",set_handler_cprim,3,kno_scheme_module);
  KNO_LINK_ALIAS("kno/handler!",set_handler_cprim,scheme_module);
  KNO_LINK_ALIAS("handler!",set_handler_cprim,scheme_module);

  KNO_LINK_CPRIM("type-set-stringfn!",type_set_stringfn_prim,2,kno_scheme_module);
  KNO_LINK_ALIAS("compound-set-stringfn!",type_set_stringfn_prim,kno_scheme_module);
  KNO_LINK_CPRIM("type-set-consfn!",type_set_consfn_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("type-set-restorefn!",type_set_restorefn_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("type-set-dumpfn!",type_set_dumpfn_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("type-set!",type_set_prim,3,kno_scheme_module);
  KNO_LINK_CPRIM("type-props",type_props_prim,2,kno_scheme_module);

  KNO_LINK_CVARARGS("kno/dispatch",dispatch_cprim,scheme_module);

  KNO_LINK_ALIAS("compound-type?",compoundp,scheme_module);
  KNO_LINK_ALIAS("vector->compound",seq2compound,scheme_module);

  KNO_LINK_CPRIM("rawptr?",rawptrp_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("rawptr/type",rawptr_type_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("rawptr/id",rawptr_id_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("rawptr/notes",rawptr_notes_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("rawptr/id!",rawptr_setid_prim,2,kno_scheme_module);
}

