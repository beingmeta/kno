/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/cprims.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

/* Standard predicates */

DEFC_PRIM("identical?",identicalp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "is the non-deterministic version of EQUAL? and "
	  "returns true if its arguments (which can be "
	  "choices) have the same structure and elements.",
	  {"x",kno_any_type,KNO_VOID},
	  {"y",kno_any_type,KNO_VOID})
static lispval identicalp(lispval x,lispval y)
{
  if (KNO_EQ(x,y)) return KNO_TRUE;
  else if (LISP_EQUAL(x,y)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("equal?",equalp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "returns true if its arguments have the same "
	  "structure and elements. If its arguments are "
	  "choices, this compares all pairings and may "
	  "return true, false, or both.",
	  {"x",kno_any_type,KNO_VOID},
	  {"y",kno_any_type,KNO_VOID})
static lispval equalp(lispval x,lispval y)
{
  if (KNO_EQ(x,y)) return KNO_TRUE;
  else if (LISP_EQUAL(x,y)) return KNO_TRUE;
  else return KNO_FALSE;
}

/***FDDOC[2]** SCHEME COMPARE
 * *x* an object
 * *y* an object

 Returns -1, 0, or 1 depending on the natural 'sort order' of *x* and
 *y*.

 This does a recursive descent and returns the first comparision which
 is non-zero. Numeric values are compared numerically by magnitude.
 Other values are compared first by type code.

 Sequences of the same type are compared element by element low to high.

 Tables are compared first by the number of keys, then by the keys
 themselves (in their natural sort order). If they have identical
 keys, then their values are compared following the natural sort order
 of the keys.

 For custom types, this calls the 'kno_comparefn' found in
 'kno_comparators[*typecode*]'.

 All other objects (especially custom types without compare methods),
 are compared based on their integer pointer values.

*/

DEFC_PRIM("compare",comparefn,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Compares the objects *x* and *y*, returning -1, 0, or 1 "
	  "depending on their generic ordering. Note that this function "
	  "is deterministic, so it may produce surprising results for "
	  "choice arguments",
	  {"x",kno_any_type,KNO_VOID},
	  {"y",kno_any_type,KNO_VOID})
static lispval comparefn(lispval x,lispval y)
{
  int n = LISP_COMPARE(x,y,KNO_COMPARE_FULL);
  return KNO_INT(n);
}

DEFC_PRIM("compare/quick",quickcomparefn,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Compares the objects *x* and *y*, returning -1, 0, or 1 "
	  "depending on pointer ordering for atomic objects and generic "
	  "ordering for consed objects",
	  {"x",kno_any_type,KNO_VOID},
	  {"y",kno_any_type,KNO_VOID})
static lispval quickcomparefn(lispval x,lispval y)
{
  int n = KNO_QCOMPARE(x,y);
  return KNO_INT(n);
}

/***FDDOC[2]** SCHEME DEEP-COPY
 * *x* an object


*/

DEFC_PRIM("deep-copy",deepcopy,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a recursive copy of *x* where all consed objects (and their "
	  "consed descendants) are reallocated if possible.\n"
	  "Custom objects are duplicated using the 'kno_copyfn' handler in "
	  "'kno_copiers[*typecode*]. These methods have the option of simply "
	  "incrementing the reference count rather than copying the pointers.",
	  {"object",kno_any_type,KNO_VOID})
static lispval deepcopy(lispval object)
{
  return kno_deep_copy(object);
}

/***FDDOC[2]** SCHEME STATIC-COPY
 * *x* an object


*/

DEFC_PRIM("static-copy",staticcopy,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a recursive copy of *x* where all consed objects (and their "
	  "consed descendants) are reallocated if possible. In addition, the "
	  "newly consed objects are declared *static* meaning they are exempt "
	  "from garbage collection. This will probably cause leaks but can "
	  "sometimes improve performance.\n"
	  "Custom objects are duplicated using the 'kno_copyfn' handler in "
	  "'kno_copiers[*typecode*]. These methods have the option of simply "
	  "incrementing the reference count rather than copying the pointers.\n"
	  "If the custom copier declines to return a new object, the existing "
	  "object will not be declared static. ",
	  {"object",kno_any_type,KNO_VOID})
static lispval staticcopy(lispval object)
{
  return kno_static_copy(object);
}

DEFC_PRIM("dontopt",dontopt,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Tells optimizers and compilers to leave this expression untouched.",
	  {"x",kno_any_type,KNO_VOID})
static lispval dontopt(lispval x)
{
  return kno_incref(x);
}

DEFC_PRIM("refcount",get_refcount,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Returns the reference count for *object* minus *delta* if provided",
	  {"object",kno_any_type,KNO_VOID},
	  {"delta",kno_fixnum_type,KNO_INT(0)})
static lispval get_refcount(lispval x,lispval delta)
{
  if (CONSP(x)) {
    struct KNO_CONS *cons = (struct KNO_CONS *)x;
    int refcount = KNO_CONS_REFCOUNT(cons);
    long long d = FIX2INT(delta);
    if (d<0) d = -d;
    if (refcount<1)
      return kno_reterr("Bad REFCOUNT","get_refcount",
			u8_mkstring("%lx",(unsigned long)x),
			VOID);
    else return KNO_INT((refcount-(d+1)));}
  else return KNO_FALSE;
}

DEFC_PRIM("eq?",eqp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID},
	  {"y",kno_any_type,KNO_VOID})
static lispval eqp(lispval x,lispval y)
{
  if (x == y) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("eqv?",eqvp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID},
	  {"y",kno_any_type,KNO_VOID})
static lispval eqvp(lispval x,lispval y)
{
  if (x == y) return KNO_TRUE;
  else if ((NUMBERP(x)) && (NUMBERP(y)))
    if (kno_numcompare(x,y)==0)
      return KNO_TRUE;
    else return KNO_FALSE;
  else return KNO_FALSE;
}

DEFC_PRIM("overlaps?",overlapsp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Returns #t if *choice1* and *choice2* have any elements in common",
	  {"choice1",kno_any_type,KNO_VOID},
	  {"choice2",kno_any_type,KNO_VOID})
static lispval overlapsp(lispval x,lispval y)
{
  if (EMPTYP(x)) return KNO_FALSE;
  else if (x == y) return KNO_TRUE;
  else if (EMPTYP(y)) return KNO_FALSE;
  else if (kno_overlapp(x,y)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("contains?",containsp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "Returns true if *subset* is a proper subset of *items*.",
	  {"subset",kno_any_type,KNO_VOID},
	  {"items",kno_any_type,KNO_VOID});
static lispval containsp(lispval subset,lispval items)
{
  if (EMPTYP(subset)) return KNO_TRUE;
  else if (subset == items) return KNO_TRUE;
  else if (EMPTYP(items)) return KNO_FALSE;
  else if (kno_containsp(subset,items)) return KNO_TRUE;
  else return KNO_FALSE;
}

static int numeric_compare(const lispval x,const lispval y)
{
  if ((FIXNUMP(x)) && (FIXNUMP(y))) {
    long long ix = FIX2INT(x), iy = FIX2INT(y);
    if (ix>iy) return 1; else if (ix<iy) return -1; else return 0;}
  else if ((KNO_FLONUMP(x)) && (KNO_FLONUMP(y))) {
    double dx = KNO_FLONUM(x), dy = KNO_FLONUM(y);
    if (dx>dy) return 1; else if (dx<dy) return -1; else return 0;}
  else return kno_numcompare(x,y);
}

DEFC_PRIM("zero?",lisp_zerop,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"x",kno_any_type,KNO_VOID})
static lispval lisp_zerop(lispval x)
{
  if (FIXNUMP(x))
    if (FIX2INT(x)==0) return KNO_TRUE; else return KNO_FALSE;
  else if (KNO_FLONUMP(x))
    if (KNO_FLONUM(x)==0.0) return KNO_TRUE; else return KNO_FALSE;
  else if (!(NUMBERP(x)))
    return KNO_FALSE;
  else {
    int cmp = kno_numcompare(x,KNO_INT(0));
    if (cmp==0) return KNO_TRUE;
    else if (cmp>1) return KNO_ERROR;
    else return KNO_FALSE;}
}

static lispval do_compare(int n,kno_argvec v,int testspec[3])
{
  int i = 1; while (i < n) {
    int comparison = numeric_compare(v[i-1],v[i]);
    if (comparison>1)
      return KNO_ERROR;
    else if (testspec[comparison+1])
      i++;
    else return KNO_FALSE;}
  return KNO_TRUE;
}

static int ltspec[3]={1,0,0}, ltespec[3]={1,1,0}, espec[3]={0,1,0};
static int gtespec[3]={0,1,1}, gtspec[3]={0,0,1}, nespec[3]={1,0,1};

DEFC_PRIMN("<",lt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval lt(int n,kno_argvec v) { return do_compare(n,v,ltspec); }

DEFC_PRIMN("<=",lte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval lte(int n,kno_argvec v) { return do_compare(n,v,ltespec); }

DEFC_PRIMN("=",numeqp,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval numeqp(int n,kno_argvec v) { return do_compare(n,v,espec); }

DEFC_PRIMN(">=",gte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval gte(int n,kno_argvec v) { return do_compare(n,v,gtespec); }

DEFC_PRIMN(">",gt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval gt(int n,kno_argvec v) { return do_compare(n,v,gtspec); }

DEFC_PRIMN("!=",numneqp,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval numneqp(int n,kno_argvec v) { return do_compare(n,v,nespec); }

/* Type predicates */

DEFC_PRIM("intern",lisp_intern,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"symbol_name",kno_any_type,KNO_VOID})
static lispval lisp_intern(lispval symbol_name)
{
  if (STRINGP(symbol_name))
    return kno_intern(CSTRING(symbol_name));
  else return kno_type_error("string","lisp_intern",symbol_name);
}

/* Pair functions */

DEFC_PRIM("cons",cons_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "Returns a CONS pair composed of *car* and *cdr*",
	  {"car",kno_any_type,KNO_VOID},
	  {"cdr",kno_any_type,KNO_VOID})
static lispval cons_prim(lispval car,lispval cdr)
{
  return kno_make_pair(car,cdr);
}

DEFC_PRIM("car",car_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the first, left-hand element of a pair",
	  {"pair",kno_pair_type,KNO_VOID})
static lispval car_prim(lispval pair)
{
  return kno_incref(KNO_CAR(pair));
}

DEFC_PRIM("cdr",cdr_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the second, right-hand element of a pair",
	  {"pair",kno_pair_type,KNO_VOID})
static lispval cdr_prim(lispval pair)
{
  return kno_incref(KNO_CDR(pair));
}

DEFC_PRIM("push",push_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"item",kno_any_type,KNO_VOID},
	  {"list",kno_any_type,KNO_VOID})
static lispval push_prim(lispval item,lispval list)
{
  if ( (KNO_VOIDP(list)) || (KNO_NILP(list) ) || (KNO_DEFAULTP(list) ) )
    return kno_init_pair(NULL,kno_incref(item),KNO_EMPTY_LIST);
  else if (KNO_PAIRP(list))
    return kno_init_pair(NULL,kno_incref(item),kno_incref(list));
  else return kno_init_pair
	 (NULL,kno_incref(item),kno_init_pair(NULL,kno_incref(list),KNO_EMPTY_LIST));
}

DEFC_PRIM("qcons",qcons_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "unless *car* or *cdr*are empty (fail), returns a "
	  "single cons pointing to both arguments, which may "
	  "be choices (and stay that way). This would be "
	  "equivalent to `(cons (qc car) (qc cdr))`.",
	  {"car",kno_any_type,KNO_VOID},
	  {"cdr",kno_any_type,KNO_VOID})
static lispval qcons_prim(lispval car,lispval cdr)
{
  if ( (KNO_EMPTYP(car)) || (KNO_EMPTYP(cdr)) )
    return KNO_EMPTY;
  else {
    kno_incref(car); kno_incref(cdr);
    return kno_init_pair(NULL,car,cdr);}
}

DEFC_PRIM("allsymbols",lisp_all_symbols,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "Returns all interned symbols as a choice.")
static lispval lisp_all_symbols()
{
  return kno_all_symbols();
}

/* 'Printed' representations */

DEFC_PRIM("string->lisp",lisp_string2lisp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Parses a text representation (a string) into a LISP object.",
	  {"string",kno_string_type,KNO_VOID})
static lispval lisp_string2lisp(lispval string)
{
  return kno_parse(CSTRING(string));
}

DEFC_PRIM("lisp->string",lisp2string,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns a textual representation for *object*. The "
	  "representation is truncated after *maxlen* bytes of "
	  "UTF-8 representation, if *maxlen* is a positive fixnum.",
	  {"object",kno_any_type,KNO_VOID},
	  {"maxlen",kno_any_type,KNO_VOID})
static lispval lisp2string(lispval object,lispval maxlen)
{
  if (KNO_FIXNUMP(maxlen)) {
    long long len = KNO_FIX2INT(maxlen);
    if (len == 0) return knostring("");
    else if (len > 0) {
      U8_FIXED_OUTPUT(fixed,len);
      kno_unparse(fixedout,object);
      return kno_stream2string(fixedout);}}
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  kno_unparse(&out,object);
  return kno_stream2string(&out);
}

DEFC_PRIM("->lisp",lisp_tolisp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Converts strings to lisp objects and just returns "
	  "other objects. Strings which start with lisp "
	  "prefix characters are parsed as lisp objects, and "
	  "strings are made into uppercase symbols when they "
	  "either start with a single quote or contain no "
	  "whitespace and are < 64 bytes. When parsing "
	  "generates an error, the string is returned as a "
	  "string object",
	  {"arg",kno_any_type,KNO_VOID})
static lispval lisp_tolisp(lispval arg)
{
  if (STRINGP(arg)) {
    u8_string string=CSTRING(arg);
    if ( (KNO_STRLEN(arg)>64) || (strchr(string,' ')) )
      return kno_mkstring(string);
    else {
      u8_string scan=string;
      int c=u8_sgetc(&scan);
      while (*scan) {
	if (u8_isspace(c))
	  return kno_mkstring(string);
	else c=u8_sgetc(&scan);}
      if ( (c>0) && (u8_isspace(c)) )
	return kno_mkstring(string);
      else return kno_parse(string);}}
  else return kno_incref(arg);
}

DEFC_PRIM("parse-arg",lisp_parse_arg,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a LISP object based on *string*.  If "
	  "*string* isn't a string, it is just returned. If "
	  "*string* can be parsed as a number or begins with "
	  "a LISP delimiter, the LISP parser is called on "
	  "the string.If *string* starts with a colon or "
	  "sinqle quote, the LISP parser is called on the "
	  "remainder of the string. In all other cases, the "
	  "*string* is just returned as a lisp string.",
	  {"string",kno_any_type,KNO_VOID})
static lispval lisp_parse_arg(lispval string)
{
  if (STRINGP(string))
    return kno_parse_arg(CSTRING(string));
  else return kno_incref(string);
}

DEFC_PRIM("parse-slotid",lisp_parse_slotid,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a LISP object, typically a slotid (symbol "
	  "or OID) based on *string*. If *string* isn't a "
	  "string, it is just returned. If *string* begins "
	  "with at at-sign @, it is parsed as an OID "
	  "reference; if *string* can be parsed as a number "
	  "or begins with a LISP delimiter, the parser is "
	  "called. If *string* starts with a colon or sinqle "
	  "quote, the parser is called on the remainder of "
	  "the string. If the string contains whitespace, it "
	  "returns a string, otherwise it returns a symbol.",
	  {"string",kno_any_type,KNO_VOID})
static lispval lisp_parse_slotid(lispval string)
{
  if (STRINGP(string))
    return kno_parse_arg(CSTRING(string));
  else return kno_incref(string);
}

DEFC_PRIM("unparse-arg",lisp_unparse_arg,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"obj",kno_any_type,KNO_VOID})
static lispval lisp_unparse_arg(lispval obj)
{
  u8_string s = kno_unparse_arg(obj);
  return kno_wrapstring(s);
}

DEFC_PRIM("symbol->string",lisp_symbol2string,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the string representation of a symbols print name",
	  {"sym",kno_symbol_type,KNO_VOID})
static lispval lisp_symbol2string(lispval sym)
{
  return kno_mkstring(SYM_NAME(sym));
}

DEFC_PRIM("string->symbol",lisp_string2symbol,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns a symbol whose print name is *s*.",
	  {"s",kno_string_type,KNO_VOID})
static lispval lisp_string2symbol(lispval s)
{
  return kno_intern(KNO_STRING_DATA(s));
}

DEFC_PRIM("getsym",lisp_getsym,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "if *arg* is a string, interns a lowercase version "
	  "of it. If *arg* is a symbol, returns it, "
	  "otherwise errors.",
	  {"s",kno_any_type,KNO_VOID})
static lispval lisp_getsym(lispval s)
{
  if (KNO_SYMBOLP(s))
    return s;
  else if (KNO_STRINGP(s))
    return kno_getsym(KNO_STRING_DATA(s));
  else return kno_err("NotStringOrSymbol","lisp_getsym",NULL,s);
}

DEFC_PRIM("getslotid",lisp_getslotid,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "tries to get a slotid (symbol or OID) from *arg*, "
	  "parsing OIDs and interning lowercase strings, "
	  "signalling an error on failure",
	  {"s",kno_any_type,KNO_VOID})
static lispval lisp_getslotid(lispval s)
{
  if ( (KNO_SYMBOLP(s)) || (KNO_OIDP(s)) )
    return s;
  else if (KNO_STRINGP(s)) {
    u8_string str = KNO_CSTRING(s);
    if ( (*str == '@') || ( (*str == ':') && (str[1] == '@') ) ) {
      lispval arg = kno_parse_arg(str);
      if (KNO_OIDP(arg))
	return arg;
      else kno_err("NotASlot","lisp_getsym",NULL,s);}
    else return kno_getsym(str);}
  return kno_err("NotStringOrSymbol","lisp_getsym",NULL,s);
}

/* HASHPTR */

DEFC_PRIM("hashptr",hashptr_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	  "Returns an integer representation for its "
	  "argument.",
	  {"x",kno_any_type,KNO_VOID})
static lispval hashptr_prim(lispval x)
{
  unsigned long long intval = (unsigned long long)x;
  if (intval<KNO_MAX_FIXNUM)
    return KNO_INT2FIX(intval);
  else return (lispval)kno_ulong_long_to_bigint(intval);
}

DEFC_PRIM("hashref",hashref_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a hashpointer string (#!0x...) which can "
	  "be read if hashpointer reading is enabled.",
	  {"x",kno_any_type,KNO_VOID})
static lispval hashref_prim(lispval x)
{
  unsigned long long intval = (unsigned long long)x;
  char buf[40], numbuf[32];
  strcpy(buf,"#!");
  strcat(buf,u8_uitoa16(intval,numbuf));
  return kno_make_string(NULL,-1,buf);
}

DEFC_PRIM("ptrlock",ptrlock_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Returns the integer pointer value of *ptr* modulo "
	  "*mod*.",
	  {"x",kno_any_type,KNO_VOID},
	  {"mod",kno_fixnum_type,KNO_VOID})
static lispval ptrlock_prim(lispval x,lispval mod)
{
  unsigned long long intval = (unsigned long long)x;
  long long int modval = ((VOIDP(mod))?
			  (KNO_N_PTRLOCKS):
			  (FIX2INT(mod)));
  if (modval==0)
    return (lispval)kno_ulong_long_to_bigint(intval);
  else {
    unsigned long long hashval = hashptrval((void *)x,modval);
    return KNO_INT(hashval);}
}

/* GETSOURCEIDS */

static void add_sourceid(u8_string s,void *vp)
{
  lispval *valp = (lispval *)vp;
  lispval string = knostring(s);
  *valp = kno_init_pair(NULL,string,*valp);
}

DEFC_PRIM("getsourceinfo",lisp_getsourceids,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "Returns all the registered sources for this session")
static lispval lisp_getsourceids()
{
  lispval result = NIL;
  u8_for_source_files(add_sourceid,&result);
  return result;
}

/* Force some errors */

DEFC_PRIM("segfault",force_sigsegv,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "Signals a segmentation fault")
static lispval force_sigsegv()
{
  lispval *values = NULL;
  kno_incref(values[3]);
  return values[3];
}

DEFC_PRIM("fperror",force_sigfpe,
	  KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	  "Signals a floating point error")
static lispval force_sigfpe()
{
  return kno_init_double(NULL,5.0/0.0);
}

/* The init function */

KNO_EXPORT void kno_init_coreops_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_CPRIM("fperror",force_sigfpe,0,scheme_module);
  KNO_LINK_CPRIM("segfault",force_sigsegv,0,scheme_module);
  KNO_LINK_CPRIM("getsourceinfo",lisp_getsourceids,0,scheme_module);
  KNO_LINK_CPRIM("ptrlock",ptrlock_prim,2,scheme_module);
  KNO_LINK_CPRIM("hashref",hashref_prim,1,scheme_module);
  KNO_LINK_CPRIM("hashptr",hashptr_prim,1,scheme_module);
  KNO_LINK_CPRIM("string->symbol",lisp_string2symbol,1,scheme_module);
  KNO_LINK_CPRIM("symbol->string",lisp_symbol2string,1,scheme_module);
  KNO_LINK_CPRIM("getsym",lisp_getsym,1,scheme_module);
  KNO_LINK_CPRIM("getslotid",lisp_getslotid,1,scheme_module);
  KNO_LINK_CPRIM("unparse-arg",lisp_unparse_arg,1,scheme_module);
  KNO_LINK_CPRIM("parse-arg",lisp_parse_arg,1,scheme_module);
  KNO_LINK_CPRIM("parse-slotid",lisp_parse_slotid,1,scheme_module);
  KNO_LINK_CPRIM("->lisp",lisp_tolisp,1,scheme_module);
  KNO_LINK_CPRIM("lisp->string",lisp2string,2,scheme_module);
  KNO_LINK_CPRIM("string->lisp",lisp_string2lisp,1,scheme_module);
  KNO_LINK_CPRIM("allsymbols",lisp_all_symbols,0,scheme_module);
  KNO_LINK_CPRIM("intern",lisp_intern,1,scheme_module);
  KNO_LINK_CPRIMN("!=",numneqp,scheme_module);
  KNO_LINK_CPRIMN(">",gt,scheme_module);
  KNO_LINK_CPRIMN(">=",gte,scheme_module);
  KNO_LINK_CPRIMN("=",numeqp,scheme_module);
  KNO_LINK_CPRIMN("<=",lte,scheme_module);
  KNO_LINK_CPRIMN("<",lt,scheme_module);
  KNO_LINK_CPRIM("zero?",lisp_zerop,1,scheme_module);
  KNO_LINK_CPRIM("overlaps?",overlapsp,2,scheme_module);
  KNO_LINK_CPRIM("contains?",containsp,2,scheme_module);
  KNO_LINK_CPRIM("eqv?",eqvp,2,scheme_module);
  KNO_LINK_CPRIM("eq?",eqp,2,scheme_module);
  KNO_LINK_CPRIM("refcount",get_refcount,2,scheme_module);
  KNO_LINK_CPRIM("dontopt",dontopt,1,scheme_module);
  KNO_LINK_CPRIM("static-copy",staticcopy,1,scheme_module);
  KNO_LINK_CPRIM("deep-copy",deepcopy,1,scheme_module);
  KNO_LINK_CPRIM("compare/quick",quickcomparefn,2,scheme_module);
  KNO_LINK_CPRIM("compare",comparefn,2,scheme_module);
  KNO_LINK_CPRIM("equal?",equalp,2,scheme_module);
  KNO_LINK_CPRIM("identical?",identicalp,2,scheme_module);

  KNO_LINK_ALIAS("=?",identicalp,scheme_module);
  KNO_LINK_ALIAS("*=?",overlapsp,scheme_module);
  KNO_LINK_ALIAS("⊆?",overlapsp,scheme_module);
  KNO_LINK_ALIAS("⊆",overlapsp,scheme_module);

  KNO_LINK_CPRIM("cons",cons_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("cdr",cdr_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("car",car_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("push",push_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("qcons",qcons_prim,2,kno_scheme_module);

}
