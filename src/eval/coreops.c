/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

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

DEFPRIM2("identical?",identicalp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(IDENTICAL? *arg0* *arg1*)` is the non-deterministic version "
	 "of EQUAL? and returns true if its arguments (which can be "
	 "choices) have the same structure and elements.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval identicalp(lispval x,lispval y)
{
  if (KNO_EQ(x,y)) return KNO_TRUE;
  else if (LISP_EQUAL(x,y)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM2("equal?",equalp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(EQUAL? *arg0* *arg1*)` returns true if its arguments "
	 "have the same structure and elements. If its arguments are "
	 "choices, this compares all pairings and may return true, false, "
	 "or both.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
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
DEFPRIM2("compare",comparefn,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(COMPARE *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval comparefn(lispval x,lispval y)
{
  int n = LISP_COMPARE(x,y,KNO_COMPARE_FULL);
  return KNO_INT(n);
}

/***FDDOC[2]** SCHEME COMPARE/QUICK
 * *x* an object
 * *y* an object

 Returns -1, 0, or 1 depending on a variant of the natural 'sort
 order' of *x* and *y*. For example, '(COMPARE 4 3)' returns 0,
 '(COMPARE 3 4)' returns -1, and '(COMPARE 4 4)' returns 0.

 This is often faster than 'COMPARE' by returning a consistent sort order
 with varies from the **natural** sort order in several ways:
 * sequences are compared by length first; shorter sequences always precede
 longer sequences;
 * numbers of different implementation types are compared based on their typecodes;
 * for custom types, this passes a third argument of 1 (for quick) to the
 'kno_comparefn' found in 'kno_comparators[*typecode*]'.

 All other objects (especially custom types without compare methods),
 are compared based on their integer pointer values.

*/
DEFPRIM2("compare/quick",quickcomparefn,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(COMPARE/QUICK *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval quickcomparefn(lispval x,lispval y)
{
  int n = KNO_QCOMPARE(x,y);
  return KNO_INT(n);
}

/***FDDOC[2]** SCHEME DEEP-COPY
 * *x* an object

 Returns a recursive copy of *x* where all consed objects (and their
 consed descendants) are reallocated if possible.

 Custom objects are duplicated using the 'kno_copyfn' handler in
 'kno_copiers[*typecode*]. These methods have the option of simply
 incrementing the reference count rather than copying the pointers.

*/
DEFPRIM1("deep-copy",deepcopy,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(DEEP-COPY *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval deepcopy(lispval x)
{
  return kno_deep_copy(x);
}

/***FDDOC[2]** SCHEME STATIC-COPY
 * *x* an object

 Returns a recursive copy of *x* where all consed objects (and their
 consed descendants) are reallocated if possible. In addition, the
 newly consed objects are declared *static* meaning they are exempt
 from garbage collection. This will probably cause leaks but can
 sometimes improve performance.

 Custom objects are duplicated using the 'kno_copyfn' handler in
 'kno_copiers[*typecode*]. These methods have the option of simply
 incrementing the reference count rather than copying the pointers.

 If the custom copier declines to return a new object, the existing
 object will not be delcared static.

*/
DEFPRIM1("static-copy",staticcopy,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STATIC-COPY *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval staticcopy(lispval x)
{
  return kno_static_copy(x);
}

DEFPRIM1("dontopt",dontopt,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "`(DONTOPT *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval dontopt(lispval x)
{
  return kno_incref(x);
}

/***FDDOC[2]** SCHEME REFCOUNT
 * *x* an object

 Returns the number of references in the current application to the
 designated object. If the object isn't a CONS, it returns #f. This
 returns 0 for all *static* conses which are exempt from reference
 counting or garbage collection.

*/
DEFPRIM2("refcount",get_refcount,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "`(REFCOUNT *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_CPP_INT(0));
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

/***FDDOC[2]** SCHEME EQ?
 * *x* an object
 * *y* an object

 Returns #t if x and y are the exact same object (if their pointers
 are the same).

 Note that because

*/
DEFPRIM2("eq?",eqp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(EQ? *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval eqp(lispval x,lispval y)
{
  if (x == y) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM2("eqv?",eqvp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(EQV? *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval eqvp(lispval x,lispval y)
{
  if (x == y) return KNO_TRUE;
  else if ((NUMBERP(x)) && (NUMBERP(y)))
    if (kno_numcompare(x,y)==0)
      return KNO_TRUE;
    else return KNO_FALSE;
  else return KNO_FALSE;
}

DEFPRIM2("overlaps?",overlapsp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(OVERLAPS? *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval overlapsp(lispval x,lispval y)
{
  if (EMPTYP(x)) return KNO_FALSE;
  else if (x == y) return KNO_TRUE;
  else if (EMPTYP(y)) return KNO_FALSE;
  else if (kno_overlapp(x,y)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM2("contains?",containsp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(CONTAINS? *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval containsp(lispval x,lispval y)
{
  if (EMPTYP(x)) return KNO_FALSE;
  else if (x == y) return KNO_TRUE;
  else if (EMPTYP(y)) return KNO_FALSE;
  else if (kno_containsp(x,y)) return KNO_TRUE;
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

DEFPRIM1("zero?",lisp_zerop,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ZERO? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
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

static lispval do_compare(int n,lispval *v,int testspec[3])
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

DEFPRIM("<",lt,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(< *arg0* *arg1* *args...*)` **undocumented**");
static lispval lt(int n,lispval *v) { return do_compare(n,v,ltspec); }
DEFPRIM("<=",lte,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(<= *arg0* *arg1* *args...*)` **undocumented**");
static lispval lte(int n,lispval *v) { return do_compare(n,v,ltespec); }
DEFPRIM("=",numeqp,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(= *arg0* *arg1* *args...*)` **undocumented**");
static lispval numeqp(int n,lispval *v) { return do_compare(n,v,espec); }
DEFPRIM(">=",gte,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(>= *arg0* *arg1* *args...*)` **undocumented**");
static lispval gte(int n,lispval *v) { return do_compare(n,v,gtespec); }
DEFPRIM(">",gt,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(> *arg0* *arg1* *args...*)` **undocumented**");
static lispval gt(int n,lispval *v) { return do_compare(n,v,gtspec); }
DEFPRIM("!=",numneqp,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	"`(!= *arg0* *arg1* *args...*)` **undocumented**");
static lispval numneqp(int n,lispval *v) { return do_compare(n,v,nespec); }

/* Type predicates */

DEFPRIM1("string?",stringp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STRING? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval stringp(lispval x)
{
  if (STRINGP(x)) return KNO_TRUE; else return KNO_FALSE;
}
DEFPRIM1("valid-utf8?",valid_utf8p,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(VALID-UTF8? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval valid_utf8p(lispval x)
{
  if ( (STRINGP(x)) || (KNO_TYPEP(x,kno_packet_type)) ) {
    int rv = u8_validp(KNO_CSTRING(x));
    if (rv)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}
DEFPRIM1("regex?",regexp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(REGEX? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval regexp(lispval x)
{
  if (KNO_REGEXP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("packet?",packetp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PACKET? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval packetp(lispval x)
{
  if (PACKETP(x)) return KNO_TRUE; else return KNO_FALSE;
}
DEFPRIM1("secret?",secretp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SECRET? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval secretp(lispval x)
{
  if (TYPEP(x,kno_secret_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}
DEFPRIM1("symbol?",symbolp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SYMBOL? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval symbolp(lispval x)
{
  if (SYMBOLP(x)) return KNO_TRUE; else return KNO_FALSE;
}
DEFPRIM1("pair?",pairp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PAIR? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval pairp(lispval x)
{
  if (PAIRP(x)) return KNO_TRUE; else return KNO_FALSE;
}
DEFPRIM1("list?",listp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LIST? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval listp(lispval x)
{
  if (NILP(x)) return KNO_TRUE;
  else if (PAIRP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}
DEFPRIM1("proper-list?",proper_listp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PROPER-LIST? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
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
DEFPRIM1("vector?",vectorp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(VECTOR? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval vectorp(lispval x)
{
  if (VECTORP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFPRIM1("number?",numberp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(NUMBER? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval numberp(lispval x)
{
  if (NUMBERP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFPRIM1("flonum?",flonump,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FLONUM? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval flonump(lispval x)
{
  if (KNO_FLONUMP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("nan?",isnanp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(NAN? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
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

DEFPRIM1("immediate?",immediatep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(IMMEDIATE? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval immediatep(lispval x)
{
  if (KNO_IMMEDIATEP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFPRIM1("consed?",consp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(CONSED? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval consp(lispval x)
{
  if (CONSP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFPRIM1("static?",staticp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STATIC? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval staticp(lispval x)
{
  if (KNO_STATICP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("character?",characterp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(CHARACTER? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval characterp(lispval x)
{
  if (KNO_CHARACTERP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFPRIM1("exception?",exceptionp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXCEPTION? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval exceptionp(lispval x)
{
  if (KNO_EXCEPTIONP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("applicable?",applicablep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(APPLICABLE? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval applicablep(lispval x)
{
  if (KNO_APPLICABLEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("fcnid?",fcnidp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FCNID? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval fcnidp(lispval x)
{
  if (KNO_FCNIDP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("boolean?",booleanp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(BOOLEAN? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval booleanp(lispval x)
{
  if ((KNO_TRUEP(x)) || (FALSEP(x)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("true?",truep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(TRUE? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval truep(lispval x)
{
  if (FALSEP(x))
    return KNO_FALSE;
  else return KNO_TRUE;
}

DEFPRIM1("false?",falsep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FALSE? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval falsep(lispval x)
{
  if (FALSEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("typeof",typeof_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(TYPEOF *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval typeof_prim(lispval x)
{
  kno_lisp_type t = KNO_PRIM_TYPE(x);
  if (kno_type_names[t])
    return kno_mkstring(kno_type_names[t]);
  else return kno_mkstring("unknown");
}

DEFPRIM2("tagged?",taggedp_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(TAGGED? *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
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

DEFPRIM1("intern",lisp_intern,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INTERN *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisp_intern(lispval symbol_name)
{
  if (STRINGP(symbol_name))
    return kno_intern(CSTRING(symbol_name));
  else return kno_type_error("string","lisp_intern",symbol_name);
}

/* Pair functions */

DEFPRIM1("empty-list?",nullp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EMPTY-LIST? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval nullp(lispval x)
{
  if (NILP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM2("cons",cons,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(CONS *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval cons(lispval x,lispval y)
{
  return kno_make_pair(x,y);
}
DEFPRIM1("car",car,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(CAR *arg0*)` **undocumented**",
	 kno_pair_type,KNO_VOID);
static lispval car(lispval x)
{
  return kno_incref(KNO_CAR(x));
}
DEFPRIM1("cdr",cdr,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(CDR *arg0*)` **undocumented**",
	 kno_pair_type,KNO_VOID);
static lispval cdr(lispval x)
{
  return kno_incref(KNO_CDR(x));
}
DEFPRIM2("push",push,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(push *head* [*tail*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval push(lispval x,lispval y)
{
  if ( (KNO_VOIDP(y)) || (KNO_NILP(y) ) || (KNO_DEFAULTP(y) ) )
    return kno_init_pair(NULL,kno_incref(x),KNO_EMPTY_LIST);
  else if (KNO_PAIRP(y))
    return kno_init_pair(NULL,kno_incref(x),kno_incref(y));
  else return kno_init_pair
	 (NULL,kno_incref(x),kno_init_pair(NULL,kno_incref(y),KNO_EMPTY_LIST));
}

DEFPRIM("allsymbols",lisp_all_symbols,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"`(ALLSYMBOLS)` **undocumented**");
static lispval lisp_all_symbols()
{
  return kno_all_symbols();
}

DEFPRIM1("string->lisp",lisp_string2lisp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STRING->LISP *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisp_string2lisp(lispval string)
{
  if (STRINGP(string))
    return kno_parse(CSTRING(string));
  else return kno_type_error("string","lisp_string2lisp",string);
}

DEFPRIM1("lisp->string",lisp2string,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LISP->STRING *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisp2string(lispval x)
{
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  kno_unparse(&out,x);
  return kno_stream2string(&out);
}

DEFPRIM1("->lisp",lisp_tolisp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Converts strings to lisp objects and just returns "
	 "other objects. Strings which start with lisp "
	 "prefix characters are parsed as lisp objects, and "
	 "strings are made into uppercase symbols when they "
	 "either start with a single quote or contain no "
	 "whitespace and are < 64 bytes. When parsing "
	 "generates an error, the string is returned as a "
	 "string object",
	 kno_any_type,KNO_VOID);
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

DEFPRIM1("parse-arg",lisp_parse_arg,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PARSE-ARG *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisp_parse_arg(lispval string)
{
  if (STRINGP(string))
    return kno_parse_arg(CSTRING(string));
  else return kno_incref(string);
}

DEFPRIM1("unparse-arg",lisp_unparse_arg,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(UNPARSE-ARG *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval lisp_unparse_arg(lispval obj)
{
  u8_string s = kno_unparse_arg(obj);
  return kno_wrapstring(s);
}

DEFPRIM1("symbol->string",lisp_symbol2string,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SYMBOL->STRING *arg0*)` **undocumented**",
	 kno_symbol_type,KNO_VOID);
static lispval lisp_symbol2string(lispval sym)
{
  return kno_mkstring(SYM_NAME(sym));
}

DEFPRIM1("string->symbol",lisp_string2symbol,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STRING->SYMBOL *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval lisp_string2symbol(lispval s)
{
  return kno_intern(KNO_STRING_DATA(s));
}

/* HASHPTR */

DEFPRIM1("hashptr",hashptr_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "Returns an integer representation for its "
	 "argument.",
	 kno_any_type,KNO_VOID);
static lispval hashptr_prim(lispval x)
{
  unsigned long long intval = (unsigned long long)x;
  if (intval<KNO_MAX_FIXNUM)
    return KNO_INT2FIX(intval);
  else return (lispval)kno_ulong_long_to_bigint(intval);
}

DEFPRIM1("hashref",hashref_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(HASHREF *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval hashref_prim(lispval x)
{
  unsigned long long intval = (unsigned long long)x;
  char buf[40], numbuf[32];
  strcpy(buf,"#!");
  strcat(buf,u8_uitoa16(intval,numbuf));
  return kno_make_string(NULL,-1,buf);
}

DEFPRIM2("ptrlock",ptrlock_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(PTRLOCK *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
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

DEFPRIM("getsourceinfo",lisp_getsourceids,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"`(GETSOURCEINFO)` **undocumented**");
static lispval lisp_getsourceids()
{
  lispval result = NIL;
  u8_for_source_files(add_sourceid,&result);
  return result;
}

/* Force some errors */

DEFPRIM("segfault",force_sigsegv,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"`(SEGFAULT)` **undocumented**");
static lispval force_sigsegv()
{
  lispval *values = NULL;
  kno_incref(values[3]);
  return values[3];
}

DEFPRIM("fperror",force_sigfpe,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"`(FPERROR)` **undocumented**");
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

  KNO_LINK_PRIM("fperror",force_sigfpe,0,scheme_module);
  KNO_LINK_PRIM("segfault",force_sigsegv,0,scheme_module);
  KNO_LINK_PRIM("getsourceinfo",lisp_getsourceids,0,scheme_module);
  KNO_LINK_PRIM("ptrlock",ptrlock_prim,2,scheme_module);
  KNO_LINK_PRIM("hashref",hashref_prim,1,scheme_module);
  KNO_LINK_PRIM("hashptr",hashptr_prim,1,scheme_module);
  KNO_LINK_PRIM("string->symbol",lisp_string2symbol,1,scheme_module);
  KNO_LINK_PRIM("symbol->string",lisp_symbol2string,1,scheme_module);
  KNO_LINK_PRIM("unparse-arg",lisp_unparse_arg,1,scheme_module);
  KNO_LINK_PRIM("parse-arg",lisp_parse_arg,1,scheme_module);
  KNO_LINK_PRIM("->lisp",lisp_tolisp,1,scheme_module);
  KNO_LINK_PRIM("lisp->string",lisp2string,1,scheme_module);
  KNO_LINK_PRIM("string->lisp",lisp_string2lisp,1,scheme_module);
  KNO_LINK_PRIM("allsymbols",lisp_all_symbols,0,scheme_module);
  KNO_LINK_PRIM("intern",lisp_intern,1,scheme_module);
  KNO_LINK_PRIM("tagged?",taggedp_prim,2,scheme_module);
  KNO_LINK_PRIM("typeof",typeof_prim,1,scheme_module);
  KNO_LINK_PRIM("false?",falsep,1,scheme_module);
  KNO_LINK_PRIM("true?",truep,1,scheme_module);
  KNO_LINK_PRIM("boolean?",booleanp,1,scheme_module);
  KNO_LINK_PRIM("fcnid?",fcnidp,1,scheme_module);
  KNO_LINK_PRIM("applicable?",applicablep,1,scheme_module);
  KNO_LINK_PRIM("exception?",exceptionp,1,scheme_module);
  KNO_LINK_PRIM("character?",characterp,1,scheme_module);
  KNO_LINK_PRIM("static?",staticp,1,scheme_module);
  KNO_LINK_PRIM("consed?",consp,1,scheme_module);
  KNO_LINK_PRIM("immediate?",immediatep,1,scheme_module);
  KNO_LINK_PRIM("nan?",isnanp,1,scheme_module);
  KNO_LINK_PRIM("flonum?",flonump,1,scheme_module);
  KNO_LINK_PRIM("number?",numberp,1,scheme_module);
  KNO_LINK_PRIM("vector?",vectorp,1,scheme_module);
  KNO_LINK_PRIM("proper-list?",proper_listp,1,scheme_module);
  KNO_LINK_PRIM("list?",listp,1,scheme_module);
  KNO_LINK_PRIM("pair?",pairp,1,scheme_module);
  KNO_LINK_PRIM("symbol?",symbolp,1,scheme_module);
  KNO_LINK_PRIM("secret?",secretp,1,scheme_module);
  KNO_LINK_PRIM("packet?",packetp,1,scheme_module);
  KNO_LINK_PRIM("regex?",regexp,1,scheme_module);
  KNO_LINK_PRIM("valid-utf8?",valid_utf8p,1,scheme_module);
  KNO_LINK_PRIM("string?",stringp,1,scheme_module);
  KNO_LINK_VARARGS("!=",numneqp,scheme_module);
  KNO_LINK_VARARGS(">",gt,scheme_module);
  KNO_LINK_VARARGS(">=",gte,scheme_module);
  KNO_LINK_VARARGS("=",numeqp,scheme_module);
  KNO_LINK_VARARGS("<=",lte,scheme_module);
  KNO_LINK_VARARGS("<",lt,scheme_module);
  KNO_LINK_PRIM("zero?",lisp_zerop,1,scheme_module);
  KNO_LINK_PRIM("contains?",containsp,2,scheme_module);
  KNO_LINK_PRIM("overlaps?",overlapsp,2,scheme_module);
  KNO_LINK_PRIM("eqv?",eqvp,2,scheme_module);
  KNO_LINK_PRIM("eq?",eqp,2,scheme_module);
  KNO_LINK_PRIM("refcount",get_refcount,2,scheme_module);
  KNO_LINK_PRIM("dontopt",dontopt,1,scheme_module);
  KNO_LINK_PRIM("static-copy",staticcopy,1,scheme_module);
  KNO_LINK_PRIM("deep-copy",deepcopy,1,scheme_module);
  KNO_LINK_PRIM("compare/quick",quickcomparefn,2,scheme_module);
  KNO_LINK_PRIM("compare",comparefn,2,scheme_module);
  KNO_LINK_PRIM("equal?",equalp,2,scheme_module);
  KNO_LINK_PRIM("identical?",identicalp,2,scheme_module);

  KNO_LINK_ALIAS("cons?",consp,scheme_module);
  KNO_LINK_ALIAS("=?",identicalp,scheme_module);
  KNO_LINK_ALIAS("*=?",overlapsp,scheme_module);
  KNO_LINK_ALIAS("⊆?",containsp,scheme_module);
  KNO_LINK_ALIAS("⊆",containsp,scheme_module);
  KNO_LINK_ALIAS("char?",characterp,scheme_module);
  KNO_LINK_ALIAS("error?",exceptionp,scheme_module);

  KNO_LINK_PRIM("cons",cons,2,kno_scheme_module);
  KNO_LINK_PRIM("cdr",cdr,1,kno_scheme_module);
  KNO_LINK_PRIM("car",car,1,kno_scheme_module);
  KNO_LINK_PRIM("push",push,2,kno_scheme_module);
  KNO_LINK_PRIM("empty-list?",nullp,1,kno_scheme_module);
  KNO_LINK_ALIAS("null?",nullp,kno_scheme_module);
  KNO_LINK_ALIAS("nil?",nullp,kno_scheme_module);
}
