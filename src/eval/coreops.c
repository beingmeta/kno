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

DEFCPRIM("identical?",identicalp,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(IDENTICAL? *arg0* *arg1*)` "
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

DEFCPRIM("equal?",equalp,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(EQUAL? *arg0* *arg1*)` "
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

DEFCPRIM("compare",comparefn,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(COMPARE *arg0* *arg1*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID},
	 {"y",kno_any_type,KNO_VOID})
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

DEFCPRIM("compare/quick",quickcomparefn,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(COMPARE/QUICK *arg0* *arg1*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID},
	 {"y",kno_any_type,KNO_VOID})
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

DEFCPRIM("deep-copy",deepcopy,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(DEEP-COPY *arg0*)` "
	 "**undocumented**",
	 {"object",kno_any_type,KNO_VOID})
static lispval deepcopy(lispval object)
{
  return kno_deep_copy(object);
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

DEFCPRIM("static-copy",staticcopy,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STATIC-COPY *arg0*)` "
	 "**undocumented**",
	 {"object",kno_any_type,KNO_VOID})
static lispval staticcopy(lispval object)
{
  return kno_static_copy(object);
}


DEFCPRIM("dontopt",dontopt,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDCALL,
	 "`(DONTOPT *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval dontopt(lispval x)
{
  return kno_incref(x);
}

DEFCPRIM("refcount",get_refcount,
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

DEFCPRIM("eq?",eqp,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(EQ? *arg0* *arg1*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID},
	 {"y",kno_any_type,KNO_VOID})
static lispval eqp(lispval x,lispval y)
{
  if (x == y) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("eqv?",eqvp,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(EQV? *arg0* *arg1*)` "
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


DEFCPRIM("overlaps?",overlapsp,
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

DEFCPRIM("contains?",containsp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(CONTAINS? *arg0* *arg1*)` **undocumented**",
	 {"choice",kno_any_type,KNO_VOID},
	 {"item",kno_any_type,KNO_VOID});
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

DEFCPRIM("zero?",lisp_zerop,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ZERO? *arg0*)` "
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


DEFCPRIMN("<",lt,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	  "`(< *arg0* *arg1* *args...*)` "
	  "**undocumented**")
static lispval lt(int n,kno_argvec v) { return do_compare(n,v,ltspec); }

DEFCPRIMN("<=",lte,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	  "`(<= *arg0* *arg1* *args...*)` "
	  "**undocumented**")
static lispval lte(int n,kno_argvec v) { return do_compare(n,v,ltespec); }

DEFCPRIMN("=",numeqp,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	  "`(= *arg0* *arg1* *args...*)` "
	  "**undocumented**")
static lispval numeqp(int n,kno_argvec v) { return do_compare(n,v,espec); }

DEFCPRIMN(">=",gte,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	  "`(>= *arg0* *arg1* *args...*)` "
	  "**undocumented**")
static lispval gte(int n,kno_argvec v) { return do_compare(n,v,gtespec); }

DEFCPRIMN(">",gt,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	  "`(> *arg0* *arg1* *args...*)` "
	  "**undocumented**")
static lispval gt(int n,kno_argvec v) { return do_compare(n,v,gtspec); }

DEFCPRIMN("!=",numneqp,
	  KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	  "`(!= *arg0* *arg1* *args...*)` "
	  "**undocumented**")
static lispval numneqp(int n,kno_argvec v) { return do_compare(n,v,nespec); }

/* Type predicates */


DEFCPRIM("string?",stringp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STRING? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval stringp(lispval x)
{
  if (STRINGP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("valid-utf8?",valid_utf8p,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(VALID-UTF8? *arg0*)` "
	 "**undocumented**",
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

DEFCPRIM("regex?",regexp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(REGEX? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval regexp(lispval x)
{
  if (KNO_REGEXP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFCPRIM("packet?",packetp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PACKET? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval packetp(lispval x)
{
  if (PACKETP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("secret?",secretp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SECRET? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval secretp(lispval x)
{
  if (TYPEP(x,kno_secret_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("symbol?",symbolp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SYMBOL? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval symbolp(lispval x)
{
  if (SYMBOLP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("pair?",pairp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PAIR? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval pairp(lispval x)
{
  if (PAIRP(x)) return KNO_TRUE; else return KNO_FALSE;
}

DEFCPRIM("list?",listp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LIST? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval listp(lispval x)
{
  if (NILP(x)) return KNO_TRUE;
  else if (PAIRP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("proper-list?",proper_listp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PROPER-LIST? *arg0*)` "
	 "**undocumented**",
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
	 "`(VECTOR? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval vectorp(lispval x)
{
  if (VECTORP(x)) return KNO_TRUE; else return KNO_FALSE;
}


DEFCPRIM("number?",numberp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(NUMBER? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval numberp(lispval x)
{
  if (NUMBERP(x)) return KNO_TRUE; else return KNO_FALSE;
}


DEFCPRIM("flonum?",flonump,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FLONUM? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval flonump(lispval x)
{
  if (KNO_FLONUMP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFCPRIM("nan?",isnanp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(NAN? *arg0*)` "
	 "**undocumented**",
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
	 "`(IMMEDIATE? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval immediatep(lispval x)
{
  if (KNO_IMMEDIATEP(x)) return KNO_TRUE; else return KNO_FALSE;
}


DEFCPRIM("consed?",consp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(CONSED? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval consp(lispval x)
{
  if (CONSP(x)) return KNO_TRUE; else return KNO_FALSE;
}


DEFCPRIM("static?",staticp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STATIC? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval staticp(lispval x)
{
  if (KNO_STATICP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFCPRIM("character?",characterp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(CHARACTER? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval characterp(lispval x)
{
  if (KNO_CHARACTERP(x)) return KNO_TRUE; else return KNO_FALSE;
}


DEFCPRIM("exception?",exceptionp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXCEPTION? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval exceptionp(lispval x)
{
  if (KNO_EXCEPTIONP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFCPRIM("applicable?",applicablep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(APPLICABLE? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval applicablep(lispval x)
{
  if (KNO_APPLICABLEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFCPRIM("fcnid?",fcnidp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FCNID? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval fcnidp(lispval x)
{
  if (KNO_FCNIDP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFCPRIM("boolean?",booleanp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(BOOLEAN? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval booleanp(lispval x)
{
  if ((KNO_TRUEP(x)) || (FALSEP(x)))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFCPRIM("true?",truep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(TRUE? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval truep(lispval x)
{
  if (FALSEP(x))
    return KNO_FALSE;
  else return KNO_TRUE;
}


DEFCPRIM("false?",falsep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FALSE? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval falsep(lispval x)
{
  if (FALSEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFCPRIM("typeof",typeof_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(TYPEOF *arg0*)` "
	 "**undocumented**",
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
	 "`(TAGGED? *arg0* [*arg1*])` "
	 "**undocumented**",
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
	 "**undocumented**",
	 {"items",kno_any_type,KNO_VOID},
	 {"types",kno_type_type,KNO_VOID})
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
	 "**undocumented**",
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

DEFCPRIM("intern",lisp_intern,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INTERN *arg0*)` "
	 "**undocumented**",
	 {"symbol_name",kno_any_type,KNO_VOID})
static lispval lisp_intern(lispval symbol_name)
{
  if (STRINGP(symbol_name))
    return kno_intern(CSTRING(symbol_name));
  else return kno_type_error("string","lisp_intern",symbol_name);
}

/* Pair functions */


DEFCPRIM("empty-list?",nullp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EMPTY-LIST? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval nullp(lispval x)
{
  if (NILP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}


DEFCPRIM("cons",cons_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "Returns a CONS pair composed of *car* and *cdr*",
	 {"car",kno_any_type,KNO_VOID},
	 {"cdr",kno_any_type,KNO_VOID})
static lispval cons_prim(lispval car,lispval cdr)
{
  return kno_make_pair(car,cdr);
}

DEFCPRIM("car",car_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the first, left-hand element of a pair",
	 {"pair",kno_pair_type,KNO_VOID})
static lispval car_prim(lispval pair)
{
  return kno_incref(KNO_CAR(pair));
}

DEFCPRIM("cdr",cdr_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the second, right-hand element of a pair",
	 {"pair",kno_pair_type,KNO_VOID})
static lispval cdr_prim(lispval pair)
{
  return kno_incref(KNO_CDR(pair));
}

DEFCPRIM("push",push_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(push *head* [*tail*])` "
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

DEFCPRIM("qcons",qcons_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	 "`(QCONS *car* *cdr*)`"
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


DEFCPRIM("allsymbols",lisp_all_symbols,
	 KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	 "`(ALLSYMBOLS)` "
	 "**undocumented**")
static lispval lisp_all_symbols()
{
  return kno_all_symbols();
}


DEFCPRIM("string->lisp",lisp_string2lisp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STRING->LISP *arg0*)` "
	 "**undocumented**",
	 {"string",kno_any_type,KNO_VOID})
static lispval lisp_string2lisp(lispval string)
{
  if (STRINGP(string))
    return kno_parse(CSTRING(string));
  else return kno_type_error("string","lisp_string2lisp",string);
}


DEFCPRIM("lisp->string",lisp2string,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LISP->STRING *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval lisp2string(lispval x)
{
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  kno_unparse(&out,x);
  return kno_stream2string(&out);
}


DEFCPRIM("->lisp",lisp_tolisp,
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


DEFCPRIM("parse-arg",lisp_parse_arg,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PARSE-ARG *string*)` "
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


DEFCPRIM("parse-slotid",lisp_parse_slotid,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PARSE-SLOTID *string*)` "
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


DEFCPRIM("unparse-arg",lisp_unparse_arg,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(UNPARSE-ARG *arg0*)` "
	 "**undocumented**",
	 {"obj",kno_any_type,KNO_VOID})
static lispval lisp_unparse_arg(lispval obj)
{
  u8_string s = kno_unparse_arg(obj);
  return kno_wrapstring(s);
}


DEFCPRIM("symbol->string",lisp_symbol2string,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SYMBOL->STRING *arg0*)` "
	 "**undocumented**",
	 {"sym",kno_symbol_type,KNO_VOID})
static lispval lisp_symbol2string(lispval sym)
{
  return kno_mkstring(SYM_NAME(sym));
}


DEFCPRIM("string->symbol",lisp_string2symbol,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(STRING->SYMBOL *arg0*)` "
	 "**undocumented**",
	 {"s",kno_string_type,KNO_VOID})
static lispval lisp_string2symbol(lispval s)
{
  return kno_intern(KNO_STRING_DATA(s));
}


DEFCPRIM("getsym",lisp_getsym,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(getsym *arg*)` "
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


DEFCPRIM("getslotid",lisp_getslotid,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(getslot *arg*)` "
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


DEFCPRIM("hashptr",hashptr_prim,
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


DEFCPRIM("hashref",hashref_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(HASHREF *arg0*)` "
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


DEFCPRIM("ptrlock",ptrlock_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(PTRLOCK *ptr* [*mod*])` "
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

/* Rawptr ops */

DEFCPRIM("rawptr?",rawptrp_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(rawptr? *obj* [*type*])` "
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
	 "`(rawptr/type *obj*)` "
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
	 "`(rawptr/id *obj*)` "
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

/* GETSOURCEIDS */

static void add_sourceid(u8_string s,void *vp)
{
  lispval *valp = (lispval *)vp;
  lispval string = knostring(s);
  *valp = kno_init_pair(NULL,string,*valp);
}

DEFCPRIM("getsourceinfo",lisp_getsourceids,
	 KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	 "Returns all the registered sources for this session")
static lispval lisp_getsourceids()
{
  lispval result = NIL;
  u8_for_source_files(add_sourceid,&result);
  return result;
}

/* Force some errors */


DEFCPRIM("segfault",force_sigsegv,
	 KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	 "Signals a segmentation fault")
static lispval force_sigsegv()
{
  lispval *values = NULL;
  kno_incref(values[3]);
  return values[3];
}

DEFCPRIM("fperror",force_sigfpe,
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
  KNO_LINK_CPRIM("lisp->string",lisp2string,1,scheme_module);
  KNO_LINK_CPRIM("string->lisp",lisp_string2lisp,1,scheme_module);
  KNO_LINK_CPRIM("allsymbols",lisp_all_symbols,0,scheme_module);
  KNO_LINK_CPRIM("intern",lisp_intern,1,scheme_module);
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
  KNO_LINK_CPRIM("static?",staticp,1,scheme_module);
  KNO_LINK_CPRIM("consed?",consp,1,scheme_module);
  KNO_LINK_CPRIM("immediate?",immediatep,1,scheme_module);
  KNO_LINK_CPRIM("nan?",isnanp,1,scheme_module);
  KNO_LINK_CPRIM("flonum?",flonump,1,scheme_module);
  KNO_LINK_CPRIM("number?",numberp,1,scheme_module);
  KNO_LINK_CPRIM("vector?",vectorp,1,scheme_module);
  KNO_LINK_CPRIM("proper-list?",proper_listp,1,scheme_module);
  KNO_LINK_CPRIM("list?",listp,1,scheme_module);
  KNO_LINK_CPRIM("pair?",pairp,1,scheme_module);
  KNO_LINK_CPRIM("symbol?",symbolp,1,scheme_module);
  KNO_LINK_CPRIM("secret?",secretp,1,scheme_module);
  KNO_LINK_CPRIM("packet?",packetp,1,scheme_module);
  KNO_LINK_CPRIM("regex?",regexp,1,scheme_module);
  KNO_LINK_CPRIM("valid-utf8?",valid_utf8p,1,scheme_module);
  KNO_LINK_CPRIM("string?",stringp,1,scheme_module);
  KNO_LINK_CVARARGS("!=",numneqp,scheme_module);
  KNO_LINK_CVARARGS(">",gt,scheme_module);
  KNO_LINK_CVARARGS(">=",gte,scheme_module);
  KNO_LINK_CVARARGS("=",numeqp,scheme_module);
  KNO_LINK_CVARARGS("<=",lte,scheme_module);
  KNO_LINK_CVARARGS("<",lt,scheme_module);
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

  KNO_LINK_ALIAS("cons?",consp,scheme_module);
  KNO_LINK_ALIAS("=?",identicalp,scheme_module);
  KNO_LINK_ALIAS("*=?",overlapsp,scheme_module);
  KNO_LINK_ALIAS("⊆?",overlapsp,scheme_module);
  KNO_LINK_ALIAS("⊆",overlapsp,scheme_module);
  KNO_LINK_ALIAS("char?",characterp,scheme_module);
  KNO_LINK_ALIAS("error?",exceptionp,scheme_module);
  KNO_LINK_ALIAS("typep",hastypep_prim,scheme_module);

  KNO_LINK_CPRIM("cons",cons_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("cdr",cdr_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("car",car_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("push",push_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("qcons",qcons_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("empty-list?",nullp,1,kno_scheme_module);
  KNO_LINK_ALIAS("null?",nullp,kno_scheme_module);
  KNO_LINK_ALIAS("nil?",nullp,kno_scheme_module);

  KNO_LINK_CPRIM("rawptr?",rawptrp_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("rawptr/type",rawptr_type_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("rawptr/id",rawptr_id_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("rawptr/notes",rawptr_notes_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("rawptr/id!",rawptr_setid_prim,2,kno_scheme_module);
}
