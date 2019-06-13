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

/***FDDOC[2]** SCHEME EQUAL?
 * *x* an object
 * *y* an object

 Returns true if *x* and *y* are the same, based
 on a recursive comparison on all builtin object types
 except for hashtables and hashsets.

 Numeric objects are compared numerically, so '(equal? 0 0.0)'

 'EQUAL?' is not comprehensive, so if called on choices, it may
 return both #t and #f or fail altogether.

 'EQUAL?' calls the lispval_compare to do it's work, returning #t for a
 return value of 0.

 For custom types, this calls the 'kno_comparefn' found in
 'kno_comparators[*typecode*]', which should return -1, 0, or 1.

*/
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
static lispval staticcopy(lispval x)
{
  return kno_static_copy(x);
}

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
static lispval eqp(lispval x,lispval y)
{
  if (x == y) return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval eqvp(lispval x,lispval y)
{
  if (x == y) return KNO_TRUE;
  else if ((NUMBERP(x)) && (NUMBERP(y)))
    if (kno_numcompare(x,y)==0)
      return KNO_TRUE;
    else return KNO_FALSE;
  else return KNO_FALSE;
}

static lispval overlapsp(lispval x,lispval y)
{
  if (EMPTYP(x)) return KNO_FALSE;
  else if (x == y) return KNO_TRUE;
  else if (EMPTYP(y)) return KNO_FALSE;
  else if (kno_overlapp(x,y)) return KNO_TRUE;
  else return KNO_FALSE;
}

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
    if (comparison>1) return KNO_ERROR;
    else if (testspec[comparison+1]) i++;
    else return KNO_FALSE;}
  return KNO_TRUE;
}

static int ltspec[3]={1,0,0}, ltespec[3]={1,1,0}, espec[3]={0,1,0};
static int gtespec[3]={0,1,1}, gtspec[3]={0,0,1}, nespec[3]={1,0,1};

static lispval lt(int n,lispval *v) { return do_compare(n,v,ltspec); }
static lispval lte(int n,lispval *v) { return do_compare(n,v,ltespec); }
static lispval numeqp(int n,lispval *v) { return do_compare(n,v,espec); }
static lispval gte(int n,lispval *v) { return do_compare(n,v,gtespec); }
static lispval gt(int n,lispval *v) { return do_compare(n,v,gtspec); }
static lispval numneqp(int n,lispval *v) { return do_compare(n,v,nespec); }

/* Type predicates */

static lispval stringp(lispval x)
{
  if (STRINGP(x)) return KNO_TRUE; else return KNO_FALSE;
}
static lispval valid_utf8p(lispval x)
{
  if (STRINGP(x)) {
    int rv = u8_validp(KNO_CSTRING(x));
    if (rv)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}
static lispval regexp(lispval x)
{
  if (KNO_REGEXP(x)) return KNO_TRUE; else return KNO_FALSE;
}

static lispval packetp(lispval x)
{
  if (PACKETP(x)) return KNO_TRUE; else return KNO_FALSE;
}
static lispval secretp(lispval x)
{
  if (TYPEP(x,kno_secret_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}
static lispval symbolp(lispval x)
{
  if (SYMBOLP(x)) return KNO_TRUE; else return KNO_FALSE;
}
static lispval pairp(lispval x)
{
  if (PAIRP(x)) return KNO_TRUE; else return KNO_FALSE;
}
static lispval listp(lispval x)
{
  if (NILP(x)) return KNO_TRUE;
  else if (PAIRP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}
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
static lispval vectorp(lispval x)
{
  if (VECTORP(x)) return KNO_TRUE; else return KNO_FALSE;
}

static lispval numberp(lispval x)
{
  if (NUMBERP(x)) return KNO_TRUE; else return KNO_FALSE;
}

static lispval flonump(lispval x)
{
  if (KNO_FLONUMP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval isnanp(lispval x)
{
  if (KNO_FLONUMP(x)) {
    double d = KNO_FLONUM(x);
    if (isnan(d))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (NUMBERP(x))
    return KNO_FALSE;
  else return KNO_TRUE;
}

static lispval immediatep(lispval x)
{
  if (KNO_IMMEDIATEP(x)) return KNO_TRUE; else return KNO_FALSE;
}

static lispval consp(lispval x)
{
  if (CONSP(x)) return KNO_TRUE; else return KNO_FALSE;
}

static lispval staticp(lispval x)
{
  if (KNO_STATICP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval characterp(lispval x)
{
  if (KNO_CHARACTERP(x)) return KNO_TRUE; else return KNO_FALSE;
}

static lispval exceptionp(lispval x)
{
  if (KNO_EXCEPTIONP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval applicablep(lispval x)
{
  if (KNO_APPLICABLEP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval fcnidp(lispval x)
{
  if (KNO_FCNIDP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval opcodep(lispval x)
{
  if (KNO_OPCODEP(x)) return KNO_TRUE; else return KNO_FALSE;
}

static lispval make_opcode(lispval x)
{
  if ( (KNO_UINTP(x)) && ((KNO_FIX2INT(x)) < KNO_IMMEDIATE_MAX) )
    return KNO_OPCODE(FIX2INT(x));
  else return kno_err(kno_RangeError,"make_opcode",NULL,x);
}

static lispval make_coderef(lispval x)
{
  if ( (KNO_UINTP(x)) && ((KNO_FIX2INT(x)) < KNO_IMMEDIATE_MAX) )
    return KNO_OPCODE(FIX2INT(x));
  else return kno_err(kno_RangeError,"make_coderef",NULL,x);
}

static lispval booleanp(lispval x)
{
  if ((KNO_TRUEP(x)) || (FALSEP(x)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval truep(lispval x)
{
  if (FALSEP(x))
    return KNO_FALSE;
  else return KNO_TRUE;
}

static lispval falsep(lispval x)
{
  if (FALSEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval typeof_prim(lispval x)
{
  kno_ptr_type t = KNO_PRIM_TYPE(x);
  if (kno_type_names[t]) return lispval_string(kno_type_names[t]);
  else return lispval_string("??");
}

#define GETEVALFN(x) ((kno_evalfn)(kno_fcnid_ref(x)))
static lispval procedure_name(lispval x)
{
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    if (f->fcn_name)
      return lispval_string(f->fcn_name);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return lispval_string(sf->evalfn_name);
    else return KNO_FALSE;}
  else return kno_type_error(_("function"),"procedure_name",x);
}

static lispval lisp_intern(lispval symbol_name)
{
  if (STRINGP(symbol_name))
    return kno_intern(CSTRING(symbol_name));
  else return kno_type_error("string","lisp_intern",symbol_name);
}

static lispval lisp_all_symbols()
{
  return kno_all_symbols();
}

static lispval lisp_string2lisp(lispval string)
{
  if (STRINGP(string))
    return kno_parse(CSTRING(string));
  else return kno_type_error("string","lisp_string2lisp",string);
}

static lispval lisp2string(lispval x)
{
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  kno_unparse(&out,x);
  return kno_stream2string(&out);
}

static lispval lisp_tolisp(lispval arg)
{
  if (STRINGP(arg)) {
    u8_string string=CSTRING(arg);
    if ( (KNO_STRLEN(arg)>64) || (strchr(string,' ')) )
      return lispval_string(string);
    else {
      u8_string scan=string;
      int c=u8_sgetc(&scan);
      while (*scan) {
        if (u8_isspace(c))
          return lispval_string(string);
        else c=u8_sgetc(&scan);}
      if ( (c>0) && (u8_isspace(c)) )
        return lispval_string(string);
      else return kno_parse(string);}}
  else return kno_incref(arg);
}

static lispval lisp_parse_arg(lispval string)
{
  if (STRINGP(string))
    return kno_parse_arg(CSTRING(string));
  else return kno_incref(string);
}

static lispval lisp_unparse_arg(lispval obj)
{
  u8_string s = kno_unparse_arg(obj);
  return kno_lispstring(s);
}

static lispval lisp_symbol2string(lispval sym)
{
  return lispval_string(SYM_NAME(sym));
}

static lispval lisp_string2symbol(lispval s)
{
  return kno_intern(KNO_STRING_DATA(s));
}

/* HASHPTR */

static lispval hashptr_prim(lispval x)
{
  unsigned long long intval = (unsigned long long)x;
  if (intval<KNO_MAX_FIXNUM)
    return KNO_INT2FIX(intval);
  else return (lispval)kno_ulong_long_to_bigint(intval);
}

static lispval hashref_prim(lispval x)
{
  unsigned long long intval = (unsigned long long)x;
  char buf[40], numbuf[32];
  strcpy(buf,"#!");
  strcat(buf,u8_uitoa16(intval,numbuf));
  return kno_make_string(NULL,-1,buf);
}

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
  *valp = kno_make_pair(lispval_string(s),*valp);
}

static lispval lisp_getsourceids()
{
  lispval result = NIL;
  u8_for_source_files(add_sourceid,&result);
  return result;
}

/* Force some errors */

static lispval force_sigsegv()
{
  lispval *values = NULL;
  kno_incref(values[3]);
  return values[3];
}

static lispval force_sigfpe()
{
  return kno_init_double(NULL,5.0/0.0);
}

/* The init function */

KNO_EXPORT void kno_init_coreprims_c()
{
  u8_register_source_file(_FILEINFO);

  kno_idefn(kno_scheme_module,kno_make_cprim2("EQ?",eqp,2));
  kno_idefn(kno_scheme_module,kno_make_cprim2("EQV?",eqvp,2));
  kno_idefn(kno_scheme_module,kno_make_cprim2("EQUAL?",equalp,2));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2("IDENTICAL?",equalp,2)));
  kno_defalias(kno_scheme_module,"=?","IDENTICAL?");
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2("OVERLAPS?",overlapsp,2)));
  kno_defalias(kno_scheme_module,"*=?","OVERLAPS?");
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2("CONTAINS?",containsp,2)));
  kno_defalias(kno_scheme_module,"⊆?","CONTAINS?");
  kno_defalias(kno_scheme_module,"⊆","CONTAINS?");
  kno_idefn(kno_scheme_module,kno_make_cprim2("COMPARE",comparefn,2));
  kno_idefn(kno_scheme_module,kno_make_cprim2("COMPARE/QUICK",quickcomparefn,2));
  kno_idefn(kno_scheme_module,kno_make_cprim1("DEEP-COPY",deepcopy,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("STATIC-COPY",staticcopy,1));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim1("DONTOPT",dontopt,1)));
  kno_idefn(kno_scheme_module,
           kno_make_ndprim(kno_make_cprim2x("REFCOUNT",get_refcount,1,-1,VOID,
                                          kno_fixnum_type,KNO_INT(0))));

  kno_idefn(kno_scheme_module,kno_make_cprimn("<",lt,2));
  kno_idefn(kno_scheme_module,kno_make_cprimn("<=",lte,2));
  kno_idefn(kno_scheme_module,kno_make_cprimn("=",numeqp,2));
  kno_idefn(kno_scheme_module,kno_make_cprimn(">=",gte,2));
  kno_idefn(kno_scheme_module,kno_make_cprimn(">",gt,2));
  kno_idefn(kno_scheme_module,kno_make_cprimn("!=",numneqp,2));
  kno_idefn(kno_scheme_module,kno_make_cprim1("ZERO?",lisp_zerop,1));

  kno_idefn(kno_scheme_module,kno_make_cprim1("STRING?",stringp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("VALID-UTF8?",valid_utf8p,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("REGEX?",regexp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("PACKET?",packetp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("SECRET?",secretp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("SYMBOL?",symbolp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("PAIR?",pairp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("LIST?",listp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("PROPER-LIST?",proper_listp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("VECTOR?",vectorp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("CHARACTER?",characterp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("OPCODE?",opcodep,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("EXCEPTION?",exceptionp,1));
  kno_defalias(kno_scheme_module,"ERROR?","EXCEPTION?");
  kno_idefn(kno_scheme_module,kno_make_cprim1("APPLICABLE?",applicablep,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("FCNID?",fcnidp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("FLONUM?",flonump,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("NAN?",isnanp,1));

  kno_defalias(kno_scheme_module,"CHAR?","CHARACTER?");
  kno_idefn(kno_scheme_module,kno_make_cprim1("BOOLEAN?",booleanp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("TRUE?",truep,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("FALSE?",falsep,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("NUMBER?",numberp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("IMMEDIATE?",immediatep,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("STATIC?",staticp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("CONS?",consp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("CONSED?",consp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("TYPEOF",typeof_prim,1));

  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("MAKE-OPCODE",make_opcode,1,
                           kno_fixnum_type,VOID));
  kno_idefn1(kno_scheme_module,"-MAKE-CODEREF",make_coderef,1,
            "(CODEREF <fixnum>)\nReturns a coderef object",
            kno_fixnum_type,KNO_VOID);

  kno_idefn(kno_scheme_module,kno_make_cprim1("PROCEDURE-NAME",procedure_name,1));

  kno_idefn(kno_scheme_module,kno_make_cprim1("INTERN",lisp_intern,1));
  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("SYMBOL->STRING",lisp_symbol2string,1,
                           kno_symbol_type,VOID));
  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("STRING->SYMBOL",lisp_string2symbol,1,
                           kno_string_type,VOID));
  kno_idefn1(kno_scheme_module,"->LISP",lisp_tolisp,1,
            "Converts strings to lisp objects and just returns other objects. "
            "Strings which start with lisp prefix characters are parsed as "
            "lisp objects, and strings are made into uppercase symbols when "
            "they either start with a single quote or contain no whitespace "
            "and are < 64 bytes. When parsing generates an error, the string "
            "is returned as a string object",
            -1,KNO_VOID);
  kno_idefn(kno_scheme_module,kno_make_cprim1("STRING->LISP",lisp_string2lisp,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("LISP->STRING",lisp2string,1));

  kno_idefn(kno_scheme_module,kno_make_cprim1("PARSE-ARG",lisp_parse_arg,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("UNPARSE-ARG",lisp_unparse_arg,1));

  kno_idefn(kno_scheme_module,kno_make_cprim0("GETSOURCEINFO",lisp_getsourceids));
  kno_idefn(kno_scheme_module,kno_make_cprim0("ALLSYMBOLS",lisp_all_symbols));

  kno_idefn(kno_scheme_module,kno_make_cprim0("SEGFAULT",force_sigsegv));
  kno_idefn(kno_scheme_module,kno_make_cprim0("FPERROR",force_sigfpe));

  kno_idefn1(kno_scheme_module,"HASHPTR",hashptr_prim,1|KNO_NDCALL,
            "Returns an integer representation for its argument.",
            -1,KNO_VOID);
  kno_idefn(kno_scheme_module,kno_make_cprim1("HASHREF",hashref_prim,1));
  kno_idefn(kno_scheme_module,
           kno_make_cprim2x("PTRLOCK",ptrlock_prim,1,
                           -1,VOID,kno_fixnum_type,VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
