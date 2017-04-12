/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"
#include "framerd/support.h"

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

 'EQUAL?' calls the fdtype_compare to do it's work, returning #t for a
 return value of 0.

 For custom types, this calls the 'fd_comparefn' found in
 'fd_comparators[*typecode*]', which should return -1, 0, or 1.

*/
static fdtype equalp(fdtype x,fdtype y)
{
  if (FD_EQ(x,y)) return FD_TRUE;
  else if (FDTYPE_EQUAL(x,y)) return FD_TRUE;
  else return FD_FALSE;
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

 For custom types, this calls the 'fd_comparefn' found in
 'fd_comparators[*typecode*]'.

 All other objects (especially custom types without compare methods),
 are compared based on their integer pointer values.

*/
static fdtype comparefn(fdtype x,fdtype y)
{
  int n=FDTYPE_COMPARE(x,y,FD_COMPARE_FULL);
  return FD_INT(n);
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
   'fd_comparefn' found in 'fd_comparators[*typecode*]'.

 All other objects (especially custom types without compare methods),
 are compared based on their integer pointer values.

*/
static fdtype quickcomparefn(fdtype x,fdtype y)
{
  int n=FD_QCOMPARE(x,y);
  return FD_INT(n);
}

/***FDDOC[2]** SCHEME DEEP-COPY
 * *x* an object

 Returns a recursive copy of *x* where all consed objects (and their
 consed descendants) are reallocated if possible.

 Custom objects are duplicated using the 'fd_copyfn' handler in
 'fd_copiers[*typecode*]. These methods have the option of simply
 incrementing the reference count rather than copying the pointers.

*/
static fdtype deepcopy(fdtype x)
{
  return fd_deep_copy(x);
}

/***FDDOC[2]** SCHEME STATIC-COPY
 * *x* an object

 Returns a recursive copy of *x* where all consed objects (and their
 consed descendants) are reallocated if possible. In addition, the
 newly consed objects are declared *static* meaning they are exempt
 from garbage collection. This will probably cause leaks but can
 sometimes improve performance.

 Custom objects are duplicated using the 'fd_copyfn' handler in
 'fd_copiers[*typecode*]. These methods have the option of simply
 incrementing the reference count rather than copying the pointers.

 If the custom copier declines to return a new object, the existing
 object will not be delcared static.

*/
static fdtype staticcopy(fdtype x)
{
  return fd_static_copy(x);
}

static fdtype dontopt(fdtype x)
{
  return fd_incref(x);
}

/***FDDOC[2]** SCHEME REFCOUNT
 * *x* an object

 Returns the number of references in the current application to the
 designated object. If the object isn't a CONS, it returns #f. This
 returns 0 for all *static* conses which are exempt from reference
 counting or garbage collection.

*/
static fdtype get_refcount(fdtype x,fdtype delta)
{
  if (FD_CONSP(x)) {
    struct FD_CONS *cons=(struct FD_CONS *)x;
    int refcount=FD_CONS_REFCOUNT(cons);
    long long d=FD_FIX2INT(delta);
    if (d<0) d=-d;
    if (refcount<1)
      return fd_reterr("Bad REFCOUNT","get_refcount",
                       u8_mkstring("%lx",(unsigned long)x),
                       FD_VOID);
    else return FD_INT((refcount-(d+1)));}
  else return FD_FALSE;
}

/***FDDOC[2]** SCHEME EQ?
 * *x* an object
 * *y* an object

 Returns #t if x and y are the exact same object (if their pointers
 are the same).

 Note that because 

*/
static fdtype eqp(fdtype x,fdtype y)
{
  if (x==y) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype eqvp(fdtype x,fdtype y)
{
  if (x==y) return FD_TRUE;
  else if ((FD_NUMBERP(x)) && (FD_NUMBERP(y)))
    if (fd_numcompare(x,y)==0)
      return FD_TRUE;
    else return FD_FALSE;
  else return FD_FALSE;
}

static fdtype overlapsp(fdtype x,fdtype y)
{
  if (FD_EMPTY_CHOICEP(x)) return FD_FALSE;
  else if (x==y) return FD_TRUE;
  else if (FD_EMPTY_CHOICEP(y)) return FD_FALSE;
  else if (fd_overlapp(x,y)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype containsp(fdtype x,fdtype y)
{
  if (FD_EMPTY_CHOICEP(x)) return FD_FALSE;
  else if (x==y) return FD_TRUE;
  else if (FD_EMPTY_CHOICEP(y)) return FD_FALSE;
  else if (fd_containsp(x,y)) return FD_TRUE;
  else return FD_FALSE;
}

static int numeric_compare(const fdtype x,const fdtype y)
{
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(y))) {
    long long ix=FD_FIX2INT(x), iy=FD_FIX2INT(y);
    if (ix>iy) return 1; else if (ix<iy) return -1; else return 0;}
  else if ((FD_FLONUMP(x)) && (FD_FLONUMP(y))) {
    double dx=FD_FLONUM(x), dy=FD_FLONUM(y);
    if (dx>dy) return 1; else if (dx<dy) return -1; else return 0;}
  else return fd_numcompare(x,y);
}

static fdtype lisp_zerop(fdtype x)
{
  if (FD_FIXNUMP(x))
    if (FD_FIX2INT(x)==0) return FD_TRUE; else return FD_FALSE;
  else if (FD_FLONUMP(x))
    if (FD_FLONUM(x)==0.0) return FD_TRUE; else return FD_FALSE;
  else if (!(FD_NUMBERP(x)))
    return FD_FALSE;
  else {
    int cmp=fd_numcompare(x,FD_INT(0));
    if (cmp==0) return FD_TRUE;
    else if (cmp>1) return FD_ERROR_VALUE;
    else return FD_FALSE;}
}

static fdtype do_compare(int n,fdtype *v,int testspec[3])
{
  int i=1; while (i < n) {
    int comparison=numeric_compare(v[i-1],v[i]);
    if (comparison>1) return FD_ERROR_VALUE;
    else if (testspec[comparison+1]) i++;
    else return FD_FALSE;}
  return FD_TRUE;
}

static int ltspec[3]={1,0,0}, ltespec[3]={1,1,0}, espec[3]={0,1,0};
static int gtespec[3]={0,1,1}, gtspec[3]={0,0,1}, nespec[3]={1,0,1};

static fdtype lt(int n,fdtype *v) { return do_compare(n,v,ltspec); }
static fdtype lte(int n,fdtype *v) { return do_compare(n,v,ltespec); }
static fdtype numeqp(int n,fdtype *v) { return do_compare(n,v,espec); }
static fdtype gte(int n,fdtype *v) { return do_compare(n,v,gtespec); }
static fdtype gt(int n,fdtype *v) { return do_compare(n,v,gtspec); }
static fdtype numneqp(int n,fdtype *v) { return do_compare(n,v,nespec); }

/* Type predicates */

static fdtype stringp(fdtype x)
{
  if (FD_STRINGP(x)) return FD_TRUE; else return FD_FALSE;
}
static fdtype packetp(fdtype x)
{
  if (FD_PACKETP(x)) return FD_TRUE; else return FD_FALSE;
}
static fdtype symbolp(fdtype x)
{
  if (FD_SYMBOLP(x)) return FD_TRUE; else return FD_FALSE;
}
static fdtype pairp(fdtype x)
{
  if (FD_PAIRP(x)) return FD_TRUE; else return FD_FALSE;
}
static fdtype listp(fdtype x)
{
  if (FD_EMPTY_LISTP(x)) return FD_TRUE;
  else if (FD_PAIRP(x)) return FD_TRUE;
  else return FD_FALSE;
}
static fdtype proper_listp(fdtype x)
{
  if (FD_EMPTY_LISTP(x)) return FD_TRUE;
  else if (FD_PAIRP(x)) {
    fdtype scan=x;
    while (FD_PAIRP(scan)) scan=FD_CDR(scan);
    if (FD_EMPTY_LISTP(scan)) return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}
static fdtype vectorp(fdtype x)
{
  if (FD_VECTORP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype railp(fdtype x)
{
  if (FD_RAILP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype numberp(fdtype x)
{
  if (FD_NUMBERP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype flonump(fdtype x)
{
  if (FD_FLONUMP(x)) 
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype isnanp(fdtype x)
{
  if (FD_FLONUMP(x)) {
    double d=FD_FLONUM(x);
    if (isnan(d))
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_NUMBERP(x))
    return FD_FALSE;
  else return FD_TRUE;
}

static fdtype immediatep(fdtype x)
{
  if (FD_IMMEDIATEP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype consp(fdtype x)
{
  if (FD_CONSP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype staticp(fdtype x)
{
  if (FD_STATICP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype characterp(fdtype x)
{
  if (FD_CHARACTERP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype errorp(fdtype x)
{
  if (FD_TYPEP(x,fd_error_type)) return FD_TRUE; else return FD_FALSE;
}

static fdtype applicablep(fdtype x)
{
  if (FD_APPLICABLEP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype fcnidp(fdtype x)
{
  if (FD_FCNIDP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype opcodep(fdtype x)
{
  if (FD_OPCODEP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype make_opcode(fdtype x)
{
  return FD_OPCODE(FD_FIX2INT(x));
}

static fdtype booleanp(fdtype x)
{
  if ((FD_TRUEP(x)) || (FD_FALSEP(x)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype truep(fdtype x)
{
  if (FD_FALSEP(x))
    return FD_FALSE;
  else return FD_TRUE;
}

static fdtype falsep(fdtype x)
{
  if (FD_FALSEP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype typeof_prim(fdtype x)
{
  fd_ptr_type t=FD_PRIM_TYPE(x);
  if (fd_type_names[t]) return fdtype_string(fd_type_names[t]);
  else return fdtype_string("??");
}

#define GETSPECFORM(x) ((fd_special_form)(fd_fcnid_ref(x)))
static fdtype procedure_name(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return fdtype_string(f->fcn_name);
    else return FD_FALSE;}
  else if (FD_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->fexpr_name)
      return fdtype_string(sf->fexpr_name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_name",x);
}

static fdtype lisp_intern(fdtype symbol_name)
{
  if (FD_STRINGP(symbol_name))
    return fd_intern(FD_STRDATA(symbol_name));
  else return fd_type_error("string","lisp_intern",symbol_name);
}

static fdtype lisp_all_symbols()
{
  return fd_all_symbols();
}

static fdtype lisp_string2lisp(fdtype string)
{
  if (FD_STRINGP(string))
    return fd_parse(FD_STRDATA(string));
  else return fd_type_error("string","lisp_string2lisp",string);
}

static fdtype lisp_parse_arg(fdtype string)
{
  if (FD_STRINGP(string))
    return fd_parse_arg(FD_STRDATA(string));
  else return fd_incref(string);
}

static fdtype lisp_unparse_arg(fdtype obj)
{
  u8_string s=fd_unparse_arg(obj);
  return fd_lispstring(s);
}

static fdtype lisp_symbol2string(fdtype sym)
{
  return fdtype_string(FD_SYMBOL_NAME(sym));
}

static fdtype lisp_string2symbol(fdtype s)
{
  return fd_intern(FD_STRING_DATA(s));
}

static fdtype config_get(fdtype vars,fdtype dflt)
{
  fdtype result=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(var,vars) {
    fdtype value;
    if (FD_STRINGP(var))
      value=fd_config_get(FD_STRDATA(var));
    else if (FD_SYMBOLP(var))
      value=fd_config_get(FD_SYMBOL_NAME(var));
    else {
      fd_decref(result);
      return fd_type_error(_("string or symbol"),"config_get",var);}
    if (FD_VOIDP(value)) {}
    else FD_ADD_TO_CHOICE(result,value);}
  if (FD_EMPTY_CHOICEP(result))
    if (FD_VOIDP(dflt))
      return FD_FALSE;
    else return fd_incref(dflt);
  else return result;
}

static fdtype set_config(int n,fdtype *args)
{
  int retval, i=0;
  if (n%2) return fd_err(fd_SyntaxError,"set_config",NULL,FD_VOID);
  while (i<n) {
    fdtype var=args[i++], val=args[i++];
    if (FD_STRINGP(var))
      retval=fd_set_config(FD_STRDATA(var),val);
    else if (FD_SYMBOLP(var))
      retval=fd_set_config(FD_SYMBOL_NAME(var),val);
    else return fd_type_error(_("string or symbol"),"set_config",var);
    if (retval<0) return FD_ERROR_VALUE;}
  return FD_VOID;
}

static fdtype config_default(fdtype var,fdtype val)
{
  int retval;
  if (FD_STRINGP(var))
    retval=fd_default_config(FD_STRDATA(var),val);
  else if (FD_SYMBOLP(var))
    retval=fd_default_config(FD_SYMBOL_NAME(var),val);
  else return fd_type_error(_("string or symbol"),"config_default",var);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype find_configs(fdtype pat,fdtype raw)
{
  int with_docs=((FD_VOIDP(raw))||(FD_FALSEP(raw))||(FD_DEFAULTP(raw)));
  fdtype configs=((with_docs)?(fd_all_configs(0)):(fd_all_configs(1)));
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(config,configs) {
    fdtype key=((FD_PAIRP(config))?(FD_CAR(config)):(config));
    u8_string keystring=
      ((FD_STRINGP(key))?(FD_STRDATA(key)):(FD_SYMBOL_NAME(key)));
    if ((FD_STRINGP(pat))?(strcasestr(keystring,FD_STRDATA(pat))!=NULL):
        (FD_TYPEP(pat,fd_regex_type))?(fd_regex_test(pat,keystring,-1)):
        (0)) {
      FD_ADD_TO_CHOICE(results,config); fd_incref(config);}}
  return results;
}

static fdtype lconfig_get(fdtype var,void *data)
{
  fdtype proc=(fdtype)data;
  fdtype result=fd_apply(proc,1,&var);
  return result;
}

static int lconfig_set(fdtype var,fdtype val,void *data)
{
  fdtype proc=(fdtype)data, args[2], result;
  args[0]=var; args[1]=val;
  result=fd_apply(proc,2,args);
  if (FD_ABORTP(result))
    return fd_interr(result);
  else if (FD_TRUEP(result)) {
    fd_decref(result); return 1;}
  else return 0;
}

static int reuse_lconfig(struct FD_CONFIG_HANDLER *e);
static fdtype config_def(fdtype var,fdtype handler,fdtype docstring)
{
  int retval;
  fd_incref(handler);
  retval=fd_register_config_x(FD_SYMBOL_NAME(var),
                              ((FD_VOIDP(docstring)) ? (NULL) :
                               (FD_STRDATA(docstring))),
                              lconfig_get,lconfig_set,(void *) handler,
                              reuse_lconfig);
  if (retval<0) {
    fd_decref(handler);
    return FD_ERROR_VALUE;}
  return FD_VOID;
}
static int reuse_lconfig(struct FD_CONFIG_HANDLER *e){
  if (e->fd_configdata) {
    fd_decref((fdtype)(e->fd_configdata));
    return 1;}
  else return 0;}

static fdtype thread_get(fdtype var)
{
  fdtype value=fd_thread_get(var);
  if (FD_VOIDP(value)) return FD_EMPTY_CHOICE;
  else return value;
}

static fdtype thread_set(fdtype var,fdtype val)
{
  if (fd_thread_set(var,val)<0)
    return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype thread_add(fdtype var,fdtype val)
{
  if (fd_thread_add(var,val)<0)
    return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype thread_ref(fdtype expr,fd_lispenv env)
{
  fdtype sym_arg=fd_get_arg(expr,1), sym, val;
  fdtype dflt_expr=fd_get_arg(expr,2);
  if ((FD_VOIDP(sym_arg))||(FD_VOIDP(dflt_expr)))
    return fd_err(fd_SyntaxError,"thread_ref",NULL,FD_VOID);
  sym=fd_eval(sym_arg,env);
  if (FD_ABORTP(sym)) return sym;
  else if (!(FD_SYMBOLP(sym))) 
    return fd_err(fd_TypeError,"thread_ref",u8_strdup("symbol"),sym);
  else val=fd_thread_get(sym);
  if (FD_ABORTP(val)) return val;
  else if (FD_VOIDP(val)) {
    fdtype useval=fd_eval(dflt_expr,env); int rv;
    if (FD_ABORTP(useval)) return useval;
    rv=fd_thread_set(sym,useval);
    if (rv<0) {
      fd_decref(useval);
      return FD_ERROR_VALUE;}
    return useval;}
  else return val;
}

/* GETSOURCEIDS */

static void add_sourceid(u8_string s,void *vp)
{
  fdtype *valp=(fdtype *)vp;
  *valp=fd_make_pair(fdtype_string(s),*valp);
}

static fdtype lisp_getsourceinfo()
{
  fdtype result=FD_EMPTY_LIST;
  u8_for_source_files(add_sourceid,&result);
  return result;
}

/* Force some errors */

static fdtype force_sigsegv()
{
  fdtype *values=NULL;
  fd_incref(values[3]);
  return values[3];
}

static fdtype force_sigfpe()
{
  return fd_init_double(NULL,5.0/0.0);
}

/* The init function */

FD_EXPORT void fd_init_coreprims_c()
{
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_scheme_module,fd_make_cprim2("EQ?",eqp,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("EQV?",eqvp,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("EQUAL?",equalp,2));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("IDENTICAL?",equalp,2)));
  fd_defalias(fd_scheme_module,"=?","IDENTICAL?");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("OVERLAPS?",overlapsp,2)));
  fd_defalias(fd_scheme_module,"*=?","OVERLAPS?");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("CONTAINS?",containsp,2)));
  fd_defalias(fd_scheme_module,"⊆?","CONTAINS?");
  fd_defalias(fd_scheme_module,"⊆","CONTAINS?");
  fd_idefn(fd_scheme_module,fd_make_cprim2("COMPARE",comparefn,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("COMPARE/QUICK",quickcomparefn,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("DEEP-COPY",deepcopy,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("STATIC-COPY",staticcopy,1));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("DONTOPT",dontopt,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2x("REFCOUNT",get_refcount,1,-1,FD_VOID,
                                          fd_fixnum_type,FD_INT(0))));

  fd_idefn(fd_scheme_module,fd_make_cprimn("<",lt,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("<=",lte,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("=",numeqp,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn(">=",gte,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn(">",gt,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("!=",numneqp,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ZERO?",lisp_zerop,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("STRING?",stringp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PACKET?",packetp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SYMBOL?",symbolp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PAIR?",pairp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LIST?",listp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PROPER-LIST?",proper_listp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("VECTOR?",vectorp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("RAIL?",railp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CHARACTER?",characterp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FCNID?",opcodep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("OPCODE?",opcodep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ERROR?",errorp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("APPLICABLE?",applicablep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FCNID?",fcnidp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FLONUM?",flonump,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("NAN?",isnanp,1));

  fd_defalias(fd_scheme_module,"CHAR?","CHARACTER?");
  fd_idefn(fd_scheme_module,fd_make_cprim1("BOOLEAN?",booleanp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TRUE?",truep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FALSE?",falsep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("NUMBER?",numberp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("IMMEDIATE?",immediatep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("STATIC?",staticp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CONS?",consp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CONSED?",consp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TYPEOF",typeof_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("MAKE-OPCODE",make_opcode,1,
                           fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PROCEDURE-NAME",procedure_name,1));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("CONFIG",config_get,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("SET-CONFIG!",set_config,2)));
  fd_defalias(fd_scheme_module,"CONFIG!","SET-CONFIG!");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("CONFIG-DEFAULT!",config_default,2)));
  fd_idefn(fd_scheme_module,fd_make_cprim2("FIND-CONFIGS",find_configs,1));
  fd_defalias(fd_scheme_module,"CONFIG?","FIND-CONFIGS");

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("CONFIG-DEF!",config_def,2,
                           fd_symbol_type,FD_VOID,-1,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("THREADGET",thread_get,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("THREADSET!",thread_set,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("THREADADD!",thread_add,2));
  fd_defspecial(fd_scheme_module,"THREADREF",thread_ref);

  fd_idefn(fd_scheme_module,fd_make_cprim1("INTERN",lisp_intern,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("SYMBOL->STRING",lisp_symbol2string,1,
                           fd_symbol_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("STRING->SYMBOL",lisp_string2symbol,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("STRING->LISP",lisp_string2lisp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PARSE-ARG",lisp_parse_arg,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("UNPARSE-ARG",lisp_unparse_arg,1));

  fd_idefn(fd_scheme_module,fd_make_cprim0("GETSOURCEINFO",lisp_getsourceinfo));
  fd_idefn(fd_scheme_module,fd_make_cprim0("ALLSYMBOLS",lisp_all_symbols));

  fd_idefn(fd_scheme_module,fd_make_cprim0("SEGFAULT",force_sigsegv));
  fd_idefn(fd_scheme_module,fd_make_cprim0("FPERROR",force_sigfpe));

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
