/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
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

 'EQUAL?' calls the lispval_compare to do it's work, returning #t for a
 return value of 0.

 For custom types, this calls the 'fd_comparefn' found in
 'fd_comparators[*typecode*]', which should return -1, 0, or 1.

*/
static lispval equalp(lispval x,lispval y)
{
  if (FD_EQ(x,y)) return FD_TRUE;
  else if (LISP_EQUAL(x,y)) return FD_TRUE;
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
static lispval comparefn(lispval x,lispval y)
{
  int n = LISP_COMPARE(x,y,FD_COMPARE_FULL);
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
static lispval quickcomparefn(lispval x,lispval y)
{
  int n = FD_QCOMPARE(x,y);
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
static lispval deepcopy(lispval x)
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
static lispval staticcopy(lispval x)
{
  return fd_static_copy(x);
}

static lispval dontopt(lispval x)
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
static lispval get_refcount(lispval x,lispval delta)
{
  if (CONSP(x)) {
    struct FD_CONS *cons = (struct FD_CONS *)x;
    int refcount = FD_CONS_REFCOUNT(cons);
    long long d = FIX2INT(delta);
    if (d<0) d = -d;
    if (refcount<1)
      return fd_reterr("Bad REFCOUNT","get_refcount",
                       u8_mkstring("%lx",(unsigned long)x),
                       VOID);
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
static lispval eqp(lispval x,lispval y)
{
  if (x == y) return FD_TRUE;
  else return FD_FALSE;
}

static lispval eqvp(lispval x,lispval y)
{
  if (x == y) return FD_TRUE;
  else if ((NUMBERP(x)) && (NUMBERP(y)))
    if (fd_numcompare(x,y)==0)
      return FD_TRUE;
    else return FD_FALSE;
  else return FD_FALSE;
}

static lispval overlapsp(lispval x,lispval y)
{
  if (EMPTYP(x)) return FD_FALSE;
  else if (x == y) return FD_TRUE;
  else if (EMPTYP(y)) return FD_FALSE;
  else if (fd_overlapp(x,y)) return FD_TRUE;
  else return FD_FALSE;
}

static lispval containsp(lispval x,lispval y)
{
  if (EMPTYP(x)) return FD_FALSE;
  else if (x == y) return FD_TRUE;
  else if (EMPTYP(y)) return FD_FALSE;
  else if (fd_containsp(x,y)) return FD_TRUE;
  else return FD_FALSE;
}

static int numeric_compare(const lispval x,const lispval y)
{
  if ((FIXNUMP(x)) && (FIXNUMP(y))) {
    long long ix = FIX2INT(x), iy = FIX2INT(y);
    if (ix>iy) return 1; else if (ix<iy) return -1; else return 0;}
  else if ((FD_FLONUMP(x)) && (FD_FLONUMP(y))) {
    double dx = FD_FLONUM(x), dy = FD_FLONUM(y);
    if (dx>dy) return 1; else if (dx<dy) return -1; else return 0;}
  else return fd_numcompare(x,y);
}

static lispval lisp_zerop(lispval x)
{
  if (FIXNUMP(x))
    if (FIX2INT(x)==0) return FD_TRUE; else return FD_FALSE;
  else if (FD_FLONUMP(x))
    if (FD_FLONUM(x)==0.0) return FD_TRUE; else return FD_FALSE;
  else if (!(NUMBERP(x)))
    return FD_FALSE;
  else {
    int cmp = fd_numcompare(x,FD_INT(0));
    if (cmp==0) return FD_TRUE;
    else if (cmp>1) return FD_ERROR;
    else return FD_FALSE;}
}

static lispval do_compare(int n,lispval *v,int testspec[3])
{
  int i = 1; while (i < n) {
    int comparison = numeric_compare(v[i-1],v[i]);
    if (comparison>1) return FD_ERROR;
    else if (testspec[comparison+1]) i++;
    else return FD_FALSE;}
  return FD_TRUE;
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
  if (STRINGP(x)) return FD_TRUE; else return FD_FALSE;
}
static lispval valid_utf8p(lispval x)
{
  if (STRINGP(x)) {
    int rv = u8_validp(FD_CSTRING(x));
    if (rv)
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}
static lispval regexp(lispval x)
{
  if (FD_REGEXP(x)) return FD_TRUE; else return FD_FALSE;
}

static lispval packetp(lispval x)
{
  if (PACKETP(x)) return FD_TRUE; else return FD_FALSE;
}
static lispval secretp(lispval x)
{
  if (TYPEP(x,fd_secret_type))
    return FD_TRUE;
  else return FD_FALSE;
}
static lispval symbolp(lispval x)
{
  if (SYMBOLP(x)) return FD_TRUE; else return FD_FALSE;
}
static lispval pairp(lispval x)
{
  if (PAIRP(x)) return FD_TRUE; else return FD_FALSE;
}
static lispval listp(lispval x)
{
  if (NILP(x)) return FD_TRUE;
  else if (PAIRP(x)) return FD_TRUE;
  else return FD_FALSE;
}
static lispval proper_listp(lispval x)
{
  if (NILP(x)) return FD_TRUE;
  else if (PAIRP(x)) {
    lispval scan = x;
    while (PAIRP(scan)) scan = FD_CDR(scan);
    if (NILP(scan)) return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}
static lispval vectorp(lispval x)
{
  if (VECTORP(x)) return FD_TRUE; else return FD_FALSE;
}

static lispval railp(lispval x)
{
  if (FD_CODEP(x)) return FD_TRUE; else return FD_FALSE;
}

static lispval numberp(lispval x)
{
  if (NUMBERP(x)) return FD_TRUE; else return FD_FALSE;
}

static lispval flonump(lispval x)
{
  if (FD_FLONUMP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval isnanp(lispval x)
{
  if (FD_FLONUMP(x)) {
    double d = FD_FLONUM(x);
    if (isnan(d))
      return FD_TRUE;
    else return FD_FALSE;}
  else if (NUMBERP(x))
    return FD_FALSE;
  else return FD_TRUE;
}

static lispval immediatep(lispval x)
{
  if (FD_IMMEDIATEP(x)) return FD_TRUE; else return FD_FALSE;
}

static lispval consp(lispval x)
{
  if (CONSP(x)) return FD_TRUE; else return FD_FALSE;
}

static lispval staticp(lispval x)
{
  if (FD_STATICP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval characterp(lispval x)
{
  if (FD_CHARACTERP(x)) return FD_TRUE; else return FD_FALSE;
}

static lispval exceptionp(lispval x)
{
  if (FD_EXCEPTIONP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval applicablep(lispval x)
{
  if (FD_APPLICABLEP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static lispval fcnidp(lispval x)
{
  if (FD_FCNIDP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval opcodep(lispval x)
{
  if (FD_OPCODEP(x)) return FD_TRUE; else return FD_FALSE;
}

static lispval make_opcode(lispval x)
{
  return FD_OPCODE(FIX2INT(x));
}

static lispval booleanp(lispval x)
{
  if ((FD_TRUEP(x)) || (FALSEP(x)))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval truep(lispval x)
{
  if (FALSEP(x))
    return FD_FALSE;
  else return FD_TRUE;
}

static lispval falsep(lispval x)
{
  if (FALSEP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval typeof_prim(lispval x)
{
  fd_ptr_type t = FD_PRIM_TYPE(x);
  if (fd_type_names[t]) return lispval_string(fd_type_names[t]);
  else return lispval_string("??");
}

#define GETEVALFN(x) ((fd_evalfn)(fd_fcnid_ref(x)))
static lispval procedure_name(lispval x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return lispval_string(f->fcn_name);
    else return FD_FALSE;}
  else if (TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return lispval_string(sf->evalfn_name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_name",x);
}

static lispval lisp_intern(lispval symbol_name)
{
  if (STRINGP(symbol_name))
    return fd_intern(CSTRING(symbol_name));
  else return fd_type_error("string","lisp_intern",symbol_name);
}

static lispval lisp_all_symbols()
{
  return fd_all_symbols();
}

static lispval lisp_string2lisp(lispval string)
{
  if (STRINGP(string))
    return fd_parse(CSTRING(string));
  else return fd_type_error("string","lisp_string2lisp",string);
}

static lispval lisp2string(lispval x)
{
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
  fd_unparse(&out,x);
  return fd_stream2string(&out);
}

static lispval lisp_tolisp(lispval arg)
{
  if (STRINGP(arg)) {
    u8_string string=CSTRING(arg);
    if ( (FD_STRLEN(arg)>64) || (strchr(string,' ')) )
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
      else return fd_parse(string);}}
  else return fd_incref(arg);
}

static lispval lisp_parse_arg(lispval string)
{
  if (STRINGP(string))
    return fd_parse_arg(CSTRING(string));
  else return fd_incref(string);
}

static lispval lisp_unparse_arg(lispval obj)
{
  u8_string s = fd_unparse_arg(obj);
  return fd_lispstring(s);
}

static lispval lisp_symbol2string(lispval sym)
{
  return lispval_string(SYM_NAME(sym));
}

static lispval lisp_string2symbol(lispval s)
{
  return fd_intern(FD_STRING_DATA(s));
}

static lispval config_get(lispval vars,lispval dflt,lispval valfn)
{
  lispval result = EMPTY;
  DO_CHOICES(var,vars) {
    lispval value;
    if (STRINGP(var))
      value = fd_config_get(CSTRING(var));
    else if (SYMBOLP(var))
      value = fd_config_get(SYM_NAME(var));
    else {
      fd_decref(result);
      return fd_type_error(_("string or symbol"),"config_get",var);}
    if (VOIDP(value)) {}
    else if (value==FD_DEFAULT_VALUE) {}
    else if (FD_APPLICABLEP(valfn)) {
      lispval converted = fd_apply(valfn,1,&value);
      if (FD_ABORTP(converted)) {
        u8_log(LOG_WARN,"ConfigConversionError",
               "Error converting config value of %q=%q using %q",
               var,value,valfn);
        fd_clear_errors(1);
        fd_decref(value);}
      else {
        CHOICE_ADD(result,converted);
        fd_decref(value);}}
    else if ((FD_STRINGP(value)) && (FD_TRUEP(valfn))) {
      u8_string valstring = FD_CSTRING(value);
      lispval parsed = fd_parse(valstring);
      if (FD_ABORTP(parsed)) {
        u8_log(LOG_WARN,"ConfigParseError",
               "Error parsing config value of %q=%q",
               var,value);
        fd_clear_errors(1);
        fd_decref(value);}
      else {
        CHOICE_ADD(result,parsed);
        fd_decref(value);}}
    else {
      CHOICE_ADD(result,value);}}
  if (EMPTYP(result))
    if (VOIDP(dflt))
      return FD_FALSE;
    else return fd_incref(dflt);
  else return result;
}

static lispval config_macro(lispval expr,fd_lexenv env,fd_stack ptr)
{
  lispval var = fd_get_arg(expr,1);
  if (FD_SYMBOLP(var)) {
    lispval config_val = (fd_config_get(FD_SYMBOL_NAME(var)));
    if (FD_VOIDP(config_val))
      return FD_FALSE;
    else return config_val;}
  else return FD_FALSE;
}

static lispval set_config(int n,lispval *args)
{
  int retval, i = 0;
  if (n%2) return fd_err(fd_SyntaxError,"set_config",NULL,VOID);
  while (i<n) {
    lispval var = args[i++], val = args[i++];
    if (STRINGP(var))
      retval = fd_set_config(CSTRING(var),val);
    else if (SYMBOLP(var))
      retval = fd_set_config(SYM_NAME(var),val);
    else return fd_type_error(_("string or symbol"),"set_config",var);
    if (retval<0) return FD_ERROR;}
  return VOID;
}

static lispval config_default(lispval var,lispval val)
{
  int retval;
  if (STRINGP(var))
    retval = fd_default_config(CSTRING(var),val);
  else if (SYMBOLP(var))
    retval = fd_default_config(SYM_NAME(var),val);
  else return fd_type_error(_("string or symbol"),"config_default",var);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static lispval find_configs(lispval pat,lispval raw)
{
  int with_docs = ((VOIDP(raw))||(FALSEP(raw))||(FD_DEFAULTP(raw)));
  lispval configs = ((with_docs)?(fd_all_configs(0)):(fd_all_configs(1)));
  lispval results = EMPTY;
  DO_CHOICES(config,configs) {
    lispval key = ((PAIRP(config))?(FD_CAR(config)):(config));
    u8_string keystring=
      ((STRINGP(key))?(CSTRING(key)):(SYM_NAME(key)));
    if ((STRINGP(pat))?(strcasestr(keystring,CSTRING(pat))!=NULL):
        (TYPEP(pat,fd_regex_type))?(fd_regex_test(pat,keystring,-1)):
        (0)) {
      CHOICE_ADD(results,config); fd_incref(config);}}
  return results;
}

static lispval lconfig_get(lispval var,void *data)
{
  lispval proc = (lispval)data;
  lispval result = fd_apply(proc,1,&var);
  return result;
}

static int lconfig_set(lispval var,lispval val,void *data)
{
  lispval proc = (lispval)data, args[2], result;
  args[0]=var; args[1]=val;
  result = fd_apply(proc,2,args);
  if (FD_ABORTP(result))
    return fd_interr(result);
  else if (FD_TRUEP(result)) {
    fd_decref(result); return 1;}
  else return 0;
}

static int reuse_lconfig(struct FD_CONFIG_HANDLER *e);
static lispval config_def(lispval var,lispval handler,lispval docstring)
{
  int retval;
  fd_incref(handler);
  retval = fd_register_config_x
    (SYM_NAME(var),
     ((STRINGP(docstring)) ? (CSTRING(docstring)) : (NULL)),
     lconfig_get,lconfig_set,(void *) handler,
     reuse_lconfig);
  if (retval<0) {
    fd_decref(handler);
    return FD_ERROR;}
  return VOID;
}
static int reuse_lconfig(struct FD_CONFIG_HANDLER *e){
  if (e->configdata) {
    fd_decref((lispval)(e->configdata));
    return 1;}
  else return 0;}

static lispval thread_get(lispval var)
{
  lispval value = fd_thread_get(var);
  if (VOIDP(value)) return EMPTY;
  else return value;
}

static lispval thread_set(lispval var,lispval val)
{
  if (fd_thread_set(var,val)<0)
    return FD_ERROR;
  else return VOID;
}

static lispval thread_add(lispval var,lispval val)
{
  if (fd_thread_add(var,val)<0)
    return FD_ERROR;
  else return VOID;
}

static lispval thread_ref_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval sym_arg = fd_get_arg(expr,1), sym, val;
  lispval dflt_expr = fd_get_arg(expr,2);
  if ((VOIDP(sym_arg))||(VOIDP(dflt_expr)))
    return fd_err(fd_SyntaxError,"thread_ref",NULL,VOID);
  sym = fd_eval(sym_arg,env);
  if (FD_ABORTP(sym)) return sym;
  else if (!(SYMBOLP(sym)))
    return fd_err(fd_TypeError,"thread_ref",u8_strdup("symbol"),sym);
  else val = fd_thread_get(sym);
  if (FD_ABORTP(val)) return val;
  else if (VOIDP(val)) {
    lispval useval = fd_eval(dflt_expr,env); int rv;
    if (FD_ABORTP(useval)) return useval;
    rv = fd_thread_set(sym,useval);
    if (rv<0) {
      fd_decref(useval);
      return FD_ERROR;}
    return useval;}
  else return val;
}

/* HASHPTR */

static lispval hashptr_prim(lispval x)
{
  unsigned long long intval = (unsigned long long)x;
  if ((intval<FD_MAX_FIXNUM)&&(intval>FD_MIN_FIXNUM))
    return FIX2INT(((int)intval));
  else return (lispval)fd_ulong_long_to_bigint(intval);
}

static lispval hashref_prim(lispval x)
{
  unsigned long long intval = (unsigned long long)x;
  char buf[40]="", numbuf[32]="";
  strcpy(buf,"#!");
  strcpy(buf,u8_uitoa16(intval,numbuf));
  return fd_make_string(NULL,-1,buf);
}

static lispval ptrlock_prim(lispval x,lispval mod)
{
  unsigned long long intval = (unsigned long long)x;
  long long int modval = ((VOIDP(mod))?
                        (FD_N_PTRLOCKS):
                        (FIX2INT(mod)));
  if (modval==0)
    return (lispval)fd_ulong_long_to_bigint(intval);
  else {
    unsigned long long hashval = hashptrval((void *)x,modval);
    return FD_INT(hashval);}
}

/* GETSOURCEIDS */

static void add_sourceid(u8_string s,void *vp)
{
  lispval *valp = (lispval *)vp;
  *valp = fd_make_pair(lispval_string(s),*valp);
}

static lispval lisp_getsourceinfo()
{
  lispval result = NIL;
  u8_for_source_files(add_sourceid,&result);
  return result;
}

/* Force some errors */

static lispval force_sigsegv()
{
  lispval *values = NULL;
  fd_incref(values[3]);
  return values[3];
}

static lispval force_sigfpe()
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
           fd_make_ndprim(fd_make_cprim2x("REFCOUNT",get_refcount,1,-1,VOID,
                                          fd_fixnum_type,FD_INT(0))));

  fd_idefn(fd_scheme_module,fd_make_cprimn("<",lt,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("<=",lte,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("=",numeqp,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn(">=",gte,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn(">",gt,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("!=",numneqp,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ZERO?",lisp_zerop,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("STRING?",stringp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("VALID-UTF8?",valid_utf8p,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("REGEX?",regexp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PACKET?",packetp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SECRET?",secretp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SYMBOL?",symbolp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PAIR?",pairp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LIST?",listp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PROPER-LIST?",proper_listp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("VECTOR?",vectorp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CODE?",railp,1));
  fd_defalias(fd_scheme_module,"RAIL?","CODE?");
  fd_idefn(fd_scheme_module,fd_make_cprim1("CHARACTER?",characterp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("OPCODE?",opcodep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("EXCEPTION?",exceptionp,1));
  fd_defalias(fd_scheme_module,"ERROR?","EXCEPTION?");
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
                           fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PROCEDURE-NAME",procedure_name,1));

  fd_idefn3(fd_scheme_module,"CONFIG",config_get,FD_NEEDS_1_ARG,
            "CONFIG *name* *default* *valfn*)\n"
            "Gets the configuration value named *name*, returning *default* "
            "if it isn't defined. *valfn*, if provided is either a function "
            "to call on the retrieved value or #t to indicate that string "
            "values should be parsed as lisp",
            -1,FD_VOID,-1,FD_VOID,-1,FD_VOID);

  fd_def_evalfn(fd_scheme_module,"#CONFIG",
                "#:CONFIG\"FDVERSION\" or #:CONFIG:LOADPATH\n"
                "evaluates to a value from the current configuration "
                "environment",
                config_macro);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("SET-CONFIG!",set_config,2)));
  fd_defalias(fd_scheme_module,"CONFIG!","SET-CONFIG!");
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("CONFIG-DEFAULT!",config_default,2)));
  fd_idefn(fd_scheme_module,fd_make_cprim2("FIND-CONFIGS",find_configs,1));
  fd_defalias(fd_scheme_module,"CONFIG?","FIND-CONFIGS");

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("CONFIG-DEF!",config_def,2,
                           fd_symbol_type,VOID,-1,VOID,
                           fd_string_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("THREADGET",thread_get,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("THREADSET!",thread_set,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("THREADADD!",thread_add,2));
  fd_def_evalfn(fd_scheme_module,"THREADREF","",thread_ref_evalfn);

  fd_idefn(fd_scheme_module,fd_make_cprim1("INTERN",lisp_intern,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("SYMBOL->STRING",lisp_symbol2string,1,
                           fd_symbol_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("STRING->SYMBOL",lisp_string2symbol,1,
                           fd_string_type,VOID));
  fd_idefn1(fd_scheme_module,"->LISP",lisp_tolisp,1,
            "Converts strings to lisp objects and just returns other objects. "
            "Strings which start with lisp prefix characters are parsed as "
            "lisp objects, and strings are made into uppercase symbols when "
            "they either start with a single quote or contain no whitespace "
            "and are < 64 bytes. When parsing generates an error, the string "
            "is returned as a string object",
            -1,FD_VOID);
  fd_idefn(fd_scheme_module,fd_make_cprim1("STRING->LISP",lisp_string2lisp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LISP->STRING",lisp2string,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("PARSE-ARG",lisp_parse_arg,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("UNPARSE-ARG",lisp_unparse_arg,1));

  fd_idefn(fd_scheme_module,fd_make_cprim0("GETSOURCEINFO",lisp_getsourceinfo));
  fd_idefn(fd_scheme_module,fd_make_cprim0("ALLSYMBOLS",lisp_all_symbols));

  fd_idefn(fd_scheme_module,fd_make_cprim0("SEGFAULT",force_sigsegv));
  fd_idefn(fd_scheme_module,fd_make_cprim0("FPERROR",force_sigfpe));

  fd_idefn1(fd_scheme_module,"HASHPTR",hashptr_prim,1|FD_NDCALL,
            "Returns an integer representation for its argument.",
            -1,FD_VOID);
  fd_idefn(fd_scheme_module,fd_make_cprim1("HASHREF",hashref_prim,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("PTRLOCK",ptrlock_prim,1,
                           -1,VOID,fd_fixnum_type,VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
