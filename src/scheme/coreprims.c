/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"
#include "framerd/support.h"
#include "framerd/fdregex.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

/* Standard predicates */

static fdtype equalp(fdtype x,fdtype y)
{
  if (FD_EQ(x,y)) return FD_TRUE;
  else if (FDTYPE_EQUAL(x,y)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype comparefn(fdtype x,fdtype y)
{
  int n=FDTYPE_COMPARE(x,y);
  return FD_INT(n);
}

static fdtype fastcomparefn(fdtype x,fdtype y)
{
  int n=FD_QCOMPARE(x,y);
  return FD_INT(n);
}

static fdtype deepcopy(fdtype x)
{
  return fd_deep_copy(x);
}

static fdtype get_refcount(fdtype x,fdtype delta)
{
  if (FD_CONSP(x)) {
    struct FD_CONS *cons=(struct FD_CONS *)x;
    int refcount=FD_CONS_REFCOUNT(cons);
    int d=FD_FIX2INT(delta);
    if (d<0) d=-d;
    if (refcount<1)
      return fd_reterr("Bad REFCOUNT","get_refcount",
                       u8_mkstring("%lx",(unsigned long)x),
                       FD_VOID);
    else return FD_INT((refcount-(d+1)));}
  else return FD_FALSE;
}

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
    int ix=FD_FIX2INT(x), iy=FD_FIX2INT(y);
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

static fdtype numberp(fdtype x)
{
  if (FD_NUMBERP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype immediatep(fdtype x)
{
  if (FD_IMMEDIATEP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype consedp(fdtype x)
{
  if (FD_CONSP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype characterp(fdtype x)
{
  if (FD_CHARACTERP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype errorp(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_error_type)) return FD_TRUE; else return FD_FALSE;
}

static fdtype applicablep(fdtype x)
{
  if (FD_APPLICABLEP(x)) return FD_TRUE;
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

#define GETSPECFORM(x) \
  ((FD_PPTRP(x)) ? ((fd_special_form)(fd_pptr_ref(x))) : ((fd_special_form)x))
static fdtype procedure_name(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->name)
      return fdtype_string(f->name);
    else return FD_FALSE;}
  else if (FD_PRIM_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->name)
      return fdtype_string(sf->name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_name",x);
}

/* Arithmetic */

static fdtype plus_lexpr(int n,fdtype *args)
{
  int i=0; int floating=0, generic=0;
  while (i < n)
    if (FD_FIXNUMP(args[i])) i++;
    else if (FD_FLONUMP(args[i])) {floating=1; i++;}
    else {generic=1; i++;}
  if ((floating==0) && (generic==0)) {
    int fixresult=0;
    i=0; while (i < n) {
      int val=0;
      if (FD_FIXNUMP(args[i])) val=fd_getint(args[i]);
      fixresult=fixresult+val; i++;}
    return FD_INT(fixresult);}
  else if (generic == 0) {
    double floresult=0.0;
    i=0; while (i < n) {
      double val;
      if (FD_FIXNUMP(args[i])) val=(double)fd_getint(args[i]);
      else if (FD_BIGINTP(args[i]))
        val=(double)fd_bigint_to_double((fd_bigint)args[i]);
      else val=((struct FD_DOUBLE *)args[i])->flonum;
      floresult=floresult+val;
      i++;}
    return fd_init_double(NULL,floresult);}
  else {
    fdtype result=FD_INT(0);
    i=0; while (i < n) {
      fdtype newv=fd_plus(result,args[i]);
      if (FD_ABORTP(newv)) {
        fd_decref(result); return newv;}
      fd_decref(result); result=newv; i++;}
    return result;}
}

static fdtype plus1(fdtype x)
{
  fdtype args[2]; args[0]=x; args[1]=FD_INT(1);
  return plus_lexpr(2,args);
}
static fdtype minus1(fdtype x)
{
  fdtype args[2]; args[0]=x; args[1]=FD_INT(-1);
  return plus_lexpr(2,args);
}

static fdtype times_lexpr(int n,fdtype *args)
{
  int i=0; int floating=0, generic=0;
  while (i < n)
    if (FD_FIXNUMP(args[i])) i++;
    else if (FD_FLONUMP(args[i])) {floating=1; i++;}
    else {generic=1; i++;}
  if ((floating==0) && (generic==0)) {
    long long fixresult=1;
    i=0; while (i < n) {
      long long mult=fd_getint(args[i]);
      if (mult==0) return FD_INT(0);
      else {
        int q=((mult>0)?(FD_MAX_FIXNUM/mult):(FD_MIN_FIXNUM/mult));
        if ((fixresult>0)?(fixresult>q):((-fixresult)>q)) {
          fdtype bigresult=fd_multiply(FD_INT(fixresult),args[i]);
          i++; while (i<n) {
            fdtype bigprod=fd_multiply(bigresult,args[i]);
            fd_decref(bigresult); bigresult=bigprod; i++;}
          return bigresult;}
        else fixresult=fixresult*mult;}
      i++;}
    return FD_INT(fixresult);}
  else if (generic == 0) {
    double floresult=1.0;
    i=0; while (i < n) {
      double val;
      if (FD_FIXNUMP(args[i])) val=(double)FD_FIX2INT(args[i]);
      else if  (FD_BIGINTP(args[i]))
        val=(double)fd_bigint_to_double((fd_bigint)args[i]);
      else val=((struct FD_DOUBLE *)args[i])->flonum;
      floresult=floresult*val;
      i++;}
    return fd_init_double(NULL,floresult);}
  else {
    fdtype result=FD_INT(1);
    i=0; while (i < n) {
      fdtype newv=fd_multiply(result,args[i]);
      fd_decref(result); result=newv; i++;}
    return result;}
}

static fdtype minus_lexpr(int n,fdtype *args)
{
  if (n == 1)
    if (FD_FIXNUMP(args[0]))
      return FD_INT(-(FD_FIX2INT(args[0])));
    else if (FD_FLONUMP(args[0]))
      return fd_init_double(NULL,-(FD_FLONUM(args[0])));
    else return fd_subtract(FD_INT(0),args[0]);
  else {
    int i=0; int floating=0, generic=0;
    while (i < n)
      if (FD_FIXNUMP(args[i])) i++;
      else if (FD_FLONUMP(args[i])) {floating=1; i++;}
      else {generic=1; i++;}
    if ((floating==0) && (generic==0)) {
      int fixresult=0;
      i=0; while (i < n) {
        int val=0;
        if (FD_FIXNUMP(args[i])) val=FD_FIX2INT(args[i]);
        else if  (FD_BIGINTP(args[i]))
          val=(double)fd_bigint_to_double((fd_bigint)args[i]);
        if (i==0) fixresult=val; else fixresult=fixresult-val;
        i++;}
      return FD_INT(fixresult);}
    else if (generic == 0) {
      double floresult=0.0;
      i=0; while (i < n) {
        double val;
        if (FD_FIXNUMP(args[i])) val=(double)FD_FIX2INT(args[i]);
        else if  (FD_BIGINTP(args[i]))
          val=(double)fd_bigint_to_double((fd_bigint)args[i]);
        else val=((struct FD_DOUBLE *)args[i])->flonum;
        if (i==0) floresult=val; else floresult=floresult-val;
        i++;}
      return fd_init_double(NULL,floresult);}
    else {
      fdtype result=fd_incref(args[0]);
      i=1; while (i < n) {
        fdtype newv=fd_subtract(result,args[i]);
        fd_decref(result); result=newv; i++;}
      return result;}}
}

static double todouble(fdtype x)
{
  if (FD_FIXNUMP(x))
    return (double)(FD_FIX2INT(x));
  else if (FD_BIGINTP(x))
    return (double)fd_bigint_to_double((fd_bigint)x);
  else if (FD_FLONUMP(x))
    return (((struct FD_DOUBLE *)x)->flonum);
  else {
    /* This won't really work, but we should catch the error before
       this point.  */
    fd_seterr(fd_TypeError,"todouble",NULL,x);
    return -1.0;}
}

static fdtype div_lexpr(int n,fdtype *args)
{
  int all_double=1, i=0;
  while (i<n)
    if (FD_FLONUMP(args[i])) i++;
    else {all_double=0; break;}
  if (all_double)
    if (n == 1)
      return fd_init_double(NULL,(1.0/(FD_FLONUM(args[0]))));
    else {
      double val=FD_FLONUM(args[0]); i=1;
      while (i<n) {val=val/FD_FLONUM(args[i]); i++;}
      return fd_init_double(NULL,val);}
  else if (n==1)
    return fd_divide(FD_INT(1),args[0]);
  else {
    fdtype value=fd_incref(args[0]); i=1;
    while (i<n) {
      fdtype newv=fd_divide(value,args[i]);
      fd_decref(value); value=newv; i++;}
    return value;}
}

static fdtype idiv_lexpr(int n,fdtype *args)
{
  int all_double=1, i=0;
  while (i<n)
    if (FD_FLONUMP(args[i])) i++;
    else if (!((FD_FIXNUMP(args[i])) || (FD_BIGINTP(args[i]))))
      return fd_type_error(_("scalar"),"idiv_lexpr",args[i]);
    else {all_double=0; i++;}
  if (all_double)
    if (n == 1)
      return fd_init_double(NULL,(1.0/(FD_FLONUM(args[0]))));
    else {
      double val=FD_FLONUM(args[0]); i=1;
      while (i<n) {val=val/FD_FLONUM(args[i]); i++;}
      return fd_init_double(NULL,val);}
  else if (n==1) {
    double d=todouble(args[0]);
    return fd_init_double(NULL,(1.0/d));}
  else {
    double val=todouble(args[0]); i=1;
    while (i<n) {
      double dval=todouble(args[i]);
      val=val/dval; i++;}
    return fd_init_double(NULL,val);}
}

static fdtype remainder_prim(fdtype x,fdtype m)
{
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(m))) {
    int ix=FD_FIX2INT(x), im=FD_FIX2INT(m);
    int r=ix%im;
    return FD_INT(r);}
  else return fd_remainder(x,m);
}

static fdtype random_prim(fdtype maxarg)
{
  int max=fd_getint(maxarg), n=u8_random(max);
  return FD_INT(n);
}

static fdtype quotient_prim(fdtype x,fdtype y)
{
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(y))) {
    int ix=FD_FIX2INT(x), iy=FD_FIX2INT(y);
    int q=ix/iy;
    return FD_INT(q);}
  else return fd_quotient(x,y);
}

static fdtype sqrt_prim(fdtype x)
{
  double v=todouble(x);
  double sqr=sqrt(v);
  return fd_init_double(NULL,sqr);
}

static fdtype round_prim(fdtype x)
{
  /* For some reason, round() doesn't work. */
  double v=todouble(x);
  double floored=floor(v);
  if (((floored+1.0)-v)>0.5)
    return fd_init_double(NULL,floored);
  else return fd_init_double(NULL,floored+1.0);
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

static fdtype config_set(fdtype var,fdtype val)
{
  int retval;
  if (FD_STRINGP(var))
    retval=fd_config_set(FD_STRDATA(var),val);
  else if (FD_SYMBOLP(var))
    retval=fd_config_set(FD_SYMBOL_NAME(var),val);
  else return fd_type_error(_("string or symbol"),"config_set",var);
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype config_default(fdtype var,fdtype val)
{
  int retval;
  if (FD_STRINGP(var))
    retval=fd_config_default(FD_STRDATA(var),val);
  else if (FD_SYMBOLP(var))
    retval=fd_config_default(FD_SYMBOL_NAME(var),val);
  else return fd_type_error(_("string or symbol"),"config_set",var);
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
        (FD_PRIM_TYPEP(pat,fd_regex_type))?(fd_regex_test(pat,keystring,-1)):
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
  if (e->data) {
    fd_decref((fdtype)(e->data));
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

/* The init function */

FD_EXPORT void fd_init_corefns_c()
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
  fd_idefn(fd_scheme_module,fd_make_cprim2("FASTCOMPARE",fastcomparefn,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("DEEP-COPY",deepcopy,1));
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
  fd_idefn(fd_scheme_module,fd_make_cprim1("CHARACTER?",characterp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("OPCODE?",opcodep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ERROR?",errorp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("APPLICABLE?",applicablep,1));

  fd_defalias(fd_scheme_module,"CHAR?","CHARACTER?");
  fd_idefn(fd_scheme_module,fd_make_cprim1("BOOLEAN?",booleanp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TRUE?",truep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FALSE?",falsep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("NUMBER?",numberp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("IMMEDIATE?",immediatep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CONSED?",consedp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TYPEOF",typeof_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("MAKE-OPCODE",make_opcode,1,
                           fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PROCEDURE-NAME",procedure_name,1));

  fd_idefn(fd_scheme_module,fd_make_cprimn("+",plus_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("-",minus_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("*",times_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("/",div_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("/~",idiv_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("REMAINDER",remainder_prim,2));

  fd_idefn(fd_scheme_module,fd_make_cprim1("RANDOM",random_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("1+",plus1,1));
  {
    fdtype minusone=fd_make_cprim1("-1+",minus1,1);
    fd_idefn(fd_scheme_module,minusone);
    fd_store(fd_scheme_module,fd_intern("1-"),minusone);
  }
  fd_idefn(fd_scheme_module,fd_make_cprim2("QUOTIENT",quotient_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SQRT",sqrt_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ROUND",round_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("CONFIG",config_get,1)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("CONFIG!",config_set,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("CONFIG-DEFAULT!",config_default,2)));
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONFIGS?",find_configs,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("CONFIG-DEF!",config_def,2,
                           fd_symbol_type,FD_VOID,-1,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("THREADGET",thread_get,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("THREADSET!",thread_set,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("THREADADD!",thread_add,2));
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

  fd_idefn(fd_scheme_module,fd_make_cprim0("GETSOURCEINFO",lisp_getsourceinfo,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("ALLSYMBOLS",lisp_all_symbols,0));

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
