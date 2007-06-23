/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/frames.h"
#include "fdb/numbers.h"
#include "fdb/support.h"

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
  return FD_INT2DTYPE(n);
}

static fdtype fastcomparefn(fdtype x,fdtype y)
{
  int n=FD_QCOMPARE(x,y);
  return FD_INT2DTYPE(n);
}

static fdtype deepcopy(fdtype x)
{
  return fd_deep_copy(x);
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
      return FD_FALSE;
    else return FD_TRUE;
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
  else if (fd_numcompare(x,FD_INT2DTYPE(0))==0)
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype do_compare(int n,fdtype *v,int testspec[3])
{
  int i=1; while (i < n) {
    int comparison=numeric_compare(v[i-1],v[i]);
    if (comparison>1) return fd_erreify();
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
static fdtype vectorp(fdtype x)
{
  if (FD_VECTORP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype numberp(fdtype x)
{
  if (FD_NUMBERP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype characterp(fdtype x)
{
  if (FD_CHARACTERP(x)) return FD_TRUE; else return FD_FALSE;
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
    return FD_INT2DTYPE(fixresult);}
  else if (generic == 0) {
    double floresult=0.0;
    i=0; while (i < n) {
      double val;
      if (FD_FIXNUMP(args[i])) val=(double)fd_getint(args[i]);
      else if (FD_PTR_TYPEP(args[i],fd_bigint_type))
	val=(double)fd_bigint_to_double((fd_bigint)args[i]);
      else val=((struct FD_DOUBLE *)args[i])->flonum;
      floresult=floresult+val;
      i++;}
    return fd_init_double(NULL,floresult);}
  else {
    fdtype result=FD_INT2DTYPE(0);
    i=0; while (i < n) {
      fdtype newv=fd_plus(result,args[i]);
      if (FD_ABORTP(newv)) {
	fd_decref(result); return newv;}
      fd_decref(result); result=newv; i++;}
    return result;}
}

static fdtype plus1(fdtype x)
{
  fdtype args[2]; args[0]=x; args[1]=FD_INT2DTYPE(1);
  return plus_lexpr(2,args);
}
static fdtype minus1(fdtype x)
{
  fdtype args[2]; args[0]=x; args[1]=FD_INT2DTYPE(-1);
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
    int fixresult=1;
    i=0; while (i < n) {
      int mult=fd_getint(args[i]);
      if (mult==0) fixresult=0;
      else {
	int prod=fixresult*mult;
	if ((prod/mult)!=fixresult) {
	  fdtype bigresult=fd_multiply(FD_INT2DTYPE(fixresult),args[i]);
	  i++; while (i<n) {
	    fdtype bigprod=fd_multiply(bigresult,args[i]);
	    fd_decref(bigresult); bigresult=bigprod; i++;}
	  return bigresult;}
	else fixresult=prod;}
      i++;}
    return FD_INT2DTYPE(fixresult);}
  else if (generic == 0) {
    double floresult=1.0;
    i=0; while (i < n) {
      double val;
      if (FD_FIXNUMP(args[i])) val=(double)FD_FIX2INT(args[i]);
      else if  (FD_PTR_TYPEP(args[i],fd_bigint_type))
	val=(double)fd_bigint_to_double((fd_bigint)args[i]);
      else val=((struct FD_DOUBLE *)args[i])->flonum;
      floresult=floresult*val;
      i++;}
    return fd_init_double(NULL,floresult);}
  else {
    fdtype result=FD_INT2DTYPE(1);
    i=0; while (i < n) {
      fdtype newv=fd_multiply(result,args[i]);
      fd_decref(result); result=newv; i++;}
    return result;}
}

static fdtype minus_lexpr(int n,fdtype *args)
{
  if (n == 1)
    if (FD_FIXNUMP(args[0]))
      return FD_INT2DTYPE(-(FD_FIX2INT(args[0])));
    else if (FD_FLONUMP(args[0]))
      return fd_init_double(NULL,-(FD_FLONUM(args[0])));
    else return fd_subtract(FD_INT2DTYPE(0),args[0]);
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
	else if  (FD_PTR_TYPEP(args[i],fd_bigint_type))
	  val=(double)fd_bigint_to_double((fd_bigint)args[i]);
	if (i==0) fixresult=val; else fixresult=fixresult-val;
	i++;}
      return FD_INT2DTYPE(fixresult);}
    else if (generic == 0) {
      double floresult=0.0;
      i=0; while (i < n) {
	double val;
	if (FD_FIXNUMP(args[i])) val=(double)FD_FIX2INT(args[i]);
	else if  (FD_PTR_TYPEP(args[i],fd_bigint_type))
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
  else if (FD_PTR_TYPEP(x,fd_bigint_type))
    return (double)fd_bigint_to_double((fd_bigint)x);
  else if (FD_PTR_TYPEP(x,fd_double_type))
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
    return fd_divide(FD_INT2DTYPE(1),args[0]);
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
    return FD_INT2DTYPE(r);}
  else return fd_remainder(x,m);
}

static fdtype random_prim(fdtype maxarg)
{
  int max=fd_getint(maxarg), n=u8_random(max);
  return FD_INT2DTYPE(n);
}

static fdtype quotient_prim(fdtype x,fdtype y)
{
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(y))) {
    int ix=FD_FIX2INT(x), iy=FD_FIX2INT(y);
    int q=ix/iy;
    return FD_INT2DTYPE(q);}
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
    return fd_parse(FD_STRDATA(string));
  else return fd_incref(string);
}

static fdtype lisp_symbol2string(fdtype sym)
{
  return fdtype_string(FD_SYMBOL_NAME(sym));
}

static fdtype lisp_string2symbol(fdtype s)
{
  return fd_intern(FD_STRING_DATA(s));
}

static fdtype config_get(fdtype var,fdtype dflt)
{
  fdtype value;
  if (FD_STRINGP(var))
    value=fd_config_get(FD_STRDATA(var));
  else if (FD_SYMBOLP(var))
    value=fd_config_get(FD_SYMBOL_NAME(var));
  else return fd_type_error(_("string or symbol"),"config_get",var);
  if (FD_VOIDP(value))
    if (FD_VOIDP(dflt)) return FD_EMPTY_CHOICE;
    else return fd_incref(dflt);
  else return value;
}

static fdtype config_set(fdtype var,fdtype val)
{
  int retval;
  if (FD_STRINGP(var))
    retval=fd_config_set(FD_STRDATA(var),val);
  else if (FD_SYMBOLP(var))
    retval=fd_config_set(FD_SYMBOL_NAME(var),val);
  else return fd_type_error(_("string or symbol"),"config_set",var);
  if (retval<0) return fd_erreify();
  else return FD_VOID;
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

static fdtype config_def(fdtype var,fdtype handler)
{
  int retval=
    fd_register_config(FD_SYMBOL_NAME(var),
		       lconfig_get,lconfig_set,(void *) handler);
  if (retval<0) return fd_erreify();
  fd_incref(handler);
  return FD_VOID;
}

static fdtype thread_get(fdtype var)
{
  fdtype value=fd_thread_get(var);
  if (FD_VOIDP(value)) return FD_EMPTY_CHOICE;
  else return value;
}

static fdtype thread_set(fdtype var,fdtype val)
{
  return fd_thread_set(var,val);
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
  fd_for_source_files(add_sourceid,&result);
  return result;
}

/* The init function */

FD_EXPORT void fd_init_corefns_c()
{
  fd_register_source_file(versionid);

  fd_idefn(fd_scheme_module,fd_make_cprim2("EQ?",eqp,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("EQV?",eqvp,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("EQUAL?",equalp,2));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("IDENTICAL?",equalp,2)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("OVERLAPS?",overlapsp,2)));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("CONTAINS?",containsp,2)));
  fd_idefn(fd_scheme_module,fd_make_cprim2("COMPARE",comparefn,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("FASTCOMPARE",fastcomparefn,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("DEEP-COPY",deepcopy,1));

  fd_idefn(fd_scheme_module,fd_make_cprimn("<",lt,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("<=",lte,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("=",numeqp,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn(">=",gte,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn(">",gt,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("!=",numneqp,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ZERO?",lisp_zerop,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("STRING?",stringp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SYMBOL?",symbolp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PAIR?",pairp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LIST?",listp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("VECTOR?",vectorp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CHARACTER?",characterp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("OPCODE?",opcodep,1));

  fd_defalias(fd_scheme_module,"CHAR?","CHARACTER?");
  fd_idefn(fd_scheme_module,fd_make_cprim1("BOOLEAN?",booleanp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("NUMBER?",numberp,1));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("MAKE-OPCODE",make_opcode,1,
			   fd_fixnum_type,FD_VOID));
  
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

  fd_idefn(fd_scheme_module,fd_make_cprim2("CONFIG",config_get,1));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("CONFIG!",config_set,2)));
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONFIG-DEF!",config_def,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("THREADGET",thread_get,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("THREADSET!",thread_set,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("INTERN",lisp_intern,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("SYMBOL->STRING",lisp_symbol2string,1,
			   fd_symbol_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("STRING->SYMBOL",lisp_string2symbol,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("STRING->LISP",lisp_string2lisp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("PARSE-ARG",lisp_parse_arg,1));

  fd_idefn(fd_scheme_module,fd_make_cprim0("GETSOURCEINFO",lisp_getsourceinfo,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("ALLSYMBOLS",lisp_all_symbols,0));

}


/* The CVS log for this file
   $Log: corefns.c,v $
   Revision 1.76  2006/01/31 03:18:19  haase
   Fix zero multiplication bug

   Revision 1.75  2006/01/27 16:41:06  haase
   Fixed fixnum overflow errors

   Revision 1.74  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.73  2005/12/26 20:14:26  haase
   Further fixes to type reorganization

   Revision 1.72  2005/12/20 20:46:22  haase
   Fixes to modulo and quotient

   Revision 1.71  2005/12/20 19:05:53  haase
   Switched to u8_random and added RANDOMSEED config variable

   Revision 1.70  2005/12/19 00:43:43  haase
   Moved INEXACT? (or flonump) into numeric

   Revision 1.69  2005/12/17 05:57:07  haase
   Added R4RS functions

   Revision 1.68  2005/11/11 05:40:19  haase
   Added PARSE-ARG

   Revision 1.67  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.66  2005/06/25 16:25:47  haase
   Added threading primitives

   Revision 1.65  2005/05/30 18:04:43  haase
   Removed legacy numeric plugin layer

   Revision 1.64  2005/05/30 17:49:59  haase
   Distinguished FD_QCOMPARE and FDTYPE_COMPARE

   Revision 1.63  2005/05/23 00:53:24  haase
   Fixes to header ordering to get off_t consistently defined

   Revision 1.62  2005/05/22 20:30:03  haase
   Pass initialization errors out of config-def! and other error improvements

   Revision 1.61  2005/05/19 22:06:07  haase
   Added full support for rationals and complex numbers

   Revision 1.60  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.59  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.58  2005/04/26 13:10:28  haase
   Fixed OVERLAPS? for empty choices

   Revision 1.57  2005/04/25 19:15:43  haase
   Added number? and inexact? scheme primitives

   Revision 1.56  2005/04/21 19:03:26  haase
   Add initialization procedures

   Revision 1.55  2005/04/15 16:20:02  haase
   Added symbol->string

   Revision 1.54  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.53  2005/04/15 13:58:10  haase
   Changed variable handing to accomodate unset (void) bindings

   Revision 1.52  2005/04/14 16:23:16  haase
   Whitespace changes

   Revision 1.51  2005/04/14 01:27:34  haase
   Added config-def! for defining scheme configuration procedures

   Revision 1.50  2005/04/14 00:27:22  haase
   Fix default! primitive to first set and then bind

   Revision 1.49  2005/04/13 15:00:55  haase
   Added bound? and default! operations

   Revision 1.48  2005/04/12 16:24:03  haase
   Made overlaps? be non-deterministic

   Revision 1.47  2005/04/12 01:25:47  haase
   Made IDENTICAL? use the equalp primitive

   Revision 1.46  2005/04/12 01:15:45  haase
   Added fd_overlapp and its applications

   Revision 1.45  2005/04/11 21:28:24  haase
   Fix bugs in error propogation for comparisons

   Revision 1.44  2005/04/09 22:44:04  haase
   Added DEEP-COPY primitive

   Revision 1.43  2005/04/07 15:27:48  haase
   Added threadget/set functions

   Revision 1.42  2005/03/26 20:06:52  haase
   Exposed APPLY to scheme and made optional arguments generally available

   Revision 1.41  2005/03/24 17:24:49  haase
   Fix multiply primitive to start with the right initial value when doing generic reduction

   Revision 1.40  2005/03/16 22:22:46  haase
   Added bignums

   Revision 1.39  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.38  2005/03/01 19:39:18  haase
   Added config set operation and renamed GETSOURCEIDS to GETSOURCEINFO

   Revision 1.37  2005/03/01 02:10:13  haase
   Renamed CONFIG-GET to CONFIG

   Revision 1.36  2005/02/19 16:22:43  haase
   Added ZERO?, RANDOM, and ALLSYMBOLS

   Revision 1.35  2005/02/18 00:42:37  haase
   Added type checking for arithmetic operations

   Revision 1.34  2005/02/17 02:36:54  haase
   Added QUOTIENT primitive

   Revision 1.33  2005/02/13 23:53:56  haase
   Added COMPARE primitive

   Revision 1.32  2005/02/12 01:34:52  haase
   Added simple portfns and default output ports

   Revision 1.31  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
