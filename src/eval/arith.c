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
#include "framerd/numbers.h"

#define NUMBERP(x) \
  ((FD_FIXNUMP(x)) || (FD_FLONUMP(x)) || (FD_BIGINTP(x)) || \
   (FD_COMPLEXP(x)) || (FD_RATIONALP(x)))
#define REALP(x) \
  ((FD_FIXNUMP(x)) || (FD_FLONUMP(x)) || (FD_BIGINTP(x)) || (FD_RATIONALP(x)))
#define INTEGERP(x) ((FD_FIXNUMP(x)) || (FD_BIGINTP(x)))

#include <libu8/libu8.h>
#include <libu8/u8printf.h>

#include <errno.h>
#include <math.h>

static fdtype complexp(fdtype x)
{
  if (NUMBERP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype fixnump(fdtype x)
{
  if (FD_FIXNUMP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype bignump(fdtype x)
{
  if (FD_BIGINTP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype integerp(fdtype x)
{
  if (INTEGERP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype rationalp(fdtype x)
{
  if ((FD_FIXNUMP(x)) || (FD_BIGINTP(x)) || (FD_RATIONALP(x)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype exactp(fdtype x)
{
  if (FD_COMPLEXP(x)) {
    fdtype real = FD_REALPART(x), imag = FD_IMAGPART(x);
    if ((FD_FLONUMP(real)) || (FD_FLONUMP(imag))) return FD_FALSE;
    else return FD_TRUE;}
  else if (FD_FLONUMP(x)) return FD_FALSE;
  else if ((FD_FIXNUMP(x)) || (FD_BIGINTP(x)) || (FD_RATIONALP(x)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype inexactp(fdtype x)
{
  if (FD_COMPLEXP(x)) {
    fdtype real = FD_REALPART(x), imag = FD_IMAGPART(x);
    if ((FD_FLONUMP(real)) || (FD_FLONUMP(imag))) return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_FLONUMP(x)) return FD_TRUE;
  else if ((FD_FIXNUMP(x)) || (FD_BIGINTP(x)) || (FD_RATIONALP(x)))
    return FD_FALSE;
  else return FD_FALSE;
}

static fdtype oddp(fdtype x)
{
  if (FD_FIXNUMP(x)) {
    long long ival = FD_FIX2INT(x);
    if (ival%2) return FD_TRUE; else return FD_FALSE;}
  else if (FD_BIGINTP(x)) {
    fdtype remainder = fd_remainder(x,FD_INT(2));
    if (FD_ABORTP(remainder)) return remainder;
    else if (FD_FIXNUMP(remainder))
      if (FD_INT(remainder)) return FD_TRUE; else return FD_FALSE;
    else {
      fd_decref(remainder); return FD_FALSE;}}
  else return FD_FALSE;
}

static fdtype evenp(fdtype x)
{
  if (FD_FIXNUMP(x)) {
    long long ival = FD_FIX2INT(x);
    if (ival%2) return FD_FALSE; else return FD_TRUE;}
  else if (FD_BIGINTP(x)) {
    fdtype remainder = fd_remainder(x,FD_INT(2));
    if (FD_ABORTP(remainder)) return remainder;
    else if (FD_FIXNUMP(remainder))
      if (FD_INT(remainder)) return FD_FALSE; else return FD_TRUE;
    else {
      fd_decref(remainder); return FD_FALSE;}}
  else return FD_FALSE;
}

static fdtype realp(fdtype x)
{
  if (REALP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype positivep(fdtype x)
{
  int sgn = fd_numcompare(x,FD_INT(0));
  if (sgn>0) return FD_TRUE; else return FD_FALSE;
}
static fdtype negativep(fdtype x)
{
  int sgn = fd_numcompare(x,FD_INT(0));
  if (sgn<0) return FD_TRUE; else return FD_FALSE;
}

/* Basic ops */

/* Arithmetic */

static fdtype plus_lexpr(int n,fdtype *args)
{
  if (n==0)
    return FD_FIXNUM_ZERO;
  else if (n==1) {
    fdtype x = args[0];
    if (FD_FIXNUMP(x)) return x;
    else return fd_incref(x);}
  else if (n==2) 
    return fd_plus(args[0],args[1]);
  else {
    int i = 0; int floating = 0, generic = 0, vector = 0;
    while (i < n)
      if (FD_FIXNUMP(args[i])) i++;
      else if (FD_FLONUMP(args[i])) {floating = 1; i++;}
      else if ((FD_VECTORP(args[i]))||(FD_NUMVECP(args[i]))) {
        generic = 1; vector = 1; i++;}
      else {generic = 1; i++;}
    if ((floating==0) && (generic==0)) {
      long long fixresult = 0;
      i = 0; while (i < n) {
        long long val = 0;
        if (FD_FIXNUMP(args[i]))
          val = fd_getint(args[i]);
        fixresult = fixresult+val;
        i++;}
      return FD_INT(fixresult);}
    else if (generic == 0) {
      double floresult = 0.0;
      i = 0; while (i < n) {
        double val;
        if (FD_FIXNUMP(args[i])) val = (double)fd_getint(args[i]);
        else if (FD_BIGINTP(args[i]))
          val = (double)fd_bigint_to_double((fd_bigint)args[i]);
        else val = ((struct FD_FLONUM *)args[i])->floval;
        floresult = floresult+val;
        i++;}
      return fd_init_double(NULL,floresult);}
    else if (vector) {
      fdtype result = fd_plus(args[0],args[1]);
      if (FD_ABORTP(result)) return result;
      i = 2; while (i < n) {
        fdtype newv = fd_plus(result,args[i]);
        if (FD_ABORTP(newv)) {
          fd_decref(result); return newv;}
        fd_decref(result); result = newv; i++;}
      return result;}
    else {
      fdtype result = FD_INT(0);
      i = 0; while (i < n) {
        fdtype newv = fd_plus(result,args[i]);
        if (FD_ABORTP(newv)) {
          fd_decref(result); return newv;}
        fd_decref(result); result = newv; i++;}
      return result;}
  }
}

static fdtype plus1(fdtype x)
{
  if (FD_FIXNUMP(x)) {
    long long iv = fd_getint(x); iv++;
    return FD_INT2DTYPE(iv);}
  else if (FD_FLONUMP(x)) {
    fd_double iv = FD_FLONUM(x); iv = iv+1;
    return fd_make_flonum(iv);}
  else {
    fdtype args[2]; args[0]=x; args[1]=FD_INT(1);
    return plus_lexpr(2,args);}
}
static fdtype minus1(fdtype x)
{
  if (FD_FIXNUMP(x)) {
    long long iv = fd_getint(x); iv--;
    return FD_INT2DTYPE(iv);}
  else if (FD_FLONUMP(x)) {
    fd_double iv = FD_FLONUM(x); iv = iv-1;
    return fd_make_flonum(iv);}
  else {
    fdtype args[2]; args[0]=x; args[1]=FD_INT(-1);
    return plus_lexpr(2,args);}
}

static fdtype times_lexpr(int n,fdtype *args)
{
  int i = 0; int floating = 0, generic = 0;
  if (n==1) {
    fdtype arg = args[0];
    if (FD_FIXNUMP(arg)) return arg;
    else if (FD_NUMBERP(arg))
      return  fd_incref(arg);
    else if ((FD_NUMVECP(arg))||(FD_VECTORP(arg)))
      return fd_incref(arg);
    return fd_type_error(_("number"),"times_lexpr",fd_incref(arg));}
  else if (n==2) 
    return fd_multiply(args[0],args[1]);
  else {
    while (i < n)
      if (FD_FIXNUMP(args[i])) i++;
      else if (FD_FLONUMP(args[i])) {floating = 1; i++;}
      else {generic = 1; i++;}
    if ((floating==0) && (generic==0)) {
      long long fixresult = 1;
      i = 0; while (i < n) {
        long long mult = fd_getint(args[i]);
        if (mult==0) return FD_INT(0);
        else {
          int q = ((mult>0)?(FD_MAX_FIXNUM/mult):(FD_MIN_FIXNUM/mult));
          if ((fixresult>0)?(fixresult>q):((-fixresult)>q)) {
            fdtype bigresult = fd_multiply(FD_INT(fixresult),args[i]);
            i++; while (i<n) {
              fdtype bigprod = fd_multiply(bigresult,args[i]);
              fd_decref(bigresult); bigresult = bigprod; i++;}
            return bigresult;}
          else fixresult = fixresult*mult;}
        i++;}
      return FD_INT(fixresult);}
    else if (generic == 0) {
      double floresult = 1.0;
      i = 0; while (i < n) {
        double val;
        if (FD_FIXNUMP(args[i])) val = (double)FD_FIX2INT(args[i]);
        else if  (FD_BIGINTP(args[i]))
          val = (double)fd_bigint_to_double((fd_bigint)args[i]);
        else val = ((struct FD_FLONUM *)args[i])->floval;
        floresult = floresult*val;
        i++;}
      return fd_init_double(NULL,floresult);}
    else {
      fdtype result = FD_INT(1);
      i = 0; while (i < n) {
        fdtype newv = fd_multiply(result,args[i]);
        fd_decref(result); result = newv; i++;}
      return result;}
  }
}

static fdtype minus_lexpr(int n,fdtype *args)
{
  if (n == 1) {
    fdtype arg = args[0];
    if (FD_FIXNUMP(arg))
      return FD_INT(-(FD_FIX2INT(arg)));
    else if (FD_FLONUMP(arg))
      return fd_init_double(NULL,-(FD_FLONUM(arg)));
    else if ((FD_VECTORP(arg))||(FD_NUMVECP(arg)))
      return fd_multiply(arg,FD_FIX2INT(-1));
    else return fd_subtract(FD_INT(0),arg);}
  else if (n == 2)
    return fd_subtract(args[0],args[1]);
  else {
    int i = 0; int floating = 0, generic = 0, vector = 0;
    while (i < n) {
      if (FD_FIXNUMP(args[i])) i++;
      else if (FD_FLONUMP(args[i])) {floating = 1; i++;}
      else if ((FD_VECTORP(args[i]))||(FD_NUMVECP(args[i]))) {
        vector = 1; generic = 1;}
      else {generic = 1; i++;}}
    if ((floating==0) && (generic==0)) {
      long long fixresult = 0;
      i = 0; while (i < n) {
        long long val = 0;
        if (FD_FIXNUMP(args[i])) val = FD_FIX2INT(args[i]);
        else if  (FD_BIGINTP(args[i]))
          val = (double)fd_bigint_to_double((fd_bigint)args[i]);
        if (i==0) fixresult = val; else fixresult = fixresult-val;
        i++;}
      return FD_INT(fixresult);}
    else if (generic == 0) {
      double floresult = 0.0;
      i = 0; while (i < n) {
        double val;
        if (FD_FIXNUMP(args[i])) val = (double)FD_FIX2INT(args[i]);
        else if  (FD_BIGINTP(args[i]))
          val = (double)fd_bigint_to_double((fd_bigint)args[i]);
        else val = ((struct FD_FLONUM *)args[i])->floval;
        if (i==0) floresult = val; else floresult = floresult-val;
        i++;}
      return fd_init_double(NULL,floresult);}
    else if (vector) {
      fdtype result = fd_subtract(args[0],args[1]);
      i = 2; while (i < n) {
        fdtype newv = fd_subtract(result,args[i]);
        fd_decref(result); result = newv; i++;}
      return result;}
    else {
      fdtype result = fd_incref(args[0]);
      i = 1; while (i < n) {
        fdtype newv = fd_subtract(result,args[i]);
        fd_decref(result); result = newv; i++;}
      return result;}}
}

static double todouble(fdtype x)
{
  if (FD_FIXNUMP(x))
    return (double)(FD_FIX2INT(x));
  else if (FD_BIGINTP(x))
    return (double)fd_bigint_to_double((fd_bigint)x);
  else if (FD_FLONUMP(x))
    return (((struct FD_FLONUM *)x)->floval);
  else {
    /* This won't really work, but we should catch the error before
       this point.  */
    fd_seterr(fd_TypeError,"todouble",NULL,x);
    return -1.0;}
}

static fdtype div_lexpr(int n,fdtype *args)
{
  int all_double = 1, i = 0;
  while (i<n)
    if (FD_FLONUMP(args[i])) i++;
    else {all_double = 0; break;}
  if (all_double)
    if (n == 1)
      return fd_init_double(NULL,(1.0/(FD_FLONUM(args[0]))));
    else {
      double val = FD_FLONUM(args[0]); i = 1;
      while (i<n) {val = val/FD_FLONUM(args[i]); i++;}
      return fd_init_double(NULL,val);}
  else if (n==1)
    return fd_divide(FD_INT(1),args[0]);
  else {
    fdtype value = fd_incref(args[0]); i = 1;
    while (i<n) {
      fdtype newv = fd_divide(value,args[i]);
      fd_decref(value); value = newv; i++;}
    return value;}
}

static fdtype idiv_lexpr(int n,fdtype *args)
{
  int all_double = 1, i = 0;
  while (i<n)
    if (FD_FLONUMP(args[i])) i++;
    else if (!((FD_FIXNUMP(args[i])) || (FD_BIGINTP(args[i]))))
      return fd_type_error(_("scalar"),"idiv_lexpr",args[i]);
    else {all_double = 0; i++;}
  if (all_double)
    if (n == 1)
      return fd_init_double(NULL,(1.0/(FD_FLONUM(args[0]))));
    else {
      double val = FD_FLONUM(args[0]); i = 1;
      while (i<n) {val = val/FD_FLONUM(args[i]); i++;}
      return fd_init_double(NULL,val);}
  else if (n==1) {
    double d = todouble(args[0]);
    return fd_init_double(NULL,(1.0/d));}
  else {
    double val = todouble(args[0]); i = 1;
    while (i<n) {
      double dval = todouble(args[i]);
      val = val/dval; i++;}
    return fd_init_double(NULL,val);}
}

static fdtype remainder_prim(fdtype x,fdtype m)
{
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(m))) {
    long long ix = FD_FIX2INT(x), im = FD_FIX2INT(m);
    long long r = ix%im;
    return FD_INT(r);}
  else return fd_remainder(x,m);
}

static fdtype random_prim(fdtype maxarg)
{
  if (FD_INTEGERP(maxarg)) {
    long long max = fd_getint(maxarg), n = u8_random(max);
    if ((max<=0)||(max>0x80000000))
      return fd_type_error("Small positive integer","random_prim",maxarg);
    else return FD_INT(n);}
  else if (FD_FLONUMP(maxarg)) {
    double flomax = FD_FLONUM(maxarg);
    long long  intmax = (long long)flomax;
    long long n = u8_random(intmax);
    double rval = (double) n;
    return fd_make_flonum(rval);}
  else return fd_type_error("integer or flonum","random_prim",maxarg);
}

/* Making some numbers */

static fdtype make_rational(fdtype n,fdtype d)
{
  return fd_make_rational(n,d);
}

static fdtype numerator_prim(fdtype x)
{
  if (FD_RATIONALP(x)) {
    fdtype num = FD_NUMERATOR(x);
    return fd_incref(num);}
  else if (FD_NUMBERP(x)) return fd_incref(x);
  else return fd_type_error("number","numerator_prim",x);
}

static fdtype denominator_prim(fdtype x)
{
  if (FD_RATIONALP(x)) {
    fdtype den = FD_DENOMINATOR(x);
    return fd_incref(den);}
  else if (FD_NUMBERP(x)) return FD_INT(1);
  else return fd_type_error("number","denominator_prim",x);
}

static fdtype make_complex(fdtype r,fdtype i)
{
  return fd_make_complex(r,i);
}

static fdtype real_part_prim(fdtype x)
{
  if (REALP(x)) return fd_incref(x);
  else if (FD_COMPLEXP(x)) {
    fdtype r = FD_REALPART(x);
    return fd_incref(r);}
  else return fd_type_error("number","real_part_prim",x);
}

static fdtype imag_part_prim(fdtype x)
{
  if (REALP(x)) return FD_INT(0);
  else if (FD_COMPLEXP(x)) {
    fdtype r = FD_IMAGPART(x);
    return fd_incref(r);}
  else return fd_type_error("number","imag_part_prim",x);
}

static double doublearg(fdtype x,fdtype *whoops)
{
  if (FD_FIXNUMP(x))
    return (double)(FD_FIX2INT(x));
  else if (FD_BIGINTP(x))
    return (double)fd_bigint_to_double((fd_bigint)x);
  else if (FD_FLONUMP(x))
    return (((struct FD_FLONUM *)x)->floval);
  else {
    *whoops = fd_type_error("number","doublearg",x);
    return 0;}
}

static fdtype exact2inexact(fdtype x)
{
  return fd_make_inexact(x);
}

static fdtype inexact2exact(fdtype x)
{
  return fd_make_exact(x);
}

static fdtype toexact(fdtype x,fdtype direction)
{
  long long dir = -1;
  if ((FD_VOIDP(direction))||(FD_FALSEP(direction)))
    dir = -1;
  else if (FD_FIXNUMP(direction))
    dir = FD_FIX2INT(direction);
  else dir = 0;
  if (FD_FLONUMP(x)) {
    if (dir== -1)
      return fd_make_exact(x);
    else {
      double d = FD_FLONUM(x);
      struct FD_FLONUM tmp;
      FD_INIT_STATIC_CONS(&tmp,fd_flonum_type);
      if (dir==1) 
        tmp.floval = ceil(d);
      else tmp.floval = round(d);
      return fd_make_exact((fdtype)(&tmp));}}
  else if (FD_COMPLEXP(x)) {
    fdtype real = FD_REALPART(x), imag = FD_IMAGPART(x);
    if ((FD_FLONUMP(real))||(FD_FLONUMP(imag))) {
      struct FD_COMPLEX *num = u8_alloc(struct FD_COMPLEX);
      fdtype xreal = toexact(real,direction);
      fdtype ximag = toexact(imag,direction);
      FD_INIT_CONS(num,fd_complex_type);
      num->realpart = xreal; num->imagpart = ximag;
      return (fdtype) num;}
    else {
      fd_incref(x);
      return x;}}
  else if (FD_NUMBERP(x)) {
    fd_incref(x);
    return x;}
  else return fd_type_error("number","toexact",x);
}

#define arithdef(sname,lname,cname) \
  static fdtype lname(fdtype x) { \
    fdtype err = FD_VOID; double val = doublearg(x,&err); \
    errno = 0; \
    if (FD_VOIDP(err)) { \
      double result = cname(val); \
      if (errno==0) return fd_init_double(NULL,result); \
      else {u8_graberr(-1,sname,u8_mkstring("%f",val)); \
            return FD_ERROR_VALUE;}} \
    else return err;}

arithdef("SQRT",lsqrt,sqrt);
arithdef("COS",lcos,cos);
arithdef("ACOS",lacos,acos);
arithdef("SIN",lsin,sin);
arithdef("ASIN",lasin,asin);
arithdef("ATAN",latan,atan);
arithdef("TAN",ltan,tan);
arithdef("LOG",llog,log);
arithdef("EXP",lexp,exp);

#define arithdef2(sname,lname,cname) \
  static fdtype lname(fdtype x,fdtype y) { \
    fdtype err = FD_VOID; double xval, yval; \
    xval = doublearg(x,&err);                  \
    if (!(FD_VOIDP(err))) return err;       \
    yval = doublearg(y,&err);                  \
    errno = 0;                                \
    if (FD_VOIDP(err)) {                    \
      double result = cname(xval,yval);       \
      if (errno==0) return fd_init_double(NULL,result); \
      else {u8_graberr(-1,sname,u8_mkstring("%f %f",xval,yval)); \
            return FD_ERROR_VALUE;}}          \
    else return err;}

arithdef2("ATAN2",latan2,atan2);
arithdef2("POW",lpow,pow);

#undef arithdef
#undef arithdef2

static fdtype pow_prim(fdtype v,fdtype n)
{
  if ((FD_EXACTP(v))&&
      (FD_FIXNUMP(n))&&(FD_FIX2INT(n)>=0)&&
      (FD_FIX2INT(n)<1000000)) {
    if (FD_ZEROP(v)) return FD_FIXNUM_ONE;
    else {
      long long i = 0, how_many = FD_FIX2INT(n);
      fdtype prod = FD_INT(1); while (i<how_many) {
        fdtype tmp = fd_multiply(prod,v);
        fd_decref(prod); prod = tmp;
        i++;}
      return prod;}}
  else {
    fdtype err = FD_VOID;
    fdtype dv = doublearg(v,&err);
    fdtype dn = (FD_VOIDP(err)) ? doublearg(n,&err) : (0);
    if (FD_ABORTP(err)) return err;
    else {
      double result = pow(dv,dn);
      return fd_make_flonum(result);}}
}

static fdtype nthroot_prim(fdtype v,fdtype n)
{
  fdtype err = FD_VOID;
  double dv = doublearg(v,&err);
  double dn = (FD_VOIDP(err)) ? (doublearg(n,&err)) :(0);
  double dexp = (FD_VOIDP(err)) ? (1/dn) : (0);
  if (FD_ABORTP(err))
    return err;
  else {
    double result = pow(dv,dexp);
    if ((remainder(result,1.0)==0)&&
        (FD_UINTP(n))&&(FD_FIX2INT(n)<1024)&&
        ((FD_FIXNUMP(v))||(FD_BIGINTP(v)))) {
      long long introot = (long long) floor(result);
      fdtype root = FD_INT(introot), prod = FD_INT(1);
      int i = 0, lim = FD_FIX2INT(n); while (i<lim) {
        fdtype tmp = fd_multiply(prod,root);
        fd_decref(prod); prod = tmp;
        i++;}
      if (fd_numcompare(v,prod)==0) {
        fd_decref(prod);
        return root;}
      else {
        fd_decref(prod);
        return fd_make_flonum(result);}}
    else return fd_make_flonum(result);}
}

static fdtype inexact_nthroot_prim(fdtype v,fdtype n)
{
  fdtype err = FD_VOID;
  double dv = doublearg(v,&err);
  double dn = (FD_VOIDP(err)) ? (doublearg(n,&err)) :(0);
  double dexp = (FD_VOIDP(err)) ? (1/dn) : (0);
  if (FD_ABORTP(err))
    return err;
  else {
    double result = pow(dv,dexp);
    return fd_make_flonum(result);}
}

/* Min/Max operators, etc */


static fdtype min_prim(int n,fdtype *args)
{
  if (n==0) 
    return fd_err(fd_TooFewArgs,"max_prim",NULL,FD_VOID);
  else {
    fdtype result = args[0]; int i = 1, inexact = FD_FLONUMP(args[0]);
    while (i<n) {
      int cmp = fd_numcompare(args[i],result);
      if (cmp>1) return FD_ERROR_VALUE;
      if (FD_FLONUMP(args[i])) inexact = 1;
      if (cmp<0) result = args[i];
      i++;}
    if (inexact)
      if (FD_FLONUMP(result))
        return fd_incref(result);
      else {
        fdtype err = FD_VOID;
        double asdouble = doublearg(result,&err);
        if (FD_VOIDP(err))
          return fd_init_double(NULL,asdouble);
        else return err;}
    else return fd_incref(result);}
}

static fdtype max_prim(int n,fdtype *args)
{
  if (n==0) return fd_err(fd_TooFewArgs,"max_prim",NULL,FD_VOID);
  else {
    fdtype result = args[0];
    int i = 1, inexact = FD_FLONUMP(args[0]);
    while (i<n) {
      int cmp = fd_numcompare(args[i],result);
      if (cmp>1) return FD_ERROR_VALUE;
      if (FD_FLONUMP(args[i])) inexact = 1;
      if (cmp>0) result = args[i];
      i++;}
    if (inexact)
      if (FD_FLONUMP(result))
        return fd_incref(result);
      else {
        fdtype err = FD_VOID;
        double asdouble = doublearg(result,&err);
        if (FD_VOIDP(err))
          return fd_init_double(NULL,asdouble);
        else return err;}
    else return fd_incref(result);}
}

static fdtype abs_prim(fdtype x)
{
  if (FD_FIXNUMP(x)) {
    long long ival = FD_FIX2INT(x);
    assert((ival<0) == ((FD_FIX2INT(x))<0));
    if (ival<0) {
      long long aval = -ival;
      return FD_INT(aval);}
    else return x;}
  else if (fd_numcompare(x,FD_INT(0))<0)
    return fd_subtract(FD_INT(0),x);
  else return fd_incref(x);
}

static fdtype modulo_prim(fdtype x,fdtype b)
{
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(b))) {
    long long ix = FD_FIX2INT(x), ib = FD_FIX2INT(b);
    if (ix==0) return FD_INT(0);
    else if (ib==0)
      return fd_type_error("nonzero","modulo_prim",b);
    else if (((ix>0) && (ib>0)) || ((ix<0) && (ib<0)))
      return FD_INT((ix%ib));
    else {
      long long rem = ix%ib;
      if (rem) return FD_INT(ib+rem);
      else return FD_INT(rem);}}
  else {
    int xsign = fd_numcompare(x,FD_INT(0));
    int bsign = fd_numcompare(b,FD_INT(0));
    if (xsign==0) return FD_INT(0);
    else if (bsign==0)
      return fd_type_error("nonzero","modulo_prim",b);
    else if (((xsign>0) && (bsign>0)) || ((xsign<0) && (bsign<0)))
      return fd_remainder(x,b);
    else {
      fdtype rem = fd_remainder(x,b);
      if ((FD_FIXNUMP(rem)) && (FD_FIX2INT(rem)==0))
        return rem;
      else {
        fdtype result = fd_plus(b,rem);
        fd_decref(rem);
        return result;}}}
}

static fdtype gcd_prim(fdtype x,fdtype y)
{
  if ((INTEGERP(x)) && (INTEGERP(y)))
    return fd_gcd(x,y);
  else if (INTEGERP(y))
    return fd_type_error("integer","gcd_prim",x);
  else return fd_type_error("integer","gcd_prim",y);
}

static fdtype lcm_prim(fdtype x,fdtype y)
{
  if ((INTEGERP(x)) && (INTEGERP(y)))
    return fd_lcm(x,y);
  else if (INTEGERP(y))
    return fd_type_error("integer","lcm_prim",x);
  else return fd_type_error("integer","lcm_prim",y);
}

static fdtype truncate_prim(fdtype x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double d = trunc(FD_FLONUM(x));
    return fd_init_double(NULL,d);}
  else if (FD_RATIONALP(x)) {
    fdtype n = FD_NUMERATOR(x), d = FD_DENOMINATOR(x);
      fdtype q = fd_quotient(n,d);
      return q;}
  else return fd_type_error(_("scalar"),"floor_prim",x);
}

static fdtype floor_prim(fdtype x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double d = floor(FD_FLONUM(x));
    return fd_init_double(NULL,d);}
  else if (FD_RATIONALP(x)) {
    fdtype n = FD_NUMERATOR(x), d = FD_DENOMINATOR(x);
    fdtype q = fd_quotient(n,d);
    if (fd_numcompare(x,FD_INT(0))<0) {
      fdtype qminus = fd_subtract(q,FD_INT(1));
      fd_decref(q);
      return qminus;}
    else return q;}
  else return fd_type_error(_("scalar"),"floor_prim",x);
}

static fdtype ceiling_prim(fdtype x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double d = ceil(FD_FLONUM(x));
    return fd_init_double(NULL,d);}
  else if (FD_RATIONALP(x)) {
    fdtype n = FD_NUMERATOR(x), d = FD_DENOMINATOR(x);
    fdtype q = fd_quotient(n,d);
    if (fd_numcompare(x,FD_INT(0))>0) {
      fdtype qminus = fd_plus(q,FD_INT(1));
      fd_decref(q);
      return qminus;}
    else return q;}
  else return fd_type_error(_("scalar"),"ceiling_prim",x);
}

static fdtype round_prim(fdtype x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double v = FD_FLONUM(x);
    double f = floor(v), c = ceil(v);
    if ((v<0) ? ((c-v)>=(v-f)) : ((c-v)>(v-f)))
      return fd_init_double(NULL,f);
    else return fd_init_double(NULL,c);}
  else if (FD_RATIONALP(x)) {
    fdtype n = FD_NUMERATOR(x), d = FD_DENOMINATOR(x);
    fdtype q = fd_quotient(n,d);
    fdtype r = fd_remainder(n,d);
    fdtype halfd = fd_quotient(d,FD_INT(2));
    if (fd_numcompare(r,halfd)<0) {
      fd_decref(halfd); fd_decref(r);
      return q;}
    else if (fd_numcompare(x,FD_INT(0))<0) {
      fdtype qplus = fd_subtract(q,FD_INT(1));
      fd_decref(q);
      return qplus;}
    else {
      fdtype qminus = fd_plus(q,FD_INT(1));
      fd_decref(q);
      return qminus;}}
  else return fd_type_error(_("scalar"),"floor_prim",x);
}

static double doround(double x)
{
  double c = ceil(x), f = floor(x);
  if ((x-f)<0.5) return f; else return c;
}

static fdtype scalerep_prim(fdtype x,fdtype scalearg)
{
  long long scale = fd_getint(scalearg);
  if (scale<0)
    if (FD_FIXNUMP(x)) return x;
    else if (FD_FLONUMP(x)) {
      long long factor = -scale;
      double val = FD_FLONUM(x), factor_up = doround(val*factor);
      long long ival = factor_up;
      return fd_conspair(FD_INT(ival),scalearg);}
    else return FD_EMPTY_CHOICE;
  else if (FD_FIXNUMP(x)) {
    long long ival = FD_FIX2INT(x), rem = ival%scale, base = (ival/scale);
    if (rem==0) return fd_conspair(x,scalearg);
    else if (rem*2>scale)
      if (ival>0)
        return fd_conspair(FD_INT((base+1)*scale),scalearg);
      else return fd_conspair(FD_INT((base-1)*scale),scalearg);
    else return fd_conspair(FD_INT(base*scale),scalearg);}
  else if (FD_FLONUMP(x)) {
    double dv = FD_FLONUM(x);
    double scaled = doround(dv/scale)*scale;
    long long ival = scaled;
    return fd_conspair(FD_INT(ival),scalearg);}
  else return FD_EMPTY_CHOICE;
}

/* More simple arithmetic functions */

static fdtype quotient_prim(fdtype x,fdtype y)
{
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(y))) {
    long long ix = FD_FIX2INT(x), iy = FD_FIX2INT(y);
    long long q = ix/iy;
    return FD_INT(q);}
  else return fd_quotient(x,y);
}

static fdtype sqrt_prim(fdtype x)
{
  double v = todouble(x);
  double sqr = sqrt(v);
  return fd_init_double(NULL,sqr);
}

/* Log and exponent functions */

static fdtype ilog_prim(fdtype n,fdtype base_arg)
{
  long long base = fd_getint(base_arg), limit = fd_getint(n);
  long long count = 0, exp = 1;
  while (exp<limit) {
    count++; exp = exp*base;}
  return FD_INT(count);
}

/* HASHPTR */

static fdtype hashptr_prim(fdtype x)
{
  unsigned long long intval = (unsigned long long)x;
  if ((intval<FD_MAX_FIXNUM)&&(intval>FD_MIN_FIXNUM))
    return FD_FIX2INT(((int)intval));
  else return (fdtype)fd_ulong_long_to_bigint(intval);
}

static fdtype hashref_prim(fdtype x)
{
  unsigned long long intval = (unsigned long long)x;
  char buf[64];
  if ((intval<FD_MAX_FIXNUM)&&(intval>FD_MIN_FIXNUM))
    sprintf(buf,"%d",((int)intval));
  else sprintf(buf,"#!%llx",intval);
  return fd_make_string(NULL,-1,buf);
}

static fdtype ptrlock_prim(fdtype x,fdtype mod)
{
  unsigned long long intval = (unsigned long long)x;
  long long int modval = ((FD_VOIDP(mod))?
                        (FD_N_PTRLOCKS):
                        (FD_FIX2INT(mod)));
  if (modval==0)
    return (fdtype)fd_ulong_long_to_bigint(intval);
  else {
    unsigned long long hashval = hashptrval((void *)x,modval);
    return FD_INT(hashval);}
}

/* Integer hashing etc. */

static fdtype knuth_hash(fdtype arg)
{
  if ((FD_FIXNUMP(arg))||(FD_BIGINTP(arg))) {
    long long num=
      ((FD_FIXNUMP(arg))?(FD_FIX2INT(arg)):
       (fd_bigint_to_long_long((fd_bigint)arg)));
    if ((num<0)||(num>=0x100000000ll))
      return fd_type_error("uint32","knuth_hash",arg);
    else {
      unsigned long long int hash = (num*2654435761ll)%0x100000000ll;
      return FD_INT(hash);}}
  else return fd_type_error("uint32","knuth_hash",arg);
}

static fdtype wang_hash32(fdtype arg)
{
  /* Adapted from Thomas Wang
     http://www.cris.com/~Ttwang/tech/inthash.htm */
  if (((FD_UINTP(arg))&&((FD_FIX2INT(arg))>=0))||
      ((FD_BIGINTP(arg))&&(!(fd_bigint_negativep((fd_bigint)arg)))&&
       (fd_bigint_fits_in_word_p(((fd_bigint)arg),32,0)))) {
    unsigned long long num=
      ((FD_FIXNUMP(arg))?(FD_FIX2INT(arg)):
       (fd_bigint_to_ulong_long((fd_bigint)arg)));
    unsigned long long constval = 0x27d4eb2dll; // a prime or an odd constant
    num = (num ^ 61) ^ (num >> 16);
    num = num + (num << 3);
    num = num ^ (num >> 4);
    num = num * constval;
    num = num ^ (num >> 15);
    return FD_INT(num);}
  else return fd_type_error("uint32","wang_hash32",arg);
}

static fdtype wang_hash64(fdtype arg)
{
  /* Adapted from Thomas Wang
     http://www.cris.com/~Ttwang/tech/inthash.htm */
  if (((FD_FIXNUMP(arg))&&((FD_FIX2INT(arg))>=0))||
      ((FD_BIGINTP(arg))&&(!(fd_bigint_negativep((fd_bigint)arg)))&&
       (fd_bigint_fits_in_word_p(((fd_bigint)arg),64,0)))) {
    unsigned long long int num=
      ((FD_FIXNUMP(arg))?(FD_FIX2INT(arg)):
       (fd_bigint_to_ulong_long((fd_bigint)arg)));
    num = (~num) + (num << 21); // num = (num << 21) - num - 1;
    num = num ^ (num >> 24);
    num = (num + (num << 3)) + (num << 8); // num * 265
    num = num ^ (num >> 14);
    num = (num + (num << 2)) + (num << 4); // num * 21
    num = num ^ (num >> 28);
    num = num + (num << 31);
    if (num<0x8000000000000000ll)
      return FD_INT(num);
    else return (fdtype) fd_ulong_long_to_bigint(num);}
  else return fd_type_error("uint64","wang_hash64",arg);
}

static fdtype flip32(fdtype arg)
{
  if (((FD_UINTP(arg))&&((FD_FIX2INT(arg))>=0))||
      ((FD_BIGINTP(arg))&&(!(fd_bigint_negativep((fd_bigint)arg)))&&
       (fd_bigint_fits_in_word_p(((fd_bigint)arg),32,0)))) {
    int word = fd_getint(arg);
    int flipped = fd_flip_word(word);
    return FD_INT(flipped);}
  else return fd_type_error("uint32","flip32",arg);
}

fd_bigint fd_ulong_long_to_bigint(unsigned long long);

static fdtype flip64(fdtype arg)
{
  if (((FD_FIXNUMP(arg))&&((FD_FIX2INT(arg))>=0))||
      ((FD_BIGINTP(arg))&&(!(fd_bigint_negativep((fd_bigint)arg)))&&
       (fd_bigint_fits_in_word_p(((fd_bigint)arg),64,0)))) {
    unsigned long long int word=
      ((FD_FIXNUMP(arg))?(FD_FIX2INT(arg)):
       (fd_bigint_to_ulong_long((fd_bigint)arg)));
    unsigned long long int flipped = fd_flip_word8(word);
    if (flipped<FD_MAX_FIXNUM) return FD_INT(flipped);
    else return (fdtype) fd_ulong_long_to_bigint(flipped);}
  else return fd_type_error("uint64","flip64",arg);
}

/* City hashing */

static fdtype cityhash64(fdtype arg,fdtype asint)
{
  u8_int8 hash;
  const u8_byte *data; size_t datalen;
  if (FD_STRINGP(arg)) {
    data = FD_STRDATA(arg); datalen = FD_STRLEN(arg);}
  else if (FD_PACKETP(arg)) {
    data = FD_PACKET_DATA(arg); datalen = FD_PACKET_LENGTH(arg);}
  else return fd_type_error("packet/string","cityhash64",arg);
  hash = u8_cityhash64(data,datalen);
  if (FD_TRUEP(asint))
    return FD_INT(hash);
  else {
    unsigned char bytes[8];
    bytes[0]=((hash>>56)&0xFF);
    bytes[1]=((hash>>48)&0xFF);
    bytes[2]=((hash>>40)&0xFF);
    bytes[3]=((hash>>32)&0xFF);
    bytes[4]=((hash>>24)&0xFF);
    bytes[5]=((hash>>16)&0xFF);
    bytes[6]=((hash>>8)&0xFF);
    bytes[7]=((hash)&0xFF);
    return fd_make_packet(NULL,8,bytes);}
}

static fdtype cityhash128(fdtype arg)
{
  unsigned char bytes[16];
  u8_int16 hash; u8_int8 hi; u8_int8 lo;
  const u8_byte *data; size_t datalen;
  if (FD_STRINGP(arg)) {
    data = FD_STRDATA(arg); datalen = FD_STRLEN(arg);}
  else if (FD_PACKETP(arg)) {
    data = FD_PACKET_DATA(arg); datalen = FD_PACKET_LENGTH(arg);}
  else return fd_type_error("packet/string","cityhash64",arg);
  hash = u8_cityhash128(data,datalen);
  hi = hash.first; lo = hash.second;
  bytes[0]=((hi>>56)&0xFF);
  bytes[1]=((hi>>48)&0xFF);
  bytes[2]=((hi>>40)&0xFF);
  bytes[3]=((hi>>32)&0xFF);
  bytes[4]=((hi>>24)&0xFF);
  bytes[5]=((hi>>16)&0xFF);
  bytes[6]=((hi>>8)&0xFF);
  bytes[7]=((hi)&0xFF);
  bytes[8]=((lo>>56)&0xFF);
  bytes[9]=((lo>>48)&0xFF);
  bytes[10]=((lo>>40)&0xFF);
  bytes[11]=((lo>>32)&0xFF);
  bytes[12]=((lo>>24)&0xFF);
  bytes[13]=((lo>>16)&0xFF);
  bytes[14]=((lo>>8)&0xFF);
  bytes[15]=((lo)&0xFF);
  return fd_make_packet(NULL,16,bytes);
}

/* ITOA */

static fdtype itoa_prim(fdtype arg,fdtype base_arg)
{
  long long base = FD_FIX2INT(base_arg); char buf[32];
  if (FD_FIXNUMP(arg)) {
    if (base==10) u8_itoa10(FD_FIX2INT(arg),buf);
    else if (FD_FIX2INT(arg)<0)
      return fd_err(_("negative numbers can't be rendered as non-decimal"),
                    "itoa_prim",NULL,fd_incref(arg));
    else if (base==8) u8_uitoa8(FD_FIX2INT(arg),buf);
    else if (base==16) u8_uitoa16(FD_FIX2INT(arg),buf);
    else return fd_type_error("16,10,or 8","itoa_prim",base_arg);}
  else if (!(FD_BIGINTP(arg))) 
    return fd_type_error("number","itoa_prim",arg);
  else {
    fd_bigint bi = (fd_bigint)arg;
    if ((base==10)&&(fd_bigint_negativep(bi))&&
        (fd_bigint_fits_in_word_p(bi,63,0))) {
      long long int n = fd_bigint_to_long_long(bi);
      u8_itoa10(n,buf);}
    else if ((base==10)&&(fd_bigint_fits_in_word_p(bi,64,0))) {
      unsigned long long int n = fd_bigint_to_ulong_long(bi);
      u8_uitoa10(n,buf);}
    else if (base==10)
      return fd_type_error("smallish bigint","itoa_prim",arg);
    else if (fd_bigint_negativep(bi)) {
      return fd_err(_("negative numbers can't be rendered as non-decimal"),
                    "itoa_prim",NULL,fd_incref(arg));}
    else if (fd_bigint_fits_in_word_p(bi,64,0)) {
      unsigned long long int n = fd_bigint_to_ulong_long(bi);
      if (base==8) u8_uitoa8(n,buf);
      else if (base==16) u8_uitoa16(n,buf);
      else return fd_type_error("16,10,or 8","itoa_prim",base_arg);}
    else return fd_type_error("smallish bigint","itoa_prim",arg);}
  return fdtype_string(buf);
}

/* Initialization */

#define arithdef(sname,lname,cname) \
  fd_idefn(fd_scheme_module,fd_make_cprim1(sname,lname,1))
#define arithdef2(sname,lname,cname) \
  fd_idefn(fd_scheme_module,fd_make_cprim2(sname,lname,2))

FD_EXPORT void fd_init_arith_c()
{
  u8_register_source_file(_FILEINFO);

  arithdef("SQRT",lsqrt,sqrt);
  arithdef("COS",lcos,cos);
  arithdef("ACOS",lacos,acos);
  arithdef("SIN",lsin,sin);
  arithdef("ASIN",lasin,asin);
  arithdef("ATAN",latan,atan);
  arithdef("TAN",ltan,tan);
  arithdef("LOG",llog,log);
  arithdef("EXP",lexp,exp);

  arithdef2("ATAN2",latan2,atan2);
  arithdef2("POW~",lpow,lpow);

  fd_idefn(fd_scheme_module,fd_make_cprimn("+",plus_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("-",minus_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("*",times_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("/",div_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("/~",idiv_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("REMAINDER",remainder_prim,2));

  fd_idefn(fd_scheme_module,fd_make_cprim1("RANDOM",random_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("1+",plus1,1));
  {
    fdtype minusone = fd_make_cprim1("-1+",minus1,1);
    fd_idefn(fd_scheme_module,minusone);
    fd_store(fd_scheme_module,fd_intern("1-"),minusone);
  }

  fd_idefn(fd_scheme_module,fd_make_cprim2("QUOTIENT",quotient_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SQRT",sqrt_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ROUND",round_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("POW",pow_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("NTHROOT",nthroot_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("NTHROOT~",inexact_nthroot_prim,2));

  fd_idefn(fd_scheme_module,fd_make_cprimn("MIN",min_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("MAX",max_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("MAKE-RATIONAL",make_rational,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MAKE-COMPLEX",make_complex,2));

  fd_idefn(fd_scheme_module,fd_make_cprim1("INTEGER?",integerp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("RATIONAL?",rationalp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ODD?",oddp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("EVEN?",evenp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("POSITIVE?",positivep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("NEGATIVE?",negativep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("EXACT?",exactp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("INEXACT?",inexactp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FIXNUM?",fixnump,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("BIGNUM?",bignump,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("COMPLEX?",complexp,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("REAL?",realp,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("EXACT->INEXACT",exact2inexact,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("INEXACT->EXACT",inexact2exact,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("->EXACT",toexact,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("REAL-PART",real_part_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("IMAG-PART",imag_part_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("NUMERATOR",numerator_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("DENOMINATOR",denominator_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("ABS",abs_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MODULO",modulo_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("GCD",gcd_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LCM",lcm_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FLOOR",floor_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CEILING",ceiling_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TRUNCATE",truncate_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ROUND",round_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("SCALEREP",scalerep_prim,2));

  fd_idefn(fd_scheme_module,fd_make_cprim2x("ILOG",ilog_prim,1,
                                            fd_fixnum_type,FD_VOID,
                                            fd_fixnum_type,FD_INT(2)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("KNUTH-HASH",knuth_hash,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("WANG-HASH32",wang_hash32,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("WANG-HASH64",wang_hash64,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FLIP32",flip32,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FLIP64",flip64,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("CITYHASH64",cityhash64,1,
                                            -1,FD_VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CITYHASH128",cityhash128,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("HASHPTR",hashptr_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("HASHREF",hashref_prim,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("PTRLOCK",ptrlock_prim,1,
                           -1,FD_VOID,fd_fixnum_type,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("U8ITOA",itoa_prim,1,-1,FD_VOID,fd_fixnum_type,FD_INT(10)));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
