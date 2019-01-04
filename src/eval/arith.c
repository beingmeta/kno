/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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

#define REALP(x) \
  ((FIXNUMP(x)) || (FD_FLONUMP(x)) || (FD_BIGINTP(x)) || (FD_RATIONALP(x)))
#define INTEGERP(x) ((FIXNUMP(x)) || (FD_BIGINTP(x)))

#include <libu8/libu8.h>
#include <libu8/u8printf.h>

#include <stdio.h> /* For sprintf */
#include <errno.h>
#include <math.h>

static lispval complexp(lispval x)
{
  if (NUMBERP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval fixnump(lispval x)
{
  if (FIXNUMP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval bignump(lispval x)
{
  if (FD_BIGINTP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval integerp(lispval x)
{
  if (INTEGERP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval rationalp(lispval x)
{
  if ((FIXNUMP(x)) || (FD_BIGINTP(x)) || (FD_RATIONALP(x)))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval exactp(lispval x)
{
  if (FD_COMPLEXP(x)) {
    lispval real = FD_REALPART(x), imag = FD_IMAGPART(x);
    if ((FD_FLONUMP(real)) || (FD_FLONUMP(imag)))
      return FD_FALSE;
    else return FD_TRUE;}
  else if (FD_FLONUMP(x))
    return FD_FALSE;
  else if ((FIXNUMP(x)) || (FD_BIGINTP(x)) || (FD_RATIONALP(x)))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval inexactp(lispval x)
{
  if (FD_COMPLEXP(x)) {
    lispval real = FD_REALPART(x), imag = FD_IMAGPART(x);
    if ((FD_FLONUMP(real)) || (FD_FLONUMP(imag)))
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_FLONUMP(x))
    return FD_TRUE;
  else if ((FIXNUMP(x)) || (FD_BIGINTP(x)) || (FD_RATIONALP(x)))
    return FD_FALSE;
  else return FD_FALSE;
}

static lispval oddp(lispval x)
{
  if (FIXNUMP(x)) {
    long long ival = FIX2INT(x);
    if (ival%2)
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_BIGINTP(x)) {
    lispval remainder = fd_remainder(x,FD_INT(2));
    if (FD_ABORTP(remainder)) return remainder;
    else if (FIXNUMP(remainder))
      if (FD_INT(remainder))
        return FD_TRUE;
      else return FD_FALSE;
    else {
      fd_decref(remainder);
      return FD_FALSE;}}
  else return FD_FALSE;
}

static lispval evenp(lispval x)
{
  if (FIXNUMP(x)) {
    long long ival = FIX2INT(x);
    if (ival%2)
      return FD_FALSE;
    else return FD_TRUE;}
  else if (FD_BIGINTP(x)) {
    lispval remainder = fd_remainder(x,FD_INT(2));
    if (FD_ABORTP(remainder))
      return remainder;
    else if (FIXNUMP(remainder))
      if (FD_INT(remainder))
        return FD_FALSE;
      else return FD_TRUE;
    else {
      fd_decref(remainder);
      return FD_FALSE;}}
  else return FD_FALSE;
}

static lispval realp(lispval x)
{
  if (REALP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval positivep(lispval x)
{
  int sgn = fd_numcompare(x,FD_INT(0));
  if (sgn>0)
    return FD_TRUE;
  else return FD_FALSE;
}
static lispval negativep(lispval x)
{
  int sgn = fd_numcompare(x,FD_INT(0));
  if (sgn<0)
    return FD_TRUE;
  else return FD_FALSE;
}

/* Basic ops */

/* Arithmetic */

static lispval plus_lexpr(int n,lispval *args)
{
  if (n==0)
    return FD_FIXNUM_ZERO;
  else if (n==1) {
    lispval x = args[0];
    if (FIXNUMP(x))
      return x;
    else return fd_incref(x);}
  else if (n==2)
    return fd_plus(args[0],args[1]);
  else {
    int i = 0; int floating = 0, generic = 0, vector = 0;
    while (i < n)
      if (FIXNUMP(args[i])) i++;
      else if (FD_FLONUMP(args[i])) {floating = 1; i++;}
      else if ((VECTORP(args[i]))||(FD_NUMVECP(args[i]))) {
        generic = 1; vector = 1; i++;}
      else {generic = 1; i++;}
    if ((floating==0) && (generic==0)) {
      long long fixresult = 0;
      i = 0; while (i < n) {
        long long val = 0;
        if (FIXNUMP(args[i]))
          val = fd_getint(args[i]);
        fixresult = fixresult+val;
        i++;}
      return FD_INT(fixresult);}
    else if (generic == 0) {
      double floresult = 0.0;
      i = 0; while (i < n) {
        double val;
        if (FIXNUMP(args[i])) val = (double)fd_getint(args[i]);
        else if (FD_BIGINTP(args[i]))
          val = (double)fd_bigint_to_double((fd_bigint)args[i]);
        else val = ((struct FD_FLONUM *)args[i])->floval;
        floresult = floresult+val;
        i++;}
      return fd_init_double(NULL,floresult);}
    else if (vector) {
      lispval result = fd_plus(args[0],args[1]);
      if (FD_ABORTP(result)) return result;
      i = 2; while (i < n) {
        lispval newv = fd_plus(result,args[i]);
        if (FD_ABORTP(newv)) {
          fd_decref(result); return newv;}
        fd_decref(result); result = newv; i++;}
      return result;}
    else {
      lispval result = FD_INT(0);
      i = 0; while (i < n) {
        lispval newv = fd_plus(result,args[i]);
        if (FD_ABORTP(newv)) {
          fd_decref(result); return newv;}
        fd_decref(result); result = newv; i++;}
      return result;}
  }
}

static lispval plus1(lispval x)
{
  if (FIXNUMP(x)) {
    long long iv = fd_getint(x); iv++;
    return FD_INT2DTYPE(iv);}
  else if (FD_FLONUMP(x)) {
    fd_double iv = FD_FLONUM(x); iv = iv+1;
    return fd_make_flonum(iv);}
  else {
    lispval args[2]; args[0]=x; args[1]=FD_INT(1);
    return plus_lexpr(2,args);}
}
static lispval minus1(lispval x)
{
  if (FIXNUMP(x)) {
    long long iv = fd_getint(x); iv--;
    return FD_INT2DTYPE(iv);}
  else if (FD_FLONUMP(x)) {
    fd_double iv = FD_FLONUM(x); iv = iv-1;
    return fd_make_flonum(iv);}
  else {
    lispval args[2]; args[0]=x; args[1]=FD_INT(-1);
    return plus_lexpr(2,args);}
}

static lispval times_lexpr(int n,lispval *args)
{
  int i = 0; int floating = 0, generic = 0;
  if (n==1) {
    lispval arg = args[0];
    if (FIXNUMP(arg)) return arg;
    else if (NUMBERP(arg))
      return  fd_incref(arg);
    else if ((FD_NUMVECP(arg))||(VECTORP(arg)))
      return fd_incref(arg);
    return fd_type_error(_("number"),"times_lexpr",fd_incref(arg));}
  else if (n==2)
    return fd_multiply(args[0],args[1]);
  else {
    while (i < n)
      if (FIXNUMP(args[i])) i++;
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
            lispval bigresult = fd_multiply(FD_INT(fixresult),args[i]);
            i++; while (i<n) {
              lispval bigprod = fd_multiply(bigresult,args[i]);
              fd_decref(bigresult); bigresult = bigprod; i++;}
            return bigresult;}
          else fixresult = fixresult*mult;}
        i++;}
      return FD_INT(fixresult);}
    else if (generic == 0) {
      double floresult = 1.0;
      i = 0; while (i < n) {
        double val;
        if (FIXNUMP(args[i])) val = (double)FIX2INT(args[i]);
        else if  (FD_BIGINTP(args[i]))
          val = (double)fd_bigint_to_double((fd_bigint)args[i]);
        else val = ((struct FD_FLONUM *)args[i])->floval;
        floresult = floresult*val;
        i++;}
      return fd_init_double(NULL,floresult);}
    else {
      lispval result = FD_INT(1);
      i = 0; while (i < n) {
        lispval newv = fd_multiply(result,args[i]);
        fd_decref(result); result = newv; i++;}
      return result;}
  }
}

static lispval minus_lexpr(int n,lispval *args)
{
  if (n == 1) {
    lispval arg = args[0];
    if (FIXNUMP(arg))
      return FD_INT(-(FIX2INT(arg)));
    else if (FD_FLONUMP(arg))
      return fd_init_double(NULL,-(FD_FLONUM(arg)));
    else if ((VECTORP(arg))||(FD_NUMVECP(arg)))
      return fd_multiply(arg,FIX2INT(-1));
    else return fd_subtract(FD_INT(0),arg);}
  else if (n == 2)
    return fd_subtract(args[0],args[1]);
  else {
    int i = 0; int floating = 0, generic = 0, vector = 0;
    while (i < n) {
      if (FIXNUMP(args[i])) i++;
      else if (FD_FLONUMP(args[i])) {floating = 1; i++;}
      else if ((VECTORP(args[i]))||(FD_NUMVECP(args[i]))) {
        vector = 1; generic = 1;}
      else {generic = 1; i++;}}
    if ((floating==0) && (generic==0)) {
      long long fixresult = 0;
      i = 0; while (i < n) {
        long long val = 0;
        if (FIXNUMP(args[i])) val = FIX2INT(args[i]);
        else if  (FD_BIGINTP(args[i]))
          val = (double)fd_bigint_to_double((fd_bigint)args[i]);
        if (i==0) fixresult = val; else fixresult = fixresult-val;
        i++;}
      return FD_INT(fixresult);}
    else if (generic == 0) {
      double floresult = 0.0;
      i = 0; while (i < n) {
        double val;
        if (FIXNUMP(args[i])) val = (double)FIX2INT(args[i]);
        else if  (FD_BIGINTP(args[i]))
          val = (double)fd_bigint_to_double((fd_bigint)args[i]);
        else val = ((struct FD_FLONUM *)args[i])->floval;
        if (i==0) floresult = val; else floresult = floresult-val;
        i++;}
      return fd_init_double(NULL,floresult);}
    else if (vector) {
      lispval result = fd_subtract(args[0],args[1]);
      i = 2; while (i < n) {
        lispval newv = fd_subtract(result,args[i]);
        fd_decref(result); result = newv; i++;}
      return result;}
    else {
      lispval result = fd_incref(args[0]);
      i = 1; while (i < n) {
        lispval newv = fd_subtract(result,args[i]);
        fd_decref(result); result = newv; i++;}
      return result;}}
}

static double todouble(lispval x)
{
  if (FIXNUMP(x))
    return (double)(FIX2INT(x));
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

static lispval div_lexpr(int n,lispval *args)
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
    lispval value = fd_incref(args[0]); i = 1;
    while (i<n) {
      lispval newv = fd_divide(value,args[i]);
      fd_decref(value); value = newv; i++;}
    return value;}
}

static lispval idiv_lexpr(int n,lispval *args)
{
  int all_double = 1, i = 0;
  while (i<n)
    if (FD_FLONUMP(args[i])) i++;
    else if (!((FIXNUMP(args[i])) || (FD_BIGINTP(args[i]))))
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

static lispval remainder_prim(lispval x,lispval m)
{
  if ((FIXNUMP(x)) && (FIXNUMP(m))) {
    long long ix = FIX2INT(x), im = FIX2INT(m);
    long long r = ix%im;
    return FD_INT(r);}
  else return fd_remainder(x,m);
}

static lispval random_prim(lispval maxarg)
{
  if (FD_INTEGERP(maxarg)) {
    long long max = fd_getint(maxarg);
    if (max<=0)
      return fd_type_error("Small positive integer","random_prim",maxarg);
    else if (max > UINT_MAX) {
      long long short_max = max%USHRT_MAX;
      unsigned long long base_rand = max-short_max;
      unsigned long long big_rand =  u8_random(short_max);
      while (base_rand>0) {
        unsigned int short_rand = u8_random(USHRT_MAX);
        big_rand = (big_rand << 16) + short_rand;
        base_rand = base_rand >> 16;}
      big_rand = big_rand % max;
      if (big_rand<FD_MAX_FIXNUM)
        return FD_INT2FIX(big_rand);
      else {
        fd_bigint bi = fd_ulong_long_to_bigint(big_rand);
        return (lispval) bi;}}
    else {
      unsigned int n = u8_random(max);
      return FD_INT(n);}}
  else if (FD_FLONUMP(maxarg)) {
    double flomax = FD_FLONUM(maxarg);
    long long  intmax = (long long)flomax;
    long long n = u8_random(intmax);
    double rval = (double) n;
    return fd_make_flonum(rval);}
  else return fd_type_error("integer or flonum","random_prim",maxarg);
}

/* Making some numbers */

static lispval make_rational(lispval n,lispval d)
{
  return fd_make_rational(n,d);
}

static lispval numerator_prim(lispval x)
{
  if (FD_RATIONALP(x)) {
    lispval num = FD_NUMERATOR(x);
    return fd_incref(num);}
  else if (NUMBERP(x)) return fd_incref(x);
  else return fd_type_error("number","numerator_prim",x);
}

static lispval denominator_prim(lispval x)
{
  if (FD_RATIONALP(x)) {
    lispval den = FD_DENOMINATOR(x);
    return fd_incref(den);}
  else if (NUMBERP(x)) return FD_INT(1);
  else return fd_type_error("number","denominator_prim",x);
}

static lispval make_complex(lispval r,lispval i)
{
  return fd_make_complex(r,i);
}

static lispval real_part_prim(lispval x)
{
  if (REALP(x)) return fd_incref(x);
  else if (FD_COMPLEXP(x)) {
    lispval r = FD_REALPART(x);
    return fd_incref(r);}
  else return fd_type_error("number","real_part_prim",x);
}

static lispval imag_part_prim(lispval x)
{
  if (REALP(x)) return FD_INT(0);
  else if (FD_COMPLEXP(x)) {
    lispval r = FD_IMAGPART(x);
    return fd_incref(r);}
  else return fd_type_error("number","imag_part_prim",x);
}

static double doublearg(lispval x,lispval *whoops)
{
  if (FIXNUMP(x))
    return (double)(FIX2INT(x));
  else if (FD_BIGINTP(x))
    return (double)fd_bigint_to_double((fd_bigint)x);
  else if (FD_FLONUMP(x))
    return (((struct FD_FLONUM *)x)->floval);
  else {
    *whoops = fd_type_error("number","doublearg",x);
    return 0;}
}

static lispval exact2inexact(lispval x)
{
  return fd_make_inexact(x);
}

static lispval inexact2exact(lispval x)
{
  return fd_make_exact(x);
}

static lispval toexact(lispval x,lispval direction)
{
  long long dir = -1;
  if ((VOIDP(direction))||(FALSEP(direction)))
    dir = -1;
  else if (FIXNUMP(direction))
    dir = FIX2INT(direction);
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
      return fd_make_exact((lispval)(&tmp));}}
  else if (FD_COMPLEXP(x)) {
    lispval real = FD_REALPART(x), imag = FD_IMAGPART(x);
    if ((FD_FLONUMP(real))||(FD_FLONUMP(imag))) {
      struct FD_COMPLEX *num = u8_alloc(struct FD_COMPLEX);
      lispval xreal = toexact(real,direction);
      lispval ximag = toexact(imag,direction);
      FD_INIT_CONS(num,fd_complex_type);
      num->realpart = xreal; num->imagpart = ximag;
      return (lispval) num;}
    else {
      fd_incref(x);
      return x;}}
  else if (NUMBERP(x)) {
    fd_incref(x);
    return x;}
  else return fd_type_error("number","toexact",x);
}

#define arithdef(sname,lname,cname) \
  static lispval lname(lispval x) { \
    lispval err = VOID; double val = doublearg(x,&err); \
    errno = 0; \
    if (VOIDP(err)) { \
      double result = cname(val); \
      if (errno==0) return fd_init_double(NULL,result); \
      else {u8_graberrno(sname,u8_mkstring("%f",val)); \
        return FD_ERROR;}}                              \
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
  static lispval lname(lispval x,lispval y) { \
    lispval err = VOID; double xval, yval; \
    xval = doublearg(x,&err);                  \
    if (!(VOIDP(err))) return err;       \
    yval = doublearg(y,&err);                  \
    errno = 0;                                \
    if (VOIDP(err)) {                    \
      double result = cname(xval,yval);       \
      if (errno==0) return fd_init_double(NULL,result); \
      else {u8_graberrno(sname,u8_mkstring("%f %f",xval,yval)); \
            return FD_ERROR;}}          \
    else return err;}

arithdef2("ATAN2",latan2,atan2);
arithdef2("POW",lpow,pow);

#undef arithdef
#undef arithdef2

static lispval pow_prim(lispval v,lispval n)
{
  if ((FD_EXACTP(v))&&
      (FIXNUMP(n))&&(FIX2INT(n)>=0)&&
      (FIX2INT(n)<1000000)) {
    if (FD_ZEROP(v)) return FD_FIXNUM_ONE;
    else {
      long long i = 0, how_many = FIX2INT(n);
      lispval prod = FD_INT(1); while (i<how_many) {
        lispval tmp = fd_multiply(prod,v);
        fd_decref(prod); prod = tmp;
        i++;}
      return prod;}}
  else {
    lispval err = VOID;
    lispval dv = doublearg(v,&err);
    lispval dn = (VOIDP(err)) ? doublearg(n,&err) : (0);
    if (FD_ABORTP(err)) return err;
    else {
      double result = pow(dv,dn);
      return fd_make_flonum(result);}}
}

static lispval nthroot_prim(lispval v,lispval n)
{
  lispval err = VOID;
  double dv = doublearg(v,&err);
  double dn = (VOIDP(err)) ? (doublearg(n,&err)) :(0);
  double dexp = (VOIDP(err)) ? (1/dn) : (0);
  if (FD_ABORTP(err))
    return err;
  else {
    double result = pow(dv,dexp);
    if ((remainder(result,1.0)==0)&&
        (FD_UINTP(n))&&(FIX2INT(n)<1024)&&
        ((FIXNUMP(v))||(FD_BIGINTP(v)))) {
      long long introot = (long long) floor(result);
      lispval root = FD_INT(introot), prod = FD_INT(1);
      int i = 0, lim = FIX2INT(n); while (i<lim) {
        lispval tmp = fd_multiply(prod,root);
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

static lispval inexact_nthroot_prim(lispval v,lispval n)
{
  lispval err = VOID;
  double dv = doublearg(v,&err);
  double dn = (VOIDP(err)) ? (doublearg(n,&err)) :(0);
  double dexp = (VOIDP(err)) ? (1/dn) : (0);
  if (FD_ABORTP(err))
    return err;
  else {
    double result = pow(dv,dexp);
    return fd_make_flonum(result);}
}

/* Min/Max operators, etc */


static lispval min_prim(int n,lispval *args)
{
  if (n==0)
    return fd_err(fd_TooFewArgs,"max_prim",NULL,VOID);
  else {
    lispval result = args[0]; int i = 1, inexact = FD_FLONUMP(args[0]);
    while (i<n) {
      int cmp = fd_numcompare(args[i],result);
      if (cmp>1) return FD_ERROR;
      if (FD_FLONUMP(args[i])) inexact = 1;
      if (cmp<0) result = args[i];
      i++;}
    if (inexact)
      if (FD_FLONUMP(result))
        return fd_incref(result);
      else {
        lispval err = VOID;
        double asdouble = doublearg(result,&err);
        if (VOIDP(err))
          return fd_init_double(NULL,asdouble);
        else return err;}
    else return fd_incref(result);}
}

static lispval max_prim(int n,lispval *args)
{
  if (n==0) return fd_err(fd_TooFewArgs,"max_prim",NULL,VOID);
  else {
    lispval result = args[0];
    int i = 1, inexact = FD_FLONUMP(args[0]);
    while (i<n) {
      int cmp = fd_numcompare(args[i],result);
      if (cmp>1) return FD_ERROR;
      if (FD_FLONUMP(args[i])) inexact = 1;
      if (cmp>0) result = args[i];
      i++;}
    if (inexact)
      if (FD_FLONUMP(result))
        return fd_incref(result);
      else {
        lispval err = VOID;
        double asdouble = doublearg(result,&err);
        if (VOIDP(err))
          return fd_init_double(NULL,asdouble);
        else return err;}
    else return fd_incref(result);}
}

static lispval abs_prim(lispval x)
{
  if (FIXNUMP(x)) {
    long long ival = FIX2INT(x);
    assert((ival<0) == ((FIX2INT(x))<0));
    if (ival<0) {
      long long aval = -ival;
      return FD_INT(aval);}
    else return x;}
  else if (fd_numcompare(x,FD_INT(0))<0)
    return fd_subtract(FD_INT(0),x);
  else return fd_incref(x);
}

static lispval modulo_prim(lispval x,lispval b)
{
  if ((FIXNUMP(x)) && (FIXNUMP(b))) {
    long long ix = FIX2INT(x), ib = FIX2INT(b);
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
      lispval rem = fd_remainder(x,b);
      if ((FIXNUMP(rem)) && (FIX2INT(rem)==0))
        return rem;
      else {
        lispval result = fd_plus(b,rem);
        fd_decref(rem);
        return result;}}}
}

static lispval gcd_prim(lispval x,lispval y)
{
  if ((INTEGERP(x)) && (INTEGERP(y)))
    return fd_gcd(x,y);
  else if (INTEGERP(y))
    return fd_type_error("integer","gcd_prim",x);
  else return fd_type_error("integer","gcd_prim",y);
}

static lispval lcm_prim(lispval x,lispval y)
{
  if ((INTEGERP(x)) && (INTEGERP(y)))
    return fd_lcm(x,y);
  else if (INTEGERP(y))
    return fd_type_error("integer","lcm_prim",x);
  else return fd_type_error("integer","lcm_prim",y);
}

static lispval truncate_prim(lispval x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double d = trunc(FD_FLONUM(x));
    return fd_init_double(NULL,d);}
  else if (FD_RATIONALP(x)) {
    lispval n = FD_NUMERATOR(x), d = FD_DENOMINATOR(x);
      lispval q = fd_quotient(n,d);
      return q;}
  else return fd_type_error(_("scalar"),"floor_prim",x);
}

static lispval floor_prim(lispval x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double d = floor(FD_FLONUM(x));
    return fd_init_double(NULL,d);}
  else if (FD_RATIONALP(x)) {
    lispval n = FD_NUMERATOR(x), d = FD_DENOMINATOR(x);
    lispval q = fd_quotient(n,d);
    if (fd_numcompare(x,FD_INT(0))<0) {
      lispval qminus = fd_subtract(q,FD_INT(1));
      fd_decref(q);
      return qminus;}
    else return q;}
  else return fd_type_error(_("scalar"),"floor_prim",x);
}

static lispval ceiling_prim(lispval x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double d = ceil(FD_FLONUM(x));
    return fd_init_double(NULL,d);}
  else if (FD_RATIONALP(x)) {
    lispval n = FD_NUMERATOR(x), d = FD_DENOMINATOR(x);
    lispval q = fd_quotient(n,d);
    if (fd_numcompare(x,FD_INT(0))>0) {
      lispval qminus = fd_plus(q,FD_INT(1));
      fd_decref(q);
      return qminus;}
    else return q;}
  else return fd_type_error(_("scalar"),"ceiling_prim",x);
}

static lispval round_prim(lispval x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double v = FD_FLONUM(x);
    double f = floor(v), c = ceil(v);
    if ((v<0) ? ((c-v)>=(v-f)) : ((c-v)>(v-f)))
      return fd_init_double(NULL,f);
    else return fd_init_double(NULL,c);}
  else if (FD_RATIONALP(x)) {
    lispval n = FD_NUMERATOR(x), d = FD_DENOMINATOR(x);
    lispval q = fd_quotient(n,d);
    lispval r = fd_remainder(n,d);
    lispval halfd = fd_quotient(d,FD_INT(2));
    if (fd_numcompare(r,halfd)<0) {
      fd_decref(halfd); fd_decref(r);
      return q;}
    else if (fd_numcompare(x,FD_INT(0))<0) {
      lispval qplus = fd_subtract(q,FD_INT(1));
      fd_decref(q);
      return qplus;}
    else {
      lispval qminus = fd_plus(q,FD_INT(1));
      fd_decref(q);
      return qminus;}}
  else return fd_type_error(_("scalar"),"floor_prim",x);
}

static double doround(double x)
{
  double c = ceil(x), f = floor(x);
  if ((x-f)<0.5) return f; else return c;
}

static lispval scalerep_prim(lispval x,lispval scalearg)
{
  long long scale = fd_getint(scalearg);
  if (scale<0)
    if (FIXNUMP(x)) return x;
    else if (FD_FLONUMP(x)) {
      long long factor = -scale;
      double val = FD_FLONUM(x), factor_up = doround(val*factor);
      long long ival = factor_up;
      return fd_conspair(FD_INT(ival),scalearg);}
    else return EMPTY;
  else if (FIXNUMP(x)) {
    long long ival = FIX2INT(x), rem = ival%scale, base = (ival/scale);
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
  else return EMPTY;
}

/* More simple arithmetic functions */

static lispval quotient_prim(lispval x,lispval y)
{
  if ((FIXNUMP(x)) && (FIXNUMP(y))) {
    long long ix = FIX2INT(x), iy = FIX2INT(y);
    long long q = ix/iy;
    return FD_INT(q);}
  else return fd_quotient(x,y);
}

static lispval sqrt_prim(lispval x)
{
  double v = todouble(x);
  double sqr = sqrt(v);
  return fd_init_double(NULL,sqr);
}

/* Log and exponent functions */

static lispval ilog_prim(lispval n,lispval base_arg)
{
  long long base = fd_getint(base_arg), limit = fd_getint(n);
  long long count = 0, exp = 1;
  while (exp<limit) {
    count++; exp = exp*base;}
  return FD_INT(count);
}

/* Integer hashing etc. */

static lispval knuth_hash(lispval arg)
{
  if ((FIXNUMP(arg))||(FD_BIGINTP(arg))) {
    long long num=
      ((FIXNUMP(arg))?(FIX2INT(arg)):
       (fd_bigint_to_long_long((fd_bigint)arg)));
    if ((num<0)||(num>=0x100000000ll))
      return fd_type_error("uint32","knuth_hash",arg);
    else {
      unsigned long long int hash = (num*2654435761ll)%0x100000000ll;
      return FD_INT(hash);}}
  else return fd_type_error("uint32","knuth_hash",arg);
}

static lispval wang_hash32(lispval arg)
{
  /* Adapted from Thomas Wang
     http://www.cris.com/~Ttwang/tech/inthash.htm */
  if (((FD_UINTP(arg))&&((FIX2INT(arg))>=0))||
      ((FD_BIGINTP(arg))&&(!(fd_bigint_negativep((fd_bigint)arg)))&&
       (fd_bigint_fits_in_word_p(((fd_bigint)arg),32,0)))) {
    unsigned long long num=
      ((FIXNUMP(arg))?(FIX2INT(arg)):
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

static lispval wang_hash64(lispval arg)
{
  /* Adapted from Thomas Wang
     http://www.cris.com/~Ttwang/tech/inthash.htm */
  if (((FIXNUMP(arg))&&((FIX2INT(arg))>=0))||
      ((FD_BIGINTP(arg))&&(!(fd_bigint_negativep((fd_bigint)arg)))&&
       (fd_bigint_fits_in_word_p(((fd_bigint)arg),64,0)))) {
    unsigned long long int num=
      ((FIXNUMP(arg))?(FIX2INT(arg)):
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
    else return (lispval) fd_ulong_long_to_bigint(num);}
  else return fd_type_error("uint64","wang_hash64",arg);
}

static lispval flip32(lispval arg)
{
  if (((FD_UINTP(arg))&&((FIX2INT(arg))>=0))||
      ((FD_BIGINTP(arg))&&(!(fd_bigint_negativep((fd_bigint)arg)))&&
       (fd_bigint_fits_in_word_p(((fd_bigint)arg),32,0)))) {
    int word = fd_getint(arg);
    int flipped = fd_flip_word(word);
    return FD_INT(flipped);}
  else return fd_type_error("uint32","flip32",arg);
}

fd_bigint fd_ulong_long_to_bigint(unsigned long long);

static lispval flip64(lispval arg)
{
  if (((FIXNUMP(arg))&&((FIX2INT(arg))>=0))||
      ((FD_BIGINTP(arg))&&(!(fd_bigint_negativep((fd_bigint)arg)))&&
       (fd_bigint_fits_in_word_p(((fd_bigint)arg),64,0)))) {
    unsigned long long int word=
      ((FIXNUMP(arg))?(FIX2INT(arg)):
       (fd_bigint_to_ulong_long((fd_bigint)arg)));
    unsigned long long int flipped = fd_flip_word8(word);
    if (flipped<FD_MAX_FIXNUM) return FD_INT(flipped);
    else return (lispval) fd_ulong_long_to_bigint(flipped);}
  else return fd_type_error("uint64","flip64",arg);
}

/* City hashing */

static lispval cityhash64(lispval arg,lispval asint)
{
  u8_int8 hash;
  const u8_byte *data; size_t datalen;
  if (STRINGP(arg)) {
    data = CSTRING(arg); datalen = STRLEN(arg);}
  else if (PACKETP(arg)) {
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

static lispval cityhash128(lispval arg)
{
  unsigned char bytes[16];
  u8_int16 hash; u8_int8 hi; u8_int8 lo;
  const u8_byte *data; size_t datalen;
  if (STRINGP(arg)) {
    data = CSTRING(arg); datalen = STRLEN(arg);}
  else if (PACKETP(arg)) {
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

static lispval itoa_prim(lispval arg,lispval base_arg)
{
  long long base = FIX2INT(base_arg); char buf[32];
  if (FIXNUMP(arg)) {
    if (base==10) u8_itoa10(FIX2INT(arg),buf);
    else if (FIX2INT(arg)<0)
      return fd_err(_("negative numbers can't be rendered as non-decimal"),
                    "itoa_prim",NULL,fd_incref(arg));
    else if (base==8) u8_uitoa8(FIX2INT(arg),buf);
    else if (base==16) u8_uitoa16(FIX2INT(arg),buf);
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
  return lispval_string(buf);
}

/* string/int conversions */


static lispval inexact2string(lispval x,lispval precision)
{
  if (FD_FLONUMP(x))
    if ((FD_UINTP(precision)) || (VOIDP(precision))) {
      int prec = ((VOIDP(precision)) ? (2) : (FIX2INT(precision)));
      char buf[128], cmd[16];
      if (sprintf(cmd,"%%.%df",prec)<1) {
        u8_seterr("Bad precision","inexact2string",NULL);
        return FD_ERROR_VALUE;}
      else if (sprintf(buf,cmd,FD_FLONUM(x)) < 1) {
        u8_seterr("Bad precision","inexact2string",NULL);
        return FD_ERROR_VALUE;}
      return lispval_string(buf);}
    else return fd_type_error("fixnum","inexact2string",precision);
  else {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    fd_unparse(&out,x);
    return fd_stream2string(&out);}
}

static lispval number2string(lispval x,lispval base)
{
  if (NUMBERP(x)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    fd_output_number(&out,x,fd_getint(base));
    return fd_stream2string(&out);}
  else return fd_err(fd_TypeError,"number2string",NULL,x);
}

static lispval number2locale(lispval x,lispval precision)
{
  if (FD_FLONUMP(x))
    if ((FD_UINTP(precision)) || (VOIDP(precision))) {
      int prec = ((VOIDP(precision)) ? (2) : (FIX2INT(precision)));
      char buf[128]; char cmd[16];
      sprintf(cmd,"%%'.%df",prec);
      sprintf(buf,cmd,FD_FLONUM(x));
      return lispval_string(buf);}
    else return fd_type_error("fixnum","inexact2string",precision);
  else if (FIXNUMP(x)) {
    long long i_val = FIX2INT(x);
    char tmpbuf[32];
    return lispval_string(u8_itoa10(i_val,tmpbuf));}
  else {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    fd_unparse(&out,x);
    return fd_stream2string(&out);}
}

static lispval string2number(lispval x,lispval base)
{
  return fd_string2number(CSTRING(x),fd_getint(base));
}

static lispval tohex(lispval x)
{
  if (FD_STRINGP(x))
    return fd_string2number(CSTRING(x),16);
  else if (FD_NUMBERP(x)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    fd_output_number(&out,x,16);
    return fd_stream2string(&out);}
  else return fd_err(fd_NotANumber,"argtohex",NULL,x);
}

static lispval just2number(lispval x,lispval base)
{
  if (NUMBERP(x)) return fd_incref(x);
  else if (STRINGP(x)) {
    lispval num = fd_string2number(CSTRING(x),fd_getint(base));
    if (FALSEP(num)) return FD_FALSE;
    else return num;}
  else return fd_type_error(_("string or number"),"->NUMBER",x);
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

  fd_store(fd_scheme_module,fd_intern("MAX-FIXNUM"),fd_max_fixnum);
  fd_store(fd_scheme_module,fd_intern("MIN-FIXNUM"),fd_min_fixnum);

  fd_idefn(fd_scheme_module,fd_make_cprimn("+",plus_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("-",minus_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("*",times_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("/",div_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("/~",idiv_lexpr,-1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("REMAINDER",remainder_prim,2));

  fd_idefn(fd_scheme_module,fd_make_cprim1("RANDOM",random_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("1+",plus1,1));
  {
    lispval minusone = fd_make_cprim1("-1+",minus1,1);
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

  fd_idefn2(fd_scheme_module,"INEXACT->STRING",inexact2string,1,
            "(inexact->string *num* [*precision*])\n"
            "Generates a string of precision *precision* for *num*.",
            -1,FD_VOID,fd_fixnum_type,FD_VOID);
  fd_idefn2(fd_scheme_module,"NUMBER->STRING",number2string,1,
            "(number->string *obj* [*base*=10])\n"
            "Converts *obj* (a number) into a string in the given *base*",
            -1,VOID,fd_fixnum_type,FD_INT(10));
  fd_idefn2(fd_scheme_module,"NUMBER->LOCALE",number2locale,1,
            "(number->locale *obj* [*precision*])\n"
            "Converts *obj* into a locale-specific string",
            -1,FD_VOID,-1,FD_VOID);
  fd_idefn2(fd_scheme_module,"STRING->NUMBER",string2number,1,
            "(string->number *string* [*base*])\n"
            "Converts *string* into a number, returning #f on failure",
            fd_string_type,VOID,fd_fixnum_type,FD_INT(-1));
  fd_idefn2(fd_scheme_module,"->NUMBER",just2number,1,
            "(->number *obj* [*base*])\nConverts *obj* into a number",
            -1,VOID,fd_fixnum_type,FD_INT(-1));
  fd_idefn1(fd_scheme_module,"->HEX",tohex,1,
            "Converts a number to a hex string or a hex string to a number",
            -1,VOID);
  fd_defalias(fd_scheme_module,"->0X","->HEX");

  fd_idefn(fd_scheme_module,fd_make_cprim2x("ILOG",ilog_prim,1,
                                            fd_fixnum_type,VOID,
                                            fd_fixnum_type,FD_INT(2)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("KNUTH-HASH",knuth_hash,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("WANG-HASH32",wang_hash32,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("WANG-HASH64",wang_hash64,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FLIP32",flip32,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FLIP64",flip64,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("CITYHASH64",cityhash64,1,
                                            -1,VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CITYHASH128",cityhash128,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("U8ITOA",itoa_prim,1,-1,VOID,fd_fixnum_type,FD_INT(10)));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
