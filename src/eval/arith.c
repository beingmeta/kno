/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
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
#include "kno/numbers.h"
#include "kno/cprims.h"

#define REALP(x)							\
  ((FIXNUMP(x)) || (KNO_FLONUMP(x)) || (KNO_BIGINTP(x)) || (KNO_RATIONALP(x)))
#define INTEGERP(x) ((FIXNUMP(x)) || (KNO_BIGINTP(x)))

#include <libu8/libu8.h>
#include <libu8/u8printf.h>

#include <stdio.h> /* For sprintf */
#include <errno.h>
#include <math.h>


DEFPRIM1("complex?",complexp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(COMPLEX? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval complexp(lispval x)
{
  if (NUMBERP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("fixnum?",fixnump,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FIXNUM? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval fixnump(lispval x)
{
  if (FIXNUMP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("bignum?",bignump,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(BIGNUM? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval bignump(lispval x)
{
  if (KNO_BIGINTP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("integer?",integerp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INTEGER? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval integerp(lispval x)
{
  if (INTEGERP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("rational?",rationalp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(RATIONAL? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval rationalp(lispval x)
{
  if ((FIXNUMP(x)) || (KNO_BIGINTP(x)) || (KNO_RATIONALP(x)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("exact?",exactp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXACT? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval exactp(lispval x)
{
  if (KNO_COMPLEXP(x)) {
    lispval real = KNO_REALPART(x), imag = KNO_IMAGPART(x);
    if ((KNO_FLONUMP(real)) || (KNO_FLONUMP(imag)))
      return KNO_FALSE;
    else return KNO_TRUE;}
  else if (KNO_FLONUMP(x))
    return KNO_FALSE;
  else if ((FIXNUMP(x)) || (KNO_BIGINTP(x)) || (KNO_RATIONALP(x)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("inexact?",inexactp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INEXACT? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval inexactp(lispval x)
{
  if (KNO_COMPLEXP(x)) {
    lispval real = KNO_REALPART(x), imag = KNO_IMAGPART(x);
    if ((KNO_FLONUMP(real)) || (KNO_FLONUMP(imag)))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_FLONUMP(x))
    return KNO_TRUE;
  else if ((FIXNUMP(x)) || (KNO_BIGINTP(x)) || (KNO_RATIONALP(x)))
    return KNO_FALSE;
  else return KNO_FALSE;
}

DEFPRIM1("odd?",oddp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ODD? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval oddp(lispval x)
{
  if (FIXNUMP(x)) {
    long long ival = FIX2INT(x);
    if (ival%2)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_BIGINTP(x)) {
    lispval remainder = kno_remainder(x,KNO_INT(2));
    if (KNO_ABORTP(remainder)) return remainder;
    else if (KNO_FIX2INT(remainder))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFPRIM1("even?",evenp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EVEN? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval evenp(lispval x)
{
  if (FIXNUMP(x)) {
    long long ival = FIX2INT(x);
    if (ival%2)
      return KNO_FALSE;
    else return KNO_TRUE;}
  else if (KNO_BIGINTP(x)) {
    lispval remainder = kno_remainder(x,KNO_INT(2));
    if (KNO_ABORTP(remainder)) return remainder;
    else if (KNO_FIX2INT(remainder))
      return KNO_FALSE;
    else return KNO_TRUE;}
  else return KNO_FALSE;
}

DEFPRIM1("real?",realp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(REAL? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval realp(lispval x)
{
  if (REALP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("positive?",positivep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(POSITIVE? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval positivep(lispval x)
{
  int sgn = kno_numcompare(x,KNO_INT(0));
  if (sgn>0)
    return KNO_TRUE;
  else return KNO_FALSE;
}
DEFPRIM1("negative?",negativep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(NEGATIVE? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval negativep(lispval x)
{
  int sgn = kno_numcompare(x,KNO_INT(0));
  if (sgn<0)
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Basic ops */

/* Arithmetic */

DEFPRIM("+",plus_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(-1),
	"`(+)` **undocumented**");
static lispval plus_lexpr(int n,kno_argvec args)
{
  if (n==0)
    return KNO_FIXNUM_ZERO;
  else if (n==1) {
    lispval x = args[0];
    if (FIXNUMP(x))
      return x;
    else return kno_incref(x);}
  else if (n==2)
    return kno_plus(args[0],args[1]);
  else {
    int i = 0; int floating = 0, generic = 0, vector = 0;
    while (i < n)
      if (FIXNUMP(args[i])) i++;
      else if (KNO_FLONUMP(args[i])) {
	floating = 1; i++;}
      else if ((VECTORP(args[i]))||(KNO_NUMVECP(args[i]))) {
	generic = 1; vector = 1; i++;}
      else {generic = 1; i++;}
    if ((floating==0) && (generic==0)) {
      long long fixresult = 0;
      i = 0; while (i < n) {
	long long val = 0;
	if (FIXNUMP(args[i]))
	  val = kno_getint(args[i]);
	fixresult = fixresult+val;
	i++;}
      return KNO_INT(fixresult);}
    else if (generic == 0) {
      double floresult = 0.0;
      i = 0; while (i < n) {
	double val;
	if (FIXNUMP(args[i])) val = (double)kno_getint(args[i]);
	else if (KNO_BIGINTP(args[i]))
	  val = (double)kno_bigint_to_double((kno_bigint)args[i]);
	else val = ((struct KNO_FLONUM *)args[i])->floval;
	floresult = floresult+val;
	i++;}
      return kno_init_double(NULL,floresult);}
    else if (vector) {
      lispval result = kno_plus(args[0],args[1]);
      if (KNO_ABORTP(result)) return result;
      i = 2; while (i < n) {
	lispval newv = kno_plus(result,args[i]);
	if (KNO_ABORTP(newv)) {
	  kno_decref(result); return newv;}
	kno_decref(result); result = newv; i++;}
      return result;}
    else {
      lispval result = KNO_INT(0);
      i = 0; while (i < n) {
	lispval newv = kno_plus(result,args[i]);
	if (KNO_ABORTP(newv)) {
	  kno_decref(result); return newv;}
	kno_decref(result); result = newv; i++;}
      return result;}
  }
}

DEFPRIM1("1+",plus1,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(1+ *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval plus1(lispval x)
{
  if (FIXNUMP(x)) {
    long long iv = kno_getint(x); iv++;
    return KNO_INT2LISP(iv);}
  else if (KNO_FLONUMP(x)) {
    kno_double iv = KNO_FLONUM(x); iv = iv+1;
    return kno_make_flonum(iv);}
  else {
    lispval args[2]; args[0]=x; args[1]=KNO_INT(1);
    return plus_lexpr(2,args);}
}
DEFPRIM1("-1+",minus1,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(-1+ *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval minus1(lispval x)
{
  if (FIXNUMP(x)) {
    long long iv = kno_getint(x); iv--;
    return KNO_INT2LISP(iv);}
  else if (KNO_FLONUMP(x)) {
    kno_double iv = KNO_FLONUM(x); iv = iv-1;
    return kno_make_flonum(iv);}
  else {
    lispval args[2] = { x, KNO_INT(-1) };
    return plus_lexpr(2,args);}
}

DEFPRIM("*",times_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(-1),
	"`(*)` **undocumented**");
static lispval times_lexpr(int n,kno_argvec args)
{
  int i = 0; int floating = 0, generic = 0;
  if (n==1) {
    lispval arg = args[0];
    if (FIXNUMP(arg)) return arg;
    else if (NUMBERP(arg))
      return  kno_incref(arg);
    else if ((KNO_NUMVECP(arg))||(VECTORP(arg)))
      return kno_incref(arg);
    return kno_type_error(_("number"),"times_lexpr",arg);}
  else if (n==2)
    return kno_multiply(args[0],args[1]);
  else {
    while (i < n)
      if (FIXNUMP(args[i])) i++;
      else if (KNO_FLONUMP(args[i])) {
	floating = 1; i++;}
      else {generic = 1; i++;}
    if ((floating==0) && (generic==0)) {
      long long fixresult = 1;
      i = 0; while (i < n) {
	long long mult = kno_getint(args[i]);
	if (mult==0)
	  return KNO_INT(0);
	else {
	  int q = ((mult>0)?(KNO_MAX_FIXNUM/mult):(KNO_MIN_FIXNUM/mult));
	  if ((fixresult>0)?(fixresult>q):((-fixresult)>q)) {
	    lispval bigresult = kno_multiply(KNO_INT(fixresult),args[i]);
	    i++; while (i<n) {
	      lispval bigprod = kno_multiply(bigresult,args[i]);
	      kno_decref(bigresult); bigresult = bigprod; i++;}
	    return bigresult;}
	  else fixresult = fixresult*mult;}
	i++;}
      return KNO_INT(fixresult);}
    else if (generic == 0) {
      double floresult = 1.0;
      i = 0; while (i < n) {
	double val;
	if (FIXNUMP(args[i])) val = (double)FIX2INT(args[i]);
	else if  (KNO_BIGINTP(args[i]))
	  val = (double)kno_bigint_to_double((kno_bigint)args[i]);
	else val = ((struct KNO_FLONUM *)args[i])->floval;
	floresult = floresult*val;
	i++;}
      return kno_init_double(NULL,floresult);}
    else {
      lispval result = KNO_INT(1);
      i = 0; while (i < n) {
	lispval newv = kno_multiply(result,args[i]);
	kno_decref(result); result = newv; i++;}
      return result;}
  }
}

DEFPRIM("-",minus_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(-1),
	"`(-)` **undocumented**");
static lispval minus_lexpr(int n,kno_argvec args)
{
  if (n == 1) {
    lispval arg = args[0];
    if (FIXNUMP(arg))
      return KNO_INT(-(FIX2INT(arg)));
    else if (KNO_FLONUMP(arg))
      return kno_init_double(NULL,-(KNO_FLONUM(arg)));
    else if ((VECTORP(arg))||(KNO_NUMVECP(arg)))
      return kno_multiply(arg,FIX2INT(-1));
    else return kno_subtract(KNO_INT(0),arg);}
  else if (n == 2)
    return kno_subtract(args[0],args[1]);
  else {
    int i = 0; int floating = 0, generic = 0, vector = 0;
    while (i < n) {
      if (FIXNUMP(args[i])) i++;
      else if (KNO_FLONUMP(args[i])) {
	floating = 1;
	i++;}
      else if ((VECTORP(args[i]))||(KNO_NUMVECP(args[i]))) {
	vector = 1;
	generic = 1;
	i++;}
      else {generic = 1; i++;}}
    if ((floating==0) && (generic==0)) {
      long long fixresult = 0;
      i = 0; while (i < n) {
	long long val = 0;
	if (FIXNUMP(args[i]))
	  val = FIX2INT(args[i]);
	else if  (KNO_BIGINTP(args[i]))
	  val = (double)kno_bigint_to_double((kno_bigint)args[i]);
	if (i==0) fixresult = val;
	else fixresult = fixresult-val;
	i++;}
      return KNO_INT(fixresult);}
    else if (generic == 0) {
      double floresult = 0.0;
      i = 0; while (i < n) {
	double val;
	if (FIXNUMP(args[i])) val = (double)FIX2INT(args[i]);
	else if  (KNO_BIGINTP(args[i]))
	  val = (double)kno_bigint_to_double((kno_bigint)args[i]);
	else val = ((struct KNO_FLONUM *)args[i])->floval;
	if (i==0) floresult = val;
	else floresult = floresult-val;
	i++;}
      return kno_init_double(NULL,floresult);}
    else if (vector) {
      lispval result = kno_subtract(args[0],args[1]);
      i = 2; while (i < n) {
	lispval newv = kno_subtract(result,args[i]);
	kno_decref(result);
	if (KNO_ABORTP(newv))
	  return newv;
	else result = newv;
	i++;}
      return result;}
    else {
      lispval result = kno_incref(args[0]);
      i = 1; while (i < n) {
	lispval newv = kno_subtract(result,args[i]);
	kno_decref(result);
	if (KNO_ABORTP(newv))
	  return newv;
	else result = newv;
	i++;}
      return result;}}
}

static double todouble(lispval x)
{
  if (FIXNUMP(x))
    return (double)(FIX2INT(x));
  else if (KNO_BIGINTP(x))
    return (double)kno_bigint_to_double((kno_bigint)x);
  else if (KNO_FLONUMP(x))
    return (((struct KNO_FLONUM *)x)->floval);
  else {
    /* This won't really work, but we should catch the error before
       this point.  */
    kno_seterr(kno_TypeError,"todouble",NULL,x);
    return -1.0;}
}

DEFPRIM("/",div_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(-1),
	"`(/)` **undocumented**");
static lispval div_lexpr(int n,kno_argvec args)
{
  int all_double = 1, i = 0;
  while (i<n)
    if (KNO_FLONUMP(args[i])) i++;
    else {all_double = 0; break;}
  if (all_double)
    if (n == 1)
      return kno_init_double(NULL,(1.0/(KNO_FLONUM(args[0]))));
    else {
      double val = KNO_FLONUM(args[0]); i = 1;
      while (i<n) {val = val/KNO_FLONUM(args[i]); i++;}
      return kno_init_double(NULL,val);}
  else if (n==1)
    return kno_divide(KNO_INT(1),args[0]);
  else {
    lispval value = kno_incref(args[0]); i = 1;
    while (i<n) {
      lispval newv = kno_divide(value,args[i]);
      kno_decref(value); value = newv; i++;}
    return value;}
}

DEFPRIM("/~",idiv_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(-1),
	"`(/~)` **undocumented**");
static lispval idiv_lexpr(int n,kno_argvec args)
{
  int all_double = 1, i = 0;
  while (i<n)
    if (KNO_FLONUMP(args[i])) i++;
    else if (!((FIXNUMP(args[i])) || (KNO_BIGINTP(args[i]))))
      return kno_type_error(_("scalar"),"idiv_lexpr",args[i]);
    else {all_double = 0; i++;}
  if (all_double)
    if (n == 1)
      return kno_init_double(NULL,(1.0/(KNO_FLONUM(args[0]))));
    else {
      double val = KNO_FLONUM(args[0]); i = 1;
      while (i<n) {val = val/KNO_FLONUM(args[i]); i++;}
      return kno_init_double(NULL,val);}
  else if (n==1) {
    double d = todouble(args[0]);
    return kno_init_double(NULL,(1.0/d));}
  else {
    double val = todouble(args[0]); i = 1;
    while (i<n) {
      double dval = todouble(args[i]);
      val = val/dval; i++;}
    return kno_init_double(NULL,val);}
}

DEFPRIM2("remainder",remainder_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(REMAINDER *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval remainder_prim(lispval x,lispval m)
{
  if ((FIXNUMP(x)) && (FIXNUMP(m))) {
    long long ix = FIX2INT(x), im = FIX2INT(m);
    long long r = ix%im;
    return KNO_INT(r);}
  else return kno_remainder(x,m);
}

DEFPRIM1("random",random_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(RANDOM *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval random_prim(lispval maxarg)
{
  if (KNO_INTEGERP(maxarg)) {
    long long max = kno_getint(maxarg);
    if (max<=0)
      return kno_type_error("Small positive integer","random_prim",maxarg);
    else if (max > UINT_MAX) {
      long long short_max = max%USHRT_MAX;
      unsigned long long base_rand = max-short_max;
      unsigned long long big_rand =  u8_random(short_max);
      while (base_rand>0) {
	unsigned int short_rand = u8_random(USHRT_MAX);
	big_rand = (big_rand << 16) + short_rand;
	base_rand = base_rand >> 16;}
      big_rand = big_rand % max;
      if (big_rand<KNO_MAX_FIXNUM)
	return KNO_INT2FIX(big_rand);
      else {
	kno_bigint bi = kno_ulong_long_to_bigint(big_rand);
	return (lispval) bi;}}
    else {
      unsigned int n = u8_random(max);
      return KNO_INT(n);}}
  else if (KNO_FLONUMP(maxarg)) {
    double flomax = KNO_FLONUM(maxarg);
    long long  intmax = (long long)flomax;
    long long n = u8_random(intmax);
    double rval = (double) n;
    return kno_make_flonum(rval);}
  else return kno_type_error("integer or flonum","random_prim",maxarg);
}

/* Making some numbers */

DEFPRIM2("make-rational",make_rational,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(MAKE-RATIONAL *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval make_rational(lispval n,lispval d)
{
  return kno_make_rational(n,d);
}

DEFPRIM1("numerator",numerator_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(NUMERATOR *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval numerator_prim(lispval x)
{
  if (KNO_RATIONALP(x)) {
    lispval num = KNO_NUMERATOR(x);
    return kno_incref(num);}
  else if (NUMBERP(x)) return kno_incref(x);
  else return kno_type_error("number","numerator_prim",x);
}

DEFPRIM1("denominator",denominator_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(DENOMINATOR *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval denominator_prim(lispval x)
{
  if (KNO_RATIONALP(x)) {
    lispval den = KNO_DENOMINATOR(x);
    return kno_incref(den);}
  else if (NUMBERP(x)) return KNO_INT(1);
  else return kno_type_error("number","denominator_prim",x);
}

DEFPRIM2("make-complex",make_complex,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(MAKE-COMPLEX *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval make_complex(lispval r,lispval i)
{
  return kno_make_complex(r,i);
}

DEFPRIM1("real-part",real_part_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(REAL-PART *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval real_part_prim(lispval x)
{
  if (REALP(x)) return kno_incref(x);
  else if (KNO_COMPLEXP(x)) {
    lispval r = KNO_REALPART(x);
    return kno_incref(r);}
  else return kno_type_error("number","real_part_prim",x);
}

DEFPRIM1("imag-part",imag_part_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(IMAG-PART *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval imag_part_prim(lispval x)
{
  if (REALP(x)) return KNO_INT(0);
  else if (KNO_COMPLEXP(x)) {
    lispval r = KNO_IMAGPART(x);
    return kno_incref(r);}
  else return kno_type_error("number","imag_part_prim",x);
}

static double doublearg(lispval x,lispval *whoops)
{
  if (FIXNUMP(x))
    return (double)(FIX2INT(x));
  else if (KNO_BIGINTP(x))
    return (double)kno_bigint_to_double((kno_bigint)x);
  else if (KNO_FLONUMP(x))
    return (((struct KNO_FLONUM *)x)->floval);
  else {
    *whoops = kno_type_error("number","doublearg",x);
    return 0;}
}

DEFPRIM1("exact->inexact",exact2inexact,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXACT->INEXACT *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval exact2inexact(lispval x)
{
  return kno_make_inexact(x);
}

DEFPRIM1("inexact->exact",inexact2exact,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(INEXACT->EXACT *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval inexact2exact(lispval x)
{
  return kno_make_exact(x);
}

DEFPRIM1("->flonum",toflonum,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(->FLONUM *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval toflonum(lispval x)
{
  if (KNO_TYPEP(x,kno_complex_type))
    return kno_err("BadFlonumCoercion","toflonum",NULL,x);
  else return kno_make_inexact(x);
}

DEFPRIM2("->exact",toexact,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(->EXACT *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval toexact(lispval x,lispval direction)
{
  long long dir = -1;
  if ((VOIDP(direction))||(FALSEP(direction)))
    dir = -1;
  else if (FIXNUMP(direction))
    dir = FIX2INT(direction);
  else dir = 0;
  if (KNO_FLONUMP(x)) {
    if (dir== -1)
      return kno_make_exact(x);
    else {
      double d = KNO_FLONUM(x);
      struct KNO_FLONUM tmp;
      KNO_INIT_STATIC_CONS(&tmp,kno_flonum_type);
      if (dir==1)
	tmp.floval = ceil(d);
      else tmp.floval = round(d);
      return kno_make_exact((lispval)(&tmp));}}
  else if (KNO_COMPLEXP(x)) {
    lispval real = KNO_REALPART(x), imag = KNO_IMAGPART(x);
    if ((KNO_FLONUMP(real))||(KNO_FLONUMP(imag))) {
      struct KNO_COMPLEX *num = u8_alloc(struct KNO_COMPLEX);
      lispval xreal = toexact(real,direction);
      lispval ximag = toexact(imag,direction);
      KNO_INIT_CONS(num,kno_complex_type);
      num->realpart = xreal; num->imagpart = ximag;
      return (lispval) num;}
    else {
      kno_incref(x);
      return x;}}
  else if (NUMBERP(x)) {
    kno_incref(x);
    return x;}
  else return kno_type_error("number","toexact",x);
}

#define arithdef(sname,lname,cname) \
  static lispval lname(lispval x) {			\
    lispval err = VOID; double val = doublearg(x,&err); \
    errno = 0;						\
    if (VOIDP(err)) {					\
      double result = cname(val);			 \
      if (errno==0) return kno_init_double(NULL,result); \
      else {u8_graberrno(sname,u8_mkstring("%f",val));	 \
	return KNO_ERROR;}}                              \
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

#define arithdef2(sname,lname,cname)	      \
  static lispval lname(lispval x,lispval y) { \
    lispval err = VOID; double xval, yval;     \
    xval = doublearg(x,&err);                  \
    if (!(VOIDP(err))) return err;	       \
    yval = doublearg(y,&err);                  \
    errno = 0;				       \
    if (VOIDP(err)) {			       \
      double result = cname(xval,yval);			 \
      if (errno==0) return kno_init_double(NULL,result);	\
      else {u8_graberrno(sname,u8_mkstring("%f %f",xval,yval)); \
	return KNO_ERROR;}}					\
    else return err;}

arithdef2("ATAN2",latan2,atan2);
arithdef2("POW",lpow,pow);

#undef arithdef
#undef arithdef2

DEFPRIM1("cos",lcos,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(COS *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);

DEFPRIM1("lsqrt",lsqrt,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SQRT *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);

DEFPRIM1("acos",lacos,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ACOS *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);

DEFPRIM1("sin",lsin,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SIN *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);

DEFPRIM1("asin",lasin,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ASIN *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);

DEFPRIM1("atan",latan,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ATAN *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);

DEFPRIM1("tan",ltan,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(TAN *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);

DEFPRIM1("log",llog,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(LOG *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);

DEFPRIM1("exp",lexp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(EXP *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);

DEFPRIM2("atan2",latan2,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(ATAN2 *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);

DEFPRIM2("pow~",lpow,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(POW~ *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);

DEFPRIM2("pow",pow_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(POW *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval pow_prim(lispval v,lispval n)
{
  if ((KNO_EXACTP(v))&&
      (FIXNUMP(n))&&(FIX2INT(n)>=0)&&
      (FIX2INT(n)<1000000)) {
    if (KNO_ZEROP(v)) return KNO_FIXNUM_ONE;
    else {
      long long i = 0, how_many = FIX2INT(n);
      lispval prod = KNO_INT(1); while (i<how_many) {
	lispval tmp = kno_multiply(prod,v);
	kno_decref(prod); prod = tmp;
	i++;}
      return prod;}}
  else {
    lispval err = VOID;
    lispval dv = doublearg(v,&err);
    lispval dn = (VOIDP(err)) ? doublearg(n,&err) : (0);
    if (KNO_ABORTP(err)) return err;
    else {
      double result = pow(dv,dn);
      return kno_make_flonum(result);}}
}

DEFPRIM2("nthroot",nthroot_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(NTHROOT *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval nthroot_prim(lispval v,lispval n)
{
  lispval err = VOID;
  double dv = doublearg(v,&err);
  double dn = (VOIDP(err)) ? (doublearg(n,&err)) :(0);
  double dexp = (VOIDP(err)) ? (1/dn) : (0);
  if (KNO_ABORTP(err))
    return err;
  else {
    double result = pow(dv,dexp);
    if ((remainder(result,1.0)==0)&&
	(KNO_UINTP(n))&&(FIX2INT(n)<1024)&&
	((FIXNUMP(v))||(KNO_BIGINTP(v)))) {
      long long introot = (long long) floor(result);
      lispval root = KNO_INT(introot), prod = KNO_INT(1);
      int i = 0, lim = FIX2INT(n); while (i<lim) {
	lispval tmp = kno_multiply(prod,root);
	kno_decref(prod); prod = tmp;
	i++;}
      if (kno_numcompare(v,prod)==0) {
	kno_decref(prod);
	return root;}
      else {
	kno_decref(prod);
	return kno_make_flonum(result);}}
    else return kno_make_flonum(result);}
}

DEFPRIM2("nthroot~",inexact_nthroot_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(NTHROOT~ *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval inexact_nthroot_prim(lispval v,lispval n)
{
  lispval err = VOID;
  double dv = doublearg(v,&err);
  double dn = (VOIDP(err)) ? (doublearg(n,&err)) :(0);
  double dexp = (VOIDP(err)) ? (1/dn) : (0);
  if (KNO_ABORTP(err))
    return err;
  else {
    double result = pow(dv,dexp);
    return kno_make_flonum(result);}
}

/* Min/Max operators, etc */


DEFPRIM("min",min_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	"`(MIN *arg0* *args...*)` **undocumented**");
static lispval min_prim(int n,kno_argvec args)
{
  if (n==0)
    return kno_err(kno_TooFewArgs,"max_prim",NULL,VOID);
  else {
    lispval result = args[0]; int i = 1, inexact = KNO_FLONUMP(args[0]);
    while (i<n) {
      int cmp = kno_numcompare(args[i],result);
      if (cmp>1) return KNO_ERROR;
      if (KNO_FLONUMP(args[i])) inexact = 1;
      if (cmp<0) result = args[i];
      i++;}
    if (inexact)
      if (KNO_FLONUMP(result))
	return kno_incref(result);
      else {
	lispval err = VOID;
	double asdouble = doublearg(result,&err);
	if (VOIDP(err))
	  return kno_init_double(NULL,asdouble);
	else return err;}
    else return kno_incref(result);}
}

DEFPRIM("max",max_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	"`(MAX *arg0* *args...*)` **undocumented**");
static lispval max_prim(int n,kno_argvec args)
{
  if (n==0) return kno_err(kno_TooFewArgs,"max_prim",NULL,VOID);
  else {
    lispval result = args[0];
    int i = 1, inexact = KNO_FLONUMP(args[0]);
    while (i<n) {
      int cmp = kno_numcompare(args[i],result);
      if (cmp>1) return KNO_ERROR;
      if (KNO_FLONUMP(args[i])) inexact = 1;
      if (cmp>0) result = args[i];
      i++;}
    if (inexact)
      if (KNO_FLONUMP(result))
	return kno_incref(result);
      else {
	lispval err = VOID;
	double asdouble = doublearg(result,&err);
	if (VOIDP(err))
	  return kno_init_double(NULL,asdouble);
	else return err;}
    else return kno_incref(result);}
}

DEFPRIM1("abs",abs_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ABS *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval abs_prim(lispval x)
{
  if (FIXNUMP(x)) {
    long long ival = FIX2INT(x);
    assert((ival<0) == ((FIX2INT(x))<0));
    if (ival<0) {
      long long aval = -ival;
      return KNO_INT(aval);}
    else return x;}
  else if (kno_numcompare(x,KNO_INT(0))<0)
    return kno_subtract(KNO_INT(0),x);
  else return kno_incref(x);
}

DEFPRIM2("modulo",modulo_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(MODULO *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval modulo_prim(lispval x,lispval b)
{
  if ((FIXNUMP(x)) && (FIXNUMP(b))) {
    long long ix = FIX2INT(x), ib = FIX2INT(b);
    if (ix==0) return KNO_INT(0);
    else if (ib==0)
      return kno_type_error("nonzero","modulo_prim",b);
    else if (((ix>0) && (ib>0)) || ((ix<0) && (ib<0)))
      return KNO_INT((ix%ib));
    else {
      long long rem = ix%ib;
      if (rem) return KNO_INT(ib+rem);
      else return KNO_INT(rem);}}
  else {
    int xsign = kno_numcompare(x,KNO_INT(0));
    int bsign = kno_numcompare(b,KNO_INT(0));
    if (xsign==0) return KNO_INT(0);
    else if (bsign==0)
      return kno_type_error("nonzero","modulo_prim",b);
    else if (((xsign>0) && (bsign>0)) || ((xsign<0) && (bsign<0)))
      return kno_remainder(x,b);
    else {
      lispval rem = kno_remainder(x,b);
      if ((FIXNUMP(rem)) && (FIX2INT(rem)==0))
	return rem;
      else {
	lispval result = kno_plus(b,rem);
	kno_decref(rem);
	return result;}}}
}

DEFPRIM2("gcd",gcd_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(GCD *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval gcd_prim(lispval x,lispval y)
{
  if ((INTEGERP(x)) && (INTEGERP(y)))
    return kno_gcd(x,y);
  else if (INTEGERP(y))
    return kno_type_error("integer","gcd_prim",x);
  else return kno_type_error("integer","gcd_prim",y);
}

DEFPRIM2("lcm",lcm_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(LCM *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval lcm_prim(lispval x,lispval y)
{
  if ((INTEGERP(x)) && (INTEGERP(y)))
    return kno_lcm(x,y);
  else if (INTEGERP(y))
    return kno_type_error("integer","lcm_prim",x);
  else return kno_type_error("integer","lcm_prim",y);
}

DEFPRIM1("truncate",truncate_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(TRUNCATE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval truncate_prim(lispval x)
{
  if (INTEGERP(x)) return kno_incref(x);
  else if (KNO_FLONUMP(x)) {
    double d = trunc(KNO_FLONUM(x));
    return kno_init_double(NULL,d);}
  else if (KNO_RATIONALP(x)) {
    lispval n = KNO_NUMERATOR(x), d = KNO_DENOMINATOR(x);
    lispval q = kno_quotient(n,d);
    return q;}
  else return kno_type_error(_("scalar"),"floor_prim",x);
}

DEFPRIM1("floor",floor_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FLOOR *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval floor_prim(lispval x)
{
  if (INTEGERP(x)) return kno_incref(x);
  else if (KNO_FLONUMP(x)) {
    double d = floor(KNO_FLONUM(x));
    return kno_init_double(NULL,d);}
  else if (KNO_RATIONALP(x)) {
    lispval n = KNO_NUMERATOR(x), d = KNO_DENOMINATOR(x);
    lispval q = kno_quotient(n,d);
    if (kno_numcompare(x,KNO_INT(0))<0) {
      lispval qminus = kno_subtract(q,KNO_INT(1));
      kno_decref(q);
      return qminus;}
    else return q;}
  else return kno_type_error(_("scalar"),"floor_prim",x);
}

DEFPRIM1("ceiling",ceiling_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(CEILING *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval ceiling_prim(lispval x)
{
  if (INTEGERP(x)) return kno_incref(x);
  else if (KNO_FLONUMP(x)) {
    double d = ceil(KNO_FLONUM(x));
    return kno_init_double(NULL,d);}
  else if (KNO_RATIONALP(x)) {
    lispval n = KNO_NUMERATOR(x), d = KNO_DENOMINATOR(x);
    lispval q = kno_quotient(n,d);
    if (kno_numcompare(x,KNO_INT(0))>0) {
      lispval qminus = kno_plus(q,KNO_INT(1));
      kno_decref(q);
      return qminus;}
    else return q;}
  else return kno_type_error(_("scalar"),"ceiling_prim",x);
}

DEFPRIM1("round",round_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ROUND *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval round_prim(lispval x)
{
  if (INTEGERP(x)) return kno_incref(x);
  else if (KNO_FLONUMP(x)) {
    double v = KNO_FLONUM(x);
    double f = floor(v), c = ceil(v);
    if ((v<0) ? ((c-v)>=(v-f)) : ((c-v)>(v-f)))
      return kno_init_double(NULL,f);
    else return kno_init_double(NULL,c);}
  else if (KNO_RATIONALP(x)) {
    lispval n = KNO_NUMERATOR(x), d = KNO_DENOMINATOR(x);
    lispval q = kno_quotient(n,d);
    lispval r = kno_remainder(n,d);
    lispval halfd = kno_quotient(d,KNO_INT(2));
    if (kno_numcompare(r,halfd)<0) {
      kno_decref(halfd); kno_decref(r);
      return q;}
    else if (kno_numcompare(x,KNO_INT(0))<0) {
      lispval qplus = kno_subtract(q,KNO_INT(1));
      kno_decref(q);
      return qplus;}
    else {
      lispval qminus = kno_plus(q,KNO_INT(1));
      kno_decref(q);
      return qminus;}}
  else return kno_type_error(_("scalar"),"floor_prim",x);
}

static double doround(double x)
{
  double c = ceil(x), f = floor(x);
  if ((x-f)<0.5) return f; else return c;
}

DEFPRIM2("scalerep",scalerep_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(SCALEREP *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval scalerep_prim(lispval x,lispval scalearg)
{
  long long scale = kno_getint(scalearg);
  if (scale<0)
    if (FIXNUMP(x)) return x;
    else if (KNO_FLONUMP(x)) {
      long long factor = -scale;
      double val = KNO_FLONUM(x), factor_up = doround(val*factor);
      long long ival = factor_up;
      return kno_conspair(KNO_INT(ival),scalearg);}
    else return EMPTY;
  else if (FIXNUMP(x)) {
    long long ival = FIX2INT(x), rem = ival%scale, base = (ival/scale);
    if (rem==0) return kno_conspair(x,scalearg);
    else if (rem*2>scale)
      if (ival>0)
	return kno_conspair(KNO_INT((base+1)*scale),scalearg);
      else return kno_conspair(KNO_INT((base-1)*scale),scalearg);
    else return kno_conspair(KNO_INT(base*scale),scalearg);}
  else if (KNO_FLONUMP(x)) {
    double dv = KNO_FLONUM(x);
    double scaled = doround(dv/scale)*scale;
    long long ival = scaled;
    return kno_conspair(KNO_INT(ival),scalearg);}
  else return EMPTY;
}

/* More simple arithmetic functions */

DEFPRIM2("quotient",quotient_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(QUOTIENT *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval quotient_prim(lispval x,lispval y)
{
  if ((FIXNUMP(x)) && (FIXNUMP(y))) {
    long long ix = FIX2INT(x), iy = FIX2INT(y);
    if (iy == 0) return kno_err("DivisionByZero","quotient_prim",NULL,y);
    long long q = ix/iy;
    return KNO_INT(q);}
  else return kno_quotient(x,y);
}

DEFPRIM1("sqrt",sqrt_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SQRT *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval sqrt_prim(lispval x)
{
  double v = todouble(x);
  double sqr = sqrt(v);
  return kno_init_double(NULL,sqr);
}

/* Log and exponent functions */

DEFPRIM2("ilog",ilog_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(ILOG *arg0* [*arg1*])` **undocumented**",
	 kno_fixnum_type,KNO_VOID,kno_fixnum_type,KNO_CPP_INT(2));
static lispval ilog_prim(lispval n,lispval base_arg)
{
  long long base = kno_getint(base_arg), limit = kno_getint(n);
  long long count = 0, exp = 1;
  while (exp<limit) {
    count++; exp = exp*base;}
  return KNO_INT(count);
}

/* Integer hashing etc. */

DEFPRIM1("knuth-hash",knuth_hash,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(KNUTH-HASH *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval knuth_hash(lispval arg)
{
  if ((FIXNUMP(arg))||(KNO_BIGINTP(arg))) {
    long long num=
      ((FIXNUMP(arg))?(FIX2INT(arg)):
       (kno_bigint_to_long_long((kno_bigint)arg)));
    if ((num<0)||(num>=0x100000000ll))
      return kno_type_error("uint32","knuth_hash",arg);
    else {
      unsigned long long int hash = (num*2654435761ll)%0x100000000ll;
      return KNO_INT(hash);}}
  else return kno_type_error("uint32","knuth_hash",arg);
}

DEFPRIM1("wang-hash32",wang_hash32,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(WANG-HASH32 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval wang_hash32(lispval arg)
{
  /* Adapted from Thomas Wang
     http://www.cris.com/~Ttwang/tech/inthash.htm */
  if (((KNO_UINTP(arg))&&((FIX2INT(arg))>=0))||
      ((KNO_BIGINTP(arg))&&(!(kno_bigint_negativep((kno_bigint)arg)))&&
       (kno_bigint_fits_in_word_p(((kno_bigint)arg),32,0)))) {
    unsigned long long num=
      ((FIXNUMP(arg))?(FIX2INT(arg)):
       (kno_bigint_to_ulong_long((kno_bigint)arg)));
    unsigned long long constval = 0x27d4eb2dll; // a prime or an odd constant
    num = (num ^ 61) ^ (num >> 16);
    num = num + (num << 3);
    num = num ^ (num >> 4);
    num = num * constval;
    num = num ^ (num >> 15);
    return KNO_INT(num);}
  else return kno_type_error("uint32","wang_hash32",arg);
}

DEFPRIM1("wang-hash64",wang_hash64,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(WANG-HASH64 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval wang_hash64(lispval arg)
{
  /* Adapted from Thomas Wang
     http://www.cris.com/~Ttwang/tech/inthash.htm */
  if (((FIXNUMP(arg))&&((FIX2INT(arg))>=0))||
      ((KNO_BIGINTP(arg))&&(!(kno_bigint_negativep((kno_bigint)arg)))&&
       (kno_bigint_fits_in_word_p(((kno_bigint)arg),64,0)))) {
    unsigned long long int num=
      ((FIXNUMP(arg))?(FIX2INT(arg)):
       (kno_bigint_to_ulong_long((kno_bigint)arg)));
    num = (~num) + (num << 21); // num = (num << 21) - num - 1;
    num = num ^ (num >> 24);
    num = (num + (num << 3)) + (num << 8); // num * 265
    num = num ^ (num >> 14);
    num = (num + (num << 2)) + (num << 4); // num * 21
    num = num ^ (num >> 28);
    num = num + (num << 31);
    if (num<0x8000000000000000ll)
      return KNO_INT(num);
    else return (lispval) kno_ulong_long_to_bigint(num);}
  else return kno_type_error("uint64","wang_hash64",arg);
}

DEFPRIM1("flip32",flip32,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FLIP32 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval flip32(lispval arg)
{
  if (((KNO_UINTP(arg))&&((FIX2INT(arg))>=0))||
      ((KNO_BIGINTP(arg))&&(!(kno_bigint_negativep((kno_bigint)arg)))&&
       (kno_bigint_fits_in_word_p(((kno_bigint)arg),32,0)))) {
    int word = kno_getint(arg);
    int flipped = kno_flip_word(word);
    return KNO_INT(flipped);}
  else return kno_type_error("uint32","flip32",arg);
}

kno_bigint kno_ulong_long_to_bigint(unsigned long long);

DEFPRIM1("flip64",flip64,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FLIP64 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval flip64(lispval arg)
{
  if (((FIXNUMP(arg))&&((FIX2INT(arg))>=0))||
      ((KNO_BIGINTP(arg))&&(!(kno_bigint_negativep((kno_bigint)arg)))&&
       (kno_bigint_fits_in_word_p(((kno_bigint)arg),64,0)))) {
    unsigned long long int word=
      ((FIXNUMP(arg))?(FIX2INT(arg)):
       (kno_bigint_to_ulong_long((kno_bigint)arg)));
    unsigned long long int flipped = kno_flip_word8(word);
    if (flipped<KNO_MAX_FIXNUM) return KNO_INT(flipped);
    else return (lispval) kno_ulong_long_to_bigint(flipped);}
  else return kno_type_error("uint64","flip64",arg);
}

/* City hashing */

DEFPRIM2("cityhash64",cityhash64,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(CITYHASH64 *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval cityhash64(lispval arg,lispval asint)
{
  u8_int8 hash;
  const u8_byte *data; size_t datalen;
  if (STRINGP(arg)) {
    data = CSTRING(arg); datalen = STRLEN(arg);}
  else if (PACKETP(arg)) {
    data = KNO_PACKET_DATA(arg); datalen = KNO_PACKET_LENGTH(arg);}
  else return kno_type_error("packet/string","cityhash64",arg);
  hash = u8_cityhash64(data,datalen);
  if (KNO_TRUEP(asint))
    return KNO_INT(hash);
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
    return kno_make_packet(NULL,8,bytes);}
}

DEFPRIM1("cityhash128",cityhash128,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(CITYHASH128 *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval cityhash128(lispval arg)
{
  unsigned char bytes[16];
  u8_int16 hash; u8_int8 hi; u8_int8 lo;
  const u8_byte *data; size_t datalen;
  if (STRINGP(arg)) {
    data = CSTRING(arg); datalen = STRLEN(arg);}
  else if (PACKETP(arg)) {
    data = KNO_PACKET_DATA(arg); datalen = KNO_PACKET_LENGTH(arg);}
  else return kno_type_error("packet/string","cityhash64",arg);
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
  return kno_make_packet(NULL,16,bytes);
}

/* ITOA */

DEFPRIM2("u8itoa",itoa_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(U8ITOA *arg0* [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_CPP_INT(10));
static lispval itoa_prim(lispval arg,lispval base_arg)
{
  long long base = FIX2INT(base_arg); char buf[32];
  if (FIXNUMP(arg)) {
    if (base==10) u8_itoa10(FIX2INT(arg),buf);
    else if (FIX2INT(arg)<0)
      return kno_err(_("negative numbers can't be rendered as non-decimal"),
		     "itoa_prim",NULL,kno_incref(arg));
    else if (base==8) u8_uitoa8(FIX2INT(arg),buf);
    else if (base==16) u8_uitoa16(FIX2INT(arg),buf);
    else return kno_type_error("16,10,or 8","itoa_prim",base_arg);}
  else if (!(KNO_BIGINTP(arg)))
    return kno_type_error("number","itoa_prim",arg);
  else {
    kno_bigint bi = (kno_bigint)arg;
    if ((base==10)&&(kno_bigint_negativep(bi))&&
	(kno_bigint_fits_in_word_p(bi,63,0))) {
      long long int n = kno_bigint_to_long_long(bi);
      u8_itoa10(n,buf);}
    else if ((base==10)&&(kno_bigint_fits_in_word_p(bi,64,0))) {
      unsigned long long int n = kno_bigint_to_ulong_long(bi);
      u8_uitoa10(n,buf);}
    else if (base==10)
      return kno_type_error("smallish bigint","itoa_prim",arg);
    else if (kno_bigint_negativep(bi)) {
      return kno_err(_("negative numbers can't be rendered as non-decimal"),
		     "itoa_prim",NULL,kno_incref(arg));}
    else if (kno_bigint_fits_in_word_p(bi,64,0)) {
      unsigned long long int n = kno_bigint_to_ulong_long(bi);
      if (base==8) u8_uitoa8(n,buf);
      else if (base==16) u8_uitoa16(n,buf);
      else return kno_type_error("16,10,or 8","itoa_prim",base_arg);}
    else return kno_type_error("smallish bigint","itoa_prim",arg);}
  return kno_mkstring(buf);
}

/* string/int conversions */


DEFPRIM2("inexact->string",inexact2string,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(inexact->string *num* [*precision*])\n"
	 "Generates a string of precision *precision* for "
	 "*num*.",
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval inexact2string(lispval x,lispval precision)
{
  if (KNO_FLONUMP(x))
    if ((KNO_UINTP(precision)) || (VOIDP(precision))) {
      int prec = ((VOIDP(precision)) ? (2) : (FIX2INT(precision)));
      char buf[128], cmd[16];
      if (sprintf(cmd,"%%.%df",prec)<1) {
	u8_seterr("Bad precision","inexact2string",NULL);
	return KNO_ERROR_VALUE;}
      else if (sprintf(buf,cmd,KNO_FLONUM(x)) < 1) {
	u8_seterr("Bad precision","inexact2string",NULL);
	return KNO_ERROR_VALUE;}
      return kno_mkstring(buf);}
    else return kno_type_error("fixnum","inexact2string",precision);
  else {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    kno_unparse(&out,x);
    return kno_stream2string(&out);}
}

DEFPRIM2("number->string",number2string,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(number->string *obj* [*base*=10])\n"
	 "Converts *obj* (a number) into a string in the "
	 "given *base*",
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_CPP_INT(10));
static lispval number2string(lispval x,lispval base)
{
  if (NUMBERP(x)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    kno_output_number(&out,x,kno_getint(base));
    return kno_stream2string(&out);}
  else return kno_err(kno_TypeError,"number2string",NULL,x);
}

DEFPRIM2("number->locale",number2locale,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(number->locale *obj* [*precision*])\n"
	 "Converts *obj* into a locale-specific string",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval number2locale(lispval x,lispval precision)
{
  if (KNO_FLONUMP(x))
    if ((KNO_UINTP(precision)) || (VOIDP(precision))) {
      int prec = ((VOIDP(precision)) ? (2) : (FIX2INT(precision)));
      char buf[128]; char cmd[16];
      sprintf(cmd,"%%'.%df",prec);
      sprintf(buf,cmd,KNO_FLONUM(x));
      return kno_mkstring(buf);}
    else return kno_type_error("fixnum","inexact2string",precision);
  else if (FIXNUMP(x)) {
    long long i_val = FIX2INT(x);
    char tmpbuf[32];
    return kno_mkstring(u8_itoa10(i_val,tmpbuf));}
  else {
    U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    kno_unparse(&out,x);
    return kno_stream2string(&out);}
}

DEFPRIM2("string->number",string2number,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(string->number *string* [*base*])\n"
	 "Converts *string* into a number, returning #f on "
	 "failure",
	 kno_string_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval string2number(lispval x,lispval base)
{
  int use_base = ((VOIDP(base))||(DEFAULTP(base))) ? (-1) : (kno_getint(base));
  return kno_string2number(CSTRING(x),use_base);
}

DEFPRIM1("->hex",tohex,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Converts a number to a hex string or a hex string "
	 "to a number",
	 kno_any_type,KNO_VOID);
static lispval tohex(lispval x)
{
  if (KNO_STRINGP(x))
    return kno_string2number(CSTRING(x),16);
  else if (KNO_NUMBERP(x)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    kno_output_number(&out,x,16);
    return kno_stream2string(&out);}
  else return kno_err(kno_NotANumber,"argtohex",NULL,x);
}

DEFPRIM2("->number",just2number,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(->number *obj* [*base*])\n"
	 "Converts *obj* into a number",
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval just2number(lispval x,lispval base)
{
  if (NUMBERP(x)) return kno_incref(x);
  else if (STRINGP(x)) {
    int use_base = ((VOIDP(base))||(DEFAULTP(base))) ? (-1) : (kno_getint(base));
    lispval num = kno_string2number(CSTRING(x),use_base);
    if (FALSEP(num))
      return KNO_FALSE;
    else return num;}
  else return kno_type_error(_("string or number"),"->NUMBER",x);
}

/* Initialization */

#define arithdef(sname,lname,cname)				\
  kno_idefn(kno_scheme_module,kno_make_cprim1(sname,lname,1))
#define arithdef2(sname,lname,cname)				\
  kno_idefn(kno_scheme_module,kno_make_cprim2(sname,lname,2))

KNO_EXPORT void kno_init_arith_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();

  kno_store(kno_scheme_module,kno_intern("max-fixnum"),kno_max_fixnum);
  kno_store(kno_scheme_module,kno_intern("min-fixnum"),kno_min_fixnum);
}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("->number",just2number,2,scheme_module);
  KNO_LINK_PRIM("->hex",tohex,1,scheme_module);
  KNO_LINK_PRIM("string->number",string2number,2,scheme_module);
  KNO_LINK_PRIM("number->locale",number2locale,2,scheme_module);
  KNO_LINK_PRIM("number->string",number2string,2,scheme_module);
  KNO_LINK_PRIM("inexact->string",inexact2string,2,scheme_module);
  KNO_LINK_PRIM("u8itoa",itoa_prim,2,scheme_module);
  KNO_LINK_PRIM("cityhash128",cityhash128,1,scheme_module);
  KNO_LINK_PRIM("cityhash64",cityhash64,2,scheme_module);
  KNO_LINK_PRIM("flip64",flip64,1,scheme_module);
  KNO_LINK_PRIM("flip32",flip32,1,scheme_module);
  KNO_LINK_PRIM("wang-hash64",wang_hash64,1,scheme_module);
  KNO_LINK_PRIM("wang-hash32",wang_hash32,1,scheme_module);
  KNO_LINK_PRIM("knuth-hash",knuth_hash,1,scheme_module);
  KNO_LINK_PRIM("ilog",ilog_prim,2,scheme_module);
  KNO_LINK_PRIM("sqrt",sqrt_prim,1,scheme_module);
  KNO_LINK_PRIM("quotient",quotient_prim,2,scheme_module);
  KNO_LINK_PRIM("scalerep",scalerep_prim,2,scheme_module);
  KNO_LINK_PRIM("round",round_prim,1,scheme_module);
  KNO_LINK_PRIM("ceiling",ceiling_prim,1,scheme_module);
  KNO_LINK_PRIM("floor",floor_prim,1,scheme_module);
  KNO_LINK_PRIM("truncate",truncate_prim,1,scheme_module);
  KNO_LINK_PRIM("lcm",lcm_prim,2,scheme_module);
  KNO_LINK_PRIM("gcd",gcd_prim,2,scheme_module);
  KNO_LINK_PRIM("modulo",modulo_prim,2,scheme_module);
  KNO_LINK_PRIM("abs",abs_prim,1,scheme_module);
  KNO_LINK_VARARGS("max",max_prim,scheme_module);
  KNO_LINK_VARARGS("min",min_prim,scheme_module);
  KNO_LINK_PRIM("nthroot~",inexact_nthroot_prim,2,scheme_module);
  KNO_LINK_PRIM("nthroot",nthroot_prim,2,scheme_module);
  KNO_LINK_PRIM("pow",pow_prim,2,scheme_module);
  KNO_LINK_PRIM("->exact",toexact,2,scheme_module);
  KNO_LINK_PRIM("->flonum",toflonum,1,scheme_module);
  KNO_LINK_PRIM("inexact->exact",inexact2exact,1,scheme_module);
  KNO_LINK_PRIM("exact->inexact",exact2inexact,1,scheme_module);
  KNO_LINK_PRIM("imag-part",imag_part_prim,1,scheme_module);
  KNO_LINK_PRIM("real-part",real_part_prim,1,scheme_module);
  KNO_LINK_PRIM("make-complex",make_complex,2,scheme_module);
  KNO_LINK_PRIM("denominator",denominator_prim,1,scheme_module);
  KNO_LINK_PRIM("numerator",numerator_prim,1,scheme_module);
  KNO_LINK_PRIM("make-rational",make_rational,2,scheme_module);
  KNO_LINK_PRIM("random",random_prim,1,scheme_module);
  KNO_LINK_PRIM("remainder",remainder_prim,2,scheme_module);
  KNO_LINK_VARARGS("/~",idiv_lexpr,scheme_module);
  KNO_LINK_VARARGS("/",div_lexpr,scheme_module);
  KNO_LINK_VARARGS("-",minus_lexpr,scheme_module);
  KNO_LINK_VARARGS("*",times_lexpr,scheme_module);
  KNO_LINK_PRIM("-1+",minus1,1,scheme_module);
  KNO_LINK_PRIM("1+",plus1,1,scheme_module);
  KNO_LINK_VARARGS("+",plus_lexpr,scheme_module);
  KNO_LINK_PRIM("negative?",negativep,1,scheme_module);
  KNO_LINK_PRIM("positive?",positivep,1,scheme_module);
  KNO_LINK_PRIM("real?",realp,1,scheme_module);
  KNO_LINK_PRIM("even?",evenp,1,scheme_module);
  KNO_LINK_PRIM("odd?",oddp,1,scheme_module);
  KNO_LINK_PRIM("inexact?",inexactp,1,scheme_module);
  KNO_LINK_PRIM("exact?",exactp,1,scheme_module);
  KNO_LINK_PRIM("rational?",rationalp,1,scheme_module);
  KNO_LINK_PRIM("integer?",integerp,1,scheme_module);
  KNO_LINK_PRIM("bignum?",bignump,1,scheme_module);
  KNO_LINK_PRIM("fixnum?",fixnump,1,scheme_module);
  KNO_LINK_PRIM("complex?",complexp,1,scheme_module);

  KNO_LINK_PRIM("SQRT",lsqrt,1,scheme_module);
  KNO_LINK_PRIM("COS",lcos,1,scheme_module);
  KNO_LINK_PRIM("ACOS",lacos,1,scheme_module);
  KNO_LINK_PRIM("SIN",lsin,1,scheme_module);
  KNO_LINK_PRIM("ASIN",lasin,1,scheme_module);
  KNO_LINK_PRIM("ATAN",latan,1,scheme_module);
  KNO_LINK_PRIM("TAN",ltan,1,scheme_module);
  KNO_LINK_PRIM("LOG",llog,1,scheme_module);
  KNO_LINK_PRIM("EXP",lexp,1,scheme_module);

  KNO_LINK_PRIM("ATAN2",latan2,2,scheme_module);
  KNO_LINK_PRIM("POW~",lpow,2,scheme_module);

  KNO_LINK_ALIAS("->inexact",exact2inexact,scheme_module);
  KNO_LINK_ALIAS("1-",minus1,scheme_module);
  KNO_LINK_ALIAS("->0x",tohex,scheme_module);
}

