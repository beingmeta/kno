/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/numbers.h"

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
    fdtype real=FD_REALPART(x), imag=FD_IMAGPART(x);
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
    fdtype real=FD_REALPART(x), imag=FD_IMAGPART(x);
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
    int ival=FD_FIX2INT(x);
    if (ival%2) return FD_TRUE; else return FD_FALSE;}
  else if (FD_BIGINTP(x)) {
    fdtype remainder=fd_remainder(x,FD_INT2DTYPE(2));
    if (FD_ABORTP(remainder)) return remainder;
    else if (FD_FIXNUMP(remainder))
      if (FD_INT2DTYPE(remainder)) return FD_TRUE; else return FD_FALSE;
    else {
      fd_decref(remainder); return FD_FALSE;}}
  else return FD_FALSE;
}

static fdtype evenp(fdtype x)
{
  if (FD_FIXNUMP(x)) {
    int ival=FD_FIX2INT(x);
    if (ival%2) return FD_FALSE; else return FD_TRUE;}
  else if (FD_BIGINTP(x)) {
    fdtype remainder=fd_remainder(x,FD_INT2DTYPE(2));
    if (FD_ABORTP(remainder)) return remainder;
    else if (FD_FIXNUMP(remainder))
      if (FD_INT2DTYPE(remainder)) return FD_FALSE; else return FD_TRUE;
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
  int sgn=fd_numcompare(x,FD_INT2DTYPE(0));
  if (sgn>0) return FD_TRUE; else return FD_FALSE;
}
static fdtype negativep(fdtype x)
{
  int sgn=fd_numcompare(x,FD_INT2DTYPE(0));
  if (sgn<0) return FD_TRUE; else return FD_FALSE;
}

static fdtype make_rational(fdtype n,fdtype d)
{
  return fd_make_rational(n,d);
}

static fdtype numerator_prim(fdtype x)
{
  if (FD_RATIONALP(x)) {
    fdtype num=FD_NUMERATOR(x);
    return fd_incref(num);}
  else if (FD_NUMBERP(x)) return fd_incref(x);
  else return fd_type_error("number","numerator_prim",x);
}

static fdtype denominator_prim(fdtype x)
{
  if (FD_RATIONALP(x)) {
    fdtype den=FD_DENOMINATOR(x);
    return fd_incref(den);}
  else if (FD_NUMBERP(x)) return FD_INT2DTYPE(1);
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
    fdtype r=FD_REALPART(x);
    return fd_incref(r);}
  else return fd_type_error("number","real_part_prim",x);
}

static fdtype imag_part_prim(fdtype x)
{
  if (REALP(x)) return FD_INT2DTYPE(0);
  else if (FD_COMPLEXP(x)) {
    fdtype r=FD_IMAGPART(x);
    return fd_incref(r);}
  else return fd_type_error("number","imag_part_prim",x);
}

static double todouble(fdtype x,fdtype *whoops)
{
  if (FD_FIXNUMP(x))
    return (double)(FD_FIX2INT(x));
  else if (FD_PTR_TYPEP(x,fd_bigint_type))
    return (double)fd_bigint_to_double((fd_bigint)x);
  else if (FD_PTR_TYPEP(x,fd_double_type))
    return (((struct FD_DOUBLE *)x)->flonum);
  else {
    *whoops=fd_type_error("number","todouble",x);
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

#define arithdef(sname,lname,cname) \
  static fdtype lname(fdtype x) { \
    fdtype err=FD_VOID; double val=todouble(x,&err); \
    errno=0; \
    if (FD_VOIDP(err)) { \
      double result=cname(val); \
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
    fdtype err=FD_VOID; double xval, yval; \
    xval=todouble(x,&err);                  \
    if (!(FD_VOIDP(err))) return err;       \
    yval=todouble(y,&err);                  \
    errno=0;                                \
    if (FD_VOIDP(err)) {                    \
      double result=cname(xval,yval);       \
      if (errno==0) return fd_init_double(NULL,result); \
      else {u8_graberr(-1,sname,u8_mkstring("%f %f",xval,yval)); \
            return FD_ERROR_VALUE;}}          \
    else return err;}

arithdef2("ATAN2",latan2,atan2);

static fdtype min_prim(int n,fdtype *args)
{
  if (n==0) return fd_err(fd_TooFewArgs,"max_prim",NULL,FD_VOID);
  else {
    fdtype result=args[0]; int i=1, inexact=FD_FLONUMP(args[0]);
    while (i<n) {
      int cmp=fd_numcompare(args[i],result);
      if (FD_FLONUMP(args[i])) inexact=1;
      if (cmp<0) result=args[i];
      i++;}
    if (inexact)
      if (FD_FLONUMP(result))
	return fd_incref(result);
      else {
	fdtype err=FD_VOID;
	double asdouble=todouble(result,&err);
	if (FD_VOIDP(err))
	  return fd_init_double(NULL,asdouble);
	else return err;}
    else return fd_incref(result);}
}

static fdtype max_prim(int n,fdtype *args)
{
  if (n==0) return fd_err(fd_TooFewArgs,"max_prim",NULL,FD_VOID);
  else {
    fdtype result=args[0]; int i=1, inexact=FD_FLONUMP(args[0]);
    while (i<n) {
      int cmp=fd_numcompare(args[i],result);
      if (FD_FLONUMP(args[i])) inexact=1;
      if (cmp>0) result=args[i];
      i++;}
    if (inexact)
      if (FD_FLONUMP(result))
	return fd_incref(result);
      else {
	fdtype err=FD_VOID;
	double asdouble=todouble(result,&err);
	if (FD_VOIDP(err))
	  return fd_init_double(NULL,asdouble);
	else return err;}
    else return fd_incref(result);}
}

static fdtype abs_prim(fdtype x)
{
  if (FD_FIXNUMP(x))
    if (FD_FIX2INT(x)<0)
      return FD_INT2DTYPE(-(FD_FIX2INT(x)));
    else return x;
  else if (fd_numcompare(x,FD_INT2DTYPE(0))<0)
    return fd_subtract(FD_INT2DTYPE(0),x);
  else return fd_incref(x);
}

static fdtype modulo_prim(fdtype x,fdtype b)
{
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(b))) {
    int ix=FD_FIX2INT(x), ib=FD_FIX2INT(b);
    if (ix==0) return FD_INT2DTYPE(0);
    else if (ib==0)
      return fd_type_error("nonzero","modulo_prim",b);
    else if (((ix>0) && (ib>0)) || ((ix<0) && (ib<0)))
      return FD_INT2DTYPE((ix%ib));
    else {
      int rem=ix%ib;
      if (rem) return FD_INT2DTYPE(ib+rem);
      else return rem;}}
  else {
    int xsign=fd_numcompare(x,FD_INT2DTYPE(0));
    int bsign=fd_numcompare(b,FD_INT2DTYPE(0));
    if (xsign==0) return FD_INT2DTYPE(0);
    else if (bsign==0)
      return fd_type_error("nonzero","modulo_prim",b);
    else if (((xsign>0) && (bsign>0)) || ((xsign<0) && (bsign<0)))
      return fd_remainder(x,b);
    else {
      fdtype rem=fd_remainder(x,b);
      if ((FD_FIXNUMP(rem)) && (FD_FIX2INT(rem)==0))
	return rem;
      else {
	fdtype result=fd_plus(b,rem);
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
    double d=trunc(FD_FLONUM(x));
    return fd_init_double(NULL,d);}
  else if (FD_RATIONALP(x)) {
    fdtype n=FD_NUMERATOR(x), d=FD_DENOMINATOR(x);
      fdtype q=fd_quotient(n,d);
      return q;}
  else return fd_type_error(_("scalar"),"floor_prim",x);
}

static fdtype floor_prim(fdtype x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double d=floor(FD_FLONUM(x));
    return fd_init_double(NULL,d);}
  else if (FD_RATIONALP(x)) {
    fdtype n=FD_NUMERATOR(x), d=FD_DENOMINATOR(x);
    fdtype q=fd_quotient(n,d);
    if (fd_numcompare(x,FD_INT2DTYPE(0))<0) {
      fdtype qminus=fd_subtract(q,FD_INT2DTYPE(1));
      fd_decref(q); 
      return qminus;}
    else return q;}
  else return fd_type_error(_("scalar"),"floor_prim",x);
}

static fdtype ceiling_prim(fdtype x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double d=ceil(FD_FLONUM(x));
    return fd_init_double(NULL,d);}
  else if (FD_RATIONALP(x)) {
    fdtype n=FD_NUMERATOR(x), d=FD_DENOMINATOR(x);
    fdtype q=fd_quotient(n,d);
    if (fd_numcompare(x,FD_INT2DTYPE(0))>0) {
      fdtype qminus=fd_plus(q,FD_INT2DTYPE(1));
      fd_decref(q); 
      return qminus;}
    else return q;}
  else return fd_type_error(_("scalar"),"ceiling_prim",x);
}

static fdtype round_prim(fdtype x)
{
  if (INTEGERP(x)) return fd_incref(x);
  else if (FD_FLONUMP(x)) {
    double v=FD_FLONUM(x);
    double f=floor(v), c=ceil(v);
    if ((v<0) ? ((c-v)>=(v-f)) : ((c-v)>(v-f)))
      return fd_init_double(NULL,f);
    else return fd_init_double(NULL,c);}
  else if (FD_RATIONALP(x)) {
    fdtype n=FD_NUMERATOR(x), d=FD_DENOMINATOR(x);
    fdtype q=fd_quotient(n,d);
    fdtype r=fd_remainder(n,d);
    fdtype halfd=fd_quotient(d,FD_INT2DTYPE(2));
    if (fd_numcompare(r,halfd)<0) {
      fd_decref(halfd); fd_decref(r);
      return q;}
    else if (fd_numcompare(x,FD_INT2DTYPE(0))<0) {
      fdtype qplus=fd_subtract(q,FD_INT2DTYPE(1));
      fd_decref(q); 
      return qplus;}
    else {
      fdtype qminus=fd_plus(q,FD_INT2DTYPE(1));
      fd_decref(q); 
      return qminus;}}
  else return fd_type_error(_("scalar"),"floor_prim",x);
}

static double doround(double x)
{
  double c=ceil(x), f=floor(x);
  if ((x-f)<0.5) return f; else return c;
}

static fdtype scalerep_prim(fdtype x,fdtype scalearg)
{
  int scale=fd_getint(scalearg);
  if (scale<0)
    if (FD_FIXNUMP(x)) return x;
    else if (FD_FLONUMP(x)) {
      int factor=-scale;
      double val=FD_FLONUM(x), factor_up=doround(val*factor);
      int ival=factor_up;
      return fd_init_pair(NULL,FD_INT2DTYPE(ival),scalearg);}
    else return FD_EMPTY_CHOICE;
  else if (FD_FIXNUMP(x)) {
    int ival=FD_FIX2INT(x), rem=ival%scale, base=(ival/scale);
    if (rem==0) return fd_init_pair(NULL,x,scalearg);
    else if (rem*2>scale)
      if (ival>0)
	return fd_init_pair(NULL,FD_INT2DTYPE((base+1)*scale),scalearg);
      else return fd_init_pair(NULL,FD_INT2DTYPE((base-1)*scale),scalearg);
    else return fd_init_pair(NULL,FD_INT2DTYPE(base*scale),scalearg);}
  else if (FD_FLONUMP(x)) {
    double dv=FD_FLONUM(x);
    double scaled=doround(dv/scale)*scale;
    int ival=scaled;
    return fd_init_pair(NULL,FD_INT2DTYPE(ival),scalearg);}
  else return FD_EMPTY_CHOICE;
}

/* Log and exponent functions */

static fdtype ilog_prim(fdtype n,fdtype base_arg)
{
  int base=fd_getint(base_arg), limit=fd_getint(n);
  int count=0, exp=1;
  while (exp<limit) {
    count++; exp=exp*base;}
  return FD_INT2DTYPE(count);
}

/* Initialization */

#undef arithdef
#undef arithdef2

#define arithdef(sname,lname,cname) \
  fd_idefn(fd_scheme_module,fd_make_cprim1(sname,lname,1))
#define arithdef2(sname,lname,cname) \
  fd_idefn(fd_scheme_module,fd_make_cprim2(sname,lname,2))

FD_EXPORT void fd_init_numeric_c()
{
  fd_register_source_file(versionid);

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
					    fd_fixnum_type,FD_INT2DTYPE(2)));
}
