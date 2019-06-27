/* -*-C-*-

    Copyright (c) 1989-1993 Massachusetts Institute of Technology
    Copyright (c) 1993-2001 Massachusetts Institute of Technology
    Copyright (c) 2001-2019 beingmeta, inc.

    Part of this code is based on the bignum code from the Scheme Project at
    MIT, which was used with minor modifications by the Media Laboratory at
    MIT, and which was further modified by beingmeta.  The notice below
    covers the original Scheme Project code.

    This material was developed by the Scheme project at the Massachusetts
    Institute of Technology, Department of Electrical Engineering and
    Computer Science.  Permission to copy this software, to redistribute
    it, and to use it for any purpose is granted, subject to the following
    restrictions and understandings.

    1. Any copy made of this software must include this copyright notice
    in full.

    2. Users of this software agree to make their best efforts (a) to
    return to the MIT Scheme project any improvements or extensions that
    they make, so that these may be included in future releases; and (b)
    to inform MIT of noteworthy uses of this software.

    3. All materials developed as a consequence of the use of this
    software shall duly acknowledge such use, in accordance with the usual
    standards of acknowledging credit in academic research.

    4. MIT has made no warrantee or representation that the operation of
    this software will be error-free, and MIT is under no obligation to
    provide any services, by way of maintenance, update, or otherwise.

    5. In conjunction with products arising from the use of this material,
    there shall be no use of the name of the Massachusetts Institute of
    Technology nor of any adaptation thereof in any advertising,
    promotional, or sales literature without prior written consent from
    MIT in each case. */

/* Implementation of various number types, including bigints
   (unlimited precision integers).  The bigint implementation
   is directly based on the bignums implementation from MIT Scheme. */

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/bigints.h"
#include "kno/numbers.h"
#include "kno/hash.h"

#include <libu8/u8stringfns.h>
#include <libu8/u8printf.h>

#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <math.h>
#include <ctype.h>
#include "biginternals.h"

/* For sprintf */
#include <stdio.h>

u8_condition kno_NotANumber=_("Not a number");
u8_condition kno_BigIntException=_("BigInt Exception");
u8_condition kno_DivideByZero=_("Division by zero");
u8_condition kno_InvalidNumericLiteral=_("Invalid numeric literal");

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshift-negative-value"

#if KNO_AVOID_MACROS
lispval kno_max_fixnum;
lispval kno_min_fixnum;
#else
lispval kno_max_fixnum = KNO_INT(KNO_MAX_FIXNUM);
lispval kno_min_fixnum = KNO_INT(KNO_MIN_FIXNUM);
#endif

#pragma clang diagnostic pop

/* These macros come from the original MIT Scheme code */
#define	DEFUN(name, arglist, args)	name(args)
#define	DEFUN_VOID(name)		name()
#define	AND		,

typedef void (*bigint_consumer)(void *,int);
typedef unsigned int (*bigint_producer)(void *);

static double todouble(lispval x);

static int numvec_length(lispval x);
static lispval vector_add(lispval x,lispval y,int mult);
static lispval vector_dotproduct(lispval x,lispval y);
static lispval vector_scale(lispval vec,lispval scalar);

int kno_numvec_showmax = 7;


static kno_bigint
DEFUN (bigint_malloc, (kno_veclen), int length)
{
  char * result = (malloc (sizeof(struct KNO_CONS)+((length + 1) * (sizeof (bigint_digit_type)))));
  BIGINT_ASSERT (result != ((char *) 0));
  KNO_INIT_CONS(((struct KNO_RAW_CONS *)result),kno_bigint_type);
  return ((kno_bigint) result);
}

static kno_bigint
DEFUN (bigint_realloc, (bigint, kno_veclen),
       kno_bigint bigint AND int length)
{
  char * result =
    (realloc (((char *) bigint),
	      ((length + 2) * (sizeof (bigint_digit_type)))));
  BIGINT_ASSERT (result != ((char *) 0));
  return ((kno_bigint) result);
}

/* Forward references */
static int bigint_equal_p_unsigned(kno_bigint, kno_bigint);
static enum kno_bigint_comparison bigint_compare_unsigned(kno_bigint, kno_bigint);
static kno_bigint bigint_add_unsigned(kno_bigint, kno_bigint, int);
static kno_bigint bigint_subtract_unsigned(kno_bigint, kno_bigint, int);
static kno_bigint bigint_multiply_unsigned(kno_bigint, kno_bigint, int);
static kno_bigint bigint_multiply_unsigned_small_factor(kno_bigint, bigint_digit_type, int);
static void bigint_destructive_scale_up(kno_bigint, bigint_digit_type);
static void bigint_destructive_add(kno_bigint, bigint_digit_type);
static void bigint_divide_unsigned_large_denominator
  (kno_bigint, kno_bigint, kno_bigint *,
   kno_bigint *,int, int);
static void bigint_destructive_normalization
  (kno_bigint, kno_bigint, int);
static void bigint_destructive_unnormalization(kno_bigint, int);
static void bigint_divide_unsigned_normalized
  (kno_bigint, kno_bigint, kno_bigint);
static bigint_digit_type bigint_divide_subtract
  (bigint_digit_type *, bigint_digit_type *,
   bigint_digit_type, bigint_digit_type *);
static void bigint_divide_unsigned_medium_denominator
  (kno_bigint, bigint_digit_type, kno_bigint *,
   kno_bigint *, int, int);
static bigint_digit_type bigint_digit_divide
  (bigint_digit_type, bigint_digit_type,
   bigint_digit_type, bigint_digit_type *);
static bigint_digit_type bigint_digit_divide_subtract
  (bigint_digit_type, bigint_digit_type,
   bigint_digit_type, bigint_digit_type *);
static void bigint_divide_unsigned_small_denominator
  (kno_bigint, bigint_digit_type, kno_bigint *,
   kno_bigint *, int, int);
static bigint_digit_type bigint_destructive_scale_down(kno_bigint, bigint_digit_type);
static kno_bigint bigint_remainder_unsigned_small_denominator(kno_bigint, bigint_digit_type, int);
static kno_bigint bigint_digit_to_bigint(bigint_digit_type, int);
static kno_bigint bigint_allocate(int, int);
static kno_bigint bigint_allocate_zeroed(int, int);
static kno_bigint bigint_shorten_length(kno_bigint, int);
static kno_bigint bigint_trim(kno_bigint);
static kno_bigint bigint_copy(kno_bigint);
static kno_bigint bigint_new_sign(kno_bigint, int);
static kno_bigint bigint_maybe_new_sign(kno_bigint, int);
static void bigint_destructive_copy(kno_bigint, kno_bigint);


/* Exports */

kno_bigint
DEFUN_VOID (bigint_make_zero)
{
  fast kno_bigint result = (BIGINT_ALLOCATE (0));
  BIGINT_SET_HEADER (result, 0, 0);
  return (result);
}

kno_bigint
DEFUN (bigint_make_one, (negative_p), int negative_p)
{
  fast kno_bigint result = (BIGINT_ALLOCATE (1));
  BIGINT_SET_HEADER (result, 1, negative_p);
  (BIGINT_REF (result, 0)) = 1;
  return (result);
}

int
DEFUN (bigint_equal_p, (x, y),
       fast kno_bigint x AND fast kno_bigint y)
{
  return
    ((BIGINT_ZERO_P (x))
     ? (BIGINT_ZERO_P (y))
     : ((! (BIGINT_ZERO_P (y)))
	&& ((BIGINT_NEGATIVE_P (x))
	    ? (BIGINT_NEGATIVE_P (y))
	    : (! (BIGINT_NEGATIVE_P (y))))
	&& (bigint_equal_p_unsigned (x, y))));
}

enum kno_bigint_comparison
DEFUN (bigint_test, (bigint), fast kno_bigint bigint)
{
  return
    ((BIGINT_ZERO_P (bigint))
     ? kno_bigint_equal
     : (BIGINT_NEGATIVE_P (bigint))
     ? kno_bigint_less
     : kno_bigint_greater);
}

enum kno_bigint_comparison
DEFUN (kno_bigint_compare, (x, y),
       fast kno_bigint x AND fast kno_bigint y)
{
  return
    ((BIGINT_ZERO_P (x))
     ? ((BIGINT_ZERO_P (y))
	? kno_bigint_equal
	: (BIGINT_NEGATIVE_P (y))
	? kno_bigint_greater
	: kno_bigint_less)
     : (BIGINT_ZERO_P (y))
     ? ((BIGINT_NEGATIVE_P (x))
	? kno_bigint_less
	: kno_bigint_greater)
     : (BIGINT_NEGATIVE_P (x))
     ? ((BIGINT_NEGATIVE_P (y))
	? (bigint_compare_unsigned (y, x))
	: (kno_bigint_less))
     : ((BIGINT_NEGATIVE_P (y))
	? (kno_bigint_greater)
	: (bigint_compare_unsigned (x, y))));
}

kno_bigint
DEFUN (kno_bigint_add, (x, y),
       fast kno_bigint x AND fast kno_bigint y)
{
  return
    ((BIGINT_ZERO_P (x))
     ? (BIGINT_MAYBE_COPY (y))
     : (BIGINT_ZERO_P (y))
     ? (BIGINT_MAYBE_COPY (x))
     : ((BIGINT_NEGATIVE_P (x))
	? ((BIGINT_NEGATIVE_P (y))
	   ? (bigint_add_unsigned (x, y, 1))
	   : (bigint_subtract_unsigned (y, x, 0)))
	: ((BIGINT_NEGATIVE_P (y))
	   ? (bigint_subtract_unsigned (x, y, 0))
	   : (bigint_add_unsigned (x, y, 0)))));
}

kno_bigint
DEFUN (kno_bigint_subtract, (x, y),
       fast kno_bigint x AND fast kno_bigint y)
{
  return
    ((BIGINT_ZERO_P (x))
     ? ((BIGINT_ZERO_P (y))
	? (BIGINT_MAYBE_COPY (y))
	: (bigint_new_sign (y, (! (BIGINT_NEGATIVE_P (y))))))
     : ((BIGINT_ZERO_P (y))
	? (BIGINT_MAYBE_COPY (x))
	: ((BIGINT_NEGATIVE_P (x))
	   ? ((BIGINT_NEGATIVE_P (y))
	      ? (bigint_subtract_unsigned (x, y, 1))
	      : (bigint_add_unsigned (y, x, 1)))
	   : ((BIGINT_NEGATIVE_P (y))
	      ? (bigint_add_unsigned (x, y, 0))
	      : (bigint_subtract_unsigned (x, y, 0))))));
}

kno_bigint
DEFUN (kno_bigint_negate, (x), fast kno_bigint x)
{
  return
    ((BIGINT_ZERO_P (x))
     ? (BIGINT_MAYBE_COPY (x))
     : (bigint_new_sign (x, (! (BIGINT_NEGATIVE_P (x))))));
}

kno_bigint
DEFUN (kno_bigint_multiply, (x, y),
       fast kno_bigint x AND fast kno_bigint y)
{
  fast int x_length = (BIGINT_LENGTH (x));
  fast int y_length = (BIGINT_LENGTH (y));
  fast int negative_p =
    ((BIGINT_NEGATIVE_P (x))
     ? (! (BIGINT_NEGATIVE_P (y)))
     : (BIGINT_NEGATIVE_P (y)));
  if (BIGINT_ZERO_P (x))
    return (BIGINT_MAYBE_COPY (x));
  if (BIGINT_ZERO_P (y))
    return (BIGINT_MAYBE_COPY (y));
  if (x_length == 1)
    {
      bigint_digit_type digit = (BIGINT_REF (x, 0));
      if (digit == 1)
	return (bigint_maybe_new_sign (y, negative_p));
      if (digit < BIGINT_RADIX_ROOT)
	return (bigint_multiply_unsigned_small_factor (y, digit, negative_p));
    }
  if (y_length == 1)
    {
      bigint_digit_type digit = (BIGINT_REF (y, 0));
      if (digit == 1)
	return (bigint_maybe_new_sign (x, negative_p));
      if (digit < BIGINT_RADIX_ROOT)
	return (bigint_multiply_unsigned_small_factor (x, digit, negative_p));
    }
  return (bigint_multiply_unsigned (x, y, negative_p));
}

int
DEFUN (kno_bigint_divide, (numerator, denominator, quotient, remainder),
       kno_bigint numerator AND kno_bigint denominator
       AND kno_bigint * quotient AND kno_bigint * remainder)
{
  if (BIGINT_ZERO_P (denominator))
    return (1);
  if (BIGINT_ZERO_P (numerator))
    {
      (*quotient) = (BIGINT_MAYBE_COPY (numerator));
      (*remainder) = (BIGINT_MAYBE_COPY (numerator));
    }
  else
    {
      int r_negative_p = (BIGINT_NEGATIVE_P (numerator));
      int q_negative_p =
	((BIGINT_NEGATIVE_P (denominator)) ? (! r_negative_p) : r_negative_p);
      switch (bigint_compare_unsigned (numerator, denominator))
	{
	case kno_bigint_equal:
	  {
	    (*quotient) = (BIGINT_ONE (q_negative_p));
	    (*remainder) = (BIGINT_ZERO ());
	    break;
	  }
	case kno_bigint_less:
	  {
	    (*quotient) = (BIGINT_ZERO ());
	    (*remainder) = (BIGINT_MAYBE_COPY (numerator));
	    break;
	  }
	case kno_bigint_greater:
	  {
	    if ((BIGINT_LENGTH (denominator)) == 1)
	      {
		bigint_digit_type digit = (BIGINT_REF (denominator, 0));
		if (digit == 1)
		  {
		    (*quotient) =
		      (bigint_maybe_new_sign (numerator, q_negative_p));
		    (*remainder) = (BIGINT_ZERO ());
		    break;
		  }
		else if (digit < BIGINT_RADIX_ROOT)
		  {
		    bigint_divide_unsigned_small_denominator
		      (numerator, digit,
		       quotient, remainder,
		       q_negative_p, r_negative_p);
		    break;
		  }
		else
		  {
		    bigint_divide_unsigned_medium_denominator
		      (numerator, digit,
		       quotient, remainder,
		       q_negative_p, r_negative_p);
		    break;
		  }
	      }
	    bigint_divide_unsigned_large_denominator
	      (numerator, denominator,
	       quotient, remainder,
	       q_negative_p, r_negative_p);
	    break;
	  }
	}
    }
  return (0);
}

kno_bigint
DEFUN (kno_bigint_quotient, (numerator, denominator),
       kno_bigint numerator AND kno_bigint denominator)
{
  if (BIGINT_ZERO_P (denominator))
    return (BIGINT_OUT_OF_BAND);
  if (BIGINT_ZERO_P (numerator))
    return (BIGINT_MAYBE_COPY (numerator));
  {
    int q_negative_p =
      ((BIGINT_NEGATIVE_P (denominator))
       ? (! (BIGINT_NEGATIVE_P (numerator)))
       : (BIGINT_NEGATIVE_P (numerator)));
    switch (bigint_compare_unsigned (numerator, denominator))
      {
      case kno_bigint_equal:
	return (BIGINT_ONE (q_negative_p));
      case kno_bigint_less:
	return (BIGINT_ZERO ());
      case kno_bigint_greater:
	{
	  kno_bigint quotient;
	  if ((BIGINT_LENGTH (denominator)) == 1)
	    {
	      bigint_digit_type digit = (BIGINT_REF (denominator, 0));
	      if (digit == 1)
		return (bigint_maybe_new_sign (numerator, q_negative_p));
	      if (digit < BIGINT_RADIX_ROOT)
		bigint_divide_unsigned_small_denominator
		  (numerator, digit,
		   (&quotient), ((kno_bigint *) 0),
		   q_negative_p, 0);
	      else
		bigint_divide_unsigned_medium_denominator
		  (numerator, digit,
		   (&quotient), ((kno_bigint *) 0),
		   q_negative_p, 0);
	    }
	  else
	    bigint_divide_unsigned_large_denominator
	      (numerator, denominator,
	       (&quotient), ((kno_bigint *) 0),
	       q_negative_p, 0);
	  return (quotient);
	}
      default:
	return (BIGINT_OUT_OF_BAND);
      }
  }
}

kno_bigint
DEFUN (kno_bigint_remainder, (numerator, denominator),
       kno_bigint numerator AND kno_bigint denominator)
{
  if (BIGINT_ZERO_P (denominator))
    return (BIGINT_OUT_OF_BAND);
  if (BIGINT_ZERO_P (numerator))
    return (BIGINT_MAYBE_COPY (numerator));
  switch (bigint_compare_unsigned (numerator, denominator))
    {
    case kno_bigint_equal:
      return (BIGINT_ZERO ());
    case kno_bigint_less:
      return (BIGINT_MAYBE_COPY (numerator));
    case kno_bigint_greater:
      {
	kno_bigint remainder = 0; kno_bigint quotient = 0;
	if ((BIGINT_LENGTH (denominator)) == 1)
	  {
	    bigint_digit_type digit = (BIGINT_REF (denominator, 0));
	    if (digit == 1)
	      return (BIGINT_ZERO ());
	    if (digit < BIGINT_RADIX_ROOT)
	      return
		(bigint_remainder_unsigned_small_denominator
		 (numerator, digit, (BIGINT_NEGATIVE_P (numerator))));
	    bigint_divide_unsigned_medium_denominator
	      (numerator, digit,
	       &quotient, (&remainder),
	       0, (BIGINT_NEGATIVE_P (numerator)));
	  }
	else
	  bigint_divide_unsigned_large_denominator
	    (numerator, denominator,
	     &quotient, (&remainder),
	     0, (BIGINT_NEGATIVE_P (numerator)));
	if (quotient) free(quotient);
	return (remainder);
      }
    default:
      return (BIGINT_OUT_OF_BAND);
    }
}


kno_bigint
DEFUN (kno_long_to_bigint, (n), long n)
{
  int negative_p;
  bigint_digit_type result_digits [BIGINT_DIGITS_FOR_LONG];
  fast bigint_digit_type * end_digits = result_digits;
  /* Special cases win when these small constants are cached. */
  if (n == 0) return (BIGINT_ZERO ());
  if (n == 1) return (BIGINT_ONE (0));
  if (n == -1) return (BIGINT_ONE (1));
  {
    fast unsigned long accumulator = ((negative_p = (n < 0)) ? (-n) : n);
    do
      {
	(*end_digits++) = (accumulator & BIGINT_DIGIT_MASK);
	accumulator >>= BIGINT_DIGIT_LENGTH;
      }
    while (accumulator != 0);
  }
  {
    kno_bigint result =
      (bigint_allocate ((end_digits - result_digits), negative_p));
    fast bigint_digit_type * scan_digits = result_digits;
    fast bigint_digit_type * scan_result = (BIGINT_START_PTR (result));
    while (scan_digits < end_digits)
      (*scan_result++) = (*scan_digits++);
    return (result);
  }
}

kno_bigint
DEFUN (kno_long_long_to_bigint, (n), long long n)
{
  int negative_p;
  bigint_digit_type result_digits [BIGINT_DIGITS_FOR_LONG_LONG];
  fast bigint_digit_type * end_digits = result_digits;
  /* Special cases win when these small constants are cached. */
  if (n == 0) return (BIGINT_ZERO ());
  if (n == 1) return (BIGINT_ONE (0));
  if (n == -1) return (BIGINT_ONE (1));
  {
    fast unsigned long long accumulator = ((negative_p = (n < 0)) ? (-n) : n);
    do
      {
	(*end_digits++) = (accumulator & BIGINT_DIGIT_MASK);
	accumulator >>= BIGINT_DIGIT_LENGTH;
      }
    while (accumulator != 0);
  }
  {
    kno_bigint result =
      (bigint_allocate ((end_digits - result_digits), negative_p));
    fast bigint_digit_type * scan_digits = result_digits;
    fast bigint_digit_type * scan_result = (BIGINT_START_PTR (result));
    while (scan_digits < end_digits)
      (*scan_result++) = (*scan_digits++);
    return (result);
  }
}


kno_bigint
DEFUN (kno_ulong_to_bigint, (n), unsigned long n)
{
  int negative_p = 0;
  bigint_digit_type result_digits [BIGINT_DIGITS_FOR_LONG];
  fast bigint_digit_type * end_digits = result_digits;
  /* Special cases win when these small constants are cached. */
  if (n == 0) return (BIGINT_ZERO ());
  if (n == 1) return (BIGINT_ONE (0));
  if (n == -1) return (BIGINT_ONE (1));
  {
    fast unsigned long accumulator = n;
    do
      {
	(*end_digits++) = (accumulator & BIGINT_DIGIT_MASK);
	accumulator >>= BIGINT_DIGIT_LENGTH;
      }
    while (accumulator != 0);
  }
  {
    kno_bigint result =
      (bigint_allocate ((end_digits - result_digits), negative_p));
    fast bigint_digit_type * scan_digits = result_digits;
    fast bigint_digit_type * scan_result = (BIGINT_START_PTR (result));
    while (scan_digits < end_digits)
      (*scan_result++) = (*scan_digits++);
    return (result);
  }
}

kno_bigint
DEFUN (kno_ulong_long_to_bigint, (n), unsigned long long n)
{
  int negative_p = 0;
  bigint_digit_type result_digits [BIGINT_DIGITS_FOR_LONG_LONG];
  fast bigint_digit_type * end_digits = result_digits;
  /* Special cases win when these small constants are cached. */
  if (n == 0) return (BIGINT_ZERO ());
  if (n == 1) return (BIGINT_ONE (0));
  if (n == -1) return (BIGINT_ONE (1));
  {
    fast unsigned long long accumulator = n;
    do
      {
	(*end_digits++) = (accumulator & BIGINT_DIGIT_MASK);
	accumulator >>= BIGINT_DIGIT_LENGTH;
      }
    while (accumulator != 0);
  }
  {
    kno_bigint result =
      (bigint_allocate ((end_digits - result_digits), negative_p));
    fast bigint_digit_type * scan_digits = result_digits;
    fast bigint_digit_type * scan_result = (BIGINT_START_PTR (result));
    while (scan_digits < end_digits)
      (*scan_result++) = (*scan_digits++);
    return (result);
  }
}

long
DEFUN (kno_bigint_to_long, (bigint), kno_bigint bigint)
{
  if (BIGINT_ZERO_P (bigint))
    return (0);
  {
    fast long accumulator = 0;
    fast bigint_digit_type * start = (BIGINT_START_PTR (bigint));
    fast bigint_digit_type * scan = (start + (BIGINT_LENGTH (bigint)));
    while (start < scan)
      accumulator = ((accumulator << BIGINT_DIGIT_LENGTH) + (*--scan));
    return ((BIGINT_NEGATIVE_P (bigint)) ? (-accumulator) : accumulator);
  }
}

int
DEFUN (kno_bigint_negativep, (bigint), kno_bigint bigint)
{
  if (BIGINT_NEGATIVE_P (bigint)) return (1);
  else return (0);
}

long long
DEFUN (kno_bigint_to_long_long, (bigint), kno_bigint bigint)
{
  if (BIGINT_ZERO_P (bigint))
    return (0);
  {
    fast long long accumulator = 0;
    fast bigint_digit_type * start = (BIGINT_START_PTR (bigint));
    fast bigint_digit_type * scan = (start + (BIGINT_LENGTH (bigint)));
    while (start < scan)
      accumulator = ((accumulator << BIGINT_DIGIT_LENGTH) + (*--scan));
    return ((BIGINT_NEGATIVE_P (bigint)) ? (-accumulator) : accumulator);
  }
}

unsigned long long
DEFUN (kno_bigint_to_ulong_long, (bigint), kno_bigint bigint)
{
  if (BIGINT_ZERO_P (bigint))
    return (0);
  {
    fast unsigned long long accumulator = 0;
    fast bigint_digit_type * start = (BIGINT_START_PTR (bigint));
    fast bigint_digit_type * scan = (start + (BIGINT_LENGTH (bigint)));
    while (start < scan)
      accumulator = ((accumulator << BIGINT_DIGIT_LENGTH) + (*--scan));
    return accumulator;
  }
}

unsigned long
DEFUN (kno_bigint_to_ulong, (bigint), kno_bigint bigint)
{
  if (BIGINT_ZERO_P (bigint))
    return (0);
  {
    fast unsigned long accumulator = 0;
    fast bigint_digit_type * start = (BIGINT_START_PTR (bigint));
    fast bigint_digit_type * scan = (start + (BIGINT_LENGTH (bigint)));
    while (start < scan)
      accumulator = ((accumulator << BIGINT_DIGIT_LENGTH) + (*--scan));
    return accumulator;
  }
}


#define DTB_WRITE_DIGIT(factor)						\
{									\
  significand *= (factor);						\
  digit = ((bigint_digit_type) significand);				\
  (*--scan) = digit;							\
  significand -= ((double) digit);					\
}

kno_bigint
DEFUN (kno_double_to_bigint, (x), double x)
{
  extern double frexp ();
  int exponent;
  fast double significand = (frexp (x, (&exponent)));
  if (exponent <= 0) return (BIGINT_ZERO ());
  if (exponent == 1) return (BIGINT_ONE (x < 0));
  if (significand < 0) significand = (-significand);
  {
    int length = (BIGINT_BITS_TO_DIGITS (exponent));
    kno_bigint result = (bigint_allocate (length, (x < 0)));
    bigint_digit_type * start = (BIGINT_START_PTR (result));
    fast bigint_digit_type * scan = (start + length);
    fast bigint_digit_type digit;
    int odd_bits = (exponent % BIGINT_DIGIT_LENGTH);
    if (odd_bits > 0)
      DTB_WRITE_DIGIT (1L << odd_bits);
    while (start < scan)
      {
	if (significand == 0)
	  {
	    while (start < scan)
	      (*--scan) = 0;
	    break;
	  }
	DTB_WRITE_DIGIT (BIGINT_RADIX);
      }
    return (result);
  }
}

#undef DTB_WRITE_DIGIT

double
DEFUN (kno_bigint_to_double, (bigint), kno_bigint bigint)
{
  if (BIGINT_ZERO_P (bigint))
    return (0);
  {
    fast double accumulator = 0;
    fast bigint_digit_type * start = (BIGINT_START_PTR (bigint));
    fast bigint_digit_type * scan = (start + (BIGINT_LENGTH (bigint)));
    while (start < scan)
      accumulator = ((accumulator * BIGINT_RADIX) + (*--scan));
    return ((BIGINT_NEGATIVE_P (bigint)) ? (-accumulator) : accumulator);
  }
}


int
DEFUN (kno_bigint_fits_in_word_p, (bigint, word_length, twos_complement_p),
       kno_bigint bigint AND long word_length AND int twos_complement_p)
{
  unsigned int n_bits = (twos_complement_p ? (word_length - 1) : word_length);
  BIGINT_ASSERT (n_bits > 0);
  {
    fast int length = (BIGINT_LENGTH (bigint));
    fast int max_digits = (BIGINT_BITS_TO_DIGITS (n_bits));
#if 0
    bigint_digit_type msd, max;
    msd = (BIGINT_REF (bigint, (kno_veclen - 1)));
    max = (1L << (n_bits - ((kno_veclen - 1) * BIGINT_DIGIT_LENGTH)));
#endif
    return
      ((length < max_digits) ||
       ((length == max_digits) &&
	((BIGINT_REF (bigint, (length - 1))) <
	 (1L << (n_bits - ((length - 1) * BIGINT_DIGIT_LENGTH))))));
  }
}

kno_bigint
DEFUN (kno_bigint_length_in_bits, (bigint), kno_bigint bigint)
{
  if (BIGINT_ZERO_P (bigint))
    return (BIGINT_ZERO ());
  {
    int index = ((BIGINT_LENGTH (bigint)) - 1);
    fast bigint_digit_type digit = (BIGINT_REF (bigint, index));
    fast kno_bigint result = (bigint_allocate (2, 0));
    (BIGINT_REF (result, 0)) = index;
    (BIGINT_REF (result, 1)) = 0;
    bigint_destructive_scale_up (result, BIGINT_DIGIT_LENGTH);
    while (digit > 0)
      {
	bigint_destructive_add (result, ((bigint_digit_type) 1));
	digit >>= 1;
      }
    return (bigint_trim (result));
  }
}

unsigned long
DEFUN (kno_bigint_length_in_bytes, (bigint), kno_bigint bigint)
{
  if (BIGINT_ZERO_P (bigint))
    return 0;
  else return BIGINT_LENGTH(bigint)*sizeof(bigint_digit_type);
}

kno_bigint
DEFUN_VOID (bigint_length_upper_limit)
{
  fast kno_bigint result = (bigint_allocate (2, 0));
  (BIGINT_REF (result, 0)) = 0;
  (BIGINT_REF (result, 1)) = BIGINT_DIGIT_LENGTH;
  return (result);
}

kno_bigint
DEFUN (kno_digit_stream_to_bigint,
       (n_digits, producer, context, radix, negative_p),
       fast unsigned int n_digits
       AND unsigned int (*producer)(void *)
       AND void *context
       AND fast unsigned int radix
       AND int negative_p)
{
  BIGINT_ASSERT ((radix > 1) && (radix <= BIGINT_RADIX_ROOT));
  if (n_digits == 0)
    return (BIGINT_ZERO ());
  if (n_digits == 1)
    {
      int digit = (int) producer(context);
      if ((digit<0) || (digit>radix)) return NULL;
      else return (kno_long_to_bigint (negative_p ? (- digit) : digit));
    }
  {
    int length;
    {
      fast unsigned int radix_copy = radix;
      fast unsigned int log_radix = 0;
      while (radix_copy > 0)
	{
	  radix_copy >>= 1;
	  log_radix += 1;
	}
      /* This length will be at least as large as needed. */
      length = (BIGINT_BITS_TO_DIGITS (n_digits * log_radix));
    }
    {
      fast kno_bigint result = (bigint_allocate_zeroed (length, negative_p));
      while ((n_digits--) > 0)
	{
	  int digit = producer(context);
	  if ((digit<0) || (digit>radix)) {
	    kno_decref((lispval)result);
	    return NULL;}
	  bigint_destructive_scale_up (result, ((bigint_digit_type) radix));
	  bigint_destructive_add
	    (result, ((bigint_digit_type) digit));
	}
      return (bigint_trim (result));
    }
  }
}

void
DEFUN (kno_bigint_to_digit_stream, (bigint, radix, consumer, context),
       kno_bigint bigint
       AND unsigned int radix
       AND void (*consumer)(void *, int)
       AND void *context)
{
  BIGINT_ASSERT ((radix > 1) && (radix <= BIGINT_RADIX_ROOT));
  if (! (BIGINT_ZERO_P (bigint)))
    {
      fast kno_bigint working_copy = (bigint_copy (bigint));
      fast bigint_digit_type * start = (BIGINT_START_PTR (working_copy));
      fast bigint_digit_type * scan = (start + (BIGINT_LENGTH (working_copy)));
      while (start < scan)
	{
	  if ((scan[-1]) == 0)
	    scan -= 1;
	  else
	    (*consumer)
	      (context, (bigint_destructive_scale_down (working_copy, radix)));
	}
      BIGINT_DEALLOCATE (working_copy);
    }
  return;
}

long
DEFUN_VOID (kno_bigint_max_digit_stream_radix)
{
  return (BIGINT_RADIX_ROOT);
}

/* Comparisons */

static int
DEFUN (bigint_equal_p_unsigned, (x, y),
       kno_bigint x AND kno_bigint y)
{
  int length = (BIGINT_LENGTH (x));
  if (length != (BIGINT_LENGTH (y)))
    return (0);
  else
    {
      fast bigint_digit_type * scan_x = (BIGINT_START_PTR (x));
      fast bigint_digit_type * scan_y = (BIGINT_START_PTR (y));
      fast bigint_digit_type * end_x = (scan_x + length);
      while (scan_x < end_x)
	if ((*scan_x++) != (*scan_y++))
	  return (0);
      return (1);
    }
}

static enum kno_bigint_comparison
DEFUN (bigint_compare_unsigned, (x, y),
       kno_bigint x AND kno_bigint y)
{
  int x_length = (BIGINT_LENGTH (x));
  int y_length = (BIGINT_LENGTH (y));
  if (x_length < y_length)
    return (kno_bigint_less);
  if (x_length > y_length)
    return (kno_bigint_greater);
  {
    fast bigint_digit_type * start_x = (BIGINT_START_PTR (x));
    fast bigint_digit_type * scan_x = (start_x + x_length);
    fast bigint_digit_type * scan_y = ((BIGINT_START_PTR (y)) + y_length);
    while (start_x < scan_x)
      {
	fast bigint_digit_type digit_x = (*--scan_x);
	fast bigint_digit_type digit_y = (*--scan_y);
	if (digit_x < digit_y)
	  return (kno_bigint_less);
	if (digit_x > digit_y)
	  return (kno_bigint_greater);
      }
  }
  return (kno_bigint_equal);
}

/* Addition */

static kno_bigint
DEFUN (bigint_add_unsigned, (x, y, negative_p),
       kno_bigint x AND kno_bigint y AND int negative_p)
{
  if ((BIGINT_LENGTH (y)) > (BIGINT_LENGTH (x)))
    {
      kno_bigint z = x;
      x = y;
      y = z;
    }
  {
    int x_length = (BIGINT_LENGTH (x));
    kno_bigint r = (bigint_allocate ((x_length + 1), negative_p));
    fast bigint_digit_type sum;
    fast bigint_digit_type carry = 0;
    fast bigint_digit_type * scan_x = (BIGINT_START_PTR (x));
    fast bigint_digit_type * scan_r = (BIGINT_START_PTR (r));
    {
      fast bigint_digit_type * scan_y = (BIGINT_START_PTR (y));
      fast bigint_digit_type * end_y = (scan_y + (BIGINT_LENGTH (y)));
      while (scan_y < end_y)
	{
	  sum = ((*scan_x++) + (*scan_y++) + carry);
	  if (sum < BIGINT_RADIX)
	    {
	      (*scan_r++) = sum;
	      carry = 0;
	    }
	  else
	    {
	      (*scan_r++) = (sum - BIGINT_RADIX);
	      carry = 1;
	    }
	}
    }
    {
      fast bigint_digit_type * end_x = ((BIGINT_START_PTR (x)) + x_length);
      if (carry != 0)
	while (scan_x < end_x)
	  {
	    sum = ((*scan_x++) + 1);
	    if (sum < BIGINT_RADIX)
	      {
		(*scan_r++) = sum;
		carry = 0;
		break;
	      }
	    else
	      (*scan_r++) = (sum - BIGINT_RADIX);
	  }
      while (scan_x < end_x)
	(*scan_r++) = (*scan_x++);
    }
    if (carry != 0)
      {
	(*scan_r) = 1;
	return (r);
      }
    return (bigint_shorten_length (r, x_length));
  }
}

/* Subtraction */

static kno_bigint
DEFUN (bigint_subtract_unsigned, (x, y),
       kno_bigint x AND kno_bigint y AND int make_negative)
{
  int negative_p = 0;
  switch (bigint_compare_unsigned (x, y))
    {
    case kno_bigint_equal:
      return (BIGINT_ZERO ());
    case kno_bigint_less:
      {
	kno_bigint z = x;
	x = y;
	y = z;
      }
      if (make_negative)
	negative_p = 0;
      else negative_p = 1;
      break;
    case kno_bigint_greater:
      if (make_negative)
	negative_p = 1;
      else negative_p = 0;
      break;
    }
  
  {
    int x_length = (BIGINT_LENGTH (x));
    kno_bigint r = (bigint_allocate (x_length, negative_p));
    fast bigint_digit_type difference;
    fast bigint_digit_type borrow = 0;
    fast bigint_digit_type * scan_x = (BIGINT_START_PTR (x));
    fast bigint_digit_type * scan_r = (BIGINT_START_PTR (r));
    {
      fast bigint_digit_type * scan_y = (BIGINT_START_PTR (y));
      fast bigint_digit_type * end_y = (scan_y + (BIGINT_LENGTH (y)));
      while (scan_y < end_y)
	{
	  difference = (((*scan_x++) - (*scan_y++)) - borrow);
	  if (difference < 0)
	    {
	      (*scan_r++) = (difference + BIGINT_RADIX);
	      borrow = 1;
	    }
	  else
	    {
	      (*scan_r++) = difference;
	      borrow = 0;
	    }
	}
    }
    {
      fast bigint_digit_type * end_x = (BIGINT_START_PTR (x))+ x_length;
      if (borrow != 0)
	while (scan_x < end_x)
	  {
	    difference = ((*scan_x++) - borrow);
	    if (difference < 0)
	      (*scan_r++) = (difference + BIGINT_RADIX);
	    else
	      {
		(*scan_r++) = difference;
		borrow = 0;
		break;
	      }
	  }
      BIGINT_ASSERT (borrow == 0);
      while (scan_x < end_x)
	(*scan_r++) = (*scan_x++);
    }
    return (bigint_trim (r));
  }
}

/* Multiplication
   Maximum value for product_low or product_high:
	((R * R) + (R * (R - 2)) + (R - 1))
   Maximum value for carry: ((R * (R - 1)) + (R - 1))
	where R == BIGINT_RADIX_ROOT */

static kno_bigint
DEFUN (bigint_multiply_unsigned, (x, y, negative_p),
       kno_bigint x AND kno_bigint y AND int negative_p)
{
  if ((BIGINT_LENGTH (y)) > (BIGINT_LENGTH (x)))
    {
      kno_bigint z = x;
      x = y;
      y = z;
    }
  {
    fast bigint_digit_type carry;
    fast bigint_digit_type y_digit_low;
    fast bigint_digit_type y_digit_high;
    fast bigint_digit_type x_digit_low;
    fast bigint_digit_type x_digit_high;
    bigint_digit_type product_low;
    fast bigint_digit_type * scan_r;
    fast bigint_digit_type * scan_y;
    int x_length = (BIGINT_LENGTH (x));
    int y_length = (BIGINT_LENGTH (y));
    kno_bigint r =
      (bigint_allocate_zeroed ((x_length + y_length), negative_p));
    bigint_digit_type * scan_x = (BIGINT_START_PTR (x));
    bigint_digit_type * end_x = (scan_x + x_length);
    bigint_digit_type * start_y = (BIGINT_START_PTR (y));
    bigint_digit_type * end_y = (start_y + y_length);
    bigint_digit_type * start_r = (BIGINT_START_PTR (r));
#define x_digit x_digit_high
#define y_digit y_digit_high
#define product_high carry
    while (scan_x < end_x)
      {
	x_digit = (*scan_x++);
	x_digit_low = (HD_LOW (x_digit));
	x_digit_high = (HD_HIGH (x_digit));
	carry = 0;
	scan_y = start_y;
	scan_r = (start_r++);
	while (scan_y < end_y)
	  {
	    y_digit = (*scan_y++);
	    y_digit_low = (HD_LOW (y_digit));
	    y_digit_high = (HD_HIGH (y_digit));
	    product_low =
	      ((*scan_r) +
	       (x_digit_low * y_digit_low) +
	       (HD_LOW (carry)));
	    product_high =
	      ((x_digit_high * y_digit_low) +
	       (x_digit_low * y_digit_high) +
	       (HD_HIGH (product_low)) +
	       (HD_HIGH (carry)));
	    (*scan_r++) =
	      (HD_CONS ((HD_LOW (product_high)), (HD_LOW (product_low))));
	    carry =
	      ((x_digit_high * y_digit_high) +
	       (HD_HIGH (product_high)));
	  }
	(*scan_r) += carry;
      }
    return (bigint_trim (r));
#undef x_digit
#undef y_digit
#undef product_high
  }
}

static kno_bigint
DEFUN (bigint_multiply_unsigned_small_factor, (x, y, negative_p),
       kno_bigint x AND bigint_digit_type y AND int negative_p)
{
  int length_x = (BIGINT_LENGTH (x));
  kno_bigint p = (bigint_allocate ((length_x + 1), negative_p));
  bigint_destructive_copy (x, p);
  (BIGINT_REF (p, length_x)) = 0;
  bigint_destructive_scale_up (p, y);
  return (bigint_trim (p));
}

static void
DEFUN (bigint_destructive_scale_up, (bigint, factor),
       kno_bigint bigint AND bigint_digit_type factor)
{
  fast bigint_digit_type carry = 0;
  fast bigint_digit_type * scan = (BIGINT_START_PTR (bigint));
  fast bigint_digit_type two_digits;
  fast bigint_digit_type product_low;
#define product_high carry
  bigint_digit_type * end = (scan + (BIGINT_LENGTH (bigint)));
  BIGINT_ASSERT ((factor > 1) && (factor < BIGINT_RADIX_ROOT));
  while (scan < end)
    {
      two_digits = (*scan);
      product_low = ((factor * (HD_LOW (two_digits))) + (HD_LOW (carry)));
      product_high =
	((factor * (HD_HIGH (two_digits))) +
	 (HD_HIGH (product_low)) +
	 (HD_HIGH (carry)));
      (*scan++) = (HD_CONS ((HD_LOW (product_high)), (HD_LOW (product_low))));
      carry = (HD_HIGH (product_high));
    }
  /* A carry here would be an overflow, i.e. it would not fit.
     Hopefully the callers allocate enough space that this will
     never happen.
   */
  BIGINT_ASSERT (carry == 0);
  return;
#undef product_high
}

static void
DEFUN (bigint_destructive_add, (bigint, n),
       kno_bigint bigint AND bigint_digit_type n)
{
  fast bigint_digit_type * scan = (BIGINT_START_PTR (bigint));
  fast bigint_digit_type digit;
  digit = ((*scan) + n);
  if (digit < BIGINT_RADIX)
    {
      (*scan) = digit;
      return;
    }
  (*scan++) = (digit - BIGINT_RADIX);
  while (1)
    {
      digit = ((*scan) + 1);
      if (digit < BIGINT_RADIX)
	{
	  (*scan) = digit;
	  return;
	}
      (*scan++) = (digit - BIGINT_RADIX);
    }
}

/* Division */

/* For help understanding this algorithm, see:
   Knuth, Donald E., "The Art of Computer Programming",
   volume 2, "Seminumerical Algorithms"
   section 4.3.1, "Multiple-Precision Arithmetic". */

static void
DEFUN (bigint_divide_unsigned_large_denominator, (numerator, denominator,
						  quotient, remainder,
						  q_negative_p, r_negative_p),
       kno_bigint numerator
       AND kno_bigint denominator
       AND kno_bigint * quotient
       AND kno_bigint * remainder
       AND int q_negative_p
       AND int r_negative_p)
{
  int length_n = ((BIGINT_LENGTH (numerator)) + 1);
  int length_d = (BIGINT_LENGTH (denominator));
  kno_bigint q =
    ((quotient != ((kno_bigint *) 0))
     ? (bigint_allocate ((length_n - length_d), q_negative_p))
     : BIGINT_OUT_OF_BAND);
  kno_bigint u = (bigint_allocate (length_n, r_negative_p));
  int shift = 0;
  BIGINT_ASSERT (length_d > 1);
  {
    fast bigint_digit_type v1 = (BIGINT_REF ((denominator), (length_d - 1)));
    while (v1 < (BIGINT_RADIX / 2))
      {
	v1 <<= 1;
	shift += 1;
      }
  }
  if (shift == 0)
    {
      bigint_destructive_copy (numerator, u);
      (BIGINT_REF (u, (length_n - 1))) = 0;
      bigint_divide_unsigned_normalized (u, denominator, q);
    }
  else
    {
      kno_bigint v = (bigint_allocate (length_d, 0));
      bigint_destructive_normalization (numerator, u, shift);
      bigint_destructive_normalization (denominator, v, shift);
      bigint_divide_unsigned_normalized (u, v, q);
      BIGINT_DEALLOCATE (v);
      if (remainder != ((kno_bigint *) 0))
	bigint_destructive_unnormalization (u, shift);
    }
  if (quotient != ((kno_bigint *) 0))
    (*quotient) = (bigint_trim (q));
  if (remainder != ((kno_bigint *) 0))
    (*remainder) = (bigint_trim (u));
  else
    BIGINT_DEALLOCATE (u);
  return;
}

static void
DEFUN (bigint_divide_unsigned_normalized, (u, v, q),
       kno_bigint u AND kno_bigint v AND kno_bigint q)
{
  int u_length = (BIGINT_LENGTH (u));
  int v_length = (BIGINT_LENGTH (v));
  bigint_digit_type * u_start = (BIGINT_START_PTR (u));
  bigint_digit_type * u_scan = (u_start + u_length);
  bigint_digit_type * u_scan_limit = (u_start + v_length);
  bigint_digit_type * u_scan_start = (u_scan - v_length);
  bigint_digit_type * v_start = (BIGINT_START_PTR (v));
  bigint_digit_type * v_end = (v_start + v_length);
  bigint_digit_type * q_scan = ((BIGINT_START_PTR (q)) + (BIGINT_LENGTH (q)));
  bigint_digit_type v1 = (v_end[-1]);
  bigint_digit_type v2 = (v_end[-2]);
  fast bigint_digit_type ph;	/* high half of double-digit product */
  fast bigint_digit_type pl;	/* low half of double-digit product */
  fast bigint_digit_type guess;
  fast bigint_digit_type gh;	/* high half-digit of guess */
  fast bigint_digit_type ch;	/* high half of double-digit comparand */
  fast bigint_digit_type v2l = (HD_LOW (v2));
  fast bigint_digit_type v2h = (HD_HIGH (v2));
  fast bigint_digit_type cl;	/* low half of double-digit comparand */
#define gl ph			/* low half-digit of guess */
#define uj pl
#define qj ph
  bigint_digit_type gm;		/* memory loc for reference parameter */
  if (q != BIGINT_OUT_OF_BAND)
    q_scan = ((BIGINT_START_PTR (q)) + (BIGINT_LENGTH (q)));
  while (u_scan_limit < u_scan)
    {
      uj = (*--u_scan);
      if (uj != v1)
	{
	  /* comparand =
	     (((((uj * BIGINT_RADIX) + uj1) % v1) * BIGINT_RADIX) + uj2);
	     guess = (((uj * BIGINT_RADIX) + uj1) / v1); */
	  cl = (u_scan[-2]);
	  ch = (bigint_digit_divide (uj, (u_scan[-1]), v1, (&gm)));
	  guess = gm;
	}
      else
	{
	  cl = (u_scan[-2]);
	  ch = ((u_scan[-1]) + v1);
	  guess = (BIGINT_RADIX - 1);
	}
      while (1)
	{
	  /* product = (guess * v2); */
	  gl = (HD_LOW (guess));
	  gh = (HD_HIGH (guess));
	  pl = (v2l * gl);
	  ph = ((v2l * gh) + (v2h * gl) + (HD_HIGH (pl)));
	  pl = (HD_CONS ((HD_LOW (ph)), (HD_LOW (pl))));
	  ph = ((v2h * gh) + (HD_HIGH (ph)));
	  /* if (comparand >= product) */
	  if ((ch > ph) || ((ch == ph) && (cl >= pl)))
	    break;
	  guess -= 1;
	  /* comparand += (v1 << BIGINT_DIGIT_LENGTH) */
	  ch += v1;
	  /* if (comparand >= (BIGINT_RADIX * BIGINT_RADIX)) */
	  if (ch >= BIGINT_RADIX)
	    break;
	}
      qj = (bigint_divide_subtract (v_start, v_end, guess, (--u_scan_start)));
      if (q != BIGINT_OUT_OF_BAND)
	(*--q_scan) = qj;
    }
  return;
#undef gl
#undef uj
#undef qj
}

static bigint_digit_type
DEFUN (bigint_divide_subtract, (v_start, v_end, guess, u_start),
       bigint_digit_type * v_start
       AND bigint_digit_type * v_end
       AND bigint_digit_type guess
       AND bigint_digit_type * u_start)
{
  bigint_digit_type * v_scan = v_start;
  bigint_digit_type * u_scan = u_start;
  fast bigint_digit_type carry = 0;
  if (guess == 0) return (0);
  {
    bigint_digit_type gl = (HD_LOW (guess));
    bigint_digit_type gh = (HD_HIGH (guess));
    fast bigint_digit_type v;
    fast bigint_digit_type pl;
    fast bigint_digit_type vl;
#define vh v
#define ph carry
#define diff pl
    while (v_scan < v_end)
      {
	v = (*v_scan++);
	vl = (HD_LOW (v));
	vh = (HD_HIGH (v));
	pl = ((vl * gl) + (HD_LOW (carry)));
	ph = ((vl * gh) + (vh * gl) + (HD_HIGH (pl)) + (HD_HIGH (carry)));
	diff = ((*u_scan) - (HD_CONS ((HD_LOW (ph)), (HD_LOW (pl)))));
	if (diff < 0)
	  {
	    (*u_scan++) = (diff + BIGINT_RADIX);
	    carry = ((vh * gh) + (HD_HIGH (ph)) + 1);
	  }
	else
	  {
	    (*u_scan++) = diff;
	    carry = ((vh * gh) + (HD_HIGH (ph)));
	  }
      }
    if (carry == 0)
      return (guess);
    diff = ((*u_scan) - carry);
    if (diff < 0)
      (*u_scan) = (diff + BIGINT_RADIX);
    else
      {
	(*u_scan) = diff;
	return (guess);
      }
#undef vh
#undef ph
#undef diff
  }
  /* Subtraction generated carry, implying guess is one too large.
     Add v back in to bring it back down. */
  v_scan = v_start;
  u_scan = u_start;
  carry = 0;
  while (v_scan < v_end)
    {
      bigint_digit_type sum = ((*v_scan++) + (*u_scan) + carry);
      if (sum < BIGINT_RADIX)
	{
	  (*u_scan++) = sum;
	  carry = 0;
	}
      else
	{
	  (*u_scan++) = (sum - BIGINT_RADIX);
	  carry = 1;
	}
    }
  if (carry == 1)
    {
      bigint_digit_type sum = ((*u_scan) + carry);
      (*u_scan) = ((sum < BIGINT_RADIX) ? sum : (sum - BIGINT_RADIX));
    }
  return (guess - 1);
}

static void
DEFUN (bigint_divide_unsigned_medium_denominator, (numerator, denominator,
						   quotient, remainder,
						   q_negative_p, r_negative_p),
       kno_bigint numerator
       AND bigint_digit_type denominator
       AND kno_bigint * quotient
       AND kno_bigint * remainder
       AND int q_negative_p
       AND int r_negative_p)
{
  int length_n = (BIGINT_LENGTH (numerator));
  int length_q;
  kno_bigint q;
  int shift = 0;
  /* Because `bigint_digit_divide' requires a normalized denominator. */
  while (denominator < (BIGINT_RADIX / 2))
    {
      denominator <<= 1;
      shift += 1;
    }
  if (shift == 0)
    {
      length_q = length_n;
      q = (bigint_allocate (length_q, q_negative_p));
      bigint_destructive_copy (numerator, q);
    }
  else
    {
      length_q = (length_n + 1);
      q = (bigint_allocate (length_q, q_negative_p));
      bigint_destructive_normalization (numerator, q, shift);
    }
  {
    fast bigint_digit_type r = 0;
    fast bigint_digit_type * start = (BIGINT_START_PTR (q));
    fast bigint_digit_type * scan = (start + length_q);
    bigint_digit_type qj;
    if (quotient != ((kno_bigint *) 0))
      {
	while (start < scan)
	  {
	    r = (bigint_digit_divide (r, (*--scan), denominator, (&qj)));
	    (*scan) = qj;
	  }
	(*quotient) = (bigint_trim (q));
      }
    else
      {
	while (start < scan)
	  r = (bigint_digit_divide (r, (*--scan), denominator, (&qj)));
	BIGINT_DEALLOCATE (q);
      }
    if (remainder != ((kno_bigint *) 0))
      {
	if (shift != 0)
	  r >>= shift;
	(*remainder) = (bigint_digit_to_bigint (r, r_negative_p));
      }
  }
  return;
}

static void
DEFUN (bigint_destructive_normalization, (source, target, shift_left),
       kno_bigint source AND kno_bigint target AND int shift_left)
{
  fast bigint_digit_type digit;
  fast bigint_digit_type * scan_source = (BIGINT_START_PTR (source));
  fast bigint_digit_type carry = 0;
  fast bigint_digit_type * scan_target = (BIGINT_START_PTR (target));
  bigint_digit_type * end_source = (scan_source + (BIGINT_LENGTH (source)));
  bigint_digit_type * end_target = (scan_target + (BIGINT_LENGTH (target)));
  int shift_right = (BIGINT_DIGIT_LENGTH - shift_left);
  bigint_digit_type mask = ((1L << shift_right) - 1);
  while (scan_source < end_source)
    {
      digit = (*scan_source++);
      (*scan_target++) = (((digit & mask) << shift_left) | carry);
      carry = (digit >> shift_right);
    }
  if (scan_target < end_target)
    (*scan_target) = carry;
  else
    BIGINT_ASSERT (carry == 0);
  return;
}

static void
DEFUN (bigint_destructive_unnormalization, (bigint, shift_right),
       kno_bigint bigint AND int shift_right)
{
  bigint_digit_type * start = (BIGINT_START_PTR (bigint));
  fast bigint_digit_type * scan = (start + (BIGINT_LENGTH (bigint)));
  fast bigint_digit_type digit;
  fast bigint_digit_type carry = 0;
  int shift_left = (BIGINT_DIGIT_LENGTH - shift_right);
  bigint_digit_type mask = ((1L << shift_right) - 1);
  while (start < scan)
    {
      digit = (*--scan);
      (*scan) = ((digit >> shift_right) | carry);
      carry = ((digit & mask) << shift_left);
    }
  BIGINT_ASSERT (carry == 0);
  return;
}

/* This is a reduced version of the division algorithm, applied to the
   case of dividing two bigint digits by one bigint digit.  It is
   assumed that the numerator and denominator are normalized. */

#define BDD_STEP(qn, j)							\
{									\
  uj = (u[j]);								\
  if (uj != v1)								\
    {									\
      uj_uj1 = (HD_CONS (uj, (u[j + 1])));				\
      guess = (uj_uj1 / v1);						\
      comparand = (HD_CONS ((uj_uj1 % v1), (u[j + 2])));		\
    }									\
  else									\
    {									\
      guess = (BIGINT_RADIX_ROOT - 1);					\
      comparand = (HD_CONS (((u[j + 1]) + v1), (u[j + 2])));		\
    }									\
  while ((guess * v2) > comparand)					\
    {									\
      guess -= 1;							\
      comparand += (v1 << BIGINT_HALF_DIGIT_LENGTH);			\
      if (comparand >= BIGINT_RADIX)					\
	break;								\
    }									\
  qn = (bigint_digit_divide_subtract (v1, v2, guess, (&u[j])));		\
}

static bigint_digit_type
DEFUN (bigint_digit_divide, (uh, ul, v, q),
       bigint_digit_type uh AND bigint_digit_type ul
       AND bigint_digit_type v AND bigint_digit_type * q) /* return value */
{
  fast bigint_digit_type guess;
  fast bigint_digit_type comparand;
  fast bigint_digit_type v1 = (HD_HIGH (v));
  fast bigint_digit_type v2 = (HD_LOW (v));
  fast bigint_digit_type uj;
  fast bigint_digit_type uj_uj1;
  bigint_digit_type q1;
  bigint_digit_type q2;
  bigint_digit_type u [4];
  if (uh == 0)
    {
      if (ul < v)
	{
	  (*q) = 0;
	  return (ul);
	}
      else if (ul == v)
	{
	  (*q) = 1;
	  return (0);
	}
    }
  (u[0]) = (HD_HIGH (uh));
  (u[1]) = (HD_LOW (uh));
  (u[2]) = (HD_HIGH (ul));
  (u[3]) = (HD_LOW (ul));
  v1 = (HD_HIGH (v));
  v2 = (HD_LOW (v));
  BDD_STEP (q1, 0);
  BDD_STEP (q2, 1);
  (*q) = (HD_CONS (q1, q2));
  return (HD_CONS ((u[2]), (u[3])));
}

#undef BDD_STEP

#define BDDS_MULSUB(vn, un, carry_in)					\
{									\
  product = ((vn * guess) + carry_in);					\
  diff = (un - (HD_LOW (product)));					\
  if (diff < 0)								\
    {									\
      un = (diff + BIGINT_RADIX_ROOT);					\
      carry = ((HD_HIGH (product)) + 1);				\
    }									\
  else									\
    {									\
      un = diff;							\
      carry = (HD_HIGH (product));					\
    }									\
}

#define BDDS_ADD(vn, un, carry_in)					\
{									\
  sum = (vn + un + carry_in);						\
  if (sum < BIGINT_RADIX_ROOT)						\
    {									\
      un = sum;								\
      carry = 0;							\
    }									\
  else									\
    {									\
      un = (sum - BIGINT_RADIX_ROOT);					\
      carry = 1;							\
    }									\
}

static bigint_digit_type
DEFUN (bigint_digit_divide_subtract, (v1, v2, guess, u),
       bigint_digit_type v1 AND bigint_digit_type v2
       AND bigint_digit_type guess AND bigint_digit_type * u)
{
  {
    fast bigint_digit_type product;
    fast bigint_digit_type diff;
    fast bigint_digit_type carry;
    BDDS_MULSUB (v2, (u[2]), 0);
    BDDS_MULSUB (v1, (u[1]), carry);
    if (carry == 0)
      return (guess);
    diff = ((u[0]) - carry);
    if (diff < 0)
      (u[0]) = (diff + BIGINT_RADIX);
    else
      {
	(u[0]) = diff;
	return (guess);
      }
  }
  {
    fast bigint_digit_type sum;
    fast bigint_digit_type carry;
    BDDS_ADD(v2, (u[2]), 0);
    BDDS_ADD(v1, (u[1]), carry);
    if (carry == 1)
      (u[0]) += 1;
  }
  return (guess - 1);
}

#undef BDDS_MULSUB
#undef BDDS_ADD

static void
DEFUN (bigint_divide_unsigned_small_denominator, (numerator, denominator,
						  quotient, remainder,
						  q_negative_p, r_negative_p),
       kno_bigint numerator
       AND bigint_digit_type denominator
       AND kno_bigint * quotient
       AND kno_bigint * remainder
       AND int q_negative_p
       AND int r_negative_p)
{
  kno_bigint q = (bigint_new_sign (numerator, q_negative_p));
  bigint_digit_type r = (bigint_destructive_scale_down (q, denominator));
  (*quotient) = (bigint_trim (q));
  if (remainder != ((kno_bigint *) 0))
    (*remainder) = (bigint_digit_to_bigint (r, r_negative_p));
  return;
}

/* Given (denominator > 1), it is fairly easy to show that
   (quotient_high < BIGINT_RADIX_ROOT), after which it is easy to see
   that all digits are < BIGINT_RADIX. */

static bigint_digit_type
DEFUN (bigint_destructive_scale_down, (bigint, denominator),
       kno_bigint bigint AND fast bigint_digit_type denominator)
{
  fast bigint_digit_type numerator;
  fast bigint_digit_type remainder = 0;
  fast bigint_digit_type two_digits;
#define quotient_high remainder
  bigint_digit_type * start = (BIGINT_START_PTR (bigint));
  bigint_digit_type * scan = (start + (BIGINT_LENGTH (bigint)));
  BIGINT_ASSERT ((denominator > 1) && (denominator < BIGINT_RADIX_ROOT));
  while (start < scan)
    {
      two_digits = (*--scan);
      numerator = (HD_CONS (remainder, (HD_HIGH (two_digits))));
      quotient_high = (numerator / denominator);
      numerator = (HD_CONS ((numerator % denominator), (HD_LOW (two_digits))));
      (*scan) = (HD_CONS (quotient_high, (numerator / denominator)));
      remainder = (numerator % denominator);
    }
  return (remainder);
#undef quotient_high
}

static kno_bigint
DEFUN (bigint_remainder_unsigned_small_denominator, (n, d, negative_p),
       kno_bigint n AND bigint_digit_type d AND int negative_p)
{
  fast bigint_digit_type two_digits;
  bigint_digit_type * start = (BIGINT_START_PTR (n));
  fast bigint_digit_type * scan = (start + (BIGINT_LENGTH (n)));
  fast bigint_digit_type r = 0;
  BIGINT_ASSERT ((d > 1) && (d < BIGINT_RADIX_ROOT));
  while (start < scan)
    {
      two_digits = (*--scan);
      r =
	((HD_CONS (((HD_CONS (r, (HD_HIGH (two_digits)))) % d),
		   (HD_LOW (two_digits))))
	 % d);
    }
  return (bigint_digit_to_bigint (r, negative_p));
}

static kno_bigint
DEFUN (bigint_digit_to_bigint, (digit, negative_p),
       fast bigint_digit_type digit AND int negative_p)
{
  if (digit == 0)
    return (BIGINT_ZERO ());
  else
    {
      fast kno_bigint result = (bigint_allocate (1, negative_p));
      (BIGINT_REF (result, 0)) = digit;
      return (result);
    }
}

/* Allocation */

static kno_bigint
DEFUN (bigint_allocate, (kno_veclen, negative_p),
       fast int length AND int negative_p)
{
  BIGINT_ASSERT ((length >= 0) || (length < BIGINT_RADIX));
  {
    fast kno_bigint result = (BIGINT_ALLOCATE (length));
    BIGINT_SET_HEADER (result, length, negative_p);
    return (result);
  }
}

static kno_bigint
DEFUN (bigint_allocate_zeroed, (kno_veclen, negative_p),
       fast int length AND int negative_p)
{
  BIGINT_ASSERT ((length >= 0) || (length < BIGINT_RADIX));
  {
    fast kno_bigint result = (BIGINT_ALLOCATE (length));
    fast bigint_digit_type * scan = (BIGINT_START_PTR (result));
    fast bigint_digit_type * end = (scan + length);
    BIGINT_SET_HEADER (result, length, negative_p);
    while (scan < end)
      (*scan++) = 0;
    return (result);
  }
}

static kno_bigint
DEFUN (bigint_shorten_length, (bigint, kno_veclen),
       fast kno_bigint bigint AND fast int length)
{
  fast int current_length = (BIGINT_LENGTH (bigint));
  BIGINT_ASSERT ((length >= 0) || (length <= current_length));
  if (length < current_length)
    {
      BIGINT_SET_HEADER
	(bigint, length, ((length != 0) && (BIGINT_NEGATIVE_P (bigint))));
      BIGINT_REDUCE_LENGTH (bigint, bigint, length);
    }
  return (bigint);
}

static kno_bigint
DEFUN (bigint_trim, (bigint), kno_bigint bigint)
{
  fast bigint_digit_type * start = (BIGINT_START_PTR (bigint));
  fast bigint_digit_type * end = (start + (BIGINT_LENGTH (bigint)));
  fast bigint_digit_type * scan = end;
  while ((start <= scan) && ((*--scan) == 0))
    ;
  scan += 1;
  if (scan < end)
    {
      fast int length = (scan - start);
      BIGINT_SET_HEADER
	(bigint, length, ((length != 0) && (BIGINT_NEGATIVE_P (bigint))));
      BIGINT_REDUCE_LENGTH (bigint, bigint, length);
    }
  return (bigint);
}

/* Copying */

static kno_bigint
DEFUN (bigint_copy, (source), fast kno_bigint source)
{
  fast kno_bigint target =
    (bigint_allocate ((BIGINT_LENGTH (source)), (BIGINT_NEGATIVE_P (source))));
  bigint_destructive_copy (source, target);
  return (target);
}

static kno_bigint
DEFUN (bigint_new_sign, (bigint, negative_p),
       fast kno_bigint bigint AND int negative_p)
{
  fast kno_bigint result =
    (bigint_allocate ((BIGINT_LENGTH (bigint)), negative_p));
  bigint_destructive_copy (bigint, result);
  return (result);
}

static kno_bigint
DEFUN (bigint_maybe_new_sign, (bigint, negative_p),
       fast kno_bigint bigint AND int negative_p)
{
#ifndef BIGINT_FORCE_NEW_RESULTS
  if ((BIGINT_NEGATIVE_P (bigint)) ? negative_p : (! negative_p))
    return (bigint);
  else
#endif /* not BIGINT_FORCE_NEW_RESULTS */
    {
      fast kno_bigint result =
	(bigint_allocate ((BIGINT_LENGTH (bigint)), negative_p));
      bigint_destructive_copy (bigint, result);
      return (result);
    }
}

static void
DEFUN (bigint_destructive_copy, (source, target),
       kno_bigint source AND kno_bigint target)
{
  fast bigint_digit_type * scan_source = (BIGINT_START_PTR (source));
  fast bigint_digit_type * end_source =
    (scan_source + (BIGINT_LENGTH (source)));
  fast bigint_digit_type * scan_target = (BIGINT_START_PTR (target));
  while (scan_source < end_source)
    (*scan_target++) = (*scan_source++);
  return;
}

/* Cheap big ints */

KNO_EXPORT lispval kno_make_bigint(long long intval)
{
  if ((intval>KNO_MAX_FIXNUM) || (intval<KNO_MIN_FIXNUM))
    return (lispval) kno_long_long_to_bigint(intval);
  else if (intval>=0)
    return (kno_fixnum_type|(intval*4));
  else
    return (kno_fixnum_type|(intval*4));
}

static lispval copy_bigint(lispval x,int deep)
{
  kno_bigint bi = kno_consptr(kno_bigint,x,kno_bigint_type);
  return LISP_CONS(bigint_copy(bi));
}

static void recycle_bigint(struct KNO_RAW_CONS *c)
{
  if (KNO_MALLOCD_CONSP(c)) {
    u8_free(c);}
}

/* Flonums */

KNO_EXPORT lispval kno_init_flonum(struct KNO_FLONUM *ptr,double flonum)
{
  if (ptr == NULL) ptr = u8_alloc(struct KNO_FLONUM);
  KNO_INIT_CONS(ptr,kno_flonum_type);
  ptr->floval = flonum;
  return LISP_CONS(ptr);
}

KNO_EXPORT lispval kno_init_double(struct KNO_FLONUM *ptr,double flonum)
{
  if (ptr == NULL) ptr = u8_alloc(struct KNO_FLONUM);
  KNO_INIT_CONS(ptr,kno_flonum_type);
  ptr->floval = flonum;
  return LISP_CONS(ptr);
}

static int unparse_flonum(struct U8_OUTPUT *out,lispval x)
{
  unsigned char buf[256]; int exp;
  struct KNO_FLONUM *d = kno_consptr(struct KNO_FLONUM *,x,kno_flonum_type);
  /* Get the exponent */
  frexp(d->floval,&exp);
  if ((exp<-10) || (exp>20))
    sprintf(buf,"%e",d->floval);
  else sprintf(buf,"%f",d->floval);
  u8_puts(out,buf);
  return 1;
}

static lispval copy_flonum(lispval x,int deep)
{
  struct KNO_FLONUM *d = kno_consptr(struct KNO_FLONUM *,x,kno_flonum_type);
  return kno_init_flonum(NULL,d->floval);
}

static lispval unpack_flonum(ssize_t n,unsigned char *packet)
{
  unsigned char bytes[8]; double *f = (double *)&bytes;
#if WORDS_BIGENDIAN
  memcpy(bytes,packet,8);
#else
  int i = 0; while (i < 8) {bytes[i]=packet[7-i]; i++;}
#endif
  u8_free(packet);
  return kno_init_flonum(NULL,*f);
}

static ssize_t write_flonum_dtype(struct KNO_OUTBUF *out,lispval x)
{
  struct KNO_FLONUM *d = kno_consptr(struct KNO_FLONUM *,x,kno_flonum_type);
  unsigned char bytes[8]; int i = 0;
  double *f = (double *)&bytes;
  *f = d->floval;
  kno_write_byte(out,dt_numeric_package);
  kno_write_byte(out,dt_double);
  kno_write_byte(out,8);
#if WORDS_BIGENDIAN
  while (i<8) {kno_write_byte(out,bytes[i]); i++;}
#else
  i = 7; while (i>=0) {kno_write_byte(out,bytes[i]); i--;}
#endif
  return 11;
}

static void recycle_flonum(struct KNO_RAW_CONS *c)
{
  if (KNO_MALLOCD_CONSP(c)) u8_free(c);
}

static int compare_flonum(lispval x,lispval y,kno_compare_flags flags)
{
  struct KNO_FLONUM *dx=
    kno_consptr(struct KNO_FLONUM *,x,kno_flonum_type);
  struct KNO_FLONUM *dy=
    kno_consptr(struct KNO_FLONUM *,y,kno_flonum_type);
  if (dx->floval < dy->floval) return -1;
  else if (dx->floval == dy->floval) return 0;
  else return 1;
}

static int hash_flonum(lispval x,unsigned int (*fn)(lispval))
{
  struct KNO_FLONUM *dx=
    kno_consptr(struct KNO_FLONUM *,x,kno_flonum_type);
  int expt;
  double mantissa = frexp(fabs(dx->floval),&expt);
  double reformed=
    ((expt<0) ? (ldexp(mantissa,0)) : (ldexp(mantissa,expt)));
  unsigned int asint = (unsigned int)reformed;
  return asint%256001281;
}

/* Utility fucntions and macros. */


#define COMPLEXP(x) (TYPEP((x),kno_complex_type))
#define REALPART(x) ((COMPLEXP(x)) ? (KNO_REALPART(x)) : (x))
#define IMAGPART(x) ((COMPLEXP(x)) ? (KNO_IMAGPART(x)) : (KNO_INT(0)))

#define RATIONALP(x) (TYPEP((x),kno_rational_type))
#define NUMERATOR(x) ((RATIONALP(x)) ? (KNO_NUMERATOR(x)) : (x))
#define DENOMINATOR(x) ((RATIONALP(x)) ? (KNO_DENOMINATOR(x)) : (KNO_INT(1)))

#define INTEGERP(x) ((FIXNUMP(x)) || (KNO_BIGINTP(x)))

static double todoublex(lispval x,kno_ptr_type xt)
{
  if (xt == kno_flonum_type) return ((struct KNO_FLONUM *)x)->floval;
  else if (xt == kno_fixnum_type)
    return (double) (FIX2INT(x));
  else if (xt == kno_bigint_type)
    return (double) kno_bigint_to_double((kno_bigint)x);
  else if (xt == kno_rational_type) {
    double num = todouble(NUMERATOR(x));
    double den = todouble(DENOMINATOR(x));
    return num/den;}
  else if (xt == kno_complex_type) {
    double real = todouble(KNO_REALPART(x)), imag = todouble(KNO_IMAGPART(x));
    return sqrt((real*real)+(imag*imag));}
  else return FP_NAN;
}
static double todouble(lispval x)
{
  return todoublex(x,KNO_PTR_TYPE(x));
}
KNO_EXPORT double kno_todouble(lispval x)
{
  return todoublex(x,KNO_PTR_TYPE(x));
}
static kno_bigint tobigint(lispval x)
{
  if (FIXNUMP(x))
    return kno_long_long_to_bigint(FIX2INT(x));
  else if (KNO_BIGINTP(x))
    return (kno_bigint)x;
  else if (KNO_FLONUMP(x))
    return kno_double_to_bigint(KNO_FLONUM(x));
  else {
    u8_raise(_("Internal error"),"tobigint","numeric");
    return NULL;}
}
static lispval simplify_bigint(kno_bigint bi)
{
  if (kno_bigint_fits_in_word_p(bi,KNO_FIXNUM_BITS,1)) {
    long long intval = kno_bigint_to_long(bi);
    kno_decref((lispval)bi);
    return KNO_INT(intval);}
  else return (lispval)bi;
}

/* Making complex and rational numbers */

static lispval make_complex(lispval real,lispval imag)
{
  if (kno_numcompare(imag,KNO_INT(0))==0) {
    kno_decref(imag); return real;}
  else {
    struct KNO_COMPLEX *result = u8_alloc(struct KNO_COMPLEX);
    KNO_INIT_CONS(result,kno_complex_type);
    result->realpart = real; result->imagpart = imag;
    return (lispval) result;}
}

static int unparse_complex(struct U8_OUTPUT *out,lispval x)
{
  lispval imag = KNO_IMAGPART(x), real = KNO_REALPART(x);
  int has_real = kno_numcompare(real,KNO_INT(0));
  if (kno_numcompare(imag,KNO_INT(0))<0) {
    lispval negated = kno_subtract(KNO_INT(0),imag);
    if (has_real)
      u8_printf(out,"%q-%qi",KNO_REALPART(x),negated);
    else u8_printf(out,"-%qi",negated);
    kno_decref(negated);}
  else if (has_real)
    u8_printf(out,"%q+%qi",KNO_REALPART(x),KNO_IMAGPART(x));
  else u8_printf(out,"+%qi",KNO_IMAGPART(x));
  return 1;
}

KNO_EXPORT
lispval kno_make_complex(lispval real,lispval imag)
{
  kno_incref(real); kno_incref(imag);
  return make_complex(real,imag);
}

static long long fix_gcd (long long x, long long y);
static lispval int_gcd(lispval x,lispval y);
static lispval int_lcm (lispval x, lispval y);

static lispval make_rational(lispval num,lispval denom)
{
  struct KNO_RATIONAL *result;
  if ((FIXNUMP(denom)) && ((FIX2INT(denom))==0))
    return kno_err(kno_DivideByZero,"make_rational",NULL,num);
  else if ((FIXNUMP(num)) && (FIXNUMP(denom))) {
    long long inum = FIX2INT(num), iden = FIX2INT(denom);
    long long igcd = fix_gcd(inum,iden);
    inum = inum/igcd; iden = iden/igcd;
    if (iden == 1) return KNO_INT(inum);
    else if (iden < 0) {
      num = KNO_INT(-inum); denom = KNO_INT(-iden);}
    else {num = KNO_INT(inum); denom = KNO_INT(iden);}}
  else if ((INTEGERP(num)) && (INTEGERP(denom))) {
    lispval gcd = int_gcd(num,denom);
    lispval new_num = kno_quotient(num,gcd);
    lispval new_denom = kno_quotient(denom,gcd);
    kno_decref(gcd);
    if (((FIXNUMP(new_denom)) && (FIX2INT(new_denom) == 1)))
      return new_num;
    else {num = new_num; denom = new_denom;}}
  else return kno_err(_("Non integral components"),"kno_make_rational",NULL,num);
  result = u8_alloc(struct KNO_RATIONAL);
  KNO_INIT_CONS(result,kno_rational_type);
  result->numerator = num;
  result->denominator = denom;
  return (lispval) result;
}

static int unparse_rational(struct U8_OUTPUT *out,lispval x)
{
  u8_printf(out,"%q/%q",KNO_NUMERATOR(x),KNO_DENOMINATOR(x));
  return 1;
}

KNO_EXPORT
lispval kno_make_rational(lispval num,lispval denom)
{
  kno_incref(num); kno_incref(denom);
  return make_rational(num,denom);
}

static long long fix_gcd (long long x, long long y)
{
  long long a;
  if (x < 0) x = -x; else {};
  if (y < 0) y = -y; else {};
  a = y; while (a != 0) {
    y = a; a = x % a; x = y; }
  return x;
}

#define INT_POSITIVEP(x) \
  ((FIXNUMP(x)) ? ((FIX2INT(x)>0)) : \
   (KNO_BIGINTP(x)) ? ((bigint_test((kno_bigint)x)) == kno_bigint_greater) : (0))
#define INT_NEGATIVEP(x) \
  ((FIXNUMP(x)) ? ((FIX2INT(x)<0)) : \
   (KNO_BIGINTP(x)) ? ((bigint_test((kno_bigint)x)) == kno_bigint_less) : (0))
#define INT_ZEROP(x) \
  ((FIXNUMP(x)) ? ((FIX2INT(x)==0)) : \
   (KNO_BIGINTP(x)) ? ((bigint_test((kno_bigint)x)) == kno_bigint_equal) : (0))

static lispval int_gcd(lispval x,lispval y)
{
  errno = 0;
  if ((FIXNUMP(x)) && (FIXNUMP(y)))
    return KNO_INT(fix_gcd(FIX2INT(x),FIX2INT(y)));
  else if ((INTEGERP(x)) && (INTEGERP(y))) {
    lispval a; kno_incref(x); kno_incref(y);
    /* Normalize the sign of x */
    if (FIXNUMP(x)) {
      long long ival = FIX2INT(x);
      if (ival<0) x = KNO_INT(-ival);}
    else if (INT_NEGATIVEP(x)) {
      lispval bval = kno_subtract(KNO_INT(0),x);
      kno_decref(x); x = bval;}
    else {}
    /* Normalize the sign of y */
    if (FIXNUMP(y)) {
      long long ival = FIX2INT(y);
      if (ival<0) y = KNO_INT(-ival);}
    else if (INT_NEGATIVEP(y)) {
      lispval bval = kno_subtract(KNO_INT(0),y);
      kno_decref(y); y = bval;}
    else {}
    a = y;
    while (!(INT_ZEROP(a))) {
      y = a; a = kno_remainder(x,a); kno_decref(x); x = y;}
    kno_decref(a);
    return x;}
  else return kno_type_error(_("not an integer"),"int_gcd",y);
}

static lispval int_lcm (lispval x, lispval y)
{
  lispval prod = kno_multiply(x,y), gcd = int_gcd(x,y), lcm;
  if (FIXNUMP(prod))
    if (FIX2INT(prod) < 0) prod = KNO_INT(-(FIX2INT(prod)));
    else {}
  else if (INT_NEGATIVEP(prod)) {
    lispval negated = kno_subtract(KNO_INT(0),prod);
    kno_decref(prod); prod = negated;}
  lcm = kno_divide(prod,gcd);
  kno_decref(prod); kno_decref(gcd);
  return lcm;
}

/* Arithmetic operations */

KNO_EXPORT int kno_small_bigintp(kno_bigint bi)
{
  return (kno_bigint_fits_in_word_p(bi,32,1));
}

KNO_EXPORT int kno_modest_bigintp(kno_bigint bi)
{
  return (kno_bigint_fits_in_word_p(bi,64,1));
}

KNO_EXPORT int kno_bigint2int(kno_bigint bi)
{
  if (kno_bigint_fits_in_word_p(bi,32,1)) 
    return kno_bigint_to_long(bi);
  else return 0;
}

KNO_EXPORT unsigned int kno_bigint2uint(kno_bigint bi)
{
  if ((kno_bigint_fits_in_word_p(bi,32,1)) &&
      (!(BIGINT_NEGATIVE_P(bi))))
    return kno_bigint_to_long(bi);
  else return 0;
}

KNO_EXPORT long long int kno_bigint2int64(kno_bigint bi)
{
  if (kno_bigint_fits_in_word_p(bi,64,1)) 
    return kno_bigint_to_long_long(bi);
  else return 0;
}

KNO_EXPORT unsigned long long int kno_bigint2uint64(kno_bigint bi)
{
  if ((kno_bigint_fits_in_word_p(bi,64,1)) &&
      (!(BIGINT_NEGATIVE_P(bi))))
    return kno_bigint_to_long_long(bi);
  else return 0;
}

KNO_EXPORT int kno_bigint_fits(kno_bigint bi,int width,int twos_complement)
{
  return kno_bigint_fits_in_word_p(bi,width,twos_complement);
}

KNO_EXPORT unsigned long kno_bigint_bytes(kno_bigint bi)
{
  return kno_bigint_length_in_bytes(bi);
}

KNO_EXPORT
int kno_numberp(lispval x)
{
  if (NUMBERP(x)) return 1; else return 0;
}

KNO_EXPORT
lispval kno_plus(lispval x,lispval y)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x), yt = KNO_PTR_TYPE(y);
  if ((xt == kno_fixnum_type) && (yt == kno_fixnum_type)) {
    long long ix = FIX2INT(x);
    long long iy = FIX2INT(y);
    if (ix==0) return y;
    else if (iy==0) return x;
    else {
      long long result = ix+iy;
      if ((result<KNO_MAX_FIXNUM) && (result>KNO_MIN_FIXNUM))
        return KNO_INT(result);
      else return (lispval) kno_long_long_to_bigint(result);}}
  else if ((xt == kno_flonum_type) && (yt == kno_flonum_type)) {
    double result = KNO_FLONUM(x)+KNO_FLONUM(y);
    return kno_init_flonum(NULL,result);}
  else if (((VECTORP(x))||(KNO_NUMVECP(x)))&&
           ((VECTORP(x))||(KNO_NUMVECP(y)))) {
    int x_len = numvec_length(x), y_len = numvec_length(y);
    if (x_len != y_len)
      return kno_err2(_("Vector size mismatch"),"kno_plus");
    return vector_add(x,y,1);}
  else if (!(NUMBERP(x)))
    return kno_type_error(_("number"),"kno_plus",x);
  else if (!(NUMBERP(y)))
    return kno_type_error(_("number"),"kno_plus",y);
  else {
    long long ix = (FIX2INT(x)>=0) ? (FIX2INT(x)) : -1;
    long long iy = (FIX2INT(y)>=0) ? (FIX2INT(y)) : -1;
    if (ix==0) return kno_incref(y);
    else if (iy==0) return kno_incref(x);
    else if ((xt == kno_complex_type) || (yt == kno_complex_type)) {
      lispval realx = REALPART(x), imagx = IMAGPART(x);
      lispval realy = REALPART(y), imagy = IMAGPART(y);
      lispval real = kno_plus(realx,realy);
      lispval imag = kno_plus(imagx,imagy);
      return make_complex(real,imag);}
    else if ((xt == kno_flonum_type) || (yt == kno_flonum_type)) {
      double dx = todoublex(x,xt), dy = todoublex(y,yt);
      return kno_init_flonum(NULL,dx+dy);}
    else if ((xt == kno_rational_type) || (yt == kno_rational_type)) {
      lispval xnum = NUMERATOR(x), xden = DENOMINATOR(x);
      lispval ynum = NUMERATOR(y), yden = DENOMINATOR(y);
      lispval new_numP1, new_numP2, new_num, new_denom, result;
      new_denom = kno_multiply(xden,yden);
      new_numP1 = kno_multiply(xnum,yden);
      new_numP2 = kno_multiply(ynum,xden);
      new_num = kno_plus(new_numP1,new_numP2);
      result = kno_make_rational(new_num,new_denom);
      kno_decref(new_numP1); kno_decref(new_numP2);
      kno_decref(new_denom); kno_decref(new_num);
      return result;}
    else {
      kno_bigint bx = tobigint(x), by = tobigint(y);
      kno_bigint result = kno_bigint_add(bx,by);
      if (!(KNO_BIGINTP(x))) kno_decref((lispval)bx);
      if (!(KNO_BIGINTP(y))) kno_decref((lispval)by);
      return simplify_bigint(result);}}
}

KNO_EXPORT
lispval kno_multiply(lispval x,lispval y)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x), yt = KNO_PTR_TYPE(y);
  if ((xt == kno_fixnum_type) && (yt == kno_fixnum_type)) {
    long long ix = FIX2INT(x), iy = FIX2INT(y);
    long long q, result;
    if ((iy==0)||(ix==0)) return KNO_INT(0);
    else if (ix==1) return kno_incref(y);
    else if (iy==1) return kno_incref(x);
    q = ((iy>0)?(KNO_MAX_FIXNUM/iy):(KNO_MIN_FIXNUM/iy));
    if ((ix>0)?(ix>q):((-ix)>q)) {
      /* This is the case where there might be an overflow, so we
         switch to bigints */
      kno_bigint bx = tobigint(x), by = tobigint(y);
      kno_bigint bresult = kno_bigint_multiply(bx,by);
      kno_decref((lispval)bx); kno_decref((lispval)by);
      return simplify_bigint(bresult);}
    else result = ix*iy;
    if ((result<KNO_MAX_FIXNUM) && (result>KNO_MIN_FIXNUM))
      return KNO_INT(result);
    else return (lispval) kno_long_long_to_bigint(result);}
  else if ((xt == kno_flonum_type) && (yt == kno_flonum_type)) {
    double result = KNO_FLONUM(x)*KNO_FLONUM(y);
    return kno_init_flonum(NULL,result);}
  else if (((VECTORP(x))||(KNO_NUMVECP(x)))&&(NUMBERP(y)))
    return vector_scale(x,y);
  else if (((VECTORP(y))||(KNO_NUMVECP(y)))&&(NUMBERP(x)))
    return vector_scale(y,x);
  else if (((VECTORP(x))||(KNO_NUMVECP(x)))&&
           ((VECTORP(y))||(KNO_NUMVECP(y)))) {
    int x_len = numvec_length(x), y_len = numvec_length(y);
    if (x_len != y_len)
      return kno_err2(_("Vector size mismatch"),"kno_multiply");
    return vector_dotproduct(x,y);}
  else if (!(NUMBERP(x)))
    return kno_type_error(_("number"),"kno_multiply",x);
  else if (!(NUMBERP(y)))
    return kno_type_error(_("number"),"kno_multiply",y);
  else {
    long long ix = (FIX2INT(x)>=0) ? (FIX2INT(x)) : -1;
    long long iy = (FIX2INT(y)>=0) ? (FIX2INT(y)) : -1;
    if ((ix==0)||(iy==0)) return KNO_FIXZERO;
    else if (ix==1) return kno_incref(y);
    else if (iy==1) return kno_incref(x);
    else if ((COMPLEXP(x)) || (COMPLEXP(y))) {
      lispval realx = REALPART(x), imagx = IMAGPART(x);
      lispval realy = REALPART(y), imagy = IMAGPART(y);
      lispval t1, t2, t3, t4, realr, imagr, result;
      t1 = kno_multiply(realx,realy); t2 = kno_multiply(imagx,imagy);
      t3 = kno_multiply(realx,imagy); t4 = kno_multiply(imagx,realy);
      realr = kno_subtract(t1,t2); imagr = kno_plus(t3,t4);
      result = make_complex(realr,imagr);
      kno_decref(t1); kno_decref(t2); kno_decref(t3); kno_decref(t4);
      return result;}
    else if ((xt == kno_flonum_type) || (yt == kno_flonum_type)) {
      double dx = todoublex(x,xt), dy = todoublex(y,yt);
      return kno_init_flonum(NULL,dx*dy);}
    else if ((xt == kno_rational_type) || (yt == kno_rational_type)) {
      lispval xnum = NUMERATOR(x), xden = DENOMINATOR(x);
      lispval ynum = NUMERATOR(y), yden = DENOMINATOR(y);
      lispval new_denom, new_num, result;
      new_num = kno_multiply(xnum,ynum);
      new_denom = kno_multiply(xden,yden);
      result = make_rational(new_num,new_denom);
      return result;}
    else {
      kno_bigint bx = tobigint(x), by = tobigint(y);
      kno_bigint result = kno_bigint_multiply(bx,by);
      if (!(KNO_BIGINTP(x))) kno_decref((lispval)bx);
      if (!(KNO_BIGINTP(y))) kno_decref((lispval)by);
      return simplify_bigint(result);}}
}

KNO_EXPORT
lispval kno_subtract(lispval x,lispval y)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x), yt = KNO_PTR_TYPE(y);
  if ((xt == kno_fixnum_type) && (yt == kno_fixnum_type)) {
    long long result = (FIX2INT(x))-(FIX2INT(y));
    if ((result<KNO_MAX_FIXNUM) && (result>KNO_MIN_FIXNUM))
      return KNO_INT(result);
    else return (lispval) kno_long_long_to_bigint(result);}
  else if ((xt == kno_flonum_type) && (yt == kno_flonum_type)) {
    double result = KNO_FLONUM(x)-KNO_FLONUM(y);
    return kno_init_flonum(NULL,result);}
  else if (((VECTORP(x))||(KNO_NUMVECP(x)))&&
           ((VECTORP(x))||(KNO_NUMVECP(y)))) {
    int x_len = numvec_length(x), y_len = numvec_length(y);
    if (x_len != y_len)
      return kno_err2(_("Vector size mismatch"),"kno_subtract");
    return vector_add(x,y,-1);}
  else if (!(NUMBERP(x)))
    return kno_type_error(_("number"),"kno_subtract",x);
  else if (!(NUMBERP(y)))
    return kno_type_error(_("number"),"kno_subtract",y);
  else if ((COMPLEXP(x)) || (COMPLEXP(y))) {
    lispval realx = REALPART(x), imagx = IMAGPART(x);
    lispval realy = REALPART(y), imagy = IMAGPART(y);
    lispval real = kno_subtract(realx,realy);
    lispval imag = kno_subtract(imagx,imagy);
    return make_complex(real,imag);}
  else if ((xt == kno_flonum_type) || (yt == kno_flonum_type)) {
    double dx = todoublex(x,xt), dy = todoublex(y,yt);
    return kno_init_flonum(NULL,dx-dy);}
  else if ((xt == kno_rational_type) || (yt == kno_rational_type)) {
    lispval xnum = NUMERATOR(x), xden = DENOMINATOR(x);
    lispval ynum = NUMERATOR(y), yden = DENOMINATOR(y);
    lispval new_numP1, new_numP2, new_num, new_denom, result;
    new_denom = kno_multiply(xden,yden);
    new_numP1 = kno_multiply(xnum,yden);
    new_numP2 = kno_multiply(ynum,xden);
    new_num = kno_subtract(new_numP1,new_numP2);
    result = kno_make_rational(new_num,new_denom);
    kno_decref(new_numP1); kno_decref(new_numP2);
    kno_decref(new_denom); kno_decref(new_num);
    return result;}
  else {
    kno_bigint bx = tobigint(x), by = tobigint(y);
    kno_bigint result = kno_bigint_subtract(bx,by);
    if (!(KNO_BIGINTP(x))) kno_decref((lispval)bx);
    if (!(KNO_BIGINTP(y))) kno_decref((lispval)by);
    return simplify_bigint(result);}
}

KNO_EXPORT
lispval kno_divide(lispval x,lispval y)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x), yt = KNO_PTR_TYPE(y);
  if ((INTEGERP(x)) && (INTEGERP(y)))
    return kno_make_rational(x,y);
  else if ((xt == kno_flonum_type) && (yt == kno_flonum_type)) {
    double result = KNO_FLONUM(x)/KNO_FLONUM(y);
    return kno_init_flonum(NULL,result);}
  else if (!(NUMBERP(x)))
    return kno_type_error(_("number"),"kno_divide",x);
  else if (!(NUMBERP(y)))
    return kno_type_error(_("number"),"kno_divide",y);
  else if ((COMPLEXP(x)) || (COMPLEXP(y))) {
    lispval a = REALPART(x), b = IMAGPART(x);
    lispval c = REALPART(y), d = IMAGPART(y);
    lispval ac, ad, cb, bd, ac_bd, cb_ad, cc, dd, ccpdd;
    lispval realr, imagr, result;
    ac = kno_multiply(a,c); ad = kno_multiply(a,d);
    cb = kno_multiply(c,b); bd = kno_multiply(b,d);
    cc = kno_multiply(c,c); dd = kno_multiply(d,d);
    ccpdd = kno_plus(cc,dd);
    ac_bd = kno_plus(ac,bd); cb_ad = kno_subtract(cb,ad);    
    realr = kno_divide(ac_bd,ccpdd); imagr = kno_divide(cb_ad,ccpdd);
    result = make_complex(realr,imagr);
    kno_decref(ac); kno_decref(ad); kno_decref(cb); kno_decref(bd);
    kno_decref(ac_bd); kno_decref(cb_ad);
    kno_decref(cc); kno_decref(dd); kno_decref(ccpdd);
    return result;}
  else if ((xt == kno_flonum_type) || (yt == kno_flonum_type)) {
    double dx = todoublex(x,xt), dy = todoublex(y,yt);
    return kno_init_flonum(NULL,dx/dy);}
  else if ((xt == kno_rational_type) || (yt == kno_rational_type)) {
    lispval xnum = NUMERATOR(x), xden = DENOMINATOR(x);
    lispval ynum = NUMERATOR(y), yden = DENOMINATOR(y);
    lispval new_denom, new_num, result;
    new_num = kno_multiply(xnum,yden);
    new_denom = kno_multiply(xden,ynum);
    result = make_rational(new_num,new_denom);
    return result;}
  else return kno_type_error(_("number"),"kno_divide",y);
}

KNO_EXPORT
lispval kno_inexact_divide(lispval x,lispval y)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x), yt = KNO_PTR_TYPE(y);
  if ((xt == kno_fixnum_type) && (yt == kno_fixnum_type)) {
    if (yt == KNO_FIXZERO)
      return kno_err("DivideByZero","kno_inexact_divide",NULL,x);
    else {
      long long result = FIX2INT(x)/FIX2INT(y);
      if ((FIX2INT(x)) == (result*(FIX2INT(y))))
        return KNO_INT(result);
      else {
        double dx = x, dy = y;
        return kno_init_flonum(NULL,dx/dy);}}}
  else if ((xt == kno_flonum_type) && (yt == kno_flonum_type)) {
    double fx = KNO_FLONUM(x), fy = KNO_FLONUM(y);
    if (fy==0)
      return kno_err("DivideByZero","kno_inexact_divide",NULL,x);
    else return kno_init_flonum(NULL,fx/fy);}
  else if (!(NUMBERP(x)))
    return kno_type_error(_("number"),"kno_builtin_divinexact",x);
  else if (!(NUMBERP(y)))
    return kno_type_error(_("number"),"kno_builtin_divinexact",y);
  else {
    double dx = todoublex(x,xt), dy = todoublex(y,yt);
    return kno_init_flonum(NULL,dx/dy);}
}

KNO_EXPORT
lispval kno_quotient(lispval x,lispval y)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x), yt = KNO_PTR_TYPE(y);
  if ((xt == kno_fixnum_type) && (yt == kno_fixnum_type)) {
    long long result = FIX2INT(x)/FIX2INT(y);
    return KNO_INT(result);}
  else if ((INTEGERP(x)) && (INTEGERP(y))) {
    kno_bigint bx = tobigint(x), by = tobigint(y);
    kno_bigint result = kno_bigint_quotient(bx,by);
    if (!(KNO_BIGINTP(x))) kno_decref((lispval)bx);
    if (!(KNO_BIGINTP(y))) kno_decref((lispval)by);
    return simplify_bigint(result);}
  else if (INTEGERP(x))
    return kno_type_error(_("integer"),"kno_quotient",y);
  else return kno_type_error(_("integer"),"kno_quotient",x);
}

KNO_EXPORT
lispval kno_remainder(lispval x,lispval y)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x), yt = KNO_PTR_TYPE(y);
  if ((xt == kno_fixnum_type) && (yt == kno_fixnum_type)) {
    long long result = FIX2INT(x)%FIX2INT(y);
    return KNO_INT(result);}
  else if ((INTEGERP(x)) && (INTEGERP(y))) {
    kno_bigint bx = tobigint(x), by = tobigint(y);
    kno_bigint result = kno_bigint_remainder(bx,by);
    if (!(KNO_BIGINTP(x))) kno_decref((lispval)bx);
    if (!(KNO_BIGINTP(y))) kno_decref((lispval)by);
    return simplify_bigint(result);}
  else if (INTEGERP(x))
    return kno_type_error(_("integer"),"kno_remainder",y);
  else return kno_type_error(_("integer"),"kno_remainder",x);
}

KNO_EXPORT
lispval kno_gcd(lispval x,lispval y)
{
  return int_gcd(x,y);
}

KNO_EXPORT
lispval kno_lcm(lispval x,lispval y)
{
  return int_lcm(x,y);
}

KNO_EXPORT
lispval kno_pow(lispval x,lispval y)
{
  double dx = todouble(x), dy = todouble(y);
  double result = pow(dx,dy);
  return kno_make_flonum(result);
}

static int signum(lispval x)
{
  if (FIXNUMP(x)) {
    long long ival = FIX2INT(x);
    if (ival<0) return -1;
    else if (ival>0) return 1;
    else return 0;}
  else if (KNO_FLONUMP(x)) {
    double dval = KNO_FLONUM(x);
    if (dval<0) return -1;
    else if (dval>0) return 1;
    else return 0;}
  else if (KNO_BIGINTP(x))
    switch (bigint_test((kno_bigint)x)) {
    case kno_bigint_less: return -1;
    case kno_bigint_greater: return 1;
    default: return 0;}
  else if (RATIONALP(x)) {
    int nsign = signum(KNO_NUMERATOR(x));
    int dsign = signum(KNO_DENOMINATOR(x));
    return (nsign*dsign);}
  else return 0;
}

KNO_EXPORT int kno_tolonglong(lispval r,long long *intval)
{
  if (FIXNUMP(r)) {
    *intval = ((long long)(FIX2INT(r)));
    return 1;}
  else if ((KNO_BIGINTP(r))&&
           (kno_bigint_fits_in_word_p((kno_bigint)r,64,1))) {
    long long ival = kno_bigint_to_long_long((kno_bigint)r);
    *intval = ival;
    return 1;}
  else if (KNO_BIGINTP(r))
    return 0;
  else return -1;
}

KNO_EXPORT
int kno_numcompare(lispval x,lispval y)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x), yt = KNO_PTR_TYPE(y);
  if ((xt == kno_fixnum_type) && (yt == kno_fixnum_type)) {
    long long dx = FIX2INT(x), dy = FIX2INT(y);
    if (dx>dy) return 1; else if (dx<dy) return -1; else return 0;}
  else if ((xt == kno_flonum_type) && (yt == kno_flonum_type)) {
    double dx = KNO_FLONUM(x), dy = KNO_FLONUM(y);
    if (dx>dy) return 1; else if (dx<dy) return -1; else return 0;}
  else if (!(NUMBERP(x)))
    /* Any number > 1 indicates an error. */
    return KNO_ERR(17,kno_TypeError,"compare","object is not a number",y);
  else if (!(NUMBERP(y)))
    /* Any number > 1 indicates an error. */
    return KNO_ERR(17,kno_TypeError,"compare","object is not a number",y);
  else if ((COMPLEXP(x)) || (COMPLEXP(x))) {
    double magx = todouble(x), magy = todouble(y);
    double diff = magx-magy;
    if (diff == 0) return 0;
    else if (diff>0) return 1;
    else return-1;}
  else {
    lispval difference = kno_subtract(x,y);
    int sgn = signum(difference);
    kno_decref(difference);
    if (sgn==0)
      if ((xt == kno_flonum_type) || (yt == kno_flonum_type)) 
	/* If either argument is inexact (double), don't return =, unless
	   both are inexact (which is handled above) */
	if (xt<yt) return -1; else return 1;
      else return sgn;
    else return sgn;}
}

/* Exact/inexact conversion */

KNO_EXPORT
lispval kno_make_inexact(lispval x)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x);
  if (xt == kno_flonum_type)
    return kno_incref(x);
  else if (xt == kno_fixnum_type)
    return kno_init_flonum(NULL,((double) (FIX2INT(x))));
  else if (xt == kno_bigint_type)
    return kno_init_flonum(NULL,((double) kno_bigint_to_double((kno_bigint)x)));
  else if (xt == kno_rational_type) {
    double num = todouble(NUMERATOR(x));
    double den = todouble(DENOMINATOR(x));
    return kno_init_flonum(NULL,num/den);}
  else if (xt == kno_complex_type) {
    lispval realpart = KNO_REALPART(x), imagpart = KNO_IMAGPART(x);
    if ((KNO_FLONUMP(realpart)) &&
	(KNO_FLONUMP(imagpart)))
      return kno_incref(x);
    else return make_complex(kno_make_inexact(realpart),
			     kno_make_inexact(imagpart));}
  else return kno_type_error(_("number"),"kno_make_inexact",x);
}

KNO_EXPORT
lispval kno_make_exact(lispval x)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x);
  if (xt == kno_flonum_type) {
    double d = KNO_FLONUM(x);
    double f = floor(d);
    if (f == d) {
      kno_bigint ival = kno_double_to_bigint(d);
      return simplify_bigint(ival);}
#if 0
    else {
      double top = significand(d)*1048576;
      long long exp = ilogb(d);
      kno_bigint itop = kno_double_to_bigint(top);
      kno_bigint ibottom = bigint_make_one(0);
      bigint_destructive_scale_up(ibottom,1024);
      bigint_destructive_scale_up(ibottom,1024);
      if (exp>0) while (exp>0) {
	bigint_destructive_scale_up(itop,2); exp--;}
      else while (exp<0) {
	bigint_destructive_scale_up(ibottom,2);
	exp++;}
      return make_rational(simplify_bigint(itop),
			   simplify_bigint(ibottom));}
#endif
    else {
      kno_bigint ival = kno_double_to_bigint(d);
      return simplify_bigint(ival);}}
  else if (xt == kno_complex_type) {
    lispval realpart = KNO_REALPART(x), imagpart = KNO_IMAGPART(x);
    if ((KNO_FLONUMP(realpart)) ||
	(KNO_FLONUMP(imagpart)))
      return make_complex(kno_make_exact(realpart),kno_make_exact(imagpart));
    else return kno_incref(x);}
  else if (NUMBERP(x)) return kno_incref(x);
  else return kno_type_error(_("number"),"kno_make_inexact",x);
}

KNO_EXPORT
int kno_exactp(lispval x)
{
  kno_ptr_type xt = KNO_PTR_TYPE(x);
  if (xt == kno_flonum_type) return 0;
  else if (xt == kno_complex_type) {
    lispval realpart = KNO_REALPART(x), imagpart = KNO_IMAGPART(x);
    if ((KNO_FLONUMP(realpart)) || (KNO_FLONUMP(imagpart)))
      return 0;
    else return 1;}
  else if (NUMBERP(x)) return 1;
  else return -1;
}


/* Homogenous vectors */

/* Numeric vector handlers */

static void recycle_numeric_vector(struct KNO_RAW_CONS *c)
{
  struct KNO_NUMERIC_VECTOR *v = (struct KNO_NUMERIC_VECTOR *)c;
  enum kno_num_elt_type elt_type = v->numvec_elt_type;
  if (v->numvec_free_elts) {
    switch(elt_type) {
    case kno_short_elt:
      u8_free(v->numvec_elts.shorts); break;
    case kno_int_elt:
      u8_free(v->numvec_elts.ints); break;
    case kno_long_elt:
      u8_free(v->numvec_elts.longs); break;
    case kno_float_elt:
      u8_free(v->numvec_elts.floats); break;
    case kno_double_elt:
      u8_free(v->numvec_elts.doubles); break;}}
  if (KNO_MALLOCD_CONSP(c)) {
    u8_free(c);}
}

static double double_ref(struct KNO_NUMERIC_VECTOR *vec,int i)
{
  enum kno_num_elt_type elt_type = vec->numvec_elt_type;
  switch (elt_type) {
  case kno_short_elt:
    return (double) (KNO_NUMVEC_SHORT(vec,i));
  case kno_int_elt:
    return (double) (KNO_NUMVEC_INT(vec,i));
  case kno_long_elt:
    return (double) (KNO_NUMVEC_LONG(vec,i));
  case kno_float_elt:
    return (double) (KNO_NUMVEC_FLOAT(vec,i));
  case kno_double_elt:
    return (double) (KNO_NUMVEC_DOUBLE(vec,i));
  default:
    return INFINITY;}
}

static size_t nvec_elt_size(enum kno_num_elt_type elt_type)
{
  switch (elt_type) {
  case kno_short_elt:
    return sizeof(kno_short);
  case kno_int_elt:
    return sizeof(kno_int);
  case kno_long_elt:
    return sizeof(kno_long);
  case kno_float_elt:
    return sizeof(kno_float);
  case kno_double_elt:
    return sizeof(kno_double);
  default:
    return sizeof(kno_double);}
}

static int compare_numeric_vector(lispval x,lispval y,kno_compare_flags flags)
{
  struct KNO_NUMERIC_VECTOR *vx = (struct KNO_NUMERIC_VECTOR *)x;
  struct KNO_NUMERIC_VECTOR *vy = (struct KNO_NUMERIC_VECTOR *)y;
  if (vx->numvec_length == vy->numvec_length) {
    int i = 0, n = vx->numvec_length; 
    while (i<n) {
      double xelt = double_ref(vx,i);
      double yelt = double_ref(vy,i);
      if (xelt>yelt)
        return -1;
      else if (yelt>xelt)
        return 1;
      else i++;}
    return 0;}
  else if (vx->numvec_length > vy->numvec_length)
    return 1;
  else return -1;
}

static int hash_numeric_vector(lispval x,unsigned int (*fn)(lispval))
{
  struct KNO_NUMERIC_VECTOR *vec = (struct KNO_NUMERIC_VECTOR *)x;
  int i = 0, n = vec->numvec_length; int hashval = vec->numvec_length;
  while (i<n) {
    double v = double_ref(vec,i); int exp;
    double mantissa = frexpf(v,&exp);
    double reformed=
      ((exp<0) ? (ldexpf(mantissa,0)) : (ldexpf(mantissa,exp)));
    int asint = (int)reformed;
    hashval = hash_combine(hashval,asint);
    i++;}
  return hashval;
}

static lispval copy_numeric_vector(lispval x,int deep)
{
  struct KNO_NUMERIC_VECTOR *vec = (struct KNO_NUMERIC_VECTOR *)x;
  enum kno_num_elt_type elt_type = vec->numvec_elt_type;
  size_t len = vec->numvec_length;
  size_t elts_size = len*nvec_elt_size(vec->numvec_elt_type);
  size_t vec_size = sizeof(struct KNO_NUMERIC_VECTOR)+elts_size;
  struct KNO_NUMERIC_VECTOR *copy = u8_malloc(vec_size);
  memset(copy,0,vec_size);
  KNO_INIT_CONS(copy,kno_numeric_vector_type);
  copy->numvec_length = len; copy->numvec_free_elts = 0;
  switch (elt_type) {
  case kno_short_elt:
    copy->numvec_elts.shorts = vec->numvec_elts.shorts;
    memcpy(copy->numvec_elts.shorts,vec->numvec_elts.shorts,elts_size); 
    break;
  case kno_int_elt:
    copy->numvec_elts.ints = vec->numvec_elts.ints;
    memcpy(copy->numvec_elts.ints,vec->numvec_elts.ints,elts_size); 
    break;
  case kno_long_elt:
    copy->numvec_elts.longs = vec->numvec_elts.longs;
    memcpy(copy->numvec_elts.longs,vec->numvec_elts.longs,elts_size); 
    break;
  case kno_float_elt:
    copy->numvec_elts.floats = vec->numvec_elts.floats;
    memcpy(copy->numvec_elts.floats,vec->numvec_elts.floats,elts_size); 
    break;
  case kno_double_elt:
    copy->numvec_elts.doubles = vec->numvec_elts.doubles;
    memcpy(copy->numvec_elts.doubles,vec->numvec_elts.doubles,elts_size); 
    break;}
  return (lispval) copy;
}

static int unparse_numeric_vector(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_NUMERIC_VECTOR *vec = (struct KNO_NUMERIC_VECTOR *)x;
  int i = 0, n = vec->numvec_length; char *typename="NUMVEC";
  enum kno_num_elt_type type = vec->numvec_elt_type;
  switch (type) {
  case kno_short_elt:
    typename="SHORTVEC"; break;
  case kno_int_elt:
    typename="INTVEC"; break;
  case kno_long_elt:
    typename="LONGVEC"; break;
  case kno_float_elt:
    typename="FLOATVEC"; break;
  case kno_double_elt:
    typename="DOUBLEVEC"; break;}
  u8_printf(out,"#<%s",typename);
  if (n>kno_numvec_showmax) switch (type) {
      case kno_short_elt: {
        long long sum = 0, dot = 0; while (i<n) {
          long long v = KNO_NUMVEC_SHORT(vec,i);
          sum = sum+v; dot = dot+v*v; i++;}
        u8_printf(out," sum=%lld/dot=%d/n=%lld",sum,dot,n);
        break;}
      case kno_int_elt: {
        long long sum = 0, dot = 0; while (i<n) {
          long long v = KNO_NUMVEC_INT(vec,i);
          sum = sum+v; dot = dot+v*v; i++;}
        u8_printf(out," sum=%lld/dot=%lld/n=%lld",sum,dot,n);
        break;}
      case kno_long_elt: {
        long long sum = 0, dot = 0; while (i<n) {
          int v = KNO_NUMVEC_LONG(vec,i);
          sum = sum+v; dot = dot+v*v; i++;}
        u8_printf(out," sum=%lld/dot=%lld/n=%lld",sum,dot,n);
        break;}
      case kno_float_elt: {
        double sum = 0, dot = 0; while (i<n) {
          double v = KNO_NUMVEC_FLOAT(vec,i);
          sum = sum+v; dot = dot+v*v; i++;}
        u8_printf(out," sum=%f/dot=%f/n=%d",sum,dot,n);
        break;}
      case kno_double_elt: {
        double sum = 0, dot = 0; while (i<n) {
          double v = KNO_NUMVEC_DOUBLE(vec,i);
          sum = sum+v; dot = dot+v*v; i++;}
        u8_printf(out," sum=%f/dot=%f/n=%d",sum,dot,n);
        break;}}
  else switch (type) {
  case kno_short_elt:
    while (i<n) {u8_printf(out," %d",KNO_NUMVEC_SHORT(vec,i)); i++;} break;
  case kno_int_elt:
    while (i<n) {u8_printf(out," %d",KNO_NUMVEC_INT(vec,i)); i++;} break;
  case kno_long_elt:
    while (i<n) {u8_printf(out," %lld",KNO_NUMVEC_LONG(vec,i)); i++;} break;
  case kno_float_elt:
    while (i<n) {u8_printf(out," %f",(double)KNO_NUMVEC_FLOAT(vec,i)); i++;} break;
  case kno_double_elt:
    while (i<n) {u8_printf(out," %f",KNO_NUMVEC_DOUBLE(vec,i)); i++;} break;}
  u8_puts(out,">");
  return 1;
}

/* Numeric vector constructors */

KNO_EXPORT lispval kno_make_double_vector(int n,kno_double *v)
{
  struct KNO_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct KNO_NUMERIC_VECTOR)+(n*sizeof(kno_double)));
  unsigned char *bytes = (unsigned char *)nvec;
  KNO_INIT_FRESH_CONS(nvec,kno_numeric_vector_type);
  nvec->numvec_elt_type = kno_double_elt;
  nvec->numvec_free_elts = 0;
  nvec->numvec_length = n;
  memcpy(bytes+sizeof(struct KNO_NUMERIC_VECTOR),v,n*sizeof(kno_double));
  nvec->numvec_elts.doubles = (kno_double *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR));
  return (lispval) nvec;
}

KNO_EXPORT lispval kno_make_float_vector(int n,kno_float *v)
{
  struct KNO_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct KNO_NUMERIC_VECTOR)+(n*sizeof(kno_float)));
  unsigned char *bytes = (unsigned char *)nvec;
  KNO_INIT_FRESH_CONS(nvec,kno_numeric_vector_type);
  nvec->numvec_elt_type = kno_float_elt;
  nvec->numvec_free_elts = 0;
  nvec->numvec_length = n;
  memcpy(bytes+sizeof(struct KNO_NUMERIC_VECTOR),v,n*sizeof(kno_float));
  nvec->numvec_elts.floats = (kno_float *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR));
  return (lispval) nvec;
}

KNO_EXPORT lispval kno_make_int_vector(int n,kno_int *v)
{
  struct KNO_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct KNO_NUMERIC_VECTOR)+(n*sizeof(kno_int)));
  unsigned char *bytes = (unsigned char *)nvec;
  KNO_INIT_FRESH_CONS(nvec,kno_numeric_vector_type);
  nvec->numvec_elt_type = kno_int_elt;
  nvec->numvec_free_elts = 0;
  nvec->numvec_length = n;
  memcpy(bytes+sizeof(struct KNO_NUMERIC_VECTOR),v,n*sizeof(kno_int));
  nvec->numvec_elts.ints = (kno_int *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR));
  return (lispval) nvec;
}

KNO_EXPORT lispval kno_make_long_vector(int n,kno_long *v)
{
  struct KNO_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct KNO_NUMERIC_VECTOR)+(n*sizeof(kno_long)));
  unsigned char *bytes = (unsigned char *)nvec;
  KNO_INIT_FRESH_CONS(nvec,kno_numeric_vector_type);
  nvec->numvec_elt_type = kno_long_elt;
  nvec->numvec_free_elts = 0;
  nvec->numvec_length = n;
  memcpy(bytes+sizeof(struct KNO_NUMERIC_VECTOR),v,n*sizeof(kno_long));
  nvec->numvec_elts.longs = (kno_long *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR));
  return (lispval) nvec;
}

KNO_EXPORT lispval kno_make_short_vector(int n,kno_short *v)
{
  struct KNO_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct KNO_NUMERIC_VECTOR)+(n*sizeof(kno_short)));
  unsigned char *bytes = (unsigned char *)nvec;
  KNO_INIT_FRESH_CONS(nvec,kno_numeric_vector_type);
  nvec->numvec_elt_type = kno_short_elt;
  nvec->numvec_free_elts = 0;
  nvec->numvec_length = n;
  memcpy(bytes+sizeof(struct KNO_NUMERIC_VECTOR),v,n*sizeof(kno_short));
  nvec->numvec_elts.shorts = (kno_short *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR));
  return (lispval) nvec;
}

KNO_EXPORT lispval kno_make_numeric_vector(int n,enum kno_num_elt_type vectype)
{
  int elt_size = nvec_elt_size(vectype);
  struct KNO_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct KNO_NUMERIC_VECTOR)+(n*elt_size));
  unsigned char *bytes = (unsigned char *)nvec;
  KNO_INIT_FRESH_CONS(nvec,kno_numeric_vector_type);
  nvec->numvec_elt_type = vectype;
  nvec->numvec_free_elts = 0;
  nvec->numvec_length = n;
  memset(((char *)nvec)+sizeof(struct KNO_NUMERIC_VECTOR),0,n*elt_size);
  switch (vectype) {
  case kno_short_elt:
    nvec->numvec_elts.shorts = (kno_short *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR)); 
    break;
  case kno_int_elt:
    nvec->numvec_elts.ints = (kno_int *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR)); 
    break;
  case kno_long_elt:
    nvec->numvec_elts.longs = (kno_long *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR)); 
    break;
  case kno_float_elt:
    nvec->numvec_elts.floats = (kno_float *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR)); 
    break;
  case kno_double_elt: 
    nvec->numvec_elts.doubles = (kno_double *)(bytes+sizeof(struct KNO_NUMERIC_VECTOR)); 
    break;}
  return (lispval) nvec;
}


/* Vector operations */

static void decref_vec(lispval *elts,int n)
{
  int i = 0; while (i<n) {
    lispval elt = elts[i++];
    kno_decref(elt);}
}

static int numvec_length(lispval x)
{
  if (VECTORP(x))
    return VEC_LEN(x);
  else if (KNO_NUMVECP(x))
    return KNO_NUMVEC_LENGTH(x);
  else return -1;
}
static kno_long EXACT_REF(lispval x,enum kno_num_elt_type xtype,int i)
{
  switch (xtype) {
  case kno_long_elt:
    return KNO_NUMVEC_LONG(x,i);
  case kno_int_elt:
    return KNO_NUMVEC_INT(x,i);
  case kno_short_elt:
    return KNO_NUMVEC_SHORT(x,i);
  default:
    return -1;}
}
static kno_double INEXACT_REF(lispval x,enum kno_num_elt_type xtype,int i)
{
  switch (xtype) {
  case kno_double_elt:
    return KNO_NUMVEC_DOUBLE(x,i);
  case kno_float_elt:
    return ((kno_double)(KNO_NUMVEC_FLOAT(x,i)));
  case kno_long_elt:
    return ((kno_double)(KNO_NUMVEC_LONG(x,i)));
  case kno_int_elt:
    return ((kno_double)(KNO_NUMVEC_INT(x,i)));
  case kno_short_elt:
    return ((kno_double)(KNO_NUMVEC_SHORT(x,i)));
  default: return 0.0;}
}
static lispval NUM_ELT(lispval x,int i)
{
  if (VECTORP(x)) {
    lispval elt = VEC_REF(x,i);
    kno_incref(elt);
    return elt;}
  else {
    struct KNO_NUMERIC_VECTOR *vx = (struct KNO_NUMERIC_VECTOR *)x;
    enum kno_num_elt_type xtype = vx->numvec_elt_type;
    switch (xtype) {
    case kno_double_elt:
      return kno_make_flonum(KNO_NUMVEC_DOUBLE(x,i));
    case kno_float_elt:
      return kno_make_flonum(KNO_NUMVEC_FLOAT(x,i));
    case kno_long_elt:
      return KNO_INT2LISP(KNO_NUMVEC_LONG(x,i));
    case kno_int_elt:
      return KNO_INT2LISP(KNO_NUMVEC_INT(x,i));
    case kno_short_elt:
      return KNO_SHORT2DTYPE(KNO_NUMVEC_SHORT(x,i));
    default: {
      kno_incref(x);
      return kno_err(_("NotAVector"),"NUM_ELT",NULL,x);}
    }
  }
}
static lispval vector_add(lispval x,lispval y,int mult)
{
  int x_len = numvec_length(x), y_len = numvec_length(y);
  if (x_len!=y_len)
    return kno_err2("Dimensional conflict","vector_add");
  else if ((KNO_NUMVECP(x))&&(KNO_NUMVECP(y))) {
    struct KNO_NUMERIC_VECTOR *vx = (struct KNO_NUMERIC_VECTOR *)x;
    struct KNO_NUMERIC_VECTOR *vy = (struct KNO_NUMERIC_VECTOR *)y;
    enum kno_num_elt_type xtype = vx->numvec_elt_type;
    enum kno_num_elt_type ytype = vy->numvec_elt_type;
    if (((xtype == kno_float_elt)||(xtype == kno_double_elt))&&
        ((ytype == kno_float_elt)||(ytype == kno_double_elt))) {
      /* Both arguments are inexact vectors*/
      if ((xtype == kno_float_elt)||(ytype == kno_float_elt)) {
        /* One of them is a float vector, so return a float vector
           (don't invent precision) */
        lispval result; kno_float *sums = u8_alloc_n(x_len,kno_float);
        int i = 0; while (i<x_len) {
          kno_float xelt = ((xtype == kno_double_elt)?
                         (KNO_NUMVEC_DOUBLE(x,i)):
                         (KNO_NUMVEC_FLOAT(x,i)));
          kno_float yelt = ((ytype == kno_double_elt)?
                         (KNO_NUMVEC_DOUBLE(y,i)):
                         (KNO_NUMVEC_FLOAT(y,i)));
          sums[i]=xelt+(yelt*mult);
          i++;}
        result = kno_make_float_vector(x_len,sums);
        u8_free(sums);
        return result;}
      else {
        /* They're both double vectors, so return a double vector */
        lispval result; kno_double *sums = u8_alloc_n(x_len,kno_double);
        int i = 0; while (i<x_len) {
          kno_double xelt = (KNO_NUMVEC_DOUBLE(x,i));
          kno_double yelt = (KNO_NUMVEC_DOUBLE(y,i));
          sums[i]=xelt+(yelt*mult);
          i++;}
        result = kno_make_double_vector(x_len,sums);
        u8_free(sums);
        return result;}}
    else if ((xtype == kno_float_elt)||(xtype == kno_double_elt)||
             (ytype == kno_float_elt)||(ytype == kno_double_elt))  {
      /* One of the vectors is a floating type, so return a floating
         point vector. */
      lispval result; kno_double *sums = u8_alloc_n(x_len,kno_double);
      int i = 0; while (i<x_len) {
        kno_double xelt = (INEXACT_REF(x,xtype,i));
        kno_double yelt = (INEXACT_REF(y,ytype,i));
        sums[i]=xelt+(yelt*mult);
        i++;}
      result = kno_make_double_vector(x_len,sums);
      u8_free(sums);
      return result;}
    else if ((xtype == kno_long_elt)||(ytype == kno_long_elt)) {
      /* One of them is a long so return a long vector. */
      lispval result; kno_long *sums = u8_alloc_n(x_len,kno_long);
      int i = 0; while (i<x_len) {
        kno_long xelt = EXACT_REF(x,xtype,i);
        kno_long yelt = EXACT_REF(y,ytype,i);
        sums[i]=xelt+(yelt*mult);
        i++;}
      result = kno_make_long_vector(x_len,sums);
      u8_free(sums);
      return result;}
    else if ((xtype == kno_int_elt)||(ytype == kno_int_elt)) {
      /* One of them is an int vector so return an int vector. */
      lispval result; kno_int *sums = u8_alloc_n(x_len,kno_int);
      int i = 0; while (i<x_len) {
        kno_int xelt = EXACT_REF(x,xtype,i);
        kno_int yelt = EXACT_REF(y,ytype,i);
        sums[i]=xelt+(yelt*mult);
        i++;}
      result = kno_make_int_vector(x_len,sums);
      u8_free(sums);
      return result;}
    else {
      /* This really means that they're both short vectors */
      lispval result; kno_short *sums = u8_alloc_n(x_len,kno_short);
      int i = 0; while (i<x_len) {
        kno_int xelt = EXACT_REF(x,xtype,i);
        kno_int yelt = EXACT_REF(y,ytype,i);
        sums[i]=xelt+(yelt*mult);
        i++;}
      result = kno_make_short_vector(x_len,sums);
      u8_free(sums);
      return result;}}
  else {
    lispval *sums = u8_alloc_n(x_len,lispval), result;
    lispval factor = KNO_INT2LISP(mult);
    int i = 0; while (i<x_len) {
      lispval xelt = NUM_ELT(x,i);
      lispval yelt = NUM_ELT(y,i);
      lispval sum;
      if (factor == KNO_FIXNUM_ONE)
        sum = kno_plus(xelt,yelt);
      else {
        lispval mult = kno_multiply(yelt,factor);
        if (KNO_ABORTP(mult)) {
          decref_vec(sums,i); u8_free(sums);
          return mult;}
        sum = kno_plus(xelt,mult);
        if (KNO_ABORTP(sum)) {
          decref_vec(sums,i); u8_free(sums);
          kno_decref(mult);
          return mult;}
        kno_decref(mult);}
      sums[i]=sum;
      i++;}
    result = kno_make_vector(x_len,sums);
    u8_free(sums);
    return result;}
}
static lispval vector_dotproduct(lispval x,lispval y)
{
  int x_len = numvec_length(x), y_len = numvec_length(y);
  if (x_len!=y_len)
    return kno_err2("Dimensional conflict","vector_add");
  else if ((KNO_NUMVECP(x))&&(KNO_NUMVECP(y))) {
    struct KNO_NUMERIC_VECTOR *vx = (struct KNO_NUMERIC_VECTOR *)x;
    struct KNO_NUMERIC_VECTOR *vy = (struct KNO_NUMERIC_VECTOR *)y;
    enum kno_num_elt_type x_elt_type = vx->numvec_elt_type;
    enum kno_num_elt_type y_elt_type = vy->numvec_elt_type;
    if (((x_elt_type == kno_float_elt)||(x_elt_type == kno_double_elt))&&
        ((y_elt_type == kno_float_elt)||(y_elt_type == kno_double_elt))) {
      /* This is the case where they're both inexact (floating) */
      double dot = 0;
      if ((x_elt_type == kno_float_elt)||(y_elt_type == kno_float_elt)) {
        /* This is the case where they're both float vectors */
        int i = 0; while (i<x_len) {
          kno_float xelt = ((x_elt_type == kno_double_elt)?
                         (KNO_NUMVEC_DOUBLE(x,i)):
                         (KNO_NUMVEC_FLOAT(x,i)));
          kno_float yelt = ((y_elt_type == kno_double_elt)?
                         (KNO_NUMVEC_DOUBLE(y,i)):
                         (KNO_NUMVEC_FLOAT(y,i)));
          dot = dot+(xelt*yelt);
          i++;}
        return kno_make_flonum(dot);}
      else {
        /* This is the case where they're either both doubles
           or mixed. */
        double dot = 0;
        int i = 0; while (i<x_len) {
          kno_double xelt = (KNO_NUMVEC_DOUBLE(x,i));
          kno_double yelt = (KNO_NUMVEC_DOUBLE(y,i));
          dot = dot+(xelt*yelt);
          i++;}
        return kno_make_flonum(dot);}}
    else if ((x_elt_type == kno_float_elt)||(x_elt_type == kno_double_elt)||
             (y_elt_type == kno_float_elt)||(y_elt_type == kno_double_elt))  {
      /* This is the case where either is inexact (floating) */
      double dot = 0;
      int i = 0; while (i<x_len) {
        kno_double xelt = (INEXACT_REF(x,x_elt_type,i));
        kno_double yelt = (INEXACT_REF(y,y_elt_type,i));
        dot = dot+(xelt*yelt);
        i++;}
      return kno_make_flonum(dot);}
    /* For the integral types, we pick the size of the larger. */
    else if ((x_elt_type == kno_long_elt)||(y_elt_type == kno_long_elt)) {
      kno_long dot = 0;
      int i = 0; while (i<x_len) {
        kno_long xelt = EXACT_REF(x,x_elt_type,i);
        kno_long yelt = EXACT_REF(y,y_elt_type,i);
        dot = dot+(xelt*yelt);
        i++;}
      return KNO_INT2LISP(dot);}
    else if ((x_elt_type == kno_int_elt)||(y_elt_type == kno_int_elt)) {
      kno_long dot = 0;
      int i = 0; while (i<x_len) {
        kno_int xelt = EXACT_REF(x,x_elt_type,i);
        kno_int yelt = EXACT_REF(y,y_elt_type,i);
        dot = dot+(xelt*yelt);
        i++;}
      return KNO_INT2LISP(dot);}
    else {
      /* They're both short vectors */
      kno_long dot = 0;
      int i = 0; while (i<x_len) {
        kno_int xelt = EXACT_REF(x,x_elt_type,i);
        kno_int yelt = EXACT_REF(y,y_elt_type,i);
        dot = dot+(xelt*yelt);
        i++;}
      return KNO_INT2LISP(dot);}}
  else {
    lispval dot = KNO_FIXNUM_ZERO;
    int i = 0; while (i<x_len) {
      lispval x_elt = NUM_ELT(x,i);
      lispval y_elt = NUM_ELT(y,i);
      lispval prod = kno_multiply(x_elt,y_elt);
      lispval new_sum=
        (KNO_ABORTP(prod))?(prod):(kno_plus(dot,prod));
      kno_decref(x_elt); kno_decref(y_elt);
      kno_decref(prod); kno_decref(dot);
      if (KNO_ABORTP(new_sum)) return new_sum;
      dot = new_sum;
      i++;}
    return dot;}
}
static lispval generic_vector_scale(lispval vec,lispval scalar)
{
  int len = (VECTORP(vec))?(VEC_LEN(vec)):(numvec_length(vec));
  lispval result, *elts = u8_alloc_n(len,lispval);
  int i = 0; while (i<len) {
    lispval elt = NUM_ELT(vec,i);
    lispval product = kno_multiply(elt,scalar);
    if (KNO_ABORTP(product)) {
      decref_vec(elts,i);
      return product;}
    elts[i]=product;
    kno_decref(elt);
    i++;}
  result = kno_make_vector(len,elts);
  u8_free(elts);
  return result;
}
static lispval vector_scale(lispval vec,lispval scalar)
{
  if (KNO_NUMVECP(vec)) {
    lispval result;
    struct KNO_NUMERIC_VECTOR *nv = (struct KNO_NUMERIC_VECTOR *)vec;
    enum kno_num_elt_type vtype = nv->numvec_elt_type; int vlen = nv->numvec_length;
    if (KNO_FLONUMP(scalar)) {
      kno_double mult = KNO_FLONUM(scalar);
      kno_double *scaled = u8_alloc_n(vlen,kno_double);
      int i = 0;
      if ((vtype == kno_float_elt)||(vtype == kno_double_elt)) {
        while (i<vlen) {
          kno_double elt = INEXACT_REF(vec,vtype,i);
          scaled[i]=elt*mult;
          i++;}}
      else while (i<vlen) {
          kno_long elt = EXACT_REF(vec,vtype,i);
          scaled[i]=elt*mult;
          i++;}
      result = kno_make_double_vector(vlen,scaled);
      u8_free(scaled);
      return result;}
    else if ((vtype == kno_float_elt)||(vtype == kno_double_elt)) {
      kno_long mult = kno_getint(scalar);
      kno_double *scaled = u8_alloc_n(vlen,kno_double);
      int i = 0; while (i<vlen) {
        kno_double elt = INEXACT_REF(vec,vtype,i);
        scaled[i]=elt*mult;
        i++;}
      result = kno_make_double_vector(vlen,scaled);
      u8_free(scaled);
      return result;}
    else {
      lispval max, min;
      lispval *scaled = u8_alloc_n(vlen,lispval);
      int i = 0; while (i<vlen) {
        lispval elt = NUM_ELT(vec,i);
        lispval product = kno_multiply(elt,scalar);
        if (i==0) {max = product; min = product;}
        else {
          if (kno_numcompare(product,max)>0) max = product;
          if (kno_numcompare(product,min)<0) min = product;}
        scaled[i]=product;
        i++;}
      if ((FIXNUMP(max))&&(FIXNUMP(min))) {
        long long imax = kno_getint(max), imin = kno_getint(min);
        if ((imax>-32768)&&(imax<32768)&&
            (imin>-32768)&&(imin<32768)) {
          kno_short *shorts = u8_alloc_n(vlen,kno_short);
          int j = 0; while (j<vlen) {
            long long elt = FIX2INT(scaled[j]);
            shorts[j]=(short)elt;
            j++;}
          result = kno_make_short_vector(vlen,shorts);
          u8_free(shorts); u8_free(scaled);}
        else {
          kno_int *ints = u8_alloc_n(vlen,kno_int);
          int j = 0; while (j<vlen) {
            int elt = kno_getint(scaled[j]);
            ints[j]=elt;
            j++;}
          result = kno_make_int_vector(vlen,ints);
          u8_free(ints); u8_free(scaled);}}
      else if (((FIXNUMP(max))||
                ((KNO_BIGINTP(max))&&
                 (kno_small_bigintp((kno_bigint)max))))&&
               ((FIXNUMP(min))||
                ((KNO_BIGINTP(min))&&
                 (kno_small_bigintp((kno_bigint)min))))) {
        kno_int *ints = u8_alloc_n(vlen,kno_int);
        int j = 0; while (j<vlen) {
          int elt = kno_getint(scaled[j]);
          ints[j]=elt;
          j++;}
        result = kno_make_int_vector(vlen,ints);
        u8_free(ints); u8_free(scaled);}
      else if (((FIXNUMP(max))||
                ((KNO_BIGINTP(max))&&
                 (kno_modest_bigintp((kno_bigint)max))))&&
               ((FIXNUMP(min))||
                ((KNO_BIGINTP(min))&&
                 (kno_modest_bigintp((kno_bigint)min))))) {
        kno_long *longs = u8_alloc_n(vlen,kno_long);
        int j = 0; while (j<vlen) {
          int elt = kno_getint(scaled[j]);
          longs[j]=elt;
          j++;}
        result = kno_make_long_vector(vlen,longs);
        u8_free(longs); u8_free(scaled);}
      else {
        result = kno_make_vector(vlen,scaled);
        u8_free(scaled);}
      return result;}}
  else return generic_vector_scale(vec,scalar);
}

/* bigint i/o */

static void output_bigint_digit(unsigned char **digits,int digit)
{
  **digits = digit; (*digits)++;
}
static int output_bigint(struct U8_OUTPUT *out,kno_bigint bi,int base)
{
  int n_bytes = kno_bigint_length_in_bytes(bi), n_digits = n_bytes*3;
  unsigned char _digits[128], *digits, *scan;
  if (n_digits>128) scan = digits = u8_alloc_n(n_digits,unsigned char);
  else scan = digits=_digits;
  if (bigint_test(bi) == kno_bigint_less) {
    base = 10; u8_putc(out,'-');}
  kno_bigint_to_digit_stream
    (bi,base,(bigint_consumer)output_bigint_digit,(void *)&scan);
  scan--; while (scan>=digits) {
    int digit = *scan--;
    if (digit<10) u8_putc(out,'0'+digit);
    else if (digit<16) u8_putc(out,'a'+(digit-10));
    else u8_putc(out,'?');}
  if (digits != _digits) u8_free(digits);
  return 1;
}
static int unparse_bigint(struct U8_OUTPUT *out,lispval x)
{
  kno_bigint bi = kno_consptr(kno_bigint,x,kno_bigint_type);
  output_bigint(out,bi,10);
  return 1;
}

static void output_bigint_byte(unsigned char **scan,int digit)
{
  **scan = digit; (*scan)++;
}
static ssize_t write_bigint_dtype(struct KNO_OUTBUF *out,lispval x)
{
  kno_bigint bi = kno_consptr(kno_bigint,x,kno_bigint_type);
  if (kno_bigint_fits_in_word_p(bi,32,1)) {
    /* We'll only get here if fixnums are smaller than 32 bits */
    long fixed = kno_bigint_to_long(bi);
    kno_write_byte(out,dt_fixnum);
    kno_write_4bytes(out,fixed);
    return 5;}
  else {
    ssize_t dtype_size;
    int n_bytes = kno_bigint_length_in_bytes(bi);
    unsigned char _bytes[64], *bytes, *scan;
    if (n_bytes>=64)
      scan = bytes = u8_malloc(n_bytes);
    else scan = bytes=_bytes;
    kno_bigint_to_digit_stream
      (bi,256,(bigint_consumer)output_bigint_byte,(void *)&scan);
    kno_write_byte(out,dt_numeric_package);
    n_bytes = scan-bytes;
    if (n_bytes+1<256) {
      dtype_size = 3+n_bytes+1;
      kno_write_byte(out,dt_small_bigint);
      kno_write_byte(out,(n_bytes+1));}
    else {
      dtype_size = 6+n_bytes+1;
      kno_write_byte(out,dt_bigint);
      kno_write_4bytes(out,(n_bytes+1));}
    if (BIGINT_NEGATIVE_P(bi)) kno_write_byte(out,1);
    else kno_write_byte(out,0);
    scan--; while (scan>=bytes) {
      int digit = *scan--; kno_write_byte(out,digit);}
    if (bytes != _bytes) u8_free(bytes);
    return dtype_size;}
}

static int compare_bigint(lispval x,lispval y,kno_compare_flags flags)
{
  kno_bigint bx = kno_consptr(kno_bigint,x,kno_bigint_type);
  kno_bigint by = kno_consptr(kno_bigint,y,kno_bigint_type);
  enum kno_bigint_comparison cmp = kno_bigint_compare(bx,by);
  switch (cmp) {
  case kno_bigint_greater: return 1;
  case kno_bigint_less: return -1;
  default: return 0;}
}

static kno_bigint bigint_magic_modulus;

static int hash_bigint(lispval x,unsigned int (*fn)(lispval))
{
  lispval rem = kno_remainder(x,(lispval)bigint_magic_modulus);
  if (FIXNUMP(rem)) {
    long long irem = FIX2INT(rem);
    if (irem<0) return -irem; else return irem;}
  else if (KNO_BIGINTP(rem)) {
    struct KNO_CONS *brem = (struct KNO_CONS *) rem;
    long irem = kno_bigint_to_long((kno_bigint)rem);
    if (KNO_MALLOCD_CONSP(brem)) u8_free(brem);
    if (irem<0) return -irem; else return irem;}
  else return kno_err(_("bad bigint"),"hash_bigint",NULL,x);
}

static int read_bigint_byte(unsigned char **data)
{
  int val = **data; (*data)++;
  return val;
}
static lispval unpack_bigint(ssize_t n,unsigned char *packet)
{
  int n_digits = n-1; unsigned char *scan = packet+1;
  kno_bigint bi = kno_digit_stream_to_bigint
    (n_digits,(bigint_producer)read_bigint_byte,(void *)&scan,256,packet[0]);
  u8_free(packet);
  if (bi) {
    if (n_digits>8)
      return (lispval)bi;
    else return simplify_bigint(bi);}
  else return VOID;
}

/* Parsing numbers */

static lispval parse_bigint(u8_string string,int base,int negativep);
static lispval make_complex(lispval real,lispval imag);
static lispval make_rational(lispval n,lispval d);

KNO_EXPORT
lispval kno_string2number(u8_string string,int base)
{
  int len = strlen(string);
  if (string[0]=='#') {
    switch (string[1]) {
    case 'o': case 'O':
      return kno_string2number(string+2,8);
    case 'x': case 'X':
      return kno_string2number(string+2,16);
    case 'd': case 'D':
      return kno_string2number(string+2,10);
    case 'b': case 'B':
      return kno_string2number(string+2,2);
    case 'i': case 'I': {
      lispval result = kno_string2number(string+2,base);
	if (PRED_TRUE(NUMBERP(result))) {
	  double dbl = todouble(result);
	  lispval inexresult = kno_init_flonum(NULL,dbl);
	  kno_decref(result);
	  return inexresult;}
	else return KNO_FALSE;}
    case 'e': case 'E': {
      if (strchr(string,'.')) {
	lispval num, den = KNO_INT(1);
	u8_byte *copy = u8_strdup(string+2);
	u8_byte *dot = strchr(copy,'.'), *scan = dot+1;
	*dot='\0'; num = kno_string2number(copy,10);
	if (PRED_FALSE(!(NUMBERP(num)))) {
	  u8_free(copy);
	  return KNO_FALSE;}
	while (*scan)
	  if (isdigit(*scan)) {
	    lispval numx10 = kno_multiply(num,KNO_INT(10));
	    int uchar = *scan;
	    int numweight = u8_digit_weight(uchar);
	    lispval add_digit = KNO_INT(numweight);
	    lispval nextnum = kno_plus(numx10,add_digit);
	    lispval nextden = kno_multiply(den,KNO_INT(10));
	    kno_decref(numx10); kno_decref(num); kno_decref(den);
	    num = nextnum; den = nextden;
	    scan++;}
	  else if (strchr("sSeEfFlL",*scan)) {
	    int i = 0, exponent = strtol(scan+1,NULL,10);
	    if (exponent>=0)
	      while (i<exponent) {
		lispval nextnum = kno_multiply(num,KNO_INT(10));
		kno_decref(num); num = nextnum; i++;}
	    else {
	      exponent = -exponent;
	      while (i<exponent) {
		lispval nextden = kno_multiply(den,KNO_INT(10));
		kno_decref(den); den = nextden; i++;}}
	    break;}
	  else {
	    kno_seterr3(kno_InvalidNumericLiteral,"kno_string2number",string);
	    return KNO_PARSE_ERROR;}
	return make_rational(num,den);}
      else return kno_string2number(string+2,base);}
    default:
      kno_seterr3(kno_InvalidNumericLiteral,"kno_string2number",string);
      return KNO_PARSE_ERROR;}}
  else if ((strchr(string,'i')) || (strchr(string,'I'))) {
    u8_byte *copy = u8_strdup(string);
    u8_byte *iend, *istart; lispval real, imag;
    iend = strchr(copy,'i');
    if (iend == NULL) iend = strchr(copy,'I');
    if (iend[1]!='\0') {u8_free(copy); return KNO_FALSE;}
    else *iend='\0';
    if ((*copy == '+') || (*copy =='-')) {
      istart = strchr(copy+1,'+');
      if (istart == NULL) istart = strchr(copy+1,'-');}
    else {
      istart = strchr(copy,'+');
      if (istart == NULL) istart = strchr(copy,'-');}
    if ((istart) && ((istart[-1]=='e') || (istart[-1]=='E'))) {
      u8_byte *estart = istart;
      istart = strchr(estart+1,'+');
      if (istart == NULL) istart = strchr(estart+1,'-');}
    if (istart == NULL) {
      imag = kno_string2number(copy,base);
      if (PRED_FALSE(!(NUMBERP(imag)))) {
	kno_decref(imag); u8_free(copy); return KNO_FALSE;}
      real = KNO_INT(0);}
    else {
      imag = kno_string2number(istart,base); *istart='\0';
      if (PRED_FALSE(!(NUMBERP(imag)))) {
	kno_decref(imag); u8_free(copy); return KNO_FALSE;}
      real = kno_string2number(copy,base);
      if (PRED_FALSE(!(NUMBERP(real)))) {
	kno_decref(imag); kno_decref(real); u8_free(copy);
	return KNO_FALSE;}}
    u8_free(copy);
    return make_complex(real,imag);}
  else if (strchr(string,'/')) {
    u8_byte *copy = u8_strdup(string);
    u8_byte *slash = strchr(copy,'/');
    lispval num, denom;
    *slash='\0';
    num = kno_string2number(copy,base);
    if (FALSEP(num)) {u8_free(copy); return KNO_FALSE;}
    denom = kno_string2number(slash+1,base);
    if (FALSEP(denom)) {
      kno_decref(num); u8_free(copy); return KNO_FALSE;}
    u8_free(copy);
    return make_rational(num,denom);}
  else if (string[0]=='\0') return KNO_FALSE;
  else if (((string[0]=='+')|| (string[0]=='-') ||
            (isdigit(string[0])) ||
            ((string[0]=='.') && (isdigit(string[1])))) &&
           (strchr(string,'.'))) {
    double flonum; u8_byte *end = NULL;
    flonum = strtod(string,(char **)&end);
    U8_CLEAR_ERRNO();
    if ((end>string) && ((end-string) == len))
      return kno_make_flonum(flonum);
    else return KNO_FALSE;}
  else if (strchr(string+1,'+'))
    return KNO_FALSE;
  else if (strchr(string+1,'-'))
    return KNO_FALSE;
  else {
    lispval result;
    long long fixnum, nbase = 0;
    const u8_byte *start = string, *end = NULL;
    if (string[0]=='0') {
      if (string[1]=='\0') return KNO_INT(0);
      else if ((string[1]=='x') || (string[1]=='X')) {
	start = string+2; nbase = 16;}}
    if ((base<0) && (nbase)) base = nbase;
    else if (base<0) base = 10;
    errno = 0;
    fixnum = strtoll(start,(char **)&end,base);
    U8_CLEAR_ERRNO();
    if (!((end>string) && ((end-string) == len)))
      return KNO_FALSE;
    else if ((fixnum) && 
             ((fixnum<KNO_MAX_FIXNUM) && (fixnum>KNO_MIN_FIXNUM)))
      return KNO_INT(fixnum);
    else if ((fixnum==0) && (end) && (*end=='\0'))
      return KNO_INT(0);
    else if ((errno) && (errno != ERANGE)) return KNO_FALSE;
    else errno = 0;
    if (!(base)) base = 10;
    if (*string =='-')
      result = parse_bigint(string+1,base,1);
    else if (*string =='+')
      result = parse_bigint(string+1,base,0);
    else result = parse_bigint(start,base,0);
    return result;}
}

lispval (*_kno_parse_number)(u8_string,int) = kno_string2number;

static int read_digit_weight(const u8_byte **scan)
{
  int c = u8_sgetc(scan), wt = c-'0';
  if ((wt>=0) && (wt<10)) return wt;
  wt = c-'a'; if ((wt>=0) && (wt<6)) return wt+10;
  wt = c-'A'; if ((wt>=0) && (wt<6)) return wt+10;
  return -1;
}

static lispval parse_bigint(u8_string string,int base,int negative)
{
  int n_digits = u8_strlen(string);
  const u8_byte *scan = string;
  if (n_digits) {
    kno_bigint bi = kno_digit_stream_to_bigint
      (n_digits,(bigint_producer)read_digit_weight,(void *)&scan,base,negative);
    if (bi) return (lispval)bi;
    else return VOID;}
  else return VOID;
}

KNO_EXPORT
int kno_output_number(u8_output out,lispval num,int base)
{
  if (FIXNUMP(num)) {
    long long fixnum = FIX2INT(num);
    if (base==10) u8_printf(out,"%lld",fixnum);
    else if (base==16) u8_printf(out,"%llx",fixnum);
    else if (base == 8) u8_printf(out,"%llo",fixnum);
    else {
      kno_bigint bi = kno_long_to_bigint(fixnum);
      output_bigint(out,bi,base);
      return 1;}
    return 1;}
  else if (KNO_FLONUMP(num)) {
    unsigned char buf[256];
    struct KNO_FLONUM *d = kno_consptr(struct KNO_FLONUM *,num,kno_flonum_type);
    sprintf(buf,"%f",d->floval);
    u8_puts(out,buf);
    return 1;}
  else if (KNO_BIGINTP(num)) {
    kno_bigint bi = kno_consptr(kno_bigint,num,kno_bigint_type);
    output_bigint(out,bi,base);
    return 1;}
  else return 0;
}


/* Initialization stuff */

void kno_init_numbers_c()
{
  if (kno_unparsers[kno_flonum_type] == NULL)
    kno_unparsers[kno_flonum_type]=unparse_flonum;
  if (kno_unparsers[kno_bigint_type] == NULL)
    kno_unparsers[kno_bigint_type]=unparse_bigint;

  kno_unparsers[kno_rational_type]=unparse_rational;
  kno_unparsers[kno_complex_type]=unparse_complex;
  kno_unparsers[kno_numeric_vector_type]=unparse_numeric_vector;

  kno_copiers[kno_flonum_type]=copy_flonum;
  kno_copiers[kno_bigint_type]=copy_bigint;
  kno_copiers[kno_numeric_vector_type]=copy_numeric_vector;

  kno_recyclers[kno_flonum_type]=recycle_flonum;
  kno_recyclers[kno_bigint_type]=recycle_bigint;
  kno_recyclers[kno_numeric_vector_type]=recycle_numeric_vector;

  kno_comparators[kno_flonum_type]=compare_flonum;
  kno_comparators[kno_bigint_type]=compare_bigint;
  kno_comparators[kno_numeric_vector_type]=compare_numeric_vector;

  kno_hashfns[kno_bigint_type]=hash_bigint;
  kno_hashfns[kno_flonum_type]=hash_flonum;
  kno_hashfns[kno_numeric_vector_type]=hash_numeric_vector;

  kno_dtype_writers[kno_bigint_type]=write_bigint_dtype;
  kno_dtype_writers[kno_flonum_type]=write_flonum_dtype;

  kno_register_packet_unpacker
    (dt_numeric_package,dt_double,unpack_flonum);
  kno_register_packet_unpacker
    (dt_numeric_package,dt_bigint,unpack_bigint);

  bigint_magic_modulus = kno_long_to_bigint(256001281);

#if KNO_AVOID_MACROS
  kno_max_fixnum = KNO_INT(KNO_MAX_FIXNUM);
  kno_min_fixnum = KNO_INT(KNO_MIN_FIXNUM);
#endif

#define ONEK ((unsigned long long)1024)

  kno_add_constname("#tib",KNO_INT((ONEK)*(ONEK)*(ONEK)*(ONEK)));
  kno_add_constname("#1tib",KNO_INT((ONEK)*(ONEK)*(ONEK)*(ONEK)));
  kno_add_constname("#2tib",KNO_INT((2)*(ONEK)*(ONEK)*(ONEK)*(ONEK)));
  kno_add_constname("#3tib",KNO_INT((3)*(ONEK)*(ONEK)*(ONEK)*(ONEK)));
  kno_add_constname("#4tib",KNO_INT((3)*(ONEK)*(ONEK)*(ONEK)*(ONEK)));
  kno_add_constname("#pib",KNO_INT((ONEK)*(ONEK)*(ONEK)*(ONEK)*(ONEK)));

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
