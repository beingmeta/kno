/* -*-C-*-

    Copyright (c) 1989-1993 Massachusetts Institute of Technology
    Copyright (c) 1993-2001 Massachusetts Institute of Technology
    Copyright (c) 2001-2016 beingmeta, inc.

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

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/bigints.h"
#include "framerd/numbers.h"
#include "framerd/hash.h"

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

fd_exception fd_BigIntException=_("BigInt Exception");
fd_exception fd_DivideByZero=_("Division by zero");
fd_exception fd_InvalidNumericLiteral=_("Invalid numeric literal");

/* These macros come from the original MIT Scheme code */
#define	DEFUN(name, arglist, args)	name(args)
#define	DEFUN_VOID(name)		name()
#define	AND		,

typedef void (*bigint_consumer)(void *,int);
typedef unsigned int (*bigint_producer)(void *);

static double todouble(fdtype x);

static int numvec_length(fdtype x);
static fdtype vector_add(fdtype x,fdtype y,int mult);
static fdtype vector_dotproduct(fdtype x,fdtype y);
static fdtype vector_scale(fdtype vec,fdtype scalar);

int fd_numvec_showmax=7;


static fd_bigint
DEFUN (bigint_malloc, (length), int length)
{
  char * result = (malloc (sizeof(struct FD_CONS)+((length + 1) * (sizeof (bigint_digit_type)))));
  BIGINT_ASSERT (result != ((char *) 0));
  FD_INIT_CONS(((struct FD_CONS *)result),fd_bigint_type);
  return ((fd_bigint) result);
}

static fd_bigint
DEFUN (bigint_realloc, (bigint, length),
       fd_bigint bigint AND int length)
{
  char * result =
    (realloc (((char *) bigint),
	      ((length + 2) * (sizeof (bigint_digit_type)))));
  BIGINT_ASSERT (result != ((char *) 0));
  return ((fd_bigint) result);
}

/* Forward references */
static int bigint_equal_p_unsigned(fd_bigint, fd_bigint);
static enum fd_bigint_comparison bigint_compare_unsigned(fd_bigint, fd_bigint);
static fd_bigint bigint_add_unsigned(fd_bigint, fd_bigint, int);
static fd_bigint bigint_subtract_unsigned(fd_bigint, fd_bigint, int);
static fd_bigint bigint_multiply_unsigned(fd_bigint, fd_bigint, int);
static fd_bigint bigint_multiply_unsigned_small_factor(fd_bigint, bigint_digit_type, int);
static void bigint_destructive_scale_up(fd_bigint, bigint_digit_type);
static void bigint_destructive_add(fd_bigint, bigint_digit_type);
static void bigint_divide_unsigned_large_denominator
  (fd_bigint, fd_bigint, fd_bigint *,
   fd_bigint *,int, int);
static void bigint_destructive_normalization
  (fd_bigint, fd_bigint, int);
static void bigint_destructive_unnormalization(fd_bigint, int);
static void bigint_divide_unsigned_normalized
  (fd_bigint, fd_bigint, fd_bigint);
static bigint_digit_type bigint_divide_subtract
  (bigint_digit_type *, bigint_digit_type *,
   bigint_digit_type, bigint_digit_type *);
static void bigint_divide_unsigned_medium_denominator
  (fd_bigint, bigint_digit_type, fd_bigint *,
   fd_bigint *, int, int);
static bigint_digit_type bigint_digit_divide
  (bigint_digit_type, bigint_digit_type,
   bigint_digit_type, bigint_digit_type *);
static bigint_digit_type bigint_digit_divide_subtract
  (bigint_digit_type, bigint_digit_type,
   bigint_digit_type, bigint_digit_type *);
static void bigint_divide_unsigned_small_denominator
  (fd_bigint, bigint_digit_type, fd_bigint *,
   fd_bigint *, int, int);
static bigint_digit_type bigint_destructive_scale_down(fd_bigint, bigint_digit_type);
static fd_bigint bigint_remainder_unsigned_small_denominator(fd_bigint, bigint_digit_type, int);
static fd_bigint bigint_digit_to_bigint(bigint_digit_type, int);
static fd_bigint bigint_allocate(int, int);
static fd_bigint bigint_allocate_zeroed(int, int);
static fd_bigint bigint_shorten_length(fd_bigint, int);
static fd_bigint bigint_trim(fd_bigint);
static fd_bigint bigint_copy(fd_bigint);
static fd_bigint bigint_new_sign(fd_bigint, int);
static fd_bigint bigint_maybe_new_sign(fd_bigint, int);
static void bigint_destructive_copy(fd_bigint, fd_bigint);


/* Exports */

fd_bigint
DEFUN_VOID (bigint_make_zero)
{
  fast fd_bigint result = (BIGINT_ALLOCATE (0));
  BIGINT_SET_HEADER (result, 0, 0);
  return (result);
}

fd_bigint
DEFUN (bigint_make_one, (negative_p), int negative_p)
{
  fast fd_bigint result = (BIGINT_ALLOCATE (1));
  BIGINT_SET_HEADER (result, 1, negative_p);
  (BIGINT_REF (result, 0)) = 1;
  return (result);
}

int
DEFUN (bigint_equal_p, (x, y),
       fast fd_bigint x AND fast fd_bigint y)
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

enum fd_bigint_comparison
DEFUN (bigint_test, (bigint), fast fd_bigint bigint)
{
  return
    ((BIGINT_ZERO_P (bigint))
     ? fd_bigint_equal
     : (BIGINT_NEGATIVE_P (bigint))
     ? fd_bigint_less
     : fd_bigint_greater);
}

enum fd_bigint_comparison
DEFUN (fd_bigint_compare, (x, y),
       fast fd_bigint x AND fast fd_bigint y)
{
  return
    ((BIGINT_ZERO_P (x))
     ? ((BIGINT_ZERO_P (y))
	? fd_bigint_equal
	: (BIGINT_NEGATIVE_P (y))
	? fd_bigint_greater
	: fd_bigint_less)
     : (BIGINT_ZERO_P (y))
     ? ((BIGINT_NEGATIVE_P (x))
	? fd_bigint_less
	: fd_bigint_greater)
     : (BIGINT_NEGATIVE_P (x))
     ? ((BIGINT_NEGATIVE_P (y))
	? (bigint_compare_unsigned (y, x))
	: (fd_bigint_less))
     : ((BIGINT_NEGATIVE_P (y))
	? (fd_bigint_greater)
	: (bigint_compare_unsigned (x, y))));
}

fd_bigint
DEFUN (fd_bigint_add, (x, y),
       fast fd_bigint x AND fast fd_bigint y)
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

fd_bigint
DEFUN (fd_bigint_subtract, (x, y),
       fast fd_bigint x AND fast fd_bigint y)
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

fd_bigint
DEFUN (fd_bigint_negate, (x), fast fd_bigint x)
{
  return
    ((BIGINT_ZERO_P (x))
     ? (BIGINT_MAYBE_COPY (x))
     : (bigint_new_sign (x, (! (BIGINT_NEGATIVE_P (x))))));
}

fd_bigint
DEFUN (fd_bigint_multiply, (x, y),
       fast fd_bigint x AND fast fd_bigint y)
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
DEFUN (fd_bigint_divide, (numerator, denominator, quotient, remainder),
       fd_bigint numerator AND fd_bigint denominator
       AND fd_bigint * quotient AND fd_bigint * remainder)
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
	case fd_bigint_equal:
	  {
	    (*quotient) = (BIGINT_ONE (q_negative_p));
	    (*remainder) = (BIGINT_ZERO ());
	    break;
	  }
	case fd_bigint_less:
	  {
	    (*quotient) = (BIGINT_ZERO ());
	    (*remainder) = (BIGINT_MAYBE_COPY (numerator));
	    break;
	  }
	case fd_bigint_greater:
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

fd_bigint
DEFUN (fd_bigint_quotient, (numerator, denominator),
       fd_bigint numerator AND fd_bigint denominator)
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
      case fd_bigint_equal:
	return (BIGINT_ONE (q_negative_p));
      case fd_bigint_less:
	return (BIGINT_ZERO ());
      case fd_bigint_greater:
	{
	  fd_bigint quotient;
	  if ((BIGINT_LENGTH (denominator)) == 1)
	    {
	      bigint_digit_type digit = (BIGINT_REF (denominator, 0));
	      if (digit == 1)
		return (bigint_maybe_new_sign (numerator, q_negative_p));
	      if (digit < BIGINT_RADIX_ROOT)
		bigint_divide_unsigned_small_denominator
		  (numerator, digit,
		   (&quotient), ((fd_bigint *) 0),
		   q_negative_p, 0);
	      else
		bigint_divide_unsigned_medium_denominator
		  (numerator, digit,
		   (&quotient), ((fd_bigint *) 0),
		   q_negative_p, 0);
	    }
	  else
	    bigint_divide_unsigned_large_denominator
	      (numerator, denominator,
	       (&quotient), ((fd_bigint *) 0),
	       q_negative_p, 0);
	  return (quotient);
	}
      default:
	return (BIGINT_OUT_OF_BAND);
      }
  }
}

fd_bigint
DEFUN (fd_bigint_remainder, (numerator, denominator),
       fd_bigint numerator AND fd_bigint denominator)
{
  if (BIGINT_ZERO_P (denominator))
    return (BIGINT_OUT_OF_BAND);
  if (BIGINT_ZERO_P (numerator))
    return (BIGINT_MAYBE_COPY (numerator));
  switch (bigint_compare_unsigned (numerator, denominator))
    {
    case fd_bigint_equal:
      return (BIGINT_ZERO ());
    case fd_bigint_less:
      return (BIGINT_MAYBE_COPY (numerator));
    case fd_bigint_greater:
      {
	fd_bigint remainder=0; fd_bigint quotient=0;
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


fd_bigint
DEFUN (fd_long_to_bigint, (n), long n)
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
    fd_bigint result =
      (bigint_allocate ((end_digits - result_digits), negative_p));
    fast bigint_digit_type * scan_digits = result_digits;
    fast bigint_digit_type * scan_result = (BIGINT_START_PTR (result));
    while (scan_digits < end_digits)
      (*scan_result++) = (*scan_digits++);
    return (result);
  }
}

fd_bigint
DEFUN (fd_long_long_to_bigint, (n), long long n)
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
    fd_bigint result =
      (bigint_allocate ((end_digits - result_digits), negative_p));
    fast bigint_digit_type * scan_digits = result_digits;
    fast bigint_digit_type * scan_result = (BIGINT_START_PTR (result));
    while (scan_digits < end_digits)
      (*scan_result++) = (*scan_digits++);
    return (result);
  }
}


fd_bigint
DEFUN (fd_ulong_to_bigint, (n), unsigned long n)
{
  int negative_p=0;
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
    fd_bigint result =
      (bigint_allocate ((end_digits - result_digits), negative_p));
    fast bigint_digit_type * scan_digits = result_digits;
    fast bigint_digit_type * scan_result = (BIGINT_START_PTR (result));
    while (scan_digits < end_digits)
      (*scan_result++) = (*scan_digits++);
    return (result);
  }
}

fd_bigint
DEFUN (fd_ulong_long_to_bigint, (n), unsigned long long n)
{
  int negative_p=0;
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
    fd_bigint result =
      (bigint_allocate ((end_digits - result_digits), negative_p));
    fast bigint_digit_type * scan_digits = result_digits;
    fast bigint_digit_type * scan_result = (BIGINT_START_PTR (result));
    while (scan_digits < end_digits)
      (*scan_result++) = (*scan_digits++);
    return (result);
  }
}

long
DEFUN (fd_bigint_to_long, (bigint), fd_bigint bigint)
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
DEFUN (fd_bigint_negativep, (bigint), fd_bigint bigint)
{
  if (BIGINT_NEGATIVE_P (bigint)) return (1);
  else return (0);
}

long long
DEFUN (fd_bigint_to_long_long, (bigint), fd_bigint bigint)
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
DEFUN (fd_bigint_to_ulong_long, (bigint), fd_bigint bigint)
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
DEFUN (fd_bigint_to_ulong, (bigint), fd_bigint bigint)
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

fd_bigint
DEFUN (fd_double_to_bigint, (x), double x)
{
  extern double frexp ();
  int exponent;
  fast double significand = (frexp (x, (&exponent)));
  if (exponent <= 0) return (BIGINT_ZERO ());
  if (exponent == 1) return (BIGINT_ONE (x < 0));
  if (significand < 0) significand = (-significand);
  {
    int length = (BIGINT_BITS_TO_DIGITS (exponent));
    fd_bigint result = (bigint_allocate (length, (x < 0)));
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
DEFUN (fd_bigint_to_double, (bigint), fd_bigint bigint)
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
DEFUN (fd_bigint_fits_in_word_p, (bigint, word_length, twos_complement_p),
       fd_bigint bigint AND long word_length AND int twos_complement_p)
{
  unsigned int n_bits = (twos_complement_p ? (word_length - 1) : word_length);
  BIGINT_ASSERT (n_bits > 0);
  {
    fast int length = (BIGINT_LENGTH (bigint));
    fast int max_digits = (BIGINT_BITS_TO_DIGITS (n_bits));
#if 0
    bigint_digit_type msd, max;
    msd = (BIGINT_REF (bigint, (length - 1)));
    max = (1L << (n_bits - ((length - 1) * BIGINT_DIGIT_LENGTH)));
#endif
    return
      ((length < max_digits) ||
       ((length == max_digits) &&
	((BIGINT_REF (bigint, (length - 1))) <
	 (1L << (n_bits - ((length - 1) * BIGINT_DIGIT_LENGTH))))));
  }
}

fd_bigint
DEFUN (fd_bigint_length_in_bits, (bigint), fd_bigint bigint)
{
  if (BIGINT_ZERO_P (bigint))
    return (BIGINT_ZERO ());
  {
    int index = ((BIGINT_LENGTH (bigint)) - 1);
    fast bigint_digit_type digit = (BIGINT_REF (bigint, index));
    fast fd_bigint result = (bigint_allocate (2, 0));
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
DEFUN (fd_bigint_length_in_bytes, (bigint), fd_bigint bigint)
{
  if (BIGINT_ZERO_P (bigint))
    return 0;
  else return BIGINT_LENGTH(bigint)*sizeof(bigint_digit_type);
}

fd_bigint
DEFUN_VOID (bigint_length_upper_limit)
{
  fast fd_bigint result = (bigint_allocate (2, 0));
  (BIGINT_REF (result, 0)) = 0;
  (BIGINT_REF (result, 1)) = BIGINT_DIGIT_LENGTH;
  return (result);
}

fd_bigint
DEFUN (fd_digit_stream_to_bigint,
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
      else return (fd_long_to_bigint (negative_p ? (- digit) : digit));
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
      fast fd_bigint result = (bigint_allocate_zeroed (length, negative_p));
      while ((n_digits--) > 0)
	{
	  int digit=producer(context);
	  if ((digit<0) || (digit>radix)) {
	    fd_decref((fdtype)result);
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
DEFUN (fd_bigint_to_digit_stream, (bigint, radix, consumer, context),
       fd_bigint bigint
       AND unsigned int radix
       AND void (*consumer)(void *, int)
       AND void *context)
{
  BIGINT_ASSERT ((radix > 1) && (radix <= BIGINT_RADIX_ROOT));
  if (! (BIGINT_ZERO_P (bigint)))
    {
      fast fd_bigint working_copy = (bigint_copy (bigint));
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
DEFUN_VOID (fd_bigint_max_digit_stream_radix)
{
  return (BIGINT_RADIX_ROOT);
}

/* Comparisons */

static int
DEFUN (bigint_equal_p_unsigned, (x, y),
       fd_bigint x AND fd_bigint y)
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

static enum fd_bigint_comparison
DEFUN (bigint_compare_unsigned, (x, y),
       fd_bigint x AND fd_bigint y)
{
  int x_length = (BIGINT_LENGTH (x));
  int y_length = (BIGINT_LENGTH (y));
  if (x_length < y_length)
    return (fd_bigint_less);
  if (x_length > y_length)
    return (fd_bigint_greater);
  {
    fast bigint_digit_type * start_x = (BIGINT_START_PTR (x));
    fast bigint_digit_type * scan_x = (start_x + x_length);
    fast bigint_digit_type * scan_y = ((BIGINT_START_PTR (y)) + y_length);
    while (start_x < scan_x)
      {
	fast bigint_digit_type digit_x = (*--scan_x);
	fast bigint_digit_type digit_y = (*--scan_y);
	if (digit_x < digit_y)
	  return (fd_bigint_less);
	if (digit_x > digit_y)
	  return (fd_bigint_greater);
      }
  }
  return (fd_bigint_equal);
}

/* Addition */

static fd_bigint
DEFUN (bigint_add_unsigned, (x, y, negative_p),
       fd_bigint x AND fd_bigint y AND int negative_p)
{
  if ((BIGINT_LENGTH (y)) > (BIGINT_LENGTH (x)))
    {
      fd_bigint z = x;
      x = y;
      y = z;
    }
  {
    int x_length = (BIGINT_LENGTH (x));
    fd_bigint r = (bigint_allocate ((x_length + 1), negative_p));
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

static fd_bigint
DEFUN (bigint_subtract_unsigned, (x, y),
       fd_bigint x AND fd_bigint y AND int make_negative)
{
  int negative_p=0;
  switch (bigint_compare_unsigned (x, y))
    {
    case fd_bigint_equal:
      return (BIGINT_ZERO ());
    case fd_bigint_less:
      {
	fd_bigint z = x;
	x = y;
	y = z;
      }
      if (make_negative)
	negative_p = 0;
      else negative_p = 1;
      break;
    case fd_bigint_greater:
      if (make_negative)
	negative_p = 1;
      else negative_p = 0;
      break;
    }
  
  {
    int x_length = (BIGINT_LENGTH (x));
    fd_bigint r = (bigint_allocate (x_length, negative_p));
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

static fd_bigint
DEFUN (bigint_multiply_unsigned, (x, y, negative_p),
       fd_bigint x AND fd_bigint y AND int negative_p)
{
  if ((BIGINT_LENGTH (y)) > (BIGINT_LENGTH (x)))
    {
      fd_bigint z = x;
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
    fd_bigint r =
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

static fd_bigint
DEFUN (bigint_multiply_unsigned_small_factor, (x, y, negative_p),
       fd_bigint x AND bigint_digit_type y AND int negative_p)
{
  int length_x = (BIGINT_LENGTH (x));
  fd_bigint p = (bigint_allocate ((length_x + 1), negative_p));
  bigint_destructive_copy (x, p);
  (BIGINT_REF (p, length_x)) = 0;
  bigint_destructive_scale_up (p, y);
  return (bigint_trim (p));
}

static void
DEFUN (bigint_destructive_scale_up, (bigint, factor),
       fd_bigint bigint AND bigint_digit_type factor)
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
       fd_bigint bigint AND bigint_digit_type n)
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
       fd_bigint numerator
       AND fd_bigint denominator
       AND fd_bigint * quotient
       AND fd_bigint * remainder
       AND int q_negative_p
       AND int r_negative_p)
{
  int length_n = ((BIGINT_LENGTH (numerator)) + 1);
  int length_d = (BIGINT_LENGTH (denominator));
  fd_bigint q =
    ((quotient != ((fd_bigint *) 0))
     ? (bigint_allocate ((length_n - length_d), q_negative_p))
     : BIGINT_OUT_OF_BAND);
  fd_bigint u = (bigint_allocate (length_n, r_negative_p));
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
      fd_bigint v = (bigint_allocate (length_d, 0));
      bigint_destructive_normalization (numerator, u, shift);
      bigint_destructive_normalization (denominator, v, shift);
      bigint_divide_unsigned_normalized (u, v, q);
      BIGINT_DEALLOCATE (v);
      if (remainder != ((fd_bigint *) 0))
	bigint_destructive_unnormalization (u, shift);
    }
  if (quotient != ((fd_bigint *) 0))
    (*quotient) = (bigint_trim (q));
  if (remainder != ((fd_bigint *) 0))
    (*remainder) = (bigint_trim (u));
  else
    BIGINT_DEALLOCATE (u);
  return;
}

static void
DEFUN (bigint_divide_unsigned_normalized, (u, v, q),
       fd_bigint u AND fd_bigint v AND fd_bigint q)
{
  int u_length = (BIGINT_LENGTH (u));
  int v_length = (BIGINT_LENGTH (v));
  bigint_digit_type * u_start = (BIGINT_START_PTR (u));
  bigint_digit_type * u_scan = (u_start + u_length);
  bigint_digit_type * u_scan_limit = (u_start + v_length);
  bigint_digit_type * u_scan_start = (u_scan - v_length);
  bigint_digit_type * v_start = (BIGINT_START_PTR (v));
  bigint_digit_type * v_end = (v_start + v_length);
  bigint_digit_type * q_scan= ((BIGINT_START_PTR (q)) + (BIGINT_LENGTH (q)));
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
       fd_bigint numerator
       AND bigint_digit_type denominator
       AND fd_bigint * quotient
       AND fd_bigint * remainder
       AND int q_negative_p
       AND int r_negative_p)
{
  int length_n = (BIGINT_LENGTH (numerator));
  int length_q;
  fd_bigint q;
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
    if (quotient != ((fd_bigint *) 0))
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
    if (remainder != ((fd_bigint *) 0))
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
       fd_bigint source AND fd_bigint target AND int shift_left)
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
       fd_bigint bigint AND int shift_right)
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
       fd_bigint numerator
       AND bigint_digit_type denominator
       AND fd_bigint * quotient
       AND fd_bigint * remainder
       AND int q_negative_p
       AND int r_negative_p)
{
  fd_bigint q = (bigint_new_sign (numerator, q_negative_p));
  bigint_digit_type r = (bigint_destructive_scale_down (q, denominator));
  (*quotient) = (bigint_trim (q));
  if (remainder != ((fd_bigint *) 0))
    (*remainder) = (bigint_digit_to_bigint (r, r_negative_p));
  return;
}

/* Given (denominator > 1), it is fairly easy to show that
   (quotient_high < BIGINT_RADIX_ROOT), after which it is easy to see
   that all digits are < BIGINT_RADIX. */

static bigint_digit_type
DEFUN (bigint_destructive_scale_down, (bigint, denominator),
       fd_bigint bigint AND fast bigint_digit_type denominator)
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

static fd_bigint
DEFUN (bigint_remainder_unsigned_small_denominator, (n, d, negative_p),
       fd_bigint n AND bigint_digit_type d AND int negative_p)
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

static fd_bigint
DEFUN (bigint_digit_to_bigint, (digit, negative_p),
       fast bigint_digit_type digit AND int negative_p)
{
  if (digit == 0)
    return (BIGINT_ZERO ());
  else
    {
      fast fd_bigint result = (bigint_allocate (1, negative_p));
      (BIGINT_REF (result, 0)) = digit;
      return (result);
    }
}

/* Allocation */

static fd_bigint
DEFUN (bigint_allocate, (length, negative_p),
       fast int length AND int negative_p)
{
  BIGINT_ASSERT ((length >= 0) || (length < BIGINT_RADIX));
  {
    fast fd_bigint result = (BIGINT_ALLOCATE (length));
    BIGINT_SET_HEADER (result, length, negative_p);
    return (result);
  }
}

static fd_bigint
DEFUN (bigint_allocate_zeroed, (length, negative_p),
       fast int length AND int negative_p)
{
  BIGINT_ASSERT ((length >= 0) || (length < BIGINT_RADIX));
  {
    fast fd_bigint result = (BIGINT_ALLOCATE (length));
    fast bigint_digit_type * scan = (BIGINT_START_PTR (result));
    fast bigint_digit_type * end = (scan + length);
    BIGINT_SET_HEADER (result, length, negative_p);
    while (scan < end)
      (*scan++) = 0;
    return (result);
  }
}

static fd_bigint
DEFUN (bigint_shorten_length, (bigint, length),
       fast fd_bigint bigint AND fast int length)
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

static fd_bigint
DEFUN (bigint_trim, (bigint), fd_bigint bigint)
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

static fd_bigint
DEFUN (bigint_copy, (source), fast fd_bigint source)
{
  fast fd_bigint target =
    (bigint_allocate ((BIGINT_LENGTH (source)), (BIGINT_NEGATIVE_P (source))));
  bigint_destructive_copy (source, target);
  return (target);
}

static fd_bigint
DEFUN (bigint_new_sign, (bigint, negative_p),
       fast fd_bigint bigint AND int negative_p)
{
  fast fd_bigint result =
    (bigint_allocate ((BIGINT_LENGTH (bigint)), negative_p));
  bigint_destructive_copy (bigint, result);
  return (result);
}

static fd_bigint
DEFUN (bigint_maybe_new_sign, (bigint, negative_p),
       fast fd_bigint bigint AND int negative_p)
{
#ifndef BIGINT_FORCE_NEW_RESULTS
  if ((BIGINT_NEGATIVE_P (bigint)) ? negative_p : (! negative_p))
    return (bigint);
  else
#endif /* not BIGINT_FORCE_NEW_RESULTS */
    {
      fast fd_bigint result =
	(bigint_allocate ((BIGINT_LENGTH (bigint)), negative_p));
      bigint_destructive_copy (bigint, result);
      return (result);
    }
}

static void
DEFUN (bigint_destructive_copy, (source, target),
       fd_bigint source AND fd_bigint target)
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

FD_EXPORT fdtype fd_make_bigint(long long intval)
{
  if ((intval>FD_MAX_FIXNUM) || (intval<FD_MIN_FIXNUM))
    return (fdtype) fd_long_long_to_bigint(intval);
  else if (intval>=0)
    return (fd_fixnum_type|((intval)<<2));
  else
    return (fd_fixnum_type|FD_FIXNUM_SIGN_BIT|((-(intval))<<2));
}

static fdtype copy_bigint(fdtype x,int deep)
{
  fd_bigint bi=FD_GET_CONS(x,fd_bigint_type,fd_bigint);
  return FDTYPE_CONS(bigint_copy(bi));
}

static void output_bigint_digit(unsigned char **digits,int digit)
{
  **digits=digit; (*digits)++;
}
static int output_bigint(struct U8_OUTPUT *out,fd_bigint bi,int base)
{
  int n_bytes=fd_bigint_length_in_bytes(bi), n_digits=n_bytes*3;
  unsigned char _digits[128], *digits, *scan;
  if (n_digits>128) scan=digits=u8_alloc_n(n_digits,unsigned char);
  else scan=digits=_digits;
  if (bigint_test(bi)==fd_bigint_less) {
    base=10; u8_putc(out,'-');}
  fd_bigint_to_digit_stream
    (bi,base,(bigint_consumer)output_bigint_digit,(void *)&scan);
  scan--; while (scan>=digits) {
    int digit=*scan--;
    if (digit<10) u8_putc(out,'0'+digit);
    else if (digit<16) u8_putc(out,'a'+(digit-10));
    else u8_putc(out,'?');}
  if (digits != _digits) u8_free(digits);
  return 1;
}
static int unparse_bigint(struct U8_OUTPUT *out,fdtype x)
{
  fd_bigint bi=FD_GET_CONS(x,fd_bigint_type,fd_bigint);
  output_bigint(out,bi,10);
  return 1;
}

static void output_bigint_byte(unsigned char **scan,int digit)
{
  **scan=digit; (*scan)++;
}
static int dtype_bigint(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  fd_bigint bi=FD_GET_CONS(x,fd_bigint_type,fd_bigint);
  if (fd_bigint_fits_in_word_p(bi,32,1)) {
    long fixed=fd_bigint_to_long(bi);
    fd_write_byte(out,dt_fixnum);
    fd_write_4bytes(out,fixed);
    return 5;}
  else {
    int n_bytes=fd_bigint_length_in_bytes(bi), dtype_size;
    unsigned char _bytes[64], *bytes, *scan;
    if (n_bytes>=64) scan=bytes=u8_malloc(n_bytes);
    else scan=bytes=_bytes;
    fd_bigint_to_digit_stream
      (bi,256,(bigint_consumer)output_bigint_byte,(void *)&scan);
    fd_write_byte(out,dt_numeric_package);
    n_bytes=scan-bytes;
    if (n_bytes+1<256) {
      dtype_size=3+n_bytes+1;
      fd_write_byte(out,dt_small_bigint);
      fd_write_byte(out,(n_bytes+1));}
    else {
      dtype_size=6+n_bytes+1;
      fd_write_byte(out,dt_bigint);
      fd_write_4bytes(out,(n_bytes+1));}
    if (BIGINT_NEGATIVE_P(bi)) fd_write_byte(out,1);
    else fd_write_byte(out,0);
    scan--; while (scan>=bytes) {
      int digit=*scan--; fd_write_byte(out,digit);}
    if (bytes != _bytes) u8_free(bytes);
    return dtype_size;}
}

static int compare_bigint(fdtype x,fdtype y,int f)
{
  fd_bigint bx=FD_GET_CONS(x,fd_bigint_type,fd_bigint);
  fd_bigint by=FD_GET_CONS(y,fd_bigint_type,fd_bigint);
  enum fd_bigint_comparison cmp=fd_bigint_compare(bx,by);
  switch (cmp) {
  case fd_bigint_greater: return 1;
  case fd_bigint_less: return -1;
  default: return 0;}
}

static fd_bigint bigint_magic_modulus;

static int hash_bigint(fdtype x,unsigned int (*fn)(fdtype))
{
  fdtype rem=fd_remainder(x,(fdtype)bigint_magic_modulus);
  if (FD_FIXNUMP(rem)) {
    int irem=FD_FIX2INT(rem);
    if (irem<0) return -irem; else return irem;}
  else if (FD_BIGINTP(rem)) {
    struct FD_CONS *brem=(struct FD_CONS *) rem;
    long irem=fd_bigint_to_long((fd_bigint)rem);
    if (FD_MALLOCD_CONSP(brem)) u8_free(brem);
    if (irem<0) return -irem; else return irem;}
  else return fd_err(_("bad bigint"),"hash_bigint",NULL,x);
}

static int read_bigint_byte(unsigned char **data)
{
  int val=**data; (*data)++;
  return val;
}
static fdtype unpack_bigint(unsigned int n,unsigned char *packet)
{
  int n_digits=n-1; unsigned char *scan=packet+1;
  fd_bigint bi=fd_digit_stream_to_bigint
    (n_digits,(bigint_producer)read_bigint_byte,(void *)&scan,256,packet[0]);
  u8_free(packet);
  if (bi) return (fdtype)bi;
  else return FD_VOID;
}

static void recycle_bigint(struct FD_CONS *c)
{
  if (FD_MALLOCD_CONSP(c)) {
    u8_free(c);}
}

/* Flonums */

FD_EXPORT fdtype fd_init_flonum(struct FD_FLONUM *ptr,double flonum)
{
  if (ptr == NULL) ptr=u8_alloc(struct FD_FLONUM);
  FD_INIT_CONS(ptr,fd_flonum_type);
  ptr->flonum=flonum;
  return FDTYPE_CONS(ptr);
}

FD_EXPORT fdtype fd_init_double(struct FD_FLONUM *ptr,double flonum)
{
  if (ptr == NULL) ptr=u8_alloc(struct FD_FLONUM);
  FD_INIT_CONS(ptr,fd_flonum_type);
  ptr->flonum=flonum;
  return FDTYPE_CONS(ptr);
}

static int unparse_flonum(struct U8_OUTPUT *out,fdtype x)
{
  unsigned char buf[256]; int exp;
  struct FD_FLONUM *d=FD_GET_CONS(x,fd_flonum_type,struct FD_FLONUM *);
  /* Get the exponent */
  frexp(d->flonum,&exp);
  if ((exp<-10) || (exp>20))
    sprintf(buf,"%e",d->flonum);
  else sprintf(buf,"%f",d->flonum);
  u8_puts(out,buf);
  return 1;
}

static fdtype copy_flonum(fdtype x,int deep)
{
  struct FD_FLONUM *d=FD_GET_CONS(x,fd_flonum_type,struct FD_FLONUM *);
  return fd_init_flonum(NULL,d->flonum);
}

static fdtype unpack_flonum(unsigned int n,unsigned char *packet)
{
  unsigned char bytes[8]; double *f=(double *)&bytes;
#if WORDS_BIGENDIAN
  memcpy(bytes,packet,8);
#else
  int i=0; while (i < 8) {bytes[i]=packet[7-i]; i++;}
#endif
  u8_free(packet);
  return fd_init_flonum(NULL,*f);
}

static int dtype_flonum(struct FD_BYTE_OUTPUT *out,fdtype x)
{
  struct FD_FLONUM *d=FD_GET_CONS(x,fd_flonum_type,struct FD_FLONUM *);
  unsigned char bytes[8]; int i=0;
  double *f=(double *)&bytes;
  *f=d->flonum;
  fd_write_byte(out,dt_numeric_package);
  fd_write_byte(out,dt_double);
  fd_write_byte(out,8);
#if WORDS_BIGENDIAN
  while (i<8) {fd_write_byte(out,bytes[i]); i++;}
#else
  i=7; while (i>=0) {fd_write_byte(out,bytes[i]); i--;}
#endif
  return 11;
}

static void recycle_flonum(struct FD_CONS *c)
{
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static int compare_flonum(fdtype x,fdtype y,int f)
{
  struct FD_FLONUM *dx=
    FD_GET_CONS(x,fd_flonum_type,struct FD_FLONUM *);
  struct FD_FLONUM *dy=
    FD_GET_CONS(y,fd_flonum_type,struct FD_FLONUM *);
  if (dx->flonum < dy->flonum) return -1;
  else if (dx->flonum==dy->flonum) return 0;
  else return 1;
}

static int hash_flonum(fdtype x,unsigned int (*fn)(fdtype))
{
  struct FD_FLONUM *dx=
    FD_GET_CONS(x,fd_flonum_type,struct FD_FLONUM *);
  int expt;
  double mantissa=frexp(fabs(dx->flonum),&expt);
  double reformed=
    ((expt<0) ? (ldexp(mantissa,0)) : (ldexp(mantissa,expt)));
  int asint=(int)reformed;
  return asint%256001281;
}

/* Parsing numbers */

static fdtype parse_bigint(u8_string string,int base,int negativep);
static fdtype make_complex(fdtype real,fdtype imag);
static fdtype make_rational(fdtype n,fdtype d);

FD_EXPORT
fdtype fd_string2number(u8_string string,int base)
{
  int len=strlen(string);
  if (string[0]=='#') {
    switch (string[1]) {
    case 'o': case 'O':
      return fd_string2number(string+2,8);
    case 'x': case 'X':
      return fd_string2number(string+2,16);
    case 'd': case 'D':
      return fd_string2number(string+2,10);
    case 'b': case 'B':
      return fd_string2number(string+2,2);
    case 'i': case 'I': {
      fdtype result=fd_string2number(string+2,base);
	if (FD_EXPECT_TRUE(FD_NUMBERP(result))) {
	  double dbl=todouble(result);
	  fdtype inexresult=fd_init_flonum(NULL,dbl);
	  fd_decref(result);
	  return inexresult;}
	else return FD_FALSE;}
    case 'e': case 'E': {
      if (strchr(string,'.')) {
	fdtype num, den=FD_INT(1);
	u8_byte *copy=u8_strdup(string+2);
	u8_byte *dot=strchr(copy,'.'), *scan=dot+1;
	*dot='\0'; num=fd_string2number(copy,10);
	if (FD_EXPECT_FALSE(!(FD_NUMBERP(num)))) {
	  u8_free(copy);
	  return FD_FALSE;}
	while (*scan)
	  if (isdigit(*scan)) {
	    fdtype numx10=fd_multiply(num,FD_INT(10));
	    int uchar=*scan;
	    int numweight=u8_digit_weight(uchar);
	    fdtype add_digit=FD_INT(numweight);
	    fdtype nextnum=fd_plus(numx10,add_digit);
	    fdtype nextden=fd_multiply(den,FD_INT(10));
	    fd_decref(numx10); fd_decref(num); fd_decref(den);
	    num=nextnum; den=nextden;
	    scan++;}
	  else if (strchr("sSeEfFlL",*scan)) {
	    int i=0, exponent=strtol(scan+1,NULL,10);
	    if (exponent>=0)
	      while (i<exponent) {
		fdtype nextnum=fd_multiply(num,FD_INT(10));
		fd_decref(num); num=nextnum; i++;}
	    else {
	      exponent=-exponent;
	      while (i<exponent) {
		fdtype nextden=fd_multiply(den,FD_INT(10));
		fd_decref(den); den=nextden; i++;}}
	    break;}
	  else {
	    fd_seterr3(fd_InvalidNumericLiteral,"fd_string2number",
		       u8_strdup(string));
	    return FD_PARSE_ERROR;}
	return make_rational(num,den);}
      else return fd_string2number(string+2,base);}
    default:
      fd_seterr3(fd_InvalidNumericLiteral,"fd_string2number",
		 u8_strdup(string));
      return FD_PARSE_ERROR;}}
  else if ((strchr(string,'i')) || (strchr(string,'I'))) {
    u8_byte *copy=u8_strdup(string);
    u8_byte *iend, *istart; fdtype real, imag;
    iend=strchr(copy,'i');
    if (iend==NULL) iend=strchr(copy,'I');
    if (iend[1]!='\0') {u8_free(copy); return FD_FALSE;}
    else *iend='\0';
    if ((*copy == '+') || (*copy =='-')) {
      istart=strchr(copy+1,'+');
      if (istart==NULL) istart=strchr(copy+1,'-');}
    else {
      istart=strchr(copy,'+');
      if (istart==NULL) istart=strchr(copy,'-');}
    if ((istart) && ((istart[-1]=='e') || (istart[-1]=='E'))) {
      u8_byte *estart=istart;
      istart=strchr(estart+1,'+');
      if (istart==NULL) istart=strchr(estart+1,'-');}
    if (istart == NULL) {
      imag=fd_string2number(copy,base);
      if (FD_EXPECT_FALSE(!(FD_NUMBERP(imag)))) {
	fd_decref(imag); u8_free(copy); return FD_FALSE;}
      real=FD_INT(0);}
    else {
      imag=fd_string2number(istart,base); *istart='\0';
      if (FD_EXPECT_FALSE(!(FD_NUMBERP(imag)))) {
	fd_decref(imag); u8_free(copy); return FD_FALSE;}
      real=fd_string2number(copy,base);
      if (FD_EXPECT_FALSE(!(FD_NUMBERP(real)))) {
	fd_decref(imag); fd_decref(real); u8_free(copy);
	return FD_FALSE;}}
    u8_free(copy);
    return make_complex(real,imag);}
  else if (strchr(string,'/')) {
    u8_byte *copy=u8_strdup(string);
    u8_byte *slash=strchr(copy,'/');
    fdtype num, denom;
    *slash='\0';
    num=fd_string2number(copy,base);
    if (FD_FALSEP(num)) {u8_free(copy); return FD_FALSE;}
    denom=fd_string2number(slash+1,base);
    if (FD_FALSEP(denom)) {
      fd_decref(num); u8_free(copy); return FD_FALSE;}
    u8_free(copy);
    return make_rational(num,denom);}
  else if (string[0]=='\0') return FD_FALSE;
  else if (((string[0]=='+')|| (string[0]=='-') ||
	    (isdigit(string[0])) ||
	    ((string[0]=='.') && (isdigit(string[1])))) &&
	   (strchr(string,'.'))) {
    double flonum; u8_byte *end=NULL;
    flonum=strtod(string,(char **)&end);
    if ((end>string) && ((end-string)==len)) 
      return fd_make_flonum(flonum);
    else return FD_FALSE;}
  else if (strchr(string+1,'+')) return FD_FALSE;
  else if (strchr(string+1,'-')) return FD_FALSE;
  else {
    fdtype result;
    long fixnum, nbase=0; const u8_byte *start=string, *end=NULL;
    if (string[0]=='0') {
      if (string[1]=='\0') return FD_INT(0);
      else if ((string[1]=='x') || (string[1]=='X')) {
	start=string+2; nbase=16;}}
    if ((base<0) && (nbase)) base=nbase;
    else if (base<0) base=10;
    errno=0;
    fixnum=strtol(start,(char **)&end,base);
    if (!((end>string) && ((end-string)==len)))
      return FD_FALSE;
    else if ((fixnum) && ((fixnum<FD_MAX_FIXNUM) && (fixnum>FD_MIN_FIXNUM))) 
      return FD_INT(fixnum);
    else if ((fixnum==0) && (end) && (*end=='\0'))
      return FD_INT(0);
    else if ((errno) && (errno != ERANGE)) return FD_FALSE;
    else errno=0;
    if (!(base)) base=10;
    if (*string =='-')
      result=parse_bigint(string+1,base,1);
    else if (*string =='+')
      result=parse_bigint(string+1,base,0);
    else result=parse_bigint(start,base,0);
    return result;}
}

fdtype (*_fd_parse_number)(u8_string,int)=fd_string2number;

static int read_digit_weight(const u8_byte **scan)
{
  int c=u8_sgetc(scan), wt=c-'0';
  if ((wt>=0) && (wt<10)) return wt;
  wt=c-'a'; if ((wt>=0) && (wt<6)) return wt+10;
  wt=c-'A'; if ((wt>=0) && (wt<6)) return wt+10;
  return -1;
}

static fdtype parse_bigint(u8_string string,int base,int negative)
{
  int n_digits=u8_strlen(string);
  const u8_byte *scan=string;
  if (n_digits) {
    fd_bigint bi=fd_digit_stream_to_bigint
      (n_digits,(bigint_producer)read_digit_weight,(void *)&scan,base,negative);
    if (bi) return (fdtype)bi;
    else return FD_VOID;}
  else return FD_VOID;
}

FD_EXPORT
int fd_output_number(u8_output out,fdtype num,int base)
{
  if (FD_FIXNUMP(num)) {
    int fixnum=FD_FIX2INT(num);
    if (base==10) u8_printf(out,"%d",fixnum);
    else if (base==16) u8_printf(out,"%x",fixnum);
    else if (base == 8) u8_printf(out,"%o",fixnum);
    else {
      fd_bigint bi=fd_long_to_bigint(fixnum);
      output_bigint(out,bi,base);
      return 1;}
    return 1;}
  else if (FD_FLONUMP(num)) {
    unsigned char buf[256];
    struct FD_FLONUM *d=FD_GET_CONS(num,fd_flonum_type,struct FD_FLONUM *);
    sprintf(buf,"%f",d->flonum);
    u8_puts(out,buf);
    return 1;}
  else if (FD_BIGINTP(num)) {
    fd_bigint bi=FD_GET_CONS(num,fd_bigint_type,fd_bigint);
    output_bigint(out,bi,base);
    return 1;}
  else return 0;
}

/* Utility fucntions and macros. */


#define COMPLEXP(x) (FD_PTR_TYPEP((x),fd_complex_type))
#define REALPART(x) ((COMPLEXP(x)) ? (FD_REALPART(x)) : (x))
#define IMAGPART(x) ((COMPLEXP(x)) ? (FD_IMAGPART(x)) : (FD_INT(0)))

#define RATIONALP(x) (FD_PTR_TYPEP((x),fd_rational_type))
#define NUMERATOR(x) ((RATIONALP(x)) ? (FD_NUMERATOR(x)) : (x))
#define DENOMINATOR(x) ((RATIONALP(x)) ? (FD_DENOMINATOR(x)) : (FD_INT(1)))

#define INTEGERP(x) ((FD_FIXNUMP(x)) || (FD_BIGINTP(x)))

#define NUMBERP(x) \
  ((FD_FIXNUMP(x)) || (FD_FLONUMP(x)) || (FD_BIGINTP(x)) || \
   (FD_COMPLEXP(x)) || (FD_RATIONALP(x)))

static double todoublex(fdtype x,fd_ptr_type xt)
{
  if (xt == fd_flonum_type) return ((struct FD_FLONUM *)x)->flonum;
  else if (xt == fd_fixnum_type)
    return (double) (FD_FIX2INT(x));
  else if (xt == fd_bigint_type)
    return (double) fd_bigint_to_double((fd_bigint)x);
  else if (xt == fd_rational_type) {
    double num=todouble(NUMERATOR(x));
    double den=todouble(DENOMINATOR(x));
    return num/den;}
  else if (xt == fd_complex_type) {
    double real=todouble(FD_REALPART(x)), imag=todouble(FD_IMAGPART(x));
    return sqrt((real*real)+(imag*imag));}
  else return FP_NAN;
}
static double todouble(fdtype x)
{
  return todoublex(x,FD_PTR_TYPE(x));
}
FD_EXPORT double fd_todouble(fdtype x)
{
  return todoublex(x,FD_PTR_TYPE(x));
}
static fd_bigint tobigint(fdtype x)
{
  if (FD_FIXNUMP(x)) 
    return fd_long_to_bigint(FD_FIX2INT(x));
  else if (FD_BIGINTP(x))
    return (fd_bigint)x;
  else if (FD_FLONUMP(x))
    return fd_double_to_bigint(FD_FLONUM(x));
  else {
    u8_raise(_("Internal error"),"tobigint","numeric");
    return NULL;}
}
static fdtype simplify_bigint(fd_bigint bi)
{
  if (fd_bigint_fits_in_word_p(bi,30,1)) {
    int intval=fd_bigint_to_long(bi);
    fd_decref((fdtype)bi);
    return FD_INT(intval);}
  else return (fdtype)bi;
}

/* Making complex and rational numbers */

static fdtype make_complex(fdtype real,fdtype imag)
{
  if (fd_numcompare(imag,FD_INT(0))==0) {
    fd_decref(imag); return real;}
  else {
    struct FD_COMPLEX *result=u8_alloc(struct FD_COMPLEX);
    FD_INIT_CONS(result,fd_complex_type);
    result->realpart=real; result->imagpart=imag;
    return (fdtype) result;}
}

static int unparse_complex(struct U8_OUTPUT *out,fdtype x)
{
  fdtype imag=FD_IMAGPART(x), real=FD_REALPART(x);
  int has_real=fd_numcompare(real,FD_INT(0));
  if (fd_numcompare(imag,FD_INT(0))<0) {
    fdtype negated=fd_subtract(FD_INT(0),imag);
    if (has_real)
      u8_printf(out,"%q-%qi",FD_REALPART(x),negated);
    else u8_printf(out,"-%qi",negated);
    fd_decref(negated);}
  else if (has_real)
    u8_printf(out,"%q+%qi",FD_REALPART(x),FD_IMAGPART(x));
  else u8_printf(out,"+%qi",FD_IMAGPART(x));
  return 1;
}

FD_EXPORT
fdtype fd_make_complex(fdtype real,fdtype imag)
{
  fd_incref(real); fd_incref(imag);
  return make_complex(real,imag);
}

static int fix_gcd (int x, int y);
static fdtype int_gcd(fdtype x,fdtype y);
static fdtype int_lcm (fdtype x, fdtype y);

static fdtype make_rational(fdtype num,fdtype denom)
{
  struct FD_RATIONAL *result;
  if ((FD_FIXNUMP(denom)) && ((FD_FIX2INT(denom))==0))
    return fd_err(fd_DivideByZero,"make_rational",NULL,num);
  else if ((FD_FIXNUMP(num)) && (FD_FIXNUMP(denom))) {
    int in=FD_FIX2INT(num), id=FD_FIX2INT(denom), igcd=fix_gcd(in,id);
    in=in/igcd; id=id/igcd;
    if (id == 1) return FD_INT(in);
    else if (id < 0) {
      num=FD_INT(-in); denom=FD_INT(-id);}
    else {num=FD_INT(in); denom=FD_INT(id);}}
  else if ((INTEGERP(num)) && (INTEGERP(denom))) {
    fdtype gcd=int_gcd(num,denom);
    fdtype new_num=fd_quotient(num,gcd);
    fdtype new_denom=fd_quotient(denom,gcd);
    fd_decref(gcd); 
    if (((FD_FIXNUMP(new_denom)) && (FD_FIX2INT(new_denom) == 1)))
      return new_num;
    else {num=new_num; denom=new_denom;}}
  else return fd_err(_("Non integral components"),"fd_make_rational",NULL,num);
  result=u8_alloc(struct FD_RATIONAL);
  FD_INIT_CONS(result,fd_rational_type);
  result->numerator=num;
  result->denominator=denom;
  return (fdtype) result;
}

static int unparse_rational(struct U8_OUTPUT *out,fdtype x)
{
  u8_printf(out,"%q/%q",FD_NUMERATOR(x),FD_DENOMINATOR(x));
  return 1;
}

FD_EXPORT
fdtype fd_make_rational(fdtype num,fdtype denom)
{
  fd_incref(num); fd_incref(denom);
  return make_rational(num,denom);
}

static int fix_gcd (int x, int y)
{
  int a;
  if (x < 0) x=-x; if (y < 0) y=-y;
  a= y; while (a != 0) { y = a; a = x % a; x = y; }
  return x;
}

#define INT_POSITIVEP(x) \
  ((FD_FIXNUMP(x)) ? ((FD_FIX2INT(x)>0)) : \
   (FD_BIGINTP(x)) ? ((bigint_test((fd_bigint)x))==fd_bigint_greater) : (0))
#define INT_NEGATIVEP(x) \
  ((FD_FIXNUMP(x)) ? ((FD_FIX2INT(x)<0)) : \
   (FD_BIGINTP(x)) ? ((bigint_test((fd_bigint)x))==fd_bigint_less) : (0))
#define INT_ZEROP(x) \
  ((FD_FIXNUMP(x)) ? ((FD_FIX2INT(x)==0)) : \
   (FD_BIGINTP(x)) ? ((bigint_test((fd_bigint)x))==fd_bigint_equal) : (0))

static fdtype int_gcd(fdtype x,fdtype y)
{
  errno=0;
  if ((FD_FIXNUMP(x)) && (FD_FIXNUMP(y)))
    return FD_INT(fix_gcd(FD_FIX2INT(x),FD_FIX2INT(y)));
  else if ((INTEGERP(x)) && (INTEGERP(y))) {
    fdtype a; fd_incref(x); fd_incref(y);
    /* Normalize the sign of x */
    if (FD_FIXNUMP(x)) {
      int ival=FD_FIX2INT(x);
      if (ival<0) x=FD_INT(-ival);}
    else if (INT_NEGATIVEP(x)) {
      fdtype bval=fd_subtract(FD_INT(0),x);
      fd_decref(x); x=bval;}
    else {}
    /* Normalize the sign of y */
    if (FD_FIXNUMP(y)) {
      int ival=FD_FIX2INT(y);
      if (ival<0) y=FD_INT(-ival);}
    else if (INT_NEGATIVEP(y)) {
      fdtype bval=fd_subtract(FD_INT(0),y);
      fd_decref(y); y=bval;}
    else {}
    a=y;
    while (!(INT_ZEROP(a))) {
      y=a; a=fd_remainder(x,a); fd_decref(x); x=y;}
    fd_decref(a);
    return x;}
  else return fd_type_error(_("not an integer"),"int_gcd",y);
}

static fdtype int_lcm (fdtype x, fdtype y)
{
  fdtype prod=fd_multiply(x,y), gcd=int_gcd(x,y), lcm;
  if (FD_FIXNUMP(prod))
    if (FD_FIX2INT(prod) < 0) prod=FD_INT(-(FD_FIX2INT(prod)));
    else {}
  else if (INT_NEGATIVEP(prod)) {
    fdtype negated=fd_subtract(FD_INT(0),prod);
    fd_decref(prod); prod=negated;}
  lcm=fd_divide(prod,gcd);
  fd_decref(prod); fd_decref(gcd);
  return lcm;
}

/* Arithmetic operations */

FD_EXPORT int fd_small_bigintp(fd_bigint bi)
{
  return (fd_bigint_fits_in_word_p(bi,32,1));
}

FD_EXPORT int fd_modest_bigintp(fd_bigint bi)
{
  return (fd_bigint_fits_in_word_p(bi,64,1));
}

FD_EXPORT int fd_bigint2int(fd_bigint bi)
{
  if (fd_bigint_fits_in_word_p(bi,32,1)) 
    return fd_bigint_to_long(bi);
  else return 0;
}

FD_EXPORT unsigned int fd_bigint2uint(fd_bigint bi)
{
  if ((fd_bigint_fits_in_word_p(bi,32,1)) &&
      (!(BIGINT_NEGATIVE_P(bi))))
    return fd_bigint_to_long(bi);
  else return 0;
}

FD_EXPORT long long int fd_bigint2int64(fd_bigint bi)
{
  if (fd_bigint_fits_in_word_p(bi,64,1)) 
    return fd_bigint_to_long_long(bi);
  else return 0;
}

FD_EXPORT unsigned long long int fd_bigint2uint64(fd_bigint bi)
{
  if ((fd_bigint_fits_in_word_p(bi,64,1)) &&
      (!(BIGINT_NEGATIVE_P(bi))))
    return fd_bigint_to_long_long(bi);
  else return 0;
}

FD_EXPORT int fd_bigint_fits(fd_bigint bi,int width,int twos_complement)
{
  return fd_bigint_fits_in_word_p(bi,width,twos_complement);
}

FD_EXPORT unsigned long fd_bigint_bytes(fd_bigint bi)
{
  return fd_bigint_length_in_bytes(bi);
}

FD_EXPORT
int fd_numberp(fdtype x)
{
  if (NUMBERP(x)) return 1; else return 0;
}

FD_EXPORT
fdtype fd_plus(fdtype x,fdtype y)
{
  fd_ptr_type xt=FD_PTR_TYPE(x), yt=FD_PTR_TYPE(y);
  if ((xt==fd_fixnum_type) && (yt==fd_fixnum_type)) {
    int result=FD_FIX2INT(x)+FD_FIX2INT(y);
    if ((result<FD_MAX_FIXNUM) && (result>FD_MIN_FIXNUM))
      return FD_INT(result);
    else return (fdtype) fd_long_long_to_bigint(result);}
  else if ((xt==fd_flonum_type) && (yt==fd_flonum_type)) {
    double result=FD_FLONUM(x)+FD_FLONUM(y);
    return fd_init_flonum(NULL,result);}
  else if (((FD_VECTORP(x))||(FD_NUMVECP(x)))&&
           ((FD_VECTORP(x))||(FD_NUMVECP(y)))) {
    int x_len=numvec_length(x), y_len=numvec_length(y);
    if (x_len != y_len) {
      fd_seterr(_("Vector size mismatch"),"fd_plus",NULL,FD_VOID);
      return FD_ERROR_VALUE;}
    return vector_add(x,y,1);}
  else if (!(NUMBERP(x)))
    return fd_type_error(_("number"),"fd_plus",x);
  else if (!(NUMBERP(y)))
    return fd_type_error(_("number"),"fd_plus",y);
  else if ((COMPLEXP(x)) || (COMPLEXP(y))) {
    fdtype realx=REALPART(x), imagx=IMAGPART(x);
    fdtype realy=REALPART(y), imagy=IMAGPART(y);
    fdtype real=fd_plus(realx,realy);
    fdtype imag=fd_plus(imagx,imagy);
    return make_complex(real,imag);}
  else if ((xt == fd_flonum_type) || (yt == fd_flonum_type)) {
    double dx=todoublex(x,xt), dy=todoublex(y,yt);
    return fd_init_flonum(NULL,dx+dy);}
  else if ((xt == fd_rational_type) || (yt == fd_rational_type)) {
    fdtype xnum=NUMERATOR(x), xden=DENOMINATOR(x);
    fdtype ynum=NUMERATOR(y), yden=DENOMINATOR(y);
    fdtype new_numP1, new_numP2, new_num, new_denom, result;
    new_denom=fd_multiply(xden,yden);
    new_numP1=fd_multiply(xnum,yden);
    new_numP2=fd_multiply(ynum,xden);
    new_num=fd_plus(new_numP1,new_numP2);
    result=fd_make_rational(new_num,new_denom);
    fd_decref(new_numP1); fd_decref(new_numP2);
    fd_decref(new_denom); fd_decref(new_num);
    return result;}
  else {
    fd_bigint bx=tobigint(x), by=tobigint(y);
    fd_bigint result=fd_bigint_add(bx,by);
    if (!(FD_BIGINTP(x))) fd_decref((fdtype)bx);
    if (!(FD_BIGINTP(y))) fd_decref((fdtype)by);
    return simplify_bigint(result);}
}

FD_EXPORT
fdtype fd_multiply(fdtype x,fdtype y)
{
  fd_ptr_type xt=FD_PTR_TYPE(x), yt=FD_PTR_TYPE(y);
  if ((xt==fd_fixnum_type) && (yt==fd_fixnum_type)) {
    int ix=FD_FIX2INT(x), iy=FD_FIX2INT(y), q;
    long long result;
    if (iy==0) return FD_INT(0);
    q=((iy>0)?(FD_MAX_FIXNUM/iy):(FD_MIN_FIXNUM/iy));
    if ((ix>0)?(ix>q):((-ix)>q)) {
      /* This is the overflow case (?) */
      fd_bigint bx=tobigint(x), by=tobigint(y);
      fd_bigint bresult=fd_bigint_multiply(bx,by);
      fd_decref((fdtype)bx); fd_decref((fdtype)by);
      return simplify_bigint(bresult);}
    else result=ix*iy;
    if ((result<FD_MAX_FIXNUM) && (result>FD_MIN_FIXNUM))
      return FD_INT(result);
    else return (fdtype) fd_long_long_to_bigint(result);}
  else if ((xt==fd_flonum_type) && (yt==fd_flonum_type)) {
    double result=FD_FLONUM(x)*FD_FLONUM(y);
    return fd_init_flonum(NULL,result);}
  else if (((FD_VECTORP(x))||(FD_NUMVECP(x)))&&(FD_NUMBERP(y)))
    return vector_scale(x,y);
  else if (((FD_VECTORP(y))||(FD_NUMVECP(y)))&&(FD_NUMBERP(x)))
    return vector_scale(y,x);
  else if (((FD_VECTORP(x))||(FD_NUMVECP(x)))&&
           ((FD_VECTORP(y))||(FD_NUMVECP(y)))) {
    int x_len=numvec_length(x), y_len=numvec_length(y);
    if (x_len != y_len) {
      fd_seterr(_("Vector size mismatch"),"fd_subtract",NULL,FD_VOID);
      return FD_ERROR_VALUE;}
    return vector_dotproduct(x,y);}
  else if (!(NUMBERP(x)))
    return fd_type_error(_("number"),"fd_multiply",x);
  else if (!(NUMBERP(y)))
    return fd_type_error(_("number"),"fd_multiply",y);
  else if ((COMPLEXP(x)) || (COMPLEXP(y))) {
    fdtype realx=REALPART(x), imagx=IMAGPART(x);
    fdtype realy=REALPART(y), imagy=IMAGPART(y);
    fdtype t1, t2, t3, t4, realr, imagr, result;
    t1=fd_multiply(realx,realy); t2=fd_multiply(imagx,imagy);
    t3=fd_multiply(realx,imagy); t4=fd_multiply(imagx,realy);
    realr=fd_subtract(t1,t2); imagr=fd_plus(t3,t4);    
    result=make_complex(realr,imagr);
    fd_decref(t1); fd_decref(t2); fd_decref(t3); fd_decref(t4);
    return result;}
  else if ((xt == fd_flonum_type) || (yt == fd_flonum_type)) {
    double dx=todoublex(x,xt), dy=todoublex(y,yt);
    return fd_init_flonum(NULL,dx*dy);}
  else if ((xt == fd_rational_type) || (yt == fd_rational_type)) {
    fdtype xnum=NUMERATOR(x), xden=DENOMINATOR(x);
    fdtype ynum=NUMERATOR(y), yden=DENOMINATOR(y);
    fdtype new_denom, new_num, result;
    new_num=fd_multiply(xnum,ynum);
    new_denom=fd_multiply(xden,yden);
    result=make_rational(new_num,new_denom);
    return result;}
  else {
    fd_bigint bx=tobigint(x), by=tobigint(y);
    fd_bigint result=fd_bigint_multiply(bx,by);
    if (!(FD_BIGINTP(x))) fd_decref((fdtype)bx);
    if (!(FD_BIGINTP(y))) fd_decref((fdtype)by);
    return simplify_bigint(result);}
}

FD_EXPORT
fdtype fd_subtract(fdtype x,fdtype y)
{
  fd_ptr_type xt=FD_PTR_TYPE(x), yt=FD_PTR_TYPE(y);
  if ((xt==fd_fixnum_type) && (yt==fd_fixnum_type)) {
    int result=(FD_FIX2INT(x))-(FD_FIX2INT(y));
    if ((result<FD_MAX_FIXNUM) && (result>FD_MIN_FIXNUM))
      return FD_INT(result);
    else return (fdtype) fd_long_long_to_bigint(result);}
  else if ((xt==fd_flonum_type) && (yt==fd_flonum_type)) {
    double result=FD_FLONUM(x)-FD_FLONUM(y);
    return fd_init_flonum(NULL,result);}
  else if (((FD_VECTORP(x))||(FD_NUMVECP(x)))&&
           ((FD_VECTORP(x))||(FD_NUMVECP(y)))) {
    int x_len=numvec_length(x), y_len=numvec_length(y);
    if (x_len != y_len) {
      fd_seterr(_("Vector size mismatch"),"fd_subtract",NULL,FD_VOID);
      return FD_ERROR_VALUE;}
    return vector_add(x,y,-1);}
  else if (!(NUMBERP(x)))
    return fd_type_error(_("number"),"fd_subtract",x);
  else if (!(NUMBERP(y)))
    return fd_type_error(_("number"),"fd_subtract",y);
  else if ((COMPLEXP(x)) || (COMPLEXP(y))) {
    fdtype realx=REALPART(x), imagx=IMAGPART(x);
    fdtype realy=REALPART(y), imagy=IMAGPART(y);
    fdtype real=fd_subtract(realx,realy);
    fdtype imag=fd_subtract(imagx,imagy);
    return make_complex(real,imag);}
  else if ((xt == fd_flonum_type) || (yt == fd_flonum_type)) {
    double dx=todoublex(x,xt), dy=todoublex(y,yt);
    return fd_init_flonum(NULL,dx-dy);}
  else if ((xt == fd_rational_type) || (yt == fd_rational_type)) {
    fdtype xnum=NUMERATOR(x), xden=DENOMINATOR(x);
    fdtype ynum=NUMERATOR(y), yden=DENOMINATOR(y);
    fdtype new_numP1, new_numP2, new_num, new_denom, result;
    new_denom=fd_multiply(xden,yden);
    new_numP1=fd_multiply(xnum,yden);
    new_numP2=fd_multiply(ynum,xden);
    new_num=fd_subtract(new_numP1,new_numP2);
    result=fd_make_rational(new_num,new_denom);
    fd_decref(new_numP1); fd_decref(new_numP2);
    fd_decref(new_denom); fd_decref(new_num);
    return result;}
  else {
    fd_bigint bx=tobigint(x), by=tobigint(y);
    fd_bigint result=fd_bigint_subtract(bx,by);
    if (!(FD_BIGINTP(x))) fd_decref((fdtype)bx);
    if (!(FD_BIGINTP(y))) fd_decref((fdtype)by);
    return simplify_bigint(result);}
}

FD_EXPORT
fdtype fd_divide(fdtype x,fdtype y)
{
  fd_ptr_type xt=FD_PTR_TYPE(x), yt=FD_PTR_TYPE(y);
  if ((INTEGERP(x)) && (INTEGERP(y)))
    return fd_make_rational(x,y);
  else if ((xt==fd_flonum_type) && (yt==fd_flonum_type)) {
    double result=FD_FLONUM(x)/FD_FLONUM(y);
    return fd_init_flonum(NULL,result);}
  else if (!(NUMBERP(x)))
    return fd_type_error(_("number"),"fd_divide",x);
  else if (!(NUMBERP(y)))
    return fd_type_error(_("number"),"fd_divide",y);
  else if ((COMPLEXP(x)) || (COMPLEXP(y))) {
    fdtype a=REALPART(x), b=IMAGPART(x);
    fdtype c=REALPART(y), d=IMAGPART(y);
    fdtype ac, ad, cb, bd, ac_bd, cb_ad, cc, dd, ccpdd;
    fdtype realr, imagr, result;
    ac=fd_multiply(a,c); ad=fd_multiply(a,d);
    cb=fd_multiply(c,b); bd=fd_multiply(b,d);
    cc=fd_multiply(c,c); dd=fd_multiply(d,d);
    ccpdd=fd_plus(cc,dd);
    ac_bd=fd_plus(ac,bd); cb_ad=fd_subtract(cb,ad);    
    realr=fd_divide(ac_bd,ccpdd); imagr=fd_divide(cb_ad,ccpdd);
    result=make_complex(realr,imagr);
    fd_decref(ac); fd_decref(ad); fd_decref(cb); fd_decref(bd);
    fd_decref(ac_bd); fd_decref(cb_ad);
    fd_decref(cc); fd_decref(dd); fd_decref(ccpdd);
    return result;}
  else if ((xt==fd_flonum_type) || (yt==fd_flonum_type)) {
    double dx=todoublex(x,xt), dy=todoublex(y,yt);
    return fd_init_flonum(NULL,dx/dy);}
  else if ((xt == fd_rational_type) || (yt == fd_rational_type)) {
    fdtype xnum=NUMERATOR(x), xden=DENOMINATOR(x);
    fdtype ynum=NUMERATOR(y), yden=DENOMINATOR(y);
    fdtype new_denom, new_num, result;
    new_num=fd_multiply(xnum,yden);
    new_denom=fd_multiply(xden,ynum);
    result=make_rational(new_num,new_denom);
    return result;}
  else return fd_type_error(_("number"),"fd_divide",y);
}

FD_EXPORT
fdtype fd_inexact_divide(fdtype x,fdtype y)
{
  fd_ptr_type xt=FD_PTR_TYPE(x), yt=FD_PTR_TYPE(y);
  if ((xt==fd_fixnum_type) && (yt==fd_fixnum_type)) {
    int result=FD_FIX2INT(x)/FD_FIX2INT(y);
    if ((FD_FIX2INT(x)) == (result*(FD_FIX2INT(y))))
      return FD_INT(result);
    else {
      double dx=x, dy=y;
      return fd_init_flonum(NULL,dx/dy);}}
  else if ((xt==fd_flonum_type) && (yt==fd_flonum_type)) {
    double result=FD_FLONUM(x)/FD_FLONUM(y);
    return fd_init_flonum(NULL,result);}
  else if (!(NUMBERP(x)))
    return fd_type_error(_("number"),"fd_builtin_divinexact",x);
  else if (!(NUMBERP(y)))
    return fd_type_error(_("number"),"fd_builtin_divinexact",y);
  else {
    double dx=todoublex(x,xt), dy=todoublex(y,yt);
    return fd_init_flonum(NULL,dx/dy);}
}

FD_EXPORT
fdtype fd_quotient(fdtype x,fdtype y)
{
  fd_ptr_type xt=FD_PTR_TYPE(x), yt=FD_PTR_TYPE(y);
  if ((xt==fd_fixnum_type) && (yt==fd_fixnum_type)) {
    int result=FD_FIX2INT(x)/FD_FIX2INT(y);
    return FD_INT(result);}
  else if ((INTEGERP(x)) && (INTEGERP(y))) {
    fd_bigint bx=tobigint(x), by=tobigint(y);
    fd_bigint result=fd_bigint_quotient(bx,by);
    if (!(FD_BIGINTP(x))) fd_decref((fdtype)bx);
    if (!(FD_BIGINTP(y))) fd_decref((fdtype)by);
    return simplify_bigint(result);}
  else if (INTEGERP(x))
    return fd_type_error(_("integer"),"fd_quotient",y);
  else return fd_type_error(_("integer"),"fd_quotient",x);
}

FD_EXPORT
fdtype fd_remainder(fdtype x,fdtype y)
{
  fd_ptr_type xt=FD_PTR_TYPE(x), yt=FD_PTR_TYPE(y);
  if ((xt==fd_fixnum_type) && (yt==fd_fixnum_type)) {
    int result=FD_FIX2INT(x)%FD_FIX2INT(y);
    return FD_INT(result);}
  else if ((INTEGERP(x)) && (INTEGERP(y))) {
    fd_bigint bx=tobigint(x), by=tobigint(y);
    fd_bigint result=fd_bigint_remainder(bx,by);
    if (!(FD_BIGINTP(x))) fd_decref((fdtype)bx);
    if (!(FD_BIGINTP(y))) fd_decref((fdtype)by);
    return simplify_bigint(result);}
  else if (INTEGERP(x))
    return fd_type_error(_("integer"),"fd_remainder",y);
  else return fd_type_error(_("integer"),"fd_remainder",x);
}

FD_EXPORT
fdtype fd_gcd(fdtype x,fdtype y)
{
  return int_gcd(x,y);
}

FD_EXPORT
fdtype fd_lcm(fdtype x,fdtype y)
{
  return int_lcm(x,y);
}

static int signum(fdtype x)
{
  if (FD_FIXNUMP(x)) {
    int ival=FD_FIX2INT(x);
    if (ival<0) return -1;
    else if (ival>0) return 1;
    else return 0;}
  else if (FD_FLONUMP(x)) {
    double dval=FD_FLONUM(x);
    if (dval<0) return -1;
    else if (dval>0) return 1;
    else return 0;}
  else if (FD_BIGINTP(x))
    switch (bigint_test((fd_bigint)x)) {
    case fd_bigint_less: return -1;
    case fd_bigint_greater: return 1;
    default: return 0;}
  else if (RATIONALP(x)) {
    int nsign=signum(FD_NUMERATOR(x));
    int dsign=signum(FD_DENOMINATOR(x));
    return (nsign*dsign);}
  else return 0;
}

FD_EXPORT
int fd_numcompare(fdtype x,fdtype y)
{
  fd_ptr_type xt=FD_PTR_TYPE(x), yt=FD_PTR_TYPE(y);
  if ((xt==fd_fixnum_type) && (yt==fd_fixnum_type)) {
    int dx=FD_FIX2INT(x), dy=FD_FIX2INT(y);
    if (dx>dy) return 1; else if (dx<dy) return -1; else return 0;}
  else if ((xt==fd_flonum_type) && (yt==fd_flonum_type)) {
    double dx=FD_FLONUM(x), dy=FD_FLONUM(y);
    if (dx>dy) return 1; else if (dx<dy) return -1; else return 0;}
  else if (!(NUMBERP(x))) {
    fd_seterr(fd_TypeError,"compare",
	      u8_mkstring(_("object is not a %m"),"number"),
	      x);
    /* Any number > 1 indicates an error. */
    return 17;}
  else if (!(NUMBERP(y))) {
    fd_seterr(fd_TypeError,"compare",
	      u8_mkstring(_("object is not a %m"),"number"),
	      y);
    /* Any number > 1 indicates an error. */
    return 17;}
  else if ((COMPLEXP(x)) || (COMPLEXP(x))) {
    double magx=todouble(x), magy=todouble(y);
    return signum(magx-magy);}
  else {
    fdtype difference=fd_subtract(x,y);
    int sgn=signum(difference);
    fd_decref(difference);
    if (sgn==0)
      if ((xt==fd_flonum_type) || (yt==fd_flonum_type)) 
	/* If either argument is inexact (double), don't return =, unless
	   both are inexact (which is handled above) */
	if (xt<yt) return -1; else return 1;
      else return sgn;
    else return sgn;}
}

/* Exact/inexact conversion */

FD_EXPORT
fdtype fd_make_inexact(fdtype x)
{
  fd_ptr_type xt=FD_PTR_TYPE(x);
  if (xt == fd_flonum_type)
    return fd_incref(x);
  else if (xt == fd_fixnum_type)
    return fd_init_flonum(NULL,((double) (FD_FIX2INT(x))));
  else if (xt == fd_bigint_type)
    return fd_init_flonum(NULL,((double) fd_bigint_to_double((fd_bigint)x)));
  else if (xt == fd_rational_type) {
    double num=todouble(NUMERATOR(x));
    double den=todouble(DENOMINATOR(x));
    return fd_init_flonum(NULL,num/den);}
  else if (xt == fd_complex_type) {
    fdtype realpart=FD_REALPART(x), imagpart=FD_IMAGPART(x);
    if ((FD_FLONUMP(realpart)) &&
	(FD_FLONUMP(imagpart)))
      return fd_incref(x);
    else return make_complex(fd_make_inexact(realpart),
			     fd_make_inexact(imagpart));}
  else return fd_type_error(_("number"),"fd_make_inexact",x);
}

FD_EXPORT
fdtype fd_make_exact(fdtype x)
{
  fd_ptr_type xt=FD_PTR_TYPE(x);
  if (xt == fd_flonum_type) {
    double d=FD_FLONUM(x);
    double f=floor(d);
    if (f==d) {
      fd_bigint ival=fd_double_to_bigint(d);
      return simplify_bigint(ival);}
#if 0
    else {
      double top=significand(d)*1048576;
      int exp=ilogb(d);
      fd_bigint itop=fd_double_to_bigint(top);
      fd_bigint ibottom=bigint_make_one(0);
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
      fd_bigint ival=fd_double_to_bigint(d);
      return simplify_bigint(ival);}}
  else if (xt==fd_complex_type) {
    fdtype realpart=FD_REALPART(x), imagpart=FD_IMAGPART(x);
    if ((FD_FLONUMP(realpart)) ||
	(FD_FLONUMP(imagpart)))
      return make_complex(fd_make_exact(realpart),fd_make_exact(imagpart));
    else return fd_incref(x);}
  else if (FD_NUMBERP(x)) return fd_incref(x);
  else return fd_type_error(_("number"),"fd_make_inexact",x);
}


/* Homogenous vectors */

/* Numeric vector handlers */

static void recycle_numeric_vector(struct FD_CONS *c)
{
  struct FD_NUMERIC_VECTOR *v=(struct FD_NUMERIC_VECTOR *)c;
  enum fd_num_elt_type elt_type=v->elt_type;
  if (v->freedata) {
    switch(elt_type) {
    case fd_short_elt:
      u8_free(v->elts.shorts); break;
    case fd_int_elt:
      u8_free(v->elts.ints); break;
    case fd_long_elt:
      u8_free(v->elts.longs); break;
    case fd_float_elt:
      u8_free(v->elts.floats); break;
    case fd_double_elt:
      u8_free(v->elts.doubles); break;}}
  if (FD_MALLOCD_CONSP(c)) {
    u8_free(c);}
}

static double double_ref(struct FD_NUMERIC_VECTOR *vec,int i)
{
  enum fd_num_elt_type elt_type=vec->elt_type;
  switch (elt_type) {
  case fd_short_elt:
    return (double) (FD_NUMVEC_SHORT(vec,i));
  case fd_int_elt:
    return (double) (FD_NUMVEC_INT(vec,i));
  case fd_long_elt:
    return (double) (FD_NUMVEC_LONG(vec,i));
  case fd_float_elt:
    return (double) (FD_NUMVEC_FLOAT(vec,i));
  case fd_double_elt:
    return (double) (FD_NUMVEC_DOUBLE(vec,i));
  default:
    return INFINITY;}
}

static size_t nvec_elt_size(enum fd_num_elt_type elt_type)
{
  switch (elt_type) {
  case fd_short_elt:
    return sizeof(fd_short);
  case fd_int_elt:
    return sizeof(fd_int);
  case fd_long_elt:
    return sizeof(fd_long);
  case fd_float_elt:
    return sizeof(fd_float);
  case fd_double_elt:
    return sizeof(fd_double);
  default:
    return sizeof(fd_double);}
}

static int compare_numeric_vector(fdtype x,fdtype y,int f)
{
  struct FD_NUMERIC_VECTOR *vx=(struct FD_NUMERIC_VECTOR *)x;
  struct FD_NUMERIC_VECTOR *vy=(struct FD_NUMERIC_VECTOR *)y;
  if (vx->length == vy->length) {
    int i=0, n=vx->length; 
    enum fd_num_elt_type xt=vx->elt_type, yt=vy->elt_type;
    while (i<n) {
      double xelt=double_ref(vx,i);
      double yelt=double_ref(vy,i);
      if (xelt>yelt)
        return -1;
      else if (yelt>xelt)
        return 1;
      else i++;}
    return 0;}
  else if (vx->length > vy->length)
    return 1;
  else return -1;
}

static int hash_numeric_vector(fdtype x,unsigned int (*fn)(fdtype))
{
  struct FD_NUMERIC_VECTOR *vec=(struct FD_NUMERIC_VECTOR *)x;
  int i=0, n=vec->length; int hashval=vec->length;
  while (i<n) {
    double v=double_ref(vec,i); int exp;
    double mantissa=frexpf(v,&exp);
    double reformed=
      ((exp<0) ? (ldexpf(mantissa,0)) : (ldexpf(mantissa,exp)));
    int asint=(int)reformed;
    hashval=hash_combine(hashval,asint);}
  return hashval;
}

static fdtype copy_numeric_vector(fdtype x,int deep)
{
  struct FD_NUMERIC_VECTOR *vec=(struct FD_NUMERIC_VECTOR *)x;
  enum fd_num_elt_type elt_type=vec->elt_type;
  size_t len=vec->length;
  size_t elts_size=len*nvec_elt_size(vec->elt_type);
  size_t vec_size=sizeof(struct FD_NUMERIC_VECTOR)+elts_size;
  struct FD_NUMERIC_VECTOR *copy=u8_malloc(vec_size);
  memset(copy,0,vec_size);
  FD_INIT_CONS(copy,fd_numeric_vector_type);
  copy->length=len; copy->freedata=0;
  switch (elt_type) {
  case fd_short_elt:
    copy->elts.shorts=vec->elts.shorts;
    memcpy(copy->elts.shorts,vec->elts.shorts,elts_size); 
    break;
  case fd_int_elt:
    copy->elts.ints=vec->elts.ints;
    memcpy(copy->elts.ints,vec->elts.ints,elts_size); 
    break;
  case fd_long_elt:
    copy->elts.longs=vec->elts.longs;
    memcpy(copy->elts.longs,vec->elts.longs,elts_size); 
    break;
  case fd_float_elt:
    copy->elts.floats=vec->elts.floats;
    memcpy(copy->elts.floats,vec->elts.floats,elts_size); 
    break;
  case fd_double_elt:
    copy->elts.doubles=vec->elts.doubles;
    memcpy(copy->elts.doubles,vec->elts.doubles,elts_size); 
    break;}
  return (fdtype) copy;
}

static int unparse_numeric_vector(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_NUMERIC_VECTOR *vec=(struct FD_NUMERIC_VECTOR *)x;
  int i=0, n=vec->length; char *typename="NUMVEC";
  enum fd_num_elt_type type=vec->elt_type;
  switch (type) {
  case fd_short_elt:
    typename="SHORTVEC"; break;
  case fd_int_elt:
    typename="INTVEC"; break;
  case fd_long_elt:
    typename="LONGVEC"; break;
  case fd_float_elt:
    typename="FLOATVEC"; break;
  case fd_double_elt:
    typename="DOUBLEVEC"; break;}
  u8_printf(out,"#<%s",typename);
  if (n>fd_numvec_showmax) switch (type) {
      case fd_short_elt: {
        int sum=0, dot=0; while (i<n) {
          int v=FD_NUMVEC_SHORT(vec,i);
          sum=sum+v; dot=dot+v*v; i++;}
        u8_printf(out," sum=%d/dot=%d/n=%d",sum,dot,n);
        break;}
      case fd_int_elt: {
        long long sum=0, dot=0; while (i<n) {
          int v=FD_NUMVEC_INT(vec,i);
          sum=sum+v; dot=dot+v*v; i++;}
        u8_printf(out," sum=%d/dot=%d/n=%d",sum,dot,n);
        break;}
      case fd_long_elt: {
        long long sum=0, dot=0; while (i<n) {
          int v=FD_NUMVEC_LONG(vec,i);
          sum=sum+v; dot=dot+v*v; i++;}
        u8_printf(out," sum=%d/dot=%d/n=%d",sum,dot,n);
        break;}
      case fd_float_elt: {
        double sum=0, dot=0; while (i<n) {
          double v=FD_NUMVEC_FLOAT(vec,i);
          sum=sum+v; dot=dot+v*v; i++;}
        u8_printf(out," sum=%f/dot=%f/n=%d",sum,dot,n);
        break;}
      case fd_double_elt: {
        double sum=0, dot=0; while (i<n) {
          double v=FD_NUMVEC_DOUBLE(vec,i);
          sum=sum+v; dot=dot+v*v; i++;}
        u8_printf(out," sum=%f/dot=%f/n=%d",sum,dot,n);
        break;}}
  else switch (type) {
  case fd_short_elt:
    while (i<n) {u8_printf(out," %d",FD_NUMVEC_SHORT(vec,i)); i++;} break;
  case fd_int_elt:
    while (i<n) {u8_printf(out," %d",FD_NUMVEC_INT(vec,i)); i++;} break;
  case fd_long_elt:
    while (i<n) {u8_printf(out," %lld",FD_NUMVEC_LONG(vec,i)); i++;} break;
  case fd_float_elt:
    while (i<n) {u8_printf(out," %f",(double)FD_NUMVEC_FLOAT(vec,i)); i++;} break;
  case fd_double_elt:
    while (i<n) {u8_printf(out," %f",FD_NUMVEC_DOUBLE(vec,i)); i++;} break;}
  u8_puts(out,">");
  return 1;
}

/* Numeric vector constructors */

FD_EXPORT fdtype fd_make_double_vector(int n,fd_double *v)
{
  struct FD_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct FD_NUMERIC_VECTOR)+(n*sizeof(fd_double)));
  unsigned char *bytes=(unsigned char *)nvec;
  FD_INIT_FRESH_CONS(nvec,fd_numeric_vector_type);
  nvec->elt_type=fd_double_elt;
  nvec->freedata=0;
  nvec->length=n;
  memcpy(bytes+sizeof(struct FD_NUMERIC_VECTOR),v,n*sizeof(fd_double));
  nvec->elts.doubles=(fd_double *)(bytes+sizeof(struct FD_NUMERIC_VECTOR));
  return (fdtype) nvec;
}

FD_EXPORT fdtype fd_make_float_vector(int n,fd_float *v)
{
  struct FD_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct FD_NUMERIC_VECTOR)+(n*sizeof(fd_float)));
  unsigned char *bytes=(unsigned char *)nvec;
  FD_INIT_FRESH_CONS(nvec,fd_numeric_vector_type);
  nvec->elt_type=fd_float_elt;
  nvec->freedata=0;
  nvec->length=n;
  memcpy(bytes+sizeof(struct FD_NUMERIC_VECTOR),v,n*sizeof(fd_float));
  nvec->elts.floats=(fd_float *)(bytes+sizeof(struct FD_NUMERIC_VECTOR));
  return (fdtype) nvec;
}

FD_EXPORT fdtype fd_make_int_vector(int n,fd_int *v)
{
  struct FD_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct FD_NUMERIC_VECTOR)+(n*sizeof(fd_int)));
  unsigned char *bytes=(unsigned char *)nvec;
  FD_INIT_FRESH_CONS(nvec,fd_numeric_vector_type);
  nvec->elt_type=fd_int_elt;
  nvec->freedata=0;
  nvec->length=n;
  memcpy(bytes+sizeof(struct FD_NUMERIC_VECTOR),v,n*sizeof(fd_int));
  nvec->elts.ints=(fd_int *)(bytes+sizeof(struct FD_NUMERIC_VECTOR));
  return (fdtype) nvec;
}

FD_EXPORT fdtype fd_make_long_vector(int n,fd_long *v)
{
  struct FD_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct FD_NUMERIC_VECTOR)+(n*sizeof(fd_long)));
  unsigned char *bytes=(unsigned char *)nvec;
  FD_INIT_FRESH_CONS(nvec,fd_numeric_vector_type);
  nvec->elt_type=fd_long_elt;
  nvec->freedata=0;
  nvec->length=n;
  memcpy(bytes+sizeof(struct FD_NUMERIC_VECTOR),v,n*sizeof(fd_long));
  nvec->elts.longs=(fd_long *)(bytes+sizeof(struct FD_NUMERIC_VECTOR));
  return (fdtype) nvec;
}

FD_EXPORT fdtype fd_make_short_vector(int n,fd_short *v)
{
  struct FD_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct FD_NUMERIC_VECTOR)+(n*sizeof(fd_short)));
  unsigned char *bytes=(unsigned char *)nvec;
  FD_INIT_FRESH_CONS(nvec,fd_numeric_vector_type);
  nvec->elt_type=fd_short_elt;
  nvec->freedata=0;
  nvec->length=n;
  memcpy(bytes+sizeof(struct FD_NUMERIC_VECTOR),v,n*sizeof(fd_short));
  nvec->elts.shorts=(fd_short *)(bytes+sizeof(struct FD_NUMERIC_VECTOR));
  return (fdtype) nvec;
}

FD_EXPORT fdtype fd_make_numeric_vector(int n,enum fd_num_elt_type vectype)
{
  int elt_size=nvec_elt_size(vectype);
  struct FD_NUMERIC_VECTOR *nvec=
    u8_malloc(sizeof(struct FD_NUMERIC_VECTOR)+(n*elt_size));
  unsigned char *bytes=(unsigned char *)nvec;
  FD_INIT_FRESH_CONS(nvec,fd_numeric_vector_type);
  nvec->elt_type=vectype;
  nvec->freedata=0;
  nvec->length=n;
  memset(((char *)nvec)+sizeof(struct FD_NUMERIC_VECTOR),0,n*elt_size);
  switch (vectype) {
  case fd_short_elt:
    nvec->elts.shorts=(fd_short *)(bytes+sizeof(struct FD_NUMERIC_VECTOR)); 
    break;
  case fd_int_elt:
    nvec->elts.ints=(fd_int *)(bytes+sizeof(struct FD_NUMERIC_VECTOR)); 
    break;
  case fd_long_elt:
    nvec->elts.longs=(fd_long *)(bytes+sizeof(struct FD_NUMERIC_VECTOR)); 
    break;
  case fd_float_elt:
    nvec->elts.floats=(fd_float *)(bytes+sizeof(struct FD_NUMERIC_VECTOR)); 
    break;
  case fd_double_elt: 
    nvec->elts.doubles=(fd_double *)(bytes+sizeof(struct FD_NUMERIC_VECTOR)); 
    break;}
  return (fdtype) nvec;
}


/* Vector operations */

static void decref_vec(fdtype *elts,int n)
{
  int i=0; while (i<n) {
    fdtype elt=elts[i++]; fd_decref(elt);}
}

static int numvec_length(fdtype x)
{
  if (FD_VECTORP(x))
    return FD_VECTOR_LENGTH(x);
  else if (FD_NUMVECP(x))
    return FD_NUMVEC_LENGTH(x);
  else return -1;
}
static fd_long EXACT_REF(fdtype x,enum fd_num_elt_type xtype,int i)
{
  switch (xtype) {
  case fd_long_elt:
    return FD_NUMVEC_LONG(x,i);
  case fd_int_elt:
    return FD_NUMVEC_INT(x,i);
  case fd_short_elt:
    return FD_NUMVEC_SHORT(x,i);
  default:
    return -1;}
}
static fd_double INEXACT_REF(fdtype x,enum fd_num_elt_type xtype,int i)
{
  switch (xtype) {
  case fd_double_elt:
    return FD_NUMVEC_DOUBLE(x,i);
  case fd_float_elt:
    return ((fd_double)(FD_NUMVEC_FLOAT(x,i)));
  case fd_long_elt:
    return ((fd_double)(FD_NUMVEC_LONG(x,i)));
  case fd_int_elt:
    return ((fd_double)(FD_NUMVEC_INT(x,i)));
  case fd_short_elt:
    return ((fd_double)(FD_NUMVEC_SHORT(x,i)));}
}
static fdtype NUM_ELT(fdtype x,int i)
{
  if (FD_VECTORP(x)) {
    fdtype elt=FD_VECTOR_REF(x,i);
    fd_incref(elt);
    return elt;}
  else {
    struct FD_NUMERIC_VECTOR *vx=(struct FD_NUMERIC_VECTOR *)x;
    enum fd_num_elt_type xtype=vx->elt_type;
    switch (xtype) {
    case fd_double_elt:
      return fd_make_flonum(FD_NUMVEC_DOUBLE(x,i));
    case fd_float_elt:
      return fd_make_flonum(FD_NUMVEC_FLOAT(x,i));
    case fd_long_elt:
      return FD_INT2DTYPE(FD_NUMVEC_LONG(x,i));
    case fd_int_elt:
      return FD_INT2DTYPE(FD_NUMVEC_INT(x,i));
    case fd_short_elt:
      return FD_SHORT2DTYPE(FD_NUMVEC_SHORT(x,i));}
  }
}
static fdtype vector_add(fdtype x,fdtype y,int mult)
{
  int x_len=numvec_length(x), y_len=numvec_length(y);
  if (x_len!=y_len) {
    fd_seterr("Dimensional conflict","vector_add",NULL,FD_VOID);
    return FD_ERROR_VALUE;}
  else if ((FD_NUMVECP(x))&&(FD_NUMVECP(y))) {
    struct FD_NUMERIC_VECTOR *vx=(struct FD_NUMERIC_VECTOR *)x;
    struct FD_NUMERIC_VECTOR *vy=(struct FD_NUMERIC_VECTOR *)y;
    enum fd_num_elt_type xtype=vx->elt_type;
    enum fd_num_elt_type ytype=vy->elt_type;
    if (((xtype==fd_float_elt)||(xtype==fd_double_elt))&&
        ((ytype==fd_float_elt)||(ytype==fd_double_elt))) {
      /* Both arguments are inexact vectors*/
      if ((xtype==fd_float_elt)||(ytype==fd_float_elt)) {
        /* One of them is a float vector, so return a float vector
           (don't invent precision) */
        fdtype result; fd_float *sums=u8_alloc_n(x_len,fd_float);
        int i=0; while (i<x_len) {
          fd_float xelt=((xtype==fd_double_elt)?
                         (FD_NUMVEC_DOUBLE(x,i)):
                         (FD_NUMVEC_FLOAT(x,i)));
          fd_float yelt=((ytype==fd_double_elt)?
                         (FD_NUMVEC_DOUBLE(y,i)):
                         (FD_NUMVEC_FLOAT(y,i)));
          sums[i]=xelt+(yelt*mult);
          i++;}
        result=fd_make_float_vector(x_len,sums);
        u8_free(sums);
        return result;}
      else {
        /* They're both double vectors, so return a double vector */
        fdtype result; fd_double *sums=u8_alloc_n(x_len,fd_double);
        int i=0; while (i<x_len) {
          fd_double xelt=(FD_NUMVEC_DOUBLE(x,i));
          fd_double yelt=(FD_NUMVEC_DOUBLE(y,i));
          sums[i]=xelt+(yelt*mult);
          i++;}
        result=fd_make_double_vector(x_len,sums);
        u8_free(sums);
        return result;}}
    else if ((xtype==fd_float_elt)||(xtype==fd_double_elt)||
             (ytype==fd_float_elt)||(ytype==fd_double_elt))  {
      /* One of the vectors is a floating type, so return a floating
         point vector. */
      fdtype result; fd_double *sums=u8_alloc_n(x_len,fd_double);
      int i=0; while (i<x_len) {
        fd_double xelt=(INEXACT_REF(x,xtype,i));
        fd_double yelt=(INEXACT_REF(y,ytype,i));
        sums[i]=xelt+(yelt*mult);
        i++;}
      result=fd_make_double_vector(x_len,sums);
      u8_free(sums);
      return result;}
    else if ((xtype==fd_long_elt)||(ytype==fd_long_elt)) {
      /* One of them is a long so return a long vector. */
      fdtype result; fd_long *sums=u8_alloc_n(x_len,fd_long);
      int i=0; while (i<x_len) {
        fd_long xelt=EXACT_REF(x,xtype,i);
        fd_long yelt=EXACT_REF(y,ytype,i);
        sums[i]=xelt+(yelt*mult);
        i++;}
      result=fd_make_long_vector(x_len,sums);
      u8_free(sums);
      return result;}
    else if ((xtype==fd_int_elt)||(ytype==fd_int_elt)) {
      /* One of them is an int vector so return an int vector. */
      fdtype result; fd_int *sums=u8_alloc_n(x_len,fd_int);
      int i=0; while (i<x_len) {
        fd_int xelt=EXACT_REF(x,xtype,i);
        fd_int yelt=EXACT_REF(y,ytype,i);
        sums[i]=xelt+(yelt*mult);
        i++;}
      result=fd_make_int_vector(x_len,sums);
      u8_free(sums);
      return result;}
    else {
      /* This really means that they're both short vectors */
      fdtype result; fd_short *sums=u8_alloc_n(x_len,fd_short);
      int i=0; while (i<x_len) {
        fd_int xelt=EXACT_REF(x,xtype,i);
        fd_int yelt=EXACT_REF(y,ytype,i);
        sums[i]=xelt+(yelt*mult);
        i++;}
      result=fd_make_short_vector(x_len,sums);
      u8_free(sums);
      return result;}}
  else {
    fdtype *sums=u8_alloc_n(x_len,fdtype), result;
    fdtype factor=FD_INT2DTYPE(mult);
    int i=0; while (i<x_len) {
      fdtype xelt=NUM_ELT(x,i);
      fdtype yelt=NUM_ELT(y,i);
      fdtype sum;
      if (factor==FD_FIXNUM_ONE)
        sum=fd_plus(xelt,yelt);
      else {
        fdtype mult=fd_multiply(yelt,factor);
        if (FD_ABORTP(mult)) {
          decref_vec(sums,i); u8_free(sums);
          return mult;}
        sum=fd_plus(xelt,mult);
        if (FD_ABORTP(sum)) {
          decref_vec(sums,i); u8_free(sums);
          fd_decref(mult);
          return mult;}
        fd_decref(mult);}
      sums[i]=sum;
      i++;}
    result=fd_make_vector(x_len,sums);
    u8_free(sums);
    return result;}
}
static fdtype vector_dotproduct(fdtype x,fdtype y)
{
  int x_len=numvec_length(x), y_len=numvec_length(y);
  if (x_len!=y_len) {
    fd_seterr("Dimensional conflict","vector_add",NULL,FD_VOID);
    return FD_ERROR_VALUE;}
  else if ((FD_NUMVECP(x))&&(FD_NUMVECP(y))) {
    struct FD_NUMERIC_VECTOR *vx=(struct FD_NUMERIC_VECTOR *)x;
    struct FD_NUMERIC_VECTOR *vy=(struct FD_NUMERIC_VECTOR *)y;
    enum fd_num_elt_type xtype=vx->elt_type;
    enum fd_num_elt_type ytype=vy->elt_type;
    if (((xtype==fd_float_elt)||(xtype==fd_double_elt))&&
        ((ytype==fd_float_elt)||(ytype==fd_double_elt))) {
      /* This is the case where they're both inexact (floating) */
      double dot=0;
      if ((xtype==fd_float_elt)||(ytype==fd_float_elt)) {
        /* This is the case where they're both float vectors */
        int i=0; while (i<x_len) {
          fd_float xelt=((xtype==fd_double_elt)?
                         (FD_NUMVEC_DOUBLE(x,i)):
                         (FD_NUMVEC_FLOAT(x,i)));
          fd_float yelt=((ytype==fd_double_elt)?
                         (FD_NUMVEC_DOUBLE(y,i)):
                         (FD_NUMVEC_FLOAT(y,i)));
          dot=dot+(xelt*yelt);
          i++;}
        return fd_make_flonum(dot);}
      else {
        /* This is the case where they're either both doubles
           or mixed. */
        double dot=0;
        int i=0; while (i<x_len) {
          fd_double xelt=(FD_NUMVEC_DOUBLE(x,i));
          fd_double yelt=(FD_NUMVEC_DOUBLE(y,i));
          dot=dot+(xelt*yelt);
          i++;}
        return fd_make_flonum(dot);}}
    else if ((xtype==fd_float_elt)||(xtype==fd_double_elt)||
             (ytype==fd_float_elt)||(ytype==fd_double_elt))  {
      /* This is the case where either is inexact (floating) */
      double dot=0;
      int i=0; while (i<x_len) {
        fd_double xelt=(INEXACT_REF(x,xtype,i));
        fd_double yelt=(INEXACT_REF(y,ytype,i));
        dot=dot+(xelt*yelt);
        i++;}
      return fd_make_flonum(dot);}
    /* For the integral types, we pick the size of the larger. */
    else if ((xtype==fd_long_elt)||(ytype==fd_long_elt)) {
      fd_long dot=0;
      int i=0; while (i<x_len) {
        fd_long xelt=EXACT_REF(x,xtype,i);
        fd_long yelt=EXACT_REF(y,ytype,i);
        dot=dot+(xelt*yelt);
        i++;}
      return FD_INT2DTYPE(dot);}
    else if ((xtype==fd_int_elt)||(ytype==fd_int_elt)) {
      fd_long dot=0;
      int i=0; while (i<x_len) {
        fd_int xelt=EXACT_REF(x,xtype,i);
        fd_int yelt=EXACT_REF(y,ytype,i);
        dot=dot+(xelt*yelt);
        i++;}
      return FD_INT2DTYPE(dot);}
    else {
      /* They're both short vectors */
      fd_long dot=0;
      int i=0; while (i<x_len) {
        fd_int xelt=EXACT_REF(x,xtype,i);
        fd_int yelt=EXACT_REF(y,ytype,i);
        dot=dot+(xelt*yelt);
        i++;}
      return FD_INT2DTYPE(dot);}}
  else {
    fdtype dot=FD_FIXNUM_ZERO;
    int i=0; while (i<x_len) {
      fdtype xelt=NUM_ELT(x,i);
      fdtype yelt=NUM_ELT(y,i);
      fdtype prod=fd_multiply(xelt,yelt);
      fdtype new_sum=
        (FD_ABORTP(prod))?(prod):(fd_plus(dot,prod));
      fd_decref(xelt); fd_decref(yelt);
      fd_decref(prod); fd_decref(dot);
      if (FD_ABORTP(new_sum)) return new_sum;
      dot=new_sum;
      i++;}
    return dot;}
}
static fdtype generic_vector_scale(fdtype vec,fdtype scalar)
{
  int len=(FD_VECTORP(vec))?(FD_VECTOR_LENGTH(vec)):(numvec_length(vec));
  fdtype result, *elts=u8_alloc_n(len,fdtype);
  int i=0; while (i<len) {
    fdtype elt=NUM_ELT(vec,i);
    fdtype product=fd_multiply(elt,scalar);
    if (FD_ABORTP(product)) {
      decref_vec(elts,i);
      return product;}
    elts[i]=product;
    fd_decref(elt);
    i++;}
  result=fd_make_vector(len,elts);
  u8_free(elts);
  return result;
}
static fdtype vector_scale(fdtype vec,fdtype scalar)
{
  if (FD_NUMVECP(vec)) {
    fdtype result;
    struct FD_NUMERIC_VECTOR *nv=(struct FD_NUMERIC_VECTOR *)vec;
    enum fd_num_elt_type vtype=nv->elt_type; int vlen=nv->length;
    if (FD_FLONUMP(scalar)) {
      fd_double mult=FD_FLONUM(scalar);
      fd_double *scaled=u8_alloc_n(vlen,fd_double);
      int i=0;
      if ((vtype==fd_float_elt)||(vtype==fd_double_elt)) {
        while (i<vlen) {
          fd_double elt=INEXACT_REF(vec,vtype,i);
          scaled[i]=elt*mult;
          i++;}}
      else while (i<vlen) {
          fd_long elt=EXACT_REF(vec,vtype,i);
          scaled[i]=elt*mult;
          i++;}
      result=fd_make_double_vector(vlen,scaled);
      u8_free(scaled);
      return result;}
    else if ((vtype==fd_float_elt)||(vtype==fd_double_elt)) {
      fd_long mult=fd_getint(scalar);
      fd_double *scaled=u8_alloc_n(vlen,fd_double);
      int i=0; while (i<vlen) {
        fd_double elt=INEXACT_REF(vec,vtype,i);
        scaled[i]=elt*mult;
        i++;}
      result=fd_make_double_vector(vlen,scaled);
      u8_free(scaled);
      return result;}
    else {
      fd_long mult=fd_getint(scalar), max, min;
      fdtype *scaled=u8_alloc_n(vlen,fdtype);
      int i=0; while (i<vlen) {
        fdtype elt=NUM_ELT(vec,i);
        fdtype product=fd_multiply(elt,scalar);
        if (i==0) {max=product; min=product;}
        else {
          if (fd_numcompare(product,max)>0) max=product;
          if (fd_numcompare(product,min)<0) min=product;}
        scaled[i]=product;
        i++;}
      if ((FD_FIXNUMP(max))&&(FD_FIXNUMP(min))) {
        int imax=fd_getint(max), imin=fd_getint(min);
        if ((imax>-32768)&&(imax<32768)&&
            (imin>-32768)&&(imin<32768)) {
          fd_short *shorts=u8_alloc_n(vlen,fd_short);
          int j=0; while (j<vlen) {
            int elt=FD_FIX2INT(scaled[j]);
            shorts[j]=(short)elt;
            j++;}
          result=fd_make_short_vector(vlen,shorts);
          u8_free(shorts); u8_free(scaled);}
        else {
          fd_int *ints=u8_alloc_n(vlen,fd_int);
          int j=0; while (j<vlen) {
            int elt=fd_getint(scaled[j]);
            ints[j]=elt;
            j++;}
          result=fd_make_int_vector(vlen,ints);
          u8_free(ints); u8_free(scaled);}}
      else if (((FD_FIXNUMP(max))||
                ((FD_BIGINTP(max))&&
                 (fd_small_bigintp((fd_bigint)max))))&&
               ((FD_FIXNUMP(min))||
                ((FD_BIGINTP(min))&&
                 (fd_small_bigintp((fd_bigint)min))))) {
        fd_int *ints=u8_alloc_n(vlen,fd_int);
        int j=0; while (j<vlen) {
          int elt=fd_getint(scaled[j]);
          ints[j]=elt;
          j++;}
        result=fd_make_int_vector(vlen,ints);
        u8_free(ints); u8_free(scaled);}
      else if (((FD_FIXNUMP(max))||
                ((FD_BIGINTP(max))&&
                 (fd_modest_bigintp((fd_bigint)max))))&&
               ((FD_FIXNUMP(min))||
                ((FD_BIGINTP(min))&&
                 (fd_modest_bigintp((fd_bigint)min))))) {
        fd_long *longs=u8_alloc_n(vlen,fd_long);
        int j=0; while (j<vlen) {
          int elt=fd_getint(scaled[j]);
          longs[j]=elt;
          j++;}
        result=fd_make_long_vector(vlen,longs);
        u8_free(longs); u8_free(scaled);}
      else {
        result=fd_make_vector(vlen,scaled);
        u8_free(scaled);}
      return result;}}
  else return generic_vector_scale(vec,scalar);
}


/* Initialization stuff */

void fd_init_numbers_c()
{
  if (fd_unparsers[fd_flonum_type] == NULL)
    fd_unparsers[fd_flonum_type]=unparse_flonum;
  if (fd_unparsers[fd_bigint_type] == NULL)
    fd_unparsers[fd_bigint_type]=unparse_bigint;

  fd_unparsers[fd_rational_type]=unparse_rational;
  fd_unparsers[fd_complex_type]=unparse_complex;
  fd_unparsers[fd_numeric_vector_type]=unparse_numeric_vector;

  fd_copiers[fd_flonum_type]=copy_flonum;
  fd_copiers[fd_bigint_type]=copy_bigint;
  fd_copiers[fd_numeric_vector_type]=copy_numeric_vector;

  fd_recyclers[fd_flonum_type]=recycle_flonum;
  fd_recyclers[fd_bigint_type]=recycle_bigint;
  fd_recyclers[fd_numeric_vector_type]=recycle_numeric_vector;

  fd_comparators[fd_flonum_type]=compare_flonum;
  fd_comparators[fd_bigint_type]=compare_bigint;
  fd_comparators[fd_numeric_vector_type]=compare_numeric_vector;

  fd_hashfns[fd_bigint_type]=hash_bigint;
  fd_hashfns[fd_flonum_type]=hash_flonum;
  fd_hashfns[fd_numeric_vector_type]=hash_numeric_vector;

  fd_dtype_writers[fd_bigint_type]=dtype_bigint;
  fd_dtype_writers[fd_flonum_type]=dtype_flonum;

  fd_register_packet_unpacker
    (dt_numeric_package,dt_double,unpack_flonum);
  fd_register_packet_unpacker
    (dt_numeric_package,dt_bigint,unpack_bigint);

  bigint_magic_modulus=fd_long_to_bigint(256001281);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
