/* -*-C-*-

Copyright (c) 1989-1992 Massachusetts Institute of Technology

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

/* Internal Interface to Bigint Code */

#undef BIGINT_ZERO_P
#undef BIGINT_NEGATIVE_P

/* The memory model is based on the following definitions, and on the
   definition of the type `kno_bigint'.  The only other special
   definition is `CHAR_BIT', which is defined in the Ansi C header
   file "limits.h". */

/*MIT Scheme used longs for these.  KNO likes 4byte chunks.*/
typedef int bigint_digit_type;
typedef int bigint_length_type;

#define BIGINT_ALLOCATE bigint_malloc
#define BIGINT_TO_POINTER(bigint) ((bigint_digit_type *) (bigint))
#define BIGINT_REDUCE_LENGTH(target, source, length)			\
  (target) = (bigint_realloc ((source), (length)))
#define BIGINT_DEALLOCATE(x) kno_decref((lispval)x)
#define BIGINT_FORCE_NEW_RESULTS
#define fast register
extern void abort ();

#define BIGINT_EXCEPTION() u8_raise(kno_BigIntException,NULL,NULL)


#define BIGINT_DIGIT_LENGTH (((sizeof (bigint_digit_type)) * CHAR_BIT) - 2)
#define BIGINT_HALF_DIGIT_LENGTH (BIGINT_DIGIT_LENGTH / 2)
#define BIGINT_RADIX (((unsigned long) 1) << BIGINT_DIGIT_LENGTH)
#define BIGINT_RADIX_ROOT (((unsigned long) 1) << BIGINT_HALF_DIGIT_LENGTH)
#define BIGINT_DIGIT_MASK	 (BIGINT_RADIX - 1)
#define BIGINT_HALF_DIGIT_MASK	 (BIGINT_RADIX_ROOT - 1)
#define CONS_HEADER_SIZE (sizeof(struct KNO_CONS)/sizeof(bigint_digit_type))

#define BIGINT_HEADER(bigint)					\
  ((BIGINT_TO_POINTER (bigint)) + CONS_HEADER_SIZE)
#define BIGINT_START_PTR(bigint)					\
  ((BIGINT_TO_POINTER (bigint)) + CONS_HEADER_SIZE + 1)

#define BIGINT_SET_HEADER(bigint, length, negative_p)			\
  (* (BIGINT_HEADER (bigint))) =					\
    ((length) | ((negative_p) ? BIGINT_RADIX : 0))

#define BIGINT_LENGTH(bigint)						\
  ((bigint_length_type)((* (BIGINT_HEADER (bigint))) & BIGINT_DIGIT_MASK))

#define BIGINT_NEGATIVE_P(bigint)					\
  (((* (BIGINT_HEADER (bigint))) & BIGINT_RADIX) != 0)

#define BIGINT_ZERO_P(bigint)						\
  ((BIGINT_LENGTH (bigint)) == 0)

#define BIGINT_REF(bigint, index)					\
  (* ((BIGINT_START_PTR (bigint)) + (index)))

#ifdef BIGINT_FORCE_NEW_RESULTS
#define BIGINT_MAYBE_COPY bigint_copy
#else
#define BIGINT_MAYBE_COPY(bigint) bigint
#endif

/* These definitions are here to facilitate caching of the constants
   0, 1, and -1. */
#define BIGINT_ZERO bigint_make_zero
#define BIGINT_ONE bigint_make_one

#define HD_LOW(digit) ((digit) & BIGINT_HALF_DIGIT_MASK)
#define HD_HIGH(digit) ((digit) >> BIGINT_HALF_DIGIT_LENGTH)
#define HD_CONS(high, low) (((high) << BIGINT_HALF_DIGIT_LENGTH) | (low))

#define BIGINT_BITS_TO_DIGITS(n)					\
  (((n) + (BIGINT_DIGIT_LENGTH - 1)) / BIGINT_DIGIT_LENGTH)

#define BIGINT_DIGITS_FOR_LONG						\
  (BIGINT_BITS_TO_DIGITS ((sizeof (long)) * CHAR_BIT))
#define BIGINT_DIGITS_FOR_LONG_LONG					\
  (BIGINT_BITS_TO_DIGITS ((sizeof (long long)) * CHAR_BIT))

#ifndef BIGINT_DISABLE_ASSERTION_CHECKS

#define BIGINT_ASSERT(expression)					\
{									\
  if (! (expression))							\
    BIGINT_EXCEPTION ();						\
}

#endif /* not BIGINT_DISABLE_ASSERTION_CHECKS */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
