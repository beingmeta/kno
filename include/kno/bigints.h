/* -*-C-*-

    Copyright (c) 1989-1992 Massachusetts Institute of Technology
    Copyright (c) 1992-2001 Massachusetts Institute of Technology
    Copyright (c) 2001-2019 beingmeta, inc.

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

#ifndef KNO_BIGINTS_H
#define KNO_BIGINTS_H 1

KNO_EXPORT u8_condition kno_BigIntException;

/* External Interface to Bigint Code */

/* The `unsigned long' type is used for the conversion procedures
   `bigint_to_long' and `long_to_bigint'.  Older implementations of C
   don't support this type; if you have such an implementation you can
   disable these procedures using the following flag (alternatively
   you could write alternate versions that don't require this type). */
/* #define BIGINT_NO_ULONG */


#define BIGINT_OUT_OF_BAND ((kno_bigint) 0)

enum kno_bigint_comparison
{
  kno_bigint_equal, kno_bigint_less, kno_bigint_greater
};

KNO_EXPORT kno_bigint kno_bigint_make_zero(void);
KNO_EXPORT kno_bigint kno_bigint_make_one(int negative_p);
KNO_EXPORT int kno_bigint_equal_p(kno_bigint, kno_bigint);
KNO_EXPORT enum kno_bigint_comparison kno_bigint_test(kno_bigint);
KNO_EXPORT enum kno_bigint_comparison kno_bigint_compare
(kno_bigint, kno_bigint);
KNO_EXPORT kno_bigint kno_bigint_add(kno_bigint, kno_bigint);
KNO_EXPORT kno_bigint kno_bigint_subtract(kno_bigint, kno_bigint);
KNO_EXPORT kno_bigint kno_bigint_negate(kno_bigint);
KNO_EXPORT kno_bigint kno_bigint_multiply(kno_bigint, kno_bigint);
KNO_EXPORT int kno_bigint_divide
		  (kno_bigint numerator, kno_bigint denominator,
		   kno_bigint * quotient, kno_bigint * remainder);
KNO_EXPORT kno_bigint kno_bigint_quotient(kno_bigint, kno_bigint);
KNO_EXPORT kno_bigint kno_bigint_remainder(kno_bigint, kno_bigint);
KNO_EXPORT kno_bigint kno_long_to_bigint(long);
KNO_EXPORT kno_bigint kno_ulong_to_bigint(unsigned long);
KNO_EXPORT long kno_bigint_to_long(kno_bigint);
KNO_EXPORT kno_bigint kno_double_to_bigint(double);
KNO_EXPORT double kno_bigint_to_double(kno_bigint);
KNO_EXPORT int kno_bigint_fits_in_word_p(kno_bigint,long width,int twosc);
KNO_EXPORT unsigned long kno_bigint_length_in_bytes(kno_bigint);
KNO_EXPORT kno_bigint kno_bigint_length_in_bits(kno_bigint);
KNO_EXPORT kno_bigint kno_bigint_length_upper_limit(void);
KNO_EXPORT kno_bigint kno_digit_stream_to_bigint
			  (unsigned int n_digits,
			   unsigned int (*producer)(),
			   void * context,
			   unsigned int radix, int negative_p);
KNO_EXPORT void kno_bigint_to_digit_stream
		   (kno_bigint, unsigned int radix,
		    void (*consumer)(),
		    void * context);
KNO_EXPORT long kno_bigint_max_digit_stream_radix(void);


#endif /* KNO_BIGINTS_H */
