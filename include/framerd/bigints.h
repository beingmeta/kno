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

#ifndef FD_BIGINTS_H
#define FD_BIGINTS_H 1

FD_EXPORT u8_condition fd_BigIntException;

/* External Interface to Bigint Code */

/* The `unsigned long' type is used for the conversion procedures
   `bigint_to_long' and `long_to_bigint'.  Older implementations of C
   don't support this type; if you have such an implementation you can
   disable these procedures using the following flag (alternatively
   you could write alternate versions that don't require this type). */
/* #define BIGINT_NO_ULONG */


#define BIGINT_OUT_OF_BAND ((fd_bigint) 0)

enum fd_bigint_comparison
{
  fd_bigint_equal, fd_bigint_less, fd_bigint_greater
};

FD_EXPORT fd_bigint fd_bigint_make_zero(void);
FD_EXPORT fd_bigint fd_bigint_make_one(int negative_p);
FD_EXPORT int fd_bigint_equal_p(fd_bigint, fd_bigint);
FD_EXPORT enum fd_bigint_comparison fd_bigint_test(fd_bigint);
FD_EXPORT enum fd_bigint_comparison fd_bigint_compare
(fd_bigint, fd_bigint);
FD_EXPORT fd_bigint fd_bigint_add(fd_bigint, fd_bigint);
FD_EXPORT fd_bigint fd_bigint_subtract(fd_bigint, fd_bigint);
FD_EXPORT fd_bigint fd_bigint_negate(fd_bigint);
FD_EXPORT fd_bigint fd_bigint_multiply(fd_bigint, fd_bigint);
FD_EXPORT int fd_bigint_divide
		  (fd_bigint numerator, fd_bigint denominator,
		   fd_bigint * quotient, fd_bigint * remainder);
FD_EXPORT fd_bigint fd_bigint_quotient(fd_bigint, fd_bigint);
FD_EXPORT fd_bigint fd_bigint_remainder(fd_bigint, fd_bigint);
FD_EXPORT fd_bigint fd_long_to_bigint(long);
FD_EXPORT fd_bigint fd_ulong_to_bigint(unsigned long);
FD_EXPORT long fd_bigint_to_long(fd_bigint);
FD_EXPORT fd_bigint fd_double_to_bigint(double);
FD_EXPORT double fd_bigint_to_double(fd_bigint);
FD_EXPORT int fd_bigint_fits_in_word_p(fd_bigint,long width,int twosc);
FD_EXPORT unsigned long fd_bigint_length_in_bytes(fd_bigint);
FD_EXPORT fd_bigint fd_bigint_length_in_bits(fd_bigint);
FD_EXPORT fd_bigint fd_bigint_length_upper_limit(void);
FD_EXPORT fd_bigint fd_digit_stream_to_bigint
			  (unsigned int n_digits,
			   unsigned int (*producer)(),
			   void * context,
			   unsigned int radix, int negative_p);
FD_EXPORT void fd_bigint_to_digit_stream
		   (fd_bigint, unsigned int radix,
		    void (*consumer)(),
		    void * context);
FD_EXPORT long fd_bigint_max_digit_stream_radix(void);


#endif /* FD_BIGINTS_H */
