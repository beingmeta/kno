/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_FDGMP_H
#define FRAMERD_FDGMP_H 1
#define FRAMERD_FDGMP_H_VERSION "$Id: fdgmp.h,v 1.4 2006/01/26 14:44:32 haase Exp $"

#include <gmp.h>

FD_EXPORT fd_ptr_type fd_gmp_type;

enum FD_GMP_TYPE {
  fd_gmp_integer, fd_gmp_flonum, fd_gmp_rational };

struct FD_GMP {
  FD_CONS_HEADER;
  enum FD_GMP_TYPE type;
  union {
    mpz_t integer;
    mpf_t flonum;
    mpq_t rational;}
    value;};

#endif /* #ifndef FRAMERD_FDGMP_H */
