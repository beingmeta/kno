/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_COMMON_H
#define FDB_COMMON_H 1
#define FDB_COMMON_H_VERSION "$Id: common.h,v 1.32 2006/01/26 14:44:32 haase Exp $"

#define _GNU_SOURCE

#include "defines.h"

/* Utility structures */

#if (SIZEOF_LONG_LONG == SIZEOF_VOID_P)
typedef unsigned long long fdtype;
#elif (SIZEOF_LONG == SIZEOF_VOID_P)
typedef unsigned long fdtype;
#else
typedef unsigned int fdtype;
#endif

#include "libu8/u8.h"

/* Built in stuff */

#include <stdlib.h>
#include <string.h>

typedef u8_condition fd_exception;

FD_EXPORT int fd_init_iobase(void);

FD_EXPORT int fd_init_errbase(void);
#define fd_whoops(ex) u8_raise(ex,NULL,NULL)

FD_EXPORT fd_exception
  fd_UnexpectedEOD, fd_ParseError, fd_TypeError, fd_DTypeError,
  fd_RangeError, fd_BadEscapeSequence, fd_ConstantTooLong,
  fd_CantParseRecord, fd_CantUnparse, fd_InvalidConstant,
  fd_InvalidCharacterConstant, fd_BadAtom, fd_NoPointerExpressions,
  fd_BadPointerRef, fd_CantOpenFile, fd_CantFindFile;

#include "malloc.h"
#include "dtypeio.h"

/* Generic support */

#include "support.h"

#endif /* ndef FDB_COMMON_H */
