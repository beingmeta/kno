/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_COMMON_H
#define FRAMERD_COMMON_H 1
#ifndef FRAMERD_COMMON_H_INFO
#define FRAMERD_COMMON_H_INFO "include/framerd/common.h"
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "defines.h"

#if ( HAVE_STDATOMIC_H && FD_LOCKFREE_REFCOUNTS && FD_INLINE_REFCOUNTS )
#include <stdatomic.h>
#endif

/* Utility structures and definitions */

#if (SIZEOF_LONG == SIZEOF_VOID_P)
typedef unsigned long lispval;
typedef unsigned long fd_ptrbits;
typedef unsigned long fd_wideint;
#define SIZEOF_LISPVAL SIZEOF_LONG
#elif (SIZEOF_LONG_LONG == SIZEOF_VOID_P)
typedef unsigned long long lispval;
typedef unsigned long long fd_ptrbits;
typedef unsigned long long fd_wideint;
#define SIZEOF_LISPVAL SIZEOF_LONG_LONG
#else
typedef unsigned int lispval;
typedef unsigned int fd_ptrbits;
typedef unsigned int fd_wideint;
#define SIZEOF_LISPVAL SIZEOF_INT
#endif

#define LISPVAL_LEN (sizeof(lispval))
#define LISPVEC_BYTELEN(n) (sizeof(lispval)*(n))

#if (SIZEOF_LONG == 8)
typedef unsigned long fd_8bytes;
#elif (SIZEOF_LONG_LONG == 8)
typedef unsigned long long fd_8bytes;
#endif

#if (SIZEOF_INT == 4)
typedef unsigned int fd_4bytes;
#elif (SIZEOF_LONG == 4)
typedef unsigned long fd_4bytes;
#endif

#ifndef HAVE_UCHAR
typedef unsigned char uchar;
#endif

#include <libu8/libu8.h>
#include <libu8/u8ctype.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

#define fd_alloc(n)  (u8_alloc_n(lispval,(n)))
#define fd_alloca(n) (alloca(SIZEOF_LISPVAL*(n)))

/* Utility functions */

#define FD_INIT_STRUCT(s,sname) memset(s,0,sizeof(sname));

/* Built in stuff */

#include <stdlib.h>
#include <string.h>
#include <limits.h>

#ifndef FD_INLINE_FCN
#define FD_INLINE_FCN U8_INLINE_FCN
#endif

#define FD_ISLOCKED 1
#define FD_UNLOCKED 0

FD_EXPORT int fd_init_iobase(void);

FD_EXPORT int fd_init_errbase(void);
#define fd_whoops(ex) u8_raise(ex,NULL,NULL)

FD_EXPORT u8_condition fd_UnexpectedEOD, fd_UnexpectedEOF;
FD_EXPORT u8_condition fd_ParseError, fd_ParseArgError, fd_TypeError;
FD_EXPORT u8_condition fd_DTypeError, fd_InconsistentDTypeSize;
FD_EXPORT u8_condition fd_RangeError, fd_DisorderedRange;
FD_EXPORT u8_condition fd_BadEscapeSequence, fd_ConstantTooLong;
FD_EXPORT u8_condition fd_CantParseRecord, fd_CantUnparse, fd_InvalidConstant;
FD_EXPORT u8_condition fd_MissingOpenQuote, fd_MissingCloseQuote;
FD_EXPORT u8_condition fd_InvalidHexChar, fd_InvalidBase64Char;
FD_EXPORT u8_condition fd_InvalidCharacterConstant, fd_BadAtom;
FD_EXPORT u8_condition fd_NoPointerExpressions, fd_BadPointerRef;
FD_EXPORT u8_condition fd_FileNotFound, fd_NoSuchFile;

#include "malloc.h"
#include "dtypeio.h"

/* Generic support */

#include "support.h"
#include "errobjs.h"

#endif /* ndef FRAMERD_COMMON_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
