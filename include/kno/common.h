/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_COMMON_H
#define KNO_COMMON_H 1
#ifndef KNO_COMMON_H_INFO
#define KNO_COMMON_H_INFO "include/kno/common.h"
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "defines.h"

#if ( HAVE_STDATOMIC_H && KNO_LOCKFREE_REFCOUNTS && KNO_INLINE_REFCOUNTS )
#include <stdatomic.h>
#endif

/* Utility structures and definitions */

#if (SIZEOF_INT == SIZEOF_VOID_P)
typedef unsigned int lispval;
typedef unsigned int kno_ptrbits;
typedef unsigned int kno_wideint;
#define SIZEOF_LISPVAL SIZEOF_LONG
#elif (SIZEOF_LONG == SIZEOF_VOID_P)
typedef unsigned long lispval;
typedef unsigned long kno_ptrbits;
typedef unsigned long kno_wideint;
#define SIZEOF_LISPVAL SIZEOF_LONG
#elif (SIZEOF_LONG_LONG == SIZEOF_VOID_P)
typedef unsigned long long lispval;
typedef unsigned long long kno_ptrbits;
typedef unsigned long long kno_wideint;
#define SIZEOF_LISPVAL SIZEOF_LONG_LONG
#else
typedef unsigned int lispval;
typedef unsigned int kno_ptrbits;
typedef unsigned int kno_wideint;
#define SIZEOF_LISPVAL SIZEOF_INT
#endif

#define LISPVAL_LEN (sizeof(lispval))
#define LISPVEC_BYTELEN(n) (sizeof(lispval)*(n))

#if (SIZEOF_LONG == 8)
typedef unsigned long kno_8bytes;
#elif (SIZEOF_LONG_LONG == 8)
typedef unsigned long long kno_8bytes;
#endif

#if (SIZEOF_INT == 4)
typedef unsigned int kno_4bytes;
#elif (SIZEOF_LONG == 4)
typedef unsigned long kno_4bytes;
#endif

#ifndef HAVE_UCHAR
typedef unsigned char uchar;
#endif

#include <libu8/libu8.h>
#include <libu8/u8ctype.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

#define kno_alloc(n)  (u8_alloc_n(lispval,(n)))
#define kno_alloca(n) (alloca(SIZEOF_LISPVAL*(n)))

/* Utility functions */

#define KNO_INIT_STRUCT(s,sname) memset(s,0,sizeof(sname));

/* Built in stuff */

#include <stdlib.h>
#include <string.h>
#include <limits.h>

#ifndef KNO_INLINE_FCN
#define KNO_INLINE_FCN U8_INLINE_FCN
#endif

#define KNO_ISLOCKED 1
#define KNO_UNLOCKED 0

KNO_EXPORT int kno_init_iobase(void);

KNO_EXPORT int kno_init_errbase(void);
#define kno_whoops(ex) u8_raise(ex,NULL,NULL)

KNO_EXPORT u8_condition kno_UnexpectedEOD, kno_UnexpectedEOF;
KNO_EXPORT u8_condition kno_ParseError, kno_ParseArgError, kno_TypeError;
KNO_EXPORT u8_condition kno_DTypeError, kno_InconsistentDTypeSize;
KNO_EXPORT u8_condition kno_RangeError, kno_DisorderedRange;
KNO_EXPORT u8_condition kno_BadEscapeSequence, kno_ConstantTooLong;
KNO_EXPORT u8_condition kno_CantParseRecord, kno_CantUnparse, kno_InvalidConstant;
KNO_EXPORT u8_condition kno_MissingOpenQuote, kno_MissingCloseQuote;
KNO_EXPORT u8_condition kno_InvalidHexChar, kno_InvalidBase64Char;
KNO_EXPORT u8_condition kno_InvalidCharacterConstant, kno_BadAtom;
KNO_EXPORT u8_condition kno_NoPointerExpressions, kno_BadPointerRef;
KNO_EXPORT u8_condition kno_FileNotFound, kno_NoSuchFile;

#include "malloc.h"
#include "dtypeio.h"

/* Generic support */

#include "support.h"
#include "errobjs.h"

#endif /* ndef KNO_COMMON_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
