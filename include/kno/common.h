/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
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

#if ( HAVE_STDATOMIC_H && KNO_LOCKFREE_REFCOUNTS )
#include <stdatomic.h>
#endif

/* Utility structures and definitions */

#if (SIZEOF_INT == SIZEOF_VOID_P)
typedef unsigned int lispval;
typedef unsigned int lisp_ptr;
typedef unsigned int kno_lispval;
typedef unsigned int kno_ptrbits;
typedef unsigned int kno_wideint;
#define SIZEOF_LISPVAL SIZEOF_LONG
#elif (SIZEOF_LONG == SIZEOF_VOID_P)
typedef unsigned long lispval;
typedef unsigned long lisp_ptr;
typedef unsigned long kno_lispval;
typedef unsigned long kno_ptrbits;
typedef unsigned long kno_wideint;
#define SIZEOF_LISPVAL SIZEOF_LONG
#elif (SIZEOF_LONG_LONG == SIZEOF_VOID_P)
typedef unsigned long long lispval;
typedef unsigned long long lisp_ptr;
typedef unsigned long long kno_lispval;
typedef unsigned long long kno_ptrbits;
typedef unsigned long long kno_wideint;
#define SIZEOF_LISPVAL SIZEOF_LONG_LONG
#else
typedef unsigned int lispval;
typedef unsigned int lisp_ptr;
typedef unsigned int kno_lispval;
typedef unsigned int kno_ptrbits;
typedef unsigned int kno_wideint;
#define SIZEOF_LISPVAL SIZEOF_INT
#endif

typedef lispval *lispvec;

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

/* Important globals */

KNO_EXPORT int kno_lockdown;

#if ((KNO_THREADS_ENABLED)&&(KNO_USE_TLS))
KNO_EXPORT u8_tld_key kno_curthread_key;
#define kno_current_thread ((lispval)u8_tld_get(kno_curthread_key))
#define _kno_set_current_thread(threadptr) \
  u8_tld_set(kno_curthread_key,(threadptr))
#elif ((KNO_THREADS_ENABLED)&&(HAVE_THREAD_STORAGE_CLASS))
KNO_EXPORT __thread lispval kno_current_thread;
#define _kno_set_current_thread(threadptr) \
  kno_current_thread=((lispval)(threadptr))
#else
KNO_EXPORT lispval kno_current_thread
#define _kno_set_current_thread(threadptr) \
  kno_current_thread=((lispval)(threadptr))
#endif

/* Utility functions */

#define KNO_INIT_STRUCT(s,sname) memset(s,0,sizeof(sname));

KNO_EXPORT int kno_always_true(lispval x);
KNO_EXPORT int kno_always_false(lispval x);

typedef int (*kno_lisp_predicate)(lispval x);

/* Built in stuff */

#include <stdlib.h>
#include <string.h>
#include <limits.h>

#ifndef KNO_INLINE_FCN
#if KNO_AVOID_INLINE
#define KNO_INLINE_FCN static U8_MAYBE_UNUSED
#else
#define KNO_INLINE_FCN U8_INLINE_FCN
#endif
#endif

#define KNO_ISLOCKED 1
#define KNO_UNLOCKED 0

KNO_EXPORT int kno_init_iobase(void);

KNO_EXPORT int kno_init_errbase(void);
#define kno_whoops(ex) kno_raisex(ex,NULL,NULL)

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

KNO_EXPORT u8_condition kno_ThreadTerminated, kno_ThreadInterrupted;

#include "malloc.h"
#include "dtypeio.h"

/* Generic support */

#include "support.h"
#include "errobjs.h"

#endif /* ndef KNO_COMMON_H */

