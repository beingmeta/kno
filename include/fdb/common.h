/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_COMMON_H
#define FDB_COMMON_H 1
#define FDB_COMMON_H_VERSION "$Id$"

#define _GNU_SOURCE

#include "defines.h"

/* Utility structures */

#if (SIZEOF_LONG_LONG == SIZEOF_VOID_P)
typedef unsigned long long fdtype;
typedef unsigned long long fd_ptrbits;
#elif (SIZEOF_LONG == SIZEOF_VOID_P)
typedef unsigned long fdtype;
typedef unsigned long fd_ptrbits;
#else
typedef unsigned int fdtype;
typedef unsigned int fd_ptrbits;
#endif

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

#include <libu8/libu8.h>
#include <libu8/u8ctype.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

/* Built in stuff */

#include <stdlib.h>
#include <string.h>

#ifndef FD_INLINE_FCN
#define FD_INLINE_FCN U8_INLINE_FCN
#endif

typedef u8_condition fd_exception;

FD_EXPORT int fd_init_iobase(void);

FD_EXPORT int fd_init_errbase(void);
#define fd_whoops(ex) u8_raise(ex,NULL,NULL)

FD_EXPORT fd_exception
fd_UnexpectedEOD, fd_UnexpectedEOF, fd_ParseError, fd_TypeError, fd_DTypeError,
  fd_RangeError, fd_BadEscapeSequence, fd_ConstantTooLong,
  fd_CantParseRecord, fd_CantUnparse, fd_InvalidConstant,
  fd_InvalidCharacterConstant, fd_BadAtom, fd_NoPointerExpressions,
  fd_BadPointerRef, fd_CantOpenFile, fd_CantFindFile;

#include "malloc.h"
#include "dtypeio.h"

/* Threads */

#if FD_THREADS_ENABLED
#define fd_lock_mutex(x) u8_lock_mutex(x)
#define fd_unlock_mutex(x) u8_unlock_mutex(x)
#define fd_init_mutex(x) u8_init_mutex(x)
#define fd_destroy_mutex(x) u8_destroy_mutex(x)
#define fd_condvar_wait(x,y) u8_condvar_wait(x,y)
#define fd_init_condvar(x) u8_init_condvar(x)
#define fd_destroy_condvar(x) u8_destroy_condvar(x)
#define fd_tld_get(key) (u8_tld_get(key))
#define fd_tld_set(key,v) (u8_tld_set(key,v))
#define fd_lock_struct(p) (u8_lock_mutex(&((p)->lock)))
#define fd_unlock_struct(p) (u8_unlock_mutex(&((p)->lock)))
#define fd_locked_struct(p) (u8_lock_mutex(&((p)->lock)),(p))
#else
typedef void *u8_tld_key;
#define fd_lock_mutex(x)
#define fd_unlock_mutex(x)
#define fd_init_mutex(x)
#define fd_destroy_mutex(x)
#define fd_condvar_wait(x,y) (1)
#define fd_init_condvar(x)
#define fd_destroy_condvar(x)
#define fd_lock_struct(p)
#define fd_unlock_struct(p)
#define fd_locked_struct(p) (p)
#define fd_tld_get(key) ((*key))
#define fd_tld_set(key,v) ((*key)=v)
#endif

/* Generic support */

#include "support.h"

#endif /* ndef FDB_COMMON_H */
