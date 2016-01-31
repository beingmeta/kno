/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
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

/* Utility structures and definitions */

#if (SIZEOF_LONG_LONG == SIZEOF_VOID_P)
typedef unsigned long long fdtype;
typedef unsigned long long fd_ptrbits;
typedef unsigned long long fd_wideint;
#elif (SIZEOF_LONG == SIZEOF_VOID_P)
typedef unsigned long fdtype;
typedef unsigned long fd_ptrbits;
typedef unsigned long fd_wideint;
#else
typedef unsigned int fdtype;
typedef unsigned int fd_ptrbits;
typedef unsigned int fd_wideint;
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

#ifndef HAVE_UCHAR
typedef unsigned char uchar;
#endif

#include <libu8/libu8.h>
#include <libu8/u8ctype.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>

/* Utility functions */

#define FD_INIT_STRUCT(s,sname) memset(s,0,sizeof(sname));

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

FD_EXPORT fd_exception fd_UnexpectedEOD, fd_UnexpectedEOF;
FD_EXPORT fd_exception fd_ParseError, fd_ParseArgError, fd_TypeError;
FD_EXPORT fd_exception fd_DTypeError, fd_InconsistentDTypeSize;
FD_EXPORT fd_exception fd_RangeError, fd_BadEscapeSequence, fd_ConstantTooLong;
FD_EXPORT fd_exception fd_CantParseRecord, fd_CantUnparse, fd_InvalidConstant;
FD_EXPORT fd_exception fd_MissingOpenQuote, fd_MissingCloseQuote;
FD_EXPORT fd_exception fd_InvalidHexChar, fd_InvalidBase64Char;
FD_EXPORT fd_exception fd_InvalidCharacterConstant, fd_BadAtom;
FD_EXPORT fd_exception fd_NoPointerExpressions, fd_BadPointerRef;
FD_EXPORT fd_exception fd_CantOpenFile, fd_FileNotFound;

#include "malloc.h"
#include "dtypeio.h"

/* Threads */

#if FD_THREADS_ENABLED
#define fd_lock_mutex(x) u8_lock_mutex(x)
#define fd_unlock_mutex(x) u8_unlock_mutex(x)
#define fd_read_lock(x) u8_read_lock(x)
#define fd_write_lock(x) u8_write_lock(x)
#define fd_rw_unlock(x) u8_rw_unlock(x)
#define fd_init_mutex(x) u8_init_mutex(x)
#define fd_destroy_mutex(x) u8_destroy_mutex(x)
#define fd_init_rwlock(x) u8_init_rwlock(x)
#define fd_destroy_rwlock(x) u8_destroy_rwlock(x)
#define fd_condvar_wait(x,y) u8_condvar_wait(x,y)
#define fd_init_condvar(x) u8_init_condvar(x)
#define fd_destroy_condvar(x) u8_destroy_condvar(x)
#define fd_tld_get(key) (u8_tld_get(key))
#define fd_tld_set(key,v) (u8_tld_set(key,v))
#define fd_lock_struct(p) (u8_lock_mutex(&((p)->lock)))
#define fd_unlock_struct(p) (u8_unlock_mutex(&((p)->lock)))
#define fd_locked_struct(p) (u8_lock_mutex(&((p)->lock)),(p))
#define fd_read_lock_struct(p) (u8_read_lock(&((p)->rwlock)))
#define fd_write_lock_struct(p) (u8_write_lock(&((p)->rwlock)))
#define fd_rw_unlock_struct(p) (u8_rw_unlock(&((p)->rwlock)))

#else
typedef void *u8_tld_key;
#define fd_lock_mutex(x)
#define fd_unlock_mutex(x)
#define fd_read_lock(x)
#define fd_write_lock(x)
#define fd_rw_unlock(x)
#define fd_init_mutex(x)
#define fd_destroy_mutex(x)
#define fd_condvar_wait(x,y) (1)
#define fd_init_condvar(x)
#define fd_destroy_condvar(x)
#define fd_lock_struct(p)
#define fd_unlock_struct(p)
#define fd_read_lock_struct(p)
#define fd_write_lock_struct(p)
#define fd_rw_unlock_struct(p)
#define fd_locked_struct(p) (p)
#define fd_tld_get(key) ((*key))
#define fd_tld_set(key,v) ((*key)=v)
#endif

/* Generic support */

#include "support.h"

/* Compile-time DTYPE checking */

#ifndef FD_CHECKFDTYPE
#define FD_CHECKFDTYPE FD_DEFAULT_CHECKFDTYPE
#endif

#endif /* ndef FRAMERD_COMMON_H */
