/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

/* Initial definitions */

#ifndef FDB_DEFINES_H
#define FDB_DEFINES_H 1
#define FDB_DEFINES_H_VERSION "$Id: defines.h,v 1.22 2006/01/26 14:44:32 haase Exp $"

#include "config.h"
#include "conf-defines.h"

#define FD_EXPORT extern
#ifndef FD_INLINE
#define FD_INLINE 0
#endif

#ifndef WORDS_BIGENDIAN
#define WORDS_BIGENDIAN 0
#endif

#ifndef FD_FASTOP
#define FD_FASTOP static
#endif

#ifndef FD_PROFILING_ENABLED
#define FD_PROFILING_ENABLED 0
#endif

#ifndef HAVE_CONSTRUCTOR_ATTRIBUTES
#define HAVE_CONSTRUCTOR_ATTRIBUTES 0
#endif

#ifndef FD_GLOBAL_IPEVAL
#define FD_GLOBAL_IPEVAL 0
#endif

#if ((FD_LARGEFILES_ENABLED) && (HAVE_FSEEKO))
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE_SOURCE 1
#define _LARGEFILE64_SOURCE 1
#else
#define fseeko(f,pos,op) fseek(f,pos,op)
#define ftello(f) ftell(f)
#endif

#ifndef FD_THREADS_ENABLED
#define FD_THREADS_ENABLED 1
#endif

#if FD_THREADS_ENABLED
#ifndef FD_N_PTRLOCKS
#define FD_N_PTRLOCKS 32
#endif
#endif

#if FD_THREADS_ENABLED
#if HAVE_PTHREAD_H
#include <pthread.h>
#define HAVE_POSIX_THREADS 1
#else
#define HAVE_POSIX_THREADS 0
#endif
#endif

#ifndef HAVE_THREAD_STORAGE_CLASS
#define HAVE_THREAD_STORAGE_CLASS 0
#endif

#if (FD_THREADS_ENABLED==0)
#define FD_USE_TLS 0
#define FD_USE__THREAD 0
#define FD_THREADVAR
#elif (FD_FORCE_TLS)
#define FD_USE_TLS 1
#define FD_USE__THREAD 0
#elif (!(HAVE_THREAD_STORAGE_CLASS))
#define FD_USE_TLS 1
#define FD_USE__THREAD 0
#else /* Able to use __thread */
#define FD_USE_TLS 0
#define FD_USE__THREAD 1
#define FD_THREADVAR __thread
#endif

#ifndef FD_INLINE_CHOICES
#define FD_INLINE_CHOICES 0
#endif

#ifndef FD_INLINE_TABLES
#define FD_INLINE_TABLES 0
#endif

#ifndef FD_MEMORY_POOL_TYPE
#define FD_MEMORY_POOL_TYPE void
#endif

#ifndef FD_TYPE_MAX
#define FD_TYPE_MAX 256
#endif

#ifndef  FD_N_POOL_OPENERS
#define FD_N_POOL_OPENERS 32
#endif

#ifndef  FD_N_INDEX_OPENERS
#define FD_N_INDEX_OPENERS 32
#endif

#ifndef FD_TRACE_IPEVAL
#define FD_TRACE_IPEVAL 1
#endif

#ifndef FD_PREFETCHING_ENABLED
#define FD_PREFETCHING_ENABLED 1
#endif

#ifndef FD_PTR_TYPE_MACRO
#define FD_PTR_TYPE_MACRO 1
#endif

#if HAVE_CONSTRUCTOR_ATTRIBUTES
#define FD_LIBINIT_FN __attribute__ ((constructor))
#define FD_DO_LIBINIT(fn) fn
#else
#define FD_LIBINIT_FN 
#define FD_DO_LIBINIT(fn) fn()
#endif

#if HAVE_BUILTIN_EXPECT
#define FD_EXPECT_TRUE(x) (__builtin_expect(x,1))
#define FD_EXPECT_FALSE(x) (__builtin_expect(x,0))
#else
#define FD_EXPECT_TRUE(x) (x)
#define FD_EXPECT_FALSE(x) (x)
#endif

#if HAVE_BUILTIN_PREFETCH
#define FD_PREFETCH(x) __builtin_prefetch(x)
#define FD_WRITE_PREFETCH(x) __builtin_prefetch(x,1)
#else
#define FD_PREFETCH(x)
#define FD_WRITE_PREFETCH(x)
#endif

#define _(x) (x)

/* How to implement OIDs */

#ifndef FD_STRUCT_OIDS
#if (SIZEOF_INT == 8)
#define FD_STRUCT_OIDS 0
#define FD_LONG_OIDS 0
#define FD_LONG_LONG_OIDS 0
#define FD_INT_OIDS 1
#elif (SIZEOF_LONG == 8)
#define FD_STRUCT_OIDS 0
#define FD_LONG_OIDS 1
#define FD_LONG_LONG_OIDS 0
#define FD_INT_OIDS 0
#elif (SIZEOF_LONG_LONG == 8)
#define FD_STRUCT_OIDS 0
#define FD_LONG_OIDS 0
#define FD_LONG_LONG_OIDS 1
#define FD_INT_OIDS 0
#else
#define FD_STRUCT_OIDS 1
#define FD_LONG_OIDS 0
#define FD_LONG_LONG_OIDS 1
#define FD_INT_OIDS 0
#endif
#endif

#endif /* FDB_DEFINES_H */

