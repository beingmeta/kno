/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2011 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

/* Initial definitions */

#ifndef FDB_DEFINES_H
#define FDB_DEFINES_H 1
#define FDB_DEFINES_H_VERSION "$Id$"

#include "config.h"

#define FD_EXPORT extern
#ifndef FD_INLINE
#define FD_INLINE 0
#endif

#ifndef WORDS_BIGENDIAN
#define WORDS_BIGENDIAN 0
#endif

#ifndef FD_FASTOP
#define FD_FASTOP static inline
#endif

/* This is for C profiling and helps generate code which is easier to profile. */
#ifndef FD_PROFILING_ENABLED
#define FD_PROFILING_ENABLED 0
#endif

#ifndef HAVE_CONSTRUCTOR_ATTRIBUTES
#define HAVE_CONSTRUCTOR_ATTRIBUTES 0
#endif

#ifndef FD_GLOBAL_IPEVAL
#define FD_GLOBAL_IPEVAL 0
#endif

#ifndef FD_MMAP_PREFETCH_WINDOW
#define FD_MMAP_PREFETCH_WINDOW 0
#endif

/* This is set to make incref/decref into no-ops, which is helpful
   for ablative benchmarking.  */
#ifndef FD_NO_GC
#define FD_NO_GC 0
#endif

/* Pointer Checking */

/* There are two global defines which control pointer checking.
   FD_PTR_DEBUG_LEVEL controls what kind of pointer checking to do,
    0 = no checking
    1 = check non-null
    2 = check integrity of immediates 
    3 = check integrity of cons pointers
   FD_PTR_DEBUG_DENSITY controls how detailed the debug checking is,
    by enabling macros of the form FD_PTRCHECKn and FD_CHECK_PTRn,
    which would otherwise be noops.
   System and user C code can call different pointer-checking macros
    which can be selectively enabled on a per-file basis.
   In addition, defining FD_PTR_DEBUG_DENSITY to be < 0 causes references
   to use the variable fd_ptr_debug_density to determine the debugging
   density.
*/
#ifndef FD_PTR_DEBUG_LEVEL
#define FD_PTR_DEBUG_LEVEL 1
#endif

/* This determines when to check for pointers (as controlled above)
   There are four basic levels, with zero being no checking
*/
#ifndef FD_PTR_DEBUG_DENSITY
#define FD_PTR_DEBUG_DENSITY 1
#endif

/* This is true (1) for executables built in the tests/ subdirectories.
   If true, the executables call the various module init functions 
   (e.g. fd_init_texttools()) directly; when dynamically linked, the
   loader calls them.  This is neccessary because static linking (at
   least on some platforms) doesn't invoke the library initializers
   at startup.  */
#ifndef FD_TESTCONFIG
#define FD_TESTCONFIG 0
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

#if ((FD_INLINE_CHOICES)||(FD_INLINE_TABLES))
#define FD_INLINE_COMPARE 1
#elif (!(defined(FD_INLINE_COMPARE)))
#define FD_INLINE_COMPARE 0
#endif

#ifndef FD_INLINE_TABLES
#define FD_INLINE_TABLES 0
#endif

#ifndef FD_USE_THREADCACHE
#define FD_USE_THEADCACHE 1
#endif

#ifndef FD_WRITETHROUGH_THREADCACHE
#define FD_WRITETHROUGH_THEADCACHE 0
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

#ifndef FD_SCHEME_BUILTINS
#define FD_INIT_SCHEME_BUILTINS() fd_init_fdscheme()
#else
#define FD_INIT_SCHEME_BUILTINS() FD_SCHEME_BUILTINS
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

/* Ints that hold pointers */

#if (SIZEOF_VOID_P==SIZEOF_INT)
#define FD_INTPTR unsigned int
#elif (SIZEOF_VOID_P==SIZEOF_LONG)
#define FD_INTPTR unsigned long
#elif (SIZEOF_VOID_P==SIZEOF_LONG_LONG)
#define FD_INTPTR unsigned long long
#else
#define FD_INTPTR unsigned long long
#endif

/* Fastcgi configuration */

#ifndef HAVE_FCGIAPP_H
#define HAVE_FCGIAPP_H 0
#endif

#ifndef HAVE_LIBFCGI
#define HAVE_LIBFCGI 0
#endif

#if ((WITH_FASTCGI) && (HAVE_FCGIAPP_H) && (HAVE_LIBFCGI))
#define FD_WITH_FASTCGI 1
#else
#define FD_WITH_FASTCGI 0
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

