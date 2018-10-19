/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Initial definitions */

#ifndef FRAMERD_DEFINES_H
#define FRAMERD_DEFINES_H 1
#ifndef FRAMERD_DEFINES_H_INFO
#define FRAMERD_DEFINES_H_INFO "include/framerd/defines.h"
#endif

#include "config.h"
#include "revision.h"

#define FD_EXPORT extern
#ifndef FD_INLINE
#define FD_INLINE 0
#endif

#ifndef FD_USE_MMAP
#if HAVE_MMAP
#if FD_WITHOUT_MMAP
#define FD_USE_MMAP 0
#else
#define FD_USE_MMAP 1
#endif
#else
#define FD_USE_MMAP 0
#endif
#endif

#ifndef WORDS_BIGENDIAN
#define WORDS_BIGENDIAN 0
#endif

#define FD_THREADS_ENABLED 1

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

#ifndef FD_USE_DTBLOCK
#define FD_USE_DTBLOCK 0
#endif

#define FD_WRITETHROUGH_THREADCACHE 1

#ifndef NO_ELSE
#define NO_ELSE {}
#endif

#if defined(FD_ENABLE_FFI)

#if ( (FD_ENABLE_FFI) && ( ! ( (HAVE_FFI_H) && (HAVE_LIBFFI) ) ) )
#undef FD_ENABLE_FFI
#define FD_ENABLE_FFI 0
#endif

#else /* not defined(FD_ENABLE_FFI) */

#if ( (HAVE_FFI_H) && (HAVE_LIBFFI) )
#define FD_ENABLE_FFI 1
#else
#define FD_ENABLE_FFI 0
#endif

#endif

#ifndef FD_INIT_UMASK
#define FD_INIT_UMASK ((mode_t) (S_IWOTH ))
#endif

/* Whether to have FD_CHECK_PTR check underlying CONS structs */
#ifdef FD_FULL_CHECK_PTR
#define FD_FULL_CHECK_PTR 1
/* It can be helpful to turn this off when doing certain kinds of
   thread debugging, since checking that a CONS is legitimate without
   locking counts as a race condition to some thread error
   detectors. */
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

#ifndef FD_LOCKFREE_REFCOUNTS
#if HAVE_STDATOMIC_H
#define FD_LOCKFREE_REFCOUNTS FD_ENABLE_LOCKFREE
#else
#define FD_LOCKFREE_REFCOUNTS 0
#endif
#endif

#ifndef FD_USE_ATOMIC
#if HAVE_STDATOMIC_H
#define FD_USE_ATOMIC 1
#else
#define FD_USE_ATOMIC 0
#endif
#endif

#if (HAVE_OFF_T)
#if ((FD_LARGEFILES_ENABLED)&&(SIZEOF_OFF_T==8))
typedef off_t fd_off_t;
#elif (FD_LARGEFILES_ENABLED)
typedef long long int fd_off_t;
#else
typedef off_t fd_off_t;
#endif
#else /* (!(HAVE_OFF_T)) */
#if (FD_LARGEFILES_ENABLED)
typedef long long int fd_off_t;
#else
typedef int fd_off_t;
#endif
#endif

#if (HAVE_SIZE_T)
#if ((FD_LARGEFILES_ENABLED)&&(SIZEOF_SIZE_T==8))
typedef ssize_t fd_size_t;
#elif (FD_LARGEFILES_ENABLED)
typedef long long int fd_size_t;
#else
typedef ssize_t fd_size_t;
#endif
#else /* (!(HAVE_SIZE_T)) */
#if (FD_LARGEFILES_ENABLED)
typedef long long int fd_size_t;
#else
typedef int fd_size_t;
#endif
#endif

#if ((FD_LARGEFILES_ENABLED) && (HAVE_FSEEKO))
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE_SOURCE 1
#define _LARGEFILE64_SOURCE 1
#else
#define fseeko(f,pos,op) fseek(f,pos,op)
#define ftello(f) ftell(f)
#endif

/* This can be configured with --with-nptrlocks.

   Larger values for this are useful when you're being more
   multi-threaded, since it avoids locking conflicts during reference
   count updates. Smaller values will reduce resources, but possibly
   not by much.

*/
#ifndef FD_N_PTRLOCKS
#define FD_N_PTRLOCKS 979
#endif

#if HAVE_PTHREAD_H
#include <pthread.h>
#define HAVE_POSIX_THREADS 1
#else
#define HAVE_POSIX_THREADS 0
#endif

#ifndef HAVE_THREAD_STORAGE_CLASS
#define HAVE_THREAD_STORAGE_CLASS 0
#endif

#if ( (FD_FORCE_TLS) || (U8_USE_TLS) || (!(HAVE_THREAD_STORAGE_CLASS)))
#define FD_USE_TLS 1
#define FD_USE__THREAD 0
#define FD_HAVE_THREADS 1
#elif U8_USE__THREAD
#define FD_USE_TLS 0
#define FD_USE__THREAD 1
#define FD_THREADVAR __thread
#define FD_HAVE_THREADS 1
#else
#define FD_USE_TLS 0
#define FD_USE__THREAD 0
#define FD_THREADVAR __thread
#define FD_HAVE_THREADS 0
#endif

#ifndef FD_INLINE_CHOICES
#define FD_INLINE_CHOICES 0
#endif

#ifndef FD_INLINE_TABLES
#define FD_INLINE_TABLES 0
#endif

#if ((FD_INLINE_CHOICES)||(FD_INLINE_TABLES))
#define FD_INLINE_COMPARE 1
#elif (!(defined(FD_INLINE_COMPARE)))
#define FD_INLINE_COMPARE 0
#endif

#ifndef FD_INLINE_REFCOUNTS
#define FD_INLINE_REFCOUNTS 1
#endif

#ifndef FD_USE_THREADCACHE
#define FD_USE_THREADCACHE 1
#endif

#ifndef FD_WRITETHROUGH_THREADCACHE
#define FD_WRITETHROUGH_THEADCACHE 0
#endif

#ifndef FD_TYPE_MAX
#define FD_TYPE_MAX 256
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

#ifndef FD_DEBUG_OUTBUF_SIZE
#define FD_DEBUG_OUTBUF_SIZE 1024
#endif

#ifndef FD_MIN_STACKSIZE
#define FD_MIN_STACKSIZE 0x10000
#endif

#ifndef FD_SCHEME_BUILTINS
#define FD_INIT_SCHEME_BUILTINS() fd_init_scheme()
#else
#define FD_INIT_SCHEME_BUILTINS() FD_SCHEME_BUILTINS
#endif

#if HAVE_CONSTRUCTOR_ATTRIBUTES
#define FD_LIBINIT_FN __attribute__ ((constructor)) /*  ((constructor (150))) */
#define FD_LIBINIT0_FN __attribute__ ((constructor))  /*  ((constructor (101))) */
#define FD_DO_LIBINIT(fn) ((void)fn)
#else
#define FD_LIBINIT_FN
#define FD_LIBINIT0_FN
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

/* Merging integer values */

#define fd_int_default(l1,l2) ( (l1>0) ? (l1) : (l2) )

/* Ints that hold pointers */

#if (SIZEOF_VOID_P == SIZEOF_INT)
#define FD_INTPTR unsigned int
#elif (SIZEOF_VOID_P == SIZEOF_LONG)
#define FD_INTPTR unsigned long
#elif (SIZEOF_VOID_P == SIZEOF_LONG_LONG)
#define FD_INTPTR unsigned long long
#else
#define FD_INTPTR unsigned long long
#endif

/* Sized numeric types */

#if (SIZEOF_INT == 8)
typedef int fd_long;
typedef unsigned int fd_ulong;
#elif (SIZEOF_LONG == 8)
typedef long fd_long;
typedef unsigned long fd_ulong;
#elif (SIZEOF_LONG_LONG == 8)
typedef long long fd_long;
typedef unsigned long long fd_ulong;
#endif

#if (SIZEOF_INT == 4)
typedef int fd_int;
typedef unsigned int fd_uint;
#elif (SIZEOF_LONG == 4)
typedef long fd_int;
typedef unsigned long fd_uint;
#elif (SIZEOF_LONG_LONG == 8)
typedef long long fd_int;
typedef unsigned long long fd_uint;
#else
typedef int fd_int;
typedef unsigned int fd_uint;
#endif

#if (SIZEOF_SHORT == 2)
typedef short fd_short;
typedef unsigned short fd_ushort;
#else
typedef int fd_short;
typedef unsigned int fd_ushort;
#endif

#if (SIZEOF_FLOAT == 32)
typedef float fd_float;
#else
typedef float fd_float;
#endif

#if (SIZEOF_DOUBLE == 64)
typedef double fd_double;
#else
typedef double fd_double;
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

#ifndef FD_LIBSCM_DIR
#ifdef FRAMERD_REVISION
#define FD_LIBSCM_DIR FD_SHARE_DIR "/libscm/" FRAMERD_REVISION
#else
#define FD_LIBSCM_DIR FD_SHARE_DIR  "/libscm/" FD_VERSION
#endif
#endif

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

#endif /* FRAMERD_DEFINES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
