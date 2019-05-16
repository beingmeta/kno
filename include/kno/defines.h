/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Initial definitions */

#ifndef KNO_DEFINES_H
#define KNO_DEFINES_H 1
#ifndef KNO_DEFINES_H_INFO
#define KNO_DEFINES_H_INFO "include/kno/defines.h"
#endif

#include "config.h"
#include "revision.h"

#define KNO_EXPORT extern
#ifndef KNO_INLINE
#define KNO_INLINE 0
#endif

#ifndef KNO_USE_MMAP
#if HAVE_MMAP
#if KNO_WITHOUT_MMAP
#define KNO_USE_MMAP 0
#else
#define KNO_USE_MMAP 1
#endif
#else
#define KNO_USE_MMAP 0
#endif
#endif

#ifndef WORDS_BIGENDIAN
#define WORDS_BIGENDIAN 0
#endif

#define KNO_THREADS_ENABLED 1

#ifndef KNO_FASTOP
#define KNO_FASTOP static inline
#endif

/* This is for C profiling and helps generate code which is easier to profile. */
#ifndef KNO_PROFILING_ENABLED
#define KNO_PROFILING_ENABLED 0
#endif

#ifndef HAVE_CONSTRUCTOR_ATTRIBUTES
#define HAVE_CONSTRUCTOR_ATTRIBUTES 0
#endif

#ifndef KNO_GLOBAL_IPEVAL
#define KNO_GLOBAL_IPEVAL 0
#endif

#ifndef KNO_USE_DTBLOCK
#define KNO_USE_DTBLOCK 0
#endif

#define KNO_WRITETHROUGH_THREADCACHE 1

#ifndef NO_ELSE
#define NO_ELSE {}
#endif

#if defined(KNO_ENABLE_FFI)

#if ( (KNO_ENABLE_FFI) && ( ! ( (HAVE_FFI_H) && (HAVE_LIBFFI) ) ) )
#undef KNO_ENABLE_FFI
#define KNO_ENABLE_FFI 0
#endif

#else /* not defined(KNO_ENABLE_FFI) */

#if ( (HAVE_FFI_H) && (HAVE_LIBFFI) )
#define KNO_ENABLE_FFI 1
#else
#define KNO_ENABLE_FFI 0
#endif

#endif

#ifndef KNO_INIT_UMASK
#define KNO_INIT_UMASK ((mode_t) (S_IWOTH ))
#endif

/* Whether to have KNO_CHECK_PTR check underlying CONS structs */
#ifdef KNO_FULL_CHECK_PTR
#define KNO_FULL_CHECK_PTR 1
/* It can be helpful to turn this off when doing certain kinds of
   thread debugging, since checking that a CONS is legitimate without
   locking counts as a race condition to some thread error
   detectors. */
#endif

/* This is set to make incref/decref into no-ops, which is helpful
   for ablative benchmarking.  */
#ifndef KNO_NO_GC
#define KNO_NO_GC 0
#endif

/* Pointer Checking */

/* There are two global defines which control pointer checking.
   KNO_PTR_DEBUG_LEVEL controls what kind of pointer checking to do,
    0 = no checking
    1 = check non-null
    2 = check integrity of immediates
    3 = check integrity of cons pointers
   KNO_PTR_DEBUG_DENSITY controls how detailed the debug checking is,
    by enabling macros of the form KNO_PTRCHECKn and KNO_CHECK_PTRn,
    which would otherwise be noops.
   System and user C code can call different pointer-checking macros
    which can be selectively enabled on a per-file basis.
   In addition, defining KNO_PTR_DEBUG_DENSITY to be < 0 causes references
   to use the variable kno_ptr_debug_density to determine the debugging
   density.
*/
#ifndef KNO_PTR_DEBUG_LEVEL
#define KNO_PTR_DEBUG_LEVEL 1
#endif

/* This determines when to check for pointers (as controlled above)
   There are four basic levels, with zero being no checking
*/
#ifndef KNO_PTR_DEBUG_DENSITY
#define KNO_PTR_DEBUG_DENSITY 1
#endif

/* This is true (1) for executables built in the tests/ subdirectories.
   If true, the executables call the various module init functions
   (e.g. kno_init_texttools()) directly; when dynamically linked, the
   loader calls them.  This is neccessary because static linking (at
   least on some platforms) doesn't invoke the library initializers
   at startup.  */
#ifndef KNO_TESTCONFIG
#define KNO_TESTCONFIG 0
#endif

#ifndef KNO_LOCKFREE_REFCOUNTS
#if HAVE_STDATOMIC_H
#define KNO_LOCKFREE_REFCOUNTS KNO_ENABLE_LOCKFREE
#else
#define KNO_LOCKFREE_REFCOUNTS 0
#endif
#endif

#ifndef KNO_USE_ATOMIC
#if HAVE_STDATOMIC_H
#define KNO_USE_ATOMIC 1
#else
#define KNO_USE_ATOMIC 0
#endif
#endif

#if (HAVE_OFF_T)
#if ((KNO_LARGEFILES_ENABLED)&&(SIZEOF_OFF_T==8))
typedef off_t kno_off_t;
#elif (KNO_LARGEFILES_ENABLED)
typedef long long int kno_off_t;
#else
typedef off_t kno_off_t;
#endif
#else /* (!(HAVE_OFF_T)) */
#if (KNO_LARGEFILES_ENABLED)
typedef long long int kno_off_t;
#else
typedef int kno_off_t;
#endif
#endif

#if (HAVE_SIZE_T)
#if ((KNO_LARGEFILES_ENABLED)&&(SIZEOF_SIZE_T==8))
typedef ssize_t kno_size_t;
#elif (KNO_LARGEFILES_ENABLED)
typedef long long int kno_size_t;
#else
typedef ssize_t kno_size_t;
#endif
#else /* (!(HAVE_SIZE_T)) */
#if (KNO_LARGEFILES_ENABLED)
typedef long long int kno_size_t;
#else
typedef int kno_size_t;
#endif
#endif

#if ((KNO_LARGEFILES_ENABLED) && (HAVE_FSEEKO))
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
#ifndef KNO_N_PTRLOCKS
#define KNO_N_PTRLOCKS 979
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

#if ( (KNO_FORCE_TLS) || (U8_USE_TLS) || (!(HAVE_THREAD_STORAGE_CLASS)))
#define KNO_USE_TLS 1
#define KNO_USE__THREAD 0
#define KNO_HAVE_THREADS 1
#elif U8_USE__THREAD
#define KNO_USE_TLS 0
#define KNO_USE__THREAD 1
#define KNO_THREADVAR __thread
#define KNO_HAVE_THREADS 1
#else
#define KNO_USE_TLS 0
#define KNO_USE__THREAD 0
#define KNO_THREADVAR __thread
#define KNO_HAVE_THREADS 0
#endif

#ifndef KNO_INLINE_CHOICES
#define KNO_INLINE_CHOICES 0
#endif

#ifndef KNO_INLINE_TABLES
#define KNO_INLINE_TABLES 0
#endif

#if ((KNO_INLINE_CHOICES)||(KNO_INLINE_TABLES))
#define KNO_INLINE_COMPARE 1
#elif (!(defined(KNO_INLINE_COMPARE)))
#define KNO_INLINE_COMPARE 0
#endif

#ifndef KNO_INLINE_REFCOUNTS
#define KNO_INLINE_REFCOUNTS 1
#endif

#ifndef KNO_USE_THREADCACHE
#define KNO_USE_THREADCACHE 1
#endif

#ifndef KNO_WRITETHROUGH_THREADCACHE
#define KNO_WRITETHROUGH_THEADCACHE 0
#endif

#ifndef KNO_TYPE_MAX
#define KNO_TYPE_MAX 256
#endif

#ifndef KNO_TRACE_IPEVAL
#define KNO_TRACE_IPEVAL 1
#endif

#ifndef KNO_PREFETCHING_ENABLED
#define KNO_PREFETCHING_ENABLED 1
#endif

#ifndef KNO_PTR_TYPE_MACRO
#define KNO_PTR_TYPE_MACRO 1
#endif

#ifndef KNO_DEBUG_OUTBUF_SIZE
#define KNO_DEBUG_OUTBUF_SIZE 1024
#endif

#ifndef KNO_MIN_STACKSIZE
#define KNO_MIN_STACKSIZE 0x10000
#endif

#ifndef KNO_SCHEME_BUILTINS
#define KNO_INIT_SCHEME_BUILTINS() kno_init_scheme()
#else
#define KNO_INIT_SCHEME_BUILTINS() KNO_SCHEME_BUILTINS
#endif

#if HAVE_CONSTRUCTOR_ATTRIBUTES
#define KNO_LIBINIT_FN __attribute__ ((constructor)) /*  ((constructor (150))) */
#define KNO_LIBINIT0_FN __attribute__ ((constructor))  /*  ((constructor (101))) */
#define KNO_DO_LIBINIT(fn) ((void)fn)
#else
#define KNO_LIBINIT_FN
#define KNO_LIBINIT0_FN
#define KNO_DO_LIBINIT(fn) fn()
#endif

#if HAVE_BUILTIN_EXPECT
#define KNO_EXPECT_TRUE(x) (__builtin_expect(x,1))
#define KNO_EXPECT_FALSE(x) (__builtin_expect(x,0))
#else
#define KNO_EXPECT_TRUE(x) (x)
#define KNO_EXPECT_FALSE(x) (x)
#endif

#if HAVE_BUILTIN_PREFETCH
#define KNO_PREFETCH(x) __builtin_prefetch(x)
#define KNO_WRITE_PREFETCH(x) __builtin_prefetch(x,1)
#else
#define KNO_PREFETCH(x)
#define KNO_WRITE_PREFETCH(x)
#endif

/* Merging integer values */

#define kno_int_default(l1,l2) ( (l1>0) ? (l1) : (l2) )

/* Ints that hold pointers */

#if (SIZEOF_VOID_P == SIZEOF_INT)
#define KNO_INTPTR unsigned int
typedef unsigned int kno_ptrval;
#elif (SIZEOF_VOID_P == SIZEOF_LONG)
#define KNO_INTPTR unsigned long
typedef unsigned long kno_ptrval;
#elif (SIZEOF_VOID_P == SIZEOF_LONG_LONG)
#define KNO_INTPTR unsigned long long
typedef unsigned long long kno_ptrval;
#else
#define KNO_INTPTR unsigned long long
typedef unsigned long long kno_ptrval;
#endif

/* These are for cases where we need to pass a 64 bit value (for
   instance printf string args) for generality. */
#define KNO_PTRVAL(x) ((kno_ptrval)(x))
#define KNO_LONGVAL(x) ((unsigned long long)((kno_ptrval)(x)))

/* Sized numeric types */

#if (SIZEOF_INT == 8)
typedef int kno_long;
typedef unsigned int kno_ulong;
#elif (SIZEOF_LONG == 8)
typedef long kno_long;
typedef unsigned long kno_ulong;
#elif (SIZEOF_LONG_LONG == 8)
typedef long long kno_long;
typedef unsigned long long kno_ulong;
#endif

#if (SIZEOF_INT == 4)
typedef int kno_int;
typedef unsigned int kno_uint;
#elif (SIZEOF_LONG == 4)
typedef long kno_int;
typedef unsigned long kno_uint;
#elif (SIZEOF_LONG_LONG == 8)
typedef long long kno_int;
typedef unsigned long long kno_uint;
#else
typedef int kno_int;
typedef unsigned int kno_uint;
#endif

#if (SIZEOF_SHORT == 2)
typedef short kno_short;
typedef unsigned short kno_ushort;
#else
typedef int kno_short;
typedef unsigned int kno_ushort;
#endif

#if (SIZEOF_FLOAT == 32)
typedef float kno_float;
#else
typedef float kno_float;
#endif

#if (SIZEOF_DOUBLE == 64)
typedef double kno_double;
#else
typedef double kno_double;
#endif

/* Fastcgi configuration */

#ifndef HAVE_FCGIAPP_H
#define HAVE_FCGIAPP_H 0
#endif

#ifndef HAVE_LIBFCGI
#define HAVE_LIBFCGI 0
#endif

#if ((WITH_FASTCGI) && (HAVE_FCGIAPP_H) && (HAVE_LIBFCGI))
#define KNO_WITH_FASTCGI 1
#else
#define KNO_WITH_FASTCGI 0
#endif

#define _(x) (x)

#ifndef KNO_LIBSCM_DIR
#ifdef KNO_REVISION
#define KNO_LIBSCM_DIR KNO_SHARE_DIR "/libscm/" KNO_REVISION
#else
#define KNO_LIBSCM_DIR KNO_SHARE_DIR  "/libscm/" KNO_VERSION
#endif
#endif

/* How to implement OIDs */

#ifndef KNO_STRUCT_OIDS
#if (SIZEOF_INT == 8)
#define KNO_STRUCT_OIDS 0
#define KNO_LONG_OIDS 0
#define KNO_LONG_LONG_OIDS 0
#define KNO_INT_OIDS 1
#elif (SIZEOF_LONG == 8)
#define KNO_STRUCT_OIDS 0
#define KNO_LONG_OIDS 1
#define KNO_LONG_LONG_OIDS 0
#define KNO_INT_OIDS 0
#elif (SIZEOF_LONG_LONG == 8)
#define KNO_STRUCT_OIDS 0
#define KNO_LONG_OIDS 0
#define KNO_LONG_LONG_OIDS 1
#define KNO_INT_OIDS 0
#else
#define KNO_STRUCT_OIDS 1
#define KNO_LONG_OIDS 0
#define KNO_LONG_LONG_OIDS 1
#define KNO_INT_OIDS 0
#endif
#endif

#endif /* KNO_DEFINES_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
