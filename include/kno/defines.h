/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
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

#ifndef KNO_PROFILING
#define KNO_PROFILING 0
#endif

#if ( KNO_DEBUGGING_BUILD | KNO_DEBUG_GC )
#ifndef KNO_MAX_REFCOUNT
#define KNO_MAX_REFCOUNT 0xFFFFFF
#endif
#endif

#ifndef KNO_SOURCE
#define KNO_SOURCE 0
#endif

#ifndef KNO_LANG_CORE
#define KNO_LANG_CORE 0
#endif

#ifndef KNO_STORAGE_CORE
#define KNO_STORAGE_CORE 0
#endif

#ifndef KNO_CORE
#if (KNO_LANG_CORE || KNO_STORAGE_CORE)
#define KNO_CORE 1
#else
#define KNO_CORE 0
#endif
#endif

#if KNO_CORE
#ifndef KNO_INLINE_XTYPEP
#define KNO_INLINE_XTYPEP 1
#endif
#ifndef KNO_INLINE_CHOICES
#define KNO_INLINE_CHOICES 1
#endif
#ifndef KNO_INLINE_TABLES
#define KNO_INLINE_TABLES 1
#endif
#endif /* KNO_CORE */

#if KNO_LANG_CORE
#ifndef KNO_INLINE_FCNIDS
#define KNO_INLINE_FCNIDS 1
#endif
#ifndef KNO_INLINE_LEXENV
#define KNO_INLINE_LEXENV 1
#endif
#ifndef KNO_INLINE_STACKS
#define KNO_INLINE_STACKS 1
#endif
#endif /* KNO_CORE */

#define KNO_DEEP_PROFILING    ( KNO_PROFILING > 1 )
#define KNO_EXTREME_PROFILING ( KNO_PROFILING > 2 )

#if KNO_PROFILING > 1
#define KNO_DO_INLINE 0
#else
#define KNO_DO_INLINE 1
#endif

#ifndef KNO_AVOID_INLINE
#define KNO_AVOID_INLINE      ( KNO_PROFILING )
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

#ifndef KNO_INLINE_XTYPEP
#define KNO_INLINE_XTYPEP 0
#endif

#ifndef KNO_INLINE_CHECKTYPE
#define KNO_INLINE_CHECKTYPE 0
#endif

/* Whether to enable using paths rather than symbols for module specs */
#ifndef KNO_PATHMODS_ENABLED
#define KNO_PATHMODS_ENABLED 0
#endif

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
#elif ( (HAVE_FFI_FFI_H) && (HAVE_LIBFFI) )
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

/* Help with tracking down bugs */

#define __KNO_STRINGIFY(x) #x
#define __KNO_TO_STRING(x) __KNO_STRINGIFY(x)
#define KNO_FILEPOS  (__FILE__ ":" __KNO_TO_STRING(__LINE__))


/*
   This can be configured with --with-nptrlocks.
*/
#ifndef KNO_N_PTRLOCKS
#define KNO_N_PTRLOCKS 979
#endif
/*
   Larger values for this are useful when you're being more
   multi-threaded, since it avoids locking conflicts during reference
   count updates. Smaller values will reduce resources, but not
   significantly.
*/

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
#define KNO_INLINE_REFCOUNTS (!(KNO_EXTREME_PROFILING))
#endif

#ifndef KNO_USE_THREADCACHE
#define KNO_USE_THREADCACHE 1
#endif

#ifndef KNO_WRITETHROUGH_THREADCACHE
#define KNO_WRITETHROUGH_THREADCACHE 0
#endif

#ifndef KNO_TYPE_MAX
#define KNO_TYPE_MAX 512
#endif

#ifndef KNO_TRACE_IPEVAL
#define KNO_TRACE_IPEVAL 1
#endif

#ifndef KNO_PREFETCHING_ENABLED
#define KNO_PREFETCHING_ENABLED 1
#endif

#ifndef KNO_LISP_TYPE_MACRO
#define KNO_LISP_TYPE_MACRO 1
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
#define KNO_USUALLY(x) (__builtin_expect(!!(x),1))
#define KNO_RARELY(x) (__builtin_expect(!!(x),0))
#define KNO_EXPECT_TRUE(x) (__builtin_expect(!!(x),1))
#define KNO_EXPECT_FALSE(x) (__builtin_expect(!!(x),0))
#else
#define KNO_USUALLY(x)      (x)
#define KNO_RARELY(x)       (x)
#define KNO_EXPECT_TRUE(x)  (x)
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

/* For gettext */
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

/* liu8 stream/port flags */

#ifndef U8_STREAM_NEXT_FLAG
#define U8_STREAM_NEXT_FLAG 0x10000
#endif

#define KNO_U8STREAM_ISATTY   ( (U8_STREAM_NEXT_FLAG) << 0 )
#define KNO_U8STREAM_NOLIMITS ( (U8_STREAM_NEXT_FLAG) << 1 )
#define KNO_U8STREAM_HISTORIC ( (U8_STREAM_NEXT_FLAG) << 2 )

/* Source aliases */

#if KNO_SOURCE
#define VOID       (KNO_VOID)
#define VOIDP(x)   (KNO_VOIDP(x))
#define DEFAULTP(x) (KNO_DEFAULTP(x))
#define EMPTY      (KNO_EMPTY_CHOICE)
#define EMPTYP(x)  (KNO_EMPTY_CHOICEP(x))
#define EXISTSP(x) (! (KNO_EMPTY_CHOICEP(x)) )
#define NIL        (KNO_EMPTY_LIST)
#define NILP(x)    (KNO_EMPTY_LISTP(x))
#define CONSP(x)   (KNO_CONSP(x))
#define ATOMICP(x) (KNO_ATOMICP(x))
#define TYPEP(o,t) (KNO_TYPEP((o),(t)))
#define CHOICEP(x) (KNO_CHOICEP(x))
#define SINGLETONP(x) (KNO_SINGLETONP(x))
#define IMMEDIATEP(x) (KNO_IMMEDIATEP(x))
#define CONSP(x)      (KNO_CONSP(x))
#define FIXNUMP(x) (KNO_FIXNUMP(x))
#define FLONUMP(x) (KNO_FLONUMP(x))
#define NUMBERP(x) (KNO_NUMBERP(x))
#define APPLICABLEP(x) (KNO_APPLICABLEP(x))
#define SLOTIDP(x) (KNO_SLOTIDP(x))
#define TABLEP(x)  (KNO_TABLEP(x))
#define POOLP(x)  (KNO_POOLP(x))
#define INDEXP(x)  (KNO_INDEXP(x))
#define PAIRP(x)   (KNO_PAIRP(x))
#define VECTORP(x) (KNO_VECTORP(x))
#define SYMBOLP(x) (KNO_SYMBOLP(x))
#define STRINGP(x) (KNO_STRINGP(x))
#define PACKETP(x) (KNO_PACKETP(x))
#define FALSEP(x)  (KNO_FALSEP(x))
#define OIDP(x)    (KNO_OIDP(x))
#define FIX2INT(x) (KNO_FIX2INT(x))
#define ABORTP(x)  (KNO_ABORTP(x))
#define ABORTED(x) (KNO_ABORTED(x))
#define STRLEN(x)  (KNO_STRLEN(x))
#define CSTRING(x) (KNO_CSTRING(x))
#define VEC_LEN(x)  (KNO_VECTOR_LENGTH(x))
#define VEC_DATA(x)  (KNO_VECTOR_DATA(x))
#define VEC_REF(x,i) (KNO_VECTOR_REF((x),(i)))
#define SYM_NAME(x) (KNO_SYMBOL_NAME(x))
#define PRECHOICEP(x) (KNO_PRECHOICEP(x))
#define QCHOICEP(x) (KNO_QCHOICEP(x))
#define AMBIGP(x)   (KNO_AMBIGP(x))
#define SLOTMAPP(x) (KNO_SLOTMAPP(x))
#define SCHEMAPP(x) (KNO_SCHEMAPP(x))
#define KEYMAPP(x) (KNO_KEYMAPP(x))
#define HASHTABLEP(x) (KNO_HASHTABLEP(x))
#define USUALLY(x)  (KNO_USUALLY(x))
#define RARELY(x) (KNO_RARELY(x))
#define SYMBOL_NAME(x) (KNO_SYMBOL_NAME(x))
#define COMPOUND_VECTORP(x) (KNO_COMPOUND_VECTORP(x))
#define COMPOUND_VECLEN(x)  (KNO_COMPOUND_VECLEN(x))
#define COMPOUND_VECELTS(x)  (KNO_COMPOUND_VECELTS(x))
#define XCOMPOUND_VEC_REF(x,i) (KNO_XCOMPOUND_VECREF((x),(i)))
#define PRED_FALSE(x)  (KNO_EXPECT_FALSE(x))
#define PRED_TRUE(x)  (KNO_EXPECT_TRUE(x))
#define ADD_TO_CHOICE(x,y) KNO_ADD_TO_CHOICE(x,y)
#define ADD_TO_CHOICE_INCREF(x,y) KNO_ADD_TO_CHOICE(x,y)
#define CHOICE_SIZE(x) KNO_CHOICE_SIZE(x)
#define DOLIST             KNO_DOLIST
#define CHOICE_ADD         KNO_ADD_TO_CHOICE
#define CHOICE_ADD_INCREF  KNO_ADD_TO_CHOICE_INCREF
#define EQ                 KNO_EQ
#define DO_CHOICES         KNO_DO_CHOICES
#define ITER_CHOICES       KNO_ITER_CHOICES
#define lspcpy             kno_lspcpy
#define lspset             kno_lspset
#endif

#endif /* KNO_DEFINES_H */

