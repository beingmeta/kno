/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

   This file implements the basic structures, macros, and function
   prototypes for dealing with lisp pointers as implemented for the
   FramerC library
*/

/* THE ANATOMY OF A POINTER

   The FramerC library is built on a dynamically typed "lisp-like" data
   model which uses an internal pointer format designed for efficency in
   the normal cases and easy extensibility to more advanced cases.

   A lisp pointer is an unsigned int the same size as a memory
   pointer.  The lower two bits of the int represent one of four
   *manifest type codes*.  These are CONS (0), IMMEDIATE (1), FIXNUM
   (2), and OID (3).  These are all referred to the "base type" of a
   pointer.  In essense, CONSes point to structures in memory,
   IMMEDIATEs represent constant persistent values, FIXNUMs represent
   low magnitude signed integers, and OIDs are compressed object
   references into 64-bit object address space.

   Fixing the CONS type at zero (0), together with structure
   alignment on 4-byte word boundaries, enables lisp pointers to be
   direclty used as structure pointers and vice versa.  A CONS
   pointer is exactly such a memory reference.  The detailed
   implementation of CONSes is described at the head of
   <include/kno/cons.h>, but it is basically a block of memory
   whose first 4 bytes encode a 25-bit reference count and a 7-bit
   type code.

   The IMMEDIATE type is used to represent a number of "constant" data
   types, including booleans, special values (error codes, etc), unicode
   characters, and symbols.  The high 7 bits of an IMMEDIATE pointer encode
   a more detailed type code and the remaining 23 bits are available to
   represent particular values of each type.

   The FIXNUM base pointer type is used for small magnitude signed
   integers.  Rather than using a two's complement representation for
   negative numbers, FramerC uses the 32nd bit (the high order bit on 32
   bit architectures) as a sign bit and stores the absolute magnitude
   in the remainder of the word (down to the two bits of type code.
   For positive numbers, converting between C ints lisp fixnums is
   simply a matter of multiply or diving by 4 and adding the type
   code (when converting to DTYPEs).  For negative numbers, it is
   trickier but not much, as in KNO_INT2LISP and KNO_LISP2INT below.
   Currently, fixnums do not take advantage of the full word on
   64-bit machines, for a mix of present practical and prospective
   design reasons.

   The OID base pointer type is used to represent object pointers into a
   64-bit object space.  It does this by dividing the 64 bit object space
   into buckets addressable by 20 bits (roughly a million objects).  The
   30 content bits in an OID pointer are then divided between a base ID
   (indicating a particular bucket in the 64-bit space) and an offset from
   that base address.  Currently, on both 32-bit and 64-bit
   architectures, ten (10) bits are used for base oids, though this could
   obviously be increased in 64-bit architectures.  However, there might
   also be value in keeping it small and allowing 32 bit OID pointers even
   on 64-bit architectures.

   This pointer layout is not just useful for compressing the 64-bit
   space, but can speed up processing, since the baseids are small and
   many lookup procedures can simply use this baseid directly as an
   index into an array.  Thus, even on 64-bit architectures, a direct
   representation of object addresses may not be desirable.

   A single unified space of type codes (kno_lisp_type) combines the four
   base type codes with the 7-bit type codes associated with CONSes and
   IMMEDIATE pointers.  In this single space, CONS types start at 0x04 and
   IMMEDIATE types start at 0x84.  For example, lisp pairs, implemented as
   structures with a stored type code of 0x03, are represented in the
   unified type space by 0x07.  Symbols, which are implemented as
   CONSTANT types with a stored type value of 0x02, are represented in
   the unified space by 0x86.

   The built in components of the unified type space are represented
   by the enumeration kno_lisp_type (enum KNO_LISP_TYPE).  New immediate
   and cons types can be allocated by kno_register_immediate_type(char
   *) and kno_register_cons_type(char *).
   */

/* LISP Types */

#ifndef KNO_PTR_H
#define KNO_PTR_H 1
#ifndef KNO_PTR_H_INFO
#define KNO_PTR_H_INFO "include/kno/ptr.h"
#endif

#include "common.h"
#include "errors.h"

#define KNO_IMMEDIATE_TYPECODE(i) (0x04+i)
#define KNO_CONS_TYPECODE(i)      (0x84+i)
#define KNO_EXTENDED_TYPECODE(i)  (0x100+i)
#define KNO_MAX_IMMEDIATE_TYPES   0x80
#define KNO_MAX_IMMEDIATE_TYPE    KNO_IMMEDIATE_TYPECODE(0x80)
#define KNO_MAX_CONS_TYPES	  0x80
#define KNO_MAX_CONS_TYPE	  KNO_CONS_TYPECODE(0x80)

/* 0 and 1 are reserved for OIDs and fixnums.
   CONS types start at 0x84 and go until 0x104, which
   is the limit to what we can store together with
   the reference count in kno_consbit
   IMMEDIATE types start after that
*/
#define kno_cons_ptr_type (0)
#define kno_immediate_ptr_type (1)
#define kno_fixnum_ptr_type (2)
#define kno_oid_ptr_type (3)

KNO_EXPORT u8_condition kno_BadPtr, kno_NullPtr;

#define KNO_PTR_WIDTH ((SIZEOF_VOID_P)*8)

#if SIZEOF_INT == SIZEOF_VOID_P
typedef unsigned int _kno_ptrbits;
typedef int _kno_sptr;
#elif SIZEOF_LONG == SIZEOF_VOID_P
typedef unsigned long _kno_ptrbits;
typedef long _kno_sptr;
#elif SIZEOF_LONG_LONG == SIZEOF_VOID_P
typedef unsigned long long _kno_ptrbits;
typedef long long _kno_sptr;
#else
typedef unsigned int _kno_ptrbits;
typedef int _kno_sptr;
#endif

/* Basic types */

typedef enum KNO_LISP_TYPE {
  kno_any_type = -1,

  kno_cons_type = 0, kno_immediate_type = 1,
  kno_fixnum_type = 2, kno_oid_type = 3,

  kno_constant_type = KNO_IMMEDIATE_TYPECODE(0),
  kno_character_type = KNO_IMMEDIATE_TYPECODE(1),
  kno_symbol_type = KNO_IMMEDIATE_TYPECODE(2),

  /* Reserved as constants */
  kno_fcnid_type = KNO_IMMEDIATE_TYPECODE(3),
  kno_lexref_type = KNO_IMMEDIATE_TYPECODE(4),
  kno_opcode_type = KNO_IMMEDIATE_TYPECODE(5),
  kno_typeref_type = KNO_IMMEDIATE_TYPECODE(6),
  kno_coderef_type = KNO_IMMEDIATE_TYPECODE(7),
  kno_poolref_type = KNO_IMMEDIATE_TYPECODE(8),
  kno_indexref_type = KNO_IMMEDIATE_TYPECODE(9),
  kno_histref_type = KNO_IMMEDIATE_TYPECODE(10),
  kno_ctype_type = KNO_IMMEDIATE_TYPECODE(11),
  kno_pooltype_type = KNO_IMMEDIATE_TYPECODE(12),
  kno_indextype_type = KNO_IMMEDIATE_TYPECODE(13),

  kno_string_type = KNO_CONS_TYPECODE(0),
  kno_packet_type = KNO_CONS_TYPECODE(1),
  kno_vector_type = KNO_CONS_TYPECODE(2),
  kno_numeric_vector_type = KNO_CONS_TYPECODE(3),
  kno_pair_type = KNO_CONS_TYPECODE(4),
  kno_cdrcode_type = KNO_CONS_TYPECODE(5),
  kno_secret_type = KNO_CONS_TYPECODE(6),
  kno_bigint_type = KNO_CONS_TYPECODE(7),

  kno_choice_type = KNO_CONS_TYPECODE(8),
  kno_prechoice_type = KNO_CONS_TYPECODE(9),
  kno_qchoice_type = KNO_CONS_TYPECODE(10),

  kno_typeinfo_type = KNO_CONS_TYPECODE(11),

  kno_tagged_type = KNO_CONS_TYPECODE(12),
  /* These types are arranged so that their top bits are the same,
     allowing them to be used with KNO_XXCONS macros, though we
     leave a type gap */
  kno_compound_type = KNO_CONS_TYPECODE(12),
  kno_rawptr_type = KNO_CONS_TYPECODE(13),

  kno_ioport_type = KNO_CONS_TYPECODE(14),
  kno_regex_type = KNO_CONS_TYPECODE(15),

  kno_coretable_type = KNO_CONS_TYPECODE(16),
  /* These types are arranged so that their top bits are the same,
     allowing them to be used with KNO_XXCONS macros, though we
     leave a type gap */
  kno_slotmap_type = KNO_CONS_TYPECODE(16),
  kno_schemap_type = KNO_CONS_TYPECODE(17),
  kno_hashtable_type = KNO_CONS_TYPECODE(18),
  kno_hashset_type = KNO_CONS_TYPECODE(19),

  /* Evaluator/apply types, defined here to be constant */

  kno_function_type = KNO_CONS_TYPECODE(20),
  /* These types are arranged so that their top bits are the same,
     allowing them to be used with KNO_XXCONS macros */
  kno_cprim_type = KNO_CONS_TYPECODE(20),
  kno_lambda_type = KNO_CONS_TYPECODE(21),
  kno_ffi_type = KNO_CONS_TYPECODE(22),
  kno_rpc_type = KNO_CONS_TYPECODE(23),

  kno_lexenv_type = KNO_CONS_TYPECODE(24),
  kno_evalfn_type = KNO_CONS_TYPECODE(25),
  kno_macro_type = KNO_CONS_TYPECODE(26),
  kno_stackframe_type = KNO_CONS_TYPECODE(27),
  kno_exception_type = KNO_CONS_TYPECODE(28),
  kno_promise_type = KNO_CONS_TYPECODE(29),
  kno_thread_type = KNO_CONS_TYPECODE(30),
  kno_synchronizer_type = KNO_CONS_TYPECODE(31),

  kno_consblock_type = KNO_CONS_TYPECODE(32),

  kno_complex_type = KNO_CONS_TYPECODE(33),
  kno_rational_type = KNO_CONS_TYPECODE(34),
  kno_flonum_type = KNO_CONS_TYPECODE(35),

  kno_timestamp_type = KNO_CONS_TYPECODE(36),
  kno_uuid_type = KNO_CONS_TYPECODE(37),

  /* Other types, not strictly core, but defined here so they'll be
     constant for the compiler */
  kno_mystery_type = KNO_CONS_TYPECODE(38),

  kno_stream_type = KNO_CONS_TYPECODE(39),

  kno_service_type = KNO_CONS_TYPECODE(40),

  kno_sqldb_type = KNO_CONS_TYPECODE(41),
  kno_sqlproc_type = KNO_CONS_TYPECODE(42),

  kno_consed_index_type = KNO_CONS_TYPECODE(43),
  kno_consed_pool_type = KNO_CONS_TYPECODE(44),

  kno_subproc_type = KNO_CONS_TYPECODE(45),

  /* Extended types */

  kno_empty_type = KNO_EXTENDED_TYPECODE(0),
  kno_exists_type = KNO_EXTENDED_TYPECODE(1),
  kno_singleton_type = KNO_EXTENDED_TYPECODE(2),
  kno_true_type = KNO_EXTENDED_TYPECODE(3),
  kno_error_type = KNO_EXTENDED_TYPECODE(4),
  kno_void_type = KNO_EXTENDED_TYPECODE(5),
  kno_satisfied_type = KNO_EXTENDED_TYPECODE(6),

  kno_number_type = KNO_EXTENDED_TYPECODE(0x10),
  kno_integer_type = KNO_EXTENDED_TYPECODE(0x11),
  kno_exact_type = KNO_EXTENDED_TYPECODE(0x12),
  kno_inexact_type = KNO_EXTENDED_TYPECODE(0x13),
  kno_sequence_type = KNO_EXTENDED_TYPECODE(0x14),
  kno_table_type = KNO_EXTENDED_TYPECODE(0x15),
  kno_applicable_type = KNO_EXTENDED_TYPECODE(0x16),
  kno_xfunction_type = KNO_EXTENDED_TYPECODE(0x17),
  kno_keymap_type = KNO_EXTENDED_TYPECODE(0x18),
  kno_type_type = KNO_EXTENDED_TYPECODE(0x19),
  kno_opts_type = KNO_EXTENDED_TYPECODE(0x1A),
  kno_frame_type = KNO_EXTENDED_TYPECODE(0x1B),
  kno_slotid_type = KNO_EXTENDED_TYPECODE(0x1C),
  kno_pool_type = KNO_EXTENDED_TYPECODE(0x1D),
  kno_index_type = KNO_EXTENDED_TYPECODE(0x1E),
  kno_max_xtype = KNO_EXTENDED_TYPECODE(0x1E)

} kno_lisp_type;

#define KNO_BUILTIN_CONS_TYPES 46
#define KNO_BUILTIN_IMMEDIATE_TYPES 14
#define KNO_BUILTIN_EXTENDED_TYPES 11

KNO_EXPORT unsigned int kno_next_cons_type;
KNO_EXPORT unsigned int kno_next_immediate_type;
KNO_EXPORT unsigned int kno_next_extended_type;

typedef int (*kno_checkfn)(lispval);
KNO_EXPORT kno_checkfn kno_immediate_checkfns[KNO_MAX_IMMEDIATE_TYPES+4];

KNO_EXPORT int kno_register_cons_type(char *name,long int longcode);
KNO_EXPORT int kno_register_immediate_type(char *name,kno_checkfn fn,long int longcode);

KNO_EXPORT u8_string kno_type_names[KNO_TYPE_MAX];
KNO_EXPORT u8_string kno_type_docs[KNO_TYPE_MAX];

#define kno_lisp_typename(tc) \
  ( (tc<kno_next_cons_type) ? (kno_type_names[tc]) : ((u8_string)"oddtype"))
#define kno_type2name(tc)        \
  (((tc<0)||(tc>KNO_TYPE_MAX))?  \
   ((u8_string)"oddtype"):      \
   (kno_type_names[tc]))

#define kno_type_name(x) (kno_type2name(KNO_TYPEOF((x))))

/* Type aliases */

KNO_EXPORT int kno_add_type_alias(int reservation,lispval type);
KNO_EXPORT lispval kno_lookup_type_alias(int reservation);

KNO_EXPORT u8_condition kno_get_pointer_exception(lispval x);
KNO_EXPORT lispval kno_badptr_err(lispval badx,u8_context cxt,u8_string details);

#define KNO_VALID_TYPECODEP(x)                                  \
  (KNO_USUALLY((((int)x)>=0) &&                             \
                  (((int)x)<0x200) &&                            \
                  (((x<0x84)&&((x)<kno_next_immediate_type)) || \
                   ((x<0x100)&&((x)<kno_next_cons_type)) || \
		   ((x<0x200)&&((x)<kno_next_cons_type)))))

/* In the type field, 0 means an integer, 1 means an oid, 2 means
   an immediate constant, and 3 means a cons. */
#define KNO_PTR_MANIFEST_TYPE(x) ((x)&(0x3))
#define KNO_CONSP(x) ((KNO_PTR_MANIFEST_TYPE(x)) == kno_cons_ptr_type)
#define KNO_ATOMICP(x) ((KNO_PTR_MANIFEST_TYPE(x))!=kno_cons_ptr_type)
#define KNO_FIXNUMP(x) ((KNO_PTR_MANIFEST_TYPE(x)) == kno_fixnum_ptr_type)
#define KNO_IMMEDIATEP(x) ((KNO_PTR_MANIFEST_TYPE(x)) == kno_immediate_ptr_type)
#define KNO_OIDP(x) ((KNO_PTR_MANIFEST_TYPE(x)) == kno_oid_ptr_type)
#define KNO_INT_DATA(x) ((x)>>2)
#define KNO_EQ(x,y) ((x) == (y))

/* Basic cons structs */

typedef unsigned int kno_consbits;

#if KNO_INLINE_REFCOUNTS && KNO_LOCKFREE_REFCOUNTS && KNO_USE_ATOMIC
#define KNO_ATOMIC_CONSHEAD _Atomic
#else
#define KNO_ATOMIC_CONSHEAD
#endif

#define KNO_CONS_HEADER const kno_consbits conshead

/* The header for typed data structures */
typedef struct KNO_CONS { KNO_CONS_HEADER;} KNO_CONS;
typedef struct KNO_CONS *kno_cons;

/* Raw conses have consbits which can change */
struct KNO_RAW_CONS { kno_consbits conshead;};
typedef struct KNO_RAW_CONS *kno_raw_cons;

#if KNO_LOCKFREE_REFCOUNTS
struct KNO_REF_CONS { KNO_ATOMIC_CONSHEAD kno_consbits conshead;};
typedef struct KNO_REF_CONS *kno_ref_cons;
#define KNO_REF_CONS(x) ((struct KNO_REF_CONS *)(x))
#endif

#if 1
#define KNO_RAW_CONS(x) ((kno_raw_cons)(x))
#else
KNO_FASTOP U8_MAYBE_UNUSED kno_raw_cons KNO_RAW_CONS(lispval x){ return (kno_raw_cons) x;}
#endif

/* The bottom 7 bits of the conshead indicates the type of the cons.  The
   rest is the reference count or zero for "static" (non-reference
   counted) conses.  The 7 bit type code is converted to a real type by
   adding 0x84. */

/* An XCONS is a CONS type which masks the lower bit, and is used for 
   typecode pairs (like kno_pair_type and kno_cdrcode_type) which share
   their high bits */

#define KNO_CONS_TYPE_OFF  (0x84)
#define KNO_CONS_TYPE_MASK (0x7f)
/* These are for cases where a set of two basic types are arranged to
   have the same high bytes, making it easy to test for them. */
#define KNO_XCONS_TYPE_MASK (0x7e)
/* These are for cases where a set of four basic types are all arranged to
   have the same high bytes, making it easy to test for them. */
#define KNO_XXCONS_TYPE_MASK (0x7c)

#if 1
#define KNO_CONS_DATA(x) ((kno_cons)(x))
#define LISPVAL(x)      ((lispval)(x))
#else
KNO_FASTOP U8_MAYBE_UNUSED kno_cons KNO_CONS_DATA(lispval x){ return (kno_cons) x;}
KNO_FASTOP U8_MAYBE_UNUSED lispval LISPVAL(lispval x){ return x;}
#endif

/* Most of the stuff for dealing with conses is in cons.h.  The
    attribute common to all conses, is that its first field is
    a combination of a typecode and a reference count.  We include
    this here so that we can define a pointer type procedure which
    gets the cons type. */
#define LISP_CONS(ptr) ((lispval)ptr)

#define KNO_CONS_TYPEOF(x) \
  (( (((kno_cons)x)->conshead) & (KNO_CONS_TYPE_MASK) )+(KNO_CONS_TYPE_OFF))

#define KNO_XXCONS_TYPEOF(x) \
  (( (((kno_cons)(x))->conshead) & (KNO_XXCONS_TYPE_MASK) )+(KNO_CONS_TYPE_OFF))
#define KNO_XXCONS_TYPEP(x,type) \
  ( (KNO_CONSP(x)) && ( (KNO_XXCONS_TYPEOF((kno_cons)(x))) == ((type)&0xFc) ) )

#define KNO_XCONS_TYPEOF(x) \
  (( (((kno_cons)(x))->conshead) & (KNO_XCONS_TYPE_MASK) )+(KNO_CONS_TYPE_OFF))
#define KNO_XCONS_TYPEP(x,type) \
  ( (KNO_CONSP(x)) && ( (KNO_XCONS_TYPEOF((kno_cons)(x))) == ((type)&0xFc) ) )

#define KNO_CONSPTR(cast,x) ((cast)((kno_cons)x))
#define kno_consptr(cast,x,typecode)                                     \
  ((KNO_USUALLY(KNO_TYPEP(x,typecode))) ?                           \
   ((cast)((kno_cons)(x))) :                                            \
   ((((KNO_CHECK_PTR(x)) ?                                              \
      (kno_raise(kno_TypeError,kno_type_names[typecode],NULL,x)) :      \
      (_kno_bad_pointer(x,kno_type_names[typecode])))),                 \
    ((cast)NULL)))

#define KNO_NULL ((lispval)(NULL))
#define KNO_NULLP(x) (((void *)x) == NULL)

#define KNO_INT2FIX(n)   ( (lispval)  ( ( ((_kno_ptrbits)(n)) << 2 ) | kno_fixnum_type) )

#define KNO_LISP_IMMEDIATE(tcode,serial) \
  ((lispval)((((((_kno_ptrbits)tcode)-0x04)&0x7F)<<25)|(((_kno_ptrbits)serial)<<2)|kno_immediate_ptr_type))
#define LISP_IMMEDIATE KNO_LISP_IMMEDIATE
#define LISPVAL_IMMEDIATE KNO_LISP_IMMEDIATE
#define KNO_GET_IMMEDIATE(x,tcode) (((LISPVAL(x))>>2)&0x7FFFFF)
#define KNO_IMMEDIATE_TYPE_FIELD(x) (((LISPVAL(x))>>25)&0x7F)
#define KNO_IMMEDIATE_TYPEOF(x) ((((LISPVAL(x))>>25)&0x7F)+0x4)
#define KNO_IMMEDIATE_DATA(x) (((LISPVAL(x))>>2)&0x7FFFFF)
#define KNO_IMM_TYPE(x) ((((LISPVAL(x))>>25)&0x7F)+0x4)
#define KNO_IMMEDIATE_TYPEP(x,typecode) \
  (((x)&((((lisp_ptr)0x7f)<<25)|(0x3)))==((((lisp_ptr)((typecode)-0x4))<<25)|0x1))
#define KNO_IMMEDIATE_MAX (1<<24)

/* TYPEOF */

#define KNO_IMMEDIATE_TYPE KNO_IMMEDIATE_TYPEOF

KNO_EXPORT kno_lisp_type _KNO_TYPEOF(lispval x);

#if KNO_EXTREME_PROFILING
#define KNO_TYPEOF _KNO_TYPEOF
#else
#define KNO_TYPEOF(x) \
  (((KNO_PTR_MANIFEST_TYPE(LISPVAL(x)))>1) ? (KNO_PTR_MANIFEST_TYPE(x)) :  \
   ((KNO_PTR_MANIFEST_TYPE(x))==1) ? (KNO_IMMEDIATE_TYPE(x)) : \
   (x) ? (KNO_CONS_TYPEOF(((struct KNO_CONS *)KNO_CONS_DATA(x)))) : \
   (-1))
#endif
#define KNO_PRIM_TYPE(x)         (KNO_TYPEOF(x))

/* TYPEP */

KNO_EXPORT int _KNO_TYPEP(lispval ptr,int type);
KNO_EXPORT int _KNO_XTYPEP(lispval ptr,kno_lisp_type type);

#define KNO_XTYPEP(p,t) (_KNO_XTYPEP(p,t))

#if KNO_EXTREME_PROFILING
#define KNO_TYPEP _KNO_TYPEP
#else
#define KNO_TYPEP(ptr,type)						                    \
  (((type) < 0x04) ? ( ( (ptr) & (0x3) ) == type) :			\
   ((type) < 0x84) ? ( (KNO_IMMEDIATEP(ptr)) && (KNO_IMMEDIATE_TYPEP(ptr,type)) ) : \
   ((type) < 0x100) ?  ( (ptr) && (KNO_CONSP(ptr)) && ((KNO_CONS_TYPEOF(ptr)) == type) ) : \
   (KNO_XTYPEP(ptr,type)))
#endif
#define KNO_PRIM_TYPEP(x,tp)   ( KNO_TYPEP(x,tp) )

#define KNO_MAKE_STATIC(ptr) \
  if (KNO_CONSP(ptr))                                                    \
    (((struct KNO_RAW_CONS *)ptr)->conshead) &= (KNO_CONS_TYPE_MASK);     \
  else {}

#define KNO_MAKE_CONS_STATIC(ptr)  \
  ((struct KNO_RAW_CONS *)ptr)->conshead  &=  KNO_CONS_TYPE_MASK

/* OIDs */

#if KNO_STRUCT_OIDS
typedef struct KNO_OID {
  unsigned int kno_oid_hi, kno_oid_lo;} KNO_OID;
typedef struct KNO_OID *kno_oid;
#define KNO_OID_HI(x) x.kno_oid_hi
#define KNO_OID_LO(x) x.kno_oid_lo
KNO_FASTOP KNO_OID KNO_OID_PLUS(KNO_OID x,unsigned int increment)
{
  x.kno_oid_lo = x.kno_oid_lo+increment;
  return x;
}
#define KNO_SET_OID_HI(oid,hiw) oid.kno_oid_hi = hiw
#define KNO_SET_OID_LO(oid,low) oid.kno_oid_lo = low
#define KNO_OID_COMPARE(oid1,oid2) \
  ((oid1.kno_oid_hi == oid2.kno_oid_hi) ? \
   ((oid1.kno_oid_lo == oid2.kno_oid_lo) ? (0) :                  \
    (oid1.kno_oid_lo > oid2.kno_oid_lo) ? (1) : (-1)) :           \
  (oid1.kno_oid_hi > oid2.kno_oid_hi) ? (1) : (-1))
#define KNO_OID_DIFFERENCE(oid1,oid2) \
  ((oid1.kno_oid_lo>oid2.kno_oid_lo) ? \
   (oid1.kno_oid_lo-oid2.kno_oid_lo) : \
   (oid2.kno_oid_lo-oid1.kno_oid_lo))
#elif KNO_INT_OIDS
typedef unsigned int KNO_OID;
#define KNO_OID_HI(x) ((unsigned int)((x)>>32))
#define KNO_OID_LO(x) ((unsigned int)((x)&(0xFFFFFFFFU)))
#define KNO_OID_PLUS(oid,increment) (oid+increment)
#define KNO_SET_OID_HI(oid,hi) oid = (oid&0xFFFFFFFFUL)|((hi)<<32)
#define KNO_SET_OID_LO(oid,lo) \
  oid = ((oid&0xFFFFFFFF00000000U)|(((KNO_OID)lo)&0xFFFFFFFFU))
#define KNO_OID_COMPARE(oid1,oid2) \
  ((oid1 == oid2) ? (0) : (oid1<oid2) ? (-1) : (1))
#define KNO_OID_DIFFERENCE(oid1,oid2) \
  ((oid1>oid2) ? (oid1-oid2) : (oid2-oid1))
#elif KNO_LONG_OIDS
typedef unsigned long KNO_OID;
#define KNO_OID_HI(x) ((unsigned int)((x)>>32))
#define KNO_OID_LO(x) ((unsigned int)((x)&(0xFFFFFFFFU)))
#define KNO_OID_PLUS(oid,increment) (oid+increment)
#define KNO_SET_OID_HI(oid,hi) \
  oid = ((KNO_OID)(oid&0xFFFFFFFFU))|(((KNO_OID)hi)<<32)
#define KNO_SET_OID_LO(oid,lo) \
  oid = (oid&0xFFFFFFFF00000000UL)|((lo)&0xFFFFFFFFUL)
#define KNO_OID_COMPARE(oid1,oid2) \
  ((oid1 == oid2) ? (0) : (oid1<oid2) ? (-1) : (1))
#define KNO_OID_DIFFERENCE(oid1,oid2) \
  ((oid1>oid2) ? (oid1-oid2) : (oid2-oid1))
#elif KNO_LONG_LONG_OIDS
typedef unsigned long long KNO_OID;
#define KNO_OID_HI(x) ((unsigned int)((x)>>32))
#define KNO_OID_LO(x) ((unsigned int)((x)&(0xFFFFFFFFULL)))
#define KNO_OID_PLUS(oid,increment) (oid+increment)
#define KNO_SET_OID_HI(oid,hi) \
  oid = ((KNO_OID)(oid&0xFFFFFFFFU))|(((KNO_OID)hi)<<32)
#define KNO_SET_OID_LO(oid,lo) \
  oid = ((oid&(0xFFFFFFFF00000000ULL))|((unsigned int)((lo)&(0xFFFFFFFFULL))))
#define KNO_OID_COMPARE(oid1,oid2) \
  ((oid1 == oid2) ? (0) : (oid1>oid2) ? (1) : (-1))
#define KNO_OID_DIFFERENCE(oid1,oid2) \
  ((oid1>oid2) ? (oid1-oid2) : (oid2-oid1))
#endif

#if KNO_STRUCT_OIDS
KNO_FASTOP KNO_OID KNO_MAKE_OID(unsigned int hi,unsigned int lo)
{
  KNO_OID result; result.kno_oid_hi = hi; result.kno_oid_lo = lo;
  return result;
}
#define KNO_NULL_OID_INIT {0,0}
#else
KNO_FASTOP KNO_OID KNO_MAKE_OID(unsigned int hi,unsigned int lo)
{
  return ((((KNO_OID)hi)<<32)|((KNO_OID)lo));
}
#define KNO_NULL_OID_INIT 0
#endif

#if SIZEOF_VOID_P == 4
#define KNO_OID_BUCKET_WIDTH 10 /* (1024 buckets) */
#else
#define KNO_OID_BUCKET_WIDTH 16 /* (65536 buckets) */
#endif

#define KNO_OID_OFFSET_WIDTH 20
#define KNO_OID_BUCKET_SIZE  (1<<KNO_OID_OFFSET_WIDTH)
#define KNO_OID_OFFSET_MASK  ((KNO_OID_BUCKET_SIZE)-1)

#define KNO_OID_BUCKET_MASK ((1<<(KNO_OID_BUCKET_WIDTH))-1)
#define KNO_N_OID_BUCKETS   (1<<(2+KNO_OID_BUCKET_WIDTH))

/* An OID references has 10 bits of base index and 20 bits of offset.
   This is encoded in a LISP pointer with the offset in the high 20 bits
   and the base in the lower portion. */

struct KNO_OID_BUCKET {
  KNO_OID bucket_base;
  int bucket_no;};

KNO_EXPORT KNO_OID kno_base_oids[KNO_N_OID_BUCKETS];
KNO_EXPORT struct KNO_OID_BUCKET *kno_oid_buckets;
KNO_EXPORT int kno_n_base_oids, kno_oid_buckets_len;


/* We represent OIDs using an indexed representation where k bits are
   used to represent 2^k possible buckets of 2^20 OIDs starting at
   "base oids". */
/*
  The bit layout of an OID pointer is:
  pos:  |                 k*4          2  0
        | offset from base | bucket id |11|
  size: |    (20 bits)     | (k bits)  |
*/

/*  In 32-bit words, this fills the whole word. In 64-bit words,
    there's a lot of room at the top, based on whatever value is
    selected for k (KNO_OID_BUCKET_WIDTH).

*/
#define KNO_OID_ADDR(x) \
  (KNO_OID_PLUS(kno_base_oids[((x>>2)&(KNO_OID_BUCKET_MASK))],\
               (x>>((KNO_OID_BUCKET_WIDTH)+2))))
#define KNO_CONSTRUCT_OID(baseid,offset) \
  ((lispval) ((((kno_ptrbits)(baseid))<<2)|				\
	      (((kno_ptrbits)(offset))<<((KNO_OID_BUCKET_WIDTH)+2))|3))
KNO_EXPORT lispval kno_make_oid(KNO_OID addr);
KNO_EXPORT int kno_get_oid_base_index(KNO_OID addr,int add);

#define KNO_OID_BASE_ID(x)     (((x)>>2)&(KNO_OID_BUCKET_MASK))
#define KNO_OID_BASE_OFFSET(x) ((x)>>(KNO_OID_BUCKET_WIDTH+2))

KNO_EXPORT const int _KNO_OID_BUCKET_WIDTH;
KNO_EXPORT const int _KNO_OID_BUCKET_MASK;

KNO_EXPORT char *kno_ulonglong_to_b32(unsigned long long offset,char *buf,int *len);
KNO_EXPORT int kno_b32_to_ulonglong(const char *digits,unsigned long long *out);
KNO_EXPORT long long kno_b32_to_longlong(const char *digits);
KNO_EXPORT u8_string kno_oid2string(lispval oidval,u8_byte *buf,ssize_t len);

/* Fixnums */

/* Fixnums use PTR_WIDTH-2 bits to represent integer values and are
   optimized to assume a standard 2s-complement representation for
   negative numbers. So, the layout of an integer is:
     signbit magnitude typecode(2 bits)
   and we try make conversion as simple as possible.
 */

#define KNO_PTRMASK     (~((_kno_ptrbits)3))
#define KNO_FIXNUM_BITS ((KNO_PTR_WIDTH)-2)
#define KNO_2CMASK      (((_kno_ptrbits)1)<<(KNO_PTR_WIDTH-1))

/* Fixnum conversion */
/* For KNO_FIX2INT, we convert the fixnum to a long long and mask out
   the type bits; we then just divide by four to get the integer
   value. */
#define KNO_FIX2INT(fx)  ( (_kno_sptr) ( ( ((_kno_sptr) fx) & (~0x3)) / 4) )
#define KNO_INT2FIX(n)   ( (lispval)  ( ( ((_kno_ptrbits)(n)) << 2 ) | kno_fixnum_type) )

#define KNO_MAX_FIXNUM ((((long long)1)<<(KNO_FIXNUM_BITS-1))-1)
#define KNO_MIN_FIXNUM -((((long long)1)<<(KNO_FIXNUM_BITS-1))-1)

#define KNO_MAX_FIXVAL (INT2FIX(KNO_MAX_FIXNUM))
#define KNO_MIN_FIXVAL (INT2FIX(KNO_MIN_FIXNUM))

#if ( KNO_FIXNUM_BITS >= 56 )
#define KNO_FIXNUM_BYTES 7
#elif ( KNO_FIXNUM_BITS >= 56 )
#define KNO_FIXNUM_BYTES 3
#else
#define KNO_FIXNUM_BYTES 3
#endif

#define to64(x) ((long long)(x))
#define to64u(x) ((unsigned long long)(x))

#define KNO_MAKE_FIXNUM KNO_INT2FIX

KNO_EXPORT lispval kno_make_bigint(long long intval);

#define KNO_CPP_INT(n)                                    \
  ( (sizeof(n) < KNO_FIXNUM_BYTES) ?                      \
    (KNO_INT2FIX(n)) :                                        \
    ( (((long long)(n)) > KNO_MAX_FIXNUM) ||                  \
      (((long long)(n)) < KNO_MIN_FIXNUM) ) ?                 \
    (kno_make_bigint(n)) :                                    \
    (KNO_INT2FIX(n)))

/* The sizeof check here avoids *tautological* range errors
   when n can't be too big for a fixnum. */
#define KNO_INT2LISP(n)               \
  ( (sizeof(n) < KNO_FIXNUM_BYTES) ?                      \
    (KNO_INT2FIX(n)) :                                    \
    ( (((long long)(n)) > KNO_MAX_FIXNUM) ||                  \
      (((long long)(n)) < KNO_MIN_FIXNUM) ) ?                 \
    (kno_make_bigint(n)) :                                    \
    (KNO_INT2FIX(n)))
#define KNO_INT(x) (KNO_INT2LISP(x))
#define KNO_MAKEINT(x) (KNO_INT2LISP(x))

#define KNO_UINT2LISP(x) \
  (((to64u(x)) > (to64(KNO_MAX_FIXNUM))) ?                       \
   (kno_make_bigint(to64u(x))) :                                 \
   ((lispval) (((to64u(x))*4)|kno_fixnum_type)))

#define KNO_SHORT2LISP(shrt) (KNO_INT2FIX((long long)shrt))
#define KNO_SHORT2FIX(shrt)   (KNO_INT2FIX((long long)shrt))

#define KNO_USHORT2LISP(x)     ((lispval)(kno_fixnum_type|((x&0xFFFF)<<2)))
#define KNO_BYTE2LISP(x)       ((lispval) (kno_fixnum_type|((x&0xFF)<<2)))
#define KNO_FIXNUM_MAGNITUDE(x) ((x<0)?((-(x))>>2):(x>>2))
#define KNO_FIXNUM_NEGATIVEP(x) (x<0)

#define KNO_ZERO            (KNO_SHORT2LISP(0))
#define KNO_FIXZERO         (KNO_SHORT2LISP(0))
#define KNO_FIXNUM_ZERO     (KNO_SHORT2LISP(0))
#define KNO_FIXNUM_ONE      (KNO_SHORT2LISP(1))
#define KNO_FIXNUM_NEGONE   (KNO_SHORT2LISP(-1))

#define KNO_POSFIXP(x) ((KNO_FIXNUMP(x))&&((KNO_FIX2INT(x))>0))
#define KNO_NEGFIXP(x) ((KNO_FIXNUMP(x))&&((KNO_FIX2INT(x))<0))
#define KNO_WHOLEP(x) ((KNO_FIXNUMP(x))&&((KNO_FIX2INT(x))>=0))

#if KNO_FIXNUM_BITS <= 30
#define KNO_INTP(x) (KNO_FIXNUMP(x))
#define KNO_UINTP(x) ((KNO_FIXNUMP(x))&&((KNO_FIX2INT(x))>=0))
#define KNO_LONGP(x) (KNO_FIXNUMP(x))
#define KNO_ULONGP(x) ((KNO_FIXNUMP(x))&&((KNO_FIX2INT(x))>=0))
#else
#define KNO_INTP(x) \
  ((KNO_FIXNUMP(x))&&(KNO_FIX2INT(x)<=INT_MAX)&&(KNO_FIX2INT(x)>=INT_MIN))
#define KNO_UINTP(x) \
  ( (KNO_FIXNUMP(x)) && ((KNO_FIX2INT(x))>=0) && (KNO_FIX2INT(x)<=UINT_MAX) )
#define KNO_LONGP(x) (KNO_FIXNUMP(x))
#define KNO_ULONGP(x) ( (KNO_FIXNUMP(x)) && ((KNO_FIX2INT(x))>=0) )
#endif

#define KNO_BYTEP(x) \
  ( (KNO_FIXNUMP(x)) && (KNO_FIX2INT(x)>=0) && (KNO_FIX2INT(x)<0x100) )
#define KNO_SHORTP(x) \
  ( (KNO_FIXNUMP(x)) && (KNO_FIX2INT(x)>=SHRT_MIN) && \
    (KNO_FIX2INT(x)<=SHRT_MAX) )
#define KNO_USHORTP(x) \
  ( (KNO_FIXNUMP(x)) && (KNO_FIX2INT(x)>=0) && (KNO_FIX2INT(x)<=USHRT_MAX) )

#define KNO_POSITIVE_FIXNUMP(n) \
  ( (KNO_FIXNUMP(x)) && (KNO_FIX2INT(x)>0) )
#define KNO_NOTNEG_FIXNUMP(n) \
  ( (KNO_FIXNUMP(x)) && (KNO_FIX2INT(x)>=0) )
#define KNO_POSITIVE_FIXINTP(n) \
  ( (KNO_INTP(x)) && (KNO_FIX2INT(x)>0) )
#define KNO_NOTNEG_FIXINTP(n) \
  ( (KNO_INTP(x)) && (KNO_FIX2INT(x)>=0) )

/* Constants */

#define KNO_CONSTANT(i) ((lispval)(kno_immediate_ptr_type|(i<<2)))

#define KNO_VOID                   KNO_CONSTANT(0)
#define KNO_FALSE                  KNO_CONSTANT(1)
#define KNO_TRUE                   KNO_CONSTANT(2)
#define KNO_EMPTY_CHOICE           KNO_CONSTANT(3)
#define KNO_EMPTY_LIST             KNO_CONSTANT(4)
#define KNO_DEFAULT_VALUE          KNO_CONSTANT(5)
#define KNO_TAIL_LOOP              KNO_CONSTANT(6)
#define KNO_EOF                    KNO_CONSTANT(7)
#define KNO_EOD                    KNO_CONSTANT(8)
#define KNO_EOX                    KNO_CONSTANT(9)
#define KNO_DTYPE_ERROR            KNO_CONSTANT(10)
#define KNO_PARSE_ERROR            KNO_CONSTANT(11)
#define KNO_OOM                    KNO_CONSTANT(12)
#define KNO_TYPE_ERROR             KNO_CONSTANT(13)
#define KNO_RANGE_ERROR            KNO_CONSTANT(14)
#define KNO_ERROR_VALUE            KNO_CONSTANT(15)
#define KNO_BADPTR                 KNO_CONSTANT(16)
#define KNO_THROW_VALUE            KNO_CONSTANT(17)
#define KNO_BREAK                  KNO_CONSTANT(18)
#define KNO_UNBOUND                KNO_CONSTANT(19)
#define KNO_NEVERSEEN              KNO_CONSTANT(20)
#define KNO_LOCKHOLDER             KNO_CONSTANT(21)
#define KNO_UNALLOCATED_OID        KNO_CONSTANT(22)
#define KNO_QVOID                  KNO_CONSTANT(23)
#define KNO_REQUIRED_VALUE         KNO_CONSTANT(24)
#define KNO_BLANK_PARSE            KNO_CONSTANT(25)

#define KNO_TAIL                   KNO_TAIL_LOOP

#define KNO_N_BUILTIN_CONSTANTS   25

KNO_EXPORT const char *kno_constant_names[];
KNO_EXPORT int kno_n_constants;
KNO_EXPORT lispval kno_register_constant(u8_string name);
KNO_EXPORT u8_string kno_constant_name(lispval x);

KNO_EXPORT lispval _kno_return_errcode(lispval x);

#define KNO_VOIDP(x) ((x) == (KNO_VOID))
#define KNO_NOVOIDP(x) ((x) != (KNO_VOID))
#define KNO_FALSEP(x) ((x) == (KNO_FALSE))
#define KNO_DEFAULTP(x) ((x) == (KNO_DEFAULT_VALUE))
#define KNO_TRUEP(x) ((x) == (KNO_TRUE))
#define KNO_EMPTY_CHOICEP(x) ((x) == (KNO_EMPTY_CHOICE))
#define KNO_EXISTSP(x) (!(x == KNO_EMPTY_CHOICE))
#define KNO_EMPTY_LISTP(x) ((x) == (KNO_EMPTY_LIST))
#define KNO_NILP(x) ((x) == (KNO_EMPTY_LIST))
#define KNO_EOFP(x) ((x) == (KNO_EOF))
#define KNO_EODP(x) ((x) == (KNO_EOD))
#define KNO_EOXP(x) ((x) == (KNO_EOX))
#define KNO_UNBOUNDP(x) ((x) == (KNO_UNBOUND))

#define KNO_AGNOSTICP(x) ( (KNO_VOIDP(x)) || (KNO_DEFAULTP(x)) )
#define KNO_BREAKP(result) ((result) == (KNO_BREAK))
#define KNO_THROWP(result) ((result) == (KNO_THROW_VALUE))

#define KNO_EMPTY     (KNO_EMPTY_CHOICE)
#define KNO_DEFAULT   (KNO_DEFAULT_VALUE)
#define KNO_ERROR     (KNO_ERROR_VALUE)
#define KNO_EMPTYP(x) (KNO_EMPTY_CHOICEP(x))

#if KNO_EXTREME_PROFILING
KNO_EXPORT int _KNO_ABORTP(lispval x);
KNO_EXPORT int _KNO_ERRORP(lispval x);
#define KNO_ABORTP _KNO_ABORTP
#define KNO_ERRORP _KNO_ERRORP
#else
#define KNO_ABORTP(x) \
  (((KNO_TYPEP(x,kno_constant_type)) && \
    (KNO_GET_IMMEDIATE(x,kno_constant_type)>7) && \
    (KNO_GET_IMMEDIATE(x,kno_constant_type)<=18)))
#define KNO_ERRORP(x) \
  (((KNO_TYPEP(x,kno_constant_type)) && \
    (KNO_GET_IMMEDIATE(x,kno_constant_type)>7) && \
    (KNO_GET_IMMEDIATE(x,kno_constant_type)<16)))
#endif
#define KNO_TROUBLEP(x) (KNO_RARELY(KNO_ERRORP(x)))
#define KNO_COOLP(x) (!(KNO_TROUBLEP(x)))

#define KNO_BROKEP(x) (KNO_RARELY(KNO_BREAKP(x)))
#define KNO_ABORTED(x) (KNO_RARELY(KNO_ABORTP(x)))
#define KNO_INTERRUPTED() (KNO_RARELY(u8_current_exception!=(NULL)))

#define KNO_CONSTANTP(x) (KNO_IMMEDIATE_TYPEP(x,kno_constant_type))

/* Various ways to say 'no value' */
#define KNO_UNSPECIFIEDP(x) \
  ( (KNO_VOIDP(x)) || (KNO_FALSEP(x)) || (KNO_EMPTYP(x)) || (KNO_DEFAULTP(x)) )

#define KNO_NIL (KNO_EMPTY_LIST)

/* Characters */

#define KNO_CHARACTERP(x) (KNO_IMMEDIATE_TYPEP(x,kno_character_type))
#define KNO_CODE2CHAR(x)  (LISPVAL_IMMEDIATE(kno_character_type,x))
#define KNO_CHARCODE(x)   (KNO_GET_IMMEDIATE(x,kno_character_type))
#define KNO_CHAR2CODE(x)  (KNO_GET_IMMEDIATE(x,kno_character_type))

/* CODEREFS */

#define KNO_CODEREFP(x)     (KNO_IMMEDIATE_TYPEP(x,kno_codref_type))
#define KNO_DECODEREF(cref) (KNO_IMMEDIATE_DATA(cref))
#define KNO_ENCODEREF(off)  (LISPVAL_IMMEDIATE(kno_coderef_type,off))

/* Lexrefs */

#define KNO_LEXREFP(x)       (KNO_IMMEDIATE_TYPEP(x,kno_lexref_type))
#define KNO_LEXREF_UP(x) (   (KNO_GET_IMMEDIATE((x),kno_lexref_type))/32)
#define KNO_LEXREF_ACROSS(x) ((KNO_GET_IMMEDIATE((x),kno_lexref_type))%32)

/* Opcodes */

/* These are used by the evaluator.  We define the immediate type here,
   rather than dyanmically, so that they can be compile time constants
   and dispatch very quickly. */

#define KNO_OPCODEP(x) (KNO_IMMEDIATE_TYPEP(x,kno_opcode_type))

#define KNO_OPCODE(num) (LISPVAL_IMMEDIATE(kno_opcode_type,num))
#define KNO_OPCODE_NUM(op) (KNO_GET_IMMEDIATE(op,kno_opcode_type))
#define KNO_NEXT_OPCODE(op) \
  (LISPVAL_IMMEDIATE(kno_opcode_type,(1+(KNO_OPCODE_NUM(op)))))

/* CTYPE types */

#define KNO_CTYPE(ctype) (LISPVAL_IMMEDIATE(kno_ctype_type,((int)ctype)))
#define KNO_CTYPEP(x)    (KNO_TYPEP(x,kno_ctype_type))
#define KNO_CTYPE_CODE(x) \
  ( (KNO_VOIDP(x)) ? (kno_any_type) :		\
    (KNO_TYPEP(x,kno_ctype_type)) ?		\
    ((kno_lisp_type)(KNO_IMMEDIATE_DATA(x))) :	\
    (kno_any_type) )

#define KNO_OID_TYPE KNO_CTYPE(kno_oid_type)
#define KNO_CONS_TYPE KNO_CTYPE(kno_cons_type)
#define KNO_FIXNUM_TYPE KNO_CTYPE(kno_fixnum_type)
#define KNO_IMEDIATE_TYPE KNO_CTYPE(kno_immediate_type)

#define KNO_CONSTANT_TYPE KNO_CTYPE(kno_constant_type)
#define KNO_CHARACTER_TYPE KNO_CTYPE(kno_character_type)
#define KNO_SYMBOL_TYPE KNO_CTYPE(kno_symbol_type)
#define KNO_FCNID_TYPE KNO_CTYPE(kno_fcnid_type)
#define KNO_LEXREF_TYPE KNO_CTYPE(kno_lexref_type)
#define KNO_OPCODE_TYPE KNO_CTYPE(kno_opcode_type)
#define KNO_TYPEREF_TYPE KNO_CTYPE(kno_typeref_type)
#define KNO_CODEREF_TYPE KNO_CTYPE(kno_coderef_type)
#define KNO_POOLREF_TYPE KNO_CTYPE(kno_poolref_type)
#define KNO_INDEXREF_TYPE KNO_CTYPE(kno_indexref_type)
#define KNO_HISTREF_TYPE KNO_CTYPE(kno_histref_type)
#define KNO_CTYPE_TYPE KNO_CTYPE(kno_ctype_type)
#define KNO_STRING_TYPE KNO_CTYPE(kno_string_type)
#define KNO_PACKET_TYPE KNO_CTYPE(kno_packet_type)
#define KNO_VECTOR_TYPE KNO_CTYPE(kno_vector_type)
#define KNO_NUMERIC_VECTOR_TYPE KNO_CTYPE(kno_numeric_vector_type)
#define KNO_PAIR_TYPE KNO_CTYPE(kno_pair_type)
#define KNO_CDRCODE_TYPE KNO_CTYPE(kno_cdrcode_type)
#define KNO_SECRET_TYPE KNO_CTYPE(kno_secret_type)
#define KNO_BIGINT_TYPE KNO_CTYPE(kno_bigint_type)
#define KNO_CHOICE_TYPE KNO_CTYPE(kno_choice_type)
#define KNO_PRECHOICE_TYPE KNO_CTYPE(kno_prechoice_type)
#define KNO_QCHOICE_TYPE KNO_CTYPE(kno_qchoice_type)
#define KNO_TYPEINFO_TYPE KNO_CTYPE(kno_typeinfo_type)
#define KNO_COMPOUND_TYPE KNO_CTYPE(kno_compound_type)
#define KNO_RAWPTR_TYPE KNO_CTYPE(kno_rawptr_type)
#define KNO_IOPORT_TYPE KNO_CTYPE(kno_ioport_type)
#define KNO_REGEX_TYPE KNO_CTYPE(kno_regex_type)
#define KNO_CORETABLE_TYPE KNO_CTYPE(kno_coretable_type)
#define KNO_SLOTMAP_TYPE KNO_CTYPE(kno_slotmap_type)
#define KNO_SCHEMAP_TYPE KNO_CTYPE(kno_schemap_type)
#define KNO_HASHTABLE_TYPE KNO_CTYPE(kno_hashtable_type)
#define KNO_HASHSET_TYPE KNO_CTYPE(kno_hashset_type)
#define KNO_FUNCTION_TYPE KNO_CTYPE(kno_function_type)
#define KNO_CPRIM_TYPE KNO_CTYPE(kno_cprim_type)
#define KNO_LAMBDA_TYPE KNO_CTYPE(kno_lambda_type)
#define KNO_FFI_TYPE KNO_CTYPE(kno_ffi_type)
#define KNO_RPC_TYPE KNO_CTYPE(kno_rpc_type)
#define KNO_LEXENV_TYPE KNO_CTYPE(kno_lexenv_type)
#define KNO_EVALFN_TYPE KNO_CTYPE(kno_evalfn_type)
#define KNO_MACRO_TYPE KNO_CTYPE(kno_macro_type)
#define KNO_STACKFRAME_TYPE KNO_CTYPE(kno_stackframe_type)
#define KNO_EXCEPTION_TYPE KNO_CTYPE(kno_exception_type)
#define KNO_PROMISE_TYPE KNO_CTYPE(kno_promise_type)
#define KNO_THREAD_TYPE KNO_CTYPE(kno_thread_type)
#define KNO_SYNCHRONIZER_TYPE KNO_CTYPE(kno_synchronizer_type)
#define KNO_CONSBLOCK_TYPE KNO_CTYPE(kno_consblock_type)
#define KNO_COMPLEX_TYPE KNO_CTYPE(kno_complex_type)
#define KNO_RATIONAL_TYPE KNO_CTYPE(kno_rational_type)
#define KNO_FLONUM_TYPE KNO_CTYPE(kno_flonum_type)
#define KNO_TIMESTAMP_TYPE KNO_CTYPE(kno_timestamp_type)
#define KNO_UUID_TYPE KNO_CTYPE(kno_uuid_type)
#define KNO_MYSTERY_TYPE KNO_CTYPE(kno_mystery_type)
#define KNO_STREAM_TYPE KNO_CTYPE(kno_stream_type)
#define KNO_SERVICE_TYPE KNO_CTYPE(kno_service_type)
#define KNO_BLOOM_FILTER KNO_CTYPE(kno_bloom_filter_type)
#define KNO_SQLDB_TYPE KNO_CTYPE(kno_sqldb_type)
#define KNO_SQLPROC_TYPE KNO_CTYPE(kno_sqlproc_type)
#define KNO_PATHSTORE_TYPE KNO_CTYPE(kno_pathstore_type)
#define KNO_CONSED_INDEX KNO_CTYPE(kno_consed_index_type)
#define KNO_CONSED_POOL KNO_CTYPE(kno_consed_pool_type)
#define KNO_SUBPROC_TYPE KNO_CTYPE(kno_subproc_type)
#define KNO_EMPTY_TYPE KNO_CTYPE(kno_empty_type)
#define KNO_EXISTS_TYPE KNO_CTYPE(kno_exists_type)
#define KNO_SINGLETON_TYPE KNO_CTYPE(kno_singleton_type)
#define KNO_TRUE_TYPE KNO_CTYPE(kno_true_type)
#define KNO_ERROR_TYPE KNO_CTYPE(kno_error_type)
#define KNO_VOID_TYPE KNO_CTYPE(kno_void_type)
#define KNO_SATISFIED_TYPE KNO_CTYPE(kno_satisfied_type)
#define KNO_NUMBER_TYPE KNO_CTYPE(kno_number_type)
#define KNO_INTEGER_TYPE KNO_CTYPE(kno_integer_type)
#define KNO_SEQUENCE_TYPE KNO_CTYPE(kno_sequence_type)
#define KNO_TABLE_TYPE KNO_CTYPE(kno_table_type)
#define KNO_APPLICABLE_TYPE KNO_CTYPE(kno_applicable_type)
#define KNO_KEYMAP_TYPE KNO_CTYPE(kno_keymap_type)
#define KNO_TYPE_TYPE KNO_CTYPE(kno_type_type)
#define KNO_OPTS_TYPE KNO_CTYPE(kno_opts_type)
#define KNO_FRAME_TYPE KNO_CTYPE(kno_frame_type)
#define KNO_SLOTID_TYPE KNO_CTYPE(kno_slotid_type)
#define KNO_POOL_TYPE KNO_CTYPE(kno_pool_type)
#define KNO_INDEX_TYPE KNO_CTYPE(kno_index_type)

/* Symbols */

#ifndef KNO_CORE_SYMBOLS
#define KNO_CORE_SYMBOLS 8192
#endif
KNO_EXPORT lispval *kno_symbol_names;
KNO_EXPORT int kno_n_symbols;
KNO_EXPORT u8_rwlock kno_symbol_lock;

#define KNO_SYMBOLP(x) (KNO_IMMEDIATE_TYPEP(x,kno_symbol_type))

#define KNO_GOOD_SYMBOLP(x) \
  ((KNO_IMMEDIATE_TYPEP(x,kno_symbol_type)) && \
   (KNO_GET_IMMEDIATE(x,kno_symbol_type)<kno_n_symbols))

#define KNO_SYMBOL2ID(x) (KNO_GET_IMMEDIATE(x,kno_symbol_type))
#define KNO_ID2SYMBOL(i) (LISPVAL_IMMEDIATE(kno_symbol_type,i))

#define KNO_SYMBOL_NAME(x) \
  ((KNO_GOOD_SYMBOLP(x)) ? \
   (KNO_CSTRING(kno_symbol_names[KNO_SYMBOL2ID(x)])) :     \
   ((u8_string )("#.bad$ymbol.#")))
#define KNO_XSYMBOL_NAME(x) (KNO_CSTRING(kno_symbol_names[KNO_SYMBOL2ID(x)]))

KNO_EXPORT lispval kno_make_symbol(u8_string string,int len);
KNO_EXPORT lispval kno_probe_symbol(u8_string string,int len);
KNO_EXPORT lispval kno_fixcase_symbol(u8_string string,int len);
KNO_EXPORT lispval kno_intern(u8_string string);
KNO_EXPORT lispval kno_getsym(u8_string string);
KNO_EXPORT lispval kno_all_symbols(void);

#define DEF_KNOSYM(s) \
  static lispval s ## _symbol = KNO_NULL
#define KNOSYM(s) \
  ( (s ## _symbol) ? (s ## _symbol) : ((s ## _symbol=kno_intern(# s))) )

KNO_EXPORT int kno_flipcase_fix;

KNO_EXPORT lispval KNOSYM_ADD, KNOSYM_ADJUNCT, KNOSYM_ALL, KNOSYM_ALWAYS;
KNO_EXPORT lispval KNOSYM_ARG, KNOSYM_ATMARK;
KNO_EXPORT lispval KNOSYM_BLOCKSIZE, KNOSYM_BUFSIZE;
KNO_EXPORT lispval KNOSYM_CACHELEVEL, KNOSYM_CACHESIZE;
KNO_EXPORT lispval KNOSYM_CONS, KNOSYM_CONTENT, KNOSYM_CREATE;
KNO_EXPORT lispval KNOSYM_DEFAULT, KNOSYM_DRIVER, KNOSYM_DOT, KNOSYM_DROP;
KNO_EXPORT lispval KNOSYM_DTYPE;
KNO_EXPORT lispval KNOSYM_ENCODING, KNOSYM_EQUALS, KNOSYM_ERROR;
KNO_EXPORT lispval KNOSYM_FILE, KNOSYM_FILENAME;
KNO_EXPORT lispval KNOSYM_FLAGS, KNOSYM_FORMAT, KNOSYM_FRONT;
KNO_EXPORT lispval KNOSYM_GT, KNOSYM_GTE;
KNO_EXPORT lispval KNOSYM_HASHMARK, KNOSYM_HISTREF, KNOSYM_HISTORY_TAG;
KNO_EXPORT lispval KNOSYM_ID, KNOSYM_INDEX, KNOSYM_INPUT;
KNO_EXPORT lispval KNOSYM_ISADJUNCT, KNOSYM_KEY, KNOSYM_KEYSLOT, KNOSYM_KEYVAL;
KNO_EXPORT lispval KNOSYM_LABEL, KNOSYM_LAZY, KNOSYM_LENGTH, KNOSYM_LOGLEVEL;
KNO_EXPORT lispval KNOSYM_LT, KNOSYM_LTE;
KNO_EXPORT lispval KNOSYM_MAIN, KNOSYM_MERGE, KNOSYM_METADATA;
KNO_EXPORT lispval KNOSYM_MINUS, KNOSYM_MODULE, KNOSYM_MODULEID;
KNO_EXPORT lispval KNOSYM_NAME, KNOSYM_NO, KNOSYM_NONE, KNOSYM_NOT;
KNO_EXPORT lispval KNOSYM_OPT, KNOSYM_OPTIONAL, KNOSYM_OPTS, KNOSYM_OUTPUT;
KNO_EXPORT lispval KNOSYM_PACKET, KNOSYM_PCTID, KNOSYM_PLUS, KNOSYM_POOL;
KNO_EXPORT lispval KNOSYM_PREFIX, KNOSYM_PROPS;
KNO_EXPORT lispval KNOSYM_QMARK, KNOSYM_QUOTE, KNOSYM_READONLY, KNOSYM_SEP;
KNO_EXPORT lispval KNOSYM_SET, KNOSYM_SIZE, KNOSYM_SORT, KNOSYM_SORTED;
KNO_EXPORT lispval KNOSYM_SOURCE, KNOSYM_STAR, KNOSYM_STORE, KNOSYM_STRING;
KNO_EXPORT lispval KNOSYM_SUFFIX;
KNO_EXPORT lispval KNOSYM_TAG, KNOSYM_TEST, KNOSYM_TEXT, KNOSYM_TYPE;
KNO_EXPORT lispval KNOSYM_UTF8;
KNO_EXPORT lispval KNOSYM_VALUE, KNOSYM_VERSION, KNOSYM_VOID;
KNO_EXPORT lispval KNOSYM_XREFS, KNOSYM_XTYPE, KNOSYM_XXREFS;
KNO_EXPORT lispval KNOSYM_ZIPSOURCE;

KNO_EXPORT lispval kno_timestamp_xtag,
  kno_rational_xtag, kno_complex_xtag, kno_regex_xtag,
  kno_qchoice_xtag, kno_bigtable_xtag, kno_bigset_xtag;
KNO_EXPORT lispval kno_zlib_xtag, kno_zstd_xtag, kno_snappy_xtag;

/* Function IDs */

/* Function IDs are immediate values which refer to
   conses stored in a persistent table.  The idea is that
   persistent pointers are not subject to GC, so they can
   be passed much more quickly and without thread contention. */

#define KNO_FCNIDP(x) (KNO_IMMEDIATE_TYPEP(x,kno_fcnid_type))

KNO_EXPORT struct KNO_CONS **_kno_fcnids[];
KNO_EXPORT int _kno_fcnid_count;
KNO_EXPORT u8_mutex _kno_fcnid_lock;
KNO_EXPORT int _kno_leak_fcnids;

#ifndef KNO_FCNID_BLOCKSIZE
#define KNO_FCNID_BLOCKSIZE 256
#endif

#ifndef KNO_FCNID_NBLOCKS
#define KNO_FCNID_NBLOCKS 256
#endif

#ifndef KNO_INLINE_FCNIDS
#define KNO_INLINE_FCNIDS 0
#endif

KNO_EXPORT lispval kno_resolve_fcnid(lispval ref);
KNO_EXPORT lispval kno_register_fcnid(lispval obj);
KNO_EXPORT lispval kno_set_fcnid(lispval ref,lispval newval);
KNO_EXPORT int kno_deregister_fcnid(lispval id,lispval value);

KNO_EXPORT u8_condition kno_InvalidFCNID, kno_FCNIDOverflow;

#if KNO_INLINE_FCNIDS
static U8_MAYBE_UNUSED lispval _kno_fcnid_ref(lispval ref)
{
  if (KNO_IMMEDIATE_TYPEP(ref,kno_fcnid_type)) {
    int serialno = KNO_GET_IMMEDIATE(ref,kno_fcnid_type);
    if (KNO_RARELY(serialno>_kno_fcnid_count))
      return kno_err(kno_InvalidFCNID,"_kno_fcnid_ref",NULL,ref);
    else return (lispval) _kno_fcnids
           [serialno/KNO_FCNID_BLOCKSIZE]
           [serialno%KNO_FCNID_BLOCKSIZE];}
  else return ref;
}
#define kno_fcnid_ref(x) ((KNO_FCNIDP(x))?(_kno_fcnid_ref(x)):(x))
#else
#define kno_fcnid_ref(x) ((KNO_FCNIDP(x))?(kno_resolve_fcnid(x)):(x))
#endif

#define KNO_FCNID_TYPEP(x,tp)    (KNO_TYPEP(kno_fcnid_ref(x),tp))
#define KNO_FCNID_TYPEOF(x)      (KNO_TYPEOF(kno_fcnid_ref(x)))

/* Numeric macros */

#define KNO_NUMBER_TYPEP(x) \
  ((x == kno_fixnum_type) || (x == kno_bigint_type) ||   \
   (x == kno_flonum_type) || (x == kno_rational_type) || \
   (x == kno_complex_type))
#define KNO_NUMBERP(x) (KNO_NUMBER_TYPEP(KNO_TYPEOF(x)))

KNO_EXPORT int kno_exactp(lispval x);

#define KNO_EXACTP(x)		\
  ((KNO_FIXNUMP(x)) ? (1) :	\
   (KNO_FLONUMP(x)) ?(0) :	\
   (KNO_BIGINTP(x)) ? (1) :	\
   (!(KNO_NUMBERP(x))) ? (0) :	\
   (kno_exactp(x)))

#define KNO_NUMBERP(x) (KNO_NUMBER_TYPEP(KNO_TYPEOF(x)))

/* SLOTIDs */

#define KNO_SLOTIDP(x) ( (KNO_OIDP(x)) || (KNO_SYMBOLP(x)) )

/* Generic handlers */

typedef unsigned int kno_compare_flags;
#define KNO_COMPARE_QUICK           ((kno_compare_flags)(0))
#define KNO_COMPARE_CODES           ((kno_compare_flags)(1))
#define KNO_COMPARE_RECURSIVE       ((kno_compare_flags)(2))
#define KNO_COMPARE_NATSORT         ((kno_compare_flags)(4))
#define KNO_COMPARE_SLOTS           ((kno_compare_flags)(8))
#define KNO_COMPARE_NUMERIC         ((kno_compare_flags)(16))
#define KNO_COMPARE_ALPHABETICAL    ((kno_compare_flags)(32))
#define KNO_COMPARE_CI              ((kno_compare_flags)(64))

/* Recursive but not natural or length oriented */
#define KNO_COMPARE_FULL     \
  ((KNO_COMPARE_RECURSIVE)|(KNO_COMPARE_SLOTS)|(KNO_COMPARE_CODES))
#define KNO_COMPARE_NATURAL     \
  ((KNO_COMPARE_RECURSIVE)|(KNO_COMPARE_SLOTS)|(KNO_COMPARE_NATSORT)|\
   (KNO_COMPARE_NUMERIC)|(KNO_COMPARE_ALPHABETICAL))

typedef unsigned int kno_walk_flags;
#define KNO_WALK_CONSES      ((kno_walk_flags)(0))
#define KNO_WALK_ALL         ((kno_walk_flags)(1))
#define KNO_WALK_TERMINALS   ((kno_walk_flags)(2))
#define KNO_WALK_CONTAINERS  ((kno_walk_flags)(4))
#define KNO_WALK_CONSTANTS   ((kno_walk_flags)(8))

typedef void (*kno_recycle_fn)(struct KNO_RAW_CONS *x);
typedef int (*kno_unparse_fn)(u8_output,lispval);
typedef ssize_t (*kno_dtype_fn)(struct KNO_OUTBUF *,lispval);
typedef int (*kno_compare_fn)(lispval,lispval,kno_compare_flags);
typedef lispval (*kno_copy_fn)(lispval,int);
typedef int (*kno_walker)(lispval,void *);

typedef int (*kno_walk_fn)(kno_walker,lispval,void *,kno_walk_flags,int);
typedef lispval (*kno_history_resolvefn)(lispval);

KNO_EXPORT kno_recycle_fn kno_recyclers[KNO_TYPE_MAX];
KNO_EXPORT kno_unparse_fn kno_unparsers[KNO_TYPE_MAX];
KNO_EXPORT kno_dtype_fn kno_dtype_writers[KNO_TYPE_MAX];
KNO_EXPORT kno_compare_fn kno_comparators[KNO_TYPE_MAX];
KNO_EXPORT kno_copy_fn kno_copiers[KNO_TYPE_MAX];
KNO_EXPORT kno_walk_fn kno_walkers[KNO_TYPE_MAX];

KNO_EXPORT int kno_numeric_oids;
typedef u8_string (*kno_oid_info_fn)(lispval x);
KNO_EXPORT kno_oid_info_fn _kno_oid_info;

#define kno_intcmp(x,y) ((x<y) ? (-1) : (x>y) ? (1) : (0))
KNO_EXPORT int lispval_compare(lispval x,lispval y,kno_compare_flags);
KNO_EXPORT int lispval_equal(lispval x,lispval y);
KNO_EXPORT int kno_numcompare(lispval x,lispval y);

#define KNO_EQUAL LISP_EQUAL
#define KNO_EQUALP LISP_EQUAL
#if KNO_PROFILING_ENABLED
#define LISP_EQUAL(x,y)          (lispval_equal(x,y))
#define LISP_EQUALV(x,y)         (lispval_equal(x,y))
#define LISP_COMPARE(x,y,flags)  (lispval_compare(x,y,flags))
#define KNO_QUICK_COMPARE(x,y)      (lispval_compare(x,y,KNO_COMPARE_QUICK))
#define KNO_FULL_COMPARE(x,y)       (lispval_compare(x,y,(KNO_COMPARE_FULL)))
#else
#define LISP_EQUAL(x,y) \
  ((x == y) || ((KNO_CONSP(x)) && (KNO_CONSP(y)) && (lispval_equal(x,y))))
#define LISP_EQUALV(x,y) \
  ((x == y) ? (1) :                       \
   (((KNO_FIXNUMP(x)) && (KNO_CONSP(x))) || \
    ((KNO_FIXNUMP(y)) && (KNO_CONSP(y))) || \
    ((KNO_CONSP(x)) && (KNO_CONSP(y)))) ?   \
   (lispval_equal(x,y)) :                 \
   (0)
#define LISP_COMPARE(x,y,flags)  ((x == y) ? (0) : (lispval_compare(x,y,flags)))
#define KNO_QUICK_COMPARE(x,y) \
  (((KNO_ATOMICP(x)) && (KNO_ATOMICP(y))) ? (kno_intcmp(x,y)) : \
   (lispval_compare(x,y,KNO_COMPARE_QUICK)))
#define KNO_FULL_COMPARE(x,y) \
  ((x == y) ? (0) : (LISP_COMPARE(x,y,(KNO_COMPARE_FULL))))
#endif

#define KNO_QCOMPARE(x,y) KNO_QUICK_COMPARE(x,y)
#define KNO_COMPARE(x,y) KNO_FULL_COMPARE(x,y)

KNO_EXPORT int kno_walk(kno_walker walker,lispval obj,void *walkdata,
                      kno_walk_flags flags,int depth);

KNO_EXPORT void lispval_sort(lispval *v,size_t n,kno_compare_flags flags);

/* Debugging support */

KNO_EXPORT kno_lisp_type _kno_typeof(lispval x);
KNO_EXPORT lispval _kno_debug(lispval x);

/* Pointer checking for internal debugging */

KNO_EXPORT int kno_check_immediate(lispval);

#define KNO_CHECK_ANY_PTR(x)				 \
  ((KNO_FIXNUMP(x)) ? (1) :				 \
   (KNO_OIDP(x)) ? (((x>>2)&0x3FF)<kno_n_base_oids) :	 \
   (x==0) ? (0) :					 \
   (KNO_CONSP(x)) ?					 \
   (x == KNO_NULL) ? (0) :				 \
   (((((KNO_CONS *)x)->conshead)<0xFFFFFF80) &&		 \
    (KNO_CONS_TYPEOF((KNO_CONS *)x)>3) &&			 \
    (KNO_CONS_TYPEOF((KNO_CONS *)x)<kno_next_cons_type)) : \
   (kno_check_immediate(x)))

#define KNO_CHECK_ATOMIC_PTR(x)				      \
  ((KNO_FIXNUMP(x)) ? (1) :				      \
   (KNO_OIDP(x)) ? (((x>>2)&0x3FF)<kno_n_base_oids) :	      \
   (KNO_CONSP(x)) ? ((x == KNO_NULL) ? (0) : (1)) :	      \
   (kno_check_immediate(x)))

#if KNO_FULL_CHECK_PTR
#define KNO_CHECK_PTR(p) (KNO_USUALLY(KNO_CHECK_ANY_PTR(p)))
#else
#define KNO_CHECK_PTR(p) (KNO_USUALLY(KNO_CHECK_ATOMIC_PTR(p)))
#endif

#ifdef KNO_PTR_DEBUG_LEVEL
#if (KNO_PTR_DEBUG_LEVEL>2)
#define KNO_DEBUG_BADPTRP(x) (KNO_RARELY(!(KNO_CHECK_PTR(x))))
#elif (KNO_PTR_DEBUG_LEVEL==2)
#define KNO_DEBUG_BADPTRP(x) (KNO_RARELY(!(KNO_CHECK_ATOMIC_PTR(x))))
#elif (KNO_PTR_DEBUG_LEVELv==1)
#define KNO_DEBUG_BADPTRP(x) (KNO_RARELY((x) == KNO_NULL))
#else
#define KNO_DEBUG_BADPTRP(x) (0)
#endif
#else
#define KNO_DEBUG_BADPTRP(x) (0)
#endif

KNO_EXPORT void _kno_bad_pointer(lispval,u8_context);


#endif /* ndef KNO_PTR_H */

