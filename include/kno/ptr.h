/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.

   This file implements the basic structures, macros, and function
   prototypes for dealing with dtype pointers as implemented for the
   FramerC library
*/

/* THE ANATOMY OF A POINTER

   The FramerC library is built on a dynamically typed "lisp-like" data
   model which uses an internal pointer format designed for efficency in
   the normal cases and easy extensibility to more advanced cases.

   A dtype pointer is an unsigned int the same size as a memory
   pointer.  The lower two bits of the int represent one of four
   *manifest type codes*.  These are CONS (0), IMMEDIATE (1), FIXNUM
   (2), and OID (3).  These are all referred to the "base type" of a
   pointer.  In essense, CONSes point to structures in memory,
   IMMEDIATEs represent constant persistent values, FIXNUMs represent
   low magnitude signed integers, and OIDs are compressed object
   references into 64-bit object address space.

   Fixing the CONS type at zero (0), together with structure
   alignment on 4-byte word boundaries, enables dtype pointers to be
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
   For positive numbers, converting between C ints dtype fixnums is
   simply a matter of multiply or diving by 4 and adding the type
   code (when converting to DTYPEs).  For negative numbers, it is
   trickier but not much, as in KNO_INT2DTYPE and KNO_DTYPE2INT below.
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

   A single unified space of type codes (kno_ptr_type) combines the four
   base type codes with the 7-bit type codes associated with CONSes and
   IMMEDIATE pointers.  In this single space, CONS types start at 0x04 and
   IMMEDIATE types start at 0x84.  For example, dtype pairs, implemented as
   structures with a stored type code of 0x03, are represented in the
   unified type space by 0x07.  Symbols, which are implemented as
   CONSTANT types with a stored type value of 0x02, are represented in
   the unified space by 0x86.

   The built in components of the unified type space are represented
   by the enumeration kno_ptr_type (enum KNO_PTR_TYPE).  New immediate
   and cons types can be allocated by kno_register_immediate_type(char
   *) and kno_register_cons_type(char *).
   */

/* DTYPE Types */

#ifndef KNO_PTR_H
#define KNO_PTR_H 1
#ifndef KNO_PTR_H_INFO
#define KNO_PTR_H_INFO "include/kno/ptr.h"
#endif

#include "common.h"

#define KNO_CONS_TYPECODE(i) (0x84+i)
#define KNO_IMMEDIATE_TYPECODE(i) (0x04+i)
#define KNO_EXTENDED_TYPECODE(i) (0x100+i)
#define KNO_MAX_CONS_TYPES  0x80
#define KNO_MAX_CONS_TYPE   KNO_CONS_TYPECODE(0x80)
#define KNO_MAX_IMMEDIATE_TYPES 0x80
#define KNO_MAX_IMMEDIATE_TYPE KNO_IMMEDIATE_TYPECODE(0x80)

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

KNO_EXPORT u8_condition kno_BadPtr;

#define KNO_PTR_WIDTH ((SIZEOF_VOID_P)*8)

#if SIZEOF_INT == SIZEOF_VOID_P
typedef unsigned int _kno_ptrbits;
typedef int _kno_sptr;
#elif SIZEOF_LONG == SIZEOF_VOID_P
typedef long _kno_sptr;
#elif SIZEOF_LONG_LONG == SIZEOF_VOID_P
typedef long long _kno_sptr;
#else
typedef int _kno_sptr;
#endif

/* Basic types */

typedef enum KNO_PTR_TYPE {
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
  kno_type_type = KNO_IMMEDIATE_TYPECODE(6),
  kno_coderef_type = KNO_IMMEDIATE_TYPECODE(7),
  kno_pool_type = KNO_IMMEDIATE_TYPECODE(8),
  kno_index_type = KNO_IMMEDIATE_TYPECODE(9),
  kno_histref_type = KNO_IMMEDIATE_TYPECODE(10),

  kno_string_type = KNO_CONS_TYPECODE(0),
  kno_packet_type = KNO_CONS_TYPECODE(1),
  kno_secret_type = KNO_CONS_TYPECODE(2),
  kno_bigint_type = KNO_CONS_TYPECODE(3),
  kno_pair_type = KNO_CONS_TYPECODE(4),

  kno_compound_type = KNO_CONS_TYPECODE(5),
  kno_choice_type = KNO_CONS_TYPECODE(6),
  kno_prechoice_type = KNO_CONS_TYPECODE(7),
  kno_qchoice_type = KNO_CONS_TYPECODE(8),
  kno_vector_type = KNO_CONS_TYPECODE(9),
  kno_matrix_type = KNO_CONS_TYPECODE(10), /* NYI */
  kno_numeric_vector_type = KNO_CONS_TYPECODE(11),

  kno_slotmap_type = KNO_CONS_TYPECODE(12),
  kno_schemap_type = KNO_CONS_TYPECODE(13),
  kno_hashtable_type = KNO_CONS_TYPECODE(14),
  kno_hashset_type = KNO_CONS_TYPECODE(15),

  /* Evaluator/apply types, defined here to be constant */
  kno_cprim_type = KNO_CONS_TYPECODE(16),
  kno_lexenv_type = KNO_CONS_TYPECODE(17),
  kno_evalfn_type = KNO_CONS_TYPECODE(18),
  kno_macro_type = KNO_CONS_TYPECODE(19),
  kno_dtproc_type = KNO_CONS_TYPECODE(20),
  kno_stackframe_type = KNO_CONS_TYPECODE(21),
  kno_tailcall_type = KNO_CONS_TYPECODE(22),
  kno_lambda_type = KNO_CONS_TYPECODE(23),
  kno_code_type = KNO_CONS_TYPECODE(24),
  kno_ffi_type = KNO_CONS_TYPECODE(25),
  kno_exception_type = KNO_CONS_TYPECODE(26),

  kno_complex_type = KNO_CONS_TYPECODE(27),
  kno_rational_type = KNO_CONS_TYPECODE(28),
  kno_flonum_type = KNO_CONS_TYPECODE(29),

  kno_timestamp_type = KNO_CONS_TYPECODE(30),
  kno_uuid_type = KNO_CONS_TYPECODE(31),

  /* Other types, also defined here to be constant*/
  kno_mystery_type = KNO_CONS_TYPECODE(32),

  kno_port_type = KNO_CONS_TYPECODE(33),
  kno_stream_type = KNO_CONS_TYPECODE(34),

  kno_regex_type = KNO_CONS_TYPECODE(35),

  kno_consblock_type = KNO_CONS_TYPECODE(36),
  kno_rawptr_type = KNO_CONS_TYPECODE(37),

  kno_dtserver_type = KNO_CONS_TYPECODE(38),
  kno_bloom_filter_type = KNO_CONS_TYPECODE(39),

  /* Extended types */

  kno_number_type = KNO_EXTENDED_TYPECODE(1),
  kno_sequence_type = KNO_EXTENDED_TYPECODE(2),
  kno_table_type = KNO_EXTENDED_TYPECODE(3),
  kno_applicable_type = KNO_EXTENDED_TYPECODE(4)

  } kno_ptr_type;

#define KNO_BUILTIN_CONS_TYPES 40
#define KNO_BUILTIN_IMMEDIATE_TYPES 11
KNO_EXPORT unsigned int kno_next_cons_type;
KNO_EXPORT unsigned int kno_next_immediate_type;

typedef int (*kno_checkfn)(lispval);
KNO_EXPORT kno_checkfn kno_immediate_checkfns[KNO_MAX_IMMEDIATE_TYPES+4];

KNO_EXPORT int kno_register_cons_type(char *name);
KNO_EXPORT int kno_register_immediate_type(char *name,kno_checkfn fn);

KNO_EXPORT u8_string kno_type_names[KNO_TYPE_MAX];

#define kno_ptr_typename(tc) \
  ( (tc<kno_next_cons_type) ? (kno_type_names[tc]) : ((u8_string)"oddtype"))
#define kno_type2name(tc)        \
  (((tc<0)||(tc>KNO_TYPE_MAX))?  \
   ((u8_string)"oddtype"):      \
   (kno_type_names[tc]))

KNO_EXPORT lispval kno_badptr_err(lispval badx,u8_context cxt,u8_string details);

#define KNO_VALID_TYPECODEP(x)                                  \
  (KNO_EXPECT_TRUE((((int)x)>=0) &&                             \
                  (((int)x)<256) &&                            \
                  (((x<0x84)&&((x)<kno_next_immediate_type)) || \
                   ((x>=0x84)&&((x)<kno_next_cons_type)))))

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

#if KNO_INLINE_REFCOUNTS
struct KNO_REF_CONS { KNO_ATOMIC_CONSHEAD kno_consbits conshead;};
typedef struct KNO_REF_CONS *kno_ref_cons;
#define KNO_REF_CONS(x) ((struct KNO_REF_CONS *)(x))
#endif

#if 0
KNO_FASTOP U8_MAYBE_UNUSED kno_raw_cons KNO_RAW_CONS(lispval x){ return (kno_raw_cons) x;}
#else
#define KNO_RAW_CONS(x) ((kno_raw_cons)(x))
#endif

/* The bottom 7 bits of the conshead indicates the type of the cons.  The
   rest is the reference count or zero for "static" (non-reference
   counted) conses.  The 7 bit type code is converted to a real type by
   adding 0x84. */

#define KNO_CONS_TYPE_MASK (0x7f)
#define KNO_CONS_TYPE_OFF  (0x84)

#if 0
KNO_FASTOP U8_MAYBE_UNUSED kno_cons KNO_CONS_DATA(lispval x){ return (kno_cons) x;}
KNO_FASTOP U8_MAYBE_UNUSED lispval FDTYPE(lispval x){ return x;}
KNO_FASTOP U8_MAYBE_UNUSED lispval LISPVAL(lispval x){ return x;}
KNO_FASTOP U8_MAYBE_UNUSED int _KNO_ISDTYPE(lispval x){ return 1;}
#define KNO_ISDTYPE(x) (KNO_EXPECT_TRUE(_KNO_ISDTYPE(x)))
#else
#define KNO_CONS_DATA(x) ((kno_cons)(x))
#define FDTYPE(x)       ((lispval)(x))
#define LISPVAL(x)      ((lispval)(x))
#define KNO_ISDTYPE(x)   (KNO_EXPECT_TRUE(1))
#endif

/* Most of the stuff for dealing with conses is in cons.h.  The
    attribute common to all conses, is that its first field is
    a combination of a typecode and a reference count.  We include
    this here so that we can define a pointer type procedure which
    gets the cons type. */
#define LISP_CONS(ptr) ((lispval)ptr)

#define KNO_CONS_TYPE(x) \
  (( ((x)->conshead) & (KNO_CONS_TYPE_MASK) )+(KNO_CONS_TYPE_OFF))
#define KNO_CONSPTR_TYPE(x) (KNO_CONS_TYPE((kno_cons)x))

#define KNO_CONSPTR(cast,x) ((cast)((kno_cons)x))
#define kno_consptr(cast,x,typecode)                                     \
  ((KNO_EXPECT_TRUE(KNO_TYPEP(x,typecode))) ? ((cast)((kno_cons)(x))) :    \
   (((KNO_CHECK_PTR(x))?                                                 \
     (kno_seterr(kno_TypeError,kno_type_names[typecode],NULL,x),           \
      (_kno_bad_pointer(x,kno_type_names[typecode]))) :                   \
     (kno_raise(kno_BadPtr,kno_type_names[typecode],NULL,x))),             \
    ((cast)NULL)))
#define kno_xconsptr(cast,x,typecode)                                    \
  ((KNO_EXPECT_TRUE(KNO_TYPEP(x,typecode))) ? ((cast)((kno_cons)(x))) :    \
   (((KNO_CHECK_PTR(x))?                                                 \
     (kno_seterr(kno_TypeError,kno_type_names[typecode],NULL,x)):          \
     (kno_seterr(kno_BadPtr,kno_type_names[typecode],NULL,x))),            \
    ((cast)NULL)))

#define KNO_NULL ((lispval)(NULL))
#define KNO_NULLP(x) (((void *)x) == NULL)

#define LISPVAL_IMMEDIATE(tcode,serial) \
  ((lispval)(((((tcode)-0x04)&0x7F)<<25)|((serial)<<2)|kno_immediate_ptr_type))
#define KNO_GET_IMMEDIATE(x,tcode) (((LISPVAL(x))>>2)&0x7FFFFF)
#define KNO_IMMEDIATE_TYPE_FIELD(x) (((LISPVAL(x))>>25)&0x7F)
#define KNO_IMMEDIATE_TYPE(x) ((((LISPVAL(x))>>25)&0x7F)+0x4)
#define KNO_IMMEDIATE_DATA(x) ((LISPVAL(x))>>2)
#define KNO_IMM_TYPE(x) ((((LISPVAL(x))>>25)&0x7F)+0x4)
#define KNO_IMMEDIATE_MAX (1<<24)

#if KNO_PTR_TYPE_MACRO
#define KNO_PTR_TYPE(x) \
  (((KNO_PTR_MANIFEST_TYPE(LISPVAL(x)))>1) ? (KNO_PTR_MANIFEST_TYPE(x)) :  \
   ((KNO_PTR_MANIFEST_TYPE(x))==1) ? (KNO_IMMEDIATE_TYPE(x)) : \
   (x) ? (KNO_CONS_TYPE(((struct KNO_CONS *)KNO_CONS_DATA(x)))) : (-1))

#else
static kno_ptr_type KNO_PTR_TYPE(lispval x)
{
  int type_field = (KNO_PTR_MANIFEST_TYPE(x));
  switch (type_field) {
  case kno_cons_ptr_type: {
    struct KNO_CONS *cons = (struct KNO_CONS *)x;
    return KNO_CONS_TYPE(cons);}
  case kno_cons_immediate_type: return KNO_IMMEDIATE_TYPE(x);
  default: return type_field;
  }
}
#endif
#define KNO_PRIM_TYPE(x)         (KNO_PTR_TYPE(x))

#define KNO_TYPEP(ptr,type)                                                    \
  ((type >= 0x84) ? ( (KNO_CONSP(ptr)) && (KNO_CONSPTR_TYPE(ptr) == type) ) :   \
   (type >= 0x04) ? ( (KNO_IMMEDIATEP(ptr)) && (KNO_IMM_TYPE(ptr) == type ) ) : \
   ( ( (ptr) & (0x3) ) == type) )
/* Other variants */
#define KNO_CONS_TYPEP(x,type) ( (KNO_CONSP(x)) && ((KNO_CONS_TYPE(x)) == type) )
#define KNO_PTR_TYPEP(x,type)  ( (KNO_PTR_TYPE(x)) == type )
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
   This is encoded in a DTYPE pointer with the offset in the high 20 bits
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
  ((lispval) ((((kno_ptrbits)baseid)<<2)|\
             (((kno_ptrbits)offset)<<((KNO_OID_BUCKET_WIDTH)+2))|3))
KNO_EXPORT lispval kno_make_oid(KNO_OID addr);
KNO_EXPORT int kno_get_oid_base_index(KNO_OID addr,int add);

#define KNO_OID_BASE_ID(x)     (((x)>>2)&(KNO_OID_BUCKET_MASK))
#define KNO_OID_BASE_OFFSET(x) ((x)>>(KNO_OID_BUCKET_WIDTH+2))

KNO_EXPORT const int _KNO_OID_BUCKET_WIDTH;
KNO_EXPORT const int _KNO_OID_BUCKET_MASK;

KNO_EXPORT char *kno_ulonglong_to_b32(unsigned long long offset,char *buf,int *len);
KNO_EXPORT int kno_b32_to_ulonglong(const char *digits,unsigned long long *out);
KNO_EXPORT long long kno_b32_to_longlong(const char *digits);

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
#define KNO_INT2FIX(n)   ( (lispval)  ( ( ((_kno_sptr)(n)) << 2 ) | kno_fixnum_type) )

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

/* The sizeof check here avoids *tautological* range errors
   when n can't be too big for a fixnum. */
#define KNO_INT2DTYPE(n)                         \
  ( ( (sizeof(n) < KNO_FIXNUM_BYTES) ||           \
      (((long long)(n)) > KNO_MAX_FIXNUM) ||      \
      (((long long)(n)) < KNO_MIN_FIXNUM) ) ?     \
    (kno_make_bigint(n)) :                        \
    (KNO_INT2FIX(n)) )
#define KNO_INT(x) (KNO_INT2DTYPE(x))
#define KNO_MAKEINT(x) (KNO_INT2DTYPE(x))

#define KNO_UINT2DTYPE(x) \
  (((to64u(x)) > (to64(KNO_MAX_FIXNUM))) ?                       \
   (kno_make_bigint(to64u(x))) :                                 \
   ((lispval) (((to64u(x))*4)|kno_fixnum_type)))

#define KNO_SHORT2DTYPE(shrt) (KNO_INT2FIX((long long)shrt))
#define KNO_SHORT2FIX(shrt)   (KNO_INT2FIX((long long)shrt))

#define KNO_USHORT2DTYPE(x)     ((lispval)(kno_fixnum_type|((x&0xFFFF)<<2)))
#define KNO_BYTE2DTYPE(x)       ((lispval) (kno_fixnum_type|((x&0xFF)<<2)))
#define KNO_BYTE2LISP(x)        (KNO_BYTE2DTYPE(x))
#define KNO_FIXNUM_MAGNITUDE(x) ((x<0)?((-(x))>>2):(x>>2))
#define KNO_FIXNUM_NEGATIVEP(x) (x<0)

#define KNO_FIXZERO         (KNO_SHORT2DTYPE(0))
#define KNO_FIXNUM_ZERO     (KNO_SHORT2DTYPE(0))
#define KNO_FIXNUM_ONE      (KNO_SHORT2DTYPE(1))
#define KNO_FIXNUM_NEGONE   (KNO_SHORT2DTYPE(-1))

#if KNO_FIXNUM_BITS <= 30
#define KNO_INTP(x) (KNO_FIXNUMP(x))
#define KNO_UINTP(x) ((KNO_FIXNUMP(x))&&((KNO_FIX2INT(x))>=0))
#define KNO_LONGP(x) (KNO_FIXNUMP(x))
#define KNO_ULONGP(x) ((KNO_FIXNUMP(x))&&((KNO_FIX2INT(x))>=0))
#else
#define KNO_INTP(x) \
  ((KNO_FIXNUMP(x))&&(KNO_FIX2INT(x)<=INT_MAX)&&(KNO_FIX2INT(x)>=INT_MIN))
#define KNO_UINTP(x) \
  ((KNO_FIXNUMP(x))&&((KNO_INT2DTYPE(x))>=0)&&(KNO_FIX2INT(x)<=UINT_MAX))
#define KNO_LONGP(x) (KNO_FIXNUMP(x))
#define KNO_ULONGP(x) ((KNO_FIXNUMP(x))&&((KNO_FIX2INT(x))>=0))
#endif

#define KNO_BYTEP(x) \
  ((KNO_FIXNUMP(x))&&(KNO_FIX2INT(x)>=0)&&(KNO_FIX2INT(x)<0x100))
#define KNO_SHORTP(x) \
  ((KNO_FIXNUMP(x))&&(KNO_FIX2INT(x)>=SHRT_MIN)&&(KNO_FIX2INT(x)<=SHRT_MAX))
#define KNO_USHORTP(x) \
  ((KNO_FIXNUMP(x))&&(KNO_FIX2INT(x)>=0)&&(KNO_FIX2INT(x)<=USHRT_MAX))

#define KNO_POSITIVE_FIXNUMP(n) \
  ((KNO_FIXNUMP(x))&&(KNO_FIX2INT(x)>0))
#define KNO_NOTNEG_FIXNUMP(n) \
  ((KNO_FIXNUMP(x))&&(KNO_FIX2INT(x)>=0))
#define KNO_POSITIVE_FIXINTP(n) \
  ((KNO_INTP(x))&&(KNO_FIX2INT(x)>0))
#define KNO_NOTNEG_FIXINTP(n) \
  ((KNO_INTP(x))&&(KNO_FIX2INT(x)>=0))

/* Constants */

#define KNO_CONSTANT(i) ((lispval)(kno_immediate_ptr_type|(i<<2)))

#define KNO_VOID                   KNO_CONSTANT(0)
#define KNO_FALSE                  KNO_CONSTANT(1)
#define KNO_TRUE                   KNO_CONSTANT(2)
#define KNO_EMPTY_CHOICE           KNO_CONSTANT(3)
#define KNO_EMPTY_LIST             KNO_CONSTANT(4)
#define KNO_EOF                    KNO_CONSTANT(5)
#define KNO_EOD                    KNO_CONSTANT(6)
#define KNO_EOX                    KNO_CONSTANT(7)
#define KNO_DTYPE_ERROR            KNO_CONSTANT(8)
#define KNO_PARSE_ERROR            KNO_CONSTANT(9)
#define KNO_OOM                    KNO_CONSTANT(10)
#define KNO_TYPE_ERROR             KNO_CONSTANT(11)
#define KNO_RANGE_ERROR            KNO_CONSTANT(12)
#define KNO_ERROR_VALUE            KNO_CONSTANT(13)
#define KNO_BADPTR                 KNO_CONSTANT(14)
#define KNO_THROW_VALUE            KNO_CONSTANT(15)
#define KNO_BREAK                  KNO_CONSTANT(16)
#define KNO_UNBOUND                KNO_CONSTANT(17)
#define KNO_NEVERSEEN              KNO_CONSTANT(18)
#define KNO_LOCKHOLDER             KNO_CONSTANT(19)
#define KNO_DEFAULT_VALUE          KNO_CONSTANT(20)
#define KNO_UNALLOCATED_OID        KNO_CONSTANT(21)

#define KNO_N_BUILTIN_CONSTANTS 22

KNO_EXPORT const char *kno_constant_names[];
KNO_EXPORT int kno_n_constants;
KNO_EXPORT lispval kno_register_constant(u8_string name);

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

#define KNO_ABORTP(x) \
  (((KNO_TYPEP(x,kno_constant_type)) && \
    (KNO_GET_IMMEDIATE(x,kno_constant_type)>6) && \
    (KNO_GET_IMMEDIATE(x,kno_constant_type)<=16)))
#define KNO_ERRORP(x) \
  (((KNO_TYPEP(x,kno_constant_type)) && \
    (KNO_GET_IMMEDIATE(x,kno_constant_type)>6) && \
    (KNO_GET_IMMEDIATE(x,kno_constant_type)<15)))
#define KNO_TROUBLEP(x) (KNO_EXPECT_FALSE(KNO_ERRORP(x)))
#define KNO_COOLP(x) (!(KNO_TROUBLEP(x)))

#define KNO_BROKEP(x) (KNO_EXPECT_FALSE(KNO_BREAKP(x)))
#define KNO_ABORTED(x) (KNO_EXPECT_FALSE(KNO_ABORTP(x)))
#define KNO_INTERRUPTED() (KNO_EXPECT_FALSE(u8_current_exception!=(NULL)))

#define KNO_CONSTANTP(x) \
  ((KNO_IMMEDIATEP(x)) && ((KNO_IMMEDIATE_TYPE(x))==0))

#define KNO_NIL (KNO_EMPTY_LIST)

/* Characters */

#define KNO_CHARACTERP(x) \
  ((KNO_PTR_MANIFEST_TYPE(x) == kno_immediate_type) && \
   (KNO_IMMEDIATE_TYPE(x) == kno_character_type))
#define KNO_CODE2CHAR(x) (LISPVAL_IMMEDIATE(kno_character_type,x))
#define KNO_CHARCODE(x) (KNO_GET_IMMEDIATE(x,kno_character_type))
#define KNO_CHAR2CODE(x) (KNO_GET_IMMEDIATE(x,kno_character_type))

/* CODEREFS */

#define KNO_CODEREFP(x) \
  ((KNO_PTR_MANIFEST_TYPE(x) == kno_immediate_ptr_type) && \
   (KNO_IMMEDIATE_TYPE(x) == kno_coderef_type))
#define KNO_DECODEREF(cref) (KNO_IMMEDIATE_DATA(cref))
#define KNO_ENCODEREF(off) (LISPVAL_IMMEDIATE((off),kno_coderef_type))

/* Lexrefs */

#define KNO_LEXREFP(x) (KNO_TYPEP(x,kno_lexref_type))
#define KNO_LEXREF_UP(x) ((KNO_GET_IMMEDIATE((x),kno_lexref_type))/32)
#define KNO_LEXREF_ACROSS(x) ((KNO_GET_IMMEDIATE((x),kno_lexref_type))%32)

/* Opcodes */

/* These are used by the evaluator.  We define the immediate type here,
   rather than dyanmically, so that they can be compile time constants
   and dispatch very quickly. */

#define KNO_OPCODEP(x) \
  ((KNO_PTR_MANIFEST_TYPE(x) == kno_immediate_ptr_type) && \
   (KNO_IMMEDIATE_TYPE(x) == kno_opcode_type))

#define KNO_OPCODE(num) (LISPVAL_IMMEDIATE(kno_opcode_type,num))
#define KNO_OPCODE_NUM(op) (KNO_GET_IMMEDIATE(op,kno_opcode_type))
#define KNO_NEXT_OPCODE(op) \
  (LISPVAL_IMMEDIATE(kno_opcode_type,(1+(KNO_OPCODE_NUM(op)))))

/* Symbols */

#ifndef KNO_CORE_SYMBOLS
#define KNO_CORE_SYMBOLS 8192
#endif
KNO_EXPORT lispval *kno_symbol_names;
KNO_EXPORT int kno_n_symbols;
KNO_EXPORT u8_rwlock kno_symbol_lock;

#define KNO_SYMBOLP(x) \
  ((KNO_PTR_MANIFEST_TYPE(x) == kno_immediate_ptr_type) && \
   (KNO_IMMEDIATE_TYPE(x) == kno_symbol_type))

#define KNO_GOOD_SYMBOLP(x) \
  ((KNO_PTR_MANIFEST_TYPE(x) == kno_immediate_ptr_type) && \
   (KNO_IMMEDIATE_TYPE(x) == kno_symbol_type) && \
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
KNO_EXPORT lispval kno_intern(u8_string string);
KNO_EXPORT lispval kno_symbolize(u8_string string);
KNO_EXPORT lispval kno_all_symbols(void);

KNO_EXPORT int kno_flipcase_fix;

KNO_EXPORT lispval FDSYM_ADD, FDSYM_ADJUNCT, FDSYM_ALL, FDSYM_ALWAYS;
KNO_EXPORT lispval FDSYM_BLOCKSIZE, FDSYM_BUFSIZE;
KNO_EXPORT lispval FDSYM_CACHELEVEL, FDSYM_CACHESIZE;
KNO_EXPORT lispval FDSYM_CONS, FDSYM_CONTENT, FDSYM_CREATE;
KNO_EXPORT lispval FDSYM_DEFAULT, FDSYM_DOT, FDSYM_DROP;
KNO_EXPORT lispval FDSYM_ENCODING, FDSYM_EQUALS, FDSYM_ERROR;
KNO_EXPORT lispval FDSYM_FILE, FDSYM_FILENAME;
KNO_EXPORT lispval FDSYM_FLAGS, FDSYM_FORMAT, FDSYM_FRONT, FDSYM_INPUT;
KNO_EXPORT lispval FDSYM_ISADJUNCT, FDSYM_KEYSLOT;
KNO_EXPORT lispval FDSYM_LABEL, FDSYM_LAZY, FDSYM_LENGTH, FDSYM_LOGLEVEL;
KNO_EXPORT lispval FDSYM_MAIN, FDSYM_MERGE, FDSYM_METADATA;
KNO_EXPORT lispval FDSYM_MINUS, FDSYM_MODULE;
KNO_EXPORT lispval FDSYM_NAME, FDSYM_NO, FDSYM_NONE, FDSYM_NOT;
KNO_EXPORT lispval FDSYM_OPT, FDSYM_OPTS, FDSYM_OUTPUT;
KNO_EXPORT lispval FDSYM_PCTID, FDSYM_PLUS, FDSYM_PREFIX, FDSYM_PROPS;
KNO_EXPORT lispval FDSYM_QMARK, FDSYM_QUOTE, FDSYM_READONLY, FDSYM_SEP;
KNO_EXPORT lispval FDSYM_SET, FDSYM_SIZE, FDSYM_SORT, FDSYM_SORTED;
KNO_EXPORT lispval FDSYM_SOURCE, FDSYM_STAR, FDSYM_STRING, FDSYM_SUFFIX;
KNO_EXPORT lispval FDSYM_TAG, FDSYM_TEST, FDSYM_TEXT, FDSYM_TYPE;
KNO_EXPORT lispval FDSYM_STORE;
KNO_EXPORT lispval FDSYM_VERSION, FDSYM_VOID;

/* Function IDs */

/* Function IDs are immediate values which refer to
   conses stored in a persistent table.  The idea is that
   persistent pointers are not subject to GC, so they can
   be passed much more quickly and without thread contention. */

#define KNO_FCNIDP(x) \
  ((KNO_PTR_MANIFEST_TYPE(x) == kno_immediate_ptr_type) && \
   (KNO_IMMEDIATE_TYPE(x) == kno_fcnid_type))

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

KNO_EXPORT lispval kno_err(u8_condition,u8_context,u8_string,lispval);

KNO_EXPORT u8_condition kno_InvalidFCNID, kno_FCNIDOverflow;

#if KNO_INLINE_FCNIDS
static U8_MAYBE_UNUSED lispval _kno_fcnid_ref(lispval ref)
{
  if (KNO_TYPEP(ref,kno_fcnid_type)) {
    int serialno = KNO_GET_IMMEDIATE(ref,kno_fcnid_type);
    if (KNO_EXPECT_FALSE(serialno>_kno_fcnid_count))
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
#define KNO_FCNID_TYPE(x)        (KNO_PTR_TYPE(kno_fcnid_ref(x)))

/* Numeric macros */

#define KNO_NUMBER_TYPEP(x) \
  ((x == kno_fixnum_type) || (x == kno_bigint_type) ||   \
   (x == kno_flonum_type) || (x == kno_rational_type) || \
   (x == kno_complex_type))
#define KNO_NUMBERP(x) (KNO_NUMBER_TYPEP(KNO_PTR_TYPE(x)))

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

/* KNO_SOURCE aliases */

#if KNO_SOURCE
#define VOID       (KNO_VOID)
#define VOIDP(x)   (KNO_VOIDP(x))
#define EMPTY      (KNO_EMPTY_CHOICE)
#define EMPTYP(x)  (KNO_EMPTY_CHOICEP(x))
#define EXISTSP(x) (! (KNO_EMPTY_CHOICEP(x)) )
#define NIL        (KNO_EMPTY_LIST)
#define NILP(x)    (KNO_EMPTY_LISTP(x))
#define CONSP(x)   (KNO_CONSP(x))
#define ATOMICP(x) (KNO_ATOMICP(x))
#define TYPEP(o,t) (KNO_TYPEP((o),(t)))
#define CHOICEP(x) (KNO_CHOICEP(x))
#define FIXNUMP(x) (KNO_FIXNUMP(x))
#define NUMBERP(x) (KNO_NUMBERP(x))
#define TABLEP(x)  (KNO_TABLEP(x))
#define PAIRP(x)   (KNO_PAIRP(x))
#define VECTORP(x) (KNO_VECTORP(x))
#define SYMBOLP(x) (KNO_SYMBOLP(x))
#define STRINGP(x) (KNO_STRINGP(x))
#define PACKETP(x) (KNO_PACKETP(x))
#define FALSEP(x)  (KNO_FALSEP(x))
#define OIDP(x)    (KNO_OIDP(x))
#define FIX2INT(x) (KNO_FIX2INT(x))
#define DO_CHOICES KNO_DO_CHOICES
#define DOLIST     KNO_DOLIST
#define CHOICE_ADD KNO_ADD_TO_CHOICE
#define EQ         KNO_EQ
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
#define HASHTABLEP(x) (KNO_HASHTABLEP(x))
#define PRED_FALSE(x)  (KNO_EXPECT_FALSE(x))
#define PRED_TRUE(x)  (KNO_EXPECT_TRUE(x))
#define SYMBOL_NAME(x) (KNO_SYMBOL_NAME(x))
#define COMPOUND_VECTORP(x) (KNO_COMPOUND_VECTORP(x))
#define COMPOUND_VECLEN(x)  (KNO_COMPOUND_VECLEN(x))
#define COMPOUND_VECELTS(x)  (KNO_COMPOUND_VECELTS(x))
#define XCOMPOUND_VEC_REF(x,i) (KNO_XCOMPOUND_VECREF((x),(i)))
#endif

/* Debugging support */

KNO_EXPORT kno_ptr_type _kno_ptr_type(lispval x);
KNO_EXPORT lispval _kno_debug(lispval x);

/* Pointer checking for internal debugging */

KNO_EXPORT int kno_check_immediate(lispval);

#define KNO_CHECK_CONS_PTR(x)                          \
 ((KNO_ISDTYPE(x))&&                                   \
  ((KNO_FIXNUMP(x)) ? (1) :                          \
   (KNO_OIDP(x)) ? (((x>>2)&0x3FF)<kno_n_base_oids) : \
   (x==0) ? (0) :                                   \
   (KNO_CONSP(x)) ?                                  \
   (((((KNO_CONS *)x)->conshead)<0xFFFFFF80) &&      \
    (KNO_CONS_TYPE((KNO_CONS *)x)>3) &&               \
    (KNO_CONS_TYPE((KNO_CONS *)x)<kno_next_cons_type)) : \
   (kno_check_immediate(x))))

#define KNO_CHECK_ATOMIC_PTR(x)                     \
 ((KNO_ISDTYPE(x))&&                                  \
  ((KNO_FIXNUMP(x)) ? (1) :                          \
   (KNO_OIDP(x)) ? (((x>>2)&0x3FF)<kno_n_base_oids) : \
   (x==0) ? (0) :                                   \
   (KNO_CONSP(x)) ? ((x == KNO_NULL) ? (0) : (1)) :           \
   (kno_check_immediate(x))))

#if KNO_FULL_CHECK_PTR
#define KNO_CHECK_PTR(p) (KNO_EXPECT_TRUE(KNO_CHECK_CONS_PTR(p)))
#else
#define KNO_CHECK_PTR(p) (KNO_EXPECT_TRUE(KNO_CHECK_ATOMIC_PTR(p)))
#endif

#ifdef KNO_PTR_DEBUG_LEVEL
#if (KNO_PTR_DEBUG_LEVEL>2)
#define KNO_DEBUG_BADPTRP(x) (KNO_EXPECT_FALSE(!(KNO_CHECK_PTR(x))))
#elif (KNO_PTR_DEBUG_LEVEL==2)
#define KNO_DEBUG_BADPTRP(x) (KNO_EXPECT_FALSE(!(KNO_CHECK_ATOMIC_PTR(x))))
#elif (KNO_PTR_DEBUG_LEVEL==1)
#define KNO_DEBUG_BADPTRP(x) (KNO_EXPECT_FALSE((x) == KNO_NULL))
#else
#define KNO_DEBUG_BADPTRP(x) (0)
#endif
#else
#define KNO_DEBUG_BADPTRP(x) (0)
#endif

/* These constitue a simple API for including ptr checks which
   turn into noops depending on the debug level, which
   can be specified in different ways.  */

/* KNO_CHECK_PTRn is the return value call,
   KNO_PTR_CHECKn is the side-effect call.

   KNO_PTR_DEBUG_DENSITY enables the numbered versions
    of each call.  If it's negative, the runtime
    variable kno_ptr_debug_level is used instead.

   Each higher level includes the levels below it.

   As a rule of thumb, currently, the levels should be used as follows:
    1: topical debugging for places you think something odd is happening;
        the system code should almost never use this level
    2: assignment debugging (called when a pointer is stored)
    3: return value debugging (called when a pointer is returned)

   Setting a breakpoint at _kno_bad_pointer is a good idea.
*/

KNO_EXPORT void _kno_bad_pointer(lispval,u8_context);

static U8_MAYBE_UNUSED lispval _kno_check_ptr(lispval x,u8_context cxt) {
  if (KNO_DEBUG_BADPTRP(x)) _kno_bad_pointer(x,cxt);
  return x;
}

KNO_EXPORT int kno_ptr_debug_density;

#if (KNO_PTR_DEBUG_DENSITY<0)
/* When the check level is negative, use a runtime variable. */
#define KNO_CHECK_PTR1(x,cxt) ((kno_ptr_debug_density>0) ? (_kno_check_ptr(x,((u8_context)cxt))) : (x))
#define KNO_PTR_CHECK1(x,cxt)                                            \
  {if ((kno_ptr_debug_density>0) && (KNO_DEBUG_BADPTRP(x))) _kno_bad_pointer(x,((u8_context)cxt));}
#define KNO_CHECK_PTR2(x,cxt) ((kno_ptr_debug_density>1) ? (_kno_check_ptr(x,((u8_context)cxt))) : (x))
#define KNO_PTR_CHECK2(x,cxt)                                            \
  {if ((kno_ptr_debug_density>1) && (KNO_DEBUG_BADPTRP(x))) _kno_bad_pointer(x,((u8_context)cxt));}
#define KNO_CHECK_PTR3(x,cxt) ((kno_ptr_debug_density>2) ? (_kno_check_ptr(x,((u8_context)cxt))) : (x))
#define KNO_PTR_CHECK3(x,cxt)                                            \
  {if ((kno_ptr_debug_density>2) && (KNO_DEBUG_BADPTRP(x))) _kno_bad_pointer(x,((u8_context)cxt));}
#else
#if (KNO_PTR_DEBUG_DENSITY > 0)
#define KNO_CHECK_PTR1(x,cxt) (_kno_check_ptr((x),((u8_context)cxt)))
#define KNO_PTR_CHECK1(x,cxt)                    \
  {if (KNO_DEBUG_BADPTRP(x)) _kno_bad_pointer(x,((u8_context)cxt));}
#endif
#if (KNO_PTR_DEBUG_DENSITY > 1)
#define KNO_CHECK_PTR2(x,cxt) (_kno_check_ptr((x),((u8_context)cxt)))
#define KNO_PTR_CHECK2(x,cxt)                    \
  {if (KNO_DEBUG_BADPTRP(x)) _kno_bad_pointer(x,((u8_context)cxt));}
#endif
#if (KNO_PTR_DEBUG_DENSITY > 2)
#define KNO_CHECK_PTR3(x,cxt) (_kno_check_ptr((x),((u8_context)cxt)))
#define KNO_PTR_CHECK3(x,cxt)                    \
  {if (KNO_DEBUG_BADPTRP(x)) _kno_bad_pointer(x,((u8_context)cxt));}
#endif
#endif

/* Now define any undefined operations. */
#ifndef KNO_PTR_CHECK1
#define KNO_PTR_CHECK1(x,cxt)
#endif
#ifndef KNO_PTR_CHECK2
#define KNO_PTR_CHECK2(x,cxt)
#endif
#ifndef KNO_PTR_CHECK3
#define KNO_PTR_CHECK3(x,cxt)
#endif

#ifndef KNO_CHECK_PTR1
#define KNO_CHECK_PTR1(x,cxt) (x)
#endif
#ifndef KNO_CHECK_PTR2
#define KNO_CHECK_PTR2(x,cxt) (x)
#endif
#ifndef KNO_CHECK_PTR3
#define KNO_CHECK_PTR3(x,cxt) (x)
#endif

#endif /* ndef KNO_PTR_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
