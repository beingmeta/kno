/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
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
    <include/framerd/cons.h>, but it is basically a block of memory
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
    trickier but not much, as in FD_INT2DTYPE and FD_DTYPE2INT below.
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

    A single unified space of type codes (fd_ptr_type) combines the four
    base type codes with the 7-bit type codes associated with CONSes and
    IMMEDIATE pointers.  In this single space, CONS types start at 0x04 and
    IMMEDIATE types start at 0x84.  For example, dtype pairs, implemented as
    structures with a stored type code of 0x03, are represented in the
    unified type space by 0x07.  Symbols, which are implemented as
    CONSTANT types with a stored type value of 0x02, are represented in
    the unified space by 0x86.

    The built in components of the unified type space are represented
    by the enumeration fd_ptr_type (enum FD_PTR_TYPE).  New immediate
    and cons types can be allocated by fd_register_immediate_type(char
    *) and fd_register_cons_type(char *).
*/

/* DTYPE Types */

#ifndef FRAMERD_PTR_H
#define FRAMERD_PTR_H 1
#ifndef FRAMERD_PTR_H_INFO
#define FRAMERD_PTR_H_INFO "include/framerd/ptr.h"
#endif

#include "common.h"

/* 0 and 1 are reserved for OIDs and fixnums.
   CONS types start at 0x84 and go until 0x104, which
    is the limit to what we can store together with
    the reference count in fd_consbit
   IMMEDIATE types start after that
*/

#define FD_CONS_TYPECODE(i) (0x84+i)
#define FD_IMMEDIATE_TYPECODE(i) (0x04+i)
#define FD_MAX_CONS_TYPES  0x80
#define FD_MAX_CONS_TYPE   FD_CONS_TYPECODE(0x80)
#define FD_MAX_IMMEDIATE_TYPES 0x80
#define FD_MAX_IMMEDIATE_TYPE FD_IMMEDIATE_TYPECODE(0x80)

FD_EXPORT fd_exception fd_BadPtr;

#define fd_cons_ptr_type (0)
#define fd_immediate_ptr_type (1)
#define fd_fixnum_ptr_type (2)
#define fd_oid_ptr_type (3)

typedef enum FD_PTR_TYPE {
  fd_cons_type = 0, fd_immediate_type = 1,
  fd_fixnum_type = 2, fd_oid_type = 3,

  fd_constant_type = FD_IMMEDIATE_TYPECODE(0),
  fd_character_type = FD_IMMEDIATE_TYPECODE(1),
  fd_symbol_type = FD_IMMEDIATE_TYPECODE(2),
  /* Reserved as constants */
  fd_fcnid_type = FD_IMMEDIATE_TYPECODE(3),
  fd_lexref_type = FD_IMMEDIATE_TYPECODE(4),
  fd_opcode_type = FD_IMMEDIATE_TYPECODE(5),
  fd_type_type = FD_IMMEDIATE_TYPECODE(6),
  fd_cdrcode_type = FD_IMMEDIATE_TYPECODE(7),

  fd_string_type = FD_CONS_TYPECODE(0),
  fd_packet_type = FD_CONS_TYPECODE(1),
  fd_secret_type = FD_CONS_TYPECODE(2),
  fd_bigint_type = FD_CONS_TYPECODE(3),
  fd_pair_type = FD_CONS_TYPECODE(4),

  fd_compound_type = FD_CONS_TYPECODE(5),
  fd_choice_type = FD_CONS_TYPECODE(6),
  fd_prechoice_type = FD_CONS_TYPECODE(7),
  fd_qchoice_type = FD_CONS_TYPECODE(8),
  fd_vector_type = FD_CONS_TYPECODE(9),
  fd_rail_type = FD_CONS_TYPECODE(10),
  fd_numeric_vector_type = FD_CONS_TYPECODE(11),

  fd_slotmap_type = FD_CONS_TYPECODE(12),
  fd_schemap_type = FD_CONS_TYPECODE(13),
  fd_hashtable_type = FD_CONS_TYPECODE(14),
  fd_hashset_type = FD_CONS_TYPECODE(15),

  /* Evaluator/apply types, defined here to be constant */
  fd_primfcn_type = FD_CONS_TYPECODE(16),
  fd_environment_type = FD_CONS_TYPECODE(17),
  fd_specform_type = FD_CONS_TYPECODE(18),
  fd_macro_type = FD_CONS_TYPECODE(19),
  fd_dtproc_type = FD_CONS_TYPECODE(20),
  fd_tailcall_type = FD_CONS_TYPECODE(21),
  fd_sproc_type = FD_CONS_TYPECODE(22),
  fd_bytecode_type = FD_CONS_TYPECODE(23),
  fd_stackframe_type = FD_CONS_TYPECODE(24),
  fd_ffi_type = FD_CONS_TYPECODE(25),
  fd_error_type = FD_CONS_TYPECODE(26),

  fd_complex_type = FD_CONS_TYPECODE(27),
  fd_rational_type = FD_CONS_TYPECODE(28),
  fd_flonum_type = FD_CONS_TYPECODE(29),
  fd_timestamp_type = FD_CONS_TYPECODE(30),
  fd_uuid_type = FD_CONS_TYPECODE(31),

  /* Other types, also defined here to be constant*/
  fd_mystery_type = FD_CONS_TYPECODE(32),
  fd_port_type = FD_CONS_TYPECODE(33),
  fd_stream_type = FD_CONS_TYPECODE(34),
  fd_regex_type = FD_CONS_TYPECODE(35),
  fd_consblock_type = FD_CONS_TYPECODE(36),
  fd_rawptr_type = FD_CONS_TYPECODE(37),
  fd_dtserver_type = FD_CONS_TYPECODE(38),
  fd_bloom_filter_type = FD_CONS_TYPECODE(39)

  } fd_ptr_type;

#define FD_BUILTIN_CONS_TYPES 40
#define FD_BUILTIN_IMMEDIATE_TYPES 8
FD_EXPORT unsigned int fd_next_cons_type;
FD_EXPORT unsigned int fd_next_immediate_type;

typedef int (*fd_checkfn)(fdtype);
FD_EXPORT fd_checkfn fd_immediate_checkfns[FD_MAX_IMMEDIATE_TYPES+4];

FD_EXPORT int fd_register_cons_type(char *name);
FD_EXPORT int fd_register_immediate_type(char *name,fd_checkfn fn);

FD_EXPORT u8_string fd_type_names[FD_TYPE_MAX];

#define fd_ptr_typename(tc) \
  ( (tc<fd_next_cons_type) ? (fd_type_names[tc]) : ((u8_string)"oddtype"))
#define fd_type2name(tc)	\
  (((tc<0)||(tc>FD_TYPE_MAX))?	\
   ((u8_string)"oddtype"):	\
   (fd_type_names[tc]))

FD_EXPORT fdtype fd_badptr_err(fdtype badx,u8_context cxt,u8_string details);

#define FD_VALID_TYPECODEP(x)				       \
  (FD_EXPECT_TRUE((((int)x)>=0) &&			       \
		  (((int)x)<256) &&			       \
		  (((x<0x84)&&((x)<fd_next_immediate_type)) || \
		   ((x>=0x84)&&((x)<fd_next_cons_type)))))

/* In the type field, 0 means an integer, 1 means an oid, 2 means
   an immediate constant, and 3 means a cons. */
#define FD_PTR_MANIFEST_TYPE(x) ((x)&(0x3))
#define FD_CONSP(x) ((FD_PTR_MANIFEST_TYPE(x)) == fd_cons_ptr_type)
#define FD_ATOMICP(x) ((FD_PTR_MANIFEST_TYPE(x))!=fd_cons_ptr_type)
#define FD_FIXNUMP(x) ((FD_PTR_MANIFEST_TYPE(x)) == fd_fixnum_ptr_type)
#define FD_IMMEDIATEP(x) ((FD_PTR_MANIFEST_TYPE(x)) == fd_immediate_ptr_type)
#define FD_OIDP(x) ((FD_PTR_MANIFEST_TYPE(x)) == fd_oid_ptr_type)
#define FD_INT_DATA(x) ((x)>>2)
#define FD_EQ(x,y) ((x) == (y))

/* Basic cons structs */

typedef unsigned int fd_consbits;

#if FD_INLINE_REFCOUNTS && FD_LOCKFREE_REFCOUNTS
#define FD_ATOMIC_CONSHEAD _Atomic
#else
#define FD_ATOMIC_CONSHEAD
#endif

#define FD_CONS_HEADER const fd_consbits conshead

/* The header for typed data structures */
typedef struct FD_CONS { FD_CONS_HEADER;} FD_CONS;
typedef struct FD_CONS *fd_cons;

/* Raw conses have consbits which can change */
struct FD_RAW_CONS { fd_consbits conshead;};
typedef struct FD_RAW_CONS *fd_raw_cons;

#if FD_INLINE_REFCOUNTS
struct FD_REF_CONS { FD_ATOMIC_CONSHEAD fd_consbits conshead;};
typedef struct FD_REF_CONS *fd_ref_cons;
#define FD_REF_CONS(x) ((struct FD_REF_CONS *)(x))
#endif

#if FD_CHECKFDTYPE
FD_FASTOP U8_MAYBE_UNUSED fd_raw_cons FD_RAW_CONS(fdtype x){ return (fd_raw_cons) x;}
#else
#define FD_RAW_CONS(x) ((fd_raw_cons)(x))
#endif

/* The bottom 7 bits of the conshead indicates the type of the cons.  The
   rest is the reference count or zero for "static" (non-reference
   counted) conses.  The 7 bit type code is converted to a real type by
   adding 0x84. */

#define FD_CONS_TYPE_MASK (0x7f)
#define FD_CONS_TYPE_OFF  (0x84)

#if FD_CHECKFDTYPE
FD_FASTOP U8_MAYBE_UNUSED fd_cons FD_CONS_DATA(fdtype x){ return (fd_cons) x;}
FD_FASTOP U8_MAYBE_UNUSED fdtype FDTYPE(fdtype x){ return x;}
FD_FASTOP U8_MAYBE_UNUSED int _FD_ISDTYPE(fdtype x){ return 1;}
#define FD_ISDTYPE(x) (FD_EXPECT_TRUE(_FD_ISDTYPE(x)))
#else
#define FD_CONS_DATA(x) ((fd_cons)(x))
#define FDTYPE(x) ((fdtype)(x))
#define FD_ISDTYPE(x) (FD_EXPECT_TRUE(1))
#endif

/* Most of the stuff for dealing with conses is in cons.h.  The
    attribute common to all conses, is that its first field is
    a combination of a typecode and a reference count.  We include
    this here so that we can define a pointer type procedure which
    gets the cons type. */
#define FDTYPE_CONS(ptr) ((fdtype)ptr)

#define FD_CONS_TYPE(x) \
  (( ((x)->conshead) & (FD_CONS_TYPE_MASK) )+(FD_CONS_TYPE_OFF))
#define FD_CONSPTR_TYPE(x) (FD_CONS_TYPE((fd_cons)x))

#define FD_CONSPTR(cast,x) ((cast)((fd_cons)x))
#define fd_consptr(cast,x,typecode)					\
  ((FD_EXPECT_TRUE(FD_TYPEP(x,typecode))) ? ((cast)((fd_cons)(x))) :	\
   (((FD_CHECK_PTR(x))?							\
     (fd_seterr(fd_TypeError,fd_type_names[typecode],NULL,x)):		\
     (fd_seterr(fd_BadPtr,fd_type_names[typecode],NULL,x))),		\
    ((cast)NULL)))
#define fd_xconsptr(cast,x,typecode)					\
  ((FD_EXPECT_TRUE(FD_TYPEP(x,typecode))) ? ((cast)((fd_cons)(x))) :	\
   (((FD_CHECK_PTR(x))?							\
     (fd_seterr(fd_TypeError,fd_type_names[typecode],NULL,x),		\
      (_fd_bad_pointer(x,fd_type_names[typecode]))) :			\
     (fd_seterr(fd_BadPtr,fd_type_names[typecode],NULL,x),		\
      u8_raise(fd_TypeError,fd_type_names[typecode],NULL))),		\
    ((cast)NULL)))

#define FD_NULL ((fdtype)(NULL))
#define FD_NULLP(x) (((void *)x) == NULL)

#define FDTYPE_IMMEDIATE(tcode,serial) \
  ((fdtype)(((((tcode)-0x04)&0x7F)<<25)|((serial)<<2)|fd_immediate_ptr_type))
#define FD_GET_IMMEDIATE(x,tcode) (((FDTYPE(x))>>2)&0x7FFFFF)
#define FD_IMMEDIATE_TYPE_FIELD(x) (((FDTYPE(x))>>25)&0x7F)
#define FD_IMMEDIATE_TYPE(x) ((((FDTYPE(x))>>25)&0x7F)+0x4)
#define FD_IMMEDIATE_DATA(x) ((FDTYPE(x))>>2)
#define FD_IMM_TYPE(x) ((((FDTYPE(x))>>25)&0x7F)+0x4)

#if FD_PTR_TYPE_MACRO
#define FD_PTR_TYPE(x) \
  (((FD_PTR_MANIFEST_TYPE(FDTYPE(x)))>1) ? (FD_PTR_MANIFEST_TYPE(x)) :  \
   ((FD_PTR_MANIFEST_TYPE(x))==1) ? (FD_IMMEDIATE_TYPE(x)) : \
   (x) ? (FD_CONS_TYPE(((struct FD_CONS *)FD_CONS_DATA(x)))) : (-1))

#else
static fd_ptr_type FD_PTR_TYPE(fdtype x)
{
  int type_field = (FD_PTR_MANIFEST_TYPE(x));
  switch (type_field) {
  case fd_cons_ptr_type: {
    struct FD_CONS *cons = (struct FD_CONS *)x;
    return FD_CONS_TYPE(cons);}
  case fd_cons_immediate_type: return FD_IMMEDIATE_TYPE(x);
  default: return type_field;
  }
}
#endif
#define FD_PRIM_TYPE(x)         (FD_PTR_TYPE(x))

#define FD_TYPEP(ptr,type)						      \
  ((type >= 0x84) ? ( (FD_CONSP(ptr)) && (FD_CONSPTR_TYPE(ptr) == type) ) :   \
   (type >= 0x04) ? ( (FD_IMMEDIATEP(ptr)) && (FD_IMM_TYPE(ptr) == type ) ) : \
   ( ( (ptr) & (0x3) ) == type) )
/* Other variants */
#define FD_CONS_TYPEP(x,type) ( (FD_CONSP(x)) && ((FD_CONS_TYPE(x)) == type) )
#define FD_PTR_TYPEP(x,type) ((FD_PTR_TYPE(x)) == type)
#define FD_PRIM_TYPEP(x,tp)     (FD_TYPEP(x,tp))

#define FD_MAKE_STATIC(ptr) \
  if (FD_CONSP(ptr))							\
    (((struct FD_RAW_CONS *)ptr)->conshead) &= (FD_CONS_TYPE_MASK);	\
  else {}

#define FD_MAKE_CONS_STATIC(ptr)  \
  ((struct FD_RAW_CONS *)ptr)->conshead  &=  FD_CONS_TYPE_MASK

/* OIDs */

#if FD_STRUCT_OIDS
typedef struct FD_OID {
  unsigned int fd_oid_hi, fd_oid_lo;} FD_OID;
typedef struct FD_OID *fd_oid;
#define FD_OID_HI(x) x.fd_oid_hi
#define FD_OID_LO(x) x.fd_oid_lo
FD_FASTOP FD_OID FD_OID_PLUS(FD_OID x,unsigned int increment)
{
  x.fd_oid_lo = x.fd_oid_lo+increment;
  return x;
}
#define FD_SET_OID_HI(oid,hiw) oid.fd_oid_hi = hiw
#define FD_SET_OID_LO(oid,low) oid.fd_oid_lo = low
#define FD_OID_COMPARE(oid1,oid2) \
  ((oid1.fd_oid_hi == oid2.fd_oid_hi) ? \
   ((oid1.fd_oid_lo == oid2.fd_oid_lo) ? (0) :			\
    (oid1.fd_oid_lo > oid2.fd_oid_lo) ? (1) : (-1)) :		\
  (oid1.fd_oid_hi > oid2.fd_oid_hi) ? (1) : (-1))
#define FD_OID_DIFFERENCE(oid1,oid2) \
  ((oid1.fd_oid_lo>oid2.fd_oid_lo) ? \
   (oid1.fd_oid_lo-oid2.fd_oid_lo) : \
   (oid2.fd_oid_lo-oid1.fd_oid_lo))
#elif FD_INT_OIDS
typedef unsigned int FD_OID;
#define FD_OID_HI(x) ((unsigned int)((x)>>32))
#define FD_OID_LO(x) ((unsigned int)((x)&(0xFFFFFFFFU)))
#define FD_OID_PLUS(oid,increment) (oid+increment)
#define FD_SET_OID_HI(oid,hi) oid = (oid&0xFFFFFFFFUL)|((hi)<<32)
#define FD_SET_OID_LO(oid,lo) \
  oid = ((oid&0xFFFFFFFF00000000U)|(((FD_OID)lo)&0xFFFFFFFFU))
#define FD_OID_COMPARE(oid1,oid2) \
  ((oid1 == oid2) ? (0) : (oid1<oid2) ? (-1) : (1))
#define FD_OID_DIFFERENCE(oid1,oid2) \
  ((oid1>oid2) ? (oid1-oid2) : (oid2-oid1))
#elif FD_LONG_OIDS
typedef unsigned long FD_OID;
#define FD_OID_HI(x) ((unsigned int)((x)>>32))
#define FD_OID_LO(x) ((unsigned int)((x)&(0xFFFFFFFFU)))
#define FD_OID_PLUS(oid,increment) (oid+increment)
#define FD_SET_OID_HI(oid,hi) \
  oid = ((FD_OID)(oid&0xFFFFFFFFU))|(((FD_OID)hi)<<32)
#define FD_SET_OID_LO(oid,lo) \
  oid = (oid&0xFFFFFFFF00000000UL)|((lo)&0xFFFFFFFFUL)
#define FD_OID_COMPARE(oid1,oid2) \
  ((oid1 == oid2) ? (0) : (oid1<oid2) ? (-1) : (1))
#define FD_OID_DIFFERENCE(oid1,oid2) \
  ((oid1>oid2) ? (oid1-oid2) : (oid2-oid1))
#elif FD_LONG_LONG_OIDS
typedef unsigned long long FD_OID;
#define FD_OID_HI(x) ((unsigned int)((x)>>32))
#define FD_OID_LO(x) ((unsigned int)((x)&(0xFFFFFFFFULL)))
#define FD_OID_PLUS(oid,increment) (oid+increment)
#define FD_SET_OID_HI(oid,hi) \
  oid = ((FD_OID)(oid&0xFFFFFFFFU))|(((FD_OID)hi)<<32)
#define FD_SET_OID_LO(oid,lo) \
  oid = ((oid&(0xFFFFFFFF00000000ULL))|((unsigned int)((lo)&(0xFFFFFFFFULL))))
#define FD_OID_COMPARE(oid1,oid2) \
  ((oid1 == oid2) ? (0) : (oid1>oid2) ? (1) : (-1))
#define FD_OID_DIFFERENCE(oid1,oid2) \
  ((oid1>oid2) ? (oid1-oid2) : (oid2-oid1))
#endif

#if FD_STRUCT_OIDS
FD_FASTOP FD_OID FD_MAKE_OID(unsigned int hi,unsigned int lo)
{
  FD_OID result; result.fd_oid_hi = hi; result.fd_oid_lo = lo;
  return result;
}
#define FD_NULL_OID_INIT {0,0}
#else
FD_FASTOP FD_OID FD_MAKE_OID(unsigned int hi,unsigned int lo)
{
  return ((((FD_OID)hi)<<32)|((FD_OID)lo));
}
#define FD_NULL_OID_INIT 0
#endif

#if SIZEOF_VOID_P == 4
#define FD_OID_BUCKET_WIDTH 10 /* (1024 buckets) */
#else
#define FD_OID_BUCKET_WIDTH 16 /* (65536 buckets) */
#endif

#define FD_OID_OFFSET_WIDTH 20
#define FD_OID_BUCKET_SIZE  (1<<FD_OID_OFFSET_WIDTH)
#define FD_OID_OFFSET_MASK  ((FD_OID_BUCKET_SIZE)-1)

#define FD_OID_BUCKET_MASK ((1<<(FD_OID_BUCKET_WIDTH))-1)
#define FD_N_OID_BUCKETS   (1<<(2+FD_OID_BUCKET_WIDTH))

/* An OID references has 10 bits of base index and 20 bits of offset.
   This is encoded in a DTYPE pointer with the offset in the high 20 bits
   and the base in the lower portion. */

FD_EXPORT FD_OID fd_base_oids[FD_N_OID_BUCKETS];
FD_EXPORT int fd_n_base_oids;

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
    selected for k (FD_OID_BUCKET_WIDTH).

*/
#define FD_OID_ADDR(x) \
  (FD_OID_PLUS(fd_base_oids[((x>>2)&(FD_OID_BUCKET_MASK))],\
	       (x>>((FD_OID_BUCKET_WIDTH)+2))))
#define FD_CONSTRUCT_OID(baseid,offset) \
  ((fdtype) ((((fd_ptrbits)baseid)<<2)|\
	     (((fd_ptrbits)offset)<<((FD_OID_BUCKET_WIDTH)+2))|3))
FD_EXPORT fdtype fd_make_oid(FD_OID addr);
FD_EXPORT int fd_get_oid_base_index(FD_OID addr,int add);

#define FD_OID_BASE_ID(x)     (((x)>>2)&(FD_OID_BUCKET_MASK))
#define FD_OID_BASE_OFFSET(x) ((x)>>(FD_OID_BUCKET_WIDTH+2))

FD_EXPORT char *fd_ulonglong_to_b32(unsigned long long offset,char *buf,int *len);
FD_EXPORT int fd_b32_to_ulonglong(const char *digits,unsigned long long *out);
FD_EXPORT long long fd_b32_to_longlong(const char *digits);

/* Fixnums */

#ifndef SIZEOF_VOID_P
#define FD_FIXNUM_BITS 30
#elif SIZEOF_VOID_P <= 4
#define FD_FIXNUM_BITS 30
#else
#define FD_FIXNUM_BITS ((SIZEOF_VOID_P*8)-3)
#endif

#define FD_MAX_FIXNUM ((((long long)1)<<(FD_FIXNUM_BITS))-1)
#define FD_MIN_FIXNUM -((((long long)1)<<(FD_FIXNUM_BITS))-1)

#define to64(x) ((long long)(x))
#define to64u(x) ((unsigned long long)(x))

#define FD_FIX2INT(x)							\
  ((long long)((((to64(x))>=0) ? ((x)/4) : (-((to64(-(x)))>>2)))))
#define FD_FIX2UINT(x)   \
  ((long long)((((to64(x))>=0) ? ((x)/4) : (-((to64(-(x)))>>2)))))

#define FD_INT2FIX(x)						\
  ((fdtype)							\
   ((FD_EXPECT_FALSE(x>FD_MAX_FIXNUM)) ? (FD_MAX_FIXNUM) :	\
    (FD_EXPECT_FALSE(x<FD_MIN_FIXNUM)) ? (FD_MIN_FIXNUM) :	\
    ((to64(x))>=0) ? (((to64(x))*4)|fd_fixnum_type) :		\
    (- ( fd_fixnum_type | ((to64(-(x)))<<2)))))

#define FD_MAKE_FIXNUM(x) \
  ((fdtype)							\
   (((to64(x))>=0) ? (((to64(x))*4)|fd_fixnum_type) :		\
    (- ( fd_fixnum_type | ((to64u(-(x)))<<2)) )))

#define FD_INT2DTYPE(x) \
  ((((to64(x)) > (to64(FD_MAX_FIXNUM))) ||			\
    ((to64(x)) < (to64(FD_MIN_FIXNUM)))) ?			\
   (fd_make_bigint(to64(x))) :					\
   ((fdtype)							\
    (((to64(x))>=0) ? (((to64(x))*4)|fd_fixnum_type) :		\
     (- ( fd_fixnum_type | ((to64u(-(x)))<<2)) ))))
#define FD_INT(x) (FD_INT2DTYPE(x))
#define FD_MAKEINT(x) (FD_INT2DTYPE(x))

#define FD_UINT2DTYPE(x) \
  (((to64u(x)) > (to64(FD_MAX_FIXNUM))) ?			\
   (fd_make_bigint(to64u(x))) :					\
   ((fdtype) (((to64u(x))*4)|fd_fixnum_type)))

#define FD_SHORT2DTYPE(x)				\
  ((fdtype)						\
   (((to64(x))>=0) ? (((to64(x))*4)|fd_fixnum_type) :	\
    (- ( fd_fixnum_type | ((to64(-(x)))<<2)))))

#define FD_SHORT2FIX(x)	(FD_SHORT2DTYPE(x))

#define FD_USHORT2DTYPE(x)     ((fdtype)(fd_fixnum_type|((x&0xFFFF)<<2)))
#define FD_BYTE2DTYPE(x)       ((fdtype) (fd_fixnum_type|((x&0xFF)<<2)))
#define FD_BYTE2LISP(x)        (FD_BYTE2DTYPE(x))
#define FD_FIXNUM_MAGNITUDE(x) ((x<0)?((-(x))>>2):(x>>2))
#define FD_FIXNUM_NEGATIVEP(x) (x<0)

#define FD_FIXZERO         (FD_SHORT2DTYPE(0))
#define FD_FIXNUM_ZERO     (FD_SHORT2DTYPE(0))
#define FD_FIXNUM_ONE      (FD_SHORT2DTYPE(1))
#define FD_FIXNUM_NEGONE   (FD_SHORT2DTYPE(-1))

#if FD_FIXNUM_BITS <= 30
#define FD_INTP(x) (FD_FIXNUMP(x))
#define FD_UINTP(x) ((FD_FIXNUMP(x))&&((FD_INT2DTYPE(x))>=0))
#define FD_LONGP(x) (FD_FIXNUMP(x))
#define FD_ULONGP(x) ((FD_FIXNUMP(x))&&((FD_INT2DTYPE(x))>=0))
#else
#define FD_INTP(x) \
  ((FD_FIXNUMP(x))&&(FD_FIX2INT(x)<=INT_MAX)&&(FD_FIX2INT(x)>=INT_MIN))
#define FD_UINTP(x) \
  ((FD_FIXNUMP(x))&&((FD_INT2DTYPE(x))>=0)&&(FD_FIX2INT(x)<=INT_MAX))
#define FD_LONGP(x) (FD_FIXNUMP(x))
#define FD_ULONGP(x) ((FD_FIXNUMP(x))&&((FD_INT2DTYPE(x))>=0))
#endif

#define FD_BYTEP(x) \
  ((FD_FIXNUMP(x))&&(FD_FIX2INT(x)>=0)&&(FD_FIX2INT(x)<0x100))
#define FD_SHORTP(x) \
  ((FD_FIXNUMP(x))&&(FD_FIX2INT(x)>=SHRT_MIN)&&(FD_FIX2INT(x)<=SHRT_MAX))
#define FD_USHORTP(x) \
  ((FD_FIXNUMP(x))&&(FD_FIX2INT(x)>=0)&&(FD_FIX2INT(x)<=USHRT_MAX))

#define FD_POSITIVE_FIXNUMP(n) \
  ((FD_FIXNUMP(x))&&(FD_FIX2INT(x)>0))
#define FD_NOTNEG_FIXNUMP(n) \
  ((FD_FIXNUMP(x))&&(FD_FIX2INT(x)>=0))
#define FD_POSITIVE_FIXINTP(n) \
  ((FD_INTP(x))&&(FD_FIX2INT(x)>0))
#define FD_NOTNEG_FIXINTP(n) \
  ((FD_INTP(x))&&(FD_FIX2INT(x)>=0))

/* Constants */

#define FD_CONSTANT(i) ((fdtype)(fd_immediate_ptr_type|(i<<2)))

#define FD_VOID                   FD_CONSTANT(0)
#define FD_FALSE                  FD_CONSTANT(1)
#define FD_TRUE                   FD_CONSTANT(2)
#define FD_EMPTY_CHOICE           FD_CONSTANT(3)
#define FD_EMPTY_LIST             FD_CONSTANT(4)
#define FD_EOF                    FD_CONSTANT(5)
#define FD_EOD                    FD_CONSTANT(6)
#define FD_EOX                    FD_CONSTANT(7)
#define FD_DTYPE_ERROR            FD_CONSTANT(8)
#define FD_PARSE_ERROR            FD_CONSTANT(9)
#define FD_OOM                    FD_CONSTANT(10)
#define FD_TYPE_ERROR             FD_CONSTANT(11)
#define FD_RANGE_ERROR            FD_CONSTANT(12)
#define FD_ERROR_VALUE            FD_CONSTANT(13)
#define FD_BADPTR                 FD_CONSTANT(14)
#define FD_THROW_VALUE            FD_CONSTANT(15)
#define FD_EXCEPTION_TAG          FD_CONSTANT(16)
#define FD_UNBOUND                FD_CONSTANT(17)
#define FD_NEVERSEEN              FD_CONSTANT(18)
#define FD_LOCKHOLDER             FD_CONSTANT(19)
#define FD_DEFAULT_VALUE          FD_CONSTANT(20)

#define FD_N_BUILTIN_CONSTANTS 21

FD_EXPORT const char *fd_constant_names[];
FD_EXPORT int fd_n_constants;
FD_EXPORT fdtype fd_register_constant(u8_string name);

#define FD_VOIDP(x) ((x) == (FD_VOID))
#define FD_NOVOIDP(x) ((x) != (FD_VOID))
#define FD_FALSEP(x) ((x) == (FD_FALSE))
#define FD_DEFAULTP(x) ((x) == (FD_DEFAULT_VALUE))
#define FD_TRUEP(x) ((x) == (FD_TRUE))
#define FD_EMPTY_CHOICEP(x) ((x) == (FD_EMPTY_CHOICE))
#define FD_EXISTSP(x) (!(x == FD_EMPTY_CHOICE))
#define FD_EMPTY_LISTP(x) ((x) == (FD_EMPTY_LIST))
#define FD_EOFP(x) ((x) == (FD_EOF))
#define FD_EODP(x) ((x) == (FD_EOD))
#define FD_EOXP(x) ((x) == (FD_EOX))

#define FD_THROWP(result) ((result) == (FD_THROW_VALUE))

#define FD_ABORTP(x) \
  (((FD_TYPEP(x,fd_constant_type)) && \
    (FD_GET_IMMEDIATE(x,fd_constant_type)>6) && \
    (FD_GET_IMMEDIATE(x,fd_constant_type)<=15)))
#define FD_TROUBLEP(x) \
  (((FD_TYPEP(x,fd_constant_type)) && \
    (FD_GET_IMMEDIATE(x,fd_constant_type)>6) && \
    (FD_GET_IMMEDIATE(x,fd_constant_type)<15)))
#define FD_COOLP(x) (!(FD_TROUBLEP(x)))

#define FD_ABORTED(x) (FD_EXPECT_FALSE(FD_ABORTP(x)))
#define FD_INTERRUPTED() (FD_EXPECT_FALSE(u8_current_exception!=(NULL)))

#define FD_CONSTANTP(x) \
  ((FD_IMMEDIATEP(x)) && ((FD_IMMEDIATE_TYPE(x))==0))

/* Characters */

#define FD_CHARACTERP(x) \
  ((FD_PTR_MANIFEST_TYPE(x) == fd_immediate_type) && \
   (FD_IMMEDIATE_TYPE(x) == fd_character_type))
#define FD_CODE2CHAR(x) (FDTYPE_IMMEDIATE(fd_character_type,x))
#define FD_CHARCODE(x) (FD_GET_IMMEDIATE(x,fd_character_type))
#define FD_CHAR2CODE(x) (FD_GET_IMMEDIATE(x,fd_character_type))

/* Symbols */

#ifndef FD_CORE_SYMBOLS
#define FD_CORE_SYMBOLS 8192
#endif
FD_EXPORT fdtype *fd_symbol_names;
FD_EXPORT int fd_n_symbols;
FD_EXPORT u8_mutex fd_symbol_lock;

#define FD_SYMBOLP(x) \
  ((FD_PTR_MANIFEST_TYPE(x) == fd_immediate_ptr_type) && \
   (FD_IMMEDIATE_TYPE(x) == fd_symbol_type))

#define FD_GOOD_SYMBOLP(x) \
  ((FD_PTR_MANIFEST_TYPE(x) == fd_immediate_ptr_type) && \
   (FD_IMMEDIATE_TYPE(x) == fd_symbol_type) && \
   (FD_GET_IMMEDIATE(x,fd_symbol_type)<fd_n_symbols))

#define FD_SYMBOL2ID(x) (FD_GET_IMMEDIATE(x,fd_symbol_type))
#define FD_ID2SYMBOL(i) (FDTYPE_IMMEDIATE(fd_symbol_type,i))

#define FD_SYMBOL_NAME(x) \
  ((FD_GOOD_SYMBOLP(x)) ? \
   (FD_STRDATA(fd_symbol_names[FD_SYMBOL2ID(x)])) :	\
   ((u8_string )("#.bad$ymbol.#")))
#define FD_XSYMBOL_NAME(x) (FD_STRDATA(fd_symbol_names[FD_SYMBOL2ID(x)]))

FD_EXPORT fdtype fd_make_symbol(u8_string string,int len);
FD_EXPORT fdtype fd_probe_symbol(u8_string string,int len);
FD_EXPORT fdtype fd_intern(u8_string string);
FD_EXPORT fdtype fd_symbolize(u8_string string);
FD_EXPORT fdtype fd_all_symbols(void);

FD_EXPORT fdtype FDSYM_TYPE, FDSYM_SIZE, FDSYM_LABEL, FDSYM_NAME;
FD_EXPORT fdtype FDSYM_BUFSIZE, FDSYM_BLOCKSIZE, FDSYM_CACHESIZE;
FD_EXPORT fdtype FDSYM_MERGE, FDSYM_SORT, FDSYM_SORTED;
FD_EXPORT fdtype FDSYM_LAZY, FDSYM_VERSION;
FD_EXPORT fdtype FDSYM_QUOTE, FDSYM_STAR, FDSYM_PLUS;
FD_EXPORT fdtype FDSYM_DOT, FDSYM_MINUS, FDSYM_EQUALS, FDSYM_QMARK;
FD_EXPORT fdtype FDSYM_CONTENT;
FD_EXPORT fdtype FDSYM_TEXT, FDSYM_LENGTH, FDSYM_TAG, FDSYM_CONS, FDSYM_STRING;
FD_EXPORT fdtype FDSYM_PREFIX, FDSYM_SUFFIX, FDSYM_SEP, FDSYM_OPT;
FD_EXPORT fdtype FDSYM_READONLY, FDSYM_ISADJUNCT;

/* Persistent pointers */

/* Persistent pointers are immediate values which refer to
   conses stored in a persistent table.  The idea is that
   persistent pointers are not subject to GC, so they can
   be passed much more quickly and without thread contention. */

#define FD_FCNIDP(x) \
  ((FD_PTR_MANIFEST_TYPE(x) == fd_immediate_ptr_type) && \
   (FD_IMMEDIATE_TYPE(x) == fd_fcnid_type))

FD_EXPORT struct FD_CONS **_fd_fcnids[];
FD_EXPORT int _fd_fcnid_count;
FD_EXPORT u8_mutex _fd_fcnid_lock;
FD_EXPORT int _fd_leak_fcnids;

#ifndef FD_FCNID_BLOCKSIZE
#define FD_FCNID_BLOCKSIZE 256
#endif

#ifndef FD_FCNID_NBLOCKS
#define FD_FCNID_NBLOCKS 256
#endif

#ifndef FD_INLINE_FCNIDS
#define FD_INLINE_FCNIDS 0
#endif

FD_EXPORT fdtype fd_resolve_fcnid(fdtype ref);
FD_EXPORT fdtype fd_register_fcnid(fdtype obj);
FD_EXPORT fdtype fd_set_fcnid(fdtype ref,fdtype newval);
FD_EXPORT int fd_deregister_fcnid(fdtype id,fdtype value);

FD_EXPORT fdtype fd_err(fd_exception,u8_context,u8_string,fdtype);

FD_EXPORT fd_exception fd_InvalidFCNID, fd_FCNIDOverflow;

#if FD_INLINE_FCNIDS
static U8_MAYBE_UNUSED fdtype _fd_fcnid_ref(fdtype ref)
{
  if (FD_TYPEP(ref,fd_fcnid_type)) {
    int serialno = FD_GET_IMMEDIATE(ref,fd_fcnid_type);
    if (FD_EXPECT_FALSE(serialno>_fd_fcnid_count))
      return fd_err(fd_InvalidFCNID,"_fd_fcnid_ref",NULL,ref);
    else return (fdtype) _fd_fcnids
	   [serialno/FD_FCNID_BLOCKSIZE]
	   [serialno%FD_FCNID_BLOCKSIZE];}
  else return ref;
}
#define fd_fcnid_ref(x) ((FD_FCNIDP(x))?(_fd_fcnid_ref(x)):(x))
#else
#define fd_fcnid_ref(x) ((FD_FCNIDP(x))?(fd_resolve_fcnid(x)):(x))
#endif

#define FD_FCNID_TYPEP(x,tp)    (FD_TYPEP(fd_fcnid_ref(x),tp))
#define FD_FCNID_TYPE(x)        (FD_PTR_TYPE(fd_fcnid_ref(x)))

/* Opcodes */

/* These are used by the evaluator.  We define the immediate type here,
   rather than dyanmically, so that they can be compile time constants
   and dispatch very quickly. */

#define FD_OPCODEP(x) \
  ((FD_PTR_MANIFEST_TYPE(x) == fd_immediate_ptr_type) && \
   (FD_IMMEDIATE_TYPE(x) == fd_opcode_type))

#define FD_OPCODE(num) (FDTYPE_IMMEDIATE(fd_opcode_type,num))
#define FD_OPCODE_NUM(op) (FD_GET_IMMEDIATE(op,fd_opcode_type))
#define FD_NEXT_OPCODE(op) \
  (FDTYPE_IMMEDIATE(fd_opcode_type,(1+(FD_OPCODE_NUM(op)))))

/* Lexrefs */

#define FD_LEXREFP(x) (FD_TYPEP(x,fd_lexref_type))
#define FD_LEXREF_UP(x) ((FD_GET_IMMEDIATE((x),fd_lexref_type))/32)
#define FD_LEXREF_ACROSS(x) ((FD_GET_IMMEDIATE((x),fd_lexref_type))%32)

/* Numeric macros */

#define FD_NUMBER_TYPEP(x) \
  ((x == fd_fixnum_type) || (x == fd_bigint_type) ||   \
   (x == fd_flonum_type) || (x == fd_rational_type) || \
   (x == fd_complex_type))
#define FD_NUMBERP(x) (FD_NUMBER_TYPEP(FD_PTR_TYPE(x)))

/* Generic handlers */

typedef unsigned int fd_compare_flags;
#define FD_COMPARE_QUICK           ((fd_compare_flags)(0))
#define FD_COMPARE_CODES           ((fd_compare_flags)(1))
#define FD_COMPARE_RECURSIVE       ((fd_compare_flags)(2))
#define FD_COMPARE_NATSORT         ((fd_compare_flags)(4))
#define FD_COMPARE_SLOTS           ((fd_compare_flags)(8))
#define FD_COMPARE_NUMERIC         ((fd_compare_flags)(16))
#define FD_COMPARE_ALPHABETICAL    ((fd_compare_flags)(32))
#define FD_COMPARE_CI              ((fd_compare_flags)(64))

/* Recursive but not natural or length oriented */
#define FD_COMPARE_FULL     \
  ((FD_COMPARE_RECURSIVE)|(FD_COMPARE_SLOTS)|(FD_COMPARE_CODES))
#define FD_COMPARE_NATURAL     \
  ((FD_COMPARE_RECURSIVE)|(FD_COMPARE_SLOTS)|(FD_COMPARE_NATSORT)|\
   (FD_COMPARE_NUMERIC)|(FD_COMPARE_ALPHABETICAL))

typedef unsigned int fd_walk_flags;
#define FD_WALK_CONSES      ((fd_walk_flags)(0))
#define FD_WALK_ALL         ((fd_walk_flags)(1))
#define FD_WALK_TERMINALS   ((fd_walk_flags)(2))
#define FD_WALK_CONTAINERS  ((fd_walk_flags)(4))
#define FD_WALK_CONSTANTS   ((fd_walk_flags)(8))

typedef void (*fd_recycle_fn)(struct FD_RAW_CONS *x);
typedef int (*fd_unparse_fn)(u8_output,fdtype);
typedef int (*fd_dtype_fn)(struct FD_OUTBUF *,fdtype);
typedef int (*fd_compare_fn)(fdtype,fdtype,fd_compare_flags);
typedef fdtype (*fd_copy_fn)(fdtype,int);
typedef int (*fd_walker)(fdtype,void *);
typedef int (*fd_walk_fn)(fd_walker,fdtype,void *,fd_walk_flags,int);

FD_EXPORT fd_recycle_fn fd_recyclers[FD_TYPE_MAX];
FD_EXPORT fd_unparse_fn fd_unparsers[FD_TYPE_MAX];
FD_EXPORT fd_dtype_fn fd_dtype_writers[FD_TYPE_MAX];
FD_EXPORT fd_compare_fn fd_comparators[FD_TYPE_MAX];
FD_EXPORT fd_copy_fn fd_copiers[FD_TYPE_MAX];
FD_EXPORT fd_walk_fn fd_walkers[FD_TYPE_MAX];

typedef u8_string (*fd_oid_info_fn)(fdtype x);
FD_EXPORT fd_oid_info_fn _fd_oid_info;

#define fd_intcmp(x,y) ((x<y) ? (-1) : (x>y) ? (1) : (0))
FD_EXPORT int fdtype_compare(fdtype x,fdtype y,fd_compare_flags);
FD_EXPORT int fdtype_equal(fdtype x,fdtype y);
FD_EXPORT int fd_numcompare(fdtype x,fdtype y);

#define FD_EQUAL FDTYPE_EQUAL
#define FD_EQUALP FDTYPE_EQUAL
#if FD_PROFILING_ENABLED
#define FDTYPE_EQUAL(x,y)          (fdtype_equal(x,y))
#define FDTYPE_EQUALV(x,y)         (fdtype_equal(x,y))
#define FDTYPE_COMPARE(x,y,flags)  (fdtype_compare(x,y,flags))
#define FD_QUICK_COMPARE(x,y)      (fdtype_compare(x,y,FD_COMPARE_QUICK))
#define FD_FULL_COMPARE(x,y)       (fdtype_compare(x,y,(FD_COMPARE_FULL)))
#else
#define FDTYPE_EQUAL(x,y) \
  ((x == y) || ((FD_CONSP(x)) && (FD_CONSP(y)) && (fdtype_equal(x,y))))
#define FDTYPE_EQUALV(x,y) \
  ((x == y) ? (1) :			  \
   (((FD_FIXNUMP(x)) && (FD_CONSP(x))) || \
    ((FD_FIXNUMP(y)) && (FD_CONSP(y))) || \
    ((FD_CONSP(x)) && (FD_CONSP(y)))) ?	  \
   (fdtype_equal(x,y)) :		  \
   (0)
#define FDTYPE_COMPARE(x,y,flags)  ((x == y) ? (0) : (fdtype_compare(x,y,flags)))
#define FD_QUICK_COMPARE(x,y) \
  (((FD_ATOMICP(x)) && (FD_ATOMICP(y))) ? (fd_intcmp(x,y)) : \
   (fdtype_compare(x,y,FD_COMPARE_QUICK)))
#define FD_FULL_COMPARE(x,y) \
  ((x == y) ? (0) : (FDTYPE_COMPARE(x,y,(FD_COMPARE_FULL))))
#endif

#define FD_QCOMPARE(x,y) FD_QUICK_COMPARE(x,y)
#define FD_COMPARE(x,y) FD_FULL_COMPARE(x,y)

FD_EXPORT int fd_walk(fd_walker walker,fdtype obj,void *walkdata,
		      fd_walk_flags flags,int depth);

FD_EXPORT void fdtype_sort(fdtype *v,size_t n,fd_compare_flags flags);

/* Debugging support */

FD_EXPORT fd_ptr_type _fd_ptr_type(fdtype x);
FD_EXPORT fdtype _fd_debug(fdtype x);

/* Pointer checking for internal debugging */

FD_EXPORT int fd_check_immediate(fdtype);

#define FD_CHECK_CONS_PTR(x)                          \
 ((FD_ISDTYPE(x))&&                                   \
  ((FD_FIXNUMP(x)) ? (1) :                          \
   (FD_OIDP(x)) ? (((x>>2)&0x3FF)<fd_n_base_oids) : \
   (x==0) ? (0) :                                   \
   (FD_CONSP(x)) ?                                  \
   (((((FD_CONS *)x)->conshead)<0xFFFFFF80) &&      \
    (FD_CONS_TYPE((FD_CONS *)x)>3) &&               \
    (FD_CONS_TYPE((FD_CONS *)x)<fd_next_cons_type)) : \
   (fd_check_immediate(x))))

#define FD_CHECK_ATOMIC_PTR(x)                     \
 ((FD_ISDTYPE(x))&&                                  \
  ((FD_FIXNUMP(x)) ? (1) :                          \
   (FD_OIDP(x)) ? (((x>>2)&0x3FF)<fd_n_base_oids) : \
   (x==0) ? (0) :                                   \
   (FD_CONSP(x)) ? ((x == FD_NULL) ? (0) : (1)) :	    \
   (fd_check_immediate(x))))

#if FD_FULL_CHECK_PTR
#define FD_CHECK_PTR FD_CHECK_CONS_PTR
#else
#define FD_CHECK_PTR FD_CHECK_ATOMIC_PTR
#endif

#ifdef FD_PTR_DEBUG_LEVEL
#if (FD_PTR_DEBUG_LEVEL>2)
#define FD_DEBUG_BADPTRP(x) (FD_EXPECT_FALSE(!(FD_CHECK_PTR(x))))
#elif (FD_PTR_DEBUG_LEVEL==2)
#define FD_DEBUG_BADPTRP(x) (FD_EXPECT_FALSE(!(FD_CHECK_ATOMIC_PTR(x))))
#elif (FD_PTR_DEBUG_LEVEL==1)
#define FD_DEBUG_BADPTRP(x) (FD_EXPECT_FALSE((x) == FD_NULL))
#else
#define FD_DEBUG_BADPTRP(x) (0)
#endif
#else
#define FD_DEBUG_BADPTRP(x) (0)
#endif

/* These constitue a simple API for including ptr checks which
   turn into noops depending on the debug level, which
   can be specified in different ways.  */

/* FD_CHECK_PTRn is the return value call,
   FD_PTR_CHECKn is the side-effect call.

   FD_PTR_DEBUG_DENSITY enables the numbered versions
    of each call.  If it's negative, the runtime
    variable fd_ptr_debug_level is used instead.

   Each higher level includes the levels below it.

   As a rule of thumb, currently, the levels should be used as follows:
    1: topical debugging for places you think something odd is happening;
        the system code should almost never use this level
    2: assignment debugging (called when a pointer is stored)
    3: return value debugging (called when a pointer is returned)

   Setting a breakpoint at _fd_bad_pointer is a good idea.
*/

FD_EXPORT void _fd_bad_pointer(fdtype,u8_context);

static U8_MAYBE_UNUSED fdtype _fd_check_ptr(fdtype x,u8_context cxt) {
  if (FD_DEBUG_BADPTRP(x)) _fd_bad_pointer(x,cxt);
  return x;
}

FD_EXPORT int fd_ptr_debug_density;

#if (FD_PTR_DEBUG_DENSITY<0)
/* When the check level is negative, use a runtime variable. */
#define FD_CHECK_PTR1(x,cxt) ((fd_ptr_debug_density>0) ? (_fd_check_ptr(x,((u8_context)cxt))) : (x))
#define FD_PTR_CHECK1(x,cxt)                                            \
  {if ((fd_ptr_debug_density>0) && (FD_DEBUG_BADPTRP(x))) _fd_bad_pointer(x,((u8_context)cxt));}
#define FD_CHECK_PTR2(x,cxt) ((fd_ptr_debug_density>1) ? (_fd_check_ptr(x,((u8_context)cxt))) : (x))
#define FD_PTR_CHECK2(x,cxt)                                            \
  {if ((fd_ptr_debug_density>1) && (FD_DEBUG_BADPTRP(x))) _fd_bad_pointer(x,((u8_context)cxt));}
#define FD_CHECK_PTR3(x,cxt) ((fd_ptr_debug_density>2) ? (_fd_check_ptr(x,((u8_context)cxt))) : (x))
#define FD_PTR_CHECK3(x,cxt)                                            \
  {if ((fd_ptr_debug_density>2) && (FD_DEBUG_BADPTRP(x))) _fd_bad_pointer(x,((u8_context)cxt));}
#else
#if (FD_PTR_DEBUG_DENSITY > 0)
#define FD_CHECK_PTR1(x,cxt) (_fd_check_ptr((x),((u8_context)cxt)))
#define FD_PTR_CHECK1(x,cxt)                    \
  {if (FD_DEBUG_BADPTRP(x)) _fd_bad_pointer(x,((u8_context)cxt));}
#endif
#if (FD_PTR_DEBUG_DENSITY > 1)
#define FD_CHECK_PTR2(x,cxt) (_fd_check_ptr((x),((u8_context)cxt)))
#define FD_PTR_CHECK2(x,cxt)                    \
  {if (FD_DEBUG_BADPTRP(x)) _fd_bad_pointer(x,((u8_context)cxt));}
#endif
#if (FD_PTR_DEBUG_DENSITY > 2)
#define FD_CHECK_PTR3(x,cxt) (_fd_check_ptr((x),((u8_context)cxt)))
#define FD_PTR_CHECK3(x,cxt)                    \
  {if (FD_DEBUG_BADPTRP(x)) _fd_bad_pointer(x,((u8_context)cxt));}
#endif
#endif

/* Now define any undefined operations. */
#ifndef FD_PTR_CHECK1
#define FD_PTR_CHECK1(x,cxt)
#endif
#ifndef FD_PTR_CHECK2
#define FD_PTR_CHECK2(x,cxt)
#endif
#ifndef FD_PTR_CHECK3
#define FD_PTR_CHECK3(x,cxt)
#endif

#ifndef FD_CHECK_PTR1
#define FD_CHECK_PTR1(x,cxt) (x)
#endif
#ifndef FD_CHECK_PTR2
#define FD_CHECK_PTR2(x,cxt) (x)
#endif
#ifndef FD_CHECK_PTR3
#define FD_CHECK_PTR3(x,cxt) (x)
#endif

#endif /* ndef FRAMERD_PTR_H */
