/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2009 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.

   This file implements the basic structures, macros, and function
    prototypes for dealing with dtype pointers as implemented for the
    fdb library
*/

/* THE ANATOMY OF A POINTER

   The fdb library is built on a dynamically typed "lisp-like" data
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
    implementation of CONSes is described at the head of <fdb/cons.h>,
    but it is basically a block of memory whose first 4 bytes encode a
    25-bit reference count and a 7-bit type code.

    The IMMEDIATE type is used to represent a number of "constant" data
    types, including booleans, special values (error codes, etc), unicode
    characters, and symbols.  The high 7 bits of an IMMEDIATE pointer encode
    a more detailed type code and the remaining 23 bits are available to
    represent particular values of each type.

    The FIXNUM base pointer type is used for small magnitude signed integers.
    Rather than using a two's complement representation for negative numbers,
    fdb uses the 32nd bit (the high order bit on 32 bit architectures) as a sign
    bit and stores the absolute magnitude in the remainder of the word (down to
    the two bits of type code.  For positive numbers, converting between C ints
    dtype fixnums is simply a matter of multiply or diving by 4 and adding the
    type code (when converting to DTYPEs).  For negative numbers, it is trickier but
    not much, as in FD_INT2DTYPE and FD_DTYPE2INT below.  Currently, fixnums
    do not take advantage of the full word on 64-bit machines, for a mix
    of present practical and prospective design reasons.

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

#ifndef FDB_PTR_H
#define FDB_PTR_H 1
#define FDB_PTR_H_VERSION "$Id$"

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
#define FD_MAX_CONS_TYPE  FD_CONS_TYPECODE(0x80)
#define FD_MAX_IMMEDIATE_TYPES 0x80
#define FD_MAX_IMMEDIATE_TYPE FD_IMMEDIATE_TYPECODE(0x80)

FD_EXPORT fd_exception fd_BadPtr;

#define fd_cons_ptr_type (0)
#define fd_immediate_ptr_type (1)
#define fd_fixnum_ptr_type (2)
#define fd_oid_ptr_type (3)

typedef enum FD_PTR_TYPE {
  fd_cons_type=0, fd_immediate_type=1,
  fd_fixnum_type=2, fd_oid_type=3,

  fd_constant_type=FD_IMMEDIATE_TYPECODE(0),
  fd_character_type=FD_IMMEDIATE_TYPECODE(1),
  fd_symbol_type=FD_IMMEDIATE_TYPECODE(2),
  fd_pptr_type=FD_IMMEDIATE_TYPECODE(3),
  /* Reserved as constants for an evaluator */
  fd_lexref_type=FD_IMMEDIATE_TYPECODE(4),
  fd_opcode_type=FD_IMMEDIATE_TYPECODE(5),

  fd_string_type=FD_CONS_TYPECODE(0),
  fd_packet_type=FD_CONS_TYPECODE(1),
  fd_bigint_type=FD_CONS_TYPECODE(2),
  fd_pair_type=FD_CONS_TYPECODE(3),
  fd_compound_type=FD_CONS_TYPECODE(4),
  fd_choice_type=FD_CONS_TYPECODE(5),
  fd_achoice_type=FD_CONS_TYPECODE(6),
  fd_qchoice_type=FD_CONS_TYPECODE(7),
  fd_vector_type=FD_CONS_TYPECODE(8), 
  fd_slotmap_type=FD_CONS_TYPECODE(9),
  fd_schemap_type=FD_CONS_TYPECODE(10),
  fd_hashtable_type=FD_CONS_TYPECODE(11),
  fd_hashset_type=FD_CONS_TYPECODE(12),
  fd_wrapper_type=FD_CONS_TYPECODE(13),
  fd_mystery_type=FD_CONS_TYPECODE(14),
  fd_function_type=FD_CONS_TYPECODE(15),
  fd_error_type=FD_CONS_TYPECODE(16),
  fd_complex_type=FD_CONS_TYPECODE(17),
  fd_rational_type=FD_CONS_TYPECODE(18),
  fd_double_type=FD_CONS_TYPECODE(19),
  fd_timestamp_type=FD_CONS_TYPECODE(20),
  fd_dtproc_type=FD_CONS_TYPECODE(21),
  fd_tail_call_type=FD_CONS_TYPECODE(22)

  } fd_ptr_type;

#define FD_BUILTIN_CONS_TYPES 23
#define FD_BUILTIN_IMMEDIATE_TYPES 6
FD_EXPORT unsigned int fd_next_cons_type;
FD_EXPORT unsigned int fd_next_immediate_type;

typedef int (*fd_checkfn)(fdtype);
FD_EXPORT fd_checkfn fd_immediate_checkfns[FD_MAX_IMMEDIATE_TYPES+4];

FD_EXPORT int fd_register_cons_type(char *name);
FD_EXPORT int fd_register_immediate_type(char *name,fd_checkfn fn);

/* In the type field, 0 means an integer, 1 means an oid, 2 means
   an immediate constant, and 3 means a cons. */
#define FD_PTR_MANIFEST_TYPE(x) ((x)&0x3)
#define FD_CONSP(x) ((FD_PTR_MANIFEST_TYPE(x))==fd_cons_ptr_type)
#define FD_ATOMICP(x) ((FD_PTR_MANIFEST_TYPE(x))!=fd_cons_ptr_type)
#define FD_FIXNUMP(x) ((FD_PTR_MANIFEST_TYPE(x))==fd_fixnum_ptr_type)
#define FD_IMMEDIATEP(x) ((FD_PTR_MANIFEST_TYPE(x))==fd_immediate_ptr_type)
#define FD_OIDP(x) ((FD_PTR_MANIFEST_TYPE(x))==fd_oid_ptr_type)
#define FD_INT_DATA(x) ((x)>>2)
#define FD_EQ(x,y) ((x)==(y))

typedef unsigned int fd_consbits;
#define FD_CONS_HEADER fd_consbits consbits
typedef struct FD_CONS { FD_CONS_HEADER; } FD_CONS;
typedef struct FD_CONS *fd_cons;

#define FD_CONS_DATA(x) ((fd_cons)(x))

/* Most of the stuff for dealing with conses is in cons.h.  The
    attribute common to all conses, is that its first field is
    a combination of a typecode and a reference count.  We include
    this here so that we can define a pointer type procedure which
    gets the cons type. */
#define FDTYPE_CONS(ptr) ((fdtype)ptr)
#define FD_INIT_CONS(ptr,type) \
  ((struct FD_CONS *)ptr)->consbits=((type-0x84)|0x80)
#define FD_INIT_FRESH_CONS(ptr,type) \
  memset(ptr,0,sizeof(*(ptr))); \
  ((struct FD_CONS *)ptr)->consbits=((type-0x84)|0x80)
#define FD_INIT_STACK_CONS(ptr,type) \
  ((struct FD_CONS *)ptr)->consbits=(type-0x84)
#define FD_CONS_TYPE(x) ((((x)->consbits)&0x7F)+0x84)
#define FD_SET_CONS_TYPE(ptr,type) \
  ((struct FD_CONS *)ptr)->consbits=\
    ((((struct FD_CONS *)ptr)->consbits&(~0x7F))|((type-0x84)&0x7f))
#define FD_NULL ((fdtype)(NULL))
#define FD_NULLP(x) (((void *)x)==NULL)

#define FDTYPE_IMMEDIATE(tcode,serial) \
  ((fdtype)(((((tcode)-0x04)&0x7F)<<25)|((serial)<<2)|fd_immediate_ptr_type))
#define FD_GET_IMMEDIATE(x,tcode) (((x)>>2)&0x7FFFFF)
#define FD_IMMEDIATE_TYPE_FIELD(x) (((x)>>25)&0x7F)
#define FD_IMMEDIATE_TYPE(x) ((((x)>>25)&0x7F)+0x4)

#if FD_PTR_TYPE_MACRO
#define FD_PTR_TYPE(x) \
  (((FD_PTR_MANIFEST_TYPE(x))>1) ? (FD_PTR_MANIFEST_TYPE(x)) : \
   ((FD_PTR_MANIFEST_TYPE(x))==1) ? (FD_IMMEDIATE_TYPE(x)) : \
   (x) ? (FD_CONS_TYPE(((struct FD_CONS *)FD_CONS_DATA(x)))) : (-1))

#else
static fd_ptr_type FD_PTR_TYPE(fdtype x)
{
  int type_field=(FD_PTR_MANIFEST_TYPE(x));
  switch (type_field) {
  case fd_cons_ptr_type: {
    struct FD_CONS *cons=(struct FD_CONS *)x;
    return FD_CONS_TYPE(cons);}
  case fd_cons_immediate_type: return FD_IMMEDIATE_TYPE(x);
  default: return type_field;
  }
}
#endif

#define FD_PTR_TYPEP(x,type) ((FD_PTR_TYPE(x)) == type)

/* OIDs */

#if FD_STRUCT_OIDS
typedef struct FD_OID {
  unsigned int hi, lo;} FD_OID;
typedef struct FD_OID *fd_oid;
#define FD_OID_HI(x) x.hi
#define FD_OID_LO(x) x.lo
FD_FASTOP FD_OID FD_OID_PLUS(FD_OID x,unsigned int increment)
{
  x.lo=x.lo+increment;
  return x;
}
#define FD_SET_OID_HI(oid,hiw) oid.hi=hiw
#define FD_SET_OID_LO(oid,low) oid.lo=low
#define FD_OID_COMPARE(oid1,oid2) \
  ((oid1.hi == oid2.hi) ? \
   ((oid1.lo == oid2.lo) ? (0) : (oid1.lo < oid2.lo) ? (-1) : (1)) :\
   (oid1.hi < oid2.hi) ? (-1) : (1))
#define FD_OID_DIFFERENCE(oid1,oid2) \
  ((oid1.lo>oid2.lo) ? (oid1.lo-oid2.lo) : (oid2.lo-oid1.lo))
#elif FD_INT_OIDS
typedef unsigned int FD_OID;
#define FD_OID_HI(x) ((unsigned int)((x)>>32))
#define FD_OID_LO(x) ((unsigned int)((x)&(0xFFFFFFFFU)))
#define FD_OID_PLUS(oid,increment) (oid+increment)
#define FD_SET_OID_HI(oid,hi) oid=(oid&0xFFFFFFFFUL)|((hi)<<32)
#define FD_SET_OID_LO(oid,lo) oid=((oid&0xFFFFFFFF00000000U)|(((FD_OID)lo)&0xFFFFFFFFU))
#define FD_OID_COMPARE(oid1,oid2) ((oid1 == oid2) ? (0) : (oid1<oid2) ? (-1) : (1))
#define FD_OID_DIFFERENCE(oid1,oid2) ((oid1>oid2) ? (oid1-oid2) : (oid2-oid1))
#elif FD_LONG_OIDS
typedef unsigned long FD_OID;
#define FD_OID_HI(x) ((unsigned int)((x)>>32))
#define FD_OID_LO(x) ((unsigned int)((x)&(0xFFFFFFFFU)))
#define FD_OID_PLUS(oid,increment) (oid+increment)
#define FD_SET_OID_HI(oid,hi) oid=((FD_OID)(oid&0xFFFFFFFFU))|(((FD_OID)hi)<<32)
#define FD_SET_OID_LO(oid,lo) oid=(oid&0xFFFFFFFF00000000UL)|((lo)&0xFFFFFFFFUL)
#define FD_OID_COMPARE(oid1,oid2) ((oid1 == oid2) ? (0) : (oid1<oid2) ? (-1) : (1))
#define FD_OID_DIFFERENCE(oid1,oid2) ((oid1>oid2) ? (oid1-oid2) : (oid2-oid1))
#elif FD_LONG_LONG_OIDS
typedef unsigned long long FD_OID;
#define FD_OID_HI(x) ((unsigned int)((x)>>32))
#define FD_OID_LO(x) ((unsigned int)((x)&(0xFFFFFFFFULL)))
#define FD_OID_PLUS(oid,increment) (oid+increment)
#define FD_SET_OID_HI(oid,hi) oid=((FD_OID)(oid&0xFFFFFFFFU))|(((FD_OID)hi)<<32)
#define FD_SET_OID_LO(oid,lo) \
  oid=((oid&(0xFFFFFFFF00000000ULL))|((unsigned int)((lo)&(0xFFFFFFFFULL))))
#define FD_OID_COMPARE(oid1,oid2) ((oid1 == oid2) ? (0) : (oid1<oid2) ? (-1) : (1))
#define FD_OID_DIFFERENCE(oid1,oid2) ((oid1>oid2) ? (oid1-oid2) : (oid2-oid1))
#endif

#if FD_STRUCT_OIDS
FD_FASTOP FD_OID FD_MAKE_OID(unsigned int hi,unsigned int lo)
{
  FD_OID result; result.hi=hi; result.lo=lo;
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

/* An OID references has 10 bits of base index and 20 bits of offset.
   This is encoded in a DTYPE pointer with the offset in the high 20 bits
   and the base in the lower portion. */
FD_EXPORT FD_OID *fd_base_oids;
FD_EXPORT int fd_n_base_oids;
/* An OID in 32 bits:
    xxxxxxxxxxxxxxxxxxxxbbbbbbbbbb11
    x=offset, b=baseid, 1=typecode
*/
#define FD_OID_ADDR(x) \
  (FD_OID_PLUS(fd_base_oids[((x>>2)&0x3FF)],(x>>12)))
#define FD_CONSTRUCT_OID(base,offset) ((fdtype) (((base)<<2)|((offset)<<12)|3))
FD_EXPORT fdtype fd_make_oid(FD_OID addr);
FD_EXPORT int fd_get_oid_base_index(FD_OID addr,int add);

#define FD_OID_BASE_ID(x) (((x)>>2)&0x3FF)
#define FD_OID_BASE_OFFSET(x) ((x)>>12)
#define FD_OID_BUCKET_SIZE    (1<<12)

/* Fixnums */

#define FD_FIXNUM_SIGN_BIT 0x80000000
#define FD_FIXNUM_MAGNITUDE_MASK 0x7FFFFFFF
#define FD_MAX_FIXNUM            0x1FFFFFFF
#define FD_MIN_FIXNUM           -0x1FFFFFFF

#define FD_FIX2INT(x) \
  ((int)((((int)x)>=0) ? ((x)/4) : (-((x&FD_FIXNUM_MAGNITUDE_MASK)>>2))))
#define FD_INT2DTYPE(x) \
  ((((x) > FD_MAX_FIXNUM) || ((x) < FD_MIN_FIXNUM)) ?	\
   (fd_make_bigint(x)) : \
   (((fdtype)((x>=0) ? (((x)*4)|fd_fixnum_type) : \
	       (FD_FIXNUM_SIGN_BIT|fd_fixnum_type|((-(x))<<2))))))
#define FD_SHORT2DTYPE(x) \
  (((fdtype)((x>=0) ? (((x)*4)|fd_fixnum_type) : \
	      (FD_FIXNUM_SIGN_BIT|fd_fixnum_type|((-(x))<<2)))))
#define FD_USHORT2DTYPE(x) ((fdtype)(fd_fixnum_type|((x&0xFFFF)<<2)))
#define FD_BYTE2DTYPE(x)((fdtype) (fd_fixnum_type|((x&0xFF)<<2)))
#define FD_BYTE2LISP(x)((fdtype) (fd_fixnum_type|((x&0xFF)<<2)))
#define FD_FIXNUM_MAGNITUDE(x) (x&FD_FIXNUM_MAGNITUDE_MASK)
#define FD_FIXNUM_NEGATIVEP(x) (x&FD_FIXNUM_SIGN_BIT)

#define FD_FIXZERO         (FD_SHORT2DTYPE(0))
#define FD_FIXNUM_ZERO     (FD_SHORT2DTYPE(0))
#define FD_FIXNUM_ONE      (FD_SHORT2DTYPE(1))
#define FD_FIXNUM_NEGONE   (FD_SHORT2DTYPE(-1))

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

#define FD_VOIDP(x) ((x) == (FD_VOID))
#define FD_NOVOIDP(x) ((x) != (FD_VOID))
#define FD_FALSEP(x) ((x) == (FD_FALSE))
#define FD_TRUEP(x) ((x) == (FD_TRUE))
#define FD_EMPTY_CHOICEP(x) ((x) == (FD_EMPTY_CHOICE))
#define FD_EXISTSP(x) (!(x == FD_EMPTY_CHOICE))
#define FD_EMPTY_LISTP(x) ((x) == (FD_EMPTY_LIST))
#define FD_EOFP(x) ((x) == (FD_EOF))
#define FD_EODP(x) ((x) == (FD_EOD))
#define FD_EOXP(x) ((x) == (FD_EOX))

#define FD_THROWP(result) ((result)==(FD_THROW_VALUE))

#define FD_ABORTP(x) \
  (((FD_PTR_TYPEP(x,fd_constant_type)) && \
    (FD_GET_IMMEDIATE(x,fd_constant_type)>6) && \
    (FD_GET_IMMEDIATE(x,fd_constant_type)<=15)))
#define FD_TROUBLEP(x) \
  (((FD_PTR_TYPEP(x,fd_constant_type)) && \
    (FD_GET_IMMEDIATE(x,fd_constant_type)>6) && \
    (FD_GET_IMMEDIATE(x,fd_constant_type)<15)))
#define FD_COOLP(x) (!(FD_TROUBLEP(x)))

#define FDTYPE_CONSTANTP(x) \
  ((FD_IMMEDIATEP(x)) && ((FD_IMMEDIATE_TYPE(x))==0))

/* Characters */

#define FD_CHARACTERP(x) \
  ((FD_PTR_MANIFEST_TYPE(x)==fd_immediate_type) && \
   (FD_IMMEDIATE_TYPE(x)==fd_character_type))
#define FD_CODE2CHAR(x) (FDTYPE_IMMEDIATE(fd_character_type,x))
#define FD_CHARCODE(x) (FD_GET_IMMEDIATE(x,fd_character_type))
#define FD_CHAR2CODE(x) (FD_GET_IMMEDIATE(x,fd_character_type))

/* Symbols */

#ifndef FD_CORE_SYMBOLS
#define FD_CORE_SYMBOLS 4096
#endif
FD_EXPORT fdtype *fd_symbol_names;
FD_EXPORT int fd_n_symbols;
#if FD_THREADS_ENABLED
FD_EXPORT u8_mutex fd_symbol_lock;
#endif

#define FD_SYMBOLP(x) \
  ((FD_PTR_MANIFEST_TYPE(x)==fd_immediate_ptr_type) && \
   (FD_IMMEDIATE_TYPE(x)==fd_symbol_type))

#define FD_SYMBOL2ID(x) (FD_GET_IMMEDIATE(x,fd_symbol_type))
#define FD_ID2SYMBOL(i) (FDTYPE_IMMEDIATE(fd_symbol_type,i))

#define FD_SYMBOL_NAME(x) \
  ((FD_SYMBOLP(x)) ? (FD_STRDATA(fd_symbol_names[FD_SYMBOL2ID(x)])) : \
   ((u8_string )NULL))
#define FD_XSYMBOL_NAME(x) (FD_STRDATA(fd_symbol_names[FD_SYMBOL2ID(x)]))

FD_EXPORT fdtype fd_make_symbol(u8_string string,int len);
FD_EXPORT fdtype fd_probe_symbol(u8_string string,int len);
FD_EXPORT fdtype fd_intern(u8_string string);
FD_EXPORT fdtype fd_all_symbols(void);

/* Persistent pointers */

/* Persistent pointers are immediate values which refer to
   conses stored in a persistent table.  The idea is that
   persistent pointers are not subject to GC, so they can
   be passed much more quickly and without thread contention. */

#define FD_PPTRP(x) \
  ((FD_PTR_MANIFEST_TYPE(x)==fd_immediate_ptr_type) && \
   (FD_IMMEDIATE_TYPE(x)==fd_pptr_type))

FD_EXPORT struct FD_CONS **_fd_pptrs[];
FD_EXPORT int _fd_npptrs;
#if FD_THREADS_ENABLED
FD_EXPORT u8_mutex _fd_pptr_lock;
#endif

#ifndef FD_PPTR_BLOCKSIZE
#define FD_PPTR_BLOCKSIZE 256
#endif

#ifndef FD_PPTR_NBLOCKS
#define FD_PPTR_NBLOCKS 256
#endif

#ifndef FD_INLINE_PPTRS
#define FD_INLINE_PPTRS 0
#endif

FD_EXPORT fdtype _fd_pptr_ref(fdtype ref);
FD_EXPORT fdtype fd_pptr_register(fdtype obj);

FD_EXPORT fdtype fd_err(fd_exception,u8_context,u8_string,fdtype);

FD_EXPORT fd_exception fd_InvalidPPtr, fd_PPtrOverflow;

#if FD_INLINE_PPTRS
static fdtype fd_pptr_ref(fdtype ref)
{
  if (FD_PTR_TYPEP(ref,fd_pptr_type)) {
    int serialno=FD_GET_IMMEDIATE(ref,fd_pptr_type);
    if (FD_EXPECT_FALSE(serialno>_fd_npptrs))
      return fd_err(fd_InvalidPPtr,"_fd_pptr_ref",NULL,ref);
    else return (fdtype) _fd_pptrs[serialno/FD_PPTR_BLOCKSIZE][serialno%FD_PPTR_BLOCKSIZE];}
  else return ref;
}
#else
#define fd_pptr_ref _fd_pptr_ref
#endif

#define FD_PRIM_TYPEP(x,tp) \
  (((FD_PTR_MANIFEST_TYPE(x)==fd_immediate_ptr_type) && \
    (FD_IMMEDIATE_TYPE(x)==fd_pptr_type)) ? \
   (FD_PTR_TYPEP((fd_pptr_ref(x)),tp)) : (FD_PTR_TYPEP(x,tp)))

#define FD_PRIM_TYPE(x) \
  (((FD_PTR_MANIFEST_TYPE(x)==fd_immediate_ptr_type) && \
    (FD_IMMEDIATE_TYPE(x)==fd_pptr_type)) ? \
    (FD_PTR_TYPE(fd_pptr_ref(x))) : (FD_PTR_TYPE(x)))

/* Opcodes */

/* These are used by the evaluator.  We define the immediate type here,
   rather than dyanmically, so that they can be compile time constants
   and dispatch very quickly. */

#define FD_OPCODEP(x) \
  ((FD_PTR_MANIFEST_TYPE(x)==fd_immediate_ptr_type) && \
   (FD_IMMEDIATE_TYPE(x)==fd_opcode_type))

#define FD_OPCODE(num) (FDTYPE_IMMEDIATE(fd_opcode_type,num))
#define FD_OPCODE_NUM(num) (FD_GET_IMMEDIATE(x,fd_opcode_type))
#define FD_NEXT_OPCODE(op) \
  (FDTYPE_IMMEDIATE(fd_opcode_type,(1+(FD_OPCODE_NUM(op)))))

/* Lexrefs */

#define FD_LEXREFP(x) (FD_PRIM_TYPEP(x,fd_lexref_type))
#define FD_LEXREF_UP(x) ((FD_GET_IMMEDIATE((x),fd_lexref_type))/32)
#define FD_LEXREF_ACROSS(x) ((FD_GET_IMMEDIATE((x),fd_lexref_type))%32)

/* Numeric macros */

#define FD_NUMBER_TYPEP(x) \
  ((x==fd_fixnum_type) || (x==fd_bigint_type) ||   \
   (x==fd_double_type) || (x==fd_rational_type) || \
   (x==fd_complex_type))
#define FD_NUMBERP(x) (FD_NUMBER_TYPEP(FD_PTR_TYPE(x)))

/* Generic handlers */

typedef void (*fd_recycle_fn)(struct FD_CONS *x);
typedef int (*fd_unparse_fn)(u8_output,fdtype);
typedef int (*fd_dtype_fn)(struct FD_BYTE_OUTPUT *,fdtype);
typedef int (*fd_compare_fn)(fdtype,fdtype,int);
typedef fdtype (*fd_copy_fn)(fdtype,int);

FD_EXPORT u8_string fd_type_names[FD_TYPE_MAX];
FD_EXPORT fd_recycle_fn fd_recyclers[FD_TYPE_MAX];
FD_EXPORT fd_unparse_fn fd_unparsers[FD_TYPE_MAX];
FD_EXPORT fd_dtype_fn fd_dtype_writers[FD_TYPE_MAX];
FD_EXPORT fd_compare_fn fd_comparators[FD_TYPE_MAX];
FD_EXPORT fd_copy_fn fd_copiers[FD_TYPE_MAX];

typedef u8_string (*fd_oid_info_fn)(fdtype x);
FD_EXPORT fd_oid_info_fn _fd_oid_info;

#define fd_intcmp(x,y) ((x<y) ? (-1) : (x>y) ? (1) : (0))
FD_EXPORT int fdtype_compare(fdtype x,fdtype y,int);
FD_EXPORT int fdtype_equal(fdtype x,fdtype y);
FD_EXPORT int fd_numcompare(fdtype x,fdtype y);


#define FD_EQUAL FDTYPE_EQUAL
#if FD_PROFILING_ENABLED
#define FDTYPE_EQUAL(x,y) (fdtype_equal(x,y))
#define FD_QCOMPARE(x,y) (fdtype_compare(x,y,1))
#define FDTYPE_COMPARE(x,y) (fdtype_compare(x,y,0))
#define FD_COMPARE(x,y,fast) (fdtype_compare(x,y,fast))
#else
#define FDTYPE_EQUAL(x,y) \
  ((x==y) || ((FD_CONSP(x)) && (FD_CONSP(y)) && (fdtype_equal(x,y))))
#define FD_QCOMPARE(x,y) \
  (((FD_ATOMICP(x)) && (FD_ATOMICP(y))) ? (fd_intcmp(x,y)) : \
   (fdtype_compare(x,y,1)))
#define FDTYPE_COMPARE(x,y) \
  (fdtype_compare(x,y,0))
#define FD_COMPARE(x,y,fast) \
  ((fast) ? (FD_QCOMPARE(x,y)) : (FDTYPE_COMPARE(x,y)))
#endif

/* Pointer check functions and macros */

/* Debugging support */

FD_EXPORT fd_ptr_type _fd_ptr_type(fdtype x);
FD_EXPORT fdtype _fd_debug(fdtype x);

/* Pointer checking for internal debugging */

FD_EXPORT int fd_check_immediate(fdtype);

#define FD_CHECK_PTR(x)                            \
 ((FD_FIXNUMP(x)) ? (1) :                          \
  (FD_OIDP(x)) ? (((x>>2)&0x3FF)<fd_n_base_oids) : \
  (x==0) ? (0) :                                   \
  (FD_CONSP(x)) ?                                  \
  (((((FD_CONS *)x)->consbits)<0xFFFFFF80) &&      \
   (FD_CONS_TYPE((FD_CONS *)x)>3) &&               \
   (FD_CONS_TYPE((FD_CONS *)x)<fd_next_cons_type)) : \
  (fd_check_immediate(x)))

#define FD_CHECK_ATOMIC_PTR(x)                     \
 ((FD_FIXNUMP(x)) ? (1) :                          \
  (FD_OIDP(x)) ? (((x>>2)&0x3FF)<fd_n_base_oids) : \
  (x==0) ? (0) :                                   \
  (FD_CONSP(x)) ? (1) :                            \
  (fd_check_immediate(x)))

#ifdef FD_PTR_DEBUG_LEVEL
#if (FD_PTR_DEBUG_LEVEL>2)
#define FD_DEBUG_BADPTRP(x) (FD_EXPECT_FALSE(!(FD_CHECK_PTR(x))))
#elif (FD_PTR_DEBUG_LEVEL==2)
#define FD_DEBUG_BADPTRP(x) (FD_EXPECT_FALSE(!(FD_CHECK_ATOMIC_PTR(x))))
#elif (FD_PTR_DEBUG_LEVEL==1)
#define FD_DEBUG_BADPTRP(x) (FD_EXPECT_FALSE((x)==FD_NULL))
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

static MAYBE_UNUSED fdtype _fd_check_ptr(fdtype x,u8_context cxt) {
  if (FD_DEBUG_BADPTRP(x)) _fd_bad_pointer(x,cxt);
  return x;
}

FD_EXPORT int fd_ptr_debug_density;

#if (FD_PTR_DEBUG_DENSITY<0)
/* When the check level is negative, use a runtime variable. */
#define FD_CHECK_PTR1(x,cxt) ((fd_ptr_debug_density>0) ? (_fd_check_ptr(x,((u8_context)cxt))) : (x))
#define FD_PTR_CHECK1(x,cxt)						\
  {if ((fd_ptr_debug_density>0) && (FD_DEBUG_BADPTRP(x))) _fd_bad_pointer(x,((u8_context)cxt));}
#define FD_CHECK_PTR2(x,cxt) ((fd_ptr_debug_density>1) ? (_fd_check_ptr(x,((u8_context)cxt))) : (x))
#define FD_PTR_CHECK2(x,cxt)						\
  {if ((fd_ptr_debug_density>1) && (FD_DEBUG_BADPTRP(x))) _fd_bad_pointer(x,((u8_context)cxt));}
#define FD_CHECK_PTR3(x,cxt) ((fd_ptr_debug_density>2) ? (_fd_check_ptr(x,((u8_context)cxt))) : (x))
#define FD_PTR_CHECK3(x,cxt)						\
  {if ((fd_ptr_debug_density>2) && (FD_DEBUG_BADPTRP(x))) _fd_bad_pointer(x,((u8_context)cxt));}
#else
#if (FD_PTR_DEBUG_DENSITY > 0)
#define FD_CHECK_PTR1(x,cxt) (_fd_check_ptr((x),((u8_context)cxt)))
#define FD_PTR_CHECK1(x,cxt)			\
  {if (FD_DEBUG_BADPTRP(x)) _fd_bad_pointer(x,((u8_context)cxt));}
#endif
#if (FD_PTR_DEBUG_DENSITY > 1)
#define FD_CHECK_PTR2(x,cxt) (_fd_check_ptr((x),((u8_context)cxt)))
#define FD_PTR_CHECK2(x,cxt)			\
  {if (FD_DEBUG_BADPTRP(x)) _fd_bad_pointer(x,((u8_context)cxt));}
#endif
#if (FD_PTR_DEBUG_DENSITY > 2)
#define FD_CHECK_PTR3(x,cxt) (_fd_check_ptr((x),((u8_context)cxt)))
#define FD_PTR_CHECK3(x,cxt)			\
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

#endif /* ndef FDB_PTR_H */

