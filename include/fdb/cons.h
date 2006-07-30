/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

/* Anatomy of a CONS

    CONSES are dynamically allocated structures used by fdb for compound
    objects.  CONSES can be any kind of structure, providing only that the
    first four bytes, an unsigned int called the "consbits," be reserved
    for FDB's typing and reference counting information.  The lower seven
    bits of this header contain type information; the reset contain a
    reference count which is used for reclaiming structures which are no
    longer needed.

    A reference counting garbage collector has known problems with
    circular structures but fdb discourages such structures while
    providing a simple model which an effectively scale to large memory
    spaces.

    The consbits consist of a seven bit type field and 25 bit reference
    count field, which allows for about 32 million inbound references and
    this limit has not been a problem to date.  Two reference count values
    have special meanings:
      * a reference count of 0 means that the structure
        is not dynamically allocated and shouldn't be reference counted.  It
        is mostly used when a structure is allocated on the stack or with a
        static top-level declaration.
      * a reference count of xx (the maximnum) indicates that the 
         CONS has been freed and is no longer in use.  Attempting to
         increment or decrement such a reference count yields an error.
    Because the reference count is in the high 30 bits, referencing and
    dereferencing actually increments or decrements the consbits by
    4, after checking for the boundary cases of a static CONS (zero reference
    count) or freed cons (maximal reference count).
    
    All lowercase functions and macros are written to record a reference
    for their return values but not record or consume references for their
    parameters.  That this means practically is that if you get a dtype
    pointer from a lower-cased function, you need to either dereference it
    (with FD_DECREF) or use another upper-case consuming operation on it
    (such as FD_ADD_TO_CHOICE (see <fdb/choices.h>)).
    
    Because the immediate type information on a dtype pointer distinguishes
    between CONSes and other types (which don't need to be reference
    counted), determining whether a value needs to be reference counted is
    very cheap.
    
    MULTI THREADING: Reference counting is made threadsafe by using global
    mutexes to protect access to the consbits fields.  In order to reduce
    contention, fdb uses a strategy called "hash locking" to regulate
    access to the consbits of CONSes.  An array of mutexes, _fd_ptr_locks,
    and computes an offset into that array by shifting the structure
    address right 16 bits and taking a remainder modulo the number of
    pointer locks (32 by default).  The shift size was chosen after a
    little experimentation but different architectures may suggest
    different sizes (though they all should be greater than two due to
    structure word-alignment).

    There are two special kinds of CONSes that produce further
    extensibility.  "Wrapped conses" (fd_wrapped_cons_type) have a second
    field which is a pointer to a FD_TYPEINFO struct providing methods for
    standard operations like displaying (unparsing), recycling, comparing,
    etc. "Compund conses"....

*/

/* Working with CONSes */

#ifndef FDB_CONS_H
#define FDB_CONS_H 1
#define FDB_CONS_H_VERSION "$Id$"

#include "ptr.h"
#include <libu8/u8timefns.h>

FD_EXPORT fd_exception fd_MallocFailed;
FD_EXPORT fd_exception fd_DoubleGC, fd_UsingFreedCons, fd_FreeingNonHeapCons;

#define FD_GET_CONS(x,typecode,typecast) \
  ((FD_PRIM_TYPEP(x,typecode)) ? ((typecast)(FD_CONS_DATA(x))) : \
   ((typecast)(u8_raise(fd_TypeError,fd_type_names[typecode],NULL),NULL)))
#define FD_STRIP_CONS(x,typecode,typecast) ((typecast)(FD_CONS_DATA(x)))
#define FD_CHECK_TYPE_THROW(x,typecode) \
  if (FD_EXPECT_FALSE(!((FD_CONS_TYPE(x)) == typecode))) \
    u8_raise(fd_TypeError,fd_type_names[typecode],NULL)
#define FD_CHECK_TYPE_RET(x,typecode) \
  if (FD_EXPECT_FALSE(!((FD_CONS_TYPE(x)) == typecode))) { \
    fd_seterr(fd_TypeError,fd_type_names[typecode],NULL,(fdtype)x); \
    return -1;}
#define FD_CHECK_TYPE_RETDTYPE(x,typecode) \
  if (FD_EXPECT_FALSE(!((FD_CONS_TYPE(x)) == typecode))) \
    return fd_err(fd_TypeError,fd_type_names[typecode],NULL,(fdtype)x);

/* Hash locking for pointers */

#if FD_THREADS_ENABLED
FD_EXPORT u8_mutex _fd_ptr_locks[FD_N_PTRLOCKS];
#define FD_PTR_LOCK_OFFSET(ptr) \
 ((((unsigned long)ptr)>>16)%FD_N_PTRLOCKS)
#define FD_LOCK_PTR(ptr) \
  fd_lock_mutex(&_fd_ptr_locks[FD_PTR_LOCK_OFFSET(ptr)])
#define FD_UNLOCK_PTR(ptr) \
  fd_unlock_mutex(&_fd_ptr_locks[FD_PTR_LOCK_OFFSET(ptr)])
#else
#define FD_LOCK_PTR(ptr)
#define FD_UNLOCK_PTR(ptr)
#endif

/* Reference counting GC */

#define FD_CONSBITS(x) (x->consbits)
#define FD_CONS_REFCOUNT(x) ((x->consbits)>>7)
#define FD_STACK_CONSP(x) (((x->consbits)>>7)==0)
#define FD_MALLOCD_CONSP(x) (((x->consbits)>>7)!=0)

FD_EXPORT void fd_recycle_cons(struct FD_CONS *);
FD_EXPORT fdtype fd_copy(fdtype x);
FD_EXPORT fdtype fd_deep_copy(fdtype x);

static fdtype _fd_incref(struct FD_CONS *x) 
{
  FD_LOCK_PTR(x);
  if (FD_CONSBITS(x)>0xFFFFFF80) {
    FD_UNLOCK_PTR(x);
    u8_raise(fd_UsingFreedCons,"fd_incref",NULL);}
  else if (FD_CONSBITS(x)>=0x80) {
    x->consbits=x->consbits+0x80;
    FD_UNLOCK_PTR(x);
    return (fdtype) x;}
  else {
    FD_UNLOCK_PTR(x);    
    return fd_copy((fdtype)x);}
}

static void _fd_decref(struct FD_CONS *x) 
{
  FD_LOCK_PTR(x);
  if (FD_CONSBITS(x)>=0xFFFFFF80) {
    FD_UNLOCK_PTR(x);
    u8_raise(fd_DoubleGC,"fd_decref",NULL);}
  else if (FD_CONSBITS(x)>=0x100) {
    x->consbits=x->consbits-0x80;
    FD_UNLOCK_PTR(x);}
  else if (FD_CONSBITS(x)>=0x80) {
    x->consbits=(0xFFFFFF80|(x->consbits&0x7F));
    FD_UNLOCK_PTR(x);
    fd_recycle_cons(x);}
  else {
    FD_UNLOCK_PTR(x);
    u8_raise(fd_FreeingNonHeapCons,"fd_decref",NULL);}
}

#define fd_incref(x) \
   ((FD_PTR_MANIFEST_TYPE(x)) ? (x) : (_fd_incref(FD_CONS_DATA(x))))
#define fd_decref(x) \
   ((FD_PTR_MANIFEST_TYPE(x)) ? (x) : (_fd_decref(FD_CONS_DATA(x)),FD_VOID))

/* Conses */

struct FD_FREE_CONS {
  FD_CONS_HEADER;
  struct FD_FREE_CONS *next;};

struct FD_WRAPPER {
  FD_CONS_HEADER;
  void * data;};

/* Strings */

typedef struct FD_STRING {
  FD_CONS_HEADER;
  unsigned int length;
  u8_string bytes;} FD_STRING;
typedef struct FD_STRING *fd_string;

#define FD_STRINGP(x) (FD_PRIM_TYPEP(x,fd_string_type))
#define FD_STRLEN(x) \
  ((FD_STRIP_CONS(x,fd_string_type,struct FD_STRING *))->length)
#define FD_STRDATA(x) \
  ((FD_STRIP_CONS(x,fd_string_type,struct FD_STRING *))->bytes)
#define FD_STRING_LENGTH(x) \
  ((FD_STRIP_CONS(x,fd_string_type,struct FD_STRING *))->length)
#define FD_STRING_DATA(x) \
  ((FD_STRIP_CONS(x,fd_string_type,struct FD_STRING *))->bytes)

#define FD_XSTRING(x) (FD_GET_CONS(x,fd_string_type,struct FD_STRING *))
#define fd_strlen(x) ((FD_GET_CONS(x,fd_string_type,struct FD_STRING *))->length)
#define fd_strdata(x) ((FD_GET_CONS(x,fd_string_type,struct FD_STRING *))->bytes)

FD_EXPORT fdtype fd_extract_string
  (struct FD_STRING *ptr,u8_byte *start,u8_byte *end);
FD_EXPORT fdtype fd_init_string
  (struct FD_STRING *ptr,int slen,u8_string string);
FD_EXPORT fdtype fdtype_string(u8_string string);

#define FD_PACKETP(x) (FD_PTR_TYPE(x) == fd_packet_type)
#define FD_PACKET_LENGTH(x) \
  ((FD_STRIP_CONS(x,fd_string_type,struct FD_STRING *))->length)
#define FD_PACKET_DATA(x) \
  ((FD_STRIP_CONS(x,fd_string_type,struct FD_STRING *))->bytes)
#define FD_PACKET_REF(x,i) \
  ((FD_STRIP_CONS(x,fd_string_type,struct FD_STRING *))->bytes[i])

FD_EXPORT fdtype fd_init_packet
  (struct FD_STRING *ptr,int len,unsigned char *data);

#define FD_XPACKET(x) (FD_GET_CONS(x,fd_packet_type,struct FD_STRING *))

/* Pairs */

typedef struct FD_PAIR {
  FD_CONS_HEADER;
  fdtype car;
  fdtype cdr;} FD_PAIR;
typedef struct FD_PAIR *fd_pair;

#define FD_PAIRP(x) (FD_PTR_TYPE(x) == fd_pair_type)
#define FD_CAR(x) \
  ((FD_STRIP_CONS(x,fd_pair_type,struct FD_PAIR *))->car)
#define FD_CDR(x) \
  ((FD_STRIP_CONS(x,fd_pair_type,struct FD_PAIR *))->cdr)
#define FD_TRY_CAR(x) \
  ((FD_PAIRP(x)) ? \
   ((FD_STRIP_CONS(x,fd_pair_type,struct FD_PAIR *))->car) : \
   (FD_VOID))
#define FD_TRY_CDR(x) \
  ((FD_PAIRP(x)) ? \
   ((FD_STRIP_CONS(x,fd_pair_type,struct FD_PAIR *))->cdr) : \
   (FD_VOID))
#define FD_CADR(x) (FD_CAR(FD_CDR(x)))

#define fd_car(x) \
  fd_incref(((FD_GET_CONS(x,fd_pair_type,struct FD_PAIR *))->car))
#define fd_cdr(x) \
  fd_incref(((FD_GET_CONS(x,fd_pair_type,struct FD_PAIR *))->cdr))

#define FD_DOLIST(x,list) \
  fdtype x, _tmp=list; \
  while ((FD_PAIRP(_tmp)) ? \
         (x=FD_CAR(_tmp),_tmp=FD_CDR(_tmp),1) : 0)

FD_EXPORT fdtype fd_init_pair(struct FD_PAIR *ptr,fdtype car,fdtype cdr);
FD_EXPORT fdtype fd_make_pair(fdtype car,fdtype cdr);
FD_EXPORT fdtype fd_make_list(int len,...);

#define FD_XPAIR(x) (FD_GET_CONS(x,fd_pair_type,struct FD_PAIR *))

/* Vectors */

typedef struct FD_VECTOR {
  FD_CONS_HEADER;
  unsigned int length;
  fdtype *data;} FD_VECTOR;
typedef struct FD_VECTOR *fd_vector;

#define FD_VECTOR_LENGTH_MASK 0x7FFFFFFF
#define FD_VECTORP(x) (FD_PRIM_TYPEP(x,fd_vector_type))
#define FD_VECTOR_BITS(x) \
  ((FD_STRIP_CONS(x,fd_string_type,struct FD_VECTOR *))->length)
#define FD_VECTOR_LENGTH(x) \
  ((FD_VECTOR_BITS(x))&FD_VECTOR_LENGTH_MASK)
#define FD_ATOMIC_VECTORP(x) \
  ((FD_VECTOR_BITS(x))&0x80000000)
#define FD_VECTOR_DATA(x) \
  ((FD_STRIP_CONS(x,fd_vector_type,struct FD_VECTOR *))->data)
#define FD_VECTOR_REF(x,i) \
  ((FD_STRIP_CONS(x,fd_vector_type,struct FD_VECTOR *))->data[i])
#define FD_VECTOR_SET(x,i,v) \
  ((FD_STRIP_CONS(x,fd_vector_type,struct FD_VECTOR *))->data[i]=v)

FD_EXPORT fdtype fd_init_vector(struct FD_VECTOR *ptr,int len,fdtype *data);
FD_EXPORT fdtype fd_make_vector(int len,...);

#define FD_XVECTOR(x) (FD_GET_CONS(x,fd_vector_type,struct FD_VECTOR *))

/* Compounds */

typedef struct FD_COMPOUND {
  FD_CONS_HEADER;
  fdtype tag;
  fdtype data;} FD_COMPOUND;
typedef struct FD_COMPOUND *fd_compound;

#define FD_COMPOUNDP(x) (FD_PTR_TYPE(x) == fd_compound_type)
#define FD_COMPOUND_TAG(x) \
  ((FD_GET_CONS(x,fd_compound_type,struct FD_COMPOUND *))->tag)
#define FD_COMPOUND_DATA(x) \
  ((FD_GET_CONS(x,fd_compound_type,struct FD_COMPOUND *))->data)

FD_EXPORT fdtype fd_init_compound
  (struct FD_COMPOUND *ptr,fdtype tag,fdtype data);

/* BIG INTs */

#define FD_BIGINTP(x) (FD_PRIM_TYPEP(x,fd_bigint_type))

typedef struct FD_BIGINT *fd_bigint;

FD_EXPORT fdtype fd_make_bigint(long long);

FD_EXPORT int fd_small_bigintp(fd_bigint bi);
FD_EXPORT int fd_bigint2int(fd_bigint bi);
FD_EXPORT unsigned int fd_bigint2uint(fd_bigint bi);
FD_EXPORT long fd_bigint_to_long(fd_bigint bi);
FD_EXPORT long long fd_bigint_to_long_long(fd_bigint bi);
FD_EXPORT fd_bigint fd_long_long_to_bigint(long long);
FD_EXPORT fd_bigint fd_long_to_bigint(long);

#define fd_getint(x) \
  ((FD_FIXNUMP(x)) ? (FD_FIX2INT(x)) : \
   ((FD_PRIM_TYPEP(x,fd_bigint_type)) && (fd_small_bigintp((fd_bigint)x))) ? \
   (fd_bigint2int((fd_bigint)x)) : (0))

/* Doubles */

typedef struct FD_DOUBLE {
  FD_CONS_HEADER;
  double flonum;} FD_DOUBLE;
typedef struct FD_DOUBLE *fd_double;

#define FD_FLONUMP(x) (FD_PRIM_TYPEP(x,fd_double_type))
#define FD_XFLONUM(x) (FD_GET_CONS(x,fd_double_type,struct FD_DOUBLE *))
#define FD_FLONUM(x) ((FD_XFLONUM(x))->flonum)

/* Rational and complex numbers */

typedef struct FD_RATIONAL {
  FD_CONS_HEADER;
  fdtype numerator;
  fdtype denominator;} FD_RATIONAL;
typedef struct FD_RATIONAL *fd_rational;

#define FD_RATIONALP(x) (FD_PTR_TYPE(x) == fd_rational_type)
#define FD_NUMERATOR(x) \
  ((FD_GET_CONS(x,fd_rational_type,struct FD_RATIONAL *))->numerator)
#define FD_DENOMINATOR(x) \
  ((FD_GET_CONS(x,fd_rational_type,struct FD_RATIONAL *))->denominator)

typedef struct FD_COMPLEX {
  FD_CONS_HEADER;
  fdtype realpart;
  fdtype imagpart;} FD_COMPLEX;
typedef struct FD_COMPLEX *fd_complex;

#define FD_COMPLEXP(x) (FD_PTR_TYPE(x) == fd_complex_type)
#define FD_REALPART(x) \
  ((FD_GET_CONS(x,fd_complex_type,struct FD_COMPLEX *))->realpart)
#define FD_IMAGPART(x) \
  ((FD_GET_CONS(x,fd_complex_type,struct FD_COMPLEX *))->imagpart)


/* Mysteries */

typedef struct FD_MYSTERY {
  FD_CONS_HEADER;
  unsigned char package, code; unsigned int size;
  union {
    fdtype *vector; unsigned char *packet;} payload;} FD_MYSTERY;
typedef struct FD_MYSTERY *fd_mystery;

/* Exceptions */

typedef struct FD_EXCEPTION_OBJECT {
  FD_CONS_HEADER;
  struct FD_ERRDATA data;
  fdtype backtrace;} FD_EXCEPTION_OBJECT;
typedef struct FD_EXCEPTION_OBJECT *fd_exception_object;

FD_EXPORT fdtype fd_make_exception
  (fd_exception,u8_context,u8_string,fdtype,fdtype);
FD_EXPORT fdtype fd_err(fd_exception,u8_context,u8_string,fdtype);
FD_EXPORT fdtype fd_passerr(fdtype err,fdtype context);
FD_EXPORT fdtype fd_passerr2(fdtype err,fdtype context,fdtype context2);
FD_EXPORT fdtype fd_type_error(u8_string,u8_context,fdtype);

#define FD_EXCEPTIONP(x) (FD_PRIM_TYPEP(x,fd_exception_type))

#define FD_ABORTP(x) \
  (FD_EXPECT_FALSE((FD_TROUBLEP(x)) || (FD_PRIM_TYPEP(x,fd_exception_type))))


/* Timestamps */

typedef struct FD_TIMESTAMP {
  FD_CONS_HEADER;
  struct U8_XTIME xtime;} FD_TIMESTAMP;
typedef struct FD_TIMESTAMP *fd_timestamp;

/* Compounds */

typedef fdtype (*fd_compound_parsefn)(FD_MEMORY_POOL_TYPE *p,int n,fdtype *);
typedef fdtype (*fd_compound_dumpfn)(fdtype);
typedef fdtype (*fd_compound_restorefn)
      (FD_MEMORY_POOL_TYPE *p,fdtype,fdtype);

typedef struct FD_COMPOUND_ENTRY {
  fdtype tag;
  fd_compound_parsefn parser;
  fd_compound_dumpfn dump;
  fd_compound_restorefn restore;
  struct FD_TABLEFNS *tablefns;
  struct FD_COMPOUND_ENTRY *next;} FD_COMPOUND_ENTRY;
typedef struct FD_COMPOUND_ENTRY *fd_compound_entry;
FD_EXPORT struct FD_COMPOUND_ENTRY *fd_compound_entries;

FD_EXPORT struct FD_COMPOUND_ENTRY *fd_register_compound(fdtype);
FD_EXPORT struct FD_COMPOUND_ENTRY *fd_lookup_compound(fdtype);

/* Choices and tables */

#include "choices.h"
#include "tables.h"

#endif /* ndef FDB_CONS_H */
