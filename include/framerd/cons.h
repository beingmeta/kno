/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Anatomy of a CONS

    CONSES are dynamically allocated structures used by fdb for compound
    objects.  CONSES can be any kind of structure, providing only that the
    first four bytes, an unsigned int called the "fd_conshead," be reserved
    for FramerD's typing and reference counting information.  The lower seven
    bits of this header contain type information; the reset contain a
    reference count which is used for reclaiming structures which are no
    longer needed.

    A reference counting garbage collector has known problems with
    circular structures but fdb discourages such structures while
    providing a simple model which an effectively scale to large memory
    spaces.

    The fd_conshead consist of a seven bit type field and 25 bit reference
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
    dereferencing actually increments or decrements the fd_conshead by
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
    mutexes to protect access to the fd_conshead fields.  In order to reduce
    contention, fdb uses a strategy called "hash locking" to regulate
    access to the fd_conshead of CONSes.  An array of mutexes, _fd_ptr_locks,
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

#ifndef FRAMERD_CONS_H
#define FRAMERD_CONS_H 1
#ifndef FRAMERD_CONS_H_INFO
#define FRAMERD_CONS_H_INFO "include/framerd/cons.h"
#endif

#include "ptr.h"
#include <libu8/u8timefns.h>

FD_EXPORT fd_exception fd_MallocFailed, fd_StringOverflow, fd_StackOverflow;
FD_EXPORT fd_exception fd_DoubleGC, fd_UsingFreedCons, fd_FreeingNonHeapCons;

#define FD_GET_CONS(x,typecode,cast)					\
  ((FD_EXPECT_TRUE(FD_TYPEP(x,typecode))) ?				\
   ((cast)(FD_CONS_DATA(x))) :						\
   ((cast)(fd_err(fd_TypeError,fd_type_names[typecode],NULL,x),NULL)))

#define FD_STRIP_CONS(x,typecode,typecast) ((typecast)(FD_CONS_DATA(x)))

#define FD_CHECK_TYPE_THROW(x,typecode)			 \
  if (FD_EXPECT_FALSE(!((FD_CONS_TYPE(x)) == typecode))) \
    u8_raise(fd_TypeError,fd_type_names[typecode],NULL)

#define FD_CHECK_TYPE_RET(x,typecode)				    \
  if (FD_EXPECT_FALSE(!((FD_CONS_TYPE(x)) == typecode))) {	    \
    fd_xseterr(fd_TypeError,fd_type_names[typecode],NULL,(fdtype)x); \
    return -1;}

#define FD_CHECK_TYPE_RETDTYPE(x,typecode)				\
  if (FD_EXPECT_FALSE(!((FD_CONS_TYPE(x)) == typecode)))		\
    return fd_err(fd_TypeError,fd_type_names[typecode],NULL,(fdtype)x);

#define type2name(tc) \
   ((tc<0)?((u8_string)"oddtype"):(fd_type_names[(short)tc]))

#define FD_PTR2CONS(x,typecode,typecast)                                \
  (((typecode<0) || (FD_TYPEP(x,typecode))) ?                       \
   ((typecast)(FD_CONS_DATA(x))) :					\
   ((typecast)(u8_raise(fd_TypeError,type2name(typecode),NULL),		\
                NULL)))

/* Hash locking for pointers */

#if (SIZEOF_VOID_P == 8)
#define FD_PTRHASH_CONSTANT 11400714819323198549ul
#else
#define FD_PTRHASH_CONSTANT 2654435761
#endif

U8_INLINE U8_MAYBE_UNUSED u8_int8 hashptrval(void *ptr,unsigned int mod) 
{
  u8_wideint intrep = ((u8_wideint) ptr)>>4;
  return (intrep * FD_PTRHASH_CONSTANT) % mod;
}

#if FD_THREADS_ENABLED
FD_EXPORT u8_mutex _fd_ptr_locks[FD_N_PTRLOCKS];
#define FD_PTR_LOCK_OFFSET(ptr) (hashptrval((ptr),FD_N_PTRLOCKS))
#define FD_LOCK_PTR(ptr) \
  fd_lock_mutex(&_fd_ptr_locks[FD_PTR_LOCK_OFFSET(ptr)])
#define FD_UNLOCK_PTR(ptr) \
  fd_unlock_mutex(&_fd_ptr_locks[FD_PTR_LOCK_OFFSET(ptr)])
#else
#define FD_LOCK_PTR(ptr)
#define FD_UNLOCK_PTR(ptr)
#endif

/* Reference counting GC */

#define FD_CONSBITS(x)      ((x)->fd_conshead)
#define FD_CONS_REFCOUNT(x) (((x)->fd_conshead)>>7)
#define FD_STATIC_CONSP(x)  ((FD_CONSBITS(x)&0x80)==0)
#define FD_MALLOCD_CONSP(x) ((FD_CONSBITS(x))>=0x80)

#define FD_STATICP(x) ((!(FD_CONSP(x)))||(FD_STATIC_CONSP((fd_cons)x)))

#define FD_MALLOCD_CONS 0
#define FD_STACK_CONS   1

FD_EXPORT void fd_recycle_cons(struct FD_RAW_CONS *);
FD_EXPORT fdtype fd_copy(fdtype x);
FD_EXPORT fdtype fd_copier(fdtype x,int flags);
FD_EXPORT fdtype fd_deep_copy(fdtype x);
FD_EXPORT fdtype fd_static_copy(fdtype x);

FD_EXPORT fdtype *fd_copy_vec(fdtype *vec,int n,fdtype *into,int copy_flags);
FD_EXPORT void fd_free_vec(fdtype *vec,int n,int free_vec);

#define FD_DEEP_COPY 2   /* Make a deep copy */
#define FD_FULL_COPY 4   /* Copy non-static objects */
#define FD_STRICT_COPY 8 /* Require methods for all objects */
#define FD_STATIC_COPY 16 /* Declare all copied objects static (this leaks) */

/*  Defining this causes a warning to be issued whenever a
     reference count passes HUGE_REFCOUNT.  This is helpful
     for rare occasions of reference count debugging.
*/
#ifndef HUGE_REFCOUNT
#define HUGE_REFCOUNT 0
#endif

/* The consheader is a 32 bit value:
   The lower 7 bits are a cons typecode
   The remaining bits are a reference count, which is always >0
   A zero reference count indicates a static CONS, which
   is never reference counted.
*/

#if (!(FD_NO_GC))
FD_INLINE_FCN fdtype _fd_incref(struct FD_RAW_CONS *x)
{
  FD_LOCK_PTR(x);
  if (FD_CONSBITS(x)>0xFFFFFF80) {
    FD_UNLOCK_PTR(x);
    u8_raise(fd_UsingFreedCons,"fd_incref",NULL);
    return (fdtype)NULL;}
  else if ((FD_CONSBITS(x)&(~0x7F)) == 0) {
    /* Static cons */
    return (fdtype) x;}
  else {
#if HUGE_REFCOUNT
    if ((FD_CONS_REFCOUNT(x))==HUGE_REFCOUNT)
      u8_log(LOG_WARN,"HUGEREFCOUNT","Huge refcount for %lx",x);
#endif
    x->fd_conshead=x->fd_conshead+0x80;
    FD_UNLOCK_PTR(x);
    return (fdtype) x;}
}

FD_INLINE_FCN fdtype _fd_getref(struct FD_RAW_CONS *x)
{
  FD_LOCK_PTR(x);
  if (FD_CONSBITS(x)>0xFFFFFF80) {
    FD_UNLOCK_PTR(x);
    u8_raise(fd_UsingFreedCons,"fd_incref",NULL);
    return (fdtype)NULL;}
  else if ((FD_CONSBITS(x)&(~0x7F)) == 0) {
    /* Static cons */
    return fd_deep_copy((fdtype)x);}
  else {
#if HUGE_REFCOUNT
    if ((FD_CONS_REFCOUNT(x))==HUGE_REFCOUNT)
      u8_log(LOG_WARN,"HUGEREFCOUNT","Huge refcount for %lx",x);
#endif
    x->fd_conshead=x->fd_conshead+0x80;
    FD_UNLOCK_PTR(x);
    return (fdtype) x;}
}

FD_INLINE_FCN void _fd_decref(struct FD_RAW_CONS *x)
{
  FD_LOCK_PTR(x);
  if (FD_CONSBITS(x)>=0xFFFFFF80) {
    FD_UNLOCK_PTR(x);
    u8_raise(fd_DoubleGC,"fd_decref",NULL);}
  else if ((FD_CONSBITS(x)&(~0x7F)) == 0) {
    /* Static cons */}
  else if (FD_CONSBITS(x)>=0x100) {
    x->fd_conshead=x->fd_conshead-0x80;
    FD_UNLOCK_PTR(x);}
  else if (FD_CONSBITS(x)>=0x80) {
    FD_UNLOCK_PTR(x);
    fd_recycle_cons(x);}
  else {}
}

#define fd_incref(x) \
   ((FD_PTR_MANIFEST_TYPE(x)) ? (x) : (_fd_incref(FD_RAW_CONS(x))))
#define fd_decref(x) \
  ((void)((FD_PTR_MANIFEST_TYPE(x)) ? (FD_VOID) : \
	  (_fd_decref(FD_RAW_CONS(x)),FD_VOID)))
#else
#define fd_incref(x) (x)
#define fd_decref(x) ((void)(x))
#endif

/* Conses */

struct FD_FREE_CONS {
  FD_CONS_HEADER;
  struct FD_FREE_CONS *fd_nextfree;};

struct FD_WRAPPER {
  FD_CONS_HEADER;
  void *fd_wrapped_data;};

/* Strings */

typedef struct FD_STRING {
  FD_CONS_HEADER;
  unsigned int fd_freedata:1;
  unsigned int fd_bytelen:31;
  u8_string fd_bytes;} FD_STRING;
typedef struct FD_STRING *fd_string;

FD_EXPORT ssize_t fd_max_strlen;

#define FD_STRINGP(x) (FD_TYPEP(x,fd_string_type))
#define FD_STRLEN(x) ((unsigned int) ((FD_CONSPTR(fd_string,x))->fd_bytelen))
#define FD_STRDATA(x) ((FD_CONSPTR(fd_string,x))->fd_bytes)
#define FD_STRING_LENGTH(x) (FD_STRLEN(x))
#define FD_STRING_DATA(x) (FD_STRDATA(x))

#define FD_XSTRING(x) (fd_consptr(fd_string,x,fd_string_type))
#define fd_xstring(x) (fd_consptr(fd_string,x,fd_string_type))
#define fd_strlen(x)  (FD_STRLEN(fd_xstring(x)))
#define fd_strdata(x) (FD_STRDATA(fd_xstring(x)))

FD_EXPORT fdtype fd_extract_string
  (struct FD_STRING *ptr,u8_string start,u8_string end);
FD_EXPORT fdtype fd_substring(u8_string start,u8_string end);
FD_EXPORT fdtype fd_init_string
  (struct FD_STRING *ptr,int slen,u8_string string);
FD_EXPORT fdtype fd_make_string
  (struct FD_STRING *ptr,int slen,u8_string string);
FD_EXPORT fdtype fd_block_string(int slen,u8_string string);
FD_EXPORT fdtype fd_conv_string
  (struct FD_STRING *ptr,int slen,u8_string string);
FD_EXPORT fdtype fdtype_string(u8_string string);

#define fd_stream2string(stream) \
  ((((stream)->u8_streaminfo)&(U8_STREAM_OWNS_BUF))?                    \
   (fd_block_string((((stream)->u8_write)-((stream)->u8_outbuf)),      \
                   ((stream)->u8_outbuf))):                             \
   (fd_make_string(NULL,(((stream)->u8_write)-((stream)->u8_outbuf)),  \
                   ((stream)->u8_outbuf))))
#define fd_stream_string(stream) \
  (fd_make_string(NULL,(((stream)->u8_write)-((stream)->u8_outbuf)),   \
                  ((stream)->u8_outbuf)))
#define fdstring(s) (fd_make_string(NULL,-1,(s)))

#define fd_lispstring(s) fd_init_string(NULL,-1,(s))
#define fd_unistring(s) fd_conv_string(NULL,-1,(s))

/* Packets */
/* Packets are blocks of binary data. */

#define FD_PACKETP(x) \
  ((FD_PTR_TYPE(x) == fd_packet_type)||(FD_PTR_TYPE(x) == fd_secret_type))
#define FD_PACKET_LENGTH(x) \
  ((unsigned int) ((FD_CONSPTR(fd_string,x))->fd_bytelen))
#define FD_PACKET_DATA(x) ((FD_CONSPTR(fd_string,x))->fd_bytes)
#define FD_PACKET_REF(x,i) ((FD_CONSPTR(fd_string,x))->fd_bytes[i])

FD_EXPORT fdtype fd_init_packet
  (struct FD_STRING *ptr,int len,const unsigned char *data);
FD_EXPORT fdtype fd_make_packet
  (struct FD_STRING *ptr,int len,const unsigned char *data);
FD_EXPORT fdtype fd_bytes2packet
  (struct FD_STRING *ptr,int len,const unsigned char *data);

#define FD_XPACKET(x) (fd_consptr(struct FD_STRING *,x,fd_packet_type))

/* Symbol tables */

typedef struct FD_SYMBOL_ENTRY {
  struct FD_STRING fd_pname;
  int fd_symid;} FD_SYMBOL_ENTRY;
typedef struct FD_SYMBOL_ENTRY *fd_symbol_entry;
struct FD_SYMBOL_TABLE {
  int table_size;
  struct FD_SYMBOL_ENTRY **fd_symbol_entries;};
FD_EXPORT struct FD_SYMBOL_TABLE fd_symbol_table;

/* Pairs */

typedef struct FD_PAIR {
  FD_CONS_HEADER;
  fdtype fd_car;
  fdtype fd_cdr;} FD_PAIR;
typedef struct FD_PAIR *fd_pair;

#define FD_PAIRP(x) (FD_PTR_TYPE(x) == fd_pair_type)
#define FD_CAR(x) ((FD_CONSPTR(FD_PAIR *,x))->fd_car)
#define FD_CDR(x) ((FD_CONSPTR(FD_PAIR *,x))->fd_cdr)
#define FD_TRY_CAR(x) \
  ((FD_PAIRP(x)) ? ((FD_CONSPTR(FD_PAIR *,x))->fd_car) : (FD_VOID))
#define FD_TRY_CDR(x) \
  ((FD_PAIRP(x)) ? ((FD_CONSPTR(fd_pair,x))->fd_cdr) :	\
   (FD_VOID))
#define FD_CADR(x) (FD_CAR(FD_CDR(x)))

#define fd_refcar(x) \
  fd_incref(((fd_consptr(struct FD_PAIR *,x,fd_pair_type))->fd_car))
#define fd_refcdr(x) \
  fd_incref(((fd_consptr(struct FD_PAIR *,x,fd_pair_type))->fd_cdr))

/* These are not threadsafe and they don't worry about GC either */
#define FD_RPLACA(p,x) ((struct FD_PAIR *)p)->fd_car=x
#define FD_RPLACD(p,x) ((struct FD_PAIR *)p)->fd_cdr=x

#define FD_DOLIST(x,list) \
  fdtype x, _tmp=list; \
  while ((FD_PAIRP(_tmp)) ? \
         (x=FD_CAR(_tmp),_tmp=FD_CDR(_tmp),1) : 0)

FD_EXPORT fdtype fd_init_pair(struct FD_PAIR *ptr,fdtype car,fdtype cdr);
FD_EXPORT fdtype fd_make_pair(fdtype car,fdtype cdr);
FD_EXPORT fdtype fd_make_list(int len,...);
FD_EXPORT fdtype fd_pmake_list(int len,...);
FD_EXPORT int fd_list_length(fdtype l);

#define FD_XPAIR(x) (fd_consptr(struct FD_PAIR *,x,fd_pair_type))
#define fd_conspair(car,cdr) fd_init_pair(NULL,car,cdr)

/* Vectors */

typedef struct FD_VECTOR {
  FD_CONS_HEADER;
  unsigned int fd_freedata:1;
  unsigned int fd_veclen:31;
  fdtype *fd_vecelts;} FD_VECTOR;
typedef struct FD_VECTOR *fd_vector;

#define FD_VECTORP(x) (FD_TYPEP((x),fd_vector_type))
#define FD_VECTOR_LENGTH(x) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_veclen)
#define FD_VECTOR_DATA(x) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_vecelts)
#define FD_VECTOR_ELTS(x) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_vecelts)
#define FD_VECTOR_REF(x,i) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_vecelts[i])
#define FD_VECTOR_SET(x,i,v) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_vecelts[i]=(v))

FD_EXPORT fdtype fd_init_vector(struct FD_VECTOR *ptr,int len,fdtype *data);
FD_EXPORT fdtype fd_make_vector(int len,fdtype *elts);
FD_EXPORT fdtype fd_make_nvector(int len,...);

#define FD_XVECTOR(x) (fd_consptr(struct FD_VECTOR *,x,fd_vector_type))

/* Rails are basically vectors but used for executable code */

#define FD_RAILP(x) (FD_TYPEP((x),fd_rail_type))
#define FD_RAIL_LENGTH(x) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_veclen)
#define FD_RAIL_DATA(x) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_vecelts)
#define FD_RAIL_ELTS(x) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_vecelts)
#define FD_RAIL_REF(x,i) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_vecelts[i])
#define FD_RAIL_SET(x,i,v) \
  ((FD_CONSPTR(fd_vector,(x)))->fd_vecelts[i]=(v))

FD_EXPORT fdtype fd_init_rail(struct FD_VECTOR *ptr,int len,fdtype *data);
FD_EXPORT fdtype fd_make_rail(int len,fdtype *elts);
FD_EXPORT fdtype fd_make_nrail(int len,...);

#define FD_XRAIL(x) (fd_consptr(struct FD_VECTOR *,x,fd_rail_type))

/* Generic-ish iteration macro */

#define FD_DOELTS(evar,seq,counter)              \
  fdtype _seq=seq, evar=FD_VOID, counter=0;      \
  fdtype _scan=FD_VOID, *_elts;                  \
  int _i=0, _islist=0, _lim=0, _ok=0;            \
  if (FD_PAIRP(seq)) {                           \
     _islist=1; _scan=_seq; _ok=1;}              \
  else if ((FD_VECTORP(_seq))||                  \
           (FD_RAILP(_seq))) {			 \
    _lim=FD_VECTOR_LENGTH(_seq);                 \
    _elts=FD_VECTOR_DATA(_seq);                  \
    _ok=1;}                                      \
  else if ((FD_EMPTY_LISTP(_seq))||              \
           (FD_EMPTY_CHOICEP(_seq))) {           \
    _ok=-1;}                                     \
  else u8_log(LOG_WARN,fd_TypeError,             \
              "Not a pair or vector: %q",_seq);  \
  if (_ok<0) {}                                  \
  else if (!(_ok)) {}                            \
  else while (((_islist)?(FD_PAIRP(_scan)):(counter<_lim))? \
	      (evar=(_islist)?(FD_CAR(_scan)):(_elts[_i]),  \
	       _scan=((_islist)?(FD_CDR(_scan)):(FD_VOID)), \
	       counter=_i++,1):				    \
	      (0))

/* Compounds */

typedef struct FD_COMPOUND {
  FD_CONS_HEADER;
  fdtype compound_typetag;
  u8_byte compound_ismutable, compound_isopaque, fd_n_elts;
#if FD_THREADS_ENABLED
  u8_mutex fd_lock;
#endif
  fdtype compound_0;} FD_COMPOUND;
typedef struct FD_COMPOUND *fd_compound;

#define FD_COMPOUNDP(x) (FD_PTR_TYPE(x) == fd_compound_type)
#define FD_COMPOUND_TAG(x) \
  ((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->compound_typetag)
#define FD_COMPOUND_DATA(x) \
  ((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->elt0)
#define FD_COMPOUND_TYPEP(x,tag)                        \
  ((FD_PTR_TYPE(x) == fd_compound_type) && (FD_COMPOUND_TAG(x)==tag))
#define FD_COMPOUND_ELTS(x) \
  (&((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->compound_0))
#define FD_COMPOUND_LENGTH(x) \
  ((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->fd_n_elts)
#define FD_COMPOUND_REF(x,i)                                            \
  ((&((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->compound_0))[i])
#define FD_XCOMPOUND(x) (fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))

FD_EXPORT fdtype fd_init_compound
  (struct FD_COMPOUND *ptr,fdtype tag,u8_byte mutable,short n,...);
FD_EXPORT fdtype fd_init_compound_from_elts
  (struct FD_COMPOUND *p,fdtype tag,u8_byte mutable,short n,fdtype *elts);


FD_EXPORT fdtype fd_compound_descriptor_type;

#define FD_COMPOUND_DESCRIPTORP(x) \
  (FD_COMPOUND_TYPEP(x,fd_compound_descriptor_type))

#define FD_COMPOUND_TYPE_TAG 0
#define FD_COMPOUND_TYPE_SIZE 1
#define FD_COMPOUND_TYPE_FIELDS 2
#define FD_COMPOUND_TYPE_INITFN 3
#define FD_COMPOUND_TYPE_FREEFN 4
#define FD_COMPOUND_TYPE_COMPAREFN 5
#define FD_COMPOUND_TYPE_STRINGFN 6
#define FD_COMPOUND_TYPE_DUMPFN 7
#define FD_COMPOUND_TYPE_RESTOREFN 8

/* UUID */

typedef struct FD_UUID {
  FD_CONS_HEADER;
  unsigned char fd_uuid16[16];} FD_UUID;
typedef struct FD_UUID *fd_uuid;

FD_EXPORT fdtype fd_cons_uuid
   (struct FD_UUID *ptr,
    struct U8_XTIME *xtime,long long nodeid,short clockid);
FD_EXPORT fdtype fd_fresh_uuid(struct FD_UUID *ptr);

/* BIG INTs */

#define FD_BIGINTP(x) (FD_TYPEP(x,fd_bigint_type))

typedef struct FD_BIGINT *fd_bigint;

FD_EXPORT fdtype fd_make_bigint(long long);

FD_EXPORT int fd_small_bigintp(fd_bigint bi);
FD_EXPORT int fd_bigint2int(fd_bigint bi);
FD_EXPORT unsigned int fd_bigint2uint(fd_bigint bi);
FD_EXPORT long fd_bigint_to_long(fd_bigint bi);
FD_EXPORT long long fd_bigint_to_long_long(fd_bigint bi);
FD_EXPORT unsigned long long fd_bigint_to_ulong_long(fd_bigint bi);
FD_EXPORT fd_bigint fd_long_long_to_bigint(long long);
FD_EXPORT fd_bigint fd_long_to_bigint(long);

#define FD_INTEGERP(x) ((FD_FIXNUMP(x))||(FD_BIGINTP(x)))

#define fd_getint(x) \
  ((FD_FIXNUMP(x)) ? (FD_FIX2INT(x)) : \
   ((FD_TYPEP(x,fd_bigint_type)) && (fd_small_bigintp((fd_bigint)x))) ? \
   (fd_bigint2int((fd_bigint)x)) : (0))
#define fd_getint64(x) \
  ((FD_FIXNUMP(x)) ? (FD_FIX2INT(x)) : \
   ((FD_TYPEP(x,fd_bigint_type)) && (fd_modest_bigintp((fd_bigint)x))) ? \
   (fd_bigint2int64((fd_bigint)x)) : (0))

#define FD_ISINT(x) \
  ((FD_FIXNUMP(x)) || \
   ((FD_TYPEP(x,fd_bigint_type)) && \
    (fd_small_bigintp((fd_bigint)x))))
#define FD_ISINT64(x) \
  ((FD_FIXNUMP(x)) || \
   ((FD_TYPEP(x,fd_bigint_type)) && \
    (fd_modest_bigintp((fd_bigint)x))))

/* Doubles */

typedef struct FD_FLONUM {
  FD_CONS_HEADER;
  double fd_dblval;} FD_FLONUM;
typedef struct FD_FLONUM *fd_flonum;

#define FD_FLONUMP(x) (FD_TYPEP(x,fd_flonum_type))
#define FD_XFLONUM(x) (fd_consptr(struct FD_FLONUM *,x,fd_flonum_type))
#define FD_FLONUM(x) ((FD_XFLONUM(x))->fd_dblval)

/* Rational and complex numbers */

typedef struct FD_RATIONAL {
  FD_CONS_HEADER;
  fdtype fd_numerator;
  fdtype fd_denominator;} FD_RATIONAL;
typedef struct FD_RATIONAL *fd_rational;

#define FD_RATIONALP(x) (FD_PTR_TYPE(x) == fd_rational_type)
#define FD_NUMERATOR(x) \
  ((fd_consptr(struct FD_RATIONAL *,x,fd_rational_type))->fd_numerator)
#define FD_DENOMINATOR(x) \
  ((fd_consptr(struct FD_RATIONAL *,x,fd_rational_type))->fd_denominator)

typedef struct FD_COMPLEX {
  FD_CONS_HEADER;
  fdtype fd_realpart;
  fdtype fd_imagpart;} FD_COMPLEX;
typedef struct FD_COMPLEX *fd_complex;

#define FD_COMPLEXP(x) (FD_PTR_TYPE(x) == fd_complex_type)
#define FD_REALPART(x) \
  ((fd_consptr(struct FD_COMPLEX *,x,fd_complex_type))->fd_realpart)
#define FD_IMAGPART(x) \
  ((fd_consptr(struct FD_COMPLEX *,x,fd_complex_type))->fd_imagpart)


/* Parsing regexes */
FD_EXPORT fdtype (*fd_regex_parser)(u8_string src,u8_string opts);

/* Mysteries */

typedef struct FD_MYSTERY_DTYPE {
  FD_CONS_HEADER;
  unsigned char fd_dtpackage, fd_dtcode; unsigned int fd_dtlen;
  union {
    fdtype *fd_dtelts; unsigned char *fd_dtbytes;} fd_mystery_payload;} FD_MYSTERY;
typedef struct FD_MYSTERY_DTYPE *fd_mystery;

/* Exceptions */

typedef struct FD_EXCEPTION_OBJECT {
  FD_CONS_HEADER; u8_exception fd_u8ex;} FD_EXCEPTION_OBJECT;
typedef struct FD_EXCEPTION_OBJECT *fd_exception_object;

FD_EXPORT fdtype fd_make_exception(fd_exception,u8_context,u8_string,fdtype);
FD_EXPORT fdtype fd_init_exception(fd_exception_object,u8_exception);

#define FD_EXCEPTIONP(x) (FD_TYPEP(x,fd_error_type))

/* Timestamps */

typedef struct FD_TIMESTAMP {
  FD_CONS_HEADER;
  struct U8_XTIME fd_u8xtime;} FD_TIMESTAMP;
typedef struct FD_TIMESTAMP *fd_timestamp;

FD_EXPORT fdtype fd_make_timestamp(struct U8_XTIME *tm);
FD_EXPORT fdtype fd_time2timestamp(time_t moment);

/* Compounds */

typedef struct FD_COMPOUND_TYPEINFO *fd_compound_typeinfo;
typedef int (*fd_compound_unparsefn)(u8_output out,fdtype,fd_compound_typeinfo);
typedef fdtype (*fd_compound_parsefn)(int n,fdtype *,fd_compound_typeinfo);
typedef fdtype (*fd_compound_dumpfn)(fdtype,fd_compound_typeinfo);
typedef fdtype (*fd_compound_restorefn)(fdtype,fdtype,fd_compound_typeinfo);

typedef struct FD_COMPOUND_TYPEINFO {
  fdtype compound_typetag, fd_compound_metadata; int fd_compound_corelen;
  fd_compound_parsefn fd_compound_parser;
  fd_compound_unparsefn fd_compound_unparser;
  fd_compound_dumpfn fd_compound_dumpfn;
  fd_compound_restorefn fd_compound_restorefn;
  struct FD_TABLEFNS *fd_compund_tablefns;
  struct FD_COMPOUND_TYPEINFO *fd_compound_nextinfo;} FD_COMPOUND_TYPEINFO;
FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_compound_entries;


FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_lookup_compound(fdtype);
FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_declare_compound(fdtype,fdtype,int);
FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_register_compound(fdtype,fdtype *,int *);

/* Cons compare */

#if FD_INLINE_COMPARE
static int cons_compare(fdtype x,fdtype y)
{
  if (FD_ATOMICP(x))
    if (FD_ATOMICP(y))
      if (x < y) return -1;
      else if (x == y)
	return 0;
      else return 1;
    else return -1;
  else if (FD_ATOMICP(y))
    return 1;
  else {
    fd_ptr_type xtype=FD_PTR_TYPE(x);
    fd_ptr_type ytype=FD_PTR_TYPE(y);
    if (FD_NUMBER_TYPEP(xtype))
      if (FD_NUMBER_TYPEP(ytype))
	return fd_numcompare(x,y);
      else return -1;
    else if (FD_NUMBER_TYPEP(ytype))
      return 1;
    else if (xtype<ytype) return -1;
    else if (xtype>ytype) return 1;
    else switch (xtype) {
      case fd_pair_type: {
	int car_cmp=FD_QCOMPARE(FD_CAR(x),FD_CAR(y));
	if (car_cmp == 0) return (FD_QCOMPARE(FD_CDR(x),FD_CDR(y)));
	else return car_cmp;}
      case fd_string_type: {
	int xlen=FD_STRLEN(x), ylen=FD_STRLEN(y);
	if (xlen>ylen) return 1; else if (xlen<ylen) return -1;
	else return strncmp(FD_STRDATA(x),FD_STRDATA(y),xlen);}
      default:
	return FDTYPE_COMPARE(x,y,FD_COMPARE_QUICK);}}
}
#endif

/* Choices, tables, regexes */

#include "choices.h"
#include "tables.h"
#include "fdregex.h"

#endif /* ndef FRAMERD_CONS_H */
