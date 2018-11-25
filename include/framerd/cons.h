/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Anatomy of a CONS

    CONSES are dynamically allocated structures used by Framer for compound
    objects.  CONSES can be any kind of structure, providing only that the
    first four bytes, an unsigned int called the "conshead," be reserved
    for FramerD's typing and reference counting information.  The lower seven
    bits of this header contain type information; the reset contain a
    reference count which is used for reclaiming structures which are no
    longer needed.

    A reference counting garbage collector has known problems with
    circular structures but Framer discourages such structures while
    providing a simple model which an effectively scale to large memory
    spaces.

    The conshead consist of a seven bit type field and 25 bit reference
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
    dereferencing actually increments or decrements the conshead by
    4, after checking for the boundary cases of a static CONS (zero reference
    count) or freed cons (maximal reference count).

    All lowercase functions and macros are written to record a reference
    for their return values but not record or consume references for their
    parameters.  That this means practically is that if you get a dtype
    pointer from a lower-cased function, you need to either dereference it
    (with FD_DECREF) or use another upper-case consuming operation on it
    (such as FD_ADD_TO_CHOICE (see <include/framerd/choices.h>)).

    Because the immediate type information on a dtype pointer distinguishes
    between CONSes and other types (which don't need to be reference
    counted), determining whether a value needs to be reference counted is
    very cheap.

    MULTI THREADING: Reference counting is made threadsafe by using global
    mutexes to protect access to the conshead fields.  In order to reduce
    contention, FramerC uses a strategy called "hash locking" to regulate
    access to the conshead of CONSes.  An array of mutexes, _fd_ptr_locks,
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

FD_EXPORT u8_condition fd_MallocFailed, fd_StringOverflow, fd_StackOverflow;
FD_EXPORT u8_condition fd_DoubleGC, fd_UsingFreedCons, fd_FreeingNonHeapCons;

#define FD_GET_CONS(x,typecode,cast)                                    \
  ((FD_EXPECT_TRUE(FD_TYPEP(x,typecode))) ?                             \
   ((cast)(FD_CONS_DATA(x))) :                                          \
   ((cast)(fd_err(fd_TypeError,fd_type_names[typecode],NULL,x),NULL)))

#define FD_STRIP_CONS(x,typecode,typecast) ((typecast)(FD_CONS_DATA(x)))

#define FD_CHECK_TYPE_THROW(x,typecode)                  \
  if (FD_EXPECT_FALSE(!((FD_CONS_TYPE(x)) == typecode))) \
    u8_raise(fd_TypeError,fd_type_names[typecode],NULL)

#define FD_CHECK_TYPE_RET(x,typecode)                               \
  if (FD_EXPECT_FALSE(!((FD_CONS_TYPE(x)) == typecode))) {          \
    fd_seterr(fd_TypeError,fd_type_names[typecode],NULL,(lispval)x); \
    return -1;}

#define FD_CHECK_TYPE_RETDTYPE(x,typecode)                              \
  if (FD_EXPECT_FALSE(!((FD_CONS_TYPE(x)) == typecode)))                \
    return fd_err(fd_TypeError,fd_type_names[typecode],NULL,(lispval)x);

#define FD_PTR2CONS(x,typecode,typecast)                            \
  (((typecode<0) || (FD_TYPEP(x,typecode))) ?                       \
   ((typecast)(FD_CONS_DATA(x))) :                                  \
   ((typecast)(u8_raise(fd_TypeError,fd_type2name(typecode),NULL),  \
                NULL)))

#ifndef FD_MAX_REFCOUNT
#define FD_MAX_REFCOUNT 0xFFFFFF
#endif

/* External functions */

FD_EXPORT void _fd_decref_fn(lispval);
FD_EXPORT lispval _fd_incref_fn(lispval);

FD_EXPORT void _FD_INIT_CONS(void *vptr,fd_ptr_type type);
FD_EXPORT void _FD_INIT_FRESH_CONS(void *vptr,fd_ptr_type type);
FD_EXPORT void _FD_INIT_STACK_CONS(void *vptr,fd_ptr_type type);
FD_EXPORT void _FD_INIT_STATIC_CONS(void *vptr,fd_ptr_type type);
FD_EXPORT void _FD_SET_CONS_TYPE(void *vptr,fd_ptr_type type);
FD_EXPORT void _FD_SET_REFCOUNT(void *vptr,unsigned int count);

/* Initializing CONSes */

#if FD_LOCKFREE_REFCOUNTS
#define FD_HEAD_INIT(type) (ATOMIC_VAR_INIT((type-(FD_CONS_TYPE_OFF))|0x80))
#define FD_STATIC_INIT(type) (ATOMIC_VAR_INIT((type-(FD_CONS_TYPE_OFF))))
#else
#define FD_HEAD_INIT(type)   ((type-(FD_CONS_TYPE_OFF))|0x80)
#define FD_STATIC_INIT(type) (type-(FD_CONS_TYPE_OFF))
#endif

#if FD_INLINE_REFCOUNTS
#define FD_INIT_CONS(ptr,type) \
  ((fd_raw_cons)ptr)->conshead = (FD_HEAD_INIT(type))
#define FD_INIT_FRESH_CONS(ptr,type) \
  memset(ptr,0,sizeof(*(ptr))); \
  ((fd_raw_cons)ptr)->conshead = (FD_HEAD_INIT(type))
#define FD_INIT_STACK_CONS(ptr,type) \
  ((fd_raw_cons)ptr)->conshead = (FD_STATIC_INIT(type))
#define FD_INIT_STATIC_CONS(ptr,type) \
  memset(ptr,0,sizeof(*(ptr))); \
  ((fd_raw_cons)ptr)->conshead = (FD_STATIC_INIT(type))
#else
#define FD_INIT_CONS(ptr,type) _FD_INIT_CONS((struct FD_RAW_CONS *)ptr,type)
#define FD_INIT_FRESH_CONS(ptr,type) _FD_INIT_FRESH_CONS((struct FD_RAW_CONS *)ptr,type)
#define FD_INIT_STACK_CONS(ptr,type) _FD_INIT_STACK_CONS((struct FD_RAW_CONS *)ptr,type)
#define FD_INIT_STATIC_CONS(ptr,type) _FD_INIT_STATIC_CONS((struct FD_RAW_CONS *)ptr,type)
#endif

#if FD_INLINE_REFCOUNTS && FD_LOCKFREE_REFCOUNTS
#define FD_CBITS(x) (((fd_ref_cons)x)->conshead)
#define FD_SET_CONS_TYPE(ptr,type) \
  atomic_store                                                  \
  (&(FD_CBITS(ptr)),                                            \
    (((atomic_load(&(FD_CBITS(ptr))))&(~(FD_CONS_TYPE_MASK)))|  \
     ((type-(FD_CONS_TYPE_OFF))&0x7f)))
#define FD_SET_REFCOUNT(ptr,count) \
  atomic_store                                                  \
  (&(FD_CBITS(ptr)),                                            \
    (((atomic_load(&(FD_CBITS(ptr))))&(FD_CONS_TYPE_MASK))|     \
     (count<<7)))
#elif FD_INLINE_REFCOUNTS
#define FD_SET_CONS_TYPE(ptr,type) \
  ((fd_raw_cons)ptr)->conshead=                               \
    ((((fd_raw_cons)ptr)->conshead&(~(FD_CONS_TYPE_MASK)))) | \
    ((type-(FD_CONS_TYPE_OFF))&0x7f)
#define FD_SET_REFCOUNT(ptr,count) \
  ((fd_raw_cons)ptr)->conshead=                                 \
    (((((fd_raw_cons)ptr)->conshead)&(FD_CONS_TYPE_MASK))|      \
     (count<<7))))
#else
#define FD_SET_CONS_TYPE(ptr,type) \
  _FD_SET_CONS_TYPE((struct FD_RAW_CONS *)ptr,type)
#define FD_SET_REFCOUNT(ptr,type) \
  _FD_SET_REFCOUNT((struct FD_RAW_CONS *)ptr,type)
#endif


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

FD_EXPORT u8_mutex _fd_ptr_locks[FD_N_PTRLOCKS];
#define FD_PTR_LOCK_OFFSET(ptr) (hashptrval((ptr),FD_N_PTRLOCKS))
#define FD_LOCK_PTR(ptr) \
  u8_lock_mutex(&_fd_ptr_locks[FD_PTR_LOCK_OFFSET(ptr)])
#define FD_UNLOCK_PTR(ptr) \
  u8_unlock_mutex(&_fd_ptr_locks[FD_PTR_LOCK_OFFSET(ptr)])

/* Reference counting GC */

#if FD_INLINE_REFCOUNTS && FD_LOCKFREE_REFCOUNTS
#define FD_CONSBITS(x)       (atomic_load(&(((fd_ref_cons)x)->conshead)))
#define FD_CONS_REFCOUNT(x)  (FD_CONSBITS(x)>>7)
#elif FD_INLINE_REFCOUNTS
#define FD_CONSBITS(x)       ((x)->conshead)
#define FD_CONS_REFCOUNT(x)  (((x)->conshead)>>7)
#else
#define FD_CONSBITS(x)      ((x)->conshead)
#define FD_CONS_REFCOUNT(x) (((x)->conshead)>>7)
#endif


#define FD_MALLOCD_CONSP(x) ((FD_CONSBITS(x))>=0x80)
#define FD_STATIC_CONSP(x)  (!(FD_MALLOCD_CONSP(x)))

#define FD_STATICP(x) ((!(FD_CONSP(x)))||(FD_STATIC_CONSP((fd_cons)x)))

#define fd_getref(x) \
  ((FD_CONSP(x)) ? \
   ((FD_STATIC_CONSP(x)) ? (fd_deep_copy(x)) : (fd_incref(x)) ) : \
   (x))
#define fd_not_static(x) \
  ((FD_CONSP(x)) ? \
   ((FD_STATIC_CONSP(x)) ? (fd_copy(x)) : (x) ) : \
   (x))

#define FD_MALLOCD_CONS 0
#define FD_STACK_CONS   1

FD_EXPORT void fd_recycle_cons(struct FD_RAW_CONS *);
FD_EXPORT lispval fd_copy(lispval x);
FD_EXPORT lispval fd_copier(lispval x,int flags);
FD_EXPORT lispval fd_deep_copy(lispval x);
FD_EXPORT lispval fd_static_copy(lispval x);

FD_EXPORT lispval *fd_copy_vec(lispval *vec,size_t n,lispval *into,int copy_flags);
FD_EXPORT void fd_incref_vec(lispval *vec,size_t n);
FD_EXPORT void fd_decref_vec(lispval *vec,size_t n);

#define FD_DEEP_COPY 2   /* Make a deep copy */
#define FD_FULL_COPY 4   /* Copy non-static objects */
#define FD_STRICT_COPY 8 /* Require methods for all objects */
#define FD_STATIC_COPY 16 /* Declare all copied objects static (this leaks) */
#define FD_COPY_TERMINALS 32 /* Copy even terminal objects */

/* The consheader is a 32 bit value:
   The lower 7 bits are a cons typecode
   The remaining bits are a reference count, which is always >0
   A zero reference count indicates a static CONS, which
   is never reference counted.
*/

#if ( FD_INLINE_REFCOUNTS && FD_LOCKFREE_REFCOUNTS )
FD_INLINE_FCN lispval _fd_incref(struct FD_REF_CONS *x)
{
  if (FD_EXPECT_FALSE(x == NULL))
    return FD_BADPTR;
  else {
    fd_consbits cb = atomic_load(&(x->conshead));
    if (cb>0xFFFFFF80) {
      u8_raise(fd_UsingFreedCons,"fd_incref",NULL);
      return (lispval)NULL;}
    else if ((cb&(~0x7F)) == 0) {
      /* Static cons */
      return (lispval) x;}
#if FD_MAX_REFCOUNT
    else if ((cb>>7) > FD_MAX_REFCOUNT) {
      atomic_fetch_add(&(x->conshead),0x80);
      _fd_debug((lispval)x);
      return (lispval) x;}
#endif
    else {
      atomic_fetch_add(&(x->conshead),0x80);
      return (lispval) x;}}
}

FD_INLINE_FCN void _fd_decref(struct FD_REF_CONS *x)
{
  if (FD_EXPECT_FALSE(x == NULL)) return;
  fd_consbits cb = atomic_load(&(x->conshead));
  if (cb>=0xFFFFFF80) {
    u8_raise(fd_DoubleGC,"fd_decref",NULL);}
  else if (cb<0x80) {
    /* Static cons */}
  else {
    fd_consbits oldcb=atomic_fetch_sub(&(x->conshead),0x80);
    if ((oldcb>=0x80)&&(oldcb<0x100)) {
      /* If the modified consbits indicated a refcount of 1,
         we've reduced it to zero, so we recycle it. Otherwise,
         someone got in to free it or incref it in the meanwhile. */
      atomic_store(&(x->conshead),((oldcb&0x7F)|0xFFFFFF80));
      fd_recycle_cons((fd_raw_cons)x);}
  }
}

#elif FD_INLINE_REFCOUNTS

FD_INLINE_FCN lispval _fd_incref(struct FD_REF_CONS *x)
{
  if (FD_CONSBITS(x)>0xFFFFFF80) {
    u8_raise(fd_UsingFreedCons,"fd_incref",NULL);
    return (lispval)NULL;}
  else if ((FD_CONSBITS(x)&(~0x7F)) == 0) {
    /* Static cons */
    return (lispval) x;}
  else {
    FD_LOCK_PTR(x);
    x->conshead = x->conshead+0x80;
    FD_UNLOCK_PTR(x);
    return (lispval) x;}
}

FD_INLINE_FCN void _fd_decref(struct FD_REF_CONS *x)
{
  if (FD_CONSBITS(x)>=0xFFFFFF80) {
    u8_raise(fd_DoubleGC,"fd_decref",NULL);}
  else if ((FD_CONSBITS(x)&(~0x7F)) == 0) {
    /* Static cons */}
  else {
    FD_LOCK_PTR(x);
    if (FD_CONSBITS(x)>=0x100) {
      /* If it's still got a refcount > 1, just decrease it */
      x->conshead = x->conshead-0x80;
      FD_UNLOCK_PTR(x);}
    else {
      /* Someone else decref'd it before we got the lock, so we
         unlock and recycle it */
      FD_UNLOCK_PTR(x);
      fd_recycle_cons((struct FD_RAW_CONS *)x);}}
}
#endif

#if FD_INLINE_REFCOUNTS

#define fd_incref(x) \
  ((FD_PTR_MANIFEST_TYPE(x)) ? ((lispval)x) : (_fd_incref(FD_REF_CONS(x))))
#define fd_decref(x) \
  ((void)((FD_PTR_MANIFEST_TYPE(x)) ? (FD_VOID) : \
          (_fd_decref(FD_REF_CONS(x)),FD_VOID)))
#else

#define fd_incref(x) \
   ((FD_PTR_MANIFEST_TYPE(x)) ? (x) : (_fd_incref_fn(x)))
#define fd_decref(x) \
  ((void)((FD_PTR_MANIFEST_TYPE(x)) ? (FD_VOID) : \
          (_fd_decref_fn(x),FD_VOID)))

#endif

/* Incref/decref for vectors */

FD_EXPORT lispval *_fd_init_elts(lispval *elts,size_t n,lispval v);

FD_EXPORT ssize_t _fd_incref_elts(const lispval *elts,size_t n);

FD_EXPORT ssize_t _fd_decref_elts(const lispval *elts,size_t n);

FD_EXPORT ssize_t _fd_free_elts(lispval *elts,size_t n);

#if FRAMERD_SOURCE
FD_FASTOP U8_MAYBE_UNUSED
lispval *fd_init_elts(lispval *elts,ssize_t n,lispval v)
{
  if (elts == NULL) elts=u8_alloc_n(n,lispval);
  if (FD_CONSP(v)) {
    int i=0; while (i<n) { elts[i++]=fd_incref(v); }}
  else {
    int i=0; while (i<n) { elts[i++]=v; }}
  return elts;
}
FD_FASTOP U8_MAYBE_UNUSED
ssize_t fd_incref_elts(const lispval *elts,size_t n)
{
  int i=0, consed=0; while (i<n) {
    lispval elt=elts[i++];
    if ((FD_CONSP(elt))&&(FD_MALLOCD_CONSP((fd_cons)elt))) {
      consed++;
      fd_decref(elt);}}
  return consed;
}

FD_FASTOP U8_MAYBE_UNUSED
size_t fd_decref_elts(const lispval *elts,size_t n)
{
  size_t conses=0;
  int i=0; while (i<n) {
    lispval elt=elts[i++];
    if (FD_CONSP(elt)) {
      fd_decref(elt);
      conses++;}}
  return conses;

}

FD_FASTOP U8_MAYBE_UNUSED
ssize_t fd_free_elts(lispval *elts,size_t n)
{
  size_t conses=0;
  const lispval *scan=(const lispval *)elts;
  int i=0; while (i<n) {
    lispval elt=scan[i++];
    if (FD_CONSP(elt)) {
      fd_decref(elt);
      conses++;}}
  u8_free(elts);
  return conses;
}
#else
#define fd_init_elts _fd_init_elts
#define fd_incref_elts _fd_incref_elts
#define fd_decref_elts _fd_decref_elts
#define fd_free_elts _fd_free_elts
#endif

#define fd_incref_ptr(p) (fd_incref((lispval)(p)))
#define fd_decref_ptr(p) (fd_decref((lispval)(p)))

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
  unsigned int str_freebytes:1;
  unsigned int str_bytelen:31;
  u8_string str_bytes;} FD_STRING;
typedef struct FD_STRING *fd_string;

#define FD_STRING_LEN (sizeof(struct FD_STRING))

FD_EXPORT ssize_t fd_max_strlen;
FD_EXPORT int fd_check_utf8;

#define FD_STRINGP(x) (FD_TYPEP(x,fd_string_type))
#define FD_STRLEN(x) ((unsigned int) ((FD_CONSPTR(fd_string,x))->str_bytelen))
#define FD_STRDATA(x) ((FD_CONSPTR(fd_string,x))->str_bytes)
#define FD_CSTRING(x) ((FD_CONSPTR(fd_string,x))->str_bytes)
#define FD_STRING_LENGTH(x) (FD_STRLEN(x))
#define FD_STRING_DATA(x) (FD_STRDATA(x))

#define FD_XSTRING(x) (fd_consptr(fd_string,x,fd_string_type))
#define fd_xstring(x) (fd_consptr(fd_string,x,fd_string_type))
#define fd_strlen(x)  (FD_STRLEN(fd_xstring(x)))
#define fd_strdata(x) (FD_STRDATA(fd_xstring(x)))

FD_EXPORT lispval fd_extract_string
  (struct FD_STRING *ptr,u8_string start,u8_string end);
FD_EXPORT lispval fd_substring(u8_string start,u8_string end);
FD_EXPORT lispval fd_init_string
  (struct FD_STRING *ptr,int slen,u8_string string);
FD_EXPORT lispval fd_make_string
  (struct FD_STRING *ptr,int slen,u8_string string);
FD_EXPORT lispval fd_block_string(int slen,u8_string string);
FD_EXPORT lispval fd_conv_string
  (struct FD_STRING *ptr,int slen,u8_string string);
FD_EXPORT lispval lispval_string(u8_string string);

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
#define FD_SECRETP(x) \
  (FD_PTR_TYPE(x) == fd_secret_type)
#define FD_PACKET_LENGTH(x) \
  ((unsigned int) ((FD_CONSPTR(fd_string,x))->str_bytelen))
#define FD_PACKET_DATA(x) ((FD_CONSPTR(fd_string,x))->str_bytes)
#define FD_PACKET_REF(x,i) ((FD_CONSPTR(fd_string,x))->str_bytes[i])

FD_EXPORT lispval fd_init_packet
  (struct FD_STRING *ptr,int len,const unsigned char *data);
FD_EXPORT lispval fd_make_packet
  (struct FD_STRING *ptr,int len,const unsigned char *data);
FD_EXPORT lispval fd_bytes2packet
  (struct FD_STRING *ptr,int len,const unsigned char *data);

#define FD_XPACKET(x) (fd_consptr(struct FD_STRING *,x,fd_packet_type))

/* Symbol tables */

typedef struct FD_SYMBOL_ENTRY {
  struct FD_STRING sym_pname;
  int symid;} FD_SYMBOL_ENTRY;
typedef struct FD_SYMBOL_ENTRY *fd_symbol_entry;
struct FD_SYMBOL_TABLE {
  int table_size;
  struct FD_SYMBOL_ENTRY **fd_symbol_entries;};
FD_EXPORT struct FD_SYMBOL_TABLE fd_symbol_table;

/* Pairs */

typedef struct FD_PAIR {
  FD_CONS_HEADER;
  lispval car;
  lispval cdr;} FD_PAIR;
typedef struct FD_PAIR *fd_pair;

#define FD_PAIRP(x) (FD_PTR_TYPE(x) == fd_pair_type)
#define FD_CAR(x) ((FD_CONSPTR(FD_PAIR *,(x)))->car)
#define FD_CDR(x) ((FD_CONSPTR(FD_PAIR *,(x)))->cdr)
#define FD_TRY_CAR(x) \
  ((FD_PAIRP(x)) ? ((FD_CONSPTR(FD_PAIR *,(x)))->car) : (FD_VOID))
#define FD_TRY_CDR(x) \
  ((FD_PAIRP(x)) ? ((FD_CONSPTR(fd_pair,(x)))->cdr) :   \
   (FD_VOID))
FD_FASTOP lispval FD_CADR(lispval x)
{
  if (FD_PAIRP(x)) {
    lispval cdr = FD_CDR(x);
    if (FD_PAIRP(cdr))
      return FD_CAR(cdr);
    else return FD_ERROR_VALUE;}
  else return FD_ERROR_VALUE;
}
FD_FASTOP lispval FD_CDDR(lispval x)
{
  if (FD_PAIRP(x)) {
    lispval cdr = FD_CDR(x);
    if (FD_PAIRP(cdr))
      return FD_CDR(cdr);
    else return FD_ERROR_VALUE;}
  else return FD_ERROR_VALUE;
}
FD_FASTOP lispval FD_CAAR(lispval x)
{
  if (FD_PAIRP(x)) {
    lispval car = FD_CAR(x);
    if (FD_PAIRP(car))
      return FD_CAR(car);
    else return FD_ERROR_VALUE;}
  else return FD_ERROR_VALUE;
}

#define fd_refcar(x) \
  fd_incref(((fd_consptr(struct FD_PAIR *,x,fd_pair_type))->car))
#define fd_refcdr(x) \
  fd_incref(((fd_consptr(struct FD_PAIR *,x,fd_pair_type))->cdr))

/* These are not threadsafe and they don't worry about GC either */
#define FD_SETCAR(p,x) ((struct FD_PAIR *)p)->car = x
#define FD_SETCDR(p,x) ((struct FD_PAIR *)p)->cdr = x

#define FD_RPLACA(p,x) ((struct FD_PAIR *)p)->car = x
#define FD_RPLACD(p,x) ((struct FD_PAIR *)p)->cdr = x

#define FD_DOLIST(x,list) \
  lispval x, _tmp = list; \
  while ((FD_PAIRP(_tmp)) ? \
         (x = FD_CAR(_tmp),_tmp = FD_CDR(_tmp),1) : 0)

FD_EXPORT lispval fd_init_pair(struct FD_PAIR *ptr,lispval car,lispval cdr);
FD_EXPORT lispval fd_make_pair(lispval car,lispval cdr);
FD_EXPORT lispval fd_make_list(int len,...);
FD_EXPORT lispval fd_pmake_list(int len,...);
FD_EXPORT int fd_list_length(lispval l);
FD_EXPORT lispval fd_reverse_list(lispval l);

#define FD_XPAIR(x) (fd_consptr(struct FD_PAIR *,x,fd_pair_type))
#define fd_conspair(car,cdr) fd_init_pair(NULL,car,cdr)

/* Vectors */

typedef struct FD_VECTOR {
  FD_CONS_HEADER;
  unsigned int vec_length:29;
  unsigned int vec_free_elts:1;
  unsigned int vec_bigalloc:1;
  unsigned int vec_bigalloc_elts:1;
  lispval *vec_elts;} FD_VECTOR;
typedef struct FD_VECTOR *fd_vector;

#define FD_VECTOR_LEN (sizeof(struct FD_VECTOR))

FD_EXPORT ssize_t fd_bigvec_threshold;

#define FD_VECTORP(x) (FD_TYPEP((x),fd_vector_type))
#define FD_VECTOR_LENGTH(x) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_length)
#define FD_VECTOR_DATA(x) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_elts)
#define FD_VECTOR_ELTS(x) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_elts)
#define FD_VECTOR_REF(x,i) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_elts[i])
#define FD_VECTOR_SET(x,i,v) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_elts[i]=(v))

FD_EXPORT lispval fd_cons_vector(struct FD_VECTOR *ptr,int len,int big,lispval *data);
FD_EXPORT lispval fd_init_vector(struct FD_VECTOR *ptr,int len,lispval *data);
FD_EXPORT lispval fd_wrap_vector(int len,lispval *data);
FD_EXPORT lispval fd_empty_vector(int len);
FD_EXPORT lispval fd_make_vector(int len,lispval *elts);
FD_EXPORT lispval fd_make_nvector(int len,...);
FD_EXPORT lispval fd_fill_vector(int len,lispval init_elt);

#define FD_XVECTOR(x) (fd_consptr(struct FD_VECTOR *,x,fd_vector_type))

/* Rails are basically vectors but used for executable code */

#define FD_CODEP(x) (FD_TYPEP((x),fd_code_type))
#define FD_CODE_LENGTH(x) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_length)
#define FD_CODE_DATA(x) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_elts)
#define FD_CODE_ELTS(x) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_elts)
#define FD_CODE_REF(x,i) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_elts[i])
#define FD_CODE_SET(x,i,v) \
  ((FD_CONSPTR(fd_vector,(x)))->vec_elts[i]=(v))

FD_EXPORT lispval fd_init_code(struct FD_VECTOR *ptr,int len,lispval *data);
FD_EXPORT lispval fd_make_code(int len,lispval *elts);
FD_EXPORT lispval fd_make_nrail(int len,...);

#define FD_XRAIL(x) (fd_consptr(struct FD_VECTOR *,x,fd_code_type))

/* Generic-ish iteration macro */

#define FD_DOELTS(evar,seq,counter)              \
  lispval _seq = seq, evar = FD_VOID, counter = 0;      \
  lispval _scan = FD_VOID, *_elts;                  \
  int _i = 0, _islist = 0, _lim = 0, _ok = 0;            \
  if (FD_PAIRP(seq)) {                           \
     _islist = 1; _scan=_seq; _ok = 1;}              \
  else if ((FD_VECTORP(_seq))||                  \
           (FD_CODEP(_seq))) {                   \
    _lim = FD_VECTOR_LENGTH(_seq);                 \
    _elts = FD_VECTOR_DATA(_seq);                  \
    _ok = 1;}                                      \
  else if ((FD_EMPTY_LISTP(_seq))||              \
           (FD_EMPTY_CHOICEP(_seq))) {           \
    _ok = -1;}                                     \
  else u8_log(LOG_WARN,fd_TypeError,             \
              "Not a pair or vector: %q",_seq);  \
  if (_ok<0) {}                                  \
  else if (!(_ok)) {}                            \
  else while (((_islist)?(FD_PAIRP(_scan)):(counter<_lim))? \
              (evar = (_islist)?(FD_CAR(_scan)):(_elts[_i]),  \
               _scan = ((_islist)?(FD_CDR(_scan)):(FD_VOID)), \
               counter=_i++,1):                             \
              (0))

/* UUID */

typedef struct FD_UUID {
  FD_CONS_HEADER;
  unsigned char uuid16[16];} FD_UUID;
typedef struct FD_UUID *fd_uuid;

FD_EXPORT lispval fd_cons_uuid
   (struct FD_UUID *ptr,
    struct U8_XTIME *xtime,long long nodeid,short clockid);
FD_EXPORT lispval fd_fresh_uuid(struct FD_UUID *ptr);

/* BIG INTs */

#define FD_BIGINTP(x) (FD_TYPEP(x,fd_bigint_type))

typedef struct FD_BIGINT *fd_bigint;

FD_EXPORT lispval fd_make_bigint(long long);

FD_EXPORT int fd_small_bigintp(fd_bigint bi);
FD_EXPORT int fd_modest_bigintp(fd_bigint bi);
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
  double floval;} FD_FLONUM;
typedef struct FD_FLONUM *fd_flonum;

#define FD_FLONUMP(x) (FD_TYPEP(x,fd_flonum_type))
#define FD_XFLONUM(x) (fd_consptr(struct FD_FLONUM *,x,fd_flonum_type))
#define FD_FLONUM(x) ((FD_XFLONUM(x))->floval)

FD_EXPORT lispval fd_init_flonum(struct FD_FLONUM *ptr,double flonum);
FD_EXPORT lispval fd_init_double(struct FD_FLONUM *ptr,double flonum);

#define fd_make_double(dbl) (fd_init_double(NULL,(dbl)))
#define fd_make_flonum(dbl) (fd_init_flonum(NULL,(dbl)))

/* Rational and complex numbers */

typedef struct FD_RATIONAL {
  FD_CONS_HEADER;
  lispval numerator;
  lispval denominator;} FD_RATIONAL;
typedef struct FD_RATIONAL *fd_rational;

#define FD_RATIONALP(x) (FD_PTR_TYPE(x) == fd_rational_type)
#define FD_NUMERATOR(x) \
  ((fd_consptr(struct FD_RATIONAL *,x,fd_rational_type))->numerator)
#define FD_DENOMINATOR(x) \
  ((fd_consptr(struct FD_RATIONAL *,x,fd_rational_type))->denominator)

typedef struct FD_COMPLEX {
  FD_CONS_HEADER;
  lispval realpart;
  lispval imagpart;} FD_COMPLEX;
typedef struct FD_COMPLEX *fd_complex;

#define FD_COMPLEXP(x) (FD_PTR_TYPE(x) == fd_complex_type)
#define FD_REALPART(x) \
  ((fd_consptr(struct FD_COMPLEX *,x,fd_complex_type))->realpart)
#define FD_IMAGPART(x) \
  ((fd_consptr(struct FD_COMPLEX *,x,fd_complex_type))->imagpart)


/* Parsing regexes */

#include <regex.h>

FD_EXPORT u8_condition fd_RegexError;

typedef struct FD_REGEX {
  FD_CONS_HEADER;
  u8_string rxsrc;
  unsigned int rxactive;
  int rxflags;
  u8_mutex rx_lock;
  regex_t rxcompiled;} FD_REGEX;
typedef struct FD_REGEX *fd_regex;

FD_EXPORT lispval fd_make_regex(u8_string src,int flags);

#define FD_REGEXP(x) (FD_PTR_TYPE(x) == fd_regex_type)

/* Mysteries */

typedef struct FD_MYSTERY_DTYPE {
  FD_CONS_HEADER;
  unsigned char myst_dtpackage, myst_dtcode;
  size_t myst_dtsize;
  union {
    lispval *elts; unsigned char *bytes;} mystery_payload;} FD_MYSTERY;
typedef struct FD_MYSTERY_DTYPE *fd_mystery;

/* Exceptions */

typedef struct FD_EXCEPTION {
  FD_CONS_HEADER;
  u8_condition ex_condition;
  u8_context ex_caller;
  u8_string  ex_details;
  lispval ex_irritant;
  lispval ex_stack;
  lispval ex_context;
  u8_string  ex_session;
  double     ex_moment;
  long long  ex_thread;
  time_t     ex_timebase;}
  FD_EXCEPTION;
typedef struct FD_EXCEPTION *fd_exception;

FD_EXPORT lispval fd_init_exception(fd_exception,
                                    u8_condition,u8_context,
                                    u8_string,lispval,
                                    lispval,lispval,
                                    u8_string,double,time_t,
                                    long long);
FD_EXPORT void fd_decref_u8x_xdata(void *ptr);
FD_EXPORT void fd_decref_embedded_exception(void *ptr);

#define FD_EXCEPTIONP(x) (FD_TYPEP((x),fd_exception_type))

#define FD_XDATA_ISLISP(ex)                                     \
  ( ((ex)->u8x_free_xdata == fd_decref_u8x_xdata) ||    \
    ((ex)->u8x_free_xdata == fd_decref_embedded_exception) )

#define FD_U8X_STACK(ex) \
  (((ex)->u8x_free_xdata != fd_decref_embedded_exception) ? (FD_VOID) : \
   (FD_TYPEP(((lispval)((ex)->u8x_xdata)),fd_exception_type)) ?         \
   (((fd_exception)((ex)->u8x_xdata))->ex_stack) :              \
   (FD_VOID))

FD_EXPORT void fd_restore_exception(struct FD_EXCEPTION *exo);

/* Timestamps */

typedef struct FD_TIMESTAMP {
  FD_CONS_HEADER;
  struct U8_XTIME u8xtimeval;} FD_TIMESTAMP;
typedef struct FD_TIMESTAMP *fd_timestamp;

FD_EXPORT lispval fd_make_timestamp(struct U8_XTIME *tm);
FD_EXPORT lispval fd_time2timestamp(time_t moment);

/* Consblocks */

typedef struct FD_CONSBLOCK {
  FD_CONS_HEADER;
  lispval consblock_head, consblock_original;
  struct FD_CONS *consblock_conses;
  size_t consblock_len;
  int consblock_contiguous;} *fd_consblock;

lispval fd_make_consblock(lispval obj);

/* Raw pointers */

typedef void (*fd_raw_recyclefn)(void *);

typedef struct FD_RAWPTR {
  FD_CONS_HEADER;
  void *ptrval;
  u8_string typestring, idstring;
  lispval raw_typespec;
  fd_raw_recyclefn recycler;} FD_RAWPTR;
typedef struct FD_RAWPTR *fd_rawptr;

FD_EXPORT lispval fd_wrap_pointer(void *ptrval,
                                 fd_raw_recyclefn recycler,
                                 lispval typespec,
                                 u8_string idstring);

/* Compounds */

typedef struct FD_COMPOUND_TYPEINFO *fd_compound_typeinfo;
typedef int (*fd_compound_unparsefn)(u8_output out,lispval,fd_compound_typeinfo);
typedef lispval (*fd_compound_parsefn)(int n,lispval *,fd_compound_typeinfo);
typedef int (*fd_compound_freefn)(lispval,fd_compound_typeinfo);
typedef lispval (*fd_compound_dumpfn)(lispval,fd_compound_typeinfo);
typedef lispval (*fd_compound_restorefn)(lispval,lispval,fd_compound_typeinfo);

typedef struct FD_COMPOUND_TYPEINFO {
  lispval compound_typetag, compound_metadata;
  short compound_corelen;
  char compound_isopaque;
  char compound_ismutable;
  char compound_istable;
  char compound_issequence;
  struct FD_COMPOUND_TYPEINFO *compound_nextinfo;
  fd_compound_parsefn compound_parser;
  fd_compound_unparsefn compound_unparser;
  fd_compound_freefn compound_freefn;
  fd_compound_dumpfn compound_dumpfn;
  fd_compound_restorefn compound_restorefn;
  struct FD_TABLEFNS *compund_tablefns;} FD_COMPOUND_TYPEINFO;
FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_compound_entries;


FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_lookup_compound(lispval);
FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_declare_compound(lispval,lispval,int);
FD_EXPORT struct FD_COMPOUND_TYPEINFO *fd_register_compound(lispval,lispval *,int *);
FD_EXPORT int fd_compound_unparser(u8_string pname,fd_compound_unparsefn fn);


/* Cons compare */

/* This provides a fast inline object comparison for files which
   define FD_INLINE_COMPARE. It is equivalent to the quick version of
   lispval_compare.

   It is used, for example, in the implementation of choices and for
   the keys of hashtables.

   It inlines atomic comparisons, string comparisons, numeric
   comparisons (eqv?), and two levels of pairs or vectors. This means
   that top level atoms are compared inline, as are 'x' and 'y' in the
   following cases:
    (x . y)
    #(x y x y x...)
    (x . #(y x y x...))
    #((y . x) (x) x y x...)
 */
#if FD_INLINE_COMPARE
static int base_compare(lispval x,lispval y)
{
  if (FD_ATOMICP(x))
    if (FD_ATOMICP(y))
      if (x < y)
        return -1;
      else if (x == y)
        return 0;
      else return 1;
    else return -1;
  else if (FD_ATOMICP(y))
    return 1;
  else {
    fd_ptr_type xtype = FD_PTR_TYPE(x);
    fd_ptr_type ytype = FD_PTR_TYPE(y);
    if (FD_NUMBER_TYPEP(xtype))
      if (FD_NUMBER_TYPEP(ytype))
        return fd_numcompare(x,y);
      else return -1;
    else if (FD_NUMBER_TYPEP(ytype))
      return 1;
    else if (xtype<ytype)
      return -1;
    else if (xtype>ytype)
      return 1;
    else switch (xtype) {
      case fd_pair_type: {
        int car_cmp = FD_QCOMPARE(FD_CAR(x),FD_CAR(y));
        if (car_cmp == 0)
          return (FD_QCOMPARE(FD_CDR(x),FD_CDR(y)));
        else return car_cmp;}
      case fd_string_type: {
        int xlen = FD_STRLEN(x), ylen = FD_STRLEN(y);
        if (xlen>ylen)
          return 1;
        else if (xlen<ylen)
          return -1;
        else return strncmp(FD_CSTRING(x),FD_CSTRING(y),xlen);}
      case fd_vector_type: {
        int i = 0, xlen = VEC_LEN(x), ylen = VEC_LEN(y);
        lispval *xdata = VEC_DATA(x), *ydata = VEC_DATA(y);
        if (xlen > ylen)
          return 1;
        else if (xlen < ylen)
          return -1;
        int minlen = (xlen < ylen) ? (xlen) : (ylen);
        while (i < minlen) {
          int cmp = FD_QCOMPARE(xdata[i],ydata[i]);
          if (cmp)
            return cmp;
          else i++;}
        return 0;}
      default:
        return LISP_COMPARE(x,y,FD_COMPARE_QUICK);}}
}
static int cons_compare(lispval x,lispval y)
{
  if (FD_ATOMICP(x))
    if (FD_ATOMICP(y))
      if (x < y)
        return -1;
      else if (x == y)
        return 0;
      else return 1;
    else return -1;
  else if (FD_ATOMICP(y))
    return 1;
  else {
    fd_ptr_type xtype = FD_PTR_TYPE(x);
    fd_ptr_type ytype = FD_PTR_TYPE(y);
    if ( (FD_NUMBER_TYPEP(xtype)) && (FD_NUMBER_TYPEP(ytype)) )
      return fd_numcompare(x,y);
    else if (FD_NUMBER_TYPEP(xtype))
      return -1;
    else if (FD_NUMBER_TYPEP(ytype))
      return 1;
    else if (xtype<ytype)
      return -1;
    else if (xtype>ytype)
      return 1;
    else switch (xtype) {
      case fd_pair_type: {
        int car_cmp = base_compare(FD_CAR(x),FD_CAR(y));
        if (car_cmp == 0)
          return base_compare(FD_CDR(x),FD_CDR(y));
        else return car_cmp;}
      case fd_string_type: {
        int xlen = FD_STRLEN(x), ylen = FD_STRLEN(y);
        if (xlen>ylen)
          return 1;
        else if (xlen<ylen)
          return -1;
        else return strncmp(FD_CSTRING(x),FD_CSTRING(y),xlen);}
      case fd_vector_type: {
        int i = 0, xlen = VEC_LEN(x), ylen = VEC_LEN(y);
        lispval *xdata = VEC_DATA(x), *ydata = VEC_DATA(y);
        if (xlen>ylen)
          return 1;
        else if (xlen<ylen)
          return -1;
        int minlen = (xlen < ylen) ? (xlen) : (ylen);
        while (i < minlen) {
          int cmp = base_compare(xdata[i],ydata[i]);
          if (cmp)
            return cmp;
          else i++;}
        return 0;}
      default:
        return LISP_COMPARE(x,y,FD_COMPARE_QUICK);}}
}

#endif

FD_EXPORT fd_compare_flags fd_get_compare_flags(lispval spec);

/* Choices, tables, regexes */

#include "choices.h"
#include "tables.h"
#include "fdregex.h"

/* The zero-pool */

/* OIDs with a high address of zero are treated specially. They are
   "ephemeral OIDs" whose values are limited to the current runtime
   session. */

#ifndef FD_ZERO_POOL_MAX
#define FD_ZERO_POOL_MAX 0x10000
#endif

FD_EXPORT lispval fd_zero_pool_values0[4096];
FD_EXPORT lispval *fd_zero_pool_buckets[FD_ZERO_POOL_MAX/4096];
FD_EXPORT unsigned int fd_zero_pool_load;
FD_EXPORT lispval fd_zero_pool_value(lispval oid);
FD_EXPORT lispval fd_zero_pool_store(lispval oid,lispval value);

#endif /* ndef FRAMERD_CONS_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
