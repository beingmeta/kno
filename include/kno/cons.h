/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* Anatomy of a CONS

    CONSES are dynamically allocated structures used by Framer for compound
    objects.  CONSES can be any kind of structure, providing only that the
    first four bytes, an unsigned int called the "conshead," be reserved
    for Kno's typing and reference counting information.  The lower seven
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
    (with KNO_DECREF) or use another upper-case consuming operation on it
    (such as KNO_ADD_TO_CHOICE (see <include/kno/choices.h>)).

    Because the immediate type information on a dtype pointer distinguishes
    between CONSes and other types (which don't need to be reference
    counted), determining whether a value needs to be reference counted is
    very cheap.

    MULTI THREADING: Reference counting is made threadsafe by using global
    mutexes to protect access to the conshead fields.  In order to reduce
    contention, FramerC uses a strategy called "hash locking" to regulate
    access to the conshead of CONSes.  An array of mutexes, _kno_ptr_locks,
    and computes an offset into that array by shifting the structure
    address right 16 bits and taking a remainder modulo the number of
    pointer locks (32 by default).  The shift size was chosen after a
    little experimentation but different architectures may suggest
    different sizes (though they all should be greater than two due to
    structure word-alignment).

    There are two special kinds of CONSes that produce further
    extensibility.  "Wrapped conses" (kno_wrapped_cons_type) have a second
    field which is a pointer to a KNO_TYPEINFO struct providing methods for
    standard operations like displaying (unparsing), recycling, comparing,
    etc. "Compund conses"....

*/

/* Working with CONSes */

#ifndef KNO_CONS_H
#define KNO_CONS_H 1
#ifndef KNO_CONS_H_INFO
#define KNO_CONS_H_INFO "include/kno/cons.h"
#endif

#include "ptr.h"
#include <libu8/u8timefns.h>

KNO_EXPORT u8_condition kno_MallocFailed, kno_StringOverflow, kno_StackOverflow;
KNO_EXPORT u8_condition kno_DoubleGC, kno_UsingFreedCons, kno_FreeingNonHeapCons;

#define KNO_GET_CONS(x,typecode,cast)                                    \
  ((KNO_EXPECT_TRUE(KNO_TYPEP(x,typecode))) ?                             \
   ((cast)(KNO_CONS_DATA(x))) :                                          \
   ((cast)(kno_err(kno_TypeError,kno_type_names[typecode],NULL,x),NULL)))

#define KNO_STRIP_CONS(x,typecode,typecast) ((typecast)(KNO_CONS_DATA(x)))

#define KNO_CHECK_TYPE_THROW(x,typecode)                  \
  if (KNO_EXPECT_FALSE(!((KNO_CONS_TYPE(x)) == typecode))) \
    u8_raise(kno_TypeError,kno_type_names[typecode],NULL)

#define KNO_CHECK_TYPE_RET(x,typecode)                               \
  if (KNO_EXPECT_FALSE(!((KNO_CONS_TYPE(x)) == typecode))) {          \
    kno_seterr(kno_TypeError,kno_type_names[typecode],NULL,(lispval)x); \
    return -1;}

#define KNO_CHECK_TYPE_RETDTYPE(x,typecode)                              \
  if (KNO_EXPECT_FALSE(!((KNO_CONS_TYPE(x)) == typecode)))                \
    return kno_err(kno_TypeError,kno_type_names[typecode],NULL,(lispval)x);

#define KNO_PTR2CONS(x,typecode,typecast)                            \
  (((typecode<0) || (KNO_TYPEP(x,typecode))) ?                       \
   ((typecast)(KNO_CONS_DATA(x))) :                                  \
   ((typecast)(u8_raise(kno_TypeError,kno_type2name(typecode),NULL),  \
                NULL)))

#ifndef KNO_MAX_REFCOUNT
#define KNO_MAX_REFCOUNT 0xFFFFFF
#endif

/* External functions */

KNO_EXPORT void _kno_decref_fn(lispval);
KNO_EXPORT lispval _kno_incref_fn(lispval);

KNO_EXPORT void _KNO_INIT_CONS(void *vptr,kno_lisp_type type);
KNO_EXPORT void _KNO_INIT_FRESH_CONS(void *vptr,kno_lisp_type type);
KNO_EXPORT void _KNO_INIT_STACK_CONS(void *vptr,kno_lisp_type type);
KNO_EXPORT void _KNO_INIT_STATIC_CONS(void *vptr,kno_lisp_type type);
KNO_EXPORT void _KNO_SET_CONS_TYPE(void *vptr,kno_lisp_type type);
KNO_EXPORT void _KNO_SET_REFCOUNT(void *vptr,unsigned int count);

/* Initializing CONSes */

#if KNO_LOCKFREE_REFCOUNTS
#define KNO_HEAD_INIT(type) (ATOMIC_VAR_INIT((type-(KNO_CONS_TYPE_OFF))|0x80))
#define KNO_STATIC_INIT(type) (ATOMIC_VAR_INIT((type-(KNO_CONS_TYPE_OFF))))
#else
#define KNO_HEAD_INIT(type)   ((type-(KNO_CONS_TYPE_OFF))|0x80)
#define KNO_STATIC_INIT(type) (type-(KNO_CONS_TYPE_OFF))
#endif

#if KNO_INLINE_REFCOUNTS
#define KNO_INIT_CONS(ptr,type) \
  ((kno_raw_cons)ptr)->conshead = (KNO_HEAD_INIT(type))
#define KNO_INIT_FRESH_CONS(ptr,type) \
  memset(ptr,0,sizeof(*(ptr))); \
  ((kno_raw_cons)ptr)->conshead = (KNO_HEAD_INIT(type))
#define KNO_INIT_STACK_CONS(ptr,type) \
  ((kno_raw_cons)ptr)->conshead = (KNO_STATIC_INIT(type))
#define KNO_INIT_STATIC_CONS(ptr,type) \
  memset(ptr,0,sizeof(*(ptr))); \
  ((kno_raw_cons)ptr)->conshead = (KNO_STATIC_INIT(type))
#else
#define KNO_INIT_CONS(ptr,type) _KNO_INIT_CONS((struct KNO_RAW_CONS *)ptr,type)
#define KNO_INIT_FRESH_CONS(ptr,type) _KNO_INIT_FRESH_CONS((struct KNO_RAW_CONS *)ptr,type)
#define KNO_INIT_STACK_CONS(ptr,type) _KNO_INIT_STACK_CONS((struct KNO_RAW_CONS *)ptr,type)
#define KNO_INIT_STATIC_CONS(ptr,type) _KNO_INIT_STATIC_CONS((struct KNO_RAW_CONS *)ptr,type)
#endif

#if KNO_INLINE_REFCOUNTS && KNO_LOCKFREE_REFCOUNTS
#define KNO_CBITS(x) (((kno_ref_cons)x)->conshead)
#define KNO_SET_CONS_TYPE(ptr,type) \
  atomic_store                                                  \
  (&(KNO_CBITS(ptr)),                                            \
    (((atomic_load(&(KNO_CBITS(ptr))))&(~(KNO_CONS_TYPE_MASK)))|  \
     ((type-(KNO_CONS_TYPE_OFF))&0x7f)))
#define KNO_SET_REFCOUNT(ptr,count) \
  atomic_store                                                  \
  (&(KNO_CBITS(ptr)),                                            \
    (((atomic_load(&(KNO_CBITS(ptr))))&(KNO_CONS_TYPE_MASK))|     \
     (count<<7)))
#elif KNO_INLINE_REFCOUNTS
#define KNO_SET_CONS_TYPE(ptr,type) \
  ((kno_raw_cons)ptr)->conshead=                               \
    ((((kno_raw_cons)ptr)->conshead&(~(KNO_CONS_TYPE_MASK)))) | \
    ((type-(KNO_CONS_TYPE_OFF))&0x7f)
#define KNO_SET_REFCOUNT(ptr,count) \
  ((kno_raw_cons)ptr)->conshead=                                 \
    (((((kno_raw_cons)ptr)->conshead)&(KNO_CONS_TYPE_MASK))|      \
     (count<<7))))
#else
#define KNO_SET_CONS_TYPE(ptr,type) \
  _KNO_SET_CONS_TYPE((struct KNO_RAW_CONS *)ptr,type)
#define KNO_SET_REFCOUNT(ptr,type) \
  _KNO_SET_REFCOUNT((struct KNO_RAW_CONS *)ptr,type)
#endif


/* Hash locking for pointers */

#if (SIZEOF_VOID_P == 8)
#define KNO_PTRHASH_CONSTANT 11400714819323198549ul
#else
#define KNO_PTRHASH_CONSTANT 2654435761
#endif

U8_INLINE U8_MAYBE_UNUSED u8_int8 hashptrval(void *ptr,unsigned int mod)
{
  u8_wideint intrep = ((u8_wideint) ptr)>>4;
  return (intrep * KNO_PTRHASH_CONSTANT) % mod;
}

KNO_EXPORT u8_mutex _kno_ptr_locks[KNO_N_PTRLOCKS];
#define KNO_PTR_LOCK_OFFSET(ptr) (hashptrval((ptr),KNO_N_PTRLOCKS))
#define KNO_LOCK_PTR(ptr) \
  u8_lock_mutex(&_kno_ptr_locks[KNO_PTR_LOCK_OFFSET(ptr)])
#define KNO_UNLOCK_PTR(ptr) \
  u8_unlock_mutex(&_kno_ptr_locks[KNO_PTR_LOCK_OFFSET(ptr)])

/* Reference counting GC */

#if KNO_INLINE_REFCOUNTS && KNO_LOCKFREE_REFCOUNTS
#define KNO_CONSBITS(x)       (atomic_load(&(((kno_ref_cons)x)->conshead)))
#define KNO_CONS_REFCOUNT(x)  (KNO_CONSBITS(x)>>7)
#elif KNO_INLINE_REFCOUNTS
#define KNO_CONSBITS(x)       ((x)->conshead)
#define KNO_CONS_REFCOUNT(x)  (((x)->conshead)>>7)
#else
#define KNO_CONSBITS(x)      ((x)->conshead)
#define KNO_CONS_REFCOUNT(x) (((x)->conshead)>>7)
#endif

#define KNO_MALLOCD_CONSP(x) ((KNO_CONSBITS(x))>=0x80)
#define KNO_STATIC_CONSP(x)  (!(KNO_MALLOCD_CONSP(x)))

#define KNO_MALLOCDP(x) ( (KNO_CONSP(x)) &&  ((KNO_CONSBITS(x))>=0x80) )


#define KNO_STATICP(x) ((!(KNO_CONSP(x)))||(KNO_STATIC_CONSP((kno_cons)x)))

#define kno_getref(x) \
  ((KNO_CONSP(x)) ? \
   ((KNO_STATIC_CONSP(x)) ? (kno_deep_copy(x)) : (kno_incref(x)) ) : \
   (x))
#define kno_not_static(x) \
  ((KNO_CONSP(x)) ? \
   ((KNO_STATIC_CONSP(x)) ? (kno_copy(x)) : (x) ) : \
   (x))

#define KNO_MALLOCD_CONS 0
#define KNO_STACK_CONS   1

KNO_EXPORT void kno_recycle_cons(struct KNO_RAW_CONS *);
KNO_EXPORT lispval kno_copy(lispval x);
KNO_EXPORT lispval kno_copier(lispval x,int flags);
KNO_EXPORT lispval kno_deep_copy(lispval x);
KNO_EXPORT lispval kno_static_copy(lispval x);

KNO_EXPORT lispval *kno_copy_vec(lispval *vec,size_t n,lispval *into,int copy_flags);
KNO_EXPORT void kno_incref_vec(lispval *vec,size_t n);
KNO_EXPORT void kno_decref_vec(lispval *vec,size_t n);

#define KNO_DEEP_COPY 2   /* Make a deep copy */
#define KNO_FULL_COPY 4   /* Copy non-static objects */
#define KNO_STRICT_COPY 8 /* Require methods for all objects */
#define KNO_STATIC_COPY 16 /* Declare all copied objects static (this leaks) */
#define KNO_COPY_TERMINALS 32 /* Copy even terminal objects */

/* The consheader is a 32 bit value:
   The lower 7 bits are a cons typecode
   The remaining bits are a reference count, which is always >0
   A zero reference count indicates a static CONS, which
   is never reference counted.
*/

#if ( KNO_INLINE_REFCOUNTS && KNO_LOCKFREE_REFCOUNTS )
KNO_INLINE_FCN lispval _kno_incref(struct KNO_REF_CONS *x)
{
  if (KNO_EXPECT_FALSE(x == NULL))
    return KNO_BADPTR;
  else {
    kno_consbits cb = atomic_load(&(x->conshead));
    if (cb>0xFFFFFF80) {
      u8_raise(kno_UsingFreedCons,"kno_incref",NULL);
      return (lispval)NULL;}
    else if ((cb&(~0x7F)) == 0) {
      /* Static cons */
      return (lispval) x;}
#if KNO_MAX_REFCOUNT
    else if ((cb>>7) > KNO_MAX_REFCOUNT) {
      atomic_fetch_add(&(x->conshead),0x80);
      _kno_debug((lispval)x);
      return (lispval) x;}
#endif
    else {
      atomic_fetch_add(&(x->conshead),0x80);
      return (lispval) x;}}
}

KNO_INLINE_FCN void _kno_decref(struct KNO_REF_CONS *x)
{
  if (KNO_EXPECT_FALSE(x == NULL)) return;
  kno_consbits cb = atomic_load(&(x->conshead));
  if (cb>=0xFFFFFF80) {
    u8_raise(kno_DoubleGC,"kno_decref",NULL);}
  else if (cb<0x80) {
    /* Static cons */}
  else {
    kno_consbits oldcb=atomic_fetch_sub(&(x->conshead),0x80);
    if ((oldcb>=0x80)&&(oldcb<0x100)) {
      /* If the modified consbits indicated a refcount of 1,
         we've reduced it to zero, so we recycle it. Otherwise,
         someone got in to free it or incref it in the meanwhile. */
      atomic_store(&(x->conshead),((oldcb&0x7F)|0xFFFFFF80));
      kno_recycle_cons((kno_raw_cons)x);}
  }
}

#elif KNO_INLINE_REFCOUNTS

KNO_INLINE_FCN lispval _kno_incref(struct KNO_REF_CONS *x)
{
  if (KNO_CONSBITS(x)>0xFFFFFF80) {
    u8_raise(kno_UsingFreedCons,"kno_incref",NULL);
    return (lispval)NULL;}
  else if ((KNO_CONSBITS(x)&(~0x7F)) == 0) {
    /* Static cons */
    return (lispval) x;}
  else {
    KNO_LOCK_PTR(x);
    x->conshead = x->conshead+0x80;
    KNO_UNLOCK_PTR(x);
    return (lispval) x;}
}

KNO_INLINE_FCN void _kno_decref(struct KNO_REF_CONS *x)
{
  if (KNO_CONSBITS(x)>=0xFFFFFF80) {
    u8_raise(kno_DoubleGC,"kno_decref",NULL);}
  else if ((KNO_CONSBITS(x)&(~0x7F)) == 0) {
    /* Static cons */}
  else {
    KNO_LOCK_PTR(x);
    if (KNO_CONSBITS(x)>=0x100) {
      /* If it's still got a refcount > 1, just decrease it */
      x->conshead = x->conshead-0x80;
      KNO_UNLOCK_PTR(x);}
    else {
      /* Someone else decref'd it before we got the lock, so we
         unlock and recycle it */
      KNO_UNLOCK_PTR(x);
      kno_recycle_cons((struct KNO_RAW_CONS *)x);}}
}
#endif

#if KNO_INLINE_REFCOUNTS

#define kno_incref(x) \
  ((KNO_PTR_MANIFEST_TYPE(x)) ? ((lispval)x) : (_kno_incref(KNO_REF_CONS(x))))
#define kno_decref(x) \
  ((void)((KNO_PTR_MANIFEST_TYPE(x)) ? (KNO_VOID) : \
          (_kno_decref(KNO_REF_CONS(x)),KNO_VOID)))
#else

#define kno_incref(x) \
   ((KNO_PTR_MANIFEST_TYPE(x)) ? (x) : (_kno_incref_fn(x)))
#define kno_decref(x) \
  ((void)((KNO_PTR_MANIFEST_TYPE(x)) ? (KNO_VOID) : \
          (_kno_decref_fn(x),KNO_VOID)))

#endif

/* Incref/decref for vectors */

KNO_EXPORT lispval *_kno_init_elts(lispval *elts,size_t n,lispval v);

KNO_EXPORT ssize_t _kno_incref_elts(const lispval *elts,size_t n);

KNO_EXPORT ssize_t _kno_decref_elts(const lispval *elts,size_t n);

KNO_EXPORT ssize_t _kno_free_elts(lispval *elts,size_t n);

#if KNO_SOURCE
KNO_FASTOP U8_MAYBE_UNUSED
lispval *kno_init_elts(lispval *elts,ssize_t n,lispval v)
{
  if (elts == NULL) elts=u8_alloc_n(n,lispval);
  if (KNO_CONSP(v)) {
    int i=0; while (i<n) { elts[i++]=kno_incref(v); }}
  else {
    int i=0; while (i<n) { elts[i++]=v; }}
  return elts;
}
KNO_FASTOP U8_MAYBE_UNUSED
ssize_t kno_incref_elts(const lispval *elts,size_t n)
{
  int i=0, consed=0; while (i<n) {
    lispval elt=elts[i++];
    if ((KNO_CONSP(elt))&&(KNO_MALLOCD_CONSP((kno_cons)elt))) {
      consed++;
      kno_decref(elt);}}
  return consed;
}

KNO_FASTOP U8_MAYBE_UNUSED
size_t kno_decref_elts(const lispval *elts,size_t n)
{
  size_t conses=0;
  int i=0; while (i<n) {
    lispval elt=elts[i++];
    if (KNO_CONSP(elt)) {
      kno_decref(elt);
      conses++;}}
  return conses;

}

KNO_FASTOP U8_MAYBE_UNUSED
ssize_t kno_free_elts(lispval *elts,size_t n)
{
  size_t conses=0;
  const lispval *scan=(const lispval *)elts;
  int i=0; while (i<n) {
    lispval elt=scan[i++];
    if (KNO_CONSP(elt)) {
      kno_decref(elt);
      conses++;}}
  u8_free(elts);
  return conses;
}
#else
#define kno_init_elts _kno_init_elts
#define kno_incref_elts _kno_incref_elts
#define kno_decref_elts _kno_decref_elts
#define kno_free_elts _kno_free_elts
#endif

#define kno_incref_ptr(p) (kno_incref((lispval)(p)))
#define kno_decref_ptr(p) (kno_decref((lispval)(p)))

/* Conses */

struct KNO_FREE_CONS {
  KNO_CONS_HEADER;
  struct KNO_FREE_CONS *kno_nextfree;};

/* Strings */

typedef struct KNO_STRING {
  KNO_CONS_HEADER;
  unsigned int str_freebytes:1;
  unsigned int str_bytelen:31;
  u8_string str_bytes;} KNO_STRING;
typedef struct KNO_STRING *kno_string;

#define KNO_STRING_LEN (sizeof(struct KNO_STRING))

KNO_EXPORT ssize_t kno_max_strlen;
KNO_EXPORT int kno_check_utf8;

#define KNO_STRINGP(x) (KNO_TYPEP(x,kno_string_type))
#define KNO_STRLEN(x) ((unsigned int) ((KNO_CONSPTR(kno_string,x))->str_bytelen))
#define KNO_STRDATA(x) ((KNO_CONSPTR(kno_string,x))->str_bytes)
#define KNO_CSTRING(x) ((KNO_CONSPTR(kno_string,x))->str_bytes)
#define KNO_STRING_LENGTH(x) (KNO_STRLEN(x))
#define KNO_STRING_DATA(x) (KNO_STRDATA(x))

#define KNO_XSTRING(x) (kno_consptr(kno_string,x,kno_string_type))
#define kno_xstring(x) (kno_consptr(kno_string,x,kno_string_type))
#define kno_strlen(x)  (KNO_STRLEN(kno_xstring(x)))
#define kno_strdata(x) (KNO_STRDATA(kno_xstring(x)))

KNO_EXPORT lispval kno_extract_string
  (struct KNO_STRING *ptr,u8_string start,u8_string end);
KNO_EXPORT lispval kno_substring(u8_string start,u8_string end);
KNO_EXPORT lispval kno_init_string
  (struct KNO_STRING *ptr,int slen,u8_string string);
KNO_EXPORT lispval kno_make_string
  (struct KNO_STRING *ptr,int slen,u8_string string);
KNO_EXPORT lispval kno_block_string(int slen,u8_string string);
KNO_EXPORT lispval kno_mkstring(u8_string string);

#define kno_stream2string(stream) \
  ((((stream)->u8_streaminfo)&(U8_STREAM_OWNS_BUF))?                    \
   (kno_block_string((((stream)->u8_write)-((stream)->u8_outbuf)),      \
                   ((stream)->u8_outbuf))):                             \
   (kno_make_string(NULL,(((stream)->u8_write)-((stream)->u8_outbuf)),  \
                   ((stream)->u8_outbuf))))
#define kno_stream_string(stream) \
  (kno_make_string(NULL,(((stream)->u8_write)-((stream)->u8_outbuf)),   \
                  ((stream)->u8_outbuf)))
#define knostring(s) (kno_make_string(NULL,-1,(s)))

#define kno_wrapstring(s) kno_init_string(NULL,-1,(s))

#define KNO_GETSTRING(x) \
  ((KNO_SYMBOLP(x)) ? (KNO_SYMBOL_NAME(x)) : \
   (KNO_STRINGP(x)) ? (KNO_CSTRING(x)) : (NULL))

/* Packets are blocks of binary data. */

#define KNO_PACKETP(x) \
  ((KNO_TYPEOF(x) == kno_packet_type)||(KNO_TYPEOF(x) == kno_secret_type))
#define KNO_SECRETP(x) \
  (KNO_TYPEOF(x) == kno_secret_type)
#define KNO_PACKET_LENGTH(x) \
  ((unsigned int) ((KNO_CONSPTR(kno_string,x))->str_bytelen))
#define KNO_PACKET_DATA(x) ((KNO_CONSPTR(kno_string,x))->str_bytes)
#define KNO_PACKET_REF(x,i) ((KNO_CONSPTR(kno_string,x))->str_bytes[i])

KNO_EXPORT lispval kno_init_packet
  (struct KNO_STRING *ptr,int len,const unsigned char *data);
KNO_EXPORT lispval kno_make_packet
  (struct KNO_STRING *ptr,int len,const unsigned char *data);
KNO_EXPORT lispval kno_bytes2packet
  (struct KNO_STRING *ptr,int len,const unsigned char *data);

#define KNO_XPACKET(x) (kno_consptr(struct KNO_STRING *,x,kno_packet_type))

/* Symbol tables */

typedef struct KNO_SYMBOL_ENTRY {
  struct KNO_STRING sym_pname;
  int symid;} KNO_SYMBOL_ENTRY;
typedef struct KNO_SYMBOL_ENTRY *kno_symbol_entry;
struct KNO_SYMBOL_TABLE {
  int table_size;
  struct KNO_SYMBOL_ENTRY **kno_symbol_entries;};
KNO_EXPORT struct KNO_SYMBOL_TABLE kno_symbol_table;

/* Pairs */

typedef struct KNO_PAIR {
  KNO_CONS_HEADER;
  lispval car;
  lispval cdr;} KNO_PAIR;
typedef struct KNO_PAIR *kno_pair;

#define KNO_PAIRP(x) (KNO_TYPEOF(x) == kno_pair_type)
#define KNO_CAR(x) ((KNO_CONSPTR(KNO_PAIR *,(x)))->car)
#define KNO_CDR(x) ((KNO_CONSPTR(KNO_PAIR *,(x)))->cdr)
#define KNO_TRY_CAR(x) \
  ((KNO_PAIRP(x)) ? ((KNO_CONSPTR(KNO_PAIR *,(x)))->car) : (KNO_VOID))
#define KNO_TRY_CDR(x) \
  ((KNO_PAIRP(x)) ? ((KNO_CONSPTR(kno_pair,(x)))->cdr) :   \
   (KNO_VOID))
KNO_FASTOP lispval KNO_CADR(lispval x)
{
  if (KNO_PAIRP(x)) {
    lispval cdr = KNO_CDR(x);
    if (KNO_PAIRP(cdr))
      return KNO_CAR(cdr);
    else return KNO_ERROR_VALUE;}
  else return KNO_ERROR_VALUE;
}
KNO_FASTOP lispval KNO_CDDR(lispval x)
{
  if (KNO_PAIRP(x)) {
    lispval cdr = KNO_CDR(x);
    if (KNO_PAIRP(cdr))
      return KNO_CDR(cdr);
    else return KNO_ERROR_VALUE;}
  else return KNO_ERROR_VALUE;
}
KNO_FASTOP lispval KNO_CAAR(lispval x)
{
  if (KNO_PAIRP(x)) {
    lispval car = KNO_CAR(x);
    if (KNO_PAIRP(car))
      return KNO_CAR(car);
    else return KNO_ERROR_VALUE;}
  else return KNO_ERROR_VALUE;
}

#define kno_refcar(x) \
  kno_incref(((kno_consptr(struct KNO_PAIR *,x,kno_pair_type))->car))
#define kno_refcdr(x) \
  kno_incref(((kno_consptr(struct KNO_PAIR *,x,kno_pair_type))->cdr))

/* These are not threadsafe and they don't worry about GC either */
#define KNO_SETCAR(p,x) ((struct KNO_PAIR *)p)->car = x
#define KNO_SETCDR(p,x) ((struct KNO_PAIR *)p)->cdr = x

#define KNO_RPLACA(p,x) ((struct KNO_PAIR *)p)->car = x
#define KNO_RPLACD(p,x) ((struct KNO_PAIR *)p)->cdr = x

#define KNO_DOLIST(x,list) \
  lispval x, _tmp = list; \
  while ((KNO_PAIRP(_tmp)) ? \
         (x = KNO_CAR(_tmp),_tmp = KNO_CDR(_tmp),1) : 0)

KNO_EXPORT lispval kno_init_pair(struct KNO_PAIR *ptr,lispval car,lispval cdr);
KNO_EXPORT lispval kno_make_pair(lispval car,lispval cdr);
KNO_EXPORT lispval kno_make_list(int len,...);
KNO_EXPORT lispval kno_pmake_list(int len,...);
KNO_EXPORT int kno_list_length(lispval l);
KNO_EXPORT lispval kno_reverse_list(lispval l);

#define KNO_XPAIR(x) (kno_consptr(struct KNO_PAIR *,x,kno_pair_type))
#define kno_conspair(car,cdr) kno_init_pair(NULL,car,cdr)

/* Vectors */

typedef struct KNO_VECTOR {
  KNO_CONS_HEADER;
  unsigned int vec_length:29;
  unsigned int vec_free_elts:1;
  unsigned int vec_bigalloc:1;
  unsigned int vec_bigalloc_elts:1;
  lispval *vec_elts;} KNO_VECTOR;
typedef struct KNO_VECTOR *kno_vector;

#define KNO_VECTOR_LEN (sizeof(struct KNO_VECTOR))

KNO_EXPORT ssize_t kno_bigvec_threshold;

#define KNO_VECTORP(x) (KNO_TYPEP((x),kno_vector_type))
#define KNO_VECTOR_LENGTH(x) \
  ((KNO_CONSPTR(kno_vector,(x)))->vec_length)
#define KNO_VECTOR_DATA(x) \
  ((KNO_CONSPTR(kno_vector,(x)))->vec_elts)
#define KNO_VECTOR_ELTS(x) \
  ((KNO_CONSPTR(kno_vector,(x)))->vec_elts)
#define KNO_VECTOR_REF(x,i) \
  ((KNO_CONSPTR(kno_vector,(x)))->vec_elts[i])
#define KNO_VECTOR_SET(x,i,v) \
  ((KNO_CONSPTR(kno_vector,(x)))->vec_elts[i]=(v))

KNO_EXPORT lispval kno_cons_vector(struct KNO_VECTOR *ptr,int len,int big,lispval *data);
KNO_EXPORT lispval kno_init_vector(struct KNO_VECTOR *ptr,int len,lispval *data);
KNO_EXPORT lispval kno_wrap_vector(int len,lispval *data);
KNO_EXPORT lispval kno_empty_vector(int len);
KNO_EXPORT lispval kno_make_vector(int len,lispval *elts);
KNO_EXPORT lispval kno_make_nvector(int len,...);
KNO_EXPORT lispval kno_fill_vector(int len,lispval init_elt);

#define KNO_XVECTOR(x) (kno_consptr(struct KNO_VECTOR *,x,kno_vector_type))

/* Generic-ish iteration macro */

#define KNO_DOELTS(evar,seq,counter)              \
  lispval _seq = seq, evar = KNO_VOID, counter = 0;      \
  lispval _scan = KNO_VOID, *_elts;                  \
  int _i = 0, _islist = 0, _lim = 0, _ok = 0;            \
  if (KNO_PAIRP(seq)) {                           \
     _islist = 1; _scan=_seq; _ok = 1;}              \
  else if (KNO_VECTORP(_seq)) {                   \
    _lim = KNO_VECTOR_LENGTH(_seq);                 \
    _elts = KNO_VECTOR_DATA(_seq);                  \
    _ok = 1;}                                      \
  else if ((KNO_EMPTY_LISTP(_seq))||              \
           (KNO_EMPTY_CHOICEP(_seq))) {           \
    _ok = -1;}                                     \
  else u8_log(LOG_WARN,kno_TypeError,             \
              "Not a pair or vector: %q",_seq);  \
  if (_ok<0) {}                                  \
  else if (!(_ok)) {}                            \
  else while (((_islist)?(KNO_PAIRP(_scan)):(counter<_lim))? \
              (evar = (_islist)?(KNO_CAR(_scan)):(_elts[_i]),  \
               _scan = ((_islist)?(KNO_CDR(_scan)):(KNO_VOID)), \
               counter=_i++,1):                             \
              (0))

/* UUID */

typedef struct KNO_UUID {
  KNO_CONS_HEADER;
  unsigned char uuid16[16];} KNO_UUID;
typedef struct KNO_UUID *kno_uuid;

KNO_EXPORT lispval kno_cons_uuid
   (struct KNO_UUID *ptr,
    struct U8_XTIME *xtime,long long nodeid,short clockid);
KNO_EXPORT lispval kno_fresh_uuid(struct KNO_UUID *ptr);

/* BIG INTs */

#define KNO_BIGINTP(x) (KNO_TYPEP(x,kno_bigint_type))

typedef struct KNO_BIGINT *kno_bigint;

KNO_EXPORT lispval kno_make_bigint(long long);

KNO_EXPORT int kno_small_bigintp(kno_bigint bi);
KNO_EXPORT int kno_modest_bigintp(kno_bigint bi);
KNO_EXPORT int kno_bigint2int(kno_bigint bi);
KNO_EXPORT unsigned int kno_bigint2uint(kno_bigint bi);
KNO_EXPORT long kno_bigint_to_long(kno_bigint bi);
KNO_EXPORT long long kno_bigint_to_long_long(kno_bigint bi);
KNO_EXPORT unsigned long long kno_bigint_to_ulong_long(kno_bigint bi);
KNO_EXPORT kno_bigint kno_long_long_to_bigint(long long);
KNO_EXPORT kno_bigint kno_long_to_bigint(long);

#define KNO_INTEGERP(x) ((KNO_FIXNUMP(x))||(KNO_BIGINTP(x)))

#define kno_getint(x) \
  ((KNO_FIXNUMP(x)) ? (KNO_FIX2INT(x)) : \
   ((KNO_TYPEP(x,kno_bigint_type)) && (kno_small_bigintp((kno_bigint)x))) ? \
   (kno_bigint2int((kno_bigint)x)) : (0))
#define kno_getint64(x) \
  ((KNO_FIXNUMP(x)) ? (KNO_FIX2INT(x)) : \
   ((KNO_TYPEP(x,kno_bigint_type)) && (kno_modest_bigintp((kno_bigint)x))) ? \
   (kno_bigint2int64((kno_bigint)x)) : (0))

#define KNO_ISINT(x) \
  ((KNO_FIXNUMP(x)) || \
   ((KNO_TYPEP(x,kno_bigint_type)) && \
    (kno_small_bigintp((kno_bigint)x))))
#define KNO_ISINT64(x) \
  ((KNO_FIXNUMP(x)) || \
   ((KNO_TYPEP(x,kno_bigint_type)) && \
    (kno_modest_bigintp((kno_bigint)x))))

/* Doubles */

typedef struct KNO_FLONUM {
  KNO_CONS_HEADER;
  double floval;} KNO_FLONUM;
typedef struct KNO_FLONUM *kno_flonum;

#define KNO_FLONUMP(x) (KNO_TYPEP(x,kno_flonum_type))
#define KNO_XFLONUM(x) (kno_consptr(struct KNO_FLONUM *,x,kno_flonum_type))
#define KNO_FLONUM(x) ((KNO_XFLONUM(x))->floval)

KNO_EXPORT lispval kno_init_flonum(struct KNO_FLONUM *ptr,double flonum);
KNO_EXPORT lispval kno_init_double(struct KNO_FLONUM *ptr,double flonum);

#define kno_make_double(dbl) (kno_init_double(NULL,(dbl)))
#define kno_make_flonum(dbl) (kno_init_flonum(NULL,(dbl)))

/* Rational and complex numbers */

typedef struct KNO_RATIONAL {
  KNO_CONS_HEADER;
  lispval numerator;
  lispval denominator;} KNO_RATIONAL;
typedef struct KNO_RATIONAL *kno_rational;

#define KNO_RATIONALP(x) (KNO_TYPEOF(x) == kno_rational_type)
#define KNO_NUMERATOR(x) \
  ((kno_consptr(struct KNO_RATIONAL *,x,kno_rational_type))->numerator)
#define KNO_DENOMINATOR(x) \
  ((kno_consptr(struct KNO_RATIONAL *,x,kno_rational_type))->denominator)

typedef struct KNO_COMPLEX {
  KNO_CONS_HEADER;
  lispval realpart;
  lispval imagpart;} KNO_COMPLEX;
typedef struct KNO_COMPLEX *kno_complex;

#define KNO_COMPLEXP(x) (KNO_TYPEOF(x) == kno_complex_type)
#define KNO_REALPART(x) \
  ((kno_consptr(struct KNO_COMPLEX *,x,kno_complex_type))->realpart)
#define KNO_IMAGPART(x) \
  ((kno_consptr(struct KNO_COMPLEX *,x,kno_complex_type))->imagpart)


/* Parsing regexes */

#include <regex.h>

KNO_EXPORT u8_condition kno_RegexError;

typedef struct KNO_REGEX {
  KNO_CONS_HEADER;
  u8_string rxsrc;
  unsigned int rxactive;
  int rxflags;
  u8_mutex rx_lock;
  regex_t rxcompiled;} KNO_REGEX;
typedef struct KNO_REGEX *kno_regex;

KNO_EXPORT lispval kno_make_regex(u8_string src,int flags);

#define KNO_REGEXP(x) (KNO_TYPEOF(x) == kno_regex_type)

/* Mysteries */

typedef struct KNO_MYSTERY_DTYPE {
  KNO_CONS_HEADER;
  unsigned char myst_dtpackage, myst_dtcode;
  size_t myst_dtsize;
  union {
    lispval *elts; unsigned char *bytes;} mystery_payload;} KNO_MYSTERY;
typedef struct KNO_MYSTERY_DTYPE *kno_mystery;

/* Exceptions */

typedef struct KNO_EXCEPTION {
  KNO_CONS_HEADER;
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
  KNO_EXCEPTION;
typedef struct KNO_EXCEPTION *kno_exception;

KNO_EXPORT lispval kno_init_exception(kno_exception,
                                    u8_condition,u8_context,
                                    u8_string,lispval,
                                    lispval,lispval,
                                    u8_string,double,time_t,
                                    long long);
KNO_EXPORT void kno_decref_u8x_xdata(void *ptr);
KNO_EXPORT void kno_decref_embedded_exception(void *ptr);

#define KNO_EXCEPTIONP(x) (KNO_TYPEP((x),kno_exception_type))

#define KNO_XDATA_ISLISP(ex)                                     \
  ( ((ex)->u8x_free_xdata == kno_decref_u8x_xdata) ||    \
    ((ex)->u8x_free_xdata == kno_decref_embedded_exception) )

#define KNO_U8X_STACK(ex) \
  (((ex)->u8x_free_xdata != kno_decref_embedded_exception) ? (KNO_VOID) : \
   (KNO_TYPEP(((lispval)((ex)->u8x_xdata)),kno_exception_type)) ?         \
   (((kno_exception)((ex)->u8x_xdata))->ex_stack) :              \
   (KNO_VOID))

KNO_EXPORT void kno_restore_exception(struct KNO_EXCEPTION *exo);

/* Timestamps */

typedef struct KNO_TIMESTAMP {
  KNO_CONS_HEADER;
  struct U8_XTIME u8xtimeval;} KNO_TIMESTAMP;
typedef struct KNO_TIMESTAMP *kno_timestamp;

KNO_EXPORT lispval kno_make_timestamp(struct U8_XTIME *tm);
KNO_EXPORT lispval kno_time2timestamp(time_t moment);

/* Consblocks */

typedef struct KNO_CONSBLOCK {
  KNO_CONS_HEADER;
  lispval consblock_head, consblock_original;
  struct KNO_CONS *consblock_conses;
  size_t consblock_len;
  int consblock_contiguous;} *kno_consblock;

lispval kno_make_consblock(lispval obj);

/* Typeinfo */

typedef struct KNO_TYPEINFO *kno_typeinfo;
typedef int (*kno_type_unparsefn)(u8_output out,lispval,kno_typeinfo);
typedef lispval (*kno_type_parsefn)(int n,lispval *,kno_typeinfo);
typedef int (*kno_type_freefn)(lispval,kno_typeinfo);
typedef lispval (*kno_type_dumpfn)(lispval,kno_typeinfo);
typedef lispval (*kno_type_restorefn)(lispval,lispval,kno_typeinfo);

typedef struct KNO_TYPEINFO {
  KNO_CONS_HEADER;
  lispval typetag, type_props, type_handlers;
  u8_string type_name, type_description;
  char type_isopaque, type_ismutable;
  kno_type_parsefn type_parsefn;
  kno_type_unparsefn type_unparsefn;
  kno_type_freefn type_freefn;
  kno_type_dumpfn type_dumpfn;
  kno_type_restorefn type_restorefn;
  struct KNO_TABLEFNS *type_tablefns;
  struct KNO_SEQFNS *type_seqfns;} KNO_TYPEINFO;

KNO_EXPORT int kno_set_unparsefn(lispval tag,kno_type_unparsefn fn);
KNO_EXPORT int kno_set_parsefn(lispval tag,kno_type_parsefn fn);
KNO_EXPORT int kno_set_freefn(lispval tag,kno_type_freefn fn);
KNO_EXPORT int kno_set_dumpfn(lispval tag,kno_type_dumpfn fn);
KNO_EXPORT int kno_set_restorefn(lispval tag,kno_type_restorefn fn);

#define KNO_TAGGED_HEAD \
  KNO_CONS_HEADER; \
  lispval typetag; \
  struct KNO_TYPEINFO *typeinfo

#define KNO_TAGGEDP(x) \
  (KNO_XXCONS_TYPEP((x),kno_tagged_type))

typedef struct KNO_TAGGED {
  KNO_TAGGED_HEAD;} KNO_TAGGED;
typedef struct KNO_TAGGED *kno_tagged;

struct KNO_TYPEINFO *kno_probe_typeinfo(lispval tag);
struct KNO_TYPEINFO *kno_use_typeinfo(lispval tag);
KNO_FASTOP struct KNO_TYPEINFO *kno_taginfo(lispval obj)
{
  if ( (KNO_TAGGEDP(obj)) ) {
    struct KNO_TAGGED *tagged = (kno_tagged) obj;
    if (tagged->typeinfo)
      return tagged->typeinfo;
    else {
      struct KNO_TYPEINFO *info = kno_use_typeinfo(tagged->typetag);
      tagged->typeinfo = info;
      return info;}}
  else return NULL;
}

/* Compound types */

typedef struct KNO_WRAPPER {
  KNO_TAGGED_HEAD;
  lispval wrapped;} KNO_WRAPPER;
typedef struct KNO_WRAPPER *kno_wrapper;

/* Compound types */

typedef struct KNO_COMPOUND {
  KNO_TAGGED_HEAD;
  int compound_length;
  char compound_ismutable, compound_isopaque;
  char compound_seqoff, compound_istable;
  u8_rwlock compound_rwlock;
  lispval compound_0;} KNO_COMPOUND;
typedef struct KNO_COMPOUND *kno_compound;

/* Raw pointers */

typedef void (*kno_raw_recyclefn)(void *);

typedef struct KNO_RAWPTR {
  KNO_TAGGED_HEAD;
  void *ptrval;
  u8_string idstring;
  lispval raw_annotations;
  kno_raw_recyclefn raw_recycler;} KNO_RAWPTR;
typedef struct KNO_RAWPTR *kno_rawptr;

KNO_EXPORT lispval kno_wrap_pointer(void *ptrval,
                                 kno_raw_recyclefn recycler,
                                 lispval typespec,
                                 u8_string idstring);

KNO_EXPORT int kno_compound_set_unparser(u8_string pname,
					 kno_type_unparsefn fn);

/* Cons compare */

/* This provides a fast inline object comparison for files which
   define KNO_INLINE_COMPARE. It is equivalent to the quick version of
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
#if KNO_INLINE_COMPARE
static int base_compare(lispval x,lispval y)
{
  if (KNO_ATOMICP(x))
    if (KNO_ATOMICP(y))
      if (x < y)
        return -1;
      else if (x == y)
        return 0;
      else return 1;
    else return -1;
  else if (KNO_ATOMICP(y))
    return 1;
  else {
    kno_lisp_type xtype = KNO_TYPEOF(x);
    kno_lisp_type ytype = KNO_TYPEOF(y);
    if (KNO_NUMBER_TYPEP(xtype))
      if (KNO_NUMBER_TYPEP(ytype))
        return kno_numcompare(x,y);
      else return -1;
    else if (KNO_NUMBER_TYPEP(ytype))
      return 1;
    else if (xtype<ytype)
      return -1;
    else if (xtype>ytype)
      return 1;
    else switch (xtype) {
      case kno_pair_type: {
        int car_cmp = KNO_QCOMPARE(KNO_CAR(x),KNO_CAR(y));
        if (car_cmp == 0)
          return (KNO_QCOMPARE(KNO_CDR(x),KNO_CDR(y)));
        else return car_cmp;}
      case kno_string_type: {
        int xlen = KNO_STRLEN(x), ylen = KNO_STRLEN(y);
        if (xlen>ylen)
          return 1;
        else if (xlen<ylen)
          return -1;
        else return strncmp(KNO_CSTRING(x),KNO_CSTRING(y),xlen);}
      case kno_vector_type: {
        int i = 0, xlen = VEC_LEN(x), ylen = VEC_LEN(y);
        lispval *xdata = VEC_DATA(x), *ydata = VEC_DATA(y);
        if (xlen > ylen)
          return 1;
        else if (xlen < ylen)
          return -1;
        int minlen = (xlen < ylen) ? (xlen) : (ylen);
        while (i < minlen) {
          int cmp = KNO_QCOMPARE(xdata[i],ydata[i]);
          if (cmp)
            return cmp;
          else i++;}
        return 0;}
      default:
        return LISP_COMPARE(x,y,KNO_COMPARE_QUICK);}}
}
static int cons_compare(lispval x,lispval y)
{
  if (KNO_ATOMICP(x))
    if (KNO_ATOMICP(y))
      if (x < y)
        return -1;
      else if (x == y)
        return 0;
      else return 1;
    else return -1;
  else if (KNO_ATOMICP(y))
    return 1;
  else {
    kno_lisp_type xtype = KNO_TYPEOF(x);
    kno_lisp_type ytype = KNO_TYPEOF(y);
    if ( (KNO_NUMBER_TYPEP(xtype)) && (KNO_NUMBER_TYPEP(ytype)) )
      return kno_numcompare(x,y);
    else if (KNO_NUMBER_TYPEP(xtype))
      return -1;
    else if (KNO_NUMBER_TYPEP(ytype))
      return 1;
    else if (xtype<ytype)
      return -1;
    else if (xtype>ytype)
      return 1;
    else switch (xtype) {
      case kno_pair_type: {
        int car_cmp = base_compare(KNO_CAR(x),KNO_CAR(y));
        if (car_cmp == 0)
          return base_compare(KNO_CDR(x),KNO_CDR(y));
        else return car_cmp;}
      case kno_string_type: {
        int xlen = KNO_STRLEN(x), ylen = KNO_STRLEN(y);
        if (xlen>ylen)
          return 1;
        else if (xlen<ylen)
          return -1;
        else return strncmp(KNO_CSTRING(x),KNO_CSTRING(y),xlen);}
      case kno_vector_type: {
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
        return LISP_COMPARE(x,y,KNO_COMPARE_QUICK);}}
}

#endif

KNO_EXPORT kno_compare_flags kno_get_compare_flags(lispval spec);

/* APPLY methods */

typedef const lispval *kno_argvec;
#define KNO_ARGS const lispval *

typedef lispval (*kno_applyfn)(lispval f,int n,kno_argvec);
KNO_EXPORT kno_applyfn kno_applyfns[];

/* This maps types to whether they have function (KNO_FUNCTION_FIELDS)
   header. */
KNO_EXPORT short kno_function_types[];

/* Choices, tables, regexes */

#include "choices.h"
#include "tables.h"
#include "sequences.h"
#include "knoregex.h"

U8_MAYBE_UNUSED static int _kno_applicablep(lispval x)
{
  if (KNO_CONSP(x)) {
    kno_lisp_type objtype = KNO_CONSPTR_TYPE(x);
    return ( (objtype == kno_cprim_type) ||
             (objtype == kno_lambda_type) ||
             (kno_applyfns[objtype] != NULL) );}
  else if (KNO_IMMEDIATEP(x)) {
    kno_lisp_type xtype = KNO_IMMEDIATE_TYPE(x);
    if (xtype == kno_fcnid_type) {
      lispval fcn = kno_fcnid_ref(x);
      kno_lisp_type objtype = KNO_TYPEOF(fcn);
      return ( (objtype == kno_cprim_type) ||
               (objtype == kno_lambda_type) ||
               (kno_applyfns[objtype] != NULL) );}
    else return (kno_applyfns[xtype] != NULL);}
  else return 0;
}

U8_MAYBE_UNUSED static int _kno_extended_typep(lispval x,int typecode)
{
  switch (typecode) {
  case kno_number_type:
    return KNO_NUMBERP(x);
  case kno_sequence_type:
    return KNO_SEQUENCEP(x);
  case kno_table_type:
    return KNO_TABLEP(x);
  case kno_applicable_type:
    return _kno_applicablep(x);
  default:
    return 0;
  }
}

#define KNO_ISA(x,typecode)                                         \
  ( ( (typecode) > 0x100) ? \
    (_kno_extended_typep((x),(typecode))) :                              \
    ( (typecode) >= 0x84) ? \
    ( (KNO_CONSP(x)) && (KNO_CONSPTR_TYPE(x) == typecode) ) :             \
    ( (typecode) >= 0x04) ? \
    ( (KNO_IMMEDIATEP(x)) && (KNO_IMM_TYPE(x) == typecode ) ) :           \
    ( (typecode) >= 0x00) ? ( ( (x) & (0x3) ) == typecode) : (1) )

/* The zero-pool */

/* OIDs with a high address of zero are treated specially. They are
   "ephemeral OIDs" whose values are limited to the current runtime
   session. */

#ifndef KNO_ZERO_POOL_MAX
#define KNO_ZERO_POOL_MAX 0x10000
#endif

KNO_EXPORT lispval kno_zero_pool_values0[4096];
KNO_EXPORT lispval *kno_zero_pool_buckets[KNO_ZERO_POOL_MAX/4096];
KNO_EXPORT unsigned int kno_zero_pool_load;
KNO_EXPORT lispval kno_zero_pool_value(lispval oid);
KNO_EXPORT lispval kno_zero_pool_store(lispval oid,lispval value);

#endif /* ndef KNO_CONS_H */

