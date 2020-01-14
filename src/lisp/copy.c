/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"

#if 0 /* From cons.h */
#define KNO_DEEP_COPY 2   /* Make a deep copy */
#define KNO_FULL_COPY 4   /* Copy non-static objects */
#define KNO_STRICT_COPY 8 /* Require methods for all objects */
#define KNO_STATIC_COPY 16 /* Declare all copied objects static (this leaks) */
#define KNO_COPY_TERMINALS 32 /* Copy even terminal objects */
#endif

KNO_FASTOP lispval copy_elt(lispval elt,int flags)
{
  if (KNO_CONSP(elt)) {
    if (KNO_STATIC_CONSP(elt))
      return kno_copier(elt,flags);
    else if (! ( (flags) & (KNO_FULL_COPY) ) )
      return kno_incref(elt);
    else {
      struct KNO_CONS *cons = (kno_cons) elt;
      kno_lisp_type cons_type = KNO_CONS_TYPE(cons);
      switch (cons_type) {
      case kno_packet_type: case kno_string_type:
      case kno_secret_type: case kno_bigint_type:
      case kno_cprim_type: case kno_evalfn_type:
      case kno_macro_type: case kno_rpcproc_type:
      case kno_ffi_type: case kno_timestamp_type:
      case kno_uuid_type: case kno_mystery_type:
      case kno_rawptr_type: case kno_regex_type:
      case kno_ioport_type: case kno_stream_type:
      case kno_dtserver_type: case kno_bloom_filter_type:
        if ( (flags) & (KNO_COPY_TERMINALS) )
          return kno_copier(elt,flags);
        else return kno_incref(elt);
      default:
        return kno_copier(elt,flags);}}}
  else return elt;
}


KNO_EXPORT
/* kno_deep_copy:
   Arguments: a dtype pointer
   Returns: a dtype pointer
   This returns a copy of its argument, possibly recurring to sub objects,
   with lots of options. */
lispval kno_copier(lispval x,int flags)
{
  int static_copy = U8_BITP(flags,KNO_STATIC_COPY);
  if (ATOMICP(x))
    return x;
  else {
    kno_lisp_type ctype = KNO_CONS_TYPE(KNO_CONS_DATA(x));
    switch (ctype) {
    case kno_pair_type: {
      lispval result = NIL, *tail = &result, scan = x;
      while (TYPEP(scan,kno_pair_type)) {
        struct KNO_PAIR *p = KNO_CONSPTR(kno_pair,scan);
        struct KNO_PAIR *newpair = u8_alloc(struct KNO_PAIR);
        lispval car = p->car;
        KNO_INIT_CONS(newpair,kno_pair_type);
        if (static_copy) {KNO_MAKE_STATIC(result);}
        newpair->car = copy_elt(car,flags);
        *tail = (lispval)newpair;
        tail = &(newpair->cdr);
        scan = p->cdr;}
      if (CONSP(scan))
        *tail = kno_copier(scan,flags);
      else *tail = scan;
      if (static_copy) {KNO_MAKE_STATIC(result);}
      return result;}
    case kno_vector_type: {
      struct KNO_VECTOR *v = KNO_CONSPTR(kno_vector,x);
      lispval *olddata = v->vec_elts; int i = 0, len = v->vec_length;
      lispval result = kno_empty_vector(len);
      lispval *newdata = KNO_VECTOR_ELTS(result);
      while (i<len) {
        lispval v = olddata[i];
        newdata[i++]=copy_elt(v,flags);}
      if (static_copy) {KNO_MAKE_STATIC(result);}
      return result;}
    case kno_string_type: {
      struct KNO_STRING *s = KNO_CONSPTR(kno_string,x);
      lispval result = kno_make_string(NULL,s->str_bytelen,s->str_bytes);
      if (static_copy) {KNO_MAKE_STATIC(result);}
      return result;}
    case kno_packet_type: case kno_secret_type: {
      struct KNO_STRING *s = KNO_CONSPTR(kno_string,x);
      lispval result;
      if (ctype == kno_secret_type) {
        result = kno_make_packet(NULL,s->str_bytelen,s->str_bytes);
        KNO_SET_CONS_TYPE(result,kno_secret_type);
        return result;}
      else result = kno_make_packet(NULL,s->str_bytelen,s->str_bytes);
      if (static_copy) {KNO_MAKE_STATIC(result);}
      return result;}
    case kno_choice_type: {
      int n = KNO_CHOICE_SIZE(x);
      int choice_flags = (KNO_ATOMIC_CHOICEP(x))?
        (KNO_CHOICE_ISATOMIC):
        (KNO_CHOICE_ISCONSES);
      struct KNO_CHOICE *copy = kno_alloc_choice(n);
      const lispval *read = KNO_CHOICE_DATA(x), *limit = read+n;
      lispval *write = (lispval *)&(copy->choice_0);
      lispval result;
      if (KNO_ATOMIC_CHOICEP(x))
        memcpy(write,read,LISPVEC_BYTELEN(n));
      else while (read<limit) {
          lispval v = *read++, c = copy_elt(v,flags);
          *write++=c;}
      result = kno_init_choice(copy,n,NULL,choice_flags);
      if (static_copy) {KNO_MAKE_STATIC(result);}
      return result;}
    default:
      if (kno_copiers[ctype]) {
        lispval copy = (kno_copiers[ctype])(x,flags);
        if ((static_copy) && (copy!=x)) {KNO_MAKE_STATIC(copy);}
        return copy;}
      else if (!(KNO_MALLOCD_CONSP((kno_cons)x)))
        return kno_err(kno_NoMethod,"kno_copier/static",
                       kno_type_names[ctype],VOID);
      else if ((flags)&(KNO_STRICT_COPY))
        return kno_err(kno_NoMethod,"kno_copier",kno_type_names[ctype],x);
      else {kno_incref(x); return x;}}}
}

KNO_EXPORT
lispval kno_deep_copy(lispval x)
{
  return kno_copier(x,(KNO_DEEP_COPY|KNO_FULL_COPY));
}

KNO_EXPORT
lispval kno_static_copy(lispval x)
{
  return kno_copier(x,(KNO_DEEP_COPY|KNO_FULL_COPY|KNO_STATIC_COPY));
}

KNO_EXPORT
/* Copies a vector of LISP pointers */
lispval *kno_copy_vec(lispval *vec,size_t n,lispval *into,int flags)
{
  lispval *dest = (into == NULL)?(u8_alloc_n(n,lispval)):(into);
  int i = 0; while (i<n) {
    lispval elt = vec[i];
    if (elt == KNO_NULL)
      break;
    else if (!(CONSP(elt)))
      dest[i]=elt;
    else if (U8_BITP(flags,KNO_DEEP_COPY)) {
      dest[i]=kno_copier(elt,flags);}
    else
      dest[i]=kno_incref(elt);
    i++;}
  while (i<n) dest[i++]=KNO_NULL;
  return dest;
}

KNO_EXPORT
/* kno_copy:
   Arguments: a dtype pointer
   Returns: a dtype pointer
   If the argument is a malloc'd cons, this just increfs it.
   If it is a static cons, it does a deep copy. */
lispval kno_copy(lispval x)
{
  if (!(CONSP(x))) return x;
  else if (KNO_MALLOCD_CONSP(((kno_cons)x)))
    return kno_incref(x);
  else return kno_copier(x,0);
}

static lispval copy_compound(lispval x,int flags)
{
  struct KNO_COMPOUND *xc = kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type);
  if (xc->compound_isopaque) {
    kno_incref(x);
    return x;}
  else {
    int i = 0, n = xc->compound_length;
    struct KNO_COMPOUND *nc =
      u8_zmalloc(sizeof(KNO_COMPOUND)+(n-1)*LISPVAL_LEN);
    lispval *data = &(xc->compound_0), *write = &(nc->compound_0);
    KNO_INIT_CONS(nc,kno_compound_type);
    if (xc->compound_ismutable) u8_init_rwlock(&(nc->compound_rwlock));
    nc->compound_ismutable = xc->compound_ismutable;
    nc->compound_isopaque = xc->compound_isopaque;
    nc->typetag = kno_incref(xc->typetag);
    nc->compound_length = xc->compound_length;
    nc->compound_seqoff = xc->compound_seqoff;
    if (flags)
      while (i<n) {
        *write = kno_copier(data[i],flags);
        write++;
        i++;}
    else while (i<n) {
        *write = kno_incref(data[i]);
        write++;
        i++;}
    return LISP_CONS(nc);}
}

void kno_init_copy_c()
{
  u8_register_source_file(_FILEINFO);

  kno_copiers[kno_compound_type]=copy_compound;

}

