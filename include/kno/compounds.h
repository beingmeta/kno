/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_COMPOUNDS_H
#define KNO_COMPOUNDS_H 1
#ifndef KNO_COMPOUNDS_H_INFO
#define KNO_COMPOUNDS_H_INFO "include/kno/compounds.h"
#endif

#include "cons.h"

/* Compounds */

#define KNO_COMPOUNDP(x) (KNO_TYPEOF(x) == kno_compound_type)
#define KNO_COMPOUND_TAG(x) \
  ((kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))->typetag)
#define KNO_COMPOUND_DATA(x) \
  ((kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))->elt0)
#define KNO_COMPOUND_TYPEP(x,tag)                        \
  ((KNO_TYPEOF(x) == kno_compound_type) && (KNO_COMPOUND_TAG(x) == tag))
#define KNO_COMPOUND_ELTS(x) \
  (&((kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))->compound_0))
#define KNO_COMPOUND_LENGTH(x) \
  ((kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))->compound_length)
#define KNO_COMPOUND_REF(x,i)                                            \
  ((&((kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))->compound_0))[i])
#define KNO_COMPOUND_VREF(x,i) ((&((x)->compound_0))[i])
#define KNO_XCOMPOUND(x) (kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))
#define KNO_2COMPOUND(x) ((kno_compound)(x))

#define KNO_COMPOUND_VECLEN(x) \
  ( ( (KNO_TYPEOF(x) == kno_compound_type) &&				\
      ((KNO_2COMPOUND(x))->compound_seqoff>=0) ) ?			\
    (((KNO_XCOMPOUND(x))->compound_length)-				\
     ((KNO_2COMPOUND(x))->compound_seqoff)) :				\
    (-1) )
#define KNO_COMPOUND_VECELTS(x)						\
  ( ( (KNO_TYPEOF(x) == kno_compound_type) &&				\
      ((KNO_2COMPOUND(x))->compound_seqoff>=0) ) ?			\
    ((&(((KNO_2COMPOUND(x))->compound_0)))+				\
     ((KNO_2COMPOUND(x))->compound_seqoff)) :				\
    (NULL) )
#define KNO_XCOMPOUND_VECREF(x,i)					\
  ( (&((KNO_XCOMPOUND(x))->compound_0))\
    [(i)+((KNO_2COMPOUND(x))->compound_seqoff)])

KNO_EXPORT lispval kno_init_compound
  (struct KNO_COMPOUND *ptr,lispval tag,int flags,int n,...);
KNO_EXPORT lispval kno_init_compound_from_elts
  (struct KNO_COMPOUND *p,lispval tag,int flags,int n,lispval *elts);
KNO_EXPORT lispval kno_compound_ref(lispval arg,lispval tag,int off,lispval dflt);

#define KNO_COMPOUND_MUTABLE    0x01
#define KNO_COMPOUND_OPAQUE     0x02
#define KNO_COMPOUND_SEQUENCE   0x04
#define KNO_COMPOUND_TABLE      0x08
#define KNO_COMPOUND_REFMASK    0x30

#define KNO_COMPOUND_INCREF     0x10
#define KNO_COMPOUND_COPYREF    0x20 /* Copy elements */
#define KNO_COMPOUND_USEREF     0x30 /* Decref on error */

#define KNO_COMPOUND_MUTABLEP(x) \
  ( (KNO_TYPEOF(x) == kno_compound_type) && \
    ((KNO_2COMPOUND(x))->compound_ismutable) )
#define KNO_COMPOUND_OPAQUEP(x) \
  ( (KNO_TYPEOF(x) == kno_compound_type) && \
    ((KNO_2COMPOUND(x))->compound_isopaque) )

#define KNO_COMPOUND_VECTORP(x) \
  ( (KNO_TYPEOF(x) == kno_compound_type) && \
    ((KNO_2COMPOUND(x))->compound_seqoff>=0) )
#define KNO_COMPOUND_SEQOFFSET(x) \
  ((kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))->compound_seqoff)

#define KNO_COMPOUND_HEADER_LENGTH(flags) \
  ( ((flags)&(KNO_COMPOUND_SEQUENCE)) ?	  \
    ( ((flags)&(KNO_COMPOUND_TABLE)) ?	  \
      ( (((flags)>>8)&(0x8f))+1 )  :	  \
      (((flags)>>8)&(0x8f))) :		  \
    (-1))

#define KNO_BIG_COMPOUND_LENGTH 1024

KNO_EXPORT int kno_compound_set_schema(lispval tag,lispval schema);

#endif

