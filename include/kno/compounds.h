/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_COMPOUNDS_H
#define KNO_COMPOUNDS_H 1
#ifndef KNO_COMPOUNDS_H_INFO
#define KNO_COMPOUNDS_H_INFO "include/kno/compounds.h"
#endif

/* Compounds */

typedef struct KNO_COMPOUND {
  KNO_CONS_HEADER;
  lispval compound_typetag;
  int compound_length;
  unsigned int compound_ismutable:1;
  unsigned int compound_isopaque:1;
  unsigned int compound_istable:1;
  char compound_off;
  u8_rwlock compound_rwlock;
  lispval compound_0;} KNO_COMPOUND;
typedef struct KNO_COMPOUND *kno_compound;

#define KNO_COMPOUNDP(x) (KNO_LISP_TYPE(x) == kno_compound_type)
#define KNO_COMPOUND_TAG(x) \
  ((kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))->compound_typetag)
#define KNO_COMPOUND_DATA(x) \
  ((kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))->elt0)
#define KNO_COMPOUND_TYPEP(x,tag)                        \
  ((KNO_LISP_TYPE(x) == kno_compound_type) && (KNO_COMPOUND_TAG(x) == tag))
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
  ( ( (KNO_LISP_TYPE(x) == kno_compound_type) && ((KNO_2COMPOUND(x))->compound_off>=0) ) ? \
    (((KNO_XCOMPOUND(x))->compound_length)-((KNO_2COMPOUND(x))->compound_off)) : (-1) )
#define KNO_COMPOUND_VECELTS(x) \
  ( ( (KNO_LISP_TYPE(x) == kno_compound_type) && ((KNO_2COMPOUND(x))->compound_off>=0) ) ? \
    ((&(((KNO_2COMPOUND(x))->compound_0)))+((KNO_2COMPOUND(x))->compound_off)) : (NULL) )
#define KNO_XCOMPOUND_VECREF(x,i)                                            \
  ( (&((KNO_XCOMPOUND(x))->compound_0))[(i)+((KNO_2COMPOUND(x))->compound_off)])

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

#define KNO_COMPOUND_TABLEP(x) \
  ( (KNO_LISP_TYPE(x) == kno_compound_type) && \
    ((KNO_2COMPOUND(x))->compound_istable) )
#define KNO_COMPOUND_MUTABLEP(x) \
  ( (KNO_LISP_TYPE(x) == kno_compound_type) && \
    ((KNO_2COMPOUND(x))->compound_ismutable) )
#define KNO_COMPOUND_OPAQUEP(x) \
  ( (KNO_LISP_TYPE(x) == kno_compound_type) && \
    ((KNO_2COMPOUND(x))->compound_isopaque) )

#define KNO_COMPOUND_VECTORP(x) \
  ( (KNO_LISP_TYPE(x) == kno_compound_type) && \
    ((KNO_2COMPOUND(x))->compound_off>=0) )
#define KNO_COMPOUND_OFFSET(x) \
  ((kno_consptr(struct KNO_COMPOUND *,x,kno_compound_type))->compound_off)

#define KNO_COMPOUND_HEADER(n) \
  ((((n)&0x8f)<<8)|(KNO_COMPOUND_SEQUENCE))
#define KNO_COMPOUND_HEADER_LENGTH(flags) \
  ( ((flags)&(KNO_COMPOUND_SEQUENCE)) ? (((n)>>8)&(0x8f)) : (-1))

KNO_EXPORT lispval kno_compound_descriptor_type;

#define KNO_COMPOUND_DESCRIPTORP(x) \
  (KNO_COMPOUND_TYPEP(x,kno_compound_descriptor_type))

#define KNO_COMPOUND_TYPE_TAG 0
#define KNO_COMPOUND_TYPE_SIZE 1
#define KNO_COMPOUND_TYPE_FIELDS 2
#define KNO_COMPOUND_TYPE_INITFN 3
#define KNO_COMPOUND_TYPE_FREEFN 4
#define KNO_COMPOUND_TYPE_COMPAREFN 5
#define KNO_COMPOUND_TYPE_STRINGFN 6
#define KNO_COMPOUND_TYPE_DUMPFN 7
#define KNO_COMPOUND_TYPE_RESTOREFN 8

#define KNO_BIG_COMPOUND_LENGTH 1024

#endif

