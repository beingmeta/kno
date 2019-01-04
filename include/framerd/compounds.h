/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_COMPOUNDS_H
#define FRAMERD_COMPOUNDS_H 1
#ifndef FRAMERD_COMPOUNDS_H_INFO
#define FRAMERD_COMPOUNDS_H_INFO "include/framerd/compounds.h"
#endif

/* Compounds */

typedef struct FD_COMPOUND {
  FD_CONS_HEADER;
  lispval compound_typetag;
  int compound_length;
  unsigned int compound_ismutable:1;
  unsigned int compound_isopaque:1;
  unsigned int compound_istable:1;
  char compound_off;
  u8_mutex compound_lock;
  lispval compound_0;} FD_COMPOUND;
typedef struct FD_COMPOUND *fd_compound;

#define FD_COMPOUNDP(x) (FD_PTR_TYPE(x) == fd_compound_type)
#define FD_COMPOUND_TAG(x) \
  ((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->compound_typetag)
#define FD_COMPOUND_DATA(x) \
  ((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->elt0)
#define FD_COMPOUND_TYPEP(x,tag)                        \
  ((FD_PTR_TYPE(x) == fd_compound_type) && (FD_COMPOUND_TAG(x) == tag))
#define FD_COMPOUND_ELTS(x) \
  (&((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->compound_0))
#define FD_COMPOUND_LENGTH(x) \
  ((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->compound_length)
#define FD_COMPOUND_REF(x,i)                                            \
  ((&((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->compound_0))[i])
#define FD_COMPOUND_VREF(x,i) ((&((x)->compound_0))[i])
#define FD_XCOMPOUND(x) (fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))
#define FD_2COMPOUND(x) ((fd_compound)(x))

#define FD_COMPOUND_VECLEN(x) \
  ( ( (FD_PTR_TYPE(x) == fd_compound_type) && ((FD_2COMPOUND(x))->compound_off>=0) ) ? \
    (((FD_XCOMPOUND(x))->compound_length)-((FD_2COMPOUND(x))->compound_off)) : (-1) )
#define FD_COMPOUND_VECELTS(x) \
  ( ( (FD_PTR_TYPE(x) == fd_compound_type) && ((FD_2COMPOUND(x))->compound_off>=0) ) ? \
    ((&(((FD_2COMPOUND(x))->compound_0)))+((FD_2COMPOUND(x))->compound_off)) : (NULL) )
#define FD_XCOMPOUND_VECREF(x,i)                                            \
  ( (&((FD_XCOMPOUND(x))->compound_0))[(i)+((FD_2COMPOUND(x))->compound_off)])

FD_EXPORT lispval fd_init_compound
  (struct FD_COMPOUND *ptr,lispval tag,int flags,int n,...);
FD_EXPORT lispval fd_init_compound_from_elts
  (struct FD_COMPOUND *p,lispval tag,int flags,int n,lispval *elts);
FD_EXPORT lispval fd_compound_ref(lispval arg,lispval tag,int off,lispval dflt);

#define FD_COMPOUND_MUTABLE    0x01
#define FD_COMPOUND_OPAQUE     0x02
#define FD_COMPOUND_SEQUENCE   0x04
#define FD_COMPOUND_TABLE      0x08
#define FD_COMPOUND_REFMASK    0x30

#define FD_COMPOUND_INCREF     0x10
#define FD_COMPOUND_COPYREF    0x20 /* Copy elements */
#define FD_COMPOUND_USEREF     0x30 /* Decref on error */

#define FD_COMPOUND_TABLEP(x) \
  ( (FD_PTR_TYPE(x) == fd_compound_type) && \
    ((FD_2COMPOUND(x))->compound_istable) )
#define FD_COMPOUND_MUTABLEP(x) \
  ( (FD_PTR_TYPE(x) == fd_compound_type) && \
    ((FD_2COMPOUND(x))->compound_ismutable) )
#define FD_COMPOUND_OPAQUEP(x) \
  ( (FD_PTR_TYPE(x) == fd_compound_type) && \
    ((FD_2COMPOUND(x))->compound_isopaque) )

#define FD_COMPOUND_VECTORP(x) \
  ( (FD_PTR_TYPE(x) == fd_compound_type) && \
    ((FD_2COMPOUND(x))->compound_off>=0) )
#define FD_COMPOUND_OFFSET(x) \
  ((fd_consptr(struct FD_COMPOUND *,x,fd_compound_type))->compound_off)

#define FD_COMPOUND_HEADER(n) \
  ((((n)&0x8f)<<8)|(FD_COMPOUND_SEQUENCE))
#define FD_COMPOUND_HEADER_LENGTH(flags) \
  ( ((flags)&(FD_COMPOUND_SEQUENCE)) ? (((n)>>8)&(0x8f)) : (-1))

FD_EXPORT lispval fd_compound_descriptor_type;

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

#define FD_BIG_COMPOUND_LENGTH 1024

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
