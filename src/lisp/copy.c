/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"

#if 0 /* From cons.h */
#define FD_DEEP_COPY 2   /* Make a deep copy */
#define FD_FULL_COPY 4   /* Copy non-static objects */
#define FD_STRICT_COPY 8 /* Require methods for all objects */
#define FD_STATIC_COPY 16 /* Declare all copied objects static (this leaks) */
#define FD_COPY_TERMINALS 32 /* Copy even terminal objects */
#endif

FD_FASTOP lispval copy_elt(lispval elt,int flags)
{
  if (FD_CONSP(elt)) {
    if (FD_STATIC_CONSP(elt))
      return fd_copier(elt,flags);
    else if (! ( (flags) & (FD_FULL_COPY) ) )
      return fd_incref(elt);
    else {
      struct FD_CONS *cons = (fd_cons) elt;
      fd_ptr_type cons_type = FD_CONS_TYPE(cons);
      switch (cons_type) {
      case fd_packet_type: case fd_string_type:
      case fd_secret_type: case fd_bigint_type:
      case fd_cprim_type: case fd_evalfn_type:
      case fd_macro_type: case fd_dtproc_type:
      case fd_ffi_type: case fd_timestamp_type:
      case fd_uuid_type: case fd_mystery_type:
      case fd_rawptr_type: case fd_regex_type:
      case fd_port_type: case fd_stream_type:
      case fd_dtserver_type: case fd_bloom_filter_type:
        if ( (flags) & (FD_COPY_TERMINALS) )
          return fd_copier(elt,flags);
        else return fd_incref(elt);
      default:
        return fd_copier(elt,flags);}}}
  else return elt;
}


FD_EXPORT
/* fd_deep_copy:
    Arguments: a dtype pointer
    Returns: a dtype pointer
  This returns a copy of its argument, possibly recurring to sub objects,
  with lots of options. */
lispval fd_copier(lispval x,int flags)
{
  int static_copy = U8_BITP(flags,FD_STATIC_COPY);
  if (ATOMICP(x))
    return x;
  else {
    fd_ptr_type ctype = FD_CONS_TYPE(FD_CONS_DATA(x));
    switch (ctype) {
    case fd_pair_type: {
      lispval result = NIL, *tail = &result, scan = x;
      while (TYPEP(scan,fd_pair_type)) {
        struct FD_PAIR *p = FD_CONSPTR(fd_pair,scan);
        struct FD_PAIR *newpair = u8_alloc(struct FD_PAIR);
        lispval car = p->car;
        FD_INIT_CONS(newpair,fd_pair_type);
        if (static_copy) {FD_MAKE_STATIC(result);}
        newpair->car = copy_elt(car,flags);
        *tail = (lispval)newpair;
        tail = &(newpair->cdr);
        scan = p->cdr;}
      if (CONSP(scan))
        *tail = fd_copier(scan,flags);
      else *tail = scan;
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_vector_type: case fd_code_type: {
      struct FD_VECTOR *v = FD_CONSPTR(fd_vector,x);
      lispval *olddata = v->vec_elts; int i = 0, len = v->vec_length;
      lispval result = ((ctype == fd_vector_type)?
                        (fd_empty_vector(len)):
                        (fd_init_code(NULL,len,NULL)));
      lispval *newdata = FD_VECTOR_ELTS(result);
      while (i<len) {
        lispval v = olddata[i];
        newdata[i++]=copy_elt(v,flags);}
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_string_type: {
      struct FD_STRING *s = FD_CONSPTR(fd_string,x);
      lispval result = fd_make_string(NULL,s->str_bytelen,s->str_bytes);
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_packet_type: case fd_secret_type: {
      struct FD_STRING *s = FD_CONSPTR(fd_string,x);
      lispval result;
      if (ctype == fd_secret_type) {
        result = fd_make_packet(NULL,s->str_bytelen,s->str_bytes);
        FD_SET_CONS_TYPE(result,fd_secret_type);
        return result;}
      else result = fd_make_packet(NULL,s->str_bytelen,s->str_bytes);
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_choice_type: {
      int n = FD_CHOICE_SIZE(x);
      int choice_flags = (FD_ATOMIC_CHOICEP(x))?
        (FD_CHOICE_ISATOMIC):
        (FD_CHOICE_ISCONSES);
      struct FD_CHOICE *copy = fd_alloc_choice(n);
      const lispval *read = FD_CHOICE_DATA(x), *limit = read+n;
      lispval *write = (lispval *)&(copy->choice_0);
      lispval result;
      if (FD_ATOMIC_CHOICEP(x))
        memcpy(write,read,LISPVEC_BYTELEN(n));
      else while (read<limit) {
          lispval v = *read++, c = copy_elt(v,flags);
          *write++=c;}
      result = fd_init_choice(copy,n,NULL,choice_flags);
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    default:
      if (fd_copiers[ctype]) {
        lispval copy = (fd_copiers[ctype])(x,flags);
        if ((static_copy) && (copy!=x)) {FD_MAKE_STATIC(copy);}
        return copy;}
      else if (!(FD_MALLOCD_CONSP((fd_cons)x)))
        return fd_err(fd_NoMethod,"fd_copier/static",
                      fd_type_names[ctype],VOID);
      else if ((flags)&(FD_STRICT_COPY))
        return fd_err(fd_NoMethod,"fd_copier",fd_type_names[ctype],x);
      else {fd_incref(x); return x;}}}
}

FD_EXPORT
lispval fd_deep_copy(lispval x)
{
  return fd_copier(x,(FD_DEEP_COPY|FD_FULL_COPY));
}

FD_EXPORT
lispval fd_static_copy(lispval x)
{
  return fd_copier(x,(FD_DEEP_COPY|FD_FULL_COPY|FD_STATIC_COPY));
}

FD_EXPORT
/* Copies a vector of LISP pointers */
lispval *fd_copy_vec(lispval *vec,size_t n,lispval *into,int flags)
{
  lispval *dest = (into == NULL)?(u8_alloc_n(n,lispval)):(into);
  int i = 0; while (i<n) {
    lispval elt = vec[i];
    if (elt == FD_NULL)
      break;
    else if (!(CONSP(elt)))
      dest[i]=elt;
    else if (U8_BITP(flags,FD_DEEP_COPY)) {
      dest[i]=fd_copier(elt,flags);}
    else
      dest[i]=fd_incref(elt);
    i++;}
  while (i<n) dest[i++]=FD_NULL;
  return dest;
}

FD_EXPORT
/* fd_copy:
    Arguments: a dtype pointer
    Returns: a dtype pointer
  If the argument is a malloc'd cons, this just increfs it.
  If it is a static cons, it does a deep copy. */
lispval fd_copy(lispval x)
{
  if (!(CONSP(x))) return x;
  else if (FD_MALLOCD_CONSP(((fd_cons)x)))
    return fd_incref(x);
  else return fd_copier(x,0);
}

static lispval copy_compound(lispval x,int flags)
{
  struct FD_COMPOUND *xc = fd_consptr(struct FD_COMPOUND *,x,fd_compound_type);
  if (xc->compound_isopaque) {
    fd_incref(x);
    return x;}
  else {
    int i = 0, n = xc->compound_length;
    struct FD_COMPOUND *nc = u8_malloc(sizeof(FD_COMPOUND)+(n-1)*LISPVAL_LEN);
    lispval *data = &(xc->compound_0), *write = &(nc->compound_0);
    FD_INIT_CONS(nc,fd_compound_type);
    if (xc->compound_ismutable) u8_init_mutex(&(nc->compound_lock));
    nc->compound_ismutable = xc->compound_ismutable;
    nc->compound_isopaque = xc->compound_isopaque;
    nc->compound_typetag = fd_incref(xc->compound_typetag);
    nc->compound_length = xc->compound_length;
    nc->compound_off = xc->compound_off;
    if (flags)
      while (i<n) {
        *write = fd_copier(data[i],flags);
        write++;
        i++;}
    else while (i<n) {
        *write = fd_incref(data[i]);
        write++;
        i++;}
    return LISP_CONS(nc);}
}

void fd_init_copy_c()
{
  u8_register_source_file(_FILEINFO);

  fd_copiers[fd_compound_type]=copy_compound;

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
