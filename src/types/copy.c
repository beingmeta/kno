/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/cons.h"


FD_EXPORT
/* fd_deep_copy:
    Arguments: a dtype pointer
    Returns: a dtype pointer
  This returns a copy of its argument, recurring to sub objects. */
lispval fd_copier(lispval x,int flags)
{
  int static_copy = U8_BITP(flags,FD_STATIC_COPY);
  if (ATOMICP(x)) return x;
  else {
    fd_ptr_type ctype = FD_CONS_TYPE(FD_CONS_DATA(x));
    switch (ctype) {
    case fd_pair_type: {
      lispval result = NIL, *tail = &result, scan = x;
      while (FD_TYPEP(scan,fd_pair_type)) {
        struct FD_PAIR *p = FD_CONSPTR(fd_pair,scan);
        struct FD_PAIR *newpair = u8_alloc(struct FD_PAIR);
        lispval car = p->car;
        FD_INIT_CONS(newpair,fd_pair_type);
        if (static_copy) {FD_MAKE_STATIC(result);}
        if (CONSP(car)) {
          struct FD_CONS *c = (struct FD_CONS *)car;
          if ( U8_BITP(flags,FD_FULL_COPY) || FD_STATIC_CONSP(c) )
            newpair->car = fd_copier(car,flags);
          else {fd_incref(car); newpair->car = car;}}
        else {
          fd_incref(car); newpair->car = car;}
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
      lispval *olddata = v->fdvec_elts; int i = 0, len = v->fdvec_length;
      lispval result = ((ctype == fd_vector_type)?
                     (fd_init_vector(NULL,len,NULL)):
                     (fd_init_code(NULL,len,NULL)));
      lispval *newdata = FD_VECTOR_ELTS(result);
      while (i<len) {
          lispval v = olddata[i], newv = v;
          if (CONSP(v)) {
            struct FD_CONS *c = (struct FD_CONS *)newv;
            if ((flags&FD_FULL_COPY)||(FD_STATIC_CONSP(c)))
              newv = fd_copier(newv,flags);
            else fd_incref(newv);}
          newdata[i++]=newv;}
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_string_type: {
      struct FD_STRING *s = FD_CONSPTR(fd_string,x);
      lispval result = fd_make_string(NULL,s->fd_bytelen,s->fd_bytes);
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_packet_type: case fd_secret_type: {
      struct FD_STRING *s = FD_CONSPTR(fd_string,x);
      lispval result;
      if (ctype == fd_secret_type) {
        result = fd_make_packet(NULL,s->fd_bytelen,s->fd_bytes);
        FD_SET_CONS_TYPE(result,fd_secret_type);
        return result;}
      else result = fd_make_packet(NULL,s->fd_bytelen,s->fd_bytes);
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    case fd_choice_type: {
      int n = FD_CHOICE_SIZE(x);
      int flags = (FD_ATOMIC_CHOICEP(x))?
        (FD_CHOICE_ISATOMIC):
        (FD_CHOICE_ISCONSES);
      struct FD_CHOICE *copy = fd_alloc_choice(n);
      const lispval *read = FD_CHOICE_DATA(x), *limit = read+n;
      lispval *write = (lispval *)&(copy->choice_0);
      lispval result;
      if (FD_ATOMIC_CHOICEP(x))
        memcpy(write,read,sizeof(lispval)*n);
      else if (flags&FD_FULL_COPY) while (read<limit) {
          lispval v = *read++, c = fd_copier(v,flags);
	  *write++=c;}
      else while (read<limit) {
          lispval v = *read++, newv = v;
          if (CONSP(newv)) {
            struct FD_CONS *c = (struct FD_CONS *)newv;
            if (FD_STATIC_CONSP(c))
              newv = fd_copier(newv,flags);
            else fd_incref(newv);}
          *write++=newv;}
      result = fd_init_choice(copy,n,NULL,flags);
      if (static_copy) {FD_MAKE_STATIC(result);}
      return result;}
    default:
      if (fd_copiers[ctype]) {
        lispval copy = (fd_copiers[ctype])(x,flags);
        if ((static_copy)&&(copy!=x)) {FD_MAKE_STATIC(copy);}
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
    fd_incref(x); return x;}
  else {
    int i = 0, n = xc->fd_n_elts;
    struct FD_COMPOUND *nc = u8_malloc(sizeof(FD_COMPOUND)+(n-1)*sizeof(lispval));
    lispval *data = &(xc->compound_0), *write = &(nc->compound_0);
    FD_INIT_CONS(nc,fd_compound_type);
    if (xc->compound_ismutable) u8_init_mutex(&(nc->compound_lock));
    nc->compound_ismutable = xc->compound_ismutable; nc->compound_isopaque = 1;
    nc->compound_typetag = fd_incref(xc->compound_typetag); 
    nc->fd_n_elts = xc->fd_n_elts;
    if (flags)
      while (i<n) {
        *write = fd_copier(data[i],flags); i++; write++;}
    else while (i<n) {
        *write = fd_incref(data[i]); i++; write++;}
    return LISP_CONS(nc);}
}

void fd_init_copy_c()
{
  u8_register_source_file(_FILEINFO);

  fd_copiers[fd_compound_type]=copy_compound;

}
