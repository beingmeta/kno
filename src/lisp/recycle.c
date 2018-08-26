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
#include "framerd/cons.h"

/* Builtin recyclers */

static void recycle_string(struct FD_STRING *s)
{
  if ( (s->str_bytes) && (s->str_freebytes) )
    u8_free(s->str_bytes);
  u8_free(s);
}

static void recycle_vector(struct FD_VECTOR *v)
{
  int len = v->vec_length;
  lispval *scan = v->vec_elts, *limit = scan+len;
  if (scan) {
    while (scan<limit) {
      fd_decref(*scan);
      scan++;}
    if (v->vec_free_elts) {
      if (v->vec_bigalloc_elts)
        u8_big_free(v->vec_elts);
      else u8_free(v->vec_elts);}}
  if (!(FD_STATIC_CONSP(v))) {
    if (v->vec_bigalloc)
      u8_big_free(v);
    else u8_free(v);}
}

static void recycle_choice(struct FD_CHOICE *cv)
{
  if (!(cv->choice_isatomic)) {
    int len = cv->choice_size;
    const lispval *scan = FD_XCHOICE_DATA(cv), *limit = scan+len;
    if (scan) while (scan<limit) {fd_decref(*scan); scan++;}}
  fd_free_choice(cv);
}

static void recycle_qchoice(struct FD_QCHOICE *qc)
{
  fd_decref(qc->qchoiceval);
  u8_free(qc);
}

/* Recycling pairs directly */

static void recycle_pair(struct FD_PAIR *pair)
{
  lispval car = pair->car, cdr = pair->cdr;
  fd_decref(car); fd_decref(cdr);
  u8_free(pair);
}

/* Recycling pairs iteratively CDR-wise */

/* We want to avoid deep call stacks for freeing long lists, so
   we iterate in the CDR direction. */
static void recycle_list(struct FD_PAIR *pair)
{
  lispval car = pair->car, cdr = pair->cdr;
  u8_free(pair); fd_decref(car);
  if (!(PAIRP(cdr))) {
    fd_decref(cdr);
    return;}
  else pair = (fd_pair)cdr;
#if FD_LOCKFREE_REFCOUNTS
  while (1) {
    struct FD_REF_CONS *cons = (struct FD_REF_CONS *)pair;
    if (FD_STATIC_CONSP(pair)) return;
    else {
      car = pair->car; cdr = pair->cdr;}
    fd_consbits newbits = atomic_fetch_sub(&(cons->conshead),0x80)-0x80;
    if (newbits<0x80) {
      fd_decref(car);
      if (PAIRP(cdr)) {
        atomic_store(&(cons->conshead),(newbits|0xFFFFFF80));
        u8_free(pair);
        pair = (fd_pair)cdr;}
      else {
        atomic_store(&(cons->conshead),(newbits|0xFFFFFF80));
        fd_decref(cdr);
        u8_free(pair);
        return;}}
    else return;}
#else
  if (CONSP(cdr)) {
    if (PAIRP(cdr)) {
      struct FD_PAIR *xcdr = (struct FD_PAIR *)cdr;
      FD_LOCK_PTR(xcdr);
      while (FD_CONS_REFCOUNT(xcdr)==1) {
        car = xcdr->car; cdr = xcdr->cdr;
        FD_UNLOCK_PTR(xcdr); u8_free(xcdr);
        fd_decref(car);
        if (PAIRP(cdr)) {
          xcdr = (fd_pair)cdr;
          FD_LOCK_PTR(xcdr);
          continue;}
        else {xcdr = NULL; break;}}
      if (xcdr) FD_UNLOCK_PTR(xcdr);
      fd_decref(cdr);}
    else fd_decref(cdr);}
#endif
}

static void recycle_uuid(struct FD_RAW_CONS *c)
{
  u8_free(c);
}

static void recycle_exception(struct FD_RAW_CONS *c)
{
  struct FD_EXCEPTION *exo = (struct FD_EXCEPTION *)c;
  if (exo->ex_details) u8_free(exo->ex_details);
  fd_decref(exo->ex_irritant);
  fd_decref(exo->ex_stack);
  fd_decref(exo->ex_context);
  u8_free(exo);
}

static void recycle_timestamp(struct FD_RAW_CONS *c)
{
  u8_free(c);
}

static void recycle_regex(struct FD_RAW_CONS *c)
{
  struct FD_REGEX *rx = (struct FD_REGEX *)c;
  regfree(&(rx->rxcompiled));
  u8_destroy_mutex(&(rx->rx_lock));
  u8_free(c);
}

static void recycle_rawptr(struct FD_RAW_CONS *c)
{
  struct FD_RAWPTR *rawptr = (struct FD_RAWPTR *)c;
  if (rawptr->recycler)
    rawptr->recycler(rawptr->ptrval);
  if (rawptr->idstring) u8_free(rawptr->idstring);
  fd_decref(rawptr->raw_typespec);
}

static void recycle_compound(struct FD_RAW_CONS *c)
{
  struct FD_COMPOUND *compound = (struct FD_COMPOUND *)c;
  lispval typetag = compound->compound_typetag;
  struct FD_COMPOUND_TYPEINFO *typeinfo = fd_lookup_compound(typetag);
  if ( (typeinfo) && (typeinfo->compound_freefn) ) {
    int rv = (typeinfo->compound_freefn)((lispval)c,typeinfo);}
  int i = 0, n = compound->compound_length;
  lispval *data = &(compound->compound_0);
  while (i<n) {fd_decref(data[i]); i++;}
  fd_decref(compound->compound_typetag);
  if (compound->compound_ismutable) u8_destroy_mutex(&(compound->compound_lock));
  u8_free(c);
}

static void recycle_mystery(struct FD_RAW_CONS *c)
{
  struct FD_MYSTERY_DTYPE *myst = (struct FD_MYSTERY_DTYPE *)c;
  if (myst->myst_dtcode&0x80)
    u8_free(myst->mystery_payload.elts);
  else u8_free(myst->mystery_payload.bytes);
  u8_free(myst);
}

/* The main function */

FD_EXPORT
/* fd_recycle_cons:
    Arguments: a pointer to an FD_CONS struct
    Returns: void
 Recycles a cons cell */
void fd_recycle_cons(fd_raw_cons c)
{
  int ctype = FD_CONS_TYPE(c);
  switch (ctype) {
  case fd_string_type: case fd_packet_type: case fd_secret_type:
    recycle_string((struct FD_STRING *)c);
    return;
  case fd_vector_type: case fd_code_type:
    recycle_vector((struct FD_VECTOR *)c);
    return;
  case fd_choice_type:
    recycle_choice((struct FD_CHOICE *)c);
    return;
  case fd_pair_type:
    recycle_list((struct FD_PAIR *)c);
    return;
  case fd_compound_type:
    recycle_compound(c);
    return;
  case fd_rational_type: case fd_complex_type:
    recycle_pair((struct FD_PAIR *)c);
    return;
  case fd_uuid_type: case fd_timestamp_type:
    u8_free(c);
    return;
  case fd_regex_type:
    recycle_regex(c);
    return;
  case fd_rawptr_type:
    recycle_rawptr(c);
    return;
  case fd_qchoice_type:
    recycle_qchoice((struct FD_QCHOICE *)c);
    return;
  default: {
    if (fd_recyclers[ctype]) fd_recyclers[ctype](c);}
  }
}

FD_EXPORT
/* Increfs the elements of a vector of LISP pointers */
void fd_incref_vec(lispval *vec,size_t n)
{
  int i = 0; while (i<n) {
    lispval elt = vec[i];
    if ( (FD_CONSP(elt)) && (FD_STATIC_CONSP(elt) ) ) {
      vec[i] = fd_copier(elt,FD_FULL_COPY);}
    else fd_incref(elt);
    i++;}
}

FD_EXPORT
/* Decrefs the elements of a vector of LISP pointers */
void fd_decref_vec(lispval *vec,size_t n)
{
  int i = 0; while (i<n) {
    lispval elt = vec[i++];
    fd_decref(elt);}
}

void fd_init_recycle_c()
{

  fd_recyclers[fd_exception_type]=recycle_exception;
  fd_recyclers[fd_mystery_type]=recycle_mystery;
  fd_recyclers[fd_uuid_type]=recycle_uuid;
  fd_recyclers[fd_timestamp_type]=recycle_timestamp;
  fd_recyclers[fd_regex_type]=recycle_regex;

  fd_recyclers[fd_compound_type]=recycle_compound;

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
