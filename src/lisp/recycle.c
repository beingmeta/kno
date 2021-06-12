/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2020 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_LISP_CORE 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/cons.h"
#include "kno/compounds.h"

kno_type_freefn kno_default_freefn = NULL;

/* Builtin recyclers */

static void recycle_string(struct KNO_STRING *s)
{
  if ( (s->str_bytes) && (s->str_freebytes) )
    u8_free(s->str_bytes);
  u8_free(s);
}

static void recycle_vector(struct KNO_VECTOR *v)
{
  int len = v->vec_length;
  lispval *scan = v->vec_elts, *limit = scan+len;
  if (scan) {
    while (scan<limit) {
      kno_decref(*scan);
      scan++;}
    if (v->vec_free_elts) {
      if (v->vec_bigalloc_elts)
        u8_big_free(v->vec_elts);
      else u8_free(v->vec_elts);}}
  if (!(KNO_STATIC_CONSP(v))) {
    if (v->vec_bigalloc)
      u8_big_free(v);
    else u8_free(v);}
}

static void recycle_choice(struct KNO_CHOICE *cv)
{
  if (!(cv->choice_isatomic)) {
    int len = cv->choice_size;
    const lispval *scan = KNO_XCHOICE_DATA(cv), *limit = scan+len;
    if (scan) while (scan<limit) {kno_decref(*scan); scan++;}}
  kno_free_choice(cv);
}

static void recycle_qchoice(struct KNO_QCHOICE *qc)
{
  kno_decref(qc->qchoiceval);
  u8_free(qc);
}

/* Recycling pairs directly */

static void recycle_pair(struct KNO_PAIR *pair)
{
  lispval car = pair->car, cdr = pair->cdr;
  kno_decref(car); kno_decref(cdr);
  u8_free(pair);
}

/* Recycling pairs iteratively CDR-wise */

/* We want to avoid deep call stacks for freeing long lists, so
   we iterate in the CDR direction. */
static void recycle_list(struct KNO_PAIR *pair)
{
  lispval car = pair->car, cdr = pair->cdr;
  kno_decref(car);
  if (!(PAIRP(cdr))) {
    kno_decref(cdr);
    u8_free(pair);
    return;}
  else {
    u8_free(pair);
    pair = (kno_pair)cdr;}
#if KNO_LOCKFREE_REFCOUNTS
  while (1) {
    struct KNO_REF_CONS *cons = (struct KNO_REF_CONS *)pair;
    if (KNO_STATIC_CONSP(pair)) return;
    else {
      car = pair->car; cdr = pair->cdr;}
    kno_consbits newbits = atomic_fetch_sub(&(cons->conshead),0x80)-0x80;
    if (newbits<0x80) {
      kno_decref(car);
      if (PAIRP(cdr)) {
        atomic_store(&(cons->conshead),(newbits|0xFFFFFF80));
        u8_free(pair);
        pair = (kno_pair)cdr;}
      else {
        atomic_store(&(cons->conshead),(newbits|0xFFFFFF80));
        kno_decref(cdr);
        u8_free(pair);
        return;}}
    else return;}
#else
  if (CONSP(cdr)) {
    if (PAIRP(cdr)) {
      struct KNO_PAIR *xcdr = (struct KNO_PAIR *)cdr;
      KNO_LOCK_PTR(xcdr);
      while (KNO_CONS_REFCOUNT(xcdr)==1) {
        car = xcdr->car; cdr = xcdr->cdr;
        KNO_UNLOCK_PTR(xcdr);
        kno_decref(car);
        u8_free(xcdr); xcdr = NULL;
        if (PAIRP(cdr)) {
          xcdr = (kno_pair)cdr;
          KNO_LOCK_PTR(xcdr);
          continue;}
        else break;}
      if (xcdr) KNO_UNLOCK_PTR(xcdr);
      kno_decref(cdr);}
    else kno_decref(cdr);}
#endif
}

static void recycle_uuid(struct KNO_RAW_CONS *c)
{
  u8_free(c);
}

static void recycle_exception(struct KNO_RAW_CONS *c)
{
  struct KNO_EXCEPTION *exo = (struct KNO_EXCEPTION *)c;
  if (exo->ex_details) u8_free(exo->ex_details);
  kno_decref(exo->ex_irritant);
  kno_decref(exo->ex_stack);
  kno_decref(exo->ex_context);
  u8_free(exo);
}

static void recycle_timestamp(struct KNO_RAW_CONS *c)
{
  u8_free(c);
}

static void recycle_regex(struct KNO_RAW_CONS *c)
{
  struct KNO_REGEX *rx = (struct KNO_REGEX *)c;
  regfree(&(rx->rxcompiled));
  u8_destroy_mutex(&(rx->rx_lock));
  u8_free(rx->rxsrc);
  u8_free(c);
}

static void recycle_rawptr(struct KNO_RAW_CONS *c)
{
  struct KNO_RAWPTR *rawptr = (struct KNO_RAWPTR *)c;
  struct KNO_TYPEINFO *info = rawptr->typeinfo;
  if (info->type_freefn)
    info->type_freefn((lispval)c,info);
  void *ptrval = rawptr->ptrval; rawptr->ptrval=NULL;
  if ( (rawptr->raw_recycler) && (ptrval) )
    rawptr->raw_recycler(ptrval);
  u8_string idstring = rawptr->idstring; rawptr->idstring=NULL;
  if (idstring) u8_free(idstring);
  kno_decref(rawptr->raw_annotations); rawptr->raw_annotations=KNO_FALSE;
  kno_decref(rawptr->raw_cleanup); rawptr->raw_cleanup=KNO_EMPTY;
  lispval typetag = rawptr->typetag; rawptr->typetag = KNO_VOID;
  kno_decref(typetag);
  u8_free(c);
}

static void recycle_compound(struct KNO_RAW_CONS *c)
{
  struct KNO_COMPOUND *compound = (struct KNO_COMPOUND *)c;
  lispval typetag = compound->typetag;
  struct KNO_TYPEINFO *typeinfo = kno_use_typeinfo(typetag);
  if ( (typeinfo) && (typeinfo->type_freefn) ) {
    int rv = (typeinfo->type_freefn)((lispval)c,typeinfo);
    if (rv < 0) {
      u8_log(LOGERR,"RecycleCompound",
             "Recycling %q compound: %q",typetag,compound);}}
  int i = 0, n = compound->compound_length;
  lispval *data = &(compound->compound_0);
  while (i<n) {kno_decref(data[i]); i++;}
  kno_decref(compound->typetag);
  if (compound->compound_ismutable) 
    u8_destroy_rwlock(&(compound->compound_rwlock));
  u8_free(c);
}

static void recycle_mystery(struct KNO_RAW_CONS *c)
{
  struct KNO_MYSTERY_DTYPE *myst = (struct KNO_MYSTERY_DTYPE *)c;
  if (myst->myst_dtcode&0x80)
    u8_free(myst->mystery_payload.elts);
  else u8_free(myst->mystery_payload.bytes);
  u8_free(myst);
}

/* The main function */

KNO_EXPORT
/* kno_recycle_cons:
   Arguments: a pointer to an KNO_CONS struct
   Returns: void
   Recycles a cons cell */
void kno_recycle_cons(kno_raw_cons c)
{
  int ctype = KNO_CONS_TYPEOF(c);
  switch (ctype) {
  case kno_string_type: case kno_packet_type: case kno_secret_type:
    recycle_string((struct KNO_STRING *)c);
    return;
  case kno_vector_type:
    recycle_vector((struct KNO_VECTOR *)c);
    return;
  case kno_choice_type:
    recycle_choice((struct KNO_CHOICE *)c);
    return;
  case kno_pair_type:
    recycle_list((struct KNO_PAIR *)c);
    return;
  case kno_compound_type:
    recycle_compound(c);
    return;
  case kno_rational_type: case kno_complex_type:
    recycle_pair((struct KNO_PAIR *)c);
    return;
  case kno_uuid_type:
    recycle_uuid(c);
    return;
  case kno_timestamp_type:
    recycle_timestamp(c);
    return;
  case kno_regex_type:
    recycle_regex(c);
    return;
  case kno_rawptr_type:
    recycle_rawptr(c);
    return;
  case kno_qchoice_type:
    recycle_qchoice((struct KNO_QCHOICE *)c);
    return;
  default: {
    if (kno_recyclers[ctype]) kno_recyclers[ctype](c);}
  }
}

void kno_init_recycle_c()
{
  kno_recyclers[kno_exception_type]=recycle_exception;
  kno_recyclers[kno_mystery_type]=recycle_mystery;
  kno_recyclers[kno_uuid_type]=recycle_uuid;
  kno_recyclers[kno_timestamp_type]=recycle_timestamp;
  kno_recyclers[kno_regex_type]=recycle_regex;

  kno_recyclers[kno_rawptr_type]=recycle_rawptr;
  kno_recyclers[kno_compound_type]=recycle_compound;

  u8_register_source_file(_FILEINFO);
}

