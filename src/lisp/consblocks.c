/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"

#include "libu8/u8printf.h"

static lispval coderef_symbol;

struct BLOCKOUT {
  struct KNO_CONS *conses;
  size_t conses_len, conses_next;};

#define CONS_SIZE (sizeof(struct KNO_CONS))
#define CONSBLOCK_SIZE(typespec) \
  ( ( (sizeof(typespec)) / (CONS_SIZE) ) +                              \
    ( ( (sizeof(typespec)) % (CONS_SIZE) ) ? (1) : (0) ) )

static lispval block_copier(lispval v,struct BLOCKOUT *block)
{
  if (KNO_CONSP(v)) {
    if (KNO_PAIRP(v)) {
      size_t need_size = block->conses_next + CONSBLOCK_SIZE(struct KNO_PAIR);
      if (need_size >= block->conses_len) {
        size_t new_size = block->conses_len*2;
        struct KNO_CONS *newblock = u8_realloc(block->conses,new_size*CONS_SIZE);
        if (newblock == NULL) {
          u8_graberrno("block_copier",u8_mkstring("%lld",new_size));
          return KNO_ERROR_VALUE;}
        block->conses = newblock;
        block->conses_len = new_size;}
      size_t off = block->conses_next;
      block->conses_next += CONSBLOCK_SIZE(struct KNO_PAIR);
      lispval car = KNO_CAR(v);
      if (KNO_CONSP(car)) car = block_copier(car,block);
      if (KNO_TROUBLEP(car)) return car;
      lispval cdr = KNO_CDR(v);
      if (KNO_CONSP(cdr)) cdr = block_copier(cdr,block);
      if (KNO_TROUBLEP(cdr)) {
        if (KNO_COMPOUND_TYPEP(car,coderef_symbol)) {kno_decref(car);}
        return cdr;}
      struct KNO_PAIR *pair = (kno_pair) (block->conses+off);
      KNO_INIT_STATIC_CONS(pair,kno_pair_type);
      pair->car = car;
      pair->cdr = cdr;
      return kno_init_compound(NULL,coderef_symbol,0,1,KNO_INT2FIX(off));}
    else return v;}
  else return v;
}

static void convert_refs(lispval v,struct KNO_CONS *conses)
{
  if (KNO_CONSP(v)) {
    if (KNO_PAIRP(v)) {
      struct KNO_PAIR *p = (kno_pair) v;
      lispval car = p->car;
      lispval cdr = p->cdr;
      if (KNO_COMPOUND_TYPEP(car,coderef_symbol)) {
        lispval offset = KNO_COMPOUND_REF(car,0);
        assert(KNO_FIXNUMP(offset));
        long long off = KNO_FIX2INT(offset);
        lispval ptr = (lispval) (conses+off);
        kno_decref(car);
        p->car = car = ptr;}
      if (KNO_COMPOUND_TYPEP(cdr,coderef_symbol)) {
        lispval offset = KNO_COMPOUND_REF(cdr,0);
        assert(KNO_FIXNUMP(offset));
        long long off = KNO_FIX2INT(offset);
        lispval ptr = (lispval) (conses+off);
        kno_decref(cdr);
        p->cdr = cdr = ptr;}
      convert_refs(car,conses);
      convert_refs(cdr,conses);}
    else {}}
}

KNO_EXPORT lispval kno_make_consblock(lispval obj)
{
  if (!(KNO_CONSP(obj))) return obj;
  size_t init_size = 4096;
  struct BLOCKOUT out = {0};
  out.conses = u8_malloc(init_size*CONS_SIZE);
  out.conses_len = init_size;
  out.conses_next = 0;
  lispval v = block_copier(obj,&out);
  if (KNO_ABORTED(v)) {
    u8_free(out.conses);
    return v;}
  else if (KNO_COMPOUND_TYPEP(v,coderef_symbol)) {
    kno_decref(v);}
  else return v;
  if (out.conses_next == 0) {
    u8_free(out.conses);
    return kno_incref(obj);}
  struct KNO_CONSBLOCK *consblock = u8_alloc(struct KNO_CONSBLOCK);
  KNO_INIT_CONS(consblock,kno_consblock_type);
  struct KNO_CONS *conses = u8_malloc(out.conses_next*CONS_SIZE);
  memcpy(conses,out.conses,out.conses_next*CONS_SIZE);
  convert_refs((lispval)(conses),conses);
  consblock->consblock_original = kno_incref(obj);
  consblock->consblock_head = (lispval) conses;
  consblock->consblock_conses = conses;
  consblock->consblock_len = out.conses_next;
  u8_free(out.conses);
  return (lispval) consblock;
}

static void recycle_consblock(struct KNO_RAW_CONS *c)
{
  struct KNO_CONSBLOCK *cb = (struct KNO_CONSBLOCK *)c;
  u8_free(cb->consblock_conses);
  kno_decref(cb->consblock_original);
  u8_free(cb);
}

static int unparse_consblock(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_CONSBLOCK *cb = kno_consptr(struct KNO_CONSBLOCK *,x,kno_consblock_type);
  u8_printf(out,"#<CONSBLOCK 0x%llx+%lld>",
            KNO_LONGVAL((cb->consblock_conses)),
            KNO_LONGVAL((cb->consblock_len)));
  return 1;
}

void kno_init_consblocks_c()
{
  coderef_symbol = kno_intern("%coderef");

  kno_recyclers[kno_consblock_type] = recycle_consblock;
  kno_unparsers[kno_consblock_type] = unparse_consblock;

  u8_register_source_file(_FILEINFO);
}

