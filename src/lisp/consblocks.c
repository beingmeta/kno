/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"

#include "libu8/u8printf.h"

static lispval coderef_symbol;

struct BLOCKOUT {
  struct FD_CONS *conses;
  size_t conses_len, conses_next;};

#define CONS_SIZE (sizeof(struct FD_CONS))
#define CONSBLOCK_SIZE(typespec) \
  ( ( (sizeof(typespec)) / (CONS_SIZE) ) +                              \
    ( ( (sizeof(typespec)) % (CONS_SIZE) ) ? (1) : (0) ) )

static lispval block_copier(lispval v,struct BLOCKOUT *block)
{
  if (FD_CONSP(v)) {
    if (FD_PAIRP(v)) {
      size_t need_size = block->conses_next + CONSBLOCK_SIZE(struct FD_PAIR);
      if (need_size >= block->conses_len) {
        size_t new_size = block->conses_len*2;
        struct FD_CONS *newblock = u8_realloc(block->conses,new_size*CONS_SIZE);
        if (newblock == NULL) {
          u8_graberrno("block_copier",u8_mkstring("%lld",new_size));
          return FD_ERROR_VALUE;}
        block->conses = newblock;
        block->conses_len = new_size;}
      size_t off = block->conses_next;
      block->conses_next += CONSBLOCK_SIZE(struct FD_PAIR);
      lispval car = FD_CAR(v);
      if (FD_CONSP(car)) car = block_copier(car,block);
      if (FD_TROUBLEP(car)) return car;
      lispval cdr = FD_CDR(v);
      if (FD_CONSP(cdr)) cdr = block_copier(cdr,block);
      if (FD_TROUBLEP(cdr)) {
        if (FD_COMPOUND_TYPEP(car,coderef_symbol)) {fd_decref(car);}
        return cdr;}
      struct FD_PAIR *pair = (fd_pair) (block->conses+off);
      FD_INIT_STATIC_CONS(pair,fd_pair_type);
      pair->car = car;
      pair->cdr = cdr;
      return fd_init_compound(NULL,coderef_symbol,0,1,FD_INT2FIX(off));}
    else return v;}
  else return v;
}

static void convert_refs(lispval v,struct FD_CONS *conses)
{
  if (FD_CONSP(v)) {
    if (FD_PAIRP(v)) {
      struct FD_PAIR *p = (fd_pair) v;
      lispval car = p->car;
      lispval cdr = p->cdr;
      if (FD_COMPOUND_TYPEP(car,coderef_symbol)) {
        lispval offset = FD_COMPOUND_REF(car,0);
        assert(FD_FIXNUMP(offset));
        long long off = FD_FIX2INT(offset);
        lispval ptr = (lispval) (conses+off);
        fd_decref(car);
        p->car = car = ptr;}
      if (FD_COMPOUND_TYPEP(cdr,coderef_symbol)) {
        lispval offset = FD_COMPOUND_REF(cdr,0);
        assert(FD_FIXNUMP(offset));
        long long off = FD_FIX2INT(offset);
        lispval ptr = (lispval) (conses+off);
        fd_decref(cdr);
        p->cdr = cdr = ptr;}
      convert_refs(car,conses);
      convert_refs(cdr,conses);}
    else {}}
}

FD_EXPORT lispval fd_make_consblock(lispval obj)
{
  if (!(FD_CONSP(obj))) return obj;
  size_t init_size = 4096;
  struct BLOCKOUT out = {0};
  out.conses = u8_malloc(init_size*CONS_SIZE);
  out.conses_len = init_size;
  out.conses_next = 0;
  lispval v = block_copier(obj,&out);
  if (FD_ABORTED(v)) {
    u8_free(out.conses);
    return v;}
  else if (FD_COMPOUND_TYPEP(v,coderef_symbol)) {
    fd_decref(v);}
  else return v;
  if (out.conses_next == 0) {
    u8_free(out.conses);
    return fd_incref(obj);}
  struct FD_CONSBLOCK *consblock = u8_alloc(struct FD_CONSBLOCK);
  FD_INIT_CONS(consblock,fd_consblock_type);
  struct FD_CONS *conses = u8_malloc(out.conses_next*CONS_SIZE);
  memcpy(conses,out.conses,out.conses_next*CONS_SIZE);
  convert_refs((lispval)(conses),conses);
  consblock->consblock_original = fd_incref(obj);
  consblock->consblock_head = (lispval) conses;
  consblock->consblock_conses = conses;
  consblock->consblock_len = out.conses_next;
  u8_free(out.conses);
  return (lispval) consblock;
}

static void recycle_consblock(struct FD_RAW_CONS *c)
{
  struct FD_CONSBLOCK *cb = (struct FD_CONSBLOCK *)c;
  u8_free(cb->consblock_conses);
  fd_decref(cb->consblock_original);
  u8_free(cb);
}

static int unparse_consblock(struct U8_OUTPUT *out,lispval x)
{
  struct FD_CONSBLOCK *cb = fd_consptr(struct FD_CONSBLOCK *,x,fd_consblock_type);
  u8_printf(out,"#<CONSBLOCK 0x%llx+%lld>",
            (unsigned long long)(cb->consblock_conses),
            (unsigned long long)(cb->consblock_len));
  return 1;
}

void fd_init_consblocks_c()
{
  coderef_symbol = fd_intern("%CODEREF");

  fd_recyclers[fd_consblock_type] = recycle_consblock;
  fd_unparsers[fd_consblock_type] = unparse_consblock;

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
