/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/eval.h"
#include "kno/sequences.h"

/* This file implements structural side effects, which are kept in a different module
   to discourage casual usage.  Note that they are not neccessarily threadsafe. */


static lispval vector_set(lispval vec,lispval offset,lispval value)
{
  int off = kno_getint(offset);
  lispval current = VEC_REF(vec,off);
  KNO_VECTOR_SET(vec,off,value);
  kno_incref(value); kno_decref(current);
  return VOID;
}

static lispval set_car(lispval pair,lispval value)
{
  struct KNO_PAIR *p = (kno_pair)pair;
  lispval current = p->car;
  p->car = kno_incref(value);
  kno_decref(current);
  return VOID;
}

static lispval set_cdr(lispval pair,lispval value)
{
  struct KNO_PAIR *p = (kno_pair)pair;
  lispval current = p->cdr;
  p->cdr = kno_incref(value);
  kno_decref(current);
  return VOID;
}

KNO_EXPORT void kno_init_side_effects_c()
{
  lispval module = kno_new_cmodule("SIDE-EFFECTS",KNO_MODULE_SAFE,
                                  kno_init_side_effects_c);
  kno_idefn(module,kno_make_cprim3x("VECTOR-SET!",vector_set,3,
                                  kno_vector_type,VOID,
                                  kno_fixnum_type,VOID,
                                  -1,VOID));
  kno_idefn(module,kno_make_cprim2x("SET-CAR!",set_car,2,
                                  kno_pair_type,VOID,
                                  -1,VOID));
  kno_idefn(module,kno_make_cprim2x("SET-CDR!",set_cdr,2,
                                  kno_pair_type,VOID,
                                  -1,VOID));

  u8_register_source_file(_FILEINFO);

  kno_finish_module(module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
