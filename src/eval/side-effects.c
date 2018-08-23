/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"

/* This file implements structural side effects, which are kept in a different module
   to discourage casual usage.  Note that they are not neccessarily threadsafe. */


static lispval vector_set(lispval vec,lispval offset,lispval value)
{
  int off = fd_getint(offset);
  lispval current = VEC_REF(vec,off);
  FD_VECTOR_SET(vec,off,value);
  fd_incref(value); fd_decref(current);
  return VOID;
}

static lispval set_car(lispval pair,lispval value)
{
  struct FD_PAIR *p = (fd_pair)pair;
  lispval current = p->car;
  p->car = fd_incref(value);
  fd_decref(current);
  return VOID;
}

static lispval set_cdr(lispval pair,lispval value)
{
  struct FD_PAIR *p = (fd_pair)pair;
  lispval current = p->cdr;
  p->cdr = fd_incref(value);
  fd_decref(current);
  return VOID;
}

FD_EXPORT void fd_init_side_effects_c()
{
  lispval module = fd_new_cmodule("SIDE-EFFECTS",FD_MODULE_SAFE,
                                  fd_init_side_effects_c);
  fd_idefn(module,fd_make_cprim3x("VECTOR-SET!",vector_set,3,
                                  fd_vector_type,VOID,
                                  fd_fixnum_type,VOID,
                                  -1,VOID));
  fd_idefn(module,fd_make_cprim2x("SET-CAR!",set_car,2,
                                  fd_pair_type,VOID,
                                  -1,VOID));
  fd_idefn(module,fd_make_cprim2x("SET-CDR!",set_cdr,2,
                                  fd_pair_type,VOID,
                                  -1,VOID));

  u8_register_source_file(_FILEINFO);

  fd_finish_module(module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
