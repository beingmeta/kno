/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
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


static fdtype vector_set(fdtype vec,fdtype offset,fdtype value)
{
  int off=fd_getint(offset);
  fdtype current=FD_VECTOR_REF(vec,off);
  FD_VECTOR_SET(vec,off,value);
  fd_incref(value); fd_decref(current);
  return FD_VOID;
}

static fdtype set_car(fdtype pair,fdtype value)
{
  struct FD_PAIR *p=(fd_pair)pair;
  fdtype current=p->fd_car;
  p->fd_car=fd_incref(value);
  fd_decref(current);
  return FD_VOID;
}

static fdtype set_cdr(fdtype pair,fdtype value)
{
  struct FD_PAIR *p=(fd_pair)pair;
  fdtype current=p->fd_cdr;
  p->fd_cdr=fd_incref(value);
  fd_decref(current);
  return FD_VOID;
}

FD_EXPORT void fd_init_side_effects_c()
{
  fdtype module=fd_new_module("SIDE-EFFECTS",FD_MODULE_SAFE);
  fd_idefn(module,fd_make_cprim3x("VECTOR-SET!",vector_set,3,
                                  fd_vector_type,FD_VOID,
                                  fd_fixnum_type,FD_VOID,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("SET-CAR!",set_car,2,
                                  fd_pair_type,FD_VOID,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim2x("SET-CDR!",set_cdr,2,
                                  fd_pair_type,FD_VOID,
                                  -1,FD_VOID));

  u8_register_source_file(_FILEINFO);

  fd_finish_module(module);
  fd_persist_module(module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
