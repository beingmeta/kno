/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"
#include "framerd/ffi.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

#if HAVE_MTRACE && HAVE_MCHECK_H
#include <mcheck.h>
#endif

static lispval struct_eval_symbol;

static lispval vector_evalfn(lispval vec,fd_lexenv env,struct FD_STACK *stackptr)
{
  lispval *eval_elts = FD_VECTOR_DATA(vec);
  int i = 0, len = FD_VECTOR_LENGTH(vec);
  lispval result = fd_init_vector(NULL,len,NULL);
  lispval *result_elts = FD_VECTOR_DATA(result);
  while (i < len) {
    lispval expr = eval_elts[i];
    lispval val = _fd_fast_eval(expr,env,stackptr,0);
    if (FD_ABORTP(val)) {
      fd_decref(result);
      return val;}
    result_elts[i++]=val;}
  return result;
}

static lispval slotmap_evalfn(lispval sm,fd_lexenv env,struct FD_STACK *stackptr)
{
  int unlock = 0;
  struct FD_SLOTMAP *smap = (fd_slotmap) sm;
  struct FD_KEYVAL *old_kv = smap->sm_keyvals;
  if (smap->table_uselock) {
    u8_read_lock(&(smap->table_rwlock));
    unlock = 1;}
  int read_slot = 0, write_slot = 0, n_slots = smap->n_slots;
  lispval result = fd_make_slotmap(n_slots,0,NULL);
  struct FD_SLOTMAP *new_slotmap = (fd_slotmap) result;
  struct FD_KEYVAL *new_kv = new_slotmap->sm_keyvals;
  while (read_slot < n_slots) {
    lispval slotid = old_kv[read_slot].kv_key;
    lispval eval_expr = old_kv[read_slot].kv_val;
    lispval val = _fd_fast_eval(eval_expr,env,stackptr,0);
    if (FD_ABORTP(val)) {
      fd_decref(result);
      return val;}
    else if (FD_EMPTYP(val)) {
      read_slot++;
      continue;}
    else if (FD_QCHOICEP(val)) {
      lispval qc = val;
      struct FD_QCHOICE *qch = (fd_qchoice) qc;
      val = qch->qchoiceval;
      fd_decref(qc);}
    else {}
    new_kv[write_slot].kv_val=val;
    new_kv[write_slot].kv_key=slotid;
    write_slot++;
    read_slot++;}
  if (unlock) u8_rw_unlock(&(smap->table_rwlock));
  new_slotmap->n_slots = write_slot;
  new_slotmap->table_modified = 0;
  new_slotmap->table_readonly = 0;
  new_slotmap->sm_sort_keyvals = smap->sm_sort_keyvals;
  return result;
}

static lispval struct_evalfn(lispval expr,fd_lexenv env,struct FD_STACK *stackptr)
{
  if ( ( !(FD_PAIRP(expr)) ) || (!(FD_PAIRP(FD_CDR(expr)))) )
    return fd_err(fd_SyntaxError,"struct_evalfn",NULL,expr);
  lispval x = FD_CADR(expr);
  if (FD_VECTORP(x))
    return vector_evalfn(x,env,stackptr);
  else if (FD_SLOTMAPP(x))
    return slotmap_evalfn(x,env,stackptr);
  else if ( (FD_PAIRP(x)) || (FD_CODEP(x)) )
    return _fd_fast_eval(x,env,stackptr,0);
  else return x;
}

/* Initialization */

static time_t struct_eval_init = 0;

FD_EXPORT int fd_init_struct_eval_c()
{
  if (struct_eval_init)
    return 0;
  else struct_eval_init = time(NULL);

  struct_eval_symbol = fd_intern("#.");

  fd_def_evalfn(fd_scheme_module,"#.",
		"Evaluate the value components of a structured object "
		"(vector, slotmap, etc) and return a copy with those "
		"values.",
		struct_evalfn);
  fd_defalias(fd_scheme_module,"STRUCT-EVAL","#.");

  u8_register_source_file(_FILEINFO);

  return 1;
}

