/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_EVAL_INTERNALS      1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/dtcall.h"
#include "kno/ffi.h"

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

static lispval vector_evalfn(lispval vec,kno_lexenv env,kno_stack stackptr)
{
  lispval *eval_elts = KNO_VECTOR_DATA(vec);
  int i = 0, len = KNO_VECTOR_LENGTH(vec);
  lispval result = kno_init_vector(NULL,len,NULL);
  lispval *result_elts = KNO_VECTOR_DATA(result);
  while (i < len) {
    lispval expr = eval_elts[i];
    lispval val = kno_eval(expr,env,stackptr);
    if (KNO_ABORTP(val)) {
      kno_decref(result);
      return val;}
    result_elts[i++]=val;}
  return result;
}

static lispval slotmap_evalfn(lispval sm,kno_lexenv env,kno_stack stackptr)
{
  int unlock = 0;
  struct KNO_SLOTMAP *smap = (kno_slotmap) sm;
  struct KNO_KEYVAL *old_kv = smap->sm_keyvals;
  if (KNO_XTABLE_USELOCKP(smap)) {
    u8_read_lock(&(smap->table_rwlock));
    unlock = 1;}
  int read_slot = 0, write_slot = 0, n_slots = smap->n_slots;
  lispval result = kno_make_slotmap(n_slots,0,NULL);
  struct KNO_SLOTMAP *new_slotmap = (kno_slotmap) result;
  struct KNO_KEYVAL *new_kv = new_slotmap->sm_keyvals;
  while (read_slot < n_slots) {
    lispval slotid = old_kv[read_slot].kv_key;
    lispval eval_expr = old_kv[read_slot].kv_val;
    lispval val = kno_eval(eval_expr,env,stackptr);
    if (KNO_ABORTP(val)) {
      if (unlock) u8_rw_unlock(&(smap->table_rwlock));
      kno_decref(result);
      return val;}
    else if (KNO_EMPTYP(val)) {
      read_slot++;
      continue;}
    else if (KNO_QCHOICEP(val)) {
      lispval qc = val;
      struct KNO_QCHOICE *qch = (kno_qchoice) qc;
      val = qch->qchoiceval;
      kno_decref(qc);}
    else {}
    new_kv[write_slot].kv_val=val;
    new_kv[write_slot].kv_key=slotid;
    write_slot++;
    read_slot++;}
  if (unlock) u8_rw_unlock(&(smap->table_rwlock));
  new_slotmap->n_slots = write_slot;
  new_slotmap->table_bits = 0;
  KNO_XTABLE_SET_BIT(new_slotmap,KNO_SLOTMAP_SORT_KEYVALS,
		     (KNO_XTABLE_BITP(smap,KNO_SLOTMAP_SORT_KEYVALS)));
  return result;
}

static lispval struct_evalfn(lispval expr,kno_lexenv env,kno_stack stackptr)
{
  if ( ( !(KNO_PAIRP(expr)) ) || (!(KNO_PAIRP(KNO_CDR(expr)))) )
    return kno_err(kno_SyntaxError,"struct_evalfn",NULL,expr);
  lispval x = KNO_CADR(expr);
  if (KNO_VECTORP(x))
    return vector_evalfn(x,env,stackptr);
  else if (KNO_SLOTMAPP(x))
    return slotmap_evalfn(x,env,stackptr);
  else if (KNO_PAIRP(x))
    return kno_eval(x,env,stackptr);
  else return x;
}

/* Initialization */

static time_t struct_eval_init = 0;

KNO_EXPORT int kno_init_struct_eval_c()
{
  if (struct_eval_init)
    return 0;
  else struct_eval_init = time(NULL);

  struct_eval_symbol = kno_intern("#.");

  kno_def_evalfn(kno_scheme_module,"#.",struct_evalfn,
		"Evaluate the value components of a structured object "
		"(vector, slotmap, etc) and return a copy with those "
		"values.");
  kno_defalias(kno_scheme_module,"STRUCT-EVAL","#.");
  link_local_cprims();

  u8_register_source_file(_FILEINFO);

  return 1;
}

static void link_local_cprims()
{
}
