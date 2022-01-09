/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/support.h"
#include "kno/cprims.h"
#include "kno/history.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

static lispval repl_module=VOID;

/* History functions */

static lispval history_symbol, histref_typetag;

DEFC_EVALFN("with-history",with_history_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(with-history *history* *body...*)` evaluates *body* "
	    "with the default history bound to the history object "
	    "*history*.")
KNO_EXPORT lispval with_history_evalfn
(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval history_arg = kno_eval(kno_get_arg(expr,1),env,stack);
  if (KNO_VOIDP(history_arg)) {
    if (u8_current_exception)
      return KNO_ERROR;
    else return kno_err("BadHistoryArg","with_history_evalfn",NULL,expr);}
  lispval outer_history = kno_thread_get(history_symbol);
  kno_thread_set(history_symbol,VOID);
  if (KNO_FIXNUMP(history_arg))
    kno_hist_init(KNO_FIX2INT(history_arg));
  else kno_hist_init(200);
  lispval body = kno_get_body(expr,2);
  lispval value = kno_eval_body(body,env,stack,"WITH-HISTORY",NULL,0);
  kno_thread_set(history_symbol,outer_history);
  kno_decref(outer_history);
  kno_decref(history_arg);
  return value;
}

DEFC_PRIM("histref",history_ref_prim,MIN_ARGS(2)|MAX_ARGS(2),
	  "returns the history item for "
	  "*ref*, using the current historical context if *history* is "
	  "not provided.",
	  {"ref",kno_any_type,KNO_VOID},
	  {"history",kno_any_type,KNO_VOID});
static lispval history_ref_prim(lispval ref,lispval history)
{
  if ( (VOIDP(history)) || (FALSEP(history)) || (DEFAULTP(history)) )
    history = kno_thread_get(history_symbol);
  else kno_incref(history);
  if (KNO_COMPOUND_TYPEP(history,history_symbol)) {
    lispval result = kno_history_ref(history,ref);
    kno_decref(history);
    return result;}
  else return KNO_EMPTY;
}

DEFC_PRIM("history/find",history_find_prim,MIN_ARGS(2)|MAX_ARGS(2),
	  "returns the history reference for "
	  "*value*, using the current historical context if *history* is "
	  "not provided.",
	  {"ref",kno_any_type,KNO_VOID},
	  {"history",kno_any_type,KNO_VOID});
static lispval history_find_prim(lispval ref,lispval history)
{
  if ( (VOIDP(history)) || (FALSEP(history)) || (DEFAULTP(history)) )
    history = kno_thread_get(history_symbol);
  else kno_incref(history);
  if (KNO_COMPOUND_TYPEP(history,history_symbol)) {
    lispval result = kno_history_find(history,ref);
    kno_decref(history);
    return result;}
  else return KNO_EMPTY;
}

DEFC_PRIM("history/add!",history_add_prim,MIN_ARGS(1)|MAX_ARGS(3),
	  "associates *val* with "
	  "the history reference *ref*, creating a new reference if *ref* "
	  "is not provided. This uses the current historical context if "
	  "*history* is not provided.",
	  {"val",kno_any_type,KNO_VOID},
	  {"history",kno_any_type,KNO_VOID},
	  {"ref",kno_any_type,KNO_VOID});
static lispval history_add_prim(lispval val,lispval history,lispval ref)
{
  if ( (VOIDP(history)) || (FALSEP(history)) || (DEFAULTP(history)) )
    history = kno_thread_get(history_symbol);
  else kno_incref(history);
  if (KNO_COMPOUND_TYPEP(history,history_symbol)) {
    lispval result = kno_history_add(history,val,ref);
    kno_decref(history);
    return result;}
  else return KNO_EMPTY;
}

DEFC_PRIM("history/subst",history_subst_prim,MIN_ARGS(1)|MAX_ARGS(3),
	  "replaces history references in *expr* from *history*, which "
	  "defualts to the thread binding for `history`.",
	  {"expr",kno_any_type,KNO_VOID},
	  {"history",kno_any_type,KNO_VOID});
static lispval history_subst_prim(lispval expr,lispval history)
{
  if ( (VOIDP(history)) || (FALSEP(history)) || (DEFAULTP(history)) )
    history = kno_thread_get(history_symbol);
  else kno_incref(history);
  lispval result = (KNO_COMPOUND_TYPEP(history,history_symbol)) ?
    (kno_eval_histrefs(expr,history)) :
    (kno_incref(expr));
  kno_decref(history);
  return result;
}

static int repl_initialized = 0;

KNO_EXPORT void kno_init_replc_c()
{
  if (repl_initialized) return;
  else repl_initialized = 1;

  repl_module = kno_new_cmodule("kno/repl",0,NULL);

  u8_register_source_file(_FILEINFO);

  KNO_LINK_EVALFN(repl_module,with_history_evalfn);

  link_local_cprims();

  history_symbol = kno_intern("%history");
  histref_typetag = kno_intern("%histref");

  kno_finish_cmodule(repl_module);

}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("history/ref",history_ref_prim,2,repl_module);
  KNO_LINK_CPRIM("history/find",history_find_prim,2,repl_module);
  KNO_LINK_CPRIM("history/add!",history_add_prim,3,repl_module);
  KNO_LINK_CPRIM("history/subst",history_subst_prim,2,repl_module);

  KNO_LINK_CPRIM("histref",history_ref_prim,2,repl_module);
  KNO_LINK_CPRIM("histfind",history_find_prim,2,repl_module);
  KNO_LINK_CPRIM("histadd!",history_add_prim,3,repl_module);
}
