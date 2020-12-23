/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
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

KNO_EXPORT lispval with_history_evalfn
(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval history_arg = kno_eval(kno_get_arg(expr,1),env,stack,0);
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

DEFCPRIM("histref",histref_prim,MIN_ARGS(2)|MAX_ARGS(2),
	 "returns the history item for "
	 "*ref*, using the current historical context if *history* is "
	 "not provided.",
	 {"ref",kno_any_type,KNO_VOID},
	 {"history",kno_any_type,KNO_VOID});
static lispval histref_prim(lispval ref,lispval history)
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

DEFCPRIM("histfind",histfind_prim,MIN_ARGS(2)|MAX_ARGS(2),
	 "returns the history reference for "
	 "*value*, using the current historical context if *history* is "
	 "not provided.",
	 {"ref",kno_any_type,KNO_VOID},
	 {"history",kno_any_type,KNO_VOID});
static lispval histfind_prim(lispval ref,lispval history)
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

DEFCPRIM("histadd!",histadd_prim,MIN_ARGS(1)|MAX_ARGS(3),
	 "associates *val* with "
	 "the history reference *ref*, creating a new reference if *ref* "
	 "is not provided. This uses the current historical context if "
	 "*history* is not provided.",
	 {"val",kno_any_type,KNO_VOID},
	 {"history",kno_any_type,KNO_VOID},
	 {"ref",kno_any_type,KNO_VOID});
static lispval histadd_prim(lispval val,lispval history,lispval ref)
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

static int repl_initialized = 0;

KNO_EXPORT void kno_init_replc_c()
{
  if (repl_initialized) return;
  else repl_initialized = 1;

  repl_module = kno_new_cmodule("kno/repl",0,NULL);

  u8_register_source_file(_FILEINFO);

  kno_def_evalfn(repl_module,"with-history",with_history_evalfn,
		 "creates a new history "
		 "context and evaluates *body* within that context. "
		 "This saves any current history context.");

  link_local_cprims();

  history_symbol = kno_intern("%history");
  histref_typetag = kno_intern("%histref");

  kno_finish_cmodule(repl_module);

}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("histref",histref_prim,2,repl_module);
  KNO_LINK_CPRIM("histfind",histfind_prim,2,repl_module);
  KNO_LINK_CPRIM("histadd!",histadd_prim,3,repl_module);
}
