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

static lispval getopt_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval opts = fd_stack_eval(fd_get_arg(expr,1),env,_stack,0);
  if (FD_ABORTED(opts))
    return opts;
  else {
    lispval keys = fd_eval(fd_get_arg(expr,2),env);
    if (FD_ABORTED(keys)) {
      fd_decref(opts); return keys;}
    else {
      lispval results = EMPTY;
      DO_CHOICES(opt,opts) {
        DO_CHOICES(key,keys) {
          lispval v = fd_getopt(opt,key,VOID);
          if (FD_ABORTED(v)) {
            fd_decref(results); results = v;
            FD_STOP_DO_CHOICES;}
          else if (!(VOIDP(v))) {CHOICE_ADD(results,v);}}
        if (FD_ABORTED(results)) {FD_STOP_DO_CHOICES;}}
      fd_decref(keys);
      fd_decref(opts);
      if (FD_ABORTED(results)) {
        return results;}
      else if (EMPTYP(results)) {
        lispval dflt_expr = fd_get_arg(expr,3);
        if (VOIDP(dflt_expr)) return FD_FALSE;
        else return fd_stack_eval(dflt_expr,env,_stack,0);}
      else return simplify_value(results);}}
}
static lispval tryopt_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval opts = fd_stack_eval(fd_get_arg(expr,1),env,_stack,0);
  lispval default_expr = fd_get_arg(expr,3);
  if ( (FD_ABORTED(opts)) || (!(FD_TABLEP(opts))) ) {
    if (FD_ABORTED(opts)) fd_clear_errors(0);
    if (FD_VOIDP(default_expr)) {
      return FD_FALSE;}
    else if (!(FD_EVALP(default_expr)))
      return fd_incref(default_expr);
    else return fd_stack_eval(default_expr,env,_stack,0);}
  else {
    lispval keys = fd_eval(fd_get_arg(expr,2),env);
    if (FD_ABORTED(keys)) {
      fd_decref(opts);
      return keys;}
    else {
      lispval results = EMPTY;
      DO_CHOICES(opt,opts) {
        DO_CHOICES(key,keys) {
          lispval v = fd_getopt(opt,key,VOID);
          if (FD_ABORTED(v)) {
            fd_clear_errors(0);
            fd_decref(results);
            results = FD_EMPTY_CHOICE;
            FD_STOP_DO_CHOICES;
            break;}
          else if (!(VOIDP(v))) {CHOICE_ADD(results,v);}}
        if (FD_ABORTED(results)) {FD_STOP_DO_CHOICES;}}
      fd_decref(keys); fd_decref(opts);
      if (FD_ABORTED(results)) { /* Not sure this ever happens */
        fd_clear_errors(0);
        results=FD_EMPTY_CHOICE;}
      if (EMPTYP(results)) {
        lispval dflt_expr = fd_get_arg(expr,3);
        if (VOIDP(dflt_expr))
          return FD_FALSE;
        else if (!(FD_EVALP(dflt_expr)))
          return fd_incref(dflt_expr);
        else return fd_stack_eval(dflt_expr,env,_stack,0);}
      else return simplify_value(results);}}
}
static lispval getopt_prim(lispval opts,lispval keys,lispval dflt)
{
  lispval results = EMPTY;
  DO_CHOICES(opt,opts) {
    DO_CHOICES(key,keys) {
      lispval v = fd_getopt(opt,key,VOID);
      if (!(VOIDP(v))) {CHOICE_ADD(results,v);}}}
  if (EMPTYP(results)) {
    fd_incref(dflt); return dflt;}
  else return simplify_value(results);
}
static lispval testopt_prim(lispval opts,lispval key,lispval val)
{
  if (fd_testopt(opts,key,val))
    return FD_TRUE;
  else return FD_FALSE;
}

static int optionsp(lispval arg)
{
  if ( (FD_FALSEP(arg)) || (FD_NILP(arg)) || (FD_EMPTYP(arg)) )
    return 1;
  else if (FD_AMBIGP(arg)) {
    FD_DO_CHOICES(elt,arg) {
      if (! (optionsp(elt)) ) {
        FD_STOP_DO_CHOICES;
        return 0;}}
    return 1;}
  else if (FD_PAIRP(arg)) {
    if (optionsp(FD_CAR(arg)))
      return optionsp(FD_CDR(arg));
    else return 0;}
  else if ( (FD_TABLEP(arg)) &&
            (!(FD_POOLP(arg))) &&
            (!(FD_INDEXP(arg))) )
    return 1;
  else return 0;
}
static lispval optionsp_prim(lispval opts)
{
  if (optionsp(opts))
    return FD_TRUE;
  else return FD_FALSE;
}
#define nulloptsp(v) ( (v == FD_FALSE) || (v == FD_DEFAULT) )
static lispval opts_plus_prim(int n,lispval *args)
{
  int i = 0;
  /* *back* is the options list and *front* is where specified values
     should be stored. We walk the arguments, either adding tables to
     the options list, setting options on *front*, or creating new
     slotmaps to get *front*. This was made more complicated when
     schemaps started to be common elements in options lists.
  */
  lispval back = FD_FALSE, front = FD_VOID;
  if (n == 0)
    return fd_make_slotmap(3,0,NULL);
  while (i < n) {
    lispval arg = args[i++];
    if (FD_TABLEP(arg)) {
      fd_incref(arg);
      if (FD_FALSEP(back))
        back = arg;
      else {
        if (!(FD_VOIDP(front))) {
          back = fd_init_pair(NULL,front,back);
          front=FD_VOID;}
        back = fd_init_pair(NULL,arg,back);}}
    else if ( (nulloptsp(arg)) || (FD_EMPTYP(arg)) ) {}
    else {
      if (FD_VOIDP(front)) front = fd_make_slotmap(n,0,NULL);
      if (i < n) {
        lispval optval = args[i++];
        if (FD_QCHOICEP(optval)) {
          struct FD_QCHOICE *qc = (fd_qchoice) optval;
          fd_add(front,arg,qc->qchoiceval);}
        else fd_add(front,arg,optval);}
      else fd_store(front,arg,FD_TRUE);}}
  if (FD_VOIDP(front))
    return back;
  else if (FD_FALSEP(back))
    return front;
  else return fd_init_pair(NULL,front,back);
}


/* Initialization */

FD_EXPORT void fd_init_eval_getopt_c()
{
  u8_register_source_file(_FILEINFO);

  fd_def_evalfn(fd_scheme_module,"GETOPT",
                "`(GETOPT *opts* *name* [*default*=#f])` returns any *name* "
                "option defined in *opts* or *default* otherwise. "
                "If *opts* or *name* are choices, this only returns *default* "
                "if none of the alternatives yield results.",
                 getopt_evalfn);
  fd_def_evalfn(fd_scheme_module,"TRYOPT",
                "`(TRYOPT *opts* *name* [*default*=#f])` returns any *name* "
                "option defined in *opts* or *default* otherwise. Any errors "
                "during option resolution are ignored. "
                "If *opts* or *name* are choices, this only returns *default* "
                "if none of the alternatives yield results. Note that the "
                "*default*, if evaluated, may signal an error.",
                tryopt_evalfn);
  fd_idefn3(fd_scheme_module,"%GETOPT",getopt_prim,FD_NEEDS_2_ARGS|FD_NDCALL,
            "`(%GETOPT *opts* *name* [*default*=#f])` gets any *name* option "
            "from opts, returning *default* if there isn't any. This is a real "
            "procedure (unlike `GETOPT`) so that *default* will be evaluated even "
            "if the option exists and is returned.",
            -1,VOID,fd_symbol_type,VOID,
            -1,FD_FALSE);
  fd_idefn3(fd_scheme_module,"TESTOPT",testopt_prim,2,
            "`(TESTOPT *opts* *name* [*value*])` returns true if "
            "the option *name* is specified in *opts* and it includes "
            "*value* (if provided).",
            -1,VOID,fd_symbol_type,VOID,
            -1,VOID);
  fd_idefn1(fd_scheme_module,"OPTS?",optionsp_prim,FD_NEEDS_1_ARG|FD_NDCALL,
            "`(OPTS? *opts*)` returns true if *opts* is a valid options "
            "object.",
            -1,VOID);
  fd_idefnN(fd_scheme_module,"OPTS+",opts_plus_prim,FD_NDCALL,
            "`(OPTS+ *add* *opts*)` or `(OPTS+ *optname* *value* *opts*) "
            "returns a new options object (a pair).");
  fd_defalias(fd_scheme_module,"OPT+","OPTS+");
}

