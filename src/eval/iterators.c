/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/storage.h"
#include "framerd/numbers.h"
#include "eval_internals.h"

/* Helper functions */

static fdtype iter_var;

/* Simple iterations */

static fdtype while_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1);
  fdtype body = fd_get_body(expr,1);
  fdtype result = FD_VOID;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"WHILE",NULL,expr);
  else {
    while ((FD_VOIDP(result)) &&
           (testeval(test_expr,env,&result,_stack))) {
      FD_DOLIST(iter_expr,body) {
        fdtype val = fast_eval(iter_expr,env);
        if (FD_ABORTED(val))
          return val;
        fd_decref(val);}}}
  if (FD_ABORTED(result))
    return result;
  else return FD_VOID;
}

static fdtype until_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1);
  fdtype body = fd_get_body(expr,1);
  fdtype result = FD_VOID;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"UNTIL",NULL,expr);
  else {
    while ((FD_VOIDP(result)) &&
           (!(testeval(test_expr,env,&result,_stack)))) {
      FD_DOLIST(iter_expr,body) {
        fdtype val = fast_eval(iter_expr,env);
        if (FD_ABORTED(val)) return val;
        else fd_decref(val);}}}
  if (FD_ABORTED(result))
    return result;
  else return FD_VOID;
}

/* Parsing for more complex iterations. */

static fdtype parse_control_spec
  (fdtype expr,fdtype *value,fdtype *count_var,
   fd_lispenv env,fd_stack _stack)
{
  fdtype control_expr = fd_get_arg(expr,1);
  if (FD_VOIDP(control_expr))
    return fd_err(fd_TooFewExpressions,"DO...",NULL,expr);
  else {
    fdtype var = fd_get_arg(control_expr,0), ivar = fd_get_arg(control_expr,2);
    fdtype val_expr = fd_get_arg(control_expr,1), val;
    if (FD_VOIDP(control_expr))
      return fd_err(fd_TooFewExpressions,"DO...",NULL,expr);
    else if (FD_VOIDP(val_expr))
      return fd_err(fd_TooFewExpressions,"DO...",NULL,control_expr);
    else if (!(FD_SYMBOLP(var)))
      return fd_err(fd_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    else if (!((FD_VOIDP(ivar)) || (FD_SYMBOLP(ivar))))
      return fd_err(fd_SyntaxError,
                    _("identifier is not a symbol"),NULL,control_expr);
    val = fast_eval(val_expr,env);
    if (FD_ABORTED(val)) return val;
    *value = val; if (count_var) *count_var = ivar;
    return var;}
}

/* DOTIMES */

static fdtype dotimes_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  int i = 0, limit;
  fdtype limit_val, var =
    parse_control_spec(expr,&limit_val,NULL,env,_stack);
  fdtype vars[2], vals[2];
  fdtype body = fd_get_body(expr,2);
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTED(var)) return var;
  else if (!(FD_UINTP(limit_val)))
    return fd_type_error("fixnum","dotimes_evalfn",limit_val);
  else limit = FD_FIX2INT(limit_val);
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.table_schema = vars; bindings.schema_values = vals; bindings.schema_length = 1;
  bindings.schemap_onstack = 1; bindings.schemap_sorted = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (fdtype)(&bindings); envstruct.env_exports = FD_VOID;
  envstruct.env_copy = NULL;
  vars[0]=var; vals[0]=FD_INT(0);
  while (i < limit) {
    vals[0]=FD_INT(i);
    {FD_DOLIST(subexpr,body) {
        fdtype val = fast_eval(subexpr,&envstruct);
        if (FD_THROWP(val)) {
          if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
          fd_decref(vals[0]);
          u8_destroy_rwlock(&(bindings.table_rwlock));
          return val;}
        else if (FD_ABORTED(val)) {
          if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
          fd_decref(vals[0]);
          u8_destroy_rwlock(&(bindings.table_rwlock));
          return val;}
        fd_decref(val);}}
    if (envstruct.env_copy) {
      fd_recycle_environment(envstruct.env_copy);
      envstruct.env_copy = NULL;}
    /* This (setting the variable bound by dotimes) is bad form, but it might happen. */
    fd_decref(vals[0]);
    i++;}
  u8_destroy_rwlock(&(bindings.table_rwlock));
  if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
  return FD_VOID;
}

/* DOSEQ */

static fdtype doseq_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  int i = 0, lim, islist = 0;
  fdtype seq, count_var = FD_VOID, *iterval = NULL;
  fdtype var = parse_control_spec(expr,&seq,&count_var,env,_stack);
  fdtype body = fd_get_body(expr,2);
  fdtype vars[2], vals[2], pairscan = FD_VOID;
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTED(var)) return var;
  else if (FD_EMPTY_CHOICEP(seq)) return FD_VOID;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","doseq_evalfn",seq);
  else lim = fd_seq_length(seq);
  if (lim==0) {
    fd_decref(seq);
    return FD_VOID;}
  if (FD_PAIRP(seq)) {islist = 1; pairscan = seq;}
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.table_schema = vars; bindings.schema_values = vals; bindings.schema_length = 1;
  bindings.schemap_onstack = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (fdtype)(&bindings); envstruct.env_exports = FD_VOID;
  envstruct.env_copy = NULL;
  vars[0]=var; vals[0]=FD_VOID;
  if (!(FD_VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT(0);
    bindings.schema_length = 2; iterval = &(vals[1]);}
  while (i<lim) {
    fdtype elt = (islist)?(fd_refcar(pairscan)):(fd_seq_elt(seq,i));
    vals[0]=elt;
    if (iterval) *iterval = FD_INT(i);
    {FD_DOLIST(subexpr,body) {
        fdtype val = fast_eval(subexpr,&envstruct);
        if (FD_THROWP(val)) {
          if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
          fd_decref(vals[0]); fd_decref(seq);
          u8_destroy_rwlock(&(bindings.table_rwlock));
          return val;}
        else if (FD_ABORTED(val)) {
          fdtype errbind;
          if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
          fd_decref(vals[0]); fd_decref(seq);
          u8_destroy_rwlock(&(bindings.table_rwlock));
          return val;}
        fd_decref(val);}}
    /* Every iteration is a new environment */
    if (envstruct.env_copy) {
      fd_recycle_environment(envstruct.env_copy);
      envstruct.env_copy = NULL;}
    fd_decref(vals[0]);
    if (islist) pairscan = FD_CDR(pairscan);
    i++;}
  fd_decref(seq);
  u8_destroy_rwlock(&(bindings.table_rwlock));
  return FD_VOID;
}

/* FORSEQ */

static fdtype forseq_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  int i = 0, lim, islist = 0;
  fdtype seq, count_var = FD_VOID, *iterval = NULL, *results, result;
  fdtype var = parse_control_spec(expr,&seq,&count_var,env,_stack);
  fdtype body = fd_get_body(expr,2);
  fdtype vars[2], vals[2], pairscan = FD_VOID;
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTED(var)) return var;
  else if (FD_EMPTY_CHOICEP(seq)) return FD_EMPTY_CHOICE;
  else if (FD_EMPTY_LISTP(seq)) return seq;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","forseq_evalfn",seq);
  else lim = fd_seq_length(seq);
  if (lim==0) return fd_incref(seq);
  else results = u8_alloc_n(lim,fdtype);
  if (FD_PAIRP(seq)) {islist = 1; pairscan = seq;}
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.table_schema = vars; bindings.schema_values = vals; bindings.schema_length = 1;
  bindings.schemap_onstack = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (fdtype)(&bindings); envstruct.env_exports = FD_VOID;
  envstruct.env_copy = NULL;
  vars[0]=var; vals[0]=FD_VOID;
  if (!(FD_VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT(0);
    bindings.schema_length = 2; iterval = &(vals[1]);}
  while (i<lim) {
    fdtype elt = (islist)?(fd_refcar(pairscan)):(fd_seq_elt(seq,i));
    fdtype val = FD_VOID;
    vals[0]=elt;
    if (iterval) *iterval = FD_INT(i);
    {FD_DOLIST(subexpr,body) {
        fd_decref(val);
        val = fast_eval(subexpr,&envstruct);
        if (FD_THROWP(val)) {
          if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
          u8_destroy_rwlock(&(bindings.table_rwlock));
          fd_decref(vals[0]); fd_decref(seq);
          return val;}
        else if (FD_ABORTED(val)) {
          fdtype errbind;
          u8_destroy_rwlock(&(bindings.table_rwlock));
          if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
          fd_decref(vals[0]); fd_decref(seq);
          return val;}}}
    if (envstruct.env_copy) {
      fd_recycle_environment(envstruct.env_copy);
      envstruct.env_copy = NULL;}
    fd_decref(vals[0]);
    results[i]=val;
    if (islist) pairscan = FD_CDR(pairscan);
    i++;}
  u8_destroy_rwlock(&(bindings.table_rwlock));
  result = fd_makeseq(FD_PTR_TYPE(seq),lim,results);
  fd_decref(seq);
  i = 0; while (i<lim) {fdtype v = results[i++]; fd_decref(v);}
  u8_free(results);
  return result;
}

/* TRYSEQ */

static fdtype tryseq_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  int i = 0, lim, islist = 0;
  fdtype seq, count_var = FD_VOID, *iterval = NULL;
  fdtype var = parse_control_spec(expr,&seq,&count_var,env,_stack);
  fdtype val = FD_EMPTY_CHOICE;
  fdtype vars[2], vals[2], pairscan = FD_VOID;
  fdtype body = fd_get_body(expr,2);
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTED(var)) return var;
  else if (FD_EMPTY_CHOICEP(seq)) return FD_EMPTY_CHOICE;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","try_evalfn",seq);
  else lim = fd_seq_length(seq);
  if (lim==0) {
    fd_decref(seq);
    return FD_EMPTY_CHOICE;}
  if (FD_PAIRP(seq)) {islist = 1; pairscan = seq;}
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.table_schema = vars; bindings.schema_values = vals; bindings.schema_length = 1;
  bindings.schemap_onstack = 1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (fdtype)(&bindings); envstruct.env_exports = FD_VOID;
  envstruct.env_copy = NULL;
  vars[0]=var; vals[0]=FD_VOID;
  if (!(FD_VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT(0);
    bindings.schema_length = 2; iterval = &(vals[1]);}
  while (i<lim) {
    fdtype elt = (islist)?(fd_refcar(pairscan)):(fd_seq_elt(seq,i));
    if (envstruct.env_copy) {
      fd_set_value(var,elt,envstruct.env_copy);
      if (iterval)
        fd_set_value(count_var,FD_INT(i),envstruct.env_copy);}
    else {
      vals[0]=elt;
      if (iterval) *iterval = FD_INT(i);}
    {FD_DOLIST(subexpr,body) {
        fd_decref(val);
        val = fast_eval(subexpr,&envstruct);
        if (FD_THROWP(val)) {
          if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
          u8_destroy_rwlock(&(bindings.table_rwlock));
          fd_decref(elt); fd_decref(seq);
          return val;}
        else if (FD_ABORTED(val)) {
          fdtype errbind;
          u8_destroy_rwlock(&(bindings.table_rwlock));
          if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
          fd_decref(elt); fd_decref(seq);
          return val;}}}
    if (envstruct.env_copy) {
      fd_recycle_environment(envstruct.env_copy);
      envstruct.env_copy = NULL;}
    fd_decref(vals[0]);
    if (FD_EMPTY_CHOICEP(val)) {
      if (islist) pairscan = FD_CDR(pairscan);
      i++;}
    else break;}
  fd_decref(seq);
  u8_destroy_rwlock(&(bindings.table_rwlock));
  return val;
}

/* DOLIST */

static fdtype dolist_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype list, count_var, var=
    parse_control_spec(expr,&list,&count_var,env,_stack);
  fdtype *vloc = NULL, *iloc = NULL;
  fdtype vars[2], vals[2];
  fdtype body = fd_get_body(expr,2);
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTED(var)) return var;
  else if (FD_EMPTY_LISTP(list)) return FD_VOID;
  else if (!(FD_PAIRP(list)))
    return fd_type_error("list","dolist_evalfn",list);
  else if (FD_EMPTY_LISTP(list)) return FD_VOID;
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  if (FD_VOIDP(count_var)) {
    bindings.schema_length = 1;
    vars[0]=var; vals[0]=FD_VOID;
    vloc = &(vals[0]);}
  else {
    bindings.schema_length = 2;
    vars[1]=var; vals[1]=FD_VOID; vloc = &(vals[1]);
    vars[0]=count_var; vals[0]=FD_INT(0); iloc = &(vals[0]);}
  bindings.schemap_onstack = 1;
  bindings.table_schema = vars; bindings.schema_values = vals;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_parent = env;
  envstruct.env_bindings = (fdtype)(&bindings); envstruct.env_exports = FD_VOID;
  envstruct.env_copy = NULL;
  {int i = 0; FD_DOLIST(elt,list) {
      *vloc = elt; fd_incref(elt); if (iloc) *iloc = FD_INT(i);
      {FD_DOLIST(subexpr,body) {
          fdtype val = fast_eval(subexpr,&envstruct);
          if (FD_THROWP(val)) {
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            fd_decref(list); fd_decref(*vloc);
            u8_destroy_rwlock(&(bindings.table_rwlock));
            return val;}
          else if (FD_ABORTED(val)) {
            fdtype errenv;
            if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
            fd_decref(list); fd_decref(*vloc);
            u8_destroy_rwlock(&(bindings.table_rwlock));
            return val;}
          fd_decref(val);}}
      if (envstruct.env_copy) {
        fd_recycle_environment(envstruct.env_copy);
        envstruct.env_copy = NULL;}
      fd_decref(*vloc);
      i++;}}
  u8_destroy_rwlock(&(bindings.table_rwlock));
  fd_decref(list);
  if (envstruct.env_copy) fd_recycle_environment(envstruct.env_copy);
  return FD_VOID;
}

/* BEGIN, PROG1, and COMMENT */

static fdtype begin_evalfn(fdtype begin_expr,fd_lispenv env,fd_stack _stack)
{
  return eval_body("BEGIN",NULL,begin_expr,1,env,_stack);
}

static fdtype prog1_evalfn(fdtype prog1_expr,fd_lispenv env,fd_stack _stack)
{
  fdtype arg1 = fd_get_arg(prog1_expr,1);
  fdtype result = fd_stack_eval(arg1,env,_stack,0);
  if (FD_ABORTED(result))
    return result;
  else {
    fdtype prog1_body = fd_get_body(prog1_expr,2);
    FD_DOLIST(subexpr,prog1_body) {
      fdtype tmp = fast_eval(subexpr,env);
      if (FD_ABORTED(tmp)) {
        fd_decref(result);
        return tmp;}
      fd_decref(tmp);}
    return result;}
}

static fdtype comment_evalfn(fdtype comment_expr,fd_lispenv env,fd_stack stack)
{
  return FD_VOID;
}

/* IPEVAL */

#if FD_IPEVAL_ENABLED
struct IPEVAL_STRUCT { fdtype expr, fd_value; fd_lispenv env;};

static int ipeval_step(struct IPEVAL_STRUCT *s)
{
  fdtype fd_value = fd_eval(s->expr,s->env);
  fd_decref(s->kv_val); s->kv_val = fd_value;
  if (FD_ABORTED(fd_value))
    return -1;
  else return 1;
}

static fdtype ipeval_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  struct IPEVAL_STRUCT tmp;
  tmp.expr = fd_refcar(fd_refcdr(expr)); tmp.env = env; tmp.kv_val = FD_VOID;
  fd_ipeval_call((fd_ipevalfn)ipeval_step,&tmp);
  return tmp.kv_val;
}

static fdtype trace_ipeval_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  struct IPEVAL_STRUCT tmp; int old_trace = fd_trace_ipeval;
  tmp.expr = fd_refcar(fd_refcdr(expr)); tmp.env = env; tmp.kv_val = FD_VOID;
  fd_trace_ipeval = 1;
  fd_ipeval_call((fd_ipevalfn)ipeval_step,&tmp);
  fd_trace_ipeval = old_trace;
  return tmp.kv_val;
}

static fdtype track_ipeval_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  struct IPEVAL_STRUCT tmp;
  struct FD_IPEVAL_RECORD *records; int n_cycles; double total_time;
  fdtype *vec; int i = 0;
  tmp.expr = fd_refcar(fd_refcdr(expr)); tmp.env = env; tmp.kv_val = FD_VOID;
  fd_tracked_ipeval_call((fd_ipevalfn)ipeval_step,&tmp,&records,&n_cycles,&total_time);
  vec = u8_alloc_n(n_cycles,fdtype);
  i = 0; while (i<n_cycles) {
    struct FD_IPEVAL_RECORD *record = &(records[i]);
    vec[i++]=
      fd_make_nvector(3,FD_INT(record->delays),
                      fd_init_double(NULL,record->exec_time),
                      fd_init_double(NULL,record->fetch_time));}
  return fd_make_nvector(3,tmp.kv_val,
                         fd_init_double(NULL,total_time),
                         fd_init_vector(NULL,n_cycles,vec));
}
#endif

/* Initialize functions */

FD_EXPORT void fd_init_iterators_c()
{
  moduleid_symbol = fd_intern("%MODULEID");
  iter_var = fd_intern("%ITER");

  u8_register_source_file(_FILEINFO);

  fd_defspecial(fd_scheme_module,"UNTIL",until_evalfn);
  fd_defspecial(fd_scheme_module,"WHILE",while_evalfn);
  fd_defspecial(fd_scheme_module,"DOTIMES",dotimes_evalfn);
  fd_defspecial(fd_scheme_module,"DOLIST",dolist_evalfn);
  fd_defspecial(fd_scheme_module,"DOSEQ",doseq_evalfn);
  fd_defspecial(fd_scheme_module,"FORSEQ",forseq_evalfn);
  fd_defspecial(fd_scheme_module,"TRYSEQ",tryseq_evalfn);

  fd_defspecial(fd_scheme_module,"BEGIN",begin_evalfn);
  fd_defspecial(fd_scheme_module,"PROG1",prog1_evalfn);
  fd_defspecial(fd_scheme_module,"COMMENT",comment_evalfn);
  fd_defalias(fd_scheme_module,"*******","COMMENT");

#if FD_IPEVAL_ENABLED
  fd_defspecial(fd_scheme_module,"IPEVAL",ipeval_evalfn);
  fd_defspecial(fd_scheme_module,"TIPEVAL",trace_ipeval_evalfn);
  fd_defspecial(fd_scheme_module,"TRACK-IPEVAL",track_ipeval_evalfn);
#endif

}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
