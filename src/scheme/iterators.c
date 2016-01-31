/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
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
#include "framerd/fddb.h"
#include "framerd/numbers.h"
#include "eval_internals.h"

/* Helper functions */

static fdtype iter_var;

/* These are for returning binding information in the backtrace. */
static fdtype iterenv1(fdtype seq,fdtype var,fdtype val)
{
  struct FD_KEYVAL keyvals[2];
  keyvals[0].key=iter_var; keyvals[0].value=fd_incref(seq);
  keyvals[1].key=var; keyvals[1].value=fd_incref(val);
  return fd_make_slotmap(2,2,keyvals);
}
static fdtype iterenv2
  (fdtype seq, fdtype var,fdtype val,fdtype xvar,fdtype xval)
{
  struct FD_KEYVAL keyvals[3];
  keyvals[0].key=iter_var; keyvals[0].value=fd_incref(seq);
  keyvals[1].key=var; keyvals[1].value=fd_incref(val);
  keyvals[2].key=xvar; keyvals[2].value=fd_incref(xval);
  return fd_make_slotmap(3,3,keyvals);
}

/* Simple iterations */

static fdtype while_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1);
  fdtype result=FD_VOID;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"WHILE",NULL,expr);
  else while ((FD_VOIDP(result)) &&
              (testeval(test_expr,env,&result))) {
      FD_DOBODY(iter_expr,expr,2) {
        fdtype val=fasteval(iter_expr,env);
        if (FD_ABORTP(val))
          return val;
        fd_decref(val);}}
  if (FD_ABORTP(result))
    return result;
  else return FD_VOID;
}

static fdtype until_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1);
  fdtype result=FD_VOID;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"UNTIL",NULL,expr);
  else while ((FD_VOIDP(result)) &&
              (!(testeval(test_expr,env,&result)))) {
      FD_DOBODY(iter_expr,expr,2) {
        fdtype val=fasteval(iter_expr,env);
        if (FD_ABORTP(val)) return val;
        else fd_decref(val);}}
  if (FD_ABORTP(result))
    return result;
  else return FD_VOID;
}

/* Parsing for more complex iterations. */

static fdtype parse_control_spec
  (fdtype expr,fdtype *value,fdtype *count_var,fd_lispenv env)
{
  fdtype control_expr=fd_get_arg(expr,1);
  if (FD_VOIDP(control_expr))
    return fd_err(fd_TooFewExpressions,"DO...",NULL,expr);
  else {
    fdtype var=fd_get_arg(control_expr,0), ivar=fd_get_arg(control_expr,2);
    fdtype val_expr=fd_get_arg(control_expr,1), val;
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
    val=fasteval(val_expr,env);
    if (FD_ABORTP(val)) return val;
    *value=val; if (count_var) *count_var=ivar;
    return var;}
}

/* DOTIMES */

static fdtype dotimes_handler(fdtype expr,fd_lispenv env)
{
  int i=0, limit;
  fdtype limit_val, var=parse_control_spec(expr,&limit_val,NULL,env);
  fdtype vars[2], vals[2];
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (!(FD_FIXNUMP(limit_val)))
    return fd_type_error("fixnum","dotimes_handler",limit_val);
  else limit=FD_FIX2INT(limit_val);
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.flags=(FD_SCHEMAP_SORTED|FD_SCHEMAP_STACK_SCHEMA);
  bindings.schema=vars; bindings.values=vals; bindings.size=1;
  fd_init_rwlock(&(bindings.rwlock));
  envstruct.parent=env;
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  vars[0]=var; vals[0]=FD_INT(0);
  while (i < limit) {
    vals[0]=FD_INT(i);
    {FD_DOBODY(subexpr,expr,2) {
        fdtype val=fasteval(subexpr,&envstruct);
        if (FD_THROWP(val)) {
          if (envstruct.copy) fd_recycle_environment(envstruct.copy);
          fd_decref(vals[0]);
          fd_destroy_rwlock(&(bindings.rwlock));
          return val;}
        else if (FD_ABORTP(val)) {
          fd_push_error_context(":DOTIMES",iterenv1(limit_val,var,FD_INT(i)));
          if (envstruct.copy) fd_recycle_environment(envstruct.copy);
          fd_decref(vals[0]);
          fd_destroy_rwlock(&(bindings.rwlock));
          return val;}
        fd_decref(val);}}
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    /* This (setting the variable bound by dotimes) is bad form, but it might happen. */
    fd_decref(vals[0]);
    i++;}
  fd_destroy_rwlock(&(bindings.rwlock));
  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
  return FD_VOID;
}

/* DOSEQ */

static fdtype doseq_handler(fdtype expr,fd_lispenv env)
{
  int i=0, lim, islist=0;
  fdtype seq, count_var=FD_VOID, *iterval=NULL;
  fdtype var=parse_control_spec(expr,&seq,&count_var,env);
  fdtype vars[2], vals[2], pairscan=FD_VOID;
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (FD_EMPTY_CHOICEP(seq)) return FD_VOID;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","doseq_handler",seq);
  else lim=fd_seq_length(seq);
  if (lim==0) {
    fd_decref(seq);
    return FD_VOID;}
  if (FD_PAIRP(seq)) {islist=1; pairscan=seq;}
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals; bindings.size=1;
  fd_init_rwlock(&(bindings.rwlock));
  envstruct.parent=env;
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  vars[0]=var; vals[0]=FD_VOID;
  if (!(FD_VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT(0);
    bindings.size=2; iterval=&(vals[1]);}
  while (i<lim) {
    fdtype elt=(islist)?(fd_car(pairscan)):(fd_seq_elt(seq,i));
    vals[0]=elt;
    if (iterval) *iterval=FD_INT(i);
    {FD_DOBODY(subexpr,expr,2) {
        fdtype val=fasteval(subexpr,&envstruct);
        if (FD_THROWP(val)) {
          if (envstruct.copy) fd_recycle_environment(envstruct.copy);
          fd_decref(vals[0]); fd_decref(seq);
          fd_destroy_rwlock(&(bindings.rwlock));
          return val;}
        else if (FD_ABORTP(val)) {
          fdtype errbind;
          if (iterval) errbind=iterenv1(seq,var,elt);
          else errbind=iterenv2(seq,var,elt,count_var,FD_INT(i));
          if (envstruct.copy) fd_recycle_environment(envstruct.copy);
          fd_decref(vals[0]); fd_decref(seq);
          fd_destroy_rwlock(&(bindings.rwlock));
          fd_push_error_context(":DOSEQ",errbind);
          return val;}
        fd_decref(val);}}
    /* Every iteration is a new environment */
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    fd_decref(vals[0]);
    if (islist) pairscan=FD_CDR(pairscan);
    i++;}
  fd_decref(seq);
  fd_destroy_rwlock(&(bindings.rwlock));
  return FD_VOID;
}

/* FORSEQ */

static fdtype forseq_handler(fdtype expr,fd_lispenv env)
{
  int i=0, lim, islist=0;
  fdtype seq, count_var=FD_VOID, *iterval=NULL, *results, result;
  fdtype var=parse_control_spec(expr,&seq,&count_var,env);
  fdtype vars[2], vals[2], pairscan=FD_VOID;
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (FD_EMPTY_CHOICEP(seq)) return FD_EMPTY_CHOICE;
  else if (FD_EMPTY_LISTP(seq)) return seq;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","forseq_handler",seq);
  else lim=fd_seq_length(seq);
  if (lim==0) return fd_incref(seq);
  else results=u8_alloc_n(lim,fdtype);
  if (FD_PAIRP(seq)) {islist=1; pairscan=seq;}
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals; bindings.size=1;
  fd_init_rwlock(&(bindings.rwlock));
  envstruct.parent=env;
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  vars[0]=var; vals[0]=FD_VOID;
  if (!(FD_VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT(0);
    bindings.size=2; iterval=&(vals[1]);}
  while (i<lim) {
    fdtype elt=(islist)?(fd_car(pairscan)):(fd_seq_elt(seq,i));
    fdtype val=FD_VOID;
    vals[0]=elt;
    if (iterval) *iterval=FD_INT(i);
    {FD_DOBODY(subexpr,expr,2) {
        fd_decref(val);
        val=fasteval(subexpr,&envstruct);
        if (FD_THROWP(val)) {
          if (envstruct.copy) fd_recycle_environment(envstruct.copy);
          fd_destroy_rwlock(&(bindings.rwlock));
          fd_decref(vals[0]); fd_decref(seq);
          return val;}
        else if (FD_ABORTP(val)) {
          fdtype errbind;
          if (iterval) errbind=iterenv1(seq,var,elt);
          else errbind=iterenv2(seq,var,elt,count_var,FD_INT(i));
          fd_destroy_rwlock(&(bindings.rwlock));
          if (envstruct.copy) fd_recycle_environment(envstruct.copy);
          fd_decref(vals[0]); fd_decref(seq);
          fd_push_error_context(":FORSEQ",errbind);
          return val;}}}
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    fd_decref(vals[0]);
    results[i]=val;
    if (islist) pairscan=FD_CDR(pairscan);
    i++;}
  fd_destroy_rwlock(&(bindings.rwlock));
  result=fd_makeseq(FD_PTR_TYPE(seq),lim,results);
  fd_decref(seq);
  i=0; while (i<lim) {fdtype v=results[i++]; fd_decref(v);}
  u8_free(results);
  return result;
}

/* TRYSEQ */

static fdtype tryseq_handler(fdtype expr,fd_lispenv env)
{
  int i=0, lim, islist=0;
  fdtype seq, count_var=FD_VOID, *iterval=NULL;
  fdtype var=parse_control_spec(expr,&seq,&count_var,env);
  fdtype val=FD_EMPTY_CHOICE;
  fdtype vars[2], vals[2], pairscan=FD_VOID;
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (FD_EMPTY_CHOICEP(seq)) return FD_EMPTY_CHOICE;
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","try_handler",seq);
  else lim=fd_seq_length(seq);
  if (lim==0) {
    fd_decref(seq);
    return FD_EMPTY_CHOICE;}
  if (FD_PAIRP(seq)) {islist=1; pairscan=seq;}
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals; bindings.size=1;
  fd_init_rwlock(&(bindings.rwlock));
  envstruct.parent=env;
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  vars[0]=var; vals[0]=FD_VOID;
  if (!(FD_VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT(0);
    bindings.size=2; iterval=&(vals[1]);}
  while (i<lim) {
    fdtype elt=(islist)?(fd_car(pairscan)):(fd_seq_elt(seq,i));
    if (envstruct.copy) {
      fd_set_value(var,elt,envstruct.copy);
      if (iterval)
        fd_set_value(count_var,FD_INT(i),envstruct.copy);}
    else {
      vals[0]=elt;
      if (iterval) *iterval=FD_INT(i);}
    {FD_DOBODY(subexpr,expr,2) {
        fd_decref(val);
        val=fasteval(subexpr,&envstruct);
        if (FD_THROWP(val)) {
          if (envstruct.copy) fd_recycle_environment(envstruct.copy);
          fd_destroy_rwlock(&(bindings.rwlock));
          fd_decref(elt); fd_decref(seq);
          return val;}
        else if (FD_ABORTP(val)) {
          fdtype errbind;
          if (iterval) errbind=iterenv1(seq,var,elt);
          else errbind=iterenv2(seq,var,elt,count_var,FD_INT(i));
          fd_destroy_rwlock(&(bindings.rwlock));
          if (envstruct.copy) fd_recycle_environment(envstruct.copy);
          fd_decref(elt); fd_decref(seq);
          fd_push_error_context(":TRYSEQ",errbind);
          return val;}}}
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    fd_decref(vals[0]);
    if (FD_EMPTY_CHOICEP(val)) {
      if (islist) pairscan=FD_CDR(pairscan);
      i++;}
    else break;}
  fd_decref(seq);
  fd_destroy_rwlock(&(bindings.rwlock));
  return val;
}

/* DOLIST */

static fdtype dolist_handler(fdtype expr,fd_lispenv env)
{
  fdtype list, count_var, var=
    parse_control_spec(expr,&list,&count_var,env);
  fdtype *vloc=NULL, *iloc=NULL;
  fdtype vars[2], vals[2];
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (FD_EMPTY_LISTP(list)) return FD_VOID;
  else if (!(FD_PAIRP(list)))
    return fd_type_error("list","dolist_handler",list);
  else if (FD_EMPTY_LISTP(list)) return FD_VOID;
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  if (FD_VOIDP(count_var)) {
    bindings.size=1;
    vars[0]=var; vals[0]=FD_VOID;
    vloc=&(vals[0]);}
  else {
    bindings.size=2;
    vars[1]=var; vals[1]=FD_VOID; vloc=&(vals[1]);
    vars[0]=count_var; vals[0]=FD_INT(0); iloc=&(vals[0]);}
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals;
  fd_init_rwlock(&(bindings.rwlock));
  envstruct.parent=env;
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  {int i=0; FD_DOLIST(elt,list) {
      *vloc=elt; fd_incref(elt); if (iloc) *iloc=FD_INT(i);
      {FD_DOBODY(subexpr,expr,2) {
          fdtype val=fasteval(subexpr,&envstruct);
          if (FD_THROWP(val)) {
            if (envstruct.copy) fd_recycle_environment(envstruct.copy);
            fd_decref(list); fd_decref(*vloc);
            fd_destroy_rwlock(&(bindings.rwlock));
            return val;}
          else if (FD_ABORTP(val)) {
            fdtype errenv;
            if (iloc) errenv=iterenv2(list,var,elt,count_var,FD_INT(i));
            else errenv=iterenv1(list,var,elt);
            if (envstruct.copy) fd_recycle_environment(envstruct.copy);
            fd_decref(list); fd_decref(*vloc);
            fd_destroy_rwlock(&(bindings.rwlock));
            fd_push_error_context(":DOLIST",errenv);
            return val;}
          fd_decref(val);}}
      if (envstruct.copy) {
        fd_recycle_environment(envstruct.copy);
        envstruct.copy=NULL;}
      fd_decref(*vloc);
      i++;}}
  fd_destroy_rwlock(&(bindings.rwlock));
  fd_decref(list);
  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
  return FD_VOID;
}

/* BEGIN, PROG1, and COMMENT */

static fdtype begin_handler(fdtype begin_expr,fd_lispenv env)
{
  return eval_body("BEGIN",begin_expr,1,env);
}

static fdtype prog1_handler(fdtype prog1_expr,fd_lispenv env)
{
  fdtype arg1=fd_get_arg(prog1_expr,1);
  fdtype results=fd_eval(arg1,env);
  if (FD_ABORTP(results))
    return results;
  else {
    FD_DOBODY(subexpr,prog1_expr,2) {
      fdtype tmp=fd_eval(subexpr,env);
      if (FD_ABORTP(tmp)) {
        fd_decref(results);
        return tmp;}
      fd_decref(tmp);}
    return results;}
}

static fdtype comment_handler(fdtype comment_expr,fd_lispenv env)
{
  return FD_VOID;
}

/* IPEVAL */

#if FD_IPEVAL_ENABLED
struct IPEVAL_STRUCT { fdtype expr, value; fd_lispenv env;};

static int ipeval_step(struct IPEVAL_STRUCT *s)
{
  fdtype value=fd_eval(s->expr,s->env);
  fd_decref(s->value); s->value=value;
  if (FD_ABORTP(value))
    return -1;
  else return 1;
}

static fdtype ipeval_handler(fdtype expr,fd_lispenv env)
{
  struct IPEVAL_STRUCT tmp;
  tmp.expr=fd_car(fd_cdr(expr)); tmp.env=env; tmp.value=FD_VOID;
  fd_ipeval_call((fd_ipevalfn)ipeval_step,&tmp);
  return tmp.value;
}

static fdtype trace_ipeval_handler(fdtype expr,fd_lispenv env)
{
  struct IPEVAL_STRUCT tmp; int old_trace=fd_trace_ipeval;
  tmp.expr=fd_car(fd_cdr(expr)); tmp.env=env; tmp.value=FD_VOID;
  fd_trace_ipeval=1;
  fd_ipeval_call((fd_ipevalfn)ipeval_step,&tmp);
  fd_trace_ipeval=old_trace;
  return tmp.value;
}

static fdtype track_ipeval_handler(fdtype expr,fd_lispenv env)
{
  struct IPEVAL_STRUCT tmp;
  struct FD_IPEVAL_RECORD *records; int n_cycles; double total_time;
  fdtype *vec; int i=0;
  tmp.expr=fd_car(fd_cdr(expr)); tmp.env=env; tmp.value=FD_VOID;
  fd_tracked_ipeval_call((fd_ipevalfn)ipeval_step,&tmp,&records,&n_cycles,&total_time);
  vec=u8_alloc_n(n_cycles,fdtype);
  i=0; while (i<n_cycles) {
    struct FD_IPEVAL_RECORD *record=&(records[i]);
    vec[i++]=
      fd_make_nvector(3,FD_INT(record->delays),
                      fd_init_double(NULL,record->exec_time),
                      fd_init_double(NULL,record->fetch_time));}
  return fd_make_nvector(3,tmp.value,
                         fd_init_double(NULL,total_time),
                         fd_init_vector(NULL,n_cycles,vec));
}
#endif

/* Initialize functions */

FD_EXPORT void fd_init_iterators_c()
{
  iter_var=fd_intern("%ITER");

  u8_register_source_file(_FILEINFO);

  fd_defspecial(fd_scheme_module,"UNTIL",until_handler);
  fd_defspecial(fd_scheme_module,"WHILE",while_handler);
  fd_defspecial(fd_scheme_module,"DOTIMES",dotimes_handler);
  fd_defspecial(fd_scheme_module,"DOLIST",dolist_handler);
  fd_defspecial(fd_scheme_module,"DOSEQ",doseq_handler);
  fd_defspecial(fd_scheme_module,"FORSEQ",forseq_handler);
  fd_defspecial(fd_scheme_module,"TRYSEQ",tryseq_handler);

  fd_defspecial(fd_scheme_module,"BEGIN",begin_handler);
  fd_defspecial(fd_scheme_module,"PROG1",prog1_handler);
  fd_defspecial(fd_scheme_module,"COMMENT",comment_handler);
  fd_defalias(fd_scheme_module,"*******","COMMENT");

#if FD_IPEVAL_ENABLED
  fd_defspecial(fd_scheme_module,"IPEVAL",ipeval_handler);
  fd_defspecial(fd_scheme_module,"TIPEVAL",trace_ipeval_handler);
  fd_defspecial(fd_scheme_module,"TRACK-IPEVAL",track_ipeval_handler);
#endif

}


/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
