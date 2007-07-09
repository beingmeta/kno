/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/sequences.h"
#include "fdb/fddb.h"
#include "fdb/numbers.h"
#include "eval_internals.h"

/* Helper functions */

static fdtype iter_var;

/* These are for returning binding information in the backtrace. */
static fdtype iterenv1(fdtype seq,fdtype var,fdtype val)
{
  struct FD_KEYVAL *keyvals=u8_malloc(sizeof(struct FD_KEYVAL)*2);
  keyvals[0].key=iter_var; keyvals[0].value=fd_incref(seq);
  keyvals[1].key=var; keyvals[1].value=fd_incref(val);
  return fd_init_slotmap(NULL,2,keyvals);
}
static fdtype iterenv2
  (fdtype seq, fdtype var,fdtype val,fdtype xvar,fdtype xval)
{
  struct FD_KEYVAL *keyvals=u8_malloc(sizeof(struct FD_KEYVAL)*3);
  keyvals[0].key=iter_var; keyvals[0].value=fd_incref(seq);
  keyvals[1].key=var; keyvals[1].value=fd_incref(val);
  keyvals[2].key=xvar; keyvals[2].value=fd_incref(xval);
  return fd_init_slotmap(NULL,3,keyvals);
}

/* Simple iterations */

static fdtype while_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2), result=FD_VOID;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"WHILE",NULL,expr);
  else while ((FD_VOIDP(result)) &&
	      (testeval(test_expr,env,&result))) {
    FD_DOLIST(iter_expr,body) {
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
  fdtype body=fd_get_body(expr,2), result=FD_VOID;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_TooFewExpressions,"UNTIL",NULL,expr);
  else while ((FD_VOIDP(result)) &&
	      (!(testeval(test_expr,env,&result)))) {
    FD_DOLIST(iter_expr,body) {
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
  fdtype body=fd_get_body(expr,2);
  fdtype vars[2], vals[2], inner_env;
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (!(FD_FIXNUMP(limit_val)))
    return fd_type_error("fixnum","dotimes_handler",limit_val);
  else limit=FD_FIX2INT(limit_val);
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=(FD_SCHEMAP_SORTED|FD_SCHEMAP_STACK_SCHEMA);
  bindings.schema=vars; bindings.values=vals; bindings.size=1;
  fd_init_mutex(&(bindings.lock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;  
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct); 
  vars[0]=var; vals[0]=FD_INT2DTYPE(0);
  while (i < limit) {
    if (envstruct.copy) 
      fd_set_value(var,FD_INT2DTYPE(i),envstruct.copy);
    else vals[0]=FD_INT2DTYPE(i);
    {FD_DOLIST(expr,body) {
      fdtype val=fasteval(expr,&envstruct);
      if (FD_THROWP(val)) {
	if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	fd_destroy_mutex(&(bindings.lock));
	return val;}
      else if (FD_ABORTP(val)) {
	fdtype retval=
	  fd_passerr(val,iterenv1(limit_val,var,FD_INT2DTYPE(i)));
	fd_destroy_mutex(&(bindings.lock));
	if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	return retval;}
      fd_decref(val);}}
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    i++;}
  fd_destroy_mutex(&(bindings.lock));
  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
  return FD_VOID;
}

/* DOSEQ */

static fdtype doseq_handler(fdtype expr,fd_lispenv env)
{
  int i=0, lim;
  fdtype seq, count_var=FD_VOID, *iterval=NULL;
  fdtype var=parse_control_spec(expr,&seq,&count_var,env);
  fdtype body=fd_get_body(expr,2);
  fdtype vars[2], vals[2], inner_env;
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
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals; bindings.size=1;
  fd_init_mutex(&(bindings.lock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;  
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct); 
  vars[0]=var; vals[0]=FD_VOID;
  if (!(FD_VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT2DTYPE(0);
    bindings.size=2; iterval=&(vals[1]);} 
  while (i<lim) {
    fdtype elt=fd_seq_elt(seq,i);
    if (envstruct.copy) {
      fd_set_value(var,elt,envstruct.copy);
      if (iterval)
	fd_set_value(count_var,FD_INT2DTYPE(i),envstruct.copy);}
    else {
      vals[0]=elt;
      if (iterval) *iterval=FD_INT2DTYPE(i);}
    {FD_DOLIST(expr,body) {
      fdtype val=fasteval(expr,&envstruct);
      if (FD_THROWP(val)) {
	if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	fd_destroy_mutex(&(bindings.lock));
	fd_decref(elt); fd_decref(seq);
	return val;}
      else if (FD_ABORTP(val)) {
	fdtype errbind;
	if (iterval) errbind=iterenv1(seq,var,elt);
	else errbind=iterenv2(seq,var,elt,count_var,FD_INT2DTYPE(i));
	fd_destroy_mutex(&(bindings.lock));
	if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	fd_decref(elt); fd_decref(seq);
	return fd_passerr(val,errbind);}
      fd_decref(val);}}
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    fd_decref(vals[0]);
    i++;}
  fd_decref(seq);
  fd_destroy_mutex(&(bindings.lock));
  return FD_VOID;
}

/* TRYSEQ */

static fdtype tryseq_handler(fdtype expr,fd_lispenv env)
{
  int i=0, lim;
  fdtype seq, count_var=FD_VOID, *iterval=NULL;
  fdtype var=parse_control_spec(expr,&seq,&count_var,env);
  fdtype body=fd_get_body(expr,2), val=FD_VOID;
  fdtype vars[2], vals[2], inner_env;
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
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals; bindings.size=1;
  fd_init_mutex(&(bindings.lock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;  
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct); 
  vars[0]=var; vals[0]=FD_VOID;
  if (!(FD_VOIDP(count_var))) {
    vars[1]=count_var; vals[1]=FD_INT2DTYPE(0);
    bindings.size=2; iterval=&(vals[1]);} 
  while (i<lim) {
    fdtype elt=fd_seq_elt(seq,i);
    if (envstruct.copy) {
      fd_set_value(var,elt,envstruct.copy);
      if (iterval)
	fd_set_value(count_var,FD_INT2DTYPE(i),envstruct.copy);}
    else {
      vals[0]=elt;
      if (iterval) *iterval=FD_INT2DTYPE(i);}
    {FD_DOLIST(expr,body) {
	fd_decref(val);
	val=fasteval(expr,&envstruct);
	if (FD_THROWP(val)) {
	  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	  fd_destroy_mutex(&(bindings.lock));
	  fd_decref(elt); fd_decref(seq);
	  return val;}
	else if (FD_ABORTP(val)) {
	  fdtype errbind;
	  if (iterval) errbind=iterenv1(seq,var,elt);
	  else errbind=iterenv2(seq,var,elt,count_var,FD_INT2DTYPE(i));
	  fd_destroy_mutex(&(bindings.lock));
	  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	  fd_decref(elt); fd_decref(seq);
	  return fd_passerr(val,errbind);}
	fd_decref(val);}}
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    fd_decref(vals[0]);
    if (FD_EMPTY_CHOICEP(val)) i++;
    else break;}
  fd_decref(seq);
  fd_destroy_mutex(&(bindings.lock));
  return val;
}

/* DOLIST */

static fdtype dolist_handler(fdtype expr,fd_lispenv env)
{
  fdtype list, count_var, var=
    parse_control_spec(expr,&list,&count_var,env);
  fdtype body=fd_get_body(expr,2), *vloc=NULL, *iloc=NULL;
  fdtype vars[2], vals[2], inner_env;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  if (FD_ABORTP(var)) return var;
  else if (FD_EMPTY_LISTP(list)) return FD_VOID;
  else if (!(FD_PAIRP(list)))
    return fd_type_error("list","dolist_handler",list);
  else if (FD_EMPTY_LISTP(list)) return FD_VOID;
  else if (FD_VOIDP(count_var)) {
    bindings.size=1; 
    vars[0]=var; vals[0]=FD_VOID;
    vloc=&(vals[0]);}
  else {
    bindings.size=2;
    vars[1]=var; vals[1]=FD_VOID; vloc=&(vals[1]);
    vars[0]=count_var; vals[0]=FD_INT2DTYPE(0); iloc=&(vals[0]);}
  FD_INIT_STACK_CONS(&bindings,fd_schemap_type);
  bindings.flags=FD_SCHEMAP_STACK_SCHEMA;
  bindings.schema=vars; bindings.values=vals;
  fd_init_mutex(&(bindings.lock));
  FD_INIT_STACK_CONS(&envstruct,fd_environment_type);
  envstruct.parent=env;  
  envstruct.bindings=(fdtype)(&bindings); envstruct.exports=FD_VOID;
  envstruct.copy=NULL;
  inner_env=(fdtype)(&envstruct);
  {int i=0; FD_DOLIST(elt,list) {
    if (envstruct.copy) {
      fd_set_value(var,elt,envstruct.copy);
      if (iloc)
	fd_set_value(count_var,FD_INT2DTYPE(i),envstruct.copy);}
    else {*vloc=elt; fd_incref(elt); if (iloc) *iloc=FD_INT2DTYPE(i);}
    {FD_DOLIST(expr,body) {
      fdtype val=fasteval(expr,&envstruct);
      if (FD_THROWP(val)) {
	if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	fd_destroy_mutex(&(bindings.lock));
	fd_decref(list);
	return val;}
      else if (FD_ABORTP(val)) {
	fdtype errenv;
	if (iloc) errenv=iterenv2(list,var,elt,count_var,FD_INT2DTYPE(i));
	else errenv=iterenv1(list,var,elt);
	if (envstruct.copy) fd_recycle_environment(envstruct.copy);
	fd_destroy_mutex(&(bindings.lock));
	fd_decref(list);
	return fd_passerr(val,errenv);}
      fd_decref(val);}}
    if (envstruct.copy) {
      fd_recycle_environment(envstruct.copy);
      envstruct.copy=NULL;}
    fd_decref(*vloc);
    i++;}}
  fd_destroy_mutex(&(bindings.lock));
  fd_decref(list);
  if (envstruct.copy) fd_recycle_environment(envstruct.copy);
  return FD_VOID;
}

/* BEGIN, PROG1, and COMMENT */

static fdtype begin_handler(fdtype begin_expr,fd_lispenv env)
{
  fdtype exprs=fd_get_body(begin_expr,1);
  return eval_body(exprs,env);
}

static fdtype prog1_handler(fdtype prog1_expr,fd_lispenv env)
{
  fdtype results=fd_eval(fd_get_arg(prog1_expr,1),env);
  fdtype exprs=fd_get_body(prog1_expr,2);
  if (FD_ABORTP(results))
    return results;
  else {
    FD_DOLIST(expr,exprs) {
      fdtype tmp=fd_eval(expr,env);
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
  vec=u8_malloc(sizeof(fdtype)*n_cycles);
  i=0; while (i<n_cycles) {
    struct FD_IPEVAL_RECORD *record=&(records[i]);
    vec[i++]=
      fd_make_vector(3,FD_INT2DTYPE(record->delays),
		     fd_init_double(NULL,record->exec_time),
		     fd_init_double(NULL,record->fetch_time));}
  return fd_make_vector(3,tmp.value,
			fd_init_double(NULL,total_time),
			fd_init_vector(NULL,n_cycles,vec));
}

/* Initialize functions */

FD_EXPORT void fd_init_iterators_c()
{
  iter_var=fd_intern("%ITER");

  fd_register_source_file(versionid);

  fd_defspecial(fd_scheme_module,"UNTIL",until_handler);
  fd_defspecial(fd_scheme_module,"WHILE",while_handler);
  fd_defspecial(fd_scheme_module,"DOTIMES",dotimes_handler);
  fd_defspecial(fd_scheme_module,"DOLIST",dolist_handler);
  fd_defspecial(fd_scheme_module,"DOSEQ",doseq_handler);
  fd_defspecial(fd_scheme_module,"TRYSEQ",tryseq_handler);

  fd_defspecial(fd_scheme_module,"BEGIN",begin_handler);
  fd_defspecial(fd_scheme_module,"PROG1",prog1_handler);
  fd_defspecial(fd_scheme_module,"COMMENT",comment_handler);
  fd_defalias(fd_scheme_module,"*******","COMMENT");

  fd_defspecial(fd_scheme_module,"IPEVAL",ipeval_handler);
  fd_defspecial(fd_scheme_module,"TIPEVAL",trace_ipeval_handler);
  fd_defspecial(fd_scheme_module,"TRACK-IPEVAL",track_ipeval_handler);

}


/* The CVS log for this file
   $Log: iterators.c,v $
   Revision 1.41  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.40  2006/01/20 04:08:59  haase
   Fixed leak in iterating over zero-length vectors

   Revision 1.39  2006/01/18 04:33:06  haase
   Fixed various iteration binding bugs

   Revision 1.38  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.37  2006/01/02 19:59:30  haase
   Fixed erroneous identity of iterative environments

   Revision 1.36  2005/12/23 16:58:29  haase
   Added IPEVAL LET/LET* binding forms

   Revision 1.35  2005/12/22 19:15:50  haase
   Added more comprehensive environment recycling

   Revision 1.34  2005/12/19 01:23:36  haase
   Added COMMENT and ******* (alias) special forms

   Revision 1.33  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.32  2005/06/20 17:37:13  haase
   Fixed stack consed environment initialization

   Revision 1.31  2005/06/20 13:56:59  haase
   Fixes to regularize CONS header initialization

   Revision 1.30  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.29  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.28  2005/04/30 16:13:23  haase
   Made UNTIL handle error returns correctly

   Revision 1.27  2005/04/24 18:30:27  haase
   Made ipeval step function return -1 on error and abort

   Revision 1.26  2005/04/16 16:54:46  haase
   Fix empty list case for DOLIST

   Revision 1.25  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.24  2005/04/13 15:01:06  haase
   Fixed GC bug

   Revision 1.23  2005/04/08 04:46:30  haase
   Improvements to backtrace accumulation

   Revision 1.22  2005/04/07 19:25:03  haase
   Fix schemap inits for stack schemas

   Revision 1.21  2005/04/07 19:11:30  haase
   Made backtraces include environments, fixed some minor GC issues

   Revision 1.20  2005/04/01 15:12:07  haase
   Cleanup from exports reimplementation

   Revision 1.19  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.18  2005/03/14 05:49:31  haase
   Updated comments and internal documentation

   Revision 1.17  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.16  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.15  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.14  2005/02/13 23:55:41  haase
   whitespace changes

*/
