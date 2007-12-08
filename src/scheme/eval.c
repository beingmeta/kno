/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_PPTRS 1

#include "fdb/dtype.h"
#include "fdb/support.h"
#include "fdb/fddb.h"
#include "fdb/eval.h"
#include "fdb/dtproc.h"
#include "fdb/numbers.h"
#include "fdb/sequences.h"
#include "fdb/ports.h"
#include "fdb/dtcall.h"

#include <libu8/u8timefns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

static int fdscheme_initialized=0;

int fd_optimize_tail_calls=1;

fdtype fd_scheme_module, fd_xscheme_module;

fdtype _fd_comment_symbol;

static fdtype quote_symbol, comment_symbol;

static fd_exception TestFailed=_("Test failed");
static fd_exception ExpiredThrow=_("Continuation is no longer valid");
static fd_exception DoubleThrow=_("Continuation used twice");
static fd_exception LostThrow=_("Lost invoked continuation");

fd_exception
  fd_SyntaxError=_("SCHEME expression syntax error"),
  fd_InvalidMacro=_("invalid macro transformer"),
  fd_UnboundIdentifier=_("the variable is unbound"),
  fd_VoidArgument=_("VOID result passed as argument"),
  fd_NotAnIdentifier=_("not an identifier"),
  fd_TooFewExpressions=_("too few subexpressions"),
  fd_CantBind=_("can't add binding to environment"),
  fd_ReadOnlyEnv=_("Read only environment");

u8_context fd_eval_context="EVAL";

/* Environment functions */

static fdtype lexref_prim(fdtype upv,fdtype acrossv)
{
  int up=FD_FIX2INT(upv), across=FD_FIX2INT(acrossv);
  int combined=((up<<5)|(across));
  return FDTYPE_IMMEDIATE(fd_lexref_type,combined);
}

static int unparse_lexref(u8_output out,fdtype lexref)
{
  int code=FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up=code/32, across=code%32;
  u8_printf(out,"#<LEXREF ^%d+%d>",up,across);
  return 1;
}

FD_EXPORT int fd_bind_value(fdtype sym,fdtype val,fd_lispenv env)
{
  if (env) {
    if (fd_store(env->bindings,sym,val)<0) {
      fd_poperr(NULL,NULL,NULL,NULL);
      fd_seterr(fd_CantBind,"fd_bind_value",NULL,sym);
      return -1;}
    if (FD_HASHTABLEP(env->exports))
      fd_hashtable_op((fd_hashtable)(env->exports),fd_table_replace,sym,val);
    return 1;}
  else return 0;
}

static int bound_in_envp(fdtype symbol,fd_lispenv env)
{
  fdtype bindings=env->bindings;
  if (FD_HASHTABLEP(bindings))
    return fd_hashtable_probe((fd_hashtable)bindings,symbol);
  else if (FD_SLOTMAPP(bindings))
    return fd_slotmap_test((fd_slotmap)bindings,symbol,FD_VOID);
  else if (FD_SCHEMAPP(bindings))
    return fd_schemap_test((fd_schemap)bindings,symbol,FD_VOID);
  else return fd_test(bindings,symbol,FD_VOID);
}

FD_EXPORT int fd_set_value(fdtype symbol,fdtype value,fd_lispenv env)
{
  if (env->copy) env=env->copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env=env->parent;
      if ((env) && (env->copy)) env=env->copy;}
    else if ((env->bindings) == (env->exports)) 
      /* This is the kind of environment produced by using a module,
	 so it's read only. */
      return fd_reterr(fd_ReadOnlyEnv,"fd_set_value",NULL,symbol);
    else {
      fd_store(env->bindings,symbol,value);
      if (FD_HASHTABLEP(env->exports))
	fd_hashtable_op((fd_hashtable)(env->exports),
			fd_table_replace,symbol,value);
      return 1;}}
  return 0;
}

FD_EXPORT fdtype _fd_symeval(fdtype sym,fd_lispenv env)
{
  return fd_symeval(sym,env);
}

FD_EXPORT fdtype _fd_lexref(fdtype lexref,fd_lispenv env)
{
  return fd_lexref(lexref,env);
}

FD_EXPORT int fd_add_value(fdtype symbol,fdtype value,fd_lispenv env)
{
  if (env->copy) env=env->copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env=env->parent;
      if ((env) && (env->copy)) env=env->copy;}
    else if ((env->bindings) == (env->exports)) 
      return fd_reterr(fd_ReadOnlyEnv,"fd_set_value",NULL,symbol);
    else {
      fd_add(env->bindings,symbol,value);
      if ((FD_HASHTABLEP(env->exports)) &&
	  (fd_hashtable_probe((fd_hashtable)(env->exports),symbol)))
	fd_add(env->exports,symbol,value);
      return 1;}}
  return 0;
}

static fd_lispenv copy_environment(fd_lispenv env);

static fd_lispenv dynamic_environment(fd_lispenv env)
{
  if (env->copy) return env->copy;
  else {
    struct FD_ENVIRONMENT *newenv=u8_alloc(struct FD_ENVIRONMENT);
    FD_INIT_CONS(newenv,fd_environment_type);
    if (env->parent) 
      newenv->parent=copy_environment(env->parent);
    else newenv->parent=NULL;
    if (FD_STACK_CONSP(FD_CONS_DATA(env->bindings)))
      newenv->bindings=fd_copy(env->bindings);
    else newenv->bindings=fd_incref(env->bindings);
    newenv->exports=fd_incref(env->exports);
    env->copy=newenv; newenv->copy=newenv;
    return newenv;}
}

static fd_lispenv copy_environment(fd_lispenv env)
{
  fd_lispenv dynamic=((env->copy) ? (env->copy) : (dynamic_environment(env)));
  return (fd_lispenv) fd_incref((fdtype)dynamic);
}

static fdtype lisp_copy_environment(fdtype env,int deep)
{
  return (fdtype) copy_environment((fd_lispenv)env);
}

FD_EXPORT fd_lispenv fd_copy_env(fd_lispenv env)
{
  if (env==NULL) return env;
  else if (env->copy)
    return (fd_lispenv)(fd_incref((fdtype)(env->copy)));
  else return copy_environment(env);
}

static void recycle_environment(struct FD_CONS *envp)
{
  struct FD_ENVIRONMENT *env=(struct FD_ENVIRONMENT *)envp;
  fd_decref(env->bindings); fd_decref(env->exports);
  if (env->parent) fd_decref((fdtype)(env->parent));
  u8_free(env);
}

static int env_recycle_depth=2;

static int count_cons_envrefs(fdtype obj,fd_lispenv env,int depth);

FD_FASTOP int count_envrefs(fdtype obj,fd_lispenv env,int depth)
{
  if (depth<=0) return 0;
  else if (FD_ATOMICP(obj)) return 0;
  else return count_cons_envrefs(obj,env,depth);
}

static int count_cons_envrefs(fdtype obj,fd_lispenv env,int depth)
{
  struct FD_CONS *cons=(struct FD_CONS *)obj;
  int refcount=FD_CONS_REFCOUNT(cons), constype=FD_CONS_TYPE(cons);
  if (refcount==1)
    switch (constype) {
    case fd_pair_type:
      return count_envrefs(FD_CAR(obj),env,depth)+count_envrefs(FD_CDR(obj),env,depth);
    case fd_vector_type: {
      int envcount=0;
      int i=0, len=FD_VECTOR_LENGTH(obj); fdtype *elts=FD_VECTOR_DATA(obj);
      while (i<len) {
	envcount=envcount+count_envrefs(elts[i],env,depth-1); i++;}
      return envcount;}
    case fd_schemap_type: {
      int envcount=0;
      int i=0, len=FD_SCHEMAP_SIZE(obj); fdtype *elts=FD_XSCHEMAP(obj)->values;
      while (i<len) {
	envcount=envcount+count_envrefs(elts[i],env,depth-1); i++;}
      return envcount;}
    case fd_slotmap_type: {
      int envcount=0;
      int i=0, len=FD_SLOTMAP_SIZE(obj);
      struct FD_KEYVAL *kv=(FD_XSLOTMAP(obj))->keyvals;
      while (i<len) {
	envcount=envcount+count_envrefs(kv[i].value,env,depth-1); i++;}
      return envcount;}
    case fd_hashtable_type: {
      int envcount=0;
      struct FD_HASHTABLE *ht=(struct FD_HASHTABLE *)obj;
      int i=0, n_slots; struct FD_HASHENTRY **slots; 
      fd_lock_struct(ht);
      n_slots=ht->n_slots; slots=ht->slots;
      while (i<n_slots)
	if (slots[i]) {
	  struct FD_HASHENTRY *hashentry=slots[i++];
	  int j=0, n_keyvals=hashentry->n_keyvals;
	  struct FD_KEYVAL *keyvals=&(hashentry->keyval0);
	  while (j<n_keyvals) {
	    envcount=envcount+count_envrefs(keyvals[j].value,env,depth-1); j++;}}
	else i++;
      fd_unlock_struct(ht);
      return envcount;}
    default:
      if (constype==fd_environment_type) {
	fd_lispenv scan=FD_STRIP_CONS(obj,fd_environment_type,fd_lispenv);
	if (scan==env) return 1; else scan=scan->copy;
	while ((scan) && (scan=scan->copy)) {
	  if (scan==env) return 1;
	  else if (FD_CONS_REFCOUNT(scan)==1)
	    scan=scan->parent;
	  else break;}
	return 0;}
      else if (constype==fd_sproc_type)
	if ((FD_STRIP_CONS(obj,fd_sproc_type,struct FD_SPROC *))->env == env)
	  return 1;
	else return 0;
      else return 0;}
  else return 0;
}

FD_EXPORT
/* fd_recycle_environment:
     Arguments: a lisp pointer to an environment
     Returns: 1 if the environment was recycled.
 This handles circular environment problems.  The problem is that environments
   commonly contain pointers to procedures which point back to the environment
   they are closed in.  This does a limited structure descent to see how many
   reclaimable environment pointers there may be.  If the reclaimable references
   are one more than the environment's reference count, then you can recycle
   the entire environment.
*/
int fd_recycle_environment(fd_lispenv env)
{
  int refcount=FD_CONS_REFCOUNT(env);
  if (refcount==0) return 0; /* Stack cons */
  else if (refcount==1) { /* Normal GC */
    fd_decref((fdtype)env);
    return 1;}
  else {
    int sproc_count=count_envrefs(env->bindings,env,env_recycle_depth);
    if (sproc_count+1==refcount) {
      fd_decref(env->bindings); fd_decref(env->exports);
      if (env->parent) fd_decref((fdtype)(env->parent));
      env->consbits=(0xFFFFFF80|(env->consbits&0x7F));
      u8_free(env);
      return 1;}
    else {fd_decref((fdtype)env); return 0;}}
}

/* Unpacking expressions, non-inline versions */

FD_EXPORT fdtype _fd_get_arg(fdtype expr,int i)
{
  return fd_get_arg(expr,i);
}
FD_EXPORT fdtype _fd_get_body(fdtype expr,int i)
{
  return fd_get_body(expr,i);
}

static fdtype getopt_prim(fdtype opts,fdtype key,fdtype dflt)
{
  return fd_getopt(opts,key,dflt);
}
static fdtype testopt_prim(fdtype opts,fdtype key,fdtype val)
{
  if (fd_testopt(opts,key,val)) return FD_TRUE;
  else return FD_FALSE;
}
static fdtype optplus_prim(fdtype opts,fdtype key,fdtype val)
{
  if (FD_VOIDP(val))
    return fd_init_pair(NULL,key,fd_incref(opts));
  else return fd_init_pair(NULL,fd_init_pair(NULL,key,fd_incref(val)),fd_incref(opts));
}

/* Quote */

static fdtype quote_handler(fdtype obj,fd_lispenv env)
{
  if ((FD_PAIRP(obj)) && (FD_PAIRP(FD_CDR(obj))) &&
      ((FD_CDR(FD_CDR(obj)))==FD_EMPTY_LIST))
    return fd_incref(FD_CAR(FD_CDR(obj)));
  else return fd_err(fd_SyntaxError,"QUOTE",NULL,obj);
}

/* Profiling functions */

static fdtype profile_symbol;

static fdtype profiled_eval(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  double start=u8_elapsed_time();
  fdtype value=fd_eval(toeval,env);
  double finish=u8_elapsed_time();
  fdtype tag=fd_get_arg(expr,2);
  fdtype profile_info=fd_symeval(profile_symbol,env), profile_data;
  if (FD_VOIDP(profile_info)) return value;
  profile_data=fd_get(profile_info,tag,FD_VOID);
  if (FD_ABORTP(profile_data)) {
    fd_decref(value); fd_decref(profile_info);
    return profile_data;}
  else if (FD_VOIDP(profile_data)) {
    fdtype time=fd_init_double(NULL,(finish-start));
    profile_data=fd_init_pair(NULL,FD_INT2DTYPE(1),time);
    fd_store(profile_info,tag,profile_data);}
  else {
    struct FD_PAIR *p=FD_GET_CONS(profile_data,fd_pair_type,fd_pair);
    struct FD_DOUBLE *d=FD_GET_CONS((p->cdr),fd_double_type,fd_double);
    p->car=FD_INT2DTYPE(fd_getint(p->car)+1);
    d->flonum=d->flonum+(finish-start);}
  fd_decref(profile_data); fd_decref(profile_info);
  return value;
}

/* Trace functions */

static fdtype timed_eval(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  double start=u8_elapsed_time();
  fdtype value=fd_eval(toeval,env);
  double finish=u8_elapsed_time();
  u8_message(";; Executed %q in %f seconds, yielding\n;;\t%q",
	     toeval,(finish-start),value);
  return value;
}

static fdtype timed_evalx(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  double start=u8_elapsed_time();
  fdtype value=fd_eval(toeval,env);
  double finish=u8_elapsed_time();
  return fd_make_vector(2,value,fd_init_double(NULL,finish-start));
}

static fdtype watched_eval(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  double start; int oneout=0;
  fdtype scan=FD_CDR(expr);
  if (!(FD_SYMBOLP(toeval))) scan=FD_CDR(scan);
  if (FD_PAIRP(scan)) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    while (FD_PAIRP(scan)) {
      fdtype towatch=FD_CAR(scan);
      if ((FD_STRINGP(towatch)) && (FD_PAIRP(FD_CDR(scan)))) {
	fdtype value=fd_eval((FD_CAR(FD_CDR(scan))),env);
	if (oneout) u8_printf(&out,"; %s=%q",FD_STRDATA(towatch),value);
	else u8_printf(&out,"%s=%q",FD_STRDATA(towatch),value);
	fd_decref(value); scan=FD_CDR(FD_CDR(scan)); oneout=1;}
      else {
	fdtype value=fd_eval(towatch,env);
	if (oneout) u8_printf(&out,"; %q=%q",towatch,value);
	else u8_printf(&out,"%q=%q",towatch,value);
	fd_decref(value); scan=FD_CDR(scan); oneout=1;}}
    if (!(FD_SYMBOLP(toeval))) u8_printf(&out,": %q",toeval);
    u8_logger(-1,"%WATCH",out.u8_outbuf);
    u8_free(out.u8_outbuf);}
  start=u8_elapsed_time();
  if (FD_SYMBOLP(toeval))
    return fd_eval(toeval,env);
  else {
    fdtype value=fd_eval(toeval,env);
    double howlong=u8_elapsed_time()-start;
    if (howlong>1.0)
      u8_log(-1,"%WATCH","<%.3fs> %q => %q",howlong*1000,toeval,value);
    else if (howlong>0.001)
      u8_log(-1,"%WATCH","<%.3fms> %q => %q",howlong*1000,toeval,value);
    else if (remainder(howlong,0.0000001)>=0.001)
      u8_log(-1,"%WATCH","<%.3fus> %q => %q",howlong*1000000,toeval,value);
    else u8_log(-1,"%WATCH","<%.1fus> %q => %q",howlong*1000000,
		toeval,value);
    return value;}
}

/* The opcode evaluator */

#include "opcodes.c"

/* The evaluator itself */

static fdtype apply_function(fdtype fn,fdtype expr,fd_lispenv env);

FD_EXPORT fdtype fd_tail_eval(fdtype expr,fd_lispenv env)
{
  switch (FD_PTR_TYPE(expr)) {
  case fd_symbol_type: {
    fdtype val=fd_symeval(expr,env);
    if (FD_EXPECT_FALSE(FD_VOIDP(val)))
      return fd_err(fd_UnboundIdentifier,"fd_eval",NULL,expr);
    else return val;}
  case fd_pair_type: {
    fdtype head=FD_CAR(expr);
    if (FD_OPCODEP(head))
      return opcode_dispatch(head,expr,env);
    else if (head == quote_symbol)
      return fd_car(FD_CDR(expr));
    else if (head == comment_symbol)
      return FD_VOID;
    else {
      fdtype headval=fasteval(head,env), result;
      int ctype=FD_PTR_TYPE(headval), gc=1;
      if (ctype==fd_pptr_type) {
	fdtype realval=fd_pptr_ref(headval);
	ctype=FD_PTR_TYPE(realval);
	gc=0;}
      if (fd_applyfns[ctype]) 
	result=apply_function(headval,expr,env);
      else if (FD_PRIM_TYPEP(headval,fd_specform_type)) {
	/* These are special forms which do all the evaluating themselves */
	struct FD_SPECIAL_FORM *handler=
	  FD_PTR2CONS(headval,fd_specform_type,struct FD_SPECIAL_FORM *);
	/* fd_calltrack_call(handler->name); */
	result=handler->eval(expr,env);
	/* fd_calltrack_return(handler->name); */
      }
      else if (FD_PRIM_TYPEP(headval,fd_macro_type)) {
	/* These are special forms which do all the evaluating themselves */
	struct FD_MACRO *macrofn=
	  FD_GET_CONS(headval,fd_macro_type,struct FD_MACRO *);
	fdtype xformer=macrofn->transformer;
	int xformer_type=FD_PRIM_TYPE(xformer);
	if (fd_applyfns[xformer_type]) {
	  fdtype new_expr=(fd_applyfns[xformer_type])(xformer,1,&expr);
	  new_expr=fd_finish_call(new_expr);
	  if (FD_ABORTP(new_expr))
	    result=fd_err(fd_SyntaxError,_("macro expansion"),NULL,new_expr);
	  else result=fd_eval(new_expr,env);
	  fd_decref(new_expr);}
	else result=fd_err(fd_InvalidMacro,NULL,macrofn->name,expr);}
      else if (FD_EXPECT_FALSE(FD_VOIDP(headval)))
	result=fd_err(fd_UnboundIdentifier,"for function",NULL,head);
      else if (FD_ABORTP(headval))
	result=fd_incref(headval);
      else result=fd_err(fd_NotAFunction,NULL,NULL,headval);
      if (FD_THROWP(result)) {} 
      else if (FD_ABORTP(result)) {
	fd_push_error_context(fd_eval_context,fd_incref(expr));
	return result;}
      if (gc) fd_decref(headval);
      return result;}}
  case fd_slotmap_type:
    return fd_deep_copy(expr);
  default:
    if (FD_PRIM_TYPEP(expr,fd_lexref_type))
      return fd_lexref(expr,env);
    else return fd_incref(expr);}
}

FD_EXPORT fdtype _fd_eval(fdtype expr,fd_lispenv env)
{
  fdtype result=fd_tail_eval(expr,env);
  return fd_finish_call(result);
}

static fdtype apply_function(fdtype fn,fdtype expr,fd_lispenv env)
{
  fdtype result=FD_VOID, arglist=FD_CDR(expr);
  struct FD_FUNCTION *fcn=FD_PTR2CONS(fn,-1,struct FD_FUNCTION *);
  fdtype _argv[FD_STACK_ARGS], *argv;
  int arg_count=0, n_args=0, args_need_gc=0, free_argv=0;
  int nd_args=0, prune=0, nd_prim=fcn->ndprim;
  int max_arity=fcn->arity, min_arity=fcn->min_arity;
  int n_params=max_arity, argv_length=max_arity;
  /* First, count the arguments */
  FD_DOLIST(elt,arglist) {
    if (!((FD_PAIRP(elt)) && (FD_EQ(FD_CAR(elt),comment_symbol))))
      n_args++;}
  /* If the max_arity is less than zero, it's a lexpr, so the
     number of args is the number of args. */
  if (max_arity<0) argv_length=n_args;
  /* Check if there are too many arguments. */
  else if (FD_EXPECT_FALSE(n_args>max_arity))
    return fd_err(fd_TooManyArgs,"apply_function",fcn->name,expr);
  else if (FD_EXPECT_FALSE((min_arity>=0) && (n_args<min_arity)))
    return fd_err(fd_TooFewArgs,"apply_function",fcn->name,expr);
  if (argv_length>FD_STACK_ARGS) {
    /* If there are more than _FD_STACK_ARGS, malloc a vector for them. */
    argv=u8_alloc_n(argv_length,fdtype);
    free_argv=1;}
  /* Otherwise, just use the stack vector */
  else argv=_argv;
  /* Now we evaluate each of the subexpressions to fill the arg vector */
  {FD_DOLIST(elt,arglist) {
    fdtype argval;
    if ((FD_PAIRP(elt)) && (FD_EQ(FD_CAR(elt),comment_symbol)))
      continue;
    argval=fasteval(elt,env);
    if (FD_ABORTP(argval)) {
      /* Break if one of the arguments returns an error */
      result=argval; break;}
    if (FD_VOIDP(argval)) {
      result=fd_err(fd_VoidArgument,"apply_function",fcn->name,elt);
      break;}
    if ((FD_EMPTY_CHOICEP(argval)) && (!(nd_prim))) {
      /* Break if the method isn't non-deterministic and one of the
	 arguments returns an empty choice. */
      prune=1; break;}
    /* Convert the argument to a simple choice (or non choice) */
    argval=fd_simplify_choice(argval);
    if (FD_CONSP(argval)) args_need_gc=1;
    /* Keep track of whether there are non-deterministic args
       which would require calling fd_ndapply. */
    if (FD_CHOICEP(argval)) nd_args=1;
    else if (FD_QCHOICEP(argval)) nd_args=1;
    /* Fill in the slot */
    argv[arg_count++]=argval;}}
  /* Now, check if we need to exit, either because some argument returned
     an error (or throw) or because the call is being pruned. */
  if ((prune) || (FD_ABORTP(result))) {
    /* In this case, we won't call the procedure at all, because
       either a parameter produced an error or because we can prune
       the call because some parameter is an empty choice. */
    int i=0;
    if (args_need_gc) while (i<arg_count) {
      /* Clean up the arguments we've already evaluated */
      fdtype arg=argv[i++]; fd_decref(arg);}
    if (free_argv) u8_free(argv);
    if (FD_ABORTP(result))
      /* This could extend the backtrace */
      return result;
    else return FD_EMPTY_CHOICE;}
  if ((n_params<0) || (fcn->xprim)) {}
  /* Don't fill anything in for lexprs or non primitives.  */
  else if (arg_count != argv_length) {
    if (fcn->defaults) 
      while (arg_count<argv_length) {
	argv[arg_count]=fd_incref(fcn->defaults[arg_count]); arg_count++;}
    else while (arg_count<argv_length) argv[arg_count++]=FD_VOID;}
  else {}
  if ((fd_optimize_tail_calls) && (FD_PTR_TYPEP(fn,fd_sproc_type)))
    result=fd_tail_call(fn,arg_count,argv);
  else if ((nd_prim==0) && (nd_args))
    result=fd_ndapply(fn,arg_count,argv);
  else {
    result=fd_dapply(fn,arg_count,argv);}
  if ((FD_ABORTP(result)) &&
      (!(FD_THROWP(result))) &&
      (!(FD_PTR_TYPEP(fn,fd_sproc_type)))) {
    /* If it's not an sproc, we add an entry to the backtrace
       that shows the arguments, since they probably don't show
       up in an environment on the backtrace. */
    fdtype *avec=u8_alloc_n((arg_count+1),fdtype);
    memcpy(avec+1,argv,sizeof(fdtype)*(arg_count));
    if (fcn->filename)
      if (fcn->name)
	avec[0]=fd_init_pair(NULL,fd_intern(fcn->name),
			     fdtype_string(fcn->filename));
      else avec[0]=fd_init_pair
	     (NULL,fd_intern("LAMBDA"),fdtype_string(fcn->filename));
    else if (fcn->name) avec[0]=fd_intern(fcn->name);
    else avec[0]=fd_intern("LAMBDA");
    if (free_argv) u8_free(argv);
    fd_push_error_context
      (fd_apply_context,fd_init_vector(NULL,arg_count+1,avec));
    return result;}
  else if (args_need_gc) {
    int i=0; while (i < arg_count) {
      fdtype arg=argv[i++]; fd_decref(arg);}}
  if (free_argv) u8_free(argv);
  return result;
}

FD_EXPORT fdtype fd_eval_exprs(fdtype exprs,fd_lispenv env)
{
  fdtype value=FD_VOID;
  while (FD_PAIRP(exprs)) {
    fd_decref(value);
    value=fd_eval(FD_CAR(exprs),env);
    if (FD_ABORTP(value)) return value;
    exprs=FD_CDR(exprs);}
  return value;
}
/* Module system */

static struct FD_HASHTABLE module_map, safe_module_map;
static fd_lispenv default_env=NULL, safe_default_env=NULL;

FD_EXPORT fd_lispenv fd_make_env(fdtype bindings,fd_lispenv parent)
{
  if (FD_EXPECT_FALSE(!(FD_TABLEP(bindings)) )) {
    fd_seterr(fd_TypeError,"fd_make_env",
	      u8_mkstring(_("object is not a %m"),"table"),
	      bindings);
    return NULL;}
  else {
    struct FD_ENVIRONMENT *e=u8_alloc(struct FD_ENVIRONMENT);
    FD_INIT_CONS(e,fd_environment_type);
    e->bindings=bindings; e->exports=FD_VOID;
    e->parent=fd_copy_env(parent);
    e->copy=e;
    return e;}
}


FD_EXPORT
/* fd_make_export_env:
    Arguments: a hashtable and an environment
    Returns: a consed environment whose bindings and exports
  are the exports table.  This indicates that the environment
  is "for export only" and cannot be modified. */
fd_lispenv fd_make_export_env(fdtype exports,fd_lispenv parent)
{
  if (FD_EXPECT_FALSE(!(FD_HASHTABLEP(exports)) )) {
    fd_seterr(fd_TypeError,"fd_make_env",
	      u8_mkstring(_("object is not a %m"),"hashtable"),
	      exports);
    return NULL;}
  else {
    struct FD_ENVIRONMENT *e=u8_alloc(struct FD_ENVIRONMENT);
    FD_INIT_CONS(e,fd_environment_type);
    e->bindings=fd_incref(exports); e->exports=fd_incref(e->bindings);
    e->parent=fd_copy_env(parent);
    e->copy=e;
    return e;}
}

FD_EXPORT fd_lispenv fd_working_environment()
{
  if (fdscheme_initialized==0) fd_init_fdscheme();
  return fd_make_env(fd_make_hashtable(NULL,17),default_env);
}
FD_EXPORT fd_lispenv fd_safe_working_environment()
{
  if (fdscheme_initialized==0) fd_init_fdscheme();
  return fd_make_env(fd_make_hashtable(NULL,17),safe_default_env);
}

FD_EXPORT fdtype fd_register_module_x(fdtype name,fdtype module,int flags)
{
  if (flags&FD_MODULE_SAFE)
    fd_hashtable_store(&safe_module_map,name,module);
  else fd_hashtable_store(&module_map,name,module);
  if (flags&FD_MODULE_DEFAULT) {
    fd_lispenv scan;
    if (flags&FD_MODULE_SAFE) {
      scan=safe_default_env;
      while (scan)
	if (FD_EQ(scan->bindings,module))
	  /* It's okay to return now, because if it's in the safe module
	     defaults it's also in the risky module defaults. */
	  return module;
	else scan=scan->parent;
      safe_default_env->parent=
	fd_make_env(fd_incref(module),safe_default_env->parent);}
    scan=default_env;
    while (scan)
      if (FD_EQ(scan->bindings,module)) return module;
      else scan=scan->parent;
    default_env->parent=
      fd_make_env(fd_incref(module),default_env->parent);}
  return module;
}

FD_EXPORT fdtype fd_register_module(char *name,fdtype module,int flags)
{
  return fd_register_module_x(fd_intern(name),module,flags);
}

FD_EXPORT fdtype fd_new_module(char *name,int flags)
{
  fdtype module_name, module, as_stored;
  if (fdscheme_initialized==0) fd_init_fdscheme();
  module_name=fd_intern(name);
  module=fd_make_hashtable(NULL,0);
  if (flags&FD_MODULE_SAFE) {
    fd_hashtable_op
      (&safe_module_map,fd_table_default,module_name,module);
    as_stored=fd_get((fdtype)&safe_module_map,module_name,FD_VOID);}
  else {
    fd_hashtable_op
      (&module_map,fd_table_default,module_name,module);
    as_stored=fd_get((fdtype)&module_map,module_name,FD_VOID);}
  if (!(FD_EQ(module,as_stored))) {
    fd_decref(module);
    return as_stored;}
  else fd_decref(as_stored);
  if (flags&FD_MODULE_DEFAULT) {
    if (flags&FD_MODULE_SAFE)
      safe_default_env->parent=fd_make_env(module,safe_default_env->parent);
    default_env->parent=fd_make_env(module,default_env->parent);}
  return module;
}

FD_EXPORT fdtype fd_get_module(fdtype name,int safe)
{
  if (safe)
    return fd_hashtable_get(&safe_module_map,name,FD_VOID);
  else {
    fdtype module=fd_hashtable_get(&module_map,name,FD_VOID);
    if (FD_VOIDP(module))
      return fd_hashtable_get(&safe_module_map,name,FD_VOID);
    else return module;}
}

FD_EXPORT int fd_discard_module(fdtype name,int safe)
{
  if (safe)
    return fd_hashtable_store(&safe_module_map,name,FD_VOID);
  else {
    fdtype module=fd_hashtable_get(&module_map,name,FD_VOID);
    if (FD_VOIDP(module))
      return fd_hashtable_store(&safe_module_map,name,FD_VOID);
    else {
      fd_decref(module);
      return fd_hashtable_store(&module_map,name,FD_VOID);}}
}

/* Making some functions */

FD_EXPORT fdtype fd_make_special_form(u8_string name,fd_evalfn fn)
{
  struct FD_SPECIAL_FORM *f=u8_alloc(struct FD_SPECIAL_FORM);
  FD_INIT_CONS(f,fd_specform_type);
  f->name=name; f->eval=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT void fd_defspecial(fdtype mod,u8_string name,fd_evalfn fn)
{
  struct FD_SPECIAL_FORM *f=u8_alloc(struct FD_SPECIAL_FORM);
  FD_INIT_CONS(f,fd_specform_type);
  f->name=name; f->eval=fn;
  fd_store(mod,fd_intern(name),FDTYPE_CONS(f));
  fd_decref(FDTYPE_CONS(f));
}

/* The Evaluator */

static fdtype eval_handler(fdtype x,fd_lispenv env)
{
  fdtype expr_expr=fd_get_arg(x,1);
  fdtype expr=fd_eval(expr_expr,env);
  fdtype result=fd_eval(expr,env);
  fd_decref(expr);
  return result;
}

static fdtype boundp_handler(fdtype expr,fd_lispenv env)
{
  fdtype symbol=fd_get_arg(expr,1);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_handler",NULL,fd_incref(expr));
  else {
    fdtype val=fd_symeval(symbol,env);
    if (FD_VOIDP(val)) return FD_FALSE;
    else if (val == FD_UNBOUND) return FD_FALSE;
    else {
      fd_decref(val); return FD_TRUE;}}
}

static fdtype voidp_handler(fdtype expr,fd_lispenv env)
{
  fdtype result=fd_eval(fd_get_arg(expr,1),env);
  if (FD_VOIDP(result)) return FD_TRUE;
  else {
    fd_decref(result);
    return FD_FALSE;}
}

static fdtype env_handler(fdtype expr,fd_lispenv env)
{
  return (fdtype)fd_copy_env(env);
}

static fdtype symbol_boundp_prim(fdtype symbol,fdtype envarg)
{
  if (!(FD_SYMBOLP(symbol)))
    return fd_type_error(_("symbol"),"boundp_prim",symbol);
  else if (FD_PTR_TYPEP(envarg,fd_environment_type)) {
    fd_lispenv env=(fd_lispenv)envarg;
    fdtype val=fd_symeval(symbol,env);
    if (FD_VOIDP(val)) return FD_FALSE;
    else if (val == FD_UNBOUND) return FD_FALSE;
    else return FD_TRUE;}
  else if (FD_TABLEP(envarg)) {
    fdtype val=fd_get(envarg,symbol,FD_VOID);
    if (FD_VOIDP(val)) return FD_FALSE;
    else return FD_TRUE;}
  else return fd_type_error(_("environment"),"symbol_boundp_prim",envarg);
}

static fdtype environmentp_prim(fdtype arg)
{
  if (FD_PTR_TYPEP(arg,fd_environment_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype get_arg_prim(fdtype expr,fdtype elt,fdtype dflt)
{
  if (FD_PAIRP(expr))
    if (FD_FIXNUMP(elt)) {
      int i=0, lim=FD_FIX2INT(elt); fdtype scan=expr;
      while ((i<lim) && (FD_PAIRP(scan))) {
	scan=FD_CDR(scan); i++;}
      if (FD_PAIRP(scan)) return fd_incref(FD_CAR(scan));
      else return fd_incref(dflt);}
    else return fd_type_error(_("fixnum"),"get_arg_prim",elt);
  else return fd_type_error(_("pair"),"get_arg_prim",expr);
}

static fdtype apply_lexpr(int n,fdtype *args)
{
  if (FD_APPLICABLEP(args[0])) {
    fdtype final_arg=args[n-1], result;
    int final_length=fd_seq_length(final_arg);
    int n_args=(n-2)+final_length;
    fdtype *values=u8_alloc_n(n_args,fdtype);
    int i=1, j=0, lim=n-1, ctype=FD_PRIM_TYPE(args[0]);
    /* Copy regular arguments */
    while (i<lim) {values[j]=fd_incref(args[i]); j++; i++;}
    i=0; while (j<n_args) {
      values[j]=fd_seq_elt(final_arg,i); j++; i++;}
    result=fd_ndapply(args[0],n_args,values);
    i=0; while (i<n_args) {fd_decref(values[i]); i++;}
    u8_free(values);
    return result;}
  else return fd_type_error("function","apply_lexpr",args[0]);
}

/* Initialization */

fd_ptr_type fd_environment_type, fd_specform_type;

extern void fd_init_corefns_c(void);

static fdtype lispenv_get(fdtype e,fdtype s,fdtype d)
{
  fdtype result=fd_symeval(s,FD_XENV(e));
  if (FD_VOIDP(result)) return fd_incref(d);
  else return result;
}
static int lispenv_store(fdtype e,fdtype s,fdtype v)
{
  return fd_bind_value(s,v,FD_XENV(e));
}

/* Some datatype methods */

static int unparse_specform(u8_output out,fdtype x)
{
  struct FD_SPECIAL_FORM *s=
    FD_GET_CONS(x,fd_specform_type,struct FD_SPECIAL_FORM *);
  u8_printf(out,"#<Special Form %s>",s->name);
  return 1;
}
static int unparse_environment(u8_output out,fdtype x)
{
  struct FD_ENVIRONMENT *env=
    FD_GET_CONS(x,fd_environment_type,struct FD_ENVIRONMENT *);
  u8_printf(out,"#<ENVIRONMENT %x>",(unsigned long)env);
  return 1;
}

FD_EXPORT void recycle_specform(struct FD_CONS *c)
{
  struct FD_SPECIAL_FORM *sf=(struct FD_SPECIAL_FORM *)c;
  u8_free(sf->name);
  u8_free(c);
}

/* Call/cc */

static fdtype call_continuation(struct FD_FUNCTION *f,fdtype arg)
{
  struct FD_CONTINUATION *cont=(struct FD_CONTINUATION *)f;
  if (cont->retval==FD_NULL)
    return fd_err(ExpiredThrow,"call_continuation",NULL,arg);
  else if (FD_VOIDP(cont->retval)) {
    cont->retval=fd_incref(arg);
    return FD_THROW_VALUE;}
  else return fd_err(DoubleThrow,"call_continuation",NULL,arg);
}

static fdtype callcc (fdtype proc)
{
  fdtype continuation, value;
  struct FD_CONTINUATION *f=u8_alloc(struct FD_CONTINUATION);
  FD_INIT_CONS(f,fd_function_type);
  f->name="continuation"; f->filename=NULL; 
  f->ndprim=1; f->xprim=1; f->arity=1; f->min_arity=1; 
  f->typeinfo=NULL; f->defaults=NULL;
  f->handler.xcall1=call_continuation; f->retval=FD_VOID;
  continuation=FDTYPE_CONS(f);
  value=fd_apply(proc,1,&continuation);
  if ((value==FD_THROW_VALUE) && (!(FD_VOIDP(f->retval)))) {
    fdtype retval=f->retval;
    f->retval=FD_NULL;
    if (FD_CONS_REFCOUNT(f)>1) 
      u8_log(LOG_WARN,ExpiredThrow,"Dangling pointer exists to continuation");
    fd_decref(continuation);
    return retval;}
  else {
    if (FD_CONS_REFCOUNT(f)>1) 
      u8_log(LOG_WARN,ExpiredThrow,"Dangling pointer exists to continuation");
    fd_decref(continuation);
    return value;}
}

/* Cache call */

static fdtype cachecall(int n,fdtype *args)
{
  if (FD_HASHTABLEP(args[0]))
    return fd_xcachecall((fd_hashtable)args[0],args[1],n-2,args+2);
  else return fd_cachecall(args[0],n-1,args+1);
}

static fdtype clear_callcache(fdtype arg)
{
  fd_clear_callcache(arg);
  return FD_VOID;
}

static fdtype tcachecall(int n,fdtype *args)
{
  return fd_tcachecall(args[0],n-1,args+1);
}

static fdtype with_threadcache_handler(fdtype expr,fd_lispenv env)
{
  struct FD_THREAD_CACHE *tc=fd_push_threadcache(NULL);
  fdtype value=FD_VOID;
  FD_DOLIST(each,FD_CDR(expr)) {
    fd_decref(value); value=FD_VOID; value=fd_eval(each,env);
    if (FD_ABORTP(value)) {
      fd_pop_threadcache(tc);
      return value;}}
  fd_pop_threadcache(tc);
  return value;
}


/* Making DTPROCs */

static fdtype make_dtproc(fdtype name,fdtype server,fdtype min_arity,fdtype arity,fdtype minsock,fdtype maxsock,fdtype initsock)
{
  fdtype result;
  if (FD_VOIDP(min_arity))
    result=fd_make_dtproc(FD_SYMBOL_NAME(name),FD_STRDATA(server),1,-1,-1,
			  FD_FIX2INT(minsock),FD_FIX2INT(maxsock),
			  FD_FIX2INT(initsock));
  else if (FD_VOIDP(arity))
    result=fd_make_dtproc
      (FD_SYMBOL_NAME(name),FD_STRDATA(server),
       1,fd_getint(arity),fd_getint(arity),
       FD_FIX2INT(minsock),FD_FIX2INT(maxsock),
       FD_FIX2INT(initsock));
  else result=
	 fd_make_dtproc
	 (FD_SYMBOL_NAME(name),FD_STRDATA(server),1,
	  fd_getint(arity),fd_getint(min_arity),
	  FD_FIX2INT(minsock),FD_FIX2INT(maxsock),
	  FD_FIX2INT(initsock));
  return result;
}

/* Remote evaluation */

static fdtype dteval(fdtype server,fdtype expr)
{
  if (FD_PRIM_TYPEP(server,fd_dtserver_type)) 
    return fd_dteval(FD_GET_CONS(server,fd_dtserver_type,fd_dtserver),expr);
  else if (FD_STRINGP(server)) {
    fdtype s=fd_open_dtserver(FD_STRDATA(server),-1);
    if (FD_ABORTP(s)) return s;
    else {
      fdtype result=fd_dteval((fd_dtserver)s,expr);
      fd_decref(s);
      return result;}}
  else return fd_type_error(_("server"),"dteval",server);
}

static fdtype dtcall(int n,fdtype *args)
{
  fdtype server; fdtype request=FD_EMPTY_LIST, result; int i=n-1;
  if (n<2) return fd_err(fd_SyntaxError,"dtcall",NULL,FD_VOID);
  if (FD_PRIM_TYPEP(args[0],fd_dtserver_type))
    server=fd_incref(args[0]);
  else if (FD_STRINGP(args[0])) server=fd_open_dtserver(FD_STRDATA(args[0]),-1);
  else return fd_type_error(_("server"),"eval/dtcall",args[0]);
  if (FD_ABORTP(server)) return server;
  while (i>=1) {
    fdtype param=args[i];
    if ((i>1) && ((FD_SYMBOLP(param)) || (FD_PAIRP(param))))
      request=fd_init_pair(NULL,fd_make_list(2,quote_symbol,param),request);
    else request=fd_init_pair(NULL,param,request);
    fd_incref(param); i--;}
  result=fd_dteval((fd_dtserver)server,request);
  fd_decref(request); fd_decref(server);
  return result;
}

static fdtype open_dtserver(fdtype server,fdtype bufsiz)
{
  return fd_open_dtserver(FD_STRDATA(server),((FD_VOIDP(bufsiz)) ? (-1) : (FD_FIX2INT(bufsiz))));
}

/* Test functions */

static fdtype applytest(int n,fdtype *args)
{
  if (n<2)
    return fd_err(fd_TooFewArgs,"applytest",NULL,FD_VOID);
  else if (FD_APPLICABLEP(args[1])) {
    fdtype value=fd_apply(args[1],n-2,args+2);
    if (FD_EQUAL(value,args[0])) {
      fd_decref(value);
      return FD_TRUE;}
    else {
      u8_string s=fd_dtype2string(args[0]);
      fdtype err=fd_err(TestFailed,"applytest",s,value);
      u8_free(s); fd_decref(value);
      return err;}}
  else if (FD_EQUAL(args[2],args[0]))
    return FD_TRUE;
  else {
    u8_string s=fd_dtype2string(args[0]);
    fdtype err=fd_err(TestFailed,"applytest",s,args[2]);
    u8_free(s);
    return err;}
}

static fdtype evaltest(fdtype expr,fd_lispenv env)
{
  fdtype testexpr=fd_get_arg(expr,2);
  fdtype expected=fd_eval(fd_get_arg(expr,1),env);
  if ((FD_VOIDP(testexpr)) || (FD_VOIDP(expected))) {
    fd_decref(expected);
    return fd_err(fd_SyntaxError,"evaltest",NULL,expr);}
  else {
    fdtype value=fd_eval(testexpr,env);
    if (FD_ABORTP(value)) {
      fd_decref(expected);
      return value;}
    else if (FD_EQUAL(value,expected)) {
      fd_decref(expected);
      fd_decref(value);
      return FD_TRUE;}
    else {
      u8_string s=fd_dtype2string(expected);
      fdtype err=fd_err(TestFailed,"applytest",s,value);
      u8_free(s); fd_decref(value); fd_decref(expected);
      return err;}}
}

/* Debugging assistance */

FD_EXPORT fdtype _fd_dbg(fdtype x)
{
  fdtype result=_fd_debug(x);
  return fd_incref(result);
}

static fdtype dbg_prim(fdtype x,fdtype msg)
{
  if (FD_VOIDP(msg))
    u8_message("Debug %q",x);
  else if (FD_FALSEP(msg)) {}
  else u8_message("Debug (%q) %q",msg,x);
  return _fd_dbg(x);
}

/* Initialization */

void fd_init_eval_c()
{
  struct FD_TABLEFNS *fns=u8_alloc(struct FD_TABLEFNS);
  fns->get=lispenv_get; fns->store=lispenv_store;
  fns->add=NULL; fns->drop=NULL; fns->test=NULL;
  
  fd_unparsers[fd_opcode_type]=unparse_opcode;
  fd_immediate_checkfns[fd_opcode_type]=validate_opcode;

  fd_environment_type=fd_register_cons_type(_("scheme environment"));
  fd_specform_type=fd_register_cons_type(_("scheme special form"));

  fd_tablefns[fd_environment_type]=fns;
  fd_copiers[fd_environment_type]=lisp_copy_environment;
  fd_recyclers[fd_environment_type]=recycle_environment;
  fd_recyclers[fd_specform_type]=recycle_specform;

  fd_unparsers[fd_environment_type]=unparse_environment;
  fd_unparsers[fd_specform_type]=unparse_specform;
  fd_unparsers[fd_lexref_type]=unparse_lexref;

  quote_symbol=fd_intern("QUOTE");
  _fd_comment_symbol=comment_symbol=fd_intern("COMMENT");
  profile_symbol=fd_intern("%PROFILE");

  fd_make_hashtable(&module_map,67);
  fd_make_hashtable(&safe_module_map,67);
}

static void init_scheme_module()
{
  fd_xscheme_module=fd_make_hashtable(NULL,71);
  fd_scheme_module=fd_make_hashtable(NULL,71);
  fd_register_module("SCHEME",fd_scheme_module,
		     (FD_MODULE_DEFAULT|FD_MODULE_SAFE));
  fd_register_module("XSCHEME",fd_xscheme_module,(FD_MODULE_DEFAULT));
}

static void init_localfns()
{
  fd_defspecial(fd_scheme_module,"EVAL",eval_handler);
  fd_defspecial(fd_scheme_module,"BOUND?",boundp_handler);
  fd_defspecial(fd_scheme_module,"VOID?",voidp_handler);
  fd_defspecial(fd_scheme_module,"QUOTE",quote_handler);
  fd_defspecial(fd_scheme_module,"%ENV",env_handler);
  fd_idefn(fd_scheme_module,fd_make_cprim1("ENVIRONMENT?",environmentp_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("SYMBOL-BOUND?",symbol_boundp_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("%LEXREF",lexref_prim,2,
					    fd_fixnum_type,FD_VOID,
					    fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim3("GET-ARG",get_arg_prim,2));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim3x("GETOPT",getopt_prim,2,
			   -1,FD_VOID,fd_symbol_type,FD_VOID,
			   -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim3x("TESTOPT",testopt_prim,2,
			   -1,FD_VOID,fd_symbol_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim3x("OPT+",optplus_prim,2,
			   -1,FD_VOID,fd_symbol_type,FD_VOID,
			   -1,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprimn("APPLY",apply_lexpr,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim7x
	   ("DTPROC",make_dtproc,2,
	    fd_symbol_type,FD_VOID,fd_string_type,FD_VOID,
	    -1,FD_VOID,-1,FD_VOID,
	    fd_fixnum_type,FD_INT2DTYPE(2),
	    fd_fixnum_type,FD_INT2DTYPE(4),
	    fd_fixnum_type,FD_INT2DTYPE(1)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("CALL/CC",callcc,1));
  fd_defalias(fd_scheme_module,"CALL-WITH-CURRENT-CONTINUATION","CALL/CC");

  fd_defspecial(fd_scheme_module,"WITH-THREADCACHE",with_threadcache_handler);
  fd_idefn(fd_scheme_module,fd_make_cprimn("TCACHECALL",tcachecall,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("CACHECALL",cachecall,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1("CLEAR-CALLCACHE!",clear_callcache,0));
  fd_defalias(fd_scheme_module,"CACHEPOINT","TCACHECALL");

  fd_defspecial(fd_scheme_module,"TIMEVAL",timed_eval);
  fd_defspecial(fd_scheme_module,"%TIMEVAL",timed_evalx);
  fd_defspecial(fd_scheme_module,"%WATCH",watched_eval);
  fd_defspecial(fd_scheme_module,"PROFILE",profiled_eval);

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("APPLYTEST",applytest,2)));
  fd_defspecial(fd_scheme_module,"EVALTEST",evaltest);

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim2("DBG",dbg_prim,1)));


  fd_idefn(fd_scheme_module,fd_make_cprim2("DTEVAL",dteval,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("DTCALL",dtcall,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("OPEN-DTSERVER",open_dtserver,1,
					    fd_string_type,FD_VOID,
					    fd_fixnum_type,FD_VOID));

  fd_register_config
    ("TAILCALL","Enable tail recursion in the Scheme evaluator",
     fd_boolconfig_get,fd_boolconfig_set,&fd_optimize_tail_calls);
}

FD_EXPORT void fd_init_errors_c(void);
FD_EXPORT void fd_init_compounds_c(void);
FD_EXPORT void fd_init_threadprims_c(void);
FD_EXPORT void fd_init_conditionals_c(void);
FD_EXPORT void fd_init_iterators_c(void);
FD_EXPORT void fd_init_choicefns_c(void);
FD_EXPORT void fd_init_binders_c(void);
FD_EXPORT void fd_init_corefns_c(void);
FD_EXPORT void fd_init_tablefns_c(void);
FD_EXPORT void fd_init_strings_c(void);
FD_EXPORT void fd_init_dbfns_c(void);
FD_EXPORT void fd_init_sequences_c(void);
FD_EXPORT void fd_init_modules_c(void);
FD_EXPORT void fd_init_load_c(void);
FD_EXPORT void fd_init_portfns_c(void);
FD_EXPORT void fd_init_timeprims_c(void);
FD_EXPORT void fd_init_numeric_c(void);
FD_EXPORT void fd_init_side_effects_c(void);
FD_EXPORT void fd_init_reflection_c(void);
FD_EXPORT void fd_init_history_c(void);
FD_EXPORT void fd_init_quasiquote_c(void);

static void init_core_builtins()
{
  init_localfns();
  fd_init_threadprims_c();
  fd_init_errors_c();
  fd_init_conditionals_c();
  fd_init_iterators_c();
  fd_init_choicefns_c();
  fd_init_binders_c();
  fd_init_corefns_c();
  fd_init_tablefns_c();
  fd_init_compounds_c();
  fd_init_strings_c();
  fd_init_dbfns_c();
  fd_init_sequences_c();
  fd_init_quasiquote_c();
  fd_init_modules_c();
  fd_init_load_c();
  fd_init_portfns_c();
  fd_init_timeprims_c();
  fd_init_numeric_c();
  fd_init_side_effects_c();
  fd_init_reflection_c();
  fd_init_history_c();
  fd_persist_module(fd_scheme_module);
  fd_persist_module(fd_xscheme_module);
}

FD_EXPORT int fd_init_fdscheme()
{
  if (fdscheme_initialized) return fdscheme_initialized;
  fdscheme_initialized=401*fd_init_db()*u8_initialize();

  fd_init_eval_c();

  default_env=fd_make_env(fd_make_hashtable(NULL,0),NULL);
  safe_default_env=fd_make_env(fd_make_hashtable(NULL,0),NULL);

  fd_register_source_file(FDB_EVAL_H_VERSION);
  fd_register_source_file(versionid);

  init_scheme_module();
  init_core_builtins();

  return fdscheme_initialized;
}
