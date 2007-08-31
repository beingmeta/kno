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
static u8_condition ThreadReturnError=_("Thread returned with error");

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
  double start=u8_elapsed_time();
  fdtype value=fd_eval(toeval,env);
  u8_log(-1,"%WATCH","<%fsec> %q => %q",u8_elapsed_time()-start,toeval,value);
  return value;
}

/* Opcode names */

const u8_string fd_opcode_names[256]={
  /* 0x00 */
  "quote","begin","and","or","not","fail",
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x10 */
  "if","when","unless","ifelse",
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x20 */
  "ambigp","singeltonp","failp","existsp",
  "singleton","car","cdr","length","qchoice","choicesize",
  NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x30 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x40 */
  "minus1","plus1","numberp","zerop",
  "vectorp","pairp","emptylistp","stringp",
  "oidp","symbolp",NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x50 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x60 */
  "numeq","numgt","numgte","numle","numlte",
  "plus","subtract","multiple","flodiv",
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x70 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x80 */
  "eq","eqv","equal","elt",NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0x90 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xA0 */
  "get","test",NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xB0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xC0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xD0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xE0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  /* 0xF0 */
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
  NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
};

int fd_opcode_table_len=256;

static int unparse_opcode(u8_output out,fdtype x)
{
  int opcode_offset=(FD_GET_IMMEDIATE(x,fd_opcode_type));
  if (opcode_offset>fd_opcode_table_len) {
    u8_printf(out,"##invalidop");
    return 1;}
  else if (fd_opcode_names[opcode_offset]==NULL) {
    u8_printf(out,"##unknownop");
    return 1;}
  else {
    u8_printf(out,"##op_%s",fd_opcode_names[opcode_offset]);
    return 1;}
}

static int validate_opcode(fdtype x)
{
  int opcode_offset=(FD_GET_IMMEDIATE(x,fd_opcode_type));
  if ((opcode_offset>=0) && (opcode_offset<fd_opcode_table_len) &&
      (fd_opcode_names[opcode_offset] != NULL))
    return 1;
  else return 0;
}

/* OPCODE dispatching */

static fdtype opcode_special_dispatch(fdtype opcode,fdtype expr,fd_lispenv env)
{
  fdtype body=FD_CDR(expr), remainder;
  if (opcode>=FD_IF_OPCODE) {
    /* It's a conditional opcode */
    fdtype test=FD_VOID;
    if (FD_EXPECT_FALSE(!(FD_PAIRP(body))))
      return fd_err(fd_SyntaxError,"OPCODE conditional",NULL,expr);
    test=fd_eval(FD_CAR(body),env); remainder=FD_CDR(body);
    if (FD_ABORTP(test)) return test;
    else if (FD_VOIDP(test))
      return fd_err(fd_VoidArgument,"conditional OPCODE",NULL,FD_CAR(body));
    else switch (opcode) {
    case FD_IF_OPCODE: {
      fdtype consequent_expr=FD_VOID;
      if (FD_EMPTY_CHOICEP(test))
	return FD_EMPTY_CHOICE;
      else if (FD_FALSEP(test)) {
	fdtype if_tail=FD_CDR(remainder);
	if (FD_PAIRP(if_tail))
	  consequent_expr=FD_CAR(if_tail);
	else consequent_expr=FD_VOID;}
      else consequent_expr=FD_CAR(remainder);
      fd_decref(test);
      if (FD_PAIRP(consequent_expr))
	return fd_tail_eval(consequent_expr,env);
      else return fasteval(consequent_expr,env);}
    case FD_WHEN_OPCODE: {
      if (!(FD_FALSEP(test))) {
	fdtype test_body=remainder;
	while (FD_PAIRP(test_body)) {
	  fdtype tmp=fd_eval(FD_CAR(test_body),env);
	  if (FD_ABORTP(tmp)) return tmp;
	  fd_decref(tmp);
	  test_body=FD_CDR(test_body);}
	return FD_VOID;}
      fd_decref(test);
      return FD_VOID;}
    case FD_UNLESS_OPCODE: {
      if (FD_FALSEP(test)) {
	fdtype test_body=remainder;
	while (FD_PAIRP(test_body)) {
	  fdtype tmp=fd_eval(FD_CAR(test_body),env);
	  if (FD_ABORTP(tmp)) return tmp;
	  fd_decref(tmp);
	  test_body=FD_CDR(test_body);}
	return FD_VOID;}
      fd_decref(test);
      return FD_VOID;}
    case FD_IFELSE_OPCODE: {
      if (FD_EMPTY_CHOICEP(test))
	return FD_EMPTY_CHOICE;
      else if (FD_FALSEP(test)) {
	fdtype if_tail=FD_CDR(remainder);
	/* Done with this: */
	fd_decref(test);
	/* Execute the else expressions */
	while (FD_PAIRP(if_tail)) {
	  fdtype else_expr=FD_CAR(if_tail);
	  if (FD_PAIRP(FD_CDR(if_tail))) {
	    fdtype result=fd_eval(else_expr,env);
	    fd_decref(result);
	    if_tail=FD_CDR(if_tail);}
	  else if (FD_PAIRP(else_expr))
	    return fd_tail_eval(else_expr,env);
	  else return fasteval(else_expr,env);}
	return FD_VOID;}
      else {
	fdtype consequent_expr=FD_CAR(remainder);
	fd_decref(test);
	if (FD_PAIRP(consequent_expr))
	  return fd_tail_eval(consequent_expr,env);
	else return fasteval(consequent_expr,env);}}
    default:
      return fd_err(_("Invalid opcode"),"opcode eval",NULL,body);
    }
  }
  else switch (opcode) {
  case FD_QUOTE_OPCODE: {
    if ((FD_PAIRP(body)) && (FD_EMPTY_LISTP(FD_CDR(body))))
      return fd_incref(FD_CAR(body));
    else return fd_err(fd_SyntaxError,"opcode QUOTE",NULL,expr);}
  case FD_BEGIN_OPCODE: {
    fdtype retval=FD_VOID;
    while (FD_PAIRP(body)) {
      fdtype each=FD_CAR(body); body=FD_CDR(body);
      fd_decref(retval);
      if (FD_PAIRP(body)) {
	retval=fd_eval(each,env);
	if (FD_ABORTP(retval)) return retval;}
      else if (FD_PAIRP(each))
	return fd_tail_eval(each,env);
      else return fasteval(each,env);}
    return retval;}
  case FD_AND_OPCODE: {
    fdtype retval=FD_TRUE;
    if (FD_EMPTY_LISTP(body)) return FD_TRUE;
    else if (!(FD_PAIRP(body)))
      return fd_err(fd_SyntaxError,"AND opcode",NULL,expr);
    else while (FD_PAIRP(body)) {
      fdtype each=FD_CAR(body); body=FD_CDR(body);
      fd_decref(retval);
      if (FD_PAIRP(body)) {
	retval=fd_eval(each,env);
	if (FD_ABORTP(retval)) return retval;
	else if (FD_VOIDP(retval))
	  return fd_err(fd_VoidArgument,"AND opcode",NULL,each);
	else if (FD_FALSEP(retval))
	  return FD_FALSE;}
      else if (FD_PAIRP(each))
	return fd_tail_eval(each,env);
      else return fasteval(each,env);}
    return retval;}
  case FD_OR_OPCODE: {
    fdtype retval=FD_FALSE;
    if (FD_EMPTY_LISTP(body)) return FD_FALSE;
    else if (!(FD_PAIRP(body)))
      return fd_err(fd_SyntaxError,"OR opcode",NULL,expr);
    else while (FD_PAIRP(body)) {
	fdtype each=FD_CAR(body); body=FD_CDR(body);
	fd_decref(retval);
	if (FD_PAIRP(body)) {
	  retval=fd_eval(each,env);
	  if (FD_ABORTP(retval)) return retval;
	  else if (FD_VOIDP(retval))
	    return fd_err(fd_VoidArgument,"OR opcode",NULL,each);
	  else if (!(FD_FALSEP(retval))) return retval;}
	else if (FD_PAIRP(each))
	  return fd_tail_eval(each,env);
	else return fasteval(each,env);}
    return FD_FALSE;}
  case FD_NOT_OPCODE: {
    if (!(FD_PAIRP(body)))
      return fd_err(fd_SyntaxError,"AND opcode",NULL,expr);
    else {
      fdtype arg1_expr=FD_CAR(body);
      fdtype arg1=fd_eval(arg1_expr,env);
      if (FD_VOIDP(arg1))
	return fd_err(fd_VoidArgument,"AND opcode",NULL,arg1_expr);
      else if (!(FD_FALSEP(arg1))) {
	fd_decref(arg1); return FD_FALSE;}
      else return FD_TRUE;}}
  case FD_FAIL_OPCODE: 
    if (FD_PAIRP(body))
      return fd_err(fd_SyntaxError,"AND opcode",NULL,expr);
    else return FD_EMPTY_CHOICE;
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,body);
  }
}

static fdtype opcode_unary_nd_dispatch(fdtype opcode,fdtype arg1)
{
  int delta=1;
  switch (opcode) {
  case FD_AMBIGP_OPCODE: 
    if (FD_CHOICEP(arg1)) return FD_TRUE;
    else return FD_FALSE;
  case FD_SINGLETONP_OPCODE: 
    if (FD_CHOICEP(arg1)) return FD_FALSE;
    else return FD_TRUE;
  case FD_FAILP_OPCODE: 
    if (arg1==FD_EMPTY_CHOICE) return FD_TRUE;
    else return FD_FALSE;
  case FD_EXISTSP_OPCODE: 
    if (arg1==FD_EMPTY_CHOICE) return FD_FALSE;
    else return FD_TRUE;
  case FD_SINGLETON_OPCODE:
    if (FD_CHOICEP(arg1)) return FD_EMPTY_CHOICE;
    else return fd_incref(arg1);
  case FD_CAR_OPCODE: 
    if (FD_EMPTY_CHOICEP(arg1)) return arg1;
    else if (FD_PAIRP(arg1))
      return fd_incref(FD_CAR(arg1));
    else if (FD_CHOICEP(arg1)) {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(arg,arg1)
	if (FD_PAIRP(arg)) {
	  fdtype car=FD_CAR(arg); fd_incref(car);
	  FD_ADD_TO_CHOICE(results,car);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("pair"),"CAR opcode",arg);}
      return results;}
    else return fd_type_error(_("pair"),"CAR opcode",arg1);
  case FD_CDR_OPCODE: 
    if (FD_EMPTY_CHOICEP(arg1)) return arg1;
    else if (FD_PAIRP(arg1))
      return fd_incref(FD_CDR(arg1));
    else if (FD_CHOICEP(arg1)) {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(arg,arg1)
	if (FD_PAIRP(arg)) {
	  fdtype cdr=FD_CDR(arg); fd_incref(cdr);
	  FD_ADD_TO_CHOICE(results,cdr);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("pair"),"CDR opcode",arg);}
      return results;}
    else return fd_type_error(_("pair"),"CDR opcode",arg1);
  case FD_LENGTH_OPCODE:
    if (arg1==FD_EMPTY_CHOICE) return FD_EMPTY_CHOICE;
    else if (FD_CHOICEP(arg1)) {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(arg,arg1) {
	if (FD_SEQUENCEP(arg)) {
	  int len=fd_seq_length(arg);
	  fdtype dlen=FD_INT2DTYPE(len);
	  FD_ADD_TO_CHOICE(results,dlen);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("sequence"),"LENGTH opcode",arg);}}
      return fd_simplify_choice(results);}
    else if (FD_SEQUENCEP(arg1))
      return FD_INT2DTYPE(fd_seq_length(arg1));
    else return fd_type_error(_("sequence"),"LENGTH opcode",arg1);
  case FD_QCHOICE_OPCODE:
    if (FD_CHOICEP(arg1)) {
      fd_incref(arg1);
      return fd_init_qchoice(NULL,arg1);}
    else if (FD_ACHOICEP(arg1)) 
      return fd_init_qchoice(NULL,fd_make_simple_choice(arg1));
    else if (FD_EMPTY_CHOICEP(arg1))
      return fd_init_qchoice(NULL,FD_EMPTY_CHOICE);
    else return fd_incref(arg1);
  case FD_CHOICE_SIZE_OPCODE:
    if (FD_CHOICEP(arg1)) {
      int sz=FD_CHOICE_SIZE(arg1);
      return FD_INT2DTYPE(sz);}
    else if (FD_ACHOICEP(arg1)) {
      fdtype simple=fd_make_simple_choice(arg1);
      int size=FD_CHOICE_SIZE(simple);
      fd_decref(simple);
      return FD_INT2DTYPE(size);}
    else if (FD_EMPTY_CHOICEP(arg1))
      return FD_INT2DTYPE(0);
    else return FD_INT2DTYPE(1);
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,FD_VOID);
  }
}

static fdtype opcode_unary_dispatch(fdtype opcode,fdtype arg1)
{
  int delta=1;
  switch (opcode) {
  case FD_MINUS1_OPCODE: delta=-1;
  case FD_PLUS1_OPCODE: 
    if (FD_FIXNUMP(arg1)) {
      int iarg=FD_FIX2INT(arg1);
      return FD_INT2DTYPE(iarg+delta);}
    else if (FD_NUMBERP(arg1))
      return fd_plus(arg1,FD_FIX2INT(-1));
    else return fd_type_error(_("number"),"opcode 1+/-",arg1);
  case FD_NUMBERP_OPCODE: 
    if (FD_NUMBERP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_ZEROP_OPCODE: 
    if (arg1==FD_INT2DTYPE(0)) return FD_TRUE; else return FD_FALSE;
  case FD_VECTORP_OPCODE: 
    if (FD_VECTORP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_PAIRP_OPCODE: 
    if (FD_PAIRP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_NULLP_OPCODE: 
    if (arg1==FD_EMPTY_LIST) return FD_TRUE; else return FD_FALSE;
  case FD_STRINGP_OPCODE: 
    if (FD_STRINGP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_OIDP_OPCODE: 
    if (FD_OIDP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_SYMBOLP_OPCODE: 
    if (FD_SYMBOLP(arg1)) return FD_TRUE; else return FD_FALSE;
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,FD_VOID);
  }
}

static fdtype opcode_binary_dispatch(fdtype opcode,fdtype arg1,fdtype arg2)
{
  switch (opcode) {
  case FD_NUMEQ_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))==(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)==0) return FD_TRUE;
    else return FD_FALSE;
  case FD_GT_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))>(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)>0) return FD_TRUE;
    else return FD_FALSE;
  case FD_GTE_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))>=(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)>=0) return FD_TRUE;
    else return FD_FALSE;
  case FD_LT_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))<(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)<0) return FD_TRUE;
    else return FD_FALSE;
  case FD_LTE_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))<=(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)<=0) return FD_TRUE;
    else return FD_FALSE;
  case FD_EQ_OPCODE: {
    if (arg1==arg2) return FD_TRUE; else return FD_FALSE;
    break;}
  case FD_EQV_OPCODE: {
    if (arg1==arg2) return FD_TRUE;
    else if ((FD_NUMBERP(arg1)) && (FD_NUMBERP(arg2)))
      if (fd_numcompare(arg1,arg2)==0)
	return FD_TRUE; else return FD_FALSE;
    else return FD_FALSE;
    break;}
  case FD_EQUAL_OPCODE: {
    if ((FD_ATOMICP(arg1)) && (FD_ATOMICP(arg2)))
      if (arg1==arg2) return FD_TRUE; else return FD_FALSE;
    else if (FD_EQUAL(arg1,arg2)) return FD_TRUE;
    else return FD_FALSE;
    break;}
  case FD_PLUS_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      return FD_INT2DTYPE(m+n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x+y);}
    else return fd_plus(arg1,arg2);
  case FD_MINUS_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      return FD_INT2DTYPE(m-n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x-y);}
    else return fd_subtract(arg1,arg2);
  case FD_TIMES_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      return FD_INT2DTYPE(m*n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x*y);}
    else return fd_multiply(arg1,arg2);
  case FD_FLODIV_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      int m=FD_FIX2INT(arg1), n=FD_FIX2INT(arg2);
      double x=(double)m, y=(double)n;
      return fd_init_double(NULL,x/y);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x=FD_FLONUM(arg1), y=FD_FLONUM(arg2);
      return fd_init_double(NULL,x/y);}
    else {
      double x=fd_todouble(arg1), y=fd_todouble(arg2);
      return fd_init_double(NULL,x/y);}
  case FD_ELT_OPCODE:
    if (FD_EMPTY_CHOICEP(arg1)) {
      fd_decref(arg2); return arg1;}
    else if ((FD_SEQUENCEP(arg1)) && (FD_FIXNUMP(arg2))) {
      fdtype result;
      int off=FD_FIX2INT(arg2), len=fd_seq_length(arg1);
      if (off<0) off=len+off;
      result=fd_seq_elt(arg1,off);
      if (result == FD_TYPE_ERROR)
	return fd_type_error(_("sequence"),"seqelt_prim",arg1);
      else if (result == FD_RANGE_ERROR) {
	char buf[32];
	sprintf(buf,"%d",off);
	return fd_err(fd_RangeError,"seqelt_prim",u8_strdup(buf),arg1);}
      else return result;}
    else if (!(FD_SEQUENCEP(arg1)))
      return fd_type_error(_("sequence"),"opcode ELT",arg1);
    else return fd_type_error(_("fixnum"),"opcode ELT",arg2);
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,FD_VOID);
  }
}

static fdtype opcode_other_dispatch
  (fdtype opcode,fdtype arg1,fdtype body,fd_lispenv env)
{
  /* Wait with implementing these */
  return FD_VOID;
}

static int numeric_argp(fdtype x)
{
  /* This checks if there is a type error.
     The empty choice isn't a type error since it will just
     generate an empty choice as a result. */
  if (FD_EMPTY_CHOICEP(x)) return 1;
  else if (FD_EXPECT_TRUE(FD_NUMBERP(x))) return 1;
  else if ((FD_ACHOICEP(x))||(FD_CHOICEP(x))) {
    FD_DO_CHOICES(a,x)
      if (FD_EXPECT_TRUE(FD_NUMBERP(a))) {}
      else {
	FD_STOP_DO_CHOICES;
	return 0;}
    return 1;}
  else return 0;
}

static fdtype opcode_binary_nd_dispatch(fdtype opcode,fdtype arg1,fdtype arg2)
{
  
  if (FD_EXPECT_FALSE((opcode<FD_EQ_OPCODE) && (!(numeric_argp(arg2))))) {
    fd_decref(arg1);
    return fd_type_error(_("number"),"numeric opcode",arg2);}
  else {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(a1,arg1) {
      {FD_DO_CHOICES(a2,arg2) {
	  fdtype result=opcode_binary_dispatch(opcode,a1,a2);
	  /* If we need to abort due to an error, we need to pop out of
	     two choice loops.  So on the inside, we decref results and
	     replace it with the error object.  We then break and
	     do FD_STOP_DO_CHOICES (for potential cleanup). */
	  if (FD_ABORTP(result)) {
	    fd_decref(results); results=result;
	    FD_STOP_DO_CHOICES; break;}}}
      /* If the inner loop aborted due to an error, results is now bound
	 to the error, so we just FD_STOP_DO_CHOICES (this time for the
	 outer loop) and break; */
      if (FD_ABORTP(results)) {
	FD_STOP_DO_CHOICES; break;}}
    fd_decref(arg1); fd_decref(arg2);
    return results;}
}

static fdtype opcode_dispatch(fdtype opcode,fdtype expr,fd_lispenv env)
{
  fdtype body=FD_CDR(expr), arg1_expr, arg1;
  if (opcode<FD_UNARY_ND_OPCODES)
    /* These handle the raw expression without evaluation */
    return opcode_special_dispatch(opcode,expr,env);
  else if (FD_EXPECT_FALSE(!(FD_PAIRP(body))))
    /* Otherwise, we should have at least one argument,
       return an error otherwise. */
    return fd_err(fd_SyntaxError,"opcode eval",NULL,expr);
  /* We have at least one argument to evaluate and we also
     "advance" the body. */
  arg1_expr=FD_CAR(body);
  arg1=fd_eval(arg1_expr,env);
  body=FD_CDR(body);
  /* Now, check the result of the first argument expression */
  if (FD_ABORTP(arg1)) return arg1;
  else if (FD_VOIDP(arg1))
    return fd_err(fd_VoidArgument,"opcode eval",NULL,arg1_expr);
  /* Check the type for numeric arguments here. */
  else if (FD_EXPECT_FALSE
	   (((opcode>=FD_NUMERIC2_OPCODES) && (opcode<FD_BINARY_OPCODES)) &&
	    (!(numeric_argp(arg1))))) {
    fdtype result=fd_type_error(_("number"),"numeric opcode",arg1);
    fd_decref(arg1);
    return result;}
  else if (FD_EXPECT_FALSE
	   (((opcode>=FD_NUMERIC2_OPCODES) && (opcode<FD_BINARY_OPCODES)) &&
	    (FD_EMPTY_CHOICEP(arg1))))
    return arg1;
  else if (FD_EMPTY_LISTP(body)) /* Unary call */
    /* Now we know that there is only one argument, which means you're either
       dispatching without doing ND iteration or with doing it.  */
    if (opcode<FD_UNARY_OPCODES)
      /* This means you don't have to bother iterating over choices. */
      if (FD_EXPECT_FALSE(FD_ACHOICEP(arg1))) {
	/* We handle choice normalization here so that the
	   inner code can be faster. */
	fdtype simplified=fd_make_simple_choice(arg1);
	fdtype result=opcode_unary_nd_dispatch(opcode,simplified);
	fd_decref(arg1); fd_decref(simplified);
	return result;}
      else {
	fdtype result=opcode_unary_nd_dispatch(opcode,arg1);
	fd_decref(arg1);
	return result;}
    else if (opcode<FD_NUMERIC2_OPCODES)
      /* Otherwise, we iterate over the argument */
      if (FD_EMPTY_CHOICEP(arg1)) return arg1;
      else if (FD_EXPECT_FALSE((FD_CHOICEP(arg1)) || (FD_ACHOICEP(arg1)))) {
	fdtype results=FD_EMPTY_CHOICE;
	FD_DO_CHOICES(arg,arg1) {
	  fdtype result=opcode_unary_dispatch(opcode,arg);
	  if (FD_ABORTP(result)) {
	    fd_decref(results); fd_decref(arg1);
	    FD_STOP_DO_CHOICES;
	    return result;}
	  else {FD_ADD_TO_CHOICE(results,result);}}
	fd_decref(arg1);
	return results;}
      else {
	/* Or just dispatch directly if we have a singleton */
	fdtype result=opcode_unary_dispatch(opcode,arg1);
	fd_decref(arg1);
	return result;}
    else { /* At this point, we must not have enough arguments, since
	      the opcode is past the unary operation range. */
      fd_decref(arg1);
      return fd_err(fd_TooFewArgs,"opcode eval",NULL,expr);}
  /* If we get here, we have additional arguments. */
  else if (FD_EXPECT_FALSE(opcode<FD_NUMERIC2_OPCODES)) {
    /* All unary opcodes live beneath FD_NUMERIC2_OPCODES.
       We should probably catch this earlier. */
    fd_decref(arg1);
    return fd_err(fd_TooManyArgs,"opcode eval",NULL,expr);}
  else if ((opcode<FD_NARY_OPCODES) && (FD_EMPTY_CHOICEP(arg1)))
    /* Prune calls */
    return arg1;
  else if (opcode<FD_NARY_OPCODES) /* Binary opcodes all live beneath FD_NARY_OPCODES */ { 
    /* Binary calls start by evaluating the second argument */
    fdtype arg2=fd_eval(FD_CAR(body),env), result;
    if (FD_ABORTP(arg2)) {
      fd_decref(arg1); return arg2;}
    else if (FD_VOIDP(arg2)) {
      fd_decref(arg1);
      return fd_err(fd_VoidArgument,"opcode eval",NULL,FD_CAR(body));}
    else if (FD_EMPTY_CHOICEP(arg2)) {
      /* Prune the call */
      fd_decref(arg1); return arg2;}
    else if ((FD_CHOICEP(arg1)) || (FD_ACHOICEP(arg1)) ||
	     (FD_CHOICEP(arg2)) || (FD_ACHOICEP(arg2))) 
      /* opcode_binary_nd_dispatch handles decref of arg1 and arg2 */
      return opcode_binary_nd_dispatch(opcode,arg1,arg2);
    else {
      /* This is the dispatch case where we just go to dispatch. */
      fdtype result;
      if (opcode<FD_EQ_OPCODE) /* Numeric operation */
	if (FD_EXPECT_FALSE(!(FD_NUMBERP(arg2))))
	  result=fd_type_error(_("number"),"numeric opcode",arg2);
	else result=opcode_binary_dispatch(opcode,arg1,arg2);
      else result=opcode_binary_dispatch(opcode,arg1,arg2);
      fd_decref(arg1); fd_decref(arg2);
      return result;}}
  else {
    fd_decref(arg1);
    return fd_err(fd_SyntaxError,"opcode eval",NULL,expr);}
}

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
	headval=fd_pptr_ref(headval);
	ctype=FD_PTR_TYPE(headval);
	gc=0;}
      if (fd_applyfns[ctype]) 
	result=apply_function(headval,expr,env);
      else if (FD_PTR_TYPEP(headval,fd_specform_type)) {
	/* These are special forms which do all the evaluating themselves */
	struct FD_SPECIAL_FORM *handler=
	  FD_GET_CONS(headval,fd_specform_type,struct FD_SPECIAL_FORM *);
	/* fd_calltrack_call(handler->name); */
	result=handler->eval(expr,env);
	/* fd_calltrack_return(handler->name); */
      }
      else if (FD_PTR_TYPEP(headval,fd_macro_type)) {
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
  fdtype result=FD_VOID, body=FD_CDR(expr);
  struct FD_FUNCTION *fcn=(struct FD_FUNCTION *)fn;
  fdtype argv[FD_STACK_ARGS], *args;
  int arg_count=0, n_args=0, args_need_gc=0, nd_args=0, prune=0;
  int max_arity=fcn->arity, min_arity=fcn->min_arity, args_length=max_arity;
  /* First, count the arguments */
  FD_DOLIST(elt,body) n_args++;
  /* Then, catch the obvious too many args case.  Note that if
     max_arity is negative, the procedure takes any number of
     arguments.  */
  if (max_arity<0) args_length=n_args;
  else if (n_args>max_arity)
    return fd_err(fd_TooManyArgs,"apply_function",fcn->name,expr);
  else if ((min_arity>=0) && (n_args<min_arity))
    return fd_err(fd_TooFewArgs,"apply_function",fcn->name,expr);
  if (args_length>FD_STACK_ARGS)
    /* If there are more than _FD_STACK_ARGS, malloc a vector for them. */
    args=u8_alloc_n(args_length,fdtype);
  /* Otherwise, just use the stack vector */
  else args=argv;
  /* Now we evaluate each of the subexpressions to fill the arg vector */
  {FD_DOLIST(elt,body) {
      fdtype argval;
      if ((FD_PAIRP(elt)) && (FD_EQ(FD_CAR(elt),comment_symbol))) {
	args_length--; continue;}
      argval=fasteval(elt,env);
      if (FD_ABORTP(argval)) {
	/* Break if one of the arguments returns an error */
	result=argval; break;}
      if (FD_VOIDP(argval)) {
	result=fd_err(fd_VoidArgument,"apply_function",fcn->name,elt);
	break;}
      if ((FD_EMPTY_CHOICEP(argval)) && (fcn->ndprim==0)) {
	/* Break if the method is non deterministic and one of the
	   arguments returns an empty choice. */
	prune=1; break;}
      /* Convert the argument to a simple choice (or non choice) */
      argval=fd_simplify_choice(argval);
      if (FD_CONSP(argval)) args_need_gc=1;
      /* Keep track of what kind of evaluation you might need to do. */
      if (FD_CHOICEP(argval)) nd_args=1;
      else if (FD_QCHOICEP(argval)) nd_args=1;
      /* Fill in the slot */
      args[arg_count++]=argval;}}
  if ((prune) || (FD_ABORTP(result))) {
    /* In this case, we won't call the procedure at all, because
       either a parameter produced an error or because we can prune
       the call because some parameter is an empty choice. */
    int i=0;
    if (args_need_gc) while (i<arg_count) {
      /* Clean up the arguments we've already evaluated */
      fdtype arg=args[i++]; fd_decref(arg);}
    if (args_length>FD_STACK_ARGS) u8_free(args);
    if (FD_ABORTP(result))
      /* This could extend the backtrace */
      return result;
    else {if (prune) return FD_EMPTY_CHOICE;}}
  if (arg_count != args_length) {
    if (fcn->defaults) 
      while (arg_count<args_length) {
	args[arg_count]=fd_incref(fcn->defaults[arg_count]); arg_count++;}
    else while (arg_count<args_length) args[arg_count++]=FD_VOID;}
  else {}
  if ((fd_optimize_tail_calls) && (FD_PTR_TYPEP(fn,fd_sproc_type)))
    result=fd_tail_call(fn,n_args,args);
  else if ((fcn->ndprim==0) && (nd_args))
    result=fd_ndapply(fn,n_args,args);
  else {
    result=fd_dapply(fn,n_args,args);}
  if ((FD_ABORTP(result)) &&
      (!(FD_THROWP(result))) &&
      (!(FD_PTR_TYPEP(fn,fd_sproc_type)))) {
    /* If it's not an sproc, we add an entry to the backtrace
       that shows the arguments, since they probably don't show
       up in an environment on the backtrace. */
    fdtype *avec=u8_alloc_n((arg_count+1),fdtype);
    memcpy(avec+1,args,sizeof(fdtype)*(arg_count));
    if (fcn->filename)
      if (fcn->name)
	avec[0]=fd_init_pair(NULL,fd_intern(fcn->name),fdtype_string(fcn->filename));
      else avec[0]=fd_init_pair(NULL,fd_intern("LAMBDA"),fdtype_string(fcn->filename));
    else if (fcn->name) avec[0]=fd_intern(fcn->name);
    else avec[0]=fd_intern("LAMBDA");
    if (args!=argv) u8_free(args);
    fd_push_error_context(fd_apply_context,fd_init_vector(NULL,arg_count+1,avec));
    return result;}
  else if (args_need_gc) {
    int i=0; while (i < arg_count) {
      fdtype arg=args[i++]; fd_decref(arg);}}
  if (args!=argv) u8_free(args);
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

/* Thread functions */

#if FD_THREADS_ENABLED

int fd_threaderror_backtrace=1;

fd_ptr_type fd_thread_type;
fd_ptr_type fd_condvar_type;

static void *thread_call(void *data)
{
  fdtype result;
  struct FD_THREAD_STRUCT *tstruct=(struct FD_THREAD_STRUCT *)data;
  if (tstruct->flags&FD_EVAL_THREAD) 
    result=fd_eval(tstruct->evaldata.expr,tstruct->evaldata.env);
  else 
    result=fd_dapply(tstruct->applydata.fn,
		     tstruct->applydata.n_args,
		     tstruct->applydata.args);
  result=fd_finish_call(result);
  if (FD_ABORTP(result)) {
    u8_exception ex=u8_erreify(), root=ex;
    u8_string errstring=fd_errstring(ex);
    if (tstruct->flags&FD_EVAL_THREAD)
      u8_log(LOG_WARN,ThreadReturnError,
	     "Thread evaluating %q encountered an error: %s",
	     tstruct->evaldata.expr,errstring);
    else u8_log(LOG_WARN,ThreadReturnError,"Thread apply %q returned %q",
		tstruct->applydata.fn,errstring);
    u8_free(errstring);
    if (tstruct->resultptr)
      *(tstruct->resultptr)=fd_init_exception(NULL,ex);
    else u8_free_exception(ex,1);}
  else if (tstruct->resultptr) *(tstruct->resultptr)=result;
  else fd_decref(result);
  tstruct->flags=tstruct->flags|FD_THREAD_DONE;
  fd_decref((fdtype)tstruct);
  return NULL;
}

static int unparse_thread_struct(u8_output out,fdtype x)
{
  struct FD_THREAD_STRUCT *th=
    FD_GET_CONS(x,fd_thread_type,struct FD_THREAD_STRUCT *);
  if (th->flags&FD_EVAL_THREAD)
    u8_printf(out,"#<THREAD 0x%x%s eval %q>",
	      (unsigned long)(th->tid),
	      ((th->flags&FD_THREAD_DONE) ? (" done") : ("")),
	      th->evaldata.expr);
  else u8_printf(out,"#<THREAD 0x%x%s apply %q>",
		 (unsigned long)(th->tid),
		 ((th->flags&FD_THREAD_DONE) ? (" done") : ("")),
		 th->applydata.fn);
  return 1;
}

FD_EXPORT void recycle_thread_struct(struct FD_CONS *c)
{
  struct FD_THREAD_STRUCT *th=(struct FD_THREAD_STRUCT *)c;
  if (th->flags&FD_EVAL_THREAD) {
    fd_decref(th->evaldata.expr);
    fd_decref((fdtype)(th->evaldata.env));}
  else {
    int i=0, n=th->applydata.n_args; fdtype *args=th->applydata.args;
    while (i<n) {fd_decref(args[i]); i++;}
    u8_free(args); fd_decref(th->applydata.fn);}
  u8_free(th);
}

/* CONDVAR support */

static fdtype make_condvar()
{
  struct FD_CONSED_CONDVAR *cv=u8_alloc(struct FD_CONSED_CONDVAR);
  FD_INIT_CONS(cv,fd_condvar_type);
  fd_init_mutex(&(cv->lock)); u8_init_condvar(&(cv->cvar));
  return FDTYPE_CONS(cv);
}

/* This primitive combine cond_wait and cond_timedwait
   through a second (optional) argument, which is an
   interval in seconds. */
static fdtype condvar_wait(fdtype x,fdtype timeout)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  if (FD_VOIDP(timeout))
    if (fd_condvar_wait(&(cv->cvar),&(cv->lock))==0)
      return FD_TRUE;
    else {
      return fd_type_error(_("valid condvar"),"condvar_wait",x);}
  else {
    struct timespec tm; int retval;
    if ((FD_FIXNUMP(timeout)) && (FD_FIX2INT(timeout)>=0)) {
      int ival=FD_FIX2INT(timeout);
      tm.tv_sec=time(NULL)+ival; tm.tv_nsec=0;}
#if 0 /* Define this later.  This allows sub-second waits but
	 is a little bit tricky. */
    else if (FD_FLONUMP(timeout)) {
      double flo=FD_FLONUM(x);
      if (flo>=0) {
	int secs=floor(flo)/1000000;
	int nsecs=(flo-(secs*1000000))*1000.0;
	tm.tv_sec=secs; tm.tv_nsec=nsecs;}
      else return fd_type_error(_("time interval"),"condvar_wait",x);}
#endif
    else return fd_type_error(_("time interval"),"condvar_wait",x);
    retval=u8_condvar_timedwait(&(cv->cvar),&(cv->lock),&tm);
    if (retval==0)
      return FD_TRUE;
    else if (retval==ETIMEDOUT)
      return FD_FALSE;
    return fd_type_error(_("valid condvar"),"condvar_wait",x);}
}

/* This primitive combines signals and broadcasts through
   a second (optional) argument which, when true, implies
   a broadcast. */
static fdtype condvar_signal(fdtype x,fdtype broadcast)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  if (FD_TRUEP(broadcast)) 
    if (u8_condvar_broadcast(&(cv->cvar))==0)
      return FD_TRUE;
    else return fd_type_error(_("valid condvar"),"condvar_signal",x);
  else if (u8_condvar_signal(&(cv->cvar))==0)
    return FD_TRUE;
  else return fd_type_error(_("valid condvar"),"condvar_signal",x);
}

static fdtype condvar_lock(fdtype x)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  fd_lock_struct(cv);
  return FD_TRUE;
}

static fdtype condvar_unlock(fdtype x)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  fd_unlock_struct(cv);
  return FD_TRUE;
}

static int unparse_condvar(u8_output out,fdtype x)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  u8_printf(out,"#<CONDVAR %lx>",cv);
  return 1;
}

FD_EXPORT void recycle_condvar(struct FD_CONS *c)
{
  struct FD_CONSED_CONDVAR *cv=
    (struct FD_CONSED_CONDVAR *)c;
  fd_destroy_mutex(&(cv->lock));  u8_destroy_condvar(&(cv->cvar));
  u8_free(cv);
}

/* These functions generically access the locks on CONDVARs
   and SPROCs */

static fdtype synchro_lock(fdtype x)
{
  if (FD_PTR_TYPEP(x,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
    fd_lock_struct(cv);
    return FD_TRUE;}
  else if (FD_PTR_TYPEP(x,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(x,fd_sproc_type,struct FD_SPROC *);
    if (sp->synchronized) {
      fd_lock_struct(sp);}
    else return fd_type_error("lockable","synchro_lock",x);
    return FD_TRUE;}
  else return fd_type_error("lockable","synchro_lock",x);
}

static fdtype synchro_unlock(fdtype x)
{
  if (FD_PTR_TYPEP(x,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
    fd_unlock_struct(cv);
    return FD_TRUE;}
  else if (FD_PTR_TYPEP(x,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(x,fd_sproc_type,struct FD_SPROC *);
    if (sp->synchronized) {
      fd_unlock_struct(sp);}
    else return fd_type_error("lockable","synchro_lock",x);
    return FD_TRUE;}
  else return fd_type_error("lockable","synchro_unlock",x);
}

/* Functions */

FD_EXPORT
fd_thread_struct fd_thread_call
  (fdtype *resultptr,fdtype fn,int n,fdtype *rail)
{
  struct FD_THREAD_STRUCT *tstruct=u8_alloc(struct FD_THREAD_STRUCT);
  FD_INIT_CONS(tstruct,fd_thread_type);
  if (resultptr) {
    tstruct->resultptr=resultptr; tstruct->result=FD_NULL;}
  else {
    tstruct->result=FD_NULL; tstruct->resultptr=&(tstruct->result);}
  tstruct->flags=0;
  tstruct->applydata.fn=fd_incref(fn);
  tstruct->applydata.n_args=n; tstruct->applydata.args=rail; 
  /* We need to do this first, before the thread exits and recycles itself! */
  fd_incref((fdtype)tstruct);
  pthread_create(&(tstruct->tid),pthread_attr_default,
		 thread_call,(void *)tstruct);
  return tstruct;
}

FD_EXPORT
fd_thread_struct fd_thread_eval(fdtype *resultptr,fdtype expr,fd_lispenv env)
{
  struct FD_THREAD_STRUCT *tstruct=u8_alloc(struct FD_THREAD_STRUCT);
  FD_INIT_CONS(tstruct,fd_thread_type);
  if (resultptr) {
    tstruct->resultptr=resultptr; tstruct->result=FD_NULL;}
  else {
    tstruct->result=FD_NULL; tstruct->resultptr=&(tstruct->result);}
  tstruct->flags=FD_EVAL_THREAD;
  tstruct->evaldata.expr=fd_incref(expr);
  tstruct->evaldata.env=fd_copy_env(env);
  /* We need to do this first, before the thread exits and recycles itself! */
  fd_incref((fdtype)tstruct);
  pthread_create(&(tstruct->tid),pthread_attr_default,
		 thread_call,(void *)tstruct);
  return tstruct;
}

/* Scheme primitives */

static fdtype threadcall_prim(int n,fdtype *args)
{
  fdtype *call_args=u8_alloc_n((n-1),fdtype), thread;
  int i=1; while (i<n) {
    call_args[i-1]=fd_incref(args[i]); i++;}
  thread=(fdtype)fd_thread_call(NULL,args[0],n-1,call_args);
  return thread;
}

static fdtype threadeval_handler(fdtype expr,fd_lispenv env)
{
  fdtype to_eval=fd_get_arg(expr,1);
  if (FD_VOIDP(to_eval))
    return fd_err(fd_SyntaxError,"threadeval_handler",NULL,expr);
  else if (FD_CHOICEP(to_eval)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(thread_expr,to_eval) {
      fdtype thread=(fdtype)fd_thread_eval(NULL,thread_expr,env);
      FD_ADD_TO_CHOICE(results,thread);}
    return results;}
  else return (fdtype)fd_thread_eval(NULL,to_eval,env);
}

static fdtype threadjoin_prim(fdtype threads)
{
  fdtype results=FD_EMPTY_CHOICE;
  {FD_DO_CHOICES(thread,threads)
     if (!(FD_PTR_TYPEP(thread,fd_thread_type)))
       return fd_type_error(_("thread"),"threadjoin_prim",thread);}
  {FD_DO_CHOICES(thread,threads) {
    struct FD_THREAD_STRUCT *tstruct=(fd_thread_struct)thread;
    int retval=pthread_join(tstruct->tid,NULL);
    if (retval==0) {
      if ((tstruct->resultptr)==&(tstruct->result))
	if (!(FD_VOIDP(tstruct->result))) {
	  FD_ADD_TO_CHOICE(results,fd_incref(tstruct->result));}}
    else u8_log(LOG_WARN,ThreadReturnError,"Bad return code %d (%s) from %q",
		 retval,strerror(retval),thread);}}
  return results;
}

static fdtype parallel_handler(fdtype expr,fd_lispenv env)
{
  fd_thread_struct _threads[6], *threads;
  fdtype _results[6], *results, scan=FD_CDR(expr), result=FD_EMPTY_CHOICE;
  int i=0, n_exprs=0;
  /* Compute number of expressions */
  while (FD_PAIRP(scan)) {n_exprs++; scan=FD_CDR(scan);}
  /* Malloc two vectors if neccessary. */
  if (n_exprs>6) {
    results=u8_alloc_n(n_exprs,fdtype);
    threads=u8_alloc_n(n_exprs,fd_thread_struct);}
  else {results=_results; threads=_threads;}
  /* Start up the threads and store the pointers. */
  scan=FD_CDR(expr); while (FD_PAIRP(scan)) {
    threads[i]=fd_thread_eval(&results[i],FD_CAR(scan),env);
    scan=FD_CDR(scan); i++;}
  /* Now wait for them to finish, accumulating values. */
  i=0; while (i<n_exprs) {
    pthread_join(threads[i]->tid,NULL);
    /* If any threads return errors, return the errors, combining
       them as contexts if multiple. */
    FD_ADD_TO_CHOICE(result,results[i]);
    fd_decref((fdtype)(threads[i]));
    i++;}
  if (n_exprs>6) {
    u8_free(threads); u8_free(results);}
  return result;
}

#endif

#if FD_THREADS_ENABLED
static void init_threadfns()
{
  fd_thread_type=fd_register_cons_type(_("thread"));
  fd_recyclers[fd_thread_type]=recycle_thread_struct;
  fd_unparsers[fd_thread_type]=unparse_thread_struct;

  fd_condvar_type=fd_register_cons_type(_("condvar"));
  fd_recyclers[fd_condvar_type]=recycle_condvar;
  fd_unparsers[fd_condvar_type]=unparse_condvar;

  fd_defspecial(fd_scheme_module,"PARALLEL",parallel_handler);
  fd_defspecial(fd_scheme_module,"SPAWN",threadeval_handler);
  fd_idefn(fd_scheme_module,fd_make_cprimn("THREADCALL",threadcall_prim,1));
  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprim1("THREADJOIN",threadjoin_prim,1)));

  fd_idefn(fd_scheme_module,fd_make_cprim0("MAKE-CONDVAR",make_condvar,0));
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONDVAR-WAIT",condvar_wait,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONDVAR-SIGNAL",condvar_signal,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CONDVAR-LOCK",condvar_lock,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CONDVAR-UNLOCK",condvar_unlock,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SYNCHRO-LOCK",synchro_lock,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SYNCHRO-UNLOCK",synchro_unlock,1));

}
#else
static void init_threadfns()
{
}
#endif

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

/* QUASIQUOTE */

static fdtype quasiquote, unquote, unquotestar;
FD_EXPORT fdtype fd_quasiquote(fdtype obj,fd_lispenv env,int level);

#define FD_BAD_UNQUOTEP(elt) \
  (((FD_EQ(FD_CAR(elt),unquote)) || \
    (FD_EQ(FD_CAR(elt),unquotestar))) && \
   (!((FD_PAIRP(FD_CDR(elt))) &&                 \
      (FD_EMPTY_LISTP(FD_CDR(FD_CDR(elt)))))))

static fdtype quasiquote_list(fdtype obj,fd_lispenv env,int level)
{
  fdtype head=FD_EMPTY_LIST, *tail=&head, scan=obj;
  while (FD_PAIRP(obj)) {
    fdtype elt=FD_CAR(obj), new_elt, new_tail;
    struct FD_PAIR *tailcons;
    if (FD_ATOMICP(elt))
      /* This handles the case of a dotted unquote. */
      if (FD_EQ(elt,unquote)) {
	if ((FD_PAIRP(FD_CDR(obj))) &&
	    (FD_EMPTY_LISTP(FD_CDR(FD_CDR(obj)))))
	  if (level==1) {
	    fdtype splice_at_end=fd_eval(FD_CADR(obj),env);
	    if (FD_ABORTP(splice_at_end)) {
	      fd_decref(head);
	      return splice_at_end;}
	    else {
	      *tail=splice_at_end;
	      return head;}}
	  else {
	    fdtype splice_at_end=fd_quasiquote(FD_CADR(obj),env,level-1);
	    fdtype with_unquote=fd_init_pair(NULL,unquote,splice_at_end);
	    *tail=with_unquote;
	    return head;}
	else {
	  fd_decref(head);
	  return fd_err(fd_SyntaxError,"malformed UNQUOTE",NULL,obj);}}
      else new_elt=elt;
    else if (FD_PAIRP(elt))
      if (FD_BAD_UNQUOTEP(elt)) {
	fd_decref(head);
	return fd_err(fd_SyntaxError,"malformed UNQUOTE",NULL,elt);}
      else if (FD_EQ(FD_CAR(elt),unquote))
	if (level==1) 
	  new_elt=fd_eval(FD_CADR(elt),env);
	else new_elt=
	       fd_make_list(2,unquote,fd_quasiquote(FD_CADR(elt),env,level-1));
      else if (FD_EQ(FD_CAR(elt),unquotestar))
	if (level==1) {
	  fdtype insertion=fd_eval(FD_CADR(elt),env);
	  if (FD_ABORTP(insertion)) {
	      fd_decref(head);
	      return insertion;}
	  else if (FD_EMPTY_LISTP(insertion)) {}
	  else if (FD_PAIRP(insertion)) {
	    fdtype last=scan=insertion;
	    while (FD_PAIRP(scan)) {last=scan; scan=FD_CDR(scan);}
	    if (!(FD_PAIRP(last))) {
	      u8_string details_string=u8_mkstring("RESULT=%q",elt);
	      fdtype err; 
	      err=fd_err(fd_SyntaxError,
			 "splicing UNQUOTE for an improper list",
			 details_string,insertion);
	      fd_decref(head); u8_free(details_string);
	      return err;}
	    else {*tail=insertion; tail=&(FD_CDR(last));}}
	  else {
	    u8_string details_string=u8_mkstring("RESULT=%q",elt);
	    fdtype err; 
	    err=fd_err(fd_SyntaxError,
		       "splicing UNQUOTE for an improper list",
		       details_string,insertion);
	    fd_decref(head); u8_free(details_string);
	    return err;}
	  obj=FD_CDR(obj);
	  continue;}
	else new_elt=
	       fd_make_list(2,unquotestar,fd_quasiquote(FD_CADR(elt),env,level-1));
      else new_elt=fd_quasiquote(elt,env,level);
    else new_elt=fd_quasiquote(elt,env,level);
    if (FD_ABORTP(new_elt)) {
      fd_decref(head); return new_elt;}
    new_tail=fd_init_pair(NULL,new_elt,FD_EMPTY_LIST);
    tailcons=FD_STRIP_CONS(new_tail,fd_pair_type,struct FD_PAIR *);
    *tail=new_tail; tail=&(tailcons->cdr);
    obj=FD_CDR(obj);}
  if (!(FD_EMPTY_LISTP(obj))) *tail=fd_incref(obj);
  return head;
}

static fdtype quasiquote_vector(fdtype obj,fd_lispenv env,int level)
{
  int i=0, j=0, len=FD_VECTOR_LENGTH(obj), newlen=len;
  if (len==0) return fd_incref(obj);
  else {
    fdtype *newelts=u8_alloc_n(len,fdtype);
    while (i < len) {
      fdtype elt=FD_VECTOR_REF(obj,i);
      if ((FD_PAIRP(elt)) &&
	  (FD_EQ(FD_CAR(elt),unquotestar)) &&
	  (FD_PAIRP(FD_CDR(elt))))
	if (level==1) {
	  fdtype insertion=fd_eval(FD_CADR(elt),env); int addlen=0;
	  if (FD_ABORTP(insertion)) {
	    int k=0; while (k<j) {fd_decref(newelts[k]); k++;}
	    u8_free(newelts);
	    return insertion;}
	  if (FD_PAIRP(insertion)) {
	    fdtype scan=insertion; while (FD_PAIRP(scan)) {
	      scan=FD_CDR(scan); addlen++;}
	    if (!(FD_EMPTY_LISTP(scan)))
	      return fd_err(fd_SyntaxError,
			    "splicing UNQUOTE for an improper list",
			    NULL,insertion);}
	  else if (FD_VECTORP(insertion)) addlen=FD_VECTOR_LENGTH(insertion);
	  else return fd_err(fd_SyntaxError,
			     "splicing UNQUOTE for an improper list",
			     NULL,insertion);
	  newelts=u8_realloc_n(newelts,newlen+addlen,fdtype);
	  newlen=newlen+addlen;
	  if (FD_PAIRP(insertion)) {
	    fdtype scan=insertion; while (FD_PAIRP(scan)) {
	      fdtype ielt=FD_CAR(scan); newelts[j++]=fd_incref(ielt);
	      scan=FD_CDR(scan);}
	    i++;}
	  else if (FD_VECTORP(insertion)) {
	    int k=0; while (k<addlen) {
	      fdtype ielt=FD_VECTOR_REF(insertion,k);
	      newelts[j++]=fd_incref(ielt); k++;}
	    i++;}
	  else {
	    fd_decref(insertion);
	    return fd_err(fd_SyntaxError,
			  "splicing UNQUOTE for an improper list",
			  NULL,insertion);}
	  fd_decref(insertion);}
	else {
	  fdtype new_elt=fd_quasiquote(elt,env,level-1);
	  if (FD_ABORTP(new_elt)) {
	    int k=0; while (k<j) {fd_decref(newelts[k]); k++;}
	    u8_free(newelts);
	    return new_elt;}
	  newelts[j]=new_elt;
	  i++; j++;}
      else {
	fdtype new_elt=fd_quasiquote(elt,env,level);
	if (FD_ABORTP(new_elt)) {
	  int k=0; while (k<j) {fd_decref(newelts[k]); k++;}
	  u8_free(newelts);
	  return new_elt;}
	newelts[j]=new_elt;
	i++; j++;}}
    return fd_init_vector(NULL,j,newelts);}
}

static fdtype quasiquote_slotmap(fdtype obj,fd_lispenv env,int level)
{
  int i=0, len=FD_SLOTMAP_SIZE(obj);
  struct FD_KEYVAL *keyvals=FD_XSLOTMAP(obj)->keyvals;
  fdtype result=fd_init_slotmap(NULL,0,NULL);
  struct FD_SLOTMAP *new_slotmap=FD_XSLOTMAP(result);
  while (i < len) {
    fdtype slotid=keyvals[i].key;
    fdtype value=keyvals[i].value;
    if (FD_PAIRP(value)) {
      fdtype qval=fd_quasiquote(value,env,level);
      if (FD_ABORTP(qval)) {
	fd_decref(result); return qval;}
      fd_slotmap_store(new_slotmap,slotid,qval);
      fd_decref(qval); i++;}
    else {
      fd_slotmap_store(new_slotmap,slotid,value);
      i++;}}
  return result;
}

static fdtype quasiquote_choice(fdtype obj,fd_lispenv env,int level)
{
  fdtype result=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(elt,obj) {
    fdtype transformed=fd_quasiquote(elt,env,level);
    FD_ADD_TO_CHOICE(result,transformed);}
  return result;
}

FD_EXPORT 
fdtype fd_quasiquote(fdtype obj,fd_lispenv env,int level)
{
  if (FD_PAIRP(obj))
    if (FD_BAD_UNQUOTEP(obj)) 
      return fd_err(fd_SyntaxError,"malformed UNQUOTE",NULL,obj);
    else if (FD_EQ(FD_CAR(obj),quasiquote))
      if (FD_PAIRP(FD_CDR(obj)))
	return fd_make_list(2,quasiquote,fd_quasiquote(FD_CADR(obj),env,level+1));
      else return fd_err(fd_SyntaxError,"malformed QUASIQUOTE",NULL,obj);
    else if (FD_EQ(FD_CAR(obj),unquote))
      if (level==1)
	return fd_eval(FD_CAR(FD_CDR(obj)),env);
      else return fd_make_list(2,unquote,fd_quasiquote(FD_CADR(obj),env,level-1));
    else if (FD_EQ(FD_CAR(obj),unquotestar))
      return fd_err(fd_SyntaxError,"UNQUOTE* (,@) in wrong context",
		    NULL,obj);
    else return quasiquote_list(obj,env,level);
  else if (FD_VECTORP(obj))
    return quasiquote_vector(obj,env,level);
  else if (FD_CHOICEP(obj))
    return quasiquote_choice(obj,env,level);
  else if (FD_SLOTMAPP(obj))
    return quasiquote_slotmap(obj,env,level);
  else return fd_incref(obj);
}

static fdtype quasiquote_handler(fdtype obj,fd_lispenv env)
{
  if ((FD_PAIRP(FD_CDR(obj))) &&
      (FD_EMPTY_LISTP(FD_CDR(FD_CDR(obj))))) {
    fdtype result=fd_quasiquote(FD_CAR(FD_CDR(obj)),env,1);
    if (FD_ABORTP(result))
      return result;
    else return result;}
  else return fd_err(fd_SyntaxError,"QUASIQUOTE",NULL,obj);
}

static fdtype quote_handler(fdtype obj,fd_lispenv env)
{
  if ((FD_PAIRP(obj)) && (FD_PAIRP(FD_CDR(obj))) &&
      ((FD_CDR(FD_CDR(obj)))==FD_EMPTY_LIST))
    return fd_incref(FD_CAR(FD_CDR(obj)));
  else return fd_err(fd_SyntaxError,"QUOTE",NULL,obj);
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

/* Making DTPROCs */

static fdtype make_dtproc(fdtype name,fdtype server,fdtype min_arity,fdtype arity)
{
  fdtype result;
  if (FD_VOIDP(min_arity))
    result=fd_make_dtproc(FD_SYMBOL_NAME(name),FD_STRDATA(server),1,-1,-1);
  else if (FD_VOIDP(arity))
    result=fd_make_dtproc(FD_SYMBOL_NAME(name),FD_STRDATA(server),1,fd_getint(arity),fd_getint(arity));
  else result=
	 fd_make_dtproc(FD_SYMBOL_NAME(name),FD_STRDATA(server),1,fd_getint(arity),fd_getint(min_arity));
  return result;
}

static fdtype cachecall(int n,fdtype *args)
{
  return fd_cachecall(args[0],n-1,args+1);
}

static fdtype clear_callcache(fdtype arg)
{
  fd_clear_callcache(arg);
  return FD_VOID;
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
  return fd_incref(x);
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
  quasiquote=fd_intern("QUASIQUOTE");
  unquote=fd_intern("UNQUOTE");
  unquotestar=fd_intern("UNQUOTE*");
  profile_symbol=fd_intern("%PROFILE");
  _fd_comment_symbol=comment_symbol=fd_intern("COMMENT");

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
  fd_idefn(fd_scheme_module,fd_make_cprimn("APPLY",apply_lexpr,1));
  fd_idefn(fd_xscheme_module,fd_make_cprim4x
	   ("DTPROC",make_dtproc,2,
	    fd_symbol_type,FD_VOID,fd_string_type,FD_VOID,
	    -1,FD_VOID,-1,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim1("CALL/CC",callcc,1));
  fd_defalias(fd_scheme_module,"CALL-WITH-CURRENT-CONTINUATION","CALL/CC");

  fd_idefn(fd_scheme_module,fd_make_cprimn("CACHECALL",cachecall,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1("CLEAR-CALLCACHE!",clear_callcache,0));

  fd_defspecial(fd_scheme_module,"QUASIQUOTE",quasiquote_handler);
  fd_defspecial(fd_scheme_module,"TIMEVAL",timed_eval);
  fd_defspecial(fd_scheme_module,"%TIMEVAL",timed_evalx);
  fd_defspecial(fd_scheme_module,"%WATCH",watched_eval);
  fd_defspecial(fd_scheme_module,"PROFILE",profiled_eval);

  fd_idefn(fd_scheme_module,
	   fd_make_ndprim(fd_make_cprimn("APPLYTEST",applytest,3)));
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

static void init_core_builtins()
{
  init_localfns();
  init_threadfns();
  fd_init_errors_c();
  fd_init_conditionals_c();
  fd_init_iterators_c();
  fd_init_choicefns_c();
  fd_init_binders_c();
  fd_init_corefns_c();
  fd_init_tablefns_c();
  fd_init_strings_c();
  fd_init_dbfns_c();
  fd_init_sequences_c();
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
