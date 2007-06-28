/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
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
    struct FD_ENVIRONMENT *newenv=u8_malloc_type(struct FD_ENVIRONMENT);
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

static fdtype lisp_copy_environment(fdtype env)
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
      fd_lock_mutex(&(ht->lock));
      n_slots=ht->n_slots; slots=ht->slots;
      while (i<n_slots)
	if (slots[i]) {
	  struct FD_HASHENTRY *hashentry=slots[i++];
	  int j=0, n_keyvals=hashentry->n_keyvals;
	  struct FD_KEYVAL *keyvals=&(hashentry->keyval0);
	  while (j<n_keyvals) {
	    envcount=envcount+count_envrefs(keyvals[j].value,env,depth-1); j++;}}
	else i++;
      fd_unlock_mutex(&(ht->lock));
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
  u8_notify("%WATCH","<%fsec> %q => %q",u8_elapsed_time()-start,toeval,value);
  return value;
}

/* OPCODE dispatching */

FD_FASTOP fdtype opcode_dispatch(fdtype opcode,fdtype arg1,fdtype body,fd_lispenv env)
{
  fdtype remainder=FD_CDR(body);
  fdtype arg2=FD_VOID, retval=FD_VOID;
  if (FD_ABORTP(arg1)) return arg1;
  switch (opcode) {
  case FD_EQ_OPCODE: case FD_EQV_OPCODE: case FD_EQUAL_OPCODE:
  case FD_GET_OPCODE: case FD_TEST_OPCODE: case FD_ELT_OPCODE:
    if (FD_PAIRP(remainder)) {
      arg2=fd_eval(FD_CAR(remainder),env); break;}
    else return fd_err(fd_SyntaxError,"opcode_dispatch",NULL,body);
  case FD_LT_OPCODE: case FD_LTE_OPCODE:
  case FD_GT_OPCODE: case FD_GTE_OPCODE:
  case FD_NUMEQ_OPCODE:
    if (!(FD_NUMBERP(arg1)))
	return fd_type_error(_("number"),"opcode_dispatch",arg1);
      if (FD_PAIRP(remainder)) {
	arg2=fd_eval(FD_CAR(remainder),env);
	if (FD_EXPECT_FALSE(!(FD_NUMBERP(arg2)))) {
	  fd_decref(arg1);
	  return fd_type_error(_("number"),"opcode_dispatch",arg2);}}
      else return fd_err(fd_SyntaxError,"opcode_dispatch",NULL,body);}
  if (FD_ABORTP(arg2)) {
    fd_decref(arg1); return arg2;}
  else switch (opcode) {
  case FD_AMBIGP_OPCODE: {
    if (FD_CHOICEP(arg1)) retval=FD_TRUE;
    else retval=FD_FALSE;
    break;}
  case FD_SINGLETONP_OPCODE: {
    if (FD_CHOICEP(arg1)) retval=FD_FALSE;
    else retval=FD_TRUE;
    break;}
  case FD_FAILP_OPCODE: {
    if (arg1==FD_EMPTY_CHOICE) retval=FD_TRUE;
    else retval=FD_FALSE;
    break;}
  case FD_EXISTSP_OPCODE: {
    if (arg1==FD_EMPTY_CHOICE) retval=FD_FALSE;
    else retval=FD_TRUE;
    break;}
  case FD_PLUS1_OPCODE: {
    if (FD_FIXNUMP(arg1)) {
      int iarg=FD_FIX2INT(arg1);
      return FD_INT2DTYPE(iarg+1);}
    else {
      retval=fd_plus(arg1,FD_FIX2INT(1));
      break;}}
  case FD_MINUS1_OPCODE: {
    if (FD_FIXNUMP(arg1)) {
      int iarg=FD_FIX2INT(arg1);
      return FD_INT2DTYPE(iarg-1);}
    else {
      retval=fd_plus(arg1,FD_FIX2INT(-1));
      break;}}
  case FD_ZEROP_OPCODE: {
    if (arg1==FD_INT2DTYPE(0)) retval=FD_TRUE; else retval=FD_FALSE;
    break;}
  case FD_NULLP_OPCODE: {
    if (arg1==FD_EMPTY_LIST) retval=FD_TRUE; else retval=FD_FALSE;
    break;}
  case FD_NUMBERP_OPCODE: {
    if (FD_NUMBERP(arg1)) retval=FD_TRUE; else retval=FD_FALSE;
    break;}
  case FD_VECTORP_OPCODE: {
    if (FD_VECTORP(arg1)) retval=FD_TRUE; else retval=FD_FALSE;
    break;}
  case FD_PAIRP_OPCODE: {
    if (FD_PAIRP(arg1)) retval=FD_TRUE; else retval=FD_FALSE;
    break;}
  case FD_CAR_OPCODE: {
    if (FD_PAIRP(arg1)) retval=fd_incref(FD_CAR(arg1));
    else return fd_type_error(_("pair"),"inline CAR",arg1);
    break;}
  case FD_CDR_OPCODE: {
    if (FD_PAIRP(arg1)) retval=fd_incref(FD_CDR(arg1)); 
    else return fd_type_error(_("pair"),"inline CDR",arg1);
    break;}
  case FD_SINGLETON_OPCODE:
    if (FD_CHOICEP(arg1)) retval=FD_EMPTY_CHOICE;
    else retval=fd_incref(arg1);
  case FD_EQ_OPCODE: {
    if (arg1==arg2) retval=FD_TRUE; else retval=FD_FALSE;
    break;}
  case FD_EQV_OPCODE: {
    if (arg1==arg2) retval=FD_TRUE;
    else if ((FD_NUMBERP(arg1)) && (FD_NUMBERP(arg2)))
      if (fd_numcompare(arg1,arg2)==0)
	retval=FD_TRUE; else retval=FD_FALSE;
    else retval=FD_FALSE;
    break;}
  case FD_EQUAL_OPCODE: {
    if ((FD_ATOMICP(arg1)) && (FD_ATOMICP(arg2)))
      if (arg1==arg2) retval=FD_TRUE; else retval=FD_FALSE;
    else if (FD_EQUAL(arg1,arg2)) retval=FD_TRUE;
    else retval=FD_FALSE;
    break;}
  case FD_GET_OPCODE: {
    if ((FD_ATOMICP(arg1)) && (FD_ATOMICP(arg2)))
      return fd_get(arg1,arg2,FD_EMPTY_CHOICE);
    else {
      retval=fd_get(arg1,arg2,FD_EMPTY_CHOICE);
      break;}}
  case FD_TEST_OPCODE: {
    fdtype arg3=FD_VOID;
    if (FD_PAIRP(FD_CDR(remainder)))
      arg3=fd_eval(FD_CAR(FD_CDR(FD_CDR(body))),env);
    if ((FD_ATOMICP(arg1)) && (FD_ATOMICP(arg2)) &&
	(FD_ATOMICP(arg3)))
      return fd_test(arg1,arg2,arg3);
    else {
      int result=fd_test(arg1,arg2,arg3);
      if (result) retval=FD_TRUE; else retval=FD_FALSE;
      break;}}
  case FD_ELT_OPCODE:
    if (!(FD_SEQUENCEP(arg1)))
      return fd_type_error(_("sequence"),"opcode ELT",arg1);
    else if (!(FD_FIXNUMP(arg2)))
      return fd_type_error(_("fixnum"),"opcode ELT",arg2);
    else {
      retval=fd_seq_elt(arg1,fd_getint(arg2)); break;}
  case FD_GT_OPCODE: {
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))>(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)>0) retval=FD_TRUE;
    else retval=FD_FALSE;
    break;}
  case FD_GTE_OPCODE: {
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))>=(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)>=0) retval=FD_TRUE;
    else retval=FD_FALSE;
    break;}
  case FD_LT_OPCODE: {
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))<(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)<0) retval=FD_TRUE;
    else retval=FD_FALSE;
    break;}
  case FD_LTE_OPCODE: {
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))<=(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)<=0) retval=FD_TRUE;
    else retval=FD_FALSE;
    break;}
  case FD_NUMEQ_OPCODE: {
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))==(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)==0) retval=FD_TRUE;
    else retval=FD_FALSE;
    break;}
  case FD_IF_OPCODE: {
    fdtype consequent_expr=FD_VOID;
    if (FD_EMPTY_CHOICEP(arg1))
      return FD_EMPTY_CHOICE;
    else if (FD_FALSEP(arg1)) {
      fdtype if_tail=FD_CDR(remainder);
      if (FD_PAIRP(if_tail))
	consequent_expr=FD_CAR(if_tail);
      else consequent_expr=FD_VOID;}
    else consequent_expr=FD_CAR(remainder);
    fd_decref(arg1);
    if (FD_PAIRP(consequent_expr))
      return fd_tail_eval(consequent_expr,env);
    else return fasteval(consequent_expr,env);}
  case FD_BEGIN_OPCODE: {
    while (FD_PAIRP(remainder)) {
      fd_decref(retval);
      retval=fd_eval(FD_CAR(remainder),env);
      remainder=FD_CDR(remainder);}
    break;}
  case FD_WHEN_OPCODE: {
    if (!(FD_FALSEP(arg1))) {
      fdtype test_body=remainder;
      while (FD_PAIRP(test_body)) {
	fdtype tmp=fd_eval(FD_CAR(test_body),env);
	fd_decref(tmp);
	test_body=FD_CDR(test_body);}}
    fd_decref(arg1);
    return FD_VOID;}
  case FD_UNLESS_OPCODE: {
    if (FD_FALSEP(arg1)) {
      fdtype test_body=remainder;
      while (FD_PAIRP(test_body)) {
	fdtype tmp=fd_eval(FD_CAR(test_body),env);
	fd_decref(tmp);
	test_body=FD_CDR(test_body);}}
    fd_decref(arg1);
    return FD_VOID;}
  case FD_IFELSE_OPCODE: {
    if (FD_EMPTY_CHOICEP(arg1))
      return FD_EMPTY_CHOICE;
    else if (FD_FALSEP(arg1)) {
      fdtype if_tail=FD_CDR(remainder);
      /* Done with this: */
      fd_decref(arg1);
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
      fd_decref(arg1);
      if (FD_PAIRP(consequent_expr))
	return fd_tail_eval(consequent_expr,env);
      else return fasteval(consequent_expr,env);}}
  case FD_AND_OPCODE: {
    if (FD_FALSEP(arg1)) return FD_FALSE;
    else {
      fdtype result=FD_FALSE;
      fd_decref(arg1);
      while (FD_PAIRP(remainder)) {
	fd_decref(result);
	result=fd_eval(FD_CAR(remainder),env);
	if (FD_ABORTP(result)) return result;
	else if (FD_FALSEP(result)) return result;
	else remainder=FD_CDR(remainder);}
      return result;}}
  case FD_OR_OPCODE: {
    if (!(FD_FALSEP(arg1))) return arg1;
    else {
      fdtype result=arg1;
      while (FD_PAIRP(remainder)) {
	fd_decref(result);
	result=fd_eval(FD_CAR(remainder),env);
	if (FD_ABORTP(result)) return result;
	else if (FD_FALSEP(result)) remainder=FD_CDR(remainder);
	else return result;}
      return FD_FALSE;}}
  case FD_NOT_OPCODE: {
    if (!(FD_FALSEP(arg1))) {
      fd_decref(arg1); return FD_FALSE;}
    else return FD_TRUE;}
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,body);
  }
  fd_decref(arg1); fd_decref(arg2);
  return retval;
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
      if (head==FD_QUOTE_OPCODE)
	return fd_car(FD_CDR(expr));
      else if (FD_PAIRP(FD_CDR(expr))) {
	fdtype arg1_expr=FD_CAR(FD_CDR(expr));
	fdtype arg1=fd_eval(arg1_expr,env);
	if (FD_ABORTP(arg1)) return arg1;
	else if (FD_VOIDP(arg1))
	  return fd_err(fd_VoidArgument,"opcode eval",NULL,arg1_expr);
	else if ((FD_CHOICEP(arg1)) || (FD_ACHOICEP(arg1))) {
	  fdtype results=FD_EMPTY_CHOICE;
	  FD_DO_CHOICES(arg,arg1) {
	    fdtype result=opcode_dispatch(head,arg,FD_CDR(expr),env);
	    if (FD_ABORTP(result)) {
	      FD_STOP_DO_CHOICES; return result;}
	    else if (FD_VOIDP(result)) {}
	    else {FD_ADD_TO_CHOICE(results,result);}}}
	else return opcode_dispatch(head,arg1,FD_CDR(expr),env);}
      else return fd_err(fd_SyntaxError,"opcode eval",NULL,expr);
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
      else if (FD_ABORTP(result))
	result=fd_passerr(result,fd_incref(expr));
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
    args=u8_malloc(args_length*sizeof(fdtype));
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
    fdtype *avec=u8_malloc(sizeof(fdtype)*(arg_count+1));
    memcpy(avec+1,args,sizeof(fdtype)*(arg_count));
    if (fcn->filename)
      if (fcn->name)
	avec[0]=fd_init_pair(NULL,fd_intern(fcn->name),fdtype_string(fcn->filename));
      else avec[0]=fd_init_pair(NULL,fd_intern("LAMBDA"),fdtype_string(fcn->filename));
    else if (fcn->name) avec[0]=fd_intern(fcn->name);
    else avec[0]=fd_intern("LAMBDA");
    if (args!=argv) u8_free(args);
    return fd_passerr(result,fd_init_vector(NULL,arg_count+1,avec));}
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
    struct FD_ENVIRONMENT *e=u8_malloc(sizeof(struct FD_ENVIRONMENT));
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
    struct FD_ENVIRONMENT *e=u8_malloc(sizeof(struct FD_ENVIRONMENT));
    FD_INIT_CONS(e,fd_environment_type);
    e->bindings=fd_incref(exports); e->exports=fd_incref(e->bindings);
    e->parent=fd_copy_env(parent);
    e->copy=e;
    return e;}
}

FD_EXPORT fd_lispenv fd_working_environment()
{
  if (fdscheme_initialized==0) fd_init_fdscheme();
  return fd_make_env(fd_make_hashtable(NULL,17,NULL),default_env);
}
FD_EXPORT fd_lispenv fd_safe_working_environment()
{
  if (fdscheme_initialized==0) fd_init_fdscheme();
  return fd_make_env(fd_make_hashtable(NULL,17,NULL),safe_default_env);
}

FD_EXPORT fdtype fd_register_module(char *name,fdtype module,int flags)
{
  if (flags&FD_MODULE_SAFE)
    fd_hashtable_store(&safe_module_map,fd_intern(name),module);
  else fd_hashtable_store(&module_map,fd_intern(name),module);
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

FD_EXPORT fdtype fd_new_module(char *name,int flags)
{
  fdtype module_name, module, as_stored;
  if (fdscheme_initialized==0) fd_init_fdscheme();
  module_name=fd_intern(name);
  module=fd_make_hashtable(NULL,0,NULL);
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

/* Making some functions */

FD_EXPORT fdtype fd_make_special_form(u8_string name,fd_evalfn fn)
{
  struct FD_SPECIAL_FORM *f=u8_malloc_type(struct FD_SPECIAL_FORM);
  FD_INIT_CONS(f,fd_specform_type);
  f->name=name; f->eval=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT void fd_defspecial(fdtype mod,u8_string name,fd_evalfn fn)
{
  struct FD_SPECIAL_FORM *f=u8_malloc_type(struct FD_SPECIAL_FORM);
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
    fdtype *values=u8_malloc(sizeof(fdtype)*n_args);
    int i=1, j=0, lim=n-1, ctype=FD_PRIM_TYPE(args[0]);
    /* Copy regular arguments */
    while (i<lim) {values[j]=fd_incref(args[i]); j++; i++;}
    i=0; while (j<n_args) {
      values[j]=fd_seq_elt(final_arg,i); j++; i++;}
    result=fd_applyfns[ctype](args[0],n_args,values);
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
  if (FD_ABORTP(result)) {
    if (tstruct->flags&FD_EVAL_THREAD)
      u8_warn(ThreadReturnError,"Thread evaluating %q returned %q",
	      tstruct->evaldata.expr,result);
    else u8_warn(ThreadReturnError,"Thread apply %q returned %q",
		 tstruct->applydata.fn,result);
    if ((fd_threaderror_backtrace) && (FD_PTR_TYPEP(result,fd_error_type))) {
      struct U8_OUTPUT out;
      struct FD_EXCEPTION_OBJECT *e=
	FD_GET_CONS(result,fd_error_type,struct FD_EXCEPTION_OBJECT *);
      U8_INIT_OUTPUT(&out,8192);
      fd_print_backtrace(&out,80,e->backtrace);
      u8_warn(ThreadReturnError,"%s",out.u8_outbuf);
      u8_free(out.u8_outbuf);}}
  if (tstruct->resultptr) *(tstruct->resultptr)=result;
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
  struct FD_CONSED_CONDVAR *cv=u8_malloc(sizeof(struct FD_CONSED_CONDVAR));
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
  fd_lock_mutex(&(cv->lock));
  return FD_TRUE;
}

static fdtype condvar_unlock(fdtype x)
{
  struct FD_CONSED_CONDVAR *cv=
    FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
  fd_unlock_mutex(&(cv->lock));
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

static fdtype synchro_lock(fdtype x)
{
  if (FD_PTR_TYPEP(x,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
    fd_lock_mutex(&(cv->lock));
    return FD_TRUE;}
  else if (FD_PTR_TYPEP(x,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(x,fd_sproc_type,struct FD_SPROC *);
    if (sp->synchronized) {
      fd_lock_mutex(&(sp->lock));}
    else return fd_type_error("lockable","synchro_lock",x);
    return FD_TRUE;}
  else return fd_type_error("lockable","synchro_lock",x);
}

static fdtype synchro_unlock(fdtype x)
{
  if (FD_PTR_TYPEP(x,fd_condvar_type)) {
    struct FD_CONSED_CONDVAR *cv=
      FD_GET_CONS(x,fd_condvar_type,struct FD_CONSED_CONDVAR *);
    fd_unlock_mutex(&(cv->lock));
    return FD_TRUE;}
  else if (FD_PTR_TYPEP(x,fd_sproc_type)) {
    struct FD_SPROC *sp=FD_GET_CONS(x,fd_sproc_type,struct FD_SPROC *);
    if (sp->synchronized) {
      fd_unlock_mutex(&(sp->lock));}
    else return fd_type_error("lockable","synchro_lock",x);
    return FD_TRUE;}
  else return fd_type_error("lockable","synchro_unlock",x);
}



/* Functions */

FD_EXPORT
fd_thread_struct fd_thread_call
  (fdtype *resultptr,fdtype fn,int n,fdtype *rail)
{
  struct FD_THREAD_STRUCT *tstruct=u8_malloc(sizeof(struct FD_THREAD_STRUCT));
  FD_INIT_CONS(tstruct,fd_thread_type);
  if (resultptr) {
    tstruct->resultptr=resultptr; tstruct->result=FD_NULL;}
  else {
    tstruct->result=FD_NULL; tstruct->resultptr=&(tstruct->result);}
  tstruct->flags=0;
  tstruct->applydata.fn=fd_incref(fn);
  tstruct->applydata.n_args=n; tstruct->applydata.args=rail; 
  pthread_create(&(tstruct->tid),pthread_attr_default,
		 thread_call,(void *)tstruct);
  fd_incref((fdtype)tstruct);
  return tstruct;
}

FD_EXPORT
fd_thread_struct fd_thread_eval(fdtype *resultptr,fdtype expr,fd_lispenv env)
{
  struct FD_THREAD_STRUCT *tstruct=u8_malloc(sizeof(struct FD_THREAD_STRUCT));
  FD_INIT_CONS(tstruct,fd_thread_type);
  if (resultptr) {
    tstruct->resultptr=resultptr; tstruct->result=FD_NULL;}
  else {
    tstruct->result=FD_NULL; tstruct->resultptr=&(tstruct->result);}
  tstruct->flags=FD_EVAL_THREAD;
  tstruct->evaldata.expr=fd_incref(expr);
  tstruct->evaldata.env=fd_copy_env(env);
  pthread_create(&(tstruct->tid),pthread_attr_default,
		 thread_call,(void *)tstruct);
  fd_incref((fdtype)tstruct);
  return tstruct;
}

/* Scheme primitives */

static fdtype threadcall_prim(int n,fdtype *args)
{
  fdtype *call_args=u8_malloc(sizeof(fdtype)*(n-1));
  int i=1; while (i<n) {
    call_args[i-1]=fd_incref(args[i]); i++;}
  return (fdtype)fd_thread_call(NULL,args[0],n-1,call_args);
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
    else u8_warn(ThreadReturnError,"Bad return code %d (%s) from %q",
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
    results=u8_malloc(sizeof(fdtype)*n_exprs);
    threads=u8_malloc(sizeof(fd_thread_struct)*n_exprs);}
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
    if (FD_ABORTP(result))
      if (FD_ABORTP(results[i]))
	result=fd_passerr(results[i],result);
      else fd_decref(results[i]);
    else if (FD_ABORTP(results[i])) {
      fd_decref(result); result=results[i];}
    else {FD_ADD_TO_CHOICE(result,results[i]);}
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
    fdtype *newelts=u8_malloc(sizeof(fdtype)*len);
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
	  newelts=u8_realloc(newelts,sizeof(fdtype)*(newlen+addlen));
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
  fdtype result=fd_init_slotmap(NULL,0,NULL,NULL);
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
    return fd_incref(cont->throwval);}
  else return fd_err(DoubleThrow,"call_continuation",NULL,arg);
}

u8_condition fd_throw_condition="CALL/CC THROW";

static fdtype callcc (fdtype proc)
{
  fdtype throwval=
    fd_err(fd_throw_condition,NULL,NULL,FD_VOID), value=FD_VOID, cont;
  struct FD_CONTINUATION *f=u8_malloc(sizeof(struct FD_CONTINUATION));
  FD_INIT_CONS(f,fd_function_type);
  f->name="continuation"; f->filename=NULL; 
  f->ndprim=1; f->xprim=1; f->arity=1; f->min_arity=1; 
  f->typeinfo=NULL; f->defaults=NULL;
  f->handler.xcall1=call_continuation;
  f->throwval=throwval; f->retval=FD_VOID;
  cont=FDTYPE_CONS(f);
  value=fd_apply(proc,1,&cont);
  if (value==throwval) {
    fdtype retval=f->retval;
    fd_decref(value);
    fd_decref(throwval);
    f->retval=FD_NULL;
    if (FD_CONS_REFCOUNT(f)>1) 
      u8_warn(ExpiredThrow,"Dangling pointer exists to continuation");
    fd_decref(cont);
    return retval;}
  else if (FD_VOIDP(f->retval)) {
    fd_decref(throwval);
    if (FD_CONS_REFCOUNT(f)>1) {
      u8_warn(ExpiredThrow,"Dangling pointer exists to continuation");
      f->retval=FD_NULL;}
    fd_decref(cont);
    return value;}
  else {
    fdtype errobj=fd_err(LostThrow,"callcc",NULL,f->retval);
    if (FD_CONS_REFCOUNT(f)>1) {
      u8_warn(ExpiredThrow,"Dangling pointer exists to continuation");
      f->retval=FD_NULL;}
    fd_decref(throwval); fd_decref(cont);
    return errobj;}
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
  while (i>0) {
    fdtype param=args[i];
    if ((i>1) && ((FD_SYMBOLP(param)) || (FD_PAIRP(param))))
      request=fd_init_pair(NULL,fd_make_list(2,quote_symbol,param),request);
    else request=fd_init_pair(NULL,param,request);
    fd_incref(param); i++;
    return request;}
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
  struct FD_TABLEFNS *fns=u8_malloc_type(struct FD_TABLEFNS);
  fns->get=lispenv_get; fns->store=lispenv_store;
  fns->add=NULL; fns->drop=NULL; fns->test=NULL;
  
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

  fd_make_hashtable(&module_map,67,NULL);
  fd_make_hashtable(&safe_module_map,67,NULL);
}

static void init_scheme_module()
{
  fd_xscheme_module=fd_make_hashtable(NULL,71,NULL);
  fd_scheme_module=fd_make_hashtable(NULL,71,NULL);
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
    ("TAILCALL",fd_boolconfig_get,fd_boolconfig_set,&fd_optimize_tail_calls);
}

FD_EXPORT void fd_init_exceptions_c(void);
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
  fd_init_exceptions_c();
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

  default_env=fd_make_env(fd_make_hashtable(NULL,0,NULL),NULL);
  safe_default_env=fd_make_env(fd_make_hashtable(NULL,0,NULL),NULL);

  fd_register_source_file(FDB_EVAL_H_VERSION);
  fd_register_source_file(versionid);

  init_scheme_module();
  init_core_builtins();

  return fdscheme_initialized;
}



/* The CVS log for this file
   $Log: eval.c,v $
   Revision 1.108  2006/02/11 18:08:52  haase
   Fixed bug in fastget for symeval

   Revision 1.107  2006/02/07 16:07:33  haase
   Issue warning when a thread exits with an error

   Revision 1.106  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.105  2006/01/23 18:16:41  haase
   Made environment get's return the empty choice rather than void and made splicing unquotes pass on error objects

   Revision 1.104  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.103  2006/01/05 19:12:08  haase
   Made ONERROR handle constant error values

   Revision 1.102  2005/12/26 18:19:44  haase
   Reorganized and documented lisp pointers and conses

   Revision 1.101  2005/12/24 16:09:18  haase
   Added PARALLEL special form

   Revision 1.100  2005/12/22 19:15:50  haase
   Added more comprehensive environment recycling

   Revision 1.99  2005/12/22 14:37:47  haase
   Handle error case in splicing non lists/vectors

   Revision 1.98  2005/12/20 17:48:53  haase
   Do arguments check for APPLYTEST

   Revision 1.97  2005/12/19 00:41:10  haase
   Fixes to quasiquoting and leak patch in evaltest

   Revision 1.96  2005/12/17 15:10:21  haase
   Simplified EVALTEST

   Revision 1.95  2005/12/17 05:55:23  haase
   Added applytest and evaltest, initial quasiquote fixes

   Revision 1.94  2005/11/29 18:07:10  haase
   Moved some names around for compatability and lisp condvars

   Revision 1.93  2005/11/18 13:49:37  haase
   Finished initial condvar implementation

   Revision 1.92  2005/11/18 04:41:35  haase
   Started condvar implementation

   Revision 1.91  2005/10/31 00:27:13  haase
   Added %WATCH primitive

   Revision 1.90  2005/10/29 20:47:52  haase
   Fixed ONERROR to use fd_dapply to process its value

   Revision 1.89  2005/10/29 19:44:15  haase
   Move RETURN-ERROR to eval.c and defined ONERROR to provide simple error catching

   Revision 1.88  2005/09/19 02:16:52  haase
   Added reflection functions for examining primitives, procedures, macros, etc.

   Revision 1.87  2005/09/11 22:52:31  haase
   Made environment construction calls initialize fdscheme to handle environments where constructor declarations aren't well handled

   Revision 1.86  2005/08/25 20:35:45  haase
   Fixed environment copying bug

   Revision 1.85  2005/08/16 17:11:53  haase
   Added test methods for most tables, made eval inline table operations, and made probes for binding use test rather than get.

   Revision 1.84  2005/08/15 03:28:56  haase
   Added file information to functions and display it in regular and HTML backtraces

   Revision 1.83  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.82  2005/07/31 22:02:30  haase
   Catch VOID results passed as arguments

   Revision 1.81  2005/07/23 22:21:48  haase
   Fixed time-earlier?

   Revision 1.80  2005/07/03 22:00:02  haase
   Fixed typo in GET-ARG declaration

   Revision 1.79  2005/07/03 12:29:38  haase
   Added GET-ARG

   Revision 1.78  2005/06/28 17:38:39  haase
   Added ENVIRONMENT? and SYMBOL-BOUND? primitives

   Revision 1.77  2005/06/27 16:54:30  haase
   Thread function improvements and addition of THREADJOIN primitive

   Revision 1.76  2005/06/25 16:25:47  haase
   Added threading primitives

   Revision 1.75  2005/06/02 17:55:59  haase
   Added SIDE-EFFECTS module

   Revision 1.74  2005/05/27 21:28:12  haase
   Fixed bug in passing argcount to fd_ndapply

   Revision 1.73  2005/05/22 20:30:03  haase
   Pass initialization errors out of config-def! and other error improvements

   Revision 1.72  2005/05/21 17:51:23  haase
   Added DTPROCs (remote procedures)

   Revision 1.71  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.70  2005/05/09 20:04:19  haase
   Move dtype hash functions into dbfile and made libfdscheme independent of libfddbfile

   Revision 1.69  2005/05/06 16:12:18  haase
   Made fd_new_module implicitly initialize scheme if neccessary

   Revision 1.68  2005/05/04 09:11:44  haase
   Moved FD_INIT_SCHEME_BUILTINS into individual executables in order to dance around an OS X dependency problem

   Revision 1.67  2005/05/02 15:03:23  haase
   Reorganized conditional compilation

   Revision 1.66  2005/04/30 16:13:03  haase
   Removed special form call tracking

   Revision 1.65  2005/04/30 12:45:03  haase
   Added CALLTRACK, an internal profiling mechanism

   Revision 1.64  2005/04/30 02:47:18  haase
   Fixed failure to gc when profiling

   Revision 1.63  2005/04/29 18:48:54  haase
   Undid some changes to position/search and cleaned up handling of negative arguments

   Revision 1.62  2005/04/29 18:10:32  haase
   Made redundant default module declarations not make a difference

   Revision 1.61  2005/04/25 19:16:08  haase
   Added numeric initialization to scheme module initialization

   Revision 1.60  2005/04/21 19:03:26  haase
   Add initialization procedures

   Revision 1.59  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.58  2005/04/15 13:58:10  haase
   Changed variable handing to accomodate unset (void) bindings

   Revision 1.57  2005/04/14 16:22:44  haase
   Add distinction of export environments for environments created by use-module etc.

   Revision 1.56  2005/04/14 00:26:39  haase
   Added PROFILE primitive, fixed binding primitives

   Revision 1.55  2005/04/11 04:33:01  haase
   Fix attempt to decref a NULL parent pointer when freeing an environment

   Revision 1.54  2005/04/08 04:46:30  haase
   Improvements to backtrace accumulation

   Revision 1.53  2005/04/06 14:40:01  haase
   Made TIMEVAL use u8_message rather than u8_fprintf

   Revision 1.52  2005/04/02 20:17:11  haase
   Added fd_new_module for safe module creation

   Revision 1.51  2005/04/02 16:06:46  haase
   Made module declarations effective current environments by doing a rplacd for the default environments

   Revision 1.50  2005/04/01 18:47:28  haase
   Added more branch predictions

   Revision 1.49  2005/04/01 15:59:31  haase
   Fixed redundancy in fd_symeval

   Revision 1.48  2005/04/01 02:42:40  haase
   Reimplemented module exports to be faster and less kludgy

   Revision 1.47  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.46  2005/03/22 20:25:39  haase
   Added macros, fixed DO syntax checking error, and made minor fixes to quasiquoting

   Revision 1.45  2005/03/22 19:06:59  haase
   Made quasiquote work

   Revision 1.44  2005/03/22 18:25:02  haase
   Added quasiquote

   Revision 1.43  2005/03/06 18:28:21  haase
   Added timeprims

   Revision 1.42  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.41  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.40  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.39  2005/02/25 16:30:28  haase
   Fix error passing from fd_eval_exprs

   Revision 1.38  2005/02/19 16:24:01  haase
   Use fd_intern rather than fd_parse to make symbols for binding

   Revision 1.37  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.36  2005/02/12 01:34:52  haase
   Added simple portfns and default output ports

   Revision 1.35  2005/02/11 03:50:12  haase
   Added some eval branch predictions

   Revision 1.34  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
