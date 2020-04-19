/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES	(!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES	(!(KNO_AVOID_INLINE))
#define KNO_INLINE_FCNIDS	(!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS	(!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV	(!(KNO_AVOID_INLINE))

#define KNO_EVAL_INTERNALS 1

#define INIT_ARGBUF_LEN 7

/* Evaluator defines */

#ifndef KNO_WITH_TAIL_EVAL
#define KNO_WITH_TAIL_EVAL 1
#endif

#ifndef KNO_WITH_TAIL_CALLS
#define KNO_WITH_TAIL_CALLS 1
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/opcodes.h"
#include "kno/dbprims.h"
#include "kno/ffi.h"
#include "kno/cprims.h"
#include "kno/profiles.h"

#include "../apply/apply_internals.h"
#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

int kno_scheme_initialized = 0;

int kno_enable_tail_calls = 1;

lispval _kno_comment_symbol, fcnids_symbol, sourceref_tag;

static lispval comment_symbol, source_symbol;

u8_condition kno_SyntaxError=_("SCHEME expression syntax error"),
  kno_InvalidMacro=_("invalid macro transformer"),
  kno_UnboundIdentifier=_("the variable is unbound"),
  kno_VoidBinding=_("VOID result used as variable binding"),
  kno_VoidSortKey=_("VOID result used as sort key"),
  kno_VoidResult=_("Unexpected VOID result returned"),
  kno_VoidBoolean=_("VOID result as boolean value"),
  kno_NotAnIdentifier=_("not an identifier"),
  kno_TooFewExpressions=_("too few subexpressions"),
  kno_CantBind=_("can't add binding to environment"),
  kno_BadEvalOp=_("Invalid evaluator operation"),
  kno_BadOpcode=_("Bad opcode in compiled code"),
  kno_OpcodeSyntaxError=_("Bad opcode syntax"),
  kno_ReadOnlyEnv=_("Read only environment"),
  kno_TailArgument=_("EvalError/RawTailValue"),
  kno_BadArgument=_("EvalError/BadArgument/Value");

u8_condition BadExpressionHead=_("BadExpressionHead");

lispval _eval_expr(lispval head,lispval expr,
		   kno_lexenv env,kno_stack stack,
		   int tail);

#define head_label(head) \
  ( (SYMBOLP(head)) ? (SYM_NAME(head)) : \
    (KNO_OPCODEP(head)) ? (opcode_name(head)) : \
    (NULL) )

/* Top level functions */

KNO_EXPORT lispval kno_eval(lispval x,kno_lexenv env,
			    kno_stack stack,
			    int tail)
{
  return fast_eval(x,env,stack,tail);
}

KNO_EXPORT lispval kno_stack_eval(lispval x,kno_lexenv env,
				 kno_stack stack)
{
  lispval result;
  KNO_START_EVAL(eval_stack,"pusheval",x,env,stack);
  result = kno_eval(x,env,eval_stack,0);
  kno_pop_stack(eval_stack);
  return result;
}

#define ND_ARGP(v) ((CHOICEP(v))||(QCHOICEP(v)))

KNO_FASTOP int commentp(lispval arg)
{
  return (PRED_FALSE((PAIRP(arg)) && (KNO_EQ(KNO_CAR(arg),comment_symbol))));
}

KNO_EXPORT lispval kno_bad_arg(lispval arg,u8_context cxt,lispval source_expr)
{
  return kno_err((arg == KNO_TAIL) ? (kno_TailArgument) :
		 (arg == KNO_VOID) ? (kno_VoidArgument) :
		 (kno_BadArgument),cxt,
		 kno_constant_name(arg),
		 source_expr);
}

/* Environment functions */

static int bound_in_envp(lispval symbol,kno_lexenv env)
{
  lispval bindings = env->env_bindings;
  if (HASHTABLEP(bindings))
    return kno_hashtable_probe((kno_hashtable)bindings,symbol);
  else if (SCHEMAPP(bindings))
    return kno_schemap_test((kno_schemap)bindings,symbol,VOID);
  else return kno_test(bindings,symbol,VOID);
}

KNO_FASTOP lispval eval_arg(lispval arg_expr,kno_lexenv env,
			    kno_stack stack)
{
  kno_lisp_type arg_type = KNO_TYPEOF(arg_expr);
  switch (arg_type) {
  case kno_fixnum_type: case kno_oid_type:
    return arg_expr;
  case kno_lexref_type:
    return eval_lexref(arg_expr,env);
  case kno_symbol_type:
    return eval_symbol(arg_expr,env);
  case kno_fcnid_type:
    return eval_fcnid(arg_expr);
  case kno_pair_type: {
    u8_string old_label = stack->stack_label;
    lispval result = _eval_expr(KNO_CAR(arg_expr),arg_expr,env,stack,0);
    stack->stack_label = old_label;
    return result;}
  case kno_schemap_type:
    return eval_schemap(arg_expr,env,stack);
  case kno_choice_type:
    return eval_choice(arg_expr,env,stack);
  default:
    if (KNO_CONSP(arg_expr))
      return kno_incref(arg_expr);
    else return arg_expr;}
}

/* Opcodes */

u8_string kno_opcode_names[0xF00]         ={NULL};
unsigned char kno_opcode_call_info[0xF00] = { 0 };
const int kno_opcodes_length = 0xF00;

u8_string opcode_name(lispval opcode)
{
  long opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if ((opcode_offset<kno_opcodes_length) &&
      (kno_opcode_names[opcode_offset]))
    return kno_opcode_names[opcode_offset];
  else return NULL;
}

KNO_EXPORT u8_string kno_opcode_name(lispval opcode)
{
  return opcode_name(opcode);
}

/* Some datatype methods */

static int unparse_evalfn(u8_output out,lispval x)
{
  struct KNO_EVALFN *s=
    kno_consptr(struct KNO_EVALFN *,x,kno_evalfn_type);
  lispval moduleid = s->evalfn_moduleid;
  u8_string modname =
    (KNO_SYMBOLP(moduleid)) ? (KNO_SYMBOL_NAME(moduleid)) : (NULL);
  if (s->evalfn_filename) {
    u8_string filename = s->evalfn_filename;
    size_t len = strlen(filename);
    u8_string space_break=strchr(filename,' ');
    u8_byte buf[len+1];
    if (space_break) {
      strcpy(buf,filename);
      buf[space_break-filename]='\0';
      filename=buf;}
    u8_printf(out,"#<EvalFN %s %s%s%s%s%s%s>",
	      s->evalfn_name,
	      U8OPTSTR(" ",modname,""),
	      U8OPTSTR(" '",buf,"'"));}
  else u8_printf(out,"#<EvalFN %s%s%s%s>",s->evalfn_name,
		 U8OPTSTR(" ",modname,""));
  return 1;
}

static ssize_t write_evalfn_dtype(struct KNO_OUTBUF *out,lispval x)
{
  struct KNO_EVALFN *s=
    kno_consptr(struct KNO_EVALFN *,x,kno_evalfn_type);
  u8_string name=s->evalfn_name;
  u8_string filename=s->evalfn_filename;
  size_t name_len=strlen(name);
  ssize_t file_len=(filename) ? (strlen(filename)) : (-1);
  int n_elts = (file_len<0) ? (1) : (0);
  unsigned char buf[100], *tagname="%EVALFN";
  struct KNO_OUTBUF tmp = { 0 };
  KNO_INIT_OUTBUF(&tmp,buf,100,0);
  kno_write_byte(&tmp,dt_compound);
  kno_write_byte(&tmp,dt_symbol);
  kno_write_4bytes(&tmp,strlen(tagname));
  kno_write_bytes(&tmp,tagname,strlen(tagname));
  kno_write_byte(&tmp,dt_vector);
  kno_write_4bytes(&tmp,n_elts);
  kno_write_byte(&tmp,dt_string);
  kno_write_4bytes(&tmp,name_len);
  kno_write_bytes(&tmp,name,name_len);
  if (file_len>=0) {
    kno_write_byte(&tmp,dt_string);
    kno_write_4bytes(&tmp,file_len);
    kno_write_bytes(&tmp,filename,file_len);}
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  kno_write_bytes(out,tmp.buffer,n_bytes);
  kno_close_outbuf(&tmp);
  return n_bytes;
}

KNO_EXPORT void recycle_evalfn(struct KNO_RAW_CONS *c)
{
  struct KNO_EVALFN *sf = (struct KNO_EVALFN *)c;
  if (sf->evalfn_name) u8_free(sf->evalfn_name);
  if (sf->evalfn_cname) u8_free(sf->evalfn_cname);
  if (sf->evalfn_documentation) u8_free(sf->evalfn_documentation);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

/* Checking if eval is needed */

KNO_EXPORT int kno_choice_evalp(lispval x)
{
  KNO_DO_CHOICES(e,x) {
    if (KNO_NEED_EVALP(e)) {
      KNO_STOP_DO_CHOICES;
      return 1;}}
  return 0;
}

KNO_EXPORT
lispval kno_eval_body(lispval body,kno_lexenv env,kno_stack stack,
		      u8_context cxt,u8_string label,
		      int tail)
{
  return eval_body(body,env,stack,cxt,label,tail);
}

/* Symbol lookup */

KNO_EXPORT lispval kno_symeval(lispval sym,kno_lexenv env)
{
  return symeval(sym,env);
}

/* Assignments */

static int add_to_value(lispval sym,lispval val,kno_lexenv env)
{
  int rv=1;
  if (env) {
    lispval bindings=env->env_bindings;
    lispval exports=env->env_exports;
    if ((kno_add(bindings,sym,val))>=0) {
      if (HASHTABLEP(exports)) {
	lispval newval=kno_get(bindings,sym,EMPTY);
	if (KNO_ABORTP(newval))
	  rv=-1;
	else {
	  int e_rv=kno_hashtable_op
	    ((kno_hashtable)exports,kno_table_replace,sym,newval);
	  kno_decref(newval);
	  if (e_rv<0) rv=e_rv;}}}
    else rv=-1;}
  if (rv<0) {
    kno_seterr(kno_CantBind,"add_to_env_value",NULL,sym);
    return -1;}
  else return rv;
}

KNO_EXPORT int kno_bind_value(lispval sym,lispval val,kno_lexenv env)
{
  /* TODO: Check for checking the return value of calls to
     `kno_bind_value` */
  if (env) {
    lispval bindings = env->env_bindings;
    lispval exports = env->env_exports;
    if (kno_store(bindings,sym,val)<0) {
      kno_poperr(NULL,NULL,NULL,NULL);
      kno_seterr(kno_CantBind,"kno_bind_value",NULL,sym);
      return -1;}
    if ( (KNO_CONSP(val)) &&
	 ( (KNO_FUNCTIONP(val)) || (KNO_APPLICABLEP(val)) ) &&
	 (KNO_HASHTABLEP(bindings)) ) {
      /* Update fcnids if needed */
      lispval fcnids = kno_get(bindings,fcnids_symbol,KNO_VOID);
      if (KNO_HASHTABLEP(fcnids)) {
	lispval fcnid = kno_get(fcnids,sym,KNO_VOID);
	if (KNO_FCNIDP(fcnid)) kno_set_fcnid(fcnid,val);}
      kno_decref(fcnids);}
    /* If there are exports, modify this variable if needed. */
    if (HASHTABLEP(exports))
      kno_hashtable_op((kno_hashtable)(exports),kno_table_replace,sym,val);
    return 1;}
  else return 0;
}

KNO_EXPORT int kno_add_value(lispval symbol,lispval value,kno_lexenv env)
{
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env = env->env_parent;
      if ((env) && (env->env_copy))
	env = env->env_copy;}
    else if ((env->env_bindings) == (env->env_exports))
      return kno_reterr(kno_ReadOnlyEnv,"kno_assign_value",NULL,symbol);
    else return add_to_value(symbol,value,env);}
  return 0;
}

KNO_EXPORT int kno_assign_value(lispval symbol,lispval value,kno_lexenv env)
{
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env = env->env_parent;
      if ((env) && (env->env_copy)) env = env->env_copy;}
    else if ((env->env_bindings) == (env->env_exports))
      /* This is the kind of environment produced by using a module,
	 so it's read only. */
      return kno_reterr(kno_ReadOnlyEnv,"kno_assign_value",NULL,symbol);
    else return kno_bind_value(symbol,value,env);}
  return 0;
}

/* FCN/REF */

KNO_EXPORT lispval kno_fcn_ref(lispval sym,kno_lexenv env,lispval val)
{
  if (env) {
    if (! ( (KNO_CONSP(val)) && 
	    ( (KNO_FUNCTIONP(val)) || 
	      (KNO_APPLICABLEP(val)) ||
	      (KNO_MACROP(val)) )) ) {
      u8_log(LOGWARN,"BadAliasValue","The value of '%q cannot be aliased: %q",
	     sym,val);
      return kno_incref(val);}
    lispval bindings = env->env_bindings;
    if (KNO_HASHTABLEP(bindings)) {
      /* Update fcnids if needed */
      lispval fcnids = kno_get(bindings,fcnids_symbol,KNO_VOID);
      if (!(KNO_HASHTABLEP(fcnids))) {
	lispval use_table = kno_make_hashtable(NULL,19);
	kno_hashtable_op((kno_hashtable)bindings,
			 kno_table_default,fcnids_symbol,use_table);
	kno_decref(use_table);
	fcnids = kno_get(bindings,fcnids_symbol,KNO_VOID);}
      lispval fcnid = kno_get(fcnids,sym,KNO_VOID);
      if (!(KNO_FCNIDP(fcnid))) {
	lispval new_fcnid = kno_register_fcnid(val);
	/* In rare cases, this may leak a fcnid reference, but
	   we're not worrying about that. */
	kno_hashtable_op((kno_hashtable)fcnids,
			 kno_table_default,sym,new_fcnid);
	fcnid = kno_get(fcnids,sym,KNO_VOID);}
      if ( (KNO_FCNIDP(fcnid)) && (KNO_CONSP(val)) &&
	   ( (KNO_FUNCTIONP(val)) || (KNO_APPLICABLEP(val)) )) {
	kno_set_fcnid(fcnid,val);}
      kno_decref(fcnids);
      return fcnid;}
    else return kno_incref(val);}
  else return kno_incref(val);
}

static lispval fcnalias_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval sym = kno_get_arg(expr,1);
  if (!(KNO_SYMBOLP(sym)))
    return kno_err(kno_SyntaxError,"fcnalias_evalfn",NULL,expr);
  lispval env_expr = kno_get_arg(expr,2), free_env = KNO_VOID;
  kno_lexenv use_env = env;
  if (!(KNO_VOIDP(env_expr))) {
    lispval env_arg = kno_eval(env_expr,env,stack,0);
    if (KNO_LEXENVP(env_arg)) {
      kno_lexenv e = (kno_lexenv) env_arg;
      if (KNO_HASHTABLEP(e->env_bindings))
	use_env = (kno_lexenv) env_arg;}
    free_env = env_arg;}
  lispval val = kno_symeval(sym,use_env);
  if (KNO_ABORTED(val)) {
    kno_decref(free_env);
    return val;}
  else if ( (KNO_CONSP(val)) && 
	    ( (KNO_FUNCTIONP(val)) || 
	      (KNO_APPLICABLEP(val)) ||
	      (KNO_MACROP(val)) ) ) {
    lispval fcnid = kno_fcn_ref(sym,use_env,val);
    kno_decref(val);
    kno_decref(free_env);
    return fcnid;}
  else {
    kno_decref(free_env);
    u8_log(LOG_WARN,"BadAlias",
	   "Can't make alias of the value of '%s: %q",
	   KNO_SYMBOL_NAME(sym),val);
    return val;}
}

/* Quote */

static lispval quote_evalfn(lispval obj,kno_lexenv env,kno_stack stack)
{
  if ((PAIRP(obj)) && (PAIRP(KNO_CDR(obj))) &&
      ((KNO_CDR(KNO_CDR(obj))) == NIL))
    return kno_incref(KNO_CAR(KNO_CDR(obj)));
  else return kno_err(kno_SyntaxError,"QUOTE",NULL,obj);
}

/* The evaluator itself */

static lispval reduce_loop(kno_stack loop_stack,int tail);

lispval eval_choice(lispval expr,kno_lexenv env,
			   kno_stack _stack)
{
  lispval result = EMPTY;
  KNO_PUSH_EVAL(choice_stack,NULL,expr,env);
  DO_CHOICES(each_expr,expr) {
    lispval r = fast_eval(each_expr,env,choice_stack,0);
    if (KNO_ABORTED(r)) {
      KNO_STOP_DO_CHOICES;
      kno_decref(result);
      result=r;
      break;}
    else {CHOICE_ADD(result,r);}}
  kno_pop_stack(choice_stack);
  return simplify_value(result);
}
KNO_EXPORT lispval _kno_eval_choice
(lispval expr,kno_lexenv env,kno_stack stack)
{
  return eval_choice(expr,env,stack);
}

/* Evaluating schemaps */

lispval eval_schemap(lispval expr,kno_lexenv env,
		     kno_stack _stack)
{
  struct KNO_SCHEMAP *skmap = (kno_schemap) expr;
  int n = skmap->schema_length;
  lispval *schema = skmap->table_schema;
  lispval *vals = skmap->table_values;
  lispval result = KNO_VOID, new_vals[n];
  KNO_PUSH_EVAL(keyval_stack,NULL,KNO_VOID,env);
  int i = 0; while (i < n) {
    lispval val_expr = vals[i];
    lispval val = fast_eval(val_expr,env,keyval_stack,0);
    if (KNO_ABORTED(val)) {
      result = val;
      break;}
    else new_vals[i++] = val;}
  if (KNO_ABORTED(result))
    kno_decref_vec(new_vals,i);
  else {
    result = kno_make_schemap(NULL,n,KNO_SCHEMAP_STATIC_VALUES,schema,new_vals);
    /* Set the template, which allows the new schemap to share the schema */
    struct KNO_SCHEMAP *newmap = (kno_schemap) result;
    newmap->schemap_template=expr; kno_incref(expr);}
  kno_pop_stack(keyval_stack);
  return result;
}
KNO_EXPORT lispval _kno_eval_schemap
(lispval expr,kno_lexenv env,kno_stack stack)
{
  return eval_schemap(expr,env,stack);
}

/* Evaluating macros */

static lispval macro_eval(struct KNO_MACRO *macrofn,lispval expr,
			  kno_lexenv env,kno_stack eval_stack)
{
  lispval xformer = macrofn->macro_transformer;
  lispval new_expr = kno_call(eval_stack,xformer,1,&expr);
  if (KNO_ABORTED(new_expr)) {
    u8_string macro_name = macrofn->macro_name;
    if (macro_name == NULL) macro_name = eval_stack->stack_label;
    return kno_err(kno_SyntaxError,_("macro expansion"),
		   (macro_name) ? (macro_name) : (eval_stack->stack_label),
		   expr);}
  else NO_ELSE;
  KNO_START_EVALX(macro_stack,"macroexpansion",new_expr,env,eval_stack,7);
  kno_add_stack_ref(macro_stack,new_expr);
  lispval result = fast_eval(new_expr,env,macro_stack,0);
  kno_pop_stack(macro_stack);
  return result;
}

/* Opcode handling */

static lispval op_arity_error(lispval op,int got,int expected)
{
  kno_seterr((got>expected) ? (kno_TooManyArgs) : (kno_TooFewArgs),
	     "eval_apply/opcode",opcode_name(op),KNO_VOID);
  return KNO_ERROR;
}

#include "opcode_defs.c"

static lispval opcode_dispatch(lispval op,int n,kno_argvec args)
{
  lispval opcode_class = KNO_OPCODE_CLASS(op);
  switch (opcode_class) {
  case KNO_D1_OPCODE_CLASS:
    if (PRED_FALSE(n != 1))
      return op_arity_error(op,n,1);
    else return d1_call(op,args[0]);
  case KNO_D2_OPCODE_CLASS:
    if (PRED_FALSE(n != 2))
      return op_arity_error(op,n,2);
    else return d2_call(op,args[0],args[1]);
  case KNO_ND1_OPCODE_CLASS:
    if (PRED_FALSE(n != 1))
      return op_arity_error(op,n,1);
    else return nd1_call(op,args[0]);
  case KNO_ND2_OPCODE_CLASS:
    if (PRED_FALSE(n != 2))
      return op_arity_error(op,n,2);
    else return nd2_call(op,args[0],args[1]);
  case KNO_NUMERIC_OPCODE_CLASS:
    return handle_numeric_opcode(op,n,args);
  case KNO_TABLE_OPCODE_CLASS:
    return handle_table_opcode(op,n,args);
  default:
    return kno_err(kno_BadOpcode,"opcode_dispatch",opcode_name(op),op);}
}

#if 0
static lispval opcode_dispatch(lispval op,int n,kno_argvec args)
{
  if (KNO_D1_OPCODEP(op))
    if (PRED_FALSE(n != 1))
      return op_arity_error(op,n,1);
    else return d1_call(op,args[0]);
  else if (KNO_D2_OPCODEP(op))
    if (PRED_FALSE(n != 2))
      return op_arity_error(op,n,2);
    else return d2_call(op,args[0],args[1]);
  else if (KNO_ND1_OPCODEP(op))
    if (PRED_FALSE(n != 1))
      return op_arity_error(op,n,1);
    else return nd1_call(op,args[0]);
  else if (KNO_ND2_OPCODEP(op))
    if (PRED_FALSE(n != 2))
      return op_arity_error(op,n,2);
    else return nd2_call(op,args[0],args[1]);
  else if (KNO_NUMERIC_OPCODEP(op))
    return handle_numeric_opcode(op,n,args);
  else if (KNO_TABLE_OPCODEP(op))
    return handle_table_opcode(op,n,args);
  else return kno_err(kno_BadOpcode,"opcode_dispatch",opcode_name(op),op);
}
#endif

static int opcode_choice_loop(lispval *resultp,
			      lispval op,int i,int n,
			      kno_argvec nd_args,
			      lispval *d_args)
{
  if (n==i) {
    lispval result = opcode_dispatch(op,n,d_args);
    if (KNO_ABORTED(result)) goto err_exit;
    else {KNO_ADD_TO_CHOICE(*resultp,result);}
    return 1;}
  lispval arg = nd_args[i];
  if (CHOICEP(arg)) {
    DO_CHOICES(each,arg) {
      d_args[i] = each;
      int rv= opcode_choice_loop(resultp,op,i+1,n,nd_args,d_args);
      if (rv<0) goto err_exit;}
    return 0;}
  else {
    d_args[i] = arg;
    return opcode_choice_loop(resultp,op,i+1,n,nd_args,d_args);}
  lispval r;
 err_exit:
  r = *resultp; kno_decref(r); *resultp=KNO_ERROR;
  return -1;
}

static lispval opcode_1choice_loop(lispval op,lispval args)
{
  lispval results = KNO_EMPTY;
  KNO_DO_CHOICES(arg,args) {
    lispval result = opcode_dispatch(op,1,&arg);
    if (KNO_ABORTED(result)) {
      kno_decref(results);
      return result;}
    else {KNO_ADD_TO_CHOICE(results,result);}}
  return results;
}

static lispval opcode_2choice_loop(lispval op,lispval args1,lispval args2)
{
  lispval results = KNO_EMPTY, err = KNO_VOID;
  if ( (KNO_CHOICEP(args1)) && (KNO_CHOICEP(args2)) ) {
    KNO_DO_CHOICES(arg1,args1) {
      KNO_DO_CHOICES(arg2,args2) {
	lispval d_args[2] = {arg1,arg2};
	lispval result = opcode_dispatch(op,2,d_args);
	if (KNO_ABORTED(result)) { err=result; goto err_exit;}
	else {KNO_ADD_TO_CHOICE(results,result);}}}}
  else if (KNO_CHOICEP(args1)) {
    KNO_DO_CHOICES(arg1,args1) {
      lispval d_args[2] = {arg1,args2};
      lispval result = opcode_dispatch(op,2,d_args);
      if (KNO_ABORTED(result)) { err=result; goto err_exit;}
      else {KNO_ADD_TO_CHOICE(results,result);}}}
 else {
   KNO_DO_CHOICES(arg2,args2) {
     lispval d_args[2] = {args1,arg2};
     lispval result = opcode_dispatch(op,2,d_args);
     if (KNO_ABORTED(result)) { err=result; goto err_exit;}
     else {KNO_ADD_TO_CHOICE(results,result);}}}
  return results;
 err_exit:
  kno_decref(results);
  return err;
}


/* Lambda application */

static lispval reduce_loop(kno_stack loop_stack,int tail);

static int lambda_check_arity(struct KNO_LAMBDA *proc,int n)
{
  int arity = proc->fcn_arity;
  int min_arity = proc->fcn_min_arity;
  if (n < min_arity) {
    kno_seterr(kno_TooFewArgs,proc->fcn_name,NULL,VOID);
    return 0;}
  else if ( (arity>=0) && (n>arity) ) {
    kno_seterr(kno_TooManyArgs,proc->fcn_name,NULL,VOID);
    return 0;}
  else return 1;
}

static lispval get_rest_arg(kno_argvec args,int n)
{
  lispval result=NIL; n--;
  while (n>=0) {
    lispval arg=args[n];
    kno_incref(arg);
    result=kno_init_pair(NULL,arg,result);
    n--;}
  return result;
}

lispval lambda_call(kno_stack stack,
		    struct KNO_LAMBDA *proc,
		    int n_given,kno_argvec given,
		    int tail)
{
  kno_lexenv proc_env=proc->lambda_env;
  lispval *vars = proc->lambda_vars;
  int synchronized = proc->lambda_synchronized;
  int arity = proc->fcn_arity;
  int n_vars = proc->lambda_n_vars;
  int decref_args = 0;

  u8_string label = (proc->fcn_name) ? (proc->fcn_name) : (U8S("lambda"));
  u8_string filename = (proc->fcn_filename) ? (proc->fcn_filename) : (NULL);
  KNO_START_EVAL(lambda_stack,label,(lispval)proc,NULL,stack);
 stackpush:
  lambda_stack->stack_file = filename;
  lispval args[n_vars];
  _lambda_stack.stack_args.elts	 = args;
  _lambda_stack.stack_args.count = 0;
  _lambda_stack.stack_args.len	 = n_vars;
  KNO_STACK_SET_BITS(lambda_stack,KNO_STACK_DECREF_ARGS);
  lispval refs[7];
  _lambda_stack.stack_refs.elts	 = refs;
  _lambda_stack.stack_refs.count = 0;
  _lambda_stack.stack_refs.len	 = 7;

  kno_profile profile = proc->fcn_profile;
  struct rusage before; struct timespec start;
  if (profile) {
    if (profile->prof_disabled) profile=NULL;
    else kno_profile_start(&before,&start);}

  int tailable = ( (!(synchronized)) && (profile == NULL) );

  int i = 0, max_positional = (arity < 0) ? (n_vars-1) : (arity);
  int last_positional = (arity < 0) ?
    ((n_given < max_positional) ? (n_given) : (max_positional)) :
    (n_given);

  /* Handle positional arguments */
  while (i<last_positional) {
    lispval arg = given[i];
    if (ABORTED(arg)) return arg;
    else if (QCHOICEP(arg))
      arg=KNO_QCHOICEVAL(arg);
    else if (KNO_BAD_ARGP(arg))
      return kno_bad_arg(arg,"lambda_call",vars[i]);
    else NO_ELSE;
    if (KNO_MALLOCDP(arg)) {
      kno_incref(arg);
      decref_args = 1;}
    args[i++] = arg;}
  _lambda_stack.stack_args.count = n_given;

  /* If we're synchronized, lock the mutex.
     We include argument defaults within the lock ?? */
  if (synchronized) u8_lock_mutex(&(proc->lambda_lock));

  /* Handle argument defaults */
  lispval *inits = proc->lambda_inits;
 eval_inits:
  if (inits) {
    while (i<max_positional) {
      lispval init_expr = inits[i], init_val = KNO_VOID;
      if (KNO_CONSP(init_expr)) {
	if ( (KNO_PAIRP(init_expr)) ||
	     (KNO_SCHEMAPP(init_expr)) ||
	     (KNO_CHOICEP(init_expr)) )
	  init_val = fast_eval(init_expr,proc_env,lambda_stack,0);
	else init_val = kno_incref(init_expr);}
      else if (KNO_VOIDP(init_expr)) {}
      else if (KNO_SYMBOLP(init_expr))
	init_val = eval_symbol(init_expr,proc_env);
      else if (KNO_LEXREFP(init_expr))
	init_val = eval_lexref(init_expr,proc_env);
      else init_val = init_expr;
      if (ABORTED(init_val)) {
	kno_pop_stack(lambda_stack);
	return init_val;}
      else if (VOIDP(init_val)) args[i++]=init_val;
      else if (KNO_BAD_ARGP(init_val)) {
	lispval err = kno_bad_arg(init_val,"lambda_call/default",init_expr);
	kno_pop_stack(lambda_stack);
	return err;}
      else {
	if (KNO_MALLOCDP(init_val)) decref_args = 1;
	_lambda_stack.stack_args.count=i;
	args[i++]=init_val;}}}

  /* Now accumulate a .rest arg if it's needed */
  if (arity < 0) {
    if (i<n_given) {
      lispval rest_arg = get_rest_arg(given+i,n_given-i);
      decref_args = 1;
      args[i] = rest_arg;}
    else args[i] = KNO_EMPTY_LIST;
    i++;}
  while (i<n_vars) { args[i++] = KNO_VOID;}

  lambda_stack->stack_args.elts = args;
  lambda_stack->stack_args.len = n_vars;
  lambda_stack->stack_args.count = n_vars;
  if (decref_args)
    KNO_STACK_SET_BITS(lambda_stack,KNO_STACK_DECREF_ARGS);

  struct KNO_LEXENV  eval_env = { 0 };
  struct KNO_SCHEMAP bindings = { 0 };
 make_env:
  init_static_env(n_vars,proc_env,
		  &bindings,&eval_env,
		  vars,args);
  lambda_stack->eval_env = &eval_env;
  KNO_STACK_SET(lambda_stack,KNO_STACK_OWNS_ENV);

  lispval scan = proc->lambda_start, result = KNO_VOID;
 eval_body:
  while (KNO_PAIRP(scan)) {
    kno_decref(result);
    lispval body_expr = KNO_CAR(scan); scan = KNO_CDR(scan);
    KNO_STACK_SET_OP(lambda_stack,body_expr,0);
    int tail_arg = ( (tailable) && (KNO_EMPTY_LISTP(scan)) ) ?
      (KNO_TAIL_EVAL) : (0);
    if (KNO_PAIRP(body_expr))
      result = reduce_loop(lambda_stack,tail_arg);
    else result = fast_eval(body_expr,&eval_env,lambda_stack,tail_arg);

    if (ABORTED(result)) break;}

  reset_env(&eval_env);

  /* If we're synchronized, unlock the mutex. */
  if (synchronized) u8_unlock_mutex(&(proc->lambda_lock));

  kno_pop_stack(lambda_stack);

  if (profile)
    kno_profile_update(profile,&before,&start,1);

  return result;
}

KNO_FASTOP kno_stack find_loop_frame(kno_stack stack)
{
  kno_stack scan = stack, loop = NULL;
  while (scan) {
    if (!(KNO_STACK_BITP(scan,KNO_STACK_TAIL_POS))) break;
    if (KNO_STACK_BITP(scan,KNO_STACK_REDUCE_LOOP)) {
      /* We disable this bit on this entry so we can throw through it
	 if there's a 'better' loop higher up the stack.
	 We reset the bit on the outermost loop. */
      KNO_STACK_CLEAR_BITS(scan,KNO_STACK_REDUCE_LOOP);
      loop=scan;}
    scan = scan->stack_caller;}
  if (loop) KNO_STACK_SET_BITS(loop,KNO_STACK_REDUCE_LOOP);
  return loop;
}

KNO_FASTOP void setup_tail_call(kno_stack loop,
				struct KNO_LAMBDA *lambda,
				int n,lispval *args,
				int decref_fn,int decref_args)
{
  /* Restore this bit */
  KNO_STACK_SET_OP(loop,((lispval)lambda),decref_fn);
  int call_width = lambda->fcn_call_width;
  int need_width = (n > call_width) ? (n) : (call_width);
  int loop_width = STACK_WIDTH(loop);
  /* Clean up any existing arguments */
  if (KNO_STACK_BITP(loop,KNO_STACK_DECREF_ARGS)) {
    kno_decref_stackvec(&(loop->stack_args));}
  if (loop_width < need_width) {
    kno_stackvec_grow(&(loop->stack_args),need_width);}
  /* Copy the arguments */
  memcpy(loop->stack_args.elts,args,n*sizeof(lispval));
  loop->stack_args.count = n;
  KNO_STACK_SET_BIT(loop,KNO_STACK_DECREF_ARGS,decref_args);
}

static U8_MAYBE_UNUSED lispval return_tail()
{
  return KNO_TAIL;
}

static lispval bad_evalop(lispval head,lispval op)
{
  if ( (KNO_VOIDP(op)) && (KNO_SYMBOLP(head)) )
    return kno_err(kno_UnboundIdentifier,"eval_expr/op",
		   KNO_SYMBOL_NAME(head),
		   head);
  else if (KNO_VOIDP(op))
    return kno_err(kno_BadEvalOp,"eval_expr/op",NULL,head);
  else return kno_err(kno_BadEvalOp,"eval_expr/op",
		      (KNO_SYMBOLP(head)?(KNO_SYMBOL_NAME(head)):(NULL)),
		      op);
}

static lispval get_evalop(lispval head,kno_lexenv env,
			  kno_stack stack,int *decref_op)
{
  lispval op = KNO_VOID;
  if (KNO_OPCODEP(head)) return head;
  else if (KNO_FCNIDP(head)) op = head;
  else if (KNO_LEXREFP(head))
    op = eval_lexref(head,env);
  else if (KNO_SYMBOLP(head))
    op = eval_symbol(head,env);
  else if (KNO_CONSP(head)) {
    kno_lisp_type headtype = KNO_CONS_TYPEOF(head);
    if ( (headtype == kno_evalfn_type) ||
	 (KNO_APPLICABLE_TYPEP(headtype)) )
      return head;
    else if  (headtype == kno_pair_type)
      op = _eval_expr(KNO_CAR(head),head,env,stack,0);
    else if (headtype == kno_choice_type)
      op = eval_choice(head,env,stack);
    else op = KNO_VOID;}
  else NO_ELSE;
  if (KNO_FCNIDP(op)) op=kno_fcnid_ref(op);
  else if (KNO_MALLOCDP(op)) *decref_op=1;
  else NO_ELSE;
  if (PRED_FALSE(KNO_VOIDP(op)))
    return bad_evalop(head,op);
  else return op;
}

static lispval call_evalfn(lispval evalop,lispval expr,kno_lexenv env,
			   kno_stack stack,int tail)
{
  struct KNO_EVALFN *handler = (kno_evalfn)evalop;
  /* These are evalfns which do all the evaluating themselves */
  int notail = handler->evalfn_notail;
  lispval result = KNO_VOID;
  KNO_STACK_SET_TAIL(stack,tail);
  KNO_START_EVAL(evalfn_stack,handler->evalfn_name,expr,env,stack);
  if (notail) KNO_STACK_SET_TAIL(evalfn_stack,0);
  else KNO_STACK_SET_TAIL(evalfn_stack,tail);
  result = handler->evalfn_handler(expr,env,evalfn_stack);
  kno_pop_stack(evalfn_stack);
  return result;
}

lispval eval_apply(lispval fn,lispval exprs,
		   kno_lexenv env,kno_stack stack,
		   int decref_op,int tail);

/* Evaluating pair expressions */
lispval _eval_expr(lispval head,lispval expr,
		   kno_lexenv env,kno_stack stack,
		   int tail)
{
  lispval result = KNO_VOID, source = expr;
  u8_string old_label = stack->stack_label;

  /* Extract core expression, note source */
  while (head == KNO_SOURCEREF_OPCODE) {
    expr   = KNO_CDR(expr);
    source = KNO_CAR(expr);
    expr   = KNO_CDR(expr);
    if (PRED_TRUE(PAIRP(expr)))
      head = KNO_CAR(expr);
    else return fast_eval(expr,env,stack,tail);}

  KNO_STACK_SET_OP(stack,expr,0);
  stack->eval_source = source;

  if (KNO_OPCODEP(head)) {
    lispval result;
    if (KNO_SPECIAL_OPCODEP(head))
      result=handle_special_opcode(head,KNO_CDR(expr),expr,env,stack,tail);
    else result=eval_apply(head,KNO_CDR(expr),env,stack,0,tail);
    stack->stack_label = old_label;
    return result;}
  else if (KNO_FCNIDP(head)) {
    lispval op = kno_fcnid_ref(head);
    kno_lisp_type op_type = KNO_TYPEOF(op);
    if (op_type == kno_evalfn_type)
      return call_evalfn(op,expr,env,stack,tail);
    else if (PRED_TRUE(KNO_APPLICABLEP(op)))
      return eval_apply(op,KNO_CDR(expr),env,stack,0,tail);
    else return kno_err(kno_BadEvalOp,"eval_expr",NULL,expr);}
  else {
    int decref_op = 0;
    lispval op = get_evalop(head,env,stack,&decref_op);

    if (op == KNO_EMPTY) return op;
    else if (KNO_ABORTED(op)) return op;
    else NO_ELSE;

    kno_lisp_type optype = KNO_TYPEOF(op);
    switch (optype) {
    case kno_opcode_type: case kno_choice_type:
    case kno_cprim_type: case kno_lambda_type:
    case kno_ffi_type: case kno_rpc_type:
      return eval_apply(op,KNO_CDR(expr),env,stack,decref_op,tail);
    case kno_evalfn_type: {
      result = call_evalfn(op,expr,env,stack,tail);
      goto got_result;}
    case kno_macro_type: {
      struct KNO_MACRO *handler = (kno_macro) op;
      stack->stack_label = handler->macro_name;
      stack->stack_file  = handler->macro_filename;
      KNO_STACK_SET_TAIL(stack,tail);
      result = macro_eval(handler,expr,env,stack);
      goto got_result;}
    default:
      if (!(KNO_APPLICABLE_TYPEP(optype)))
	return kno_err(kno_BadEvalOp,"eval_expr",NULL,op);
      else return eval_apply(op,KNO_CDR(expr),env,stack,decref_op,tail);}

  got_result:
    stack->stack_label=old_label;
    if (decref_op) kno_decref(op);
    if (KNO_PRECHOICEP(result))
      return kno_simplify_choice(result);
    else return result;}}

lispval eval_expr(lispval head,lispval expr,
		  kno_lexenv env,kno_stack stack,
		  int tail)
{
  return _eval_expr(head,expr,env,stack,tail);
}

lispval eval_apply(lispval fn,lispval exprs,
		   kno_lexenv env,kno_stack stack,
		   int decref_fn,int tail)
{
  lispval result = KNO_VOID;
  kno_lisp_type fntype = KNO_TYPEOF(fn);
  kno_function f = NULL;
  int argbuf_len = INIT_ARGBUF_LEN;
  int decref_args = 0;
  u8_string old_label = stack->stack_label;
  if (KNO_FUNCTION_TYPEP(fntype)) f = (kno_function) fn;

  stack->stack_label = (f) ? (f->fcn_name) :
    (fntype == kno_opcode_type) ? (opcode_name(fn)) :
    (kno_type2name(fntype));

  /* Straightforward eval/apply */
  KNO_DECL_STACKVEC(args,argbuf_len);
  int nd_args = (KNO_CHOICEP(fn));
  int call_bits = (f) ? (f->fcn_call) :
    (fntype == kno_opcode_type) ?
    (kno_opcode_call_info[KNO_IMMEDIATE_DATA(fn)]) :
    (kno_type_call_info[fntype]);
  int prune = (!((call_bits)&(KNO_CALL_XPRUNE)));
  int iter  = (!((call_bits)&(KNO_CALL_XITER)));

  while (PAIRP(exprs)) {
    lispval arg_expr = pop_arg(exprs);
    lispval arg = eval_arg(arg_expr,env,stack);
    if (KNO_CONSTANTP(arg)) {
      if (KNO_ABORTED(arg)) {
	result = arg;
	goto cleanup_args;}
      else if (arg == KNO_TAIL) {
	kno_seterr(kno_TailArgument,"kno_eval_expr",NULL,arg_expr);
	result = KNO_ERROR;
	goto cleanup_args;}
      else if ( (KNO_EMPTYP(arg)) && (prune) ) {
	result = arg;
	goto cleanup_args;}}
    else if (KNO_CONSP(arg)) {
      if ( (nd_args == 0) && (CHOICEP(arg)) )
	nd_args = 1;
      else if (KNO_QCHOICEP(arg)) {
	lispval unwrapped = KNO_QCHOICEVAL(arg);
	kno_incref(unwrapped);
	kno_decref(arg);
	arg = unwrapped;}
      decref_args = 1;}
    else NO_ELSE;
    kno_stackvec_push(args,arg);}

  /* KNO_STACK_SET_OP(stack,fn); */

  lispval *argbuf = KNO_STACKVEC_ELTS(args);
  int n_args = KNO_STACKVEC_COUNT(args);
  int ndcall = (nd_args) && (iter) && (n_args>0);
  if (fntype == kno_opcode_type) {
    if (!(ndcall))
      result = opcode_dispatch(fn,n_args,argbuf);
    else switch (n_args) {
      case 1: result = opcode_1choice_loop(fn,argbuf[0]); break;
      case 2: result = opcode_2choice_loop(fn,argbuf[0],argbuf[1]); break;
      default: {
	lispval d_args[n_args];
	lispval combined = KNO_EMPTY;
	opcode_choice_loop(&combined,fn,0,n_args,argbuf,d_args);
	result = combined;}}}
  else if ( (fntype == kno_lambda_type) && (!(ndcall)) ) {
    struct KNO_LAMBDA *proc = (kno_lambda) fn;
  lambda:
    if (PRED_TRUE(lambda_check_arity(proc,n_args))) {
      kno_stack loop = (tail) ? (find_loop_frame(stack)) : (NULL);
      if (loop) {
      tail:
	setup_tail_call(loop,proc,n_args,argbuf,decref_fn,decref_args);
	stack->stack_label = old_label;
	return KNO_TAIL;}
      else result = lambda_call(stack,proc,n_args,(kno_argvec)argbuf,tail);}
    else result = KNO_ERROR;}
  else if (ndcall)
    result = kno_call(stack,fn,n_args,argbuf);
  else if (CHOICEP(fn))
    result = kno_call(stack,fn,n_args,argbuf);
  else result = kno_dcall(stack,fn,n_args,argbuf);

 cleanup_args:
  /* We jump here if we haven't yet put *args* on the stack, i.e.
     there was an error while processing them. */
  if (decref_fn) kno_decref(fn);
  if (decref_args) kno_decref_stackvec(&_args);
  kno_free_stackvec(&_args);

  stack->stack_label = old_label;

 if (KNO_PRECHOICEP(result))
    return kno_simplify_choice(result);
  else return result;
}


/* Opcode evaluation */

KNO_EXPORT lispval kno_eval_arg(lispval expr,kno_lexenv env)
{
  return fast_eval(expr,env,kno_stackptr,0);
}

/* The eval/reduce loop */

static lispval reduce_loop(kno_stack loop_stack,int tail)
{
  lispval result = KNO_TAIL;
  lispval op	 = loop_stack->stack_op;
  kno_lisp_type op_type = KNO_TYPEOF(op);
  KNO_STACK_SET_BITS(loop_stack,KNO_STACK_REDUCE_LOOP);
  KNO_STACK_SET_TAIL(loop_stack,tail);
  if (op_type == kno_pair_type)
    loop_stack->eval_source  = op;
  if (KNO_VOIDP(loop_stack->eval_context))
    loop_stack->eval_context  = op;
  kno_lexenv env = loop_stack->eval_env;
  while (result == KNO_TAIL) {
    if (op_type == kno_lambda_type) {
      int decref = (KNO_STACK_BITP(loop_stack,KNO_STACK_DECREF_OP));
      if (decref) KNO_STACK_CLEAR(loop_stack,KNO_STACK_DECREF_OP);
      loop_stack->stack_op=KNO_VOID;
      result = lambda_call
	(loop_stack,(kno_lambda)op,
	 KNO_STACK_ARGCOUNT(loop_stack),
	 (kno_argvec)KNO_STACK_ARGS(loop_stack),
	 1);
      if (decref) kno_decref(op);}
    else if (op_type == kno_pair_type)
      /* We pass a tail arg of 1 here because it's okay 
	 to return to this loop stack */
      result = kno_eval_expr(KNO_CAR(op),op,env,loop_stack,1);
    else if (KNO_APPLICABLE_TYPEP(op_type))
      result = kno_call(loop_stack,op,STACK_ARGCOUNT(loop_stack),
			STACK_ARGS(loop_stack));
    else if (op_type == kno_schemap_type)
      result = eval_schemap(op,env,loop_stack);
    else if (op_type == kno_choice_type)
      result = eval_choice(op,env,loop_stack);
    else result = kno_incref(op);
    if (result == KNO_TAIL) {
      /* Check if we were thrown through */
      if (KNO_STACK_BITP(loop_stack,KNO_STACK_REDUCE_LOOP)) {
	op      = loop_stack->stack_op;
	env     = loop_stack->eval_env;
	op_type = KNO_TYPEOF(op);}
      else break;}}
  KNO_STACK_CLEAR_BITS(loop_stack,KNO_STACK_REDUCE_LOOP);
  if (ABORTED(result)) return result;
  else if (result == KNO_TAIL) return result;
  else if (KNO_STACK_BITP(loop_stack,KNO_STACK_VOID_VAL)) {
    kno_decref(result);
    return KNO_VOID;}
  else return result;
}

static int unparse_lexref(u8_output out,lispval lexref)
{
  int code = KNO_GET_IMMEDIATE(lexref,kno_lexref_type);
  int up = code/32, across = code%32;
  u8_printf(out,"#<LEXREF ^%d+%d>",up,across);
  return 1;
}

static ssize_t write_lexref_dtype(struct KNO_OUTBUF *out,lispval x)
{
  int code = KNO_GET_IMMEDIATE(x,kno_lexref_type);
  int up = code/32, across = code%32;
  unsigned char buf[100], *tagname="%LEXREF";
  struct KNO_OUTBUF tmp = { 0 };
  KNO_INIT_OUTBUF(&tmp,buf,100,0);
  kno_write_byte(&tmp,dt_compound);
  kno_write_byte(&tmp,dt_symbol);
  kno_write_4bytes(&tmp,7);
  kno_write_bytes(&tmp,tagname,7);
  kno_write_byte(&tmp,dt_vector);
  kno_write_4bytes(&tmp,2);
  kno_write_byte(&tmp,dt_fixnum);
  kno_write_4bytes(&tmp,up);
  kno_write_byte(&tmp,dt_fixnum);
  kno_write_4bytes(&tmp,across);
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  kno_write_bytes(out,tmp.buffer,n_bytes);
  kno_close_outbuf(&tmp);
  return n_bytes;
}

static int unparse_coderef(u8_output out,lispval coderef)
{
  long long off = KNO_GET_IMMEDIATE(coderef,kno_coderef_type);
  u8_printf(out,"#<CODEREF %lld>",off);
  return 1;
}

static ssize_t write_coderef_dtype(struct KNO_OUTBUF *out,lispval x)
{
  int offset = KNO_GET_IMMEDIATE(x,kno_coderef_type);
  unsigned char buf[100], *tagname="%CODEREF";
  struct KNO_OUTBUF tmp = { 0 };
  KNO_INIT_OUTBUF(&tmp,buf,100,0);
  kno_write_byte(&tmp,dt_compound);
  kno_write_byte(&tmp,dt_symbol);
  kno_write_4bytes(&tmp,strlen(tagname));
  kno_write_bytes(&tmp,tagname,strlen(tagname));
  kno_write_byte(&tmp,dt_fixnum);
  kno_write_4bytes(&tmp,offset);
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  kno_write_bytes(out,tmp.buffer,n_bytes);
  kno_close_outbuf(&tmp);
  return n_bytes;
}

static void set_opcode_name(lispval opcode,u8_string name)
{
  int off = KNO_OPCODE_NUM(opcode);
  u8_string constname=u8_string_append("#",name,NULL);
  kno_opcode_names[off]=name;
  kno_add_constname(constname,opcode);
  u8_free(constname);
}

static void set_opcode_info(lispval opcode,u8_string name,
			    unsigned char call_flags)
{
  int off = KNO_OPCODE_NUM(opcode);
  u8_string constname=u8_string_append("#",name,NULL);
  kno_opcode_names[off] = name;
  kno_opcode_call_info[off] = call_flags;
  kno_add_constname(constname,opcode);
  u8_free(constname);
}

KNO_EXPORT lispval kno_get_opcode(u8_string name)
{
  int i = 0; while (i<kno_opcodes_length) {
    u8_string opname = kno_opcode_names[i];
    if ((opname)&&(strcasecmp(name,opname)==0))
      return KNO_OPCODE(i);
    else i++;}
  return KNO_FALSE;
}

static int unparse_opcode(u8_output out,lispval opcode)
{
  int opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if (opcode_offset>kno_opcodes_length) {
    u8_printf(out,"#<INVALIDOPCODE>");
    return 1;}
  else if (kno_opcode_names[opcode_offset]==NULL) {
    u8_printf(out,"#<OPCODE_0x%x>",opcode_offset);
    return 1;}
  else {
    u8_printf(out,"#<OPCODE_%s>",kno_opcode_names[opcode_offset]);
    return 1;}
}

static int validate_opcode(lispval opcode)
{
  long opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if ((opcode_offset>=0) && (opcode_offset<kno_opcodes_length))
    return 1;
  else return 0;
}

/* Making some functions */

KNO_EXPORT lispval kno_make_evalfn(u8_string name,int flags,
				   kno_eval_handler fn)
{
  struct KNO_EVALFN *f = u8_alloc(struct KNO_EVALFN);
  KNO_INIT_CONS(f,kno_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_filename = NULL;
  f->evalfn_handler = fn;
  f->evalfn_notail = U8_BITP(flags,KNO_EVALFN_NOTAIL);
  return LISP_CONS(f);
}

KNO_EXPORT void kno_new_evalfn(lispval mod,u8_string name,u8_string cname,
			       u8_string filename,u8_string doc,int flags,
			       kno_eval_handler fn)
{
  struct KNO_EVALFN *f = u8_alloc(struct KNO_EVALFN);
  KNO_INIT_CONS(f,kno_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_handler = fn;
  f->evalfn_filename = filename;
  f->evalfn_cname = u8_strdup(cname);
  f->evalfn_documentation = u8_strdup(doc);
  f->evalfn_notail = U8_BITP(flags,KNO_EVALFN_NOTAIL);
  kno_store(mod,kno_getsym(name),LISP_CONS(f));
  f->evalfn_moduleid = kno_get(mod,KNOSYM_MODULEID,KNO_VOID);
  kno_decref(LISP_CONS(f));
}

/* The Evaluator */

static lispval eval_evalfn(lispval x,kno_lexenv env,kno_stack stack)
{
  lispval expr_expr = kno_get_arg(x,1);
  lispval expr = kno_eval(expr_expr,env,stack,0);
  if (KNO_ABORTED(expr)) return expr;
  lispval result = kno_eval(expr,env,stack,0);
  kno_decref(expr);
  return result;
}

static lispval boundp_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"boundp_evalfn",NULL,expr);
  else {
    lispval val = kno_symeval(symbol,env);
    if (KNO_ABORTED(val))
      return val;
    else if (VOIDP(val))
      return KNO_FALSE;
    else if (val == KNO_UNBOUND)
      return KNO_FALSE;
    else {
      kno_decref(val);
      return KNO_TRUE;}}
}

static lispval unboundp_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"unboundp_evalfn",NULL,expr);
  else {
    lispval val = kno_symeval(symbol,env);
    if (KNO_ABORTED(val))
      return val;
    else if (VOIDP(val))
      return KNO_TRUE;
    else if (val == KNO_UNBOUND)
      return KNO_TRUE;
    else {
      kno_decref(val);
      return KNO_FALSE;}}
}

static lispval definedp_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"definedp_evalfn",NULL,expr);
  else {
    lispval val = kno_symeval(symbol,env);
    if (KNO_ABORTED(val))
      return val;
    if ( (VOIDP(val)) || (val == KNO_DEFAULT_VALUE) )
      return KNO_FALSE;
    else if (val == KNO_UNBOUND)
      return KNO_FALSE;
    else {
      kno_decref(val);
      return KNO_TRUE;}}
}

static lispval modref_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval module = kno_get_arg(expr,1);
  lispval symbol = kno_get_arg(expr,2);
  if ((VOIDP(module))||(VOIDP(module)))
    return kno_err(kno_SyntaxError,"modref_evalfn",NULL,expr);
  else if (!(HASHTABLEP(module)))
    return kno_type_error("module hashtable","modref_evalfn",module);
  else if (!(SYMBOLP(symbol)))
    return kno_type_error("symbol","modref_evalfn",symbol);
  else return kno_hashtable_get((kno_hashtable)module,symbol,KNO_UNBOUND);
}

static lispval constantp_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval to_eval = kno_get_arg(expr,1);
  if (KNO_SYMBOLP(to_eval)) {
    lispval v = kno_symeval(to_eval,env);
    if	(v == KNO_NULL)
      return kno_err(kno_BadPtr,"constantp_evalfn","NULL pointer",to_eval);
    else if (KNO_ABORTED(v))
      return v;
    else if (KNO_VOIDP(v))
      return kno_err(kno_UnboundIdentifier,"constantp_evalfn",NULL,to_eval);
    else if (KNO_CONSTANTP(v))
      return KNO_TRUE;
    else {
      kno_decref(v);
      return KNO_FALSE;}}
  else {
    lispval v = fast_eval(to_eval,env,_stack,0);
    if	(v == KNO_NULL)
      return kno_err(kno_BadPtr,"constantp_evalfn","NULL pointer",to_eval);
    else if (KNO_ABORTED(v))
      return v;
    else if (KNO_CONSTANTP(v))
      return KNO_TRUE;
    else {
      kno_decref(v);
      return KNO_FALSE;}}
}

static lispval voidp_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval to_eval = kno_get_arg(expr,1);
  if (KNO_SYMBOLP(to_eval)) {
    lispval v = kno_symeval(to_eval,env);
    if (KNO_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == KNO_UNBOUND ) )
      return KNO_TRUE;
    else if  (v == KNO_NULL) {
      return KNO_TRUE;}
    else {
      kno_decref(v);
      return KNO_FALSE;}}
  else {
    lispval v = fast_eval(to_eval,env,_stack,0);
    if (KNO_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == KNO_UNBOUND ) )
      return KNO_TRUE;
    else {
      kno_decref(v);
      return KNO_FALSE;}}
}

static lispval defaultp_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval to_eval = kno_get_arg(expr,1);
  if (KNO_SYMBOLP(to_eval)) {
    lispval v = kno_symeval(to_eval,env);
    if (KNO_ABORTED(v)) return v;
    else if (v == KNO_DEFAULT)
      return KNO_TRUE;
    else if ( ( v == VOID ) || ( v == KNO_UNBOUND ) )
      return kno_err(kno_UnboundIdentifier,"defaultp_evalfn",NULL,to_eval);
    else if  (v == KNO_NULL)
      return kno_err(kno_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else {
      kno_decref(v);
      return KNO_FALSE;}}
  else {
    lispval v = fast_eval(to_eval,env,_stack,0);
    if	(v == KNO_NULL)
      return kno_err(kno_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else if (KNO_ABORTED(v)) return v;
    else if (v == KNO_DEFAULT) return KNO_TRUE;
    else {
      kno_decref(v);
      return KNO_FALSE;}}
}

static lispval badp_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval to_eval = kno_get_arg(expr,1);
  if (KNO_SYMBOLP(to_eval)) {
    lispval v = kno_symeval(to_eval,env);
    if (KNO_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == KNO_UNBOUND ) )
      return KNO_TRUE;
    else if  ( (v == KNO_NULL) || (! (KNO_CHECK_PTR(v)) ) ) {
      u8_log(LOG_WARN,kno_BadPtr,"Bad pointer value 0x%llx for %q",v,to_eval);
      return KNO_TRUE;}
    else return KNO_FALSE;}
  else {
    lispval v = kno_eval(to_eval,env,_stack,0);
    if (KNO_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == KNO_UNBOUND ) )
      return KNO_TRUE;
    else if  ( (v == KNO_NULL) || (! (KNO_CHECK_PTR(v)) ) ) {
      u8_log(LOG_WARN,kno_BadPtr,"Bad pointer value 0x%llx for %q",v,to_eval);
      return KNO_TRUE;}
    else {
      kno_decref(v);
      return KNO_FALSE;}}
}

static lispval env_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return (lispval)kno_copy_env(env);
}

static lispval env_reset_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  if ( (env->env_copy) && (env != env->env_copy) ) {
    lispval tofree = (lispval) env;
    env->env_copy=NULL;
    kno_decref(tofree);}
  return VOID;
}

static lispval symbol_boundp_evalfn(lispval expr,kno_lexenv env,
				    kno_stack stack)
{
  lispval symbol_expr = kno_get_arg(expr,1), symbol;
  kno_lexenv use_env = NULL;
  if (VOIDP(symbol_expr))
    return kno_err(kno_SyntaxError,"symbol_boundp_evalfn",NULL,expr);
  else symbol = fast_eval(symbol_expr,env,stack,0);
  if (KNO_ABORTED(symbol))
    return symbol;
  else if (!(KNO_SYMBOLP(symbol))) {
    lispval err = kno_err("NotASymbol","symbol_boundp_evalfn",NULL,symbol);
    kno_decref(symbol);
    return err;}
  else NO_ELSE;
  lispval env_expr = kno_get_arg(expr,2), env_value = VOID;
  if (VOIDP(env_expr))
    use_env = env;
  else {
    env_value = fast_eval(env_expr,env,stack,0);
    if (KNO_FALSEP(env_value))
      use_env = env;
    else if (KNO_LEXENVP(env_value))
      use_env = (kno_lexenv) env_value;
    else {
      lispval err = kno_err("NotAnEnvironment","symbol_boundp_evalfn",
			    NULL,env_value);
      kno_decref(env_value);
      return err;}}
  lispval v = kno_symeval(symbol,use_env);
  if (KNO_ABORTED(v)) {
    kno_decref(env_value);
    return v;}
  else if ( (VOIDP(v)) || (v == KNO_DEFAULT_VALUE) || (v == KNO_UNBOUND) ) {
    kno_decref(env_value);
    return KNO_FALSE;}
  else {
    kno_decref(env_value);
    kno_decref(v);
    return KNO_TRUE;}
}

/* Withenv forms */

static lispval withenv(kno_stack stack,
		       lispval expr,kno_lexenv env,
		       kno_lexenv consed_env,
		       u8_context cxt)
{
  lispval bindings = kno_get_arg(expr,1);
  if (VOIDP(bindings))
    return kno_err(kno_TooFewExpressions,cxt,NULL,expr);
  else if ((NILP(bindings))||(FALSEP(bindings))) {}
  else if (PAIRP(bindings)) {
    KNO_DOLIST(varval,bindings) {
      if ((PAIRP(varval))&&(SYMBOLP(KNO_CAR(varval)))&&
	  (PAIRP(KNO_CDR(varval)))&&
	  (NILP(KNO_CDR(KNO_CDR(varval))))) {
	lispval var = KNO_CAR(varval);
	lispval val = kno_eval(KNO_CADR(varval),env,stack,0);
	if (KNO_ABORTED(val)) return val;
	int rv = kno_bind_value(var,val,consed_env);
	kno_decref(val);
	if (rv<0)
	  return KNO_ERROR;}
      else return kno_err(kno_SyntaxError,cxt,NULL,expr);}}
  else if (TABLEP(bindings)) {
    int abort = 0;
    lispval keys = kno_getkeys(bindings);
    DO_CHOICES(key,keys) {
      if (SYMBOLP(key)) {
	int rv = 0;
	lispval value = kno_get(bindings,key,VOID);
	if (!(VOIDP(value)))
	  rv = kno_bind_value(key,value,consed_env);
	kno_decref(value);
	if (rv<0) {
	  abort = 1;
	  KNO_STOP_DO_CHOICES;
	  break;}}
      else {
	kno_seterr(kno_SyntaxError,cxt,NULL,expr);
	abort = 1;
	KNO_STOP_DO_CHOICES;
	break;}}
    kno_decref(keys);
    if (abort) return KNO_ERROR;}
  else return kno_err(kno_SyntaxError,cxt,NULL,expr);
  /* Execute the body */ {
    lispval result = VOID;
    lispval body = kno_get_body(expr,2);
    KNO_DOLIST(elt,body) {
      kno_decref(result);
      result = kno_eval(elt,consed_env,stack,0);
      if (KNO_ABORTED(result)) {
	return result;}}
    return result;}
}

static lispval withenv_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  kno_lexenv consed_env = kno_working_lexenv();
  KNO_START_EVALX(envstack,"withenv",expr,consed_env,_stack,7);
  KNO_STACK_SET(envstack,KNO_STACK_FREE_ENV);
  lispval result = withenv(envstack,expr,env,consed_env,"WITHENV");
  kno_pop_stack(envstack);
  return result;
}

/* Eval/apply related primitives */

/* Table functions used everywhere (:)) */

static lispval lispenv_get(lispval e,lispval s,lispval d)
{
  lispval result = symeval(s,KNO_XENV(e));
  if (VOIDP(result)) return kno_incref(d);
  else return result;
}
static int lispenv_store(lispval e,lispval s,lispval v)
{
  return kno_bind_value(s,v,KNO_XENV(e));
}

static int lispenv_add(lispval e,lispval s,lispval v)
{
  return add_to_value(s,v,KNO_XENV(e));
}

/* Getting documentation */

KNO_EXPORT u8_string kno_get_documentation(lispval x)
{
  lispval proc = (KNO_FCNIDP(x)) ? (kno_fcnid_ref(x)) : (x);
  kno_lisp_type proctype = KNO_TYPEOF(proc);
  if (proctype == kno_lambda_type) {
    struct KNO_LAMBDA *lambda = (kno_lambda)proc;
    if (lambda->fcn_doc)
      return u8_strdup(lambda->fcn_doc);
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,120);
      lispval arglist = lambda->lambda_arglist, scan = arglist;
      if (lambda->fcn_name)
	u8_puts(&out,lambda->fcn_name);
      else u8_puts(&out,"λ");
      while (PAIRP(scan)) {
	lispval arg = KNO_CAR(scan);
	if (SYMBOLP(arg))
	  u8_printf(&out," %ls",SYM_NAME(arg));
	else if ((PAIRP(arg))&&(SYMBOLP(KNO_CAR(arg))))
	  u8_printf(&out," [%ls]",SYM_NAME(KNO_CAR(arg)));
	else u8_printf(&out," %q",arg);
	scan = KNO_CDR(scan);}
      if (SYMBOLP(scan))
	u8_printf(&out," [%ls...]",SYM_NAME(scan));
      lambda->fcn_doc = out.u8_outbuf;
      lambda->fcn_free |= KNO_FCN_FREE_DOC;
      return u8_strdup(out.u8_outbuf);}}
  else if (kno_isfunctionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_GETFUNCTION(proc);
    return u8_strdup(f->fcn_doc);}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = (kno_evalfn)proc;
    return u8_strdup(sf->evalfn_documentation);}
  else return NULL;
}

/* Helpful forms and functions */

static lispval void_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = KNO_CDR(expr);
  KNO_DOLIST(subex,body) {
    lispval v = fast_eval(subex,env,_stack,0);
    if (KNO_ABORTED(v))
      return v;
    else kno_decref(v);}
  return VOID;
}

static lispval null_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  /* This is for breaking things */
  return KNO_NULL;
}

static lispval break_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = KNO_CDR(expr);
  KNO_DOLIST(subex,body) {
    lispval v = fast_eval(subex,env,_stack,0);
    if (KNO_BREAKP(v))
      return KNO_BREAK;
    else if (KNO_ABORTED(v))
      return v;
    else kno_decref(v);}
  return KNO_BREAK;
}

static lispval default_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  lispval default_expr = kno_get_arg(expr,2);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"default_evalfn",NULL,kno_incref(expr));
  else if (VOIDP(default_expr))
    return kno_err(kno_SyntaxError,"default_evalfn",NULL,kno_incref(expr));
  else {
    lispval val = kno_symeval(symbol,env);
    if (KNO_ABORTED(val)) return val;
    else if (VOIDP(val))
      return fast_eval(default_expr,env,_stack,0);
    else if (val == KNO_UNBOUND)
      return fast_eval(default_expr,env,_stack,0);
    else return val;}
}

/* External versions of internal functions */

KNO_EXPORT lispval kno_get_arg(lispval expr,int i)
{
  return get_arg(expr,i);
}
KNO_EXPORT lispval kno_get_body(lispval expr,int i)
{
  return get_body(expr,i);
}

KNO_EXPORT lispval kno_eval_symbol(lispval sym,kno_lexenv env)
{
  return eval_symbol(sym,env);
}

KNO_EXPORT
lispval kno_eval_expr(lispval head,lispval expr,
		      kno_lexenv env,kno_stack s,
		      int tail)
{
  return eval_expr(head,expr,env,s,tail);
}

KNO_EXPORT
lispval _kno_reduce_loop(kno_stack stack,int tail)
{
  return reduce_loop(stack,tail);
}

/* Initialization */

void kno_init_module_tables(void);

#include "opcode_info.h"

static void init_types_and_tables()
{
  struct KNO_TABLEFNS *fns = u8_zalloc(struct KNO_TABLEFNS);
  fns->get = lispenv_get; fns->store = lispenv_store;
  fns->add = lispenv_add; fns->drop = NULL; fns->test = NULL;

  kno_tablefns[kno_lexenv_type]=fns;
  kno_recyclers[kno_evalfn_type]=recycle_evalfn;

  kno_unparsers[kno_evalfn_type]=unparse_evalfn;
  kno_dtype_writers[kno_evalfn_type]=write_evalfn_dtype;

  kno_unparsers[kno_lexref_type]=unparse_lexref;
  kno_dtype_writers[kno_lexref_type]=write_lexref_dtype;
  kno_type_names[kno_lexref_type]=_("lexref");

  kno_unparsers[kno_coderef_type]=unparse_coderef;
  kno_dtype_writers[kno_coderef_type]=write_coderef_dtype;
  kno_type_names[kno_coderef_type]=_("coderef");

  kno_type_names[kno_opcode_type]=_("opcode");
  kno_unparsers[kno_opcode_type]=unparse_opcode;
  kno_immediate_checkfns[kno_opcode_type]=validate_opcode;

  _kno_comment_symbol = comment_symbol = kno_intern("comment");
  source_symbol = kno_intern("%source");
  fcnids_symbol = kno_intern("%fcnids");
  sourceref_tag = kno_intern("%sourceref");

  kno_init_module_tables();
  init_opcode_names();
}

static void init_localfns()
{
  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"EVAL",eval_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"BOUND?",boundp_evalfn,
		 "`(BOUND? *sym*)` returns true if *sym* (not evaluated) "
		 "is bound in the current environment.");
  kno_def_evalfn(kno_scheme_module,"UNBOUND?",unboundp_evalfn,
		 "`(UNBOUND? *sym*)` returns true if *sym* (*not evaluated*) "
		 "is *not* bound in the current environment.");
  kno_def_evalfn(kno_scheme_module,"DEFINED?",definedp_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"VOID?",voidp_evalfn,
		 "`(VOID? *expr*)` returns true if evaluating *expr* "
		 "returns the **VOID** value.");
  kno_def_evalfn(kno_scheme_module,"DEFAULT?",defaultp_evalfn,
		 "`(DEFAULT? *expr*)` returns true if evaluating *expr* "
		 "returns the **DEFAULT** value token.");
  kno_def_evalfn(kno_scheme_module,"CONSTANT?",constantp_evalfn,
		 "`(CONSTANT? *expr*)` returns true if evaluating *expr* "
		 "returns a Scheme constant.");
  kno_def_evalfn(kno_scheme_module,"BAD?",badp_evalfn,
		 "`(BAD? *expr*)` returns true if evaluating *expr* "
		 "returns an invalid pointer");
  kno_def_evalfn(kno_scheme_module,"QUOTE",quote_evalfn,
		 "`(QUOTE *x*)` returns the subexpression *x*, "
		 "which is *not* evaluated.");
  kno_def_evalfn(kno_scheme_module,"FCN/ALIAS",fcnalias_evalfn,
		 "`(FCN/ALIAS *sym*)` returns a *fcnid* pointer aliasing the "
		 "definition of *sym* in the current environment.");
  kno_def_evalfn(kno_scheme_module,"%ENV",env_evalfn,
		 "`(%ENV)` returns the current lexical environment.");
  kno_def_evalfn(kno_scheme_module,"%MODREF",modref_evalfn,
		 "`(%MODREF *modobj* *symbol*) returns the binding of "
		 "*symbol* in the module *modobj*, neither of which are "
		 "evaluated. This is intended for use in automatically "
		 "generated/optimized code");
  kno_def_evalfn(kno_scheme_module,"SYMBOL-BOUND?",symbol_boundp_evalfn,
		 "`(SYMBOL-BOUND? *sym* [*env*])` returns #t "
		 "if *sym* is bound in *env*, which defaults to "
		 "the current environment.");

  kno_def_evalfn(kno_scheme_module,"%ENV/RESET!",env_reset_evalfn,
		 "Resets the cached dynamic copy of the current "
		 "environment (if any). This means that procedures "
		 "closed in the current environment will not be "
		 "effected by future changes");

  kno_def_evalfn(kno_scheme_module,"WITHENV",withenv_evalfn,
		 "*undocumented*");

  kno_def_evalfn(kno_scheme_module,"VOID",void_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"!!!NULL!!!",null_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"BREAK",break_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DEFAULT",default_evalfn,
		 "*undocumented*");

  kno_register_config
    ("TAILCALL",
     "Enable/disable tail recursion in the Scheme evaluator. "
     "This may cause various source packages to break in some "
     "or all cases.",
     kno_boolconfig_get,kno_boolconfig_set,
     &kno_enable_tail_calls);

}

KNO_EXPORT void kno_init_errfns_c(void);
KNO_EXPORT void kno_init_threads_c(void);
KNO_EXPORT void kno_init_conditionals_c(void);
KNO_EXPORT void kno_init_iterators_c(void);
KNO_EXPORT void kno_init_choicefns_c(void);
KNO_EXPORT void kno_init_binders_c(void);
KNO_EXPORT void kno_init_lambdas_c(void);
KNO_EXPORT void kno_init_macros_c(void);
KNO_EXPORT void kno_init_compoundfns_c(void);
KNO_EXPORT void kno_init_evalops_c(void);
KNO_EXPORT void kno_init_coreops_c(void);
KNO_EXPORT void kno_init_tableprims_c(void);
KNO_EXPORT void kno_init_stringprims_c(void);
KNO_EXPORT void kno_init_dbprims_c(void);
KNO_EXPORT void kno_init_seqprims_c(void);
KNO_EXPORT void kno_init_modules_c(void);
KNO_EXPORT void kno_init_loadmods_c(void);
KNO_EXPORT void kno_init_load_c(void);
KNO_EXPORT void kno_init_getsource_c(void);
KNO_EXPORT void kno_init_logprims_c(void);
KNO_EXPORT void kno_init_portprims_c(void);
KNO_EXPORT void kno_init_streamprims_c(void);
KNO_EXPORT void kno_init_dtypeprims_c(void);
KNO_EXPORT void kno_init_xtypeprims_c(void);
KNO_EXPORT void kno_init_timeprims_c(void);
KNO_EXPORT void kno_init_sysprims_c(void);
KNO_EXPORT void kno_init_arith_c(void);
KNO_EXPORT void kno_init_reflection_c(void);
KNO_EXPORT void kno_init_reqstate_c(void);
KNO_EXPORT void kno_init_regex_c(void);
KNO_EXPORT void kno_init_promises_c(void);
KNO_EXPORT void kno_init_quasiquote_c(void);
KNO_EXPORT void kno_init_struct_eval_c(void);
KNO_EXPORT void kno_init_sqldbprims_c(void);
KNO_EXPORT void kno_init_history_c(void);
KNO_EXPORT void kno_init_eval_appenv_c(void);
KNO_EXPORT void kno_init_eval_moduleops_c(void);
KNO_EXPORT void kno_init_eval_getopt_c(void);
KNO_EXPORT void kno_init_eval_debug_c(void);
KNO_EXPORT void kno_init_eval_testops_c(void);
KNO_EXPORT void kno_init_configops_c(void);
KNO_EXPORT void kno_init_neteval_c(void);

static void init_eval_core()
{
  init_localfns();

  kno_init_configops_c();
  kno_init_eval_getopt_c();
  kno_init_eval_debug_c();
  kno_init_eval_testops_c();
  kno_init_evalops_c();
  kno_init_neteval_c();
  kno_init_tableprims_c();
  kno_init_loadmods_c();
  kno_init_modules_c();
  kno_init_arith_c();
  kno_init_threads_c();
  kno_init_errfns_c();
  kno_init_coreops_c();
  kno_init_getsource_c();
  kno_init_load_c();
  kno_init_conditionals_c();
  kno_init_iterators_c();
  kno_init_choicefns_c();
  kno_init_binders_c();
  kno_init_lambdas_c();
  kno_init_macros_c();
  kno_init_compoundfns_c();
  kno_init_quasiquote_c();
  kno_init_struct_eval_c();
  kno_init_reflection_c();
  kno_init_reqstate_c();

  kno_init_eval_appenv_c();

  kno_init_promises_c();

  kno_init_regex_c();

  kno_init_history_c();

  kno_init_stringprims_c();
  kno_init_dbprims_c();
  kno_init_seqprims_c();
  kno_init_logprims_c();
  kno_init_portprims_c();
  kno_init_streamprims_c();
  kno_init_dtypeprims_c();
  kno_init_xtypeprims_c();
  kno_init_timeprims_c();
  kno_init_sysprims_c();
  kno_init_sqldbprims_c();

  u8_threadcheck();

  kno_finish_module(kno_scheme_module);
  kno_finish_module(kno_db_module);
  kno_finish_module(kno_io_module);
  kno_finish_module(kno_sys_module);
}

KNO_EXPORT int kno_load_scheme()
{
  return kno_init_scheme();
}

KNO_EXPORT int kno_init_scheme()
{
  if (kno_scheme_initialized) return kno_scheme_initialized;
  else {
    kno_scheme_initialized = 401*kno_init_storage()*u8_initialize();

    init_types_and_tables();

    u8_register_source_file(KNO_EVAL_H_INFO);
    u8_register_source_file(_FILEINFO);

    init_eval_core();

    /* This sets up the kno_atexit handler for recycling the
       application environment. Consequently, it needs to be called
       before setting up any kno_atexit handlers which might use the
       application environment. */
    kno_setup_app_env();}

  return kno_scheme_initialized;
}

static void link_local_cprims()
{
}
