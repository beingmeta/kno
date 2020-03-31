/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES      (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_FCNIDS       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV       (!(KNO_AVOID_INLINE))

#define KNO_INLINE_EVAL    (!(KNO_AVOID_INLINE))

#ifndef KNO_EVAL_CALLBUF_WIDTH
#define KNO_EVAL_CALLBUF_WIDTH 11
#endif

#ifndef KNO_EVAL_CALLBUF_DELTA
#define KNO_EVAL_CALLBUF_DELTA 23
#endif

#define INIT_ARGBUF_LEN 7

/* Evaluator defines */

#ifndef KNO_WITH_TAIL_EVAL
#define KNO_WITH_TAIL_EVAL 1
#endif

#ifndef KNO_WITH_TAIL_CALLS
#define KNO_WITH_TAIL_CALLS 0
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

#include "../apply/apply_internals.h"
#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>
#include <sys/resource.h>
#include "kno/profiles.h"

int kno_scheme_initialized = 0;

int kno_enable_tail_calls = 1;

lispval _kno_comment_symbol, fcnids_symbol, sourceref_tag;
static lispval consfn_symbol, stringfn_symbol;

static lispval quote_symbol, comment_symbol, moduleid_symbol, source_symbol;

static u8_condition ExpiredThrow=_("Continuation is no longer valid");
static u8_condition DoubleThrow=_("Continuation used twice");
static u8_condition LostThrow=_("Lost invoked continuation");

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
  kno_BadOpcode=_("Bad opcode in compiled code"),
  kno_ReadOnlyEnv=_("Read only environment");

static u8_condition BadExpressionHead=_("BadExpressionHead");

#define head_label(head) \
  ( (SYMBOLP(head)) ? (SYM_NAME(head)) : \
    (KNO_OPCODEP(head)) ? (opcode_name(head)) : \
    (NULL) )

/* Reading from expressions */

KNO_EXPORT lispval _kno_get_arg(lispval expr,int i)
{
  return kno_get_arg(expr,i);
}
KNO_EXPORT lispval _kno_get_body(lispval expr,int i)
{
  return kno_get_body(expr,i);
}

KNO_EXPORT lispval _kno_eval_symbol(lispval sym,kno_lexenv env)
{
  return kno_eval_symbol(sym,env);
}

/* Top level functions */

KNO_EXPORT lispval _kno_eval(lispval x,kno_lexenv env,
				 kno_stack stack,
				 int tail)
{
  return kno_eval(x,env,stack,tail);
}

#define ND_ARGP(v) ((CHOICEP(v))||(QCHOICEP(v)))

KNO_FASTOP int commentp(lispval arg)
{
  return (PRED_FALSE((PAIRP(arg)) && (KNO_EQ(KNO_CAR(arg),comment_symbol))));
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

/* Opcodes */

u8_string kno_opcode_names[0xF00]={NULL};
int kno_opcodes_length = 0xF00;

static u8_string opcode_name(lispval opcode)
{
  long opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if ((opcode_offset<kno_opcodes_length) &&
      (kno_opcode_names[opcode_offset]))
    return kno_opcode_names[opcode_offset];
  else return "anonymous_opcode";
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

KNO_EXPORT
lispval kno_eval_exprs(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  return eval_body(body,env,stack,"kno_eval_exprs",NULL,tail);
}

/* Symbol lookup */

KNO_EXPORT lispval _kno_symeval(lispval sym,kno_lexenv env)
{
  return kno_symeval(sym,env);
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
    lispval env_arg = kno_stack_eval(env_expr,env,stack);
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

static lispval schemap_eval(lispval expr,kno_lexenv env,
			    kno_stack _stack);
static lispval choice_eval(lispval expr,kno_lexenv env,
			   kno_stack _stack);

static lispval eval_loop(kno_stack eval_stack);

lispval eval_pair(lispval expr,kno_lexenv env,
		  kno_stack stack,
		  int tail);

KNO_EXPORT
lispval kno_cons_eval(lispval expr,kno_lexenv env,
		      kno_stack stack,
		      int tail)
{
  kno_lisp_type ctype = KNO_CONSPTR_TYPE(expr);
  kno_stack caller = stack->stack_caller;
  switch (ctype) {
  case kno_slotmap_type:
    return kno_deep_copy(expr);
  case kno_pair_type:
    if ( (KNO_TAIL_EVAL) && (tail) && (caller) &&
	 (KNO_STACK_BITP(stack,KNO_STACK_TAIL_POS)) &&
	 (KNO_STACK_BITP(stack,KNO_STACK_EVAL_LOOP)) &&
	 ( env == caller->eval_env) ) {
      if (tail&KNO_VOID_VAL) {
	KNO_STACK_SET_BITS(caller,KNO_STACK_VOID_VAL);}
      caller->stack_point  = expr;
      return KNO_TAIL;}
    else {
      lispval point  = expr;
      KNO_START_EVAL(loop_stack,"loop",expr,env,stack);
      int tail_bits = ( (tail) ? (KNO_STACK_TAIL_POS) : (0) );
      KNO_STACK_SET_BITS(loop_stack,((KNO_STACK_EVAL_LOOP) | tail_bits));
      loop_stack->stack_point  = point;
      loop_stack->eval_source  = expr;
      loop_stack->eval_context = expr;
      loop_stack->eval_env=env;
      lispval result = eval_loop(loop_stack);
      kno_pop_stack(loop_stack);
      return result;}
  case kno_choice_type:
    return choice_eval(expr,env,stack);
  case kno_schemap_type:
    return schemap_eval(expr,env,stack);
  default:
    return kno_incref(expr);
  }
}

static lispval choice_eval(lispval expr,kno_lexenv env,
			   kno_stack _stack)
{
  lispval result = EMPTY;
  KNO_PUSH_EVAL(choice_stack,NULL,expr,env);
  DO_CHOICES(each_expr,expr) {
    lispval r = kno_eval(each_expr,env,choice_stack,0);
    if (KNO_ABORTED(r)) {
      KNO_STOP_DO_CHOICES;
      kno_decref(result);
      result=r;
      break;}
    else {CHOICE_ADD(result,r);}}
  kno_pop_stack(choice_stack);
  return simplify_value(result);
}

/* Evaluating schemaps */

lispval schemap_eval(lispval expr,kno_lexenv env,
		     kno_stack _stack)
{
  struct KNO_SCHEMAP *skmap = (kno_schemap) expr;
  int n = skmap->schema_length;
  lispval *schema = skmap->table_schema;
  lispval *vals = skmap->schema_values;
  lispval result = KNO_VOID, new_vals[n];
  KNO_PUSH_EVAL(keyval_stack,NULL,KNO_VOID,env);
  int i = 0; while (i < n) {
    lispval val_expr = vals[i];
    lispval val = kno_eval(val_expr,env,keyval_stack,0);
    if (KNO_ABORTED(val)) {
      result = val;
      break;}
    else new_vals[i++] = val;}
  if (KNO_ABORTED(result))
    kno_decref_vec(new_vals,i);
  else {
    result = kno_make_schemap(NULL,n,KNO_SCHEMAP_INLINE,schema,new_vals);
    /* Set the template, which allows the new schemap to share the schema */
    struct KNO_SCHEMAP *newmap = (kno_schemap) result;
    newmap->schemap_template=expr; kno_incref(expr);}
  kno_pop_stack(keyval_stack);
  return result;
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
  KNO_START_EVAL(macro_stack,"macroexpansion",new_expr,env,eval_stack);
  kno_add_stack_ref(macro_stack,new_expr);
  lispval result = kno_eval(new_expr,env,macro_stack,0);
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

KNO_FASTOP int opcode_choice_loop(lispval *resultp,
				  lispval op,int i,int n,
				  kno_argvec nd_args,
				  lispval *d_args)
{
  if (n==i) {
    lispval result = opcode_dispatch(op,n,d_args);
    KNO_ADD_TO_CHOICE(*resultp,result);
    return 1;}
  lispval arg = nd_args[i];
  if (CHOICEP(arg)) {
    DO_CHOICES(each,arg) {
      d_args[i] = each;
      int rv= opcode_choice_loop(resultp,op,i+1,n,nd_args,d_args);
      if (rv<0) return -1;}
    return 0;}
  else {
    d_args[i] = arg;
    return opcode_choice_loop(resultp,op,i+1,n,nd_args,d_args);}
}

/* Applying lambdas */

/* Lambda application */

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

lispval execute_lambda(kno_stack stack,int tail)
{
  struct KNO_LAMBDA *proc = (kno_lambda) (stack->stack_point);
  int synchronized = proc->lambda_synchronized;
  kno_lexenv proc_env=proc->lambda_env;
  int arity = proc->fcn_arity;
  int n_vars = proc->lambda_n_vars;

  u8_string label = (proc->fcn_name) ? (proc->fcn_name) : (U8S("lambda"));
  KNO_START_EVAL(lambda_stack,label,(lispval)proc,NULL,stack);
  lispval args[n_vars];
  _lambda_stack.stack_args.elts  = args;
  _lambda_stack.stack_args.count = 0;
  _lambda_stack.stack_args.len   = n_vars;
  KNO_STACK_SET_BITS(lambda_stack,KNO_STACK_DECREF_ARGS);

  lispval *given = KNO_STACK_ARGS(stack);
  int    n_given = STACK_ARGCOUNT(stack);

  kno_profile profile = proc->fcn_profile;
  struct rusage before; struct timespec start;
  if (profile) {
    if (profile->prof_disabled) profile=NULL;
    else kno_profile_start(&before,&start);}

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
    else NO_ELSE;
    kno_incref(arg);
    args[i++] = arg;}

  /* If we're synchronized, lock the mutex.
     We include argument defaults within the lock ?? */
  if (synchronized) u8_lock_mutex(&(proc->lambda_lock));

  /* Handle argument defaults */
  lispval *inits = proc->lambda_inits;
  if (inits) {
    while (i<max_positional) {
      lispval init_expr = inits[i], init_val = KNO_VOID;
      if (KNO_CONSP(init_expr)) {
	if ( (KNO_PAIRP(init_expr)) || (KNO_SCHEMAPP(init_expr)) )
	  init_val = kno_eval(init_expr,proc_env,stack,0);
	else init_val = kno_incref(init_expr);}
      else if (KNO_VOIDP(init_expr)) {}
      else if (KNO_SYMBOLP(init_expr))
	init_val = kno_eval_symbol(init_expr,proc_env);
      else if (KNO_LEXREFP(init_expr))
	init_val = kno_lexref(init_expr,proc_env);
      else init_val = init_expr;
      if (ABORTED(init_val)) {
	kno_pop_stack(lambda_stack);
	return init_val;}
      else {
	args[i++]=init_val;
	_lambda_stack.stack_args.count=i;}}}

  /* Now accumulate a .rest arg if it's needed */
  if (arity < 0) {
    if (i<n_given) {
      lispval rest_arg = get_rest_arg(given+i,n_given-i);
      args[i] = rest_arg;}
    else args[i] = KNO_EMPTY_LIST;
    i++;}
  while (i<n_vars) { args[i++] = KNO_VOID;}

  KNO_STACK_SET_BITS(lambda_stack,KNO_STACK_DECREF_ARGS);
  lambda_stack->stack_args.count = n_vars;

  lispval *vars = proc->lambda_vars;
  struct KNO_LEXENV  eval_env = { 0 };
  struct KNO_SCHEMAP bindings = { 0 };
  init_static_env(n_vars,proc_env,
		  &bindings,&eval_env,
		  vars,args);
  lambda_stack->eval_env = &eval_env;

  lispval scan = proc->lambda_start, result = KNO_VOID;
  while (KNO_PAIRP(scan)) {
    kno_decref(result);
    lispval body_expr = KNO_CAR(scan);
    scan = KNO_CDR(scan);
    int tailable = (tail) && (!(synchronized)) && (KNO_EMPTY_LISTP(scan));
    result = kno_eval(body_expr,&eval_env,lambda_stack,tailable);
    if (ABORTED(result)) break;}

  reset_env(&eval_env);
  if (stack->eval_env == &eval_env)
    stack->eval_env = NULL;

  /* If we're synchronized, unlock the mutex. */
  if (synchronized) u8_unlock_mutex(&(proc->lambda_lock));

  kno_pop_stack(lambda_stack);

  if (profile)
    kno_profile_update(profile,&before,&start,1);

  return result;
}

lispval lambda_call(kno_stack stack,int tail)
{
  lispval fn = stack->stack_point;
  struct KNO_LAMBDA *lambda = (kno_lambda) fn;
  if (!(lambda_check_arity(lambda,KNO_STACK_ARGCOUNT(stack))))
    return KNO_ERROR;
  if ( (tail) && ( (KNO_WITH_TAIL_CALLS) || (kno_enable_tail_calls) ) &&
       (stack->stack_caller) ) {
    kno_stack scan = stack->stack_caller, loop = NULL;
    while (scan) {
      if (KNO_STACK_BITP(scan,KNO_STACK_EVAL_LOOP)) {
	if (loop) {KNO_STACK_CLEAR_BITS(loop,KNO_STACK_EVAL_LOOP);}
	loop=scan;}
      if (KNO_STACK_BITP(scan,KNO_STACK_TAIL_POS))
	scan = scan->stack_caller;
      else break;}
    if (loop) {
      /* Copy the args to the loop stack */
      int heap_args   = STACKVEC_ONHEAP(&(stack->stack_args));
      loop->stack_point = fn; kno_incref(fn); kno_add_stack_ref(loop,fn);
      int argcount    = STACK_ARGCOUNT(stack);
      int width       = STACK_WIDTH(stack);
      int decref_args = (KNO_STACK_BITP(stack,KNO_STACK_DECREF_ARGS));
      if (heap_args) {
	KNO_STACK_SET_ARGS(loop,STACK_ARGS(stack),width,
			   argcount,
			   ( (decref_args) ? (KNO_DECREF_ARGS) : (0) ) |
			   (KNO_FREE_ARGBUF) );}
      else {
	int n = STACK_ARGCOUNT(stack);
	int call_width = lambda->fcn_call_width;
	if (KNO_STACK_BITP(loop,KNO_STACK_DECREF_ARGS)) {
	  kno_decref_stackvec(&(loop->stack_args));
	  KNO_STACK_CLEAR_BITS(loop,KNO_STACK_DECREF_ARGS);
	  loop->stack_args.count = 0;}
	int need_width = (n > call_width) ? (n) : (call_width);
	int loop_width = STACK_WIDTH(loop);
	if (loop_width < width) {
	  __kno_stackvec_grow(&(loop->stack_args),need_width);}
	memcpy(loop->stack_args.elts,
	       stack->stack_args.elts,
	       n*sizeof(lispval));
	if (KNO_STACK_BITP(stack,KNO_STACK_DECREF_ARGS))
	  KNO_STACK_SET_BITS(loop,KNO_STACK_DECREF_ARGS);
	else KNO_STACK_CLEAR_BITS(loop,KNO_STACK_DECREF_ARGS);
	loop->stack_args.count = n;}
      KNO_STACK_CLEAR_BITS(stack,KNO_STACK_DECREF_ARGS);
      struct KNO_STACKVEC no_args = { 0 };
      stack->stack_args = no_args;
      return KNO_TAIL;}
    else return execute_lambda(stack,tail);}
  else return execute_lambda(stack,tail);
}

/* Getting the function value for an expression */

static lispval get_headval(lispval head,kno_lexenv env,
			   kno_stack eval_stack)
{
  lispval headval = VOID;
  if (KNO_LEXREFP(head)) {
      headval=kno_lexref(head,env);
      if (KNO_FCNIDP(headval))
	return kno_fcnid_ref(headval);
      kno_add_stack_ref(((kno_stack)eval_stack),headval);
      return headval;}
  else if (KNO_FCNIDP(head))
    return kno_fcnid_ref(head);
  else if (KNO_SYMBOLP(head)) {
    if (head == quote_symbol)
	return KNO_QUOTE_OPCODE;
    headval=kno_symeval(head,env);
    if (KNO_FCNIDP(headval))
      return kno_fcnid_ref(headval);
    kno_add_stack_ref(((kno_stack)eval_stack),headval);
    return headval;}
  else if (KNO_IMMEDIATEP(head))
    return kno_err("NotEvalable","op_pair_eval",NULL,head);
  else if ( (KNO_FUNCTIONP(head)) ||
	    (TYPEP(head,kno_evalfn_type)) ||
	    (KNO_APPLICABLEP(head)) )
    return head;
  else if ( (PAIRP(head)) || (CHOICEP(head)) ) {
    headval = kno_eval(head,env,eval_stack,0);
    headval = simplify_value(headval);
    if (KNO_FCNIDP(headval))
      return kno_fcnid_ref(headval);
    else kno_add_stack_ref(((kno_stack)eval_stack),headval);
    return headval;}
  else return kno_err("NotEvalable","op_pair_eval",NULL,head);
}

/* Evaluating pair expressions */

lispval eval_pair(lispval expr,kno_lexenv env,
		  kno_stack stack,
		  int tail)
{
  lispval result = KNO_VOID, source = expr;
  lispval old_point  = stack->stack_point;
  lispval old_source = stack->eval_source;
  lispval head = KNO_CAR(expr), headval = KNO_VOID;
  int old_tail = KNO_STACK_BITP(stack,KNO_STACK_TAIL_POS);
  kno_function f = NULL;
  if (tail) {KNO_STACK_SET_BITS(stack,KNO_STACK_TAIL_POS);}
  else {KNO_STACK_CLEAR_BITS(stack,KNO_STACK_TAIL_POS);}
  while (head == KNO_SOURCEREF_OPCODE) {
    expr = KNO_CDR(expr);
    source = KNO_CAR(expr);
    expr = KNO_CDR(expr);
    if (!(KNO_PAIRP(expr))) {
      stack->eval_source = source;
      return kno_eval(expr,env,stack,tail);}
    else head = KNO_CAR(expr);}
  stack->eval_source = expr;
  if (KNO_FCNIDP(head))
    headval=kno_fcnid_ref(head);
  else if (KNO_OPCODEP(head))
    headval = head;
  else if ( KNO_FAST_FUNCTIONP(head)) {
    headval = head; f = (kno_function) head;}
  else headval = get_headval(head,env,stack);
  if ( (f==NULL) && (KNO_FUNCTIONP(headval)) )
    f = (kno_function) headval;
  else if (KNO_CONSP(headval)) {}
  else if (KNO_ABORTED(headval)) {
    result = headval; goto clean_exit;}
  else if (PRED_FALSE(( (VOIDP(headval)) || (KNO_FALSEP(headval))))) {
    if (KNO_SYMBOLP(head)) {
      result = kno_err(kno_UnboundIdentifier,"eval_pair",
		       KNO_SYMBOL_NAME(head),head);}
    else {result = kno_err(BadExpressionHead,"eval_pair",NULL,head);}
    goto clean_exit;}
  else if (EMPTYP(headval) ) {
    result = KNO_EMPTY;
    goto clean_exit;}
  else NO_ELSE;

  stack->stack_point = headval;

  int argbuf_len = INIT_ARGBUF_LEN;
  kno_lisp_type headtype = KNO_TYPEOF(headval);
  lispval op = headval, exprs = KNO_CDR(expr);
  switch (headtype) {
  case kno_opcode_type:
    if (KNO_SPECIAL_OPCODEP(headval)) {
      result = handle_special_opcode(headval,KNO_CDR(expr),expr,env,stack,tail);
      goto clean_exit;}
    else if (KNO_APPLY_OPCODEP(headval)) {
      lispval fn = pop_arg(exprs);
      if (KNO_APPLICABLEP(fn)) op = fn;
      else {
	result = kno_err(kno_NotAFunction,"eval_pair",
			 (KNO_SYMBOLP(head))? (KNO_SYMBOL_NAME(head)) : (NULL),
			 headval);
	goto clean_exit;}
      if (headval == KNO_APPLY_N_OPCODE) {
	lispval argcount = pop_arg(exprs);
	int argc = (KNO_FIXNUMP(argcount)) ? (KNO_FIX2INT(argcount)) : (-1);
	if (argc<0) {
	  result = kno_err(kno_SyntaxError,"apply_op",NULL,expr);
	  goto clean_exit;}
	if (argc > argbuf_len) argbuf_len = argc;}
      else {
	int argc = ((KNO_IMMEDIATE_DATA(headval))&0xF);
	if (argc > argbuf_len) argbuf_len = argc;}
      break;}
    else break;
  case kno_evalfn_type: {
    struct KNO_EVALFN *handler = (kno_evalfn)headval;
    /* These are evalfns which do all the evaluating themselves */
    stack->stack_label=handler->evalfn_name;
    result = handler->evalfn_handler(expr,env,stack);
    goto clean_exit;}
  case kno_macro_type: {
    result = macro_eval((struct KNO_MACRO *)headval,expr,env,stack);
    goto clean_exit;}
  case kno_choice_type: break;
  default:
    if (KNO_FUNCTION_TYPEP(headtype)) {}
    else if (kno_applyfns[headtype] == NULL) {
      result = kno_err(kno_NotAFunction,"eval_pair",
		       (KNO_SYMBOLP(head))? (KNO_SYMBOL_NAME(head)) : (NULL),
		       headval);
      goto clean_exit;}
    else NO_ELSE;
  } /* switch(headtype) */

  /* Straightforward eval/apply */

  {
    KNO_DECL_STACKVEC(args,argbuf_len);
    int decref_args=0;
    kno_lisp_type optype = KNO_TYPEOF(op);
    int nd_op = (f) ? (KNO_FCN_NDCALLP(f)) :
      (KNO_OPCODEP(head)) ? (KNO_ND_OPCODEP(head)) :
      (kno_isndfunctionp[optype]);
    int nd_args = 0;
    while (PAIRP(exprs)) {
      lispval arg_expr = pop_arg(exprs), arg = KNO_VOID;
      kno_lisp_type arg_type = KNO_TYPEOF(arg_expr);
      switch (arg_type) {
      case kno_oid_type: case kno_fixnum_type:
	arg = arg_expr; break;
      case kno_lexref_type:
	arg = kno_lexref(arg_expr,env); break;
      case kno_symbol_type:
	arg = kno_eval_symbol(arg_expr,env); break;
      case kno_pair_type:
	arg = eval_pair(arg_expr,env,stack,0); break;
      case kno_schemap_type:
	arg = schemap_eval(arg_expr,env,stack); break;
      case kno_choice_type:
	arg = choice_eval(arg_expr,env,stack); break;
      default:
	arg = kno_incref(arg_expr);}
      if (KNO_ABORTED(arg)) {
	result = arg;
	goto cleanup_args;}
      else if (arg == KNO_TAIL) {
	kno_seterr("TailValueReturned","eval_pair",NULL,arg_expr);
	result = KNO_ERROR;
	goto cleanup_args;}
      else if ( (KNO_EMPTYP(arg)) && (!(nd_op)) ) {
	if (decref_args) kno_decref_stackvec(args);
	kno_free_stackvec(args);
	return KNO_EMPTY;}
      else if (KNO_CONSP(arg)) {
	if ( (nd_args == 0) && (CHOICEP(arg)) )
	  nd_args = 1;
	decref_args = 1;}
      else NO_ELSE;
      kno_stackvec_push(args,arg);}

    kno_argvec argbuf = STACKVEC_ARGS(args);
    int n_args        = STACKVEC_COUNT(args);

    if (KNO_STACK_BITP(stack,KNO_STACK_DECREF_ARGS))
      kno_decref_stackvec(&(stack->stack_args));
    kno_free_stackvec(&(stack->stack_args));

    stack->stack_args = _args;
    if (decref_args)
      KNO_STACK_SET_BITS(stack,KNO_STACK_DECREF_ARGS);
    else KNO_STACK_SET_BITS(stack,KNO_STACK_DECREF_ARGS);
    args = NULL;

    int ndcall = (nd_args) && (!(nd_op));
    switch (optype) {
    case kno_opcode_type: {
      if (ndcall) {
	lispval combined = KNO_EMPTY;
	lispval d_args[n_args];
	int rv = opcode_choice_loop(&combined,head,0,n_args,argbuf,d_args);
	if (rv<0) {
	  kno_decref(result);
	  result = KNO_ERROR;}
	else result = combined;}
      else result = opcode_dispatch(head,n_args,argbuf);
      break;}
    case kno_choice_type:
      result = kno_call(stack,op,n_args,argbuf);
      break;
    case kno_lambda_type: {
      if (ndcall)
	result = kno_call(stack,op,n_args,argbuf);
      else result = lambda_call(stack,tail);
      break;}
    default: {
      if (ndcall)
	result = kno_call(stack,op,n_args,argbuf);
      else result = kno_dcall(stack,op,n_args,argbuf);}
    } /* switch(optype) */
  cleanup_args:
    /* We jump here if we haven't yet put *args* on the stack */
    if (args) {
      if (decref_args) kno_decref_stackvec(&_args);
      kno_free_stackvec(&_args);}
    else {
      if (KNO_STACK_BITP(stack,KNO_STACK_DECREF_ARGS)) {
	KNO_STACK_CLEAR_BITS(stack,KNO_STACK_DECREF_ARGS);
	kno_decref_stackvec(&(stack->stack_args));}
      kno_free_stackvec(&(stack->stack_args));}}

  /* Restore old things */
 clean_exit:
  if (old_tail) {KNO_STACK_SET_BITS(stack,KNO_STACK_TAIL_POS);}
  else {KNO_STACK_CLEAR_BITS(stack,KNO_STACK_TAIL_POS);}
  if (result != KNO_TAIL) {
    stack->stack_point = old_point;
    stack->eval_source = old_source;}
  return result;
}

/* Opcode evaluation */

KNO_EXPORT lispval _kno_eval_expr(lispval expr,kno_lexenv env)
{
  lispval result = kno_stack_eval(expr,env,kno_stackptr);
  return result;
}

/* The eval/reduce loop */

static lispval eval_loop(kno_stack loop_stack)
{
  lispval point  = loop_stack->stack_point;
  kno_lexenv env = loop_stack->eval_env;
  lispval result = KNO_TAIL;
  kno_lisp_type point_type = KNO_TYPEOF(point);
  while (result == KNO_TAIL) {
    if (point_type == kno_lambda_type)
      result = execute_lambda(loop_stack,1);
    else if (point_type == kno_pair_type) {
      KNO_START_EVAL(eval_stack,"eval",point,env,loop_stack);
      KNO_STACK_SET_BITS(eval_stack,KNO_STACK_TAIL_POS);
      eval_stack->eval_source = point;
      result = eval_pair(point,env,eval_stack,1);
      kno_pop_stack(eval_stack);}
    else if (KNO_APPLICABLE_TYPEP(point_type))
      result = kno_call(loop_stack,point,STACK_ARGCOUNT(loop_stack),
			STACK_ARGS(loop_stack));
    else if (point_type == kno_schemap_type)
      result = schemap_eval(point,env,loop_stack);
    else if (point_type == kno_choice_type)
      result = choice_eval(point,env,loop_stack);
    else result = kno_incref(point);
    if (result == KNO_TAIL) {
      /* Check if we were thrown through */
      if (KNO_STACK_BITP(loop_stack,KNO_STACK_EVAL_LOOP)) {
	point = loop_stack->stack_point;
	env   = loop_stack->eval_env;
	point_type = KNO_TYPEOF(point);}
      else break;}}
  if (ABORTED(result)) return result;
  else if (KNO_STACK_BITP(loop_stack,KNO_STACK_VOID_VAL)) {
    kno_decref(result);
    return KNO_VOID;}
  else return result;
}

/* Lexrefs */

DEFPRIM2("%lexref",lexref_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "(%LEXREF *up* *across*) "
	 "returns a lexref (lexical reference) given a "
	 "'number of environments' *up* and a 'number of "
	 "bindings' across",
	 kno_fixnum_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval lexref_prim(lispval upv,lispval acrossv)
{
  long long up = FIX2INT(upv), across = FIX2INT(acrossv);
  long long combined = ((up<<5)|(across));
  /* Not the exact range limits, but good enough */
  if ((up>=0)&&(across>=0)&&(up<256)&&(across<256))
    return LISPVAL_IMMEDIATE(kno_lexref_type,combined);
  else if ((up<0)||(up>256))
    return kno_type_error("short","lexref_prim",up);
  else return kno_type_error("short","lexref_prim",across);
}

DEFPRIM1("%lexref?",lexrefp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(%LEXREF? *val*) "
	 "returns true if it's argument is a lexref "
	 "(lexical reference)",
	 kno_any_type,KNO_VOID);
static lispval lexrefp_prim(lispval ref)
{
  if (KNO_TYPEP(ref,kno_lexref_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("%lexrefval",lexref_value_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(%LEXREFVAL *lexref*) "
	 "returns the offsets of a lexref as a pair (*up* . "
	 "*across*)",
	 kno_lexref_type,KNO_VOID);
static lispval lexref_value_prim(lispval lexref)
{
  int code = KNO_GET_IMMEDIATE(lexref,kno_lexref_type);
  int up = code/32, across = code%32;
  return kno_init_pair(NULL,KNO_INT(up),KNO_INT(across));
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

/* Code refs */

DEFPRIM1("%coderef",coderef_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(%CODEREF *nelts*) "
	 "returns a 'coderef' (a relative position) value",
	 kno_fixnum_type,KNO_VOID);
static lispval coderef_prim(lispval offset)
{
  long long off = FIX2INT(offset);
  return LISPVAL_IMMEDIATE(kno_coderef_type,off);
}

DEFPRIM1("coderef?",coderefp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(CODEREF? *nelts*) "
	 "returns #t if *arg* is a coderef",
	 kno_any_type,KNO_VOID);
static lispval coderefp_prim(lispval ref)
{
  if (KNO_TYPEP(ref,kno_coderef_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("%coderefval",coderef_value_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(%CODEREFVAL *coderef*) "
	 "returns the integer relative offset of a coderef",
	 kno_coderef_type,KNO_VOID);
static lispval coderef_value_prim(lispval offset)
{
  long long off = KNO_GET_IMMEDIATE(offset,kno_coderef_type);
  return KNO_INT(off);
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

DEFPRIM1("make-coderef",make_coderef,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(MAKE-CODEREF <fixnum>)\n"
	 "Returns a coderef object",
	 kno_fixnum_type,KNO_VOID);
static lispval make_coderef(lispval x)
{
  if ( (KNO_UINTP(x)) && ((KNO_FIX2INT(x)) < KNO_IMMEDIATE_MAX) ) {
    int off = FIX2INT(x);
    return KNO_ENCODEREF(off);}
  else return kno_err(kno_RangeError,"make_coderef",NULL,x);
}


DEFPRIM1("opcode?",opcodep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(OPCODE? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval opcodep(lispval x)
{
  if (KNO_OPCODEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static void set_opcode_name(lispval opcode,u8_string name)
{
  int off = KNO_OPCODE_NUM(opcode);
  u8_string constname=u8_string_append("#",name,NULL);
  kno_opcode_names[off]=name;
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

DEFPRIM1("name->opcode",name2opcode_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(NAME->OPCODE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval name2opcode_prim(lispval arg)
{
  if (KNO_SYMBOLP(arg))
    return kno_get_opcode(SYM_NAME(arg));
  else if (STRINGP(arg))
    return kno_get_opcode(CSTRING(arg));
  else return kno_type_error(_("opcode name"),"name2opcode_prim",arg);
}

DEFPRIM1("make-opcode",make_opcode,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(MAKE-OPCODE *arg0*)` **undocumented**",
	 kno_fixnum_type,KNO_VOID);
static lispval make_opcode(lispval x)
{
  if ( (KNO_UINTP(x)) && ((KNO_FIX2INT(x)) < KNO_IMMEDIATE_MAX) )
    return KNO_OPCODE(FIX2INT(x));
  else return kno_err(kno_RangeError,"make_opcode",NULL,x);
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

DEFPRIM1("opcode-name",opcode_name_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(OPCODE-NAME *opcode*) "
	 "returns the name of *opcode* or #f it's valid but "
	 "anonymous, and errors if it is not an opcode or "
	 "invalid",
	 kno_opcode_type,KNO_VOID);
static lispval opcode_name_prim(lispval opcode)
{
  long opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if ((opcode_offset<kno_opcodes_length) &&
      (kno_opcode_names[opcode_offset]))
    return knostring(kno_opcode_names[opcode_offset]);
  else if (opcode_offset<kno_opcodes_length)
    return KNO_FALSE;
  else return kno_err("InvalidOpcode","opcode_name_prim",NULL,opcode);
}

/* Making some functions */

KNO_EXPORT lispval kno_make_evalfn(u8_string name,kno_eval_handler fn)
{
  struct KNO_EVALFN *f = u8_alloc(struct KNO_EVALFN);
  KNO_INIT_CONS(f,kno_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_filename = NULL;
  f->evalfn_handler = fn;
  return LISP_CONS(f);
}

KNO_EXPORT void kno_new_evalfn(lispval mod,u8_string name,u8_string cname,
			       u8_string filename,u8_string doc,
			       kno_eval_handler fn)
{
  struct KNO_EVALFN *f = u8_alloc(struct KNO_EVALFN);
  KNO_INIT_CONS(f,kno_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_handler = fn;
  f->evalfn_filename = filename;
  f->evalfn_cname = u8_strdup(cname);
  f->evalfn_documentation = u8_strdup(doc);
  kno_store(mod,kno_getsym(name),LISP_CONS(f));
  f->evalfn_moduleid = kno_get(mod,moduleid_symbol,KNO_VOID);
  kno_decref(LISP_CONS(f));
}

/* The Evaluator */

static lispval eval_evalfn(lispval x,kno_lexenv env,kno_stack stack)
{
  lispval expr_expr = kno_get_arg(x,1);
  lispval expr = kno_stack_eval(expr_expr,env,stack);
  if (KNO_ABORTED(expr)) return expr;
  lispval result = kno_stack_eval(expr,env,stack);
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
    if  (v == KNO_NULL)
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
    lispval v = kno_eval_expr(to_eval,env);
    if  (v == KNO_NULL)
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
    lispval v = kno_eval_expr(to_eval,env);
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
    lispval v = kno_eval_expr(to_eval,env);
    if  (v == KNO_NULL)
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
    lispval v = kno_eval_expr(to_eval,env);
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

DEFPRIM2("symbol-bound-in?",symbol_boundin_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(SYMBOL-BOUND-IN? *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval symbol_boundin_prim(lispval symbol,lispval envarg)
{
  if (!(SYMBOLP(symbol)))
    return kno_type_error(_("symbol"),"symbol_boundp_prim",symbol);
  else if (KNO_LEXENVP(envarg)) {
    kno_lexenv env = (kno_lexenv)envarg;
    lispval val = kno_symeval(symbol,env);
    if (KNO_ABORTED(val))
      return val;
    else if (VOIDP(val))
      return KNO_FALSE;
    else if (val == KNO_DEFAULT_VALUE)
      return KNO_FALSE;
    else if (val == KNO_UNBOUND)
      return KNO_FALSE;
    else {
      kno_decref(val);
      return KNO_TRUE;}}
  else if (TABLEP(envarg)) {
    lispval val = kno_get(envarg,symbol,VOID);
    if (VOIDP(val))
      return KNO_FALSE;
    else {
      kno_decref(val);
      return KNO_TRUE;}}
  else return kno_type_error(_("environment"),"symbol_boundp_prim",envarg);
}

static lispval symbol_boundp_evalfn(lispval expr,kno_lexenv env,
				    kno_stack stack)
{
  lispval symbol_expr = kno_get_arg(expr,1), symbol;
  kno_lexenv use_env = NULL;
  if (VOIDP(symbol_expr))
    return kno_err(kno_SyntaxError,"symbol_boundp_evalfn",NULL,expr);
  else symbol = kno_stack_eval(symbol_expr,env,stack);
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
    env_value = kno_stack_eval(env_expr,env,stack);
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

DEFPRIM1("environment?",environmentp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ENVIRONMENT? *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval environmentp_prim(lispval arg)
{
  if (KNO_LEXENVP(arg))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Withenv forms */

static lispval withenv(lispval expr,kno_lexenv env,
		       kno_lexenv consed_env,u8_context cxt)
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
	lispval var = KNO_CAR(varval), val = kno_eval_expr(KNO_CADR(varval),env);
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
      result = kno_eval_expr(elt,consed_env);
      if (KNO_ABORTED(result)) {
	return result;}}
    return result;}
}

static lispval withenv_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  kno_lexenv consed_env = kno_working_lexenv();
  lispval result = withenv(expr,env,consed_env,"WITHENV");
  kno_recycle_lexenv(consed_env);
  return result;
}

/* Eval/apply related primitives */

DEFPRIM3("get-arg",get_arg_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(GET-ARG *expression* *i* [*default*])` "
	 "returns the *i*'th parameter in *expression*, or "
	 "*default* (otherwise)",
	 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval get_arg_prim(lispval expr,lispval elt,lispval dflt)
{
  if (PAIRP(expr))
    if (KNO_UINTP(elt)) {
      int i = 0, lim = FIX2INT(elt); lispval scan = expr;
      while ((i<lim) && (PAIRP(scan))) {
	scan = KNO_CDR(scan);
	i++;}
      if (PAIRP(scan))
	return kno_incref(KNO_CAR(scan));
      else return kno_incref(dflt);}
    else return kno_type_error(_("fixnum"),"get_arg_prim",elt);
  else return kno_type_error(_("pair"),"get_arg_prim",expr);
}

DEFPRIM("apply",apply_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDOP,
	"`(APPLY *arg0* *args...*)` **undocumented**");
static lispval apply_lexpr(int n,kno_argvec args)
{
  DO_CHOICES(fn,args[0])
    if (!(KNO_APPLICABLEP(args[0]))) {
      KNO_STOP_DO_CHOICES;
      return kno_type_error("function","apply_lexpr",args[0]);}
  {
    lispval results = EMPTY;
    DO_CHOICES(fn,args[0]) {
      DO_CHOICES(final_arg,args[n-1]) {
	lispval result = VOID;
	int final_length = kno_seq_length(final_arg);
	int n_args = (n-2)+final_length;
	lispval *values = u8_alloc_n(n_args,lispval);
	int i = 1, j = 0, lim = n-1;
	/* Copy regular arguments */
	while (i<lim) {values[j]=kno_incref(args[i]); j++; i++;}
	i = 0; while (j<n_args) {
	  values[j]=kno_seq_elt(final_arg,i);
	  j++; i++;}
	result = kno_apply(fn,n_args,values);
	if (KNO_ABORTED(result)) {
	  kno_decref(results);
	  KNO_STOP_DO_CHOICES;
	  i = 0; while (i<n_args) {
	    kno_decref(values[i]);
	    i++;}
	  u8_free(values);
	  results = result;
	  break;}
	else {CHOICE_ADD(results,result);}
	i = 0; while (i<n_args) {
	  kno_decref(values[i]);
	  i++;}
	u8_free(values);}
      if (KNO_ABORTED(results)) {
	KNO_STOP_DO_CHOICES;
	return results;}}
    return simplify_value(results);
  }
}

/* Table functions used everywhere (:)) */

static lispval lispenv_get(lispval e,lispval s,lispval d)
{
  lispval result = kno_symeval(s,KNO_XENV(e));
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

/* Call/cc */

static lispval call_continuation(struct KNO_STACK *stack,
				 struct KNO_FUNCTION *f,
				 int n,kno_argvec args)
{
  struct KNO_CONTINUATION *cont = (struct KNO_CONTINUATION *)f;
  lispval arg = args[0];
  if (cont->retval == KNO_NULL)
    return kno_err(ExpiredThrow,"call_continuation",NULL,arg);
  else if (VOIDP(cont->retval)) {
    cont->retval = kno_incref(arg);
    return KNO_THROW_VALUE;}
  else return kno_err(DoubleThrow,"call_continuation",NULL,arg);
}

DEFPRIM1("call/cc",callcc,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(call/cc *fn.callback*)` applies *fn.callback* to a single argument, "
	 "which is a *continuation* procedure. When the application of *fn.callback* "
	 "calls *continuation* to an argument, that argument is immediately returned "
	 "by the call to `call/cc`. If *continuation* is never called, `call/cc` simply "
	 "returns the value returned by *fn.callback*.",
	 kno_any_type,KNO_VOID);
static lispval callcc(lispval proc)
{
  lispval continuation, value;
  struct KNO_CONTINUATION *f = u8_alloc(struct KNO_CONTINUATION);
  KNO_INIT_FRESH_CONS(f,kno_cprim_type);
  f->fcn_name="continuation"; f->fcn_filename = NULL;
  f->fcn_call = KNO_FCN_CALL_NDCALL | KNO_FCN_CALL_XCALL;
  f->fcn_call_width = f->fcn_arity = 1;
  f->fcn_min_arity = 1;
  f->fcn_handler.xcalln = call_continuation;
  f->fcn_typeinfo = NULL;
  f->fcn_defaults = NULL;
  f->retval = VOID;
  continuation = LISP_CONS(f);
  value = kno_apply(proc,1,&continuation);
  if ((value == KNO_THROW_VALUE) && (!(VOIDP(f->retval)))) {
    lispval retval = f->retval;
    f->retval = KNO_NULL;
    if (KNO_CONS_REFCOUNT(f)>1)
      u8_log(LOG_WARN,ExpiredThrow,"Dangling pointer exists to continuation");
    kno_decref(continuation);
    return retval;}
  else {
    if (KNO_CONS_REFCOUNT(f)>1)
      u8_log(LOG_WARN,LostThrow,"Dangling pointer exists to continuation");
    kno_decref(continuation);
    return value;}
}

/* Cache call */

DEFPRIM("cachecall",cachecall,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	"`(CACHECALL *arg0* *args...*)` **undocumented**");
static lispval cachecall(int n,kno_argvec args)
{
  if (HASHTABLEP(args[0]))
    return kno_xcachecall((kno_hashtable)args[0],args[1],n-2,args+2);
  else return kno_cachecall(args[0],n-1,args+1);
}

DEFPRIM("cachecall/probe",cachecall_probe,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	"`(CACHECALL/PROBE *arg0* *args...*)` **undocumented**");
static lispval cachecall_probe(int n,kno_argvec args)
{
  if (HASHTABLEP(args[0]))
    return kno_xcachecall_try((kno_hashtable)args[0],args[1],n-2,args+2);
  else return kno_cachecall_try(args[0],n-1,args+1);
}

DEFPRIM("cachedcall?",cachedcallp,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	"`(CACHEDCALL? *arg0* *args...*)` **undocumented**");
static lispval cachedcallp(int n,kno_argvec args)
{
  if (HASHTABLEP(args[0]))
    if (kno_xcachecall_probe((kno_hashtable)args[0],args[1],n-2,args+2))
      return KNO_TRUE;
    else return KNO_FALSE;
  else if (kno_cachecall_probe(args[0],n-1,args+1))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("clear-callcache!",clear_callcache,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "`(CLEAR-CALLCACHE! [*arg0*])` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval clear_callcache(lispval arg)
{
  kno_clear_callcache(arg);
  return VOID;
}

DEFPRIM("thread/cachecall",tcachecall,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	"`(THREAD/CACHECALL *arg0* *args...*)` **undocumented**");
static lispval tcachecall(int n,kno_argvec args)
{
  return kno_tcachecall(args[0],n-1,args+1);
}

static lispval with_threadcache_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct KNO_THREAD_CACHE *tc = kno_push_threadcache(NULL);
  lispval value = VOID;
  KNO_DOLIST(each,KNO_CDR(expr)) {
    kno_decref(value); value = VOID; value = kno_eval_expr(each,env);
    if (KNO_ABORTED(value)) {
      kno_pop_threadcache(tc);
      return value;}}
  kno_pop_threadcache(tc);
  return value;
}

static lispval using_threadcache_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct KNO_THREAD_CACHE *tc = kno_use_threadcache();
  lispval value = VOID;
  KNO_DOLIST(each,KNO_CDR(expr)) {
    kno_decref(value); value = VOID; value = kno_eval_expr(each,env);
    if (KNO_ABORTED(value)) {
      if (tc) kno_pop_threadcache(tc);
      return value;}}
  if (tc) kno_pop_threadcache(tc);
  return value;
}

DEFPRIM1("use-threadcache",use_threadcache_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "`(USE-THREADCACHE [*arg0*])` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval use_threadcache_prim(lispval arg)
{
  if (FALSEP(arg)) {
    if (!(kno_threadcache)) return KNO_FALSE;
    while (kno_threadcache) kno_pop_threadcache(kno_threadcache);
    return KNO_TRUE;}
  else {
    struct KNO_THREAD_CACHE *tc = kno_use_threadcache();
    if (tc) return KNO_TRUE;
    else return KNO_FALSE;}
}

/* Getting documentation */

DEFPRIM1("documentation",get_documentation,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(DOCUMENTATION *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
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

static lispval get_documentation(lispval x)
{
  u8_string doc = kno_get_documentation(x);
  if (doc)
    return kno_wrapstring(doc);
  else return KNO_FALSE;
}

/* Stringify */

static int stringify_method(u8_output out,lispval compound,kno_typeinfo e)
{
  if (e->type_handlers) {
    lispval method = kno_get(e->type_handlers,stringfn_symbol,VOID);
    if (VOIDP(method)) return 0;
    else {
      lispval result = kno_apply(method,1,&compound);
      kno_decref(method);
      if (STRINGP(result)) {
        u8_putn(out,CSTRING(result),STRLEN(result));
        kno_decref(result);
        return 1;}
      else {
        kno_decref(result);
        return 0;}}}
  else return 0;
}

static lispval cons_method(int n,lispval *args,kno_typeinfo e)
{
  if (e->type_handlers) {
    lispval method = kno_get(e->type_handlers,KNOSYM_CONS,VOID);
    if (VOIDP(method))
      return VOID;
    else {
      lispval result = kno_apply(method,n,args);
      if (! ( (KNO_VOIDP(result)) || (KNO_EMPTYP(result)) ) ) {
	kno_decref(method);
	return result;}}}
  /* This is the default cons method */
  return kno_init_compound_from_elts(NULL,e->typetag,
				     KNO_COMPOUND_INCREF,
				     n,args);
}

/* Setting type cons functions */

DEFPRIM2("type-set-consfn!",type_set_consfn_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
         "`(TYPE-SET-CONSFN! *arg0* *arg1*)` **undocumented**",
         kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval type_set_consfn_prim(lispval tag,lispval consfn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(consfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_drop(e->type_handlers,KNOSYM_CONS,VOID);
      e->type_parsefn = NULL;
      return VOID;}
    else if (KNO_APPLICABLEP(consfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_store(e->type_handlers,KNOSYM_CONS,consfn);
      e->type_parsefn = cons_method;
      return VOID;}
    else return kno_type_error("applicable","set_compound_consfn_prim",tag);
  else return kno_type_error("compound tag","set_compound_consfn_prim",tag);
}

DEFPRIM2("type-set-stringfn!",type_set_stringfn_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(TYPE-SET-STRINGFN! *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval type_set_stringfn_prim(lispval tag,lispval stringfn)
{
  if ((SYMBOLP(tag))||(OIDP(tag)))
    if (FALSEP(stringfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_drop(e->type_handlers,stringfn_symbol,VOID);
      e->type_unparsefn = NULL;
      return VOID;}
    else if (KNO_APPLICABLEP(stringfn)) {
      struct KNO_TYPEINFO *e = kno_use_typeinfo(tag);
      kno_store(e->type_handlers,stringfn_symbol,stringfn);
      e->type_unparsefn = stringify_method;
      return VOID;}
    else return kno_type_error("applicable","set_type_stringfn_prim",tag);
  else return kno_type_error("type tag","set_type_stringfn_prim",tag);
}

DEFPRIM2("type-props",type_props_prim,KNO_MIN_ARGS(1),
         "`(type-props *tag* [*field*])` accesses the metadata "
	 "associated with the typetag assigned to *compound*. If *field* "
	 "is specified, that particular metadata field is returned. Otherwise "
	 "the entire metadata object (a slotmap) is copied and returned.",
	 kno_any_type,KNO_VOID,kno_symbol_type,KNO_VOID);
static lispval type_props_prim(lispval arg,lispval field)
{
  struct KNO_TYPEINFO *e =
    (KNO_TAGGEDP(arg)) ? (kno_taginfo(arg)) : (kno_use_typeinfo(arg));;
  if (VOIDP(field))
    return kno_deep_copy(e->type_props);
  else return kno_get(e->type_props,field,EMPTY);
}

DEFPRIM2("type-handlers",type_handlers_prim,KNO_MIN_ARGS(1),
         "`(type-handlers *tag* [*field*])` accesses the metadata "
	 "associated with the typetag assigned to *compound*. If *field* "
	 "is specified, that particular metadata field is returned. Otherwise "
	 "the entire metadata object (a slotmap) is copied and returned.",
	 kno_any_type,KNO_VOID,kno_symbol_type,KNO_VOID);
static lispval type_handlers_prim(lispval arg,lispval method)
{
  struct KNO_TYPEINFO *e =
    (KNO_TAGGEDP(arg)) ? (kno_taginfo(arg)) : (kno_use_typeinfo(arg));;
  if (VOIDP(method))
    return kno_deep_copy(e->type_handlers);
  else return kno_get(e->type_handlers,method,EMPTY);
}

DEFPRIM3("type-set!",type_set_prim,KNO_MIN_ARGS(3),
         "`(type-set! *tag* *field* *value*)` stores *value* "
	 "in *field* of the properties associated with the type tag *tag*.",
	 kno_any_type,KNO_VOID,kno_symbol_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval type_set_prim(lispval arg,lispval field,lispval value)
{
  struct KNO_TYPEINFO *e =
    (KNO_TAGGEDP(arg)) ? (kno_taginfo(arg)) : (kno_use_typeinfo(arg));;
  int rv = kno_store(e->type_props,field,value);
  if (rv < 0)
    return KNO_ERROR;
  else if (rv)
    return KNO_TRUE;
  else return KNO_FALSE;
}



/* Helpful forms and functions */

static lispval void_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = KNO_CDR(expr);
  KNO_DOLIST(subex,body) {
    lispval v = kno_stack_eval(subex,env,_stack);
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
    lispval v = kno_stack_eval(subex,env,_stack);
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
      return kno_eval_expr(default_expr,env);
    else if (val == KNO_UNBOUND)
      return kno_eval_expr(default_expr,env);
    else return val;}
}

/* Checking version numbers */

static int check_num(lispval arg,int num)
{
  if ((!(FIXNUMP(arg)))||(FIX2INT(arg)<0)) {
    kno_seterr(kno_TypeError,"check_version_prim",NULL,arg);
    return -1;}
  else {
    int n = FIX2INT(arg);
    if (num>=n)
      return 1;
    else return 0;}
}

DEFPRIM("check-version",check_version_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	"`(CHECK-VERSION *arg0* *args...*)` **undocumented**");
static lispval check_version_prim(int n,kno_argvec args)
{
  int rv = check_num(args[0],KNO_MAJOR_VERSION);
  if (rv<0) return KNO_ERROR;
  else if (rv==0) return KNO_FALSE;
  else if (n==1) return KNO_TRUE;
  else rv = check_num(args[1],KNO_MINOR_VERSION);
  if (rv<0) return KNO_ERROR;
  else if (rv==0) return KNO_FALSE;
  else if (n==2) return KNO_TRUE;
  else rv = check_num(args[2],KNO_RELEASE_VERSION);
  if (rv<0) return KNO_ERROR;
  else if (rv==0) return KNO_FALSE;
  else if (n==3) return KNO_TRUE;
  else rv = check_num(args[2],KNO_RELEASE_VERSION-1);
  /* The fourth argument should be a patch level, but we're not
     getting that in builds yet. So if there are more arguments,
     we see if required release number is larger than release-1
     (which means that we should be okay, since patch levels
     are reset with releases. */
  if (rv<0) return KNO_ERROR;
  else if (rv) {
    int i = 3; while (i<n) {
      if (!(FIXNUMP(args[i])))
	return kno_err(kno_TypeError,"check_version_prim",NULL,args[i]);
      else i++;}
    return KNO_TRUE;}
  else return KNO_FALSE;
}

DEFPRIM("require-version",require_version_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
	"`(REQUIRE-VERSION *arg0* *args...*)` **undocumented**");
static lispval require_version_prim(int n,kno_argvec args)
{
  lispval result = check_version_prim(n,args);
  if (KNO_ABORTP(result))
    return result;
  else if (KNO_TRUEP(result))
    return result;
  else {
    u8_byte buf[50];
    int i = 0; while (i<n) {
      if (!(KNO_FIXNUMP(args[i])))
	return kno_type_error("version number(integer)","require_version_prim",args[i]);
      else i++;}
    lispval version_vec = kno_make_vector(n,(lispval *)args);
    kno_seterr("VersionError","require_version_prim",
	       u8_sprintf(buf,50,"Version is %s",KNO_REVISION),
	       /* We don't need to incref *args* because they're all fixnums */
	       version_vec);
    kno_decref(version_vec);
    return KNO_ERROR;}
}

/* Choice functions */

DEFPRIM1("%fixchoice",fixchoice_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1)|KNO_NDOP,
	 "`(%FIXCHOICE *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
static lispval fixchoice_prim(lispval arg)
{
  if (PRECHOICEP(arg))
    return kno_make_simple_choice(arg);
  else return kno_incref(arg);
}

DEFPRIM2("%choiceref",choiceref_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDOP,
	 "`(%CHOICEREF *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval choiceref_prim(lispval arg,lispval off)
{
  if (PRED_TRUE(FIXNUMP(off))) {
    long long i = FIX2INT(off);
    if (EMPTYP(arg)) {
      kno_seterr(kno_RangeError,"choiceref_prim","0",arg);
      return KNO_ERROR;}
    else if (CHOICEP(arg)) {
      struct KNO_CHOICE *ch = (kno_choice)arg;
      if (i>ch->choice_size) {
	u8_byte buf[50];
	return kno_err(kno_RangeError,"choiceref_prim",
		       u8_sprintf(buf,50,"%lld",i),
		       arg);}
      else {
	lispval elt = KNO_XCHOICE_DATA(ch)[i];
	return kno_incref(elt);}}
    else if (i == 0)
      return kno_incref(arg);
    else {
      u8_byte buf[50];
      return kno_err(kno_RangeError,"choiceref_prim",
		     u8_sprintf(buf,50,"%lld",i),
		     arg);}}
  else return kno_type_error("fixnum","choiceref_prim",off);
}

/* FFI */

DEFPRIM("ffi/proc",ffi_proc,KNO_VAR_ARGS|KNO_MIN_ARGS(3),
	"`(FFI/PROC *arg0* *arg1* *arg2* *args...*)` **undocumented**");

#if KNO_ENABLE_FFI
static lispval ffi_proc(int n,kno_argvec args)
{
  lispval name_arg = args[0], filename_arg = args[1];
  lispval return_type = args[2];
  u8_string name = (STRINGP(name_arg)) ? (CSTRING(name_arg)) : (NULL);
  u8_string filename = (STRINGP(filename_arg)) ?
    (CSTRING(filename_arg)) :
    (NULL);
  if (!(name))
    return kno_type_error("String","ffi_proc/name",name_arg);
  else if (!( (STRINGP(filename_arg)) || (FALSEP(filename_arg)) ))
    return kno_type_error("String","ffi_proc/filename",filename_arg);
  else {
    struct KNO_FFI_PROC *fcn=
      kno_make_ffi_proc(name,filename,n-3,return_type,args+3);
    if (fcn)
      return (lispval) fcn;
    else return KNO_ERROR;}
}

DEFPRIM2("ffi/found?",ffi_foundp_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(FFI/FOUND? *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval ffi_foundp_prim(lispval name,lispval modname)
{
  void *module=NULL, *sym=NULL;
  if (STRINGP(modname)) {
    module=u8_dynamic_load(CSTRING(modname));
    if (module==NULL) {
      kno_clear_errors(0);
      return KNO_FALSE;}}
  sym=u8_dynamic_symbol(CSTRING(name),module);
  if (sym)
    return KNO_TRUE;
  else return KNO_FALSE;
}
#else
static lispval ffi_proc(int n,lispval *args)
{
  u8_seterr("NotImplemented","ffi_proc",
	    u8_strdup("No FFI support is available in this build of Kno"));
  return KNO_ERROR;
}
static lispval ffi_foundp_prim(lispval name,lispval modname)
{
  return KNO_FALSE;
}
#endif

/* Opcode eval */


/* Initialization */

void kno_init_module_tables(void);

#include "opcode_names.h"

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

  quote_symbol = kno_intern("quote");
  _kno_comment_symbol = comment_symbol = kno_intern("comment");
  moduleid_symbol = kno_intern("%moduleid");
  source_symbol = kno_intern("%source");
  fcnids_symbol = kno_intern("%fcnids");
  sourceref_tag = kno_intern("%sourceref");
  consfn_symbol = kno_intern("cons");
  stringfn_symbol = kno_intern("stringify");

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

  /* This pushes a new threadcache */
  kno_def_evalfn(kno_scheme_module,"WITH-THREADCACHE",with_threadcache_evalfn,
		 "*undocumented*");
  /* This ensures that there's an active threadcache, pushing a new one if
     needed or using the current one if it exists. */
  kno_def_evalfn(kno_scheme_module,"USING-THREADCACHE",using_threadcache_evalfn,
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
KNO_EXPORT void kno_init_dteval_c(void);

static void init_eval_core()
{
  init_localfns();

  kno_init_configops_c();
  kno_init_eval_getopt_c();
  kno_init_eval_debug_c();
  kno_init_eval_testops_c();
  kno_init_dteval_c();
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

DEFPRIM("%buildinfo",kno_get_build_info,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"Information about the build and startup "
	"environment");

static void link_local_cprims()
{
  KNO_LINK_VARARGS("ffi/proc",ffi_proc,kno_scheme_module);
  KNO_LINK_PRIM("ffi/found?",ffi_foundp_prim,2,kno_scheme_module);

  KNO_LINK_PRIM("symbol-bound-in?",symbol_boundin_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("%choiceref",choiceref_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("get-arg",get_arg_prim,3,kno_scheme_module);

  KNO_LINK_PRIM("documentation",get_documentation,1,kno_scheme_module);

  KNO_LINK_PRIM("environment?",environmentp_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("%lexref",lexref_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("%lexrefval",lexref_value_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("%lexref?",lexrefp_prim,1,kno_scheme_module);

  KNO_LINK_PRIM("%coderef",coderef_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("%coderefval",coderef_value_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("coderef?",coderefp_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("make-coderef",make_coderef,1,kno_scheme_module);

  KNO_LINK_PRIM("opcode-name",opcode_name_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("name->opcode",name2opcode_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("make-opcode",make_opcode,1,kno_scheme_module);
  KNO_LINK_PRIM("opcode?",opcodep,1,kno_scheme_module);

  KNO_LINK_PRIM("%fixchoice",fixchoice_prim,1,kno_scheme_module);

  KNO_LINK_PRIM("call/cc",callcc,1,kno_scheme_module);
  KNO_LINK_ALIAS("call-with-current-continuation",callcc,kno_scheme_module);

  KNO_LINK_VARARGS("apply",apply_lexpr,kno_scheme_module);

  KNO_LINK_PRIM("use-threadcache",use_threadcache_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("clear-callcache!",clear_callcache,1,kno_scheme_module);
  KNO_LINK_VARARGS("thread/cachecall",tcachecall,kno_scheme_module);
  KNO_LINK_VARARGS("cachecall",cachecall,kno_scheme_module);
  KNO_LINK_VARARGS("cachecall/probe",cachecall_probe,kno_scheme_module);
  KNO_LINK_VARARGS("cachedcall?",cachedcallp,kno_scheme_module);

  KNO_LINK_VARARGS("check-version",check_version_prim,kno_scheme_module);
  KNO_LINK_VARARGS("require-version",require_version_prim,kno_scheme_module);
  KNO_LINK_PRIM("%buildinfo",kno_get_build_info,0,kno_scheme_module);

  KNO_LINK_PRIM("type-set-stringfn!",type_set_stringfn_prim,2,kno_scheme_module);
  KNO_LINK_ALIAS("compound-set-stringfn!",type_set_stringfn_prim,kno_scheme_module);
  KNO_LINK_PRIM("type-set-consfn!",type_set_consfn_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("type-set!",type_set_prim,3,kno_scheme_module);
  KNO_LINK_PRIM("type-props",type_props_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("type-handlers",type_handlers_prim,2,kno_scheme_module);
}
