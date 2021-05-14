/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_LANG_CORE		(!(KNO_AVOID_INLINE))
#define KNO_EVAL_INTERNALS 1
#define KNO_INLINE_CHECKTYPE    (!(KNO_AVOID_INLINE))
#define KNO_INLINE_CHOICES      (!(KNO_AVOID_INLINE))
#define KNO_INLINE_FCNIDS	(!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV       (!(KNO_AVOID_INLINE))

/* Evaluator defines */

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/opcodes.h"
#include "kno/dbprims.h"
#include "kno/ffi.h"
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

u8_condition kno_InvalidMacro=_("invalid macro transformer"),
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
u8_condition UnconfiguredSource="Unconfigured source";

lispval lisp_eval(lispval head,lispval expr,
		   kno_lexenv env,kno_stack stack,
		   int tail);

#define head_label(head) \
  ( (SYMBOLP(head)) ? (SYM_NAME(head)) : \
    (KNO_OPCODEP(head)) ? (opcode_name(head)) : \
    (NULL) )

static u8_string default_scheme_modules[] = { "kno/exec", NULL };

/* Top level functions */

KNO_EXPORT lispval kno_tail_eval(lispval x,kno_lexenv env,kno_stack stack)
{
if (stack==NULL) stack = kno_stackptr;
  return doeval(x,env,stack,1);
}

KNO_EXPORT lispval kno_eval(lispval x,kno_lexenv env,kno_stack stack)
{
  if (stack==NULL) stack = kno_stackptr;
  return doeval(x,env,stack,0);
}

KNO_EXPORT lispval kno_eval_arg(lispval x,kno_lexenv env,kno_stack stack)
{
  if (stack==NULL) stack = kno_stackptr;
  return eval_arg(x,env,stack);
}

lispval eval_body_error(u8_context cxt,u8_string label,lispval body)
{
  return kno_err(kno_SyntaxError,
		 ( (cxt) && (label) ) ? (cxt) :
		 ((u8_string)"eval_body"),
		 (label) ? (label) : (cxt) ? (cxt) : (NULL),
		 body);
}

#define ND_ARGP(v) ((CHOICEP(v))||(QCHOICEP(v)))

KNO_EXPORT lispval kno_bad_arg(lispval arg,u8_context cxt,lispval source_expr)
{
  return kno_err((arg == KNO_TAIL) ? (kno_TailArgument) :
		 (arg == KNO_VOID) ? (kno_VoidArgument) :
		 (kno_BadArgument),cxt,
		 kno_constant_name(arg),
		 source_expr);
}

/* Eval routines */

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

u8_string kno_opcode_names[0xF00]         = {NULL};
unsigned char kno_opcode_call_info[0xF00] = { 0 };
const int kno_opcodes_length = 0xF00;

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
    if ( (env->env_copy) && (env->env_copy!=env) ) env=env->env_copy;
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

KNO_EXPORT lispval kno_fcn_ref(lispval sym,lispval from,lispval val)
{
  if ( (KNO_LEXENVP(from)) || (KNO_HASHTABLEP(from)) ) {
    if (! ( (KNO_CONSP(val)) &&
	    ( (KNO_FUNCTIONP(val)) ||
	      (KNO_APPLICABLEP(val)) ||
	      (KNO_EVALFNP(val)) ||
	      (KNO_MACROP(val)) )) ) {
      u8_log(LOGWARN,"BadAliasValue","The value of '%q cannot be aliased: %q",
	     sym,val);
      return kno_incref(val);}
    lispval bindings = (KNO_HASHTABLEP(from)) ? (from) :
      (((kno_lexenv)from)->env_bindings);
    if (KNO_HASHTABLEP(bindings)) {
      /* Create fcnids if needed */
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
  lispval env_expr = kno_get_arg(expr,2), env_arg = KNO_VOID;
  lispval source_table = KNO_VOID;
  kno_lexenv source_env = NULL;
  if (!( (KNO_DEFAULTP(env_expr)) || (KNO_VOIDP(env_expr)) )) {
    env_arg = kno_eval(env_expr,env,stack);
    if (KNO_LEXENVP(env_arg)) {
      kno_lexenv e = (kno_lexenv) env_arg;
      if (kno_test(e->env_bindings,sym,KNO_VOID)) {
	source_env = e;}
      else e = kno_find_binding(e,sym,0);
      if (KNO_HASHTABLEP(e->env_bindings))
	source_env = (kno_lexenv) env_arg;}
    else if (KNO_HASHTABLEP(env_arg))
      source_table = env_arg;
    else if (KNO_SYMBOLP(env_arg)) {
      env_arg = kno_find_module(env_arg,1);
      if (KNO_LEXENVP(env_arg))
	source_env = (kno_lexenv)env_arg;
      else if (KNO_HASHTABLEP(env_arg))
	source_table = env_arg;
      else NO_ELSE;}
    else NO_ELSE;
    if ( (source_env == NULL) && (!(KNO_TABLEP(source_table))) ) {
      kno_seterr("InvalidSourceEnv","fcnalias_evalfn",
		 KNO_SYMBOL_NAME(sym),
		 env_arg);
      kno_decref(env_arg);
      return KNO_ERROR;}}
  else {
    kno_lexenv e = kno_find_binding(env,sym,1);
    if (e == NULL) {
      kno_seterr("UnboundVariable","fcnalias_evalfn",
		 KNO_SYMBOL_NAME(sym),
		 KNO_VOID);
      return KNO_ERROR;}
    source_env = e;}
  if (KNO_TABLEP(source_table)) {
    lispval val = kno_get(source_table,sym,KNO_VOID);
    if (KNO_VOIDP(val)) {
      kno_seterr("UnboundVariable","fcnalias_evalfn",
		 KNO_SYMBOL_NAME(sym),
		 source_table);
      kno_decref(env_arg);
      return KNO_ERROR;}
    lispval ref = kno_fcn_ref(sym,source_table,val);
    kno_decref(val);
    kno_decref(env_arg);
    return ref;}
  lispval val = symeval(sym,source_env);
  if (KNO_ABORTED(val)) {
    kno_decref(env_arg);
    return val;}
  else if ( (KNO_CONSP(val)) &&
	    ( (KNO_FUNCTIONP(val)) ||
	      (KNO_APPLICABLEP(val)) ||
	      (KNO_EVALFNP(val)) ||
	      (KNO_MACROP(val)) ) ) {
    lispval fcnid = kno_fcn_ref(sym,(lispval)source_env,val);
    kno_decref(env_arg);
    kno_decref(val);
    return fcnid;}
  else {
    kno_decref(env_arg);
    kno_decref(val);
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

lispval eval_choice(lispval expr,kno_lexenv env,
			   kno_stack _stack)
{
  lispval result = EMPTY;
  KNO_PUSH_EVAL(choice_stack,NULL,expr,env);
  DO_CHOICES(each_expr,expr) {
    lispval r = doeval(each_expr,env,choice_stack,0);
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
    lispval val = doeval(val_expr,env,keyval_stack,0);
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
  lispval result = doeval(new_expr,env,macro_stack,0);
  kno_pop_stack(macro_stack);
  return result;
}

/* Lambda application */

static int check_fcn_arity(struct KNO_FUNCTION *proc,int n)
{
  int arity = proc->fcn_arity;
  int min_arity = proc->fcn_min_arity;
  if (n < min_arity) {
    kno_seterr(kno_TooFewArgs,"call",proc->fcn_name,VOID);
    return -1;}
  else if ( (arity>=0) && (n>arity) ) {
    kno_seterr(kno_TooManyArgs,"call",proc->fcn_name,VOID);
    return -1;}
  else return 0;
}

static lispval bad_evalop(lispval head,lispval op)
{
  if ( (KNO_VOIDP(op)) && (KNO_SYMBOLP(head)) )
    return kno_err(kno_UnboundIdentifier,"lisp_eval/op",
		   KNO_SYMBOL_NAME(head),
		   head);
  else if (KNO_VOIDP(op))
    return kno_err(kno_BadEvalOp,"lisp_eval/op",NULL,head);
  else return kno_err(kno_BadEvalOp,"lisp_eval/op",
		      (KNO_SYMBOLP(head)?(KNO_SYMBOL_NAME(head)):(NULL)),
		      op);
}

lispval get_evalop(lispval head,kno_lexenv env,kno_stack stack)
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
      op = lisp_eval(KNO_CAR(head),head,env,stack,0);
    else if (headtype == kno_choice_type)
      op = eval_choice(head,env,stack);
    else op = KNO_VOID;}
  else NO_ELSE;
  if (KNO_FCNIDP(op)) op=kno_fcnid_ref(op);
  else if (KNO_MALLOCDP(op))
    kno_stackvec_push(&(stack->stack_refs),op);
  else NO_ELSE;
  if (RARELY(KNO_VOIDP(op)))
    return bad_evalop(head,op);
  else return op;
}

lispval call_evalfn(lispval evalop,lispval expr,kno_lexenv env,
			   kno_stack stack,int tail)
{
  struct KNO_EVALFN *handler = (kno_evalfn)evalop;
  /* These are evalfns which do all the evaluating themselves */
  int notail = handler->evalfn_notail;
  lispval result = KNO_VOID;
  u8_string old_label = stack->stack_label;
  stack->stack_label = handler->evalfn_name;
  if (notail) {KNO_STACK_SET_TAIL(stack,0);}
  else {KNO_STACK_SET_TAIL(stack,tail);}
  result = handler->evalfn_handler(expr,env,stack);
  stack->stack_label = old_label;
  return result;
}

static lispval iter_dcall(kno_stack stack,lispval fns,int n,kno_argvec argvec)
{
  lispval results = KNO_EMPTY;
  ITER_CHOICES(fscan,flimit,fns);
  while (fscan<flimit) {
    lispval f = *fscan++;
    lispval r = kno_dcall(stack,f,n,argvec);
    if (KNO_ABORTED(r)) {
      kno_decref(results);
      return r;}
    else {KNO_ADD_TO_CHOICE(results,r);}}
  return results;
}

lispval eval_apply(lispval fn,lispval exprs,
		   kno_lexenv env,kno_stack stack,
		   int tail);

/* Evaluating pair expressions using stack frames for each subexpression */
lispval lisp_eval(lispval head,lispval expr,
			kno_lexenv env,kno_stack stack,
			int tail)
{
  if (KNO_OPCODEP(head))
    return vm_eval(head,expr,env,stack,tail);
  lispval result = KNO_VOID, source = expr, op = head;
  u8_string use_label = (KNO_SYMBOLP(head)) ? (KNO_SYMBOL_NAME(head)) :
    (U8S("eval"));
  KNO_STACK_SET_BIT(stack,KNO_STACK_TAIL_POS,tail);
  KNO_START_EVAL(_stack,use_label,expr,env,stack);
 entry:
  KNO_STACK_SET_BIT(stack,KNO_STACK_TAIL_POS,tail);
  _stack->eval_source = source;
  if (KNO_FCNIDP(head)) op = kno_fcnid_ref(head);
  else op = get_evalop(head,env,_stack);
  if (op == KNO_EMPTY) _return op;
  else if (KNO_ABORTED(op)) _return op;
  else NO_ELSE;

  kno_lisp_type optype = KNO_TYPEOF(op);
  switch (optype) {
  case kno_choice_type: case kno_cprim_type: case kno_lambda_type:
  case kno_ffi_type: case kno_rpc_type:
    result = eval_apply(op,KNO_CDR(expr),env,_stack,tail);
    goto got_result;
  case kno_evalfn_type: {
    result = call_evalfn(op,expr,env,_stack,tail);
    goto got_result;}
  case kno_macro_type: {
    struct KNO_MACRO *handler = (kno_macro) op;
    result = macro_eval(handler,expr,env,_stack);
    goto got_result;}
  default:
    if (!(KNO_APPLICABLE_TYPEP(optype)))
      _return kno_err(kno_BadEvalOp,"lisp_eval",NULL,op);
    else result = eval_apply(op,KNO_CDR(expr),env,_stack,tail);}
 got_result:
  if (KNO_PRECHOICEP(result)) {
    result = kno_simplify_choice(result);
    _return result;}
  else _return result;
}

lispval eval_apply(lispval fn,lispval exprs,
		   kno_lexenv env,kno_stack stack,
		   int tail)
{
  kno_function f = (KNO_FUNCTIONP(fn)) ? ((kno_function)fn) : (NULL);
  int call_bits = (f) ? (f->fcn_call) : (0);
  int width = (f) ? (f->fcn_call_width) : (INIT_ARGBUF_LEN), n_args = 0;
  if (KNO_PAIRP(exprs)) { /* count args */
    lispval scan = exprs; while (PAIRP(scan)) {
      n_args++; scan = KNO_CDR(scan);}}
  else if (KNO_NILP(exprs))
    n_args = 0;
  else return kno_err(kno_SyntaxError,"eval_apply",stack->stack_label,VOID);
  if (n_args>width) width = n_args;

  if ((f) && (check_fcn_arity(f,n_args)<0))
    return KNO_ERROR;

  lispval result = KNO_VOID;
  kno_lisp_type fntype = KNO_TYPEOF(fn);

  lispval argbuf[width];
  stack->stack_args = argbuf;
  stack->stack_width = width;
  stack->stack_argc = 0;

  stack->stack_op=fn;
  stack->stack_origin = "eval_apply";
  stack->stack_label = (f) ? (f->fcn_name) :
    (fntype == kno_opcode_type) ? (opcode_name(fn)) :
    (kno_type2name(fntype));

  /* Straightforward eval/apply */
  int prune = (!((call_bits)&(KNO_CALL_XPRUNE)));
  int iter_args = (!((call_bits)&(KNO_CALL_XITER)));

  int arg_info = (PAIRP(exprs)) ?
    (eval_args(n_args,argbuf,exprs,env,stack,prune)) :
    1;

  if (arg_info<0) {
    result=KNO_ERROR; goto cleanup;}
  else if (arg_info == 0) {
    result=KNO_EMPTY; goto cleanup;}
  else if (arg_info == KNO_THROWN_ARG) {
    result=KNO_THROW_VALUE; goto cleanup;}
  else NO_ELSE;

  int choice_args = (U8_BITP(arg_info,KNO_AMBIG_ARGS));
  int qchoice_args = (U8_BITP(arg_info,KNO_QCHOICE_ARGS));
  int decref_args = U8_BITP(arg_info,KNO_CONSED_ARGS);
  int need_iter = (iter_args) && (choice_args);

  if ( (need_iter) || (RARELY(CHOICEP(fn))) ) {
    KNO_STACK_SET_TAIL(stack,0);}
  else {KNO_STACK_SET_TAIL(stack,tail);}

  if ( (!(need_iter)) && (qchoice_args)) unwrap_qchoices(n_args,argbuf);

  if ( (fntype == kno_lambda_type) && (!(need_iter) ) ) {
    struct KNO_LAMBDA *proc = (kno_lambda) fn;
  lambda:
    KNO_STACK_RESET_ARGS(stack);
    result = lambda_call(stack,proc,n_args,(kno_argvec)argbuf,1,tail);}
  else {
    KNO_STACK_SET_ARGS(stack,argbuf,width,n_args,
		       ((decref_args) ? (KNO_STACK_DECREF_ARGS) : (0)));
    if (need_iter)
      result = kno_call(stack,fn,n_args,argbuf);
    else if (RARELY(CHOICEP(fn)))
      result = iter_dcall(stack,fn,n_args,argbuf);
    else result = kno_dcall(stack,fn,n_args,argbuf);}

  KNO_STACK_FREE_ARGS(stack);

 cleanup:
  if (KNO_PRECHOICEP(result))
    return kno_simplify_choice(result);
  else return result;
}

/* Lexrefs support */

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

KNO_EXPORT lispval kno_lexref_name(lispval lexref,kno_lexenv env)
{
  int code = KNO_GET_IMMEDIATE(lexref,kno_lexref_type);
  int up = code/32, across = code%32;
  while ((env) && (up)) {
    if (RARELY((env->env_copy!=NULL))) env = env->env_copy;
    env = env->env_parent;
    up--;}
  if (KNO_USUALLY(env != NULL)) {
    if (RARELY((env->env_copy != NULL))) env = env->env_copy;
    lispval bindings = env->env_bindings;
    if (KNO_USUALLY(KNO_SCHEMAPP(bindings))) {
      struct KNO_SCHEMAP *s = (struct KNO_SCHEMAP *)bindings;
      if ( across < s->schema_length)
	return s->table_schema[across];
      else return KNO_FALSE;}
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

lispval _lexref_error(lispval ref,int up,kno_lexenv env,kno_lexenv root)
{
  lispval root_ptr = (lispval) root;
  lispval env_ptr = (lispval) env;
  u8_byte errbuf[64];
  int code = KNO_GET_IMMEDIATE(ref,kno_lexref_type);
  int init_up = code/32, across=code%32;
  if (env == NULL)
    return kno_err("Bad lexical reference","kno_lexref",
		   u8_bprintf(errbuf,"up=%d(%d),across=%d",
			      init_up,up,across),
		   ((KNO_STATICP(root_ptr)) ? KNO_FALSE : (root_ptr)));
  else return kno_err("Bad lexical reference","kno_lexref",
		      u8_bprintf(errbuf,"up=%d,across=%d",
				 init_up,across),
		      ((!(KNO_STATICP(env_ptr))) ? (env_ptr) :
		       (!(KNO_STATICP(root_ptr))) ? (root_ptr) :
		       (KNO_FALSE)));
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

KNO_EXPORT lispval kno_new_evalfn(lispval mod,u8_string name,u8_string cname,
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
  return (lispval) f;
}

/* The Evaluator */

static lispval eval_evalfn(lispval x,kno_lexenv env,kno_stack stack)
{
  lispval expr_expr = kno_get_arg(x,1);
  lispval expr = kno_eval(expr_expr,env,stack);
  if (KNO_ABORTED(expr)) return expr;
  lispval result = kno_eval(expr,env,stack);
  kno_decref(expr);
  return result;
}

static lispval boundp_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval symbol = kno_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return kno_err(kno_SyntaxError,"boundp_evalfn",NULL,expr);
  else {
    lispval val = symeval(symbol,env);
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
    lispval val = symeval(symbol,env);
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
    lispval val = symeval(symbol,env);
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
    lispval v = symeval(to_eval,env);
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
    lispval v = doeval(to_eval,env,_stack,0);
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
    lispval v = symeval(to_eval,env);
    if (KNO_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == KNO_UNBOUND ) )
      return KNO_TRUE;
    else if  (v == KNO_NULL) {
      return KNO_TRUE;}
    else {
      kno_decref(v);
      return KNO_FALSE;}}
  else {
    lispval v = doeval(to_eval,env,_stack,0);
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
    lispval v = symeval(to_eval,env);
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
    lispval v = doeval(to_eval,env,_stack,0);
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
    lispval v = symeval(to_eval,env);
    if (KNO_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == KNO_UNBOUND ) )
      return KNO_TRUE;
    else if  ( (v == KNO_NULL) || (! (KNO_CHECK_PTR(v)) ) ) {
      u8_log(LOG_WARN,kno_BadPtr,"Bad pointer value %p for %q",v,to_eval);
      return KNO_TRUE;}
    else return KNO_FALSE;}
  else {
    lispval v = kno_eval(to_eval,env,_stack);
    if (KNO_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == KNO_UNBOUND ) )
      return KNO_TRUE;
    else if  ( (v == KNO_NULL) || (! (KNO_CHECK_PTR(v)) ) ) {
      u8_log(LOG_WARN,kno_BadPtr,"Bad pointer value %p for %q",v,to_eval);
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
  else symbol = doeval(symbol_expr,env,stack,0);
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
    env_value = doeval(env_expr,env,stack,0);
    if (KNO_FALSEP(env_value))
      use_env = env;
    else if (KNO_LEXENVP(env_value))
      use_env = (kno_lexenv) env_value;
    else {
      lispval err = kno_err("NotAnEnvironment","symbol_boundp_evalfn",
			    NULL,env_value);
      kno_decref(env_value);
      return err;}}
  lispval v = symeval(symbol,use_env);
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
	lispval val = kno_eval(KNO_CADR(varval),env,stack);
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
      result = kno_eval(elt,consed_env,stack);
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

static lispval withbindings_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  kno_lexenv use_env = env;
  lispval env_expr = kno_get_arg(expr,1), env_arg=KNO_VOID;
  if (KNO_VOIDP(env_expr))
    return kno_err(kno_SyntaxError,"withbindings_evalfn",NULL,expr);
  lispval body = kno_get_body(expr,2);
  if ( (KNO_PAIRP(body)) || (body == KNO_EMPTY_LIST) ) {
    env_arg = kno_eval(env_expr,env,_stack);
    if (KNO_ABORTED(env_arg))
      return env_arg;
    else if (KNO_LEXENVP(env_arg))
      use_env=(kno_lexenv) env_arg;
    else if (KNO_TABLEP(env_arg)) {
      use_env = kno_make_env(env_arg,env);}
    else {
      kno_seterr("BadBindingsArg","withbindings_evalfn",NULL,env_arg);
      kno_decref(env_arg);
      return KNO_ERROR;}
    lispval result = KNO_VOID;
    KNO_DOLIST(sub_expr,body) {
      kno_decref(result);
      result = kno_eval(sub_expr,use_env,_stack);
      if (KNO_ABORTED(result)) break;}
    if (use_env == ((kno_lexenv)env_arg))
      kno_decref(env_arg);
    else kno_free_lexenv(use_env);
    return result;}
  else return kno_err(kno_SyntaxError,"withbindings_evalfn",NULL,expr);
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
    lispval v = doeval(subex,env,_stack,0);
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
    lispval v = doeval(subex,env,_stack,0);
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
    lispval val = symeval(symbol,env);
    if (KNO_ABORTED(val)) return val;
    else if (VOIDP(val))
      return doeval(default_expr,env,_stack,0);
    else if (val == KNO_UNBOUND)
      return doeval(default_expr,env,_stack,0);
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
  return lisp_eval(head,expr,env,s,tail);
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

  kno_def_evalfn(kno_scheme_module,"eval",eval_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"bound?",boundp_evalfn,
		 "`(bound? *sym*)` returns true if *sym* (not evaluated) "
		 "is bound in the current environment.");
  kno_def_evalfn(kno_scheme_module,"unbound?",unboundp_evalfn,
		 "`(unbound? *sym*)` returns true if *sym* (*not evaluated*) "
		 "is *not* bound in the current environment.");
  kno_def_evalfn(kno_scheme_module,"defined?",definedp_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"void?",voidp_evalfn,
		 "`(void? *expr*)` returns true if evaluating *expr* "
		 "returns the **VOID** value.");
  kno_def_evalfn(kno_scheme_module,"default?",defaultp_evalfn,
		 "`(default? *expr*)` returns true if evaluating *expr* "
		 "returns the **DEFAULT** value token.");
  kno_def_evalfn(kno_scheme_module,"constant?",constantp_evalfn,
		 "`(constant? *expr*)` returns true if evaluating *expr* "
		 "returns a Scheme constant.");
  kno_def_evalfn(kno_scheme_module,"bad?",badp_evalfn,
		 "`(bad? *expr*)` returns true if evaluating *expr* "
		 "returns an invalid pointer");
  kno_def_evalfn(kno_scheme_module,"quote",quote_evalfn,
		 "`(quote *x*)` returns the subexpression *x*, "
		 "which is *not* evaluated.");
  kno_def_evalfn(kno_scheme_module,"fcn/alias",fcnalias_evalfn,
		 "`(fcn/alias *sym*)` returns a *fcnid* pointer aliasing the "
		 "definition of *sym* in the current environment.");
  kno_def_evalfn(kno_scheme_module,"%env",env_evalfn,
		 "`(%env)` returns the current lexical environment.");
  kno_def_evalfn(kno_scheme_module,"%modref",modref_evalfn,
		 "`(%modref *modobj* *symbol*) returns the binding of "
		 "*symbol* in the module *modobj*, neither of which are "
		 "evaluated. This is intended for use in automatically "
		 "generated/optimized code");
  kno_def_evalfn(kno_scheme_module,"symbol-bound?",symbol_boundp_evalfn,
		 "`(symbol-bound? *sym* [*env*])` returns #t "
		 "if *sym* is bound in *env*, which defaults to "
		 "the current environment.");

  kno_def_evalfn(kno_scheme_module,"%env/reset!",env_reset_evalfn,
		 "Resets the cached dynamic copy of the current "
		 "environment (if any). This means that procedures "
		 "closed in the current environment will not be "
		 "effected by future changes");

  kno_def_evalfn(kno_scheme_module,"withenv",withenv_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"with-bindings",withbindings_evalfn,
		 "`(with-bindings *bindings* body...)` evaluates body using "
		 "*bindings*. If *bindings* is an environment, the body is "
		 "evaluated in that environment; if *bindings* is a table, "
		 "a new environment is created based on *bindings* and the "
		 "current environment");

  kno_def_evalfn(kno_scheme_module,"void",void_evalfn,"*undocumented*");
  kno_def_evalfn(kno_scheme_module,"!!!null!!!",null_evalfn,"*undocumented*");
  kno_def_evalfn(kno_scheme_module,"break",break_evalfn,"*undocumented*");
  kno_def_evalfn(kno_scheme_module,"default",default_evalfn,"*undocumented*");

  kno_register_config
    ("TAILCALL",
     "Enable/disable tail recursion in the Scheme evaluator. "
     "This may cause various source packages to break in some "
     "or all cases.",
     kno_boolconfig_get,kno_boolconfig_set,
     &kno_enable_tail_calls);

}

void init_vm_c(void);

void kno_init_errfns_c(void);
void kno_init_threads_c(void);
void kno_init_conditionals_c(void);
void kno_init_iterators_c(void);
void kno_init_choicefns_c(void);
void kno_init_binders_c(void);
void kno_init_lambdas_c(void);
void kno_init_macros_c(void);
void kno_init_typeops_c(void);
void kno_init_evalops_c(void);
void kno_init_coreops_c(void);
void kno_init_tableops_c(void);
void kno_init_tableprims_c(void);
void kno_init_stringprims_c(void);
void kno_init_dbprims_c(void);
void kno_init_seqprims_c(void);
void kno_init_modules_c(void);
void kno_init_loadmods_c(void);
void kno_init_load_c(void);
void kno_init_logprims_c(void);
void kno_init_portprims_c(void);
void kno_init_streamprims_c(void);
void kno_init_fileprims_c(void);
void kno_init_driverprims_c(void);
void kno_init_dtypeprims_c(void);
void kno_init_xtypeprims_c(void);
void kno_init_timeprims_c(void);
void kno_init_knosockd_c(void);
void kno_init_sysprims_c(void);
void kno_init_arith_c(void);
void kno_init_reflection_c(void);
void kno_init_reqstate_c(void);
void kno_init_regex_c(void);
void kno_init_promises_c(void);
void kno_init_quasiquote_c(void);
void kno_init_struct_eval_c(void);
void kno_init_sqldbprims_c(void);
void kno_init_history_c(void);
void kno_init_eval_appenv_c(void);
void kno_init_eval_moduleops_c(void);
void kno_init_eval_getopt_c(void);
void kno_init_eval_debug_c(void);
void kno_init_eval_testops_c(void);
void kno_init_configops_c(void);
void kno_init_srvcall_c(void);

static void init_eval_core()
{
  init_localfns();
  init_vm_c();

  kno_init_configops_c();
  kno_init_eval_getopt_c();
  kno_init_eval_debug_c();
  kno_init_eval_testops_c();
  kno_init_evalops_c();
  kno_init_srvcall_c();
  kno_init_tableprims_c();
  kno_init_loadmods_c();
  kno_init_modules_c();
  kno_init_arith_c();
  kno_init_threads_c();
  kno_init_errfns_c();
  kno_init_coreops_c();
  kno_init_tableops_c();
  kno_init_load_c();
  kno_init_conditionals_c();
  kno_init_iterators_c();
  kno_init_choicefns_c();
  kno_init_binders_c();
  kno_init_lambdas_c();
  kno_init_macros_c();
  kno_init_typeops_c();
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
  kno_init_portprims_c();
  kno_init_streamprims_c();
  kno_init_dtypeprims_c();
  kno_init_xtypeprims_c();
  kno_init_seqprims_c();
  kno_init_logprims_c();
  kno_init_timeprims_c();
  kno_init_sysprims_c();
  kno_init_sqldbprims_c();
  kno_init_knosockd_c();

  kno_init_fileprims_c();
  kno_init_driverprims_c();

  u8_threadcheck();

  kno_finish_cmodule(kno_scheme_module);
  kno_finish_cmodule(kno_textio_module);
  kno_finish_cmodule(kno_binio_module);
  kno_finish_cmodule(kno_db_module);
  kno_finish_cmodule(kno_sys_module);

  u8_string *modules = default_scheme_modules;
  while (*modules) {
    u8_string modname = *modules;
    lispval modsym = kno_intern(modname);
    lispval module = kno_find_module(modsym,0);
    if (!(KNO_VOIDP(module)))
      kno_add_default_module(module);
    else u8_log(LOGERR,"BadModule","Couldn't load module %s",modname);
    modules++;}
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
