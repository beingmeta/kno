/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_TABLES (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_FCNIDS (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_STACKS (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_LEXENV (!(KNO_AVOID_CHOICES))

#define KNO_PROVIDE_FASTEVAL (!(KNO_AVOID_CHOICES))

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

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

int kno_scheme_initialized = 0;

u8_string kno_evalstack_type="eval";
u8_string kno_ndevalstack_type="ndeval";

int kno_optimize_tail_calls = 1;

lispval _kno_comment_symbol, fcnids_symbol, sourceref_tag;

static lispval quote_symbol, comment_symbol, moduleid_symbol, source_symbol;

static u8_condition ExpiredThrow=_("Continuation is no longer valid");
static u8_condition DoubleThrow=_("Continuation used twice");
static u8_condition LostThrow=_("Lost invoked continuation");

u8_condition kno_SyntaxError=_("SCHEME expression syntax error"),
  kno_InvalidMacro=_("invalid macro transformer"),
  kno_UnboundIdentifier=_("the variable is unbound"),
  kno_VoidArgument=_("VOID result passed as argument"),
  kno_VoidBinding=_("VOID result used as variable binding"),
  kno_VoidBoolean=_("VOID result as boolean value"),
  kno_NotAnIdentifier=_("not an identifier"),
  kno_TooFewExpressions=_("too few subexpressions"),
  kno_CantBind=_("can't add binding to environment"),
  kno_ReadOnlyEnv=_("Read only environment");

/* Reading from expressions */

KNO_EXPORT lispval _kno_get_arg(lispval expr,int i)
{
  return kno_get_arg(expr,i);
}
KNO_EXPORT lispval _kno_get_body(lispval expr,int i)
{
  return kno_get_body(expr,i);
}

/* Environment functions */

static int bound_in_envp(lispval symbol,kno_lexenv env)
{
  lispval bindings = env->env_bindings;
  if (HASHTABLEP(bindings))
    return kno_hashtable_probe((kno_hashtable)bindings,symbol);
  else if (SLOTMAPP(bindings))
    return kno_slotmap_test((kno_slotmap)bindings,symbol,VOID);
  else if (SCHEMAPP(bindings))
    return kno_schemap_test((kno_schemap)bindings,symbol,VOID);
  else return kno_test(bindings,symbol,VOID);
}

/* Lexrefs */

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

static lispval lexrefp_prim(lispval ref)
{
  if (KNO_TYPEP(ref,kno_lexref_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

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

static lispval coderef_prim(lispval offset)
{
  long long off = FIX2INT(offset);
  return LISPVAL_IMMEDIATE(kno_coderef_type,off);
}

static lispval coderefp_prim(lispval ref)
{
  if (KNO_TYPEP(ref,kno_coderef_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

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

static lispval make_coderef(lispval x)
{
  if ( (KNO_UINTP(x)) && ((KNO_FIX2INT(x)) < KNO_IMMEDIATE_MAX) ) {
    int off = FIX2INT(x);
    return KNO_ENCODEREF(off);}
  else return kno_err(kno_RangeError,"make_coderef",NULL,x);
}

/* Opcodes */

u8_string kno_opcode_names[0x800]={NULL};
int kno_opcodes_length = 0x800;

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

static lispval name2opcode_prim(lispval arg)
{
  if (KNO_SYMBOLP(arg))
    return kno_get_opcode(SYM_NAME(arg));
  else if (STRINGP(arg))
    return kno_get_opcode(CSTRING(arg));
  else return kno_type_error(_("opcode name"),"name2opcode_prim",arg);
}

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

static u8_string opcode_name(lispval opcode)
{
  long opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if ((opcode_offset<kno_opcodes_length) &&
      (kno_opcode_names[opcode_offset]))
    return kno_opcode_names[opcode_offset];
  else return "anonymous_opcode";
}

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
  u8_free(sf->evalfn_name);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

/* Checking if eval is needed */

KNO_EXPORT int kno_choice_evalp(lispval x)
{
  if (KNO_NEED_EVALP(x))
    return 1;
  else if (KNO_AMBIGP(x)) {
    KNO_DO_CHOICES(e,x) {
      if (KNO_NEED_EVALP(e)) {
        KNO_STOP_DO_CHOICES;
        return 1;}}
    return 0;}
  else return 0;
}

KNO_EXPORT
lispval kno_eval_exprs(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  return eval_body(body,env,stack,tail);
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

/* Unpacking expressions, non-inline versions */

/* Quote */

static lispval quote_evalfn(lispval obj,kno_lexenv env,kno_stack stake)
{
  if ((PAIRP(obj)) && (PAIRP(KNO_CDR(obj))) &&
      ((KNO_CDR(KNO_CDR(obj))) == NIL))
    return kno_incref(KNO_CAR(KNO_CDR(obj)));
  else return kno_err(kno_SyntaxError,"QUOTE",NULL,obj);
}

/* Handle sourcerefs */

static lispval handle_sourcerefs(lispval expr,struct KNO_STACK *stack)
{
  while (KNO_CAR(expr) == KNO_SOURCEREF_OPCODE) {
    expr = KNO_CDR(expr);
    lispval source = KNO_CAR(expr);
    expr = KNO_CDR(expr);
    stack->stack_source=source;
    if (!(KNO_PAIRP(expr))) return expr;}
  return expr;
}

/* The evaluator itself */

static int applicable_choicep(lispval choice);

static lispval eval_apply(u8_string fname,
                          lispval fn,lispval arg_exprs,
                          kno_lexenv env,
                          struct KNO_STACK *stack,
                          int tail);

static lispval pair_eval(lispval head,lispval expr,kno_lexenv env,
                         struct KNO_STACK *_stack,
                         int tail,int pushed);
static lispval schemap_eval(lispval expr,kno_lexenv env,
                            struct KNO_STACK *_stack);
static lispval opcode_eval(lispval opcode,lispval expr,
                           kno_lexenv env,
                           kno_stack _stack,
                           int tail);
static lispval choice_eval(lispval expr,kno_lexenv env,
                           struct KNO_STACK *_stack,
                           int tail);

KNO_EXPORT
lispval kno_stack_eval(lispval expr,kno_lexenv env,
                       struct KNO_STACK *_stack,
                       int tail)
{
  if (_stack==NULL) _stack=kno_stackptr;
  if (KNO_IMMEDIATEP(expr)) {
    switch (KNO_PTR_TYPE(expr)) {
    case kno_lexref_type:
      return kno_lexref(expr,env);
    case kno_symbol_type: {
      lispval val = kno_symeval(expr,env);
      if (PRED_FALSE(VOIDP(val)))
        return kno_err(kno_UnboundIdentifier,"kno_eval",
                       SYM_NAME(expr),expr);
      else return simplify_value(val);}
    default:
      return expr;}}
  else if (KNO_CONSP(expr)) {
    kno_ptr_type ctype = KNO_CONSPTR_TYPE(expr);
    switch (ctype) {
    case kno_pair_type: {
      lispval result = VOID;
      KNO_PUSH_STACK(eval_stack,kno_evalstack_type,NULL,expr);
      result = pair_eval(KNO_CAR(expr),expr,env,eval_stack,tail,1);
      kno_pop_stack(eval_stack);
      return result;}
    case kno_choice_type:
      return choice_eval(expr,env,_stack,tail);
    case kno_prechoice_type: {
      lispval normalized = kno_make_simple_choice(expr);
      lispval result = kno_stack_eval(normalized,env,_stack,tail);
      kno_decref(normalized);
      return result;}
    case kno_slotmap_type:
      return kno_deep_copy(expr);
    case kno_schemap_type: {
      lispval result = VOID;
      KNO_PUSH_STACK(eval_stack,kno_evalstack_type,NULL,expr);
      result = schemap_eval(expr,env,eval_stack);
      kno_pop_stack(eval_stack);
      return result;}
    default:
      return kno_incref(expr);}}
  else return expr;
}

static lispval choice_eval(lispval expr,kno_lexenv env,
                           struct KNO_STACK *_stack,
                           int tail)
{
  lispval result = EMPTY;
  KNO_PUSH_STACK(nd_eval_stack,kno_ndevalstack_type,NULL,expr);
  DO_CHOICES(each_expr,expr) {
    lispval r = stack_eval(each_expr,env,nd_eval_stack);
    if (KNO_ABORTED(r)) {
      KNO_STOP_DO_CHOICES;
      kno_decref(result);
      kno_pop_stack(nd_eval_stack);
      return r;}
    else {CHOICE_ADD(result,r);}}
  kno_pop_stack(nd_eval_stack);
  return simplify_value(result);
}

/* Function application */

static lispval get_headval(lispval head,kno_lexenv env,kno_stack eval_stack,
                           int *gc_headval);

lispval pair_eval(lispval head,lispval expr,kno_lexenv env,
                  struct KNO_STACK *eval_stack,
                  int tail,int fresh_stack)
{
  u8_string label=(SYMBOLP(head)) ? (SYM_NAME(head)) :
    (KNO_OPCODEP(head)) ? (opcode_name(head)) : (NULL);
  if (head == KNO_SOURCEREF_OPCODE) {
    expr = handle_sourcerefs(expr,eval_stack);
    head = (KNO_PAIRP(expr)) ? (KNO_CAR(expr)) : (KNO_VOID);
    label=(SYMBOLP(head)) ? (SYM_NAME(head)) :
      (KNO_OPCODEP(head)) ? (opcode_name(head)) : (NULL);}
  int flags = eval_stack->stack_flags;
  if (U8_BITP(flags,KNO_STACK_DECREF_OP)) {
    kno_decref(eval_stack->stack_op);}
  if (label) {
    if (U8_BITP(flags,KNO_STACK_FREE_LABEL)) {
      u8_free(eval_stack->stack_label);}
    eval_stack->stack_label=label;}
  eval_stack->stack_flags = KNO_STACK_LIVE | ( (flags) & (KNO_STACK_FREE_SRC) );
  int gc_head=0;
  lispval result = VOID, headval = (KNO_OPCODEP(head)) ? (head) :
    (get_headval(head,env,eval_stack,&gc_head));
  if (gc_head) {
    KNO_ADD_TO_CHOICE(eval_stack->stack_vals,headval);}

  kno_ptr_type headtype = KNO_PTR_TYPE(headval);
  switch (headtype) {
  case kno_opcode_type:
    result = opcode_eval(headval,expr,env,eval_stack,tail); break;
  case kno_cprim_type: case kno_lambda_type: {
    struct KNO_FUNCTION *f = (struct KNO_FUNCTION *) headval;
    if (f->fcn_name) eval_stack->stack_label=f->fcn_name;
    result = eval_apply(f->fcn_name,headval,KNO_CDR(expr),env,eval_stack,tail);
    break;}
  case kno_evalfn_type: {
    struct KNO_EVALFN *handler = (kno_evalfn)headval;
    if (fresh_stack) {
      /* These are evalfns which do all the evaluating themselves */
      if (handler->evalfn_name) eval_stack->stack_label=handler->evalfn_name;
      result = handler->evalfn_handler(expr,env,eval_stack);}
    else {
      KNO_NEW_STACK(eval_stack,kno_evalstack_type,handler->evalfn_name,expr);
      result = pair_eval(KNO_CAR(expr),expr,env,_stack,tail,1);
      kno_pop_stack(_stack);}
    break;}
  case kno_macro_type: {
    /* These expand into expressions which are
       then evaluated. */
    struct KNO_MACRO *macrofn=
      kno_consptr(struct KNO_MACRO *,headval,kno_macro_type);
    eval_stack->stack_type="macro";
    lispval xformer = macrofn->macro_transformer;
    lispval new_expr = kno_call(eval_stack,xformer,1,&expr);
    if (KNO_ABORTED(new_expr)) {
      u8_string show_name = macrofn->macro_name;
      if (show_name == NULL) show_name = label;
      result = kno_err(kno_SyntaxError,_("macro expansion"),show_name,new_expr);}
    else result = kno_stack_eval(new_expr,env,eval_stack,tail);
    kno_decref(new_expr);
    break;}
  case kno_choice_type: {
    int applicable = applicable_choicep(headval);
    if (applicable)
      result = eval_apply("fnchoice",headval,KNO_CDR(expr),env,eval_stack,0);
    else result =kno_err(kno_SyntaxError,"kno_stack_eval",
                         "not applicable or evalfn",
                         headval);
    break;}
  default:
    if (kno_functionp[headtype]) {
      struct KNO_FUNCTION *f = (struct KNO_FUNCTION *) headval;
      result = eval_apply(f->fcn_name,headval,KNO_CDR(expr),env,
                          eval_stack,tail);}
    else if (kno_applyfns[headtype]) {
      result = eval_apply("extfcn",headval,KNO_CDR(expr),env,eval_stack,tail);}
    else if (KNO_ABORTED(headval)) {
      result=headval;}
    else if (PRED_FALSE(VOIDP(headval))) {
      result=kno_err(kno_UnboundIdentifier,"for function",
                     ((SYMBOLP(head))?(SYM_NAME(head)):
                      (NULL)),
                     head);}
    else if (EMPTYP(headval) )
      result=EMPTY;
    else result=kno_err(kno_NotAFunction,NULL,NULL,headval);}
  if (!(KNO_CHECK_PTR(result)))
    return kno_err(kno_NullPtr,"pair_eval",NULL,expr);
  else if (!tail) {
    if (KNO_TAILCALLP(result))
      result=kno_finish_call(result);
    else {}}
  return simplify_value(result);
}

static int applicable_choicep(lispval headvals)
{
  DO_CHOICES(fcn,headvals) {
    lispval hv = (KNO_FCNIDP(fcn)) ? (kno_fcnid_ref(fcn)) : (fcn);
    int hvtype = KNO_PRIM_TYPE(hv);
    /* Check that all the elements are either applicable or special
       forms and not mixed */
    if ( (hvtype == kno_cprim_type) ||
         (hvtype == kno_lambda_type) ||
         (kno_applyfns[hvtype]) ) {}
    else if ((hvtype == kno_evalfn_type) ||
             (hvtype == kno_macro_type))
      return 0;
    /* In this case, all the headvals so far are evalfns */
    else return 0;}
  return 1;
}

KNO_EXPORT lispval _kno_eval(lispval expr,kno_lexenv env)
{
  lispval result = kno_tail_eval(expr,env);
  if (KNO_ABORTED(result)) return result;
  else return kno_finish_call(result);
}

/* Applying functions */

#define ND_ARGP(v) ((CHOICEP(v))||(QCHOICEP(v)))

KNO_FASTOP int commentp(lispval arg)
{
  return
    (PRED_FALSE
     ((PAIRP(arg)) &&
      (KNO_EQ(KNO_CAR(arg),comment_symbol))));
}

static lispval get_headval(lispval head,kno_lexenv env,kno_stack eval_stack,
                           int *gc_headval)
{
  lispval headval = VOID;
  if (KNO_IMMEDIATEP(head)) {
    if (KNO_OPCODEP(head))
      return head;
    else if (KNO_LEXREFP(head)) {
      headval=kno_lexref(head,env);
      if ( (KNO_CONSP(headval)) && (KNO_MALLOCD_CONSP(headval)) )
        *gc_headval=1;}
    else if (KNO_SYMBOLP(head)) {
      if (head == quote_symbol) return KNO_QUOTE_OPCODE;
      headval=kno_symeval(head,env);
      if (KNO_CONSP(headval)) *gc_headval=1;}
    else headval = head;}
  else if ( (PAIRP(head)) || (CHOICEP(head)) ) {
    headval=stack_eval(head,env,eval_stack);
    headval=simplify_value(headval);
    *gc_headval=1;}
  else headval=head;
  if (KNO_ABORTED(headval))
    return headval;
  else if (KNO_FCNIDP(headval)) {
    headval=kno_fcnid_ref(headval);
    if (PRECHOICEP(headval)) {
      headval=kno_make_simple_choice(headval);
      *gc_headval=1;}}
  else NO_ELSE;
  return headval;
}

static int fast_eval_args = 1;

KNO_FASTOP lispval arg_eval(lispval x,kno_lexenv env,struct KNO_STACK *stack)
{
  switch (KNO_PTR_MANIFEST_TYPE(x)) {
  case kno_oid_ptr_type: case kno_fixnum_ptr_type:
    return x;
  case kno_immediate_ptr_type:
    if (KNO_TYPEP(x,kno_lexref_type))
      return kno_lexref(x,env);
    else if (KNO_SYMBOLP(x)) {
      lispval val = kno_symeval(x,env);
      if (PRED_FALSE(KNO_VOIDP(val)))
        return kno_err(kno_UnboundIdentifier,"kno_eval",KNO_SYMBOL_NAME(x),x);
      else return val;}
    else return x;
  case kno_cons_ptr_type: {
    kno_ptr_type type = KNO_CONSPTR_TYPE(x);
    switch (type) {
    case kno_pair_type:
      if (fast_eval_args)
        return pair_eval(KNO_CAR(x),x,env,stack,0,0);
      else return kno_stack_eval(x,env,stack,0);
    case kno_choice_type: case kno_prechoice_type:
      return kno_stack_eval(x,env,stack,0);
    case kno_slotmap_type:
      return kno_deep_copy(x);
    case kno_schemap_type:
      return schemap_eval(x,env,stack);
    default:
      return kno_incref(x);}}
  default: /* Never reached */
    return x;
  }
}

static lispval eval_apply(u8_string fname,
                          lispval fn,lispval arg_exprs,
                          kno_lexenv env,
                          struct KNO_STACK *stack,
                          int tail)
{
  int max_args = 0, min_args = 0, n_fns = 0;
  int lambda = -1, gc_args = 0, nd_args = 0, d_prim = 1, qc_args = 0;
  if (KNO_EXPECT_FALSE(KNO_CHOICEP(fn))) {
    KNO_DO_CHOICES(f,fn) {
      if (KNO_FUNCTIONP(f)) {
        struct KNO_FUNCTION *fcn=KNO_XFUNCTION(f);
        int max_arity = fcn->fcn_arity, min_arity = fcn->fcn_min_arity;
        if (max_args >= 0) {
          if (max_arity < 0) max_args = max_arity;
          else if (max_arity > max_args)
            max_args = max_arity;
          else {}}
        if (min_args >= 0) {
          if (min_arity < min_args)
            min_args = min_arity;}
        if (fcn->fcn_ndcall) d_prim = 0;
        if (fcn->fcn_notail) tail = 0;
        if (lambda < 0) {
          if (KNO_LAMBDAP(fn)) lambda = 1;}
        else if (KNO_LAMBDAP(fn))
          lambda = 0;
        else NO_ELSE;
        n_fns++;}
      else if (!(KNO_APPLICABLEP(f))) {
        KNO_STOP_DO_CHOICES;
        return kno_err("NotApplicable","eval_apply",NULL,f);}
      else n_fns++;}}
  else if (KNO_FUNCTIONP(fn)) {
    struct KNO_FUNCTION *fcn=KNO_XFUNCTION(fn);
    lambda = KNO_LAMBDAP(fn);
    d_prim = (! (fcn->fcn_ndcall) );
    tail   = (! (fcn->fcn_notail) );
    max_args = fcn->fcn_arity;
    min_args = fcn->fcn_min_arity;}
  else if (KNO_APPLICABLEP(fn))
    n_fns++;
  else return kno_err("NotApplicable","eval_apply",NULL,fn);

  if (n_fns > 1) tail = 0;
  int flags = stack->stack_flags;
  int arg_i = 0, argbuf_len = (max_args>=0) ? (max_args) : (min_args+8);
  lispval init_argbuf[argbuf_len], *argbuf=init_argbuf;
  lispval result = KNO_VOID;
  lispval scan = arg_exprs;
  lispval op = (U8_BITP(flags,KNO_STACK_DECREF_OP)) ? (KNO_VOID) :
    (stack->stack_op);
  u8_string label = (U8_BITP(flags,KNO_STACK_FREE_LABEL)) ? (NULL) :
    (stack->stack_label);
  u8_string sourcefile = stack->stack_src;
  while (KNO_PAIRP(scan)) {
    lispval arg_expr = pop_arg(scan);
    if (commentp(arg_expr)) continue;
    else if ( (max_args >= 0) && (arg_i > max_args) ) {
      if (gc_args) kno_decref_vec(argbuf,arg_i);
      return kno_err(kno_TooManyArgs,"eval_apply",fname,arg_expr);}
    lispval arg_val = arg_eval(arg_expr,env,stack);
    if (PRED_FALSE(VOIDP(arg_val))) {
      if (gc_args) kno_decref_vec(argbuf,arg_i);
      return kno_err(kno_VoidArgument,"eval_apply/arg",NULL,arg_expr);}
    else if (KNO_ABORTED(arg_val)) {
      /* Clean up the arguments we've already evaluated */
      if (gc_args) kno_decref_vec(argbuf,arg_i);
      return arg_val;}
    else {
      /* Otherwise, reset the stack op and label, if advisable */
      flags = stack->stack_flags;
      if (label) {
        if (U8_BITP(flags,KNO_STACK_FREE_LABEL)) u8_free(stack->stack_label);
        stack->stack_label = label;}
      if (!(KNO_VOIDP(op))) {
        if (U8_BITP(flags,KNO_STACK_DECREF_OP)) kno_decref(stack->stack_op);
        stack->stack_op = op;}
      if (sourcefile)
        stack->stack_src = sourcefile;}
    if (PRED_FALSE( (d_prim) && (EMPTYP(arg_val)) )) {
      /* Clean up the arguments we've already evaluated */
      if (gc_args) kno_decref_vec(argbuf,arg_i);
      return arg_val;}
    else if (CONSP(arg_val)) {
      if (KNO_PRECHOICEP(arg_val)) arg_val = kno_simplify_choice(arg_val);
      if ( (nd_args == 0) && (CHOICEP(arg_val)) )
        nd_args = 1;
      else if ( (qc_args == 0) && (QCHOICEP(arg_val)) )
        qc_args = 1;
      else NO_ELSE;
      gc_args = 1;}
    else {}
    if (arg_i >= argbuf_len) {
      lispval new_argbuf_len = argbuf_len*2;
      lispval *new_argbuf = alloca(sizeof(lispval)*new_argbuf_len);
      memcpy(new_argbuf,argbuf,sizeof(lispval)*arg_i);
      argbuf_len = new_argbuf_len;
      argbuf=new_argbuf;}
    argbuf[arg_i++]=arg_val;}
  if ( (tail) && (lambda) && (kno_optimize_tail_calls) &&
       (! ((d_prim) && (nd_args)) ) )
    result=kno_tail_call(fn,arg_i,argbuf);
  else if ((CHOICEP(fn)) || (PRECHOICEP(fn)) ||
           ((d_prim) && ( (nd_args) || (qc_args) ) ))
    result=kno_ndcall(stack,fn,arg_i,argbuf);
  else if (gc_args)
    result=kno_dcall(stack,fn,arg_i,argbuf);
  else return kno_dcall(stack,fn,arg_i,argbuf);
  if (gc_args) kno_decref_vec(argbuf,arg_i);
  return result;
}

/* Evaluating schemaps */

lispval schemap_eval(lispval expr,kno_lexenv env,
                     struct KNO_STACK *eval_stack)
{
  struct KNO_SCHEMAP *skmap = (kno_schemap) expr;
  int n = skmap->schema_length;
  lispval *schema = skmap->table_schema;
  lispval *vals = skmap->schema_values;
  lispval new_vals[n];
  int i = 0; while (i < n) {
    lispval val_expr = vals[i];
    lispval val = arg_eval(val_expr,env,eval_stack);
    if (KNO_ABORTP(val)) {
      kno_decref_vec(new_vals,i);
      return val;}
    else new_vals[i++] = val;}
  lispval result = kno_make_schemap(NULL,n,KNO_SCHEMAP_INLINE,schema,new_vals);
  /* Set the template, which allows the new schemap to share the schema */
  struct KNO_SCHEMAP *newmap = (kno_schemap) result;
  newmap->schemap_template=expr; kno_incref(expr);
  return result;
}

/* Opcode eval */

#include "opcode_defs.c"

static lispval opcode_eval(lispval opcode,lispval expr,
                           kno_lexenv env,
                           kno_stack _stack,
                           int tail)
{
  lispval args = KNO_CDR(expr);

  if (KNO_SPECIAL_OPCODEP(opcode))
    return handle_special_opcode(opcode,args,expr,env,_stack,tail);
  else if ( (KNO_D1_OPCODEP(opcode)) || (KNO_ND1_OPCODEP(opcode)) ) {
    int nd_call = (KNO_ND1_OPCODEP(opcode));
    lispval results = KNO_EMPTY_CHOICE;
    lispval arg = pop_arg(args), val = arg_eval(arg,env,_stack);
    if (KNO_ABORTED(val)) _return val;
    else if (KNO_VOIDP(val))
      return kno_err(kno_VoidArgument,"OPCODE_APPLY",opcode_name(opcode),arg);
    else if (PRED_TRUE(args == KNO_EMPTY_LIST)) {}
    else return kno_err(kno_TooManyArgs,"opcode_eval",opcode_name(opcode),expr);
    if (KNO_PRECHOICEP(val))
      val = kno_simplify_choice(val);
    /* Get results */
    if ( (!(nd_call)) && (KNO_EMPTY_CHOICEP(val)) )
      results = KNO_EMPTY_CHOICE;
    else if (nd_call)
      results = nd1_call(opcode,val);
    else if (KNO_CHOICEP(val)) {
      KNO_DO_CHOICES(v,val) {
        lispval r = d1_call(opcode,val);
        if (KNO_ABORTP(r)) {
          kno_decref(results);
          KNO_STOP_DO_CHOICES;
          _return r;}
        else {KNO_ADD_TO_CHOICE(results,r);}}}
    else results = d1_call(opcode,val);
    kno_decref(val);
    return results;}
  else if ( (KNO_D2_OPCODEP(opcode)) ||
            (KNO_ND2_OPCODEP(opcode)) ||
            (KNO_NUMERIC_OPCODEP(opcode)) ) {
    int nd_call = (KNO_ND2_OPCODEP(opcode));
    lispval results = KNO_EMPTY_CHOICE;
    int numericp = (KNO_NUMERIC_OPCODEP(opcode));
    lispval arg1 = pop_arg(args), arg2 = pop_arg(args);
    if (PRED_FALSE(KNO_VOIDP(arg1)))
      return kno_err(kno_TooFewArgs,"opcode_eval",opcode_name(opcode),
                     expr);
    else if (PRED_FALSE(args != KNO_EMPTY_LIST))
      return kno_err(kno_TooManyArgs,"opcode_eval",opcode_name(opcode),expr);
    else NO_ELSE;
    lispval val1 = arg_eval(arg1,env,_stack), val2;
    if (KNO_ABORTED(val1))
      return val1;
    else if ( (KNO_EMPTY_CHOICEP(val1)) && (!(nd_call)) )
      return val1;
    else val2 = arg_eval(arg2,env,_stack);
    if (KNO_ABORTED(val2)) {
      kno_decref(val1);
      return val2;}
    else if ( (KNO_EMPTY_CHOICEP(val2)) && (!(nd_call)) ) {
      kno_decref(val1);
      return val2;}
    if (KNO_PRECHOICEP(val1)) val1 = kno_simplify_choice(val1);
    if (KNO_PRECHOICEP(val2)) val2 = kno_simplify_choice(val2);
    if ( (KNO_CHOICEP(val1)) || (KNO_CHOICEP(val2)) ) {
      if (nd_call)
        results = nd2_call(opcode,val1,val2);
      else {
        KNO_DO_CHOICES(v1,val1) {
          KNO_DO_CHOICES(v2,val2) {
            lispval r = (numericp) ? (numop_call(opcode,v1,v2)) :
              (d2_call(opcode,v1,v2));
            if (KNO_ABORTP(r)) {
              kno_decref(results);
              KNO_STOP_DO_CHOICES;
              results = r;
              break;}
            else {KNO_ADD_TO_CHOICE(results,r);}}
          if (KNO_ABORTP(results)) {
            KNO_STOP_DO_CHOICES;
            break;}}}}
    else if (numericp)
      results = numop_call(opcode,val1,val2);
    else if (KNO_ND2_OPCODEP(opcode))
      results = nd2_call(opcode,val1,val2);
    else results = d2_call(opcode,val1,val2);
    kno_decref(val1); kno_decref(val2);
    return results;}
  else if (KNO_TABLE_OPCODEP(opcode))
    return handle_table_opcode(opcode,expr,env,_stack,tail);
  else return kno_err(_("Invalid opcode"),"numop_call",NULL,expr);
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

KNO_EXPORT void kno_new_evalfn(lispval mod,u8_string name,
                               u8_string filename,u8_string doc,
                               kno_eval_handler fn)
{
  struct KNO_EVALFN *f = u8_alloc(struct KNO_EVALFN);
  KNO_INIT_CONS(f,kno_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_handler = fn;
  f->evalfn_filename = filename;
  f->evalfn_documentation = doc;
  kno_store(mod,kno_getsym(name),LISP_CONS(f));
  f->evalfn_moduleid = kno_get(mod,moduleid_symbol,KNO_VOID);
  kno_decref(LISP_CONS(f));
}

/* The Evaluator */

static lispval eval_evalfn(lispval x,kno_lexenv env,kno_stack stack)
{
  lispval expr_expr = kno_get_arg(x,1);
  lispval expr = kno_stack_eval(expr_expr,env,stack,0);
  if (KNO_ABORTED(expr)) return expr;
  lispval result = kno_stack_eval(expr,env,stack,0);
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
    else if (KNO_CONSTANTP(v))
      return KNO_TRUE;
    else {
      kno_decref(v);
      return KNO_FALSE;}}
  else {
    lispval v = kno_eval(to_eval,env);
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
    lispval v = kno_eval(to_eval,env);
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
    lispval v = kno_eval(to_eval,env);
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
    lispval v = kno_eval(to_eval,env);
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
  else symbol = kno_stack_eval(symbol_expr,env,stack,0);
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
    env_value = kno_stack_eval(env_expr,env,stack,0);
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
        lispval var = KNO_CAR(varval), val = kno_eval(KNO_CADR(varval),env);
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
        break;}
      kno_decref(keys);
      if (abort) return KNO_ERROR;}}
  else return kno_err(kno_SyntaxError,cxt,NULL,expr);
  /* Execute the body */ {
    lispval result = VOID;
    lispval body = kno_get_body(expr,2);
    KNO_DOLIST(elt,body) {
      kno_decref(result);
      result = kno_eval(elt,consed_env);
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

static lispval apply_lexpr(int n,lispval *args)
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

static lispval call_continuation(struct KNO_FUNCTION *f,lispval arg)
{
  struct KNO_CONTINUATION *cont = (struct KNO_CONTINUATION *)f;
  if (cont->retval == KNO_NULL)
    return kno_err(ExpiredThrow,"call_continuation",NULL,arg);
  else if (VOIDP(cont->retval)) {
    cont->retval = kno_incref(arg);
    return KNO_THROW_VALUE;}
  else return kno_err(DoubleThrow,"call_continuation",NULL,arg);
}

static lispval callcc (lispval proc)
{
  lispval continuation, value;
  struct KNO_CONTINUATION *f = u8_alloc(struct KNO_CONTINUATION);
  KNO_INIT_FRESH_CONS(f,kno_cprim_type);
  f->fcn_name="continuation"; f->fcn_filename = NULL;
  f->fcn_ndcall = 1; f->fcn_xcall = 1; f->fcn_arity = 1; f->fcn_min_arity = 1;
  f->fcn_typeinfo = NULL; f->fcn_defaults = NULL;
  f->fcn_handler.xcall1 = call_continuation; f->retval = VOID;
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

static lispval cachecall(int n,lispval *args)
{
  if (HASHTABLEP(args[0]))
    return kno_xcachecall((kno_hashtable)args[0],args[1],n-2,args+2);
  else return kno_cachecall(args[0],n-1,args+1);
}

static lispval cachecall_probe(int n,lispval *args)
{
  if (HASHTABLEP(args[0]))
    return kno_xcachecall_try((kno_hashtable)args[0],args[1],n-2,args+2);
  else return kno_cachecall_try(args[0],n-1,args+1);
}

static lispval cachedcallp(int n,lispval *args)
{
  if (HASHTABLEP(args[0]))
    if (kno_xcachecall_probe((kno_hashtable)args[0],args[1],n-2,args+2))
      return KNO_TRUE;
    else return KNO_FALSE;
  else if (kno_cachecall_probe(args[0],n-1,args+1))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval clear_callcache(lispval arg)
{
  kno_clear_callcache(arg);
  return VOID;
}

static lispval tcachecall(int n,lispval *args)
{
  return kno_tcachecall(args[0],n-1,args+1);
}

static lispval with_threadcache_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct KNO_THREAD_CACHE *tc = kno_push_threadcache(NULL);
  lispval value = VOID;
  KNO_DOLIST(each,KNO_CDR(expr)) {
    kno_decref(value); value = VOID; value = kno_eval(each,env);
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
    kno_decref(value); value = VOID; value = kno_eval(each,env);
    if (KNO_ABORTED(value)) {
      if (tc) kno_pop_threadcache(tc);
      return value;}}
  if (tc) kno_pop_threadcache(tc);
  return value;
}

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

/* Making DTPROCs */

static lispval make_dtproc(lispval name,lispval server,lispval min_arity,
                           lispval arity,lispval minsock,lispval maxsock,
                           lispval initsock)
{
  lispval result;
  if (VOIDP(min_arity))
    result = kno_make_dtproc(SYM_NAME(name),CSTRING(server),1,-1,-1,
                             FIX2INT(minsock),FIX2INT(maxsock),
                             FIX2INT(initsock));
  else if (VOIDP(arity))
    result = kno_make_dtproc
      (SYM_NAME(name),CSTRING(server),
       1,kno_getint(arity),kno_getint(arity),
       FIX2INT(minsock),FIX2INT(maxsock),
       FIX2INT(initsock));
  else result=
         kno_make_dtproc
         (SYM_NAME(name),CSTRING(server),1,
          kno_getint(arity),kno_getint(min_arity),
          FIX2INT(minsock),FIX2INT(maxsock),
          FIX2INT(initsock));
  return result;
}

/* Getting documentation */

KNO_EXPORT
u8_string kno_get_documentation(lispval x)
{
  lispval proc = (KNO_FCNIDP(x)) ? (kno_fcnid_ref(x)) : (x);
  kno_ptr_type proctype = KNO_PTR_TYPE(proc);
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
      lambda->fcn_free_doc = 1;
      return u8_strdup(out.u8_outbuf);}}
  else if (kno_functionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(proc);
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

/* Helpful forms and functions */

static lispval void_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = KNO_CDR(expr);
  KNO_DOLIST(subex,body) {
    lispval v = kno_stack_eval(subex,env,_stack,0);
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
    lispval v = kno_stack_eval(subex,env,_stack,0);
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
      return kno_eval(default_expr,env);
    else if (val == KNO_UNBOUND)
      return kno_eval(default_expr,env);
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

static lispval check_version_prim(int n,lispval *args)
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
      if (!(FIXNUMP(args[i]))) {
        kno_seterr(kno_TypeError,"check_version_prim",NULL,args[i]);
        return -1;}
      else i++;}
    return KNO_TRUE;}
  else return KNO_FALSE;
}

static lispval require_version_prim(int n,lispval *args)
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
    lispval version_vec = kno_make_vector(n,args);
    kno_seterr("VersionError","require_version_prim",
               u8_sprintf(buf,50,"Version is %s",KNO_REVISION),
	       /* We don't need to incref *args* because they're all fixnums */
	       version_vec);
    kno_decref(version_vec);
    return KNO_ERROR;}
}

/* Choice functions */

static lispval fixchoice_prim(lispval arg)
{
  if (PRECHOICEP(arg))
    return kno_make_simple_choice(arg);
  else return kno_incref(arg);
}

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
        kno_seterr(kno_RangeError,"choiceref_prim",
                   u8_sprintf(buf,50,"%lld",i),
                   arg);
        return KNO_ERROR;}
      else {
        lispval elt = KNO_XCHOICE_DATA(ch)[i];
        return kno_incref(elt);}}
    else if (PRECHOICEP(arg)) {
      lispval simplified = kno_make_simple_choice(arg);
      lispval result = choiceref_prim(simplified,off);
      kno_decref(simplified);
      return result;}
    else if (i == 0)
      return kno_incref(arg);
    else {
      u8_byte buf[50];
      kno_seterr(kno_RangeError,"choiceref_prim",
                 u8_sprintf(buf,50,"%lld",i),kno_incref(arg));
      return KNO_ERROR;}}
  else return kno_type_error("fixnum","choiceref_prim",off);
}

/* FFI */

#if KNO_ENABLE_FFI
static lispval ffi_proc(int n,lispval *args)
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

static lispval ffi_found_prim(lispval name,lispval modname)
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
static lispval ffi_found_prim(lispval name,lispval modname)
{
  return KNO_FALSE;
}
#endif

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

  kno_init_module_tables();
  init_opcode_names();
}

static void init_localfns()
{
  kno_def_evalfn(kno_scheme_module,"EVAL","",eval_evalfn);
  kno_def_evalfn(kno_scheme_module,"BOUND?",
                 "`(BOUND? *sym*)` returns true if *sym* (not evaluated) "
                 "is bound in the current environment.",
                 boundp_evalfn);
  kno_def_evalfn(kno_scheme_module,"UNBOUND?",
                 "`(UNBOUND? *sym*)` returns true if *sym* (*not evaluated*) "
                 "is *not* bound in the current environment.",unboundp_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFINED?","",definedp_evalfn);
  kno_def_evalfn(kno_scheme_module,"VOID?",
                 "`(VOID? *expr*)` returns true if evaluating *expr* "
                 "returns the **VOID** value.",
                 voidp_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFAULT?",
                 "`(DEFAULT? *expr*)` returns true if evaluating *expr* "
                 "returns the **DEFAULT** value token.",
                 defaultp_evalfn);
  kno_def_evalfn(kno_scheme_module,"CONSTANT?",
                 "`(CONSTANT? *expr*)` returns true if evaluating *expr* "
                 "returns a Scheme constant.",
                 constantp_evalfn);
  kno_def_evalfn(kno_scheme_module,"BAD?",
                 "`(BAD? *expr*)` returns true if evaluating *expr* "
                 "returns an invalid pointer",
                 badp_evalfn);
  kno_def_evalfn(kno_scheme_module,"QUOTE",
                 "`(QUOTE *x*)` returns the subexpression *x*, "
                 "which is *not* evaluated.",
                 quote_evalfn);
  kno_def_evalfn(kno_scheme_module,"%ENV",
                 "`(%ENV)` returns the current lexical environment.",
                 env_evalfn);
  kno_def_evalfn(kno_scheme_module,"%MODREF",
                 "`(%MODREF *modobj* *symbol*) returns the binding of "
                 "*symbol* in the module *modobj*, neither of which are "
                 "evaluated. This is intended for use in automatically "
                 "generated/optimized code",
                 modref_evalfn);
  kno_def_evalfn(kno_scheme_module,"SYMBOL-BOUND?",
                 "`(SYMBOL-BOUND? *sym* [*env*])` returns #t "
                 "if *sym* is bound in *env*, which defaults to "
                 "the current environment.",
                 symbol_boundp_evalfn);

  kno_def_evalfn(kno_scheme_module,"%ENV/RESET!",
                 "Resets the cached dynamic copy of the current "
                 "environment (if any). This means that procedures "
                 "closed in the current environment will not be "
                 "effected by future changes",
                 env_reset_evalfn);

  kno_idefn(kno_scheme_module,
            kno_make_cprim1("DOCUMENTATION",get_documentation,1));

  kno_idefn(kno_scheme_module,
            kno_make_cprim1("ENVIRONMENT?",environmentp_prim,1));
  kno_idefn(kno_scheme_module,
            kno_make_cprim2("SYMBOL-BOUND-IN?",symbol_boundin_prim,2));
  kno_idefn2(kno_scheme_module,"%LEXREF",lexref_prim,2,
             "(%LEXREF *up* *across*) returns a lexref (lexical reference) "
             "given a 'number of environments' *up* and a 'number of bindings' "
             "across",
             kno_fixnum_type,VOID,kno_fixnum_type,VOID);
  kno_idefn1(kno_scheme_module,"%LEXREFVAL",lexref_value_prim,1,
             "(%LEXREFVAL *lexref*) returns the offsets "
             "of a lexref as a pair (*up* . *across*)",
             kno_lexref_type,VOID);
  kno_idefn1(kno_scheme_module,"%LEXREF?",lexrefp_prim,1,
             "(%LEXREF? *val*) returns true if it's argument "
             "is a lexref (lexical reference)",
             -1,KNO_VOID);

  kno_idefn1(kno_scheme_module,"%CODEREF",coderef_prim,1,
             "(%CODEREF *nelts*) returns a 'coderef' (a relative position) value",
             kno_fixnum_type,VOID);
  kno_idefn1(kno_scheme_module,"%CODEREFVAL",coderef_value_prim,1,
             "(%CODEREFVAL *coderef*) returns the integer relative offset of a coderef",
             kno_coderef_type,VOID);
  kno_idefn1(kno_scheme_module,"CODEREF?",coderefp_prim,1,
             "(CODEREF? *nelts*) returns #t if *arg* is a coderef",
             -1,VOID);
  kno_idefn1(kno_scheme_module,"MAKE-CODEREF",make_coderef,1,
            "(MAKE-CODEREF <fixnum>)\nReturns a coderef object",
            kno_fixnum_type,KNO_VOID);

  kno_idefn(kno_scheme_module,kno_make_cprim1("NAME->OPCODE",name2opcode_prim,1));
  kno_idefn(kno_scheme_module,kno_make_cprim1("NAME->OPCODE",name2opcode_prim,1));
  kno_idefn1(kno_scheme_module,"OPCODE-NAME",opcode_name_prim,1,
             "(OPCODE-NAME *opcode*) returns the name of *opcode* "
             "or #f it's valid but anonymous, and errors if it is "
             "not an opcode or invalid",
             kno_opcode_type,VOID);
  kno_idefn(kno_scheme_module,
           kno_make_cprim1x("MAKE-OPCODE",make_opcode,1,
                           kno_fixnum_type,VOID));
  kno_idefn(kno_scheme_module,kno_make_cprim1("OPCODE?",opcodep,1));

  kno_idefn(kno_scheme_module,
            kno_make_ndprim(kno_make_cprim2("%CHOICEREF",choiceref_prim,2)));
  kno_idefn(kno_scheme_module,
            kno_make_ndprim(kno_make_cprim1("%FIXCHOICE",fixchoice_prim,1)));

  kno_def_evalfn(kno_scheme_module,"WITHENV","",withenv_evalfn);

  kno_idefn3(kno_scheme_module,"GET-ARG",get_arg_prim,2,
             "`(GET-ARG *expression* *i* [*default*])` "
             "returns the *i*'th parameter in *expression*, "
             "or *default* (otherwise)",
             -1,KNO_VOID,kno_fixnum_type,KNO_VOID,-1,KNO_VOID);
  kno_idefn(kno_scheme_module,
            kno_make_ndprim(kno_make_cprimn("APPLY",apply_lexpr,1)));
  kno_idefn(kno_scheme_module,kno_make_cprim7x
            ("DTPROC",make_dtproc,2,
             kno_symbol_type,VOID,kno_string_type,VOID,
             -1,VOID,-1,VOID,
             kno_fixnum_type,KNO_INT(2),
             kno_fixnum_type,KNO_INT(4),
             kno_fixnum_type,KNO_INT(1)));

  kno_idefn(kno_scheme_module,kno_make_cprim1("CALL/CC",callcc,1));
  kno_defalias(kno_scheme_module,"CALL-WITH-CURRENT-CONTINUATION","CALL/CC");

  /* This pushes a new threadcache */
  kno_def_evalfn(kno_scheme_module,"WITH-THREADCACHE","",with_threadcache_evalfn);
  /* This ensures that there's an active threadcache, pushing a new one if
     needed or using the current one if it exists. */
  kno_def_evalfn(kno_scheme_module,"USING-THREADCACHE","",using_threadcache_evalfn);
  /* This sets up the current thread to use a threadcache */
  kno_idefn(kno_scheme_module,
            kno_make_cprim1("USE-THREADCACHE",use_threadcache_prim,0));

  kno_idefn(kno_scheme_module,kno_make_cprimn("TCACHECALL",tcachecall,1));
  kno_idefn(kno_scheme_module,kno_make_cprimn("CACHECALL",cachecall,1));
  kno_idefn(kno_scheme_module,kno_make_cprimn("CACHECALL/PROBE",cachecall_probe,1));
  kno_idefn(kno_scheme_module,kno_make_cprimn("CACHEDCALL?",cachedcallp,1));
  kno_idefn(kno_scheme_module,
            kno_make_cprim1("CLEAR-CALLCACHE!",clear_callcache,0));
  kno_defalias(kno_scheme_module,"CACHEPOINT","TCACHECALL");

  kno_def_evalfn(kno_scheme_module,"VOID","",void_evalfn);
  kno_def_evalfn(kno_scheme_module,"!!!NULL!!!","",null_evalfn);
  kno_def_evalfn(kno_scheme_module,"BREAK","",break_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFAULT","",default_evalfn);

  kno_idefn(kno_scheme_module,kno_make_cprimn("CHECK-VERSION",check_version_prim,1));
  kno_idefn(kno_scheme_module,kno_make_cprimn("REQUIRE-VERSION",require_version_prim,1));

  kno_idefn0(kno_scheme_module,"%BUILDINFO",kno_get_build_info,
             "Information about the build and startup environment");

  kno_idefn(kno_scheme_module,kno_make_cprimn("FFI/PROC",ffi_proc,3));
  kno_idefn(kno_scheme_module,
            kno_make_cprim2x("FFI/FOUND?",ffi_found_prim,1,
                             kno_string_type,VOID,
                             -1,VOID));

  kno_register_config
    ("TAILCALL",
     "Enable/disable tail recursion in the Scheme evaluator. "
     "This may cause various source packages to break in some "
     "or all cases.",
     kno_boolconfig_get,kno_boolconfig_set,
     &kno_optimize_tail_calls);

}

KNO_EXPORT void kno_init_errfns_c(void);
KNO_EXPORT void kno_init_compoundfns_c(void);
KNO_EXPORT void kno_init_threads_c(void);
KNO_EXPORT void kno_init_conditionals_c(void);
KNO_EXPORT void kno_init_iterators_c(void);
KNO_EXPORT void kno_init_choicefns_c(void);
KNO_EXPORT void kno_init_binders_c(void);
KNO_EXPORT void kno_init_lambdas_c(void);
KNO_EXPORT void kno_init_macros_c(void);
KNO_EXPORT void kno_init_coreprims_c(void);
KNO_EXPORT void kno_init_tableprims_c(void);
KNO_EXPORT void kno_init_stringprims_c(void);
KNO_EXPORT void kno_init_dbprims_c(void);
KNO_EXPORT void kno_init_seqprims_c(void);
KNO_EXPORT void kno_init_modules_c(void);
KNO_EXPORT void kno_init_load_c(void);
KNO_EXPORT void kno_init_logprims_c(void);
KNO_EXPORT void kno_init_portprims_c(void);
KNO_EXPORT void kno_init_streamprims_c(void);
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
  kno_init_modules_c();
  kno_init_arith_c();
  kno_init_threads_c();
  kno_init_errfns_c();
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

  kno_init_coreprims_c();
  kno_init_stringprims_c();
  kno_init_dbprims_c();
  kno_init_seqprims_c();
  kno_init_logprims_c();
  kno_init_portprims_c();
  kno_init_streamprims_c();
  kno_init_timeprims_c();
  kno_init_sysprims_c();
  kno_init_sqldbprims_c();

  u8_threadcheck();

  kno_finish_module(kno_scheme_module);
  kno_finish_module(kno_scheme_module);
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
    kno_setup_app_env();

    return kno_scheme_initialized;}
}
