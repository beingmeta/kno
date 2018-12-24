/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"
#include "framerd/opcodes.h"
#include "framerd/dbprims.h"
#include "framerd/ffi.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

int fd_scheme_initialized = 0;

u8_string fd_evalstack_type="eval";
u8_string fd_ndevalstack_type="ndeval";

int fd_optimize_tail_calls = 1;

lispval _fd_comment_symbol, fcnids_symbol, sourceref_tag;

static lispval quote_symbol, comment_symbol, moduleid_symbol, source_symbol;

static u8_condition ExpiredThrow=_("Continuation is no longer valid");
static u8_condition DoubleThrow=_("Continuation used twice");
static u8_condition LostThrow=_("Lost invoked continuation");

u8_condition
  fd_SyntaxError=_("SCHEME expression syntax error"),
  fd_InvalidMacro=_("invalid macro transformer"),
  fd_UnboundIdentifier=_("the variable is unbound"),
  fd_VoidArgument=_("VOID result passed as argument"),
  fd_VoidBinding=_("VOID result used as variable binding"),
  fd_NotAnIdentifier=_("not an identifier"),
  fd_TooFewExpressions=_("too few subexpressions"),
  fd_CantBind=_("can't add binding to environment"),
  fd_ReadOnlyEnv=_("Read only environment");

/* Reading from expressions */

FD_EXPORT lispval _fd_get_arg(lispval expr,int i)
{
  return fd_get_arg(expr,i);
}
FD_EXPORT lispval _fd_get_body(lispval expr,int i)
{
  return fd_get_body(expr,i);
}

/* Environment functions */

static int bound_in_envp(lispval symbol,fd_lexenv env)
{
  lispval bindings = env->env_bindings;
  if (HASHTABLEP(bindings))
    return fd_hashtable_probe((fd_hashtable)bindings,symbol);
  else if (SLOTMAPP(bindings))
    return fd_slotmap_test((fd_slotmap)bindings,symbol,VOID);
  else if (SCHEMAPP(bindings))
    return fd_schemap_test((fd_schemap)bindings,symbol,VOID);
  else return fd_test(bindings,symbol,VOID);
}

/* Lexrefs */

static lispval lexref_prim(lispval upv,lispval acrossv)
{
  long long up = FIX2INT(upv), across = FIX2INT(acrossv);
  long long combined = ((up<<5)|(across));
  /* Not the exact range limits, but good enough */
  if ((up>=0)&&(across>=0)&&(up<256)&&(across<256))
    return LISPVAL_IMMEDIATE(fd_lexref_type,combined);
  else if ((up<0)||(up>256))
    return fd_type_error("short","lexref_prim",up);
  else return fd_type_error("short","lexref_prim",across);
}

static lispval lexrefp_prim(lispval ref)
{
  if (FD_TYPEP(ref,fd_lexref_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval lexref_value_prim(lispval lexref)
{
  int code = FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up = code/32, across = code%32;
  return fd_init_pair(NULL,FD_INT(up),FD_INT(across));
}

static int unparse_lexref(u8_output out,lispval lexref)
{
  int code = FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up = code/32, across = code%32;
  u8_printf(out,"#<LEXREF ^%d+%d>",up,across);
  return 1;
}

static ssize_t write_lexref_dtype(struct FD_OUTBUF *out,lispval x)
{
  int code = FD_GET_IMMEDIATE(x,fd_lexref_type);
  int up = code/32, across = code%32;
  unsigned char buf[100], *tagname="%LEXREF";
  struct FD_OUTBUF tmp = { 0 };
  FD_INIT_OUTBUF(&tmp,buf,100,0);
  fd_write_byte(&tmp,dt_compound);
  fd_write_byte(&tmp,dt_symbol);
  fd_write_4bytes(&tmp,7);
  fd_write_bytes(&tmp,tagname,7);
  fd_write_byte(&tmp,dt_vector);
  fd_write_4bytes(&tmp,2);
  fd_write_byte(&tmp,dt_fixnum);
  fd_write_4bytes(&tmp,up);
  fd_write_byte(&tmp,dt_fixnum);
  fd_write_4bytes(&tmp,across);
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

/* Code refs */

static lispval coderef_prim(lispval offset)
{
  long long off = FIX2INT(offset);
  return LISPVAL_IMMEDIATE(fd_coderef_type,off);
}

static lispval coderef_value_prim(lispval offset)
{
  long long off = FD_GET_IMMEDIATE(offset,fd_coderef_type);
  return FD_INT(off);
}

static int unparse_coderef(u8_output out,lispval coderef)
{
  long long off = FD_GET_IMMEDIATE(coderef,fd_coderef_type);
  u8_printf(out,"#<CODEREF %lld>",off);
  return 1;
}

static ssize_t write_coderef_dtype(struct FD_OUTBUF *out,lispval x)
{
  int offset = FD_GET_IMMEDIATE(x,fd_coderef_type);
  unsigned char buf[100], *tagname="%CODEREF";
  struct FD_OUTBUF tmp = { 0 };
  FD_INIT_OUTBUF(&tmp,buf,100,0);
  fd_write_byte(&tmp,dt_compound);
  fd_write_byte(&tmp,dt_symbol);
  fd_write_4bytes(&tmp,strlen(tagname));
  fd_write_bytes(&tmp,tagname,strlen(tagname));
  fd_write_byte(&tmp,dt_fixnum);
  fd_write_4bytes(&tmp,offset);
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

/* Opcodes */

u8_string fd_opcode_names[0x800]={NULL};
int fd_opcodes_length = 0x800;

static void set_opcode_name(lispval opcode,u8_string name)
{
  int off = FD_OPCODE_NUM(opcode);
  u8_string constname=u8_string_append("#",name,NULL);
  fd_opcode_names[off]=name;
  fd_add_constname(constname,opcode);
  u8_free(constname);
}

FD_EXPORT lispval fd_get_opcode(u8_string name)
{
  int i = 0; while (i<fd_opcodes_length) {
    u8_string opname = fd_opcode_names[i];
    if ((opname)&&(strcasecmp(name,opname)==0))
      return FD_OPCODE(i);
    else i++;}
  return FD_FALSE;
}

static lispval name2opcode_prim(lispval arg)
{
  if (FD_SYMBOLP(arg))
    return fd_get_opcode(SYM_NAME(arg));
  else if (STRINGP(arg))
    return fd_get_opcode(CSTRING(arg));
  else return fd_type_error(_("opcode name"),"name2opcode_prim",arg);
}

static int unparse_opcode(u8_output out,lispval opcode)
{
  int opcode_offset = (FD_GET_IMMEDIATE(opcode,fd_opcode_type));
  if (opcode_offset>fd_opcodes_length) {
    u8_printf(out,"#<INVALIDOPCODE>");
    return 1;}
  else if (fd_opcode_names[opcode_offset]==NULL) {
    u8_printf(out,"#<OPCODE_0x%x>",opcode_offset);
    return 1;}
  else {
    u8_printf(out,"#<OPCODE_%s>",fd_opcode_names[opcode_offset]);
    return 1;}
}

static int validate_opcode(lispval opcode)
{
  long opcode_offset = (FD_GET_IMMEDIATE(opcode,fd_opcode_type));
  if ((opcode_offset>=0) && (opcode_offset<fd_opcodes_length))
    return 1;
  else return 0;
}

static u8_string opcode_name(lispval opcode)
{
  long opcode_offset = (FD_GET_IMMEDIATE(opcode,fd_opcode_type));
  if ((opcode_offset<fd_opcodes_length) &&
      (fd_opcode_names[opcode_offset]))
    return fd_opcode_names[opcode_offset];
  else return "anonymous_opcode";
}

/* Some datatype methods */

static int unparse_evalfn(u8_output out,lispval x)
{
  struct FD_EVALFN *s=
    fd_consptr(struct FD_EVALFN *,x,fd_evalfn_type);
  lispval moduleid = s->evalfn_moduleid;
  u8_string modname =
    (FD_SYMBOLP(moduleid)) ? (FD_SYMBOL_NAME(moduleid)) : (NULL);
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

static ssize_t write_evalfn_dtype(struct FD_OUTBUF *out,lispval x)
{
  struct FD_EVALFN *s=
    fd_consptr(struct FD_EVALFN *,x,fd_evalfn_type);
  u8_string name=s->evalfn_name;
  u8_string filename=s->evalfn_filename;
  size_t name_len=strlen(name);
  ssize_t file_len=(filename) ? (strlen(filename)) : (-1);
  int n_elts = (file_len<0) ? (1) : (0);
  unsigned char buf[100], *tagname="%EVALFN";
  struct FD_OUTBUF tmp = { 0 };
  FD_INIT_OUTBUF(&tmp,buf,100,0);
  fd_write_byte(&tmp,dt_compound);
  fd_write_byte(&tmp,dt_symbol);
  fd_write_4bytes(&tmp,strlen(tagname));
  fd_write_bytes(&tmp,tagname,strlen(tagname));
  fd_write_byte(&tmp,dt_vector);
  fd_write_4bytes(&tmp,n_elts);
  fd_write_byte(&tmp,dt_string);
  fd_write_4bytes(&tmp,name_len);
  fd_write_bytes(&tmp,name,name_len);
  if (file_len>=0) {
    fd_write_byte(&tmp,dt_string);
    fd_write_4bytes(&tmp,file_len);
    fd_write_bytes(&tmp,filename,file_len);}
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

FD_EXPORT void recycle_evalfn(struct FD_RAW_CONS *c)
{
  struct FD_EVALFN *sf = (struct FD_EVALFN *)c;
  u8_free(sf->evalfn_name);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

/* Checking if eval is needed */

FD_EXPORT int fd_choice_evalp(lispval x)
{
  if (FD_NEED_EVALP(x))
    return 1;
  else if (FD_AMBIGP(x)) {
    FD_DO_CHOICES(e,x) {
      if (FD_NEED_EVALP(e)) {
        FD_STOP_DO_CHOICES;
        return 1;}}
    return 0;}
  else return 0;
}

FD_EXPORT
lispval fd_eval_exprs(lispval body,fd_lexenv env,fd_stack stack,int tail)
{
  return eval_body(body,env,stack,tail);
}


/* Symbol lookup */

FD_EXPORT lispval _fd_symeval(lispval sym,fd_lexenv env)
{
  return fd_symeval(sym,env);
}

/* Assignments */

static int add_to_value(lispval sym,lispval val,fd_lexenv env)
{
  int rv=1;
  if (env) {
    lispval bindings=env->env_bindings;
    lispval exports=env->env_exports;
    if ((fd_add(bindings,sym,val))>=0) {
      if (HASHTABLEP(exports)) {
        lispval newval=fd_get(bindings,sym,EMPTY);
        if (FD_ABORTP(newval))
          rv=-1;
        else {
          int e_rv=fd_hashtable_op
            ((fd_hashtable)exports,fd_table_replace,sym,newval);
          fd_decref(newval);
          if (e_rv<0) rv=e_rv;}}}
    else rv=-1;}
  if (rv<0) {
    fd_seterr(fd_CantBind,"add_to_env_value",NULL,sym);
    return -1;}
  else return rv;
}

FD_EXPORT int fd_bind_value(lispval sym,lispval val,fd_lexenv env)
{
  /* TODO: Check for checking the return value of calls to
     `fd_bind_value` */
  if (env) {
    lispval bindings = env->env_bindings;
    lispval exports = env->env_exports;
    if (fd_store(bindings,sym,val)<0) {
      fd_poperr(NULL,NULL,NULL,NULL);
      fd_seterr(fd_CantBind,"fd_bind_value",NULL,sym);
      return -1;}
    if ( (FD_CONSP(val)) && 
         ( (FD_FUNCTIONP(val)) || (FD_APPLICABLEP(val)) ) &&
         (FD_HASHTABLEP(bindings)) ) {
      lispval fcnids = fd_get(bindings,fcnids_symbol,FD_VOID);
      if (FD_HASHTABLEP(fcnids)) {
        lispval fcnid = fd_get(fcnids,sym,FD_VOID);
        if (FD_FCNIDP(fcnid)) fd_set_fcnid(fcnid,val);}
      fd_decref(fcnids);}
    if (HASHTABLEP(exports))
      fd_hashtable_op((fd_hashtable)(exports),fd_table_replace,sym,val);
    return 1;}
  else return 0;
}

FD_EXPORT int fd_add_value(lispval symbol,lispval value,fd_lexenv env)
{
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env = env->env_parent;
      if ((env) && (env->env_copy))
        env = env->env_copy;}
    else if ((env->env_bindings) == (env->env_exports))
      return fd_reterr(fd_ReadOnlyEnv,"fd_assign_value",NULL,symbol);
    else return add_to_value(symbol,value,env);}
  return 0;
}

FD_EXPORT int fd_assign_value(lispval symbol,lispval value,fd_lexenv env)
{
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env = env->env_parent;
      if ((env) && (env->env_copy)) env = env->env_copy;}
    else if ((env->env_bindings) == (env->env_exports))
      /* This is the kind of environment produced by using a module,
         so it's read only. */
      return fd_reterr(fd_ReadOnlyEnv,"fd_assign_value",NULL,symbol);
    else return fd_bind_value(symbol,value,env);}
  return 0;
}

/* Unpacking expressions, non-inline versions */

/* Quote */

static lispval quote_evalfn(lispval obj,fd_lexenv env,fd_stack stake)
{
  if ((PAIRP(obj)) && (PAIRP(FD_CDR(obj))) &&
      ((FD_CDR(FD_CDR(obj))) == NIL))
    return fd_incref(FD_CAR(FD_CDR(obj)));
  else return fd_err(fd_SyntaxError,"QUOTE",NULL,obj);
}

/* Handle sourcerefs */

static lispval handle_sourcerefs(lispval expr,struct FD_STACK *stack)
{
  while (FD_CAR(expr) == FD_SOURCEREF_OPCODE) {
    expr = FD_CDR(expr);
    lispval source = FD_CAR(expr);
    expr = FD_CDR(expr);
    stack->stack_source=source;
    if (!(FD_PAIRP(expr))) return expr;}
  return expr;
}

/* The evaluator itself */

static int applicable_choicep(lispval choice);

static lispval eval_apply(u8_string fname,
                          lispval fn,lispval arg_exprs,
                          fd_lexenv env,
                          struct FD_STACK *stack,
                          int tail);

static lispval pair_eval(lispval head,lispval expr,fd_lexenv env,
                         struct FD_STACK *_stack,
                         int tail);
static lispval schemap_eval(lispval expr,fd_lexenv env,
                            struct FD_STACK *_stack);
static lispval opcode_eval(lispval opcode,lispval expr,
                           fd_lexenv env,
                           fd_stack _stack,
                           int tail);
static lispval choice_eval(lispval expr,fd_lexenv env,
                           struct FD_STACK *_stack,
                           int tail);
static lispval prechoice_eval(lispval expr,fd_lexenv env,
                              struct FD_STACK *_stack,
                              int tail);

FD_EXPORT
lispval fd_stack_eval(lispval expr,fd_lexenv env,
                     struct FD_STACK *_stack,
                     int tail)
{
  if (_stack==NULL) _stack=fd_stackptr;
  if (FD_IMMEDIATEP(expr)) {
    switch (FD_PTR_TYPE(expr)) {
    case fd_lexref_type:
      return fd_lexref(expr,env);
    case fd_symbol_type: {
      lispval val = fd_symeval(expr,env);
      if (PRED_FALSE(VOIDP(val)))
        return fd_err(fd_UnboundIdentifier,"fd_eval",
                      SYM_NAME(expr),expr);
      else return simplify_value(val);}
    default:
      return expr;}}
  else if (FD_CONSP(expr)) {
    fd_ptr_type ctype = FD_CONSPTR_TYPE(expr);
    switch (ctype) {
    case fd_pair_type: {
      lispval result = VOID;
      FD_PUSH_STACK(eval_stack,fd_evalstack_type,NULL,expr);
      result = pair_eval(FD_CAR(expr),expr,env,eval_stack,tail);
      fd_pop_stack(eval_stack);
      return result;}
    case fd_choice_type:
      return choice_eval(expr,env,_stack,tail);
    case fd_prechoice_type:
      return prechoice_eval(expr,env,_stack,tail);
    case fd_code_type:
      return fd_incref(expr);
    case fd_slotmap_type:
      return fd_deep_copy(expr);
    case fd_schemap_type: {
      lispval result = VOID;
      FD_PUSH_STACK(eval_stack,fd_evalstack_type,NULL,expr);
      result = schemap_eval(expr,env,eval_stack);
      fd_pop_stack(eval_stack);
      return result;}
    default:
      return fd_incref(expr);}}
  else return expr;
}

static lispval choice_eval(lispval expr,fd_lexenv env,
                           struct FD_STACK *_stack,
                           int tail)
{
  lispval result = EMPTY;
  FD_PUSH_STACK(nd_eval_stack,fd_ndevalstack_type,NULL,expr);
  DO_CHOICES(each_expr,expr) {
    lispval r = stack_eval(each_expr,env,nd_eval_stack);
    if (FD_ABORTED(r)) {
      FD_STOP_DO_CHOICES;
      fd_decref(result);
      fd_pop_stack(nd_eval_stack);
      return r;}
    else {CHOICE_ADD(result,r);}}
  fd_pop_stack(nd_eval_stack);
  return simplify_value(result);
}

static lispval prechoice_eval(lispval expr,fd_lexenv env,
                              struct FD_STACK *_stack,
                              int tail)
{
  lispval exprs = fd_make_simple_choice(expr);
  FD_PUSH_STACK(prechoice_eval_stack,fd_evalstack_type,NULL,expr);
  if (CHOICEP(exprs)) {
    lispval results = EMPTY;
    DO_CHOICES(expr,exprs) {
      lispval result = stack_eval(expr,env,prechoice_eval_stack);
      if (FD_ABORTED(result)) {
        FD_STOP_DO_CHOICES;
        fd_decref(results);
        fd_pop_stack(prechoice_eval_stack);
        return result;}
      else {CHOICE_ADD(results,result);}}
    fd_decref(exprs);
    fd_pop_stack(prechoice_eval_stack);
    return simplify_value(results);}
  else {
    lispval result = fd_stack_eval(exprs,env,prechoice_eval_stack,tail);
    fd_decref(exprs);
    fd_pop_stack(prechoice_eval_stack);
    return result;}
}

/* Function application */

static lispval get_headval(lispval head,fd_lexenv env,fd_stack eval_stack,
                           int *gc_headval);

lispval pair_eval(lispval head,lispval expr,fd_lexenv env,
                  struct FD_STACK *eval_stack,
                  int tail)
{
  u8_string label=(SYMBOLP(head)) ? (SYM_NAME(head)) :
    (FD_OPCODEP(head)) ? (opcode_name(head)) : (NULL);
  if (head == FD_SOURCEREF_OPCODE) {
    expr = handle_sourcerefs(expr,eval_stack);
    head = (FD_PAIRP(expr)) ? (FD_CAR(expr)) : (FD_VOID);
    label=(SYMBOLP(head)) ? (SYM_NAME(head)) :
      (FD_OPCODEP(head)) ? (opcode_name(head)) : (NULL);}
  int flags = eval_stack->stack_flags;
  if (U8_BITP(flags,FD_STACK_DECREF_OP)) {fd_decref(eval_stack->stack_op);}
  if (label) {
    if (U8_BITP(flags,FD_STACK_FREE_LABEL)) {u8_free(eval_stack->stack_label);}
    eval_stack->stack_label=label;}
  eval_stack->stack_flags = FD_STACK_LIVE | ( (flags) & (FD_STACK_FREE_SRC) );
  int gc_head=0;
  lispval result = VOID, headval =
    (FD_OPCODEP(head)) ? (head) : (get_headval(head,env,eval_stack,&gc_head));
  if (gc_head) {
    FD_ADD_TO_CHOICE(eval_stack->stack_vals,headval);}

  fd_ptr_type headtype = FD_PTR_TYPE(headval);
  switch (headtype) {
  case fd_opcode_type:
    result = opcode_eval(headval,expr,env,eval_stack,tail); break;
  case fd_cprim_type: case fd_lambda_type: {
    struct FD_FUNCTION *f = (struct FD_FUNCTION *) headval;
    if (f->fcn_name) eval_stack->stack_label=f->fcn_name;
    result = eval_apply(f->fcn_name,headval,FD_CDR(expr),env,eval_stack,tail);
    break;}
  case fd_evalfn_type: {
    /* These are evalfns which do all the evaluating themselves */
    struct FD_EVALFN *handler = (fd_evalfn)headval;
    if (handler->evalfn_name) eval_stack->stack_label=handler->evalfn_name;
    result = handler->evalfn_handler(expr,env,eval_stack);
    break;}
  case fd_macro_type: {
    /* These expand into expressions which are
       then evaluated. */
    struct FD_MACRO *macrofn=
      fd_consptr(struct FD_MACRO *,headval,fd_macro_type);
    eval_stack->stack_type="macro";
    lispval xformer = macrofn->macro_transformer;
    lispval new_expr = fd_call(eval_stack,xformer,1,&expr);
    if (FD_ABORTED(new_expr))
      result = fd_err(fd_SyntaxError,
                      _("macro expansion"),NULL,new_expr);
    else result = fd_stack_eval(new_expr,env,eval_stack,tail);
    fd_decref(new_expr);
    break;}
  case fd_choice_type: {
    int applicable = applicable_choicep(headval);
    if (applicable)
      result = eval_apply("fnchoice",headval,FD_CDR(expr),env,eval_stack,0);
    else result =fd_err(fd_SyntaxError,"fd_stack_eval",
                        "not applicable or evalfn",
                        headval);
    break;}
  default:
    if (fd_functionp[headtype]) {
      struct FD_FUNCTION *f = (struct FD_FUNCTION *) headval;
      result = eval_apply(f->fcn_name,headval,FD_CDR(expr),env,
                          eval_stack,tail);}
    else if (fd_applyfns[headtype]) {
      result = eval_apply("extfcn",headval,FD_CDR(expr),env,eval_stack,tail);}
    else if (FD_ABORTED(headval)) {
      result=headval;}
    else if (VOIDP(headval)) {
      result=fd_err(fd_UnboundIdentifier,"for function",
                    ((SYMBOLP(head))?(SYM_NAME(head)):
                     (NULL)),
                    head);}
    else if (EMPTYP(headval) )
      result=EMPTY;
    else result=fd_err(fd_NotAFunction,NULL,NULL,headval);}
  if (!tail) {
    if (FD_TAILCALLP(result))
      result=fd_finish_call(result);
    else {}}
  return simplify_value(result);
}

static int applicable_choicep(lispval headvals)
{
  DO_CHOICES(fcn,headvals) {
    lispval hv = (FD_FCNIDP(fcn)) ? (fd_fcnid_ref(fcn)) : (fcn);
    int hvtype = FD_PRIM_TYPE(hv);
    /* Check that all the elements are either applicable or special
       forms and not mixed */
    if ( (hvtype == fd_cprim_type) ||
         (hvtype == fd_lambda_type) ||
         (fd_applyfns[hvtype]) ) {}
    else if ((hvtype == fd_evalfn_type) ||
             (hvtype == fd_macro_type))
      return 0;
    /* In this case, all the headvals so far are evalfns */
    else return 0;}
  return 1;
}

FD_EXPORT lispval _fd_eval(lispval expr,fd_lexenv env)
{
  lispval result = fd_tail_eval(expr,env);
  if (FD_ABORTED(result)) return result;
  else return fd_finish_call(result);
}

/* Applying functions */

#define ND_ARGP(v) ((CHOICEP(v))||(QCHOICEP(v)))

FD_FASTOP int commentp(lispval arg)
{
  return
    (PRED_FALSE
     ((PAIRP(arg)) &&
      (FD_EQ(FD_CAR(arg),comment_symbol))));
}

static lispval get_headval(lispval head,fd_lexenv env,fd_stack eval_stack,
                           int *gc_headval)
{
  lispval headval = VOID;
  if (FD_IMMEDIATEP(head)) {
    if (FD_OPCODEP(head))
      return head;
    else if (FD_LEXREFP(head)) {
      headval=fd_lexref(head,env);
      if ( (FD_CONSP(headval)) && (FD_MALLOCD_CONSP(headval)) )
        *gc_headval=1;}
    else if (FD_SYMBOLP(head)) {
      if (head == quote_symbol) return FD_QUOTE_OPCODE;
      headval=fd_symeval(head,env);
      if (FD_CONSP(headval)) *gc_headval=1;}
    else headval = head;}
  else if ( (PAIRP(head)) || (FD_CODEP(head)) || (CHOICEP(head)) ) {
    headval=stack_eval(head,env,eval_stack);
    headval=simplify_value(headval);
    *gc_headval=1;}
  else headval=head;
  if (FD_ABORTED(headval))
    return headval;
  else if (FD_FCNIDP(headval)) {
    headval=fd_fcnid_ref(headval);
    if (PRECHOICEP(headval)) {
      headval=fd_make_simple_choice(headval);
      *gc_headval=1;}}
  else NO_ELSE;
  return headval;
}

static int fast_eval_args = 1;

FD_FASTOP lispval arg_eval(lispval x,fd_lexenv env,struct FD_STACK *stack)
{
  switch (FD_PTR_MANIFEST_TYPE(x)) {
  case fd_oid_ptr_type: case fd_fixnum_ptr_type:
    return x;
  case fd_immediate_ptr_type:
    if (FD_TYPEP(x,fd_lexref_type))
      return fd_lexref(x,env);
    else if (FD_SYMBOLP(x)) {
      lispval val = fd_symeval(x,env);
      if (FD_EXPECT_FALSE(FD_VOIDP(val)))
        return fd_err(fd_UnboundIdentifier,"fd_eval",FD_SYMBOL_NAME(x),x);
      else return val;}
    else return x;
  case fd_cons_ptr_type: {
    fd_ptr_type type = FD_CONSPTR_TYPE(x);
    switch (type) {
    case fd_pair_type:
      if (fast_eval_args)
        return pair_eval(FD_CAR(x),x,env,stack,0);
      else return fd_stack_eval(x,env,stack,0);
    case fd_code_type:
    case fd_choice_type: case fd_prechoice_type:
      return fd_stack_eval(x,env,stack,0);
    case fd_slotmap_type:
      return fd_deep_copy(x);
    case fd_schemap_type:
      return schemap_eval(x,env,stack);
    default:
      return fd_incref(x);}}
  default: /* Never reached */
    return x;
  }
}

static lispval eval_apply(u8_string fname,
                          lispval fn,lispval arg_exprs,
                          fd_lexenv env,
                          struct FD_STACK *stack,
                          int tail)
{
  int max_args = 0, min_args = 0, n_fns = 0;
  int lambda = -1, gc_args = 0, nd_args = 0, d_prim = 1;
  if (FD_EXPECT_FALSE(FD_CHOICEP(fn))) {
    FD_DO_CHOICES(f,fn) {
      if (FD_FUNCTIONP(f)) {
        struct FD_FUNCTION *fcn=FD_XFUNCTION(f);
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
          if (FD_LAMBDAP(fn)) lambda = 1;}
        else if (FD_LAMBDAP(fn))
          lambda = 0;
        else NO_ELSE;
        n_fns++;}
      else if (!(FD_APPLICABLEP(f))) {
        FD_STOP_DO_CHOICES;
        return fd_err("NotApplicable","eval_apply",NULL,f);}
      else n_fns++;}}
    else if (FD_FUNCTIONP(fn)) {
      struct FD_FUNCTION *fcn=FD_XFUNCTION(fn);
      lambda = FD_LAMBDAP(fn);
      d_prim = (! (fcn->fcn_ndcall) );
      tail   = (! (fcn->fcn_notail) );
      max_args = fcn->fcn_arity;
      min_args = fcn->fcn_min_arity;}
    else if (FD_APPLICABLEP(fn))
      n_fns++;
    else return fd_err("NotApplicable","eval_apply",NULL,fn);

  if (n_fns > 1) tail = 0;
  int arg_i = 0, argbuf_len = (max_args>=0) ? (max_args) : (min_args+8);
  lispval init_argbuf[argbuf_len], *argbuf=init_argbuf;
  lispval result = FD_VOID;
  lispval scan = arg_exprs;
  while (FD_PAIRP(scan)) {
    lispval arg_expr = pop_arg(scan);
    if (commentp(arg_expr)) continue;
    else if ( (max_args >= 0) && (arg_i > max_args) ) {
      if (gc_args) fd_decref_vec(argbuf,arg_i);
      return fd_err(fd_TooManyArgs,"eval_apply",fname,arg_expr);}
    lispval arg_val = arg_eval(arg_expr,env,stack);
    if (PRED_FALSE(VOIDP(arg_val))) {
      if (gc_args) fd_decref_vec(argbuf,arg_i);
      return fd_err(fd_VoidArgument,"eval_apply/arg",NULL,arg_expr);}
    else if ( ( (d_prim) && (EMPTYP(arg_val)) ) || (FD_ABORTED(arg_val)) ) {
      /* Clean up the arguments we've already evaluated */
      if (gc_args) fd_decref_vec(argbuf,arg_i);
      return arg_val;}
    else if (CONSP(arg_val)) {
      if (FD_PRECHOICEP(arg_val)) arg_val = fd_simplify_choice(arg_val);
      if ( (nd_args == 0) && (ND_ARGP(arg_val)) )
        nd_args = 1;
      gc_args = 1;}
    else {}
    if (arg_i >= argbuf_len) {
      lispval new_argbuf_len = argbuf_len*2;
      lispval *new_argbuf = alloca(sizeof(lispval)*new_argbuf_len);
      memcpy(new_argbuf,argbuf,sizeof(lispval)*arg_i);
      argbuf_len = new_argbuf_len;
      argbuf=new_argbuf;}
    argbuf[arg_i++]=arg_val;}
  if ( (tail) && (lambda) && (fd_optimize_tail_calls) )
    result=fd_tail_call(fn,arg_i,argbuf);
  else if ((CHOICEP(fn)) || (PRECHOICEP(fn)) || ((d_prim) && (nd_args)))
    result=fd_ndcall(stack,fn,arg_i,argbuf);
  else if (gc_args)
    result=fd_dcall(stack,fn,arg_i,argbuf);
  else return fd_dcall(stack,fn,arg_i,argbuf);
  if (gc_args) fd_decref_vec(argbuf,arg_i);
  return result;
}

/* Evaluating schemaps */

lispval schemap_eval(lispval expr,fd_lexenv env,
                     struct FD_STACK *eval_stack)
{
  struct FD_SCHEMAP *skmap = (fd_schemap) expr;
  int n = skmap->schema_length;
  lispval *schema = skmap->table_schema;
  lispval *vals = skmap->schema_values;
  lispval new_vals[n];
  int i = 0; while (i < n) {
    lispval val_expr = vals[i];
    lispval val = arg_eval(val_expr,env,eval_stack);
    if (FD_ABORTP(val)) {
      fd_decref_vec(new_vals,i);
      return val;}
    else new_vals[i++] = val;}
  return fd_make_schemap(NULL,n,
                         FD_SCHEMAP_INLINE|FD_SCHEMAP_COPY_SCHEMA,
                         schema,new_vals);
}


/* Opcode eval */

#include "opcode_defs.c"

static lispval opcode_eval(lispval opcode,lispval expr,
                           fd_lexenv env,
                           fd_stack _stack,
                           int tail)
{
  lispval args = FD_CDR(expr);

  if (FD_SPECIAL_OPCODEP(opcode))
    return handle_special_opcode(opcode,args,expr,env,_stack,tail);
  else if ( (FD_D1_OPCODEP(opcode)) || (FD_ND1_OPCODEP(opcode)) ) {
    int nd_call = (FD_ND1_OPCODEP(opcode));
    lispval results = FD_EMPTY_CHOICE;
    lispval arg = FD_CAR(args), val = arg_eval(arg,env,_stack);
    if (FD_ABORTED(val)) _return val;
    else if (FD_VOIDP(val))
      return fd_err(fd_VoidArgument,"OPCODE_APPLY",opcode_name(opcode),arg);
    else NO_ELSE;
    if (FD_PRECHOICEP(val))
      val = fd_simplify_choice(val);
    /* Get results */
    if ( (!(nd_call)) && (FD_EMPTY_CHOICEP(val)) )
      results = FD_EMPTY_CHOICE;
    else if (nd_call)
      results = nd1_call(opcode,val);
    else if (FD_CHOICEP(val)) {
      FD_DO_CHOICES(v,val) {
        lispval r = d1_call(opcode,val);
        if (FD_ABORTP(r)) {
          fd_decref(results);
          FD_STOP_DO_CHOICES;
          _return r;}
        else {FD_ADD_TO_CHOICE(results,r);}}}
    else results = d1_call(opcode,val);
    fd_decref(val);
    return results;}
  else if ( (FD_D2_OPCODEP(opcode)) ||
            (FD_ND2_OPCODEP(opcode)) ||
            (FD_NUMERIC_OPCODEP(opcode)) ) {
    int nd_call = (FD_ND2_OPCODEP(opcode));
    lispval results = FD_EMPTY_CHOICE;
    int numericp = (FD_NUMERIC_OPCODEP(opcode));
    lispval arg1 = pop_arg(args), val1 = arg_eval(arg1,env,_stack);
    if (FD_ABORTED(val1)) return val1;
    else if (! (FD_EXPECT_TRUE(FD_PAIRP(args))) )
      return fd_err(fd_TooFewArgs,"opcode_eval",opcode_name(opcode),
                    expr);
    else if ( (FD_EMPTY_CHOICEP(val1)) && (!(nd_call)) )
      return val1;
    else if (FD_EXPECT_FALSE(FD_VOIDP(val1)))
      return fd_err(fd_VoidArgument,"OPCODE_APPLY",opcode_name(opcode),arg1);
    else NO_ELSE;
    lispval arg2 = pop_arg(args), val2 = arg_eval(arg2,env,_stack);
    if (FD_ABORTED(val2)) {fd_decref(val1); return val2;}
    else if ( (FD_EMPTY_CHOICEP(val2)) && (!(nd_call)) ) {
      fd_decref(val1);
      return val2;}
    else if (FD_EXPECT_FALSE(FD_VOIDP(val2))) {
      fd_decref(val1);
      return fd_err(fd_VoidArgument,"OPCODE_APPLY",opcode_name(opcode),arg2);}
    else NO_ELSE;
    if (FD_PRECHOICEP(val1)) val1 = fd_simplify_choice(val1);
    if (FD_PRECHOICEP(val2)) val2 = fd_simplify_choice(val2);
    if ( (FD_CHOICEP(val1)) || (FD_CHOICEP(val2)) ) {
      if (nd_call)
        results = nd2_call(opcode,val1,val2);
      else {
        FD_DO_CHOICES(v1,val1) {
          FD_DO_CHOICES(v2,val2) {
            lispval r = (numericp) ? (numop_call(opcode,v1,v2)) :
              (d2_call(opcode,v1,v2));
            if (FD_ABORTP(r)) {
              fd_decref(results);
              FD_STOP_DO_CHOICES;
              results = r;
              break;}
            else {FD_ADD_TO_CHOICE(results,r);}}
          if (FD_ABORTP(results)) {
            FD_STOP_DO_CHOICES;
            break;}}}}
    else if (numericp)
      results = numop_call(opcode,val1,val2);
    else if (FD_ND2_OPCODEP(opcode))
      results = nd2_call(opcode,val1,val2);
    else results = d2_call(opcode,val1,val2);
    fd_decref(val1); fd_decref(val2);
    return results;}
  else if (FD_TABLE_OPCODEP(opcode))
    return handle_table_opcode(opcode,expr,env,_stack,tail);
  else return fd_err(_("Invalid opcode"),"numop_call",NULL,expr);
}

/* Making some functions */

FD_EXPORT lispval fd_make_evalfn(u8_string name,fd_eval_handler fn)
{
  struct FD_EVALFN *f = u8_alloc(struct FD_EVALFN);
  FD_INIT_CONS(f,fd_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_filename = NULL;
  f->evalfn_handler = fn;
  return LISP_CONS(f);
}

FD_EXPORT void fd_defspecial(lispval mod,u8_string name,fd_eval_handler fn)
{
  struct FD_EVALFN *f = u8_alloc(struct FD_EVALFN);
  FD_INIT_CONS(f,fd_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_handler = fn;
  f->evalfn_filename = NULL;
  fd_store(mod,fd_intern(name),LISP_CONS(f));
  f->evalfn_moduleid = fd_get(mod,moduleid_symbol,FD_VOID);
  fd_decref(LISP_CONS(f));
}

FD_EXPORT void fd_new_evalfn(lispval mod,u8_string name,
                             u8_string filename,u8_string doc,
                             fd_eval_handler fn)
{
  struct FD_EVALFN *f = u8_alloc(struct FD_EVALFN);
  FD_INIT_CONS(f,fd_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_handler = fn;
  f->evalfn_filename = filename;
  f->evalfn_documentation = doc;
  fd_store(mod,fd_intern(name),LISP_CONS(f));
  f->evalfn_moduleid = fd_get(mod,moduleid_symbol,FD_VOID);
  fd_decref(LISP_CONS(f));
}

/* The Evaluator */

static lispval eval_evalfn(lispval x,fd_lexenv env,fd_stack stack)
{
  lispval expr_expr = fd_get_arg(x,1);
  lispval expr = fd_stack_eval(expr_expr,env,stack,0);
  if (FD_ABORTED(expr)) return expr;
  lispval result = fd_stack_eval(expr,env,stack,0);
  fd_decref(expr);
  return result;
}

static lispval boundp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    else if (VOIDP(val)) return FD_FALSE;
    else if (val == FD_UNBOUND) return FD_FALSE;
    else {
      fd_decref(val);
      return FD_TRUE;}}
}

static lispval unboundp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    else if (VOIDP(val)) return FD_TRUE;
    else if (val == FD_UNBOUND) return FD_TRUE;
    else {
      fd_decref(val);
      return FD_FALSE;}}
}

static lispval definedp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"definedp_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    if ( (VOIDP(val)) || (val == FD_DEFAULT_VALUE) )
      return FD_FALSE;
    else if (val == FD_UNBOUND)
      return FD_FALSE;
    else {
      fd_decref(val);
      return FD_TRUE;}}
}

static lispval modref_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval module = fd_get_arg(expr,1);
  lispval symbol = fd_get_arg(expr,2);
  if ((VOIDP(module))||(VOIDP(module)))
    return fd_err(fd_SyntaxError,"modref_evalfn",NULL,fd_incref(expr));
  else if (!(HASHTABLEP(module)))
    return fd_type_error("module hashtable","modref_evalfn",module);
  else if (!(SYMBOLP(symbol)))
    return fd_type_error("symbol","modref_evalfn",symbol);
  else return fd_hashtable_get((fd_hashtable)module,symbol,FD_UNBOUND);
}

static lispval constantp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval to_eval = fd_get_arg(expr,1);
  if (FD_SYMBOLP(to_eval)) {
    lispval v = fd_symeval(to_eval,env);
    if  (v == FD_NULL)
      return fd_err(fd_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else if (FD_ABORTED(v))
      return v;
    else if (FD_CONSTANTP(v))
      return FD_TRUE;
    else {
      fd_decref(v);
      return FD_FALSE;}}
  else {
    lispval v = fd_eval(to_eval,env);
    if  (v == FD_NULL)
      return fd_err(fd_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else if (FD_ABORTED(v))
      return v;
    else if (FD_CONSTANTP(v))
      return FD_TRUE;
    else {
      fd_decref(v);
      return FD_FALSE;}}
}

static lispval voidp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval to_eval = fd_get_arg(expr,1);
  if (FD_SYMBOLP(to_eval)) {
    lispval v = fd_symeval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return FD_TRUE;
    else if  (v == FD_NULL) {
      return FD_TRUE;}
    else {
      fd_decref(v);
      return FD_FALSE;}}
  else {
    lispval v = fd_eval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return FD_TRUE;
    else {
      fd_decref(v);
      return FD_FALSE;}}
}

static lispval defaultp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval to_eval = fd_get_arg(expr,1);
  if (FD_SYMBOLP(to_eval)) {
    lispval v = fd_symeval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if (v == FD_DEFAULT)
      return FD_TRUE;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return fd_err(fd_UnboundIdentifier,"defaultp_evalfn",NULL,to_eval);
    else if  (v == FD_NULL)
      return fd_err(fd_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else {
      fd_decref(v);
      return FD_FALSE;}}
  else {
    lispval v = fd_eval(to_eval,env);
    if  (v == FD_NULL)
      return fd_err(fd_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else if (FD_ABORTED(v)) return v;
    else if (v == FD_DEFAULT) return FD_TRUE;
    else {
      fd_decref(v);
      return FD_FALSE;}}
}

static lispval badp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval to_eval = fd_get_arg(expr,1);
  if (FD_SYMBOLP(to_eval)) {
    lispval v = fd_symeval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return FD_TRUE;
    else if  ( (v == FD_NULL) || (! (FD_CHECK_PTR(v)) ) ) {
      u8_log(LOG_WARN,fd_BadPtr,"Bad pointer value 0x%llx for %q",v,to_eval);
      return FD_TRUE;}
    else return FD_FALSE;}
  else {
    lispval v = fd_eval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return FD_TRUE;
    else if  ( (v == FD_NULL) || (! (FD_CHECK_PTR(v)) ) ) {
      u8_log(LOG_WARN,fd_BadPtr,"Bad pointer value 0x%llx for %q",v,to_eval);
      return FD_TRUE;}
    else {
      fd_decref(v);
      return FD_FALSE;}}
}

static lispval env_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return (lispval)fd_copy_env(env);
}

static lispval env_reset_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  if ( (env->env_copy) && (env != env->env_copy) ) {
    lispval tofree = (lispval) env;
    env->env_copy=NULL;
    fd_decref(tofree);}
  return VOID;
}

static lispval symbol_boundp_prim(lispval symbol,lispval envarg)
{
  if (!(SYMBOLP(symbol)))
    return fd_type_error(_("symbol"),"symbol_boundp_prim",symbol);
  else if (FD_LEXENVP(envarg)) {
    fd_lexenv env = (fd_lexenv)envarg;
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    else if (VOIDP(val)) return FD_FALSE;
    else if (val == FD_DEFAULT_VALUE)
      return FD_FALSE;
    else if (val == FD_UNBOUND)
      return FD_FALSE;
    else {
      fd_decref(val);
      return FD_TRUE;}}
  else if (TABLEP(envarg)) {
    lispval val = fd_get(envarg,symbol,VOID);
    if (VOIDP(val)) return FD_FALSE;
    else {
      fd_decref(val);
      return FD_TRUE;}}
  else return fd_type_error(_("environment"),"symbol_boundp_prim",envarg);
}

static lispval environmentp_prim(lispval arg)
{
  if (FD_LEXENVP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

/* Withenv forms */

static lispval withenv(lispval expr,fd_lexenv env,
                      fd_lexenv consed_env,u8_context cxt)
{
  lispval bindings = fd_get_arg(expr,1);
  if (VOIDP(bindings))
    return fd_err(fd_TooFewExpressions,cxt,NULL,expr);
  else if ((NILP(bindings))||(FALSEP(bindings))) {}
  else if (PAIRP(bindings)) {
    FD_DOLIST(varval,bindings) {
      if ((PAIRP(varval))&&(SYMBOLP(FD_CAR(varval)))&&
          (PAIRP(FD_CDR(varval)))&&
          (NILP(FD_CDR(FD_CDR(varval))))) {
        lispval var = FD_CAR(varval), val = fd_eval(FD_CADR(varval),env);
        if (FD_ABORTED(val)) return val;
        int rv=fd_bind_value(var,val,consed_env);
        fd_decref(val);
        if (rv<0) return FD_ERROR;}
      else return fd_err(fd_SyntaxError,cxt,NULL,expr);}}
  else if (TABLEP(bindings)) {
    lispval keys = fd_getkeys(bindings);
    DO_CHOICES(key,keys) {
      if (SYMBOLP(key)) {
         int rv=0; lispval value = fd_get(bindings,key,VOID);
        if (!(VOIDP(value)))
          rv=fd_bind_value(key,value,consed_env);
        fd_decref(value);
        if (rv<0) return FD_ERROR;}
      else {
        FD_STOP_DO_CHOICES;
        fd_recycle_lexenv(consed_env);
        return fd_err(fd_SyntaxError,cxt,NULL,expr);}
      fd_decref(keys);}}
  else return fd_err(fd_SyntaxError,cxt,NULL,expr);
  /* Execute the body */ {
    lispval result = VOID;
    lispval body = fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result);
      result = fd_eval(elt,consed_env);
      if (FD_ABORTED(result)) {
        return result;}}
    return result;}
}

static lispval withenv_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv consed_env = fd_working_lexenv();
  lispval result = withenv(expr,env,consed_env,"WITHENV");
  fd_recycle_lexenv(consed_env);
  return result;
}

static lispval withenv_safe_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv consed_env = fd_safe_working_lexenv();
  lispval result = withenv(expr,env,consed_env,"WITHENV/SAFE");
  fd_recycle_lexenv(consed_env);
  return result;
}

/* Eval/apply related primitives */

static lispval get_arg_prim(lispval expr,lispval elt,lispval dflt)
{
  if (PAIRP(expr))
    if (FD_UINTP(elt)) {
      int i = 0, lim = FIX2INT(elt); lispval scan = expr;
      while ((i<lim) && (PAIRP(scan))) {
        scan = FD_CDR(scan); i++;}
      if (PAIRP(scan)) return fd_incref(FD_CAR(scan));
      else return fd_incref(dflt);}
    else return fd_type_error(_("fixnum"),"get_arg_prim",elt);
  else return fd_type_error(_("pair"),"get_arg_prim",expr);
}

static lispval apply_lexpr(int n,lispval *args)
{
  DO_CHOICES(fn,args[0])
    if (!(FD_APPLICABLEP(args[0]))) {
      FD_STOP_DO_CHOICES;
      return fd_type_error("function","apply_lexpr",args[0]);}
  {
    lispval results = EMPTY;
    DO_CHOICES(fn,args[0]) {
      DO_CHOICES(final_arg,args[n-1]) {
        lispval result = VOID;
        int final_length = fd_seq_length(final_arg);
        int n_args = (n-2)+final_length;
        lispval *values = u8_alloc_n(n_args,lispval);
        int i = 1, j = 0, lim = n-1;
        /* Copy regular arguments */
        while (i<lim) {values[j]=fd_incref(args[i]); j++; i++;}
        i = 0; while (j<n_args) {
          values[j]=fd_seq_elt(final_arg,i); j++; i++;}
        result = fd_apply(fn,n_args,values);
        if (FD_ABORTED(result)) {
          fd_decref(results);
          FD_STOP_DO_CHOICES;
          i = 0; while (i<n_args) {fd_decref(values[i]); i++;}
          u8_free(values);
          results = result;
          break;}
        else {CHOICE_ADD(results,result);}
        i = 0; while (i<n_args) {fd_decref(values[i]); i++;}
        u8_free(values);}
      if (FD_ABORTED(results)) {
        FD_STOP_DO_CHOICES;
        return results;}}
    return simplify_value(results);
  }
}

/* Initialization */

extern void fd_init_coreprims_c(void);

static lispval lispenv_get(lispval e,lispval s,lispval d)
{
  lispval result = fd_symeval(s,FD_XENV(e));
  if (VOIDP(result)) return fd_incref(d);
  else return result;
}
static int lispenv_store(lispval e,lispval s,lispval v)
{
  return fd_bind_value(s,v,FD_XENV(e));
}

static int lispenv_add(lispval e,lispval s,lispval v)
{
  return add_to_value(s,v,FD_XENV(e));
}

/* Call/cc */

static lispval call_continuation(struct FD_FUNCTION *f,lispval arg)
{
  struct FD_CONTINUATION *cont = (struct FD_CONTINUATION *)f;
  if (cont->retval == FD_NULL)
    return fd_err(ExpiredThrow,"call_continuation",NULL,arg);
  else if (VOIDP(cont->retval)) {
    cont->retval = fd_incref(arg);
    return FD_THROW_VALUE;}
  else return fd_err(DoubleThrow,"call_continuation",NULL,arg);
}

static lispval callcc (lispval proc)
{
  lispval continuation, value;
  struct FD_CONTINUATION *f = u8_alloc(struct FD_CONTINUATION);
  FD_INIT_FRESH_CONS(f,fd_cprim_type);
  f->fcn_name="continuation"; f->fcn_filename = NULL;
  f->fcn_ndcall = 1; f->fcn_xcall = 1; f->fcn_arity = 1; f->fcn_min_arity = 1;
  f->fcn_typeinfo = NULL; f->fcn_defaults = NULL;
  f->fcn_handler.xcall1 = call_continuation; f->retval = VOID;
  continuation = LISP_CONS(f);
  value = fd_apply(proc,1,&continuation);
  if ((value == FD_THROW_VALUE) && (!(VOIDP(f->retval)))) {
    lispval retval = f->retval;
    f->retval = FD_NULL;
    if (FD_CONS_REFCOUNT(f)>1)
      u8_log(LOG_WARN,ExpiredThrow,"Dangling pointer exists to continuation");
    fd_decref(continuation);
    return retval;}
  else {
    if (FD_CONS_REFCOUNT(f)>1)
      u8_log(LOG_WARN,LostThrow,"Dangling pointer exists to continuation");
    fd_decref(continuation);
    return value;}
}

/* Cache call */

static lispval cachecall(int n,lispval *args)
{
  if (HASHTABLEP(args[0]))
    return fd_xcachecall((fd_hashtable)args[0],args[1],n-2,args+2);
  else return fd_cachecall(args[0],n-1,args+1);
}

static lispval cachecall_probe(int n,lispval *args)
{
  if (HASHTABLEP(args[0]))
    return fd_xcachecall_try((fd_hashtable)args[0],args[1],n-2,args+2);
  else return fd_cachecall_try(args[0],n-1,args+1);
}

static lispval cachedcallp(int n,lispval *args)
{
  if (HASHTABLEP(args[0]))
    if (fd_xcachecall_probe((fd_hashtable)args[0],args[1],n-2,args+2))
      return FD_TRUE;
    else return FD_FALSE;
  else if (fd_cachecall_probe(args[0],n-1,args+1))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval clear_callcache(lispval arg)
{
  fd_clear_callcache(arg);
  return VOID;
}

static lispval tcachecall(int n,lispval *args)
{
  return fd_tcachecall(args[0],n-1,args+1);
}

static lispval with_threadcache_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  struct FD_THREAD_CACHE *tc = fd_push_threadcache(NULL);
  lispval value = VOID;
  FD_DOLIST(each,FD_CDR(expr)) {
    fd_decref(value); value = VOID; value = fd_eval(each,env);
    if (FD_ABORTED(value)) {
      fd_pop_threadcache(tc);
      return value;}}
  fd_pop_threadcache(tc);
  return value;
}

static lispval using_threadcache_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  struct FD_THREAD_CACHE *tc = fd_use_threadcache();
  lispval value = VOID;
  FD_DOLIST(each,FD_CDR(expr)) {
    fd_decref(value); value = VOID; value = fd_eval(each,env);
    if (FD_ABORTED(value)) {
      if (tc) fd_pop_threadcache(tc);
      return value;}}
  if (tc) fd_pop_threadcache(tc);
  return value;
}

static lispval use_threadcache_prim(lispval arg)
{
  if (FALSEP(arg)) {
    if (!(fd_threadcache)) return FD_FALSE;
    while (fd_threadcache) fd_pop_threadcache(fd_threadcache);
    return FD_TRUE;}
  else {
    struct FD_THREAD_CACHE *tc = fd_use_threadcache();
    if (tc) return FD_TRUE;
    else return FD_FALSE;}
}

/* Making DTPROCs */

static lispval make_dtproc(lispval name,lispval server,lispval min_arity,
                          lispval arity,lispval minsock,lispval maxsock,
                          lispval initsock)
{
  lispval result;
  if (VOIDP(min_arity))
    result = fd_make_dtproc(SYM_NAME(name),CSTRING(server),1,-1,-1,
                          FIX2INT(minsock),FIX2INT(maxsock),
                          FIX2INT(initsock));
  else if (VOIDP(arity))
    result = fd_make_dtproc
      (SYM_NAME(name),CSTRING(server),
       1,fd_getint(arity),fd_getint(arity),
       FIX2INT(minsock),FIX2INT(maxsock),
       FIX2INT(initsock));
  else result=
         fd_make_dtproc
         (SYM_NAME(name),CSTRING(server),1,
          fd_getint(arity),fd_getint(min_arity),
          FIX2INT(minsock),FIX2INT(maxsock),
          FIX2INT(initsock));
  return result;
}

/* Remote evaluation */

static u8_condition ServerUndefined=_("Server unconfigured");

FD_EXPORT lispval fd_open_dtserver(u8_string server,int bufsiz)
{
  struct FD_DTSERVER *dts = u8_alloc(struct FD_DTSERVER);
  u8_string server_addr; u8_socket socket;
  /* Start out by parsing the address */
  if ((*server)==':') {
    lispval server_id = fd_config_get(server+1);
    if (STRINGP(server_id))
      server_addr = u8_strdup(CSTRING(server_id));
    else  {
      fd_seterr(ServerUndefined,"open_server",
                dts->dtserverid,server_id);
      u8_free(dts);
      return -1;}}
  else server_addr = u8_strdup(server);
  dts->dtserverid = u8_strdup(server);
  dts->dtserver_addr = server_addr;
  /* Then try to connect, just to see if that works */
  socket = u8_connect_x(server,&(dts->dtserver_addr));
  if (socket<0) {
    /* If connecting fails, signal an error rather than creating
       the dtserver connection pool. */
    u8_free(dts->dtserverid);
    u8_free(dts->dtserver_addr);
    u8_free(dts);
    return fd_err(fd_ConnectionFailed,"fd_open_dtserver",
                  u8_strdup(server),VOID);}
  /* Otherwise, close the socket */
  else close(socket);
  /* And create a connection pool */
  dts->connpool = u8_open_connpool(dts->dtserverid,2,4,1);
  /* If creating the connection pool fails for some reason,
     cleanup and return an error value. */
  if (dts->connpool == NULL) {
    u8_free(dts->dtserverid);
    u8_free(dts->dtserver_addr);
    u8_free(dts);
    return FD_ERROR;}
  /* Otherwise, returh a dtserver object */
  FD_INIT_CONS(dts,fd_dtserver_type);
  return LISP_CONS(dts);
}

static lispval dtserverp(lispval arg)
{
  if (TYPEP(arg,fd_dtserver_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval dtserver_id(lispval arg)
{
  struct FD_DTSERVER *dts = (struct FD_DTSERVER *) arg;
  return lispval_string(dts->dtserverid);
}

static lispval dtserver_address(lispval arg)
{
  struct FD_DTSERVER *dts = (struct FD_DTSERVER *) arg;
  return lispval_string(dts->dtserver_addr);
}

static lispval dteval(lispval server,lispval expr)
{
  if (TYPEP(server,fd_dtserver_type))  {
    struct FD_DTSERVER *dtsrv=
      fd_consptr(fd_stream_erver,server,fd_dtserver_type);
    return fd_dteval(dtsrv->connpool,expr);}
  else if (STRINGP(server)) {
    lispval s = fd_open_dtserver(CSTRING(server),-1);
    if (FD_ABORTED(s)) return s;
    else {
      lispval result = fd_dteval(((fd_stream_erver)s)->connpool,expr);
      fd_decref(s);
      return result;}}
  else return fd_type_error(_("server"),"dteval",server);
}

static lispval dtcall(int n,lispval *args)
{
  lispval server; lispval request = NIL, result; int i = n-1;
  if (n<2) return fd_err(fd_SyntaxError,"dtcall",NULL,VOID);
  if (TYPEP(args[0],fd_dtserver_type))
    server = fd_incref(args[0]);
  else if (STRINGP(args[0])) server = fd_open_dtserver(CSTRING(args[0]),-1);
  else return fd_type_error(_("server"),"eval/dtcall",args[0]);
  if (FD_ABORTED(server)) return server;
  while (i>=1) {
    lispval param = args[i];
    if ((i>1) && ((SYMBOLP(param)) || (PAIRP(param))))
      request = fd_conspair(fd_make_list(2,quote_symbol,param),request);
    else request = fd_conspair(param,request);
    fd_incref(param); i--;}
  result = fd_dteval(((fd_stream_erver)server)->connpool,request);
  fd_decref(request);
  fd_decref(server);
  return result;
}

static lispval open_dtserver(lispval server,lispval bufsiz)
{
  return fd_open_dtserver(CSTRING(server),
                             ((VOIDP(bufsiz)) ? (-1) :
                              (FIX2INT(bufsiz))));
}

/* Getting documentation */

FD_EXPORT
u8_string fd_get_documentation(lispval x)
{
  lispval proc = (FD_FCNIDP(x)) ? (fd_fcnid_ref(x)) : (x);
  fd_ptr_type proctype = FD_PTR_TYPE(proc);
  if (proctype == fd_lambda_type) {
    struct FD_LAMBDA *lambda = (fd_lambda)proc;
    if (lambda->fcn_doc)
      return u8_strdup(lambda->fcn_doc);
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,120);
      lispval arglist = lambda->lambda_arglist, scan = arglist;
      if (lambda->fcn_name)
        u8_puts(&out,lambda->fcn_name);
      else u8_puts(&out,"λ");
      while (PAIRP(scan)) {
        lispval arg = FD_CAR(scan);
        if (SYMBOLP(arg))
          u8_printf(&out," %ls",SYM_NAME(arg));
        else if ((PAIRP(arg))&&(SYMBOLP(FD_CAR(arg))))
          u8_printf(&out," [%ls]",SYM_NAME(FD_CAR(arg)));
        else u8_printf(&out," %q",arg);
        scan = FD_CDR(scan);}
      if (SYMBOLP(scan))
        u8_printf(&out," [%ls...]",SYM_NAME(scan));
      lambda->fcn_doc = out.u8_outbuf;
      lambda->fcn_freedoc = 1;
      return out.u8_outbuf;}}
  else if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(proc);
    return u8_strdup(f->fcn_doc);}
  else if (TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = (fd_evalfn)proc;
    return u8_strdup(sf->evalfn_documentation);}
  else return NULL;
}

static lispval get_documentation(lispval x)
{
  u8_string doc = fd_get_documentation(x);
  if (doc) return lispval_string(doc);
  else return FD_FALSE;
}

/* Helpful forms and functions */

static lispval void_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = FD_CDR(expr);
  FD_DOLIST(subex,body) {
    lispval v = fd_stack_eval(subex,env,_stack,0);
    if (FD_ABORTED(v))
      return v;
    else fd_decref(v);}
  return VOID;
}

static lispval break_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = FD_CDR(expr);
  FD_DOLIST(subex,body) {
    lispval v = fd_stack_eval(subex,env,_stack,0);
    if (FD_BREAKP(v))
      return FD_BREAK;
    else if (FD_ABORTED(v))
      return v;
    else fd_decref(v);}
  return FD_BREAK;
}

static lispval default_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  lispval default_expr = fd_get_arg(expr,2);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"default_evalfn",NULL,fd_incref(expr));
  else if (VOIDP(default_expr))
    return fd_err(fd_SyntaxError,"default_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    else if (VOIDP(val))
      return fd_eval(default_expr,env);
    else if (val == FD_UNBOUND)
      return fd_eval(default_expr,env);
    else return val;}
}

/* Checking version numbers */

static int check_num(lispval arg,int num)
{
  if ((!(FIXNUMP(arg)))||(FIX2INT(arg)<0)) {
    fd_seterr(fd_TypeError,"check_version_prim",NULL,arg);
    return -1;}
  else {
    int n = FIX2INT(arg);
    if (num>=n)
      return 1;
    else return 0;}
}

static lispval check_version_prim(int n,lispval *args)
{
  int rv = check_num(args[0],FD_MAJOR_VERSION);
  if (rv<0) return FD_ERROR;
  else if (rv==0) return FD_FALSE;
  else if (n==1) return FD_TRUE;
  else rv = check_num(args[1],FD_MINOR_VERSION);
  if (rv<0) return FD_ERROR;
  else if (rv==0) return FD_FALSE;
  else if (n==2) return FD_TRUE;
  else rv = check_num(args[2],FD_RELEASE_VERSION);
  if (rv<0) return FD_ERROR;
  else if (rv==0) return FD_FALSE;
  else if (n==3) return FD_TRUE;
  else rv = check_num(args[2],FD_RELEASE_VERSION-1);
  /* The fourth argument should be a patch level, but we're not
     getting that in builds yet. So if there are more arguments,
     we see if required release number is larger than release-1
     (which means that we should be okay, since patch levels
     are reset with releases. */
  if (rv<0) return FD_ERROR;
  else if (rv) {
    int i = 3; while (i<n) {
      if (!(FIXNUMP(args[i]))) {
        fd_seterr(fd_TypeError,"check_version_prim",NULL,args[i]);
        return -1;}
      else i++;}
    return FD_TRUE;}
  else return FD_FALSE;
}

static lispval require_version_prim(int n,lispval *args)
{
  lispval result = check_version_prim(n,args);
  if (FD_ABORTP(result))
    return result;
  else if (FD_TRUEP(result))
    return result;
  else {
    u8_byte buf[50];
    int i = 0; while (i<n) {
      if (!(FD_FIXNUMP(args[i])))
        return fd_type_error("version number(integer)","require_version_prim",args[i]);
      else i++;}
    fd_seterr("VersionError","require_version_prim",
              u8_sprintf(buf,50,"Version is %s",FRAMERD_REVISION),
              /* We don't need to incref *args* because they're all fixnums */
              fd_make_vector(n,args));
    return FD_ERROR;}
}

/* Choice functions */

static lispval fixchoice_prim(lispval arg)
{
  if (PRECHOICEP(arg))
    return fd_make_simple_choice(arg);
  else return fd_incref(arg);
}

static lispval choiceref_prim(lispval arg,lispval off)
{
  if (PRED_TRUE(FIXNUMP(off))) {
    long long i = FIX2INT(off);
    if (i==0) {
      if (EMPTYP(arg)) {
        fd_seterr(fd_RangeError,"choiceref_prim","0",arg);
        return FD_ERROR;}
      else return fd_incref(arg);}
    else if (CHOICEP(arg)) {
      struct FD_CHOICE *ch = (fd_choice)arg;
      if (i>ch->choice_size) {
        u8_byte buf[50];
        fd_seterr(fd_RangeError,"choiceref_prim",
                  u8_sprintf(buf,50,"%lld",i),
                  fd_incref(arg));
        return FD_ERROR;}
      else {
        lispval elt = FD_XCHOICE_DATA(ch)[i];
        return fd_incref(elt);}}
    else if (PRECHOICEP(arg)) {
      lispval simplified = fd_make_simple_choice(arg);
      lispval result = choiceref_prim(simplified,off);
      fd_decref(simplified);
      return result;}
    else {
      u8_byte buf[50];
      fd_seterr(fd_RangeError,"choiceref_prim",
                u8_sprintf(buf,50,"%lld",i),fd_incref(arg));
      return FD_ERROR;}}
  else return fd_type_error("fixnum","choiceref_prim",off);
}

/* FFI */

#if FD_ENABLE_FFI
static lispval ffi_proc_helper(int err,int n,lispval *args)
{
  lispval name_arg = args[0], filename_arg = args[1];
  lispval return_type = args[2];
  u8_string name = (STRINGP(name_arg)) ? (CSTRING(name_arg)) : (NULL);
  u8_string filename = (STRINGP(filename_arg)) ?
    (CSTRING(filename_arg)) :
    (NULL);
  if (!(name))
    return fd_type_error("String","ffi_proc/name",name_arg);
  else if (!((STRINGP(filename_arg))||(FALSEP(filename_arg))))
    return fd_type_error("String","ffi_proc/filename",filename_arg);
  else {
    struct FD_FFI_PROC *fcn=
      fd_make_ffi_proc(name,filename,n-3,return_type,args+3);
    if (fcn)
      return (lispval) fcn;
    else {
      fd_clear_errors(1);
      return FD_FALSE;}}
}
static lispval ffi_proc(int n,lispval *args)
{
  return ffi_proc_helper(1,n,args);
}
static lispval ffi_probe(int n,lispval *args)
{
  return ffi_proc_helper(0,n,args);
}
static lispval ffi_found_prim(lispval name,lispval modname)
{
  void *module=NULL, *sym=NULL;
  if (STRINGP(modname)) {
    module=u8_dynamic_load(CSTRING(modname));
    if (module==NULL) {
      fd_clear_errors(0);
      return FD_FALSE;}}
  sym=u8_dynamic_symbol(CSTRING(name),module);
  if (sym)
    return FD_TRUE;
  else return FD_FALSE;
}
#else
static lispval ffi_proc(int n,lispval *args)
{
  u8_seterr("NotImplemented","ffi_proc",
            u8_strdup("No FFI support is available in this build of FramerD"));
  return FD_ERROR;
}
static lispval ffi_probe(int n,lispval *args)
{
  return FD_FALSE;
}
static lispval ffi_found_prim(lispval name,lispval modname)
{
  return FD_FALSE;
}
#endif

/* Initialization */

void fd_init_module_tables(void);
#include "opcode_names.h"

static void init_types_and_tables()
{
  struct FD_TABLEFNS *fns = u8_alloc(struct FD_TABLEFNS);
  fns->get = lispenv_get; fns->store = lispenv_store;
  fns->add = lispenv_add; fns->drop = NULL; fns->test = NULL;

  fd_tablefns[fd_lexenv_type]=fns;
  fd_recyclers[fd_evalfn_type]=recycle_evalfn;

  fd_unparsers[fd_evalfn_type]=unparse_evalfn;
  fd_dtype_writers[fd_evalfn_type]=write_evalfn_dtype;

  fd_unparsers[fd_lexref_type]=unparse_lexref;
  fd_dtype_writers[fd_lexref_type]=write_lexref_dtype;
  fd_type_names[fd_lexref_type]=_("lexref");

  fd_unparsers[fd_coderef_type]=unparse_coderef;
  fd_dtype_writers[fd_coderef_type]=write_coderef_dtype;
  fd_type_names[fd_coderef_type]=_("coderef");

  fd_type_names[fd_opcode_type]=_("opcode");
  fd_unparsers[fd_opcode_type]=unparse_opcode;
  fd_immediate_checkfns[fd_opcode_type]=validate_opcode;

  quote_symbol = fd_intern("QUOTE");
  _fd_comment_symbol = comment_symbol = fd_intern("COMMENT");
  moduleid_symbol = fd_intern("%MODULEID");
  source_symbol = fd_intern("%SOURCE");
  fcnids_symbol = fd_intern("%FCNIDS");
  sourceref_tag = fd_intern("%SOURCEREF");

  fd_init_module_tables();
  init_opcode_names();
}

static void init_localfns()
{
  fd_def_evalfn(fd_scheme_module,"EVAL","",eval_evalfn);
  fd_def_evalfn(fd_scheme_module,"BOUND?","",boundp_evalfn);
  fd_def_evalfn(fd_scheme_module,"UNBOUND?","",unboundp_evalfn);
  fd_def_evalfn(fd_scheme_module,"DEFINED?","",definedp_evalfn);
  fd_def_evalfn(fd_scheme_module,"VOID?","",voidp_evalfn);
  fd_def_evalfn(fd_scheme_module,"DEFAULT?","",defaultp_evalfn);
  fd_def_evalfn(fd_scheme_module,"CONSTANT?","",constantp_evalfn);
  fd_def_evalfn(fd_scheme_module,"BAD?","",badp_evalfn);
  fd_def_evalfn(fd_scheme_module,"QUOTE","",quote_evalfn);
  fd_def_evalfn(fd_scheme_module,"%ENV","",env_evalfn);
  fd_def_evalfn(fd_scheme_module,"%MODREF","",modref_evalfn);

  fd_def_evalfn(fd_scheme_module,"%ENV/RESET!",
                "Resets the cached dynamic copy of the current "
                "environment (if any). This means that procedures "
                "closed in the current environment will not be "
                "effected by future changes",
                env_reset_evalfn);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1("DOCUMENTATION",get_documentation,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1("ENVIRONMENT?",environmentp_prim,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2("SYMBOL-BOUND?",symbol_boundp_prim,2));
  fd_idefn2(fd_scheme_module,"%LEXREF",lexref_prim,2,
            "(%LEXREF *up* *across*) returns a lexref (lexical reference) "
            "given a 'number of environments' *up* and a 'number of bindings' "
            "across",
            fd_fixnum_type,VOID,fd_fixnum_type,VOID);
  fd_idefn1(fd_scheme_module,"%LEXREFVAL",lexref_value_prim,1,
            "(%LEXREFVAL *lexref*) returns the offsets "
            "of a lexref as a pair (*up* . *across*)",
            fd_lexref_type,VOID);
  fd_idefn1(fd_scheme_module,"%LEXREF?",lexrefp_prim,1,
            "(%LEXREF? *val*) returns true if it's argument "
            "is a lexref (lexical reference)",
            -1,FD_VOID);

  fd_idefn1(fd_scheme_module,"%CODEREF",coderef_prim,1,
            "(%CODEREF *nelts*) returns a 'coderef' (a relative position) value",
            fd_fixnum_type,VOID);
  fd_idefn1(fd_scheme_module,"%CODREFVAL",coderef_value_prim,1,
            "(%CODEREFVAL *coderef*) returns the integer relative offset of a coderef",
            fd_lexref_type,VOID);

  fd_idefn(fd_scheme_module,fd_make_cprim1("NAME->OPCODE",name2opcode_prim,1));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("%CHOICEREF",choiceref_prim,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("%FIXCHOICE",fixchoice_prim,1)));


  fd_def_evalfn(fd_scheme_module,"WITHENV","",withenv_safe_evalfn);
  fd_def_evalfn(fd_xscheme_module,"WITHENV","",withenv_evalfn);
  fd_def_evalfn(fd_xscheme_module,"WITHENV/SAFE","",withenv_safe_evalfn);


  fd_idefn3(fd_scheme_module,"GET-ARG",get_arg_prim,2,
            "`(GET-ARG *expression* *i* [*default*])` "
            "returns the *i*'th parameter in *expression*, "
            "or *default* (otherwise)",
            -1,FD_VOID,fd_fixnum_type,FD_VOID,-1,FD_VOID);
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("APPLY",apply_lexpr,1)));
  fd_idefn(fd_xscheme_module,fd_make_cprim7x
           ("DTPROC",make_dtproc,2,
            fd_symbol_type,VOID,fd_string_type,VOID,
            -1,VOID,-1,VOID,
            fd_fixnum_type,FD_INT(2),
            fd_fixnum_type,FD_INT(4),
            fd_fixnum_type,FD_INT(1)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("CALL/CC",callcc,1));
  fd_defalias(fd_scheme_module,"CALL-WITH-CURRENT-CONTINUATION","CALL/CC");

  /* This pushes a new threadcache */
  fd_def_evalfn(fd_scheme_module,"WITH-THREADCACHE","",with_threadcache_evalfn);
  /* This ensures that there's an active threadcache, pushing a new one if
     needed or using the current one if it exists. */
  fd_def_evalfn(fd_scheme_module,"USING-THREADCACHE","",using_threadcache_evalfn);
  /* This sets up the current thread to use a threadcache */
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("USE-THREADCACHE",use_threadcache_prim,0));

  fd_idefn(fd_scheme_module,fd_make_cprimn("TCACHECALL",tcachecall,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("CACHECALL",cachecall,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("CACHECALL/PROBE",cachecall_probe,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("CACHEDCALL?",cachedcallp,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("CLEAR-CALLCACHE!",clear_callcache,0));
  fd_defalias(fd_scheme_module,"CACHEPOINT","TCACHECALL");

  fd_def_evalfn(fd_scheme_module,"VOID","",void_evalfn);
  fd_def_evalfn(fd_scheme_module,"BREAK","",break_evalfn);
  fd_def_evalfn(fd_scheme_module,"DEFAULT","",default_evalfn);

  fd_idefn(fd_scheme_module,fd_make_cprimn("CHECK-VERSION",check_version_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("REQUIRE-VERSION",require_version_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("DTEVAL",dteval,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("DTCALL",dtcall,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("OPEN-DTSERVER",open_dtserver,1,
                                            fd_string_type,VOID,
                                            fd_fixnum_type,VOID));
  fd_idefn1(fd_scheme_module,"DTSERVER?",dtserverp,FD_NEEDS_1_ARG,
            "Returns true if it's argument is a dtype server object",
            -1,VOID);
  fd_idefn1(fd_scheme_module,"DTSERVER-ID",
            dtserver_id,FD_NEEDS_1_ARG,
            "Returns the ID of a dtype server (the argument used to create it)",
            fd_dtserver_type,VOID);
  fd_idefn1(fd_scheme_module,"DTSERVER-ADDRESS",
            dtserver_address,FD_NEEDS_1_ARG,
            "Returns the address (host/port) of a dtype server",
            fd_dtserver_type,VOID);

  fd_idefn0(fd_scheme_module,"%BUILDINFO",fd_get_build_info,
            "Information about the build and startup environment");

  fd_idefn(fd_xscheme_module,fd_make_cprimn("FFI/PROC",ffi_proc,3));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("FFI/PROBE",ffi_probe,3));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("FFI/FOUND?",ffi_found_prim,1,
                           fd_string_type,VOID,
                           fd_string_type,VOID));

  fd_register_config
    ("TAILCALL","Enable/disable tail recursion in the Scheme evaluator",
     fd_boolconfig_get,fd_boolconfig_set,&fd_optimize_tail_calls);

}

FD_EXPORT void fd_init_errfns_c(void);
FD_EXPORT void fd_init_compoundfns_c(void);
FD_EXPORT void fd_init_threads_c(void);
FD_EXPORT void fd_init_conditionals_c(void);
FD_EXPORT void fd_init_iterators_c(void);
FD_EXPORT void fd_init_choicefns_c(void);
FD_EXPORT void fd_init_binders_c(void);
FD_EXPORT void fd_init_lambdas_c(void);
FD_EXPORT void fd_init_macros_c(void);
FD_EXPORT void fd_init_coreprims_c(void);
FD_EXPORT void fd_init_tableprims_c(void);
FD_EXPORT void fd_init_stringprims_c(void);
FD_EXPORT void fd_init_dbprims_c(void);
FD_EXPORT void fd_init_seqprims_c(void);
FD_EXPORT void fd_init_modules_c(void);
FD_EXPORT void fd_init_load_c(void);
FD_EXPORT void fd_init_logprims_c(void);
FD_EXPORT void fd_init_portprims_c(void);
FD_EXPORT void fd_init_streamprims_c(void);
FD_EXPORT void fd_init_timeprims_c(void);
FD_EXPORT void fd_init_sysprims_c(void);
FD_EXPORT void fd_init_arith_c(void);
FD_EXPORT void fd_init_side_effects_c(void);
FD_EXPORT void fd_init_reflection_c(void);
FD_EXPORT void fd_init_reqstate_c(void);
FD_EXPORT void fd_init_regex_c(void);
FD_EXPORT void fd_init_quasiquote_c(void);
FD_EXPORT void fd_init_struct_eval_c(void);
FD_EXPORT void fd_init_extdbprims_c(void);
FD_EXPORT void fd_init_history_c(void);
FD_EXPORT void fd_init_eval_appenv_c(void);
FD_EXPORT void fd_init_eval_moduleops_c(void);
FD_EXPORT void fd_init_eval_getopt_c(void);
FD_EXPORT void fd_init_eval_debug_c(void);
FD_EXPORT void fd_init_eval_testops_c(void);

static void init_eval_core()
{
  init_localfns();
  fd_init_eval_getopt_c();
  fd_init_eval_debug_c();
  fd_init_eval_testops_c();
  fd_init_tableprims_c();
  fd_init_modules_c();
  fd_init_arith_c();
  fd_init_threads_c();
  fd_init_errfns_c();
  fd_init_load_c();
  fd_init_conditionals_c();
  fd_init_iterators_c();
  fd_init_choicefns_c();
  fd_init_binders_c();
  fd_init_lambdas_c();
  fd_init_macros_c();
  fd_init_compoundfns_c();
  fd_init_quasiquote_c();
  fd_init_struct_eval_c();
  fd_init_side_effects_c();
  fd_init_reflection_c();
  fd_init_reqstate_c();

  fd_init_eval_appenv_c();

  fd_init_regex_c();

  fd_init_history_c();

  fd_init_coreprims_c();
  fd_init_stringprims_c();
  fd_init_dbprims_c();
  fd_init_seqprims_c();
  fd_init_logprims_c();
  fd_init_portprims_c();
  fd_init_streamprims_c();
  fd_init_timeprims_c();
  fd_init_sysprims_c();
  fd_init_extdbprims_c();

  u8_threadcheck();

  fd_finish_module(fd_scheme_module);
  fd_finish_module(fd_xscheme_module);
}

FD_EXPORT int fd_load_scheme()
{
  return fd_init_scheme();
}

FD_EXPORT int fd_init_scheme()
{
  if (fd_scheme_initialized) return fd_scheme_initialized;
  else {
    fd_scheme_initialized = 401*fd_init_storage()*u8_initialize();

    init_types_and_tables();

    u8_register_source_file(FRAMERD_EVAL_H_INFO);
    u8_register_source_file(_FILEINFO);

    init_eval_core();

    /* This sets up the fd_atexit handler for recycling the
       application environment. Consequently, it needs to be called
       before setting up any fd_atexit handlers which might use the
       application environment. */
    fd_setup_app_env();

    return fd_scheme_initialized;}
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
