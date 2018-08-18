/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_EVAL_H
#define FRAMERD_EVAL_H 1
#ifndef FRAMERD_EVAL_H_INFO
#define FRAMERD_EVAL_H_INFO "include/framerd/eval.h"
#endif

#include "framerd/lexenv.h"
#include "framerd/apply.h"
#include "framerd/threads.h"
#include <assert.h>

FD_EXPORT u8_condition fd_UnboundIdentifier, fd_BindError;
FD_EXPORT u8_condition fd_VoidArgument, fd_VoidBinding;
FD_EXPORT u8_condition fd_NoSuchModule, fd_SyntaxError, fd_BindSyntaxError;
FD_EXPORT u8_condition fd_TooFewExpressions, fd_NotAnIdentifier;
FD_EXPORT u8_condition fd_InvalidMacro, FD_BadArglist;
FD_EXPORT u8_condition fd_ReadOnlyEnv;

FD_EXPORT lispval fd_scheme_module, fd_xscheme_module;

FD_EXPORT lispval _fd_comment_symbol;

FD_EXPORT int fd_load_scheme(void) FD_LIBINIT0_FN;
FD_EXPORT int fd_init_scheme(void);
FD_EXPORT void fd_init_schemeio(void) FD_LIBINIT0_FN;

FD_EXPORT int (*fd_dump_exception)(lispval bt);

FD_EXPORT int fd_choice_evalp(lispval x);

#define FD_NEED_EVALP(x) \
  ((FD_SYMBOLP(x)) || (FD_LEXREFP(x)) ||        \
   (FD_PAIRP(x))   || (FD_CODEP(x)))
#define FD_EVALP(x) \
  ( (FD_SYMBOLP(x)) || (FD_LEXREFP(x)) || \
    (FD_PAIRP(x))   || (FD_CODEP(x))   ||       \
    ( (FD_AMBIGP(x)) && (fd_choice_evalp(x)) ) )

FD_EXPORT u8_string fd_evalstack_type, fd_ndevalstack_type;

/* Constants */

#define FD_STACK_ARGS 6

/* Environments */

#define FD_MODULE_SAFE 1
#define FD_MODULE_DEFAULT 2

FD_EXPORT int fd_assign_value(lispval,lispval,fd_lexenv);
FD_EXPORT int fd_add_value(lispval,lispval,fd_lexenv);
FD_EXPORT int fd_bind_value(lispval,lispval,fd_lexenv);

FD_EXPORT fd_lexenv fd_app_env;

FD_EXPORT void fd_set_app_env(fd_lexenv env);

FD_EXPORT void fd_autoload_config
(u8_string module_inits,u8_string file_inits,u8_string run_inits);

/* Eval functions (for special forms, FEXPRs, whatever) */

typedef lispval (*fd_eval_handler)(lispval expr,
                            struct FD_LEXENV *,
                            struct FD_STACK *stack);

typedef struct FD_EVALFN {
  FD_CONS_HEADER;
  u8_string evalfn_name, evalfn_filename;
  u8_string evalfn_documentation;
  lispval evalfn_moduleid;
  fd_eval_handler evalfn_handler;} FD_EVALFN;
typedef struct FD_EVALFN *fd_evalfn;

FD_EXPORT lispval fd_make_evalfn(u8_string name,fd_eval_handler fn);
FD_EXPORT void fd_defspecial(lispval mod,u8_string name,fd_eval_handler fn);
FD_EXPORT void fd_new_evalfn(lispval mod,u8_string name,
                             u8_string filename,
                             u8_string doc,
                             fd_eval_handler fn);

#define fd_def_evalfn(mod,name,doc,evalfn) \
  fd_new_evalfn(mod,name,_FILEINFO,doc,evalfn)

typedef struct FD_MACRO {
  FD_CONS_HEADER;
  u8_string macro_name;
  u8_string macro_filename;
  lispval macro_moduleid;
  lispval macro_transformer;} FD_MACRO;
typedef struct FD_MACRO *fd_macro;

#define FD_MACROP(x) (FD_TYPEP((x),fd_macro_type))

/* These should probably get their own header file */

FD_EXPORT lispval fd_printout(lispval,fd_lexenv);
FD_EXPORT lispval fd_printout_to(U8_OUTPUT *,lispval,fd_lexenv);

/* Getting documentation strings */

FD_EXPORT u8_string fd_get_documentation(lispval x);

/* DT servers */

typedef struct FD_DTSERVER {
  FD_CONS_HEADER;
  u8_string dtserverid, dtserver_addr;
  struct U8_CONNPOOL *connpool;} FD_DTSERVER;
typedef struct FD_DTSERVER *fd_stream_erver;

/* Modules */

FD_EXPORT fd_lexenv fd_new_lexenv(lispval bindings,int safe);
FD_EXPORT fd_lexenv fd_working_lexenv(void);
FD_EXPORT fd_lexenv fd_safe_working_lexenv(void);
FD_EXPORT fd_lexenv fd_make_env(lispval module,fd_lexenv parent);
FD_EXPORT fd_lexenv fd_make_export_env(lispval exports,fd_lexenv parent);
FD_EXPORT lispval fd_register_module_x(lispval name,lispval module,int flags);
FD_EXPORT lispval fd_register_module(u8_string name,lispval module,int flags);
FD_EXPORT lispval fd_get_module(lispval name,int safe);
FD_EXPORT int fd_discard_module(lispval name,int safe);

FD_EXPORT lispval fd_all_modules(void);
FD_EXPORT lispval fd_safe_modules(void);

FD_EXPORT int fd_module_finished(lispval module,int flags);
FD_EXPORT int fd_finish_module(lispval module);
FD_EXPORT int fd_static_module(lispval module);
FD_EXPORT int fd_lock_exports(lispval module);

FD_EXPORT lispval fd_find_module(lispval,int,int);
FD_EXPORT lispval fd_new_module(char *name,int flags);
FD_EXPORT lispval fd_new_cmodule(char *name,int flags,void *addr);

FD_EXPORT lispval fd_use_module(fd_lexenv env,lispval module);


FD_EXPORT void fd_add_module_loader(int (*loader)(lispval,int,void *),void *);

#define FD_LOCK_EXPORTS 0x01
#define FD_FIX_EXPORTS 0x02
#define FD_STATIC_EXPORTS 0x04
#define FD_LOCK_MODULES 0x08
#define FD_FIX_MODULES 0x10
#define FD_STATIC_MODULES 0x20
#define FD_OPTIMIZE_EXPORTS 0x03

/* LAMBDAs */

typedef struct FD_LAMBDA {
  FD_FUNCTION_FIELDS;
  short lambda_n_vars, lambda_synchronized;
  lispval *lambda_vars, lambda_arglist, lambda_body, lambda_source;
  lispval lambda_optimizer;
  struct FD_VECTOR *lambda_bytecode;
  fd_lexenv lambda_env;
  U8_MUTEX_DECL(lambda_lock);
} FD_LAMBDA;
typedef struct FD_LAMBDA *fd_lambda;

FD_EXPORT int fd_record_source;

#define FD_SET_LAMBDA_SOURCE(lambda,src)                        \
  if (fd_record_source) {                               \
    struct FD_LAMBDA *s=((struct FD_LAMBDA *)lambda);   \
  s->lambda_source=src; fd_incref(s->lambda_source);}   \
  else {}

FD_EXPORT lispval fd_apply_lambda(struct FD_STACK *,struct FD_LAMBDA *fn,
                                  int n,lispval *args);
FD_EXPORT lispval fd_xapply_lambda
(struct FD_LAMBDA *fn,void *data,lispval (*getval)(void *,lispval));

FD_EXPORT lispval fd_make_lambda(u8_string name,
                                 lispval arglist,lispval body,fd_lexenv env,
                                 int nd,int sync);



/* Loading files and config data */

typedef struct FD_SOURCEFN {
  u8_string (*getsource)(int op,u8_string,u8_string,u8_string *,time_t *timep,void *);
  void *getsource_data;
  struct FD_SOURCEFN *getsource_next;} FD_SOURCEFN;
typedef struct FD_SOURCEFN *fd_sourcefn;

FD_EXPORT u8_string fd_get_source(u8_string,u8_string,u8_string *,time_t *);
FD_EXPORT int fd_probe_source(u8_string,u8_string *,time_t *);
FD_EXPORT void fd_register_sourcefn
  (u8_string (*fn)(int op,u8_string,u8_string,u8_string *,time_t *,void *),
   void *sourcefn_data);

FD_EXPORT int fd_load_config(u8_string sourceid);
FD_EXPORT int fd_load_default_config(u8_string sourceid);
FD_EXPORT lispval fd_load_stream
  (u8_input loadstream,fd_lexenv env,u8_string sourcebase);
FD_EXPORT lispval fd_load_source
  (u8_string sourceid,fd_lexenv env,u8_string enc_name);
FD_EXPORT lispval fd_load_source_with_date
  (u8_string sourceid,fd_lexenv env,u8_string enc_name,time_t *modtime);

typedef struct FD_CONFIG_RECORD {
  u8_string config_filename;
  struct FD_CONFIG_RECORD *loaded_after;} FD_CONFIG_RECORD;

/* The Evaluator */

FD_EXPORT
lispval fd_stack_eval(lispval expr,fd_lexenv env,
                     struct FD_STACK *stack,
                     int tail);
#define fd_tail_eval(expr,env) (fd_stack_eval(expr,env,fd_stackptr,1))
FD_EXPORT
lispval fd_pair_eval(lispval head,lispval expr,fd_lexenv env,
                     struct FD_STACK *_stack,
                     int tail);

FD_EXPORT lispval fd_eval_exprs(lispval exprs,fd_lexenv env);

/* These are for non-static/inline versions */
FD_EXPORT lispval _fd_eval(lispval expr,fd_lexenv env);
FD_EXPORT lispval _fd_get_arg(lispval expr,int i);
FD_EXPORT lispval _fd_get_body(lispval expr,int i);

#if FD_PROVIDE_FASTEVAL
FD_FASTOP lispval fastget(lispval table,lispval key)
{
  fd_ptr_type argtype = FD_PTR_TYPE(table);
  switch (argtype) {
  case fd_schemap_type:
    return fd_schemap_get((fd_schemap)table,key,FD_UNBOUND);
  case fd_slotmap_type:
    return fd_slotmap_get((fd_slotmap)table,key,FD_UNBOUND);
  case fd_hashtable_type:
    return fd_hashtable_get((fd_hashtable)table,key,FD_UNBOUND);
  default: return fd_get(table,key,FD_UNBOUND);}
}
FD_FASTOP lispval fd_lexref(lispval lexref,fd_lexenv env)
{
  int code = FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up = code/32, across = code%32;
  while ((env) && (up)) {
    if (env->env_copy) env = env->env_copy;
    env = env->env_parent; up--;}
  if (FD_EXPECT_TRUE(env!=NULL)) {
    if (env->env_copy) env = env->env_copy;
    lispval bindings = env->env_bindings;
    if (FD_EXPECT_TRUE(FD_SCHEMAPP(bindings))) {
      struct FD_SCHEMAP *s = (struct FD_SCHEMAP *)bindings;
      return fd_incref(s->schema_values[across]);}}
  return fd_err("Bad lexical reference","fd_lexref",NULL,FD_VOID);
}
FD_FASTOP lispval fd_symeval(lispval symbol,fd_lexenv env)
{
  if (env == NULL) return FD_VOID;
  if (env->env_copy) env = env->env_copy;
  while (env) {
    lispval val = fastget(env->env_bindings,symbol);
    if (val == FD_UNBOUND)
      env = env->env_parent;
    else return val;
    if ((env) && (env->env_copy))
      env = env->env_copy;}
  return FD_VOID;
}

FD_FASTOP lispval _fd_fast_eval(lispval x,fd_lexenv env,
                               struct FD_STACK *stack,
                               int tail)
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
  case fd_slotmap_type: case fd_schemap_type:
    return fd_deep_copy(x);
  case fd_cons_ptr_type: {
    fd_ptr_type type = FD_CONSPTR_TYPE(x);
    switch (type) {
    case fd_pair_type: case fd_code_type:
    case fd_choice_type: case fd_prechoice_type:
      return fd_stack_eval(x,env,stack,tail);
    case fd_slotmap_type: case fd_schemap_type:
      return fd_deep_copy(x);
    default:
      return fd_incref(x);}}
  default: /* Never reached */
    return x;
  }
}

#define fd_eval(x,env) (_fd_fast_eval(x,env,fd_stackptr,0))

FD_FASTOP lispval fd_get_arg(lispval expr,int i)
{
  while (FD_PAIRP(expr))
    if ((FD_PAIRP(FD_CAR(expr))) &&
        (FD_EQ(FD_CAR(FD_CAR(expr)),_fd_comment_symbol)))
      expr = FD_CDR(expr);
    else if (i == 0) return FD_CAR(expr);
    else {expr = FD_CDR(expr); i--;}
  return FD_VOID;
}
FD_FASTOP lispval fd_get_body(lispval expr,int i)
{
  while (FD_PAIRP(expr))
    if (i == 0) break;
    else if ((FD_PAIRP(FD_CAR(expr))) &&
             (FD_EQ(FD_CAR(FD_CAR(expr)),_fd_comment_symbol)))
      expr = FD_CDR(expr);
    else {expr = FD_CDR(expr); i--;}
  return expr;
}
#else
FD_EXPORT lispval _fd_symeval(lispval,fd_lexenv);
#define fd_eval(x,env) _fd_eval(x,env)
#define fd_symeval(x,env) _fd_symeval(x,env)
#define fd_lexref(x,env) _fd_lexref(x,env)
#define fd_get_arg(x,i) _fd_get_arg(x,i)
#define fd_get_body(x,i) _fd_get_body(x,i)
#endif

/* Body iteration */

/* Bindings iteration */

#define FD_UNPACK_BINDING(pair,var,val)         \
  if (FD_PAIRP(pair)) {                         \
    lispval _cdr = FD_CDR(pair);                        \
    var = FD_CAR(pair);                         \
    if (FD_PAIRP(_cdr)) val = FD_CAR(_cdr);     \
    else val = FD_VOID;}                                \
  else {}

#define FD_DOBINDINGS(var,val,bindings)                                 \
  U8_MAYBE_UNUSED lispval var, val, _scan = bindings, _binding = FD_VOID;       \
  for (_scan = bindings,                                                        \
         _binding = FD_PAIRP(_scan)?(FD_CAR(_scan)):(FD_VOID),          \
         var = FD_PAIRP(_binding)?(FD_CAR(_binding)):(FD_VOID),         \
         val = ((FD_PAIRP(_binding)&&(FD_PAIRP(FD_CDR(_binding))))?     \
              (FD_CAR(FD_CDR(_binding))):(FD_VOID));                    \
       FD_PAIRP(_scan);                                                 \
       _scan = FD_CDR(_scan),                                           \
         _binding = FD_PAIRP(_scan)?(FD_CAR(_scan)):(FD_VOID),          \
         var = FD_PAIRP(_binding)?(FD_CAR(_binding)):(FD_VOID),         \
         val = ((FD_PAIRP(_binding)&&(FD_PAIRP(FD_CDR(_binding))))?     \
              (FD_CAR(FD_CDR(_binding))):(FD_VOID)) )

/* Simple continuations */

typedef struct FD_CONTINUATION {
  FD_FUNCTION_FIELDS; lispval retval;} FD_CONTINUATION;
typedef struct FD_CONTINUATION *fd_continuation;

/* Basic thread eval/apply functions */

FD_EXPORT fd_thread_struct fd_thread_call(lispval *,lispval,int,lispval *,int);
FD_EXPORT fd_thread_struct fd_thread_eval(lispval *,lispval,fd_lexenv,int);

/* Opcodes */

FD_EXPORT lispval fd_opcode_dispatch
(lispval opcode,lispval expr,fd_lexenv env,
 struct FD_STACK *,int tail);

/* Recording bugs */

FD_EXPORT int fd_dump_bug(lispval ex,u8_string dir);
FD_EXPORT int fd_record_bug(lispval ex);

FD_EXPORT u8_string fd_bugdir;

#endif /* FRAMERD_EVAL_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
