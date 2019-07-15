/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_EVAL_H
#define KNO_EVAL_H 1
#ifndef KNO_EVAL_H_INFO
#define KNO_EVAL_H_INFO "include/kno/eval.h"
#endif

#include "kno/lexenv.h"
#include "kno/apply.h"
#include "kno/threads.h"

#include <libu8/u8printf.h>
#include <assert.h>

KNO_EXPORT u8_condition kno_UnboundIdentifier, kno_BindError;
KNO_EXPORT u8_condition kno_VoidArgument, kno_VoidBinding;
KNO_EXPORT u8_condition kno_VoidBoolean, knoVoid_Result, kno_VoidSortKey;
KNO_EXPORT u8_condition kno_NoSuchModule, kno_SyntaxError, kno_BindSyntaxError;
KNO_EXPORT u8_condition kno_TooFewExpressions, kno_NotAnIdentifier;
KNO_EXPORT u8_condition kno_InvalidMacro, KNO_BadArglist;
KNO_EXPORT u8_condition kno_ReadOnlyEnv;

KNO_EXPORT lispval kno_scheme_module;
KNO_EXPORT lispval kno_io_module;
KNO_EXPORT lispval kno_sys_module;
KNO_EXPORT lispval kno_db_module;

KNO_EXPORT lispval _kno_comment_symbol;

KNO_EXPORT int kno_scheme_initialized;
KNO_EXPORT int kno_load_scheme(void) KNO_LIBINIT0_FN;
KNO_EXPORT int kno_init_scheme(void);
KNO_EXPORT void kno_init_schemeio(void) KNO_LIBINIT0_FN;

KNO_EXPORT int (*kno_dump_exception)(lispval bt);

KNO_EXPORT int kno_choice_evalp(lispval x);

#define KNO_NEED_EVALP(x) \
  ((KNO_SYMBOLP(x)) || (KNO_LEXREFP(x)) ||        \
   (KNO_PAIRP(x)))
#define KNO_EVALP(x) \
  ( (KNO_SYMBOLP(x)) || (KNO_LEXREFP(x)) || \
    (KNO_PAIRP(x))  ||       \
    ( (KNO_AMBIGP(x)) && (kno_choice_evalp(x)) ) )

KNO_EXPORT u8_string kno_evalstack_type, kno_ndevalstack_type;

#define KNO_MODULE_OPTIONAL 0
#define KNO_MODULE_DEFAULT 1

/* Constants */

#define KNO_STACK_ARGS 6

/* Environments */

KNO_EXPORT int kno_assign_value(lispval,lispval,kno_lexenv);
KNO_EXPORT int kno_add_value(lispval,lispval,kno_lexenv);
KNO_EXPORT int kno_bind_value(lispval,lispval,kno_lexenv);

KNO_EXPORT kno_lexenv kno_app_env;

KNO_EXPORT void kno_set_app_env(kno_lexenv env);
KNO_EXPORT void kno_setup_app_env(void);

KNO_EXPORT void kno_autoload_config
(u8_string module_inits,u8_string file_inits,u8_string run_inits);

/* Eval functions (for special forms, FEXPRs, whatever) */

typedef lispval (*kno_eval_handler)(lispval expr,
                            struct KNO_LEXENV *,
                            struct KNO_STACK *stack);

typedef struct KNO_EVALFN {
  KNO_CONS_HEADER;
  u8_string evalfn_name, evalfn_filename;
  u8_string evalfn_documentation;
  lispval evalfn_moduleid;
  kno_eval_handler evalfn_handler;} KNO_EVALFN;
typedef struct KNO_EVALFN *kno_evalfn;

KNO_EXPORT lispval kno_make_evalfn(u8_string name,kno_eval_handler fn);
KNO_EXPORT void kno_new_evalfn(lispval mod,u8_string name,
                             u8_string filename,
                             u8_string doc,
                             kno_eval_handler fn);

#define kno_def_evalfn(mod,name,doc,evalfn) \
  kno_new_evalfn(mod,name,_FILEINFO,doc,evalfn)

typedef struct KNO_MACRO {
  KNO_CONS_HEADER;
  u8_string macro_name;
  u8_string macro_filename;
  lispval macro_moduleid;
  lispval macro_transformer;} KNO_MACRO;
typedef struct KNO_MACRO *kno_macro;

#define KNO_MACROP(x) (KNO_TYPEP((x),kno_macro_type))

/* These should probably get their own header file */

KNO_EXPORT lispval kno_printout(lispval,kno_lexenv);
KNO_EXPORT lispval kno_printout_to(U8_OUTPUT *,lispval,kno_lexenv);

/* Getting documentation strings */

KNO_EXPORT u8_string kno_get_documentation(lispval x);

/* DT servers */

typedef struct KNO_DTSERVER {
  KNO_CONS_HEADER;
  u8_string dtserverid, dtserver_addr;
  struct U8_CONNPOOL *connpool;} KNO_DTSERVER;
typedef struct KNO_DTSERVER *kno_stream_erver;

/* Modules */

KNO_EXPORT kno_lexenv kno_new_lexenv(lispval bindings);
KNO_EXPORT kno_lexenv kno_working_lexenv(void);
KNO_EXPORT kno_lexenv kno_make_env(lispval module,kno_lexenv parent);
KNO_EXPORT kno_lexenv kno_make_export_env(lispval exports,kno_lexenv parent);
KNO_EXPORT lispval kno_register_module_x(lispval name,lispval module,int flags);
KNO_EXPORT lispval kno_register_module(u8_string name,lispval module,int flags);
KNO_EXPORT lispval kno_get_module(lispval name);
KNO_EXPORT int kno_discard_module(lispval name);

KNO_EXPORT lispval kno_all_modules(void);

KNO_EXPORT int kno_module_finished(lispval module,int flags);
KNO_EXPORT int kno_finish_module(lispval module);
KNO_EXPORT int kno_static_module(lispval module);
KNO_EXPORT int kno_lock_exports(lispval module);

KNO_EXPORT lispval kno_find_module(lispval,int);
KNO_EXPORT lispval kno_new_module(char *name,int flags);
KNO_EXPORT lispval kno_new_cmodule_x
(char *name,int flags,void *addr,u8_string filename);

#define kno_new_cmodule(name,flags,addr) \
  kno_new_cmodule_x(name,flags,addr,__FILE__)

KNO_EXPORT lispval kno_use_module(kno_lexenv env,lispval module);

KNO_EXPORT int kno_log_reloads;

KNO_EXPORT void kno_add_module_loader(int (*loader)(lispval,void *),void *);

#define KNO_LOCK_EXPORTS 0x01
#define KNO_FIX_EXPORTS 0x02
#define KNO_STATIC_EXPORTS 0x04
#define KNO_LOCK_MODULES 0x08
#define KNO_FIX_MODULES 0x10
#define KNO_STATIC_MODULES 0x20
#define KNO_OPTIMIZE_EXPORTS 0x03

/* LAMBDAs */

typedef struct KNO_LAMBDA {
  KNO_FUNCTION_FIELDS;
  short lambda_n_vars, lambda_synchronized;
  lispval *lambda_vars, *lambda_inits;
  lispval lambda_arglist, lambda_body, lambda_source;
  lispval lambda_optimizer, lambda_start;
  struct KNO_CONSBLOCK *lambda_consblock;
  kno_lexenv lambda_env;
  U8_MUTEX_DECL(lambda_lock);
} KNO_LAMBDA;
typedef struct KNO_LAMBDA *kno_lambda;

KNO_EXPORT int kno_record_source;

#define KNO_SET_LAMBDA_SOURCE(lambda,src)                        \
  if (kno_record_source) {                               \
    struct KNO_LAMBDA *s=((struct KNO_LAMBDA *)lambda);   \
  s->lambda_source=src; kno_incref(s->lambda_source);}   \
  else {}

KNO_EXPORT lispval kno_apply_lambda(struct KNO_STACK *,struct KNO_LAMBDA *fn,
                                  int n,lispval *args);
KNO_EXPORT lispval kno_xapply_lambda
(struct KNO_LAMBDA *fn,void *data,lispval (*getval)(void *,lispval));

KNO_EXPORT lispval kno_make_lambda(u8_string name,
                                 lispval arglist,lispval body,kno_lexenv env,
                                 int nd,int sync);
KNO_EXPORT int kno_set_lambda_schema(struct KNO_LAMBDA *s,lispval args);

/* QCODE */

typedef struct KNO_QCODE {
  const lispval *codes;
  const int code_len;} *kno_qcode;

/* Loading files and config data */

KNO_EXPORT int kno_load_config(u8_string sourceid);
KNO_EXPORT int kno_load_default_config(u8_string sourceid);
KNO_EXPORT lispval kno_load_stream
  (u8_input loadstream,kno_lexenv env,u8_string sourcebase);
KNO_EXPORT lispval kno_load_source
  (u8_string sourceid,kno_lexenv env,u8_string enc_name);
KNO_EXPORT lispval kno_load_source_with_date
  (u8_string sourceid,kno_lexenv env,u8_string enc_name,time_t *modtime);

typedef struct KNO_CONFIG_RECORD {
  u8_string config_filename;
  struct KNO_CONFIG_RECORD *loaded_after;} KNO_CONFIG_RECORD;

/* The Evaluator */

KNO_EXPORT
lispval kno_stack_eval(lispval expr,kno_lexenv env,
                     struct KNO_STACK *stack,
                     int tail);
#define kno_tail_eval(expr,env) (kno_stack_eval(expr,env,kno_stackptr,1))

KNO_EXPORT lispval kno_eval_exprs(lispval exprs,kno_lexenv env,kno_stack s,int);

/* These are for non-static/inline versions */
KNO_EXPORT lispval _kno_eval(lispval expr,kno_lexenv env);
KNO_EXPORT lispval _kno_get_arg(lispval expr,int i);
KNO_EXPORT lispval _kno_get_body(lispval expr,int i);

#if KNO_PROVIDE_FASTEVAL
KNO_FASTOP lispval fastget(lispval table,lispval key)
{
  kno_lisp_type argtype = KNO_LISP_TYPE(table);
  switch (argtype) {
  case kno_schemap_type:
    return kno_schemap_get((kno_schemap)table,key,KNO_UNBOUND);
  case kno_hashtable_type:
    return kno_hashtable_get((kno_hashtable)table,key,KNO_UNBOUND);
  default: return kno_get(table,key,KNO_UNBOUND);}
}
KNO_FASTOP lispval kno_lexref(lispval lexref,kno_lexenv env_arg)
{
  int code = KNO_GET_IMMEDIATE(lexref,kno_lexref_type);
  int up = code/32, across = code%32;
  kno_lexenv env = env_arg;
  while ((env) && (up)) {
    if (env->env_copy) env = env->env_copy;
    env = env->env_parent; up--;}
  if (KNO_EXPECT_TRUE(env!=NULL)) {
    if (env->env_copy) env = env->env_copy;
    lispval bindings = env->env_bindings;
    if (KNO_EXPECT_TRUE(KNO_SCHEMAPP(bindings))) {
      struct KNO_SCHEMAP *s = (struct KNO_SCHEMAP *)bindings;
      if ( across < s->schema_length)
        return kno_make_simple_choice(s->schema_values[across]);}}
  lispval env_ptr = (lispval) env_arg;
  u8_byte errbuf[64];
  return kno_err("Bad lexical reference","kno_lexref",
                 u8_bprintf(errbuf,"up=%d,across=%d",up, across),
                 ((KNO_STATICP(env_ptr)) ? KNO_FALSE : (env_ptr)));
}
KNO_FASTOP lispval kno_symeval(lispval symbol,kno_lexenv env)
{
  if (env == NULL) return KNO_VOID;
  if (env->env_copy) env = env->env_copy;
  while (env) {
    lispval val = fastget(env->env_bindings,symbol);
    if (val == KNO_UNBOUND)
      env = env->env_parent;
    else return val;
    if ((env) && (env->env_copy))
      env = env->env_copy;}
  return KNO_VOID;
}

KNO_FASTOP lispval _kno_fast_eval(lispval x,kno_lexenv env,
                                  struct KNO_STACK *stack,
                                  int tail)
{
  switch (KNO_PTR_MANIFEST_TYPE(x)) {
  case kno_oid_ptr_type: case kno_fixnum_ptr_type:
    return x;
  case kno_immediate_ptr_type:
    if (KNO_TYPEP(x,kno_lexref_type))
      return kno_lexref(x,env);
    else if (KNO_SYMBOLP(x)) {
      lispval val = kno_symeval(x,env);
      if (KNO_EXPECT_FALSE(KNO_VOIDP(val)))
        return kno_err(kno_UnboundIdentifier,"kno_eval",KNO_SYMBOL_NAME(x),x);
      else return val;}
    else return x;
  case kno_slotmap_type:
    return kno_deep_copy(x);
  case kno_cons_ptr_type: {
    kno_lisp_type type = KNO_CONSPTR_TYPE(x);
    switch (type) {
    case kno_pair_type: case kno_choice_type: case kno_prechoice_type:
    case kno_schemap_type:
      return kno_stack_eval(x,env,stack,tail);
    case kno_slotmap_type:
      return kno_deep_copy(x);
    default:
      return kno_incref(x);}}
  default: return x; /* Never reached */
  }
}

#define kno_eval(x,env) (_kno_fast_eval(x,env,kno_stackptr,0))

KNO_FASTOP lispval kno_get_arg(lispval expr,int i)
{
  while (KNO_PAIRP(expr))
    if ((KNO_PAIRP(KNO_CAR(expr))) &&
        (KNO_EQ(KNO_CAR(KNO_CAR(expr)),_kno_comment_symbol)))
      expr = KNO_CDR(expr);
    else if (i == 0) return KNO_CAR(expr);
    else {expr = KNO_CDR(expr); i--;}
  return KNO_VOID;
}
KNO_FASTOP lispval kno_get_body(lispval expr,int i)
{
  while (KNO_PAIRP(expr))
    if (i == 0) break;
    else if ((KNO_PAIRP(KNO_CAR(expr))) &&
             (KNO_EQ(KNO_CAR(KNO_CAR(expr)),_kno_comment_symbol)))
      expr = KNO_CDR(expr);
    else {expr = KNO_CDR(expr); i--;}
  return expr;
}
#else
KNO_EXPORT lispval _kno_symeval(lispval,kno_lexenv);
#define kno_eval(x,env) _kno_eval(x,env)
#define kno_symeval(x,env) _kno_symeval(x,env)
#define kno_lexref(x,env) _kno_lexref(x,env)
#define kno_get_arg(x,i) _kno_get_arg(x,i)
#define kno_get_body(x,i) _kno_get_body(x,i)
#endif

/* Body iteration */

/* Bindings iteration */

#define KNO_UNPACK_BINDING(pair,var,val)         \
  if (KNO_PAIRP(pair)) {                         \
    lispval _cdr = KNO_CDR(pair);                        \
    var = KNO_CAR(pair);                         \
    if (KNO_PAIRP(_cdr)) val = KNO_CAR(_cdr);     \
    else val = KNO_VOID;}                                \
  else {}

#define KNO_DOBINDINGS(var,val,bindings)                                 \
  U8_MAYBE_UNUSED lispval var, val, _scan = bindings, _binding = KNO_VOID;       \
  for (_scan = bindings,                                                        \
         _binding = KNO_PAIRP(_scan)?(KNO_CAR(_scan)):(KNO_VOID),          \
         var = KNO_PAIRP(_binding)?(KNO_CAR(_binding)):(KNO_VOID),         \
         val = ((KNO_PAIRP(_binding)&&(KNO_PAIRP(KNO_CDR(_binding))))?     \
              (KNO_CAR(KNO_CDR(_binding))):(KNO_VOID));                    \
       KNO_PAIRP(_scan);                                                 \
       _scan = KNO_CDR(_scan),                                           \
         _binding = KNO_PAIRP(_scan)?(KNO_CAR(_scan)):(KNO_VOID),          \
         var = KNO_PAIRP(_binding)?(KNO_CAR(_binding)):(KNO_VOID),         \
         val = ((KNO_PAIRP(_binding)&&(KNO_PAIRP(KNO_CDR(_binding))))?     \
              (KNO_CAR(KNO_CDR(_binding))):(KNO_VOID)) )

/* Simple continuations */

typedef struct KNO_CONTINUATION {
  KNO_FUNCTION_FIELDS; lispval retval;} KNO_CONTINUATION;
typedef struct KNO_CONTINUATION *kno_continuation;

/* Delays */

typedef struct KNO_PROMISE {
  KNO_CONS_HEADER;
  lispval promise_expr;
  kno_lexenv promise_env;
  u8_mutex promise_lock;
  int promise_broken;
  lispval promise_value;
  lispval promise_consumers;} KNO_PROMISE;
typedef struct KNO_PROMISE *kno_promise;

KNO_EXPORT lispval kno_force_promise(lispval promise);

/* Basic thread eval/apply functions */

KNO_EXPORT kno_thread kno_thread_call(lispval *,lispval,int,lispval *,int);
KNO_EXPORT kno_thread kno_thread_eval(lispval *,lispval,kno_lexenv,int);

/* Opcodes */

KNO_EXPORT lispval kno_opcode_dispatch
(lispval opcode,lispval expr,kno_lexenv env,
 struct KNO_STACK *,int tail);

/* Recording bugs */

KNO_EXPORT int kno_dump_bug(lispval ex,u8_string dir);
KNO_EXPORT int kno_record_bug(lispval ex);

KNO_EXPORT u8_string kno_bugdir;

#endif /* KNO_EVAL_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
