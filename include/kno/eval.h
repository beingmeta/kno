/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_EVAL_H
#define KNO_EVAL_H 1
#ifndef KNO_EVAL_H_INFO
#define KNO_EVAL_H_INFO "include/kno/eval.h"
#endif

/* Important DEFINES */

/* If KNO_FAST_EVAL is true, inline versions (starting with __) of
   many evaluator functions are defined. */
/* If KNO_INLINE_EVAL is true, those inline versions are made (via macros)
   the default versions of those operations. */

#include "kno/lexenv.h"
#include "kno/apply.h"
#include "kno/threads.h"

#include <libu8/u8printf.h>
#include <assert.h>

KNO_EXPORT u8_condition kno_UnboundIdentifier, kno_BindError;
KNO_EXPORT u8_condition kno_VoidBinding;
KNO_EXPORT u8_condition kno_VoidBoolean, knoVoid_Result, kno_VoidSortKey;
KNO_EXPORT u8_condition kno_NoSuchModule, kno_BindSyntaxError;
KNO_EXPORT u8_condition kno_TooFewExpressions, kno_NotAnIdentifier;
KNO_EXPORT u8_condition kno_InvalidMacro, KNO_BadArglist;
KNO_EXPORT u8_condition kno_ReadOnlyEnv;
KNO_EXPORT u8_condition kno_BadOpcode, kno_BadEvalOp, kno_OpcodeSyntaxError;
KNO_EXPORT u8_condition kno_TailArgument;
KNO_EXPORT u8_condition kno_BadArgument;

KNO_EXPORT lispval kno_scheme_module;
KNO_EXPORT lispval kno_textio_module;
KNO_EXPORT lispval kno_binio_module;
KNO_EXPORT lispval kno_sys_module;
KNO_EXPORT lispval kno_db_module;

KNO_EXPORT lispval _kno_comment_symbol;

KNO_EXPORT int kno_scheme_initialized;
KNO_EXPORT int kno_load_scheme(void) KNO_LIBINIT0_FN;
KNO_EXPORT int kno_init_scheme(void);

KNO_EXPORT u8_string kno_default_libscm;

KNO_EXPORT int (*kno_dump_exception)(lispval bt);

KNO_EXPORT int kno_choice_evalp(lispval x);

#define KNO_LAMBDXP(x) \
  ( (KNO_TYPEP(x,kno_lambda_type)) && \
    ((((kno_lambda)(x))->lambda_env)==NULL) )

#define KNO_NEED_EVALP(x) \
  ((KNO_SYMBOLP(x)) || (KNO_LEXREFP(x)) || (KNO_PAIRP(x)) || (KNO_SCHEMAPP(x)))
#define KNO_EVALP(x) \
  ( (KNO_SYMBOLP(x)) || (KNO_LEXREFP(x)) || (KNO_PAIRP(x)) || \
    (KNO_SCHEMAPP(x)) || ( (KNO_AMBIGP(x)) && (kno_choice_evalp(x)) ) )

#define KNO_IMMEDIATE_EVAL(expr,env)			\
  ( (KNO_LEXREFP(expr)) ? (kno_lexref(expr,env)) :	\
    (KNO_SYMBOLP(expr)) ? (kno_eval_symbol(expr,env)) : \
    (KNO_QONSTP(expr)) ? (kno_qonst_eval(expr,env)) : \
    (expr))

KNO_EXPORT u8_string kno_evalstack_type, kno_ndevalstack_type;
KNO_EXPORT u8_string kno_lambda_stack_type;

#define KNO_MODULE_OPTIONAL 0
#define KNO_MODULE_DEFAULT 1

KNO_EXPORT lispval kno_bad_arg(lispval arg,u8_context cxt,lispval source_expr);
#define KNO_BAD_ARGP(x) ( ( (x) == KNO_VOID) || ( ( (x) == KNO_TAIL) ) )

/* Eval stacks */

#define KNO_SETUP_EVAL(name,label,expr,env,caller)		\
  struct KNO_STACK _ ## name = { 0 }, *name = &_ ## name;	\
  KNO_SETUP_STACK(&_ ## name,label);				\
  KNO_STACK_SET_CALLER(&_ ## name,((kno_stack)caller));		\
  if (caller) {							\
    _ ## name.eval_context = caller->eval_context;		\
    _ ## name.stack_file = caller->stack_file;}			\
  _ ## name.stack_op = expr;					\
  _ ## name.eval_env = env

#define KNO_SETUP_EVAL_NREFS(name,label,expr,env,caller,n_refs)	\
  KNO_SETUP_EVAL(name,label,expr,env,caller);			\
  lispval _ ## name ## _refs[n_refs];				\
  _ ## name.stack_refs.elts  = _ ## name ## _refs;		\
  _ ## name.stack_refs.len = n_refs;				\
  _ ## name.stack_refs.count = 0;				\
  _ ## name.eval_env = env

#define KNO_START_EVAL(name,label,expr,env,caller)		\
  KNO_SETUP_EVAL_NREFS(name,label,expr,env,caller,3);		\
  KNO_PUSH_STACK(((kno_stack)(& _ ## name)))

#define KNO_START_EVALX(name,label,expr,env,caller,n_refs)	\
  KNO_SETUP_EVAL_NREFS(name,label,expr,env,caller,n_refs);	\
  KNO_PUSH_STACK(((kno_stack)(& _ ## name)))

#define KNO_EVAL_ROOT(name,label,expr)				\
  struct KNO_STACK _ ## name = { 0 }, *name = &_ ## name;	\
  KNO_SETUP_STACK(&_ ## name,label);				\
  _ ## name.stack_op = expr;					\
  _ ## name.eval_source = expr;					\
  _ ## name.eval_context = expr;				\
  set_call_stack(_ ##name)

#define KNO_NEW_EVAL(label,expr,env,caller)		\
  KNO_START_EVAL(_stack,label,expr,env,caller);		\
  _stack->eval_context =_stack->eval_source = expr

#define KNO_PUSH_EVAL(name,label,expr,env)			\
  KNO_START_EVAL(name,label,expr,env,((kno_stack)_stack))

#define KNO_STACK_TAILP(stack) (KNO_STACK_BITP((stack),KNO_STACK_TAIL_POS))

#define KNO_STACK_SET_TAIL(stack,on) \
  KNO_STACK_SET_BIT((stack),KNO_STACK_TAIL_POS,(on))

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
  u8_string evalfn_name, evalfn_cname, evalfn_filename;
  u8_string evalfn_documentation;
  lispval evalfn_moduleid;
  unsigned char evalfn_notail;
  kno_eval_handler evalfn_handler;} KNO_EVALFN;
typedef struct KNO_EVALFN *kno_evalfn;

#define KNO_EVALFN_NOTAIL  0x01

KNO_EXPORT lispval kno_make_evalfn(u8_string name,int flags,kno_eval_handler fn);
KNO_EXPORT lispval kno_new_evalfn(lispval mod,u8_string name,u8_string cname,
				  u8_string filename,
				  u8_string doc,
				  int flags,
				  kno_eval_handler fn);

#define kno_def_evalfn(mod,name,evalfn,doc)                             \
  kno_new_evalfn(mod,name,# evalfn,                                     \
		 _FILEINFO " L#" STRINGIFY(__LINE__),doc,0,evalfn)
#define kno_def_xevalfn(mod,name,evalfn,flags,doc)			\
  kno_new_evalfn(mod,name,# evalfn,                                     \
		 _FILEINFO " L#" STRINGIFY(__LINE__),doc,flags,evalfn)

typedef struct KNO_EVALFN_INFO {
  u8_string pname, cname, filename, docstring;
  int flags;} KNO_EVALFN_INFO;
typedef struct KNO_EVALFN_INFO *kno_evalfn_info;

#define KNO_DEFC_EVALFN(pname,cname,flags,docstring)	\
  struct KNO_EVALFN_INFO cname ## _info = { \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), \
    docstring, flags};

#define KNO_EVALFN_DEFAULTS (0)

#define KNO_LINK_EVALFN(module,cname) \
  kno_new_evalfn(module,cname ## _info.pname,# cname,cname ## _info.filename, \
		 cname ## _info.docstring,cname ## _info.flags,cname);

#define KNO_EVALFNP(x) (KNO_TYPEP((x),kno_evalfn_type))

/* Macros */

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

/* Modules */

KNO_EXPORT kno_lexenv kno_new_lexenv(lispval bindings);
KNO_EXPORT kno_lexenv kno_working_lexenv(void);
KNO_EXPORT kno_lexenv kno_make_env(lispval module,kno_lexenv parent);
KNO_EXPORT kno_lexenv kno_make_export_env(lispval exports,kno_lexenv parent);
KNO_EXPORT lispval kno_register_module_x(lispval name,lispval module,int flags);
KNO_EXPORT lispval kno_register_module(u8_string name,lispval module,int flags);
KNO_EXPORT lispval kno_get_module(lispval name);
KNO_EXPORT int kno_discard_module(lispval name);

KNO_EXPORT void kno_run_loadmod_hooks(lispval module);

KNO_EXPORT int kno_load_module(u8_string modname);

KNO_EXPORT lispval kno_all_modules(void);

KNO_EXPORT int kno_module_finished(lispval module,int flags);
KNO_EXPORT int kno_finish_module(lispval module);
KNO_EXPORT int kno_finish_cmodule(lispval module);

KNO_EXPORT lispval kno_find_module(lispval,int);
KNO_EXPORT lispval kno_new_module(char *name,int flags);
KNO_EXPORT lispval kno_new_cmodule_x
(char *name,int flags,void *addr,u8_string filename);

KNO_EXPORT int kno_add_default_module(lispval module);

KNO_EXPORT lispval kno_get_moduleid(lispval x,int err);

#define kno_new_cmodule(name,flags,addr) \
  kno_new_cmodule_x(name,flags,addr,__FILE__)

KNO_EXPORT kno_hashtable kno_get_exports(kno_lexenv);

KNO_EXPORT lispval kno_use_module(kno_lexenv env,lispval module);

KNO_EXPORT int kno_log_reloads;

KNO_EXPORT void kno_add_module_loader(int (*loader)(lispval,void *),void *);

/* LAMBDAs */

typedef struct KNO_LAMBDA {
  KNO_FUNCTION_FIELDS;
  unsigned short lambda_n_vars;
  unsigned char lambda_n_locals;
  unsigned char lambda_synchronized;
  lispval *lambda_vars, *lambda_inits;
  lispval lambda_arglist, lambda_body, lambda_source;
  lispval lambda_entry;
  struct KNO_LAMBDA *lambda_template;
  struct KNO_CONSBLOCK *lambda_consblock;
  kno_lexenv lambda_env;
  U8_MUTEX_DECL(lambda_lock);}
  KNO_LAMBDA;
typedef struct KNO_LAMBDA *kno_lambda;

KNO_EXPORT int kno_enclose_lambdas;
KNO_EXPORT int kno_record_source;
KNO_EXPORT int kno_tail_max;

KNO_EXPORT kno_lambda _KNO_LAMBDA_INFO(lispval x);
KNO_EXPORT kno_lexenv _KNO_LAMBDA_ENV(lispval x);

#if KNO_INLINE_EVAL
KNO_FASTOP kno_lambda KNO_LAMBDA_INFO(lispval x)
{
  if (KNO_QONSTP(x)) x = kno_qonst_val(x);
  if (KNO_TYPEP(x,kno_lambda_type))
    return (kno_lambda)x;
  else if ( (KNO_TYPEP(x,kno_closure_type)) &&
	    (KNO_TYPEP((((kno_pair)x)->car),kno_lambda_type)) )
    return (kno_lambda) (((kno_pair)x)->car);
  else return NULL;
}
KNO_FASTOP kno_lexenv KNO_LAMBDA_ENV(lispval x)
{
  if (KNO_QONSTP(x)) x = kno_qonst_val(x);
  if (KNO_TYPEP(x,kno_lambda_type))
    return ((kno_lambda)x)->lambda_env;
  else if ( (KNO_TYPEP(x,kno_closure_type)) &&
	    (KNO_TYPEP((((kno_pair)x)->car),kno_lambda_type)) )
    return (kno_lexenv) (((kno_pair)x)->cdr);
  else return NULL;
}
#else
#define KNO_LAMBDA_INFO _KNO_LAMBDA_INFO
#define KNO_LAMBDA_ENV _KNO_LAMBDA_ENV
#endif

#define KNO_SET_LAMBDA_SOURCE(lambda,src)                        \
  if (kno_record_source) {                               \
    struct KNO_LAMBDA *s=((struct KNO_LAMBDA *)lambda);   \
  s->lambda_source=src; kno_incref(s->lambda_source);}   \
  else {}

KNO_EXPORT lispval kno_apply_lambda(kno_stack,struct KNO_LAMBDA *fn,
                                    int n,kno_argvec args);
KNO_EXPORT lispval kno_xapply_lambda(lispval fn,void *data,lispval (*getval)(void *,lispval));

KNO_EXPORT lispval kno_make_lambda(u8_string name,
                                 lispval arglist,lispval body,kno_lexenv env,
                                 int nd,int sync);
KNO_EXPORT int kno_set_lambda_schema
(struct KNO_LAMBDA *s,int n,lispval *args,lispval *inits,lispval *types);

/* QCODE */

typedef struct KNO_QCODE {
  const lispval *codes;
  const int code_len;} *kno_qcode;

/* Loading files and config data */

typedef lispval (*kno_readfn)(void *source);

KNO_EXPORT lispval kno_load_loop
(u8_string sourcebase,
 kno_readfn readexpr,void *source,
 kno_lexenv env,kno_stack load_stack);

KNO_EXPORT lispval kno_load_fasl(u8_string sourceid,kno_lexenv env);
KNO_EXPORT lispval kno_load_stream
  (u8_input loadstream,kno_lexenv env,u8_string sourcebase);
KNO_EXPORT lispval kno_load_source
  (u8_string sourceid,kno_lexenv env,u8_string enc_name);
KNO_EXPORT lispval kno_load_source_with_date
  (u8_string sourceid,kno_lexenv env,u8_string enc_name,time_t *modtime);

/* The Evaluator */

#define KNO_TAIL_EVAL 0x01
#define KNO_VOID_VAL  0x02

#define KNO_PRUNED       0x00
#define KNO_GOOD_ARGS    0x01
#define KNO_FAILED_ARGS  0x02
#define KNO_CONSED_ARGS  0x04
#define KNO_AMBIG_ARGS   0x08
#define KNO_QCHOICE_ARGS 0x10
#define KNO_THROWN_ARG   0x20

KNO_EXPORT lispval kno_eval_body(lispval body,kno_lexenv env,kno_stack stack,
				 u8_context cxt,u8_string label,
				 int tail);
KNO_EXPORT lispval kno_eval_expr(lispval head,lispval expr,
				 kno_lexenv env,kno_stack stack,
				 int tail);
KNO_EXPORT lispval kno_eval(lispval,kno_lexenv,kno_stack);
KNO_EXPORT lispval kno_tail_eval(lispval,kno_lexenv,kno_stack);
KNO_EXPORT lispval kno_eval_arg(lispval,kno_lexenv,kno_stack);


KNO_EXPORT lispval kno_get_arg(lispval expr,int i);
KNO_EXPORT lispval kno_get_body(lispval expr,int i);
KNO_EXPORT lispval kno_symeval(lispval sym,kno_lexenv env);
KNO_EXPORT lispval kno_lexref(lispval lexref,kno_lexenv env);
KNO_EXPORT lispval kno_eval_symbol(lispval sym,kno_lexenv env);
KNO_EXPORT lispval kno_lexref_name(lispval lexref,kno_lexenv env);

KNO_EXPORT lispval kno_qonst_ref(lispval sym,lispval from,lispval val);

#define kno_simplify_value(v) \
  ( (KNO_PRECHOICEP(v)) ? (kno_simplify_choice(v)) : (v) )

#define kno_eval_return(v) return (kno_pop_stack(_stack),(v))
#define kno_eval_return_from(stack,v) return (kno_pop_stack(stack),(v))

#if KNO_SOURCE
#define _eval_return return kno_pop_stack(_stack),
#endif

KNO_EXPORT lispval kno_debug_wait(lispval obj,lispval msg,int global);

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
  KNO_FUNCTION_FIELDS;
  /* We have these because the cons type for a continuation
     is a cprim and we have to run through that apply loop. */
  lispval *fcn_defaults;
  /* This is the one we use */
  lispval retval;} KNO_CONTINUATION;
typedef struct KNO_CONTINUATION *kno_continuation;

/* Delays */

enum KNO_PROMISE_TYPEVAL
  { kno_bad_promise=0, kno_eval_promise=1, kno_apply_promise=2, kno_async_promise=3 };

typedef struct KNO_PROMISE {
  KNO_ANNOTATED_HEADER;
  int promise_bits;
  lispval promise_value;
  lispval promise_callbacks;
  u8_mutex promise_lock;
  u8_condvar promise_condition;
  time_t promise_updated;
  union {
    struct {
      lispval expr;
      kno_lexenv env;} eval;
    struct {
      lispval fcn;
      lispval *args;
      int n_args;} apply;
    struct {
      lispval source;
      lispval task;
      lispval fallback;} async;}
    promise_body;} KNO_PROMISE;
typedef struct KNO_PROMISE *kno_promise;

#define KNO_PROMISE_TYPEMASK   0x0003

#define KNO_PROMISE_TYPEOF(p) \
  ((enum KNO_PROMISE_TYPEVAL)(((p)->promise_bits)&(KNO_PROMISE_TYPEMASK)))
#define KNO_PROMISE_TYPEP(p,pt)						\
  (((enum KNO_PROMISE_TYPEVAL)(((p)->promise_bits)&(KNO_PROMISE_TYPEMASK)))==(pt))

#define KNO_PROMISE_FINAL      0x0004
#define KNO_PROMISE_BROKEN     0x0008
#define KNO_PROMISE_PARTIAL    0x0010
/* 'Ignores' errors */
#define KNO_PROMISE_ROBUST     0x0020

#define KNO_PROMISE_RESOLVEDP(p) (((p)->promise_value)!=(KNO_NULL))

#define KNO_PROMISE_FINALP(p)    (((p)->promise_bits)&(KNO_PROMISE_FINAL))
#define KNO_PROMISE_BROKENP(p)   (((p)->promise_bits)&(KNO_PROMISE_BROKEN))
#define KNO_PROMISE_PARTIALP(p)  (((p)->promise_bits)&(KNO_PROMISE_PARTIAL))
#define KNO_PROMISE_ROBUSTP(p)   (((p)->promise_bits)&(KNO_PROMISE_ROBUST))

#define KNO_PROMISE_SET(p,flag) (p)->promise_bits |= (flag)
#define KNO_PROMISE_CLEAR(p,flag) (p)->promise_bits &= (~(flag))

KNO_EXPORT lispval kno_force_promise(lispval promise);
KNO_EXPORT struct KNO_PROMISE *kno_init_promise
(struct KNO_PROMISE *promise,enum KNO_PROMISE_TYPEVAL type,int flags);
KNO_EXPORT int kno_promise_resolve(kno_promise p,lispval value,int flags);

/* Basic thread eval/apply functions */

KNO_EXPORT kno_thread kno_thread_call(lispval *,lispval,int,lispval *,lispval,int);
KNO_EXPORT kno_thread kno_thread_eval(lispval *,lispval,kno_lexenv,lispval,int);

/* DT servers */

typedef struct KNO_DTSERVER {
  KNO_CONS_HEADER;
  u8_string dtserverid, dtserver_addr;
  /*
  enum { dtype_protocol=0, xtype_protocol=1 } service_protocol 
  struct XTYPE_REFS refs;
  */
  struct U8_CONNPOOL *connpool;} KNO_DTSERVER;
typedef struct KNO_DTSERVER *kno_stream_erver;

/* Recording bugs */

KNO_EXPORT int kno_dump_bug(lispval ex,u8_string dir);
KNO_EXPORT int kno_record_bug(lispval ex);

KNO_EXPORT u8_string kno_bugdir;

/* Declaring watched evalfns */

#define KNO_DECL_WRAP(name)			\
  KNO_EXPORT int _kno_in_ ## name;		\
  KNO_EXPORT lispval _kno_ ## name		\
  (lispval expr,kno_lexenv env,kno_stack s)

KNO_DECL_WRAP(wrap1);
KNO_DECL_WRAP(wrap2);
KNO_DECL_WRAP(wrap3);
KNO_DECL_WRAP(wrap4);
KNO_DECL_WRAP(wrap5);
KNO_DECL_WRAP(wrap6);
KNO_DECL_WRAP(wrap7);

/* Aliases in KNO_SOURCE */

#if KNO_SOURCE
#define DEFC_EVALFN KNO_DEFC_EVALFN
#define EVALFN_DEFAULTS KNO_EVALFN_DEFAULTS
#define LINK_EVALFN KNO_LINK_EVALFN
#define EVALFNP     KNO_EVALFNP
#endif

#endif /* KNO_EVAL_H */

