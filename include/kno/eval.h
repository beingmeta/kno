/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
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

#define KNO_NEED_EVALP(x) \
  ((KNO_SYMBOLP(x)) || (KNO_LEXREFP(x)) || (KNO_PAIRP(x)))
#define KNO_EVALP(x) \
  ( (KNO_SYMBOLP(x)) || (KNO_LEXREFP(x)) || (KNO_PAIRP(x)) || \
    ( (KNO_AMBIGP(x)) && (kno_choice_evalp(x)) ) )

#define KNO_IMMEDIATE_EVAL(expr,env)			\
  ( (KNO_LEXREFP(expr)) ? (kno_lexref(expr,env)) :	\
    (KNO_SYMBOLP(expr)) ? (kno_eval_symbol(expr,env)) : \
    (KNO_FCNIDP(expr)) ? (kno_fcnid_eval(expr,env)) : \
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

#define KNO_DEF_EVALFN(pname,cname,docstring) \
  struct KNO_EVALFN_INFO cname ## _info = { \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), \
    docstring, 0};
#define KNO_DEF_XEVALFN(pname,cname,flags,docstring)	\
  struct KNO_EVALFN_INFO cname ## _info = { \
    pname, # cname, _FILEINFO " L#" STRINGIFY(__LINE__), \
    docstring, flags};

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

KNO_EXPORT lispval kno_all_modules(void);

KNO_EXPORT int kno_module_finished(lispval module,int flags);
KNO_EXPORT int kno_finish_module(lispval module);
KNO_EXPORT int kno_finish_cmodule(lispval module);

KNO_EXPORT lispval kno_find_module(lispval,int);
KNO_EXPORT lispval kno_new_module(char *name,int flags);
KNO_EXPORT lispval kno_new_cmodule_x
(char *name,int flags,void *addr,u8_string filename);

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
  lispval lambda_optimizer, lambda_start;
  struct KNO_CONSBLOCK *lambda_consblock;
  kno_lexenv lambda_env;
  U8_MUTEX_DECL(lambda_lock);}
  KNO_LAMBDA;
typedef struct KNO_LAMBDA *kno_lambda;

KNO_EXPORT int kno_record_source;

#define KNO_SET_LAMBDA_SOURCE(lambda,src)                        \
  if (kno_record_source) {                               \
    struct KNO_LAMBDA *s=((struct KNO_LAMBDA *)lambda);   \
  s->lambda_source=src; kno_incref(s->lambda_source);}   \
  else {}

KNO_EXPORT lispval kno_apply_lambda(kno_stack,struct KNO_LAMBDA *fn,
                                    int n,kno_argvec args);
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

#define KNO_TAIL_EVAL 0x01
#define KNO_VOID_VAL  0x02

KNO_EXPORT lispval kno_eval_body(lispval body,kno_lexenv env,kno_stack stack,
				 u8_context cxt,u8_string label,
				 int tail);
KNO_EXPORT lispval kno_eval_expr(lispval head,lispval expr,
				 kno_lexenv env,kno_stack stack,
				 int tail);
KNO_EXPORT lispval kno_eval(lispval expr,kno_lexenv env,
			    kno_stack stack,
			    int tail);
KNO_EXPORT lispval kno_eval_arg(lispval expr,kno_lexenv env);
KNO_EXPORT lispval kno_stack_eval(lispval expr,kno_lexenv env,kno_stack stack);


KNO_EXPORT lispval kno_get_arg(lispval expr,int i);
KNO_EXPORT lispval kno_get_body(lispval expr,int i);
KNO_EXPORT lispval kno_symeval(lispval sym,kno_lexenv env);
KNO_EXPORT lispval kno_lexref(lispval lexref,kno_lexenv env);
KNO_EXPORT lispval kno_eval_symbol(lispval sym,kno_lexenv env);
KNO_EXPORT lispval kno_lexref(lispval lexref,kno_lexenv env);

KNO_EXPORT lispval kno_fcn_ref(lispval sym,lispval from,lispval val);

#define kno_simplify_value(v) \
  ( (KNO_PRECHOICEP(v)) ? (kno_simplify_choice(v)) : (v) )

#define kno_eval_return(v) return (kno_pop_stack(_stack),(v))
#define kno_eval_return_from(stack,v) return (kno_pop_stack(stack),(v))

#if KNO_SOURCE
#define _eval_return return kno_pop_stack(_stack),
#endif

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
  int *fcn_typeinfo;
  lispval *fcn_defaults;
  /* This is the one we use */
  lispval retval;} KNO_CONTINUATION;
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

/* Aliases in KNO_SOURCE */

#if KNO_SOURCE
#define DEF_EVALFN  KNO_DEF_EVALFN
#define DEF_XEVALFN KNO_DEF_EVALFN
#define LINK_EVALFN KNO_LINK_EVALFN
#define EVALFNP     KNO_EVALFNP
#endif

#endif /* KNO_EVAL_H */

