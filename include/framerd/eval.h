/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_EVAL_H
#define FRAMERD_EVAL_H 1
#ifndef FRAMERD_EVAL_H_INFO
#define FRAMERD_EVAL_H_INFO "include/framerd/eval.h"
#endif

#include "framerd/apply.h"
#include <assert.h>

FD_EXPORT fd_exception fd_UnboundIdentifier, fd_BindError, fd_VoidArgument;
FD_EXPORT fd_exception fd_NoSuchModule, fd_SyntaxError, fd_BindSyntaxError;
FD_EXPORT fd_exception fd_TooFewExpressions, fd_NotAnIdentifier;
FD_EXPORT fd_exception fd_InvalidMacro, FD_BadArglist;
FD_EXPORT fd_exception fd_ReadOnlyEnv;

FD_EXPORT fdtype fd_scheme_module, fd_xscheme_module;

FD_EXPORT fd_ptr_type fd_environment_type, fd_specform_type;
FD_EXPORT fd_ptr_type fd_macro_type;

FD_EXPORT fdtype _fd_comment_symbol;

FD_EXPORT int fd_load_fdscheme(void) FD_LIBINIT0_FN;
FD_EXPORT int fd_init_fdscheme(void);
FD_EXPORT void fd_init_schemeio(void) FD_LIBINIT0_FN;

FD_EXPORT u8_context fd_eval_context;

FD_EXPORT void (*fd_dump_backtrace)(u8_string bt);

#define FD_NEED_EVALP(x) ((FD_SYMBOLP(x)) || (FD_LEXREFP(x)) || \
                          (FD_PAIRP(x)) || (FD_RAILP(x)))

/* Constants */

#define FD_STACK_ARGS 6

/* Environments */

#define FD_MODULE_SAFE 1
#define FD_MODULE_DEFAULT 2

typedef struct FD_ENVIRONMENT {
  FD_CONS_HEADER;
  fdtype env_bindings;
  fdtype env_exports;
  struct FD_ENVIRONMENT *env_parent;
  struct FD_ENVIRONMENT *env_copy;} FD_ENVIRONMENT;
typedef struct FD_ENVIRONMENT *fd_environment;
typedef struct FD_ENVIRONMENT *fd_lispenv;

FD_EXPORT int fd_set_value(fdtype,fdtype,fd_lispenv);
FD_EXPORT int fd_add_value(fdtype,fdtype,fd_lispenv);
FD_EXPORT int fd_bind_value(fdtype,fdtype,fd_lispenv);

#define FD_XENV(x) \
  (fd_consptr(struct FD_ENVIRONMENT *,x,fd_environment_type))
#define FD_XENVIRONMENT(x) \
  (fd_consptr(struct FD_ENVIRONMENT *,x,fd_environment_type))
#define FD_ENVIRONMENTP(x) (FD_TYPEP(x,fd_environment_type))

FD_EXPORT int fd_recycle_environment(fd_lispenv env);

/* Special forms */

typedef fdtype (*fd_evalfn)(fdtype expr,struct FD_ENVIRONMENT *);

typedef struct FD_SPECIAL_FORM {
  FD_CONS_HEADER;
  u8_string fexpr_name, fexpr_filename;
  fd_evalfn fexpr_handler;} FD_SPECIAL_FORM;
typedef struct FD_SPECIAL_FORM *fd_special_form;

FD_EXPORT fdtype fd_make_special_form(u8_string name,fd_evalfn fn);
FD_EXPORT void fd_defspecial(fdtype mod,u8_string name,fd_evalfn fn);

typedef struct FD_MACRO {
  FD_CONS_HEADER;
  u8_string fd_macro_name;
  fdtype fd_macro_transformer;} FD_MACRO;
typedef struct FD_MACRO *fd_macro;

/* These should probably get their own header file */

FD_EXPORT fdtype fd_printout(fdtype,fd_lispenv);
FD_EXPORT fdtype fd_printout_to(U8_OUTPUT *,fdtype,fd_lispenv);

/* DT servers */

FD_EXPORT fd_ptr_type fd_stream_erver_type;

typedef struct FD_STREAM_ERVER {
  FD_CONS_HEADER;
  u8_string fd_serverid, fd_server_address;
  struct U8_CONNPOOL *fd_connpool;} FD_STREAM_ERVER;
typedef struct FD_STREAM_ERVER *fd_stream_erver;

/* Modules */

FD_EXPORT fd_lispenv fd_new_environment(fdtype bindings,int safe);
FD_EXPORT fd_lispenv fd_working_environment(void);
FD_EXPORT fd_lispenv fd_safe_working_environment(void);
FD_EXPORT fd_lispenv fd_make_env(fdtype module,fd_lispenv parent);
FD_EXPORT fd_lispenv fd_make_export_env(fdtype exports,fd_lispenv parent);
FD_EXPORT fdtype fd_register_module_x(fdtype name,fdtype module,int flags);
FD_EXPORT fdtype fd_register_module(u8_string name,fdtype module,int flags);
FD_EXPORT fdtype fd_get_module(fdtype name,int safe);
FD_EXPORT int fd_discard_module(fdtype name,int safe);

FD_EXPORT int fd_module_finished(fdtype module,int flags);
FD_EXPORT int fd_finish_module(fdtype module);
FD_EXPORT int fd_static_module(fdtype module);
FD_EXPORT int fd_lock_exports(fdtype module);


FD_EXPORT fdtype fd_make_special_form(u8_string name,fd_evalfn fn);
FD_EXPORT void fd_defspecial(fdtype mod,u8_string name,fd_evalfn fn);

FD_EXPORT fdtype fd_find_module(fdtype,int,int);
FD_EXPORT fdtype fd_new_module(char *name,int flags);

FD_EXPORT fdtype fd_use_module(fd_lispenv env,fdtype module);


FD_EXPORT void fd_add_module_loader(int (*loader)(fdtype,int,void *),void *);

#define FD_LOCK_EXPORTS 0x01
#define FD_FIX_EXPORTS 0x02
#define FD_STATIC_EXPORTS 0x04
#define FD_LOCK_MODULES 0x08
#define FD_FIX_MODULES 0x10
#define FD_STATIC_MODULES 0x20
#define FD_OPTIMIZE_EXPORTS 0x03

/* SPROCs */

typedef struct FD_SPROC {
  FD_FUNCTION_FIELDS;
  short sproc_n_vars, sproc_synchronized;
  fdtype *sproc_vars, sproc_arglist, sproc_body;
  fd_lispenv sproc_env;
  U8_MUTEX_DECL(sproc_lock);
} FD_SPROC;
typedef struct FD_SPROC *fd_sproc;

FD_EXPORT fdtype fd_apply_sproc(struct FD_SPROC *fn,int n,fdtype *args);
FD_EXPORT fdtype fd_xapply_sproc
  (struct FD_SPROC *fn,void *data,fdtype (*getval)(void *,fdtype));

FD_EXPORT fdtype fd_make_sproc(u8_string name,
                               fdtype arglist,fdtype body,fd_lispenv env,
                               int nd,int sync);

/* Loading files and config data */

typedef struct FD_SOURCEFN {
  u8_string (*fd_getsource)(int op,u8_string,u8_string,u8_string *,time_t *timep,void *);
  void *fd_getsource_data;
  struct FD_SOURCEFN *fd_next_sourcefn;} FD_SOURCEFN;
typedef struct FD_SOURCEFN *fd_sourcefn;

FD_EXPORT u8_string fd_get_source(u8_string,u8_string,u8_string *,time_t *);
FD_EXPORT int fd_probe_source(u8_string,u8_string *,time_t *);
FD_EXPORT void fd_register_sourcefn
  (u8_string (*fn)(int op,u8_string,u8_string,u8_string *,time_t *,void *),
   void *sourcefn_data);

FD_EXPORT int fd_load_config(u8_string sourceid);
FD_EXPORT int fd_load_default_config(u8_string sourceid);
FD_EXPORT fdtype fd_load_source_with_date
  (u8_string sourceid,fd_lispenv env,u8_string enc_name,time_t *modtime);
FD_EXPORT fdtype fd_load_source
  (u8_string sourceid,fd_lispenv env,u8_string enc_name);
FD_EXPORT u8_string fd_sourcebase();
FD_EXPORT u8_string fd_get_component(u8_string spec);
FD_EXPORT u8_string fd_bind_sourcebase(u8_string sourcebase);
FD_EXPORT void fd_restore_sourcebase(u8_string sourcebase);

typedef struct FD_CONFIG_RECORD {
  u8_string fd_config_source;
  struct FD_CONFIG_RECORD *fd_config_next;} FD_CONFIG_RECORD;

/* The Evaluator */

/* This is the non-static version of fd_eval */
FD_EXPORT fdtype _fd_eval(fdtype expr,fd_lispenv env);
FD_EXPORT fdtype fd_tail_eval(fdtype expr,fd_lispenv env);
FD_EXPORT fdtype fd_eval_exprs(fdtype exprs,fd_lispenv env);
FD_EXPORT fdtype _fd_get_arg(fdtype expr,int i);
FD_EXPORT fdtype _fd_get_body(fdtype expr,int i);

FD_EXPORT fd_lispenv fd_copy_env(fd_lispenv env);

#if FD_PROVIDE_FASTEVAL
FD_FASTOP fdtype fastget(fdtype table,fdtype key)
{
  fd_ptr_type argtype=FD_PTR_TYPE(table);
  switch (argtype) {
  case fd_schemap_type:
    return fd_schemap_get((fd_schemap)table,key,FD_UNBOUND);
  case fd_slotmap_type:
    return fd_slotmap_get((fd_slotmap)table,key,FD_UNBOUND);
  case fd_hashtable_type:
    return fd_hashtable_get((fd_hashtable)table,key,FD_UNBOUND);
  default: return fd_get(table,key,FD_UNBOUND);}
}
FD_FASTOP fdtype fd_lexref(fdtype lexref,fd_lispenv env)
{
  int code=FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up=code/32, across=code%32;
  while ((env) && (up)) {
    if (env->env_copy) env=env->env_copy;
    env=env->env_parent; up--;}
  if (env->env_copy) env=env->env_copy;
  if (FD_EXPECT_TRUE(env!=NULL)) {
    fdtype bindings=env->env_bindings;
    if (FD_EXPECT_TRUE(FD_SCHEMAPP(bindings))) {
      struct FD_SCHEMAP *s=(struct FD_SCHEMAP *)bindings;
      return fd_incref(s->schema_values[across]);}}
  return fd_err("Bad lexical reference","fd_lexref",NULL,FD_VOID);
}
FD_FASTOP fdtype fd_symeval(fdtype symbol,fd_lispenv env)
{
  if (env==NULL) return FD_VOID;
  if (env->env_copy) env=env->env_copy;
  while (env) {
    fdtype val=fastget(env->env_bindings,symbol);
    if (val==FD_UNBOUND)
      env=env->env_parent;
    else return val;
    if ((env) && (env->env_copy)) env=env->env_copy;}
  return FD_VOID;
}

FD_FASTOP fdtype fd_eval(fdtype x,fd_lispenv env)
{
  fdtype result=fd_tail_eval(x,env);
  if (FD_TYPEP(result,fd_tailcall_type))
    return _fd_finish_call(result);
  else return result;
}

FD_FASTOP fdtype fasteval(fdtype x,fd_lispenv env)
{
  switch (FD_PTR_MANIFEST_TYPE(x)) {
  case fd_oid_ptr_type: case fd_fixnum_ptr_type:
    return x;
  case fd_immediate_ptr_type:
    if (FD_TYPEP(x,fd_lexref_type))
      return fd_lexref(x,env);
    else if (FD_SYMBOLP(x)) {
      fdtype val=fd_symeval(x,env);
      if (FD_EXPECT_FALSE(FD_VOIDP(val)))
        return fd_err(fd_UnboundIdentifier,"fd_eval",FD_SYMBOL_NAME(x),x);
      else return val;}
    else return x;
  case fd_slotmap_type:
    return fd_deep_copy(x);
  case fd_cons_ptr_type:
    if ((FD_TYPEP(x,fd_pair_type)) ||
        (FD_TYPEP(x,fd_rail_type)) ||
        (FD_TYPEP(x,fd_choice_type)) ||
        (FD_TYPEP(x,fd_achoice_type)))
      return fd_eval(x,env);
    else return fd_incref(x);
  default: /* Never reached */
    return x;
  }
}

FD_FASTOP fdtype fast_tail_eval(fdtype x,fd_lispenv env)
{
  switch (FD_PTR_MANIFEST_TYPE(x)) {
  case fd_oid_ptr_type: case fd_fixnum_ptr_type:
    return x;
  case fd_immediate_ptr_type:
    if (FD_TYPEP(x,fd_lexref_type))
      return fd_lexref(x,env);
    else if (FD_SYMBOLP(x)) {
      fdtype val=fd_symeval(x,env);
      if (FD_EXPECT_FALSE(FD_VOIDP(val)))
        return fd_err(fd_UnboundIdentifier,"fd_eval",FD_SYMBOL_NAME(x),x);
      else return val;}
    else return x;
  case fd_cons_ptr_type: {
    fd_ptr_type ctype=FD_PTR_TYPE(x);
    switch (ctype) {
    case fd_pair_type: case fd_rail_type:
      return fd_tail_eval(x,env);
    case fd_slotmap_type:
      return fd_deep_copy(x);
    case fd_choice_type: case fd_achoice_type:
      return fd_eval(x,env);
    default:
      return fd_incref(x);}}
  default: /* Never reached */
    return x;
  }
}

FD_FASTOP fdtype fd_get_arg(fdtype expr,int i)
{
  while (FD_PAIRP(expr))
    if ((FD_PAIRP(FD_CAR(expr))) &&
        (FD_EQ(FD_CAR(FD_CAR(expr)),_fd_comment_symbol)))
      expr=FD_CDR(expr);
    else if (i == 0) return FD_CAR(expr);
    else {expr=FD_CDR(expr); i--;}
  return FD_VOID;
}
FD_FASTOP fdtype fd_get_body(fdtype expr,int i)
{
  while (FD_PAIRP(expr))
    if (i == 0) break;
    else if ((FD_PAIRP(FD_CAR(expr))) &&
             (FD_EQ(FD_CAR(FD_CAR(expr)),_fd_comment_symbol)))
      expr=FD_CDR(expr);
    else {expr=FD_CDR(expr); i--;}
  return expr;
}
#else
FD_EXPORT fdtype _fd_symeval(fdtype,fd_lispenv);
#define fd_eval(x,env) _fd_eval(x,env)
#define fd_symeval(x,env) _fd_symeval(x,env)
#define fd_lexref(x,env) _fd_lexref(x,env)
#define fd_get_arg(x,i) _fd_get_arg(x,i)
#define fd_get_body(x,i) _fd_get_body(x,i)
#endif

/* Body iteration */

/* Bindings iteration */

#define FD_UNPACK_BINDING(pair,var,val)		\
  if (FD_PAIRP(pair)) {				\
    fdtype _cdr=FD_CDR(pair);			\
    var=FD_CAR(pair);				\
    if (FD_PAIRP(_cdr)) val=FD_CAR(_cdr);	\
    else val=FD_VOID;}				\
  else {}

#define FD_DOBINDINGS(var,val,bindings)					\
  U8_MAYBE_UNUSED fdtype var, val, _scan=bindings, _binding=FD_VOID;	\
  for (_scan=bindings,							\
	 _binding=FD_PAIRP(_scan)?(FD_CAR(_scan)):(FD_VOID),		\
	 var=FD_PAIRP(_binding)?(FD_CAR(_binding)):(FD_VOID),		\
	 val=((FD_PAIRP(_binding)&&(FD_PAIRP(FD_CDR(_binding))))?	\
	      (FD_CAR(FD_CDR(_binding))):(FD_VOID));			\
       FD_PAIRP(_scan);							\
       _scan=FD_CDR(_scan),						\
	 _binding=FD_PAIRP(_scan)?(FD_CAR(_scan)):(FD_VOID),		\
	 var=FD_PAIRP(_binding)?(FD_CAR(_binding)):(FD_VOID),		\
	 val=((FD_PAIRP(_binding)&&(FD_PAIRP(FD_CDR(_binding))))?	\
	      (FD_CAR(FD_CDR(_binding))):(FD_VOID)) )

/* Simple continuations */

typedef struct FD_CONTINUATION {
  FD_FUNCTION_FIELDS; fdtype retval;} FD_CONTINUATION;
typedef struct FD_CONTINUATION *fd_continuation;

/* Threading stuff */

#define FD_THREAD_DONE 1
#define FD_EVAL_THREAD 2
#define FD_THREAD_TRACE_EXIT 4
#define FD_THREAD_QUIET_EXIT 8
typedef struct FD_THREAD_STRUCT {
  FD_CONS_HEADER; int flags; pthread_t tid; 
  int *errnop; double started, finished;
  fdtype *resultptr, result;
  pthread_attr_t attr;
  union {
    struct {fdtype expr; fd_lispenv env;} evaldata;
    struct {fdtype fn, *args; int n_args;} applydata;};} FD_THREAD;
typedef struct FD_THREAD_STRUCT *fd_thread_struct;

typedef struct FD_CONSED_CONDVAR {
  FD_CONS_HEADER; u8_mutex fd_cvlock; u8_condvar fd_cvar;} FD_CONSED_CONDVAR;
typedef struct FD_CONDVAR *fd_consed_condvar;

FD_EXPORT fd_ptr_type fd_thread_type;
FD_EXPORT fd_ptr_type fd_condvar_type;
FD_EXPORT fd_thread_struct fd_thread_call(fdtype *,fdtype,int,fdtype *,int);
FD_EXPORT fd_thread_struct fd_thread_eval(fdtype *,fdtype,fd_lispenv,int);

/* Opcodes */

FD_EXPORT u8_string fd_opcode_names[];
FD_EXPORT int fd_opcode_table_len;

#define FD_SPECIAL_OPCODES   FD_OPCODE(0x00)
/* Special forms, which may not evaluate or use their first argument. */
#define FD_QUOTE_OPCODE      FD_OPCODE(0x00)
#define FD_BEGIN_OPCODE      FD_OPCODE(0x01)
#define FD_AND_OPCODE        FD_OPCODE(0x02)
#define FD_OR_OPCODE         FD_OPCODE(0x03)
#define FD_NOT_OPCODE        FD_OPCODE(0x04)
#define FD_FAIL_OPCODE       FD_OPCODE(0x05)
#define FD_MODREF_OPCODE     FD_OPCODE(0x06)
#define FD_COMMENT_OPCODE    FD_OPCODE(0x07)
#define FD_TRY_OPCODE        FD_OPCODE(0x07) /* NYI */

#define FD_IF_OPCODE         FD_OPCODE(0x10)
#define FD_WHEN_OPCODE       FD_OPCODE(0x11)
#define FD_UNLESS_OPCODE     FD_OPCODE(0x12)
#define FD_IFELSE_OPCODE     FD_OPCODE(0x13)
#define FD_TRYIF_OPCODE      FD_OPCODE(0x14) /* NYI */
/* More to come */
#define FD_MAX_FEXPR_OPCODE  FD_OPCODE(0x20)

/* Unary primitives which handle their own non-determinism. */
#define FD_ND1_OPCODES        FD_OPCODE(0x20)
#define FD_AMBIGP_OPCODE      FD_OPCODE(0x20)
#define FD_SINGLETONP_OPCODE  FD_OPCODE(0x21)
#define FD_FAILP_OPCODE       FD_OPCODE(0x22)
#define FD_EXISTSP_OPCODE     FD_OPCODE(0x23)
#define FD_SINGLETON_OPCODE   FD_OPCODE(0x24)
#define FD_CAR_OPCODE         FD_OPCODE(0x25)
#define FD_CDR_OPCODE         FD_OPCODE(0x26)
#define FD_LENGTH_OPCODE      FD_OPCODE(0x27)
#define FD_QCHOICE_OPCODE     FD_OPCODE(0x28)
#define FD_CHOICE_SIZE_OPCODE FD_OPCODE(0x29)
#define FD_PICKOIDS_OPCODE    FD_OPCODE(0x2A)
#define FD_PICKSTRINGS_OPCODE FD_OPCODE(0x2B)
#define FD_PICKONE_OPCODE     FD_OPCODE(0x2C)
#define FD_IFEXISTS_OPCODE    FD_OPCODE(0x2D)
/* More to come */
#define FD_MAX_ND1_OPCODE     FD_OPCODE(0x40)

/* Unary primitives which don't handle their own non-determinism. */
#define FD_UNARY_OPCODES     FD_OPCODE(0x40)
#define FD_MINUS1_OPCODE     FD_OPCODE(0x40)
#define FD_PLUS1_OPCODE      FD_OPCODE(0x41)
#define FD_NUMBERP_OPCODE    FD_OPCODE(0x42)
#define FD_ZEROP_OPCODE      FD_OPCODE(0x43)
#define FD_VECTORP_OPCODE    FD_OPCODE(0x44)
#define FD_PAIRP_OPCODE      FD_OPCODE(0x45)
#define FD_NULLP_OPCODE      FD_OPCODE(0x46)
#define FD_STRINGP_OPCODE    FD_OPCODE(0x47)
#define FD_OIDP_OPCODE       FD_OPCODE(0x48)
#define FD_SYMBOLP_OPCODE    FD_OPCODE(0x49)
#define FD_FIRST_OPCODE      FD_OPCODE(0x4A)
#define FD_SECOND_OPCODE     FD_OPCODE(0x4B)
#define FD_THIRD_OPCODE      FD_OPCODE(0x4C)
#define FD_TONUMBER_OPCODE   FD_OPCODE(0x4D)
/* More to come */
#define FD_MAX_UNARY_OPCODE  FD_OPCODE(0x60)

/* Arithmetic primitives with two arguments */
#define FD_NUM2_OPCODES      FD_OPCODE(0x60)
#define FD_NUMEQ_OPCODE      FD_OPCODE(0x60)
#define FD_GT_OPCODE         FD_OPCODE(0x61)
#define FD_GTE_OPCODE        FD_OPCODE(0x62)
#define FD_LT_OPCODE         FD_OPCODE(0x63)
#define FD_LTE_OPCODE        FD_OPCODE(0x64)
#define FD_PLUS_OPCODE       FD_OPCODE(0x65)
#define FD_MINUS_OPCODE      FD_OPCODE(0x66)
#define FD_TIMES_OPCODE      FD_OPCODE(0x67)
#define FD_FLODIV_OPCODE     FD_OPCODE(0x68)
#define FD_MAX_NUM2_OPCODES  FD_OPCODE(0x80)

#define FD_BINARY_OPCODES    FD_OPCODE(0x80)
/* Other primitives with two arguments that automatically
   iterate over the arguments */
#define FD_EQ_OPCODE         FD_OPCODE(0x80)
#define FD_EQV_OPCODE        FD_OPCODE(0x81)
#define FD_EQUAL_OPCODE      FD_OPCODE(0x82)
#define FD_ELT_OPCODE        FD_OPCODE(0x83)

#define FD_MAX_BINARY_OPCODE      FD_OPCODE(0xA0)
#define FD_NARY_OPCODES      FD_OPCODE(0xA0)
/* Other primitives with more than two arguments */
#define FD_GET_OPCODE        FD_OPCODE(0xA0)
#define FD_TEST_OPCODE       FD_OPCODE(0xA1)
#define FD_XREF_OPCODE       FD_OPCODE(0xA2)
#define FD_PGET_OPCODE       FD_OPCODE(0xA3)
#define FD_PTEST_OPCODE      FD_OPCODE(0xA4)

#define FD_SETOPS_OPCODES    FD_OPCODE(0xC0)
#define FD_IDENTICAL_OPCODE  FD_OPCODE(0xC0)
#define FD_OVERLAPS_OPCODE   FD_OPCODE(0xC1)
#define FD_CONTAINSP_OPCODE  FD_OPCODE(0xC2)
#define FD_UNION_OPCODE      FD_OPCODE(0xC3)
#define FD_INTERSECT_OPCODE  FD_OPCODE(0xC4)
#define FD_DIFFERENCE_OPCODE FD_OPCODE(0xC5)

#define FD_FEXPR_OPCODES     FD_OPCODE(0xD0)
#define FD_LET_OPCODE        FD_OPCODE(0xD0)
#define FD_LETSTAR_OPCODE    FD_OPCODE(0xD1)
#define FD_COND_OPCODE       FD_OPCODE(0xD2)

#endif /* FRAMERD_EVAL_H */
