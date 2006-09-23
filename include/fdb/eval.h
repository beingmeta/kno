/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FDB_EVAL_H
#define FDB_EVAL_H 1
#define FDB_EVAL_H_VERSION "$Id$"

#include "fdb/dtype.h"
#include "fdb/apply.h"
#include <assert.h>

FD_EXPORT fd_exception fd_UnboundIdentifier, fd_BindError;
FD_EXPORT fd_exception fd_NoSuchModule, fd_SyntaxError, fd_BindSyntaxError;
FD_EXPORT fd_exception fd_TooFewExpressions, fd_NotAnIdentifier;
FD_EXPORT fd_exception fd_InvalidMacro, FD_BadArglist;
FD_EXPORT fd_exception fd_ReadOnlyEnv;

FD_EXPORT fdtype fd_scheme_module, fd_xscheme_module;

FD_EXPORT fd_ptr_type fd_environment_type, fd_specform_type;
FD_EXPORT fd_ptr_type  fd_sproc_type, fd_macro_type;

FD_EXPORT int fd_init_fdscheme(void) FD_LIBINIT_FN;
FD_EXPORT void fd_init_schemeio(void);

/* Constants */

#define FD_STACK_ARGS 6

/* Environments */

#define FD_MODULE_SAFE 1
#define FD_MODULE_DEFAULT 2

typedef struct FD_ENVIRONMENT {
  FD_CONS_HEADER;
  fdtype bindings, exports;
  struct FD_ENVIRONMENT *parent, *copy;} FD_ENVIRONMENT;
typedef struct FD_ENVIRONMENT *fd_environment;
typedef struct FD_ENVIRONMENT *fd_lispenv;

FD_EXPORT fdtype fd_symeval(fdtype,fd_lispenv);
FD_EXPORT int fd_set_value(fdtype,fdtype,fd_lispenv);
FD_EXPORT int fd_add_value(fdtype,fdtype,fd_lispenv);
FD_EXPORT int fd_bind_value(fdtype,fdtype,fd_lispenv);

#define FD_XENV(x) \
  (FD_GET_CONS(x,fd_environment_type,struct FD_ENVIRONMENT *))
#define FD_XENVIRONMENT(x) \
 (FD_GET_CONS(x,fd_environment_type,struct FD_ENVIRONMENT *))
#define FD_ENVIRONMENTP(x) (FD_PRIM_TYPEP(x,fd_environment_type))

FD_EXPORT int fd_recycle_environment(fd_lispenv env);

/* Special forms */

typedef fdtype (*fd_evalfn)(fdtype expr,struct FD_ENVIRONMENT *);

typedef struct FD_SPECIAL_FORM {
  FD_CONS_HEADER;
  u8_string name;
  fd_evalfn eval;} FD_SPECIAL_FORM;
typedef struct FD_SPECIAL_FORM *fd_special_form;

FD_EXPORT fdtype fd_make_special_form(u8_string name,fd_evalfn fn);
FD_EXPORT void fd_defspecial(fdtype mod,u8_string name,fd_evalfn fn);

typedef struct FD_MACRO {
  FD_CONS_HEADER;
  u8_string name;
  fdtype transformer;} FD_MACRO;
typedef struct FD_MACRO *fd_macro;

/* Modules */

FD_EXPORT fd_lispenv fd_working_environment(void);
FD_EXPORT fd_lispenv fd_safe_working_environment(void);
FD_EXPORT fd_lispenv fd_make_env(fdtype module,fd_lispenv parent);
FD_EXPORT fd_lispenv fd_make_export_env(fdtype exports,fd_lispenv parent);
FD_EXPORT fdtype fd_register_module(char *name,fdtype module,int flags);
FD_EXPORT fdtype fd_get_module(fdtype name,int safe);
FD_EXPORT int fd_finish_module(fdtype module);

FD_EXPORT fdtype fd_make_special_form(u8_string name,fd_evalfn fn);
FD_EXPORT void fd_defspecial(fdtype mod,u8_string name,fd_evalfn fn);

FD_EXPORT void fd_set_module_resolver(u8_string (*resolve)(u8_string,int));
FD_EXPORT fdtype fd_find_module(fdtype,int,int);

/* SPROCs */

typedef struct FD_SPROC {
  FD_FUNCTION_FIELDS;
  short n_vars, synchronized; 
  fdtype *schema, arglist, body;
  fd_lispenv env;
  U8_MUTEX_DECL(lock);
} FD_SPROC;
typedef struct FD_SPROC *fd_sproc;

FD_EXPORT fdtype fd_apply_sproc(struct FD_SPROC *fn,int n,fdtype *args);
FD_EXPORT fdtype fd_xapply_sproc
  (struct FD_SPROC *fn,void *data,fdtype (*getval)(void *,fdtype));

/* Loading files and config data */

typedef struct FD_SOURCEFN {
  u8_string (*getsource)(u8_string,u8_string,u8_string *,time_t *timep);
  struct FD_SOURCEFN *next;} FD_SOURCEFN;
typedef struct FD_SOURCEFN *fd_sourcefn;

FD_EXPORT u8_string fd_get_source(u8_string,u8_string,u8_string *,time_t *);
FD_EXPORT void fd_register_sourcefn(u8_string (*fn)(u8_string,u8_string,u8_string *,time_t *));

FD_EXPORT int fd_load_config(u8_string sourceid);
FD_EXPORT fdtype fd_load_source
  (u8_string sourceid,fd_lispenv env,u8_string enc_name);
FD_EXPORT u8_string fd_sourcebase();
FD_EXPORT u8_string fd_get_component(u8_string spec);
FD_EXPORT u8_string fd_bind_sourcebase(u8_string sourcebase);
FD_EXPORT void fd_restore_sourcebase(u8_string sourcebase);

typedef struct FD_CONFIG_RECORD {
  u8_string source;
  struct FD_CONFIG_RECORD *next;} FD_CONFIG_RECORD;

/* The Evaluator */

FD_EXPORT fdtype fd_eval(fdtype expr,fd_lispenv env);
FD_EXPORT fdtype fd_eval_exprs(fdtype exprs,fd_lispenv env);
FD_EXPORT fdtype _fd_get_arg(fdtype expr,int i);
FD_EXPORT fdtype _fd_get_body(fdtype expr,int i);

FD_EXPORT fd_lispenv fd_copy_env(fd_lispenv env);

#if FD_PROVIDE_FASTEVAL
FD_FASTOP fdtype fasteval(fdtype x,fd_lispenv env)
{
  switch (FD_PTR_MANIFEST_TYPE(x)) {
  case fd_oid_ptr_type: case fd_fixnum_ptr_type:
    return x;
  case fd_immediate_ptr_type:
    if (FD_SYMBOLP(x)) return fd_eval(x,env);
    else return x;
  case fd_cons_ptr_type:
    if (FD_PRIM_TYPEP(x,fd_pair_type))
      return fd_eval(x,env);
    else return fd_incref(x);
  }
}
FD_FASTOP fdtype fd_get_arg(fdtype expr,int i)
{
  while (FD_PAIRP(expr))
    if (i == 0) return FD_CAR(expr);
    else {expr=FD_CDR(expr); i--;}
  return FD_VOID;
}
FD_FASTOP fdtype fd_get_body(fdtype expr,int i)
{
  while (FD_PAIRP(expr))
    if (i == 0) break;
    else {expr=FD_CDR(expr); i--;}
  return expr;
}
#else
#define fd_get_arg(x,i) _fd_get_arg(x,i)
#define fd_get_body(x,i) _fd_get_body(x,i)
#endif

/* Simple continuations */

typedef struct FD_CONTINUATION {
  FD_FUNCTION_FIELDS;
  fdtype throwval, retval;} *fd_continuation;
typedef struct FD_CONTINUATION FD_CONTINUATION;

/* Threading stuff */

#if FD_THREADS_ENABLED
#define FD_THREAD_DONE 1
#define FD_EVAL_THREAD 2
typedef struct FD_THREAD_STRUCT {
  FD_CONS_HEADER; int flags; pthread_t tid;
  fdtype *resultptr, result;
  union {
    struct {fdtype expr; fd_lispenv env;} evaldata;
    struct {fdtype fn, *args; int n_args;} applydata;};} FD_THREAD;
typedef struct FD_THREAD_STRUCT *fd_thread_struct;

typedef struct FD_CONSED_CONDVAR {
  FD_CONS_HEADER; u8_mutex lock; u8_condvar cvar;} FD_CONSED_CONDVAR;
typedef struct FD_CONDVAR *fd_consed_condvar;

FD_EXPORT fd_ptr_type fd_thread_type;
FD_EXPORT fd_ptr_type fd_condvar_type;
FD_EXPORT fd_thread_struct fd_thread_call(fdtype *,fdtype,int,fdtype *);
FD_EXPORT fd_thread_struct fd_thread_eval(fdtype *,fdtype,fd_lispenv);
#endif /* FD_THREADS_ENABLED */


#endif /* FDB_EVAL_H */

