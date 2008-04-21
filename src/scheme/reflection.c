/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_PPTRS 1

#include "fdb/dtype.h"
#include "fdb/eval.h"

static MAYBE_UNUSED char versionid[] = 
  "$Id$";

static fdtype macrop(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_macro_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype compound_procedurep(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_sproc_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype applicablep(fdtype x)
{
  if (FD_APPLICABLEP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype special_formp(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_specform_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype primitivep(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_function_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype procedurep(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_sproc_type)) return FD_TRUE;
  else if (FD_PRIM_TYPEP(x,fd_function_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype fcn_name(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->name)
      return fdtype_string(f->name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"fcn_name",x);
}

static fdtype fcn_symbol(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->name)
      return fd_intern(f->name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"fcn_symbol",x);
}

static fdtype fcn_id(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->name)
      return fd_intern(f->name);
    else return fd_incref(x);}
  else return fd_incref(x);
}

static fdtype fcn_filename(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->filename)
      return fdtype_string(f->filename);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"fcn_filename",x);
}

static fdtype fcn_arity(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    int arity=f->arity;
    if (arity<0) return FD_FALSE;
    else return FD_INT2DTYPE(arity);}
  else return fd_type_error(_("procedure"),"fcn_arity",x);
}

static fdtype fcn_min_arity(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    int arity=f->min_arity;
    return FD_INT2DTYPE(arity);}
  else return fd_type_error(_("procedure"),"fcn_min_arity",x);
}

static fdtype compound_procedure_args(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_sproc_type)) {
    struct FD_SPROC *proc=(fd_sproc)fd_pptr_ref(x);
    return fd_incref(proc->arglist);}
  else return fd_type_error("compound procedure","compound_procedure_args",x);
}

static fdtype compound_procedure_env(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_sproc_type)) {
    struct FD_SPROC *proc=(fd_sproc)fd_pptr_ref(x);
    return (fdtype) fd_copy_env(proc->env);}
  else return fd_type_error("compound procedure","compound_procedure_args",x);
}

static fdtype compound_procedure_body(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_sproc_type)) {
    struct FD_SPROC *proc=(fd_sproc)fd_pptr_ref(x);
    return fd_incref(proc->body);}
  else return fd_type_error("compound procedure","compound_procedure_args",x);
}

static fdtype set_compound_procedure_body(fdtype x,fdtype new_body)
{
  if (FD_PRIM_TYPEP(x,fd_sproc_type)) {
    struct FD_SPROC *proc=(fd_sproc)fd_pptr_ref(x);
    fdtype body=proc->body;
    proc->body=fd_incref(new_body);
    fd_decref(body);
    return FD_VOID;}
  else return fd_type_error("compound procedure","compound_procedure_args",x);
}

/* Macro expand */

static fdtype macroexpand(fdtype expander,fdtype expr)
{
  if (FD_PAIRP(expr)) {
    if (FD_PRIM_TYPEP(expander,fd_macro_type)) {
      struct FD_MACRO *macrofn=(struct FD_MACRO *)fd_pptr_ref(expander);
      fd_ptr_type xformer_type=FD_PTR_TYPE(macrofn->transformer);
      if (fd_applyfns[xformer_type]) {
	/* These are special forms which do all the evaluating themselves */
	fdtype new_expr=
	  (fd_applyfns[xformer_type])(fd_pptr_ref(macrofn->transformer),1,&expr);
	new_expr=fd_finish_call(new_expr);
	if (FD_ABORTP(new_expr))
	  return fd_err(fd_SyntaxError,_("macro expansion"),NULL,new_expr);
	else return new_expr;}
      else return fd_err(fd_InvalidMacro,NULL,macrofn->name,expr);}
    else return fd_type_error("macro","macroexpand",expander);}
  else return fd_incref(expr);
}

/* Apropos */

static fdtype apropos_prim(fdtype arg)
{
  u8_string seeking; fdtype all, results=FD_EMPTY_CHOICE;
  if (FD_SYMBOLP(arg)) seeking=FD_SYMBOL_NAME(arg);
  else if (FD_STRINGP(arg)) seeking=FD_STRDATA(arg);
  else return fd_type_error(_("string or symbol"),"apropos",arg);
  all=fd_all_symbols();
  {FD_DO_CHOICES(sym,all) {
    u8_string name=FD_SYMBOL_NAME(sym);
    if (strstr(name,seeking)) {FD_ADD_TO_CHOICE(results,sym);}}}
  return results;
}

/* Module bindings */

static fdtype module_bindings(fdtype arg)
{
  if (FD_PTR_TYPEP(arg,fd_environment_type)) {
    fd_lispenv envptr=FD_GET_CONS(arg,fd_environment_type,fd_lispenv);
    return fd_getkeys(envptr->bindings);}
  else if (FD_TABLEP(arg))
    return fd_getkeys(arg);
  else if (FD_SYMBOLP(arg)) {
    fdtype module=fd_get_module(arg,1);
    if (FD_VOIDP(module))
      return fd_type_error(_("module"),"module_bindings",arg);
    else {
      fdtype v=module_bindings(module);
      fd_decref(module);
      return v;}}
  else return fd_type_error(_("module"),"module_bindings",arg);
}

static fdtype modulep(fdtype arg)
{
  if ((FD_PTR_TYPEP(arg,fd_environment_type)) || (FD_TABLEP(arg)))
    return FD_TRUE;
  else return FD_TRUE;
}

static fdtype module_exports(fdtype arg)
{
  if (FD_PTR_TYPEP(arg,fd_environment_type)) {
    fd_lispenv envptr=FD_GET_CONS(arg,fd_environment_type,fd_lispenv);
    return fd_getkeys(envptr->exports);}
  else if (FD_TABLEP(arg))
    return fd_getkeys(arg);
  else if (FD_SYMBOLP(arg)) {
    fdtype module=fd_get_module(arg,1);
    if (FD_VOIDP(module))
      return fd_type_error(_("module"),"module_exports",arg);
    else {
      fdtype v=module_exports(module);
      fd_decref(module);
      return v;}}
  else return fd_type_error(_("module"),"module_exports",arg);
}

static fdtype local_bindings_handler(fdtype expr,fd_lispenv env)
{
  if (env->copy)
    return fd_incref(env->copy->bindings);
  else {
    fd_lispenv copied=fd_copy_env(env);
    fdtype bindings=copied->bindings;
    fd_incref(bindings);
    fd_decref((fdtype)copied);
    return bindings;}
}

static fdtype thisenv_handler(fdtype expr,fd_lispenv env)
{
  return (fdtype) fd_copy_env(env);
}

/* Finding where a symbol comes from */

static fdtype wherefrom_handler(fdtype expr,fd_lispenv call_env)
{
  fdtype symbol_arg=fd_get_arg(expr,1), symbol=fd_eval(symbol_arg,call_env);
  if (FD_SYMBOLP(symbol)) {
    fdtype env_arg=fd_eval(fd_get_arg(expr,2),call_env); fd_lispenv env;
    if (FD_VOIDP(env_arg)) env=call_env;
    else if (FD_PRIM_TYPEP(env_arg,fd_environment_type))
      env=FD_GET_CONS(env_arg,fd_environment_type,fd_lispenv);
    else return fd_type_error(_("environment"),"wherefrom",env_arg);
    if (env->copy) env=env->copy;
    while (env) {
      if (fd_test(env->bindings,symbol,FD_VOID)) {
	fdtype bindings=env->bindings;
	if ((FD_CONSP(bindings)) &&
	    (FD_MALLOCD_CONSP((fd_cons)bindings)))
	  return fd_incref(env->bindings);
	else {
	  fd_decref(env_arg);
	  return FD_FALSE;}}
      env=env->parent;
      if ((env) && (env->copy)) env=env->copy;}
    fd_decref(env_arg);
    return FD_FALSE;}
  else return fd_type_error(_("symbol"),"wherefrom",symbol);
}

/* Initialization */

FD_EXPORT void fd_init_reflection_c()
{
  fdtype module=fd_new_module("REFLECTION",FD_MODULE_SAFE);

  fdtype apropos_cprim=fd_make_cprim1("APROPOS",apropos_prim,1);
  fd_idefn(module,apropos_cprim);
  fd_defn(fd_scheme_module,apropos_cprim);
  
  fd_idefn(module,fd_make_cprim1("MACRO?",macrop,1));
  fd_idefn(module,fd_make_cprim1("APPLICABLE?",applicablep,1));
  fd_idefn(module,fd_make_cprim1("COMPOUND-PROCEDURE?",compound_procedurep,1));
  fd_idefn(module,fd_make_cprim1("SPECIAL-FORM?",special_formp,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE?",procedurep,1));
  /* FCN? is defined separately because it tells whether or not you can apply the
     FCN-* functions.  Note that this isn't consistent and that PROCEDURE?
     should probably really be COMPOUND-PROCEDURE? */
  fd_idefn(module,fd_make_cprim1("FCN?",procedurep,1));
  fd_idefn(module,fd_make_cprim1("PRIMITIVE?",primitivep,1));
  fd_idefn(module,fd_make_cprim1("MODULE?",modulep,1));
  fd_idefn(module,fd_make_cprim1("FCN-NAME",fcn_name,1));
  fd_idefn(module,fd_make_cprim1("FCN-FILENAME",fcn_filename,1));
  fd_idefn(module,fd_make_cprim1("FCN-ARITY",fcn_arity,1));
  fd_idefn(module,fd_make_cprim1("FCN-MIN-ARITY",fcn_min_arity,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-NAME",fcn_name,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-SYMBOL",fcn_symbol,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ID",fcn_id,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ARGS",compound_procedure_args,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-BODY",compound_procedure_body,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ENV",compound_procedure_env,1));
  fd_idefn(module,
	   fd_make_cprim2("SET-PROCEDURE-BODY!",
			  set_compound_procedure_body,1));
  fd_idefn(module,fd_make_cprim2("MACROEXPAND",macroexpand,2));

  fd_idefn(module,fd_make_cprim1("MODULE-BINDINGS",module_bindings,1));
  fd_idefn(module,fd_make_cprim1("MODULE-EXPORTS",module_exports,1));

  fd_defspecial(module,"%ENV",thisenv_handler);
  fd_defspecial(module,"%BINDINGS",local_bindings_handler);

  fd_defspecial(module,"WHEREFROM",wherefrom_handler);

  fd_finish_module(module);
  fd_persist_module(module);
}

