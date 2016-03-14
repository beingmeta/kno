/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_PPTRS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define GETSPECFORM(x) \
  ((FD_PPTRP(x)) ? ((fd_special_form)(fd_pptr_ref(x))) : ((fd_special_form)x))


static fdtype macrop(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_macro_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype compound_procedurep(fdtype x)
{
  if (FD_SPROCP(x)) return FD_TRUE;
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
  if (FD_FUNCTIONP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype procedure_name(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->name)
      return fdtype_string(f->name);
    else return FD_FALSE;}
  else if (FD_PRIM_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->name)
      return fdtype_string(sf->name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_name",x);
}

static fdtype procedure_filename(fdtype x)
{
  if (FD_FUNCTIONP(x)) {
    struct FD_FUNCTION *f=FD_XFUNCTION(x);
    if (f->filename)
      return fdtype_string(f->filename);
    else return FD_FALSE;}
  else if (FD_PRIM_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->filename)
      return fdtype_string(sf->filename);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_filename",x);
}

static fdtype procedure_symbol(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->name)
      return fd_intern(f->name);
    else return FD_FALSE;}
  else if (FD_PRIM_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->name)
      return fd_intern(sf->name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_symbol",x);
}

static fdtype procedure_id(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->name)
      return fd_intern(f->name);
    else return fd_incref(x);}
  else if (FD_PRIM_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->name)
      return fd_intern(sf->name);
    else return fd_incref(x);}
  else return fd_incref(x);
}

static fdtype procedure_arity(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    int arity=f->arity;
    if (arity<0) return FD_FALSE;
    else return FD_INT(arity);}
  else return fd_type_error(_("procedure"),"procedure_arity",x);
}

static fdtype procedure_min_arity(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    int arity=f->min_arity;
    return FD_INT(arity);}
  else return fd_type_error(_("procedure"),"procedure_min_arity",x);
}

static fdtype compound_procedure_args(fdtype x)
{
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_pptr_ref(x);
    return fd_incref(proc->arglist);}
  else return fd_type_error("compound procedure","compound_procedure_args",x);
}

static fdtype set_compound_procedure_args(fdtype x,fdtype new_arglist)
{
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_pptr_ref(x);
    fdtype arglist=proc->arglist;
    proc->arglist=fd_incref(new_arglist);
    fd_decref(arglist);
    return FD_VOID;}
  else return fd_type_error("compound procedure","set_compound_procedure_args",x);
}

static fdtype compound_procedure_env(fdtype x)
{
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_pptr_ref(x);
    return (fdtype) fd_copy_env(proc->env);}
  else return fd_type_error("compound procedure","compound_procedure_env",x);
}

static fdtype compound_procedure_body(fdtype x)
{
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_pptr_ref(x);
    return fd_incref(proc->body);}
  else return fd_type_error("compound procedure","compound_procedure_body",x);
}

static fdtype set_compound_procedure_body(fdtype x,fdtype new_body)
{
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_pptr_ref(x);
    fdtype body=proc->body;
    proc->body=fd_incref(new_body);
    fd_decref(body);
    return FD_VOID;}
  else return fd_type_error("compound procedure","set_compound_procedure_body",x);
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
  if (FD_ENVIRONMENTP(arg)) {
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
  if ((FD_ENVIRONMENTP(arg)) ||
      (FD_HASHTABLEP(arg)) || (FD_SLOTMAPP(arg)) ||
      (FD_SCHEMAPP(arg)))
    return FD_TRUE;
  else return FD_TRUE;
}

static fdtype module_exports(fdtype arg)
{
  if (FD_ENVIRONMENTP(arg)) {
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

/* Finding all the modules used from an environment */

static fdtype moduleid_symbol;

static fdtype getmodules_handler(fdtype expr,fd_lispenv call_env)
{
  fdtype env_arg=fd_eval(fd_get_arg(expr,1),call_env), modules=FD_EMPTY_CHOICE;
  fd_lispenv env=call_env;
  if (FD_VOIDP(env_arg)) {}
  else if (FD_PRIM_TYPEP(env_arg,fd_environment_type))
    env=FD_GET_CONS(env_arg,fd_environment_type,fd_lispenv);
  else return fd_type_error(_("environment"),"wherefrom",env_arg);
  if (env->copy) env=env->copy;
  while (env) {
    if (fd_test(env->bindings,moduleid_symbol,FD_VOID)) {
      fdtype ids=fd_get(env->bindings,moduleid_symbol,FD_VOID);
      if ((FD_CHOICEP(ids))||(FD_ACHOICEP(ids))) {
        FD_DO_CHOICES(id,ids) {
          if (FD_SYMBOLP(id)) {FD_ADD_TO_CHOICE(modules,id);}}}
      else if (FD_SYMBOLP(ids)) {FD_ADD_TO_CHOICE(modules,ids);}
      else {}}
    env=env->parent;
    if ((env) && (env->copy)) env=env->copy;}
  fd_decref(env_arg);
  return modules;
}

/* Initialization */

FD_EXPORT void fd_init_reflection_c()
{
  fdtype module=fd_new_module("REFLECTION",FD_MODULE_SAFE);

  fdtype apropos_cprim=fd_make_cprim1("APROPOS",apropos_prim,1);
  fd_idefn(module,apropos_cprim);
  fd_defn(fd_scheme_module,apropos_cprim);

  moduleid_symbol=fd_intern("%MODULEID");

  fd_idefn(module,fd_make_cprim1("MACRO?",macrop,1));
  fd_idefn(module,fd_make_cprim1("APPLICABLE?",applicablep,1));
  fd_idefn(module,fd_make_cprim1("COMPOUND-PROCEDURE?",compound_procedurep,1));
  fd_idefn(module,fd_make_cprim1("SPECIAL-FORM?",special_formp,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE?",procedurep,1));

  fd_idefn(module,fd_make_cprim1("PRIMITIVE?",primitivep,1));
  fd_idefn(module,fd_make_cprim1("MODULE?",modulep,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-NAME",procedure_name,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-FILENAME",procedure_filename,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ARITY",procedure_arity,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-MIN-ARITY",procedure_min_arity,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-SYMBOL",procedure_symbol,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ID",procedure_id,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ARGS",compound_procedure_args,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-BODY",compound_procedure_body,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ENV",compound_procedure_env,1));
  fd_idefn(module,
           fd_make_cprim2("SET-PROCEDURE-BODY!",
                          set_compound_procedure_body,1));
  fd_idefn(module,
           fd_make_cprim2("SET-PROCEDURE-ARGS!",
                          set_compound_procedure_args,1));
  fd_idefn(module,fd_make_cprim2("MACROEXPAND",macroexpand,2));

#if 0
  fd_idefn(module,fd_make_cprim1("FCN?",procedurep,1));
  fd_idefn(module,fd_make_cprim1("FCN-NAME",procedure_name,1));
  fd_idefn(module,fd_make_cprim1("FCN-FILENAME",procedure_filename,1));
  fd_idefn(module,fd_make_cprim1("FCN-ARITY",procedure_arity,1));
  fd_idefn(module,fd_make_cprim1("FCN-MIN-ARITY",procedure_min_arity,1));
#endif

  fd_idefn(module,fd_make_cprim1("MODULE-BINDINGS",module_bindings,1));
  fd_idefn(module,fd_make_cprim1("MODULE-EXPORTS",module_exports,1));

  fd_defspecial(module,"%ENV",thisenv_handler);
  fd_defspecial(module,"%BINDINGS",local_bindings_handler);

  fd_defspecial(module,"WHEREFROM",wherefrom_handler);
  fd_defspecial(module,"GETMODULES",getmodules_handler);

  fd_finish_module(module);
  fd_persist_module(module);
}
