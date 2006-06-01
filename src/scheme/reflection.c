/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] = 
  "$Id: reflection.c,v 1.9 2006/01/26 14:44:32 haase Exp $";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"


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
    struct FD_FUNCTION *f=(fd_function)x;
    if (f->name)
      return fdtype_string(f->name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"fcn_name",x);
}

static fdtype fcn_filename(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=(fd_function)x;
    if (f->filename)
      return fdtype_string(f->filename);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"fcn_filename",x);
}

static fdtype fcn_arity(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=(fd_function)x;
    int arity=f->arity;
    if (arity<0) return FD_FALSE;
    else return FD_INT2DTYPE(arity);}
  else return fd_type_error(_("procedure"),"fcn_arity",x);
}

static fdtype fcn_min_arity(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=(fd_function)x;
    int arity=f->min_arity;
    return FD_INT2DTYPE(arity);}
  else return fd_type_error(_("procedure"),"fcn_min_arity",x);
}

static fdtype compound_procedure_args(fdtype x)
{
  struct FD_SPROC *proc=FD_GET_CONS(x,fd_sproc_type,fd_sproc);
  return fd_incref(proc->arglist);
}

static fdtype compound_procedure_env(fdtype x)
{
  struct FD_SPROC *proc=FD_GET_CONS(x,fd_sproc_type,fd_sproc);
  return (fdtype) fd_copy_env(proc->env);
}

static fdtype compound_procedure_body(fdtype x)
{
  struct FD_SPROC *proc=FD_GET_CONS(x,fd_sproc_type,fd_sproc);
  return fd_incref(proc->body);
}

static fdtype set_compound_procedure_body(fdtype x,fdtype new_body)
{
  struct FD_SPROC *proc=FD_GET_CONS(x,fd_sproc_type,fd_sproc);
  fdtype body=proc->body;
  proc->body=fd_incref(new_body);
  fd_decref(body);
  return FD_VOID;
}

/* Macro expand */

static fdtype macroexpand(fdtype expander,fdtype expr)
{
  if (FD_PAIRP(expr)) {
    if (FD_PRIM_TYPEP(expander,fd_macro_type)) {
      struct FD_MACRO *macrofn=(struct FD_MACRO *)expander;
      if (fd_applyfns[FD_PTR_TYPE(macrofn->transformer)]) {
	/* These are special forms which do all the evaluating themselves */
	fdtype new_expr=
	  (fd_applyfns[FD_PTR_TYPE(macrofn->transformer)])
	  (macrofn->transformer,1,&expr);
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
  if (FD_PRIM_TYPEP(arg,fd_environment_type)) {
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
  fd_idefn(module,fd_make_cprim1("PRIMITIVE?",primitivep,1));
  fd_idefn(module,fd_make_cprim1("FCN-NAME",fcn_name,1));
  fd_idefn(module,fd_make_cprim1("FCN-FILENAME",fcn_filename,1));
  fd_idefn(module,fd_make_cprim1("FCN-ARITY",fcn_arity,1));
  fd_idefn(module,fd_make_cprim1("FCN-MIN-ARITY",fcn_min_arity,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-NAME",fcn_name,1));
  fd_idefn(module,fd_make_cprim1x("PROCEDURE-ARGS",compound_procedure_args,1,
				  fd_sproc_type,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("PROCEDURE-BODY",compound_procedure_body,1,
				  fd_sproc_type,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("PROCEDURE-ENV",compound_procedure_env,1,
				  fd_sproc_type,FD_VOID));
  fd_idefn(module,
	   fd_make_cprim2("SET-PROCEDURE-BODY!",
			  set_compound_procedure_body,1));
  fd_idefn(module,fd_make_cprim2("MACROEXPAND",macroexpand,2));

  fd_idefn(module,fd_make_cprim1("MODULE-BINDINGS",module_bindings,1));

  fd_finish_module(module);
}

