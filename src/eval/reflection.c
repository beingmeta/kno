/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"

#include "libu8/u8streamio.h"
#include "libu8/u8printf.h"

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static fdtype moduleid_symbol;

#define GETSPECFORM(x) ((fd_special_form)(fd_fcnid_ref(x)))

static fdtype macrop(fdtype x)
{
  if (FD_TYPEP(x,fd_macro_type)) return FD_TRUE;
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
  if (FD_TYPEP(x,fd_specform_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype primitivep(fdtype x)
{
  if (FD_TYPEP(x,fd_primfcn_type)) return FD_TRUE;
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
    if (f->fcn_name)
      return fdtype_string(f->fcn_name);
    else return FD_FALSE;}
  else if (FD_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->fexpr_name)
      return fdtype_string(sf->fexpr_name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_name",x);
}

static fdtype procedure_filename(fdtype x)
{
  if (FD_FUNCTIONP(x)) {
    struct FD_FUNCTION *f=FD_XFUNCTION(x);
    if (f->fcn_filename)
      return fdtype_string(f->fcn_filename);
    else return FD_FALSE;}
  else if (FD_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->fexpr_filename)
      return fdtype_string(sf->fexpr_filename);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_filename",x);
}

static fdtype procedure_symbol(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return fd_intern(f->fcn_name);
    else return FD_FALSE;}
  else if (FD_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->fexpr_name)
      return fd_intern(sf->fexpr_name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_symbol",x);
}

static fdtype procedure_id(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return fd_intern(f->fcn_name);
    else return fd_incref(x);}
  else if (FD_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(x);
    if (sf->fexpr_name)
      return fd_intern(sf->fexpr_name);
    else return fd_incref(x);}
  else return fd_incref(x);
}

static fdtype procedure_documentation(fdtype x)
{
  fdtype proc=(FD_FCNIDP(x)) ? (fd_fcnid_ref(x)) : (x);
  fd_ptr_type proctype=FD_PTR_TYPE(proc);
  if (proctype==fd_sproc_type) {
    struct FD_SPROC *sproc=(fd_sproc)proc;
    if (sproc->fcn_documentation)
      return fdtype_string(sproc->fcn_documentation);
    else {
      struct U8_OUTPUT out; u8_byte _buf[256];
      U8_INIT_OUTPUT_BUF(&out,256,_buf);
      fdtype arglist=sproc->sproc_arglist, scan=arglist;
      if (sproc->fcn_name)
        u8_puts(&out,sproc->fcn_name);
      else u8_puts(&out,"λ");
      while (FD_PAIRP(scan)) {
        fdtype arg=FD_CAR(scan);
        if (FD_SYMBOLP(arg))
          u8_printf(&out," %ls",FD_SYMBOL_NAME(arg));
        else if ((FD_PAIRP(arg))&&(FD_SYMBOLP(FD_CAR(arg))))
          u8_printf(&out," [%ls]",FD_SYMBOL_NAME(FD_CAR(arg)));
        else u8_printf(&out," %q",arg);
        scan=FD_CDR(scan);}
      if (FD_SYMBOLP(scan))
        u8_printf(&out," [%ls...]",FD_SYMBOL_NAME(scan));
      if (out.u8_outbuf==_buf)
        return fdtype_string(_buf);
      else return fd_init_string(NULL,out.u8_write-out.u8_outbuf,
                                 out.u8_outbuf);}}
  else if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(proc);
    if (f->fcn_documentation)
      return fdtype_string(f->fcn_documentation);
    else return FD_FALSE;}
  else if (FD_TYPEP(x,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(proc);
    if (sf->fexpr_documentation)
      return fdtype_string(sf->fexpr_documentation);
    else FD_FALSE;}
  else return FD_FALSE;
}

static fdtype set_procedure_documentation(fdtype x,fdtype doc)
{
  fdtype proc=(FD_FCNIDP(x)) ? (fd_fcnid_ref(x)) : (x);
  fd_ptr_type proctype=FD_PTR_TYPE(proc);
  if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(proc);
    if (f->fcn_documentation) u8_free(f->fcn_documentation);
    f->fcn_documentation=FD_STRDATA(doc);
    return FD_VOID;}
  else if (FD_TYPEP(proc,fd_specform_type)) {
    struct FD_SPECIAL_FORM *sf=GETSPECFORM(proc);
    if (sf->fexpr_documentation) u8_free(sf->fexpr_documentation);
    sf->fexpr_documentation=FD_STRDATA(doc);
    return FD_VOID;}
  else return fd_err("Not Handled","set_procedure_documentation",
                     NULL,x);
}

static fdtype procedure_arity(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    int arity=f->fcn_arity;
    if (arity<0) return FD_FALSE;
    else return FD_INT(arity);}
  else return fd_type_error(_("procedure"),"procedure_arity",x);
}

static fdtype non_deterministicp(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    if (f->fcn_ndcall)
      return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error(_("procedure"),"non_deterministicp",x);
}

static fdtype synchronizedp(fdtype x)
{
  if (FD_TYPEP(x,fd_sproc_type)) {
    fd_sproc f=(fd_sproc)x;
    if (f->sproc_synchronized)
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_APPLICABLEP(x))
    return FD_FALSE;
  else return fd_type_error(_("procedure"),"non_deterministicp",x);
}

static fdtype procedure_min_arity(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(x);
    int arity=f->fcn_min_arity;
    return FD_INT(arity);}
  else return fd_type_error(_("procedure"),"procedure_min_arity",x);
}

static fdtype compound_procedure_args(fdtype arg)
{
  fdtype x=fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)x;
    return fd_incref(proc->sproc_arglist);}
  else return fd_type_error
	 ("compound procedure","compound_procedure_args",x);
}

static fdtype set_compound_procedure_args(fdtype arg,fdtype new_arglist)
{
  fdtype x=fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_fcnid_ref(x);
    fdtype arglist=proc->sproc_arglist;
    proc->sproc_arglist=fd_incref(new_arglist);
    fd_decref(arglist);
    return FD_VOID;}
  else return fd_type_error
	 ("compound procedure","set_compound_procedure_args",x);
}

static fdtype compound_procedure_env(fdtype arg)
{
  fdtype x=fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_fcnid_ref(x);
    return (fdtype) fd_copy_env(proc->sproc_env);}
  else return fd_type_error("compound procedure","compound_procedure_env",x);
}

static fdtype compound_procedure_body(fdtype arg)
{
  fdtype x=fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_fcnid_ref(x);
    return fd_incref(proc->sproc_body);}
  else return fd_type_error
	 ("compound procedure","compound_procedure_body",x);
}

static fdtype set_compound_procedure_body(fdtype arg,fdtype new_body)
{
  fdtype x=fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_fcnid_ref(x);
    fdtype body=proc->sproc_body;
    proc->sproc_body=fd_incref(new_body);
    fd_decref(body);
    return FD_VOID;}
  else return fd_type_error
	 ("compound procedure","set_compound_procedure_body",x);
}

static fdtype compound_procedure_bytecode(fdtype arg)
{
  fdtype x=fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_fcnid_ref(x);
    if (proc->sproc_bytecode) {
      fdtype cur=(fdtype)(proc->sproc_bytecode);
      fd_incref(cur);
      return cur;}
    else return FD_FALSE;}
  else return fd_type_error
	 ("compound procedure","compound_procedure_body",x);
}

static fdtype set_compound_procedure_bytecode(fdtype arg,fdtype bytecode)
{
  fdtype x=fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)fd_fcnid_ref(x);
    if (proc->sproc_bytecode) {
      fdtype cur=(fdtype)(proc->sproc_bytecode);
      fd_decref(cur);}
    fd_incref(bytecode);
    proc->sproc_bytecode=(struct FD_VECTOR *)bytecode;
    return FD_VOID;}
  else return fd_type_error
	 ("compound procedure","set_compound_procedure_bytecode",x);
}

static fdtype set_compound_procedure_optimizer(fdtype arg,fdtype optimizer)
{
  fdtype x=fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc=(fd_sproc)x;
    if (proc->sproc_optimizer) {
      fdtype cur=(fdtype)(proc->sproc_optimizer);
      fd_decref(cur);}
    fd_incref(optimizer);
    proc->sproc_optimizer=optimizer;
    return FD_VOID;}
  else return fd_type_error
	 ("compound procedure","set_compound_procedure_optimizer",x);
}

/* Function IDs */

static fdtype fcnid_refprim(fdtype arg)
{
  fdtype result=fd_fcnid_ref(arg);
  fd_incref(result);
  return result;
}

static fdtype fcnid_registerprim(fdtype value)
{
  if (FD_FCNIDP(value))
    return value;
  else return fd_register_fcnid(value);
}

static fdtype fcnid_setprim(fdtype arg,fdtype value)
{
  return fd_set_fcnid(arg,value);
}

/* Macro expand */

static fdtype macroexpand(fdtype expander,fdtype expr)
{
  if (FD_PAIRP(expr)) {
    if (FD_TYPEP(expander,fd_macro_type)) {
      struct FD_MACRO *macrofn=(struct FD_MACRO *)fd_fcnid_ref(expander);
      fd_ptr_type xformer_type=FD_PTR_TYPE(macrofn->macro_transformer);
      if (fd_applyfns[xformer_type]) {
        /* These are special forms which do all the evaluating themselves */
        fdtype new_expr=
          (fd_applyfns[xformer_type])
          (fd_fcnid_ref(macrofn->macro_transformer),1,&expr);
        new_expr=fd_finish_call(new_expr);
        if (FD_ABORTP(new_expr))
          return fd_err(fd_SyntaxError,_("macro expansion"),NULL,new_expr);
        else return new_expr;}
      else return fd_err(fd_InvalidMacro,NULL,macrofn->fd_macro_name,expr);}
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
    fd_lispenv envptr=fd_consptr(fd_lispenv,arg,fd_environment_type);
    return fd_getkeys(envptr->env_bindings);}
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
  if (FD_ENVIRONMENTP(arg)) {
    struct FD_ENVIRONMENT *env=
      fd_consptr(struct FD_ENVIRONMENT *,arg,fd_environment_type);
    if (fd_test(env->env_bindings,moduleid_symbol,FD_VOID))
      return FD_TRUE;
    else return FD_FALSE;}
  else if ((FD_HASHTABLEP(arg)) || (FD_SLOTMAPP(arg)) || (FD_SCHEMAPP(arg))) {
    if (fd_test(arg,moduleid_symbol,FD_VOID))
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static fdtype module_exports(fdtype arg)
{
  if (FD_ENVIRONMENTP(arg)) {
    fd_lispenv envptr=fd_consptr(fd_lispenv,arg,fd_environment_type);
    return fd_getkeys(envptr->env_exports);}
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
  if (env->env_copy)
    return fd_incref(env->env_copy->env_bindings);
  else {
    fd_lispenv copied=fd_copy_env(env);
    fdtype bindings=copied->env_bindings;
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
    else if (FD_TYPEP(env_arg,fd_environment_type))
      env=fd_consptr(fd_lispenv,env_arg,fd_environment_type);
    else return fd_type_error(_("environment"),"wherefrom",env_arg);
    if (env->env_copy) env=env->env_copy;
    while (env) {
      if (fd_test(env->env_bindings,symbol,FD_VOID)) {
        fdtype bindings=env->env_bindings;
        if ((FD_CONSP(bindings)) &&
            (FD_MALLOCD_CONSP((fd_cons)bindings)))
          return fd_incref(env->env_bindings);
        else {
          fd_decref(env_arg);
          return FD_FALSE;}}
      env=env->env_parent;
      if ((env) && (env->env_copy)) env=env->env_copy;}
    fd_decref(env_arg);
    return FD_FALSE;}
  else return fd_type_error(_("symbol"),"wherefrom",symbol);
}

/* Finding all the modules used from an environment */

static fdtype getmodules_handler(fdtype expr,fd_lispenv call_env)
{
  fdtype env_arg=fd_eval(fd_get_arg(expr,1),call_env), modules=FD_EMPTY_CHOICE;
  fd_lispenv env=call_env;
  if (FD_VOIDP(env_arg)) {}
  else if (FD_TYPEP(env_arg,fd_environment_type))
    env=fd_consptr(fd_lispenv,env_arg,fd_environment_type);
  else return fd_type_error(_("environment"),"wherefrom",env_arg);
  if (env->env_copy) env=env->env_copy;
  while (env) {
    if (fd_test(env->env_bindings,moduleid_symbol,FD_VOID)) {
      fdtype ids=fd_get(env->env_bindings,moduleid_symbol,FD_VOID);
      if ((FD_CHOICEP(ids))||(FD_ACHOICEP(ids))) {
        FD_DO_CHOICES(id,ids) {
          if (FD_SYMBOLP(id)) {FD_ADD_TO_CHOICE(modules,id);}}}
      else if (FD_SYMBOLP(ids)) {FD_ADD_TO_CHOICE(modules,ids);}
      else {}}
    env=env->env_parent;
    if ((env) && (env->env_copy)) env=env->env_copy;}
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
  fd_idefn(module,fd_make_cprim1("NON-DETERMINISTIC?",non_deterministicp,1));
  fd_idefn(module,fd_make_cprim1("SYNCHRONIZED?",synchronizedp,1));
  fd_idefn(module,fd_make_cprim1("MODULE?",modulep,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-NAME",procedure_name,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-FILENAME",procedure_filename,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ARITY",procedure_arity,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-MIN-ARITY",procedure_min_arity,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-SYMBOL",procedure_symbol,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-DOCUMENTATION",
                                 procedure_documentation,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ID",procedure_id,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ARGS",compound_procedure_args,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-BODY",compound_procedure_body,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-ENV",compound_procedure_env,1));
  fd_idefn(module,fd_make_cprim1("PROCEDURE-BYTECODE",
                                 compound_procedure_bytecode,1));

  fd_idefn(module,
           fd_make_cprim2x("SET-PROCEDURE-DOCUMENTATION!",
                           set_procedure_documentation,2,
                           -1,FD_VOID,fd_string_type,FD_VOID));

  fd_idefn(module,
           fd_make_cprim2("SET-PROCEDURE-BODY!",
                          set_compound_procedure_body,2));
  fd_idefn(module,
           fd_make_cprim2("SET-PROCEDURE-ARGS!",
                          set_compound_procedure_args,2));
  fd_idefn(module,
           fd_make_cprim2x("SET-PROCEDURE-OPTIMIZER!",
                           set_compound_procedure_optimizer,2,
                           fd_sproc_type,FD_VOID,-1,FD_VOID));
  fd_idefn(module,
           fd_make_cprim2x("SET-PROCEDURE-BYTECODE!",
                           set_compound_procedure_bytecode,2,
                           -1,FD_VOID,fd_vector_type,FD_VOID));

  fd_idefn(module,fd_make_cprim2("MACROEXPAND",macroexpand,2));

  fd_idefn(module,fd_make_cprim1x
           ("FCNID/REF",fcnid_refprim,1,fd_fcnid_type,FD_VOID));
  fd_idefn(module,fd_make_cprim1x
           ("FCNID/REGISTER",fcnid_registerprim,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim2x
           ("FCNID/SET!",fcnid_setprim,1,fd_fcnid_type,FD_VOID,-1,FD_VOID));

  fd_idefn(module,fd_make_cprim1("MODULE-BINDINGS",module_bindings,1));
  fd_idefn(module,fd_make_cprim1("MODULE-EXPORTS",module_exports,1));

  fd_defspecial(module,"%ENV",thisenv_handler);
  fd_defspecial(module,"%BINDINGS",local_bindings_handler);

  fd_defspecial(module,"WHEREFROM",wherefrom_handler);
  fd_defspecial(module,"GETMODULES",getmodules_handler);

  fd_finish_module(module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
