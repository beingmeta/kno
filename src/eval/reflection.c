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

#define GETEVALFN(x) ((fd_evalfn)(fd_fcnid_ref(x)))

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

static fdtype evalfnp(fdtype x)
{
  if (FD_TYPEP(x,fd_evalfn_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype primitivep(fdtype x)
{
  if (FD_TYPEP(x,fd_cprim_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype procedurep(fdtype x)
{
  if (FD_FUNCTIONP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype procedure_name(fdtype x)
{
  if (FD_FUNCTIONP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return fdtype_string(f->fcn_name);
    else return FD_FALSE;}
  else if (FD_APPLICABLEP(x))
    return FD_FALSE;
  else if (FD_TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return fdtype_string(sf->evalfn_name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_name",x);
}

static fdtype procedure_filename(fdtype x)
{
  if (FD_FUNCTIONP(x)) {
    struct FD_FUNCTION *f = FD_XFUNCTION(x);
    if (f->fcn_filename)
      return fdtype_string(f->fcn_filename);
    else return FD_FALSE;}
  else if (FD_TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_filename)
      return fdtype_string(sf->evalfn_filename);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_filename",x);
}

static fdtype procedure_symbol(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return fd_intern(f->fcn_name);
    else return FD_FALSE;}
  else if (FD_TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return fd_intern(sf->evalfn_name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_symbol",x);
}

static fdtype procedure_id(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return fd_intern(f->fcn_name);
    else return fd_incref(x);}
  else if (FD_TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return fd_intern(sf->evalfn_name);
    else return fd_incref(x);}
  else return fd_incref(x);
}

static fdtype procedure_documentation(fdtype x)
{
  u8_string doc = fd_get_documentation(x);
  if (doc)
    return fdtype_string(doc);
  else return FD_FALSE;
}

static fdtype set_procedure_documentation(fdtype x,fdtype doc)
{
  fdtype proc = (FD_FCNIDP(x)) ? (fd_fcnid_ref(x)) : (x);
  fd_ptr_type proctype = FD_PTR_TYPE(proc);
  if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_documentation) u8_free(f->fcn_documentation);
    f->fcn_documentation = CSTRING(doc);
    return VOID;}
  else if (FD_TYPEP(proc,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(proc);
    if (sf->evalfn_documentation) u8_free(sf->evalfn_documentation);
    sf->evalfn_documentation = CSTRING(doc);
    return VOID;}
  else return fd_err("Not Handled","set_procedure_documentation",
                     NULL,x);
}

static fdtype procedure_arity(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    int arity = f->fcn_arity;
    if (arity<0) return FD_FALSE;
    else return FD_INT(arity);}
  else return fd_type_error(_("procedure"),"procedure_arity",x);
}

static fdtype non_deterministicp(fdtype x)
{
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_ndcall)
      return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error(_("procedure"),"non_deterministicp",x);
}

static fdtype synchronizedp(fdtype x)
{
  if (FD_TYPEP(x,fd_sproc_type)) {
    fd_sproc f = (fd_sproc)x;
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
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    int arity = f->fcn_min_arity;
    return FD_INT(arity);}
  else return fd_type_error(_("procedure"),"procedure_min_arity",x);
}

/* Procedure attribs */


static fdtype get_proc_attribs(fdtype x,int create)
{
  fd_ptr_type proctype = FD_PTR_TYPE(x);
  if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    fdtype attribs = f->fcn_attribs;
    if (!(create)) {
      if ((attribs!=FD_NULL)&&(TABLEP(attribs)))
        return attribs;
      else return VOID;}
    if ((attribs == FD_NULL)||(!(TABLEP(attribs))))
      f->fcn_attribs = attribs = fd_init_slotmap(NULL,4,NULL);
    return attribs;}
  else if (create) {
    fd_seterr("NoAttribs","get_proc_attribs",NULL,x);
    return FD_ERROR;}
  else return VOID;
}

static fdtype get_procedure_attribs(fdtype x)
{
  fdtype attribs = get_proc_attribs(x,1);
  if (FD_ABORTP(attribs)) return attribs;
  else fd_incref(attribs);
  return attribs;
}

static fdtype set_procedure_attribs(fdtype x,fdtype value)
{
  fd_ptr_type proctype = FD_PTR_TYPE(x);
  if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    fdtype table = f->fcn_attribs;
    if (table!=FD_NULL) fd_decref(table);
    f->fcn_attribs = fd_incref(value);
    return VOID;}
  else return fd_err("Not Handled","set_procedure_documentation",
                     NULL,x);
}

static fdtype reflect_get(fdtype x,fdtype attrib)
{
  fdtype attribs = get_proc_attribs(x,0);
  if (TABLEP(attribs))
    return fd_get(attribs,attrib,FD_FALSE);
  else return FD_FALSE;
}

static fdtype reflect_store(fdtype x,fdtype attrib,fdtype value)
{
  fdtype attribs = get_proc_attribs(x,1);
  if (FD_ABORTP(attribs)) return attribs;
  else if (TABLEP(attribs)) {
    int rv = fd_store(attribs,attrib,value);
    if (rv<0) return FD_ERROR;
    else if (rv==0) return FD_FALSE;
    else return FD_INT(rv);}
  else return FD_ERROR;
}

static fdtype reflect_add(fdtype x,fdtype attrib,fdtype value)
{
  fdtype attribs = get_proc_attribs(x,1);
  if (FD_ABORTP(attribs)) return attribs;
  else if (TABLEP(attribs)) {
    int rv = fd_add(attribs,attrib,value);
    if (rv<0) return FD_ERROR;
    else if (rv==0) return FD_FALSE;
    else return FD_INT(rv);}
  else return FD_ERROR;
}

static fdtype reflect_drop(fdtype x,fdtype attrib,fdtype value)
{
  fdtype attribs = get_proc_attribs(x,1);
  if (FD_ABORTP(attribs)) return attribs;
  else if (TABLEP(attribs)) {
    int rv = fd_drop(attribs,attrib,value);
    if (rv<0) return FD_ERROR;
    else if (rv==0) return FD_FALSE;
    else return FD_INT(rv);}
  else return FD_ERROR;
}

/* SPROC functions */

static fdtype compound_procedure_args(fdtype arg)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)x;
    return fd_incref(proc->sproc_arglist);}
  else return fd_type_error
	 ("compound procedure","compound_procedure_args",x);
}

static fdtype set_compound_procedure_args(fdtype arg,fdtype new_arglist)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)fd_fcnid_ref(x);
    fdtype arglist = proc->sproc_arglist;
    proc->sproc_arglist = fd_incref(new_arglist);
    fd_decref(arglist);
    return VOID;}
  else return fd_type_error
	 ("compound procedure","set_compound_procedure_args",x);
}

static fdtype compound_procedure_env(fdtype arg)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)fd_fcnid_ref(x);
    return (fdtype) fd_copy_env(proc->sproc_env);}
  else return fd_type_error("compound procedure","compound_procedure_env",x);
}

static fdtype compound_procedure_body(fdtype arg)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)fd_fcnid_ref(x);
    return fd_incref(proc->sproc_body);}
  else return fd_type_error
	 ("compound procedure","compound_procedure_body",x);
}

static fdtype compound_procedure_source(fdtype arg)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)fd_fcnid_ref(x);
    if (VOIDP(proc->sproc_source))
      return FD_FALSE;
    else return fd_incref(proc->sproc_source);}
  else return fd_type_error
	 ("compound procedure","compound_procedure_source",x);
}

static fdtype set_compound_procedure_body(fdtype arg,fdtype new_body)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)fd_fcnid_ref(x);
    fdtype body = proc->sproc_body;
    proc->sproc_body = fd_incref(new_body);
    fd_decref(body);
    return VOID;}
  else return fd_type_error
	 ("compound procedure","set_compound_procedure_body",x);
}

static fdtype set_compound_procedure_source(fdtype arg,fdtype new_source)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)fd_fcnid_ref(x);
    fdtype source = proc->sproc_source;
    proc->sproc_source = fd_incref(new_source);
    fd_decref(source);
    return VOID;}
  else return fd_type_error
	 ("compound procedure","set_compound_procedure_source",x);
}

static fdtype compound_procedure_bytecode(fdtype arg)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)fd_fcnid_ref(x);
    if (proc->sproc_bytecode) {
      fdtype cur = (fdtype)(proc->sproc_bytecode);
      fd_incref(cur);
      return cur;}
    else return FD_FALSE;}
  else return fd_type_error
	 ("compound procedure","compound_procedure_body",x);
}

static fdtype set_compound_procedure_bytecode(fdtype arg,fdtype bytecode)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)fd_fcnid_ref(x);
    if (proc->sproc_bytecode) {
      fdtype cur = (fdtype)(proc->sproc_bytecode);
      fd_decref(cur);}
    fd_incref(bytecode);
    proc->sproc_bytecode = (struct FD_VECTOR *)bytecode;
    return VOID;}
  else return fd_type_error
	 ("compound procedure","set_compound_procedure_bytecode",x);
}

static fdtype set_compound_procedure_optimizer(fdtype arg,fdtype optimizer)
{
  fdtype x = fd_fcnid_ref(arg);
  if (FD_SPROCP(x)) {
    struct FD_SPROC *proc = (fd_sproc)x;
    if (proc->sproc_optimizer) {
      fdtype cur = (fdtype)(proc->sproc_optimizer);
      fd_decref(cur);}
    fd_incref(optimizer);
    proc->sproc_optimizer = optimizer;
    return VOID;}
  else return fd_type_error
	 ("compound procedure","set_compound_procedure_optimizer",x);
}

/* Function IDs */

static fdtype fcnid_refprim(fdtype arg)
{
  fdtype result = fd_fcnid_ref(arg);
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
  if (PAIRP(expr)) {
    if (FD_TYPEP(expander,fd_macro_type)) {
      struct FD_MACRO *macrofn = (struct FD_MACRO *)fd_fcnid_ref(expander);
      fd_ptr_type xformer_type = FD_PTR_TYPE(macrofn->macro_transformer);
      if (fd_applyfns[xformer_type]) {
        /* These are evalfns which do all the evaluating themselves */
        fdtype new_expr=
          fd_dcall(fd_stackptr,fd_fcnid_ref(macrofn->macro_transformer),1,&expr);
        new_expr = fd_finish_call(new_expr);
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
  u8_string seeking; fdtype all, results = EMPTY;
  if (SYMBOLP(arg)) seeking = SYM_NAME(arg);
  else if (STRINGP(arg)) seeking = CSTRING(arg);
  else return fd_type_error(_("string or symbol"),"apropos",arg);
  all = fd_all_symbols();
  {DO_CHOICES(sym,all) {
    u8_string name = SYM_NAME(sym);
    if (strstr(name,seeking)) {CHOICE_ADD(results,sym);}}}
  return results;
}

/* Module bindings */

static fdtype module_bindings(fdtype arg)
{
  if (FD_LEXENVP(arg)) {
    fd_lexenv envptr = fd_consptr(fd_lexenv,arg,fd_lexenv_type);
    return fd_getkeys(envptr->env_bindings);}
  else if (TABLEP(arg))
    return fd_getkeys(arg);
  else if (SYMBOLP(arg)) {
    fdtype module = fd_get_module(arg,1);
    if (VOIDP(module))
      return fd_type_error(_("module"),"module_bindings",arg);
    else {
      fdtype v = module_bindings(module);
      fd_decref(module);
      return v;}}
  else return fd_type_error(_("module"),"module_bindings",arg);
}

static fdtype modulep(fdtype arg)
{
  if (FD_LEXENVP(arg)) {
    struct FD_LEXENV *env=
      fd_consptr(struct FD_LEXENV *,arg,fd_lexenv_type);
    if (fd_test(env->env_bindings,moduleid_symbol,VOID))
      return FD_TRUE;
    else return FD_FALSE;}
  else if ((HASHTABLEP(arg)) || (SLOTMAPP(arg)) || (SCHEMAPP(arg))) {
    if (fd_test(arg,moduleid_symbol,VOID))
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static fdtype module_exports(fdtype arg)
{
  if (FD_LEXENVP(arg)) {
    fd_lexenv envptr = fd_consptr(fd_lexenv,arg,fd_lexenv_type);
    return fd_getkeys(envptr->env_exports);}
  else if (TABLEP(arg))
    return fd_getkeys(arg);
  else if (SYMBOLP(arg)) {
    fdtype module = fd_get_module(arg,1);
    if (VOIDP(module))
      return fd_type_error(_("module"),"module_exports",arg);
    else {
      fdtype v = module_exports(module);
      fd_decref(module);
      return v;}}
  else return fd_type_error(_("module"),"module_exports",arg);
}

static fdtype local_bindings_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  if (env->env_copy)
    return fd_incref(env->env_copy->env_bindings);
  else {
    fd_lexenv copied = fd_copy_env(env);
    fdtype bindings = copied->env_bindings;
    fd_incref(bindings);
    fd_decref((fdtype)copied);
    return bindings;}
}

static fdtype thisenv_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  return (fdtype) fd_copy_env(env);
}

/* Finding where a symbol comes from */

static fdtype wherefrom_evalfn(fdtype expr,fd_lexenv call_env,fd_stack _stack)
{
  fdtype symbol_arg = fd_get_arg(expr,1);
  fdtype symbol = fd_eval(symbol_arg,call_env);
  if (SYMBOLP(symbol)) {
    fd_lexenv env;
    fdtype env_arg = fd_eval(fd_get_arg(expr,2),call_env);
    if (VOIDP(env_arg)) env = call_env;
    else if (FD_TYPEP(env_arg,fd_lexenv_type))
      env = fd_consptr(fd_lexenv,env_arg,fd_lexenv_type);
    else return fd_type_error(_("environment"),"wherefrom",env_arg);
    if (env->env_copy) env = env->env_copy;
    while (env) {
      if (fd_test(env->env_bindings,symbol,VOID)) {
        fdtype bindings = env->env_bindings;
        if ((CONSP(bindings)) &&
            (FD_MALLOCD_CONSP((fd_cons)bindings)))
          return fd_incref((fdtype)env);
        else {
          fd_decref(env_arg);
          return FD_FALSE;}}
      env = env->env_parent;
      if ((env) && (env->env_copy)) env = env->env_copy;}
    fd_decref(env_arg);
    return FD_FALSE;}
  else return fd_type_error(_("symbol"),"wherefrom",symbol);
}

/* Finding all the modules used from an environment */

static fdtype getmodules_evalfn(fdtype expr,fd_lexenv call_env,fd_stack _stack)
{
  fdtype env_arg = fd_eval(fd_get_arg(expr,1),call_env), modules = EMPTY;
  fd_lexenv env = call_env;
  if (VOIDP(env_arg)) {}
  else if (FD_TYPEP(env_arg,fd_lexenv_type))
    env = fd_consptr(fd_lexenv,env_arg,fd_lexenv_type);
  else return fd_type_error(_("environment"),"wherefrom",env_arg);
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (fd_test(env->env_bindings,moduleid_symbol,VOID)) {
      fdtype ids = fd_get(env->env_bindings,moduleid_symbol,VOID);
      if ((CHOICEP(ids))||(PRECHOICEP(ids))) {
        DO_CHOICES(id,ids) {
          if (SYMBOLP(id)) {CHOICE_ADD(modules,id);}}}
      else if (SYMBOLP(ids)) {CHOICE_ADD(modules,ids);}
      else {}}
    env = env->env_parent;
    if ((env) && (env->env_copy)) env = env->env_copy;}
  fd_decref(env_arg);
  return modules;
}

/* Initialization */

FD_EXPORT void fd_init_reflection_c()
{
  fdtype module = fd_new_module("REFLECTION",FD_MODULE_SAFE);

  fdtype apropos_cprim = fd_make_cprim1("APROPOS",apropos_prim,1);
  fd_idefn(module,apropos_cprim);
  fd_defn(fd_scheme_module,apropos_cprim);

  moduleid_symbol = fd_intern("%MODULEID");

  fd_idefn1(module,"MACRO?",macrop,1,
            "Returns true if its argument is an evaluator macro",
            -1,VOID);
  fd_idefn1(module,"APPLICABLE?",applicablep,1,
            "Returns true if its argument is applicable "
            "(can be passed to apply, used as a function, etc",
            -1,VOID);

  fd_idefn1(module,"COMPOUND-PROCEDURE?",compound_procedurep,1,
            "",
            -1,VOID);
  fd_idefn1(module,"SPECIAL-FORM?",evalfnp,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE?",procedurep,1,
            "",
            -1,VOID);

  fd_idefn1(module,"PRIMITIVE?",primitivep,1,
            "",
            -1,VOID);
  fd_idefn1(module,"NON-DETERMINISTIC?",non_deterministicp,1,
            "",
            -1,VOID);
  fd_idefn1(module,"SYNCHRONIZED?",synchronizedp,1,
            "",
            -1,VOID);
  fd_idefn1(module,"MODULE?",modulep,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-NAME",procedure_name,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-FILENAME",procedure_filename,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-ARITY",procedure_arity,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-MIN-ARITY",procedure_min_arity,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-SYMBOL",procedure_symbol,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-DOCUMENTATION",
            procedure_documentation,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-ID",procedure_id,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-ARGS",compound_procedure_args,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-BODY",compound_procedure_body,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-SOURCE",compound_procedure_source,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-ENV",compound_procedure_env,1,
            "",
            -1,VOID);
  fd_idefn1(module,"PROCEDURE-BYTECODE",
            compound_procedure_bytecode,1,
            "",
            -1,VOID);

  fd_idefn2(module,"REFLECT/GET",reflect_get,2,
            "Returns a meta-property of a procedure",
            -1,VOID,-1,VOID);
  fd_idefn(module,fd_make_cprim3("REFLECT/STORE!",reflect_store,3));
  fd_idefn(module,fd_make_cprim3("REFLECT/ADD!",reflect_add,3));
  fd_idefn(module,fd_make_cprim3("REFLECT/DROP!",reflect_drop,2));
  fd_idefn(module,fd_make_cprim1("REFLECT/ATTRIBS",get_procedure_attribs,1));
  fd_idefn(module,fd_make_cprim2("REFLECT/SET-ATTRIBS!",
                                 set_procedure_attribs,2));
  fd_idefn2(module,"SET-PROCEDURE-DOCUMENTATION!",
            set_procedure_documentation,2,
            "",
            -1,VOID,fd_string_type,VOID);

  fd_idefn2(module,"SET-PROCEDURE-BODY!",set_compound_procedure_body,2,
            "",fd_sproc_type,VOID,-1,VOID);
  fd_idefn2(module,"SET-PROCEDURE-ARGS!",set_compound_procedure_args,2,
           "",fd_sproc_type,VOID,-1,VOID);
  fd_idefn2(module,"SET-PROCEDURE-SOURCE!",set_compound_procedure_source,2,
            "",fd_sproc_type,VOID,-1,VOID);
  fd_idefn2(module,"SET-PROCEDURE-OPTIMIZER!",
            set_compound_procedure_optimizer,2,
            "",fd_sproc_type,VOID,-1,VOID);
  fd_idefn2(module,"SET-PROCEDURE-BYTECODE!",set_compound_procedure_bytecode,2,
            "",fd_sproc_type,VOID,fd_code_type,VOID);

  fd_idefn(module,fd_make_cprim2("MACROEXPAND",macroexpand,2));

  fd_idefn1(module,"FCNID/REF",fcnid_refprim,1,
            "",fd_fcnid_type,VOID);
  fd_idefn1(module,"FCNID/REGISTER",fcnid_registerprim,1,
            "",-1,VOID);
  fd_idefn2(module,"FCNID/SET!",fcnid_setprim,1,
            "",fd_fcnid_type,VOID,-1,VOID);

  fd_idefn1(module,"MODULE-BINDINGS",module_bindings,1,
            "Returns the bindings table for a module's environment",
            -1,VOID);
  fd_idefn1(module,"MODULE-EXPORTS",module_exports,1,
            "Returns the exports table for a module",
            -1,VOID);

  fd_defspecial(module,"%ENV",thisenv_evalfn);
  fd_defspecial(module,"%BINDINGS",local_bindings_evalfn);

  fd_defspecial(module,"WHEREFROM",wherefrom_evalfn);
  fd_defspecial(module,"GETMODULES",getmodules_evalfn);

  fd_finish_module(module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
