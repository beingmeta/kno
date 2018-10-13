/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/eval.h"
#include "framerd/profiles.h"

#include "libu8/u8streamio.h"
#include "libu8/u8printf.h"

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static lispval moduleid_symbol, source_symbol;

#define GETEVALFN(x) ((fd_evalfn)(fd_fcnid_ref(x)))

static lispval macrop(lispval x)
{
  if (TYPEP(x,fd_macro_type)) return FD_TRUE;
  else return FD_FALSE;
}

static lispval compound_procedurep(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_LAMBDAP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval applicablep(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_APPLICABLEP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval evalfnp(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (TYPEP(x,fd_evalfn_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval primitivep(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (TYPEP(x,fd_cprim_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval procedurep(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_FUNCTIONP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval procedure_name(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_FUNCTIONP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return lispval_string(f->fcn_name);
    else return FD_FALSE;}
  else if (FD_APPLICABLEP(x))
    return FD_FALSE;
  else if (TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return lispval_string(sf->evalfn_name);
    else return FD_FALSE;}
  else if (TYPEP(x,fd_macro_type)) {
    struct FD_MACRO *m = (fd_macro) x;
    if (m->macro_name)
      return lispval_string(m->macro_name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_name",x);
}

static lispval procedure_filename(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_FUNCTIONP(x)) {
    struct FD_FUNCTION *f = FD_XFUNCTION(x);
    if (f->fcn_filename)
      return lispval_string(f->fcn_filename);
    else return FD_FALSE;}
  else if (TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_filename)
      return lispval_string(sf->evalfn_filename);
    else return FD_FALSE;}
  else if (TYPEP(x,fd_macro_type)) {
    struct FD_MACRO *m = (fd_macro) x;
    if (m->macro_filename)
      return lispval_string(m->macro_filename);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_filename",x);
}

static lispval procedure_moduleid(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_FUNCTIONP(x)) {
    struct FD_FUNCTION *f = FD_XFUNCTION(x);
    lispval id = f->fcn_moduleid;
    if ( (FD_NULLP(id)) || (FD_VOIDP(id)) )
      return FD_FALSE;
    else return fd_incref(id);}
  else if (TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    lispval id = sf->evalfn_moduleid;
    if ( (FD_NULLP(id)) || (FD_VOIDP(id)) )
      return FD_FALSE;
    else return fd_incref(id);}
  else if (TYPEP(x,fd_macro_type)) {
    struct FD_MACRO *m = (fd_macro) x;
    lispval id = m->macro_moduleid;
    if ( (FD_NULLP(id)) || (FD_VOIDP(id)) )
      return FD_FALSE;
    else return fd_incref(id);}
  else return fd_type_error(_("function"),"procedure_module",x);
}

static lispval procedure_symbol(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return fd_intern(f->fcn_name);
    else return FD_FALSE;}
  else if (TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return fd_intern(sf->evalfn_name);
    else return FD_FALSE;}
  else if (TYPEP(x,fd_macro_type)) {
    struct FD_MACRO *m = (fd_macro) x;
    if (m->macro_name)
      return fd_intern(m->macro_name);
    else return FD_FALSE;}
  else return fd_type_error(_("function"),"procedure_symbol",x);
}

static lispval procedure_id(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_name)
      return fd_intern(f->fcn_name);
    else return fd_incref(x);}
  else if (TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return fd_intern(sf->evalfn_name);
    else return fd_incref(x);}
  else return fd_incref(x);
}

static lispval procedure_documentation(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  u8_string doc = fd_get_documentation(x);
  if (doc)
    return fd_lispstring(doc);
  else return FD_FALSE;
}

static lispval set_procedure_documentation(lispval x,lispval doc)
{
  lispval proc = (FD_FCNIDP(x)) ? (fd_fcnid_ref(x)) : (x);
  fd_ptr_type proctype = FD_PTR_TYPE(proc);
  if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if ( (f->fcn_doc) && (f->fcn_freedoc) )
      u8_free(f->fcn_doc);
    f->fcn_doc = u8_strdup(CSTRING(doc));
    f->fcn_freedoc = 1;
    return VOID;}
  else if (TYPEP(proc,fd_evalfn_type)) {
    struct FD_EVALFN *sf = GETEVALFN(proc);
    if (sf->evalfn_documentation) u8_free(sf->evalfn_documentation);
    sf->evalfn_documentation = u8_strdup(CSTRING(doc));
    return VOID;}
  else return fd_err("Not Handled","set_procedure_documentation",NULL,x);
}

static lispval procedure_tailablep(lispval x)
{
  lispval proc = (FD_FCNIDP(x)) ? (fd_fcnid_ref(x)) : (x);
  fd_ptr_type proctype = FD_PTR_TYPE(proc);
  if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_notail)
      return FD_FALSE;
    else return FD_TRUE;}
  else return fd_err("Not Handled","procedure_tailablep",NULL,x);
}
static lispval set_procedure_tailable(lispval x,lispval bool)
{
  lispval proc = (FD_FCNIDP(x)) ? (fd_fcnid_ref(x)) : (x);
  fd_ptr_type proctype = FD_PTR_TYPE(proc);
  if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (FD_FALSEP(bool))
      f->fcn_notail = 1;
    else f->fcn_notail = 0;
    return VOID;}
  else return fd_err("Not Handled","set_procedure_tailable",NULL,x);
}

static lispval procedure_arity(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    int arity = f->fcn_arity;
    if (arity<0) return FD_FALSE;
    else return FD_INT(arity);}
  else return fd_type_error(_("procedure"),"procedure_arity",x);
}

static lispval non_deterministicp(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    if (f->fcn_ndcall)
      return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error(_("procedure"),"non_deterministicp",x);
}

static lispval synchronizedp(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (TYPEP(x,fd_lambda_type)) {
    fd_lambda f = (fd_lambda)x;
    if (f->lambda_synchronized)
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_APPLICABLEP(x))
    return FD_FALSE;
  else return fd_type_error(_("procedure"),"non_deterministicp",x);
}

static lispval procedure_min_arity(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  if (FD_APPLICABLEP(x)) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    int arity = f->fcn_min_arity;
    return FD_INT(arity);}
  else return fd_type_error(_("procedure"),"procedure_min_arity",x);
}

/* Procedure attribs */


static lispval get_proc_attribs(lispval x,int create)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  fd_ptr_type proctype = FD_PTR_TYPE(x);
  if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    lispval attribs = f->fcn_attribs;
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

static lispval get_procedure_attribs(lispval x)
{
  if (FD_FCNIDP(x)) x = fd_fcnid_ref(x);
  lispval attribs = get_proc_attribs(x,1);
  if (FD_ABORTP(attribs)) return attribs;
  else fd_incref(attribs);
  return attribs;
}

static lispval set_procedure_attribs(lispval x,lispval value)
{
  fd_ptr_type proctype = FD_PTR_TYPE(x);
  if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(x);
    lispval table = f->fcn_attribs;
    if (table!=FD_NULL) fd_decref(table);
    f->fcn_attribs = fd_incref(value);
    return VOID;}
  else return fd_err("Not Handled","set_procedure_documentation",
                     NULL,x);
}

static lispval reflect_get(lispval x,lispval attrib)
{
  lispval attribs = get_proc_attribs(x,0);
  if (TABLEP(attribs))
    return fd_get(attribs,attrib,FD_FALSE);
  else return FD_FALSE;
}

static lispval reflect_store(lispval x,lispval attrib,lispval value)
{
  lispval attribs = get_proc_attribs(x,1);
  if (FD_ABORTP(attribs))
    return attribs;
  else if (TABLEP(attribs)) {
    int rv = fd_store(attribs,attrib,value);
    if (rv<0) return FD_ERROR;
    else if (rv==0) return FD_FALSE;
    else return FD_INT(rv);}
  else return FD_ERROR;
}

static lispval reflect_add(lispval x,lispval attrib,lispval value)
{
  lispval attribs = get_proc_attribs(x,1);
  if (FD_ABORTP(attribs))
    return attribs;
  else if (TABLEP(attribs)) {
    int rv = fd_add(attribs,attrib,value);
    if (rv<0) return FD_ERROR;
    else if (rv==0) return FD_FALSE;
    else return FD_INT(rv);}
  else return FD_ERROR;
}

static lispval reflect_drop(lispval x,lispval attrib,lispval value)
{
  lispval attribs = get_proc_attribs(x,1);
  if (FD_ABORTP(attribs))
    return attribs;
  else if (TABLEP(attribs)) {
    int rv = fd_drop(attribs,attrib,value);
    if (rv<0) return FD_ERROR;
    else if (rv==0) return FD_FALSE;
    else return FD_INT(rv);}
  else return FD_ERROR;
}

/* LAMBDA functions */

static lispval compound_procedure_args(lispval arg)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)x;
    return fd_incref(proc->lambda_arglist);}
  else return fd_type_error
         ("compound procedure","compound_procedure_args",x);
}

static lispval set_compound_procedure_args(lispval arg,lispval new_arglist)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)fd_fcnid_ref(x);
    lispval arglist = proc->lambda_arglist;
    proc->lambda_arglist = fd_incref(new_arglist);
    fd_decref(arglist);
    return VOID;}
  else return fd_type_error
         ("compound procedure","set_compound_procedure_args",x);
}

static lispval compound_procedure_env(lispval arg)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)fd_fcnid_ref(x);
    return (lispval) fd_copy_env(proc->lambda_env);}
  else return fd_type_error("compound procedure","compound_procedure_env",x);
}

static lispval compound_procedure_body(lispval arg)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)fd_fcnid_ref(x);
    return fd_incref(proc->lambda_body);}
  else return fd_type_error
         ("compound procedure","compound_procedure_body",x);
}

static lispval compound_procedure_source(lispval arg)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)fd_fcnid_ref(x);
    if (VOIDP(proc->lambda_source))
      return FD_FALSE;
    else return fd_incref(proc->lambda_source);}
  else return fd_type_error
         ("compound procedure","compound_procedure_source",x);
}

static lispval set_compound_procedure_body(lispval arg,lispval new_body)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)fd_fcnid_ref(x);
    lispval body = proc->lambda_body;
    proc->lambda_body = fd_incref(new_body);
    fd_decref(body);
    return VOID;}
  else return fd_type_error
         ("compound procedure","set_compound_procedure_body",x);
}

static lispval set_compound_procedure_source(lispval arg,lispval new_source)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)fd_fcnid_ref(x);
    lispval source = proc->lambda_source;
    proc->lambda_source = fd_incref(new_source);
    fd_decref(source);
    return VOID;}
  else return fd_type_error
         ("compound procedure","set_compound_procedure_source",x);
}

static lispval compound_procedure_bytecode(lispval arg)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)fd_fcnid_ref(x);
    if (proc->lambda_bytecode) {
      lispval cur = (lispval)(proc->lambda_bytecode);
      fd_incref(cur);
      return cur;}
    else return FD_FALSE;}
  else return fd_type_error
         ("compound procedure","compound_procedure_body",x);
}

static lispval set_compound_procedure_bytecode(lispval arg,lispval bytecode)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)fd_fcnid_ref(x);
    if (proc->lambda_bytecode) {
      lispval cur = (lispval)(proc->lambda_bytecode);
      fd_decref(cur);}
    fd_incref(bytecode);
    proc->lambda_bytecode = (struct FD_VECTOR *)bytecode;
    return VOID;}
  else return fd_type_error
         ("compound procedure","set_compound_procedure_bytecode",x);
}

static lispval set_compound_procedure_optimizer(lispval arg,lispval optimizer)
{
  lispval x = fd_fcnid_ref(arg);
  if (FD_LAMBDAP(x)) {
    struct FD_LAMBDA *proc = (fd_lambda)x;
    if (proc->lambda_optimizer) {
      lispval cur = (lispval)(proc->lambda_optimizer);
      fd_decref(cur);}
    fd_incref(optimizer);
    proc->lambda_optimizer = optimizer;
    return VOID;}
  else return fd_type_error
         ("compound procedure","set_compound_procedure_optimizer",x);
}

/* Function IDs */

static lispval fcnid_refprim(lispval arg)
{
  lispval result = fd_fcnid_ref(arg);
  fd_incref(result);
  return result;
}

static lispval fcnid_registerprim(lispval value)
{
  if (FD_FCNIDP(value))
    return value;
  else return fd_register_fcnid(value);
}

static lispval fcnid_setprim(lispval arg,lispval value)
{
  return fd_set_fcnid(arg,value);
}

/* Macro expand */

static lispval macroexpand(lispval expander,lispval expr)
{
  if (PAIRP(expr)) {
    if (TYPEP(expander,fd_macro_type)) {
      struct FD_MACRO *macrofn = (struct FD_MACRO *)fd_fcnid_ref(expander);
      fd_ptr_type xformer_type = FD_PTR_TYPE(macrofn->macro_transformer);
      if (fd_applyfns[xformer_type]) {
        /* These are evalfns which do all the evaluating themselves */
        lispval new_expr=
          fd_dcall(fd_stackptr,fd_fcnid_ref(macrofn->macro_transformer),1,&expr);
        new_expr = fd_finish_call(new_expr);
        if (FD_ABORTP(new_expr))
          return fd_err(fd_SyntaxError,_("macro expansion"),NULL,new_expr);
        else return new_expr;}
      else return fd_err(fd_InvalidMacro,NULL,macrofn->macro_name,expr);}
    else return fd_type_error("macro","macroexpand",expander);}
  else return fd_incref(expr);
}

/* Apropos */

static lispval apropos_prim(lispval arg)
{
  u8_string seeking; lispval all, results = EMPTY;
  if (SYMBOLP(arg)) seeking = SYM_NAME(arg);
  else if (STRINGP(arg)) seeking = CSTRING(arg);
  else return fd_type_error(_("string or symbol"),"apropos",arg);
  all = fd_all_symbols();
  {DO_CHOICES(sym,all) {
    u8_string name = SYM_NAME(sym);
    if (strstr(name,seeking)) {CHOICE_ADD(results,sym);}}}
  fd_decref(all);
  return results;
}

/* Module bindings */

static lispval module_bindings(lispval arg)
{
  if (FD_LEXENVP(arg)) {
    fd_lexenv envptr = fd_consptr(fd_lexenv,arg,fd_lexenv_type);
    return fd_getkeys(envptr->env_bindings);}
  else if (TABLEP(arg))
    return fd_getkeys(arg);
  else return fd_type_error(_("module"),"module_bindings",arg);
}

static lispval module_getsource(lispval arg)
{
  lispval ids = FD_EMPTY;
  if (FD_LEXENVP(arg)) {
    fd_lexenv envptr = fd_consptr(fd_lexenv,arg,fd_lexenv_type);
    ids = fd_get(envptr->env_bindings,source_symbol,FD_VOID);
    if (FD_VOIDP(ids))
      ids = fd_get(envptr->env_bindings,moduleid_symbol,FD_VOID);}
  else if (TABLEP(arg)) {
    ids = fd_get(arg,source_symbol,FD_VOID);
    if (FD_VOIDP(ids))
      ids = fd_get(arg,moduleid_symbol,FD_VOID);}
  else return fd_type_error(_("module"),"module_bindings",arg);
  if (FD_VOIDP(ids))
    return FD_FALSE;
  else {
    FD_DO_CHOICES(id,ids) {
      if (FD_STRINGP(id)) {
        fd_incref(id);
        fd_decref(ids);
        FD_STOP_DO_CHOICES;
        return id;}}
    return FD_FALSE;}
}

static lispval modulep(lispval arg)
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

static lispval module_exports(lispval arg)
{
  if (FD_LEXENVP(arg)) {
    fd_lexenv envptr = fd_consptr(fd_lexenv,arg,fd_lexenv_type);
    return fd_getkeys(envptr->env_exports);}
  else if (TABLEP(arg))
    return fd_getkeys(arg);
  else if (SYMBOLP(arg)) {
    lispval module = fd_find_module(arg,0,1);
    if (FD_ABORTP(module))
      return module;
    else if (VOIDP(module))
      return fd_type_error(_("module"),"module_exports",arg);
    else {
      lispval v = module_exports(module);
      fd_decref(module);
      return v;}}
  else return fd_type_error(_("module"),"module_exports",arg);
}

static lispval local_bindings_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  if (env->env_copy)
    return fd_incref(env->env_copy->env_bindings);
  else {
    fd_lexenv copied = fd_copy_env(env);
    lispval bindings = copied->env_bindings;
    fd_incref(bindings);
    fd_decref((lispval)copied);
    return bindings;}
}

static lispval thisenv_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return (lispval) fd_copy_env(env);
}

/* Finding where a symbol comes from */

static lispval wherefrom_evalfn(lispval expr,fd_lexenv call_env,fd_stack _stack)
{
  lispval symbol_arg = fd_get_arg(expr,1);
  lispval symbol = fd_eval(symbol_arg,call_env);
  if (SYMBOLP(symbol)) {
    fd_lexenv env, scan;
    lispval env_arg = fd_eval(fd_get_arg(expr,2),call_env);
    if (VOIDP(env_arg)) env = call_env;
    else if (TYPEP(env_arg,fd_lexenv_type))
      env = fd_consptr(fd_lexenv,env_arg,fd_lexenv_type);
    else return fd_type_error(_("environment"),"wherefrom",env_arg);
    if (env->env_copy)
      scan = env->env_copy;
    else scan = env;
    while (scan) {
      if (fd_test(scan->env_bindings,symbol,VOID)) {
        lispval bindings = scan->env_bindings;
        if ((CONSP(bindings)) &&
            (FD_MALLOCD_CONSP((fd_cons)bindings))) {
          fd_decref(env_arg);
          return fd_incref((lispval)scan);}
        else {
          fd_decref(env_arg);
          return FD_FALSE;}}
      scan = scan->env_parent;
      if ((scan) && (scan->env_copy))
        scan = scan->env_copy;}
    fd_decref(env_arg);
    return FD_FALSE;}
  else return fd_type_error(_("symbol"),"wherefrom",symbol);
}

/* Finding all the modules used from an environment */

static lispval getmodules_evalfn(lispval expr,fd_lexenv call_env,fd_stack _stack)
{
  lispval env_arg = fd_eval(fd_get_arg(expr,1),call_env), modules = EMPTY;
  fd_lexenv env = call_env;
  if (VOIDP(env_arg)) {}
  else if (TYPEP(env_arg,fd_lexenv_type))
    env = fd_consptr(fd_lexenv,env_arg,fd_lexenv_type);
  else return fd_type_error(_("environment"),"wherefrom",env_arg);
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (fd_test(env->env_bindings,moduleid_symbol,VOID)) {
      lispval ids = fd_get(env->env_bindings,moduleid_symbol,VOID);
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

/* Profiling */

static lispval profile_fcn_prim(lispval fcn,lispval bool)
{
  if (FD_FUNCTIONP(fcn)) {
    struct FD_FUNCTION *f = FD_XFUNCTION(fcn);
    if (FD_FALSEP(bool)) {
      struct FD_PROFILE *profile = f->fcn_profile;
      if (profile)
        return FD_FALSE;
      else {
        f->fcn_profile = NULL;
        u8_free(profile);}}
    else f->fcn_profile = fd_make_profile(f->fcn_name);
    return FD_TRUE;}
  else return fd_type_error("function","profile_fcn",fcn);
}

static lispval profile_reset_prim(lispval fcn)
{
  if (FD_FUNCTIONP(fcn)) {
    struct FD_FUNCTION *f = FD_XFUNCTION(fcn);
    struct FD_PROFILE *profile = f->fcn_profile;
    if (profile == NULL) return FD_FALSE;
#if HAVE_STDATOMIC_H
    profile->prof_calls = ATOMIC_VAR_INIT(0);
    profile->prof_items = ATOMIC_VAR_INIT(0);
    profile->prof_nsecs = ATOMIC_VAR_INIT(0);
#else
    u8_lock_mutex(&(profile->prof_lock));
    profile->prof_calls = 0;
    profile->prof_items = 0;
    profile->prof_nsecs = 0;
    u8_unlock_mutex(&(profile->prof_lock));
#endif
    return FD_TRUE;}
  else return fd_type_error("function","profile_fcn",fcn);
}

static lispval profiledp_prim(lispval fcn)
{
  if (FD_FUNCTIONP(fcn)) {
    struct FD_FUNCTION *f = FD_XFUNCTION(fcn);
    if (f->fcn_profile)
      return FD_TRUE;
    else return FD_FALSE;}
  else return FD_FALSE;
}

static lispval call_profile_symbol;

static lispval getcalls_prim(lispval fcn)
{
  if (FD_FUNCTIONP(fcn)) {
    struct FD_FUNCTION *f = FD_XFUNCTION(fcn);
    struct FD_PROFILE *p = f->fcn_profile;
    if (p==NULL) return FD_FALSE;
#if HAVE_STDATOMIC_H
    long long calls = atomic_load(&(p->prof_calls));
    long long items = atomic_load(&(p->prof_items));
    long long nsecs = atomic_load(&(p->prof_nsecs));
    long long user_nsecs = atomic_load(&(p->prof_nsecs_user));
    long long system_nsecs = atomic_load(&(p->prof_nsecs_system));
    long long n_waits = atomic_load(&(p->prof_n_waits));
    long long n_contests = atomic_load(&(p->prof_n_contests));
    long long n_faults = atomic_load(&(p->prof_n_faults));
#else
    long long calls = p->prof_calls;
    long long items = p->prof_items;
    long long nsecs = p->prof_nsecs;
    long long user_nsecs = p->prof_nsecs_user;
    long long system_nsecs = p->prof_nsecs_system;
    long long n_waits = prof_n_waits;
    long long n_contests = prof_n_waits;
    long long n_faults = p->prof_n_faults;
#endif
    double exec_time = ((double)((nsecs)|(calls)))/1000000000.0;
    double user_time = ((double)((user_nsecs)|(calls)))/1000000000.0;
    double system_time = ((double)((system_nsecs)|(calls)))/1000000000.0;
    fd_incref(fcn);
    return fd_init_compound
      (NULL,call_profile_symbol,FD_COMPOUND_USEREF,10,
       fcn,fd_make_flonum(exec_time),
       FD_INT(user_time),FD_INT(system_time),
       FD_INT(n_waits),FD_INT(n_contests),FD_INT(n_faults),
       FD_INT(nsecs),FD_INT(calls),FD_INT(items));}
  else return fd_type_error("function","profile_fcn",fcn);
}

/* Accessors */

static lispval profile_getfcn(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,0);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_getfcn",profile);
}
static lispval profile_gettime(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,1);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_gettime",profile);
}
static lispval profile_getutime(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,2);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_getutime",profile);
}
static lispval profile_getstime(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,3);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_getstime",profile);
}
static lispval profile_getwaits(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,4);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_getwaits",profile);
}
static lispval profile_getcontests(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,5);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_getcontests",profile);
}
static lispval profile_getfaults(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,6);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_getfaults",profile);
}
static lispval profile_nsecs(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,7);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_nsecs",profile);
}
static lispval profile_ncalls(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,8);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_ncalls",profile);
}
static lispval profile_nitems(lispval profile)
{
  if (FD_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct FD_COMPOUND *p = (fd_compound) profile;
    lispval v = FD_COMPOUND_VREF(p,9);
    return fd_incref(v);}
  else return fd_type_error("call profile","profile_nitems",profile);
}

/* Getting all modules */

static lispval get_all_modules_prim()
{
  return fd_all_modules();
}

/* Initialization */

FD_EXPORT void fd_init_reflection_c()
{
  lispval module =
    fd_new_cmodule("REFLECTION",FD_MODULE_SAFE,fd_init_reflection_c);

  lispval apropos_cprim = fd_make_cprim1("APROPOS",apropos_prim,1);
  fd_idefn(module,apropos_cprim);
  fd_defn(fd_scheme_module,apropos_cprim);

  moduleid_symbol = fd_intern("%MODULEID");
  source_symbol = fd_intern("%SOURCE");
  call_profile_symbol = fd_intern("%CALLPROFILE");

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
  fd_idefn1(module,"PROCEDURE-MODULE",procedure_moduleid,1,
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
  fd_idefn1(module,"PROCEDURE-TAILABLE?",
            procedure_tailablep,1,
            "`(PROCEDURE-TAILABLE? *fcn*)` "
            "Returns true if *fcn* can be tail called, and "
            "false otherwise. By default, all procedures "
            "are tailable when called deterministically.",
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
  fd_idefn2(module,"SET-PROCEDURE-TAILABLE!",
            set_procedure_tailable,2,
            "",
            -1,VOID,-1,VOID);

  fd_idefn2(module,"SET-PROCEDURE-BODY!",set_compound_procedure_body,2,
            "",fd_lambda_type,VOID,-1,VOID);
  fd_idefn2(module,"SET-PROCEDURE-ARGS!",set_compound_procedure_args,2,
           "",fd_lambda_type,VOID,-1,VOID);
  fd_idefn2(module,"SET-PROCEDURE-SOURCE!",set_compound_procedure_source,2,
            "",fd_lambda_type,VOID,-1,VOID);
  fd_idefn2(module,"SET-PROCEDURE-OPTIMIZER!",
            set_compound_procedure_optimizer,2,
            "",fd_lambda_type,VOID,-1,VOID);
  fd_idefn2(module,"SET-PROCEDURE-BYTECODE!",set_compound_procedure_bytecode,2,
            "",fd_lambda_type,VOID,fd_code_type,VOID);

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
  fd_idefn1(module,"MODULE-SOURCE",module_getsource,1,
            "Returns the source (a string) for a module",
            -1,VOID);

  fd_idefn0(module,"ALL-MODULES",get_all_modules_prim,
            "(ALL-MODULES) "
            "Returns all loaded modules as an alist "
            "of module names and modules");
  fd_idefn0(module,"SAFE-MODULES",get_all_modules_prim,
            "(SAFE-MODULES) "
            "Returns all 'safe' loaded modules as an alist"
            "of module names and modules");

  fd_idefn2(module,"REFLECT/PROFILE!",profile_fcn_prim,1,
            "`(REFLECT/PROFILE! *fcn* *boolean*)`"
            "Enables profiling for the function *fcn* or "
            "if *boolean* is false, disable profiling for *fcn*",
            -1,FD_VOID,-1,FD_TRUE);
  fd_idefn1(module,"REFLECT/PROFILED?",profiledp_prim,1,
            "`(REFLECT/PROFILED? *arg*)`Returns true if *arg* is being profiled, "
            "and false otherwise. It also returns false if the argument is "
            "not a function or not a function which supports profiling.",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/GETCALLS",getcalls_prim,1,
            "`(PROFILE/GETCALLS *fcn*)`Returns the profile information for "
            "*fcn*, a vector of *fcn*, the number of calls, the number of "
            "nanoseconds spent in *fcn*",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/RESET!",profile_reset_prim,1,
            "`(REFLECT/PROFILE-RESET! *fcn*)` resets the profile counts "
            "for *fcn*",
            -1,FD_VOID);

  fd_idefn1(module,"PROFILE/FCN",profile_getfcn,1,
            "`(PROFILE/FCN *profile*)` returns the function a profile describes",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/TIME",profile_gettime,1,
            "`(PROFILE/TIME *profile*)` returns the number of seconds "
            "spent in the profiled function",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/UTIME",profile_getutime,1,
            "`(PROFILE/ *profile*)` returns the number of seconds "
            "of user time spent in the profiled function",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/STIME",profile_getutime,1,
            "`(PROFILE/STIME *profile*)` returns the number of seconds "
            "of system time spent in the profiled function",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/STIME",profile_getstime,1,
            "`(PROFILE/STIME *profile*)` returns the number of seconds "
            "of system time spent in the profiled function",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/WAITS",profile_getwaits,1,
            "`(PROFILE/WAITS *profile*)` returns the number of voluntary "
            "context switches (usually when waiting for something) "
            "during the execution of the profiled function",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/CONTESTS",profile_getcontests,1,
            "`(PROFILE/CONTESTS *profile*)` returns the number of involuntary "
            "context switches (often indicating contested resources) during "
            "the execution of the profiled function",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/FAULTS",profile_getfaults,1,
            "`(PROFILE/FAULTS *profile*)` returns the number of page "
            "faults during the execution of the profiled function",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/NSECS",profile_nsecs,1,
            "`(PROFILE/FAULTS *profile*)` returns the number of calls "
            "to the profiled function",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/NCALLS",profile_ncalls,1,
            "`(PROFILE/FAULTS *profile*)` returns the number of calls "
            "to the profiled function",
            -1,FD_VOID);
  fd_idefn1(module,"PROFILE/NITEMS",profile_nitems,1,
            "`(PROFILE/FAULTS *profile*)` returns the number of items "
            "noted as processed by the profiled function",
            -1,FD_VOID);


  fd_def_evalfn(module,"%ENV","",thisenv_evalfn);
  fd_def_evalfn(module,"%BINDINGS","",local_bindings_evalfn);

  fd_def_evalfn(module,"WHEREFROM","",wherefrom_evalfn);
  fd_def_evalfn(module,"GETMODULES","",getmodules_evalfn);

  fd_finish_module(module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
