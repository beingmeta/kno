/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#define KNO_PROVIDE_FASTEVAL 1
#define KNO_INLINE_FCNIDS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/eval.h"
#include "kno/ffi.h"
#include "kno/profiles.h"

#include "libu8/u8streamio.h"
#include "libu8/u8printf.h"

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static lispval moduleid_symbol, source_symbol;

#define GETEVALFN(x) ((kno_evalfn)(kno_fcnid_ref(x)))

static lispval macrop(lispval x)
{
  if (TYPEP(x,kno_macro_type)) return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval lambdap(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_LAMBDAP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval evalfnp(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (TYPEP(x,kno_evalfn_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval primitivep(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (TYPEP(x,kno_cprim_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval procedurep(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval procedure_name(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    if (f->fcn_name)
      return kno_mkstring(f->fcn_name);
    else return KNO_FALSE;}
  else if (KNO_APPLICABLEP(x))
    return KNO_FALSE;
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return kno_mkstring(sf->evalfn_name);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_macro_type)) {
    struct KNO_MACRO *m = (kno_macro) x;
    if (m->macro_name)
      return kno_mkstring(m->macro_name);
    else return KNO_FALSE;}
  else return kno_type_error(_("function"),"procedure_name",x);
}

static lispval procedure_cname(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_TYPEP(x,kno_cprim_type)) {
    struct KNO_CPRIM *f = (kno_cprim) x;
    if (f->cprim_name)
      return knostring(f->cprim_name);
    else return KNO_FALSE;}
  else if (KNO_TYPEP(x,kno_ffi_type)) {
    struct KNO_FFI_PROC *f = (kno_ffi_proc) x;
    if (f->fcn_name)
      return knostring(f->fcn_name);
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

static lispval procedure_filename(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *f = KNO_XFUNCTION(x);
    if (f->fcn_filename)
      return kno_mkstring(f->fcn_filename);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_filename)
      return kno_mkstring(sf->evalfn_filename);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_macro_type)) {
    struct KNO_MACRO *m = (kno_macro) x;
    if (m->macro_filename)
      return kno_mkstring(m->macro_filename);
    else return KNO_FALSE;}
  else return kno_type_error(_("function"),"procedure_filename",x);
}

static lispval procedure_moduleid(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_FUNCTIONP(x)) {
    struct KNO_FUNCTION *f = KNO_XFUNCTION(x);
    lispval id = f->fcn_moduleid;
    if ( (KNO_NULLP(id)) || (KNO_VOIDP(id)) )
      return KNO_FALSE;
    else return kno_incref(id);}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    lispval id = sf->evalfn_moduleid;
    if ( (KNO_NULLP(id)) || (KNO_VOIDP(id)) )
      return KNO_FALSE;
    else return kno_incref(id);}
  else if (TYPEP(x,kno_macro_type)) {
    struct KNO_MACRO *m = (kno_macro) x;
    lispval id = m->macro_moduleid;
    if ( (KNO_NULLP(id)) || (KNO_VOIDP(id)) )
      return KNO_FALSE;
    else return kno_incref(id);}
  else return kno_type_error(_("function"),"procedure_module",x);
}

static lispval procedure_symbol(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    if (f->fcn_name)
      return kno_getsym(f->fcn_name);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return kno_getsym(sf->evalfn_name);
    else return KNO_FALSE;}
  else if (TYPEP(x,kno_macro_type)) {
    struct KNO_MACRO *m = (kno_macro) x;
    if (m->macro_name)
      return kno_getsym(m->macro_name);
    else return KNO_FALSE;}
  else return kno_type_error(_("function"),"procedure_symbol",x);
}

static lispval procedure_id(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    if (f->fcn_name)
      return kno_intern(f->fcn_name);
    else return kno_incref(x);}
  else if (TYPEP(x,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(x);
    if (sf->evalfn_name)
      return kno_intern(sf->evalfn_name);
    else return kno_incref(x);}
  else return kno_incref(x);
}

static lispval procedure_documentation(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  u8_string doc = kno_get_documentation(x);
  if (doc)
    return kno_wrapstring(doc);
  else return KNO_FALSE;
}

static lispval set_procedure_documentation(lispval x,lispval doc)
{
  lispval proc = (KNO_FCNIDP(x)) ? (kno_fcnid_ref(x)) : (x);
  kno_ptr_type proctype = KNO_PTR_TYPE(proc);
  if (kno_functionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    u8_string to_free = ( (f->fcn_doc) && (f->fcn_free_doc) ) ?
      (f->fcn_doc) : (NULL);
    f->fcn_doc = u8_strdup(CSTRING(doc));
    f->fcn_free_doc = 1;
    if (to_free) u8_free(to_free);
    return VOID;}
  else if (TYPEP(proc,kno_evalfn_type)) {
    struct KNO_EVALFN *sf = GETEVALFN(proc);
    u8_string prev = sf->evalfn_documentation;
    sf->evalfn_documentation = u8_strdup(CSTRING(doc));
    if (prev) u8_free(prev);
    return VOID;}
  else return kno_err("Not Handled","set_procedure_documentation",NULL,x);
}

static lispval procedure_tailablep(lispval x)
{
  lispval proc = (KNO_FCNIDP(x)) ? (kno_fcnid_ref(x)) : (x);
  kno_ptr_type proctype = KNO_PTR_TYPE(proc);
  if (kno_functionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    if (f->fcn_notail)
      return KNO_FALSE;
    else return KNO_TRUE;}
  else return kno_err("Not Handled","procedure_tailablep",NULL,x);
}
static lispval set_procedure_tailable(lispval x,lispval bool)
{
  lispval proc = (KNO_FCNIDP(x)) ? (kno_fcnid_ref(x)) : (x);
  kno_ptr_type proctype = KNO_PTR_TYPE(proc);
  if (kno_functionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    if (KNO_FALSEP(bool))
      f->fcn_notail = 1;
    else f->fcn_notail = 0;
    return VOID;}
  else return kno_err("Not Handled","set_procedure_tailable",NULL,x);
}

static lispval procedure_arity(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    int arity = f->fcn_arity;
    if (arity<0) return KNO_FALSE;
    else return KNO_INT(arity);}
  else return kno_type_error(_("procedure"),"procedure_arity",x);
}

static lispval non_deterministicp(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    if (f->fcn_ndcall)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return kno_type_error(_("procedure"),"non_deterministicp",x);
}

static lispval synchronizedp(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (TYPEP(x,kno_lambda_type)) {
    kno_lambda f = (kno_lambda)x;
    if (f->lambda_synchronized)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_APPLICABLEP(x))
    return KNO_FALSE;
  else return kno_type_error(_("procedure"),"non_deterministicp",x);
}

static lispval procedure_min_arity(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  if (KNO_APPLICABLEP(x)) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    int arity = f->fcn_min_arity;
    return KNO_INT(arity);}
  else return kno_type_error(_("procedure"),"procedure_min_arity",x);
}

/* Procedure attribs */


static lispval get_proc_attribs(lispval x,int create)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  kno_ptr_type proctype = KNO_PTR_TYPE(x);
  if (kno_functionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    lispval attribs = f->fcn_attribs;
    if (!(create)) {
      if ((attribs!=KNO_NULL)&&(TABLEP(attribs)))
        return attribs;
      else return VOID;}
    if ((attribs == KNO_NULL)||(!(TABLEP(attribs))))
      f->fcn_attribs = attribs = kno_init_slotmap(NULL,4,NULL);
    return attribs;}
  else return kno_type_error("function","get_proc_attribs",x);
}

static lispval get_procedure_attribs(lispval x)
{
  if (KNO_FCNIDP(x)) x = kno_fcnid_ref(x);
  lispval attribs = get_proc_attribs(x,1);
  if (KNO_ABORTP(attribs))
    return attribs;
  else kno_incref(attribs);
  return attribs;
}

static lispval set_procedure_attribs(lispval x,lispval value)
{
  kno_ptr_type proctype = KNO_PTR_TYPE(x);
  if (kno_functionp[proctype]) {
    struct KNO_FUNCTION *f = KNO_DTYPE2FCN(x);
    lispval table = f->fcn_attribs;
    if (table!=KNO_NULL) kno_decref(table);
    f->fcn_attribs = kno_incref(value);
    return VOID;}
  else return kno_err("Not Handled","set_procedure_documentation",
                     NULL,x);
}

static lispval reflect_get(lispval x,lispval attrib)
{
  lispval attribs = get_proc_attribs(x,0);
  if (ABORTP(attribs))
    return attribs;
  else if (TABLEP(attribs))
    return kno_get(attribs,attrib,KNO_FALSE);
  else return KNO_FALSE;
}

static lispval reflect_store(lispval x,lispval attrib,lispval value)
{
  lispval attribs = get_proc_attribs(x,1);
  if (KNO_ABORTP(attribs))
    return attribs;
  else if (TABLEP(attribs)) {
    int rv = kno_store(attribs,attrib,value);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_VOID;}
  else return KNO_ERROR;
}

static lispval reflect_add(lispval x,lispval attrib,lispval value)
{
  lispval attribs = get_proc_attribs(x,1);
  if (KNO_ABORTP(attribs))
    return attribs;
  else if (TABLEP(attribs)) {
    int rv = kno_add(attribs,attrib,value);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_VOID;}
  else return KNO_ERROR;
}

static lispval reflect_drop(lispval x,lispval attrib,lispval value)
{
  lispval attribs = get_proc_attribs(x,1);
  if (KNO_ABORTP(attribs))
    return attribs;
  else if (TABLEP(attribs)) {
    int rv = kno_drop(attribs,attrib,value);
    if (rv<0)
      return KNO_ERROR;
    else return KNO_VOID;}
  else return KNO_ERROR;
}

/* LAMBDA functions */

static lispval lambda_args(lispval arg)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)x;
    return kno_incref(proc->lambda_arglist);}
  else return kno_type_error
         ("lambda","lambda_args",x);
}

static lispval set_lambda_args(lispval arg,lispval new_arglist)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    lispval arglist = proc->lambda_arglist;
    proc->lambda_arglist = kno_incref(new_arglist);
    kno_decref(arglist);
    return VOID;}
  else return kno_type_error
         ("lambda","set_lambda_args",x);
}

static lispval lambda_env(lispval arg)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    return (lispval) kno_copy_env(proc->lambda_env);}
  else return kno_type_error("lambda","lambda_env",x);
}

static lispval lambda_body(lispval arg)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    return kno_incref(proc->lambda_body);}
  else return kno_type_error("lambda","lambda_body",x);
}

static lispval lambda_start(lispval arg)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    lispval start = proc->lambda_start;
    if ( (KNO_CONSP(start)) && (KNO_STATIC_CONSP(start)) )
      return kno_copier(start,KNO_DEEP_COPY);
    else return kno_incref(proc->lambda_start);}
  else return kno_type_error("lambda","lambda_start",x);
}

static lispval lambda_source(lispval arg)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    if (VOIDP(proc->lambda_source))
      return KNO_FALSE;
    else return kno_incref(proc->lambda_source);}
  else return kno_type_error("lambda","lambda_source",x);
}

static lispval set_lambda_body(lispval arg,lispval new_body)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    lispval old_body = proc->lambda_body;
    proc->lambda_body = kno_incref(new_body);
    kno_decref(old_body);
    if (proc->lambda_consblock) {
      lispval cb = (lispval) (proc->lambda_consblock);
      kno_decref(cb);
      proc->lambda_consblock = NULL;}
    proc->lambda_start = new_body;
    return VOID;}
  else return kno_type_error
         ("lambda","set_lambda_body",x);
}

static lispval optimize_lambda_body(lispval arg,lispval new_body)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)x;
    if (KNO_FALSEP(new_body)) {
      if (proc->lambda_consblock) {
        lispval cb = (lispval) (proc->lambda_consblock);
        proc->lambda_consblock = NULL;
        kno_decref(cb);}
      proc->lambda_start = proc->lambda_body;}
    else {
      lispval new_consblock = (KNO_TRUEP(new_body)) ?
        (kno_make_consblock(proc->lambda_body)) :
        (kno_make_consblock(new_body));
      if (KNO_ABORTP(new_consblock))
        return new_consblock;
      else if (KNO_TYPEP(new_consblock,kno_consblock_type)) {
        struct KNO_CONSBLOCK *cb = (kno_consblock) new_consblock;
        proc->lambda_start = cb->consblock_head;}
      else proc->lambda_start = new_consblock;
      if (proc->lambda_consblock) {
        lispval cb = (lispval) (proc->lambda_consblock);
        kno_decref(cb);}
      if (KNO_TYPEP(new_consblock,kno_consblock_type))
        proc->lambda_consblock = (kno_consblock) new_consblock;
      else proc->lambda_consblock = NULL;}
    return VOID;}
  else return kno_type_error("lambda","optimize_lambda_body",x);
}

static lispval optimize_lambda_args(lispval arg,lispval new_args)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *s = (kno_lambda)x;
    int n = kno_set_lambda_schema(s,new_args);
    if (n<0) return KNO_ERROR_VALUE;
    else return KNO_INT(n);}
  else return kno_type_error("lambda","optimize_lambda_args",x);
}

static lispval set_lambda_source(lispval arg,lispval new_source)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)kno_fcnid_ref(x);
    lispval source = proc->lambda_source;
    proc->lambda_source = kno_incref(new_source);
    kno_decref(source);
    return VOID;}
  else return kno_type_error("lambda","set_lambda_source",x);
}

static lispval set_lambda_optimizer(lispval arg,lispval optimizer)
{
  lispval x = kno_fcnid_ref(arg);
  if (KNO_LAMBDAP(x)) {
    struct KNO_LAMBDA *proc = (kno_lambda)x;
    if (proc->lambda_optimizer) {
      lispval cur = (lispval)(proc->lambda_optimizer);
      kno_decref(cur);}
    kno_incref(optimizer);
    proc->lambda_optimizer = optimizer;
    return VOID;}
  else return kno_type_error
         ("lambda","set_lambda_optimizer",x);
}

/* Function IDs */

static lispval fcnid_refprim(lispval arg)
{
  lispval result = kno_fcnid_ref(arg);
  kno_incref(result);
  return result;
}

static lispval fcnid_registerprim(lispval value)
{
  if (KNO_FCNIDP(value))
    return value;
  else return kno_register_fcnid(value);
}

static lispval fcnid_setprim(lispval arg,lispval value)
{
  return kno_set_fcnid(arg,value);
}

/* Macro expand */

static lispval macroexpand(lispval expander,lispval expr)
{
  if (PAIRP(expr)) {
    if (TYPEP(expander,kno_macro_type)) {
      struct KNO_MACRO *macrofn = (struct KNO_MACRO *)kno_fcnid_ref(expander);
      kno_ptr_type xformer_type = KNO_PTR_TYPE(macrofn->macro_transformer);
      if (kno_applyfns[xformer_type]) {
        /* These are evalfns which do all the evaluating themselves */
        lispval new_expr=
          kno_dcall(kno_stackptr,kno_fcnid_ref(macrofn->macro_transformer),1,&expr);
        new_expr = kno_finish_call(new_expr);
        if (KNO_ABORTP(new_expr))
          return kno_err(kno_SyntaxError,_("macro expansion"),NULL,new_expr);
        else return new_expr;}
      else return kno_err(kno_InvalidMacro,NULL,macrofn->macro_name,expr);}
    else return kno_type_error("macro","macroexpand",expander);}
  else return kno_incref(expr);
}

/* Apropos */

static lispval apropos_prim(lispval arg)
{
  u8_string seeking; lispval all, results = EMPTY;
  if (SYMBOLP(arg)) seeking = SYM_NAME(arg);
  else if (STRINGP(arg)) seeking = CSTRING(arg);
  else return kno_type_error(_("string or symbol"),"apropos",arg);
  all = kno_all_symbols();
  {DO_CHOICES(sym,all) {
    u8_string name = SYM_NAME(sym);
    if (strstr(name,seeking)) {CHOICE_ADD(results,sym);}}}
  kno_decref(all);
  return results;
}

/* Module bindings */

static lispval module_bindings(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    return kno_getkeys(envptr->env_bindings);}
  else if (TABLEP(arg))
    return kno_getkeys(arg);
  else return kno_type_error(_("module"),"module_bindings",arg);
}

static lispval module_getsource(lispval arg)
{
  lispval ids = KNO_EMPTY;
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    ids = kno_get(envptr->env_bindings,source_symbol,KNO_VOID);
    if (KNO_VOIDP(ids))
      ids = kno_get(envptr->env_bindings,moduleid_symbol,KNO_VOID);}
  else if (TABLEP(arg)) {
    ids = kno_get(arg,source_symbol,KNO_VOID);
    if (KNO_VOIDP(ids))
      ids = kno_get(arg,moduleid_symbol,KNO_VOID);}
  else return kno_type_error(_("module"),"module_bindings",arg);
  if (KNO_VOIDP(ids))
    return KNO_FALSE;
  else {
    KNO_DO_CHOICES(id,ids) {
      if (KNO_STRINGP(id)) {
        kno_incref(id);
        kno_decref(ids);
        KNO_STOP_DO_CHOICES;
        return id;}}
    return KNO_FALSE;}
}

static lispval module_table(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    if (KNO_TABLEP(envptr->env_exports))
      return kno_incref(envptr->env_exports);
    else return KNO_FALSE;}
  else if (TABLEP(arg))
    return kno_incref(arg);
  else return kno_type_error(_("module"),"module_table",arg);
}

static lispval module_environment(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    if ( ( (envptr->env_bindings) != (envptr->env_exports) ) &&
         (KNO_TABLEP(envptr->env_bindings)) )
      return kno_incref(envptr->env_bindings);
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

static lispval modulep(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    struct KNO_LEXENV *env=
      kno_consptr(struct KNO_LEXENV *,arg,kno_lexenv_type);
    if (kno_test(env->env_bindings,moduleid_symbol,VOID))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if ((HASHTABLEP(arg)) || (SLOTMAPP(arg)) || (SCHEMAPP(arg))) {
    if (kno_test(arg,moduleid_symbol,VOID))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

static lispval module_exports(lispval arg)
{
  if (KNO_LEXENVP(arg)) {
    kno_lexenv envptr = kno_consptr(kno_lexenv,arg,kno_lexenv_type);
    return kno_getkeys(envptr->env_exports);}
  else if (TABLEP(arg))
    return kno_getkeys(arg);
  else if (SYMBOLP(arg)) {
    lispval module = kno_find_module(arg,1);
    if (KNO_ABORTP(module))
      return module;
    else if (VOIDP(module))
      return kno_type_error(_("module"),"module_exports",arg);
    else {
      lispval v = module_exports(module);
      kno_decref(module);
      return v;}}
  else return kno_type_error(_("module"),"module_exports",arg);
}

static lispval local_bindings_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  if (env->env_copy)
    return kno_incref(env->env_copy->env_bindings);
  else {
    kno_lexenv copied = kno_copy_env(env);
    lispval bindings = copied->env_bindings;
    kno_incref(bindings);
    kno_decref((lispval)copied);
    return bindings;}
}

/* Finding where a symbol comes from */

static lispval wherefrom_evalfn(lispval expr,kno_lexenv call_env,
                                kno_stack _stack)
{
  lispval symbol_arg = kno_get_arg(expr,1);
  lispval symbol = kno_eval(symbol_arg,call_env);
  if (SYMBOLP(symbol)) {
    kno_lexenv env = NULL, scan = env;
    int lookup_ids = 1, decref_env = 0;
    lispval env_arg = kno_get_arg(expr,2);
    lispval env_val = kno_get_arg(expr,2);
    if (KNO_VOIDP(env_arg))
      env = call_env;
    else {
      env_val = kno_eval(env_arg,call_env);
      if (KNO_ABORTED(env_val))
        return env_val;
      else if (TYPEP(env_val,kno_lexenv_type)) {
        env = kno_consptr(kno_lexenv,env_val,kno_lexenv_type);
        decref_env = 1;}
      else  {
        lispval err = kno_type_error(_("environment"),"wherefrom",env_val);
        kno_decref(env_val);
        return err;}}
    lispval lookup = kno_get_arg(expr,3);
    if (!(KNO_VOIDP(lookup))) {
      lookup = kno_eval(lookup,call_env);
      if (KNO_ABORTED(lookup)) {
        if (decref_env) kno_decref(env_val);
        return lookup;}
      else if (KNO_FALSEP(lookup))
        lookup_ids = 0;
      else lookup_ids = 1;
      kno_decref(lookup);}
    if (env->env_copy)
      scan = env->env_copy;
    else scan = env;
    while (scan) {
      if (kno_test(scan->env_bindings,symbol,VOID)) {
        lispval bindings = scan->env_bindings;
        if (!(CONSP(bindings))) return KNO_FALSE;
        lispval id = kno_get(bindings,moduleid_symbol,KNO_VOID);
        if ( (KNO_SYMBOLP(id)) &&
             ( (lookup_ids) || (!(KNO_MALLOCD_CONSP((kno_cons)bindings))) ) ) {
          lispval mod = kno_get_module(id);
          if ( (KNO_ABORTP(mod)) || (KNO_TABLEP(mod))  ) {
            if (decref_env) kno_decref(env_val);
            return mod;}
          else kno_decref(mod);}
        if (KNO_MALLOCD_CONSP((kno_cons)bindings)) {
          lispval result = (lispval) scan;
          kno_incref(result);
          if (decref_env) kno_decref(env_val);
          return result;}
        else return KNO_FALSE;}
      scan = scan->env_parent;
      if ((scan) && (scan->env_copy))
        scan = scan->env_copy;}
    if (decref_env) kno_decref(env_val);
    return KNO_FALSE;}
  else {
    lispval err = kno_type_error(_("symbol"),"wherefrom",symbol);
    kno_decref(symbol);
    return err;}
}

/* Finding all the modules used from an environment */

static lispval getmodules_evalfn(lispval expr,kno_lexenv call_env,kno_stack _stack)
{
  lispval env_arg = kno_eval(kno_get_arg(expr,1),call_env);
  lispval modules = EMPTY;
  kno_lexenv env = call_env;
  if (VOIDP(env_arg)) {}
  else if (TYPEP(env_arg,kno_lexenv_type))
    env = kno_consptr(kno_lexenv,env_arg,kno_lexenv_type);
  else {
    lispval err =
      kno_type_error(_("environment"),"wherefrom",env_arg);
    kno_decref(env_arg);
    return err;}
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (kno_test(env->env_bindings,moduleid_symbol,VOID)) {
      lispval ids = kno_get(env->env_bindings,moduleid_symbol,VOID);
      if (CHOICEP(ids)) {
        DO_CHOICES(id,ids) {
          if (SYMBOLP(id)) {CHOICE_ADD(modules,id);}}}
      else if (SYMBOLP(ids)) {CHOICE_ADD(modules,ids);}
      else {}}
    env = env->env_parent;
    if ((env) && (env->env_copy)) env = env->env_copy;}
  kno_decref(env_arg);
  return modules;
}

/* CONSBLOCKS */

static lispval make_consblock(lispval obj)
{
  return kno_make_consblock(obj);
}

static lispval consblock_original(lispval obj)
{
  struct KNO_CONSBLOCK *cb = (kno_consblock) obj;
  return kno_incref(cb->consblock_original);
}

static lispval consblock_head(lispval obj)
{
  struct KNO_CONSBLOCK *cb = (kno_consblock) obj;
  return cb->consblock_head;
}

/* Profiling */

static lispval profile_fcn_prim(lispval fcn,lispval bool)
{
  if (KNO_FUNCTIONP(fcn)) {
    struct KNO_FUNCTION *f = KNO_XFUNCTION(fcn);
    if (KNO_FALSEP(bool)) {
      struct KNO_PROFILE *profile = f->fcn_profile;
      if (profile)
        return KNO_FALSE;
      else {
        f->fcn_profile = NULL;
        u8_free(profile);}}
    else f->fcn_profile = kno_make_profile(f->fcn_name);
    return KNO_TRUE;}
  else return kno_type_error("function","profile_fcn",fcn);
}

static lispval profile_reset_prim(lispval fcn)
{
  if (KNO_FUNCTIONP(fcn)) {
    struct KNO_FUNCTION *f = KNO_XFUNCTION(fcn);
    struct KNO_PROFILE *profile = f->fcn_profile;
    if (profile == NULL) return KNO_FALSE;
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
    return KNO_TRUE;}
  else return kno_type_error("function","profile_reset(profile)",fcn);
}

static lispval profiledp_prim(lispval fcn)
{
  if (KNO_FUNCTIONP(fcn)) {
    struct KNO_FUNCTION *f = KNO_XFUNCTION(fcn);
    if (f->fcn_profile)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

static lispval call_profile_symbol;

static lispval getcalls_prim(lispval fcn)
{
  if (KNO_FUNCTIONP(fcn)) {
    struct KNO_FUNCTION *f = KNO_XFUNCTION(fcn);
    struct KNO_PROFILE *p = f->fcn_profile;
    if (p==NULL) return KNO_FALSE;
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
    kno_incref(fcn);
    return kno_init_compound
      (NULL,call_profile_symbol,KNO_COMPOUND_USEREF,10,
       fcn,kno_make_flonum(exec_time),
       KNO_INT(user_time),KNO_INT(system_time),
       KNO_INT(n_waits),KNO_INT(n_contests),KNO_INT(n_faults),
       KNO_INT(nsecs),KNO_INT(calls),KNO_INT(items));}
  else return kno_type_error("function","getcalls_prim(profile)",fcn);
}

/* Accessors */

static lispval profile_getfcn(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,0);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_getfcn",profile);
}
static lispval profile_gettime(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,1);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_gettime",profile);
}
static lispval profile_getutime(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,2);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_getutime",profile);
}
static lispval profile_getstime(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,3);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_getstime",profile);
}
static lispval profile_getwaits(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,4);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_getwaits",profile);
}
static lispval profile_getcontests(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,5);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_getcontests",profile);
}
static lispval profile_getfaults(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,6);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_getfaults",profile);
}
static lispval profile_nsecs(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,7);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_nsecs",profile);
}
static lispval profile_ncalls(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,8);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_ncalls",profile);
}
static lispval profile_nitems(lispval profile)
{
  if (KNO_COMPOUND_TYPEP(profile,call_profile_symbol)) {
    struct KNO_COMPOUND *p = (kno_compound) profile;
    lispval v = KNO_COMPOUND_VREF(p,9);
    return kno_incref(v);}
  else return kno_type_error("call profile","profile_nitems",profile);
}

/* with sourcebase */

static lispval with_sourcebase_evalfn(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval usebase_expr = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  if (VOIDP(usebase_expr))
    return kno_err(kno_SyntaxError,"with_sourcebase_evalfn",NULL,expr);
  if (!(PAIRP(body)))
    return kno_err(kno_SyntaxError,"with_sourcebase_evalfn",NULL,expr);

  lispval usebase = kno_stack_eval(usebase_expr,env,stack,0);
  u8_string temp_base;
  if (KNO_ABORTP(usebase))
    return usebase;
  else if (KNO_STRINGP(usebase))
    temp_base = KNO_CSTRING(usebase);
  else if (KNO_FALSEP(usebase))
    temp_base = NULL;
  else {
    lispval err = kno_err("Sourcebase not string or #f","WITH-SOURCEBASE",NULL,
                          usebase);
    kno_decref(usebase);
    return err;}

  lispval result = VOID;
  u8_string old_base = NULL;
  U8_UNWIND_PROTECT("with-sourcebase",0) {
    old_base = kno_bind_sourcebase(temp_base);
    result = kno_eval_exprs(body,env,stack,0);}
  U8_ON_UNWIND {
    kno_restore_sourcebase(old_base);
    kno_decref(usebase);}
  U8_END_UNWIND;
  return result;
}
/* Getting all modules */

static lispval get_all_modules_prim()
{
  return kno_all_modules();
}

/* Initialization */

KNO_EXPORT void kno_init_reflection_c()
{
  lispval module = kno_new_cmodule("reflection",0,kno_init_reflection_c);

  lispval apropos_cprim = kno_make_cprim1("APROPOS",apropos_prim,1);
  kno_idefn(module,apropos_cprim);
  kno_defn(kno_scheme_module,apropos_cprim);

  moduleid_symbol = kno_intern("%moduleid");
  source_symbol = kno_intern("%source");
  call_profile_symbol = kno_intern("%callprofile");

  kno_idefn1(module,"MACRO?",macrop,1,
            "Returns true if its argument is an evaluator macro",
            -1,VOID);

  kno_idefn1(module,"COMPOUND-PROCEDURE?",lambdap,1,
            "",
            -1,VOID);
  kno_idefn1(module,"SPECIAL-FORM?",evalfnp,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE?",procedurep,1,
            "",
            -1,VOID);

  kno_idefn1(module,"PRIMITIVE?",primitivep,1,
            "",
            -1,VOID);
  kno_idefn1(module,"NON-DETERMINISTIC?",non_deterministicp,1,
            "",
            -1,VOID);
  kno_idefn1(module,"SYNCHRONIZED?",synchronizedp,1,
            "",
            -1,VOID);
  kno_idefn1(module,"MODULE?",modulep,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-NAME",procedure_name,1,
            "",
            -1,VOID);
  kno_idefn1(kno_scheme_module,"PROCEDURE-NAME",procedure_name,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-CNAME",procedure_cname,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-FILENAME",procedure_filename,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-MODULE",procedure_moduleid,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-ARITY",procedure_arity,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-MIN-ARITY",procedure_min_arity,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-SYMBOL",procedure_symbol,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-DOCUMENTATION",
            procedure_documentation,1,
            "",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-TAILABLE?",
            procedure_tailablep,1,
            "`(PROCEDURE-TAILABLE? *fcn*)` "
            "Returns true if *fcn* can be tail called, and "
            "false otherwise. By default, all procedures "
            "are tailable when called deterministically.",
            -1,VOID);
  kno_idefn1(module,"PROCEDURE-ID",procedure_id,1,"",-1,VOID);
  kno_idefn1(module,"LAMBDA-ARGS",lambda_args,1,"",-1,VOID);
  kno_defalias(module,"PROCEDURE-ARGS","LAMBDA-ARGS");
  kno_idefn1(module,"LAMBDA-ARGS",lambda_args,1,"",-1,VOID);
  kno_idefn1(module,"LAMBDA-BODY",lambda_body,1,"",-1,VOID);
  kno_defalias(module,"PROCEDURE-BODY","LAMBDA-BODY");
  kno_idefn1(module,"LAMBDA-START",lambda_start,1,"",-1,VOID);
  kno_idefn1(module,"LAMBDA-SOURCE",lambda_source,1,"",-1,VOID);
  kno_idefn1(module,"LAMBDA-ENV",lambda_env,1,"",-1,VOID);
  kno_defalias(module,"PROCEDURE-ENV","LAMBDA-ENV");

  kno_idefn2(module,"REFLECT/GET",reflect_get,2,
            "Returns a meta-property of a procedure",
            -1,VOID,-1,VOID);
  kno_idefn(module,kno_make_cprim3("REFLECT/STORE!",reflect_store,3));
  kno_idefn(module,kno_make_cprim3("REFLECT/ADD!",reflect_add,3));
  kno_idefn(module,kno_make_cprim3("REFLECT/DROP!",reflect_drop,2));
  kno_idefn(module,kno_make_cprim1("REFLECT/ATTRIBS",get_procedure_attribs,1));
  kno_idefn(module,kno_make_cprim2("REFLECT/SET-ATTRIBS!",
                                 set_procedure_attribs,2));
  kno_idefn2(module,"SET-PROCEDURE-DOCUMENTATION!",
            set_procedure_documentation,2,
            "",
            -1,VOID,kno_string_type,VOID);
  kno_idefn2(module,"SET-PROCEDURE-TAILABLE!",
            set_procedure_tailable,2,
            "",
            -1,VOID,-1,VOID);

  kno_idefn2(module,"SET-LAMBDA-BODY!",set_lambda_body,2,
            "",kno_lambda_type,VOID,-1,VOID);
  kno_idefn2(module,"SET-LAMBDA-ARGS!",set_lambda_args,2,
           "",kno_lambda_type,VOID,-1,VOID);
  kno_idefn2(module,"SET-LAMBDA-SOURCE!",set_lambda_source,2,
            "",kno_lambda_type,VOID,-1,VOID);
  kno_idefn2(module,"SET-LAMBDA-OPTIMIZER!",
            set_lambda_optimizer,2,
            "",kno_lambda_type,VOID,-1,VOID);

  kno_idefn2(module,"OPTIMIZE-LAMBDA-BODY!",optimize_lambda_body,2,
            "(OPTIMIZE-LAMBDA-BODY! *lambda*) updates the consblock "
            "body of a procedure",
            kno_lambda_type,VOID,-1,VOID);
  kno_idefn2(module,"OPTIMIZE-LAMBDA-ARGS!",optimize_lambda_args,2,
            "(OPTIMIZE-LAMBDA-ARGS! *lambda*) updates the parsed vars and "
            "defaults for lambda procedure",
            kno_lambda_type,VOID,-1,VOID);

  kno_idefn(module,kno_make_cprim2("MACROEXPAND",macroexpand,2));

  kno_idefn1(module,"FCNID/REF",fcnid_refprim,1,
            "",kno_fcnid_type,VOID);
  kno_idefn1(module,"FCNID/REGISTER",fcnid_registerprim,1,
            "",-1,VOID);
  kno_idefn2(module,"FCNID/SET!",fcnid_setprim,1,
            "",kno_fcnid_type,VOID,-1,VOID);

  kno_idefn1(module,"MODULE-BINDINGS",module_bindings,1,
            "Returns the symbols bound in a module's environment",
            -1,VOID);
  kno_idefn1(module,"MODULE-EXPORTS",module_exports,1,
            "Returns the symbols exported from a module",
            -1,VOID);
  kno_idefn1(module,"MODULE-SOURCE",module_getsource,1,
            "Returns the source (a string) from which module was loaded",
            -1,VOID);
  kno_idefn1(module,"MODULE-TABLE",module_table,1,
            "Returns the table used for getting symbols from this module",
            -1,VOID);
  kno_idefn1(module,"MODULE-ENVIRONMENT",module_environment,1,
            "Returns the table used for this module's internal environment",
            -1,VOID);

  kno_idefn0(module,"ALL-MODULES",get_all_modules_prim,
            "(ALL-MODULES) "
            "Returns all loaded modules as an alist "
            "of module names and modules");

  kno_idefn1(module,"CONSBLOCK",make_consblock,1,
            "(CONSBLOCK *obj*) returns a consblock structure "
            "which copies *obj* into a static contiguous block "
            "of memory.",
            -1,KNO_VOID);
  kno_idefn1(module,"CONSBLOCK-ORIGIN",consblock_original,1,
            "(CONSBLOCK-ORIGIN *consblock*) returns the original object "
            "from which a consblock was generated.",
            kno_consblock_type,KNO_VOID);
  kno_idefn1(module,"CONSBLOCK-HEAD",consblock_head,1,
            "(CONSBLOCK-HEAD *consblock*) returns the head of the consblock",
            kno_consblock_type,KNO_VOID);

  kno_idefn2(module,"REFLECT/PROFILE!",profile_fcn_prim,1,
            "`(REFLECT/PROFILE! *fcn* *boolean*)`"
            "Enables profiling for the function *fcn* or "
            "if *boolean* is false, disable profiling for *fcn*",
            -1,KNO_VOID,-1,KNO_TRUE);
  kno_idefn1(module,"REFLECT/PROFILED?",profiledp_prim,1,
            "`(REFLECT/PROFILED? *arg*)`Returns true if *arg* is being profiled, "
            "and false otherwise. It also returns false if the argument is "
            "not a function or not a function which supports profiling.",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/GETCALLS",getcalls_prim,1,
            "`(PROFILE/GETCALLS *fcn*)`Returns the profile information for "
            "*fcn*, a vector of *fcn*, the number of calls, the number of "
            "nanoseconds spent in *fcn*",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/RESET!",profile_reset_prim,1,
             "`(PROFILE/RESET! *fcn*)` resets the profile counts "
             "for *fcn*",
             -1,KNO_VOID);

  kno_idefn1(module,"PROFILE/FCN",profile_getfcn,1,
            "`(PROFILE/FCN *profile*)` returns the function a profile describes",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/TIME",profile_gettime,1,
            "`(PROFILE/TIME *profile*)` returns the number of seconds "
            "spent in the profiled function",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/UTIME",profile_getutime,1,
            "`(PROFILE/UTIME *profile*)` returns the number of seconds "
            "of user time spent in the profiled function",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/STIME",profile_getstime,1,
            "`(PROFILE/STIME *profile*)` returns the number of seconds "
            "of system time spent in the profiled function",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/WAITS",profile_getwaits,1,
            "`(PROFILE/WAITS *profile*)` returns the number of voluntary "
            "context switches (usually when waiting for something) "
            "during the execution of the profiled function",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/CONTESTS",profile_getcontests,1,
            "`(PROFILE/CONTESTS *profile*)` returns the number of involuntary "
            "context switches (often indicating contested resources) during "
            "the execution of the profiled function",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/FAULTS",profile_getfaults,1,
            "`(PROFILE/FAULTS *profile*)` returns the number of (major) page "
            "faults during the execution of the profiled function",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/NSECS",profile_nsecs,1,
            "`(PROFILE/NSECS *profile*)` returns the number of nanoseconds "
            "spent in the profiled function",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/NCALLS",profile_ncalls,1,
            "`(PROFILE/FAULTS *profile*)` returns the number of calls "
            "to the profiled function",
            -1,KNO_VOID);
  kno_idefn1(module,"PROFILE/NITEMS",profile_nitems,1,
            "`(PROFILE/FAULTS *profile*)` returns the number of items "
            "noted as processed by the profiled function",
            -1,KNO_VOID);


  kno_def_evalfn(module,"%BINDINGS","",local_bindings_evalfn);

  kno_def_evalfn(module,"WHEREFROM","",wherefrom_evalfn);
  kno_def_evalfn(module,"GETMODULES","",getmodules_evalfn);
  kno_def_evalfn(module,"WITH-SOURCEBASE","",with_sourcebase_evalfn);

  kno_finish_module(module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
