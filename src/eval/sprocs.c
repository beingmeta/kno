/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "eval_internals.h"
#include "framerd/storage.h"

#include <libu8/u8printf.h>

fd_exception fd_BadArglist=_("Malformed argument list");
fd_exception fd_BadDefineForm=_("Bad procedure defining form");

static fdtype tail_symbol;

static u8_string sproc_id(struct FD_SPROC *fn)
{
  if ((fn->fcn_name)&&(fn->fcn_filename))
    return u8_mkstring("%s:%s",fn->fcn_name,fn->fcn_filename);
  else if (fn->fcn_name)
    return u8_strdup(fn->fcn_name);
  else if (fn->fcn_filename)
    return u8_mkstring("λ%lx:%s",
                       ((unsigned long)
                        (((unsigned long long)fn)&0xFFFFFFFF)),
                       fn->fcn_filename);
  else return u8_mkstring("λ%lx",
                          ((unsigned long)
                           (((unsigned long long)fn)&0xFFFFFFFF)));
}

static int no_defaults(fdtype *args,int n)
{
  int i=0; while (i<n) {
    if (args[i] == FD_DEFAULT_VALUE)
      return 0;
    else i++;}
  return 1;
}

static fdtype get_rest_arg(fdtype *args,int n)
{
  fdtype result=FD_EMPTY_LIST; n--;
  while (n>=0) {
    fdtype arg=args[n--];
    result=fd_init_pair(NULL,fd_incref(arg),result);}
  return result;
}

/* SPROCs */

FD_FASTOP
fdtype call_sproc(struct FD_STACK *_stack,
                  struct FD_SPROC *fn,
                  int n,fdtype *args)
{
  fdtype result = FD_VOID;
  fdtype *proc_vars=fn->sproc_vars;
  fd_lispenv proc_env=fn->sproc_env;
  fd_lispenv call_env=proc_env;
  int n_vars = fn->sproc_n_vars, arity = fn->fcn_arity;

  if (n<fn->fcn_min_arity)
    return fd_err(fd_TooFewArgs,fn->fcn_name,NULL,FD_VOID);
  else if ( (arity>=0) && (n>arity) )
    return fd_err(fd_TooManyArgs,fn->fcn_name,NULL,FD_VOID);
  else {}

  if ( (_stack) && (_stack->stack_label == NULL) ) {
    if (fn->fcn_name)
      _stack->stack_label=fn->fcn_name;
    else {
      _stack->stack_label=sproc_id(fn);
      _stack->stack_free_label=1;}}

  int direct_call = ( ( n == arity ) && ( no_defaults(args,n) ) );
  struct FD_SCHEMAP _bindings, *bindings=&_bindings;
  struct FD_ENVIRONMENT stack_env;
  fdtype vals[n_vars];

  if (n_vars) {
    if  (direct_call) {
      fd_make_schemap(&_bindings,n_vars,0,proc_vars,args);
      _bindings.schemap_stackvals=1;}
    else {
      fd_init_elts(vals,n_vars,FD_VOID);
      fd_make_schemap(&_bindings,n_vars,0,proc_vars,vals);}

    /* Make it static */
    FD_SET_REFCOUNT(bindings,0);
    FD_INIT_STATIC_CONS(&stack_env,fd_environment_type);
    stack_env.env_bindings = (fdtype) bindings;
    stack_env.env_exports  = FD_VOID;
    stack_env.env_parent   = proc_env;
    stack_env.env_copy     = NULL;

    _stack->stack_env = call_env = &stack_env;}

  if (!(direct_call)) {
    fdtype arglist = fn->sproc_arglist;
    int i = 0; while (FD_PAIRP(arglist)) {
      fdtype arg = (i<n) ? (args[i]) : (FD_DEFAULT_VALUE);
      if (arg != FD_DEFAULT_VALUE)
        vals[i]=fd_incref(args[i]);
      else {
        fdtype argspec = FD_CAR(arglist);
        fdtype default_expr =
          ( (FD_PAIRP(argspec)) && (FD_PAIRP(FD_CDR(argspec))) ) ?
          (FD_CAR(FD_CDR(argspec))) :
          (FD_VOID);
        fdtype default_value = fd_eval(default_expr,proc_env);
        if (FD_THROWP(default_value))
          _return default_value;
        else if (FD_ABORTED(default_value))
          _return default_value;
        else vals[i]=default_value;}
      arglist=FD_CDR(arglist);
      i++;}
    if (FD_SYMBOLP(arglist)) {
      assert(arity<0);
      fdtype rest_arg=get_rest_arg(args+i,n-i);
      vals[i++]=rest_arg;
      assert(i==n_vars);}
    else {}}
  /* If we're synchronized, lock the mutex. */
  if (fn->sproc_synchronized) u8_lock_mutex(&(fn->sproc_lock));
  result = eval_body(":SPROC",fn->fcn_name,fn->sproc_body,0,
                     call_env,fd_stackptr);
  if (fn->sproc_synchronized) {
    /* If we're synchronized, finish any tail calls and unlock the
       mutex. */
    result = fd_finish_call(result);
    u8_unlock_mutex(&(fn->sproc_lock));}
  _return result;
}

FD_EXPORT fdtype fd_apply_sproc(struct FD_STACK *stack,
                                struct FD_SPROC *fn,
                                int n,fdtype *args)
{
  return call_sproc(stack,fn,n,args);
}

static fdtype apply_sproc(fdtype fn,int n,fdtype *args)
{
  return call_sproc(fd_stackptr,(struct FD_SPROC *)fn,n,args);
}

static fdtype
_make_sproc(u8_string name,
            fdtype arglist,fdtype body,fd_lispenv env,
            int nd,int sync,
            int incref,int copy_env)
{
  int i = 0, n_vars = 0, min_args = 0;
  fdtype scan = arglist, *schema = NULL;
  struct FD_SPROC *s = u8_alloc(struct FD_SPROC);
  FD_INIT_FRESH_CONS(s,fd_sproc_type);
  s->fcn_name = ((name) ? (u8_strdup(name)) : (NULL));
  while (FD_PAIRP(scan)) {
    fdtype argspec = FD_CAR(scan);
    n_vars++; scan = FD_CDR(scan);
    if (FD_SYMBOLP(argspec)) min_args = n_vars;}
  if (FD_EMPTY_LISTP(scan)) {
    s->sproc_n_vars = s->fcn_arity = n_vars;}
  else {
    n_vars++; s->sproc_n_vars = n_vars; s->fcn_arity = -1;}
  s->fcn_min_arity = min_args; s->fcn_xcall = 1; s->fcn_ndcall = nd;
  s->fcn_handler.fnptr = NULL;
  s->fcn_typeinfo = NULL;
  if (n_vars)
    s->sproc_vars = schema = u8_alloc_n((n_vars+1),fdtype);
  else s->sproc_vars = NULL;
  s->fcn_defaults = NULL; s->fcn_filename = NULL;
  s->fcn_attribs = FD_VOID;
  s->fcnid = FD_VOID;
  if (incref) {
    s->sproc_body = fd_incref(body);
    s->sproc_arglist = fd_incref(arglist);}
  else {
    s->sproc_body = body; s->sproc_arglist = arglist;}
  s->sproc_bytecode = NULL;
  if (env == NULL)
    s->sproc_env = env;
  else if ( (copy_env) || (FD_MALLOCD_CONSP(env)) )
    s->sproc_env = fd_copy_env(env);
  else s->sproc_env = fd_copy_env(env); /* s->sproc_env = env; */
  if (sync) {
    s->sproc_synchronized = 1;
    u8_init_mutex(&(s->sproc_lock));}
  else s->sproc_synchronized = 0;
  { /* Write documentation string */
    u8_string docstring = NULL;
    if ((FD_PAIRP(body))&&
        (FD_STRINGP(FD_CAR(body))) &&
        (FD_PAIRP(FD_CDR(body))))
      docstring = FD_STRDATA(FD_CAR(body));
    if ((docstring) && (*docstring=='<'))
      s->fcn_documentation = u8_strdup(docstring);
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
      fdtype scan = arglist;
      u8_puts(&out,"`(");
      if (name) u8_puts(&out,name); else u8_puts(&out,"λ");
      while (FD_PAIRP(scan)) {
        fdtype arg = FD_CAR(scan);
        if (FD_SYMBOLP(arg))
          u8_printf(&out," %ls",FD_SYMBOL_NAME(arg));
        else if ((FD_PAIRP(arg))&&(FD_SYMBOLP(FD_CAR(arg))))
          u8_printf(&out," [%ls]",FD_SYMBOL_NAME(FD_CAR(arg)));
        else u8_puts(&out," ??");
        scan = FD_CDR(scan);}
      if (FD_SYMBOLP(scan))
        u8_printf(&out," [%ls...]",FD_SYMBOL_NAME(scan));
      u8_puts(&out,")`");
      if (docstring) {
        u8_puts(&out,"\n\n");
        u8_puts(&out,docstring);}
      s->fcn_documentation = out.u8_outbuf;}}
  scan = arglist; i = 0; while (FD_PAIRP(scan)) {
    fdtype argspec = FD_CAR(scan);
    if (FD_PAIRP(argspec)) {
      schema[i]=FD_CAR(argspec);}
    else {
      schema[i]=argspec;}
    i++; scan = FD_CDR(scan);}
  if (i<s->sproc_n_vars) schema[i]=scan;
  return FDTYPE_CONS(s);
}

static fdtype make_sproc(u8_string name,
                         fdtype arglist,fdtype body,fd_lispenv env,
                         int nd,int sync)
{
  return _make_sproc(name,arglist,body,env,nd,sync,1,1);
}

FD_EXPORT fdtype fd_make_sproc(u8_string name,
                               fdtype arglist,fdtype body,fd_lispenv env,
                               int nd,int sync)
{
  return make_sproc(name,arglist,body,env,nd,sync);
}

FD_EXPORT void recycle_sproc(struct FD_RAW_CONS *c)
{
  struct FD_SPROC *sproc = (struct FD_SPROC *)c;
  int mallocd = FD_MALLOCD_CONSP(c);
  if (sproc->fcn_typeinfo) u8_free(sproc->fcn_typeinfo);
  if (sproc->fcn_defaults) u8_free(sproc->fcn_defaults);
  if (sproc->fcn_documentation) u8_free(sproc->fcn_documentation);
  if (sproc->fcn_attribs) fd_decref(sproc->fcn_attribs);
  fd_decref(sproc->sproc_arglist); fd_decref(sproc->sproc_body);
  u8_free(sproc->sproc_vars);
  if (sproc->sproc_env->env_copy) {
    fd_decref((fdtype)(sproc->sproc_env->env_copy));
    /* fd_recycle_environment(sproc->sproc_env->env_copy); */
  }
  if (sproc->sproc_synchronized)
    u8_destroy_mutex(&(sproc->sproc_lock));
  if (sproc->sproc_bytecode) {
    fdtype bc = (fdtype)(sproc->sproc_bytecode);
    fd_decref(bc);}
  /* Put these last to help with debugging, when needed */
  if (sproc->fcn_name) u8_free(sproc->fcn_name);
  if (sproc->fcn_filename) u8_free(sproc->fcn_filename);
  if (mallocd) {
    memset(sproc,0,sizeof(struct FD_SPROC));
    u8_free(sproc);}
}

static int unparse_sproc(u8_output out,fdtype x)
{
  struct FD_SPROC *sproc = fd_consptr(fd_sproc,x,fd_sproc_type);
  fdtype arglist = sproc->sproc_arglist;
  unsigned long long addr = (unsigned long long) sproc;
  u8_string codes=
    (((sproc->sproc_synchronized)&&(sproc->fcn_ndcall))?("∀∥"):
     (sproc->sproc_synchronized)?("∥"):
     (sproc->fcn_ndcall)?("∀"):(""));
  if (sproc->fcn_name)
    u8_printf(out,"#<λ%s%s",codes,sproc->fcn_name);
  else u8_printf(out,"#<λ%s0x%04x",codes,((addr>>2)%0x10000));
  if (FD_PAIRP(arglist)) {
    int first = 1; fdtype scan = sproc->sproc_arglist;
    fdtype spec = FD_VOID, arg = FD_VOID;
    u8_putc(out,'(');
    while (FD_PAIRP(scan)) {
      if (first) first = 0; else u8_putc(out,' ');
      spec = FD_CAR(scan);
      arg = FD_SYMBOLP(spec)?(spec):(FD_PAIRP(spec))?(FD_CAR(spec)):(FD_VOID);
      if (FD_SYMBOLP(arg))
        u8_puts(out,FD_SYMBOL_NAME(arg));
      else u8_puts(out,"??");
      if (FD_PAIRP(spec)) u8_putc(out,'?');
      scan = FD_CDR(scan);}
    if (FD_EMPTY_LISTP(scan))
      u8_putc(out,')');
    else if (FD_SYMBOLP(scan))
      u8_printf(out,"%s…)",FD_SYMBOL_NAME(scan));
    else u8_printf(out,"…%q…)",scan);}
  else if (FD_EMPTY_LISTP(arglist))
    u8_puts(out,"()");
  else if (FD_SYMBOLP(arglist))
    u8_printf(out,"(%s…)",FD_SYMBOL_NAME(arglist));
  else u8_printf(out,"(…%q…)",arglist);
  if (!(sproc->fcn_name))
    u8_printf(out," #!0x%llx",(unsigned long long)sproc);
  if (sproc->fcn_filename) {
    u8_string filename=sproc->fcn_filename;
    /* Elide information after the filename (such as time/size/hash) */
    u8_string space=strchr(filename,' ');
    u8_puts(out," '");
    if (space) u8_putn(out,filename,space-filename);
    else u8_puts(out,filename);
    u8_puts(out,"'>");}
  else u8_puts(out,">");
  return 1;
}

static int *copy_intvec(int *vec,int n,int *into)
{
  int *dest = (into)?(into):(u8_alloc_n(n,int));
  int i = 0; while (i<n) {
    dest[i]=vec[i]; i++;}
  return dest;
}

FD_EXPORT fdtype copy_sproc(struct FD_CONS *c,int flags)
{
  struct FD_SPROC *sproc = (struct FD_SPROC *)c;
  if (sproc->sproc_synchronized) {
    fdtype sp = (fdtype)sproc;
    fd_incref(sp);
    return sp;}
  else {
    struct FD_SPROC *fresh = u8_alloc(struct FD_SPROC);
    int n_args = sproc->sproc_n_vars+1, arity = sproc->fcn_arity;
    memcpy(fresh,sproc,sizeof(struct FD_SPROC));

    /* This sets a new reference count or declares it static */
    FD_INIT_FRESH_CONS(fresh,fd_sproc_type);

    if (sproc->fcn_name) fresh->fcn_name = u8_strdup(sproc->fcn_name);
    if (sproc->fcn_filename)
      fresh->fcn_filename = u8_strdup(sproc->fcn_filename);
    if (sproc->fcn_typeinfo)
      fresh->fcn_typeinfo = copy_intvec(sproc->fcn_typeinfo,arity,NULL);

    fresh->fcn_attribs = FD_VOID;
    fresh->sproc_arglist = fd_copier(sproc->sproc_arglist,flags);
    fresh->sproc_body = fd_copier(sproc->sproc_body,flags);
    fresh->sproc_bytecode = NULL;
    if (sproc->sproc_vars)
      fresh->sproc_vars = fd_copy_vec(sproc->sproc_vars,n_args,NULL,flags);
    if (sproc->fcn_defaults)
      fresh->fcn_defaults = fd_copy_vec(sproc->fcn_defaults,arity,NULL,flags);

    if (fresh->sproc_synchronized)
      u8_init_mutex(&(fresh->sproc_lock));

    if (U8_BITP(flags,FD_STATIC_COPY)) {
      FD_MAKE_CONS_STATIC(fresh);}

    return (fdtype) fresh;}
}

/* SPROC generators */

static fdtype lambda_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype arglist = fd_get_arg(expr,1);
  fdtype body = fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"LAMBDA",NULL,expr);
  if (FD_CODEP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,0,0,0,0);}
  else return make_sproc(NULL,arglist,body,env,0,0);
}

static fdtype ambda_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype arglist = fd_get_arg(expr,1);
  fdtype body = fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"AMBDA",NULL,expr);
  if (FD_CODEP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,1,0,0,0);}
  else return make_sproc(NULL,arglist,body,env,1,0);
}

static fdtype nambda_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype name_expr = fd_get_arg(expr,1), name;
  fdtype arglist = fd_get_arg(expr,2);
  fdtype body = fd_get_body(expr,3);
  u8_string namestring = NULL;
  if ((FD_VOIDP(name_expr))||(FD_VOIDP(arglist)))
    return fd_err(fd_TooFewExpressions,"NAMBDA",NULL,expr);
  else name = fd_eval(name_expr,env);
  if (FD_SYMBOLP(name)) namestring = FD_SYMBOL_NAME(name);
  else if (FD_STRINGP(name)) namestring = FD_STRDATA(name);
  else return fd_type_error("procedure name (string or symbol)",
                            "nambda_evalfn",name);
  if (FD_CODEP(body)) {
    fd_incref(arglist);
    return _make_sproc(namestring,arglist,body,env,1,0,0,0);}
  else return make_sproc(namestring,arglist,body,env,1,0);
}

static fdtype slambda_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype arglist = fd_get_arg(expr,1);
  fdtype body = fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"SLAMBDA",NULL,expr);
 if (FD_CODEP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,0,1,0,0);}
  else return make_sproc(NULL,arglist,body,env,0,1);
}

static fdtype sambda_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype arglist = fd_get_arg(expr,1);
  fdtype body = fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"SLAMBDA",NULL,expr);
 if (FD_CODEP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,1,1,0,0);}
  else return make_sproc(NULL,arglist,body,env,1,1);
}

static fdtype thunk_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype body = fd_get_body(expr,1);
  if (FD_CODEP(body))
    return _make_sproc(NULL,FD_EMPTY_LIST,body,env,0,0,0,0);
  else return make_sproc(NULL,FD_EMPTY_LIST,body,env,0,0);
}

/* DEFINE */

static fdtype define_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype var = fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE",NULL,expr);
  else if (FD_SYMBOLP(var)) {
    fdtype val_expr = fd_get_arg(expr,2);
    if (FD_VOIDP(val_expr))
      return fd_err(fd_TooFewExpressions,"DEFINE",NULL,expr);
    else {
      fdtype value = fd_eval(val_expr,env);
      if (FD_ABORTED(value)) return value;
      else if (fd_bind_value(var,value,env)>=0) {
        fdtype fvalue = (FD_FCNIDP(value))?(fd_fcnid_ref(value)):(value);
        if (FD_SPROCP(fvalue)) {
          struct FD_SPROC *s = (fd_sproc) fvalue;
          if (s->fcn_filename == NULL) {
            u8_string sourcebase = fd_sourcebase();
            if (sourcebase) s->fcn_filename = u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE",FD_SYMBOL_NAME(var),var);}}}
  else if (FD_PAIRP(var)) {
    fdtype fn_name = FD_CAR(var), args = FD_CDR(var);
    fdtype body = fd_get_body(expr,2);
    if (!(FD_SYMBOLP(fn_name)))
      return fd_err(fd_NotAnIdentifier,"DEFINE",NULL,fn_name);
    else {
      fdtype value = make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,0,0);
      if (FD_ABORTED(value)) return value;
      else if (fd_bind_value(fn_name,value,env)>=0) {
        fdtype fvalue = (FD_FCNIDP(value))?(fd_fcnid_ref(value)):(value);
        if (FD_SPROCP(fvalue)) {
          struct FD_SPROC *s = (fd_sproc)fvalue;
          if (s->fcn_filename == NULL) {
            u8_string sourcebase = fd_sourcebase();
            if (sourcebase) s->fcn_filename = u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE",FD_SYMBOL_NAME(fn_name),var);}}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE",NULL,var);
}

static fdtype defslambda_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype var = fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-SYNCHRONIZED",NULL,expr);
  else if (FD_SYMBOLP(var))
    return fd_err(fd_BadDefineForm,"DEFINE-SYNCHRONIZED",NULL,expr);
  else if (FD_PAIRP(var)) {
    fdtype fn_name = FD_CAR(var), args = FD_CDR(var);
    if (!(FD_SYMBOLP(fn_name)))
      return fd_err(fd_NotAnIdentifier,"DEFINE-SYNCHRONIZED",NULL,fn_name);
    else {
      fdtype body = fd_get_body(expr,2);
      fdtype value = make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,0,1);
      if (FD_ABORTED(value))
        return value;
      else if (fd_bind_value(fn_name,value,env)>=0) {
        fdtype opvalue = (FD_FCNIDP(value))?(fd_fcnid_ref(value)):(value);
        if (FD_SPROCP(opvalue)) {
          struct FD_SPROC *s = (fd_sproc)opvalue;
          if (s->fcn_filename == NULL) {
            u8_string sourcebase = fd_sourcebase();
            if (sourcebase) s->fcn_filename = u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE-SYNCHRONIZED",
                      FD_SYMBOL_NAME(var),var);}}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE-SYNCHRONIZED",NULL,var);
}

static fdtype defambda_evalfn(fdtype expr,fd_lispenv env,fd_stack _stack)
{
  fdtype var = fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-AMB",NULL,expr);
  else if (FD_SYMBOLP(var))
    return fd_err(fd_BadDefineForm,"DEFINE-AMB",FD_SYMBOL_NAME(var),expr);
  else if (FD_PAIRP(var)) {
    fdtype fn_name = FD_CAR(var), args = FD_CDR(var);
    fdtype body = fd_get_body(expr,2);
    if (!(FD_SYMBOLP(fn_name)))
      return fd_err(fd_NotAnIdentifier,"DEFINE-AMB",NULL,fn_name);
    else {
      fdtype value = make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,1,0);
      if (FD_ABORTED(value)) return value;
      else if (fd_bind_value(fn_name,value,env)>=0) {
        fdtype opvalue = fd_fcnid_ref(value);
        if (FD_SPROCP(opvalue)) {
          struct FD_SPROC *s = (fd_sproc)opvalue;
          if (s->fcn_filename == NULL) {
            u8_string sourcebase = fd_sourcebase();
            if (sourcebase) s->fcn_filename = u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE-AMB",FD_SYMBOL_NAME(fn_name),var);}}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE-AMB",NULL,var);
}

/* Extended apply */

static fdtype tail_symbol;

FD_EXPORT
/* fd_xapply_sproc:
     Arguments: a pointer to an sproc, a void* data pointer, and a function
      of a void* pointer and a dtype pointer
     Returns: the application result
  This uses an external function to get parameter values from some
  other data structure (cast as a void* pointer).  This is used, for instance,
  to expose CGI data fields as arguments to a main function, or to
  apply XML attributes and elements similarly. */
fdtype fd_xapply_sproc
  (struct FD_SPROC *fn,void *data,fdtype (*getval)(void *,fdtype))
{
  struct FD_STACK *_stack=fd_stackptr;
  int i = 0, n = fn->sproc_n_vars;
  fdtype arglist = fn->sproc_arglist, result = FD_VOID;
  fd_lispenv env = fn->sproc_env;
  INIT_STACK_ENV(call_env,env,n);
  fdtype *vals=call_env_bindings.schema_values;
  while (FD_PAIRP(arglist)) {
    fdtype argspec = FD_CAR(arglist), argname = FD_VOID, argval;
    if (FD_SYMBOLP(argspec)) argname = argspec;
    else if (FD_PAIRP(argspec)) argname = FD_CAR(argspec);
    if (!(FD_SYMBOLP(argname)))
      _return fd_err(fd_BadArglist,fn->fcn_name,NULL,fn->sproc_arglist);
    argval = getval(data,argname);
    if (FD_ABORTED(argval))
      _return argval;
    else if (( (FD_VOIDP(argval)) || (argval == FD_DEFAULT_VALUE) ) &&
             (FD_PAIRP(argspec)) && (FD_PAIRP(FD_CDR(argspec)))) {
      fdtype default_expr = FD_CADR(argspec);
      fdtype default_value = fd_eval(default_expr,fn->sproc_env);
      vals[i++]=default_value;}
    else vals[i++]=argval;
    arglist = FD_CDR(arglist);}
  /* This means we have a lexpr arg. */
  if (i<fn->sproc_n_vars) {
    /* We look for the arg directly and then we use the special
       tail_symbol (%TAIL) to get something. */
    fdtype argval = getval(data,arglist);
    if (FD_VOIDP(argval))
      argval = getval(data,tail_symbol);
    if (FD_ABORTED(argval))
      _return argval;
    else vals[i++]=argval;}
  assert(i == fn->sproc_n_vars);
  /* If we're synchronized, lock the mutex. */
  if (fn->sproc_synchronized) u8_lock_mutex(&(fn->sproc_lock));
  result = eval_body(":XPROC",fn->fcn_name,fn->sproc_body,0,call_env,_stack);
  /* if (fn->sproc_synchronized) result = fd_finish_call(result); */
  /* We always finish tail calls here */
  result = fd_finish_call(result);
  if (fn->sproc_synchronized)
    u8_unlock_mutex(&(fn->sproc_lock));
  _return result;
}

static fdtype tablegetval(void *obj,fdtype var)
{
  fdtype tbl = (fdtype)obj;
  return fd_get(tbl,var,FD_VOID);
}

static fdtype xapply_prim(fdtype proc,fdtype obj)
{
  struct FD_SPROC *sproc = fd_consptr(fd_sproc,proc,fd_sproc_type);
  if (!(FD_TABLEP(obj)))
    return fd_type_error("table","xapply_prim",obj);
  return fd_xapply_sproc(sproc,(void *)obj,tablegetval);
}

/* Walking an sproc */

static int walk_sproc(fd_walker walker,fdtype obj,void *walkdata,
                      fd_walk_flags flags,int depth)
{
  struct FD_SPROC *sproc = (fd_sproc)obj;
  fdtype env = (fdtype)sproc->sproc_env;
  if (fd_walk(walker,sproc->sproc_body,walkdata,flags,depth-1)<0)
    return -1;
  else if (fd_walk(walker,sproc->sproc_arglist,
                   walkdata,flags,depth-1)<0)
    return -1;
  else if ((!(FD_STATICP(env)))&&
           (fd_walk(walker,sproc->sproc_arglist,
                    walkdata,flags,depth-1)<0))
    return -1;
  else return 3;
}

/* Unparsing fcnids referring to sprocs */

static int unparse_extended_fcnid(u8_output out,fdtype x)
{
  fdtype lp = fd_fcnid_ref(x);
  if (FD_TYPEP(lp,fd_sproc_type)) {
    struct FD_SPROC *sproc = fd_consptr(fd_sproc,lp,fd_sproc_type);
    unsigned long long addr = (unsigned long long) sproc;
    fdtype arglist = sproc->sproc_arglist;
    u8_string codes=
      (((sproc->sproc_synchronized)&&(sproc->fcn_ndcall))?("∀∥"):
       (sproc->sproc_synchronized)?("∥"):
       (sproc->fcn_ndcall)?("∀"):(""));
    if (sproc->fcn_name)
      u8_printf(out,"#<~%d<λ%s%s",
                FD_GET_IMMEDIATE(x,fd_fcnid_type),
                codes,sproc->fcn_name);
    else u8_printf(out,"#<~%d<λ%s0x%04x",
                   FD_GET_IMMEDIATE(x,fd_fcnid_type),
                   codes,((addr>>2)%0x10000));
    if (FD_PAIRP(arglist)) {
      int first = 1; fdtype scan = sproc->sproc_arglist;
      fdtype spec = FD_VOID, arg = FD_VOID;
      u8_putc(out,'(');
      while (FD_PAIRP(scan)) {
        if (first) first = 0; else u8_putc(out,' ');
        spec = FD_CAR(scan);
        arg = (FD_SYMBOLP(spec)) ? (spec) :
          (FD_PAIRP(spec)) ? (FD_CAR(spec)) :
          (FD_VOID);
        if (FD_SYMBOLP(arg))
          u8_puts(out,FD_SYMBOL_NAME(arg));
        else u8_puts(out,"??");
        if (FD_PAIRP(spec)) u8_putc(out,'?');
        scan = FD_CDR(scan);}
      if (FD_EMPTY_LISTP(scan))
        u8_putc(out,')');
      else if (FD_SYMBOLP(scan))
        u8_printf(out,"%s…)",FD_SYMBOL_NAME(scan));
      else u8_printf(out,"…%q…)",scan);}
    else if (FD_EMPTY_LISTP(arglist))
      u8_puts(out,"()");
    else if (FD_SYMBOLP(arglist))
      u8_printf(out,"(%s…)",FD_SYMBOL_NAME(arglist));
    else u8_printf(out,"(…%q…)",arglist);
    if (!(sproc->fcn_name))
      u8_printf(out," #!0x%llx",(unsigned long long)sproc);
    if (sproc->fcn_filename)
      u8_printf(out," '%s'>>",sproc->fcn_filename);
    else u8_puts(out,">>");
    return 1;}
  else if (FD_TYPEP(lp,fd_cprim_type)) {
      struct FD_FUNCTION *fcn = (fd_function)lp;
      unsigned long long addr = (unsigned long long) fcn;
      u8_string name = fcn->fcn_name;
      u8_string filename = fcn->fcn_filename;
      u8_byte arity[16]=""; u8_byte codes[16]="";
      if ((filename)&&(filename[0]=='\0')) filename = NULL;
      if (name == NULL) name = fcn->fcn_name;
      if (fcn->fcn_ndcall) strcat(codes,"∀");
      if ((fcn->fcn_arity<0)&&(fcn->fcn_min_arity<0))
        strcat(arity,"…");
      else if (fcn->fcn_arity == fcn->fcn_min_arity)
        sprintf(arity,"[%d]",fcn->fcn_min_arity);
      else if (fcn->fcn_arity<0)
        sprintf(arity,"[%d,…]",fcn->fcn_min_arity);
      else sprintf(arity,"[%d,%d]",fcn->fcn_min_arity,fcn->fcn_arity);
      if (name)
        u8_printf(out,"#<~%d<%s%s%s%s%s%s>>",
                  FD_GET_IMMEDIATE(x,fd_fcnid_type),
                  codes,name,arity,U8OPTSTR("'",filename,"'"));
      else u8_printf(out,"#<~%d<Φ%s0x%04x%s #!0x%llx%s%s%s>>",
                     FD_GET_IMMEDIATE(x,fd_fcnid_type),
                     codes,((addr>>2)%0x10000),arity,
                     (unsigned long long) fcn,
                     arity,U8OPTSTR("'",filename,"'"));
      return 1;}
  else u8_printf(out,"#<~%ld %q>",
                 FD_GET_IMMEDIATE(x,fd_fcnid_type),lp);
  return 1;
}

/* Initialization */

FD_EXPORT void fd_init_sprocs_c()
{
  u8_register_source_file(_FILEINFO);

  tail_symbol = fd_intern("%TAIL");
  moduleid_symbol = fd_intern("%MODULEID");

  fd_applyfns[fd_sproc_type]=apply_sproc;
  fd_functionp[fd_sproc_type]=1;

  fd_unparsers[fd_sproc_type]=unparse_sproc;
  fd_recyclers[fd_sproc_type]=recycle_sproc;
  fd_walkers[fd_sproc_type]=walk_sproc;

  fd_unparsers[fd_fcnid_type]=unparse_extended_fcnid;

  fd_defspecial(fd_scheme_module,"LAMBDA",lambda_evalfn);
  fd_defspecial(fd_scheme_module,"AMBDA",ambda_evalfn);
  fd_defspecial(fd_scheme_module,"NAMBDA",nambda_evalfn);
  fd_defspecial(fd_scheme_module,"SLAMBDA",slambda_evalfn);
  fd_defspecial(fd_scheme_module,"SAMBDA",sambda_evalfn);
  fd_defspecial(fd_scheme_module,"THUNK",thunk_evalfn);
  fd_defspecial(fd_scheme_module,"DEFINE",define_evalfn);
  fd_defspecial(fd_scheme_module,"DEFSLAMBDA",defslambda_evalfn);
  fd_defspecial(fd_scheme_module,"DEFAMBDA",defambda_evalfn);

  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("XAPPLY",xapply_prim,2,fd_sproc_type,FD_VOID,-1,FD_VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
