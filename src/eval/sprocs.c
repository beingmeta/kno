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
#include "framerd/fdkbase.h"

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

/* SPROCs */

FD_FASTOP fdtype apply_sproc(struct FD_SPROC *fn,int n,fdtype *args)
{
  fdtype _vals[6], *vals=_vals, lexpr_arg=FD_EMPTY_LIST, result=FD_VOID;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  int n_vars=fn->sproc_n_vars;
  /* We're optimizing to avoid GC (and thread contention) for the
     simple case where the arguments exactly match the argument list.
     Essentially, we use the args vector as the values vector of
     the SCHEMAP used for binding.  The problem is when the arguments
     don't match the number of arguments (lexprs or optionals).  In this
     case we set free_env=1 and just use a regular environment where
     all the values are incref'd.  */
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  bindings.table_schema=fn->sproc_vars;
  bindings.schema_length=n_vars;
  bindings.schemap_onstack=1;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_bindings=FDTYPE_CONS(&bindings);
  envstruct.env_exports=FD_VOID;
  envstruct.env_parent=fn->sproc_env; envstruct.env_copy=NULL;
  if (n_vars>6) vals=u8_alloc_n(fn->sproc_n_vars,fdtype);
  bindings.schema_values=vals;
  if (fn->fcn_arity>0) {
    if (n<fn->fcn_min_arity) {
      u8_destroy_rwlock(&(bindings.table_rwlock));
      return fd_err(fd_TooFewArgs,fn->fcn_name,NULL,FD_VOID);}
    else if (n>fn->fcn_arity) {
      u8_destroy_rwlock(&(bindings.table_rwlock));
      return fd_err(fd_TooManyArgs,fn->fcn_name,NULL,FD_VOID);}
    else {
      /* This code handles argument defaults for sprocs */
      int i=0;
      if (FD_PAIRP(fn->sproc_arglist)) {
        FD_DOLIST(arg,fn->sproc_arglist)
          if (i<n) {
            fdtype val=args[i];
            if ((val==FD_DEFAULT_VALUE)&&(FD_PAIRP(arg))&&
                (FD_PAIRP(FD_CDR(arg)))) {
              fdtype default_expr=FD_CADR(arg);
              fdtype default_value=fd_eval(default_expr,fn->sproc_env);
              vals[i]=default_value;}
            else if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val)))
              vals[i]=fd_incref(val);
            else vals[i]=val;
            i++;}
          else if ((FD_PAIRP(arg)) && (FD_PAIRP(FD_CDR(arg)))) {
            /* This code handles argument defaults for sprocs */
            fdtype default_expr=FD_CADR(arg);
            fdtype default_value=fd_eval(default_expr,fn->sproc_env);
            vals[i]=default_value;
            i++;}
          else vals[i++]=FD_VOID;}
      else if (FD_RAILP(fn->sproc_arglist)) {
        struct FD_VECTOR *v=fd_consptr(fd_vector,fn->sproc_arglist,fd_rail_type);
        int len=v->fd_veclen; fdtype *dflts=v->fd_vecelts;
        while (i<len) {
          fdtype val=args[i];
          if ((val==FD_DEFAULT_VALUE)&&(dflts))  {
            fdtype default_expr=dflts[i];
            fdtype default_value=fd_eval(default_expr,fn->sproc_env);
            if (FD_VOIDP(default_value)) vals[i]=val;
            else vals[i]=default_value;}
          else if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
            vals[i]=fd_incref(val);}
          else vals[i]=val;
          i++;}
        while (i<n_vars) vals[i++]=FD_VOID;
        assert(i==fn->sproc_n_vars);}}}
  else if (fn->fcn_arity==0) {}
  else { /* We have a lexpr */
    int i=0, j=n-1;
    {FD_DOLIST(arg,fn->sproc_arglist)
       if (i<n) {
         fdtype val=args[i];
         if ((val==FD_DEFAULT_VALUE)&&(FD_PAIRP(arg))&&
             (FD_PAIRP(FD_CDR(arg)))) {
           /* This code handles argument defaults for sprocs */
           fdtype default_expr=FD_CADR(arg);
           fdtype default_value=fd_eval(default_expr,fn->sproc_env);
           vals[i]=default_value; i++;}
         else if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val)))
           vals[i]=fd_incref(val);
         else vals[i]=val;
         i++;}
       else if ((FD_PAIRP(arg)) && (FD_PAIRP(FD_CDR(arg)))) {
         /* This code handles argument defaults for sprocs */
         fdtype default_expr=FD_CADR(arg);
         fdtype default_value=fd_eval(default_expr,fn->sproc_env);
         vals[i]=default_value; i++;}
       else {vals[i]=FD_VOID; i++;}}
    while (j >= i) {
      lexpr_arg=fd_conspair(fd_incref(args[j]),lexpr_arg);
      j--;}
    fd_incref(lexpr_arg);
    vals[i]=lexpr_arg;}
  /* If we're synchronized, lock the mutex. */
  if (fn->sproc_synchronized) u8_lock_mutex(&(fn->sproc_lock));
  result=eval_body(":SPROC",fn->fcn_name,fn->sproc_body,0,&envstruct);
  if (fn->sproc_synchronized) result=fd_finish_call(result);
  if (FD_THROWP(result)) {}
  else if (FD_ABORTED(result)) {
    u8_exception ex;
    ex=u8_current_exception;
    if (ex->u8x_details) u8_free(ex->u8x_details);
    ex->u8x_details=sproc_id(fn);}
  else {}
  /* If we're synchronized, unlock the mutex. */
  if (fn->sproc_synchronized) u8_unlock_mutex(&(fn->sproc_lock));
  fd_decref(lexpr_arg);
  free_environment(&envstruct);
  if (vals!=_vals) u8_free(vals);
  return result;
}

FD_EXPORT fdtype fd_apply_sproc(struct FD_SPROC *fn,int n,fdtype *args)
{
  return apply_sproc(fn,n,args);
}

static fdtype _make_sproc(u8_string name,
                          fdtype arglist,fdtype body,fd_lispenv env,
                          int nd,int sync,
                          int incref,int copy_env)
{
  int i=0, n_vars=0, min_args=0;
  fdtype scan=arglist, *schema=NULL;
  struct FD_SPROC *s=u8_alloc(struct FD_SPROC);
  FD_INIT_CONS(s,fd_sproc_type);
  s->fcn_name=((name) ? (u8_strdup(name)) : (NULL));
  while (FD_PAIRP(scan)) {
    fdtype argspec=FD_CAR(scan);
    n_vars++; scan=FD_CDR(scan);
    if (FD_SYMBOLP(argspec)) min_args=n_vars;}
  if (FD_EMPTY_LISTP(scan)) {
    s->sproc_n_vars=s->fcn_arity=n_vars;}
  else {
    n_vars++; s->sproc_n_vars=n_vars; s->fcn_arity=-1;}
  s->fcn_min_arity=min_args; s->fcn_xcall=1; s->fcn_ndcall=nd;
  s->fcn_handler.fnptr=NULL;
  s->fcn_typeinfo=NULL;
  if (n_vars)
    s->sproc_vars=schema=u8_alloc_n((n_vars+1),fdtype);
  else s->sproc_vars=NULL;
  s->fcn_defaults=NULL; s->fcn_filename=NULL;
  if (incref) {
    s->sproc_body=fd_incref(body);
    s->sproc_arglist=fd_incref(arglist);}
  else {
    s->sproc_body=body; s->sproc_arglist=arglist;}
  if (env==NULL)
    s->sproc_env=env;
  else if ( (copy_env) || (FD_MALLOCD_CONSP(env)) )
    s->sproc_env=fd_copy_env(env);
  else s->sproc_env=fd_copy_env(env); /* s->sproc_env=env; */
  if (sync) {
    s->sproc_synchronized=1;
    u8_init_mutex(&(s->sproc_lock));}
  else s->sproc_synchronized=0;
  scan=arglist; i=0; while (FD_PAIRP(scan)) {
    fdtype argspec=FD_CAR(scan);
    if (FD_PAIRP(argspec)) {
      schema[i]=FD_CAR(argspec);}
    else {
      schema[i]=argspec;}
    i++; scan=FD_CDR(scan);}
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
  struct FD_SPROC *sproc=(struct FD_SPROC *)c;
  int mallocd=FD_MALLOCD_CONSP(c);
  if (sproc->fcn_name) u8_free(sproc->fcn_name);
  if (sproc->fcn_typeinfo) u8_free(sproc->fcn_typeinfo);
  if (sproc->fcn_defaults) u8_free(sproc->fcn_defaults);
  fd_decref(sproc->sproc_arglist); fd_decref(sproc->sproc_body);
  u8_free(sproc->sproc_vars);
  if (sproc->sproc_env->env_copy) {
    fd_decref((fdtype)(sproc->sproc_env->env_copy));
    /* fd_recycle_environment(sproc->sproc_env->env_copy); */
  }
  if (sproc->sproc_synchronized)
    u8_destroy_mutex(&(sproc->sproc_lock));
  if (sproc->fcn_filename) u8_free(sproc->fcn_filename);
  if (mallocd) {
    memset(sproc,0,sizeof(struct FD_SPROC));
    u8_free(sproc);}
}

static int unparse_sproc(u8_output out,fdtype x)
{
  struct FD_SPROC *sproc=fd_consptr(fd_sproc,x,fd_sproc_type);
  fdtype arglist=sproc->sproc_arglist;
  unsigned long long addr=(unsigned long long) sproc;
  u8_string codes=
    (((sproc->sproc_synchronized)&&(sproc->fcn_ndcall))?("∀∥"):
     (sproc->sproc_synchronized)?("∥"):
     (sproc->fcn_ndcall)?("∀"):(""));
  if (sproc->fcn_name)
    u8_printf(out,"#<λ%s%s",codes,sproc->fcn_name);
  else u8_printf(out,"#<λ%s0x%04x",codes,((addr>>2)%0x10000));
  if (FD_PAIRP(arglist)) {
    int first=1; fdtype scan=sproc->sproc_arglist;
    fdtype spec=FD_VOID, arg=FD_VOID;
    u8_putc(out,'(');
    while (FD_PAIRP(scan)) {
      if (first) first=0; else u8_putc(out,' ');
      spec=FD_CAR(scan);
      arg=FD_SYMBOLP(spec)?(spec):(FD_PAIRP(spec))?(FD_CAR(spec)):(FD_VOID);
      if (FD_SYMBOLP(arg))
        u8_puts(out,FD_SYMBOL_NAME(arg));
      else u8_puts(out,"??");
      if (FD_PAIRP(spec)) u8_putc(out,'?');
      scan=FD_CDR(scan);}
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
    u8_printf(out," '%s'>",sproc->fcn_filename);
  else u8_puts(out,">");
  return 1;
}

static int *copy_intvec(int *vec,int n,int *into)
{
  int *dest=(into)?(into):(u8_alloc_n(n,int));
  int i=0; while (i<n) {
    dest[i]=vec[i]; i++;}
  return dest;
}

FD_EXPORT fdtype copy_sproc(struct FD_CONS *c,int flags)
{
  struct FD_SPROC *sproc=(struct FD_SPROC *)c;
  if (sproc->sproc_synchronized) {
    fdtype sp=(fdtype)sproc;
    fd_incref(sp);
    return sp;}
  else {
    struct FD_SPROC *fresh=u8_alloc(struct FD_SPROC);
    int n_args=sproc->sproc_n_vars+1, arity=sproc->fcn_arity;
    memcpy(fresh,sproc,sizeof(struct FD_SPROC));

    /* This sets a new reference count or declares it static */
    FD_INIT_CONS(fresh,fd_sproc_type);

    if (sproc->fcn_name) fresh->fcn_name=u8_strdup(sproc->fcn_name);
    if (sproc->fcn_filename)
      fresh->fcn_filename=u8_strdup(sproc->fcn_filename);
    if (sproc->fcn_typeinfo)
      fresh->fcn_typeinfo=copy_intvec(sproc->fcn_typeinfo,arity,NULL);

    fresh->sproc_arglist=fd_copier(sproc->sproc_arglist,flags);
    fresh->sproc_body=fd_copier(sproc->sproc_body,flags);
    if (sproc->sproc_vars)
      fresh->sproc_vars=fd_copy_vec(sproc->sproc_vars,n_args,NULL,flags);
    if (sproc->fcn_defaults)
      fresh->fcn_defaults=fd_copy_vec(sproc->fcn_defaults,arity,NULL,flags);

    if (fresh->sproc_synchronized)
      u8_init_mutex(&(fresh->sproc_lock));

    if (U8_BITP(flags,FD_STATIC_COPY)) {
      FD_MAKE_CONS_STATIC(fresh);}

    return (fdtype) fresh;}
}

/* SPROC generators */

static fdtype lambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"LAMBDA",NULL,expr);
  if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,0,0,0,0);}
  else return make_sproc(NULL,arglist,body,env,0,0);
}

static fdtype ambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"AMBDA",NULL,expr);
  if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,1,0,0,0);}
  else return make_sproc(NULL,arglist,body,env,1,0);
}

static fdtype nambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype name_expr=fd_get_arg(expr,1), name;
  fdtype arglist=fd_get_arg(expr,2);
  fdtype body=fd_get_body(expr,3);
  u8_string namestring=NULL;
  if ((FD_VOIDP(name_expr))||(FD_VOIDP(arglist)))
    return fd_err(fd_TooFewExpressions,"NAMBDA",NULL,expr);
  else name=fd_eval(name_expr,env);
  if (FD_SYMBOLP(name)) namestring=FD_SYMBOL_NAME(name);
  else if (FD_STRINGP(name)) namestring=FD_STRDATA(name);
  else return fd_type_error("procedure name (string or symbol)",
                            "nambda_handler",name);
  if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(namestring,arglist,body,env,1,0,0,0);}
  else return make_sproc(namestring,arglist,body,env,1,0);
}

static fdtype slambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"SLAMBDA",NULL,expr);
 if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,0,1,0,0);}
  else return make_sproc(NULL,arglist,body,env,0,1);
}

static fdtype sambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype arglist=fd_get_arg(expr,1);
  fdtype body=fd_get_body(expr,2);
  if (FD_VOIDP(arglist))
    return fd_err(fd_TooFewExpressions,"SLAMBDA",NULL,expr);
 if (FD_RAILP(body)) {
    fd_incref(arglist);
    return _make_sproc(NULL,arglist,body,env,1,1,0,0);}
  else return make_sproc(NULL,arglist,body,env,1,1);
}

static fdtype thunk_handler(fdtype expr,fd_lispenv env)
{
  fdtype body=fd_get_body(expr,1);
  if (FD_RAILP(body))
    return _make_sproc(NULL,FD_EMPTY_LIST,body,env,0,0,0,0);
  else return make_sproc(NULL,FD_EMPTY_LIST,body,env,0,0);
}

/* DEFINE */

static fdtype define_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE",NULL,expr);
  else if (FD_SYMBOLP(var)) {
    fdtype val_expr=fd_get_arg(expr,2);
    if (FD_VOIDP(val_expr))
      return fd_err(fd_TooFewExpressions,"DEFINE",NULL,expr);
    else {
      fdtype value=fd_eval(val_expr,env);
      if (FD_ABORTED(value)) return value;
      else if (fd_bind_value(var,value,env)) {
        fdtype fvalue=(FD_FCNIDP(value))?(fd_fcnid_ref(value)):(value);
        if (FD_SPROCP(fvalue)) {
          struct FD_SPROC *s=(fd_sproc) fvalue;
          if (s->fcn_filename==NULL) {
            u8_string sourcebase=fd_sourcebase();
            if (sourcebase) s->fcn_filename=u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE",FD_SYMBOL_NAME(var),var);}}}
  else if (FD_PAIRP(var)) {
    fdtype fn_name=FD_CAR(var), args=FD_CDR(var);
    fdtype body=fd_get_body(expr,2);
    if (!(FD_SYMBOLP(fn_name)))
      return fd_err(fd_NotAnIdentifier,"DEFINE",NULL,fn_name);
    else {
      fdtype value=make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,0,0);
      if (FD_ABORTED(value)) return value;
      else if (fd_bind_value(fn_name,value,env)) {
        fdtype fvalue=(FD_FCNIDP(value))?(fd_fcnid_ref(value)):(value);
        if (FD_SPROCP(fvalue)) {
          struct FD_SPROC *s=(fd_sproc)fvalue;
          if (s->fcn_filename==NULL) {
            u8_string sourcebase=fd_sourcebase();
            if (sourcebase) s->fcn_filename=u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE",FD_SYMBOL_NAME(fn_name),var);}}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE",NULL,var);
}

static fdtype defslambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-SYNCHRONIZED",NULL,expr);
  else if (FD_SYMBOLP(var))
    return fd_err(fd_BadDefineForm,"DEFINE-SYNCHRONIZED",NULL,expr);
  else if (FD_PAIRP(var)) {
    fdtype fn_name=FD_CAR(var), args=FD_CDR(var);
    if (!(FD_SYMBOLP(fn_name)))
      return fd_err(fd_NotAnIdentifier,"DEFINE-SYNCHRONIZED",NULL,fn_name);
    else {
      fdtype body=fd_get_body(expr,2);
      fdtype value=make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,0,1);
      if (FD_ABORTED(value))
        return value;
      else if (fd_bind_value(fn_name,value,env)) {
        fdtype opvalue=(FD_FCNIDP(value))?(fd_fcnid_ref(value)):(value);
        if (FD_SPROCP(opvalue)) {
          struct FD_SPROC *s=(fd_sproc)opvalue;
          if (s->fcn_filename==NULL) {
            u8_string sourcebase=fd_sourcebase();
            if (sourcebase) s->fcn_filename=u8_strdup(sourcebase);}}
        fd_decref(value);
        return FD_VOID;}
      else {
        fd_decref(value);
        return fd_err(fd_BindError,"DEFINE-SYNCHRONIZED",
                      FD_SYMBOL_NAME(var),var);}}}
  else return fd_err(fd_NotAnIdentifier,"DEFINE-SYNCHRONIZED",NULL,var);
}

static fdtype defambda_handler(fdtype expr,fd_lispenv env)
{
  fdtype var=fd_get_arg(expr,1);
  if (FD_VOIDP(var))
    return fd_err(fd_TooFewExpressions,"DEFINE-AMB",NULL,expr);
  else if (FD_SYMBOLP(var))
    return fd_err(fd_BadDefineForm,"DEFINE-AMB",FD_SYMBOL_NAME(var),expr);
  else if (FD_PAIRP(var)) {
    fdtype fn_name=FD_CAR(var), args=FD_CDR(var);
    fdtype body=fd_get_body(expr,2);
    if (!(FD_SYMBOLP(fn_name)))
      return fd_err(fd_NotAnIdentifier,"DEFINE-AMB",NULL,fn_name);
    else {
      fdtype value=make_sproc(FD_SYMBOL_NAME(fn_name),args,body,env,1,0);
      if (FD_ABORTED(value)) return value;
      else if (fd_bind_value(fn_name,value,env)) {
        fdtype opvalue=fd_fcnid_ref(value);
        if (FD_SPROCP(opvalue)) {
          struct FD_SPROC *s=(fd_sproc)opvalue;
          if (s->fcn_filename==NULL) {
            u8_string sourcebase=fd_sourcebase();
            if (sourcebase) s->fcn_filename=u8_strdup(sourcebase);}}
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
  int i=0;
  fdtype _vals[12], *vals=_vals, arglist=fn->sproc_arglist, result=FD_VOID;
  struct FD_SCHEMAP bindings; struct FD_ENVIRONMENT envstruct;
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.table_schema=fn->sproc_vars;
  bindings.schema_length=fn->sproc_n_vars;
  u8_init_rwlock(&(bindings.table_rwlock));
  envstruct.env_bindings=FDTYPE_CONS(&bindings);
  envstruct.env_exports=FD_VOID;
  envstruct.env_parent=fn->sproc_env; envstruct.env_copy=NULL;
  if (fn->sproc_n_vars>=12)
    bindings.schema_values=vals=u8_alloc_n(fn->sproc_n_vars,fdtype);
  else bindings.schema_values=vals=_vals;
  while (FD_PAIRP(arglist)) {
    fdtype argspec=FD_CAR(arglist), argname=FD_VOID, argval;
    if (FD_SYMBOLP(argspec)) argname=argspec;
    else if (FD_PAIRP(argspec)) argname=FD_CAR(argspec);
    if (!(FD_SYMBOLP(argname)))
      return fd_err(fd_BadArglist,fn->fcn_name,NULL,fn->sproc_arglist);
    argval=getval(data,argname);
    if (FD_ABORTED(argval)) {
      int j=0; while (j<i) {
        fdtype val=vals[j++];
        if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val)))
          fd_decref(val);}
      if (vals!=_vals) u8_free(vals);
      return argval;}
    else if (((FD_VOIDP(argval))||(argval==FD_DEFAULT_VALUE)) &&
             (FD_PAIRP(argspec)) && (FD_PAIRP(FD_CDR(argspec)))) {
      fdtype default_expr=FD_CADR(argspec);
      fdtype default_value=fd_eval(default_expr,fn->sproc_env);
      vals[i++]=default_value;}
    else vals[i++]=argval;
    arglist=FD_CDR(arglist);}
  /* This means we have a lexpr arg. */
  if (i<fn->sproc_n_vars) {
    /* We look for the arg directly and then we use the special
       tail_symbol (%TAIL) to get something. */
    fdtype argval=getval(data,arglist);
    if (FD_VOIDP(argval)) argval=getval(data,tail_symbol);
    if (FD_ABORTED(argval)) {
      int j=0; while (j<i) {
        fdtype val=vals[j++];
        if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
          fd_decref(val);}}
      if (vals!=_vals) u8_free(vals);
      return argval;}
    else vals[i++]=argval;}
  assert(i==fn->sproc_n_vars);
  /* If we're synchronized, lock the mutex. */
  if (fn->sproc_synchronized) u8_lock_mutex(&(fn->sproc_lock));
  result=eval_body(":XPROC",fn->fcn_name,fn->sproc_body,0,&envstruct);
  /* if (fn->sproc_synchronized) result=fd_finish_call(result); */
  /* We always finish tail calls here */
  result=fd_finish_call(result);
  if (FD_THROWP(result)) {}
  else if ((FD_ABORTED(result)) && (fn->fcn_filename))
    u8_current_exception->u8x_details=sproc_id(fn);
  else {}
  /* If we're synchronized, unlock the mutex. */
  if (fn->sproc_synchronized)
    u8_unlock_mutex(&(fn->sproc_lock));
  u8_destroy_rwlock(&(bindings.table_rwlock));
  if (envstruct.env_copy) {
    fd_recycle_environment(envstruct.env_copy);
    envstruct.env_copy=NULL;}
  free_environment(&envstruct);
  if (vals!=_vals) u8_free(vals);
  return result;
}

static fdtype tablegetval(void *obj,fdtype var)
{
  fdtype tbl=(fdtype)obj;
  return fd_get(tbl,var,FD_VOID);
}

static fdtype xapply_prim(fdtype proc,fdtype obj)
{
  struct FD_SPROC *sproc=fd_consptr(fd_sproc,proc,fd_sproc_type);
  if (!(FD_TABLEP(obj)))
    return fd_type_error("table","xapply_prim",obj);
  return fd_xapply_sproc(sproc,(void *)obj,tablegetval);
}

/* Initialization */

FD_EXPORT void fd_init_sprocs_c()
{
  u8_register_source_file(_FILEINFO);

  tail_symbol=fd_intern("%TAIL");
  moduleid_symbol=fd_intern("%MODULEID");

  fd_applyfns[fd_sproc_type]=(fd_applyfn)apply_sproc;
  fd_functionp[fd_sproc_type]=1;

  fd_unparsers[fd_sproc_type]=unparse_sproc;
  fd_recyclers[fd_sproc_type]=recycle_sproc;

  fd_defspecial(fd_scheme_module,"LAMBDA",lambda_handler);
  fd_defspecial(fd_scheme_module,"AMBDA",ambda_handler);
  fd_defspecial(fd_scheme_module,"NAMBDA",nambda_handler);
  fd_defspecial(fd_scheme_module,"SLAMBDA",slambda_handler);
  fd_defspecial(fd_scheme_module,"SAMBDA",sambda_handler);
  fd_defspecial(fd_scheme_module,"THUNK",thunk_handler);
  fd_defspecial(fd_scheme_module,"DEFINE",define_handler);
  fd_defspecial(fd_scheme_module,"DEFSLAMBDA",defslambda_handler);
  fd_defspecial(fd_scheme_module,"DEFAMBDA",defambda_handler);

  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("XAPPLY",xapply_prim,2,fd_sproc_type,FD_VOID,-1,FD_VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
