/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "eval_internals.h"
#include "kno/storage.h"

#include <libu8/u8printf.h>

u8_condition kno_BadArglist=_("Malformed argument list");
u8_condition kno_BadDefineForm=_("Bad procedure defining form");

int kno_record_source=1;

static lispval tail_symbol, decls_symbol, flags_symbol;

static u8_string lambda_id(struct KNO_LAMBDA *fn)
{
  if ((fn->fcn_name)&&(fn->fcn_filename))
    return u8_mkstring("%s:%s",fn->fcn_name,fn->fcn_filename);
  else if (fn->fcn_name)
    return u8_strdup(fn->fcn_name);
  else if (fn->fcn_filename)
    return u8_mkstring("λ%lx:%s",
                       ((unsigned long)
                        ((KNO_LONGVAL(fn))&0xFFFFFFFF)),
                       fn->fcn_filename);
  else return u8_mkstring("λ%lx",
                          ((unsigned long)
                           ((KNO_LONGVAL(fn))&0xFFFFFFFF)));
}

static int no_defaults(lispval *args,int n)
{
  int i=0; while (i<n) {
    if (args[i] == KNO_DEFAULT_VALUE)
      return 0;
    else i++;}
  return 1;
}

static lispval get_rest_arg(lispval *args,int n)
{
  lispval result=NIL; n--;
  while (n>=0) {
    lispval arg=args[n--];
    result=kno_init_pair(NULL,kno_incref(arg),result);}
  return result;
}

static int add_autodocp(u8_string s)
{
  if (s == NULL)
    return 1;
  else if ( ( *s == '(') || (*s == '<') ||
            (*s == '\n') || (*s == '`' ) )
    return 0;
  else return 1;
}

/* LAMBDAs */

KNO_FASTOP
lispval call_lambda(struct KNO_STACK *_stack,
                    struct KNO_LAMBDA *fn,
                    int n,lispval *args)
{
  lispval result = VOID;
  lispval *proc_vars=fn->lambda_vars;
  kno_lexenv proc_env=fn->lambda_env;
  kno_lexenv call_env=proc_env;
  int n_vars = fn->lambda_n_vars, arity = fn->fcn_arity;

  if (_stack == NULL) _stack=kno_stackptr;
  if (_stack == NULL) {
    KNO_ALLOCA_STACK(_stack);
    _stack->stack_label = fn->fcn_name;}

  if (n<fn->fcn_min_arity)
    return kno_err(kno_TooFewArgs,fn->fcn_name,NULL,VOID);
  else if ( (arity>=0) && (n>arity) )
    return kno_err(kno_TooManyArgs,fn->fcn_name,NULL,VOID);
  else {}

  if ( (_stack) && (_stack->stack_label == NULL) ) {
    if (fn->fcn_name)
      _stack->stack_label=fn->fcn_name;
    else {
      _stack->stack_label=lambda_id(fn);
      U8_SETBITS(_stack->stack_flags,KNO_STACK_FREE_LABEL);}}

  lispval vals[n_vars];

  struct KNO_LEXENV stack_env = { 0 };
  KNO_INIT_STATIC_CONS(&stack_env,kno_lexenv_type);
  stack_env.env_exports  = VOID;
  stack_env.env_parent   = proc_env;
  stack_env.env_copy     = NULL;

  int direct_call = ( ( n == arity ) && ( no_defaults(args,n) ) );
  struct KNO_SCHEMAP _bindings = { 0 }, *bindings=&_bindings;
  kno_make_schemap(bindings,n_vars,0,proc_vars,vals);
  _bindings.schemap_stackvals = direct_call;
  KNO_SET_REFCOUNT(bindings,0);
  stack_env.env_bindings = (lispval) bindings;

  kno_init_elts(vals,n_vars,VOID);

  /* Make it static */
  if (_stack) _stack->stack_env = call_env = &stack_env;

  if (direct_call)
    memcpy(vals,args,sizeof(lispval)*n_vars);
  else {
    lispval *inits = fn->lambda_inits;
    int n_inits = (arity < 0) ? (n_vars-1) : (n_vars);
    int i = 0; while (i < n_inits) {
      lispval arg = (i<n) ? (args[i]) : (KNO_DEFAULT_VALUE);
      if (arg != KNO_DEFAULT_VALUE)
        vals[i]=kno_incref(args[i]);
      else if (inits) {
        lispval default_expr = inits[i];
        lispval use_value = fast_stack_eval(default_expr,proc_env,_stack);
        if (KNO_THROWP(use_value)) {
          kno_decref_vec(vals,i);
          _return use_value;}
        else if (KNO_ABORTED(use_value)) {
          kno_decref_vec(vals,i);
          _return use_value;}
        else vals[i]=use_value;}
      i++;}
    if (arity < 0) {
      lispval rest_arg=get_rest_arg(args+i,n-i);
      vals[i++]=rest_arg;
      assert(i==n_vars);}
    else {}}
  /* If we're synchronized, lock the mutex. */
  if (fn->lambda_synchronized) u8_lock_mutex(&(fn->lambda_lock));
  result = eval_inner_body(":LAMBDA",fn->fcn_name,fn->lambda_start,0,
                     call_env,kno_stackptr);
  if (fn->lambda_synchronized) {
    /* If we're synchronized, finish any tail calls and unlock the
       mutex. */
    result = kno_finish_call(result);
    u8_unlock_mutex(&(fn->lambda_lock));}
  _return result;
}

KNO_EXPORT lispval kno_apply_lambda(struct KNO_STACK *stack,
                                struct KNO_LAMBDA *fn,
                                int n,lispval *args)
{
  return call_lambda(stack,fn,n,args);
}

static lispval apply_lambda(lispval fn,int n,lispval *args)
{
  return call_lambda(kno_stackptr,(struct KNO_LAMBDA *)fn,n,args);
}

KNO_EXPORT int kno_set_lambda_schema(struct KNO_LAMBDA *s,lispval args)
{
  int n_vars = 0, has_inits = 0;
  lispval scan = args;
  while (KNO_PAIRP(scan)) {
    lispval arg = KNO_CAR(scan);
    if ( (KNO_PAIRP(arg)) && (KNO_PAIRP(KNO_CDR(arg))) ) has_inits=1;
    scan = KNO_CDR(scan);
    n_vars++;}
  if (! (KNO_EMPTY_LISTP(scan)) ) n_vars++;
  lispval *cur_vars = s->lambda_vars;
  lispval *cur_inits = s->lambda_inits;
  int n_cur = s->lambda_n_vars;
  if (n_vars) {
    lispval *schema = s->lambda_vars = u8_alloc_n((n_vars),lispval);
    lispval *inits = s->lambda_inits = (has_inits) ?
      (u8_alloc_n((n_vars),lispval)) :
      (NULL);
    int i = 0; scan = args; while (PAIRP(scan)) {
      lispval arg = KNO_CAR(scan);
      if (KNO_PAIRP(arg)) {
        if (KNO_SYMBOLP(KNO_CAR(arg))) {
          schema[i] = KNO_CAR(arg);
          if (KNO_PAIRP(KNO_CDR(arg))) {
            lispval init = KNO_CADR(arg);
            inits[i] = kno_incref(init);}
          else if (KNO_EMPTY_LISTP(KNO_CDR(arg))) {
            if (inits) inits[i] = KNO_VOID;}
          else {
            u8_free(schema); if (inits) u8_free(inits);
            kno_seterr(kno_SyntaxError,"optimize_procedure_args",NULL,arg);
            return -1;}}
        else {
          u8_free(schema); if (inits) u8_free(inits);
          kno_seterr(kno_SyntaxError,"optimize_procedure_args",NULL,arg);
          return -1;}}
      else if (KNO_SYMBOLP(arg)) {
        schema[i]=arg; if (inits) inits[i]=KNO_VOID;}
      else {
        u8_free(schema); if (inits) u8_free(inits);
        kno_seterr(kno_SyntaxError,"optimize_procedure_args",NULL,arg);
        return -1;}
      scan = KNO_CDR(scan);
      i++;}
    if (KNO_SYMBOLP(scan)) {
      schema[i] = scan;
      if (inits) inits[i] = KNO_NIL;}}
  else {
    s->lambda_vars = NULL;
    s->lambda_inits = NULL;}
  s->lambda_n_vars = n_vars;
  if (cur_vars) u8_free(cur_vars);
  if (cur_inits) {
    kno_decref_vec(cur_inits,n_cur);
    u8_free(cur_inits);}
  return n_vars;
}

static lispval document_lambda(struct KNO_LAMBDA *s,u8_string name,
                               lispval arglist,lispval body)
{
  struct U8_OUTPUT docstream;
  U8_INIT_OUTPUT(&docstream,256);
  int n_lines = 0;
  while ( (PAIRP(body)) &&
          (STRINGP(KNO_CAR(body))) &&
          (PAIRP(KNO_CDR(body))) ) {
    u8_string docstring = KNO_CSTRING(KNO_CAR(body));
    if (n_lines == 0) {
      if (add_autodocp(docstring)) {
        /* Generate an initial docstring from the name and args */
        lispval scan = arglist;
        u8_puts(&docstream,"`(");
        if (name) u8_puts(&docstream,name); else u8_puts(&docstream,"λ");
        while (PAIRP(scan)) {
          lispval arg = KNO_CAR(scan);
          if (SYMBOLP(arg))
            u8_printf(&docstream," %ls",SYM_NAME(arg));
          else if ((PAIRP(arg))&&(SYMBOLP(KNO_CAR(arg))))
          u8_printf(&docstream," [%ls]",SYM_NAME(KNO_CAR(arg)));
        else u8_puts(&docstream," ??");
        scan = KNO_CDR(scan);}
      if (SYMBOLP(scan))
        u8_printf(&docstream," [%ls...]",SYM_NAME(scan));
      u8_puts(&docstream,")`\n");}
      u8_puts(&docstream,docstring);}
    else {
      u8_putc(&docstream,'\n');
      u8_puts(&docstream,docstring);}
    body = KNO_CDR(body);
    n_lines++;}
  if ( (u8_outbuf_written(&docstream)) ) {
    s->fcn_doc=docstream.u8_outbuf;
    s->fcn_free_doc = 1;}
  else u8_close_output(&docstream);
  return body;
}

static int process_decls(struct KNO_LAMBDA *s,lispval body)
{
  lispval attribs = s->fcn_attribs;
  if (attribs == KNO_VOID)
    s->fcn_attribs = attribs = kno_make_slotmap(4,0,NULL);
  while ( (KNO_PAIRP(body)) && (KNO_CAR(body) == decls_symbol) ) {
    lispval decls = KNO_CAR(body); body = KNO_CDR(body);
    lispval scan =  KNO_CDR(decls);
    int len = kno_list_length(scan);
    if (len > 0) {
      if (len == 1)
        kno_add(attribs,flags_symbol,KNO_CAR(scan));
      else if (len%2) {
        while ( KNO_PAIRP(scan) ) {
          lispval key = KNO_CAR(scan);
          lispval value = KNO_CADR(scan);
          kno_add(attribs,key,value);
          scan=KNO_CDR(KNO_CDR(scan));}}
      else {
        u8_log(LOG_ERR,"BadProcedureDECL",
               "Couldn't get decls from %q",decls);}}
    body = KNO_CDR(body);}
  return body;
}

static lispval
_make_lambda(u8_string name,
             lispval arglist,lispval body,kno_lexenv env,
             int nd,int sync,
             int incref,int copy_env,
             int autodoc)
{
  int min_args = 0;
  struct KNO_LAMBDA *s = u8_alloc(struct KNO_LAMBDA);
  KNO_INIT_FRESH_CONS(s,kno_lambda_type);
  s->fcn_name = ((name) ? (u8_strdup(name)) : (NULL));
  int n_vars = kno_set_lambda_schema(s,arglist);
  lispval scan = arglist;
  while (PAIRP(scan)) {
    lispval argspec = KNO_CAR(scan);
    if (SYMBOLP(argspec)) {
      min_args++; scan=KNO_CDR(scan);}
    else {
      while (PAIRP(scan)) scan=KNO_CDR(scan);}}
  if (NILP(scan))
    s->fcn_arity = n_vars;
  else s->fcn_arity = -1;
  s->fcn_min_arity = min_args;
  s->fcn_ndcall = nd;
  s->fcn_xcall = 1;
  s->fcn_handler.fnptr = NULL;
  s->fcn_typeinfo = NULL;
  if (n_vars)
    kno_set_lambda_schema(s,arglist);
  else {
    s->lambda_vars = NULL;
    s->lambda_inits = NULL;}
  s->fcn_defaults = NULL; s->fcn_filename = NULL;
  s->fcn_attribs = VOID;
  s->fcnid = VOID;
  if (incref) {
    s->lambda_body = kno_incref(body);
    s->lambda_arglist = kno_incref(arglist);}
  else {
    s->lambda_body = body;
    s->lambda_arglist = arglist;}
  s->lambda_consblock = NULL;
  if (env == NULL)
    s->lambda_env = env;
  else if ( (copy_env) || (KNO_MALLOCD_CONSP(env)) )
    s->lambda_env = kno_copy_env(env);
  else s->lambda_env = kno_copy_env(env); /* s->lambda_env = env; */
  if (sync) {
    s->lambda_synchronized = 1;
    u8_init_mutex(&(s->lambda_lock));}
  else s->lambda_synchronized = 0;
  if (autodoc) body = document_lambda(s,name,arglist,body);

  if ( ( KNO_PAIRP(body) ) &&
       ( KNO_PAIRP(KNO_CAR(body)) ) &&
       ( (KNO_CAR(KNO_CAR(body))) == decls_symbol ) )
    body = process_decls(s,body);
  s->lambda_start = body;

  s->lambda_source = VOID;

  return LISP_CONS(s);
}

static lispval make_lambda(u8_string name,
                           lispval arglist,lispval body,kno_lexenv env,
                           int nd,int sync,int autodoc)
{
  return _make_lambda(name,arglist,body,env,nd,sync,1,1,autodoc);
}

KNO_EXPORT lispval kno_make_lambda(u8_string name,
                                 lispval arglist,lispval body,kno_lexenv env,
                                 int nd,int sync)
{
  return make_lambda(name,arglist,body,env,nd,sync,0);
}

KNO_EXPORT void recycle_lambda(struct KNO_RAW_CONS *c)
{
  struct KNO_LAMBDA *lambda = (struct KNO_LAMBDA *)c;
  int mallocd = KNO_MALLOCD_CONSP(c), n_vars = lambda->lambda_n_vars;
  if (lambda->fcn_typeinfo) u8_free(lambda->fcn_typeinfo);
  if (lambda->fcn_defaults) u8_free(lambda->fcn_defaults);
  if ( (lambda->fcn_doc) && (lambda->fcn_free_doc) )
    u8_free(lambda->fcn_doc);
  if (lambda->fcn_attribs) kno_decref(lambda->fcn_attribs);
  if (lambda->fcn_moduleid) kno_decref(lambda->fcn_moduleid);
  kno_decref(lambda->lambda_arglist);
  kno_decref(lambda->lambda_body);
  kno_decref(lambda->lambda_source);
  u8_free(lambda->lambda_vars);
  if (lambda->lambda_inits) {
    kno_decref_vec(lambda->lambda_inits,n_vars);
    u8_free(lambda->lambda_inits);}
  if (lambda->lambda_env->env_copy) {
    kno_decref((lispval)(lambda->lambda_env->env_copy));
    /* kno_recycle_lexenv(lambda->lambda_env->env_copy); */
  }

  if (lambda->lambda_consblock) {
    lispval cb = (lispval) lambda->lambda_consblock;
    lambda->lambda_consblock=NULL;
    kno_decref(cb);}
  else if (lambda->lambda_start != lambda->lambda_body) {
    /* kno_decref(lambda->lambda_start); */ }
  else {}
  lambda->lambda_start = KNO_VOID;

  if (lambda->lambda_synchronized)
    u8_destroy_mutex(&(lambda->lambda_lock));
  if (lambda->lambda_consblock) {
    lispval bc = (lispval)(lambda->lambda_consblock);
    lambda->lambda_consblock = NULL;
    kno_decref(bc);}
  /* Put these last to help with debugging, when needed */
  if (lambda->fcn_name) u8_free(lambda->fcn_name);
  if (lambda->fcn_filename) u8_free(lambda->fcn_filename);
  if (mallocd) {
    memset(lambda,0,sizeof(struct KNO_LAMBDA));
    u8_free(lambda);}
}

static void output_callsig(u8_output out,lispval arglist);

static int unparse_lambda(u8_output out,lispval x)
{
  struct KNO_LAMBDA *lambda = kno_consptr(kno_lambda,x,kno_lambda_type);
  lispval arglist = lambda->lambda_arglist;
  kno_ptrval addr = (kno_ptrval) lambda;
  lispval moduleid = lambda->fcn_moduleid;
  u8_string modname =
    (KNO_SYMBOLP(moduleid)) ? (KNO_SYMBOL_NAME(moduleid)) : (NULL);
  u8_string codes=
    (((lambda->lambda_synchronized)&&(lambda->fcn_ndcall))?("∀∥"):
     (lambda->lambda_synchronized)?("∥"):
     (lambda->fcn_ndcall)?("∀"):(""));
  if (lambda->fcn_name)
    u8_printf(out,"#<λ%s%s",codes,lambda->fcn_name);
  else u8_printf(out,"#<λ%s0x%04x",codes,((addr>>2)%0x10000));
  u8_byte namebuf[100];
  u8_string sig = kno_fcn_sig((kno_function)lambda,namebuf);
  if (sig)
    u8_puts(out,sig);
  else if (PAIRP(arglist))
    output_callsig(out,arglist);
  else if (NILP(arglist))
    u8_puts(out,"()");
  else if (SYMBOLP(arglist))
    u8_printf(out,"(%s…)",SYM_NAME(arglist));
  else u8_printf(out,"(…%q…)",arglist);
  if (!(lambda->fcn_name))
    u8_printf(out," #!0x%llx",KNO_LONGVAL(lambda));
  if (modname) {
    u8_putc(out,' ');
    u8_puts(out,modname);}
  if (lambda->fcn_filename) {
    u8_string filename=lambda->fcn_filename;
    /* Elide information after the filename (such as time/size/hash) */
    u8_string space=strchr(filename,' ');
    u8_puts(out," '");
    if (space) u8_putn(out,filename,space-filename);
    else u8_puts(out,filename);
    u8_puts(out,"'>");}
  else u8_puts(out,">");
  return 1;
}

static void output_callsig(u8_output out,lispval arglist)
{
  int first = 1; lispval scan = arglist;
  lispval spec = VOID, arg = VOID;
  u8_putc(out,'(');
  while (PAIRP(scan)) {
    if (first) first = 0; else u8_putc(out,' ');
    spec = KNO_CAR(scan);
    arg = SYMBOLP(spec)?(spec):(PAIRP(spec))?(KNO_CAR(spec)):(VOID);
    if (SYMBOLP(arg))
      u8_puts(out,SYM_NAME(arg));
    else u8_puts(out,"??");
    if (PAIRP(spec)) u8_putc(out,'?');
    scan = KNO_CDR(scan);}
  if (NILP(scan))
    u8_putc(out,')');
  else if (SYMBOLP(scan))
    u8_printf(out,"%s…)",SYM_NAME(scan));
  else u8_printf(out,"…%q…)",scan);
}

static int *copy_intvec(int *vec,int n,int *into)
{
  int *dest = (into)?(into):(u8_alloc_n(n,int));
  int i = 0; while (i<n) {
    dest[i]=vec[i]; i++;}
  return dest;
}

KNO_EXPORT lispval copy_lambda(struct KNO_CONS *c,int flags)
{
  struct KNO_LAMBDA *lambda = (struct KNO_LAMBDA *)c;
  if (lambda->lambda_synchronized) {
    lispval sp = (lispval)lambda;
    kno_incref(sp);
    return sp;}
  else {
    struct KNO_LAMBDA *fresh = u8_alloc(struct KNO_LAMBDA);
    int n_args = lambda->lambda_n_vars+1, arity = lambda->fcn_arity;
    memcpy(fresh,lambda,sizeof(struct KNO_LAMBDA));

    /* This sets a new reference count or declares it static */
    KNO_INIT_FRESH_CONS(fresh,kno_lambda_type);

    if (lambda->fcn_name) fresh->fcn_name = u8_strdup(lambda->fcn_name);
    if (lambda->fcn_filename)
      fresh->fcn_filename = u8_strdup(lambda->fcn_filename);
    if (lambda->fcn_typeinfo)
      fresh->fcn_typeinfo = copy_intvec(lambda->fcn_typeinfo,arity,NULL);

    fresh->fcn_attribs = VOID;
    fresh->lambda_arglist = kno_copier(lambda->lambda_arglist,flags);
    fresh->lambda_body = kno_copier(lambda->lambda_body,flags);
    fresh->lambda_source = lambda->lambda_source;
    kno_incref(lambda->lambda_source);
    fresh->lambda_consblock = NULL;
    if (lambda->lambda_vars)
      fresh->lambda_vars = kno_copy_vec(lambda->lambda_vars,n_args,NULL,flags);
    if (lambda->fcn_defaults)
      fresh->fcn_defaults = kno_copy_vec(lambda->fcn_defaults,arity,NULL,flags);

    fresh->lambda_start = fresh->lambda_body;
    fresh->lambda_consblock = NULL;

    if (fresh->lambda_synchronized)
      u8_init_mutex(&(fresh->lambda_lock));

    if (U8_BITP(flags,KNO_STATIC_COPY)) {
      KNO_MAKE_CONS_STATIC(fresh);}

    return (lispval) fresh;}
}

/* LAMBDA generators */

static lispval lambda_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval arglist = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  lispval proc = VOID;
  if (VOIDP(arglist))
    return kno_err(kno_TooFewExpressions,"LAMBDA",NULL,expr);
  if (KNO_CODEP(body)) {
    kno_incref(arglist);
    proc=_make_lambda(NULL,arglist,body,env,0,0,0,0,0);}
  else proc=make_lambda(NULL,arglist,body,env,0,0,0);
  KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

static lispval ambda_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval arglist = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  lispval proc = VOID;
  if (VOIDP(arglist))
    return kno_err(kno_TooFewExpressions,"AMBDA",NULL,expr);
  if (KNO_CODEP(body)) {
    kno_incref(arglist);
    proc=_make_lambda(NULL,arglist,body,env,1,0,0,0,0);}
  else proc=make_lambda(NULL,arglist,body,env,1,0,0);
  KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

static lispval nlambda_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval name_expr = kno_get_arg(expr,1), name;
  lispval arglist = kno_get_arg(expr,2);
  lispval body = kno_get_body(expr,3);
  lispval proc = VOID;
  u8_string namestring = NULL;
  if ((VOIDP(name_expr))||(VOIDP(arglist)))
    return kno_err(kno_TooFewExpressions,"NLAMBDA",NULL,expr);
  else name = kno_eval(name_expr,env);
  if (SYMBOLP(name)) namestring = SYM_NAME(name);
  else if (STRINGP(name)) namestring = CSTRING(name);
  else return kno_type_error("procedure name (string or symbol)",
                            "nlambda_evalfn",name);
  if (KNO_CODEP(body)) {
    kno_incref(arglist);
    proc=_make_lambda(namestring,arglist,body,env,1,0,0,0,0);}
  else proc=make_lambda(namestring,arglist,body,env,1,0,0);
  KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

static lispval def_helper(lispval expr,kno_lexenv env,int nd,int sync)
{
  lispval template = kno_get_arg(expr,1);
  if (!(KNO_PAIRP(template)))
    return kno_err(kno_SyntaxError,"def_evalfn",NULL,template);
  lispval name = KNO_CAR(template);
  lispval arglist = KNO_CDR(template);
  lispval body = kno_get_body(expr,2);
  lispval proc = VOID;
  u8_string namestring = NULL;
  if (SYMBOLP(name)) namestring = SYM_NAME(name);
  else if (STRINGP(name)) namestring = CSTRING(name);
  else return kno_type_error
         ("procedure name (string or symbol)","def_evalfn",name);
  if (KNO_CODEP(body)) {
    kno_incref(arglist);
    proc=_make_lambda(namestring,arglist,body,env,1,0,0,0,0);}
  else proc=make_lambda(namestring,arglist,body,env,1,0,0);
  KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

static lispval def_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return def_helper(expr,env,0,0);
}

static lispval defamb_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return def_helper(expr,env,1,0);
}

static lispval defsync_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return def_helper(expr,env,0,1);
}

static lispval slambda_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval arglist = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  lispval proc = VOID;
  if (VOIDP(arglist))
    return kno_err(kno_TooFewExpressions,"SLAMBDA",NULL,expr);
 if (KNO_CODEP(body)) {
    kno_incref(arglist);
    proc=_make_lambda(NULL,arglist,body,env,0,1,0,0,0);}
 else proc=make_lambda(NULL,arglist,body,env,0,1,0);
 KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

static lispval sambda_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval arglist = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  lispval proc = VOID;
  if (VOIDP(arglist))
    return kno_err(kno_TooFewExpressions,"SLAMBDA",NULL,expr);
 if (KNO_CODEP(body)) {
    kno_incref(arglist);
    proc=_make_lambda(NULL,arglist,body,env,1,1,0,0,0);}
 else proc=make_lambda(NULL,arglist,body,env,1,1,0);
 KNO_SET_LAMBDA_SOURCE(proc,expr);
 return proc;
}

static lispval thunk_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = kno_get_body(expr,1);
  lispval proc = VOID;
  if (KNO_CODEP(body))
    return _make_lambda(NULL,NIL,body,env,0,0,0,0,0);
  else return make_lambda(NULL,NIL,body,env,0,0,0);
  KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

/* DEFINE */

static void init_definition(lispval fcn,lispval expr,kno_lexenv env)
{
  struct KNO_FUNCTION *f = (kno_function) fcn;
  if ( (KNO_NULLP(f->fcn_moduleid)) || (KNO_VOIDP(f->fcn_moduleid)) ) {
    lispval moduleid = kno_get(env->env_bindings,moduleid_symbol,KNO_VOID);
    if (!(KNO_VOIDP(moduleid)))
      f->fcn_moduleid = moduleid;}
  if (f->fcn_filename == NULL) {
    u8_string sourcebase = kno_sourcebase();
    if (sourcebase) f->fcn_filename = u8_strdup(sourcebase);}
  if ( (kno_record_source) && (KNO_LAMBDAP(fcn)) )  {
    struct KNO_LAMBDA *l = (kno_lambda) fcn;
    if ( (KNO_NULLP(l->lambda_source)) || (KNO_VOIDP(l->lambda_source)) ) {
      l->lambda_source=expr;
      kno_incref(expr);}}
}

static lispval define_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"DEFINE",NULL,expr);
  else if (SYMBOLP(var)) {
    lispval val_expr = kno_get_arg(expr,2);
    if (VOIDP(val_expr))
      return kno_err(kno_TooFewExpressions,"DEFINE",NULL,expr);
    else {
      lispval value = kno_eval(val_expr,env);
      if (KNO_ABORTED(value))
        return value;
      else if (kno_bind_value(var,value,env)>=0) {
        lispval fvalue = (KNO_FCNIDP(value))?(kno_fcnid_ref(value)):(value);
        if (KNO_FUNCTIONP(fvalue)) init_definition(fvalue,expr,env);
        if (KNO_MACROP(fvalue)) {
          struct KNO_MACRO *macro = (kno_macro) fvalue;
          if (KNO_VOIDP(macro->macro_moduleid)) {
            macro->macro_moduleid =
              kno_get(env->env_bindings,moduleid_symbol,KNO_VOID);}}
        kno_decref(value);
        return VOID;}
      else {
        kno_decref(value);
        return kno_err(kno_BindError,"DEFINE",SYM_NAME(var),var);}}}
  else if (PAIRP(var)) {
    lispval fn_name = KNO_CAR(var), args = KNO_CDR(var);
    lispval body = kno_get_body(expr,2);
    if (!(SYMBOLP(fn_name)))
      return kno_err(kno_NotAnIdentifier,"DEFINE",NULL,fn_name);
    else {
      lispval value = make_lambda(SYM_NAME(fn_name),args,body,env,0,0,1);
      if (KNO_ABORTED(value))
        return value;
      else if (kno_bind_value(fn_name,value,env)>=0) {
        lispval fvalue = (KNO_FCNIDP(value))?(kno_fcnid_ref(value)):(value);
        if (KNO_FUNCTIONP(fvalue)) init_definition(fvalue,expr,env);
        if (KNO_MACROP(fvalue)) {
          struct KNO_MACRO *macro = (kno_macro) fvalue;
          if (KNO_VOIDP(macro->macro_moduleid)) {
            macro->macro_moduleid =
              kno_get(env->env_bindings,moduleid_symbol,KNO_VOID);}}
        kno_decref(value);
        return VOID;}
      else {
        kno_decref(value);
        return kno_err(kno_BindError,"DEFINE",SYM_NAME(fn_name),var);}}}
  else return kno_err(kno_NotAnIdentifier,"DEFINE",NULL,var);
}

static lispval defslambda_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"DEFINE-SYNCHRONIZED",NULL,expr);
  else if (SYMBOLP(var))
    return kno_err(kno_BadDefineForm,"DEFINE-SYNCHRONIZED",NULL,expr);
  else if (PAIRP(var)) {
    lispval fn_name = KNO_CAR(var), args = KNO_CDR(var);
    if (!(SYMBOLP(fn_name)))
      return kno_err(kno_NotAnIdentifier,"DEFINE-SYNCHRONIZED",NULL,fn_name);
    else {
      lispval body = kno_get_body(expr,2);
      lispval value = make_lambda(SYM_NAME(fn_name),args,body,env,0,1,1);
      if (KNO_ABORTED(value))
        return value;
      else if (kno_bind_value(fn_name,value,env)>=0) {
        lispval opvalue = (KNO_FCNIDP(value))?(kno_fcnid_ref(value)):(value);
        if (KNO_FUNCTIONP(opvalue)) init_definition(opvalue,expr,env);
        kno_decref(value);
        return VOID;}
      else {
        kno_decref(value);
        return kno_err(kno_BindError,"DEFINE-SYNCHRONIZED",
                      SYM_NAME(var),var);}}}
  else return kno_err(kno_NotAnIdentifier,"DEFINE-SYNCHRONIZED",NULL,var);
}

static lispval defambda_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"DEFINE-AMB",NULL,expr);
  else if (SYMBOLP(var))
    return kno_err(kno_BadDefineForm,"DEFINE-AMB",SYM_NAME(var),expr);
  else if (PAIRP(var)) {
    lispval fn_name = KNO_CAR(var), args = KNO_CDR(var);
    lispval body = kno_get_body(expr,2);
    if (!(SYMBOLP(fn_name)))
      return kno_err(kno_NotAnIdentifier,"DEFINE-AMB",NULL,fn_name);
    else {
      lispval value = make_lambda(SYM_NAME(fn_name),args,body,env,1,0,1);
      if (KNO_ABORTED(value)) return value;
      else if (kno_bind_value(fn_name,value,env)>=0) {
        lispval opvalue = kno_fcnid_ref(value);
        if (KNO_FUNCTIONP(opvalue)) init_definition(opvalue,expr,env);
        kno_decref(value);
        return VOID;}
      else {
        kno_decref(value);
        return kno_err(kno_BindError,"DEFINE-AMB",SYM_NAME(fn_name),var);}}}
  else return kno_err(kno_NotAnIdentifier,"DEFINE-AMB",NULL,var);
}

/* Extended apply */

static lispval tail_symbol;

KNO_EXPORT
/* kno_xapply_lambda:
     Arguments: a pointer to an lambda, a void* data pointer, and a function
      of a void* pointer and a dtype pointer
     Returns: the application result
  This uses an external function to get parameter values from some
  other data structure (cast as a void* pointer).  This is used, for instance,
  to expose CGI data fields as arguments to a main function, or to
  apply XML attributes and elements similarly. */
lispval kno_xapply_lambda
  (struct KNO_LAMBDA *fn,void *data,lispval (*getval)(void *,lispval))
{
  KNO_SETUP_NAMED_STACK(_stack,kno_stackptr,"xapply",fn->fcn_name,(lispval)fn);
  int n = fn->lambda_n_vars;
  lispval arglist = fn->lambda_arglist, result = VOID;
  kno_lexenv env = fn->lambda_env;
  INIT_STACK_SCHEMA(_stack,call_env,env,n,fn->lambda_vars);
  while (PAIRP(arglist)) {
    lispval argspec = KNO_CAR(arglist), argname = VOID, argval;
    if (SYMBOLP(argspec)) argname = argspec;
    else if (PAIRP(argspec)) argname = KNO_CAR(argspec);
    if (!(SYMBOLP(argname)))
      _return kno_err(kno_BadArglist,fn->fcn_name,NULL,fn->lambda_arglist);
    argval = getval(data,argname);
    if (KNO_ABORTED(argval))
      _return argval;
    else if (( (VOIDP(argval)) || (argval == KNO_DEFAULT_VALUE) ) &&
             (PAIRP(argspec)) && (PAIRP(KNO_CDR(argspec)))) {
      lispval default_expr = KNO_CADR(argspec);
      lispval default_value = kno_eval(default_expr,fn->lambda_env);
      kno_schemap_store(&call_env_bindings,argname,default_value);
      kno_decref(default_value);}
    else {
      kno_schemap_store(&call_env_bindings,argname,argval);
      kno_decref(argval);}
    arglist = KNO_CDR(arglist);}
  /* This means we have a lexpr arg. */
  /* If we're synchronized, lock the mutex. */
  if (fn->lambda_synchronized) u8_lock_mutex(&(fn->lambda_lock));
  result = eval_inner_body(":XPROC",fn->fcn_name,fn->lambda_body,0,call_env,_stack);
  /* if (fn->lambda_synchronized) result = kno_finish_call(result); */
  /* We always finish tail calls here */
  result = kno_finish_call(result);
  if (fn->lambda_synchronized)
    u8_unlock_mutex(&(fn->lambda_lock));
  _return result;
}

static lispval tablegetval(void *obj,lispval var)
{
  lispval tbl = (lispval)obj;
  return kno_get(tbl,var,VOID);
}

static lispval xapply_prim(lispval proc,lispval obj)
{
  struct KNO_LAMBDA *lambda = kno_consptr(kno_lambda,proc,kno_lambda_type);
  if (!(TABLEP(obj)))
    return kno_type_error("table","xapply_prim",obj);
  return kno_xapply_lambda(lambda,(void *)obj,tablegetval);
}

/* Walking an lambda */

static int walk_lambda(kno_walker walker,lispval obj,void *walkdata,
                      kno_walk_flags flags,int depth)
{
  struct KNO_LAMBDA *lambda = (kno_lambda)obj;
  lispval env = (lispval)lambda->lambda_env;
  if (kno_walk(walker,lambda->lambda_body,walkdata,flags,depth-1)<0)
    return -1;
  else if (kno_walk(walker,lambda->lambda_arglist,
                   walkdata,flags,depth-1)<0)
    return -1;
  else if ( (!(KNO_STATICP(env))) &&
            (kno_walk(walker,env,walkdata,flags,depth-1)<0) )
    return -1;
  else return 3;
}

/* Unparsing fcnids referring to lambdas */

static int unparse_extended_fcnid(u8_output out,lispval x)
{
  lispval lp = kno_fcnid_ref(x);
  if (TYPEP(lp,kno_lambda_type)) {
    struct KNO_LAMBDA *lambda = kno_consptr(kno_lambda,lp,kno_lambda_type);
    kno_ptrval addr = (kno_ptrval) lambda;
    lispval arglist = lambda->lambda_arglist;
    u8_string codes=
      (((lambda->lambda_synchronized)&&(lambda->fcn_ndcall))?("∀∥"):
       (lambda->lambda_synchronized)?("∥"):
       (lambda->fcn_ndcall)?("∀"):(""));
    if (lambda->fcn_name)
      u8_printf(out,"#<~%d<λ%s%s",
                KNO_GET_IMMEDIATE(x,kno_fcnid_type),
                codes,lambda->fcn_name);
    else u8_printf(out,"#<~%d<λ%s0x%04x",
                   KNO_GET_IMMEDIATE(x,kno_fcnid_type),
                   codes,((addr>>2)%0x10000));
    if (PAIRP(arglist)) {
      int first = 1; lispval scan = lambda->lambda_arglist;
      lispval spec = VOID, arg = VOID;
      u8_putc(out,'(');
      while (PAIRP(scan)) {
        if (first) first = 0; else u8_putc(out,' ');
        spec = KNO_CAR(scan);
        arg = (SYMBOLP(spec)) ? (spec) :
          (PAIRP(spec)) ? (KNO_CAR(spec)) :
          (VOID);
        if (SYMBOLP(arg))
          u8_puts(out,SYM_NAME(arg));
        else u8_puts(out,"??");
        if (PAIRP(spec)) u8_putc(out,'?');
        scan = KNO_CDR(scan);}
      if (NILP(scan))
        u8_putc(out,')');
      else if (SYMBOLP(scan))
        u8_printf(out,"%s…)",SYM_NAME(scan));
      else u8_printf(out,"…%q…)",scan);}
    else if (NILP(arglist))
      u8_puts(out,"()");
    else if (SYMBOLP(arglist))
      u8_printf(out,"(%s…)",SYM_NAME(arglist));
    else u8_printf(out,"(…%q…)",arglist);
    if (!(lambda->fcn_name))
      u8_printf(out," #!0x%llx",KNO_LONGVAL(lambda));
    if (lambda->fcn_filename)
      u8_printf(out," '%s'>>",lambda->fcn_filename);
    else u8_puts(out,">>");
    return 1;}
  else if (TYPEP(lp,kno_cprim_type)) {
    struct KNO_FUNCTION *fcn = (kno_function)lp;
    kno_ptrval addr = (kno_ptrval) fcn;
    u8_string name = fcn->fcn_name;
    u8_string filename = fcn->fcn_filename;
    u8_byte arity[64]="", codes[64]="", numbuf[32]="";
    if ((filename)&&(filename[0]=='\0')) filename = NULL;
    if (name == NULL) name = fcn->fcn_name;
    if (fcn->fcn_ndcall) strcat(codes,"∀");
    if ((fcn->fcn_arity<0)&&(fcn->fcn_min_arity<0))
      strcat(arity,"[…]");
    else if (fcn->fcn_arity == fcn->fcn_min_arity) {
      strcat(arity,"[");
      strcat(arity,u8_itoa10(fcn->fcn_arity,numbuf));
      strcat(arity,"]");}
    else if (fcn->fcn_arity<0) {
      strcat(arity,"[");
      strcat(arity,u8_itoa10(fcn->fcn_min_arity,numbuf));
      strcat(arity,"…]");}
    else {
      strcat(arity,"[");
      strcat(arity,u8_itoa10(fcn->fcn_min_arity,numbuf));
      strcat(arity,"-");
      strcat(arity,u8_itoa10(fcn->fcn_arity,numbuf));
      strcat(arity,"]");}
    if (name)
      u8_printf(out,"#<~%d<%s%s%s%s%s%s>>",
                KNO_GET_IMMEDIATE(x,kno_fcnid_type),
                codes,name,arity,U8OPTSTR("'",filename,"'"));
    else u8_printf(out,"#<~%d<Φ%s0x%04x%s #!0x%llx%s%s%s>>",
                   KNO_GET_IMMEDIATE(x,kno_fcnid_type),
                   codes,((addr>>2)%0x10000),arity,
                   KNO_LONGVAL( fcn),
                   arity,U8OPTSTR("'",filename,"'"));
    return 1;}
  else u8_printf(out,"#<~%ld %q>",
                 KNO_GET_IMMEDIATE(x,kno_fcnid_type),lp);
  return 1;
}

#define BUFOUT_FLAGS \
  (KNO_IS_WRITING|KNO_BUFFER_NO_FLUSH|KNO_USE_DTYPEV2|KNO_WRITE_OPAQUE)

static ssize_t write_lambda_dtype(struct KNO_OUTBUF *out,lispval x)
{
  int n_elts=1; /* Always include some source */
  struct KNO_LAMBDA *fcn = (struct KNO_LAMBDA *)x;
  unsigned char buf[200], *tagname="%LAMBDA";
  struct KNO_OUTBUF tmp = { 0 };
  KNO_INIT_OUTBUF(&tmp,buf,200,((out->buf_flags)&(BUFOUT_FLAGS)));
  kno_write_byte(&tmp,dt_compound);
  kno_write_byte(&tmp,dt_symbol);
  kno_write_4bytes(&tmp,strlen(tagname));
  kno_write_bytes(&tmp,tagname,strlen(tagname));
  if (fcn->fcn_name) n_elts++;
  if (fcn->fcn_filename) n_elts++;
  kno_write_byte(&tmp,dt_vector);
  kno_write_4bytes(&tmp,n_elts);
  if (fcn->fcn_name) {
    size_t len=strlen(fcn->fcn_name);
    kno_write_byte(&tmp,dt_symbol);
    kno_write_4bytes(&tmp,len);
    kno_write_bytes(&tmp,fcn->fcn_name,len);}
  if (fcn->fcn_filename) {
    size_t len=strlen(fcn->fcn_filename);
    kno_write_byte(&tmp,dt_string);
    kno_write_4bytes(&tmp,len);
    kno_write_bytes(&tmp,fcn->fcn_filename,len);}
  {
    kno_write_byte(&tmp,dt_pair);
    kno_write_dtype(&tmp,fcn->lambda_arglist);
    kno_write_dtype(&tmp,fcn->lambda_body);}
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  kno_write_bytes(out,tmp.buffer,n_bytes);
  kno_close_outbuf(&tmp);
  return n_bytes;
}

/* Initialization */

KNO_EXPORT void kno_init_lambdas_c()
{
  u8_register_source_file(_FILEINFO);

  tail_symbol = kno_intern("%tail");
  decls_symbol = kno_intern("%decls");
  flags_symbol = kno_intern("flags");
  moduleid_symbol = kno_intern("%moduleid");

  kno_applyfns[kno_lambda_type]=apply_lambda;
  kno_functionp[kno_lambda_type]=1;

  kno_unparsers[kno_lambda_type]=unparse_lambda;
  kno_recyclers[kno_lambda_type]=recycle_lambda;
  kno_walkers[kno_lambda_type]=walk_lambda;

  kno_unparsers[kno_fcnid_type]=unparse_extended_fcnid;

  kno_dtype_writers[kno_lambda_type] = write_lambda_dtype;

  kno_def_evalfn(kno_scheme_module,"LAMBDA","",lambda_evalfn);
  kno_def_evalfn(kno_scheme_module,"AMBDA","",ambda_evalfn);
  kno_def_evalfn(kno_scheme_module,"NLAMBDA","",nlambda_evalfn);
  kno_def_evalfn(kno_scheme_module,"SLAMBDA","",slambda_evalfn);
  kno_def_evalfn(kno_scheme_module,"SAMBDA","",sambda_evalfn);
  kno_def_evalfn(kno_scheme_module,"THUNK","",thunk_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFINE","",define_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFSLAMBDA","",defslambda_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFAMBDA","",defambda_evalfn);

  kno_def_evalfn(kno_scheme_module,"DEF",
                "Returns a named lambda procedure",
                def_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFAMB",
                "Returns a named non-determinstic lambda procedure",
                defamb_evalfn);
  kno_def_evalfn(kno_scheme_module,"DEFSYNC",
                "Returns a named synchronized lambda procedure",
                defsync_evalfn);

  kno_idefn(kno_scheme_module,kno_make_cprim2x
           ("XAPPLY",xapply_prim,2,kno_lambda_type,VOID,-1,VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
