/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_EVAL_INTERNALS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/cprims.h"
#include "kno/storage.h"

#include "eval_internals.h"
#include "../apply/apply_internals.h"

#include <libu8/u8printf.h>

#include <sys/resource.h>
#include "kno/profiles.h"

u8_string kno_lambda_stack_type = "lambda";

u8_condition kno_BadArglist=_("Malformed argument list");
u8_condition kno_BadDefineForm=_("Bad procedure defining form");

int kno_record_source=1;

static lispval tail_symbol, decls_symbol, flags_symbol;

#if 0
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
#endif

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

static lispval lambda_docall(kno_stack caller,
			     struct KNO_LAMBDA *proc,
			     short n,kno_argvec args)
{
  if (caller)
    return lambda_call(caller,proc,n,args,0);
 else {
    KNO_START_EVAL(xeval,proc->fcn_name,KNO_VOID,NULL,kno_stackptr);
    lispval result = lambda_call(xeval,proc,n,args,0);
    kno_pop_stack(xeval);
    return result;}
}

KNO_EXPORT lispval kno_lambda_call(kno_stack caller,
				   struct KNO_LAMBDA *proc,
				   int n,kno_argvec args)
{
  return lambda_docall(caller,proc,n,args);
}

static lispval apply_lambda(lispval fn,int n,kno_argvec args)
{
  struct KNO_LAMBDA *proc = (kno_lambda) fn;
  return lambda_docall(kno_stackptr,proc,n,args);
}

KNO_EXPORT int kno_set_lambda_schema(struct KNO_LAMBDA *s,lispval args)
{
  int i = 0, n_vars = 0, has_inits = 0, arity = 0, min_arity = -1;
  lispval scan = args;
  if (scan == KNO_EMPTY_LIST) {
    arity = 0; min_arity = 0;}
  else while (KNO_PAIRP(scan)) {
      lispval arg = KNO_CAR(scan);
      if (KNO_SYMBOLP(arg)) {}
      else if (KNO_PAIRP(arg)) {
        if ( (KNO_SYMBOLP(KNO_CAR(arg))) &&
	     ( (KNO_CDR(arg) == KNO_EMPTY_LIST) ||
               (KNO_PAIRP(KNO_CDR(arg))) ) ) {
          if (min_arity < 0) min_arity = i;
	  has_inits = 1;}
        else {
          kno_seterr(kno_SyntaxError,"lambda",s->fcn_name,args);
          return -1;}}
      else {
        kno_seterr(kno_SyntaxError,"lambda",s->fcn_name,args);
        return -1;}
      scan = KNO_CDR(scan);
      n_vars++;
      i++;}
  if (min_arity < 0) min_arity = i;
  if (KNO_EMPTY_LISTP(scan))
    arity = i;
  else if (KNO_SYMBOLP(scan)) {
    n_vars++;
    arity = -1;}
  else {
    kno_seterr(kno_SyntaxError,"lambda_args",s->fcn_name,args);
    return -1;}
  lispval *cur_vars = s->lambda_vars;
  lispval *cur_inits = s->lambda_inits;
  int n_cur = s->lambda_n_vars;
  if (n_vars) {
    lispval *schema = s->lambda_vars = u8_alloc_n((n_vars),lispval);
    lispval *inits = s->lambda_inits = (has_inits) ?
      (u8_alloc_n((n_vars),lispval)) :
      (NULL);
    i = 0; scan = args; while (PAIRP(scan)) {
      lispval arg = KNO_CAR(scan);
      if (KNO_PAIRP(arg)) {
        schema[i] = KNO_CAR(arg);
        if (KNO_PAIRP(KNO_CDR(arg))) {
          lispval init = KNO_CADR(arg);
          if (inits) inits[i] = kno_incref(init);}
        else if (inits) inits[i] = KNO_VOID;}
      else {
        schema[i]=arg;
        if (inits) inits[i]=KNO_VOID;}
      scan = KNO_CDR(scan);
      i++;}
    if (KNO_SYMBOLP(scan)) {
      schema[i] = scan;
      if (inits) inits[i] = KNO_NIL;}}
  else {
    s->lambda_vars = NULL;
    s->lambda_inits = NULL;}
  s->fcn_call_width = s->lambda_n_vars = n_vars;
  s->fcn_arity = arity;
  s->fcn_min_arity = min_arity;
  if (cur_vars) u8_free(cur_vars);
  if (cur_inits) {
    kno_decref_vec(cur_inits,n_cur);
    u8_free(cur_inits);}
  return n_vars;
}

static lispval process_docstring(struct KNO_LAMBDA *s,u8_string name,
                                 lispval arglist,lispval body)
{
  int merge_docstrings = (s->fcn_doc == NULL);
  struct U8_OUTPUT docstream;
  if (merge_docstrings) {
    U8_INIT_OUTPUT(&docstream,256);}
  int n_lines = 0;
  while ( (PAIRP(body)) &&
          (STRINGP(KNO_CAR(body))) &&
          (PAIRP(KNO_CDR(body))) ) {
    if (!(merge_docstrings)) {
      body = KNO_CDR(body);
      continue;}
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
    s->fcn_free |= KNO_FCN_FREE_DOC;}
  else u8_close_output(&docstream);
  return body;
}

static lispval process_decls(struct KNO_LAMBDA *s,lispval body,
			     kno_lexenv env,kno_stack stack)
{
  lispval decls = KNO_CAR(body);
  int decref_table = 0;
  lispval attribs = s->fcn_attribs, table = VOID;
  if (KNO_SLOTMAPP(decls))
    table = decls;
  else if (KNO_SCHEMAPP(decls)) {
    table = kno_eval(decls,env,stack,0);
    decref_table = 1;}
  else return body;
  if (KNO_ABORTP(table)) return table;
  if (attribs == KNO_VOID)
    s->fcn_attribs = attribs = kno_make_slotmap(4,0,NULL);
  lispval assocs = kno_getassocs(table);
  DO_CHOICES(keyval,assocs) {
    kno_add(attribs,KNO_CAR(keyval),KNO_CDR(keyval));}
  kno_decref(assocs);
  if (decref_table) kno_decref(table);
  return KNO_CDR(body);
}

static lispval process_body(struct KNO_LAMBDA *s,u8_string name,
                            lispval arglist,lispval body,
			    kno_lexenv env,kno_stack stack)
{
  lispval scan = body;
  while ( (KNO_PAIRP(scan)) && (KNO_PAIRP(KNO_CDR(scan))) ) {
    lispval elt = KNO_CAR(scan);
    if (KNO_STRINGP(elt))
      scan = process_docstring(s,name,arglist,scan);
    else if ( (KNO_SLOTMAPP(elt)) || (KNO_SCHEMAPP(elt)) )
      scan = process_decls(s,scan,env,stack);
    else return scan;}
  return scan;
}

static lispval
_make_lambda(u8_string name,
             lispval arglist,lispval body,
	     kno_lexenv env,kno_stack stack,
             int nd,int sync,
             int incref,int copy_env)
{
  struct KNO_LAMBDA *s = u8_alloc(struct KNO_LAMBDA);
  KNO_INIT_FRESH_CONS(s,kno_lambda_type);
  s->fcn_name = ((name) ? (u8_strdup(name)) : (NULL));

  s->fcn_call = ( ((nd) ? (KNO_CALL_NDCALL) : (0)) |
		  (KNO_CALL_XCALL) );
  s->fcn_handler.xcalln = (kno_xprimn) lambda_docall;
  s->fcn_filename = NULL;
  s->fcn_attribs = VOID;
  s->fcnid = VOID;
  s->lambda_consblock = NULL;
  s->lambda_source = VOID;

  int n_vars = kno_set_lambda_schema(s,arglist);
  if (n_vars < 0) {
    if (name) u8_free(s->fcn_name);
    u8_free(s);
    return KNO_ERROR;}
  if (! (n_vars) ) {
    s->lambda_vars = NULL;
    s->lambda_inits = NULL;}

  /* Process the body */
  body = process_body(s,name,arglist,body,env,stack);
  if (KNO_ABORTP(body)) {
    return body;}
  else if (incref) {
    s->lambda_body = kno_incref(body);
    s->lambda_arglist = kno_incref(arglist);}
  else {
    s->lambda_body = body;
    s->lambda_arglist = arglist;}
  s->lambda_start = body;

  if (env == NULL)
    s->lambda_env = env;
  else s->lambda_env = kno_copy_env(env);
  /* Maybe don't copy if it's static, i.e. not
     ( (copy_env) || (KNO_MALLOCD_CONSP(env)) ) */
  /* s->lambda_env = env; */
  if (sync) {
    s->lambda_synchronized = 1;
    u8_init_mutex(&(s->lambda_lock));}
  else s->lambda_synchronized = 0;

  return LISP_CONS(s);
}

static lispval make_lambda(u8_string name,
                           lispval arglist,lispval body,
			   kno_lexenv env,kno_stack stack,
                           int nd,int sync)
{
  return _make_lambda(name,arglist,body,env,stack,nd,sync,1,1);
}

KNO_EXPORT lispval kno_make_lambda(u8_string name,
				   lispval arglist,lispval body,
				   kno_lexenv env,
				   int nd,int sync)
{
  return make_lambda(name,arglist,body,env,kno_stackptr,nd,sync);
}

KNO_EXPORT void recycle_lambda(struct KNO_RAW_CONS *c)
{
  struct KNO_LAMBDA *lambda = (struct KNO_LAMBDA *)c;
  int mallocd = KNO_MALLOCD_CONSP(c), n_vars = lambda->lambda_n_vars;
  int free_flags = lambda->fcn_free;
  if ( (lambda->fcn_doc) && ( (free_flags) & (KNO_FCN_FREE_DOC) ) ) {
    u8_free(lambda->fcn_doc);
    lambda->fcn_doc = NULL;}
  if (lambda->fcn_attribs) kno_decref(lambda->fcn_attribs);
  if (lambda->fcn_moduleid) kno_decref(lambda->fcn_moduleid);
  if (lambda->fcn_profile) {
    if (lambda->fcn_profile->prof_label)
      u8_free(lambda->fcn_profile->prof_label);
    u8_free(lambda->fcn_profile);
    lambda->fcn_profile = NULL;}
  kno_decref(lambda->lambda_arglist);
  kno_decref(lambda->lambda_body);
  kno_decref(lambda->lambda_source);
  u8_free(lambda->lambda_vars);
  if (lambda->lambda_inits) {
    kno_decref_vec(lambda->lambda_inits,n_vars);
    u8_free(lambda->lambda_inits);
    lambda->lambda_inits=NULL;}
  if ( (lambda->lambda_env) &&
       (lambda->lambda_env->env_copy) ) {
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
    (( (lambda->lambda_synchronized) && (FCN_NDOPP(lambda)) ) ? ("∀∥") :
     (lambda->lambda_synchronized) ? ("∥") :
     (FCN_NDOPP(lambda)) ? ("∀") : (""));
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
    if (SYMBOLP(arg)) u8_puts(out,SYM_NAME(arg));
    if (PAIRP(spec)) u8_putc(out,'?');
    scan = KNO_CDR(scan);}
  if (NILP(scan))
    u8_putc(out,')');
  else if (SYMBOLP(scan))
    u8_printf(out,"%s…)",SYM_NAME(scan));
}

KNO_EXPORT lispval copy_lambda(lispval c,int flags)
{
  struct KNO_LAMBDA *lambda = (struct KNO_LAMBDA *)c;
  if (lambda->lambda_synchronized) {
    lispval sp = (lispval)lambda;
    kno_incref(sp);
    return sp;}
  else {
    struct KNO_LAMBDA *fresh = u8_alloc(struct KNO_LAMBDA);
    int n_vars = lambda->lambda_n_vars;
    memcpy(fresh,lambda,sizeof(struct KNO_LAMBDA));

    /* This sets a new reference count or declares it static */
    KNO_INIT_CONS(fresh,kno_lambda_type);

    if (lambda->fcn_doc) {
      if (KNO_FCN_FREE_DOCP(lambda))
        fresh->fcn_doc = u8_strdup(lambda->fcn_doc);
      else fresh->fcn_doc = lambda->fcn_doc;}
    if (lambda->fcn_name)
      fresh->fcn_name = u8_strdup(lambda->fcn_name);
    if (lambda->fcn_filename)
      fresh->fcn_filename = u8_strdup(lambda->fcn_filename);
    if (lambda->lambda_env)
      fresh->lambda_env = kno_copy_env(lambda->lambda_env);
    fresh->fcn_attribs = VOID;

    fresh->lambda_arglist = kno_copier(lambda->lambda_arglist,flags);
    fresh->lambda_body = kno_copier(lambda->lambda_body,flags);
    fresh->lambda_source = lambda->lambda_source;
    kno_incref(lambda->lambda_source);
    fresh->lambda_consblock = NULL;
    if (lambda->lambda_vars)
      fresh->lambda_vars = kno_copy_vec(lambda->lambda_vars,n_vars,NULL,flags);
    if (lambda->lambda_inits) {
      fresh->lambda_inits = kno_copy_vec(lambda->lambda_inits,n_vars,NULL,flags);}

    fresh->lambda_start = fresh->lambda_body;
    fresh->lambda_consblock = NULL;

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
  proc=make_lambda(NULL,arglist,body,env,_stack,0,0);
  if (KNO_ABORTED(proc))
    return proc;
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
  proc=make_lambda(NULL,arglist,body,env,_stack,1,0);
  if (KNO_ABORTED(proc))
    return proc;
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
  else name = kno_eval(name_expr,env,_stack,0);
  if (KNO_ABORTED(name))
    return name;
  else if (SYMBOLP(name))
    namestring = SYM_NAME(name);
  else if (STRINGP(name))
    namestring = CSTRING(name);
  else {
    lispval err = kno_type_error("procedure name (string or symbol)",
                                 "nlambda_evalfn",name);
    kno_decref(name);
    return err;}
  proc=make_lambda(namestring,arglist,body,env,_stack,1,0);
  kno_decref(name);
  if (KNO_ABORTED(proc))
    return proc;
  KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

static lispval def_helper(lispval expr,
			  kno_lexenv env,kno_stack stack,
			  int nd,int sync)
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
  proc=make_lambda(namestring,arglist,body,env,stack,nd,sync);
  if (KNO_ABORTED(proc))
    return proc;
  KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

static lispval def_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return def_helper(expr,env,_stack,0,0);
}

static lispval defamb_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return def_helper(expr,env,_stack,1,0);
}

static lispval defsync_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return def_helper(expr,env,_stack,0,1);
}

static lispval slambda_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval arglist = kno_get_arg(expr,1);
  lispval body = kno_get_body(expr,2);
  lispval proc = VOID;
  if (VOIDP(arglist))
    return kno_err(kno_TooFewExpressions,"SLAMBDA",NULL,expr);
  proc=make_lambda(NULL,arglist,body,env,_stack,0,1);
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
  proc=make_lambda(NULL,arglist,body,env,_stack,1,1);
  if (KNO_ABORTED(proc))
    return proc;
  KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

static lispval thunk_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = kno_get_body(expr,1);
  lispval proc = make_lambda(NULL,NIL,body,env,_stack,0,0);
  if (KNO_ABORTED(proc))
    return proc;
  KNO_SET_LAMBDA_SOURCE(proc,expr);
  return proc;
}

/* DEFINE */

static void init_definition(lispval fcn,lispval expr,kno_lexenv env)
{
  struct KNO_FUNCTION *f = (kno_function) fcn;
  if ( (KNO_NULLP(f->fcn_moduleid)) || (KNO_VOIDP(f->fcn_moduleid)) ) {
    lispval moduleid = kno_get(env->env_bindings,KNOSYM_MODULEID,KNO_VOID);
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
    lispval value = kno_eval(val_expr,env,_stack,0);
    if (KNO_ABORTED(value))
      return value;
    else if (kno_bind_value(var,value,env)>=0) {
      lispval fvalue = (KNO_FCNIDP(value))?(kno_fcnid_ref(value)):(value);
      if (KNO_FUNCTIONP(fvalue))
	init_definition(fvalue,expr,env);
      else if (KNO_MACROP(fvalue)) {
	struct KNO_MACRO *macro = (kno_macro) fvalue;
	if (KNO_VOIDP(macro->macro_moduleid)) {
	  macro->macro_moduleid =
	    kno_get(env->env_bindings,KNOSYM_MODULEID,KNO_VOID);}
	if (macro->macro_name == NULL)
	  macro->macro_name = u8_strdup(KNO_SYMBOL_NAME(var));}
      else NO_ELSE;
      kno_decref(value);
      return VOID;}
    else {
      kno_decref(value);
      return kno_err(kno_BindError,"DEFINE",SYM_NAME(var),var);}}
  else if (PAIRP(var)) {
    lispval fn_name = KNO_CAR(var), args = KNO_CDR(var);
    lispval body = kno_get_body(expr,2);
    if (!(SYMBOLP(fn_name)))
      return kno_err(kno_NotAnIdentifier,"DEFINE",NULL,fn_name);
    else {
      lispval value = make_lambda(SYM_NAME(fn_name),args,body,env,_stack,0,0);
      if (KNO_ABORTED(value))
        return value;
      else if (kno_bind_value(fn_name,value,env)>=0) {
        lispval fvalue = (KNO_FCNIDP(value))?(kno_fcnid_ref(value)):(value);
        if (KNO_FUNCTIONP(fvalue)) init_definition(fvalue,expr,env);
        kno_decref(value);
        return VOID;}
      else {
        kno_decref(value);
        return kno_err(kno_BindError,"DEFINE",SYM_NAME(fn_name),var);}}}
  else return kno_err(kno_NotAnIdentifier,"DEFINE",NULL,var);
}

static lispval defexport_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval rv = define_evalfn(expr,env,_stack);
  if (ABORTED(rv)) return rv;
  lispval var = kno_get_arg(expr,1);
  if (PAIRP(var)) var = KNO_CAR(var);
  kno_hashtable exports =
    (HASHTABLEP(env->env_exports)) ? ((kno_hashtable)(env->env_exports)) :
    (kno_get_exports(env));
  lispval val = kno_get(env->env_bindings,var,VOID);
  kno_hashtable_store(exports,var,val);
  kno_decref(val);
  return rv;
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
      lispval value = make_lambda(SYM_NAME(fn_name),args,body,env,_stack,0,1);
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
      lispval value = make_lambda(SYM_NAME(fn_name),args,body,env,_stack,1,0);
      if (KNO_ABORTED(value))
        return value;
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
  u8_string label = (fn->fcn_name) ? (fn->fcn_name) : ((u8_string)"xapply");
  KNO_START_EVALX(_stack,label,((lispval)fn),NULL,kno_stackptr,7);
  int n = fn->lambda_n_vars;
  lispval arglist = fn->lambda_arglist, result = VOID;
  kno_lexenv env = fn->lambda_env;
  INIT_STACK_SCHEMA(_stack,call_env,env,n,fn->lambda_vars);
  KNO_STACK_SET_BITS(_stack,KNO_STACK_FREE_ENV);
  while (PAIRP(arglist)) {
    lispval argspec = KNO_CAR(arglist), argname = VOID, argval;
    if (SYMBOLP(argspec)) argname = argspec;
    else if (PAIRP(argspec)) argname = KNO_CAR(argspec);
    if (!(SYMBOLP(argname))) {
      result =kno_err(kno_BadArglist,fn->fcn_name,NULL,fn->lambda_arglist);
      break;}
    argval = getval(data,argname);
    if (KNO_ABORTED(argval)) {
      result = argval;
      break;}
    else if (( (VOIDP(argval)) || (argval == KNO_DEFAULT_VALUE) ||
	       (EMPTYP(argval)) ) &&
	     (PAIRP(argspec)) &&
	     (PAIRP(KNO_CDR(argspec)))) {
      lispval default_expr = KNO_CADR(argspec);
      lispval default_value = kno_eval(default_expr,fn->lambda_env,_stack,0);
      kno_schemap_store(&call_env_bindings,argname,default_value);
      kno_decref(default_value);}
    else {
      kno_schemap_store(&call_env_bindings,argname,argval);
      kno_decref(argval);}
    arglist = KNO_CDR(arglist);}
  if (!(ABORTED(result))) {
    /* If we're synchronized, lock the mutex. */
    if (fn->lambda_synchronized) u8_lock_mutex(&(fn->lambda_lock));
    result = eval_body(fn->lambda_start,call_env,_stack,
		       ":XPROC",fn->fcn_name,0);
    if (fn->lambda_synchronized)
      u8_unlock_mutex(&(fn->lambda_lock));}
   kno_pop_stack(_stack);
  return result;
}

static lispval tablegetval(void *obj,lispval var)
{
  lispval tbl = (lispval)obj;
  return kno_get(tbl,var,VOID);
}

static lispval xapplygetval(void *xobj,lispval var)
{
  lispval fn_obj = (lispval)xobj;
  if (KNO_PAIRP(fn_obj)) {
    lispval fn = KNO_CAR(fn_obj);
    if (KNO_APPLICABLEP(fn)) {
      lispval args[2] = { KNO_CDR(fn_obj), var };
      return kno_apply(fn,2,args);}
    else return kno_err(kno_NotAFunction,"xapplygetval",NULL,fn);}
  else return kno_err("InternalXapplyBug","xapplygetval",NULL,fn_obj);
}

DEFPRIM3("xapply",xapply_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
         "`(XAPPLY *arg0* *arg1* [*arg2*])` **undocumented**",
         kno_lambda_type,KNO_VOID,kno_any_type,KNO_VOID,
         kno_any_type,KNO_VOID);
static lispval xapply_prim(lispval proc,lispval obj,lispval getfn)
{
  struct KNO_LAMBDA *lambda = kno_consptr(kno_lambda,proc,kno_lambda_type);
  if ( (KNO_VOIDP(getfn)) || (KNO_DEFAULTP(getfn)) || (KNO_FALSEP(getfn)) ) {
    if (!(TABLEP(obj)))
      return kno_type_error("table","xapply_prim",obj);
    else return kno_xapply_lambda(lambda,(void *)obj,tablegetval);}
  else {
    struct KNO_PAIR _xobj = { 0 };
    KNO_INIT_STACK_CONS(&_xobj,kno_pair_type);
    _xobj.car = getfn; _xobj.cdr = obj;
    return kno_xapply_lambda(lambda,(void *)&_xobj,xapplygetval);}
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

static int better_unparse_fcnid(u8_output out,lispval x)
{
  lispval lp = kno_fcnid_ref(x);
  if (TYPEP(lp,kno_lambda_type)) {
    struct KNO_LAMBDA *lambda = kno_consptr(kno_lambda,lp,kno_lambda_type);
    kno_ptrval addr = (kno_ptrval) lambda;
    lispval arglist = lambda->lambda_arglist;
    int ndcallp = (FCN_NDOPP(lambda));
    u8_string codes=
      (((lambda->lambda_synchronized)&&(ndcallp))?("∀∥"):
       (lambda->lambda_synchronized)?("∥"):
       (ndcallp)?("∀"):(""));
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
        else u8_puts(out,"?anon?");
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
    if (FCN_NDOPP(fcn)) strcat(codes,"∀");
    if ((fcn->fcn_arity<0)&&(fcn->fcn_min_arity<0))
      strcat(arity,"[…]");
    else if (fcn->fcn_arity==fcn->fcn_min_arity) {
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

#define BUFOUT_FLAGS                                                    \
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

  kno_applyfns[kno_lambda_type]=apply_lambda;
  kno_isfunctionp[kno_lambda_type]=1;

  kno_unparsers[kno_lambda_type]=unparse_lambda;
  kno_recyclers[kno_lambda_type]=recycle_lambda;
  kno_walkers[kno_lambda_type]=walk_lambda;

  kno_unparsers[kno_fcnid_type]=better_unparse_fcnid;

  kno_dtype_writers[kno_lambda_type] = write_lambda_dtype;
  kno_copiers[kno_lambda_type] = copy_lambda;

  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"LAMBDA",lambda_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"AMBDA",ambda_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"NLAMBDA",nlambda_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"SLAMBDA",slambda_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"SAMBDA",sambda_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"THUNK",thunk_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DEFINE",define_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DEFEXPORT",defexport_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DEFSLAMBDA",defslambda_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"DEFAMBDA",defambda_evalfn,
		 "*undocumented*");

  kno_def_evalfn(kno_scheme_module,"DEF",def_evalfn,
                 "Returns a named lambda procedure");
  kno_def_evalfn(kno_scheme_module,"DEFAMB",defamb_evalfn,
                 "Returns a named non-determinstic lambda procedure");
  kno_def_evalfn(kno_scheme_module,"DEFSYNC",defsync_evalfn,
                 "Returns a named synchronized lambda procedure");
}



static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;
  KNO_LINK_PRIM("xapply",xapply_prim,3,scheme_module);
}
