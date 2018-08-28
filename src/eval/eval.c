/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"
#include "framerd/ffi.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

#if HAVE_MTRACE && HAVE_MCHECK_H
#include <mcheck.h>
#endif

#if HAVE_DLFCN_H
#include <dlfcn.h>
#endif

static volatile int scheme_initialized = 0;

u8_string fd_evalstack_type="eval";
u8_string fd_ndevalstack_type="ndeval";

int fd_optimize_tail_calls = 1;

fd_lexenv fd_app_env=NULL;
static lispval init_list = NIL;
static lispval module_list = NIL;
static lispval loadfile_list = NIL;

u8_string fd_bugdir = NULL;

lispval fd_scheme_module, fd_xscheme_module;

lispval _fd_comment_symbol;

static lispval quote_symbol, comment_symbol, moduleid_symbol, source_symbol;

static lispval log_testfail = FD_FALSE;

static u8_condition TestFailed=_("Test failed");
static u8_condition TestError=_("Test error");
static u8_condition ExpiredThrow=_("Continuation is no longer valid");
static u8_condition DoubleThrow=_("Continuation used twice");
static u8_condition LostThrow=_("Lost invoked continuation");

static char *cpu_profilename = NULL;

u8_condition
  fd_SyntaxError=_("SCHEME expression syntax error"),
  fd_InvalidMacro=_("invalid macro transformer"),
  fd_UnboundIdentifier=_("the variable is unbound"),
  fd_VoidArgument=_("VOID result passed as argument"),
  fd_VoidBinding=_("VOID result used as variable binding"),
  fd_NotAnIdentifier=_("not an identifier"),
  fd_TooFewExpressions=_("too few subexpressions"),
  fd_CantBind=_("can't add binding to environment"),
  fd_ReadOnlyEnv=_("Read only environment");

#define simplify_value(v) \
  ( (FD_PRECHOICEP(v)) ? (fd_simplify_choice(v)) : (v) )

/* Environment functions */

static int bound_in_envp(lispval symbol,fd_lexenv env)
{
  lispval bindings = env->env_bindings;
  if (HASHTABLEP(bindings))
    return fd_hashtable_probe((fd_hashtable)bindings,symbol);
  else if (SLOTMAPP(bindings))
    return fd_slotmap_test((fd_slotmap)bindings,symbol,VOID);
  else if (SCHEMAPP(bindings))
    return fd_schemap_test((fd_schemap)bindings,symbol,VOID);
  else return fd_test(bindings,symbol,VOID);
}

/* Lexrefs */

static lispval lexref_prim(lispval upv,lispval acrossv)
{
  long long up = FIX2INT(upv), across = FIX2INT(acrossv);
  long long combined = ((up<<5)|(across));
  /* Not the exact range limits, but good enough */
  if ((up>=0)&&(across>=0)&&(up<256)&&(across<256))
    return LISPVAL_IMMEDIATE(fd_lexref_type,combined);
  else if ((up<0)||(up>256))
    return fd_type_error("short","lexref_prim",up);
  else return fd_type_error("short","lexref_prim",across);
}

static lispval lexrefp_prim(lispval ref)
{
  if (FD_TYPEP(ref,fd_lexref_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval lexref_value_prim(lispval lexref)
{
  int code = FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up = code/32, across = code%32;
  return fd_init_pair(NULL,FD_INT(up),FD_INT(across));
}

static int unparse_lexref(u8_output out,lispval lexref)
{
  int code = FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up = code/32, across = code%32;
  u8_printf(out,"#<LEXREF ^%d+%d>",up,across);
  return 1;
}

static ssize_t write_lexref_dtype(struct FD_OUTBUF *out,lispval x)
{
  int code = FD_GET_IMMEDIATE(x,fd_lexref_type);
  int up = code/32, across = code%32;
  unsigned char buf[100], *tagname="%LEXREF";
  struct FD_OUTBUF tmp = { 0 };
  FD_INIT_OUTBUF(&tmp,buf,100,0);
  fd_write_byte(&tmp,dt_compound);
  fd_write_byte(&tmp,dt_symbol);
  fd_write_4bytes(&tmp,7);
  fd_write_bytes(&tmp,tagname,7);
  fd_write_byte(&tmp,dt_vector);
  fd_write_4bytes(&tmp,2);
  fd_write_byte(&tmp,dt_fixnum);
  fd_write_4bytes(&tmp,up);
  fd_write_byte(&tmp,dt_fixnum);
  fd_write_4bytes(&tmp,across);
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

/* Code refs */

static lispval coderef_prim(lispval offset)
{
  long long off = FIX2INT(offset);
  return LISPVAL_IMMEDIATE(fd_coderef_type,off);
}

static lispval coderef_value_prim(lispval offset)
{
  long long off = FD_GET_IMMEDIATE(offset,fd_coderef_type);
  return FD_INT(off);
}

static int unparse_coderef(u8_output out,lispval coderef)
{
  long long off = FD_GET_IMMEDIATE(coderef,fd_coderef_type);
  u8_printf(out,"#<CODEREF %lld>",off);
  return 1;
}

static ssize_t write_coderef_dtype(struct FD_OUTBUF *out,lispval x)
{
  int offset = FD_GET_IMMEDIATE(x,fd_coderef_type);
  unsigned char buf[100], *tagname="%CODEREF";
  struct FD_OUTBUF tmp = { 0 };
  FD_INIT_OUTBUF(&tmp,buf,100,0);
  fd_write_byte(&tmp,dt_compound);
  fd_write_byte(&tmp,dt_symbol);
  fd_write_4bytes(&tmp,strlen(tagname));
  fd_write_bytes(&tmp,tagname,strlen(tagname));
  fd_write_byte(&tmp,dt_fixnum);
  fd_write_4bytes(&tmp,offset);
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

/* Checking if eval is needed */

FD_EXPORT int fd_choice_evalp(lispval x)
{
  if (FD_NEED_EVALP(x))
    return 1;
  else if (FD_AMBIGP(x)) {
    FD_DO_CHOICES(e,x) {
      if (FD_NEED_EVALP(e)) {
        FD_STOP_DO_CHOICES;
        return 1;}}
    return 0;}
  else return 0;
}

/* Symbol lookup */

FD_EXPORT lispval _fd_symeval(lispval sym,fd_lexenv env)
{
  return fd_symeval(sym,env);
}

/* Assignments */

static int add_to_value(lispval sym,lispval val,fd_lexenv env)
{
  int rv=1;
  if (env) {
    lispval bindings=env->env_bindings;
    lispval exports=env->env_exports;
    if ((fd_add(bindings,sym,val))>=0) {
      if (HASHTABLEP(exports)) {
        lispval newval=fd_get(bindings,sym,EMPTY);
        if (FD_ABORTP(newval))
          rv=-1;
        else {
          int e_rv=fd_hashtable_op
            ((fd_hashtable)exports,fd_table_replace,sym,newval);
          fd_decref(newval);
          if (e_rv<0) rv=e_rv;}}}
    else rv=-1;}
  if (rv<0) {
    fd_seterr(fd_CantBind,"add_to_env_value",NULL,sym);
    return -1;}
  else return rv;
}

FD_EXPORT int fd_bind_value(lispval sym,lispval val,fd_lexenv env)
{
  /* TODO: Check for checking the return value of calls to
     `fd_bind_value` */
  if (env) {
    if (fd_store(env->env_bindings,sym,val)<0) {
      fd_poperr(NULL,NULL,NULL,NULL);
      fd_seterr(fd_CantBind,"fd_bind_value",NULL,sym);
      return -1;}
    if (HASHTABLEP(env->env_exports))
      fd_hashtable_op((fd_hashtable)(env->env_exports),
                      fd_table_replace,sym,val);
    return 1;}
  else return 0;
}

FD_EXPORT int fd_add_value(lispval symbol,lispval value,fd_lexenv env)
{
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env = env->env_parent;
      if ((env) && (env->env_copy))
        env = env->env_copy;}
    else if ((env->env_bindings) == (env->env_exports))
      return fd_reterr(fd_ReadOnlyEnv,"fd_assign_value",NULL,symbol);
    else return add_to_value(symbol,value,env);}
  return 0;
}

FD_EXPORT int fd_assign_value(lispval symbol,lispval value,fd_lexenv env)
{
  if (env->env_copy) env = env->env_copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env = env->env_parent;
      if ((env) && (env->env_copy)) env = env->env_copy;}
    else if ((env->env_bindings) == (env->env_exports))
      /* This is the kind of environment produced by using a module,
         so it's read only. */
      return fd_reterr(fd_ReadOnlyEnv,"fd_assign_value",NULL,symbol);
    else {
      fd_store(env->env_bindings,symbol,value);
      if (HASHTABLEP(env->env_exports))
        fd_hashtable_op((fd_hashtable)(env->env_exports),
                        fd_table_replace,symbol,value);
      return 1;}}
  return 0;
}

/* Unpacking expressions, non-inline versions */

FD_EXPORT lispval _fd_get_arg(lispval expr,int i)
{
  return fd_get_arg(expr,i);
}
FD_EXPORT lispval _fd_get_body(lispval expr,int i)
{
  return fd_get_body(expr,i);
}

static lispval getopt_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval opts = fd_stack_eval(fd_get_arg(expr,1),env,_stack,0);
  if (FD_ABORTED(opts))
    return opts;
  else {
    lispval keys = fd_eval(fd_get_arg(expr,2),env);
    if (FD_ABORTED(keys)) {
      fd_decref(opts); return keys;}
    else {
      lispval results = EMPTY;
      DO_CHOICES(opt,opts) {
        DO_CHOICES(key,keys) {
          lispval v = fd_getopt(opt,key,VOID);
          if (FD_ABORTED(v)) {
            fd_decref(results); results = v;
            FD_STOP_DO_CHOICES;}
          else if (!(VOIDP(v))) {CHOICE_ADD(results,v);}}
        if (FD_ABORTED(results)) {FD_STOP_DO_CHOICES;}}
      fd_decref(keys);
      fd_decref(opts);
      if (FD_ABORTED(results)) {
        return results;}
      else if (EMPTYP(results)) {
        lispval dflt_expr = fd_get_arg(expr,3);
        if (VOIDP(dflt_expr)) return FD_FALSE;
        else return fd_stack_eval(dflt_expr,env,_stack,0);}
      else return simplify_value(results);}}
}
static lispval tryopt_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval opts = fd_stack_eval(fd_get_arg(expr,1),env,_stack,0);
  lispval default_expr = fd_get_arg(expr,3);
  if ( (FD_ABORTED(opts)) || (!(FD_TABLEP(opts))) ) {
    if (FD_ABORTED(opts)) fd_clear_errors(0);
    if (FD_VOIDP(default_expr)) {
      return FD_FALSE;}
    else if (!(FD_EVALP(default_expr)))
      return fd_incref(default_expr);
    else return fd_stack_eval(default_expr,env,_stack,0);}
  else {
    lispval keys = fd_eval(fd_get_arg(expr,2),env);
    if (FD_ABORTED(keys)) {
      fd_decref(opts);
      return keys;}
    else {
      lispval results = EMPTY;
      DO_CHOICES(opt,opts) {
        DO_CHOICES(key,keys) {
          lispval v = fd_getopt(opt,key,VOID);
          if (FD_ABORTED(v)) {
            fd_clear_errors(0);
            fd_decref(results);
            results = FD_EMPTY_CHOICE;
            FD_STOP_DO_CHOICES;
            break;}
          else if (!(VOIDP(v))) {CHOICE_ADD(results,v);}}
        if (FD_ABORTED(results)) {FD_STOP_DO_CHOICES;}}
      fd_decref(keys); fd_decref(opts);
      if (FD_ABORTED(results)) { /* Not sure this ever happens */
        fd_clear_errors(0);
        results=FD_EMPTY_CHOICE;}
      if (EMPTYP(results)) {
        lispval dflt_expr = fd_get_arg(expr,3);
        if (VOIDP(dflt_expr))
          return FD_FALSE;
        else if (!(FD_EVALP(dflt_expr)))
          return fd_incref(dflt_expr);
        else return fd_stack_eval(dflt_expr,env,_stack,0);}
      else return simplify_value(results);}}
}
static lispval getopt_prim(lispval opts,lispval keys,lispval dflt)
{
  lispval results = EMPTY;
  DO_CHOICES(opt,opts) {
    DO_CHOICES(key,keys) {
      lispval v = fd_getopt(opt,key,VOID);
      if (!(VOIDP(v))) {CHOICE_ADD(results,v);}}}
  if (EMPTYP(results)) {
    fd_incref(dflt); return dflt;}
  else return simplify_value(results);
}
static lispval testopt_prim(lispval opts,lispval key,lispval val)
{
  if (fd_testopt(opts,key,val))
    return FD_TRUE;
  else return FD_FALSE;
}

static int optionsp(lispval arg)
{
  if ( (FD_FALSEP(arg)) || (FD_NILP(arg)) || (FD_EMPTYP(arg)) )
    return 1;
  else if (FD_AMBIGP(arg)) {
    FD_DO_CHOICES(elt,arg) {
      if (! (optionsp(elt)) ) {
        FD_STOP_DO_CHOICES;
        return 0;}}
    return 1;}
  else if (FD_PAIRP(arg)) {
    if (optionsp(FD_CAR(arg)))
      return optionsp(FD_CDR(arg));
    else return 0;}
  else if ( (FD_TABLEP(arg)) &&
            (!(FD_POOLP(arg))) &&
            (!(FD_INDEXP(arg))) )
    return 1;
  else return 0;
}
static lispval optionsp_prim(lispval opts)
{
  if (optionsp(opts))
    return FD_TRUE;
  else return FD_FALSE;
}
#define nulloptsp(v) ( (v == FD_FALSE) || (v == FD_DEFAULT) )
static lispval opts_plus_prim(int n,lispval *args)
{
  int i = 0;
  lispval back = FD_FALSE, front = FD_VOID;
  if (n == 0)
    return fd_make_slotmap(3,0,NULL);
  while (i < n) {
    lispval arg = args[i++];
    if (FD_TABLEP(arg)) {
      fd_incref(arg);
      if (FD_FALSEP(back))
        back = arg;
      else {
        if (!(FD_VOIDP(front))) {
          back = fd_init_pair(NULL,front,back);
          front=FD_VOID;}
        back = fd_init_pair(NULL,arg,back);}}
    else if ( (nulloptsp(arg)) || (FD_EMPTYP(arg)) ) {}
    else {
      if (FD_VOIDP(front)) front = fd_make_slotmap(n,0,NULL);
      if (i < n) {
        lispval optval = args[i++];
        if (FD_QCHOICEP(optval)) {
          struct FD_QCHOICE *qc = (fd_qchoice) optval;
          fd_add(front,arg,qc->qchoiceval);}
        else fd_add(front,arg,optval);}
      else fd_store(front,arg,FD_TRUE);}}
  if (FD_VOIDP(front))
    return back;
  else if (FD_FALSEP(back))
    return front;
  else return fd_init_pair(NULL,front,back);
}

/* Quote */

static lispval quote_evalfn(lispval obj,fd_lexenv env,fd_stack stake)
{
  if ((PAIRP(obj)) && (PAIRP(FD_CDR(obj))) &&
      ((FD_CDR(FD_CDR(obj))) == NIL))
    return fd_incref(FD_CAR(FD_CDR(obj)));
  else return fd_err(fd_SyntaxError,"QUOTE",NULL,obj);
}

/* Profiling functions */

static lispval profile_symbol;

static lispval profiled_eval_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval toeval = fd_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = fd_eval(toeval,env);
  if (FD_ABORTED(value)) return value;
  double finish = u8_elapsed_time();
  lispval tag = fd_get_arg(expr,2);
  lispval profile_info = fd_symeval(profile_symbol,env), profile_data;
  if (VOIDP(profile_info)) return value;
  profile_data = fd_get(profile_info,tag,VOID);
  if (FD_ABORTED(profile_data)) {
    fd_decref(value); fd_decref(profile_info);
    return profile_data;}
  else if (VOIDP(profile_data)) {
    lispval time = fd_init_double(NULL,(finish-start));
    profile_data = fd_conspair(FD_INT(1),time);
    fd_store(profile_info,tag,profile_data);}
  else {
    struct FD_PAIR *p = fd_consptr(fd_pair,profile_data,fd_pair_type);
    struct FD_FLONUM *d = fd_consptr(fd_flonum,(p->cdr),fd_flonum_type);
    p->car = FD_INT(fd_getint(p->car)+1);
    d->floval = d->floval+(finish-start);}
  fd_decref(profile_data); fd_decref(profile_info);
  return value;
}

#if __clang__
#define DONT_OPTIMIZE  __attribute__((optnone))
#else
#define DONT_OPTIMIZE __attribute__((optimize("O0")))
#endif

/* These are for wrapping around Scheme code to see in C profilers */
static DONT_OPTIMIZE lispval eval1(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval2(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval3(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval4(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval5(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval6(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}
static DONT_OPTIMIZE lispval eval7(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval result = fd_stack_eval(fd_get_arg(expr,1),env,s,0);
  return result;
}

/* Google profiler usage */

#if USING_GOOGLE_PROFILER
static lispval gprofile_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  int start = 1; char *filename = NULL;
  if ((PAIRP(FD_CDR(expr)))&&(STRINGP(FD_CAR(FD_CDR(expr))))) {
    filename = u8_strdup(CSTRING(FD_CAR(FD_CDR(expr))));
    start = 2;}
  else if ((PAIRP(FD_CDR(expr)))&&(SYMBOLP(FD_CAR(FD_CDR(expr))))) {
    lispval val = fd_symeval(FD_CADR(expr),env);
    if (FD_ABORTED(val)) return val;
    else if (STRINGP(val)) {
      filename = u8_strdup(CSTRING(val));
      fd_decref(val);
      start = 2;}
    else return fd_type_error("filename","GOOGLE/PROFILE",val);}
  else if (cpu_profilename)
    filename = u8_strdup(cpu_profilename);
  else if (getenv("CPUPROFILE"))
    filename = u8_strdup(getenv("CPUPROFILE"));
  else filename = u8_mkstring("/tmp/gprof%ld.pid",(long)getpid());
  ProfilerStart(filename); {
    lispval value = VOID;
    lispval body = fd_get_body(expr,start);
    FD_DOLIST(ex,body) {
      fd_decref(value); value = fd_eval(ex,env);
      if (FD_ABORTED(value)) {
        ProfilerStop();
        return value;}
      else {}}
    ProfilerStop();
    u8_free(filename);
    return value;}
}

static lispval gprofile_stop()
{
  ProfilerStop();
  return VOID;
}
#endif


/* Trace functions */

static lispval timed_eval_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval toeval = fd_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = fd_eval(toeval,env);
  double finish = u8_elapsed_time();
  u8_message(";; Executed %q in %f seconds, yielding\n;;\t%q",
             toeval,(finish-start),value);
  return value;
}

static lispval timed_evalx_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval toeval = fd_get_arg(expr,1);
  double start = u8_elapsed_time();
  lispval value = fd_eval(toeval,env);
  double finish = u8_elapsed_time();
  if (FD_ABORTED(value)) {
    u8_exception ex = u8_erreify();
    lispval exception = fd_wrap_exception(ex);
    return fd_make_nvector(2,exception,fd_init_double(NULL,finish-start));}
  else return fd_make_nvector(2,value,fd_init_double(NULL,finish-start));
}

/* Inserts a \n\t line break if the current output line is longer
    than max_len.  *off*, if > 0, is the last offset on the current line,
    which is where the line break goes if inserted;  */
static int check_line_length(u8_output out,int off,int max_len)
{
  u8_byte *start = out->u8_outbuf, *end = out->u8_write, *scanner = end;
  int scan_off, line_len, len = end-start;
  while ((scanner>start)&&((*scanner)!='\n')) scanner--;
  line_len = end-scanner; scan_off = scanner-start;
  /* If the current line is less than max_len, return the current offset */
  if (line_len<max_len) return len;
  /* If the offset is non-positive, the last item was the first
     item on a line and gets the whole line to itself, so we still
     return the current offset.  We don't insert a \n\t now because
     it might be the last item output. */
  else if (off<=0)
    return len;
  else {
    /* TODO: abstract out u8_need_len, abstract out whole "if longer
       than" logic */
    /* The line is too long, insert a \n\t at off */
    if ((end+5)>(out->u8_outlim)) {
      /* Grow the stream if needed */
      if (u8_grow_stream((u8_stream)out,U8_BUF_MIN_GROW)<=0)
        return -1;
      start = out->u8_outbuf; end = out->u8_write;
      scanner = start+scan_off;}
    /* Use memmove because it's overlapping */
    memmove(start+off+2,start+off,len-off);
    start[off]='\n'; start[off+1]='\t';
    out->u8_write = out->u8_write+2;
    start[len+2]='\0';
    return -1;}
}

static lispval watchcall(lispval expr,fd_lexenv env,int with_proc)
{
  struct U8_OUTPUT out;
  u8_string dflt_label="%CALL", label = dflt_label, arglabel="%ARG";
  lispval watch, head = fd_get_arg(expr,1), *rail, result = EMPTY;
  int i = 0, n_args, expr_len = fd_seq_length(expr);
  if (VOIDP(head))
    return fd_err(fd_SyntaxError,"%WATCHCALL",NULL,expr);
  else if (STRINGP(head)) {
    if (expr_len==2)
      return fd_err(fd_SyntaxError,"%WATCHCALL",NULL,expr);
    else {
      label = u8_strdup(CSTRING(head));
      arglabel = u8_mkstring("%s/ARG",CSTRING(head));
      if ((expr_len>3)||(FD_APPLICABLEP(fd_get_arg(expr,2)))) {
        watch = fd_get_body(expr,2);}
      else watch = fd_get_arg(expr,2);}}
  else if ((expr_len==2)&&(PAIRP(head)))
    watch = head;
  else watch = fd_get_body(expr,1);
  n_args = fd_seq_length(watch);
  rail = u8_alloc_n(n_args,lispval);
  U8_INIT_OUTPUT(&out,1024);
  u8_printf(&out,"Watched call %q",watch);
  u8_logger(U8_LOG_MSG,label,out.u8_outbuf);
  out.u8_write = out.u8_outbuf;
  while (i<n_args) {
    lispval arg = fd_get_arg(watch,i);
    lispval val = fd_eval(arg,env);
    val = simplify_value(val);
    if (FD_ABORTED(val)) {
      u8_string errstring = fd_errstring(NULL);
      i--; while (i>=0) {fd_decref(rail[i]); i--;}
      u8_free(rail);
      u8_printf(&out,"\t%q !!!> %s",arg,errstring);
      u8_logger(U8_LOG_MSG,arglabel,out.u8_outbuf);
      if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
      u8_free(out.u8_outbuf); u8_free(errstring);
      return val;}
    if ((i==0)&&(with_proc==0)&&(SYMBOLP(arg))) {}
    else if (FD_EVALP(arg)) {
      u8_printf(&out,"%q ==> %q",arg,val);
      u8_logger(U8_LOG_MSG,arglabel,out.u8_outbuf);
      out.u8_write = out.u8_outbuf;}
    else {
      u8_printf(&out,"%q",arg);
      u8_logger(U8_LOG_MSG,arglabel,out.u8_outbuf);
      out.u8_write = out.u8_outbuf;}
    rail[i++]=val;}
  if (CHOICEP(rail[0])) {
    DO_CHOICES(fn,rail[0]) {
      lispval r = fd_apply(fn,n_args-1,rail+1);
      if (FD_ABORTED(r)) {
        u8_string errstring = fd_errstring(NULL);
        i--; while (i>=0) {fd_decref(rail[i]); i--;}
        u8_free(rail);
        fd_decref(result);
        if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
        u8_free(out.u8_outbuf); u8_free(errstring);
        return r;}
      else {CHOICE_ADD(result,r);}}}
  else result = fd_apply(rail[0],n_args-1,rail+1);
  if (FD_ABORTED(result)) {
    u8_string errstring = fd_errstring(NULL);
    u8_printf(&out,"%q !!!> %s",watch,errstring);
    u8_free(errstring);}
  else u8_printf(&out,"%q ===> %q",watch,result);
  u8_logger(U8_LOG_MSG,label,out.u8_outbuf);
  i--; while (i>=0) {fd_decref(rail[i]); i--;}
  u8_free(rail);
  if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
  u8_free(out.u8_outbuf);
  return simplify_value(result);
}

static lispval watchcall_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return watchcall(expr,env,0);
}
static lispval watchcall_plus_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return watchcall(expr,env,1);
}

static u8_string get_label(lispval arg,u8_byte *buf,size_t buflen)
{
  if (SYMBOLP(arg))
    return SYM_NAME(arg);
  else if (STRINGP(arg))
    return CSTRING(arg);
  else if (FIXNUMP(arg))
    return u8_write_long_long((FIX2INT(arg)),buf,buflen);
  else if ((FD_BIGINTP(arg))&&(fd_modest_bigintp((fd_bigint)arg)))
    return u8_write_long_long
      (fd_bigint2int64((fd_bigint)arg),buf,buflen);
  else return NULL;
}

static void log_ptr(lispval val,lispval label_arg,lispval expr)
{
  u8_byte buf[64];
  u8_string label = get_label(label_arg,buf,64);
  u8_string src_string = (FD_VOIDP(expr)) ? (NULL) :
    (fd_lisp2string(expr));
  if (FD_IMMEDIATEP(val)) {
    unsigned long long itype = FD_IMMEDIATE_TYPE(val);
    unsigned long long data = FD_IMMEDIATE_DATA(val);
    u8_string type_name = fd_type2name(itype);;
    u8_log(U8_LOG_MSG,"Pointer/Immediate",
           "%s%s%s0x%llx [ T0x%llx(%s) data=%llu ] == %q%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           itype,type_name,data,val,
           U8OPTSTR(" <== ",src_string,""));}
  else if (FIXNUMP(val))
    u8_log(U8_LOG_MSG,"Pointer/Fixnum",
           "%s%s%sFixnum","0x%llx == %d%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),FIX2INT(val),
           U8OPTSTR(" <== ",src_string,""));
  else if (OIDP(val)) {
    FD_OID addr = FD_OID_ADDR(val);
    u8_log(U8_LOG_MSG,"Pointer/OID",
           "%s%s%s0x%llx [ base=%llx off=%llx ] == %llx/%llx%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           (FD_OID_BASE_ID(val)),(FD_OID_BASE_OFFSET(val)),
           FD_OID_HI(addr),FD_OID_LO(addr),
           U8OPTSTR(" <== ",src_string,""));}
  else if (FD_STATICP(val)) {
    fd_ptr_type ptype = FD_CONS_TYPE((fd_cons)val);
    u8_string type_name = fd_ptr_typename(ptype);
    u8_log(U8_LOG_MSG,"Pointer/Static",
           "%s%s%s0x%llx [ T0x%llx(%s) ] == %q%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           ptype,type_name,val,
           U8OPTSTR(" <== ",src_string,""));}
  else if (CONSP(val)) {
    fd_cons c = (fd_cons) val;
    fd_ptr_type ptype = FD_CONS_TYPE(c);
    u8_string type_name = fd_ptr_typename(ptype);
    unsigned int refcount = FD_CONS_REFCOUNT(c);
    u8_log(U8_LOG_MSG,"Pointer/Consed",
           "%s%s%s0x%llx [ T0x%llx(%s) refs=%d ] == %q%s%s%s",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           ptype,type_name,refcount,val,
           U8OPTSTR(" <== ",src_string,""));}
  else {}
  if (src_string) u8_free(src_string);
}

static lispval watchptr_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval val_expr = fd_get_arg(expr,1);
  lispval value = fd_stack_eval(val_expr,env,_stack,0);
  log_ptr(value,FD_VOID,val_expr);
  return value;
}

static lispval watchptr_prim(lispval val,lispval label_arg)
{
  log_ptr(val,label_arg,FD_VOID);
  return fd_incref(val);
}

static lispval watched_eval_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval toeval = fd_get_arg(expr,1);
  double start; int oneout = 0;
  lispval scan = FD_CDR(expr);
  u8_string label="%WATCH";
  if ((PAIRP(toeval))) {
    /* EXPR "label" . watchexprs */
    scan = FD_CDR(scan);
    if ((PAIRP(scan)) && (STRINGP(FD_CAR(scan)))) {
      label = CSTRING(FD_CAR(scan)); scan = FD_CDR(scan);}}
  else if (STRINGP(toeval)) {
    /* "label" . watchexprs, no expr, call should be side-effect */
    label = CSTRING(toeval); scan = FD_CDR(scan);}
  else if (SYMBOLP(toeval)) {
    /* If the first argument is a symbol, we change the label and
       treat all the arguments as context variables and output
       them.  */
    label="%WATCHED";}
  else scan = FD_CDR(scan);
  if (PAIRP(scan)) {
    int off = 0; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    if (PAIRP(toeval)) {
      u8_printf(&out,"Context for %q: ",toeval);
      off = check_line_length(&out,off,50);}
    /* A watched expr can be just a symbol or pair, which is output as:
         <expr>=<value>
       or a series of "<label>" <expr>, which is output as:
         label=<value> */
    while (PAIRP(scan)) {
      /* A watched expr can be just a symbol or pair, which is output as:
           <expr>=<value>
         or a series of "<label>" <expr>, which is output as:
           label=<value> */
      lispval towatch = FD_CAR(scan), wval = VOID;
      if ((STRINGP(towatch)) && (PAIRP(FD_CDR(scan)))) {
        lispval label = towatch; u8_string lbl = CSTRING(label);
        towatch = FD_CAR(FD_CDR(scan)); scan = FD_CDR(FD_CDR(scan));
        wval = ((SYMBOLP(towatch))?(fd_symeval(towatch,env)):
              (fd_eval(towatch,env)));
        if (FD_ABORTED(wval)) {
          u8_exception ex = u8_erreify();
          wval = fd_wrap_exception(ex);}
        if (lbl[0]=='\n') {
          u8_printf(&out,"\n  ");
          oneout = 1;
          fd_list_object(&out,wval,lbl+1,NULL,"  ",NULL,100,2);
          if (PAIRP(scan)) {
            u8_puts(&out,"\n"); off = 0;}}
        else {
          if (oneout) u8_puts(&out," // "); else oneout = 1;
          u8_printf(&out,"%s=%q",CSTRING(label),wval);
          off = check_line_length(&out,off,100);}
        fd_decref(wval); wval = VOID;}
      else {
        wval = ((SYMBOLP(towatch))?(fd_symeval(towatch,env)):
              (fd_eval(towatch,env)));
        if (FD_ABORTED(wval)) {
          u8_exception ex = u8_erreify();
          wval = fd_wrap_exception(ex);}
        scan = FD_CDR(scan);
        if (oneout) u8_printf(&out," // %q=%q",towatch,wval);
        else {
          u8_printf(&out,"%q=%q",towatch,wval);
          oneout = 1;}
        off = check_line_length(&out,off,50);
        fd_decref(wval); wval = VOID;}}
    u8_logger(U8_LOG_MSG,label,out.u8_outbuf);
    u8_free(out.u8_outbuf);}
  start = u8_elapsed_time();
  if (SYMBOLP(toeval))
    return fd_eval(toeval,env);
  else if (STRINGP(toeval)) {
    /* This is probably the 'side effect' form of %WATCH, so
       we don't bother 'timing' it. */
    return fd_incref(toeval);}
  else {
    lispval value = fd_eval(toeval,env);
    double howlong = u8_elapsed_time()-start;
    if (FD_ABORTED(value)) {
      u8_exception ex = u8_erreify();
      value = fd_wrap_exception(ex);}
    if (howlong>1.0)
      u8_log(U8_LOG_MSG,label,"<%.3fs> %q => %q",howlong*1000,toeval,value);
    else if (howlong>0.001)
      u8_log(U8_LOG_MSG,label,"<%.3fms> %q => %q",howlong*1000,toeval,value);
    else if (remainder(howlong,0.0000001)>=0.001)
      u8_log(U8_LOG_MSG,label,"<%.3fus> %q => %q",howlong*1000000,toeval,value);
    else u8_log(U8_LOG_MSG,label,"<%.1fus> %q => %q",howlong*1000000,
                toeval,value);
    return value;}
}

/* The evaluator itself */

static lispval call_function(u8_string fname,lispval f,
                             lispval expr,fd_lexenv env,
                             fd_stack s,
                             int tail);
static int applicable_choicep(lispval choice);

FD_EXPORT
lispval fd_stack_eval(lispval expr,fd_lexenv env,
                     struct FD_STACK *_stack,
                     int tail)
{
  if (_stack==NULL) _stack=fd_stackptr;
  switch (FD_PTR_TYPE(expr)) {
  case fd_lexref_type:
    return fd_lexref(expr,env);
  case fd_code_type:
    return fd_incref(expr);
  case fd_symbol_type: {
    lispval val = fd_symeval(expr,env);
    if (PRED_FALSE(VOIDP(val)))
      return fd_err(fd_UnboundIdentifier,"fd_eval",
                    SYM_NAME(expr),expr);
    else return simplify_value(val);}
  case fd_pair_type: {
    lispval head = (FD_CAR(expr));
    if (FD_OPCODEP(head))
      return fd_opcode_dispatch(head,expr,env,_stack,tail);
    else if (head == quote_symbol)
      return fd_refcar(FD_CDR(expr));
    else if (head == comment_symbol)
      return VOID;
    else if (!(fd_stackcheck())) {
      u8_byte buf[128]=""; struct U8_OUTPUT out;
      U8_INIT_FIXED_OUTPUT(&out,128,buf);
      u8_printf(&out,"%lld > %lld",u8_stack_depth(),fd_stack_limit);
      return fd_err(fd_StackOverflow,"fd_tail_eval",buf,expr);}
    else return fd_pair_eval(head,expr,env,_stack,tail);}
  case fd_slotmap_type:
    return fd_deep_copy(expr);
  case fd_choice_type: {
    lispval result = EMPTY;
    FD_PUSH_STACK(nd_eval_stack,fd_ndevalstack_type,NULL,expr);
    DO_CHOICES(each_expr,expr) {
      lispval r = stack_eval(each_expr,env,nd_eval_stack);
      if (FD_ABORTED(r)) {
        FD_STOP_DO_CHOICES;
        fd_decref(result);
        fd_pop_stack(nd_eval_stack);
        return r;}
      else {CHOICE_ADD(result,r);}}
    fd_pop_stack(nd_eval_stack);
    return simplify_value(result);}
  case fd_prechoice_type: {
    lispval exprs = fd_make_simple_choice(expr);
    FD_PUSH_STACK(prechoice_eval_stack,fd_evalstack_type,NULL,expr);
    if (CHOICEP(exprs)) {
      lispval results = EMPTY;
      DO_CHOICES(expr,exprs) {
        lispval result = stack_eval(expr,env,prechoice_eval_stack);
        if (FD_ABORTED(result)) {
          FD_STOP_DO_CHOICES;
          fd_decref(results);
          fd_pop_stack(prechoice_eval_stack);
          return result;}
        else {CHOICE_ADD(results,result);}}
      fd_decref(exprs);
      fd_pop_stack(prechoice_eval_stack);
      return simplify_value(results);}
    else {
      lispval result = fd_stack_eval(exprs,env,prechoice_eval_stack,tail);
      fd_decref(exprs);
      fd_pop_stack(prechoice_eval_stack);
      return result;}}
  default:
    return fd_incref(expr);}
}

FD_EXPORT
lispval fd_pair_eval(lispval head,lispval expr,fd_lexenv env,
                     struct FD_STACK *_stack,
                     int tail)
{
  u8_string label=(SYMBOLP(head)) ? (SYM_NAME(head)) : (NULL);
  FD_PUSH_STACK(eval_stack,fd_evalstack_type,label,expr);
  lispval result = VOID, headval = VOID;
  int gc_head=0;
  if (FD_LEXREFP(head)) {
    headval=fd_lexref(head,env);
    if (FD_CONSP(headval)) gc_head=1;}
  else if (FD_FCNIDP(head)) {
    headval=fd_fcnid_ref(head);
    if (PRECHOICEP(headval)) {
      headval=fd_make_simple_choice(headval);
      gc_head=1;}}
  else if ( (SYMBOLP(head)) || (PAIRP(head)) ||
            (FD_CODEP(head)) || (CHOICEP(head)) ) {
    headval=stack_eval(head,env,eval_stack);
    headval=simplify_value(headval);
    gc_head=1;}
  else headval=head;
  if (FD_ABORTED(headval)) {
    fd_pop_stack(eval_stack);
    return headval;}
  else if (FD_FCNIDP(headval)) {
    headval=fd_fcnid_ref(headval);
    if (PRECHOICEP(headval)) {
      headval=fd_make_simple_choice(headval);
      gc_head=1;}
    else gc_head=0;}
  int headtype = FD_PTR_TYPE(headval);
  if (gc_head) fd_push_cleanup(eval_stack,FD_DECREF,headval,NULL);
  switch (headtype) {
  case fd_cprim_type: case fd_lambda_type: {
    struct FD_FUNCTION *f = (struct FD_FUNCTION *) headval;
    if (f->fcn_name) eval_stack->stack_label=f->fcn_name;
    result=call_function(f->fcn_name,headval,expr,env,
                         eval_stack,tail);
    break;}
  case fd_evalfn_type: {
    /* These are evalfns which do all the evaluating themselves */
    struct FD_EVALFN *handler = (fd_evalfn)headval;
    if (handler->evalfn_name)
      eval_stack->stack_label=handler->evalfn_name;
    result=handler->evalfn_handler(expr,env,eval_stack);
    break;}
  case fd_macro_type: {
    /* These expand into expressions which are
       then evaluated. */
    struct FD_MACRO *macrofn=
      fd_consptr(struct FD_MACRO *,headval,fd_macro_type);
    eval_stack->stack_type="macro";
    lispval xformer = macrofn->macro_transformer;
    lispval new_expr = fd_call(eval_stack,xformer,1,&expr);
    if (FD_ABORTED(new_expr))
      result = fd_err(fd_SyntaxError,
                      _("macro expansion"),NULL,new_expr);
    else result = fd_stack_eval(new_expr,env,eval_stack,tail);
    fd_decref(new_expr);
    break;}
  case fd_choice_type: {
    int applicable = applicable_choicep(headval);
    if (applicable)
      result = call_function("fnchoice",headval,expr,env,eval_stack,0);
    else result=fd_err(fd_SyntaxError,"fd_stack_eval",
                       "not applicable or evalfn",
                       headval);
    break;}
  default:
    if ( (fd_functionp[headtype]) || (fd_applyfns[headtype]) ) {
      struct FD_FUNCTION *f = (struct FD_FUNCTION *) headval;
      result=call_function(f->fcn_name,headval,expr,env,
                           eval_stack,tail);}
    else if (FD_ABORTED(headval)) {
      result=headval;}
    else if (VOIDP(headval)) {
      result=fd_err(fd_UnboundIdentifier,"for function",
                    ((SYMBOLP(head))?(SYM_NAME(head)):
                     (NULL)),
                    head);}
    else if (EMPTYP(headval) )
      result=EMPTY;
    else result=fd_err(fd_NotAFunction,NULL,NULL,headval);}
  if (!tail) {
    if (FD_TAILCALLP(result))
      result=fd_finish_call(result);
    else {}}
  fd_pop_stack(eval_stack);
  return simplify_value(result);
}

static int applicable_choicep(lispval headvals)
{
  DO_CHOICES(fcn,headvals) {
    lispval hv = (FD_FCNIDP(fcn)) ? (fd_fcnid_ref(fcn)) : (fcn);
    int hvtype = FD_PRIM_TYPE(hv);
    /* Check that all the elements are either applicable or special
       forms and not mixed */
    if ( (hvtype == fd_cprim_type) ||
         (hvtype == fd_lambda_type) ||
         (fd_applyfns[hvtype]) ) {}
    else if ((hvtype == fd_evalfn_type) ||
             (hvtype == fd_macro_type))
      return 0;
    /* In this case, all the headvals so far are evalfns */
    else return 0;}
  return 1;
}

FD_EXPORT lispval _fd_eval(lispval expr,fd_lexenv env)
{
  lispval result = fd_tail_eval(expr,env);
  if (FD_ABORTED(result)) return result;
  else return fd_finish_call(result);
}

static int count_args(lispval args)
{
  int n_args = 0; FD_DOLIST(arg,args) {
    if (!((PAIRP(arg)) &&
          (FD_EQ(FD_CAR(arg),comment_symbol))))
      n_args++;}
  return n_args;
}

static lispval process_arg(lispval arg,fd_lexenv env,
                          struct FD_STACK *_stack)
{
  lispval argval = fast_eval(arg,env);
  if (PRED_FALSE(VOIDP(argval)))
    return fd_err(fd_VoidArgument,"call_function/process_arg",NULL,arg);
  else if (FD_ABORTED(argval))
    return argval;
  else return simplify_value(argval);
}

#define ND_ARGP(v) ((CHOICEP(v))||(QCHOICEP(v)))

FD_FASTOP int commentp(lispval arg)
{
  return
    (PRED_FALSE
     ((PAIRP(arg)) &&
      (FD_EQ(FD_CAR(arg),comment_symbol))));
}

 static lispval call_function(u8_string fname,lispval headval,
                              lispval expr,fd_lexenv env,
                              struct FD_STACK *stack,
                              int tail)
{
  lispval arg_exprs = fd_get_body(expr,1), result=VOID;
  int n_args = count_args(arg_exprs), arg_count = 0, lambda = 0;
  int gc_args = 0, nd_args = 0, d_prim = 0;
  lispval argbuf[n_args]; /* *argv=fd_alloca(n_args); */
  int i=0; while (i<n_args) argbuf[i++]=VOID;
  lispval fn = (FD_FCNIDP(headval)) ? (fd_fcnid_ref(headval)) : (headval);
  if (FD_AMBIGP(fn)) {
    FD_DO_CHOICES(f,fn) {
      if (!(FD_APPLICABLEP(f))) {
        FD_STOP_DO_CHOICES;
        return fd_err("NotApplicable","call_function/eval",NULL,f);}}
    tail=0;}
  else if (!(FD_APPLICABLEP(fn)))
    return fd_err("NotApplicable","call_function/eval",NULL,fn);
  else if (FD_FUNCTIONP(fn)) {
    struct FD_FUNCTION *fcn=FD_XFUNCTION(fn);
    int max_arity = fcn->fcn_arity, min_arity = fcn->fcn_min_arity;
    if (max_arity<0) {}
    else if (PRED_FALSE(n_args>max_arity))
      return fd_err(fd_TooManyArgs,"call_function",fcn->fcn_name,expr);
    else if (PRED_FALSE((min_arity>=0) && (n_args<min_arity)))
      return fd_err(fd_TooFewArgs,"call_function",fcn->fcn_name,expr);
    else {}
    d_prim=(fcn->fcn_ndcall==0);
    if (fcn->fcn_notail) tail = 0;
    lambda = FD_LAMBDAP(fn);}
  else NO_ELSE;
  /* Now we evaluate each of the subexpressions to fill the arg
     vector */
  {FD_DOLIST(elt,arg_exprs) {
      if (commentp(elt)) continue;
      lispval argval = process_arg(elt,env,stack);
      if ( (FD_ABORTED(argval)) ||
           ( (d_prim) && (EMPTYP(argval)) ) ) {
        /* Clean up the arguments we've already evaluated */
        if (gc_args) fd_decref_vec(argbuf,arg_count);
        return argval;}
      else if (CONSP(argval)) {
        if ( (nd_args == 0) && (ND_ARGP(argval)) ) nd_args = 1;
        gc_args = 1;}
      else {}
      argbuf[arg_count++]=argval;}}
  if ((tail) && (lambda) && (fd_optimize_tail_calls)  )
    result=fd_tail_call(fn,arg_count,argbuf);
  else if ((CHOICEP(fn)) ||
           (PRECHOICEP(fn)) ||
           ((d_prim) && (nd_args)))
    result=fd_ndcall(stack,fn,arg_count,argbuf);
  else result=fd_dcall(stack,fn,arg_count,argbuf);
  if (gc_args) fd_decref_vec(argbuf,arg_count);
  return result;
}

FD_EXPORT lispval fd_eval_exprs(lispval exprs,fd_lexenv env)
{
  if (PAIRP(exprs)) {
    lispval next = FD_CDR(exprs), val = VOID;
    while (PAIRP(exprs)) {
      fd_decref(val); val = VOID;
      if (NILP(next))
        return fd_tail_eval(FD_CAR(exprs),env);
      else {
        val = fd_eval(FD_CAR(exprs),env);
        if (FD_ABORTED(val)) return val;
        else exprs = next;
        if (PAIRP(exprs)) next = FD_CDR(exprs);}}
    return val;}
  else if (FD_CODEP(exprs)) {
    struct FD_VECTOR *v = fd_consptr(fd_vector,exprs,fd_code_type);
    int len = v->vec_length; lispval *elts = v->vec_elts, val = VOID;
    int i = 0; while (i<len) {
      lispval expr = elts[i++];
      fd_decref(val); val = VOID;
      if (i == len)
        return fd_eval(expr,env);
      else val = fd_eval(expr,env);
      if (FD_ABORTED(val)) return val;}
    return val;}
  else return VOID;
}

/* Module system */

static struct FD_HASHTABLE module_map, safe_module_map;
static fd_lexenv default_env = NULL, safe_default_env = NULL;

FD_EXPORT fd_lexenv fd_make_env(lispval bindings,fd_lexenv parent)
{
  if (PRED_FALSE(!(TABLEP(bindings)) )) {
    u8_byte buf[100];
    fd_seterr(fd_TypeError,"fd_make_env",
              u8_sprintf(buf,100,_("object is not a %m"),"table"),
              bindings);
    return NULL;}
  else {
    struct FD_LEXENV *e = u8_alloc(struct FD_LEXENV);
    FD_INIT_FRESH_CONS(e,fd_lexenv_type);
    e->env_bindings = bindings; e->env_exports = VOID;
    e->env_parent = fd_copy_env(parent);
    e->env_copy = e;
    return e;}
}


FD_EXPORT
/* fd_make_export_env:
    Arguments: a hashtable and an environment
    Returns: a consed environment whose bindings and exports
  are the exports table.  This indicates that the environment
  is "for export only" and cannot be modified. */
fd_lexenv fd_make_export_env(lispval exports,fd_lexenv parent)
{
  if (PRED_FALSE(!(HASHTABLEP(exports)) )) {
    u8_byte buf[100];
    fd_seterr(fd_TypeError,"fd_make_env",
              u8_sprintf(buf,100,_("object is not a %m"),"hashtable"),
              exports);
    return NULL;}
  else {
    struct FD_LEXENV *e = u8_alloc(struct FD_LEXENV);
    FD_INIT_FRESH_CONS(e,fd_lexenv_type);
    e->env_bindings = fd_incref(exports);
    e->env_exports = fd_incref(e->env_bindings);
    e->env_parent = fd_copy_env(parent);
    e->env_copy = e;
    return e;}
}

FD_EXPORT fd_lexenv fd_new_lexenv(lispval bindings,int safe)
{
  if (scheme_initialized==0) fd_init_scheme();
  if (VOIDP(bindings))
    bindings = fd_make_hashtable(NULL,17);
  else fd_incref(bindings);
  return fd_make_env(bindings,((safe)?(safe_default_env):(default_env)));
}
FD_EXPORT fd_lexenv fd_working_lexenv()
{
  if (scheme_initialized==0) fd_init_scheme();
  return fd_make_env(fd_make_hashtable(NULL,17),default_env);
}
FD_EXPORT fd_lexenv fd_safe_working_lexenv()
{
  if (scheme_initialized==0) fd_init_scheme();
  return fd_make_env(fd_make_hashtable(NULL,17),safe_default_env);
}

FD_EXPORT lispval fd_register_module_x(lispval name,lispval module,int flags)
{
  if (flags&FD_MODULE_SAFE)
    fd_hashtable_store(&safe_module_map,name,module);
  else fd_hashtable_store(&module_map,name,module);

  /* Set the module ID*/
  if (FD_LEXENVP(module)) {
    fd_lexenv env = (fd_lexenv)module;
    fd_add(env->env_bindings,moduleid_symbol,name);}
  else if (HASHTABLEP(module))
    fd_add(module,moduleid_symbol,name);
  else {}

  /* Add to the appropriate default environment */
  if (flags&FD_MODULE_DEFAULT) {
    fd_lexenv scan;
    if (flags&FD_MODULE_SAFE) {
      scan = safe_default_env;
      while (scan)
        if (FD_EQ(scan->env_bindings,module))
          /* It's okay to return now, because if it's in the safe module
             defaults it's also in the risky module defaults. */
          return module;
        else scan = scan->env_parent;
      safe_default_env->env_parent=
        fd_make_env(fd_incref(module),safe_default_env->env_parent);}
    scan = default_env;
    while (scan)
      if (FD_EQ(scan->env_bindings,module)) return module;
      else scan = scan->env_parent;
    default_env->env_parent=
      fd_make_env(fd_incref(module),default_env->env_parent);}
  return module;
}

FD_EXPORT lispval fd_register_module(u8_string name,lispval module,int flags)
{
  return fd_register_module_x(fd_intern(name),module,flags);
}

FD_EXPORT lispval fd_new_module(char *name,int flags)
{
  lispval module_name, module, as_stored;
  if (scheme_initialized==0) fd_init_scheme();
  module_name = fd_intern(name);
  module = fd_make_hashtable(NULL,0);
  fd_add(module,moduleid_symbol,module_name);
  if (flags&FD_MODULE_SAFE) {
    fd_hashtable_op
      (&safe_module_map,fd_table_default,module_name,module);
    as_stored = fd_get((lispval)&safe_module_map,module_name,VOID);}
  else {
    fd_hashtable_op
      (&module_map,fd_table_default,module_name,module);
    as_stored = fd_get((lispval)&module_map,module_name,VOID);}
  if (!(FD_EQ(module,as_stored))) {
    fd_decref(module);
    return as_stored;}
  else fd_decref(as_stored);
  if (flags&FD_MODULE_DEFAULT) {
    if (flags&FD_MODULE_SAFE)
      safe_default_env->env_parent = fd_make_env(module,safe_default_env->env_parent);
    default_env->env_parent = fd_make_env(module,default_env->env_parent);}
  return module;
}

FD_EXPORT lispval fd_new_cmodule(char *name,int flags,void *addr)
{
  lispval mod = fd_new_module(name,flags);
#if HAVE_DLADDR
  Dl_info  dlinfo;
  if (dladdr(addr,&dlinfo)) {
    const char *cfilename = dlinfo.dli_fname;
    if (cfilename) {
      u8_string filename = u8_fromlibc((char *)cfilename);
      lispval fname = fd_make_string(NULL,-1,filename);
      u8_free(filename);
      fd_add(mod,source_symbol,fname);
      fd_decref(fname);}}
#endif
  return mod;
}

FD_EXPORT lispval fd_get_module(lispval name,int safe)
{
  if (safe)
    return fd_hashtable_get(&safe_module_map,name,VOID);
  else {
    lispval module = fd_hashtable_get(&module_map,name,VOID);
    if (VOIDP(module))
      return fd_hashtable_get(&safe_module_map,name,VOID);
    else return module;}
}

FD_EXPORT lispval fd_all_modules()
{
  return fd_hashtable_assocs(&module_map);
}

FD_EXPORT lispval fd_safe_modules()
{
  return fd_hashtable_assocs(&safe_module_map);
}

FD_EXPORT int fd_discard_module(lispval name,int safe)
{
  if (safe)
    return fd_hashtable_store(&safe_module_map,name,VOID);
  else {
    lispval module = fd_hashtable_get(&module_map,name,VOID);
    if (VOIDP(module))
      return fd_hashtable_store(&safe_module_map,name,VOID);
    else {
      fd_decref(module);
      return fd_hashtable_store(&module_map,name,VOID);}}
}

/* Making some functions */

FD_EXPORT lispval fd_make_evalfn(u8_string name,fd_eval_handler fn)
{
  struct FD_EVALFN *f = u8_alloc(struct FD_EVALFN);
  FD_INIT_CONS(f,fd_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_filename = NULL;
  f->evalfn_handler = fn;
  return LISP_CONS(f);
}

FD_EXPORT void fd_defspecial(lispval mod,u8_string name,fd_eval_handler fn)
{
  struct FD_EVALFN *f = u8_alloc(struct FD_EVALFN);
  FD_INIT_CONS(f,fd_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_handler = fn;
  f->evalfn_filename = NULL;
  fd_store(mod,fd_intern(name),LISP_CONS(f));
  f->evalfn_moduleid = fd_get(mod,moduleid_symbol,FD_VOID);
  fd_decref(LISP_CONS(f));
}

FD_EXPORT void fd_new_evalfn(lispval mod,u8_string name,
                             u8_string filename,u8_string doc,
                             fd_eval_handler fn)
{
  struct FD_EVALFN *f = u8_alloc(struct FD_EVALFN);
  FD_INIT_CONS(f,fd_evalfn_type);
  f->evalfn_name = u8_strdup(name);
  f->evalfn_handler = fn;
  f->evalfn_filename = filename;
  f->evalfn_documentation = doc;
  fd_store(mod,fd_intern(name),LISP_CONS(f));
  f->evalfn_moduleid = fd_get(mod,moduleid_symbol,FD_VOID);
  fd_decref(LISP_CONS(f));
}

/* The Evaluator */

static lispval eval_evalfn(lispval x,fd_lexenv env,fd_stack stack)
{
  lispval expr_expr = fd_get_arg(x,1);
  lispval expr = fd_stack_eval(expr_expr,env,stack,0);
  if (FD_ABORTED(expr)) return expr;
  lispval result = fd_stack_eval(expr,env,stack,0);
  fd_decref(expr);
  return result;
}

static lispval boundp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    else if (VOIDP(val)) return FD_FALSE;
    else if (val == FD_UNBOUND) return FD_FALSE;
    else {
      fd_decref(val);
      return FD_TRUE;}}
}

static lispval unboundp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    else if (VOIDP(val)) return FD_TRUE;
    else if (val == FD_UNBOUND) return FD_TRUE;
    else {
      fd_decref(val);
      return FD_FALSE;}}
}

static lispval definedp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"definedp_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    if ( (VOIDP(val)) || (val == FD_DEFAULT_VALUE) )
      return FD_FALSE;
    else if (val == FD_UNBOUND)
      return FD_FALSE;
    else {
      fd_decref(val);
      return FD_TRUE;}}
}

static lispval modref_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval module = fd_get_arg(expr,1);
  lispval symbol = fd_get_arg(expr,2);
  if ((VOIDP(module))||(VOIDP(module)))
    return fd_err(fd_SyntaxError,"modref_evalfn",NULL,fd_incref(expr));
  else if (!(HASHTABLEP(module)))
    return fd_type_error("module hashtable","modref_evalfn",module);
  else if (!(SYMBOLP(symbol)))
    return fd_type_error("symbol","modref_evalfn",symbol);
  else return fd_hashtable_get((fd_hashtable)module,symbol,FD_UNBOUND);
}

static lispval constantp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval to_eval = fd_get_arg(expr,1);
  if (FD_SYMBOLP(to_eval)) {
    lispval v = fd_symeval(to_eval,env);
    if  (v == FD_NULL)
      return fd_err(fd_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else if (FD_ABORTED(v))
      return v;
    else if (FD_CONSTANTP(v))
      return FD_TRUE;
    else {
      fd_decref(v);
      return FD_FALSE;}}
  else {
    lispval v = fd_eval(to_eval,env);
    if  (v == FD_NULL)
      return fd_err(fd_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else if (FD_ABORTED(v))
      return v;
    else if (FD_CONSTANTP(v))
      return FD_TRUE;
    else {
      fd_decref(v);
      return FD_FALSE;}}
}

static lispval voidp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval to_eval = fd_get_arg(expr,1);
  if (FD_SYMBOLP(to_eval)) {
    lispval v = fd_symeval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return FD_TRUE;
    else if  (v == FD_NULL) {
      return FD_TRUE;}
    else {
      fd_decref(v);
      return FD_FALSE;}}
  else {
    lispval v = fd_eval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return FD_TRUE;
    else {
      fd_decref(v);
      return FD_FALSE;}}
}

static lispval defaultp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval to_eval = fd_get_arg(expr,1);
  if (FD_SYMBOLP(to_eval)) {
    lispval v = fd_symeval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if (v == FD_DEFAULT)
      return FD_TRUE;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return fd_err(fd_UnboundIdentifier,"defaultp_evalfn",NULL,to_eval);
    else if  (v == FD_NULL)
      return fd_err(fd_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else {
      fd_decref(v);
      return FD_FALSE;}}
  else {
    lispval v = fd_eval(to_eval,env);
    if  (v == FD_NULL)
      return fd_err(fd_BadPtr,"defaultp_evalfn","NULL pointer",to_eval);
    else if (FD_ABORTED(v)) return v;
    else if (v == FD_DEFAULT) return FD_TRUE;
    else {
      fd_decref(v);
      return FD_FALSE;}}
}

static lispval badp_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval to_eval = fd_get_arg(expr,1);
  if (FD_SYMBOLP(to_eval)) {
    lispval v = fd_symeval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return FD_TRUE;
    else if  ( (v == FD_NULL) || (! (FD_CHECK_PTR(v)) ) ) {
      u8_log(LOG_WARN,fd_BadPtr,"Bad pointer value 0x%llx for %q",v,to_eval);
      return FD_TRUE;}
    else return FD_FALSE;}
  else {
    lispval v = fd_eval(to_eval,env);
    if (FD_ABORTED(v)) return v;
    else if ( ( v == VOID ) || ( v == FD_UNBOUND ) )
      return FD_TRUE;
    else if  ( (v == FD_NULL) || (! (FD_CHECK_PTR(v)) ) ) {
      u8_log(LOG_WARN,fd_BadPtr,"Bad pointer value 0x%llx for %q",v,to_eval);
      return FD_TRUE;}
    else {
      fd_decref(v);
      return FD_FALSE;}}
}

static lispval env_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  return (lispval)fd_copy_env(env);
}

static lispval env_reset_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  if ( (env->env_copy) && (env != env->env_copy) ) {
    lispval tofree = (lispval) env;
    env->env_copy=NULL;
    fd_decref(tofree);}
  return VOID;
}

static lispval symbol_boundp_prim(lispval symbol,lispval envarg)
{
  if (!(SYMBOLP(symbol)))
    return fd_type_error(_("symbol"),"symbol_boundp_prim",symbol);
  else if (FD_LEXENVP(envarg)) {
    fd_lexenv env = (fd_lexenv)envarg;
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    else if (VOIDP(val)) return FD_FALSE;
    else if (val == FD_DEFAULT_VALUE)
      return FD_FALSE;
    else if (val == FD_UNBOUND)
      return FD_FALSE;
    else {
      fd_decref(val);
      return FD_TRUE;}}
  else if (TABLEP(envarg)) {
    lispval val = fd_get(envarg,symbol,VOID);
    if (VOIDP(val)) return FD_FALSE;
    else {
      fd_decref(val);
      return FD_TRUE;}}
  else return fd_type_error(_("environment"),"symbol_boundp_prim",envarg);
}

static lispval environmentp_prim(lispval arg)
{
  if (FD_LEXENVP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

/* Withenv forms */

static lispval withenv(lispval expr,fd_lexenv env,
                      fd_lexenv consed_env,u8_context cxt)
{
  lispval bindings = fd_get_arg(expr,1);
  if (VOIDP(bindings))
    return fd_err(fd_TooFewExpressions,cxt,NULL,expr);
  else if ((NILP(bindings))||(FALSEP(bindings))) {}
  else if (PAIRP(bindings)) {
    FD_DOLIST(varval,bindings) {
      if ((PAIRP(varval))&&(SYMBOLP(FD_CAR(varval)))&&
          (PAIRP(FD_CDR(varval)))&&
          (NILP(FD_CDR(FD_CDR(varval))))) {
        lispval var = FD_CAR(varval), val = fd_eval(FD_CADR(varval),env);
        if (FD_ABORTED(val)) return val;
        int rv=fd_bind_value(var,val,consed_env);
        fd_decref(val);
        if (rv<0) return FD_ERROR;}
      else return fd_err(fd_SyntaxError,cxt,NULL,expr);}}
  else if (TABLEP(bindings)) {
    lispval keys = fd_getkeys(bindings);
    DO_CHOICES(key,keys) {
      if (SYMBOLP(key)) {
         int rv=0; lispval value = fd_get(bindings,key,VOID);
        if (!(VOIDP(value)))
          rv=fd_bind_value(key,value,consed_env);
        fd_decref(value);
        if (rv<0) return FD_ERROR;}
      else {
        FD_STOP_DO_CHOICES;
        fd_recycle_lexenv(consed_env);
        return fd_err(fd_SyntaxError,cxt,NULL,expr);}
      fd_decref(keys);}}
  else return fd_err(fd_SyntaxError,cxt,NULL,expr);
  /* Execute the body */ {
    lispval result = VOID;
    lispval body = fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result);
      result = fd_eval(elt,consed_env);
      if (FD_ABORTED(result)) {
        return result;}}
    return result;}
}

static lispval withenv_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv consed_env = fd_working_lexenv();
  lispval result = withenv(expr,env,consed_env,"WITHENV");
  fd_recycle_lexenv(consed_env);
  return result;
}

static lispval withenv_safe_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv consed_env = fd_safe_working_lexenv();
  lispval result = withenv(expr,env,consed_env,"WITHENV/SAFE");
  fd_recycle_lexenv(consed_env);
  return result;
}

/* Eval/apply related primitives */

static lispval get_arg_prim(lispval expr,lispval elt,lispval dflt)
{
  if (PAIRP(expr))
    if (FD_UINTP(elt)) {
      int i = 0, lim = FIX2INT(elt); lispval scan = expr;
      while ((i<lim) && (PAIRP(scan))) {
        scan = FD_CDR(scan); i++;}
      if (PAIRP(scan)) return fd_incref(FD_CAR(scan));
      else return fd_incref(dflt);}
    else return fd_type_error(_("fixnum"),"get_arg_prim",elt);
  else return fd_type_error(_("pair"),"get_arg_prim",expr);
}

static lispval apply_lexpr(int n,lispval *args)
{
  DO_CHOICES(fn,args[0])
    if (!(FD_APPLICABLEP(args[0]))) {
      FD_STOP_DO_CHOICES;
      return fd_type_error("function","apply_lexpr",args[0]);}
  {
    lispval results = EMPTY;
    DO_CHOICES(fn,args[0]) {
      DO_CHOICES(final_arg,args[n-1]) {
        lispval result = VOID;
        int final_length = fd_seq_length(final_arg);
        int n_args = (n-2)+final_length;
        lispval *values = u8_alloc_n(n_args,lispval);
        int i = 1, j = 0, lim = n-1;
        /* Copy regular arguments */
        while (i<lim) {values[j]=fd_incref(args[i]); j++; i++;}
        i = 0; while (j<n_args) {
          values[j]=fd_seq_elt(final_arg,i); j++; i++;}
        result = fd_apply(fn,n_args,values);
        if (FD_ABORTED(result)) {
          fd_decref(results);
          FD_STOP_DO_CHOICES;
          i = 0; while (i<n_args) {fd_decref(values[i]); i++;}
          u8_free(values);
          results = result;
          break;}
        else {CHOICE_ADD(results,result);}
        i = 0; while (i<n_args) {fd_decref(values[i]); i++;}
        u8_free(values);}
      if (FD_ABORTED(results)) {
        FD_STOP_DO_CHOICES;
        return results;}}
    return simplify_value(results);
  }
}

/* Initialization */

extern void fd_init_coreprims_c(void);

static lispval lispenv_get(lispval e,lispval s,lispval d)
{
  lispval result = fd_symeval(s,FD_XENV(e));
  if (VOIDP(result)) return fd_incref(d);
  else return result;
}
static int lispenv_store(lispval e,lispval s,lispval v)
{
  return fd_bind_value(s,v,FD_XENV(e));
}

static int lispenv_add(lispval e,lispval s,lispval v)
{
  return add_to_value(s,v,FD_XENV(e));
}

/* Some datatype methods */

static int unparse_evalfn(u8_output out,lispval x)
{
  struct FD_EVALFN *s=
    fd_consptr(struct FD_EVALFN *,x,fd_evalfn_type);
  lispval moduleid = s->evalfn_moduleid;
  u8_string modname =
    (FD_SYMBOLP(moduleid)) ? (FD_SYMBOL_NAME(moduleid)) : (NULL);
  if (s->evalfn_filename) {
    u8_string filename = s->evalfn_filename;
    size_t len = strlen(filename);
    u8_string space_break=strchr(filename,' ');
    u8_byte buf[len+1];
    if (space_break) {
      strcpy(buf,filename);
      buf[space_break-filename]='\0';
      filename=buf;}
    u8_printf(out,"#<EvalFN %s %s%s%s%s%s%s>",
              s->evalfn_name,
              U8OPTSTR(" ",modname,""),
              U8OPTSTR(" '",buf,"'"));}
  else u8_printf(out,"#<EvalFN %s%s%s%s>",s->evalfn_name,
                 U8OPTSTR(" ",modname,""));
  return 1;
}

static ssize_t write_evalfn_dtype(struct FD_OUTBUF *out,lispval x)
{
  struct FD_EVALFN *s=
    fd_consptr(struct FD_EVALFN *,x,fd_evalfn_type);
  u8_string name=s->evalfn_name;
  u8_string filename=s->evalfn_filename;
  size_t name_len=strlen(name);
  ssize_t file_len=(filename) ? (strlen(filename)) : (-1);
  int n_elts = (file_len<0) ? (1) : (0);
  unsigned char buf[100], *tagname="%EVALFN";
  struct FD_OUTBUF tmp = { 0 };
  FD_INIT_OUTBUF(&tmp,buf,100,0);
  fd_write_byte(&tmp,dt_compound);
  fd_write_byte(&tmp,dt_symbol);
  fd_write_4bytes(&tmp,strlen(tagname));
  fd_write_bytes(&tmp,tagname,strlen(tagname));
  fd_write_byte(&tmp,dt_vector);
  fd_write_4bytes(&tmp,n_elts);
  fd_write_byte(&tmp,dt_string);
  fd_write_4bytes(&tmp,name_len);
  fd_write_bytes(&tmp,name,name_len);
  if (file_len>=0) {
    fd_write_byte(&tmp,dt_string);
    fd_write_4bytes(&tmp,file_len);
    fd_write_bytes(&tmp,filename,file_len);}
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

FD_EXPORT void recycle_evalfn(struct FD_RAW_CONS *c)
{
  struct FD_EVALFN *sf = (struct FD_EVALFN *)c;
  u8_free(sf->evalfn_name);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

/* Call/cc */

static lispval call_continuation(struct FD_FUNCTION *f,lispval arg)
{
  struct FD_CONTINUATION *cont = (struct FD_CONTINUATION *)f;
  if (cont->retval == FD_NULL)
    return fd_err(ExpiredThrow,"call_continuation",NULL,arg);
  else if (VOIDP(cont->retval)) {
    cont->retval = fd_incref(arg);
    return FD_THROW_VALUE;}
  else return fd_err(DoubleThrow,"call_continuation",NULL,arg);
}

static lispval callcc (lispval proc)
{
  lispval continuation, value;
  struct FD_CONTINUATION *f = u8_alloc(struct FD_CONTINUATION);
  FD_INIT_FRESH_CONS(f,fd_cprim_type);
  f->fcn_name="continuation"; f->fcn_filename = NULL;
  f->fcn_ndcall = 1; f->fcn_xcall = 1; f->fcn_arity = 1; f->fcn_min_arity = 1;
  f->fcn_typeinfo = NULL; f->fcn_defaults = NULL;
  f->fcn_handler.xcall1 = call_continuation; f->retval = VOID;
  continuation = LISP_CONS(f);
  value = fd_apply(proc,1,&continuation);
  if ((value == FD_THROW_VALUE) && (!(VOIDP(f->retval)))) {
    lispval retval = f->retval;
    f->retval = FD_NULL;
    if (FD_CONS_REFCOUNT(f)>1)
      u8_log(LOG_WARN,ExpiredThrow,"Dangling pointer exists to continuation");
    fd_decref(continuation);
    return retval;}
  else {
    if (FD_CONS_REFCOUNT(f)>1)
      u8_log(LOG_WARN,LostThrow,"Dangling pointer exists to continuation");
    fd_decref(continuation);
    return value;}
}

/* Cache call */

static lispval cachecall(int n,lispval *args)
{
  if (HASHTABLEP(args[0]))
    return fd_xcachecall((fd_hashtable)args[0],args[1],n-2,args+2);
  else return fd_cachecall(args[0],n-1,args+1);
}

static lispval cachecall_probe(int n,lispval *args)
{
  if (HASHTABLEP(args[0]))
    return fd_xcachecall_try((fd_hashtable)args[0],args[1],n-2,args+2);
  else return fd_cachecall_try(args[0],n-1,args+1);
}

static lispval cachedcallp(int n,lispval *args)
{
  if (HASHTABLEP(args[0]))
    if (fd_xcachecall_probe((fd_hashtable)args[0],args[1],n-2,args+2))
      return FD_TRUE;
    else return FD_FALSE;
  else if (fd_cachecall_probe(args[0],n-1,args+1))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval clear_callcache(lispval arg)
{
  fd_clear_callcache(arg);
  return VOID;
}

static lispval tcachecall(int n,lispval *args)
{
  return fd_tcachecall(args[0],n-1,args+1);
}

static lispval with_threadcache_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  struct FD_THREAD_CACHE *tc = fd_push_threadcache(NULL);
  lispval value = VOID;
  FD_DOLIST(each,FD_CDR(expr)) {
    fd_decref(value); value = VOID; value = fd_eval(each,env);
    if (FD_ABORTED(value)) {
      fd_pop_threadcache(tc);
      return value;}}
  fd_pop_threadcache(tc);
  return value;
}

static lispval using_threadcache_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  struct FD_THREAD_CACHE *tc = fd_use_threadcache();
  lispval value = VOID;
  FD_DOLIST(each,FD_CDR(expr)) {
    fd_decref(value); value = VOID; value = fd_eval(each,env);
    if (FD_ABORTED(value)) {
      if (tc) fd_pop_threadcache(tc);
      return value;}}
  if (tc) fd_pop_threadcache(tc);
  return value;
}

static lispval use_threadcache_prim(lispval arg)
{
  if (FALSEP(arg)) {
    if (!(fd_threadcache)) return FD_FALSE;
    while (fd_threadcache) fd_pop_threadcache(fd_threadcache);
    return FD_TRUE;}
  else {
    struct FD_THREAD_CACHE *tc = fd_use_threadcache();
    if (tc) return FD_TRUE;
    else return FD_FALSE;}
}

/* Making DTPROCs */

static lispval make_dtproc(lispval name,lispval server,lispval min_arity,
                          lispval arity,lispval minsock,lispval maxsock,
                          lispval initsock)
{
  lispval result;
  if (VOIDP(min_arity))
    result = fd_make_dtproc(SYM_NAME(name),CSTRING(server),1,-1,-1,
                          FIX2INT(minsock),FIX2INT(maxsock),
                          FIX2INT(initsock));
  else if (VOIDP(arity))
    result = fd_make_dtproc
      (SYM_NAME(name),CSTRING(server),
       1,fd_getint(arity),fd_getint(arity),
       FIX2INT(minsock),FIX2INT(maxsock),
       FIX2INT(initsock));
  else result=
         fd_make_dtproc
         (SYM_NAME(name),CSTRING(server),1,
          fd_getint(arity),fd_getint(min_arity),
          FIX2INT(minsock),FIX2INT(maxsock),
          FIX2INT(initsock));
  return result;
}

/* Remote evaluation */

static u8_condition ServerUndefined=_("Server unconfigured");

FD_EXPORT lispval fd_open_dtserver(u8_string server,int bufsiz)
{
  struct FD_DTSERVER *dts = u8_alloc(struct FD_DTSERVER);
  u8_string server_addr; u8_socket socket;
  /* Start out by parsing the address */
  if ((*server)==':') {
    lispval server_id = fd_config_get(server+1);
    if (STRINGP(server_id))
      server_addr = u8_strdup(CSTRING(server_id));
    else  {
      fd_seterr(ServerUndefined,"open_server",
                dts->dtserverid,server_id);
      u8_free(dts);
      return -1;}}
  else server_addr = u8_strdup(server);
  dts->dtserverid = u8_strdup(server);
  dts->dtserver_addr = server_addr;
  /* Then try to connect, just to see if that works */
  socket = u8_connect_x(server,&(dts->dtserver_addr));
  if (socket<0) {
    /* If connecting fails, signal an error rather than creating
       the dtserver connection pool. */
    u8_free(dts->dtserverid);
    u8_free(dts->dtserver_addr);
    u8_free(dts);
    return fd_err(fd_ConnectionFailed,"fd_open_dtserver",
                  u8_strdup(server),VOID);}
  /* Otherwise, close the socket */
  else close(socket);
  /* And create a connection pool */
  dts->connpool = u8_open_connpool(dts->dtserverid,2,4,1);
  /* If creating the connection pool fails for some reason,
     cleanup and return an error value. */
  if (dts->connpool == NULL) {
    u8_free(dts->dtserverid);
    u8_free(dts->dtserver_addr);
    u8_free(dts);
    return FD_ERROR;}
  /* Otherwise, returh a dtserver object */
  FD_INIT_CONS(dts,fd_dtserver_type);
  return LISP_CONS(dts);
}

static lispval dtserverp(lispval arg)
{
  if (TYPEP(arg,fd_dtserver_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval dtserver_id(lispval arg)
{
  struct FD_DTSERVER *dts = (struct FD_DTSERVER *) arg;
  return lispval_string(dts->dtserverid);
}

static lispval dtserver_address(lispval arg)
{
  struct FD_DTSERVER *dts = (struct FD_DTSERVER *) arg;
  return lispval_string(dts->dtserver_addr);
}

static lispval dteval(lispval server,lispval expr)
{
  if (TYPEP(server,fd_dtserver_type))  {
    struct FD_DTSERVER *dtsrv=
      fd_consptr(fd_stream_erver,server,fd_dtserver_type);
    return fd_dteval(dtsrv->connpool,expr);}
  else if (STRINGP(server)) {
    lispval s = fd_open_dtserver(CSTRING(server),-1);
    if (FD_ABORTED(s)) return s;
    else {
      lispval result = fd_dteval(((fd_stream_erver)s)->connpool,expr);
      fd_decref(s);
      return result;}}
  else return fd_type_error(_("server"),"dteval",server);
}

static lispval dtcall(int n,lispval *args)
{
  lispval server; lispval request = NIL, result; int i = n-1;
  if (n<2) return fd_err(fd_SyntaxError,"dtcall",NULL,VOID);
  if (TYPEP(args[0],fd_dtserver_type))
    server = fd_incref(args[0]);
  else if (STRINGP(args[0])) server = fd_open_dtserver(CSTRING(args[0]),-1);
  else return fd_type_error(_("server"),"eval/dtcall",args[0]);
  if (FD_ABORTED(server)) return server;
  while (i>=1) {
    lispval param = args[i];
    if ((i>1) && ((SYMBOLP(param)) || (PAIRP(param))))
      request = fd_conspair(fd_make_list(2,quote_symbol,param),request);
    else request = fd_conspair(param,request);
    fd_incref(param); i--;}
  result = fd_dteval(((fd_stream_erver)server)->connpool,request);
  fd_decref(request);
  fd_decref(server);
  return result;
}

static lispval open_dtserver(lispval server,lispval bufsiz)
{
  return fd_open_dtserver(CSTRING(server),
                             ((VOIDP(bufsiz)) ? (-1) :
                              (FIX2INT(bufsiz))));
}

/* Test functions */

#define LEAVE_EXSTACK 0

static lispval failed_tests = FD_EMPTY_CHOICE;
static u8_mutex failed_tests_lock;

static void add_failed_test(lispval object)
{
  u8_lock_mutex(&failed_tests_lock);
  FD_ADD_TO_CHOICE(failed_tests,object);
  u8_unlock_mutex(&failed_tests_lock);
}

static int config_failed_tests_set(lispval var,lispval val,void *data)
{
  lispval *ptr = data;
  u8_lock_mutex(&failed_tests_lock);
  lispval cur = *ptr;
  if (FD_FALSEP(val)) {
    *ptr = FD_EMPTY;
    fd_decref(cur);}
  else {
    fd_incref(val);
    FD_ADD_TO_CHOICE(cur,val);
    *ptr = cur;}
  u8_unlock_mutex(&failed_tests_lock);
  return 1;
}

static lispval applytest_inner(int n,lispval *args)
{
  if (n<2)
    return fd_err(fd_TooFewArgs,"applytest",NULL,VOID);
  else if (FD_APPLICABLEP(args[1])) {
    lispval value = fd_apply(args[1],n-2,args+2);
    if (FD_ABORTP(value)) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      u8_exception ex = u8_erreify();
      u8_printf(&out,"Error returned from %q",args[1]);
      int i=2; while (i<n) {u8_printf(&out," %q",args[i]); i++;}
      u8_printf(&out,"\n   Error:    %m <%s> %s%s%s",
                ex->u8x_cond,ex->u8x_context,
                U8OPTSTR(" (",ex->u8x_details,")"));
      u8_printf(&out,"\n   Expected: %q",args[0]);
      u8_restore_exception(ex);
      lispval err = fd_err(TestError,"applytest",out.u8_outbuf,args[1]);
      u8_close_output(&out);
      fd_decref(value);
      return err;}
    else if (FD_EQUAL(value,args[0])) {
      fd_decref(value);
      return FD_TRUE;}
    else if ( (FD_VOIDP(value)) && (args[0] == FDSYM_VOID) ) {
      fd_decref(value);
      return FD_TRUE;}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      u8_printf(&out,"Unexpected result from %q",args[1]);
      u8_printf(&out,"\n   Expected: %q",args[0]);
      u8_printf(&out,"\n        Got: %q",value);
      lispval err = fd_err(TestFailed,"applytest",out.u8_outbuf,value);
      u8_close_output(&out);
      fd_decref(value);
      return err;}}
  else if (n==2) {
    if (FD_EQUAL(args[1],args[0]))
      return FD_TRUE;
    else {
      u8_string s = u8_mkstring("%q ≭ %q",args[0],args[1]);
      lispval err = fd_err(TestFailed,"applytest",s,args[1]);
      u8_free(s);
      return err;}}
  else if (n == 3) {
    if (FD_EQUAL(args[2],args[0]))
      return FD_TRUE;
    else {
      u8_string s = u8_mkstring("%q: %q ≭ %q",args[1],args[0],args[2]);
      lispval err = fd_err(TestFailed,"applytest",s,args[2]);
      u8_free(s);
      return err;}}
  else return fd_err(fd_TooManyArgs,"applytest",NULL,VOID);
}

static lispval applytest(int n,lispval *args)
{
  lispval v = applytest_inner(n,args);
  if (FD_ABORTP(v)) {
    if ( (FD_VOIDP(log_testfail)) || (FD_FALSEP(log_testfail)) )
      return v;
    else {
      u8_exception ex = u8_erreify();
      if (ex->u8x_cond == TestFailed)
        u8_log(LOG_CRIT,TestFailed,"In applytest:\n %s",ex->u8x_details);
      else u8_log(LOG_CRIT,TestError,"In applytest:\n %s",ex->u8x_details);
      add_failed_test(fd_get_exception(ex));
      u8_free_exception(ex,LEAVE_EXSTACK);
      fd_clear_errors(1);
      return FD_FALSE;}}
  else return v;
}

static u8_string name2string(lispval x)
{
  if (FD_SYMBOLP(x)) return FD_SYMBOL_NAME(x);
  else if (FD_STRINGP(x)) return FD_CSTRING(x);
  else return NULL;
}

static lispval evaltest_inner(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval testexpr  = fd_get_arg(expr,2);
  if (VOIDP(testexpr))
    return fd_err(fd_SyntaxError,"evaltest",NULL,expr);
  lispval name_expr = fd_get_arg(expr,3);
  lispval name_value = (FD_VOIDP(name_expr)) ? (FD_VOID) :
    (FD_SYMBOLP(name_expr)) ? (name_expr) :
    (fd_eval(name_expr,env));
  u8_string name = name2string(name_value);

  lispval expected_expr  = fd_get_arg(expr,1);
  lispval expected  = fd_eval(expected_expr,env);
  if (FD_ABORTED(expected)) {
    fd_seterr("BadExpectedValue","evaltest_evalfn",name,expected_expr);
    return expected;}
  else {
    lispval value = fd_eval(testexpr,env);
    if (FD_ABORTED(value)) {
      u8_exception ex = u8_erreify();
      u8_string msg =
        u8_mkstring("%s%s%s%q signaled an error %m <%s> %s%s%s, expecting %q",
                    U8OPTSTR("(",name,") "),testexpr,
                    ex->u8x_cond,ex->u8x_context,
                    U8OPTSTR(" (",ex->u8x_details,")"),
                    expected);
      lispval err = fd_err(TestError,"evaltest",msg,value);
      fd_decref(expected); u8_free(msg);
      return err;}
    else if (FD_EQUAL(value,expected)) {
      fd_decref(name_value);
      fd_decref(expected);
      fd_decref(value);
      return FD_TRUE;}
    else if ( (FD_VOIDP(value)) &&
              (expected == FDSYM_VOID) ) {
      fd_decref(name_value);
      fd_decref(expected);
      fd_decref(value);
      return FD_TRUE;}
    else {
      u8_string msg =
        u8_mkstring("%s%s%s%q"
                    "\n returned   %q"
                    "\n instead of %q",
                    U8OPTSTR("(",name,") "),testexpr,
                    value,expected);
      lispval err = fd_err(TestFailed,"evaltest",msg,value);
      u8_free(msg); fd_decref(name_value);
      fd_decref(value); fd_decref(expected);
      return err;}}
}

static lispval evaltest_evalfn(lispval expr,fd_lexenv env,fd_stack s)
{
  lispval v = evaltest_inner(expr,env,s);
  if (FD_ABORTP(v)) {
    if ( (FD_VOIDP(log_testfail)) || (FD_FALSEP(log_testfail)) )
      return v;
    else {
      u8_exception ex = u8_erreify();
      if (ex->u8x_cond == TestFailed)
        u8_log(LOG_CRIT,TestFailed,"In evaltest:\n %s",ex->u8x_details);
      else u8_log(LOG_CRIT,TestError,"In applytest:\n %s",ex->u8x_details);
      add_failed_test(fd_get_exception(ex));
      u8_free_exception(ex,LEAVE_EXSTACK);
      fd_clear_errors(1);
      return FD_FALSE;}}
  else return v;
}

/* Getting documentation */

FD_EXPORT
u8_string fd_get_documentation(lispval x)
{
  lispval proc = (FD_FCNIDP(x)) ? (fd_fcnid_ref(x)) : (x);
  fd_ptr_type proctype = FD_PTR_TYPE(proc);
  if (proctype == fd_lambda_type) {
    struct FD_LAMBDA *lambda = (fd_lambda)proc;
    if (lambda->fcn_documentation)
      return lambda->fcn_documentation;
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,120);
      lispval arglist = lambda->lambda_arglist, scan = arglist;
      if (lambda->fcn_name)
        u8_puts(&out,lambda->fcn_name);
      else u8_puts(&out,"λ");
      while (PAIRP(scan)) {
        lispval arg = FD_CAR(scan);
        if (SYMBOLP(arg))
          u8_printf(&out," %ls",SYM_NAME(arg));
        else if ((PAIRP(arg))&&(SYMBOLP(FD_CAR(arg))))
          u8_printf(&out," [%ls]",SYM_NAME(FD_CAR(arg)));
        else u8_printf(&out," %q",arg);
        scan = FD_CDR(scan);}
      if (SYMBOLP(scan))
        u8_printf(&out," [%ls...]",SYM_NAME(scan));
      lambda->fcn_documentation = out.u8_outbuf;
      return out.u8_outbuf;}}
  else if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(proc);
    return f->fcn_documentation;}
  else if (TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = (fd_evalfn)proc;
    return sf->evalfn_documentation;}
  else return NULL;
}

static lispval get_documentation(lispval x)
{
  u8_string doc = fd_get_documentation(x);
  if (doc) return lispval_string(doc);
  else return FD_FALSE;
}

/* Debugging assistance */

FD_EXPORT lispval _fd_dbg(lispval x)
{
  lispval result=_fd_debug(x);
  if (result == x)
    return result;
  else {
    fd_incref(result);
    fd_decref(x);
    return result;}
}

int (*fd_dump_exception)(lispval ex);

static lispval dbg_evalfn(lispval expr,fd_lexenv env,fd_stack stack)
{
  lispval arg_expr=fd_get_arg(expr,1);
  lispval msg_expr=fd_get_arg(expr,2);
  lispval arg=fd_eval(arg_expr,env);
  if (VOIDP(msg_expr))
    u8_message("Debug %q",arg);
  else {
    lispval msg=fd_eval(msg_expr,env);
    if (VOIDP(msg_expr))
      u8_message("Debug %q",arg);
    else if (FALSEP(msg)) {}
    else if (VOIDP(arg))
      u8_message("Debug called");
    else u8_message("Debug (%q) %q",msg,arg);
    fd_decref(msg);}
  return _fd_dbg(arg);
}

/* Recording bugs */

FD_EXPORT int fd_dump_bug(lispval ex,u8_string into)
{
  u8_string bugpath;
  if (into == NULL)
    return 0;
  else if (u8_directoryp(into)) {
    struct U8_XTIME now;
    u8_string bugdir = u8_abspath(into,NULL);
    long long pid = getpid();
    long long tid = u8_threadid();
    u8_string appid = u8_appid();
    u8_now(&now);
    u8_string filename = ( pid == tid ) ?
      (u8_mkstring("%s-p%lld-%4d-%02d-%02dT%02d:%02d:%02f.err",
                   appid,pid,
                   now.u8_year,now.u8_mon+1,now.u8_mday,
                   now.u8_hour,now.u8_min,
                   (1.0*now.u8_sec)+(now.u8_nsecs/1000000000.0))) :
      (u8_mkstring("%s-p%lld-%4d-%02d-%02dT%02d:%02d:%02f-t%lld.err",
                   appid,pid,
                   now.u8_year,now.u8_mon+1,now.u8_mday,
                   now.u8_hour,now.u8_min,
                   (1.0*now.u8_sec)+(now.u8_nsecs/1000000000.0),
                   pid,tid));
    bugpath = u8_mkpath(bugdir,filename);
    u8_free(bugdir);
    u8_free(filename);}
  else bugpath = u8_strdup(into);
  struct FD_EXCEPTION *exo = (fd_exception) ex;
  int rv = fd_write_dtype_to_file(ex,bugpath);
  if (rv<0)
    u8_log(LOG_CRIT,"RecordBug",
           "Couldn't write exception object to %s",bugpath);
  else if (exo->ex_details)
    u8_log(LOG_WARN,"RecordBug",
           "Wrote exception %m <%s> (%s) to %s",
           exo->ex_condition,exo->ex_caller,exo->ex_details,
           bugpath);
  else u8_log(LOG_WARN,"RecordBug",
              "Wrote exception %m <%s> to %s",
              exo->ex_condition,exo->ex_caller,bugpath);
  u8_free(bugpath);
  return rv;
}

FD_EXPORT int fd_record_bug(lispval ex)
{
  if (fd_bugdir == NULL)
    return 0;
  else return fd_dump_bug(ex,fd_bugdir);
}

FD_EXPORT lispval dumpbug_prim(lispval ex,lispval where)
{
  if (FD_TRUEP(where)) {
    struct FD_OUTBUF bugout; FD_INIT_BYTE_OUTPUT(&bugout,16000);
    int rv = fd_write_dtype(&bugout,ex);
    if (rv<0) {
      fd_close_outbuf(&bugout);
      return FD_ERROR;}
    else {
      lispval packet = fd_make_packet(NULL,BUFIO_POINT(&bugout),bugout.buffer);
      fd_close_outbuf(&bugout);
      return packet;}}
  else if ( (FD_VOIDP(where)) || (FD_FALSEP(where)) || (FD_DEFAULTP(where)) ) {
    u8_string bugdir = fd_bugdir;
    if (bugdir == NULL) {
      u8_string cwd = u8_getcwd();
      u8_string errpath = u8_mkpath(cwd,"_bugjar");
      if ( (u8_directoryp(errpath)) && (u8_file_writablep(errpath)) )
        bugdir = errpath;
      else if ( (u8_directoryp(cwd)) && (u8_file_writablep(cwd)) )
        bugdir = cwd;
      else bugdir = u8_strdup("/tmp/");
      if (bugdir != errpath) u8_free(errpath);
      if (bugdir != cwd) u8_free(cwd);}
    else NO_ELSE; /* got bugdir already */
    int rv = fd_dump_bug(ex,bugdir);
    if (rv<0)
      fd_seterr("RECORD-BUG failed","record_bug",NULL,where);
    if (bugdir != fd_bugdir) u8_free(bugdir);
    if (rv<0) return FD_ERROR;
    else return FD_TRUE;}
  else if (FD_STRINGP(where)) {
    int rv = fd_dump_bug(ex,FD_CSTRING(where));
    if (rv<0) {
      fd_seterr("RECORD-BUG failed","record_bug",
                FD_CSTRING(where),ex);
      return FD_ERROR;}
    else return FD_TRUE;}
  else return fd_type_error("filename","record_bug",where);
}

static int config_bugdir(lispval var,lispval val,void *state)
{
  if (FD_FALSEP(val)) {
    fd_dump_exception = NULL;
    u8_free(fd_bugdir);
    fd_bugdir = NULL;
    return 0;}
  else if ( ( fd_dump_exception == NULL ) ||
            ( fd_dump_exception == fd_record_bug ) ) {
    if (FD_STRINGP(val)) {
      u8_string old = fd_bugdir;
      u8_string new = FD_CSTRING(val);
      if (u8_directoryp(new)) {}
      else if (u8_file_existsp(new)) {
        fd_seterr("not a directory","config_bugdir",u8_strdup(new),VOID);
        return -1;}
      else {
        int rv = u8_mkdirs(new,0664);
        if (rv<0) {
          u8_graberrno("config_bugdir/mkdir",u8_strdup(new));
          u8_seterr(_("missing directory"),"config_bugdir",u8_strdup(new));
          return -1;}}
      fd_bugdir = u8_strdup(new);
      if (old) u8_free(old);
      fd_dump_exception = fd_record_bug;
      return 1;}
    else {
      fd_seterr(fd_TypeError,"config_bugdir",u8_strdup("pathstring"),val);
      return -1;}}
  else {
    u8_seterr("existing bug handler","config_bugdir",NULL);
    return -1;}
}

/* Helpful forms and functions */

static lispval void_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = FD_CDR(expr);
  FD_DOLIST(subex,body) {
    lispval v = fd_stack_eval(subex,env,_stack,0);
    if (FD_ABORTED(v))
      return v;
    else fd_decref(v);}
  return VOID;
}

static lispval break_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval body = FD_CDR(expr);
  FD_DOLIST(subex,body) {
    lispval v = fd_stack_eval(subex,env,_stack,0);
    if (FD_BREAKP(v))
      return FD_BREAK;
    else if (FD_ABORTED(v))
      return v;
    else fd_decref(v);}
  return FD_BREAK;
}

static lispval default_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval symbol = fd_get_arg(expr,1);
  lispval default_expr = fd_get_arg(expr,2);
  if (!(SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"default_evalfn",NULL,fd_incref(expr));
  else if (VOIDP(default_expr))
    return fd_err(fd_SyntaxError,"default_evalfn",NULL,fd_incref(expr));
  else {
    lispval val = fd_symeval(symbol,env);
    if (FD_ABORTED(val)) return val;
    else if (VOIDP(val))
      return fd_eval(default_expr,env);
    else if (val == FD_UNBOUND)
      return fd_eval(default_expr,env);
    else return val;}
}

/* Checking version numbers */

static int check_num(lispval arg,int num)
{
  if ((!(FIXNUMP(arg)))||(FIX2INT(arg)<0)) {
    fd_seterr(fd_TypeError,"check_version_prim",NULL,arg);
    return -1;}
  else {
    int n = FIX2INT(arg);
    if (num>=n)
      return 1;
    else return 0;}
}

static lispval check_version_prim(int n,lispval *args)
{
  int rv = check_num(args[0],FD_MAJOR_VERSION);
  if (rv<0) return FD_ERROR;
  else if (rv==0) return FD_FALSE;
  else if (n==1) return FD_TRUE;
  else rv = check_num(args[1],FD_MINOR_VERSION);
  if (rv<0) return FD_ERROR;
  else if (rv==0) return FD_FALSE;
  else if (n==2) return FD_TRUE;
  else rv = check_num(args[2],FD_RELEASE_VERSION);
  if (rv<0) return FD_ERROR;
  else if (rv==0) return FD_FALSE;
  else if (n==3) return FD_TRUE;
  else rv = check_num(args[2],FD_RELEASE_VERSION-1);
  /* The fourth argument should be a patch level, but we're not
     getting that in builds yet. So if there are more arguments,
     we see if required release number is larger than release-1
     (which means that we should be okay, since patch levels
     are reset with releases. */
  if (rv<0) return FD_ERROR;
  else if (rv) {
    int i = 3; while (i<n) {
      if (!(FIXNUMP(args[i]))) {
        fd_seterr(fd_TypeError,"check_version_prim",NULL,args[i]);
        return -1;}
      else i++;}
    return FD_TRUE;}
  else return FD_FALSE;
}

static lispval require_version_prim(int n,lispval *args)
{
  lispval result = check_version_prim(n,args);
  if (FD_ABORTP(result))
    return result;
  else if (FD_TRUEP(result))
    return result;
  else {
    u8_byte buf[50];
    int i = 0; while (i<n) {
      if (!(FD_FIXNUMP(args[i])))
        return fd_type_error("version number(integer)","require_version_prim",args[i]);
      else i++;}
    fd_seterr("VersionError","require_version_prim",
              u8_sprintf(buf,50,"Version is %s",FRAMERD_REVISION),
              /* We don't need to incref *args* because they're all fixnums */
              fd_make_vector(n,args));
    return FD_ERROR;}
}

/* Choice functions */

static lispval fixchoice_prim(lispval arg)
{
  if (PRECHOICEP(arg))
    return fd_make_simple_choice(arg);
  else return fd_incref(arg);
}

static lispval choiceref_prim(lispval arg,lispval off)
{
  if (PRED_TRUE(FIXNUMP(off))) {
    long long i = FIX2INT(off);
    if (i==0) {
      if (EMPTYP(arg)) {
        fd_seterr(fd_RangeError,"choiceref_prim","0",arg);
        return FD_ERROR;}
      else return fd_incref(arg);}
    else if (CHOICEP(arg)) {
      struct FD_CHOICE *ch = (fd_choice)arg;
      if (i>ch->choice_size) {
        u8_byte buf[50];
        fd_seterr(fd_RangeError,"choiceref_prim",
                  u8_sprintf(buf,50,"%lld",i),
                  fd_incref(arg));
        return FD_ERROR;}
      else {
        lispval elt = FD_XCHOICE_DATA(ch)[i];
        return fd_incref(elt);}}
    else if (PRECHOICEP(arg)) {
      lispval simplified = fd_make_simple_choice(arg);
      lispval result = choiceref_prim(simplified,off);
      fd_decref(simplified);
      return result;}
    else {
      u8_byte buf[50];
      fd_seterr(fd_RangeError,"choiceref_prim",
                u8_sprintf(buf,50,"%lld",i),fd_incref(arg));
      return FD_ERROR;}}
  else return fd_type_error("fixnum","choiceref_prim",off);
}

/* FFI */

#if FD_ENABLE_FFI
static lispval ffi_proc_helper(int err,int n,lispval *args)
{
  lispval name_arg = args[0], filename_arg = args[1];
  lispval return_type = args[2];
  u8_string name = (STRINGP(name_arg)) ? (CSTRING(name_arg)) : (NULL);
  u8_string filename = (STRINGP(filename_arg)) ?
    (CSTRING(filename_arg)) :
    (NULL);
  if (!(name))
    return fd_type_error("String","ffi_proc/name",name_arg);
  else if (!((STRINGP(filename_arg))||(FALSEP(filename_arg))))
    return fd_type_error("String","ffi_proc/filename",filename_arg);
  else {
    struct FD_FFI_PROC *fcn=
      fd_make_ffi_proc(name,filename,n-3,return_type,args+3);
    if (fcn)
      return (lispval) fcn;
    else {
      fd_clear_errors(1);
      return FD_FALSE;}}
}
static lispval ffi_proc(int n,lispval *args)
{
  return ffi_proc_helper(1,n,args);
}
static lispval ffi_probe(int n,lispval *args)
{
  return ffi_proc_helper(0,n,args);
}
static lispval ffi_found_prim(lispval name,lispval modname)
{
  void *module=NULL, *sym=NULL;
  if (STRINGP(modname)) {
    module=u8_dynamic_load(CSTRING(modname));
    if (module==NULL) {
      fd_clear_errors(0);
      return FD_FALSE;}}
  sym=u8_dynamic_symbol(CSTRING(name),module);
  if (sym)
    return FD_TRUE;
  else return FD_FALSE;
}
#else
static lispval ffi_proc(int n,lispval *args)
{
  u8_seterr("NotImplemented","ffi_proc",
            u8_strdup("No FFI support is available in this build of FramerD"));
  return FD_ERROR;
}
static lispval ffi_probe(int n,lispval *args)
{
  return FD_FALSE;
}
static lispval ffi_found_prim(lispval name,lispval modname)
{
  return FD_FALSE;
}
#endif

/* WITH-CONTEXT */

static lispval with_log_context_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval label_expr=fd_get_arg(expr,1);
  if (FD_VOIDP(label_expr))
    return fd_err(fd_SyntaxError,"with_context_evalfn",NULL,expr);
  else {
    lispval label=fd_stack_eval(label_expr,env,_stack,0);
    if (FD_ABORTED(label)) return label;
    else if (!(FD_STRINGP(label)))
      return fd_type_error("string","with_context_evalfn",label);
    else {
      u8_string outer_context=u8_log_context;
      u8_set_log_context(FD_CSTRING(label));
      lispval result=eval_body("with_log_context",FD_CSTRING(label),
                               expr,2,env,_stack);
      u8_set_log_context(outer_context);
      fd_decref(label);
      return result;}}
}

/* The "application" environment */

static int app_cleanup_started=0;
static u8_mutex app_cleanup_lock;

static lispval cleanup_app_env()
{
  if (fd_app_env==NULL) return FD_VOID;
  if (app_cleanup_started) return FD_VOID;
  if ( (fd_exiting) && (fd_fast_exit) ) return FD_VOID;
  u8_lock_mutex(&app_cleanup_lock);
  if (app_cleanup_started) {
    u8_unlock_mutex(&app_cleanup_lock);
    return FD_VOID;}
  else app_cleanup_started=1;
  fd_lexenv env=fd_app_env; fd_app_env=NULL;
  /* Hollow out the environment, which should let it be reclaimed.
     This patches around some of the circular references that might
     exist because working_lexenv may contain procedures which
     are closed in the working environment, so the working environment
     itself won't be GC'd because of those circular pointers. */
  unsigned int refcount = FD_CONS_REFCOUNT(env);
  fd_decref((lispval)env);
  if (refcount>1) {
    if (HASHTABLEP(env->env_bindings))
      fd_reset_hashtable((fd_hashtable)(env->env_bindings),0,1);}
  u8_unlock_mutex(&app_cleanup_lock);
  return FD_VOID;
}

static void setup_app_env()
{
  lispval exit_handler = fd_make_cprim0("APPENV/ATEXIT",cleanup_app_env);
  fd_config_set("ATEXIT",exit_handler);
  fd_decref(exit_handler);
}

static int run_init(lispval init,fd_lexenv env)
{
  lispval v = FD_VOID;
  if (FD_APPLICABLEP(init)) {
    if (FD_FUNCTIONP(init)) {
      struct FD_FUNCTION *f = (fd_function) init;
      v = (f->fcn_arity == 0) ?
        (fd_apply(init,0,NULL)) :
        (fd_apply(init,1,(lispval *)(&env)));}
    else v = fd_apply(init,0,NULL);}
  else if (FD_PAIRP(init))
    v = fd_eval(init,env);
  else NO_ELSE;
  if (FD_ABORTP(v)) {
    u8_exception ex = u8_erreify();
    u8_log(LOGCRIT,"InitFailed",
           "Failed to apply init %q: %m <%s> %s%s%s",
           init,ex->u8x_cond,ex->u8x_context,
           U8OPTSTR(" (",ex->u8x_details,")"));
    u8_free_exception(ex,1);
    return -1;}
  else {
    fd_decref(v);
    return 1;}
}

FD_EXPORT void fd_set_app_env(fd_lexenv env)
{
  if (env==fd_app_env)
    return;
  else if (fd_app_env) {
    fd_lexenv old_env=fd_app_env;
    fd_app_env=NULL;
    fd_decref((lispval)old_env);}
  fd_app_env=env;
  if (env) {
    int modules_loaded = 0, files_loaded = 0;
    int modules_failed = 0, files_failed = 0;
    int inits_run = 0, inits_failed = 0;
    lispval modules = fd_reverse(module_list);
    {FD_DOLIST(modname,modules) {
        lispval module = fd_find_module(modname,0,0);
        if (FD_ABORTP(module)) {
          u8_log(LOG_WARN,"LoadModuleError","Error loading module %q",modname);
          fd_clear_errors(1);
          modules_failed++;}
        else {
          lispval used = fd_use_module(fd_app_env,module);
          if (FD_ABORTP(used)) {
            u8_log(LOG_WARN,"UseModuleError","Error using module %q",module);
            fd_clear_errors(1);
            modules_failed++;}
          else modules_loaded++;
          fd_decref(module);
          fd_decref(used);}}}
    fd_decref(modules); modules=FD_VOID;
    lispval files = fd_reverse(loadfile_list);
    {FD_DOLIST(file,files) {
        lispval loadval = fd_load_source(FD_CSTRING(file),fd_app_env,NULL);
        if (FD_ABORTP(loadval)) {
          u8_log(LOG_WARN,"LoadError","fd_set_app_end",
                 "Error loading %s into the application environment",
                 FD_CSTRING(file));
          fd_clear_errors(1);
          files_failed++;}
        else files_loaded++;
        fd_decref(loadval);}}
    fd_decref(files); files=FD_VOID;
    lispval inits = init_list;
    {FD_DOLIST(init,inits) {
        int rv = run_init(init,env);
        if (rv > 0) inits_run++;
        else if (rv<0) inits_failed++;
        else NO_ELSE;}}
    fd_decref(inits); inits=FD_VOID;
    u8_log(LOG_INFO,"AppEnvLoad",
           "%d:%d:%d modules:files:inits loaded/run, "
           "%d:%d:%d modules:files:init failed",
           modules_loaded,files_loaded,inits_run,
           modules_failed,files_failed,inits_failed);}
}

static lispval appenv_prim()
{
  if (fd_app_env)
    return (lispval)fd_copy_env(fd_app_env);
  else return FD_FALSE;
}

/* MTrace */

#if HAVE_MTRACE
static int mtracing=0;
#endif

static lispval mtrace_prim(lispval arg)
{
  /* TODO: Check whether we're running under libc malloc (so mtrace
     will do anything). */
#if HAVE_MTRACE
  if (mtracing)
    return FD_TRUE;
  else if (getenv("MALLOC_TRACE")) {
    mtrace();
    mtracing=1;
    return FD_TRUE;}
  else if ((STRINGP(arg)) &&
           (u8_file_writablep(CSTRING(arg)))) {
    setenv("MALLOC_TRACE",CSTRING(arg),1);
    mtrace();
    mtracing=1;
    return FD_TRUE;}
  else return FD_FALSE;
#else
  return FD_FALSE;
#endif
}

static lispval muntrace_prim()
{
#if HAVE_MTRACE
  if (mtracing) {
    muntrace();
    return FD_TRUE;}
  else return FD_FALSE;
#else
  return FD_FALSE;
#endif
}

/* Primitives for testing purposes */

static lispval list9(lispval arg1,lispval arg2,
                    lispval arg3,lispval arg4,
                    lispval arg5,lispval arg6,
                    lispval arg7,lispval arg8,
                    lispval arg9)
{
  return fd_make_list(9,fd_incref(arg1),fd_incref(arg2),
                      fd_incref(arg3),fd_incref(arg4),
                      fd_incref(arg5),fd_incref(arg6),
                      fd_incref(arg7),fd_incref(arg8),
                      fd_incref(arg9));
}

/* Module and loading config */

static u8_string get_next(u8_string pt,u8_string seps);

static lispval parse_module_spec(u8_string s)
{
  if (*s) {
    u8_string brk = get_next(s," ,;");
    if (brk) {
      u8_string elt = u8_slice(s,brk);
      lispval parsed = fd_parse(elt);
      if (FD_ABORTP(parsed)) {
        u8_free(elt);
        return parsed;}
      else return fd_init_pair(NULL,parsed,
                               parse_module_spec(brk+1));}
    else {
      lispval parsed = fd_parse(s);
      if (FD_ABORTP(parsed)) return parsed;
      else return fd_init_pair(NULL,parsed,NIL);}}
  else return NIL;
}

static u8_string get_next(u8_string pt,u8_string seps)
{
  u8_string closest = NULL;
  while (*seps) {
    u8_string brk = strchr(pt,*seps);
    if ((brk) && ((brk<closest) || (closest == NULL)))
      closest = brk;
    seps++;}
  return closest;
}

static int add_modname(lispval modname)
{
  if (fd_app_env) {
    lispval module = fd_find_module(modname,0,0);
    if (FD_ABORTP(module))
      return -1;
    else if (FD_VOIDP(module)) {
      u8_log(LOG_WARN,fd_NoSuchModule,"module_config_set",
             "No module found for %q",modname);
      return -1;}
    lispval used = fd_use_module(fd_app_env,module);
    if (FD_ABORTP(used)) {
      u8_log(LOG_WARN,"LoadModuleError",
             "Error using module %q",module);
      fd_clear_errors(1);
      fd_decref(module);
      fd_decref(used);
      return -1;}
    module_list = fd_conspair(modname,module_list);
    fd_incref(modname);
    fd_decref(module);
    fd_decref(used);
    return 1;}
  else {
    module_list = fd_conspair(modname,module_list);
    fd_incref(modname);
    return 0;}
}

static int module_config_set(lispval var,lispval vals,void *d)
{
  int loads = 0; DO_CHOICES(val,vals) {
    lispval modname = ((SYMBOLP(val))?(val):
                       (STRINGP(val))?
                       (parse_module_spec(CSTRING(val))):
                       (VOID));
    if (VOIDP(modname)) {
      fd_seterr(fd_TypeError,"module_config_set","module",val);
      return -1;}
    else if (PAIRP(modname)) {
      FD_DOLIST(elt,modname) {
        if (!(SYMBOLP(elt))) {
          u8_log(LOG_WARN,fd_TypeError,"module_config_set",
                 "Not a valid module name: %q",elt);}
        else {
          int added = add_modname(elt);
          if (added>0) loads++;}}
      fd_decref(modname);}
    else if (!(SYMBOLP(modname))) {
      fd_seterr(fd_TypeError,"module_config_set","module name",val);
      fd_decref(modname);
      return -1;}
    else {
      int added = add_modname(modname);
      if (added > 0) loads++;}}
  return loads;
}

static lispval module_config_get(lispval var,void *d)
{
  return fd_incref(module_list);
}

static int loadfile_config_set(lispval var,lispval vals,void *d)
{
  int loads = 0; DO_CHOICES(val,vals) {
    if (!(STRINGP(val))) {
      fd_seterr(fd_TypeError,"loadfile_config_set","filename",val);
      return -1;}}
  if (fd_app_env == NULL) {
    FD_DO_CHOICES(val,vals) {
      u8_string loadpath = (!(strchr(CSTRING(val),':'))) ?
        (u8_abspath(CSTRING(val),NULL)) :
        (u8_strdup(CSTRING(val)));
      loadfile_list = fd_conspair(fd_lispstring(loadpath),loadfile_list);}}
  else {
    FD_DO_CHOICES(val,vals) {
      u8_string loadpath = (!(strchr(CSTRING(val),':'))) ?
        (u8_abspath(CSTRING(val),NULL)) :
        (u8_strdup(CSTRING(val)));
      lispval loadval = fd_load_source(loadpath,fd_app_env,NULL);
      if (FD_ABORTP(loadval)) {
        fd_seterr(_("load error"),"loadfile_config_set",loadpath,val);
        return -1;}
      else {
        loadfile_list = fd_conspair(fdstring(loadpath),loadfile_list);
        u8_free(loadpath);
        loads++;}}}
  return loads;
}

static lispval loadfile_config_get(lispval var,void *d)
{
  return fd_incref(loadfile_list);
}

static lispval inits_config_get(lispval var,void *d)
{
  return fd_incref(init_list);
}

static int inits_config_set(lispval var,lispval inits,void *d)
{
  int run_count = 0;
  if (fd_app_env == NULL) {
    FD_DO_CHOICES(init,inits) {
      fd_incref(init);
      init_list = fd_conspair(init,init_list);}}
  else {
    FD_DO_CHOICES(init,inits) {
      int rv = run_init(init,fd_app_env);
      if (rv > 0) {
        fd_incref(init);
        init_list = fd_conspair(init,init_list);
        run_count++;}}}
  return run_count;
}

FD_EXPORT
void fd_autoload_config(u8_string module_inits,
                        u8_string file_inits,
                        u8_string run_inits)
{
  fd_register_config
    (module_inits,_("Which modules to load into the application environment"),
     module_config_get,module_config_set,&module_list);
  fd_register_config
    (file_inits,_("Which files to load into the application environment"),
     loadfile_config_get,loadfile_config_set,&loadfile_list);
  fd_register_config
    (run_inits,_("Which functions/forms to execute to set up "
                 "the application environment"),
     inits_config_get,inits_config_set,&init_list);
}

/* Initialization */

void fd_init_eval_c()
{
  struct FD_TABLEFNS *fns = u8_alloc(struct FD_TABLEFNS);
  fns->get = lispenv_get; fns->store = lispenv_store;
  fns->add = lispenv_add; fns->drop = NULL; fns->test = NULL;

  fd_tablefns[fd_lexenv_type]=fns;
  fd_recyclers[fd_evalfn_type]=recycle_evalfn;

  fd_unparsers[fd_evalfn_type]=unparse_evalfn;
  fd_dtype_writers[fd_evalfn_type]=write_evalfn_dtype;

  fd_unparsers[fd_lexref_type]=unparse_lexref;
  fd_dtype_writers[fd_lexref_type]=write_lexref_dtype;
  fd_type_names[fd_lexref_type]=_("lexref");

  fd_unparsers[fd_coderef_type]=unparse_coderef;
  fd_dtype_writers[fd_coderef_type]=write_coderef_dtype;
  fd_type_names[fd_coderef_type]=_("coderef");

  quote_symbol = fd_intern("QUOTE");
  _fd_comment_symbol = comment_symbol = fd_intern("COMMENT");
  profile_symbol = fd_intern("%PROFILE");
  moduleid_symbol = fd_intern("%MODULEID");
  source_symbol = fd_intern("%SOURCE");

  FD_INIT_STATIC_CONS(&module_map,fd_hashtable_type);
  fd_make_hashtable(&module_map,67);

  FD_INIT_STATIC_CONS(&safe_module_map,fd_hashtable_type);
  fd_make_hashtable(&safe_module_map,67);
}

static void init_scheme_module()
{
  fd_xscheme_module = fd_make_hashtable(NULL,71);
  fd_scheme_module = fd_make_hashtable(NULL,71);
  fd_register_module("SCHEME",fd_scheme_module,
                     (FD_MODULE_DEFAULT|FD_MODULE_SAFE));
  fd_register_module("XSCHEME",fd_xscheme_module,(FD_MODULE_DEFAULT));
}

static void init_localfns()
{
  fd_def_evalfn(fd_scheme_module,"EVAL","",eval_evalfn);
  fd_def_evalfn(fd_scheme_module,"BOUND?","",boundp_evalfn);
  fd_def_evalfn(fd_scheme_module,"UNBOUND?","",unboundp_evalfn);
  fd_def_evalfn(fd_scheme_module,"DEFINED?","",definedp_evalfn);
  fd_def_evalfn(fd_scheme_module,"VOID?","",voidp_evalfn);
  fd_def_evalfn(fd_scheme_module,"DEFAULT?","",defaultp_evalfn);
  fd_def_evalfn(fd_scheme_module,"CONSTANT?","",constantp_evalfn);
  fd_def_evalfn(fd_scheme_module,"BAD?","",badp_evalfn);
  fd_def_evalfn(fd_scheme_module,"QUOTE","",quote_evalfn);
  fd_def_evalfn(fd_scheme_module,"%ENV","",env_evalfn);
  fd_def_evalfn(fd_scheme_module,"%MODREF","",modref_evalfn);

  fd_def_evalfn(fd_scheme_module,"%ENV/RESET!",
                "Resets the cached dynamic copy of the current "
                "environment (if any). This means that procedures "
                "closed in the current environment will not be "
                "effected by future changes",
                env_reset_evalfn);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1("DOCUMENTATION",get_documentation,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1("ENVIRONMENT?",environmentp_prim,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2("SYMBOL-BOUND?",symbol_boundp_prim,2));
  fd_idefn2(fd_scheme_module,"%LEXREF",lexref_prim,2,
            "(%LEXREF *up* *across*) returns a lexref (lexical reference) "
            "given a 'number of environments' *up* and a 'number of bindings' "
            "across",
            fd_fixnum_type,VOID,fd_fixnum_type,VOID);
  fd_idefn1(fd_scheme_module,"%LEXREFVAL",lexref_value_prim,1,
            "(%LEXREFVAL *lexref*) returns the offsets "
            "of a lexref as a pair (*up* . *across*)",
            fd_lexref_type,VOID);
  fd_idefn1(fd_scheme_module,"%LEXREF?",lexrefp_prim,1,
            "(%LEXREF? *val*) returns true if it's argument "
            "is a lexref (lexical reference)",
            -1,FD_VOID);
  fd_idefn0(fd_scheme_module,"%APPENV",appenv_prim,
            "Returns the base 'application environment' for the "
            "current instance");

  fd_idefn2(fd_scheme_module,"DUMP-BUG",dumpbug_prim,1,
            "(DUMP-BUG *err* [*to*]) writes a DType representation of *err* "
            "to either *to* or the configured BUGDIR. If *err* is #t, "
            "returns a packet of the representation. Without *to* or "
            "if *to* is #f or #default, writes the exception into either "
            "'./errors/' or './'",
            fd_exception_type,FD_VOID,-1,FD_VOID);

  fd_idefn1(fd_scheme_module,"%CODEREF",coderef_prim,1,
            "(%CODEREF *nelts*) returns a 'coderef' (a relative position) value",
            fd_fixnum_type,VOID);
  fd_idefn1(fd_scheme_module,"%CODREFVAL",coderef_value_prim,1,
            "(%CODEREFVAL *coderef*) returns the integer relative offset of a coderef",
            fd_lexref_type,VOID);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("%CHOICEREF",choiceref_prim,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("%FIXCHOICE",fixchoice_prim,1)));


  fd_def_evalfn(fd_scheme_module,"WITHENV","",withenv_safe_evalfn);
  fd_def_evalfn(fd_xscheme_module,"WITHENV","",withenv_evalfn);
  fd_def_evalfn(fd_xscheme_module,"WITHENV/SAFE","",withenv_safe_evalfn);


  fd_idefn3(fd_scheme_module,"GET-ARG",get_arg_prim,2,
            "`(GET-ARG *expression* *i* [*default*])` "
            "returns the *i*'th parameter in *expression*, "
            "or *default* (otherwise)",
            -1,FD_VOID,fd_fixnum_type,FD_VOID,-1,FD_VOID);
  fd_def_evalfn(fd_scheme_module,"GETOPT",
                "`(GETOPT *opts* *name* [*default*=#f])` returns any *name* "
                "option defined in *opts* or *default* otherwise. "
                "If *opts* or *name* are choices, this only returns *default* "
                "if none of the alternatives yield results.",
                 getopt_evalfn);
  fd_def_evalfn(fd_scheme_module,"TRYOPT",
                "`(TRYOPT *opts* *name* [*default*=#f])` returns any *name* "
                "option defined in *opts* or *default* otherwise. Any errors "
                "during option resolution are ignored. "
                "If *opts* or *name* are choices, this only returns *default* "
                "if none of the alternatives yield results. Note that the "
                "*default*, if evaluated, may signal an error.",
                tryopt_evalfn);
  fd_idefn3(fd_scheme_module,"%GETOPT",getopt_prim,FD_NEEDS_2_ARGS|FD_NDCALL,
            "`(%GETOPT *opts* *name* [*default*=#f])` gets any *name* option "
            "from opts, returning *default* if there isn't any. This is a real "
            "procedure (unlike `GETOPT`) so that *default* will be evaluated even "
            "if the option exists and is returned.",
            -1,VOID,fd_symbol_type,VOID,
            -1,FD_FALSE);
  fd_idefn3(fd_scheme_module,"TESTOPT",testopt_prim,2,
            "`(TESTOPT *opts* *name* [*value*])` returns true if "
            "the option *name* is specified in *opts* and it includes "
            "*value* (if provided).",
            -1,VOID,fd_symbol_type,VOID,
            -1,VOID);
  fd_idefn1(fd_scheme_module,"OPTS?",optionsp_prim,FD_NEEDS_1_ARG|FD_NDCALL,
            "`(OPTS? *opts*)` returns true if *opts* is a valid options "
            "object.",
            -1,VOID);
  fd_idefnN(fd_scheme_module,"OPTS+",opts_plus_prim,FD_NDCALL,
            "`(OPTS+ *add* *opts*)` or `(OPTS+ *optname* *value* *opts*) "
            "returns a new options object (a pair).");
  fd_defalias(fd_scheme_module,"OPT+","OPTS+");

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("APPLY",apply_lexpr,1)));
  fd_idefn(fd_xscheme_module,fd_make_cprim7x
           ("DTPROC",make_dtproc,2,
            fd_symbol_type,VOID,fd_string_type,VOID,
            -1,VOID,-1,VOID,
            fd_fixnum_type,FD_INT(2),
            fd_fixnum_type,FD_INT(4),
            fd_fixnum_type,FD_INT(1)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("CALL/CC",callcc,1));
  fd_defalias(fd_scheme_module,"CALL-WITH-CURRENT-CONTINUATION","CALL/CC");

  /* This pushes a log context */
  fd_def_evalfn(fd_scheme_module,"WITH-LOG-CONTEXT","",with_log_context_evalfn);

  /* This pushes a new threadcache */
  fd_def_evalfn(fd_scheme_module,"WITH-THREADCACHE","",with_threadcache_evalfn);
  /* This ensures that there's an active threadcache, pushing a new one if
     needed or using the current one if it exists. */
  fd_def_evalfn(fd_scheme_module,"USING-THREADCACHE","",using_threadcache_evalfn);
  /* This sets up the current thread to use a threadcache */
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("USE-THREADCACHE",use_threadcache_prim,0));

  fd_idefn(fd_scheme_module,fd_make_cprimn("TCACHECALL",tcachecall,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("CACHECALL",cachecall,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("CACHECALL/PROBE",cachecall_probe,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("CACHEDCALL?",cachedcallp,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("CLEAR-CALLCACHE!",clear_callcache,0));
  fd_defalias(fd_scheme_module,"CACHEPOINT","TCACHECALL");

  fd_def_evalfn(fd_scheme_module,"TIMEVAL","",timed_eval_evalfn);
  fd_def_evalfn(fd_scheme_module,"%TIMEVAL","",timed_evalx_evalfn);
  fd_def_evalfn(fd_scheme_module,"%WATCHPTR","",watchptr_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("%WATCHPTRVAL",watchptr_prim,1)));
  fd_def_evalfn(fd_scheme_module,"%WATCH","",watched_eval_evalfn);
  fd_def_evalfn(fd_scheme_module,"PROFILE","",profiled_eval_evalfn);
  fd_def_evalfn(fd_scheme_module,"%WATCHCALL","",watchcall_evalfn);
  fd_defalias(fd_scheme_module,"%WC","%WATCHCALL");
  fd_def_evalfn(fd_scheme_module,"%WATCHCALL+","",watchcall_plus_evalfn);
  fd_defalias(fd_scheme_module,"%WC+","%WATCHCALL+");
  fd_def_evalfn(fd_scheme_module,"EVAL1","",eval1);
  fd_def_evalfn(fd_scheme_module,"EVAL2","",eval2);
  fd_def_evalfn(fd_scheme_module,"EVAL3","",eval3);
  fd_def_evalfn(fd_scheme_module,"EVAL4","",eval4);
  fd_def_evalfn(fd_scheme_module,"EVAL5","",eval5);
  fd_def_evalfn(fd_scheme_module,"EVAL6","",eval6);
  fd_def_evalfn(fd_scheme_module,"EVAL7","",eval7);

#if USING_GOOGLE_PROFILER
  fd_def_evalfn(fd_scheme_module,"GOOGLE/PROFILE","",gprofile_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_cprim0("GOOGLE/PROFILE/STOP",gprofile_stop));
#endif
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("APPLYTEST",applytest,2)));
  fd_def_evalfn(fd_scheme_module,"EVALTEST","",evaltest_evalfn);

  fd_def_evalfn(fd_scheme_module,"DBG","",dbg_evalfn);
  fd_def_evalfn(fd_scheme_module,"VOID","",void_evalfn);
  fd_def_evalfn(fd_scheme_module,"BREAK","",break_evalfn);
  fd_def_evalfn(fd_scheme_module,"DEFAULT","",default_evalfn);

  fd_idefn(fd_scheme_module,fd_make_cprimn("CHECK-VERSION",check_version_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("REQUIRE-VERSION",require_version_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("DTEVAL",dteval,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("DTCALL",dtcall,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("OPEN-DTSERVER",open_dtserver,1,
                                            fd_string_type,VOID,
                                            fd_fixnum_type,VOID));
  fd_idefn1(fd_scheme_module,"DTSERVER?",dtserverp,FD_NEEDS_1_ARG,
            "Returns true if it's argument is a dtype server object",
            -1,VOID);
  fd_idefn1(fd_scheme_module,"DTSERVER-ID",
            dtserver_id,FD_NEEDS_1_ARG,
            "Returns the ID of a dtype server (the argument used to create it)",
            fd_dtserver_type,VOID);
  fd_idefn1(fd_scheme_module,"DTSERVER-ADDRESS",
            dtserver_address,FD_NEEDS_1_ARG,
            "Returns the address (host/port) of a dtype server",
            fd_dtserver_type,VOID);

  fd_idefn0(fd_scheme_module,"%BUILDINFO",fd_get_build_info,
            "Information about the build and startup environment");

  fd_idefn(fd_xscheme_module,fd_make_cprimn("FFI/PROC",ffi_proc,3));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("FFI/PROBE",ffi_probe,3));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("FFI/FOUND?",ffi_found_prim,1,
                           fd_string_type,VOID,
                           fd_string_type,VOID));

  fd_idefn1(fd_xscheme_module,"MTRACE",mtrace_prim,FD_NEEDS_0_ARGS,
            "Activates LIBC heap tracing to MALLOC_TRACE and "
            "returns true if it worked. Optional argument is a "
            "filename to set as MALLOC_TRACE",
            -1,VOID);
  fd_idefn0(fd_xscheme_module,"MUNTRACE",muntrace_prim,
            "Deactivates LIBC heap tracing, returns true if it did anything");

  /* for testing */
  fd_idefn9(fd_scheme_module,"LIST9",list9,0,"Returns a nine-element list",
            -1,FD_FALSE,-1,FD_FALSE,-1,FD_FALSE,
            -1,FD_FALSE,-1,FD_FALSE,-1,FD_FALSE,
            -1,FD_FALSE,-1,FD_FALSE, -1,FD_FALSE);

  fd_register_config
    ("GPROFILE","Set filename for the Google CPU profiler",
     fd_sconfig_get,fd_sconfig_set,&cpu_profilename);
  fd_register_config
    ("TAILCALL","Enable/disable tail recursion in the Scheme evaluator",
     fd_boolconfig_get,fd_boolconfig_set,&fd_optimize_tail_calls);

  fd_register_config
    ("LOGTESTS",
     "Whether to log failed tests, rather than treating them as errors",
     fd_lconfig_get,fd_lconfig_set,&log_testfail);
  fd_register_config
    ("FAILEDTESTS","All failed tests",
     fd_lconfig_get,config_failed_tests_set,&failed_tests);

  fd_register_config
    ("BUGDIR","Save exceptions to this directory",
     fd_sconfig_get,config_bugdir,&fd_bugdir);

}

FD_EXPORT void fd_init_errfns_c(void);
FD_EXPORT void fd_init_compoundfns_c(void);
FD_EXPORT void fd_init_threads_c(void);
FD_EXPORT void fd_init_conditionals_c(void);
FD_EXPORT void fd_init_iterators_c(void);
FD_EXPORT void fd_init_choicefns_c(void);
FD_EXPORT void fd_init_binders_c(void);
FD_EXPORT void fd_init_lambdas_c(void);
FD_EXPORT void fd_init_macros_c(void);
FD_EXPORT void fd_init_coreprims_c(void);
FD_EXPORT void fd_init_tableprims_c(void);
FD_EXPORT void fd_init_stringprims_c(void);
FD_EXPORT void fd_init_dbprims_c(void);
FD_EXPORT void fd_init_seqprims_c(void);
FD_EXPORT void fd_init_modules_c(void);
FD_EXPORT void fd_init_load_c(void);
FD_EXPORT void fd_init_logprims_c(void);
FD_EXPORT void fd_init_portprims_c(void);
FD_EXPORT void fd_init_streamprims_c(void);
FD_EXPORT void fd_init_timeprims_c(void);
FD_EXPORT void fd_init_sysprims_c(void);
FD_EXPORT void fd_init_arith_c(void);
FD_EXPORT void fd_init_side_effects_c(void);
FD_EXPORT void fd_init_reflection_c(void);
FD_EXPORT void fd_init_opcodes_c(void);
FD_EXPORT void fd_init_reqstate_c(void);
FD_EXPORT void fd_init_regex_c(void);
FD_EXPORT void fd_init_quasiquote_c(void);
FD_EXPORT void fd_init_struct_eval_c(void);
FD_EXPORT void fd_init_extdbprims_c(void);
FD_EXPORT void fd_init_history_c(void);

static void init_eval_core()
{
  init_localfns();
  fd_init_opcodes_c();
  fd_init_tableprims_c();
  fd_init_modules_c();
  fd_init_arith_c();
  fd_init_threads_c();
  fd_init_errfns_c();
  fd_init_load_c();
  fd_init_conditionals_c();
  fd_init_iterators_c();
  fd_init_choicefns_c();
  fd_init_binders_c();
  fd_init_lambdas_c();
  fd_init_macros_c();
  fd_init_compoundfns_c();
  fd_init_quasiquote_c();
  fd_init_struct_eval_c();
  fd_init_side_effects_c();
  fd_init_reflection_c();
  fd_init_reqstate_c();

  fd_init_regex_c();

  fd_init_history_c();

  fd_init_coreprims_c();
  fd_init_stringprims_c();
  fd_init_dbprims_c();
  fd_init_seqprims_c();
  fd_init_logprims_c();
  fd_init_portprims_c();
  fd_init_streamprims_c();
  fd_init_timeprims_c();
  fd_init_sysprims_c();
  fd_init_extdbprims_c();

  u8_threadcheck();

  fd_finish_module(fd_scheme_module);
  fd_finish_module(fd_xscheme_module);
}

FD_EXPORT int fd_load_scheme()
{
  return fd_init_scheme();
}

FD_EXPORT int fd_init_scheme()
{
  if (scheme_initialized) return scheme_initialized;
  else {
    scheme_initialized = 401*fd_init_storage()*u8_initialize();

    fd_init_eval_c();

    default_env = fd_make_env(fd_make_hashtable(NULL,0),fd_app_env);
    safe_default_env = fd_make_env(fd_make_hashtable(NULL,0),NULL);

    u8_register_source_file(FRAMERD_EVAL_H_INFO);
    u8_register_source_file(_FILEINFO);

    init_scheme_module();
    init_eval_core();

    u8_init_mutex(&app_cleanup_lock);

    /* This sets up the fd_atexit handler for recycling the
       application environment. Consequently, it needs to be called
       before setting up any fd_atexit handlers which might use the
       application environment. */
    setup_app_env();

    return scheme_initialized;}
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
