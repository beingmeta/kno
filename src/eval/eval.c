/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

#if HAVE_MTRACE && HAVE_MCHECK_H
#include <mcheck.h>
#endif

static volatile int scheme_initialized = 0;

u8_string fd_evalstack_type="eval";
u8_string fd_ndevalstack_type="ndeval";

int fd_optimize_tail_calls = 1;

fdtype fd_scheme_module, fd_xscheme_module;

fdtype _fd_comment_symbol;

static fdtype quote_symbol, comment_symbol, moduleid_symbol;

static fd_exception TestFailed=_("Test failed");
static fd_exception ExpiredThrow=_("Continuation is no longer valid");
static fd_exception DoubleThrow=_("Continuation used twice");
static fd_exception LostThrow=_("Lost invoked continuation");

static char *cpu_profilename = NULL;

fd_exception
  fd_SyntaxError=_("SCHEME expression syntax error"),
  fd_InvalidMacro=_("invalid macro transformer"),
  fd_UnboundIdentifier=_("the variable is unbound"),
  fd_VoidArgument=_("VOID result passed as argument"),
  fd_NotAnIdentifier=_("not an identifier"),
  fd_TooFewExpressions=_("too few subexpressions"),
  fd_CantBind=_("can't add binding to environment"),
  fd_ReadOnlyEnv=_("Read only environment");

/* Environment functions */

static int bound_in_envp(fdtype symbol,fd_lexenv env)
{
  fdtype bindings = env->env_bindings;
  if (FD_HASHTABLEP(bindings))
    return fd_hashtable_probe((fd_hashtable)bindings,symbol);
  else if (FD_SLOTMAPP(bindings))
    return fd_slotmap_test((fd_slotmap)bindings,symbol,FD_VOID);
  else if (FD_SCHEMAPP(bindings))
    return fd_schemap_test((fd_schemap)bindings,symbol,FD_VOID);
  else return fd_test(bindings,symbol,FD_VOID);
}

/* Lexrefs */

static fdtype lexref_prim(fdtype upv,fdtype acrossv)
{
  long long up = FD_FIX2INT(upv), across = FD_FIX2INT(acrossv);
  long long combined = ((up<<5)|(across));
  /* Not the exact range limits, but good enough */
  if ((up>=0)&&(across>=0)&&(up<256)&&(across<256))
    return FDTYPE_IMMEDIATE(fd_lexref_type,combined);
  else if ((up<0)||(up>256))
    return fd_type_error("short","lexref_prim",up);
  else return fd_type_error("short","lexref_prim",across);
}

static fdtype lexref_value_prim(fdtype lexref)
{
  int code = FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up = code/32, across = code%32;
  return fd_init_pair(NULL,FD_INT(up),FD_INT(across));
}

static int unparse_lexref(u8_output out,fdtype lexref)
{
  int code = FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up = code/32, across = code%32;
  u8_printf(out,"#<LEXREF ^%d+%d>",up,across);
  return 1;
}

static int dtype_lexref(struct FD_OUTBUF *out,fdtype x)
{
  int code = FD_GET_IMMEDIATE(x,fd_lexref_type);
  int up = code/32, across = code%32;
  unsigned char buf[100], *tagname="%LEXREF";
  struct FD_OUTBUF tmp;
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
  size_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

/* Code refs */

static fdtype coderef_prim(fdtype offset)
{
  long long off = FD_FIX2INT(offset);
  return FDTYPE_IMMEDIATE(fd_coderef_type,off);
}

static fdtype coderef_value_prim(fdtype offset)
{
  long long off = FD_GET_IMMEDIATE(offset,fd_coderef_type);
  return FD_INT(off);
}

static int unparse_coderef(u8_output out,fdtype coderef)
{
  long long off = FD_GET_IMMEDIATE(coderef,fd_coderef_type);
  u8_printf(out,"#<CODEREF %lld>",off);
  return 1;
}

static int dtype_coderef(struct FD_OUTBUF *out,fdtype x)
{
  int offset = FD_GET_IMMEDIATE(x,fd_coderef_type);
  unsigned char buf[100], *tagname="%CODEREF";
  struct FD_OUTBUF tmp;
  FD_INIT_OUTBUF(&tmp,buf,100,0);
  fd_write_byte(&tmp,dt_compound);
  fd_write_byte(&tmp,dt_symbol);
  fd_write_4bytes(&tmp,strlen(tagname));
  fd_write_bytes(&tmp,tagname,strlen(tagname));
  fd_write_byte(&tmp,dt_fixnum);
  fd_write_4bytes(&tmp,offset);
  size_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

/* Symbol lookup */

FD_EXPORT fdtype _fd_symeval(fdtype sym,fd_lexenv env)
{
  return fd_symeval(sym,env);
}

/* Assignments */

static int add_to_value(fdtype sym,fdtype val,fd_lexenv env)
{
  int rv=1;
  if (env) {
    fdtype bindings=env->env_bindings;
    fdtype exports=env->env_exports;
    if ((fd_add(bindings,sym,val))>=0) {
      if (FD_HASHTABLEP(exports)) {
        fdtype newval=fd_get(bindings,sym,FD_EMPTY_CHOICE);
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

FD_EXPORT int fd_bind_value(fdtype sym,fdtype val,fd_lexenv env)
{
  /* TODO: Check for checking the return value of calls to
     `fd_bind_value` */
  if (env) {
    if (fd_store(env->env_bindings,sym,val)<0) {
      fd_poperr(NULL,NULL,NULL,NULL);
      fd_seterr(fd_CantBind,"fd_bind_value",NULL,sym);
      return -1;}
    if (FD_HASHTABLEP(env->env_exports))
      fd_hashtable_op((fd_hashtable)(env->env_exports),
                      fd_table_replace,sym,val);
    return 1;}
  else return 0;
}

FD_EXPORT int fd_add_value(fdtype symbol,fdtype value,fd_lexenv env)
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

FD_EXPORT int fd_assign_value(fdtype symbol,fdtype value,fd_lexenv env)
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
      if (FD_HASHTABLEP(env->env_exports))
        fd_hashtable_op((fd_hashtable)(env->env_exports),
                        fd_table_replace,symbol,value);
      return 1;}}
  return 0;
}

/* Unpacking expressions, non-inline versions */

FD_EXPORT fdtype _fd_get_arg(fdtype expr,int i)
{
  return fd_get_arg(expr,i);
}
FD_EXPORT fdtype _fd_get_body(fdtype expr,int i)
{
  return fd_get_body(expr,i);
}

static fdtype getopt_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype opts = fd_eval(fd_get_arg(expr,1),env);
  if (FD_ABORTED(opts)) return opts;
  else {
    fdtype keys = fd_eval(fd_get_arg(expr,2),env);
    if (FD_ABORTED(keys)) {
      fd_decref(opts); return keys;}
    else {
      fdtype results = FD_EMPTY_CHOICE;
      FD_DO_CHOICES(opt,opts) {
        FD_DO_CHOICES(key,keys) {
          fdtype v = fd_getopt(opt,key,FD_VOID);
          if (FD_ABORTED(v)) {
            fd_decref(results); results = v;
            FD_STOP_DO_CHOICES;}
          else if (!(FD_VOIDP(v))) {FD_ADD_TO_CHOICE(results,v);}}
        if (FD_ABORTED(results)) {FD_STOP_DO_CHOICES;}}
      fd_decref(keys); fd_decref(opts);
      if (FD_ABORTED(results)) {
        return results;}
      else if (FD_EMPTY_CHOICEP(results)) {
        fdtype dflt_expr = fd_get_arg(expr,3);
        if (FD_VOIDP(dflt_expr)) return FD_FALSE;
        else return fd_eval(dflt_expr,env);}
      else return results;}}
}
static fdtype getopt_prim(fdtype opts,fdtype keys,fdtype dflt)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(opt,opts) {
    FD_DO_CHOICES(key,keys) {
      fdtype v = fd_getopt(opt,key,FD_VOID);
      if (!(FD_VOIDP(v))) {FD_ADD_TO_CHOICE(results,v);}}}
  if (FD_EMPTY_CHOICEP(results)) {
    fd_incref(dflt); return dflt;}
  else return results;
}
static fdtype testopt_prim(fdtype opts,fdtype key,fdtype val)
{
  if (fd_testopt(opts,key,val)) return FD_TRUE;
  else return FD_FALSE;
}
static fdtype optplus_prim(fdtype opts,fdtype key,fdtype val)
{
  if (FD_VOIDP(val))
    return fd_conspair(key,fd_incref(opts));
  else return fd_conspair(fd_conspair(key,fd_incref(val)),fd_incref(opts));
}

/* Quote */

static fdtype quote_evalfn(fdtype obj,fd_lexenv env,fd_stack stake)
{
  if ((FD_PAIRP(obj)) && (FD_PAIRP(FD_CDR(obj))) &&
      ((FD_CDR(FD_CDR(obj))) == FD_EMPTY_LIST))
    return fd_incref(FD_CAR(FD_CDR(obj)));
  else return fd_err(fd_SyntaxError,"QUOTE",NULL,obj);
}

/* Profiling functions */

static fdtype profile_symbol;

static fdtype profiled_eval_evalfn(fdtype expr,fd_lexenv env,fd_stack stack)
{
  fdtype toeval = fd_get_arg(expr,1);
  double start = u8_elapsed_time();
  fdtype value = fd_eval(toeval,env);
  double finish = u8_elapsed_time();
  fdtype tag = fd_get_arg(expr,2);
  fdtype profile_info = fd_symeval(profile_symbol,env), profile_data;
  if (FD_VOIDP(profile_info)) return value;
  profile_data = fd_get(profile_info,tag,FD_VOID);
  if (FD_ABORTED(profile_data)) {
    fd_decref(value); fd_decref(profile_info);
    return profile_data;}
  else if (FD_VOIDP(profile_data)) {
    fdtype time = fd_init_double(NULL,(finish-start));
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

/* These are for wrapping around Scheme code to see in C profilers */
static fdtype eval1(fdtype expr,fd_lexenv env,fd_stack s) { return fd_stack_eval(expr,env,s,0);}
static fdtype eval2(fdtype expr,fd_lexenv env,fd_stack s) { return fd_stack_eval(expr,env,s,0);}
static fdtype eval3(fdtype expr,fd_lexenv env,fd_stack s) { return fd_stack_eval(expr,env,s,0);}
static fdtype eval4(fdtype expr,fd_lexenv env,fd_stack s) { return fd_stack_eval(expr,env,s,0);}
static fdtype eval5(fdtype expr,fd_lexenv env,fd_stack s) { return fd_stack_eval(expr,env,s,0);}
static fdtype eval6(fdtype expr,fd_lexenv env,fd_stack s) { return fd_stack_eval(expr,env,s,0);}
static fdtype eval7(fdtype expr,fd_lexenv env,fd_stack s) { return fd_stack_eval(expr,env,s,0);}

/* Google profiler usage */

#if USING_GOOGLE_PROFILER
static fdtype gprofile_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  int start = 1; char *filename = NULL;
  if ((FD_PAIRP(FD_CDR(expr)))&&(FD_STRINGP(FD_CAR(FD_CDR(expr))))) {
    filename = u8_strdup(FD_STRDATA(FD_CAR(FD_CDR(expr))));
    start = 2;}
  else if ((FD_PAIRP(FD_CDR(expr)))&&(FD_SYMBOLP(FD_CAR(FD_CDR(expr))))) {
    fdtype val = fd_symeval(FD_CADR(expr),env);
    if (FD_STRINGP(val)) {
      filename = u8_strdup(FD_STRDATA(val));
      fd_decref(val);
      start = 2;}
    else return fd_type_error("filename","GOOGLE/PROFILE",val);}
  else if (cpu_profilename)
    filename = u8_strdup(cpu_profilename);
  else if (getenv("CPUPROFILE"))
    filename = u8_strdup(getenv("CPUPROFILE"));
  else filename = u8_mkstring("/tmp/gprof%ld.pid",(long)getpid());
  ProfilerStart(filename); {
    fdtype fd_value = FD_VOID;
    fdtype body = fd_get_body(expr,start);
    FD_DOLIST(ex,body) {
      fd_decref(fd_value); fd_value = fd_eval(ex,env);
      if (FD_ABORTED(fd_value)) {
        ProfilerStop();
        return fd_value;}
      else {}}
    ProfilerStop();
    u8_free(filename);
    return fd_value;}
}

static fdtype gprofile_stop()
{
  ProfilerStop();
  return FD_VOID;
}
#endif


/* Trace functions */

static fdtype timed_eval_evalfn(fdtype expr,fd_lexenv env,fd_stack stack)
{
  fdtype toeval = fd_get_arg(expr,1);
  double start = u8_elapsed_time();
  fdtype value = fd_eval(toeval,env);
  double finish = u8_elapsed_time();
  u8_message(";; Executed %q in %f seconds, yielding\n;;\t%q",
             toeval,(finish-start),value);
  return value;
}

static fdtype timed_evalx_evalfn(fdtype expr,fd_lexenv env,fd_stack stack)
{
  fdtype toeval = fd_get_arg(expr,1);
  double start = u8_elapsed_time();
  fdtype value = fd_eval(toeval,env);
  double finish = u8_elapsed_time();
  return fd_make_nvector(2,value,fd_init_double(NULL,finish-start));
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
    /* The line is too long, insert a \n\t at off */
    if ((end+5)>(out->u8_outlim)) {
      /* Grow the stream if needed */
      u8_grow_stream((u8_stream)out,U8_BUF_MIN_GROW);
      start = out->u8_outbuf; end = out->u8_write;
      scanner = start+scan_off;}
    /* Use memmove because it's overlapping */
    memmove(start+off+2,start+off,len-off);
    start[off]='\n'; start[off+1]='\t';
    out->u8_write = out->u8_write+2;
    start[len+2]='\0';
    return -1;}
}

static fdtype watchcall(fdtype expr,fd_lexenv env,int with_proc)
{
  struct U8_OUTPUT out;
  u8_string dflt_label="%CALL", label = dflt_label, arglabel="%ARG";
  fdtype watch, head = fd_get_arg(expr,1), *rail, result = FD_EMPTY_CHOICE;
  int i = 0, n_args, expr_len = fd_seq_length(expr);
  if (FD_VOIDP(head))
    return fd_err(fd_SyntaxError,"%WATCHCALL",NULL,expr);
  else if (FD_STRINGP(head)) {
    if (expr_len==2)
      return fd_err(fd_SyntaxError,"%WATCHCALL",NULL,expr);
    else {
      label = u8_strdup(FD_STRDATA(head));
      arglabel = u8_mkstring("%s/ARG",FD_STRDATA(head));
      if ((expr_len>3)||(FD_APPLICABLEP(fd_get_arg(expr,2)))) {
        watch = fd_get_body(expr,2);}
      else watch = fd_get_arg(expr,2);}}
  else if ((expr_len==2)&&(FD_PAIRP(head)))
    watch = head;
  else watch = fd_get_body(expr,1);
  n_args = fd_seq_length(watch);
  rail = u8_alloc_n(n_args,fdtype);
  U8_INIT_OUTPUT(&out,1024);
  u8_printf(&out,"Watched call %q",watch);
  u8_logger(-10,label,out.u8_outbuf);
  out.u8_write = out.u8_outbuf;
  while (i<n_args) {
    fdtype arg = fd_get_arg(watch,i);
    fdtype val = fd_eval(arg,env);
    val = fd_simplify_choice(val);
    if (FD_ABORTED(val)) {
      u8_string errstring = fd_errstring(NULL);
      i--; while (i>=0) {fd_decref(rail[i]); i--;}
      u8_free(rail);
      u8_printf(&out,"\t%q !!!> %s",arg,errstring);
      u8_logger(-10,arglabel,out.u8_outbuf);
      if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
      u8_free(out.u8_outbuf); u8_free(errstring);
      return val;}
    if ((i==0)&&(with_proc==0)&&(FD_SYMBOLP(arg))) {}
    else if ((FD_PAIRP(arg))||(FD_SYMBOLP(arg))) {
      u8_printf(&out,"%q ==> %q",arg,val);
      u8_logger(-10,arglabel,out.u8_outbuf);
      out.u8_write = out.u8_outbuf;}
    else {
      u8_printf(&out,"%q",arg);
      u8_logger(-10,arglabel,out.u8_outbuf);
      out.u8_write = out.u8_outbuf;}
    rail[i++]=val;}
  if (FD_CHOICEP(rail[0])) {
    FD_DO_CHOICES(fn,rail[0]) {
      fdtype r = fd_apply(fn,n_args-1,rail+1);
      if (FD_ABORTED(r)) {
        u8_string errstring = fd_errstring(NULL);
        i--; while (i>=0) {fd_decref(rail[i]); i--;}
        u8_free(rail);
        fd_decref(result);
        if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
        u8_free(out.u8_outbuf); u8_free(errstring);
        return r;}
      else {FD_ADD_TO_CHOICE(result,r);}}}
  else result = fd_apply(rail[0],n_args-1,rail+1);
  if (FD_ABORTED(result)) {
    u8_string errstring = fd_errstring(NULL);
    u8_printf(&out,"%q !!!> %s",watch,errstring);
    u8_free(errstring);}
  else u8_printf(&out,"%q ===> %q",watch,result);
  u8_logger(-10,label,out.u8_outbuf);
  i--; while (i>=0) {fd_decref(rail[i]); i--;}
  u8_free(rail);
  if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
  u8_free(out.u8_outbuf);
  return result;
}

static fdtype watchcall_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  return watchcall(expr,env,0);
}
static fdtype watchcall_plus_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  return watchcall(expr,env,1);
}

static u8_string get_label(fdtype arg,u8_byte *buf,size_t buflen)
{
  if (FD_SYMBOLP(arg))
    return FD_SYMBOL_NAME(arg);
  else if (FD_STRINGP(arg))
    return FD_STRDATA(arg);
  else if (FD_FIXNUMP(arg))
    return u8_write_long_long((FD_FIX2INT(arg)),buf,buflen);
  else if ((FD_BIGINTP(arg))&&(fd_modest_bigintp((fd_bigint)arg)))
    return u8_write_long_long
      (fd_bigint2int64((fd_bigint)arg),buf,buflen);
  else return NULL;
}

static fdtype watchptr_prim(fdtype val,fdtype label_arg)
{
  u8_byte buf[64];
  u8_string label = get_label(label_arg,buf,64);
  if (FD_IMMEDIATEP(val)) {
    unsigned long long itype = FD_IMMEDIATE_TYPE(val);
    unsigned long long data = FD_IMMEDIATE_DATA(val);
    u8_string type_name = fd_type2name(itype);;
    u8_log(-10,"Immediate pointer",
           "%s%s%s0x%llx [ T0x%llx(%s) data=%llu ] == %q",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),itype,type_name,data,val);}
  else if (FD_FIXNUMP(val))
    u8_log(-10,"%s%s%sFixnum","0x%llx == %d",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),FD_FIX2INT(val));
  else if (FD_OIDP(val)) {
    FD_OID addr = FD_OID_ADDR(val);
    u8_log(-10,"OID",
           "%s%s%s0x%llx [ base=%llx off=%llx ] == %llx/%llx",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           (FD_OID_BASE_ID(val)),(FD_OID_BASE_OFFSET(val)),
           FD_OID_HI(addr),FD_OID_LO(addr));}
  else if (FD_STATICP(val)) {
    fd_ptr_type ptype = FD_CONS_TYPE((fd_cons)val);
    u8_string type_name = fd_ptr_typename(ptype);
    u8_log(-10,"Static pointer",
           "%s%s%s0x%llx [ T0x%llx(%s) ] == %q",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           ptype,type_name,val);}
  else if (FD_CONSP(val)) {
    fd_cons c = (fd_cons) val;
    fd_ptr_type ptype = FD_CONS_TYPE(c);
    u8_string type_name = fd_ptr_typename(ptype);
    unsigned int refcount = FD_CONS_REFCOUNT(c);
    u8_log(-10,"Consed pointer",
           "%s%s%s0x%llx [ T0x%llx(%s) refs=%d ] == %q",
           U8OPTSTR("",label,": "),
           ((unsigned long long)val),
           ptype,type_name,refcount,val);}
  else {}
  return fd_incref(val);
}

static fdtype watched_eval_evalfn(fdtype expr,fd_lexenv env,fd_stack stack)
{
  fdtype toeval = fd_get_arg(expr,1);
  double start; int oneout = 0;
  fdtype scan = FD_CDR(expr);
  u8_string label="%WATCH";
  if ((FD_PAIRP(toeval))) {
    /* EXPR "label" . watchexprs */
    scan = FD_CDR(scan);
    if ((FD_PAIRP(scan)) && (FD_STRINGP(FD_CAR(scan)))) {
      label = FD_STRDATA(FD_CAR(scan)); scan = FD_CDR(scan);}}
  else if (FD_STRINGP(toeval)) {
    /* "label" . watchexprs, no expr, call should be side-effect */
    label = FD_STRDATA(toeval); scan = FD_CDR(scan);}
  else if (FD_SYMBOLP(toeval)) {
    /* If the first argument is a symbol, we change the label and
       treat all the arguments as context variables and output
       them.  */
    label="%WATCHED";}
  else scan = FD_CDR(scan);
  if (FD_PAIRP(scan)) {
    int off = 0; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    if (FD_PAIRP(toeval)) {
      u8_printf(&out,"Context for %q: ",toeval);
      off = check_line_length(&out,off,50);}
    /* A watched expr can be just a symbol or pair, which is output as:
         <expr>=<value>
       or a series of "<label>" <expr>, which is output as:
         label=<value> */
    while (FD_PAIRP(scan)) {
      /* A watched expr can be just a symbol or pair, which is output as:
           <expr>=<value>
         or a series of "<label>" <expr>, which is output as:
           label=<value> */
      fdtype towatch = FD_CAR(scan), wval = FD_VOID;
      if ((FD_STRINGP(towatch)) && (FD_PAIRP(FD_CDR(scan)))) {
        fdtype label = towatch; u8_string lbl = FD_STRDATA(label);
        towatch = FD_CAR(FD_CDR(scan)); scan = FD_CDR(FD_CDR(scan));
        wval = ((FD_SYMBOLP(towatch))?(fd_symeval(towatch,env)):
              (fd_eval(towatch,env)));
        if (lbl[0]=='\n') {
          if (oneout) {
            if (off>0) u8_printf(&out,"\n  // %s=",lbl+1);
            else u8_printf(&out," // %s=",lbl+1);}
          else oneout = 1;
          fd_pprint(&out,wval,"   ",0,3,100,0);
          if (FD_PAIRP(scan)) {
            u8_puts(&out,"\n"); off = 0;}}
        else {
          if (oneout) u8_puts(&out," // "); else oneout = 1;
          u8_printf(&out,"%s=%q",FD_STRDATA(label),wval);
          off = check_line_length(&out,off,100);}
        fd_decref(wval); wval = FD_VOID;}
      else {
        wval = ((FD_SYMBOLP(towatch))?(fd_symeval(towatch,env)):
              (fd_eval(towatch,env)));
        scan = FD_CDR(scan);
        if (oneout) u8_printf(&out," // %q=%q",towatch,wval);
        else {
          u8_printf(&out,"%q=%q",towatch,wval);
          oneout = 1;}
        off = check_line_length(&out,off,50);
        fd_decref(wval); wval = FD_VOID;}}
    u8_logger(-10,label,out.u8_outbuf);
    u8_free(out.u8_outbuf);}
  start = u8_elapsed_time();
  if (FD_SYMBOLP(toeval))
    return fd_eval(toeval,env);
  else if (FD_STRINGP(toeval)) {
    /* This is probably the 'side effect' form of %WATCH, so
       we don't bother 'timing' it. */
    return fd_incref(toeval);}
  else {
    fdtype value = fd_eval(toeval,env);
    double howlong = u8_elapsed_time()-start;
    if (howlong>1.0)
      u8_log(-10,label,"<%.3fs> %q => %q",howlong*1000,toeval,value);
    else if (howlong>0.001)
      u8_log(-10,label,"<%.3fms> %q => %q",howlong*1000,toeval,value);
    else if (remainder(howlong,0.0000001)>=0.001)
      u8_log(-10,label,"<%.3fus> %q => %q",howlong*1000000,toeval,value);
    else u8_log(-10,label,"<%.1fus> %q => %q",howlong*1000000,
                toeval,value);
    return value;}
}

/* The evaluator itself */

static fdtype call_function(u8_string fname,fdtype f,
                            fdtype expr,fd_lexenv env,
                            fd_stack s,
                            int tail);
static int applicable_choicep(fdtype choice);

FD_EXPORT
fdtype fd_stack_eval(fdtype expr,fd_lexenv env,
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
    fdtype val = fd_symeval(expr,env);
    if (FD_EXPECT_FALSE(FD_VOIDP(val)))
      return fd_err(fd_UnboundIdentifier,"fd_eval",
                    FD_SYMBOL_NAME(expr),expr);
    else return val;}
  case fd_pair_type: {
    fdtype head = (FD_CAR(expr));
    if (FD_OPCODEP(head))
      return fd_opcode_dispatch(head,expr,env,_stack,tail);
    else if (head == quote_symbol)
      return fd_refcar(FD_CDR(expr));
    else if (head == comment_symbol)
      return FD_VOID;
    else if (!(fd_stackcheck())) {
      u8_byte buf[128]=""; struct U8_OUTPUT out;
      U8_INIT_FIXED_OUTPUT(&out,128,buf);
      u8_printf(&out,"%lld > %lld",u8_stack_depth(),fd_stack_limit);
      return fd_err(fd_StackOverflow,"fd_tail_eval",buf,expr);}
    else {
      u8_string label=(FD_SYMBOLP(head)) ? (FD_SYMBOL_NAME(head)) : (NULL);
      FD_PUSH_STACK(eval_stack,fd_evalstack_type,label,expr);
      fdtype result = FD_VOID, headval = FD_VOID;
      int gc_head=0;
      if (FD_FCNIDP(head)) {
        headval=fd_fcnid_ref(head);
        if (FD_PRECHOICEP(headval)) {
          headval=fd_make_simple_choice(headval);
          gc_head=1;}}
      else if ( (FD_SYMBOLP(head)) || (FD_PAIRP(head)) ||
                (FD_CODEP(head)) || (FD_CHOICEP(head)) ) {
        headval=stack_eval(head,env,eval_stack);
        if (FD_PRECHOICEP(headval)) headval=fd_simplify_choice(headval);
        gc_head=1;}
      else headval=head;
      int headtype = FD_PTR_TYPE(headval);
      if (gc_head) fd_push_cleanup(eval_stack,FD_DECREF,headval,NULL);
      switch (headtype) {
      case fd_cprim_type: case fd_sproc_type: {
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
        fdtype xformer = macrofn->macro_transformer;
        fdtype new_expr = fd_call(eval_stack,xformer,1,&expr);
        if (FD_ABORTED(new_expr))
          result = fd_err(fd_SyntaxError,
                          _("macro expansion"),NULL,new_expr);
        else result = fd_stack_eval(new_expr,env,eval_stack,tail);
        fd_decref(new_expr);
        break;}
      case fd_choice_type: {
        int applicable = applicable_choicep(headval);
        eval_stack->stack_type="ndhandler";
        if (applicable)
          result=call_function("fnchoice",headval,expr,env,eval_stack,tail);
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
        else if (FD_VOIDP(headval)) {
          result=fd_err(fd_UnboundIdentifier,"for function",
                        ((FD_SYMBOLP(head))?(FD_SYMBOL_NAME(head)):
                         (NULL)),
                        head);}
        else if (FD_EMPTY_CHOICEP(headval) )
          result=FD_EMPTY_CHOICE;
        else result=fd_err(fd_NotAFunction,NULL,NULL,headval);}
      if (!tail) {
        if (FD_TAILCALLP(result)) result=fd_finish_call(result);
        else {}}
      fd_pop_stack(eval_stack);
      return result;}}
  case fd_slotmap_type:
    return fd_deep_copy(expr);
  case fd_choice_type: {
    fdtype result = FD_EMPTY_CHOICE;
    FD_PUSH_STACK(eval_stack,fd_ndevalstack_type,NULL,expr);
    FD_DO_CHOICES(each_expr,expr) {
      fdtype r = stack_eval(each_expr,env,eval_stack);
      if (FD_ABORTED(r)) {
        FD_STOP_DO_CHOICES;
        fd_decref(result);
        fd_pop_stack(eval_stack);
        return r;}
      else {FD_ADD_TO_CHOICE(result,r);}}
    fd_pop_stack(eval_stack);
    return result;}
  case fd_prechoice_type: {
    fdtype exprs = fd_make_simple_choice(expr);
    FD_PUSH_STACK(eval_stack,fd_evalstack_type,NULL,expr);
    if (FD_CHOICEP(exprs)) {
      fdtype results = FD_EMPTY_CHOICE;
      FD_DO_CHOICES(expr,exprs) {
        fdtype result = stack_eval(expr,env,eval_stack);
        if (FD_ABORTP(result)) {
          FD_STOP_DO_CHOICES;
          fd_decref(results);
          return result;}
        else {FD_ADD_TO_CHOICE(results,result);}}
      fd_decref(exprs);
      fd_pop_stack(eval_stack);
      return results;}
    else {
      fdtype result = fd_stack_eval(exprs,env,eval_stack,tail);
      fd_decref(exprs);
      fd_pop_stack(eval_stack);
      return result;}}
  default:
    return fd_incref(expr);}
}

static int applicable_choicep(fdtype headvals)
{
  FD_DO_CHOICES(hv,headvals) {
    int hvtype = FD_PRIM_TYPE(hv);
    /* Check that all the elements are either applicable or special
       forms and not mixed */
    if ( (hvtype == fd_cprim_type) ||
         (hvtype == fd_sproc_type) ||
         (fd_applyfns[hvtype]) ) {}
    else if ((hvtype == fd_evalfn_type) ||
             (hvtype == fd_macro_type))
      return 0;
    /* In this case, all the headvals so far are evalfns */
    else return 0;}
  return 1;
}

FD_EXPORT fdtype _fd_eval(fdtype expr,fd_lexenv env)
{
  fdtype result = fd_tail_eval(expr,env);
  return fd_finish_call(result);
}

static int count_args(fdtype args)
{
  int n_args = 0; FD_DOLIST(arg,args) {
    if (!((FD_PAIRP(arg)) &&
          (FD_EQ(FD_CAR(arg),comment_symbol))))
      n_args++;}
  return n_args;
}

static fdtype process_arg(fdtype arg,fd_lexenv env,
                          struct FD_STACK *_stack)
{
  fdtype argval = fast_eval(arg,env);
  if (FD_EXPECT_FALSE(FD_VOIDP(argval)))
    return fd_err(fd_VoidArgument,"call_function/process_arg",NULL,arg);
  else if (FD_EXPECT_FALSE(FD_ABORTED(argval)))
    return argval;
  else if ((FD_CONSP(argval))&&(FD_PRECHOICEP(argval)))
    return fd_simplify_choice(argval);
  else return argval;
}

#define ND_ARGP(v) ((FD_CHOICEP(v))||(FD_QCHOICEP(v)))

FD_FASTOP int commentp(fdtype arg)
{
  return
    (FD_EXPECT_FALSE
     ((FD_PAIRP(arg)) &&
      (FD_EQ(FD_CAR(arg),comment_symbol))));
}

static fdtype call_function(u8_string fname,fdtype fn,
                            fdtype expr,fd_lexenv env,
                            struct FD_STACK *stack,
                            int tail)
{
  fdtype arg_exprs = fd_get_body(expr,1), result=FD_VOID;
  int n_args = count_args(arg_exprs), arg_count = 0;
  int gc_args = 0, nd_args = 0, d_prim = 0, argbuf_len=0;
  fdtype argbuf[n_args]; /* *argv=fd_alloca(argv_length); */
  if (FD_FUNCTIONP(fn)) {
    struct FD_FUNCTION *fcn=(fd_function)fn;
    int max_arity = fcn->fcn_arity, min_arity = fcn->fcn_min_arity;
    if (max_arity<0) {}
    else if (FD_EXPECT_FALSE(n_args>max_arity))
      return fd_err(fd_TooManyArgs,"call_function",fcn->fcn_name,expr);
    else if (FD_EXPECT_FALSE((min_arity>=0) && (n_args<min_arity)))
      return fd_err(fd_TooFewArgs,"call_function",fcn->fcn_name,expr);
    else {}
    d_prim=(fcn->fcn_ndcall==0);}
  int i=0; while (i<argbuf_len) argbuf[i++]=FD_VOID;
  /* Now we evaluate each of the subexpressions to fill the arg
     vector */
  {FD_DOLIST(elt,arg_exprs) {
      if (commentp(elt)) continue;
      fdtype argval = process_arg(elt,env,stack);
      if ( (FD_ABORTED(argval)) ||
           ( (d_prim) && (FD_EMPTY_CHOICEP(argval)) ) ) {
        /* Clean up the arguments we've already evaluated */
        if (gc_args) fd_decref_vec(argbuf,arg_count,0);
        return argval;}
      else if (FD_CONSP(argval)) {
        if ( (nd_args == 0) && (ND_ARGP(argval)) ) nd_args = 1;
        gc_args = 1;}
      else {}
      argbuf[arg_count++]=argval;}}
  if ((tail) && (fd_optimize_tail_calls) && (FD_SPROCP(fn)))
    result=fd_tail_call(fn,arg_count,argbuf);
  else if ((FD_CHOICEP(fn)) ||
           (FD_PRECHOICEP(fn)) ||
           ((d_prim) && (nd_args)))
    result=fd_ndcall(stack,fn,arg_count,argbuf);
  else result=fd_dcall(stack,fn,arg_count,argbuf);
  if (gc_args) fd_decref_vec(argbuf,arg_count,0);
  return result;
}

FD_EXPORT fdtype fd_eval_exprs(fdtype exprs,fd_lexenv env)
{
  if (FD_PAIRP(exprs)) {
    fdtype next = FD_CDR(exprs), val = FD_VOID;
    while (FD_PAIRP(exprs)) {
      fd_decref(val); val = FD_VOID;
      if (FD_EMPTY_LISTP(next))
        return fd_tail_eval(FD_CAR(exprs),env);
      else {
        val = fd_eval(FD_CAR(exprs),env);
        if (FD_ABORTED(val)) return val;
        else exprs = next;
        if (FD_PAIRP(exprs)) next = FD_CDR(exprs);}}
    return val;}
  else if (FD_CODEP(exprs)) {
    struct FD_VECTOR *v = fd_consptr(fd_vector,exprs,fd_code_type);
    int len = v->fdvec_length; fdtype *elts = v->fdvec_elts, val = FD_VOID;
    int i = 0; while (i<len) {
      fdtype expr = elts[i++];
      fd_decref(val); val = FD_VOID;
      if (i == len)
        return fd_eval(expr,env);
      else val = fd_eval(expr,env);
      if (FD_ABORTED(val)) return val;}
    return val;}
  else return FD_VOID;
}

/* Module system */

static struct FD_HASHTABLE module_map, safe_module_map;
static fd_lexenv default_env = NULL, safe_default_env = NULL;

FD_EXPORT fd_lexenv fd_make_env(fdtype bindings,fd_lexenv parent)
{
  if (FD_EXPECT_FALSE(!(FD_TABLEP(bindings)) )) {
    fd_seterr(fd_TypeError,"fd_make_env",
              u8_mkstring(_("object is not a %m"),"table"),
              bindings);
    return NULL;}
  else {
    struct FD_LEXENV *e = u8_alloc(struct FD_LEXENV);
    FD_INIT_FRESH_CONS(e,fd_lexenv_type);
    e->env_bindings = bindings; e->env_exports = FD_VOID;
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
fd_lexenv fd_make_export_env(fdtype exports,fd_lexenv parent)
{
  if (FD_EXPECT_FALSE(!(FD_HASHTABLEP(exports)) )) {
    fd_seterr(fd_TypeError,"fd_make_env",
              u8_mkstring(_("object is not a %m"),"hashtable"),
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

FD_EXPORT fd_lexenv fd_new_lexenv(fdtype bindings,int safe)
{
  if (scheme_initialized==0) fd_init_scheme();
  if (FD_VOIDP(bindings))
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

FD_EXPORT fdtype fd_register_module_x(fdtype name,fdtype module,int flags)
{
  if (flags&FD_MODULE_SAFE)
    fd_hashtable_store(&safe_module_map,name,module);
  else fd_hashtable_store(&module_map,name,module);

  /* Set the module ID*/
  if (FD_LEXENVP(module)) {
    fd_lexenv env = (fd_lexenv)module;
    fd_add(env->env_bindings,moduleid_symbol,name);}
  else if (FD_HASHTABLEP(module))
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

FD_EXPORT fdtype fd_register_module(u8_string name,fdtype module,int flags)
{
  return fd_register_module_x(fd_intern(name),module,flags);
}

FD_EXPORT fdtype fd_new_module(char *name,int flags)
{
  fdtype module_name, module, as_stored;
  if (scheme_initialized==0) fd_init_scheme();
  module_name = fd_intern(name);
  module = fd_make_hashtable(NULL,0);
  fd_add(module,moduleid_symbol,module_name);
  if (flags&FD_MODULE_SAFE) {
    fd_hashtable_op
      (&safe_module_map,fd_table_default,module_name,module);
    as_stored = fd_get((fdtype)&safe_module_map,module_name,FD_VOID);}
  else {
    fd_hashtable_op
      (&module_map,fd_table_default,module_name,module);
    as_stored = fd_get((fdtype)&module_map,module_name,FD_VOID);}
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

FD_EXPORT fdtype fd_get_module(fdtype name,int safe)
{
  if (safe)
    return fd_hashtable_get(&safe_module_map,name,FD_VOID);
  else {
    fdtype module = fd_hashtable_get(&module_map,name,FD_VOID);
    if (FD_VOIDP(module))
      return fd_hashtable_get(&safe_module_map,name,FD_VOID);
    else return module;}
}

FD_EXPORT int fd_discard_module(fdtype name,int safe)
{
  if (safe)
    return fd_hashtable_store(&safe_module_map,name,FD_VOID);
  else {
    fdtype module = fd_hashtable_get(&module_map,name,FD_VOID);
    if (FD_VOIDP(module))
      return fd_hashtable_store(&safe_module_map,name,FD_VOID);
    else {
      fd_decref(module);
      return fd_hashtable_store(&module_map,name,FD_VOID);}}
}

/* Making some functions */

FD_EXPORT fdtype fd_make_evalfn(u8_string name,fd_eval_handler fn)
{
  struct FD_EVALFN *f = u8_alloc(struct FD_EVALFN);
  FD_INIT_CONS(f,fd_evalfn_type);
  f->evalfn_name = u8_strdup(name); 
  f->evalfn_filename = NULL; 
  f->evalfn_handler = fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT void fd_defspecial(fdtype mod,u8_string name,fd_eval_handler fn)
{
  struct FD_EVALFN *f = u8_alloc(struct FD_EVALFN);
  FD_INIT_CONS(f,fd_evalfn_type);
  f->evalfn_name = u8_strdup(name); 
  f->evalfn_handler = fn; 
  f->evalfn_filename = NULL;
  fd_store(mod,fd_intern(name),FDTYPE_CONS(f));
  fd_decref(FDTYPE_CONS(f));
}

/* The Evaluator */

static fdtype eval_evalfn(fdtype x,fd_lexenv env,fd_stack stack)
{
  fdtype expr_expr = fd_get_arg(x,1);
  fdtype expr = fd_stack_eval(expr_expr,env,stack,0);
  fdtype result = fd_stack_eval(expr,env,stack,0);
  fd_decref(expr);
  return result;
}

static fdtype boundp_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype symbol = fd_get_arg(expr,1);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_evalfn",NULL,fd_incref(expr));
  else {
    fdtype val = fd_symeval(symbol,env);
    if ((FD_VOIDP(val))||(val == FD_DEFAULT_VALUE))
      return FD_FALSE;
    else if (val == FD_UNBOUND) 
      return FD_FALSE;
    else {
      fd_decref(val); 
      return FD_TRUE;}}
}

static fdtype modref_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype module = fd_get_arg(expr,1);
  fdtype symbol = fd_get_arg(expr,2);
  if ((FD_VOIDP(module))||(FD_VOIDP(module)))
    return fd_err(fd_SyntaxError,"modref_evalfn",NULL,fd_incref(expr));
  else if (!(FD_HASHTABLEP(module)))
    return fd_type_error("module hashtable","modref_evalfn",module);
  else if (!(FD_SYMBOLP(symbol)))
    return fd_type_error("symbol","modref_evalfn",symbol);
  else return fd_hashtable_get((fd_hashtable)module,symbol,FD_UNBOUND);
}

static fdtype voidp_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype result = fd_eval(fd_get_arg(expr,1),env);
  if (FD_VOIDP(result)) return FD_TRUE;
  else {
    fd_decref(result);
    return FD_FALSE;}
}

static fdtype env_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  return (fdtype)fd_copy_env(env);
}

static fdtype symbol_boundp_prim(fdtype symbol,fdtype envarg)
{
  if (!(FD_SYMBOLP(symbol)))
    return fd_type_error(_("symbol"),"boundp_prim",symbol);
  else if (FD_LEXENVP(envarg)) {
    fd_lexenv env = (fd_lexenv)envarg;
    fdtype val = fd_symeval(symbol,env);
    if (FD_VOIDP(val)) return FD_FALSE;
    else if (val == FD_DEFAULT_VALUE) return FD_FALSE;
    else if (val == FD_UNBOUND) return FD_FALSE;
    else return FD_TRUE;}
  else if (FD_TABLEP(envarg)) {
    fdtype val = fd_get(envarg,symbol,FD_VOID);
    if (FD_VOIDP(val)) return FD_FALSE;
    else return FD_TRUE;}
  else return fd_type_error(_("environment"),"symbol_boundp_prim",envarg);
}

static fdtype environmentp_prim(fdtype arg)
{
  if (FD_LEXENVP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

/* Withenv forms */

static fdtype withenv(fdtype expr,fd_lexenv env,
                      fd_lexenv consed_env,u8_context cxt)
{
  fdtype bindings = fd_get_arg(expr,1);
  if (FD_VOIDP(bindings))
    return fd_err(fd_TooFewExpressions,cxt,NULL,expr);
  else if ((FD_EMPTY_LISTP(bindings))||(FD_FALSEP(bindings))) {}
  else if (FD_PAIRP(bindings)) {
    FD_DOLIST(varval,bindings) {
      if ((FD_PAIRP(varval))&&(FD_SYMBOLP(FD_CAR(varval)))&&
          (FD_PAIRP(FD_CDR(varval)))&&
          (FD_EMPTY_LISTP(FD_CDR(FD_CDR(varval))))) {
        fdtype var = FD_CAR(varval), val = fd_eval(FD_CADR(varval),env);
        if (FD_ABORTED(val)) return FD_ERROR_VALUE;
        int rv=fd_bind_value(var,val,consed_env);
        fd_decref(val);
        if (rv<0) return FD_ERROR_VALUE;}
      else return fd_err(fd_SyntaxError,cxt,NULL,expr);}}
  else if (FD_TABLEP(bindings)) {
    fdtype keys = fd_getkeys(bindings);
    FD_DO_CHOICES(key,keys) {
      if (FD_SYMBOLP(key)) {
         int rv=0; fdtype value = fd_get(bindings,key,FD_VOID);
        if (!(FD_VOIDP(value)))
          rv=fd_bind_value(key,value,consed_env);
        fd_decref(value);
        if (rv<0) return FD_ERROR_VALUE;}
      else {
        FD_STOP_DO_CHOICES;
        fd_recycle_lexenv(consed_env);
        return fd_err(fd_SyntaxError,cxt,NULL,expr);}
      fd_decref(keys);}}
  else return fd_err(fd_SyntaxError,cxt,NULL,expr);
  /* Execute the body */ {
    fdtype result = FD_VOID;
    fdtype body = fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result);
      result = fd_eval(elt,consed_env);
      if (FD_ABORTED(result)) {
        return result;}}
    return result;}
}

static fdtype withenv_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv consed_env = fd_working_lexenv();
  fdtype result = withenv(expr,env,consed_env,"WITHENV");
  fd_recycle_lexenv(consed_env);
  return result;
}

static fdtype withenv_safe_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv consed_env = fd_safe_working_lexenv();
  fdtype result = withenv(expr,env,consed_env,"WITHENV/SAFE");
  fd_recycle_lexenv(consed_env);
  return result;
}

/* Eval/apply related primitives */

static fdtype get_arg_prim(fdtype expr,fdtype elt,fdtype dflt)
{
  if (FD_PAIRP(expr))
    if (FD_UINTP(elt)) {
      int i = 0, lim = FD_FIX2INT(elt); fdtype scan = expr;
      while ((i<lim) && (FD_PAIRP(scan))) {
        scan = FD_CDR(scan); i++;}
      if (FD_PAIRP(scan)) return fd_incref(FD_CAR(scan));
      else return fd_incref(dflt);}
    else return fd_type_error(_("fixnum"),"get_arg_prim",elt);
  else return fd_type_error(_("pair"),"get_arg_prim",expr);
}

static fdtype apply_lexpr(int n,fdtype *args)
{
  FD_DO_CHOICES(fn,args[0])
    if (!(FD_APPLICABLEP(args[0]))) {
      FD_STOP_DO_CHOICES;
      return fd_type_error("function","apply_lexpr",args[0]);}
  {
    fdtype results = FD_EMPTY_CHOICE;
    FD_DO_CHOICES(fn,args[0]) {
      FD_DO_CHOICES(final_arg,args[n-1]) {
        fdtype result = FD_VOID;
        int final_length = fd_seq_length(final_arg);
        int n_args = (n-2)+final_length;
        fdtype *values = u8_alloc_n(n_args,fdtype);
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
        else {FD_ADD_TO_CHOICE(results,result);}
        i = 0; while (i<n_args) {fd_decref(values[i]); i++;}
        u8_free(values);}
      if (FD_ABORTED(results)) {
        FD_STOP_DO_CHOICES;
        return results;}}
    return results;
  }
}

/* Initialization */

extern void fd_init_coreprims_c(void);

static fdtype lispenv_get(fdtype e,fdtype s,fdtype d)
{
  fdtype result = fd_symeval(s,FD_XENV(e));
  if (FD_VOIDP(result)) return fd_incref(d);
  else return result;
}
static int lispenv_store(fdtype e,fdtype s,fdtype v)
{
  return fd_bind_value(s,v,FD_XENV(e));
}

static int lispenv_add(fdtype e,fdtype s,fdtype v)
{
  return add_to_value(s,v,FD_XENV(e));
}

/* Some datatype methods */

static int unparse_evalfn(u8_output out,fdtype x)
{
  struct FD_EVALFN *s=
    fd_consptr(struct FD_EVALFN *,x,fd_evalfn_type);
  u8_printf(out,"#<EvalFN %s>",s->evalfn_name);
  return 1;
}

static int dtype_evalfn(struct FD_OUTBUF *out,fdtype x)
{
  struct FD_EVALFN *s=
    fd_consptr(struct FD_EVALFN *,x,fd_evalfn_type);
  u8_string name=s->evalfn_name;
  u8_string filename=s->evalfn_filename;
  size_t name_len=strlen(name);
  ssize_t file_len=(filename) ? (strlen(filename)) : (-1);
  int n_elts = (file_len<0) ? (1) : (0);
  unsigned char buf[100], *tagname="%EVALFN";
  struct FD_OUTBUF tmp;
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
  size_t n_bytes=tmp.bufwrite-tmp.buffer;
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

static fdtype call_continuation(struct FD_FUNCTION *f,fdtype arg)
{
  struct FD_CONTINUATION *cont = (struct FD_CONTINUATION *)f;
  if (cont->retval == FD_NULL)
    return fd_err(ExpiredThrow,"call_continuation",NULL,arg);
  else if (FD_VOIDP(cont->retval)) {
    cont->retval = fd_incref(arg);
    return FD_THROW_VALUE;}
  else return fd_err(DoubleThrow,"call_continuation",NULL,arg);
}

static fdtype callcc (fdtype proc)
{
  fdtype continuation, value;
  struct FD_CONTINUATION *f = u8_alloc(struct FD_CONTINUATION);
  FD_INIT_FRESH_CONS(f,fd_cprim_type);
  f->fcn_name="continuation"; f->fcn_filename = NULL;
  f->fcn_ndcall = 1; f->fcn_xcall = 1; f->fcn_arity = 1; f->fcn_min_arity = 1;
  f->fcn_typeinfo = NULL; f->fcn_defaults = NULL;
  f->fcn_handler.xcall1 = call_continuation; f->retval = FD_VOID;
  continuation = FDTYPE_CONS(f);
  value = fd_apply(proc,1,&continuation);
  if ((value == FD_THROW_VALUE) && (!(FD_VOIDP(f->retval)))) {
    fdtype retval = f->retval;
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

static fdtype cachecall(int n,fdtype *args)
{
  if (FD_HASHTABLEP(args[0]))
    return fd_xcachecall((fd_hashtable)args[0],args[1],n-2,args+2);
  else return fd_cachecall(args[0],n-1,args+1);
}

static fdtype cachecall_probe(int n,fdtype *args)
{
  if (FD_HASHTABLEP(args[0]))
    return fd_xcachecall_try((fd_hashtable)args[0],args[1],n-2,args+2);
  else return fd_cachecall_try(args[0],n-1,args+1);
}

static fdtype cachedcallp(int n,fdtype *args)
{
  if (FD_HASHTABLEP(args[0]))
    if (fd_xcachecall_probe((fd_hashtable)args[0],args[1],n-2,args+2))
      return FD_TRUE;
    else return FD_FALSE;
  else if (fd_cachecall_probe(args[0],n-1,args+1))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype clear_callcache(fdtype arg)
{
  fd_clear_callcache(arg);
  return FD_VOID;
}

static fdtype tcachecall(int n,fdtype *args)
{
  return fd_tcachecall(args[0],n-1,args+1);
}

static fdtype with_threadcache_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  struct FD_THREAD_CACHE *tc = fd_push_threadcache(NULL);
  fdtype value = FD_VOID;
  FD_DOLIST(each,FD_CDR(expr)) {
    fd_decref(value); value = FD_VOID; value = fd_eval(each,env);
    if (FD_ABORTED(value)) {
      fd_pop_threadcache(tc);
      return value;}}
  fd_pop_threadcache(tc);
  return value;
}

static fdtype using_threadcache_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  struct FD_THREAD_CACHE *tc = fd_use_threadcache();
  fdtype value = FD_VOID;
  FD_DOLIST(each,FD_CDR(expr)) {
    fd_decref(value); value = FD_VOID; value = fd_eval(each,env);
    if (FD_ABORTED(value)) {
      if (tc) fd_pop_threadcache(tc);
      return value;}}
  if (tc) fd_pop_threadcache(tc);
  return value;
}

static fdtype use_threadcache_prim(fdtype arg)
{
  if (FD_FALSEP(arg)) {
    if (!(fd_threadcache)) return FD_FALSE;
    while (fd_threadcache) fd_pop_threadcache(fd_threadcache);
    return FD_TRUE;}
  else {
    struct FD_THREAD_CACHE *tc = fd_use_threadcache();
    if (tc) return FD_TRUE;
    else return FD_FALSE;}
}

/* Making DTPROCs */

static fdtype make_dtproc(fdtype name,fdtype server,fdtype min_arity,
                          fdtype arity,fdtype minsock,fdtype maxsock,
                          fdtype initsock)
{
  fdtype result;
  if (FD_VOIDP(min_arity))
    result = fd_make_dtproc(FD_SYMBOL_NAME(name),FD_STRDATA(server),1,-1,-1,
                          FD_FIX2INT(minsock),FD_FIX2INT(maxsock),
                          FD_FIX2INT(initsock));
  else if (FD_VOIDP(arity))
    result = fd_make_dtproc
      (FD_SYMBOL_NAME(name),FD_STRDATA(server),
       1,fd_getint(arity),fd_getint(arity),
       FD_FIX2INT(minsock),FD_FIX2INT(maxsock),
       FD_FIX2INT(initsock));
  else result=
         fd_make_dtproc
         (FD_SYMBOL_NAME(name),FD_STRDATA(server),1,
          fd_getint(arity),fd_getint(min_arity),
          FD_FIX2INT(minsock),FD_FIX2INT(maxsock),
          FD_FIX2INT(initsock));
  return result;
}

/* Remote evaluation */

static fd_exception ServerUndefined=_("Server unconfigured");

FD_EXPORT fdtype fd_open_dtserver(u8_string server,int bufsiz)
{
  struct FD_DTSERVER *dts = u8_alloc(struct FD_DTSERVER);
  u8_string server_addr; u8_socket socket;
  /* Start out by parsing the address */
  if ((*server)==':') {
    fdtype server_id = fd_config_get(server+1);
    if (FD_STRINGP(server_id))
      server_addr = u8_strdup(FD_STRDATA(server_id));
    else  {
      fd_seterr(ServerUndefined,"open_server",
                u8_strdup(dts->dtserverid),server_id);
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
                  u8_strdup(server),FD_VOID);}
  /* Otherwise, close the socket */
  else close(socket);
  /* And create a connection pool */
  dts->fd_connpool = u8_open_connpool(dts->dtserverid,2,4,1);
  /* If creating the connection pool fails for some reason,
     cleanup and return an error value. */
  if (dts->fd_connpool == NULL) {
    u8_free(dts->dtserverid); 
    u8_free(dts->dtserver_addr);
    u8_free(dts);
    return FD_ERROR_VALUE;}
  /* Otherwise, returh a dtserver object */
  FD_INIT_CONS(dts,fd_dtserver_type);
  return FDTYPE_CONS(dts);
}

static fdtype dtserverp(fdtype arg)
{
  if (FD_TYPEP(arg,fd_dtserver_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype dtserver_id(fdtype arg)
{
  struct FD_DTSERVER *dts = (struct FD_DTSERVER *) arg;
  return fdtype_string(dts->dtserverid);
}

static fdtype dtserver_address(fdtype arg)
{
  struct FD_DTSERVER *dts = (struct FD_DTSERVER *) arg;
  return fdtype_string(dts->dtserver_addr);
}

static fdtype dteval(fdtype server,fdtype expr)
{
  if (FD_TYPEP(server,fd_dtserver_type))  {
    struct FD_DTSERVER *dtsrv=
      fd_consptr(fd_stream_erver,server,fd_dtserver_type);
    return fd_dteval(dtsrv->fd_connpool,expr);}
  else if (FD_STRINGP(server)) {
    fdtype s = fd_open_dtserver(FD_STRDATA(server),-1);
    if (FD_ABORTED(s)) return s;
    else {
      fdtype result = fd_dteval(((fd_stream_erver)s)->fd_connpool,expr);
      fd_decref(s);
      return result;}}
  else return fd_type_error(_("server"),"dteval",server);
}

static fdtype dtcall(int n,fdtype *args)
{
  fdtype server; fdtype request = FD_EMPTY_LIST, result; int i = n-1;
  if (n<2) return fd_err(fd_SyntaxError,"dtcall",NULL,FD_VOID);
  if (FD_TYPEP(args[0],fd_dtserver_type))
    server = fd_incref(args[0]);
  else if (FD_STRINGP(args[0])) server = fd_open_dtserver(FD_STRDATA(args[0]),-1);
  else return fd_type_error(_("server"),"eval/dtcall",args[0]);
  if (FD_ABORTED(server)) return server;
  while (i>=1) {
    fdtype param = args[i];
    if ((i>1) && ((FD_SYMBOLP(param)) || (FD_PAIRP(param))))
      request = fd_conspair(fd_make_list(2,quote_symbol,param),request);
    else request = fd_conspair(param,request);
    fd_incref(param); i--;}
  result = fd_dteval(((fd_stream_erver)server)->fd_connpool,request);
  fd_decref(request);
  fd_decref(server);
  return result;
}

static fdtype open_dtserver(fdtype server,fdtype bufsiz)
{
  return fd_open_dtserver(FD_STRDATA(server),
                             ((FD_VOIDP(bufsiz)) ? (-1) :
                              (FD_FIX2INT(bufsiz))));
}

/* Test functions */

static fdtype applytest(int n,fdtype *args)
{
  if (n<2)
    return fd_err(fd_TooFewArgs,"applytest",NULL,FD_VOID);
  else if (FD_APPLICABLEP(args[1])) {
    fdtype value = fd_apply(args[1],n-2,args+2);
    if (FD_EQUAL(value,args[0])) {
      fd_decref(value);
      return FD_TRUE;}
    else {
      u8_string s = fd_dtype2string(args[0]);
      fdtype err = fd_err(TestFailed,"applytest",s,value);
      u8_free(s); fd_decref(value);
      return err;}}
  else if (n==2) {
    if (FD_EQUAL(args[1],args[0]))
      return FD_TRUE;
    else {
      u8_string s = fd_dtype2string(args[0]);
      fdtype err = fd_err(TestFailed,"applytest",s,args[1]);
      u8_free(s);
      return err;}}
  else if (FD_EQUAL(args[2],args[0]))
    return FD_TRUE;
  else {
    u8_string s = fd_dtype2string(args[0]);
    fdtype err = fd_err(TestFailed,"applytest",s,args[2]);
    u8_free(s);
    return err;}
}

static fdtype evaltest_evalfn(fdtype expr,fd_lexenv env,fd_stack s)
{
  fdtype testexpr = fd_get_arg(expr,2);
  fdtype expected = fd_eval(fd_get_arg(expr,1),env);
  if ((FD_VOIDP(testexpr)) || (FD_VOIDP(expected))) {
    fd_decref(expected);
    return fd_err(fd_SyntaxError,"evaltest",NULL,expr);}
  else {
    fdtype value = fd_eval(testexpr,env);
    if (FD_ABORTED(value)) {
      fd_decref(expected);
      return value;}
    else if (FD_EQUAL(value,expected)) {
      fd_decref(expected);
      fd_decref(value);
      return FD_TRUE;}
    else {
      u8_string s = fd_dtype2string(expected);
      fdtype err = fd_err(TestFailed,"applytest",s,value);
      u8_free(s); fd_decref(value); fd_decref(expected);
      return err;}}
}

/* Getting documentation */

FD_EXPORT
u8_string fd_get_documentation(fdtype x)
{
  fdtype proc = (FD_FCNIDP(x)) ? (fd_fcnid_ref(x)) : (x);
  fd_ptr_type proctype = FD_PTR_TYPE(proc);
  if (proctype == fd_sproc_type) {
    struct FD_SPROC *sproc = (fd_sproc)proc;
    if (sproc->fcn_documentation)
      return sproc->fcn_documentation;
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,120);
      fdtype arglist = sproc->sproc_arglist, scan = arglist;
      if (sproc->fcn_name)
        u8_puts(&out,sproc->fcn_name);
      else u8_puts(&out,"λ");
      while (FD_PAIRP(scan)) {
        fdtype arg = FD_CAR(scan);
        if (FD_SYMBOLP(arg))
          u8_printf(&out," %ls",FD_SYMBOL_NAME(arg));
        else if ((FD_PAIRP(arg))&&(FD_SYMBOLP(FD_CAR(arg))))
          u8_printf(&out," [%ls]",FD_SYMBOL_NAME(FD_CAR(arg)));
        else u8_printf(&out," %q",arg);
        scan = FD_CDR(scan);}
      if (FD_SYMBOLP(scan))
        u8_printf(&out," [%ls...]",FD_SYMBOL_NAME(scan));
      sproc->fcn_documentation = out.u8_outbuf;
      return out.u8_outbuf;}}
  else if (fd_functionp[proctype]) {
    struct FD_FUNCTION *f = FD_DTYPE2FCN(proc);
    return f->fcn_documentation;}
  else if (FD_TYPEP(x,fd_evalfn_type)) {
    struct FD_EVALFN *sf = (fd_evalfn)proc;
    return sf->evalfn_documentation;}
  else return NULL;
}

static fdtype get_documentation(fdtype x)
{
  u8_string doc = fd_get_documentation(x);
  if (doc) return fdtype_string(doc);
  else return FD_FALSE;
}

/* Debugging assistance */

FD_EXPORT fdtype _fd_dbg(fdtype x)
{
  fdtype result=_fd_debug(x);
  if (result == x)
    return result;
  else {
    fd_incref(result);
    fd_decref(x);
    return result;}
}

void (*fd_dump_backtrace)(fdtype bt);

static fdtype dbg_evalfn(fdtype expr,fd_lexenv env,fd_stack stack)
{
  fdtype arg_expr=fd_get_arg(expr,1);
  fdtype msg_expr=fd_get_arg(expr,2);
  fdtype arg=fd_eval(arg_expr,env);
  if (FD_VOIDP(msg_expr))
    u8_message("Debug %q",arg);
  else {
    fdtype msg=fd_eval(msg_expr,env);
    if (FD_VOIDP(msg_expr))
      u8_message("Debug %q",arg);
    else if (FD_FALSEP(msg)) {}
    else if (FD_VOIDP(arg))
      u8_message("Debug called");
    else u8_message("Debug (%q) %q",msg,arg);
    fd_decref(msg);}
  return _fd_dbg(arg);
}

static fdtype void_prim(int n,fdtype *args)
{
  return FD_VOID;
}

static fdtype default_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype symbol = fd_get_arg(expr,1);
  fdtype default_expr = fd_get_arg(expr,2);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_evalfn",NULL,fd_incref(expr));
  else if (FD_VOIDP(default_expr))
    return fd_err(fd_SyntaxError,"boundp_evalfn",NULL,fd_incref(expr));
  else {
    fdtype val = fd_symeval(symbol,env);
    if (FD_VOIDP(val))
      return fd_eval(default_expr,env);
    else if (val == FD_UNBOUND)
      return fd_eval(default_expr,env);
    else return val;}
}

/* Checking version numbers */

static int check_num(fdtype arg,int num)
{
  if ((!(FD_FIXNUMP(arg)))||(FD_FIX2INT(arg)<0)) {
    fd_xseterr(fd_TypeError,"check_version_prim",NULL,arg);
    return -1;}
  else {
    int n = FD_FIX2INT(arg);
    if (num>=n) 
      return 1;
    else return 0;}
}

static fdtype check_version_prim(int n,fdtype *args)
{
  int rv = check_num(args[0],FD_MAJOR_VERSION);
  if (rv<0) return FD_ERROR_VALUE; 
  else if (rv==0) return FD_FALSE;
  else if (n==1) return FD_TRUE;
  else rv = check_num(args[1],FD_MINOR_VERSION);
  if (rv<0) return FD_ERROR_VALUE; 
  else if (rv==0) return FD_FALSE;
  else if (n==2) return FD_TRUE;
  else rv = check_num(args[2],FD_RELEASE_VERSION);
  if (rv<0) return FD_ERROR_VALUE; 
  else if (rv==0) return FD_FALSE;
  else if (n==3) return FD_TRUE;
  else rv = check_num(args[2],FD_RELEASE_VERSION-1);
  /* The fourth argument should be a patch level, but we're not
     getting that in builds yet. So if there are more arguments,
     we see if required release number is larger than release-1 
     (which means that we should be okay, since patch levels
     are reset with releases. */
  if (rv<0) return FD_ERROR_VALUE;
  else if (rv) {
    int i = 3; while (i<n) {
      if (!(FD_FIXNUMP(args[i]))) {
        fd_xseterr(fd_TypeError,"check_version_prim",NULL,args[i]);
        return -1;}
      else i++;}
    return FD_TRUE;}
  else return FD_FALSE;
}

static fdtype require_version_prim(int n,fdtype *args)
{
  fdtype result = check_version_prim(n,args);
  if (FD_ABORTP(result))
    return result;
  else if (FD_TRUEP(result))
    return result;
  else {
    fd_seterr("VersionError","require_version_prim",
              u8_mkstring("Version is %s",FRAMERD_REVISION),
              /* We know that args are all fixnums or we would have had an error. */
              fd_make_vector(n,args));
    return FD_ERROR_VALUE;}
}

/* Choice functions */

static fdtype fixchoice_prim(fdtype arg)
{
  if (FD_PRECHOICEP(arg))
    return fd_make_simple_choice(arg);
  else return fd_incref(arg);
}

static fdtype choiceref_prim(fdtype arg,fdtype off)
{
  if (FD_EXPECT_TRUE(FD_FIXNUMP(off))) {
    long long i = FD_FIX2INT(off);
    if (i==0) {
      if (FD_EMPTY_CHOICEP(arg)) {
        fd_seterr(fd_RangeError,"choiceref_prim",u8dup("0"),arg);
        return FD_ERROR_VALUE;}
      else return fd_incref(arg);}
    else if (FD_CHOICEP(arg)) {
      struct FD_CHOICE *ch = (fd_choice)arg;
      if (i>ch->choice_size) {
        fd_seterr(fd_RangeError,"choiceref_prim",
                  u8_mkstring("%lld",i),fd_incref(arg));
        return FD_ERROR_VALUE;}
      else {
        fdtype elt = FD_XCHOICE_DATA(ch)[i];
        return fd_incref(elt);}}
    else if (FD_PRECHOICEP(arg)) {
      fdtype simplified = fd_make_simple_choice(arg); 
      fdtype result = choiceref_prim(simplified,off);
      fd_decref(simplified);
      return result;}
    else {
      fd_seterr(fd_RangeError,"choiceref_prim",
                u8_mkstring("%lld",i),fd_incref(arg));
      return FD_ERROR_VALUE;}}
  else return fd_type_error("fixnum","choiceref_prim",off);
}

/* FFI */

#if FD_ENABLE_FFI
static fdtype ffi_proc_helper(int err,int n,fdtype *args)
{
  fdtype name_arg = args[0], filename_arg = args[1];
  fdtype return_type = args[2];
  u8_string name = (FD_STRINGP(name_arg)) ? (FD_STRDATA(name_arg)) : (NULL);
  u8_string filename = (FD_STRINGP(filename_arg)) ?
    (FD_STRDATA(filename_arg)) :
    (NULL);
  if (!(name))
    return fd_type_error("String","ffi_proc/name",name_arg);
  else if (!((FD_STRINGP(filename_arg))||(FD_FALSEP(filename_arg))))
    return fd_type_error("String","ffi_proc/filename",filename_arg);
  else {
    struct FD_FFI_PROC *fcn=
      fd_make_ffi_proc(name,filename,n-3,return_type,args+3);
    if (fcn)
      return (fdtype) fcn;
    else {
      fd_clear_errors(1);
      return FD_FALSE;}}
}
static fdtype ffi_proc(int n,fdtype *args)
{
  return ffi_proc_helper(1,n,args);
}
static fdtype ffi_probe(int n,fdtype *args)
{
  return ffi_proc_helper(0,n,args);
}
static fdtype ffi_found_prim(fdtype name,fdtype modname)
{
  void *module=NULL, *sym=NULL;
  if (FD_STRINGP(modname)) {
    module=u8_dynamic_load(FD_STRDATA(modname));
    if (module==NULL) {
      fd_clear_errors(0);
      return FD_FALSE;}}
  sym=u8_dynamic_symbol(FD_STRDATA(name),module);
  if (sym)
    return FD_TRUE;
  else return FD_FALSE;
}
#else
static fdtype ffi_proc(int n,fdtype *args)
{
  u8_seterr("NotImplemented","ffi_proc",
            u8_strdup("No FFI support is available in this build of FramerD"));
  return FD_ERROR_VALUE;
}
static fdtype ffi_probe(int n,fdtype *args)
{
  return FD_FALSE;
}
static fdtype ffi_found_prim(fdtype name,fdtype modname)
{
  return FD_FALSE;
}
#endif

/* MTrace */

static int mtracing=0;

static fdtype mtrace_prim(fdtype arg)
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
  else if ((FD_STRINGP(arg)) &&
           (u8_file_writablep(FD_STRDATA(arg)))) {
    setenv("MALLOC_TRACE",FD_STRDATA(arg),1);
    mtrace();
    mtracing=1;
    return FD_TRUE;}
  else return FD_FALSE;
#else
  return FD_FALSE;
#endif
}

static fdtype muntrace_prim()
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

static fdtype list9(fdtype arg1,fdtype arg2,
                    fdtype arg3,fdtype arg4,
                    fdtype arg5,fdtype arg6,
                    fdtype arg7,fdtype arg8,
                    fdtype arg9)
{
  return fd_make_list(9,fd_incref(arg1),fd_incref(arg2),
                      fd_incref(arg3),fd_incref(arg4),
                      fd_incref(arg5),fd_incref(arg6),
                      fd_incref(arg7),fd_incref(arg8),
                      fd_incref(arg9));
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
  fd_dtype_writers[fd_evalfn_type]=dtype_evalfn;

  fd_unparsers[fd_lexref_type]=unparse_lexref;
  fd_dtype_writers[fd_lexref_type]=dtype_lexref;
  fd_type_names[fd_lexref_type]=_("lexref");

  fd_unparsers[fd_coderef_type]=unparse_coderef;
  fd_dtype_writers[fd_coderef_type]=dtype_coderef;
  fd_type_names[fd_coderef_type]=_("coderef");

  quote_symbol = fd_intern("QUOTE");
  _fd_comment_symbol = comment_symbol = fd_intern("COMMENT");
  profile_symbol = fd_intern("%PROFILE");
  moduleid_symbol = fd_intern("%MODULEID");

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
  fd_defspecial(fd_scheme_module,"EVAL",eval_evalfn);
  fd_defspecial(fd_scheme_module,"BOUND?",boundp_evalfn);
  fd_defspecial(fd_scheme_module,"VOID?",voidp_evalfn);
  fd_defspecial(fd_scheme_module,"QUOTE",quote_evalfn);
  fd_defspecial(fd_scheme_module,"%ENV",env_evalfn);
  fd_defspecial(fd_scheme_module,"%MODREF",modref_evalfn);

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
            fd_fixnum_type,FD_VOID,fd_fixnum_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"%LEXREFVAL",lexref_value_prim,1,
            "(%LEXREFVAL *lexref*) returns the offsets "
            "of a lexref as a pair (*up* . *across*)",
            fd_lexref_type,FD_VOID);

  fd_idefn1(fd_scheme_module,"%CODEREF",coderef_prim,1,
            "(%CODEREF *nelts*) returns a 'coderef' (a relative position) value",
            fd_fixnum_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"%CODREFVAL",coderef_value_prim,1,
            "(%CODEREFVAL *coderef*) returns the integer relative offset of a coderef",
            fd_lexref_type,FD_VOID);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("%CHOICEREF",choiceref_prim,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim1("%FIXCHOICE",fixchoice_prim,1)));


  fd_defspecial(fd_scheme_module,"WITHENV",withenv_safe_evalfn);
  fd_defspecial(fd_xscheme_module,"WITHENV",withenv_evalfn);
  fd_defspecial(fd_xscheme_module,"WITHENV/SAFE",withenv_safe_evalfn);


  fd_idefn(fd_scheme_module,fd_make_cprim3("GET-ARG",get_arg_prim,2));
  fd_defspecial(fd_scheme_module,"GETOPT",getopt_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim3x("%GETOPT",getopt_prim,2,
                                          -1,FD_VOID,fd_symbol_type,FD_VOID,
                                          -1,FD_FALSE)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("TESTOPT",testopt_prim,2,
                           -1,FD_VOID,fd_symbol_type,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("OPT+",optplus_prim,2,
                           -1,FD_VOID,fd_symbol_type,FD_VOID,
                           -1,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("APPLY",apply_lexpr,1)));
  fd_idefn(fd_xscheme_module,fd_make_cprim7x
           ("DTPROC",make_dtproc,2,
            fd_symbol_type,FD_VOID,fd_string_type,FD_VOID,
            -1,FD_VOID,-1,FD_VOID,
            fd_fixnum_type,FD_INT(2),
            fd_fixnum_type,FD_INT(4),
            fd_fixnum_type,FD_INT(1)));

  fd_idefn(fd_scheme_module,fd_make_cprim1("CALL/CC",callcc,1));
  fd_defalias(fd_scheme_module,"CALL-WITH-CURRENT-CONTINUATION","CALL/CC");

  /* This pushes a new threadcache */
  fd_defspecial(fd_scheme_module,"WITH-THREADCACHE",with_threadcache_evalfn);
  /* This ensures that there's an active threadcache, pushing a new one if
     needed or using the current one if it exists. */
  fd_defspecial(fd_scheme_module,"USING-THREADCACHE",using_threadcache_evalfn);
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

  fd_defspecial(fd_scheme_module,"TIMEVAL",timed_eval_evalfn);
  fd_defspecial(fd_scheme_module,"%TIMEVAL",timed_evalx_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("%WATCHPTR",watchptr_prim,1)));
  fd_defspecial(fd_scheme_module,"%WATCH",watched_eval_evalfn);
  fd_defspecial(fd_scheme_module,"PROFILE",profiled_eval_evalfn);
  fd_defspecial(fd_scheme_module,"%WATCHCALL",watchcall_evalfn);
  fd_defalias(fd_scheme_module,"%WC","%WATCHCALL");
  fd_defspecial(fd_scheme_module,"%WATCHCALL+",watchcall_plus_evalfn);
  fd_defalias(fd_scheme_module,"%WC+","%WATCHCALL+");
  fd_defspecial(fd_scheme_module,"EVAL1",eval1);
  fd_defspecial(fd_scheme_module,"EVAL2",eval2);
  fd_defspecial(fd_scheme_module,"EVAL3",eval3);
  fd_defspecial(fd_scheme_module,"EVAL4",eval4);
  fd_defspecial(fd_scheme_module,"EVAL5",eval5);
  fd_defspecial(fd_scheme_module,"EVAL6",eval6);
  fd_defspecial(fd_scheme_module,"EVAL7",eval7);

#if USING_GOOGLE_PROFILER
  fd_defspecial(fd_scheme_module,"GOOGLE/PROFILE",gprofile_evalfn);
  fd_idefn(fd_scheme_module,
           fd_make_cprim0("GOOGLE/PROFILE/STOP",gprofile_stop));
#endif
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("APPLYTEST",applytest,2)));
  fd_defspecial(fd_scheme_module,"EVALTEST",evaltest_evalfn);

  fd_defspecial(fd_scheme_module,"DBG",dbg_evalfn);
  fd_idefn(fd_scheme_module,fd_make_ndprim(fd_make_cprimn("VOID",void_prim,0)));
  fd_defspecial(fd_scheme_module,"DEFAULT",default_evalfn);

  fd_idefn(fd_scheme_module,fd_make_cprimn("CHECK-VERSION",check_version_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("REQUIRE-VERSION",require_version_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("DTEVAL",dteval,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("DTCALL",dtcall,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("OPEN-DTSERVER",open_dtserver,1,
                                            fd_string_type,FD_VOID,
                                            fd_fixnum_type,FD_VOID));
  fd_idefn1(fd_scheme_module,"DTSERVER?",dtserverp,FD_NEEDS_1_ARG,
            "Returns true if it's argument is a dtype server object",
            -1,FD_VOID);
  fd_idefn1(fd_scheme_module,"DTSERVER-ID",
            dtserver_id,FD_NEEDS_1_ARG,
            "Returns the ID of a dtype server (the argument used to create it)",
            fd_dtserver_type,FD_VOID);
  fd_idefn1(fd_scheme_module,"DTSERVER-ADDRESS",
            dtserver_address,FD_NEEDS_1_ARG,
            "Returns the address (host/port) of a dtype server",
            fd_dtserver_type,FD_VOID);

  fd_idefn0(fd_scheme_module,"%BUILDINFO",fd_get_build_info,
            "Information about the build and startup environment");

  fd_idefn(fd_xscheme_module,fd_make_cprimn("FFI/PROC",ffi_proc,3));
  fd_idefn(fd_xscheme_module,fd_make_cprimn("FFI/PROBE",ffi_probe,3));
  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x("FFI/FOUND?",ffi_found_prim,1,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));

  fd_idefn1(fd_xscheme_module,"MTRACE",mtrace_prim,FD_NEEDS_0_ARGS,
            "Activates LIBC heap tracing to MALLOC_TRACE and "
            "returns true if it worked. Optional argument is a "
            "filename to set as MALLOC_TRACE",
            -1,FD_VOID);
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
}

FD_EXPORT void fd_init_errors_c(void);
FD_EXPORT void fd_init_compounds_c(void);
FD_EXPORT void fd_init_threads_c(void);
FD_EXPORT void fd_init_conditionals_c(void);
FD_EXPORT void fd_init_iterators_c(void);
FD_EXPORT void fd_init_choicefns_c(void);
FD_EXPORT void fd_init_binders_c(void);
FD_EXPORT void fd_init_sprocs_c(void);
FD_EXPORT void fd_init_macros_c(void);
FD_EXPORT void fd_init_coreprims_c(void);
FD_EXPORT void fd_init_tableprims_c(void);
FD_EXPORT void fd_init_stringprims_c(void);
FD_EXPORT void fd_init_dbprims_c(void);
FD_EXPORT void fd_init_seqprims_c(void);
FD_EXPORT void fd_init_modules_c(void);
FD_EXPORT void fd_init_load_c(void);
FD_EXPORT void fd_init_portprims_c(void);
FD_EXPORT void fd_init_streamprims_c(void);
FD_EXPORT void fd_init_timeprims_c(void);
FD_EXPORT void fd_init_sysprims_c(void);
FD_EXPORT void fd_init_arith_c(void);
FD_EXPORT void fd_init_side_effects_c(void);
FD_EXPORT void fd_init_reflection_c(void);
FD_EXPORT void fd_init_history_c(void);
FD_EXPORT void fd_init_opcodes_c(void);
FD_EXPORT void fd_init_reqstate_c(void);
FD_EXPORT void fd_init_regex_c(void);
FD_EXPORT void fd_init_quasiquote_c(void);
FD_EXPORT void fd_init_extdbprims_c(void);

static void init_eval_core()
{
  init_localfns();
  fd_init_opcodes_c();
  fd_init_tableprims_c();
  fd_init_modules_c();
  fd_init_arith_c();
  fd_init_threads_c();
  fd_init_errors_c();
  fd_init_load_c();
  fd_init_conditionals_c();
  fd_init_iterators_c();
  fd_init_choicefns_c();
  fd_init_binders_c();
  fd_init_sprocs_c();
  fd_init_macros_c();
  fd_init_compounds_c();
  fd_init_quasiquote_c();
  fd_init_side_effects_c();
  fd_init_reflection_c();
  fd_init_reqstate_c();

  fd_init_regex_c();
  fd_init_history_c();

  fd_init_coreprims_c();
  fd_init_stringprims_c();
  fd_init_dbprims_c();
  fd_init_seqprims_c();
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

    default_env = fd_make_env(fd_make_hashtable(NULL,0),NULL);
    safe_default_env = fd_make_env(fd_make_hashtable(NULL,0),NULL);

    u8_register_source_file(FRAMERD_EVAL_H_INFO);
    u8_register_source_file(_FILEINFO);

    init_scheme_module();
    init_eval_core();

    return scheme_initialized;}
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
