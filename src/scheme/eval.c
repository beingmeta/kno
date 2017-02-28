/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/fddb.h"
#include "framerd/eval.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

static volatile int fdscheme_initialized=0;

int fd_optimize_tail_calls=1;

fdtype fd_scheme_module, fd_xscheme_module;

fdtype _fd_comment_symbol;

static fdtype quote_symbol, comment_symbol, moduleid_symbol;

static fd_exception TestFailed=_("Test failed");
static fd_exception ExpiredThrow=_("Continuation is no longer valid");
static fd_exception DoubleThrow=_("Continuation used twice");
static fd_exception LostThrow=_("Lost invoked continuation");

static char *cpu_profilename=NULL;

fd_exception
  fd_SyntaxError=_("SCHEME expression syntax error"),
  fd_InvalidMacro=_("invalid macro transformer"),
  fd_UnboundIdentifier=_("the variable is unbound"),
  fd_VoidArgument=_("VOID result passed as argument"),
  fd_NotAnIdentifier=_("not an identifier"),
  fd_TooFewExpressions=_("too few subexpressions"),
  fd_CantBind=_("can't add binding to environment"),
  fd_ReadOnlyEnv=_("Read only environment");

u8_context fd_eval_context="EVAL";

/* Environment functions */

static fdtype lexref_prim(fdtype upv,fdtype acrossv)
{
  int up=FD_FIX2INT(upv), across=FD_FIX2INT(acrossv);
  int combined=((up<<5)|(across));
  return FDTYPE_IMMEDIATE(fd_lexref_type,combined);
}

static int unparse_lexref(u8_output out,fdtype lexref)
{
  int code=FD_GET_IMMEDIATE(lexref,fd_lexref_type);
  int up=code/32, across=code%32;
  u8_printf(out,"#<LEXREF ^%d+%d>",up,across);
  return 1;
}

FD_EXPORT int fd_bind_value(fdtype sym,fdtype val,fd_lispenv env)
{
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

static int bound_in_envp(fdtype symbol,fd_lispenv env)
{
  fdtype bindings=env->env_bindings;
  if (FD_HASHTABLEP(bindings))
    return fd_hashtable_probe((fd_hashtable)bindings,symbol);
  else if (FD_SLOTMAPP(bindings))
    return fd_slotmap_test((fd_slotmap)bindings,symbol,FD_VOID);
  else if (FD_SCHEMAPP(bindings))
    return fd_schemap_test((fd_schemap)bindings,symbol,FD_VOID);
  else return fd_test(bindings,symbol,FD_VOID);
}

FD_EXPORT int fd_set_value(fdtype symbol,fdtype value,fd_lispenv env)
{
  if (env->env_copy) env=env->env_copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env=env->env_parent;
      if ((env) && (env->env_copy)) env=env->env_copy;}
    else if ((env->env_bindings) == (env->env_exports))
      /* This is the kind of environment produced by using a module,
         so it's read only. */
      return fd_reterr(fd_ReadOnlyEnv,"fd_set_value",NULL,symbol);
    else {
      fd_store(env->env_bindings,symbol,value);
      if (FD_HASHTABLEP(env->env_exports))
        fd_hashtable_op((fd_hashtable)(env->env_exports),
                        fd_table_replace,symbol,value);
      return 1;}}
  return 0;
}

FD_EXPORT fdtype _fd_symeval(fdtype sym,fd_lispenv env)
{
  return fd_symeval(sym,env);
}

FD_EXPORT fdtype _fd_lexref(fdtype lexref,fd_lispenv env)
{
  return fd_lexref(lexref,env);
}

FD_EXPORT int fd_add_value(fdtype symbol,fdtype value,fd_lispenv env)
{
  if (env->env_copy) env=env->env_copy;
  while (env) {
    if (!(bound_in_envp(symbol,env))) {
      env=env->env_parent;
      if ((env) && (env->env_copy)) env=env->env_copy;}
    else if ((env->env_bindings) == (env->env_exports))
      return fd_reterr(fd_ReadOnlyEnv,"fd_set_value",NULL,symbol);
    else {
      fd_add(env->env_bindings,symbol,value);
      if ((FD_HASHTABLEP(env->env_exports)) &&
          (fd_hashtable_probe((fd_hashtable)(env->env_exports),symbol)))
        fd_add(env->env_exports,symbol,value);
      return 1;}}
  return 0;
}

static fd_lispenv copy_environment(fd_lispenv env);

static fd_lispenv dynamic_environment(fd_lispenv env)
{
  if (env->env_copy) return env->env_copy;
  else {
    struct FD_ENVIRONMENT *newenv=u8_alloc(struct FD_ENVIRONMENT);
    FD_INIT_FRESH_CONS(newenv,fd_environment_type);
    if (env->env_parent)
      newenv->env_parent=copy_environment(env->env_parent);
    else newenv->env_parent=NULL;
    if (FD_STATIC_CONSP(FD_CONS_DATA(env->env_bindings)))
      newenv->env_bindings=fd_copy(env->env_bindings);
    else newenv->env_bindings=fd_incref(env->env_bindings);
    newenv->env_exports=fd_incref(env->env_exports);
    env->env_copy=newenv; newenv->env_copy=newenv;
    return newenv;}
}

static fd_lispenv copy_environment(fd_lispenv env)
{
  fd_lispenv dynamic=((env->env_copy) ? (env->env_copy) : (dynamic_environment(env)));
  return (fd_lispenv) fd_incref((fdtype)dynamic);
}

static fdtype lisp_copy_environment(fdtype env,int deep)
{
  return (fdtype) copy_environment((fd_lispenv)env);
}

FD_EXPORT fd_lispenv fd_copy_env(fd_lispenv env)
{
  if (env==NULL) return env;
  else if (env->env_copy) {
    fdtype existing=(fdtype)env->env_copy;
    fd_incref(existing);
    return env->env_copy;}
  else {
    fd_lispenv fresh=dynamic_environment(env);
    fdtype ref=(fdtype)fresh; fd_incref(ref);
    return fresh;}
}

static void recycle_environment(struct FD_RAW_CONS *envp)
{
  struct FD_ENVIRONMENT *env=(struct FD_ENVIRONMENT *)envp;
  fd_decref(env->env_bindings); fd_decref(env->env_exports);
  if (env->env_parent) fd_decref((fdtype)(env->env_parent));
  if (!(FD_STATIC_CONSP(envp))) {
    memset(env,0,sizeof(struct FD_ENVIRONMENT));
    u8_free(envp);}
}

static int env_recycle_depth=2;

static int count_cons_envrefs(fdtype obj,fd_lispenv env,int depth);

FD_FASTOP int count_envrefs(fdtype obj,fd_lispenv env,int depth)
{
  if (depth<=0) return 0;
  else if (FD_ATOMICP(obj)) return 0;
  else return count_cons_envrefs(obj,env,depth);
}

static int count_cons_envrefs(fdtype obj,fd_lispenv env,int depth)
{
  struct FD_CONS *cons=(struct FD_CONS *)obj;
  int constype=FD_CONS_TYPE(cons);
  if (1) /* (refcount==1) */
    switch (constype) {
    case fd_pair_type:
      return count_envrefs(FD_CAR(obj),env,depth)+
        count_envrefs(FD_CDR(obj),env,depth);
    case fd_vector_type: {
      int envcount=0;
      int i=0, len=FD_VECTOR_LENGTH(obj);
      fdtype *elts=FD_VECTOR_DATA(obj);
      while (i<len) {
        envcount=envcount+count_envrefs(elts[i],env,depth-1); i++;}
      return envcount;}
    case fd_schemap_type: {
      int envcount=0;
      int i=0, len=FD_SCHEMAP_SIZE(obj);
      fdtype *elts=FD_XSCHEMAP(obj)->schema_values;
      while (i<len) {
        envcount=envcount+count_envrefs(elts[i],env,depth-1); i++;}
      return envcount;}
    case fd_slotmap_type: {
      int envcount=0;
      int i=0, len=FD_SLOTMAP_NUSED(obj);
      struct FD_KEYVAL *kv=(FD_XSLOTMAP(obj))->sm_keyvals;
      while (i<len) {
        envcount=envcount+count_envrefs(kv[i].kv_val,env,depth-1); i++;}
      return envcount;}
    case fd_choice_type: {
      struct FD_CHOICE *ch=(struct FD_CHOICE *)obj;
      const fdtype *data=FD_XCHOICE_DATA(ch);
      int i=0, len=FD_XCHOICE_SIZE(ch), envcount=0;
      while (i<len) {
        fdtype e=data[i++];
        if ((FD_CONSP(e))) {
          int etype=FD_CONS_TYPE(((struct FD_CONS *)e));
          if (!((etype==fd_string_type)||(etype==fd_packet_type)||
                (etype==fd_bigint_type)||(etype==fd_flonum_type)||
                (etype==fd_complex_type)||(etype==fd_rational_type)||
                (etype==fd_timestamp_type)||(etype==fd_uuid_type)))
            envcount=envcount+count_envrefs(e,env,depth-1);}}
      return envcount;}
    case fd_achoice_type: {
      int envcount=0;
      struct FD_ACHOICE *ach=(struct FD_ACHOICE *)obj;
      fdtype *data=ach->achoice_data, *end=ach->achoice_write;
      while (data<end) {
        fdtype e=*data++;
        if ((FD_CONSP(e))) {
          int etype=FD_CONS_TYPE(((struct FD_CONS *)e));
          if (!((etype==fd_string_type)||(etype==fd_packet_type)||
                (etype==fd_bigint_type)||(etype==fd_flonum_type)||
                (etype==fd_complex_type)||(etype==fd_rational_type)||
                (etype==fd_timestamp_type)||(etype==fd_uuid_type)))
            envcount=envcount+count_envrefs(e,env,depth-1);}}
      return envcount;}
    case fd_hashtable_type: {
      int envcount=0;
      struct FD_HASHTABLE *ht=(struct FD_HASHTABLE *)obj;
      int i=0, n_slots; struct FD_HASH_BUCKET **slots;
      fd_read_lock_table(ht);
      n_slots=ht->ht_n_buckets; slots=ht->ht_buckets;
      while (i<n_slots)
        if (slots[i]) {
          struct FD_HASH_BUCKET *hashentry=slots[i++];
          int j=0, n_keyvals=hashentry->fd_n_entries;
          struct FD_KEYVAL *keyvals=&(hashentry->kv_val0);
          while (j<n_keyvals) {
            envcount=envcount+count_envrefs(keyvals[j].kv_val,env,depth-1);
            j++;}}
        else i++;
      fd_unlock_table(ht);
      return envcount;}
    default:
      if (constype==fd_environment_type) {
        fd_lispenv scan=FD_CONSPTR(fd_lispenv,obj);
        while (scan)
          if ((scan==env)||(scan->env_copy==env))
            return 1;
          else scan=scan->env_parent;
        return 0;}
      else if (constype==fd_macro_type) {
        struct FD_MACRO *m=FD_CONSPTR(fd_macro,cons);
        if ((m)&&(FD_TYPEP((m->fd_macro_transformer),fd_sproc_type)))
          return count_cons_envrefs(m->fd_macro_transformer,env,depth);
        else return 0;}
      else if (constype==fd_sproc_type) {
        struct FD_SPROC *sp=FD_CONSPTR(fd_sproc,obj);
        struct FD_ENVIRONMENT *scan=sp->sproc_env;
        while (scan) {
          if ((scan==env)||(scan->env_copy==env))
            return 1;
          else scan=scan->env_parent;}
        return 0;}
      else if (constype==fd_raw_pool_type) {
        struct FD_POOL *p=(struct FD_POOL *)obj;
        int count=0;
        if (FD_VOIDP(p->pool_namefn))
          count=count+count_envrefs(p->pool_namefn,env,depth);
        if (p->pool_handler==&fd_extpool_handler) {
          struct FD_EXTPOOL *xp=(struct FD_EXTPOOL *)obj;
          if (!(FD_VOIDP(xp->fetchfn)))
            count=count+count_envrefs(xp->fetchfn,env,depth);
          if (!(FD_VOIDP(xp->savefn)))
            count=count+count_envrefs(xp->savefn,env,depth);
          if (!(FD_VOIDP(xp->lockfn)))
            count=count+count_envrefs(xp->lockfn,env,depth);
          if (!(FD_VOIDP(xp->allocfn)))
            count=count+count_envrefs(xp->allocfn,env,depth);
          if (!(FD_VOIDP(xp->state)))
            count=count+count_envrefs(xp->state,env,depth);}
        return count;}
      else if (constype==fd_raw_index_type) {
        struct FD_INDEX *ix=(struct FD_INDEX *)obj;
        int count=0;
        if (ix->index_handler==&fd_extindex_handler) {
          struct FD_EXTINDEX *xi=(struct FD_EXTINDEX *)obj;
          if (!(FD_VOIDP(xi->fetchfn)))
            count=count+count_envrefs(xi->fetchfn,env,depth);
          if (!(FD_VOIDP(xi->commitfn)))
            count=count+count_envrefs(xi->commitfn,env,depth);
          if (!(FD_VOIDP(xi->state)))
            count=count+count_envrefs(xi->state,env,depth);}
        return count;}
      else return 0;}
  else return 0;
}

FD_EXPORT
/* fd_recycle_environment:
     Arguments: a lisp pointer to an environment
     Returns: 1 if the environment was recycled.
 This handles circular environment problems.  The problem is that environments
   commonly contain pointers to procedures which point back to the environment
   they are closed in.  This does a limited structure descent to see how many
   reclaimable environment pointers there may be.  If the reclaimable references
   are one more than the environment's reference count, then you can recycle
   the entire environment.
*/
int fd_recycle_environment(fd_lispenv env)
{
  int refcount=FD_CONS_REFCOUNT(env);
  if (refcount==0) return 0; /* Stack cons */
  else if (refcount==1) { /* Normal GC */
    fd_decref((fdtype)env);
    return 1;}
  else {
    int sproc_count=count_envrefs(env->env_bindings,env,env_recycle_depth);
    if (sproc_count+1==refcount) {
      struct FD_RAW_CONS *envstruct=(struct FD_RAW_CONS *)env;
      fd_decref(env->env_bindings); fd_decref(env->env_exports);
      if (env->env_parent) fd_decref((fdtype)(env->env_parent));
      envstruct->fd_conshead=(0xFFFFFF80|(env->fd_conshead&0x7F));
      u8_free(env);
      return 1;}
    else {fd_decref((fdtype)env); return 0;}}
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

static fdtype getopt_handler(fdtype expr,fd_lispenv env)
{
  fdtype opts=fd_eval(fd_get_arg(expr,1),env);
  if (FD_ABORTED(opts)) return opts;
  else {
    fdtype keys=fd_eval(fd_get_arg(expr,2),env);
    if (FD_ABORTED(keys)) {
      fd_decref(opts); return keys;}
    else {
      fdtype results=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(opt,opts) {
        FD_DO_CHOICES(key,keys) {
          fdtype v=fd_getopt(opt,key,FD_VOID);
          if (FD_ABORTED(v)) {
            fd_decref(results); results=v;
            FD_STOP_DO_CHOICES;}
          else if (!(FD_VOIDP(v))) {FD_ADD_TO_CHOICE(results,v);}}
        if (FD_ABORTED(results)) {FD_STOP_DO_CHOICES;}}
      fd_decref(keys); fd_decref(opts);
      if (FD_ABORTED(results)) {
        return results;}
      else if (FD_EMPTY_CHOICEP(results)) {
        fdtype dflt_expr=fd_get_arg(expr,3);
        if (FD_VOIDP(dflt_expr)) return FD_FALSE;
        else return fd_eval(dflt_expr,env);}
      else return results;}}
}
static fdtype getopt_prim(fdtype opts,fdtype keys,fdtype dflt)
{
  fdtype results=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(opt,opts) {
    FD_DO_CHOICES(key,keys) {
      fdtype v=fd_getopt(opt,key,FD_VOID);
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

static fdtype quote_handler(fdtype obj,fd_lispenv env)
{
  if ((FD_PAIRP(obj)) && (FD_PAIRP(FD_CDR(obj))) &&
      ((FD_CDR(FD_CDR(obj)))==FD_EMPTY_LIST))
    return fd_incref(FD_CAR(FD_CDR(obj)));
  else return fd_err(fd_SyntaxError,"QUOTE",NULL,obj);
}

/* Profiling functions */

static fdtype profile_symbol;

static fdtype profiled_eval(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  double start=u8_elapsed_time();
  fdtype value=fd_eval(toeval,env);
  double finish=u8_elapsed_time();
  fdtype tag=fd_get_arg(expr,2);
  fdtype profile_info=fd_symeval(profile_symbol,env), profile_data;
  if (FD_VOIDP(profile_info)) return value;
  profile_data=fd_get(profile_info,tag,FD_VOID);
  if (FD_ABORTED(profile_data)) {
    fd_decref(value); fd_decref(profile_info);
    return profile_data;}
  else if (FD_VOIDP(profile_data)) {
    fdtype time=fd_init_double(NULL,(finish-start));
    profile_data=fd_conspair(FD_INT(1),time);
    fd_store(profile_info,tag,profile_data);}
  else {
    struct FD_PAIR *p=fd_consptr(fd_pair,profile_data,fd_pair_type);
    struct FD_FLONUM *d=fd_consptr(fd_flonum,(p->fd_cdr),fd_flonum_type);
    p->fd_car=FD_INT(fd_getint(p->fd_car)+1);
    d->fd_dblval=d->fd_dblval+(finish-start);}
  fd_decref(profile_data); fd_decref(profile_info);
  return value;
}

/* These are for wrapping around Scheme code to see in C profilers */
static fdtype eval1(fdtype expr,fd_lispenv env) { return fd_eval(expr,env);}
static fdtype eval2(fdtype expr,fd_lispenv env) { return fd_eval(expr,env);}
static fdtype eval3(fdtype expr,fd_lispenv env) { return fd_eval(expr,env);}
static fdtype eval4(fdtype expr,fd_lispenv env) { return fd_eval(expr,env);}
static fdtype eval5(fdtype expr,fd_lispenv env) { return fd_eval(expr,env);}
static fdtype eval6(fdtype expr,fd_lispenv env) { return fd_eval(expr,env);}
static fdtype eval7(fdtype expr,fd_lispenv env) { return fd_eval(expr,env);}

/* Google profiler usage */

#if USING_GOOGLE_PROFILER
static fdtype gprofile_handler(fdtype expr,fd_lispenv env)
{
  int start=1; char *filename=NULL;
  if ((FD_PAIRP(FD_CDR(expr)))&&(FD_STRINGP(FD_CAR(FD_CDR(expr))))) {
    filename=u8_strdup(FD_STRDATA(FD_CAR(FD_CDR(expr))));
    start=2;}
  else if ((FD_PAIRP(FD_CDR(expr)))&&(FD_SYMBOLP(FD_CAR(FD_CDR(expr))))) {
    fdtype val=fd_symeval(FD_CADR(expr),env);
    if (FD_STRINGP(val)) {
      filename=u8_strdup(FD_STRDATA(val));
      fd_decref(val);
      start=2;}
    else return fd_type_error("filename","GOOGLE/PROFILE",val);}
  else if (cpu_profilename)
    filename=u8_strdup(cpu_profilename);
  else if (getenv("CPUPROFILE"))
    filename=u8_strdup(getenv("CPUPROFILE"));
  else filename=u8_mkstring("/tmp/gprof%ld.pid",(long)getpid());
  ProfilerStart(filename); {
    fdtype fd_value=FD_VOID;
    fdtype body=fd_get_body(expr,start);
    FD_DOLIST(ex,body) {
      fd_decref(fd_value); fd_value=fd_eval(ex,env);
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

static fdtype timed_eval(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  double start=u8_elapsed_time();
  fdtype value=fd_eval(toeval,env);
  double finish=u8_elapsed_time();
  u8_message(";; Executed %q in %f seconds, yielding\n;;\t%q",
             toeval,(finish-start),value);
  return value;
}

static fdtype timed_evalx(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  double start=u8_elapsed_time();
  fdtype value=fd_eval(toeval,env);
  double finish=u8_elapsed_time();
  return fd_make_nvector(2,value,fd_init_double(NULL,finish-start));
}

/* Inserts a \n\t line break if the current output line is longer
    than max_len.  *off*, if > 0, is the last offset on the current line,
    which is where the line break goes if inserted;  */

static int check_line_length(u8_output out,int off,int max_len)
{
  u8_byte *start=out->u8_outbuf, *end=out->u8_write, *scanner=end;
  int scan_off, line_len, len=end-start;
  while ((scanner>start)&&((*scanner)!='\n')) scanner--;
  line_len=end-scanner; scan_off=scanner-start;
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
      start=out->u8_outbuf; end=out->u8_write;
      scanner=start+scan_off;}
    /* Use memmove because it's overlapping */
    memmove(start+off+2,start+off,len-off);
    start[off]='\n'; start[off+1]='\t';
    out->u8_write=out->u8_write+2;
    start[len+2]='\0';
    return -1;}
}

static fdtype watchcall(fdtype expr,fd_lispenv env,int with_proc)
{
  struct U8_OUTPUT out;
  u8_string dflt_label="%CALL", label=dflt_label, arglabel="%ARG";
  fdtype watch, head=fd_get_arg(expr,1), *rail, result=FD_EMPTY_CHOICE;
  int i=0, n_args, expr_len=fd_seq_length(expr);
  if (FD_VOIDP(head))
    return fd_err(fd_SyntaxError,"%WATCHCALL",NULL,expr);
  else if (FD_STRINGP(head)) {
    if (expr_len==2)
      return fd_err(fd_SyntaxError,"%WATCHCALL",NULL,expr);
    else {
      label=u8_strdup(FD_STRDATA(head));
      arglabel=u8_mkstring("%s/ARG",FD_STRDATA(head));
      if ((expr_len>3)||(FD_APPLICABLEP(fd_get_arg(expr,2)))) {
        watch=fd_get_body(expr,2);}
      else watch=fd_get_arg(expr,2);}}
  else if ((expr_len==2)&&(FD_PAIRP(head)))
    watch=head;
  else watch=fd_get_body(expr,1);
  n_args=fd_seq_length(watch);
  rail=u8_alloc_n(n_args,fdtype);
  U8_INIT_OUTPUT(&out,1024);
  u8_printf(&out,"Watched call %q",watch);
  u8_logger(-10,label,out.u8_outbuf);
  out.u8_write=out.u8_outbuf;
  while (i<n_args) {
    fdtype arg=fd_get_arg(watch,i);
    fdtype val=fd_eval(arg,env);
    val=fd_simplify_choice(val);
    if (FD_ABORTED(val)) {
      u8_string errstring=fd_errstring(NULL);
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
      out.u8_write=out.u8_outbuf;}
    else {
      u8_printf(&out,"%q",arg);
      u8_logger(-10,arglabel,out.u8_outbuf);
      out.u8_write=out.u8_outbuf;}
    rail[i++]=val;}
  if (FD_CHOICEP(rail[0])) {
    FD_DO_CHOICES(fn,rail[0]) {
      fdtype r=fd_apply(fn,n_args-1,rail+1);
      if (FD_ABORTED(r)) {
        u8_string errstring=fd_errstring(NULL);
        i--; while (i>=0) {fd_decref(rail[i]); i--;}
        u8_free(rail);
        fd_decref(result);
        if (label!=dflt_label) {u8_free(label); u8_free(arglabel);}
        u8_free(out.u8_outbuf); u8_free(errstring);
        return r;}
      else {FD_ADD_TO_CHOICE(result,r);}}}
  else result=fd_apply(rail[0],n_args-1,rail+1);
  if (FD_ABORTED(result)) {
    u8_string errstring=fd_errstring(NULL);
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

static fdtype watchcall_handler(fdtype expr,fd_lispenv env)
{
  return watchcall(expr,env,0);
}
static fdtype watchcall_plus_handler(fdtype expr,fd_lispenv env)
{
  return watchcall(expr,env,1);
}

static fdtype watched_eval(fdtype expr,fd_lispenv env)
{
  fdtype toeval=fd_get_arg(expr,1);
  double start; int oneout=0;
  fdtype scan=FD_CDR(expr);
  u8_string label="%WATCH";
  if ((FD_PAIRP(toeval))) {
    /* EXPR "label" . watchexprs */
    scan=FD_CDR(scan);
    if ((FD_PAIRP(scan)) && (FD_STRINGP(FD_CAR(scan)))) {
      label=FD_STRDATA(FD_CAR(scan)); scan=FD_CDR(scan);}}
  else if (FD_STRINGP(toeval)) {
    /* "label" . watchexprs, no expr, call should be side-effect */
    label=FD_STRDATA(toeval); scan=FD_CDR(scan);}
  else if (FD_SYMBOLP(toeval)) {
    /* If the first argument is a symbol, we change the label and
       treat all the arguments as context variables and output
       them.  */
    label="%WATCHED";}
  else scan=FD_CDR(scan);
  if (FD_PAIRP(scan)) {
    int off=0; struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
    if (FD_PAIRP(toeval)) {
      u8_printf(&out,"Context for %q: ",toeval);
      off=check_line_length(&out,off,50);}
    /* A watched expr can be just a symbol or pair, which is output as:
         <expr>=<value>
       or a series of "<label>" <expr>, which is output as:
         label=<value> */
    while (FD_PAIRP(scan)) {
      /* A watched expr can be just a symbol or pair, which is output as:
           <expr>=<value>
         or a series of "<label>" <expr>, which is output as:
           label=<value> */
      fdtype towatch=FD_CAR(scan), wval=FD_VOID;
      if ((FD_STRINGP(towatch)) && (FD_PAIRP(FD_CDR(scan)))) {
        fdtype label=towatch; u8_string lbl=FD_STRDATA(label);
        towatch=FD_CAR(FD_CDR(scan)); scan=FD_CDR(FD_CDR(scan));
        wval=((FD_SYMBOLP(towatch))?(fd_symeval(towatch,env)):
              (fd_eval(towatch,env)));
        if (lbl[0]=='\n') {
          if (oneout) {
            if (off>0) u8_printf(&out,"\n  // %s=",lbl+1);
            else u8_printf(&out," // %s=",lbl+1);}
          else oneout=1;
          fd_pprint(&out,wval,"   ",0,3,100,0);
          if (FD_PAIRP(scan)) {
            u8_puts(&out,"\n"); off=0;}}
        else {
          if (oneout) u8_puts(&out," // "); else oneout=1;
          u8_printf(&out,"%s=%q",FD_STRDATA(label),wval);
          off=check_line_length(&out,off,100);}
        fd_decref(wval); wval=FD_VOID;}
      else {
        wval=((FD_SYMBOLP(towatch))?(fd_symeval(towatch,env)):
              (fd_eval(towatch,env)));
        scan=FD_CDR(scan);
        if (oneout) u8_printf(&out," // %q=%q",towatch,wval);
        else {
          u8_printf(&out,"%q=%q",towatch,wval);
          oneout=1;}
        off=check_line_length(&out,off,50);
        fd_decref(wval); wval=FD_VOID;}}
    u8_logger(-10,label,out.u8_outbuf);
    u8_free(out.u8_outbuf);}
  start=u8_elapsed_time();
  if (FD_SYMBOLP(toeval))
    return fd_eval(toeval,env);
  else if (FD_STRINGP(toeval)) {
    /* This is probably the 'side effect' form of %WATCH, so
       we don't bother 'timing' it. */
    return fd_incref(toeval);}
  else {
    fdtype value=fd_eval(toeval,env);
    double howlong=u8_elapsed_time()-start;
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

/* The opcode evaluator */

#include "opcodes.c"

/* The evaluator itself */

static fdtype apply_function(fdtype fn,fdtype expr,fd_lispenv env);
static fdtype apply_functions(fdtype fn,fdtype expr,fd_lispenv env);

FD_EXPORT fdtype fd_tail_eval(fdtype expr,fd_lispenv env)
{
  switch (FD_PTR_TYPE(expr)) {
  case fd_symbol_type: {
    fdtype val=fd_symeval(expr,env);
    if (FD_EXPECT_FALSE(FD_VOIDP(val)))
      return fd_err(fd_UnboundIdentifier,"fd_eval",
                    FD_SYMBOL_NAME(expr),expr);
    else return val;}
  case fd_pair_type: {
    fdtype head=(FD_CAR(expr));
    if (FD_OPCODEP(head))
      return opcode_dispatch(head,expr,env);
    else if (head == quote_symbol)
      if (FD_PAIRP(expr))
        return fd_refcar(FD_CDR(expr));
      else {
        fdtype v=FD_RAIL_REF(expr,1); fd_incref(v);
        return v;}
    else if (head == comment_symbol)
      return FD_VOID;
    else {
      fdtype result=FD_VOID;
      fdtype headval = (FD_FCNIDP(head))?
        (fd_fcnid_ref(head)) :
        (fasteval(head,env));
      int gchead=((FD_CONSP(head))||(FD_SYMBOLP(head)));
      int headtype=FD_PTR_TYPE(headval);
      if (fd_applyfns[headtype])
        result=apply_function(headval,expr,env);
      else if (FD_TYPEP(headval,fd_specform_type)) {
        /* These are special forms which do all the evaluating themselves */
        struct FD_SPECIAL_FORM *handler=(fd_special_form)headval;
        /* fd_calltrack_call(handler->name); */
        /* fd_calltrack_return(handler->name); */
        result=handler->fexpr_handler(expr,env);}
      else if (FD_TYPEP(headval,fd_macro_type)) {
        /* These are special forms which do all the evaluating themselves */
        struct FD_MACRO *macrofn=
          fd_consptr(struct FD_MACRO *,headval,fd_macro_type);
        fdtype xformer=macrofn->fd_macro_transformer;
        int xformer_type=FD_PRIM_TYPE(xformer);
        if (fd_applyfns[xformer_type]) {
          fdtype new_expr=(fd_applyfns[xformer_type])(xformer,1,&expr);
          new_expr=fd_finish_call(new_expr);
          if (FD_ABORTED(new_expr))
            result=fd_err(fd_SyntaxError,_("macro expansion"),NULL,new_expr);
          else result=fd_eval(new_expr,env);
          fd_decref(new_expr);}
        else result=fd_err(fd_InvalidMacro,NULL,macrofn->fd_macro_name,expr);}
      else if ((FD_CHOICEP(headval)) || (FD_ACHOICEP(headval))) {
        int applicable=-1;
        FD_DO_CHOICES(hv,headval) {
          int hvtype=FD_PRIM_TYPE(hv);
          /* Check that all the elements are either applicable or special
             forms  */
          if (fd_applyfns[hvtype])
            if (applicable<0) applicable=1;
            else if (applicable) {}
            else result=fd_err
                   ("Inconsistent NDCALL","fd_tail_eval",NULL,headval);
          else if (hvtype==fd_specform_type)
            if (applicable<0) applicable=0;
            else if (applicable)
              result=fd_err("Inconsistent NDCALL","fd_tail_eval",NULL,headval);
          /* In this case, all the headvals so far are special forms */
            else {}
          else result=fd_err("Inconsistent NDCALL","fd_tail_eval",NULL,headval);}
        if (FD_ABORTED(result)) {}
        else if (applicable<0)
          result=fd_err("Inconsistent NDCALL","fd_tail_eval",NULL,headval);
        else if (applicable)
          result=apply_functions(headval,expr,env);
        else {
          fdtype results=FD_EMPTY_CHOICE;
          FD_DO_CHOICES(hv,headval) {
            struct FD_SPECIAL_FORM *handler=(fd_special_form)hv;
            /* fd_calltrack_call(handler->name); */
            /* fd_calltrack_return(handler->name); */
            fdtype one_result=handler->fexpr_handler(expr,env);
            if (FD_ABORTED(one_result)) {
              fd_decref(results);
              result=one_result;}
            else {FD_ADD_TO_CHOICE(results,result);}}
          result=results;}}
      else if (FD_EXPECT_FALSE(FD_VOIDP(headval)))
        result=fd_err(fd_UnboundIdentifier,"for function",
                      ((FD_SYMBOLP(head))?(FD_SYMBOL_NAME(head)):(NULL)),
                      head);
      else if (FD_ABORTED(headval))
        result=fd_incref(headval);
      else if (FD_EMPTY_CHOICEP(headval))
        result=FD_EMPTY_CHOICE;
      else result=fd_err(fd_NotAFunction,NULL,NULL,headval);
      if (FD_THROWP(result)) {}
      else if (FD_ABORTED(result)) {
        fd_push_error_context(fd_eval_context,fd_incref(expr));
        return result;}
      if (gchead) fd_decref(headval);
      return result;}}
  case fd_rail_type:
    return fd_rail_eval(FD_RAIL_LENGTH(expr)-1,FD_RAIL_DATA(expr)+1,env);
  case fd_slotmap_type:
    return fd_deep_copy(expr);
  case fd_choice_type: case fd_achoice_type: {
    fdtype exprs=fd_simplify_choice(expr);
    if (FD_CHOICEP(exprs)) {
      fdtype result=FD_EMPTY_CHOICE;
      FD_DO_CHOICES(each_expr,expr) {
        fdtype r=fd_eval(each_expr,env);
        if (FD_ABORTED(r)) {
          fd_decref(result);
          return r;}
        else {FD_ADD_TO_CHOICE(result,r);}}
      return result;}
    else return fd_tail_eval(exprs,env);}
  default:
    if (FD_TYPEP(expr,fd_lexref_type))
      return fd_lexref(expr,env);
    else return fd_incref(expr);}
}

FD_EXPORT fdtype _fd_eval(fdtype expr,fd_lispenv env)
{
  fdtype result=fd_tail_eval(expr,env);
  return fd_finish_call(result);
}

static fdtype apply_functions(fdtype fns,fdtype expr,fd_lispenv env)
{
  int n_args=0, i=0, gc_args=0;
  fdtype _argv[FD_STACK_ARGS], *argv, arglist=FD_CDR(expr);
  fdtype results=FD_EMPTY_CHOICE;
  {FD_DOLIST(elt,arglist)
      if (!((FD_PAIRP(elt)) && (FD_EQ(FD_CAR(elt),comment_symbol))))
        n_args++;}
  if (n_args<FD_STACK_ARGS) argv=_argv;
  else argv=u8_alloc_n(n_args,fdtype);
  {FD_DOLIST(arg,arglist) {
      fdtype argval;
      if (FD_EXPECT_FALSE
          ((FD_PAIRP(arg)) &&(FD_EQ(FD_CAR(arg),comment_symbol)))) continue;
      else argval=fasteval(arg,env);
      if (FD_ABORTED(argval)) {
        int j=0; while (j<i) {fd_decref(argv[j]); j++;}
        if (argv!=_argv) u8_free(argv);
        return argval;}
      argv[i++]=argval;
      if (FD_CONSP(argval)) gc_args=1;}}
  {FD_DO_CHOICES(fn,fns) {
      fdtype result=fd_apply(fn,n_args,argv);
      if (FD_ABORTED(result)) {
        if (gc_args) {
          int j=0; while (j<n_args) {fd_decref(argv[j]); j++;}}
        if (argv!=_argv) u8_free(argv);
        fd_decref(results);
        return result;}
      else {FD_ADD_TO_CHOICE(results,result);}}}
  if (gc_args) {
    int j=0; while (j<n_args) {fd_decref(argv[j]); j++;}}
  if (argv!=_argv) u8_free(argv);
  return results;
}

static fdtype apply_normal_function(fdtype fn,fdtype expr,fd_lispenv env)
{
  fdtype result=FD_VOID;
  struct FD_FUNCTION *fcn=(fd_function)fn;
  fdtype _argv[FD_STACK_ARGS], *argv;
  int arg_count=0, n_args=0, args_need_gc=0, free_argv=0;
  int nd_args=0, prune=0, nd_prim=fcn->fcn_ndcall;
  int max_arity=fcn->fcn_arity, min_arity=fcn->fcn_min_arity;
  int n_params=max_arity, argv_length=max_arity;
  /* First, count the arguments */
  {fdtype body=fd_get_body(expr,1);
    FD_DOLIST(arg,body) {
      if (!((FD_PAIRP(arg)) && (FD_EQ(FD_CAR(arg),comment_symbol))))
        n_args++;}}
    /* If the max_arity is less than zero, it's a lexpr, so the
       number of args is the number of args. */
  if (max_arity<0) argv_length=n_args;
  /* Check if there are too many arguments. */
  else if (FD_EXPECT_FALSE(n_args>max_arity))
    return fd_err(fd_TooManyArgs,"apply_function",fcn->fcn_name,expr);
  else if (FD_EXPECT_FALSE((min_arity>=0) && (n_args<min_arity)))
    return fd_err(fd_TooFewArgs,"apply_function",fcn->fcn_name,expr);
  if (argv_length>FD_STACK_ARGS) {
    /* If there are more than _FD_STACK_ARGS, malloc a vector for them. */
    argv=u8_alloc_n(argv_length,fdtype);
    free_argv=1;}
  /* Otherwise, just use the stack vector */
  else argv=_argv;
  /* Now we evaluate each of the subexpressions to fill the arg vector */
  {fdtype arg_exprs=fd_get_body(expr,1);
    FD_DOLIST(elt,arg_exprs) {
      fdtype argval;
      if ((FD_PAIRP(elt)) && (FD_EQ(FD_CAR(elt),comment_symbol)))
        continue;
      argval=fasteval(elt,env);
      if (FD_ABORTED(argval)) {
        /* Break if one of the arguments returns an error */
        result=argval; break;}
      if (FD_VOIDP(argval)) {
        result=fd_err(fd_VoidArgument,"apply_function",fcn->fcn_name,elt);
        break;}
      if ((FD_EMPTY_CHOICEP(argval)) && (!(nd_prim))) {
        /* Break if the method isn't non-deterministic and one of the
           arguments returns an empty choice. */
        prune=1; break;}
      /* Convert the argument to a simple choice (or non choice) */
      argval=fd_simplify_choice(argval);
      if (FD_CONSP(argval)) args_need_gc=1;
      /* Keep track of whether there are non-deterministic args
         which would require calling fd_ndapply. */
      if (FD_CHOICEP(argval)) nd_args=1;
      else if (FD_QCHOICEP(argval)) nd_args=1;
      /* Fill in the slot */
      argv[arg_count++]=argval;}}
  /* Now, check if we need to exit, either because some argument returned
     an error (or throw) or because the call is being pruned. */
  if ((prune) || (FD_ABORTED(result))) {
    /* In this case, we won't call the procedure at all, because
       either a parameter produced an error or because we can prune
       the call because some parameter is an empty choice. */
    int i=0;
    if (args_need_gc) while (i<arg_count) {
        /* Clean up the arguments we've already evaluated */
        fdtype arg=argv[i++]; fd_decref(arg);}
    if (free_argv) u8_free(argv);
    if (FD_ABORTED(result))
      /* This could extend the backtrace */
      return result;
    else return FD_EMPTY_CHOICE;}
  if ((n_params<0) || (fcn->fcn_xcall)) {}
  /* Don't fill anything in for lexprs or non primitives.  */
  else if (arg_count != argv_length) {
    if (fcn->fcn_defaults)
      while (arg_count<argv_length) {
        argv[arg_count]=fd_incref(fcn->fcn_defaults[arg_count]); arg_count++;}
    else while (arg_count<argv_length) argv[arg_count++]=FD_VOID;}
  else {}
  if ((fd_optimize_tail_calls) && (FD_SPROCP(fn)))
    result=fd_tail_call(fn,arg_count,argv);
  else if ((nd_prim==0) && (nd_args))
    result=fd_ndapply(fn,arg_count,argv);
  else {
    result=fd_dapply(fn,arg_count,argv);}
  if ((FD_ABORTED(result)) &&
      (!(FD_THROWP(result))) &&
      (!(FD_SPROCP(fn)))) {
    /* If it's not an sproc, we add an entry to the backtrace
       that shows the arguments, since they probably don't show
       up in an environment on the backtrace. */
    fdtype *avec=u8_alloc_n((arg_count+1),fdtype), call_context;
    memcpy(avec+1,argv,sizeof(fdtype)*(arg_count));
    if (fcn->fcn_filename)
      if (fcn->fcn_name)
        avec[0]=fd_conspair(fd_intern(fcn->fcn_name),
                            fdtype_string(fcn->fcn_filename));
      else avec[0]=
             fd_conspair(fd_intern("LAMBDA"),
                         fdtype_string(fcn->fcn_filename));
    else if (fcn->fcn_name) avec[0]=fd_intern(fcn->fcn_name);
    else avec[0]=fd_intern("LAMBDA");
    if (free_argv) u8_free(argv);
    call_context=fd_init_vector(NULL,arg_count+1,avec);
    fd_push_error_context(fd_apply_context,call_context);
    return result;}
  else if (args_need_gc) {
    int i=0; while (i < arg_count) {
      fdtype arg=argv[i++]; fd_decref(arg);}}
  if (free_argv) u8_free(argv);
  return result;
}

static fdtype apply_weird_function(fdtype fn,fdtype expr,fd_lispenv env)
{
  fdtype result=FD_VOID;
  fdtype _argv[FD_STACK_ARGS], *argv;
  int i=0, n_args=0, args_need_gc=0, free_argv=0;
  fdtype arg_exprs=fd_get_body(expr,1);
  {FD_DOLIST(elt,arg_exprs) {
      if (!((FD_PAIRP(elt)) && (FD_EQ(FD_CAR(elt),comment_symbol))))
        n_args++;}}
  if (n_args>FD_STACK_ARGS) {
    /* If there are more than _FD_STACK_ARGS, malloc a vector for them. */
    argv=u8_alloc_n(n_args,fdtype);
    free_argv=1;}
  /* Otherwise, just use the stack vector */
  else argv=_argv;
  {FD_DOLIST(arg,arg_exprs)
      if (!((FD_PAIRP(arg)) && (FD_EQ(FD_CAR(arg),comment_symbol)))) {
        fdtype argval=fd_eval(arg,env);
        if ((FD_ABORTED(argval)) || (FD_EMPTY_CHOICEP(argval)) ||
            (FD_VOIDP(argval))) {
          if (args_need_gc) {
            int j=0; while (j<i) {fdtype arg=argv[j++]; fd_decref(arg);}}
          if (free_argv) u8_free(argv);
          if (FD_VOIDP(argval)) {
            fd_seterr(fd_VoidArgument,"apply_weird_function",NULL,expr);
            return FD_ERROR_VALUE;}
          else return argval;}
        else {
          argv[i++]=argval; if (FD_CONSP(argval)) args_need_gc=1;}}}
  result=fd_apply(fn,n_args,argv);
  if (args_need_gc) {
    i=0; while (i<n_args) {
      fdtype argval=argv[i++]; fd_decref(argval);}}
  if (free_argv) u8_free(argv);
  return result;
}

static fdtype apply_function(fdtype fn,fdtype expr,fd_lispenv env)
{
  fd_ptr_type fntype=FD_PRIM_TYPE(fn);
  if (fd_functionp[fntype])
    return apply_normal_function(fn,expr,env);
  else return apply_weird_function(fn,expr,env);
}

FD_EXPORT fdtype fd_eval_exprs(fdtype exprs,fd_lispenv env)
{
  if (FD_PAIRP(exprs)) {
    fdtype next=FD_CDR(exprs), val=FD_VOID;
    while (FD_PAIRP(exprs)) {
      fd_decref(val); val=FD_VOID;
      if (FD_EMPTY_LISTP(next))
        return fd_tail_eval(FD_CAR(exprs),env);
      else {
        val=fd_eval(FD_CAR(exprs),env);
        if (FD_ABORTED(val)) return val;
        else exprs=next;
        if (FD_PAIRP(exprs)) next=FD_CDR(exprs);}}
    return val;}
  else if (FD_RAILP(exprs)) {
    struct FD_VECTOR *v=fd_consptr(fd_vector,exprs,fd_rail_type);
    int len=v->fd_veclen; fdtype *elts=v->fd_vecelts, val=FD_VOID;
    int i=0; while (i<len) {
      fdtype expr=elts[i++];
      fd_decref(val); val=FD_VOID;
      if (i==len)
        return fd_eval(expr,env);
      else val=fd_eval(expr,env);
      if (FD_ABORTED(val)) return val;}
    return val;}
  else return FD_VOID;
}

/* Module system */

static struct FD_HASHTABLE module_map, safe_module_map;
static fd_lispenv default_env=NULL, safe_default_env=NULL;

FD_EXPORT fd_lispenv fd_make_env(fdtype bindings,fd_lispenv parent)
{
  if (FD_EXPECT_FALSE(!(FD_TABLEP(bindings)) )) {
    fd_seterr(fd_TypeError,"fd_make_env",
              u8_mkstring(_("object is not a %m"),"table"),
              bindings);
    return NULL;}
  else {
    struct FD_ENVIRONMENT *e=u8_alloc(struct FD_ENVIRONMENT);
    FD_INIT_FRESH_CONS(e,fd_environment_type);
    e->env_bindings=bindings; e->env_exports=FD_VOID;
    e->env_parent=fd_copy_env(parent);
    e->env_copy=e;
    return e;}
}


FD_EXPORT
/* fd_make_export_env:
    Arguments: a hashtable and an environment
    Returns: a consed environment whose bindings and exports
  are the exports table.  This indicates that the environment
  is "for export only" and cannot be modified. */
fd_lispenv fd_make_export_env(fdtype exports,fd_lispenv parent)
{
  if (FD_EXPECT_FALSE(!(FD_HASHTABLEP(exports)) )) {
    fd_seterr(fd_TypeError,"fd_make_env",
              u8_mkstring(_("object is not a %m"),"hashtable"),
              exports);
    return NULL;}
  else {
    struct FD_ENVIRONMENT *e=u8_alloc(struct FD_ENVIRONMENT);
    FD_INIT_FRESH_CONS(e,fd_environment_type);
    e->env_bindings=fd_incref(exports);
    e->env_exports=fd_incref(e->env_bindings);
    e->env_parent=fd_copy_env(parent);
    e->env_copy=e;
    return e;}
}

FD_EXPORT fd_lispenv fd_new_environment(fdtype bindings,int safe)
{
  if (fdscheme_initialized==0) fd_init_fdscheme();
  if (FD_VOIDP(bindings))
    bindings=fd_make_hashtable(NULL,17);
  else fd_incref(bindings);
  return fd_make_env(bindings,((safe)?(safe_default_env):(default_env)));
}
FD_EXPORT fd_lispenv fd_working_environment()
{
  if (fdscheme_initialized==0) fd_init_fdscheme();
  return fd_make_env(fd_make_hashtable(NULL,17),default_env);
}
FD_EXPORT fd_lispenv fd_safe_working_environment()
{
  if (fdscheme_initialized==0) fd_init_fdscheme();
  return fd_make_env(fd_make_hashtable(NULL,17),safe_default_env);
}

FD_EXPORT fdtype fd_register_module_x(fdtype name,fdtype module,int flags)
{
  if (flags&FD_MODULE_SAFE)
    fd_hashtable_store(&safe_module_map,name,module);
  else fd_hashtable_store(&module_map,name,module);

  /* Set the module ID*/
  if (FD_ENVIRONMENTP(module)) {
    fd_environment env=(fd_environment)module;
    fd_add(env->env_bindings,moduleid_symbol,name);}
  else if (FD_HASHTABLEP(module))
    fd_add(module,moduleid_symbol,name);
  else {}

  /* Add to the appropriate default environment */
  if (flags&FD_MODULE_DEFAULT) {
    fd_lispenv scan;
    if (flags&FD_MODULE_SAFE) {
      scan=safe_default_env;
      while (scan)
        if (FD_EQ(scan->env_bindings,module))
          /* It's okay to return now, because if it's in the safe module
             defaults it's also in the risky module defaults. */
          return module;
        else scan=scan->env_parent;
      safe_default_env->env_parent=
        fd_make_env(fd_incref(module),safe_default_env->env_parent);}
    scan=default_env;
    while (scan)
      if (FD_EQ(scan->env_bindings,module)) return module;
      else scan=scan->env_parent;
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
  if (fdscheme_initialized==0) fd_init_fdscheme();
  module_name=fd_intern(name);
  module=fd_make_hashtable(NULL,0);
  fd_add(module,moduleid_symbol,module_name);
  if (flags&FD_MODULE_SAFE) {
    fd_hashtable_op
      (&safe_module_map,fd_table_default,module_name,module);
    as_stored=fd_get((fdtype)&safe_module_map,module_name,FD_VOID);}
  else {
    fd_hashtable_op
      (&module_map,fd_table_default,module_name,module);
    as_stored=fd_get((fdtype)&module_map,module_name,FD_VOID);}
  if (!(FD_EQ(module,as_stored))) {
    fd_decref(module);
    return as_stored;}
  else fd_decref(as_stored);
  if (flags&FD_MODULE_DEFAULT) {
    if (flags&FD_MODULE_SAFE)
      safe_default_env->env_parent=fd_make_env(module,safe_default_env->env_parent);
    default_env->env_parent=fd_make_env(module,default_env->env_parent);}
  return module;
}

FD_EXPORT fdtype fd_get_module(fdtype name,int safe)
{
  if (safe)
    return fd_hashtable_get(&safe_module_map,name,FD_VOID);
  else {
    fdtype module=fd_hashtable_get(&module_map,name,FD_VOID);
    if (FD_VOIDP(module))
      return fd_hashtable_get(&safe_module_map,name,FD_VOID);
    else return module;}
}

FD_EXPORT int fd_discard_module(fdtype name,int safe)
{
  if (safe)
    return fd_hashtable_store(&safe_module_map,name,FD_VOID);
  else {
    fdtype module=fd_hashtable_get(&module_map,name,FD_VOID);
    if (FD_VOIDP(module))
      return fd_hashtable_store(&safe_module_map,name,FD_VOID);
    else {
      fd_decref(module);
      return fd_hashtable_store(&module_map,name,FD_VOID);}}
}

/* Making some functions */

FD_EXPORT fdtype fd_make_special_form(u8_string name,fd_evalfn fn)
{
  struct FD_SPECIAL_FORM *f=u8_alloc(struct FD_SPECIAL_FORM);
  FD_INIT_CONS(f,fd_specform_type);
  f->fexpr_name=u8_strdup(name); 
  f->fexpr_filename=NULL; 
  f->fexpr_handler=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT void fd_defspecial(fdtype mod,u8_string name,fd_evalfn fn)
{
  struct FD_SPECIAL_FORM *f=u8_alloc(struct FD_SPECIAL_FORM);
  FD_INIT_CONS(f,fd_specform_type);
  f->fexpr_name=u8_strdup(name); 
  f->fexpr_handler=fn; 
  f->fexpr_filename=NULL;
  fd_store(mod,fd_intern(name),FDTYPE_CONS(f));
  fd_decref(FDTYPE_CONS(f));
}

/* The Evaluator */

static fdtype eval_handler(fdtype x,fd_lispenv env)
{
  fdtype expr_expr=fd_get_arg(x,1);
  fdtype expr=fd_eval(expr_expr,env);
  fdtype result=fd_eval(expr,env);
  fd_decref(expr);
  return result;
}

static fdtype boundp_handler(fdtype expr,fd_lispenv env)
{
  fdtype symbol=fd_get_arg(expr,1);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_handler",NULL,fd_incref(expr));
  else {
    fdtype val=fd_symeval(symbol,env);
    if ((FD_VOIDP(val))||(val==FD_DEFAULT_VALUE))
      return FD_FALSE;
    else if (val == FD_UNBOUND) 
      return FD_FALSE;
    else {
      fd_decref(val); 
      return FD_TRUE;}}
}

static fdtype modref_handler(fdtype expr,fd_lispenv env)
{
  fdtype module=fd_get_arg(expr,1);
  fdtype symbol=fd_get_arg(expr,2);
  if ((FD_VOIDP(module))||(FD_VOIDP(module)))
    return fd_err(fd_SyntaxError,"modref_handler",NULL,fd_incref(expr));
  else if (!(FD_HASHTABLEP(module)))
    return fd_type_error("module hashtable","modref_handler",module);
  else if (!(FD_SYMBOLP(symbol)))
    return fd_type_error("symbol","modref_handler",symbol);
  else return fd_hashtable_get((fd_hashtable)module,symbol,FD_UNBOUND);
}

static fdtype default_handler(fdtype expr,fd_lispenv env)
{
  fdtype symbol=fd_get_arg(expr,1);
  fdtype default_expr=fd_get_arg(expr,2);
  if (!(FD_SYMBOLP(symbol)))
    return fd_err(fd_SyntaxError,"boundp_handler",NULL,fd_incref(expr));
  else if (FD_VOIDP(default_expr))
    return fd_err(fd_SyntaxError,"boundp_handler",NULL,fd_incref(expr));
  else {
    fdtype val=fd_symeval(symbol,env);
    if (FD_VOIDP(val))
      return fd_eval(default_expr,env);
    else if (val == FD_UNBOUND)
      return fd_eval(default_expr,env);
    else return val;}
}

static fdtype voidp_handler(fdtype expr,fd_lispenv env)
{
  fdtype result=fd_eval(fd_get_arg(expr,1),env);
  if (FD_VOIDP(result)) return FD_TRUE;
  else {
    fd_decref(result);
    return FD_FALSE;}
}

static fdtype env_handler(fdtype expr,fd_lispenv env)
{
  return (fdtype)fd_copy_env(env);
}

static fdtype symbol_boundp_prim(fdtype symbol,fdtype envarg)
{
  if (!(FD_SYMBOLP(symbol)))
    return fd_type_error(_("symbol"),"boundp_prim",symbol);
  else if (FD_ENVIRONMENTP(envarg)) {
    fd_lispenv env=(fd_lispenv)envarg;
    fdtype val=fd_symeval(symbol,env);
    if (FD_VOIDP(val)) return FD_FALSE;
    else if (val == FD_DEFAULT_VALUE) return FD_FALSE;
    else if (val == FD_UNBOUND) return FD_FALSE;
    else return FD_TRUE;}
  else if (FD_TABLEP(envarg)) {
    fdtype val=fd_get(envarg,symbol,FD_VOID);
    if (FD_VOIDP(val)) return FD_FALSE;
    else return FD_TRUE;}
  else return fd_type_error(_("environment"),"symbol_boundp_prim",envarg);
}

static fdtype environmentp_prim(fdtype arg)
{
  if (FD_ENVIRONMENTP(arg))
    return FD_TRUE;
  else return FD_FALSE;
}

/* Withenv forms */

static fdtype withenv(fdtype expr,fd_lispenv env,
                      fd_lispenv consed_env,u8_context cxt)
{
  fdtype bindings=fd_get_arg(expr,1);
  if (FD_VOIDP(bindings))
    return fd_err(fd_TooFewExpressions,cxt,NULL,expr);
  else if ((FD_EMPTY_LISTP(bindings))||(FD_FALSEP(bindings))) {}
  else if (FD_PAIRP(bindings)) {
    FD_DOLIST(varval,bindings) {
      if ((FD_PAIRP(varval))&&(FD_SYMBOLP(FD_CAR(varval)))&&
          (FD_PAIRP(FD_CDR(varval)))&&
          (FD_EMPTY_LISTP(FD_CDR(FD_CDR(varval))))) {
        fdtype var=FD_CAR(varval), val=fd_eval(FD_CADR(varval),env);
        if (FD_ABORTED(val)) return FD_ERROR_VALUE;
        fd_bind_value(var,val,consed_env);
        fd_decref(val);}
      else return fd_err(fd_SyntaxError,cxt,NULL,expr);}}
  else if (FD_TABLEP(bindings)) {
    fdtype keys=fd_getkeys(bindings);
    FD_DO_CHOICES(key,keys) {
      if (FD_SYMBOLP(key)) {
        fdtype value=fd_get(bindings,key,FD_VOID);
        if (!(FD_VOIDP(value)))
          fd_bind_value(key,value,consed_env);
        fd_decref(value);}
      else {
        FD_STOP_DO_CHOICES;
        fd_recycle_environment(consed_env);
        return fd_err(fd_SyntaxError,cxt,NULL,expr);}
      fd_decref(keys);}}
  else return fd_err(fd_SyntaxError,cxt,NULL,expr);
  /* Execute the body */ {
    fdtype result=FD_VOID;
    fdtype body=fd_get_body(expr,2);
    FD_DOLIST(elt,body) {
      fd_decref(result);
      result=fd_eval(elt,consed_env);
      if (FD_ABORTED(result)) {
        return result;}}
    return result;}
}

static fdtype withenv_handler(fdtype expr,fd_lispenv env)
{
  fd_lispenv consed_env=fd_working_environment();
  fdtype result=withenv(expr,env,consed_env,"WITHENV");
  fd_recycle_environment(consed_env);
  return result;
}

static fdtype withenv_safe_handler(fdtype expr,fd_lispenv env)
{
  fd_lispenv consed_env=fd_safe_working_environment();
  fdtype result=withenv(expr,env,consed_env,"WITHENV/SAFE");
  fd_recycle_environment(consed_env);
  return result;
}

/* Eval/apply related primitives */

static fdtype get_arg_prim(fdtype expr,fdtype elt,fdtype dflt)
{
  if (FD_PAIRP(expr))
    if (FD_FIXNUMP(elt)) {
      int i=0, lim=FD_FIX2INT(elt); fdtype scan=expr;
      while ((i<lim) && (FD_PAIRP(scan))) {
        scan=FD_CDR(scan); i++;}
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
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(fn,args[0]) {
      FD_DO_CHOICES(final_arg,args[n-1]) {
        fdtype result=FD_VOID;
        int final_length=fd_seq_length(final_arg);
        int n_args=(n-2)+final_length;
        fdtype *values=u8_alloc_n(n_args,fdtype);
        int i=1, j=0, lim=n-1;
        /* Copy regular arguments */
        while (i<lim) {values[j]=fd_incref(args[i]); j++; i++;}
        i=0; while (j<n_args) {
          values[j]=fd_seq_elt(final_arg,i); j++; i++;}
        result=fd_apply(fn,n_args,values);
        if (FD_ABORTED(result)) {
          fd_decref(results);
          FD_STOP_DO_CHOICES;
          i=0; while (i<n_args) {fd_decref(values[i]); i++;}
          u8_free(values);
          results=result;
          break;}
        else {FD_ADD_TO_CHOICE(results,result);}
        i=0; while (i<n_args) {fd_decref(values[i]); i++;}
        u8_free(values);}
      if (FD_ABORTED(results)) {
        FD_STOP_DO_CHOICES;
        return results;}}
    return results;
  }
}

/* Initialization */

fd_ptr_type fd_environment_type, fd_specform_type;

extern void fd_init_corefns_c(void);

static fdtype lispenv_get(fdtype e,fdtype s,fdtype d)
{
  fdtype result=fd_symeval(s,FD_XENV(e));
  if (FD_VOIDP(result)) return fd_incref(d);
  else return result;
}
static int lispenv_store(fdtype e,fdtype s,fdtype v)
{
  return fd_bind_value(s,v,FD_XENV(e));
}

/* Some datatype methods */

static int unparse_specform(u8_output out,fdtype x)
{
  struct FD_SPECIAL_FORM *s=
    fd_consptr(struct FD_SPECIAL_FORM *,x,fd_specform_type);
  u8_printf(out,"#<Special Form %s>",s->fexpr_name);
  return 1;
}
static int unparse_environment(u8_output out,fdtype x)
{
  struct FD_ENVIRONMENT *env=
    fd_consptr(struct FD_ENVIRONMENT *,x,fd_environment_type);
  if (FD_HASHTABLEP(env->env_bindings)) {
    fdtype ids=fd_get(env->env_bindings,moduleid_symbol,FD_EMPTY_CHOICE);
    fdtype mid=FD_VOID;
    FD_DO_CHOICES(id,ids) {
      if (FD_SYMBOLP(id)) mid=id;}
    if (FD_SYMBOLP(mid))
      u8_printf(out,"#<MODULE %q #!%x>",mid,(unsigned long)env);
    else u8_printf(out,"#<ENVIRONMENT #!%x>",(unsigned long)env);}
  else u8_printf(out,"#<ENVIRONMENT #!%x>",(unsigned long)env);
  return 1;
}

FD_EXPORT void recycle_specform(struct FD_RAW_CONS *c)
{
  struct FD_SPECIAL_FORM *sf=(struct FD_SPECIAL_FORM *)c;
  u8_free(sf->fexpr_name);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

/* Call/cc */

static fdtype call_continuation(struct FD_FUNCTION *f,fdtype arg)
{
  struct FD_CONTINUATION *cont=(struct FD_CONTINUATION *)f;
  if (cont->retval==FD_NULL)
    return fd_err(ExpiredThrow,"call_continuation",NULL,arg);
  else if (FD_VOIDP(cont->retval)) {
    cont->retval=fd_incref(arg);
    return FD_THROW_VALUE;}
  else return fd_err(DoubleThrow,"call_continuation",NULL,arg);
}

static fdtype callcc (fdtype proc)
{
  fdtype continuation, value;
  struct FD_CONTINUATION *f=u8_alloc(struct FD_CONTINUATION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name="continuation"; f->fcn_filename=NULL;
  f->fcn_ndcall=1; f->fcn_xcall=1; f->fcn_arity=1; f->fcn_min_arity=1;
  f->fcn_typeinfo=NULL; f->fcn_defaults=NULL;
  f->fcn_handler.xcall1=call_continuation; f->retval=FD_VOID;
  continuation=FDTYPE_CONS(f);
  value=fd_apply(proc,1,&continuation);
  if ((value==FD_THROW_VALUE) && (!(FD_VOIDP(f->retval)))) {
    fdtype retval=f->retval;
    f->retval=FD_NULL;
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

static fdtype with_threadcache_handler(fdtype expr,fd_lispenv env)
{
  struct FD_THREAD_CACHE *tc=fd_push_threadcache(NULL);
  fdtype value=FD_VOID;
  FD_DOLIST(each,FD_CDR(expr)) {
    fd_decref(value); value=FD_VOID; value=fd_eval(each,env);
    if (FD_ABORTED(value)) {
      fd_pop_threadcache(tc);
      return value;}}
  fd_pop_threadcache(tc);
  return value;
}

static fdtype using_threadcache_handler(fdtype expr,fd_lispenv env)
{
  struct FD_THREAD_CACHE *tc=fd_use_threadcache();
  fdtype value=FD_VOID;
  FD_DOLIST(each,FD_CDR(expr)) {
    fd_decref(value); value=FD_VOID; value=fd_eval(each,env);
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
    struct FD_THREAD_CACHE *tc=fd_use_threadcache();
    if (tc) return FD_TRUE;
    else return FD_FALSE;}
}

/* Making DTPROCs */

static fdtype make_dtproc(fdtype name,fdtype server,fdtype min_arity,fdtype arity,fdtype minsock,fdtype maxsock,fdtype initsock)
{
  fdtype result;
  if (FD_VOIDP(min_arity))
    result=fd_make_dtproc(FD_SYMBOL_NAME(name),FD_STRDATA(server),1,-1,-1,
                          FD_FIX2INT(minsock),FD_FIX2INT(maxsock),
                          FD_FIX2INT(initsock));
  else if (FD_VOIDP(arity))
    result=fd_make_dtproc
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
fd_ptr_type fd_stream_erver_type;

FD_EXPORT fdtype fd_open_bytstrerver(u8_string server,int bufsiz)
{
  struct FD_STREAM_ERVER *dts=u8_alloc(struct FD_STREAM_ERVER);
  u8_string server_addr; u8_socket socket;
  /* Start out by parsing the address */
  if ((*server)==':') {
    fdtype server_id=fd_config_get(server+1);
    if (FD_STRINGP(server_id))
      server_addr=u8_strdup(FD_STRDATA(server_id));
    else  {
      fd_seterr(ServerUndefined,"open_server",
                u8_strdup(dts->fd_serverid),server_id);
      u8_free(dts);
      return -1;}}
  else server_addr=u8_strdup(server);
  dts->fd_serverid=u8_strdup(server); 
  dts->fd_server_address=server_addr;
  /* Then try to connect, just to see if that works */
  socket=u8_connect_x(server,&(dts->fd_server_address));
  if (socket<0) {
    /* If connecting fails, signal an error rather than creating
       the dtserver connection pool. */
    u8_free(dts->fd_serverid); 
    u8_free(dts->fd_server_address); 
    u8_free(dts);
    return fd_err(fd_ConnectionFailed,"fd_open_bytstrerver",
                  u8_strdup(server),FD_VOID);}
  /* Otherwise, close the socket */
  else close(socket);
  /* And create a connection pool */
  dts->fd_connpool=u8_open_connpool(dts->fd_serverid,2,4,1);
  /* If creating the connection pool fails for some reason,
     cleanup and return an error value. */
  if (dts->fd_connpool==NULL) {
    u8_free(dts->fd_serverid); 
    u8_free(dts->fd_server_address);
    u8_free(dts);
    return FD_ERROR_VALUE;}
  /* Otherwise, returh a dtserver object */
  FD_INIT_CONS(dts,fd_stream_erver_type);
  return FDTYPE_CONS(dts);
}

static fdtype dteval(fdtype server,fdtype expr)
{
  if (FD_TYPEP(server,fd_stream_erver_type))  {
    struct FD_STREAM_ERVER *dtsrv=
      fd_consptr(fd_stream_erver,server,fd_stream_erver_type);
    return fd_dteval(dtsrv->fd_connpool,expr);}
  else if (FD_STRINGP(server)) {
    fdtype s=fd_open_bytstrerver(FD_STRDATA(server),-1);
    if (FD_ABORTED(s)) return s;
    else {
      fdtype result=fd_dteval(((fd_stream_erver)s)->fd_connpool,expr);
      fd_decref(s);
      return result;}}
  else return fd_type_error(_("server"),"dteval",server);
}

static fdtype dtcall(int n,fdtype *args)
{
  fdtype server; fdtype request=FD_EMPTY_LIST, result; int i=n-1;
  if (n<2) return fd_err(fd_SyntaxError,"dtcall",NULL,FD_VOID);
  if (FD_TYPEP(args[0],fd_stream_erver_type))
    server=fd_incref(args[0]);
  else if (FD_STRINGP(args[0])) server=fd_open_bytstrerver(FD_STRDATA(args[0]),-1);
  else return fd_type_error(_("server"),"eval/dtcall",args[0]);
  if (FD_ABORTED(server)) return server;
  while (i>=1) {
    fdtype param=args[i];
    if ((i>1) && ((FD_SYMBOLP(param)) || (FD_PAIRP(param))))
      request=fd_conspair(fd_make_list(2,quote_symbol,param),request);
    else request=fd_conspair(param,request);
    fd_incref(param); i--;}
  result=fd_dteval(((fd_stream_erver)server)->fd_connpool,request);
  fd_decref(request);
  fd_decref(server);
  return result;
}

static fdtype open_bytstrerver(fdtype server,fdtype bufsiz)
{
  return fd_open_bytstrerver(FD_STRDATA(server),((FD_VOIDP(bufsiz)) ? (-1) : (FD_FIX2INT(bufsiz))));
}

/* Test functions */

static fdtype applytest(int n,fdtype *args)
{
  if (n<2)
    return fd_err(fd_TooFewArgs,"applytest",NULL,FD_VOID);
  else if (FD_APPLICABLEP(args[1])) {
    fdtype value=fd_apply(args[1],n-2,args+2);
    if (FD_EQUAL(value,args[0])) {
      fd_decref(value);
      return FD_TRUE;}
    else {
      u8_string s=fd_dtype2string(args[0]);
      fdtype err=fd_err(TestFailed,"applytest",s,value);
      u8_free(s); fd_decref(value);
      return err;}}
  else if (n==2) {
    if (FD_EQUAL(args[1],args[0]))
      return FD_TRUE;
    else {
      u8_string s=fd_dtype2string(args[0]);
      fdtype err=fd_err(TestFailed,"applytest",s,args[1]);
      u8_free(s);
      return err;}}
  else if (FD_EQUAL(args[2],args[0]))
    return FD_TRUE;
  else {
    u8_string s=fd_dtype2string(args[0]);
    fdtype err=fd_err(TestFailed,"applytest",s,args[2]);
    u8_free(s);
    return err;}
}

static fdtype evaltest(fdtype expr,fd_lispenv env)
{
  fdtype testexpr=fd_get_arg(expr,2);
  fdtype expected=fd_eval(fd_get_arg(expr,1),env);
  if ((FD_VOIDP(testexpr)) || (FD_VOIDP(expected))) {
    fd_decref(expected);
    return fd_err(fd_SyntaxError,"evaltest",NULL,expr);}
  else {
    fdtype value=fd_eval(testexpr,env);
    if (FD_ABORTED(value)) {
      fd_decref(expected);
      return value;}
    else if (FD_EQUAL(value,expected)) {
      fd_decref(expected);
      fd_decref(value);
      return FD_TRUE;}
    else {
      u8_string s=fd_dtype2string(expected);
      fdtype err=fd_err(TestFailed,"applytest",s,value);
      u8_free(s); fd_decref(value); fd_decref(expected);
      return err;}}
}

/* Debugging assistance */

FD_EXPORT fdtype _fd_dbg(fdtype x)
{
  fdtype result=_fd_debug(x);
  return fd_incref(result);
}

void (*fd_dump_backtrace)(u8_string bt);

static fdtype dbg_prim(fdtype x,fdtype msg)
{
  if (FD_VOIDP(msg))
    u8_message("Debug %q",x);
  else if (FD_FALSEP(msg)) {}
  else if (FD_VOIDP(x))
    u8_message("Debug called");
  else u8_message("Debug (%q) %q",msg,x);
  return _fd_dbg(x);
}

static fdtype void_prim(int n,fdtype *args)
{
  return FD_VOID;
}

/* Initialization */

void fd_init_eval_c()
{
  struct FD_TABLEFNS *fns=u8_zalloc(struct FD_TABLEFNS);
  fns->get=lispenv_get; fns->store=lispenv_store;
  fns->add=NULL; fns->drop=NULL; fns->test=NULL;

  fd_type_names[fd_opcode_type]=_("opcode");
  fd_unparsers[fd_opcode_type]=unparse_opcode;
  fd_immediate_checkfns[fd_opcode_type]=validate_opcode;

  fd_environment_type=fd_register_cons_type(_("scheme environment"));
  fd_specform_type=fd_register_cons_type(_("scheme special form"));
  fd_stream_erver_type=fd_register_cons_type(_("DType server"));

  fd_tablefns[fd_environment_type]=fns;
  fd_copiers[fd_environment_type]=lisp_copy_environment;
  fd_recyclers[fd_environment_type]=recycle_environment;
  fd_recyclers[fd_specform_type]=recycle_specform;

  fd_unparsers[fd_environment_type]=unparse_environment;
  fd_unparsers[fd_specform_type]=unparse_specform;
  fd_unparsers[fd_lexref_type]=unparse_lexref;
  fd_type_names[fd_lexref_type]=_("lexref");

  quote_symbol=fd_intern("QUOTE");
  _fd_comment_symbol=comment_symbol=fd_intern("COMMENT");
  profile_symbol=fd_intern("%PROFILE");
  moduleid_symbol=fd_intern("%MODULEID");

  FD_INIT_STATIC_CONS(&module_map,fd_hashtable_type);
  fd_make_hashtable(&module_map,67);

  FD_INIT_STATIC_CONS(&safe_module_map,fd_hashtable_type);
  fd_make_hashtable(&safe_module_map,67);
}

static void init_scheme_module()
{
  fd_xscheme_module=fd_make_hashtable(NULL,71);
  fd_scheme_module=fd_make_hashtable(NULL,71);
  fd_register_module("SCHEME",fd_scheme_module,
                     (FD_MODULE_DEFAULT|FD_MODULE_SAFE));
  fd_register_module("XSCHEME",fd_xscheme_module,(FD_MODULE_DEFAULT));
}

static void init_localfns()
{
  fd_defspecial(fd_scheme_module,"EVAL",eval_handler);
  fd_defspecial(fd_scheme_module,"BOUND?",boundp_handler);
  fd_defspecial(fd_scheme_module,"VOID?",voidp_handler);
  fd_defspecial(fd_scheme_module,"QUOTE",quote_handler);
  fd_defspecial(fd_scheme_module,"%ENV",env_handler);
  fd_defspecial(fd_scheme_module,"%MODREF",modref_handler);
  fd_defspecial(fd_scheme_module,"DEFAULT",default_handler);
  fd_idefn(fd_scheme_module,fd_make_cprim1("ENVIRONMENT?",environmentp_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("SYMBOL-BOUND?",symbol_boundp_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("%LEXREF",lexref_prim,2,
                                            fd_fixnum_type,FD_VOID,
                                            fd_fixnum_type,FD_VOID));

  fd_defspecial(fd_scheme_module,"WITHENV",withenv_safe_handler);
  fd_defspecial(fd_xscheme_module,"WITHENV",withenv_handler);
  fd_defspecial(fd_xscheme_module,"WITHENV/SAFE",withenv_safe_handler);


  fd_idefn(fd_scheme_module,fd_make_cprim3("GET-ARG",get_arg_prim,2));
  fd_defspecial(fd_scheme_module,"GETOPT",getopt_handler);
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
  fd_defspecial(fd_scheme_module,"WITH-THREADCACHE",with_threadcache_handler);
  /* This ensures that there's an active threadcache, pushing a new one if
     needed or using the current one if it exists. */
  fd_defspecial(fd_scheme_module,"USING-THREADCACHE",using_threadcache_handler);
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

  fd_defspecial(fd_scheme_module,"TIMEVAL",timed_eval);
  fd_defspecial(fd_scheme_module,"%TIMEVAL",timed_evalx);
  fd_defspecial(fd_scheme_module,"%WATCH",watched_eval);
  fd_defspecial(fd_scheme_module,"PROFILE",profiled_eval);
  fd_defspecial(fd_scheme_module,"%WATCHCALL",watchcall_handler);
  fd_defalias(fd_scheme_module,"%WC","%WATCHCALL");
  fd_defspecial(fd_scheme_module,"%WATCHCALL+",watchcall_plus_handler);
  fd_defalias(fd_scheme_module,"%WC+","%WATCHCALL+");
  fd_defspecial(fd_scheme_module,"EVAL1",eval1);
  fd_defspecial(fd_scheme_module,"EVAL2",eval2);
  fd_defspecial(fd_scheme_module,"EVAL3",eval3);
  fd_defspecial(fd_scheme_module,"EVAL4",eval4);
  fd_defspecial(fd_scheme_module,"EVAL5",eval5);
  fd_defspecial(fd_scheme_module,"EVAL6",eval6);
  fd_defspecial(fd_scheme_module,"EVAL7",eval7);

#if USING_GOOGLE_PROFILER
  fd_defspecial(fd_scheme_module,"GOOGLE/PROFILE",gprofile_handler);
  fd_idefn(fd_scheme_module,fd_make_cprim0("GOOGLE/PROFILE/STOP",gprofile_stop,0));
#endif
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("APPLYTEST",applytest,2)));
  fd_defspecial(fd_scheme_module,"EVALTEST",evaltest);

  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("DBG",dbg_prim,0)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("VOID",void_prim,0)));


  fd_idefn(fd_scheme_module,fd_make_cprim2("DTEVAL",dteval,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("DTCALL",dtcall,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("OPEN-DTSERVER",open_bytstrerver,1,
                                            fd_string_type,FD_VOID,
                                            fd_fixnum_type,FD_VOID));

  fd_register_config
    ("GPROFILE","Set filename for the Google CPU profiler",
     fd_sconfig_get,fd_sconfig_set,&cpu_profilename);
  fd_register_config
    ("TAILCALL","Enable/disable tail recursion in the Scheme evaluator",
     fd_boolconfig_get,fd_boolconfig_set,&fd_optimize_tail_calls);
}

FD_EXPORT void fd_init_errors_c(void);
FD_EXPORT void fd_init_compounds_c(void);
FD_EXPORT void fd_init_threadprims_c(void);
FD_EXPORT void fd_init_conditionals_c(void);
FD_EXPORT void fd_init_iterators_c(void);
FD_EXPORT void fd_init_choicefns_c(void);
FD_EXPORT void fd_init_binders_c(void);
FD_EXPORT void fd_init_corefns_c(void);
FD_EXPORT void fd_init_tablefns_c(void);
FD_EXPORT void fd_init_strings_c(void);
FD_EXPORT void fd_init_dbfns_c(void);
FD_EXPORT void fd_init_sequences_c(void);
FD_EXPORT void fd_init_modules_c(void);
FD_EXPORT void fd_init_load_c(void);
FD_EXPORT void fd_init_portfns_c(void);
FD_EXPORT void fd_init_streamprims_c(void);
FD_EXPORT void fd_init_timeprims_c(void);
FD_EXPORT void fd_init_numeric_c(void);
FD_EXPORT void fd_init_side_effects_c(void);
FD_EXPORT void fd_init_reflection_c(void);
FD_EXPORT void fd_init_history_c(void);
FD_EXPORT void fd_init_reqstate_c(void);
FD_EXPORT void fd_init_regex_c(void);
FD_EXPORT void fd_init_quasiquote_c(void);
FD_EXPORT void fd_init_extdbi_c(void);

static void init_core_builtins()
{
  init_localfns();
  fd_init_threadprims_c();
  fd_init_errors_c();
  fd_init_conditionals_c();
  fd_init_iterators_c();
  fd_init_choicefns_c();
  fd_init_binders_c();
  fd_init_corefns_c();
  fd_init_tablefns_c();
  fd_init_compounds_c();
  fd_init_strings_c();
  fd_init_dbfns_c();
  fd_init_sequences_c();
  fd_init_quasiquote_c();
  fd_init_modules_c();
  fd_init_load_c();
  fd_init_portfns_c();
  fd_init_streamprims_c();
  fd_init_timeprims_c();
  fd_init_numeric_c();
  fd_init_side_effects_c();
  fd_init_reflection_c();
  fd_init_history_c();
  fd_init_extdbi_c();
  fd_init_reqstate_c();
  fd_init_regex_c();

  u8_threadcheck();

  fd_finish_module(fd_scheme_module);
  fd_finish_module(fd_xscheme_module);
}

FD_EXPORT int fd_load_fdscheme()
{
  return fd_init_fdscheme();
}

FD_EXPORT int fd_init_fdscheme()
{
  if (fdscheme_initialized) return fdscheme_initialized;
  else {
    fdscheme_initialized=401*fd_init_dblib()*u8_initialize();

    fd_init_eval_c();

    default_env=fd_make_env(fd_make_hashtable(NULL,0),NULL);
    safe_default_env=fd_make_env(fd_make_hashtable(NULL,0),NULL);

    u8_register_source_file(FRAMERD_EVAL_H_INFO);
    u8_register_source_file(_FILEINFO);

    init_scheme_module();
    init_core_builtins();

    return fdscheme_initialized;}
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
