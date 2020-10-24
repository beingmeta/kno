/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/streams.h"
#include "kno/support.h"
#include "kno/getsource.h"
#include "kno/fileprims.h"
#include "kno/cprims.h"

#include <libu8/libu8io.h>
#include <libu8/u8exceptions.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>


#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

u8_condition FileLoad=_("File Load"), FileDone=_("File Done");
u8_condition LoadEval=_("Load Eval");

static int trace_load = 0, trace_load_eval = 0, log_load_errs = 1;

int kno_log_reloads = 1;

static lispval after_symbol, traceloadeval_symbol, postload_symbol;

extern u8_condition UnconfiguredSource;

static lispval loading_symbol, loadstamps_symbol;

#define LOAD_CONTEXT_SIZE 40

KNO_EXPORT lispval kno_load_stream(u8_input loadstream,kno_lexenv env,
				   u8_string sourcebase)
{
  u8_string outer_sourcebase = kno_bind_sourcebase(sourcebase);
  double start = u8_elapsed_time();
  kno_stack _stack = kno_stackptr;
  lispval postload = VOID;
  u8_byte label[strlen(sourcebase)+1]; strcpy(label,sourcebase);
  KNO_CHECK_ERRNO(loadstream,"before loading");
  KNO_PUSH_EVAL(load_stack,label,VOID,env);
  {
    /* This does a read/eval loop. */
    lispval result = VOID;
    lispval expr = VOID, last_expr = VOID;
    double start_time;
    kno_skip_whitespace(loadstream);
    while (!((KNO_ABORTP(expr)) || (KNO_EOFP(expr)))) {
      kno_decref(result);
      if ((trace_load_eval) ||
	  (kno_test(env->env_bindings,traceloadeval_symbol,KNO_TRUE))) {
	u8_log(LOG_WARN,LoadEval,"From %s, evaluating %q",sourcebase,expr);
	KNO_CHECK_ERRNO_OBJ(expr,"before evaluating");
	start_time = u8_elapsed_time();}
      else start_time = -1.0;
      result = kno_stack_eval(expr,env,load_stack);
      kno_reset_stack(load_stack);
      if (KNO_ABORTP(result)) {
	if (KNO_TROUBLEP(result)) {
	  u8_exception ex = u8_current_exception;
	  if (ex == NULL)
	    u8_log(LOG_ERR,"UnknownError", "Errorin %s while evaluating %q",
		   sourcebase,expr);
	  else if (log_load_errs)
	    u8_log(LOG_ERR,ex->u8x_cond,
		   "Error (%s:%s) in %s while evaluating %q",
		   ((ex->u8x_context)?(ex->u8x_context):((u8_string)"")),
		   ((ex->u8x_details)?(ex->u8x_details):((u8_string)"")),
		   sourcebase,expr);}
	kno_restore_sourcebase(outer_sourcebase);
	kno_decref(last_expr); last_expr = VOID;
	kno_decref(expr);
	kno_pop_stack(load_stack);
	return result;}
      else if ((trace_load_eval) ||
	       (kno_test(env->env_bindings,traceloadeval_symbol,KNO_TRUE))) {
	if (start_time>0)
	  u8_log(LOG_WARN,LoadEval,"Took %fs to evaluate %q",
		 u8_elapsed_time()-start_time,expr);
	KNO_CHECK_ERRNO_OBJ(expr,"after evaluating");}
      else NO_ELSE;
      kno_decref(last_expr);
      kno_decref_stackvec(&(load_stack->stack_refs));
      last_expr = expr;
      kno_skip_whitespace(loadstream);
      expr = kno_parse_expr(loadstream);}
    if (expr == KNO_EOF) {
      kno_decref(last_expr);
      last_expr = VOID;}
    else if (KNO_TROUBLEP(expr)) {
      u8_log(LOGERR,"ParseError","In %s, just after %q",sourcebase,last_expr);
      kno_decref(result); /* This is the previous result */
      kno_decref(last_expr);
      last_expr = VOID;
      /* This is now also the result */
      result = expr;
      kno_incref(expr);}
    else {}
    /* Clear the stack status */
    if ((trace_load) || (trace_load_eval))
      u8_log(LOG_WARN,FileDone,"Loaded %s in %f seconds",
	     sourcebase,u8_elapsed_time()-start);
    postload = kno_symeval(postload_symbol,env);
    if ((!(KNO_ABORTP(result)))&&(!(VOIDP(postload)))) {
      if ((FALSEP(postload))||(EMPTYP(postload))) {}
      else if (KNO_APPLICABLEP(postload)) {
	lispval post_result = kno_apply(postload,0,NULL);
	if (KNO_ABORTP(post_result)) {
	  kno_clear_errors(1);}
	kno_decref(post_result);}
      else u8_log(LOG_WARN,"kno_load_source",
		  "Postload method is not applicable: ",
		  postload);}
    kno_decref(postload);
    kno_restore_sourcebase(outer_sourcebase);
    if (last_expr == expr) {
      kno_decref(last_expr);
      last_expr = VOID;}
    else {
      kno_decref(expr);
      kno_decref(last_expr);
      expr = VOID;
      last_expr = VOID;}
    KNO_CHECK_ERRNO(sourcebase,"after loading");
    kno_pop_stack(load_stack);
    return result;}
}

KNO_EXPORT lispval kno_load_source_with_date
(u8_string sourceid,kno_lexenv env,u8_string enc_name,time_t *modtime)
{
  struct U8_INPUT stream;
  u8_string sourcebase = NULL;
  u8_string encoding = ((enc_name)?(enc_name):((u8_string)("auto")));
  u8_string content = kno_get_source(sourceid,encoding,&sourcebase,modtime,NULL);
  if (content == NULL) {
    kno_seterr(kno_FileNotFound,"kno_load_source_with_date",sourceid,KNO_VOID);
    return KNO_ERROR;}
  const u8_byte *input = content;
  if ((trace_load) || (trace_load_eval))
    u8_log(LOG_WARN,FileLoad,
	   "Loading %s (%d bytes)",sourcebase,u8_strlen(content));
  if ((input[0]=='#') && (input[1]=='!')) input = strchr(input,'\n');
  U8_INIT_STRING_INPUT((&stream),-1,input);
  lispval result = kno_load_stream(&stream,env,sourcebase);
  if (sourcebase) u8_free(sourcebase);
  u8_free(content);
  return result;
}

KNO_EXPORT lispval kno_load_source
(u8_string sourceid,kno_lexenv env,u8_string enc_name)
{
  return kno_load_source_with_date(sourceid,env,enc_name,NULL);
}

static u8_string get_component(u8_string spec)
{
  u8_string base = kno_sourcebase();
  if (base) return u8_realpath(spec,base);
  else return u8_strdup(spec);
}

/* Scheme primitives */

static lispval load_source_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval source_expr = kno_get_arg(expr,1), source, result;
  lispval encname_expr = kno_get_arg(expr,2), encval = VOID;
  u8_string encname;
  if (VOIDP(source_expr))
    return kno_err(kno_TooFewExpressions,"LOAD",NULL,expr);
  else source = kno_eval_arg(source_expr,env);
  if (SYMBOLP(source)) {
    lispval config_val = kno_config_get(SYM_NAME(source));
    if (STRINGP(config_val)) {
      u8_log(LOG_NOTICE,"Config","Loading %s = %s",
	     SYM_NAME(source),
	     CSTRING(config_val));
      source = config_val;}
    else if (VOIDP(config_val)) {
      return kno_err(UnconfiguredSource,"load_source",
		     "this source is not configured",
		     source_expr);}
    else return kno_err(UnconfiguredSource,"load_source",
			"this source is misconfigured",
			config_val);}
  if (!(STRINGP(source))) {
    lispval err = kno_type_error("filename","LOAD",source);
    kno_decref(source);
    return err;}
  encval = kno_eval_arg(encname_expr,env);
  if (VOIDP(encval)) encname="auto";
  else if (STRINGP(encval))
    encname = CSTRING(encval);
  else if (SYMBOLP(encval))
    encname = SYM_NAME(encval);
  else encname = NULL;
  while (!(HASHTABLEP(env->env_bindings))) env = env->env_parent;
  result = kno_load_source(CSTRING(source),env,encname);
  kno_decref(source); kno_decref(encval);
  return result;
}

DEFPRIM3("load->env",load_into_env_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(LOAD->ENV *filename* *env* [*resultfn*])` Loads *filename* "
	 "into *env*, applying *resultfn* (if provided) to the result.",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval load_into_env_prim(lispval source,lispval envarg,
				  lispval resultfn)
{
  lispval result = VOID;
  kno_lexenv env;
  if (!((VOIDP(resultfn))||(KNO_APPLICABLEP(resultfn))))
    return kno_type_error("callback procedure","LOAD->ENV",envarg);
  if ( (VOIDP(envarg)) || (KNO_TRUEP(envarg)) || (KNO_DEFAULTP(envarg)))
    env = kno_working_lexenv();
  else if (KNO_LEXENVP(envarg)) {
    env = (kno_lexenv)envarg;
    kno_incref(envarg);}
  else if (TABLEP(envarg))
    env = kno_new_lexenv(envarg);
  else return kno_type_error("environment","LOAD->ENV",envarg);
  if (KNO_STRINGP(source))
    result = kno_load_source(CSTRING(source),env,NULL);
  else return kno_type_error("pathname","load_into_env_prim",source);
  if (KNO_ABORTP(result)) {
    if (HASHTABLEP(env->env_bindings))
      kno_reset_hashtable((kno_hashtable)(env->env_bindings),0,1);
    kno_recycle_lexenv(env);
    return result;}
  if (KNO_APPLICABLEP(resultfn)) {
    lispval tmp = (KNO_VOIDP(result)) ?
      (kno_apply(resultfn,0,NULL)) :
      (kno_apply(resultfn,1,&result));
    if (KNO_ABORTP(tmp)) kno_clear_errors(1);
    kno_decref(tmp);}
  kno_decref(result);
  return (lispval) env;
}

DEFPRIM3("env/load",env_load_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(ENV/LOAD *env* [*filename*])` Updates *env* by loading "
	 "(if needed) the latest version of *filename*. If *filename* "
	 "is not provided, all files previously loaded with `env/load` "
	 "are updated.",
	 kno_lexenv_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval env_load_prim(lispval envarg,lispval source,lispval onload)
{
  kno_lexenv env = (kno_lexenv) envarg;
  if (KNO_VOIDP(source)) {
    int n_loads = kno_load_updates(env);
    if (n_loads<0)
      return KNO_ERROR;
    else return KNO_INT(n_loads);}
  else {
    int rv = kno_load_latest(CSTRING(source),env,1,onload);
    if (rv<0)
      return kno_type_error("pathname","env_load_prim",source);
    else return KNO_INT(1);}
}

static lispval load_component_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval source_expr = kno_get_arg(expr,1), source, result;
  lispval encname_expr = kno_get_arg(expr,2), encval = VOID;
  u8_string encname;
  if (VOIDP(source_expr))
    return kno_err(kno_TooFewExpressions,"LOAD-COMPONENT",NULL,expr);
  else source = kno_eval_arg(source_expr,env);
  if (KNO_ABORTP(source))
    return source;
  else if (!(STRINGP(source))) {
    lispval err = kno_type_error("filename","LOAD-COMPONENT",source);
    kno_decref(source);
    return err;}
  encval = kno_eval_arg(encname_expr,env);
  if (VOIDP(encval)) encname="auto";
  else if (STRINGP(encval))
    encname = CSTRING(encval);
  else if (SYMBOLP(encval))
    encname = SYM_NAME(encval);
  else encname = NULL;
  while (!(HASHTABLEP(env->env_bindings))) env = env->env_parent;
  {
    u8_string abspath = get_component(CSTRING(source));
    result = kno_load_source(abspath,env,encname);
    u8_free(abspath);
  }
  kno_decref(source);
  kno_decref(encval);
  return result;
}

DEFPRIM2("get-component",lisp_get_component,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0),
	 "`(GET-COMPONENT [*arg0*] [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);
static lispval lisp_get_component(lispval string,lispval base)
{
  if (VOIDP(string)) {
    u8_string source_base = kno_sourcebase();
    return knostring(source_base);}
  else if (VOIDP(base)) {
    u8_string fullpath = get_component(CSTRING(string));
    return kno_wrapstring(fullpath);}
  else {
    u8_string thepath = u8_realpath(CSTRING(string),CSTRING(base));
    return kno_wrapstring(thepath);}
}

static lispval path_macro(lispval expr,kno_lexenv env,kno_stack ptr)
{
  lispval arg = kno_get_arg(expr,1);
  if (KNO_STRINGP(arg)) {
    u8_string fullpath = (kno_sourcebase()) ?
      (kno_get_component(KNO_CSTRING(arg))) :
      (u8_abspath(KNO_CSTRING(arg),NULL));
    return kno_init_string(NULL,-1,fullpath);}
  else return kno_err(kno_TypeError,"path_macro","string",arg);
}

/* Latest source functions */

static lispval get_entry(lispval key,lispval entries)
{
  lispval entry = EMPTY;
  DO_CHOICES(each,entries)
    if (!(PAIRP(each))) {}
    else if (LISP_EQUAL(key,KNO_CAR(each))) {
      entry = each;
      KNO_STOP_DO_CHOICES;
      break;}
    else {}
  return entry;
}

KNO_EXPORT
int kno_load_updates(kno_lexenv env)
{
  int loads = 0;
  kno_lexenv scan = env;
  lispval result = VOID;
  lispval loadstamps = kno_get(scan->env_bindings,loadstamps_symbol,EMPTY);
  DO_CHOICES(entry,loadstamps) {
    if ( (KNO_PAIRP(entry)) && (KNO_PAIRP(KNO_CDR(entry))) ) {
      lispval cdr = KNO_CDR(entry), tstamp = (KNO_CAR(cdr));
      struct KNO_TIMESTAMP *loadstamp=
	kno_consptr(kno_timestamp,tstamp,kno_timestamp_type);
      time_t mod_time = u8_file_mtime(CSTRING(KNO_CAR(entry)));
      if (mod_time>loadstamp->u8xtimeval.u8_tick) {
	struct KNO_TIMESTAMP *newtime = u8_alloc(struct KNO_TIMESTAMP);
	KNO_INIT_CONS(tstamp,kno_timestamp_type);
	u8_init_xtime(&(newtime->u8xtimeval),mod_time,u8_second,0,0,0);
	if (kno_log_reloads)
	  u8_log(LOG_WARN,"kno_load_latest",
		 "Reloading %s",CSTRING(KNO_CAR(entry)));
	result = kno_load_source(CSTRING(KNO_CAR(entry)),scan,"auto");
	if (KNO_ABORTP(result)) {
	  KNO_STOP_DO_CHOICES;
	  break;}
	else {
	  KNO_SETCAR(cdr,((lispval)newtime));
	  kno_decref(tstamp);
	  kno_decref(result);}
	loads++;}}}
  kno_decref(loadstamps);
  if (KNO_ABORTP(result))
    return kno_interr(result);
  return loads;
}

KNO_EXPORT
int kno_load_latest(u8_string filename,kno_lexenv env,int refresh,
		    lispval onload)
{
  u8_string abspathstring = u8_abspath(filename,NULL);
  if (! (u8_file_existsp(abspathstring)) ) {
    kno_seterr(kno_FileNotFound,"kno_load_latest",abspathstring,(lispval)env);
    u8_free(abspathstring);
    return -1;}
  lispval abspath = kno_mkstring(abspathstring);
  lispval loadstamps = kno_get(env->env_bindings,loadstamps_symbol,EMPTY);
  lispval entry = get_entry(abspath,loadstamps);
  lispval add_entry = KNO_EMPTY;
  lispval result = VOID;
  if (PAIRP(entry)) {
    lispval cdr = (KNO_CDR(entry));
    lispval tstamp = (KNO_PAIRP(cdr)) ? (KNO_CAR(cdr)) : (cdr);
    if (TYPEP(tstamp,kno_timestamp_type)) {
      struct KNO_TIMESTAMP *curstamp =
	kno_consptr(kno_timestamp,tstamp,kno_timestamp_type);
      time_t last_loaded = curstamp->u8xtimeval.u8_tick;
      time_t mod_time = u8_file_mtime(abspathstring);
      if (mod_time<=last_loaded) {
	if ((refresh)&&(!(KNO_PAIRP(cdr)))) {
	  lispval new_cdr = kno_init_pair(NULL,tstamp,KNO_EMPTY_LIST);
	  KNO_RPLACD(entry,new_cdr);}
	kno_decref(abspath);
	kno_decref(loadstamps);
	u8_free(abspathstring);
	return 0;}
      else {
	struct KNO_TIMESTAMP *newtime = u8_alloc(struct KNO_TIMESTAMP);
	KNO_INIT_CONS(newtime,kno_timestamp_type);
	u8_init_xtime(&(newtime->u8xtimeval),mod_time,u8_second,0,0,0);
	if (PAIRP(cdr)) {
	  KNO_SETCAR(cdr,((lispval)newtime));}
	else if (refresh) {
	  lispval new_cdr =
	    kno_init_pair(NULL,((lispval)newtime),KNO_EMPTY_LIST);
	  KNO_SETCDR(entry,new_cdr);}
	else {KNO_SETCDR(entry,((lispval)newtime));}
	kno_decref(tstamp);}}
    else {
      kno_seterr("Invalid load_latest record","load_latest",
		 abspathstring,entry);
      kno_decref(loadstamps);
      kno_decref(abspath);
      u8_free(abspathstring);
      return -1;}}
  else {
    time_t mod_time = u8_file_mtime(abspathstring);
    struct KNO_TIMESTAMP *tstamp = u8_alloc(struct KNO_TIMESTAMP);
    KNO_INIT_CONS(tstamp,kno_timestamp_type);
    u8_init_xtime(&(tstamp->u8xtimeval),mod_time,u8_second,0,0,0);
    add_entry = entry = (refresh) ?
      (kno_make_list(2,kno_incref(abspath),LISP_CONS(tstamp))) :
      (kno_conspair(kno_incref(abspath),LISP_CONS(tstamp)));}
  if (kno_log_reloads)
    u8_log(LOG_WARN,"kno_load_latest","Reloading %s",abspathstring);
  result = kno_load_source(abspathstring,env,"auto");
  if (KNO_APPLICABLEP(onload)) {
    lispval onload_result = kno_apply(onload,1,&result);
    kno_decref(onload_result);}
  kno_decref(abspath);
  u8_free(abspathstring);
  kno_decref(loadstamps);
  if (KNO_EXISTSP(add_entry))
    kno_add(env->env_bindings,loadstamps_symbol,add_entry);
  if (KNO_ABORTP(result))
    return kno_interr(result);
  else kno_decref(result);
  return 1;
}

static lispval load_latest_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  int retval = -1;
  lispval path_expr = kno_get_arg(expr,1);
  if (KNO_VOIDP(path_expr))
    return kno_err(kno_SyntaxError,"load_latest_evalfn",NULL,expr);
  lispval path = kno_eval_arg(path_expr,env);
  if (!(STRINGP(path))) {
    lispval err = kno_type_error("pathname","load_latest",path);
    kno_decref(path);
    return err;}
  lispval onload_expr = kno_get_arg(expr,2);
  if (KNO_VOIDP(onload_expr))
    retval = kno_load_latest(CSTRING(path),env,0,KNO_VOID);
  else {
    lispval onload = kno_eval_arg(onload_expr,env);
    if (KNO_ABORTED(onload)) return onload;
    retval = kno_load_latest(CSTRING(path),env,0,onload);
    kno_decref(onload);}
  kno_decref(path);
  if (retval<0)
    return KNO_ERROR;
  else if (retval)
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval load_update_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  if (NILP(KNO_CDR(expr))) {
    int loads = kno_load_updates(env);
    return KNO_INT(loads);}
  else {
    int retval = -1;
    lispval path_expr = kno_get_arg(expr,1);
    lispval path = kno_eval_arg(path_expr,env);
    if (!(STRINGP(path))) {
      lispval err = kno_type_error("pathname","load_latest",path);
      kno_decref(path);
      return err;}
    lispval onload_expr = kno_get_arg(expr,2);
    if (KNO_VOIDP(onload_expr))
      retval = kno_load_latest(CSTRING(path),env,1,KNO_VOID);
    else {
      lispval onload = kno_eval_arg(onload_expr,env);
      if (KNO_ABORTED(onload)) return onload;
      retval = kno_load_latest(CSTRING(path),env,1,onload);
      kno_decref(onload);}
    kno_decref(path);
    if (retval<0)
      return KNO_ERROR;
    else if (retval)
      return KNO_TRUE;
    else return KNO_FALSE;}
}

/* Lisp run */

static lispval kno_run(u8_string source_file,struct U8_OUTPUT *out,
		       int n,kno_argvec args)
{
  kno_lexenv env = kno_working_lexenv();
  lispval load_result = kno_load_source(source_file,env,NULL);
  if (KNO_ABORTP(load_result)) {
    kno_recycle_lexenv(env);
    return load_result;}
  else {
    lispval main_proc = kno_symeval(KNOSYM_MAIN,env);
    if (KNO_VOIDP(main_proc)) {
      u8_log(LOG_CRIT,"NoMain",
	     "No (MAIN) was defined in '%s', returning last value",
	     source_file);
      kno_recycle_lexenv(env);
      return load_result;}
    else if (!(KNO_APPLICABLEP(main_proc))) {
      kno_seterr("BadMainProc","lisp_run_file",u8_strdup(source_file),main_proc);
      /* kno_recycle_lexenv(env); */
      kno_recycle_lexenv(env);
      return KNO_ERROR_VALUE;}
    else {
      u8_output prev = u8_current_output;
      if (out) u8_set_default_output(out);
      lispval result = kno_dapply(main_proc,n,args);
      if (KNO_ABORTP(result)) {
	kno_simplify_exception(NULL);}
      kno_decref(main_proc);
      kno_decref(load_result);
      kno_recycle_lexenv(env);
      if (out) u8_set_default_output(prev);
      return result;}}
}

DEFPRIM("kno/run-file",kno_run_file,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	"Loads a file and applies its (main) procedure to "
	"the arguments");
static lispval kno_run_file(int n,kno_argvec args)
{
  if ( (KNO_STRINGP(args[0])) &&
       (u8_file_existsp(KNO_CSTRING(args[0]))) )
    return kno_run(KNO_CSTRING(args[0]),NULL,n-1,args+1);
  else return kno_type_error("filename","kno_run_file",args[0]);
}

DEFPRIM("kno/run->string",kno_run_file_2string,KNO_VAR_ARGS|KNO_MIN_ARGS(1)|KNO_NDCALL,
	"Loads a KNO file and applies its (main) procedure "
	"to the arguments, returns the output as a string");
static lispval kno_run_file_2string(int n,kno_argvec args)
{
  if ( (KNO_STRINGP(args[0])) &&
       (u8_file_existsp(KNO_CSTRING(args[0]))) ) {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,4096);
    lispval result = kno_run(KNO_CSTRING(args[0]),&out,n-1,args+1);
    if (KNO_ABORTP(result)) {
      if ( (out.u8_write-out.u8_outbuf) > 0) {
	/* lispval output = kno_stream_string(&out); */
	u8_close_output(&out);
#if 0
	kno_seterr("RunFailed","kno_run_file_2string",
		   KNO_CSTRING(args[0]),
		   output);
	kno_decref(output);
#endif
	return result;}
      else {
	u8_close_output(&out);
	return result;}}
    else {
      lispval output = kno_stream_string(&out);
      kno_decref(result);
      u8_close_output(&out);
      return output;}}
  else return kno_type_error("filename","kno_run_file_2string",args[0]);
}

/* Initialization */

KNO_EXPORT void kno_init_load_c()
{
  u8_register_source_file(_FILEINFO);

  after_symbol = kno_intern("afterexpr");
  loading_symbol = kno_intern("%loading");
  loadstamps_symbol = kno_intern("%loadstamps");
  traceloadeval_symbol = kno_intern("%traceloadeval");
  postload_symbol = kno_intern("%postload");


  kno_def_evalfn(kno_scheme_module,"LOAD",load_source_evalfn,
		 "*undocumented*");
  kno_def_evalfn(kno_scheme_module,"LOAD-COMPONENT",load_component_evalfn,
		 "*undocumented*");

  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"LOAD-LATEST",load_latest_evalfn,
		 "`(load-latest *filename*)` loads the latest version of "
		 "*filename* into the innermost non-static environment. "
		 "If the loaded version is newer than *filename*, "
		 "nothing is done. This uses the binding "
		 "%loadstamps in the current environment.");

  kno_def_evalfn(kno_scheme_module,"LOAD-UPDATES",load_update_evalfn,
		 "`(load-updates [*filename*])` loads the latest version of "
		 "*filename* into the innermost non-static environment "
		 "and marks it for automatic updating. "
		 "If the loaded version is newer than *filename*, "
		 "nothing is done. This uses the binding "
		 "%loadstamps in the current environment. "
		 "Without any argument, this updates any changed files "
		 "in the load environment.");

  kno_def_evalfn(kno_scheme_module,"#PATH",path_macro,
		 "#:PATH\"init/foo.scm\" or #:PATH:home.scm\n"
		 "evaluates to an environment variable");

  kno_register_config("LOAD:TRACE","Trace file load starts and ends",
		      kno_boolconfig_get,kno_boolconfig_set,&trace_load);
  kno_register_config("TRACELOAD","Trace file load starts and ends",
		      kno_boolconfig_get,kno_boolconfig_set,&trace_load);

  kno_register_config("LOAD:LOGERRS",
		      "Whether to log errors during file loading",
		      kno_boolconfig_get,kno_boolconfig_set,&log_load_errs);
  kno_register_config("LOGLOADERRS","Whether to log errors during file loading",
		      kno_boolconfig_get,kno_boolconfig_set,&log_load_errs);

  kno_register_config("LOAD:LOGEVAL","Trace expressions while loading files",
		      kno_boolconfig_get,kno_boolconfig_set,&trace_load_eval);
  kno_register_config("LOAD:LOGRELOADS","Log file and module reloads",
		      kno_boolconfig_get,kno_boolconfig_set,&kno_log_reloads);
  kno_register_config("TRACELOADEVAL","Trace expressions while loading files",
		      kno_boolconfig_get,kno_boolconfig_set,&trace_load_eval);
}


static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_VARARGS("kno/run->string",kno_run_file_2string,scheme_module);
  KNO_LINK_VARARGS("kno/run-file",kno_run_file,scheme_module);
  KNO_LINK_PRIM("get-component",lisp_get_component,2,scheme_module);
  KNO_LINK_PRIM("load->env",load_into_env_prim,3,scheme_module);
  KNO_LINK_PRIM("env/load",env_load_prim,3,scheme_module);
}
