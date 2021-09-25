/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_EVAL_INTERNALS 1
#define KNO_INLINE_CHECKTYPE    (!(KNO_AVOID_INLINE))
#define KNO_INLINE_CHOICES      (!(KNO_AVOID_INLINE))
#define KNO_INLINE_TABLES       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_QONSTS	(!(KNO_AVOID_INLINE))
#define KNO_INLINE_STACKS       (!(KNO_AVOID_INLINE))
#define KNO_INLINE_LEXENV       (!(KNO_AVOID_INLINE))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"

#include "../apply/apply_internals.h"
#include "eval_internals.h"

#include <libu8/u8printf.h>

#include <sys/resource.h>
#include "kno/profiles.h"

u8_string kno_lambda_stack_type = "lambda";

u8_condition kno_BadArglist=_("Malformed argument list");
u8_condition kno_BadDefineForm=_("Bad procedure defining form");

int kno_enclose_lambdas=0;
int kno_record_source=1;
int kno_tail_max=-1;

static lispval tail_symbol, decls_symbol, flags_symbol;

/* Setting up a lambda call stack */

static lispval get_rest_arg(kno_argvec args,int n,int incref)
{
  lispval result=NIL; n--;
  while (n>=0) {
    lispval arg=args[n];
    if (incref) kno_incref(arg);
    result=kno_init_pair(NULL,arg,result);
    n--;}
  return result;
}

int lambda_setup(kno_stack stack,kno_stack origin,
		 lispval lambda,
		 int n_given,kno_argvec given,
		 int free_given,
		 int tail)
{
  struct KNO_LAMBDA *proc = KNO_LAMBDA_INFO(lambda);
  kno_lexenv proc_env = KNO_LAMBDA_ENV(lambda);
  int n_vars = proc->lambda_n_vars;
  lispval op = (lispval) lambda; kno_incref(op);

  if (RARELY(proc_env==NULL)) {
    kno_seterr("HomelessLambda","lambda_setup",NULL,lambda);
    return -1;}

  /* This does the tail call cleanup */
  lispval *args = stack->stack_args;
  struct KNO_LEXENV  *eval_env = stack->eval_env;
  struct KNO_SCHEMAP *bindings = (kno_schemap) eval_env->env_bindings;
  if (origin) {
    lispval cur_op = stack->stack_op;
    kno_function cur = KNO_FUNCTION_INFO(cur_op);
    if (cur) {
      if ( (cur->fcn_trace) & (KNO_FCN_TRACE_EXIT) )
	kno_trace_exit
	  (stack,KNO_TAIL,cur_op,cur,stack->stack_argc,stack->stack_args);}
    /* Clean up old stuff */
    if (stack->stack_argc>0) {
      lispval *scan = args, *limit = args+stack->stack_argc;
      while (scan<limit) {
	lispval arg = *scan;
	kno_decref(arg);
	*scan++=KNO_VOID;}
      stack->stack_argc=0;}
    reset_env(eval_env);}

  kno_stackvec refs = &(stack->stack_refs);
  kno_decref_stackvec(refs);

 setup:
  KNO_STACK_SET_BIT(stack,KNO_STACK_DECREF_ARGS,1);
  KNO_STACK_SET_BIT(stack,KNO_STACK_TAIL_POS,tail);
  kno_stackvec_push(refs,op);
  kno_profile profile = proc->fcn_profile;
  struct rusage init_usage; struct timespec start_time;
  if (profile) {
    if (profile->prof_disabled) profile=NULL;
    else kno_profile_start(&init_usage,&start_time);}

  u8_string label = (proc->fcn_name) ? (proc->fcn_name) : (U8S("lambda"));
  u8_string filename = (proc->fcn_filename) ? (proc->fcn_filename) : (NULL);
  stack->stack_bits |= KNO_STACK_LAMBDA_CALL;
  stack->stack_file = filename;
  stack->stack_origin = stack->stack_label = label;

  if (proc->fcn_trace)
    kno_trace_call(stack,lambda,(kno_function)proc,n_given,given);

  lispval *vars = proc->lambda_vars;
  int arity = proc->fcn_arity;

  eval_env->env_copy = NULL;
  eval_env->env_parent = proc_env;
  bindings->table_schema = vars;
  bindings->schema_length = n_vars;

  if (proc->lambda_synchronized) u8_lock_mutex(&(proc->lambda_lock));

  int max_positional = (arity<0) ?
    (n_vars - 1 - proc->lambda_n_locals) :
    (arity);

  /* Argument processing */
  lispval *inits = proc->lambda_inits;
  lispval *typeinfo = proc->fcn_typeinfo;
  int i = 0, typeinfo_len = proc->fcn_arginfo_len;
 process_args:
  while (i<max_positional) {
    lispval arg = (i<n_given) ? (given[i]) : (KNO_VOID);
    if (KNO_IMMEDIATEP(arg)) {
      if ( ( (KNO_VOIDP(arg)) || (KNO_DEFAULTP(arg)) ) &&
	   (inits) && (!(KNO_VOIDP(inits[i]))) ) {
	lispval init_expr = inits[i];
	lispval init_val  = eval_arg(init_expr,proc_env,stack);
	if (init_val == KNO_TAIL) {
	  kno_bad_arg(init_val,"lambda_call/default",init_expr);
	  return -1;}
	else if (init_val == KNO_VOID)
	  init_val = arg;
	else if (KNO_ABORTED(init_val))
	  goto err_exit;
	else NO_ELSE;
	arg = init_val;}
      else if (arg == KNO_DEFAULT_VALUE) {}
      else if ( (arg == KNO_TAIL) || (ABORTED(arg)) ) {
	/* I don't think this should ever happen */
	u8_condition c = (arg == KNO_TAIL) ? _("TailValueArg") :
	  ("ErrorValueArg");
	kno_seterr(c,"lambda_call",KNO_SYMBOL_NAME(vars[i]),lambda);
	goto err_exit;}
      else NO_ELSE;}
    else if (CONSP(arg)) {
      if (QCHOICEP(arg)) {
	lispval qarg=KNO_QCHOICEVAL(arg);
	if (KNO_CONSP(qarg)) {
	  kno_incref(qarg);
	  if (!(free_given)) kno_stackvec_push(refs,arg);}
	arg = qarg;}
      else if (!(free_given)) kno_incref(arg);
      else NO_ELSE;}
    else NO_ELSE;
    if ( (KNO_EMPTYP(arg)) || (KNO_VOIDP(arg)) || (KNO_DEFAULTP(arg)) ) {}
    else if ( (typeinfo) && (i<typeinfo_len) ) {
      lispval typeval = typeinfo[i];
      if (KNO_FALSEP(typeval)) {}
      else if (!(KNO_CHECKTYPE(arg,typeval))) {
	u8_byte buf[100];
	u8_string details =
	  u8_bprintf(buf,"%s.%s[%d] !~ %q",
		     label,KNO_SYMBOL_NAME(vars[i]),i,typeval);
	kno_seterr(kno_TypeError,"lambda",details,arg);
	return -1;}}
    else NO_ELSE;
    args[i++] = arg;
    stack->stack_argc = i;}

  /* Now accumulate a .rest arg if it's needed */
  if (arity < 0) {
    if (i<n_given) {
      lispval rest_arg = get_rest_arg(given+i,n_given-i,(!(free_given)));
      args[i++] = rest_arg;}
    else args[i++] = KNO_EMPTY_LIST;}

  /* Now initialize the locals */
  while (i<n_vars) {args[i++] = KNO_UNBOUND;}
  stack->stack_argc = i;

  stack->eval_context = proc->lambda_source;
  stack->eval_source = proc->lambda_source;
  stack->stack_op = op;

  if (profile)
    /* Update just for init */
    kno_profile_update(profile,&init_usage,&start_time,0);

  /* Note that this will leave the proc's mutex locked if it's synchronized */
  return n_vars;
 err_exit:
  return -1;
}

/* Initializing locals for the call */
/* This isn't currently used because the locals are initialized in the body,
   which is probably the right thing ? */

/* Doing the call itself */

lispval lambda_call(kno_stack stack,
		    lispval lambda,
		    int n_given,kno_argvec given,
		    int free_given,
		    int tail)
{
  struct KNO_LAMBDA *proc = KNO_LAMBDA_INFO(lambda);
  int n_vars = proc->lambda_n_vars;
  kno_profile profile = proc->fcn_profile;

  kno_stack found = NULL;
 entry:
  if ( (tail) && (profile == NULL) ) {
    kno_stack scan = stack;
    int tail_max = kno_tail_max, tail_span = 0;
    while (scan) {
      if ( ! (KNO_STACK_BITP(scan,KNO_STACK_TAIL_POS)) ) break;
      else if (!(KNO_STACK_BITP(scan,KNO_STACK_LAMBDA_CALL)))
	scan = scan->stack_caller;
      else {
	if (scan->stack_width >= n_vars) {
	  found=scan; tail_span++;
	  /* This can increase debuggability by specifying how
	     far up the stack tail calls will go */
	  if ( (tail_max>0) && (tail_span>=tail_max) ) break;}
	scan=scan->stack_caller;}}}
  if (found) {
    int rv;
  tail:
    rv = lambda_setup(found,stack,lambda,n_given,given,free_given,tail);
    if (rv<0) return KNO_ERROR;
    KNO_STACK_SET_BIT(found,KNO_STACK_TAIL_LOOP,1);
    return KNO_TAIL;}

  KNO_START_EVAL(lambda_stack,"lambda",(lispval)proc,NULL,stack);
 push:
  lambda_stack->stack_bits |= KNO_STACK_LAMBDA_CALL;
  lispval args[n_vars];
  struct KNO_LEXENV  body_env = { 0 };
  struct KNO_SCHEMAP bindings = { 0 };
  KNO_INIT_STATIC_CONS(&bindings,kno_schemap_type);
  KNO_INIT_STATIC_CONS(&body_env,kno_lexenv_type);
  bindings.table_bits = KNO_SCHEMAP_STATIC_SCHEMA | KNO_SCHEMAP_STATIC_VALUES;
  bindings.table_values = args;
  bindings.schemap_template = KNO_VOID;
  lspset(args,KNO_VOID,n_vars);

  body_env.env_bindings = (lispval) &bindings;
  body_env.env_exports	= KNO_VOID;

  lambda_stack->eval_env = &body_env;
  lambda_stack->stack_args = args;
  lambda_stack->stack_argc = 0;
  lambda_stack->stack_width = n_vars;
  KNO_STACK_SET(lambda_stack,KNO_STACK_OWNS_ENV);

  int rv = lambda_setup(lambda_stack,NULL,lambda,n_given,given,free_given,tail);
  if (rv<0) {
    kno_pop_stack(lambda_stack);
    return KNO_ERROR;}

  lispval result = KNO_VOID;
  unsigned long long loop_count = 0;

  while (1) {
    lispval lambda = lambda_stack->stack_op;
    struct KNO_LAMBDA *proc = KNO_LAMBDA_INFO(lambda);
    int synchronized = proc->lambda_synchronized, ok = 0;
    int tailable = !( (synchronized) || ( (proc->fcn_call) & KNO_CALL_NOTAIL) );
    stack->eval_source = stack->eval_context = proc->lambda_body;
    U8_PAUSEPOINT();
    struct rusage init_usage; struct timespec start_time;
    kno_profile profile = proc->fcn_profile;
    if (profile) {
      if (profile->prof_disabled) profile=NULL;
      else kno_profile_start(&init_usage,&start_time);}

    if (ok>=0) {
      lispval start = proc->lambda_entry;
      result = eval_body(start,&body_env,lambda_stack,
			 "lambda",proc->fcn_name,
			 tailable);}
    else result = KNO_ERROR;

    /* If we're synchronized, unlock the mutex (locked by lambda_setup) */
    if (synchronized) u8_unlock_mutex(&(proc->lambda_lock));

    lambda_stack->eval_context = KNO_VOID;
    lambda_stack->eval_source = KNO_VOID;
    reset_env(&body_env);

    if (profile) kno_profile_update(profile,&init_usage,&start_time,1);

    if ( (result == KNO_TAIL) &&
	 (KNO_STACK_BITP(lambda_stack,KNO_STACK_TAIL_LOOP))) {
      KNO_STACK_SET_BIT(lambda_stack,KNO_STACK_TAIL_LOOP,0);
      loop_count++;
      continue;}
    else {
      if ( (proc->fcn_trace) & (KNO_FCN_TRACE_EXIT) )
	kno_trace_exit(lambda_stack,result,
		       lambda,(kno_function)proc,
		       stack->stack_argc,stack->stack_args);
      break;}}
  kno_pop_stack(lambda_stack);
  return kno_simplify_choice(result);
}

/* Evaluating a lambda template */
lispval eval_lambda(kno_lambda into,kno_lambda lambda,kno_lexenv env)
{
  lispval llambda = (lispval) lambda;
  struct KNO_LAMBDA *result;
  if (lambda->lambda_env) {
    kno_incref(llambda);
    return llambda;}
  if (into == NULL)
    result = u8_alloc(struct KNO_LAMBDA);
  else result = into;
  memcpy(result,lambda,sizeof(struct KNO_LAMBDA));
  if (into) {KNO_INIT_STATIC_CONS(result,kno_lambda_type);}
  else {KNO_INIT_CONS(result,kno_lambda_type);}
  if (KNO_MALLOCDP(llambda)) kno_incref(llambda);
  result->lambda_template = lambda;
  if (env->env_copy) {
    result->lambda_env = env->env_copy;
    kno_incref((lispval)(env->env_copy));}
  return (lispval) result;
}

/* LAMBDAs */

static lispval lambda_docall(kno_stack caller,
			     lispval lambda,
			     short n,kno_argvec args)
{
  if (caller)
    return lambda_call(caller,lambda,n,args,0,1);
 else {
   kno_lambda proc = KNO_LAMBDA_INFO(lambda);
   KNO_START_EVAL(xeval,proc->fcn_name,KNO_VOID,NULL,kno_stackptr);
   lispval result = lambda_call(xeval,lambda,n,args,0,1);
   kno_pop_stack(xeval);
   return result;}
}

KNO_EXPORT lispval kno_lambda_call(kno_stack caller,lispval handler,
				   int n,kno_argvec args)
{
  return lambda_docall(caller,handler,n,args);
}

static lispval apply_lambda(lispval fn,int n,kno_argvec args)
{
  return lambda_docall(kno_stackptr,fn,n,args);
}

KNO_EXPORT int kno_set_lambda_schema
(struct KNO_LAMBDA *s,int n,lispval *args,lispval *inits,lispval *types)
{
  lispval *use_args = u8_alloc_n(n,lispval);
  kno_lspcpy(use_args,args,n);
  s->fcn_argnames=s->lambda_vars=use_args;

  lispval *use_inits = NULL;
  if (inits) {
    int i = 0;
    while (i<n) {
      lispval init = inits[i++];
      if (!( (KNO_VOIDP(init)) || (init==KNO_QVOID) )) {
	use_inits = u8_alloc_n(n,lispval);
	break;}}
    if (use_inits) {
      i=0; while (i<n) {
	lispval elt = inits[i];
	if (elt == KNO_QVOID) elt = KNO_VOID;
	use_inits[i]=kno_incref(elt);
	i++;}}}
  s->lambda_inits = use_inits;

  lispval *use_types = NULL;
  if (types) {
    int i = 0;
    while (i<n) {
      if ( (KNO_VOIDP(types[i])) || (KNO_FALSEP(types[i])) ) i++;
      else {
	use_types = u8_alloc_n(n,lispval);
	break;}}
    if (use_types) {
      i=0; while (i<n) {
	lispval type = types[i];
	if ((KNO_VOIDP(type)) || (KNO_FALSEP(type)))
	  use_types[i]=KNO_FALSE;
	else use_types[i]=kno_incref(type);
	i++;}}}
  s->fcn_typeinfo = use_types;

  s->fcn_call_width = s->lambda_n_vars = n;

  return n;
}

DEF_KNOSYM(locals); DEF_KNOSYM(local); DEF_KNOSYM(define);

static lispval defaultbang_symbol, setbang_symbol;

static lispval scan_body(lispval body,u8_string name,
			 kno_lambda s,
			 u8_output docout,
			 kno_stackvec args,
			 kno_stackvec inits,
			 kno_stackvec types,
			 kno_lexenv env,
			 kno_stack stack)
{
  lispval scan = body, entry = body;
  while ( (PAIRP(scan)) &&
	  (STRINGP(KNO_CAR(scan))) &&
	  (PAIRP(KNO_CDR(scan))) ) {
    lispval head = KNO_CAR(scan); scan=KNO_CDR(scan);
    if (docout->u8_write>docout->u8_outbuf) u8_putc(docout,'\n');
    u8_puts(docout,KNO_CSTRING(head));}
  if (KNO_EMPTY_LISTP(body)) return body;
  else if (!(KNO_PAIRP(body)))
    return kno_err(kno_SyntaxError,"lambda_body",name,body);
  else if (KNO_EMPTY_LISTP(KNO_CDR(scan)))
    return scan;
  else if (KNO_SCHEMAPP(KNO_CAR(scan))) {
    lispval attribs = eval_schemap(KNO_CAR(scan),env,stack);
    if (KNO_ABORTED(attribs)) return attribs;
    else s->fcn_attribs=attribs;}
  else if (KNO_SLOTMAPP(KNO_CAR(scan))) {
    s->fcn_attribs=kno_deep_copy(KNO_CAR(scan));}
  else NO_ELSE;
  if (KNO_EMPTY_LISTP(KNO_CDR(scan))) return scan;
  entry = scan;
  lispval def = KNO_CAR(scan);
  while (KNO_PAIRP(def)) {
    lispval head = KNO_CAR(def);
    if ( (head == KNOSYM(local)) || (head == KNOSYM(locals)) ) {
      lispval vars = KNO_CDR(def); while (KNO_PAIRP(vars)) {
	lispval head = KNO_CAR(vars);
	if (KNO_SYMBOLP(head)) {
	  kno_stackvec_push(args,head);
	  kno_stackvec_push(inits,KNO_VOID);
	  kno_stackvec_push(types,KNO_FALSE);
	  vars = KNO_CDR(vars);
	  if (KNO_PAIRP(scan)) vars = KNO_CDR(vars);
	  else return kno_err(kno_SyntaxError,"lambda_body/locals",name,def);}
	else if ( (KNO_PAIRP(head)) && (KNO_SYMBOLP(KNO_CAR(head))) ) {
	  lispval arg = KNO_CAR(head), type = kno_get_arg(head,2);
	  kno_stackvec_push(args,arg);
	  kno_stackvec_push(inits,KNO_VOID);
	  if (KNO_VOIDP(type))
	    kno_stackvec_push(types,KNO_FALSE);
	  else {
	    kno_incref(type);
	    kno_stackvec_push(types,type);}
	  vars = KNO_CDR(vars);}
	else return kno_err(kno_SyntaxError,"lambda_body",name,def);}}
    else if (head == KNOSYM(define)) {
      lispval spec = kno_get_arg(def,1);
      lispval arg, type = KNO_FALSE;
      if (KNO_SYMBOLP(spec)) {
	arg = spec;
	type = KNO_FALSE;}
      else if ( (KNO_PAIRP(spec)) && (KNO_SYMBOLP(KNO_CAR(spec))) ) {
	arg = KNO_CAR(spec);
	type = KNO_FALSE;}
      else return kno_err(kno_SyntaxError,"lambda",name,def);
      kno_stackvec_push(args,arg);
      /* kno_stackvec_push(inits,init); kno_incref(init);*/
      kno_stackvec_push(inits,KNO_VOID);
      if ( (KNO_VOIDP(type)) || (KNO_FALSEP(type)) )
	kno_stackvec_push(types,KNO_FALSE);
      else {
	lispval type_val = kno_eval(type,env,stack);
	if (KNO_ABORTED(type_val)) return type_val;
	kno_stackvec_push(inits,type_val);}}
    else NO_ELSE;
    if (KNO_EMPTY_LISTP(KNO_CDR(scan))) break;
    scan = KNO_CDR(scan);
    def = KNO_CAR(scan);}
  return entry;
}

DEF_KNOSYM(synchronized); DEF_KNOSYM(sync);
DEF_KNOSYM(choiceop); DEF_KNOSYM(choicefn); DEF_KNOSYM(ndcall);

static lispval
_make_lambda(u8_string name,
	     lispval arglist,lispval body,
	     kno_lexenv env,kno_stack stack,
	     int nd,int sync,
	     int incref,int copy_env)
{
  struct KNO_LAMBDA *s = u8_alloc(struct KNO_LAMBDA);
  lispval result = KNO_ERROR;
  KNO_INIT_FRESH_CONS(s,kno_lambda_type);

  s->fcn_name = ((name) ? (u8_strdup(name)) : (NULL));

  s->fcn_call = ( ((nd) ? (KNO_CALL_NDCALL) : (0)) |
		  (KNO_CALL_XCALL) );
  s->fcn_handler.xcalln = (kno_xprimn) lambda_docall;
  s->fcn_filename = NULL;
  s->fcn_attribs = KNO_EMPTY;
  s->lambda_consblock = NULL;
  s->lambda_source = VOID;

  int i = 0, first_optional = -1;
  U8_STATIC_OUTPUT(doc,500);
  KNO_DECL_STACKVEC(args,32);
  KNO_DECL_STACKVEC(inits,32);
  KNO_DECL_STACKVEC(types,32);
  lispval scan = arglist; while (PAIRP(scan)) {
    lispval elt = KNO_CAR(scan); scan=KNO_CDR(scan);
    if (KNO_SYMBOLP(elt)) {
      kno_stackvec_push(args,elt);
      kno_stackvec_push(inits,KNO_VOID);
      kno_stackvec_push(types,KNO_FALSE);}
    else if (KNO_PAIRP(elt)) {
      lispval argname = pop_arg(elt), init_expr=pop_arg(elt);
      lispval type_expr=pop_arg(elt);
      if (!(KNO_EMPTY_LISTP(elt))) {
	result = kno_err(kno_SyntaxError,"lambda/optarg",name,arglist);
	goto err_exit;}
      kno_stackvec_push(args,argname);
      if (init_expr == KNO_REQUIRED_VALUE)
	kno_stackvec_push(inits,KNO_VOID);
      else {
	kno_stackvec_push(inits,init_expr);
	kno_incref(init_expr);}
      if ( (KNO_VOIDP(type_expr)) || (KNO_FALSEP(type_expr)) ||
	   (KNO_EMPTYP(type_expr)) )
	kno_stackvec_push(types,KNO_FALSE);
      else {
	lispval type = kno_eval(type_expr,env,stack);
	if (KNO_ABORTED(type)) goto err_exit;
	else kno_stackvec_push(types,type);}
      if (init_expr==KNO_REQUIRED_VALUE)  {}
      else if (first_optional<0)
	first_optional=i;
      else NO_ELSE;}
    else {
      result = kno_err(kno_SyntaxError,"lambda/arglist",s->fcn_name,arglist);
      goto err_exit;}
    i++;}
  if (scan == KNO_EMPTY_LIST) {
    s->fcn_arginfo_len = s->fcn_arity=i;
    if (first_optional>=0)
      s->fcn_min_arity=first_optional;
    else s->fcn_min_arity=i;}
  else if (KNO_SYMBOLP(scan)) {
    kno_stackvec_push(args,scan);
    kno_stackvec_push(inits,KNO_EMPTY_LIST);
    kno_stackvec_push(types,KNO_FALSE);
    if (first_optional>=0)
      s->fcn_min_arity=first_optional;
    else s->fcn_min_arity=i;
    s->fcn_arginfo_len = i+1;
    s->fcn_arity=-1;}
  else {
    kno_seterr(kno_SyntaxError,"lambda/arglist",s->fcn_name,arglist);
    goto err_exit;}

  lispval entry = scan_body
    (body,name,s,docout,args,inits,types,env,stack);
  if (KNO_ABORTED(entry)) goto err_exit;
  else if (incref) {
    s->lambda_body = kno_incref(body);
    s->lambda_entry = entry;
    s->lambda_arglist = kno_incref(arglist);
    if (body != entry) kno_incref(entry);}
  else {
    s->lambda_body = body;
    s->lambda_entry = entry;
    s->lambda_arglist = arglist;
    if (body != entry) kno_incref(entry);}

  if (KNO_TABLEP(s->fcn_attribs)) {
    lispval attribs = s->fcn_attribs;
    if ( (kno_test(attribs,KNOSYM(synchronized),KNO_VOID)) ||
	 (kno_test(attribs,KNOSYM(sync),KNO_VOID)) )
      sync=1;
    if ( (kno_test(attribs,KNOSYM(choiceop),KNO_VOID)) ||
	 (kno_test(attribs,KNOSYM(choicefn),KNO_VOID)) ) {
      s->fcn_call = (KNO_CALL_NDCALL);
      nd=1;}
    if (name == NULL) {
      lispval name_opt = kno_get(attribs,KNOSYM_NAME,KNO_VOID);
      if (KNO_STRINGP(name_opt))
	s->fcn_name=name=u8_strdup(KNO_CSTRING(name_opt));
      else if (KNO_SYMBOLP(name_opt))
	s->fcn_name=name=u8_strdup(KNO_SYMBOL_NAME(name_opt));
      else NO_ELSE;
      kno_decref(name_opt);}}

  if (docout->u8_write>docout->u8_outbuf) {
    s->fcn_doc = u8_strdup(docout->u8_outbuf);
    s->fcn_free |= KNO_FCN_FREE_DOC;}
  else s->fcn_doc = NULL;

  int n = KNO_STACKVEC_COUNT(args);
  int n_vars = kno_set_lambda_schema
    (s,n,KNO_STACKVEC_ELTS(args),
     KNO_STACKVEC_ELTS(inits),
     KNO_STACKVEC_ELTS(types));
  if (n_vars < 0) goto err_exit;
  s->lambda_n_locals = n_vars-s->fcn_arginfo_len;

  int enclose = (env!=NULL) && (!(sync)) && (kno_enclose_lambdas);

  if (enclose) { /* Make all procs be closures */
    s->lambda_env = NULL;}
  else if (env) {
    s->lambda_env = kno_copy_env(env);}
  else s->lambda_env = NULL;

  if (sync) {
    s->fcn_free |= KNO_FCN_DONT_COPY;
    s->lambda_synchronized = 1;
    u8_init_mutex(&(s->lambda_lock));}
  else s->lambda_synchronized = 0;

  kno_decref_stackvec(args);
  kno_free_stackvec(args);
  kno_decref_stackvec(inits);
  kno_free_stackvec(inits);
  kno_decref_stackvec(types);
  kno_free_stackvec(types);

  u8_close_output(docout);

  if (enclose) {
    struct KNO_PAIR *closure = u8_alloc(struct KNO_PAIR);
    KNO_INIT_CONS(closure,kno_closure_type);
    closure->car = LISP_CONS(s);
    closure->cdr = (lispval) kno_copy_env(env);
    return LISP_CONS(closure);}
  else return LISP_CONS(s);

 err_exit:
  kno_decref_stackvec(args);
  kno_free_stackvec(args);
  kno_decref_stackvec(inits);
  kno_free_stackvec(inits);
  kno_decref_stackvec(types);
  kno_free_stackvec(types);
  if (s->fcn_name) u8_free(s->fcn_name);
  u8_free(s);
  return result;
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

/* Restoring lambda objects */

static lispval *vector2elts(lispval vec,ssize_t n)
{
  if (KNO_VECTORP(vec)) {
    ssize_t len = KNO_VECTOR_LENGTH(vec);
    ssize_t alloc_len = (len>n) ? (len) : (n);
    lispval *elts = KNO_VECTOR_ELTS(vec);
    lispval *copy = u8_alloc_n(alloc_len,lispval);
    ssize_t i=0; while (i<len) {
      lispval elt = elts[i]; kno_incref(elt); copy[i++]=elt;}
    while (i<alloc_len) copy[i++]=KNO_VOID;
    return copy;}
  else return NULL;
}

lispval restore_lambda(lispval name,lispval attribs,lispval env,
		       lispval xschema,lispval xinits,lispval xtypes,
		       lispval args,lispval body,lispval doc,lispval source,
		       lispval entry)
{
  u8_string namestring = (SYMBOLP(name)) ? (SYMBOL_NAME(name)) :
    (STRINGP(name)) ? (CSTRING(name)) : (NULL);
  if (!(KNO_VECTORP(xschema)))
    return kno_err("NotAVector","restore_lambda/schema",namestring,xschema);
  int n = KNO_VECTOR_LENGTH(xschema);

  int sync = 0, nd = 0;
  struct KNO_LAMBDA *s = u8_alloc(struct KNO_LAMBDA);
  lispval result = KNO_ERROR;
  KNO_INIT_FRESH_CONS(s,kno_lambda_type);

  if (KNO_TABLEP(attribs)) {
    if ( (kno_test(attribs,KNOSYM(synchronized),KNO_VOID)) ||
	 (kno_test(attribs,KNOSYM(sync),KNO_VOID)) )
      sync=1;
    if ( (kno_test(attribs,KNOSYM(choiceop),KNO_VOID)) ||
	 (kno_test(attribs,KNOSYM(choicefn),KNO_VOID)) ||
	 (kno_test(attribs,KNOSYM(ndcall),KNO_VOID)) ) {
      s->fcn_call = (KNO_CALL_NDCALL);
      nd=1;}
    if (namestring == NULL) {
      lispval name_opt = kno_get(attribs,KNOSYM_NAME,KNO_VOID);
      if (KNO_STRINGP(name_opt))
	namestring=KNO_CSTRING(name_opt);
      else if (KNO_SYMBOLP(name_opt))
	namestring=KNO_SYMBOL_NAME(name_opt);
      else NO_ELSE;
      kno_decref(name_opt);}}
  s->fcn_name = u8_strdup(namestring);

  s->fcn_handler.xcalln = (kno_xprimn) lambda_docall;
  s->fcn_filename = NULL;
  s->fcn_attribs = kno_incref(attribs);
  s->lambda_consblock = NULL;
  s->lambda_source = kno_incref(source);

  s->lambda_body = kno_incref(body);
  s->lambda_arglist = kno_incref(body);
  s->lambda_entry = kno_incref(entry);

  if (KNO_STRINGP(doc)) {
    s->fcn_doc = u8_strdup(KNO_CSTRING(doc));
    s->fcn_free |= KNO_FCN_FREE_DOC;}

  lispval *schema = u8_alloc_n(n,lispval);
  kno_set_lambda_schema
    (s,n,VECTOR_ELTS(xschema),
     (VECTORP(xinits)) ? (VECTOR_ELTS(xinits)) : (NULL),
     (VECTORP(xtypes)) ? (VECTOR_ELTS(xtypes)) : (NULL));

  kno_lexenv use_env = (KNO_LEXENVP(env)) ? ((kno_lexenv)env) : (NULL);
  if ( (use_env== NULL) && ( (KNO_SYMBOLP(env)) || (KNO_STRINGP(env)) ) ) {}

  return result;
}

/*
  name (symbol or string or #f);
  opts{arity,min_arity,ndetc} (table);
  attribs (table);
  filename (string or false);
  module (symbol or false);
  schema (vector);
  inits (vector);
  types (vector);
  arglist (list);
  body (list);
  source (expr);
  doc (string or false);
  entry (expr);
  env (symbol or string or #f);
*/

/* Copying lambda objects */

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

    fresh->lambda_entry = fresh->lambda_body;
    fresh->lambda_consblock = NULL;

    if (U8_BITP(flags,KNO_STATIC_COPY)) {
      KNO_MAKE_CONS_STATIC(fresh);}

    return (lispval) fresh;}
}

KNO_EXPORT lispval copy_closure(lispval c,int flags)
{
  struct KNO_PAIR *closure = (kno_pair) c;
  kno_function info = KNO_FUNCTION_INFO(closure->car);
  if (info->fcn_free & KNO_FCN_DONT_COPY) return kno_incref(c);
  struct KNO_PAIR *newc = u8_alloc(struct KNO_PAIR);
  KNO_INIT_CONS(newc,kno_closure_type);
  if (flags&KNO_SIMPLE_COPY) {
    newc->car=kno_incref(closure->car);
    newc->cdr=kno_incref(closure->cdr);}
  else if (flags&KNO_DEEP_COPY) {
    newc->car=kno_copier(closure->car,flags);
    newc->cdr=kno_copier(closure->cdr,flags);}
  else {
    newc->car=kno_incref(closure->car);
    newc->cdr=kno_incref(closure->cdr);}
  return LISP_CONS(newc);
}

/* Recycling lambda objects */

KNO_EXPORT void recycle_lambda(struct KNO_RAW_CONS *c)
{
  struct KNO_LAMBDA *lambda = (struct KNO_LAMBDA *)c;
  int mallocd = KNO_MALLOCD_CONSP(c), n_vars = lambda->lambda_n_vars;
  int free_flags = lambda->fcn_free;
  if (lambda->fcn_profile) {
    if (lambda->fcn_profile->prof_label)
      u8_free(lambda->fcn_profile->prof_label);
    u8_free(lambda->fcn_profile);
    lambda->fcn_profile = NULL;}
  if ( (lambda->lambda_env) &&
       (lambda->lambda_env->env_copy) ) {
    kno_decref((lispval)(lambda->lambda_env->env_copy));}
  if (lambda->lambda_template) {
    lispval template = (lispval) lambda->lambda_template;
    if (KNO_MALLOCD_CONSP(template)) kno_decref(template);
    if (mallocd) {
      memset(lambda,0,sizeof(struct KNO_LAMBDA));
      u8_free(lambda);}
    return;}
  if ( (lambda->fcn_doc) && ( (free_flags) & (KNO_FCN_FREE_DOC) ) ) {
    u8_free(lambda->fcn_doc);
    lambda->fcn_doc = NULL;}
  if (lambda->fcn_attribs) kno_decref(lambda->fcn_attribs);
  if (lambda->fcn_moduleid) kno_decref(lambda->fcn_moduleid);
  kno_decref(lambda->lambda_arglist);
  kno_decref(lambda->lambda_source);
  u8_free(lambda->lambda_vars);
  if (lambda->lambda_inits) {
    kno_decref_elts(lambda->lambda_inits,n_vars);
    u8_free(lambda->lambda_inits);
    lambda->lambda_inits=NULL;}
  if (lambda->lambda_consblock) {
    lispval cb = (lispval) lambda->lambda_consblock;
    lambda->lambda_consblock=NULL;
    kno_decref(cb);}
  else if (lambda->lambda_entry != lambda->lambda_body) {
    kno_decref(lambda->lambda_entry);}
  else {}
  lambda->lambda_entry = KNO_VOID;
  kno_decref(lambda->lambda_body);

  if (lambda->lambda_synchronized)
    u8_destroy_mutex(&(lambda->lambda_lock));

  /* Put these last to help with debugging, when needed */
  if (lambda->fcn_name) u8_free(lambda->fcn_name);
  if (lambda->fcn_filename) u8_free(lambda->fcn_filename);
  if (mallocd) {
    memset(lambda,0,sizeof(struct KNO_LAMBDA));
    u8_free(lambda);}
}

KNO_EXPORT void recycle_closure(struct KNO_RAW_CONS *c)
{
  struct KNO_PAIR *closure = (kno_pair) c;
  int mallocd = KNO_MALLOCD_CONSP(c);
  kno_decref(closure->car);
  kno_decref(closure->cdr);
  if (mallocd) u8_free(closure);
}

/* Unparsing lambdas */

static void output_callsig(u8_output out,lispval arglist);

static int unparse_lambda_helper(u8_output out,lispval x,int closure)
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
    u8_printf(out,"#<λ$%s%s%s",
	      (closure)?("C"):(""),codes,lambda->fcn_name);
  else u8_printf(out,"#<λ%s%s0x%04x",
		 (closure)?("C"):(""),codes,((addr>>2)%0x10000));
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
    u8_printf(out," #!%p",lambda);
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

static int unparse_lambda(u8_output out,lispval x)
{
  return unparse_lambda_helper(out,x,0);
}

static int unparse_closure(u8_output out,lispval x)
{
  struct KNO_PAIR *closure = (kno_pair)x;
  return unparse_lambda_helper(out,(closure->car),1);
}

static void set_lambda_source(lispval proc,lispval source)
{
  if (KNO_TYPEP(proc,kno_lambda_type)) {
    KNO_SET_LAMBDA_SOURCE(proc,source);}
  else if (KNO_TYPEP(proc,kno_closure_type)) {
    struct KNO_PAIR *closure = (kno_pair) proc;
    struct KNO_LAMBDA *lambda = (KNO_TYPEP(closure->car,kno_lambda_type)) ?
      ((kno_lambda)(closure->car)) : (NULL);
    if (lambda) {
      KNO_SET_LAMBDA_SOURCE(lambda,source);}}
  else NO_ELSE;
}

/* LAMBDA generators */

DEFC_EVALFN("lambda",lambda_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(lambda *args* *body...*)` returns a "
	    "lambda procedure.")
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
  set_lambda_source(proc,expr);
  return proc;
}

DEFC_EVALFN("nlambda",nlambda_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(nlambda *name* *args* *body...*)` returns a "
	    "named lambda procedure.")
static lispval nlambda_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval name_expr = kno_get_arg(expr,1), name;
  lispval arglist = kno_get_arg(expr,2);
  lispval body = kno_get_body(expr,3);
  lispval proc = VOID;
  u8_string namestring = NULL;
  if ((VOIDP(name_expr))||(VOIDP(arglist)))
    return kno_err(kno_TooFewExpressions,"NLAMBDA",NULL,expr);
  else name = kno_eval(name_expr,env,_stack);
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
  set_lambda_source(proc,expr);
  return proc;
}

/* This supports a bunch of syntatic variants of lambda with various
   flag values (nd,synchronous,iterator) as well as (named) which indicates
   whether the first element of the signature (the second element of the expr)
   should be used as the display name of the lambda */
static lispval lambda_helper(lispval lambda_spec,
			     kno_lexenv env,kno_stack stack,
			     int named,int nd,int sync,
			     int iterator)
{
  lispval template = kno_get_arg(lambda_spec,1);
  if ( (named) && (!(KNO_PAIRP(template))) )
    return kno_err(kno_SyntaxError,(KNO_SYMBOL_NAME(KNO_CAR(lambda_spec))),
		   NULL,template);
  else if (!( (template==KNO_EMPTY_LIST) || (KNO_PAIRP(template)) ) )
    return kno_err(kno_SyntaxError,(KNO_SYMBOL_NAME(KNO_CAR(lambda_spec))),
		   NULL,template);
  else NO_ELSE;
  lispval name = (named) ? (KNO_CAR(template)) : (KNO_VOID);
  lispval arglist = (named) ? (KNO_CDR(template)) : template;
  lispval body = kno_get_body(lambda_spec,2);
  lispval proc = VOID;
  u8_string namestring = NULL;
  if (named) {
    if (SYMBOLP(name)) namestring = SYM_NAME(name);
    else if (STRINGP(name)) namestring = CSTRING(name);
    else return kno_type_error
	   ("procedure name (string or symbol)",KNO_SYMBOL_NAME(KNO_CAR(lambda_spec)),
	    name);}
  proc=make_lambda(namestring,arglist,body,env,stack,nd,sync);
  if (KNO_ABORTED(proc))
    return proc;
  if (iterator) {
    kno_lambda info = KNO_LAMBDA_INFO(proc);
    if (!(info)) return kno_err("BadLambda","lambda_helper",NULL,lambda_spec);
    if (info->fcn_min_arity>0) {
      lispval err = kno_err("BadIterator","lambda_helper",NULL,lambda_spec);
      kno_decref(proc);
      return err;}
    else info->fcn_other |= KNO_OTHER_ITERATOR;}
  set_lambda_source(proc,lambda_spec);
  return proc;
}

DEFC_EVALFN("defn",defn_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(defn (*name* *..args..*) *body...*)` returns a "
	    "named lambda procedure.")
static lispval defn_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return lambda_helper(expr,env,_stack,1,0,0,0);
}

DEFC_EVALFN("choicefn",choicefn_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(choicefn *args* *body...*)` returns a "
	    "choicefn (lambda) which does not automatically iterate over "
	    "non-determinstic arguments, but receives those arguments "
	    "as a direct value.")
static lispval choicefn_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return lambda_helper(expr,env,_stack,0,1,0,0);
}

DEFC_EVALFN("nchoicefn",nchoicefn_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(nchoicefn (*name* *..args..*) *body...*)` returns a "
	    "choicefn (lambda) which does not automatically iterate over "
	    "non-determinstic arguments, but receives those arguments "
	    "as a direct value.")
static lispval nchoicefn_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return lambda_helper(expr,env,_stack,1,1,0,0);
}

DEFC_EVALFN("syncfn",syncfn_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(syncfn *args* *body...*)` returns a synchronized "
	    "lambda which can only called by one thread at a time.")
static lispval syncfn_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return lambda_helper(expr,env,_stack,0,0,1,0);
}

DEFC_EVALFN("nsyncfn",nsyncfn_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(nsyncfn (*name* . *args...*) *body...*)` returns a named "
	    "synchronized lambda which can only called by one thread at a "
	    "time.")
static lispval nsyncfn_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return lambda_helper(expr,env,_stack,1,0,1,0);
}

DEFC_EVALFN("iterator",iterator_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(iterator *args* *body...*)` returns an iterator lambda.")
static lispval iterator_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return lambda_helper(expr,env,_stack,0,0,0,1);
}

DEFC_EVALFN("niterator",niterator_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(niterator *args* *body...*)` returns an iterator lambda.")
static lispval niterator_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  return lambda_helper(expr,env,_stack,1,0,0,1);
}

DEFC_EVALFN("thunkfn",thunkfn_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(thunkfn *body...*)` returns a procedure of no arguments "
	    "which executes *body*.")
static lispval thunkfn_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval body = kno_get_body(expr,1);
  lispval proc = make_lambda(NULL,NIL,body,env,_stack,0,0);
  if (KNO_ABORTED(proc))
    return proc;
  set_lambda_source(proc,expr);
  return proc;
}

/* DEFINE */

static int init_definition(lispval fcn,lispval expr,kno_lexenv env)
{
  struct KNO_FUNCTION *f = KNO_FUNCTION_INFO(fcn);
  if (f == NULL) return 0;
  if ( (KNO_NULLP(f->fcn_moduleid)) || (KNO_VOIDP(f->fcn_moduleid)) ) {
    lispval moduleid = kno_get(env->env_bindings,KNOSYM_MODULEID,KNO_VOID);
    if (!(KNO_VOIDP(moduleid)))
      f->fcn_moduleid = moduleid;}
  if (f->fcn_filename == NULL) {
    u8_string sourcebase = kno_sourcebase();
    if (sourcebase) f->fcn_filename = u8_strdup(sourcebase);}
  if ( (f) && (kno_record_source) && (KNO_FCN_TYPEP(f,kno_lambda_type)) )  {
    struct KNO_LAMBDA *l = (kno_lambda) f;
    if ( (KNO_NULLP(l->lambda_source)) || (KNO_VOIDP(l->lambda_source)) ) {
      l->lambda_source=expr;
      kno_incref(expr);}}
  return 1;
}

DEFC_EVALFN("define",define_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(define *name* *expr*)` \n"
	    "`(define (*name* *args...*) *body..*)`\n"
	    "either defines a local variable *name* initialized "
	    "to *value* or defines *name* to be a procedure "
	    "in the current environment with *args* and *body*.")
static lispval define_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval var = kno_get_arg(expr,1);
  if (VOIDP(var))
    return kno_err(kno_TooFewExpressions,"DEFINE",NULL,expr);
  else if (SYMBOLP(var)) {
    lispval val_expr = kno_get_arg(expr,2);
    if (VOIDP(val_expr))
      return kno_err(kno_TooFewExpressions,"DEFINE",NULL,expr);
    lispval value = kno_eval(val_expr,env,_stack);
    if (KNO_ABORTED(value))
      return value;
    else if (kno_bind_value(var,value,env)>=0) {
      if (init_definition(value,expr,env)) {}
      else {
	lispval fvalue = (KNO_QONSTP(value))?(kno_qonst_val(value)):(value);
	if (KNO_MACROP(fvalue)) {
	  struct KNO_MACRO *macro = (kno_macro) fvalue;
	  if (KNO_VOIDP(macro->macro_moduleid)) {
	    macro->macro_moduleid =
	      kno_get(env->env_bindings,KNOSYM_MODULEID,KNO_VOID);}
	if (macro->macro_name == NULL)
	  macro->macro_name = u8_strdup(KNO_SYMBOL_NAME(var));}
	else NO_ELSE;}
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
	init_definition(value,expr,env);
	kno_decref(value);
	return VOID;}
      else {
	kno_decref(value);
	return kno_err(kno_BindError,"DEFINE",SYM_NAME(fn_name),var);}}}
  else return kno_err(kno_NotAnIdentifier,"DEFINE",NULL,var);
}

DEFC_EVALFN("defexport",defexport_evalfn,KNO_EVALFN_DEFAULTS,
	    "`defexport` is just like `define` but also exports "
	    "the defined name from the current module.")
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

DEFC_EVALFN("defslambda",defslambda_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(defslambda (*name* *args...*) *body..*)`\n"
	    "defines *name* to be a sychronized procedure "
	    "in the current environment with *args* and *body*.")
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
	init_definition(value,expr,env);
	kno_decref(value);
	return VOID;}
      else {
	kno_decref(value);
	return kno_err(kno_BindError,"DEFINE-SYNCHRONIZED",
		       SYM_NAME(var),var);}}}
  else return kno_err(kno_NotAnIdentifier,"DEFINE-SYNCHRONIZED",NULL,var);
}

DEFC_EVALFN("defambda",defambda_evalfn,KNO_EVALFN_DEFAULTS,
	    "`(defambda (*name* *args...*) *body..*)`\n"
	    "defines *name* to be a non-deterministic procedure "
	    "in the current environment with *args* and *body*.")
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
	init_definition(value,expr,env);
	kno_decref(value);
	return VOID;}
      else {
	kno_decref(value);
	return kno_err(kno_BindError,"DEFINE-AMB",SYM_NAME(fn_name),var);}}}
  else return kno_err(kno_NotAnIdentifier,"DEFINE-AMB",NULL,var);
}

/* LAMBDA */

KNO_EXPORT kno_lambda _KNO_LAMBDA_INFO(lispval x)
{
  if (KNO_QONSTP(x)) x = kno_qonst_val(x);
  if (KNO_TYPEP(x,kno_lambda_type))
    return (kno_lambda)x;
  else if ( (KNO_TYPEP(x,kno_closure_type)) &&
	    (KNO_TYPEP((((kno_pair)x)->car),kno_lambda_type)) )
    return (kno_lambda) (((kno_pair)x)->car);
  else return NULL;
}

KNO_EXPORT kno_lexenv _KNO_LAMBDA_ENV(lispval x)
{
  if (KNO_QONSTP(x)) x = kno_qonst_val(x);
  if (KNO_TYPEP(x,kno_lambda_type))
    return ((kno_lambda)x)->lambda_env;
  else if ( (KNO_TYPEP(x,kno_closure_type)) &&
	    (KNO_TYPEP((((kno_pair)x)->car),kno_lambda_type)) )
    return (kno_lexenv) (((kno_pair)x)->cdr);
  else return NULL;
}


/* Extended apply */

static lispval tail_symbol;

KNO_EXPORT
/* kno_xapply_lambda:
   Arguments: a pointer to an lambda, a void* data pointer, and a function
   of a void* pointer and a lisp pointer
   Returns: the application result
   This uses an external function to get parameter values from some
   other data structure (cast as a void* pointer).  This is used, for instance,
   to expose CGI data fields as arguments to a main function, or to
   apply XML attributes and elements similarly. */
lispval kno_xapply_lambda
(lispval handler,void *data,lispval (*getval)(void *,lispval))
{
  struct KNO_LAMBDA *fn = KNO_LAMBDA_INFO(handler);
  if (fn) {
    kno_lexenv env = (KNO_TYPEP(handler,kno_closure_type)) ?
      ((kno_lexenv)(((kno_pair)handler)->cdr)) : (fn->lambda_env);
    u8_string label = (fn->fcn_name) ? (fn->fcn_name) : ((u8_string)"xapply");
    KNO_START_EVALX(_stack,label,((lispval)fn),NULL,kno_stackptr,7);
    int n = fn->lambda_n_vars;
    lispval arglist = fn->lambda_arglist, result = VOID;
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
	lispval default_value = kno_eval(default_expr,env,_stack);
	kno_schemap_store(&call_env_bindings,argname,default_value);
	kno_decref(default_value);}
      else {
	kno_schemap_store(&call_env_bindings,argname,argval);
	kno_decref(argval);}
      arglist = KNO_CDR(arglist);}
    if (!(ABORTED(result))) {
      /* If we're synchronized, lock the mutex. */
      if (fn->lambda_synchronized) u8_lock_mutex(&(fn->lambda_lock));
      result = eval_body(fn->lambda_entry,call_env,_stack,
			 ":XPROC",fn->fcn_name,0);
      if (fn->lambda_synchronized)
	u8_unlock_mutex(&(fn->lambda_lock));}
    kno_pop_stack(_stack);
    return result;}
  else return kno_type_error("lambda","kno_xapply_proc",handler);
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

DEFC_PRIM("xapply",xapply_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"proc",kno_any_type,KNO_VOID},
	  {"obj",kno_any_type,KNO_VOID},
	  {"getfn",kno_any_type,KNO_VOID})
static lispval xapply_prim(lispval proc,lispval obj,lispval getfn)
{
  if ( (KNO_VOIDP(getfn)) || (KNO_DEFAULTP(getfn)) || (KNO_FALSEP(getfn)) ) {
    if (!(TABLEP(obj)))
      return kno_type_error("table","xapply_prim",obj);
    else return kno_xapply_lambda(proc,(void *)obj,tablegetval);}
  else {
    struct KNO_PAIR _xobj = { 0 };
    KNO_INIT_STACK_CONS(&_xobj,kno_pair_type);
    _xobj.car = getfn; _xobj.cdr = obj;
    return kno_xapply_lambda(proc,(void *)&_xobj,xapplygetval);}
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
  else if ( (env) && (!(KNO_STATICP(env))) &&
	    (kno_walk(walker,env,walkdata,flags,depth-1)<0) )
    return -1;
  else return 3;
}

static int walk_closure(kno_walker walker,lispval obj,void *walkdata,
			kno_walk_flags flags,int depth)
{
  struct KNO_PAIR *closure = (kno_pair)obj;
  lispval lambda = closure->car;
  lispval env = closure->cdr;
  if (kno_walk(walker,lambda,walkdata,flags,depth-1)<0)
    return -1;
  else if ( (!(KNO_STATICP(env))) &&
	    (kno_walk(walker,env,walkdata,flags,depth-1)<0) )
    return -1;
  else return 3;
}

#define BUFOUT_FLAGS							\
  (KNO_IS_WRITING|KNO_BUFFER_NO_FLUSH|KNO_USE_DTYPEV2|KNO_WRITE_OPAQUE)

static ssize_t write_lambda_dtype(struct KNO_OUTBUF *out,lispval x)
{
  int n_elts=1; /* Always include some source */
  struct KNO_LAMBDA *fcn = KNO_LAMBDA_INFO(x);
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
  defaultbang_symbol = kno_intern("default!");
  setbang_symbol = kno_intern("set!");

  kno_applyfns[kno_lambda_type]=apply_lambda;
  kno_isfunctionp[kno_lambda_type]=1;

  kno_applyfns[kno_closure_type]=apply_lambda;
  kno_isfunctionp[kno_closure_type]=0;

  kno_unparsers[kno_lambda_type]=unparse_lambda;
  kno_recyclers[kno_lambda_type]=recycle_lambda;
  kno_walkers[kno_lambda_type]=walk_lambda;

  kno_unparsers[kno_closure_type]=unparse_closure;
  kno_recyclers[kno_closure_type]=recycle_closure;
  kno_walkers[kno_closure_type]=walk_closure;

  kno_dtype_writers[kno_lambda_type] = write_lambda_dtype;
  kno_dtype_writers[kno_closure_type] = write_lambda_dtype;
  kno_copiers[kno_lambda_type] = copy_lambda;
  kno_copiers[kno_closure_type] = copy_closure;

  kno_register_config("KNO:ENCLOSE_LAMBDAS","Wrap all lambdas in closure objects",
		      kno_boolconfig_get,kno_boolconfig_set,
		      &kno_enclose_lambdas);

  link_local_cprims();

  LINK_EVALFN(kno_scheme_module,lambda_evalfn);
  KNO_LINK_ALIAS("fn",lambda_evalfn,kno_scheme_module);
  LINK_EVALFN(kno_scheme_module,nlambda_evalfn);
  LINK_EVALFN(kno_scheme_module,defn_evalfn);
  KNO_LINK_ALIAS("namedfn",defn_evalfn,kno_scheme_module);

  LINK_EVALFN(kno_scheme_module,choicefn_evalfn);
  KNO_LINK_ALIAS("ambda",choicefn_evalfn,kno_scheme_module);

  LINK_EVALFN(kno_scheme_module,nchoicefn_evalfn);
  KNO_LINK_ALIAS("defambfn",nchoicefn_evalfn,kno_scheme_module);
  KNO_LINK_ALIAS("defamb",nchoicefn_evalfn,kno_scheme_module);
  KNO_LINK_ALIAS("nambda",nchoicefn_evalfn,kno_scheme_module);

  LINK_EVALFN(kno_scheme_module,syncfn_evalfn);
  KNO_LINK_ALIAS("slambda",syncfn_evalfn,kno_scheme_module);

  LINK_EVALFN(kno_scheme_module,nsyncfn_evalfn);
  KNO_LINK_ALIAS("defsyncfn",nsyncfn_evalfn,kno_scheme_module);
  KNO_LINK_ALIAS("nslambda",nsyncfn_evalfn,kno_scheme_module);
  KNO_LINK_ALIAS("defsync",nsyncfn_evalfn,kno_scheme_module);

  LINK_EVALFN(kno_scheme_module,iterator_evalfn);
  KNO_LINK_ALIAS("iterfn",iterator_evalfn,kno_scheme_module);

  LINK_EVALFN(kno_scheme_module,niterator_evalfn);
  KNO_LINK_ALIAS("niterfn",niterator_evalfn,kno_scheme_module);

  LINK_EVALFN(kno_scheme_module,thunkfn_evalfn);
  KNO_LINK_ALIAS("thunk",thunkfn_evalfn,kno_scheme_module);

  LINK_EVALFN(kno_scheme_module,define_evalfn);
  KNO_LINK_ALIAS("def",define_evalfn,kno_scheme_module);

  LINK_EVALFN(kno_scheme_module,defexport_evalfn);
  LINK_EVALFN(kno_scheme_module,defslambda_evalfn);
  LINK_EVALFN(kno_scheme_module,defambda_evalfn);
  KNO_LINK_ALIAS("defchoicefn",defambda_evalfn,kno_scheme_module);

}

static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;
  KNO_LINK_CPRIM("xapply",xapply_prim,3,scheme_module);
}
