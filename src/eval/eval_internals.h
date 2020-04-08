u8_condition BadExpressionHead;

lispval get_evalop(lispval head,kno_lexenv env,kno_stack eval_stack,int *freep);

lispval schemap_eval(lispval expr,kno_lexenv env,kno_stack stack);
lispval choice_eval(lispval expr,kno_lexenv env,kno_stack stack);

lispval lambda_call(kno_stack stack,
		    struct KNO_LAMBDA *proc,
		    int n,kno_argvec args,
		    int tail);
lispval core_eval(lispval,kno_lexenv,kno_stack,int);

KNO_FASTOP lispval fastget(lispval table,lispval key)
{
  kno_lisp_type argtype = KNO_TYPEOF(table);
  switch (argtype) {
  case kno_schemap_type:
    return kno_schemap_get((kno_schemap)table,key,KNO_UNBOUND);
  case kno_hashtable_type:
    return kno_hashtable_get((kno_hashtable)table,key,KNO_UNBOUND);
  default: return kno_get(table,key,KNO_UNBOUND);}
}
KNO_FASTOP lispval __kno_lexref(lispval lexref,kno_lexenv env_arg)
{
  int code = KNO_GET_IMMEDIATE(lexref,kno_lexref_type);
  int up = code/32, across = code%32;
  kno_lexenv env = env_arg;
  while ((env) && (up)) {
    if (env->env_copy) env = env->env_copy;
    env = env->env_parent;
    up--;}
  if (KNO_EXPECT_TRUE(env != NULL)) {
    if (env->env_copy) env = env->env_copy;
    lispval bindings = env->env_bindings;
    if (KNO_EXPECT_TRUE(KNO_SCHEMAPP(bindings))) {
      struct KNO_SCHEMAP *s = (struct KNO_SCHEMAP *)bindings;
      if ( across < s->schema_length) {
	lispval v = s->table_values[across];
	if (KNO_PRECHOICEP(v))
	  return kno_make_simple_choice(v);
	else return kno_incref(v);}}}
  lispval env_ptr = (lispval) env_arg;
  u8_byte errbuf[64];
  return kno_err("Bad lexical reference","kno_lexref",
                 u8_bprintf(errbuf,"up=%d,across=%d",up, across),
                 ((KNO_STATICP(env_ptr)) ? KNO_FALSE : (env_ptr)));
}
KNO_FASTOP lispval __kno_symeval(lispval symbol,kno_lexenv env)
{
  if (env == NULL) return KNO_VOID;
  if (env->env_copy) env = env->env_copy;
  while (env) {
    lispval val = fastget(env->env_bindings,symbol);
    if (val == KNO_UNBOUND)
      env = env->env_parent;
    else return val;
    if ((env) && (env->env_copy))
      env = env->env_copy;}
  return KNO_VOID;
}
KNO_FASTOP lispval __kno_symbol_eval(lispval symbol,kno_lexenv env)
{
  if (env == NULL)
    return kno_err("NoEnvironment","eval_symbol",NULL,symbol);
  if (env->env_copy) env = env->env_copy;
  while (env) {
    lispval val = fastget(env->env_bindings,symbol);
    if (val == KNO_UNBOUND)
      env = env->env_parent;
    else if (val == KNO_VOID)
      break;
    else return val;
    if ((env) && (env->env_copy))
      env = env->env_copy;}
  return kno_err(kno_UnboundIdentifier,"kno_eval",
		 KNO_SYMBOL_NAME(symbol),symbol);
}
KNO_FASTOP lispval __kno_fcnid_eval(lispval fcnid)
{
  lispval v = kno_fcnid_ref(fcnid);
  kno_incref(v);
  return v;
}
KNO_FASTOP lispval __kno_get_arg(lispval expr,int i)
{
  while (KNO_PAIRP(expr))
    if ((KNO_PAIRP(KNO_CAR(expr))) &&
        (KNO_EQ(KNO_CAR(KNO_CAR(expr)),_kno_comment_symbol)))
      expr = KNO_CDR(expr);
    else if (i == 0) return KNO_CAR(expr);
    else {expr = KNO_CDR(expr); i--;}
  return KNO_VOID;
}
KNO_FASTOP lispval __kno_get_body(lispval expr,int i)
{
  while (KNO_PAIRP(expr))
    if (i == 0) break;
    else if ((KNO_PAIRP(KNO_CAR(expr))) &&
             (KNO_EQ(KNO_CAR(KNO_CAR(expr)),_kno_comment_symbol)))
      expr = KNO_CDR(expr);
    else {expr = KNO_CDR(expr); i--;}
  return expr;
}

KNO_FASTOP lispval __kno_eval(lispval x,kno_lexenv env,
			      kno_stack stack,
			      int tail)
{
  switch (KNO_PTR_MANIFEST_TYPE(x)) {
  case kno_oid_ptr_type: case kno_fixnum_ptr_type:
    return x;
  case kno_immediate_ptr_type: {
    switch (KNO_IMMEDIATE_TYPE(x)) {
    case kno_lexref_type:
      return __kno_lexref(x,env);
    case kno_symbol_type:
      return __kno_symbol_eval(x,env);
    case kno_fcnid_type:
      return __kno_fcnid_eval(x);
    default:
      return x;}}}
  kno_lisp_type type = KNO_CONSPTR_TYPE(x);
  switch (type) {
  case kno_pair_type:
    return kno_pair_eval(x,env,stack,tail);
  case kno_choice_type: case kno_prechoice_type:
    return choice_eval(x,env,stack);
  case kno_schemap_type:
    return schemap_eval(x,env,stack);
  case kno_slotmap_type:
    return kno_deep_copy(x);
  default:
    return kno_incref(x);}
}

static int testeval(lispval expr,kno_lexenv env,int fail_val,
		    lispval *whoops,struct KNO_STACK *s) U8_MAYBE_UNUSED;

static int testeval(lispval expr,kno_lexenv env,int fail_val,
		    lispval *whoops,
		    kno_stack _stack)
{
  lispval val = kno_eval(expr,env,_stack,0);
  if (KNO_CONSP(val)) {
    kno_decref(val);
    return 1;}
  else if (KNO_ABORTED(val)) {
    *whoops = val;
    return -1;}
  else if (KNO_FALSEP(val))
    return 0;
  else if (KNO_EMPTYP(val))
    return fail_val;
  else if (KNO_VOIDP(val)) {
    *whoops = KNO_ERROR;
    return KNO_ERR(-1,kno_VoidBoolean,"testeval",NULL,expr);}
  else return 1;
}
#define TESTEVAL_FAIL_FALSE 0
#define TESTEVAL_FAIL_TRUE  1

KNO_FASTOP lispval _pop_arg(lispval *scan)
{
  lispval expr = *scan;
  if (PAIRP(expr)) {
    lispval arg = KNO_CAR(expr), next = KNO_CDR(expr);
    if (KNO_CONSP(arg)) KNO_PREFETCH((struct KNO_CONS *)arg);
    if (KNO_CONSP(next)) KNO_PREFETCH((struct KNO_CONS *)next);
    *scan = next;
    return arg;}
  else return VOID;
}

#define pop_arg(args) (_pop_arg(&args))

#define simplify_value(v) \
  ( (KNO_PRECHOICEP(v)) ? (kno_simplify_choice(v)) : (v) )

KNO_FASTOP lispval eval_body(lispval body,kno_lexenv env,kno_stack stack,
			     u8_context cxt,u8_string label,
			     int tail)
{
  if (!(KNO_STACK_BITP(stack,KNO_STACK_TAIL_POS))) tail = 0;
  lispval scan = body;
  while (PAIRP(scan)) {
      lispval subex = pop_arg(scan);
      if (PAIRP(scan)) {
	lispval v = kno_eval(subex,env,stack,0);
	if (KNO_IMMEDIATEP(v)) {
	  if (KNO_ABORTED(v))
	    return v;
	  else if (v==KNO_TAIL) {
	    kno_seterr(kno_TailArgument,cxt,label,subex);
	    return KNO_ERROR;}}
	else kno_decref(v);}
      else if (KNO_EMPTY_LISTP(scan))
	return kno_eval(subex,env,stack,tail);
      else return kno_err(kno_SyntaxError,
			  U8ALT(cxt,"eval_body"),
			  label,
			  body);}
  if (KNO_EMPTY_LISTP(body))
    return KNO_VOID;
  else return kno_err(kno_SyntaxError,
		      U8ALT(cxt,"eval_body"),
		      label,
		      body);
}

KNO_FASTOP kno_lexenv init_static_env
  (int n,kno_lexenv parent,
   struct KNO_SCHEMAP *bindings,struct KNO_LEXENV *envstruct,
   lispval *vars,lispval *vals)
{
  memset(envstruct,0,sizeof(struct KNO_LEXENV));
  memset(bindings,0,KNO_SCHEMAP_LEN);
  KNO_INIT_STATIC_CONS(envstruct,kno_lexenv_type);
  KNO_INIT_STATIC_CONS(bindings,kno_schemap_type);
  bindings->schemap_onstack = 1;
  bindings->table_schema = vars;
  bindings->table_values = vals;
  bindings->schema_length = n;
  bindings->table_uselock = 0;
  // u8_init_rwlock(&(bindings->table_rwlock));
  envstruct->env_bindings = LISP_CONS((bindings));
  envstruct->env_exports = KNO_VOID;
  envstruct->env_parent = parent;
  envstruct->env_copy = NULL;
  return envstruct;
}

static U8_MAYBE_UNUSED
void release_stack_env(struct KNO_STACK *stack)
{
  kno_lexenv env = stack->eval_env;
  if (env) {
    stack->stack_bits &= (~(KNO_STACK_FREE_ENV));
    stack->eval_env = NULL;
    if (env->env_copy) {
      kno_free_lexenv(env->env_copy);
      env->env_copy=NULL;}
    if (KNO_SCHEMAPP(env->env_bindings)) {
      struct KNO_SCHEMAP *map = (kno_schemap) env->env_bindings;
      lispval *vals = map->table_values;
      int i = 0, n = map->schema_length;
      while (i < n) {
	kno_decref(vals[i]);
	i++;}}}
}

#define INIT_STACK_ENV(stack,name,parent,n)		    \
  struct KNO_SCHEMAP name ## _bindings;			    \
  struct KNO_LEXENV _ ## name, *name=&_ ## name;	    \
  lispval name ## _vars[n];				    \
  lispval name ## _vals[n];				    \
  kno_init_elts(name ## _vars,n,KNO_VOID);		    \
  kno_init_elts(name ## _vals,n,KNO_VOID);		    \
  stack->stack_bits |= KNO_STACK_FREE_ENV;		    \
  stack->stack_args.elts = name ## _vals;		    \
  stack->stack_args.count = n;				    \
  stack->stack_args.len = n;				    \
  stack->eval_env =					    \
    init_static_env(n,parent,				    \
		    &name ## _bindings,			    \
		    &_ ## name,				    \
		    name ## _vars,			    \
		    name ## _vals);			    \
  KNO_STACK_SET((stack),KNO_STACK_OWNS_ENV)

#define INIT_STACK_SCHEMA(stack,name,parent,n,schema)	    \
  struct KNO_SCHEMAP name ## _bindings;			    \
  struct KNO_LEXENV _ ## name, *name=&_ ## name;	    \
  lispval name ## _vals[n];				    \
  kno_init_elts(name ## _vals,n,KNO_VOID);		    \
  stack->stack_args.elts = name ## _vals;		    \
  stack->stack_args.count = n;				    \
  stack->stack_args.len = n;				    \
  stack->stack_bits |= KNO_STACK_FREE_ENV;		    \
  stack->eval_env =					    \
    init_static_env(n,parent,				    \
		    &name ## _bindings,			    \
		    &_ ## name,				    \
		    schema,				    \
		    name ## _vals);			    \
  KNO_STACK_SET((stack),KNO_STACK_OWNS_ENV)

#define ENV_STACK_FLAGS (KNO_STACK_OWNS_ENV|KNO_STACK_FREE_ENV)

#define PUSH_STACK_ENV(name,parent,n,caller)		     \
  struct KNO_STACK _ ## name ## _stack = { 0 };		     \
  struct KNO_STACK *name ## _stack = &(_ ## name ## _stack); \
  struct KNO_SCHEMAP name ## _bindings;			     \
  struct KNO_LEXENV _ ## name, *name=&_ ## name;	     \
  lispval name ## _vars[n];				     \
  lispval name ## _vals[n];				     \
  KNO_SETUP_STACK((name ## _stack),# name);		\
  KNO_STACK_SET_BITS(name ## _stack,ENV_STACK_FLAGS); \
  KNO_STACK_SET_CALLER((name ## _stack),(caller));			\
  kno_init_elts(name ## _vars,n,KNO_VOID);				\
  kno_init_elts(name ## _vals,n,KNO_VOID);				\
  (name ## _stack)->stack_bits |= KNO_STACK_FREE_ENV;			\
  (name ## _stack)->stack_args.elts = name ## _vals;			\
  (name ## _stack)->stack_args.count = n;				\
  (name ## _stack)->stack_args.len = n;					\
  (name ## _stack)->eval_env =						\
    init_static_env(n,parent,						\
		    &name ## _bindings,					\
		    &_ ## name,						\
		    name ## _vars,					\
		    name ## _vals);					\
  KNO_PUSH_STACK(name ## _stack)


KNO_FASTOP U8_MAYBE_UNUSED
void reset_env(kno_lexenv env)
{
  /* This decrefs any dynamic copy of this env, resetting it to its initial bindings. */
  if (env->env_copy) {
    kno_free_lexenv(env->env_copy);
    env->env_copy=NULL;}
}

KNO_FASTOP U8_MAYBE_UNUSED
void reset_stack_env(kno_stack stack)
{
  kno_lexenv env = stack->eval_env;
  if (PRED_TRUE(env != NULL)) {
    stack->eval_env=NULL;
    stack->stack_bits &= (~(KNO_STACK_FREE_ENV));
    if (env->env_copy) {
      kno_free_lexenv(env->env_copy);
      env->env_copy=NULL;}}
}

#define set_stack_label(stack,label) (stack)->stack_label=label

#define KNO_INIT_ITER_LOOP(name,var,value,n_vars,caller,parent_env)	\
  u8_byte iter_label_buf[32];						\
  u8_string label = (KNO_SYMBOLP(var)) ?				\
    u8_bprintf(iter_label_buf,"%s.%s", # name,KNO_SYMBOL_NAME(var)) :	\
    ((u8_string)(# name));						\
  KNO_START_EVALX(name ## _stack,# name,expr,NULL,caller,5);		\
  set_stack_label((kno_stack)caller,label);				\
  kno_add_stack_ref(caller,value);					\
  INIT_STACK_ENV(name ## _stack,name,parent_env,n_vars);

/* Environment utilities */

KNO_FASTOP int check_bindexprs(lispval bindexprs,lispval *why_not)
{
  if (PAIRP(bindexprs)) {
    int n = 0; KNO_DOLIST(bindexpr,bindexprs) {
      lispval var = kno_get_arg(bindexpr,0);
      if (VOIDP(var)) {
	*why_not = kno_err(kno_BindSyntaxError,NULL,NULL,bindexpr);
	return -1;}
      else n++;}
    return n;}
  else return -1;
}

