static U8_MAYBE_UNUSED lispval moduleid_symbol;

#define fast_eval(x,env) (kno_eval(x,env,_stack,0))
#define stack_eval(x,env,s) (kno_eval(x,env,s,0))

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

KNO_FASTOP lispval eval_body(lispval body,kno_lexenv env,
			     kno_stack stack,
			     u8_context cxt,u8_string label,
			     int tail)
{
  if (KNO_EMPTY_LISTP(body))
    return KNO_VOID;
  else if (!(KNO_PAIRP(body)))
    return kno_err(kno_SyntaxError,
		   ( (cxt) && (label) ) ? (cxt) :
		   ((u8_string)"eval_inner_body"),
		   (label) ? (label) : (cxt) ? (cxt) : (NULL),
		   body);
  lispval scan = body;
  while (PAIRP(scan)) {
      lispval subex = pop_arg(scan);
      if (PAIRP(scan)) {
	lispval v = stack_eval(subex,env,stack);
	if (KNO_ABORTED(v))
	  return v;
	else kno_decref(v);}
      else if (KNO_EMPTY_LISTP(scan))
	return kno_eval(subex,env,stack,tail);
      else return kno_err(kno_SyntaxError,
			  ( (cxt) && (label) ) ? (cxt) :
			  ((u8_string)"eval_inner_body"),
			  (label) ? (label) : (cxt) ? (cxt) : (NULL),
			  body);}
  return KNO_VOID;
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
  bindings->schema_values = vals;
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
      lispval *vals = map->schema_values;
      int i = 0, n = map->schema_length;
      while (i < n) {
	kno_decref(vals[i]);
	i++;}}}
}

#define INIT_STACK_ENV(stack,name,parent,n)	      \
  struct KNO_SCHEMAP name ## _bindings;		      \
  struct KNO_LEXENV _ ## name, *name=&_ ## name;      \
  lispval name ## _vars[n];			      \
  lispval name ## _vals[n];				    \
  kno_init_elts(name ## _vars,n,KNO_VOID);		    \
  kno_init_elts(name ## _vals,n,KNO_VOID);		    \
  stack->stack_bits |= KNO_STACK_FREE_ENV;		    \
  stack->eval_env =					    \
    init_static_env(n,parent,				    \
		    &name ## _bindings,			    \
		    &_ ## name,				    \
		    name ## _vars,			    \
		    name ## _vals)

#define INIT_STACK_SCHEMA(stack,name,parent,n,schema)	 \
  struct KNO_SCHEMAP name ## _bindings;			 \
  struct KNO_LEXENV _ ## name, *name=&_ ## name;	 \
  lispval name ## _vals[n];				  \
  kno_init_elts(name ## _vals,n,KNO_VOID);		  \
  stack->stack_bits |= KNO_STACK_FREE_ENV;		  \
  stack->eval_env =					  \
    init_static_env(n,parent,				  \
		    &name ## _bindings,			  \
		    &_ ## name,				  \
		    schema,				  \
		    name ## _vals)


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

static U8_MAYBE_UNUSED
void set_stack_label(struct KNO_STACK *stack,u8_string label,int needs_free)
{
  if (stack->stack_label) {
    u8_string old_label = stack->stack_label;
    stack->stack_label = label;
    int free_old = ( (stack->stack_bits) & (KNO_STACK_FREE_LABEL) );
    if (free_old) u8_free(old_label);
    if (needs_free) {
      if (!(free_old)) KNO_STACK_SET(stack,KNO_STACK_FREE_LABEL);}
    else if (free_old)
      KNO_STACK_CLEAR(stack,KNO_STACK_FREE_LABEL);
    else NO_ELSE;}
  else {
    stack->stack_label = label;
    if (needs_free) KNO_STACK_SET(stack,KNO_STACK_FREE_LABEL);}
}

#define KNO_INIT_ITER_LOOP(name,var,value,n_vars,caller,parent_env)	\
  kno_add_stack_ref(((kno_stack)caller),value);				\
  u8_byte iter_label_buf[32];						\
  u8_string label = (KNO_SYMBOLP(var)) ?				\
    u8_bprintf(iter_label_buf,"%s.%s", # name,KNO_SYMBOL_NAME(var)) :	\
    ((u8_string)(# name));						\
  KNO_START_EVAL(name ## _stack,# name,expr,NULL,caller);		\
  set_stack_label((kno_stack)caller,label,0);				\
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

