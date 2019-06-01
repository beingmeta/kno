static lispval moduleid_symbol;

#define fast_eval(x,env) (_kno_fast_eval(x,env,_stack,0))
#define fast_stack_eval(x,env,stack) (_kno_fast_eval(x,env,stack,0))
#define fast_tail_eval(x,env) (_kno_fast_eval(x,env,_stack,1))
#define stack_eval(x,env,s) (_kno_fast_eval(x,env,s,0))
#define stack_tail_eval(x,env,s) (_kno_fast_eval(x,env,s,1))

static int testeval(lispval expr,kno_lexenv env,
                    lispval *whoops,kno_stack s) U8_MAYBE_UNUSED;

static int testeval(lispval expr,kno_lexenv env,lispval *whoops,
                    kno_stack _stack)
{
  lispval val = _kno_fast_eval(expr,env,_stack,0);
  if (KNO_ABORTP(val)) {
    *whoops = val; return 0;}
  else if (KNO_FALSEP(val)) return 0;
  else {kno_decref(val); return 1;}
}

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

static U8_MAYBE_UNUSED
lispval find_module_id( kno_lexenv env )
{
  if (!(env)) return KNO_VOID;
  else if (KNO_HASHTABLEP(env->env_bindings))
    return kno_get(env->env_bindings,moduleid_symbol,KNO_VOID);
  else if (env->env_copy)
    return find_module_id(env->env_copy->env_parent);
  else return find_module_id(env->env_parent);
}

static U8_MAYBE_UNUSED
void free_lexenv(struct KNO_LEXENV *env)
{
  /* There are three cases:
        a simple static environment (env->env_copy == NULL)
        a static environment copied into a dynamic environment
          (env->env_copy!=env)
        a dynamic environment (env->env_copy == env->env_copy)
  */
  if (env->env_copy)
    if (env == env->env_copy)
      kno_recycle_lexenv(env->env_copy);
    else {
      struct KNO_SCHEMAP *sm = KNO_XSCHEMAP(env->env_bindings);
      int i = 0, n = KNO_XSCHEMAP_SIZE(sm); 
      lispval *vals = sm->schema_values;
      while (i < n) {
        lispval val = vals[i++];
        if ((KNO_CONSP(val))&&(KNO_MALLOCD_CONSP((kno_cons)val))) {
          kno_decref(val);}}
      u8_destroy_rwlock(&(sm->table_rwlock));
      kno_recycle_lexenv(env->env_copy);}
  else {
    struct KNO_SCHEMAP *sm = KNO_XSCHEMAP(env->env_bindings);
    int i = 0, n = KNO_XSCHEMAP_SIZE(sm);
    lispval *vals = sm->schema_values;
    while (i < n) {
      lispval val = vals[i++];
      if ((KNO_CONSP(val))&&(KNO_MALLOCD_CONSP((kno_cons)val))) {
        kno_decref(val);}}
    u8_destroy_rwlock(&(sm->table_rwlock));}
}

KNO_FASTOP lispval eval_body(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  while (PAIRP(body)) {
    lispval subex = pop_arg(body);
    if (PAIRP(body)) {
      lispval v = stack_eval(subex,env,stack);
      if (KNO_ABORTED(v))
        return v;
      else kno_decref(v);}
    else return _kno_fast_eval(subex,env,stack,tail);}
  return KNO_VOID;
}

KNO_FASTOP lispval eval_inner_body(u8_context cxt,u8_string label,
                                  lispval expr,int offset,
                                  kno_lexenv inner_env,
                                  struct KNO_STACK *_stack)
{
  lispval body = kno_get_body(expr,offset);
  if (KNO_EMPTY_LISTP(body))
    return KNO_VOID;
  else if (!(KNO_PAIRP(body))) {
    kno_seterr(kno_SyntaxError,"eval_body",label,expr);
    return KNO_ERROR_VALUE;}
  else return eval_body(body,inner_env,_stack,1);
}

#define KNO_VOID_RESULT(result)                          \
  if (KNO_ABORTP(result)) return result;                 \
  else if (KNO_TYPEP(result,kno_tailcall_type)) {         \
    struct KNO_TAILCALL *tc = (kno_tailcall)result;         \
    tc->tailcall_flags |= KNO_TAILCALL_VOID_VALUE;}        \
  else { kno_decref(result); result = KNO_VOID; }

#define KNO_DISCARD_RESULT(result)               \
  if (KNO_ABORTP(result)) return result;         \
  result = kno_finish_call(result);                \
  if (KNO_ABORTP(result)) return result;         \
  else { kno_decref(result); result = KNO_VOID;}


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

#define INIT_STACK_ENV(stack,name,parent,n)          \
  struct KNO_SCHEMAP name ## _bindings;               \
  struct KNO_LEXENV _ ## name, *name=&_ ## name; \
  lispval name ## _vars[n];                           \
  lispval name ## _vals[n];                           \
  kno_init_elts(name ## _vars,n,KNO_VOID);                  \
  kno_init_elts(name ## _vals,n,KNO_VOID);                  \
  _stack->stack_env =                                \
    init_static_env(n,parent,                        \
                    &name ## _bindings,              \
                    &_ ## name,                      \
                    name ## _vars,                   \
                    name ## _vals)

#define INIT_STACK_SCHEMA(stack,name,parent,n,schema)   \
  struct KNO_SCHEMAP name ## _bindings;                  \
  struct KNO_LEXENV _ ## name, *name=&_ ## name;    \
  lispval name ## _vals[n];                              \
  kno_init_elts(name ## _vals,n,KNO_VOID);                \
  stack->stack_env =                                    \
    init_static_env(n,parent,                           \
                    &name ## _bindings,                 \
                    &_ ## name,                         \
                    schema,                             \
                    name ## _vals)


U8_MAYBE_UNUSED KNO_FASTOP
void reset_env(kno_lexenv env)
{
  if (env->env_copy) {
    kno_free_lexenv(env->env_copy);
    env->env_copy=NULL;}
}

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
  else if (KNO_CODEP(bindexprs)) {
    int len = KNO_CODE_LENGTH(bindexprs);
    if ((len%2)==1) {
      *why_not = kno_err(kno_BindSyntaxError,"check_bindexprs",NULL,bindexprs);
      return -1;}
    else return len/2;}
  else return -1;
}

KNO_FASTOP kno_lexenv make_dynamic_env(int n,kno_lexenv parent)
{
  int i = 0;
  struct KNO_LEXENV *e = u8_alloc(struct KNO_LEXENV);
  lispval *vars = u8_alloc_n(n,lispval);
  lispval *vals = u8_alloc_n(n,lispval);
  lispval schemap = kno_make_schemap(NULL,n,KNO_SCHEMAP_PRIVATE,vars,vals);
  while (i<n) {vars[i]=VOID; vals[i]=VOID; i++;}
  KNO_INIT_FRESH_CONS(e,kno_lexenv_type);
  e->env_copy = e;
  e->env_bindings = schemap;
  e->env_exports = VOID;
  e->env_parent = kno_copy_env(parent);
  return e;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
