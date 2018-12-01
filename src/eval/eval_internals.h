static lispval moduleid_symbol;

#define fast_eval(x,env) (_fd_fast_eval(x,env,_stack,0))
#define fast_stack_eval(x,env,stack) (_fd_fast_eval(x,env,stack,0))
#define fast_tail_eval(x,env) (_fd_fast_eval(x,env,_stack,1))
#define stack_eval(x,env,s) (_fd_fast_eval(x,env,s,0))
#define stack_tail_eval(x,env,s) (_fd_fast_eval(x,env,s,1))

static int testeval(lispval expr,fd_lexenv env,
                    lispval *whoops,fd_stack s) U8_MAYBE_UNUSED;

static int testeval(lispval expr,fd_lexenv env,lispval *whoops,
                    fd_stack _stack)
{
  lispval val = _fd_fast_eval(expr,env,_stack,0);
  if (FD_ABORTP(val)) {
    *whoops = val; return 0;}
  else if (FD_FALSEP(val)) return 0;
  else {fd_decref(val); return 1;}
}

FD_FASTOP lispval _pop_arg(lispval *scan)
{
  lispval expr = *scan;
  if (PAIRP(expr)) {
    lispval arg = FD_CAR(expr), next = FD_CDR(expr);
    if (FD_CONSP(arg)) FD_PREFETCH((struct FD_CONS *)arg);
    if (FD_CONSP(next)) FD_PREFETCH((struct FD_CONS *)next);
    *scan = next;
    return arg;}
  else return VOID;
}

#define pop_arg(args) (_pop_arg(&args))

#define simplify_value(v) \
  ( (FD_PRECHOICEP(v)) ? (fd_simplify_choice(v)) : (v) )

static U8_MAYBE_UNUSED
lispval find_module_id( fd_lexenv env )
{
  if (!(env)) return FD_VOID;
  else if (FD_HASHTABLEP(env->env_bindings))
    return fd_get(env->env_bindings,moduleid_symbol,FD_VOID);
  else if (env->env_copy)
    return find_module_id(env->env_copy->env_parent);
  else return find_module_id(env->env_parent);
}

static U8_MAYBE_UNUSED
void free_lexenv(struct FD_LEXENV *env)
{
  /* There are three cases:
        a simple static environment (env->env_copy == NULL)
        a static environment copied into a dynamic environment
          (env->env_copy!=env)
        a dynamic environment (env->env_copy == env->env_copy)
  */
  if (env->env_copy)
    if (env == env->env_copy)
      fd_recycle_lexenv(env->env_copy);
    else {
      struct FD_SCHEMAP *sm = FD_XSCHEMAP(env->env_bindings);
      int i = 0, n = FD_XSCHEMAP_SIZE(sm); 
      lispval *vals = sm->schema_values;
      while (i < n) {
        lispval val = vals[i++];
        if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
          fd_decref(val);}}
      u8_destroy_rwlock(&(sm->table_rwlock));
      fd_recycle_lexenv(env->env_copy);}
  else {
    struct FD_SCHEMAP *sm = FD_XSCHEMAP(env->env_bindings);
    int i = 0, n = FD_XSCHEMAP_SIZE(sm);
    lispval *vals = sm->schema_values;
    while (i < n) {
      lispval val = vals[i++];
      if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
        fd_decref(val);}}
    u8_destroy_rwlock(&(sm->table_rwlock));}
}

FD_FASTOP lispval eval_body(lispval body,fd_lexenv env,fd_stack stack,int tail)
{
  while (PAIRP(body)) {
    lispval subex = pop_arg(body);
    if (PAIRP(body)) {
      lispval v = stack_eval(subex,env,stack);
      if (FD_ABORTED(v))
        return v;
      else fd_decref(v);}
    else return _fd_fast_eval(subex,env,stack,tail);}
  return FD_VOID;
}

FD_FASTOP lispval eval_inner_body(u8_context cxt,u8_string label,
                                  lispval expr,int offset,
                                  fd_lexenv inner_env,
                                  struct FD_STACK *_stack)
{
  lispval body = fd_get_body(expr,offset);
  if (FD_EMPTY_LISTP(body))
    return FD_VOID;
  else if (!(FD_PAIRP(body))) {
    fd_seterr(fd_SyntaxError,"eval_body",label,expr);
    return FD_ERROR_VALUE;}
  else return eval_body(body,inner_env,_stack,1);
}

#define FD_VOID_RESULT(result)                          \
  if (FD_ABORTP(result)) return result;                 \
  else if (FD_TYPEP(result,fd_tailcall_type)) {         \
    struct FD_TAILCALL *tc = (fd_tailcall)result;         \
    tc->tailcall_flags |= FD_TAILCALL_VOID_VALUE;}        \
  else { fd_decref(result); result = FD_VOID; }

#define FD_DISCARD_RESULT(result)               \
  if (FD_ABORTP(result)) return result;         \
  result = fd_finish_call(result);                \
  if (FD_ABORTP(result)) return result;         \
  else { fd_decref(result); result = FD_VOID;}


FD_FASTOP fd_lexenv init_static_env
  (int n,fd_lexenv parent,
   struct FD_SCHEMAP *bindings,struct FD_LEXENV *envstruct,
   lispval *vars,lispval *vals)
{
  memset(envstruct,0,sizeof(struct FD_LEXENV));
  memset(bindings,0,FD_SCHEMAP_LEN);
  FD_INIT_STATIC_CONS(envstruct,fd_lexenv_type);
  FD_INIT_STATIC_CONS(bindings,fd_schemap_type);
  bindings->schemap_onstack = 1;
  bindings->table_schema = vars;
  bindings->schema_values = vals;
  bindings->schema_length = n;
  bindings->table_uselock = 0;
  // u8_init_rwlock(&(bindings->table_rwlock));
  envstruct->env_bindings = LISP_CONS((bindings));
  envstruct->env_exports = FD_VOID;
  envstruct->env_parent = parent;
  envstruct->env_copy = NULL;
  return envstruct;
}

#define INIT_STACK_ENV(stack,name,parent,n)          \
  struct FD_SCHEMAP name ## _bindings;               \
  struct FD_LEXENV _ ## name, *name=&_ ## name; \
  lispval name ## _vars[n];                           \
  lispval name ## _vals[n];                           \
  fd_init_elts(name ## _vars,n,FD_VOID);                  \
  fd_init_elts(name ## _vals,n,FD_VOID);                  \
  _stack->stack_env =                                \
    init_static_env(n,parent,                        \
                    &name ## _bindings,              \
                    &_ ## name,                      \
                    name ## _vars,                   \
                    name ## _vals)

#define INIT_STACK_SCHEMA(stack,name,parent,n,schema)   \
  struct FD_SCHEMAP name ## _bindings;                  \
  struct FD_LEXENV _ ## name, *name=&_ ## name;    \
  lispval name ## _vals[n];                              \
  fd_init_elts(name ## _vals,n,FD_VOID);                \
  stack->stack_env =                                    \
    init_static_env(n,parent,                           \
                    &name ## _bindings,                 \
                    &_ ## name,                         \
                    schema,                             \
                    name ## _vals)


U8_MAYBE_UNUSED FD_FASTOP
void reset_env(fd_lexenv env)
{
  if (env->env_copy) {
    fd_free_lexenv(env->env_copy);
    env->env_copy=NULL;}
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
