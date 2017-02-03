static fdtype moduleid_symbol;

static int testeval(fdtype expr,fd_lispenv env,fdtype *whoops) U8_MAYBE_UNUSED;

static int testeval(fdtype expr,fd_lispenv env,fdtype *whoops)
{
  fdtype val=fasteval(expr,env);
  if (FD_ABORTP(val)) {
    *whoops=val; return 0;}
  else if (FD_FALSEP(val)) return 0;
  else {fd_decref(val); return 1;}
}

static fdtype find_module_id( fd_lispenv env )
{
  if (!(env)) return FD_VOID;
  else if (FD_HASHTABLEP(env->bindings)) 
    return fd_get(env->bindings,moduleid_symbol,FD_VOID);
  else if (env->copy)
    return find_module_id(env->copy->parent);
  else return find_module_id(env->parent);
}

static fdtype error_bindings(fd_lispenv env)
{
  fdtype bindings = (env->copy) ? (env->copy->bindings) : (env->bindings);
  fdtype moduleid = find_module_id ( env );
  if (FD_VOIDP(moduleid))
    return fd_copy(bindings);
  else if (!(FD_TABLEP(bindings))) {
    struct FD_KEYVAL *kv=u8_alloc_n(1,struct FD_KEYVAL);
    kv[0].key=moduleid_symbol; kv[1].value=moduleid;
    return fd_init_slotmap(NULL,1,kv);}
  else {
    fdtype vars = fd_getkeys(bindings);
    int n_vars = FD_CHOICE_SIZE(vars);
    struct FD_KEYVAL *kv = u8_alloc_n(n_vars+1,struct FD_KEYVAL), *write=kv;
    write[0].key=moduleid_symbol; write[0].value=moduleid; write++;
    {FD_DO_CHOICES(var,vars){
        write[0].key=var; write[0].value=fd_get(bindings,var,FD_VOID);
        write++;}}
    return fd_init_slotmap(NULL,n_vars+1,kv);}
}

static void free_environment(struct FD_ENVIRONMENT *env)
{
  /* There are three cases:
        a simple static environment (env->copy==NULL)
        a static environment copied into a dynamic environment
          (env->copy!=env)
        a dynamic environment (env->copy==env->copy)
  */
  if (env->copy)
    if (env==env->copy)
      fd_recycle_environment(env->copy);
    else {
      struct FD_SCHEMAP *sm=FD_XSCHEMAP(env->bindings);
      int i=0, n=FD_XSCHEMAP_SIZE(sm); fdtype *vals=sm->values;
      while (i < n) {
        fdtype val=vals[i++];
        if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
          fd_decref(val);}}
      fd_destroy_rwlock(&(sm->rwlock));
      fd_recycle_environment(env->copy);}
  else {
    struct FD_SCHEMAP *sm=FD_XSCHEMAP(env->bindings);
    int i=0, n=FD_XSCHEMAP_SIZE(sm); fdtype *vals=sm->values;
    while (i < n) {
      fdtype val=vals[i++];
      if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
        fd_decref(val);}}
    fd_destroy_rwlock(&(sm->rwlock));}
}

FD_INLINE_FCN fdtype return_error_env
  (fdtype error,u8_context cxt,fd_lispenv env)
{
  if (FD_THROWP(error)) {
    free_environment(env);
    return error;}
  fd_push_error_context(cxt,error_bindings(env));
  free_environment(env);
  return error;
}

FD_FASTOP fdtype eval_body(u8_context cxt,fdtype expr,int offset,
                           fd_lispenv inner_env)
{
  fdtype result=FD_VOID;
  FD_DOBODY(bodyexpr,expr,offset) {
    if (FD_PRIM_TYPEP(result,fd_tail_call_type))
      result=_fd_finish_call(result);
    if (FD_ABORTP(result)) {
      if (FD_THROWP(result)) return result;
      else if ( u8_current_exception->u8x_cond == fd_StackOverflow )
        return result;
      else {
        fd_push_error_context(cxt,error_bindings(inner_env));
        return result;}}
    else {fd_decref(result);}
    result=fast_tail_eval(bodyexpr,inner_env);}
  if (FD_THROWP(result)) return result;
  else if (FD_ABORTP(result)) {
    fd_push_error_context(cxt,error_bindings(inner_env));
    return result;}
  else return result;
}

FD_FASTOP fdtype eval_exprs(fdtype body,fd_lispenv inner_env)
{
  fdtype result=FD_VOID;
  FD_DOLIST(bodyexpr,body) {
    if (FD_PRIM_TYPEP(result,fd_tail_call_type))
      result=_fd_finish_call(result);
    if (FD_ABORTP(result)) return result;
    else {fd_decref(result);}
    result=fd_tail_eval(bodyexpr,inner_env);}
  return result;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
