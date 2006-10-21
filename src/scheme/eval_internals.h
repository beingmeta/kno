static int testeval(fdtype expr,fd_lispenv env,fdtype *whoops)
{
  fdtype val=fasteval(expr,env);
  if (FD_ABORTP(val)) {
    *whoops=val; return 0;}
  else if (FD_FALSEP(val)) return 0;
  else {fd_decref(val); return 1;}
}

#define copy_bindings(env) \
  ((env->copy) ? (fd_copy(env->copy->bindings)) : (fd_copy(env->bindings)))

static void free_environment(struct FD_ENVIRONMENT *env)
{
  if (env->copy) 
    fd_recycle_environment(env->copy);
  else {
    struct FD_SCHEMAP *sm=FD_XSCHEMAP(env->bindings);
    int i=0, n=FD_XSCHEMAP_SIZE(sm); fdtype *vals=sm->values;
    while (i < n) {fd_decref(vals[i]); i++;}}
}

static passerr_env(fdtype error,fd_lispenv env)
{
  fdtype bindings=copy_bindings(env);
  free_environment(env);
  return fd_passerr(error,bindings);
}

static fdtype eval_body(fdtype body,fd_lispenv inner_env)
{
  fdtype result=FD_VOID;
  FD_DOLIST(bodyexpr,body) {
    if (FD_PRIM_TYPEP(result,fd_tail_call_type))
      result=_fd_finish_call(result);
    if (FD_ABORTP(result))
      if (FD_THROWP(result))
	return result;
      else return passerr_env(result,inner_env);
    else {fd_decref(result);}
    result=fasteval(bodyexpr,inner_env);}
  if (FD_THROWP(result)) return result;
  else if (FD_ABORTP(result))
    return passerr_env(result,inner_env);
  else return result;
}

