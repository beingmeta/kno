static MAYBE_UNUSED int testeval(fdtype expr,fd_lispenv env,fdtype *whoops)
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
      while (i < n) {fd_decref(vals[i]); i++;}
      fd_recycle_environment(env->copy);}
  else {
    struct FD_SCHEMAP *sm=FD_XSCHEMAP(env->bindings);
    int i=0, n=FD_XSCHEMAP_SIZE(sm); fdtype *vals=sm->values;
    while (i < n) {fd_decref(vals[i]); i++;}}
}

FD_INLINE_FCN fdtype return_error_env(fdtype error,u8_context cxt,fd_lispenv env)
{
  fd_push_error_context(cxt,copy_bindings(env));
  free_environment(env);
  return error;
}

FD_FASTOP fdtype eval_body(u8_context cxt,fdtype body,fd_lispenv inner_env)
{
  fdtype result=FD_VOID;
  FD_DOLIST(bodyexpr,body) {
    if (FD_PRIM_TYPEP(result,fd_tail_call_type))
      result=_fd_finish_call(result);
    if (FD_ABORTP(result))
      if (FD_THROWP(result))
	return result;
      else {
	fd_push_error_context(cxt,copy_bindings(inner_env));
	return result;}
    else {fd_decref(result);}
    result=fast_tail_eval(bodyexpr,inner_env);}
  if (FD_THROWP(result)) return result;
  else if (FD_ABORTP(result)) {
    fd_push_error_context(cxt,copy_bindings(inner_env));
    return result;}
  else return result;
}

FD_FASTOP fdtype eval_exprs(fdtype body,fd_lispenv inner_env)
{
  fdtype result=FD_VOID;
  FD_DOLIST(bodyexpr,body) {
    if (FD_PRIM_TYPEP(result,fd_tail_call_type))
      result=_fd_finish_call(result);
    if (FD_ABORTP(result))
      if (FD_THROWP(result))
	return result;
      else return result;
    else {fd_decref(result);}
    result=fd_tail_eval(bodyexpr,inner_env);}
  return result;
}
