static fdtype moduleid_symbol;

#define fast_eval(x,env) (_fd_fast_eval(x,env,_stack,0))
#define fast_tail_eval(x,env) (_fd_fast_eval(x,env,_stack,1))
#define stack_eval(x,env,s) (_fd_fast_eval(x,env,s,0))
#define stack_tail_eval(x,env,s) (_fd_fast_eval(x,env,s,1))

static int testeval(fdtype expr,fd_lispenv env,
                    fdtype *whoops,fd_stack s) U8_MAYBE_UNUSED;

static int testeval(fdtype expr,fd_lispenv env,fdtype *whoops,
                    fd_stack _stack)
{
  fdtype val = _fd_fast_eval(expr,env,_stack,0);
  if (FD_ABORTP(val)) {
    *whoops = val; return 0;}
  else if (FD_FALSEP(val)) return 0;
  else {fd_decref(val); return 1;}
}

static fdtype find_module_id( fd_lispenv env )
{
  if (!(env)) return FD_VOID;
  else if (FD_HASHTABLEP(env->env_bindings)) 
    return fd_get(env->env_bindings,moduleid_symbol,FD_VOID);
  else if (env->env_copy)
    return find_module_id(env->env_copy->env_parent);
  else return find_module_id(env->env_parent);
}

FD_FASTOP fdtype eval_body(u8_context cxt,u8_string label,
                           fdtype expr,int offset,
                           fd_lispenv inner_env,
                           struct FD_STACK *_stack)
{
  fdtype result = FD_VOID, body = fd_get_body(expr,offset);
  if (FD_EMPTY_LISTP(body))
    return FD_VOID;
  else if (!(FD_PAIRP(body))) {
    fd_xseterr(fd_SyntaxError,"eval_body",label,expr);
    return FD_ERROR_VALUE;}
  else {
    FD_DOLIST(bodyexpr,body) {
      if (FD_TYPEP(result,fd_tailcall_type))
        result=_fd_finish_call(result);
      if (FD_ABORTP(result)) {
        if (FD_THROWP(result)) return result;
        else if ( (u8_current_exception)->u8x_cond == fd_StackOverflow )
          return result;
        else return result;}
      else {fd_decref(result);}
      result = fast_tail_eval(bodyexpr,inner_env);}
    if (FD_THROWP(result)) return result;
    else if (FD_ABORTP(result))
      return result;
    else return result;}
}

FD_FASTOP fdtype eval_exprs(fdtype body,fd_lispenv inner_env)
{
  fdtype result = FD_VOID;
  FD_DOLIST(bodyexpr,body) {
    if (FD_TYPEP(result,fd_tailcall_type))
      result=_fd_finish_call(result);
    if (FD_ABORTP(result)) return result;
    else {fd_decref(result);}
    result = fd_tail_eval(bodyexpr,inner_env);}
  return result;
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



/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
