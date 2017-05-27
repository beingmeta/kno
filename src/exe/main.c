/* Environment debugging */

FD_EXPORT int _fd_showenv(fd_lexenv env)
{
  fdtype moduleid = fd_intern("%MODULEID");
  int depth = 1;
  while (env) {
    fdtype bindings = env->env_bindings; fd_ptr_type btype = FD_PTR_TYPE(bindings);
    fdtype name = fd_get(bindings,moduleid,FD_VOID);
    fdtype keys = fd_getkeys(bindings);
    u8_fprintf(stderr,"#%d %s %s(%d) %ld/%lx\n\t%q\n",
               depth,
               ((FD_STRINGP(name))?(FD_STRDATA(name)):
                (FD_SYMBOLP(name))?(FD_SYMBOL_NAME(name)):((u8_string)"")),
               fd_type_names[btype],FD_CHOICE_SIZE(keys),
               bindings,bindings,
               keys);
    fd_decref(keys); env = env->env_parent;}
  return depth;
}

/* Exename tweaking */

static char **exenamep, *exename;

static void U8_MAYBE_UNUSED set_exename(char **argv)
{
  exenamep = &(argv[0]);
  exename = *exenamep;
}

static void U8_MAYBE_UNUSED tweak_exename(char *str,int off,char altc)
{
  char *starts = strstr(exename,str);
  if (starts) {
    starts[off]=altc;
    *exenamep = exename;}
}

/* Identifing configs */

static int isconfig(char *arg)
{
  char *eq = strchr(arg,'=');
  return ( (eq != NULL) && (eq > arg) && ((*(eq-1)) != '\\') );
}

/* Stack traces */

static void summarize_stack_frame(u8_output out,struct FD_STACK *stack)
{
  if (stack->stack_label)
    u8_puts(out,stack->stack_label);
  if ( (stack->stack_status) &&
       (stack->stack_status[0]) &&
       (stack->stack_status!=stack->stack_label) ) {
    u8_printf(out,"(%s)",stack->stack_status);}
  if ((stack->stack_type) &&
      (strcmp(stack->stack_type,stack->stack_label)))
    u8_printf(out,".%s",stack->stack_type);
}

static void _showstack_frame(struct FD_STACK *stack)
{
  if (stack==NULL) stack=fd_stackptr;
  if (stack==NULL) {
    fprintf(stderr,"!! No stack\n");
    return;}
  u8_string summary=NULL;
  fdtype op = stack->stack_op;
  U8_FIXED_OUTPUT(tmp,128);
  if ( (stack->stack_label) || (stack->stack_status) ) {
    summarize_stack_frame(tmpout,stack);
    summary=tmp.u8_outbuf;}
  if (summary)
    fprintf(stderr,"%s",summary);
  else if (stack->stack_type)
    fprintf(stderr,"??.%s",stack->stack_type);
  else fprintf(stderr,"unitialized stack");
  if (stack->stack_args)
    fprintf(stderr,", %d args",stack->n_args);
  if (FD_SYMBOLP(op))
    fprintf(stderr,", op=%s",FD_SYMBOL_NAME(op));
  else if (FD_FUNCTIONP(op)) {
    struct FD_FUNCTION *fn=(fd_function)op;
    if (fn->fcn_name)
      fprintf(stderr,", op=%s",fn->fcn_name);}
  else if (FD_TYPEP(op,fd_evalfn_type)) {
    struct FD_EVALFN *evfn=(fd_evalfn)op;
    fprintf(stderr,", op=%s",evfn->evalfn_name);}
  else {}
  if ((stack->stack_env) &&
      (FD_SCHEMAPP(stack->stack_env->env_bindings))) {
    struct FD_SCHEMAP *sm = (fd_schemap)stack->stack_env->env_bindings;
    fdtype *schema=sm->table_schema;
    fprintf(stderr,", binding");
    int n=sm->schema_length, i=0; while (i<n) {
      fdtype var=schema[i++];
      if (FD_SYMBOLP(var)) 
	fprintf(stderr," %s",FD_SYMBOL_NAME(var));}}
  fprintf(stderr,"\n");
}

static void _showstack(struct FD_STACK *stack,int limit)
{
  int count=0;
  if (stack==NULL) stack=fd_stackptr;
  if (stack==NULL) {
    fprintf(stderr,"!! No stack\n");
    return;}
  while (stack) {
    fprintf(stderr,"(%d) ",count);
    _showstack_frame(stack);
    if ( (limit > 0) && (count > limit) ) break;
    stack=stack->stack_caller;
    count++;}
}
