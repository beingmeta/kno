/* Writing the cmd file */

#define need_escape(s) \
  ((strchr(s,'"'))||(strchr(s,'\\'))|| \
   (strchr(s,' '))||(strchr(s,'\t'))|| \
   (strchr(s,'\n'))||(strchr(s,'\r')))

U8_MAYBE_UNUSED static
void write_cmd_file(u8_string cmd_file,u8_condition label,int argc,char **argv)
{
  const char *abspath = u8_abspath(cmd_file,NULL);
  int i = 0, fd = open(abspath,O_CREAT|O_RDWR|O_TRUNC,
                   S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH);
  u8_byte buf[512]; struct U8_OUTPUT out;
  U8_INIT_OUTPUT_BUF(&out,512,buf);
  while (i<argc) {
    char *arg = argv[i];
    u8_string argstring = u8_fromlibc(arg);
    if (i>0) u8_putc(&out,' '); i++;
    if (need_escape(argstring)) {
      u8_string scan = argstring;
      int c = u8_sgetc(&scan); u8_putc(&out,'"');
      while (c>=0) {
        if (c=='\\') {
          u8_putc(&out,'\\'); c = u8_sgetc(&scan);}
        else if ((c==' ')||(c=='\n')||(c=='\t')||(c=='\r')||(c=='"')) {
          u8_putc(&out,'\\');}
        if (c>=0) u8_putc(&out,c);
        c = u8_sgetc(&scan);}
      u8_putc(&out,'"');}
    else u8_puts(&out,argstring);
    if (argstring!=((u8_string)arg)) u8_free(argstring);}
  u8_log(LOG_INFO,label,"%s",out.u8_outbuf);
  if (fd>=0) {
    ssize_t rv = write(fd,out.u8_outbuf,out.u8_write-out.u8_outbuf);
    if (rv<0) {
      int got_errno = errno; errno=0;
      u8_log(LOG_WARN,"SaveFailed",
             "Couldn't save the command line to %s (errno=%d:%s)",
             cmd_file,got_errno,u8_strerror(got_errno));}}
  u8_free(abspath);
  u8_close_output(&out);
  close(fd);
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

/* Identifying configs */

static int isconfig(char *arg)
{
  char *eq = strchr(arg,'=');
  return ( (eq != NULL) && (eq > arg) && ((*(eq-1)) != '\\') );
}

/* Stack traces */

static struct FD_STACK *_get_stack_frame(void *arg)
{
  struct FD_STACK *curstack=fd_stackptr;
  unsigned long long intval=(unsigned long long) arg;
  if (arg==NULL)
    return curstack;
  else if ((intval < 100000) && (curstack) &&
           (intval <= (curstack->stack_depth))) {
    struct FD_STACK *scan=curstack;
    while (scan) {
      if ( scan->stack_depth == intval )
        return scan;
      else scan=scan->stack_caller;}
    if (scan==NULL)
      fprintf(stderr,"!! No stack frame %lld\n",intval);
    return NULL;}
  else return (fd_stack)arg;
}

static void stack_frame_label(u8_output out,struct FD_STACK *stack)
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

FD_EXPORT void _fdbg_show_env(fd_lexenv start,int limit)
{
  lispval moduleid = fd_intern("%MODULEID");
  int depth = 0;
  fd_lexenv env=start;
  if (limit<0)  {
    lispval bindings = env->env_bindings;
    lispval keys = fd_getkeys(bindings);
    u8_byte buf[128];
    FD_DO_CHOICES(key,keys) {
      if (FD_SYMBOLP(key)) {
        lispval val=fd_get(bindings,key,FD_VOID);
        u8_string vstring=u8_sprintf(buf,128,"%q",val);
        fprintf(stderr,"  %s\t=\t%s\n",FD_SYMBOL_NAME(key),vstring);
        fd_decref(val);}}
    fd_decref(keys);}
  else while ( (env) && (depth < limit) ) {
      lispval bindings = env->env_bindings;
      fd_ptr_type btype = FD_PTR_TYPE(bindings);
      lispval name = fd_get(bindings,moduleid,FD_VOID);
      if (FD_VOIDP(name)) {
        lispval keys = fd_getkeys(bindings);
        u8_fprintf(stderr,"  env#%d %q\t\t\t(%s[%d]) 0x%llx/0x%llx\n",
                   depth,keys,fd_type_names[btype],FD_CHOICE_SIZE(keys),
                   bindings,env);
        fd_decref(keys);}
      else u8_fprintf(stderr,"  env#%d module %q\t\t0x%llx\n",depth,name,env);
      fd_decref(name);
      env=env->env_parent;
      depth++;}
}

static void _concise_stack_frame(struct FD_STACK *stack)
{
  u8_string summary=NULL;
  lispval op = stack->stack_op;
  fprintf(stderr,"(%d) ",stack->stack_depth);
  U8_FIXED_OUTPUT(tmp,128);
  if ( (stack->stack_label) || (stack->stack_status) ) {
    stack_frame_label(tmpout,stack);
    summary=tmp.u8_outbuf;}
  if (summary)
    fprintf(stderr,"%s",summary);
  else if (stack->stack_type)
    fprintf(stderr,"??.%s",stack->stack_type);
  else fprintf(stderr,"unitialized stack");
  if (stack->stack_args)
    fprintf(stderr,", %d args",stack->n_args);
  if (FD_SYMBOLP(op))
    fprintf(stderr,", op=%s",SYM_NAME(op));
  else if (FD_FUNCTIONP(op)) {
    struct FD_FUNCTION *fn=FD_XFUNCTION(op);
    if (fn->fcn_name)
      fprintf(stderr,", op=%s",fn->fcn_name);}
  else if (TYPEP(op,fd_evalfn_type)) {
    struct FD_EVALFN *evalfn=(fd_evalfn)op;
    fprintf(stderr,", op=%s",evalfn->evalfn_name);}
  else {}
  if (stack->n_cleanups)
    fprintf(stderr,", %d cleanups",stack->n_cleanups);
  if ((stack->stack_env) &&
      (SCHEMAPP(stack->stack_env->env_bindings))) {
    struct FD_SCHEMAP *sm = (fd_schemap)stack->stack_env->env_bindings;
    lispval *schema=sm->table_schema;
    fprintf(stderr,", binding");
    int n=sm->schema_length, i=0; while (i<n) {
      lispval var=schema[i++];
      if (SYMBOLP(var))
        fprintf(stderr," %s",SYM_NAME(var));}}
  if (stack->stack_src)
    fprintf(stderr," // src='%s'",stack->stack_src);
  fprintf(stderr,"\n");
}

FD_EXPORT void _fdbg_show_stack_frame(void *arg)
{
  struct FD_STACK *stack=_get_stack_frame(arg);
  _concise_stack_frame(stack);
  if (stack->stack_env) _fdbg_show_env(stack->stack_env,20);
  if (PAIRP(stack->stack_op))
    u8_fprintf(stderr,"%Q",stack->stack_op);
  else if (FD_APPLICABLEP(stack->stack_op)) {
    u8_fprintf(stderr,"Applying %q to",stack->stack_op);
    if (stack->n_args) {
      u8_byte buf[128];
      lispval *args=stack->stack_args;
      int i=0, n=stack->n_args;
      while (i<n) {
        u8_string line=u8_sprintf(buf,128,"\n#%d\t%q",i,args[i]);
        fputs(line,stderr);
        i++;}}}
  fputs("\n",stderr);
  if (stack->stack_env)
    _fdbg_show_env(stack->stack_env,-1);
}

FD_EXPORT lispval _fdbg_get_stack_arg(void *arg,int n)
{
  struct FD_STACK *stack=_get_stack_frame(arg);
  if (stack->stack_args)
    if ( (n>=0) && (n < (stack->n_args) ) )
      return stack->stack_args[n];
    else return FD_NULL;
  else return FD_NULL;
}

FD_EXPORT lispval _fdbg_get_stack_var(void *arg,u8_string varname)
{
  struct FD_STACK *stack=_get_stack_frame(arg);
  if (stack->stack_env) {
    lispval sym = fd_symbolize(varname);
    return fd_symeval(sym,stack->stack_env);}
  else return FD_NULL;
}

FD_EXPORT void _fdbg_show_stack(void *arg,int limit)
{
  int count=0;
  struct FD_STACK *stack=_get_stack_frame(arg);
  if (stack==NULL) {
    fprintf(stderr,"!! No stack\n");
    return;}
  while (stack) {
    _concise_stack_frame(stack);
    count++;
    stack=stack->stack_caller;
    if ( (limit > 0) && (count >= limit) ) break;}
}

FD_EXPORT void _fdbg_show_stack_env(void *arg)
{
  struct FD_STACK *stack=_get_stack_frame(arg);
  if (stack==NULL) {
    fprintf(stderr,"!! No stack\n");
    return;}
  _concise_stack_frame(stack);
  if (stack->stack_env)
    _fdbg_show_env(stack->stack_env,-1);
  else fprintf(stderr,"!! No env\n");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
