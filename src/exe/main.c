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

static struct KNO_STACK *_get_stack_frame(void *arg)
{
  struct KNO_STACK *curstack=kno_stackptr;
  unsigned long long intval=KNO_LONGVAL( arg);
  if (arg==NULL)
    return curstack;
  else if ((intval < 100000) && (curstack) &&
           (intval <= (curstack->stack_depth))) {
    struct KNO_STACK *scan=curstack;
    while (scan) {
      if ( scan->stack_depth == intval )
        return scan;
      else scan=scan->stack_caller;}
    if (scan==NULL)
      fprintf(stderr,"!! No stack frame %lld\n",intval);
    return NULL;}
  else return (kno_stack)arg;
}

static void stack_frame_label(u8_output out,struct KNO_STACK *stack)
{
  if (stack->stack_label)
    u8_puts(out,stack->stack_label);
}

KNO_EXPORT void _knodbg_show_env(kno_lexenv start,int limit)
{
  lispval moduleid = kno_intern("%moduleid");
  int depth = 0;
  kno_lexenv env=start;
  if (limit<0)  {
    lispval bindings = env->env_bindings;
    lispval keys = kno_getkeys(bindings);
    u8_byte buf[128];
    KNO_DO_CHOICES(key,keys) {
      if (KNO_SYMBOLP(key)) {
        lispval val=kno_get(bindings,key,KNO_VOID);
        u8_string vstring=u8_sprintf(buf,128,"%q",val);
        fprintf(stderr,"  %s\t=\t%s\n",KNO_SYMBOL_NAME(key),vstring);
        kno_decref(val);}}
    kno_decref(keys);}
  else while ( (env) && (depth < limit) ) {
      lispval bindings = env->env_bindings;
      kno_lisp_type btype = KNO_TYPEOF(bindings);
      lispval name = kno_get(bindings,moduleid,KNO_VOID);
      if (KNO_VOIDP(name)) {
        lispval keys = kno_getkeys(bindings);
        u8_fprintf(stderr,"  env#%d %q\t\t\t(%s[%d]) 0x%llx/0x%llx\n",
                   depth,keys,kno_type_names[btype],KNO_CHOICE_SIZE(keys),
                   bindings,env);
        kno_decref(keys);}
      else u8_fprintf(stderr,"  env#%d module %q\t\t0x%llx\n",depth,name,env);
      kno_decref(name);
      env=env->env_parent;
      depth++;}
}

static void _concise_stack_frame(struct KNO_STACK *stack)
{
  u8_string summary=NULL;
  lispval op = stack->stack_point;
  kno_stack_type stack_type = KNO_STACK_TYPE(stack);
  fprintf(stderr,"(%d) ",stack->stack_depth);
  U8_FIXED_OUTPUT(tmp,128);
  if (stack->stack_label) {
    stack_frame_label(tmpout,stack);
    summary=tmp.u8_outbuf;}
  if (summary)
    fprintf(stderr,"%s",summary);
  else fprintf(stderr,"unitialized stack");
  if (STACK_ARGS(stack))
    fprintf(stderr,", %d args",STACK_WIDTH(stack));
  if (KNO_SYMBOLP(op))
    fprintf(stderr,", op=%s",SYM_NAME(op));
  else if (KNO_FUNCTIONP(op)) {
    struct KNO_FUNCTION *fn=KNO_GETFUNCTION(op);
    if (fn->fcn_name)
      fprintf(stderr,", op=%s",fn->fcn_name);}
  else if (TYPEP(op,kno_evalfn_type)) {
    struct KNO_EVALFN *evalfn=(kno_evalfn)op;
    fprintf(stderr,", op=%s",evalfn->evalfn_name);}
  else {}
  if (stack_type == kno_reduce_stack) {
    kno_stack estack = (kno_stack) stack;
    if ((estack->eval_env) && (SCHEMAPP(estack->eval_env->env_bindings))) {
      struct KNO_SCHEMAP *sm = (kno_schemap)(estack->eval_env->env_bindings);
      lispval *schema=sm->table_schema;
      fprintf(stderr,", binding");
      int n=sm->schema_length, i=0; while (i<n) {
	lispval var=schema[i++];
	if (SYMBOLP(var))
	  fprintf(stderr," %s",SYM_NAME(var));}}}
  if (stack->stack_file)
    fprintf(stderr," // src='%s'",stack->stack_file);
  fprintf(stderr,"\n");
}

KNO_EXPORT void _knodbg_show_stack_frame(void *arg)
{
  struct KNO_STACK *stack=_get_stack_frame(arg);
  kno_stack_type stack_type = KNO_STACK_TYPE(stack);
  kno_lexenv env = NULL;
  _concise_stack_frame(stack);
  if (stack_type == kno_reduce_stack) {
    kno_stack estack = (kno_stack)stack;
    if (estack->eval_env) {
      env = estack->eval_env;
      _knodbg_show_env(env,20);}}
  if (PAIRP(stack->stack_point))
    u8_fprintf(stderr,"%Q",stack->stack_point);
  else if (KNO_APPLICABLEP(stack->stack_point)) {
    u8_fprintf(stderr,"Applying %q to",stack->stack_point);
    if (STACK_WIDTH(stack)) {
      u8_byte buf[128];
      kno_argvec args=STACK_ARGS(stack);
      int i=0, n=STACK_WIDTH(stack);
      while (i<n) {
        u8_string line=u8_sprintf(buf,128,"\n#%d\t%q",i,args[i]);
        fputs(line,stderr);
        i++;}}}
  fputs("\n",stderr);
  if (env) _knodbg_show_env(env,-1);
}

KNO_EXPORT lispval _knodbg_get_stack_arg(void *arg,int n)
{
  struct KNO_STACK *stack=_get_stack_frame(arg);
  if (STACK_ARGS(stack))
    if ( (n>=0) && (n < (STACK_WIDTH(stack)) ) )
      return STACK_ARGS(stack)[n];
    else return KNO_NULL;
  else return KNO_NULL;
}

KNO_EXPORT lispval _knodbg_get_stack_var(void *arg,u8_string varname)
{
  struct KNO_STACK *stack=(kno_stack)_get_stack_frame(arg);
  if (KNO_STACK_TYPE(stack) == kno_reduce_stack) {
    if (stack->eval_env) {
      lispval sym = kno_getsym(varname);
      return kno_symeval(sym,stack->eval_env);}}
  return KNO_NULL;
}

KNO_EXPORT void _knodbg_show_stack(void *arg,int limit)
{
  int count=0;
  struct KNO_STACK *stack=_get_stack_frame(arg);
  if (stack==NULL) {
    fprintf(stderr,"!! No stack\n");
    return;}
  while (stack) {
    _concise_stack_frame(stack);
    count++;
    stack=stack->stack_caller;
    if ( (limit > 0) && (count >= limit) ) break;}
}

KNO_EXPORT void _knodbg_show_stack_env(void *arg)
{
  struct KNO_STACK *stack=(kno_stack)_get_stack_frame(arg);
  if (stack==NULL) {
    fprintf(stderr,"!! No stack\n");
    return;}
  else if (KNO_STACK_TYPE(stack) != kno_reduce_stack) {
    fprintf(stderr,"!! No env\n");
    return;}
  else {
    _concise_stack_frame((kno_stack)stack);
    if (stack->eval_env)
      _knodbg_show_env(stack->eval_env,-1);
    else fprintf(stderr,"!! No env\n");}
}

