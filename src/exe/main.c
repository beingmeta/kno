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

KNO_EXPORT void _knodbg_show_env(kno_lexenv start,int limit)
{
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
        fprintf(stderr,"  %s\t 0x%llx\t%s\n",
		KNO_SYMBOL_NAME(key),(unsigned long long)val,
		vstring);
        kno_decref(val);}}
    kno_decref(keys);}
  else while ( (env) && (depth < limit) ) {
      lispval bindings = env->env_bindings;
      kno_lisp_type btype = KNO_TYPEOF(bindings);
      lispval name = kno_get(bindings,KNOSYM_MODULEID,KNO_VOID);
      if (KNO_VOIDP(name)) {
        lispval keys = kno_getkeys(bindings);
        u8_fprintf(stderr,"  env#%d %q\t\t\t(%s[%d]) %p/%p\n",
                   depth,keys,kno_type_names[btype],KNO_CHOICE_SIZE(keys),
		   bindings,env);
        kno_decref(keys);}
      else u8_fprintf(stderr,"  env#%d module %q\t\t%p\n",depth,name,env);
      kno_decref(name);
      env=env->env_parent;
      depth++;}
}

static void _concise_stack_frame(struct KNO_STACK *stack)
{
  lispval op = stack->stack_op;
  kno_stackvec refs = &(stack->stack_refs);
  u8_string file = stack->stack_file;
  if ( (stack->stack_label) && (stack->stack_origin) &&
       ( (stack->stack_origin) != (stack->stack_label) ) )
    fprintf(stderr,"(%d) 0x%llx %s:%s%s%s%s %d/%d args, %d/%d%s refs",
	    stack->stack_depth,KNO_LONGVAL(stack),
	    stack->stack_label,stack->stack_origin,
	    U8OPTSTR("(",file,")"),
	    stack->stack_argc,stack->stack_width,
	    refs->count,KNO_STACKVEC_LEN(refs),
	    (KNO_STACKVEC_ONHEAP(refs)) ? ("(heap)") : (""));
  else if ( (stack->stack_label) || (stack->stack_origin) )
    fprintf(stderr,"(%d) 0x%llx %s%s%s%s %d/%d args, %d/%d%s refs",
	    stack->stack_depth,KNO_LONGVAL(stack),
	    ( (stack->stack_label) ?
	      (stack->stack_label) :
	      (stack->stack_origin) ),
	    U8OPTSTR("(",file,")"),
	    stack->stack_argc,stack->stack_width,
	    refs->count,KNO_STACKVEC_LEN(refs),
	    (KNO_STACKVEC_ONHEAP(refs)) ? ("(heap)") : (""));
  else fprintf(stderr,"(%d) 0x%llx %s%s%s %d/%d args, %d/%d refs",
	       stack->stack_depth,KNO_LONGVAL(stack),
	       U8OPTSTR("(",file,")"),
	       stack->stack_argc,stack->stack_width,
	       refs->count,refs->len);
  unsigned int bits = stack->stack_bits;
  if (!(U8_BITP(bits,KNO_STACK_LIVE))) fputs(" dead",stderr);
  if (U8_BITP(bits,KNO_STACK_TAIL_POS)) fputs(" tailpos",stderr);
  if (U8_BITP(bits,KNO_STACK_TAIL_LOOP)) fputs(" loop",stderr);
  if (U8_BITP(bits,KNO_STACK_LAMBDA_CALL)) fputs(" lambda",stderr);
  if (U8_BITP(bits,KNO_STACK_VOID_VAL)) fputs(" void",stderr);
  if (U8_BITP(bits,KNO_STACK_DECREF_ARGS)) fputs(" decref",stderr);
  if (U8_BITP(bits,KNO_STACK_FREE_ARGVEC)) fputs(" freevec",stderr);
  if ( (U8_BITP(bits,KNO_STACK_OWNS_ENV)) && (stack->eval_env) )
    fputs(" env",stderr);
  if (U8_BITP(bits,KNO_STACK_FREE_ENV)) fputs("/free",stderr);
  if ( (U8_BITP(bits,KNO_STACK_OWNS_ENV)) && (stack->eval_env) ) {
    kno_lexenv env = stack->eval_env;
    lispval bindings = env->env_bindings;
    if (KNO_SCHEMAPP(bindings)) {
      struct KNO_SCHEMAP *map = (kno_schemap) bindings;
      lispval *schema = map->table_schema;
      int i = 0, n = map->schema_length;
      if (n>0) fputs(" binds:",stderr);
      while (i<n) {
	lispval argname = schema[i];
	if (i>0) fputc(',',stderr);
	i++;
	fputc(' ',stderr);
	if (i == n) fputs("and ",stderr);
	if (KNO_SYMBOLP(argname))
	  fputs(KNO_SYMBOL_NAME(argname),stderr);
	else fputs("+weird+",stderr);}
      fputs("; ",stderr);}}
  if (stack->stack_file) fprintf(stderr," %s;",stack->stack_file);
  if (KNO_SYMBOLP(op))
    fprintf(stderr," point=:%s",SYM_NAME(op));
  else if ( (KNO_FUNCTIONP(op)) &&
	    ((KNO_GETFUNCTION(op))->fcn_name) ) {
    struct KNO_FUNCTION *fn=KNO_GETFUNCTION(op);
    u8_string prefix = (KNO_CPRIMP(op)) ? ("c#") :
      (KNO_LAMBDAP(op)) ? ("l#") : ("#");
    fprintf(stderr," point=%s%s",prefix,fn->fcn_name);}
  else if (TYPEP(op,kno_evalfn_type)) {
    struct KNO_EVALFN *evalfn=(kno_evalfn)op;
    fprintf(stderr," point=z#%s",evalfn->evalfn_name);}
  else if (!(KNO_CONSP(op))) {
    u8_byte buf[64];
    fprintf(stderr," point=%s",u8_bprintf(buf,"%q",op));}
  else fprintf(stderr," point=%s",kno_type_name(op));
  fprintf(stderr,"\n");
}

KNO_EXPORT void _knodbg_show_stack_frame(void *arg)
{
  struct KNO_STACK *stack=_get_stack_frame(arg);
  kno_lexenv env = NULL;
  _concise_stack_frame(stack);
  if (KNO_CONSP(stack->eval_source))
    u8_fprintf(stderr,"\tsource: %Q\n",stack->eval_source);
  if (KNO_APPLICABLEP(stack->stack_op)) {
    u8_fprintf(stderr,"Applying %q to %d args\n",
	       stack->stack_op,
	       stack->stack_argc);
    if (stack->stack_argc) {
      u8_byte buf[200];
      kno_argvec args = stack->stack_args;
      int i=0, n = stack->stack_argc;
      while (i<n) {
	lispval arg = args[i];
	u8_string line=u8_bprintf(buf,"#%d %p\t%q",i,arg,arg);
	fputs(line,stderr);
	fputc('\n',stderr);
	i++;}}
    fputc('\n',stderr);}
  else if (CONSP(stack->stack_op)) {
    u8_fprintf(stderr,"Evaluating in %p %Q\n",stack->eval_env,stack->stack_op);}
  if ( (KNO_STACK_BITP(stack,KNO_STACK_OWNS_ENV)) ) {
    if (stack->eval_env) {
      env = stack->eval_env;
      lispval bindings = env->env_bindings;
      if (KNO_SCHEMAPP(bindings)) {
	kno_schemap map = (kno_schemap)bindings;
	lispval *schema = map->table_schema;
	lispval *values = map->table_values;
	int i = 0, n = map->schema_length;
	while (i<n) {
	  lispval key = schema[i];
	  lispval val = values[i];
	  u8_byte buf[256];
	  fputs(u8_bprintf(buf,"  %q\t%p\t%q",key,val,val),stderr);
	  fputc('\n',stderr);
	  i++;}}}}
  else NO_ELSE;
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
  if (KNO_STACK_LIVEP(stack)) {
    if (stack->eval_env) {
      lispval sym = kno_getsym(varname);
      return kno_symeval(sym,stack->eval_env);}}
  return KNO_NULL;
}

KNO_EXPORT lispval _knodbg_find_stack_var(void *arg,u8_string varname)
{
  struct KNO_STACK *stack=(kno_stack)_get_stack_frame(arg);
  lispval sym = kno_getsym(varname);
  while ( (stack) && (KNO_STACK_LIVEP(stack)) ) {
    if (stack->eval_env) {
      lispval v = kno_symeval(sym,stack->eval_env);
      if (!(KNO_VOIDP(v))) {
	_concise_stack_frame(stack);
	return v;}}
    stack = stack->stack_caller;}
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
    fprintf(stderr,"!! No stack frame\n");
    return;}
  else if (KNO_STACK_LIVEP(stack)) {
    fprintf(stderr,"!! Dead stack frame\n");
    return;}
  else {
    _concise_stack_frame((kno_stack)stack);
    if (stack->eval_env)
      _knodbg_show_env(stack->eval_env,-1);
    else fprintf(stderr,"!! No env\n");}
}

