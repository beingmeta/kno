/* Environment debugging */

FD_EXPORT int _fd_showenv(fd_lispenv env)
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

#if 0
static fdtype config_table=FD_VOID;
static fdtype _fd_features_symbol;

#define config_int(var) \
  fd_store(config_table,,FD_INT(var));
#define config_bool(var) \
  fd_store(config_table,fd_intern(# var),((var)?(FD_TRUE):(FD_FALSE)));
  fd_add(config_table,_fd_features_symbol,((var)?(FD_TRUE):(FD_FALSE)));
static void config_string(u8_string var,u8_string val)
{
  fdtype string=fdtype_string(val);
  fd_store(config_table,fd_intern(var),string);
  fd_decref(string);
}

static void init_config_table()
{
  _fd_features_symbol=fd_intern("FEATURES");
  if (FD_VOIDP(config_table))
    config_table=fd_make_slotmap(64,0,NULL);
  config_bool(WORDS_ARE_ALIGNED);
  config_int(SIZEOF_FDTYPE);
}

static void fd_main_init()
{
  init_config_table();
}

#endif
