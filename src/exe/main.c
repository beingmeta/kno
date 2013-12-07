/* Environment debugging */

FD_EXPORT int _fd_showenv(fd_lispenv env)
{
  fdtype moduleid=fd_intern("%MODULEID");
  int depth=1;
  while (env) {
    fdtype bindings=env->bindings; fd_ptr_type btype=FD_PTR_TYPE(bindings);
    fdtype name=fd_get(bindings,moduleid,FD_VOID);
    fdtype keys=fd_getkeys(bindings);
    u8_fprintf(stderr,"#%d %s %s(%d) %ld/%lx\n\t%q\n",
               depth,
               ((FD_STRINGP(name))?(FD_STRDATA(name)):
                (FD_SYMBOLP(name))?(FD_SYMBOL_NAME(name)):((u8_string)"")),
               fd_type_names[btype],FD_CHOICE_SIZE(keys),
               bindings,bindings,
               keys);
    fd_decref(keys); env=env->parent;}
  return depth;
}


