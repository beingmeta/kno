/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_LEXENV_H
#define FRAMERD_LEXENV_H 1
#ifndef FRAMERD_LEXENV_H_INFO
#define FRAMERD_LEXENV_H_INFO "include/framerd/lexenv.h"
#endif

#ifndef FD_INLINE_LEXENV
#define FD_INLINE_LEXENV 0
#endif

typedef struct FD_LEXENV {
  FD_CONS_HEADER;
  lispval env_bindings;
  lispval env_exports;
  struct FD_LEXENV *env_parent;
  struct FD_LEXENV *env_copy;} FD_LEXENV;
typedef struct FD_LEXENV *fd_lexenv;

FD_EXPORT fd_lexenv fd_copy_env(fd_lexenv env);
FD_EXPORT void _fd_free_lexenv(struct FD_LEXENV *env);
FD_EXPORT int fd_recycle_lexenv(fd_lexenv env);

#define FD_XENV(x) \
  (fd_consptr(struct FD_LEXENV *,x,fd_lexenv_type))
#define FD_XENVIRONMENT(x) \
  (fd_consptr(struct FD_LEXENV *,x,fd_lexenv_type))
#define FD_LEXENVP(x) (FD_TYPEP(x,fd_lexenv_type))

#if FD_INLINE_LEXENV
FD_FASTOP
void fd_free_lexenv(struct FD_LEXENV *env)
{
  /* There are three cases:
	a simple static environment (env->env_copy == NULL)
	a static environment copied into a dynamic environment
	  (env->env_copy!=env)
	a dynamic environment (env->env_copy == env->env_copy)
  */
  if (env->env_copy)
    if (env == env->env_copy)
      fd_recycle_lexenv(env->env_copy);
    else {
      struct FD_SCHEMAP *sm = FD_XSCHEMAP(env->env_bindings);
      int i = 0, n = FD_XSCHEMAP_SIZE(sm);
      lispval *vals = sm->schema_values;
      if ( sm->schemap_stackvals == 0)
	while (i < n) {
	  lispval val = vals[i++];
	  if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
	    fd_decref(val);}}
      u8_destroy_rwlock(&(sm->table_rwlock));
      fd_recycle_lexenv(env->env_copy);}
  else {
    struct FD_SCHEMAP *sm = FD_XSCHEMAP(env->env_bindings);
    int i = 0, n = FD_XSCHEMAP_SIZE(sm);
    lispval *vals = sm->schema_values;
    if ( sm->schemap_stackvals == 0)
      while (i < n) {
	lispval val = vals[i++];
	if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
	  fd_decref(val);}}
    u8_destroy_rwlock(&(sm->table_rwlock));}
}
#else
#define fd_free_lexenv _fd_free_lexenv
#endif

#endif

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
