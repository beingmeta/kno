/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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

typedef struct FD_ENVIRONMENT {
  FD_CONS_HEADER;
  fdtype env_bindings;
  fdtype env_exports;
  struct FD_ENVIRONMENT *env_parent;
  struct FD_ENVIRONMENT *env_copy;} FD_ENVIRONMENT;
typedef struct FD_ENVIRONMENT *fd_lispenv;

FD_EXPORT void _fd_free_environment(struct FD_ENVIRONMENT *env);

#if FD_INLINE_LEXENV
FD_FASTOP
void fd_free_environment(struct FD_ENVIRONMENT *env)
{
  /* There are three cases:
        a simple static environment (env->env_copy == NULL)
        a static environment copied into a dynamic environment
          (env->env_copy!=env)
        a dynamic environment (env->env_copy == env->env_copy)
  */
  if (env->env_copy)
    if (env == env->env_copy)
      fd_recycle_environment(env->env_copy);
    else {
      struct FD_SCHEMAP *sm = FD_XSCHEMAP(env->env_bindings);
      int i = 0, n = FD_XSCHEMAP_SIZE(sm);
      fdtype *vals = sm->schema_values;
      while (i < n) {
        fdtype val = vals[i++];
        if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
          fd_decref(val);}}
      u8_destroy_rwlock(&(sm->table_rwlock));
      fd_recycle_environment(env->env_copy);}
  else {
    struct FD_SCHEMAP *sm = FD_XSCHEMAP(env->env_bindings);
    int i = 0, n = FD_XSCHEMAP_SIZE(sm);
    fdtype *vals = sm->schema_values;
    while (i < n) {
      fdtype val = vals[i++];
      if ((FD_CONSP(val))&&(FD_MALLOCD_CONSP((fd_cons)val))) {
        fd_decref(val);}}
    u8_destroy_rwlock(&(sm->table_rwlock));}
}
#else
#define fd_free_environment _fd_free_environment
#endif

#endif
