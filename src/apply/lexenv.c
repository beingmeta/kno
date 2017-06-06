/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2015-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1
#define FD_INLINE_APPLY  1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/lexenv.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

static fdtype moduleid_symbol;

static fd_lexenv copy_lexenv(fd_lexenv env);

FD_EXPORT
fd_lexenv dynamic_lexenv(fd_lexenv env)
{
  if (env->env_copy) return env->env_copy;
  else {
    struct FD_LEXENV *newenv = u8_alloc(struct FD_LEXENV);
    FD_INIT_FRESH_CONS(newenv,fd_lexenv_type);
    if (env->env_parent)
      newenv->env_parent = copy_lexenv(env->env_parent);
    else newenv->env_parent = NULL;
    if (FD_STATIC_CONSP(FD_CONS_DATA(env->env_bindings)))
      newenv->env_bindings = fd_copy(env->env_bindings);
    else newenv->env_bindings = fd_incref(env->env_bindings);
    newenv->env_exports = fd_incref(env->env_exports);
    env->env_copy = newenv; newenv->env_copy = newenv;
    return newenv;}
}

static fd_lexenv copy_lexenv(fd_lexenv env)
{
  fd_lexenv dynamic = (env->env_copy) ? (env->env_copy) :
    (dynamic_lexenv(env));
  return (fd_lexenv) fd_incref((fdtype)dynamic);
}

static fdtype lisp_copy_lexenv(fdtype env,int deep)
{
  return (fdtype) copy_lexenv((fd_lexenv)env);
}

FD_EXPORT fd_lexenv fd_copy_env(fd_lexenv env)
{
  if (env == NULL) return env;
  else if (env->env_copy) {
    fdtype existing = (fdtype)env->env_copy;
    fd_incref(existing);
    return env->env_copy;}
  else {
    fd_lexenv fresh = dynamic_lexenv(env);
    fdtype ref = (fdtype)fresh;
    fd_incref(ref);
    return fresh;}
}

static void recycle_lexenv(struct FD_RAW_CONS *envp)
{
  struct FD_LEXENV *env = (struct FD_LEXENV *)envp;
  fd_decref(env->env_bindings); fd_decref(env->env_exports);
  if (env->env_parent) fd_decref((fdtype)(env->env_parent));
  if (!(FD_STATIC_CONSP(envp))) {
    memset(env,0,sizeof(struct FD_LEXENV));
    u8_free(envp);}
}

/* Counting environment refs. This is a bit of a kludge to get around
   some inherently circular structures. */

static int env_recycle_depth = 4;

struct ENVCOUNT_STATE {
  fd_lexenv env;
  int count;};

static int envcountproc(fdtype v,void *data)
{
  struct ENVCOUNT_STATE *state = (struct ENVCOUNT_STATE *)data;
  fd_lexenv env = state->env;
  if (!(FD_CONSP(v))) return 1;
  else if (FD_STATICP(v)) return 1;
  else if (FD_LEXENVP(v)) {
    fd_lexenv scan = (fd_lexenv)v;
    while (scan)
      if ((scan == env)||(scan->env_copy == env)) {
        state->count++;
        return 1;}
      else scan = scan->env_parent;
    return 1;}
  else return 1;
}

static int count_envrefs(fdtype root,fd_lexenv env,int depth)
{
  struct ENVCOUNT_STATE state={env,0};
  fd_walk(envcountproc,root,&state,FD_WALK_CONSES,depth);
  return state.count;
}

FD_EXPORT
/* fd_recycle_lexenv:
     Arguments: a lisp pointer to an environment
     Returns: 1 if the environment was recycled.
 This handles circular environment problems.  The problem is that environments
   commonly contain pointers to procedures which point back to the environment
   they are closed in.  This does a limited structure descent to see how many
   reclaimable environment pointers there may be.  If the reclaimable references
   are one more than the environment's reference count, then you can recycle
   the entire environment.
*/
int fd_recycle_lexenv(fd_lexenv env)
{
  int refcount = FD_CONS_REFCOUNT(env);
  if (refcount==0) return 0; /* Stack cons */
  else if (refcount==1) { /* Normal GC */
    fd_decref((fdtype)env);
    return 1;}
  else {
    int sproc_count = count_envrefs(env->env_bindings,env,env_recycle_depth);
    if (sproc_count+1==refcount) {
      struct FD_RAW_CONS *envstruct = (struct FD_RAW_CONS *)env;
      fd_decref(env->env_bindings); fd_decref(env->env_exports);
      if (env->env_parent) fd_decref((fdtype)(env->env_parent));
      envstruct->conshead = (0xFFFFFF80|(env->conshead&0x7F));
      u8_free(env);
      return 1;}
    else {fd_decref((fdtype)env); return 0;}}
}

FD_EXPORT
void _fd_free_lexenv(fd_lexenv env)
{
  fd_free_lexenv(env);
}


static int unparse_lexenv(u8_output out,fdtype x)
{
  struct FD_LEXENV *env=
    fd_consptr(struct FD_LEXENV *,x,fd_lexenv_type);
  if (FD_HASHTABLEP(env->env_bindings)) {
    fdtype ids = fd_get(env->env_bindings,moduleid_symbol,FD_EMPTY_CHOICE);
    fdtype mid = FD_VOID;
    FD_DO_CHOICES(id,ids) {
      if (FD_SYMBOLP(id)) mid = id;}
    if (FD_SYMBOLP(mid))
      u8_printf(out,"#<MODULE %q #!%x>",mid,(unsigned long)env);
    else u8_printf(out,"#<ENVIRONMENT #!%x>",(unsigned long)env);}
  else u8_printf(out,"#<ENVIRONMENT #!%x>",(unsigned long)env);
  return 1;
}

static int dtype_lexenv(struct FD_OUTBUF *out,fdtype x)
{
  struct FD_LEXENV *env=
    fd_consptr(struct FD_LEXENV *,x,fd_lexenv_type);
  u8_string modname=NULL,  modfile=NULL;
  if (FD_HASHTABLEP(env->env_bindings)) {
    fdtype ids = fd_get(env->env_bindings,moduleid_symbol,FD_EMPTY_CHOICE);
    FD_DO_CHOICES(id,ids) {
      if (FD_SYMBOLP(id))
	modname=FD_SYMBOL_NAME(id);
      else if (FD_STRINGP(id))
	modfile=FD_STRDATA(id);
      else {}}}
  u8_byte buf[200]; struct FD_OUTBUF tmp;
  FD_INIT_OUTBUF(&tmp,buf,200,0);
  if ((modname)||(modfile)) {
    u8_byte *tagname="%MODULE";
    int n_elts = ((modname)&&(modfile)) ? (2) : (1);
    fd_write_byte(&tmp,dt_compound);
    fd_write_byte(&tmp,dt_symbol);
    fd_write_4bytes(&tmp,strlen(tagname));
    fd_write_bytes(&tmp,tagname,strlen(tagname));
    fd_write_byte(&tmp,dt_vector);
    fd_write_4bytes(&tmp,n_elts);
    size_t len=strlen(modname);
    fd_write_byte(&tmp,dt_symbol);
    fd_write_4bytes(&tmp,len);
    fd_write_bytes(&tmp,modname,len);
    if (modfile) {
      size_t len=strlen(modfile);
      fd_write_byte(&tmp,dt_string);
      fd_write_4bytes(&tmp,len);
      fd_write_bytes(&tmp,modfile,len);}}
  else {
    u8_byte *tagname="%LEXENV";
    fd_write_byte(&tmp,dt_compound);
    fd_write_byte(&tmp,dt_symbol);
    fd_write_4bytes(&tmp,strlen(tagname));
    fd_write_bytes(&tmp,tagname,strlen(tagname));
    fd_write_byte(&tmp,dt_vector);
    fd_write_4bytes(&tmp,1);
    unsigned char buf[32], *numstring=
      u8_uitoa16((unsigned long long)x,buf);
    size_t len=strlen(numstring);
    fd_write_byte(&tmp,dt_string);
    fd_write_4bytes(&tmp,(len+2));
    fd_write_bytes(&tmp,"#!",2);
    fd_write_bytes(&tmp,numstring,len);}
  size_t n_bytes=tmp.bufwrite-tmp.buffer;
  fd_write_bytes(out,tmp.buffer,n_bytes);
  fd_close_outbuf(&tmp);
  return n_bytes;
}

FD_EXPORT void fd_init_lexenv_c()
{
  moduleid_symbol = fd_intern("%MODULEID");

  fd_unparsers[fd_lexenv_type]=unparse_lexenv;
  fd_copiers[fd_lexenv_type]=lisp_copy_lexenv;
  fd_recyclers[fd_lexenv_type]=recycle_lexenv;
  fd_dtype_writers[fd_lexenv_type]=dtype_lexenv;

}
