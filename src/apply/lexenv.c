/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2015-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_FCNIDS 1
#define KNO_INLINE_STACKS 1
#define KNO_INLINE_LEXENV 1
#define KNO_INLINE_APPLY  1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/lexenv.h"
#include "kno/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

static lispval moduleid_symbol;

static kno_lexenv copy_lexenv(kno_lexenv env);

KNO_EXPORT
kno_lexenv dynamic_lexenv(kno_lexenv env)
{
  if (env->env_copy)
    return env->env_copy;
  else {
    kno_lexenv parent = (env->env_parent) ?
      (copy_lexenv(env->env_parent)) :
      (NULL);
    lispval bindings =
      (KNO_STATIC_CONSP(KNO_CONS_DATA(env->env_bindings))) ?
      (kno_copy(env->env_bindings)) :
      (kno_incref(env->env_bindings));
    KNO_LOCK_PTR((void *)env);
    if (env->env_copy) {
      kno_decref(bindings);
      kno_decref((lispval)parent);
      KNO_UNLOCK_PTR((void *)env);
      return env->env_copy;}
    else  {
      struct KNO_LEXENV *newenv = u8_alloc(struct KNO_LEXENV);
      KNO_INIT_FRESH_CONS(newenv,kno_lexenv_type);
      newenv->env_exports = kno_incref(env->env_exports);
      newenv->env_parent = parent;
      newenv->env_bindings = bindings;
      newenv->env_copy = newenv;
      env->env_copy = newenv;
      KNO_UNLOCK_PTR((void *)env);
      return newenv;}}
}

static kno_lexenv copy_lexenv(kno_lexenv env)
{
  kno_lexenv dynamic = (env->env_copy) ? (env->env_copy) :
    (dynamic_lexenv(env));
  return (kno_lexenv) kno_incref((lispval)dynamic);
}

static lispval lisp_copy_lexenv(lispval env,int deep)
{
  return (lispval) copy_lexenv((kno_lexenv)env);
}

KNO_EXPORT kno_lexenv kno_copy_env(kno_lexenv env)
{
  if (env == NULL) return env;
  else if (env->env_copy) {
    lispval existing = (lispval)env->env_copy;
    kno_incref(existing);
    return env->env_copy;}
  else {
    kno_lexenv fresh = dynamic_lexenv(env);
    lispval ref = (lispval)fresh;
    kno_incref(ref);
    return fresh;}
}

static void recycle_lexenv(struct KNO_RAW_CONS *envp)
{
  struct KNO_LEXENV *env = (struct KNO_LEXENV *)envp;
  kno_decref(env->env_bindings); kno_decref(env->env_exports);
  if (env->env_parent) kno_decref((lispval)(env->env_parent));
  if (!(KNO_STATIC_CONSP(envp))) {
    memset(env,0,sizeof(struct KNO_LEXENV));
    u8_free(envp);}
}

/* Counting environment refs. This is a bit of a kludge to get around
   some inherently circular structures. */

static int env_recycle_depth = 42;

struct ENVCOUNT_STATE {
  kno_lexenv env;
  int count;};

static int envcountproc(lispval v,void *data)
{
  struct ENVCOUNT_STATE *state = (struct ENVCOUNT_STATE *)data;
  kno_lexenv env = state->env;
  if (!(CONSP(v)))
    return 1;
  else if (KNO_STATICP(v))
    return 0;
  else if (KNO_LEXENVP(v)) {
    kno_lexenv scan = (kno_lexenv)v;
    while (scan)
      if ( (scan == env) ||
           (scan->env_copy == env) ) {
        state->count++;
        return 0;}
      else scan = scan->env_parent;
    return 0;}
  else return 1;
}

static int count_envrefs(lispval root,kno_lexenv env,int depth)
{
  struct ENVCOUNT_STATE state={env,0};
  kno_walk(envcountproc,root,&state,KNO_WALK_CONSES,depth);
  return state.count;
}

KNO_EXPORT
/* kno_recycle_lexenv:
     Arguments: a lisp pointer to an environment
     Returns: 1 if the environment was recycled.
 This handles circular environment problems.  The problem is that environments
   commonly contain pointers to procedures which point back to the environment
   they are closed in.  This does a limited structure descent to see how many
   reclaimable environment pointers there may be.  If the reclaimable references
   are one more than the environment's reference count, then you can recycle
   the entire environment.
*/
int kno_recycle_lexenv(kno_lexenv env)
{
  int refcount = KNO_CONS_REFCOUNT(env);
  if (refcount==0) return 0; /* Stack cons */
  else if (refcount==1) { /* Normal GC */
    kno_decref((lispval)env);
    return 1;}
  else {
    int env_refs = count_envrefs(env->env_bindings,env,env_recycle_depth);
    if (env_refs == refcount) {
      struct KNO_RAW_CONS *envstruct = (struct KNO_RAW_CONS *)env;
      kno_decref(env->env_bindings);
      kno_decref(env->env_exports);
      if (env->env_parent)
        kno_decref((lispval)(env->env_parent));
      envstruct->conshead = (0xFFFFFF80|(env->conshead&0x7F));
      u8_free(env);
      return 1;}
    else {
      kno_decref((lispval)env);
      return 0;}}
}

KNO_EXPORT
void _kno_free_lexenv(kno_lexenv env)
{
  kno_free_lexenv(env);
}

static int unparse_lexenv(u8_output out,lispval x)
{
  struct KNO_LEXENV *env=
    kno_consptr(struct KNO_LEXENV *,x,kno_lexenv_type);
  if (HASHTABLEP(env->env_bindings)) {
    lispval ids = kno_get(env->env_bindings,moduleid_symbol,EMPTY);
    lispval mid = VOID;
    /* The symbol in the module_id binding is the actual module name,
       if it is a module (at least a registered one). */
    DO_CHOICES(id,ids) { if (SYMBOLP(id)) mid = id;}
    if (SYMBOLP(mid))
      u8_printf(out,"#<MODULE %q #!%x>",mid,(unsigned long)env);
    else u8_printf(out,"#<ENVIRONMENT #!%x>",(unsigned long)env);}
  else u8_printf(out,"#<ENVIRONMENT #!%x>",(unsigned long)env);
  return 1;
}

/* Of course this doesn't preserve "eqness" in any way */
static ssize_t lexenv_dtype(struct KNO_OUTBUF *out,lispval x)
{
  struct KNO_LEXENV *env=
    kno_consptr(struct KNO_LEXENV *,x,kno_lexenv_type);
  u8_string modname=NULL,  modfile=NULL;
  if (HASHTABLEP(env->env_bindings)) {
    lispval ids = kno_get(env->env_bindings,moduleid_symbol,EMPTY);
    DO_CHOICES(id,ids) {
      if (SYMBOLP(id))
        modname=SYM_NAME(id);
      else if (STRINGP(id))
        modfile=CSTRING(id);
      else {}}}
  u8_byte buf[200]; struct KNO_OUTBUF tmp = { 0 };
  KNO_INIT_OUTBUF(&tmp,buf,200,0);
  if ((modname)||(modfile)) {
    u8_byte *tagname="%MODULE";
    int n_elts = ((modname)&&(modfile)) ? (2) : (1);
    kno_write_byte(&tmp,dt_compound);
    kno_write_byte(&tmp,dt_symbol);
    kno_write_4bytes(&tmp,strlen(tagname));
    kno_write_bytes(&tmp,tagname,strlen(tagname));
    kno_write_byte(&tmp,dt_vector);
    kno_write_4bytes(&tmp,n_elts);
    size_t len=strlen(modname);
    kno_write_byte(&tmp,dt_symbol);
    kno_write_4bytes(&tmp,len);
    kno_write_bytes(&tmp,modname,len);
    if (modfile) {
      size_t len=strlen(modfile);
      kno_write_byte(&tmp,dt_string);
      kno_write_4bytes(&tmp,len);
      kno_write_bytes(&tmp,modfile,len);}}
  else {
    u8_byte *tagname="%LEXENV";
    kno_write_byte(&tmp,dt_compound);
    kno_write_byte(&tmp,dt_symbol);
    kno_write_4bytes(&tmp,strlen(tagname));
    kno_write_bytes(&tmp,tagname,strlen(tagname));
    kno_write_byte(&tmp,dt_vector);
    kno_write_4bytes(&tmp,1);
    unsigned char buf[32], *numstring=
      u8_uitoa16(KNO_LONGVAL(x),buf);
    size_t len=strlen(numstring);
    kno_write_byte(&tmp,dt_string);
    kno_write_4bytes(&tmp,(len+2));
    kno_write_bytes(&tmp,"#!",2);
    kno_write_bytes(&tmp,numstring,len);}
  ssize_t n_bytes=tmp.bufwrite-tmp.buffer;
  kno_write_bytes(out,tmp.buffer,n_bytes);
  kno_close_outbuf(&tmp);
  return n_bytes;
}

KNO_EXPORT void kno_init_lexenv_c()
{
  moduleid_symbol = kno_intern("%MODULEID");

  kno_unparsers[kno_lexenv_type]=unparse_lexenv;
  kno_copiers[kno_lexenv_type]=lisp_copy_lexenv;
  kno_recyclers[kno_lexenv_type]=recycle_lexenv;
  kno_dtype_writers[kno_lexenv_type]=lexenv_dtype;

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
