/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2015-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_FCNIDS 1
#define KNO_INLINE_STACKS 1
#define KNO_INLINE_LEXENV 1
#define KNO_INLINE_APPLY  1

#include "kno/knosource.h"
#include "kno/lisp.h"
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

static kno_lexenv copy_lexenv(kno_lexenv env);

KNO_EXPORT
kno_lexenv kno_dynamic_lexenv(kno_lexenv env)
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
      newenv->env_exports  = kno_incref(env->env_exports);
      newenv->env_parent   = parent;
      newenv->env_bindings = bindings;
      newenv->env_copy     = newenv;
      env->env_copy        = newenv;
      if (KNO_SCHEMAPP(bindings)) {
	struct KNO_SCHEMAP *smap = (kno_schemap) bindings;
	int vlen = smap->schema_length;
	if (vlen < 256) {
	  newenv->env_vals = smap->table_values;
	  newenv->env_bits = vlen;}
	else {
	  newenv->env_vals     = NULL;
	  newenv->env_bits    = 0;}
	KNO_UNLOCK_PTR((void *)env);}
	return newenv;}}
}

static kno_lexenv copy_lexenv(kno_lexenv env)
{
  kno_lexenv dynamic = (env->env_copy) ? (env->env_copy) :
    (kno_dynamic_lexenv(env));
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
    kno_lexenv fresh = kno_dynamic_lexenv(env);
    lispval ref = (lispval)fresh;
    kno_incref(ref);
    return fresh;}
}

static void recycle_lexenv(struct KNO_RAW_CONS *envp)
{
  struct KNO_LEXENV *env = (struct KNO_LEXENV *)envp;
  kno_decref(env->env_bindings);
  kno_decref(env->env_exports);
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
  else if (KNO_NULLP(v))
    return 0;
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
  else if (KNO_REFCOUNT(v) == 1)
    return 1;
  else return 0;
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
    int env_refs = count_envrefs(env->env_bindings,env,env_recycle_depth) +
      count_envrefs(env->env_exports,env,env_recycle_depth);
    if ( (env_refs) && ( (refcount-env_refs) <= 1 ) ) {
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

KNO_EXPORT void kno_destroy_lexenv(kno_lexenv env)
{
  lispval bindings = env->env_bindings;
  env->env_bindings = KNO_EMPTY;
  env->env_vals = NULL;
  env->env_bits &= (~(KNO_LEXENV_NVALS_MASK));
  kno_decref(bindings);
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
    lispval ids = kno_get(env->env_bindings,KNOSYM_MODULEID,EMPTY);
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
    lispval ids = kno_get(env->env_bindings,KNOSYM_MODULEID,EMPTY);
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

/* Finding bindings */

KNO_EXPORT kno_lexenv kno_find_binding(kno_lexenv env,lispval symbol,int any)
{
  kno_lexenv scan = (env->env_copy) ? (env->env_copy) : (env);
  while (scan) {
    if ( (any) ? (kno_test(scan->env_bindings,symbol,VOID)) :
	 (KNO_TABLEP(scan->env_exports)) ?
	 (kno_test(scan->env_exports,symbol,VOID)) : (0) )
      return scan;
    scan = scan->env_parent;
    if ((scan) && (scan->env_copy))
      scan = scan->env_copy;}
  return NULL;
}

/* Lexenv dispatch function */

static lispval lexenv_dispatch(lispval env,lispval message,
			       kno_dispatch_flags flags,
			       kno_argvec args,kno_typeinfo info)
{
  struct KNO_LEXENV *lexenv = (kno_lexenv) env;
  lispval handler = KNO_VOID;
  int n = (flags & KNO_DISPATCH_ARG_MASK);
  if (kno_test(info->type_props,message,KNO_VOID)) {
    handler = kno_get(info->type_props,message,KNO_VOID);
    if (!(KNO_APPLICABLEP(handler))) { kno_decref(handler); handler=KNO_VOID;}}
  if (!(KNO_VOIDP(handler))) {}
  else if (KNO_VOIDP(lexenv->env_exports))
    handler = kno_get(lexenv->env_bindings,message,KNO_VOID);
  else handler = kno_get(lexenv->env_exports,message,KNO_VOID);
  if (KNO_VOIDP(handler)) {
    if ( flags & (KNO_DISPATCH_OPTIONAL|KNO_DISPATCH_NOERR) )
      return KNO_EMPTY;
    else return kno_dispatch_unhandled(env,message);}
  else return kno_dispatch_apply(kno_stackptr,handler,
				 flags|KNO_DISPATCH_DECREF,
				 n,args);
}

/* Initializing lexenv */

KNO_EXPORT void kno_init_lexenv_c()
{
  struct KNO_TYPEINFO *typeinfo = kno_use_typeinfo(KNO_CTYPE(kno_lexenv_type));
  typeinfo->type_dispatchfn = lexenv_dispatch;

  kno_unparsers[kno_lexenv_type]=unparse_lexenv;
  kno_copiers[kno_lexenv_type]=lisp_copy_lexenv;
  kno_recyclers[kno_lexenv_type]=recycle_lexenv;
  kno_dtype_writers[kno_lexenv_type]=lexenv_dtype;

}

