/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef KNO_LEXENV_H
#define KNO_LEXENV_H 1
#ifndef KNO_LEXENV_H_INFO
#define KNO_LEXENV_H_INFO "include/kno/lexenv.h"
#endif

#ifndef KNO_INLINE_LEXENV
#define KNO_INLINE_LEXENV 0
#endif

typedef struct KNO_LEXENV {
  KNO_CONS_HEADER;
  lispval env_bindings;
  lispval env_exports;
  struct KNO_LEXENV *env_parent;
  struct KNO_LEXENV *env_copy;} KNO_LEXENV;
typedef struct KNO_LEXENV *kno_lexenv;

KNO_EXPORT kno_lexenv kno_copy_env(kno_lexenv env);
KNO_EXPORT void _kno_free_lexenv(struct KNO_LEXENV *env);
KNO_EXPORT kno_lexenv kno_dynamic_lexenv(kno_lexenv env);
KNO_EXPORT int kno_recycle_lexenv(kno_lexenv env);

#define KNO_XENV(x) \
  (kno_consptr(struct KNO_LEXENV *,x,kno_lexenv_type))
#define KNO_XENVIRONMENT(x) \
  (kno_consptr(struct KNO_LEXENV *,x,kno_lexenv_type))
#define KNO_LEXENVP(x) (KNO_TYPEP(x,kno_lexenv_type))

#if KNO_INLINE_LEXENV
KNO_FASTOP
void kno_free_lexenv(struct KNO_LEXENV *env)
{
  /* There are three cases:
	a simple static environment (env->env_copy == NULL)
	a static environment copied into a dynamic environment
	  (env->env_copy!=env)
	a dynamic environment (env->env_copy == env->env_copy)
  */
  if (env->env_copy)
    if (env == env->env_copy)
      kno_recycle_lexenv(env->env_copy);
    else {
      struct KNO_SCHEMAP *sm = KNO_XSCHEMAP(env->env_bindings);
      int i = 0, n = KNO_XSCHEMAP_SIZE(sm);
      lispval *vals = sm->schema_values;
      if ( sm->schemap_stackvals == 0)
	while (i < n) {
	  lispval val = vals[i++];
	  if ((KNO_CONSP(val))&&(KNO_MALLOCD_CONSP((kno_cons)val))) {
	    kno_decref(val);}}
      u8_destroy_rwlock(&(sm->table_rwlock));
      kno_recycle_lexenv(env->env_copy);}
  else {
    struct KNO_SCHEMAP *sm = KNO_XSCHEMAP(env->env_bindings);
    int i = 0, n = KNO_XSCHEMAP_SIZE(sm);
    lispval *vals = sm->schema_values;
    if ( sm->schemap_stackvals == 0)
      while (i < n) {
	lispval val = vals[i++];
	if ((KNO_CONSP(val))&&(KNO_MALLOCD_CONSP((kno_cons)val))) {
	  kno_decref(val);}}
    u8_destroy_rwlock(&(sm->table_rwlock));}
}
#else
#define kno_free_lexenv _kno_free_lexenv
#endif

KNO_EXPORT void kno_destroy_lexenv(kno_lexenv env);

#endif

