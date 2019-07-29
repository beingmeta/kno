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
#include "kno/lisp.h"
#include "kno/compounds.h"
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

#if (KNO_USE_TLS)
u8_tld_key kno_stackptr_key;
#elif (U8_USE__THREAD)
__thread struct KNO_STACK *kno_stackptr=NULL;
#else
struct KNO_STACK *kno_stackptr=NULL;
#endif

static lispval stack_entry_symbol, stack_target_symbol, opaque_symbol,
  pbound_symbol, pargs_symbol;
static lispval null_sym, unbound_sym, void_sym, default_sym, nofile_symbol;

static int tidy_stack_frames = 1;

static lispval annotate_source(lispval expr,lispval target);

static lispval source_subst(lispval expr,lispval target)
{
  if ( expr == target )
    return stack_target_symbol;
  else if (! (KNO_CONSP(expr)) )
    return expr;
  else return annotate_source(expr,target);
}

static lispval annotate_source(lispval expr,lispval target)
{
  if ( expr == target )
    return stack_target_symbol;
  else if (! (KNO_CONSP(expr)) )
    return expr;
  else {
    kno_lisp_type type = KNO_TYPEOF(expr);
    switch (type) {
    case kno_pair_type: {
      lispval car = source_subst(KNO_CAR(expr),target);
      lispval cdr = source_subst(KNO_CDR(expr),target);
      return kno_init_pair(NULL,car,cdr);}
    case kno_vector_type: {
      struct KNO_VECTOR *vec = (kno_vector) expr;
      lispval *elts = vec->vec_elts;
      int i=0, n=vec->vec_length;
      lispval newvec = kno_init_vector(NULL,n,NULL);
      KNO_SET_CONS_TYPE(newvec,type);
      while (i<n) {
        lispval old = elts[i], new = source_subst(old,target);
        KNO_VECTOR_SET(newvec,i,new);
        i++;}
      return newvec;}
    case kno_choice_type: {
      lispval results = KNO_EMPTY_CHOICE;
      KNO_DO_CHOICES(elt,expr) {
        lispval new_elt = source_subst(elt,target);
        KNO_ADD_TO_CHOICE(results,new_elt);}
      return results;}
    case kno_slotmap_type: {
      struct KNO_SLOTMAP *smap = (kno_slotmap) expr;
      int n_slots = smap->n_slots;
      struct KNO_SLOTMAP *newsmap = (kno_slotmap)
        kno_make_slotmap(n_slots,n_slots,NULL);
      struct KNO_KEYVAL *kvals = smap->sm_keyvals;
      struct KNO_KEYVAL *new_kvals = newsmap->sm_keyvals;
      int i=0, n = smap->n_slots; while (i<n) {
        new_kvals[i].kv_key = source_subst(kvals[i].kv_key,target);
        new_kvals[i].kv_val = source_subst(kvals[i].kv_val,target);
        i++;}
      return (lispval) newsmap;}
    default:
      return kno_incref(expr);}}
}

static lispval copy_bindings(lispval bindings)
{
  if (KNO_SLOTMAPP(bindings)) {
    int unlock = 0;
    struct KNO_SLOTMAP *smap = (kno_slotmap) bindings;
    struct KNO_SLOTMAP *copy =
      (kno_slotmap) kno_make_slotmap(smap->n_allocd,0,NULL);
    if (smap->table_uselock) {
      u8_read_lock(&(smap->table_rwlock));
      unlock = 1;}
    struct KNO_KEYVAL *read = smap->sm_keyvals, *limit = read + smap->n_slots;
    struct KNO_KEYVAL *write = copy->sm_keyvals;
    while (read<limit) {
      lispval key = read->kv_key;
      if (!( (KNO_NULLP(key)) || (KNO_VOIDP(key)) || (key == KNO_UNBOUND) )) {
        lispval val = read->kv_val;
        if (val == KNO_NULL) val = null_sym;
        else if (val == KNO_UNBOUND) val = unbound_sym;
        else if (val == KNO_VOID) val = void_sym;
        else if (val == KNO_DEFAULT) val = default_sym;
        else NO_ELSE;
        write->kv_key = kno_incref(key);
        write->kv_val = kno_incref(val);
        write++; read++;}
      else read++;}
    copy->n_slots = write-(copy->sm_keyvals);
    if (unlock) u8_rw_unlock(&(smap->table_rwlock));
    return (lispval) copy;}
  else if (KNO_SCHEMAPP(bindings)) {
    lispval copy = kno_copier(bindings,KNO_FULL_COPY);
    struct KNO_SCHEMAP *smap = (kno_schemap) copy;
    lispval *vals = smap->schema_values;
    int i = 0, n = smap->schema_length;
    while (i<n) {
      lispval val = vals[i];
      if (val == KNO_NULL) vals[i] = null_sym;
      else if (val == KNO_UNBOUND) vals[i] = unbound_sym;
      else if (val == KNO_VOID) vals[i] = void_sym;
      else if (val == KNO_DEFAULT) vals[i] = default_sym;
      else NO_ELSE;
      i++;}
    return copy;}
  else return kno_copier(bindings,KNO_FULL_COPY);
}

#define IS_EVAL_EXPR(x) ( (!(KNO_NULLP(x))) && ((KNO_PAIRP(x))) )

/* Stacks are rendered into LISP as vectors as follows:
   1. depth  (integer, increasing with calls)
   2. type   (apply, eval, load, other)
   3. label  (often the name of a function or expression type)
   4. status (a string (or #f) describing the state of the operation)
   5. op     (the thing being executed (expression, procedure applied, etc)
   6. args   (a vector of arguments to an applied procedure, or () otherwise)
   7. env    (the lexical environment established by the frame)
   8. source (the original source code for the call, if available)
*/

#define STACK_CREATE_OPTS KNO_COMPOUND_USEREF

static lispval stack2lisp(struct KNO_STACK *stack,struct KNO_STACK *inner)
{
  int n = 10;
  lispval depth = KNO_INT(stack->stack_depth);
  lispval type = KNO_FALSE, label = KNO_FALSE, status = KNO_FALSE;
  lispval op = stack->stack_op, env = KNO_FALSE;
  lispval argvec = ( stack->stack_args ) ?
    (kno_init_compound_from_elts(NULL,pargs_symbol,
                                 KNO_COMPOUND_COPYREF|KNO_COMPOUND_SEQUENCE,
                                 stack->n_args,
				 (lispval *) stack->stack_args)) :
    (KNO_FALSE);
  unsigned int icrumb = stack->stack_crumb;
  lispval source = stack->stack_source;
  lispval srcfile = nofile_symbol;
  if (icrumb == 0) {
    icrumb = u8_random(UINT_MAX);
    if (icrumb > KNO_MAX_FIXNUM) icrumb = icrumb%KNO_MAX_FIXNUM;
    stack->stack_crumb=icrumb;}
  lispval crumb = KNO_INT(icrumb);
  if (stack->stack_src) srcfile = kno_mkstring(stack->stack_src);
  if (stack->stack_type) type = kno_intern(stack->stack_type);
  if (stack->stack_label) label = kno_intern(stack->stack_label);
  if (stack->stack_status) status = kno_mkstring(stack->stack_status);
  if ( (tidy_stack_frames) &&
       (IS_EVAL_EXPR(source)) &&
       (IS_EVAL_EXPR(stack->stack_source)) ) {
    if ( (inner) && (IS_EVAL_EXPR(inner->stack_source)) )
      op = annotate_source(stack->stack_source,inner->stack_source);
    else op = kno_incref(stack->stack_source);}
  else if ( (inner) &&
            (IS_EVAL_EXPR(op)) &&
            (IS_EVAL_EXPR(stack->stack_op)) )
    op = annotate_source(stack->stack_op,inner->stack_op);
  else kno_incref(op);

  if (stack->stack_env) {
    lispval bindings = stack->stack_env->env_bindings;
    if ( (SLOTMAPP(bindings)) || (SCHEMAPP(bindings)) ) {
      lispval copied = copy_bindings(bindings);
      env = kno_init_compound(NULL,pbound_symbol,0,1,copied);}}

  if (KNO_VOIDP(op)) op = KNO_FALSE;
  if (KNO_VOIDP(source))
    source = KNO_FALSE;
  else if (KNO_EQUALP(op,source))
    source = KNO_FALSE;
  else if ( (inner) && (IS_EVAL_EXPR(source)) &&
            (IS_EVAL_EXPR(inner->stack_source)) )
    source = annotate_source(stack->stack_source,inner->stack_source);
  else kno_incref(source);

  if ( (KNO_FALSEP(argvec)) && (KNO_CONSP(source)) ) argvec=KNO_NIL;
  if (KNO_FALSEP(status)) {
    n--; if (KNO_FALSEP(env)) { n--; if (KNO_FALSEP(source)) {
        n--; if (KNO_FALSEP(argvec)) {
          n--;}}}}
  switch (n) {
  case 6:
    return kno_init_compound
      (NULL,stack_entry_symbol,
       STACK_CREATE_OPTS,6,depth,type,label,op,srcfile,crumb);
  case 7:
    return kno_init_compound
      (NULL,stack_entry_symbol,
       STACK_CREATE_OPTS,7,depth,type,label,op,srcfile,crumb,
       argvec);
  case 8:
    return kno_init_compound
      (NULL,stack_entry_symbol,
       STACK_CREATE_OPTS,8,depth,type,label,op,srcfile,crumb,
       argvec,source);
  case 9:
    return kno_init_compound
      (NULL,stack_entry_symbol,
       STACK_CREATE_OPTS,9,depth,type,label,op,srcfile,crumb,
       argvec,source,env);
  default:
    return kno_init_compound
      (NULL,stack_entry_symbol,
       STACK_CREATE_OPTS,10,depth,type,label,op,srcfile,crumb,
       argvec,source,env,status);
  }
}

KNO_EXPORT
lispval kno_get_backtrace(struct KNO_STACK *stack)
{
  if (stack == NULL) stack=kno_stackptr;
  if (stack == NULL) return KNO_EMPTY_LIST;
  int n = stack->stack_depth+1, i = 0;
  lispval result = kno_make_vector(n,NULL);
  struct KNO_STACK *prev = NULL;
  while (stack) {
    lispval entry = stack2lisp(stack,prev);
    if (i < n) {
      KNO_VECTOR_SET(result,i,entry);}
    else u8_log(LOG_CRIT,"BacktraceOverflow",
                "Inconsistent depth %d",n-1);
    prev=stack;
    stack=stack->stack_caller;
    i++;}
  return result;
}

KNO_EXPORT
void kno_sum_backtrace(u8_output out,lispval stacktrace)
{
  if (KNO_VECTORP(stacktrace)) {
    int i = 0, len = KNO_VECTOR_LENGTH(stacktrace), n = 0;
    while (i<len) {
      lispval entry = KNO_VECTOR_REF(stacktrace,i);
      if (KNO_COMPOUND_TYPEP(entry,stack_entry_symbol)) {
        lispval type=KNO_COMPOUND_REF(entry,1);
        lispval label=KNO_COMPOUND_REF(entry,2);
        lispval status=KNO_COMPOUND_REF(entry,3);
        if (n) u8_puts(out," ⇒ ");
        if (STRINGP(label)) u8_puts(out,CSTRING(label));
        else u8_putc(out,'?');
        if (STRINGP(type)) {
          u8_putc(out,'.');
          u8_puts(out,CSTRING(type));}
        else u8_puts(out,".?");
        if (STRINGP(status)) {
          u8_putc(out,'(');
          u8_puts(out,CSTRING(status));
          u8_putc(out,')');}
        n++;}
      i++;}}
}

void kno_init_stacks_c()
{
#if (KNO_USE_TLS)
  u8_new_threadkey(&kno_stackptr_key,NULL);
  u8_tld_set(kno_stackptr_key,(void *)NULL);
#else
  kno_stackptr=NULL;
#endif

  kno_register_config
    ("TIDYSTACKS",
     _("Whether to include raw ops in stacks when source is available"),
     kno_boolconfig_get,kno_boolconfig_set,&tidy_stack_frames);

  opaque_symbol = kno_intern("%opaque");
  stack_entry_symbol = kno_intern("_stack");
  stack_target_symbol = kno_intern("$<<*eval*>>$");
  pbound_symbol = kno_intern("%bound");
  pargs_symbol = kno_intern("%args");

  null_sym = kno_intern("#null");
  unbound_sym = kno_intern("#unbound");
  void_sym = kno_intern("#void");
  default_sym = kno_intern("#default");
  nofile_symbol = kno_intern("nofile");

}

