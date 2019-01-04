/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2015-2019 beingmeta, inc.
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
#include "framerd/compounds.h"
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

#if (FD_USE_TLS)
u8_tld_key fd_stackptr_key;
#elif (U8_USE__THREAD)
__thread struct FD_STACK *fd_stackptr=NULL;
#else
struct FD_STACK *fd_stackptr=NULL;
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
  else if (! (FD_CONSP(expr)) )
    return expr;
  else return annotate_source(expr,target);
}

static lispval annotate_source(lispval expr,lispval target)
{
  if ( expr == target )
    return stack_target_symbol;
  else if (! (FD_CONSP(expr)) )
    return expr;
  else {
    fd_ptr_type type = FD_PTR_TYPE(expr);
    switch (type) {
    case fd_pair_type: {
      lispval car = source_subst(FD_CAR(expr),target);
      lispval cdr = source_subst(FD_CDR(expr),target);
      return fd_init_pair(NULL,car,cdr);}
    case fd_vector_type: case fd_code_type: {
      struct FD_VECTOR *vec = (fd_vector) expr;
      lispval *elts = vec->vec_elts;
      int i=0, n=vec->vec_length;
      lispval newvec = fd_init_vector(NULL,n,NULL);
      FD_SET_CONS_TYPE(newvec,type);
      while (i<n) {
        lispval old = elts[i], new = source_subst(old,target);
        FD_VECTOR_SET(newvec,i,new);
        i++;}
      return newvec;}
    case fd_choice_type: {
      lispval results = FD_EMPTY_CHOICE;
      FD_DO_CHOICES(elt,expr) {
        lispval new_elt = source_subst(elt,target);
        FD_ADD_TO_CHOICE(results,new_elt);}
      return results;}
    case fd_slotmap_type: {
      struct FD_SLOTMAP *smap = (fd_slotmap) expr;
      int n_slots = smap->n_slots;
      struct FD_SLOTMAP *newsmap = (fd_slotmap)
        fd_make_slotmap(n_slots,n_slots,NULL);
      struct FD_KEYVAL *kvals = smap->sm_keyvals;
      struct FD_KEYVAL *new_kvals = newsmap->sm_keyvals;
      int i=0, n = smap->n_slots; while (i<n) {
        new_kvals[i].kv_key = source_subst(kvals[i].kv_key,target);
        new_kvals[i].kv_val = source_subst(kvals[i].kv_val,target);
        i++;}
      return (lispval) newsmap;}
    default:
      return fd_incref(expr);}}
}

static lispval copy_bindings(lispval bindings)
{
  if (FD_SLOTMAPP(bindings)) {
    int unlock = 0;
    struct FD_SLOTMAP *smap = (fd_slotmap) bindings;
    struct FD_SLOTMAP *copy =
      (fd_slotmap) fd_make_slotmap(smap->n_allocd,0,NULL);
    if (smap->table_uselock) {
      u8_read_lock(&(smap->table_rwlock));
      unlock = 1;}
    struct FD_KEYVAL *read = smap->sm_keyvals, *limit = read + smap->n_slots;
    struct FD_KEYVAL *write = copy->sm_keyvals;
    while (read<limit) {
      lispval key = read->kv_key;
      if (!( (FD_NULLP(key)) || (FD_VOIDP(key)) || (key == FD_UNBOUND) )) {
        lispval val = read->kv_val;
        if (val == FD_NULL) val = null_sym;
        else if (val == FD_UNBOUND) val = unbound_sym;
        else if (val == FD_VOID) val = void_sym;
        else if (val == FD_DEFAULT) val = default_sym;
        else NO_ELSE;
        write->kv_key = fd_incref(key);
        write->kv_val = fd_incref(val);
        write++; read++;}
      else read++;}
    copy->n_slots = write-(copy->sm_keyvals);
    if (unlock) u8_rw_unlock(&(smap->table_rwlock));
    return (lispval) copy;}
  else if (FD_SCHEMAPP(bindings)) {
    lispval copy = fd_copier(bindings,FD_FULL_COPY);
    struct FD_SCHEMAP *smap = (fd_schemap) copy;
    lispval *vals = smap->schema_values;
    int i = 0, n = smap->schema_length;
    while (i<n) {
      lispval val = vals[i];
      if (val == FD_NULL) vals[i] = null_sym;
      else if (val == FD_UNBOUND) vals[i] = unbound_sym;
      else if (val == FD_VOID) vals[i] = void_sym;
      else if (val == FD_DEFAULT) vals[i] = default_sym;
      else NO_ELSE;
      i++;}
    return copy;}
  else return fd_copier(bindings,FD_FULL_COPY);
}

#define IS_EVAL_EXPR(x) ( (!(FD_NULLP(x))) && ((FD_PAIRP(x)) || (FD_CODEP(x))) )

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

#define STACK_CREATE_OPTS FD_COMPOUND_USEREF

static lispval stack2lisp(struct FD_STACK *stack,struct FD_STACK *inner)
{
  int n = 9;
  lispval depth = FD_INT(stack->stack_depth);
  lispval type = FD_FALSE, label = FD_FALSE, status = FD_FALSE;
  lispval op = stack->stack_op, env = FD_FALSE;
  lispval argvec = ( stack->stack_args ) ?
    (fd_init_compound_from_elts(NULL,pargs_symbol,
                                FD_COMPOUND_COPYREF|FD_COMPOUND_SEQUENCE,
                                stack->n_args,
                                stack->stack_args)) :
    (FD_FALSE);
  lispval source = stack->stack_source;
  lispval srcfile = nofile_symbol;
  if (stack->stack_src) srcfile = lispval_string(stack->stack_src);
  if (stack->stack_type) type = fd_intern(stack->stack_type);
  if (stack->stack_label) label = fd_intern(stack->stack_label);
  if (stack->stack_status) status = lispval_string(stack->stack_status);
  if ( (tidy_stack_frames) &&
       (IS_EVAL_EXPR(source)) &&
       (IS_EVAL_EXPR(stack->stack_source)) ) {
    if ( (inner) && (IS_EVAL_EXPR(inner->stack_source)) )
      op = annotate_source(stack->stack_source,inner->stack_source);
    else op = fd_incref(stack->stack_source);}
  else if ( (inner) &&
            (IS_EVAL_EXPR(op)) &&
            (IS_EVAL_EXPR(stack->stack_op)) )
    op = annotate_source(stack->stack_op,inner->stack_op);
  else fd_incref(op);

  if (stack->stack_env) {
    lispval bindings = stack->stack_env->env_bindings;
    if ( (SLOTMAPP(bindings)) || (SCHEMAPP(bindings)) ) {
      lispval copied = copy_bindings(bindings);
      env = fd_init_compound(NULL,pbound_symbol,0,1,copied);}}

  if (FD_VOIDP(op)) op = FD_FALSE;
  if (FD_VOIDP(source))
    source = FD_FALSE;
  else if (FD_EQUALP(op,source))
    source = FD_FALSE;
  else if ( (inner) && (IS_EVAL_EXPR(source)) &&
            (IS_EVAL_EXPR(inner->stack_source)) )
    source = annotate_source(stack->stack_source,inner->stack_source);
  else fd_incref(source);

  if ( (FD_FALSEP(argvec)) && (FD_CONSP(source)) ) argvec=FD_NIL;
  if (FD_FALSEP(status)) {
    n--; if (FD_FALSEP(env)) { n--; if (FD_FALSEP(source)) {
        n--; if (FD_FALSEP(argvec)) {
          n--;}}}}
  switch (n) {
  case 5:
    return fd_init_compound(NULL,stack_entry_symbol,
                            STACK_CREATE_OPTS,5,depth,type,label,op,srcfile);
  case 6:
    return fd_init_compound(NULL,stack_entry_symbol,
                            STACK_CREATE_OPTS,6,depth,type,label,op,srcfile,
                            argvec);
  case 7:
    return fd_init_compound(NULL,stack_entry_symbol,
                            STACK_CREATE_OPTS,7,depth,type,label,op,srcfile,
                            argvec,source);
  case 8:
    return fd_init_compound(NULL,stack_entry_symbol,
                            STACK_CREATE_OPTS,8,depth,type,label,op,srcfile,
                            argvec,source,env);
  default:
    return fd_init_compound(NULL,stack_entry_symbol,
                            STACK_CREATE_OPTS,9,depth,type,label,op,srcfile,
                            argvec,source,env,status);
  }
}

FD_EXPORT
lispval fd_get_backtrace(struct FD_STACK *stack)
{
  if (stack == NULL) stack=fd_stackptr;
  if (stack == NULL) return FD_EMPTY_LIST;
  int n = stack->stack_depth+1, i = 0;
  lispval result = fd_make_vector(n,NULL);
  struct FD_STACK *prev = NULL;
  while (stack) {
    lispval entry = stack2lisp(stack,prev);
    if (i < n) {
      FD_VECTOR_SET(result,i,entry);}
    else u8_log(LOG_CRIT,"BacktraceOverflow",
                "Inconsistent depth %d",n-1);
    prev=stack;
    stack=stack->stack_caller;
    i++;}
  return result;
}

FD_EXPORT
void fd_sum_backtrace(u8_output out,lispval stacktrace)
{
  if (FD_VECTORP(stacktrace)) {
    int i = 0, len = FD_VECTOR_LENGTH(stacktrace), n = 0;
    while (i<len) {
      lispval entry = FD_VECTOR_REF(stacktrace,i);
      if (FD_COMPOUND_TYPEP(entry,stack_entry_symbol)) {
        lispval type=FD_COMPOUND_REF(entry,1);
        lispval label=FD_COMPOUND_REF(entry,2);
        lispval status=FD_COMPOUND_REF(entry,3);
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

void fd_init_stacks_c()
{
#if (FD_USE_TLS)
  u8_new_threadkey(&fd_stackptr_key,NULL);
  u8_tld_set(fd_stackptr_key,(void *)NULL);
#else
  fd_stackptr=NULL;
#endif

  fd_register_config
    ("TIDYSTACKS",
     _("Whether to include raw ops in stacks when source is available"),
     fd_boolconfig_get,fd_boolconfig_set,&tidy_stack_frames);

  opaque_symbol = fd_intern("%OPAQUE");
  stack_entry_symbol = fd_intern("_STACK");
  stack_target_symbol = fd_intern("$<<*eval*>>$");
  pbound_symbol = fd_intern("%BOUND");
  pargs_symbol = fd_intern("%ARGS");

  null_sym = fd_intern("#null");
  unbound_sym = fd_intern("#unbound");
  void_sym = fd_intern("#void");
  default_sym = fd_intern("#default");
  nofile_symbol = fd_intern("nofile");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
