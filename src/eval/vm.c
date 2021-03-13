/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_LANG_CORE		(!(KNO_AVOID_INLINE))
#define KNO_EVAL_INTERNALS 1

#define INIT_ARGBUF_LEN 7

/* Evaluator defines */

#ifndef KNO_WITH_TAIL_EVAL
#define KNO_WITH_TAIL_EVAL 1
#endif

#ifndef KNO_WITH_TAIL_CALLS
#define KNO_WITH_TAIL_CALLS 1
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/opcodes.h"
#include "kno/dbprims.h"
#include "kno/ffi.h"
#include "kno/cprims.h"
#include "kno/profiles.h"

#include "../apply/apply_internals.h"
#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

static lispval isa_op(lispval args,kno_lexenv env,kno_stack stack,int require);

typedef lispval (*op_handler)(int,lispval *,kno_lexenv,kno_stack,int);
typedef lispval (*op2)(lispval,lispval);

static lispval call2(op2 fn,lispval x,lispval y)
{
  if ( (KNO_CHOICEP(x)) || (KNO_CHOICEP(y)) ) {
    lispval results = KNO_EMPTY;
    ITER_CHOICES(xscan,xlimit,x);
    ITER_CHOICES(ystart,ylimit,y);
    while (xscan<xlimit) {
      const lispval arg1 = *xscan++, *yscan = ystart;
      while (yscan<ylimit) {
	const lispval arg2 = *yscan++, r = fn(arg1,arg2);
	if (KNO_ABORTED(r)) {
	  kno_decref(results);
	  return r;}
	else ADD_TO_CHOICE(results,r);}}
    return results;}
  else return fn(x,y);
}

int eval_args(int argc,lispval *into,lispval exprs,
	      kno_lexenv env,kno_stack stack,
	      int prune)
{
  int conses = 0, choices = 0, qchoices = 0, empties = 0, n = 0;
  lispval scan = exprs, *write = into;
  while (PAIRP(scan)) {
    lispval expr = KNO_CAR(scan); scan = KNO_CDR(scan);
    lispval v = (KNO_CONSP(expr)) ?
      ((KNO_PAIRP(expr)) ? (vm_eval(KNO_CAR(expr),expr,env,stack,0)) :
       (KNO_SCHEMAPP(expr)) ? (eval_schemap(expr,env,stack)) :
       (KNO_CHOICEP(expr)) ? (eval_choice(expr,env,stack)) :
       (kno_incref(expr))) :
      (KNO_IMMEDIATEP(expr)) ?
      ((KNO_LEXREFP(expr)) ? (eval_lexref(expr,env)) :
       (KNO_SYMBOLP(expr)) ? (eval_symbol(expr,env)) :
       (expr) ):
      (expr);
    if (KNO_PRECHOICEP(v)) {
      v = kno_simplify_choice(v);}
    if (KNO_CONSP(v)) {
      conses=1;
      if (KNO_CHOICEP(v)) choices=1;
      else if (KNO_QCHOICEP(v)) qchoices=1;
      else NO_ELSE;}
    else if (IMMEDIATEP(v)) {
      if (KNO_EMPTYP(v)) {
	if (prune) {
	  kno_decref_vec(into,write-into);
	  return KNO_PRUNED;}
	else empties=1;}
      else if (v == KNO_THROW_VALUE) {
	kno_decref_vec(into,write-into);
	return KNO_THROWN_ARG;}
      else if (BAD_ARGP(v)) {
	kno_decref_vec(into,write-into);
	if (KNO_VOIDP(v)) kno_seterr(kno_VoidArgument,"eval_args",NULL,expr);
	return -1;}}
    else NO_ELSE;
    n++; if (RARELY(n>argc)) return -2;;
    *write++=v;}
  return (KNO_GOOD_ARGS) |
    ( (empties) ?  (KNO_FAILED_ARGS) : (0) ) |
    ( (conses) ?   (KNO_CONSED_ARGS) : (0)) |
    ( (choices) ?  (KNO_AMBIG_ARGS)  : (0)) |
    ( (qchoices) ? (KNO_QCHOICE_ARGS) : (0));
}

/* Reduce ops */

typedef lispval (*reducefn)(lispval state,lispval arg,int *done);

KNO_FASTOP lispval reduce_op(kno_stack stack,
			     lispval exprs,
			     kno_lexenv env,
			     lispval state,
			     lispval prune,
			     reducefn fn)
{
  int done = 0; kno_incref(state);
  while ( (done == 0) && (KNO_PAIRP(exprs)) ) {
    lispval expr = pop_arg(exprs);
    lispval arg = kno_eval(expr,env,stack);
    if (KNO_ABORTED(arg)) {
      kno_decref(state);
      return arg;}
    else NO_ELSE;
    lispval reduced = fn(state,arg,&done);
    if (KNO_ABORTED(reduced)) return reduced;
    if ( (KNO_EMPTYP(reduced)) && (!(KNO_VOIDP(prune))) )
      return kno_incref(prune);
    else state = reduced;}
  return kno_simplify_choice(state);
}

KNO_FASTOP lispval nd_reduce_op(kno_stack stack,
				lispval exprs,
				kno_lexenv env,
				lispval state,
				reducefn fn)
{
  lispval result = kno_incref(state), arg = VOID;
  int done = 0;
  while ( (done == 0) && (KNO_PAIRP(exprs)) ) {
    lispval expr = pop_arg(exprs);
    arg = kno_eval(expr,env,stack);
    if (KNO_ABORTED(arg)) {
      kno_decref(state);
      return arg;}
    else if ( (KNO_CHOICEP(state)) || (KNO_CHOICEP(arg)) ) {
      lispval new_state = KNO_EMPTY;
      if (KNO_CHOICEP(state))
	if (KNO_CHOICEP(arg)) {
	  KNO_DO_CHOICES(state_x,state) {
	    KNO_DO_CHOICES(arg_x,arg) {
	      lispval reduced = fn(state_x,arg_x,&done);
	      if (ABORTED(reduced)) {
		kno_decref(new_state);
		KNO_STOP_DO_CHOICES;
		result = reduced;
		goto error_exit;}
	      else {KNO_ADD_TO_CHOICE(new_state,reduced);}}}}
	else {
	  KNO_DO_CHOICES(state_x,state) {
	    lispval reduced = fn(state_x,arg,&done);
	    if (ABORTED(reduced)) {
	      kno_decref(new_state);
	      KNO_STOP_DO_CHOICES;
	      result = reduced;
	      goto error_exit;}
	    else {KNO_ADD_TO_CHOICE(new_state,reduced);}}}
      else {
	KNO_DO_CHOICES(arg_x,arg) {
	  lispval reduced = fn(state,arg_x,&done);
	  if (ABORTED(reduced)) {
	    kno_decref(new_state);
	    KNO_STOP_DO_CHOICES;
	    result = reduced;
	    goto error_exit;}
	  else {KNO_ADD_TO_CHOICE(new_state,reduced);}}}
      kno_decref(state);
      state = kno_simplify_choice(new_state);}
    else {
      lispval reduced = fn(state,arg,&done);
      if (KNO_ABORTP(reduced)) {
	kno_decref(state);
	kno_decref(arg);
	return reduced;}
      else if (state != reduced) {
	lispval prev_state = state;
	state = kno_simplify_choice(reduced);
	kno_decref(prev_state);}
      else NO_ELSE;}}
  return kno_simplify_choice(state);
 error_exit:
  kno_decref(state);
  kno_decref(arg);
  return result;
}

/* Logical operations */

static lispval try_op(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  while (PAIRP(body)) {
    lispval expr = KNO_CAR(body); body = KNO_CDR(body);
    if (body==KNO_NIL)
      return doeval(expr,env,stack,tail);
    else {
      lispval v = doeval(expr,env,stack,0);
      if (!((KNO_EMPTYP(v)))) return v;
      else kno_decref(v);}}
  return KNO_EMPTY;
}

static lispval and_op(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  while (PAIRP(body)) {
    lispval expr = KNO_CAR(body); body = KNO_CDR(body);
    if (body==KNO_NIL)
      return doeval(expr,env,stack,tail);
    else {
      lispval v = doeval(expr,env,stack,0);
      if (KNO_EMPTYP(v)) return v;
      else if (KNO_ABORTED(v)) return v;
      else if (KNO_FALSEP(v)) return v;
      else kno_decref(v);}}
  return KNO_TRUE;
}

static lispval or_op(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  while (PAIRP(body)) {
    lispval expr = KNO_CAR(body); body = KNO_CDR(body);
    if (body==KNO_NIL)
      return doeval(expr,env,stack,tail);
    else {
      lispval v = doeval(expr,env,stack,0);
      if (KNO_EMPTYP(v)) return v;
      else if (KNO_ABORTED(v)) return v;
      else if (!(KNO_FALSEP(v))) return v;
      else kno_decref(v);}}
  return KNO_FALSE;
}

/* Set operations */

/* This does a simple binary search of a sorted choice vector,
   looking for a particular element. Once more, we separate out the
   atomic case because it just requires pointer comparisons.  */
static int inchoicep(lispval x,struct KNO_CHOICE *choice)
{
  int size = KNO_XCHOICE_SIZE(choice);
  const lispval *bottom = KNO_XCHOICE_DATA(choice), *top = bottom+(size-1);
  if (ATOMICP(x)) {
    while (top>=bottom) {
      const lispval *middle = bottom+(top-bottom)/2;
      if (x == *middle) return 1;
      else if (CONSP(*middle)) top = middle-1;
      else if (x < *middle) top = middle-1;
      else bottom = middle+1;}
    return 0;}
  else {
    while (top>=bottom) {
      const lispval *middle = bottom+(top-bottom)/2;
      int comparison = __kno_cons_compare(x,*middle);
      if (comparison == 0) return 1;
      else if (comparison<0) top = middle-1;
      else bottom = middle+1;}
    return 0;}
}

static lispval do_union(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  lispval result = KNO_VOID;
  while (KNO_PAIRP(body)) {
    lispval expr = pop_arg(body);
    lispval arg  = doeval(expr,env,stack,0);
    if (KNO_ABORTED(arg)) {
      kno_decref(result);
      return arg;}
    else if (KNO_VOIDP(result))
      result = arg;
    else {KNO_ADD_TO_CHOICE(result,arg);}}
  return kno_simplify_choice(result);
}

static void free_choices(struct KNO_CHOICE **choices,int n,int free_vec)
{
  int i=0; while (i<n) {
    lispval ch = (lispval)choices[i++];
    kno_decref(ch);}
  if (free_vec) u8_free(choices);
}

static lispval do_intersection(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  lispval result = KNO_VOID, single = KNO_VOID;
  int n_choices = 0, max_choices = 17, overflow = 0;
  struct KNO_CHOICE *_choices[17], **choices=_choices;
  while (KNO_PAIRP(body)) {
    lispval expr = pop_arg(body);
    lispval arg  = doeval(expr,env,stack,0);
    if (KNO_ABORTED(arg)) {
      result=arg;
      goto early_exit;}
    else if (KNO_EMPTYP(arg)) {
      result = arg;
      goto early_exit;}
    else if (KNO_VOIDP(arg)) {
      result = kno_err(kno_VoidArgument,"intersection",NULL,expr);
      goto early_exit;}
    else if (!(CHOICEP(arg))) {
      if (n_choices == 0)
	single = arg;
      else {
	int i = 0; while (i<n_choices) {
	  if (inchoicep(arg,choices[i])) i++;
	  else {
	    result=KNO_EMPTY;
	    goto early_exit;}}
	/* It's in every choice we've seen so far */
	if (KNO_VOIDP(single)) single=arg;
	else kno_decref(arg);
	free_choices(choices,n_choices,overflow);
	overflow=n_choices=0;}}
    else if (!(VOIDP(single))) {
      if ( (CHOICEP(arg)) ?
	   (inchoicep(single,(kno_choice)arg)) :
	   (KNO_EQUALP(arg,single)) ) {
	kno_decref(arg);}
      else {
	result = KNO_EMPTY;
	kno_decref(arg);
	goto early_exit;}}
    else {
      if (n_choices<max_choices)
	choices[n_choices++]=(kno_choice)arg;
      else {
	int new_max = max_choices*2;
	ssize_t new_size = sizeof(struct KNO_CHOICE *)*new_max;
	struct KNO_CHOICE **onheap = u8_malloc(new_size);
	memcpy(onheap,choices,n_choices*sizeof(struct KNO_CHOICE *));
	choices=onheap;
	max_choices=new_max;
	overflow=1;
	choices[n_choices++]=(kno_choice)arg;}}}
  if (KNO_VOIDP(single)) {
    if (n_choices>0) {
      lispval combined = kno_intersect_choices(choices,n_choices);
      free_choices(choices,n_choices,overflow);
      return combined;}
    else return KNO_EMPTY;}
  else return single;
 early_exit:
  free_choices(choices,n_choices,overflow);
  return result;
}

static lispval do_difference(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  lispval result = KNO_VOID;
  while (KNO_PAIRP(body)) {
    lispval expr = pop_arg(body);
    lispval arg = doeval(expr,env,stack,0);
    if (result == arg) {
      kno_decref(result); kno_decref(arg);
      return  KNO_EMPTY;}
    else if (KNO_ABORTED(arg)) {
      kno_decref(result);
      return arg;}
    else if (KNO_VOIDP(arg)) {
      kno_decref(result);
      return kno_err(kno_VoidArgument,"difference",NULL,expr);}
    else if (KNO_VOIDP(result)) {
      /* This is the initial state */
      result = arg;}
    else if (KNO_EMPTYP(arg)) {}
    else if (KNO_CHOICEP(result)) {
      if ( (KNO_CHOICEP(arg)) || (inchoicep(arg,(kno_choice)result)) ) {
	lispval new_result = kno_difference(result,arg);
	kno_decref(result); kno_decref(arg);
	if (KNO_EMPTYP(new_result)) return new_result;
	else result=new_result;}
      else NO_ELSE;}
    else if (CHOICEP(arg)) {
      if (inchoicep(result,(kno_choice)arg)) {
	/* result is in choice, so we go empty */
	kno_decref(result); kno_decref(arg);
	return KNO_EMPTY;}
      else NO_ELSE;}
    else if (KNO_EQUALP(result,arg)) {
      kno_decref(result); kno_decref(arg);
      return KNO_EMPTY;}
    else {
      /* Doesn't change result, keep going */
      kno_decref(arg);}}
  return result;
}

/* XREF opcodes */

static lispval xref_type_error(lispval x,lispval tag)
{
  if (VOIDP(tag))
    kno_seterr(kno_TypeError,"XREF_OPCODE","compound",x);
  else {
    u8_string buf=kno_lisp2string(tag);
    kno_seterr(kno_TypeError,"XREF_OPCODE",buf,x);
    u8_free(buf);}
  return KNO_ERROR_VALUE;
}

static lispval xref_op(struct KNO_COMPOUND *c,long long i,lispval tag,int free)
{
  if ((VOIDP(tag)) || ((c->typetag) == tag)) {
    if ((i>=0) && (i<c->compound_length)) {
      lispval *values = &(c->compound_0), value;
      if (c->compound_ismutable)
	u8_read_lock(&(c->compound_rwlock));
      value = values[i];
      kno_incref(value);
      if (c->compound_ismutable)
	u8_rw_unlock(&(c->compound_rwlock));
      if (free) kno_decref((lispval)c);
      return kno_simplify_choice(value);}
    else {
      kno_seterr(kno_RangeError,"xref",NULL,(lispval)c);
      if (free) kno_decref((lispval)c);
      return KNO_ERROR_VALUE;}}
  else {
    lispval err=xref_type_error((lispval)c,tag);
    if (free) kno_decref((lispval)c);
    return err;}
}

static lispval xref_opcode(lispval x,long long i,lispval tag)
{
  if (!(CONSP(x))) {
    if (EMPTYP(x))
      return EMPTY;
    else return xref_type_error(x,tag);}
  else if (KNO_COMPOUNDP(x))
    return xref_op((struct KNO_COMPOUND *)x,i,tag,1);
  else if (CHOICEP(x)) {
    lispval results = EMPTY;
    DO_CHOICES(c,x) {
      lispval r = xref_op((struct KNO_COMPOUND *)c,i,tag,0);
      if (KNO_ABORTED(r)) {
	KNO_STOP_DO_CHOICES;
	kno_decref(results);
	results = r;
	break;}
      else {CHOICE_ADD(results,r);}}
    /* Need to free this */
    kno_decref(x);
    return results;}
  else return kno_err(kno_TypeError,"xref",kno_lisp2string(tag),x);
}

static lispval xpred_opcode(lispval x,lispval tag)
{
  if (KNO_COMPOUNDP(x)) {
    int match = KNO_COMPOUND_TYPEP(x,tag);
    kno_decref(x);
    if (match)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_CHOICEP(x)) {
    int match = 0, nomatch = 0;
    KNO_DO_CHOICES(e,x) {
      if (KNO_COMPOUND_TYPEP(e,tag)) {
	match=1; if (nomatch) {
	  KNO_STOP_DO_CHOICES;
	  break;}}
      else {
	nomatch = 1;
	if (match) {
	  KNO_STOP_DO_CHOICES;
	  break;}}}
    kno_decref(x);
    if (match) {
      if (nomatch) {
	lispval tmpvec[2] = {KNO_FALSE,KNO_TRUE};
	return kno_init_choice(NULL,2,tmpvec,KNO_CHOICE_ISATOMIC);}
      else return KNO_TRUE;}
    else return KNO_FALSE;}
  else {
    kno_decref(x);
    return KNO_FALSE;}
}


/* Loop */

static lispval until_opcode(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval body = KNO_CDR(expr);
  lispval test_expr = KNO_CAR(body), loop_body = KNO_CDR(body);
  if (VOIDP(test_expr))
    return kno_err(kno_SyntaxError,"KNO_LOOP_OPCODE",NULL,expr);
  lispval test_val = kno_eval(test_expr,env,stack);
  if (ABORTED(test_val))
    return test_val;
  else while ( (FALSEP(test_val)) || (EMPTYP(test_val)) ) {
      lispval body_result = eval_body(loop_body,env,stack,"UNTIL",NULL,0);
      if (KNO_BROKEP(body_result))
	return KNO_FALSE;
      else if (KNO_ABORTED(body_result))
	return body_result;
      else kno_decref(body_result);
      test_val = kno_eval(test_expr,env,stack);
      if (KNO_ABORTED(test_val))
	return test_val;}
  return test_val;
}

/* Assignment */

#define CURRENT_VALUEP(x)				    \
  (! ((cur == KNO_DEFAULT_VALUE) || (cur == KNO_UNBOUND) || \
      (cur == VOID) || (cur == KNO_NULL)))

static lispval combine_values(lispval combiner,lispval cur,lispval value)
{
  int use_cur=((KNO_ABORTP(cur)) || (CURRENT_VALUEP(cur)));
  switch (combiner) {
  case VOID: case KNO_FALSE:
    return value;
  case KNO_TRUE: case KNO_DEFAULT_VALUE:
    if (use_cur)
      return cur;
    else return value;
  case KNO_PLUS_OPCODE:
    if (!(use_cur)) cur=KNO_FIXNUM_ZERO;
    if ( (FIXNUMP(value)) && (FIXNUMP(cur)) ) {
      long long ic=FIX2INT(cur), ip=FIX2INT(value);
      return KNO_MAKE_FIXNUM(ic+ip);}
    else return kno_plus(cur,value);
  case KNO_MINUS_OPCODE:
    if (!(use_cur)) cur=KNO_FIXNUM_ZERO;
    if ( (FIXNUMP(value)) && (FIXNUMP(cur)) ) {
      long long ic=FIX2INT(cur), im=FIX2INT(value);
      return KNO_MAKE_FIXNUM(ic-im);}
    else return kno_subtract(cur,value);
  default:
    return value;
  }
}
static lispval assignop(kno_stack stack,kno_lexenv env,
			lispval var,lispval expr,lispval combiner)
{
  if (KNO_LEXREFP(var)) {
    int up = KNO_LEXREF_UP(var);
    int across = KNO_LEXREF_ACROSS(var);
    kno_lexenv scan = ( (env->env_copy) ? (env->env_copy) : (env) );
    while ((up)&&(scan)) {
      kno_lexenv parent = scan->env_parent;
      if ((parent) && (parent->env_copy))
	scan = parent->env_copy;
      else scan = parent;
      up--;}
    if (USUALLY(scan!=NULL)) {
      lispval bindings = scan->env_bindings;
      if (USUALLY(SCHEMAPP(bindings))) {
	struct KNO_SCHEMAP *map = (struct KNO_SCHEMAP *)bindings;
	int map_len = map->schema_length;
	if (USUALLY( across < map_len )) {
	  lispval *values = map->table_values;
	  lispval cur	  = values[across];
	  if (KNO_XTABLE_BITP(map,KNO_SCHEMAP_STACK_VALUES)) {
	    kno_incref_vec(map->table_values,map_len);
	    KNO_XTABLE_SET_BIT(map,KNO_SCHEMAP_STACK_VALUES,0);}
	  if ( ( (combiner == KNO_TRUE) || (combiner == KNO_DEFAULT) ) &&
	       ( (CURRENT_VALUEP(cur)) || (KNO_ABORTED(cur)) ) ) {
	    if (KNO_ABORTED(cur))
	      return cur;
	    else return VOID;}
	  else {
	    lispval value = kno_eval(expr,env,stack);
	    /* This gnarly bit of code handles the case where
	       evaluating 'expr' changed the environment structure,
	       by, for instance, creating a lambda which made a
	       dynamic environment copy. If so, we need to change the
	       value of 'values' so that we store any resulting values
	       in the right place. */
	    if ( (scan->env_copy) && (scan->env_copy != scan) ) {
	      lispval new_bindings = scan->env_copy->env_bindings;
	      if ( (new_bindings != bindings) &&
		   (USUALLY(SCHEMAPP(bindings))) ) {
		struct KNO_SCHEMAP *new_map =
		  (struct KNO_SCHEMAP *) new_bindings;
		values = new_map->table_values;
		cur = values[across];}}
	    if (KNO_ABORTED(value))
	      return value;
	    else if ( (combiner == KNO_FALSE) || (combiner == VOID) ) {
	      /* Replace the currnet value */
	      values[across]=value;
	      kno_decref(cur);}
	    else if (combiner == KNO_UNION_OPCODE) {
	      if (KNO_ABORTED(value)) return value;
	      if ((cur==VOID)||(cur==KNO_UNBOUND)||(cur==EMPTY))
		values[across]=value;
	      else {CHOICE_ADD(values[across],value);}}
	    else {
	      lispval newv=combine_values(combiner,cur,value);
	      if (cur != newv) {
		values[across]=newv;
		kno_decref(cur);
		if (newv != value) kno_decref(value);}
	      else kno_decref(value);}
	    return VOID;}}}}
    u8_string lexref=u8_mkstring("up%d/across%d",up,across);
    lispval env_copy=(lispval)kno_copy_env(env);
    return kno_err("BadLexref","ASSIGN_OPCODE",lexref,env_copy);}
  else if ((PAIRP(var)) &&
	   (KNO_SYMBOLP(KNO_CAR(var))) &&
	   (TABLEP(KNO_CDR(var)))) {
    int rv=-1;
    lispval table=KNO_CDR(var), sym=KNO_CAR(var);
    lispval value = kno_eval(expr,env,stack);
    if (KNO_ABORTED(value))
      return value;
    else if ( (combiner == KNO_FALSE) || (combiner == VOID) ) {
      if (KNO_LEXENVP(table))
	rv=kno_assign_value(sym,value,(kno_lexenv)table);
      else rv=kno_store(table,sym,value);}
    else if (combiner == KNO_UNION_OPCODE) {
      if (KNO_LEXENVP(table))
	rv=kno_add_value(sym,value,(kno_lexenv)table);
      else rv=kno_add(table,sym,value);}
    else {
      lispval cur=kno_get(table,sym,KNO_UNBOUND);
      lispval newv=combine_values(combiner,cur,value);
      if (KNO_ABORTED(newv))
	rv=-1;
      else rv=kno_store(table,sym,newv);
      kno_decref(cur);
      kno_decref(newv);}
    kno_decref(value);
    if (rv<0) {
      kno_seterr("AssignFailed","ASSIGN_OPCODE",NULL,expr);
      return KNO_ERROR_VALUE;}
    else return VOID;}
  return kno_err(kno_SyntaxError,"ASSIGN_OPCODE",NULL,expr);
}

/* Binding */

static lispval bindop(lispval op,
		      kno_stack _stack,kno_lexenv env,
		      lispval vars,lispval inits,lispval body,
		      int tail)
{
  int i=0, n=VEC_LEN(vars);
  KNO_PUSH_EVAL(bind_stack,"bindop",op,env);
  INIT_STACK_SCHEMA(bind_stack,bound,env,n,VEC_DATA(vars));
  lispval *values=bound_bindings.table_values;
  lispval scan_inits = inits;
  kno_lexenv env_copy=NULL;
  while (i<n) values[i++]=KNO_UNBOUND;
  i=0; while (i<n) {
    lispval val_expr = pop_arg(scan_inits);
    lispval val = kno_eval(val_expr,bound,bind_stack);
    if (KNO_ABORTED(val)) {
      kno_pop_stack(bind_stack);
      return val;}
    else if (KNO_BAD_ARGP(val)) {
      lispval err = kno_bad_arg(val,"letrec_evalfn",val_expr);
      kno_pop_stack(bind_stack);
      return err;}
    else if ( (env_copy == NULL) && (bound->env_copy) ) {
      env_copy=bound->env_copy; bound=env_copy;
      values=((kno_schemap)(bound->env_bindings))->table_values;}
    values[i++]	 = val;}
  lispval result = eval_body(body,bound,bind_stack,"#BINDOP",NULL,tail);
  kno_pop_stack(bind_stack);
  return result;
}

static lispval vector_bindop(lispval op,
			     kno_stack _stack,kno_lexenv env,
			     lispval vars,lispval inits,lispval body,
			     int tail)
{
  int i=0, n=VEC_LEN(vars);
  KNO_PUSH_EVAL(bind_stack,"vector_bindop",op,env);
  INIT_STACK_SCHEMA(bind_stack,bound,env,n,VEC_DATA(vars));
  lispval *values=bound_bindings.table_values;
  lispval *exprs=VEC_DATA(inits);
  kno_lexenv env_copy=NULL;
  while (i<n) values[i++]=KNO_UNBOUND;
  i=0;  while (i<n) {
    lispval val_expr=exprs[i];
    lispval val=kno_eval(val_expr,bound,bind_stack);
    if (KNO_ABORTED(val)) {
      kno_pop_stack(bind_stack);
      return val;}
    else if (KNO_BAD_ARGP(val)) {
      lispval err = kno_bad_arg(val,"letrec_evalfn",val_expr);
      kno_pop_stack(bind_stack);
      return err;}
    if ( (env_copy == NULL) && (bound->env_copy) ) {
      env_copy=bound->env_copy; bound=env_copy;
      values=((kno_schemap)(bound->env_bindings))->table_values;}
    values[i++]=val;}
  lispval result = eval_body(body,bound,bind_stack,"#VECTORBIND",NULL,tail);
  kno_pop_stack(bind_stack);
  return result;
}

static void reset_env_op(kno_lexenv env)
{
  if ( (env->env_copy) && (env != env->env_copy) ) {
    lispval tofree = (lispval) env;
    env->env_copy=NULL;
    kno_decref(tofree);}
}

/* DOCALL: For non-iterating calls, handling tail calls and fast cprims */

static lispval docall(lispval fn,int n,kno_argvec args,kno_stack stack,
		      int tail,int free_args)
{
  if ( (KNO_TYPEP(fn,kno_cprim_type)) &&
       ( ((kno_function)fn)->fcn_profile == NULL) ) {
    struct KNO_CPRIM *prim = (kno_cprim) fn;
    if (free_args) {
      lispval result = cprim_call(prim->fcn_name,prim,n,args,stack);
      kno_decref_vec((lispval *)args,n);
      return result;}
    else return cprim_call(prim->fcn_name,prim,n,args,stack);}
  else if (KNO_TYPEP(fn,kno_lambda_type))
    return lambda_call(stack,(kno_lambda)fn,n,args,free_args,tail);
  else if (free_args) {
    lispval result = kno_dcall(stack,fn,n,args);
    kno_decref_vec((lispval *)args,n);
    return result;}
  else return kno_dcall(stack,fn,n,args);
}

/* Apply ops */

static lispval call_op(lispval fn_arg,int n,lispval exprs,
		       kno_lexenv env,kno_stack stack,int tail)
{
  lispval fn;
  if (KNO_FCNIDP(fn_arg)) fn = kno_fcnid_ref(fn_arg);
  else if (KNO_LEXREFP(fn_arg)) {
    fn = eval_lexref(fn_arg,env);
    if (KNO_CONSP(fn))
      kno_stackvec_push(&(stack->stack_refs),fn);
    else if (KNO_ABORTED(fn)) return fn;
    else NO_ELSE;}
  else fn = get_evalop(fn_arg,env,stack);
  int nd_call = 0, prune_call = 1, ambig_fn =0;
  if ( (KNO_FUNCTIONP(fn)) ) {
    kno_function f = (kno_function) fn;
    if (f->fcn_call & KNO_CALL_NDCALL) nd_call = 1;
    if (f->fcn_call & KNO_CALL_XPRUNE) prune_call = 0;}
  else if (KNO_APPLICABLEP(fn)) {
    prune_call = 1; nd_call = 0;}
  else if (KNO_CHOICEP(fn)) {
    prune_call = 0; nd_call = 0; ambig_fn = 1;
    KNO_ITER_CHOICES(scan,limit,fn);
    while (scan<limit) {
      lispval each = *scan++;
      if (!(KNO_APPLICABLEP(each))) {
	return kno_err(kno_NotAFunction,"call_op",NULL,each);}}}
  else if (KNO_ABORTED(fn)) return fn;
  else return kno_err(kno_NotAFunction,"call_op",NULL,fn);
  lispval args[n];
  int rv = eval_args(n,args,exprs,env,stack,prune_call);
  if (RARELY(rv<0)) {
    if (rv == -2) return kno_err(kno_TooManyArgs,"call_op",NULL,fn);
    return KNO_ERROR;}
  else if (rv == 0) return KNO_EMPTY;
  else if ( (ambig_fn) || ( (rv&KNO_AMBIG_ARGS) && (!(nd_call)) ) ) {
    lispval result = kno_call(stack,fn,n,args);
    kno_decref_vec(args,n);
    return result;}
  if (rv&KNO_QCHOICE_ARGS) unwrap_qchoices(n,args);
  return docall(fn,n,args,stack,tail,(rv&KNO_CONSED_ARGS));
}

/* Sequence opcodes */

static lispval elt_opcode(lispval arg1,lispval arg2)
{
  if ((KNO_SEQUENCEP(arg1)) && (KNO_INTP(arg2))) {
    lispval result;
    long long off = FIX2INT(arg2), len = kno_seq_length(arg1);
    if (off<0) off = len+off;
    result = kno_seq_elt(arg1,off);
    if (result == KNO_TYPE_ERROR)
      return kno_type_error(_("sequence"),"KNO_OPCODE_ELT",arg1);
    else if (result == KNO_RANGE_ERROR) {
      char buf[32];
      sprintf(buf,"%lld",off);
      return kno_err(kno_RangeError,"KNO_OPCODE_ELT",u8_strdup(buf),arg1);}
    else return result;}
  else if (!(KNO_SEQUENCEP(arg1)))
    return kno_type_error(_("sequence"),"KNO_OPCODE_ELT",arg1);
  else return kno_type_error(_("fixnum"),"KNO_OPCODE_ELT",arg2);
}

static lispval eltn_opcode(lispval arg1,int n,u8_context opname)
{
  if ( ( n == 0) && (KNO_PAIRP(arg1)) )
    return kno_incref(KNO_CAR(arg1));
  else if (VECTORP(arg1))
    if (VEC_LEN(arg1) > n)
      return kno_incref(VEC_REF(arg1,n));
    else return kno_err(kno_RangeError,opname,NULL,arg1);
  else if (COMPOUND_VECTORP(arg1))
    if (COMPOUND_VECLEN(arg1) > n)
      return kno_incref(XCOMPOUND_VEC_REF(arg1,n));
    else return kno_err(kno_RangeError,opname,NULL,arg1);
  else return kno_seq_elt(arg1,n);
}

static lispval pickone_opcode(lispval normal)
{
  int n = KNO_CHOICE_SIZE(normal);
  if (n) {
    lispval chosen;
    int i = u8_random(n);
    const lispval *data = KNO_CHOICE_DATA(normal);
    chosen = data[i]; kno_incref(chosen);
    return chosen;}
  else return EMPTY;
}

static lispval nd1_call(lispval opcode,lispval arg1)
{
  switch (opcode) {
  case KNO_AMBIGP_OPCODE:
    if (CHOICEP(arg1))
      return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_SINGLETONP_OPCODE:
    if (EMPTYP(arg1))
      return KNO_FALSE;
    else if (CHOICEP(arg1))
      return KNO_FALSE;
    else return KNO_TRUE;
  case KNO_FAILP_OPCODE:
    if (arg1==EMPTY)
      return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_EXISTSP_OPCODE:
    if (arg1==EMPTY)
      return KNO_FALSE;
    else return KNO_TRUE;
  case KNO_SINGLETON_OPCODE:
    if (CHOICEP(arg1))
      return EMPTY;
    else return kno_incref(arg1);
  case KNO_CAR_OPCODE: {
    if (EMPTYP(arg1))
      return arg1;
    else if (PAIRP(arg1))
      return kno_incref(KNO_CAR(arg1));
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      KNO_ITER_CHOICES(scan,limit,arg1);
      while (scan<limit) {
	lispval arg = *scan++;
	if (PAIRP(arg)) {CHOICE_ADD_INCREF(results,KNO_CAR(arg));}
	else { kno_decref(results);
	  return kno_type_error("pair","car_opcode",arg);}}
      return kno_simplify_choice(results);}
    else return kno_type_error("pair","car_opcode",arg1);}
  case KNO_CDR_OPCODE: {
    if (EMPTYP(arg1))
      return arg1;
    else if (PAIRP(arg1)) {
      lispval car = KNO_CDR(arg1);
      return kno_incref(car);}
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      KNO_ITER_CHOICES(scan,limit,arg1);
      while (scan<limit) {
	lispval arg = *scan++;
	if (PAIRP(arg)) {CHOICE_ADD_INCREF(results,KNO_CAR(arg));}
	else { kno_decref(results);
	  return kno_type_error("pair","cdr_opcode",arg);}}
      return kno_simplify_choice(results);}
    else return kno_type_error("pair","cdr_opcode",arg1);}
  case KNO_LENGTH_OPCODE:
    if (arg1==EMPTY) return EMPTY;
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1) {
	if (VECTORP(arg)) {
	  int len = VEC_LENGTH(arg);;
	  lispval add = KNO_INT(len);
	  CHOICE_ADD(results,add);}
	else if (KNO_SEQUENCEP(arg)) {
	  int len = kno_seq_length(arg);
	  lispval add = KNO_INT(len);
	  CHOICE_ADD(results,add);}
	else {
	  kno_decref(results);
	  return kno_type_error(_("sequence"),"LENGTH opcode",arg);}}
      return kno_simplify_choice(results);}
    else if (KNO_VECTORP(arg1)) {
      int len = VEC_LENGTH(arg1);
      return KNO_INT(len);}
    else if (KNO_PAIRP(arg1)) {
      int len = kno_list_length(arg1);
      return KNO_INT(len);}
    else if (KNO_SEQUENCEP(arg1)) {
      int len = kno_seq_length(arg1);
      return KNO_INT(len);}
    else return kno_type_error(_("sequence"),"LENGTH opcode",arg1);
  case KNO_QCHOICE_OPCODE:
    if (CHOICEP(arg1)) {
      kno_incref(arg1);
      return kno_init_qchoice(NULL,arg1);}
    else if (EMPTYP(arg1))
      return kno_init_qchoice(NULL,EMPTY);
    else return kno_incref(arg1);
  case KNO_CHOICE_SIZE_OPCODE:
    if (CHOICEP(arg1)) {
      int sz = KNO_CHOICE_SIZE(arg1);
      return KNO_INT(sz);}
    else if (EMPTYP(arg1))
      return KNO_INT(0);
    else return KNO_INT(1);
  case KNO_PICKONE_OPCODE:
    if (CHOICEP(arg1))
      return pickone_opcode(arg1);
    else return kno_incref(arg1);
  case KNO_IFEXISTS_OPCODE:
    if (EMPTYP(arg1))
      return VOID;
    else return kno_incref(arg1);
  case KNO_SOMETRUE_OPCODE:
    if ( (EMPTYP(arg1)) || (FALSEP(arg1)) )
      return KNO_FALSE;
    else return KNO_TRUE;
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval nd2_call(lispval opcode,lispval arg1,lispval arg2)
{
  lispval result = KNO_ERROR_VALUE;
  if (KNO_ABORTED(arg2))
    result = arg2;
  else if (VOIDP(arg2)) {
    result = kno_err(kno_VoidArgument,"OPCODE setop",NULL,opcode);}
  else switch (opcode) {
    case KNO_IDENTICAL_OPCODE:
      if (arg1==arg2)
	result = KNO_TRUE;
      else if (KNO_EQUAL(arg1,arg2))
	result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_OVERLAPS_OPCODE:
      if (arg1==arg2) {
	if (EMPTYP(arg1))
	  result = KNO_FALSE;
	else result = KNO_TRUE;}
      else if (kno_overlapp(arg1,arg2))
	result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_CONTAINSP_OPCODE:
      if (EMPTYP(arg1))
	result = KNO_TRUE;
      else if (EMPTYP(arg2))
	result = KNO_FALSE;
      else if (arg1 == arg2)
	result = KNO_TRUE;
      else if (kno_containsp(arg1,arg2))
	result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_CHOICEREF_OPCODE:
      if (!(FIXNUMP(arg2))) {
	kno_decref(arg1);
	result = kno_err(kno_SyntaxError,"choiceref_opcode",NULL,arg2);}
      else {
	int i = FIX2INT(arg2);
	if (i<0)
	  result = kno_err(kno_SyntaxError,"choiceref_opcode",NULL,arg2);
	else if ((i==0)&&(EMPTYP(arg1)))
	  result = VOID;
	else if (CHOICEP(arg1)) {
	  struct KNO_CHOICE *ch = (kno_choice)arg1;
	  if (i<ch->choice_size) {
	    const lispval *elts = KNO_XCHOICE_DATA(ch);
	    lispval elt = elts[i];
	    result = kno_incref(elt);}
	  else result = VOID;}
	else if (i==0) result=arg1;
	else result = VOID;}
    default:
      result = kno_err(_("Invalid opcode"),"numop_call",NULL,VOID);
    }
  return result;
}

static lispval d1_call_base(lispval opcode,lispval arg1)
{
  switch (opcode) {
  case KNO_FIRST_OPCODE:
    return eltn_opcode(arg1,0,"KNO_FIRST_OPCODE");
  case KNO_SECOND_OPCODE:
    return eltn_opcode(arg1,1,"KNO_SECOND_OPCODE");
  case KNO_THIRD_OPCODE:
    return eltn_opcode(arg1,2,"KNO_THIRD_OPCODE");
  case KNO_CADR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) return kno_incref(KNO_CAR(cdr));
    else return kno_err(kno_RangeError,"KNO_CADR",NULL,arg1);}
  case KNO_CDDR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) return kno_incref(KNO_CDR(cdr));
    else return kno_err(kno_RangeError,"KNO_CDDR",NULL,arg1);}
  case KNO_CADDR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) {
      lispval cddr = KNO_CDR(cdr);
      if (PAIRP(cddr)) return kno_incref(KNO_CAR(cddr));
      else return kno_err(kno_RangeError,"KNO_CADDR",NULL,arg1);}
    else return kno_err(kno_RangeError,"KNO_CADDR",NULL,arg1);}
  case KNO_CDDDR_OPCODE: {
    lispval cdr = KNO_CDR(arg1);
    if (PAIRP(cdr)) {
      lispval cddr = KNO_CDR(cdr);
      if (PAIRP(cddr))
	return kno_incref(KNO_CDR(cddr));
      else return kno_err(kno_RangeError,"KNO_CDDR",NULL,arg1);}
    else return kno_err(kno_RangeError,"KNO_CDDDR",NULL,arg1);}
  case KNO_TONUMBER_OPCODE:
    if (FIXNUMP(arg1)) return arg1;
    else if (NUMBERP(arg1)) return kno_incref(arg1);
    else if (STRINGP(arg1))
      return kno_string2number(CSTRING(arg1),10);
    else if (KNO_CHARACTERP(arg1))
      return KNO_INT(KNO_CHARCODE(arg1));
    else return kno_type_error(_("number|string"),"opcode ->number",arg1);
  case KNO_ELTS_OPCODE: {
    int n_elts = -1;
    lispval *elts = kno_seq_elts(arg1,&n_elts);
    if (elts == NULL) {
      if (n_elts == 0)
	return KNO_EMPTY_CHOICE;
      else return KNO_ERROR;}
    else return kno_make_choice(n_elts,elts,
				( KNO_CHOICE_COMPRESS |
				  KNO_CHOICE_DOSORT   |
				  KNO_CHOICE_REALLOC  |
				  KNO_CHOICE_FREEDATA ));}
  case KNO_GETKEYS_OPCODE:
    return kno_getkeys(arg1);
  case KNO_GETVALUES_OPCODE:
    return kno_getvalues(arg1);
  case KNO_GETASSOCS_OPCODE:
    return kno_getassocs(arg1);
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval d1_call(lispval opcode,lispval arg1)
{
  if (KNO_ABORTED(arg1)) return arg1;
  else if (KNO_EMPTYP(arg1)) return arg1;
  else if (KNO_CHOICEP(arg1)) {
    lispval results = KNO_EMPTY;
    ITER_CHOICES(scan,limit,arg1);
    while (scan<limit) {
      lispval arg = *scan++;
      lispval r = d1_call_base(opcode,arg);
      if (KNO_ABORTED(r)) {
	kno_decref(results); results=r;
	break;}
      else {CHOICE_ADD(results,r);}}
    return results;}
  else return d1_call_base(opcode,arg1);
}

static lispval d2_call_base(lispval opcode,lispval arg1,lispval arg2)
{
  switch (opcode) {
  case KNO_EQ_OPCODE:
    if (arg1==arg2) return KNO_TRUE; else return KNO_FALSE;
  case KNO_EQV_OPCODE: {
    if (arg1==arg2) return KNO_TRUE;
    else if ((NUMBERP(arg1)) && (NUMBERP(arg2)))
      if (kno_numcompare(arg1,arg2)==0)
	return KNO_TRUE; else return KNO_FALSE;
    else return KNO_FALSE;}
  case KNO_EQUAL_OPCODE: {
    if ((ATOMICP(arg1)) && (ATOMICP(arg2)))
      if (arg1==arg2) return KNO_TRUE; else return KNO_FALSE;
    else if (KNO_EQUAL(arg1,arg2)) return KNO_TRUE;
    else return KNO_FALSE;}
  case KNO_ELT_OPCODE:
    return elt_opcode(arg1,arg2);
  case KNO_CONS_OPCODE:
    return kno_init_pair(NULL,kno_incref(arg1),kno_incref(arg2));
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval d2_call(lispval opcode,lispval arg1,lispval arg2)
{
  if (KNO_ABORTED(arg1)) return arg1;
  else if (KNO_ABORTED(arg2)) return arg2;
  else if ( (KNO_EMPTYP(arg1)) || (KNO_EMPTYP(arg2)) )
    return KNO_EMPTY;
  else if ( (KNO_CHOICEP(arg1)) || (KNO_CHOICEP(arg2)) ) {
    lispval results = KNO_EMPTY;
    ITER_CHOICES(scan1,limit1,arg1);
    ITER_CHOICES(start2,limit2,arg1);
    while (scan1<limit1) {
      lispval arg1 = *scan1++;
      const lispval *scan2 = start2;
      while (scan2<limit2) {
	const lispval arg2 = *scan2++;
	lispval r = d2_call_base(opcode,arg1,arg2);
	if (KNO_ABORTED(r)) {
	  kno_decref(results); results=r;
	  break;}
	else {CHOICE_ADD(results,r);}}
      if (KNO_ABORTED(results)) break;}
    return results;}
  else return d2_call_base(opcode,arg1,arg2);}


/* Numeric ops */

typedef int (reducer)(lispval *result,lispval cur,lispval arg);

#define NOT_A_NUMBERP(x) (RARELY(!(KNO_NUMBERP(x))))

static lispval doplus(lispval x,lispval y)
{
  if ( (KNO_FIX_ZEROP(y)) && (KNO_NUMBERP(x)) )
    return kno_incref(x);
  else if ( (KNO_FIX_ZEROP(x)) && (KNO_NUMBERP(y)) )
    return kno_incref(y);
  else if ( (KNO_FIXNUMP(x)) && (KNO_FIXNUMP(y)) ) {
	long long x_int = KNO_FIX2INT(x);
	long long y_int = KNO_FIX2INT(y);
	long long result_int = x_int+y_int;
	return KNO_INT(result_int);}
  else if ( (KNO_FLONUMP(x)) && (KNO_FLONUMP(y)) ) {
    double x_float = KNO_FLONUM(x);
    double y_float = KNO_FLONUM(y);
    double result_float = x_float+y_float;
    return kno_make_flonum(result_float);}
  else if (RARELY(NOT_A_NUMBERP(y)))
    return kno_type_error("number","plus",x);
  else if (RARELY(NOT_A_NUMBERP(x)))
    return kno_type_error("number","plus",y);
  else return call2(kno_plus,x,y);
}

static lispval dosubtract(lispval x,lispval y)
{
  if ( (KNO_FIX_ZEROP(y)) && (USUALLY(KNO_NUMBERP(x))) )
    return kno_incref(x);
  else if ( (KNO_FIXNUMP(x)) && (KNO_FIXNUMP(y)) ) {
	long long x_int = KNO_FIX2INT(x);
	long long y_int = KNO_FIX2INT(y);
	long long result_int = x_int-y_int;
	return KNO_INT(result_int);}
  else if ( (KNO_FLONUMP(x)) && (KNO_FLONUMP(y)) ) {
    double x_float = KNO_FLONUM(x);
    double y_float = KNO_FLONUM(y);
    double result_float = x_float-y_float;
    return kno_make_flonum(result_float);}
  else if (RARELY(NOT_A_NUMBERP(y)))
    return kno_type_error("number","minus",x);
  else if (RARELY(NOT_A_NUMBERP(x)))
    return kno_type_error("number","minus",y);
  else return call2(kno_subtract,x,y);
}

static lispval domultiply(lispval x,lispval y)
{
  if ( (KNO_FIX_ZEROP(y)) && (USUALLY(KNO_NUMBERP(x))) )
    return KNO_ZERO;
  else if ( (KNO_FIX_ZEROP(x)) && (USUALLY(KNO_NUMBERP(y))) )
    return KNO_ZERO;
  else if ( (KNO_FIX_ONEP(y)) && (USUALLY(KNO_NUMBERP(x))) )
    return kno_incref(x);
  else if ( (KNO_FIX_ONEP(x)) && (USUALLY(KNO_NUMBERP(y))) )
    return kno_incref(y);
  else if ( (KNO_FIXNUMP(x)) && (KNO_FIXNUMP(y)) ) {
    long long ix = FIX2INT(x), iy = FIX2INT(y);
    long long q = ((iy>0)?(KNO_MAX_FIXNUM/iy):(KNO_MIN_FIXNUM/iy));
    if (!((ix>0)?(ix>q):((-ix)>q))) {
      long long result = ix*iy;
      return KNO_INT(result);}
    else return kno_plus(x,y);}
  else if ( (KNO_FLONUMP(x)) && (KNO_FLONUMP(y)) ) {
	double x_float = KNO_FLONUM(x);
	double y_float = KNO_FLONUM(y);
	double result_float = x_float*y_float;
	return kno_make_flonum(result_float);}
  else if (RARELY(NOT_A_NUMBERP(y)))
    return kno_type_error("number","plus",x);
  else if (RARELY(NOT_A_NUMBERP(x)))
    return kno_type_error("number","plus",y);
  else return call2(kno_multiply,x,y);
}

static lispval dofloatdiv(lispval nval,lispval dval)
{
  if ( (USUALLY(KNO_NUMBERP(nval))) && (USUALLY(KNO_NUMBERP(dval))) ) {
    double n = (FLONUMP(nval)) ? (KNO_FLONUM(nval)) : (kno_todouble(nval));
    double d = (FLONUMP(dval)) ? (KNO_FLONUM(dval)) : (kno_todouble(dval));
    double ratio = n/d;
    return kno_make_flonum(ratio);}
  else if (!(KNO_NUMBERP(nval)))
    return kno_type_error("number","dofloatdiv/numerator",nval);
  else return kno_type_error("number","dofloatdiv/numerator",dval);
}

static int docompare(lispval opcode,lispval arg1,lispval arg2)
{
  if ( (KNO_FIXNUMP(arg1)) && (KNO_FIXNUMP(arg2)) ) {
    long long v1=KNO_FIX2INT(arg1), v2=KNO_FIX2INT(arg2);
    switch (opcode) {
    case KNO_NUMEQ_OPCODE: return (v1==v2);
    case KNO_GT_OPCODE: return (v1>v2);
    case KNO_GTE_OPCODE: return (v1>=v2);
    case KNO_LTE_OPCODE: return (v1<=v2);
    case KNO_LT_OPCODE: return (v1<v2);
    default:
      kno_seterr("BadOpcode",opname(opcode),"docompare",KNO_VOID);
      return -1;}}
  else if ( (KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2)) ) {
    double v1=KNO_FLONUM(arg1), v2=KNO_FLONUM(arg2);
    switch (opcode) {
    case KNO_NUMEQ_OPCODE: return (v1==v2);
    case KNO_GT_OPCODE: return (v1>v2);
    case KNO_GTE_OPCODE: return (v1>=v2);
    case KNO_LTE_OPCODE: return (v1<=v2);
    case KNO_LT_OPCODE: return (v1<v2);
    default:
      kno_seterr("BadOpcode",opname(opcode),"docompare",KNO_VOID);
      return -1;}}
  else if ( (KNO_NUMBERP(arg1)) && (KNO_NUMBERP(arg2)) ) {
    int comparison = kno_numcompare(arg1,arg2);
    switch (opcode) {
    case KNO_NUMEQ_OPCODE: return (comparison==0);
    case KNO_GT_OPCODE: return (comparison>0);
    case KNO_GTE_OPCODE: return (comparison>=0);
    case KNO_LTE_OPCODE: return (comparison<=0);
    case KNO_LT_OPCODE: return (comparison<0);
    default:
      kno_seterr("BadOpcode",opname(opcode),"docompare",KNO_VOID);
      return -1;}}
  else if (!(KNO_NUMBERP(arg1))) {
    kno_seterr("number",opname(opcode),"docompare",arg1);
    return -1;}
  else {
    kno_seterr("number",opname(opcode),"docompare",arg2);
    return -1;}
}

static lispval handle_numeric_opcode(lispval opcode,lispval arg1,lispval arg2)
{
  switch (opcode) {
  case KNO_PLUS_OPCODE:
    return doplus(arg1,arg2);
  case KNO_MINUS_OPCODE:
    return dosubtract(arg1,arg2);
  case KNO_TIMES_OPCODE:
    return domultiply(arg1,arg2);
  case KNO_DIV_OPCODE:
    return kno_divide(arg1,arg2);
  case KNO_FLODIV_OPCODE:
    return dofloatdiv(arg1,arg2);
  case KNO_NUMEQ_OPCODE:
  case KNO_GT_OPCODE: case KNO_GTE_OPCODE:
  case KNO_LTE_OPCODE: case KNO_LT_OPCODE: {
    int result = docompare(opcode,arg1,arg2);
    if (result<0) return KNO_ERROR;
    else if (result) return KNO_TRUE;
    else return KNO_FALSE;}
  default:
    return kno_err(kno_BadOpcode,"handle_numeric_opcode",NULL,opcode);
  }
}

static lispval handle_numeric_opcode_nd(lispval opcode,lispval arg1,lispval arg2)
{
  lispval results = KNO_EMPTY;
  ITER_CHOICES(scan1,limit1,arg1);
  ITER_CHOICES(start2,limit2,arg2);
  while (scan1<limit1) {
    lispval v1 = *scan1++;
    if (RARELY(!(NUMBERP(v1)))) {
      kno_decref(results);
      results = kno_err(kno_NotANumber,opname(opcode),NULL,v1);
      break;}
    const lispval *scan2 = start2; while (scan2<limit2) {
      lispval v2 = *scan2++;
      if (RARELY(!(NUMBERP(v2)))) {
	kno_decref(results);
	results = kno_err(kno_NotANumber,opname(opcode),NULL,v1);
	break;}
      lispval r = handle_numeric_opcode(opcode,v1,v2);
      if (KNO_ABORTED(r)) {
	kno_decref(results);
	results = r;
	break;}}
    if (KNO_ABORTED(results)) break;}
  return kno_simplify_choice(results);
}

/* Table ops */

static lispval handle_table_result(int rv)
{
  if (RARELY(rv<0))
    return KNO_ERROR;
  else if (rv)
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval failed_table_op(lispval op,u8_string name,lispval value)
{
  if (KNO_EMPTYP(value)) switch (op) {
    case KNO_ASSERT_OPCODE: case KNO_ADD_OPCODE:
    case KNO_RETRACT_OPCODE: case KNO_DROP_OPCODE:
      return KNO_VOID;
    case KNO_TEST_OPCODE: case KNO_PRIMTEST_OPCODE:
      return KNO_FALSE;
    default:
      return KNO_EMPTY;}
  else if (KNO_ABORTED(value)) return value;
  else if (KNO_VOIDP(value))
    return kno_err(kno_VoidArgument,kno_opcode_name(op),name,KNO_VOID);
  else return kno_err(kno_TypeError,kno_opcode_name(op),name,value);
}

static lispval handle_table_opcode
(lispval opcode,lispval subject,lispval slotid,lispval value)
{
  if ( (EMPTYP(subject)) || (EMPTYP(slotid)) ) {
    switch (opcode) {
    case KNO_GET_OPCODE: case KNO_PRIMGET_OPCODE:
      return KNO_EMPTY_CHOICE;
    case KNO_TEST_OPCODE: case KNO_PRIMTEST_OPCODE:
      return KNO_FALSE;
    case KNO_ASSERT_OPCODE: case KNO_ADD_OPCODE:
    case KNO_RETRACT_OPCODE: case KNO_DROP_OPCODE:
      return KNO_VOID;}}
  if (KNO_EMPTYP(value)) {
    switch (opcode) {
    case KNO_ASSERT_OPCODE: case KNO_ADD_OPCODE:
    case KNO_RETRACT_OPCODE: case KNO_DROP_OPCODE:
      return KNO_VOID;
    case KNO_TEST_OPCODE: case KNO_PRIMTEST_OPCODE:
      return KNO_FALSE;}}
  switch (opcode) {
  case KNO_GET_OPCODE:
    return kno_fget(subject,slotid);
  case KNO_PRIMGET_OPCODE: {
    if (KNO_VOIDP(value)) value = KNO_EMPTY_CHOICE;
    return kno_get(subject,slotid,value);}
  case KNO_TEST_OPCODE:
    return kno_ftest(subject,slotid,value);
  case KNO_PRIMTEST_OPCODE:
    return handle_table_result(kno_test(subject,slotid,value));
  case KNO_ADD_OPCODE:
    return handle_table_result(kno_add(subject,slotid,value));
  case KNO_DROP_OPCODE:
    return handle_table_result(kno_drop(subject,slotid,value));
  case KNO_STORE_OPCODE:
    return handle_table_result(kno_store(subject,slotid,value));
  case KNO_ASSERT_OPCODE:
    return handle_table_result(kno_assert(subject,slotid,value));
  case KNO_RETRACT_OPCODE:
    return handle_table_result(kno_retract(subject,slotid,value));
  default:
    return kno_err(kno_BadOpcode,"handle_table_opcode",NULL,opcode);}
}

/* Opcode dispatch */

static lispval isa_op(lispval args,kno_lexenv env,kno_stack stack,int require);

static lispval handle_special_opcode(lispval opcode,lispval args,lispval expr,
				     kno_lexenv env,
				     kno_stack _stack,
				     int tail)
{
  KNO_STACK_SET_TAIL(_stack,tail);
  switch (opcode) {
  case KNO_BRANCH_OPCODE: {
    lispval test_expr = pop_arg(args);
    if (VOIDP(test_expr))
      return kno_err(kno_SyntaxError,opname(opcode),NULL,expr);
    lispval test_val = doeval(test_expr,env,_stack,0);
    if (KNO_ABORTED(test_val))
      return test_val;
    else if (KNO_FALSEP(test_val)) { /* (  || (KNO_EMPTYP(test_val)) ) */
      lispval else_expr = KNO_CDR(args);
      if (KNO_EMPTY_LISTP(else_expr))
	return KNO_VOID;
      else return doeval(else_expr,env,_stack,tail);}
    else if (KNO_EMPTYP(test_val)) return KNO_EMPTY;
    else {
      lispval then_expr = KNO_CAR(args);
      kno_decref(test_val);
      return doeval(then_expr,env,_stack,tail);}}
  case KNO_NOT_OPCODE: {
    lispval arg_val = kno_eval(args,env,_stack);
    if (FALSEP(arg_val))
      return KNO_TRUE;
    else {
      kno_decref(arg_val);
      return KNO_FALSE;}}
  case KNO_UNTIL_OPCODE:
    return until_opcode(expr,env,_stack);
  case KNO_BEGIN_OPCODE:
    return eval_body(KNO_CDR(expr),env,_stack,"#BEGINOP",NULL,tail);
  case KNO_QUOTE_OPCODE:
    return kno_incref(args);
  case KNO_ASSIGN_OPCODE: {
    lispval var = pop_arg(args);
    lispval combiner = KNO_CAR(args);
    lispval val_expr = KNO_CDR(args);
    return assignop(_stack,env,var,val_expr,combiner);}
  case KNO_SYMREF_OPCODE: {
    lispval refenv=pop_arg(args);
    lispval sym=pop_arg(args);
    if (KNO_RARELY(!(KNO_SYMBOLP(sym))))
      return kno_err(kno_SyntaxError,"KNO_SYMREF_OPCODE/badsym",NULL,expr);
    if (HASHTABLEP(refenv))
      return kno_hashtable_get((kno_hashtable)refenv,sym,KNO_UNBOUND);
    else if (KNO_LEXENVP(refenv))
      return symeval(sym,(kno_lexenv)refenv);
    else if (TABLEP(refenv))
      return kno_get(refenv,sym,KNO_UNBOUND);
    else return kno_err(kno_SyntaxError,"KNO_SYMREF_OPCODE/badenv",NULL,expr);}
  case KNO_BIND_OPCODE: {
    lispval vars=pop_arg(args);
    lispval inits=pop_arg(args);
    lispval body=args;
    if (!(KNO_VECTORP(vars)))
      return kno_err(kno_SyntaxError,"BINDOP",NULL,expr);
    else if (KNO_VECTORP(inits))
      return vector_bindop(opcode,_stack,env,vars,inits,body,tail);
    else return bindop(opcode,_stack,env,vars,inits,body,tail);}
  case KNO_VOID_OPCODE: {
    return VOID;}
  case KNO_TRY_OPCODE:
    return try_op(args,env,_stack,tail);
  case KNO_AND_OPCODE:
    return and_op(args,env,_stack,tail);
  case KNO_OR_OPCODE:
    return or_op(args,env,_stack,tail);
  case KNO_SOURCEREF_OPCODE: {
    if (KNO_PAIRP(args))
      return kno_eval(KNO_CDR(args),env,_stack);
    else return kno_seterr(kno_SyntaxError,"KNO_XREF_OPCODE",NULL,expr);}
  case KNO_RESET_ENV_OPCODE: {
    reset_env_op(env);
    return VOID;}
  case KNO_XREF_OPCODE: {
    lispval off_arg = pop_arg(args);
    lispval type_arg = pop_arg(args);
    lispval obj_expr = pop_arg(args);
    if ( (FIXNUMP(off_arg)) && (! (VOIDP(obj_expr)) ) ) {
      lispval obj_arg = kno_eval(obj_expr,env,_stack);
      return xref_opcode(kno_simplify_choice(obj_arg),
			 FIX2INT(off_arg),
			 type_arg);}
    kno_seterr(kno_SyntaxError,"KNO_XREF_OPCODE",NULL,expr);
    return KNO_ERROR_VALUE;}

  case KNO_XPRED_OPCODE: {
    lispval type_arg = pop_arg(args);
    lispval obj_expr = pop_arg(args);
    if (KNO_RARELY(VOIDP(obj_expr))) {
      kno_seterr(kno_SyntaxError,"KNO_XREF_OPCODE",NULL,expr);
      return KNO_ERROR_VALUE;}
    lispval obj_arg = kno_eval(obj_expr,env,_stack);
    if (KNO_ABORTED(obj_arg)) return obj_arg;
    else if (KNO_EMPTYP(obj_arg)) return KNO_EMPTY;
    else if ( (KNO_COMPOUNDP(obj_arg)) || (KNO_AMBIGP(obj_arg)) )
      return xpred_opcode(kno_simplify_choice(obj_arg),type_arg);
    else {
      kno_decref(obj_arg);
      return KNO_FALSE;}}
  case KNO_BREAK_OPCODE:
    return KNO_BREAK;

  case KNO_UNION_OPCODE:
    return do_union(args,env,_stack,0);
  case KNO_INTERSECT_OPCODE:
    return do_intersection(args,env,_stack,0);
  case KNO_DIFFERENCE_OPCODE:
    return do_difference(args,env,_stack,0);

  case KNO_EVALFN_OPCODE: {
    lispval evalfn = KNO_CAR(args);
    lispval expr   = KNO_CDR(args);
    return call_evalfn(evalfn,expr,env,_stack,tail);}

#if 0
  case KNO_APPLY_OPCODE: {
    int len = 0;
    lispval fn = KNO_CAR(args), scan = KNO_CDR(args);
    if (KNO_PAIRP(scan)) {
      lispval head = KNO_VOID, *tail = &head;
      while (KNO_PAIRP(scan)) {
	lispval expr = pop_arg(scan);
	lispval value = eval_arg(expr,env,_stack);
	if (KNO_ABORTED(value)) {
	  kno_decref(head);
	  return value;}
	else if ( (KNO_EMPTYP(value)) && (prune) ) {
	  kno_decref(head);
	  return value;}
	else if (scan==KNO_EMPTY_LIST) {
	  if (KNO_EMPTY_LISTP(value)) {}
	  else if (KNO_PAIRP(value)) {
	    len += kno_list_length(value);
	    *tail = value;}
	  else if (KNO_VECTORP(value)) {
	    lispval *elts = VEC_ELTS(value), *limit = elts+VEC_LEN(value);
	    while (elts<limit) {
	      lispval elt = *elts;
	      lispval next = kno_init_pair(NULL,value,KNO_EMPTY_LIST);
	      *tail = next;
	      struct KNO_PAIR *p = (kno_pair)next;
	      *tail = &(p->cdr);
	      *elts++=KNO_FALSE;}
	    len += VEC_LEN(value);
	    kno_decref(value);}
	  else {
	    lispval err = kno_type_error("sequence","apply(tail)",NULL,value);
	    kno_decref(head);
	    return err;}}
	else {
	  lispval next = kno_init_pair(NULL,value,KNO_EMPTY_LIST);
	  *tail = next; len++;
	  struct KNO_PAIR *p = (kno_pair)next;
	  *tail = &(p->cdr);}}
      lispval argvec[len], *write=argvec, *limit=scan+len;
      scan=head; while (write<limit) {
	lispval val = KNO_CAR(scan); pop_arg(scan);
	*write++=val;}
      lispval result = docall(fn,n,(kno_argvec)argvec,_stack,tail,0);
      kno_decref(head);
      return result;}
    else return kno_err("OpcodeError",opname(opcode),NULL,expr);}
#else
  case KNO_APPLY_OPCODE:
    return kno_err("OpcodeError",opname(opcode),NULL,expr);
#endif

  case KNO_ISA_OPCODE: return isa_op(args,env,_stack,0);
  case KNO_ISA_ALL_OPCODE: return isa_op(args,env,_stack,1);
  case KNO_ISA_ANY_OPCODE: return isa_op(args,env,_stack,-1);

  case KNO_PICK_TYPE_OPCODE: {
    lispval type = KNO_CAR(args);
    lispval expr = KNO_CDR(args);
    lispval candidates = kno_eval(expr,env,_stack);
    if (KNO_CHOICEP(candidates)) {
      lispval results = KNO_EMPTY;
      KNO_ITER_CHOICES(scan,limit,candidates);
      while (scan<limit) {
	lispval cand = *scan++;
	if (KNO_CHECKTYPE(cand,type)) {
	  CHOICE_ADD_INCREF(results,cand);}}
      kno_decref(candidates);
      return kno_simplify_choice(results);}
    else if (KNO_ABORTED(candidates))
      return candidates;
    else if (KNO_CHECKTYPE(candidates,type))
      return candidates;
    else {
      kno_decref(candidates);
      return KNO_EMPTY;}}

  case KNO_SKIP_TYPE_OPCODE: {
    lispval type = KNO_CAR(args);
    lispval expr = KNO_CDR(args);
    lispval candidates = kno_eval(expr,env,_stack);
    if (KNO_CHOICEP(candidates)) {
      lispval results = KNO_EMPTY;
      KNO_ITER_CHOICES(scan,limit,candidates);
      while (scan<limit) {
	lispval cand = *scan++;
	if (!(KNO_CHECKTYPE(cand,type))) {
	  CHOICE_ADD_INCREF(results,cand);}}
      kno_decref(candidates);
      return kno_simplify_choice(results);}
    else if (KNO_ABORTED(candidates))
      return candidates;
    else if (!(KNO_CHECKTYPE(candidates,type)))
      return candidates;
    else {
      kno_decref(candidates);
      return KNO_EMPTY;}}

  case KNO_CHECK_TYPE_OPCODE: {
    lispval type = KNO_CAR(args);
    lispval expr = KNO_CDR(args);
    lispval candidates = kno_eval(expr,env,_stack);
    if (KNO_CHOICEP(candidates)) {
      KNO_ITER_CHOICES(scan,limit,candidates);
      while (scan<limit) {
	lispval cand = *scan++;
	if (!(KNO_CHECKTYPE(cand,type))) {
	  u8_byte buf[50];
	  kno_seterr(kno_TypeError,"CHECK_TYPE_OP",
		     u8_bprintf(buf,"%q",cand),
		     cand);
	  kno_decref(candidates);
	  return KNO_ERROR;}}
      return candidates;}
    else if (KNO_ABORTED(candidates))
      return candidates;
    else if (KNO_CHECKTYPE(candidates,type))
      return candidates;
    else {
      u8_byte buf[50];
      kno_seterr(kno_TypeError,"checktype",
		 u8_bprintf(buf,"%q",type),
		 candidates);
      kno_decref(candidates);
      return KNO_ERROR;}}

  default:
    return kno_err("BadOpcode","handle_special_opcode",NULL,expr);
  }
  return kno_err("CorruptedQCode","quote_opcode",NULL,expr);
}

/* ISA type checking ops */

  static lispval isa_op(lispval args,kno_lexenv env,kno_stack stack,int require)
{
    lispval type = KNO_CAR(args);
    lispval expr = KNO_CDR(args);
    lispval obj = kno_eval(expr,env,stack);
    if (KNO_ABORTED(obj)) return obj;
    int rv;
    if ( (require==0) || (!(CHOICEP(obj))) )
      rv =KNO_CHECKTYPE(obj,type);
    else if (require>1) {
      ITER_CHOICES(scan,limit,obj);
      rv = 1;
      while (scan<limit) {
	lispval elt = *scan++;
	if (!(KNO_CHECKTYPE(elt,type))) {
	  rv=0; break;}}}
    else {
      ITER_CHOICES(scan,limit,obj);
      rv = 0;
      while (scan<limit) {
	lispval elt = *scan++;
	if (!(KNO_CHECKTYPE(elt,type))) {
	  rv=1; break;}}}
    kno_decref(obj);
    if (rv)
      return KNO_TRUE;
    else return KNO_FALSE;
}

/* CALL ops */

static lispval handle_call_op(lispval opcode,lispval args,lispval expr,
			      kno_lexenv env,
			      kno_stack _stack,
			      int tail)
{
  int n_args = KNO_CALL_OP_ARITY(opcode);
  if (n_args>=0x10) {
    lispval given_arity = pop_arg(args);
    int arity = (KNO_FIXNUMP(given_arity)) ? (KNO_FIX2INT(given_arity)) : (-1);
    if (arity<0)
      return kno_err("BadOpcode","handle_call_op",kno_opcode_name(opcode),expr);
    else n_args = arity;}
  lispval fn = pop_arg(args);
  return call_op(fn,n_args,args,env,_stack,tail);
}


/* VM main function */

lispval vm_eval(lispval op,lispval expr,
		kno_lexenv env,kno_stack stack,
		int tail)
{
  if (RARELY(!(KNO_OPCODEP(op))))
    return lisp_eval(op,expr,env,stack,tail);
  lispval payload = KNO_CDR(expr), source = expr;
  while (op == KNO_SOURCEREF_OPCODE) {
    source = KNO_CAR(payload);
    expr = KNO_CDR(payload);
    if (KNO_PAIRP(expr)) {
      op = KNO_CAR(expr);
      payload = KNO_CDR(expr);}
    else return doeval(expr,env,stack,0);}
  if (USUALLY(KNO_OPCODEP(op))) {
    int opcode_class = KNO_OPCODE_CLASS(op);
    lispval old_source = stack->eval_source;
    lispval result = KNO_VOID;
    lispval arg1 = VOID, arg2 = VOID, arg3 = VOID;
    stack->eval_source = source;
    switch (opcode_class) {
    case KNO_SPECIAL_OPCODE_CLASS: {
      result = handle_special_opcode(op,payload,expr,env,stack,tail);
      break;}
    case KNO_FNCALL_OPCODE_CLASS: {
      result = handle_call_op(op,payload,expr,env,stack,tail);
      break;}
    case KNO_ND1_OPCODE_CLASS: {
      arg1 = eval_arg(payload,env,stack);
      if (KNO_ABORTED(arg1))
	result = arg1;
      else if (KNO_CONSP(arg1))
	result = nd1_call(op,arg1);
      else result = nd1_call(op,arg1);
      break;}
    case KNO_ND2_OPCODE_CLASS: {
      if (PAIRP(payload)) {
	arg1 = eval_arg(KNO_CAR(payload),env,stack);
	arg2 = eval_arg(KNO_CDR(payload),env,stack);
	if (KNO_ABORTED(arg1))
	  result = arg1;
	else if (KNO_ABORTED(arg2))
	  result = arg2;
	else if ( (KNO_CONSP(arg1)) || (KNO_CONSP(arg2)) )
	  result = nd2_call(op,arg1,arg2);
	else result = nd2_call(op,arg1,arg2);}
      else result = kno_err("OpcodeError",opname(op),NULL,expr);
      break;}
    case KNO_D1_OPCODE_CLASS: {
      arg1 = eval_arg(payload,env,stack);
      if (KNO_ABORTED(arg1))
	result = arg1;
      else if (KNO_EMPTYP(arg1))
	result = KNO_EMPTY;
      else if (KNO_CONSP(arg1)) {
	if (CHOICEP(arg1)) {
	  result = KNO_EMPTY;
	  ITER_CHOICES(scan,limit,arg1);
	  while (scan<limit) {
	    lispval arg = *scan++;
	    lispval r = d1_call(op,arg);
	    if (KNO_ABORTED(r)) {
	      kno_decref(result);
	      result = r;
	      break;}
	    else {KNO_ADD_TO_CHOICE(result,r);}}}
	result = d1_call(op,arg1);}
      else result = d1_call(op,arg1);
      break;}
    case KNO_D2_OPCODE_CLASS: {
      if (PAIRP(payload)) {
	arg1 = eval_arg(KNO_CAR(payload),env,stack);
	arg2 = eval_arg(KNO_CDR(payload),env,stack);
	if (KNO_ABORTED(arg1))
	  result = arg1;
	else if (KNO_ABORTED(arg2))
	  result = arg2;
	else if ( (KNO_EMPTYP(arg1)) || (KNO_EMPTYP(arg2)) )
	  result = KNO_EMPTY;
	else result = d2_call(op,arg1,arg2);}
      else result = kno_err("OpcodeError",opname(op),NULL,expr);
      break;}
    case KNO_NUMERIC_OPCODE_CLASS: {
      if (PAIRP(payload)) {
	arg1 = eval_arg(KNO_CAR(payload),env,stack);
	arg2 = eval_arg(KNO_CDR(payload),env,stack);
	if ( (KNO_NUMBERP(arg1)) && (KNO_NUMBERP(arg2)) )
	  result = handle_numeric_opcode(op,arg1,arg2);
	else if (KNO_ABORTED(arg1))
	  result = arg1;
	else if (KNO_ABORTED(arg2))
	  result = arg2;
	else if ( (KNO_EMPTYP(arg1)) || (KNO_EMPTYP(arg2)) )
	  result = KNO_EMPTY;
	else if ( (KNO_CHOICEP(arg1)) || (KNO_CHOICEP(arg2)) )
	  result = handle_numeric_opcode_nd(op,arg1,arg2);
	else if (!(KNO_NUMBERP(arg1)))
	  result = kno_err(kno_NotANumber,opname(op),"arg1",arg1);
	else result = kno_err(kno_NotANumber,opname(op),"arg2",arg2);}
      else result = kno_err("OpcodeError",opname(op),NULL,expr);
      break;}
    case KNO_TABLE_OPCODE_CLASS: {
      if (KNO_PAIRP(payload)) {
	lispval args = payload;
	lispval subject_arg = pop_arg(args);
	arg1 = doeval(subject_arg,env,stack,0);
	lispval key_arg = pop_arg(args);
	arg2 = doeval(key_arg,env,stack,0);
	lispval value_arg = pop_arg(args);
	arg3 = doeval(value_arg,env,stack,0);
	if (KNO_TABLEP(arg1))
	  result = handle_table_opcode(op,arg1,arg2,arg3);
	else if (KNO_EMPTYP(arg1))
	  result = KNO_EMPTY;
	else result = failed_table_op(op,"subject",arg1);}
      else result = kno_err("VMError","table_opcode",kno_opcode_name(op),expr);
      break;}
    default:
      result = kno_err(kno_BadOpcode,"opcode_dispatch",NULL,op);}
    kno_decref(arg1);
    kno_decref(arg2);
    kno_decref(arg3);
    stack->eval_source=old_source;
    return result;}
  else return lisp_eval(op,expr,env,stack,tail);
}



static void link_local_cprims()
{
}

void init_vm_c()
{
  link_local_cprims();
}
