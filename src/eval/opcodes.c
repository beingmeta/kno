/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1
#define FD_INLINE_STACKS 1
#define FD_INLINE_LEXENV 1

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/support.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/opcodes.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/dbprims.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

#define simplify_value(v)                                       \
  ( (FD_PRECHOICEP(v)) ? (fd_simplify_choice(v)) : (v) )

static u8_string opcode_name(lispval opcode);

static lispval pickone_opcode(lispval normal);
static lispval pickstrings_opcode(lispval arg1);
static lispval pickoids_opcode(lispval arg1);
static lispval eltn_opcode(lispval arg1,int n,u8_context opname);
static lispval elt_opcode(lispval arg1,lispval arg2);
static lispval xref_opcode(lispval x,long long i,lispval tag);

static lispval op_eval(lispval x,fd_lexenv env,fd_stack stack,int tail);

static lispval opcode_dispatch_inner(lispval opcode,lispval expr,
                                     fd_lexenv env,
                                     fd_stack _stack,
                                     int tail)
{
  lispval args = FD_CDR(expr);
  if (opcode == FD_QUOTE_OPCODE)
    if (FD_EXPECT_TRUE(FD_PAIRP(args)))
      return fd_incref(FD_CAR(args));
    else return fd_err(fd_SyntaxError,"opcode_dispatch_inner",NULL,expr);
  else while (opcode == FD_SOURCEREF_OPCODE) {
    if (!(FD_PAIRP(FD_CDR(expr)))) {
      lispval err = fd_err(fd_SyntaxError,"opcode_dispatch",NULL,expr);
      _return err;}
    lispval source = pop_arg(args);
    expr = args;
    opcode = pop_arg(args);
    if ( (FD_NULLP(_stack->stack_source)) ||
         (FD_VOIDP(_stack->stack_source)) ||
         (_stack->stack_source == expr) )
      _stack->stack_source=source;
    if (! (FD_OPCODEP(opcode)) ) {
      _stack->stack_type = fd_evalstack_type;
      if (FD_PAIRP(expr))
        return fd_pair_eval(opcode,expr,env,_stack,tail);
      else return fast_stack_eval(expr,env,_stack);}}
  if (FD_SPECIAL_OPCODEP(opcode))
    return handle_special_opcode(opcode,expr,env,_stack,tail);
  else if ( (FD_D1_OPCODEP(opcode)) || (FD_ND1_OPCODEP(opcode)) ) {
    int nd_call = (FD_ND1_OPCODEP(opcode));
    lispval results = FD_EMPTY_CHOICE;
    lispval arg = FD_CAR(args), val = fast_stack_eval(arg,env,_stack);
    if (FD_PRECHOICEP(val)) val = fd_simplify_choice(val);
    if (FD_ABORTP(val)) _return val;
    else if ( (!(nd_call)) && (FD_EMPTY_CHOICEP(val)) )
      results = FD_EMPTY_CHOICE;
    else if (nd_call)
      results = nd1_call(opcode,val);
    else if (FD_CHOICEP(val)) {
      FD_DO_CHOICES(v,val) {
        lispval r = d1_call(opcode,val);
        if (FD_ABORTP(r)) {
          fd_decref(results);
          FD_STOP_DO_CHOICES;
          _return r;}
        else {FD_ADD_TO_CHOICE(results,r);}}}
    else results = d1_call(opcode,val);
    fd_decref(val);
    return results;}
  else if ( (FD_D2_OPCODEP(opcode)) ||
            (FD_ND2_OPCODEP(opcode)) ||
            (FD_NUMERIC_OPCODEP(opcode)) ) {
    int nd_call = (FD_ND2_OPCODEP(opcode));
    lispval results = FD_EMPTY_CHOICE;
    int numericp = (FD_NUMERIC_OPCODEP(opcode));
    lispval arg1 = pop_arg(args), val1 = fast_stack_eval(arg1,env,_stack);
    if (FD_ABORTP(val1)) return val1;
    else if (! (FD_EXPECT_TRUE(FD_PAIRP(args))) )
      return fd_err(fd_TooFewArgs,"opcode_dispatch_inner",opcode_name(opcode),
                    expr);
    else if ( (FD_EMPTY_CHOICEP(val1)) && (!(nd_call)) )
      return val1;
    else NO_ELSE;
    lispval arg2 = pop_arg(args), val2 = fast_stack_eval(arg2,env,_stack);
    if (FD_ABORTP(val2)) {fd_decref(val1); return val2;}
    else if ( (FD_EMPTY_CHOICEP(val2)) && (!(nd_call)) ) {
      fd_decref(val1); return val2;}
    else NO_ELSE;
    if (FD_PRECHOICEP(val1)) val1 = fd_simplify_choice(val1);
    if (FD_PRECHOICEP(val2)) val2 = fd_simplify_choice(val2);
    if ( (FD_CHOICEP(val1)) || (FD_CHOICEP(val2)) ) {
      if (nd_call)
        results = nd2_call(opcode,val1,val2);
      else {
        FD_DO_CHOICES(v1,val1) {
          FD_DO_CHOICES(v2,val2) {
            lispval r = (numericp) ? (numop_call(opcode,v1,v2)) :
              (d2_call(opcode,v1,v2));
            if (FD_ABORTP(r)) {
              fd_decref(results);
              FD_STOP_DO_CHOICES;
              results = r;
              break;}
            else {FD_ADD_TO_CHOICE(results,r);}}
          if (FD_ABORTP(results)) {
            FD_STOP_DO_CHOICES;
            break;}}}}
    else if (numericp)
      results = numop_call(opcode,val1,val2);
    else if (FD_ND2_OPCODEP(opcode))
      return nd2_call(opcode,val1,val2);
    else results = d2_call(opcode,val1,val2);
    fd_decref(val1); fd_decref(val2);
    return results;}
  else if (FD_TABLE_OPCODEP(opcode))
    return handle_table_opcode(opcode,expr,env,_stack,tail);
  else return fd_err(_("Invalid opcode"),"numop_call",NULL,expr);
}

static lispval opcode_dispatch(lispval opcode,lispval expr,
                               fd_lexenv env,
                               fd_stack caller,
                               int tail)
{
  FD_NEW_STACK(caller,"opcode",opcode_name(opcode),expr);
  lispval result = opcode_dispatch_inner(opcode,expr,env,_stack,tail);
  _return simplify_value(result);
}

FD_FASTOP lispval op_eval(lispval x,fd_lexenv env,
                          struct FD_STACK *stack,
                          int tail)
{
  switch (FD_PTR_MANIFEST_TYPE(x)) {
  case fd_oid_ptr_type: case fd_fixnum_ptr_type:
    return x;
  case fd_immediate_ptr_type:
    if (TYPEP(x,fd_lexref_type)) {
      lispval v = fd_lexref(x,env);
      return simplify_value(v);}
    else if (FD_SYMBOLP(x)) {
      lispval val = fd_symeval(x,env);
      if (PRED_FALSE(VOIDP(val)))
        return fd_err(fd_UnboundIdentifier,"op_eval",SYM_NAME(x),x);
      else return val;}
    else return x;
  case fd_cons_ptr_type: {
    fd_ptr_type cons_type = FD_PTR_TYPE(x);
    switch (cons_type) {
    case fd_pair_type: {
      lispval car = FD_CAR(x);
      if (TYPEP(car,fd_opcode_type)) {
        if (tail)
          return opcode_dispatch(car,x,env,stack,tail);
        else {
          lispval v = opcode_dispatch(car,x,env,stack,tail);
          return fd_finish_call(v);}}
      else return fd_stack_eval(x,env,stack,tail);}
    case fd_choice_type: case fd_prechoice_type:
      return fd_stack_eval(x,env,stack,0);
    case fd_slotmap_type:
      return fd_deep_copy(x);
    default:
      return fd_incref(x);}}
  default: /* Never reached */
    return x;
  }
}

/* Deterministic opcodes */

/* Initialization */

static double opcodes_initialized = 0;

FD_EXPORT
lispval fd_opcode_dispatch(lispval opcode,lispval expr,fd_lexenv env,
                           struct FD_STACK *stack,int tail)
{
  return opcode_dispatch(opcode,expr,env,stack,tail);
}

/* Initialization */

void fd_init_opcodes_c()
{
  if (opcodes_initialized) return;
  else opcodes_initialized = 1;

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
