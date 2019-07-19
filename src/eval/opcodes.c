/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_CHOICES (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_TABLES (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_FCNIDS (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_STACKS (!(KNO_AVOID_CHOICES))
#define KNO_INLINE_LEXENV (!(KNO_AVOID_CHOICES))

#define KNO_PROVIDE_FASTEVAL (!(KNO_AVOID_CHOICES))

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/opcodes.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/dbprims.h"
#include "kno/ports.h"
#include "kno/dtcall.h"

#include "eval_internals.h"

#include <libu8/u8timefns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

#define simplify_value(v) \
  ( (KNO_PRECHOICEP(v)) ? (kno_simplify_choice(v)) : (v) )

static u8_string opcode_name(lispval opcode);

static lispval pickone_opcode(lispval normal);
static lispval pickstrings_opcode(lispval arg1);
static lispval pickoids_opcode(lispval arg1);
static lispval eltn_opcode(lispval arg1,int n,u8_context opname);
static lispval elt_opcode(lispval arg1,lispval arg2);
static lispval xref_opcode(lispval x,long long i,lispval tag);
static lispval tableop(lispval opcode,lispval arg1,lispval arg2,lispval arg3);

static lispval op_eval(lispval x,kno_lexenv env,kno_stack stack,int tail);

KNO_FASTOP lispval op_eval_body(lispval body,kno_lexenv env,kno_stack stack,int tail)
{
  lispval result=VOID;
  if (body == NIL)
    return VOID;
  else while (PAIRP(body)) {
      lispval subex = KNO_CAR(body), next = KNO_CDR(body);
      if (PAIRP(next)) {
        lispval v = op_eval(subex,env,stack,0);
        if (KNO_ABORTED(v)) return v;
        kno_decref(v);
        body = next;}
      else return op_eval(subex,env,stack,tail);}
  return kno_err(kno_SyntaxError,"op_eval_body",NULL,body);
}

KNO_FASTOP lispval _next_qcode(lispval *scan)
{
  lispval expr = *scan;
  if (PAIRP(expr)) {
    lispval arg = KNO_CAR(expr);
    *scan = KNO_CDR(expr);
    return arg;}
  else return VOID;
}

#define next_qcode(args) (_next_qcode(&args))

static lispval unary_nd_dispatch(lispval opcode,lispval arg1)
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
  case KNO_CAR_OPCODE:
    if (EMPTYP(arg1))
      return arg1;
    else if (PAIRP(arg1))
      return kno_incref(KNO_CAR(arg1));
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1)
        if (PAIRP(arg)) {
          lispval car = KNO_CAR(arg);
          kno_incref(car);
          CHOICE_ADD(results,car);}
        else {
          kno_decref(results);
          return kno_type_error(_("pair"),"CAR opcode",arg);}
      return results;}
    else return kno_type_error(_("pair"),"CAR opcode",arg1);
  case KNO_CDR_OPCODE:
    if (EMPTYP(arg1)) return arg1;
    else if (PAIRP(arg1))
      return kno_incref(KNO_CDR(arg1));
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1)
        if (PAIRP(arg)) {
          lispval cdr = KNO_CDR(arg); kno_incref(cdr);
          CHOICE_ADD(results,cdr);}
        else {
          kno_decref(results);
          return kno_type_error(_("pair"),"CDR opcode",arg);}
      return results;}
    else return kno_type_error(_("pair"),"CDR opcode",arg1);
  case KNO_LENGTH_OPCODE:
    if (arg1==EMPTY) return EMPTY;
    else if (CHOICEP(arg1)) {
      lispval results = EMPTY;
      DO_CHOICES(arg,arg1) {
        if (KNO_SEQUENCEP(arg)) {
          int len = kno_seq_length(arg);
          lispval dlen = KNO_INT(len);
          CHOICE_ADD(results,dlen);}
        else {
          kno_decref(results);
          return kno_type_error(_("sequence"),"LENGTH opcode",arg);}}
      return kno_simplify_choice(results);}
    else if (KNO_SEQUENCEP(arg1))
      return KNO_INT(kno_seq_length(arg1));
    else return kno_type_error(_("sequence"),"LENGTH opcode",arg1);
  case KNO_QCHOICE_OPCODE:
    if (CHOICEP(arg1)) {
      kno_incref(arg1);
      return kno_init_qchoice(NULL,arg1);}
    else if (PRECHOICEP(arg1))
      return kno_init_qchoice(NULL,kno_make_simple_choice(arg1));
     else if (EMPTYP(arg1))
      return kno_init_qchoice(NULL,EMPTY);
    else return kno_incref(arg1);
  case KNO_CHOICE_SIZE_OPCODE:
    if (CHOICEP(arg1)) {
      int sz = KNO_CHOICE_SIZE(arg1);
      return KNO_INT(sz);}
    else if (PRECHOICEP(arg1)) {
      lispval simple = kno_make_simple_choice(arg1);
      int size = KNO_CHOICE_SIZE(simple);
      kno_decref(simple);
      return KNO_INT(size);}
    else if (EMPTYP(arg1))
      return KNO_INT(0);
    else return KNO_INT(1);
  case KNO_PICKOIDS_OPCODE:
    return pickoids_opcode(arg1);
  case KNO_PICKSTRINGS_OPCODE:
    return pickstrings_opcode(arg1);
  case KNO_PICKONE_OPCODE:
    if (CHOICEP(arg1))
      return pickone_opcode(arg1);
    else return kno_incref(arg1);
  case KNO_IFEXISTS_OPCODE:
    if (EMPTYP(arg1))
      return VOID;
    else return kno_incref(arg1);
  case KNO_FIXCHOICE_OPCODE:
    if (PRECHOICEP(arg1))
      return kno_simplify_choice(arg1);
    else return kno_incref(arg1);
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval unary_dispatch(lispval opcode,lispval arg1)
{
  int delta = 1;
  switch (opcode) {
  case KNO_MINUS1_OPCODE: delta = -1;
  case KNO_PLUS1_OPCODE:
    if (FIXNUMP(arg1)) {
      long long iarg = FIX2INT(arg1);
      return KNO_INT(iarg+delta);}
    else if (NUMBERP(arg1))
      return kno_plus(arg1,KNO_INT(-1));
    else return kno_type_error(_("number"),"opcode 1+/-",arg1);
  case KNO_NUMBERP_OPCODE:
    if (NUMBERP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_ZEROP_OPCODE:
    if (arg1==KNO_INT(0)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_VECTORP_OPCODE:
    if (VECTORP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_PAIRP_OPCODE:
    if (PAIRP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_EMPTY_LISTP_OPCODE:
    if (arg1==NIL) return KNO_TRUE; else return KNO_FALSE;
  case KNO_STRINGP_OPCODE:
    if (STRINGP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_OIDP_OPCODE:
    if (OIDP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_SYMBOLP_OPCODE:
    if (KNO_SYMBOLP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_FIXNUMP_OPCODE:
    if (FIXNUMP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_FLONUMP_OPCODE:
    if (KNO_FLONUMP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_TABLEP_OPCODE:
    if (TABLEP(arg1)) return KNO_TRUE; else return KNO_FALSE;
  case KNO_SEQUENCEP_OPCODE:
    if (KNO_SEQUENCEP(arg1)) return KNO_TRUE; else return KNO_FALSE;
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
  case KNO_FIRST_OPCODE:
    return eltn_opcode(arg1,0,"KNO_FIRST_OPCODE");
  case KNO_SECOND_OPCODE:
    return eltn_opcode(arg1,1,"KNO_SECOND_OPCODE");
  case KNO_THIRD_OPCODE:
    return eltn_opcode(arg1,2,"KNO_THIRD_OPCODE");
  case KNO_TONUMBER_OPCODE:
    if (FIXNUMP(arg1)) return arg1;
    else if (NUMBERP(arg1)) return kno_incref(arg1);
    else if (STRINGP(arg1))
      return kno_string2number(CSTRING(arg1),10);
    else if (KNO_CHARACTERP(arg1))
      return KNO_INT(KNO_CHARCODE(arg1));
    else return kno_type_error(_("number|string"),"opcode ->number",arg1);
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

static lispval unary_call(lispval opcode,lispval arg1)
{
  if (EMPTYP(arg1))
    return EMPTY;
  else if (!(CONSP(arg1)))
    return unary_dispatch(opcode,arg1);
  else if (!(CHOICEP(arg1))) {
    lispval result = unary_dispatch(opcode,arg1);
    kno_decref(arg1);
    return result;}
  else {
    lispval results = EMPTY;
    DO_CHOICES(arg,arg1) {
      lispval result = unary_dispatch(opcode,arg);
      if (KNO_ABORTED(result)) {
        kno_decref(results); kno_decref(arg1);
        KNO_STOP_DO_CHOICES;
        return result;}
      else {CHOICE_ADD(results,result);}}
    kno_decref(arg1);
    return results;}
}

static lispval binary_dispatch(lispval opcode,lispval arg1,lispval arg2)
{
  switch (opcode) {
  case KNO_NUMEQ_OPCODE:
    if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
      if ((FIX2INT(arg1)) == (FIX2INT(arg2)))
        return KNO_TRUE;
      else return KNO_FALSE;
    else if (kno_numcompare(arg1,arg2)==0) return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_GT_OPCODE:
    if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
      if ((FIX2INT(arg1))>(FIX2INT(arg2)))
        return KNO_TRUE;
      else return KNO_FALSE;
    else if (kno_numcompare(arg1,arg2)>0) return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_GTE_OPCODE:
    if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
      if ((FIX2INT(arg1))>=(FIX2INT(arg2)))
        return KNO_TRUE;
      else return KNO_FALSE;
    else if (kno_numcompare(arg1,arg2)>=0) return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_LT_OPCODE:
    if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
      if ((FIX2INT(arg1))<(FIX2INT(arg2)))
        return KNO_TRUE;
      else return KNO_FALSE;
    else if (kno_numcompare(arg1,arg2)<0) return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_LTE_OPCODE:
    if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))
      if ((FIX2INT(arg1))<=(FIX2INT(arg2)))
        return KNO_TRUE;
      else return KNO_FALSE;
    else if (kno_numcompare(arg1,arg2)<=0) return KNO_TRUE;
    else return KNO_FALSE;
  case KNO_PLUS_OPCODE:
    if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
      long long m = FIX2INT(arg1), n = FIX2INT(arg2);
      return KNO_INT(m+n);}
    else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
      double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
      return kno_init_double(NULL,x+y);}
    else return kno_plus(arg1,arg2);
  case KNO_MINUS_OPCODE:
    if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
      long long m = FIX2INT(arg1), n = FIX2INT(arg2);
      return KNO_INT(m-n);}
    else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
      double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
      return kno_init_double(NULL,x-y);}
    else return kno_subtract(arg1,arg2);
  case KNO_TIMES_OPCODE:
    if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
      long long m = FIX2INT(arg1), n = FIX2INT(arg2);
      return KNO_INT(m*n);}
    else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
      double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
      return kno_init_double(NULL,x*y);}
    else return kno_multiply(arg1,arg2);
  case KNO_FLODIV_OPCODE:
    if ((FIXNUMP(arg1)) && (FIXNUMP(arg2)))  {
      long long m = FIX2INT(arg1), n = FIX2INT(arg2);
      double x = (double)m, y = (double)n;
      return kno_init_double(NULL,x/y);}
    else if ((KNO_FLONUMP(arg1)) && (KNO_FLONUMP(arg2))) {
      double x = KNO_FLONUM(arg1), y = KNO_FLONUM(arg2);
      return kno_init_double(NULL,x/y);}
    else {
      double x = kno_todouble(arg1), y = kno_todouble(arg2);
      return kno_init_double(NULL,x/y);}
  case KNO_ELT_OPCODE:
    return elt_opcode(arg1,arg2);
  case KNO_CONS_OPCODE:
    return kno_init_pair(NULL,arg1,arg2);
  case KNO_EQ_OPCODE: {
    if (arg1==arg2) return KNO_TRUE; else return KNO_FALSE;
    break;}
  case KNO_EQV_OPCODE: {
    if (arg1==arg2) return KNO_TRUE;
    else if ((NUMBERP(arg1)) && (NUMBERP(arg2)))
      if (kno_numcompare(arg1,arg2)==0)
        return KNO_TRUE; else return KNO_FALSE;
    else return KNO_FALSE;
    break;}
  case KNO_EQUAL_OPCODE: {
    if ((ATOMICP(arg1)) && (ATOMICP(arg2)))
      if (arg1==arg2) return KNO_TRUE; else return KNO_FALSE;
    else if (KNO_EQUAL(arg1,arg2)) return KNO_TRUE;
    else return KNO_FALSE;
    break;}
  default:
    return kno_err(_("Invalid opcode"),"opcode eval",NULL,VOID);
  }
}

KNO_FASTOP int numeric_argp(lispval x)
{
  /* This checks if there is a type error.
     The empty choice isn't a type error since it will just
     generate an empty choice as a result. */
  if ((EMPTYP(x))||(FIXNUMP(x)))
    return 1;
  else if (!(CONSP(x)))
    return 0;
  else switch (KNO_CONSPTR_TYPE(x)) {
    case kno_flonum_type: case kno_bigint_type:
    case kno_rational_type: case kno_complex_type:
      return 1;
    case kno_choice_type: case kno_prechoice_type: {
      DO_CHOICES(a,x) {
        if (FIXNUMP(a)) {}
        else if (PRED_TRUE(NUMBERP(a))) {}
        else {
          KNO_STOP_DO_CHOICES;
          return 0;}}
      return 1;}
    default:
      return 0;}
}

static lispval d2_call(lispval opcode,lispval arg1,lispval arg2)
{
  lispval results = EMPTY;
  DO_CHOICES(a1,arg1) {
    {DO_CHOICES(a2,arg2) {
        lispval result = binary_dispatch(opcode,a1,a2);
        /* If we need to abort due to an error, we need to pop out of
           two choice loops.  So on the inside, we decref results and
           replace it with the error object.  We then break and
           do KNO_STOP_DO_CHOICES (for potential cleanup). */
        if (KNO_ABORTED(result)) {
          kno_decref(results); results = result;
          KNO_STOP_DO_CHOICES; break;}
        else {CHOICE_ADD(results,result);}}}
    /* If the inner loop aborted due to an error, results is now bound
       to the error, so we just KNO_STOP_DO_CHOICES (this time for the
       outer loop) and break; */
    if (KNO_ABORTED(results)) {
      KNO_STOP_DO_CHOICES; break;}}
  kno_decref(arg1); kno_decref(arg2);
  return results;
}

static lispval binary_nd_dispatch(lispval opcode,lispval arg1,lispval arg2)
{
  lispval result = KNO_ERROR_VALUE;
  if (PRECHOICEP(arg2)) arg2 = kno_simplify_choice(arg2);
  if (PRECHOICEP(arg1)) arg1 = kno_simplify_choice(arg1);
  lispval argv[2]={arg1,arg2};
  if (KNO_ABORTED(arg2)) result = arg2;
  else if (VOIDP(arg2)) {
    result = kno_err(kno_VoidArgument,"OPCODE setop",NULL,opcode);}
  else switch (opcode) {
    case KNO_IDENTICAL_OPCODE:
      if (arg1==arg2) result = KNO_TRUE;
      else if (KNO_EQUAL(arg1,arg2)) result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_OVERLAPS_OPCODE:
      if (arg1==arg2) result = KNO_TRUE;
      else if (kno_overlapp(arg1,arg2)) result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_CONTAINSP_OPCODE:
      if (kno_containsp(arg1,arg2)) result = KNO_TRUE;
      else result = KNO_FALSE;
      break;
    case KNO_INTERSECT_OPCODE:
      if ((EMPTYP(arg1)) || (EMPTYP(arg2)))
        result = EMPTY;
      else result = kno_intersection(argv,2);
      break;
    case KNO_UNION_OPCODE:
      if (EMPTYP(arg1)) result = kno_incref(arg2);
      else if (EMPTYP(arg2)) result = kno_incref(arg1);
      else result = kno_union(argv,2);
      break;
    case KNO_DIFFERENCE_OPCODE:
      if ((EMPTYP(arg1)) || (EMPTYP(arg2)))
        result = kno_incref(arg1);
      else result = kno_difference(arg1,arg2);
      break;
    case KNO_CHOICEREF_OPCODE:
      if (!(FIXNUMP(arg2))) {
        kno_decref(arg1);
        return kno_err(kno_SyntaxError,"choiceref_opcode",NULL,arg2);}
      else {
        int i = FIX2INT(arg2);
        if (i<0) return kno_err(kno_SyntaxError,"choiceref_opcode",NULL,arg2);
        if ((i==0)&&(EMPTYP(arg1))) return VOID;
        else if (CHOICEP(arg1)) {
          struct KNO_CHOICE *ch = (kno_choice)arg1;
          if (i<ch->choice_size) {
            const lispval *elts = KNO_XCHOICE_DATA(ch);
            lispval elt = elts[i];
            return kno_incref(elt);}
          else return VOID;}
        else if (i==0) return arg1;
        else {kno_decref(arg1); return VOID;}}
    }
  kno_decref(arg1); kno_decref(arg2);
  return result;
}

static lispval try_op(lispval exprs,kno_lexenv env,
                     kno_stack stack,int tail)
{
  while (PAIRP(exprs)) {
    lispval expr = next_qcode(exprs);
    if (NILP(exprs))
      return op_eval(expr,env,stack,tail);
    else {
      lispval val  = op_eval(expr,env,stack,0);
      if (KNO_ABORTED(val)) return val;
      else if (!(EMPTYP(val)))
        return val;
      else kno_decref(val);}}
  return EMPTY;
}

static lispval and_op(lispval exprs,kno_lexenv env,kno_stack stack,int tail)
{
  while (PAIRP(exprs)) {
    lispval expr = next_qcode(exprs);
    if (NILP(exprs))
      return op_eval(expr,env,stack,tail);
    else {
      lispval val  = op_eval(expr,env,stack,0);
      if (KNO_ABORTED(val)) return val;
      else if (FALSEP(val))
        return KNO_FALSE;
      else kno_decref(val);}}
  return KNO_TRUE;
}

static lispval or_op(lispval exprs,kno_lexenv env,kno_stack stack,int tail)
{
  while (PAIRP(exprs)) {
    lispval expr = next_qcode(exprs);
    if (NILP(exprs))
      return op_eval(expr,env,stack,tail);
    else {
      lispval val  = op_eval(expr,env,stack,0);
      if (KNO_ABORTED(val)) return val;
      else if (!(FALSEP(val)))
        return val;
      else {}}}
  return KNO_FALSE;
}

/* Loop */

static lispval until_opcode(lispval expr,kno_lexenv env,kno_stack stack)
{
  lispval params = KNO_CDR(expr);
  lispval test_expr = KNO_CAR(params), loop_body = KNO_CDR(params);
  if (VOIDP(test_expr))
    return kno_err(kno_SyntaxError,"KNO_LOOP_OPCODE",NULL,expr);
  lispval test_val = op_eval(test_expr,env,stack,0);
  if (KNO_ABORTED(test_val))
    return test_val;
  else while (FALSEP(test_val)) {
      lispval body_result=op_eval_body(loop_body,env,stack,0);
      if (KNO_BROKEP(body_result))
        return KNO_FALSE;
      else if (KNO_ABORTED(body_result))
        return body_result;
      else kno_decref(body_result);
      test_val = op_eval(test_expr,env,stack,0);
      if (KNO_ABORTED(test_val))
        return test_val;}
  return test_val;
}

/* Assignment */

#define CURRENT_VALUEP(x) \
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
    if (PRED_TRUE(scan!=NULL)) {
      lispval bindings = scan->env_bindings;
      if (PRED_TRUE(SCHEMAPP(bindings))) {
        struct KNO_SCHEMAP *map = (struct KNO_SCHEMAP *)bindings;
        int map_len = map->schema_length;
        if (PRED_TRUE( across < map_len )) {
          lispval *values = map->schema_values;
          lispval cur     = values[across];
          if ( map->schemap_stackvals ) {
            kno_incref_vec(values,map_len);
            map->schemap_stackvals = 0;}
          if ( ( (combiner == KNO_TRUE) || (combiner == KNO_DEFAULT) ) &&
               ( (CURRENT_VALUEP(cur)) || (KNO_ABORTED(cur)) ) ) {
            if (KNO_ABORTED(cur))
              return cur;
            else return VOID;}
          else {
            lispval value = op_eval(expr,env,stack,0);
            /* This gnarly bit of code handles the case where
               evaluating 'expr' changed the environment structure,
               by, for instance, creating a lambda which made a
               dynamic environment copy. If so, we need to change the
               value of 'values' so that we store any resulting values
               in the right place. */
            if ( (scan->env_copy) && (scan->env_copy != scan) ) {
              lispval new_bindings = scan->env_copy->env_bindings;
              if ( (new_bindings != bindings) &&
                   (PRED_TRUE(SCHEMAPP(bindings))) ) {
                struct KNO_SCHEMAP *new_map =
                  (struct KNO_SCHEMAP *) new_bindings;
                values = new_map->schema_values;
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
    lispval value = op_eval(expr,env,stack,0);
    if ( (combiner == KNO_FALSE) || (combiner == VOID) ) {
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
                      struct KNO_STACK *_stack,kno_lexenv env,
                      lispval vars,lispval inits,lispval body,
                      int tail)
{
  int i=0, n=VEC_LEN(vars);
  KNO_PUSH_STACK(bind_stack,"bindop","opframe",op);
  INIT_STACK_SCHEMA(bind_stack,bound,env,n,VEC_DATA(vars));
  lispval *values=bound_bindings.schema_values;
  lispval *exprs=VEC_DATA(inits);
  kno_lexenv env_copy=NULL;
  while (i<n) {
    lispval val_expr=exprs[i];
    lispval val=op_eval(val_expr,bound,bind_stack,0);
    if (KNO_ABORTED(val)) _return val;
    if ( (env_copy == NULL) && (bound->env_copy) ) {
      env_copy=bound->env_copy; bound=env_copy;
      values=((kno_schemap)(bound->env_bindings))->schema_values;}
    values[i++]=val;}
  lispval result = op_eval_body(body,bound,bind_stack,tail);
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

/* Opcode dispatch */

static lispval opcode_dispatch_inner(lispval opcode,lispval expr,
                                     kno_lexenv env,
                                     kno_stack _stack,
                                     int tail)
{
  if (opcode == KNO_QUOTE_OPCODE)
    return kno_incref(next_qcode(expr));
  if (opcode == KNO_SOURCEREF_OPCODE) {
    if (!(KNO_PAIRP(KNO_CDR(expr)))) {
      lispval err = kno_err(kno_SyntaxError,"opcode_dispatch",NULL,expr);
      _return err;}
    /* Unpack sourcerefs */
    while (opcode == KNO_SOURCEREF_OPCODE) {
      lispval source = KNO_CAR(KNO_CDR(expr));
      lispval code   = KNO_CDR(KNO_CDR(expr));
      if (!(KNO_PAIRP(code))) {
        lispval err = kno_err(kno_SyntaxError,"opcode_dispatch",NULL,expr);
        return err;}
      else expr = code;
      if ( (KNO_NULLP(_stack->stack_source)) ||
           (KNO_VOIDP(_stack->stack_source)) ||
           (_stack->stack_source == expr) )
        _stack->stack_source=source;
      lispval old_op = _stack->stack_op;
      kno_incref(code);
      _stack->stack_op=code;
      if (_stack->stack_decref_op) kno_decref(old_op);
      _stack->stack_decref_op = 1;
      lispval realop = KNO_CAR(code);
      if (!(KNO_OPCODEP(realop))) {
        opcode = realop;
        break;}
      opcode=realop;
      expr = code;}
    if (! (KNO_OPCODEP(opcode)) ) {
      _stack->stack_type = kno_evalstack_type;
      if (KNO_PAIRP(expr))
        return kno_pair_eval(opcode,expr,env,_stack,tail);
      else return _kno_fast_eval(expr,env,_stack,tail);}}
  lispval args = KNO_CDR(expr);
  switch (opcode) {
  case KNO_SYMREF_OPCODE: {
    lispval refenv=next_qcode(args);
    lispval sym=next_qcode(args);
    if (!(KNO_SYMBOLP(sym)))
      return kno_err(kno_SyntaxError,"KNO_SYMREF_OPCODE/badsym",NULL,expr);
    if (HASHTABLEP(refenv))
      return kno_hashtable_get((kno_hashtable)refenv,sym,KNO_UNBOUND);
    else if (KNO_LEXENVP(refenv))
      return kno_symeval(sym,(kno_lexenv)refenv);
    else if (TABLEP(refenv))
      return kno_get(refenv,sym,KNO_UNBOUND);
    else return kno_err(kno_SyntaxError,"KNO_SYMREF_OPCODE/badenv",NULL,expr);}
  case KNO_VOID_OPCODE: {
    return VOID;}
  case KNO_RESET_ENV_OPCODE: {
    reset_env_op(env);
    return VOID;}
  case KNO_BEGIN_OPCODE:
    return op_eval_body(KNO_CDR(expr),env,_stack,tail);
  case KNO_UNTIL_OPCODE:
    return until_opcode(expr,env,_stack);
  case KNO_BRANCH_OPCODE: {
    lispval test_expr = next_qcode(args);
    if (VOIDP(test_expr))
      return kno_err(kno_SyntaxError,"KNO_BRANCH_OPCODE",NULL,expr);
    lispval test_val = op_eval(test_expr,env,_stack,0);
    if (KNO_ABORTED(test_val))
      return test_val;
    else if (KNO_FALSEP(test_val)) { /* (  || (KNO_EMPTYP(test_val)) ) */
      next_qcode(args);
      return op_eval(next_qcode(args),env,_stack,tail);}
    else {
      lispval then = next_qcode(args);
      U8_MAYBE_UNUSED lispval ignore = next_qcode(args);
      kno_decref(test_val);
      return op_eval(then,env,_stack,tail);}}

  case KNO_BIND_OPCODE: {
    lispval vars=next_qcode(args);
    lispval inits=next_qcode(args);
    lispval body=next_qcode(args);
    return bindop(opcode,_stack,env,vars,inits,body,tail);}

  case KNO_ASSIGN_OPCODE: {
    lispval var = next_qcode(args);
    lispval combiner = next_qcode(args);
    lispval val_expr = next_qcode(args);
    return assignop(_stack,env,var,val_expr,combiner);}
  case KNO_XREF_OPCODE: {
    lispval off_arg = next_qcode(args);
    lispval type_arg = next_qcode(args);
    lispval obj_expr = next_qcode(args);
    if ( (FIXNUMP(off_arg)) && (! (VOIDP(obj_expr)) ) ) {
      lispval obj_arg=fast_eval(obj_expr,env);
      return xref_opcode(kno_simplify_choice(obj_arg),
                         FIX2INT(off_arg),
                         type_arg);}
    kno_seterr(kno_SyntaxError,"KNO_XREF_OPCODE",NULL,expr);
    return KNO_ERROR_VALUE;}
  case KNO_NOT_OPCODE: {
    lispval arg_val = op_eval(next_qcode(args),env,_stack,0);
    if (FALSEP(arg_val))
      return KNO_TRUE;
    else {
      kno_decref(arg_val);
      return KNO_FALSE;}}
  case KNO_TRY_OPCODE:
    return try_op(args,env,_stack,tail);
  case KNO_AND_OPCODE:
    return and_op(args,env,_stack,tail);
  case KNO_OR_OPCODE:
    return or_op(args,env,_stack,tail);
  case KNO_GET_OPCODE: case KNO_PRIMGET_OPCODE:
  case KNO_TEST_OPCODE: case KNO_PRIMTEST_OPCODE:
  case KNO_ASSERT_OPCODE: case KNO_ADD_OPCODE:
  case KNO_RETRACT_OPCODE: case KNO_DROP_OPCODE:
  case KNO_STORE_OPCODE: {
    lispval expr1=next_qcode(args), expr2=next_qcode(args), expr3=next_qcode(args);
    lispval arg1=op_eval(expr1,env,_stack,0), arg2, arg3;
    lispval result=VOID;
    if (KNO_ABORTED(arg1)) return arg1;
    arg2=op_eval(expr2,env,_stack,0);
    if (KNO_ABORTED(arg2)) {
      kno_decref(arg1);
      return arg2;}
    arg3=(VOIDP(expr3))?(VOID):(op_eval(expr3,env,_stack,0));
    if (KNO_ABORTED(arg3)) {
      kno_decref(arg1);
      kno_decref(arg2);
      return arg3;}
    else result=tableop(opcode,arg1,arg2,arg3);
    kno_decref(arg1); kno_decref(arg2); kno_decref(arg3);
    return result;}}
  if (!(PRED_FALSE(PAIRP(KNO_CDR(expr))))) {
    /* Otherwise, we should have at least one argument,
       return an error otherwise. */
    return kno_err(kno_SyntaxError,"opcode eval",NULL,expr);}
  else {
    /* We have at least one argument to evaluate and we also get the body. */
    lispval arg1_expr = next_qcode(args), arg1 = op_eval(arg1_expr,env,_stack,0);
    lispval arg2_expr = next_qcode(args), arg2;
    /* Now, check the result of the first argument expression */
    if (KNO_ABORTED(arg1)) return arg1;
    else if (VOIDP(arg1))
      return kno_err(kno_VoidArgument,"opcode eval",NULL,arg1_expr);
    else if (PRECHOICEP(arg1))
      arg1 = kno_simplify_choice(arg1);
    else {}
    if (KNO_ND1_OPCODEP(opcode)) {
      if (opcode == KNO_FIXCHOICE_OPCODE) {
        if (PRECHOICEP(arg1))
          return kno_simplify_choice(arg1);
        else return arg1;}
      else if (CONSP(arg1)) {
        lispval result = unary_nd_dispatch(opcode,arg1);
        kno_decref(arg1);
        return result;}
      else return unary_nd_dispatch(opcode,arg1);}
    else if (KNO_D1_OPCODEP(opcode))
      return unary_call(opcode,arg1);
    /* Check the type for numeric arguments here. */
    else if (KNO_NUMERIC_OPCODEP(opcode)) {
      if (PRED_FALSE(EMPTYP(arg1)))
        return EMPTY;
      else if (PRED_FALSE(!(numeric_argp(arg1)))) {
        lispval result = kno_type_error(_("number"),"numeric opcode",arg1);
        kno_decref(arg1);
        return result;}
      else {
        if (VOIDP(arg2_expr)) {
          kno_decref(arg1);
          return kno_err(kno_TooFewArgs,opcode_name(opcode),NULL,expr);}
        else arg2 = op_eval(arg2_expr,env,_stack,0);
        if (PRECHOICEP(arg2)) arg2 = kno_simplify_choice(arg2);
        if (KNO_ABORTED(arg2)) {
          kno_decref(arg1);
          return arg2;}
        else if (EMPTYP(arg2)) {
          kno_decref(arg1);
          return EMPTY;}
        else if ((CHOICEP(arg1))||(CHOICEP(arg2)))
          return d2_call(opcode,arg1,arg2);
        else if ((CONSP(arg1))||(CONSP(arg2))) {
          lispval result = binary_dispatch(opcode,arg1,arg2);
          kno_decref(arg1); kno_decref(arg2);
          return result;}
        else return binary_dispatch(opcode,arg1,arg2);}}
    else if (KNO_D2_OPCODEP(opcode)) {
      if (EMPTYP(arg1))
        return EMPTY;
      else {
        if (VOIDP(arg2_expr)) {
          kno_decref(arg1);
          return kno_err(kno_TooFewArgs,opcode_name(opcode),NULL,expr);}
        else arg2 = op_eval(arg2_expr,env,_stack,0);
        if (PRECHOICEP(arg2)) arg2 = kno_simplify_choice(arg2);
        if (KNO_ABORTED(arg2)) {
          kno_decref(arg1); return arg2;}
        else if (VOIDP(arg2)) {
          kno_decref(arg1);
          return kno_err(kno_VoidArgument,"opcode eval",NULL,arg2_expr);}
        else if (EMPTYP(arg2)) {
          /* Prune the call */
          kno_decref(arg1); return arg2;}
        else if ((CHOICEP(arg1)) || (CHOICEP(arg2)))
          /* binary_nd_dispatch handles decref of arg1 and arg2 */
          return d2_call(opcode,arg1,arg2);
        else {
          /* This is the dispatch case where we just go to dispatch. */
          lispval result = binary_dispatch(opcode,arg1,arg2);
          kno_decref(arg1); kno_decref(arg2);
          return result;}}}
    else if (KNO_ND2_OPCODEP(opcode))
      /* This decrefs its arguments itself */
      return binary_nd_dispatch(opcode,arg1,op_eval(arg2_expr,env,_stack,0));
    else {
      kno_decref(arg1);
      return kno_err(kno_SyntaxError,"opcode eval",NULL,expr);}
  }
}

static lispval opcode_dispatch(lispval opcode,lispval expr,
                              kno_lexenv env,
                              kno_stack caller,
                              int tail)
{
  KNO_NEW_STACK(caller,"opcode",opcode_name(opcode),expr);
  lispval result = opcode_dispatch_inner(opcode,expr,env,_stack,tail);
  _return simplify_value(result);
}

KNO_FASTOP lispval op_eval(lispval x,kno_lexenv env,
                         struct KNO_STACK *stack,
                         int tail)
{
  switch (KNO_PTR_MANIFEST_TYPE(x)) {
  case kno_oid_ptr_type: case kno_fixnum_ptr_type:
    return x;
  case kno_immediate_ptr_type:
    if (TYPEP(x,kno_lexref_type)) {
      lispval v = kno_lexref(x,env);
      return simplify_value(v);}
    else if (KNO_SYMBOLP(x)) {
      lispval val = kno_symeval(x,env);
      if (PRED_FALSE(VOIDP(val)))
        return kno_err(kno_UnboundIdentifier,"op_eval",SYM_NAME(x),x);
      else return val;}
    else return x;
  case kno_cons_ptr_type: {
    kno_lisp_type cons_type = KNO_LISP_TYPE(x);
    switch (cons_type) {
    case kno_pair_type: {
      lispval car = KNO_CAR(x);
      if (TYPEP(car,kno_opcode_type)) {
        if (tail)
          return opcode_dispatch(car,x,env,stack,tail);
        else {
          lispval v = opcode_dispatch(car,x,env,stack,tail);
          return kno_finish_call(v);}}
      else if (tail)
        return kno_tail_eval(x,env);
      else return kno_eval(x,env);}
    case kno_choice_type: case kno_prechoice_type:
      return kno_eval(x,env);
    case kno_slotmap_type:
      return kno_deep_copy(x);
    default:
      return kno_incref(x);}}
  default: /* Never reached */
    return x;
  }
}

/* Deterministic opcodes */

static lispval tableop(lispval opcode,lispval arg1,lispval arg2,lispval arg3)
{
  if (EMPTYP(arg1)) {
    switch (opcode) {
    case KNO_GET_OPCODE: case KNO_PRIMGET_OPCODE:
      return EMPTY;
    case KNO_TEST_OPCODE: case KNO_PRIMTEST_OPCODE:
      return KNO_FALSE;
    case KNO_STORE_OPCODE:
    case KNO_RETRACT_OPCODE: case KNO_DROP_OPCODE:
    case KNO_ASSERT_OPCODE: case KNO_ADD_OPCODE:
      return VOID;
    default:
      return VOID;}}
  else if (opcode == KNO_TEST_OPCODE)
    return kno_ftest(arg1,arg2,arg3);
  else if (opcode == KNO_PRIMTEST_OPCODE) {
    int rv = kno_test(arg1,arg2,arg3);
    if (rv<0) return KNO_ERROR_VALUE;
    else if (rv>0) return KNO_TRUE;
    else return KNO_FALSE;}
  else if (CHOICEP(arg1)) {
    lispval results=EMPTY;
    DO_CHOICES(tbl,arg1) {
      lispval partial=tableop(opcode,tbl,arg2,arg3);
      if (KNO_ABORTED(partial)) {
        kno_decref(results);
        KNO_STOP_DO_CHOICES;
        return partial;}
      else {
        CHOICE_ADD(results,partial);}}
    return results;}
  else {
    int rv=0;
    switch (opcode) {
    case KNO_PRIMGET_OPCODE:
      if (KNO_VOIDP(arg3))
        return kno_get(arg1,arg2,KNO_EMPTY);
      else return kno_get(arg1,arg2,arg3);
    case KNO_PRIMTEST_OPCODE:
      rv=kno_test(arg1,arg2,arg3);
      if (rv<0) return KNO_ERROR_VALUE;
      else if (rv==0) return KNO_FALSE;
      else return KNO_TRUE;
    case KNO_ADD_OPCODE:
      rv=kno_add(arg1,arg2,arg3);
      if (rv<0) return KNO_ERROR_VALUE;
      else if (rv==0) return KNO_FALSE;
      else return KNO_TRUE;
    case KNO_DROP_OPCODE:
      rv=kno_drop(arg1,arg2,arg3);
      if (rv<0) return KNO_ERROR_VALUE;
      else if (rv==0) return KNO_FALSE;
      else return KNO_TRUE;
    case KNO_STORE_OPCODE:
      rv=kno_store(arg1,arg2,arg3);
      if (rv<0) return KNO_ERROR_VALUE;
      else if (rv==0) return KNO_FALSE;
      else return KNO_TRUE;
    case KNO_TEST_OPCODE:
      if (OIDP(arg1))
        rv=kno_frame_test(arg1,arg2,arg3);
      else rv=kno_test(arg1,arg2,arg3);
      if (rv<0) return KNO_ERROR_VALUE;
      else if (rv==0) return KNO_FALSE;
      else return KNO_TRUE;
    case KNO_GET_OPCODE:
      return kno_fget(arg1,arg2);
    case KNO_ASSERT_OPCODE:
      if (OIDP(arg1))
        rv=kno_assert(arg1,arg2,arg3);
      else rv=kno_add(arg1,arg2,arg3);
      if (rv<0) return KNO_ERROR_VALUE;
      else if (rv==0) return KNO_FALSE;
      else return KNO_TRUE;
    case KNO_RETRACT_OPCODE:
      if (OIDP(arg1))
        rv=kno_retract(arg1,arg2,arg3);
      else rv=kno_drop(arg1,arg2,arg3);
      if (rv<0) return KNO_ERROR_VALUE;
      else if (rv==0) return KNO_FALSE;
      else return KNO_TRUE;
    default:
      return VOID;}}
}

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
  if ((VOIDP(tag)) || ((c->compound_typetag) == tag)) {
    if ((i>=0) && (i<c->compound_length)) {
      lispval *values = &(c->compound_0), value;
      if (c->compound_ismutable)
        u8_lock_mutex(&(c->compound_rwlock));
      value = values[i];
      kno_incref(value);
      if (c->compound_ismutable)
        u8_unlock_mutex(&(c->compound_rwlock));
      if (free) kno_decref((lispval)c);
      return value;}
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
        kno_decref(results); results = r;
        KNO_STOP_DO_CHOICES;
        break;}
      else {CHOICE_ADD(results,r);}}
    /* Need to free this */
    kno_decref(x);
    return results;}
  else return kno_err(kno_TypeError,"xref",kno_lisp2string(tag),x);
}

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

/* PICK opcodes */

static lispval pickoids_opcode(lispval arg1)
{
  if (OIDP(arg1)) return arg1;
  else if (EMPTYP(arg1)) return arg1;
  else if ((CHOICEP(arg1)) || (PRECHOICEP(arg1))) {
    lispval choice, results = EMPTY;
    int free_choice = 0, all_oids = 1;
    if (CHOICEP(arg1)) choice = arg1;
    else {choice = kno_make_simple_choice(arg1); free_choice = 1;}
    {DO_CHOICES(elt,choice) {
        if (OIDP(elt)) {CHOICE_ADD(results,elt);}
        else if (all_oids) all_oids = 0;}}
    if (all_oids) {
      kno_decref(results);
      if (free_choice) return choice;
      else return kno_incref(choice);}
    else if (free_choice) kno_decref(choice);
    return kno_simplify_choice(results);}
  else return EMPTY;
}

static lispval pickstrings_opcode(lispval arg1)
{
  if ((CHOICEP(arg1)) || (PRECHOICEP(arg1))) {
    lispval choice, results = EMPTY;
    int free_choice = 0, all_strings = 1;
    if (CHOICEP(arg1)) choice = arg1;
    else {choice = kno_make_simple_choice(arg1); free_choice = 1;}
    {DO_CHOICES(elt,choice) {
        if (STRINGP(elt)) {
          kno_incref(elt); CHOICE_ADD(results,elt);}
        else if (all_strings) all_strings = 0;}}
    if (all_strings) {
      kno_decref(results);
      if (free_choice) return choice;
      else return kno_incref(choice);}
    else if (free_choice) kno_decref(choice);
    return kno_simplify_choice(results);}
  else if (STRINGP(arg1)) return kno_incref(arg1);
  else return EMPTY;
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

/* Initialization */

static double opcodes_initialized = 0;

KNO_EXPORT
lispval kno_opcode_dispatch(lispval opcode,lispval expr,kno_lexenv env,
                          struct KNO_STACK *stack,int tail)
{
  return opcode_dispatch(opcode,expr,env,stack,tail);
}

/* Opcode objects/names */

u8_string kno_opcode_names[0x800]={NULL};

int kno_opcodes_length = 0x800;

/* Opcodes and names */

static void set_opcode_name(lispval opcode,u8_string name)
{
  int off = KNO_OPCODE_NUM(opcode);
  u8_string constname=u8_string_append("#",name,NULL);
  kno_opcode_names[off]=name;
  kno_add_constname(constname,opcode);
  u8_free(constname);
}

static void init_opcode_names()
{
  set_opcode_name(KNO_BRANCH_OPCODE,"OP_BRANCH");
  set_opcode_name(KNO_NOT_OPCODE,"OP_NOT");
  set_opcode_name(KNO_UNTIL_OPCODE,"OP_UNTIL");
  set_opcode_name(KNO_BEGIN_OPCODE,"OP_BEGIN");
  set_opcode_name(KNO_QUOTE_OPCODE,"OP_QUOTE");
  set_opcode_name(KNO_ASSIGN_OPCODE,"OP_ASSIGN");
  set_opcode_name(KNO_SYMREF_OPCODE,"OP_SYMREF");
  set_opcode_name(KNO_BIND_OPCODE,"OP_BIND");
  set_opcode_name(KNO_VOID_OPCODE,"OP_VOID");
  set_opcode_name(KNO_AND_OPCODE,"OP_AND");
  set_opcode_name(KNO_OR_OPCODE,"OP_OR");
  set_opcode_name(KNO_TRY_OPCODE,"OP_TRY");
  set_opcode_name(KNO_CHOICEREF_OPCODE,"OP_CHOICEREF");
  set_opcode_name(KNO_FIXCHOICE_OPCODE,"OP_FIXCHOICE");

  set_opcode_name(KNO_SOURCEREF_OPCODE,"OP_SOURCEREF");
  set_opcode_name(KNO_RESET_ENV_OPCODE,"OP_RESET_ENV");

  set_opcode_name(KNO_AMBIGP_OPCODE,"OP_AMBIGP");
  set_opcode_name(KNO_SINGLETONP_OPCODE,"OP_SINGLETONP");
  set_opcode_name(KNO_FAILP_OPCODE,"OP_FAILP");
  set_opcode_name(KNO_EXISTSP_OPCODE,"OP_EXISTSP");
  set_opcode_name(KNO_SINGLETON_OPCODE,"OP_SINGLETON");
  set_opcode_name(KNO_CAR_OPCODE,"OP_CAR");
  set_opcode_name(KNO_CDR_OPCODE,"OP_CDR");
  set_opcode_name(KNO_LENGTH_OPCODE,"OP_LENGTH");
  set_opcode_name(KNO_QCHOICE_OPCODE,"OP_QCHOICE");
  set_opcode_name(KNO_CHOICE_SIZE_OPCODE,"OP_CHOICESIZE");
  set_opcode_name(KNO_PICKOIDS_OPCODE,"OP_PICKOIDS");
  set_opcode_name(KNO_PICKSTRINGS_OPCODE,"OP_PICKSTRINGS");
  set_opcode_name(KNO_PICKONE_OPCODE,"OP_PICKONE");
  set_opcode_name(KNO_IFEXISTS_OPCODE,"OP_IFEXISTS");
  set_opcode_name(KNO_MINUS1_OPCODE,"OP_MINUS1");
  set_opcode_name(KNO_PLUS1_OPCODE,"OP_PLUS1");
  set_opcode_name(KNO_NUMBERP_OPCODE,"OP_NUMBERP");
  set_opcode_name(KNO_ZEROP_OPCODE,"OP_ZEROP");
  set_opcode_name(KNO_VECTORP_OPCODE,"OP_VECTORP");
  set_opcode_name(KNO_PAIRP_OPCODE,"OP_PAIRP");
  set_opcode_name(KNO_EMPTY_LISTP_OPCODE,"OP_NILP");
  set_opcode_name(KNO_STRINGP_OPCODE,"OP_STRINGP");
  set_opcode_name(KNO_OIDP_OPCODE,"OP_OIDP");
  set_opcode_name(KNO_SYMBOLP_OPCODE,"OP_SYMBOLP");
  set_opcode_name(KNO_FIRST_OPCODE,"OP_FIRST");
  set_opcode_name(KNO_SECOND_OPCODE,"OP_SECOND");
  set_opcode_name(KNO_THIRD_OPCODE,"OP_THIRD");
  set_opcode_name(KNO_CADR_OPCODE,"OP_CADR");
  set_opcode_name(KNO_CDDR_OPCODE,"OP_CDDR");
  set_opcode_name(KNO_CADDR_OPCODE,"OP_CADDR");
  set_opcode_name(KNO_CDDDR_OPCODE,"OP_CDDDR");
  set_opcode_name(KNO_TONUMBER_OPCODE,"OP_2NUMBER");
  set_opcode_name(KNO_NUMEQ_OPCODE,"OP_NUMEQ");
  set_opcode_name(KNO_GT_OPCODE,"OP_GT");
  set_opcode_name(KNO_GTE_OPCODE,"OP_GTE");
  set_opcode_name(KNO_LT_OPCODE,"OP_LT");
  set_opcode_name(KNO_LTE_OPCODE,"OP_LTE");
  set_opcode_name(KNO_PLUS_OPCODE,"OP_PLUS");
  set_opcode_name(KNO_MINUS_OPCODE,"OP_MINUS");
  set_opcode_name(KNO_TIMES_OPCODE,"OP_MULT");
  set_opcode_name(KNO_FLODIV_OPCODE,"OP_FLODIV");
  set_opcode_name(KNO_IDENTICAL_OPCODE,"OP_IDENTICALP");
  set_opcode_name(KNO_OVERLAPS_OPCODE,"OP_OVERLAPSP");
  set_opcode_name(KNO_CONTAINSP_OPCODE,"OP_CONTAINSP");
  set_opcode_name(KNO_UNION_OPCODE,"OP_UNION");
  set_opcode_name(KNO_INTERSECT_OPCODE,"OP_INTERSECTION");
  set_opcode_name(KNO_DIFFERENCE_OPCODE,"OP_DIFFERENCE");
  set_opcode_name(KNO_EQ_OPCODE,"OP_EQP");
  set_opcode_name(KNO_EQV_OPCODE,"OP_EQVP");
  set_opcode_name(KNO_EQUAL_OPCODE,"OP_EQUALP");
  set_opcode_name(KNO_ELT_OPCODE,"OP_SEQELT");
  set_opcode_name(KNO_ASSERT_OPCODE,"OP_ASSERT");
  set_opcode_name(KNO_RETRACT_OPCODE,"OP_RETRACT");
  set_opcode_name(KNO_GET_OPCODE,"OP_FGET");
  set_opcode_name(KNO_TEST_OPCODE,"OP_FTEST");
  set_opcode_name(KNO_ADD_OPCODE,"OP_ADD");
  set_opcode_name(KNO_DROP_OPCODE,"OP_DROP");
  set_opcode_name(KNO_XREF_OPCODE,"OP_XREF");
  set_opcode_name(KNO_PRIMGET_OPCODE,"OP_PGET");
  set_opcode_name(KNO_PRIMTEST_OPCODE,"OP_PTEST");
  set_opcode_name(KNO_STORE_OPCODE,"OP_PSTORE");
}

KNO_EXPORT lispval kno_get_opcode(u8_string name)
{
  int i = 0; while (i<kno_opcodes_length) {
    u8_string opname = kno_opcode_names[i];
    if ((opname)&&(strcasecmp(name,opname)==0))
      return KNO_OPCODE(i);
    else i++;}
  return KNO_FALSE;
}

static int unparse_opcode(u8_output out,lispval opcode)
{
  int opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if (opcode_offset>kno_opcodes_length) {
    u8_printf(out,"#<INVALIDOPCODE>");
    return 1;}
  else if (kno_opcode_names[opcode_offset]==NULL) {
    u8_printf(out,"#<OPCODE_0x%x>",opcode_offset);
    return 1;}
  else {
    u8_printf(out,"#<OPCODE_%s>",kno_opcode_names[opcode_offset]);
    return 1;}
}

static int validate_opcode(lispval opcode)
{
  int opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if ((opcode_offset>=0) && (opcode_offset<kno_opcodes_length))
    return 1;
  else return 0;
}

static u8_string opcode_name(lispval opcode)
{
  int opcode_offset = (KNO_GET_IMMEDIATE(opcode,kno_opcode_type));
  if ((opcode_offset<kno_opcodes_length) &&
      (kno_opcode_names[opcode_offset]))
    return kno_opcode_names[opcode_offset];
  else return "anonymous_opcode";
}

/* Initialization */

void kno_init_opcodes_c()
{
  if (opcodes_initialized) return;
  else opcodes_initialized = 1;

  kno_type_names[kno_opcode_type]=_("opcode");
  kno_unparsers[kno_opcode_type]=unparse_opcode;
  kno_immediate_checkfns[kno_opcode_type]=validate_opcode;

  memset(kno_opcode_names,0,sizeof(kno_opcode_names));

  init_opcode_names();
}
