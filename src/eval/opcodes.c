/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
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

static fdtype op_eval(fdtype x,fd_lispenv env,int tail);

FD_FASTOP fdtype op_eval_body(fdtype body,fd_lispenv env)
{
  fdtype result=FD_VOID;
  if (FD_CODEP(body)) {
    int j=0, n_sub_exprs=FD_CODE_LENGTH(body);
    fdtype *sub_exprs=FD_CODE_DATA(body);
    while (j<n_sub_exprs) {
      fdtype sub_expr=sub_exprs[j++];
      fd_decref(result);
      result=op_eval(sub_expr,env,j==n_sub_exprs);
      if (FD_ABORTED(result))
        return result;}
    return result;}
  else if (body == FD_EMPTY_LIST)
    return FD_VOID;
  else while (FD_PAIRP(body)) {
      fdtype subex = FD_CAR(body), next = FD_CDR(body);
      if (FD_PAIRP(next)) {
        fdtype v = op_eval(subex,env,0);
        if (FD_ABORTED(v)) return v;
        fd_decref(v);
        body = next;}
      else return op_eval(subex,env,1);}
  return fd_err(fd_SyntaxError,"op_eval_body",NULL,body);
}

FD_FASTOP fdtype _pop_arg(fdtype *scan)
{
  fdtype expr = *scan;
  if (FD_PAIRP(expr)) {
    fdtype arg = FD_CAR(expr);
    *scan = FD_CDR(expr);
    return arg;}
  else return FD_VOID;
}

#define pop_arg(args) (_pop_arg(&args))

/* Opcode names */

u8_string fd_opcode_names[0x800]={NULL};

int fd_opcodes_length = 0x800;

static int unparse_opcode(u8_output out,fdtype opcode)
{
  int opcode_offset = (FD_GET_IMMEDIATE(opcode,fd_opcode_type));
  if (opcode_offset>fd_opcodes_length) {
    u8_printf(out,"#<INVALIDOPCODE>");
    return 1;}
  else if (fd_opcode_names[opcode_offset]==NULL) {
    u8_printf(out,"#<OPCODE_0x%x>",opcode_offset);
    return 1;}
  else {
    u8_printf(out,"#<OPCODE_%s>",fd_opcode_names[opcode_offset]);
    return 1;}
}

static int validate_opcode(fdtype opcode)
{
  int opcode_offset = (FD_GET_IMMEDIATE(opcode,fd_opcode_type));
  if ((opcode_offset>=0) && (opcode_offset<fd_opcodes_length))
    return 1;
  else return 0;
}

static u8_string opcode_name(fdtype opcode)
{
  int opcode_offset = (FD_GET_IMMEDIATE(opcode,fd_opcode_type));
  if ((opcode_offset<fd_opcodes_length) &&
      (fd_opcode_names[opcode_offset]))
    return fd_opcode_names[opcode_offset];
  else return "anonymous_opcode";
}

static fdtype pickoids_opcode(fdtype arg1);
static fdtype pickstrings_opcode(fdtype arg1);
static fdtype pickone_opcode(fdtype normal);

static fdtype nd1_dispatch(fdtype opcode,fdtype arg1)
{
  switch (opcode) {
  case FD_AMBIGP_OPCODE: 
    if (FD_CHOICEP(arg1)) return FD_TRUE;
    else return FD_FALSE;
  case FD_SINGLETONP_OPCODE: 
    if (FD_EMPTY_CHOICEP(arg1)) return FD_FALSE;
    else if (FD_CHOICEP(arg1)) return FD_FALSE;
    else return FD_TRUE;
  case FD_FAILP_OPCODE: 
    if (arg1==FD_EMPTY_CHOICE) return FD_TRUE;
    else return FD_FALSE;
  case FD_EXISTSP_OPCODE: 
    if (arg1==FD_EMPTY_CHOICE) return FD_FALSE;
    else return FD_TRUE;
  case FD_SINGLETON_OPCODE:
    if (FD_CHOICEP(arg1)) return FD_EMPTY_CHOICE;
    else return fd_incref(arg1);
  case FD_CAR_OPCODE: 
    if (FD_EMPTY_CHOICEP(arg1)) return arg1;
    else if (FD_PAIRP(arg1))
      return fd_incref(FD_CAR(arg1));
    else if (FD_CHOICEP(arg1)) {
      fdtype results = FD_EMPTY_CHOICE;
      FD_DO_CHOICES(arg,arg1)
	if (FD_PAIRP(arg)) {
	  fdtype car = FD_CAR(arg); fd_incref(car);
	  FD_ADD_TO_CHOICE(results,car);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("pair"),"CAR opcode",arg);}
      return results;}
    else return fd_type_error(_("pair"),"CAR opcode",arg1);
  case FD_CDR_OPCODE: 
    if (FD_EMPTY_CHOICEP(arg1)) return arg1;
    else if (FD_PAIRP(arg1))
      return fd_incref(FD_CDR(arg1));
    else if (FD_CHOICEP(arg1)) {
      fdtype results = FD_EMPTY_CHOICE;
      FD_DO_CHOICES(arg,arg1)
	if (FD_PAIRP(arg)) {
	  fdtype cdr = FD_CDR(arg); fd_incref(cdr);
	  FD_ADD_TO_CHOICE(results,cdr);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("pair"),"CDR opcode",arg);}
      return results;}
    else return fd_type_error(_("pair"),"CDR opcode",arg1);
  case FD_LENGTH_OPCODE:
    if (arg1==FD_EMPTY_CHOICE) return FD_EMPTY_CHOICE;
    else if (FD_CHOICEP(arg1)) {
      fdtype results = FD_EMPTY_CHOICE;
      FD_DO_CHOICES(arg,arg1) {
	if (FD_SEQUENCEP(arg)) {
	  int len = fd_seq_length(arg);
	  fdtype dlen = FD_INT(len);
	  FD_ADD_TO_CHOICE(results,dlen);}
	else {
	  fd_decref(results);
	  return fd_type_error(_("sequence"),"LENGTH opcode",arg);}}
      return fd_simplify_choice(results);}
    else if (FD_SEQUENCEP(arg1))
      return FD_INT(fd_seq_length(arg1));
    else return fd_type_error(_("sequence"),"LENGTH opcode",arg1);
  case FD_QCHOICE_OPCODE:
    if (FD_CHOICEP(arg1)) {
      fd_incref(arg1);
      return fd_init_qchoice(NULL,arg1);}
    else if (FD_PRECHOICEP(arg1)) 
      return fd_init_qchoice(NULL,fd_make_simple_choice(arg1));
     else if (FD_EMPTY_CHOICEP(arg1))
      return fd_init_qchoice(NULL,FD_EMPTY_CHOICE);
    else return fd_incref(arg1);
  case FD_CHOICE_SIZE_OPCODE:
    if (FD_CHOICEP(arg1)) {
      int sz = FD_CHOICE_SIZE(arg1);
      return FD_INT(sz);}
    else if (FD_PRECHOICEP(arg1)) {
      fdtype simple = fd_make_simple_choice(arg1);
      int size = FD_CHOICE_SIZE(simple);
      fd_decref(simple);
      return FD_INT(size);}
    else if (FD_EMPTY_CHOICEP(arg1))
      return FD_INT(0);
    else return FD_INT(1);
  case FD_PICKOIDS_OPCODE:
    return pickoids_opcode(arg1);
  case FD_PICKSTRINGS_OPCODE:
    return pickstrings_opcode(arg1);
  case FD_PICKONE_OPCODE:
    if (FD_CHOICEP(arg1))
      return pickone_opcode(arg1);
    else return fd_incref(arg1);
  case FD_IFEXISTS_OPCODE:
    if (FD_EMPTY_CHOICEP(arg1))
      return FD_VOID;
    else return fd_incref(arg1);
  case FD_FIXCHOICE_OPCODE:
    if (FD_PRECHOICEP(arg1))
      return fd_simplify_choice(arg1);
    else return fd_incref(arg1);
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,FD_VOID);
  }
}

static fdtype first_opcode(fdtype arg1)
{
  if (FD_PAIRP(arg1)) return fd_incref(FD_CAR(arg1));
  else if (FD_VECTORP(arg1))
    if (FD_VECTOR_LENGTH(arg1)>0)
      return fd_incref(FD_VECTOR_REF(arg1,0));
    else return fd_err(fd_RangeError,"FD_FIRST_OPCODE",NULL,arg1);
  else if (FD_STRINGP(arg1)) {
    u8_string data = FD_STRDATA(arg1); int c = u8_sgetc(&data);
    if (c<0) return fd_err(fd_RangeError,"FD_FIRST_OPCODE",NULL,arg1);
    else return FD_CODE2CHAR(c);}
  else return fd_seq_elt(arg1,0);
}

static fdtype eltn_opcode(fdtype arg1,int n,u8_context opname)
{
  if (FD_VECTORP(arg1))
    if (FD_VECTOR_LENGTH(arg1)>n)
      return fd_incref(FD_VECTOR_REF(arg1,n));
    else return fd_err(fd_RangeError,opname,NULL,arg1);
  else return fd_seq_elt(arg1,n);
}

static fdtype d1_dispatch(fdtype opcode,fdtype arg1)
{
  int delta = 1;
  switch (opcode) {
  case FD_MINUS1_OPCODE: delta = -1;
  case FD_PLUS1_OPCODE: 
    if (FD_FIXNUMP(arg1)) {
      long long iarg = FD_FIX2INT(arg1);
      return FD_INT(iarg+delta);}
    else if (FD_NUMBERP(arg1))
      return fd_plus(arg1,FD_FIX2INT(-1));
    else return fd_type_error(_("number"),"opcode 1+/-",arg1);
  case FD_NUMBERP_OPCODE: 
    if (FD_NUMBERP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_ZEROP_OPCODE: 
    if (arg1==FD_INT(0)) return FD_TRUE; else return FD_FALSE;
  case FD_VECTORP_OPCODE: 
    if (FD_VECTORP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_PAIRP_OPCODE: 
    if (FD_PAIRP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_EMPTY_LISTP_OPCODE: 
    if (arg1==FD_EMPTY_LIST) return FD_TRUE; else return FD_FALSE;
  case FD_STRINGP_OPCODE: 
    if (FD_STRINGP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_OIDP_OPCODE: 
    if (FD_OIDP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_SYMBOLP_OPCODE: 
    if (FD_SYMBOLP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_FIXNUMP_OPCODE: 
    if (FD_FIXNUMP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_FLONUMP_OPCODE: 
    if (FD_FLONUMP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_TABLEP_OPCODE: 
    if (FD_TABLEP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_SEQUENCEP_OPCODE: 
    if (FD_SEQUENCEP(arg1)) return FD_TRUE; else return FD_FALSE;
  case FD_CADR_OPCODE: {
    fdtype cdr = FD_CDR(arg1);
    if (FD_PAIRP(cdr)) return fd_incref(FD_CAR(cdr));
    else return fd_err(fd_RangeError,"FD_CADR",NULL,arg1);}
  case FD_CDDR_OPCODE: {
    fdtype cdr = FD_CDR(arg1);
    if (FD_PAIRP(cdr)) return fd_incref(FD_CDR(cdr));
    else return fd_err(fd_RangeError,"FD_CADR",NULL,arg1);}
  case FD_CADDR_OPCODE: {
    fdtype cdr = FD_CDR(arg1);
    if (FD_PAIRP(cdr)) {
      fdtype cddr = FD_CDR(cdr);
      if (FD_PAIRP(cddr)) return fd_incref(FD_CAR(cdr));
      else return fd_err(fd_RangeError,"FD_CADDR",NULL,arg1);}
    else return fd_err(fd_RangeError,"FD_CADDR",NULL,arg1);}
  case FD_CDDDR_OPCODE: {
    fdtype cdr = FD_CDR(arg1);
    if (FD_PAIRP(cdr)) {
      fdtype cddr = FD_CDR(cdr);
      if (FD_PAIRP(cddr)) return fd_incref(FD_CDR(cdr));
      else return fd_err(fd_RangeError,"FD_DDDR",NULL,arg1);}
    else return fd_err(fd_RangeError,"FD_CDDDR",NULL,arg1);}
  case FD_FIRST_OPCODE:
    return first_opcode(arg1);
  case FD_SECOND_OPCODE:
    return eltn_opcode(arg1,1,"FD_SECOND_OPCODE");
  case FD_THIRD_OPCODE:
    return eltn_opcode(arg1,2,"FD_THIRD_OPCODE");
  case FD_TONUMBER_OPCODE:
    if (FD_FIXNUMP(arg1)) return arg1;
    else if (FD_NUMBERP(arg1)) return fd_incref(arg1);
    else if (FD_STRINGP(arg1))
      return fd_string2number(FD_STRDATA(arg1),10);
    else if (FD_CHARACTERP(arg1))
      return FD_INT(FD_CHARCODE(arg1));
    else return fd_type_error(_("number|string"),"opcode ->number",arg1);
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,FD_VOID);
  }
}

static fdtype d1_call(fdtype opcode,fdtype arg1)
{
  if (FD_EMPTY_CHOICEP(arg1))
    return FD_EMPTY_CHOICE;
  else if (!(FD_CONSP(arg1)))
    return d1_dispatch(opcode,arg1);
  else if (!(FD_CHOICEP(arg1))) {
    fdtype result = d1_dispatch(opcode,arg1);
    fd_decref(arg1);
    return result;}
  else {
    fdtype results = FD_EMPTY_CHOICE;
    FD_DO_CHOICES(arg,arg1) {
      fdtype result = d1_dispatch(opcode,arg);
      if (FD_ABORTED(result)) {
        fd_decref(results); fd_decref(arg1);
        FD_STOP_DO_CHOICES;
        return result;}
      else {FD_ADD_TO_CHOICE(results,result);}}
    fd_decref(arg1);
    return results;}
}

static fdtype elt_opcode(fdtype arg1,fdtype arg2)
{
  if ((FD_SEQUENCEP(arg1)) && (FD_INTP(arg2))) {
    fdtype result;
    long long off = FD_FIX2INT(arg2), len = fd_seq_length(arg1);
    if (off<0) off = len+off;
    result = fd_seq_elt(arg1,off);
    if (result == FD_TYPE_ERROR)
      return fd_type_error(_("sequence"),"FD_OPCODE_ELT",arg1);
    else if (result == FD_RANGE_ERROR) {
      char buf[32];
      sprintf(buf,"%lld",off);
      return fd_err(fd_RangeError,"FD_OPCODE_ELT",u8_strdup(buf),arg1);}
    else return result;}
  else if (!(FD_SEQUENCEP(arg1)))
    return fd_type_error(_("sequence"),"FD_OPCODE_ELT",arg1);
  else return fd_type_error(_("fixnum"),"FD_OPCODE_ELT",arg2);
}

static fdtype try_op(fdtype exprs,fd_lispenv env)
{
  while (FD_PAIRP(exprs)) {
    fdtype expr = pop_arg(exprs);
    if (FD_EMPTY_LISTP(exprs))
      return op_eval(expr,env,1);
    else {
      fdtype val  = op_eval(expr,env,0);
      if (FD_ABORTED(val)) return val;
      else if (!(FD_EMPTY_CHOICEP(val)))
        return val;
      else fd_decref(val);}}
  return FD_EMPTY_CHOICE;
}

static fdtype and_op(fdtype exprs,fd_lispenv env)
{
  while (FD_PAIRP(exprs)) {
    fdtype expr = pop_arg(exprs);
    if (FD_EMPTY_LISTP(exprs))
      return op_eval(expr,env,1);
    else {
      fdtype val  = op_eval(expr,env,0);
      if (FD_ABORTED(val)) return val;
      else if (FD_FALSEP(val))
        return FD_FALSE;
      else fd_decref(val);}}
  return FD_TRUE;
}

static fdtype or_op(fdtype exprs,fd_lispenv env)
{
  while (FD_PAIRP(exprs)) {
    fdtype expr = pop_arg(exprs);
    if (FD_EMPTY_LISTP(exprs))
      return op_eval(expr,env,1);
    else {
      fdtype val  = op_eval(expr,env,0);
      if (FD_ABORTED(val)) return val;
      else if (!(FD_FALSEP(val)))
        return val;
      else {}}}
  return FD_FALSE;
}

static fdtype d2_dispatch(fdtype opcode,fdtype arg1,fdtype arg2)
{
  switch (opcode) {
  case FD_NUMEQ_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1)) == (FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)==0) return FD_TRUE;
    else return FD_FALSE;
  case FD_GT_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))>(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)>0) return FD_TRUE;
    else return FD_FALSE;
  case FD_GTE_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))>=(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)>=0) return FD_TRUE;
    else return FD_FALSE;
  case FD_LT_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))<(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)<0) return FD_TRUE;
    else return FD_FALSE;
  case FD_LTE_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2))) 
      if ((FD_FIX2INT(arg1))<=(FD_FIX2INT(arg2)))
	return FD_TRUE;
      else return FD_FALSE;
    else if (fd_numcompare(arg1,arg2)<=0) return FD_TRUE;
    else return FD_FALSE;
  case FD_PLUS_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      long long m = FD_FIX2INT(arg1), n = FD_FIX2INT(arg2);
      return FD_INT(m+n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x = FD_FLONUM(arg1), y = FD_FLONUM(arg2);
      return fd_init_double(NULL,x+y);}
    else return fd_plus(arg1,arg2);
  case FD_MINUS_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      long long m = FD_FIX2INT(arg1), n = FD_FIX2INT(arg2);
      return FD_INT(m-n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x = FD_FLONUM(arg1), y = FD_FLONUM(arg2);
      return fd_init_double(NULL,x-y);}
    else return fd_subtract(arg1,arg2);
  case FD_TIMES_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      long long m = FD_FIX2INT(arg1), n = FD_FIX2INT(arg2);
      return FD_INT(m*n);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x = FD_FLONUM(arg1), y = FD_FLONUM(arg2);
      return fd_init_double(NULL,x*y);}
    else return fd_multiply(arg1,arg2);
  case FD_FLODIV_OPCODE:
    if ((FD_FIXNUMP(arg1)) && (FD_FIXNUMP(arg2)))  {
      long long m = FD_FIX2INT(arg1), n = FD_FIX2INT(arg2);
      double x = (double)m, y = (double)n;
      return fd_init_double(NULL,x/y);}
    else if ((FD_FLONUMP(arg1)) && (FD_FLONUMP(arg2))) {
      double x = FD_FLONUM(arg1), y = FD_FLONUM(arg2);
      return fd_init_double(NULL,x/y);}
    else {
      double x = fd_todouble(arg1), y = fd_todouble(arg2);
      return fd_init_double(NULL,x/y);}
  case FD_ELT_OPCODE:
    return elt_opcode(arg1,arg2);
  case FD_CONS_OPCODE:
    return fd_init_pair(NULL,arg1,arg2);
  case FD_EQ_OPCODE: {
    if (arg1==arg2) return FD_TRUE; else return FD_FALSE;
    break;}
  case FD_EQV_OPCODE: {
    if (arg1==arg2) return FD_TRUE;
    else if ((FD_NUMBERP(arg1)) && (FD_NUMBERP(arg2)))
      if (fd_numcompare(arg1,arg2)==0)
	return FD_TRUE; else return FD_FALSE;
    else return FD_FALSE;
    break;}
  case FD_EQUAL_OPCODE: {
    if ((FD_ATOMICP(arg1)) && (FD_ATOMICP(arg2)))
      if (arg1==arg2) return FD_TRUE; else return FD_FALSE;
    else if (FD_EQUAL(arg1,arg2)) return FD_TRUE;
    else return FD_FALSE;
    break;}
  default:
    return fd_err(_("Invalid opcode"),"opcode eval",NULL,FD_VOID);
  }
}

FD_FASTOP int numeric_argp(fdtype x)
{
  /* This checks if there is a type error.
     The empty choice isn't a type error since it will just
     generate an empty choice as a result. */
  if ((FD_EMPTY_CHOICEP(x))||(FD_FIXNUMP(x)))
    return 1;
  else if (!(FD_CONSP(x)))
    return 0;
  else switch (FD_CONSPTR_TYPE(x)) {
    case fd_flonum_type: case fd_bigint_type:
    case fd_rational_type: case fd_complex_type:
      return 1;
    case fd_choice_type: case fd_prechoice_type: {
      FD_DO_CHOICES(a,x) {
        if (FD_FIXNUMP(a)) {}
        else if (FD_EXPECT_TRUE(FD_NUMBERP(a))) {}
        else {
          FD_STOP_DO_CHOICES;
          return 0;}}
      return 1;}
    default:
      return 0;}
}

static fdtype d2_call(fdtype opcode,fdtype arg1,fdtype arg2)
{
  fdtype results = FD_EMPTY_CHOICE;
  FD_DO_CHOICES(a1,arg1) {
    {FD_DO_CHOICES(a2,arg2) {
        fdtype result = d2_dispatch(opcode,a1,a2);
        /* If we need to abort due to an error, we need to pop out of
           two choice loops.  So on the inside, we decref results and
           replace it with the error object.  We then break and
           do FD_STOP_DO_CHOICES (for potential cleanup). */
        if (FD_ABORTED(result)) {
          fd_decref(results); results = result;
          FD_STOP_DO_CHOICES; break;}
        else {FD_ADD_TO_CHOICE(results,result);}}}
    /* If the inner loop aborted due to an error, results is now bound
       to the error, so we just FD_STOP_DO_CHOICES (this time for the
       outer loop) and break; */
    if (FD_ABORTED(results)) {
      FD_STOP_DO_CHOICES; break;}}
  fd_decref(arg1); fd_decref(arg2);
  return results;
}

static fdtype nd2_dispatch(fdtype opcode,fdtype arg1,fdtype arg2)
{
  fdtype result = FD_ERROR_VALUE;
  if (FD_PRECHOICEP(arg2)) arg2 = fd_simplify_choice(arg2);
  if (FD_PRECHOICEP(arg1)) arg1 = fd_simplify_choice(arg1);
  fdtype argv[2]={arg1,arg2};
  if (FD_ABORTED(arg2)) result = arg2;
  else if (FD_VOIDP(arg2)) {
    result = fd_err(fd_VoidArgument,"OPCODE setop",NULL,opcode);}
  else switch (opcode) {
    case FD_IDENTICAL_OPCODE:
      if (arg1==arg2) result = FD_TRUE;
      else if (FD_EQUAL(arg1,arg2)) result = FD_TRUE;
      else result = FD_FALSE;
      break;
    case FD_OVERLAPS_OPCODE:
      if (arg1==arg2) result = FD_TRUE;
      else if (fd_overlapp(arg1,arg2)) result = FD_TRUE;
      else result = FD_FALSE;
      break;
    case FD_CONTAINSP_OPCODE:
      if (fd_containsp(arg1,arg2)) result = FD_TRUE;
      else result = FD_FALSE;
      break;
    case FD_INTERSECT_OPCODE:
      if ((FD_EMPTY_CHOICEP(arg1)) || (FD_EMPTY_CHOICEP(arg2)))
        result = FD_EMPTY_CHOICE;
      else result = fd_intersection(argv,2);
      break;
    case FD_UNION_OPCODE:
      if (FD_EMPTY_CHOICEP(arg1)) result = fd_incref(arg2);
      else if (FD_EMPTY_CHOICEP(arg2)) result = fd_incref(arg1);
      else result = fd_union(argv,2);
      break;
    case FD_DIFFERENCE_OPCODE:
      if ((FD_EMPTY_CHOICEP(arg1)) || (FD_EMPTY_CHOICEP(arg2)))
        result = fd_incref(arg1);
      else result = fd_difference(arg1,arg2);
      break;
    case FD_CHOICEREF_OPCODE:
      if (!(FD_FIXNUMP(arg2))) {
        fd_decref(arg1);
        return fd_err(fd_SyntaxError,"choiceref_opcode",NULL,arg2);}
      else {
        int i = FD_FIX2INT(arg2);
        if (i<0) return fd_err(fd_SyntaxError,"choiceref_opcode",NULL,arg2);
        if ((i==0)&&(FD_EMPTY_CHOICEP(arg1))) return FD_VOID;
        else if (FD_CHOICEP(arg1)) {
          struct FD_CHOICE *ch = (fd_choice)arg1;
          if (i<ch->choice_size) {
            const fdtype *elts = FD_XCHOICE_DATA(ch);
            fdtype elt = elts[i];
            return fd_incref(elt);}
          else return FD_VOID;}
        else if (i==0) return arg1;
        else {fd_decref(arg1); return FD_VOID;}}
    }
  fd_decref(arg1); fd_decref(arg2);
  return result;
}

static fdtype xref_type_error(fdtype x,fdtype tag)
{
  if (FD_VOIDP(tag))
    fd_seterr(fd_TypeError,"XREF_OPCODE",u8_strdup("compound"),x);
  else fd_seterr(fd_TypeError,"XREF_OPCODE",fd_dtype2string(tag),x);
  return FD_ERROR_VALUE;
}

static fdtype xref_op(struct FD_COMPOUND *c,long long i,fdtype tag)
{
  if ((FD_VOIDP(tag)) || ((c->compound_typetag) == tag)) {
    if ((i>=0) && (i<c->fd_n_elts)) {
      fdtype *values = &(c->compound_0), value;
      if (c->compound_ismutable)
        u8_lock_mutex(&(c->compound_lock));
      value = values[i];
      fd_incref(value);
      if (c->compound_ismutable)
        u8_unlock_mutex(&(c->compound_lock));
      return value;}
    else {
      fd_seterr(fd_RangeError,"xref",NULL,(fdtype)c);
      return FD_ERROR_VALUE;}}
  else return xref_type_error((fdtype)c,tag);
}

static fdtype xref_opcode(fdtype x,long long i,fdtype tag)
{
  if (!(FD_CONSP(x)))
    return xref_type_error(x,tag);
  else if (FD_COMPOUNDP(x))
    return xref_op((struct FD_COMPOUND *)x,i,tag);
  else if (FD_CHOICEP(x)) {
    fdtype results = FD_EMPTY_CHOICE;
    FD_DO_CHOICES(c,x) {
      fdtype r = xref_op((struct FD_COMPOUND *)c,i,tag);
      if (FD_ABORTED(r)) {
        fd_decref(results); results = r;
        FD_STOP_DO_CHOICES;
        break;}
      else {FD_ADD_TO_CHOICE(results,r);}}
    return results;}
  else return fd_err(fd_TypeError,"xref",fd_dtype2string(tag),x);
}

static fdtype until_opcode(fdtype expr,fd_lispenv env)
{
  fdtype params = FD_CDR(expr);
  fdtype test_expr = FD_CAR(params), loop_body = FD_CDR(params);
  if (FD_VOIDP(test_expr))
    return fd_err(fd_SyntaxError,"FD_LOOP_OPCODE",NULL,expr);
  fdtype test_val = op_eval(test_expr,env,0);
  if (FD_ABORTED(test_val)) return test_val;
  else while (FD_FALSEP(test_val)) {
      fdtype body_result=op_eval_body(loop_body,env);
      if (FD_TAILCALLP(body_result)) 
        body_result=fd_finish_call(body_result);
      fd_decref(body_result);
      test_val = op_eval(test_expr,env,0);
      if (FD_ABORTED(test_val)) return test_val;}
  return test_val;
}

static fdtype tableop(fdtype opcode,fdtype arg1,fdtype arg2,fdtype arg3)
{
  if (FD_EMPTY_CHOICEP(arg1)) {
    switch (opcode) {
    case FD_GET_OPCODE: case FD_PRIMGET_OPCODE:
      return FD_EMPTY_CHOICE;
    case FD_TEST_OPCODE: case FD_PRIMTEST_OPCODE:
      return FD_FALSE;
    case FD_STORE_OPCODE:
    case FD_RETRACT_OPCODE: case FD_DROP_OPCODE:
    case FD_ASSERT_OPCODE: case FD_ADD_OPCODE:
      return FD_VOID;
    default:
      return FD_VOID;}}
  else if (opcode == FD_TEST_OPCODE)
    return fd_ftest(arg1,arg2,arg3);
  else if (opcode == FD_PRIMTEST_OPCODE) {
    int rv = fd_test(arg1,arg2,arg3);
    if (rv<0) return FD_ERROR_VALUE;
    else if (rv>0) return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_CHOICEP(arg1)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(tbl,arg1) {
      fdtype partial=tableop(opcode,tbl,arg2,arg3);
      if (FD_ABORTED(partial)) {
        fd_decref(results);
        FD_STOP_DO_CHOICES;
        return partial;}
      else {
        FD_ADD_TO_CHOICE(results,partial);}}
    return results;}
  else {
    int rv=0;
    switch (opcode) {
    case FD_PRIMGET_OPCODE:
      return fd_get(arg1,arg2,arg3);
    case FD_PRIMTEST_OPCODE:
      rv=fd_test(arg1,arg2,arg3);
      if (rv<0) return FD_ERROR_VALUE;
      else if (rv==0) return FD_FALSE;
      else return FD_TRUE;
    case FD_ADD_OPCODE:
      rv=fd_add(arg1,arg2,arg3);
      if (rv<0) return FD_ERROR_VALUE;
      else if (rv==0) return FD_FALSE;
      else return FD_TRUE;
    case FD_DROP_OPCODE:
      rv=fd_drop(arg1,arg2,arg3);
      if (rv<0) return FD_ERROR_VALUE;
      else if (rv==0) return FD_FALSE;
      else return FD_TRUE;
    case FD_STORE_OPCODE:
      rv=fd_store(arg1,arg2,arg3);
      if (rv<0) return FD_ERROR_VALUE;
      else if (rv==0) return FD_FALSE;
      else return FD_TRUE;
    case FD_TEST_OPCODE:
      if (FD_OIDP(arg1))
        rv=fd_frame_test(arg1,arg2,arg3);
      else rv=fd_test(arg1,arg2,arg3);
      if (rv<0) return FD_ERROR_VALUE;
      else if (rv==0) return FD_FALSE;
      else return FD_TRUE;
    case FD_GET_OPCODE:
      return fd_fget(arg1,arg2);
    case FD_ASSERT_OPCODE:
      if (FD_OIDP(arg1))
        rv=fd_assert(arg1,arg2,arg3);
      else rv=fd_add(arg1,arg2,arg3);
      if (rv<0) return FD_ERROR_VALUE;
      else if (rv==0) return FD_FALSE;
      else return FD_TRUE;
    case FD_RETRACT_OPCODE:
      if (FD_OIDP(arg1))
        rv=fd_retract(arg1,arg2,arg3);
      else rv=fd_drop(arg1,arg2,arg3);
      if (rv<0) return FD_ERROR_VALUE;
      else if (rv==0) return FD_FALSE;
      else return FD_TRUE;
    default:
      return FD_VOID;}}
}

static fdtype assignop(fd_lispenv env,fdtype var,fdtype expr,fdtype combiner)
{
  fdtype value = op_eval(expr,env,0);
  if (FD_ABORTED(value))
    return value;
  else if (FD_LEXREFP(var)) {
    int up = FD_LEXREF_UP(var);
    int across = FD_LEXREF_ACROSS(var);
    fd_lispenv scan = ( (env->env_copy) ? (env->env_copy) : (env) );
    while ((up)&&(scan)) {
      fd_lispenv parent = scan->env_parent;
      if ((parent) && (parent->env_copy))
        scan = parent->env_copy;
      else scan = parent;
      up--;}
    if (FD_EXPECT_TRUE(scan!=NULL)) {
      fdtype bindings = scan->env_bindings;
      if (FD_EXPECT_TRUE(FD_SCHEMAPP(bindings))) {
        struct FD_SCHEMAP *skimap = (struct FD_SCHEMAP *)bindings;
        if (FD_EXPECT_TRUE(across<skimap->schema_length)) {
          fdtype *values = skimap->schema_values;
          fdtype cur     = values[across];
          switch (combiner) {
          case FD_VOID: case FD_FALSE:
            values[across]=value;
            fd_decref(cur); cur=FD_VOID;
            return FD_VOID;
          case FD_UNION_OPCODE:
            FD_ADD_TO_CHOICE(values[across],value);
            return FD_VOID;
          case FD_TRUE:
            if ((cur == FD_DEFAULT_VALUE) ||
                (cur == FD_UNBOUND) ||
                (cur == FD_VOID) ||
                (cur == FD_NULL))
              values[across]=value;
            return FD_VOID;
          case FD_PLUS_OPCODE:
            if ( (FD_FIXNUMP(value)) && (FD_FIXNUMP(cur)) ) {
              long long ic=FD_FIX2INT(cur), ip=FD_FIX2INT(value);
              values[across]=FD_MAKE_FIXNUM(ic+ip);
              return FD_VOID;}
            else {
              fdtype result=fd_plus(cur,value);
              fd_decref(cur); fd_decref(value);
              if (FD_ABORTED(result))
                return result;
              else skimap->schema_values[across]=result;
              return FD_VOID;}
          case FD_MINUS_OPCODE:
            if ( (FD_FIXNUMP(value)) && (FD_FIXNUMP(cur)) ) {
              long long ic=FD_FIX2INT(cur), im=FD_FIX2INT(value);
              values[across]=FD_MAKE_FIXNUM(ic-im);
              return FD_VOID;}
            else {
              fdtype result=fd_subtract(cur,value);
              fd_decref(cur); fd_decref(value);
              if (FD_ABORTED(result))
                return result;
              else values[across]=result;
              return FD_VOID;}}}}}
    u8_string lexref=u8_mkstring("up%d/across%d",up,across);
    fdtype env_copy=(fdtype)fd_copy_env(env);
    return fd_err("BadLexref","ASSIGN_OPCODE",lexref,env_copy);}
  else if ((FD_PAIRP(var)) &&
           (FD_SYMBOLP(FD_CAR(var))) &&
           (FD_TABLEP(FD_CDR(var)))) {
    fdtype table=FD_CDR(var), sym=FD_CAR(var);
    int rv=fd_store(table,sym,value);
    fd_decref(value);
    if (rv<0)
      return fd_err(fd_SyntaxError,"ASSIGN_OPCODE",NULL,expr);
    else return FD_VOID;}
  return fd_err(fd_SyntaxError,"ASSIGN_OPCODE",NULL,expr);
}

static fdtype bindop(fd_lispenv env,fdtype vars,fdtype inits,fdtype body)
{
  int i=0, n=FD_VECTOR_LENGTH(vars);
  struct FD_SCHEMAP bindings;
  struct FD_ENVIRONMENT envstruct, *inner_env=&envstruct;
  fdtype *exprs=FD_VECTOR_DATA(inits);
  fdtype values[n]; /* fdtype *values=fd_alloca(n); */
  FD_INIT_STATIC_CONS(&bindings,fd_schemap_type);
  bindings.schema_length=n;
  bindings.table_schema=FD_VECTOR_DATA(vars);
  bindings.schema_values=values;
  bindings.schemap_onstack=1;
  FD_INIT_STATIC_CONS(&envstruct,fd_environment_type);
  envstruct.env_bindings=(fdtype)&bindings;
  envstruct.env_exports=FD_VOID;
  envstruct.env_parent=env;
  while (i<n) {
    fdtype val_expr=exprs[i];
    fdtype val=op_eval(val_expr,inner_env,0);
    if (FD_ABORTED(val)) {
      while (i>=0) {fd_decref(values[i]); i--;}
      free_environment(inner_env);
      return val;}
    else values[i++]=val;}
  fdtype result = op_eval_body(body,inner_env);
  free_environment(inner_env);
  return result;
}

static fdtype opcode_dispatch(fdtype opcode,fdtype expr,fd_lispenv env)
{
  fdtype args = FD_CDR(expr);
  switch (opcode) {
  case FD_QUOTE_OPCODE:
    return fd_incref(pop_arg(args));
  case FD_NOT_OPCODE: {
    fdtype arg_val = op_eval(pop_arg(args),env,0);
    if (FD_FALSEP(arg_val))
      return FD_TRUE;
    else {
      fd_decref(arg_val);
      return FD_FALSE;}}
  case FD_BEGIN_OPCODE:
    return op_eval_body(FD_CDR(expr),env);
  case FD_UNTIL_OPCODE:
    return until_opcode(expr,env);
  case FD_BRANCH_OPCODE: {
    fdtype test_expr = pop_arg(args);
    if (FD_VOIDP(test_expr))
      return fd_err(fd_SyntaxError,"FD_BRANCH_OPCODE",NULL,expr);
    fdtype test_val = op_eval(test_expr,env,0);
    if (FD_ABORTED(test_val)) return test_val;
    if (!(FD_FALSEP(test_val))) {
      fdtype then = pop_arg(args);
      U8_MAYBE_UNUSED fdtype ignore = pop_arg(args);
      fd_decref(test_val);
      return op_eval(then,env,1);}
    else {
      pop_arg(args);
      return op_eval(pop_arg(args),env,1);}}
  case FD_SYMREF_OPCODE: {
    fdtype refenv=pop_arg(args);
    fdtype sym=pop_arg(args);
    if (!(FD_SYMBOLP(sym)))
      return fd_err(fd_SyntaxError,"FD_SYMREF_OPCODE/badsym",NULL,expr);
    if (FD_HASHTABLEP(refenv))
      return fd_hashtable_get((fd_hashtable)refenv,sym,FD_UNBOUND);
    else if (FD_ENVIRONMENTP(refenv))
      return fd_symeval(sym,(fd_lispenv)refenv);
    else if (FD_TABLEP(refenv))
      return fd_get(refenv,sym,FD_UNBOUND);
    else return fd_err(fd_SyntaxError,"FD_SYMREF_OPCODE/badenv",NULL,expr);}
  case FD_TRY_OPCODE:
    return try_op(args,env);
  case FD_AND_OPCODE:
    return and_op(args,env);
  case FD_OR_OPCODE:
    return or_op(args,env);
  case FD_ASSIGN_OPCODE: {
    fdtype var = pop_arg(args);
    fdtype val_expr = pop_arg(args);
    fdtype combiner = pop_arg(args);
    return assignop(env,var,val_expr,combiner);}
  case FD_VOID_OPCODE: {
    return FD_VOID;}
  case FD_XREF_OPCODE: {
    fdtype obj_expr = pop_arg(args);
    fdtype off_arg = pop_arg(args);
    if ((FD_VOIDP(obj_expr))||(!(FD_FIXNUMP(off_arg)))) {
      fd_seterr(fd_SyntaxError,"FD_XREF_OPCODE",NULL,expr);
      return FD_ERROR_VALUE;}
    else return xref_opcode(fd_simplify_choice(fasteval(obj_expr,env)),
                            FD_FIX2INT(off_arg),
                            pop_arg(args));}
  case FD_BIND_OPCODE: {
    fdtype vars=pop_arg(args), inits=pop_arg(args), body=pop_arg(args);
    return bindop(env,vars,inits,body);}
  case FD_GET_OPCODE: case FD_PRIMGET_OPCODE:
  case FD_TEST_OPCODE: case FD_PRIMTEST_OPCODE:
  case FD_ASSERT_OPCODE: case FD_ADD_OPCODE:
  case FD_RETRACT_OPCODE: case FD_DROP_OPCODE:
  case FD_STORE_OPCODE: {
    fdtype expr1=pop_arg(args), expr2=pop_arg(args), expr3=pop_arg(args);
    fdtype arg1=op_eval(expr1,env,0), arg2, arg3;
    fdtype result=FD_VOID;
    if (FD_ABORTED(arg1)) return arg1;
    arg2=op_eval(expr2,env,0);
    if (FD_ABORTED(arg2)) {
      fd_decref(arg1);
      return arg2;}
    arg3=(FD_VOIDP(expr3))?(FD_VOID):(op_eval(expr3,env,0));
    if (FD_ABORTED(arg3)) {
      fd_decref(arg1);
      fd_decref(arg2);
      return arg3;}
    else result=tableop(opcode,arg1,arg2,arg3);
    fd_decref(arg1); fd_decref(arg2); fd_decref(arg3);
    return result;}}
  if (!(FD_EXPECT_FALSE(FD_PAIRP(FD_CDR(expr))))) {
    /* Otherwise, we should have at least one argument,
       return an error otherwise. */
    return fd_err(fd_SyntaxError,"opcode eval",NULL,expr);}
  else {
    /* We have at least one argument to evaluate and we also get the body. */
    fdtype arg1_expr = pop_arg(args), arg1 = op_eval(arg1_expr,env,0);
    fdtype arg2_expr = pop_arg(args), arg2;
    /* Now, check the result of the first argument expression */
    if (FD_ABORTED(arg1)) return arg1;
    else if (FD_VOIDP(arg1))
      return fd_err(fd_VoidArgument,"opcode eval",NULL,arg1_expr);
    else if (FD_PRECHOICEP(arg1))
      arg1 = fd_simplify_choice(arg1);
    else {}
    if (FD_ND1_OPCODEP(opcode)) {
      if (opcode == FD_FIXCHOICE_OPCODE) {
        if (FD_PRECHOICEP(arg1))
          return fd_simplify_choice(arg1);
        else return arg1;}
      else if (FD_CONSP(arg1)) {
        fdtype result = nd1_dispatch(opcode,arg1);
        fd_decref(arg1);
        return result;}
      else return nd1_dispatch(opcode,arg1);}
    else if (FD_D1_OPCODEP(opcode))
      return d1_call(opcode,arg1);
    /* Check the type for numeric arguments here. */
    else if (FD_NUMERIC_OPCODEP(opcode)) {
      if (FD_EXPECT_FALSE(FD_EMPTY_CHOICEP(arg1)))
        return FD_EMPTY_CHOICE;
      else if (FD_EXPECT_FALSE(!(numeric_argp(arg1)))) {
        fdtype result = fd_type_error(_("number"),"numeric opcode",arg1);
        fd_decref(arg1);
        return result;}
      else {
        if (FD_VOIDP(arg2_expr)) {
          fd_decref(arg1);
          return fd_err(fd_TooFewArgs,opcode_name(opcode),NULL,expr);}
        else arg2 = op_eval(arg2_expr,env,0);
        if (FD_PRECHOICEP(arg2)) arg2 = fd_simplify_choice(arg2);
        if (FD_ABORTED(arg2)) {
          fd_decref(arg1);
          return arg2;}
        else if (FD_EMPTY_CHOICEP(arg2)) {
          fd_decref(arg1);
          return FD_EMPTY_CHOICE;}
        else if ((FD_CHOICEP(arg1))||(FD_CHOICEP(arg2)))
          return d2_call(opcode,arg1,arg2);
        else if ((FD_CONSP(arg1))||(FD_CONSP(arg2))) {
          fdtype result = d2_dispatch(opcode,arg1,arg2);
          fd_decref(arg1); fd_decref(arg2);
          return result;}
        else return d2_dispatch(opcode,arg1,arg2);}}
    else if (FD_D2_OPCODEP(opcode)) {
      if (FD_EMPTY_CHOICEP(arg1))
        return FD_EMPTY_CHOICE;
      else {
        if (FD_VOIDP(arg2_expr)) {
          fd_decref(arg1);
          return fd_err(fd_TooFewArgs,opcode_name(opcode),NULL,expr);}
        else arg2 = op_eval(arg2_expr,env,0);
        if (FD_PRECHOICEP(arg2)) arg2 = fd_simplify_choice(arg2);
        if (FD_ABORTED(arg2)) {
          fd_decref(arg1); return arg2;}
        else if (FD_VOIDP(arg2)) {
          fd_decref(arg1);
          return fd_err(fd_VoidArgument,"opcode eval",NULL,arg2_expr);}
        else if (FD_EMPTY_CHOICEP(arg2)) {
          /* Prune the call */
          fd_decref(arg1); return arg2;}
        else if ((FD_CHOICEP(arg1)) || (FD_CHOICEP(arg2)))
          /* nd2_dispatch handles decref of arg1 and arg2 */
          return d2_call(opcode,arg1,arg2);
        else {
          /* This is the dispatch case where we just go to dispatch. */
          fdtype result = d2_dispatch(opcode,arg1,arg2);
          fd_decref(arg1); fd_decref(arg2);
          return result;}}}
    else if (FD_ND2_OPCODEP(opcode))
      /* This decrefs its arguments itself */
      return nd2_dispatch(opcode,arg1,op_eval(arg2_expr,env,0));
    else {
      fd_decref(arg1);
      return fd_err(fd_SyntaxError,"opcode eval",NULL,expr);}
  }
}

FD_FASTOP fdtype op_eval(fdtype x,fd_lispenv env,int tail)
{
  switch (FD_PTR_MANIFEST_TYPE(x)) {
  case fd_oid_ptr_type: case fd_fixnum_ptr_type:
    return x;
  case fd_immediate_ptr_type:
    if (FD_TYPEP(x,fd_lexref_type))
      return fd_lexref(x,env);
    else if (FD_SYMBOLP(x)) {
      fdtype val = fd_symeval(x,env);
      if (FD_EXPECT_FALSE(FD_VOIDP(val)))
        return fd_err(fd_UnboundIdentifier,"op_eval",FD_SYMBOL_NAME(x),x);
      else return val;}
    else return x;
  case fd_cons_ptr_type: {
    fd_ptr_type cons_type = FD_PTR_TYPE(x);
    switch (cons_type) {
    case fd_pair_type: {
      fdtype car = FD_CAR(x);
      if (FD_TYPEP(car,fd_opcode_type)) {
        if (tail)
          return opcode_dispatch(car,x,env);
        else {
          fdtype v = opcode_dispatch(car,x,env);
          return fd_finish_call(v);}}
      else if (tail)
        return fd_tail_eval(x,env);
      else return fd_eval(x,env);}
    case fd_choice_type: case fd_prechoice_type:
      return fd_eval(x,env);
    case fd_slotmap_type:
      return fd_deep_copy(x);
    default:
      return fd_incref(x);}}
  default: /* Never reached */
    return x;
  }
}

static void set_opcode_name(fdtype opcode,u8_string name)
{
  int off = FD_OPCODE_NUM(opcode);
  u8_string hashname=u8_string_append("#OPCODE_",name,NULL);
  fd_opcode_names[off]=name;
  fd_add_hashname(hashname,opcode);
}

static void init_opcode_names()
{
  set_opcode_name(FD_BRANCH_OPCODE,"IFOP");
  set_opcode_name(FD_NOT_OPCODE,"NOT");
  set_opcode_name(FD_UNTIL_OPCODE,"UNTILOP");
  set_opcode_name(FD_BEGIN_OPCODE,"BEGIN");
  set_opcode_name(FD_QUOTE_OPCODE,"QUOTEOP");
  set_opcode_name(FD_ASSIGN_OPCODE,"ASSIGN!");
  set_opcode_name(FD_SYMREF_OPCODE,"SYMREF");
  set_opcode_name(FD_BIND_OPCODE,"BINDOP");
  set_opcode_name(FD_VOID_OPCODE,"VOIDOP");
  set_opcode_name(FD_AND_OPCODE,"AND");
  set_opcode_name(FD_OR_OPCODE,"OR");
  set_opcode_name(FD_TRY_OPCODE,"TRY");

  set_opcode_name(FD_AMBIGP_OPCODE,"AMBIGUOUS?");
  set_opcode_name(FD_SINGLETONP_OPCODE,"SINGLETON?");
  set_opcode_name(FD_FAILP_OPCODE,"FAIL?");
  set_opcode_name(FD_EXISTSP_OPCODE,"EXISTS?");
  set_opcode_name(FD_SINGLETON_OPCODE,"SINGLETON");
  set_opcode_name(FD_CAR_OPCODE,"CAR");
  set_opcode_name(FD_CDR_OPCODE,"CDR");
  set_opcode_name(FD_LENGTH_OPCODE,"LENGTH");
  set_opcode_name(FD_QCHOICE_OPCODE,"QCHOICE");
  set_opcode_name(FD_CHOICE_SIZE_OPCODE,"CHOICE-SIZE");
  set_opcode_name(FD_PICKOIDS_OPCODE,"PICKOIDS");
  set_opcode_name(FD_PICKSTRINGS_OPCODE,"PICKSTRINGS");
  set_opcode_name(FD_PICKONE_OPCODE,"PICK-ONE");
  set_opcode_name(FD_IFEXISTS_OPCODE,"IFEXISTS");
  set_opcode_name(FD_FIXCHOICE_OPCODE,"%FIXCHOICE");
  set_opcode_name(FD_MINUS1_OPCODE,"-1+");
  set_opcode_name(FD_PLUS1_OPCODE,"1+");
  set_opcode_name(FD_NUMBERP_OPCODE,"NUMBER?");
  set_opcode_name(FD_ZEROP_OPCODE,"ZERO?");
  set_opcode_name(FD_VECTORP_OPCODE,"VECTOR?");
  set_opcode_name(FD_PAIRP_OPCODE,"PAIR?");
  set_opcode_name(FD_EMPTY_LISTP_OPCODE,"EMPTY-LIST?");
  set_opcode_name(FD_STRINGP_OPCODE,"STRING?");
  set_opcode_name(FD_OIDP_OPCODE,"OID?");
  set_opcode_name(FD_SYMBOLP_OPCODE,"SYMBOL?");
  set_opcode_name(FD_FIRST_OPCODE,"FIRST");
  set_opcode_name(FD_SECOND_OPCODE,"SECOND");
  set_opcode_name(FD_THIRD_OPCODE,"THIRD");
  set_opcode_name(FD_CADR_OPCODE,"CADR");
  set_opcode_name(FD_CDDR_OPCODE,"CADR");
  set_opcode_name(FD_CADDR_OPCODE,"CADDR");
  set_opcode_name(FD_CDDDR_OPCODE,"CDDDR");
  set_opcode_name(FD_TONUMBER_OPCODE,"->NUMBER");
  set_opcode_name(FD_NUMEQ_OPCODE,"=");
  set_opcode_name(FD_GT_OPCODE,">");
  set_opcode_name(FD_GTE_OPCODE,">=");
  set_opcode_name(FD_LT_OPCODE,"<");
  set_opcode_name(FD_LTE_OPCODE,"<=");
  set_opcode_name(FD_PLUS_OPCODE,"+");
  set_opcode_name(FD_MINUS_OPCODE,"-");
  set_opcode_name(FD_TIMES_OPCODE,"*");
  set_opcode_name(FD_FLODIV_OPCODE,"/~");
  set_opcode_name(FD_IDENTICAL_OPCODE,"IDENTICAL?");
  set_opcode_name(FD_OVERLAPS_OPCODE,"OVERLAPS?");
  set_opcode_name(FD_CONTAINSP_OPCODE,"CONTAINS?");
  set_opcode_name(FD_UNION_OPCODE,"UNION");
  set_opcode_name(FD_INTERSECT_OPCODE,"INTERSECTION");
  set_opcode_name(FD_DIFFERENCE_OPCODE,"DIFFERENCE");
  set_opcode_name(FD_CHOICEREF_OPCODE,"%CHOICEREF");
  set_opcode_name(FD_EQ_OPCODE,"EQ?");
  set_opcode_name(FD_EQV_OPCODE,"EQV?");
  set_opcode_name(FD_EQUAL_OPCODE,"EQUAL?");
  set_opcode_name(FD_ELT_OPCODE,"ELT");
  set_opcode_name(FD_ASSERT_OPCODE,"ASSERT!");
  set_opcode_name(FD_RETRACT_OPCODE,"RETRACT!");
  set_opcode_name(FD_GET_OPCODE,"GET");
  set_opcode_name(FD_TEST_OPCODE,"TEST");
  set_opcode_name(FD_ADD_OPCODE,"ADD");
  set_opcode_name(FD_DROP_OPCODE,"DROP");
  set_opcode_name(FD_XREF_OPCODE,"XREF");
  set_opcode_name(FD_PRIMGET_OPCODE,"%GET");
  set_opcode_name(FD_PRIMTEST_OPCODE,"%TEST");
  set_opcode_name(FD_STORE_OPCODE,"STORE!");
}

FD_EXPORT fdtype fd_get_opcode(u8_string name)
{
  int i = 0; while (i<fd_opcodes_length) {
    u8_string opname = fd_opcode_names[i];
    if ((opname)&&(strcasecmp(name,opname)==0)) 
      return FD_OPCODE(i);
    else i++;}
  return FD_FALSE;
}

static fdtype name2opcode_prim(fdtype arg)
{
  if (FD_SYMBOLP(arg))
    return fd_get_opcode(FD_SYMBOL_NAME(arg));
  else if (FD_STRINGP(arg))
    return fd_get_opcode(FD_STRDATA(arg));
  else return fd_type_error(_("opcode name"),"name2opcode_prim",arg);
}

/* PICK opcodes */

static fdtype pickoids_opcode(fdtype arg1)
{
  if (FD_OIDP(arg1)) return arg1;
  else if (FD_EMPTY_CHOICEP(arg1)) return arg1;
  else if ((FD_CHOICEP(arg1)) || (FD_PRECHOICEP(arg1))) {
    fdtype choice, results = FD_EMPTY_CHOICE;
    int free_choice = 0, all_oids = 1;
    if (FD_CHOICEP(arg1)) choice = arg1;
    else {choice = fd_make_simple_choice(arg1); free_choice = 1;}
    {FD_DO_CHOICES(elt,choice) {
	if (FD_OIDP(elt)) {FD_ADD_TO_CHOICE(results,elt);}
	else if (all_oids) all_oids = 0;}}
    if (all_oids) {
      fd_decref(results);
      if (free_choice) return choice;
      else return fd_incref(choice);}
    else if (free_choice) fd_decref(choice);
    return fd_simplify_choice(results);}
  else return FD_EMPTY_CHOICE;
}

static fdtype pickstrings_opcode(fdtype arg1)
{
  if ((FD_CHOICEP(arg1)) || (FD_PRECHOICEP(arg1))) {
    fdtype choice, results = FD_EMPTY_CHOICE;
    int free_choice = 0, all_strings = 1;
    if (FD_CHOICEP(arg1)) choice = arg1;
    else {choice = fd_make_simple_choice(arg1); free_choice = 1;}
    {FD_DO_CHOICES(elt,choice) {
	if (FD_STRINGP(elt)) {
	  fd_incref(elt); FD_ADD_TO_CHOICE(results,elt);}
	else if (all_strings) all_strings = 0;}}
    if (all_strings) {
      fd_decref(results);
      if (free_choice) return choice;
      else return fd_incref(choice);}
    else if (free_choice) fd_decref(choice);
    return fd_simplify_choice(results);}
  else if (FD_STRINGP(arg1)) return fd_incref(arg1);
  else return FD_EMPTY_CHOICE;
}

static fdtype pickone_opcode(fdtype normal)
{
  int n = FD_CHOICE_SIZE(normal);
  if (n) {
    fdtype chosen;
    int i = u8_random(n);
    const fdtype *data = FD_CHOICE_DATA(normal);
    chosen = data[i]; fd_incref(chosen);
    return chosen;}
  else return FD_EMPTY_CHOICE;
}

/* Initialization */

static double opcodes_initialized = 0;

FD_EXPORT
fdtype fd_opcode_dispatch(fdtype opcode,fdtype expr,fd_lispenv env)
{
  return opcode_dispatch(opcode,expr,env);
}

void fd_init_opcodes_c()
{
  if (opcodes_initialized) return;
  else opcodes_initialized = 1;

  fd_type_names[fd_opcode_type]=_("opcode");
  fd_unparsers[fd_opcode_type]=unparse_opcode;
  fd_immediate_checkfns[fd_opcode_type]=validate_opcode;

  memset(fd_opcode_names,0,sizeof(fd_opcode_names));

  init_opcode_names();

  fd_idefn(fd_scheme_module,fd_make_cprim1("NAME->OPCODE",name2opcode_prim,1));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
