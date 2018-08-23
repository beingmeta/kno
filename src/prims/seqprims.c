/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif
/* FD_MODULE = scheme */

#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/compounds.h"
#include "framerd/sequences.h"
#include "framerd/numbers.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/seqprims.h"
#include "framerd/sorting.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8printf.h>

#include <limits.h>

static u8_condition SequenceMismatch=_("Mismatch of sequence lengths");
static u8_condition EmptyReduce=_("No sequence elements to reduce");

#define FD_EQV(x,y) \
  ((FD_EQ(x,y)) || \
   ((NUMBERP(x)) && (NUMBERP(y)) && \
    (fd_numcompare(x,y)==0)))

#define string_start(bytes,i) ((i==0) ? (bytes) : (u8_substring(bytes,i)))

static lispval make_float_vector(int n,lispval *from_elts);
static lispval make_double_vector(int n,lispval *from_elts);
static lispval make_short_vector(int n,lispval *from_elts);
static lispval make_int_vector(int n,lispval *from_elts);
static lispval make_long_vector(int n,lispval *from_elts);

/* Exported primitives */

FD_EXPORT int applytest(lispval test,lispval elt)
{
  if (FD_HASHSETP(test))
    return fd_hashset_get(FD_XHASHSET(test),elt);
  else if (HASHTABLEP(test))
    return fd_hashtable_probe(FD_XHASHTABLE(test),elt);
  else if ((SYMBOLP(test)) || (OIDP(test)))
    return fd_test(elt,test,VOID);
  else if (TABLEP(test))
    return fd_test(test,elt,VOID);
  else if (FD_APPLICABLEP(test)) {
    lispval result = fd_apply(test,1,&elt);
    if (FALSEP(result)) return 0;
    else {fd_decref(result); return 1;}}
  else if (EMPTYP(test)) return 0;
  else if (CHOICEP(test)) {
    DO_CHOICES(each_test,test) {
      int result = applytest(each_test,elt);
      if (result<0) return result;
      else if (result) return result;}
    return 0;}
  else {
    fd_seterr(fd_TypeError,"removeif",_("test"),test);
    return -1;}
}

FD_EXPORT lispval fd_removeif(lispval test,lispval sequence,int invert)
{
  if (!(FD_SEQUENCEP(sequence)))
    return fd_type_error("sequence","fd_remove",sequence);
  else if (NILP(sequence)) return sequence;
  else {
    int i = 0, j = 0, removals = 0, len = fd_seq_length(sequence);
    fd_ptr_type result_type = FD_PTR_TYPE(sequence);
    lispval *results = u8_alloc_n(len,lispval), result;
    while (i < len) {
      lispval elt = fd_seq_elt(sequence,i);
      int compare = applytest(test,elt); i++;
      if (compare<0) {u8_free(results); return -1;}
      else if ((invert) ? (!(compare)) : (compare)) {
        removals++; fd_decref(elt);}
      else results[j++]=elt;}
    if (removals) {
      result = fd_makeseq(result_type,j,results);
      i = 0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return result;}
    else {
      i = 0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return fd_incref(sequence);}}
}

/* Mapping */

FD_EXPORT lispval fd_mapseq(lispval fn,int n_seqs,lispval *sequences)
{
  int i = 1, seqlen = -1;
  lispval firstseq = sequences[0];
  lispval result, *results, _argvec[8], *argvec = NULL;
  fd_ptr_type result_type = FD_PTR_TYPE(firstseq);
  if (FD_FCNIDP(fn)) fn = fd_fcnid_ref(fn);
  if ((TABLEP(fn)) || (ATOMICP(fn))) {
    if (n_seqs>1)
      return fd_err(fd_TooManyArgs,"fd_mapseq",NULL,fn);
    else if (NILP(firstseq))
      return firstseq;}
  if (!(FD_SEQUENCEP(firstseq)))
    return fd_type_error("sequence","fd_mapseq",firstseq);
  else seqlen = fd_seq_length(firstseq);
  while (i<n_seqs)
    if (!(FD_SEQUENCEP(sequences[i])))
      return fd_type_error("sequence","fd_mapseq",sequences[i]);
    else if (seqlen!=fd_seq_length(sequences[i]))
      return fd_err(SequenceMismatch,"fd_foreach",NULL,sequences[i]);
    else i++;
  if (seqlen==0) return fd_incref(firstseq);
  enum { applyfn, tablefn, oidfn, getfn } fntype;
  results = u8_alloc_n(seqlen,lispval);
  if (FD_APPLICABLEP(fn)) {
    if (n_seqs<8) argvec=_argvec;
    else argvec = u8_alloc_n(n_seqs,lispval);
    fntype = applyfn;}
  else if (OIDP(fn))
    fntype = oidfn;
  else if (TABLEP(fn))
    fntype = tablefn;
  else fntype = getfn;
  i = 0; while (i < seqlen) {
    lispval elt = fd_seq_elt(firstseq,i), new_elt;
    if (fntype == applyfn) {
      int j = 1; while (j<n_seqs) {
        argvec[j]=fd_seq_elt(sequences[j],i); j++;}
      argvec[0]=elt;
      new_elt = fd_apply(fn,n_seqs,argvec);
      j = 1; while (j<n_seqs) {fd_decref(argvec[j]); j++;}}
    else if (fntype == tablefn)
      new_elt = fd_get(fn,elt,elt);
    else if (fntype == oidfn)
      new_elt = fd_frame_get(elt,fn);
    else new_elt = fd_get(elt,fn,elt);
    fd_decref(elt);
    if (result_type == fd_string_type) {
      if (!(TYPEP(new_elt,fd_character_type)))
        result_type = fd_vector_type;}
    else if ((result_type == fd_packet_type)||
             (result_type == fd_secret_type)) {
      if (FIXNUMP(new_elt)) {
        long long intval = FIX2INT(new_elt);
        if ((intval<0) || (intval>=0x100))
          result_type = fd_vector_type;}
      else result_type = fd_vector_type;}
    if (FD_ABORTED(new_elt)) {
      int j = 0; while (j<i) {fd_decref(results[j]); j++;}
      u8_free(results);
      return new_elt;}
    else results[i++]=new_elt;}
  result = fd_makeseq(result_type,seqlen,results);
  i = 0; while (i<seqlen) {fd_decref(results[i]); i++;}
  u8_free(results);
  return result;
}
static lispval mapseq_prim(int n,lispval *args)
{
  return fd_mapseq(args[0],n-1,args+1);
}

FD_EXPORT lispval fd_foreach(lispval fn,int n_seqs,lispval *sequences)
{
  int i = 0, seqlen = -1;
  lispval firstseq = sequences[0];
  lispval _argvec[8], *argvec = NULL;
  fd_ptr_type result_type = FD_PTR_TYPE(firstseq);
  if ((TABLEP(fn)) || ((ATOMICP(fn)) && (FD_FCNIDP(fn)))) {
    if (n_seqs>1)
      return fd_err(fd_TooManyArgs,"fd_foreach",NULL,fn);
    else if (NILP(firstseq))
      return firstseq;}
  if (!(FD_SEQUENCEP(firstseq)))
    return fd_type_error("sequence","fd_mapseq",firstseq);
  else seqlen = fd_seq_length(firstseq);
  while (i<n_seqs)
    if (!(FD_SEQUENCEP(sequences[i])))
      return fd_type_error("sequence","fd_mapseq",sequences[i]);
    else if (seqlen!=fd_seq_length(sequences[i]))
      return fd_err(SequenceMismatch,"fd_foreach",NULL,sequences[i]);
    else i++;
  if (seqlen==0) return VOID;
  enum { applyfn, tablefn, oidfn, getfn } fntype;
  if (FD_APPLICABLEP(fn)) {
    if (n_seqs<8) argvec=_argvec;
    else argvec = u8_alloc_n(n_seqs,lispval);
    fntype = applyfn;}
  else if (OIDP(fn))
    fntype = oidfn;
  else if (TABLEP(fn))
    fntype = tablefn;
  else fntype = getfn;
  i = 0; while (i < seqlen) {
    lispval elt = argvec[0]=fd_seq_elt(firstseq,i), new_elt;
    if (fntype == tablefn)
      new_elt = fd_get(fn,elt,elt);
    else if (fntype == oidfn)
      new_elt = fd_frame_get(elt,fn);
    else if (fntype == applyfn) {
      int j = 1; while (j<n_seqs) {
        argvec[j]=fd_seq_elt(sequences[j],i); j++;}
      argvec[0]=elt;
      new_elt = fd_apply(fn,n_seqs,argvec);
      j = 1; while (j<n_seqs) {
        fd_decref(argvec[j]); j++;}}
    else new_elt = fd_get(elt,fn,elt);
    fd_decref(elt);
    if (result_type == fd_string_type) {
      if (!(TYPEP(new_elt,fd_character_type)))
        result_type = fd_vector_type;}
    else if ((result_type == fd_packet_type)||
             (result_type == fd_secret_type)) {
      if (FIXNUMP(new_elt)) {
        long long intval = FIX2INT(new_elt);
        if ((intval<0) || (intval>=0x100))
          result_type = fd_vector_type;}
      else result_type = fd_vector_type;}
    if (FD_ABORTED(new_elt)) return new_elt;
    else {fd_decref(new_elt); i++;}}
  return VOID;
}
FD_CPRIM(foreach_prim,"FOR-EACH",int n,lispval *args)
/* This iterates over a sequence or set of sequences and applies
   FN to each of the elements. */
{
  return fd_foreach(args[0],n-1,args+1);
}

FD_EXPORT lispval fd_map2choice(lispval fn,lispval sequence)
{
  if (!(FD_SEQUENCEP(sequence)))
    return fd_type_error("sequence","fd_mapseq",sequence);
  else if (NILP(sequence)) return sequence;
  else if ((FD_APPLICABLEP(fn)) || (TABLEP(fn)) || (ATOMICP(fn))) {
    int i = 0, len = fd_seq_length(sequence);
    fd_ptr_type result_type = FD_PTR_TYPE(sequence);
    lispval *results = u8_alloc_n(len,lispval);
    while (i < len) {
      lispval elt = fd_seq_elt(sequence,i), new_elt;
      if (TABLEP(fn))
        new_elt = fd_get(fn,elt,elt);
      else if (FD_APPLICABLEP(fn))
        new_elt = fd_apply(fn,1,&elt);
      else if (OIDP(elt))
        new_elt = fd_frame_get(elt,fn);
      else new_elt = fd_get(elt,fn,elt);
      fd_decref(elt);
      if (result_type == fd_string_type) {
        if (!(TYPEP(new_elt,fd_character_type)))
          result_type = fd_vector_type;}
      else if ((result_type == fd_packet_type)||
               (result_type == fd_secret_type)) {
        if (FIXNUMP(new_elt)) {
          long long intval = FIX2INT(new_elt);
          if ((intval<0) || (intval>=0x100))
            result_type = fd_vector_type;}
        else result_type = fd_vector_type;}
      if (FD_ABORTED(new_elt)) {
        int j = 0; while (j<i) {fd_decref(results[j]); j++;}
        u8_free(results);
        return new_elt;}
      else results[i++]=new_elt;}
    return fd_make_choice(len,results,0);}
  else return fd_err(fd_NotAFunction,"MAP",NULL,fn);
}

/* Reduction */

FD_EXPORT lispval fd_reduce(lispval fn,lispval sequence,lispval result)
{
  int i = 0, len = fd_seq_length(sequence);
  if (len==0)
    if (VOIDP(result))
      return fd_err(EmptyReduce,"fd_reduce",NULL,sequence);
    else return fd_incref(result);
  else if (len<0)
    return FD_ERROR;
  else if (!(FD_APPLICABLEP(fn)))
    return fd_err(fd_NotAFunction,"MAP",NULL,fn);
  else if (VOIDP(result)) {
    result = fd_seq_elt(sequence,0); i = 1;}
  while (i < len) {
    lispval elt = fd_seq_elt(sequence,i), rail[2], new_result;
    rail[0]=elt; rail[1]=result;
    new_result = fd_apply(fn,2,rail);
    fd_decref(result); fd_decref(elt);
    if (FD_ABORTP(new_result)) return new_result;
    result = new_result;
    i++;}
  return result;
}

/* Scheme primitives */

static lispval sequencep_prim(lispval x)
{
  if (FD_COMPOUNDP(x)) {
    struct FD_COMPOUND *c = (fd_compound) x;
    if (c->compound_off >= 0)
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_SEQUENCEP(x))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval seqlen_prim(lispval x)
{
  int len = fd_seq_length(x);
  if (len<0) return fd_type_error(_("sequence"),"seqlen",x);
  else return FD_INT(len);
}

static lispval seqelt_prim(lispval x,lispval offset)
{
  char buf[32]; int off; lispval result;
  if (!(FD_SEQUENCEP(x)))
    return fd_type_error(_("sequence"),"seqelt_prim",x);
  else if (FD_INTP(offset)) off = FIX2INT(offset);
  else return fd_type_error(_("fixnum"),"seqelt_prim",offset);
  if (off<0) off = fd_seq_length(x)+off;
  result = fd_seq_elt(x,off);
  if (result == FD_TYPE_ERROR)
    return fd_type_error(_("sequence"),"seqelt_prim",x);
  else if (result == FD_RANGE_ERROR) {
    return fd_err(fd_RangeError,"seqelt_prim",
                  u8_uitoa10(fd_getint(offset),buf),
                  x);}
  else return result;
}

/* This is for use in filters, especially PICK and REJECT */

enum COMPARISON {
  cmp_lt, cmp_lte, cmp_eq, cmp_gte, cmp_gt };

static int has_length_helper(lispval x,lispval length_arg,enum COMPARISON cmp)
{
  int seqlen = fd_seq_length(x), testlen;
  if (seqlen<0) return fd_type_error(_("sequence"),"seqlen",x);
  else if (FD_INTP(length_arg))
    testlen = (FIX2INT(length_arg));
  else return fd_type_error(_("fixnum"),"has-length?",x);
  switch (cmp) {
  case cmp_lt: return (seqlen<testlen);
  case cmp_lte: return (seqlen<=testlen);
  case cmp_eq: return (seqlen == testlen);
  case cmp_gte: return (seqlen>=testlen);
  case cmp_gt: return (seqlen>testlen);
  default:
    fd_seterr("Unknown length comparison","has_length_helper",NULL,x);
    return -1;}
}

static lispval has_length_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_eq);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static lispval has_length_lt_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_lt);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static lispval has_length_lte_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_lte);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static lispval has_length_gt_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_gt);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static lispval has_length_gte_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_gte);
  if (retval<0) return FD_ERROR;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static lispval has_length_gt_zero_prim(lispval x)
{
  int seqlen = fd_seq_length(x);
  if (seqlen>0) return FD_TRUE; else return FD_FALSE;
}

static lispval has_length_gt_one_prim(lispval x)
{
  int seqlen = fd_seq_length(x);
  if (seqlen>1) return FD_TRUE; else return FD_FALSE;
}

/* Element access */

/* We separate this out to give the compiler the option of not inline
   coding this guy. */
static lispval check_empty_list_range
  (u8_string prim,lispval seq,
   lispval start_arg,lispval end_arg,
   int *startp,int *endp)
{
  if ((FALSEP(start_arg)) ||
      (VOIDP(start_arg)) ||
      (start_arg == FD_INT(0))) {}
  else if (FIXNUMP(start_arg))
    return fd_err(fd_RangeError,prim,"start",start_arg);
  else return fd_type_error("fixnum",prim,start_arg);
  if ((FALSEP(end_arg)) ||
      (VOIDP(end_arg)) ||
      (end_arg == FD_INT(0))) {}
  else if (FIXNUMP(end_arg))
    return fd_err(fd_RangeError,prim,"start",end_arg);
  else return fd_type_error("fixnum",prim,end_arg);
  *startp = 0; *endp = 0;
  return VOID;
}

static int interpret_range_args(int len,u8_context caller,
                                lispval start_arg,lispval end_arg,
                                int *startp,int *endp,int *deltap)
{
  int start, end;
  if (FD_VOIDP(start_arg)) {
    *startp=0; *endp=len; *deltap=1;
    return len;}
  else if (FD_FIXNUMP(start_arg))
    start = FD_FIX2INT(start_arg);
  else if (FD_FALSEP(start_arg))
    start = 0;
  else return -1;
  if (start<0) start = len+start;
  if ( (FD_VOIDP(end_arg)) || (FD_FALSEP(end_arg)) )
    end = len;
  else if (FD_FIXNUMP(end_arg)) {
    end = FD_FIX2INT(end_arg);
    if (end<0) end = len-start;}
  else return -1;
  if ( (start<0) || (end<0) || (start > len) || (end > len) )
    return -1;
  else {
    *startp = start;
    *endp = end;
    *deltap = (end<start) ? (-1) : (1);
    return len;}
}

static lispval check_range(u8_string prim,lispval seq,
                           lispval start_arg,lispval end_arg,
                           int *startp,int *endp)
{
  if (NILP(seq))
    return check_empty_list_range(prim,seq,start_arg,end_arg,startp,endp);
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error(_("sequence"),prim,seq);
  else if (FD_INTP(start_arg)) {
    int arg = FIX2INT(start_arg);
    if (arg<0)
      return fd_err(fd_RangeError,prim,"start",start_arg);
    else *startp = arg;}
  else if ((VOIDP(start_arg)) || (FALSEP(start_arg)))
    *startp = 0;
  else return fd_type_error("fixnum",prim,start_arg);
  if ((VOIDP(end_arg)) || (FALSEP(end_arg)))
    *endp = fd_seq_length(seq);
  else if (FD_INTP(end_arg)) {
    int arg = FIX2INT(end_arg);
    if (arg<0) {
      int len = fd_seq_length(seq), off = len+arg;
      if (off>=0) *endp = off;
      else return fd_err(fd_RangeError,prim,"start",end_arg);}
    else if (arg>fd_seq_length(seq))
      return fd_err(fd_RangeError,prim,"start",end_arg);
    else *endp = arg;}
  else return fd_type_error("fixnum",prim,end_arg);
  return VOID;
}

static lispval slice_prim(lispval x,lispval start_arg,lispval end_arg)
{
  int start, end; char buf[128];
  lispval result = check_range("slice_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(result)) return result;
  else result = fd_slice(x,start,end);
  if (result == FD_TYPE_ERROR)
    return fd_type_error(_("sequence"),"slice_prim",x);
  else if (result == FD_RANGE_ERROR) {
    return fd_err(fd_RangeError,"slice_prim",
                  u8_sprintf(buf,128,"%d[%d:%d]",fd_seq_length(x),start,end),
                  x);}
  else return result;
}

static lispval position_prim(lispval key,lispval x,lispval start_arg,lispval end_arg)
{
  int result, start, end; char buf[128];
  lispval check = check_range("position_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else result = fd_position(key,x,start,end);
  if (result>=0) return FD_INT(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2)
    return fd_type_error(_("sequence"),"position_prim",x);
  else if (result == -3) {
    return fd_err(fd_RangeError,"position_prim",
                  u8_sprintf(buf,128,"%d[%d:%d]",fd_seq_length(x),start,end),
                  x);}
  else return FD_INT(result);
}

static lispval rposition_prim(lispval key,lispval x,lispval start_arg,lispval end_arg)
{
  int result, start, end; char buf[128];
  lispval check = check_range("rposition_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else result = fd_rposition(key,x,start,end);
  if (result>=0) return FD_INT(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2)
    return fd_type_error(_("sequence"),"rposition_prim",x);
  else if (result == -3) {
    return fd_err(fd_RangeError,"rposition_prim",
                  u8_sprintf(buf,128,"%d[%d:%d]",fd_seq_length(x),start,end),
                  x);}
  else return FD_INT(result);
}

static lispval find_prim(lispval key,lispval x,lispval start_arg,lispval end_arg)
{
  int result, start, end; char buf[128];
  lispval check = check_range("find_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else result = fd_position(key,x,start,end);
  if (result>=0) return FD_TRUE;
  else if (result == -1) return FD_FALSE;
  else if (result == -2)
    return fd_type_error(_("sequence"),"find_prim",x);
  else if (result == -3) {
    return fd_err(fd_RangeError,"find_prim",
                  u8_sprintf(buf,128,"%d[%d:%d]",fd_seq_length(x),start,end),
                  x);}
  else return FD_FALSE;
}

static lispval search_prim(lispval key,lispval x,lispval start_arg,lispval end_arg)
{
  int result, start, end; char buf[128];
  lispval check = check_range("search_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else result = fd_search(key,x,start,end);
  if (result>=0) return FD_INT(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2) {
    return fd_err(fd_RangeError,"search_prim",
                  u8_sprintf(buf,128,"%d:%d",start,end),
                  x);}
  else if (result == -3)
    return fd_type_error(_("sequence"),"search_prim",x);
  else return result;
}

static lispval every_prim(lispval proc,lispval x,lispval start_arg,lispval end_arg)
{
  int start, end;
  lispval check = check_range("every_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("function"),"every_prim",x);
  else if (STRINGP(x)) {
    const u8_byte *scan = CSTRING(x);
    int i = 0; while (i<end) {
      int c = u8_sgetc(&scan);
      if (i<start) i++;
      else if (i<end) {
        lispval lc = FD_CODE2CHAR(c);
        lispval testval = fd_apply(proc,1,&lc);
        if (FALSEP(testval)) return FD_FALSE;
        else if (FD_ABORTED(testval)) return testval;
        else {
          fd_decref(testval); i++;}}
      else return FD_TRUE;}
    return FD_TRUE;}
  else {
    int i = start; while (i<end) {
      lispval elt = fd_seq_elt(x,i);
      lispval testval = fd_apply(proc,1,&elt);
      if (FD_ABORTED(testval)) {
        fd_decref(elt); return testval;}
      else if (FALSEP(testval)) {
        fd_decref(elt); return FD_FALSE;}
      else {
        fd_decref(elt); fd_decref(testval); i++;}}
    return FD_TRUE;}
}

static lispval some_prim(lispval proc,lispval x,lispval start_arg,lispval end_arg)
{
  int start, end;
  lispval check = check_range("some_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("function"),"some_prim",x);
  else if (STRINGP(x)) {
    const u8_byte *scan = CSTRING(x);
    int i = 0; while (i<end) {
      int c = u8_sgetc(&scan);
      if (i<start) i++;
      else if (i< end) {
        lispval lc = FD_CODE2CHAR(c);
        lispval testval = fd_apply(proc,1,&lc);
        if (FD_ABORTED(testval)) return testval;
        else if (FALSEP(testval)) i++;
        else {
          fd_decref(testval); return FD_TRUE;}}
      else return FD_FALSE;}
    return FD_FALSE;}
  else {
    int i = start; while (i<end) {
      lispval elt = fd_seq_elt(x,i);
      lispval testval = fd_apply(proc,1,&elt);
      if (FD_ABORTED(testval)) {
        fd_decref(elt); return testval;}
      else if ((FALSEP(testval)) || (EMPTYP(testval))) {
        fd_decref(elt); i++;}
      else {
        fd_decref(elt); fd_decref(testval);
        return FD_TRUE;}}
    return FD_FALSE;}
}

FD_CPRIM(removeif_prim,"REMOVE-IF",lispval test,lispval sequence)
/* Removes elements of sequence which pass the predicate 'test'.
   The element E is removed when:
   * if FN is a function, applying FN to E returns true
   * if FN is a table, FN contains E as a KEY
   * or, if E is a table, E contains the key FN. */
{
  if (EMPTYP(sequence)) return sequence;
  else if (CHOICEP(sequence)) {
    lispval results = EMPTY;
    DO_CHOICES(seq,sequence) {
      lispval r = fd_removeif(test,seq,0);
      if (FD_ABORTED(r)) {
        fd_decref(results); return r;}
      CHOICE_ADD(results,r);}
    return results;}
  else return fd_removeif(test,sequence,0);
}

static lispval removeifnot_prim(lispval test,lispval sequence)
{
  if (EMPTYP(sequence)) return sequence;
  else if (CHOICEP(sequence)) {
    lispval results = EMPTY;
    DO_CHOICES(seq,sequence) {
      lispval r = fd_removeif(test,seq,1);
      if (FD_ABORTED(r)) {
        fd_decref(results); return r;}
      CHOICE_ADD(results,r);}
    return results;}
  else return fd_removeif(test,sequence,1);
}

static lispval range_error(u8_context caller,lispval seq,int len,int start,int end)
{
  u8_byte buf[128];
  fd_seterr(fd_RangeError,caller,
            u8_sprintf(buf,128,"(0,%d,%d,%d)",start,end,len),
            seq);
  return FD_ERROR_VALUE;
}

/* Sequence-if/if-not functions */

lispval position_if_prim(lispval test,lispval seq,lispval start_arg,
                         lispval end_arg)
{
  int end, start = (FD_FIXNUMP(start_arg)) ? (FD_FIX2INT(start_arg)) : (0);
  int ctype = FD_PTR_TYPE(seq);
  switch (ctype) {
  case fd_vector_type: case fd_code_type: {
    int len = FD_VECTOR_LENGTH(seq), delta = 1;
    if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
    int ok = interpret_range_args
      (len,"position_if_prim",start_arg,end_arg,&start,&end,&delta);
    if (ok<0) return range_error("position_if_prim",seq,len,start,end);
    lispval *data = FD_VECTOR_ELTS(seq);
    int i = start; while (i!=end) {
      lispval v = fd_apply(test,1,data+i);
      if (FD_ABORTP(v)) return v;
      else if (!(FD_FALSEP(v))) {
        fd_decref(v);
        return FD_INT(i);}
      else i += delta;}
    return FD_FALSE;}
  case fd_compound_type: {
      int len = FD_COMPOUND_VECLEN(seq), delta = 1;
      if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
      lispval *data = FD_COMPOUND_VECELTS(seq);
      int ok = interpret_range_args
        (len,"position_if_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("position_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = fd_apply(test,1,data+i);
        if (FD_ABORTP(v)) return v;
        else if (!(FD_FALSEP(v))) {
          fd_decref(v);
          return FD_INT(i);}
        else i += delta;}
      return FD_FALSE;}
    case fd_pair_type: {
      int i = 0, pos = -1, len = -1, delta = 1;
      lispval scan = seq;
      if ( (start<0) || (!(FD_FIXNUMP(end_arg))) ||
           ( (FD_FIX2INT(end_arg)) < 0) ) {
        len = fd_list_length(seq);
        if (start<0) start = start+len;
        if (!(FD_FIXNUMP(end_arg)))
          end = len;
        else if ((FD_FIX2INT(end_arg))<0)
          end = len + FD_FIX2INT(end_arg);
        else end = FD_FIX2INT(end_arg);
        int ok = interpret_range_args
          (len,"position_if_prim",start_arg,end_arg,&start,&end,&delta);
        if (ok<0) return range_error("position_if_prim",seq,len,start,end);}
      else end = FD_FIX2INT(end_arg);
      while (FD_PAIRP(scan)) {
        lispval elt = FD_CAR(scan);
        if (i < start) {
          i++; scan=FD_CDR(scan);
          continue;}
        else if (i > end) break;
        lispval v = fd_apply(test,1,&elt);
        if (FD_ABORTP(v)) return v;
        else if (!(FD_FALSEP(v))) {
          fd_decref(v);
          if (start>end) pos=i;
          else return FD_INT(i);}
        else {
          scan = FD_CDR(scan);
          i++;}}
      if (pos<0) return FD_FALSE;
      else return FD_INT(pos);}
  default: {
      if (NILP(seq))
        return FD_FALSE;
      int len = -1, delta = 1;
      lispval *data = fd_seq_elts(seq,&len);
      if (len < 0) {
        fd_seterr("UnenumerableSequence","position_if_prim",NULL,seq);
        return FD_ERROR_VALUE;}
      if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
      int ok = interpret_range_args
        (len,"position_if_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("position_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = fd_apply(test,1,data+i);
        if (FD_ABORTP(v)) {
          fd_decref_vec(data,len);
          u8_free(data);
          return v;}
        else if (!(FD_FALSEP(v))) {
          fd_decref(v);
          fd_decref_vec(data,len);
          u8_free(data);
          return FD_INT(i);}
        else i += delta;}
      fd_decref_vec(data,len);
      u8_free(data);
      return FD_FALSE;}
  }
}

lispval position_if_not_prim(lispval test,lispval seq,lispval start_arg,
                             lispval end_arg)
{
  int start = FD_FIX2INT(start_arg), end;
  int ctype = FD_PTR_TYPE(seq);
  switch (ctype) {
  case fd_vector_type: case fd_code_type: {
    int len = FD_VECTOR_LENGTH(seq), delta = 1;
    if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
    int ok = interpret_range_args
      (len,"position_if_not_prim",start_arg,end_arg,&start,&end,&delta);
    if (ok<0) return range_error("position_if_prim",seq,len,start,end);
    lispval *data = FD_VECTOR_ELTS(seq);
    int i = start; while (i!=end) {
      lispval v = fd_apply(test,1,data+i);
      if (FD_ABORTP(v)) return v;
      else if (FD_FALSEP(v)) {
        return FD_INT(i);}
      else {
        fd_decref(v);
        i += delta;}}
    return FD_FALSE;}
  case fd_compound_type: {
      int len = FD_COMPOUND_VECLEN(seq), delta = 1;
      if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
      lispval *data = FD_COMPOUND_VECELTS(seq);
      int ok = interpret_range_args
        (len,"position_if_not_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("position_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = fd_apply(test,1,data+i);
        if (FD_ABORTP(v)) return v;
        else if (FD_FALSEP(v))
          return FD_INT(i);
        else {
          fd_decref(v);
          i += delta;}
        return FD_FALSE;}}
  case fd_pair_type: {
    int i = 0, pos = -1, len = -1, delta = 1;
    lispval scan = seq;
    if ( (start<0) || (!(FD_FIXNUMP(end_arg))) ||
         ( (FD_FIX2INT(end_arg)) < 0) ) {
      len = fd_list_length(seq);
      if (start<0) start = start+len;
      if (!(FD_FIXNUMP(end_arg)))
          end = len;
        else if ((FD_FIX2INT(end_arg))<0)
          end = len + FD_FIX2INT(end_arg);
        else end = FD_FIX2INT(end_arg);
        int ok = interpret_range_args
          (len,"position_if_not_prim",start_arg,end_arg,&start,&end,&delta);
        if (ok<0) return range_error("position_if_prim",seq,len,start,end);}
      else end = FD_FIX2INT(end_arg);
      while (FD_PAIRP(scan)) {
        lispval elt = FD_CAR(scan);
        if (i < start) {
          i++; scan=FD_CDR(scan);
          continue;}
        else if (i > end) break;
        lispval v = fd_apply(test,1,&elt);
        if (FD_ABORTP(v)) return v;
        else if (FD_FALSEP(v)) {
          if (start>end) pos=i;
          else return FD_INT(i);}
        else {
          fd_decref(v);
          scan = FD_CDR(scan);
          i++;}}
      if (pos<0) return FD_FALSE;
      else return FD_INT(pos);}
  default: {
      if (NILP(seq))
        return FD_FALSE;
      int len = -1, delta = 1;
      lispval *data = fd_seq_elts(seq,&len);
      if (len < 0) {
        fd_seterr("UnenumerableSequence","position_if_prim",NULL,seq);
        return FD_ERROR_VALUE;}
      if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
      int ok = interpret_range_args
        (len,"position_if_not_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("position_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = fd_apply(test,1,data+i);
        if (FD_ABORTP(v)) {
          fd_decref_vec(data,len);
          u8_free(data);
          return v;}
        else if (FD_FALSEP(v)) {
          fd_decref_vec(data,len);
          u8_free(data);
          return FD_INT(i);}
        else {
          fd_decref(v);
          i += delta;}}
      fd_decref_vec(data,len);
      u8_free(data);
      return FD_FALSE;}
  }
}

lispval find_if_prim(lispval test,lispval seq,lispval start_arg,
                     lispval end_arg,lispval fail_val)
{
  int end, start = (FD_FIXNUMP(start_arg)) ? (FD_FIX2INT(start_arg)) : (0);
  int ctype = FD_PTR_TYPE(seq);
  switch (ctype) {
  case fd_vector_type: case fd_code_type: {
    int len = FD_VECTOR_LENGTH(seq), delta = 1;
    if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
    int ok = interpret_range_args
      (len,"find_if_prim",start_arg,end_arg,&start,&end,&delta);
    if (ok<0) return range_error("find_if_prim",seq,len,start,end);
    lispval *data = FD_VECTOR_ELTS(seq);
    int i = start; while (i!=end) {
      lispval v = fd_apply(test,1,data+i);
      if (FD_ABORTP(v)) return v;
      else if (!(FD_FALSEP(v))) {
        fd_decref(v);
        return fd_incref(data[i]);}
      else i += delta;}
    return fd_incref(fail_val);}
  case fd_compound_type: {
      int len = FD_COMPOUND_VECLEN(seq), delta = 1;
      if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
      lispval *data = FD_COMPOUND_VECELTS(seq);
      int ok = interpret_range_args
        (len,"find_if_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("find_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = fd_apply(test,1,data+i);
        if (FD_ABORTP(v)) return v;
        else if (!(FD_FALSEP(v))) {
          fd_decref(v);
          return fd_incref(data[i]);}
        else i += delta;}
      return fd_incref(fail_val);}
    case fd_pair_type: {
      int i = 0, pos = -1, len = -1, delta = 1;
      lispval scan = seq, pos_elt = FD_VOID;
      if ( (start<0) || (!(FD_FIXNUMP(end_arg))) ||
           ( (FD_FIX2INT(end_arg)) < 0) ) {
        len = fd_list_length(seq);
        if (start<0) start = start+len;
        if (!(FD_FIXNUMP(end_arg)))
          end = len;
        else if ((FD_FIX2INT(end_arg))<0)
          end = len + FD_FIX2INT(end_arg);
        else end = FD_FIX2INT(end_arg);
        int ok = interpret_range_args
          (len,"find_if_prim",start_arg,end_arg,&start,&end,&delta);
        if (ok<0) return range_error("find_if_prim",seq,len,start,end);}
      else end = FD_FIX2INT(end_arg);
      while (FD_PAIRP(scan)) {
        lispval elt = FD_CAR(scan);
        if (i < start) {
          i++; scan=FD_CDR(scan);
          continue;}
        else if (i > end) break;
        lispval v = fd_apply(test,1,&elt);
        if (FD_ABORTP(v)) return v;
        else if (!(FD_FALSEP(v))) {
          fd_decref(v);
          if (start>end) {
            pos=i; pos_elt = elt;}
          else return fd_incref(elt);}
        else {
          scan = FD_CDR(scan);
          i++;}}
      if (pos<0)
        return fd_incref(fail_val);
      else return fd_incref(pos_elt);}
  default: {
      if (NILP(seq))
        return fd_incref(fail_val);
      int len = -1, delta = 1;
      lispval *data = fd_seq_elts(seq,&len);
      if (len < 0) {
        fd_seterr("UnenumerableSequence","find_if_prim",NULL,seq);
        return FD_ERROR_VALUE;}
      if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
      int ok = interpret_range_args
        (len,"find_if_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("find_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = fd_apply(test,1,data+i);
        if (FD_ABORTP(v)) {
          fd_decref_vec(data,len);
          u8_free(data);
          return v;}
        else if (!(FD_FALSEP(v))) {
          lispval elt = data[i]; fd_incref(elt);
          fd_decref(v);
          fd_decref_vec(data,len);
          u8_free(data);
          return elt;}
        else i += delta;}
      fd_decref_vec(data,len);
      u8_free(data);
      return fd_incref(fail_val);}
  }
}

lispval find_if_not_prim(lispval test,lispval seq,lispval start_arg,
                         lispval end_arg,
                         lispval fail_val)
{
  int start = FD_FIX2INT(start_arg), end;
  int ctype = FD_PTR_TYPE(seq);
  switch (ctype) {
  case fd_vector_type: case fd_code_type: {
    int len = FD_VECTOR_LENGTH(seq), delta = 1;
    if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
    int ok = interpret_range_args
      (len,"find_if_not_prim",start_arg,end_arg,&start,&end,&delta);
    if (ok<0) return range_error("find_if_prim",seq,len,start,end);
    lispval *data = FD_VECTOR_ELTS(seq);
    int i = start; while (i!=end) {
      lispval v = fd_apply(test,1,data+i);
      if (FD_ABORTP(v)) return v;
      else if (FD_FALSEP(v)) {
        return fd_incref(data[i]);}
      else {
        fd_decref(v);
        i += delta;}}
    return fd_incref(fail_val);}
  case fd_compound_type: {
      int len = FD_COMPOUND_VECLEN(seq), delta = 1;
      if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
      lispval *data = FD_COMPOUND_VECELTS(seq);
      int ok = interpret_range_args
        (len,"find_if_not_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("find_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = fd_apply(test,1,data+i);
        if (FD_ABORTP(v)) return v;
        else if (FD_FALSEP(v))
          return fd_incref(data[i]);
        else {
          fd_decref(v);
          i += delta;}
        return fd_incref(fail_val);}}
  case fd_pair_type: {
    int i = 0, pos = -1, len = -1, delta = 1;
    lispval scan = seq, pos_elt = FD_VOID;
    if ( (start<0) || (!(FD_FIXNUMP(end_arg))) ||
         ( (FD_FIX2INT(end_arg)) < 0) ) {
      len = fd_list_length(seq);
      if (start<0) start = start+len;
      if (!(FD_FIXNUMP(end_arg)))
          end = len;
        else if ((FD_FIX2INT(end_arg))<0)
          end = len + FD_FIX2INT(end_arg);
        else end = FD_FIX2INT(end_arg);
        int ok = interpret_range_args
          (len,"find_if_not_prim",start_arg,end_arg,&start,&end,&delta);
        if (ok<0) return range_error("find_if_prim",seq,len,start,end);}
      else end = FD_FIX2INT(end_arg);
      while (FD_PAIRP(scan)) {
        lispval elt = FD_CAR(scan);
        if (i < start) {
          i++; scan=FD_CDR(scan);
          continue;}
        else if (i > end) break;
        lispval v = fd_apply(test,1,&elt);
        if (FD_ABORTP(v)) return v;
        else if (FD_FALSEP(v)) {
          if (start>end) {
            pos=i; pos_elt=elt;}
          else return fd_incref(elt);}
        else {
          fd_decref(v);
          scan = FD_CDR(scan);
          i++;}}
      if (pos<0)
        return fd_incref(fail_val);
      else fd_incref(pos_elt);}
  default: {
      if (NILP(seq))
        return fd_incref(fail_val);
      int len = -1, delta = 1;
      lispval *data = fd_seq_elts(seq,&len);
      if (len < 0) {
        fd_seterr("UnenumerableSequence","find_if_prim",NULL,seq);
        return FD_ERROR_VALUE;}
      if (FD_FIXNUMP(end_arg)) end = FD_FIX2INT(end_arg); else end = len;
      int ok = interpret_range_args
        (len,"find_if_not_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("find_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = fd_apply(test,1,data+i);
        if (FD_ABORTP(v)) {
          fd_decref_vec(data,len);
          u8_free(data);
          return v;}
        else if (FD_FALSEP(v)) {
          lispval elt = data[i]; fd_incref(elt);
          fd_decref_vec(data,len);
          u8_free(data);
          return elt;}
        else {
          fd_decref(v);
          i += delta;}}
      fd_decref_vec(data,len);
      u8_free(data);
      return fd_incref(fail_val);}
  }
}

/* Small element functions */

static lispval seq_elt(lispval x,char *cxt,int i)
{
  lispval v = fd_seq_elt(x,i);
  if (FD_TROUBLEP(v))
    return fd_err(fd_retcode_to_exception(v),cxt,NULL,x);
  else return v;
}

static lispval first(lispval x)
{
  return seq_elt(x,"first",0);
}
static lispval rest(lispval x)
{
  if (PAIRP(x)) return fd_incref(FD_CDR(x));
  else {
    lispval v = fd_slice(x,1,-1);
    if (FD_TROUBLEP(v))
      return fd_err(fd_retcode_to_exception(v),"rest",NULL,x);
    else return v;}
}

static lispval second(lispval x)
{
  return seq_elt(x,"second",1);
}
static lispval third(lispval x)
{
  return seq_elt(x,"third",2);
}
static lispval fourth(lispval x)
{
  return seq_elt(x,"fourth",3);
}
static lispval fifth(lispval x)
{
  return seq_elt(x,"fifth",4);
}
static lispval sixth(lispval x)
{
  return seq_elt(x,"sixth",5);
}
static lispval seventh(lispval x)
{
  return seq_elt(x,"seventh",6);
}

/* Pair functions */

static lispval nullp(lispval x)
{
  if (NILP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static lispval cons(lispval x,lispval y)
{
  return fd_make_pair(x,y);
}
static lispval car(lispval x)
{
  return fd_incref(FD_CAR(x));
}
static lispval cdr(lispval x)
{
  return fd_incref(FD_CDR(x));
}
static lispval cddr(lispval x)
{
  if (PAIRP(FD_CDR(x)))
    return fd_incref(FD_CDR(FD_CDR(x)));
  else return fd_err(fd_RangeError,"CDDR",NULL,x);
}
static lispval cadr(lispval x)
{
  if (PAIRP(FD_CDR(x)))
    return fd_incref(FD_CAR(FD_CDR(x)));
  else return fd_err(fd_RangeError,"CADR",NULL,x);
}
static lispval cdar(lispval x)
{
  if (PAIRP(FD_CAR(x)))
    return fd_incref(FD_CDR(FD_CAR(x)));
  else return fd_err(fd_RangeError,"CDAR",NULL,x);
}
static lispval caar(lispval x)
{
  if (PAIRP(FD_CAR(x)))
    return fd_incref(FD_CAR(FD_CAR(x)));
  else return fd_err(fd_RangeError,"CAAR",NULL,x);
}
static lispval caddr(lispval x)
{
  if ((PAIRP(FD_CDR(x)))&&(PAIRP(FD_CDR(FD_CDR(x)))))
    return fd_incref(FD_CAR(FD_CDR(FD_CDR(x))));
  else return fd_err(fd_RangeError,"CADR",NULL,x);
}
static lispval cdddr(lispval x)
{
  if ((PAIRP(FD_CDR(x)))&&(PAIRP(FD_CDR(FD_CDR(x)))))
    return fd_incref(FD_CDR(FD_CDR(FD_CDR(x))));
  else return fd_err(fd_RangeError,"CADR",NULL,x);
}

static lispval cons_star(int n,lispval *args)
{
  int i = n-2; lispval list = fd_incref(args[n-1]);
  while (i>=0) {
    list = fd_conspair(fd_incref(args[i]),list);
    i--;}
  return list;
}

/* Association list functions */

static lispval assq_prim(lispval key,lispval list)
{
  if (NILP(list)) return FD_FALSE;
  else if (!(PAIRP(list)))
    return fd_type_error("alist","assq_prim",list);
  else {
    lispval scan = list;
    while (PAIRP(scan))
      if (!(PAIRP(FD_CAR(scan))))
        return fd_type_error("alist","assq_prim",list);
      else {
        lispval item = FD_CAR(FD_CAR(scan));
        if (FD_EQ(key,item))
          return fd_incref(FD_CAR(scan));
        else scan = FD_CDR(scan);}
    return FD_FALSE;}
}

static lispval assv_prim(lispval key,lispval list)
{
  if (NILP(list)) return FD_FALSE;
  else if (!(PAIRP(list)))
    return fd_type_error("alist","assv_prim",list);
  else {
    lispval scan = list;
    while (PAIRP(scan))
      if (!(PAIRP(FD_CAR(scan))))
        return fd_type_error("alist","assv_prim",list);
      else {
        lispval item = FD_CAR(FD_CAR(scan));
        if (FD_EQV(key,item))
          return fd_incref(FD_CAR(scan));
        else scan = FD_CDR(scan);}
    return FD_FALSE;}
}

static lispval assoc_prim(lispval key,lispval list)
{
  if (NILP(list)) return FD_FALSE;
  else if (!(PAIRP(list)))
    return fd_type_error("alist","assoc_prim",list);
  else {
    lispval scan = list;
    while (PAIRP(scan))
      if (!(PAIRP(FD_CAR(scan))))
        return fd_type_error("alist","assoc_prim",list);
      else {
        lispval item = FD_CAR(FD_CAR(scan));
        if (LISP_EQUAL(key,item))
          return fd_incref(FD_CAR(scan));
        else scan = FD_CDR(scan);}
    return FD_FALSE;}
}

/* MEMBER functions */

static lispval memq_prim(lispval key,lispval list)
{
  if (NILP(list)) return FD_FALSE;
  else if (PAIRP(list)) {
    lispval scan = list;
    while (PAIRP(scan))
      if (FD_EQ(FD_CAR(scan),key))
        return fd_incref(scan);
      else scan = FD_CDR(scan);
    return FD_FALSE;}
  else return fd_type_error("list","memq_prim",list);
}

static lispval memv_prim(lispval key,lispval list)
{
  if (NILP(list)) return FD_FALSE;
  else if (PAIRP(list)) {
    lispval scan = list;
    while (PAIRP(scan))
      if (FD_EQV(FD_CAR(scan),key))
        return fd_incref(scan);
      else scan = FD_CDR(scan);
    return FD_FALSE;}
  else return fd_type_error("list","memv_prim",list);
}

static lispval member_prim(lispval key,lispval list)
{
  if (NILP(list)) return FD_FALSE;
  else if (PAIRP(list)) {
    lispval scan = list;
    while (PAIRP(scan))
      if (FD_EQUAL(FD_CAR(scan),key))
        return fd_incref(scan);
      else scan = FD_CDR(scan);
    return FD_FALSE;}
  else return fd_type_error("list","member_prim",list);
}

/* LIST AND VECTOR */

static lispval list(int n,lispval *elts)
{
  lispval head = NIL, *tail = &head; int i = 0;
  while (i < n) {
    lispval pair = fd_make_pair(elts[i],NIL);
    *tail = pair; tail = &((struct FD_PAIR *)pair)->cdr; i++;}
  return head;
}

static lispval vector(int n,lispval *elts)
{
  int i = 0; while (i < n) {fd_incref(elts[i]); i++;}
  return fd_make_vector(n,elts);
}

static lispval make_vector(lispval size,lispval dflt)
{
  int n = fd_getint(size);
  if (n==0)
    return fd_empty_vector(0);
  else if (n>0) {
    lispval result = fd_empty_vector(n);
    lispval *elts = FD_VECTOR_ELTS(result);
    int i = 0; while (i < n) {
      elts[i]=fd_incref(dflt); i++;}
    return result;}
  else return fd_type_error(_("positive"),"make_vector",size);
}

static lispval seq2vector(lispval seq)
{
  if (NILP(seq))
    return fd_empty_vector(0);
  else if (FD_SEQUENCEP(seq)) {
    int n = -1; lispval *data = fd_seq_elts(seq,&n);
    if (n>=0) {
      lispval result = fd_make_vector(n,data);
      u8_free(data);
      return result;}
    else {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,seq);
      return FD_ERROR_VALUE;}}
  else return fd_type_error(_("sequence"),"seq2vector",seq);
}

static lispval onevector_prim(int n,lispval *args)
{
  lispval elts[32], result = VOID;
  struct U8_PILE pile; int i = 0;
  if (n==0) return fd_empty_vector(0);
  else if (n==1) {
    if (VECTORP(args[0])) return fd_incref(args[0]);
    else if (PAIRP(args[0])) {}
    else if ((EMPTYP(args[0]))||(FD_EMPTY_QCHOICEP(args[0])))
      return fd_empty_vector(0);
    else if (!(CONSP(args[0])))
      return fd_make_vector(1,args);
    else {
      fd_incref(args[0]); return fd_make_vector(1,args);}}
  U8_INIT_STATIC_PILE((&pile),elts,32);
  while (i<n) {
    lispval arg = args[i++];
    DO_CHOICES(each,arg) {
      if (!(CONSP(each))) {u8_pile_add((&pile),each);}
      else if (VECTORP(each)) {
        int len = VEC_LEN(each); int j = 0;
        while (j<len) {
          lispval elt = VEC_REF(each,j); fd_incref(elt);
          u8_pile_add(&pile,elt); j++;}}
      else if (PAIRP(each)) {
        FD_DOLIST(elt,each) {
          fd_incref(elt); u8_pile_add(&pile,elt);}}
      else {
        fd_incref(each); u8_pile_add(&pile,each);}}}
  result = fd_make_vector(pile.u8_len,(lispval *)pile.u8_elts);
  if (pile.u8_mallocd) u8_free(pile.u8_elts);
  return result;
}

static lispval seq2rail(lispval seq)
{
  if (NILP(seq))
    return fd_make_code(0,NULL);
  else if (FD_SEQUENCEP(seq)) {
    int n = -1;
    lispval *data = fd_seq_elts(seq,&n);
    if (n < 0) {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,seq);
      return FD_ERROR;}
    lispval result = fd_make_code(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2rail",seq);
}

static lispval seq2list(lispval seq)
{
  if (NILP(seq)) return NIL;
  else if (FD_SEQUENCEP(seq)) {
    int n = -1;
    lispval *data = fd_seq_elts(seq,&n), result = NIL;
    if (n < 0) {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,seq);
      return FD_ERROR;}
    n--; while (n>=0) {
      result = fd_conspair(data[n],result); n--;}
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2list",seq);
}

static lispval seq2packet(lispval seq)
{
  if (NILP(seq))
    return fd_init_packet(NULL,0,NULL);
  else if (FD_SEQUENCEP(seq)) {
    int i = 0, n = -1;
    lispval result = VOID;
    lispval *data = fd_seq_elts(seq,&n);
    if (n < 0) {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,seq);
      return FD_ERROR;}
    unsigned char *bytes = u8_malloc(n);
    while (i<n) {
      if (FD_BYTEP(data[i])) {
        bytes[i]=FIX2INT(data[i]); i++;}
      else {
        lispval bad = fd_incref(data[i]);
        i = 0; while (i < n) {fd_decref(data[i]); i++;}
        u8_free(data);
        return fd_type_error(_("byte"),"seq2packet",bad);}}
    u8_free(data);
    result = fd_make_packet(NULL,n,bytes);
    u8_free(bytes);
    return result;}
  else return fd_type_error(_("sequence"),"seq2packet",seq);
}

static lispval x2string(lispval seq)
{
  if (SYMBOLP(seq))
    return lispval_string(SYM_NAME(seq));
  else if (FD_CHARACTERP(seq)) {
    int c = FD_CHAR2CODE(seq);
    U8_OUTPUT out; u8_byte buf[16];
    U8_INIT_STATIC_OUTPUT_BUF(out,16,buf);
    u8_putc(&out,c);
    return lispval_string(out.u8_outbuf);}
  else if (NILP(seq)) return lispval_string("");
  else if (STRINGP(seq)) return fd_incref(seq);
  else if (FD_SEQUENCEP(seq)) {
    U8_OUTPUT out;
    int i = 0, n = -1;
    lispval *data = fd_seq_elts(seq,&n);
    if (n < 0) {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,seq);
      return FD_ERROR;}
    U8_INIT_OUTPUT(&out,n*2);
    while (i<n) {
      if (FIXNUMP(data[i])) {
        long long charcode = FIX2INT(data[i]);
        if ((charcode<0)||(charcode>=0x10000)) {
          lispval bad = fd_incref(data[i]);
          i = 0; while (i<n) {fd_decref(data[i]); i++;}
          u8_free(data);
          return fd_type_error(_("character"),"seq2string",bad);}
        u8_putc(&out,charcode); i++;}
      else if (FD_CHARACTERP(data[i])) {
        int charcode = FD_CHAR2CODE(data[i]);
        u8_putc(&out,charcode); i++;}
      else {
        lispval bad = fd_incref(data[i]);
        i = 0; while (i<n) {fd_decref(data[i]); i++;}
        u8_free(data);
        return fd_type_error(_("character"),"seq2string",bad);}}
    u8_free(data);
    return fd_stream2string(&out);}
  else return fd_type_error(_("sequence"),"x2string",seq);
}

FD_EXPORT lispval fd_seq2choice(lispval x)
{
  if (CHOICEP(x)) {
    lispval results = EMPTY;
    DO_CHOICES(e,x) {
      lispval subelts = fd_seq2choice(e);
      CHOICE_ADD(results,subelts);}
    return results;}
  else {
    lispval result = EMPTY;
    int ctype = FD_PTR_TYPE(x), i = 0, len;
    switch (ctype) {
    case fd_vector_type: case fd_code_type:
      len = VEC_LEN(x); while (i < len) {
        lispval elt = fd_incref(VEC_REF(x,i));
        CHOICE_ADD(result,elt); i++;}
      return result;
    case fd_packet_type: case fd_secret_type:
      len = FD_PACKET_LENGTH(x); while (i < len) {
        lispval elt = FD_BYTE2LISP(FD_PACKET_REF(x,i));
        CHOICE_ADD(result,elt); i++;}
      return result;
    case fd_pair_type: {
      lispval scan = x;
      while (PAIRP(scan)) {
        lispval elt = fd_incref(FD_CAR(scan));
        CHOICE_ADD(result,elt);
        scan = FD_CDR(scan);}
      if (!(NILP(scan))) {
        lispval tail = fd_incref(scan);
        CHOICE_ADD(result,tail);}
      return result;}
    case fd_string_type: {
      const u8_byte *scan = CSTRING(x);
      int c = u8_sgetc(&scan);
      while (c>=0) {
        CHOICE_ADD(result,FD_CODE2CHAR(c));
        c = u8_sgetc(&scan);}
      return result;}
    default:
      len = fd_seq_length(x); while (i<len) {
        lispval elt = fd_seq_elt(x,i);
        CHOICE_ADD(result,elt); i++;}
      return result;
    }}
}

static lispval elts_prim(lispval x,lispval start_arg,lispval end_arg)
{
  int start, end;
  lispval check = check_range("elts_prim",x,start_arg,end_arg,&start,&end);
  lispval results = EMPTY;
  int ctype = FD_PTR_TYPE(x);
  if (FD_ABORTED(check)) return check;
  else switch (ctype) {
    case fd_vector_type: case fd_code_type: {
      lispval *read, *limit;
      read = VEC_DATA(x)+start; limit = VEC_DATA(x)+end;
      while (read<limit) {
        lispval v = *read++; fd_incref(v);
        CHOICE_ADD(results,v);}
      return results;}
    case fd_compound_type: {
      struct FD_COMPOUND *compound = (fd_compound) x;
      int off = compound->compound_off;
      if (off < 0)
        return fd_err("NotACompoundSequence","elts_prim",NULL,x);
      lispval *scan = (&(compound->compound_0))+off+start;
      lispval *limit = scan+(end-start);
      while (scan<limit) {
        lispval v = fd_incref(*scan);
        CHOICE_ADD(results,v);
        scan++;}
      return results;}
    case fd_packet_type: case fd_secret_type: {
      const unsigned char *read = FD_PACKET_DATA(x), *lim = read+end;
      while (read<lim) {
        unsigned char v = *read++;
        CHOICE_ADD(results,FD_SHORT2FIX(v));}
      return results;}
    case fd_numeric_vector_type: {
      int len = FD_NUMVEC_LENGTH(x);
      int i = start, lim = ((end<0)?(len+end):(end));
      if (lim>len) lim = len;
      switch (FD_NUMVEC_TYPE(x)) {
      case fd_short_elt: {
        fd_short *vec = FD_NUMVEC_SHORTS(x);
        while (i<lim) {
          fd_short num = vec[i++];
          lispval elt = FD_SHORT2FIX(num);
          CHOICE_ADD(results,elt);
          i++;}}
      case fd_int_elt: {
        fd_int *vec = FD_NUMVEC_INTS(x);
        while (i<lim) {
          fd_int num = vec[i++]; lispval elt = FD_INT(num);
          CHOICE_ADD(results,elt);}
        break;}
      case fd_long_elt: {
        fd_long *vec = FD_NUMVEC_LONGS(x);
        while (i<lim) {
          fd_long num = vec[i++]; lispval elt = FD_INT(num);
          CHOICE_ADD(results,elt);}
        break;}
      case fd_float_elt: {
        fd_float *vec = FD_NUMVEC_FLOATS(x);
        while (i<lim) {
          fd_float num = vec[i++]; lispval elt = fd_make_flonum(num);
          CHOICE_ADD(results,elt);}
        break;}
      case fd_double_elt: {
        fd_double *vec = FD_NUMVEC_DOUBLES(x);
        while (i<lim) {
          fd_double num = vec[i++]; lispval elt = fd_make_flonum(num);
          CHOICE_ADD(results,elt);}
        break;}
      default: {
        fd_seterr(_("Corrputed numerical vector"),"fd_seq2choice",NULL,x);
        fd_incref(x); fd_decref(results);
        results = FD_ERROR;}}
      break;}
    case fd_pair_type: {
      int j = 0; lispval scan = x;
      while (PAIRP(scan))
        if (j == end) return results;
        else if (j>=start) {
          lispval car = FD_CAR(scan); fd_incref(car);
          CHOICE_ADD(results,car);
          j++; scan = FD_CDR(scan);}
        else {j++; scan = FD_CDR(scan);}
      return results;}
    case fd_string_type: {
      int count = 0, c;
      const u8_byte *scan = CSTRING(x);
      while ((c = u8_sgetc(&scan))>=0)
        if (count<start) count++;
        else if (count>=end) break;
        else {CHOICE_ADD(results,FD_CODE2CHAR(c));}
      return results;}
    default:
      if (NILP(x))
        if ((start == end) && (start == 0))
          return EMPTY;
        else return FD_RANGE_ERROR;
      else if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->elt)) {
        int scan = start; while (scan<end) {
          lispval elt = (fd_seqfns[ctype]->elt)(x,scan);
          CHOICE_ADD(results,elt);
          scan++;}
        return results;}
      else if (fd_seqfns[ctype]==NULL)
        return fd_type_error(_("sequence"),"elts_prim",x);
      else return fd_err(fd_NoMethod,"elts_prim",NULL,x);}
  return results;
}

static lispval vec2elts_prim(lispval x)
{
  if (VECTORP(x)) {
    lispval result = EMPTY;
    int i = 0; int len = VEC_LEN(x); lispval *elts = FD_VECTOR_ELTS(x);
    while (i<len) {
      lispval e = elts[i++]; fd_incref(e); CHOICE_ADD(result,e);}
    return result;}
  else return fd_incref(x);
}

/* Vector length predicates */

static lispval veclen_lt_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return FD_FALSE;
  else if (VEC_LEN(x)<FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval veclen_lte_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return FD_FALSE;
  else if (VEC_LEN(x)<=FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval veclen_gt_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return FD_FALSE;
  else if (VEC_LEN(x)>FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval veclen_gte_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return FD_FALSE;
  else if (VEC_LEN(x)>=FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval veclen_eq_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return FD_FALSE;
  else if (VEC_LEN(x) == FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

/* Sequence length predicates */

static lispval seqlen_lt_prim(lispval x,lispval len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x)<FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval seqlen_lte_prim(lispval x,lispval len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x)<=FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval seqlen_gt_prim(lispval x,lispval len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x)>FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval seqlen_gte_prim(lispval x,lispval len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x)>=FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval seqlen_eq_prim(lispval x,lispval len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x) == FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

/* Matching vectors */

static lispval seqmatch_prim(lispval prefix,lispval seq,lispval startarg)
{
  if (!(FD_UINTP(startarg)))
    return fd_type_error("uint","seqmatchprim",startarg);
  int start = FIX2INT(startarg);
  if ((VECTORP(prefix))&&(VECTORP(seq))) {
    int plen = VEC_LEN(prefix);
    int seqlen = VEC_LEN(seq);
    if ((start+plen)<=seqlen) {
      int j = 0; int i = start;
      while (j<plen)
        if (FD_EQUAL(VEC_REF(prefix,j),VEC_REF(seq,i))) {
          j++; i++;}
        else return FD_FALSE;
      return FD_TRUE;}
    else return FD_FALSE;}
  else if ((STRINGP(prefix))&&(STRINGP(seq))) {
    int plen = STRLEN(prefix);
    int seqlen = STRLEN(seq);
    int off = u8_byteoffset(CSTRING(seq),start,seqlen);
    if ((off+plen)<=seqlen)
      if (strncmp(CSTRING(prefix),CSTRING(seq)+off,plen)==0)
        return FD_TRUE;
      else return FD_FALSE;
    else return FD_FALSE;}
  else if (!(FD_SEQUENCEP(prefix)))
    return fd_type_error("sequence","seqmatch_prim",prefix);
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","seqmatch_prim",seq);
  else {
    int plen = fd_seq_length(prefix);
    int seqlen = fd_seq_length(seq);
    if ((start+plen)<=seqlen) {
      int j = 0; int i = start;
      while (j<plen) {
        lispval pelt = fd_seq_elt(prefix,j);
        lispval velt = fd_seq_elt(prefix,i);
        int cmp = FD_EQUAL(pelt,velt);
        fd_decref(pelt); fd_decref(velt);
        if (cmp) {j++; i++;}
        else return FD_FALSE;}
      return FD_TRUE;}
    else return FD_FALSE;}
}

/* Sorting vectors */

static lispval sortvec_primfn(lispval vec,lispval keyfn,int reverse,int lexsort)
{
  if (VEC_LEN(vec)==0)
    return fd_empty_vector(0);
  else if (VEC_LEN(vec)==1)
    return fd_incref(vec);
  else {
    int i = 0, n = VEC_LEN(vec), j = 0;
    lispval result = fd_empty_vector(n);
    lispval *vecdata = FD_VECTOR_ELTS(result);
    struct FD_SORT_ENTRY *sentries = u8_alloc_n(n,struct FD_SORT_ENTRY);
    while (i<n) {
      lispval elt = VEC_REF(vec,i);
      lispval value=_fd_apply_keyfn(elt,keyfn);
      if (FD_ABORTED(value)) {
        int j = 0; while (j<i) {fd_decref(sentries[j].sortval); j++;}
        u8_free(sentries); u8_free(vecdata);
        return value;}
      sentries[i].sortval = elt;
      sentries[i].sortkey = value;
      i++;}
    if (lexsort)
      qsort(sentries,n,sizeof(struct FD_SORT_ENTRY),_fd_lexsort_helper);
    else qsort(sentries,n,sizeof(struct FD_SORT_ENTRY),_fd_sort_helper);
    i = 0; j = n-1; if (reverse) while (i < n) {
      fd_decref(sentries[i].sortkey);
      vecdata[j]=fd_incref(sentries[i].sortval);
      i++; j--;}
    else while (i < n) {
      fd_decref(sentries[i].sortkey);
      vecdata[i]=fd_incref(sentries[i].sortval);
      i++;}
    u8_free(sentries);
    return result;}
}

static lispval sortvec_prim(lispval vec,lispval keyfn)
{
  return sortvec_primfn(vec,keyfn,0,0);
}

static lispval lexsortvec_prim(lispval vec,lispval keyfn)
{
  return sortvec_primfn(vec,keyfn,0,1);
}

static lispval rsortvec_prim(lispval vec,lispval keyfn)
{
  return sortvec_primfn(vec,keyfn,1,0);
}

/* RECONS reconstitutes CONSes, returning the original if
   nothing has changed. This is handy for some recursive list
   functions. */

static lispval recons_prim(lispval car,lispval cdr,lispval orig)
{
  if ((FD_EQ(car,FD_CAR(orig)))&&(FD_EQ(cdr,FD_CDR(orig))))
    return fd_incref(orig);
  else {
    lispval cons = fd_conspair(car,cdr);
    fd_incref(car); fd_incref(cdr);
    return cons;}
}

/* Numeric vectors */

static lispval make_short_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = fd_make_numeric_vector(n,fd_short_elt);
  fd_short *elts = FD_NUMVEC_SHORTS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (FD_SHORTP(elt))
      elts[i++]=(fd_short)(FIX2INT(elt));
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("short element"),"make_short_vector",elt);}}
  return vec;
}

static lispval seq2shortvec(lispval arg)
{
  if ((TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg) == fd_short_elt)) {
    fd_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_short_vector(VEC_LEN(arg),FD_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return fd_make_short_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = fd_seq_elts(arg,&n);
    if (n < 0) {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return FD_ERROR;}
    lispval result = make_short_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2shortvec",arg);
}

static lispval make_int_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = fd_make_numeric_vector(n,fd_int_elt);
  fd_int *elts = FD_NUMVEC_INTS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (FD_INTP(elt))
      elts[i++]=((fd_int)(FIX2INT(elt)));
    else if ((FD_BIGINTP(elt))&&
             (fd_bigint_fits_in_word_p((fd_bigint)elt,sizeof(fd_int),1)))
      elts[i++]=fd_bigint_to_long((fd_bigint)elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("int element"),"make_int_vector",elt);}}
  return vec;
}

static lispval seq2intvec(lispval arg)
{
  if ((TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg) == fd_int_elt)) {
    fd_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_int_vector(VEC_LEN(arg),FD_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return fd_make_int_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = fd_seq_elts(arg,&n);
    if (FD_EXPECT_FALSE(n < 0)) {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return FD_ERROR;}
    lispval result = make_int_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2intvec",arg);
}

static lispval make_long_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = fd_make_numeric_vector(n,fd_long_elt);
  fd_long *elts = FD_NUMVEC_LONGS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (FIXNUMP(elt))
      elts[i++]=((fd_long)(FIX2INT(elt)));
    else if (FD_BIGINTP(elt))
      elts[i++]=fd_bigint_to_long_long((fd_bigint)elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("flonum element"),"make_long_vector",elt);}}
  return vec;
}

static lispval seq2longvec(lispval arg)
{
  if ((TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg) == fd_long_elt)) {
    fd_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_long_vector(VEC_LEN(arg),FD_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return fd_make_long_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = fd_seq_elts(arg,&n);
    if (FD_EXPECT_TRUE(n < 0)) {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return FD_ERROR;}
    lispval result = make_long_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2longvec",arg);
}

static lispval make_float_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = fd_make_numeric_vector(n,fd_float_elt);
  float *elts = FD_NUMVEC_FLOATS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (FD_FLONUMP(elt))
      elts[i++]=FD_FLONUM(elt);
    else if (NUMBERP(elt))
      elts[i++]=fd_todouble(elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("float element"),"make_float_vector",elt);}}
  return vec;
}

static lispval seq2floatvec(lispval arg)
{
  if ((TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg) == fd_float_elt)) {
    fd_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_float_vector(VEC_LEN(arg),FD_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return fd_make_float_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = fd_seq_elts(arg,&n);
    if (FD_EXPECT_TRUE(n < 0)) {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return FD_ERROR;}
    lispval result = make_float_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2floatvec",arg);
}

static lispval make_double_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = fd_make_numeric_vector(n,fd_double_elt);
  double *elts = FD_NUMVEC_DOUBLES(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (FD_FLONUMP(elt))
      elts[i++]=FD_FLONUM(elt);
    else if (NUMBERP(elt))
      elts[i++]=fd_todouble(elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("double(float) element"),"make_double_vector",elt);}}
  return vec;
}

static lispval seq2doublevec(lispval arg)
{
  if ((TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg) == fd_double_elt)) {
    fd_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_double_vector(VEC_LEN(arg),FD_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return fd_make_double_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = fd_seq_elts(arg,&n);
    if (FD_EXPECT_TRUE(n < 0)) {
      fd_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return FD_ERROR;}
    lispval result = make_double_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2doublevec",arg);
}

/* Rails */

static lispval make_code(int n,lispval *elts)
{
  int i = 0; while (i<n) {
    lispval v = elts[i++]; fd_incref(v);}
  return fd_make_code(n,elts);
}

/* side effecting operations (not threadsafe) */

static lispval set_car(lispval pair,lispval val)
{
  struct FD_PAIR *p = fd_consptr(struct FD_PAIR *,pair,fd_pair_type);
  lispval oldv = p->car; p->car = fd_incref(val);
  fd_decref(oldv);
  return VOID;
}

static lispval set_cdr(lispval pair,lispval val)
{
  struct FD_PAIR *p = fd_consptr(struct FD_PAIR *,pair,fd_pair_type);
  lispval oldv = p->cdr; p->cdr = fd_incref(val);
  fd_decref(oldv);
  return VOID;
}

static lispval vector_set(lispval vec,lispval index,lispval val)
{
  struct FD_VECTOR *v = fd_consptr(struct FD_VECTOR *,vec,fd_vector_type);
  if (!(FD_UINTP(index))) return fd_type_error("uint","vector_set",index);
  int offset = FIX2INT(index); lispval *elts = v->vec_elts;
  if (offset>v->vec_length) {
    char buf[32];
    return fd_err(fd_RangeError,"vector_set",
                  u8_uitoa10(offset,buf),
                  vec);}
  else {
    lispval oldv = elts[offset];
    elts[offset]=fd_incref(val);
    fd_decref(oldv);
    return VOID;}
}

FD_EXPORT void fd_init_seqprims_c()
{
  u8_register_source_file(_FILEINFO);

  /* Generic sequence functions */
  fd_idefn(fd_scheme_module,fd_make_cprim1("SEQUENCE?",sequencep_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LENGTH",seqlen_prim,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH=",has_length_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH>",has_length_gt_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH>=",has_length_gte_prim,2));
  fd_defalias(fd_scheme_module,"LENGTH=>","LENGTH>=");
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH<",has_length_lt_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("LENGTH=<",has_length_lte_prim,2));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("LENGTH>0",has_length_gt_zero_prim,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1("LENGTH>1",has_length_gt_one_prim,1));
  fd_defalias(fd_scheme_module,"LENGTH<=","LENGTH=<");

  fd_idefn(fd_scheme_module,fd_make_cprim2("ELT",seqelt_prim,2));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("SLICE",slice_prim,2,
                           -1,VOID,-1,VOID,
                           -1,FD_FALSE));
  fd_defalias(fd_scheme_module,"SUBSEQ","SLICE");

  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("POSITION",position_prim,2,
                           -1,VOID,-1,VOID,
                           -1,FD_INT(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("RPOSITION",rposition_prim,2,
                           -1,VOID,-1,VOID,
                           -1,FD_INT(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("FIND",find_prim,2,
                           -1,VOID,-1,VOID,
                           -1,FD_INT(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("SEARCH",search_prim,2,
                           -1,VOID,-1,VOID,
                           -1,FD_INT(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,fd_make_cprim1("REVERSE",fd_reverse,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("APPEND",fd_append,0));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("MATCH?",seqmatch_prim,2,
                           -1,VOID,-1,VOID,
                           fd_fixnum_type,FD_INT(0)));

  fd_idefn4(fd_scheme_module,"POSITION-IF",position_if_prim,2,
            "`(POSITION-IF *test* *sequence* [*start*] [*end*])` "
            "returns the position of element of *sequence* for which "
            "*test* returns true. POSITION-IF searches between "
            "between *start* and *end*, which default to "
            "zero and the length of the sequence. if *start* > *end*, this "
            "returns the last position in the range, otherwise it returns "
            "the first. If either *start* or *end* are negative, "
            "they are taken as offsets from the end of the sequence.",
            -1,VOID,-1,VOID,-1,FD_INT(0),-1,FD_FALSE);

  fd_idefn4(fd_scheme_module,"POSITION-IF-NOT",position_if_not_prim,2,
            "`(POSITION-IF-NOT *test* *sequence* [*start*] [*end*])` "
            "returns the position of element of *sequence* for which "
            "*test* returns false. POSITION-IF-NOT searches between "
            "between *start* and *end*, which default to "
            "zero and the length of the sequence. if *start* > *end*, this "
            "returns the last position in the range, otherwise it returns "
            "the first. If either *start* or *end* are negative, "
            "they are taken as offsets from the end of the sequence.",
            -1,VOID,-1,VOID,-1,FD_INT(0),-1,FD_FALSE);

  fd_idefn5(fd_scheme_module,"FIND-IF",find_if_prim,2,
            "`(FIND-IF *test* *sequence* [*start*] [*end*] [*noneval*])` "
            "returns an element of *sequence* for which *test* returns true "
            "or *noneval* otherwise."
            "FIND-IF searches between between *start* and *end*, defaulting "
            "to zero and the length of the sequence. if *start* > *end*, this "
            "returns the last occurrence in the range, otherwise it returns "
            "the first. If either *start* or *end* are negative, "
            "they are taken as offsets from the end of the sequence.",
            -1,VOID,-1,VOID,-1,FD_INT(0),-1,FD_FALSE,-1,FD_FALSE);

  fd_idefn5(fd_scheme_module,"FIND-IF-NOT",find_if_not_prim,2,
            "`(FIND-IF-NOT *test* *sequence* [*start*] [*end*] [*noneval*])` "
            "returns an element of *sequence* for which *test* returns false "
            "or *noneval* otherwise."
            "FIND-IF_NOT searches between between *start* and *end*, which "
            "default to zero and the length of the sequence. "
            "if *start* > *end*, this returns the last occurrence "
            "in the range, otherwise it returns the first. "
            "If either *start* or *end* are negative, they are "
            "taken as offsets from the end of the sequence.",
            -1,VOID,-1,VOID,-1,FD_INT(0),-1,FD_FALSE,-1,FD_FALSE);

  /* Initial sequence functions */
  fd_idefn(fd_scheme_module,fd_make_cprim1("FIRST",first,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SECOND",second,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("THIRD",third,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FOURTH",fourth,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("FIFTH",fifth,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SIXTH",sixth,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("SEVENTH",seventh,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("REST",rest,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("EMPTY-LIST?",nullp,1));
  fd_defalias(fd_scheme_module,"NULL?","EMPTY-LIST?");
  fd_defalias(fd_scheme_module,"NIL?","EMPTY-LIST?");

  /* Standard pair functions */
  fd_idefn(fd_scheme_module,fd_make_cprim2("CONS",cons,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("CONS*",cons_star,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CAR",car,1,fd_pair_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDR",cdr,1,fd_pair_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CADR",cadr,1,fd_pair_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDDR",cddr,1,fd_pair_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CAAR",caar,1,fd_pair_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDAR",cdar,1,fd_pair_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CADDR",caddr,1,fd_pair_type,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDDDR",cdddr,1,fd_pair_type,VOID));


  /* Assoc functions */
  fd_idefn(fd_scheme_module,fd_make_cprim2("ASSQ",assq_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("ASSV",assv_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("ASSOC",assoc_prim,2));

  /* Member functions */
  fd_idefn(fd_scheme_module,fd_make_cprim2("MEMQ",memq_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MEMV",memv_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MEMBER",member_prim,2));


  /* Standard lexprs */
  fd_idefn(fd_scheme_module,fd_make_cprimn("LIST",list,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("VECTOR",vector,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("MAKE-CODE",make_code,0));
  fd_defalias(fd_scheme_module,"MAKE-RAIL","MAKE-CODE");

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("MAKE-VECTOR",make_vector,1,
                           fd_fixnum_type,VOID,
                           -1,FD_FALSE));

  fd_idefn(fd_scheme_module,fd_make_cprim1("->CODE",seq2rail,1));
  fd_defalias(fd_scheme_module,"->RAIL","->CODE");

  fd_idefn(fd_scheme_module,fd_make_cprim1("->VECTOR",seq2vector,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->LIST",seq2list,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->STRING",x2string,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->PACKET",seq2packet,1));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprimn("1VECTOR",onevector_prim,0)));
  fd_defalias(fd_scheme_module,"ONEVECTOR","1VECTOR");
  fd_idefn(fd_scheme_module,fd_make_cprim1("->SHORTVEC",seq2shortvec,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->INTVEC",seq2intvec,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->LONGVEC",seq2longvec,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->FLOATVEC",seq2floatvec,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->DOUBLEVEC",seq2doublevec,1));

  fd_idefn(fd_scheme_module,fd_make_cprimn("SHORTVEC",make_short_vector,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("INTVEC",make_int_vector,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("LONGVEC",make_long_vector,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("FLOATVEC",make_float_vector,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("DOUBLEVEC",make_double_vector,0));

  fd_idefn(fd_scheme_module,fd_make_cprim3("ELTS",elts_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprimn("MAP",mapseq_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprimn("FOR-EACH",foreach_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("MAP->CHOICE",fd_map2choice,2));
  fd_idefn(fd_scheme_module,fd_make_cprim3("REDUCE",fd_reduce,2));
  fd_idefn(fd_scheme_module,fd_make_cprim2("REMOVE",fd_remove,2));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2("REMOVE-IF",removeif_prim,2)));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim(fd_make_cprim2
                          ("REMOVE-IF-NOT",removeifnot_prim,2)));

  fd_idefn(fd_scheme_module,fd_make_cprim4("SOME?",some_prim,2));
  fd_idefn(fd_scheme_module,fd_make_cprim4("EVERY?",every_prim,2));

  fd_idefn(fd_scheme_module,fd_make_cprim1("VECTOR->ELTS",vec2elts_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("VECLEN>=?",veclen_gte_prim,2,-1,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("VECLEN>?",veclen_gt_prim,2,-1,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("VECLEN=?",veclen_eq_prim,2,-1,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("VECLEN<?",veclen_lt_prim,2,-1,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("VECLEN<=?",veclen_lte_prim,2,-1,VOID,fd_fixnum_type,VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN>=?",seqlen_gte_prim,2,-1,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN>?",seqlen_gt_prim,2,-1,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN=?",seqlen_eq_prim,2,-1,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN<?",seqlen_lt_prim,2,-1,VOID,fd_fixnum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN<=?",seqlen_lte_prim,2,-1,VOID,fd_fixnum_type,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SORTVEC",sortvec_prim,1,
                           fd_vector_type,VOID,
                           -1,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("LEXSORTVEC",lexsortvec_prim,1,
                           fd_vector_type,VOID,
                           -1,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("RSORTVEC",rsortvec_prim,1,
                           fd_vector_type,VOID,
                           -1,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("RECONS",recons_prim,3,
                           -1,VOID,-1,VOID,
                           fd_pair_type,VOID));

  /* Note that these are not threadsafe */
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SET-CAR!",set_car,2,
                           fd_pair_type,VOID,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SET-CDR!",set_cdr,2,
                           fd_pair_type,VOID,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("VECTOR-SET!",vector_set,3,
                           fd_vector_type,VOID,fd_fixnum_type,VOID,
                           -1,VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
