/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif
/* FD_MODULE=scheme */

#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fdkbase.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/numbers.h"
#include "framerd/sorting.h"
#include "framerd/seqprims.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>

#include <limits.h>

static fd_exception SequenceMismatch=_("Mismatch of sequence lengths");
static fd_exception EmptyReduce=_("No sequence elements to reduce");

#define FD_EQV(x,y) \
  ((FD_EQ(x,y)) || \
   ((FD_NUMBERP(x)) && (FD_NUMBERP(y)) && \
    (fd_numcompare(x,y)==0)))

#define string_start(bytes,i) ((i==0) ? (bytes) : (u8_substring(bytes,i)))

static fdtype make_float_vector(int n,fdtype *from_elts);
static fdtype make_double_vector(int n,fdtype *from_elts);
static fdtype make_short_vector(int n,fdtype *from_elts);
static fdtype make_int_vector(int n,fdtype *from_elts);
static fdtype make_long_vector(int n,fdtype *from_elts);

/* Exported primitives */

FD_EXPORT int applytest(fdtype test,fdtype elt)
{
  if (FD_HASHSETP(test))
    return fd_hashset_get(FD_XHASHSET(test),elt);
  else if (FD_HASHTABLEP(test))
    return fd_hashtable_probe(FD_XHASHTABLE(test),elt);
  else if ((FD_SYMBOLP(test)) || (FD_OIDP(test)))
    return fd_test(elt,test,FD_VOID);
  else if (FD_TABLEP(test))
    return fd_test(test,elt,FD_VOID);
  else if (FD_APPLICABLEP(test)) {
    fdtype result=fd_apply(test,1,&elt);
    if (FD_FALSEP(result)) return 0;
    else {fd_decref(result); return 1;}}
  else if (FD_EMPTY_CHOICEP(test)) return 0;
  else if (FD_CHOICEP(test)) {
    FD_DO_CHOICES(each_test,test) {
      int result=applytest(each_test,elt);
      if (result<0) return result;
      else if (result) return result;}
    return 0;}
  else {
    fd_seterr(fd_TypeError,"removeif",u8_strdup(_("test")),test);
    return -1;}
}

FD_EXPORT fdtype fd_removeif(fdtype test,fdtype sequence,int invert)
{
  if (!(FD_SEQUENCEP(sequence)))
    return fd_type_error("sequence","fd_remove",sequence);
  else if (FD_EMPTY_LISTP(sequence)) return sequence;
  else {
    int i=0, j=0, removals=0, len=fd_seq_length(sequence);
    fd_ptr_type result_type=FD_PTR_TYPE(sequence);
    fdtype *results=u8_alloc_n(len,fdtype), result;
    while (i < len) {
      fdtype elt=fd_seq_elt(sequence,i);
      int compare=applytest(test,elt); i++;
      if (compare<0) {u8_free(results); return -1;}
      else if ((invert) ? (!(compare)) : (compare)) {
        removals++; fd_decref(elt);}
      else results[j++]=elt;}
    if (removals) {
      result=fd_makeseq(result_type,j,results);
      i=0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return result;}
    else {
      i=0; while (i<j) {fd_decref(results[i]); i++;}
      u8_free(results);
      return fd_incref(sequence);}}
}

/* Mapping */

FD_EXPORT fdtype fd_mapseq(fdtype fn,int n_seqs,fdtype *sequences)
{
  int i=1, seqlen=-1;
  fdtype firstseq=sequences[0];
  fdtype result, *results, _argvec[8], *argvec=NULL;
  fd_ptr_type result_type=FD_PTR_TYPE(firstseq);
  if (FD_FCNIDP(fn)) fn=fd_fcnid_ref(fn);
  if ((FD_TABLEP(fn)) || (FD_ATOMICP(fn))) {
    if (n_seqs>1)
      return fd_err(fd_TooManyArgs,"fd_mapseq",NULL,fn);
    else if (FD_EMPTY_LISTP(firstseq)) 
      return firstseq;}
  if (!(FD_SEQUENCEP(firstseq)))
    return fd_type_error("sequence","fd_mapseq",firstseq);
  else seqlen=fd_seq_length(firstseq);
  while (i<n_seqs)
    if (!(FD_SEQUENCEP(sequences[i])))
      return fd_type_error("sequence","fd_mapseq",sequences[i]);
    else if (seqlen!=fd_seq_length(sequences[i]))
      return fd_err(SequenceMismatch,"fd_foreach",NULL,sequences[i]);
    else i++;
  if (seqlen==0) return fd_incref(firstseq);
  enum { applyfn, tablefn, oidfn, getfn } fntype;
  results=u8_alloc_n(seqlen,fdtype);
  if (FD_APPLICABLEP(fn)) {
    if (n_seqs<8) argvec=_argvec;
    else argvec=u8_alloc_n(n_seqs,fdtype);
    fntype=applyfn;}
  else if (FD_OIDP(fn))
    fntype=oidfn;
  else if (FD_TABLEP(fn))
    fntype=tablefn;
  else fntype=getfn;
  i=0; while (i < seqlen) {
    fdtype elt=fd_seq_elt(firstseq,i), new_elt;
    if (fntype==applyfn) {
      int j=1; while (j<n_seqs) {
        argvec[j]=fd_seq_elt(sequences[j],i); j++;}
      argvec[0]=elt;
      new_elt=fd_apply(fn,n_seqs,argvec);
      j=1; while (j<n_seqs) {fd_decref(argvec[j]); j++;}}
    else if (fntype==tablefn)
      new_elt=fd_get(fn,elt,elt);
    else if (fntype==oidfn)
      new_elt=fd_frame_get(elt,fn);
    else new_elt=fd_get(elt,fn,elt);
    fd_decref(elt);
    if (result_type == fd_string_type) {
      if (!(FD_TYPEP(new_elt,fd_character_type)))
        result_type=fd_vector_type;}
    else if ((result_type == fd_packet_type)||
             (result_type == fd_secret_type)) {
      if (FD_FIXNUMP(new_elt)) {
        long long intval=FD_FIX2INT(new_elt);
        if ((intval<0) || (intval>=0x100))
          result_type=fd_vector_type;}
      else result_type=fd_vector_type;}
    if (FD_ABORTED(new_elt)) {
      int j=0; while (j<i) {fd_decref(results[j]); j++;}
      u8_free(results);
      return new_elt;}
    else results[i++]=new_elt;}
  result=fd_makeseq(result_type,seqlen,results);
  i=0; while (i<seqlen) {fd_decref(results[i]); i++;}
  u8_free(results);
  return result;
}
static fdtype mapseq_prim(int n,fdtype *args)
{
  return fd_mapseq(args[0],n-1,args+1);
}

FD_EXPORT fdtype fd_foreach(fdtype fn,int n_seqs,fdtype *sequences)
{
  int i=0, seqlen=-1;
  fdtype firstseq=sequences[0];
  fdtype _argvec[8], *argvec=NULL;
  fd_ptr_type result_type=FD_PTR_TYPE(firstseq);
  if ((FD_TABLEP(fn)) || ((FD_ATOMICP(fn)) && (FD_FCNIDP(fn)))) {
    if (n_seqs>1)
      return fd_err(fd_TooManyArgs,"fd_foreach",NULL,fn);
    else if (FD_EMPTY_LISTP(firstseq)) 
      return firstseq;}
  if (!(FD_SEQUENCEP(firstseq)))
    return fd_type_error("sequence","fd_mapseq",firstseq);
  else seqlen=fd_seq_length(firstseq);
  while (i<n_seqs)
    if (!(FD_SEQUENCEP(sequences[i])))
      return fd_type_error("sequence","fd_mapseq",sequences[i]);
    else if (seqlen!=fd_seq_length(sequences[i]))
      return fd_err(SequenceMismatch,"fd_foreach",NULL,sequences[i]);
    else i++;
  if (seqlen==0) return FD_VOID;
  enum { applyfn, tablefn, oidfn, getfn } fntype;
  if (FD_APPLICABLEP(fn)) {
    if (n_seqs<8) argvec=_argvec;
    else argvec=u8_alloc_n(n_seqs,fdtype);
    fntype=applyfn;}
  else if (FD_OIDP(fn))
    fntype=oidfn;
  else if (FD_TABLEP(fn))
    fntype=tablefn;
  else fntype=getfn;
  i=0; while (i < seqlen) {
    fdtype elt=argvec[0]=fd_seq_elt(firstseq,i), new_elt;
    if (fntype==tablefn)
      new_elt=fd_get(fn,elt,elt);
    else if (fntype==oidfn)
      new_elt=fd_frame_get(elt,fn);
    else if (fntype==applyfn) {
      int j=1; while (j<n_seqs) {
        argvec[j]=fd_seq_elt(sequences[j],i); j++;}
      argvec[0]=elt;
      new_elt=fd_apply(fn,n_seqs,argvec);
      j=1; while (j<n_seqs) {
        fd_decref(argvec[j]); j++;}}
    else new_elt=fd_get(elt,fn,elt);
    fd_decref(elt);
    if (result_type == fd_string_type) {
      if (!(FD_TYPEP(new_elt,fd_character_type)))
        result_type=fd_vector_type;}
    else if ((result_type == fd_packet_type)||
             (result_type == fd_secret_type)) {
      if (FD_FIXNUMP(new_elt)) {
        long long intval=FD_FIX2INT(new_elt);
        if ((intval<0) || (intval>=0x100))
          result_type=fd_vector_type;}
      else result_type=fd_vector_type;}
    if (FD_ABORTED(new_elt)) return new_elt;
    else {fd_decref(new_elt); i++;}}
  return FD_VOID;
}
FD_CPRIM(foreach_prim,"FOR-EACH",int n,fdtype *args)
/* This iterates over a sequence or set of sequences and applies
   FN to each of the elements. */
{
  return fd_foreach(args[0],n-1,args+1);
}

FD_EXPORT fdtype fd_map2choice(fdtype fn,fdtype sequence)
{
  if (!(FD_SEQUENCEP(sequence)))
    return fd_type_error("sequence","fd_mapseq",sequence);
  else if (FD_EMPTY_LISTP(sequence)) return sequence;
  else if ((FD_APPLICABLEP(fn)) || (FD_TABLEP(fn)) || (FD_ATOMICP(fn))) {
    int i=0, len=fd_seq_length(sequence);
    fd_ptr_type result_type=FD_PTR_TYPE(sequence);
    fdtype *results=u8_alloc_n(len,fdtype);
    while (i < len) {
      fdtype elt=fd_seq_elt(sequence,i), new_elt;
      if (FD_TABLEP(fn))
        new_elt=fd_get(fn,elt,elt);
      else if (FD_APPLICABLEP(fn))
        new_elt=fd_apply(fn,1,&elt);
      else if (FD_OIDP(elt))
        new_elt=fd_frame_get(elt,fn);
      else new_elt=fd_get(elt,fn,elt);
      fd_decref(elt);
      if (result_type == fd_string_type) {
        if (!(FD_TYPEP(new_elt,fd_character_type)))
          result_type=fd_vector_type;}
      else if ((result_type == fd_packet_type)||
               (result_type == fd_secret_type)) {
        if (FD_FIXNUMP(new_elt)) {
          long long intval=FD_FIX2INT(new_elt);
          if ((intval<0) || (intval>=0x100))
            result_type=fd_vector_type;}
        else result_type=fd_vector_type;}
      if (FD_ABORTED(new_elt)) {
        int j=0; while (j<i) {fd_decref(results[j]); j++;}
        u8_free(results);
        return new_elt;}
      else results[i++]=new_elt;}
    return fd_make_choice(len,results,0);}
  else return fd_err(fd_NotAFunction,"MAP",NULL,fn);
}

/* Reduction */

FD_EXPORT fdtype fd_reduce(fdtype fn,fdtype sequence,fdtype result)
{
  int i=0, len=fd_seq_length(sequence);
  if (len==0)
    if (FD_VOIDP(result))
      return fd_err(EmptyReduce,"fd_reduce",NULL,sequence);
    else return fd_incref(result);
  else if (len<0)
    return FD_ERROR_VALUE;
  else if (!(FD_APPLICABLEP(fn)))
    return fd_err(fd_NotAFunction,"MAP",NULL,fn);
  else if (FD_VOIDP(result)) {
    result=fd_seq_elt(sequence,0); i=1;}
  while (i < len) {
    fdtype elt=fd_seq_elt(sequence,i), rail[2], new_result;
    rail[0]=elt; rail[1]=result;
    new_result=fd_apply(fn,2,rail);
    fd_decref(result); fd_decref(elt);
    if (FD_ABORTP(new_result)) return new_result;
    result=new_result;
    i++;}
  return result;
}

/* Scheme primitives */

static fdtype sequencep_prim(fdtype x)
{
  if (FD_SEQUENCEP(x)) return FD_TRUE; else return FD_FALSE;
}

static fdtype seqlen_prim(fdtype x)
{
  int len=fd_seq_length(x);
  if (len<0) return fd_type_error(_("sequence"),"seqlen",x);
  else return FD_INT(len);
}

static fdtype seqelt_prim(fdtype x,fdtype offset)
{
  char buf[16]; int off; fdtype result;
  if (!(FD_SEQUENCEP(x)))
    return fd_type_error(_("sequence"),"seqelt_prim",x);
  else if (FD_INTP(offset)) off=FD_FIX2INT(offset);
  else return fd_type_error(_("fixnum"),"seqelt_prim",offset);
  if (off<0) off=fd_seq_length(x)+off;
  result=fd_seq_elt(x,off);
  if (result == FD_TYPE_ERROR)
    return fd_type_error(_("sequence"),"seqelt_prim",x);
  else if (result == FD_RANGE_ERROR) {
    sprintf(buf,"%lld",fd_getint(offset));
    return fd_err(fd_RangeError,"seqelt_prim",u8_strdup(buf),x);}
  else return result;
}

/* This is for use in filters, especially PICK and REJECT */

enum COMPARISON {
  cmp_lt, cmp_lte, cmp_eq, cmp_gte, cmp_gt };

static int has_length_helper(fdtype x,fdtype length_arg,enum COMPARISON cmp)
{
  int seqlen=fd_seq_length(x), testlen;
  if (seqlen<0) return fd_type_error(_("sequence"),"seqlen",x);
  else if (FD_INTP(length_arg))
    testlen=(FD_FIX2INT(length_arg));
  else return fd_type_error(_("fixnum"),"has-length?",x);
  switch (cmp) {
  case cmp_lt: return (seqlen<testlen);
  case cmp_lte: return (seqlen<=testlen);
  case cmp_eq: return (seqlen==testlen);
  case cmp_gte: return (seqlen>=testlen);
  case cmp_gt: return (seqlen>testlen);
  default:
    fd_seterr("Unknown length comparison","has_length_helper",NULL,x);
    return -1;}
}

static fdtype has_length_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_eq);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_lt_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_lt);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_lte_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_lte);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_gt_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_gt);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_gte_prim(fdtype x,fdtype length_arg)
{
  int retval=has_length_helper(x,length_arg,cmp_gte);
  if (retval<0) return FD_ERROR_VALUE;
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype has_length_gt_zero_prim(fdtype x)
{
  int seqlen=fd_seq_length(x);
  if (seqlen>0) return FD_TRUE; else return FD_FALSE;
}

static fdtype has_length_gt_one_prim(fdtype x)
{
  int seqlen=fd_seq_length(x);
  if (seqlen>1) return FD_TRUE; else return FD_FALSE;
}

/* Element access */

/* We separate this out to give the compiler the option of not inline
   coding this guy. */
static fdtype check_empty_list_range
  (u8_string prim,fdtype seq,
   fdtype start_arg,fdtype end_arg,
   int *startp,int *endp)
{
  if ((FD_FALSEP(start_arg)) ||
      (FD_VOIDP(start_arg)) ||
      (start_arg==FD_INT(0))) {}
  else if (FD_FIXNUMP(start_arg))
    return fd_err(fd_RangeError,prim,"start",start_arg);
  else return fd_type_error("fixnum",prim,start_arg);
  if ((FD_FALSEP(end_arg)) ||
      (FD_VOIDP(end_arg)) ||
      (end_arg==FD_INT(0))) {}
  else if (FD_FIXNUMP(end_arg))
    return fd_err(fd_RangeError,prim,"start",end_arg);
  else return fd_type_error("fixnum",prim,end_arg);
  *startp=0; *endp=0;
  return FD_VOID;
}

static fdtype check_range(u8_string prim,fdtype seq,
                           fdtype start_arg,fdtype end_arg,
                           int *startp,int *endp)
{
  if (FD_EMPTY_LISTP(seq))
    return check_empty_list_range(prim,seq,start_arg,end_arg,startp,endp);
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error(_("sequence"),prim,seq);
  else if (FD_INTP(start_arg)) {
    int arg=FD_FIX2INT(start_arg);
    if (arg<0)
      return fd_err(fd_RangeError,prim,"start",start_arg);
    else *startp=arg;}
  else if ((FD_VOIDP(start_arg)) || (FD_FALSEP(start_arg)))
    *startp=0;
  else return fd_type_error("fixnum",prim,start_arg);
  if ((FD_VOIDP(end_arg)) || (FD_FALSEP(end_arg)))
    *endp=fd_seq_length(seq);
  else if (FD_INTP(end_arg)) {
    int arg=FD_FIX2INT(end_arg);
    if (arg<0) {
      int len=fd_seq_length(seq), off=len+arg;
      if (off>=0) *endp=off;
      else return fd_err(fd_RangeError,prim,"start",end_arg);}
    else if (arg>fd_seq_length(seq))
      return fd_err(fd_RangeError,prim,"start",end_arg);
    else *endp=arg;}
  else return fd_type_error("fixnum",prim,end_arg);
  return FD_VOID;
}

static fdtype slice_prim(fdtype x,fdtype start_arg,fdtype end_arg)
{
  int start, end; char buf[32];
  fdtype result=check_range("slice_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(result)) return result;
  else result=fd_slice(x,start,end);
  if (result == FD_TYPE_ERROR)
    return fd_type_error(_("sequence"),"slice_prim",x);
  else if (result == FD_RANGE_ERROR) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),start,end);
    return fd_err(fd_RangeError,"slice_prim",u8_strdup(buf),x);}
  else return result;
}

static fdtype position_prim(fdtype key,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int result, start, end; char buf[32];
  fdtype check=check_range("position_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else result=fd_position(key,x,start,end);
  if (result>=0) return FD_INT(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2)
    return fd_type_error(_("sequence"),"position_prim",x);
  else if (result == -3) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),start,end);
    return fd_err(fd_RangeError,"position_prim",u8_strdup(buf),x);}
  else return FD_INT(result);
}

static fdtype rposition_prim(fdtype key,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int result, start, end; char buf[32];
  fdtype check=check_range("rposition_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else result=fd_rposition(key,x,start,end);
  if (result>=0) return FD_INT(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2)
    return fd_type_error(_("sequence"),"rposition_prim",x);
  else if (result == -3) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),start,end);
    return fd_err(fd_RangeError,"rposition_prim",u8_strdup(buf),x);}
  else return FD_INT(result);
}

static fdtype find_prim(fdtype key,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int result, start, end; char buf[32];
  fdtype check=check_range("find_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else result=fd_position(key,x,start,end);
  if (result>=0) return FD_TRUE;
  else if (result == -1) return FD_FALSE;
  else if (result == -2)
    return fd_type_error(_("sequence"),"find_prim",x);
  else if (result == -3) {
    sprintf(buf,"%d[%d:%d]",fd_seq_length(x),start,end);
    return fd_err(fd_RangeError,"find_prim",u8_strdup(buf),x);}
  else return FD_FALSE;
}

static fdtype search_prim(fdtype key,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int result, start, end; char buf[32];
  fdtype check=check_range("search_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else result=fd_search(key,x,start,end);
  if (result>=0) return FD_INT(result);
  else if (result == -1) return FD_FALSE;
  else if (result == -2) {
    sprintf(buf,"%d:%d",start,end);
    return fd_err(fd_RangeError,"search_prim",u8_strdup(buf),x);}
  else if (result == -3)
    return fd_type_error(_("sequence"),"search_prim",x);
  else return result;
}

static fdtype every_prim(fdtype proc,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int start, end;
  fdtype check=check_range("every_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("function"),"every_prim",x);
  else if (FD_STRINGP(x)) {
    const u8_byte *scan=FD_STRDATA(x);
    int i=0; while (i<end) {
      int c=u8_sgetc(&scan);
      if (i<start) i++;
      else if (i<end) {
        fdtype lc=FD_CODE2CHAR(c);
        fdtype testval=fd_apply(proc,1,&lc);
        if (FD_FALSEP(testval)) return FD_FALSE;
        else if (FD_ABORTED(testval)) return testval;
        else {
          fd_decref(testval); i++;}}
      else return FD_TRUE;}
    return FD_TRUE;}
  else {
    int i=start; while (i<end) {
      fdtype elt=fd_seq_elt(x,i);
      fdtype testval=fd_apply(proc,1,&elt);
      if (FD_ABORTED(testval)) {
        fd_decref(elt); return testval;}
      else if (FD_FALSEP(testval)) {
        fd_decref(elt); return FD_FALSE;}
      else {
        fd_decref(elt); fd_decref(testval); i++;}}
    return FD_TRUE;}
}

static fdtype some_prim(fdtype proc,fdtype x,fdtype start_arg,fdtype end_arg)
{
  int start, end;
  fdtype check=check_range("some_prim",x,start_arg,end_arg,&start,&end);
  if (FD_ABORTED(check)) return check;
  else if (!(FD_APPLICABLEP(proc)))
    return fd_type_error(_("function"),"some_prim",x);
  else if (FD_STRINGP(x)) {
    const u8_byte *scan=FD_STRDATA(x);
    int i=0; while (i<end) {
      int c=u8_sgetc(&scan);
      if (i<start) i++;
      else if (i< end) {
        fdtype lc=FD_CODE2CHAR(c);
        fdtype testval=fd_apply(proc,1,&lc);
        if (FD_ABORTED(testval)) return testval;
        else if (FD_FALSEP(testval)) i++;
        else {
          fd_decref(testval); return FD_TRUE;}}
      else return FD_FALSE;}
    return FD_FALSE;}
  else {
    int i=start; while (i<end) {
      fdtype elt=fd_seq_elt(x,i);
      fdtype testval=fd_apply(proc,1,&elt);
      if (FD_ABORTED(testval)) {
        fd_decref(elt); return testval;}
      else if ((FD_FALSEP(testval)) || (FD_EMPTY_CHOICEP(testval))) {
        fd_decref(elt); i++;}
      else {
        fd_decref(elt); fd_decref(testval);
        return FD_TRUE;}}
    return FD_FALSE;}
}

FD_CPRIM(removeif_prim,"REMOVE-IF",fdtype test,fdtype sequence)
/* Removes elements of sequence which pass the predicate 'test'.
   The element E is removed when:
   * if FN is a function, applying FN to E returns true
   * if FN is a table, FN contains E as a KEY
   * or, if E is a table, E contains the key FN. */
{
  if (FD_EMPTY_CHOICEP(sequence)) return sequence;
  else if (FD_CHOICEP(sequence)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(seq,sequence) {
      fdtype r=fd_removeif(test,seq,0);
      if (FD_ABORTED(r)) {
        fd_decref(results); return r;}
      FD_ADD_TO_CHOICE(results,r);}
    return results;}
  else return fd_removeif(test,sequence,0);
}

static fdtype removeifnot_prim(fdtype test,fdtype sequence)
{
  if (FD_EMPTY_CHOICEP(sequence)) return sequence;
  else if (FD_CHOICEP(sequence)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(seq,sequence) {
      fdtype r=fd_removeif(test,seq,1);
      if (FD_ABORTED(r)) {
        fd_decref(results); return r;}
      FD_ADD_TO_CHOICE(results,r);}
    return results;}
  else return fd_removeif(test,sequence,1);
}

/* Small element functions */

static fdtype seq_elt(fdtype x,char *cxt,int i)
{
  fdtype v=fd_seq_elt(x,i);
  if (FD_TROUBLEP(v))
    return fd_err(fd_retcode_to_exception(v),cxt,NULL,x);
  else return v;
}

static fdtype first(fdtype x)
{
  return seq_elt(x,"first",0);
}
static fdtype rest(fdtype x)
{
  if (FD_PAIRP(x)) return fd_incref(FD_CDR(x));
  else {
    fdtype v=fd_slice(x,1,-1);
    if (FD_TROUBLEP(v))
      return fd_err(fd_retcode_to_exception(v),"rest",NULL,x);
    else return v;}
}

static fdtype second(fdtype x)
{
  return seq_elt(x,"second",1);
}
static fdtype third(fdtype x)
{
  return seq_elt(x,"third",2);
}
static fdtype fourth(fdtype x)
{
  return seq_elt(x,"fourth",3);
}
static fdtype fifth(fdtype x)
{
  return seq_elt(x,"fifth",4);
}
static fdtype sixth(fdtype x)
{
  return seq_elt(x,"sixth",5);
}
static fdtype seventh(fdtype x)
{
  return seq_elt(x,"seventh",6);
}

/* Pair functions */

static fdtype nullp(fdtype x)
{
  if (FD_EMPTY_LISTP(x)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype cons(fdtype x,fdtype y)
{
  return fd_make_pair(x,y);
}
static fdtype car(fdtype x)
{
  return fd_incref(FD_CAR(x));
}
static fdtype cdr(fdtype x)
{
  return fd_incref(FD_CDR(x));
}
static fdtype cddr(fdtype x)
{
  if (FD_PAIRP(FD_CDR(x)))
    return fd_incref(FD_CDR(FD_CDR(x)));
  else return fd_err(fd_RangeError,"CDDR",NULL,x);
}
static fdtype cadr(fdtype x)
{
  if (FD_PAIRP(FD_CDR(x)))
    return fd_incref(FD_CAR(FD_CDR(x)));
  else return fd_err(fd_RangeError,"CADR",NULL,x);
}
static fdtype cdar(fdtype x)
{
  if (FD_PAIRP(FD_CAR(x)))
    return fd_incref(FD_CDR(FD_CAR(x)));
  else return fd_err(fd_RangeError,"CDAR",NULL,x);
}
static fdtype caar(fdtype x)
{
  if (FD_PAIRP(FD_CAR(x)))
    return fd_incref(FD_CAR(FD_CAR(x)));
  else return fd_err(fd_RangeError,"CAAR",NULL,x);
}
static fdtype caddr(fdtype x)
{
  if ((FD_PAIRP(FD_CDR(x)))&&(FD_PAIRP(FD_CDR(FD_CDR(x)))))
    return fd_incref(FD_CAR(FD_CDR(FD_CDR(x))));
  else return fd_err(fd_RangeError,"CADR",NULL,x);
}
static fdtype cdddr(fdtype x)
{
  if ((FD_PAIRP(FD_CDR(x)))&&(FD_PAIRP(FD_CDR(FD_CDR(x)))))
    return fd_incref(FD_CDR(FD_CDR(FD_CDR(x))));
  else return fd_err(fd_RangeError,"CADR",NULL,x);
}

static fdtype cons_star(int n,fdtype *args)
{
  int i=n-2; fdtype list=fd_incref(args[n-1]);
  while (i>=0) {
    list=fd_conspair(fd_incref(args[i]),list);
    i--;}
  return list;
}

/* Association list functions */

static fdtype assq_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (!(FD_PAIRP(list)))
    return fd_type_error("alist","assq_prim",list);
  else {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (!(FD_PAIRP(FD_CAR(scan))))
        return fd_type_error("alist","assq_prim",list);
      else {
        fdtype item=FD_CAR(FD_CAR(scan));
        if (FD_EQ(key,item))
          return fd_incref(FD_CAR(scan));
        else scan=FD_CDR(scan);}
    return FD_FALSE;}
}

static fdtype assv_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (!(FD_PAIRP(list)))
    return fd_type_error("alist","assv_prim",list);
  else {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (!(FD_PAIRP(FD_CAR(scan))))
        return fd_type_error("alist","assv_prim",list);
      else {
        fdtype item=FD_CAR(FD_CAR(scan));
        if (FD_EQV(key,item))
          return fd_incref(FD_CAR(scan));
        else scan=FD_CDR(scan);}
    return FD_FALSE;}
}

static fdtype assoc_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (!(FD_PAIRP(list)))
    return fd_type_error("alist","assoc_prim",list);
  else {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (!(FD_PAIRP(FD_CAR(scan))))
        return fd_type_error("alist","assoc_prim",list);
      else {
        fdtype item=FD_CAR(FD_CAR(scan));
        if (FDTYPE_EQUAL(key,item))
          return fd_incref(FD_CAR(scan));
        else scan=FD_CDR(scan);}
    return FD_FALSE;}
}

/* MEMBER functions */

static fdtype memq_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (FD_PAIRP(list)) {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (FD_EQ(FD_CAR(scan),key))
        return fd_incref(scan);
      else scan=FD_CDR(scan);
    return FD_FALSE;}
  else return fd_type_error("list","memq_prim",list);
}

static fdtype memv_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (FD_PAIRP(list)) {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (FD_EQV(FD_CAR(scan),key))
        return fd_incref(scan);
      else scan=FD_CDR(scan);
    return FD_FALSE;}
  else return fd_type_error("list","memv_prim",list);
}

static fdtype member_prim(fdtype key,fdtype list)
{
  if (FD_EMPTY_LISTP(list)) return FD_FALSE;
  else if (FD_PAIRP(list)) {
    fdtype scan=list;
    while (FD_PAIRP(scan))
      if (FD_EQUAL(FD_CAR(scan),key))
        return fd_incref(scan);
      else scan=FD_CDR(scan);
    return FD_FALSE;}
  else return fd_type_error("list","member_prim",list);
}

/* LIST AND VECTOR */

static fdtype list(int n,fdtype *elts)
{
  fdtype head=FD_EMPTY_LIST, *tail=&head; int i=0;
  while (i < n) {
    fdtype pair=fd_make_pair(elts[i],FD_EMPTY_LIST);
    *tail=pair; tail=&((struct FD_PAIR *)pair)->cdr; i++;}
  return head;
}

static fdtype vector(int n,fdtype *elts)
{
  int i=0; while (i < n) {fd_incref(elts[i]); i++;}
  return fd_make_vector(n,elts);
}

static fdtype make_vector(fdtype size,fdtype dflt)
{
  int n=fd_getint(size);
  if (n==0)
    return fd_init_vector(NULL,0,NULL);
  else if (n>0) {
    fdtype result=fd_init_vector(NULL,n,NULL);
    fdtype *elts=FD_VECTOR_ELTS(result);
    int i=0; while (i < n) {
      elts[i]=fd_incref(dflt); i++;}
    return result;}
  else return fd_type_error(_("positive"),"make_vector",size);
}

static fdtype seq2vector(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq))
    return fd_init_vector(NULL,0,NULL);
  else if (FD_SEQUENCEP(seq)) {
    int n; fdtype *data=fd_elts(seq,&n);
    fdtype result=fd_make_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2vector",seq);
}

static fdtype onevector_prim(int n,fdtype *args)
{
  fdtype elts[32], result=FD_VOID;
  struct U8_PILE pile; int i=0;
  if (n==0) return fd_init_vector(NULL,0,NULL);
  else if (n==1) {
    if (FD_VECTORP(args[0])) return fd_incref(args[0]);
    else if (FD_PAIRP(args[0])) {}
    else if ((FD_EMPTY_CHOICEP(args[0]))||(FD_EMPTY_QCHOICEP(args[0])))
      return fd_init_vector(NULL,0,NULL);
    else if (!(FD_CONSP(args[0])))
      return fd_make_vector(1,args);
    else {
      fd_incref(args[0]); return fd_make_vector(1,args);}}
  U8_INIT_STATIC_PILE((&pile),elts,32);
  while (i<n) {
    fdtype arg=args[i++];
    FD_DO_CHOICES(each,arg) {
      if (!(FD_CONSP(each))) {u8_pile_add((&pile),each);}
      else if (FD_VECTORP(each)) {
        int len=FD_VECTOR_LENGTH(each); int j=0;
        while (j<len) {
          fdtype elt=FD_VECTOR_REF(each,j); fd_incref(elt);
          u8_pile_add(&pile,elt); j++;}}
      else if (FD_PAIRP(each)) {
        FD_DOLIST(elt,each) {
          fd_incref(elt); u8_pile_add(&pile,elt);}}
      else {
        fd_incref(each); u8_pile_add(&pile,each);}}}
  result=fd_make_vector(pile.u8_len,(fdtype *)pile.u8_elts);
  if (pile.u8_mallocd) u8_free(pile.u8_elts);
  return result;
}

static fdtype seq2rail(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq))
    return fd_make_rail(0,NULL);
  else if (FD_SEQUENCEP(seq)) {
    int n; fdtype *data=fd_elts(seq,&n);
    fdtype result=fd_make_rail(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2rail",seq);
}

static fdtype seq2list(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq)) return FD_EMPTY_LIST;
  else if (FD_SEQUENCEP(seq)) {
    int n; fdtype *data=fd_elts(seq,&n), result=FD_EMPTY_LIST;
    n--; while (n>=0) {
      result=fd_conspair(data[n],result); n--;}
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2list",seq);
}

static fdtype seq2packet(fdtype seq)
{
  if (FD_EMPTY_LISTP(seq))
    return fd_init_packet(NULL,0,NULL);
  else if (FD_SEQUENCEP(seq)) {
    int i=0, n;
    fdtype result=FD_VOID;
    fdtype *data=fd_elts(seq,&n);
    unsigned char *bytes=u8_malloc(n);
    while (i<n) {
      if (FD_BYTEP(data[i])) {
        bytes[i]=FD_FIX2INT(data[i]); i++;}
      else {
        fdtype bad=fd_incref(data[i]);
        i=0; while (i < n) {fd_decref(data[i]); i++;}
        u8_free(data);
        return fd_type_error(_("byte"),"seq2packet",bad);}}
    u8_free(data);
    result=fd_make_packet(NULL,n,bytes);
    u8_free(bytes);
    return result;}
  else return fd_type_error(_("sequence"),"seq2packet",seq);
}

static fdtype x2string(fdtype seq)
{
  if (FD_SYMBOLP(seq))
    return fdtype_string(FD_SYMBOL_NAME(seq));
  else if (FD_CHARACTERP(seq)) {
    int c=FD_CHAR2CODE(seq);
    U8_OUTPUT out; u8_byte buf[16];
    U8_INIT_STATIC_OUTPUT_BUF(out,16,buf);
    u8_putc(&out,c);
    return fdtype_string(out.u8_outbuf);}
  else if (FD_EMPTY_LISTP(seq)) return fdtype_string("");
  else if (FD_STRINGP(seq)) return fd_incref(seq);
  else if (FD_SEQUENCEP(seq)) {
    U8_OUTPUT out;
    int i=0, n;
    fdtype *data=fd_elts(seq,&n);
    U8_INIT_OUTPUT(&out,n*2);
    while (i<n) {
      if (FD_FIXNUMP(data[i])) {
        long long charcode=FD_FIX2INT(data[i]);
        if ((charcode<0)||(charcode>=0x10000)) {
          fdtype bad=fd_incref(data[i]);
          i=0; while (i<n) {fd_decref(data[i]); i++;}
          u8_free(data);
          return fd_type_error(_("character"),"seq2string",bad);}
        u8_putc(&out,charcode); i++;}
      else if (FD_CHARACTERP(data[i])) {
        int charcode=FD_CHAR2CODE(data[i]);
        u8_putc(&out,charcode); i++;}
      else {
        fdtype bad=fd_incref(data[i]);
        i=0; while (i<n) {fd_decref(data[i]); i++;}
        u8_free(data);
        return fd_type_error(_("character"),"seq2string",bad);}}
    u8_free(data);
    return fd_stream2string(&out);}
  else return fd_type_error(_("sequence"),"x2string",seq);
}

FD_EXPORT fdtype fd_seq_elts(fdtype x)
{
  if (FD_CHOICEP(x)) {
    fdtype results=FD_EMPTY_CHOICE;
    FD_DO_CHOICES(e,x) {
      fdtype subelts=fd_seq_elts(e);
      FD_ADD_TO_CHOICE(results,subelts);}
    return results;}
  else {
    fdtype result=FD_EMPTY_CHOICE;
    int ctype=FD_PTR_TYPE(x), i=0, len;
    switch (ctype) {
    case fd_vector_type: case fd_rail_type:
      len=FD_VECTOR_LENGTH(x); while (i < len) {
        fdtype elt=fd_incref(FD_VECTOR_REF(x,i));
        FD_ADD_TO_CHOICE(result,elt); i++;}
      return result;
    case fd_packet_type: case fd_secret_type:
      len=FD_PACKET_LENGTH(x); while (i < len) {
        fdtype elt=FD_BYTE2LISP(FD_PACKET_REF(x,i));
        FD_ADD_TO_CHOICE(result,elt); i++;}
      return result;
    case fd_pair_type: {
      fdtype scan=x;
      while (FD_PAIRP(scan)) {
        fdtype elt=fd_incref(FD_CAR(scan));
        FD_ADD_TO_CHOICE(result,elt);
        scan=FD_CDR(scan);}
      if (!(FD_EMPTY_LISTP(scan))) {
        fdtype tail=fd_incref(scan);
        FD_ADD_TO_CHOICE(result,tail);}
      return result;}
    case fd_string_type: {
      const u8_byte *scan=FD_STRDATA(x);
      int c=u8_sgetc(&scan);
      while (c>=0) {
        FD_ADD_TO_CHOICE(result,FD_CODE2CHAR(c));
        c=u8_sgetc(&scan);}
      return result;}
    default:
      len=fd_seq_length(x); while (i<len) {
        fdtype elt=fd_seq_elt(x,i);
        FD_ADD_TO_CHOICE(result,elt); i++;}
      return result;
    }}
}

static fdtype elts_prim(fdtype x,fdtype start_arg,fdtype end_arg)
{
  int start, end;
  fdtype check=check_range("elts_prim",x,start_arg,end_arg,&start,&end);
  fdtype results=FD_EMPTY_CHOICE;
  int ctype=FD_PTR_TYPE(x);
  if (FD_ABORTED(check)) return check;
  else switch (ctype) {
    case fd_vector_type: case fd_rail_type: {
      fdtype *read, *limit;
      read=FD_VECTOR_DATA(x)+start; limit=FD_VECTOR_DATA(x)+end;
      while (read<limit) {
        fdtype v=*read++; fd_incref(v);
        FD_ADD_TO_CHOICE(results,v);}
      return results;}
    case fd_packet_type: case fd_secret_type: {
      const unsigned char *read=FD_PACKET_DATA(x), *lim=read+end;
      while (read<lim) {
        unsigned char v=*read++;
        FD_ADD_TO_CHOICE(results,FD_SHORT2FIX(v));}
      return results;}
    case fd_numeric_vector_type: {
      int len=FD_NUMVEC_LENGTH(x);
      int i=start, lim=((end<0)?(len+end):(end));
      if (lim>len) lim=len;
      switch (FD_NUMVEC_TYPE(x)) {
      case fd_short_elt: {
        fd_short *vec=FD_NUMVEC_SHORTS(x);
        while (i<lim) {
          fd_short num=vec[i++]; 
          fdtype elt=FD_SHORT2FIX(num); 
          FD_ADD_TO_CHOICE(results,elt); 
          i++;}}
      case fd_int_elt: {
        fd_int *vec=FD_NUMVEC_INTS(x);
        while (i<lim) {
          fd_int num=vec[i++]; fdtype elt=FD_INT(num);
          FD_ADD_TO_CHOICE(results,elt);}
        break;}
      case fd_long_elt: {
        fd_long *vec=FD_NUMVEC_LONGS(x);
        while (i<lim) {
          fd_long num=vec[i++]; fdtype elt=FD_INT(num);
          FD_ADD_TO_CHOICE(results,elt);}
        break;}
      case fd_float_elt: {
        fd_float *vec=FD_NUMVEC_FLOATS(x);
        while (i<lim) {
          fd_float num=vec[i++]; fdtype elt=fd_make_flonum(num);
          FD_ADD_TO_CHOICE(results,elt);}
        break;}
      case fd_double_elt: {
        fd_double *vec=FD_NUMVEC_DOUBLES(x);
        while (i<lim) {
          fd_double num=vec[i++]; fdtype elt=fd_make_flonum(num);
          FD_ADD_TO_CHOICE(results,elt);}
        break;}
      default: {
        fd_seterr(_("Corrputed numerical vector"),"fd_seq_elts",NULL,x); 
        fd_incref(x); fd_decref(results);
        results=FD_ERROR_VALUE;}}
      break;}
    case fd_pair_type: {
      int j=0; fdtype scan=x;
      while (FD_PAIRP(scan))
        if (j==end) return results;
        else if (j>=start) {
          fdtype car=FD_CAR(scan); fd_incref(car);
          FD_ADD_TO_CHOICE(results,car);
          j++; scan=FD_CDR(scan);}
        else {j++; scan=FD_CDR(scan);}
      return results;}
    case fd_string_type: {
      int count=0, c;
      const u8_byte *scan=FD_STRDATA(x);
      while ((c=u8_sgetc(&scan))>=0)
        if (count<start) count++;
        else if (count>=end) break;
        else {FD_ADD_TO_CHOICE(results,FD_CODE2CHAR(c));}
      return results;}
    default:
      if (FD_EMPTY_LISTP(x))
        if ((start == end) && (start == 0))
          return FD_EMPTY_CHOICE;
        else return FD_RANGE_ERROR;
      else if ((fd_seqfns[ctype]) && (fd_seqfns[ctype]->elt)) {
        int scan=start; while (start<end) {
          fdtype elt=(fd_seqfns[ctype]->elt)(x,scan);
          FD_ADD_TO_CHOICE(results,elt); scan++;}
        return results;}
      else if (fd_seqfns[ctype]==NULL)
        return fd_type_error(_("sequence"),"elts_prim",x);
      else return fd_err(fd_NoMethod,"elts_prim",NULL,x);}
  return results;
}

static fdtype vec2elts_prim(fdtype x)
{
  if (FD_VECTORP(x)) {
    fdtype result=FD_EMPTY_CHOICE;
    int i=0; int len=FD_VECTOR_LENGTH(x); fdtype *elts=FD_VECTOR_ELTS(x);
    while (i<len) {
      fdtype e=elts[i++]; fd_incref(e); FD_ADD_TO_CHOICE(result,e);}
    return result;}
  else return fd_incref(x);
}

/* Vector length predicates */

static fdtype veclen_lt_prim(fdtype x,fdtype len)
{
  if (!(FD_VECTORP(x)))
    return FD_FALSE;
  else if (FD_VECTOR_LENGTH(x)<FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype veclen_lte_prim(fdtype x,fdtype len)
{
  if (!(FD_VECTORP(x)))
    return FD_FALSE;
  else if (FD_VECTOR_LENGTH(x)<=FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype veclen_gt_prim(fdtype x,fdtype len)
{
  if (!(FD_VECTORP(x)))
    return FD_FALSE;
  else if (FD_VECTOR_LENGTH(x)>FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype veclen_gte_prim(fdtype x,fdtype len)
{
  if (!(FD_VECTORP(x)))
    return FD_FALSE;
  else if (FD_VECTOR_LENGTH(x)>=FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype veclen_eq_prim(fdtype x,fdtype len)
{
  if (!(FD_VECTORP(x)))
    return FD_FALSE;
  else if (FD_VECTOR_LENGTH(x)==FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

/* Sequence length predicates */

static fdtype seqlen_lt_prim(fdtype x,fdtype len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x)<FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype seqlen_lte_prim(fdtype x,fdtype len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x)<=FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype seqlen_gt_prim(fdtype x,fdtype len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x)>FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype seqlen_gte_prim(fdtype x,fdtype len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x)>=FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype seqlen_eq_prim(fdtype x,fdtype len)
{
  if (!(FD_SEQUENCEP(x)))
    return FD_FALSE;
  else if (fd_seq_length(x)==FD_FIX2INT(len))
    return FD_TRUE;
  else return FD_FALSE;
}

/* Matching vectors */

static fdtype seqmatch_prim(fdtype prefix,fdtype seq,fdtype startarg)
{
  if (!(FD_UINTP(startarg)))
    return fd_type_error("uint","seqmatchprim",startarg);
  int start=FD_FIX2INT(startarg);
  if ((FD_VECTORP(prefix))&&(FD_VECTORP(seq))) {
    int plen=FD_VECTOR_LENGTH(prefix);
    int seqlen=FD_VECTOR_LENGTH(seq);
    if ((start+plen)<=seqlen) {
      int j=0; int i=start;
      while (j<plen)
        if (FD_EQUAL(FD_VECTOR_REF(prefix,j),FD_VECTOR_REF(seq,i))) {
          j++; i++;}
        else return FD_FALSE;
      return FD_TRUE;}
    else return FD_FALSE;}
  else if ((FD_STRINGP(prefix))&&(FD_STRINGP(seq))) {
    int plen=FD_STRLEN(prefix);
    int seqlen=FD_STRLEN(seq);
    int off=u8_byteoffset(FD_STRDATA(seq),start,seqlen);
    if ((off+plen)<=seqlen)
      if (strncmp(FD_STRDATA(prefix),FD_STRDATA(seq)+off,plen)==0)
        return FD_TRUE;
      else return FD_FALSE;
    else return FD_FALSE;}
  else if (!(FD_SEQUENCEP(prefix)))
    return fd_type_error("sequence","seqmatch_prim",prefix);
  else if (!(FD_SEQUENCEP(seq)))
    return fd_type_error("sequence","seqmatch_prim",seq);
  else {
    int plen=fd_seq_length(prefix);
    int seqlen=fd_seq_length(seq);
    if ((start+plen)<=seqlen) {
      int j=0; int i=start;
      while (j<plen) {
        fdtype pelt=fd_seq_elt(prefix,j);
        fdtype velt=fd_seq_elt(prefix,i);
        int cmp=FD_EQUAL(pelt,velt);
        fd_decref(pelt); fd_decref(velt);
        if (cmp) {j++; i++;}
        else return FD_FALSE;}
      return FD_TRUE;}
    else return FD_FALSE;}
}

/* Sorting vectors */

static fdtype sortvec_primfn(fdtype vec,fdtype keyfn,int reverse,int lexsort)
{
  if (FD_VECTOR_LENGTH(vec)==0)
    return fd_init_vector(NULL,0,NULL);
  else if (FD_VECTOR_LENGTH(vec)==1)
    return fd_incref(vec);
  else {
    int i=0, n=FD_VECTOR_LENGTH(vec), j=0;
    fdtype result=fd_init_vector(NULL,n,NULL);
    fdtype *vecdata=FD_VECTOR_ELTS(result);
    struct FD_SORT_ENTRY *sentries=u8_alloc_n(n,struct FD_SORT_ENTRY);
    while (i<n) {
      fdtype elt=FD_VECTOR_REF(vec,i);
      fdtype value=_fd_apply_keyfn(elt,keyfn);
      if (FD_ABORTED(value)) {
        int j=0; while (j<i) {fd_decref(sentries[j].fd_sortval); j++;}
        u8_free(sentries); u8_free(vecdata);
        return value;}
      sentries[i].fd_sortval=elt;
      sentries[i].fd_sortkey=value;
      i++;}
    if (lexsort)
      qsort(sentries,n,sizeof(struct FD_SORT_ENTRY),_fd_lexsort_helper);
    else qsort(sentries,n,sizeof(struct FD_SORT_ENTRY),_fd_sort_helper);
    i=0; j=n-1; if (reverse) while (i < n) {
      fd_decref(sentries[i].fd_sortkey);
      vecdata[j]=fd_incref(sentries[i].fd_sortval);
      i++; j--;}
    else while (i < n) {
      fd_decref(sentries[i].fd_sortkey);
      vecdata[i]=fd_incref(sentries[i].fd_sortval);
      i++;}
    u8_free(sentries);
    return result;}
}

static fdtype sortvec_prim(fdtype vec,fdtype keyfn)
{
  return sortvec_primfn(vec,keyfn,0,0);
}

static fdtype lexsortvec_prim(fdtype vec,fdtype keyfn)
{
  return sortvec_primfn(vec,keyfn,0,1);
}

static fdtype rsortvec_prim(fdtype vec,fdtype keyfn)
{
  return sortvec_primfn(vec,keyfn,1,0);
}

/* RECONS reconstitutes CONSes, returning the original if
   nothing has changed. This is handy for some recursive list
   functions. */

static fdtype recons_prim(fdtype car,fdtype cdr,fdtype orig)
{
  if ((FD_EQ(car,FD_CAR(orig)))&&(FD_EQ(cdr,FD_CDR(orig))))
    return fd_incref(orig);
  else {
    fdtype cons=fd_conspair(car,cdr);
    fd_incref(car); fd_incref(cdr);
    return cons;}
}

/* Numeric vectors */

static fdtype make_short_vector(int n,fdtype *from_elts)
{
  int i=0; fdtype vec=fd_make_numeric_vector(n,fd_short_elt);
  fd_short *elts=FD_NUMVEC_SHORTS(vec);
  while (i<n) {
    fdtype elt=from_elts[i];
    if (FD_SHORTP(elt))
      elts[i++]=(fd_short)(FD_FIX2INT(elt));
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("short element"),"make_short_vector",elt);}}
  return vec;
}

static fdtype seq2shortvec(fdtype arg)
{
  if ((FD_TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg)==fd_short_elt)) {
    fd_incref(arg);
    return arg;}
  else if (FD_VECTORP(arg))
    return make_short_vector(FD_VECTOR_LENGTH(arg),FD_VECTOR_ELTS(arg));
  else if (FD_EMPTY_LISTP(arg))
    return fd_make_short_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n; fdtype *data=fd_elts(arg,&n);
    fdtype result=make_short_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2shortvec",arg);
}

static fdtype make_int_vector(int n,fdtype *from_elts)
{
  int i=0; fdtype vec=fd_make_numeric_vector(n,fd_int_elt);
  fd_int *elts=FD_NUMVEC_INTS(vec);
  while (i<n) {
    fdtype elt=from_elts[i];
    if (FD_INTP(elt))
      elts[i++]=((fd_int)(FD_FIX2INT(elt)));
    else if ((FD_BIGINTP(elt))&&
             (fd_bigint_fits_in_word_p((fd_bigint)elt,sizeof(fd_int),1)))
      elts[i++]=fd_bigint_to_long((fd_bigint)elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("int element"),"make_int_vector",elt);}}
  return vec;
}

static fdtype seq2intvec(fdtype arg)
{
  if ((FD_TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg)==fd_int_elt)) {
    fd_incref(arg);
    return arg;}
  else if (FD_VECTORP(arg))
    return make_int_vector(FD_VECTOR_LENGTH(arg),FD_VECTOR_ELTS(arg));
  else if (FD_EMPTY_LISTP(arg))
    return fd_make_int_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n; fdtype *data=fd_elts(arg,&n);
    fdtype result=make_int_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2intvec",arg);
}

static fdtype make_long_vector(int n,fdtype *from_elts)
{
  int i=0; fdtype vec=fd_make_numeric_vector(n,fd_long_elt);
  fd_long *elts=FD_NUMVEC_LONGS(vec);
  while (i<n) {
    fdtype elt=from_elts[i];
    if (FD_FIXNUMP(elt))
      elts[i++]=((fd_long)(FD_FIX2INT(elt)));
    else if (FD_BIGINTP(elt))
      elts[i++]=fd_bigint_to_long_long((fd_bigint)elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("flonum element"),"make_long_vector",elt);}}
  return vec;
}

static fdtype seq2longvec(fdtype arg)
{
  if ((FD_TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg)==fd_long_elt)) {
    fd_incref(arg);
    return arg;}
  else if (FD_VECTORP(arg))
    return make_long_vector(FD_VECTOR_LENGTH(arg),FD_VECTOR_ELTS(arg));
  else if (FD_EMPTY_LISTP(arg))
    return fd_make_long_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n; fdtype *data=fd_elts(arg,&n);
    fdtype result=make_long_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2longvec",arg);
}

static fdtype make_float_vector(int n,fdtype *from_elts)
{
  int i=0; fdtype vec=fd_make_numeric_vector(n,fd_float_elt);
  float *elts=FD_NUMVEC_FLOATS(vec);
  while (i<n) {
    fdtype elt=from_elts[i];
    if (FD_FLONUMP(elt))
      elts[i++]=FD_FLONUM(elt);
    else if (FD_NUMBERP(elt))
      elts[i++]=fd_todouble(elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("float element"),"make_float_vector",elt);}}
  return vec;
}

static fdtype seq2floatvec(fdtype arg)
{
  if ((FD_TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg)==fd_float_elt)) {
    fd_incref(arg);
    return arg;}
  else if (FD_VECTORP(arg))
    return make_float_vector(FD_VECTOR_LENGTH(arg),FD_VECTOR_ELTS(arg));
  else if (FD_EMPTY_LISTP(arg))
    return fd_make_float_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n; fdtype *data=fd_elts(arg,&n);
    fdtype result=make_float_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2floatvec",arg);
}

static fdtype make_double_vector(int n,fdtype *from_elts)
{
  int i=0; fdtype vec=fd_make_numeric_vector(n,fd_double_elt);
  double *elts=FD_NUMVEC_DOUBLES(vec);
  while (i<n) {
    fdtype elt=from_elts[i];
    if (FD_FLONUMP(elt))
      elts[i++]=FD_FLONUM(elt);
    else if (FD_NUMBERP(elt))
      elts[i++]=fd_todouble(elt);
    else {
      u8_free((struct FD_CONS *)vec); fd_incref(elt);
      return fd_type_error(_("double(float) element"),"make_double_vector",elt);}}
  return vec;
}

static fdtype seq2doublevec(fdtype arg)
{
  if ((FD_TYPEP(arg,fd_numeric_vector_type))&&
      (FD_NUMVEC_TYPE(arg)==fd_double_elt)) {
    fd_incref(arg);
    return arg;}
  else if (FD_VECTORP(arg))
    return make_double_vector(FD_VECTOR_LENGTH(arg),FD_VECTOR_ELTS(arg));
  else if (FD_EMPTY_LISTP(arg))
    return fd_make_double_vector(0,NULL);
  else if (FD_SEQUENCEP(arg)) {
    int n; fdtype *data=fd_elts(arg,&n);
    fdtype result=make_double_vector(n,data);
    u8_free(data);
    return result;}
  else return fd_type_error(_("sequence"),"seq2doublevec",arg);
}

/* Rails */

static fdtype make_rail(int n,fdtype *elts)
{
  int i=0; while (i<n) {
    fdtype v=elts[i++]; fd_incref(v);}
  return fd_make_rail(n,elts);
}

/* side effecting operations (not threadsafe) */

static fdtype set_car(fdtype pair,fdtype val)
{
  struct FD_PAIR *p=fd_consptr(struct FD_PAIR *,pair,fd_pair_type);
  fdtype oldv=p->car; p->car=fd_incref(val);
  fd_decref(oldv);
  return FD_VOID;
}

static fdtype set_cdr(fdtype pair,fdtype val)
{
  struct FD_PAIR *p=fd_consptr(struct FD_PAIR *,pair,fd_pair_type);
  fdtype oldv=p->cdr; p->cdr=fd_incref(val);
  fd_decref(oldv);
  return FD_VOID;
}

static fdtype vector_set(fdtype vec,fdtype index,fdtype val)
{
  struct FD_VECTOR *v=fd_consptr(struct FD_VECTOR *,vec,fd_vector_type);
  if (!(FD_UINTP(index))) return fd_type_error("uint","vector_set",index);
  int offset=FD_FIX2INT(index); fdtype *elts=v->fdvec_elts;
  if (offset>v->fdvec_length) {
    char buf[256]; sprintf(buf,"%d",offset);
    return fd_err(fd_RangeError,"vector_set",buf,vec);}
  else {
    fdtype oldv=elts[offset];
    elts[offset]=fd_incref(val);
    fd_decref(oldv);
    return FD_VOID;}
}

/* Miscellaneous sequence creation functions */

static fdtype seqpair(int n,fdtype *elts) {
  return fd_makeseq(fd_pair_type,n,elts);}
static fdtype seqstring(int n,fdtype *elts) {
  return fd_makeseq(fd_string_type,n,elts);}
static fdtype seqpacket(int n,fdtype *elts) {
  return fd_makeseq(fd_packet_type,n,elts);}
static fdtype seqsecret(int n,fdtype *elts) {
  return fd_makeseq(fd_secret_type,n,elts);}
static fdtype seqvector(int n,fdtype *elts) {
  return fd_makeseq(fd_vector_type,n,elts);}
static fdtype seqrail(int n,fdtype *elts) {
  return fd_makeseq(fd_rail_type,n,elts);}

static struct FD_SEQFNS pair_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqpair};
static struct FD_SEQFNS string_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqstring};
static struct FD_SEQFNS packet_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqpacket};
static struct FD_SEQFNS vector_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqvector};
static struct FD_SEQFNS numeric_vector_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqvector};
static struct FD_SEQFNS rail_seqfns={
  fd_seq_length,
  fd_seq_elt,
  fd_slice,
  fd_position,
  fd_search,
  fd_elts,
  seqrail};
static struct FD_SEQFNS secret_seqfns={
  fd_seq_length,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  seqsecret};


FD_EXPORT void fd_init_seqprims_c()
{
  int i=0; while (i<FD_TYPE_MAX) fd_seqfns[i++]=NULL;
  fd_seqfns[fd_pair_type]=&pair_seqfns;
  fd_seqfns[fd_string_type]=&string_seqfns;
  fd_seqfns[fd_packet_type]=&packet_seqfns;
  fd_seqfns[fd_secret_type]=&secret_seqfns;
  fd_seqfns[fd_vector_type]=&vector_seqfns;
  fd_seqfns[fd_rail_type]=&rail_seqfns;
  fd_seqfns[fd_numeric_vector_type]=&numeric_vector_seqfns;

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
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_FALSE));
  fd_defalias(fd_scheme_module,"SUBSEQ","SLICE");
  /*
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("SUBSEQ",slice_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_FALSE));
  */
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("POSITION",position_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_INT(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("RPOSITION",rposition_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_INT(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("FIND",find_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_INT(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("SEARCH",search_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           -1,FD_INT(0),
                           -1,FD_FALSE));
  fd_idefn(fd_scheme_module,fd_make_cprim1("REVERSE",fd_reverse,1));
  fd_idefn(fd_scheme_module,fd_make_cprimn("APPEND",fd_append,0));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("MATCH?",seqmatch_prim,2,
                           -1,FD_VOID,-1,FD_VOID,
                           fd_fixnum_type,FD_INT(0)));

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
           fd_make_cprim1x("CAR",car,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDR",cdr,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CADR",cadr,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDDR",cddr,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CAAR",caar,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDAR",cdar,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CADDR",caddr,1,fd_pair_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CDDDR",cdddr,1,fd_pair_type,FD_VOID));


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
  fd_idefn(fd_scheme_module,fd_make_cprimn("MAKE-RAIL",make_rail,0));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("MAKE-VECTOR",make_vector,1,
                           fd_fixnum_type,FD_VOID,
                           -1,FD_FALSE));

  fd_idefn(fd_scheme_module,fd_make_cprim1("->RAIL",seq2rail,1));
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
           ("VECLEN>=?",veclen_gte_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("VECLEN>?",veclen_gt_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("VECLEN=?",veclen_eq_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("VECLEN<?",veclen_lt_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("VECLEN<=?",veclen_lte_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN>=?",seqlen_gte_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN>?",seqlen_gt_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN=?",seqlen_eq_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN<?",seqlen_lt_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("SEQLEN<=?",seqlen_lte_prim,2,-1,FD_VOID,fd_fixnum_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SORTVEC",sortvec_prim,1,
                           fd_vector_type,FD_VOID,
                           -1,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("LEXSORTVEC",lexsortvec_prim,1,
                           fd_vector_type,FD_VOID,
                           -1,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("RSORTVEC",rsortvec_prim,1,
                           fd_vector_type,FD_VOID,
                           -1,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("RECONS",recons_prim,3,
                           -1,FD_VOID,-1,FD_VOID,
                           fd_pair_type,FD_VOID));

  /* Note that these are not threadsafe */
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SET-CAR!",set_car,2,
                           fd_pair_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SET-CDR!",set_cdr,2,
                           fd_pair_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("VECTOR-SET!",vector_set,3,
                           fd_vector_type,FD_VOID,fd_fixnum_type,FD_VOID,
                           -1,FD_VOID));
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
