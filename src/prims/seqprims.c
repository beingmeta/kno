/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif
/* KNO_MODULE = scheme */

#define KNO_INLINE_FCNIDS 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/sequences.h"
#include "kno/numbers.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/seqprims.h"
#include "kno/sorting.h"

#include <libu8/libu8.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8printf.h>

#include <limits.h>
#include <kno/cprims.h>


static u8_condition SequenceMismatch=_("Mismatch of sequence lengths");
static u8_condition EmptyReduce=_("No sequence elements to reduce");

#define KNO_EQV(x,y) \
  ((KNO_EQ(x,y)) || \
   ((NUMBERP(x)) && (NUMBERP(y)) && \
    (kno_numcompare(x,y)==0)))

#define string_start(bytes,i) ((i==0) ? (bytes) : (u8_substring(bytes,i)))

static lispval make_float_vector(int n,lispval *from_elts);
static lispval make_double_vector(int n,lispval *from_elts);
static lispval make_short_vector(int n,lispval *from_elts);
static lispval make_int_vector(int n,lispval *from_elts);
static lispval make_long_vector(int n,lispval *from_elts);

/* Exported primitives */

KNO_EXPORT int applytest(lispval test,lispval elt)
{
  if (KNO_HASHSETP(test))
    return kno_hashset_get(KNO_XHASHSET(test),elt);
  else if (HASHTABLEP(test))
    return kno_hashtable_probe(KNO_XHASHTABLE(test),elt);
  else if ((SYMBOLP(test)) || (OIDP(test)))
    return kno_test(elt,test,VOID);
  else if (TABLEP(test))
    return kno_test(test,elt,VOID);
  else if (KNO_APPLICABLEP(test)) {
    lispval result = kno_apply(test,1,&elt);
    if (FALSEP(result)) return 0;
    else {kno_decref(result); return 1;}}
  else if (EMPTYP(test)) return 0;
  else if (CHOICEP(test)) {
    DO_CHOICES(each_test,test) {
      int result = applytest(each_test,elt);
      if (result<0) return result;
      else if (result) return result;}
    return 0;}
  else {
    kno_seterr(kno_TypeError,"removeif",_("test"),test);
    return -1;}
}

KNO_EXPORT lispval kno_removeif(lispval test,lispval sequence,int invert)
{
  if (!(KNO_SEQUENCEP(sequence)))
    return kno_type_error("sequence","kno_remove",sequence);
  else if (NILP(sequence)) return sequence;
  else {
    int i = 0, j = 0, removals = 0, len = kno_seq_length(sequence);
    kno_lisp_type result_type = KNO_LISP_TYPE(sequence);
    lispval *results = u8_alloc_n(len,lispval), result;
    while (i < len) {
      lispval elt = kno_seq_elt(sequence,i);
      int compare = applytest(test,elt); i++;
      if (compare<0) {u8_free(results); return -1;}
      else if ((invert) ? (!(compare)) : (compare)) {
        removals++; kno_decref(elt);}
      else results[j++]=elt;}
    if (removals) {
      result = kno_makeseq(result_type,j,results);
      i = 0; while (i<j) {kno_decref(results[i]); i++;}
      u8_free(results);
      return result;}
    else {
      i = 0; while (i<j) {kno_decref(results[i]); i++;}
      u8_free(results);
      return kno_incref(sequence);}}
}

/* Mapping */

KNO_EXPORT lispval kno_mapseq(lispval fn,int n_seqs,lispval *sequences)
{
  int i = 1, seqlen = -1;
  lispval firstseq = sequences[0];
  lispval result, *results, _argvec[8], *argvec = NULL;
  kno_lisp_type result_type = KNO_LISP_TYPE(firstseq);
  if (KNO_FCNIDP(fn)) fn = kno_fcnid_ref(fn);
  if ((TABLEP(fn)) || (ATOMICP(fn))) {
    if (n_seqs>1)
      return kno_err(kno_TooManyArgs,"kno_mapseq",NULL,fn);
    else if (NILP(firstseq))
      return firstseq;}
  if (!(KNO_SEQUENCEP(firstseq)))
    return kno_type_error("sequence","kno_mapseq",firstseq);
  else seqlen = kno_seq_length(firstseq);
  while (i<n_seqs)
    if (!(KNO_SEQUENCEP(sequences[i])))
      return kno_type_error("sequence","kno_mapseq",sequences[i]);
    else if (seqlen!=kno_seq_length(sequences[i]))
      return kno_err(SequenceMismatch,"kno_foreach",NULL,sequences[i]);
    else i++;
  if (seqlen==0) return kno_incref(firstseq);
  enum { applyfn, tablefn, oidfn, getfn } fntype;
  results = u8_alloc_n(seqlen,lispval);
  if (KNO_APPLICABLEP(fn)) {
    if (n_seqs<8) argvec=_argvec;
    else argvec = u8_alloc_n(n_seqs,lispval);
    fntype = applyfn;}
  else if (OIDP(fn))
    fntype = oidfn;
  else if (TABLEP(fn))
    fntype = tablefn;
  else fntype = getfn;
  i = 0; while (i < seqlen) {
    lispval elt = kno_seq_elt(firstseq,i), new_elt;
    if (fntype == applyfn) {
      int j = 1; while (j<n_seqs) {
        argvec[j]=kno_seq_elt(sequences[j],i); j++;}
      argvec[0]=elt;
      new_elt = kno_apply(fn,n_seqs,argvec);
      j = 1; while (j<n_seqs) {kno_decref(argvec[j]); j++;}}
    else if (fntype == tablefn)
      new_elt = kno_get(fn,elt,elt);
    else if (fntype == oidfn)
      new_elt = kno_frame_get(elt,fn);
    else new_elt = kno_get(elt,fn,elt);
    kno_decref(elt);
    if (result_type == kno_string_type) {
      if (!(TYPEP(new_elt,kno_character_type)))
        result_type = kno_vector_type;}
    else if ((result_type == kno_packet_type)||
             (result_type == kno_secret_type)) {
      if (FIXNUMP(new_elt)) {
        long long intval = FIX2INT(new_elt);
        if ((intval<0) || (intval>=0x100))
          result_type = kno_vector_type;}
      else result_type = kno_vector_type;}
    if (KNO_ABORTED(new_elt)) {
      int j = 0; while (j<i) {kno_decref(results[j]); j++;}
      u8_free(results);
      return new_elt;}
    else results[i++]=new_elt;}
  result = kno_makeseq(result_type,seqlen,results);
  i = 0; while (i<seqlen) {kno_decref(results[i]); i++;}
  u8_free(results);
  return result;
}
KNO_DCLPRIM("map",mapseq_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
 "`(MAP *arg0* *arg1* *args...*)` **undocumented**");
static lispval mapseq_prim(int n,lispval *args)
{
  return kno_mapseq(args[0],n-1,args+1);
}

KNO_EXPORT lispval kno_foreach(lispval fn,int n_seqs,lispval *sequences)
{
  int i = 0, seqlen = -1;
  lispval firstseq = sequences[0];
  lispval _argvec[8], *argvec = NULL;
  kno_lisp_type result_type = KNO_LISP_TYPE(firstseq);
  if ((TABLEP(fn)) || ((ATOMICP(fn)) && (KNO_FCNIDP(fn)))) {
    if (n_seqs>1)
      return kno_err(kno_TooManyArgs,"kno_foreach",NULL,fn);
    else if (NILP(firstseq))
      return firstseq;}
  if (!(KNO_SEQUENCEP(firstseq)))
    return kno_type_error("sequence","kno_mapseq",firstseq);
  else seqlen = kno_seq_length(firstseq);
  while (i<n_seqs)
    if (!(KNO_SEQUENCEP(sequences[i])))
      return kno_type_error("sequence","kno_mapseq",sequences[i]);
    else if (seqlen!=kno_seq_length(sequences[i]))
      return kno_err(SequenceMismatch,"kno_foreach",NULL,sequences[i]);
    else i++;
  if (seqlen==0) return VOID;
  enum { applyfn, tablefn, oidfn, getfn } fntype;
  if (KNO_APPLICABLEP(fn)) {
    if (n_seqs<8) argvec=_argvec;
    else argvec = u8_alloc_n(n_seqs,lispval);
    fntype = applyfn;}
  else if (OIDP(fn))
    fntype = oidfn;
  else if (TABLEP(fn))
    fntype = tablefn;
  else fntype = getfn;
  i = 0; while (i < seqlen) {
    lispval elt = argvec[0]=kno_seq_elt(firstseq,i), new_elt;
    if (fntype == tablefn)
      new_elt = kno_get(fn,elt,elt);
    else if (fntype == oidfn)
      new_elt = kno_frame_get(elt,fn);
    else if (fntype == applyfn) {
      int j = 1; while (j<n_seqs) {
        argvec[j]=kno_seq_elt(sequences[j],i); j++;}
      argvec[0]=elt;
      new_elt = kno_apply(fn,n_seqs,argvec);
      j = 1; while (j<n_seqs) {
        kno_decref(argvec[j]); j++;}}
    else new_elt = kno_get(elt,fn,elt);
    kno_decref(elt);
    if (result_type == kno_string_type) {
      if (!(TYPEP(new_elt,kno_character_type)))
        result_type = kno_vector_type;}
    else if ((result_type == kno_packet_type)||
             (result_type == kno_secret_type)) {
      if (FIXNUMP(new_elt)) {
        long long intval = FIX2INT(new_elt);
        if ((intval<0) || (intval>=0x100))
          result_type = kno_vector_type;}
      else result_type = kno_vector_type;}
    if (KNO_ABORTED(new_elt)) return new_elt;
    else {kno_decref(new_elt); i++;}}
  return VOID;
}

KNO_DCLPRIM("for-each",foreach_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	    "`(for-each *arg0* *arg1* *args...*)` **undocumented**");
static lispval foreach_prim(int n,lispval *args)
{
  /* This iterates over a sequence or set of sequences and applies
     FN to each of the elements. */
  return kno_foreach(args[0],n-1,args+1);
}

KNO_DCLPRIM("map->choice",kno_map2choice,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	    "`(map->choice-each *fn* *seq*)` **undocumented**");
KNO_EXPORT lispval kno_map2choice(lispval fn,lispval sequence)
{
  if (!(KNO_SEQUENCEP(sequence)))
    return kno_type_error("sequence","kno_mapseq",sequence);
  else if (NILP(sequence)) return sequence;
  else if ((KNO_APPLICABLEP(fn)) || (TABLEP(fn)) || (ATOMICP(fn))) {
    int i = 0, len = kno_seq_length(sequence);
    kno_lisp_type result_type = KNO_LISP_TYPE(sequence);
    lispval *results = u8_alloc_n(len,lispval);
    while (i < len) {
      lispval elt = kno_seq_elt(sequence,i), new_elt;
      if (TABLEP(fn))
        new_elt = kno_get(fn,elt,elt);
      else if (KNO_APPLICABLEP(fn))
        new_elt = kno_apply(fn,1,&elt);
      else if (OIDP(elt))
        new_elt = kno_frame_get(elt,fn);
      else new_elt = kno_get(elt,fn,elt);
      kno_decref(elt);
      if (result_type == kno_string_type) {
        if (!(TYPEP(new_elt,kno_character_type)))
          result_type = kno_vector_type;}
      else if ((result_type == kno_packet_type)||
               (result_type == kno_secret_type)) {
        if (FIXNUMP(new_elt)) {
          long long intval = FIX2INT(new_elt);
          if ((intval<0) || (intval>=0x100))
            result_type = kno_vector_type;}
        else result_type = kno_vector_type;}
      if (KNO_ABORTED(new_elt)) {
        int j = 0; while (j<i) {kno_decref(results[j]); j++;}
        u8_free(results);
        return new_elt;}
      else results[i++]=new_elt;}
    return kno_make_choice(len,results,KNO_CHOICE_FREEDATA);}
  else return kno_err(kno_NotAFunction,"MAP",NULL,fn);
}

KNO_DCLPRIM("remove",kno_remove,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	    "`(remove *elt* *seq*)` **undocumented**");
KNO_DCLPRIM("reduce",kno_reduce,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	    "`(reduce *fcn* *base* *seq*)` **undocumented**");

/* Reduction */

KNO_EXPORT lispval kno_reduce(lispval fn,lispval sequence,lispval result)
{
  int i = 0, len = kno_seq_length(sequence);
  if (len==0)
    if (VOIDP(result))
      return kno_err(EmptyReduce,"kno_reduce",NULL,sequence);
    else return kno_incref(result);
  else if (len<0)
    return KNO_ERROR;
  else if (!(KNO_APPLICABLEP(fn)))
    return kno_err(kno_NotAFunction,"MAP",NULL,fn);
  else if (VOIDP(result)) {
    result = kno_seq_elt(sequence,0); i = 1;}
  while (i < len) {
    lispval elt = kno_seq_elt(sequence,i), rail[2], new_result;
    rail[0]=elt; rail[1]=result;
    new_result = kno_apply(fn,2,rail);
    kno_decref(result); kno_decref(elt);
    if (KNO_ABORTP(new_result)) return new_result;
    result = new_result;
    i++;}
  return result;
}

/* Scheme primitives */

KNO_DCLPRIM1("sequence?",sequencep_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(SEQUENCE? *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval sequencep_prim(lispval x)
{
  if (KNO_COMPOUNDP(x)) {
    struct KNO_COMPOUND *c = (kno_compound) x;
    if (c->compound_off >= 0)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (KNO_SEQUENCEP(x))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM1("length",seqlen_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(LENGTH *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seqlen_prim(lispval x)
{
  int len = kno_seq_length(x);
  if (len<0)
    return kno_type_error(_("sequence"),"seqlen",x);
  else return KNO_INT(len);
}

KNO_DCLPRIM2("elt",seqelt_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(ELT *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval seqelt_prim(lispval x,lispval offset)
{
  char buf[32]; int off; lispval result;
  if (!(KNO_SEQUENCEP(x)))
    return kno_type_error(_("sequence"),"seqelt_prim",x);
  else if (KNO_SECRETP(x))
    return kno_err(kno_SecretData,"seqelt_prim",NULL,x);
  else if (KNO_INTP(offset))
    off = FIX2INT(offset);
  else return kno_type_error(_("fixnum"),"seqelt_prim",offset);
  if (off<0) off = kno_seq_length(x)+off;
  result = kno_seq_elt(x,off);
  if (result == KNO_TYPE_ERROR)
    return kno_type_error(_("sequence"),"seqelt_prim",x);
  else if (result == KNO_RANGE_ERROR) {
    return kno_err(kno_RangeError,"seqelt_prim",
                  u8_uitoa10(kno_getint(offset),buf),
                  x);}
  else return result;
}

/* This is for use in filters, especially PICK and REJECT */

enum COMPARISON {
  cmp_lt, cmp_lte, cmp_eq, cmp_gte, cmp_gt };

static int has_length_helper(lispval x,lispval length_arg,enum COMPARISON cmp)
{
  int seqlen = kno_seq_length(x), testlen;
  if (seqlen<0)
    return kno_type_error(_("sequence"),"seqlen",x);
  else if (KNO_INTP(length_arg))
    testlen = (FIX2INT(length_arg));
  else return kno_type_error(_("fixnum"),"has-length?",x);
  switch (cmp) {
  case cmp_lt: return (seqlen<testlen);
  case cmp_lte: return (seqlen<=testlen);
  case cmp_eq: return (seqlen == testlen);
  case cmp_gte: return (seqlen>=testlen);
  case cmp_gt: return (seqlen>testlen);
  default:
    kno_seterr("Unknown length comparison","has_length_helper",NULL,x);
    return -1;}
}

KNO_DCLPRIM2("length=",has_length_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(LENGTH= *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval has_length_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_eq);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("length<",has_length_lt_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(LENGTH< *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval has_length_lt_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_lt);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("length=<",has_length_lte_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(LENGTH=< *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval has_length_lte_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_lte);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("length>",has_length_gt_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(LENGTH> *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval has_length_gt_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_gt);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("length>=",has_length_gte_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(LENGTH>= *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval has_length_gte_prim(lispval x,lispval length_arg)
{
  int retval = has_length_helper(x,length_arg,cmp_gte);
  if (retval<0) return KNO_ERROR;
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM1("length>0",has_length_gt_zero_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(LENGTH>0 *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval has_length_gt_zero_prim(lispval x)
{
  int seqlen = kno_seq_length(x);
  if (seqlen>0) return KNO_TRUE; else return KNO_FALSE;
}

KNO_DCLPRIM1("length>1",has_length_gt_one_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(LENGTH>1 *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval has_length_gt_one_prim(lispval x)
{
  int seqlen = kno_seq_length(x);
  if (seqlen>1) return KNO_TRUE; else return KNO_FALSE;
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
      (start_arg == KNO_INT(0))) {}
  else if (FIXNUMP(start_arg))
    return kno_err(kno_RangeError,prim,"start",start_arg);
  else return kno_type_error("fixnum",prim,start_arg);
  if ((FALSEP(end_arg)) ||
      (VOIDP(end_arg)) ||
      (end_arg == KNO_INT(0))) {}
  else if (FIXNUMP(end_arg))
    return kno_err(kno_RangeError,prim,"start",end_arg);
  else return kno_type_error("fixnum",prim,end_arg);
  *startp = 0; *endp = 0;
  return VOID;
}

static int interpret_range_args(int len,u8_context caller,
                                lispval start_arg,lispval end_arg,
                                int *startp,int *endp,int *deltap)
{
  int start, end;
  if (KNO_VOIDP(start_arg)) {
    *startp=0; *endp=len; *deltap=1;
    return len;}
  else if (KNO_FIXNUMP(start_arg))
    start = KNO_FIX2INT(start_arg);
  else if (KNO_FALSEP(start_arg))
    start = 0;
  else return -1;
  if (start<0) start = len+start;
  if ( (KNO_VOIDP(end_arg)) || (KNO_FALSEP(end_arg)) )
    end = len;
  else if (KNO_FIXNUMP(end_arg)) {
    end = KNO_FIX2INT(end_arg);
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
  else if (!(KNO_SEQUENCEP(seq)))
    return kno_type_error(_("sequence"),prim,seq);
  else if (KNO_INTP(start_arg)) {
    int arg = FIX2INT(start_arg);
    if (arg<0)
      return kno_err(kno_RangeError,prim,"start",start_arg);
    else *startp = arg;}
  else if ((VOIDP(start_arg)) || (FALSEP(start_arg)))
    *startp = 0;
  else return kno_type_error("fixnum",prim,start_arg);
  if ((VOIDP(end_arg)) || (FALSEP(end_arg)))
    *endp = kno_seq_length(seq);
  else if (KNO_INTP(end_arg)) {
    int arg = FIX2INT(end_arg);
    if (arg<0) {
      int len = kno_seq_length(seq), off = len+arg;
      if (off>=0) *endp = off;
      else return kno_err(kno_RangeError,prim,"start",end_arg);}
    else if (arg>kno_seq_length(seq))
      return kno_err(kno_RangeError,prim,"start",end_arg);
    else *endp = arg;}
  else return kno_type_error("fixnum",prim,end_arg);
  return VOID;
}

KNO_DCLPRIM3("slice",slice_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
 "`(SLICE *arg0* *arg1* [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_FALSE);
static lispval slice_prim(lispval x,lispval start_arg,lispval end_arg)
{
  int start, end; char buf[128];
  lispval result = check_range("slice_prim",x,start_arg,end_arg,&start,&end);
  if (KNO_ABORTED(result)) return result;
  else result = kno_slice(x,start,end);
  if (result == KNO_TYPE_ERROR)
    return kno_type_error(_("sequence"),"slice_prim",x);
  else if (result == KNO_RANGE_ERROR) {
    return kno_err(kno_RangeError,"slice_prim",
                  u8_sprintf(buf,128,"%d[%d:%d]",kno_seq_length(x),start,end),
                  x);}
  else return result;
}

KNO_DCLPRIM4("position",position_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
 "`(POSITION *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_INT(0),kno_any_type,KNO_FALSE);
static lispval position_prim(lispval key,lispval x,lispval start_arg,lispval end_arg)
{
  int result, start, end; char buf[128];
  lispval check = check_range("position_prim",x,start_arg,end_arg,&start,&end);
  if (KNO_ABORTED(check))
    return check;
  else result = kno_position(key,x,start,end);
  if (result>=0) return KNO_INT(result);
  else if (result == -1) return KNO_FALSE;
  else if (result == -2)
    return KNO_ERROR;
  else if (result == -3) {
    return kno_err(kno_RangeError,"position_prim",
                  u8_sprintf(buf,128,"%d[%d:%d]",kno_seq_length(x),start,end),
                  x);}
  else return KNO_INT(result);
}

KNO_DCLPRIM4("rposition",rposition_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
 "`(RPOSITION *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_INT(0),kno_any_type,KNO_FALSE);
static lispval rposition_prim(lispval key,lispval x,lispval start_arg,lispval end_arg)
{
  int result, start, end; char buf[128];
  lispval check = check_range("rposition_prim",x,start_arg,end_arg,&start,&end);
  if (KNO_ABORTED(check)) return check;
  else result = kno_rposition(key,x,start,end);
  if (result>=0) return KNO_INT(result);
  else if (result == -1) return KNO_FALSE;
  else if (result == -2)
    return KNO_ERROR;
  else if (result == -3) {
    return kno_err(kno_RangeError,"rposition_prim",
                  u8_sprintf(buf,128,"%d[%d:%d]",kno_seq_length(x),start,end),
                  x);}
  else return KNO_INT(result);
}

KNO_DCLPRIM4("find",find_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
 "`(FIND *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_INT(0),kno_any_type,KNO_FALSE);
static lispval find_prim(lispval key,lispval x,lispval start_arg,lispval end_arg)
{
  int result, start, end; char buf[128];
  lispval check = check_range("find_prim",x,start_arg,end_arg,&start,&end);
  if (KNO_ABORTED(check)) return check;
  else result = kno_position(key,x,start,end);
  if (result>=0) return KNO_TRUE;
  else if (result == -1) return KNO_FALSE;
  else if (result == -2)
    return kno_type_error(_("sequence"),"find_prim",x);
  else if (result == -3) {
    return kno_err(kno_RangeError,"find_prim",
                  u8_sprintf(buf,128,"%d[%d:%d]",kno_seq_length(x),start,end),
                  x);}
  else return KNO_FALSE;
}

KNO_DCLPRIM4("search",search_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
 "`(SEARCH *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_INT(0),kno_any_type,KNO_FALSE);
static lispval search_prim(lispval key,lispval x,lispval start_arg,lispval end_arg)
{
  int result, start, end;
  lispval check = check_range("search_prim",x,start_arg,end_arg,&start,&end);
  if (KNO_ABORTED(check))
    return check;
  else result = kno_search(key,x,start,end);
  if (result>=0) return KNO_INT(result);
  else if (result == -1)
    return KNO_FALSE;
  else if (result == -2)
    return KNO_ERROR;
  else if (result == -3)
    return kno_type_error(_("sequence"),"search_prim",x);
  else return result;
}

KNO_DCLPRIM4("every?",every_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
 "`(EVERY? *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval every_prim(lispval proc,lispval x,lispval start_arg,lispval end_arg)
{
  int start, end;
  lispval check = check_range("every_prim",x,start_arg,end_arg,&start,&end);
  if (KNO_ABORTED(check)) return check;
  else if (!(KNO_APPLICABLEP(proc)))
    return kno_type_error(_("function"),"every_prim",x);
  else if (STRINGP(x)) {
    const u8_byte *scan = CSTRING(x);
    int i = 0; while (i<end) {
      int c = u8_sgetc(&scan);
      if (i<start) i++;
      else if (i<end) {
        lispval lc = KNO_CODE2CHAR(c);
        lispval testval = kno_apply(proc,1,&lc);
        if (FALSEP(testval)) return KNO_FALSE;
        else if (KNO_ABORTED(testval)) return testval;
        else {
          kno_decref(testval); i++;}}
      else return KNO_TRUE;}
    return KNO_TRUE;}
  else {
    int i = start; while (i<end) {
      lispval elt = kno_seq_elt(x,i);
      lispval testval = kno_apply(proc,1,&elt);
      if (KNO_ABORTED(testval)) {
        kno_decref(elt); return testval;}
      else if (FALSEP(testval)) {
        kno_decref(elt); return KNO_FALSE;}
      else {
        kno_decref(elt); kno_decref(testval); i++;}}
    return KNO_TRUE;}
}

KNO_DCLPRIM4("some?",some_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
 "`(SOME? *arg0* *arg1* [*arg2*] [*arg3*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval some_prim(lispval proc,lispval x,lispval start_arg,lispval end_arg)
{
  int start, end;
  lispval check = check_range("some_prim",x,start_arg,end_arg,&start,&end);
  if (KNO_ABORTED(check)) return check;
  else if (!(KNO_APPLICABLEP(proc)))
    return kno_type_error(_("function"),"some_prim",x);
  else if (STRINGP(x)) {
    const u8_byte *scan = CSTRING(x);
    int i = 0; while (i<end) {
      int c = u8_sgetc(&scan);
      if (i<start) i++;
      else if (i< end) {
        lispval lc = KNO_CODE2CHAR(c);
        lispval testval = kno_apply(proc,1,&lc);
        if (KNO_ABORTED(testval)) return testval;
        else if (FALSEP(testval)) i++;
        else {
          kno_decref(testval); return KNO_TRUE;}}
      else return KNO_FALSE;}
    return KNO_FALSE;}
  else {
    int i = start; while (i<end) {
      lispval elt = kno_seq_elt(x,i);
      lispval testval = kno_apply(proc,1,&elt);
      if (KNO_ABORTED(testval)) {
        kno_decref(elt); return testval;}
      else if ((FALSEP(testval)) || (EMPTYP(testval))) {
        kno_decref(elt); i++;}
      else {
        kno_decref(elt); kno_decref(testval);
        return KNO_TRUE;}}
    return KNO_FALSE;}
}

KNO_DCLPRIM2("remove-if",removeif_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	     "`(REMOVE-IF *pred* *seq*)` **undocumented**",
	     kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval removeif_prim(lispval test,lispval sequence)
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
      lispval r = kno_removeif(test,seq,0);
      if (KNO_ABORTED(r)) {
	kno_decref(results); return r;}
      CHOICE_ADD(results,r);}
    return results;}
  else return kno_removeif(test,sequence,0);
}

KNO_DCLPRIM2("remove-if-not",removeifnot_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
 "`(REMOVE-IF-NOT *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval removeifnot_prim(lispval test,lispval sequence)
{
  if (EMPTYP(sequence)) return sequence;
  else if (CHOICEP(sequence)) {
    lispval results = EMPTY;
    DO_CHOICES(seq,sequence) {
      lispval r = kno_removeif(test,seq,1);
      if (KNO_ABORTED(r)) {
        kno_decref(results); return r;}
      CHOICE_ADD(results,r);}
    return results;}
  else return kno_removeif(test,sequence,1);
}

static lispval range_error(u8_context caller,lispval seq,int len,int start,int end)
{
  u8_byte buf[128];
  kno_seterr(kno_RangeError,caller,
            u8_sprintf(buf,128,"(0,%d,%d,%d)",start,end,len),
            seq);
  return KNO_ERROR_VALUE;
}

/* Sequence-if/if-not functions */
KNO_DCLPRIM4("position-if",position_if_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
 "`(POSITION-IF *test* *sequence* [*start*] [*end*])` "
 "returns the position of element of *sequence* for "
 "which *test* returns true. POSITION-IF searches "
 "between between *start* and *end*, which default "
 "to zero and the length of the sequence. if "
 "*start* > *end*, this returns the last position "
 "in the range, otherwise it returns the first. If "
 "either *start* or *end* are negative, they are "
 "taken as offsets from the end of the sequence.",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_INT(0),kno_any_type,KNO_FALSE);

lispval position_if_prim(lispval test,lispval seq,lispval start_arg,
                         lispval end_arg)
{
  int end, start = (KNO_FIXNUMP(start_arg)) ? (KNO_FIX2INT(start_arg)) : (0);
  int ctype = KNO_LISP_TYPE(seq);
  switch (ctype) {
  case kno_vector_type: {
    int len = KNO_VECTOR_LENGTH(seq), delta = 1;
    if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
    int ok = interpret_range_args
      (len,"position_if_prim",start_arg,end_arg,&start,&end,&delta);
    if (ok<0) return range_error("position_if_prim",seq,len,start,end);
    lispval *data = KNO_VECTOR_ELTS(seq);
    int i = start; while (i!=end) {
      lispval v = kno_apply(test,1,data+i);
      if (KNO_ABORTP(v)) return v;
      else if (!(KNO_FALSEP(v))) {
        kno_decref(v);
        return KNO_INT(i);}
      else i += delta;}
    return KNO_FALSE;}
  case kno_compound_type: {
      int len = KNO_COMPOUND_VECLEN(seq), delta = 1;
      if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
      lispval *data = KNO_COMPOUND_VECELTS(seq);
      int ok = interpret_range_args
        (len,"position_if_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("position_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = kno_apply(test,1,data+i);
        if (KNO_ABORTP(v)) return v;
        else if (!(KNO_FALSEP(v))) {
          kno_decref(v);
          return KNO_INT(i);}
        else i += delta;}
      return KNO_FALSE;}
    case kno_pair_type: {
      int i = 0, pos = -1, len = -1, delta = 1;
      lispval scan = seq;
      if ( (start<0) || (!(KNO_FIXNUMP(end_arg))) ||
           ( (KNO_FIX2INT(end_arg)) < 0) ) {
        len = kno_list_length(seq);
        if (start<0) start = start+len;
        if (!(KNO_FIXNUMP(end_arg)))
          end = len;
        else if ((KNO_FIX2INT(end_arg))<0)
          end = len + KNO_FIX2INT(end_arg);
        else end = KNO_FIX2INT(end_arg);
        int ok = interpret_range_args
          (len,"position_if_prim",start_arg,end_arg,&start,&end,&delta);
        if (ok<0) return range_error("position_if_prim",seq,len,start,end);}
      else end = KNO_FIX2INT(end_arg);
      while (KNO_PAIRP(scan)) {
        lispval elt = KNO_CAR(scan);
        if (i < start) {
          i++; scan=KNO_CDR(scan);
          continue;}
        else if (i > end) break;
        lispval v = kno_apply(test,1,&elt);
        if (KNO_ABORTP(v)) return v;
        else if (!(KNO_FALSEP(v))) {
          kno_decref(v);
          if (start>end) pos=i;
          else return KNO_INT(i);}
        else {
          scan = KNO_CDR(scan);
          i++;}}
      if (pos<0) return KNO_FALSE;
      else return KNO_INT(pos);}
  default: {
      if (NILP(seq))
        return KNO_FALSE;
      int len = -1, delta = 1;
      lispval *data = kno_seq_elts(seq,&len);
      if (len < 0) {
        kno_seterr("UnenumerableSequence","position_if_prim",NULL,seq);
        return KNO_ERROR_VALUE;}
      if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
      int ok = interpret_range_args
        (len,"position_if_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("position_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = kno_apply(test,1,data+i);
        if (KNO_ABORTP(v)) {
          kno_decref_vec(data,len);
          u8_free(data);
          return v;}
        else if (!(KNO_FALSEP(v))) {
          kno_decref(v);
          kno_decref_vec(data,len);
          u8_free(data);
          return KNO_INT(i);}
        else i += delta;}
      kno_decref_vec(data,len);
      u8_free(data);
      return KNO_FALSE;}
  }
}
KNO_DCLPRIM4("position-if-not",position_if_not_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
 "`(POSITION-IF-NOT *test* *sequence* [*start*] [*end*])` "
 "returns the position of element of *sequence* for "
 "which *test* returns false. POSITION-IF-NOT "
 "searches between between *start* and *end*, which "
 "default to zero and the length of the sequence. "
 "if *start* > *end*, this returns the last "
 "position in the range, otherwise it returns the "
 "first. If either *start* or *end* are negative, "
 "they are taken as offsets from the end of the "
 "sequence.",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_INT(0),kno_any_type,KNO_FALSE);

lispval position_if_not_prim(lispval test,lispval seq,lispval start_arg,
                             lispval end_arg)
{
  int start = KNO_FIX2INT(start_arg), end;
  int ctype = KNO_LISP_TYPE(seq);
  switch (ctype) {
  case kno_vector_type: {
    int len = KNO_VECTOR_LENGTH(seq), delta = 1;
    if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
    int ok = interpret_range_args
      (len,"position_if_not_prim",start_arg,end_arg,&start,&end,&delta);
    if (ok<0) return range_error("position_if_prim",seq,len,start,end);
    lispval *data = KNO_VECTOR_ELTS(seq);
    int i = start; while (i!=end) {
      lispval v = kno_apply(test,1,data+i);
      if (KNO_ABORTP(v)) return v;
      else if (KNO_FALSEP(v)) {
        return KNO_INT(i);}
      else {
        kno_decref(v);
        i += delta;}}
    return KNO_FALSE;}
  case kno_compound_type: {
      int len = KNO_COMPOUND_VECLEN(seq), delta = 1;
      if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
      lispval *data = KNO_COMPOUND_VECELTS(seq);
      int ok = interpret_range_args
        (len,"position_if_not_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("position_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = kno_apply(test,1,data+i);
        if (KNO_ABORTP(v)) return v;
        else if (KNO_FALSEP(v))
          return KNO_INT(i);
        else {
          kno_decref(v);
          i += delta;}
        return KNO_FALSE;}}
  case kno_pair_type: {
    int i = 0, pos = -1, len = -1, delta = 1;
    lispval scan = seq;
    if ( (start<0) || (!(KNO_FIXNUMP(end_arg))) ||
         ( (KNO_FIX2INT(end_arg)) < 0) ) {
      len = kno_list_length(seq);
      if (start<0) start = start+len;
      if (!(KNO_FIXNUMP(end_arg)))
          end = len;
        else if ((KNO_FIX2INT(end_arg))<0)
          end = len + KNO_FIX2INT(end_arg);
        else end = KNO_FIX2INT(end_arg);
        int ok = interpret_range_args
          (len,"position_if_not_prim",start_arg,end_arg,&start,&end,&delta);
        if (ok<0) return range_error("position_if_prim",seq,len,start,end);}
      else end = KNO_FIX2INT(end_arg);
      while (KNO_PAIRP(scan)) {
        lispval elt = KNO_CAR(scan);
        if (i < start) {
          i++; scan=KNO_CDR(scan);
          continue;}
        else if (i > end) break;
        lispval v = kno_apply(test,1,&elt);
        if (KNO_ABORTP(v)) return v;
        else if (KNO_FALSEP(v)) {
          if (start>end) pos=i;
          else return KNO_INT(i);}
        else {
          kno_decref(v);
          scan = KNO_CDR(scan);
          i++;}}
      if (pos<0) return KNO_FALSE;
      else return KNO_INT(pos);}
  default: {
      if (NILP(seq))
        return KNO_FALSE;
      int len = -1, delta = 1;
      lispval *data = kno_seq_elts(seq,&len);
      if (len < 0) {
        kno_seterr("UnenumerableSequence","position_if_prim",NULL,seq);
        return KNO_ERROR_VALUE;}
      if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
      int ok = interpret_range_args
        (len,"position_if_not_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("position_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = kno_apply(test,1,data+i);
        if (KNO_ABORTP(v)) {
          kno_decref_vec(data,len);
          u8_free(data);
          return v;}
        else if (KNO_FALSEP(v)) {
          kno_decref_vec(data,len);
          u8_free(data);
          return KNO_INT(i);}
        else {
          kno_decref(v);
          i += delta;}}
      kno_decref_vec(data,len);
      u8_free(data);
      return KNO_FALSE;}
  }
}
KNO_DCLPRIM5("find-if",find_if_prim,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
 "`(FIND-IF *test* *sequence* [*start*] [*end*] [*noneval*])` "
 "returns an element of *sequence* for which *test* "
 "returns true or *noneval* otherwise.FIND-IF "
 "searches between between *start* and *end*, "
 "defaulting to zero and the length of the "
 "sequence. if *start* > *end*, this returns the "
 "last occurrence in the range, otherwise it "
 "returns the first. If either *start* or *end* are "
 "negative, they are taken as offsets from the end "
 "of the sequence.",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_INT(0),kno_any_type,KNO_FALSE,
 kno_any_type,KNO_FALSE);

lispval find_if_prim(lispval test,lispval seq,lispval start_arg,
                     lispval end_arg,lispval fail_val)
{
  int end, start = (KNO_FIXNUMP(start_arg)) ? (KNO_FIX2INT(start_arg)) : (0);
  int ctype = KNO_LISP_TYPE(seq);
  switch (ctype) {
  case kno_vector_type: {
    int len = KNO_VECTOR_LENGTH(seq), delta = 1;
    if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
    int ok = interpret_range_args
      (len,"find_if_prim",start_arg,end_arg,&start,&end,&delta);
    if (ok<0) return range_error("find_if_prim",seq,len,start,end);
    lispval *data = KNO_VECTOR_ELTS(seq);
    int i = start; while (i!=end) {
      lispval v = kno_apply(test,1,data+i);
      if (KNO_ABORTP(v)) return v;
      else if (!(KNO_FALSEP(v))) {
        kno_decref(v);
        return kno_incref(data[i]);}
      else i += delta;}
    return kno_incref(fail_val);}
  case kno_compound_type: {
      int len = KNO_COMPOUND_VECLEN(seq), delta = 1;
      if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
      lispval *data = KNO_COMPOUND_VECELTS(seq);
      int ok = interpret_range_args
        (len,"find_if_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("find_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = kno_apply(test,1,data+i);
        if (KNO_ABORTP(v)) return v;
        else if (!(KNO_FALSEP(v))) {
          kno_decref(v);
          return kno_incref(data[i]);}
        else i += delta;}
      return kno_incref(fail_val);}
    case kno_pair_type: {
      int i = 0, pos = -1, len = -1, delta = 1;
      lispval scan = seq, pos_elt = KNO_VOID;
      if ( (start<0) || (!(KNO_FIXNUMP(end_arg))) ||
           ( (KNO_FIX2INT(end_arg)) < 0) ) {
        len = kno_list_length(seq);
        if (start<0) start = start+len;
        if (!(KNO_FIXNUMP(end_arg)))
          end = len;
        else if ((KNO_FIX2INT(end_arg))<0)
          end = len + KNO_FIX2INT(end_arg);
        else end = KNO_FIX2INT(end_arg);
        int ok = interpret_range_args
          (len,"find_if_prim",start_arg,end_arg,&start,&end,&delta);
        if (ok<0) return range_error("find_if_prim",seq,len,start,end);}
      else end = KNO_FIX2INT(end_arg);
      while (KNO_PAIRP(scan)) {
        lispval elt = KNO_CAR(scan);
        if (i < start) {
          i++; scan=KNO_CDR(scan);
          continue;}
        else if (i > end) break;
        lispval v = kno_apply(test,1,&elt);
        if (KNO_ABORTP(v)) return v;
        else if (!(KNO_FALSEP(v))) {
          kno_decref(v);
          if (start>end) {
            pos=i; pos_elt = elt;}
          else return kno_incref(elt);}
        else {
          scan = KNO_CDR(scan);
          i++;}}
      if (pos<0)
        return kno_incref(fail_val);
      else return kno_incref(pos_elt);}
  default: {
      if (NILP(seq))
        return kno_incref(fail_val);
      int len = -1, delta = 1;
      lispval *data = kno_seq_elts(seq,&len);
      if (len < 0) {
        kno_seterr("UnenumerableSequence","find_if_prim",NULL,seq);
        return KNO_ERROR_VALUE;}
      if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
      int ok = interpret_range_args
        (len,"find_if_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("find_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = kno_apply(test,1,data+i);
        if (KNO_ABORTP(v)) {
          kno_decref_vec(data,len);
          u8_free(data);
          return v;}
        else if (!(KNO_FALSEP(v))) {
          lispval elt = data[i]; kno_incref(elt);
          kno_decref(v);
          kno_decref_vec(data,len);
          u8_free(data);
          return elt;}
        else i += delta;}
      kno_decref_vec(data,len);
      u8_free(data);
      return kno_incref(fail_val);}
  }
}
KNO_DCLPRIM5("find-if-not",find_if_not_prim,KNO_MAX_ARGS(5)|KNO_MIN_ARGS(2),
 "`(FIND-IF-NOT *test* *sequence* [*start*] [*end*] [*noneval*])` "
 "returns an element of *sequence* for which *test* "
 "returns false or *noneval* otherwise.FIND-IF_NOT "
 "searches between between *start* and *end*, which "
 "default to zero and the length of the sequence. "
 "if *start* > *end*, this returns the last "
 "occurrence in the range, otherwise it returns the "
 "first. If either *start* or *end* are negative, "
 "they are taken as offsets from the end of the "
 "sequence.",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_INT(0),kno_any_type,KNO_FALSE,
 kno_any_type,KNO_FALSE);

lispval find_if_not_prim(lispval test,lispval seq,lispval start_arg,
                         lispval end_arg,
                         lispval fail_val)
{
  int start = KNO_FIX2INT(start_arg), end;
  int ctype = KNO_LISP_TYPE(seq);
  switch (ctype) {
  case kno_vector_type: {
    int len = KNO_VECTOR_LENGTH(seq), delta = 1;
    if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
    int ok = interpret_range_args
      (len,"find_if_not_prim",start_arg,end_arg,&start,&end,&delta);
    if (ok<0) return range_error("find_if_prim",seq,len,start,end);
    lispval *data = KNO_VECTOR_ELTS(seq);
    int i = start; while (i!=end) {
      lispval v = kno_apply(test,1,data+i);
      if (KNO_ABORTP(v)) return v;
      else if (KNO_FALSEP(v)) {
        return kno_incref(data[i]);}
      else {
        kno_decref(v);
        i += delta;}}
    return kno_incref(fail_val);}
  case kno_compound_type: {
      int len = KNO_COMPOUND_VECLEN(seq), delta = 1;
      if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
      lispval *data = KNO_COMPOUND_VECELTS(seq);
      int ok = interpret_range_args
        (len,"find_if_not_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("find_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = kno_apply(test,1,data+i);
        if (KNO_ABORTP(v)) return v;
        else if (KNO_FALSEP(v))
          return kno_incref(data[i]);
        else {
          kno_decref(v);
          i += delta;}
        return kno_incref(fail_val);}}
  case kno_pair_type: {
    int i = 0, pos = -1, len = -1, delta = 1;
    lispval scan = seq, pos_elt = KNO_VOID;
    if ( (start<0) || (!(KNO_FIXNUMP(end_arg))) ||
         ( (KNO_FIX2INT(end_arg)) < 0) ) {
      len = kno_list_length(seq);
      if (start<0) start = start+len;
      if (!(KNO_FIXNUMP(end_arg)))
          end = len;
        else if ((KNO_FIX2INT(end_arg))<0)
          end = len + KNO_FIX2INT(end_arg);
        else end = KNO_FIX2INT(end_arg);
        int ok = interpret_range_args
          (len,"find_if_not_prim",start_arg,end_arg,&start,&end,&delta);
        if (ok<0) return range_error("find_if_prim",seq,len,start,end);}
      else end = KNO_FIX2INT(end_arg);
      while (KNO_PAIRP(scan)) {
        lispval elt = KNO_CAR(scan);
        if (i < start) {
          i++; scan=KNO_CDR(scan);
          continue;}
        else if (i > end) break;
        lispval v = kno_apply(test,1,&elt);
        if (KNO_ABORTP(v)) return v;
        else if (KNO_FALSEP(v)) {
          if (start>end) {
            pos=i; pos_elt=elt;}
          else return kno_incref(elt);}
        else {
          kno_decref(v);
          scan = KNO_CDR(scan);
          i++;}}
      if (pos<0)
        return kno_incref(fail_val);
      else kno_incref(pos_elt);}
  default: {
      if (NILP(seq))
        return kno_incref(fail_val);
      int len = -1, delta = 1;
      lispval *data = kno_seq_elts(seq,&len);
      if (len < 0) {
        kno_seterr("UnenumerableSequence","find_if_prim",NULL,seq);
        return KNO_ERROR_VALUE;}
      if (KNO_FIXNUMP(end_arg)) end = KNO_FIX2INT(end_arg); else end = len;
      int ok = interpret_range_args
        (len,"find_if_not_prim",start_arg,end_arg,&start,&end,&delta);
      if (ok<0) return range_error("find_if_prim",seq,len,start,end);
      int i = start; while (i!=end) {
        lispval v = kno_apply(test,1,data+i);
        if (KNO_ABORTP(v)) {
          kno_decref_vec(data,len);
          u8_free(data);
          return v;}
        else if (KNO_FALSEP(v)) {
          lispval elt = data[i]; kno_incref(elt);
          kno_decref_vec(data,len);
          u8_free(data);
          return elt;}
        else {
          kno_decref(v);
          i += delta;}}
      kno_decref_vec(data,len);
      u8_free(data);
      return kno_incref(fail_val);}
  }
}

/* Small element functions */

static lispval seq_elt(lispval x,char *cxt,int i)
{
  lispval v = kno_seq_elt(x,i);
  if (KNO_TROUBLEP(v))
    return kno_err(kno_retcode_to_exception(v),cxt,NULL,x);
  else return v;
}

KNO_DCLPRIM1("first",first,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(FIRST *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval first(lispval x)
{
  return seq_elt(x,"first",0);
}
KNO_DCLPRIM1("rest",rest,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(REST *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval rest(lispval x)
{
  if (PAIRP(x))
    return kno_incref(KNO_CDR(x));
  else {
    lispval v = kno_slice(x,1,-1);
    if (KNO_TROUBLEP(v))
      return kno_err(kno_retcode_to_exception(v),"rest",NULL,x);
    else return v;}
}

KNO_DCLPRIM1("second",second,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(SECOND *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval second(lispval x)
{
  return seq_elt(x,"second",1);
}
KNO_DCLPRIM1("third",third,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(THIRD *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval third(lispval x)
{
  return seq_elt(x,"third",2);
}
KNO_DCLPRIM1("fourth",fourth,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(FOURTH *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval fourth(lispval x)
{
  return seq_elt(x,"fourth",3);
}
KNO_DCLPRIM1("fifth",fifth,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(FIFTH *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval fifth(lispval x)
{
  return seq_elt(x,"fifth",4);
}
KNO_DCLPRIM1("sixth",sixth,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(SIXTH *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval sixth(lispval x)
{
  return seq_elt(x,"sixth",5);
}
KNO_DCLPRIM1("seventh",seventh,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(SEVENTH *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seventh(lispval x)
{
  return seq_elt(x,"seventh",6);
}

/* Pair functions */

KNO_DCLPRIM1("empty-list?",nullp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(EMPTY-LIST? *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval nullp(lispval x)
{
  if (NILP(x)) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("cons",cons,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(CONS *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval cons(lispval x,lispval y)
{
  return kno_make_pair(x,y);
}
KNO_DCLPRIM1("car",car,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(CAR *arg0*)` **undocumented**",
 kno_pair_type,KNO_VOID);
static lispval car(lispval x)
{
  return kno_incref(KNO_CAR(x));
}
KNO_DCLPRIM1("cdr",cdr,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(CDR *arg0*)` **undocumented**",
 kno_pair_type,KNO_VOID);
static lispval cdr(lispval x)
{
  return kno_incref(KNO_CDR(x));
}
KNO_DCLPRIM1("cddr",cddr,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(CDDR *arg0*)` **undocumented**",
 kno_pair_type,KNO_VOID);
static lispval cddr(lispval x)
{
  if (PAIRP(KNO_CDR(x)))
    return kno_incref(KNO_CDR(KNO_CDR(x)));
  else return kno_err(kno_RangeError,"CDDR",NULL,x);
}
KNO_DCLPRIM1("cadr",cadr,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(CADR *arg0*)` **undocumented**",
 kno_pair_type,KNO_VOID);
static lispval cadr(lispval x)
{
  if (PAIRP(KNO_CDR(x)))
    return kno_incref(KNO_CAR(KNO_CDR(x)));
  else return kno_err(kno_RangeError,"CADR",NULL,x);
}
KNO_DCLPRIM1("cdar",cdar,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(CDAR *arg0*)` **undocumented**",
 kno_pair_type,KNO_VOID);
static lispval cdar(lispval x)
{
  if (PAIRP(KNO_CAR(x)))
    return kno_incref(KNO_CDR(KNO_CAR(x)));
  else return kno_err(kno_RangeError,"CDAR",NULL,x);
}
KNO_DCLPRIM1("caar",caar,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(CAAR *arg0*)` **undocumented**",
 kno_pair_type,KNO_VOID);
static lispval caar(lispval x)
{
  if (PAIRP(KNO_CAR(x)))
    return kno_incref(KNO_CAR(KNO_CAR(x)));
  else return kno_err(kno_RangeError,"CAAR",NULL,x);
}
KNO_DCLPRIM1("caddr",caddr,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(CADDR *arg0*)` **undocumented**",
 kno_pair_type,KNO_VOID);
static lispval caddr(lispval x)
{
  if ((PAIRP(KNO_CDR(x)))&&(PAIRP(KNO_CDR(KNO_CDR(x)))))
    return kno_incref(KNO_CAR(KNO_CDR(KNO_CDR(x))));
  else return kno_err(kno_RangeError,"CADR",NULL,x);
}
KNO_DCLPRIM1("cdddr",cdddr,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(CDDDR *arg0*)` **undocumented**",
 kno_pair_type,KNO_VOID);
static lispval cdddr(lispval x)
{
  if ((PAIRP(KNO_CDR(x)))&&(PAIRP(KNO_CDR(KNO_CDR(x)))))
    return kno_incref(KNO_CDR(KNO_CDR(KNO_CDR(x))));
  else return kno_err(kno_RangeError,"CADR",NULL,x);
}

KNO_DCLPRIM("cons*",cons_star,KNO_VAR_ARGS|KNO_MIN_ARGS(1),
 "`(CONS* *arg0* *args...*)` **undocumented**");
static lispval cons_star(int n,lispval *args)
{
  int i = n-2; lispval list = kno_incref(args[n-1]);
  while (i>=0) {
    list = kno_conspair(kno_incref(args[i]),list);
    i--;}
  return list;
}

/* Association list functions */

KNO_DCLPRIM2("assq",assq_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(ASSQ *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval assq_prim(lispval key,lispval list)
{
  if (NILP(list)) return KNO_FALSE;
  else if (!(PAIRP(list)))
    return kno_type_error("alist","assq_prim",list);
  else {
    lispval scan = list;
    while (PAIRP(scan))
      if (!(PAIRP(KNO_CAR(scan))))
        return kno_type_error("alist","assq_prim",list);
      else {
        lispval item = KNO_CAR(KNO_CAR(scan));
        if (KNO_EQ(key,item))
          return kno_incref(KNO_CAR(scan));
        else scan = KNO_CDR(scan);}
    return KNO_FALSE;}
}

KNO_DCLPRIM2("assv",assv_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(ASSV *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval assv_prim(lispval key,lispval list)
{
  if (NILP(list)) return KNO_FALSE;
  else if (!(PAIRP(list)))
    return kno_type_error("alist","assv_prim",list);
  else {
    lispval scan = list;
    while (PAIRP(scan))
      if (!(PAIRP(KNO_CAR(scan))))
        return kno_type_error("alist","assv_prim",list);
      else {
        lispval item = KNO_CAR(KNO_CAR(scan));
        if (KNO_EQV(key,item))
          return kno_incref(KNO_CAR(scan));
        else scan = KNO_CDR(scan);}
    return KNO_FALSE;}
}

KNO_DCLPRIM2("assoc",assoc_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(ASSOC *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval assoc_prim(lispval key,lispval list)
{
  if (NILP(list)) return KNO_FALSE;
  else if (!(PAIRP(list)))
    return kno_type_error("alist","assoc_prim",list);
  else {
    lispval scan = list;
    while (PAIRP(scan))
      if (!(PAIRP(KNO_CAR(scan))))
        return kno_type_error("alist","assoc_prim",list);
      else {
        lispval item = KNO_CAR(KNO_CAR(scan));
        if (LISP_EQUAL(key,item))
          return kno_incref(KNO_CAR(scan));
        else scan = KNO_CDR(scan);}
    return KNO_FALSE;}
}

/* MEMBER functions */

KNO_DCLPRIM2("memq",memq_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(MEMQ *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval memq_prim(lispval key,lispval list)
{
  if (NILP(list)) return KNO_FALSE;
  else if (PAIRP(list)) {
    lispval scan = list;
    while (PAIRP(scan))
      if (KNO_EQ(KNO_CAR(scan),key))
        return kno_incref(scan);
      else scan = KNO_CDR(scan);
    return KNO_FALSE;}
  else return kno_type_error("list","memq_prim",list);
}

KNO_DCLPRIM2("memv",memv_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(MEMV *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval memv_prim(lispval key,lispval list)
{
  if (NILP(list)) return KNO_FALSE;
  else if (PAIRP(list)) {
    lispval scan = list;
    while (PAIRP(scan))
      if (KNO_EQV(KNO_CAR(scan),key))
        return kno_incref(scan);
      else scan = KNO_CDR(scan);
    return KNO_FALSE;}
  else return kno_type_error("list","memv_prim",list);
}

KNO_DCLPRIM2("member",member_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(MEMBER *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval member_prim(lispval key,lispval list)
{
  if (NILP(list)) return KNO_FALSE;
  else if (PAIRP(list)) {
    lispval scan = list;
    while (PAIRP(scan))
      if (KNO_EQUAL(KNO_CAR(scan),key))
        return kno_incref(scan);
      else scan = KNO_CDR(scan);
    return KNO_FALSE;}
  else return kno_type_error("list","member_prim",list);
}

/* LIST AND VECTOR */

KNO_DCLPRIM("list",list,KNO_VAR_ARGS|KNO_MIN_ARGS(0),
 "`(LIST *args...*)` **undocumented**");
static lispval list(int n,lispval *elts)
{
  lispval head = NIL, *tail = &head; int i = 0;
  while (i < n) {
    lispval pair = kno_make_pair(elts[i],NIL);
    *tail = pair; tail = &((struct KNO_PAIR *)pair)->cdr; i++;}
  return head;
}

KNO_DCLPRIM("vector",vector,KNO_VAR_ARGS|KNO_MIN_ARGS(0),
 "`(VECTOR *args...*)` **undocumented**");
static lispval vector(int n,lispval *elts)
{
  int i = 0; while (i < n) {kno_incref(elts[i]); i++;}
  return kno_make_vector(n,elts);
}

KNO_DCLPRIM2("make-vector",make_vector,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(MAKE-VECTOR *arg0* [*arg1*])` **undocumented**",
 kno_fixnum_type,KNO_VOID,kno_any_type,KNO_FALSE);
static lispval make_vector(lispval size,lispval dflt)
{
  int n = kno_getint(size);
  if (n==0)
    return kno_empty_vector(0);
  else if (n>0) {
    lispval result = kno_empty_vector(n);
    lispval *elts = KNO_VECTOR_ELTS(result);
    int i = 0; while (i < n) {
      elts[i]=kno_incref(dflt); i++;}
    return result;}
  else return kno_type_error(_("positive"),"make_vector",size);
}

KNO_DCLPRIM1("->vector",seq2vector,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(->VECTOR *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seq2vector(lispval seq)
{
  if (NILP(seq))
    return kno_empty_vector(0);
  else if (KNO_SEQUENCEP(seq)) {
    int n = -1; lispval *data = kno_seq_elts(seq,&n);
    if (n>=0) {
      lispval result = kno_make_vector(n,data);
      u8_free(data);
      return result;}
    else {
      kno_seterr("NonEnumerableSequence","seq2vector",NULL,seq);
      return KNO_ERROR_VALUE;}}
  else return kno_type_error(_("sequence"),"seq2vector",seq);
}

KNO_DCLPRIM("1vector",onevector_prim,KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
 "`(1VECTOR *args...*)` **undocumented**");
static lispval onevector_prim(int n,lispval *args)
{
  lispval elts[32], result = VOID;
  struct U8_PILE pile; int i = 0;
  if (n==0) return kno_empty_vector(0);
  else if (n==1) {
    if (VECTORP(args[0])) return kno_incref(args[0]);
    else if (PAIRP(args[0])) {}
    else if ((EMPTYP(args[0]))||(KNO_EMPTY_QCHOICEP(args[0])))
      return kno_empty_vector(0);
    else if (!(CONSP(args[0])))
      return kno_make_vector(1,args);
    else {
      kno_incref(args[0]); return kno_make_vector(1,args);}}
  U8_INIT_STATIC_PILE((&pile),elts,32);
  while (i<n) {
    lispval arg = args[i++];
    DO_CHOICES(each,arg) {
      if (!(CONSP(each))) {u8_pile_add((&pile),each);}
      else if (VECTORP(each)) {
        int len = VEC_LEN(each); int j = 0;
        while (j<len) {
          lispval elt = VEC_REF(each,j); kno_incref(elt);
          u8_pile_add(&pile,elt); j++;}}
      else if (PAIRP(each)) {
        KNO_DOLIST(elt,each) {
          kno_incref(elt); u8_pile_add(&pile,elt);}}
      else {
        kno_incref(each); u8_pile_add(&pile,each);}}}
  result = kno_make_vector(pile.u8_len,(lispval *)pile.u8_elts);
  if (pile.u8_mallocd) u8_free(pile.u8_elts);
  return result;
}

KNO_DCLPRIM1("->list",seq2list,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(->LIST *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seq2list(lispval seq)
{
  if (NILP(seq)) return NIL;
  else if (KNO_SEQUENCEP(seq)) {
    int n = -1;
    lispval *data = kno_seq_elts(seq,&n), result = NIL;
    if (n < 0) {
      kno_seterr("NonEnumerableSequence","seq2vector",NULL,seq);
      return KNO_ERROR;}
    n--; while (n>=0) {
      result = kno_conspair(data[n],result); n--;}
    u8_free(data);
    return result;}
  else return kno_type_error(_("sequence"),"seq2list",seq);
}

KNO_DCLPRIM1("->packet",seq2packet,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(->PACKET *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seq2packet(lispval seq)
{
  if (NILP(seq))
    return kno_init_packet(NULL,0,NULL);
  else if (KNO_SEQUENCEP(seq)) {
    int i = 0, n = -1;
    lispval result = VOID;
    lispval *data = kno_seq_elts(seq,&n);
    if (n < 0) {
      kno_seterr("NonEnumerableSequence","seq2vector",NULL,seq);
      return KNO_ERROR;}
    unsigned char *bytes = u8_malloc(n);
    while (i<n) {
      if (KNO_BYTEP(data[i])) {
        bytes[i]=FIX2INT(data[i]);
        i++;}
      else {
        lispval err = kno_type_error(_("byte"),"seq2packet",data[i]);
        i = 0; while (i < n) {
          kno_decref(data[i]);
          i++;}
        u8_free(data);
        return err;}}
    u8_free(data);
    result = kno_make_packet(NULL,n,bytes);
    u8_free(bytes);
    return result;}
  else return kno_type_error(_("sequence"),"seq2packet",seq);
}

KNO_DCLPRIM1("->string",x2string,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(->STRING *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval x2string(lispval seq)
{
  if (SYMBOLP(seq))
    return kno_mkstring(SYM_NAME(seq));
  else if (KNO_CHARACTERP(seq)) {
    int c = KNO_CHAR2CODE(seq);
    U8_OUTPUT out; u8_byte buf[16];
    U8_INIT_STATIC_OUTPUT_BUF(out,16,buf);
    u8_putc(&out,c);
    return kno_mkstring(out.u8_outbuf);}
  else if (NILP(seq)) return kno_mkstring("");
  else if (STRINGP(seq)) return kno_incref(seq);
  else if (KNO_SEQUENCEP(seq)) {
    U8_OUTPUT out;
    int i = 0, n = -1;
    lispval *data = kno_seq_elts(seq,&n);
    if (n < 0) {
      kno_seterr("NonEnumerableSequence","seq2vector",NULL,seq);
      return KNO_ERROR;}
    U8_INIT_OUTPUT(&out,n*2);
    while (i<n) {
      if (FIXNUMP(data[i])) {
        long long charcode = FIX2INT(data[i]);
        if ((charcode<0)||(charcode>=0x10000)) {
          lispval err = kno_type_error(_("character"),"seq2string",data[i]);
          kno_decref_vec(data,n);
          u8_free(data);
          return err;}
        u8_putc(&out,charcode); i++;}
      else if (KNO_CHARACTERP(data[i])) {
        int charcode = KNO_CHAR2CODE(data[i]);
        u8_putc(&out,charcode); i++;}
      else {
        lispval err = kno_type_error(_("character"),"seq2string",data[i]);
        kno_decref_vec(data,n);
        u8_free(data);
        return err;}}
    u8_free(data);
    return kno_stream2string(&out);}
  else return kno_type_error(_("sequence"),"x2string",seq);
}

KNO_EXPORT lispval kno_seq2choice(lispval x)
{
  if (CHOICEP(x)) {
    lispval results = EMPTY;
    DO_CHOICES(e,x) {
      lispval subelts = kno_seq2choice(e);
      CHOICE_ADD(results,subelts);}
    return results;}
  else {
    lispval result = EMPTY;
    int ctype = KNO_LISP_TYPE(x), i = 0, len;
    switch (ctype) {
    case kno_vector_type:
      len = VEC_LEN(x); while (i < len) {
        lispval elt = kno_incref(VEC_REF(x,i));
        CHOICE_ADD(result,elt); i++;}
      return result;
    case kno_packet_type: case kno_secret_type:
      len = KNO_PACKET_LENGTH(x); while (i < len) {
        lispval elt = KNO_BYTE2LISP(KNO_PACKET_REF(x,i));
        CHOICE_ADD(result,elt); i++;}
      return result;
    case kno_pair_type: {
      lispval scan = x;
      while (PAIRP(scan)) {
        lispval elt = kno_incref(KNO_CAR(scan));
        CHOICE_ADD(result,elt);
        scan = KNO_CDR(scan);}
      if (!(NILP(scan))) {
        lispval tail = kno_incref(scan);
        CHOICE_ADD(result,tail);}
      return result;}
    case kno_string_type: {
      const u8_byte *scan = CSTRING(x);
      int c = u8_sgetc(&scan);
      while (c>=0) {
        CHOICE_ADD(result,KNO_CODE2CHAR(c));
        c = u8_sgetc(&scan);}
      return result;}
    default:
      len = kno_seq_length(x); while (i<len) {
        lispval elt = kno_seq_elt(x,i);
        CHOICE_ADD(result,elt); i++;}
      return result;
    }}
}

KNO_DCLPRIM3("elts",elts_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
 "`(ELTS *arg0* [*arg1*] [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval elts_prim(lispval x,lispval start_arg,lispval end_arg)
{
  int start, end;
  lispval check = check_range("elts_prim",x,start_arg,end_arg,&start,&end);
  lispval results = EMPTY;
  int ctype = KNO_LISP_TYPE(x);
  if (KNO_ABORTED(check))
    return check;
  else switch (ctype) {
    case kno_vector_type: {
      lispval *read, *limit;
      read = VEC_DATA(x)+start; limit = VEC_DATA(x)+end;
      while (read<limit) {
        lispval v = *read++; kno_incref(v);
        CHOICE_ADD(results,v);}
      return results;}
    case kno_compound_type: {
      struct KNO_COMPOUND *compound = (kno_compound) x;
      int off = compound->compound_off;
      if (off < 0)
        return kno_err("NotACompoundSequence","elts_prim",NULL,x);
      lispval *scan = (&(compound->compound_0))+off+start;
      lispval *limit = scan+(end-start);
      while (scan<limit) {
        lispval v = kno_incref(*scan);
        CHOICE_ADD(results,v);
        scan++;}
      return results;}
    case kno_secret_type:
      return kno_err(kno_SecretData,"position_prim",NULL,x);
    case kno_packet_type: {
      const unsigned char *read = KNO_PACKET_DATA(x), *lim = read+end;
      while (read<lim) {
        unsigned char v = *read++;
        CHOICE_ADD(results,KNO_SHORT2FIX(v));}
      return results;}
    case kno_numeric_vector_type: {
      int len = KNO_NUMVEC_LENGTH(x);
      int i = start, lim = ((end<0)?(len+end):(end));
      if (lim>len) lim = len;
      switch (KNO_NUMVEC_TYPE(x)) {
      case kno_short_elt: {
        kno_short *vec = KNO_NUMVEC_SHORTS(x);
        while (i<lim) {
          kno_short num = vec[i++];
          lispval elt = KNO_SHORT2FIX(num);
          CHOICE_ADD(results,elt);}}
      case kno_int_elt: {
        kno_int *vec = KNO_NUMVEC_INTS(x);
        while (i<lim) {
          kno_int num = vec[i++];
          lispval elt = KNO_INT(num);
          CHOICE_ADD(results,elt);}
        break;}
      case kno_long_elt: {
        kno_long *vec = KNO_NUMVEC_LONGS(x);
        while (i<lim) {
          kno_long num = vec[i++];
          lispval elt = KNO_INT(num);
          CHOICE_ADD(results,elt);}
        break;}
      case kno_float_elt: {
        kno_float *vec = KNO_NUMVEC_FLOATS(x);
        while (i<lim) {
          kno_float num = vec[i++];
          lispval elt = kno_make_flonum(num);
          CHOICE_ADD(results,elt);}
        break;}
      case kno_double_elt: {
        kno_double *vec = KNO_NUMVEC_DOUBLES(x);
        while (i<lim) {
          kno_double num = vec[i++];
          lispval elt = kno_make_flonum(num);
          CHOICE_ADD(results,elt);}
        break;}
      default: {
        kno_seterr(_("Corrputed numerical vector"),"kno_seq2choice",NULL,x);
        kno_incref(x);
        kno_decref(results);
        results = KNO_ERROR;}}
      break;}
    case kno_pair_type: {
      int j = 0; lispval scan = x;
      while (PAIRP(scan))
        if (j == end)
          return results;
        else if (j>=start) {
          lispval car = KNO_CAR(scan);
          kno_incref(car);
          CHOICE_ADD(results,car);
          scan = KNO_CDR(scan);
          j++;}
        else {j++; scan = KNO_CDR(scan);}
      return results;}
    case kno_string_type: {
      int count = 0, c;
      const u8_byte *scan = CSTRING(x);
      while ((c = u8_sgetc(&scan))>=0)
        if (count<start) count++;
        else if (count>=end) break;
        else {CHOICE_ADD(results,KNO_CODE2CHAR(c));}
      return results;}
    default:
      if (NILP(x))
        if ((start == end) && (start == 0))
          return EMPTY;
        else return KNO_RANGE_ERROR;
      else if ((kno_seqfns[ctype]) && (kno_seqfns[ctype]->elt)) {
        int scan = start; while (scan<end) {
          lispval elt = (kno_seqfns[ctype]->elt)(x,scan);
          CHOICE_ADD(results,elt);
          scan++;}
        return results;}
      else if (kno_seqfns[ctype]==NULL)
        return kno_type_error(_("sequence"),"elts_prim",x);
      else return kno_err(kno_NoMethod,"elts_prim",NULL,x);}
  return results;
}

KNO_DCLPRIM1("vector->elts",vec2elts_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(VECTOR->ELTS *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval vec2elts_prim(lispval x)
{
  if (VECTORP(x)) {
    lispval result = EMPTY;
    int i = 0; int len = VEC_LEN(x); lispval *elts = KNO_VECTOR_ELTS(x);
    while (i<len) {
      lispval e = elts[i++]; kno_incref(e); CHOICE_ADD(result,e);}
    return result;}
  else return kno_incref(x);
}

/* Vector length predicates */

KNO_DCLPRIM2("veclen<?",veclen_lt_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(VECLEN<? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval veclen_lt_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return KNO_FALSE;
  else if (VEC_LEN(x)<FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("veclen<=?",veclen_lte_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(VECLEN<=? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval veclen_lte_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return KNO_FALSE;
  else if (VEC_LEN(x)<=FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("veclen>?",veclen_gt_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(VECLEN>? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval veclen_gt_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return KNO_FALSE;
  else if (VEC_LEN(x)>FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("veclen>=?",veclen_gte_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(VECLEN>=? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval veclen_gte_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return KNO_FALSE;
  else if (VEC_LEN(x)>=FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("veclen=?",veclen_eq_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(VECLEN=? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval veclen_eq_prim(lispval x,lispval len)
{
  if (!(VECTORP(x)))
    return KNO_FALSE;
  else if (VEC_LEN(x) == FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Sequence length predicates */

KNO_DCLPRIM2("seqlen<?",seqlen_lt_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(SEQLEN<? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval seqlen_lt_prim(lispval x,lispval len)
{
  if (!(KNO_SEQUENCEP(x)))
    return KNO_FALSE;
  else if (kno_seq_length(x)<FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("seqlen<=?",seqlen_lte_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(SEQLEN<=? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval seqlen_lte_prim(lispval x,lispval len)
{
  if (!(KNO_SEQUENCEP(x)))
    return KNO_FALSE;
  else if (kno_seq_length(x)<=FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("seqlen>?",seqlen_gt_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(SEQLEN>? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval seqlen_gt_prim(lispval x,lispval len)
{
  if (!(KNO_SEQUENCEP(x)))
    return KNO_FALSE;
  else if (kno_seq_length(x)>FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("seqlen>=?",seqlen_gte_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(SEQLEN>=? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval seqlen_gte_prim(lispval x,lispval len)
{
  if (!(KNO_SEQUENCEP(x)))
    return KNO_FALSE;
  else if (kno_seq_length(x)>=FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DCLPRIM2("seqlen=?",seqlen_eq_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(SEQLEN=? *arg0* *arg1*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval seqlen_eq_prim(lispval x,lispval len)
{
  if (!(KNO_SEQUENCEP(x)))
    return KNO_FALSE;
  else if (kno_seq_length(x) == FIX2INT(len))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Matching vectors */

KNO_DCLPRIM3("match?",seqmatch_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
 "`(MATCH? *arg0* *arg1* [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_fixnum_type,KNO_INT(0));
static lispval seqmatch_prim(lispval prefix,lispval seq,lispval startarg)
{
  if (!(KNO_UINTP(startarg)))
    return kno_type_error("uint","seqmatchprim",startarg);
  int start = FIX2INT(startarg);
  if ((VECTORP(prefix))&&(VECTORP(seq))) {
    int plen = VEC_LEN(prefix);
    int seqlen = VEC_LEN(seq);
    if ((start+plen)<=seqlen) {
      int j = 0; int i = start;
      while (j<plen)
        if (KNO_EQUAL(VEC_REF(prefix,j),VEC_REF(seq,i))) {
          j++; i++;}
        else return KNO_FALSE;
      return KNO_TRUE;}
    else return KNO_FALSE;}
  else if ((STRINGP(prefix))&&(STRINGP(seq))) {
    int plen = STRLEN(prefix);
    int seqlen = STRLEN(seq);
    int off = u8_byteoffset(CSTRING(seq),start,seqlen);
    if ((off+plen)<=seqlen)
      if (strncmp(CSTRING(prefix),CSTRING(seq)+off,plen)==0)
        return KNO_TRUE;
      else return KNO_FALSE;
    else return KNO_FALSE;}
  else if (!(KNO_SEQUENCEP(prefix)))
    return kno_type_error("sequence","seqmatch_prim",prefix);
  else if (!(KNO_SEQUENCEP(seq)))
    return kno_type_error("sequence","seqmatch_prim",seq);
  else {
    int plen = kno_seq_length(prefix);
    int seqlen = kno_seq_length(seq);
    if ((start+plen)<=seqlen) {
      int j = 0; int i = start;
      while (j<plen) {
        lispval pelt = kno_seq_elt(prefix,j);
        lispval velt = kno_seq_elt(prefix,i);
        int cmp = KNO_EQUAL(pelt,velt);
        kno_decref(pelt); kno_decref(velt);
        if (cmp) {j++; i++;}
        else return KNO_FALSE;}
      return KNO_TRUE;}
    else return KNO_FALSE;}
}

/* Sorting vectors */

static lispval sortvec_primfn(lispval vec,lispval keyfn,int reverse,int lexsort)
{
  if (VEC_LEN(vec)==0)
    return kno_empty_vector(0);
  else if (VEC_LEN(vec)==1)
    return kno_incref(vec);
  else {
    int i = 0, n = VEC_LEN(vec), j = 0;
    lispval result = kno_empty_vector(n);
    lispval *vecdata = KNO_VECTOR_ELTS(result);
    struct KNO_SORT_ENTRY *sentries = u8_alloc_n(n,struct KNO_SORT_ENTRY);
    while (i<n) {
      lispval elt = VEC_REF(vec,i);
      lispval value=_kno_apply_keyfn(elt,keyfn);
      if (KNO_ABORTED(value)) {
        int j = 0; while (j<i) {kno_decref(sentries[j].sortval); j++;}
        u8_free(sentries); u8_free(vecdata);
        return value;}
      sentries[i].sortval = elt;
      sentries[i].sortkey = value;
      i++;}
    if (lexsort)
      qsort(sentries,n,sizeof(struct KNO_SORT_ENTRY),_kno_lexsort_helper);
    else qsort(sentries,n,sizeof(struct KNO_SORT_ENTRY),_kno_sort_helper);
    i = 0; j = n-1; if (reverse) while (i < n) {
      kno_decref(sentries[i].sortkey);
      vecdata[j]=kno_incref(sentries[i].sortval);
      i++; j--;}
    else while (i < n) {
      kno_decref(sentries[i].sortkey);
      vecdata[i]=kno_incref(sentries[i].sortval);
      i++;}
    u8_free(sentries);
    return result;}
}

KNO_DCLPRIM2("sortvec",sortvec_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(SORTVEC *arg0* [*arg1*])` **undocumented**",
 kno_vector_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval sortvec_prim(lispval vec,lispval keyfn)
{
  return sortvec_primfn(vec,keyfn,0,0);
}

KNO_DCLPRIM2("lexsortvec",lexsortvec_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(LEXSORTVEC *arg0* [*arg1*])` **undocumented**",
 kno_vector_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval lexsortvec_prim(lispval vec,lispval keyfn)
{
  return sortvec_primfn(vec,keyfn,0,1);
}

KNO_DCLPRIM2("rsortvec",rsortvec_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(RSORTVEC *arg0* [*arg1*])` **undocumented**",
 kno_vector_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval rsortvec_prim(lispval vec,lispval keyfn)
{
  return sortvec_primfn(vec,keyfn,1,0);
}

/* RECONS reconstitutes CONSes, returning the original if
   nothing has changed. This is handy for some recursive list
   functions. */

KNO_DCLPRIM3("recons",recons_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
 "`(RECONS *arg0* *arg1* *arg2*)` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_pair_type,KNO_VOID);
static lispval recons_prim(lispval car,lispval cdr,lispval orig)
{
  if ((KNO_EQ(car,KNO_CAR(orig)))&&(KNO_EQ(cdr,KNO_CDR(orig))))
    return kno_incref(orig);
  else {
    lispval cons = kno_conspair(car,cdr);
    kno_incref(car); kno_incref(cdr);
    return cons;}
}

/* Numeric vectors */

KNO_DCLPRIM("shortvec",make_short_vector,KNO_VAR_ARGS|KNO_MIN_ARGS(0),
 "`(SHORTVEC *args...*)` **undocumented**");
static lispval make_short_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_short_elt);
  kno_short *elts = KNO_NUMVEC_SHORTS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (KNO_SHORTP(elt))
      elts[i++]=(kno_short)(FIX2INT(elt));
    else {
      u8_free((struct KNO_CONS *)vec);
      return kno_type_error(_("short element"),"make_short_vector",elt);}}
  return vec;
}

KNO_DCLPRIM1("->shortvec",seq2shortvec,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(->SHORTVEC *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seq2shortvec(lispval arg)
{
  if ((TYPEP(arg,kno_numeric_vector_type))&&
      (KNO_NUMVEC_TYPE(arg) == kno_short_elt)) {
    kno_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_short_vector(VEC_LEN(arg),KNO_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return kno_make_short_vector(0,NULL);
  else if (KNO_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = kno_seq_elts(arg,&n);
    if (n < 0) {
      kno_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return KNO_ERROR;}
    lispval result = make_short_vector(n,data);
    u8_free(data);
    return result;}
  else return kno_type_error(_("sequence"),"seq2shortvec",arg);
}

KNO_DCLPRIM("intvec",make_int_vector,KNO_VAR_ARGS|KNO_MIN_ARGS(0),
 "`(INTVEC *args...*)` **undocumented**");
static lispval make_int_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_int_elt);
  kno_int *elts = KNO_NUMVEC_INTS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (KNO_INTP(elt))
      elts[i++]=((kno_int)(FIX2INT(elt)));
    else if ((KNO_BIGINTP(elt))&&
             (kno_bigint_fits_in_word_p((kno_bigint)elt,sizeof(kno_int),1)))
      elts[i++]=kno_bigint_to_long((kno_bigint)elt);
    else {
      u8_free((struct KNO_CONS *)vec);
      return kno_type_error(_("int element"),"make_int_vector",elt);}}
  return vec;
}

KNO_DCLPRIM1("->intvec",seq2intvec,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(->INTVEC *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seq2intvec(lispval arg)
{
  if ((TYPEP(arg,kno_numeric_vector_type))&&
      (KNO_NUMVEC_TYPE(arg) == kno_int_elt)) {
    kno_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_int_vector(VEC_LEN(arg),KNO_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return kno_make_int_vector(0,NULL);
  else if (KNO_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = kno_seq_elts(arg,&n);
    if (KNO_EXPECT_FALSE(n < 0)) {
      kno_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return KNO_ERROR;}
    lispval result = make_int_vector(n,data);
    u8_free(data);
    return result;}
  else return kno_type_error(_("sequence"),"seq2intvec",arg);
}

KNO_DCLPRIM("longvec",make_long_vector,KNO_VAR_ARGS|KNO_MIN_ARGS(0),
 "`(LONGVEC *args...*)` **undocumented**");
static lispval make_long_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_long_elt);
  kno_long *elts = KNO_NUMVEC_LONGS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (FIXNUMP(elt))
      elts[i++]=((kno_long)(FIX2INT(elt)));
    else if (KNO_BIGINTP(elt))
      elts[i++]=kno_bigint_to_long_long((kno_bigint)elt);
    else {
      u8_free((struct KNO_CONS *)vec);
      return kno_type_error(_("flonum element"),"make_long_vector",elt);}}
  return vec;
}

KNO_DCLPRIM1("->longvec",seq2longvec,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(->LONGVEC *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seq2longvec(lispval arg)
{
  if ((TYPEP(arg,kno_numeric_vector_type))&&
      (KNO_NUMVEC_TYPE(arg) == kno_long_elt)) {
    kno_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_long_vector(VEC_LEN(arg),KNO_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return kno_make_long_vector(0,NULL);
  else if (KNO_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = kno_seq_elts(arg,&n);
    if (KNO_EXPECT_TRUE(n < 0)) {
      kno_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return KNO_ERROR;}
    lispval result = make_long_vector(n,data);
    u8_free(data);
    return result;}
  else return kno_type_error(_("sequence"),"seq2longvec",arg);
}

KNO_DCLPRIM("floatvec",make_float_vector,KNO_VAR_ARGS|KNO_MIN_ARGS(0),
 "`(FLOATVEC *args...*)` **undocumented**");
static lispval make_float_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_float_elt);
  float *elts = KNO_NUMVEC_FLOATS(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (KNO_FLONUMP(elt))
      elts[i++]=KNO_FLONUM(elt);
    else if (NUMBERP(elt))
      elts[i++]=kno_todouble(elt);
    else {
      u8_free((struct KNO_CONS *)vec);
      return kno_type_error(_("float element"),"make_float_vector",elt);}}
  return vec;
}

KNO_DCLPRIM1("->floatvec",seq2floatvec,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(->FLOATVEC *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seq2floatvec(lispval arg)
{
  if ((TYPEP(arg,kno_numeric_vector_type))&&
      (KNO_NUMVEC_TYPE(arg) == kno_float_elt)) {
    kno_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_float_vector(VEC_LEN(arg),KNO_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return kno_make_float_vector(0,NULL);
  else if (KNO_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = kno_seq_elts(arg,&n);
    if (KNO_EXPECT_TRUE(n < 0)) {
      kno_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return KNO_ERROR;}
    lispval result = make_float_vector(n,data);
    u8_free(data);
    return result;}
  else return kno_type_error(_("sequence"),"seq2floatvec",arg);
}

KNO_DCLPRIM("doublevec",make_double_vector,KNO_VAR_ARGS|KNO_MIN_ARGS(0),
 "`(DOUBLEVEC *args...*)` **undocumented**");
static lispval make_double_vector(int n,lispval *from_elts)
{
  int i = 0; lispval vec = kno_make_numeric_vector(n,kno_double_elt);
  double *elts = KNO_NUMVEC_DOUBLES(vec);
  while (i<n) {
    lispval elt = from_elts[i];
    if (KNO_FLONUMP(elt))
      elts[i++]=KNO_FLONUM(elt);
    else if (NUMBERP(elt))
      elts[i++]=kno_todouble(elt);
    else {
      u8_free((struct KNO_CONS *)vec);
      return kno_type_error(_("double(float) element"),"make_double_vector",elt);}}
  return vec;
}

KNO_DCLPRIM1("->doublevec",seq2doublevec,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(->DOUBLEVEC *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval seq2doublevec(lispval arg)
{
  if ((TYPEP(arg,kno_numeric_vector_type))&&
      (KNO_NUMVEC_TYPE(arg) == kno_double_elt)) {
    kno_incref(arg);
    return arg;}
  else if (VECTORP(arg))
    return make_double_vector(VEC_LEN(arg),KNO_VECTOR_ELTS(arg));
  else if (NILP(arg))
    return kno_make_double_vector(0,NULL);
  else if (KNO_SEQUENCEP(arg)) {
    int n = -1;
    lispval *data = kno_seq_elts(arg,&n);
    if (KNO_EXPECT_TRUE(n < 0)) {
      kno_seterr("NonEnumerableSequence","seq2vector",NULL,arg);
      return KNO_ERROR;}
    lispval result = make_double_vector(n,data);
    u8_free(data);
    return result;}
  else return kno_type_error(_("sequence"),"seq2doublevec",arg);
}

/* side effecting operations (not threadsafe) */

KNO_DCLPRIM2("set-car!",set_car,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(SET-CAR! *arg0* *arg1*)` **undocumented**",
 kno_pair_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval set_car(lispval pair,lispval val)
{
  struct KNO_PAIR *p = kno_consptr(struct KNO_PAIR *,pair,kno_pair_type);
  kno_incref(val);
  KNO_LOCK_PTR(p);
  lispval oldv = p->car; p->car = val;
  KNO_UNLOCK_PTR(p);
  kno_decref(oldv);
  return VOID;
}

KNO_DCLPRIM2("set-cdr!",set_cdr,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
 "`(SET-CDR! *arg0* *arg1*)` **undocumented**",
 kno_pair_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval set_cdr(lispval pair,lispval val)
{
  struct KNO_PAIR *p = kno_consptr(struct KNO_PAIR *,pair,kno_pair_type);
  kno_incref(val); 
  KNO_LOCK_PTR(p);
  lispval oldv = p->cdr; p->cdr = val;
  KNO_UNLOCK_PTR(p);
  kno_decref(oldv);
  return VOID;
}

KNO_DCLPRIM3("vector-set!",vector_set,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
 "`(VECTOR-SET! *arg0* *arg1* *arg2*)` **undocumented**",
 kno_vector_type,KNO_VOID,kno_fixnum_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval vector_set(lispval vec,lispval index,lispval val)
{
  struct KNO_VECTOR *v = kno_consptr(struct KNO_VECTOR *,vec,kno_vector_type);
  if (!(KNO_UINTP(index)))
    return kno_type_error("uint","vector_set",index);
  int offset = FIX2INT(index);
  if (offset>v->vec_length) {
    char buf[32];
    return kno_err(kno_RangeError,"vector_set",
                  u8_uitoa10(offset,buf),
                  vec);}
  else {
    kno_incref(val);
    KNO_LOCK_PTR(v);
    lispval *elts = v->vec_elts, oldv = elts[offset]; 
    elts[offset] = val;
    KNO_UNLOCK_PTR(v);
    kno_decref(oldv);
    return VOID;}
}

KNO_EXPORT void kno_init_seqprims_c()
{
  u8_register_source_file(_FILEINFO);
  kno_idefn(kno_scheme_module,kno_make_cprim1("REVERSE",kno_reverse,1));
  kno_idefn(kno_scheme_module,kno_make_cprimn("APPEND",kno_append,0));
  init_local_cprims();
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/


static void init_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("vector-set!",vector_set,3,kno_scheme_module);
  KNO_LINK_PRIM("set-cdr!",set_cdr,2,kno_scheme_module);
  KNO_LINK_PRIM("set-car!",set_car,2,kno_scheme_module);
  KNO_LINK_PRIM("->doublevec",seq2doublevec,1,kno_scheme_module);
  KNO_LINK_VARARGS("doublevec",make_double_vector,kno_scheme_module);
  KNO_LINK_PRIM("->floatvec",seq2floatvec,1,kno_scheme_module);
  KNO_LINK_VARARGS("floatvec",make_float_vector,kno_scheme_module);
  KNO_LINK_PRIM("->longvec",seq2longvec,1,kno_scheme_module);
  KNO_LINK_VARARGS("longvec",make_long_vector,kno_scheme_module);
  KNO_LINK_PRIM("->intvec",seq2intvec,1,kno_scheme_module);
  KNO_LINK_VARARGS("intvec",make_int_vector,kno_scheme_module);
  KNO_LINK_PRIM("->shortvec",seq2shortvec,1,kno_scheme_module);
  KNO_LINK_VARARGS("shortvec",make_short_vector,kno_scheme_module);
  KNO_LINK_PRIM("recons",recons_prim,3,kno_scheme_module);
  KNO_LINK_PRIM("rsortvec",rsortvec_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("lexsortvec",lexsortvec_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("sortvec",sortvec_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("match?",seqmatch_prim,3,kno_scheme_module);
  KNO_LINK_PRIM("seqlen=?",seqlen_eq_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("seqlen>=?",seqlen_gte_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("seqlen>?",seqlen_gt_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("seqlen<=?",seqlen_lte_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("seqlen<?",seqlen_lt_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("veclen=?",veclen_eq_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("veclen>=?",veclen_gte_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("veclen>?",veclen_gt_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("veclen<=?",veclen_lte_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("veclen<?",veclen_lt_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("vector->elts",vec2elts_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("elts",elts_prim,3,kno_scheme_module);
  KNO_LINK_PRIM("->string",x2string,1,kno_scheme_module);
  KNO_LINK_PRIM("->packet",seq2packet,1,kno_scheme_module);
  KNO_LINK_PRIM("->list",seq2list,1,kno_scheme_module);
  KNO_LINK_VARARGS("1vector",onevector_prim,kno_scheme_module);
  KNO_LINK_PRIM("->vector",seq2vector,1,kno_scheme_module);
  KNO_LINK_PRIM("make-vector",make_vector,2,kno_scheme_module);
  KNO_LINK_VARARGS("vector",vector,kno_scheme_module);
  KNO_LINK_VARARGS("list",list,kno_scheme_module);
  KNO_LINK_PRIM("member",member_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("memv",memv_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("memq",memq_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("assoc",assoc_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("assv",assv_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("assq",assq_prim,2,kno_scheme_module);
  KNO_LINK_VARARGS("cons*",cons_star,kno_scheme_module);
  KNO_LINK_PRIM("cdddr",cdddr,1,kno_scheme_module);
  KNO_LINK_PRIM("caddr",caddr,1,kno_scheme_module);
  KNO_LINK_PRIM("caar",caar,1,kno_scheme_module);
  KNO_LINK_PRIM("cdar",cdar,1,kno_scheme_module);
  KNO_LINK_PRIM("cadr",cadr,1,kno_scheme_module);
  KNO_LINK_PRIM("cddr",cddr,1,kno_scheme_module);
  KNO_LINK_PRIM("cdr",cdr,1,kno_scheme_module);
  KNO_LINK_PRIM("car",car,1,kno_scheme_module);
  KNO_LINK_PRIM("cons",cons,2,kno_scheme_module);
  KNO_LINK_PRIM("empty-list?",nullp,1,kno_scheme_module);
  KNO_LINK_ALIAS("null?",nullp,kno_scheme_module);
  KNO_LINK_ALIAS("nil?",nullp,kno_scheme_module);
  KNO_LINK_PRIM("seventh",seventh,1,kno_scheme_module);
  KNO_LINK_PRIM("sixth",sixth,1,kno_scheme_module);
  KNO_LINK_PRIM("fifth",fifth,1,kno_scheme_module);
  KNO_LINK_PRIM("fourth",fourth,1,kno_scheme_module);
  KNO_LINK_PRIM("third",third,1,kno_scheme_module);
  KNO_LINK_PRIM("second",second,1,kno_scheme_module);
  KNO_LINK_PRIM("rest",rest,1,kno_scheme_module);
  KNO_LINK_PRIM("first",first,1,kno_scheme_module);
  KNO_LINK_PRIM("find-if-not",find_if_not_prim,5,kno_scheme_module);
  KNO_LINK_PRIM("find-if",find_if_prim,5,kno_scheme_module);
  KNO_LINK_PRIM("position-if-not",position_if_not_prim,4,kno_scheme_module);
  KNO_LINK_PRIM("position-if",position_if_prim,4,kno_scheme_module);
  KNO_LINK_PRIM("remove-if",removeif_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("remove-if-not",removeifnot_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("some?",some_prim,4,kno_scheme_module);
  KNO_LINK_PRIM("every?",every_prim,4,kno_scheme_module);
  KNO_LINK_PRIM("search",search_prim,4,kno_scheme_module);
  KNO_LINK_PRIM("find",find_prim,4,kno_scheme_module);
  KNO_LINK_PRIM("rposition",rposition_prim,4,kno_scheme_module);
  KNO_LINK_PRIM("position",position_prim,4,kno_scheme_module);
  KNO_LINK_PRIM("slice",slice_prim,3,kno_scheme_module);
  KNO_LINK_ALIAS("subseq",slice_prim,kno_scheme_module);
  KNO_LINK_PRIM("length>1",has_length_gt_one_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("length>0",has_length_gt_zero_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("length>=",has_length_gte_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("length>",has_length_gt_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("length=<",has_length_lte_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("length<",has_length_lt_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("length=",has_length_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("elt",seqelt_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("length",seqlen_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("sequence?",sequencep_prim,1,kno_scheme_module);

  KNO_LINK_PRIM("map->choice",kno_map2choice,2,kno_scheme_module);
  KNO_LINK_PRIM("remove",kno_remove,2,kno_scheme_module);
  KNO_LINK_PRIM("reduce",kno_reduce,3,kno_scheme_module);

  KNO_LINK_VARARGS("map",mapseq_prim,kno_scheme_module);
  KNO_LINK_VARARGS("for-each",foreach_prim,kno_scheme_module);
  KNO_LINK_VARARGS("longvec",make_long_vector,kno_scheme_module);
  KNO_LINK_VARARGS("intvec",make_int_vector,kno_scheme_module);
  KNO_LINK_VARARGS("shortvec",make_short_vector,kno_scheme_module);
  KNO_LINK_VARARGS("doublevec",make_double_vector,kno_scheme_module);
  KNO_LINK_VARARGS("floatvec",make_float_vector,kno_scheme_module);
  KNO_LINK_ALIAS("length=>",has_length_gte_prim,scheme_module);
  KNO_LINK_ALIAS("length<=",has_length_lte_prim,scheme_module);
  KNO_LINK_ALIAS("subseq",slice_prim,scheme_module);
  KNO_LINK_ALIAS("null?",nullp,scheme_module);
  KNO_LINK_ALIAS("nil?",nullp,scheme_module);
  KNO_LINK_ALIAS("onevector",onevector_prim,scheme_module);
}
