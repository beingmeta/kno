/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1
#define FD_INLINE_TABLES 1
#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/support.h"
#include "framerd/storage.h"
#include "framerd/eval.h"
#include "framerd/dtproc.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/ports.h"
#include "framerd/dtcall.h"

#include <libu8/u8timefns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

/* QUASIQUOTE */

static lispval quasiquote, unquote, unquotestar, quote_symbol;
FD_EXPORT lispval fd_quasiquote(lispval obj,fd_lexenv env,int level);

#define FD_BAD_UNQUOTEP(elt) \
  (((FD_EQ(FD_CAR(elt),unquote)) || \
    (FD_EQ(FD_CAR(elt),unquotestar))) && \
   (!((PAIRP(FD_CDR(elt))) &&                 \
      (NILP(FD_CDR(FD_CDR(elt)))))))

static lispval quasiquote_list(lispval obj,fd_lexenv env,int level)
{
  lispval head = NIL, *tail = &head;
  while (PAIRP(obj)) {
    lispval elt = FD_CAR(obj), new_elt, new_tail;
    struct FD_PAIR *tailcons;
    if (ATOMICP(elt)) {
      /* This handles the case of a dotted unquote. */
      if (FD_EQ(elt,unquote)) {
        if ((PAIRP(FD_CDR(obj))) &&
            (NILP(FD_CDR(FD_CDR(obj)))))
          if (level==1) {
            lispval splice_at_end = fd_eval(FD_CADR(obj),env);
            if (FD_ABORTED(splice_at_end)) {
              fd_decref(head);
              return splice_at_end;}
            else if (VOIDP(splice_at_end)) {
              fd_seterr(fd_VoidArgument,"quasiquote_list",NULL,FD_CADR(obj));
              fd_decref(head);
              return FD_ERROR;}
            else {
              if (PRECHOICEP(splice_at_end))
                splice_at_end=fd_simplify_choice(splice_at_end);
              *tail = splice_at_end;
              return head;}}
          else {
            lispval splice_at_end = fd_quasiquote(FD_CADR(obj),env,level-1);
            if (FD_ABORTED(splice_at_end)) {
              fd_decref(head);
              return splice_at_end;}
            else {
              lispval with_unquote = fd_conspair(unquote,splice_at_end);
              *tail = with_unquote;
              return head;}}
        else {
          fd_decref(head);
          return fd_err(fd_SyntaxError,"malformed UNQUOTE",NULL,obj);}}
      else new_elt = elt;}
    else if (PAIRP(elt))
      if (FD_BAD_UNQUOTEP(elt)) {
        fd_decref(head);
        return fd_err(fd_SyntaxError,"malformed UNQUOTE",NULL,elt);}
      else if (FD_EQ(FD_CAR(elt),unquote)) {
        if (level==1) {
          new_elt = fd_eval(FD_CADR(elt),env);
          if (VOIDP(new_elt))
            new_elt = fd_err(fd_VoidArgument,"quasiquote_list",
                             NULL,FD_CADR(elt));
          else if (PRECHOICEP(new_elt))
            new_elt=fd_simplify_choice(new_elt);}
        else {
          lispval embed = fd_quasiquote(FD_CADR(elt),env,level-1);
          if (FD_ABORTED(embed)) new_elt = embed;
          else new_elt = fd_make_list(2,unquote,embed);}
        if (FD_ABORTED(new_elt)) {
          fd_decref(head);
          return new_elt;}}
      else if (FD_EQ(FD_CAR(elt),unquotestar))
        if (level==1) {
          lispval insertion = fd_eval(FD_CADR(elt),env);
          if (PRECHOICEP(insertion))
            insertion=fd_simplify_choice(insertion);
          if (FD_ABORTED(insertion)) {
              fd_decref(head);
              return insertion;}
          else if (VOIDP(insertion)) {
            fd_seterr(fd_VoidArgument,"quasiquote_list",NULL,FD_CADR(elt));
            fd_decref(head);
            return FD_ERROR;}
          else if (NILP(insertion)) {}
          else if (PAIRP(insertion)) {
            lispval scan = insertion, last = VOID;
            while (PAIRP(scan)) {last = scan; scan = FD_CDR(scan);}
            if (!(PAIRP(last))) {
              u8_string details_string = u8_mkstring("RESULT=%q",elt);
              lispval err;
              err = fd_err(fd_SyntaxError,
                         "splicing UNQUOTE for an improper list",
                         details_string,insertion);
              fd_decref(head); u8_free(details_string);
              fd_decref(insertion);
              return err;}
            else {
              FD_DOLIST(insert_elt,insertion) {
                lispval new_pair = fd_conspair(insert_elt,NIL);
                *tail = new_pair; tail = &(FD_CDR(new_pair));
                fd_incref(insert_elt);}}}
          else if (VECTORP(insertion)) {
            int i = 0, lim = VEC_LEN(insertion);
            while (i<lim) {
              lispval insert_elt = VEC_REF(insertion,i);
              lispval new_pair = fd_conspair(insert_elt,NIL);
              *tail = new_pair; tail = &(FD_CDR(new_pair));
              fd_incref(insert_elt);
              i++;}}
          else {
            u8_string details_string =
              u8_mkstring("RESULT=%q=%q",elt,insertion);
            lispval err;
            err = fd_err(fd_SyntaxError,
                       "splicing UNQUOTE used with a non-squence",
                       details_string,insertion);
            fd_decref(head); u8_free(details_string);
            fd_decref(insertion);
            return err;}
          obj = FD_CDR(obj);
          fd_decref(insertion);
          continue;}
        else {
          lispval embed = fd_quasiquote(FD_CADR(elt),env,level-1);
          if (FD_ABORTED(embed)) new_elt = embed;
          else new_elt = fd_make_list(2,unquotestar,embed);}
      else new_elt = fd_quasiquote(elt,env,level);
    else new_elt = fd_quasiquote(elt,env,level);
    if (FD_ABORTED(new_elt)) {
      fd_decref(head); return new_elt;}
    new_tail = fd_conspair(new_elt,NIL);
    tailcons = FD_CONSPTR(fd_pair,new_tail);
    *tail = new_tail; tail = &(tailcons->cdr);
    obj = FD_CDR(obj);}
  if (!(NILP(obj))) {
    lispval final = fd_quasiquote(obj,env,level);
    if (FD_ABORTED(final)) {
      fd_decref(head);
      return final;}
    else *tail = final;}
  return head;
}

static lispval quasiquote_vector(lispval obj,fd_lexenv env,int level)
{
  lispval result = VOID;
  int i = 0, j = 0, len = VEC_LEN(obj), newlen = len;
  if (len==0) return fd_incref(obj);
  else {
    lispval *newelts = u8_alloc_n(len,lispval);
    while (i < len) {
      lispval elt = VEC_REF(obj,i);
      if ((PAIRP(elt)) &&
          (FD_EQ(FD_CAR(elt),unquotestar)) &&
          (PAIRP(FD_CDR(elt))))
        if (level==1) {
          lispval insertion = fd_eval(FD_CADR(elt),env); int addlen = 0;
          if (PRECHOICEP(insertion))
            insertion=fd_simplify_choice(insertion);
          if (FD_ABORTED(insertion)) {
            int k = 0; while (k<j) {fd_decref(newelts[k]); k++;}
            u8_free(newelts);
            return insertion;}
          else if (VOIDP(insertion)) {
            fd_seterr(fd_VoidArgument,"quasiquote_vector",NULL,FD_CADR(elt));
            int k = 0; while (k<j) {fd_decref(newelts[k]); k++;}
            u8_free(newelts);
            return FD_ERROR;}
          if (NILP(insertion)) {}
          else if (PAIRP(insertion)) {
            lispval scan = insertion; while (PAIRP(scan)) {
              scan = FD_CDR(scan); addlen++;}
            if (!(NILP(scan)))
              return fd_err(fd_SyntaxError,
                            "splicing UNQUOTE for an improper list",
                            NULL,insertion);}
          else if (VECTORP(insertion)) addlen = VEC_LEN(insertion);
          else return fd_err(fd_SyntaxError,
                             "splicing UNQUOTE for an improper list",
                             NULL,insertion);
          if (addlen==0) {
            i++; fd_decref(insertion); continue;}
          newelts = u8_realloc_n(newelts,newlen+addlen,lispval);
          newlen = newlen+addlen;
          if (PAIRP(insertion)) {
            lispval scan = insertion; while (PAIRP(scan)) {
              lispval ielt = FD_CAR(scan); newelts[j++]=ielt;
              fd_incref(ielt); scan = FD_CDR(scan);}
            i++;}
          else if (VECTORP(insertion)) {
            int k = 0; while (k<addlen) {
              lispval ielt = VEC_REF(insertion,k);
              newelts[j++]=ielt; fd_incref(ielt); k++;}
            i++;}
          else {
            fd_decref(insertion);
            return fd_err(fd_SyntaxError,
                          "splicing UNQUOTE for an improper list",
                          NULL,insertion);}
          fd_decref(insertion);}
        else {
          lispval new_elt = fd_quasiquote(elt,env,level-1);
          if (FD_ABORTED(new_elt)) {
            int k = 0; while (k<j) {fd_decref(newelts[k]); k++;}
            u8_free(newelts);
            return new_elt;}
          newelts[j]=new_elt;
          i++; j++;}
      else {
        lispval new_elt = fd_quasiquote(elt,env,level);
        if (FD_ABORTED(new_elt)) {
          int k = 0; while (k<j) {fd_decref(newelts[k]); k++;}
          u8_free(newelts);
          return new_elt;}
        newelts[j]=new_elt;
        i++; j++;}}
    result = fd_make_vector(j,newelts);
    u8_free(newelts);
    return result;}
}

static lispval quasiquote_slotmap(lispval obj,fd_lexenv env,int level)
{
  int i = 0, len = FD_SLOTMAP_NUSED(obj);
  struct FD_KEYVAL *keyvals = FD_XSLOTMAP(obj)->sm_keyvals;
  lispval result = fd_empty_slotmap();
  struct FD_SLOTMAP *new_slotmap = FD_XSLOTMAP(result);
  while (i < len) {
    int free_slotid = 0;
    lispval slotid = keyvals[i].kv_key;
    lispval value = keyvals[i].kv_val;
    if (PAIRP(slotid)) {
      slotid = fd_quasiquote(slotid,env,level); free_slotid = 1;}
    if ((EMPTYP(slotid))||(VOIDP(slotid))) {
      if (free_slotid) fd_decref(slotid);
      i++; continue;}
    if ((PAIRP(value))||
        (VECTORP(value))||
        (SLOTMAPP(value))||
        (CHOICEP(value))||
        (PRECHOICEP(value))) {
      lispval qval = fd_quasiquote(value,env,level);
      if (FD_ABORTED(qval)) {
        fd_decref(result); return qval;}
      if (PRECHOICEP(qval)) qval = fd_simplify_choice(qval);
      fd_slotmap_store(new_slotmap,slotid,qval);
      fd_decref(qval);
      i++;}
    else {
      fd_slotmap_store(new_slotmap,slotid,value);
      i++;}}
  return result;
}

static lispval quasiquote_choice(lispval obj,fd_lexenv env,int level)
{
  lispval result = EMPTY;
  DO_CHOICES(elt,obj) {
    lispval transformed = fd_quasiquote(elt,env,level);
    if (FD_ABORTED(transformed)) {
      FD_STOP_DO_CHOICES; fd_decref(result);
      return transformed;}
    CHOICE_ADD(result,transformed);}
  return fd_simplify_choice(result);
}

FD_EXPORT
lispval fd_quasiquote(lispval obj,fd_lexenv env,int level)
{
  if (FD_ABORTED(obj)) return obj;
  else if (PAIRP(obj))
    if (FD_BAD_UNQUOTEP(obj))
      return fd_err(fd_SyntaxError,"malformed UNQUOTE",NULL,obj);
    else if (FD_EQ(FD_CAR(obj),quasiquote))
      if (PAIRP(FD_CDR(obj))) {
        lispval embed = fd_quasiquote(FD_CADR(obj),env,level+1);
        if (FD_ABORTED(embed)) return embed;
        else return fd_make_list(2,quasiquote,embed);}
      else return fd_err(fd_SyntaxError,"malformed QUASIQUOTE",NULL,obj);
    else if (FD_EQ(FD_CAR(obj),unquote))
      if (level==1) {
        lispval result=fd_eval(FD_CAR(FD_CDR(obj)),env);
        if (PRECHOICEP(result))
          result=fd_simplify_choice(result);
        return result;}
      else {
        lispval embed = fd_quasiquote(FD_CADR(obj),env,level-1);
        if (FD_ABORTED(embed)) return embed;
        else return fd_make_list(2,unquote,embed);}
    else if (FD_EQ(FD_CAR(obj),unquotestar))
      return fd_err(fd_SyntaxError,"UNQUOTE* (,@) in wrong context",
                    NULL,obj);
    else return quasiquote_list(obj,env,level);
  else if (VECTORP(obj))
    return quasiquote_vector(obj,env,level);
  else if (CHOICEP(obj))
    return quasiquote_choice(obj,env,level);
  else if (SLOTMAPP(obj))
    return quasiquote_slotmap(obj,env,level);
  else return fd_incref(obj);
}

static lispval quasiquote_evalfn(lispval obj,fd_lexenv env,fd_stack s)
{
  if ((PAIRP(FD_CDR(obj))) &&
      (NILP(FD_CDR(FD_CDR(obj))))) {
    lispval result = fd_quasiquote(FD_CAR(FD_CDR(obj)),env,1);
    if (FD_ABORTED(result))
      return result;
    else return result;}
  else return fd_err(fd_SyntaxError,"QUASIQUOTE",NULL,obj);
}

FD_EXPORT void fd_init_quasiquote_c()
{
  quote_symbol = fd_intern("QUOTE");
  quasiquote = fd_intern("QUASIQUOTE");
  unquote = fd_intern("UNQUOTE");
  unquotestar = fd_intern("UNQUOTE*");

  fd_def_evalfn(fd_scheme_module,"QUASIQUOTE","",quasiquote_evalfn);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
