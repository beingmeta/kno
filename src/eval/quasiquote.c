/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1
#define KNO_INLINE_TABLES 1
#define KNO_INLINE_FCNIDS 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/support.h"
#include "kno/storage.h"
#include "kno/eval.h"
#include "kno/dtproc.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/ports.h"
#include "kno/dtcall.h"

#include <libu8/u8timefns.h>
#include <libu8/u8printf.h>

#include <math.h>
#include <pthread.h>
#include <errno.h>

/* QUASIQUOTE */

static lispval quasiquote, unquote, unquotestar, quote_symbol;
KNO_EXPORT lispval kno_quasiquote(lispval obj,kno_lexenv env,int level);

#define KNO_BAD_UNQUOTEP(elt) \
  (((KNO_EQ(KNO_CAR(elt),unquote)) || \
    (KNO_EQ(KNO_CAR(elt),unquotestar))) && \
   (!((PAIRP(KNO_CDR(elt))) &&                 \
      (NILP(KNO_CDR(KNO_CDR(elt)))))))

static lispval quasiquote_list(lispval obj,kno_lexenv env,int level)
{
  lispval head = NIL, *tail = &head;
  while (PAIRP(obj)) {
    lispval elt = KNO_CAR(obj), new_elt, new_tail;
    struct KNO_PAIR *tailcons;
    if (ATOMICP(elt)) {
      /* This handles the case of a dotted unquote. */
      if (KNO_EQ(elt,unquote)) {
        if ((PAIRP(KNO_CDR(obj))) &&
            (NILP(KNO_CDR(KNO_CDR(obj)))))
          if (level==1) {
            lispval splice_at_end = kno_eval(KNO_CADR(obj),env);
            if (KNO_ABORTED(splice_at_end)) {
              kno_decref(head);
              return splice_at_end;}
            else if (VOIDP(splice_at_end)) {
              kno_seterr(kno_VoidArgument,"quasiquote_list",NULL,KNO_CADR(obj));
              kno_decref(head);
              return KNO_ERROR;}
            else {
              if (PRECHOICEP(splice_at_end))
                splice_at_end=kno_simplify_choice(splice_at_end);
              *tail = splice_at_end;
              return head;}}
          else {
            lispval splice_at_end = kno_quasiquote(KNO_CADR(obj),env,level-1);
            if (KNO_ABORTED(splice_at_end)) {
              kno_decref(head);
              return splice_at_end;}
            else {
              lispval with_unquote = kno_conspair(unquote,splice_at_end);
              *tail = with_unquote;
              return head;}}
        else {
          kno_decref(head);
          return kno_err(kno_SyntaxError,"malformed UNQUOTE",NULL,obj);}}
      else new_elt = elt;}
    else if (PAIRP(elt))
      if (KNO_BAD_UNQUOTEP(elt)) {
        kno_decref(head);
        return kno_err(kno_SyntaxError,"malformed UNQUOTE",NULL,elt);}
      else if (KNO_EQ(KNO_CAR(elt),unquote)) {
        if (level==1) {
          new_elt = kno_eval(KNO_CADR(elt),env);
          if (VOIDP(new_elt))
            new_elt = kno_err(kno_VoidArgument,"quasiquote_list",
                             NULL,KNO_CADR(elt));
          else if (PRECHOICEP(new_elt))
            new_elt=kno_simplify_choice(new_elt);}
        else {
          lispval embed = kno_quasiquote(KNO_CADR(elt),env,level-1);
          if (KNO_ABORTED(embed)) new_elt = embed;
          else new_elt = kno_make_list(2,unquote,embed);}
        if (KNO_ABORTED(new_elt)) {
          kno_decref(head);
          return new_elt;}}
      else if (KNO_EQ(KNO_CAR(elt),unquotestar))
        if (level==1) {
          lispval insertion = kno_eval(KNO_CADR(elt),env);
          if (PRECHOICEP(insertion))
            insertion=kno_simplify_choice(insertion);
          if (KNO_ABORTED(insertion)) {
              kno_decref(head);
              return insertion;}
          else if (VOIDP(insertion)) {
            kno_seterr(kno_VoidArgument,"quasiquote_list",NULL,KNO_CADR(elt));
            kno_decref(head);
            return KNO_ERROR;}
          else if (NILP(insertion)) {}
          else if (PAIRP(insertion)) {
            lispval scan = insertion, last = VOID;
            while (PAIRP(scan)) {last = scan; scan = KNO_CDR(scan);}
            if (!(PAIRP(last))) {
              u8_string details_string = u8_mkstring("RESULT=%q",elt);
              lispval err;
              err = kno_err(kno_SyntaxError,
                         "splicing UNQUOTE for an improper list",
                         details_string,insertion);
              kno_decref(head); u8_free(details_string);
              kno_decref(insertion);
              return err;}
            else {
              KNO_DOLIST(insert_elt,insertion) {
                lispval new_pair = kno_conspair(insert_elt,NIL);
                *tail = new_pair; tail = &(KNO_CDR(new_pair));
                kno_incref(insert_elt);}}}
          else if (VECTORP(insertion)) {
            int i = 0, lim = VEC_LEN(insertion);
            while (i<lim) {
              lispval insert_elt = VEC_REF(insertion,i);
              lispval new_pair = kno_conspair(insert_elt,NIL);
              *tail = new_pair; tail = &(KNO_CDR(new_pair));
              kno_incref(insert_elt);
              i++;}}
          else {
            u8_string details_string =
              u8_mkstring("RESULT=%q=%q",elt,insertion);
            lispval err;
            err = kno_err(kno_SyntaxError,
                       "splicing UNQUOTE used with a non-squence",
                       details_string,insertion);
            kno_decref(head); u8_free(details_string);
            kno_decref(insertion);
            return err;}
          obj = KNO_CDR(obj);
          kno_decref(insertion);
          continue;}
        else {
          lispval embed = kno_quasiquote(KNO_CADR(elt),env,level-1);
          if (KNO_ABORTED(embed)) new_elt = embed;
          else new_elt = kno_make_list(2,unquotestar,embed);}
      else new_elt = kno_quasiquote(elt,env,level);
    else new_elt = kno_quasiquote(elt,env,level);
    if (KNO_ABORTED(new_elt)) {
      kno_decref(head); return new_elt;}
    new_tail = kno_conspair(new_elt,NIL);
    tailcons = KNO_CONSPTR(kno_pair,new_tail);
    *tail = new_tail; tail = &(tailcons->cdr);
    obj = KNO_CDR(obj);}
  if (!(NILP(obj))) {
    lispval final = kno_quasiquote(obj,env,level);
    if (KNO_ABORTED(final)) {
      kno_decref(head);
      return final;}
    else *tail = final;}
  return head;
}

static lispval quasiquote_vector(lispval obj,kno_lexenv env,int level)
{
  lispval result = VOID;
  int i = 0, j = 0, len = VEC_LEN(obj), newlen = len;
  if (len==0) return kno_incref(obj);
  else {
    lispval *newelts = u8_alloc_n(len,lispval);
    while (i < len) {
      lispval elt = VEC_REF(obj,i);
      if ((PAIRP(elt)) &&
          (KNO_EQ(KNO_CAR(elt),unquotestar)) &&
          (PAIRP(KNO_CDR(elt))))
        if (level==1) {
          lispval insertion = kno_eval(KNO_CADR(elt),env); int addlen = 0;
          if (PRECHOICEP(insertion))
            insertion=kno_simplify_choice(insertion);
          if (KNO_ABORTED(insertion)) {
            int k = 0; while (k<j) {kno_decref(newelts[k]); k++;}
            u8_free(newelts);
            return insertion;}
          else if (VOIDP(insertion)) {
            kno_seterr(kno_VoidArgument,"quasiquote_vector",NULL,KNO_CADR(elt));
            int k = 0; while (k<j) {kno_decref(newelts[k]); k++;}
            u8_free(newelts);
            return KNO_ERROR;}
          if (NILP(insertion)) {}
          else if (PAIRP(insertion)) {
            lispval scan = insertion; while (PAIRP(scan)) {
              scan = KNO_CDR(scan); addlen++;}
            if (!(NILP(scan)))
              return kno_err(kno_SyntaxError,
                            "splicing UNQUOTE for an improper list",
                            NULL,insertion);}
          else if (VECTORP(insertion)) addlen = VEC_LEN(insertion);
          else return kno_err(kno_SyntaxError,
                             "splicing UNQUOTE for an improper list",
                             NULL,insertion);
          if (addlen==0) {
            i++; kno_decref(insertion); continue;}
          newelts = u8_realloc_n(newelts,newlen+addlen,lispval);
          newlen = newlen+addlen;
          if (PAIRP(insertion)) {
            lispval scan = insertion; while (PAIRP(scan)) {
              lispval ielt = KNO_CAR(scan); newelts[j++]=ielt;
              kno_incref(ielt); scan = KNO_CDR(scan);}
            i++;}
          else if (VECTORP(insertion)) {
            int k = 0; while (k<addlen) {
              lispval ielt = VEC_REF(insertion,k);
              newelts[j++]=ielt; kno_incref(ielt); k++;}
            i++;}
          else {
            kno_decref(insertion);
            return kno_err(kno_SyntaxError,
                          "splicing UNQUOTE for an improper list",
                          NULL,insertion);}
          kno_decref(insertion);}
        else {
          lispval new_elt = kno_quasiquote(elt,env,level-1);
          if (KNO_ABORTED(new_elt)) {
            int k = 0; while (k<j) {kno_decref(newelts[k]); k++;}
            u8_free(newelts);
            return new_elt;}
          newelts[j]=new_elt;
          i++; j++;}
      else {
        lispval new_elt = kno_quasiquote(elt,env,level);
        if (KNO_ABORTED(new_elt)) {
          int k = 0; while (k<j) {kno_decref(newelts[k]); k++;}
          u8_free(newelts);
          return new_elt;}
        newelts[j]=new_elt;
        i++; j++;}}
    result = kno_make_vector(j,newelts);
    u8_free(newelts);
    return result;}
}

static lispval quasiquote_slotmap(lispval obj,kno_lexenv env,int level)
{
  int i = 0, len = KNO_SLOTMAP_NUSED(obj);
  struct KNO_KEYVAL *keyvals = KNO_XSLOTMAP(obj)->sm_keyvals;
  lispval result = kno_empty_slotmap();
  struct KNO_SLOTMAP *new_slotmap = KNO_XSLOTMAP(result);
  while (i < len) {
    int free_slotid = 0;
    lispval slotid = keyvals[i].kv_key;
    lispval value = keyvals[i].kv_val;
    if (PAIRP(slotid)) {
      slotid = kno_quasiquote(slotid,env,level); free_slotid = 1;}
    if ((EMPTYP(slotid))||(VOIDP(slotid))) {
      if (free_slotid) kno_decref(slotid);
      i++; continue;}
    if ((PAIRP(value))||
        (VECTORP(value))||
        (SLOTMAPP(value))||
        (CHOICEP(value))||
        (PRECHOICEP(value))) {
      lispval qval = kno_quasiquote(value,env,level);
      if (KNO_ABORTED(qval)) {
        kno_decref(result); return qval;}
      if (PRECHOICEP(qval)) qval = kno_simplify_choice(qval);
      kno_slotmap_store(new_slotmap,slotid,qval);
      kno_decref(qval);
      i++;}
    else {
      kno_slotmap_store(new_slotmap,slotid,value);
      i++;}}
  return result;
}

static lispval quasiquote_choice(lispval obj,kno_lexenv env,int level)
{
  lispval result = EMPTY;
  DO_CHOICES(elt,obj) {
    lispval transformed = kno_quasiquote(elt,env,level);
    if (KNO_ABORTED(transformed)) {
      KNO_STOP_DO_CHOICES; kno_decref(result);
      return transformed;}
    CHOICE_ADD(result,transformed);}
  return kno_simplify_choice(result);
}

KNO_EXPORT
lispval kno_quasiquote(lispval obj,kno_lexenv env,int level)
{
  if (KNO_ABORTED(obj)) return obj;
  else if (PAIRP(obj))
    if (KNO_BAD_UNQUOTEP(obj))
      return kno_err(kno_SyntaxError,"malformed UNQUOTE",NULL,obj);
    else if (KNO_EQ(KNO_CAR(obj),quasiquote))
      if (PAIRP(KNO_CDR(obj))) {
        lispval embed = kno_quasiquote(KNO_CADR(obj),env,level+1);
        if (KNO_ABORTED(embed)) return embed;
        else return kno_make_list(2,quasiquote,embed);}
      else return kno_err(kno_SyntaxError,"malformed QUASIQUOTE",NULL,obj);
    else if (KNO_EQ(KNO_CAR(obj),unquote))
      if (level==1) {
        lispval result=kno_eval(KNO_CAR(KNO_CDR(obj)),env);
        if (PRECHOICEP(result))
          result=kno_simplify_choice(result);
        return result;}
      else {
        lispval embed = kno_quasiquote(KNO_CADR(obj),env,level-1);
        if (KNO_ABORTED(embed)) return embed;
        else return kno_make_list(2,unquote,embed);}
    else if (KNO_EQ(KNO_CAR(obj),unquotestar))
      return kno_err(kno_SyntaxError,"UNQUOTE* (,@) in wrong context",
                    NULL,obj);
    else return quasiquote_list(obj,env,level);
  else if (VECTORP(obj))
    return quasiquote_vector(obj,env,level);
  else if (CHOICEP(obj))
    return quasiquote_choice(obj,env,level);
  else if (SLOTMAPP(obj))
    return quasiquote_slotmap(obj,env,level);
  else return kno_incref(obj);
}

static lispval quasiquote_evalfn(lispval obj,kno_lexenv env,kno_stack s)
{
  if ((PAIRP(KNO_CDR(obj))) &&
      (NILP(KNO_CDR(KNO_CDR(obj))))) {
    lispval result = kno_quasiquote(KNO_CAR(KNO_CDR(obj)),env,1);
    if (KNO_ABORTED(result))
      return result;
    else return result;}
  else return kno_err(kno_SyntaxError,"QUASIQUOTE",NULL,obj);
}

KNO_EXPORT void kno_init_quasiquote_c()
{
  quote_symbol = kno_intern("QUOTE");
  quasiquote = kno_intern("QUASIQUOTE");
  unquote = kno_intern("UNQUOTE");
  unquotestar = kno_intern("UNQUOTE*");

  kno_def_evalfn(kno_scheme_module,"QUASIQUOTE","",quasiquote_evalfn);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
