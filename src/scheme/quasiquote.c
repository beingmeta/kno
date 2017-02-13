/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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
#include "framerd/fddb.h"
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

static fdtype quasiquote, unquote, unquotestar, quote_symbol;
FD_EXPORT fdtype fd_quasiquote(fdtype obj,fd_lispenv env,int level);

#define FD_BAD_UNQUOTEP(elt) \
  (((FD_EQ(FD_CAR(elt),unquote)) || \
    (FD_EQ(FD_CAR(elt),unquotestar))) && \
   (!((FD_PAIRP(FD_CDR(elt))) &&                 \
      (FD_EMPTY_LISTP(FD_CDR(FD_CDR(elt)))))))

static fdtype quasiquote_list(fdtype obj,fd_lispenv env,int level)
{
  fdtype head=FD_EMPTY_LIST, *tail=&head;
  while (FD_PAIRP(obj)) {
    fdtype elt=FD_CAR(obj), new_elt, new_tail;
    struct FD_PAIR *tailcons;
    if (FD_ATOMICP(elt)) {
      /* This handles the case of a dotted unquote. */
      if (FD_EQ(elt,unquote)) {
        if ((FD_PAIRP(FD_CDR(obj))) &&
            (FD_EMPTY_LISTP(FD_CDR(FD_CDR(obj)))))
          if (level==1) {
            fdtype splice_at_end=fd_eval(FD_CADR(obj),env);
            if (FD_ABORTED(splice_at_end)) {
              fd_decref(head);
              return splice_at_end;}
            else {
              *tail=splice_at_end;
              return head;}}
          else {
            fdtype splice_at_end=fd_quasiquote(FD_CADR(obj),env,level-1);
            if (FD_ABORTED(splice_at_end)) {
              fd_decref(head); 
              return splice_at_end;}
            else {
              fdtype with_unquote=fd_conspair(unquote,splice_at_end);
              *tail=with_unquote;
              return head;}}
        else {
          fd_decref(head);
          return fd_err(fd_SyntaxError,"malformed UNQUOTE",NULL,obj);}}
      else new_elt=elt;}
    else if (FD_PAIRP(elt))
      if (FD_BAD_UNQUOTEP(elt)) {
        fd_decref(head);
        return fd_err(fd_SyntaxError,"malformed UNQUOTE",NULL,elt);}
      else if (FD_EQ(FD_CAR(elt),unquote)) {
        if (level==1)
          new_elt=fd_eval(FD_CADR(elt),env);
        else {
          fdtype embed=fd_quasiquote(FD_CADR(elt),env,level-1);
          if (FD_ABORTED(embed)) new_elt=embed;
          else new_elt=fd_make_list(2,unquote,embed);}
        if (FD_ABORTED(new_elt)) {
          fd_decref(head);
          return new_elt;}}
      else if (FD_EQ(FD_CAR(elt),unquotestar))
        if (level==1) {
          fdtype insertion=fd_eval(FD_CADR(elt),env);
          if (FD_ABORTED(insertion)) {
              fd_decref(head);
              return insertion;}
          else if (FD_EMPTY_LISTP(insertion)) {}
          else if (FD_PAIRP(insertion)) {
            fdtype scan=insertion, last=FD_VOID;
            while (FD_PAIRP(scan)) {last=scan; scan=FD_CDR(scan);}
            if (!(FD_PAIRP(last))) {
              u8_string details_string=u8_mkstring("RESULT=%q",elt);
              fdtype err;
              err=fd_err(fd_SyntaxError,
                         "splicing UNQUOTE for an improper list",
                         details_string,insertion);
              fd_decref(head); u8_free(details_string);
              fd_decref(insertion);
              return err;}
            else {
              FD_DOLIST(insert_elt,insertion) {
                fdtype new_pair=fd_conspair(insert_elt,FD_EMPTY_LIST);
                *tail=new_pair; tail=&(FD_CDR(new_pair));
                fd_incref(insert_elt);}}}
          else if (FD_VECTORP(insertion)) {
            int i=0, lim=FD_VECTOR_LENGTH(insertion);
            while (i<lim) {
              fdtype insert_elt=FD_VECTOR_REF(insertion,i);
              fdtype new_pair=fd_conspair(insert_elt,FD_EMPTY_LIST);
              *tail=new_pair; tail=&(FD_CDR(new_pair));
              fd_incref(insert_elt); 
              i++;}}
          else {
            u8_string details_string=u8_mkstring("RESULT=%q",elt);
            fdtype err;
            err=fd_err(fd_SyntaxError,
                       "splicing UNQUOTE for an improper list",
                       details_string,insertion);
            fd_decref(head); u8_free(details_string);
            fd_decref(insertion);
            return err;}
          obj=FD_CDR(obj);
          fd_decref(insertion);
          continue;}
        else {
          fdtype embed=fd_quasiquote(FD_CADR(elt),env,level-1);
          if (FD_ABORTED(embed)) new_elt=embed;
          else new_elt=fd_make_list(2,unquotestar,embed);}
      else new_elt=fd_quasiquote(elt,env,level);
    else new_elt=fd_quasiquote(elt,env,level);
    if (FD_ABORTED(new_elt)) {
      fd_decref(head); return new_elt;}
    new_tail=fd_conspair(new_elt,FD_EMPTY_LIST);
    tailcons=FD_STRIP_CONS(new_tail,fd_pair_type,struct FD_PAIR *);
    *tail=new_tail; tail=&(tailcons->fd_cdr);
    obj=FD_CDR(obj);}
  if (!(FD_EMPTY_LISTP(obj))) {
    fdtype final=fd_quasiquote(obj,env,level);
    if (FD_ABORTED(final)) {
      fd_decref(head); 
      return final;}
    else *tail=final;}
  return head;
}

static fdtype quasiquote_vector(fdtype obj,fd_lispenv env,int level)
{
  fdtype result=FD_VOID;
  int i=0, j=0, len=FD_VECTOR_LENGTH(obj), newlen=len;
  if (len==0) return fd_incref(obj);
  else {
    fdtype *newelts=u8_alloc_n(len,fdtype);
    while (i < len) {
      fdtype elt=FD_VECTOR_REF(obj,i);
      if ((FD_PAIRP(elt)) &&
          (FD_EQ(FD_CAR(elt),unquotestar)) &&
          (FD_PAIRP(FD_CDR(elt))))
        if (level==1) {
          fdtype insertion=fd_eval(FD_CADR(elt),env); int addlen=0;
          if (FD_ABORTED(insertion)) {
            int k=0; while (k<j) {fd_decref(newelts[k]); k++;}
            u8_free(newelts);
            return insertion;}
          if (FD_EMPTY_LISTP(insertion)) {}
          else if (FD_PAIRP(insertion)) {
            fdtype scan=insertion; while (FD_PAIRP(scan)) {
              scan=FD_CDR(scan); addlen++;}
            if (!(FD_EMPTY_LISTP(scan)))
              return fd_err(fd_SyntaxError,
                            "splicing UNQUOTE for an improper list",
                            NULL,insertion);}
          else if (FD_VECTORP(insertion)) addlen=FD_VECTOR_LENGTH(insertion);
          else return fd_err(fd_SyntaxError,
                             "splicing UNQUOTE for an improper list",
                             NULL,insertion);
          if (addlen==0) {
            i++; fd_decref(insertion); continue;}
          newelts=u8_realloc_n(newelts,newlen+addlen,fdtype);
          newlen=newlen+addlen;
          if (FD_PAIRP(insertion)) {
            fdtype scan=insertion; while (FD_PAIRP(scan)) {
              fdtype ielt=FD_CAR(scan); newelts[j++]=ielt;
              fd_incref(ielt); scan=FD_CDR(scan);}
            i++;}
          else if (FD_VECTORP(insertion)) {
            int k=0; while (k<addlen) {
              fdtype ielt=FD_VECTOR_REF(insertion,k);
              newelts[j++]=ielt; fd_incref(ielt); k++;}
            i++;}
          else {
            fd_decref(insertion);
            return fd_err(fd_SyntaxError,
                          "splicing UNQUOTE for an improper list",
                          NULL,insertion);}
          fd_decref(insertion);}
        else {
          fdtype new_elt=fd_quasiquote(elt,env,level-1);
          if (FD_ABORTED(new_elt)) {
            int k=0; while (k<j) {fd_decref(newelts[k]); k++;}
            u8_free(newelts);
            return new_elt;}
          newelts[j]=new_elt;
          i++; j++;}
      else {
        fdtype new_elt=fd_quasiquote(elt,env,level);
        if (FD_ABORTED(new_elt)) {
          int k=0; while (k<j) {fd_decref(newelts[k]); k++;}
          u8_free(newelts);
          return new_elt;}
        newelts[j]=new_elt;
        i++; j++;}}
    result=fd_make_vector(j,newelts);
    u8_free(newelts);
    return result;}
}

static fdtype quasiquote_slotmap(fdtype obj,fd_lispenv env,int level)
{
  int i=0, len=FD_SLOTMAP_SIZE(obj);
  struct FD_KEYVAL *keyvals=FD_XSLOTMAP(obj)->sm_keyvals;
  fdtype result=fd_empty_slotmap();
  struct FD_SLOTMAP *new_slotmap=FD_XSLOTMAP(result);
  while (i < len) {
    int free_slotid=0;
    fdtype slotid=keyvals[i].fd_kvkey;
    fdtype value=keyvals[i].fd_keyval;
    if (FD_PAIRP(slotid)) {
      slotid=fd_quasiquote(slotid,env,level); free_slotid=1;}
    if ((FD_EMPTY_CHOICEP(slotid))||(FD_VOIDP(slotid))) {
      if (free_slotid) fd_decref(slotid);
      i++; continue;}
    if ((FD_PAIRP(value))||(FD_VECTORP(value))||(FD_SLOTMAPP(value))||
        (FD_CHOICEP(value))||(FD_ACHOICEP(value))) {
      fdtype qval=fd_quasiquote(value,env,level);
      if (FD_ABORTED(qval)) {
        fd_decref(result); return qval;}
      fd_slotmap_store(new_slotmap,slotid,qval);
      fd_decref(qval); i++;}
    else {
      fd_slotmap_store(new_slotmap,slotid,value);
      i++;}}
  return result;
}

static fdtype quasiquote_choice(fdtype obj,fd_lispenv env,int level)
{
  fdtype result=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(elt,obj) {
    fdtype transformed=fd_quasiquote(elt,env,level);
    if (FD_ABORTED(transformed)) {
      FD_STOP_DO_CHOICES; fd_decref(result);
      return transformed;}
    FD_ADD_TO_CHOICE(result,transformed);}
  return result;
}

FD_EXPORT
fdtype fd_quasiquote(fdtype obj,fd_lispenv env,int level)
{
  if (FD_ABORTED(obj)) return obj;
  else if (FD_PAIRP(obj))
    if (FD_BAD_UNQUOTEP(obj))
      return fd_err(fd_SyntaxError,"malformed UNQUOTE",NULL,obj);
    else if (FD_EQ(FD_CAR(obj),quasiquote))
      if (FD_PAIRP(FD_CDR(obj))) {
        fdtype embed=fd_quasiquote(FD_CADR(obj),env,level+1);
        if (FD_ABORTED(embed)) return embed;
        else return fd_make_list(2,quasiquote,embed);}
      else return fd_err(fd_SyntaxError,"malformed QUASIQUOTE",NULL,obj);
    else if (FD_EQ(FD_CAR(obj),unquote))
      if (level==1)
        return fd_eval(FD_CAR(FD_CDR(obj)),env);
      else {
        fdtype embed=fd_quasiquote(FD_CADR(obj),env,level-1);
        if (FD_ABORTED(embed)) return embed;
        else return fd_make_list(2,unquote,embed);}
    else if (FD_EQ(FD_CAR(obj),unquotestar))
      return fd_err(fd_SyntaxError,"UNQUOTE* (,@) in wrong context",
                    NULL,obj);
    else return quasiquote_list(obj,env,level);
  else if (FD_VECTORP(obj))
    return quasiquote_vector(obj,env,level);
  else if (FD_CHOICEP(obj))
    return quasiquote_choice(obj,env,level);
  else if (FD_SLOTMAPP(obj))
    return quasiquote_slotmap(obj,env,level);
  else return fd_incref(obj);
}

static fdtype quasiquote_handler(fdtype obj,fd_lispenv env)
{
  if ((FD_PAIRP(FD_CDR(obj))) &&
      (FD_EMPTY_LISTP(FD_CDR(FD_CDR(obj))))) {
    fdtype result=fd_quasiquote(FD_CAR(FD_CDR(obj)),env,1);
    if (FD_ABORTED(result))
      return result;
    else return result;}
  else return fd_err(fd_SyntaxError,"QUASIQUOTE",NULL,obj);
}

FD_EXPORT void fd_init_quasiquote_c()
{
  quote_symbol=fd_intern("QUOTE");
  quasiquote=fd_intern("QUASIQUOTE");
  unquote=fd_intern("UNQUOTE");
  unquotestar=fd_intern("UNQUOTE*");

  fd_defspecial(fd_scheme_module,"QUASIQUOTE",quasiquote_handler);

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
