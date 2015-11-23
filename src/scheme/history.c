/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/history.h"

static fdtype history_symbol;

/* Data stucture:
   a history object is a vector consisting of three elements
      1. top (the length of the history, including expired items)
      2. vec (the window vector, where recent history values are stored)
      3. tbl (the history table, which can map numbers to items when
         they've moved out of the window.

   To store an item in the history, place it in the top%len(vec) element
    and increment top.
   To retrieve the nth item from the history,
    if it is in range, get it from n%len(vec);
    if it isn't in range, try to get it from tbl

   The user-level primitive for accessing history items puts
    an item in the table when it accesses it.

*/

int unpack_history(fdtype history,int *top,int *len,fdtype **data,
                   fd_hashtable *table)
{
  if ((!(FD_VECTORP(history))) ||
      (!(FD_VECTOR_LENGTH(history)==3)) ||
      (!(FD_FIXNUMP(FD_VECTOR_REF(history,0)))) ||
      (!(FD_VECTORP(FD_VECTOR_REF(history,1)))) ||
      (!(FD_HASHTABLEP(FD_VECTOR_REF(history,2)))))
    return -1;
  else {
    *top=FD_FIX2INT(FD_VECTOR_REF(history,0));
    *len=FD_VECTOR_LENGTH(FD_VECTOR_REF(history,1));
    *data=FD_VECTOR_DATA(FD_VECTOR_REF(history,1));
    *table=(fd_hashtable)FD_VECTOR_REF(history,2);
    return *top;}
}

FD_EXPORT fdtype fd_history_ref(fdtype history,int ref)
{
  int top, len; fdtype *data; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_ref",history);
  if (ref<0) ref=top+ref;
  if (ref>=top)
    return fd_err(fd_RangeError,"fd_history_ref",
                  _("invalid history reference"),FD_INT(ref));
  else if (ref<(top-len))
    return fd_hashtable_get(h,FD_INT(ref),FD_VOID);
  else return fd_incref(data[ref%len]);
}

FD_EXPORT int fd_history_set(fdtype history,int ref,fdtype value)
{
  int top, len; fdtype *data; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_set",history);
  if (ref>=top)
    return fd_reterr(fd_RangeError,"fd_history_set",
                     _("invalid history reference"),history);
  else if (ref<(top-len))
    return fd_hashtable_op(h,fd_table_replace,FD_INT(ref),FD_VOID);
  else {
    int retval=fd_hashtable_op(h,fd_table_replace,FD_INT(ref),FD_VOID);
    fd_decref(data[ref%len]);
    data[ref%len]=FD_VOID;
    return retval;}
}

FD_EXPORT int fd_history_keep(fdtype history,int ref,fdtype value)
{
  int top, len; fdtype *data; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_set",history);
  if (fd_hashtable_store(h,FD_INT(ref),value)<0)
    return -1;
  else return ref;
}

FD_EXPORT int fd_history_find(fdtype history,fdtype value,int equal)
{
  int top, len; fdtype *data; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_find",history);
  else {
    int i=0; while (i<len)
      if ((FD_EQ(value,data[i])) ||
          ((equal) && (FDTYPE_EQUAL(value,data[i]))))
        break;
      else i++;
    if (i<len) return i;
    else {
      fdtype keys=fd_getkeys((fdtype)h);
      FD_DO_CHOICES(key,keys) {
        fdtype v=fd_hashtable_get(h,key,FD_VOID);
        if ((FD_EQ(v,value)) || ((equal) && (FDTYPE_EQUAL(v,value)))) {
          fd_decref(keys);
          return key;}}
      fd_decref(keys);
      return -1;}}
}

FD_EXPORT int fd_history_push(fdtype history,fdtype value)
{
  int top, len; fdtype *data, current, stored; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_push",history);
  current=data[top%len];
  stored=fd_incref(value);
  if (FD_ABORTP(stored)) {
    fd_clear_errors(1);
    return -1;}
  data[top%len]=stored;
  fd_decref(current);
  FD_VECTOR_SET(history,0,FD_INT(top+1));
  return top;
}

FD_EXPORT int fd_histpush(fdtype value)
{
  fdtype history=fd_thread_get(history_symbol);
  if (FD_VOIDP(history)) return -1;
  else {
    int retval=fd_history_push(history,value);
    fd_decref(history);
    return retval;}
}

FD_EXPORT int fd_hist_top()
{
  fdtype history=fd_thread_get(history_symbol);
  if (FD_VOIDP(history)) return -1;
  else {
    int pos=FD_FIX2INT(FD_VECTOR_REF(history,0));
    fd_decref(history);
    return pos;}
}

FD_EXPORT fdtype fd_histref(int ref)
{
  fdtype history=fd_thread_get(history_symbol);
  if (FD_VOIDP(history)) return FD_VOID;
  else {
    fdtype result=fd_history_ref(history,ref);
    fd_decref(history);
    return result;}
}

FD_EXPORT int fd_histfind(fdtype value)
{
  fdtype history=fd_thread_get(history_symbol);
  if (FD_VOIDP(history)) return FD_VOID;
  else {
    int pos=fd_history_find(history,value,0);
    if (pos<0) pos=fd_history_find(history,value,1);
    fd_decref(history);
    return pos;}
}

static int histkeep(int ref,fdtype value)
{
  fdtype history=fd_thread_get(history_symbol);
  if (FD_VOIDP(history)) return 0;
  else if (fd_history_keep(history,ref,value)<0) {
    fd_decref(history);
    return -1;}
  fd_decref(history);
  return 1;
}

FD_EXPORT void fd_histinit(int size)
{
  fdtype history=fd_thread_get(history_symbol);
  if (size<=0) {
    fdtype configval=fd_config_get("HISTORYSIZE");
    if (FD_FIXNUMP(configval))
      size=FD_FIX2INT(configval);
    else {
      fd_decref(configval);
      size=128;}}
  if (FD_VOIDP(history)) {
    history=fd_make_nvector(3,FD_INT(0),
                            fd_init_vector(NULL,size,NULL),
                            fd_make_hashtable(NULL,17));
    fd_thread_set(history_symbol,history);}
  else {
    fdtype newvec=fd_init_vector(NULL,size,NULL);
    fdtype oldvec=FD_VECTOR_REF(history,1);
    fdtype topval=FD_VECTOR_REF(history,0);
    int i=0, n=FD_VECTOR_LENGTH(oldvec), top=FD_FIX2INT(topval);
    int top_at=top%n;
    if (top<n) n=top;
    while (i<n) {
      int item_no=top-((i<top_at) ? (i) : (top_at+(n-i)));
      int new_loc=item_no%size;
      FD_VECTOR_SET(newvec,new_loc,FD_VECTOR_REF(oldvec,i));
      FD_VECTOR_SET(oldvec,i,FD_VOID);
      i++;}
    FD_VECTOR_SET(history,1,newvec);
    fd_decref(oldvec);}
  fd_decref(history);
}

FD_EXPORT void fd_histclear(int size)
{
  fd_thread_set(history_symbol,FD_VOID);
  fd_histinit(size);
}

static fdtype histref_prim(fdtype arg)
{
  if (!(FD_FIXNUMP(arg)))
    return fd_err(fd_SyntaxError,"histref_prim",
                  _("Invalid history references"),arg);
  else {
    fdtype val=fd_histref(FD_FIX2INT(arg));
    if (FD_ABORTP(val)) return val;
    else if (FD_VOIDP(val))
      u8_log(LOG_WARN,_("Lost history"),"Lost history for ##%q",arg);
    else if (histkeep(FD_FIX2INT(arg),val)<0)
      return FD_ERROR_VALUE;
    return val;}
}

static fdtype history_prim()
{
  return fd_thread_get(history_symbol);
}

static fdtype histclear_prim(fdtype arg)
{
  if (FD_VOIDP(arg)) {
    /* We're clearing the whole thing, the simple case. */
    fd_histclear(-1);
    return FD_VOID;}
  else {
    /* Get the arg */
    if (FD_FIXNUMP(arg)) {
      fdtype history=fd_thread_get(history_symbol);
      if (FD_VOIDP(history)) return FD_TRUE;
      else fd_history_set(history,FD_FIX2INT(arg),FD_VOID);
      return FD_VOID;}
    else {
      int loc=fd_histfind(arg);
      if (loc<0) {
        u8_log(LOG_WARN,_("History error"),"Couldn't find item in history: %q",arg);
        return FD_FALSE;}
      else {
        fdtype history=fd_thread_get(history_symbol);
        fd_history_set(history,loc,FD_VOID);
        return FD_TRUE;}}}
}

static int scheme_history_initialized=0;

FD_EXPORT void fd_init_history_c()
{
  fdtype history_module;
  if (scheme_history_initialized) return;
  else scheme_history_initialized=1;
  fd_init_fdscheme();
  history_module=fd_new_module("HISTORY",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

  fd_idefn(history_module,fd_make_cprim1("%HISTREF",histref_prim,1));
  fd_idefn(history_module,fd_make_cprim0("%HISTORY",history_prim,0));
  fd_idefn(history_module,fd_make_cprim1("%HISTCLEAR",histclear_prim,0));

  history_symbol=fd_intern("%HISTORY");

  fd_finish_module(history_module);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
