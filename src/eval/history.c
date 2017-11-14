/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"
#include "framerd/eval.h"
#include "framerd/history.h"

static lispval history_symbol;

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

int unpack_history(lispval history,int *top,int *len,lispval **data,
                   fd_hashtable *table)
{
  if ((!(VECTORP(history))) ||
      (!(VEC_LEN(history)==3)) ||
      (!(FIXNUMP(VEC_REF(history,0)))) ||
      (!(VECTORP(VEC_REF(history,1)))) ||
      (!(HASHTABLEP(VEC_REF(history,2)))))
    return -1;
  else {
    *top = FIX2INT(VEC_REF(history,0));
    *len = VEC_LEN(VEC_REF(history,1));
    *data = VEC_DATA(VEC_REF(history,1));
    *table = (fd_hashtable)VEC_REF(history,2);
    return *top;}
}

FD_EXPORT lispval fd_history_ref(lispval history,int ref)
{
  int top, len; lispval *data; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_ref",history);
  if (ref<0) ref = top+ref;
  if (ref>=top)
    return fd_err(fd_RangeError,"fd_history_ref",
                  _("invalid history reference"),FD_INT(ref));
  else if (ref<(top-len))
    return fd_hashtable_get(h,FD_INT(ref),VOID);
  else return fd_incref(data[ref%len]);
}

FD_EXPORT int fd_history_set(lispval history,int ref,lispval value)
{
  int top, len; lispval *data; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_set",history);
  if (ref>=top)
    return fd_reterr(fd_RangeError,"fd_history_set",
                     _("invalid history reference"),history);
  else if (ref<(top-len))
    return fd_hashtable_op(h,fd_table_replace,FD_INT(ref),VOID);
  else {
    int retval = fd_hashtable_op(h,fd_table_replace,FD_INT(ref),VOID);
    fd_decref(data[ref%len]);
    data[ref%len]=VOID;
    return retval;}
}

FD_EXPORT int fd_history_keep(lispval history,int ref,lispval value)
{
  int top, len; lispval *data; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_set",history);
  if (fd_hashtable_store(h,FD_INT(ref),value)<0)
    return -1;
  else return ref;
}

FD_EXPORT int fd_history_find(lispval history,lispval value,int equal)
{
  int top, len; lispval *data; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_find",history);
  else {
    int i = 0; while (i<len)
      if ((FD_EQ(value,data[i])) ||
          ((equal) && (LISP_EQUAL(value,data[i]))))
        break;
      else i++;
    if (i<len) return i;
    else {
      lispval keys = fd_getkeys((lispval)h);
      DO_CHOICES(key,keys) {
        lispval v = fd_hashtable_get(h,key,VOID);
        if ((FD_EQ(v,value)) || ((equal) && (LISP_EQUAL(v,value)))) {
          fd_decref(keys);
          return key;}}
      fd_decref(keys);
      return -1;}}
}

FD_EXPORT int fd_history_push(lispval history,lispval value)
{
  int top, len; lispval *data, current, stored; fd_hashtable h;
  if (unpack_history(history,&top,&len,&data,&h)<0)
    return fd_type_error(_("history"),"fd_history_push",history);
  current = data[top%len];
  stored = fd_incref(value);
  if (FD_ABORTP(stored)) {
    fd_clear_errors(1);
    return -1;}
  data[top%len]=stored;
  fd_decref(current);
  FD_VECTOR_SET(history,0,FD_INT(top+1));
  return top;
}

FD_EXPORT int fd_histpush(lispval value)
{
  lispval history = fd_thread_get(history_symbol);
  if (VOIDP(history)) return -1;
  else {
    int retval = fd_history_push(history,value);
    fd_decref(history);
    return retval;}
}

FD_EXPORT int fd_hist_top()
{
  lispval history = fd_thread_get(history_symbol);
  if (VOIDP(history)) return -1;
  else {
    int pos = FIX2INT(VEC_REF(history,0));
    fd_decref(history);
    return pos;}
}

FD_EXPORT lispval fd_histref(int ref)
{
  lispval history = fd_thread_get(history_symbol);
  if (VOIDP(history)) return VOID;
  else {
    lispval result = fd_history_ref(history,ref);
    fd_decref(history);
    return result;}
}

FD_EXPORT int fd_histfind(lispval value)
{
  lispval history = fd_thread_get(history_symbol);
  if (VOIDP(history)) return VOID;
  else {
    int pos = fd_history_find(history,value,0);
    if (pos<0) pos = fd_history_find(history,value,1);
    fd_decref(history);
    return pos;}
}

static int histkeep(int ref,lispval value)
{
  lispval history = fd_thread_get(history_symbol);
  if (VOIDP(history)) return 0;
  else if (fd_history_keep(history,ref,value)<0) {
    fd_decref(history);
    return -1;}
  fd_decref(history);
  return 1;
}

FD_EXPORT void fd_histinit(int size)
{
  lispval history = fd_thread_get(history_symbol);
  if (size<=0) {
    lispval configval = fd_config_get("HISTORYSIZE");
    if (FD_UINTP(configval))
      size = FIX2INT(configval);
    else {
      fd_decref(configval);
      size = 128;}}
  if (VOIDP(history)) {
    history = fd_make_nvector(3,FD_INT(0),
                              fd_empty_vector(size),
                              fd_make_hashtable(NULL,17));
    fd_thread_set(history_symbol,history);}
  else {
    lispval newvec = fd_empty_vector(size);
    lispval oldvec = VEC_REF(history,1);
    lispval topval = VEC_REF(history,0);
    if (!(FD_UINTP(topval))) {
      u8_log(LOG_WARN,"Bad history data",
             "Negative topval in %q",history);
      fd_decref(newvec);
      return;}
    int i = 0, n = VEC_LEN(oldvec), top = FIX2INT(topval);
    int top_at = top%n;
    if (top<n) n = top;
    while (i<n) {
      int item_no = top-((i<top_at) ? (i) : (top_at+(n-i)));
      int new_loc = item_no%size;
      FD_VECTOR_SET(newvec,new_loc,VEC_REF(oldvec,i));
      FD_VECTOR_SET(oldvec,i,VOID);
      i++;}
    FD_VECTOR_SET(history,1,newvec);
    fd_decref(oldvec);}
  fd_decref(history);
}

FD_EXPORT void fd_histclear(int size)
{
  fd_thread_set(history_symbol,VOID);
  fd_histinit(size);
}

static lispval histref_prim(lispval arg)
{
  if (!(FD_UINTP(arg)))
    return fd_err(fd_SyntaxError,"histref_prim",
                  _("Invalid history references"),arg);
  else {
    lispval val = fd_histref(FIX2INT(arg));
    if (FD_ABORTP(val)) return val;
    else if (VOIDP(val))
      u8_log(LOG_WARN,_("Lost history"),"Lost history for ##%q",arg);
    else if (histkeep(FIX2INT(arg),val)<0)
      return FD_ERROR;
    return val;}
}

static lispval history_prim()
{
  return fd_thread_get(history_symbol);
}

static lispval histclear_prim(lispval arg)
{
  if (VOIDP(arg)) {
    /* We're clearing the whole thing, the simple case. */
    fd_histclear(-1);
    return VOID;}
  else {
    /* Get the arg */
    if (FD_UINTP(arg)) {
      lispval history = fd_thread_get(history_symbol);
      if (VOIDP(history)) return FD_TRUE;
      else fd_history_set(history,FIX2INT(arg),VOID);
      return VOID;}
    else {
      int loc = fd_histfind(arg);
      if (loc<0) {
        u8_log(LOG_WARN,_("History error"),"Couldn't find item in history: %q",arg);
        return FD_FALSE;}
      else {
        lispval history = fd_thread_get(history_symbol);
        fd_history_set(history,loc,VOID);
        return FD_TRUE;}}}
}

static int scheme_history_initialized = 0;

FD_EXPORT void fd_init_history_c()
{
  lispval history_module;
  if (scheme_history_initialized) return;
  else scheme_history_initialized = 1;
  fd_init_scheme();
  history_module = fd_new_module("HISTORY",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);

  fd_idefn(history_module,fd_make_cprim1("%HISTREF",histref_prim,1));
  fd_idefn(history_module,fd_make_cprim0("%HISTORY",history_prim));
  fd_idefn(history_module,fd_make_cprim1("%HISTCLEAR",histclear_prim,0));

  history_symbol = fd_intern("%HISTORY");

  fd_finish_module(history_module);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
