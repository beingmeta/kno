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
#include "framerd/numbers.h"
#include "framerd/apply.h"

#include <libu8/u8signals.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <signal.h>
#include <sys/types.h>
#include <pwd.h>

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#if FD_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

#include <sys/time.h>

#if HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if HAVE_GRP_H
#include <grp.h>
#endif

/* Thread Tables */

#if FD_USE_TLS
static u8_tld_key threadtable_key;
static fdtype get_threadtable()
{
  fdtype table = (fdtype)u8_tld_get(threadtable_key);
  if (table) return table;
  else {
    table = fd_empty_slotmap();
    u8_tld_set(threadtable_key,(void*)table);
    return table;}
}
FD_EXPORT void fd_reset_threadvars()
{
  fdtype table = (fdtype)u8_tld_get(threadtable_key);
  fdtype new_table = fd_empty_slotmap();
  u8_tld_set(threadtable_key,(void*)new_table);
  if (table) fd_decref(table);
}
static void recycle_thread_table()
{
  fdtype table = (fdtype)u8_tld_get(threadtable_key);
  u8_tld_set(threadtable_key,(void*)NULL);
  if (table) fd_decref(table);
}
#else
static fdtype __thread thread_table = FD_VOID;
static fdtype get_threadtable()
{
  if (FD_TABLEP(thread_table)) return thread_table;
  else return (thread_table = fd_empty_slotmap());
}
FD_EXPORT void fd_reset_threadvars()
{
  fdtype table = thread_table;
  thread_table = fd_empty_slotmap();
  fd_decref(table);
}
static void recycle_thread_table()
{
  fdtype table = thread_table;
  thread_table = FD_VOID;
  if (table) fd_decref(table);
}
#endif

FD_EXPORT fdtype fd_thread_get(fdtype var)
{
  return fd_get(get_threadtable(),var,FD_VOID);
}

FD_EXPORT int fd_thread_set(fdtype var,fdtype val)
{
  return fd_store(get_threadtable(),var,val);
}

FD_EXPORT int fd_thread_add(fdtype var,fdtype val)
{
  return fd_add(get_threadtable(),var,val);
}

/* Request objects */

#if FD_USE_TLS
static u8_tld_key reqinfo_key;
static fdtype try_reqinfo()
{
  fdtype table = (fdtype)u8_tld_get(reqinfo_key);
  if (table) return table;
  else return FD_EMPTY_CHOICE;
}
static fdtype get_reqinfo()
{
  fdtype table = (fdtype)u8_tld_get(reqinfo_key);
  if ((table)&&(FD_TABLEP(table))) return table;
  else {
    fdtype newinfo = fd_empty_slotmap();
    fd_slotmap sm = fd_consptr(fd_slotmap,newinfo,fd_slotmap_type);
    u8_write_lock(&(sm->table_rwlock)); sm->table_uselock = 0;
    u8_tld_set(reqinfo_key,(void *)newinfo);
    return newinfo;}
}
static void set_reqinfo(fdtype table)
{
  u8_tld_set(reqinfo_key,(void *)table);
}
#else
static fdtype __thread reqinfo = FD_VOID;
static fdtype try_reqinfo()
{
  if ((reqinfo)&&(FD_TABLEP(reqinfo))) return reqinfo;
  else return FD_EMPTY_CHOICE;
}
static fdtype get_reqinfo()
{
  if ((reqinfo)&&(FD_TABLEP(reqinfo))) return reqinfo;
  else {
    fdtype newinfo = fd_empty_slotmap();
    fd_slotmap sm = fd_consptr(fd_slotmap,newinfo,fd_slotmap_type);
    u8_write_lock(&(sm->table_rwlock)); sm->table_uselock = 0;
    reqinfo = newinfo;
    return newinfo;}
}
static void set_reqinfo(fdtype table)
{
  reqinfo = table;
}
#endif

FD_EXPORT fdtype fd_req(fdtype var)
{
  return fd_get(try_reqinfo(),var,FD_VOID);
}

FD_EXPORT int fd_isreqlive()
{
  fdtype info = try_reqinfo();
  if (FD_TABLEP(info)) return 1; else return 0;
}

FD_EXPORT fdtype fd_req_get(fdtype var,fdtype dflt)
{
  fdtype info = try_reqinfo();
  if (FD_TABLEP(info)) return fd_get(info,var,dflt);
  else return fd_incref(dflt);
}

FD_EXPORT int fd_req_store(fdtype var,fdtype val)
{
  fdtype info = get_reqinfo();
  return fd_store(info,var,val);
}

FD_EXPORT int fd_req_test(fdtype var,fdtype val)
{
  return fd_test(try_reqinfo(),var,val);
}

FD_EXPORT int fd_req_add(fdtype var,fdtype val)
{
  return fd_add(get_reqinfo(),var,val);
}

FD_EXPORT int fd_req_drop(fdtype var,fdtype val)
{
  return fd_drop(try_reqinfo(),var,val);
}

FD_EXPORT int fd_req_push(fdtype var,fdtype val)
{
  fdtype info = get_reqinfo(), cur = fd_get(info,var,FD_EMPTY_LIST);
  fdtype new_pair = fd_conspair(val,cur);
  fd_store(info,var,new_pair);
  fd_incref(val); fd_decref(new_pair);
  return 1;
}

FD_EXPORT fdtype fd_req_call(fd_reqfn reqfn)
{
  fdtype info = get_reqinfo();
  return reqfn(info);
}

FD_EXPORT void fd_use_reqinfo(fdtype newinfo)
{
  fdtype curinfo = try_reqinfo();
  if (curinfo == newinfo) return;
  if ((FD_TRUEP(newinfo))&&(FD_TABLEP(curinfo))) return;
  if (FD_SLOTMAPP(curinfo)) {
    fd_slotmap sm = fd_consptr(fd_slotmap,curinfo,fd_slotmap_type);
    if (sm->table_uselock==0) {
      sm->table_uselock = 1;
      u8_rw_unlock(&(sm->table_rwlock));}}
  else if (FD_HASHTABLEP(curinfo)) {
    fd_hashtable ht = fd_consptr(fd_hashtable,curinfo,fd_hashtable_type);
    if (ht->table_uselock==0) {
      ht->table_uselock = 1;
      u8_rw_unlock(&(ht->table_rwlock));}}
  else {}
  if ((FD_FALSEP(newinfo))||
      (FD_VOIDP(newinfo))||
      (FD_EMPTY_CHOICEP(newinfo))) {
    fd_decref(curinfo);
    set_reqinfo(newinfo);
    return;}
  else if (FD_TRUEP(newinfo)) {
    fd_slotmap sm; newinfo = fd_empty_slotmap();
    sm = fd_consptr(fd_slotmap,newinfo,fd_slotmap_type);
    u8_write_lock(&(sm->table_rwlock)); sm->table_uselock = 0;}
  else if ((FD_SLOTMAPP(newinfo))||(FD_SLOTMAPP(curinfo)))
    fd_incref(newinfo);
  else {
    u8_log(LOG_CRIT,fd_TypeError,
           "USE_REQINFO arg isn't slotmap or table: %q",newinfo);
    fd_slotmap sm; newinfo = fd_empty_slotmap();
    sm = fd_consptr(fd_slotmap,newinfo,fd_slotmap_type);
    u8_write_lock(&(sm->table_rwlock)); sm->table_uselock = 0;}
  if (FD_SLOTMAPP(newinfo)) {
    fd_slotmap sm = fd_consptr(fd_slotmap,newinfo,fd_slotmap_type);
    u8_write_lock(&(sm->table_rwlock));
    sm->table_uselock = 0;}
  else if (FD_HASHTABLEP(newinfo)) {
    fd_hashtable ht = fd_consptr(fd_hashtable,newinfo,fd_hashtable_type);
    u8_write_lock(&(ht->table_rwlock));
    ht->table_uselock = 0;}
  set_reqinfo(newinfo);
  fd_decref(curinfo);
}

FD_EXPORT fdtype fd_push_reqinfo(fdtype newinfo)
{
  fdtype curinfo = try_reqinfo();
  if (curinfo == newinfo) return curinfo;
  if (FD_SLOTMAPP(curinfo)) {
    fd_slotmap sm = fd_consptr(fd_slotmap,curinfo,fd_slotmap_type);
    if (sm->table_uselock==0) {
      sm->table_uselock = 1;
      u8_rw_unlock(&(sm->table_rwlock));}}
  else if (FD_HASHTABLEP(curinfo)) {
    fd_hashtable ht = fd_consptr(fd_hashtable,curinfo,fd_hashtable_type);
    if (ht->table_uselock==0) {
      ht->table_uselock = 1;
      u8_rw_unlock(&(ht->table_rwlock));}}
  if ((FD_FALSEP(newinfo))||
      (FD_VOIDP(newinfo))||
      (FD_EMPTY_CHOICEP(newinfo))) {
    set_reqinfo(newinfo);
    return curinfo;}
  else if (FD_TRUEP(newinfo)) newinfo = fd_empty_slotmap();
  else if ((FD_SLOTMAPP(newinfo))||(FD_SLOTMAPP(curinfo)))
    fd_incref(newinfo);
  else {
    u8_log(LOG_CRIT,fd_TypeError,
           "PUSH_REQINFO arg isn't slotmap or table: %q",newinfo);
    newinfo = fd_empty_slotmap();}
  if (FD_SLOTMAPP(newinfo)) {
    fd_slotmap sm = fd_consptr(fd_slotmap,newinfo,fd_slotmap_type);
    u8_write_lock(&(sm->table_rwlock));
    sm->table_uselock = 0;}
  else if (FD_HASHTABLEP(newinfo)) {
    fd_hashtable ht = fd_consptr(fd_hashtable,newinfo,fd_hashtable_type);
    u8_write_lock(&(ht->table_rwlock));
    ht->table_uselock = 0;}
  set_reqinfo(newinfo);
  return curinfo;
}

#if FD_USE_TLS
static u8_tld_key reqlog_key;
FD_EXPORT struct U8_OUTPUT *fd_try_reqlog()
{
  struct U8_OUTPUT *table = (struct U8_OUTPUT *)u8_tld_get(reqlog_key);
  if (table) return table;
  else return NULL;
}
FD_EXPORT struct U8_OUTPUT *fd_get_reqlog()
{
  struct U8_OUTPUT *log = (struct U8_OUTPUT *)u8_tld_get(reqlog_key);
  if (log) return log;
  else {
    struct U8_OUTPUT *newlog = u8_open_output_string(1024);
    u8_tld_set(reqlog_key,(void *)newlog);
    return newlog;}
}
static void set_reqlog(struct U8_OUTPUT *stream)
{
  struct U8_OUTPUT *log = (struct U8_OUTPUT *)u8_tld_get(reqlog_key);
  if (stream == log) return;
  else if (log) u8_close_output(log);
  u8_tld_set(reqlog_key,(void *)stream);
}
#else
static struct U8_OUTPUT __thread *reqlog = NULL;
FD_EXPORT struct U8_OUTPUT *fd_try_reqlog()
{
  return reqlog;
}
FD_EXPORT struct U8_OUTPUT *fd_get_reqlog()
{
  if (reqlog) return reqlog;
  else {
    struct U8_OUTPUT *newlog = u8_open_output_string(1024);
    reqlog = newlog;
    return newlog;}
}
static void set_reqlog(struct U8_OUTPUT *stream)
{
  if (reqlog == stream) return;
  else {
    if (reqlog) u8_close_output(reqlog);
    reqlog = stream;}
}
#endif


FD_EXPORT struct U8_OUTPUT *fd_reqlog(int force)
{
  if (force<0) {
    set_reqlog(NULL);
    return NULL;}
  else if (force) return fd_get_reqlog();
  else return fd_try_reqlog();
}

FD_EXPORT int fd_reqlogger
  (u8_condition c,u8_context cxt,u8_string message)
{
  struct U8_OUTPUT *out = fd_get_reqlog();
  if (!(out)) return 0;
  if ((c)&&(cxt))
    u8_printf(out,"[%l*t] (%s) @%s %s",c,cxt,message);
  else if (c)
    u8_printf(out,"[%l*t] (%s) %s",c,message);
  else if (cxt)
    u8_printf(out,"[%l*t] @%s %s",cxt,message);
  else u8_printf(out,"[%l*t] %s",message);
  return 1;
}

void fd_init_fluid_c()
{
  u8_register_source_file(_FILEINFO);

#if FD_USE_TLS
  u8_new_threadkey(&threadtable_key,NULL);
  u8_new_threadkey(&reqinfo_key,NULL);
  u8_new_threadkey(&reqlog_key,NULL);
#endif

  u8_register_threadexit(recycle_thread_table);
}

