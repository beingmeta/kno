/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"

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
#if KNO_FILECONFIG_ENABLED
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

#if KNO_USE_TLS
static u8_tld_key threadtable_key;
static lispval get_threadtable()
{
  lispval table = (lispval)u8_tld_get(threadtable_key);
  if (table) return table;
  else {
    table = kno_empty_slotmap();
    u8_tld_set(threadtable_key,(void*)table);
    return table;}
}
KNO_EXPORT lispval kno_init_threadtable(lispval init_table)
{
  lispval table = (lispval)u8_tld_get(threadtable_key);
  if (table)
    return table;
  else if (KNO_TABLEP(init_table)) {
    kno_incref(init_table);
    u8_tld_set(threadtable_key,(void*)init_table);
    return init_table;}
  else {
    table = kno_empty_slotmap();
    u8_tld_set(threadtable_key,(void*)table);
    return table;}
}
static void recycle_thread_table()
{
  lispval table = (lispval)u8_tld_get(threadtable_key);
  u8_tld_set(threadtable_key,(void*)NULL);
  if ((kno_exiting) && (kno_fast_exit)) return;
  if (table) kno_decref(table);
}
#define thread_table (get_threadtable())
#else
static lispval __thread thread_table = VOID;
static lispval get_threadtable()
{
  if (TABLEP(thread_table))
    return thread_table;
  else {
    lispval new_table = kno_empty_slotmap();
    thread_table = new_table;
    return new_table;}
}
KNO_EXPORT lispval kno_init_threadtable(lispval init_table)
{
  lispval table = thread_table;
  if (KNO_TABLEP(table))
    return table;
  else if (KNO_TABLEP(init_table)) {
    kno_incref(init_table);
    thread_table = init_table;
    return init_table;}
  else {
    lispval new_table = kno_empty_slotmap();
    thread_table = new_table;
    return new_table;}
}
static void recycle_thread_table()
{
  lispval table = thread_table;
  if ((kno_exiting) && (kno_fast_exit)) return;
  thread_table = VOID;
  if (table) kno_decref(table);
}
#endif

KNO_EXPORT void kno_reset_threadvars()
{
  lispval table = thread_table;
  if (KNO_SLOTMAPP(table))
    kno_reset_slotmap((kno_slotmap)table);
  else if (KNO_SCHEMAPP(table))
    kno_reset_schemap((kno_schemap)table);
  else if (KNO_HASHTABLEP(table))
    kno_reset_hashtable((kno_hashtable)table,-1,0);
  else {
    lispval init_table = kno_empty_slotmap();
    kno_init_threadtable(init_table);
    kno_decref(init_table);}
}


KNO_EXPORT lispval kno_thread_get(lispval var)
{
  return kno_get(get_threadtable(),var,VOID);
}

KNO_EXPORT int kno_thread_probe(lispval var)
{
  return kno_test(get_threadtable(),var,VOID);
}

KNO_EXPORT int kno_thread_set(lispval var,lispval val)
{
  return kno_store(get_threadtable(),var,val);
}

KNO_EXPORT int kno_thread_add(lispval var,lispval val)
{
  return kno_add(get_threadtable(),var,val);
}

/* Request objects */

#if KNO_USE_TLS
static u8_tld_key reqinfo_key;
static lispval try_reqinfo()
{
  lispval table = (lispval)u8_tld_get(reqinfo_key);
  if (table) return table;
  else return EMPTY;
}
static lispval get_reqinfo()
{
  lispval table = (lispval)u8_tld_get(reqinfo_key);
  if ((table)&&(TABLEP(table))) return table;
  else {
    lispval newinfo = kno_empty_slotmap();
    kno_slotmap sm = kno_consptr(kno_slotmap,newinfo,kno_slotmap_type);
    u8_write_lock(&(sm->table_rwlock)); 
    KNO_XTABLE_SET_USELOCK(sm,0);
    u8_tld_set(reqinfo_key,(void *)newinfo);
    return newinfo;}
}
static void set_reqinfo(lispval table)
{
  u8_tld_set(reqinfo_key,(void *)table);
}
#else
static lispval __thread reqinfo = VOID;
static lispval try_reqinfo()
{
  if ((reqinfo)&&(TABLEP(reqinfo))) return reqinfo;
  else return EMPTY;
}
static lispval get_reqinfo()
{
  if ((reqinfo)&&(TABLEP(reqinfo))) return reqinfo;
  else {
    lispval newinfo = kno_empty_slotmap();
    kno_slotmap sm = kno_consptr(kno_slotmap,newinfo,kno_slotmap_type);
    u8_write_lock(&(sm->table_rwlock));
    KNO_XTABLE_SET_USELOCK(sm,0);
    reqinfo = newinfo;
    return newinfo;}
}
static void set_reqinfo(lispval table)
{
  reqinfo = table;
}
#endif

KNO_EXPORT lispval kno_req(lispval var)
{
  return kno_get(try_reqinfo(),var,VOID);
}

KNO_EXPORT int kno_isreqlive()
{
  lispval info = try_reqinfo();
  if (TABLEP(info)) return 1; else return 0;
}

KNO_EXPORT lispval kno_req_get(lispval var,lispval dflt)
{
  lispval info = try_reqinfo();
  if (TABLEP(info)) return kno_get(info,var,dflt);
  else return kno_incref(dflt);
}

KNO_EXPORT int kno_req_store(lispval var,lispval val)
{
  lispval info = get_reqinfo();
  return kno_store(info,var,val);
}

KNO_EXPORT int kno_req_test(lispval var,lispval val)
{
  return kno_test(try_reqinfo(),var,val);
}

KNO_EXPORT int kno_req_add(lispval var,lispval val)
{
  return kno_add(get_reqinfo(),var,val);
}

KNO_EXPORT int kno_req_drop(lispval var,lispval val)
{
  return kno_drop(try_reqinfo(),var,val);
}

KNO_EXPORT int kno_req_push(lispval var,lispval val)
{
  lispval info = get_reqinfo(), cur = kno_get(info,var,NIL);
  lispval new_pair = kno_conspair(val,cur);
  kno_store(info,var,new_pair);
  kno_incref(val); kno_decref(new_pair);
  return 1;
}

KNO_EXPORT lispval kno_req_call(kno_reqfn reqfn)
{
  lispval info = get_reqinfo();
  return reqfn(info);
}

KNO_EXPORT void kno_use_reqinfo(lispval newinfo)
{
  lispval curinfo = try_reqinfo();
  if (curinfo == newinfo) return;
  if ((KNO_TRUEP(newinfo))&&(TABLEP(curinfo))) return;
  if (SLOTMAPP(curinfo)) {
    kno_slotmap sm = kno_consptr(kno_slotmap,curinfo,kno_slotmap_type);
    if (!(KNO_XTABLE_USELOCKP(sm))) {
      KNO_XTABLE_SET_USELOCK(sm,1);
      u8_rw_unlock(&(sm->table_rwlock));}}
  else if (HASHTABLEP(curinfo)) {
    kno_hashtable ht = kno_consptr(kno_hashtable,curinfo,kno_hashtable_type);
    if (!(KNO_XTABLE_USELOCKP(ht))) {
      KNO_XTABLE_SET_USELOCK(ht,1);
      u8_rw_unlock(&(ht->table_rwlock));}}
  else {}
  if ((FALSEP(newinfo))||
      (VOIDP(newinfo))||
      (EMPTYP(newinfo))) {
    kno_decref(curinfo);
    set_reqinfo(KNO_FALSE);
    return;}
  else if (KNO_TRUEP(newinfo)) {
    kno_slotmap sm; newinfo = kno_empty_slotmap();
    sm = kno_consptr(kno_slotmap,newinfo,kno_slotmap_type);
    u8_write_lock(&(sm->table_rwlock));
    KNO_XTABLE_SET_USELOCK(sm,0);}
  else if ((SLOTMAPP(newinfo))||(SLOTMAPP(curinfo)))
    kno_incref(newinfo);
  else {
    u8_log(LOG_CRIT,kno_TypeError,
           "USE_REQINFO arg isn't slotmap or table: %q",newinfo);
    kno_slotmap sm; newinfo = kno_empty_slotmap();
    sm = kno_consptr(kno_slotmap,newinfo,kno_slotmap_type);
    u8_write_lock(&(sm->table_rwlock));
    KNO_XTABLE_SET_USELOCK(sm,0);}
  if (SLOTMAPP(newinfo)) {
    kno_slotmap sm = kno_consptr(kno_slotmap,newinfo,kno_slotmap_type);
    u8_write_lock(&(sm->table_rwlock));
    KNO_XTABLE_SET_USELOCK(sm,0);}
  else if (HASHTABLEP(newinfo)) {
    kno_hashtable ht = kno_consptr(kno_hashtable,newinfo,kno_hashtable_type);
    u8_write_lock(&(ht->table_rwlock));
    KNO_XTABLE_SET_USELOCK(ht,0);}
  set_reqinfo(newinfo);
  kno_decref(curinfo);
}

KNO_EXPORT lispval kno_push_reqinfo(lispval newinfo)
{
  lispval curinfo = try_reqinfo();
  if (curinfo == newinfo) return curinfo;
  if (SLOTMAPP(curinfo)) {
    kno_slotmap sm = kno_consptr(kno_slotmap,curinfo,kno_slotmap_type);
    if (!(KNO_XTABLE_USELOCKP(sm))) {
      KNO_XTABLE_SET_USELOCK(sm,1);
      u8_rw_unlock(&(sm->table_rwlock));}}
  else if (HASHTABLEP(curinfo)) {
    kno_hashtable ht = kno_consptr(kno_hashtable,curinfo,kno_hashtable_type);
    if (!(KNO_XTABLE_USELOCKP(ht))) {
      KNO_XTABLE_SET_USELOCK(ht,1);
      u8_rw_unlock(&(ht->table_rwlock));}}
  if ((FALSEP(newinfo))||
      (VOIDP(newinfo))||
      (EMPTYP(newinfo))) {
    set_reqinfo(newinfo);
    return curinfo;}
  else if (KNO_TRUEP(newinfo)) newinfo = kno_empty_slotmap();
  else if ((SLOTMAPP(newinfo))||(SLOTMAPP(curinfo)))
    kno_incref(newinfo);
  else {
    u8_log(LOG_CRIT,kno_TypeError,
           "PUSH_REQINFO arg isn't slotmap or table: %q",newinfo);
    newinfo = kno_empty_slotmap();}
  if (SLOTMAPP(newinfo)) {
    kno_slotmap sm = kno_consptr(kno_slotmap,newinfo,kno_slotmap_type);
    u8_write_lock(&(sm->table_rwlock));
    KNO_XTABLE_SET_USELOCK(sm,0);}
  else if (HASHTABLEP(newinfo)) {
    kno_hashtable ht = kno_consptr(kno_hashtable,newinfo,kno_hashtable_type);
    u8_write_lock(&(ht->table_rwlock));
    KNO_XTABLE_SET_USELOCK(ht,0);}
  set_reqinfo(newinfo);
  return curinfo;
}

#if KNO_USE_TLS
static u8_tld_key reqlog_key;
KNO_EXPORT struct U8_OUTPUT *kno_try_reqlog()
{
  struct U8_OUTPUT *output = (struct U8_OUTPUT *)u8_tld_get(reqlog_key);
  return output;
}
KNO_EXPORT struct U8_OUTPUT *kno_get_reqlog()
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
KNO_EXPORT struct U8_OUTPUT *kno_try_reqlog()
{
  return reqlog;
}
KNO_EXPORT struct U8_OUTPUT *kno_get_reqlog()
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


KNO_EXPORT struct U8_OUTPUT *kno_reqlog(int force)
{
  if (force<0) {
    set_reqlog(NULL);
    return NULL;}
  else if (force) return kno_get_reqlog();
  else return kno_try_reqlog();
}

KNO_EXPORT int kno_reqlogger
(u8_condition c,u8_context cxt,u8_string message)
{
  struct U8_OUTPUT *out = kno_get_reqlog();
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

void kno_init_fluid_c()
{
  u8_register_source_file(_FILEINFO);

#if KNO_USE_TLS
  u8_new_threadkey(&threadtable_key,NULL);
  u8_new_threadkey(&reqinfo_key,NULL);
  u8_new_threadkey(&reqlog_key,NULL);
#endif

  u8_register_threadexit(recycle_thread_table);
  /* This is for the main thread. It's safe to call again because it
     resets the thread local variable */
  atexit(recycle_thread_table);
}

