/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_OBJTYPE 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/tables.h"
#include "kno/cons.h"
#include "kno/compounds.h"

#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>

#include <stdarg.h>

static struct KNO_HASHTABLE typeinfo;

kno_type_dumpfn kno_default_dumpfn = NULL;
kno_type_restorefn kno_default_restorefn = NULL;

/* Default objtype handler */

KNO_EXPORT struct KNO_TYPEINFO *_kno_objtype(lispval obj)
{
  return kno_objtype(obj);
}

/* Typeinfo */

static struct KNO_HASHTABLE typeinfo;

KNO_EXPORT struct KNO_TYPEINFO *kno_probe_typeinfo(lispval tag)
{
  lispval v = kno_hashtable_get(&typeinfo,tag,KNO_FALSE);
  if (KNO_TYPEP(v,kno_typeinfo_type))
    return (kno_typeinfo) v;
  else return NULL;
}

KNO_EXPORT struct KNO_TYPEINFO *kno_use_typeinfo(lispval tag)
{
  lispval exists = kno_hashtable_get(&typeinfo,tag,KNO_VOID);
  if (KNO_VOIDP(exists)) {
    struct KNO_TYPEINFO *info = u8_alloc(struct KNO_TYPEINFO);
    KNO_INIT_STATIC_CONS(info,kno_typeinfo_type);
    info->typetag = tag; kno_incref(tag);
    info->type_props = kno_make_slotmap(2,0,NULL);
    info->type_name = (KNO_SYMBOLP(tag)) ? (KNO_SYMBOL_NAME(tag)) :
      (KNO_STRINGP(tag)) ? (KNO_CSTRING(tag)) :
      (kno_lisp2string(tag));
    info->type_description = NULL;
    int rv = kno_hashtable_op(&typeinfo,kno_table_init,tag,((lispval)info));
    if (rv > 0)
      return info;
    else {
      kno_decref(info->typetag);
      kno_decref(info->type_props);
      kno_decref(info->type_props);
      u8_free(info);
      if (rv < 0)
	return NULL;
      else {
	lispval useval = kno_hashtable_get(&typeinfo,tag,KNO_FALSE);
	if (KNO_TYPEP(useval,kno_typeinfo_type))
	  return (kno_typeinfo) useval;
	else return NULL;}}}
  else return (kno_typeinfo) exists;
}

static void recycle_typeinfo(struct KNO_RAW_CONS *c)
{
  struct KNO_TYPEINFO *typeinfo = (struct KNO_TYPEINFO *)c;
  kno_decref(typeinfo->type_props); typeinfo->type_props=KNO_VOID;
  if (typeinfo->type_tablefns) {
    u8_free(typeinfo->type_tablefns);
    typeinfo->type_tablefns=NULL;}
  if (typeinfo->type_seqfns) {
    u8_free(typeinfo->type_seqfns);
    typeinfo->type_seqfns=NULL;}
  if (typeinfo->type_name) {
    u8_free(typeinfo->type_name);
    typeinfo->type_name=NULL;}
  if (typeinfo->type_description) {
    u8_free(typeinfo->type_description);
    typeinfo->type_description=NULL;}
  u8_free(typeinfo);
}

/* Setting C handlers for a type */

KNO_EXPORT
int kno_set_unparsefn(lispval tag,kno_type_unparsefn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_unparsefn = fn;
    return 1;}
  else return 0;
}

KNO_EXPORT
int kno_set_freefn(lispval tag,kno_type_freefn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_freefn = fn;
    return 1;}
  else return 0;
}

KNO_EXPORT
int kno_set_parsefn(lispval tag,kno_type_consfn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_consfn = fn;
    return 1;}
  else return 0;
}

KNO_EXPORT
int kno_set_testfn(lispval tag,kno_type_testfn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_testfn = fn;
    return 1;}
  else return 0;
}

KNO_EXPORT
int kno_set_dumpfn(lispval tag,kno_type_dumpfn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_dumpfn = fn;
    return 1;}
  else return 0;
}

KNO_EXPORT
int kno_set_restorefn(lispval tag,kno_type_restorefn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_restorefn = fn;
    return 1;}
  else return 0;
}

/* Typeinfo table functions */

static lispval typeinfo_get(lispval info,lispval key,lispval dflt)
{
  struct KNO_TYPEINFO *typeinfo = (kno_typeinfo) info;
  return kno_get(typeinfo->type_props,key,dflt);
}

static int typeinfo_store(lispval info,lispval key,lispval val)
{
  struct KNO_TYPEINFO *typeinfo = (kno_typeinfo) info;
  return kno_store(typeinfo->type_props,key,val);
}

static int typeinfo_add(lispval info,lispval key,lispval val)
{
  struct KNO_TYPEINFO *typeinfo = (kno_typeinfo) info;
  return kno_add(typeinfo->type_props,key,val);
}

static int typeinfo_drop(lispval info,lispval key,lispval val)
{
  struct KNO_TYPEINFO *typeinfo = (kno_typeinfo) info;
  return kno_drop(typeinfo->type_props,key,val);
}

static int typeinfo_test(lispval info,lispval key,lispval val)
{
  struct KNO_TYPEINFO *typeinfo = (kno_typeinfo) info;
  return kno_test(typeinfo->type_props,key,val);
}

static lispval typeinfo_getkeys(lispval info)
{
  struct KNO_TYPEINFO *typeinfo = (kno_typeinfo) info;
  return kno_getkeys(typeinfo->type_props);
}

static struct KNO_TABLEFNS typeinfo_tablefns =
  {
   typeinfo_get,
   typeinfo_store,
   typeinfo_add,
   typeinfo_drop,
   typeinfo_test,
   NULL,
   NULL,
   NULL,
   NULL,
   typeinfo_getkeys,
   NULL,
   NULL,
   NULL};

static void recycle_typeinfo_map()
{
  unsigned int n = typeinfo.ht_n_buckets;
  struct KNO_HASH_BUCKET **buckets = typeinfo.ht_buckets;
  unsigned int i = 0; while (i<n) {
    struct KNO_HASH_BUCKET *bucket = buckets[i];
    if (bucket) {
      int bucket_width = bucket->bucket_len;
      struct KNO_KEYVAL *kv = &(bucket->kv_val0);
      int j = 0; while (j<bucket_width) {
	lispval key = kv[j].kv_key, val = kv[j].kv_val;
	if (KNO_STATICP(val)) {
	  if (KNO_TYPEP(val,kno_typeinfo_type))
	    recycle_typeinfo((struct KNO_RAW_CONS *)val);}
	else kno_decref(val);
	kno_decref(key);
	j++;}
      buckets[i]=bucket;
      u8_free(bucket);}
    i++;}
  u8_free(buckets);
  memset(&typeinfo,0,sizeof(struct KNO_HASHTABLE));
}

void kno_init_typeinfo_c()
{
  u8_register_source_file(_FILEINFO);

  kno_tablefns[kno_typeinfo_type] = &typeinfo_tablefns;
  kno_recyclers[kno_typeinfo_type]=recycle_typeinfo;

  kno_init_hashtable(&typeinfo,231,NULL);

  atexit(recycle_typeinfo_map);
}
