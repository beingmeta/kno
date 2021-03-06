/* Mode: C; Character-encoding: utf-8; -*- */

/* Copyright 2004-2020 beingmeta, inc.
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
static u8_mutex ctypeinfo_lock;

kno_subtypefn kno_subtypefns[KNO_TYPE_MAX] = { NULL };
struct KNO_TYPEINFO *kno_ctypeinfo[KNO_TYPE_MAX] = { NULL };

kno_type_dumpfn kno_default_dumpfn = NULL;
kno_type_restorefn kno_default_restorefn = NULL;

/* Default objtype handler */

KNO_EXPORT struct KNO_TYPEINFO *_kno_objtype(lispval obj)
{
  return kno_objtype(obj);
}

/* Typeinfo */

static lispval getctypeinfo(lispval ctype)
{
  int off = KNO_IMMEDIATE_DATA(ctype);
  if (RARELY(off >= KNO_TYPE_MAX)) return KNO_VOID;
  struct KNO_TYPEINFO *info = kno_ctypeinfo[off];
  if (info) return (lispval) info; else return KNO_VOID;
}

static int setctypeinfo(lispval ctype,struct KNO_TYPEINFO *info)
{
  int off = KNO_IMMEDIATE_DATA(ctype);
  if (RARELY(off >= KNO_TYPE_MAX)) return 0;
  u8_lock_mutex(&ctypeinfo_lock);
  struct KNO_TYPEINFO *cur = kno_ctypeinfo[off];
  int rv = (cur == NULL);
  if (cur == NULL) kno_ctypeinfo[off]=info;
  u8_unlock_mutex(&ctypeinfo_lock);
  return rv;
}

KNO_EXPORT struct KNO_TYPEINFO *kno_probe_typeinfo(lispval tag)
{
  if (KNO_TYPEP(tag,kno_ctype_type)) {
    int off = KNO_IMMEDIATE_DATA(tag);
    if (RARELY(off >= KNO_TYPE_MAX)) return NULL;
    else return kno_ctypeinfo[off];}
  lispval v = kno_hashtable_get(&typeinfo,tag,KNO_FALSE);
  if (KNO_TYPEP(v,kno_typeinfo_type))
    return (kno_typeinfo) v;
  else return NULL;
}

KNO_EXPORT struct KNO_TYPEINFO *kno_use_typeinfo(lispval tag)
{
  if (RARELY( (KNO_TYPEP(tag,kno_ctype_type)) &&
	      ( (KNO_IMMEDIATE_DATA(tag)) >= KNO_TYPE_MAX) ))
    return NULL;
  lispval exists = (KNO_TYPEP(tag,kno_ctype_type)) ? (getctypeinfo(tag)) :
    (kno_hashtable_get(&typeinfo,tag,KNO_VOID));
  if (KNO_VOIDP(exists)) {
    struct KNO_TYPEINFO *info = u8_alloc(struct KNO_TYPEINFO);
    KNO_INIT_STATIC_CONS(info,kno_typeinfo_type);
    info->typetag = tag; kno_incref(tag);
    info->type_props = kno_make_slotmap(2,0,NULL);
    if ( (KNO_OIDP(tag)) || (KNO_SYMBOLP(tag)) )
      info->type_usetag=tag;
    else if (KNO_TYPEP(tag,kno_ctype_type)) {
      u8_string name = kno_type2name((kno_lisp_type)(KNO_IMMEDIATE_DATA(tag)));
      u8_byte buf[strlen(name)+17];
      info->type_usetag=kno_intern(u8_bprintf(buf,"kno:basetype:%s",name));}
    else {
      U8_STATIC_OUTPUT(tagid,42);
      kno_unparse(tagidout,tag);
      info->type_usetag=kno_stream_string(tagidout);
      u8_close_output(tagidout);}
    info->type_name = (KNO_SYMBOLP(tag)) ? (KNO_SYMBOL_NAME(tag)) :
      (KNO_STRINGP(tag)) ? (u8_strdup(KNO_CSTRING(tag))) :
      (KNO_TYPEP(tag,kno_ctype_type)) ?
      (u8_strdup(kno_type2name((kno_lisp_type)(KNO_IMMEDIATE_DATA(tag))))) :
      (kno_lisp2string(tag));
    info->type_description = NULL;
    int rv = (KNO_TYPEP(tag,kno_ctype_type)) ? (setctypeinfo(tag,info)) :
      (kno_hashtable_op(&typeinfo,kno_table_init,tag,((lispval)info)));
    if (rv > 0)
      return info;
    else {
      kno_decref(info->typetag);
      kno_decref(info->type_props);
      u8_free(info);
      if (rv < 0)
	return NULL;
      else {
	lispval useval = (KNO_TYPEP(tag,kno_ctype_type)) ?
	  (getctypeinfo(tag)) :
	  (kno_hashtable_get(&typeinfo,tag,KNO_VOID));
	if (KNO_TYPEP(useval,kno_typeinfo_type))
	  return (kno_typeinfo) useval;
	else return NULL;}}}
  else return (kno_typeinfo) exists;
}

static void recycle_typeinfo(struct KNO_RAW_CONS *c)
{
  struct KNO_TYPEINFO *typeinfo = (struct KNO_TYPEINFO *)c;
  kno_decref(typeinfo->type_props); typeinfo->type_props=KNO_VOID;
  if (typeinfo->type_schema) {
    kno_decref(typeinfo->type_schema);
    typeinfo->type_schema=KNO_VOID;}
  if (typeinfo->type_tablefns) {
    if (typeinfo->type_free_tablefns)
      u8_free(typeinfo->type_tablefns);
    typeinfo->type_tablefns=NULL;}
  if (typeinfo->type_seqfns) {
    if (typeinfo->type_free_seqfns)
      u8_free(typeinfo->type_seqfns);
    typeinfo->type_seqfns=NULL;}
  if (typeinfo->type_name) {
    u8_free(typeinfo->type_name);
    typeinfo->type_name=NULL;}
  if (typeinfo->type_description) {
    u8_free(typeinfo->type_description);
    typeinfo->type_description=NULL;}
  kno_decref(typeinfo->type_usetag);
  if (typeinfo->type_defdata) {
    if (typeinfo->type_free_defdata)
      u8_free(typeinfo->type_defdata);
    typeinfo->type_defdata=NULL;}
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
int kno_set_dispatchfn(lispval tag,kno_type_dispatchfn fn)
{
  struct KNO_TYPEINFO *info = kno_use_typeinfo(tag);
  if (info) {
    info->type_dispatchfn = fn;
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

static void release_typeinfo()
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
  i=0; while (i<KNO_TYPE_MAX) {
    struct KNO_TYPEINFO *info = kno_ctypeinfo[i];
    if (info) {
      kno_ctypeinfo[i]=NULL;
      recycle_typeinfo((struct KNO_RAW_CONS *)info);}
    i++;}
}

void kno_init_typeinfo_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&ctypeinfo_lock);
  kno_init_hashtable(&typeinfo,231,NULL);

  kno_tablefns[kno_typeinfo_type] = &typeinfo_tablefns;
  kno_recyclers[kno_typeinfo_type]=recycle_typeinfo;

  atexit(release_typeinfo);
}
