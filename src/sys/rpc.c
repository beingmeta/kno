/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2016 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_SOURCE 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/rpc.h"
#include "kno/apply.h"

#include <libu8/libu8io.h>
#include <libu8/u8filefns.h>

#include <errno.h>
#include <math.h>

static KNO_HASHTABLE rpc_registry;

KNO_EXPORT int kno_register_rpc(kno_rpc pt)
{
  u8_string spec = pt->rpc_spec;
  if (!spec) {
    u8_seterr("BadRPC","kno_register_rpc",u8_strdup(pt->rpc_type));
    return -1;}
  struct KNO_STRING tmp_string;
  kno_init_string(&tmp_string,-1,spec);
  /* Make it static */
  KNO_SET_REFCOUNT(&tmp_string,0);
  lispval known = kno_hashtable_get
    (&rpc_registry,(lispval)&tmp_string,KNO_VOID);
  if (KNO_VOIDP(known)) {
    lispval key = knostring(spec);
    int rv = kno_hashtable_store(&rpc_registry,key,(lispval)pt);
    if (rv<0) return rv;
    kno_decref(key);
    return 1;}
  else if ( known == ((lispval)pt) )
    return 0;
  else {
    u8_seterr("ExistingRPC","kno_register_rpc",u8_strdup(spec));
    return -1;}
}

KNO_EXPORT
int kno_deregister_rpc(kno_rpc pt)
{
  u8_string spec = pt->rpc_spec;
  if (!spec) {
    u8_seterr("BadRPC","kno_deregister_rpc",u8_strdup(pt->rpc_type));
    return -1;}
  struct KNO_STRING tmp_string;
  kno_init_string(&tmp_string,-1,spec);
  /* Make it static */
  KNO_SET_REFCOUNT(&tmp_string,0);
  lispval known = kno_hashtable_get
    (&rpc_registry,(lispval)&tmp_string,KNO_VOID);
  if (KNO_VOIDP(known))
    return 0;
  else if ( known == ((lispval)pt) ) {
    int rv = kno_hashtable_drop(&rpc_registry,(lispval)&tmp_string,KNO_VOID);
    if (rv<0) return rv;
    else return 1;}
  else {
    u8_seterr("NotRegistered","kno_deregister_rpc",u8_strdup(spec));
    return -1;}
}

KNO_EXPORT
struct KNO_RPC *kno_lookup_rpc(u8_string spec)
{
  struct KNO_STRING tmp_string;
  kno_init_string(&tmp_string,-1,spec);
  /* Make it static */
  KNO_SET_REFCOUNT(&tmp_string,0);
  lispval known = kno_hashtable_get
    (&rpc_registry,(lispval)&tmp_string,KNO_VOID);
  if (VOIDP(known))
    return NULL;
  else if (KNO_TYPEP(known,kno_rpc_type))
    return (struct KNO_RPC *) known;
  else {
    kno_seterr("BadRPC","kno_lookup_rpc",spec,known);
    kno_decref(known);
    return NULL;}
}

KNO_EXPORT int kno_release_rpc(struct KNO_RPC *rpc)
{
  if (rpc == NULL) {
    u8_seterr("InvalidRPC","kno_release_rpc","NULL");
    return -1;}
  lispval lval = (lispval) rpc;
  if (TYPEP(lval,kno_rpc_type)) {
    kno_decref(lval);
    return 1;}
  else return kno_err("InvalidRPC","kno_release_rpc",NULL,lval);
}

KNO_EXPORT
lispval kno_rpc_apply(kno_rpc pt,lispval m,int n,lispval *args)
{
  rpc_handlers handlers = pt->rpc_handlers;
  if ( (handlers) && (handlers->rpc_apply) )
    return handlers->rpc_apply(pt,m,n,args);
  else return kno_err("MissingMethod","rpc_apply",
		      (SYMBOLP(m)) ? (SYMBOL_NAME(m)) :
		      (STRINGP(m)) ? (CSTRING(m)) :
		      (NULL),
		      (lispval) pt);
}

KNO_EXPORT
lispval kno_rpc_call(kno_rpc pt,lispval method,int n,...)
{
  int i = 0; va_list arglist;
  lispval args[n], result;
  va_start(arglist,n);
  while (i<n) args[i++]=va_arg(arglist,lispval);
  return kno_rpc_apply(pt,method,n,args);
}

KNO_EXPORT
lispval kno_rpc_ccall(kno_rpc pt,u8_string m,u8_string args,...)
{
  return KNO_VOID;
}

KNO_EXPORT void kno_init_rpc_c()
{
  kno_type_names[kno_rpc_type]="rpc-point";

  kno_make_hashtable(&rpc_registry,128);

  u8_register_source_file(_FILEINFO);
}
