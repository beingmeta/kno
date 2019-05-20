/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/storage.h"
#include "kno/dtcall.h"
#include "kno/drivers.h"

#include "headers/netindex.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

static struct KNO_INDEX_HANDLER netindex_handler;

static lispval boundp, quote;
static lispval iserver_writable, iserver_fetchkeys, iserver_fetch, iserver_fetchsize, iserver_fetchn;
static lispval iserver_add, iserver_drop, iserver_addn, iserver_changes, iserver_reset;

static lispval ixserver_writable, ixserver_fetchkeys, ixserver_fetch, ixserver_fetchsize, ixserver_fetchn;
static lispval ixserver_changes, ixserver_add, ixserver_drop, ixserver_addn, ixserver_reset;

static lispval set_symbol, drop_symbol;

u8_condition kno_NoServerMethod=_("Server doesn't support method");

static int server_supportsp(struct KNO_NETWORK_INDEX *ni,lispval operation)
{
  lispval request=
    kno_conspair(boundp,kno_conspair(operation,NIL));
  lispval response = kno_dteval(ni->index_connpool,request);
  kno_decref(request);
  if ((FALSEP(response)) || (KNO_ABORTP(response))) return 0;
  else {kno_decref(response); return 1;}
}

KNO_EXPORT kno_index kno_open_network_index(u8_string spec,
                                         kno_storage_flags flags,
                                         lispval opts)
{
  struct KNO_NETWORK_INDEX *ix;
  lispval writable_response; u8_string xid = NULL;
  u8_connpool cp = u8_open_connpool(spec,kno_dbconn_reserve_default,
                                    kno_dbconn_cap_default,
                                    kno_dbconn_init_default);
  if (cp == NULL) return NULL;
  ix = u8_alloc(struct KNO_NETWORK_INDEX); memset(ix,0,sizeof(*ix));
  kno_init_index((kno_index)ix,&netindex_handler,
                spec,xid,xid,
                flags,KNO_VOID,opts);
  ix->index_connpool = cp; ix->xname = VOID;
  writable_response = kno_dtcall(ix->index_connpool,1,iserver_writable);
  if ((KNO_ABORTP(writable_response))||
      (!(FALSEP(writable_response))))
    U8_SETBITS(ix->index_flags,(KNO_STORAGE_READ_ONLY));
  else U8_CLEARBITS(ix->index_flags,(KNO_STORAGE_READ_ONLY));
  kno_decref(writable_response);

  ix->capabilities = 0;
  if (server_supportsp(ix,iserver_fetchn)) ix->capabilities |= KNO_ISERVER_FETCHN;
  if (server_supportsp(ix,iserver_addn)) ix->capabilities |= KNO_ISERVER_ADDN;
  if (server_supportsp(ix,iserver_drop)) ix->capabilities |= KNO_ISERVER_DROP;
  if (server_supportsp(ix,iserver_reset)) ix->capabilities |= KNO_ISERVER_RESET;

  if ((ix)&&(!(U8_BITP(flags,KNO_STORAGE_UNREGISTERED))))
    kno_register_index((kno_index)ix);

  return (kno_index) ix;
}

static lispval netindex_fetch(kno_index ix,lispval key)
{
  struct KNO_NETWORK_INDEX *nix = (struct KNO_NETWORK_INDEX *)ix;
  if (VOIDP(nix->xname))
    return kno_dtcall(nix->index_connpool,2,iserver_fetch,key);
  else return kno_dtcall_x(nix->index_connpool,3,3,
                          ixserver_fetch,nix->xname,key);
}

static int netindex_fetchsize(kno_index ix,lispval key)
{
  struct KNO_NETWORK_INDEX *nix = (struct KNO_NETWORK_INDEX *)ix;
  lispval result;
  if (VOIDP(nix->xname))
    result = kno_dtcall(nix->index_connpool,2,iserver_fetchsize,key);
  else result = kno_dtcall_x(nix->index_connpool,3,3,
                            ixserver_fetchsize,nix->xname,key);
  if (KNO_ABORTP(result))
    return -1;
  else return kno_getint(result);
}

static lispval *netindex_fetchn(kno_index ix,int n,const lispval *keys)
{
  struct KNO_NETWORK_INDEX *nix = (struct KNO_NETWORK_INDEX *)ix;
  lispval vector, result;
  vector = kno_wrap_vector(n,(lispval *)keys);
  if (VOIDP(nix->xname))
    result = kno_dtcall(nix->index_connpool,2,iserver_fetchn,vector);
  else result = kno_dtcall_x(nix->index_connpool,3,3,
                            ixserver_fetchn,nix->xname,vector);
  if (KNO_ABORTP(result)) return NULL;
  else if (VECTORP(result)) {
    lispval *results = u8_alloc_n(n,lispval);
    memcpy(results,KNO_VECTOR_ELTS(result),LISPVEC_BYTELEN(n));
    return results;}
  else {
    kno_seterr(kno_BadServerResponse,"netindex_fetchn",ix->indexid,
              kno_incref(result));
    return NULL;}
}

static lispval *netindex_fetchkeys(kno_index ix,int *n)
{
  struct KNO_NETWORK_INDEX *nix = (struct KNO_NETWORK_INDEX *)ix;
  lispval result;
  if (VOIDP(nix->xname))
    result = kno_dtcall(nix->index_connpool,1,iserver_fetchkeys);
  else result = kno_dtcall(nix->index_connpool,3,2,ixserver_fetchkeys,nix->xname);
  if (KNO_ABORTP(result)) {
    *n = -1;
    kno_interr(result);
    return NULL;}
  if (CHOICEP(result)) {
    int size = KNO_CHOICE_SIZE(result);
    lispval *dtypes = u8_alloc_n(size,lispval);
    memcpy(dtypes,KNO_CHOICE_DATA(result),LISPVEC_BYTELEN(size));
    *n = size;
    u8_free((kno_cons)result);
    return dtypes;}
  else {
    lispval *dtypes = u8_alloc_n(1,lispval); *n = 1;
    dtypes[0]=result;
    return dtypes;}
}

static int netindex_save(struct KNO_INDEX *ix,
                         struct KNO_CONST_KEYVAL *adds,int n_adds,
                         struct KNO_CONST_KEYVAL *drops,int n_drops,
                         struct KNO_CONST_KEYVAL *stores,int n_stores,
                         lispval changed_metadata)
{
  struct KNO_NETWORK_INDEX *nix = (struct KNO_NETWORK_INDEX *)ix;
  lispval xname = nix->xname;
  int n_transactions = 0;

  if (n_stores) {
    if (nix->capabilities&KNO_ISERVER_RESET) {
      int i = 0; while ( i< n_stores ) {
        lispval result = VOID;
        lispval key = stores[i].kv_key;
        lispval val = stores[i].kv_val;
        if (VOIDP(xname))
          result = kno_dtcall(nix->index_connpool,3,iserver_reset,key,val);
        else result = kno_dtcall_nrx(nix->index_connpool,3,4,
                                    ixserver_reset,xname,key,val);
        if (KNO_ABORTP(result)) {
          kno_clear_errors(1);}
        else n_transactions++;
        i++;}}}

  if (n_drops) {
    if (nix->capabilities&KNO_ISERVER_DROP) {
      int i = 0; while ( i< n_drops ) {
        lispval result = VOID;
        lispval key = drops[i].kv_key;
        lispval val = drops[i].kv_val;
        if (VOIDP(xname))
          result = kno_dtcall(nix->index_connpool,3,iserver_drop,key,val);
        else result = kno_dtcall_x(nix->index_connpool,3,4,ixserver_drop,xname,key,val);
        if (KNO_ABORTP(result)) {
          kno_clear_errors(1);}
        else n_transactions++;
        i++;}}}

  if (n_adds) {
    if (nix->capabilities&KNO_ISERVER_ADDN) {
      lispval vector = kno_make_vector(n_adds*2,NULL), result=KNO_VOID;
      int i=0; while (i<n_adds) {
        lispval key = adds[i].kv_key, val = adds[i].kv_val;
        KNO_VECTOR_SET(vector,i*2,key); kno_incref(key);
        KNO_VECTOR_SET(vector,i*2+1,val); kno_incref(val);
        i=i+2;}
      if (VOIDP(xname))
        result = kno_dtcall_nr(nix->index_connpool,2,iserver_addn,vector);
      else result = kno_dtcall_nrx(nix->index_connpool,3,3,ixserver_addn,xname,vector);
      if (KNO_ABORTP(result)) {
        kno_clear_errors(1);}
      n_transactions++;}
    else {
      int i = 0; while (i < n_adds) {
        lispval result = VOID;
        lispval key = adds[i].kv_key, val = adds[i].kv_val;
        if (VOIDP(xname))
          result = kno_dtcall_nr(nix->index_connpool,3,iserver_add,key,val);
        else result = kno_dtcall_nr(nix->index_connpool,4,iserver_add,xname,key,val);
        if (KNO_ABORTP(result))
          kno_clear_errors(1);
        else n_transactions++;
        i++;}}}

  return n_transactions;
}

static int netindex_commit(kno_index ix,kno_commit_phase phase,
                           struct KNO_INDEX_COMMITS *commit)
{
  switch (phase) {
  case kno_commit_write: {
    return netindex_save(ix,
                         (struct KNO_CONST_KEYVAL *)commit->commit_adds,
                         commit->commit_n_adds,
                         (struct KNO_CONST_KEYVAL *)commit->commit_drops,
                         commit->commit_n_drops,
                         (struct KNO_CONST_KEYVAL *)commit->commit_stores,
                         commit->commit_n_stores,
                         commit->commit_metadata);}
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return 0;}
  }
}

static void netindex_close(kno_index ix)
{
}

static struct KNO_INDEX_HANDLER netindex_handler={
  "netindex", 1, sizeof(struct KNO_NETWORK_INDEX), 14,
  netindex_close, /* close */
  netindex_commit, /* commit */
  netindex_fetch, /* fetch */
  netindex_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  netindex_fetchn, /* fetchn */
  netindex_fetchkeys, /* fetchkeys */
  NULL, /* fetchinfo */
  NULL, /* batchadd */
  NULL, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  NULL  /* indexctl */
};

KNO_EXPORT void kno_init_netindex_c()
{
  u8_register_source_file(_FILEINFO);

  boundp = kno_intern("bound?");
  quote = kno_intern("quote");
  set_symbol = kno_intern("set");
  drop_symbol = kno_intern("drop");

  iserver_writable = kno_intern("iserver-writable?");
  iserver_fetchkeys = kno_intern("iserver-keys");
  iserver_changes = kno_intern("iserver-changes");
  iserver_fetch = kno_intern("iserver-get");
  iserver_fetchsize = kno_intern("iserver-get-size");
  iserver_add = kno_intern("iserver-add!");
  iserver_drop = kno_intern("iserver-drop!");
  iserver_fetchn = kno_intern("iserver-bulk-get");
  iserver_addn = kno_intern("iserver-bulk-add!");
  iserver_reset = kno_intern("iserver-reset!");

  ixserver_writable = kno_intern("ixserver-writable?");
  ixserver_fetchkeys = kno_intern("ixserver-keys");
  ixserver_changes = kno_intern("ixserver-changes");
  ixserver_fetch = kno_intern("ixserver-get");
  ixserver_fetchsize = kno_intern("ixserver-get-size");
  ixserver_add = kno_intern("ixserver-add!");
  ixserver_drop = kno_intern("ixserver-drop!");
  ixserver_fetchn = kno_intern("ixserver-bulk-get");
  ixserver_addn = kno_intern("ixserver-bulk-add!");
  iserver_reset = kno_intern("ixserver-reset!");

  kno_register_index_type
    ("network_index",
     &netindex_handler,
     kno_open_network_index,
     kno_netspecp,
     (void*)NULL);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
