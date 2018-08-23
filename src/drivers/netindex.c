/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/components/storage_layer.h"
#define FD_INLINE_BUFIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/dtcall.h"
#include "framerd/drivers.h"

#include "headers/netindex.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

static struct FD_INDEX_HANDLER netindex_handler;

static lispval boundp, quote;
static lispval iserver_writable, iserver_fetchkeys, iserver_fetch, iserver_fetchsize, iserver_fetchn;
static lispval iserver_add, iserver_drop, iserver_addn, iserver_changes, iserver_reset;

static lispval ixserver_writable, ixserver_fetchkeys, ixserver_fetch, ixserver_fetchsize, ixserver_fetchn;
static lispval ixserver_changes, ixserver_add, ixserver_drop, ixserver_addn, ixserver_reset;

static lispval set_symbol, drop_symbol;

u8_condition fd_NoServerMethod=_("Server doesn't support method");

static int server_supportsp(struct FD_NETWORK_INDEX *ni,lispval operation)
{
  lispval request=
    fd_conspair(boundp,fd_conspair(operation,NIL));
  lispval response = fd_dteval(ni->index_connpool,request);
  fd_decref(request);
  if ((FALSEP(response)) || (FD_ABORTP(response))) return 0;
  else {fd_decref(response); return 1;}
}

FD_EXPORT fd_index fd_open_network_index(u8_string spec,
                                         fd_storage_flags flags,
                                         lispval opts)
{
  struct FD_NETWORK_INDEX *ix;
  lispval writable_response; u8_string xid = NULL;
  u8_connpool cp = u8_open_connpool(spec,fd_dbconn_reserve_default,
                                    fd_dbconn_cap_default,
                                    fd_dbconn_init_default);
  if (cp == NULL) return NULL;
  ix = u8_alloc(struct FD_NETWORK_INDEX); memset(ix,0,sizeof(*ix));
  fd_init_index((fd_index)ix,&netindex_handler,
                spec,xid,xid,
                flags,FD_VOID,opts);
  ix->index_connpool = cp; ix->xname = VOID;
  writable_response = fd_dtcall(ix->index_connpool,1,iserver_writable);
  if ((FD_ABORTP(writable_response))||
      (!(FALSEP(writable_response))))
    U8_SETBITS(ix->index_flags,(FD_STORAGE_READ_ONLY));
  else U8_CLEARBITS(ix->index_flags,(FD_STORAGE_READ_ONLY));
  fd_decref(writable_response);

  ix->capabilities = 0;
  if (server_supportsp(ix,iserver_fetchn)) ix->capabilities |= FD_ISERVER_FETCHN;
  if (server_supportsp(ix,iserver_addn)) ix->capabilities |= FD_ISERVER_ADDN;
  if (server_supportsp(ix,iserver_drop)) ix->capabilities |= FD_ISERVER_DROP;
  if (server_supportsp(ix,iserver_reset)) ix->capabilities |= FD_ISERVER_RESET;

  if ((ix)&&(!(U8_BITP(flags,FD_STORAGE_UNREGISTERED))))
    fd_register_index((fd_index)ix);

  return (fd_index) ix;
}

static lispval netindex_fetch(fd_index ix,lispval key)
{
  struct FD_NETWORK_INDEX *nix = (struct FD_NETWORK_INDEX *)ix;
  if (VOIDP(nix->xname))
    return fd_dtcall(nix->index_connpool,2,iserver_fetch,key);
  else return fd_dtcall_x(nix->index_connpool,3,3,
                          ixserver_fetch,nix->xname,key);
}

static int netindex_fetchsize(fd_index ix,lispval key)
{
  struct FD_NETWORK_INDEX *nix = (struct FD_NETWORK_INDEX *)ix;
  lispval result;
  if (VOIDP(nix->xname))
    result = fd_dtcall(nix->index_connpool,2,iserver_fetchsize,key);
  else result = fd_dtcall_x(nix->index_connpool,3,3,
                            ixserver_fetchsize,nix->xname,key);
  if (FD_ABORTP(result))
    return -1;
  else return fd_getint(result);
}

static lispval *netindex_fetchn(fd_index ix,int n,const lispval *keys)
{
  struct FD_NETWORK_INDEX *nix = (struct FD_NETWORK_INDEX *)ix;
  lispval vector, result;
  vector = fd_wrap_vector(n,(lispval *)keys);
  if (VOIDP(nix->xname))
    result = fd_dtcall(nix->index_connpool,2,iserver_fetchn,vector);
  else result = fd_dtcall_x(nix->index_connpool,3,3,
                            ixserver_fetchn,nix->xname,vector);
  if (FD_ABORTP(result)) return NULL;
  else if (VECTORP(result)) {
    lispval *results = u8_alloc_n(n,lispval);
    memcpy(results,FD_VECTOR_ELTS(result),LISPVEC_BYTELEN(n));
    return results;}
  else {
    fd_seterr(fd_BadServerResponse,"netindex_fetchn",ix->indexid,
              fd_incref(result));
    return NULL;}
}

static lispval *netindex_fetchkeys(fd_index ix,int *n)
{
  struct FD_NETWORK_INDEX *nix = (struct FD_NETWORK_INDEX *)ix;
  lispval result;
  if (VOIDP(nix->xname))
    result = fd_dtcall(nix->index_connpool,1,iserver_fetchkeys);
  else result = fd_dtcall(nix->index_connpool,3,2,ixserver_fetchkeys,nix->xname);
  if (FD_ABORTP(result)) {
    *n = -1;
    fd_interr(result);
    return NULL;}
  if (CHOICEP(result)) {
    int size = FD_CHOICE_SIZE(result);
    lispval *dtypes = u8_alloc_n(size,lispval);
    memcpy(dtypes,FD_CHOICE_DATA(result),LISPVEC_BYTELEN(size));
    *n = size;
    u8_free((fd_cons)result);
    return dtypes;}
  else {
    lispval *dtypes = u8_alloc_n(1,lispval); *n = 1;
    dtypes[0]=result;
    return dtypes;}
}

static int netindex_save(struct FD_INDEX *ix,
                         struct FD_CONST_KEYVAL *adds,int n_adds,
                         struct FD_CONST_KEYVAL *drops,int n_drops,
                         struct FD_CONST_KEYVAL *stores,int n_stores,
                         lispval changed_metadata)
{
  struct FD_NETWORK_INDEX *nix = (struct FD_NETWORK_INDEX *)ix;
  lispval xname = nix->xname;
  int n_transactions = 0;

  if (n_stores) {
    if (nix->capabilities&FD_ISERVER_RESET) {
      int i = 0; while ( i< n_stores ) {
        lispval result = VOID;
        lispval key = stores[i].kv_key;
        lispval val = stores[i].kv_val;
        if (VOIDP(xname))
          result = fd_dtcall(nix->index_connpool,3,iserver_reset,key,val);
        else result = fd_dtcall_nrx(nix->index_connpool,3,4,
                                    ixserver_reset,xname,key,val);
        if (FD_ABORTP(result)) {
          fd_clear_errors(1);}
        else n_transactions++;
        i++;}}}

  if (n_drops) {
    if (nix->capabilities&FD_ISERVER_DROP) {
      int i = 0; while ( i< n_drops ) {
        lispval result = VOID;
        lispval key = drops[i].kv_key;
        lispval val = drops[i].kv_val;
        if (VOIDP(xname))
          result = fd_dtcall(nix->index_connpool,3,iserver_drop,key,val);
        else result = fd_dtcall_x(nix->index_connpool,3,4,ixserver_drop,xname,key,val);
        if (FD_ABORTP(result)) {
          fd_clear_errors(1);}
        else n_transactions++;
        i++;}}}

  if (n_adds) {
    if (nix->capabilities&FD_ISERVER_ADDN) {
      lispval vector = fd_make_vector(n_adds*2,NULL), result=FD_VOID;
      int i=0; while (i<n_adds) {
        lispval key = adds[i].kv_key, val = adds[i].kv_val;
        FD_VECTOR_SET(vector,i*2,key); fd_incref(key);
        FD_VECTOR_SET(vector,i*2+1,val); fd_incref(val);
        i=i+2;}
      if (VOIDP(xname))
        result = fd_dtcall_nr(nix->index_connpool,2,iserver_addn,vector);
      else result = fd_dtcall_nrx(nix->index_connpool,3,3,ixserver_addn,xname,vector);
      if (FD_ABORTP(result)) {
        fd_clear_errors(1);}
      n_transactions++;}
    else {
      int i = 0; while (i < n_adds) {
        lispval result = VOID;
        lispval key = adds[i].kv_key, val = adds[i].kv_val;
        if (VOIDP(xname))
          result = fd_dtcall_nr(nix->index_connpool,3,iserver_add,key,val);
        else result = fd_dtcall_nr(nix->index_connpool,4,iserver_add,xname,key,val);
        if (FD_ABORTP(result))
          fd_clear_errors(1);
        else n_transactions++;
        i++;}}}

  return n_transactions;
}

static int netindex_commit(fd_index ix,fd_commit_phase phase,
                           struct FD_INDEX_COMMITS *commit)
{
  switch (phase) {
  case fd_commit_write: {
    return netindex_save(ix,
                         (struct FD_CONST_KEYVAL *)commit->commit_adds,
                         commit->commit_n_adds,
                         (struct FD_CONST_KEYVAL *)commit->commit_drops,
                         commit->commit_n_drops,
                         (struct FD_CONST_KEYVAL *)commit->commit_stores,
                         commit->commit_n_stores,
                         commit->commit_metadata);}
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return 0;}
  }
}

static void netindex_close(fd_index ix)
{
}

static struct FD_INDEX_HANDLER netindex_handler={
  "netindex", 1, sizeof(struct FD_NETWORK_INDEX), 14,
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

FD_EXPORT void fd_init_netindex_c()
{
  u8_register_source_file(_FILEINFO);

  boundp = fd_intern("BOUND?");
  quote = fd_intern("QUOTE");
  set_symbol = fd_intern("SET");
  drop_symbol = fd_intern("DROP");

  iserver_writable = fd_intern("ISERVER-WRITABLE?");
  iserver_fetchkeys = fd_intern("ISERVER-KEYS");
  iserver_changes = fd_intern("ISERVER-CHANGES");
  iserver_fetch = fd_intern("ISERVER-GET");
  iserver_fetchsize = fd_intern("ISERVER-GET-SIZE");
  iserver_add = fd_intern("ISERVER-ADD!");
  iserver_drop = fd_intern("ISERVER-DROP!");
  iserver_fetchn = fd_intern("ISERVER-BULK-GET");
  iserver_addn = fd_intern("ISERVER-BULK-ADD!");
  iserver_reset = fd_intern("ISERVER-RESET!");

  ixserver_writable = fd_intern("IXSERVER-WRITABLE?");
  ixserver_fetchkeys = fd_intern("IXSERVER-KEYS");
  ixserver_changes = fd_intern("IXSERVER-CHANGES");
  ixserver_fetch = fd_intern("IXSERVER-GET");
  ixserver_fetchsize = fd_intern("IXSERVER-GET-SIZE");
  ixserver_add = fd_intern("IXSERVER-ADD!");
  ixserver_drop = fd_intern("IXSERVER-DROP!");
  ixserver_fetchn = fd_intern("IXSERVER-BULK-GET");
  ixserver_addn = fd_intern("IXSERVER-BULK-ADD!");
  iserver_reset = fd_intern("IXSERVER-RESET!");

  fd_register_index_type
    ("network_index",
     &netindex_handler,
     fd_open_network_index,
     fd_netspecp,
     (void*)NULL);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
