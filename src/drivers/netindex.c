/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

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

fd_exception fd_NoServerMethod=_("Server doesn't support method");

static int server_supportsp(struct FD_NETWORK_INDEX *ni,lispval operation)
{
  lispval request=
    fd_conspair(boundp,fd_conspair(operation,NIL));
  lispval response = fd_dteval(ni->index_connpool,request);
  fd_decref(request);
  if ((FALSEP(response)) || (FD_ABORTP(response))) return 0;
  else {fd_decref(response); return 1;}
}

FD_EXPORT fd_index fd_open_network_index(u8_string spec,fd_storage_flags flags,lispval opts)
{
  struct FD_NETWORK_INDEX *ix;
  lispval writable_response; u8_string xid = NULL;
  u8_connpool cp = u8_open_connpool(spec,fd_dbconn_reserve_default,
                                  fd_dbconn_cap_default,
                                  fd_dbconn_init_default);
  if (cp == NULL) return NULL;
  ix = u8_alloc(struct FD_NETWORK_INDEX); memset(ix,0,sizeof(*ix));
  fd_init_index((fd_index)ix,&netindex_handler,spec,xid,flags);
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

static lispval *netindex_fetchn(fd_index ix,int n,lispval *keys)
{
  struct FD_NETWORK_INDEX *nix = (struct FD_NETWORK_INDEX *)ix;
  lispval vector, result;
  vector = fd_init_vector(NULL,n,keys);
  if (VOIDP(nix->xname))
    result = fd_dtcall(nix->index_connpool,2,iserver_fetchn,vector);
  else result = fd_dtcall_x(nix->index_connpool,3,3,
                          ixserver_fetchn,nix->xname,vector);
  if (FD_ABORTP(result)) return NULL;
  else if (VECTORP(result)) {
    lispval *results = u8_alloc_n(n,lispval);
    memcpy(results,FD_VECTOR_ELTS(result),sizeof(lispval)*n);
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
    memcpy(dtypes,FD_CHOICE_DATA(result),sizeof(lispval)*size);
    *n = size;
    u8_free((fd_cons)result);
    return dtypes;}
  else {
    lispval *dtypes = u8_alloc_n(1,lispval); *n = 1;
    dtypes[0]=result;
    return dtypes;}
}

static int netindex_commit(fd_index ix)
{
  struct FD_NETWORK_INDEX *nix = (struct FD_NETWORK_INDEX *)ix;
  int n_transactions = 0;
  fd_write_lock_table(&(nix->index_adds));
  fd_write_lock_table(&(nix->index_edits));
  if (nix->index_edits.table_n_keys) {
    int n_edits;
    struct FD_KEYVAL *kvals = fd_hashtable_keyvals(&(nix->index_edits),&n_edits,0);
    struct FD_KEYVAL *scan = kvals, *limit = kvals+n_edits;
    while (scan<limit) {
      lispval key = scan->kv_key, result = VOID;
      if ((PAIRP(key)) && (FD_EQ(FD_CAR(key),set_symbol)))
        if (nix->capabilities&FD_ISERVER_RESET) {
          n_transactions++;
          if (VOIDP(nix->xname))
            result = fd_dtcall(nix->index_connpool,3,iserver_reset,FD_CDR(key),scan->kv_val);
          else result = fd_dtcall_nrx(nix->index_connpool,3,4,
                                    ixserver_reset,nix->xname,
                                    FD_CDR(key),scan->kv_val);}
        else u8_log(LOG_WARN,fd_NoServerMethod,
                    "Server %s doesn't support resets",ix->index_source);
      else if ((PAIRP(key)) && (FD_EQ(FD_CAR(key),drop_symbol)))
        if (nix->capabilities&FD_ISERVER_DROP) {
          n_transactions++;
          if (VOIDP(nix->xname))
            result = fd_dtcall(nix->index_connpool,3,iserver_drop,FD_CDR(key),scan->kv_val);
          else result = fd_dtcall_x(nix->index_connpool,3,4,ixserver_drop,nix->xname,
                                  FD_CDR(key),scan->kv_val);}
        else u8_log(LOG_WARN,fd_NoServerMethod,
                    "Server %s doesn't support drops",ix->index_source);
      else u8_raise(_("Bad edit key in index"),"fd_netindex_commit",NULL);
      if (FD_ABORTP(result)) {
        fd_unlock_table(&(nix->index_adds));
        fd_unlock_table(&(nix->index_edits));
        return -1;}
      else fd_decref(result);
      scan++;}
    scan = kvals; while (scan<kvals) {
      fd_decref(scan->kv_key); fd_decref(scan->kv_val); scan++;}}
  if (nix->capabilities&FD_ISERVER_ADDN) {
    int n_adds;
    struct FD_KEYVAL *kvals = fd_hashtable_keyvals(&(nix->index_adds),&n_adds,0);
    lispval vector = fd_init_vector(NULL,n_adds*2,(lispval *)kvals), result = VOID;
    if (VOIDP(nix->xname))
      result = fd_dtcall_nr(nix->index_connpool,2,iserver_addn,vector);
    else result = fd_dtcall_nrx(nix->index_connpool,3,3,ixserver_addn,nix->xname,vector);
    if (FD_ABORTP(result)) {
      fd_unlock_table(&(nix->index_adds));
      fd_unlock_table(&(nix->index_edits));
      return -1;}
    else fd_decref(result);
    return n_transactions+1;}
  else {
    int n_adds; lispval xname = nix->xname;
    struct FD_KEYVAL *kvals = fd_hashtable_keyvals(&(nix->index_adds),&n_adds,0);
    struct FD_KEYVAL *scan = kvals, *limit = scan+n_adds;
    if (VOIDP(xname))
      while (scan<limit) {
        lispval result = VOID;
        n_transactions++;
        result = fd_dtcall_nr(nix->index_connpool,3,
                            iserver_add,scan->kv_key,scan->kv_val);
        if (FD_ABORTP(result)) {
          fd_unlock_table(&(nix->index_adds));
          fd_unlock_table(&(nix->index_edits));
          return -1;}
        else fd_decref(result);
        scan++;}
    else while (scan<limit) {
      lispval result = VOID;
      n_transactions++;
      result = fd_dtcall_nrx(nix->index_connpool,3,3,
                           ixserver_add,xname,scan->kv_key,scan->kv_val);
      if (FD_ABORTP(result)) {
        fd_unlock_table(&(nix->index_adds));
        fd_unlock_table(&(nix->index_edits));
        return -1;}
      else fd_decref(result);
      scan++;}
    scan = kvals; while (scan<kvals) {
      fd_decref(scan->kv_key); fd_decref(scan->kv_val); scan++;}
    u8_free(kvals);
    fd_reset_hashtable(&(nix->index_adds),67,0);
    fd_reset_hashtable(&(nix->index_edits),67,0);
    fd_unlock_table(&(nix->index_adds));
    fd_unlock_table(&(nix->index_edits));
    return n_transactions;}
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
  NULL, /* metadata */
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
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
