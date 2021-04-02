/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/services.h"
#include "kno/drivers.h"

#include "headers/netindex.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

static struct KNO_INDEX_HANDLER netindex_handler;

static lispval boundp, quote;
static lispval iserver_fetchkey, iserver_fetchkeys, iserver_listkeys;
static lispval iserver_fetchsize;

static lispval set_symbol, drop_symbol;
static lispval supportedp_symbol = KNO_NULL, dbname_symbol = KNO_NULL;

u8_condition kno_NoServerMethod=_("Server doesn't support method");

U8_MAYBE_UNUSED static int server_supportsp(kno_network_index ni,lispval op)
{
  lispval response =
    kno_service_call(ni->index_server,KNOSYM(supportedp),1,op);
  if ((FALSEP(response)) || (KNO_ABORTP(response)))
    return 0;
  else {
    kno_decref(response);
    return 1;}
}

struct INDEX_SEARCH {
  kno_index found; u8_string spec; lispval dbname;};

static int find_netindex_iterfn(kno_index ix,void *state)
{
  struct INDEX_SEARCH *search = (struct INDEX_SEARCH *)state;
  if (search->found) /* Shouldn't ever happen */
    return 1;
  else if (ix->index_handler == &netindex_handler) {
    struct KNO_NETWORK_INDEX *nx = (kno_network_index) ix;
    if ( (strcasecmp(search->spec,nx->indexid)==0) &&
	 ( (search->dbname) == (nx->index_dbname) ) ) {
      search->found = ix;
      return 1;}
    else return 0;}
  else return 0;
}

static kno_index find_network_index(u8_string spec,lispval dbname)
{
  struct INDEX_SEARCH state = { NULL, spec, dbname};
  kno_for_indexes(find_netindex_iterfn,(void *)&state);
  return state.found;
}

KNO_EXPORT kno_index kno_open_network_index(u8_string spec,lispval dbname,
					    kno_storage_flags flags,
					    lispval opts)
{
  kno_index existing = find_network_index(spec,dbname);
  if (existing) return existing;

  lispval spec_obj = knostring(spec);
  kno_service server = (kno_service) kno_open_service(spec_obj,opts);
  kno_decref(spec_obj);
  if (server == NULL) return NULL;
  struct KNO_NETWORK_INDEX *ix = u8_zalloc(struct KNO_NETWORK_INDEX);
  kno_init_index((kno_index)ix,&netindex_handler,
		 spec,NULL,NULL,
		 flags,KNO_VOID,opts);
  ix->index_server = server;
  U8_SETBITS(ix->index_flags,(KNO_STORAGE_READ_ONLY));
  ix->index_dbname = dbname;

  ix->capabilities = 0;
  ix->capabilities |= KNO_ISERVER_FETCHN;

  if ((ix)&&(!(U8_BITP(flags,KNO_STORAGE_UNREGISTERED))))
    kno_register_index((kno_index)ix);

  return (kno_index) ix;
}

static lispval netindex_fetch(kno_index ix,lispval key)
{
  struct KNO_NETWORK_INDEX *nix = (struct KNO_NETWORK_INDEX *)ix;
  if (VOIDP(nix->index_dbname))
    return kno_service_call(nix->index_server,iserver_fetchkey,1,key);
  else return kno_service_call
	 (nix->index_server,iserver_fetchkey,2,key,nix->index_dbname);
}

static int netindex_fetchsize(kno_index ix,lispval key)
{
  struct KNO_NETWORK_INDEX *nix = (struct KNO_NETWORK_INDEX *)ix;
  lispval result;
  if (VOIDP(nix->index_dbname))
    result = kno_service_call(nix->index_server,iserver_fetchsize,1,key);
  else result = kno_service_call
	 (nix->index_server,iserver_fetchsize,2,key,nix->index_dbname);
  if (KNO_ABORTP(result))
    return -1;
  else return kno_getint(result);
}

static lispval *netindex_fetchn(kno_index ix,int n,const lispval *keys)
{
  struct KNO_NETWORK_INDEX *nix = (struct KNO_NETWORK_INDEX *)ix;
  lispval vector, result;
  vector = kno_wrap_vector(n,(lispval *)keys);
  if (VOIDP(nix->index_dbname))
    result = kno_service_call(nix->index_server,iserver_fetchkeys,1,vector);
  else result = kno_service_call
	 (nix->index_server,iserver_fetchkeys,2,vector,nix->index_dbname);
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
  if (VOIDP(nix->index_dbname))
    result = kno_service_call(nix->index_server,1,iserver_listkeys);
  else result = kno_service_call
	 (nix->index_server,iserver_listkeys,1,nix->index_dbname);
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

#if 0
static lispval ixserver_changes, ixserver_add, ixserver_drop;
static lispval ixserver_addn, ixserver_reset;

static int netindex_save(struct KNO_INDEX *ix,
			 struct KNO_CONST_KEYVAL *adds,int n_adds,
			 struct KNO_CONST_KEYVAL *drops,int n_drops,
			 struct KNO_CONST_KEYVAL *stores,int n_stores,
			 lispval changed_metadata)
{
  struct KNO_NETWORK_INDEX *nix = (struct KNO_NETWORK_INDEX *)ix;
  lispval xname = nix->index_dbname;
  int n_transactions = 0;

  if (n_stores) {
    if (nix->capabilities&KNO_ISERVER_RESET) {
      int i = 0; while ( i< n_stores ) {
	lispval result = VOID;
	lispval key = stores[i].kv_key;
	lispval val = stores[i].kv_val;
	if (VOIDP(xname))
	  result = kno_service_call(nix->index_server,iserver_reset,2,key,val);
	else result = kno_service_call
	       (nix->index_server,ixserver_reset,3,xname,key,val);
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
	  result = kno_service_call(nix->index_server,iserver_drop,2,key,val);
	else result = kno_service_call
	       (nix->index_server,ixserver_drop,3,xname,key,val);
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
	result = kno_service_call(nix->index_server,iserver_addn,1,vector);
      else result = kno_service_call
	     (nix->index_server,ixserver_addn,2,xname,vector);
      if (KNO_ABORTP(result)) {
	kno_clear_errors(1);}
      n_transactions++;}
    else {
      int i = 0; while (i < n_adds) {
	lispval result = VOID;
	lispval key = adds[i].kv_key, val = adds[i].kv_val;
	if (VOIDP(xname))
	  result = kno_service_call(nix->index_server,iserver_add,2,key,val);
	else result = kno_service_call
	       (nix->index_server,iserver_add,3,xname,key,val);
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
#endif

static void netindex_close(kno_index ix)
{
}

static struct KNO_INDEX_HANDLER netindex_handler={
  "netindex", 1, sizeof(struct KNO_NETWORK_INDEX), 14, NULL,
  netindex_close, /* close */
  NULL, /* netindex_commit */
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

static kno_index open_network_index(u8_string spec,kno_storage_flags flags,
				   lispval opts)
{
  lispval dbname = KNO_VOID; u8_string host_spec = spec, slash;
  ssize_t spec_len = strlen(spec);
  u8_byte host_buf[spec_len+1];
  if ((slash=(strchr(spec,'/')))) {
    strcpy(host_buf,spec);
    if (slash[1])
      dbname = kno_parse(slash+1);
    else dbname=KNO_VOID;
    host_buf[slash-spec]='\0';
    host_spec = host_buf;}
  else dbname = kno_getopt(opts,KNOSYM(dbname),KNO_VOID);
  if ( (KNO_FALSEP(dbname)) || (KNO_DEFAULTP(dbname)) ) dbname = KNO_VOID;
  if (KNO_VOIDP(dbname)) {}
  else if (!( (OIDP(dbname)) || (SYMBOLP(dbname)) ) ) {
    u8_log(LOGWARN,"InvalidDBName","For pool %s: %q",spec,dbname);
    kno_decref(dbname); dbname = KNO_VOID;}
  else NO_ELSE;
  return kno_open_network_index(host_spec,dbname,flags,opts);
}

KNO_EXPORT void kno_init_netindex_c()
{
  u8_register_source_file(_FILEINFO);

  boundp = kno_intern("bound?");
  quote = kno_intern("quote");
  set_symbol = kno_intern("set");
  drop_symbol = kno_intern("drop");

  iserver_fetchkey = kno_intern("fetchkey");
  iserver_fetchsize = kno_intern("fetchsize");
  iserver_fetchkeys = kno_intern("fetchkeys");
  iserver_listkeys = kno_intern("listkeys");

  kno_register_index_type
    ("network_index",
     &netindex_handler,
     open_network_index,
     kno_netspecp,
     (void*)NULL);

}

