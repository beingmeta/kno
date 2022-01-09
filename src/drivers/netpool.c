/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"
#define KNO_INLINE_POOLS 1
#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"
#include "kno/services.h"

#include "headers/netpool.h"

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

#include <stdarg.h>

static struct KNO_POOL_HANDLER netpool_handler;

static lispval pool_data_symbol, new_oid_symbol, get_load_symbol;
static lispval oid_value_symbol, fetch_oids_symbol;
static lispval lock_oid_symbol, unlock_oid_symbol, clear_oid_lock_symbol;
static lispval boundp, bulk_commit_symbol;

static lispval client_id = VOID;
static void init_client_id(void);
static u8_mutex client_id_lock;

KNO_EXPORT kno_pool kno_open_network_pool
(u8_string spec,lispval dbname,kno_storage_flags flags,lispval opts);

DEF_KNOSYM(base); DEF_KNOSYM(capacity);
DEF_KNOSYM(metadata); DEF_KNOSYM(flags);
DEF_KNOSYM(label); DEF_KNOSYM(adjuncts);
DEF_KNOSYM(dbname);

static int init_network_pool
(struct KNO_NETWORK_POOL *p,lispval netinfo,
 u8_string spec,u8_string source,kno_storage_flags flags,
 lispval opts)
{
  lispval pool_base = kno_get(netinfo,KNOSYM(base),KNO_VOID);
  lispval pool_cap = kno_get(netinfo,KNOSYM(capacity),KNO_VOID);
  lispval adjunct = kno_get(netinfo,KNOSYM_ADJUNCT,KNO_VOID);
  lispval adjuncts = (KNO_VOIDP(adjunct)) ?
    (kno_get(netinfo,KNOSYM(adjuncts),KNO_EMPTY)) :
    (KNO_EMPTY);
  lispval metadata = kno_get(netinfo,KNOSYM(metadata),KNO_VOID);
  KNO_OID base; unsigned int capacity;
  if (KNO_OIDP(pool_base)) base = KNO_OID_ADDR(pool_base); else return -1;
  if (KNO_UINTP(pool_cap)) capacity = kno_getint(pool_cap); else return -1;
  if ( ! (KNO_VOIDP(adjunct)) ) {
    kno_store(metadata,KNOSYM_ADJUNCT,adjunct);
    kno_add(metadata,KNOSYM(flags),KNOSYM_ISADJUNCT);}
  flags |= KNO_STORAGE_READ_ONLY;
  kno_init_pool((kno_pool)p,base,capacity,&netpool_handler,
		spec,source,source,
		flags,metadata,opts);
  /* Network pool specific stuff */
  if (kno_testopt(netinfo,KNOSYM(label),KNO_VOID)) {
    lispval label_opt = kno_get(netinfo,KNOSYM(label),KNO_VOID);
    if (KNO_STRINGP(label_opt)) {
      u8_string label = KNO_CSTRING(label_opt);
      p->pool_label = u8_strdup(label);}
    else if (KNO_SYMBOLP(label_opt)) {
      u8_string label = KNO_SYMBOL_NAME(label_opt);
      p->pool_label = u8_strdup(label);}
    else NO_ELSE;
    kno_decref(label_opt);}
  /* Register the pool */
  int rv = kno_register_pool((kno_pool)p);
  if (rv<0) return rv;
  if (!(KNO_EMPTYP(adjuncts))) {
    KNO_DO_CHOICES(adjunct,adjuncts) {
      lispval opts = kno_make_slotmap(7,0,NULL);
      kno_store(opts,KNOSYM(base),pool_base);
      kno_store(opts,KNOSYM(capacity),pool_cap);
      kno_store(opts,KNOSYM_ADJUNCT,adjunct);
      unsigned int flags =
	KNO_STORAGE_ISPOOL | KNO_STORAGE_READ_ONLY | KNO_POOL_ADJUNCT;
      kno_pool adj_pool = kno_open_network_pool(spec,adjunct,flags,opts);
      kno_set_adjunct((kno_pool)p,adjunct,kno_pool2lisp(adj_pool));
      kno_decref(opts);}}
  return rv;
}

struct POOL_SEARCH {
  kno_pool found; u8_string spec; lispval dbname;};

static int find_netpool_iterfn(kno_pool p,void *state)
{
  struct POOL_SEARCH *search = (struct POOL_SEARCH *)state;
  if (search->found) /* Shouldn't ever happen */
    return 1;
  else if (p->pool_handler == &netpool_handler) {
    struct KNO_NETWORK_POOL *np = (kno_network_pool) p;
    if ( (strcasecmp(search->spec,np->poolid)==0) &&
	 ( (search->dbname) == (np->pool_dbname) ) ) {
      search->found = p;
      return 1;}
    else return 0;}
  else return 0;
}

static kno_pool find_network_pool(u8_string spec,lispval dbname)
{
  struct POOL_SEARCH state = { NULL, spec, dbname};
  kno_for_pools(find_netpool_iterfn,(void *)&state);
  return state.found;
}

KNO_EXPORT kno_pool kno_open_network_pool(u8_string spec,
					  lispval dbname,
					  kno_storage_flags flags,
					  lispval opts)
{
  kno_pool existing = find_network_pool(spec,dbname);
  if (existing) return existing;

  struct KNO_NETWORK_POOL *np = u8_alloc(struct KNO_NETWORK_POOL);
  u8_string xid = NULL, cid = u8_canonical_addr(spec);
  lispval spec_obj = knostring(spec);
  if (VOIDP(client_id)) init_client_id();
  kno_service s = kno_open_service(spec_obj,opts);
  lispval pooldata = (s) ?
    (kno_service_call(s,pool_data_symbol,1,client_id)) :
    (KNO_ERROR);
  if (s == NULL) return NULL;
  else if (KNO_ABORTP(pooldata)) {
    u8_free(np); u8_free(cid);
    if (s) kno_decref((lispval)s);
    return NULL;}
  else if (KNO_EXCEPTIONP(pooldata)) {
    kno_seterr("BadNetworkPoolData","kno_open_network_pool",spec,pooldata);
    if (s) kno_decref((lispval)s);
    return NULL;}
  else NO_ELSE;
  np->poolid = cid;
  np->pool_source = xid;
  np->pool_server = s;
  np->pool_dbname = dbname;
  kno_decref(spec_obj);
  int rv = init_network_pool(np,pooldata,spec,cid,flags,opts);
  if (rv<0) {
    u8_free(np);
    kno_decref((lispval)s);
    kno_seterr("NetworkPoolInitFailed","kno_open_network_pool",spec,pooldata);
    kno_decref(pooldata);
    return NULL;}
  // np->bulk_commitp = server_supportsp(np,bulk_commit_symbol);
  kno_decref(pooldata);
  return (kno_pool)np;
}

static int network_pool_load(kno_pool p)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval value;
  value = kno_service_call
    (np->pool_server,get_load_symbol,1,kno_make_oid(p->pool_base));
  if (KNO_UINTP(value)) return FIX2INT(value);
  else if (KNO_ABORTP(value))
    return kno_interr(value);
  else {
    kno_seterr(kno_BadServerResponse,"POOL-LOAD",NULL,value);
    return -1;}
}

static lispval network_pool_fetch(kno_pool p,lispval oid)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval dbname = np->pool_dbname, value;
  value = (KNO_VOIDP(dbname)) ?
    (kno_service_call(np->pool_server,oid_value_symbol,1,oid)) :
    (kno_service_call(np->pool_server,oid_value_symbol,2,oid,dbname));
  return value;
}

static lispval *network_pool_fetchn(kno_pool p,int n,lispval *oids)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval oidvec = kno_make_vector(n,oids), dbname = np->pool_dbname;
  lispval value = (KNO_VOIDP(dbname)) ?
    (kno_service_call(np->pool_server,fetch_oids_symbol,1,oidvec)) :
    (kno_service_call(np->pool_server,fetch_oids_symbol,2,oidvec,dbname));
  kno_decref(oidvec);
  if (VECTORP(value)) {
    lispval *values = u8_alloc_n(n,lispval);
    memcpy(values,KNO_VECTOR_ELTS(value),LISPVEC_BYTELEN(n));
    return values;}
  else {
    kno_seterr(kno_BadServerResponse,"netpool_fetchn",
              np->poolid,kno_incref(value));
    return NULL;}
}

#if 0
static int network_pool_lock(kno_pool p,lispval oid)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval value;
  value = kno_service_call(np->pool_server,lock_oid_symbol,2,oid,client_id);
  if (VOIDP(value)) return 0;
  else if (KNO_ABORTP(value))
    return kno_interr(value);
  else {
    kno_hashtable_store(&(p->pool_cache),oid,value);
    kno_decref(value);
    return 1;}
}

static int network_pool_unlock(kno_pool p,lispval oids)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  lispval result;
  result = kno_service_call(np->pool_server,clear_oid_lock_symbol,2,oids,client_id);
  if (KNO_ABORTP(result)) {
    kno_decref(result); return 0;}
  else {kno_decref(result); return 1;}
}

static int network_pool_storen(kno_pool p,int n,lispval *oids,lispval *values)
{
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  if (np->bulk_commitp) {
    int i = 0;
    lispval *storevec = u8_alloc_n(n*2,lispval), vec, result;
    while (i < n) {
      storevec[i*2]=oids[i];
      storevec[i*2+1]=values[i];
      i++;}
    vec = kno_wrap_vector(n*2,storevec);
    result = kno_service_call(np->pool_server,bulk_commit_symbol,
			     2,client_id,vec);
    /* Don't decref the individual elements because you didn't incref them. */
    u8_free((struct KNO_CONS *)vec); u8_free(storevec);
    kno_decref(result);
    return 1;}
  else {
    int i = 0;
    while (i < n) {
      lispval result = kno_service_call(np->pool_server,unlock_oid_symbol,
				       3,oids[i],client_id,values[i]);
      kno_decref(result);
      i++;}
    return 1;}
}
static int network_pool_commit(kno_pool p,kno_commit_phase phase,
                               struct KNO_POOL_COMMITS *commits)
{
  switch (phase) {
  case kno_commit_write:
    return network_pool_storen(p,commits->commit_count,
                               commits->commit_oids,
                               commits->commit_vals);
  default: {
    u8_logf(LOG_INFO,"NoPhasedCommit",
            "The pool %s doesn't support phased commits",
            p->poolid);
    return 0;}
  }
}

static lispval network_pool_alloc(kno_pool p,int n)
{
  lispval results = EMPTY; int i = 0;
  struct KNO_NETWORK_POOL *np = (struct KNO_NETWORK_POOL *)p;
  while (i < n) {
    lispval result = kno_service_call(np->pool_server,new_oid_symbol,0,NULL);
    CHOICE_ADD(results,result);
    i++;}
  return results;
}
#endif

static void network_pool_close(kno_pool p)
{
}

static struct KNO_POOL_HANDLER netpool_handler={
  "netpool", 1, sizeof(struct KNO_NETWORK_POOL), 12, NULL,
  network_pool_close, /* close */
  NULL, /* alloc */
  network_pool_fetch, /* fetch */
  network_pool_fetchn, /* fetchn */
  network_pool_load, /* getload */
  NULL, /* lock */
  NULL, /* release */
  NULL, /* commit */
  NULL, /* swapout */
  NULL, /* create */
  NULL,  /* walk */
  NULL, /* recycle */
  NULL  /* poolctl */
};

static void init_client_id()
{
  u8_lock_mutex(&client_id_lock);
  if (VOIDP(client_id)) client_id = kno_mkstring(u8_sessionid());
  u8_unlock_mutex(&client_id_lock);
}

static kno_pool open_network_pool(u8_string spec,kno_storage_flags flags,
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
  return kno_open_network_pool(host_spec,dbname,flags,opts);
}

KNO_EXPORT void kno_init_netpool_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&client_id_lock);

  pool_data_symbol = kno_intern("pool-data");
  oid_value_symbol = kno_intern("fetchoid");
  fetch_oids_symbol = kno_intern("fetchoids");
  bulk_commit_symbol = kno_intern("bulk-commit");
  get_load_symbol = kno_intern("getload");

  boundp = kno_intern("bound?");
  new_oid_symbol = kno_intern("new-oid");
  lock_oid_symbol = kno_intern("lock-oid");
  unlock_oid_symbol = kno_intern("unlock-oid");
  clear_oid_lock_symbol = kno_intern("clear-oid-lock");

  kno_register_pool_type
    ("network_pool",
     &netpool_handler,
     open_network_pool,
     kno_netspecp,
     (void*)NULL);


}

