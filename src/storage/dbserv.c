/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_INLINE_POOLS 1
#define KNO_INLINE_CHOICES 1
#define KNO_INLINE_IPEVAL 1
static int dbserv_loglevel;
#define U8_LOGLEVEL dbserv_loglevel

#include "kno/knosource.h"
#include "kno/dtype.h"
#include "kno/storage.h"
#include "kno/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8filefns.h>
#if HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#include <libu8/u8srvfns.h>

static kno_pool primary_pool = NULL;
static kno_pool served_pools[KNO_DBSERV_MAX_POOLS];
static int n_served_pools = 0;
struct KNO_AGGREGATE_INDEX *primary_index = NULL;
static int read_only = 0, locking = 1;
static int dbserv_loglevel = LOG_NOTICE;

u8_condition kno_PrivateOID=_("private OID");

static int served_poolp(kno_pool p)
{
  int i = 0; while (i<n_served_pools)
               if (served_pools[i]==p) return 1;
               else i++;
  return 0;
}

/* Change logs */

static int init_timestamp = 0;
static int change_count = 0;
static u8_mutex changelog_lock;

struct KNO_CHANGELOG_ENTRY {
  int moment; lispval keys;};

struct KNO_CHANGELOG {
  int point, max, full;
  struct KNO_CHANGELOG_ENTRY *entries;};

static struct KNO_CHANGELOG oid_changelog, index_changelog;
static struct KNO_SUBINDEX_CHANGELOG {
  kno_index ix; struct KNO_CHANGELOG *clog;} *subindex_changelogs = NULL;
static int n_subindex_changelogs = 0;

static void init_changelog(struct KNO_CHANGELOG *clog,int size)
{
  clog->entries = u8_alloc_n(size,struct KNO_CHANGELOG_ENTRY);
  clog->max = size; clog->point = 0; clog->full = 0;
}

static struct KNO_CHANGELOG *get_subindex_changelog(kno_index ix,int make)
{
  struct KNO_CHANGELOG *clog = NULL; int i = 0;
  u8_lock_mutex(&changelog_lock);
  while (i < n_subindex_changelogs)
    if (subindex_changelogs[i].ix == ix) break; else i++;
  if (subindex_changelogs[i].ix == ix) clog = (subindex_changelogs[i].clog);
  else if (make == 0) clog = NULL;
  else {
    clog = u8_alloc(struct KNO_CHANGELOG);
    if (subindex_changelogs)
      subindex_changelogs=
        u8_realloc_n(subindex_changelogs,n_subindex_changelogs+1,
                     struct KNO_SUBINDEX_CHANGELOG);
    else subindex_changelogs = u8_alloc(struct KNO_SUBINDEX_CHANGELOG);
    subindex_changelogs[n_subindex_changelogs].ix = ix; subindex_changelogs[n_subindex_changelogs].clog = clog;
    init_changelog(clog,1024);
    n_subindex_changelogs++;}
  u8_unlock_mutex(&changelog_lock);
  return clog;
}

static void add_to_changelog(struct KNO_CHANGELOG *clog,lispval keys)
{
  struct KNO_CHANGELOG_ENTRY *entries; int point;
  u8_lock_mutex(&changelog_lock);
  entries = clog->entries; point = clog->point;
  if (clog->full) {
    kno_decref(entries[point].keys);}
  entries[point].moment = change_count++;
  entries[point].keys = kno_incref(keys);
  if (clog->full)
    if (clog->point == clog->max) clog->point = 0;
    else clog->point++;
  else if (clog->point == clog->max) {
    clog->point = 0; clog->full = 1;}
  else clog->point++;
  u8_unlock_mutex(&changelog_lock);
}

static lispval get_changes(struct KNO_CHANGELOG *clog,int cstamp,int *new_cstamp)
{
  lispval result;
  u8_lock_mutex(&changelog_lock);
  {
    int bottom = ((clog->full) ? (clog->point) : 0);
    int top = ((clog->full) ? ((clog->point) ? (clog->point-1) : (clog->max)) : (clog->point-1));
    struct KNO_CHANGELOG_ENTRY *entries = clog->entries;
    *new_cstamp = change_count;
    if ((clog->full == 0) && (clog->point == 0)) result = EMPTY; /* Changelog is empty */
    else if (cstamp < entries[bottom].moment) result = KNO_FALSE; /* Too far back. */
    else if (cstamp > entries[top].moment) result = EMPTY; /* No changes. */
    else {
      lispval changes = EMPTY;
      int i = top, point = clog->point; while (i >= 0)
                                          if (cstamp <= entries[i].moment) {
                                            lispval key = entries[i--].keys;
                                            kno_incref(key);
                                            CHOICE_ADD(changes,key);}
                                          else break;
      if (cstamp > entries[i].moment) {
        i = clog->max; while (i >= point)
                         if (cstamp <= entries[i].moment) {
                           lispval key = entries[i--].keys;
                           kno_incref(key);
                           CHOICE_ADD(changes,key);}
                         else break;}
      result = changes;}}
  u8_unlock_mutex(&changelog_lock);
  return result;
}

static lispval get_syncstamp_prim()
{
  return kno_make_list(2,KNO_INT(init_timestamp),KNO_INT(change_count));
}

static lispval oid_server_changes(lispval sid,lispval xid)
{
  if (FIX2INT(sid) != init_timestamp)
    return KNO_FALSE;
  else {
    int new_syncstamp;
    lispval changes=
      get_changes(&oid_changelog,FIX2INT(xid),&new_syncstamp);
    if (FALSEP(changes))
      return kno_make_list(1,kno_make_list(2,KNO_INT(init_timestamp),
                                         KNO_INT(new_syncstamp)));
    else return kno_conspair(kno_make_list(2,KNO_INT(init_timestamp),
                                         KNO_INT(new_syncstamp)),
                            changes);}
}

static lispval iserver_changes(lispval sid,lispval xid)
{
  if (FIX2INT(sid) != init_timestamp) return KNO_FALSE;
  else {
    int new_syncstamp;
    lispval changes=
      get_changes(&index_changelog,FIX2INT(xid),&new_syncstamp);
    if (FALSEP(changes))
      return kno_make_list(1,kno_make_list(2,KNO_INT(init_timestamp),
                                         KNO_INT(new_syncstamp)));
    else return kno_conspair(kno_make_list(2,KNO_INT(init_timestamp),
                                         KNO_INT(new_syncstamp)),
                            changes);}
}

static lispval ixserver_changes(lispval index,lispval sid,lispval xid)
{
  kno_index ix = kno_indexptr(index);
  struct KNO_CHANGELOG *clog = get_subindex_changelog(ix,0);
  if (clog == NULL) return EMPTY;
  else if (FIX2INT(sid) != init_timestamp) return KNO_FALSE;
  else {
    int new_syncstamp;
    lispval changes = get_changes(clog,FIX2INT(xid),&new_syncstamp);
    if (FALSEP(changes))
      return kno_make_list(1,kno_make_list(2,KNO_INT(init_timestamp),
                                         KNO_INT(new_syncstamp)));
    else return kno_conspair(kno_make_list(2,KNO_INT(init_timestamp),
                                         KNO_INT(new_syncstamp)),
                            changes);}
}

/** OID Locking **/

static struct KNO_HASHTABLE server_locks, server_locks_inv;
static unsigned int n_locks = 0;
static KNO_STREAM *locks_file = NULL;
static u8_string locks_filename = NULL;
static void update_server_lock_file();

u8_condition OIDNotLocked=_("The OID is not locked");
u8_condition CantLockOID=_("Can't lock OID");

static u8_mutex server_locks_lock;

static int lock_oid(lispval oid,lispval id)
{
  lispval holder;
  if (locking == 0) return 1;
  u8_lock_mutex(&server_locks_lock);
  holder = kno_hashtable_get(&server_locks,oid,EMPTY);
  if (EMPTYP(holder)) {
    kno_pool p = kno_oid2pool(oid);
    if ((kno_pool_lock(p,oid)) == 0) {
      u8_unlock_mutex(&server_locks_lock); return 0;}
    kno_hashtable_store(&server_locks,oid,id); n_locks++;
    kno_hashtable_add(&server_locks_inv,id,oid);
    if (locks_file) {
      kno_write_dtype(kno_writebuf(locks_file),oid);
      kno_write_dtype(kno_writebuf(locks_file),id);
      kno_flush_stream(locks_file);}
    u8_unlock_mutex(&server_locks_lock);
    return 1;}
  else if (LISP_EQUAL(id,holder)) {
    u8_unlock_mutex(&server_locks_lock); kno_decref(holder);
    return 1;}
  else {
    u8_unlock_mutex(&server_locks_lock);
    kno_decref(holder);
    return 0;}
}

static int check_server_lock(lispval oid,lispval id)
{
  lispval holder = kno_hashtable_get(&server_locks,oid,EMPTY);
  if (EMPTYP(holder)) return 0;
  else if (LISP_EQUAL(id,holder)) {
    kno_decref(holder); return 1;}
  else {kno_decref(holder); return 0;}
}

static int clear_server_lock(lispval oid,lispval id)
{
  lispval holder;
  if (locking == 0) return 1;
  u8_lock_mutex(&server_locks_lock);
  holder = kno_hashtable_get(&server_locks,oid,EMPTY);
  if (EMPTYP(holder)) {u8_unlock_mutex(&server_locks_lock); return 0;}
  else if (LISP_EQUAL(id,holder)) {
    lispval all_locks = kno_hashtable_get(&server_locks_inv,id,EMPTY);
    int lock_count = KNO_CHOICE_SIZE(all_locks);
    kno_decref(holder);
    kno_hashtable_store(&server_locks,oid,EMPTY);
    if (lock_count == 0)
      return kno_err(OIDNotLocked,"lock_oid",kno_strdata(id),oid);
    else if (lock_count == 1)
      kno_hashtable_store(&server_locks_inv,id,EMPTY);
    else kno_hashtable_drop(&server_locks_inv,id,oid);
    kno_hashtable_drop(&server_locks,oid,VOID); n_locks--;
    if (locks_file) {
      kno_write_dtype(kno_writebuf(locks_file),id);
      kno_write_dtype(kno_writebuf(locks_file),oid);
      kno_flush_stream(locks_file);}
    u8_unlock_mutex(&server_locks_lock);
    return 1;}
  else {
    kno_decref(holder);
    u8_unlock_mutex(&server_locks_lock);
    return 0;}
}

static void remove_all_server_locks(lispval id)
{
  if (locking == 0) return;
  u8_lock_mutex(&server_locks_lock);
  {
    lispval locks = kno_hashtable_get(&server_locks_inv,id,EMPTY);
    DO_CHOICES(oid,locks) {
      kno_hashtable_drop(&server_locks,oid,VOID);}
    kno_decref(locks);
    kno_hashtable_store(&server_locks_inv,id,EMPTY);
    u8_unlock_mutex(&server_locks_lock);
  }
}

static int add_to_server_locks_file(lispval key,lispval value,void *outfilep)
{
  struct KNO_STREAM *out = (kno_stream )outfilep;
  kno_write_dtype(kno_writebuf(out),key);
  kno_write_dtype(kno_writebuf(out),value);
  return 0;
}

static void open_server_lock_stream(u8_string file)
{
  if (u8_file_existsp(file)) {
    struct KNO_STREAM *stream = kno_open_file(file,KNO_FILE_READ);
    kno_inbuf in = kno_readbuf(stream);
    lispval a = kno_read_dtype(in), b = kno_read_dtype(in);
    while (!(KNO_EOFP(a))) {
      if (OIDP(a)) lock_oid(a,b); else clear_server_lock(b,a);
      a = kno_read_dtype(in); b = kno_read_dtype(in);}
    kno_close_stream(stream,0);
    u8_removefile(file);}
  locks_file = kno_open_file(file,KNO_FILE_CREATE);
  locks_filename = u8_strdup(file);
  kno_for_hashtable(&server_locks,add_to_server_locks_file,(void *)locks_file,1);
  kno_flush_stream(locks_file);
}

/* This writes out the current state of locks in memory to an external file.
   The locks file is appended to while the server is running; this means that
   it may contain OIDs which have been unlocked.  update_server_lock_file updates
   the file from memory, making it only include the OIDs which are currently
   locked.  */
static void update_server_lock_file()
{
  u8_string temp_file;
  if (locks_filename == NULL) return;
  u8_lock_mutex(&server_locks_lock);
  temp_file = u8_mkstring("%s.bak",locks_filename);
  if (locks_file) kno_close_stream(locks_file,0);
  u8_movefile(locks_filename,temp_file);
  locks_file = kno_open_file(locks_filename,KNO_FILE_CREATE);
  kno_for_hashtable(&server_locks,add_to_server_locks_file,(void *)locks_file,1);
  kno_flush_stream(locks_file);
  u8_removefile(temp_file);
  u8_free(temp_file);
  u8_unlock_mutex(&server_locks_lock);
}

static lispval config_get_locksfile(lispval var,void U8_MAYBE_UNUSED *data)
{
  if (locks_filename) return KNO_FALSE;
  else return lispval_string(locks_filename);
}

static int config_set_locksfile(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  if (locks_filename)
    if ((STRINGP(val)) && (strcmp(CSTRING(val),locks_filename)==0))
      return 0;
    else return kno_reterr(_("Locks file already set"),"kno_set_config",NULL,val);
  else if (STRINGP(val)) {
    open_server_lock_stream(CSTRING(val));
    return 1;}
  else return kno_reterr(kno_TypeError,"kno_set_config",u8_strdup("string"),val);
}

/** OID Access API **/

static lispval lock_oid_prim(lispval oid,lispval id)
{
  if (!(OIDP(oid)))
    return kno_type_error(_("oid"),"lock_oid_prim",oid);
  if ((locking == 0) ||  (lock_oid(oid,id))) {
    return kno_oid_value(oid);}
  else return kno_err(CantLockOID,"lock_oid_prim",NULL,oid);
}

static lispval unlock_oid_prim(lispval oid,lispval id,lispval value)
{
  if (locking == 0) {
    kno_pool p = kno_oid2pool(oid);
    kno_set_oid_value(oid,value);
    add_to_changelog(&oid_changelog,oid);
    kno_pool_unlock(p,oid,commit_modified);
    return KNO_TRUE;}
  else {
    kno_pool p = kno_oid2pool(oid);
    if (check_server_lock(oid,id)) {
      kno_set_oid_value(oid,value);
      clear_server_lock(oid,id);
      add_to_changelog(&oid_changelog,oid);
      kno_pool_unlock(p,oid,commit_modified);
      return KNO_TRUE;}
    else return KNO_FALSE;}
}

static lispval clear_server_lock_prim(lispval oid,lispval id)
{
  if (locking == 0) return KNO_TRUE;
  else if (clear_server_lock(oid,id)) {
    return KNO_TRUE;}
  else return KNO_FALSE;
}

static lispval break_server_lock_prim(lispval oid)
{
  if (locking == 0) return KNO_TRUE;
  else {
    lispval id = kno_hashtable_get(&server_locks,oid,EMPTY);
    if (EMPTYP(id)) return KNO_FALSE;
    else {
      clear_server_lock(oid,id);
      kno_decref(id);
      return KNO_TRUE;}}
}

static lispval unlock_all_prim(lispval id)
{
  remove_all_server_locks(id);
  update_server_lock_file();
  return KNO_TRUE;
}

static lispval update_locks_prim()
{
  update_server_lock_file();
  return VOID;
}

static lispval store_oid_proc(lispval oid,lispval value)
{
  kno_pool p = kno_oid2pool(oid);
  int i = 0; while (i < n_served_pools)
               if (served_pools[i] == p) {
                 kno_set_oid_value(oid,value);
                 add_to_changelog(&oid_changelog,oid);
                 /* Commit the pool.  Journalling should now happen on a per-pool
                    rather than a server-wide basis. */
                 if (kno_commit_pool(p,oid)>=0) {
                   kno_pool_unlock(p,oid,leave_modified);
                   return KNO_TRUE;}
                 else i++;}
               else i++;
  return KNO_FALSE;
}

static lispval bulk_commit_cproc(lispval id,lispval vec)
{
  int i = 0, l = VEC_LEN(vec);
  lispval changed_oids = EMPTY;
  /* First check that all the OIDs were really locked under the assigned ID. */
  if (locking) {
    i = 0; while (i < l) {
      lispval oid = VEC_REF(vec,i);
      if (!(OIDP(oid))) i = i+2;
      else if (check_server_lock(oid,id)) i = i+2;
      else return kno_err(OIDNotLocked,"bulk_commit_proc",NULL,oid);}}
  /* Then set the corresponding OID value, but don't commit yet. */
  i = 0; while (i < l) {
    lispval oid = VEC_REF(vec,i);
    lispval value = VEC_REF(vec,i+1);
    if (OIDP(oid)) {
      kno_set_oid_value(oid,value);
      CHOICE_ADD(changed_oids,oid);}
    i = i+2;}
  kno_commit_oids(changed_oids);
  kno_unlock_oids(changed_oids,leave_modified);
  if (locking) {
    i = 0; while (i < l) {
      lispval oid = VEC_REF(vec,i);
      if (OIDP(oid)) clear_server_lock(oid,id);
      i = i+2;}}
  add_to_changelog(&oid_changelog,changed_oids); kno_decref(changed_oids);
  return KNO_TRUE;
}

static lispval iserver_add(lispval key,lispval values)
{
  kno_index_add((kno_index)primary_index,key,values);
  add_to_changelog(&index_changelog,key);
  return KNO_TRUE;
}

static lispval ixserver_add(lispval ixarg,lispval key,lispval values)
{
  kno_index ix = kno_indexptr(ixarg);
  struct KNO_CHANGELOG *clog = get_subindex_changelog(ix,1);
  kno_index_add(ix,key,values);
  add_to_changelog(clog,key);
  return KNO_TRUE;
}

static lispval iserver_bulk_add(lispval vec)
{
  if ((read_only) || (primary_index == NULL)) return KNO_FALSE;
  else if (VECTORP(vec)) {
    lispval *data = VEC_DATA(vec), keys = EMPTY;
    int i = 0, limit = VEC_LEN(vec);
    while (i < limit) {
      if (VOIDP(data[i])) break;
      else {
        lispval key = data[i++], value = data[i++];
        kno_index_add((kno_index)primary_index,key,value);
        kno_incref(key); CHOICE_ADD(keys,key);}}
    add_to_changelog(&index_changelog,keys); kno_decref(keys);
    return KNO_TRUE;}
  else return VOID;
}

static lispval ixserver_bulk_add(lispval ixarg,lispval vec)
{
  if (read_only) return KNO_FALSE;
  else if (VECTORP(vec)) {
    kno_index ix = kno_indexptr(ixarg);
    struct KNO_CHANGELOG *clog = get_subindex_changelog(ix,1);
    lispval *data = VEC_DATA(vec), keys = EMPTY;
    int i = 0, limit = VEC_LEN(vec);
    while (i < limit) {
      if (VOIDP(data[i])) break;
      else {
        lispval key = data[i++], value = data[i++];
        kno_index_add(ix,key,value);
        kno_incref(key); CHOICE_ADD(keys,key);}}
    add_to_changelog(clog,keys); kno_decref(keys);
    return KNO_TRUE;}
  else return VOID;
}

static lispval iserver_drop(lispval key,lispval values)
{
  kno_index_drop((kno_index)primary_index,key,values);
  add_to_changelog(&index_changelog,key);
  return KNO_TRUE;
}

static lispval ixserver_drop(lispval ixarg,lispval key,lispval values)
{
  kno_index ix = kno_indexptr(ixarg);
  struct KNO_CHANGELOG *clog = get_subindex_changelog(ix,1);
  kno_index_drop(ix,key,values);
  add_to_changelog(clog,key);
  return KNO_TRUE;
}

/* pool DB methods */

static lispval server_get_load(lispval oid_arg)
{
  if (VOIDP(oid_arg))
    if (primary_pool) {
      int load = kno_pool_load(primary_pool);
      if (load<0) return KNO_ERROR;
      else return KNO_INT(load);}
    else return kno_err(_("No primary pool"),"server_get_load",NULL,VOID);
  else if (OIDP(oid_arg)) {
    kno_pool p = kno_oid2pool(oid_arg);
    int load = kno_pool_load(p);
    if (load<0) return KNO_ERROR;
    else return KNO_INT(load);}
  else return kno_type_error("OID","server_get_load",oid_arg);
}

static lispval server_oid_value(lispval x)
{
  kno_pool p = kno_oid2pool(x);
  if (p == NULL)
    return kno_err(kno_AnonymousOID,"server_oid_value",NULL,x);
  else if (served_poolp(p))
    return kno_fetch_oid(p,x);
  else return kno_err(kno_PrivateOID,"server_oid_value",NULL,x);
}

static lispval server_fetch_oids(lispval oidvec)
{
  /* We assume here that all the OIDs in oidvec are in the same pool.  This should
     be the case because clients see the different pools and sort accordingly.  */
  kno_pool p = NULL;
  int n = VEC_LEN(oidvec), fetchn = 0;
  lispval *elts = VEC_DATA(oidvec);
  if (n==0)
    return kno_empty_vector(0);
  else if (!(OIDP(elts[0])))
    return kno_type_error(_("oid vector"),"server_fetch_oids",oidvec);
  else if ((p = (kno_oid2pool(elts[0]))))
    if (served_poolp(p)) {
      lispval *results = NULL;
      if (p->pool_handler->fetchn) {
        lispval *fetch = u8_alloc_n(n,lispval);
        kno_hashtable cache = &(p->pool_cache), locks = &(p->pool_changes);
        int i = 0; while (i<n)
                     if ((kno_hashtable_probe_novoid(cache,elts[i])==0) &&
                         (kno_hashtable_probe_novoid(locks,elts[i])==0))
                       fetch[fetchn++]=elts[i++];
                     else i++;
        results = p->pool_handler->fetchn(p,fetchn,fetch);
        return kno_cons_vector(NULL,n,1,results);}
      else {
        results = u8_big_alloc_n(n,lispval);
        int i = 0; while (i<n) {
          results[i]=kno_fetch_oid(p,elts[i]); i++;}
        return kno_cons_vector(NULL,n,1,results);}}
    else return kno_err(kno_PrivateOID,"server_oid_value",NULL,elts[0]);
  else return kno_err(kno_AnonymousOID,"server_oid_value",NULL,elts[0]);
}

static lispval server_pool_data(lispval session_id)
{
  int len = n_served_pools;
  lispval *elts = u8_alloc_n(len,lispval);
  int i = 0; while (i<len) {
    kno_pool p = served_pools[i];
    lispval base = kno_make_oid(p->pool_base);
    lispval capacity = KNO_INT(p->pool_capacity);
    lispval ro = (U8_BITP(p->pool_flags,KNO_STORAGE_READ_ONLY)) ? (KNO_FALSE) : (KNO_TRUE);
    elts[i++]=
      ((p->pool_label) ?
       (kno_make_list(4,base,capacity,ro,lispval_string(p->pool_label))) :
       (kno_make_list(3,base,capacity,ro)));}
  return kno_wrap_vector(len,elts);
}

/* index DB methods */

static lispval iserver_get(lispval key)
{
  return kno_index_get((kno_index)(primary_index),key);
}
static lispval iserver_bulk_get(lispval keys)
{
  if (VECTORP(keys)) {
    int i = 0, n = VEC_LEN(keys), retval;
    lispval *data = VEC_DATA(keys), *results = u8_alloc_n(n,lispval);
    /* |KNO_CHOICE_ISATOMIC */
    lispval aschoice = kno_make_choice
      (n,data,(KNO_CHOICE_DOSORT|KNO_CHOICE_INCREF));
    retval = kno_index_prefetch((kno_index)(primary_index),aschoice);
    if (retval<0) {
      kno_decref(aschoice); u8_free(results);
      return KNO_ERROR;}
    while (i<n) {
      results[i]=kno_index_get((kno_index)(primary_index),data[i]); i++;}
    kno_decref(aschoice);
    return kno_wrap_vector(n,results);}
  else return kno_type_error("vector","iserver_bulk_get",keys);
}
static lispval iserver_get_size(lispval key)
{
  lispval value = kno_index_get((kno_index)(primary_index),key);
  int size = KNO_CHOICE_SIZE(value);
  kno_decref(value);
  return KNO_INT(size);
}
static lispval iserver_keys(lispval key)
{
  return kno_index_keys((kno_index)(primary_index));
}
static lispval iserver_sizes(lispval key)
{
  return kno_index_sizes((kno_index)(primary_index));
}
static lispval iserver_writablep()
{
  return KNO_FALSE;
}

static lispval ixserver_get(lispval index,lispval key)
{
  if (KNO_INDEXP(index))
    return kno_index_get(kno_indexptr(index),key);
  else if (TABLEP(index))
    return kno_get(index,key,EMPTY);
  else return kno_type_error("index","ixserver_get",VOID);
}
static lispval ixserver_bulk_get(lispval index,lispval keys)
{
  if (KNO_INDEXP(index))
    if (VECTORP(keys)) {
      kno_index ix = kno_indexptr(index);
      int i = 0, n = VEC_LEN(keys);
      lispval *data = VEC_DATA(keys),
        *results = u8_alloc_n(n,lispval);
      lispval aschoice=
        kno_make_choice(n,data,(KNO_CHOICE_DOSORT|KNO_CHOICE_INCREF));
      kno_index_prefetch(ix,aschoice);
      while (i<n) {
        results[i]=kno_index_get(ix,data[i]); i++;}
      kno_decref(aschoice);
      return kno_wrap_vector(n,results);}
    else return kno_type_error("vector","ixserver_bulk_get",keys);
  else if (TABLEP(index))
    if (VECTORP(keys)) {
      int i = 0, n = VEC_LEN(keys);
      lispval *data = VEC_DATA(keys),
        *results = u8_alloc_n(n,lispval);
      while (i<n) {
        results[i]=kno_get(index,data[i],EMPTY);
        i++;}
      return kno_wrap_vector(n,results);}
    else return kno_type_error("vector","ixserver_bulk_get",keys);
  else return kno_type_error("index","ixserver_get",VOID);
}
static lispval ixserver_get_size(lispval index,lispval key)
{
  if (KNO_INDEXP(index)) {
    lispval value = kno_index_get(kno_indexptr(index),key);
    int size = KNO_CHOICE_SIZE(value);
    kno_decref(value);
    return KNO_INT(size);}
  else if (TABLEP(index)) {
    lispval value = kno_get(index,key,EMPTY);
    int size = KNO_CHOICE_SIZE(value);
    kno_decref(value);
    return KNO_INT(size);}
  else return kno_type_error("index","ixserver_get",VOID);
}
static lispval ixserver_keys(lispval index)
{
  if (KNO_INDEXP(index))
    return kno_index_keys(kno_indexptr(index));
  else if (TABLEP(index))
    return kno_getkeys(index);
  else return kno_type_error("index","ixserver_get",VOID);
}
static lispval ixserver_sizes(lispval index)
{
  if (KNO_INDEXP(index))
    return kno_index_sizes(kno_indexptr(index));
  else if (TABLEP(index)) {
    lispval results = EMPTY, keys = kno_getkeys(index);
    DO_CHOICES(key,keys) {
      lispval value = kno_get(index,key,EMPTY);
      lispval keypair = kno_conspair(kno_incref(key),KNO_INT(KNO_CHOICE_SIZE(value)));
      CHOICE_ADD(results,keypair);
      kno_decref(value);}
    kno_decref(keys);
    return results;}
  else return kno_type_error("index","ixserver_get",VOID);
}
static lispval ixserver_writablep(lispval index)
{
  return KNO_FALSE;
}

/* Configuration methods */

static int serve_pool(lispval var,lispval val,void *data)
{
  kno_pool p;
  if (CHOICEP(val)) {
    DO_CHOICES(v,val) {
      int retval = serve_pool(var,v,data);
      if (retval<0) return retval;}
    return 1;}
  else if (KNO_POOLP(val)) p = kno_lisp2pool(val);
  else if (STRINGP(val)) {
    if ((p = kno_name2pool(CSTRING(val))) == NULL)
      p = kno_use_pool(CSTRING(val),0,VOID);}
  else return kno_reterr(kno_NotAPool,"serve_pool",NULL,val);
  if (p)
    if (served_poolp(p)) return 0;
    else if (n_served_pools>=KNO_DBSERV_MAX_POOLS) {
      kno_seterr(_("too many pools to serve"),"serve_pool",NULL,val);
      return -1;}
    else {
      u8_logf(LOG_NOTICE,"SERVE_POOL","Serving objects from %s",p->poolid);
      served_pools[n_served_pools++]=p;
      return n_served_pools;}
  else return kno_reterr(kno_NotAPool,"serve_pool",NULL,val);
}

static lispval get_served_pools(lispval var,void *data)
{
  lispval result = EMPTY;
  int i = 0; while (i<n_served_pools) {
    kno_pool p = served_pools[i++];
    lispval lp = kno_pool2lisp(p);
    CHOICE_ADD(result,lp);}
  return result;
}

static int serve_primary_pool(lispval var,lispval val,void *data)
{
  kno_pool p;
  if (KNO_POOLP(val)) p = kno_lisp2pool(val);
  else if (STRINGP(val)) {
    if ((p = kno_name2pool(CSTRING(val))) == NULL)
      p = kno_use_pool(CSTRING(val),0,VOID);}
  else return kno_reterr(kno_NotAPool,"serve_pool",NULL,val);
  if (p)
    if (p == primary_pool) return 0;
    else {primary_pool = p; return 1;}
  else return kno_reterr(kno_NotAPool,"serve_pool",NULL,val);
}

static lispval get_primary_pool(lispval var,void *data)
{
  if (primary_pool) return kno_pool2lisp(primary_pool);
  else return EMPTY;
}

static int serve_index(lispval var,lispval val,void *data)
{
  kno_index ix = NULL;
  if (CHOICEP(val)) {
    DO_CHOICES(v,val) {
      int retval = serve_index(var,v,data);
      if (retval<0) return retval;}
    return 1;}
  else if (KNO_INDEXP(val))
    ix = kno_indexptr(val);
  else if (STRINGP(val))
    ix = kno_get_index(CSTRING(val),0,VOID);
  else if (val == KNO_TRUE)
    if (kno_background) ix = (kno_index)kno_background;
    else {
      u8_logf(LOG_WARN,_("No background"),
              "No current background index to serve");
      return 0;}
  else {}
  if (ix) {
    u8_logf(LOG_NOTICE,"SERVE_INDEX","Serving index %s",ix->indexid);
    kno_add_to_aggregate_index(primary_index,ix);
    return 1;}
  else return kno_reterr(kno_BadIndexSpec,"serve_index",NULL,val);
}

static lispval get_served_indexes(lispval var,void *data)
{
  return kno_index_ref((kno_index)(primary_index));
}

/* Initialization */

lispval kno_dbserv_module;

static int dbserv_init = 0;

void kno_init_dbserv_c()
{
  lispval module;

  if (dbserv_init) return; else dbserv_init = 1;

  init_timestamp = (int)time(NULL);

  KNO_INIT_STATIC_CONS(&server_locks,kno_hashtable_type);
  KNO_INIT_STATIC_CONS(&server_locks_inv,kno_hashtable_type);
  kno_init_hashtable(&server_locks,0,NULL);
  kno_init_hashtable(&server_locks_inv,0,NULL);

  u8_init_mutex(&server_locks_lock);
  u8_init_mutex(&changelog_lock);

  module = kno_make_hashtable(NULL,67);

  kno_defn(module,kno_make_cprim1("POOL-DATA",server_pool_data,0));
  kno_defn(module,kno_make_cprim1("OID-VALUE",server_oid_value,1));
  kno_defn(module,kno_make_cprim1("FETCH-OIDS",server_fetch_oids,1));
  kno_defn(module,kno_make_cprim1("GET-LOAD",server_get_load,0));
  kno_defn(module,kno_make_cprim0("UPDATE-LOCKS!",update_locks_prim));
  kno_defn(module,kno_make_cprim2("STORE-OID!",store_oid_proc,2));
  kno_defn(module,kno_make_cprim2("BULK-COMMIT",bulk_commit_cproc,2));


  kno_defn(module,kno_make_cprim1("ISERVER-GET",iserver_get,1));
  kno_defn(module,kno_make_cprim2("ISERVER-ADD!",iserver_add,2));
  kno_defn(module,kno_make_cprim2("ISERVER-DROP!",iserver_drop,2));
  kno_defn(module,kno_make_cprim1("ISERVER-BULK-ADD!",iserver_bulk_add,1));
  kno_defn(module,kno_make_cprim1x("ISERVER-BULK-GET",iserver_bulk_get,1,
                                 kno_vector_type,VOID));
  kno_defn(module,kno_make_cprim1("ISERVER-GET-SIZE",iserver_get_size,1));
  kno_defn(module,kno_make_cprim0("ISERVER-KEYS",iserver_keys));
  kno_defn(module,kno_make_cprim0("ISERVER-SIZES",iserver_sizes));
  kno_defn(module,kno_make_cprim0("ISERVER-WRITABLE?",iserver_writablep));
  kno_defn(module,kno_make_cprim2("ISERVER-CHANGES",iserver_changes,2));

  kno_defn(module,kno_make_cprim2x("IXSERVER-GET",ixserver_get,2,
                                 -1,VOID,-1,VOID));
  kno_defn(module,kno_make_cprim3("IXSERVER-ADD!",ixserver_add,3));
  kno_defn(module,kno_make_cprim3("IXSERVER-DROP!",ixserver_drop,3));
  kno_defn(module,kno_make_cprim2x("IXSERVER-BULK-GET",ixserver_bulk_get,2,
                                 -1,VOID,
                                 kno_vector_type,VOID));
  kno_defn(module,kno_make_cprim2("IXSERVER-BULK-ADD!",ixserver_bulk_add,2));
  kno_defn(module,kno_make_cprim2x("IXSERVER-GET-SIZE",ixserver_get_size,2,
                                 -1,VOID,-1,VOID));
  kno_defn(module,kno_make_cprim1x("IXSERVER-KEYS",ixserver_keys,1,
                                 -1,VOID));
  kno_defn(module,kno_make_cprim1x("IXSERVER-SIZES",ixserver_sizes,1,
                                 -1,VOID));
  kno_defn(module,kno_make_cprim1x("IXSERVER-WRITABLE?",ixserver_writablep,1,
                                 -1,VOID));
  kno_defn(module,kno_make_cprim3("IXSERVER-CHANGES",ixserver_changes,3));

  kno_defn(module,kno_make_cprim0("GET-SYNCSTAMP",get_syncstamp_prim));
  kno_defn(module,kno_make_cprim2("LOCK-OID",lock_oid_prim,2));
  kno_defn(module,kno_make_cprim3("UNLOCK-OID",unlock_oid_prim,3));
  kno_defn(module,kno_make_cprim2("CLEAR-OID-LOCK",clear_server_lock_prim,2));
  kno_defn(module,kno_make_cprim1("BREAK-OID-LOCK",break_server_lock_prim,1));
  kno_defn(module,kno_make_cprim1("UNLOCK-ALL",unlock_all_prim,1));
  kno_defn(module,kno_make_cprim2("OID-CHANGES",oid_server_changes,2));


  kno_register_config("SERVEPOOLS","OID pools to be served",
                     get_served_pools,
                     serve_pool,
                     NULL);
  kno_register_config("PRIMARYPOOL","OID pool where new OIDs are allocated",
                     get_primary_pool,
                     serve_primary_pool,
                     NULL);
  kno_register_config("SERVEINDEXES","indexes to be served",
                     get_served_indexes,
                     serve_index,
                     NULL);
  kno_register_config("DBSERV:LOGLEVEL","the dbserv loglevel",
                     kno_intconfig_get,
                     kno_loglevelconfig_set,
                     &dbserv_loglevel);
  kno_register_config("LOCKSFILE","location of the persistent locks file",
                     config_get_locksfile,
                     config_set_locksfile,
                     NULL);

  primary_index = kno_make_aggregate_index(KNO_FALSE,32,0,NULL);

  kno_dbserv_module = module;
}

static int dbserv_initialized = 0;

KNO_EXPORT int kno_init_dbserv()
{
  if (dbserv_initialized) return dbserv_initialized;
  dbserv_initialized = 211*kno_init_storage();

  u8_register_source_file(_FILEINFO);
  kno_init_dbserv_c();

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
