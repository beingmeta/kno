/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_POOLS 1
#define FD_INLINE_CHOICES 1
#define FD_INLINE_IPEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8filefns.h>
#if HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#include <libu8/u8srvfns.h>

static fd_pool primary_pool=NULL;
static fd_pool served_pools[FDBSERV_MAX_POOLS];
static int n_served_pools=0;
struct FD_COMPOUND_INDEX *primary_index=NULL;
static int read_only=0, locking=1;

fd_exception fd_PrivateOID=_("private OID");

static int served_poolp(fd_pool p)
{
  int i=0; while (i<n_served_pools)
             if (served_pools[i]==p) return 1;
             else i++;
  return 0;
}

/* Change logs */

static int init_timestamp=0;
static int change_count=0;

#if FD_THREADS_ENABLED
static u8_mutex changelog_lock;
#endif

struct FD_CHANGELOG_ENTRY {
  int moment; fdtype keys;};

struct FD_CHANGELOG {
  int point, max, full;
  struct FD_CHANGELOG_ENTRY *entries;};

static struct FD_CHANGELOG oid_changelog, index_changelog;
static struct FD_SUBINDEX_CHANGELOG {
  fd_index ix; struct FD_CHANGELOG *clog;} *subindex_changelogs=NULL;
static int n_subindex_changelogs=0;

static void init_changelog(struct FD_CHANGELOG *clog,int size)
{
  clog->entries=u8_alloc_n(size,struct FD_CHANGELOG_ENTRY);
  clog->max=size; clog->point=0; clog->full=0;
}

static struct FD_CHANGELOG *get_subindex_changelog(fd_index ix,int make)
{
  struct FD_CHANGELOG *clog=NULL; int i=0;
  fd_lock_mutex(&changelog_lock);
  while (i < n_subindex_changelogs)
    if (subindex_changelogs[i].ix == ix) break; else i++;
  if (subindex_changelogs[i].ix == ix) clog=(subindex_changelogs[i].clog);
  else if (make == 0) clog=NULL;
  else {
    clog=u8_alloc(struct FD_CHANGELOG);
    if (subindex_changelogs)
      subindex_changelogs=
        u8_realloc_n(subindex_changelogs,n_subindex_changelogs+1,
                     struct FD_SUBINDEX_CHANGELOG);
    else subindex_changelogs=u8_alloc(struct FD_SUBINDEX_CHANGELOG);
    subindex_changelogs[n_subindex_changelogs].ix=ix; subindex_changelogs[n_subindex_changelogs].clog=clog;
    init_changelog(clog,1024);
    n_subindex_changelogs++;}
  fd_unlock_mutex(&changelog_lock);
  return clog;
}

static void add_to_changelog(struct FD_CHANGELOG *clog,fdtype keys)
{
  struct FD_CHANGELOG_ENTRY *entries; int point;
  fd_lock_mutex(&changelog_lock);
  entries=clog->entries; point=clog->point;
  if (clog->full) {
    fd_decref(entries[point].keys);}
  entries[point].moment=change_count++;
  entries[point].keys=fd_incref(keys);
  if (clog->full)
    if (clog->point == clog->max) clog->point=0;
    else clog->point++;
  else if (clog->point == clog->max) {
    clog->point=0; clog->full=1;}
  else clog->point++;
  fd_unlock_mutex(&changelog_lock);
}

static fdtype get_changes(struct FD_CHANGELOG *clog,int cstamp,int *new_cstamp)
{
  fdtype result;
  fd_lock_mutex(&changelog_lock);
  {
    int bottom=((clog->full) ? (clog->point) : 0);
    int top=((clog->full) ? ((clog->point) ? (clog->point-1) : (clog->max)) : (clog->point-1));
    struct FD_CHANGELOG_ENTRY *entries=clog->entries;
    *new_cstamp=change_count;
    if ((clog->full == 0) && (clog->point == 0)) result=FD_EMPTY_CHOICE; /* Changelog is empty */
    else if (cstamp < entries[bottom].moment) result=FD_FALSE; /* Too far back. */
    else if (cstamp > entries[top].moment) result=FD_EMPTY_CHOICE; /* No changes. */
    else {
      fdtype changes=FD_EMPTY_CHOICE;
      int i=top, point=clog->point; while (i >= 0)
        if (cstamp <= entries[i].moment) {
          fdtype key=entries[i--].keys;
          fd_incref(key);
          FD_ADD_TO_CHOICE(changes,key);}
        else break;
      if (cstamp > entries[i].moment) {
        i=clog->max; while (i >= point)
          if (cstamp <= entries[i].moment) {
            fdtype key=entries[i--].keys;
            fd_incref(key);
            FD_ADD_TO_CHOICE(changes,key);}
          else break;}
      result=changes;}}
  fd_unlock_mutex(&changelog_lock);
  return result;
}

static fdtype get_syncstamp_prim()
{
  return fd_make_list(2,FD_INT(init_timestamp),FD_INT(change_count));
}

static fdtype oid_server_changes(fdtype sid,fdtype xid)
{
  if (FD_FIX2INT(sid) != init_timestamp) return FD_FALSE;
  else {
    int new_syncstamp;
    fdtype changes=get_changes(&oid_changelog,FD_FIX2INT(xid),&new_syncstamp);
    if (FD_FALSEP(changes))
      return fd_make_list(1,fd_make_list(2,FD_INT(init_timestamp),FD_INT(new_syncstamp)));
    else return fd_conspair(fd_make_list(2,FD_INT(init_timestamp),FD_INT(new_syncstamp)),
                            changes);}
}

static fdtype iserver_changes(fdtype sid,fdtype xid)
{
  if (FD_FIX2INT(sid) != init_timestamp) return FD_FALSE;
  else {
    int new_syncstamp;
    fdtype changes=get_changes(&index_changelog,FD_FIX2INT(xid),&new_syncstamp);
    if (FD_FALSEP(changes))
      return fd_make_list(1,fd_make_list(2,FD_INT(init_timestamp),FD_INT(new_syncstamp)));
    else return fd_conspair(fd_make_list(2,FD_INT(init_timestamp),FD_INT(new_syncstamp)),
                            changes);}
}

static fdtype ixserver_changes(fdtype index,fdtype sid,fdtype xid)
{
  fd_index ix=fd_indexptr(index);
  struct FD_CHANGELOG *clog=get_subindex_changelog(ix,0);
  if (clog == NULL) return FD_EMPTY_CHOICE;
  else if (FD_FIX2INT(sid) != init_timestamp) return FD_FALSE;
  else {
    int new_syncstamp;
    fdtype changes=get_changes(clog,FD_FIX2INT(xid),&new_syncstamp);
    if (FD_FALSEP(changes))
      return fd_make_list(1,fd_make_list(2,FD_INT(init_timestamp),FD_INT(new_syncstamp)));
    else return fd_conspair(fd_make_list(2,FD_INT(init_timestamp),FD_INT(new_syncstamp)),
                            changes);}
}

/** OID Locking **/

static struct FD_HASHTABLE server_locks, server_locks_inv;
static unsigned int n_locks=0;
static FD_STREAM *locks_file=NULL;
static u8_string locks_filename=NULL;
static void update_server_lock_file();

fd_exception OIDNotLocked=_("The OID is not locked");
fd_exception CantLockOID=_("Can't lock OID");

#if FD_THREADS_ENABLED
static u8_mutex server_locks_lock;
#endif

static int lock_oid(fdtype oid,fdtype id)
{
  fdtype holder;
  if (locking == 0) return 1;
  fd_lock_mutex(&server_locks_lock);
  holder=fd_hashtable_get(&server_locks,oid,FD_EMPTY_CHOICE);
  if (FD_EMPTY_CHOICEP(holder)) {
    fd_pool p=fd_oid2pool(oid);
    if ((fd_pool_lock(p,oid)) == 0) {
      fd_unlock_mutex(&server_locks_lock); return 0;}
    fd_hashtable_store(&server_locks,oid,id); n_locks++;
    fd_hashtable_add(&server_locks_inv,id,oid);
    if (locks_file) {
      fd_write_dtype(fd_writebuf(locks_file),oid);
      fd_write_dtype(fd_writebuf(locks_file),id);
      fd_flush_stream(locks_file);}
    fd_unlock_mutex(&server_locks_lock);
    return 1;}
  else if (FDTYPE_EQUAL(id,holder)) {
    fd_unlock_mutex(&server_locks_lock); fd_decref(holder);
    return 1;}
  else {
    fd_unlock_mutex(&server_locks_lock);
    fd_decref(holder);
    return 0;}
}

static int check_server_lock(fdtype oid,fdtype id)
{
  fdtype holder=fd_hashtable_get(&server_locks,oid,FD_EMPTY_CHOICE);
  if (FD_EMPTY_CHOICEP(holder)) return 0;
  else if (FDTYPE_EQUAL(id,holder)) {
    fd_decref(holder); return 1;}
  else {fd_decref(holder); return 0;}
}

static int clear_server_lock(fdtype oid,fdtype id)
{
  fdtype holder;
  if (locking == 0) return 1;
  fd_lock_mutex(&server_locks_lock);
  holder=fd_hashtable_get(&server_locks,oid,FD_EMPTY_CHOICE);
  if (FD_EMPTY_CHOICEP(holder)) {fd_unlock_mutex(&server_locks_lock); return 0;}
  else if (FDTYPE_EQUAL(id,holder)) {
    fdtype all_locks=fd_hashtable_get(&server_locks_inv,id,FD_EMPTY_CHOICE);
    int lock_count=FD_CHOICE_SIZE(all_locks);
    fd_decref(holder);
    fd_hashtable_store(&server_locks,oid,FD_EMPTY_CHOICE);
    if (lock_count == 0)
      return fd_err(OIDNotLocked,"lock_oid",fd_strdata(id),oid);
    else if (lock_count == 1)
      fd_hashtable_store(&server_locks_inv,id,FD_EMPTY_CHOICE);
    else fd_hashtable_drop(&server_locks_inv,id,oid);
    fd_hashtable_drop(&server_locks,oid,FD_VOID); n_locks--;
    if (locks_file) {
      fd_write_dtype(fd_writebuf(locks_file),id);
      fd_write_dtype(fd_writebuf(locks_file),oid);
      fd_flush_stream(locks_file);}
    fd_unlock_mutex(&server_locks_lock);
    return 1;}
  else {
    fd_decref(holder);
    fd_unlock_mutex(&server_locks_lock);
    return 0;}
}

static void remove_all_server_locks(fdtype id)
{
  if (locking == 0) return;
  fd_lock_mutex(&server_locks_lock);
  {
    fdtype locks=fd_hashtable_get(&server_locks_inv,id,FD_EMPTY_CHOICE);
    FD_DO_CHOICES(oid,locks) {
      fd_hashtable_drop(&server_locks,oid,FD_VOID);}
    fd_decref(locks);
    fd_hashtable_store(&server_locks_inv,id,FD_EMPTY_CHOICE);
    fd_unlock_mutex(&server_locks_lock);
  }
}

static int add_to_server_locks_file(fdtype key,fdtype value,void *outfilep)
{
  struct FD_STREAM *out=(fd_stream )outfilep;
  fd_write_dtype(fd_writebuf(out),key);
  fd_write_dtype(fd_writebuf(out),value);
  return 0;
}

static void open_server_lock_stream(u8_string file)
{
  if (u8_file_existsp(file)) {
    struct FD_STREAM *stream=
      fd_open_dtype_file(file,FD_STREAM_READ,65536);
    fd_inbuf in=fd_readbuf(stream);
    fdtype a=fd_read_dtype(in), b=fd_read_dtype(in);
    while (!(FD_EOFP(a))) {
      if (FD_OIDP(a)) lock_oid(a,b); else clear_server_lock(b,a);
      a=fd_read_dtype(in); b=fd_read_dtype(in);}
    fd_close_stream(stream,1);
    u8_removefile(file);}
  locks_file=fd_open_dtype_file(file,FD_STREAM_CREATE,65536);
  locks_filename=u8_strdup(file);
  fd_for_hashtable(&server_locks,add_to_server_locks_file,(void *)locks_file,1);
  fd_flush_stream(locks_file);
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
  fd_lock_mutex(&server_locks_lock);
  temp_file=u8_mkstring("%s.bak",locks_filename);
  if (locks_file) fd_close_stream(locks_file,1);
  u8_movefile(locks_filename,temp_file);
  locks_file=fd_open_dtype_file(locks_filename,FD_STREAM_CREATE,65536);
  fd_for_hashtable(&server_locks,add_to_server_locks_file,(void *)locks_file,1);
  fd_flush_stream(locks_file);
  u8_removefile(temp_file);
  u8_free(temp_file);
  fd_unlock_mutex(&server_locks_lock);
}

static fdtype config_get_locksfile(fdtype var,void U8_MAYBE_UNUSED *data)
{
  if (locks_filename) return FD_FALSE;
  else return fdtype_string(locks_filename);
}

static int config_set_locksfile(fdtype var,fdtype val,void U8_MAYBE_UNUSED *data)
{
  if (locks_filename)
    if ((FD_STRINGP(val)) && (strcmp(FD_STRDATA(val),locks_filename)==0))
      return 0;
    else return fd_reterr(_("Locks file already set"),"fd_set_config",NULL,val);
  else if (FD_STRINGP(val)) {
    open_server_lock_stream(FD_STRDATA(val));
    return 1;}
  else return fd_reterr(fd_TypeError,"fd_set_config",u8_strdup("string"),val);
}

/** OID Access API **/

static fdtype lock_oid_prim(fdtype oid,fdtype id)
{
  if (!(FD_OIDP(oid)))
    return fd_type_error(_("oid"),"lock_oid_prim",oid);
  if ((locking == 0) ||  (lock_oid(oid,id))) {
    return fd_oid_value(oid);}
  else return fd_err(CantLockOID,"lock_oid_prim",NULL,oid);
}

static fdtype unlock_oid_prim(fdtype oid,fdtype id,fdtype value)
{
  if (locking == 0) {
    fd_pool p=fd_oid2pool(oid);
    fd_set_oid_value(oid,value);
    add_to_changelog(&oid_changelog,oid);
    fd_pool_unlock(p,oid,commit_modified);
    return FD_TRUE;}
  else {
    fd_pool p=fd_oid2pool(oid);
    if (check_server_lock(oid,id)) {
      fd_set_oid_value(oid,value);
      clear_server_lock(oid,id);
      add_to_changelog(&oid_changelog,oid);
      fd_pool_unlock(p,oid,commit_modified);
      return FD_TRUE;}
    else return FD_FALSE;}
}

static fdtype clear_server_lock_prim(fdtype oid,fdtype id)
{
  if (locking == 0) return FD_TRUE;
  else if (clear_server_lock(oid,id)) {
    return FD_TRUE;}
  else return FD_FALSE;
}

static fdtype break_server_lock_prim(fdtype oid)
{
  if (locking == 0) return FD_TRUE;
  else {
    fdtype id=fd_hashtable_get(&server_locks,oid,FD_EMPTY_CHOICE);
    if (FD_EMPTY_CHOICEP(id)) return FD_FALSE;
    else {
      clear_server_lock(oid,id);
      fd_decref(id);
      return FD_TRUE;}}
}

static fdtype unlock_all_prim(fdtype id)
{
  remove_all_server_locks(id);
  update_server_lock_file();
  return FD_TRUE;
}

static fdtype update_locks_prim()
{
  update_server_lock_file();
  return FD_VOID;
}

static fdtype store_oid_proc(fdtype oid,fdtype value)
{
  fd_pool p=fd_oid2pool(oid);
  int i=0; while (i < n_served_pools)
    if (served_pools[i] == p) {
      fd_set_oid_value(oid,value);
      add_to_changelog(&oid_changelog,oid);
      /* Commit the pool.  Journalling should now happen on a per-pool
         rather than a server-wide basis. */
      if (fd_pool_commit(p,oid)>=0) {
        fd_pool_unlock(p,oid,leave_modified);
        return FD_TRUE;}
      else i++;}
    else i++;
  return FD_FALSE;
}

static fdtype bulk_commit_cproc(fdtype id,fdtype vec)
{
  int i=0, l=FD_VECTOR_LENGTH(vec);
  fdtype changed_oids=FD_EMPTY_CHOICE;
  /* First check that all the OIDs were really locked under the assigned ID. */
  if (locking) {
    i=0; while (i < l) {
      fdtype oid=FD_VECTOR_REF(vec,i);
      if (!(FD_OIDP(oid))) i=i+2;
      else if (check_server_lock(oid,id)) i=i+2;
      else return fd_err(OIDNotLocked,"bulk_commit_proc",NULL,oid);}}
  /* Then set the corresponding OID value, but don't commit yet. */
  i=0; while (i < l) {
    fdtype oid=FD_VECTOR_REF(vec,i);
    fdtype value=FD_VECTOR_REF(vec,i+1);
    if (FD_OIDP(oid)) {
      fd_set_oid_value(oid,value);
      FD_ADD_TO_CHOICE(changed_oids,oid);}
    i=i+2;}
  fd_commit_oids(changed_oids);
  fd_unlock_oids(changed_oids,leave_modified);
  if (locking) {
    i=0; while (i < l) {
      fdtype oid=FD_VECTOR_REF(vec,i);
      if (FD_OIDP(oid)) clear_server_lock(oid,id);
      i=i+2;}}
  add_to_changelog(&oid_changelog,changed_oids); fd_decref(changed_oids);
  return FD_TRUE;
}

static fdtype iserver_add(fdtype key,fdtype values)
{
  fd_index_add((fd_index)primary_index,key,values);
  add_to_changelog(&index_changelog,key);
  return FD_TRUE;
}

static fdtype ixserver_add(fdtype ixarg,fdtype key,fdtype values)
{
  fd_index ix=fd_indexptr(ixarg);
  struct FD_CHANGELOG *clog=get_subindex_changelog(ix,1);
  fd_index_add(ix,key,values);
  add_to_changelog(clog,key);
  return FD_TRUE;
}

static fdtype iserver_bulk_add(fdtype vec)
{
  if ((read_only) || (primary_index==NULL)) return FD_FALSE;
  else if (FD_VECTORP(vec)) {
    fdtype *data=FD_VECTOR_DATA(vec), keys=FD_EMPTY_CHOICE;
    int i=0, limit=FD_VECTOR_LENGTH(vec);
    while (i < limit) {
      if (FD_VOIDP(data[i])) break;
      else {
        fdtype key=data[i++], value=data[i++];
        fd_index_add((fd_index)primary_index,key,value);
        fd_incref(key); FD_ADD_TO_CHOICE(keys,key);}}
    add_to_changelog(&index_changelog,keys); fd_decref(keys);
    return FD_TRUE;}
  else return FD_VOID;
}

static fdtype ixserver_bulk_add(fdtype ixarg,fdtype vec)
{
  if (read_only) return FD_FALSE;
  else if (FD_VECTORP(vec)) {
    fd_index ix=fd_indexptr(ixarg);
    struct FD_CHANGELOG *clog=get_subindex_changelog(ix,1);
    fdtype *data=FD_VECTOR_DATA(vec), keys=FD_EMPTY_CHOICE;
    int i=0, limit=FD_VECTOR_LENGTH(vec);
    while (i < limit) {
      if (FD_VOIDP(data[i])) break;
      else {
        fdtype key=data[i++], value=data[i++];
        fd_index_add(ix,key,value);
        fd_incref(key); FD_ADD_TO_CHOICE(keys,key);}}
    add_to_changelog(clog,keys); fd_decref(keys);
    return FD_TRUE;}
  else return FD_VOID;
}

static fdtype iserver_drop(fdtype key,fdtype values)
{
  fd_index_drop((fd_index)primary_index,key,values);
  add_to_changelog(&index_changelog,key);
  return FD_TRUE;
}

static fdtype ixserver_drop(fdtype ixarg,fdtype key,fdtype values)
{
  fd_index ix=fd_indexptr(ixarg);
  struct FD_CHANGELOG *clog=get_subindex_changelog(ix,1);
  fd_index_drop(ix,key,values);
  add_to_changelog(clog,key);
  return FD_TRUE;
}

/* pool DB methods */

static fdtype server_get_load(fdtype oid_arg)
{
  if (FD_VOIDP(oid_arg))
    if (primary_pool) {
      int load=fd_pool_load(primary_pool);
      if (load<0) return FD_ERROR_VALUE;
      else return FD_INT(load);}
    else return fd_err(_("No primary pool"),"server_get_load",NULL,FD_VOID);
  else if (FD_OIDP(oid_arg)) {
    fd_pool p=fd_oid2pool(oid_arg);
    int load=fd_pool_load(p);
    if (load<0) return FD_ERROR_VALUE;
    else return FD_INT(load);}
  else return fd_type_error("OID","server_get_load",oid_arg);
}

static fdtype server_oid_value(fdtype x)
{
  fd_pool p=fd_oid2pool(x);
  if (p==NULL)
    return fd_err(fd_AnonymousOID,"server_oid_value",NULL,x);
  else if (served_poolp(p))
    return fd_fetch_oid(p,x);
  else return fd_err(fd_PrivateOID,"server_oid_value",NULL,x);
}

static fdtype server_fetch_oids(fdtype oidvec)
{
  /* We assume here that all the OIDs in oidvec are in the same pool.  This should
     be the case because clients see the different pools and sort accordingly.  */
  fd_pool p=NULL;
  int n=FD_VECTOR_LENGTH(oidvec), fetchn=0;
  fdtype *elts=FD_VECTOR_DATA(oidvec);
  if (n==0)
    return fd_init_vector(NULL,0,NULL);
  else if (!(FD_OIDP(elts[0])))
    return fd_type_error(_("oid vector"),"server_fetch_oids",oidvec);
  else if ((p=(fd_oid2pool(elts[0]))))
    if (served_poolp(p)) {
      fdtype *results=u8_alloc_n(n,fdtype);
      if (p->pool_handler->fetchn) {
        fdtype *fetch=u8_alloc_n(n,fdtype);
        fd_hashtable cache=&(p->pool_cache), locks=&(p->pool_changes);
        int i=0; while (i<n)
                   if ((fd_hashtable_probe_novoid(cache,elts[i])==0) &&
                       (fd_hashtable_probe_novoid(locks,elts[i])==0))
                     fetch[fetchn++]=elts[i++];
                   else i++;
        p->pool_handler->fetchn(p,fetchn,fetch);
        i=0; while (i<n) {
          results[i]=fd_fetch_oid(p,elts[i]); i++;}
        return fd_init_vector(NULL,n,results);}
      else {
        int i=0; while (i<n) {
          results[i]=fd_fetch_oid(p,elts[i]); i++;}
        return fd_init_vector(NULL,n,results);}}
    else return fd_err(fd_PrivateOID,"server_oid_value",NULL,elts[0]);
 else return fd_err(fd_AnonymousOID,"server_oid_value",NULL,elts[0]);
}

static fdtype server_pool_data(fdtype session_id)
{
  int len=n_served_pools;
  fdtype *elts=u8_alloc_n(len,fdtype);
  int i=0; while (i<len) {
    fd_pool p=served_pools[i];
    fdtype base=fd_make_oid(p->pool_base);
    fdtype capacity=FD_INT(p->pool_capacity);
    fdtype ro=(U8_BITP(p->pool_flags,FDB_READ_ONLY)) ? (FD_FALSE) : (FD_TRUE);
    elts[i++]=
      ((p->pool_label) ?
       (fd_make_list(4,base,capacity,ro,fdtype_string(p->pool_label))) :
       (fd_make_list(3,base,capacity,ro)));}
  return fd_init_vector(NULL,len,elts);
}

/* index DB methods */

static fdtype iserver_get(fdtype key)
{
  return fd_index_get((fd_index)(primary_index),key);
}
static fdtype iserver_bulk_get(fdtype keys)
{
  if (FD_VECTORP(keys)) {
    int i=0, n=FD_VECTOR_LENGTH(keys), retval;
    fdtype *data=FD_VECTOR_DATA(keys), *results=u8_alloc_n(n,fdtype);
    /* |FD_CHOICE_ISATOMIC */
    fdtype aschoice=fd_make_choice
      (n,data,(FD_CHOICE_DOSORT|FD_CHOICE_INCREF));
    retval=fd_index_prefetch((fd_index)(primary_index),aschoice);
    if (retval<0) {
      fd_decref(aschoice); u8_free(results);
      return FD_ERROR_VALUE;}
    while (i<n) {
      results[i]=fd_index_get((fd_index)(primary_index),data[i]); i++;}
    fd_decref(aschoice);
    return fd_init_vector(NULL,n,results);}
  else return fd_type_error("vector","iserver_bulk_get",keys);
}
static fdtype iserver_get_size(fdtype key)
{
  fdtype value=fd_index_get((fd_index)(primary_index),key);
  int size=FD_CHOICE_SIZE(value);
  fd_decref(value);
  return FD_INT(size);
}
static fdtype iserver_keys(fdtype key)
{
  return fd_index_keys((fd_index)(primary_index));
}
static fdtype iserver_sizes(fdtype key)
{
  return fd_index_sizes((fd_index)(primary_index));
}
static fdtype iserver_writablep()
{
  return FD_FALSE;
}

static fdtype ixserver_get(fdtype index,fdtype key)
{
  if ((FD_INDEXP(index))||(FD_TYPEP(index,fd_raw_index_type)))
    return fd_index_get(fd_indexptr(index),key);
  else if (FD_TABLEP(index))
    return fd_get(index,key,FD_EMPTY_CHOICE);
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_bulk_get(fdtype index,fdtype keys)
{
  if ((FD_INDEXP(index))||(FD_TYPEP(index,fd_raw_index_type)))
    if (FD_VECTORP(keys)) {
      fd_index ix=fd_indexptr(index);
      int i=0, n=FD_VECTOR_LENGTH(keys);
      fdtype *data=FD_VECTOR_DATA(keys),
        *results=u8_alloc_n(n,fdtype);
      fdtype aschoice=
        fd_make_choice(n,data,(FD_CHOICE_DOSORT|FD_CHOICE_INCREF));
      fd_index_prefetch(ix,aschoice);
      while (i<n) {
        results[i]=fd_index_get(ix,data[i]); i++;}
      fd_decref(aschoice);
      return fd_init_vector(NULL,n,results);}
    else return fd_type_error("vector","ixserver_bulk_get",keys);
  else if (FD_TABLEP(index))
    if (FD_VECTORP(keys)) {
      int i=0, n=FD_VECTOR_LENGTH(keys);
      fdtype *data=FD_VECTOR_DATA(keys),
        *results=u8_alloc_n(n,fdtype);
      while (i<n) {
        results[i]=fd_get(index,data[i],FD_EMPTY_CHOICE);
        i++;}
      return fd_init_vector(NULL,n,results);}
    else return fd_type_error("vector","ixserver_bulk_get",keys);
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_get_size(fdtype index,fdtype key)
{
  if ((FD_INDEXP(index))||(FD_TYPEP(index,fd_raw_index_type))) {
    fdtype value=fd_index_get(fd_indexptr(index),key);
    int size=FD_CHOICE_SIZE(value);
    fd_decref(value);
    return FD_INT(size);}
  else if (FD_TABLEP(index)) {
    fdtype value=fd_get(index,key,FD_EMPTY_CHOICE);
    int size=FD_CHOICE_SIZE(value);
    fd_decref(value);
    return FD_INT(size);}
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_keys(fdtype index)
{
  if ((FD_INDEXP(index))||(FD_TYPEP(index,fd_raw_index_type)))
    return fd_index_keys(fd_indexptr(index));
  else if (FD_TABLEP(index))
    return fd_getkeys(index);
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_sizes(fdtype index)
{
  if ((FD_INDEXP(index))||(FD_TYPEP(index,fd_raw_index_type)))
    return fd_index_sizes(fd_indexptr(index));
  else if (FD_TABLEP(index)) {
    fdtype results=FD_EMPTY_CHOICE, keys=fd_getkeys(index);
    FD_DO_CHOICES(key,keys) {
      fdtype value=fd_get(index,key,FD_EMPTY_CHOICE);
      fdtype keypair=fd_conspair(fd_incref(key),FD_INT(FD_CHOICE_SIZE(value)));
      FD_ADD_TO_CHOICE(results,keypair);
      fd_decref(value);}
    fd_decref(keys);
    return results;}
  else return fd_type_error("index","ixserver_get",FD_VOID);
}
static fdtype ixserver_writablep(fdtype index)
{
  return FD_FALSE;
}

/* Configuration methods */

static int serve_pool(fdtype var,fdtype val,void *data)
{
  fd_pool p;
  if (FD_CHOICEP(val)) {
    FD_DO_CHOICES(v,val) {
      int retval=serve_pool(var,v,data);
      if (retval<0) return retval;}
    return 1;}
  else if (FD_POOLP(val)) p=fd_lisp2pool(val);
  else if (FD_STRINGP(val)) {
    if ((p=fd_name2pool(FD_STRDATA(val)))==NULL)
      p=fd_use_pool(FD_STRDATA(val),0);}
  else return fd_reterr(fd_NotAPool,"serve_pool",NULL,val);
  if (p)
    if (served_poolp(p)) return 0;
    else if (n_served_pools>=FDBSERV_MAX_POOLS) {
      fd_seterr(_("too many pools to serve"),"serve_pool",NULL,val);
      return -1;}
    else {
      u8_log(LOG_INFO,"SERVE_POOL","Serving objects from %s",p->pool_cid);
      served_pools[n_served_pools++]=p;
      return n_served_pools;}
  else return fd_reterr(fd_NotAPool,"serve_pool",NULL,val);
}

static fdtype get_served_pools(fdtype var,void *data)
{
  fdtype result=FD_EMPTY_CHOICE;
  int i=0; while (i<n_served_pools) {
    fd_pool p=served_pools[i++];
    fdtype lp=fd_pool2lisp(p);
    FD_ADD_TO_CHOICE(result,lp);}
  return result;
}

static int serve_primary_pool(fdtype var,fdtype val,void *data)
{
  fd_pool p;
  if (FD_POOLP(val)) p=fd_lisp2pool(val);
  else if (FD_STRINGP(val)) {
    if ((p=fd_name2pool(FD_STRDATA(val)))==NULL)
      p=fd_use_pool(FD_STRDATA(val),0);}
  else return fd_reterr(fd_NotAPool,"serve_pool",NULL,val);
  if (p)
    if (p==primary_pool) return 0;
    else {primary_pool=p; return 1;}
  else return fd_reterr(fd_NotAPool,"serve_pool",NULL,val);
}

static fdtype get_primary_pool(fdtype var,void *data)
{
  if (primary_pool) return fd_pool2lisp(primary_pool);
  else return FD_EMPTY_CHOICE;
}

static int serve_index(fdtype var,fdtype val,void *data)
{
  fd_index ix=NULL;
  if (FD_CHOICEP(val)) {
    FD_DO_CHOICES(v,val) {
      int retval=serve_index(var,v,data);
      if (retval<0) return retval;}
    return 1;}
  else if (FD_INDEXP(val)) ix=fd_indexptr(val);
  else if (FD_STRINGP(val))
    ix=fd_get_index(FD_STRDATA(val),0);
  else if (val==FD_TRUE)
    if (fd_background) ix=(fd_index)fd_background;
    else {
      u8_log(LOG_WARN,_("No background"),"No current background index");
      return 0;}
  else {}
  if (ix) {
    u8_log(LOG_NOTICE,"SERVE_INDEX","Serving index %s",ix->index_cid);
    fd_add_to_compound_index(primary_index,ix);
    return 1;}
  else return fd_reterr(fd_BadIndexSpec,"serve_index",NULL,val);
}

static fdtype get_served_indices(fdtype var,void *data)
{
  return fd_index2lisp((fd_index)(primary_index));
}

/* Initialization */

fdtype fd_fdbserv_module;

static int dbserv_init=0;

void fd_init_dbserv_c()
{
  fdtype module;

  if (dbserv_init) return; else dbserv_init=1;

  init_timestamp=(int)time(NULL);

  FD_INIT_STATIC_CONS(&server_locks,fd_hashtable_type);
  FD_INIT_STATIC_CONS(&server_locks_inv,fd_hashtable_type);
  fd_init_hashtable(&server_locks,0,NULL);
  fd_init_hashtable(&server_locks_inv,0,NULL);

#if FD_THREADS_ENABLED
  fd_init_mutex(&server_locks_lock);
  fd_init_mutex(&changelog_lock);
#endif

  module=fd_make_hashtable(NULL,67);

  fd_defn(module,fd_make_cprim1("POOL-DATA",server_pool_data,0));
  fd_defn(module,fd_make_cprim1("OID-VALUE",server_oid_value,1));
  fd_defn(module,fd_make_cprim1("FETCH-OIDS",server_fetch_oids,1));
  fd_defn(module,fd_make_cprim1("GET-LOAD",server_get_load,0));
  fd_defn(module,fd_make_cprim0("UPDATE-LOCKS!",update_locks_prim,0));
  fd_defn(module,fd_make_cprim2("STORE-OID!",store_oid_proc,2));
  fd_defn(module,fd_make_cprim2("BULK-COMMIT",bulk_commit_cproc,2));


  fd_defn(module,fd_make_cprim1("ISERVER-GET",iserver_get,1));
  fd_defn(module,fd_make_cprim2("ISERVER-ADD!",iserver_add,2));
  fd_defn(module,fd_make_cprim2("ISERVER-DROP!",iserver_drop,2));
  fd_defn(module,fd_make_cprim1("ISERVER-BULK-ADD!",iserver_bulk_add,1));
  fd_defn(module,fd_make_cprim1x("ISERVER-BULK-GET",iserver_bulk_get,1,
                                 fd_vector_type,FD_VOID));
  fd_defn(module,fd_make_cprim1("ISERVER-GET-SIZE",iserver_get_size,1));
  fd_defn(module,fd_make_cprim0("ISERVER-KEYS",iserver_keys,0));
  fd_defn(module,fd_make_cprim0("ISERVER-SIZES",iserver_sizes,0));
  fd_defn(module,fd_make_cprim0("ISERVER-WRITABLE?",iserver_writablep,0));
  fd_defn(module,fd_make_cprim2("ISERVER-CHANGES",iserver_changes,2));

  fd_defn(module,fd_make_cprim2x("IXSERVER-GET",ixserver_get,2,
                                 -1,FD_VOID,-1,FD_VOID));
  fd_defn(module,fd_make_cprim3("IXSERVER-ADD!",ixserver_add,3));
  fd_defn(module,fd_make_cprim3("IXSERVER-DROP!",ixserver_drop,3));
  fd_defn(module,fd_make_cprim2x("IXSERVER-BULK-GET",ixserver_bulk_get,2,
                                 -1,FD_VOID,
                                 fd_vector_type,FD_VOID));
  fd_defn(module,fd_make_cprim2("IXSERVER-BULK-ADD!",ixserver_bulk_add,2));
  fd_defn(module,fd_make_cprim2x("IXSERVER-GET-SIZE",ixserver_get_size,2,
                                 -1,FD_VOID,-1,FD_VOID));
  fd_defn(module,fd_make_cprim1x("IXSERVER-KEYS",ixserver_keys,1,
                                 -1,FD_VOID));
  fd_defn(module,fd_make_cprim1x("IXSERVER-SIZES",ixserver_sizes,1,
                                 -1,FD_VOID));
  fd_defn(module,fd_make_cprim1x("IXSERVER-WRITABLE?",ixserver_writablep,1,
                                 -1,FD_VOID));
  fd_defn(module,fd_make_cprim3("IXSERVER-CHANGES",ixserver_changes,3));

  fd_defn(module,fd_make_cprim0("GET-SYNCSTAMP",get_syncstamp_prim,0));
  fd_defn(module,fd_make_cprim2("LOCK-OID",lock_oid_prim,2));
  fd_defn(module,fd_make_cprim3("UNLOCK-OID",unlock_oid_prim,3));
  fd_defn(module,fd_make_cprim2("CLEAR-OID-LOCK",clear_server_lock_prim,2));
  fd_defn(module,fd_make_cprim1("BREAK-OID-LOCK",break_server_lock_prim,1));
  fd_defn(module,fd_make_cprim1("UNLOCK-ALL",unlock_all_prim,1));
  fd_defn(module,fd_make_cprim2("OID-CHANGES",oid_server_changes,2));


  fd_register_config("SERVEPOOLS","OID pools to be served",
                     get_served_pools,
                     serve_pool,
                     NULL);
  fd_register_config("PRIMARYPOOL","OID pool where new OIDs are allocated",
                     get_primary_pool,
                     serve_primary_pool,
                     NULL);
  fd_register_config("SERVEINDICES","indices to be served",
                     get_served_indices,
                     serve_index,
                     NULL);
  fd_register_config("LOCKSFILE","location of the persistent locks file",
                     config_get_locksfile,
                     config_set_locksfile,
                     NULL);

  primary_index=(fd_compound_index)fd_make_compound_index(0,NULL);

  fd_fdbserv_module=module;
}

static int fddbserv_initialized=0;

FD_EXPORT int fd_init_fddbserv()
{
  if (fddbserv_initialized) return fddbserv_initialized;
  fddbserv_initialized=211*fd_init_dblib();

  u8_register_source_file(_FILEINFO);
  fd_init_dbserv_c();

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
