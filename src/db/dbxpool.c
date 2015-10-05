/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_DTYPEIO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/dbfile.h"
#include "framerd/dbxfile.h"

static struct FD_POOL_HANDLER dbxpool_handler;

FD_EXPORT
int fd_init_dbx_pool(u8_string filename,u8_string dbname,FD_OID base,unsigned int cap,unsigned int load,u8_string label)
{
  DB *dbp;
  int retval=db_create(&dbp,NULL,0);
  if (retval) {
    fd_seterr(db_strerror(retval),"fd_init_dbx_pool",filename,FD_VOID);
    return -1;}
  else {
    struct FD_BYTE_OUTPUT out;
    char namebuf[1024]; DBT key, value;
    unsigned char data[1024];
    if (dbname) sprintf(namebuf,"%s_metadata",dbname);
    else strcpy(namebuf,"metadata");
    if (retval=(dbp->open(filename,namebuf,DB_BTREE,DB_CREATE|DB_EXCL,
                          (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP)))) {
      fd_seterr(db_strerror(retval),"fd_init_dbx_pool",filename,FD_VOID);
      return retval;}
    memset(&key,0,sizeof(key)); memset(&value,0,sizeof(value));
    key.data="poolinfo"; key.size=sizeof("poolinfo");
    out.start=out.ptr=data; out.end=data+1024; out.mpool=NULL;
    fd_write_4bytes(&out,FD_OID_HIGH(base));
    fd_write_4bytes(&out,FD_OID_LOW(base));
    fd_write_4bytes(&out,cap);
    if (label) {
      int len=strlen(label);
      fd_write_byte(&out,dt_string_type);
      fd_write_4bytes(&out,len);
      fd_write_bytes(&out,label,len);}
    else fd_write_dtype(&out,FD_VOID);
    value.data=data; value.size=out.ptr-out.start;
    if (retval=(dbp->put(dbp,NULL,key,value,0))) {
      fd_seterr(db_strerror(retval),"fd_init_dbx_pool",filename,FD_VOID);
      return -1;}
    if (retval=(dbp->close(dbp,0))) {
      fd_seterr(db_strerror(retval),"fd_init_dbx_pool",filename,FD_VOID);
      return -1;}
    if (dbname) sprintf(namebuf,"%s_pool",dbname);
    else strcpy(namebuf,"pool");
    if (retval=(dbp->open(filename,namebuf,DB_RECNO,DB_CREATE|DB_EXCL,
                          (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP)))) {
      fd_seterr(db_strerror(retval),"fd_init_dbx_pool",filename,FD_VOID);
      return -1;}
    if (retval=(dbp->close(dbp,0))) {
      fd_seterr(db_strerror(retval),"fd_init_dbx_pool",filename,FD_VOID);
      return -1;}
    return 0;}
}

FD_EXPORT fd_pool fd_open_dbx_pool(u8_string filename,u8_string dbname)
{
  DB *dbp;
  int retval=db_create(&dbp,NULL,0);
  if (retval) {
    fd_seterr(db_strerror(retval),"fd_open_dbx_pool",filename,FD_VOID);
    return NULL;}
  else {
    struct FD_BYTE_INPUT in;
    FD_OID base; unsigned int cap, load;
    struct FD_DBX_POOL *dbxp;
    char namebuf[1024]; DBT key, value;
    unsigned char data[1024];
    u8_string rname=u8_realpath(filename,NULL);
    if (dbname) sprintf(namebuf,"%s_metadata",dbname);
    else strcpy(namebuf,"metadata");
    if (retval=(dbp->open(dbp,rname,namebuf,DB_BTREE,DB_RDONLY,
                          (S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP)))) {
      fd_seterr(db_strerror(retval),"fd_open_dbx_pool",filename,FD_VOID);
      u8_free(rname);
      return retval;}
    memset(&key,0,sizeof(key)); memset(&value,0,sizeof(value));
    if (retval=(dbp->get(dbp,NULL,&key,&value,0))) {
      fd_seterr(db_strerror(retval),"fd_open_dbx_pool",filename,FD_VOID);
      u8_free(rname);
      return NULL;}
    else {
      int base_hi, base_lo;
      FD_INIT_BYTE_INPUT(&in,value.data,value.size);
      base_hi=fd_read_4bytes(&in); base_lo=fd_read_4bytes(&in);
      cap=fd_read_4bytes(&in);
      label=fd_read_dtype(&in);
      FD_SET_OID_HI(base,base_hi); FD_SET_OID_LO(base,base_lo);
      if (FD_STRINGP(label)) fp->label=u8_strdup(FD_STRDATA(label));
      fd_decref(label);}
    if (retval=(dbp->close(dbp,0))) {
      fd_seterr(db_strerror(retval),"fd_open_dbx_pool",filename,FD_VOID);
      u8_free(rname);
      return NULL;}
    if (dbname) sprintf(namebuf,"%s_pool",dbname);
    else strcpy(namebuf,"pool");
    if (retval=(dbp->open(dbp,rname,dbname,DB_RECNO,DB_RDONLY|DB_THREAD,0))) {
      fd_seterr(db_strerror(retval),"fd_open_dbx_pool",filename,FD_VOID);
      u8_free(rname);
      return NULL;}
    dbxp=u8_alloc(struct FD_DBX_POOL);
    fd_init_pool((fd_pool)dbxp,base,capacity,&dbxpool_handler,filename,rname);
    dbxp->dbp=dbp; dbxp->read_only=1;
    dbxp->filename=filename; dbxp->dbname=dbname; u8_free(rname);
    u8_init_mutex(&(dbxp->lock));
    return (fd_pool) dbxp;}
}

static int dbx_pool_lock(struct FD_DBX_POOL *fp,int use_mutex)
{
  if (FD_DBXPOOL_LOCKED(fp)) return 1;
  else {
    DB *dbp=fp->dbp; int retval; char namebuf[256];
    u8_string rname=u8_realpath(fp->filename,NULL);
    if (dbname) sprintf(namebuf,"%s_pool",dbname);
    else strcpy(namebuf,"pool");
    u8_lock_struct(fp);
    if (retval=(dbp->close(dbp,0))) {
      fd_seterr(db_strerror(retval),"dbx_pool_lock",fp->filename,FD_VOID);
      u8_unlock_struct(fp);
      u8_free(rname);
      return -1;}
    if (retval=(dbp->open(dbp,rname,dbname,DB_RECNO,DB_THREAD,0))) {
      fd_seterr(db_strerror(retval),"dbx_pool_lock",fp->filename,FD_VOID);
      u8_unlock_struct(fp);
      u8_free(rname);
      return -1;}
    fp->locked=1;
    u8_unlock_struct(fp);
    return 1;}
}

static int dbx_pool_unlock(struct FD_DBX_POOL *fp,int use_mutex)
{
  if (FD_DBXPOOL_LOCKED(fp)) {
    DB *dbp=fp->dbp; int retval; char namebuf[256];
    u8_string rname=u8_realpath(fp->filename,NULL);
    if (dbname) sprintf(namebuf,"%s_pool",dbname);
    else strcpy(namebuf,"pool");
    u8_lock_struct(fp);
    if (retval=(dbp->close(dbp,0))) {
      fd_seterr(db_strerror(retval),"dbx_pool_unlock",fp->filename,FD_VOID);
      u8_unlock_struct(fp);
      u8_free(rname);
      return -1;}
    if (retval=(dbp->open(dbp,rname,dbname,DB_RECNO,DB_RDONLY|DB_THREAD,0))) {
      fd_seterr(db_strerror(retval),"dbx_pool_unlock",fp->filename,FD_VOID);
      u8_unlock_struct(fp);
      u8_free(rname);
      return -1;}
    fp->locked=1;
    u8_unlock_struct(fp);
    return 1;}
  else return 0;
}

static int dbx_pool_load(fd_pool p)
{
  fd_dbx_pool fp=(fd_dbx_pool)p;
  if (FD_DBXPOOL_LOCKED(fp)) return fp->load;
  else {
    int load; DB *dbp=fp->dbp;
    struct DB_BTREE_STAT *stats;
    u8_lock_struct(fp);
    if (dbp->stat(dbp,&stats,NULL,DB_RECORDCOUNT)) {
      fd_seterr(db_strerror(retval),"dbx_pool_load",fp->filename,FD_VOID);
      u8_unlock_struct(fp);
      return -1;}
    u8_unlock_struct(fp);
    return stats->bt_ndata;}
}

static fdtype dbx_pool_fetch(fd_pool p,fdtype oid)
{
  fdtype value; int retval;
  struct FD_DBX_POOL *fp=(struct FD_DBX_POOL *)p;
  FD_OID addr=FD_OID_ADDR(oid); struct FD_BYTE_INPUT in;
  int offset=FD_OID_DIFFERENCE(addr,fp->base);
  DB *dbp=fp->dbp; DBT key, value; db_recno_t loc=offset;
  u8_lock_struct(fp);
  key.data=&loc; key.size=sizeof(db_recno_t);
  if (retval=(dbp->get(dbp,NULL,&key,&value)))
    if (retval==DB_NOTFOUND) {
      u8_unlock_struct(fp);
      return fd_err(fd_UnallocatedOID,"file_pool_fetch",fp->cid,oid);}
    else if (retval==DB_KEYEMPTY) {
      u8_unlock_struct(fp);
      return FD_VOID;}
    else {
      u8_unlock_struct(fp);
      return fd_err(db_strerror(retval),"dbx_pool_fetch",fp->cid,oid);}
  FD_INIT_BYTE_INPUT(&in,value.data,value.size);
  result=fd_read_dtype(&in);
  u8_unlock_struct(fp);
  return result;
}

struct FETCH_SCHEDULE {
  unsigned int offset, serial; fdtype oid;};

static int compare_offsets(const void *x1,const void *x2)
{
  const struct FETCH_SCHEDULE *s1=x1, *s2=x2;
  if (s1->offset<s2->offset) return -1;
  else if (s1->offset>s2->offset) return 1;
  else return 0;
}

static fdtype *dbx_pool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_DBX_POOL *fp=(struct FD_DBX_POOL *)p;
  struct FETCH_SCHEDULE *schedule; fdtype *values;
  FD_OID base=fp->base;
  DB *dbp=fp->dbp; DBC *cursor; int retval;
  u8_lock_struct(fp);
  if (retval=(dbp->cursor(dbp,NULL,&cursor,0))) {
    fd_seterr(db_strerror(retval),"dbx_pool_fetchn",fp->filename,FD_VOID);
    u8_unlock_struct(fp);
    return NULL;}
  schedule=u8_alloc_n(n,struct FETCH_SCHEDULE);
  values=u8_alloc_n(n,fdtype);
  {int i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      schedule[i].oid=oids[i]; schedule[i].serial=i;
      schedule[i].offset=FD_OID_DIFFERENCE(addr,base);
      i++;}}
  qsort(schedule,n,sizeof(struct FETCH_SCHEDULE),compare_offsets);
  {int i=0, retval; DBT key, value; db_recno_t recno;
    key.data=&recno; key.size=sizeof(db_recno_t);
    while (i<n) {
      fdtype result; struct FD_BYTE_INPUT in;
      recno=schedule[i].offset;
      if (retval=(dbc->c_get(dbc,&key,&value,0))) {
        fd_seterr(db_strerror(retval),"dbx_pool_fetchn",fp->filename,oids[schedule[i].serial]);
        u8_unlock_struct(fp);
        return NULL;}
      else {
        FD_INIT_BYTE_INPUT(in,value.data,value.size);
        result=fd_read_dtype(&in);
        values[schedule[i].serial]=result;}
      i++;}}
  dbc->c_close(dbc);
  u8_unlock_struct(fp);
  u8_free(schedule);
  return values;
}

static int file_pool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_DBX_POOL *fp=(struct FD_DBX_POOL *)p;
  struct FETCH_SCHEDULE *schedule; fdtype *values;
  FD_OID base=fp->base;
  DB *dbp=fp->dbp; DBC *cursor; int retval;
  struct FD_BYTE_OUTPUT out;
  u8_lock_struct(fp);
  if (retval=(dbp->cursor(dbp,NULL,&cursor,0))) {
    fd_seterr(db_strerror(retval),"dbx_pool_storen",fp->filename,FD_VOID);
    u8_unlock_struct(fp);
    return NULL;}
  schedule=u8_alloc_n(n,struct FETCH_SCHEDULE);
  values=u8_alloc_n(n,fdtype);
  FD_INIT_BYTE_OUTPUT(&out,4096);
  {int i=0; while (i<n) {
      FD_OID addr=FD_OID_ADDR(oids[i]);
      schedule[i].oid=oids[i]; schedule[i].serial=i;
      schedule[i].offset=FD_OID_DIFFERENCE(addr,base);
      i++;}}
  qsort(schedule,n,sizeof(struct FETCH_SCHEDULE),compare_offsets);
  {int i=0, retval; DBT key, value; db_recno_t recno;
    key.data=&recno; key.size=sizeof(db_recno_t);
    while (i<n) {
      fdtype result;
      recno=schedule[i].offset;
      out.ptr=out.start;
      if (fd_write_dtype(&out,values[schedule[i].serial])<0) {
        u8_unlock_struct(fp);
        u8_free(schedule);
        u8_free(values);
        return NULL;}
      value.data=out.start; value.size=out.ptr-out.start;
      if (retval=(dbc->c_put(dbc,&key,&value,0))) {
        fd_seterr(db_strerror(retval),"dbx_pool_fetchn",fp->filename,oids[schedule[i].serial]);
        u8_unlock_struct(fp);
        u8_free(schedule);
        u8_free(values);
        return NULL;}
      i++;}}
  u8_unlock_struct(fp);
  dbc->c_close(dbc);
  u8_free(schedule);
  return values;
}

static fdtype dbx_pool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  struct FD_DBX_POOL *fp=(struct FD_FILE_POOL *)p;
  u8_lock_struct(fp);
  if (!(FD_DBXPOOL_LOCKED(fp))) dbx_pool_lock(fp,0);
  if (!(FD_DBXPOOL_LOCKED(fp)))
    return FD_ERROR_VALUE;
  while (i < n) {
    int retval; unsigned int offset;
    DBT key, value; value.data=NULL; value.size=0;
    if (retval=(dbp->put(dbp,key,value,DB_APPEND))) {
      u8_unlock_struct(fp);
      fd_decref(results);
      return fd_err(db_strerror(retval),"dbx_pool_storen",fp->filename,FD_VOID);}
    offset=*((unsigned int *)db.data);
    FD_OID new_addr=FD_OID_PLUS(fp->base,offset);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    i++; fp->load++; fp->n_locks++;}
  u8_unlock_struct(fp);
  return results;
}

static struct FD_POOL_HANDLER dbxpool_handler={
  "file_pool", 1, sizeof(struct FD_FILE_POOL), 12,
   dbx_pool_close, /* close */
   NULL, /* setcache */
   NULL, /* setbuf */
   dbx_pool_alloc, /* alloc */
   dbx_pool_fetch, /* fetch */
   dbx_pool_fetchn, /* fetchn */
   dbx_pool_load, /* getload */
   dbx_pool_lock, /* lock */
   dbx_pool_unlock, /* release */
   dbx_pool_storen, /* storen */
   NULL, /* swapout */
   NULL, /* metadata */
   NULL}; /* sync */



/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
