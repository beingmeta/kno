/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* leveldb.c
   This implements FramerD bindings to leveldb.
   Copyright (C) 2007-2017 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"
#include "framerd/bigints.h"
#include "framerd/fdregex.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/drivers.h"

#include <libu8/libu8.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include "leveldb/c.h"
#include "framerd/leveldb.h"

fd_ptr_type fd_leveldb_type;

static ssize_t default_writebuf_size=-1;
static ssize_t default_cache_size=-1;
static ssize_t default_block_size=-1;
static int default_maxfiles=-1;
static int default_restart_interval=-1;
static int default_compression=0;

#define SYM(x) (fd_intern(x))

/* Initialization */

FD_EXPORT int fd_init_leveldb(void) FD_LIBINIT_FN;

static long long int leveldb_initialized=0;

/* Getting various LevelDB option objects */

static leveldb_options_t *get_leveldb_options(fdtype opts)
{
  leveldb_options_t *ldbopts=leveldb_options_create();
  fdtype bufsize_spec=fd_getopt(opts,SYM("WRITEBUF"),FD_VOID);
  fdtype maxfiles_spec=fd_getopt(opts,SYM("MAXFILES"),FD_VOID);
  fdtype blocksize_spec=fd_getopt(opts,SYM("BLOCKSIZE"),FD_VOID);
  fdtype restart_spec=fd_getopt(opts,SYM("RESTART"),FD_VOID);
  fdtype compress_spec=fd_getopt(opts,SYM("COMPRESS"),FD_VOID);
  if (fd_testopt(opts,SYM("INIT"),FD_VOID)) {
    leveldb_options_set_create_if_missing(ldbopts,1);
    leveldb_options_set_error_if_exists(ldbopts,1);}
  else if (!(fd_testopt(opts,SYM("READ"),FD_VOID))) {
    leveldb_options_set_create_if_missing(ldbopts,1);}
  else {}
  if (fd_testopt(opts,SYM("PARANOID"),FD_VOID)) {
    leveldb_options_set_paranoid_checks(ldbopts,1);}
  if (FD_FIXNUMP(bufsize_spec))
    leveldb_options_set_write_buffer_size(ldbopts,FD_FIX2INT(bufsize_spec));
  else if (default_writebuf_size>0)
    leveldb_options_set_write_buffer_size(ldbopts,default_writebuf_size);
  else {}
  if (FD_FIXNUMP(maxfiles_spec))
    leveldb_options_set_max_open_files(ldbopts,FD_FIX2INT(maxfiles_spec));
  else if (default_maxfiles>0)
    leveldb_options_set_max_open_files(ldbopts,default_maxfiles);
  else {}
  if (FD_FIXNUMP(blocksize_spec))
    leveldb_options_set_block_size(ldbopts,FD_FIX2INT(blocksize_spec));
  else if (default_block_size>0)
    leveldb_options_set_block_size(ldbopts,default_block_size);
  else {}
  if (FD_FIXNUMP(restart_spec))
    leveldb_options_set_block_restart_interval(ldbopts,FD_FIX2INT(restart_spec));
  else if (default_block_size>0)
    leveldb_options_set_block_restart_interval(ldbopts,default_restart_interval);
  else {}
  if (FD_TRUEP(compress_spec))
    leveldb_options_set_compression(ldbopts,leveldb_snappy_compression);
  else if (default_compression)
    leveldb_options_set_compression(ldbopts,leveldb_snappy_compression);
  else leveldb_options_set_compression(ldbopts,leveldb_no_compression);
  return ldbopts;
}

static leveldb_cache_t *get_leveldb_cache(leveldb_options_t *ldbopts,
					  fdtype opts)
{
  leveldb_cache_t *cache=NULL;
  fdtype cache_size=fd_getopt(opts,SYM("CACHESIZE"),FD_VOID);
  if (FD_FIXNUMP(cache_size))
    cache=leveldb_cache_create_lru(FD_FIX2INT(cache_size));
  else if (default_cache_size>0)
      cache=leveldb_cache_create_lru(default_cache_size);
  else cache=NULL;
  if (cache) leveldb_options_set_cache(ldbopts,cache);
  return cache;
}

static leveldb_env_t *get_leveldb_env(leveldb_options_t *ldbopts,fdtype opts)
{
  if ((FD_VOIDP(opts))||(FD_FALSEP(opts)))
    return NULL;
  else {
    leveldb_env_t *env=leveldb_create_default_env(); int needed=0;
    /* Setup env from opts and other defaults */
    if (needed) {
      leveldb_options_set_env(ldbopts,env);
      return env;}
    else {
      leveldb_env_destroy(env);
      return NULL;}}
}

static leveldb_readoptions_t *get_read_options(framerd_leveldb db,fdtype opts_arg)
{
  if ( (FD_VOIDP(opts_arg)) ||
       (FD_FALSEP(opts_arg)) ||
       (FD_DEFAULTP(opts_arg)) )
    return NULL;
  else {
    int real=0, free_opts=0;
    fdtype opts=(FD_VOIDP(opts_arg)) ? (db->opts) :
      (FD_VOIDP(db->opts)) ? (opts_arg) :
      (free_opts=1,fd_make_pair(opts_arg,db->opts));
    leveldb_readoptions_t *readopts=leveldb_readoptions_create();
    /* Set up readopts based on opts */
    if (free_opts) fd_decref(opts);
    if (real)
      return readopts;
    else {
      leveldb_readoptions_destroy(readopts);
      return NULL;}}
}

static leveldb_writeoptions_t *get_write_options(framerd_leveldb db,fdtype opts_arg)
{
  if ((FD_VOIDP(opts_arg))||(FD_FALSEP(opts_arg))||(FD_DEFAULTP(opts_arg)))
    return NULL;
  else {
    int real=0, free_opts=0;
    fdtype opts=(FD_VOIDP(opts_arg)) ? (db->opts) :
      (FD_VOIDP(db->opts)) ? (opts_arg) :
      (free_opts=1,fd_make_pair(opts_arg,db->opts));
    leveldb_writeoptions_t *writeopts=leveldb_writeoptions_create();
    /* Set up writeopts based on opts */
    if (free_opts) fd_decref(opts);
    if (real)
      return writeopts;
    else {
      leveldb_writeoptions_destroy(writeopts);
      return NULL;}}
}

/* Initializing FRAMERD_LEVELDB structs */

FD_EXPORT
struct FRAMERD_LEVELDB *fd_setup_leveldb
   (struct FRAMERD_LEVELDB *db,u8_string path,fdtype opts)
{
  char *errmsg;
  u8_string usepath=db->path=u8_strdup(path);
  fdtype useopts=db->opts=fd_incref(opts);
  leveldb_options_t *options=get_leveldb_options(opts);
  leveldb_cache_t *cache=get_leveldb_cache(options,opts);
  leveldb_env_t *env=get_leveldb_env(options,opts);
  leveldb_readoptions_t *readopts=get_read_options(db,opts);
  leveldb_writeoptions_t *writeopts=get_write_options(db,opts);
  enum leveldb_status status=db->dbstatus;
  if (status==leveldb_raw) {
    u8_init_mutex(&(db->leveldb_lock));
    db->dbstatus=leveldb_sketchy;}
  u8_lock_mutex(&(db->leveldb_lock));
  db->dbstatus=leveldb_opening;
  if (readopts) db->readopts=readopts;
  else db->readopts=leveldb_readoptions_create();
  if (writeopts) db->writeopts=writeopts;
  else db->writeopts=leveldb_writeoptions_create();
  db->optionsptr=options;
  db->cacheptr=cache;
  db->envptr=env;
  if (db->dbptr=leveldb_open(options,path,&errmsg)) {
    db->dbstatus=leveldb_opened;
    u8_unlock_mutex(&(db->leveldb_lock));
    return db;}
  else {
    fd_seterr("OpenFailed","fd_open_leveldb",errmsg,opts);
    db->dbstatus=leveldb_error;
    u8_free(db->path); db->path=NULL;
    leveldb_options_destroy(options);
    if (readopts) leveldb_readoptions_destroy(readopts);
    if (writeopts) leveldb_writeoptions_destroy(writeopts);
    if (cache) leveldb_cache_destroy(cache);
    if (env) leveldb_env_destroy(env);
    if (status==leveldb_raw) {
      u8_unlock_mutex(&(db->leveldb_lock));
      u8_destroy_mutex(&(db->leveldb_lock));}
    else u8_unlock_mutex(&(db->leveldb_lock));
    return NULL;}
}

FD_EXPORT
int fd_close_leveldb(framerd_leveldb db)
{
  int closed=0;
  if ( (db->dbstatus == leveldb_opened) ||
       (db->dbstatus == leveldb_opening) ) {
    u8_lock_mutex(&(db->leveldb_lock));
    if ((db->dbstatus == leveldb_opened) ) {
      db->dbstatus=leveldb_closing;
      leveldb_close(db->dbptr);
      db->dbstatus=leveldb_closed;
      leveldb_free(db->dbptr);
      db->dbptr=NULL;
      closed=1;}
    u8_unlock_mutex(&(db->leveldb_lock));}
  return closed;
}

FD_EXPORT
framerd_leveldb fd_open_leveldb(framerd_leveldb db)
{
  char *errmsg;
  if ( (db->dbstatus==leveldb_opened) ||
       (db->dbstatus==leveldb_opening) )
    return db;
  else {
    char *errmsg;
    u8_lock_mutex(&(db->leveldb_lock));
    if ( (db->dbstatus==leveldb_opened) ||
	 (db->dbstatus==leveldb_opening) ) {
      u8_unlock_mutex(&(db->leveldb_lock));
      return db;}
    db->dbstatus=leveldb_opening;
    if (db->dbptr=leveldb_open(db->optionsptr,db->path,&errmsg)) {
      db->dbstatus=leveldb_opened;
      u8_unlock_mutex(&(db->leveldb_lock));
      return db;}
    else {
      u8_unlock_mutex(&(db->leveldb_lock));
      fd_seterr("OpenFailed","fd_open_leveldb",errmsg,FD_VOID);
      return NULL;}}
}

/* DType object methods */

static int unparse_leveldb(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_LEVELDB *db=(fd_leveldb)x;
  u8_printf(out,"#<LevelDB %s>",db->leveldb.path);
  return 1;
}
static void recycle_leveldb(struct FD_CONS *c)
{
  struct FD_LEVELDB *db=(fd_leveldb)c;
  u8_free(db->leveldb.path); db->leveldb.path=NULL;
  fd_decref(db->leveldb.opts);
  leveldb_close(db->leveldb.dbptr);
  leveldb_options_destroy(db->leveldb.optionsptr);
  u8_free(c);
}

/* Primitives */

static fdtype leveldb_open_prim(fdtype path,fdtype opts)
{
  struct FD_LEVELDB *db=u8_zalloc(struct FD_LEVELDB);
  fd_setup_leveldb(&(db->leveldb),FD_STRDATA(path),opts);
  if (db->leveldb.dbptr) {
    FD_INIT_CONS(db,fd_leveldb_type);
    return (fdtype) db;}
  else {
    u8_free(db);
    return FD_ERROR_VALUE;}
}

static fdtype leveldbp_prim(fdtype arg)
{
  if (FD_TYPEP(arg,fd_leveldb_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype leveldb_close_prim(fdtype leveldb)
{
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  leveldb_close(db->leveldb.dbptr);
  return FD_TRUE;
}

static fdtype leveldb_reopen_prim(fdtype leveldb)
{
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *reopened=fd_open_leveldb(&(db->leveldb));
  if (reopened) {
    fd_incref(leveldb);
    return leveldb;}
  else return FD_ERROR_VALUE;
}

/* Basic operations */

static fdtype leveldb_get_prim(fdtype leveldb,fdtype key,fdtype opts)
{
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *fdldb=&(db->leveldb);
  char *errmsg=NULL;
  leveldb_readoptions_t *readopts=get_read_options(fdldb,opts);
  if (FD_PACKETP(key)) {
    ssize_t binary_size;
    unsigned char *binary_data=
      leveldb_get(db->leveldb.dbptr,readopts,
		  FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
		  &binary_size,&errmsg);
    if (readopts!=fdldb->readopts)
      leveldb_readoptions_destroy(readopts);
    if (binary_data)
      return fd_bytes2packet(NULL,binary_size,binary_data);
    else if (errmsg)
      return fd_err("LevelDBError","leveldb_get_prim",errmsg,FD_VOID);
    else return FD_EMPTY_CHOICE;}
  else {
    struct FD_BYTE_OUTPUT keyout; FD_INIT_BYTE_OUTPUT(&keyout,1024);
    if (fd_write_dtype(&keyout,key)>0) {
      fdtype result=FD_VOID;
      ssize_t binary_size;
      unsigned char *binary_data=
	leveldb_get(db->leveldb.dbptr,readopts,
		    keyout.bs_bufstart,
		    keyout.bs_bufptr-keyout.bs_bufstart,
		    &binary_size,&errmsg);
      u8_free(keyout.bs_bufstart);
      if (readopts!=fdldb->readopts)
	leveldb_readoptions_destroy(readopts);
      if (binary_data==NULL) {
	if (errmsg)
	  result=fd_err("LevelDBError","leveldb_get_prim",errmsg,FD_VOID);
	else result=FD_EMPTY_CHOICE;}
      else {
	struct FD_BYTE_INPUT valuein;
	FD_INIT_BYTE_INPUT(&valuein,binary_data,binary_size);
	result=fd_read_dtype(&valuein);
	u8_free(binary_data);}
      return result;}
    else {
      if (readopts!=fdldb->readopts)
	leveldb_readoptions_destroy(readopts);
      return FD_ERROR_VALUE;}}
}

static fdtype leveldb_put_prim(fdtype leveldb,fdtype key,fdtype value,
			       fdtype opts)
{
  char *errmsg=NULL;
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *fdldb=&(db->leveldb);
  if ((FD_PACKETP(key))&&(FD_PACKETP(value))) {
    ssize_t binary_size;
    leveldb_writeoptions_t *useopts=get_write_options(fdldb,opts);
    leveldb_writeoptions_t *writeopts=(useopts)?(useopts):(fdldb->writeopts);
    leveldb_put(db->leveldb.dbptr,writeopts,
		FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
		FD_PACKET_DATA(value),FD_PACKET_LENGTH(value),
		&errmsg);
    if (useopts) leveldb_writeoptions_destroy(useopts);
    if (errmsg)
      return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
    else return FD_VOID;}
  else {
    struct FD_BYTE_OUTPUT keyout; FD_INIT_BYTE_OUTPUT(&keyout,1024);
    struct FD_BYTE_OUTPUT valout; FD_INIT_BYTE_OUTPUT(&valout,1024);
    if (fd_write_dtype(&keyout,key)<0) {
      u8_free(keyout.bs_bufstart);
      u8_free(valout.bs_bufstart);
      return FD_ERROR_VALUE;}
    else if (fd_write_dtype(&valout,value)<0) {
      u8_free(keyout.bs_bufstart);
      u8_free(valout.bs_bufstart);
      return FD_ERROR_VALUE;}
    else {
      leveldb_writeoptions_t *useopts=get_write_options(fdldb,opts);
      leveldb_writeoptions_t *writeopts=(useopts)?(useopts):(fdldb->writeopts);
      leveldb_put(db->leveldb.dbptr,writeopts,
		  keyout.bs_bufstart,keyout.bs_bufptr-keyout.bs_bufstart,
		  valout.bs_bufstart,valout.bs_bufptr-valout.bs_bufstart,
		  &errmsg);
      u8_free(keyout.bs_bufstart);
      u8_free(valout.bs_bufstart);
      if (useopts) leveldb_writeoptions_destroy(useopts);
      if (errmsg)
	return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
      else return FD_VOID;}}
}

static fdtype leveldb_drop_prim(fdtype leveldb,fdtype key,fdtype opts)
{
  char *errmsg=NULL;
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *fdldb=&(db->leveldb);
  leveldb_writeoptions_t *writeopts=get_write_options(fdldb,opts);
  if (FD_PACKETP(key)) {
    ssize_t binary_size;
    leveldb_writeoptions_t *useopts=get_write_options(fdldb,opts);
    leveldb_writeoptions_t *writeopts=(useopts)?(useopts):(fdldb->writeopts);
    leveldb_delete(db->leveldb.dbptr,writeopts,
		   FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
		   &errmsg);
    if (useopts) leveldb_writeoptions_destroy(useopts);
    if (errmsg)
      return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
    else return FD_VOID;}
  else {
    struct FD_BYTE_OUTPUT keyout; FD_INIT_BYTE_OUTPUT(&keyout,1024);
    if (fd_write_dtype(&keyout,key)<0) {
      u8_free(keyout.bs_bufstart);
      return FD_ERROR_VALUE;}
    else {
      leveldb_writeoptions_t *useopts=get_write_options(fdldb,opts);
      leveldb_writeoptions_t *writeopts=(useopts)?(useopts):(fdldb->writeopts);
      leveldb_delete(db->leveldb.dbptr,writeopts,
		     keyout.bs_bufstart,keyout.bs_bufptr-keyout.bs_bufstart,
		     &errmsg);
      u8_free(keyout.bs_bufstart);
      if (useopts) leveldb_writeoptions_destroy(useopts);
      if (errmsg)
	return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
      else return FD_VOID;}}
}

static leveldb_readoptions_t *default_readopts;
static leveldb_writeoptions_t *default_writeopts;
static leveldb_writeoptions_t *sync_writeopts;

static fdtype get_prop(leveldb_t *dbptr,char *key,fdtype dflt)
{
  ssize_t data_size; char *errmsg=NULL;
  unsigned char *buf=leveldb_get
    (dbptr,default_readopts,key,strlen(key),&data_size,&errmsg);
  if (buf) {
    fdtype result=FD_VOID;
    struct FD_BYTE_INPUT in;
    FD_INIT_BYTE_INPUT(&in,buf,data_size);
    result=fd_read_dtype(&in);
    u8_free(buf);
    return result;}
  else if (errmsg)
    return fd_err("LevelDBerror","get_prop",NULL,FD_VOID);
  else return dflt;
}

static ssize_t set_prop(leveldb_t *dbptr,char *key,fdtype value,
			leveldb_writeoptions_t *writeopts)
{
  ssize_t data_size; ssize_t dtype_len; char *errmsg=NULL;
  struct FD_BYTE_OUTPUT out;
  FD_INIT_BYTE_OUTPUT(&out,512);
  if ((dtype_len=fd_write_dtype(&out,value))>0) {
    leveldb_put(dbptr,writeopts,key,strlen(key),
		out.bs_bufstart,out.bs_bufptr-out.bs_bufstart,
		&errmsg);
    u8_free(out.bs_bufstart);
    if (errmsg)
      return fd_reterr("LevelDBerror","set_prop",errmsg,FD_VOID);
    else return dtype_len;}
  else return -1;
}

/* LevelDB pool backends */

static struct FD_POOL_HANDLER leveldb_pool_handler;

FD_EXPORT
fd_pool fd_use_leveldb_pool(u8_string path,fdtype opts)
{
  struct FD_LEVELDB_POOL *pool=u8_alloc(struct FD_LEVELDB_POOL);
  if (fd_setup_leveldb(&(pool->leveldb),path,opts)) {
    leveldb_t *dbptr=pool->leveldb.dbptr;
    fdtype base=get_prop(dbptr,"\377BASE",FD_VOID);
    fdtype cap=get_prop(dbptr,"\377CAPACITY",FD_VOID);
    fdtype load=get_prop(dbptr,"\377LOAD",FD_VOID);
    if ((FD_OIDP(base)) && (FD_FIXNUMP(cap)) && (FD_FIXNUMP(load))) {
      u8_string rname=u8_realpath(path,NULL);
      fdtype label=get_prop(dbptr,"\377LABEL",FD_VOID);
      fd_init_pool((fd_pool)pool,
		   FD_OID_ADDR(base),FD_FIX2INT(cap),
		   &leveldb_pool_handler,
		   u8_strdup(path),rname);
      u8_free(rname);
      if (fd_testopt(opts,SYM("READONLY"),FD_VOID))
	pool->pool_flags|=FDB_READ_ONLY;
      pool->pool_load=FD_FIX2INT(load);
      if (FD_STRINGP(label)) {
	pool->pool_label=u8_strdup(FD_STRDATA(label));}
      fd_register_pool((fd_pool)pool);
      return (fd_pool)pool;}
    else  {
      fd_close_leveldb(&(pool->leveldb));
      u8_free(pool);
      fd_seterr("NotAPoolDB","fd_leveldb_pool",NULL,FD_VOID);
      return (fd_pool)NULL;}}
  else return NULL;
}

FD_EXPORT
fd_pool fd_make_leveldb_pool(u8_string path,fdtype base,fdtype cap,fdtype opts)
{
  struct FD_LEVELDB_POOL *pool=u8_zalloc(struct FD_LEVELDB_POOL);
  fdtype load=fd_getopt(opts,SYM("LOAD"),FD_FIXZERO);
  fdtype label=fd_getopt(opts,SYM("LABEL"),FD_VOID);
  if ((!(FD_OIDP(base)))||(!(FD_FIXNUMP(cap)))) {
    u8_free(pool);
    fd_seterr("Not enough information to create a pool",
	      "fd_make_leveldb_pool",path,opts);
    return (fd_pool)NULL;}
  else if (fd_setup_leveldb(&(pool->leveldb),path,opts)) {
    leveldb_t *dbptr=pool->leveldb.dbptr;
    fdtype given_base=get_prop(dbptr,"\377BASE",FD_VOID);
    fdtype given_cap=get_prop(dbptr,"\377CAPACITY",FD_VOID);
    fdtype given_load=get_prop(dbptr,"\377LOAD",FD_VOID);
    fdtype cur_label=get_prop(dbptr,"\377LABEL",FD_VOID);
    u8_string rname=u8_realpath(path,NULL);
    if (!((FD_VOIDP(given_base))||(FD_EQUALP(base,given_base)))) {
      u8_free(pool); u8_free(rname);
      fd_seterr("Conflicting base OIDs",
		"fd_make_leveldb_pool",path,opts);
      return NULL;}
    if (!((FD_VOIDP(given_cap))||(FD_EQUALP(cap,given_cap)))) {
      u8_free(pool); u8_free(rname);
      fd_seterr("Conflicting pool capacities",
		"fd_make_leveldb_pool",path,opts);
      return NULL;}
    if (!(FD_VOIDP(label))) {
      if (!(FD_EQUALP(label,cur_label)))
	set_prop(dbptr,"\377LABEL",label,sync_writeopts);}
    set_prop(dbptr,"\377BASE",base,sync_writeopts);
    set_prop(dbptr,"\377CAPACITY",cap,sync_writeopts);
    if (FD_VOIDP(given_load))
      set_prop(dbptr,"\377LOAD",load,sync_writeopts);
    fd_init_pool((fd_pool)pool,
		 FD_OID_ADDR(base),FD_FIX2INT(cap),
		 &leveldb_pool_handler,
		 u8_strdup(path),rname);
    u8_free(rname);
    pool->pool_flags&=~FDB_READ_ONLY;
    pool->pool_load=FD_FIX2INT(load);
    if (FD_STRINGP(label)) {
      pool->pool_label=u8_strdup(FD_STRDATA(label));}
    fd_register_pool((fd_pool)pool);
    return (fd_pool)pool;}
  else  {
    fd_close_leveldb(&(pool->leveldb));
    u8_free(pool);
    fd_seterr("NotAPoolDB","fd_leveldb_pool",NULL,FD_VOID);
    return (fd_pool)NULL;}
}

static fdtype read_oid_value(fd_leveldb_pool p,struct FD_BYTE_INPUT *in)
{
  return fd_read_dtype(in);
}

static int write_oid_value(fd_leveldb_pool p,
			   struct FD_BYTE_OUTPUT *out,
			   fdtype value)
{
  return fd_write_dtype(out,value);
}

static fdtype get_oid_value(fd_leveldb_pool ldp,unsigned int offset)
{
  ssize_t data_size; char *errmsg=NULL;
  leveldb_t *dbptr=ldp->leveldb.dbptr;
  leveldb_readoptions_t *readopts=ldp->leveldb.readopts;
  unsigned char keybuf[5];
  keybuf[0]=0xFE;
  keybuf[1]=((offset>>24)&0XFF);
  keybuf[2]=((offset>>16)&0XFF);
  keybuf[3]=((offset>>8)&0XFF);
  keybuf[4]=(offset&0XFF);
  unsigned char *buf=leveldb_get
    (dbptr,readopts,keybuf,5,&data_size,&errmsg);
  if (buf) {
    fdtype result=FD_VOID;
    struct FD_BYTE_INPUT in;
    FD_INIT_BYTE_INPUT(&in,buf,data_size);
    result=read_oid_value(ldp,&in);
    u8_free(buf);
    return result;}
  else if (errmsg)
    return fd_err("LevelDBerror","get_prop",NULL,FD_VOID);
  else return FD_VOID;
}

static int set_oid_value(fd_leveldb_pool ldp,
			 unsigned int offset,
			 fdtype value,
			 leveldb_writeoptions_t *writeopts)
{
  struct FD_BYTE_OUTPUT out; ssize_t dtype_len;
  leveldb_t *dbptr=ldp->leveldb.dbptr;
  if (writeopts==NULL) writeopts=ldp->leveldb.writeopts;
  FD_INIT_BYTE_OUTPUT(&out,512);
  unsigned char buf[5];
  buf[0]=0xFE;
  buf[1]=((offset>>24)&0XFF);
  buf[2]=((offset>>16)&0XFF);
  buf[3]=((offset>>8)&0XFF);
  buf[4]=(offset&0XFF);
  if ((dtype_len=write_oid_value(ldp,&out,value))>0) {
    char *errmsg=NULL;
    leveldb_put
      (dbptr,writeopts,buf,5,
       out.bs_bufstart,out.bs_bufptr-out.bs_bufstart,
       &errmsg);
    u8_free(out.bs_bufstart);
    if (errmsg==NULL)
      return dtype_len;
    fd_seterr("LevelDB pool save error","set_oidvalue",errmsg,FD_VOID);
    return -1;}
  else return -1;
}

static int queue_oid_value(fd_leveldb_pool ldp,
			   unsigned int offset,
			   fdtype value,
			   leveldb_writebatch_t *batch)
{
  struct FD_BYTE_OUTPUT out; ssize_t dtype_len;
  leveldb_t *dbptr=ldp->leveldb.dbptr;
  unsigned char buf[5];
  buf[0]=0xFE;
  buf[1]=((offset>>24)&0XFF);
  buf[2]=((offset>>16)&0XFF);
  buf[3]=((offset>>8)&0XFF);
  buf[4]=(offset&0XFF);
  FD_INIT_BYTE_OUTPUT(&out,512);
  if ((dtype_len=write_oid_value(ldp,&out,value))>0) {
    leveldb_writebatch_put
      (batch,buf,5,out.bs_bufstart,out.bs_bufptr-out.bs_bufstart);
    u8_free(out.bs_bufstart);
    return dtype_len;}
  else return -1;
}

static fdtype leveldb_pool_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; unsigned int i=0, start;
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  u8_lock_mutex(&(pool->pool_lock));
  if (pool->pool_load+n>=pool->pool_capacity) {
    u8_unlock_mutex(&(pool->pool_lock));
    return fd_err(fd_ExhaustedPool,"leveldb_pool_alloc",pool->pool_cid,FD_VOID);}
  start=pool->pool_load;
  pool->pool_load=start+n;
  u8_unlock_mutex(&(pool->pool_lock));
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(pool->pool_base,start+i);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    i++;}
  return results;
}

static fdtype leveldb_pool_fetchoid(fd_pool p,fdtype oid)
{
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  FD_OID addr=FD_OID_ADDR(oid);
  unsigned int offset=FD_OID_DIFFERENCE(addr,pool->pool_base);
  return get_oid_value(pool,offset);
}

struct OFFSET_ENTRY {
  unsigned int oid_offset;
  unsigned int fetch_offset;};

static int off_compare(const void *x,const void *y)
{
  const struct OFFSET_ENTRY *ex=x, *ey=y;
  if (ex->oid_offset<ey->oid_offset) return -1;
  else if (ex->oid_offset>ey->oid_offset) return 1;
  else return 0;
}

static fdtype *leveldb_pool_fetchn(fd_pool p,int n,fdtype *oids)
{
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  struct OFFSET_ENTRY *entries=u8_zalloc_n(n,struct OFFSET_ENTRY);
  leveldb_readoptions_t *readopts=pool->leveldb.readopts;
  unsigned int largest_offset=0, offsets_sorted=1;
  fdtype *values=u8_zalloc_n(n,fdtype);
  FD_OID base=p->pool_base;
  int i=0; while (i<n) {
    FD_OID addr=FD_OID_ADDR(oids[i]);
    unsigned int offset=FD_OID_DIFFERENCE(addr,base);
    entries[i].oid_offset=FD_OID_DIFFERENCE(addr,base);
    entries[i].fetch_offset=i;
    if (offsets_sorted) {
      if (offset>=largest_offset) largest_offset=offset;
      else offsets_sorted=0;}
    i++;}
  if (!(offsets_sorted)) {
    qsort(entries,n,sizeof(struct OFFSET_ENTRY),off_compare);}
  leveldb_iterator_t *iterator=
    leveldb_create_iterator(pool->leveldb.dbptr,readopts);
  i=0; while (i<n) {
    unsigned char keybuf[5];
    unsigned int offset=entries[i].oid_offset;
    unsigned int fetch_offset=entries[i].fetch_offset;
    keybuf[0]=0xFE;
    keybuf[1]=((offset>>24)&0XFF);
    keybuf[2]=((offset>>16)&0XFF);
    keybuf[3]=((offset>>8)&0XFF);
    keybuf[4]=(offset&0XFF);
    leveldb_iter_seek(iterator,keybuf,5);
    ssize_t bytes_len;
    const unsigned char *bytes=leveldb_iter_value(iterator,&bytes_len);
    if (bytes) {
      struct FD_BYTE_INPUT in;
      FD_INIT_BYTE_INPUT(&in,bytes,bytes_len);
      fdtype oidvalue=read_oid_value(pool,&in);
      values[fetch_offset]=oidvalue;}
    else values[fetch_offset]=FD_VOID;
    i++;}
  leveldb_iter_destroy(iterator);
  u8_free(entries);
  return values;
}

static int leveldb_pool_getload(fd_pool p)
{
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  return pool->pool_load;
}

static int leveldb_pool_lock(fd_pool p,fdtype oids)
{
  /* What should this really do? */
  return 1;
}

static int leveldb_pool_unlock(fd_pool p,fdtype oids)
{
  /* What should this really do? Flush edits to disk? storen? */
  return 1;
}

static void leveldb_pool_close(fd_pool p)
{
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  fd_close_leveldb(&(pool->leveldb));
}


static int leveldb_pool_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  leveldb_t *dbptr=pool->leveldb.dbptr;
  int i=0, errval=0;
  if (n>7) {
    char *errmsg=NULL;
    leveldb_writebatch_t *batch=leveldb_writebatch_create();
    ssize_t n_bytes=0;
    while (i<n) {
      fdtype oid=oids[i], value=values[i];
      FD_OID addr=FD_OID_ADDR(oid);
      unsigned int offset=FD_OID_DIFFERENCE(addr,pool->pool_base);
      ssize_t len=queue_oid_value(pool,offset,value,batch);
      if (len<0) {errval=len; break;}
      else {n_bytes+=len; i++;}}
    if (errval>=0) {
      leveldb_write(dbptr,sync_writeopts,batch,&errmsg);
      if (errmsg) {
	u8_seterr("LevelDBError","leveldb_pool_storen",errmsg);
	errval=-1;}}
    leveldb_writebatch_destroy(batch);}
  else while (i<n) {
    fdtype oid=oids[i], value=values[i];
    FD_OID addr=FD_OID_ADDR(oid);
    unsigned int offset=FD_OID_DIFFERENCE(addr,pool->pool_base);
    if ((errval=set_oid_value(pool,offset,value,sync_writeopts))<0)
      break;
    else i++;}
  if (errval>=0) {
    u8_lock_mutex(&(pool->pool_lock)); {
      fdtype loadval=FD_INT(pool->pool_load);
      int retval=set_prop(dbptr,"\377LOAD",loadval,sync_writeopts);
      fd_decref(loadval);
      u8_unlock_mutex(&(pool->pool_lock));
      if (retval<0) return retval;}}
  if (errval<0)
    return errval;
  else return n;
}

/* The LevelDB pool handler */

static struct FD_POOL_HANDLER leveldb_pool_handler={
  "leveldb_pool", 1, sizeof(struct FD_LEVELDB_POOL), 12,
  leveldb_pool_close, /* close */
  NULL, /* setcache */
  NULL, /* setbuf */
  leveldb_pool_alloc, /* alloc */
  leveldb_pool_fetchoid, /* fetch */
  leveldb_pool_fetchn, /* fetchn */
  leveldb_pool_getload, /* getload */
  leveldb_pool_lock, /* lock */
  leveldb_pool_unlock, /* release */
  leveldb_pool_storen, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  NULL}; /* sync */

/* Scheme primitives */

static fdtype use_leveldb_pool_prim(fdtype path,fdtype opts)
{
  fd_pool pool=fd_use_leveldb_pool(FD_STRDATA(path),opts);
  return fd_pool2lisp(pool);
}

static fdtype make_leveldb_pool_prim(fdtype path,fdtype base,fdtype cap,
				     fdtype opts)
{
  fd_pool pool=fd_make_leveldb_pool(FD_STRDATA(path),base,cap,opts);
  return fd_pool2lisp(pool);
}

/* Initialization */

FD_EXPORT int fd_init_leveldb()
{
  fdtype module;
  if (leveldb_initialized) return 0;
  leveldb_initialized=u8_millitime();

  default_readopts=leveldb_readoptions_create();
  default_writeopts=leveldb_writeoptions_create();
  sync_writeopts=leveldb_writeoptions_create();
  leveldb_writeoptions_set_sync(sync_writeopts,1);

  fd_leveldb_type=fd_register_cons_type("leveldb");
  fd_unparsers[fd_leveldb_type]=unparse_leveldb;
  fd_recyclers[fd_leveldb_type]=recycle_leveldb;
  fd_type_names[fd_leveldb_type]="LevelDB";

  module=fd_new_module("LEVELDB",0);

  fd_idefn(module,fd_make_cprim2x("LEVELDB/OPEN",leveldb_open_prim,1,
				  fd_string_type,FD_VOID,
				  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("LEVELDB?",leveldbp_prim,1,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("LEVELDB/CLOSE",leveldb_close_prim,1,
				  fd_leveldb_type,FD_VOID));
  fd_idefn(module,fd_make_cprim1x("LEVELDB/REOPEN",leveldb_reopen_prim,1,
				  fd_leveldb_type,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("LEVELDB/GET",leveldb_get_prim,2,
				  fd_leveldb_type,FD_VOID,
				  -1,FD_VOID,-1,FD_VOID));
  fd_idefn(module,fd_make_cprim4x("LEVELDB/PUT!",leveldb_put_prim,3,
				  fd_leveldb_type,FD_VOID,
				  -1,FD_VOID,-1,FD_VOID,
				  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("LEVELDB/DROP!",leveldb_drop_prim,2,
				  fd_leveldb_type,FD_VOID,
				  -1,FD_VOID,-1,FD_VOID));


  fd_idefn(module,
	   fd_make_cprim2x("LEVELDB/USE-POOL",
			   use_leveldb_pool_prim,1,
			   fd_string_type,FD_VOID,-1,FD_VOID));
  fd_idefn(module,
	   fd_make_cprim4x("LEVELDB/MAKE-POOL",
			   make_leveldb_pool_prim,3,
			   fd_string_type,FD_VOID,
			   fd_oid_type,FD_VOID,
			   fd_fixnum_type,FD_VOID,
			   -1,FD_VOID));


  fd_register_config("LEVELDB:WRITEBUF",
                     "Default writebuf size for leveldb",
                     fd_sizeconfig_get,fd_sizeconfig_set,
		     &default_writebuf_size);
  fd_register_config("LEVELDB:BLOCKSIZE",
                     "Default block size for leveldb",
                     fd_sizeconfig_get,fd_sizeconfig_set,
		     &default_block_size);
  fd_register_config("LEVELDB:CACHESIZE",
                     "Default block size for leveldb",
                     fd_sizeconfig_get,fd_sizeconfig_set,
		     &default_cache_size);
  fd_register_config("LEVELDB:MAXFILES",
                     "The maximnum number of file descriptions LevelDB may open",
                     fd_intconfig_get,fd_intconfig_set,
		     &default_maxfiles);
  fd_register_config("LEVELDB:COMPRESS",
                     "Whether to compress data stored in leveldb",
                     fd_boolconfig_get,fd_boolconfig_set,
		     &default_compression);
  fd_register_config("LEVELDB:RESTART",
                     "The restart interval for leveldb",
                     fd_intconfig_get,fd_intconfig_set,
		     &default_restart_interval);

  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}
