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
#include "framerd/dbdriver.h"
#include "framerd/indices.h"
#include "framerd/pools.h"

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

static leveldb_options_t *get_leveldb_options(fdtype opts);
static leveldb_cache_t *get_leveldb_cache(leveldb_options_t *,fdtype opts);
static leveldb_env_t *get_leveldb_env(leveldb_options_t *,fdtype opts);

static long long int leveldb_initialized=0;

FD_EXPORT
struct FRAMERD_LEVELDB *fd_use_leveldb
   (struct FRAMERD_LEVELDB *db,u8_string path,fdtype opts)
{
  char *errmsg;
  leveldb_options_t *options=get_leveldb_options(opts);
  leveldb_cache_t *cache=get_leveldb_cache(options,opts);
  leveldb_env_t *env=get_leveldb_env(options,opts);
  leveldb_t *dbptr=leveldb_open(options,FD_STRDATA(path),&errmsg);
  if (dbptr) {
    db->path=u8_strdup(path);
    db->opts=opts; fd_incref(opts);
    db->optionsptr=options;
    db->cacheptr=cache;
    db->envptr=env;
    db->dbptr=dbptr;
    return db;}
  else {
    fd_seterr("OpenFailed","fd_open_leveldb",errmsg,opts);
    leveldb_options_destroy(options);
    return NULL;}
}

static int fd_close_leveldb(struct FRAMERD_LEVELDB *db)
{
  leveldb_close(db->dbptr);
  db->closed=1;
  leveldb_free(db->dbptr);
  db->dbptr=NULL;
  return 1;
}

FD_EXPORT
struct FRAMERD_LEVELDB *fd_reopen_leveldb(struct FRAMERD_LEVELDB *db)
{
  char *errmsg;
  if (db->closed) {
    char *errmsg;
    leveldb_t *fresh=leveldb_open(db->optionsptr,
				  db->path,
				  &errmsg);
    if (fresh) {
      db->dbptr=fresh;
      db->closed=0;}
    else {
      fd_seterr("OpenFailed","leveldb_reopen_prim",errmsg,FD_VOID);
      return NULL;}}
  else return db;
}

static fdtype leveldb_open_prim(fdtype path,fdtype opts)
{
  struct FD_LEVELDB *db=u8_zalloc(struct FD_LEVELDB);
  fd_use_leveldb(&(db->leveldb),FD_STRDATA(path),opts);
  if (db->leveldb.dbptr) {
    FD_INIT_CONS(db,fd_leveldb_type);
    return (fdtype) db;}
  else {
    u8_free(db);
    return FD_ERROR_VALUE;}
}
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
  db->leveldb.closed=1;
  leveldb_free(db->leveldb.dbptr);
  db->leveldb.dbptr=NULL;
  return FD_TRUE;
}

static fdtype leveldb_reopen_prim(fdtype leveldb)
{
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *reopened=fd_reopen_leveldb(&(db->leveldb));
  if (reopened) {
    fd_incref(leveldb);
    return leveldb;}
  else return FD_ERROR_VALUE;
}

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

static leveldb_cache_t *get_leveldb_cache(leveldb_options_t *ldbopts,fdtype opts)
{
  fdtype cache_size=fd_getopt(opts,SYM("CACHESIZE"),FD_VOID);
  leveldb_cache_t *cache=NULL;
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
  leveldb_env_t *env=leveldb_create_default_env();
  leveldb_options_set_env(ldbopts,env);
  return env;
}

static leveldb_readoptions_t *get_read_options(struct FD_LEVELDB *db,
					       fdtype opts_arg)
{
  fdtype opts;
  int free_opts=0;
  if (FD_VOIDP(opts_arg))
    opts=db->leveldb.opts;
  else if (FD_VOIDP(db->leveldb.opts))
    opts=opts_arg;
  else {
    opts=fd_make_pair(opts_arg,db->leveldb.opts);
    free_opts=1;}
  leveldb_readoptions_t *readopts=leveldb_readoptions_create();
  if (free_opts) fd_decref(opts);
  return readopts;
}

static leveldb_writeoptions_t *get_write_options(struct FD_LEVELDB *db,
						 fdtype opts_arg)
{
  fdtype opts;
  int free_opts=0;
  if (FD_VOIDP(opts_arg))
    opts=db->leveldb.opts;
  else if (FD_VOIDP(db->leveldb.opts))
    opts=opts_arg;
  else {
    opts=fd_make_pair(opts_arg,db->leveldb.opts);
    free_opts=1;}
  leveldb_writeoptions_t *writeopts=leveldb_writeoptions_create();
  if (free_opts) fd_decref(opts);
  return writeopts;
}

/* Basic operations */

static fdtype leveldb_get_prim(fdtype leveldb,fdtype key,fdtype opts)
{
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  char *errmsg=NULL;
  leveldb_readoptions_t *readopts=get_read_options(db,opts);
  if (FD_PACKETP(key)) {
    ssize_t binary_size;
    unsigned char *binary_data=
      leveldb_get(db->leveldb.dbptr,readopts,
		  FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
		  &binary_size,&errmsg);
    if (readopts) leveldb_readoptions_destroy(readopts);
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
      if (binary_data==NULL) {
	if (readopts) leveldb_readoptions_destroy(readopts);
	if (errmsg)
	  result=fd_err("LevelDBError","leveldb_get_prim",errmsg,FD_VOID);
	else result=FD_EMPTY_CHOICE;}
      else {
	struct FD_BYTE_INPUT valuein;
	if (readopts) leveldb_readoptions_destroy(readopts);
	FD_INIT_BYTE_INPUT(&valuein,binary_data,binary_size);
	result=fd_read_dtype(&valuein);
	u8_free(binary_data);}
      return result;}
    else {
      if (readopts) leveldb_readoptions_destroy(readopts);
      return FD_ERROR_VALUE;}}
}

static fdtype leveldb_put_prim(fdtype leveldb,fdtype key,fdtype value,
			       fdtype opts)
{
  char *errmsg=NULL;
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  leveldb_writeoptions_t *writeopts=get_write_options(db,opts);
  if ((FD_PACKETP(key))&&(FD_PACKETP(value))) {
    ssize_t binary_size;
    leveldb_put(db->leveldb.dbptr,writeopts,
		FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
		FD_PACKET_DATA(value),FD_PACKET_LENGTH(value),
		&errmsg);
    if (writeopts) leveldb_writeoptions_destroy(writeopts);
    if (errmsg)
      return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
    else return FD_VOID;}
  else {
    struct FD_BYTE_OUTPUT keyout; FD_INIT_BYTE_OUTPUT(&keyout,1024);
    struct FD_BYTE_OUTPUT valout; FD_INIT_BYTE_OUTPUT(&valout,1024);
    if (fd_write_dtype(&keyout,key)<0) {
      if (writeopts) leveldb_writeoptions_destroy(writeopts);
      u8_free(keyout.bs_bufstart);
      u8_free(valout.bs_bufstart);
      return FD_ERROR_VALUE;}
    else if (fd_write_dtype(&valout,value)<0) {
      if (writeopts) leveldb_writeoptions_destroy(writeopts);
      u8_free(keyout.bs_bufstart);
      u8_free(valout.bs_bufstart);
      return FD_ERROR_VALUE;}
    else {
      leveldb_put(db->leveldb.dbptr,writeopts,
		  keyout.bs_bufstart,keyout.bs_bufptr-keyout.bs_bufstart,
		  valout.bs_bufstart,valout.bs_bufptr-valout.bs_bufstart,
		  &errmsg);
      u8_free(keyout.bs_bufstart);
      u8_free(valout.bs_bufstart);
      if (writeopts) leveldb_writeoptions_destroy(writeopts);
      if (errmsg)
	return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
      else return FD_VOID;}}
}

static fdtype leveldb_drop_prim(fdtype leveldb,fdtype key,fdtype opts)
{
  char *errmsg=NULL;
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  leveldb_writeoptions_t *writeopts=get_write_options(db,opts);
  if (FD_PACKETP(key)) {
    ssize_t binary_size;
    leveldb_delete(db->leveldb.dbptr,writeopts,
		   FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
		   &errmsg);
    if (writeopts) leveldb_writeoptions_destroy(writeopts);
    if (errmsg)
      return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
    else return FD_VOID;}
  else {
    struct FD_BYTE_OUTPUT keyout; FD_INIT_BYTE_OUTPUT(&keyout,1024);
    if (fd_write_dtype(&keyout,key)<0) {
      if (writeopts) leveldb_writeoptions_destroy(writeopts);
      u8_free(keyout.bs_bufstart);
      return FD_ERROR_VALUE;}
    leveldb_delete(db->leveldb.dbptr,writeopts,
		   keyout.bs_bufstart,keyout.bs_bufptr-keyout.bs_bufstart,
		   &errmsg);
    u8_free(keyout.bs_bufstart);
    if (writeopts) leveldb_writeoptions_destroy(writeopts);
    if (errmsg)
      return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
    else return FD_VOID;}
}

static leveldb_readoptions_t *default_readopts;
static leveldb_writeoptions_t *default_writeopts;

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

static FD_OID get_oidprop(leveldb_t *dbptr,char *key)
{
  FD_OID oid;
  ssize_t data_size; char *errmsg=NULL;
  unsigned char *buf=leveldb_get
    (dbptr,default_readopts,key,strlen(key),&data_size,&errmsg);
  if (buf) {
    struct FD_BYTE_INPUT in;
    FD_INIT_BYTE_INPUT(&in,buf,data_size);
    FD_SET_OID_HI(oid,fd_read_4bytes(&in));
    FD_SET_OID_LO(oid,fd_read_4bytes(&in));
    u8_free(buf);
    return oid;}
  else {
    FD_SET_OID_HI(oid,0); FD_SET_OID_LO(oid,0);
    return oid;}
}

static ssize_t set_prop(leveldb_t *dbptr,char *key,fdtype value)
{
  ssize_t data_size; ssize_t dtype_len; char *errmsg=NULL;
  struct FD_BYTE_OUTPUT out;
  FD_INIT_BYTE_OUTPUT(&out,512);
  if ((dtype_len=fd_write_dtype(&out,value))>0) {
    leveldb_put(dbptr,default_writeopts,key,strlen(key),
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
  if (fd_use_leveldb(&(pool->leveldb),path,opts)) {
    leveldb_t *dbptr=pool->leveldb.dbptr;
    fdtype base=get_prop(dbptr,"\0177BASE",FD_VOID);
    fdtype cap=get_prop(dbptr,"\0177CAPACITY",FD_VOID);
    fdtype load=get_prop(dbptr,"\0177LOAD",FD_VOID);
    if ((FD_OIDP(base)) && (FD_FIXNUMP(cap)) && (FD_FIXNUMP(load))) {
      u8_string rname=u8_realpath(path,NULL);
      fdtype label=get_prop(dbptr,"\0177LABEL",FD_VOID);
      fd_init_pool((fd_pool)pool,base,FD_FIX2INT(cap),
		   &leveldb_pool_handler,
		   u8_strdup(path),rname);
      u8_free(rname);
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
fd_pool fd_make_leveldb_pool(u8_string path,fdtype opts)
{
  struct FD_LEVELDB_POOL *pool=u8_zalloc(struct FD_LEVELDB_POOL);
  fdtype base=fd_getopt(opts,SYM("BASE"),FD_VOID);
  fdtype cap=fd_getopt(opts,SYM("CAPACITY"),FD_VOID);
  fdtype load=fd_getopt(opts,SYM("LOAD"),FD_FIXZERO);
  fdtype label=fd_getopt(opts,SYM("LABEL"),FD_VOID);
  if ((!(FD_OIDP(base)))||(!(FD_FIXNUMP(cap)))) {
    u8_free(pool);
    fd_seterr("Not enough information to create a pool",
	      "fd_make_leveldb_pool",path,opts);
    return (fd_pool)NULL;}
  else if (fd_use_leveldb(&(pool->leveldb),path,opts)) {
    leveldb_t *dbptr=pool->leveldb.dbptr;
    fdtype given_base=get_prop(dbptr,"\0177BASE",FD_VOID);
    fdtype given_cap=get_prop(dbptr,"\0177CAPACITY",FD_VOID);
    fdtype given_load=get_prop(dbptr,"\0177LOAD",FD_VOID);
    fdtype cur_label=get_prop(dbptr,"\0177LABEL",FD_VOID);
    if (!((FD_VOIDP(given_base))||(FD_EQUALP(base,given_base)))) {
      u8_free(pool);
      fd_seterr("Conflicting base OIDs",
		"fd_make_leveldb_pool",path,opts);
      return NULL;}
    if (!((FD_VOIDP(given_cap))||(FD_EQUALP(cap,given_cap)))) {
      u8_free(pool);
      fd_seterr("Conflicting pool capacities",
		"fd_make_leveldb_pool",path,opts);
      return NULL;}
    u8_string rname=u8_realpath(path,NULL);
    if (!(FD_VOIDP(label))) {
      if (!(FD_EQUALP(label,cur_label)))
	set_prop(dbptr,"\0177LABEL",label);}
    set_prop(dbptr,"\0177BASE",base);
    set_prop(dbptr,"\0177CAPACITY",cap);
    if (FD_VOIDP(given_load))
      set_prop(dbptr,"\0177LOAD",load);
    fd_init_pool((fd_pool)pool,base,FD_FIX2INT(cap),
		 &leveldb_pool_handler,
		 u8_strdup(path),rname);
    u8_free(rname);
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

static fdtype leveldb_alloc(fd_pool p,int n)
{
  fdtype results=FD_EMPTY_CHOICE; int i=0;
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  if (pool->pool_load+n>=pool->pool_capacity) {
    return fd_err(fd_ExhaustedPool,"leveldb_alloc",pool->pool_cid,FD_VOID);}
  while (i < n) {
    FD_OID new_addr=FD_OID_PLUS(pool->pool_base,pool->pool_load);
    fdtype new_oid=fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    pool->pool_load++; i++; pool->pool_n_locked++;}
  return results;
}

static fdtype read_oid_value(struct FD_BYTE_INPUT *in)
{
  return fd_read_dtype(in);
}

static fdtype get_oidvalue(leveldb_t *dbptr,unsigned int offset)
{
  ssize_t data_size; char *errmsg=NULL;
  unsigned char keybuf[8];
  keybuf[0]=0xFE;
  keybuf[1]=((offset>>24)&0XFF);
  keybuf[2]=((offset>>16)&0XFF);
  keybuf[3]=((offset>>8)&0XFF);
  keybuf[3]=(offset&0XFF);
  unsigned char *buf=leveldb_get
    (dbptr,default_readopts,keybuf,5,&data_size,&errmsg);
  if (buf) {
    fdtype result=FD_VOID;
    struct FD_BYTE_INPUT in;
    FD_INIT_BYTE_INPUT(&in,buf,data_size);
    result=read_oid_value(&in);
    u8_free(buf);
    return result;}
  else if (errmsg)
    return fd_err("LevelDBerror","get_prop",NULL,FD_VOID);
  else return FD_VOID;
}

static int set_oidvalue(leveldb_t *dbptr,unsigned int offset,fdtype value)
{
  struct FD_BYTE_OUTPUT out; ssize_t dtype_len;
  FD_INIT_BYTE_OUTPUT(&out,512);
  unsigned char buf[8];
  buf[0]=0xFE;
  buf[1]=((offset>>24)&0XFF);
  buf[2]=((offset>>16)&0XFF);
  buf[3]=((offset>>8)&0XFF);
  buf[3]=(offset&0XFF);
  if ((dtype_len=fd_write_dtype(&out,value))>0) {
    char *errmsg=NULL;
    leveldb_put
      (dbptr,default_writeopts,buf,5,
       out.bs_bufstart,out.bs_bufptr-out.bs_bufstart,
       &errmsg);
    u8_free(out.bs_bufstart);
    if (errmsg==NULL)
      return dtype_len;
    fd_seterr("LevelDB pool save error","set_oidvalue",errmsg,FD_VOID);
    return -1;}
  else return -1;
}

static fdtype leveldb_fetchoid(fd_pool p,fdtype oid)
{
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  leveldb_t *dbptr=pool->leveldb.dbptr;
  FD_OID addr=FD_OID_ADDR(oid);
  unsigned int offset=FD_OID_DIFFERENCE(addr,pool->pool_base);
  return get_oidvalue(dbptr,offset);
}

static int leveldb_getload(fd_pool p)
{
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  return pool->pool_load;
}

static void leveldb_close_pool(fd_pool p)
{
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  fd_close_leveldb(&(pool->leveldb));
}


static int leveldb_storen(fd_pool p,int n,fdtype *oids,fdtype *values)
{
  struct FD_LEVELDB_POOL *pool=(struct FD_LEVELDB_POOL *)p;
  leveldb_t *dbptr=pool->leveldb.dbptr;
  fdtype loadval=FD_INT(pool->pool_load);
  if (set_prop(dbptr,"\177LOAD",loadval)<0) {
    fd_decref(loadval); return -1;}
  else {
    int i=0; while (i<n) {
      fdtype oid=oids[i], value=values[i]; i++;
      FD_OID addr=FD_OID_ADDR(oid);
      unsigned int offset=FD_OID_DIFFERENCE(addr,pool->pool_base);
      set_oidvalue(dbptr,offset,value);}
    fd_decref(loadval);
    return n;}
}

static fdtype use_leveldb_pool_prim(fdtype path,fdtype opts)
{
  fd_pool pool=fd_use_leveldb_pool(FD_STRDATA(path),opts);
  return fd_pool2lisp(pool);
}

static fdtype make_leveldb_pool_prim(fdtype path,fdtype opts)
{
  fd_pool pool=fd_make_leveldb_pool(FD_STRDATA(path),opts);
  return fd_pool2lisp(pool);
}


static struct FD_POOL_HANDLER leveldb_pool_handler={
  "leveldb_pool", 1, sizeof(struct FD_LEVELDB_POOL), 12,
  leveldb_close_pool, /* close */
  NULL, /* setcache */
  NULL, /* setbuf */
  leveldb_alloc, /* alloc */
  leveldb_fetchoid, /* fetch */
  NULL, /* fetchn */
  leveldb_getload, /* getload */
  NULL, /* lock */
  NULL, /* release */
  leveldb_storen, /* storen */
  NULL, /* swapout */
  NULL, /* metadata */
  NULL}; /* sync */

/* Initialization */

FD_EXPORT int fd_init_leveldb()
{
  fdtype module;
  if (leveldb_initialized) return 0;
  leveldb_initialized=u8_millitime();

  default_readopts=leveldb_readoptions_create();
  default_writeopts=leveldb_writeoptions_create();

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
	   fd_make_cprim2x("LEVELDB/MAKE-POOL",
			   make_leveldb_pool_prim,1,
			   fd_string_type,FD_VOID,-1,FD_VOID));


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
