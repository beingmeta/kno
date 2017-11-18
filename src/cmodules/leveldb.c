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
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include <libu8/libu8.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include "leveldb/c.h"
#include "framerd/leveldb.h"

fd_ptr_type fd_leveldb_type;

static ssize_t default_writebuf_size = -1;
static ssize_t default_cache_size = -1;
static ssize_t default_block_size = -1;
static int default_maxfiles = 256;
static int default_restart_interval = -1;
static int default_compression = 0;

#define SYM(x) (fd_intern(x))

/* Initialization */

FD_EXPORT int fd_init_leveldb(void) FD_LIBINIT_FN;

static long long int leveldb_initialized = 0;

/* Getting various LevelDB option objects */

static leveldb_options_t *get_leveldb_options(lispval opts)
{
  leveldb_options_t *ldbopts = leveldb_options_create();
  lispval bufsize_spec = fd_getopt(opts,SYM("WRITEBUF"),FD_VOID);
  lispval maxfiles_spec = fd_getopt(opts,SYM("MAXFILES"),FD_VOID);
  lispval blocksize_spec = fd_getopt(opts,SYM("BLOCKSIZE"),FD_VOID);
  lispval restart_spec = fd_getopt(opts,SYM("RESTART"),FD_VOID);
  lispval compress_spec = fd_getopt(opts,SYM("COMPRESS"),FD_VOID);
  if (fd_testopt(opts,SYM("INIT"),FD_VOID)) {
    leveldb_options_set_create_if_missing(ldbopts,1);
    leveldb_options_set_error_if_exists(ldbopts,1);}
  else if (!(fd_testopt(opts,SYM("READ"),FD_VOID))) {
    leveldb_options_set_create_if_missing(ldbopts,1);}
  else {}
  if (fd_testopt(opts,SYM("PARANOID"),FD_VOID)) {
    leveldb_options_set_paranoid_checks(ldbopts,1);}
  if (FD_UINTP(bufsize_spec))
    leveldb_options_set_write_buffer_size(ldbopts,FD_FIX2INT(bufsize_spec));
  else if (default_writebuf_size>0)
    leveldb_options_set_write_buffer_size(ldbopts,default_writebuf_size);
  else {}
  if (FD_UINTP(maxfiles_spec))
    leveldb_options_set_max_open_files(ldbopts,FD_FIX2INT(maxfiles_spec));
  else if (default_maxfiles>0)
    leveldb_options_set_max_open_files(ldbopts,default_maxfiles);
  else {}
  if (FD_UINTP(blocksize_spec))
    leveldb_options_set_block_size(ldbopts,FD_FIX2INT(blocksize_spec));
  else if (default_block_size>0)
    leveldb_options_set_block_size(ldbopts,default_block_size);
  else {}
  if (FD_UINTP(restart_spec))
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
                                          lispval opts)
{
  leveldb_cache_t *cache = NULL;
  lispval cache_size = fd_getopt(opts,SYM("CACHESIZE"),FD_VOID);
  if (FD_UINTP(cache_size))
    cache = leveldb_cache_create_lru(FD_FIX2INT(cache_size));
  else if (default_cache_size>0)
      cache = leveldb_cache_create_lru(default_cache_size);
  else cache = NULL;
  if (cache) leveldb_options_set_cache(ldbopts,cache);
  return cache;
}

static leveldb_env_t *get_leveldb_env(leveldb_options_t *ldbopts,lispval opts)
{
  if ((FD_VOIDP(opts))||(FD_FALSEP(opts)))
    return NULL;
  else {
    leveldb_env_t *env = leveldb_create_default_env(); int needed = 0;
    /* Setup env from opts and other defaults */
    if (needed) {
      leveldb_options_set_env(ldbopts,env);
      return env;}
    else {
      leveldb_env_destroy(env);
      return NULL;}}
}

static leveldb_readoptions_t *get_read_options(framerd_leveldb db,lispval opts_arg)
{
  if ( (FD_VOIDP(opts_arg)) ||
       (FD_FALSEP(opts_arg)) ||
       (FD_DEFAULTP(opts_arg)) )
    return db->readopts;
  else {
    int real = 0, free_opts = 0;
    lispval opts = (FD_VOIDP(opts_arg)) ? (db->opts) :
      (FD_VOIDP(db->opts)) ? (opts_arg) :
      (free_opts = 1,fd_make_pair(opts_arg,db->opts));
    leveldb_readoptions_t *readopts = leveldb_readoptions_create();
    /* Set up readopts based on opts */
    if (free_opts) fd_decref(opts);
    if (real)
      return readopts;
    else {
      leveldb_readoptions_destroy(readopts);
      return db->readopts;}}
}

static leveldb_writeoptions_t *get_write_options(framerd_leveldb db,lispval opts_arg)
{
  if ((FD_VOIDP(opts_arg))||(FD_FALSEP(opts_arg))||(FD_DEFAULTP(opts_arg)))
    return NULL;
  else {
    int real = 0, free_opts = 0;
    lispval opts = (FD_VOIDP(opts_arg)) ? (db->opts) :
      (FD_VOIDP(db->opts)) ? (opts_arg) :
      (free_opts = 1,fd_make_pair(opts_arg,db->opts));
    leveldb_writeoptions_t *writeopts = leveldb_writeoptions_create();
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
   (struct FRAMERD_LEVELDB *db,u8_string path,lispval opts)
{
  char *errmsg;
  memset(db,0,sizeof(struct FRAMERD_LEVELDB));
  db->path = u8_strdup(path);
  db->opts = fd_incref(opts);
  leveldb_options_t *options = get_leveldb_options(opts);
  leveldb_cache_t *cache = get_leveldb_cache(options,opts);
  leveldb_env_t *env = get_leveldb_env(options,opts);
  leveldb_readoptions_t *readopts = get_read_options(db,opts);
  leveldb_writeoptions_t *writeopts = get_write_options(db,opts);
  enum leveldb_status status = db->dbstatus;
  if (status == leveldb_raw) {
    u8_init_mutex(&(db->leveldb_lock));
    db->dbstatus = leveldb_sketchy;}
  u8_lock_mutex(&(db->leveldb_lock));
  db->dbstatus = leveldb_opening;
  if (readopts) db->readopts = readopts;
  else db->readopts = leveldb_readoptions_create();
  if (writeopts) db->writeopts = writeopts;
  else db->writeopts = leveldb_writeoptions_create();
  db->optionsptr = options;
  db->cacheptr = cache;
  db->envptr = env;
  if ((db->dbptr = leveldb_open(options,path,&errmsg))) {
    db->dbstatus = leveldb_opened;
    u8_unlock_mutex(&(db->leveldb_lock));
    return db;}
  else {
    fd_seterr("OpenFailed","fd_open_leveldb",errmsg,opts);
    db->dbstatus = leveldb_error;
    u8_free(db->path); db->path = NULL;
    leveldb_options_destroy(options);
    if (readopts) leveldb_readoptions_destroy(readopts);
    if (writeopts) leveldb_writeoptions_destroy(writeopts);
    if (cache) leveldb_cache_destroy(cache);
    if (env) leveldb_env_destroy(env);
    if (status == leveldb_raw) {
      u8_unlock_mutex(&(db->leveldb_lock));
      u8_destroy_mutex(&(db->leveldb_lock));}
    else u8_unlock_mutex(&(db->leveldb_lock));
    return NULL;}
}

FD_EXPORT
int fd_close_leveldb(framerd_leveldb db)
{
  int closed = 0;
  if ( (db->dbstatus == leveldb_opened) ||
       (db->dbstatus == leveldb_opening) ) {
    u8_lock_mutex(&(db->leveldb_lock));
    if (db->dbstatus == leveldb_opened) {
      db->dbstatus = leveldb_closing;
      leveldb_close(db->dbptr);
      db->dbstatus = leveldb_closed;
      leveldb_free(db->dbptr);
      db->dbptr = NULL;
      closed = 1;}
    u8_unlock_mutex(&(db->leveldb_lock));}
  return closed;
}

FD_EXPORT
framerd_leveldb fd_open_leveldb(framerd_leveldb db)
{
  if ( (db->dbstatus == leveldb_opened) ||
       (db->dbstatus == leveldb_opening) )
    return db;
  else {
    char *errmsg;
    u8_lock_mutex(&(db->leveldb_lock));
    if ( (db->dbstatus == leveldb_opened) ||
         (db->dbstatus == leveldb_opening) ) {
      u8_unlock_mutex(&(db->leveldb_lock));
      return db;}
    db->dbstatus = leveldb_opening;
    if ((db->dbptr = (leveldb_open(db->optionsptr,db->path,&errmsg)))) {
      db->dbstatus = leveldb_opened;
      u8_unlock_mutex(&(db->leveldb_lock));
      return db;}
    else {
      u8_unlock_mutex(&(db->leveldb_lock));
      fd_seterr("OpenFailed","fd_open_leveldb",errmsg,FD_VOID);
      return NULL;}}
}

/* DType object methods */

static int unparse_leveldb(struct U8_OUTPUT *out,lispval x)
{
  struct FD_LEVELDB *db = (fd_leveldb)x;
  u8_printf(out,"#<LevelDB %s>",db->leveldb.path);
  return 1;
}
static void recycle_leveldb(struct FD_RAW_CONS *c)
{
  struct FD_LEVELDB *db = (fd_leveldb)c;
  fd_close_leveldb(&(db->leveldb));
  if (db->leveldb.path) {
    u8_free(db->leveldb.path);
    db->leveldb.path = NULL;}
  fd_decref(db->leveldb.opts);
  leveldb_options_destroy(db->leveldb.optionsptr);
  u8_free(c);
}

/* Primitives */

static lispval leveldb_open_prim(lispval path,lispval opts)
{
  struct FD_LEVELDB *db = u8_alloc(struct FD_LEVELDB);
  fd_setup_leveldb(&(db->leveldb),FD_CSTRING(path),opts);
  if (db->leveldb.dbptr) {
    FD_INIT_CONS(db,fd_leveldb_type);
    return (lispval) db;}
  else {
    u8_free(db);
    return FD_ERROR_VALUE;}
}

static lispval leveldbp_prim(lispval arg)
{
  if (FD_TYPEP(arg,fd_leveldb_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval leveldb_close_prim(lispval leveldb)
{
  struct FD_LEVELDB *db = (fd_leveldb)leveldb;
  fd_close_leveldb(&(db->leveldb));
  return FD_TRUE;
}

static lispval leveldb_reopen_prim(lispval leveldb)
{
  struct FD_LEVELDB *db = (fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *reopened = fd_open_leveldb(&(db->leveldb));
  if (reopened) {
    fd_incref(leveldb);
    return leveldb;}
  else return FD_ERROR_VALUE;
}

/* Basic operations */

static lispval leveldb_get_prim(lispval leveldb,lispval key,lispval opts)
{
  struct FD_LEVELDB *db = (fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *fdldb = &(db->leveldb);
  char *errmsg = NULL;
  leveldb_readoptions_t *readopts = get_read_options(fdldb,opts);
  if (FD_PACKETP(key)) {
    ssize_t binary_size;
    unsigned char *binary_data=
      leveldb_get(db->leveldb.dbptr,readopts,
                  FD_PACKET_DATA(key),
                  FD_PACKET_LENGTH(key),
                  &binary_size,&errmsg);
    if (readopts!=fdldb->readopts)
      leveldb_readoptions_destroy(readopts);
    if (binary_data)
      return fd_bytes2packet(NULL,binary_size,binary_data);
    else if (errmsg)
      return fd_err("LevelDBError","leveldb_get_prim",errmsg,FD_VOID);
    else return FD_EMPTY_CHOICE;}
  else {
    struct FD_OUTBUF keyout; FD_INIT_BYTE_OUTPUT(&keyout,1024);
    if (fd_write_dtype(&keyout,key)>0) {
      lispval result = FD_VOID;
      ssize_t binary_size;
      unsigned char *binary_data=
        leveldb_get(db->leveldb.dbptr,readopts,
                    keyout.buffer,
                    keyout.bufwrite-keyout.buffer,
                    &binary_size,&errmsg);
      fd_close_outbuf(&keyout);
      if (readopts!=fdldb->readopts)
        leveldb_readoptions_destroy(readopts);
      if (binary_data == NULL) {
        if (errmsg)
          result = fd_err("LevelDBError","leveldb_get_prim",errmsg,FD_VOID);
        else result = FD_EMPTY_CHOICE;}
      else {
        struct FD_INBUF valuein;
        FD_INIT_BYTE_INPUT(&valuein,binary_data,binary_size);
        result = fd_read_dtype(&valuein);
        u8_free(binary_data);}
      return result;}
    else {
      if (readopts!=fdldb->readopts)
        leveldb_readoptions_destroy(readopts);
      return FD_ERROR_VALUE;}}
}

static lispval leveldb_put_prim(lispval leveldb,lispval key,lispval value,
                               lispval opts)
{
  char *errmsg = NULL;
  struct FD_LEVELDB *db = (fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *fdldb = &(db->leveldb);
  if ((FD_PACKETP(key))&&(FD_PACKETP(value))) {
    leveldb_writeoptions_t *useopts = get_write_options(fdldb,opts);
    leveldb_writeoptions_t *writeopts = (useopts)?(useopts):(fdldb->writeopts);
    leveldb_put(db->leveldb.dbptr,writeopts,
                FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                FD_PACKET_DATA(value),FD_PACKET_LENGTH(value),
                &errmsg);
    if (useopts) leveldb_writeoptions_destroy(useopts);
    if (errmsg)
      return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
    else return FD_VOID;}
  else {
    struct FD_OUTBUF keyout; FD_INIT_BYTE_OUTPUT(&keyout,1024);
    struct FD_OUTBUF valout; FD_INIT_BYTE_OUTPUT(&valout,1024);
    if (fd_write_dtype(&keyout,key)<0) {
      fd_close_outbuf(&keyout);
      fd_close_outbuf(&valout);
      return FD_ERROR_VALUE;}
    else if (fd_write_dtype(&valout,value)<0) {
      fd_close_outbuf(&keyout);
      fd_close_outbuf(&valout);
      return FD_ERROR_VALUE;}
    else {
      leveldb_writeoptions_t *useopts = get_write_options(fdldb,opts);
      leveldb_writeoptions_t *writeopts = (useopts)?(useopts):(fdldb->writeopts);
      leveldb_put(db->leveldb.dbptr,writeopts,
                  keyout.buffer,keyout.bufwrite-keyout.buffer,
                  valout.buffer,valout.bufwrite-valout.buffer,
                  &errmsg);
      fd_close_outbuf(&keyout);
      fd_close_outbuf(&valout);
      if (useopts) leveldb_writeoptions_destroy(useopts);
      if (errmsg)
        return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
      else return FD_VOID;}}
}

static lispval leveldb_drop_prim(lispval leveldb,lispval key,lispval opts)
{
  char *errmsg = NULL;
  struct FD_LEVELDB *db = (fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *fdldb = &(db->leveldb);
  leveldb_writeoptions_t *useopts = get_write_options(fdldb,opts);
  leveldb_writeoptions_t *writeopts = get_write_options(fdldb,opts);
  if (FD_PACKETP(key)) {
    leveldb_delete(db->leveldb.dbptr,writeopts,
                   FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                   &errmsg);
    if (useopts) leveldb_writeoptions_destroy(useopts);
    if (errmsg)
      return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
    else return FD_VOID;}
  else {
    struct FD_OUTBUF keyout; FD_INIT_BYTE_OUTPUT(&keyout,1024);
    if (fd_write_dtype(&keyout,key)<0) {
      fd_close_outbuf(&keyout);
      return FD_ERROR_VALUE;}
    else {
      leveldb_delete(db->leveldb.dbptr,writeopts,
                     keyout.buffer,keyout.bufwrite-keyout.buffer,
                     &errmsg);
      fd_close_outbuf(&keyout);
      if (useopts) leveldb_writeoptions_destroy(useopts);
      if (errmsg)
        return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
      else return FD_VOID;}}
}

struct LEVELDB_KEYBUF {
  unsigned int keypos:31, dtype:1;
  size_t len;
  const unsigned char *bytes;};

static int cmp_keybufs(const void *vx,const void *vy)
{
  const struct LEVELDB_KEYBUF *kbx = (struct LEVELDB_KEYBUF *) vx;
  const struct LEVELDB_KEYBUF *kby = (struct LEVELDB_KEYBUF *) vy;
  int min_len = (kbx->len < kby->len) ? (kbx->len) : (kby->len);
  int cmp = memcmp(kbx->bytes,kby->bytes,min_len);
  if (cmp)
    return cmp;
  else if (kbx->len < kby->len)
    return -1;
  else if (kbx->len == kby->len)
    return 0;
  else return 1;
}

static struct LEVELDB_KEYBUF *fetchn(struct FRAMERD_LEVELDB *db,int n,
                                     struct LEVELDB_KEYBUF *keys,
                                     leveldb_readoptions_t *readopts)
{
  struct LEVELDB_KEYBUF *results = u8_alloc_n(n,struct LEVELDB_KEYBUF);
  qsort(keys,n,sizeof(struct LEVELDB_KEYBUF),cmp_keybufs);
  leveldb_iterator_t *iterator = leveldb_create_iterator(db->dbptr,readopts);
  int i = 0; while (i<n) {
    results[i].keypos = keys[i].keypos;
    results[i].dtype  = keys[i].dtype;
    leveldb_iter_seek(iterator,keys[i].bytes,keys[i].len);
    results[i].bytes = leveldb_iter_value(iterator,&(results[i].len));
    i++;}
  leveldb_iter_destroy(iterator);
  return results;
}

static lispval leveldb_getn_prim(lispval leveldb,lispval keys,lispval opts)
{
  struct FD_LEVELDB *dbcons = (fd_leveldb)leveldb;
  struct FRAMERD_LEVELDB *db = &(dbcons->leveldb);
  leveldb_readoptions_t *readopts = get_read_options(db,opts);
  int i = 0, n = FD_VECTOR_LENGTH(keys);
  lispval results = fd_empty_vector(n);
  struct LEVELDB_KEYBUF *keyvec = u8_alloc_n(n,struct LEVELDB_KEYBUF);
  unsigned char buf[5000];
  struct FD_OUTBUF out; FD_INIT_OUTBUF(&out,buf,5000,FD_IS_WRITING);
  while (i<n) {
    lispval key = FD_VECTOR_REF(keys,i);
    keyvec[i].keypos = i;
    if (FD_PACKETP(key)) {
      keyvec[i].bytes = FD_PACKET_DATA(key);
      keyvec[i].len   = FD_PACKET_LENGTH(key);
      keyvec[i].dtype = 0;}
    else {
      size_t pos = (out.bufwrite-out.buffer);
      ssize_t len = fd_write_dtype(&out,key);
      keyvec[i].len = len;
      keyvec[i].bytes = (unsigned char *) pos;
      keyvec[i].dtype = 1;}
    i++;}
  struct LEVELDB_KEYBUF *values = fetchn(db,n,keyvec,readopts);
  i=0; while (i<n) {
    int keypos = values[i].keypos;
    if (values[i].dtype) {
      struct FD_INBUF in; FD_INIT_INBUF(&in,values[i].bytes,values[i].len,0);
      lispval v = fd_read_dtype(&in);
      FD_VECTOR_SET(results,keypos,v);}
    else {
      lispval v = fd_init_packet(NULL,values[i].len,values[i].bytes);
      FD_VECTOR_SET(results,keypos,v);}
    u8_free(values[i].bytes);
    i++;}
  fd_close_outbuf(&out);
  if (readopts!=db->readopts)
    leveldb_readoptions_destroy(readopts);
  return results;
}

static leveldb_readoptions_t *default_readopts;
static leveldb_writeoptions_t *default_writeopts;
static leveldb_writeoptions_t *sync_writeopts;

static lispval get_prop(leveldb_t *dbptr,char *key,lispval dflt)
{
  ssize_t data_size; char *errmsg = NULL;
  unsigned char *buf = leveldb_get
    (dbptr,default_readopts,key,strlen(key),&data_size,&errmsg);
  if (buf) {
    lispval result = FD_VOID;
    struct FD_INBUF in;
    FD_INIT_BYTE_INPUT(&in,buf,data_size);
    result = fd_read_dtype(&in);
    u8_free(buf);
    return result;}
  else if (errmsg)
    return fd_err("LevelDBerror","get_prop",NULL,FD_VOID);
  else return dflt;
}

static ssize_t set_prop(leveldb_t *dbptr,char *key,lispval value,
                        leveldb_writeoptions_t *writeopts)
{
  ssize_t dtype_len; char *errmsg = NULL;
  struct FD_OUTBUF out;
  FD_INIT_BYTE_OUTPUT(&out,512);
  if ((dtype_len = fd_write_dtype(&out,value))>0) {
    leveldb_put(dbptr,writeopts,key,strlen(key),
                out.buffer,out.bufwrite-out.buffer,
                &errmsg);
    fd_close_outbuf(&out);
    if (errmsg)
      return fd_reterr("LevelDBerror","set_prop",errmsg,FD_VOID);
    else return dtype_len;}
  else return -1;
}

/* LevelDB pool backends */

static struct FD_POOL_HANDLER leveldb_pool_handler;

FD_EXPORT
fd_pool fd_use_leveldb_pool(u8_string path,lispval opts)
{
  struct FD_LEVELDB_POOL *pool = u8_alloc(struct FD_LEVELDB_POOL);
  if (fd_setup_leveldb(&(pool->leveldb),path,opts)) {
    leveldb_t *dbptr = pool->leveldb.dbptr;
    lispval base = get_prop(dbptr,"\377BASE",FD_VOID);
    lispval cap = get_prop(dbptr,"\377CAPACITY",FD_VOID);
    lispval load = get_prop(dbptr,"\377LOAD",FD_VOID);
    lispval metadata = get_prop(dbptr,"\377METADATA",FD_VOID);
    if ((FD_OIDP(base)) && (FD_UINTP(cap)) && (FD_UINTP(load))) {
      u8_string rname = u8_realpath(path,NULL);
      lispval adjunct_opt = fd_getopt(opts,SYM("ADJUNCT"),FD_VOID);
      lispval read_only_opt = fd_getopt(opts,SYM("READONLY"),FD_VOID);
      lispval label = get_prop(dbptr,"\377LABEL",FD_VOID);
      fd_init_pool((fd_pool)pool,
                   FD_OID_ADDR(base),FD_FIX2INT(cap),
                   &leveldb_pool_handler,
                   u8_strdup(path),rname);
      u8_free(rname);
      if (FD_VOIDP(read_only_opt))
        read_only_opt = get_prop(dbptr,"\377READONLY",FD_VOID);
      if (FD_VOIDP(read_only_opt))
        adjunct_opt = get_prop(dbptr,"\377ADJUNCT",FD_VOID);
      if ( (! FD_VOIDP(read_only_opt)) && (FD_TRUEP(read_only_opt)) )
        pool->pool_flags |= FD_STORAGE_READ_ONLY;
      if ( (! FD_VOIDP(adjunct_opt)) && (!(FD_FALSEP(adjunct_opt))) )
        pool->pool_flags |= FD_POOL_ADJUNCT;
      pool->pool_load = FD_FIX2INT(load);
      if (FD_STRINGP(label)) {
        pool->pool_label = u8_strdup(FD_CSTRING(label));}
      if (FD_SLOTMAPP(metadata)) {
        fd_copy_slotmap((fd_slotmap)metadata,
                        &(pool->pool_metadata));
        fd_set_modified(((lispval)(&(pool->pool_metadata))),0);}
      fd_decref(adjunct_opt); fd_decref(read_only_opt);
      fd_decref(metadata); fd_decref(label);
      fd_decref(base); fd_decref(cap); fd_decref(load);
      fd_register_pool((fd_pool)pool);
      return (fd_pool)pool;}
    else  {
      fd_decref(base); fd_decref(cap); fd_decref(load);
      fd_close_leveldb(&(pool->leveldb));
      u8_free(pool);
      fd_seterr("NotAPoolDB","fd_leveldb_pool",NULL,FD_VOID);
      return (fd_pool)NULL;}}
  else return NULL;
}

FD_EXPORT
fd_pool fd_make_leveldb_pool(u8_string path,lispval base,lispval cap,
                             lispval opts)
{
  struct FD_LEVELDB_POOL *pool = u8_alloc(struct FD_LEVELDB_POOL);

  lispval load = fd_getopt(opts,SYM("LOAD"),FD_FIXZERO);

  if ((!(FD_OIDP(base)))||(!(FD_UINTP(cap)))||(!(FD_UINTP(load)))) {
    u8_free(pool);
    fd_seterr("Not enough information to create a pool",
              "fd_make_leveldb_pool",path,opts);
    return (fd_pool)NULL;}
  else if (fd_setup_leveldb(&(pool->leveldb),path,opts)) {
    leveldb_t *dbptr = pool->leveldb.dbptr;
    lispval label = fd_getopt(opts,SYM("LABEL"),FD_VOID);
    lispval metadata = fd_getopt(opts,SYM("METADATA"),FD_VOID);
    lispval given_base = get_prop(dbptr,"\377BASE",FD_VOID);
    lispval given_cap = get_prop(dbptr,"\377CAPACITY",FD_VOID);
    lispval given_load = get_prop(dbptr,"\377LOAD",FD_VOID);
    lispval cur_label = get_prop(dbptr,"\377LABEL",FD_VOID);
    lispval cur_metadata = get_prop(dbptr,"\377METADATA",FD_VOID);
    lispval ctime_val = fd_getopt(opts,fd_intern("CTIME"),FD_VOID);
    lispval mtime_val = fd_getopt(opts,fd_intern("MTIME"),FD_VOID);
    lispval generation_val = fd_getopt(opts,fd_intern("GENERATION"),FD_VOID);

    u8_string rname = u8_realpath(path,NULL);
    time_t now=time(NULL), ctime, mtime;
    int generation = 0;

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

    if (! (FD_VOIDP(metadata)) ) {
      if (FD_VOIDP(cur_metadata))
        set_prop(dbptr,"\377METADATA",metadata,sync_writeopts);
      else if (fd_testopt(opts,SYM("FORCE"),FD_VOID))
        set_prop(dbptr,"\377METADATA",metadata,sync_writeopts);
      else u8_log(LOG_WARN,"ExistingMetadata",
                  "Not overwriting existing metatdata in leveldb pool %s",path);}

    if (FD_FIXNUMP(ctime_val))
      ctime = (time_t) FD_FIX2INT(ctime_val);
    else if (FD_PRIM_TYPEP(ctime_val,fd_timestamp_type)) {
      struct FD_TIMESTAMP *moment = (fd_timestamp) ctime_val;
      ctime = moment->u8xtimeval.u8_tick;}
    else ctime=now;
    fd_decref(ctime_val);
    ctime_val = fd_time2timestamp(ctime);
    set_prop(dbptr,"\377CTIME",ctime_val,sync_writeopts);

    if (FD_FIXNUMP(mtime_val))
      mtime = (time_t) FD_FIX2INT(mtime_val);
    else if (FD_PRIM_TYPEP(ctime_val,fd_timestamp_type)) {
      struct FD_TIMESTAMP *moment = (fd_timestamp) mtime_val;
      mtime = moment->u8xtimeval.u8_tick;}
    else mtime=now;
    fd_decref(mtime_val);
    mtime_val = fd_time2timestamp(mtime);
    set_prop(dbptr,"\377MTIME",mtime_val,sync_writeopts);

    if (FD_FIXNUMP(generation_val))
      generation=FD_FIX2INT(generation_val);
    else generation=0;
    fd_decref(generation_val);
    set_prop(dbptr,"\377GENERATION",FD_INT(generation),sync_writeopts);



    fd_init_pool((fd_pool)pool,
                 FD_OID_ADDR(base),FD_FIX2INT(cap),
                 &leveldb_pool_handler,
                 u8_strdup(path),rname);
    u8_free(rname);
    pool->pool_flags &= ~FD_STORAGE_READ_ONLY;
    pool->pool_load = FD_FIX2INT(load);
    if (FD_STRINGP(label)) {
      pool->pool_label = u8_strdup(FD_CSTRING(label));}
    fd_register_pool((fd_pool)pool);
    return (fd_pool)pool;}
  else  {
    fd_close_leveldb(&(pool->leveldb));
    u8_free(pool);
    fd_seterr("NotAPoolDB","fd_leveldb_pool",NULL,FD_VOID);
    return (fd_pool)NULL;}
}

static lispval read_oid_value(fd_leveldb_pool p,struct FD_INBUF *in)
{
  return fd_read_dtype(in);
}

static int write_oid_value(fd_leveldb_pool p,
                           struct FD_OUTBUF *out,
                           lispval value)
{
  return fd_write_dtype(out,value);
}

static lispval get_oid_value(fd_leveldb_pool ldp,unsigned int offset)
{
  ssize_t data_size; char *errmsg = NULL;
  leveldb_t *dbptr = ldp->leveldb.dbptr;
  leveldb_readoptions_t *readopts = ldp->leveldb.readopts;
  unsigned char keybuf[5];
  keybuf[0]=0xFE;
  keybuf[1]=((offset>>24)&0XFF);
  keybuf[2]=((offset>>16)&0XFF);
  keybuf[3]=((offset>>8)&0XFF);
  keybuf[4]=(offset&0XFF);
  unsigned char *buf = leveldb_get
    (dbptr,readopts,keybuf,5,&data_size,&errmsg);
  if (buf) {
    lispval result = FD_VOID;
    struct FD_INBUF in;
    FD_INIT_BYTE_INPUT(&in,buf,data_size);
    result = read_oid_value(ldp,&in);
    u8_free(buf);
    return result;}
  else if (errmsg)
    return fd_err("LevelDBerror","get_prop",NULL,FD_VOID);
  else return FD_VOID;
}

static int set_oid_value(fd_leveldb_pool ldp,
                         unsigned int offset,
                         lispval value,
                         leveldb_writeoptions_t *writeopts)
{
  struct FD_OUTBUF out; ssize_t dtype_len;
  leveldb_t *dbptr = ldp->leveldb.dbptr;
  if (writeopts == NULL) writeopts = ldp->leveldb.writeopts;
  FD_INIT_BYTE_OUTPUT(&out,512);
  unsigned char buf[5];
  buf[0]=0xFE;
  buf[1]=((offset>>24)&0XFF);
  buf[2]=((offset>>16)&0XFF);
  buf[3]=((offset>>8)&0XFF);
  buf[4]=(offset&0XFF);
  if ((dtype_len = write_oid_value(ldp,&out,value))>0) {
    char *errmsg = NULL;
    leveldb_put
      (dbptr,writeopts,buf,5,
       out.buffer,out.bufwrite-out.buffer,
       &errmsg);
    fd_close_outbuf(&out);
    if (errmsg == NULL)
      return dtype_len;
    fd_seterr("LevelDB pool save error","set_oidvalue",errmsg,FD_VOID);
    return -1;}
  else return -1;
}

static int queue_oid_value(fd_leveldb_pool ldp,
                           unsigned int offset,
                           lispval value,
                           leveldb_writebatch_t *batch)
{
  struct FD_OUTBUF out; ssize_t dtype_len;
  unsigned char buf[5];
  buf[0]=0xFE;
  buf[1]=((offset>>24)&0XFF);
  buf[2]=((offset>>16)&0XFF);
  buf[3]=((offset>>8)&0XFF);
  buf[4]=(offset&0XFF);
  FD_INIT_BYTE_OUTPUT(&out,512);
  if ((dtype_len = write_oid_value(ldp,&out,value))>0) {
    leveldb_writebatch_put
      (batch,buf,5,out.buffer,out.bufwrite-out.buffer);
    fd_close_outbuf(&out);
    return dtype_len;}
  else return -1;
}

static lispval leveldb_pool_alloc(fd_pool p,int n)
{
  lispval results = FD_EMPTY_CHOICE; unsigned int i = 0, start;
  struct FD_LEVELDB_POOL *pool = (struct FD_LEVELDB_POOL *)p;
  fd_lock_pool_struct(p,1);
  if (pool->pool_load+n>=pool->pool_capacity) {
    fd_unlock_pool_struct(p);
    return fd_err(fd_ExhaustedPool,"leveldb_pool_alloc",pool->poolid,FD_VOID);}
  start = pool->pool_load;
  pool->pool_load = start+n;
  fd_unlock_pool_struct(p);
  while (i < n) {
    FD_OID new_addr = FD_OID_PLUS(pool->pool_base,start+i);
    lispval new_oid = fd_make_oid(new_addr);
    FD_ADD_TO_CHOICE(results,new_oid);
    i++;}
  return results;
}

static lispval leveldb_pool_fetchoid(fd_pool p,lispval oid)
{
  struct FD_LEVELDB_POOL *pool = (struct FD_LEVELDB_POOL *)p;
  FD_OID addr = FD_OID_ADDR(oid);
  unsigned int offset = FD_OID_DIFFERENCE(addr,pool->pool_base);
  return get_oid_value(pool,offset);
}

struct OFFSET_ENTRY {
  unsigned int oid_offset;
  unsigned int fetch_offset;};

static int off_compare(const void *x,const void *y)
{
  const struct OFFSET_ENTRY *ex = x, *ey = y;
  if (ex->oid_offset<ey->oid_offset) return -1;
  else if (ex->oid_offset>ey->oid_offset) return 1;
  else return 0;
}

static lispval *leveldb_pool_fetchn(fd_pool p,int n,lispval *oids)
{
  struct FD_LEVELDB_POOL *pool = (struct FD_LEVELDB_POOL *)p;
  struct OFFSET_ENTRY *entries = u8_alloc_n(n,struct OFFSET_ENTRY);
  leveldb_readoptions_t *readopts = pool->leveldb.readopts;
  unsigned int largest_offset = 0, offsets_sorted = 1;
  lispval *values = u8_alloc_n(n,lispval);
  FD_OID base = p->pool_base;
  int i = 0; while (i<n) {
    FD_OID addr = FD_OID_ADDR(oids[i]);
    unsigned int offset = FD_OID_DIFFERENCE(addr,base);
    entries[i].oid_offset = FD_OID_DIFFERENCE(addr,base);
    entries[i].fetch_offset = i;
    if (offsets_sorted) {
      if (offset>=largest_offset) largest_offset = offset;
      else offsets_sorted = 0;}
    i++;}
  if (!(offsets_sorted)) {
    qsort(entries,n,sizeof(struct OFFSET_ENTRY),off_compare);}
  leveldb_iterator_t *iterator=
    leveldb_create_iterator(pool->leveldb.dbptr,readopts);
  i = 0; while (i<n) {
    unsigned char keybuf[5];
    unsigned int offset = entries[i].oid_offset;
    unsigned int fetch_offset = entries[i].fetch_offset;
    keybuf[0]=0xFE;
    keybuf[1]=((offset>>24)&0XFF);
    keybuf[2]=((offset>>16)&0XFF);
    keybuf[3]=((offset>>8)&0XFF);
    keybuf[4]=(offset&0XFF);
    leveldb_iter_seek(iterator,keybuf,5);
    ssize_t bytes_len;
    const unsigned char *bytes = leveldb_iter_value(iterator,&bytes_len);
    if (bytes) {
      struct FD_INBUF in;
      FD_INIT_BYTE_INPUT(&in,bytes,bytes_len);
      lispval oidvalue = read_oid_value(pool,&in);
      values[fetch_offset]=oidvalue;}
    else values[fetch_offset]=FD_VOID;
    i++;}
  leveldb_iter_destroy(iterator);
  u8_free(entries);
  return values;
}

static int leveldb_pool_getload(fd_pool p)
{
  struct FD_LEVELDB_POOL *pool = (struct FD_LEVELDB_POOL *)p;
  return pool->pool_load;
}

static int leveldb_pool_lock(fd_pool p,lispval oids)
{
  /* What should this really do? */
  return 1;
}

static int leveldb_pool_unlock(fd_pool p,lispval oids)
{
  /* What should this really do? Flush edits to disk? storen? */
  return 1;
}

static void leveldb_pool_close(fd_pool p)
{
  struct FD_LEVELDB_POOL *pool = (struct FD_LEVELDB_POOL *)p;
  fd_close_leveldb(&(pool->leveldb));
}


static int leveldb_pool_storen(fd_pool p,int n,lispval *oids,lispval *values)
{
  struct FD_LEVELDB_POOL *pool = (struct FD_LEVELDB_POOL *)p;
  leveldb_t *dbptr = pool->leveldb.dbptr;
  int i = 0, errval = 0;
  if (n>7) {
    char *errmsg = NULL;
    leveldb_writebatch_t *batch = leveldb_writebatch_create();
    ssize_t n_bytes = 0;
    while (i<n) {
      lispval oid = oids[i], value = values[i];
      FD_OID addr = FD_OID_ADDR(oid);
      unsigned int offset = FD_OID_DIFFERENCE(addr,pool->pool_base);
      ssize_t len = queue_oid_value(pool,offset,value,batch);
      if (len<0) {errval = len; break;}
      else {n_bytes+=len; i++;}}
    if (errval>=0) {
      leveldb_write(dbptr,sync_writeopts,batch,&errmsg);
      if (errmsg) {
        u8_seterr("LevelDBError","leveldb_pool_storen",errmsg);
        errval = -1;}}
    leveldb_writebatch_destroy(batch);}
  else while (i<n) {
    lispval oid = oids[i], value = values[i];
    FD_OID addr = FD_OID_ADDR(oid);
    unsigned int offset = FD_OID_DIFFERENCE(addr,pool->pool_base);
    if ((errval = set_oid_value(pool,offset,value,sync_writeopts))<0)
      break;
    else i++;}
  if (errval>=0) {
    fd_lock_pool_struct(p,1); {
      lispval loadval = FD_INT(pool->pool_load);
      int retval = set_prop(dbptr,"\377LOAD",loadval,sync_writeopts);
      fd_decref(loadval);
      fd_unlock_pool_struct(p);
      if (retval<0) return retval;}}
  if (errval<0)
    return errval;
  else return n;
}

static int leveldb_pool_commit(fd_pool p,fd_commit_phase phase,
                               struct FD_POOL_COMMITS *commits)
{
  switch (phase) {
  case fd_commit_save:
    return leveldb_pool_storen(p,commits->commit_count,
                               commits->commit_oids,
                               commits->commit_vals);
  default: 
    return 0;
  }
}

/* Creating leveldb pools */

static fd_pool leveldb_pool_create(u8_string spec,void *type_data,
                                   fd_storage_flags storage_flags,
                                   lispval opts)
{
  lispval base_oid = fd_getopt(opts,fd_intern("BASE"),VOID);
  lispval capacity_arg = fd_getopt(opts,fd_intern("CAPACITY"),VOID);
  lispval load_arg = fd_getopt(opts,fd_intern("LOAD"),VOID);
  lispval metadata = fd_getopt(opts,fd_intern("METADATA"),VOID);
  unsigned int capacity;
  fd_pool dbpool = NULL;
  int rv = 0;
  if (u8_file_existsp(spec)) {
    fd_seterr(_("FileAlreadyExists"),"bigpool_create",spec,VOID);
    return NULL;}
  else if (!(OIDP(base_oid))) {
    fd_seterr("Not a base oid","bigpool_create",spec,base_oid);
    rv = -1;}
  else if (FD_ISINT(capacity_arg)) {
    int capval = fd_getint(capacity_arg);
    if (capval<=0) {
      fd_seterr("Not a valid capacity","bigpool_create",
                spec,capacity_arg);
      rv = -1;}
    else capacity = capval;}
  else {
    fd_seterr("Not a valid capacity","bigpool_create",
              spec,capacity_arg);
    rv = -1;}
  if (rv<0) {}
  else if (FD_ISINT(load_arg)) {
    int loadval = fd_getint(load_arg);
    if (loadval<0) {
      fd_seterr("Not a valid load","bigpool_create",spec,load_arg);
      rv = -1;}
    else if (loadval > capacity) {
      fd_seterr(fd_PoolOverflow,"bigpool_create",spec,load_arg);
      rv = -1;}
    else NO_ELSE;}
  else if ( (FALSEP(load_arg)) || (EMPTYP(load_arg)) ||
            (VOIDP(load_arg)) || (load_arg == FD_DEFAULT_VALUE)) {}
  else {
    fd_seterr("Not a valid load","bigpool_create",spec,load_arg);
    rv = -1;}

  if (rv>=0)
    dbpool = fd_make_leveldb_pool(spec,FD_OID_ADDR(base_oid),capacity,opts);
  if (dbpool == NULL) rv=-1;
  fd_decref(base_oid);
  fd_decref(capacity_arg);
  fd_decref(load_arg);
  fd_decref(load_arg);
  fd_decref(metadata);

  if (rv>=0) {
    fd_set_file_opts(spec,opts);
    return fd_open_pool(spec,storage_flags,opts);}
  else return NULL;
}

/* The LevelDB pool handler */

static struct FD_POOL_HANDLER leveldb_pool_handler={
  "leveldb_pool", 1, sizeof(struct FD_LEVELDB_POOL), 12,
  leveldb_pool_close, /* close */
  leveldb_pool_alloc, /* alloc */
  leveldb_pool_fetchoid, /* fetch */
  leveldb_pool_fetchn, /* fetchn */
  leveldb_pool_getload, /* getload */
  leveldb_pool_lock, /* lock */
  leveldb_pool_unlock, /* release */
  leveldb_pool_commit, /* commit */
  NULL, /* swapout */
  leveldb_pool_create, /* create */
  NULL,  /* walk */
  NULL, /* recycle */
  NULL  /* poolctl */
};

#if 0
/* LevelDB indexes */

static struct FD_INDEX_HANDLER leveldb_index_handler;

fd_index fd_open_leveldb_index(u8_string path,fd_storage_flags flags,lispval opts)
{
  struct FD_LEVELDB_INDEX *index = u8_alloc(struct FD_LEVELDB_INDEX);
  if (fd_setup_leveldb(&(index->leveldb),path,opts)) {
    leveldb_t *dbptr = index->leveldb.dbptr;
    u8_string rname = u8_realpath(path,NULL);
    lispval label = get_prop(dbptr,"\377LABEL",FD_VOID);
    fd_init_index((fd_index)index,
                  &leveldb_index_handler,
                  (FD_STRINGP(label)) ? (FD_CSTRING(label)) : (path),
                  rname,
                  0);
    fd_decref(label);
    if (fd_testopt(opts,SYM("READONLY"),FD_VOID))
      index->index_flags |= FD_STORAGE_READ_ONLY;
    index->index_flags = flags;
    fd_register_index((fd_index)index);
    return (fd_index)index;}
  else {
    u8_free(index);
    return NULL;}
}

FD_EXPORT
fd_index fd_make_leveldb_index(u8_string path,lispval opts)
{
  struct FD_LEVELDB_INDEX *index = u8_alloc(struct FD_LEVELDB_INDEX);
  lispval label = fd_getopt(opts,SYM("LABEL"),FD_VOID);
  if (fd_setup_leveldb(&(index->leveldb),path,opts)) {
    leveldb_t *dbptr = index->leveldb.dbptr;
    lispval cur_label = get_prop(dbptr,"\377LABEL",FD_VOID);
    u8_string rname = u8_realpath(path,NULL);
    if (!(FD_VOIDP(label))) {
      if (!(FD_EQUALP(label,cur_label)))
        set_prop(dbptr,"\377LABEL",label,sync_writeopts);}
    fd_init_index((fd_index)index,
                  &leveldb_index_handler,
                  (FD_STRINGP(label)) ? (FD_CSTRING(label)) : (rname),
                  rname,
                  0);
    index->index_flags &= ~FD_STORAGE_READ_ONLY;
    fd_register_index((fd_index)index);
    return (fd_index)index;}
  else {
    u8_free(index);
    return (fd_index) NULL;}
}

static void leveldb_index_close(fd_index ix)
{
  struct FD_LEVELDB_INDEX *ldbx = (struct FD_LEVELDB_INDEX *)ix;
  fd_close_leveldb(&(ldbx->leveldb));
}


/* Initializing the driver module */

static struct FD_INDEX_HANDLER leveldb_index_handler={
  "leveldb_index", 1, sizeof(struct FD_LEVELDB_INDEX), 14,
  leveldb_index_close, /* close */
  NULL, /* commit */
  NULL, /* fetch */
  NULL, /* fetchsize */
  NULL, /* prefetch */
  NULL, /* fetchn */
  NULL, /* fetchkeys */
  NULL, /* fetchinfo */
  NULL, /* batchadd */
  NULL, /* create */
  NULL, /* walk */
  NULL, /* recycle */
  NULL /* indexctl */
};

#endif


/* Scheme primitives */

static lispval use_leveldb_pool_prim(lispval path,lispval opts)
{
  fd_pool pool = fd_use_leveldb_pool(FD_CSTRING(path),opts);
  return fd_pool2lisp(pool);
}

static lispval make_leveldb_pool_prim(lispval path,lispval base,lispval cap,
                                     lispval opts)
{
  fd_pool pool = fd_make_leveldb_pool(FD_CSTRING(path),base,cap,opts);
  return fd_pool2lisp(pool);
}

/* Table functions */

static lispval leveldb_table_get(lispval db,lispval key,lispval dflt)
{
  lispval rv = leveldb_get_prim(db,key,FD_FALSE);
  if (FD_EMPTYP(rv))
    return fd_incref(dflt);
  else return rv;
}

static int leveldb_table_store(lispval db,lispval key,lispval val)
{
  lispval rv = leveldb_put_prim(db,key,val,FD_FALSE);
  if (FD_ABORTP(rv))
    return -1;
  else {
    fd_decref(rv);
    return 1;}
}

/* Initialization */

FD_EXPORT int fd_init_leveldb()
{
  lispval module;
  if (leveldb_initialized) return 0;
  leveldb_initialized = u8_millitime();

  default_readopts = leveldb_readoptions_create();
  default_writeopts = leveldb_writeoptions_create();
  sync_writeopts = leveldb_writeoptions_create();
  leveldb_writeoptions_set_sync(sync_writeopts,1);

  fd_leveldb_type = fd_register_cons_type("leveldb");

  fd_unparsers[fd_leveldb_type]=unparse_leveldb;
  fd_recyclers[fd_leveldb_type]=recycle_leveldb;
  fd_type_names[fd_leveldb_type]="LevelDB";

  /* Table functions for leveldbs */
  fd_tablefns[fd_leveldb_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_leveldb_type]->get = leveldb_table_get;
  fd_tablefns[fd_leveldb_type]->store = leveldb_table_store;

  module = fd_new_module("LEVELDB",0);

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
  fd_idefn(module,fd_make_cprim3x("LEVELDB/GETN",leveldb_getn_prim,2,
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

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
