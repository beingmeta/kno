/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* leveldb.c
   This implements FramerD bindings to leveldb.
   Copyright (C) 2007-2018 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#define FD_INLINE_BUFIO 1

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
#include "framerd/bufio.h"

#include <libu8/libu8.h>
#include <libu8/libu8io.h>
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
static int default_maxfiles = -1;
static int default_restart_interval = -1;
static int default_compression = 0;

#define LEVELDB_META_KEY       0xFF
#define LEVELDB_CODED_KEY      0xFE
#define LEVELDB_CODED_VALUES   0XFD
#define LEVELDB_KEYINFO        0xFC

#define SYM(x) (fd_intern(x))

struct BYTEVEC {
  size_t n_bytes;
  const unsigned char *bytes;};
struct LEVELDB_KEYBUF {
  lispval key;
  unsigned int keypos:31, dtype:1;
  struct BYTEVEC encoded;};

typedef int (*leveldb_prefix_iterfn)(lispval key,struct BYTEVEC *prefix,
                                     struct BYTEVEC *keybuf,
                                     struct BYTEVEC *valbuf,
                                     void *state);
typedef int (*leveldb_prefix_key_iterfn)(lispval key,struct BYTEVEC *prefix,
                                         struct BYTEVEC *keybuf,
                                         void *state);

static ssize_t leveldb_encode_key(struct FD_OUTBUF *out,lispval key,
                                  struct FD_SLOTCODER *slotcodes,
                                  int addcode);
static ssize_t leveldb_encode_value(struct FD_OUTBUF *out,lispval val,
                                    struct FD_OIDCODER *oc);

static lispval leveldb_decode_value(struct FD_INBUF *in,
                                    struct FD_OIDCODER *oidcodes);

/* Initialization */

FD_EXPORT int fd_init_leveldb(void) FD_LIBINIT_FN;

static long long int leveldb_initialized = 0;

static leveldb_readoptions_t *default_readopts;
static leveldb_writeoptions_t *sync_writeopts;

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
    int modified = 0, free_opts = 0;
    lispval opts = (FD_VOIDP(opts_arg)) ? (db->opts) :
      (FD_VOIDP(db->opts)) ? (opts_arg) :
      (free_opts = 1,fd_make_pair(opts_arg,db->opts));
    leveldb_readoptions_t *readopts = leveldb_readoptions_create();
    /* Set up readopts based on opts */
    if (free_opts) fd_decref(opts);
    if (modified)
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
    int modified = 0, free_opts = 0;
    lispval opts = (FD_VOIDP(opts_arg)) ? (db->opts) :
      (FD_VOIDP(db->opts)) ? (opts_arg) :
      (free_opts = 1,fd_make_pair(opts_arg,db->opts));
    leveldb_writeoptions_t *writeopts = leveldb_writeoptions_create();
    /* Set up writeopts based on opts */
    if (free_opts) fd_decref(opts);
    if (modified)
      return writeopts;
    else {
      leveldb_writeoptions_destroy(writeopts);
      return db->writeopts;}}
}

/* Initializing FD_LEVELDB structs */

FD_EXPORT
struct FD_LEVELDB *fd_setup_leveldb
   (struct FD_LEVELDB *db,u8_string path,lispval opts)
{
  char *errmsg = NULL;
  memset(db,0,sizeof(struct FD_LEVELDB));
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
      /* if (db->dbptr) leveldb_free(db->dbptr); */
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
  struct FD_LEVELDB_CONS *db = (fd_leveldb)x;
  u8_printf(out,"#<LevelDB %s>",db->leveldb.path);
  return 1;
}
static void recycle_leveldb(struct FD_RAW_CONS *c)
{
  struct FD_LEVELDB_CONS *db = (fd_leveldb)c;
  fd_close_leveldb(&(db->leveldb));
  if (db->leveldb.path) {
    u8_free(db->leveldb.path);
    db->leveldb.path = NULL;}
  fd_decref(db->leveldb.opts);
  if (db->leveldb.writeopts)
    leveldb_writeoptions_destroy(db->leveldb.writeopts);
  leveldb_options_destroy(db->leveldb.optionsptr);
  u8_free(c);
}

/* Primitives */

static lispval leveldb_open_prim(lispval path,lispval opts)
{
  struct FD_LEVELDB_CONS *db = u8_alloc(struct FD_LEVELDB_CONS);
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
  struct FD_LEVELDB_CONS *db = (fd_leveldb)leveldb;
  fd_close_leveldb(&(db->leveldb));
  return FD_TRUE;
}

static lispval leveldb_reopen_prim(lispval leveldb)
{
  struct FD_LEVELDB_CONS *db = (fd_leveldb)leveldb;
  struct FD_LEVELDB *reopened = fd_open_leveldb(&(db->leveldb));
  if (reopened) {
    fd_incref(leveldb);
    return leveldb;}
  else return FD_ERROR_VALUE;
}

/* Basic operations */

static lispval leveldb_get_prim(lispval leveldb,lispval key,lispval opts)
{
  struct FD_LEVELDB_CONS *db = (fd_leveldb)leveldb;
  struct FD_LEVELDB *fdldb = &(db->leveldb);
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
    if (binary_data) {
      lispval result = fd_bytes2packet(NULL,binary_size,binary_data);
      u8_free(binary_data);
      return result;}
    else if (errmsg)
      return fd_err("LevelDBError","leveldb_get_prim",errmsg,FD_VOID);
    else return FD_EMPTY_CHOICE;}
  else {
    struct FD_OUTBUF keyout = { 0 };
    FD_INIT_BYTE_OUTPUT(&keyout,1024);
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
        struct FD_INBUF valuein = { 0 };
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
  struct FD_LEVELDB_CONS *db = (fd_leveldb)leveldb;
  struct FD_LEVELDB *fdldb = &(db->leveldb);
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
    struct FD_OUTBUF keyout = { 0 };
    FD_INIT_BYTE_OUTPUT(&keyout,1024);
    struct FD_OUTBUF valout = { 0 };
    FD_INIT_BYTE_OUTPUT(&valout,1024);
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
  struct FD_LEVELDB_CONS *db = (fd_leveldb)leveldb;
  struct FD_LEVELDB *fdldb = &(db->leveldb);
  leveldb_writeoptions_t *useopts = get_write_options(fdldb,opts);
  leveldb_writeoptions_t *writeopts = (useopts)?(useopts):(fdldb->writeopts);
  if (FD_PACKETP(key)) {
    leveldb_delete(db->leveldb.dbptr,writeopts,
                   FD_PACKET_DATA(key),FD_PACKET_LENGTH(key),
                   &errmsg);
    if (useopts) leveldb_writeoptions_destroy(useopts);
    if (errmsg)
      return fd_err("LevelDBError","leveldb_put_prim",errmsg,FD_VOID);
    else return FD_VOID;}
  else {
    struct FD_OUTBUF keyout = { 0 };
    FD_INIT_BYTE_OUTPUT(&keyout,1024);
    if (fd_write_dtype(&keyout,key)<0) {
      fd_close_outbuf(&keyout);
      if (useopts) leveldb_writeoptions_destroy(useopts);
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

static int cmp_keybufs(const void *vx,const void *vy)
{
  const struct LEVELDB_KEYBUF *kbx = (struct LEVELDB_KEYBUF *) vx;
  const struct LEVELDB_KEYBUF *kby = (struct LEVELDB_KEYBUF *) vy;
  int min_len = (kbx->encoded.n_bytes < kby->encoded.n_bytes) ? 
    (kbx->encoded.n_bytes) : (kby->encoded.n_bytes);
  int cmp = memcmp(kbx->encoded.bytes,kby->encoded.bytes,min_len);
  if (cmp)
    return cmp;
  else if (kbx->encoded.n_bytes < kby->encoded.n_bytes)
    return -1;
  else if (kbx->encoded.n_bytes == kby->encoded.n_bytes)
    return 0;
  else return 1;
}

static struct LEVELDB_KEYBUF *fetchn(struct FD_LEVELDB *db,int n,
                                     struct LEVELDB_KEYBUF *keys,
                                     leveldb_readoptions_t *readopts)
{
  struct LEVELDB_KEYBUF *results = u8_alloc_n(n,struct LEVELDB_KEYBUF);
  qsort(keys,n,sizeof(struct LEVELDB_KEYBUF),cmp_keybufs);
  leveldb_iterator_t *iterator = leveldb_create_iterator(db->dbptr,readopts);
  int i = 0; while (i<n) {
    results[i].keypos = keys[i].keypos;
    results[i].dtype  = keys[i].dtype;
    leveldb_iter_seek(iterator,keys[i].encoded.bytes,keys[i].encoded.n_bytes);
    results[i].encoded.bytes =
      leveldb_iter_value(iterator,&(results[i].encoded.n_bytes));
    i++;}
  leveldb_iter_destroy(iterator);
  return results;
}

static lispval leveldb_getn_prim(lispval leveldb,lispval keys,lispval opts)
{
  struct FD_LEVELDB_CONS *dbcons = (fd_leveldb)leveldb;
  struct FD_LEVELDB *db = &(dbcons->leveldb);
  leveldb_readoptions_t *readopts = get_read_options(db,opts);
  int i = 0, n = FD_VECTOR_LENGTH(keys);
  lispval results = fd_empty_vector(n);
  struct LEVELDB_KEYBUF *keyvec = u8_alloc_n(n,struct LEVELDB_KEYBUF);
  unsigned char buf[5000];
  struct FD_OUTBUF out = { 0 };
  FD_INIT_OUTBUF(&out,buf,5000,FD_IS_WRITING);
  while (i<n) {
    lispval key = FD_VECTOR_REF(keys,i);
    keyvec[i].keypos = i;
    if (FD_PACKETP(key)) {
      keyvec[i].key   = key;
      keyvec[i].encoded.bytes   = FD_PACKET_DATA(key);
      keyvec[i].encoded.n_bytes = FD_PACKET_LENGTH(key);
      keyvec[i].dtype = 0;}
    else {
      size_t pos = (out.bufwrite-out.buffer);
      ssize_t len = fd_write_dtype(&out,key);
      keyvec[i].key = key;
      keyvec[i].encoded.n_bytes = len;
      keyvec[i].encoded.bytes = (unsigned char *) pos;
      keyvec[i].dtype = 1;}
    i++;}
  struct LEVELDB_KEYBUF *values = fetchn(db,n,keyvec,readopts);
  i=0; while (i<n) {
    int keypos = values[i].keypos;
    if (values[i].dtype) {
      struct FD_INBUF in = { 0 };
      FD_INIT_INBUF(&in,values[i].encoded.bytes,
                    values[i].encoded.n_bytes,0);
      lispval v = fd_read_dtype(&in);
      FD_VECTOR_SET(results,keypos,v);}
    else {
      lispval v = fd_init_packet(NULL,values[i].encoded.n_bytes,
                                 values[i].encoded.bytes);
      FD_VECTOR_SET(results,keypos,v);}
    u8_free(values[i].encoded.bytes);
    i++;}
  fd_close_outbuf(&out);
  if (readopts!=db->readopts)
    leveldb_readoptions_destroy(readopts);
  return results;
}

static struct BYTEVEC *write_prefix(struct BYTEVEC *prefix,lispval key)
{
  struct FD_OUTBUF keyout = { 0 };
  FD_INIT_BYTE_OUTPUT(&keyout,1000);
  ssize_t n_bytes = fd_write_dtype(&keyout,key);
  if (n_bytes < 0) {
    fd_close_outbuf(&keyout);
    return NULL;}
  else if ( n_bytes != (keyout.bufwrite-keyout.buffer) ) {
    u8_seterr("Inconsistent DTYPE size for key","leveldb_scanner",NULL);
    return NULL;}
  else {
    prefix->bytes   = keyout.buffer;
    prefix->n_bytes = n_bytes;}
  return prefix;
}

static int leveldb_scanner(struct FD_LEVELDB *db,lispval opts,
                           leveldb_iterator_t *use_iterator,
                           lispval key,struct BYTEVEC *use_prefix,
                           leveldb_prefix_iterfn iterfn,
                           void *state)
{
  leveldb_readoptions_t *readopts = get_read_options(db,opts);
  struct BYTEVEC _prefix = {0}, *prefix;
  int rv = 0, free_prefix=0;
  if (use_prefix)
    prefix=use_prefix;
  else if (FD_PACKETP(key)) {
    _prefix.bytes =   FD_PACKET_DATA(key);
    _prefix.n_bytes = FD_PACKET_LENGTH(key);
    prefix = &_prefix;}
  else {
    prefix = write_prefix(&_prefix,key);
    free_prefix=1;}
  if (prefix == NULL) return -1;
  leveldb_iterator_t *iterator = (use_iterator) ? (use_iterator) :
    (leveldb_create_iterator(db->dbptr,readopts));
  leveldb_iter_seek(iterator,prefix->bytes,prefix->n_bytes);
  while (leveldb_iter_valid(iterator)) {
    struct BYTEVEC keybuf, valbuf;
    keybuf.bytes = leveldb_iter_key(iterator,&(keybuf.n_bytes));
    if ( (keybuf.n_bytes >= prefix->n_bytes) &&
         (memcmp(keybuf.bytes,prefix->bytes,prefix->n_bytes) == 0) ) {
      valbuf.bytes = leveldb_iter_value(iterator,&(valbuf.n_bytes));
      rv = iterfn(key,prefix,&keybuf,&valbuf,state);
      /* u8_free(keybuf.bytes); u8_free(valbuf.bytes); */
      if (rv <= 0) break;}
    else {
      /* u8_free(keybuf.bytes); */
      break;}
    leveldb_iter_next(iterator);}
  if (free_prefix) u8_free(prefix->bytes);
  if (use_iterator == NULL) leveldb_iter_destroy(iterator);
  return rv;
}

#if 0
static int leveldb_key_scanner(struct FD_LEVELDB *db,lispval opts,
                               leveldb_iterator_t *use_iterator,
                               lispval key,struct BYTEVEC *use_prefix,
                               leveldb_prefix_key_iterfn iterfn,
                               void *state)
{
  leveldb_readoptions_t *readopts = get_read_options(db,opts);
  struct BYTEVEC _prefix = {0}, *prefix;
  int rv = 0, free_prefix=0;
  if (use_prefix)
    prefix=use_prefix;
  else if (FD_PACKETP(key)) {
    _prefix.bytes =   FD_PACKET_DATA(key);
    _prefix.n_bytes = FD_PACKET_LENGTH(key);
    prefix = &_prefix;}
  else {
    prefix = write_prefix(&_prefix,key);
    free_prefix=1;}
  if (prefix == NULL) return -1;
  leveldb_iterator_t *iterator = (use_iterator) ? (use_iterator) :
    (leveldb_create_iterator(db->dbptr,readopts));
  leveldb_iter_seek(iterator,prefix->bytes,prefix->n_bytes);
  while (leveldb_iter_valid(iterator)) {
    struct BYTEVEC keybuf;
    keybuf.bytes = leveldb_iter_key(iterator,&(keybuf.n_bytes));
    if ( (keybuf.n_bytes >= prefix->n_bytes) &&
         (memcmp(keybuf.bytes,prefix->bytes,prefix->n_bytes) == 0) ) {
      rv = iterfn(key,prefix,&keybuf,state);
      /* u8_free(keybuf.bytes); u8_free(valbuf.bytes); */
      if (rv <= 0) break;}
    else {
      /* u8_free(keybuf.bytes); */
      break;}
    leveldb_iter_next(iterator);}
  if (free_prefix) u8_free(prefix->bytes);
  if (use_iterator == NULL) leveldb_iter_destroy(iterator);
  return rv;
}
#endif

static int leveldb_editor(struct FD_LEVELDB *db,lispval opts,
                          leveldb_iterator_t *use_iterator,
                          leveldb_writebatch_t *batch,
                          lispval key,struct BYTEVEC *use_prefix,
                          struct BYTEVEC *value)
{
  leveldb_readoptions_t *readopts = get_read_options(db,opts);
  struct BYTEVEC _prefix = { 0 }, *prefix;
  int rv = 0, free_prefix=0;
  if (use_prefix)
    prefix=use_prefix;
  else if (FD_PACKETP(key)) {
    _prefix.bytes =   FD_PACKET_DATA(key);
    _prefix.n_bytes = FD_PACKET_LENGTH(key);
    prefix = &_prefix;}
  else {
    prefix = write_prefix(&_prefix,key);
    free_prefix=1;}
  if (prefix == NULL) return -1;
  leveldb_iterator_t *iterator = (use_iterator) ? (use_iterator) :
    (leveldb_create_iterator(db->dbptr,readopts));
  leveldb_iter_seek(iterator,prefix->bytes,prefix->n_bytes);
  while (leveldb_iter_valid(iterator)) {
    struct BYTEVEC keybuf;
    keybuf.bytes = leveldb_iter_key(iterator,&(keybuf.n_bytes));
    if ( (keybuf.n_bytes >= prefix->n_bytes) &&
         (memcmp(keybuf.bytes,prefix->bytes,prefix->n_bytes) == 0) ) {
      if (keybuf.n_bytes == prefix->n_bytes)
        leveldb_writebatch_put
          (batch,prefix->bytes,prefix->n_bytes,value->bytes,value->n_bytes);
      else leveldb_writebatch_delete(batch,keybuf.bytes,keybuf.n_bytes);}
    else {
      /* u8_free(keybuf.bytes); */
      break;}
    leveldb_iter_next(iterator);}
  if (free_prefix) u8_free(prefix->bytes);
  if (use_iterator == NULL) leveldb_iter_destroy(iterator);
  return rv;
}

/* Prefix operations */

static int prefix_get_iterfn(lispval key,
                             struct BYTEVEC *prefix,
                             struct BYTEVEC *keybuf,
                             struct BYTEVEC *valbuf,
                             void *vptr)
{
  lispval *results = (lispval *) vptr;
  if (valbuf->n_bytes == 0) return 1;
  lispval key_packet = fd_make_packet(NULL,keybuf->n_bytes,keybuf->bytes);
  lispval val_packet = fd_make_packet(NULL,valbuf->n_bytes,valbuf->bytes);
  lispval pair = fd_init_pair(NULL,key_packet,val_packet);
  FD_ADD_TO_CHOICE(*results,pair);
  return 1;
}

static lispval leveldb_prefix_get_prim(lispval leveldb,lispval key,lispval opts)
{
  struct FD_LEVELDB_CONS *dbcons = (fd_leveldb)leveldb;
  struct FD_LEVELDB *db = &(dbcons->leveldb);
  lispval results = FD_EMPTY;
  int rv = leveldb_scanner(db,opts,NULL,key,NULL,prefix_get_iterfn,&results);
  if (rv<0) {
    fd_decref(results);
    return FD_ERROR;}
  else return results;
}

static lispval leveldb_prefix_getn_prim(lispval leveldb,lispval keys,lispval opts)
{
  struct FD_LEVELDB_CONS *dbcons = (fd_leveldb)leveldb;
  struct FD_LEVELDB *db = &(dbcons->leveldb);
  leveldb_readoptions_t *readopts = get_read_options(db,opts);
  int n_keys = FD_VECTOR_LENGTH(keys);
  lispval result = fd_make_vector(n_keys,NULL);
  lispval *elts = FD_VECTOR_ELTS(result);
  struct LEVELDB_KEYBUF *prefixes = u8_alloc_n(n_keys,struct LEVELDB_KEYBUF);
  int i=0; while (i<n_keys) {
    lispval key = FD_VECTOR_REF(keys,i);
    struct FD_OUTBUF keyout = { 0 };
    FD_INIT_BYTE_OUTPUT(&keyout,512);
    fd_write_dtype(&keyout,key);
    prefixes[i].key = key;
    prefixes[i].keypos = i;
    prefixes[i].encoded.n_bytes = keyout.bufwrite-keyout.buffer;
    prefixes[i].encoded.bytes =   keyout.buffer;
    i++;}
  qsort(prefixes,n_keys,sizeof(struct LEVELDB_KEYBUF),cmp_keybufs);
  leveldb_iterator_t *iterator = leveldb_create_iterator(db->dbptr,readopts);
  i=0; while (i < n_keys) {
    lispval key = prefixes[i].key;
    int pos = prefixes[i].keypos;
    FD_VECTOR_SET(result,pos,FD_EMPTY);
    int rv = leveldb_scanner(db,opts,iterator,key,NULL,
                             prefix_get_iterfn,&(elts[pos]));
    if (rv<0) {
      int j=0; while (j < n_keys) {
        u8_free(prefixes[j].encoded.bytes); j++;}
      fd_decref(result);
      u8_free(prefixes);
      if (readopts!=db->readopts) leveldb_readoptions_destroy(readopts);
      return FD_ERROR;}
    i++;}
  if (readopts!=db->readopts) leveldb_readoptions_destroy(readopts);
  leveldb_iter_destroy(iterator);
  return result;
}

/* Index functions */

struct KEY_VALUES {
  size_t allocated, used;
  lispval *vec;};

static int accumulate_values(struct KEY_VALUES *values,int n,const lispval *add)
{
  if ( (values->used+n) >= values->allocated ) {
    if (values->allocated==0) {
      size_t new_size = 2*(1+(n/2));
      values->vec = u8_alloc_n(n,lispval);
      values->allocated = new_size;
      values->used = 0;}
    else {
      size_t need_size = values->used+n;
      size_t new_size = 2*(values->allocated);
      while (new_size < need_size) new_size=new_size*2;
      lispval *new_vec = u8_realloc(values->vec,sizeof(lispval)*new_size);
      if (new_vec) {
        values->vec = new_vec;
        values->allocated = new_size;}
      else return -1;}}
  memcpy(values->vec+(values->used),add,sizeof(lispval)*n);
  values->used += n;
  return n;
}

static int index_get_iterfn(lispval key,
                            struct BYTEVEC *prefix,
                            struct BYTEVEC *keybuf,
                            struct BYTEVEC *valbuf,
                            void *vptr)
{
  struct KEY_VALUES *values = vptr;
  if (valbuf->n_bytes == 0) return 1;
  if (valbuf->bytes[0] == LEVELDB_META_KEY) {
    return 1;}
  else {
    struct FD_INBUF in = { 0 };
    FD_INIT_BYTE_INPUT(&in,valbuf->bytes,valbuf->n_bytes);
    lispval v = fd_read_dtype(&in);
    if (FD_ABORTP(v))
      return -1;
    if (FD_EMPTY_CHOICEP(v)) return 1;
    if (FD_CHOICEP(v)) {
      int rv = accumulate_values(values,FD_CHOICE_SIZE(v),FD_CHOICE_DATA(v));
      struct FD_CHOICE *ch = (fd_choice) v;
      /* The elements are now pointed to by *values, so we just free the choice
         object */
      u8_free(ch);
      return rv;}
    else return accumulate_values(values,1,&v);}
}

static lispval leveldb_index_get_prim(lispval leveldb,lispval key,lispval opts)
{
  struct FD_LEVELDB_CONS *dbcons = (fd_leveldb)leveldb;
  struct FD_LEVELDB *db = &(dbcons->leveldb);
  struct KEY_VALUES accumulator = {0};
  int rv = leveldb_scanner(db,opts,NULL,key,NULL,index_get_iterfn,&accumulator);
  if (rv<0) {
    fd_decref_vec(accumulator.vec,accumulator.used);
    u8_free(accumulator.vec);
    return FD_ERROR_VALUE;}
  else return fd_init_choice(NULL,accumulator.used,accumulator.vec,-1);
}

static ssize_t leveldb_add_helper(struct FD_LEVELDB *db,
                                  struct FD_SLOTCODER *slotcodes,
                                  struct FD_OIDCODER *oidcodes,
                                  leveldb_writebatch_t *batch,
                                  lispval key,lispval values,
                                  lispval opts)
{
  struct FD_OUTBUF keyout= { 0 }, valout = { 0 }, countbuf = { 0 };
  struct BYTEVEC keybuf = {0}, valbuf = {0};
  leveldb_readoptions_t *readopts = get_read_options(db,opts);
  char *errmsg=NULL;
  long long n_vals=0, n_blocks=0, header_type = 0;
  ssize_t rv = 0;
  FD_INIT_BYTE_OUTPUT(&keyout,1000);
  if (slotcodes)
    rv=leveldb_encode_key(&keyout,key,slotcodes,1);
  else rv=fd_write_dtype(&keyout,key);
  if (rv<0) return rv;
  FD_INIT_BYTE_OUTPUT(&valout,1000);
  keybuf.n_bytes = keyout.bufwrite-keyout.buffer;
  keybuf.bytes   = keyout.buffer;
  valbuf.bytes   = leveldb_get
    (db->dbptr,readopts,keybuf.bytes,keybuf.n_bytes,&valbuf.n_bytes,&errmsg);
  if (readopts != db->readopts) leveldb_readoptions_destroy(readopts);
  if (errmsg) {
    fd_seterr("LevelDBError","leveldb_add_helper",errmsg,FD_VOID);
    fd_close_outbuf(&keyout);
    fd_close_outbuf(&valout);
    return -1;}
  if (valbuf.bytes == NULL) {
    if (oidcodes)
      rv=leveldb_encode_value(&valout,values,oidcodes);
    else rv=fd_write_dtype(&valout,values);
    if (rv >= 0)
      leveldb_writebatch_put
        (batch,keybuf.bytes,keybuf.n_bytes,
         valout.buffer,valout.bufwrite-valout.buffer);
    fd_close_outbuf(&valout);
    fd_close_outbuf(&keyout);
    if (rv<0) return rv;
    else return FD_CHOICE_SIZE(values);}
  struct FD_INBUF curbuf = { 0 };
  FD_INIT_INBUF(&curbuf,valbuf.bytes,valbuf.n_bytes,FD_STATIC_BUFFER);
  if (valbuf.bytes[0] == LEVELDB_KEYINFO) {
    fd_read_byte(&curbuf); /* Skip LEVELDB_KEYINFO code */
    header_type = fd_read_4bytes(&curbuf);
    n_blocks = fd_read_8bytes(&curbuf);
    n_vals   = fd_read_8bytes(&curbuf);
    if ( (header_type<0) || (n_blocks<0) || (n_vals<0) )
      rv = -1;
    else {
      if (FD_CHOICEP(values))
        n_vals += FD_CHOICE_SIZE(values);
      else n_vals++;
      n_blocks++;
      if (oidcodes)
        rv=leveldb_encode_value(&valout,values,oidcodes);
      else rv=fd_write_dtype(&valout,values);}
    if (rv<0) n_vals = -1;}
  else {
    lispval combined = leveldb_decode_value(&curbuf,oidcodes);
    if (FD_ABORTP(combined)) rv=-1;
    else {
      fd_incref(values);
      FD_ADD_TO_CHOICE(combined,values);
      combined = fd_simplify_choice(combined);
      n_vals = FD_CHOICE_SIZE(combined);
      n_blocks = 1;
      if (oidcodes)
        rv=leveldb_encode_value(&valout,combined,oidcodes);
      else rv=fd_write_dtype(&valout,combined);
      fd_decref(combined);}}
  if (rv>=0) {
    unsigned char count_bytes[64];
    FD_INIT_OUTBUF(&countbuf,count_bytes,64,0);
    if (rv>=0) rv = fd_write_byte(&countbuf,LEVELDB_KEYINFO);
    if (rv>=0) rv = fd_write_4bytes((&countbuf),header_type);
    if (rv>=0) rv = fd_write_8bytes((&countbuf),n_blocks);
    if (rv>=0) rv = fd_write_8bytes((&countbuf),n_vals);
    if (rv>=0) {
      leveldb_writebatch_put
        (batch,keybuf.bytes,keybuf.n_bytes,
         countbuf.buffer,countbuf.bufwrite-countbuf.buffer);
      fd_write_8bytes(&keyout,n_blocks);
      leveldb_writebatch_put
        (batch,keyout.buffer,keyout.bufwrite-keyout.buffer,
         valout.buffer,valout.bufwrite-valout.buffer);}
    fd_close_outbuf(&countbuf);}
  fd_close_inbuf(&curbuf);
  fd_close_outbuf(&keyout);
  fd_close_outbuf(&valout);
  u8_free(valbuf.bytes);
  if (rv<0) return -1;
  else return n_vals;
}

static ssize_t leveldb_adder(struct FD_LEVELDB *db,lispval key,
                             lispval values,lispval opts)
{
  leveldb_writebatch_t *batch = leveldb_writebatch_create();
  ssize_t added = leveldb_add_helper(db,NULL,NULL,batch,key,values,opts);
  if (added<0) return added;
  char *errmsg = NULL;
  leveldb_write(db->dbptr,sync_writeopts,batch,&errmsg);
  if (errmsg)
    fd_seterr("LevelDBError","leveldb_add_helper",errmsg,FD_VOID);
  leveldb_writebatch_destroy(batch);
  if (errmsg)
    return -1;
  else return added;
}


static lispval leveldb_index_add_prim(lispval leveldb,lispval key,
                                      lispval values,lispval opts)
{
  struct FD_LEVELDB_CONS *dbcons = (fd_leveldb)leveldb;
  struct FD_LEVELDB *db = &(dbcons->leveldb);
  ssize_t rv = leveldb_adder(db,key,values,opts);
  if (rv<0)
    return FD_ERROR_VALUE;
  else return FD_INT(rv);
}

/* Accessing various metadata fields */

static lispval get_prop(leveldb_t *dbptr,char *key,lispval dflt)
{
  ssize_t data_size; char *errmsg = NULL;
  unsigned char *buf = leveldb_get
    (dbptr,default_readopts,key,strlen(key),&data_size,&errmsg);
  if (buf) {
    lispval result = FD_VOID;
    struct FD_INBUF in = { 0 };
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
  struct FD_OUTBUF out = { 0 };
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
fd_pool fd_open_leveldb_pool(u8_string path,fd_storage_flags flags,lispval opts)
{
  struct FD_LEVELDB_POOL *pool = u8_alloc(struct FD_LEVELDB_POOL);
  if (fd_setup_leveldb(&(pool->leveldb),path,opts)) {
    leveldb_t *dbptr = pool->leveldb.dbptr;
    lispval base = get_prop(dbptr,"\377BASE",FD_VOID);
    lispval cap = get_prop(dbptr,"\377CAPACITY",FD_VOID);
    lispval load = get_prop(dbptr,"\377LOAD",FD_VOID);
    lispval metadata = get_prop(dbptr,"\377METADATA",FD_VOID);
    lispval slotcodes = get_prop(dbptr,"\377SLOTIDS",FD_VOID);
    if ((FD_OIDP(base)) && (FD_UINTP(cap)) && (FD_UINTP(load))) {
      u8_string abspath = u8_abspath(path,NULL);
      u8_string realpath = u8_realpath(path,NULL);
      lispval adjunct_opt = fd_getopt(opts,SYM("ADJUNCT"),FD_VOID);
      lispval read_only_opt = fd_getopt(opts,SYM("READONLY"),FD_VOID);
      lispval label = get_prop(dbptr,"\377LABEL",FD_VOID);
      fd_init_pool((fd_pool)pool,
                   FD_OID_ADDR(base),FD_FIX2INT(cap),
                   &leveldb_pool_handler,
                   path,abspath,realpath,
                   FD_STORAGE_ISPOOL,
                   metadata,
                   opts);
      u8_free(abspath);
      u8_free(realpath);
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
      if (FD_VECTORP(slotcodes))
        fd_init_slotcoder(&(pool->slotcodes),
                          FD_VECTOR_LENGTH(slotcodes),
                          FD_VECTOR_ELTS(slotcodes));
      else fd_init_slotcoder(&(pool->slotcodes),0,NULL);
      fd_decref(adjunct_opt); fd_decref(read_only_opt);
      fd_decref(metadata); fd_decref(label);
      fd_decref(base); fd_decref(cap); fd_decref(load);
      fd_decref(slotcodes);
      fd_register_pool((fd_pool)pool);
      return (fd_pool)pool;}
    else  {
      fd_decref(base); fd_decref(cap); fd_decref(load);
      fd_decref(slotcodes); fd_decref(metadata);
      fd_close_leveldb(&(pool->leveldb));
      u8_free(pool);
      fd_seterr("NotAPoolDB","fd_leveldb_pool",NULL,FD_VOID);
      return (fd_pool)NULL;}}
  else return NULL;
}

FD_EXPORT
fd_pool fd_make_leveldb_pool(u8_string path,
                             lispval base,
                             lispval cap,
                             lispval opts)
{
  lispval load = fd_getopt(opts,SYM("LOAD"),FD_FIXZERO);

  if ((!(FD_OIDP(base)))||(!(FD_UINTP(cap)))||(!(FD_UINTP(load)))) {
    fd_seterr("Not enough information to create a pool",
              "fd_make_leveldb_pool",path,opts);
    return (fd_pool)NULL;}

  struct FD_LEVELDB_POOL *pool = u8_alloc(struct FD_LEVELDB_POOL);

  if (fd_setup_leveldb(&(pool->leveldb),path,opts)) {
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
    lispval slotids = fd_getopt(opts,fd_intern("SLOTIDS"),FD_VOID);

    u8_string realpath = u8_realpath(path,NULL);
    u8_string abspath = u8_abspath(path,NULL);
    time_t now=time(NULL), ctime, mtime;

    if (!((FD_VOIDP(given_base))||(FD_EQUALP(base,given_base)))) {
      u8_free(pool); u8_free(abspath); u8_free(realpath);
      fd_seterr("Conflicting base OIDs",
                "fd_make_leveldb_pool",path,opts);
      return NULL;}
    if (!((FD_VOIDP(given_cap))||(FD_EQUALP(cap,given_cap)))) {
      u8_free(pool); u8_free(abspath); u8_free(realpath);
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

    if (FD_SLOTMAPP(metadata)) {
      if ( (FD_VOIDP(cur_metadata)) || (fd_testopt(opts,SYM("FORCE"),FD_VOID)) )
        set_prop(dbptr,"\377METADATA",metadata,sync_writeopts);
      else u8_log(LOG_WARN,"ExistingMetadata",
                  "Not overwriting existing metatdata in leveldb pool %s:",
                  path,cur_metadata);}
    fd_init_pool((fd_pool)pool,
                 FD_OID_ADDR(base),FD_FIX2INT(cap),
                 &leveldb_pool_handler,
                 path,abspath,realpath,
                 FD_STORAGE_ISPOOL,
                 metadata,
                 opts);

    if (FD_VECTORP(slotids)) {
      set_prop(dbptr,"\377SLOTIDS",slotids,sync_writeopts);
      fd_init_slotcoder(&(pool->slotcodes),
                        FD_VECTOR_LENGTH(slotids),
                        FD_VECTOR_ELTS(slotids));}
    else if (FD_UINTP(slotids)) {
      lispval tmp = fd_fill_vector(FD_FIX2INT(slotids),FD_FALSE);
      set_prop(dbptr,"\377SLOTIDS",tmp,sync_writeopts);
      fd_init_slotcoder(&(pool->slotcodes),
                        FD_VECTOR_LENGTH(tmp),
                        FD_VECTOR_ELTS(tmp));
      fd_decref(tmp);}
    else {
      fd_init_slotcoder(&(pool->slotcodes),0,NULL);
      if (! ( (FD_VOIDP(slotids)) || (FD_FALSEP(slotids)) ) )
        u8_log(LOG_WARN,"BadSlotIDs","%q",slotids);}

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
    else if (FD_PRIM_TYPEP(mtime_val,fd_timestamp_type)) {
      struct FD_TIMESTAMP *moment = (fd_timestamp) mtime_val;
      mtime = moment->u8xtimeval.u8_tick;}
    else mtime=now;
    fd_decref(mtime_val);
    mtime_val = fd_time2timestamp(mtime);
    set_prop(dbptr,"\377MTIME",mtime_val,sync_writeopts);

    if (FD_FIXNUMP(generation_val))
      set_prop(dbptr,"\377GENERATION",generation_val,sync_writeopts);
    else set_prop(dbptr,"\377GENERATION",FD_INT(0),sync_writeopts);

    pool->pool_flags = fd_get_dbflags(opts,FD_STORAGE_ISPOOL);
    pool->pool_flags &= ~FD_STORAGE_READ_ONLY;
    pool->pool_load = FD_FIX2INT(load);

    if (FD_STRINGP(label)) {
      pool->pool_label = u8_strdup(FD_CSTRING(label));}

    fd_decref(slotids);
    fd_decref(metadata);
    fd_decref(generation_val);
    fd_decref(ctime_val);
    fd_decref(mtime_val);
    u8_free(realpath);
    u8_free(abspath);

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
  return fd_decode_slotmap(in,&(p->slotcodes));
}

static ssize_t write_oid_value(fd_leveldb_pool p,
                               struct FD_OUTBUF *out,
                               lispval value)
{
  if (FD_SLOTMAPP(value))
    return fd_encode_slotmap(out,value,&(p->slotcodes));
  else return fd_write_dtype(out,value);
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
    struct FD_INBUF in = { 0 };
    FD_INIT_BYTE_INPUT(&in,buf,data_size);
    result = read_oid_value(ldp,&in);
    u8_free(buf);
    return result;}
  else if (errmsg)
    return fd_err("LevelDBerror","get_prop",NULL,FD_VOID);
  else return FD_VOID;
}

static int queue_oid_value(fd_leveldb_pool ldp,
                           unsigned int offset,
                           lispval value,
                           leveldb_writebatch_t *batch)
{
  struct FD_OUTBUF out = { 0 };
  ssize_t dtype_len;
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
  lispval *values = u8_big_alloc_n(n,lispval);
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
      struct FD_INBUF in = { 0 };
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
  char *errmsg = NULL;
  leveldb_writebatch_t *batch = leveldb_writebatch_create();
  ssize_t n_bytes = 0;
  while (i<n) {
    lispval oid = oids[i], value = values[i];
    FD_OID addr = FD_OID_ADDR(oid);
    unsigned int offset = FD_OID_DIFFERENCE(addr,pool->pool_base);
    if (offset >= pool->pool_load) {
      if ( (pool->pool_flags) & FD_POOL_ADJUNCT )
        pool->pool_load = offset+1;
      else {
        fd_seterr("Saving unallocated OID","leveldb_pool_storen",
                  p->poolid,oid);
        errval=-1; break;}}
    ssize_t len = queue_oid_value(pool,offset,value,batch);
    if (len<0) {errval = len; break;}
    else {n_bytes+=len; i++;}}
  if (errval>=0) {
    fd_lock_pool_struct(p,1); {
      struct FD_OUTBUF dtout = { 0 };
      unsigned char intbuf[5];
      int load = pool->pool_load;
      intbuf[0]= dt_fixnum;
      intbuf[1]= (load>>24)&0xFF;
      intbuf[2]= (load>>16)&0xFF;
      intbuf[3]= (load>>8)&0xFF;
      intbuf[4]= load&0xFF;
      leveldb_writebatch_put(batch,"\377LOAD",strlen("\377LOAD"),intbuf,5);
      if (pool->slotcodes.n_slotcodes != pool->slotcodes.init_n_slotcodes) {
        FD_INIT_BYTE_OUTPUT( &dtout, ((pool->slotcodes.n_slotcodes) * 16) );
        ssize_t len = fd_write_dtype(&dtout, (lispval) (pool->slotcodes.slotids) );
        if (len>0)
          leveldb_writebatch_put(batch,"\377SLOTIDS",strlen("\377SLOTIDS"),
                                 dtout.buffer,len);
        else {
          fd_seterr("LevelDB/FailedSaveSlotids","leveldb_pool_storen",
                    p->poolid,FD_VOID);
          errval=-1;}
        fd_close_outbuf( &dtout );}}
    fd_unlock_pool_struct(p);
    if (errval>=0) {
      leveldb_write(dbptr,sync_writeopts,batch,&errmsg);
      if (errmsg) {
        u8_seterr("LevelDBError","leveldb_pool_storen",errmsg);
        errval = -1;}}
    leveldb_writebatch_destroy(batch);}
  if (errval<0)
    return errval;
  else return n;
}

static int leveldb_pool_commit(fd_pool p,fd_commit_phase phase,
                               struct FD_POOL_COMMITS *commits)
{
  switch (phase) {
  case fd_commit_start:
    return 0;
  case fd_commit_write:
    return leveldb_pool_storen(p,commits->commit_count,
                               commits->commit_oids,
                               commits->commit_vals);
  case fd_commit_sync:
    return 0;
  case fd_commit_rollback:
    return 0;
  case fd_commit_cleanup:
    return 0;
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
    fd_seterr(_("FileAlreadyExists"),"leveldb_pool_create",spec,VOID);
    return NULL;}
  else if (!(OIDP(base_oid))) {
    fd_seterr("Not a base oid","leveldb_pool_create",spec,base_oid);
    rv = -1;}
  else if (FD_ISINT(capacity_arg)) {
    int capval = fd_getint(capacity_arg);
    if (capval<=0) {
      fd_seterr("Not a valid capacity","leveldb_pool_create",
                spec,capacity_arg);
      rv = -1;}
    else capacity = capval;}
  else {
    fd_seterr("Not a valid capacity","leveldb_pool_create",
              spec,capacity_arg);
    rv = -1;}
  if (rv<0) {}
  else if (FD_ISINT(load_arg)) {
    int loadval = fd_getint(load_arg);
    if (loadval<0) {
      fd_seterr("Not a valid load","leveldb_pool_create",spec,load_arg);
      rv = -1;}
    else if (loadval > capacity) {
      fd_seterr(fd_PoolOverflow,"leveldb_pool_create",spec,load_arg);
      rv = -1;}
    else NO_ELSE;}
  else if ( (FALSEP(load_arg)) || (EMPTYP(load_arg)) ||
            (VOIDP(load_arg)) || (load_arg == FD_DEFAULT_VALUE)) {}
  else {
    fd_seterr("Not a valid load","leveldb_pool_create",spec,load_arg);
    rv = -1;}

  if (rv>=0)
    dbpool = fd_make_leveldb_pool(spec,base_oid,capacity_arg,opts);

  if (dbpool == NULL) rv=-1;

  fd_decref(base_oid);
  fd_decref(capacity_arg);
  fd_decref(load_arg);
  fd_decref(metadata);

  if (rv>=0) {
    fd_set_file_opts(spec,opts);
    return fd_open_pool(spec,storage_flags,opts);}
  else return NULL;
}

static fd_pool leveldb_pool_open(u8_string spec,fd_storage_flags flags,lispval opts)
{
  return fd_open_leveldb_pool(spec,flags,opts);
}

static void recycle_leveldb_pool(fd_pool p)
{
  struct FD_LEVELDB_POOL *db = (fd_leveldb_pool) p;
  fd_close_leveldb(&(db->leveldb));
  if (db->leveldb.path) {
    u8_free(db->leveldb.path);
    db->leveldb.path = NULL;}
  fd_decref(db->leveldb.opts);
  if (db->leveldb.writeopts)
    leveldb_writeoptions_destroy(db->leveldb.writeopts);
  leveldb_options_destroy(db->leveldb.optionsptr);

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
  recycle_leveldb_pool, /* recycle */
  NULL  /* poolctl */
};

/* LevelDB indexes */

/* Indexes use leveldb in a particular way, based on prefixes to the
   stored keys and values.

*/

static struct FD_INDEX_HANDLER leveldb_index_handler;

fd_index fd_open_leveldb_index(u8_string path,fd_storage_flags flags,lispval opts)
{
  struct FD_LEVELDB_INDEX *index = u8_alloc(struct FD_LEVELDB_INDEX);
  if (fd_setup_leveldb(&(index->leveldb),path,opts)) {
    leveldb_t *dbptr = index->leveldb.dbptr;
    u8_string abspath = u8_abspath(path,NULL);
    u8_string realpath = u8_realpath(path,NULL);
    lispval label = get_prop(dbptr,"\377LABEL",FD_VOID);
    lispval metadata = get_prop(dbptr,"\377METADATA",FD_VOID);
    lispval slotcodes = get_prop(dbptr,"\377SLOTIDS",FD_VOID);
    lispval oidcodes = get_prop(dbptr,"\377BASEOIDS",FD_VOID);

    fd_init_index((fd_index)index,
                  &leveldb_index_handler,
                  (FD_STRINGP(label)) ? (FD_CSTRING(label)) : (path),
                  abspath,realpath,
                  0,metadata,opts);
    u8_free(abspath); u8_free(realpath);

    if (FD_VOIDP(metadata)) {}
    else if (FD_SLOTMAPP(metadata)) {}
    else u8_log(LOG_WARN,"LevelDB/Index/BadMetadata",
                "Bad metadata for level db index %s: %q",path,metadata);

    if (FD_FALSEP(slotcodes))
      fd_init_slotcoder(&(index->slotcodes),0,NULL);
    else if (FD_VECTORP(slotcodes))
      fd_init_slotcoder(&(index->slotcodes),
                        FD_VECTOR_LENGTH(slotcodes),
                        FD_VECTOR_ELTS(slotcodes));
    else if (FD_UINTP(slotcodes)) {
      lispval tmp = fd_fill_vector(FD_FIX2INT(slotcodes),FD_FALSE);
      fd_init_slotcoder(&(index->slotcodes),
                        FD_VECTOR_LENGTH(tmp),
                        FD_VECTOR_ELTS(tmp));
      fd_decref(tmp);}
    else u8_log(LOG_WARN,"LevelDB/Index/BadSlotCodes",
                "Bad slotcodes for level db index %s: %q",path,slotcodes);

    if (FD_FALSEP(oidcodes))
      fd_init_oidcoder(&(index->oidcodes),0,NULL);
    else if (FD_VECTORP(oidcodes))
      fd_init_oidcoder(&(index->oidcodes),
                        FD_VECTOR_LENGTH(oidcodes),
                        FD_VECTOR_ELTS(oidcodes));
    else if (FD_UINTP(oidcodes)) {
      lispval tmp = fd_fill_vector(FD_FIX2INT(oidcodes),FD_FALSE);
      fd_init_oidcoder(&(index->oidcodes),
                        FD_VECTOR_LENGTH(tmp),
                        FD_VECTOR_ELTS(tmp));
      fd_decref(tmp);}
    else u8_log(LOG_WARN,"LevelDB/Index/BadOidcodes",
                "Bad oidcodes for level db index %s: %q",path,oidcodes);

    fd_decref(label);
    fd_decref(metadata);
    fd_decref(slotcodes);
    fd_decref(oidcodes);
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
  lispval metadata = fd_getopt(opts,SYM("METADATA"),FD_VOID);
  lispval slotids = fd_getopt(opts,SYM("SLOTIDS"),FD_VOID);
  lispval baseoids = fd_getopt(opts,SYM("BASEOIDS"),FD_VOID);

  lispval keyslot = fd_getopt(opts,FDSYM_KEYSLOT,FD_VOID);

  if ( (FD_VOIDP(keyslot)) || (FD_FALSEP(keyslot)) ) {}
  else if ( (FD_SYMBOLP(keyslot)) || (FD_OIDP(keyslot)) ) {
    if (FD_SLOTMAPP(metadata))
      fd_store(metadata,FDSYM_KEYSLOT,keyslot);
    else {
      metadata = fd_empty_slotmap();
      fd_store(metadata,FDSYM_KEYSLOT,keyslot);}}
  else u8_log(LOG_WARN,"InvalidKeySlot",
              "Not initializing keyslot of %s to %q",path,keyslot);

  if (fd_setup_leveldb(&(index->leveldb),path,opts)) {
    leveldb_t *dbptr = index->leveldb.dbptr;
    lispval cur_label = get_prop(dbptr,"\377LABEL",FD_VOID);
    u8_string abspath = u8_abspath(path,NULL);
    u8_string realpath = u8_realpath(path,NULL);
    if (!(FD_VOIDP(label))) {
      if (!(FD_EQUALP(label,cur_label)))
        set_prop(dbptr,"\377LABEL",label,sync_writeopts);}
    if (FD_VOIDP(metadata)) {}
    else if (FD_SLOTMAPP(metadata))
      set_prop(dbptr,"\377METADATA",metadata,sync_writeopts);
    else u8_log(LOG_WARN,"LevelDB/Index/InvalidMetadata",
                "For %s: %q",path,metadata);

    if (FD_FALSEP(slotids)) {
      set_prop(dbptr,"\377SLOTIDS",FD_FALSE,sync_writeopts);
      fd_init_slotcoder(&(index->slotcodes),0,NULL);}
    else if (FD_VOIDP(slotids)) {
      lispval tmp = fd_fill_vector(16,FD_FALSE);
      set_prop(dbptr,"\377SLOTIDS",tmp,sync_writeopts);
      fd_init_slotcoder(&(index->slotcodes),
                        FD_VECTOR_LENGTH(tmp),
                        FD_VECTOR_ELTS(tmp));
      fd_decref(tmp);}
    else if (FD_VECTORP(slotids)) {
      set_prop(dbptr,"\377SLOTIDS",slotids,sync_writeopts);
      fd_init_slotcoder(&(index->slotcodes),
                        FD_VECTOR_LENGTH(slotids),
                        FD_VECTOR_ELTS(slotids));}
    else if (FD_UINTP(slotids)) {
      lispval tmp = fd_fill_vector(FD_FIX2INT(slotids),FD_FALSE);
      set_prop(dbptr,"\377SLOTIDS",tmp,sync_writeopts);
      fd_init_slotcoder(&(index->slotcodes),
                        FD_VECTOR_LENGTH(tmp),
                        FD_VECTOR_ELTS(tmp));
      fd_decref(tmp);}
    else {
      set_prop(dbptr,"\377SLOTIDS",FD_FALSE,sync_writeopts);
      fd_init_slotcoder(&(index->slotcodes),0,NULL);
      u8_log(LOG_WARN,"LevelDB/Index/InvalidSlotids",
             "For %s: %q",path,slotids);}

    if (FD_FALSEP(baseoids)) {
      fd_init_oidcoder(&(index->oidcodes),0,NULL);
      set_prop(dbptr,"\377BASEOIDS",FD_FALSE,sync_writeopts);}
    else if (FD_VOIDP(baseoids)) {
      lispval tmp = fd_fill_vector(16,FD_FALSE);
      set_prop(dbptr,"\377BASEOIDS",tmp,sync_writeopts);
      fd_init_oidcoder(&(index->oidcodes),
                       FD_VECTOR_LENGTH(tmp),
                       FD_VECTOR_ELTS(tmp));
      fd_decref(tmp);}
    else if (FD_VECTORP(baseoids)) {
      set_prop(dbptr,"\377BASEOIDS",baseoids,sync_writeopts);
      fd_init_oidcoder(&(index->oidcodes),
                       FD_VECTOR_LENGTH(baseoids),
                       FD_VECTOR_ELTS(baseoids));}
    else if (FD_UINTP(baseoids)) {
      lispval tmp = fd_fill_vector(FD_FIX2INT(baseoids),FD_FALSE);
      set_prop(dbptr,"\377BASEOIDS",tmp,sync_writeopts);
      fd_init_oidcoder(&(index->oidcodes),
                       FD_VECTOR_LENGTH(tmp),
                       FD_VECTOR_ELTS(tmp));
      fd_decref(tmp);}
    else {
      fd_init_oidcoder(&(index->oidcodes),0,NULL);
      set_prop(dbptr,"\377BASEOIDS",FD_FALSE,sync_writeopts);
      u8_log(LOG_WARN,"LevelDB/Index/InvalidBaseOIDs",
             "For %s: %q",path,baseoids);}

    fd_init_index((fd_index)index,
                  &leveldb_index_handler,
                  (FD_STRINGP(label)) ? (FD_CSTRING(label)) : (abspath),
                  abspath,realpath,
                  0,metadata,opts);
    u8_free(abspath); u8_free(realpath);

    index->index_flags = fd_get_dbflags(opts,FD_STORAGE_ISINDEX);
    index->index_flags &= ~FD_STORAGE_READ_ONLY;

    fd_decref(metadata);
    fd_decref(baseoids);
    fd_decref(slotids);

    fd_register_index((fd_index)index);

    return (fd_index)index;}
  else {
    u8_free(index);
    return (fd_index) NULL;}
}

static ssize_t leveldb_encode_key(struct FD_OUTBUF *out,lispval key,
                                  struct FD_SLOTCODER *slotcodes,
                                  int addcode)
{
  ssize_t key_len = 0;

  if ( (slotcodes) && (slotcodes->slotids) &&
       (FD_PAIRP(key)) &&
       ( (FD_OIDP(FD_CAR(key))) || (FD_SYMBOLP(FD_CAR(key))) ) ) {
    lispval slotid = FD_CAR(key);

    int code = fd_slotid2code(slotcodes,slotid);
    if (code<0) {
      if (addcode)
        code = fd_add_slotcode(slotcodes,slotid);
      else return 0;}

    if (code<0)
      return fd_write_dtype(out,key);
    else {
      DT_BUILD(key_len,fd_write_byte(out,LEVELDB_CODED_KEY));
      DT_BUILD(key_len,fd_write_zint(out,code));
      DT_BUILD(key_len,fd_write_dtype(out,FD_CDR(key)));}
    return key_len;}
  else return fd_write_dtype(out,key);
}

static lispval leveldb_decode_key(struct FD_INBUF *in,
                                  struct FD_SLOTCODER *slotcodes)
{
  if (fd_probe_byte(in) == LEVELDB_CODED_KEY) {
    if ( (slotcodes) && (slotcodes->slotids) ) {
      fd_read_byte(in); /* skip LEVELDB_CODED_KEY */
      int code = fd_read_zint(in);
      if (code < 0) {
        u8_seterr("BadEncodedKey","leveldb_decode_key",NULL);
        return FD_ERROR;}
      lispval slotid = fd_code2slotid(slotcodes,code);
      if (FD_ABORTP(slotid)) {
        u8_seterr("BadSlotCode","leveldb_decode_key",
                  u8_mkstring("%d",code));
        return FD_ERROR;}
      else {
        lispval value = fd_read_dtype(in);
        if (FD_ABORTP(value))
          return value;
        else return fd_init_pair(NULL,slotid,value);}}
    else {
      u8_seterr("NoSlotCodes","leveldb_decode_key",NULL);
      return FD_ERROR;}}
  else return fd_read_dtype(in);
}

static lispval leveldb_decode_value(struct FD_INBUF *in,
                                    struct FD_OIDCODER *oidcodes)
{
  if (fd_probe_byte(in) == LEVELDB_CODED_VALUES) {
    if ( (oidcodes) && (oidcodes->n_oids) ) {
      fd_read_byte(in);
      size_t n_values = fd_read_zint(in);
      lispval results = FD_EMPTY_CHOICE;
      int err=0, i = 0; while (i<n_values) {
        int code = fd_read_zint(in);
        if (code<0) { err=1; break;}
        if (code == 0) {
          lispval elt = fd_read_dtype(in);
          if (FD_ABORTP(elt)) { err=1; break; }
          else {FD_ADD_TO_CHOICE(results,elt);}}
        else {
          lispval baseoid = fd_get_baseoid(oidcodes,code-1);
          if (FD_ABORTP(baseoid)) { err=1; break;}
          int baseid = FD_OID_BASE_ID(baseoid);
          int off = fd_read_zint(in);
          if (off < 0) { err=1; break;}
          lispval elt = FD_CONSTRUCT_OID(baseid,off);
          FD_ADD_TO_CHOICE(results,elt);}
        i++;}
      if (err) {
        fd_seterr("BadOIDCode","leveldb_decode_value",NULL,FD_VOID);
        fd_decref(results);
        return FD_ERROR_VALUE;}
      else return fd_simplify_choice(results);}
    else {
      u8_seterr("InvalidCodedValue","leveldb_decode_value",NULL);
      return FD_ERROR;}}
  else return fd_read_dtype(in);
}

static ssize_t leveldb_encode_value(struct FD_OUTBUF *out,lispval val,
                                    struct FD_OIDCODER *oc)
{
  if ( (oc) && (oc->oids_len) ) {
    int all_oids = 1; size_t n_vals = 0;
    FD_DO_CHOICES(elt,val) {
      if (!(FD_OIDP(elt))) {
        all_oids=0; FD_STOP_DO_CHOICES; break;}
      else n_vals++;}
    if (all_oids) {
      ssize_t dtype_len = 0;
      DT_BUILD(dtype_len,fd_write_byte(out,LEVELDB_CODED_VALUES));
      DT_BUILD(dtype_len,fd_write_zint(out,n_vals));
      FD_DO_CHOICES(elt,val) {
        int base = FD_OID_BASE_ID(elt);
        int oidcode = fd_get_oidcode(oc,base);
        lispval baseoid = FD_CONSTRUCT_OID(base,0);
        if (oidcode<0)
          oidcode = fd_add_oidcode(oc,baseoid);
        if (oidcode<0) {
          DT_BUILD(dtype_len,fd_write_byte(out,0));
          DT_BUILD(dtype_len,fd_write_dtype(out,elt));}
        else {
          int offset = FD_OID_BASE_OFFSET(elt);
          DT_BUILD(dtype_len,fd_write_zint(out,oidcode+1));
          DT_BUILD(dtype_len,fd_write_zint(out,offset));}}
      return dtype_len;}
    else return fd_write_dtype(out,val);}
  else return fd_write_dtype(out,val);
}

/* index driver methods */

struct LEVELDB_INDEX_RESULTS {
  lispval results;
  fd_index index;
  struct FD_OIDCODER *oidcodes;};
struct LEVELDB_INDEX_COUNT {
  ssize_t count;
  fd_index index;
  struct FD_OIDCODER *oidcodes;};

static int leveldb_index_gather(lispval key,
                                struct BYTEVEC *prefix,
                                struct BYTEVEC *keybuf,
                                struct BYTEVEC *valbuf,
                                void *vptr)
{
  struct LEVELDB_INDEX_RESULTS *state = vptr;
  if (valbuf->n_bytes == 0)
    return 1;
  else if (valbuf->bytes[0] == LEVELDB_KEYINFO)
    return 1;
  else {
    struct FD_INBUF valstream = { 0 };
    FD_INIT_BYTE_INPUT(&valstream,valbuf->bytes,valbuf->n_bytes);
    lispval value = leveldb_decode_value(&valstream,state->oidcodes);
    if (FD_ABORTP(value))
      return -1;
    else {
      FD_ADD_TO_CHOICE((state->results),value);
      return 1;}}
}

static int leveldb_index_count(lispval key,
                               struct BYTEVEC *prefix,
                               struct BYTEVEC *keybuf,
                               struct BYTEVEC *valbuf,
                               void *vptr)
{
  struct LEVELDB_INDEX_COUNT *state = vptr;
  if (valbuf->n_bytes == 0) return 1;
  struct FD_INBUF valstream = { 0 };
  FD_INIT_BYTE_INPUT(&valstream,valbuf->bytes,valbuf->n_bytes);
  if ( (keybuf->n_bytes == prefix->n_bytes) &&
       (valbuf->bytes[0] == 0xFF) ) {
    long long header_type = fd_read_4bytes(&valstream);
    if (header_type != 1) {
      fd_seterr("BadLeveDBIndexHeader","leveldb_index_count",
                state->index->indexid,key);
      return -1;}
    U8_MAYBE_UNUSED ssize_t n_blocks = fd_read_8bytes(&valstream);
    ssize_t n_keys = fd_read_8bytes(&valstream);
    if (n_keys>=0) {
      state->count=n_keys;
      return 0;}
    else {
      fd_seterr("BadLeveDBIndexHeader","leveldb_index_count",
                state->index->indexid,key);
      return -1;}}
  else {
    lispval values = leveldb_decode_value(&valstream,state->oidcodes);
    if (FD_ABORTP(values)) return -1;
    else state->count += FD_CHOICE_SIZE(values);
    fd_decref(values);
    return 1;}
}

static lispval leveldb_index_fetch(fd_index ix,lispval key)
{
  struct FD_LEVELDB_INDEX *lx = (fd_leveldb_index)ix;
  struct FD_LEVELDB *db = &(lx->leveldb);
  FD_DECL_OUTBUF(keybuf,1000);
  ssize_t len = leveldb_encode_key(&keybuf,key,&(lx->slotcodes),0);
  if (len<0) {
    fd_close_outbuf(&keybuf);
    return FD_ERROR_VALUE;}
  else if (len == 0) {
    /* This means the key is a pair whose CAR doesn't have a slot
       code, so its not in the index at all */
    fd_close_outbuf(&keybuf);
    return FD_EMPTY;}
  else {
    struct LEVELDB_INDEX_RESULTS state = { FD_EMPTY, ix, &(lx->oidcodes) };
    struct BYTEVEC prefix = { keybuf.bufwrite-keybuf.buffer, keybuf.buffer };
    int rv =leveldb_scanner(db,lx->index_opts,NULL,key,&prefix,
                            leveldb_index_gather,&state);
    fd_close_outbuf(&keybuf);
    if (rv<0)
      return FD_ERROR_VALUE;
    else return state.results;}
}

static int leveldb_index_fetchsize(fd_index ix,lispval key)
{
  struct FD_LEVELDB_INDEX *lx = (fd_leveldb_index)ix;
  struct FD_LEVELDB *db = &(lx->leveldb);
  FD_DECL_OUTBUF(keybuf,1000);
  ssize_t len = leveldb_encode_key(&keybuf,key,&(lx->slotcodes),1);
  if (len<0) {
    fd_close_outbuf(&keybuf);
    return -1;}
  else if (len == 0) {
    /* This means the key is a pair whose CAR doesn't have a slot
       code. */
    return 0;}
  else {
    struct LEVELDB_INDEX_COUNT state = { 0, ix, &(lx->oidcodes) };
    struct BYTEVEC prefix = { keybuf.bufwrite-keybuf.buffer, keybuf.buffer };
    int rv = leveldb_scanner(db,lx->index_opts,NULL,key,&prefix,
                             leveldb_index_count,&state);
    if (rv<0)
      return rv;
    else return state.count;}
}

static lispval *leveldb_index_fetchn(fd_index ix,int n,const lispval *keys)
{
  struct FD_LEVELDB_INDEX *lx = (fd_leveldb_index)ix;
  struct FD_LEVELDB *db = &(lx->leveldb);
  struct LEVELDB_KEYBUF *keybufs = u8_alloc_n(n,struct LEVELDB_KEYBUF);
  struct FD_OUTBUF keyreps = { 0 };
  /* Pre-allocated for 60 bytes per key */
  FD_INIT_BYTE_OUTPUT(&keyreps,(n*60));
  struct LEVELDB_INDEX_RESULTS *states =
    u8_alloc_n(n,struct LEVELDB_INDEX_RESULTS);
  int i=0, to_fetch=0;
  while (i<n) {
    lispval key = keys[i];
    size_t offset = keyreps.bufwrite - keyreps.buffer;
    ssize_t key_len = leveldb_encode_key(&keyreps,key,&(lx->slotcodes),0);
    if (key_len < 0) {
      fd_close_outbuf(&keyreps);
      u8_free(states);
      return NULL;}
    states[i].results=FD_EMPTY;
    states[i].index=ix;
    states[i].oidcodes=&(lx->oidcodes);
    if (key_len) {
      keybufs[to_fetch].key = key;
      keybufs[to_fetch].keypos = i;
      keybufs[to_fetch].dtype = 1;
      /* We'll turn this into a real pointer when we're done */
      keybufs[to_fetch].encoded.bytes = (unsigned char *) offset;
      keybufs[to_fetch].encoded.n_bytes = key_len;
      to_fetch++;}
    i++;}
  /* Now convert the buffer offsets into buffer pointers */
  unsigned char *bufstart = keyreps.buffer;
  i=0; while (i<to_fetch) {
    size_t offset = (size_t) keybufs[i].encoded.bytes;
    keybufs[i].encoded.bytes = bufstart+offset;
    i++;}

  /* Sort before starting to retrieve */
  qsort(keybufs,n,sizeof(struct LEVELDB_KEYBUF),cmp_keybufs);

  leveldb_iterator_t *iterator = leveldb_create_iterator(db->dbptr,db->readopts);
  i=0; while (i<n) {
    int keypos = keybufs[i].keypos;
    int rv = leveldb_scanner(db,lx->index_opts,iterator,
                             keybufs[i].key,
                             & keybufs[i].encoded,
                             leveldb_index_gather,
                             & states[keypos]);
    if (rv<0) {
      u8_free(keybufs);
      fd_close_outbuf(&keyreps);
      return NULL;}
    i++;}

  leveldb_iter_destroy(iterator);

  lispval *results = u8_big_alloc_n(n,lispval);
  i=0; while (i<n) {
    results[i] = states[i].results;
    i++;}

  u8_free(keybufs);
  fd_close_outbuf(&keyreps);
  u8_free(states);

  return results;
}

static int leveldb_index_save(fd_leveldb_index lx,
                              struct FD_INDEX_COMMITS *commits)
{
  struct FD_LEVELDB *db = &(lx->leveldb);
  leveldb_writebatch_t *batch = leveldb_writebatch_create();
  ssize_t n_stores = commits->commit_n_stores;
  ssize_t n_drops = commits->commit_n_drops;
  int err = 0;
  if (n_stores+n_drops) {
    struct FD_CONST_KEYVAL *stores = commits->commit_stores;
    size_t store_i = 0;
    leveldb_iterator_t *iterator =
      leveldb_create_iterator(db->dbptr,db->readopts);
    FD_DECL_OUTBUF(keybuf,100);
    FD_DECL_OUTBUF(valbuf,1000);
    if (n_stores) {
      while (store_i < n_stores) {
        lispval key = stores[store_i].kv_key;
        lispval val = stores[store_i].kv_val;
        ssize_t key_rv = leveldb_encode_key(&keybuf,key,& lx->slotcodes,1);
        ssize_t val_rv = leveldb_encode_value(&valbuf,val,& lx->oidcodes);
        if ( (key_rv<0) || (val_rv<0) ) {err=-1; break;}
        struct BYTEVEC keyv = { keybuf.bufwrite-keybuf.buffer, keybuf.buffer };
        struct BYTEVEC valv = { valbuf.bufwrite-valbuf.buffer, valbuf.buffer };
        int edit_rv = leveldb_editor(db,lx->index_opts,iterator,batch,
                                     key,&keyv,&valv);
        if (edit_rv<0) {err=-1; break;}
        keybuf.bufwrite = keybuf.buffer;
        valbuf.bufwrite = valbuf.buffer;
        store_i++;}}
    if ( (n_drops) && (err==0) ) {
      struct FD_CONST_KEYVAL *drops = commits->commit_drops;
      lispval *drop_keys = u8_alloc_n(n_drops,lispval);
      int drop_i = 0; while (drop_i<n_drops) {
        drop_keys[drop_i] = drops[drop_i].kv_key;
        drop_i++;}
      lispval *drop_vals = leveldb_index_fetchn((fd_index)lx,n_drops,drop_keys);
      if (drop_vals) {
        drop_i = 0; while (drop_i < n_drops) {
          lispval key = drop_keys[drop_i];
          lispval cur = drop_vals[drop_i];
          lispval drop = drops[drop_i].kv_val;
          lispval diff = fd_difference(cur,drop);
          ssize_t key_rv = leveldb_encode_key(&keybuf,key,& lx->slotcodes,0);
          if (key_rv<0) {}
          struct BYTEVEC keyv = { keybuf.bufwrite-keybuf.buffer, keybuf.buffer };
          struct BYTEVEC valv = { 0 };
          if (!(FD_EMPTYP(diff))) {
            ssize_t val_rv = leveldb_encode_value(&valbuf,diff,& lx->oidcodes);
            if (val_rv<0) {}
            valv.n_bytes = valbuf.bufwrite-valbuf.buffer;
            valv.bytes = valbuf.buffer;
            fd_decref(diff);}
          int edit_rv = leveldb_editor(db,lx->index_opts,iterator,batch,
                                       key,&keyv,&valv);
          if (edit_rv<0) {}
          keybuf.bufwrite = keybuf.buffer;
          valbuf.bufwrite = valbuf.buffer;
          drop_i++;}}
      u8_free(drop_keys);
      if (drop_vals) {
        fd_decref_elts(drop_vals,n_drops);
        u8_big_free(drop_vals);}}
    leveldb_iter_destroy(iterator);
    fd_close_outbuf(&keybuf);
    fd_close_outbuf(&valbuf);}

  if (err) return -1;

  struct FD_CONST_KEYVAL *adds = commits->commit_adds;
  ssize_t n_adds = commits->commit_n_adds;
  size_t i = 0; while (i<n_adds) {
    int rv = leveldb_add_helper(db,&(lx->slotcodes),&(lx->oidcodes),batch,
                                adds[i].kv_key,adds[i].kv_val,
                                lx->index_opts);
    if (rv<0) {
      leveldb_writebatch_destroy(batch);
      return -1;}
    else i++;}

  struct FD_OIDCODER *oc = &(lx->oidcodes);
  if (oc->n_oids > oc->init_n_oids) {
    FD_DECL_OUTBUF(out,16384);
    struct FD_VECTOR vec;
    FD_INIT_STATIC_CONS(&vec,fd_vector_type);
    vec.vec_length = oc->n_oids;
    vec.vec_free_elts = vec.vec_bigalloc = vec.vec_bigalloc_elts = 0;
    vec.vec_elts = oc->baseoids;
    ssize_t len = fd_write_dtype(&out,(lispval)&vec);
    if (len<0) err=-1;
    else leveldb_writebatch_put
           (batch,"\377BASEOIDS",strlen("\377BASEOIDS"),out.buffer,len);
    fd_close_outbuf(&out);}

  if (err) {
    leveldb_writebatch_destroy(batch);
    return -1;}

  struct FD_SLOTCODER *sc = &(lx->slotcodes);
  if (sc->n_slotcodes > sc->init_n_slotcodes) {
    FD_DECL_OUTBUF(out,16384);
    ssize_t len =fd_write_dtype(&out,(lispval)(sc->slotids));
    leveldb_writebatch_put
      (batch,"\377SLOTIDS",strlen("\377SLOTIDS"),out.buffer,len);
    fd_close_outbuf(&out);}

  char *errmsg = NULL;
  leveldb_write(db->dbptr,sync_writeopts,batch,&errmsg);

  leveldb_writebatch_destroy(batch);

  if (errmsg)
    return -1;
  else return n_adds+n_stores+n_drops;
}

static int leveldb_index_commit(fd_index ix,fd_commit_phase phase,
                                struct FD_INDEX_COMMITS *commits)
{
  struct FD_LEVELDB_INDEX *lx = (fd_leveldb_index)ix;
  switch (phase) {
  case fd_no_commit:
    u8_seterr("BadCommitPhase(commit_none)","hashindex_commit",
              u8_strdup(ix->indexid));
    return -1;
  case fd_commit_start: {
    return 1;}
  case fd_commit_write: {
    return leveldb_index_save(lx,commits);}
  case fd_commit_rollback: {
    return 1;}
  case fd_commit_sync: {
    return 1;}
  case fd_commit_cleanup: {
    return 1;}
  default: {
    u8_logf(LOG_WARN,"NoPhasedCommit",
            "The index %s doesn't support phased commits",
            ix->indexid);
    return -1;}
  }
}

static lispval *leveldb_index_fetchkeys(fd_index ix,int *nptr)
{
  struct FD_LEVELDB_INDEX *lx = (fd_leveldb_index)ix;
  lispval results = FD_EMPTY_CHOICE;
  leveldb_iterator_t *iterator=
    leveldb_create_iterator(lx->leveldb.dbptr,lx->leveldb.readopts);
  unsigned char *prefix = NULL; ssize_t prefix_len = -1;
  struct BYTEVEC keybuf;
  leveldb_iter_seek(iterator,NULL,0);
  if (leveldb_iter_valid(iterator))
    keybuf.bytes = leveldb_iter_key(iterator,&(keybuf.n_bytes));
  else {
    *nptr = 0;
    return NULL;}
  while (leveldb_iter_valid(iterator)) {
    struct FD_INBUF keystream = { 0 };
    /* Skip metadata fields, starting with 0xFF */
    if ( (keybuf.n_bytes) && (keybuf.bytes[0]>=0x80) ) {
      leveldb_iter_next(iterator);
      if (leveldb_iter_valid(iterator))
        keybuf.bytes = leveldb_iter_key(iterator,&(keybuf.n_bytes));
      continue;}
    if (prefix) {u8_free(prefix); prefix=NULL;}
    FD_INIT_BYTE_INPUT(&keystream,keybuf.bytes,keybuf.n_bytes);
    lispval key = leveldb_decode_key(&keystream,& lx->slotcodes);
    if (FD_ABORTP(key)) {
      leveldb_iter_destroy(iterator);
      fd_close_inbuf(&keystream);
      return NULL;}
    else {FD_ADD_TO_CHOICE(results,key);}
    prefix = u8_memdup(keybuf.n_bytes,keybuf.bytes);
    prefix_len = keybuf.n_bytes;
    leveldb_iter_next(iterator);
    while (leveldb_iter_valid(iterator)) {
      keybuf.bytes = leveldb_iter_key(iterator,&(keybuf.n_bytes));
      if (memcmp(keybuf.bytes,prefix,prefix_len) == 0)
        leveldb_iter_next(iterator);
      else break;}}
  leveldb_iter_destroy(iterator);
  if (prefix) u8_free(prefix);
  results = fd_simplify_choice(results);
  if (FD_EMPTYP(results)) {
    *nptr = 0;
    return NULL;}
  else if (FD_CHOICEP(results)) {
    int n = FD_CHOICE_SIZE(results);
    if (FD_CONS_REFCOUNT(results) == 1) {
      size_t n_bytes = SIZEOF_LISPVAL*n;
      lispval *keys = u8_big_alloc(n_bytes);
      memcpy(keys,FD_CHOICE_ELTS(results),n_bytes);
      *nptr = n;
      fd_free_choice((fd_choice)results);
      return keys;}
    else {
      lispval *elts = (lispval *) FD_CHOICE_ELTS(results);
      lispval *keys = u8_big_alloc_n(n,lispval);
      fd_copy_vec(elts,n,keys,0);
      *nptr = n;
      fd_decref(results);
      return keys;}}
  else {
    lispval *one = u8_big_alloc_n(1,lispval);
    *one = results;
    *nptr = 1;
    return one;}
}

static fd_index leveldb_index_open(u8_string spec,fd_storage_flags flags,
                                   lispval opts)
{
  return fd_open_leveldb_index(spec,flags,opts);
}


static void leveldb_index_close(fd_index ix)
{
  struct FD_LEVELDB_INDEX *ldbx = (struct FD_LEVELDB_INDEX *)ix;
  fd_close_leveldb(&(ldbx->leveldb));
}

static fd_index leveldb_index_create(u8_string spec,void *typedata,
                                     fd_storage_flags flags,lispval opts)
{
  return fd_make_leveldb_index(spec,opts);
}

static void recycle_leveldb_index(fd_index ix)
{
  struct FD_LEVELDB_INDEX *db = (fd_leveldb_index) ix;
  fd_close_leveldb(&(db->leveldb));
  if (db->leveldb.path) {
    u8_free(db->leveldb.path);
    db->leveldb.path = NULL;}
  fd_decref(db->leveldb.opts);
  if (db->leveldb.writeopts)
    leveldb_writeoptions_destroy(db->leveldb.writeopts);
  leveldb_options_destroy(db->leveldb.optionsptr);

}


/* Initializing the index driver */

static struct FD_INDEX_HANDLER leveldb_index_handler={
  "leveldb_index", 1, sizeof(struct FD_LEVELDB_INDEX), 14,
  leveldb_index_close, /* close */
  leveldb_index_commit, /* commit */
  leveldb_index_fetch, /* fetch */
  leveldb_index_fetchsize, /* fetchsize */
  NULL, /* prefetch */
  leveldb_index_fetchn, /* fetchn */
  leveldb_index_fetchkeys, /* fetchkeys */
  NULL, /* fetchinfo */
  NULL, /* batchadd */
  leveldb_index_create, /* create */
  NULL, /* walk */
  recycle_leveldb_index, /* recycle */
  NULL /* indexctl */
};

/* Scheme primitives */

static lispval use_leveldb_pool_prim(lispval path,lispval opts)
{
  fd_pool pool = fd_open_leveldb_pool(FD_CSTRING(path),-1,opts);
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

/* Matching rocksdb  pathnames */

static u8_string leveldb_matcher(u8_string name,void *data)
{
  int rv = 0;
  if (u8_directoryp(name)) {
    u8_string lock_file = u8_mkpath(name,"LOCK");
    u8_string current_file = u8_mkpath(name,"CURRENT");
    u8_string identity_file = u8_mkpath(name,"IDENTITY");
    if ( (u8_file_existsp(lock_file)) &&
         (u8_file_existsp(current_file)) &&
         (!(u8_file_existsp(identity_file))) ) {
      u8_byte *contents  = (u8_byte *) u8_filestring(current_file,NULL);
      if (contents) {
        size_t len = strlen(contents);
        if (contents[len-1] == '\n') contents[len-1]='\0';
        u8_string current = u8_mkpath(name,contents);
        if ( (*contents) && (u8_file_existsp(current)) )
          rv = 1;
        u8_free(current);
        u8_free(contents);}}
    u8_free(lock_file);
    u8_free(current_file);
    u8_free(identity_file);}
  U8_CLEAR_ERRNO();
  if (rv)
    return u8_strdup(name);
  else return NULL;
}

/* Initialization */

FD_EXPORT int fd_init_leveldb()
{
  lispval module;
  if (leveldb_initialized) return 0;
  leveldb_initialized = u8_millitime();

  default_readopts = leveldb_readoptions_create();
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

  module = fd_new_cmodule("LEVELDB",0,fd_init_leveldb);

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
                                  fd_vector_type,FD_VOID,-1,FD_VOID));

  fd_idefn3(module,"LEVELDB/PREFIX/GET",
            leveldb_prefix_get_prim,2,
            "(LEVELDB/PREFIX/GET *db* *key* [*opts*]) returns all the "
            "key/value pairs (as packets), whose keys begin with the "
            "DTYPE representation of *key*.",
            fd_leveldb_type,FD_VOID,
            -1,FD_VOID,-1,FD_VOID);
  fd_idefn3(module,"LEVELDB/PREFIX/GETN",
            leveldb_prefix_getn_prim,2,
            "(LEVELDB/PREFIX/GET *db* *key* [*opts*]) returns all the "
            "key/value pairs (as packets), whose keys begin with the "
            "DTYPE representation of *key*.",
            fd_leveldb_type,FD_VOID,
            fd_vector_type,FD_VOID,-1,FD_VOID);


  fd_idefn(module,fd_make_cprim4x("LEVELDB/PUT!",leveldb_put_prim,3,
                                  fd_leveldb_type,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID,
                                  -1,FD_VOID));
  fd_idefn(module,fd_make_cprim3x("LEVELDB/DROP!",leveldb_drop_prim,2,
                                  fd_leveldb_type,FD_VOID,
                                  -1,FD_VOID,-1,FD_VOID));


  fd_idefn3(module,"LEVELDB/INDEX/GET",leveldb_index_get_prim,2,
            "(LEVELDB/INDEX/GET *db* *key* [*opts*]) gets values "
            "associated with *key* in *db*, using the "
            "leveldb database as an index and options provided in *opts*.",
            fd_leveldb_type,FD_VOID,-1,FD_VOID,-1,FD_VOID);
  fd_idefn4(module,"LEVELDB/INDEX/ADD!",leveldb_index_add_prim,3,
            "(LEVELDB/INDEX/ADD! *db* *key* *value [*opts*]) Saves *values* "
            "in *db*, associating them with *key* and using the "
            "leveldb database as an index with options provided in *opts*.",
            fd_leveldb_type,FD_VOID,-1,FD_VOID,-1,FD_VOID,-1,FD_VOID);


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

  fd_register_pool_type
    ("leveldb",
     &leveldb_pool_handler,
     leveldb_pool_open,
     leveldb_matcher,
     NULL);

  fd_register_index_type
    ("leveldb",
     &leveldb_index_handler,
     leveldb_index_open,
     leveldb_matcher,
     NULL);

  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
