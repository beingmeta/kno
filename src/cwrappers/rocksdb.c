/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* rocksdb.c
   This implements Kno bindings to rocksdb.
   Copyright (C) 2007-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#define KNO_INLINE_BUFIO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/eval.h"
#include "kno/sequences.h"
#include "kno/texttools.h"
#include "kno/bigints.h"
#include "kno/knoregex.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"
#include "kno/bufio.h"
#include "kno/cprims.h"

#include <libu8/libu8.h>
#include <libu8/libu8io.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include "rocksdb/c.h"
#include "rocksdb.h"


kno_lisp_type kno_rocksdb_type;
#define KNO_ROCKSDB_TYPE 0x584cb00
u8_condition RocksDB_Error = "RocksDB/Error";

static ssize_t default_writebuf_size = -1;
static ssize_t default_cache_size = -1;
static ssize_t default_block_size = -1;
static int default_maxfiles = -1;
static int default_restart_interval = -1;
static int default_compression = 0;

#define ROCKSDB_META_KEY       0xFF
#define ROCKSDB_OIDKEY_PREFIX  0xFE
#define ROCKSDB_KEYINFO        0x80

#define SYM(x) (kno_intern(x))

DEF_KNOSYM(xrefs); DEF_KNOSYM(maxrefs); DEF_KNOSYM(debug); DEF_KNOSYM(vector);

struct BYTEVEC {
  size_t n_bytes;
  const unsigned char *bytes;};
struct ROCKSDB_KEYBUF {
  lispval key;
  unsigned int keypos:31, dtype:1;
  struct BYTEVEC encoded;};

typedef int (*rocksdb_prefix_iterfn)(struct KNO_ROCKSDB *db,
				     lispval key,struct BYTEVEC *prefix,
				     struct BYTEVEC *keybuf,
				     struct BYTEVEC *valbuf,
				     void *state);
typedef int (*rocksdb_prefix_key_iterfn)(struct KNO_ROCKSDB *db,
					 lispval key,struct BYTEVEC *prefix,
					 struct BYTEVEC *keybuf,
					 void *state);

KNO_EXPORT int kno_init_rocksdb(void) KNO_LIBINIT_FN;

static u8_mutex rocksdb_list_lock;
static struct KNO_ROCKSDB *rocksdb_list = NULL;

static long long int rocksdb_initialized = 0;

static rocksdb_readoptions_t *default_readopts;
static rocksdb_writeoptions_t *sync_writeopts;

/* Accessing various metadata fields */

static lispval get_prop(rocksdb_t *dbptr,char *key,lispval dflt)
{
  ssize_t data_size; char *errmsg = NULL;
  unsigned char *buf = rocksdb_get
    (dbptr,default_readopts,key,strlen(key),&data_size,&errmsg);
  if (buf) {
    lispval result = KNO_VOID;
    struct KNO_INBUF in = { 0 };
    KNO_INIT_BYTE_INPUT(&in,buf,data_size);
    result = kno_read_xtype(&in,NULL);
    u8_free(buf);
    return result;}
  else if (errmsg)
    return kno_err(RocksDB_Error,"get_prop",NULL,KNO_VOID);
  else return dflt;
}

static ssize_t set_prop(rocksdb_t *dbptr,char *key,lispval value,
			rocksdb_writeoptions_t *writeopts)
{
  ssize_t xtype_len; char *errmsg = NULL;
  struct KNO_OUTBUF out = { 0 };
  KNO_INIT_BYTE_OUTPUT(&out,512);
  if ((xtype_len = kno_write_xtype(&out,value,NULL))>0) {
    rocksdb_put(dbptr,writeopts,key,strlen(key),
		out.buffer,out.bufwrite-out.buffer,
		&errmsg);
    kno_close_outbuf(&out);
    if (errmsg)
      return kno_reterr(RocksDB_Error,"set_prop",errmsg,KNO_VOID);
    else return xtype_len;}
  else return -1;
}

/* Initialization */

/* Getting various Rocksdb option objects */

static rocksdb_options_t *get_rocksdb_options(lispval opts,
					      rocksdb_env_t *env,
					      rocksdb_cache_t *cache)
{
  rocksdb_options_t *options = rocksdb_options_create();
  rocksdb_block_based_table_options_t *blockopts = rocksdb_block_based_options_create();
  lispval bufsize_spec = kno_getopt(opts,SYM("WRITEBUF"),KNO_VOID);
  lispval maxfiles_spec = kno_getopt(opts,SYM("MAXFILES"),KNO_VOID);
  lispval blocksize_spec = kno_getopt(opts,SYM("BLOCKSIZE"),KNO_VOID);
  lispval restart_spec = kno_getopt(opts,SYM("RESTART"),KNO_VOID);
  lispval compress_spec = kno_getopt(opts,SYM("COMPRESS"),KNO_VOID);
  if (env) rocksdb_options_set_env(options,env);
  if (kno_testopt(opts,SYM("INIT"),KNO_VOID)) {
    rocksdb_options_set_create_if_missing(options,1);
    rocksdb_options_set_error_if_exists(options,1);}
  else if (!(kno_testopt(opts,SYM("READ"),KNO_VOID))) {
    rocksdb_options_set_create_if_missing(options,1);}
  else {}
  if (kno_testopt(opts,SYM("PARANOID"),KNO_VOID)) {
    rocksdb_options_set_paranoid_checks(options,1);}
  if (KNO_UINTP(bufsize_spec))
    rocksdb_options_set_write_buffer_size(options,KNO_FIX2INT(bufsize_spec));
  else if (default_writebuf_size>0)
    rocksdb_options_set_write_buffer_size(options,default_writebuf_size);
  else {}
  if (KNO_UINTP(maxfiles_spec))
    rocksdb_options_set_max_open_files(options,KNO_FIX2INT(maxfiles_spec));
  else if (default_maxfiles>0)
    rocksdb_options_set_max_open_files(options,default_maxfiles);
  else {}
  if (KNO_UINTP(blocksize_spec))
    rocksdb_block_based_options_set_block_size(blockopts,KNO_FIX2INT(blocksize_spec));
  else if (default_block_size>0)
    rocksdb_block_based_options_set_block_size(blockopts,default_block_size);
  else {}
  if (KNO_UINTP(restart_spec))
    rocksdb_block_based_options_set_block_restart_interval
      (blockopts,KNO_FIX2INT(restart_spec));
  else if (default_block_size>0)
    rocksdb_block_based_options_set_block_restart_interval
      (blockopts,default_restart_interval);
  else {}
  if (KNO_TRUEP(compress_spec))
    rocksdb_options_set_compression(options,rocksdb_snappy_compression);
  else if (default_compression)
    rocksdb_options_set_compression(options,rocksdb_snappy_compression);
  else rocksdb_options_set_compression(options,rocksdb_no_compression);
  if (cache) rocksdb_block_based_options_set_block_cache(blockopts,cache);
  rocksdb_options_set_block_based_table_factory(options,blockopts);
  rocksdb_block_based_options_destroy(blockopts);
  return options;
}

static rocksdb_cache_t *get_rocksdb_cache(lispval opts)
{
  rocksdb_cache_t *cache = NULL;
  lispval cache_size = kno_getopt(opts,SYM("CACHESIZE"),KNO_VOID);
  if (KNO_UINTP(cache_size))
    cache = rocksdb_cache_create_lru(KNO_FIX2INT(cache_size));
  else if (default_cache_size>0)
    cache = rocksdb_cache_create_lru(default_cache_size);
  else cache = NULL;
  return cache;
}

static rocksdb_env_t *get_rocksdb_env(lispval opts)
{
  if ((KNO_VOIDP(opts))||(KNO_FALSEP(opts)))
    return NULL;
  else {
    rocksdb_env_t *env = rocksdb_create_default_env(); int needed = 0;
    /* Setup env from opts and other defaults */
    if (needed)
      return env;
    else {
      rocksdb_env_destroy(env);
      return NULL;}}
}

static rocksdb_readoptions_t *get_read_options(kno_rocksdb db,lispval opts_arg)
{
  if ( (KNO_VOIDP(opts_arg)) ||
       (KNO_FALSEP(opts_arg)) ||
       (KNO_DEFAULTP(opts_arg)) )
    return db->readopts;
  else {
    int modified = 0, free_opts = 0;
    lispval opts = (KNO_VOIDP(opts_arg)) ? (db->opts) :
      (KNO_VOIDP(db->opts)) ? (opts_arg) :
      (free_opts = 1,kno_make_pair(opts_arg,db->opts));
    rocksdb_readoptions_t *readopts = rocksdb_readoptions_create();
    /* Set up readopts based on opts */
    if (free_opts) kno_decref(opts);
    if (modified)
      return readopts;
    else {
      rocksdb_readoptions_destroy(readopts);
      return db->readopts;}}
}

static rocksdb_writeoptions_t *get_write_options(kno_rocksdb db,lispval opts_arg)
{
  if ((KNO_VOIDP(opts_arg))||(KNO_FALSEP(opts_arg))||(KNO_DEFAULTP(opts_arg)))
    return db->writeopts;
  else {
    int modified = 0, free_opts = 0;
    lispval opts = (KNO_VOIDP(opts_arg)) ? (db->opts) :
      (KNO_VOIDP(db->opts)) ? (opts_arg) :
      (free_opts = 1,kno_make_pair(opts_arg,db->opts));
    rocksdb_writeoptions_t *writeopts = rocksdb_writeoptions_create();
    /* Set up writeopts based on opts */
    if (free_opts) kno_decref(opts);
    if (modified)
      return writeopts;
    else {
      rocksdb_writeoptions_destroy(writeopts);
      return db->writeopts;}}
}

/* Initializing xrefs */

static int init_rocksdb_xrefs(struct KNO_ROCKSDB *db,lispval opts)
{
  /* This initializes the xrefs value in the database and on the structure */
  rocksdb_writeoptions_t *writeopts = get_write_options(db,opts);
  lispval xrefs_init = kno_getopt(opts,KNOSYM(xrefs),KNO_VOID);
  int init_maxrefs = kno_getfixopt(opts,"maxrefs",-1);
  struct XTYPE_REFS *xrefs = &(db->xrefs);
  lispval *refs = NULL;
  ssize_t n_xrefs = 0, max_xrefs = init_maxrefs, xrefs_len = 0;
  if (KNO_VOIDP(xrefs_init)) {
    if (max_xrefs < 0)
      xrefs_len = 256;
    else if (max_xrefs == 0)
      xrefs_len = 0;
    else if (max_xrefs >= 1024)
      xrefs_len = 1024;
    else xrefs_len = max_xrefs;}
  else if (KNO_FALSEP(xrefs_init)) {
    xrefs_len = 0; max_xrefs = 0;}
  else if (KNO_UINTP(xrefs_init)) {
    ssize_t maxlen = KNO_FIX2INT(xrefs_init);
    xrefs_len = maxlen;
    if (maxlen > max_xrefs) max_xrefs = maxlen;}
  else if (KNO_VECTORP(xrefs_init)) {
    refs = KNO_VECTOR_ELTS(xrefs_init);
    n_xrefs = KNO_VECTOR_LENGTH(xrefs_init);
    xrefs_len = 256; while (xrefs_len<n_xrefs) xrefs_len *= 2;}
  else {
    xrefs_len = 256; n_xrefs = 0; max_xrefs = -1;}
  if (xrefs_len>0)
    refs = u8_alloc_n(xrefs_len,lispval);
  else refs = NULL;
  if (refs == NULL)
    return kno_init_xrefs(xrefs,0,-1,n_xrefs,xrefs_len,max_xrefs,refs,NULL);
  char *errmsg = NULL;
  KNO_DECL_OUTBUF(keyout,100);
  KNO_DECL_OUTBUF(valout,100);
  kno_write_bytes(&keyout,"\377XREFS",6);
  kno_write_varint(&valout,n_xrefs);
  if ( max_xrefs >= 0) kno_write_varint(&valout,max_xrefs);
  rocksdb_put(db->dbptr,writeopts,
	      keyout.buffer,BUFIO_POINT(&keyout),
	      valout.buffer,BUFIO_POINT(&valout),
	      &errmsg);
  if (errmsg) {
    u8_byte buf[100];
    return kno_reterr(RocksDB_Error,"init_rocksdb_xrefs",
		      u8_bprintf(buf,"%s: %s",db->path,errmsg),KNO_VOID);}
  int i = 0; while (i<n_xrefs) {
    keyout.bufwrite=keyout.buffer;
    kno_write_bytes(&keyout,"\377XREFS",6);
    kno_write_4bytes(&keyout,i);
    valout.bufwrite=valout.buffer;
    kno_write_xtype(&valout,refs[i],NULL);
    rocksdb_put(db->dbptr,writeopts,
		keyout.buffer,BUFIO_POINT(&keyout),
		valout.buffer,BUFIO_POINT(&valout),
		&errmsg);
    if (errmsg) {
      u8_byte buf[100];
      return kno_reterr(RocksDB_Error,"init_rocksdb_xrefs",
			u8_bprintf(buf,"%s: %s",db->path,errmsg),KNO_VOID);}
    i++;}
  int rv = kno_init_xrefs(xrefs,0,-1,n_xrefs,xrefs_len,max_xrefs,refs,NULL);
  if (rv<0) return rv;
  db->saved_xrefs = n_xrefs;
  return rv;
}

static int setup_rocksdb_xrefs(struct KNO_ROCKSDB *db,lispval opts)
{
  rocksdb_readoptions_t *readopts = get_read_options(db,opts);
  struct XTYPE_REFS *xrefs = &(db->xrefs);
  struct BYTEVEC _prefix = {0}, *prefix;
  int rv = 0, i = -1, found = 0;
  rocksdb_iterator_t *iterator = rocksdb_create_iterator(db->dbptr,readopts);
  ssize_t n_xrefs = 0, max_xrefs = 0, xrefs_len = 0;
  lispval *refs = NULL;
  rocksdb_iter_seek(iterator,"\377XREFS",6);
  while (rocksdb_iter_valid(iterator)) {
    struct BYTEVEC keybuf, valbuf;
    keybuf.bytes = rocksdb_iter_key(iterator,&(keybuf.n_bytes));
    if ( (keybuf.n_bytes<6) || (memcmp(keybuf.bytes,"\377XREFS",6)) ) break;
    else if ( (i == -1) && (keybuf.n_bytes == 6) ) {
      valbuf.bytes = rocksdb_iter_value(iterator,&(valbuf.n_bytes));
      struct KNO_INBUF valstream;
      KNO_INIT_INBUF(&valstream,valbuf.bytes,valbuf.n_bytes,0);
      n_xrefs   = kno_read_varint(&valstream);
      max_xrefs = kno_read_varint(&valstream);
      if ( (max_xrefs == 0) && (n_xrefs == 0) ) break;
      else if ( max_xrefs == n_xrefs ) {
	xrefs_len = n_xrefs;
	refs = u8_alloc_n(xrefs_len,lispval);}
      else {
	ssize_t len = 256;
	while (len<n_xrefs) len = len*2;
	if ( (max_xrefs>0) && (len > n_xrefs) && (len > max_xrefs) )
	  xrefs_len = max_xrefs;
	else xrefs_len = len;
	refs = u8_alloc_n(xrefs_len,lispval);}
      found = 1;
      i++;}
    else {
      struct KNO_INBUF keystream;
      KNO_INIT_INBUF(&keystream,keybuf.bytes+6,keybuf.n_bytes-6,0);
      ssize_t xref_i = kno_read_4bytes(&keystream);
      if (xref_i != i) goto corrupted_xrefs;
      valbuf.bytes = rocksdb_iter_value(iterator,&(valbuf.n_bytes));
      struct KNO_INBUF valstream;
      KNO_INIT_INBUF(&valstream,valbuf.bytes,valbuf.n_bytes,0);
      lispval ref = kno_read_xtype(&valstream,NULL);
      if (KNO_ABORTED(ref)) {
	kno_clear_errors(1);
	goto corrupted_xrefs;}
      if (i>=xrefs_len) {
	ssize_t new_len = xrefs_len + 1;
	refs = u8_realloc(refs,sizeof(lispval)*xrefs_len);}
      refs[i++]=ref;}
    rocksdb_iter_next(iterator);}
  if (i > n_xrefs) n_xrefs=i;
  if (found)
    rv = kno_init_xrefs(xrefs,0,-1,n_xrefs,xrefs_len,max_xrefs,refs,NULL);
  else rv = init_rocksdb_xrefs(db,opts);
  rocksdb_iter_destroy(iterator);
  if (rv >= 0) return rv;
 corrupted_xrefs:
  if (kno_testopt(opts,KNOSYM(debug),KNO_VOID)) {
    u8_log(LOGWARN,"CorruptedXRefs","For RocksDB @#%d %s",i,db->path);
    kno_init_xrefs(xrefs,0,-1,0,0,0,refs,NULL);
    return 0;}
  else {
    kno_init_xrefs(xrefs,0,-1,0,0,0,refs,NULL);
    kno_seterr("CorruptedXRefs","Rocksdb/open",db->path,KNO_VOID);
    return -1;}
}

static int sync_rocksdb_xrefs(struct KNO_ROCKSDB *db)
{
  if (db->saved_xrefs == db->xrefs.xt_n_refs) return 0;
  u8_lock_mutex(&(db->rocksdb_lock));
  if (db->saved_xrefs == db->xrefs.xt_n_refs) {
    u8_unlock_mutex(&(db->rocksdb_lock));
    return 0;}
  int i = db->saved_xrefs,  target = db->xrefs.xt_n_refs;
  lispval *refs = db->xrefs.xt_refs;
  while (i<target) {
    char *errmsg = NULL;
    KNO_DECL_OUTBUF(keyout,100);
    KNO_DECL_OUTBUF(valout,100);
    kno_write_bytes(&keyout,"\377XREFS",6);
    kno_write_4bytes(&keyout,i);
    kno_write_xtype(&valout,refs[i],NULL);
    rocksdb_put(db->dbptr,db->writeopts,
		keyout.buffer,BUFIO_POINT(&keyout),
		valout.buffer,BUFIO_POINT(&valout),
		&errmsg);
    i++;}
  db->saved_xrefs = target;
  ssize_t max_xrefs = db->xrefs.xt_refs_max;
  KNO_DECL_OUTBUF(valout,100);
  kno_write_varint(&valout,target);
  if (target>max_xrefs) max_xrefs=target;
  if (target>=0) kno_write_varint(&valout,target);
  char *errmsg = NULL;
  rocksdb_put(db->dbptr,db->writeopts,
	      "\377XREFS",6,
	      valout.buffer,BUFIO_POINT(&valout),
	      &errmsg);
  u8_unlock_mutex(&(db->rocksdb_lock));
  return target;
}

static int sync_rocksdb_xrefs(struct KNO_ROCKSDB *db);
#define SYNC_XREFS(db) \
  if (db->saved_xrefs != db->xrefs.n_xrefs) sync_rocksdb_xrefs(db); else {};

static ssize_t db_write_xtype(struct KNO_ROCKSDB *db,kno_outbuf out,lispval x)
{
  ssize_t rv = kno_write_xtype(out,x,&(db->xrefs));
  if (rv<0) return rv;
  if ( (db->xrefs.xt_refs_flags) & (XTYPE_REFS_CHANGED) ) sync_rocksdb_xrefs(db);
  return rv;
}

/* Initializing KNO_ROCKSDB structs */

struct KNO_ROCKSDB *init_rocksdb(struct KNO_ROCKSDB *db)
{
  char *errmsg = NULL;
  u8_string path = db->path;
  lispval opts = db->opts;
  rocksdb_env_t *env = get_rocksdb_env(opts);
  rocksdb_cache_t *cache = get_rocksdb_cache(opts);
  rocksdb_options_t *options = get_rocksdb_options(opts,env,cache);
  if (env) rocksdb_options_set_env(options,env);
  rocksdb_readoptions_t *readopts = get_read_options(db,opts);
  rocksdb_writeoptions_t *writeopts = get_write_options(db,opts);
  enum rocksdb_status status = db->dbstatus;
  if (status == rocksdb_raw) {
    u8_init_mutex(&(db->rocksdb_lock));
    db->dbstatus = rocksdb_sketchy;}
  u8_lock_mutex(&(db->rocksdb_lock));
  db->dbstatus = rocksdb_opening;
  if (readopts) db->readopts = readopts;
  else db->readopts = rocksdb_readoptions_create();
  if (writeopts) db->writeopts = writeopts;
  else db->writeopts = rocksdb_writeoptions_create();
  db->optionsptr = options;
  db->cacheptr = cache;
  db->envptr = env;
  if ((db->dbptr = rocksdb_open(options,path,&errmsg)) &&
      (setup_rocksdb_xrefs(db,opts) >= 0) ) {
    db->dbstatus = rocksdb_opened;
    u8_unlock_mutex(&(db->rocksdb_lock));
     return db;}
  else {
    kno_seterr("OpenFailed","init_rocksdb",errmsg,opts);
    db->dbstatus = rocksdb_error;
    u8_free(db->path); db->path = NULL;
    kno_decref(db->opts); db->opts = KNO_VOID;
    rocksdb_options_destroy(options);
    if (readopts) rocksdb_readoptions_destroy(readopts);
    if (writeopts) rocksdb_writeoptions_destroy(writeopts);
    if (cache) rocksdb_cache_destroy(cache);
    if (env) rocksdb_env_destroy(env);
    if (status == rocksdb_raw) {
      u8_unlock_mutex(&(db->rocksdb_lock));
      u8_destroy_mutex(&(db->rocksdb_lock));}
    else u8_unlock_mutex(&(db->rocksdb_lock));
    return NULL;}
}

KNO_EXPORT struct KNO_ROCKSDB *kno_open_rocksdb(u8_string path,lispval opts)
{
  u8_lock_mutex(&(rocksdb_list_lock));
  u8_string realpath = u8_realpath(path,NULL);
  struct KNO_ROCKSDB *scan = rocksdb_list;
  while (scan) {
    if (strcmp(realpath,scan->realpath)==0) {
      u8_unlock_mutex(&(rocksdb_list_lock));
      kno_incref((lispval)scan);
      u8_free(realpath);
      return scan;}
    else scan = scan->rocksdb_list;}
  struct KNO_ROCKSDB *db = u8_alloc(struct KNO_ROCKSDB);
  KNO_INIT_FRESH_CONS(db,kno_rocksdb_type);
  db->path = u8_strdup(path);
  db->realpath = realpath;
  db->opts = kno_incref(opts);
  db = init_rocksdb(db);
  if (db == NULL) {
    u8_unlock_mutex(&(rocksdb_list_lock));
    return NULL;}
  db->rocksdb_list = rocksdb_list;
  rocksdb_list = db;
  u8_unlock_mutex(&(rocksdb_list_lock));
  kno_incref((lispval)db);
  return db;
}

KNO_EXPORT
int kno_close_rocksdb(kno_rocksdb db)
{
  int closed = 0;
  if ( (db->dbstatus == rocksdb_opened) ||
       (db->dbstatus == rocksdb_opening) ) {
    u8_lock_mutex(&(db->rocksdb_lock));
    if (db->dbstatus == rocksdb_opened) {
      db->dbstatus = rocksdb_closing;
      rocksdb_close(db->dbptr);
      db->dbstatus = rocksdb_closed;
      /* if (db->dbptr) rocksdb_free(db->dbptr); */
      db->dbptr = NULL;
      closed = 1;}
    u8_unlock_mutex(&(db->rocksdb_lock));}
  return closed;
}

KNO_EXPORT
kno_rocksdb kno_reopen_rocksdb(kno_rocksdb db)
{
  if ( (db->dbstatus == rocksdb_opened) ||
       (db->dbstatus == rocksdb_opening) )
    return db;
  else {
    char *errmsg;
    u8_lock_mutex(&(db->rocksdb_lock));
    if ( (db->dbstatus == rocksdb_opened) ||
	 (db->dbstatus == rocksdb_opening) ) {
      u8_unlock_mutex(&(db->rocksdb_lock));
      return db;}
    db->dbstatus = rocksdb_opening;
    if ((db->dbptr = (rocksdb_open(db->optionsptr,db->path,&errmsg)))) {
      db->dbstatus = rocksdb_opened;
      u8_unlock_mutex(&(db->rocksdb_lock));
      return db;}
    else {
      u8_unlock_mutex(&(db->rocksdb_lock));
      kno_seterr("OpenFailed","kno_reopen_rocksdb",errmsg,KNO_VOID);
      return NULL;}}
}

/* DType object methods */

static int unparse_rocksdb(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)x;
  u8_printf(out,"#<Rocksdb %s>",db->path);
  return 1;
}

static void free_rocksdb(struct KNO_ROCKSDB *db)
{
  u8_lock_mutex(&(rocksdb_list_lock));
  if (db == rocksdb_list) {
    rocksdb_list = db->rocksdb_list;
    u8_unlock_mutex(&(rocksdb_list_lock));
    u8_free(db);
    return;}
  struct KNO_ROCKSDB *scan = rocksdb_list;
  while ( (scan) && (scan->rocksdb_list != db) )
    scan = scan->rocksdb_list;
  if (scan==NULL) return;
  scan->rocksdb_list = db->rocksdb_list;
  u8_unlock_mutex(&(rocksdb_list_lock));  
  u8_free(db);
}

static void recycle_rocksdb(struct KNO_RAW_CONS *c)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)c;
  kno_close_rocksdb(db);
  kno_recycle_xrefs(&(db->xrefs));
  if (db->path) {
    u8_free(db->path);
    db->path = NULL;}
  kno_decref(db->opts);
  if (db->writeopts)
    rocksdb_writeoptions_destroy(db->writeopts);
  rocksdb_options_destroy(db->optionsptr);
  free_rocksdb(db);
}

/* Primitives */


DEFC_PRIM("rocksdb/open",rocksdb_open_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"path",kno_string_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval rocksdb_open_prim(lispval path,lispval opts)
{
  struct KNO_ROCKSDB *db = kno_open_rocksdb(KNO_CSTRING(path),opts);
  if (db->dbptr) {
    KNO_INIT_CONS(db,kno_rocksdb_type);
    return (lispval) db;}
  else {
    u8_free(db);
    return KNO_ERROR_VALUE;}
}


DEFC_PRIM("rocksdb?",rocksdbp_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval rocksdbp_prim(lispval arg)
{
  if (KNO_TYPEP(arg,kno_rocksdb_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFC_PRIM("rocksdb/close",rocksdb_close_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID})
static lispval rocksdb_close_prim(lispval rocksdb)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  kno_close_rocksdb(db);
  return KNO_TRUE;
}

DEFC_PRIM("rocksdb/reopen",rocksdb_reopen_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID})
static lispval rocksdb_reopen_prim(lispval rocksdb)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  struct KNO_ROCKSDB *reopened = kno_reopen_rocksdb(db);
  if (reopened) {
    kno_incref(rocksdb);
    return rocksdb;}
  else return KNO_ERROR_VALUE;
}

/* Basic operations */

DEFC_PRIM("rocksdb/get",rocksdb_get_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval rocksdb_get_prim(lispval rocksdb,lispval key,lispval opts)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  char *errmsg = NULL;
  rocksdb_readoptions_t *readopts = get_read_options(db,opts);
  if (KNO_PACKETP(key)) {
    ssize_t binary_size;
    unsigned char *binary_data=
      rocksdb_get(db->dbptr,readopts,
		  KNO_PACKET_DATA(key),
		  KNO_PACKET_LENGTH(key),
		  &binary_size,&errmsg);
    if (readopts!=db->readopts)
      rocksdb_readoptions_destroy(readopts);
    if (binary_data) {
      lispval result = kno_bytes2packet(NULL,binary_size,binary_data);
      u8_free(binary_data);
      return result;}
    else if (errmsg)
      return kno_err(RocksDB_Error,"rocksdb_get_prim",errmsg,KNO_VOID);
    else return KNO_EMPTY_CHOICE;}
  else {
    struct KNO_OUTBUF keyout = { 0 };
    KNO_INIT_BYTE_OUTPUT(&keyout,1024);
    if (db_write_xtype(db,&keyout,key)>0) {
      lispval result = KNO_VOID;
      ssize_t binary_size;
      unsigned char *binary_data=
	rocksdb_get(db->dbptr,readopts,
		    keyout.buffer,
		    keyout.bufwrite-keyout.buffer,
		    &binary_size,&errmsg);
      kno_close_outbuf(&keyout);
      if (readopts!=db->readopts)
	rocksdb_readoptions_destroy(readopts);
      if (binary_data == NULL) {
	if (errmsg)
	  result = kno_err(RocksDB_Error,"rocksdb_get_prim",errmsg,KNO_VOID);
	else result = KNO_EMPTY_CHOICE;}
      else {
	struct KNO_INBUF valuein = { 0 };
	KNO_INIT_BYTE_INPUT(&valuein,binary_data,binary_size);
	result = kno_read_xtype(&valuein,&(db->xrefs));
	u8_free(binary_data);}
      return result;}
    else {
      if (readopts!=db->readopts)
	rocksdb_readoptions_destroy(readopts);
      return KNO_ERROR_VALUE;}}
}

DEFC_PRIM("rocksdb/put!",rocksdb_put_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3),
	  "**undocumented**",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"value",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval rocksdb_put_prim(lispval rocksdb,lispval key,lispval value,
				lispval opts)
{
  char *errmsg = NULL;
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  if ((KNO_PACKETP(key))&&(KNO_PACKETP(value))) {
    rocksdb_writeoptions_t *useopts = get_write_options(db,opts);
    rocksdb_writeoptions_t *writeopts = (useopts)?(useopts):(db->writeopts);
    rocksdb_put(db->dbptr,writeopts,
		KNO_PACKET_DATA(key),KNO_PACKET_LENGTH(key),
		KNO_PACKET_DATA(value),KNO_PACKET_LENGTH(value),
		&errmsg);
    if (useopts) rocksdb_writeoptions_destroy(useopts);
    if (errmsg)
      return kno_err(RocksDB_Error,"rocksdb_put_prim",errmsg,KNO_VOID);
    else return KNO_VOID;}
  else {
    struct KNO_OUTBUF keyout = { 0 };
    KNO_INIT_BYTE_OUTPUT(&keyout,1024);
    struct KNO_OUTBUF valout = { 0 };
    KNO_INIT_BYTE_OUTPUT(&valout,1024);
    if (db_write_xtype(db,&keyout,key)<0) {
      kno_close_outbuf(&keyout);
      kno_close_outbuf(&valout);
      return KNO_ERROR_VALUE;}
    else if (db_write_xtype(db,&valout,value)<0) {
      kno_close_outbuf(&keyout);
      kno_close_outbuf(&valout);
      return KNO_ERROR_VALUE;}
    else {
      rocksdb_writeoptions_t *useopts = get_write_options(db,opts);
      rocksdb_writeoptions_t *writeopts = (useopts)?(useopts):(db->writeopts);
      rocksdb_put(db->dbptr,writeopts,
		  keyout.buffer,keyout.bufwrite-keyout.buffer,
		  valout.buffer,valout.bufwrite-valout.buffer,
		  &errmsg);
      kno_close_outbuf(&keyout);
      kno_close_outbuf(&valout);
      if (useopts) rocksdb_writeoptions_destroy(useopts);
      if (errmsg)
	return kno_err(RocksDB_Error,"rocksdb_put_prim",errmsg,KNO_VOID);
      else return KNO_VOID;}}
}

DEFC_PRIM("rocksdb/drop!",rocksdb_drop_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval rocksdb_drop_prim(lispval rocksdb,lispval key,lispval opts)
{
  char *errmsg = NULL;
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  rocksdb_writeoptions_t *useopts = get_write_options(db,opts);
  rocksdb_writeoptions_t *writeopts = (useopts)?(useopts):(db->writeopts);
  if (KNO_PACKETP(key)) {
    rocksdb_delete(db->dbptr,writeopts,
		   KNO_PACKET_DATA(key),KNO_PACKET_LENGTH(key),
		   &errmsg);
    if (useopts) rocksdb_writeoptions_destroy(useopts);
    if (errmsg)
      return kno_err(RocksDB_Error,"rocksdb_put_prim",errmsg,KNO_VOID);
    else return KNO_VOID;}
  else {
    struct KNO_OUTBUF keyout = { 0 };
    KNO_INIT_BYTE_OUTPUT(&keyout,1024);
    if (db_write_xtype(db,&keyout,key)<0) {
      kno_close_outbuf(&keyout);
      if (useopts) rocksdb_writeoptions_destroy(useopts);
      return KNO_ERROR_VALUE;}
    else {
      rocksdb_delete(db->dbptr,writeopts,
		     keyout.buffer,keyout.bufwrite-keyout.buffer,
		     &errmsg);
      kno_close_outbuf(&keyout);
      if (useopts) rocksdb_writeoptions_destroy(useopts);
      if (errmsg)
	return kno_err(RocksDB_Error,"rocksdb_put_prim",errmsg,KNO_VOID);
      else return KNO_VOID;}}
}

static int cmp_keybufs(const void *vx,const void *vy)
{
  const struct ROCKSDB_KEYBUF *kbx = (struct ROCKSDB_KEYBUF *) vx;
  const struct ROCKSDB_KEYBUF *kby = (struct ROCKSDB_KEYBUF *) vy;
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

static struct ROCKSDB_KEYBUF *fetchn(struct KNO_ROCKSDB *db,int n,
				     struct ROCKSDB_KEYBUF *keys,
				     rocksdb_readoptions_t *readopts)
{
  struct ROCKSDB_KEYBUF *results = u8_alloc_n(n,struct ROCKSDB_KEYBUF);
  qsort(keys,n,sizeof(struct ROCKSDB_KEYBUF),cmp_keybufs);
  rocksdb_iterator_t *iterator = rocksdb_create_iterator(db->dbptr,readopts);
  int i = 0; while (i<n) {
    results[i].keypos = keys[i].keypos;
    results[i].dtype  = keys[i].dtype;
    rocksdb_iter_seek(iterator,keys[i].encoded.bytes,keys[i].encoded.n_bytes);
    results[i].encoded.bytes =
      rocksdb_iter_value(iterator,&(results[i].encoded.n_bytes));
    i++;}
  rocksdb_iter_destroy(iterator);
  return results;
}

DEFC_PRIM("rocksdb/getn",rocksdb_getn_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID},
	  {"keys",kno_vector_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval rocksdb_getn_prim(lispval rocksdb,lispval keys,lispval opts)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  rocksdb_readoptions_t *readopts = get_read_options(db,opts);
  int i = 0, n = KNO_VECTOR_LENGTH(keys);
  lispval results = kno_empty_vector(n);
  struct ROCKSDB_KEYBUF *keyvec = u8_alloc_n(n,struct ROCKSDB_KEYBUF);
  unsigned char buf[5000];
  struct KNO_OUTBUF out = { 0 };
  KNO_INIT_OUTBUF(&out,buf,5000,KNO_IS_WRITING);
  while (i<n) {
    lispval key = KNO_VECTOR_REF(keys,i);
    keyvec[i].keypos = i;
    if (KNO_PACKETP(key)) {
      keyvec[i].key   = key;
      keyvec[i].encoded.bytes   = KNO_PACKET_DATA(key);
      keyvec[i].encoded.n_bytes = KNO_PACKET_LENGTH(key);
      keyvec[i].dtype = 0;}
    else {
      size_t off = (out.bufwrite-out.buffer);
      ssize_t len = db_write_xtype(db,&out,key);
      if (len<0) {
	kno_decref(results);
	u8_free(keyvec);
	kno_close_outbuf(&out);
	return KNO_ERROR;}
      keyvec[i].key = key;
      keyvec[i].encoded.n_bytes = len;
      keyvec[i].encoded.bytes = (unsigned char *) off;
      keyvec[i].dtype = 1;}
    i++;}
  /* Convert offsets to byte pointers */
  unsigned char *keybuf = out.buffer;
  i = 0; while (i<n) {
    if (keyvec[i].dtype) {
      size_t off = (size_t) keyvec[i].encoded.bytes;
      keyvec[i].encoded.bytes = keybuf+off;}
    i++;}
  struct ROCKSDB_KEYBUF *values = fetchn(db,n,keyvec,readopts);
  i=0; while (i<n) {
    int keypos = values[i].keypos;
    if (values[i].dtype) {
      struct KNO_INBUF in = { 0 };
      KNO_INIT_INBUF(&in,values[i].encoded.bytes,
		     values[i].encoded.n_bytes,0);
      lispval v = kno_read_xtype(&in,&(db->xrefs));
      KNO_VECTOR_SET(results,keypos,v);}
    else {
      lispval v = kno_init_packet(NULL,values[i].encoded.n_bytes,
				  values[i].encoded.bytes);
      KNO_VECTOR_SET(results,keypos,v);}
    u8_free(values[i].encoded.bytes);
    i++;}
  kno_close_outbuf(&out);
  if (readopts!=db->readopts)
    rocksdb_readoptions_destroy(readopts);
  return results;
}

static struct BYTEVEC *write_prefix
(struct BYTEVEC *prefix,lispval key,struct KNO_ROCKSDB *db)
{
  struct KNO_OUTBUF keyout = { 0 };
  KNO_INIT_BYTE_OUTPUT(&keyout,1000);
  ssize_t n_bytes = db_write_xtype(db,&keyout,key);
  if (n_bytes < 0) {
    kno_close_outbuf(&keyout);
    return NULL;}
  else if ( n_bytes != (keyout.bufwrite-keyout.buffer) ) {
    u8_seterr("Inconsistent XTYPE size for key",
	      "rocksdb_scanner/write_prefix",NULL);
    return NULL;}
  else {
    prefix->bytes   = keyout.buffer;
    prefix->n_bytes = n_bytes;}
  return prefix;
}

static int rocksdb_scanner(struct KNO_ROCKSDB *db,lispval opts,
			   rocksdb_iterator_t *use_iterator,
			   lispval key,struct BYTEVEC *use_prefix,
			   rocksdb_prefix_iterfn iterfn,
			   void *state)
{
  rocksdb_readoptions_t *readopts = get_read_options(db,opts);
  struct XTYPE_REFS *xrefs = &(db->xrefs);
  struct BYTEVEC _prefix = {0}, *prefix;
  int rv = 0, free_prefix=0;
  if (use_prefix)
    prefix=use_prefix;
  else if (KNO_PACKETP(key)) {
    _prefix.bytes =   KNO_PACKET_DATA(key);
    _prefix.n_bytes = KNO_PACKET_LENGTH(key);
    prefix = &_prefix;}
  else {
    prefix = write_prefix(&_prefix,key,db);
    free_prefix=1;}
  if (prefix == NULL) return -1;
  rocksdb_iterator_t *iterator = (use_iterator) ? (use_iterator) :
    (rocksdb_create_iterator(db->dbptr,readopts));
  rocksdb_iter_seek(iterator,prefix->bytes,prefix->n_bytes);
  while (rocksdb_iter_valid(iterator)) {
    struct BYTEVEC keybuf, valbuf;
    keybuf.bytes = rocksdb_iter_key(iterator,&(keybuf.n_bytes));
    if ( (keybuf.n_bytes >= prefix->n_bytes) &&
	 (memcmp(keybuf.bytes,prefix->bytes,prefix->n_bytes) == 0) ) {
      valbuf.bytes = rocksdb_iter_value(iterator,&(valbuf.n_bytes));
      rv = iterfn(db,key,prefix,&keybuf,&valbuf,state);
      if (rv <= 0) break;}
    else break;
    rocksdb_iter_next(iterator);}
  if (free_prefix) u8_free(prefix->bytes);
  if (use_iterator == NULL) rocksdb_iter_destroy(iterator);
  return rv;
}

static int rocksdb_key_scanner(struct KNO_ROCKSDB *db,lispval opts,
			       rocksdb_iterator_t *use_iterator,
			       lispval key,struct BYTEVEC *use_prefix,
			       rocksdb_prefix_key_iterfn iterfn,
			       void *state)
{
  rocksdb_readoptions_t *readopts = get_read_options(db,opts);
  struct BYTEVEC _prefix = {0}, *prefix;
  xtype_refs xrefs = &(db->xrefs);
  int rv = 0, free_prefix=0;
  if (use_prefix)
    prefix=use_prefix;
  else if (KNO_PACKETP(key)) {
    _prefix.bytes =   KNO_PACKET_DATA(key);
    _prefix.n_bytes = KNO_PACKET_LENGTH(key);
    prefix = &_prefix;}
  else {
    prefix = write_prefix(&_prefix,key,db);
    free_prefix=1;}
  if (prefix == NULL) return -1;
  rocksdb_iterator_t *iterator = (use_iterator) ? (use_iterator) :
    (rocksdb_create_iterator(db->dbptr,readopts));
  rocksdb_iter_seek(iterator,prefix->bytes,prefix->n_bytes);
  while (rocksdb_iter_valid(iterator)) {
    struct BYTEVEC keybuf;
    keybuf.bytes = rocksdb_iter_key(iterator,&(keybuf.n_bytes));
    if ( (keybuf.n_bytes >= prefix->n_bytes) &&
	 (memcmp(keybuf.bytes,prefix->bytes,prefix->n_bytes) == 0) ) {
      rv = iterfn(db,key,prefix,&keybuf,state);
      /* u8_free(keybuf.bytes); u8_free(valbuf.bytes); */
      if (rv <= 0) break;}
    else {
      /* u8_free(keybuf.bytes); */
      break;}
    rocksdb_iter_next(iterator);}
  if (free_prefix) u8_free(prefix->bytes);
  if (use_iterator == NULL) rocksdb_iter_destroy(iterator);
  return rv;
}

static int rocksdb_editor(struct KNO_ROCKSDB *db,lispval opts,
			  rocksdb_iterator_t *use_iterator,
			  rocksdb_writebatch_t *batch,
			  lispval key,struct BYTEVEC *use_prefix,
			  struct BYTEVEC *value)
{
  rocksdb_readoptions_t *readopts = get_read_options(db,opts);
  struct BYTEVEC _prefix = { 0 }, *prefix;
  int rv = 0, free_prefix=0;
  if (use_prefix)
    prefix=use_prefix;
  else if (KNO_PACKETP(key)) {
    _prefix.bytes =   KNO_PACKET_DATA(key);
    _prefix.n_bytes = KNO_PACKET_LENGTH(key);
    prefix = &_prefix;}
  else {
    prefix = write_prefix(&_prefix,key,db);
    free_prefix=1;}
  if (prefix == NULL) return -1;
  rocksdb_iterator_t *iterator = (use_iterator) ? (use_iterator) :
    (rocksdb_create_iterator(db->dbptr,readopts));
  rocksdb_iter_seek(iterator,prefix->bytes,prefix->n_bytes);
  while (rocksdb_iter_valid(iterator)) {
    struct BYTEVEC keybuf;
    keybuf.bytes = rocksdb_iter_key(iterator,&(keybuf.n_bytes));
    if ( (keybuf.n_bytes >= prefix->n_bytes) &&
	 (memcmp(keybuf.bytes,prefix->bytes,prefix->n_bytes) == 0) ) {
      if (keybuf.n_bytes == prefix->n_bytes)
	rocksdb_writebatch_put
	  (batch,prefix->bytes,prefix->n_bytes,value->bytes,value->n_bytes);
      else rocksdb_writebatch_delete(batch,keybuf.bytes,keybuf.n_bytes);}
    else {
      /* u8_free(keybuf.bytes); */
      break;}
    rocksdb_iter_next(iterator);}
  if (free_prefix) u8_free(prefix->bytes);
  if (use_iterator == NULL) rocksdb_iter_destroy(iterator);
  return rv;
}

/* Prefix operations */

static int prefix_get_iterfn(struct KNO_ROCKSDB *db,lispval key,
			     struct BYTEVEC *prefix,
			     struct BYTEVEC *keybuf,
			     struct BYTEVEC *valbuf,
			     void *vptr)
{
  lispval *results = (lispval *) vptr;
  if (valbuf->n_bytes == 0) return 1;
  lispval key_packet = kno_make_packet(NULL,keybuf->n_bytes,keybuf->bytes);
  lispval val_packet = kno_make_packet(NULL,valbuf->n_bytes,valbuf->bytes);
  lispval pair = kno_init_pair(NULL,key_packet,val_packet);
  KNO_ADD_TO_CHOICE(*results,pair);
  return 1;
}

DEFC_PRIM("rocksdb/prefix/get",rocksdb_prefix_get_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "(ROCKSDB/PREFIX/GET *db* *key* [*opts*]) "
	  "returns all the key/value pairs (as packets), "
	  "whose keys begin with the DTYPE representation of "
	  "*key*.",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval rocksdb_prefix_get_prim(lispval rocksdb,lispval key,lispval opts)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  lispval results = KNO_EMPTY;
  int rv = rocksdb_scanner(db,opts,NULL,key,NULL,prefix_get_iterfn,&results);
  if (rv<0) {
    kno_decref(results);
    return KNO_ERROR;}
  else if (kno_testopt(opts,KNOSYM(vector),KNO_VOID))
    return kno_results2vector(results,1);
  else return results;
}

DEFC_PRIM("rocksdb/prefix/getn",rocksdb_prefix_getn_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "(ROCKSDB/PREFIX/GET *db* *key* [*opts*]) "
	  "returns all the key/value pairs (as packets), "
	  "whose keys begin with the DTYPE representation of "
	  "*key*.",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID},
	  {"keys",kno_vector_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval rocksdb_prefix_getn_prim(lispval rocksdb,lispval keys,lispval opts)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  rocksdb_readoptions_t *readopts = get_read_options(db,opts);
  int n_keys = KNO_VECTOR_LENGTH(keys);
  lispval result = kno_make_vector(n_keys,NULL);
  lispval *elts = KNO_VECTOR_ELTS(result);
  struct ROCKSDB_KEYBUF *prefixes = u8_alloc_n(n_keys,struct ROCKSDB_KEYBUF);
  int i=0; while (i<n_keys) {
    lispval key = KNO_VECTOR_REF(keys,i);
    struct KNO_OUTBUF keyout = { 0 };
    KNO_INIT_BYTE_OUTPUT(&keyout,512);
    ssize_t rv = db_write_xtype(db,&keyout,key);
    if (rv<0) {
      kno_decref(result);
      int j = 0; while (j<0) {
	u8_free(prefixes[j].encoded.bytes);
	j++;}
      u8_free(prefixes);
      return KNO_ERROR;}
    prefixes[i].key = key;
    prefixes[i].keypos = i;
    prefixes[i].encoded.n_bytes = keyout.bufwrite-keyout.buffer;
    prefixes[i].encoded.bytes =   keyout.buffer;
    i++;}
  qsort(prefixes,n_keys,sizeof(struct ROCKSDB_KEYBUF),cmp_keybufs);
  rocksdb_iterator_t *iterator = rocksdb_create_iterator(db->dbptr,readopts);
  i=0; while (i < n_keys) {
    lispval key = prefixes[i].key;
    int pos = prefixes[i].keypos;
    KNO_VECTOR_SET(result,pos,KNO_EMPTY);
    int rv = rocksdb_scanner(db,opts,iterator,key,NULL,
			     prefix_get_iterfn,&(elts[pos]));
    if (rv<0) {
      int j=0; while (j < n_keys) {
	u8_free(prefixes[j].encoded.bytes); j++;}
      kno_decref(result);
      u8_free(prefixes);
      if (readopts!=db->readopts) rocksdb_readoptions_destroy(readopts);
      return KNO_ERROR;}
    i++;}
  if (readopts!=db->readopts) rocksdb_readoptions_destroy(readopts);
  rocksdb_iter_destroy(iterator);
  if (kno_testopt(opts,KNOSYM(vector),KNO_VOID))
    return kno_results2vector(result,1);
  else return result;
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

static int index_get_iterfn(struct KNO_ROCKSDB *db,lispval key,
			    struct BYTEVEC *prefix,
			    struct BYTEVEC *keybuf,
			    struct BYTEVEC *valbuf,
			    void *vptr)
{
  struct KEY_VALUES *values = vptr;
  if (valbuf->n_bytes == 0) return 1;
  if (valbuf->bytes[0] == ROCKSDB_META_KEY) {
    return 1;}
  else {
    struct KNO_INBUF in = { 0 };
    KNO_INIT_BYTE_INPUT(&in,valbuf->bytes,valbuf->n_bytes);
    lispval v = kno_read_xtype(&in,&(db->xrefs));
    if (KNO_ABORTP(v))
      return -1;
    if (KNO_EMPTY_CHOICEP(v)) return 1;
    if (KNO_CHOICEP(v)) {
      int rv = accumulate_values(values,KNO_CHOICE_SIZE(v),KNO_CHOICE_DATA(v));
      struct KNO_CHOICE *ch = (kno_choice) v;
      /* The elements are now pointed to by *values, so we just free the choice
	 object */
      u8_free(ch);
      return rv;}
    else return accumulate_values(values,1,&v);}
}


DEFC_PRIM("rocksdb/index/get",rocksdb_index_get_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "(ROCKSDB/INDEX/GET *db* *key* [*opts*]) "
	  "gets values associated with *key* in *db*, using "
	  "the rocksdb database as an index and options "
	  "provided in *opts*.",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval rocksdb_index_get_prim(lispval rocksdb,lispval key,lispval opts)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  struct KEY_VALUES accumulator = {0};
  int rv = rocksdb_scanner(db,opts,NULL,key,NULL,index_get_iterfn,&accumulator);
  if (rv<0) {
    kno_decref_elts(accumulator.vec,accumulator.used);
    u8_free(accumulator.vec);
    return KNO_ERROR_VALUE;}
  else return kno_init_choice(NULL,accumulator.used,accumulator.vec,-1);
}

static ssize_t rocksdb_add_helper(struct KNO_ROCKSDB *db,
				  rocksdb_writebatch_t *batch,
				  lispval key,lispval values,
				  lispval opts)
{
  struct KNO_OUTBUF keyout= { 0 }, valout = { 0 }, countbuf = { 0 };
  struct BYTEVEC keybuf = {0}, valbuf = {0};
  rocksdb_readoptions_t *readopts = get_read_options(db,opts);
  char *errmsg=NULL;
  long long n_vals=0, n_blocks=0, header_type=0;
  ssize_t rv = 0;
  KNO_INIT_BYTE_OUTPUT(&keyout,1000);
  rv=db_write_xtype(db,&keyout,key);
  if (rv<0) return rv;
  KNO_INIT_BYTE_OUTPUT(&valout,1000);
  keybuf.n_bytes = keyout.bufwrite-keyout.buffer;
  keybuf.bytes   = keyout.buffer;
  valbuf.bytes   = rocksdb_get
    (db->dbptr,readopts,keybuf.bytes,keybuf.n_bytes,&valbuf.n_bytes,&errmsg);
  if (readopts != db->readopts) rocksdb_readoptions_destroy(readopts);
  if (errmsg) {
    kno_seterr(RocksDB_Error,"rocksdb_add_helper",errmsg,KNO_VOID);
    kno_close_outbuf(&keyout);
    kno_close_outbuf(&valout);
    return -1;}
  if (valbuf.bytes == NULL) {
    rv = db_write_xtype(db,&valout,values);
    if (rv >= 0)
      rocksdb_writebatch_put
	(batch,keybuf.bytes,keybuf.n_bytes,
	 valout.buffer,valout.bufwrite-valout.buffer);
    kno_close_outbuf(&valout);
    kno_close_outbuf(&keyout);
    if (rv<0) return rv;
    else return KNO_CHOICE_SIZE(values);}
  struct KNO_INBUF curbuf = { 0 };
  KNO_INIT_INBUF(&curbuf,valbuf.bytes,valbuf.n_bytes,KNO_STATIC_BUFFER);
  if (valbuf.bytes[0] == ROCKSDB_KEYINFO) {
    kno_read_byte(&curbuf); /* Skip ROCKSDB_KEYINFO code */
    n_blocks = kno_read_varint(&curbuf);
    n_vals   = kno_read_varint(&curbuf);
    if ( (n_blocks<0) || (n_vals<0) )
      rv = -1;
    else {
      if (KNO_CHOICEP(values))
	n_vals += KNO_CHOICE_SIZE(values);
      else n_vals++;
      n_blocks++;
      rv = db_write_xtype(db,&valout,values);}
    if (rv<0) n_vals=-1;}
  else {
    lispval combined = kno_read_xtype(&curbuf,&(db->xrefs));
    if (KNO_ABORTP(combined)) rv=-1;
    else {
      kno_incref(values);
      KNO_ADD_TO_CHOICE(combined,values);
      combined = kno_simplify_choice(combined);
      n_vals = KNO_CHOICE_SIZE(combined);
      n_blocks = 1;
      rv = db_write_xtype(db,&valout,combined);
      kno_decref(combined);}}
  if (rv>=0) {
    unsigned char count_bytes[64];
    KNO_INIT_OUTBUF(&countbuf,count_bytes,64,0);
    if (rv>=0) rv = kno_write_byte(&countbuf,ROCKSDB_KEYINFO);
    if (rv>=0) rv = kno_write_varint((&countbuf),n_blocks);
    if (rv>=0) rv = kno_write_varint((&countbuf),n_vals);
    if (rv>=0) {
      rocksdb_writebatch_put
	(batch,keybuf.bytes,keybuf.n_bytes,
	 countbuf.buffer,countbuf.bufwrite-countbuf.buffer);
      kno_write_8bytes(&keyout,n_blocks);
      rocksdb_writebatch_put
	(batch,keyout.buffer,keyout.bufwrite-keyout.buffer,
	 valout.buffer,valout.bufwrite-valout.buffer);}
    kno_close_outbuf(&countbuf);}
  kno_close_inbuf(&curbuf);
  kno_close_outbuf(&keyout);
  kno_close_outbuf(&valout);
  u8_free(valbuf.bytes);
  if (rv<0) return -1;
  else return n_vals;
}

static ssize_t rocksdb_adder(struct KNO_ROCKSDB *db,lispval key,
			     lispval values,lispval opts)
{
  rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
  ssize_t added = rocksdb_add_helper(db,batch,key,values,opts);
  if (added<0) return added;
  char *errmsg = NULL;
  rocksdb_write(db->dbptr,sync_writeopts,batch,&errmsg);
  if (errmsg)
    kno_seterr(RocksDB_Error,"rocksdb_add_helper",errmsg,KNO_VOID);
  rocksdb_writebatch_destroy(batch);
  if (errmsg)
    return -1;
  else return added;
}



DEFC_PRIM("rocksdb/index/add!",rocksdb_index_add_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3),
	  "(ROCKSDB/INDEX/ADD! *db* *key* *value [*opts*]) "
	  "Saves *values* in *db*, associating them with "
	  "*key* and using the rocksdb database as an index "
	  "with options provided in *opts*.",
	  {"rocksdb",KNO_ROCKSDB_TYPE,KNO_VOID},
	  {"key",kno_any_type,KNO_VOID},
	  {"values",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval rocksdb_index_add_prim(lispval rocksdb,lispval key,
				      lispval values,lispval opts)
{
  struct KNO_ROCKSDB *db = (kno_rocksdb)rocksdb;
  ssize_t rv = rocksdb_adder(db,key,values,opts);
  if (rv<0)
    return KNO_ERROR_VALUE;
  else return KNO_INT(rv);
}

/* Rocksdb indexes */

/* Indexes use rocksdb in a particular way, based on prefixes to the
   stored keys and values.

*/

static struct KNO_INDEX_HANDLER rocksdb_index_handler;

kno_index kno_open_rocksdb_index(u8_string path,kno_storage_flags flags,lispval opts)
{
  struct KNO_ROCKSDB *rocksdb = kno_open_rocksdb(path,opts);
  if (rocksdb==NULL) return NULL;
  struct KNO_ROCKSDB_INDEX *index = u8_alloc(struct KNO_ROCKSDB_INDEX);
  rocksdb_t *dbptr = rocksdb->dbptr;
  u8_string realpath = u8_realpath(path,NULL);
  u8_string abspath = u8_abspath(path,NULL);
  lispval label = get_prop(dbptr,"\377LABEL",KNO_VOID);
  lispval metadata = get_prop(dbptr,"\377METADATA",KNO_VOID);
  lispval slotcodes = get_prop(dbptr,"\377SLOTIDS",KNO_VOID);
  lispval oidcodes = get_prop(dbptr,"\377BASEOIDS",KNO_VOID);

  kno_init_index((kno_index)index,
		 &rocksdb_index_handler,
		 (KNO_STRINGP(label)) ? (KNO_CSTRING(label)) : (path),
		 abspath,realpath,
		 -1,metadata,opts);
  u8_free(realpath); u8_free(abspath);
  index->rocksdb = rocksdb;

  if (KNO_VOIDP(metadata)) {}
  else if (KNO_SLOTMAPP(metadata)) {}
  else u8_log(LOG_WARN,"Rocksdb/Index/BadMetadata",
	      "Bad metadata for RocksDB index %s: %q",path,metadata);

  kno_decref(label);
  kno_decref(metadata);
  kno_decref(slotcodes);
  kno_decref(oidcodes);
  if (kno_testopt(opts,SYM("READONLY"),KNO_VOID))
    index->index_flags |= KNO_STORAGE_READ_ONLY;
  index->index_flags = flags;
  kno_register_index((kno_index)index);
  return (kno_index)index;
}

KNO_EXPORT
kno_index kno_make_rocksdb_index(u8_string path,lispval opts)
{
  struct KNO_ROCKSDB_INDEX *index = u8_alloc(struct KNO_ROCKSDB_INDEX);
  lispval label = kno_getopt(opts,SYM("LABEL"),KNO_VOID);
  lispval metadata = kno_getopt(opts,SYM("METADATA"),KNO_VOID);
  lispval slotids = kno_getopt(opts,SYM("SLOTIDS"),KNO_VOID);
  lispval baseoids = kno_getopt(opts,SYM("BASEOIDS"),KNO_VOID);
  lispval keyslot = kno_getopt(opts,KNOSYM_KEYSLOT,KNO_VOID);

  if ( (KNO_VOIDP(keyslot)) || (KNO_FALSEP(keyslot)) ) {}
  else if ( (KNO_SYMBOLP(keyslot)) || (KNO_OIDP(keyslot)) ) {
    if (KNO_SLOTMAPP(metadata))
      kno_store(metadata,KNOSYM_KEYSLOT,keyslot);
    else {
      metadata = kno_empty_slotmap();
      kno_store(metadata,KNOSYM_KEYSLOT,keyslot);}}
  else u8_log(LOG_WARN,"InvalidKeySlot",
	      "Not initializing keyslot of %s to %q",path,keyslot);

  struct KNO_ROCKSDB *rocksdb = kno_open_rocksdb(path,opts);
  
  rocksdb_t *dbptr = rocksdb->dbptr;
  lispval cur_label = get_prop(dbptr,"\377LABEL",KNO_VOID);
  u8_string realpath = u8_realpath(path,NULL);
  u8_string abspath = u8_abspath(path,NULL);
  if (!(KNO_VOIDP(label))) {
    if (!(KNO_EQUALP(label,cur_label)))
      set_prop(dbptr,"\377LABEL",label,sync_writeopts);}
  if (KNO_VOIDP(metadata)) {}
  else if (KNO_SLOTMAPP(metadata))
    set_prop(dbptr,"\377METADATA",metadata,sync_writeopts);
  else u8_log(LOG_WARN,"Rocksdb/Index/InvalidMetadata",
	      "For %s: %q",path,metadata);

  kno_init_index((kno_index)index,
		 &rocksdb_index_handler,
		 (KNO_STRINGP(label)) ? (KNO_CSTRING(label)) : (abspath),
		 abspath,realpath,
		 -1,metadata,opts);
  u8_free(abspath); u8_free(realpath);
  index->rocksdb = rocksdb;

  index->index_flags = kno_get_dbflags(opts,KNO_STORAGE_ISINDEX);
  index->index_flags &= ~KNO_STORAGE_READ_ONLY;

  kno_decref(metadata);
  kno_decref(baseoids);
  kno_decref(slotids);

  kno_register_index((kno_index)index);

  return (kno_index)index;
}

/* index driver methods */

struct ROCKSDB_INDEX_RESULTS {
  lispval results;
  kno_index index;
  struct KNO_OIDCODER *oidcodes;};
struct ROCKSDB_INDEX_COUNT {
  ssize_t count;
  kno_index index;
  struct KNO_OIDCODER *oidcodes;};

static int rocksdb_index_gather(struct KNO_ROCKSDB *db,lispval key,
				struct BYTEVEC *prefix,
				struct BYTEVEC *keybuf,
				struct BYTEVEC *valbuf,
				void *vptr)
{
  struct ROCKSDB_INDEX_RESULTS *state = vptr;
  if (valbuf->n_bytes == 0)
    return 1;
  else if (valbuf->bytes[0] == ROCKSDB_KEYINFO)
    return 1;
  else {
    struct KNO_INBUF valstream = { 0 };
    KNO_INIT_BYTE_INPUT(&valstream,valbuf->bytes,valbuf->n_bytes);
    lispval value = kno_read_xtype(&valstream,&(db->xrefs));
    if (KNO_ABORTP(value))
      return -1;
    else {
      KNO_ADD_TO_CHOICE((state->results),value);
      return 1;}}
}

static int rocksdb_index_count(struct KNO_ROCKSDB *db,lispval key,
			       struct BYTEVEC *prefix,
			       struct BYTEVEC *keybuf,
			       struct BYTEVEC *valbuf,
			       void *vptr)
{
  struct ROCKSDB_INDEX_COUNT *state = vptr;
  if (valbuf->n_bytes == 0) return 1;
  struct KNO_INBUF valstream = { 0 };
  KNO_INIT_BYTE_INPUT(&valstream,valbuf->bytes,valbuf->n_bytes);
  if ( (keybuf->n_bytes == prefix->n_bytes) &&
       (valbuf->bytes[0] == 0xFF) ) {
    long long header_type = kno_read_4bytes(&valstream);
    if (header_type != 1) {
      kno_seterr("BadLeveDBIndexHeader","rocksdb_index_count",
		 state->index->indexid,key);
      return -1;}
    U8_MAYBE_UNUSED ssize_t n_blocks = kno_read_8bytes(&valstream);
    ssize_t n_keys = kno_read_8bytes(&valstream);
    if (n_keys>=0) {
      state->count=n_keys;
      return 0;}
    else {
      kno_seterr("BadLeveDBIndexHeader","rocksdb_index_count",
		 state->index->indexid,key);
      return -1;}}
  else {
    lispval values = kno_read_xtype(&valstream,&(db->xrefs));
    if (KNO_ABORTP(values)) return -1;
    else state->count += KNO_CHOICE_SIZE(values);
    kno_decref(values);
    return 1;}
}

static lispval rocksdb_index_fetch(kno_index ix,lispval key)
{
  struct KNO_ROCKSDB_INDEX *lx = (kno_rocksdb_index)ix;
  struct KNO_ROCKSDB *db = lx->rocksdb;
  KNO_DECL_OUTBUF(keybuf,1000);
  ssize_t len = db_write_xtype(db,&keybuf,key);
  if (len<0) {
    kno_close_outbuf(&keybuf);
    return KNO_ERROR_VALUE;}
  else if (len == 0) {
    /* This means the key is a pair whose CAR doesn't have a slot
       code, so its not in the index at all */
    kno_close_outbuf(&keybuf);
    return KNO_EMPTY;}
  else {
    struct ROCKSDB_INDEX_RESULTS state = { KNO_EMPTY, ix, NULL };
    struct BYTEVEC prefix = { keybuf.bufwrite-keybuf.buffer, keybuf.buffer };
    int rv = rocksdb_scanner(db,lx->index_opts,NULL,key,&prefix,
			     rocksdb_index_gather,&state);
    kno_close_outbuf(&keybuf);
    if (rv<0)
      return KNO_ERROR_VALUE;
    else return state.results;}
}

static int rocksdb_index_fetchsize(kno_index ix,lispval key)
{
  struct KNO_ROCKSDB_INDEX *lx = (kno_rocksdb_index)ix;
  struct KNO_ROCKSDB *db = lx->rocksdb;
  KNO_DECL_OUTBUF(keybuf,1000);
  ssize_t len = kno_write_xtype(&keybuf,key,&(db->xrefs));
  if (len < 0) {
    kno_close_outbuf(&keybuf);
    return -1;}
  else if (len == 0) {
    /* This means the key is a pair whose CAR doesn't have a slot
       code. */
    return 0;}
  else {
    struct ROCKSDB_INDEX_COUNT state = { 0, ix, NULL };
    struct BYTEVEC prefix = { keybuf.bufwrite-keybuf.buffer, keybuf.buffer };
    int rv = rocksdb_scanner(db,lx->index_opts,NULL,key,&prefix,
			     rocksdb_index_count,&state);
    if (rv<0)
      return rv;
    else return state.count;}
}

static lispval *rocksdb_index_fetchn(kno_index ix,int n,const lispval *keys)
{
  struct KNO_ROCKSDB_INDEX *lx = (kno_rocksdb_index)ix;
  struct KNO_ROCKSDB *db = lx->rocksdb;
  struct ROCKSDB_KEYBUF *keybufs = u8_alloc_n(n,struct ROCKSDB_KEYBUF);
  struct KNO_OUTBUF keyreps = { 0 };
  /* Pre-allocated for 60 bytes per key */
  KNO_INIT_BYTE_OUTPUT(&keyreps,(n*60));
  struct ROCKSDB_INDEX_RESULTS *states =
    u8_alloc_n(n,struct ROCKSDB_INDEX_RESULTS);
  int i=0, to_fetch=0;
  while (i<n) {
    lispval key = keys[i];
    size_t offset = keyreps.bufwrite - keyreps.buffer;
    ssize_t key_len = kno_write_xtype(&keyreps,key,&(db->xrefs));
    if (key_len < 0) {
      kno_close_outbuf(&keyreps);
      u8_free(states);
      return NULL;}
    states[i].results=KNO_EMPTY;
    states[i].index=ix;
    states[i].oidcodes=NULL;
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
  qsort(keybufs,n,sizeof(struct ROCKSDB_KEYBUF),cmp_keybufs);

  rocksdb_iterator_t *iterator = rocksdb_create_iterator(db->dbptr,db->readopts);
  i=0; while (i<n) {
    int keypos = keybufs[i].keypos;
    int rv = rocksdb_scanner(db,lx->index_opts,iterator,
			     keybufs[i].key,
			     & keybufs[i].encoded,
			     rocksdb_index_gather,
			     & states[keypos]);
    if (rv<0) {
      u8_free(keybufs);
      kno_close_outbuf(&keyreps);
      return NULL;}
    i++;}

  rocksdb_iter_destroy(iterator);

  lispval *results = u8_big_alloc_n(n,lispval);
  i=0; while (i<n) {
    results[i] = states[i].results;
    i++;}

  u8_free(keybufs);
  kno_close_outbuf(&keyreps);
  u8_free(states);

  return results;
}

static int rocksdb_index_save(kno_rocksdb_index lx,
			      struct KNO_INDEX_COMMITS *commits)
{
  struct KNO_ROCKSDB *db = lx->rocksdb;
  rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
  ssize_t n_stores = commits->commit_n_stores;
  ssize_t n_drops = commits->commit_n_drops;
  int err = 0;
  if (n_stores+n_drops) {
    struct KNO_CONST_KEYVAL *stores = commits->commit_stores;
    size_t store_i = 0;
    rocksdb_iterator_t *iterator =
      rocksdb_create_iterator(db->dbptr,db->readopts);
    KNO_DECL_OUTBUF(keybuf,100);
    KNO_DECL_OUTBUF(valbuf,1000);
    if (n_stores) {
      while (store_i < n_stores) {
	lispval key = stores[store_i].kv_key;
	lispval val = stores[store_i].kv_val;
	ssize_t key_rv = db_write_xtype(db,&keybuf,key);
	ssize_t val_rv = db_write_xtype(db,&valbuf,val);
	if ( (key_rv<0) || (val_rv<0) ) {err=-1; break;}
	struct BYTEVEC keyv = { keybuf.bufwrite-keybuf.buffer, keybuf.buffer };
	struct BYTEVEC valv = { valbuf.bufwrite-valbuf.buffer, valbuf.buffer };
	int edit_rv = rocksdb_editor(db,lx->index_opts,iterator,batch,
				     key,&keyv,&valv);
	if (edit_rv<0) {err=-1; break;}
	keybuf.bufwrite = keybuf.buffer;
	valbuf.bufwrite = valbuf.buffer;
	store_i++;}}
    if ( (n_drops) && (err==0) ) {
      struct KNO_CONST_KEYVAL *drops = commits->commit_drops;
      lispval *drop_keys = u8_alloc_n(n_drops,lispval);
      int drop_i = 0; while (drop_i<n_drops) {
	drop_keys[drop_i] = drops[drop_i].kv_key;
	drop_i++;}
      lispval *drop_vals = rocksdb_index_fetchn((kno_index)lx,n_drops,drop_keys);
      if (drop_vals) {
	drop_i = 0; while (drop_i < n_drops) {
	  lispval key = drop_keys[drop_i];
	  lispval cur = drop_vals[drop_i];
	  lispval drop = drops[drop_i].kv_val;
	  lispval diff = kno_difference(cur,drop);
	  ssize_t key_rv = db_write_xtype(db,&keybuf,key);
	  if (key_rv<0) {}
	  struct BYTEVEC keyv = { keybuf.bufwrite-keybuf.buffer, keybuf.buffer };
	  struct BYTEVEC valv = { 0 };
	  if (!(KNO_EMPTYP(diff))) {
	    ssize_t val_rv = db_write_xtype(db,&valbuf,diff);
	    if (val_rv<0) {}
	    valv.n_bytes = valbuf.bufwrite-valbuf.buffer;
	    valv.bytes = valbuf.buffer;
	    kno_decref(diff);}
	  int edit_rv = rocksdb_editor(db,lx->index_opts,iterator,batch,
				       key,&keyv,&valv);
	  if (edit_rv<0) {}
	  keybuf.bufwrite = keybuf.buffer;
	  valbuf.bufwrite = valbuf.buffer;
	  drop_i++;}}
      u8_free(drop_keys);
      if (drop_vals) {
	kno_decref_elts(drop_vals,n_drops);
	u8_big_free(drop_vals);}
      else err=-1;}
    rocksdb_iter_destroy(iterator);
    kno_close_outbuf(&keybuf);
    kno_close_outbuf(&valbuf);}

  if (err) return -1;

  struct KNO_CONST_KEYVAL *adds = commits->commit_adds;
  ssize_t n_adds = commits->commit_n_adds;
  size_t i = 0; while (i<n_adds) {
    int rv = rocksdb_add_helper(db,batch,
				adds[i].kv_key,adds[i].kv_val,
				lx->index_opts);
    if (rv<0) {
      rocksdb_writebatch_destroy(batch);
      return -1;}
    else i++;}

  char *errmsg = NULL;
  rocksdb_write(db->dbptr,sync_writeopts,batch,&errmsg);

  rocksdb_writebatch_destroy(batch);

  if (errmsg)
    return -1;
  else return n_adds+n_stores+n_drops;
}

static int rocksdb_index_commit(kno_index ix,kno_commit_phase phase,
				struct KNO_INDEX_COMMITS *commits)
{
  struct KNO_ROCKSDB_INDEX *lx = (kno_rocksdb_index)ix;
  switch (phase) {
  case kno_no_commit:
    u8_seterr("BadCommitPhase(commit_none)","hashindex_commit",
	      u8_strdup(ix->indexid));
    return -1;
  case kno_commit_start: {
    return 1;}
  case kno_commit_write: {
    return rocksdb_index_save(lx,commits);}
  case kno_commit_rollback: {
    return 1;}
  case kno_commit_sync: {
    return 1;}
  case kno_commit_cleanup: {
    return 1;}
  default: {
    u8_logf(LOG_WARN,"NoPhasedCommit",
	    "The index %s doesn't support phased commits",
	    ix->indexid);
    return -1;}
  }
}

static lispval *rocksdb_index_fetchkeys(kno_index ix,int *nptr)
{
  struct KNO_ROCKSDB_INDEX *lx = (kno_rocksdb_index)ix;
  lispval results = KNO_EMPTY_CHOICE;
  rocksdb_iterator_t *iterator=
    rocksdb_create_iterator(lx->rocksdb->dbptr,lx->rocksdb->readopts);
  unsigned char *prefix = NULL; ssize_t prefix_len = -1;
  struct BYTEVEC keybuf;
  rocksdb_iter_seek(iterator,NULL,0);
  if (rocksdb_iter_valid(iterator))
    keybuf.bytes = rocksdb_iter_key(iterator,&(keybuf.n_bytes));
  else {
    *nptr = 0;
    return NULL;}
  while (rocksdb_iter_valid(iterator)) {
    struct KNO_INBUF keystream = { 0 };
    if ( (keybuf.n_bytes) && ( (keybuf.bytes[0]<0x80) || (keybuf.bytes[0]>=0xF0) ) ) {
      /* Skip metadata, OID, or miscellaneous field (invalid xtype prefix) */
      rocksdb_iter_next(iterator);
      if (rocksdb_iter_valid(iterator))
	keybuf.bytes = rocksdb_iter_key(iterator,&(keybuf.n_bytes));
      continue;}
    /* We've got a key entry and it's also the first entry for the key, 
       so we add it to the results and skip all the RocksDB keys with the
       same prefix (they're additional values) */
    if (prefix) {u8_free(prefix); prefix=NULL;}
    KNO_INIT_BYTE_INPUT(&keystream,keybuf.bytes,keybuf.n_bytes);
    lispval key = kno_read_xtype(&keystream,&(lx->rocksdb->xrefs));
    if (KNO_ABORTP(key)) {
      rocksdb_iter_destroy(iterator);
      kno_close_inbuf(&keystream);
      return NULL;}
    else {KNO_ADD_TO_CHOICE(results,key);}
    prefix = u8_memdup(keybuf.n_bytes,keybuf.bytes);
    prefix_len = keybuf.n_bytes;
    /* Now we skip all the value entries for the key */
    rocksdb_iter_next(iterator);
    while (rocksdb_iter_valid(iterator)) {
      keybuf.bytes = rocksdb_iter_key(iterator,&(keybuf.n_bytes));
      if (memcmp(keybuf.bytes,prefix,prefix_len) == 0)
	rocksdb_iter_next(iterator);
      else break;}}
  rocksdb_iter_destroy(iterator);
  if (prefix) u8_free(prefix);
  results = kno_simplify_choice(results);
  if (KNO_EMPTYP(results)) {
    *nptr = 0;
    return NULL;}
  else if (KNO_CHOICEP(results)) {
    int n = KNO_CHOICE_SIZE(results);
    if (KNO_CONS_REFCOUNT(results) == 1) {
      size_t n_bytes = SIZEOF_LISPVAL*n;
      lispval *keys = u8_big_alloc(n_bytes);
      memcpy(keys,KNO_CHOICE_ELTS(results),n_bytes);
      *nptr = n;
      kno_free_choice((kno_choice)results);
      return keys;}
    else {
      lispval *elts = (lispval *) KNO_CHOICE_ELTS(results);
      lispval *keys = u8_big_alloc_n(n,lispval);
      kno_copy_vec(elts,n,keys,0);
      *nptr = n;
      kno_decref(results);
      return keys;}}
  else {
    lispval *one = u8_big_alloc_n(1,lispval);
    *one = results;
    *nptr = 1;
    return one;}
}

static kno_index rocksdb_index_open(u8_string spec,kno_storage_flags flags,
				    lispval opts)
{
  return kno_open_rocksdb_index(spec,flags,opts);
}


static void rocksdb_index_close(kno_index ix)
{
  struct KNO_ROCKSDB_INDEX *ldbx = (struct KNO_ROCKSDB_INDEX *)ix;
}

static kno_index rocksdb_index_create(u8_string spec,void *typedata,
				      kno_storage_flags flags,lispval opts)
{
  return kno_make_rocksdb_index(spec,opts);
}

static void recycle_rocksdb_index(kno_index ix)
{
  struct KNO_ROCKSDB_INDEX *db = (kno_rocksdb_index) ix;
  kno_decref((lispval)(db->rocksdb));
}

/* Initializing the index driver */

static struct KNO_INDEX_HANDLER rocksdb_index_handler=
  {
   "rocksdb_index", 1, sizeof(struct KNO_ROCKSDB_INDEX), 14, NULL,
   rocksdb_index_close, /* close */
   rocksdb_index_commit, /* commit */
   rocksdb_index_fetch, /* fetch */
   rocksdb_index_fetchsize, /* fetchsize */
   NULL, /* prefetch */
   rocksdb_index_fetchn, /* fetchn */
   rocksdb_index_fetchkeys, /* fetchkeys */
   NULL, /* fetchinfo */
   NULL, /* batchadd */
   rocksdb_index_create, /* create */
   NULL, /* walk */
   recycle_rocksdb_index, /* recycle */
   NULL /* indexctl */
  };

/* Rocksdb pool backends */

static struct KNO_POOL_HANDLER rocksdb_pool_handler;

KNO_EXPORT
kno_pool kno_open_rocksdb_pool(u8_string path,kno_storage_flags flags,lispval opts)
{
  struct KNO_ROCKSDB *rocksdb = kno_open_rocksdb(path,opts);
  if (rocksdb == NULL) return NULL;
  struct KNO_ROCKSDB_POOL *pool = u8_alloc(struct KNO_ROCKSDB_POOL);
  rocksdb_t *dbptr = rocksdb->dbptr;
  lispval base = get_prop(dbptr,"\377BASE",KNO_VOID);
  lispval cap = get_prop(dbptr,"\377CAPACITY",KNO_VOID);
  lispval load = get_prop(dbptr,"\377LOAD",KNO_VOID);
  lispval metadata = get_prop(dbptr,"\377METADATA",KNO_VOID);
  lispval slotcodes = get_prop(dbptr,"\377SLOTIDS",KNO_VOID);
  if ((KNO_OIDP(base)) && (KNO_UINTP(cap)) && (KNO_UINTP(load))) {
    u8_string realpath = u8_realpath(path,NULL);
    u8_string abspath = u8_abspath(path,NULL);
    lispval adjunct_opt = kno_getopt(opts,SYM("ADJUNCT"),KNO_VOID);
    lispval read_only_opt = kno_getopt(opts,SYM("READONLY"),KNO_VOID);
    lispval label = get_prop(dbptr,"\377LABEL",KNO_VOID);
    kno_init_pool((kno_pool)pool,
		  KNO_OID_ADDR(base),KNO_FIX2INT(cap),
		  &rocksdb_pool_handler,
		  path,abspath,realpath,
		  KNO_STORAGE_ISPOOL,metadata,opts);
    u8_free(realpath);
    u8_free(abspath);
    pool->rocksdb = rocksdb;
    if (KNO_VOIDP(read_only_opt))
      read_only_opt = get_prop(dbptr,"\377READONLY",KNO_VOID);
    if (KNO_VOIDP(read_only_opt))
      adjunct_opt = get_prop(dbptr,"\377ADJUNCT",KNO_VOID);
    if ( (! KNO_VOIDP(read_only_opt)) && (KNO_TRUEP(read_only_opt)) )
      pool->pool_flags |= KNO_STORAGE_READ_ONLY;
    if ( (! KNO_VOIDP(adjunct_opt)) && (!(KNO_FALSEP(adjunct_opt))) )
      pool->pool_flags |= KNO_POOL_ADJUNCT;
    pool->pool_load = KNO_FIX2INT(load);
    if (KNO_STRINGP(label)) {
      pool->pool_label = u8_strdup(KNO_CSTRING(label));}
    if (KNO_SLOTMAPP(metadata)) {
      kno_copy_slotmap((kno_slotmap)metadata,
		       &(pool->pool_metadata));
      kno_set_modified(((lispval)(&(pool->pool_metadata))),0);}
    kno_decref(adjunct_opt); kno_decref(read_only_opt);
    kno_decref(metadata); kno_decref(label);
    kno_decref(base); kno_decref(cap); kno_decref(load);
    kno_decref(slotcodes);
    kno_register_pool((kno_pool)pool);
    return (kno_pool)pool;}
  else  {
    kno_decref(base); kno_decref(cap); kno_decref(load);
    kno_decref(slotcodes); kno_decref(metadata);
    u8_free(pool);
    kno_seterr("NotAPoolDB","kno_rocksdb_pool",NULL,KNO_VOID);
    return (kno_pool)NULL;}
}

KNO_EXPORT
kno_pool kno_make_rocksdb_pool(u8_string path,
			       lispval base,
			       lispval cap,
			       lispval opts)
{
  lispval load = kno_getopt(opts,SYM("LOAD"),KNO_FIXZERO);

  if ((!(KNO_OIDP(base)))||(!(KNO_UINTP(cap)))||(!(KNO_UINTP(load)))) {
    kno_seterr("Not enough information to create a pool",
	       "kno_make_rocksdb_pool",path,opts);
    return (kno_pool)NULL;}

  struct KNO_ROCKSDB_POOL *pool = u8_alloc(struct KNO_ROCKSDB_POOL);
  struct KNO_ROCKSDB *rocksdb = kno_open_rocksdb(path,opts);

  rocksdb_t *dbptr = rocksdb->dbptr;
  lispval label = kno_getopt(opts,SYM("LABEL"),KNO_VOID);
  lispval metadata = kno_getopt(opts,SYM("METADATA"),KNO_VOID);
  lispval given_base = get_prop(dbptr,"\377BASE",KNO_VOID);
  lispval given_cap = get_prop(dbptr,"\377CAPACITY",KNO_VOID);
  lispval given_load = get_prop(dbptr,"\377LOAD",KNO_VOID);
  lispval cur_label = get_prop(dbptr,"\377LABEL",KNO_VOID);
  lispval cur_metadata = get_prop(dbptr,"\377METADATA",KNO_VOID);
  lispval ctime_val = kno_getopt(opts,kno_intern("ctime"),KNO_VOID);
  lispval mtime_val = kno_getopt(opts,kno_intern("mtime"),KNO_VOID);
  lispval generation_val = kno_getopt(opts,kno_intern("generation"),KNO_VOID);
  lispval slotids = kno_getopt(opts,kno_intern("slotids"),KNO_VOID);

  u8_string realpath = u8_realpath(path,NULL);
  u8_string abspath = u8_abspath(path,NULL);
  time_t now=time(NULL), ctime, mtime;

  if (!((KNO_VOIDP(given_base))||(KNO_EQUALP(base,given_base)))) {
    u8_free(pool); u8_free(realpath); u8_free(abspath);
    kno_seterr("Conflicting base OIDs",
	       "kno_make_rocksdb_pool",path,opts);
    return NULL;}
  if (!((KNO_VOIDP(given_cap))||(KNO_EQUALP(cap,given_cap)))) {
    u8_free(pool); u8_free(realpath); u8_free(abspath);
    kno_seterr("Conflicting pool capacities",
	       "kno_make_rocksdb_pool",path,opts);
    return NULL;}
  if (!(KNO_VOIDP(label))) {
    if (!(KNO_EQUALP(label,cur_label)))
      set_prop(dbptr,"\377LABEL",label,sync_writeopts);}

  set_prop(dbptr,"\377BASE",base,sync_writeopts);
  set_prop(dbptr,"\377CAPACITY",cap,sync_writeopts);

  if (KNO_VOIDP(given_load))
    set_prop(dbptr,"\377LOAD",load,sync_writeopts);

  if (KNO_SLOTMAPP(metadata)) {
    if ( (KNO_VOIDP(cur_metadata)) || (kno_testopt(opts,SYM("FORCE"),KNO_VOID)) ) {
      set_prop(dbptr,"\377METADATA",metadata,sync_writeopts);}
    else u8_log(LOG_WARN,"ExistingMetadata",
		"Not overwriting existing metatdata in rocksdb pool %s:",
		path,cur_metadata);}

  kno_init_pool((kno_pool)pool,
		KNO_OID_ADDR(base),KNO_FIX2INT(cap),
		&rocksdb_pool_handler,
		path,abspath,realpath,
		KNO_STORAGE_ISPOOL,
		metadata,
		opts);
  pool->rocksdb = rocksdb;

  if (KNO_FIXNUMP(ctime_val))
    ctime = (time_t) KNO_FIX2INT(ctime_val);
  else if (KNO_PRIM_TYPEP(ctime_val,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *moment = (kno_timestamp) ctime_val;
    ctime = moment->u8xtimeval.u8_tick;}
  else ctime=now;
  kno_decref(ctime_val);
  ctime_val = kno_time2timestamp(ctime);
  set_prop(dbptr,"\377CTIME",ctime_val,sync_writeopts);

  if (KNO_FIXNUMP(mtime_val))
    mtime = (time_t) KNO_FIX2INT(mtime_val);
  else if (KNO_PRIM_TYPEP(mtime_val,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *moment = (kno_timestamp) mtime_val;
    mtime = moment->u8xtimeval.u8_tick;}
  else mtime=now;
  kno_decref(mtime_val);
  mtime_val = kno_time2timestamp(mtime);
  set_prop(dbptr,"\377MTIME",mtime_val,sync_writeopts);

  if (KNO_FIXNUMP(generation_val))
    set_prop(dbptr,"\377GENERATION",generation_val,sync_writeopts);
  else set_prop(dbptr,"\377GENERATION",KNO_INT(0),sync_writeopts);

  pool->pool_flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL);
  pool->pool_flags &= ~KNO_STORAGE_READ_ONLY;
  pool->pool_load = KNO_FIX2INT(load);

  if (KNO_STRINGP(label)) {
    pool->pool_label = u8_strdup(KNO_CSTRING(label));}

  kno_decref(slotids);
  kno_decref(metadata);
  kno_decref(generation_val);
  kno_decref(ctime_val);
  kno_decref(mtime_val);
  u8_free(realpath);
  u8_free(abspath);

  kno_register_pool((kno_pool)pool);

  return (kno_pool)pool;
}

static lispval read_oid_value(kno_rocksdb_pool p,struct KNO_INBUF *in)
{
  return kno_read_xtype(in,&(p->rocksdb->xrefs));
}

static ssize_t write_oid_value(kno_rocksdb_pool p,
			       struct KNO_OUTBUF *out,
			       lispval value)
{
  return db_write_xtype(p->rocksdb,out,value);
}

static lispval get_oid_value(kno_rocksdb_pool ldp,unsigned int offset)
{
  ssize_t data_size; char *errmsg = NULL;
  rocksdb_t *dbptr = ldp->rocksdb->dbptr;
  rocksdb_readoptions_t *readopts = ldp->rocksdb->readopts;
  unsigned char keybuf[5];
  keybuf[0]=ROCKSDB_OIDKEY_PREFIX;
  keybuf[1]=((offset>>24)&0XFF);
  keybuf[2]=((offset>>16)&0XFF);
  keybuf[3]=((offset>>8)&0XFF);
  keybuf[4]=(offset&0XFF);
  unsigned char *buf = rocksdb_get
    (dbptr,readopts,keybuf,5,&data_size,&errmsg);
  if (buf) {
    lispval result = KNO_VOID;
    struct KNO_INBUF in = { 0 };
    KNO_INIT_BYTE_INPUT(&in,buf,data_size);
    result = read_oid_value(ldp,&in);
    u8_free(buf);
    return result;}
  else if (errmsg)
    return kno_err(RocksDB_Error,"get_prop",NULL,KNO_VOID);
  else return KNO_VOID;
}

static int queue_oid_value(kno_rocksdb_pool ldp,
			   unsigned int offset,
			   lispval value,
			   rocksdb_writebatch_t *batch)
{
  struct KNO_OUTBUF out = { 0 };
  ssize_t dtype_len;
  unsigned char buf[5];
  buf[0]=ROCKSDB_OIDKEY_PREFIX;
  buf[1]=((offset>>24)&0XFF);
  buf[2]=((offset>>16)&0XFF);
  buf[3]=((offset>>8)&0XFF);
  buf[4]=(offset&0XFF);
  KNO_INIT_BYTE_OUTPUT(&out,512);
  if ((dtype_len = write_oid_value(ldp,&out,value))>0) {
    rocksdb_writebatch_put
      (batch,buf,5,out.buffer,out.bufwrite-out.buffer);
    kno_close_outbuf(&out);
    return dtype_len;}
  else return -1;
}

static lispval rocksdb_pool_alloc(kno_pool p,int n)
{
  lispval results = KNO_EMPTY_CHOICE; unsigned int i = 0, start;
  struct KNO_ROCKSDB_POOL *pool = (struct KNO_ROCKSDB_POOL *)p;
  kno_lock_pool_struct(p,1);
  if (pool->pool_load+n>=pool->pool_capacity) {
    kno_unlock_pool_struct(p);
    return kno_err(kno_ExhaustedPool,"rocksdb_pool_alloc",pool->poolid,KNO_VOID);}
  start = pool->pool_load;
  pool->pool_load = start+n;
  kno_unlock_pool_struct(p);
  while (i < n) {
    KNO_OID new_addr = KNO_OID_PLUS(pool->pool_base,start+i);
    lispval new_oid = kno_make_oid(new_addr);
    KNO_ADD_TO_CHOICE(results,new_oid);
    i++;}
  return results;
}

static lispval rocksdb_pool_fetchoid(kno_pool p,lispval oid)
{
  struct KNO_ROCKSDB_POOL *pool = (struct KNO_ROCKSDB_POOL *)p;
  KNO_OID addr = KNO_OID_ADDR(oid);
  unsigned int offset = KNO_OID_DIFFERENCE(addr,pool->pool_base);
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

static lispval *rocksdb_pool_fetchn(kno_pool p,int n,lispval *oids)
{
  struct KNO_ROCKSDB_POOL *pool = (struct KNO_ROCKSDB_POOL *)p;
  struct OFFSET_ENTRY *entries = u8_alloc_n(n,struct OFFSET_ENTRY);
  rocksdb_readoptions_t *readopts = pool->rocksdb->readopts;
  unsigned int largest_offset = 0, offsets_sorted = 1;
  lispval *values = u8_big_alloc_n(n,lispval);
  KNO_OID base = p->pool_base;
  int i = 0; while (i<n) {
    KNO_OID addr = KNO_OID_ADDR(oids[i]);
    unsigned int offset = KNO_OID_DIFFERENCE(addr,base);
    entries[i].oid_offset = KNO_OID_DIFFERENCE(addr,base);
    entries[i].fetch_offset = i;
    if (offsets_sorted) {
      if (offset>=largest_offset) largest_offset = offset;
      else offsets_sorted = 0;}
    i++;}
  if (!(offsets_sorted)) {
    qsort(entries,n,sizeof(struct OFFSET_ENTRY),off_compare);}
  rocksdb_iterator_t *iterator=
    rocksdb_create_iterator(pool->rocksdb->dbptr,readopts);
  i = 0; while (i<n) {
    unsigned char keybuf[5];
    unsigned int offset = entries[i].oid_offset;
    unsigned int fetch_offset = entries[i].fetch_offset;
    keybuf[0]=ROCKSDB_OIDKEY_PREFIX;
    keybuf[1]=((offset>>24)&0XFF);
    keybuf[2]=((offset>>16)&0XFF);
    keybuf[3]=((offset>>8)&0XFF);
    keybuf[4]=(offset&0XFF);
    rocksdb_iter_seek(iterator,keybuf,5);
    ssize_t bytes_len;
    const unsigned char *bytes = rocksdb_iter_value(iterator,&bytes_len);
    if (bytes) {
      struct KNO_INBUF in = { 0 };
      KNO_INIT_BYTE_INPUT(&in,bytes,bytes_len);
      lispval oidvalue = read_oid_value(pool,&in);
      values[fetch_offset]=oidvalue;}
    else values[fetch_offset]=KNO_VOID;
    i++;}
  rocksdb_iter_destroy(iterator);
  u8_free(entries);
  return values;
}

static int rocksdb_pool_getload(kno_pool p)
{
  struct KNO_ROCKSDB_POOL *pool = (struct KNO_ROCKSDB_POOL *)p;
  return pool->pool_load;
}

static int rocksdb_pool_lock(kno_pool p,lispval oids)
{
  /* What should this really do? */
  return 1;
}

static int rocksdb_pool_unlock(kno_pool p,lispval oids)
{
  /* What should this really do? Flush edits to disk? storen? */
  return 1;
}

static void rocksdb_pool_close(kno_pool p)
{
  struct KNO_ROCKSDB_POOL *pool = (struct KNO_ROCKSDB_POOL *)p;
}


static int rocksdb_pool_storen(kno_pool p,int n,lispval *oids,lispval *values)
{
  struct KNO_ROCKSDB_POOL *pool = (struct KNO_ROCKSDB_POOL *)p;
  rocksdb_t *dbptr = pool->rocksdb->dbptr;
  int i = 0, errval = 0;
  char *errmsg = NULL;
  rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
  ssize_t n_bytes = 0;
  while (i<n) {
    lispval oid = oids[i], value = values[i];
    KNO_OID addr = KNO_OID_ADDR(oid);
    unsigned int offset = KNO_OID_DIFFERENCE(addr,pool->pool_base);
    if (offset >= pool->pool_load) {
      if ( (pool->pool_flags) & KNO_POOL_ADJUNCT )
	pool->pool_load = offset+1;
      else {
	kno_seterr("Saving unallocated OID","rocksdb_pool_storen",
		   p->poolid,oid);
	errval=-1; break;}}
    ssize_t len = queue_oid_value(pool,offset,value,batch);
    if (len<0) {errval = len; break;}
    else {n_bytes+=len; i++;}}
  if (errval>=0) {
    kno_lock_pool_struct(p,1); {
      KNO_DECL_OUTBUF(loadout,10);
      kno_write_byte(&loadout,xt_posint);
      kno_write_varint(&loadout,pool->pool_load);
      rocksdb_writebatch_put(batch,"\377LOAD",strlen("\377LOAD"),
			     loadout.buffer,BUFIO_POINT(&loadout));}
    kno_unlock_pool_struct(p);
    if (errval>=0) {
      rocksdb_write(dbptr,sync_writeopts,batch,&errmsg);
      if (errmsg) {
	u8_seterr(RocksDB_Error,"rocksdb_pool_storen",errmsg);
	errval = -1;}}
    rocksdb_writebatch_destroy(batch);}
  if (errval<0)
    return errval;
  else return n;
}

static int rocksdb_pool_commit(kno_pool p,kno_commit_phase phase,
			       struct KNO_POOL_COMMITS *commits)
{
  switch (phase) {
  case kno_commit_start:
    return 0;
  case kno_commit_write:
    return rocksdb_pool_storen(p,commits->commit_count,
			       commits->commit_oids,
			       commits->commit_vals);
  case kno_commit_sync:
    return 0;
  case kno_commit_rollback:
    return 0;
  case kno_commit_cleanup:
    return 0;
  default:
    return 0;
  }
}

/* Creating rocksdb pools */

static kno_pool rocksdb_pool_create(u8_string spec,void *type_data,
				    kno_storage_flags storage_flags,
				    lispval opts)
{
  lispval base_oid = kno_getopt(opts,kno_intern("base"),VOID);
  lispval capacity_arg = kno_getopt(opts,kno_intern("capacity"),VOID);
  lispval load_arg = kno_getopt(opts,kno_intern("load"),VOID);
  lispval metadata = kno_getopt(opts,kno_intern("metadata"),VOID);
  unsigned int capacity;
  kno_pool dbpool = NULL;
  int rv = 0;
  if (u8_file_existsp(spec)) {
    kno_seterr(_("FileAlreadyExists"),"rocksdb_pool_create",spec,VOID);
    return NULL;}
  else if (!(OIDP(base_oid))) {
    kno_seterr("Not a base oid","rocksdb_pool_create",spec,base_oid);
    rv = -1;}
  else if (KNO_ISINT(capacity_arg)) {
    int capval = kno_getint(capacity_arg);
    if (capval<=0) {
      kno_seterr("Not a valid capacity","rocksdb_pool_create",
		 spec,capacity_arg);
      rv = -1;}
    else capacity = capval;}
  else {
    kno_seterr("Not a valid capacity","rocksdb_pool_create",
	       spec,capacity_arg);
    rv = -1;}
  if (rv<0) {}
  else if (KNO_ISINT(load_arg)) {
    int loadval = kno_getint(load_arg);
    if (loadval<0) {
      kno_seterr("Not a valid load","rocksdb_pool_create",spec,load_arg);
      rv = -1;}
    else if (loadval > capacity) {
      kno_seterr(kno_PoolOverflow,"rocksdb_pool_create",spec,load_arg);
      rv = -1;}
    else NO_ELSE;}
  else if ( (FALSEP(load_arg)) || (EMPTYP(load_arg)) ||
	    (VOIDP(load_arg)) || (load_arg == KNO_DEFAULT_VALUE)) {}
  else {
    kno_seterr("Not a valid load","rocksdb_pool_create",spec,load_arg);
    rv = -1;}

  if (rv>=0)
    dbpool = kno_make_rocksdb_pool(spec,base_oid,capacity_arg,opts);

  if (dbpool == NULL) rv=-1;

  kno_decref(base_oid);
  kno_decref(capacity_arg);
  kno_decref(load_arg);
  kno_decref(metadata);

  if (rv>=0) {
    kno_set_file_opts(spec,opts);
    return kno_open_pool(spec,storage_flags,opts);}
  else return NULL;
}

static kno_pool rocksdb_pool_open(u8_string spec,kno_storage_flags flags,lispval opts)
{
  return kno_open_rocksdb_pool(spec,flags,opts);
}

static void recycle_rocksdb_pool(kno_pool p)
{
  struct KNO_ROCKSDB_POOL *db = (kno_rocksdb_pool) p;
  kno_decref((lispval)(db->rocksdb));
}

/* The Rocksdb pool handler */

static struct KNO_POOL_HANDLER rocksdb_pool_handler=
  {
   "rocksdb_pool", 1, sizeof(struct KNO_ROCKSDB_POOL), 12, NULL,
   rocksdb_pool_close, /* close */
   rocksdb_pool_alloc, /* alloc */
   rocksdb_pool_fetchoid, /* fetch */
   rocksdb_pool_fetchn, /* fetchn */
   rocksdb_pool_getload, /* getload */
   rocksdb_pool_lock, /* lock */
   rocksdb_pool_unlock, /* release */
   rocksdb_pool_commit, /* commit */
   NULL, /* swapout */
   rocksdb_pool_create, /* create */
   NULL,  /* walk */
   recycle_rocksdb_pool, /* recycle */
   NULL  /* poolctl */
  };

/* Scheme primitives */


DEFC_PRIM("rocksdb/use-pool",use_rocksdb_pool_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"path",kno_string_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval use_rocksdb_pool_prim(lispval path,lispval opts)
{
  kno_pool pool = kno_open_rocksdb_pool(KNO_CSTRING(path),-1,opts);
  return kno_pool2lisp(pool);
}


DEFC_PRIM("rocksdb/make-pool",make_rocksdb_pool_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3),
	  "**undocumented**",
	  {"path",kno_string_type,KNO_VOID},
	  {"base",kno_oid_type,KNO_VOID},
	  {"cap",kno_fixnum_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval make_rocksdb_pool_prim(lispval path,lispval base,lispval cap,
				      lispval opts)
{
  kno_pool pool = kno_make_rocksdb_pool(KNO_CSTRING(path),base,cap,opts);
  return kno_pool2lisp(pool);
}

/* Table functions */

static lispval rocksdb_table_get(lispval db,lispval key,lispval dflt)
{
  lispval rv = rocksdb_get_prim(db,key,KNO_FALSE);
  if (KNO_EMPTYP(rv))
    return kno_incref(dflt);
  else return rv;
}

static int rocksdb_table_store(lispval db,lispval key,lispval val)
{
  lispval rv = rocksdb_put_prim(db,key,val,KNO_FALSE);
  if (KNO_ABORTP(rv))
    return -1;
  else {
    kno_decref(rv);
    return 1;}
}

/* Matching rocksdb  pathnames */

static u8_string rocksdb_matcher(u8_string name,void *data)
{
  int rv = 0;
  if (u8_directoryp(name)) {
    u8_string lock_file = u8_mkpath(name,"LOCK");
    u8_string current_file = u8_mkpath(name,"CURRENT");
    u8_string identity_file = u8_mkpath(name,"IDENTITY");
    if ( (u8_file_existsp(lock_file)) &&
	 (u8_file_existsp(current_file)) &&
	 (u8_file_existsp(identity_file)) ) {
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

static lispval rocksdb_module;

KNO_EXPORT int kno_init_rocksdb()
{
  if (rocksdb_initialized) return 0;
  rocksdb_initialized = u8_millitime();

  u8_init_mutex(&rocksdb_list_lock);

  default_readopts = rocksdb_readoptions_create();
  sync_writeopts = rocksdb_writeoptions_create();
  rocksdb_writeoptions_set_sync(sync_writeopts,1);

  kno_rocksdb_type = kno_register_cons_type("RocksDB",KNO_ROCKSDB_TYPE);

  kno_unparsers[kno_rocksdb_type]=unparse_rocksdb;
  kno_recyclers[kno_rocksdb_type]=recycle_rocksdb;

  /* Table functions for rocksdbs */
  kno_tablefns[kno_rocksdb_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_rocksdb_type]->get = rocksdb_table_get;
  kno_tablefns[kno_rocksdb_type]->store = rocksdb_table_store;

  rocksdb_module = kno_new_cmodule("rocksdb",0,kno_init_rocksdb);

  link_local_cprims();

  kno_register_config("ROCKSDB:WRITEBUF",
		      "Default writebuf size for rocksdb",
		      kno_sizeconfig_get,kno_sizeconfig_set,
		      &default_writebuf_size);
  kno_register_config("ROCKSDB:BLOCKSIZE",
		      "Default block size for rocksdb",
		      kno_sizeconfig_get,kno_sizeconfig_set,
		      &default_block_size);
  kno_register_config("ROCKSDB:CACHESIZE",
		      "Default block size for rocksdb",
		      kno_sizeconfig_get,kno_sizeconfig_set,
		      &default_cache_size);
  kno_register_config("ROCKSDB:MAXFILES",
		      "The maximnum number of file descriptions Rocksdb may open",
		      kno_intconfig_get,kno_intconfig_set,
		      &default_maxfiles);
  kno_register_config("ROCKSDB:COMPRESS",
		      "Whether to compress data stored in rocksdb",
		      kno_boolconfig_get,kno_boolconfig_set,
		      &default_compression);
  kno_register_config("ROCKSDB:RESTART",
		      "The restart interval for rocksdb",
		      kno_intconfig_get,kno_intconfig_set,
		      &default_restart_interval);

  kno_register_pool_type
    ("rocksdb:pool",
     &rocksdb_pool_handler,
     rocksdb_pool_open,
     rocksdb_matcher,
     NULL);

  kno_register_index_type
    ("rocksdb:index",
     &rocksdb_index_handler,
     rocksdb_index_open,
     rocksdb_matcher,
     NULL);

  kno_finish_module(rocksdb_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("rocksdb/make-pool",make_rocksdb_pool_prim,4,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/use-pool",use_rocksdb_pool_prim,2,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/index/add!",rocksdb_index_add_prim,4,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/index/get",rocksdb_index_get_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/prefix/getn",rocksdb_prefix_getn_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/prefix/get",rocksdb_prefix_get_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/getn",rocksdb_getn_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/drop!",rocksdb_drop_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/put!",rocksdb_put_prim,4,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/get",rocksdb_get_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/reopen",rocksdb_reopen_prim,1,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/close",rocksdb_close_prim,1,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb?",rocksdbp_prim,1,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/open",rocksdb_open_prim,2,rocksdb_module);

  KNO_LINK_CPRIM("rocksdb/close",rocksdb_close_prim,1,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/reopen",rocksdb_reopen_prim,1,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/get",rocksdb_get_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/put!",rocksdb_put_prim,4,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/drop!",rocksdb_drop_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/reopen",rocksdb_reopen_prim,1,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/getn",rocksdb_getn_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/prefix/get",rocksdb_prefix_get_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/prefix/getn",rocksdb_prefix_getn_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/index/get",rocksdb_index_get_prim,3,rocksdb_module);
  KNO_LINK_CPRIM("rocksdb/index/add!",rocksdb_index_add_prim,4,rocksdb_module);
}
