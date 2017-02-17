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

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include "framerd/fdregex.h"
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

static fdtype leveldb_open_prim(fdtype path,fdtype opts)
{
  leveldb_options_t *options=get_leveldb_options(opts);
  leveldb_cache_t *cache=get_leveldb_cache(options,opts);
  leveldb_env_t *env=get_leveldb_env(options,opts);
  char *errmsg;
  leveldb_t *ldb=leveldb_open(options,FD_STRDATA(path),&errmsg);
  if (ldb) {
    struct FD_LEVELDB *db=u8_zalloc(struct FD_LEVELDB);
    FD_INIT_CONS(db,fd_leveldb_type);
    db->ldb_path=u8_strdup(FD_STRDATA(path));
    db->ldb_options=opts; fd_incref(opts);
    db->cldb_options=options;
    db->cldb_cache=cache;
    db->cldb_env=env;
    db->cldb_ptr=ldb;
    return (fdtype) db;}
  else {
    fd_seterr("OpenFailed","leveldb_open_prim",errmsg,opts);
    leveldb_options_destroy(options);
    return FD_ERROR_VALUE;}
}
static int unparse_leveldb(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_LEVELDB *db=(fd_leveldb)x;
  u8_printf(out,"#<LevelDB %s>",db->ldb_path);
  return 1;
}
static void recycle_leveldb(struct FD_CONS *c)
{
  struct FD_LEVELDB *db=(fd_leveldb)c;
  u8_free(db->ldb_path); db->ldb_path=NULL;
  fd_decref(db->ldb_options);
  leveldb_close(db->cldb_ptr);
  leveldb_options_destroy(db->cldb_options);
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
  leveldb_close(db->cldb_ptr);
  db->ldb_closed=1;
  leveldb_free(db->cldb_ptr);
  db->cldb_ptr=NULL;
  return FD_TRUE;
}

static fdtype leveldb_reopen_prim(fdtype leveldb)
{
  struct FD_LEVELDB *db=(fd_leveldb)leveldb;
  if (db->ldb_closed) {
    char *errmsg;
    leveldb_t *fresh=leveldb_open(db->cldb_options,db->ldb_path,&errmsg);
    if (fresh) {
      db->cldb_ptr=fresh;
      db->ldb_closed=0;}
    else {
      fd_seterr("OpenFailed","leveldb_reopen_prim",errmsg,FD_VOID);
      return FD_ERROR_VALUE;}}
  fd_incref(leveldb);
  return leveldb;
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

static leveldb_readoptions_t *get_read_options(struct FD_LEVELDB *db,fdtype opts_arg)
{
  fdtype opts;
  int free_opts=0;
  if (FD_VOIDP(opts_arg))
    opts=db->ldb_options;
  else if (FD_VOIDP(db->ldb_options))
    opts=opts_arg;
  else {
    opts=fd_make_pair(opts_arg,db->ldb_options);
    free_opts=1;}
  leveldb_readoptions_t *readopts=leveldb_readoptions_create();
  if (free_opts) fd_decref(opts);
  return readopts;
}

static leveldb_writeoptions_t *get_write_options(struct FD_LEVELDB *db,fdtype opts_arg)
{
  fdtype opts;
  int free_opts=0;
  if (FD_VOIDP(opts_arg))
    opts=db->ldb_options;
  else if (FD_VOIDP(db->ldb_options))
    opts=opts_arg;
  else {
    opts=fd_make_pair(opts_arg,db->ldb_options);
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
      leveldb_get(db->cldb_ptr,readopts,
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
	leveldb_get(db->cldb_ptr,readopts,
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
    leveldb_put(db->cldb_ptr,writeopts,
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
      leveldb_put(db->cldb_ptr,writeopts,
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

/* Initialization */

FD_EXPORT int fd_init_leveldb()
{
  fdtype module;
  if (leveldb_initialized) return 0;
  leveldb_initialized=u8_millitime();

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


