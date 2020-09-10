/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_LOGLEVEL (u8_merge_loglevels(local_loglevel,zpathstore_loglevel))
static int zpathstore_loglevel = 5;
static int local_loglevel = -1;

#include "kno/knosource.h"
#include "kno/defines.h"
#include "kno/lisp.h"
#include "kno/pathstore.h"
#include "kno/getsource.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8convert.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8timefns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>
#include <libu8/u8netfns.h>
#include <libu8/u8srvfns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8uuid.h>

#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <signal.h>
#include <stdio.h>
#include <math.h>

#define MINIZ_HEADER_FILE_ONLY 1
#include "headers/miniz.h"
#include "headers/zip.h"

DEF_KNOSYM(modified); DEF_KNOSYM(pathtype);
DEF_KNOSYM(symlink); DEF_KNOSYM(directory);
DEF_KNOSYM(file); DEF_KNOSYM(weird);
DEF_KNOSYM(content);

static u8_string get_string_opt(lispval opts,lispval optname,ssize_t *sizep)
{
  u8_string result = NULL;
  lispval val = kno_getopt(opts,optname,KNO_VOID);
  if (KNO_STRINGP(val)) {
    result = u8_strdup(KNO_CSTRING(val));
    if (sizep) *sizep = KNO_STRLEN(val);}
  else {
    result = NULL;
    if (sizep) *sizep = -1;}
  kno_decref(val);
  return result;
}

#define zip_entry_type(zip) (((zip)->entry.external_attr&0xF0000000) >> 28)

DEF_KNOSYM(cacheroot); DEF_KNOSYM(cachesize);
DEF_KNOSYM(prefix); DEF_KNOSYM(mountpoint);

static int zpathstore_exists(struct KNO_PATHSTORE *ps,u8_string path)
{
  struct zip_t *zip = (struct zip_t *) ps->knops_data;
  int rv = zip_entry_open(zip,path);
  zip_entry_close(zip);
  if (rv>=0)
    return 1;
  else return 0;
}

static lispval zip_entry_content(struct zip_t *zip,
				 struct KNO_PATHSTORE *ps,
				 u8_string path,u8_string enc)
{
  unsigned char *bytes = NULL; ssize_t n_bytes;
  int rv = zip_entry_read(zip,(void *)&bytes,&n_bytes);
  if (rv<0) {
    kno_seterr("NoSuchPath","zpathstore_content",path,(lispval)ps);
    return KNO_ERROR;}
  if ( (enc == NULL) || (strcasecmp(enc,"auto")==0) ) {
    lispval result;
    if (u8_validate(bytes,n_bytes) == n_bytes)
      result = kno_make_string(NULL,n_bytes,bytes);
    else result = kno_make_packet(NULL,n_bytes,bytes);
    u8_free(bytes);
    return result;}
  else if ( (strcasecmp(enc,"bytes")==0) ||
	    (strcasecmp(enc,"packet")==0) ||
	    (strcasecmp(enc,"binary")==0) ) {
    lispval result = kno_make_packet(NULL,n_bytes,bytes);
    u8_free(bytes);
    return result;}
  else {
    struct U8_TEXT_ENCODING *encoding = u8_get_encoding(enc);
    if (encoding == NULL) {
      kno_seterr("UnknownEncoding","zpathstore/content",
		 KNO_CSTRING(enc),(lispval)ps);
      u8_free(bytes);
      return KNO_ERROR;}
    else {
      u8_string converted = u8_make_string(encoding,bytes,bytes+n_bytes);
      u8_free(bytes);
      return kno_init_string(NULL,-1,converted);}}
}

static int zip_entry_follow(struct zip_t *zip,
			    struct KNO_PATHSTORE *ps,
			    u8_string path_arg)
{
  u8_string path = u8_strdup(path_arg);
  int count = 0; while ( (count<42) && (zip_entry_type(zip) == 0xA) ) {
    u8_string dir = u8_dirname(path);
    u8_string linkname; ssize_t linkname_len;
    int rv = zip_entry_read(zip,(void *)&linkname,&linkname_len);
    if (rv<0) {}
    u8_byte _namebuf[linkname_len+1], *usename = _namebuf;
    /* We copy this byte by byte, because we may get memory errors reading from
       linkname (which can somehow manage to be linkname_len bytes rather than
       linkname_len+1 bytes. */
    int i = 0; while (i<linkname_len) {
      _namebuf[i] = linkname[i]; i++;}
    _namebuf[i]='\0';
    u8_string new_path = u8_mkpath(dir,usename);
    zip_entry_close(zip);
    rv = zip_entry_open(zip,new_path);
    u8_free(linkname); u8_free(dir);
    if (rv<0) {
      u8_byte buf[256];
      kno_seterr("ZPathStore/DanglingSymlink",
		 "zip_entry_follow",
		 u8_bprintf(buf,"%s =...=> %s !=> %s",
			    path_arg,path,new_path),
		 (lispval)ps);
      u8_free(path);
      return -1;}
    else {
      u8_free(path);
      path = new_path;
      count++;}}
  if (zip_entry_type(zip) == 0xA) {
    u8_byte buf[256];
    kno_seterr("ZPathStore/TooManySymlinks",
	       "zip_entry_follow",
	       u8_bprintf(buf,"%s =...=> %s",path_arg,path),
	       (lispval)ps);
    u8_free(path);
    return -1;}
  u8_free(path);
  return 1;
}

static lispval zpathstore_content
(struct KNO_PATHSTORE *ps,u8_string path,u8_string enc,int follow)
{
  struct zip_t *zip = (struct zip_t *) ps->knops_data;
  int rv = zip_entry_open(zip,path);
  if (rv<0) {
    kno_seterr("ZPathStoreError","zpathstore_content",path,(lispval)ps);
    return KNO_ERROR;}
  else if ( (follow) && (zip_entry_type(zip)==0xA) ) {
    if (zip_entry_follow(zip,ps,path)<0) goto onerror;}
  lispval content = zip_entry_content(zip,ps,path,enc);
  zip_entry_close(zip);
  return content;
 onerror:
  zip_entry_close(zip);
  return KNO_ERROR;
}

static lispval zpathstore_info(struct KNO_PATHSTORE *ps,u8_string path,int follow)
{
  struct zip_t *zip = (struct zip_t *) ps->knops_data;
  int rv = zip_entry_open(zip,path);
  if (rv<0) return KNO_FALSE;
  if ( (follow) && (zip_entry_type(zip)==0xA) ) {
    if (zip_entry_follow(zip,ps,path)<0) goto onerror;}
  lispval name = knostring(zip->entry.name);
  lispval size = KNO_INT(zip->entry.uncomp_size);
  lispval mtime = kno_time2timestamp(zip->entry.m_time);
  int type_bits = zip_entry_type(zip);
  lispval ziptype =
    ( (type_bits == 0xa) ? (symlink_symbol) :
      (type_bits == 0x4) ? (directory_symbol) :
      (type_bits == 0x8) ? (file_symbol) :
      (weird_symbol) );
  /* Maybe have an argument for getting the content too */
  lispval content = KNO_VOID;
  struct KNO_KEYVAL kv[5] =
    { { KNOSYM_NAME, name },
      { KNOSYM_SIZE, size },
      { modified_symbol, mtime },
      { pathtype_symbol, ziptype },
      { content_symbol, content } };
  zip_entry_close(zip);
  return kno_make_slotmap(4,4,kv);
 onerror:
  zip_entry_close(zip);
  return KNO_ERROR;
}

static void recycle_zpathstore(struct KNO_PATHSTORE *ps)
{
  struct zip_t *zip = (struct zip_t *) (ps->knops_data);
  zip_close(zip);
}

static struct KNO_PATHSTORE_HANDLERS zpathstore_handlers =
  { "zpathstore", 3,
    zpathstore_exists,
    zpathstore_info,
    zpathstore_content,
    NULL,
    NULL,
    recycle_zpathstore};

KNO_EXPORT lispval kno_open_zpathstore(u8_string path,lispval opts)
{
  struct zip_t *zip = zip_open(path,9,'r');
  if (zip == NULL) {
    kno_seterr("ZPathStoreOpenFailed","kno_open_zpathstore",path,opts);
    return KNO_ERROR;}
  /* Maybe read additional opts data from zip file?? */
  struct KNO_PATHSTORE *ps = u8_alloc(struct KNO_PATHSTORE);
  KNO_INIT_FRESH_CONS(ps,kno_pathstore_type);
  ps->knops_id = u8_strdup(path);
  ps->knops_config = kno_incref(opts);
  ps->knops_mountpoint =
    get_string_opt(opts,KNOSYM(mountpoint),
		   &(ps->knops_mountpoint_len));
  ps->knops_prefix =
    get_string_opt(opts,KNOSYM(prefix),&(ps->knops_prefix_len));
  if ( ps->knops_prefix == NULL) {
    ps->knops_prefix = u8_abspath(path,NULL);
    ps->knops_prefix_len = strlen(ps->knops_prefix);}
  ps->knops_cacheroot = get_string_opt(opts,KNOSYM(cacheroot),NULL);
  ps->knops_flags = 0;
  ps->knops_data = zip;
  ps->knops_handlers = &zpathstore_handlers;
  lispval cachesize = kno_getopt(opts,KNOSYM(cachesize),KNO_VOID);
  struct KNO_HASHTABLE *cache = &(ps->knops_cache);
  if (KNO_UINTP(cachesize))
    kno_make_hashtable(cache,KNO_FIX2INT(cachesize));
  else if (KNO_TRUEP(cachesize))
    kno_make_hashtable(cache,117);
  else {
    kno_make_hashtable(cache,0);
    kno_decref(cachesize);}
  u8_logf(LOG_DEBUG,"ZPathOpen","Opened %q",ps);
  return (lispval) ps;
}

/* Zip pathstore cache */

static struct KNO_HASHTABLE zipsources;
static u8_mutex zipsources_lock;

static lispval get_zipsource(u8_string zip_path,size_t len,int locked)
{
  struct KNO_STRING probe;
  KNO_INIT_STATIC_CONS(&probe,kno_string_type);
  probe.str_bytes = zip_path;
  probe.str_bytelen = len;
  probe.str_freebytes = 1;
  lispval known = kno_hashtable_get(&zipsources,((lispval)&probe),KNO_VOID);
  if (!(KNO_VOIDP(known))) return known;
  if (locked) {
    u8_lock_mutex(&zipsources_lock);
    known = kno_hashtable_get(&zipsources,((lispval)&probe),KNO_VOID);}
  if (!(KNO_VOIDP(known))) {
    u8_unlock_mutex(&zipsources_lock);
    return known;}
  if (u8_file_existsp(zip_path)) {
    lispval ps = kno_open_zpathstore(zip_path,KNO_FALSE);
    if (KNO_TYPEP(ps,kno_pathstore_type))
      kno_hashtable_store(&zipsources,((lispval)&probe),ps);
    else kno_hashtable_store(&zipsources,((lispval)&probe),KNO_FALSE);
    if (KNO_ABORTED(ps)) {
      u8_exception ex = u8_pop_exception();
      u8_log(LOGWARN,"BadZipFile",
	     "Couldn't open zip file %s: %s <%s> (%s)",
	     zip_path,ex->u8x_cond,ex->u8x_context,
	     ex->u8x_details);
      u8_free_exception(ex,0);
      ps = KNO_FALSE;}
    return ps;}
  else {
    kno_hashtable_store(&zipsources,((lispval)&probe),KNO_FALSE);
    return KNO_FALSE;}
}

KNO_EXPORT lispval kno_get_zipsource(u8_string path)
{
  return get_zipsource(path,strlen(path),0);
}

/* Getting file sources */

static u8_string zip_source_fn(int fetch,lispval pathspec,u8_string encname,
			       u8_string *abspath,time_t *timep,ssize_t *sizep,
			       void *ignored)
{
  u8_byte *result = NULL;
  if (KNO_STRINGP(pathspec)) {
    u8_string pathstring = KNO_CSTRING(pathspec), slash;
    if ((slash=(strstr(pathstring,".zip/")))) {
      ssize_t path_len = KNO_STRLEN(pathspec), zip_len = (slash-pathstring)+4;
      u8_byte path[path_len+1], *subpath = path+zip_len+1;
      memcpy(path,pathstring,path_len+1);
      path[zip_len]='\0';
      lispval pathstore = get_zipsource(path,zip_len,0);
      if (KNO_TYPEP(pathstore,kno_pathstore_type)) {
	struct KNO_PATHSTORE *ps = (kno_pathstore) pathstore;
	lispval info = zpathstore_info(ps,subpath,1);
	if (KNO_FALSEP(info)) {
	  kno_decref(pathstore);
	  return NULL;}
	lispval real_name = kno_get(info,KNOSYM_NAME,KNO_VOID);
	lispval data_len = kno_get(info,KNOSYM_SIZE,KNO_VOID);
	lispval modified = kno_get(info,KNOSYM(modified),KNO_VOID);
	if ( (abspath) && (KNO_STRINGP(real_name)) )
	  *abspath=u8_mkpath(path,KNO_CSTRING(real_name));
	else if (abspath) *abspath=u8_mkpath(path,subpath);
	else NO_ELSE;
	if ( (sizep) && (KNO_FIXNUMP(data_len)) )
	  *sizep = KNO_FIX2INT(data_len);
	if ( (timep) && (KNO_TYPEP(modified,kno_timestamp_type)) ) {
	  struct KNO_TIMESTAMP *ts = (kno_timestamp) modified;
	  *timep = ts->u8xtimeval.u8_tick;}
	kno_decref(real_name);
	kno_decref(data_len); kno_decref(modified);
	kno_decref(info);
	if (fetch) {
	  lispval v = zpathstore_content(ps,subpath,encname,1);
	  kno_decref(pathstore);
	  if (KNO_STRINGP(v)) {
	    result = u8_strdup(KNO_CSTRING(v));
	    kno_decref(v);}
	  else if (KNO_PACKETP(v)) {
	    ssize_t len = KNO_PACKET_LENGTH(v);
	    result = u8_malloc(len);
	    memcpy(result,KNO_PACKET_DATA(v),len);
	    kno_decref(v);}
	  else kno_decref(v);}}}}
  return (u8_string) result;
}

/* Initialization */

static int zpathstore_initialized = 0;

int kno_init_zpathstore_c()
{
  if (zpathstore_initialized) return 0;

  u8_init_mutex(&zipsources_lock);

  KNOSYM(modified); KNOSYM(pathtype);
  KNOSYM(symlink); KNOSYM(directory);
  KNOSYM(file); KNOSYM(weird);
  KNOSYM(content);
  KNOSYM(prefix);

  kno_init_hashtable(&zipsources,17,NULL);

  kno_register_sourcefn(zip_source_fn,NULL);

  u8_register_source_file(_FILEINFO);

  zpathstore_initialized = u8_millitime();

  return 1;
}
