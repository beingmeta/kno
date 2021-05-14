/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
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
#include "kno/zipsource.h"
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

#define STR_EXTRACT(into,start,end)					\
  u8_string into ## _end = end;						\
  size_t into ## _strlen = (into ## _end) ? ((into ## _end)-(start)) : (strlen(start)); \
  u8_byte into[into ## _strlen +1];					\
  memcpy(into,start,into ## _strlen);					\
  into[into ## _strlen]='\0'

DEF_KNOSYM(modified); DEF_KNOSYM(pathtype);
DEF_KNOSYM(symlink); DEF_KNOSYM(directory);
DEF_KNOSYM(file); DEF_KNOSYM(weird);
DEF_KNOSYM(content);

#define ZIPSOURCEP(x) (KNO_RAW_TYPEP(x,KNOSYM_ZIPSOURCE))

#define zip_entry_type(zip) (((zip)->entry.external_attr&0xF0000000) >> 28)

KNO_EXPORT int kno_zipsource_existsp(lispval zs,u8_string path)
{
  if (!(ZIPSOURCEP(zs)))
    return kno_err("NotZipSource","kno_zipsource_existsp",path,zs);
  struct zip_t *zip = (struct zip_t *) KNO_RAWPTR_VALUE(zs);
  u8_lock_mutex(&(zip->lock));
  int rv = zip_entry_open(zip,path);
  zip_entry_close(zip);
  if (rv>=0)
    return 1;
  else return 0;
}

static lispval zip_entry_content(struct zip_t *zip,
				 lispval zipsource,
				 u8_string path,u8_string enc)
{
  unsigned char *bytes = NULL; ssize_t n_bytes;
  int rv = zip_entry_read(zip,(void *)&bytes,&n_bytes);
  if (rv<0) {
    kno_seterr("NoSuchPath","zpathstore_content",path,zipsource);
    return KNO_ERROR;}
  if ( (enc == NULL) || (strcasecmp(enc,"auto")==0) ) {
    lispval result;
    u8_byte headbuf[501];
    ssize_t copy_len = (n_bytes>500) ? (500) : (n_bytes);
    memcpy(headbuf,bytes,copy_len); headbuf[copy_len]='\0';
    struct U8_TEXT_ENCODING *new_enc = u8_guess_encoding((u8_string)headbuf);
    if (new_enc) {
      struct U8_OUTPUT out; int retval=0;
      const unsigned char *scan=bytes;
      U8_INIT_STATIC_OUTPUT(out,n_bytes+n_bytes/2);
      retval=u8_convert(new_enc,1,&out,&scan,bytes+n_bytes);
      if (retval<0) {
	u8_free(out.u8_outbuf);
	lispval result = kno_make_packet(NULL,n_bytes,bytes);
	u8_free(bytes);
	return result;}
      else {
	lispval result = kno_stream_string(&out);
	u8_free(out.u8_outbuf);
	u8_free(bytes);
	return result;}}
    else if (u8_validate(bytes,n_bytes) == n_bytes)
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
		 KNO_CSTRING(enc),zipsource);
      u8_free(bytes);
      return KNO_ERROR;}
    else {
      u8_string converted = u8_make_string(encoding,bytes,bytes+n_bytes);
      u8_free(bytes);
      return kno_init_string(NULL,-1,converted);}}
}

static int zip_entry_follow(struct zip_t *zip,
			    lispval zipsource,
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
		 zipsource);
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
	       zipsource);
    u8_free(path);
    return -1;}
  u8_free(path);
  return 1;
}

KNO_EXPORT lispval kno_zipsource_content
(lispval zs,u8_string path,u8_string enc,int follow)
{
  if (!(ZIPSOURCEP(zs)))
    return kno_err("NotZipSource","kno_zipsource_content",path,zs);
  struct zip_t *zip = (struct zip_t *) KNO_RAWPTR_VALUE(zs);
  u8_lock_mutex(&(zip->lock));
  int rv = zip_entry_open(zip,path);
  if (rv<0) {
    kno_seterr("ZPathStoreError","zpathstore_content",path,zs);
    u8_unlock_mutex(&(zip->lock));
    return KNO_ERROR;}
  else if ( (follow) && (zip_entry_type(zip)==0xA) ) {
    if (zip_entry_follow(zip,zs,path)<0) goto onerror;}
  lispval content = zip_entry_content(zip,zs,path,enc);
  zip_entry_close(zip);
  u8_unlock_mutex(&(zip->lock));
  return content;
 onerror:
  zip_entry_close(zip);
  u8_unlock_mutex(&(zip->lock));
  return KNO_ERROR;
}

KNO_EXPORT lispval kno_zipsource_info(lispval zs,u8_string path,int follow)
{
  if (!(ZIPSOURCEP(zs)))
    return kno_err("NotZipSource","kno_zipsource_info",path,zs);
  struct zip_t *zip = (struct zip_t *) KNO_RAWPTR_VALUE(zs);
  u8_lock_mutex(&(zip->lock));
  int rv = zip_entry_open(zip,path);
  if (rv<0) {
    u8_unlock_mutex(&(zip->lock));
    return KNO_FALSE;}
  if ( (follow) && (zip_entry_type(zip)==0xA) ) {
    if (zip_entry_follow(zip,zs,path)<0) goto onerror;}
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
  u8_lock_mutex(&(zip->lock));
  return kno_make_slotmap(4,4,kv);
 onerror:
  zip_entry_close(zip);
  u8_lock_mutex(&(zip->lock));
  return KNO_ERROR;
}

static void recycle_zipsource(void *zipval)
{
  struct zip_t *zip = (struct zip_t *) zipval;
  zip_close(zip);
}

KNO_EXPORT lispval kno_open_zipsource(u8_string path,lispval opts)
{
  struct zip_t *zip = zip_open(path,9,'r');
  if (zip == NULL) {
    kno_seterr("ZPathStoreOpenFailed","kno_open_zipsource",path,opts);
    return KNO_ERROR;}
  lispval result=
    kno_wrap_pointer(zip,sizeof(struct zip_t),
		     recycle_zipsource,
		     KNOSYM_ZIPSOURCE,
		     path);
  u8_logf(LOG_DEBUG,"ZPathOpen","Opened %q",result);
  return result;
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
    lispval zs = kno_open_zipsource(zip_path,KNO_FALSE);
    if (ZIPSOURCEP(zs))
      kno_hashtable_store(&zipsources,((lispval)&probe),zs);
    else kno_hashtable_store(&zipsources,((lispval)&probe),KNO_FALSE);
    if (KNO_ABORTED(zs)) {
      u8_exception ex = u8_pop_exception();
      u8_log(LOGWARN,"BadZipFile",
	     "Couldn't open zip file %s: %s <%s> (%s)",
	     zip_path,ex->u8x_cond,ex->u8x_context,
	     ex->u8x_details);
      u8_free_exception(ex,0);
      zs = KNO_FALSE;}
    return zs;}
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
    u8_string pathstring = KNO_CSTRING(pathspec), zip_suffix;
    /* Must start with zip: */
    if (strncasecmp(pathstring,"zip:",4)) return NULL;
    if ((zip_suffix=(strstr(pathstring+4,".zip/")))) {
      int zipsource_len = (zip_suffix+4)-pathstring-4;
      lispval zipsource = get_zipsource(pathstring+4,zipsource_len,0);
      u8_string subpath = zip_suffix+5;
      if (ZIPSOURCEP(zipsource)) {
	lispval info = kno_zipsource_info(zipsource,subpath,1);
	if (KNO_FALSEP(info)) {
	  kno_decref(zipsource);
	  return NULL;}
	STR_EXTRACT(path,pathstring,zip_suffix+5);
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
	  lispval v = kno_zipsource_content(zipsource,subpath,encname,1);
	  kno_decref(zipsource);
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

int unparse_zipsource(u8_output out,lispval x,kno_typeinfo info)
{
  struct zip_t *zip = KNO_RAWPTR_VALUE(x);
  u8_printf(out,"#%%(ZIPSOURCE \"%s\")",zip->pathname);
  return 1;
}

lispval cons_zipsource(int n,lispval *args,kno_typeinfo info)
{
  if (n<2) return KNO_VOID;
  else if ( (STRINGP(args[1])) &&
	    (u8_file_existsp(CSTRING(args[1]))) ) {
    lispval restored = kno_get_zipsource(CSTRING(args[1]));
    if (KNO_ABORTED(restored))
      return restored;
    else return restored;}
  else return KNO_VOID;
}

static lispval dump_zipsource(lispval x,kno_typeinfo i)
{
  struct zip_t *zip = KNO_RAWPTR_VALUE(x);
  return knostring(zip->pathname);
}

static lispval restore_zipsource(lispval tag,lispval x,kno_typeinfo e)
{
  if (KNO_STRINGP(x))
    return kno_get_zipsource(KNO_CSTRING(x));
  else return kno_err("RestoreError","bad zipsource",NULL,x);
}

/* Initialization */

static int zipsource_initialized = 0;

static lispval zipsource_get(lispval zs,lispval key,lispval dflt)
{
  if (!(KNO_ZIPSOURCEP(zs))) return dflt;
  if (KNO_STRINGP(key)) {
    if (kno_zipsource_existsp(zs,KNO_CSTRING(key))) {
      lispval content = kno_zipsource_content(zs,KNO_CSTRING(key),NULL,1);
      return content;}
    else return dflt;}
  else return dflt;
}

static struct KNO_TABLEFNS zipsource_tablefns =
  {
   zipsource_get,
   NULL, /* store */
   NULL, /* add */
   NULL, /* drop */
   NULL, /* test */
   NULL, /* readonly */
   NULL, /* modified */
   NULL, /* finished */
   NULL, /* getsize */
   NULL, /* getkeys */
   NULL, /* getkeyvec */
   NULL, /* getkeyvals */
   NULL /* tablep */
  };

int kno_init_zipsource_c()
{
  if (zipsource_initialized) return 0;

  struct KNO_TYPEINFO *e = kno_use_typeinfo(KNOSYM_ZIPSOURCE);
  e->type_basetype = kno_rawptr_type;
  e->type_restorefn = restore_zipsource;
  e->type_dumpfn = dump_zipsource;
  e->type_unparsefn = unparse_zipsource;
  e->type_consfn = cons_zipsource;
  e->type_tablefns = &zipsource_tablefns;

  u8_init_mutex(&zipsources_lock);

  KNOSYM(modified); KNOSYM(pathtype);
  KNOSYM(symlink); KNOSYM(directory);
  KNOSYM(file); KNOSYM(weird);
  KNOSYM(content);

  kno_init_hashtable(&zipsources,17,NULL);

  kno_register_sourcefn(zip_source_fn,NULL);

  u8_register_source_file(_FILEINFO);

  zipsource_initialized = u8_millitime();

  return 1;
}
