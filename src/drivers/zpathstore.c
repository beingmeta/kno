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

#include "headers/zip.h"

static u8_string get_string_opt(lispval opts,lispval optname)
{
  u8_string result = NULL;
  lispval val = kno_getopt(opts,optname,KNO_VOID);
  if (KNO_STRINGP(val))
    result = u8_strdup(KNO_CSTRING(val));
  kno_decref(val);
  return result;
}

DEF_KNOSYM(cacheroot); DEF_KNOSYM(cachesize); DEF_KNOSYM(prefix);

static int zpathstore_exists(struct KNO_PATHSTORE *ps,u8_string path)
{
  struct zip_t *zip = (struct zip_t *) ps->knops_data;
  int rv = zip_entry_open(zip,path);
  if (rv>=0)
    return 1;
  else return 0;
}

DEF_KNOSYM(modified);

static lispval zpathstore_content
(struct KNO_PATHSTORE *ps,u8_string path,u8_string enc)
{
  struct zip_t *zip = (struct zip_t *) ps->knops_data;
  int rv = zip_entry_open(zip,path);
  if (rv<0) {
    kno_seterr("ZPathStoreError","zpathstore_content",path,(lispval)ps);
    return KNO_ERROR;}
  else {
    unsigned char *bytes = NULL; ssize_t n_bytes;
    int rv = zip_entry_read(zip,(void *)&bytes,&n_bytes);
    if (rv<0) {
      kno_seterr("NoSuchPath","zpathstore_content",path,(lispval)ps);
      return KNO_ERROR;}
    if ( (enc == NULL) || (strcasecmp(enc,"auto")==0) ) {
      if (u8_validate(bytes,n_bytes) == n_bytes)
	return kno_init_string(NULL,n_bytes,bytes);
      else return kno_init_packet(NULL,n_bytes,bytes);}
    else if ( (strcasecmp(enc,"bytes")==0) ||
	      (strcasecmp(enc,"packet")==0) ||
	      (strcasecmp(enc,"binary")==0))
      return kno_init_packet(NULL,n_bytes,bytes);
    else {
      struct U8_TEXT_ENCODING *encoding = u8_get_encoding(KNO_CSTRING(enc));
      if (encoding == NULL) {
	kno_seterr("UnknownEncoding","zpathstore/content",
		   KNO_CSTRING(enc),(lispval)ps);
	return KNO_ERROR;}
      else {
	u8_string converted = u8_make_string(encoding,bytes,bytes+n_bytes);
	return kno_init_string(NULL,-1,converted);}}}
}

static lispval zpathstore_info(struct KNO_PATHSTORE *ps,u8_string path)
{
  struct zip_t *zip = (struct zip_t *) ps->knops_data;
  int rv = zip_entry_open(zip,path);
  if (rv<0) return KNO_FALSE;
  lispval name = knostring(zip->entry.name);
  lispval size = KNO_INT(zip->entry.uncomp_size);
  lispval mtime = kno_time2timestamp(zip->entry.m_time);
  lispval modsym = KNOSYM(modified);
  struct KNO_KEYVAL kv[3] =
    { { KNOSYM_NAME, name },
      { KNOSYM_SIZE, size },
      { modsym, mtime } };
  return kno_init_slotmap(NULL,3,kv);
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
  struct KNO_PATHSTORE *ps = u8_alloc(struct KNO_PATHSTORE);
  KNO_INIT_FRESH_CONS(ps,kno_pathstore_type);
  ps->knops_id = u8_strdup(path);
  ps->knops_config = kno_incref(opts);
  ps->knops_prefix = get_string_opt(opts,KNOSYM(prefix));
  if (ps->knops_prefix)
    ps->knops_prefix_len = strlen(ps->knops_prefix);
  else ps->knops_prefix_len = -1;
  ps->knops_cacheroot = get_string_opt(opts,KNOSYM(cacheroot));
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

/* Initialization */

static int zpathstore_initialized = 0;

int kno_init_zpathstore_c()
{
  if (zpathstore_initialized) return 0;

  u8_register_source_file(_FILEINFO);

  zpathstore_initialized = u8_millitime();

  return 1;
}
