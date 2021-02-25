/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/pathstore.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>

static u8_string get_relpath(kno_pathstore ps,u8_string path)
{
  if (ps->knops_mountpoint == NULL)
    return path;
  else if (strncmp(path,ps->knops_mountpoint,ps->knops_mountpoint_len)==0) {
    char termchar = path[ps->knops_mountpoint_len];
    if (termchar == '\0')
      return NULL;
    else if (termchar == '/')
      return path + ps->knops_mountpoint_len + 1;
    else return path + ps->knops_mountpoint_len;}
  else return NULL;
}

KNO_EXPORT int knops_existsp(kno_pathstore ps,u8_string path)
{
  u8_string relpath = get_relpath(ps,path);
  if (relpath)
    return (ps->knops_handlers->existsp)(ps,relpath);
  else return 0;
}

KNO_EXPORT lispval knops_pathinfo(kno_pathstore ps,u8_string path,int follow)
{
  u8_string relpath = get_relpath(ps,path);
  if (relpath)
    return (ps->knops_handlers->info)(ps,relpath,follow);
  else return KNO_FALSE;
}

KNO_EXPORT lispval knops_content(kno_pathstore ps,u8_string path,
				 u8_string enc,int follow)
{
  u8_string relpath = get_relpath(ps,path);
  if (relpath)
    return (ps->knops_handlers->content)(ps,relpath,enc,follow);
  else return KNO_FALSE;
}

static void recycle_pathstore(struct KNO_RAW_CONS *c)
{
  kno_pathstore ps = (kno_pathstore) c;
  if (ps->knops_handlers->recycle) ps->knops_handlers->recycle(ps);
  if (ps->knops_cacheroot) u8_free(ps->knops_cacheroot);
  if (ps->knops_mountpoint) u8_free(ps->knops_mountpoint);
  u8_free(ps->knops_id);
  kno_decref(ps->knops_config);
  kno_recycle_hashtable(&(ps->knops_cache));
  u8_free(ps);
}

static int unparse_pathstore(u8_output out,lispval x)
{
  kno_pathstore ps = (kno_pathstore) x;
  u8_printf(out,"#<PATHSTORE %s (%s) #!0x%llx>",
	    ps->knops_id,
	    ps->knops_handlers->typeid,
	    (kno_ptrval)ps);
  return 1;
}

/* Initialization */

static int sys_pathstore_c_init_done = 0;

void kno_init_pathstore_c()
{
  if (sys_pathstore_c_init_done)
    return;

  kno_recyclers[kno_pathstore_type] = recycle_pathstore;
  kno_unparsers[kno_pathstore_type] = unparse_pathstore;

  sys_pathstore_c_init_done=1;

  u8_register_source_file(_FILEINFO);
}

