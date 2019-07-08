/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/streams.h"
#include "kno/support.h"
#include "kno/getsource.h"

#include <libu8/libu8io.h>
#include <libu8/u8exceptions.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* Getting sources */

static struct KNO_SOURCEFN *sourcefns = NULL;
static u8_mutex sourcefns_lock;

KNO_EXPORT u8_string kno_get_source
(u8_string path,u8_string enc,u8_string *basepathp,time_t *timep,
 ssize_t *sizep)
{
  struct KNO_STRING _tmpstring;
  lispval lpath = kno_init_string(&_tmpstring,-1,path);
  KNO_INIT_STACK_CONS(&_tmpstring,kno_string_type);
  struct KNO_SOURCEFN *scan = sourcefns;
  while (scan) {
    u8_string basepath = NULL;
    u8_string data = scan->getsource
      (1,lpath,enc,&basepath,timep,sizep,scan->getsource_data);
    if (data) {
      *basepathp = basepath;
      kno_clear_errors(0);
      return data;}
    else scan = scan->getsource_next;}
  return NULL;
}

KNO_EXPORT int kno_probe_source
(u8_string path,u8_string *basepathp,time_t *timep,ssize_t *sizep)
{
  struct KNO_STRING _tmpstring;
  lispval lpath = kno_init_string(&_tmpstring,-1,path);
  KNO_INIT_STATIC_CONS(&_tmpstring,kno_string_type);
  struct KNO_SOURCEFN *scan = sourcefns;
  while (scan) {
    u8_string basepath = NULL;
    u8_string data = scan->getsource
      (0,lpath,NULL,&basepath,timep,sizep,scan->getsource_data);
    if (data) {
      *basepathp = basepath;
      kno_clear_errors(0);
      return 1;}
    else scan = scan->getsource_next;}
  return 0;
}

KNO_EXPORT void kno_register_sourcefn
(u8_string (*fn)(int op,lispval,u8_string,u8_string *,
		 time_t *,ssize_t *,
		 void *),
 void *sourcefn_data)
{
  struct KNO_SOURCEFN *new_entry = u8_alloc(struct KNO_SOURCEFN);
  u8_lock_mutex(&sourcefns_lock);
  new_entry->getsource = fn;
  new_entry->getsource_next = sourcefns;
  new_entry->getsource_data = sourcefn_data;
  sourcefns = new_entry;
  u8_unlock_mutex(&sourcefns_lock);
}

/* Getting file sources */

static u8_string file_source_fn(int fetch,lispval pathspec,u8_string encname,
				u8_string *abspath,time_t *timep,ssize_t *sizep,
				void *ignored)
{
  u8_string filename = NULL;
  if (KNO_STRINGP(pathspec)) {
    u8_string path = KNO_CSTRING(pathspec);
    if (strncmp(path,"file:",5)==0)
      filename = path+5;
    else if (strchr(path,':')!=NULL)
      return NULL;
    else filename = path;}
  else return NULL;
  if (fetch) {
    u8_string data = u8_filestring(filename,encname);
    if (data) {
      if (abspath) *abspath = u8_realpath(filename,NULL); /* u8_abspath? */
      if (timep) *timep = u8_file_mtime(filename);
      if (sizep) *sizep = u8_file_size(filename);
      return data;}
    else return NULL;}
  else {
    time_t mtime = u8_file_mtime(filename);
    if (mtime<0) return NULL;
    if (abspath) *abspath = u8_realpath(filename,NULL); /* u8_abspath? */
    if (sizep) *sizep = u8_file_size(filename);
    if (timep) *timep = mtime;
    return "exists";}
}

KNO_EXPORT void kno_init_getsource_c()
{
  u8_init_mutex(&sourcefns_lock);
  kno_register_sourcefn(file_source_fn,NULL);
}
