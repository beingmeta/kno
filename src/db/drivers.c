/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/drivers.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <stdio.h>

static fdtype rev_symbol, gentime_symbol, packtime_symbol, modtime_symbol;

fd_exception fd_MMAPError=_("MMAP Error");
fd_exception fd_MUNMAPError=_("MUNMAP Error");
fd_exception fd_CorruptedPool=_("Corrupted file pool");
fd_exception fd_InvalidOffsetType=_("Invalid offset type");
fd_exception fd_FileSizeOverflow=_("File pool overflowed file size");
fd_exception fd_RecoveryRequired=_("RECOVERY");

int fd_acid_files=1;
size_t fd_driver_bufsize=FD_DRIVER_BUFSIZE;

/* Matching word prefixes */

FD_EXPORT
int fd_match4bytes(u8_string file,void *data)
{
  u8_wideint magic_number = (u8_wideint) data;
  if (u8_file_existsp(file)) {
    FILE *f=u8_fopen(file,"rb");
    unsigned int i=0, word=0, read_only=0; unsigned char byte;
    if (f==NULL) return 0;
    byte=getc(f); word=word|(byte<<24);
    byte=getc(f); word=word|(byte<<16);
    byte=getc(f); word=word|(byte<<8);
    byte=getc(f); word=word|(byte<<0);
    fclose(f);
    if (word == magic_number)
      return 1;
    else return 0;}
  else return 0;
}

FD_EXPORT
int fd_netspecp(u8_string spec,void *ignored)
{
  u8_byte buf[64]; fdtype xname;
  u8_byte *at=strchr(spec,'@'), *col=strchr(at,':');
  if ((at==NULL)&&(col==NULL))
    return 0;
  else return 1;
}


/* Opening pools */

static struct FD_POOL_OPENER pool_openers[FD_N_POOL_OPENERS];
static int n_pool_openers=0;
#if FD_THREADS_ENABLED
static u8_mutex pool_openers_lock;
#endif

FD_EXPORT void fd_register_pool_opener
  (fd_pool_handler handler,
   fd_pool (*opener)(u8_string filename,fddb_flags flags),
   int (*matcher)(u8_string filename,void *),
   void *matcher_data)
{
  int i=0;
  u8_lock_mutex(&pool_openers_lock);
  while (i<n_pool_openers) {
    struct FD_POOL_OPENER *opener=&(pool_openers[i]);
    if ( ( opener->matcher == matcher ) &&
         ( opener->matcher_data == matcher_data ) ) {
      if ( (handler) && (opener->handler) &&
           (handler != opener->handler ) )
        u8_log(LOGCRIT,"InconsistentPoolHandlers",
               "Identical matchers for handlers %s (0x%llx) and %s (0x%llx)",
               opener->handler->name,(unsigned long long) (opener->handler),
               handler->name,(unsigned long long) (handler));
      else if ( opener->handler == NULL) {
        opener->handler=handler;}
      else {}
      return;}
    else i++;}
  if (n_pool_openers<FD_N_POOL_OPENERS) {
    pool_openers[n_pool_openers].handler=handler;
    pool_openers[n_pool_openers].opener=opener;
    pool_openers[n_pool_openers].matcher=matcher;
    pool_openers[n_pool_openers].matcher_data=matcher_data;
    n_pool_openers++;
    fd_unlock_mutex(&pool_openers_lock);}
  else {
    fd_unlock_mutex(&pool_openers_lock);
    u8_raise("Too many pool openers",
             "fd_register_pool_opener",
             NULL);}
}

FD_EXPORT
fd_pool fd_open_pool(u8_string spec,fddb_flags flags)
{
  fd_pool p;
  int i=0, n=n_pool_openers;
  while (i<n) {
    struct FD_POOL_OPENER *po=&pool_openers[i];
    if ((po->matcher) && (po->matcher(spec,po->matcher_data)))
      return po->opener(spec,flags);
    else i++;}
  fd_seterr(fd_NotAPool,"fd_open_pool",spec,FD_VOID);
  return NULL;
}

/* TODO */
FD_EXPORT
fd_pool fd_unregistered_file_pool(u8_string filename)
{
  return NULL;
}

/* Opening indices */

static struct FD_INDEX_OPENER index_openers[FD_N_INDEX_OPENERS];
static int n_index_openers=0;
#if FD_THREADS_ENABLED
static u8_mutex index_openers_lock;
#endif

FD_EXPORT void fd_register_index_opener
  (fd_index_handler handler,
   fd_index (*opener)(u8_string filename,fddb_flags flags),
   int (*matcher)(u8_string filename,void *),
   void *matcher_data)
{
  int i=0;
  u8_lock_mutex(&index_openers_lock);
  while (i<n_index_openers) {
    struct FD_INDEX_OPENER *opener=&(index_openers[i]);
    if ( ( opener->matcher == matcher ) &&
         ( opener->matcher_data == matcher_data ) ) {
      if ( (handler) && (opener->handler) &&
           (handler != opener->handler ) )
        u8_log(LOGCRIT,"InconsistentIndexHandlers",
               "Identical matchers for handlers %s (0x%llx) and %s (0x%llx)",
               opener->handler->name,(unsigned long long) (opener->handler),
               handler->name,(unsigned long long) (handler));
      else if ( opener->handler == NULL) {
        opener->handler=handler;}
      else {}
      return;}
    else i++;}
  if (n_index_openers<FD_N_INDEX_OPENERS) {
    index_openers[n_index_openers].handler=handler;
    index_openers[n_index_openers].opener=opener;
    index_openers[n_index_openers].matcher=matcher;
    index_openers[n_index_openers].matcher_data=matcher_data;
    n_index_openers++;
    fd_unlock_mutex(&index_openers_lock);}
  else {
    fd_unlock_mutex(&index_openers_lock);
    u8_raise("Too many index openers",
             "fd_register_index_opener",
             NULL);}
}

FD_EXPORT
fd_index fd_open_index(u8_string spec,fddb_flags flags)
{
  fd_index p;
  int i=0, n=n_index_openers;
  while (i<n) {
    struct FD_INDEX_OPENER *po=&index_openers[i];
    if ((po->matcher) && (po->matcher(spec,po->matcher_data)))
      return po->opener(spec,flags);
    else i++;}
  /* TODO */
  fd_seterr("NotAnIndex","fd_get_index",spec,FD_VOID);
  return NULL;
}

FD_EXPORT
int fd_file_indexp(u8_string filename)
{
  return 1;
}

/* Reading memindices from disk */

static int memindex_commitfn(struct FD_MEM_INDEX *ix,u8_string file)
{
  struct FD_BYTESTREAM stream, *rstream;
  if ((ix->index_adds.table_n_keys>0) || (ix->index_edits.table_n_keys>0)) {
    rstream=fd_init_file_bytestream
      (&stream,file,FD_BYTESTREAM_CREATE,fd_driver_bufsize);
    if (rstream==NULL) return -1;
    stream.stream_mallocd=0;
    fd_write_dtype(fd_writebuf(&stream),(fdtype)&(ix->index_cache));
    fd_close_bytestream(&stream,FD_BYTESTREAM_FREE);
    return 1;}
  else return 0;
}

/* Initialization */

static int drivers_c_initialized=0;

FD_EXPORT int fd_init_drivers_c()
{
  if (drivers_c_initialized) return drivers_c_initialized;
  drivers_c_initialized=307*fd_init_dblib();

  u8_register_source_file(_FILEINFO);

  rev_symbol=fd_intern("REV");
  gentime_symbol=fd_intern("GENTIME");
  packtime_symbol=fd_intern("PACKTIME");
  modtime_symbol=fd_intern("MODTIME");

#if FD_THREADS_ENABLED
  fd_init_mutex(&pool_openers_lock);
  fd_init_mutex(&index_openers_lock);
#endif

  fd_file_pool_opener=fd_open_pool;
  fd_file_index_opener=fd_open_index;

  fd_register_config("ACIDFILES",
                     "Maintain acidity of individual file pools and indices",
                     fd_boolconfig_get,fd_boolconfig_set,&fd_acid_files);
  fd_register_config("DRIVERBUFSIZE",
                     "The size of file streams used in database files",
                     fd_sizeconfig_get,fd_sizeconfig_set,&fd_driver_bufsize);


  return drivers_c_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then make -C ../.. debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
