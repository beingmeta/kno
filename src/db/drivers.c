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
#include "framerd/indexes.h"
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
size_t fd_driver_bufsize=FD_DBDRIVER_BUFSIZE;

/* Matching word prefixes */

FD_EXPORT
u8_string fd_match4bytes(u8_string file,void *data)
{
  u8_wideint magic_number = (u8_wideint) data;
  if (u8_file_existsp(file)) {
    FILE *f=u8_fopen(file,"rb");
    unsigned int word=0; unsigned char byte;
    if (f==NULL) return 0;
    byte=getc(f); word=word|(byte<<24);
    byte=getc(f); word=word|(byte<<16);
    byte=getc(f); word=word|(byte<<8);
    byte=getc(f); word=word|(byte<<0);
    fclose(f);
    if (word == magic_number)
      return file;
    else return NULL;}
  else return NULL;
}

FD_EXPORT
u8_string fd_netspecp(u8_string spec,void *ignored)
{
  u8_byte *at=strchr(spec,'@'), *col=strchr(spec,':');
  if ((at==NULL)&&(col==NULL))
    return NULL;
  else return spec;
}


/* Opening pools */

static struct FD_POOL_TYPEINFO *pool_typeinfo;
#if FD_THREADS_ENABLED
static u8_mutex pool_typeinfo_lock;
#endif

FD_EXPORT void fd_register_pool_type
  (u8_string name,
   fd_pool_handler handler,
   fd_pool (*opener)(u8_string filename,fddb_flags flags),
   u8_string (*matcher)(u8_string filename,void *),
   void *type_data)
{
  struct FD_POOL_TYPEINFO *ptype;
  u8_lock_mutex(&pool_typeinfo_lock);
  ptype=pool_typeinfo; while (ptype) {
    if (strcasecmp(name,ptype->pool_typename)==0) {
      if ((matcher) && (ptype->matcher) && (matcher!=ptype->matcher))
        u8_log(LOGWARN,"PoolTypeInconsistency",
               "Attempt to redefine pool type '%s' with different matcher",
               name);
      else if ((type_data) && (ptype->type_data) &&
               (type_data!=ptype->type_data))
        u8_log(LOGWARN,"PoolTypeInconsistency",
               "Attempt to redefine pool type '%s' with different type data",
               name);
      else if ((handler) && (ptype->handler) &&
               (handler!=ptype->handler))
        u8_log(LOGWARN,"PoolTypeInconsistency",
               "Attempt to redefine pool type '%s' with different handler",
               name);
      else break;}
    else ptype=ptype->next_type;}
  if (ptype) {
    fd_unlock_mutex(&pool_typeinfo_lock);
    return;}
  ptype=u8_alloc(struct FD_POOL_TYPEINFO);
  ptype->pool_typename=u8_strdup(name);
  ptype->handler=handler;
  ptype->opener=opener;
  ptype->matcher=matcher;
  ptype->type_data=type_data;
  ptype->next_type=pool_typeinfo;
  pool_typeinfo=ptype;
  fd_unlock_mutex(&pool_typeinfo_lock);
}

static fd_pool_typeinfo get_pool_typeinfo(u8_string name)
{
  struct FD_POOL_TYPEINFO *ptype=pool_typeinfo; 
  while (ptype) {
    if (strcasecmp(name,ptype->pool_typename)==0)
      return ptype;
    else ptype=ptype->next_type;}
  return NULL;
}

FD_EXPORT
fd_pool fd_open_pool(u8_string spec,fddb_flags flags)
{
  struct FD_POOL_TYPEINFO *ptype;
  ptype=pool_typeinfo; while (ptype) {
    if (ptype->matcher) {
      u8_string use_spec=ptype->matcher(spec,ptype->type_data);
      if (use_spec) {
        fd_pool opened=ptype->opener(use_spec,flags);
        if (use_spec!=spec) u8_free(use_spec);
        return opened;}
      else ptype=ptype->next_type;}
    else ptype=ptype->next_type;}
  fd_seterr(fd_UnknownPoolType,"fd_open_pool",spec,FD_VOID);
  return NULL;
}

FD_EXPORT
fd_pool_handler fd_get_pool_handler(u8_string name)
{
  struct FD_POOL_TYPEINFO *ptype=get_pool_typeinfo(name);
  if (ptype)
    return ptype->handler;
  else return NULL;
}

FD_EXPORT
fd_pool fd_make_pool(
                     u8_string spec,
                     u8_string pooltype,
                     fddb_flags flags,
                     fdtype opts)
{
  fd_pool_typeinfo ptype=get_pool_typeinfo(pooltype);
  if (ptype==NULL) {
    fd_seterr(fd_UnknownPoolType,"fd_make_pool",pooltype,FD_VOID);
    return NULL;}
  else if (ptype->handler==NULL) {
    fd_seterr(_("NoPoolHandler"),"fd_make_pool",pooltype,FD_VOID);
    return NULL;}
  else if (ptype->handler->create==NULL) {
    fd_seterr(_("NoCreateHandler"),"fd_make_pool",pooltype,FD_VOID);
    return NULL;}
  else return ptype->handler->create(spec,ptype->type_data,flags,opts);
}

/* TODO */
FD_EXPORT
fd_pool fd_unregistered_file_pool(u8_string filename)
{
  return NULL;
}

/* Opening indexes */

static struct FD_INDEX_TYPEINFO *index_typeinfo;
static int n_index_types=0;
#if FD_THREADS_ENABLED
static u8_mutex index_typeinfo_lock;
#endif

FD_EXPORT void fd_register_index_type
  (u8_string name,
   fd_index_handler handler,
   fd_index (*opener)(u8_string filename,fddb_flags flags),
   u8_string (*matcher)(u8_string filename,void *),
   void *type_data)
{
  struct FD_INDEX_TYPEINFO *ixtype;
  u8_lock_mutex(&index_typeinfo_lock);
  ixtype=index_typeinfo; while (ixtype) {
    if (strcasecmp(name,ixtype->index_typename)==0) {
      if ((matcher) && (ixtype->matcher) && (matcher!=ixtype->matcher))
        u8_log(LOGWARN,"IndexTypeInconsistency",
               "Attempt to redefine index type '%s' with different matcher",
               name);
      else if ((type_data) && (ixtype->type_data) &&
               (type_data!=ixtype->type_data))
        u8_log(LOGWARN,"IndexTypeInconsistency",
               "Attempt to redefine index type '%s' with different type data",
               name);
      else if ((handler) && (ixtype->handler) &&
               (handler!=ixtype->handler))
        u8_log(LOGWARN,"IndexTypeInconsistency",
               "Attempt to redefine index type '%s' with different handler",
               name);
      else break;}
    else ixtype=ixtype->next_type;}
  if (ixtype) {
    fd_unlock_mutex(&index_typeinfo_lock);
    return;}
  ixtype=u8_alloc(struct FD_INDEX_TYPEINFO);
  ixtype->index_typename=u8_strdup(name);
  ixtype->handler=handler;
  ixtype->opener=opener;
  ixtype->matcher=matcher;
  ixtype->type_data=type_data;
  ixtype->next_type=index_typeinfo;
  index_typeinfo=ixtype;
  fd_unlock_mutex(&index_typeinfo_lock);
}

static fd_index_typeinfo get_index_typeinfo(u8_string name)
{
  struct FD_INDEX_TYPEINFO *ixtype=index_typeinfo;
  while (ixtype) {
    if (strcasecmp(name,ixtype->index_typename)==0)
      return ixtype;
    else ixtype=ixtype->next_type;}
  return NULL;
}

FD_EXPORT
fd_index fd_open_index(u8_string spec,fddb_flags flags)
{
  struct FD_INDEX_TYPEINFO *ixtype;
  ixtype=index_typeinfo; while (ixtype) {
    if (ixtype->matcher) {
      u8_string use_spec=ixtype->matcher(spec,ixtype->type_data);
      if (use_spec) {
        fd_index opened=ixtype->opener(use_spec,flags);
        if (use_spec!=spec) u8_free(use_spec);
        return opened;}
      else ixtype=ixtype->next_type;}
    else ixtype=ixtype->next_type;}
  return NULL;
}

FD_EXPORT
fd_index_handler fd_get_index_handler(u8_string name)
{
  struct FD_INDEX_TYPEINFO *ixtype=index_typeinfo;
  while (ixtype) {
    if ((strcasecmp(name,ixtype->index_typename))==0)
      return ixtype->handler;
    else ixtype=ixtype->next_type;}
  return NULL;
}

FD_EXPORT
fd_index fd_make_index(
                       u8_string spec,
                       u8_string indextype,
                       fddb_flags flags,
                       fdtype opts)
{
  fd_index_typeinfo ixtype=get_index_typeinfo(indextype);
  if (ixtype==NULL) {
    fd_seterr(_("UnknownIndexType"),"fd_make_index",indextype,FD_VOID);
    return NULL;}
  else if (ixtype->handler==NULL) {
    fd_seterr(_("NoIndexHandler"),"fd_make_index",indextype,FD_VOID);
    return NULL;}
  else if (ixtype->handler->create==NULL) {
    fd_seterr(_("NoCreateHandler"),"fd_make_index",indextype,FD_VOID);
    return NULL;}
  else return ixtype->handler->create(spec,ixtype->type_data,flags,opts);
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
  fd_init_mutex(&pool_typeinfo_lock);
  fd_init_mutex(&index_typeinfo_lock);
#endif

  fd_file_pool_type=fd_open_pool;
  fd_file_index_type=fd_open_index;

  fd_register_config("ACIDFILES",
                     "Maintain acidity of individual file pools and indexes",
                     fd_boolconfig_get,fd_boolconfig_set,&fd_acid_files);
  fd_register_config("DRIVERBUFSIZE",
                     "The size of file streams used in database files",
                     fd_sizeconfig_get,fd_sizeconfig_set,&fd_driver_bufsize);


  return drivers_c_initialized;
}

/* Needs to be replaced */

/*
static int load_pool_cache(fd_pool p,void *ignored)
{
  if ((p->pool_handler==NULL) ||
      (p->pool_handler->name==NULL))
    return 0;
  else if (strcmp(p->pool_handler->name,"file_pool")==0) {
    struct FD_FILE_POOL *fp=(struct FD_FILE_POOL *)p;
    if (fp->pool_offsets) load_cache(fp->pool_offsets,fp->pool_offsets_size);}
  else if (strcmp(p->pool_handler->name,"oidpool")==0) {
    struct FD_OIDPOOL *fp=(struct FD_OIDPOOL *)p;
    if (fp->pool_offsets)
      load_cache(fp->pool_offsets,fp->pool_offsets_size);}
  return 0;
}

static int load_index_cache(fd_index ix,void *ignored)
{
  if ((ix->index_handler==NULL) || (ix->index_handler->name==NULL)) return 0;
  else if (strcmp(ix->index_handler->name,"file_index")==0) {
    struct FD_FILE_INDEX *fx=(struct FD_FILE_INDEX *)ix;
    if (fx->index_offsets) load_cache(fx->index_offsets,fx->index_n_slots);}
  else if (strcmp(ix->index_handler->name,"hash_index")==0) {
    struct FD_HASH_INDEX *hx=(struct FD_HASH_INDEX *)ix;
    if (hx->index_offdata)
      load_cache(hx->index_offdata,hx->index_n_buckets*2);}
  return 0;
}

static fdtype load_caches_prim(fdtype arg)
{
  if (FD_VOIDP(arg)) {
    fd_for_pools(load_pool_cache,NULL);
    fd_for_indexes(load_index_cache,NULL);}
  else if (FD_TYPEP(arg,fd_index_type))
    load_index_cache(fd_indexptr(arg),NULL);
  else if (FD_TYPEP(arg,fd_pool_type))
    load_pool_cache(fd_lisp2pool(arg),NULL);
  else {}
  return FD_VOID;
}

  fd_idefn(driverfns_module,fd_make_cprim1("LOAD-CACHES",load_caches_prim,0));

*/

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
