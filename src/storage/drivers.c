/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/drivers.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <stdio.h>

static lispval rev_symbol, gentime_symbol, packtime_symbol, modtime_symbol;
static lispval adjuncts_symbol;

lispval fd_cachelevel_op, fd_bufsize_op, fd_mmap_op, fd_preload_op;
lispval fd_metadata_op, fd_raw_metadata_op, fd_reload_op;
lispval fd_stats_op, fd_label_op, fd_populate_op, fd_swapout_op;
lispval fd_getmap_op, fd_slotids_op, fd_baseoids_op;
lispval fd_load_op, fd_capacity_op;
lispval fd_keys_op;

u8_condition fd_MMAPError=_("MMAP Error");
u8_condition fd_MUNMAPError=_("MUNMAP Error");
u8_condition fd_CorruptedPool=_("Corrupted file pool");
u8_condition fd_InvalidOffsetType=_("Invalid offset type");
u8_condition fd_RecoveryRequired=_("RECOVERY");

u8_condition fd_CantOpenPool=_("Can't open pool");
u8_condition fd_CantFindPool=_("Can't find pool");
u8_condition fd_CantOpenIndex=_("Can't open index");
u8_condition fd_CantFindIndex=_("Can't find index");

u8_condition fd_IndexDriverError=_("Internal error with index file");
u8_condition fd_PoolDriverError=_("Internal error with index file");

u8_condition fd_PoolFileSizeOverflow=_("file pool overflowed file size");
u8_condition fd_FileIndexSizeOverflow=_("file index overflowed file size");

int fd_acid_files = 1;
size_t fd_driver_bufsize = FD_STORAGE_DRIVER_BUFSIZE;

static fd_pool_typeinfo default_pool_type = NULL;
static fd_index_typeinfo default_index_type = NULL;

#define CHECK_ERRNO U8_CLEAR_ERRNO

/* Matching word prefixes */

FD_EXPORT
u8_string fd_match4bytes(u8_string file,void *data)
{
  u8_wideint magic_number = (u8_wideint) data;
  if (u8_file_existsp(file)) {
    FILE *f = u8_fopen(file,"rb");
    unsigned int word = 0; unsigned char byte;
    if (f == NULL) return 0;
    byte = getc(f); word = word|(byte<<24);
    byte = getc(f); word = word|(byte<<16);
    byte = getc(f); word = word|(byte<<8);
    byte = getc(f); word = word|(byte<<0);
    fclose(f);
    if (word == magic_number)
      return file;
    else return NULL;}
  else return NULL;
}

FD_EXPORT
u8_string fd_netspecp(u8_string spec,void *ignored)
{
  u8_byte *at = strchr(spec,'@'), *col = strchr(spec,':');
  if ((at == NULL)&&(col == NULL))
    return NULL;
  else return spec;
}

/* Opening pools */

static struct FD_POOL_TYPEINFO *pool_typeinfo;
static u8_mutex pool_typeinfo_lock;

FD_EXPORT void fd_register_pool_type
(u8_string name,
 fd_pool_handler handler,
 fd_pool (*opener)(u8_string filename,fd_storage_flags flags,lispval opts),
 u8_string (*matcher)(u8_string filename,void *),
 void *type_data)
{
  struct FD_POOL_TYPEINFO *ptype;
  u8_lock_mutex(&pool_typeinfo_lock);
  ptype = pool_typeinfo; while (ptype) {
    if (strcasecmp(name,ptype->pool_typename)==0) {
      if ((matcher) && (ptype->matcher) && (matcher!=ptype->matcher))
        u8_logf(LOG_WARN,"PoolTypeInconsistency",
                "Attempt to redefine pool type '%s' with different matcher",
                name);
      else if ((type_data) && (ptype->type_data) &&
               (type_data!=ptype->type_data))
        u8_logf(LOG_WARN,"PoolTypeInconsistency",
                "Attempt to redefine pool type '%s' with different type data",
                name);
      else if ((handler) && (ptype->handler) &&
               (handler!=ptype->handler))
        u8_logf(LOG_WARN,"PoolTypeInconsistency",
                "Attempt to redefine pool type '%s' with different handler",
                name);
      else break;}
    else ptype = ptype->next_type;}
  if (ptype) {
    u8_unlock_mutex(&pool_typeinfo_lock);
    return;}
  ptype = u8_alloc(struct FD_POOL_TYPEINFO);
  ptype->pool_typename = u8_strdup(name);
  ptype->handler = handler;
  ptype->opener = opener;
  ptype->matcher = matcher;
  ptype->type_data = type_data;
  ptype->next_type = pool_typeinfo;
  pool_typeinfo = ptype;
  u8_unlock_mutex(&pool_typeinfo_lock);
}

FD_EXPORT
fd_pool_typeinfo fd_get_pool_typeinfo(u8_string name)
{
  struct FD_POOL_TYPEINFO *ptype = pool_typeinfo;
  if (name == NULL)
    return default_pool_type;
  else while (ptype) {
      if (strcasecmp(name,ptype->pool_typename)==0)
        return ptype;
      else ptype = ptype->next_type;}
  return NULL;
}

FD_EXPORT fd_pool_typeinfo fd_set_default_pool_type(u8_string id)
{
  fd_pool_typeinfo info = (id) ?  (fd_get_pool_typeinfo(id)) :
    (default_pool_type);
  if (info)
    default_pool_type = info;
  return info;
}

static fd_pool open_pool(fd_pool_typeinfo ptype,u8_string spec,
                         fd_storage_flags flags,lispval opts)
{
  fd_pool found = (flags&FD_STORAGE_UNREGISTERED) ? (NULL) :
    (fd_find_pool_by_source(spec));
  fd_pool opened = (found) ? (found) :
    (ptype->opener(spec,flags,opts));
  if (opened==NULL) {
    fd_seterr(fd_CantOpenPool,"fd_open_pool",spec,opts);
    return opened;}
  else if (fd_testopt(opts,adjuncts_symbol,VOID)) {
    lispval adjuncts=fd_getopt(opts,adjuncts_symbol,EMPTY);
    int rv=fd_set_adjuncts(opened,adjuncts);
    fd_decref(adjuncts);
    if (rv<0) {
      if (flags & FD_STORAGE_NOERR) {
        u8_logf(LOG_CRIT,fd_AdjunctError,
                "Opening pool '%s' with opts=%q",
                opened->poolid,opts);
        fd_clear_errors(1);}
      else {
        fd_seterr(fd_AdjunctError,"fd_open_pool",spec,opts);
        return NULL;}}}
  lispval old_opts=opened->pool_opts;
  opened->pool_opts=fd_incref(opts);
  fd_decref(old_opts);
  return opened;
}

FD_EXPORT
fd_pool fd_open_pool(u8_string spec,fd_storage_flags flags,lispval opts)
{
  CHECK_ERRNO();
  lispval pool_typeid = fd_getopt(opts,FDSYM_TYPE,FD_VOID);
  struct FD_POOL_TYPEINFO *ptype = (FD_STRINGP(pool_typeid)) ?
    (fd_get_pool_typeinfo(FD_CSTRING(pool_typeid))) :
    (FD_SYMBOLP(pool_typeid)) ? 
    (fd_get_pool_typeinfo(FD_SYMBOL_NAME(pool_typeid))) :
    (NULL);
  if (flags<0) flags = fd_get_dbflags(opts,FD_STORAGE_ISPOOL);
  if (ptype) {
    u8_string use_spec = 
      (ptype->matcher) ? (ptype->matcher(spec,ptype->type_data)) : (spec);
    fd_pool p = open_pool(ptype,use_spec,flags,opts);
    if (use_spec != spec) u8_free(use_spec);
    return p;}
  else ptype = pool_typeinfo; while (ptype) {
    if (ptype->matcher) {
      u8_string use_spec = ptype->matcher(spec,ptype->type_data);
      if (use_spec) {
        fd_pool found = (flags&FD_STORAGE_UNREGISTERED) ? (NULL) :
          (fd_find_pool_by_source(use_spec));
        fd_pool opened = (found) ? (found) :
          (ptype->opener(use_spec,flags,opts));
        if (use_spec!=spec) u8_free(use_spec);
        if (opened==NULL) {
          fd_seterr(fd_CantOpenPool,"fd_open_pool",spec,opts);
          return opened;}
        else if (fd_testopt(opts,adjuncts_symbol,VOID)) {
          lispval adjuncts=fd_getopt(opts,adjuncts_symbol,EMPTY);
          int rv=fd_set_adjuncts(opened,adjuncts);
          fd_decref(adjuncts);
          if (rv<0) {
            if (flags & FD_STORAGE_NOERR) {
              u8_logf(LOG_CRIT,fd_AdjunctError,
                      "Opening pool '%s' with opts=%q",
                      opened->poolid,opts);
              fd_clear_errors(1);}
            else {
              fd_seterr(fd_AdjunctError,"fd_open_pool",spec,opts);
              return NULL;}}}
        lispval old_opts=opened->pool_opts;
        opened->pool_opts=fd_incref(opts);
        fd_decref(old_opts);
        return opened;}
      CHECK_ERRNO();}
    ptype = ptype->next_type;}
  if (!(flags & FD_STORAGE_NOERR))
    fd_seterr(fd_CantFindPool,"fd_open_pool",spec,opts);
  return NULL;
}

FD_EXPORT
fd_pool_handler fd_get_pool_handler(u8_string name)
{
  struct FD_POOL_TYPEINFO *ptype = fd_get_pool_typeinfo(name);
  if (ptype)
    return ptype->handler;
  else return NULL;
}

typedef unsigned long long ull;

static int fix_pool_opts(u8_string spec,lispval opts)
{
  lispval base_oid = fd_getopt(opts,fd_intern("BASE"),VOID);
  lispval capacity_arg = fd_getopt(opts,fd_intern("CAPACITY"),VOID);
  if (!(OIDP(base_oid))) {
    fd_seterr("PoolBaseNotOID","fd_make_pool",spec,opts);
    return -1;}
  else if (!(FIXNUMP(capacity_arg))) {
    fd_seterr("PoolCapacityNotFixnum","fd_make_pool",spec,opts);
    return -1;}
  else {
    FD_OID addr=FD_OID_ADDR(base_oid);
    unsigned int lo=FD_OID_LO(addr);
    int capacity=FD_FIX2INT(capacity_arg);
    if (capacity<=0) {
      fd_seterr("NegativePoolCapacity","fd_make_pool",spec,opts);
      return -1;}
    else if (capacity>=0x40000000) {
      fd_seterr("PoolCapacityTooLarge","fd_make_pool",spec,opts);
      return -1;}
    else {}
    int span_pool=(capacity>FD_OID_BUCKET_SIZE);
    if ((span_pool)&&(lo%FD_OID_BUCKET_SIZE)) {
      fd_seterr("MisalignedBaseOID","fd_make_pool",spec,opts);
      return -1;}
    else if ((span_pool)&&(capacity%FD_OID_BUCKET_SIZE)) {
      lispval opt_root=opts;
      unsigned int new_capacity =
        (1+(capacity/FD_OID_BUCKET_SIZE))*FD_OID_BUCKET_SIZE;
      u8_logf(LOG_WARN,"FixingCapacity",
              "Rounding up the capacity of %s from %llu to 0x%llx",
              spec,(ull)capacity,(ull)new_capacity);
      if (FD_PAIRP(opts)) opt_root=FD_CAR(opts);
      int rv=fd_store(opt_root,fd_intern("CAPACITY"),
                      FD_INT(new_capacity));
      return rv;}
    /* TODO: Add more checks for non-spanning pools */
    else return 1;}
}

FD_EXPORT
fd_pool fd_make_pool(u8_string spec,
                     u8_string pooltype,
                     fd_storage_flags flags,
                     lispval opts)
{
  fd_pool_typeinfo ptype = fd_get_pool_typeinfo(pooltype);
  if (ptype == NULL) {
    fd_seterr3(fd_UnknownPoolType,"fd_make_pool",pooltype);
    return NULL;}
  else if (ptype->handler == NULL) {
    fd_seterr3(_("NoPoolHandler"),"fd_make_pool",pooltype);
    return NULL;}
  else if (ptype->handler->create == NULL) {
    fd_seterr3(_("NoCreateHandler"),"fd_make_pool",pooltype);
    return NULL;}
  else if (fix_pool_opts(spec,opts)<0)
    return NULL;
  else return ptype->handler->create(spec,ptype->type_data,flags,opts);
}

/* Opening indexes */

static struct FD_INDEX_TYPEINFO *index_typeinfo;
static u8_mutex index_typeinfo_lock;

FD_EXPORT void fd_register_index_type
(u8_string name,
 fd_index_handler handler,
 fd_index (*opener)(u8_string filename,fd_storage_flags flags,lispval opts),
 u8_string (*matcher)(u8_string filename,void *),
 void *type_data)
{
  struct FD_INDEX_TYPEINFO *ixtype;
  u8_lock_mutex(&index_typeinfo_lock);
  ixtype = index_typeinfo; while (ixtype) {
    if (strcasecmp(name,ixtype->index_typename)==0) {
      if ((matcher) && (ixtype->matcher) && (matcher!=ixtype->matcher))
        u8_logf(LOG_WARN,"IndexTypeInconsistency",
                "Attempt to redefine index type '%s' with different matcher",
                name);
      else if ((type_data) && (ixtype->type_data) &&
               (type_data!=ixtype->type_data))
        u8_logf(LOG_WARN,"IndexTypeInconsistency",
                "Attempt to redefine index type '%s' with different type data",
                name);
      else if ((handler) && (ixtype->handler) &&
               (handler!=ixtype->handler))
        u8_logf(LOG_WARN,"IndexTypeInconsistency",
                "Attempt to redefine index type '%s' with different handler",
                name);
      else break;}
    else ixtype = ixtype->next_type;}
  if (ixtype) {
    u8_unlock_mutex(&index_typeinfo_lock);
    return;}
  ixtype = u8_alloc(struct FD_INDEX_TYPEINFO);
  ixtype->index_typename = u8_strdup(name);
  ixtype->handler = handler;
  ixtype->opener = opener;
  ixtype->matcher = matcher;
  ixtype->type_data = type_data;
  ixtype->next_type = index_typeinfo;
  index_typeinfo = ixtype;
  u8_unlock_mutex(&index_typeinfo_lock);
}

FD_EXPORT fd_index_typeinfo fd_get_index_typeinfo(u8_string name)
{
  struct FD_INDEX_TYPEINFO *ixtype = index_typeinfo;
  if (name == NULL)
    return default_index_type;
  else while (ixtype) {
      if (strcasecmp(name,ixtype->index_typename)==0)
        return ixtype;
      else ixtype = ixtype->next_type;}
  return NULL;
}

FD_EXPORT fd_index_typeinfo fd_set_default_index_type(u8_string id)
{
  fd_index_typeinfo info = (id) ? (fd_get_index_typeinfo(id)) :
    (default_index_type);
  if (info)
    default_index_type=info;
  return info;
}


static fd_index open_index(fd_index_typeinfo ixtype,u8_string spec,
                           fd_storage_flags flags,lispval opts)
{
  fd_index found = (flags&FD_STORAGE_UNREGISTERED) ? (NULL) :
    (fd_find_index_by_source(spec));
  fd_index opened = (found) ? (found) : (ixtype->opener(spec,flags,opts));
  if (opened==NULL) {
    fd_seterr(fd_CantOpenIndex,"fd_open_index",spec,opts);
    return opened;}
  lispval old_opts=opened->index_opts;
  opened->index_opts=fd_incref(opts);
  fd_decref(old_opts);
  return opened;
}

FD_EXPORT
fd_index fd_open_index(u8_string spec,fd_storage_flags flags,lispval opts)
{
  CHECK_ERRNO();
  lispval index_typeid = fd_getopt(opts,FDSYM_TYPE,FD_VOID);
  struct FD_INDEX_TYPEINFO *ixtype = (FD_STRINGP(index_typeid)) ?
    (fd_get_index_typeinfo(FD_CSTRING(index_typeid))) :
    (FD_SYMBOLP(index_typeid)) ? 
    (fd_get_index_typeinfo(FD_SYMBOL_NAME(index_typeid))) :
    (NULL);
  if (flags<0) flags = fd_get_dbflags(opts,FD_STORAGE_ISINDEX);
  if (ixtype) {
    u8_string use_spec = 
      (ixtype->matcher) ? (ixtype->matcher(spec,ixtype->type_data)) : (spec);
    fd_index ix = open_index(ixtype,use_spec,flags,opts);
    if (use_spec != spec) u8_free(use_spec);
    return ix;}
  else ixtype = index_typeinfo; while (ixtype) {
    if (ixtype->matcher) {
      u8_string use_spec = ixtype->matcher(spec,ixtype->type_data);
      if (use_spec) {
        fd_index found = (flags&FD_STORAGE_UNREGISTERED) ? (NULL) :
          (fd_find_index_by_source(use_spec));
        fd_index opened = (found) ? (found) :
          (ixtype->opener(use_spec,flags,opts));
        if (opened==NULL) {
          fd_seterr(fd_CantOpenIndex,"fd_open_index",spec,opts);
          return opened;}
        if (use_spec!=spec) u8_free(use_spec);
        lispval old_opts=opened->index_opts;
        opened->index_opts=fd_incref(opts);
        fd_decref(old_opts);
        return opened;}
      else ixtype = ixtype->next_type;
      CHECK_ERRNO();}
    else ixtype = ixtype->next_type;}
  if (!(flags & FD_STORAGE_NOERR))
    fd_seterr(fd_CantOpenIndex,"fd_open_index",spec,opts);
  return NULL;
}

FD_EXPORT
fd_index_handler fd_get_index_handler(u8_string name)
{
  struct FD_INDEX_TYPEINFO *ixtype = index_typeinfo;
  while (ixtype) {
    if ((strcasecmp(name,ixtype->index_typename))==0)
      return ixtype->handler;
    else ixtype = ixtype->next_type;}
  return NULL;
}

FD_EXPORT
fd_index fd_make_index(u8_string spec,
                       u8_string indextype,
                       fd_storage_flags flags,
                       lispval opts)
{
  fd_index_typeinfo ixtype = fd_get_index_typeinfo(indextype);
  if (ixtype == NULL) {
    fd_seterr3(_("UnknownIndexType"),"fd_make_index",indextype);
    return NULL;}
  else if (ixtype->handler == NULL) {
    fd_seterr3(_("NoIndexHandler"),"fd_make_index",indextype);
    return NULL;}
  else if (ixtype->handler->create == NULL) {
    fd_seterr3(_("NoCreateHandler"),"fd_make_index",indextype);
    return NULL;}
  else {
    if (FIXNUMP(opts)) {
      lispval tmp_opts = fd_init_slotmap(NULL,3,NULL);
      fd_store(tmp_opts,fd_intern("SIZE"),opts);
      fd_index ix=ixtype->handler->create(spec,ixtype->type_data,
                                          flags,tmp_opts);
      fd_decref(tmp_opts);
      return ix;}
    else return ixtype->handler->create(spec,ixtype->type_data,flags,opts);}
}

/* Getting compression type from options */

static lispval compression_symbol, snappy_symbol, none_symbol, no_symbol;
static lispval zlib_symbol, zlib9_symbol;

#define DEFAULT_COMPRESSION FD_ZLIB

FD_EXPORT
fd_compress_type fd_compression_type(lispval opts,fd_compress_type dflt)
{
  if (fd_testopt(opts,compression_symbol,FD_FALSE))
    return FD_NOCOMPRESS;
#if HAVE_SNAPPYC_H
  else if (fd_testopt(opts,compression_symbol,snappy_symbol))
    return FD_SNAPPY;
#endif
  else if (fd_testopt(opts,compression_symbol,zlib_symbol))
    return FD_ZLIB;
  else if ( (fd_testopt(opts,compression_symbol,zlib9_symbol)) ||
            (fd_testopt(opts,compression_symbol,FD_INT(9))) )
    return FD_ZLIB9;
  else if ( (fd_testopt(opts,compression_symbol,FDSYM_NO)) ||
            (fd_testopt(opts,compression_symbol,FD_INT(0))) )
    return FD_NOCOMPRESS;
  else if ( (fd_testopt(opts,compression_symbol,FD_TRUE)) ||
            (fd_testopt(opts,compression_symbol,FD_DEFAULT_VALUE)) ||
            (fd_testopt(opts,compression_symbol,FDSYM_DEFAULT)) ) {
    if (dflt)
      return dflt;
    else return DEFAULT_COMPRESSION;}
  else return dflt;
}

/* Matching file pools/indexes */

FD_EXPORT u8_string fd_match_pool_file(u8_string spec,void *data)
{
  u8_string rpath=u8_realpath(spec,NULL);
  if ((u8_file_existsp(rpath)) &&
      (fd_match4bytes(rpath,data)))
    return rpath;
  else if (u8_has_suffix(spec,".pool",1)) {
    u8_free(rpath);
    return NULL;}
  else {
    u8_string new_spec = u8_mkstring("%s.pool",spec);
    u8_string variation = u8_realpath(new_spec,NULL);
    u8_free(rpath); u8_free(new_spec);
    if ((u8_file_existsp(variation))&&
        (fd_match4bytes(variation,data))) {
      return variation;}
    else {
      u8_free(variation);
      return NULL;}}
}

FD_EXPORT u8_string fd_match_index_file(u8_string spec,void *data)
{
  u8_string rpath=u8_realpath(spec,NULL);
  if ((u8_file_existsp(rpath)) &&
      (fd_match4bytes(rpath,data)))
    return rpath;
  else if (u8_has_suffix(spec,".index",1)) {
    u8_free(rpath);
    return NULL;}
  else {
    u8_string new_spec = u8_mkstring("%s.index",spec);
    u8_string variation = u8_realpath(new_spec,NULL);
    u8_free(rpath); u8_free(new_spec);
    if ((u8_file_existsp(variation))&&
        (fd_match4bytes(variation,data))) {
      return variation;}
    else {
      u8_free(variation);
      return NULL;}}
}

/* Setting fileinfo */

static u8_uid get_owner(lispval spec)
{
  if (FD_UINTP(spec))
    return (u8_uid) (FD_FIX2INT(spec));
  else if (FD_STRINGP(spec)) {
    u8_uid uid = u8_getuid(FD_CSTRING(spec));
    if (uid>=0) return uid;}
  else {}
  u8_logf(LOG_WARN,"NoSuchUser",
          "Couldn't identify a user from %q",spec);
  return (u8_gid) -1;
}

static u8_gid get_group(lispval spec)
{
  if (FD_UINTP(spec))
    return (u8_gid) (FD_FIX2INT(spec));
  else if (FD_STRINGP(spec)) {
    u8_gid gid = u8_getgid(FD_CSTRING(spec));
    if (gid>=0) return gid;}
  else {}
  u8_logf(LOG_WARN,"NoSuchGroup",
          "Couldn't identify a group from %q",spec);
  return (u8_gid) -1;
}

FD_EXPORT int fd_set_file_opts(u8_string filename,lispval opts)
{
  lispval owner = fd_getopt(opts,fd_intern("OWNER"),FD_VOID);
  lispval group = fd_getopt(opts,fd_intern("GROUP"),FD_VOID);
  lispval mode = fd_getopt(opts,fd_intern("MODE"),FD_VOID);
  int set_rv = 0;
  if ( (FD_VOIDP(owner))  &&
       (FD_VOIDP(group)) &&
       (FD_VOIDP(mode)) )
    return set_rv;
  if (! ((FD_VOIDP(owner)) && (FD_VOIDP(group))) ) {
    u8_uid file_owner = (FD_VOIDP(owner)) ? (-1) : (get_owner(owner));
    u8_gid file_group = (FD_VOIDP(group)) ? (-1) : (get_group(group));
    if ( (file_owner >= 0) || (file_group >= 0) ) {
      const char *path = u8_tolibc(filename);
      int rv = chown(path,file_owner,file_group);
      u8_free(path);
      if (rv<0) {
        if (file_owner < 0)
          u8_logf(LOG_WARN,"FileOptsFailed",
                  "Couldn't set the group of '%s' to %q",filename,group);
        else if (file_group < 0)
          u8_logf(LOG_WARN,"FileOptsFailed",
                  "Couldn't set the owner of '%s' to %q",filename,owner);
        else u8_logf(LOG_WARN,"FileOptsFailed",
                     "Couldn't set the owner:group of '%s' to %q:%q",
                     filename,owner,group);
        return -1;}
      set_rv=1;}}
  if (FD_VOIDP(mode))
    return set_rv;
  else if ( (FD_UINTP(mode)) && ((FD_FIX2INT(mode)) < 512) ) {
    char *s = u8_tolibc(filename);
    int rv = u8_chmod(s,((mode_t)(FD_FIX2INT(mode))));
    if (rv<0) {
      u8_logf(LOG_WARN,"FileOptsFailed",
              "Couldn't change file mode of '%s' to %q",filename,mode);
      return rv;}
    else return 1;}
  else {}
  u8_logf(LOG_WARN,"FileOptsFailed",
          "Couldn't set the mode of '%s' to %q",filename,mode);
  return -1;
}

/* Initialization */

int fd_init_mempool_c(void);
int fd_init_extpool_c(void);
int fd_init_extindex_c(void);
int fd_init_procpool_c(void);
int fd_init_procindex_c(void);

static int drivers_c_initialized = 0;

FD_EXPORT int fd_init_drivers_c()
{
  if (drivers_c_initialized) return drivers_c_initialized;
  drivers_c_initialized = 307*fd_init_storage();

  u8_register_source_file(_FILEINFO);

  fd_init_extindex_c();
  fd_init_mempool_c();
  fd_init_extpool_c();
  fd_init_procpool_c();
  fd_init_procindex_c();

  rev_symbol = fd_intern("REV");
  gentime_symbol = fd_intern("GENTIME");
  packtime_symbol = fd_intern("PACKTIME");
  modtime_symbol = fd_intern("MODTIME");
  compression_symbol = fd_intern("COMPRESSION");
  snappy_symbol = fd_intern("SNAPPY");
  zlib_symbol = fd_intern("ZLIB");
  zlib9_symbol = fd_intern("ZLIB9");
  no_symbol = fd_intern("NO");
  none_symbol = fd_intern("NONE");
  adjuncts_symbol = fd_intern("ADJUNCTS");

  fd_cachelevel_op=fd_intern("CACHELEVEL");
  fd_bufsize_op=fd_intern("BUFSIZE");
  fd_mmap_op=fd_intern("MMAP");
  fd_preload_op=fd_intern("PRELOAD");
  fd_swapout_op=fd_intern("SWAPOUT");
  fd_reload_op=fd_intern("RELOAD");
  fd_stats_op=fd_intern("STATS");
  fd_label_op=fd_intern("LABEL");
  fd_populate_op=fd_intern("POPULATE");
  fd_getmap_op=fd_intern("GETMAP");
  fd_slotids_op=fd_intern("SLOTIDS");
  fd_baseoids_op=fd_intern("BASEOIDS");
  fd_load_op=fd_intern("LOAD");
  fd_capacity_op=fd_intern("CAPACITY");
  fd_metadata_op=fd_intern("METADATA");
  fd_raw_metadata_op=fd_intern("%METADATA");
  fd_keys_op=fd_intern("KEYS");

  u8_init_mutex(&pool_typeinfo_lock);
  u8_init_mutex(&index_typeinfo_lock);

  fd_register_config("ACIDFILES",
                     "Maintain acidity of individual file pools and indexes",
                     fd_boolconfig_get,fd_boolconfig_set,&fd_acid_files);
  fd_register_config("DRIVERBUFSIZE",
                     "The size of file streams used in database files",
                     fd_sizeconfig_get,fd_sizeconfig_set,&fd_driver_bufsize);


  return drivers_c_initialized;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
