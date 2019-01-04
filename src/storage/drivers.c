/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/components/storage_layer.h"

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/bufio.h"
#include "framerd/streams.h"
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
static lispval adjuncts_symbol, pooltype_symbol, indextype_symbol;

lispval fd_cachelevel_op, fd_bufsize_op, fd_mmap_op, fd_preload_op;
lispval fd_metadata_op, fd_raw_metadata_op, fd_reload_op;
lispval fd_stats_op, fd_label_op, fd_populate_op, fd_swapout_op;
lispval fd_getmap_op, fd_slotids_op, fd_baseoids_op;
lispval fd_load_op, fd_capacity_op, fd_keycount_op;
lispval fd_keys_op, fd_keycount_op, fd_partitions_op;

u8_condition fd_MMAPError=_("MMAP Error");
u8_condition fd_MUNMAPError=_("MUNMAP Error");
u8_condition fd_CorruptedPool=_("Corrupted file pool");
u8_condition fd_InvalidOffsetType=_("Invalid offset type");
u8_condition fd_RecoveryRequired=_("RECOVERY");

u8_condition fd_UnknownPoolType=_("Unknown pool type");
u8_condition fd_UnknownIndexType=_("Unknown index type");

u8_condition fd_CantOpenPool=_("Can't open pool");
u8_condition fd_CantOpenIndex=_("Can't open index");

u8_condition fd_CantFindPool=_("Can't find pool");
u8_condition fd_CantFindIndex=_("Can't find index");

u8_condition fd_PoolDriverError=_("Internal error with pool file");
u8_condition fd_IndexDriverError=_("Internal error with index file");

u8_condition fd_PoolFileSizeOverflow=_("file pool overflowed file size");
u8_condition fd_FileIndexSizeOverflow=_("file index overflowed file size");

int fd_acid_files = 1;
size_t fd_driver_bufsize = FD_STORAGE_DRIVER_BUFSIZE;

static fd_pool_typeinfo default_pool_type = NULL;
static fd_index_typeinfo default_index_type = NULL;

#define CHECK_ERRNO U8_CLEAR_ERRNO

#ifndef SIZEOF_UINT
#define SIZEOF_UINT (sizeof(unsigned int))
#endif

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

FD_EXPORT
int fd_same_sourcep(u8_string ref,u8_string source)
{
  if ( (source == NULL) || (ref == NULL) )
    return 0;
  else if (strcmp(ref,source) == 0)
    return 1;
  else if ( (strchr(source,'@')) || (strchr(source,':')) ||
            (strchr(ref,'@')) || (strchr(ref,':')) )
    return 0;
  else if ( (u8_file_existsp(source)) && (u8_file_existsp(ref)) ) {
    u8_string real_source = u8_realpath(source,NULL);
    u8_string ref_source = u8_realpath(ref,NULL);
    int rv = (strcmp(real_source,ref_source)==0);
    u8_free(real_source);
    u8_free(ref_source);
    return rv;}
  else return 0;
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
      if ( ( (matcher == NULL) || (matcher == ptype->matcher) ) &&
           ( (handler == NULL) || (handler == ptype->handler) ) ) {
        if ( (type_data) && (type_data != ptype->type_data) ) {
          u8_logf(LOG_WARN,"PoolTypeRedefinition",
                  "Redefining pool type '%s' with different type data",
                  name);}
        else {}}
      else if ((handler) && (ptype->handler) &&
               (handler!=ptype->handler))
        u8_logf(LOG_WARN,"PoolTypeInconsistency",
                "Attempt to redefine pool type '%s' with different handler",
                name);
      u8_unlock_mutex(&pool_typeinfo_lock);
      return;}
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
  u8_string search_spec = spec;
  if ( (strchr(search_spec,'@')==NULL) || (strchr(search_spec,':')==NULL) ) {
    if (u8_file_existsp(search_spec))
      search_spec = u8_realpath(spec,NULL);}
  fd_pool found = (flags&FD_STORAGE_UNREGISTERED) ? (NULL) :
    (fd_find_pool_by_source(search_spec));
  if (spec != search_spec) u8_free(search_spec);
  fd_pool opened = (found) ? (found) :
    (ptype->opener(spec,flags,opts));
  if (opened==NULL) {
    if (! ( flags & FD_STORAGE_NOERR) )
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
  if (flags<0) flags = fd_get_dbflags(opts,FD_STORAGE_ISPOOL);
  struct FD_POOL_TYPEINFO *ptype = pool_typeinfo; while (ptype) {
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
  lispval pool_typeid = fd_getopt(opts,pooltype_symbol,FD_VOID);
  if (FD_VOIDP(pool_typeid))
    pool_typeid = fd_getopt(opts,FDSYM_TYPE,FD_VOID);
  /* MODULE is an alias for type and 'may' have the additional
     semantics of being auto-loaded. */
  if (FD_VOIDP(pool_typeid))
    pool_typeid = fd_getopt(opts,FDSYM_MODULE,FD_VOID);
  ptype = (FD_STRINGP(pool_typeid)) ?
    (fd_get_pool_typeinfo(FD_CSTRING(pool_typeid))) :
    (FD_SYMBOLP(pool_typeid)) ?
    (fd_get_pool_typeinfo(FD_SYMBOL_NAME(pool_typeid))) :
    (NULL);
  fd_decref(pool_typeid);
  if (ptype) {
    u8_string open_spec = spec;
    if (ptype->matcher) {
      open_spec = ptype->matcher(spec,ptype->type_data);
      if (open_spec==NULL) {
        unsigned char buf[200];
        fd_seterr(fd_CantOpenPool,"fd_open_pool",
                  u8_sprintf(buf,200,"(%s)%s",ptype->pool_typename,spec),
                  opts);
        return NULL;}}
    fd_pool p = open_pool(ptype,open_spec,flags,opts);
    if (open_spec != spec) u8_free(open_spec);
    return p;}
  if (!(flags & FD_STORAGE_NOERR))
    fd_seterr(fd_UnknownPoolType,"fd_open_pool",spec,opts);
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
      if ( ( (handler == NULL) || (handler == ixtype->handler) ) &&
           ( (matcher == NULL) || (matcher == ixtype->matcher) ) ) {
        if (type_data != ixtype->type_data) {
          u8_logf(LOG_WARN,"IndexTypeRedefinition",
                  "Redefining index type '%s' with different type data",
                  name);
          ixtype->type_data = type_data;}
        u8_unlock_mutex(&index_typeinfo_lock);
        return;}
      else if ((matcher) && (ixtype->matcher) && (matcher!=ixtype->matcher))
        u8_logf(LOG_ERR,"IndexTypeInconsistency",
                "Attempt to redefine index type '%s' with different matcher",
                name);
      else if ((handler) && (ixtype->handler) &&
               (handler!=ixtype->handler))
        u8_logf(LOG_ERR,"IndexTypeInconsistency",
                "Attempt to redefine index type '%s' with different handler",
                name);
      else {}
    return;}
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
  u8_string search_spec = spec;
  if ( (strchr(search_spec,'@')==NULL) || (strchr(search_spec,':')==NULL) ) {
    if (u8_file_existsp(search_spec))
      search_spec = u8_realpath(spec,NULL);}
  fd_index found = (flags&FD_STORAGE_UNREGISTERED) ? (NULL) :
    (fd_find_index_by_source(search_spec));
  fd_index opened = (found) ? (found) : (ixtype->opener(spec,flags,opts));
  if (search_spec != spec) u8_free(search_spec);
  if (opened==NULL) {
    if (! ( flags & FD_STORAGE_NOERR) )
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
  struct FD_INDEX_TYPEINFO *ixtype = index_typeinfo;
  if (flags<0) flags = fd_get_dbflags(opts,FD_STORAGE_ISINDEX);
  while (ixtype) {
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
  lispval index_typeid = fd_getopt(opts,indextype_symbol,FD_VOID);
  if (FD_VOIDP(index_typeid))
    index_typeid = fd_getopt(opts,FDSYM_TYPE,FD_VOID);
  /* MODULE is an alias for type and 'may' have the additional
     semantics of being auto-loaded. */
  if (FD_VOIDP(index_typeid))
    index_typeid = fd_getopt(opts,FDSYM_MODULE,FD_VOID);
  ixtype = (FD_STRINGP(index_typeid)) ?
    (fd_get_index_typeinfo(FD_CSTRING(index_typeid))) :
    (FD_SYMBOLP(index_typeid)) ?
    (fd_get_index_typeinfo(FD_SYMBOL_NAME(index_typeid))) :
    (NULL);
  fd_decref(index_typeid);
  if (ixtype) {
    u8_string open_spec = spec;
    if (ixtype->matcher) {
      open_spec = ixtype->matcher(spec,ixtype->type_data);
      if (open_spec == NULL) {
        unsigned char buf[200];
        fd_seterr(fd_CantOpenIndex,"fd_open_index",
                  u8_sprintf(buf,200,"(%s)%s",ixtype->index_typename,spec),
                  opts);
        return NULL;}}
    fd_index ix = open_index(ixtype,open_spec,flags,opts);
    if (open_spec != spec) u8_free(open_spec);
    return ix;}
  if (!(flags & FD_STORAGE_NOERR))
    fd_seterr(fd_UnknownIndexType,"fd_open_index",spec,opts);
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

/* OIDCODE maps */

FD_EXPORT lispval _fd_get_baseoid(struct FD_OIDCODER *map,unsigned int oidcode)
{
  if (oidcode > map->n_oids)
    return FD_VOID;
  else return map->baseoids[oidcode];
}

FD_EXPORT int _fd_get_oidcode(struct FD_OIDCODER *map,int oidbaseid)
{
  if (oidbaseid > map->max_baseid)
    return -1;
  else return map->oidcodes[oidbaseid];
}

FD_EXPORT int fd_add_oidcode(struct FD_OIDCODER *map,lispval oid)
{
  if (map->oidcodes == NULL) return -1;
  int baseid = FD_OID_BASE_ID(oid);
  lispval baseoid = FD_CONSTRUCT_OID(baseid,0);
  if (baseid <= map->max_baseid) {
    int v = map->oidcodes[baseid];
    if (v>0) return v;}
  if (baseid >= map->codes_len) {
    int new_len = map->codes_len;
    while (baseid >= new_len) new_len = new_len*2;
    unsigned int *new_codes =
      u8_realloc(map->oidcodes,sizeof(unsigned int)*new_len);
    if (new_codes) {
      int i = map->codes_len;
      while (i<new_len) new_codes[i++]=-1;
      map->oidcodes = new_codes;
      map->codes_len = new_len;}
    else return -1;}
  if (map->n_oids >= map->oids_len) {
    /* Grow the base OIDs table */
    unsigned int len = map->oids_len, new_len = (len<4) ? (8) : (len*2);
    lispval *cur_oids = map->baseoids;
    lispval *new_oids = u8_malloc(sizeof(lispval)*new_len);
    memcpy(new_oids,cur_oids,sizeof(lispval)*len);
    int i = len; while (i<new_len) {
      new_oids[i++]=FD_FALSE;}
    if (new_oids) {
      map->baseoids = new_oids;
      map->oids_len = new_len;}
    else return -1;}
  int code = map->n_oids++;
  map->baseoids[code]    = baseoid;
  map->oidcodes[baseid]  = code;
  if (baseid > map->max_baseid)
    map->max_baseid=baseid;
  return code;
}

FD_EXPORT void fd_init_oidcoder(struct FD_OIDCODER *oidmap,
                                int oids_len,lispval *oids)
{
  lispval *baseoids;
  unsigned int *codes;
  if (oidmap->oids_len) {
    if (oidmap->baseoids)
      u8_free(oidmap->baseoids);
    if (oidmap->oidcodes) u8_free(oidmap->oidcodes);}
  if (oids_len < 0) {
    oidmap->baseoids    = NULL;
    oidmap->n_oids      = 0;
    oidmap->oids_len    = 0;
    oidmap->oidcodes    = NULL;
    oidmap->codes_len   = 0;
    oidmap->max_baseid  = -1;
    return;}
  else if (oids == NULL) {
    baseoids = u8_alloc_n(oids_len,lispval);
    int i=0; while (i<oids_len) oids[i++]=FD_FALSE;}
  else {
    baseoids = u8_alloc_n(oids_len,lispval);
    int i=0; while (i<oids_len) {
      lispval init = oids[i];
      if ( (init) && (FD_OIDP(init)) )
        baseoids[i++] = init;
      else baseoids[i++] = FD_FALSE;}}
  int i = 0, codes_len = 1024, max_baseid=-1, last_oid = -1;
  while (codes_len < fd_n_base_oids) codes_len = codes_len*2;
  codes = u8_alloc_n(codes_len,unsigned int);
  i = 0; while (i<codes_len) codes[i++]= -1;
  i = 0; while (i<oids_len) {
      lispval baseoid = baseoids[i];
      if (FD_OIDP(baseoid)) {
        int baseid    = FD_OID_BASE_ID(baseoid);
        baseoids[i]   = baseoid;
        if (codes[baseid] != -1) {
          FD_OID addr = FD_OID_ADDR(baseoid);
          u8_log(LOG_WARN,"Duplicate baseoid",
                 "The baseoid @%llx/%llx (baseid=%d) is coded at %d and %d",
                 FD_OID_LO(addr),FD_OID_HI(addr),baseid,
                 i,codes[baseid]);}
        codes[baseid] = i;
        if (baseid > max_baseid) max_baseid = baseid;
        last_oid = i;}
      i++;}
  oidmap->n_oids      = last_oid+1;
  oidmap->init_n_oids = last_oid+1;
  oidmap->baseoids    = baseoids;
  oidmap->oidcodes    = codes;
  oidmap->oids_len    = oids_len;
  oidmap->codes_len   = codes_len;
  oidmap->max_baseid  = max_baseid;
}

FD_EXPORT struct FD_OIDCODER fd_copy_oidcodes(fd_oidcoder src)
{
  if (src->baseoids) {
    u8_read_lock(&(src->rwlock));

    struct FD_OIDCODER oc = *src;
    memset(&(oc.rwlock),0,sizeof(oc.rwlock));   /* Just to be tidy */
    oc.baseoids = u8_memdup((SIZEOF_LISPVAL*oc.oids_len),oc.baseoids);
    oc.oidcodes = u8_memdup((SIZEOF_UINT*oc.codes_len),oc.oidcodes);
    u8_rw_unlock(&(src->rwlock));

    return oc;}
  else {
    struct FD_OIDCODER oc = { 0 };
    return oc;}
}

FD_EXPORT void fd_update_oidcodes(fd_oidcoder dest,fd_oidcoder src)
{
  u8_write_lock(&(dest->rwlock));
  lispval *old_baseoids = dest->baseoids;
  unsigned int *old_oidcodes = dest->oidcodes;
  dest->baseoids = src->baseoids;
  dest->oidcodes = src->oidcodes;
  dest->oids_len = src->oids_len;
  dest->n_oids   = src->n_oids;
  dest->codes_len = src->codes_len;
  dest->max_baseid = src->max_baseid;
  u8_rw_unlock(& dest->rwlock);
  if (old_baseoids) u8_free(old_baseoids);
  if (old_oidcodes) u8_free(old_oidcodes);
}

FD_EXPORT void fd_recycle_oidcoder(struct FD_OIDCODER *oc)
{
  if (oc->baseoids) u8_free(oc->baseoids);
  if (oc->oidcodes) u8_free(oc->oidcodes);
  memset(oc,0,sizeof(struct FD_OIDCODER));
}

FD_EXPORT lispval fd_baseoids_arg(lispval arg)
{
  if ( (FD_FALSEP(arg)) || (FD_VOIDP(arg)) ||
       ( (FD_FIXNUMP(arg)) && (FD_FIX2INT(arg) == 0)) )
    return FD_VOID;
  else if (FD_TRUEP(arg)) {
    lispval vec = fd_empty_vector(4);
    int i=0; while (i<4) {FD_VECTOR_SET(vec,i,FD_FALSE); i++;}
    return vec;}
  else if (FD_FIXNUMP(arg)) {
    long long n = FD_FIX2INT(arg);
    if (n<0) return FD_ERROR_VALUE;
    lispval vec = fd_empty_vector(n);
    int i=0; while (i<n) {FD_VECTOR_SET(vec,i,FD_FALSE); i++;}
    return vec;}
  else if (FD_VECTORP(arg)) {
    int i = 0, n = FD_VECTOR_LENGTH(arg);
    while (i<n) {
      lispval elt = FD_VECTOR_REF(arg,i);
      if (FD_OIDP(elt)) i++;
      else if ( (FD_FALSEP(elt)) || (FD_VOIDP(elt)) ) {
        FD_VECTOR_SET(arg,i,FD_FALSE);
        i++;}
      else return FD_ERROR_VALUE;}
    return fd_incref(arg);}
  else if (FD_CHOICEP(arg)) {
    const lispval *elts = FD_CHOICE_ELTS(arg);
    int i = 0, n = FD_CHOICE_SIZE(arg);
    while (i<n) {
      lispval elt = elts[i];
      if (FD_OIDP(elt)) i++;
      else return FD_ERROR_VALUE;}
    return fd_make_vector(n,(lispval *)elts);}
  else return FD_ERROR_VALUE;
}

/* Slot codes */

FD_EXPORT lispval _fd_code2slotid(struct FD_SLOTCODER *sc,unsigned int code)
{
  if (sc->slotids == NULL)
    return FD_ERROR;
  else if (code < sc->n_slotcodes)
    return sc->slotids->vec_elts[code];
  else return FD_ERROR;
}

FD_EXPORT int _fd_slotid2code(struct FD_SLOTCODER *sc,lispval slotid)
{
  if (sc->n_slotcodes <= 0)
    return -1;
  else {
    struct FD_KEYVAL *kv =
      fd_sortvec_get(slotid,sc->lookup->sm_keyvals,sc->lookup->n_slots);
    if (kv) {
      lispval v = kv->kv_val;
      if (FD_FIXNUMP(v))
        return FD_FIX2INT(v);
      else return -1;}
    else return -1;}
}

static int cmp_slotkeys(const void *vx,const void *vy)
{
  const struct FD_KEYVAL *kvx = vx;
  const struct FD_KEYVAL *kvy = vy;
  if (kvx->kv_key < kvy->kv_key)
    return -1;
  else if (kvx->kv_key == kvy->kv_key)
    return 0;
  else return 1;
}

FD_EXPORT int fd_add_slotcode(struct FD_SLOTCODER *sc,lispval slotid)
{
  if (sc->slotids == NULL) return -1;
  int probe = fd_slotid2code(sc,slotid);
  if (! ( (OIDP(slotid)) || (SYMBOLP(slotid)) ) ) return -1;
  if (probe>=0)
    return probe;
  if (sc->n_slotcodes >= sc->slotids->vec_length) {
    size_t len = sc->slotids->vec_length;
    size_t new_len = (len<8) ? (16) : (len*2);
    lispval new_vec = fd_make_vector(new_len,NULL);
    lispval new_lookup = fd_make_slotmap(new_len,len,NULL);
    lispval *slotids = FD_VECTOR_ELTS(((lispval)(sc->slotids)));
    struct FD_KEYVAL *keyvals = FD_XSLOTMAP_KEYVALS(sc->lookup);
    struct FD_SLOTMAP *new_slotmap = (fd_slotmap) new_lookup;
    struct FD_KEYVAL *new_keyvals = FD_XSLOTMAP_KEYVALS(new_slotmap);
    new_slotmap->sm_sort_keyvals = 1;
    int i = 0, lim = sc->n_slotcodes;
    while (i < lim) {
      lispval slotid = slotids[i];
      lispval slotkey = keyvals[i].kv_key;
      FD_VECTOR_SET(new_vec,i,slotid);
      new_keyvals[i].kv_key = slotkey;
      new_keyvals[i].kv_val = keyvals[i].kv_val;;
      fd_incref(slotid);
      fd_incref(slotkey);
      i++;}
    lispval old_slotids = (lispval) sc->slotids;
    lispval old_lookup = (lispval) sc->lookup;
    sc->slotids = (fd_vector) new_vec;
    sc->lookup = (fd_slotmap) new_lookup;
    fd_decref(old_slotids);
    fd_decref(old_lookup);}
  if (sc->n_slotcodes < sc->slotids->vec_length) {
    lispval vec = (lispval) sc->slotids;
    struct FD_SLOTMAP *map = sc->lookup;
    int new_code = sc->n_slotcodes++;
    FD_VECTOR_SET(vec,new_code,slotid);
    fd_slotmap_store(map,slotid,FD_INT2FIX(new_code));
    return new_code;}
  return -1;
}

FD_EXPORT int fd_init_slotcoder(struct FD_SLOTCODER *sc,
                                int slotids_len,
                                lispval *slotids)
{
  if (sc->slotids) {
    fd_decref((lispval)(sc->slotids));
    sc->slotids = NULL;}
  if (sc->lookup) {
    fd_decref((lispval)(sc->lookup));
    sc->lookup = NULL;}
  if ( (slotids_len>=0) || (slotids) ) {
    lispval slot_vec = fd_make_vector(slotids_len,slotids);
    lispval lookup = fd_make_slotmap(slotids_len,0,NULL);
    struct FD_KEYVAL *keyvals = FD_SLOTMAP_KEYVALS(lookup);
    int i = 0, j = 0; while (i < slotids_len) {
      lispval slotid = (slotids) ? (slotids[i]) : (FD_FALSE);
      if ( (FD_SYMBOLP(slotid)) || (FD_OIDP(slotid)) ) {
        keyvals[j].kv_key = slotid;
        keyvals[j].kv_val = FD_INT2FIX(j);
        FD_VECTOR_SET(slot_vec,j,slotid);
        j++;}
      i++;}
    sc->slotids = (fd_vector) slot_vec;
    sc->lookup = (fd_slotmap) lookup;
    sc->lookup->n_slots = j;
    sc->lookup->sm_sort_keyvals = 1;
    qsort(keyvals,j,FD_KEYVAL_LEN,cmp_slotkeys);
    sc->n_slotcodes = j;
    sc->init_n_slotcodes = j;
    return slotids_len;}
  else {
    sc->n_slotcodes = 0;
    sc->init_n_slotcodes = 0;
    sc->slotids = (fd_vector)  NULL;
    sc->lookup  = (fd_slotmap) NULL;
    return 0;}
}

FD_EXPORT struct FD_SLOTCODER fd_copy_slotcodes(fd_slotcoder src)
{
  if (src->slotids) {
    u8_read_lock(&(src->rwlock));
    struct FD_SLOTCODER sc = *src;
    memset(&(sc.rwlock),0,sizeof(sc.rwlock));   /* Just to be tidy */
    sc.slotids = (fd_vector) fd_copy((lispval)sc.slotids);
    sc.lookup = (fd_slotmap) fd_copy((lispval)sc.lookup);
    u8_rw_unlock(&(src->rwlock));
    return sc;}
  else {
    struct FD_SLOTCODER sc = { 0 };
    return sc;}
}

FD_EXPORT void fd_update_slotcodes(fd_slotcoder dest,fd_slotcoder src)
{
  u8_write_lock(&(dest->rwlock));
  struct FD_VECTOR *old_vec = dest->slotids;
  struct FD_SLOTMAP *old_map = dest->lookup;
  dest->n_slotcodes = src->n_slotcodes;
  dest->slotids = src->slotids;
  dest->lookup = src->lookup;
  u8_rw_unlock(& dest->rwlock);
  if (old_vec) fd_decref((lispval)old_vec);
  if (old_map) fd_decref((lispval)old_map);
}

#define DTOUT(lenvar,out) \
  if ((lenvar)>=0) {      \
    ssize_t _outlen = out; \
    if (_outlen<0) lenvar = -1; \
    else lenvar += _outlen;}

FD_EXPORT void fd_recycle_slotcoder(struct FD_SLOTCODER *sc)
{
  u8_destroy_rwlock(&(sc->rwlock));
  if (sc->slotids) fd_decref(((lispval)(sc->slotids)));
  if (sc->lookup) fd_decref(((lispval)(sc->lookup)));
  memset(sc,0,sizeof(struct FD_SLOTCODER));
}

FD_EXPORT lispval fd_slotids_arg(lispval arg)
{
  if ( (FD_FALSEP(arg)) || (FD_VOIDP(arg)) ||
       ( (FD_FIXNUMP(arg)) && (FD_FIX2INT(arg) == 0)) )
    return FD_VOID;
  else if (FD_FIXNUMP(arg)) {
    long long n = FD_FIX2INT(arg);
    if (n<0) return FD_ERROR_VALUE;
    lispval vec = fd_empty_vector(n);
    int i=0; while (i<n) {FD_VECTOR_SET(vec,i,FD_FALSE); i++;}
    return vec;}
  else if (FD_TRUEP(arg)) {
    lispval vec = fd_empty_vector(4);
    int i=0; while (i<4) {FD_VECTOR_SET(vec,i,FD_FALSE); i++;}
    return vec;}
  else if (FD_VECTORP(arg)) {
    int i = 0, n = FD_VECTOR_LENGTH(arg);
    while (i<n) {
      lispval elt = FD_VECTOR_REF(arg,i);
      if ( (FD_OIDP(elt)) || (FD_SYMBOLP(elt)) ) i++;
      else if ( (FD_FALSEP(elt)) || (FD_VOIDP(elt)) ) {
        FD_VECTOR_SET(arg,i,FD_FALSE);
        i++;}
      else return FD_ERROR_VALUE;}
    return fd_incref(arg);}
  else if (FD_CHOICEP(arg)) {
    const lispval *elts = FD_CHOICE_ELTS(arg);
    int i = 0, n = FD_CHOICE_SIZE(arg);
    while (i<n) {
      lispval elt = elts[i];
      if ( (FD_OIDP(elt)) || (FD_SYMBOLP(elt)) ) i++;
      else return FD_ERROR_VALUE;}
    return fd_make_vector(n,(lispval *) elts);}
  else return FD_ERROR_VALUE;
}

FD_EXPORT ssize_t fd_encode_slotmap(struct FD_OUTBUF *out,
                                    lispval value,
                                    struct FD_SLOTCODER *slotcodes)
{
  if ( (slotcodes) && (slotcodes->slotids) && (FD_SLOTMAPP(value)) ) {
    ssize_t dtype_len = 0;
    struct FD_SLOTMAP *map = (fd_slotmap) value;
    int n_slots = map->n_slots;
    struct FD_KEYVAL *kv = map->sm_keyvals;
    DTOUT(dtype_len,fd_write_byte(out,0xFF));
    DTOUT(dtype_len,fd_write_zint(out,n_slots));
    int i=0; while (i<n_slots) {
      lispval key = kv[i].kv_key;
      lispval val = kv[i].kv_val;
      int code = -1;
      if ( (FD_SYMBOLP(key)) || (FD_OIDP(key)) ) {
        code = fd_slotid2code(slotcodes,key);
        if (code<0)
          code = fd_add_slotcode(slotcodes,key);}
      if (code<0) {
        DTOUT(dtype_len,fd_write_dtype(out,key));
        DTOUT(dtype_len,fd_write_dtype(out,val));}
      else {
        DTOUT(dtype_len,fd_write_byte(out,0xFE));
        DTOUT(dtype_len,fd_write_zint(out,code));
        DTOUT(dtype_len,fd_write_dtype(out,val));}
      i++;}
    return dtype_len;}
  else return fd_write_dtype(out,value);
}

FD_EXPORT lispval fd_decode_slotmap(struct FD_INBUF *in,struct FD_SLOTCODER *slotcodes)
{
  if ( (slotcodes) && (slotcodes->slotids) ) {
    int byte = fd_probe_byte(in);
    if (byte == 0xFF) {
      fd_read_byte(in); /* Already checked */ 
      int n_slots = fd_read_zint(in);
      lispval result = fd_make_slotmap(n_slots,n_slots,NULL);
      if (FD_ABORTP(result)) return result;
      struct FD_SLOTMAP *map = (fd_slotmap) result;
      struct FD_KEYVAL *kv = map->sm_keyvals;
      int i = 0; while (i<n_slots) {
        lispval key, val;
        if (fd_probe_byte(in) == 0xFE) {
          fd_read_byte(in); /* Already checked */ 
          int code = fd_read_zint(in);
          key = fd_code2slotid(slotcodes,code);}
        else key = fd_read_dtype(in);
        if (FD_ABORTP(key)) {
          fd_decref(result); return key;}
        else kv[i].kv_key=key;
        val = fd_read_dtype(in);
        if (FD_ABORTP(val)) {
          fd_decref(result); return val;}
        else kv[i].kv_val = val;
        i++;}
      return result;}}
  return fd_read_dtype(in);
}

/* Cleaning up dbfiles */

static int try_remove(u8_string file,u8_condition cond,u8_context caller)
{
  int rv = 0;
  if (u8_file_existsp(file)) {
    if (u8_file_writablep(file)) {
      u8_log(LOG_WARN,cond,"Removing leftover file %s (%s)",
             file,caller);
      rv = u8_removefile(file);
      if (rv < 0)
        u8_seterr("FailedRemove",caller,
                  u8_mkstring("Can't remove/overwrite file %s for %s",
                              file,cond));}
    else {
      u8_seterr("FailedRemove",caller,
                u8_mkstring("Can't remove/overwrite file %s for %s",
                            file,cond));
      rv = -1;}}
  return rv;
}

FD_EXPORT int fd_write_rollback(u8_context caller,
                                u8_string id,u8_string source,
                                size_t size)
{
  u8_string rollback_file = u8_mkstring("%s.rollback",source);
  u8_string commit_file = u8_mkstring("%s.commit",source);
  int rv = try_remove(rollback_file,"LeftoverRollback","fd_write_rollback");
  if (rv<0) try_remove(commit_file,"LeftoverCommit","fd_write_rollback");
  else rv = try_remove(commit_file,"LeftoverCommit","fd_write_rollback");
  if (rv>=0) {
    ssize_t save_rv = fd_save_head(source,rollback_file,size);
    if (save_rv<0) {
      u8_seterr("CantSaveRollback",caller,rollback_file);
      rollback_file = NULL;
      rv = -1;}
    else rv=1;}
  if (commit_file) u8_free(commit_file);
  if (rollback_file) u8_free(rollback_file);
  return 1;
}

FD_EXPORT int fd_check_rollback(u8_context caller,u8_string source)
{
  u8_string rollback_file = u8_mkstring("%s.rollback",source);
  if (u8_file_existsp(rollback_file)) {
    int source_fd = u8_open_fd(source,O_RDWR,S_IWUSR|S_IRUSR);
    if (source_fd >= 0) {
      int lock_rv = u8_lock_fd(source_fd,1);
      if (lock_rv<0) {
        u8_close_fd(source_fd);
        u8_seterr("RollbackLockFailed","fd_check_rollback",u8_strdup(source));
        return -1;}
      u8_log(LOG_WARN,"Rollback",
             "Applying rollback file %s to %s (%s)",
             rollback_file,source,caller);
      int rv = fd_apply_head(rollback_file,source);
      if (rv<0) {
        if (errno) u8_graberrno(caller,source);
        u8_string err_file = u8_string_append(rollback_file,".bad",NULL);
        int move_rv = u8_movefile(rollback_file,err_file);
        if (move_rv<0) {
          u8_graberrno("fd_check_rollback/cleanup",rollback_file);
          u8_log(LOG_CRIT,"RollbackCleanupFailed",
                 "Couldn't move %s to %s",rollback_file,err_file);}
        u8_seterr("RollbackError",caller,u8_strdup(source));
        return -1;}
      else {
        u8_log(LOG_NOTICE,"Rollback",
               "Finished applying rollback file %s to %s (%s)",
               rollback_file,source,caller);
        u8_string applied_file = u8_mkstring("%s.applied",rollback_file);
        u8_string commit_file = u8_mkstring("%s.commit",source);
        if (u8_file_existsp(applied_file)) {
          int rm_rv = u8_removefile(applied_file);
          if (rm_rv<0) fd_clear_errors(1);}
        if (u8_file_existsp(commit_file)) {
          int rm_rv = u8_removefile(commit_file);
          if (rm_rv<0) fd_clear_errors(1);}
        int mv_rv = u8_movefile(rollback_file,applied_file);
        if (mv_rv<0) fd_clear_errors(1);
        u8_free(commit_file);
        u8_free(applied_file);}
      u8_free(rollback_file);
      u8_unlock_fd(source_fd);
      u8_close_fd(source_fd);
      return rv;}
    else return 0;}
  u8_free(rollback_file);
  return 0;
}

FD_EXPORT int fd_remove_suffix(u8_string base,u8_string suffix)
{
  size_t base_len = strlen(base);
  size_t full_len = base_len + strlen(suffix) + 1;
  unsigned char buf[full_len];
  strcpy(buf,base); strcpy(buf+base_len,suffix);
  if (u8_file_existsp(buf))
    return u8_removefile(buf);
  else return 0;
}

/* Matching file pools/indexes */

FD_EXPORT u8_string fd_match_pool_file(u8_string spec,void *data)
{
  u8_string abspath=u8_abspath(spec,NULL);
  if ((u8_file_existsp(abspath)) &&
      (fd_match4bytes(abspath,data)))
    return abspath;
  else if (u8_has_suffix(spec,".pool",1)) {
    u8_free(abspath);
    return NULL;}
  else {
    u8_string new_spec = u8_mkstring("%s.pool",spec);
    u8_string variation = u8_abspath(new_spec,NULL);
    u8_free(abspath); u8_free(new_spec);
    if ((u8_file_existsp(variation))&&
        (fd_match4bytes(variation,data))) {
      return variation;}
    else {
      u8_free(variation);
      return NULL;}}
}

FD_EXPORT u8_string fd_match_index_file(u8_string spec,void *data)
{
  u8_string abspath=u8_abspath(spec,NULL);
  if ((u8_file_existsp(abspath)) &&
      (fd_match4bytes(abspath,data)))
    return abspath;
  else if (u8_has_suffix(spec,".index",1)) {
    u8_free(abspath);
    return NULL;}
  else {
    u8_string new_spec = u8_mkstring("%s.index",spec);
    u8_string variation = u8_abspath(new_spec,NULL);
    u8_free(abspath); u8_free(new_spec);
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

void fd_init_mempool_c(void);
void fd_init_extpool_c(void);
void fd_init_extindex_c(void);
void fd_init_procpool_c(void);
void fd_init_procindex_c(void);
void fd_init_aggregates_c(void);
void fd_init_tempindex_c(void);

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
  fd_init_aggregates_c();
  fd_init_tempindex_c();

  rev_symbol = fd_intern("REV");
  gentime_symbol = fd_intern("GENTIME");
  packtime_symbol = fd_intern("PACKTIME");
  modtime_symbol = fd_intern("MODTIME");
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
  fd_keycount_op=fd_intern("KEYCOUNT");
  fd_partitions_op=fd_intern("PARTITIONS");
  pooltype_symbol=fd_intern("POOLTYPE");
  indextype_symbol=fd_intern("INDEXTYPE");

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
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
