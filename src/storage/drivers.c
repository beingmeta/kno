/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/components/storage_layer.h"

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/bufio.h"
#include "kno/streams.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/drivers.h"

#include <libu8/libu8io.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8fileio.h>

#include <stdio.h>

static lispval rev_symbol, gentime_symbol, packtime_symbol, modtime_symbol;
static lispval adjuncts_symbol, wadjuncts_symbol;
static lispval pooltype_symbol, indextype_symbol;

lispval kno_cachelevel_op, kno_bufsize_op, kno_mmap_op, kno_preload_op;
lispval kno_metadata_op, kno_raw_metadata_op, kno_reload_op;
lispval kno_stats_op, kno_label_op, kno_source_op, kno_populate_op, kno_swapout_op;
lispval kno_getmap_op, kno_xrefs_op, kno_slotids_op, kno_baseoids_op;
lispval kno_load_op, kno_capacity_op, kno_keycount_op;
lispval kno_keys_op, kno_keycount_op, kno_partitions_op;

u8_condition kno_MMAPError=_("MMAP Error");
u8_condition kno_MUNMAPError=_("MUNMAP Error");
u8_condition kno_CorruptedPool=_("Corrupted file pool");
u8_condition kno_InvalidOffsetType=_("Invalid offset type");
u8_condition kno_RecoveryRequired=_("RECOVERY");

u8_condition kno_UnknownPoolType=_("Unknown pool type");
u8_condition kno_UnknownIndexType=_("Unknown index type");

u8_condition kno_CantOpenPool=_("Can't open pool");
u8_condition kno_CantOpenIndex=_("Can't open index");

u8_condition kno_CantFindPool=_("Can't find pool");
u8_condition kno_CantFindIndex=_("Can't find index");

u8_condition kno_PoolDriverError=_("Internal error with pool file");
u8_condition kno_IndexDriverError=_("Internal error with index file");

u8_condition kno_PoolFileSizeOverflow=_("file pool overflowed file size");
u8_condition kno_FileIndexSizeOverflow=_("file index overflowed file size");

int kno_acid_files = 1;
size_t kno_driver_bufsize = KNO_STORAGE_DRIVER_BUFSIZE;

static kno_pool_typeinfo default_pool_type = NULL;
static kno_index_typeinfo default_index_type = NULL;

#define CHECK_ERRNO U8_CLEAR_ERRNO

#ifndef SIZEOF_UINT
#define SIZEOF_UINT (sizeof(unsigned int))
#endif

/* Matching word prefixes */

KNO_EXPORT
u8_string kno_match4bytes(u8_string file,void *data)
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

KNO_EXPORT
u8_string kno_netspecp(u8_string spec,void *ignored)
{
  u8_byte *at = strchr(spec,'@'), *col = strchr(spec,':');
  if ((at == NULL)&&(col == NULL))
    return NULL;
  else return spec;
}

KNO_EXPORT
int kno_same_sourcep(u8_string ref,u8_string source)
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

static struct KNO_POOL_TYPEINFO *pool_typeinfo;
static u8_mutex pool_typeinfo_lock;

KNO_EXPORT void kno_register_pool_type
(u8_string name,
 kno_pool_handler handler,
 kno_pool (*opener)(u8_string filename,kno_storage_flags flags,lispval opts),
 u8_string (*matcher)(u8_string filename,void *),
 void *type_data)
{
  struct KNO_POOL_TYPEINFO *ptype;
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
  ptype = u8_alloc(struct KNO_POOL_TYPEINFO);
  ptype->pool_typename = u8_strdup(name);
  ptype->handler = handler;
  ptype->opener = opener;
  ptype->matcher = matcher;
  ptype->type_data = type_data;
  ptype->next_type = pool_typeinfo;
  pool_typeinfo = ptype;
  u8_unlock_mutex(&pool_typeinfo_lock);
}

KNO_EXPORT
kno_pool_typeinfo kno_get_pool_typeinfo(u8_string name)
{
  struct KNO_POOL_TYPEINFO *ptype = pool_typeinfo;
  if (name == NULL)
    return default_pool_type;
  else while (ptype) {
      if (strcasecmp(name,ptype->pool_typename)==0)
        return ptype;
      else ptype = ptype->next_type;}
  return NULL;
}

KNO_EXPORT kno_pool_typeinfo kno_set_default_pool_type(u8_string id)
{
  kno_pool_typeinfo info = (id) ?  (kno_get_pool_typeinfo(id)) :
    (default_pool_type);
  if (info)
    default_pool_type = info;
  return info;
}

static kno_pool open_pool(kno_pool_typeinfo ptype,u8_string spec,
                         kno_storage_flags flags,lispval opts)
{
  u8_string search_spec = spec;
  if ( (strchr(search_spec,'@')==NULL) || (strchr(search_spec,':')==NULL) ) {
    if (u8_file_existsp(search_spec))
      search_spec = u8_realpath(spec,NULL);}
  kno_pool found = (flags&KNO_STORAGE_UNREGISTERED) ? (NULL) :
    (kno_find_pool_by_source(search_spec));
  if (spec != search_spec) u8_free(search_spec);
  kno_pool opened = (found) ? (found) :
    (ptype->opener(spec,flags,opts));
  if (opened==NULL) {
    if (! ( flags & KNO_STORAGE_NOERR) )
      return KNO_ERR(NULL,kno_CantOpenPool,"kno_open_pool",spec,opts);
    else return opened;}
  else if (kno_testopt(opts,wadjuncts_symbol,VOID)) {
    lispval adjuncts=kno_getopt(opts,wadjuncts_symbol,EMPTY);
    int rv=kno_set_adjuncts(opened,adjuncts);
    kno_decref(adjuncts);
    if (rv<0) {
      if (flags & KNO_STORAGE_NOERR) {
        u8_logf(LOG_CRIT,kno_AdjunctError,
                "Opening pool '%s' with opts=%q",
                opened->poolid,opts);
        kno_clear_errors(1);}
      else return KNO_ERR(NULL,kno_AdjunctError,"kno_open_pool",spec,opts);}}
  lispval old_opts=opened->pool_opts;
  if (old_opts != opts) {
    opened->pool_opts=kno_incref(opts);
    kno_decref(old_opts);}
  return opened;
}

KNO_EXPORT
kno_pool kno_open_pool(u8_string spec,kno_storage_flags flags,lispval opts)
{
  CHECK_ERRNO();
  if (flags<0) flags = kno_get_dbflags(opts,KNO_STORAGE_ISPOOL);
  struct KNO_POOL_TYPEINFO *ptype = pool_typeinfo; while (ptype) {
    if (ptype->matcher) {
      u8_string use_spec = ptype->matcher(spec,ptype->type_data);
      if (use_spec) {
        kno_pool found = (flags&KNO_STORAGE_UNREGISTERED) ? (NULL) :
          (kno_find_pool_by_source(use_spec));
        kno_pool opened = (found) ? (found) :
          (ptype->opener(use_spec,flags,opts));
        if (use_spec!=spec) u8_free(use_spec);
        if (opened==NULL)
          return KNO_ERR(NULL,kno_CantOpenPool,"kno_open_pool",spec,opts);
        else if (kno_testopt(opts,wadjuncts_symbol,VOID)) {
          lispval adjuncts=kno_getopt(opts,wadjuncts_symbol,EMPTY);
          int rv=kno_set_adjuncts(opened,adjuncts);
          kno_decref(adjuncts);
          if (rv<0) {
            if (flags & KNO_STORAGE_NOERR) {
              u8_logf(LOG_CRIT,kno_AdjunctError,
                      "Opening pool '%s' with opts=%q",
                      opened->poolid,opts);
              kno_clear_errors(1);}
            else return KNO_ERR(NULL,kno_AdjunctError,"kno_open_pool",spec,opts);}}
        lispval old_opts=opened->pool_opts;
	if (old_opts != opts) {
	  opened->pool_opts=kno_incref(opts);
	  kno_decref(old_opts);}
        return opened;}
      CHECK_ERRNO();}
    ptype = ptype->next_type;}
  lispval pool_typeid = kno_getopt(opts,pooltype_symbol,KNO_VOID);
  if (KNO_VOIDP(pool_typeid))
    pool_typeid = kno_getopt(opts,KNOSYM_TYPE,KNO_VOID);
  /* MODULE is an alias for type and 'may' have the additional
     semantics of being auto-loaded. */
  if (KNO_VOIDP(pool_typeid))
    pool_typeid = kno_getopt(opts,KNOSYM_MODULE,KNO_VOID);
  ptype = (KNO_STRINGP(pool_typeid)) ?
    (kno_get_pool_typeinfo(KNO_CSTRING(pool_typeid))) :
    (KNO_SYMBOLP(pool_typeid)) ?
    (kno_get_pool_typeinfo(KNO_SYMBOL_NAME(pool_typeid))) :
    (NULL);
  kno_decref(pool_typeid);
  if (ptype) {
    u8_string open_spec = spec;
    if (ptype->matcher) {
      open_spec = ptype->matcher(spec,ptype->type_data);
      if (open_spec==NULL) {
        unsigned char buf[200];
        return KNO_ERR(NULL,kno_CantOpenPool,"kno_open_pool",
                       u8_sprintf(buf,200,"(%s)%s",ptype->pool_typename,spec),
                       opts);}}
    kno_pool p = open_pool(ptype,open_spec,flags,opts);
    if (open_spec != spec) u8_free(open_spec);
    return p;}
  if (!(flags & KNO_STORAGE_NOERR))
    kno_seterr(kno_UnknownPoolType,"kno_open_pool",spec,opts);
  return NULL;
}

KNO_EXPORT
kno_pool_handler kno_get_pool_handler(u8_string name)
{
  struct KNO_POOL_TYPEINFO *ptype = kno_get_pool_typeinfo(name);
  if (ptype)
    return ptype->handler;
  else return NULL;
}

typedef unsigned long long ull;

static int fix_pool_opts(u8_string spec,lispval opts)
{
  lispval base_oid = kno_getopt(opts,kno_intern("base"),VOID);
  lispval capacity_arg = kno_getopt(opts,kno_intern("capacity"),VOID);
  if (!(OIDP(base_oid)))
    return KNO_ERR(-1,"PoolBaseNotOID","kno_make_pool",spec,opts);
  else if (!(FIXNUMP(capacity_arg)))
    return KNO_ERR(-1,"PoolCapacityNotFixnum","kno_make_pool",spec,opts);
  else {
    KNO_OID addr=KNO_OID_ADDR(base_oid);
    unsigned int lo=KNO_OID_LO(addr);
    int capacity=KNO_FIX2INT(capacity_arg);
    if (capacity<=0)
      return KNO_ERR(-1,"NegativePoolCapacity","kno_make_pool",spec,opts);
    else if (capacity>=0x40000000)
      return KNO_ERR(-1,"PoolCapacityTooLarge","kno_make_pool",spec,opts);
    else {}
    int span_pool=(capacity>KNO_OID_BUCKET_SIZE);
    if ((span_pool)&&(lo%KNO_OID_BUCKET_SIZE))
      return KNO_ERR(-1,"MisalignedBaseOID","kno_make_pool",spec,opts);
    else if ((span_pool)&&(capacity%KNO_OID_BUCKET_SIZE)) {
      lispval opt_root=opts;
      unsigned int new_capacity =
        (1+(capacity/KNO_OID_BUCKET_SIZE))*KNO_OID_BUCKET_SIZE;
      u8_logf(LOG_WARN,"FixingCapacity",
              "Rounding up the capacity of %s from %llu to 0x%llx",
              spec,(ull)capacity,(ull)new_capacity);
      if (KNO_PAIRP(opts)) opt_root=KNO_CAR(opts);
      int rv=kno_store(opt_root,kno_intern("capacity"),
                      KNO_INT(new_capacity));
      return rv;}
    /* TODO: Add more checks for non-spanning pools */
    else return 1;}
}

KNO_EXPORT
kno_pool kno_make_pool(u8_string spec,
                     u8_string pooltype,
                     kno_storage_flags flags,
                     lispval opts)
{
  kno_pool_typeinfo ptype = kno_get_pool_typeinfo(pooltype);
  if (ptype == NULL)
    return KNO_ERR3(NULL,kno_UnknownPoolType,"kno_make_pool",pooltype);
  else if (ptype->handler == NULL)
    return KNO_ERR3(NULL,_("NoPoolHandler"),"kno_make_pool",pooltype);
  else if (ptype->handler->create == NULL)
    return KNO_ERR3(NULL,_("NoCreateHandler"),"kno_make_pool",pooltype);
  else if (fix_pool_opts(spec,opts)<0)
    return NULL;
  else return ptype->handler->create(spec,ptype->type_data,flags,opts);
}

/* Opening indexes */

static struct KNO_INDEX_TYPEINFO *index_typeinfo;
static u8_mutex index_typeinfo_lock;

KNO_EXPORT void kno_register_index_type
(u8_string name,
 kno_index_handler handler,
 kno_index (*opener)(u8_string filename,kno_storage_flags flags,lispval opts),
 u8_string (*matcher)(u8_string filename,void *),
 void *type_data)
{
  struct KNO_INDEX_TYPEINFO *ixtype;
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
  ixtype = u8_alloc(struct KNO_INDEX_TYPEINFO);
  ixtype->index_typename = u8_strdup(name);
  ixtype->handler = handler;
  ixtype->opener = opener;
  ixtype->matcher = matcher;
  ixtype->type_data = type_data;
  ixtype->next_type = index_typeinfo;
  index_typeinfo = ixtype;
  u8_unlock_mutex(&index_typeinfo_lock);
}

KNO_EXPORT kno_index_typeinfo kno_get_index_typeinfo(u8_string name)
{
  struct KNO_INDEX_TYPEINFO *ixtype = index_typeinfo;
  if (name == NULL)
    return default_index_type;
  else while (ixtype) {
      if (strcasecmp(name,ixtype->index_typename)==0)
        return ixtype;
      else ixtype = ixtype->next_type;}
  return NULL;
}

KNO_EXPORT kno_index_typeinfo kno_set_default_index_type(u8_string id)
{
  kno_index_typeinfo info = (id) ? (kno_get_index_typeinfo(id)) :
    (default_index_type);
  if (info)
    default_index_type=info;
  return info;
}


static kno_index open_index(kno_index_typeinfo ixtype,u8_string spec,
                           kno_storage_flags flags,lispval opts)
{
  u8_string search_spec = spec;
  if ( (strchr(search_spec,'@')==NULL) || (strchr(search_spec,':')==NULL) ) {
    if (u8_file_existsp(search_spec))
      search_spec = u8_realpath(spec,NULL);}
  kno_index found = (flags&KNO_STORAGE_UNREGISTERED) ? (NULL) :
    (kno_find_index_by_source(search_spec));
  kno_index opened = (found) ? (found) : (ixtype->opener(spec,flags,opts));
  if (search_spec != spec) u8_free(search_spec);
  if (opened==NULL) {
    if (! ( flags & KNO_STORAGE_NOERR) )
      return KNO_ERR(NULL,kno_CantOpenIndex,"kno_open_index",spec,opts);
    return opened;}
  lispval old_opts=opened->index_opts;
  if (old_opts != opts) {
    opened->index_opts=kno_incref(opts);
    kno_decref(old_opts);}
  return opened;
}

KNO_EXPORT
kno_index kno_open_index(u8_string spec,kno_storage_flags flags,lispval opts)
{
  CHECK_ERRNO();
  struct KNO_INDEX_TYPEINFO *ixtype = index_typeinfo;
  if (flags<0) flags = kno_get_dbflags(opts,KNO_STORAGE_ISINDEX);
  while (ixtype) {
    if (ixtype->matcher) {
      u8_string use_spec = ixtype->matcher(spec,ixtype->type_data);
      if (use_spec) {
        kno_index found = (flags&KNO_STORAGE_UNREGISTERED) ? (NULL) :
          (kno_find_index_by_source(use_spec));
        kno_index opened = (found) ? (found) :
          (ixtype->opener(use_spec,flags,opts));
        if (opened==NULL)
          return KNO_ERR(NULL,kno_CantOpenIndex,"kno_open_index",spec,opts);
        if (use_spec!=spec) u8_free(use_spec);
        lispval old_opts=opened->index_opts;
	if (old_opts != opts) {
	  opened->index_opts=kno_incref(opts);
	  kno_decref(old_opts);}
	if (found) kno_incref((lispval)found);
	return opened;}
      else ixtype = ixtype->next_type;
      CHECK_ERRNO();}
    else ixtype = ixtype->next_type;}
  lispval index_typeid = kno_getopt(opts,indextype_symbol,KNO_VOID);
  if (KNO_VOIDP(index_typeid))
    index_typeid = kno_getopt(opts,KNOSYM_TYPE,KNO_VOID);
  /* MODULE is an alias for type and 'may' have the additional
     semantics of being auto-loaded. */
  if (KNO_VOIDP(index_typeid))
    index_typeid = kno_getopt(opts,KNOSYM_MODULE,KNO_VOID);
  ixtype = (KNO_STRINGP(index_typeid)) ?
    (kno_get_index_typeinfo(KNO_CSTRING(index_typeid))) :
    (KNO_SYMBOLP(index_typeid)) ?
    (kno_get_index_typeinfo(KNO_SYMBOL_NAME(index_typeid))) :
    (NULL);
  kno_decref(index_typeid);
  if (ixtype) {
    u8_string open_spec = spec;
    if (ixtype->matcher) {
      open_spec = ixtype->matcher(spec,ixtype->type_data);
      if (open_spec == NULL) {
        unsigned char buf[200];
        return KNO_ERR(NULL,kno_CantOpenIndex,"kno_open_index",
                       u8_sprintf(buf,200,"(%s)%s",ixtype->index_typename,spec),
                       opts);}}
    kno_index ix = open_index(ixtype,open_spec,flags,opts);
    if (open_spec != spec) u8_free(open_spec);
    return ix;}
  if (!(flags & KNO_STORAGE_NOERR))
    kno_seterr(kno_UnknownIndexType,"kno_open_index",spec,opts);
  return NULL;
}

KNO_EXPORT
kno_index_handler kno_get_index_handler(u8_string name)
{
  struct KNO_INDEX_TYPEINFO *ixtype = index_typeinfo;
  while (ixtype) {
    if ((strcasecmp(name,ixtype->index_typename))==0)
      return ixtype->handler;
    else ixtype = ixtype->next_type;}
  return NULL;
}

KNO_EXPORT
kno_index kno_make_index(u8_string spec,
                       u8_string indextype,
                       kno_storage_flags flags,
                       lispval opts)
{
  kno_index_typeinfo ixtype = kno_get_index_typeinfo(indextype);
  if (ixtype == NULL)
    return KNO_ERR3(NULL,_("UnknownIndexType"),"kno_make_index",indextype);
  else if (ixtype->handler == NULL)
    return KNO_ERR3(NULL,_("NoIndexHandler"),"kno_make_index",indextype);
  else if (ixtype->handler->create == NULL)
    return KNO_ERR3(NULL,_("NoCreateHandler"),"kno_make_index",indextype);
  else {
    if (FIXNUMP(opts)) {
      lispval tmp_opts = kno_init_slotmap(NULL,3,NULL);
      kno_store(tmp_opts,kno_intern("size"),opts);
      kno_index ix=ixtype->handler->create(spec,ixtype->type_data,
                                          flags,tmp_opts);
      kno_decref(tmp_opts);
      return ix;}
    else return ixtype->handler->create(spec,ixtype->type_data,flags,opts);}
}

/* OIDCODE maps */

KNO_EXPORT lispval _kno_get_baseoid(struct KNO_OIDCODER *map,unsigned int oidcode)
{
  if (oidcode > map->n_oids)
    return KNO_VOID;
  else return map->baseoids[oidcode];
}

KNO_EXPORT int _kno_get_oidcode(struct KNO_OIDCODER *map,int oidbaseid)
{
  if (oidbaseid > map->max_baseid)
    return -1;
  else return map->oidcodes[oidbaseid];
}

KNO_EXPORT int kno_add_oidcode(struct KNO_OIDCODER *map,lispval oid)
{
  if (map->oidcodes == NULL) return -1;
  int baseid = KNO_OID_BASE_ID(oid);
  lispval baseoid = KNO_CONSTRUCT_OID(baseid,0);
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
      new_oids[i++]=KNO_FALSE;}
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

KNO_EXPORT void kno_init_oidcoder(struct KNO_OIDCODER *oidmap,
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
    int i=0; while (i<oids_len) oids[i++]=KNO_FALSE;}
  else {
    baseoids = u8_alloc_n(oids_len,lispval);
    int i=0; while (i<oids_len) {
      lispval init = oids[i];
      if ( (init) && (KNO_OIDP(init)) )
        baseoids[i++] = init;
      else baseoids[i++] = KNO_FALSE;}}
  int i = 0, codes_len = 1024, max_baseid=-1, last_oid = -1;
  while (codes_len < kno_n_base_oids) codes_len = codes_len*2;
  codes = u8_alloc_n(codes_len,unsigned int);
  i = 0; while (i<codes_len) codes[i++]= -1;
  i = 0; while (i<oids_len) {
      lispval baseoid = baseoids[i];
      if (KNO_OIDP(baseoid)) {
        int baseid    = KNO_OID_BASE_ID(baseoid);
        baseoids[i]   = baseoid;
        if (codes[baseid] != -1) {
          KNO_OID addr = KNO_OID_ADDR(baseoid);
          u8_log(LOG_WARN,"Duplicate baseoid",
                 "The baseoid @%llx/%llx (baseid=%d) is coded at %d and %d",
                 KNO_OID_LO(addr),KNO_OID_HI(addr),baseid,
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

KNO_EXPORT struct KNO_OIDCODER kno_copy_oidcodes(kno_oidcoder src)
{
  if (src->baseoids) {
    u8_read_lock(&(src->rwlock));

    struct KNO_OIDCODER oc = *src;
    memset(&(oc.rwlock),0,sizeof(oc.rwlock));   /* Just to be tidy */
    oc.baseoids = u8_memdup((SIZEOF_LISPVAL*oc.oids_len),oc.baseoids);
    oc.oidcodes = u8_memdup((SIZEOF_UINT*oc.codes_len),oc.oidcodes);
    u8_rw_unlock(&(src->rwlock));

    return oc;}
  else {
    struct KNO_OIDCODER oc = { 0 };
    return oc;}
}

KNO_EXPORT void kno_update_oidcodes(kno_oidcoder dest,kno_oidcoder src)
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

KNO_EXPORT void kno_recycle_oidcoder(struct KNO_OIDCODER *oc)
{
  if (oc->baseoids) u8_free(oc->baseoids);
  if (oc->oidcodes) u8_free(oc->oidcodes);
  memset(oc,0,sizeof(struct KNO_OIDCODER));
}

KNO_EXPORT lispval kno_baseoids_arg(lispval arg)
{
  if ( (KNO_FALSEP(arg)) || (KNO_VOIDP(arg)) ||
       ( (KNO_FIXNUMP(arg)) && (KNO_FIX2INT(arg) == 0)) )
    return KNO_VOID;
  else if (KNO_TRUEP(arg)) {
    lispval vec = kno_empty_vector(4);
    int i=0; while (i<4) {KNO_VECTOR_SET(vec,i,KNO_FALSE); i++;}
    return vec;}
  else if (KNO_FIXNUMP(arg)) {
    long long n = KNO_FIX2INT(arg);
    if (n<0) return KNO_ERROR_VALUE;
    lispval vec = kno_empty_vector(n);
    int i=0; while (i<n) {KNO_VECTOR_SET(vec,i,KNO_FALSE); i++;}
    return vec;}
  else if (KNO_VECTORP(arg)) {
    int i = 0, n = KNO_VECTOR_LENGTH(arg);
    while (i<n) {
      lispval elt = KNO_VECTOR_REF(arg,i);
      if (KNO_OIDP(elt)) i++;
      else if ( (KNO_FALSEP(elt)) || (KNO_VOIDP(elt)) ) {
        KNO_VECTOR_SET(arg,i,KNO_FALSE);
        i++;}
      else return KNO_ERROR_VALUE;}
    return kno_incref(arg);}
  else if (KNO_CHOICEP(arg)) {
    const lispval *elts = KNO_CHOICE_ELTS(arg);
    int i = 0, n = KNO_CHOICE_SIZE(arg);
    while (i<n) {
      lispval elt = elts[i];
      if (KNO_OIDP(elt)) i++;
      else return KNO_ERROR_VALUE;}
    return kno_make_vector(n,(lispval *)elts);}
  else return KNO_ERROR_VALUE;
}

/* Slot codes */

KNO_EXPORT lispval _kno_code2slotid(struct KNO_SLOTCODER *sc,unsigned int code)
{
  if (sc->slotids == NULL)
    return KNO_ERROR;
  else if (code < sc->n_slotcodes)
    return sc->slotids->vec_elts[code];
  else return KNO_ERROR;
}

KNO_EXPORT int _kno_slotid2code(struct KNO_SLOTCODER *sc,lispval slotid)
{
  if (sc->n_slotcodes <= 0)
    return -1;
  else {
    struct KNO_KEYVAL *kv =
      kno_sortvec_get(slotid,sc->lookup->sm_keyvals,sc->lookup->n_slots);
    if (kv) {
      lispval v = kv->kv_val;
      if (KNO_FIXNUMP(v))
        return KNO_FIX2INT(v);
      else return -1;}
    else return -1;}
}

static int cmp_slotkeys(const void *vx,const void *vy)
{
  const struct KNO_KEYVAL *kvx = vx;
  const struct KNO_KEYVAL *kvy = vy;
  if (kvx->kv_key < kvy->kv_key)
    return -1;
  else if (kvx->kv_key == kvy->kv_key)
    return 0;
  else return 1;
}

KNO_EXPORT int kno_add_slotcode(struct KNO_SLOTCODER *sc,lispval slotid)
{
  if (sc->slotids == NULL) return -1;
  int probe = kno_slotid2code(sc,slotid);
  if (! ( (OIDP(slotid)) || (SYMBOLP(slotid)) ) ) return -1;
  if (probe>=0)
    return probe;
  if (sc->n_slotcodes >= sc->slotids->vec_length) {
    size_t len = sc->slotids->vec_length;
    size_t new_len = (len<8) ? (16) : (len*2);
    lispval new_vec = kno_make_vector(new_len,NULL);
    lispval new_lookup = kno_make_slotmap(new_len,len,NULL);
    lispval *slotids = KNO_VECTOR_ELTS(((lispval)(sc->slotids)));
    struct KNO_KEYVAL *keyvals = KNO_XSLOTMAP_KEYVALS(sc->lookup);
    struct KNO_SLOTMAP *new_slotmap = (kno_slotmap) new_lookup;
    struct KNO_KEYVAL *new_keyvals = KNO_XSLOTMAP_KEYVALS(new_slotmap);
    KNO_XTABLE_SET_BIT(new_slotmap,KNO_SLOTMAP_SORT_KEYVALS,1);
    int i = 0, lim = sc->n_slotcodes;
    while (i < lim) {
      lispval slotid = slotids[i];
      lispval slotkey = keyvals[i].kv_key;
      KNO_VECTOR_SET(new_vec,i,slotid);
      new_keyvals[i].kv_key = slotkey;
      new_keyvals[i].kv_val = keyvals[i].kv_val;;
      kno_incref(slotid);
      kno_incref(slotkey);
      i++;}
    lispval old_slotids = (lispval) sc->slotids;
    lispval old_lookup = (lispval) sc->lookup;
    sc->slotids = (kno_vector) new_vec;
    sc->lookup = (kno_slotmap) new_lookup;
    kno_decref(old_slotids);
    kno_decref(old_lookup);}
  if (sc->n_slotcodes < sc->slotids->vec_length) {
    lispval vec = (lispval) sc->slotids;
    struct KNO_SLOTMAP *map = sc->lookup;
    int new_code = sc->n_slotcodes++;
    KNO_VECTOR_SET(vec,new_code,slotid);
    kno_slotmap_store(map,slotid,KNO_INT2FIX(new_code));
    return new_code;}
  return -1;
}

KNO_EXPORT int kno_init_slotcoder(struct KNO_SLOTCODER *sc,
                                int slotids_len,
                                lispval *slotids)
{
  if (sc->slotids) {
    kno_decref((lispval)(sc->slotids));
    sc->slotids = NULL;}
  if (sc->lookup) {
    kno_decref((lispval)(sc->lookup));
    sc->lookup = NULL;}
  if ( (slotids_len>=0) || (slotids) ) {
    lispval slot_vec = kno_make_vector(slotids_len,slotids);
    lispval lookup = kno_make_slotmap(slotids_len,0,NULL);
    struct KNO_KEYVAL *keyvals = KNO_SLOTMAP_KEYVALS(lookup);
    int i = 0, j = 0; while (i < slotids_len) {
      lispval slotid = (slotids) ? (slotids[i]) : (KNO_FALSE);
      if ( (KNO_SYMBOLP(slotid)) || (KNO_OIDP(slotid)) ) {
        keyvals[j].kv_key = slotid;
        keyvals[j].kv_val = KNO_INT2FIX(j);
        KNO_VECTOR_SET(slot_vec,j,slotid);
        j++;}
      i++;}
    sc->slotids = (kno_vector) slot_vec;
    sc->lookup = (kno_slotmap) lookup;
    sc->lookup->n_slots = j;
    KNO_XTABLE_SET_BIT(sc->lookup,KNO_SLOTMAP_SORT_KEYVALS,1);
    qsort(keyvals,j,KNO_KEYVAL_LEN,cmp_slotkeys);
    sc->n_slotcodes = j;
    sc->init_n_slotcodes = j;
    return slotids_len;}
  else {
    sc->n_slotcodes = 0;
    sc->init_n_slotcodes = 0;
    sc->slotids = (kno_vector)  NULL;
    sc->lookup  = (kno_slotmap) NULL;
    return 0;}
}

KNO_EXPORT struct KNO_SLOTCODER kno_copy_slotcodes(kno_slotcoder src)
{
  if (src->slotids) {
    u8_read_lock(&(src->rwlock));
    struct KNO_SLOTCODER sc = *src;
    memset(&(sc.rwlock),0,sizeof(sc.rwlock));   /* Just to be tidy */
    sc.slotids = (kno_vector) kno_copy((lispval)sc.slotids);
    sc.lookup = (kno_slotmap) kno_copy((lispval)sc.lookup);
    u8_rw_unlock(&(src->rwlock));
    return sc;}
  else {
    struct KNO_SLOTCODER sc = { 0 };
    return sc;}
}

KNO_EXPORT void kno_update_slotcodes(kno_slotcoder dest,kno_slotcoder src)
{
  u8_write_lock(&(dest->rwlock));
  struct KNO_VECTOR *old_vec = dest->slotids;
  struct KNO_SLOTMAP *old_map = dest->lookup;
  dest->n_slotcodes = src->n_slotcodes;
  dest->slotids = src->slotids;
  dest->lookup = src->lookup;
  u8_rw_unlock(& dest->rwlock);
  if (old_vec) kno_decref((lispval)old_vec);
  if (old_map) kno_decref((lispval)old_map);
}

#define DTOUT(lenvar,out) \
  if ((lenvar)>=0) {      \
    ssize_t _outlen = out; \
    if (_outlen<0) lenvar = -1; \
    else lenvar += _outlen;}

KNO_EXPORT void kno_recycle_slotcoder(struct KNO_SLOTCODER *sc)
{
  u8_destroy_rwlock(&(sc->rwlock));
  if (sc->slotids) kno_decref(((lispval)(sc->slotids)));
  if (sc->lookup) kno_decref(((lispval)(sc->lookup)));
  memset(sc,0,sizeof(struct KNO_SLOTCODER));
}

KNO_EXPORT lispval kno_slotids_arg(lispval arg)
{
  if ( (KNO_FALSEP(arg)) || (KNO_VOIDP(arg)) ||
       ( (KNO_FIXNUMP(arg)) && (KNO_FIX2INT(arg) == 0)) )
    return KNO_VOID;
  else if (KNO_FIXNUMP(arg)) {
    long long n = KNO_FIX2INT(arg);
    if (n<0) return KNO_ERROR_VALUE;
    lispval vec = kno_empty_vector(n);
    int i=0; while (i<n) {KNO_VECTOR_SET(vec,i,KNO_FALSE); i++;}
    return vec;}
  else if (KNO_TRUEP(arg)) {
    lispval vec = kno_empty_vector(4);
    int i=0; while (i<4) {KNO_VECTOR_SET(vec,i,KNO_FALSE); i++;}
    return vec;}
  else if (KNO_VECTORP(arg)) {
    int i = 0, n = KNO_VECTOR_LENGTH(arg);
    while (i<n) {
      lispval elt = KNO_VECTOR_REF(arg,i);
      if ( (KNO_OIDP(elt)) || (KNO_SYMBOLP(elt)) ) i++;
      else if ( (KNO_FALSEP(elt)) || (KNO_VOIDP(elt)) ) {
        KNO_VECTOR_SET(arg,i,KNO_FALSE);
        i++;}
      else return KNO_ERROR_VALUE;}
    return kno_incref(arg);}
  else if (KNO_CHOICEP(arg)) {
    const lispval *elts = KNO_CHOICE_ELTS(arg);
    int i = 0, n = KNO_CHOICE_SIZE(arg);
    while (i<n) {
      lispval elt = elts[i];
      if ( (KNO_OIDP(elt)) || (KNO_SYMBOLP(elt)) ) i++;
      else return KNO_ERROR_VALUE;}
    return kno_make_vector(n,(lispval *) elts);}
  else return KNO_ERROR_VALUE;
}

KNO_EXPORT ssize_t kno_encode_slotmap(struct KNO_OUTBUF *out,
                                    lispval value,
                                    struct KNO_SLOTCODER *slotcodes)
{
  if ( (slotcodes) && (slotcodes->slotids) && (KNO_SLOTMAPP(value)) ) {
    ssize_t dtype_len = 0;
    struct KNO_SLOTMAP *map = (kno_slotmap) value;
    int n_slots = map->n_slots;
    struct KNO_KEYVAL *kv = map->sm_keyvals;
    DTOUT(dtype_len,kno_write_byte(out,0xFF));
    DTOUT(dtype_len,kno_write_varint(out,n_slots));
    int i=0; while (i<n_slots) {
      lispval key = kv[i].kv_key;
      lispval val = kv[i].kv_val;
      int code = -1;
      if ( (KNO_SYMBOLP(key)) || (KNO_OIDP(key)) ) {
        code = kno_slotid2code(slotcodes,key);
        if (code<0)
          code = kno_add_slotcode(slotcodes,key);}
      if (code<0) {
        DTOUT(dtype_len,kno_write_dtype(out,key));
        DTOUT(dtype_len,kno_write_dtype(out,val));}
      else {
        DTOUT(dtype_len,kno_write_byte(out,0xFE));
        DTOUT(dtype_len,kno_write_varint(out,code));
        DTOUT(dtype_len,kno_write_dtype(out,val));}
      i++;}
    return dtype_len;}
  else return kno_write_dtype(out,value);
}

KNO_EXPORT lispval kno_decode_slotmap(struct KNO_INBUF *in,struct KNO_SLOTCODER *slotcodes)
{
  if ( (slotcodes) && (slotcodes->slotids) ) {
    int byte = kno_probe_byte(in);
    if (byte == 0xFF) {
      kno_read_byte(in); /* Already checked */ 
      int n_slots = kno_read_varint(in);
      lispval result = kno_make_slotmap(n_slots,n_slots,NULL);
      if (KNO_ABORTP(result)) return result;
      struct KNO_SLOTMAP *map = (kno_slotmap) result;
      struct KNO_KEYVAL *kv = map->sm_keyvals;
      int i = 0; while (i<n_slots) {
        lispval key, val;
        if (kno_probe_byte(in) == 0xFE) {
          kno_read_byte(in); /* Already checked */ 
          int code = kno_read_varint(in);
          key = kno_code2slotid(slotcodes,code);}
        else key = kno_read_dtype(in);
        if (KNO_ABORTP(key)) {
          kno_decref(result); return key;}
        else kv[i].kv_key=key;
        val = kno_read_dtype(in);
        if (KNO_ABORTP(val)) {
          kno_decref(result); return val;}
        else kv[i].kv_val = val;
        i++;}
      return result;}}
  return kno_read_dtype(in);
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

KNO_EXPORT int kno_write_rollback(u8_context caller,
                                u8_string id,u8_string source,
                                size_t size)
{
  u8_string rollback_file = u8_mkstring("%s.rollback",source);
  u8_string commit_file = u8_mkstring("%s.commit",source);
  int rv = try_remove(rollback_file,"LeftoverRollback","kno_write_rollback");
  if (rv<0) try_remove(commit_file,"LeftoverCommit","kno_write_rollback");
  else rv = try_remove(commit_file,"LeftoverCommit","kno_write_rollback");
  if (rv>=0) {
    ssize_t save_rv = kno_save_head(source,rollback_file,size);
    if (save_rv<0) {
      u8_seterr("CantSaveRollback",caller,rollback_file);
      rollback_file = NULL;
      rv = -1;}
    else rv=1;}
  if (commit_file) u8_free(commit_file);
  if (rollback_file) u8_free(rollback_file);
  return 1;
}

KNO_EXPORT int kno_check_rollback(u8_context caller,u8_string source)
{
  u8_string rollback_file = u8_mkstring("%s.rollback",source);
  if (u8_file_existsp(rollback_file)) {
    int source_fd = u8_open_fd(source,O_RDWR,S_IWUSR|S_IRUSR);
    if (source_fd >= 0) {
      int lock_rv = u8_lock_fd(source_fd,1);
      if (lock_rv<0) {
        u8_close_fd(source_fd);
        u8_seterr("RollbackLockFailed","kno_check_rollback",u8_strdup(source));
        return -1;}
      u8_log(LOG_WARN,"Rollback",
             "Applying rollback file %s to %s (%s)",
             rollback_file,source,caller);
      int rv = kno_apply_head(rollback_file,source);
      if (rv<0) {
        if (errno) u8_graberrno(caller,source);
        u8_string err_file = u8_string_append(rollback_file,".bad",NULL);
        int move_rv = u8_movefile(rollback_file,err_file);
        if (move_rv<0) {
          u8_graberrno("kno_check_rollback/cleanup",rollback_file);
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
          if (rm_rv<0) kno_clear_errors(1);}
        if (u8_file_existsp(commit_file)) {
          int rm_rv = u8_removefile(commit_file);
          if (rm_rv<0) kno_clear_errors(1);}
        int mv_rv = u8_movefile(rollback_file,applied_file);
        if (mv_rv<0) kno_clear_errors(1);
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

KNO_EXPORT int kno_remove_suffix(u8_string base,u8_string suffix)
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

KNO_EXPORT u8_string kno_match_pool_file(u8_string spec,void *data)
{
  u8_string abspath=u8_abspath(spec,NULL);
  if ((u8_file_existsp(abspath)) &&
      (kno_match4bytes(abspath,data)))
    return abspath;
  else if (u8_has_suffix(spec,".pool",1)) {
    u8_free(abspath);
    return NULL;}
  else {
    u8_string new_spec = u8_mkstring("%s.pool",spec);
    u8_string variation = u8_abspath(new_spec,NULL);
    u8_free(abspath); u8_free(new_spec);
    if ((u8_file_existsp(variation))&&
        (kno_match4bytes(variation,data))) {
      return variation;}
    else {
      u8_free(variation);
      return NULL;}}
}

KNO_EXPORT u8_string kno_match_index_file(u8_string spec,void *data)
{
  u8_string abspath=u8_abspath(spec,NULL);
  if ((u8_file_existsp(abspath)) &&
      (kno_match4bytes(abspath,data)))
    return abspath;
  else if (u8_has_suffix(spec,".index",1)) {
    u8_free(abspath);
    return NULL;}
  else {
    u8_string new_spec = u8_mkstring("%s.index",spec);
    u8_string variation = u8_abspath(new_spec,NULL);
    u8_free(abspath); u8_free(new_spec);
    if ((u8_file_existsp(variation))&&
        (kno_match4bytes(variation,data))) {
      return variation;}
    else {
      u8_free(variation);
      return NULL;}}
}

/* Setting fileinfo */

static u8_uid get_owner(lispval spec)
{
  if (KNO_UINTP(spec))
    return (u8_uid) (KNO_FIX2INT(spec));
  else if (KNO_STRINGP(spec)) {
    u8_uid uid = u8_getuid(KNO_CSTRING(spec));
    if (uid>=0) return uid;}
  else {}
  u8_logf(LOG_WARN,"NoSuchUser",
          "Couldn't identify a user from %q",spec);
  return (u8_gid) -1;
}

static u8_gid get_group(lispval spec)
{
  if (KNO_UINTP(spec))
    return (u8_gid) (KNO_FIX2INT(spec));
  else if (KNO_STRINGP(spec)) {
    u8_gid gid = u8_getgid(KNO_CSTRING(spec));
    if (gid>=0) return gid;}
  else {}
  u8_logf(LOG_WARN,"NoSuchGroup",
          "Couldn't identify a group from %q",spec);
  return (u8_gid) -1;
}

KNO_EXPORT int kno_set_file_opts(u8_string filename,lispval opts)
{
  lispval owner = kno_getopt(opts,kno_intern("owner"),KNO_VOID);
  lispval group = kno_getopt(opts,kno_intern("group"),KNO_VOID);
  lispval mode = kno_getopt(opts,kno_intern("mode"),KNO_VOID);
  int set_rv = 0;
  if ( (KNO_VOIDP(owner))  &&
       (KNO_VOIDP(group)) &&
       (KNO_VOIDP(mode)) )
    return set_rv;
  if (! ((KNO_VOIDP(owner)) && (KNO_VOIDP(group))) ) {
    u8_uid file_owner = (KNO_VOIDP(owner)) ? (-1) : (get_owner(owner));
    u8_gid file_group = (KNO_VOIDP(group)) ? (-1) : (get_group(group));
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
  if (KNO_VOIDP(mode))
    return set_rv;
  else if ( (KNO_UINTP(mode)) && ((KNO_FIX2INT(mode)) < 512) ) {
    char *s = u8_tolibc(filename);
    int rv = u8_chmod(s,((mode_t)(KNO_FIX2INT(mode))));
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

void kno_init_mempool_c(void);
void kno_init_extpool_c(void);
void kno_init_extindex_c(void);
void kno_init_procpool_c(void);
void kno_init_procindex_c(void);
void kno_init_aggregates_c(void);
void kno_init_tempindex_c(void);

static int drivers_c_initialized = 0;

KNO_EXPORT int kno_init_drivers_c()
{
  if (drivers_c_initialized) return drivers_c_initialized;
  drivers_c_initialized = 307*kno_init_storage();

  u8_register_source_file(_FILEINFO);

  kno_init_extindex_c();
  kno_init_mempool_c();
  kno_init_extpool_c();
  kno_init_procpool_c();
  kno_init_procindex_c();
  kno_init_aggregates_c();
  kno_init_tempindex_c();

  rev_symbol = kno_intern("rev");
  gentime_symbol = kno_intern("gentime");
  packtime_symbol = kno_intern("packtime");
  modtime_symbol = kno_intern("modtime");
  adjuncts_symbol = kno_intern("adjuncts");
  wadjuncts_symbol = kno_intern("w/adjuncts");

  kno_cachelevel_op=kno_intern("cachelevel");
  kno_bufsize_op=kno_intern("bufsize");
  kno_mmap_op=kno_intern("mmap");
  kno_preload_op=kno_intern("preload");
  kno_swapout_op=kno_intern("swapout");
  kno_reload_op=kno_intern("reload");
  kno_stats_op=kno_intern("stats");
  kno_label_op=kno_intern("label");
  kno_source_op=kno_intern("source");
  kno_populate_op=kno_intern("populate");
  kno_getmap_op=kno_intern("getmap");
  kno_xrefs_op=kno_intern("xrefs");
  kno_slotids_op=kno_intern("slotids");
  kno_baseoids_op=kno_intern("baseoids");
  kno_load_op=kno_intern("load");
  kno_capacity_op=kno_intern("capacity");
  kno_metadata_op=kno_intern("metadata");
  kno_raw_metadata_op=kno_intern("%metadata");
  kno_keys_op=kno_intern("keys");
  kno_keycount_op=kno_intern("keycount");
  kno_partitions_op=kno_intern("partitions");
  pooltype_symbol=kno_intern("pooltype");
  indextype_symbol=kno_intern("indextype");

  u8_init_mutex(&pool_typeinfo_lock);
  u8_init_mutex(&index_typeinfo_lock);

  kno_register_config("ACIDFILES",
                     "Maintain acidity of individual file pools and indexes",
                     kno_boolconfig_get,kno_boolconfig_set,&kno_acid_files);
  kno_register_config("DRIVERBUFSIZE",
                     "The size of file streams used in database files",
                     kno_sizeconfig_get,kno_sizeconfig_set,&kno_driver_bufsize);


  return drivers_c_initialized;
}

