/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef FRAMERD_DRIVERS_H
#define FRAMERD_DRIVERS_H 1
#ifndef FRAMERD_DRIVERS_H_INFO
#define FRAMERD_DRIVERS_H_INFO "include/framerd/drivers.h"
#endif

FD_EXPORT size_t fd_driver_bufsize;

FD_EXPORT int fd_acid_files;

FD_EXPORT int fd_init_drivers(void) FD_LIBINIT_FN;

FD_EXPORT fd_compress_type fd_compression_type(lispval,fd_compress_type);

/* These are common pool/index ops (for use with fd_poolctl/fd_indexctl) */

FD_EXPORT lispval fd_cachelevel_op, fd_bufsize_op, fd_mmap_op, fd_preload_op;
FD_EXPORT lispval fd_metadata_op, fd_raw_metadata_op, fd_reload_op;
FD_EXPORT lispval fd_stats_op, fd_label_op, fd_populate_op, fd_swapout_op;
FD_EXPORT lispval fd_getmap_op, fd_slotids_op, fd_baseoids_op, fd_keys_op;
FD_EXPORT lispval fd_load_op, fd_capacity_op, fd_keycount_op, fd_partitions_op;

FD_EXPORT u8_condition fd_InvalidOffsetType;
FD_EXPORT u8_condition fd_BadMetaData, fd_FutureMetaData;
FD_EXPORT u8_condition fd_MMAPError, fd_MUNMAPError;
FD_EXPORT u8_condition fd_RecoveryRequired;

FD_EXPORT u8_condition fd_PoolDriverError, fd_IndexDriverError;
FD_EXPORT u8_condition fd_UnknownPoolType, fd_UnknownIndexType;
FD_EXPORT u8_condition fd_CantOpenPool, fd_CantOpenIndex;
FD_EXPORT u8_condition fd_CantFindPool, fd_CantFindIndex;
FD_EXPORT u8_condition fd_PoolFileSizeOverflow, fd_FileIndexSizeOverflow;
FD_EXPORT u8_condition fd_CorruptedPool, fd_CorruptedIndex;

FD_EXPORT u8_string fd_match4bytes(u8_string file,void *data);
FD_EXPORT u8_string fd_netspecp(u8_string file,void *data);
FD_EXPORT int fd_same_sourcep(u8_string ref,u8_string source);


struct FD_POOL_TYPEINFO {
  u8_string pool_typename;
  fd_pool_handler handler;
  fd_pool (*opener)(u8_string filename,fd_storage_flags flags,lispval opts);
  u8_string (*matcher)(u8_string filename,void *);
  void *type_data;
  struct FD_POOL_TYPEINFO *next_type;};
typedef struct FD_POOL_TYPEINFO *fd_pool_typeinfo;
struct FD_POOL_TYPEINFO *fd_get_pool_typeinfo(u8_string string);

FD_EXPORT
void fd_register_pool_type
(u8_string name,
 fd_pool_handler pool_handler,
 fd_pool (*opener)(u8_string path,fd_storage_flags flags,lispval opts),
 u8_string (*matcher)(u8_string path,void *),
 void *type_data);

FD_EXPORT fd_pool_typeinfo fd_set_default_pool_type(u8_string name);

FD_EXPORT fd_pool_handler fd_get_pool_handler(u8_string name);

FD_EXPORT fd_pool fd_make_pool(u8_string spec,u8_string pooltype,
                               fd_storage_flags flags,lispval opts);

#define FD_POOLSTREAM_LOCKEDP(fp) \
  (((fp)->pool_stream.stream_flags)&FD_STREAM_FILE_LOCKED)

/* File indexes */

struct FD_INDEX_TYPEINFO {
  u8_string index_typename;
  fd_index_handler handler;
  fd_index (*opener)(u8_string filename,fd_storage_flags flags,lispval opts);
  u8_string (*matcher)(u8_string filename,void *);
  void *type_data;
  struct FD_INDEX_TYPEINFO *next_type;};
typedef struct FD_INDEX_TYPEINFO *fd_index_typeinfo;
struct FD_INDEX_TYPEINFO *fd_get_index_typeinfo(u8_string string);

FD_EXPORT
void fd_register_index_type
(u8_string name,
 fd_index_handler handler,
 fd_index (*opener)(u8_string spec,
                    fd_storage_flags flags,
                    lispval opts),
 u8_string (*matcher)(u8_string spec,
                      void *),
 void *type_data);

FD_EXPORT fd_index_typeinfo fd_set_default_index_type(u8_string name);

FD_EXPORT fd_index_handler fd_get_index_handler(u8_string name);

FD_EXPORT fd_index fd_make_index(u8_string spec,u8_string indextype,
                                 fd_storage_flags flags,lispval opts);

FD_EXPORT lispval fd_read_index_metadata(fd_stream ds);
FD_EXPORT lispval fd_write_index_metadata(fd_stream,lispval);
FD_EXPORT int fd_make_fileindex(u8_string,unsigned int,int);

unsigned int fd_hash_lisp1(lispval x);
unsigned int fd_hash_lisp2(lispval x);
unsigned int fd_hash_lisp3(lispval x);
unsigned int fd_hash_dtype_rep(lispval x);

/* Coding OIDs */

typedef struct FD_OIDCODER {
  unsigned int n_oids, oids_len, init_n_oids;
  int max_baseid, codes_len;
  lispval *baseoids;
  unsigned int *oidcodes;
  U8_RWLOCK_DECL(rwlock);} *fd_oidcoder;

FD_EXPORT lispval _fd_get_baseoid(struct FD_OIDCODER *map,unsigned int code);
FD_EXPORT int _fd_get_oidcode(struct FD_OIDCODER *map,int oidbaseid);
FD_EXPORT int fd_add_oidcode(struct FD_OIDCODER *map,lispval oid);
FD_EXPORT void fd_init_oidcoder(struct FD_OIDCODER *,int,lispval *);
FD_EXPORT void fd_recycle_oidcoder(struct FD_OIDCODER *);

#if (FRAMERD_SOURCE || FD_DRIVER_SOURCE)
FD_FASTOP lispval fd_get_baseoid(struct FD_OIDCODER *map,unsigned int code)
{
  if (map->baseoids == NULL)
    return FD_VOID;
  else if (code > map->n_oids)
    return FD_VOID;
  else return map->baseoids[code];
}

FD_FASTOP int fd_get_oidcode(struct FD_OIDCODER *map,int baseid)
{
  if (map->oidcodes == NULL)
    return -1;
  else if (baseid > map->max_baseid)
    return -1;
  else return map->oidcodes[baseid];
}
#else
#define fd_get_baseoid _fd_get_baseoid
#define fd_get_oidcode _fd_get_oidcode
#endif

FD_EXPORT struct FD_OIDCODER fd_copy_oidcodes(fd_oidcoder src);
FD_EXPORT void fd_update_oidcodes(fd_oidcoder dest,fd_oidcoder src);
FD_EXPORT lispval fd_baseoids_arg(lispval arg);

/* Coding slotids */

typedef struct FD_SLOTCODER {
  int n_slotcodes, init_n_slotcodes;
  struct FD_VECTOR *slotids;
  struct FD_SLOTMAP *lookup;
  U8_RWLOCK_DECL(rwlock);} *fd_slotcoder;

FD_EXPORT int _fd_slotid2code(struct FD_SLOTCODER *,lispval slot);
FD_EXPORT lispval _fd_code2slotid(struct FD_SLOTCODER *,unsigned int code);
FD_EXPORT int fd_add_slotcode(struct FD_SLOTCODER *,lispval slot);
FD_EXPORT int fd_init_slotcoder(struct FD_SLOTCODER *,int,lispval *);
FD_EXPORT void fd_recycle_slotcoder(struct FD_SLOTCODER *);

FD_EXPORT lispval fd_decode_slotmap(struct FD_INBUF *in,struct FD_SLOTCODER *slotcodes);
FD_EXPORT ssize_t fd_encode_slotmap
(struct FD_OUTBUF *out,lispval value,struct FD_SLOTCODER *slotcodes);

#if (FRAMERD_SOURCE || FD_DRIVER_SOURCE)
FD_FASTOP lispval fd_code2slotid(struct FD_SLOTCODER *sc,unsigned int code)
{
  if (sc->slotids == NULL)
    return FD_ERROR;
  else if (code < sc->n_slotcodes)
    return sc->slotids->vec_elts[code];
  else return FD_ERROR;
}

FD_FASTOP int fd_slotid2code(struct FD_SLOTCODER *sc,lispval slotid)
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
#else
#define fd_slotid2code _fd_slotid2code
#define fd_code2slotid _fd_code2slotid
#endif

U8_MAYBE_UNUSED
static void fd_use_slotcodes(struct FD_SLOTCODER *sc)
{
  u8_read_lock(&(sc->rwlock));
}

U8_MAYBE_UNUSED
static void fd_release_slotcodes(struct FD_SLOTCODER *sc)
{
  u8_rw_unlock(&(sc->rwlock));
}

FD_EXPORT void fd_update_slotcodes(fd_slotcoder dest,fd_slotcoder src);
FD_EXPORT struct FD_SLOTCODER fd_copy_slotcodes(fd_slotcoder src);
FD_EXPORT lispval fd_slotids_arg(lispval arg);

/* Functional arguments */

#define FD_ISFUNARG(fn) \
  (FD_EXPECT_TRUE((fn == FD_VOID)||(fn == FD_FALSE)||(FD_APPLICABLEP(fn))))

/* Matching db file names */

FD_EXPORT u8_string fd_match_pool_file(u8_string spec,void *data);
FD_EXPORT u8_string fd_match_index_file(u8_string spec,void *data);

FD_EXPORT int fd_remove_suffix(u8_string base,u8_string suffix);

FD_EXPORT int fd_write_rollback(u8_context caller,
                                u8_string id,u8_string source,
                                size_t size);
FD_EXPORT int fd_check_rollback(u8_context caller,u8_string source);

/* Setting file options */

FD_EXPORT int fd_set_file_opts(u8_string filename,lispval opts);

#endif /* #ifndef FRAMERD_DRIVERS_H */

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
