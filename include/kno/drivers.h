/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef KNO_DRIVERS_H
#define KNO_DRIVERS_H 1
#ifndef KNO_DRIVERS_H_INFO
#define KNO_DRIVERS_H_INFO "include/kno/drivers.h"
#endif

KNO_EXPORT size_t kno_driver_bufsize;
KNO_EXPORT size_t kno_driver_writesize;

KNO_EXPORT ssize_t kno_get_bufsize(lispval,ssize_t,int);

KNO_EXPORT int kno_acid_files;

KNO_EXPORT int kno_init_drivers(void) KNO_LIBINIT_FN;

KNO_EXPORT kno_compress_type kno_compression_type(lispval,kno_compress_type);

/* These are common pool/index ops (for use with kno_poolctl/kno_indexctl) */

KNO_EXPORT lispval kno_cachelevel_op, kno_bufsize_op, kno_mmap_op, kno_preload_op;
KNO_EXPORT lispval kno_metadata_op, kno_raw_metadata_op, kno_reload_op;
KNO_EXPORT lispval kno_stats_op, kno_label_op, kno_source_op, kno_populate_op, kno_swapout_op;
KNO_EXPORT lispval kno_getmap_op, kno_xrefs_op, kno_slotids_op, kno_baseoids_op, kno_keys_op;
KNO_EXPORT lispval kno_load_op, kno_capacity_op, kno_keycount_op, kno_partitions_op;

KNO_EXPORT u8_condition kno_InvalidOffsetType;
KNO_EXPORT u8_condition kno_BadMetaData, kno_FutureMetaData;
KNO_EXPORT u8_condition kno_MMAPError, kno_MUNMAPError;
KNO_EXPORT u8_condition kno_RecoveryRequired;

KNO_EXPORT u8_condition kno_PoolDriverError, kno_IndexDriverError;
KNO_EXPORT u8_condition kno_UnknownPoolType, kno_UnknownIndexType;
KNO_EXPORT u8_condition kno_CantOpenPool, kno_CantOpenIndex;
KNO_EXPORT u8_condition kno_CantFindPool, kno_CantFindIndex;
KNO_EXPORT u8_condition kno_PoolFileSizeOverflow, kno_FileIndexSizeOverflow;
KNO_EXPORT u8_condition kno_CorruptedPool, kno_CorruptedIndex;

KNO_EXPORT u8_string kno_match4bytes(u8_string file,void *data);
KNO_EXPORT u8_string kno_netspecp(u8_string file,void *data);
KNO_EXPORT int kno_same_sourcep(u8_string ref,u8_string source);


struct KNO_POOL_TYPEINFO {
  u8_string pool_typename;
  kno_pool_handler handler;
  kno_pool (*opener)(u8_string filename,kno_storage_flags flags,lispval opts);
  u8_string (*matcher)(u8_string filename,void *);
  void *type_data;
  struct KNO_POOL_TYPEINFO *next_type;};
typedef struct KNO_POOL_TYPEINFO *kno_pool_typeinfo;
struct KNO_POOL_TYPEINFO *kno_get_pool_typeinfo(u8_string string);

KNO_EXPORT
void kno_register_pool_type
(u8_string name,
 kno_pool_handler pool_handler,
 kno_pool (*opener)(u8_string path,kno_storage_flags flags,lispval opts),
 u8_string (*matcher)(u8_string path,void *),
 void *type_data);

KNO_EXPORT kno_pool_typeinfo kno_set_default_pool_type(u8_string name);

KNO_EXPORT kno_pool_handler kno_get_pool_handler(u8_string name);

KNO_EXPORT kno_pool kno_make_pool(u8_string spec,u8_string pooltype,
                               kno_storage_flags flags,lispval opts);

#define KNO_POOLSTREAM_LOCKEDP(fp) \
  (((fp)->pool_stream.stream_flags)&KNO_STREAM_FILE_LOCKED)

/* File indexes */

struct KNO_INDEX_TYPEINFO {
  u8_string index_typename;
  kno_index_handler handler;
  kno_index (*opener)(u8_string filename,kno_storage_flags flags,lispval opts);
  u8_string (*matcher)(u8_string filename,void *);
  void *type_data;
  struct KNO_INDEX_TYPEINFO *next_type;};
typedef struct KNO_INDEX_TYPEINFO *kno_index_typeinfo;
struct KNO_INDEX_TYPEINFO *kno_get_index_typeinfo(u8_string string);

KNO_EXPORT
void kno_register_index_type
(u8_string name,
 kno_index_handler handler,
 kno_index (*opener)(u8_string spec,
                    kno_storage_flags flags,
                    lispval opts),
 u8_string (*matcher)(u8_string spec,
                      void *),
 void *type_data);

KNO_EXPORT kno_index_typeinfo kno_set_default_index_type(u8_string name);

KNO_EXPORT kno_index_handler kno_get_index_handler(u8_string name);

KNO_EXPORT kno_index kno_make_index(u8_string spec,u8_string indextype,
                                 kno_storage_flags flags,lispval opts);

KNO_EXPORT lispval kno_read_index_metadata(kno_stream ds);
KNO_EXPORT lispval kno_write_index_metadata(kno_stream,lispval);
KNO_EXPORT int kno_make_fileindex(u8_string,unsigned int,int);

unsigned int kno_hash_lisp1(lispval x);
unsigned int kno_hash_lisp2(lispval x);
unsigned int kno_hash_lisp3(lispval x);
unsigned int kno_hash_dtype_rep(lispval x);

/* Coding OIDs */

typedef struct KNO_OIDCODER {
  unsigned int n_oids, oids_len, init_n_oids;
  int max_baseid, codes_len;
  lispval *baseoids;
  unsigned int *oidcodes;
  U8_RWLOCK_DECL(rwlock);} *kno_oidcoder;

KNO_EXPORT lispval _kno_get_baseoid(struct KNO_OIDCODER *map,unsigned int code);
KNO_EXPORT int _kno_get_oidcode(struct KNO_OIDCODER *map,int oidbaseid);
KNO_EXPORT int kno_add_oidcode(struct KNO_OIDCODER *map,lispval oid);
KNO_EXPORT void kno_init_oidcoder(struct KNO_OIDCODER *,int,lispval *);
KNO_EXPORT void kno_recycle_oidcoder(struct KNO_OIDCODER *);

#if (KNO_SOURCE || KNO_DRIVER_SOURCE)
KNO_FASTOP lispval kno_get_baseoid(struct KNO_OIDCODER *map,unsigned int code)
{
  if (map->baseoids == NULL)
    return KNO_VOID;
  else if (code > map->n_oids)
    return KNO_VOID;
  else return map->baseoids[code];
}

KNO_FASTOP int kno_get_oidcode(struct KNO_OIDCODER *map,int baseid)
{
  if (map->oidcodes == NULL)
    return -1;
  else if (baseid > map->max_baseid)
    return -1;
  else return map->oidcodes[baseid];
}
#else
#define kno_get_baseoid _kno_get_baseoid
#define kno_get_oidcode _kno_get_oidcode
#endif

KNO_EXPORT struct KNO_OIDCODER kno_copy_oidcodes(kno_oidcoder src);
KNO_EXPORT void kno_update_oidcodes(kno_oidcoder dest,kno_oidcoder src);
KNO_EXPORT lispval kno_baseoids_arg(lispval arg);

/* Coding slotids */

typedef struct KNO_SLOTCODER {
  int n_slotcodes, init_n_slotcodes;
  struct KNO_VECTOR *slotids;
  struct KNO_SLOTMAP *lookup;
  U8_RWLOCK_DECL(rwlock);} *kno_slotcoder;

KNO_EXPORT int _kno_slotid2code(struct KNO_SLOTCODER *,lispval slot);
KNO_EXPORT lispval _kno_code2slotid(struct KNO_SLOTCODER *,unsigned int code);
KNO_EXPORT int kno_add_slotcode(struct KNO_SLOTCODER *,lispval slot);
KNO_EXPORT int kno_init_slotcoder(struct KNO_SLOTCODER *,int,lispval *);
KNO_EXPORT void kno_recycle_slotcoder(struct KNO_SLOTCODER *);

KNO_EXPORT lispval kno_decode_slotmap(struct KNO_INBUF *in,struct KNO_SLOTCODER *slotcodes);
KNO_EXPORT ssize_t kno_encode_slotmap
(struct KNO_OUTBUF *out,lispval value,struct KNO_SLOTCODER *slotcodes);

#if (KNO_SOURCE || KNO_DRIVER_SOURCE)
KNO_FASTOP lispval kno_code2slotid(struct KNO_SLOTCODER *sc,unsigned int code)
{
  if (sc->slotids == NULL)
    return KNO_ERROR;
  else if (code < sc->n_slotcodes)
    return sc->slotids->vec_elts[code];
  else return KNO_ERROR;
}

KNO_FASTOP int kno_slotid2code(struct KNO_SLOTCODER *sc,lispval slotid)
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
#else
#define kno_slotid2code _kno_slotid2code
#define kno_code2slotid _kno_code2slotid
#endif

U8_MAYBE_UNUSED
static void kno_use_slotcodes(struct KNO_SLOTCODER *sc)
{
  u8_read_lock(&(sc->rwlock));
}

U8_MAYBE_UNUSED
static void kno_release_slotcodes(struct KNO_SLOTCODER *sc)
{
  u8_rw_unlock(&(sc->rwlock));
}

KNO_EXPORT void kno_update_slotcodes(kno_slotcoder dest,kno_slotcoder src);
KNO_EXPORT struct KNO_SLOTCODER kno_copy_slotcodes(kno_slotcoder src);
KNO_EXPORT lispval kno_slotids_arg(lispval arg);

/* Functional arguments */

#define KNO_ISFUNARG(fn) \
  (KNO_USUALLY((fn == KNO_VOID)||(fn == KNO_FALSE)||(KNO_APPLICABLEP(fn))))

/* Matching db file names */

KNO_EXPORT u8_string kno_match_pool_file(u8_string spec,void *data);
KNO_EXPORT u8_string kno_match_index_file(u8_string spec,void *data);

KNO_EXPORT int kno_remove_suffix(u8_string base,u8_string suffix);

KNO_EXPORT int kno_write_rollback(u8_context caller,
                                u8_string id,u8_string source,
                                size_t size);
KNO_EXPORT int kno_check_rollback(u8_context caller,u8_string source);

/* Setting file options */

KNO_EXPORT int kno_set_file_opts(u8_string filename,lispval opts);

/* zpathstore prototypes */

KNO_EXPORT lispval kno_open_zpathstore(u8_string path,lispval opts);
KNO_EXPORT lispval kno_get_zipsource(u8_string path);

#endif /* #ifndef KNO_DRIVERS_H */

