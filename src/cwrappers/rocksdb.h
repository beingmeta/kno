typedef struct KNO_ROCKSDB {
  KNO_CONS_HEADER;
  u8_string path, realpath;
  lispval opts;
  struct KNO_ROCKSDB *rocksdb_list;
  unsigned int readonly, saved_xrefs;
  enum rocksdb_status {
    rocksdb_raw = 0,
    rocksdb_sketchy,
    rocksdb_closed,
    rocksdb_opened,
    rocksdb_opening,
    rocksdb_closing,
    rocksdb_error } dbstatus;
  struct XTYPE_REFS xrefs;
  U8_MUTEX_DECL(rocksdb_lock);
  struct rocksdb_t *dbptr;
  struct rocksdb_options_t *optionsptr;
  struct rocksdb_readoptions_t *readopts;
  struct rocksdb_writeoptions_t *writeopts;
  rocksdb_block_based_table_options_t *blockopts;
  struct rocksdb_cache_t *cacheptr;
  struct rocksdb_env_t *envptr;} *kno_rocksdb;

KNO_EXPORT kno_lisp_type kno_rocksdb_type;

KNO_EXPORT int kno_init_rocksdb(void) KNO_LIBINIT_FN;

typedef struct KNO_ROCKSDB_POOL {
  KNO_POOL_FIELDS;
  unsigned int pool_load; time_t pool_mtime;
  unsigned int locked:1;
  struct KNO_ROCKSDB *rocksdb;} KNO_ROCKSDB_POOL;
typedef struct KNO_ROCKSDB_POOL *kno_rocksdb_pool;

typedef struct KNO_ROCKSDB_INDEX {
  KNO_INDEX_FIELDS;
  unsigned int locked:1;
  struct KNO_ROCKSDB *rocksdb;} KNO_ROCKSDB_INDEX;
typedef struct KNO_ROCKSDB_INDEX *kno_rocksdb_index;

