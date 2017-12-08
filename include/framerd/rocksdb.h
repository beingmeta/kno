typedef struct FRAMERD_ROCKSDB {
  u8_string path; lispval opts;
  unsigned int readonly:1;
  enum rocksdb_status {
    rocksdb_raw = 0,
    rocksdb_sketchy,
    rocksdb_closed,
    rocksdb_opened,
    rocksdb_opening,
    rocksdb_closing,
    rocksdb_error } dbstatus;
  U8_MUTEX_DECL(rocksdb_lock);
  struct rocksdb_t *dbptr;
  struct rocksdb_options_t *optionsptr;
  struct rocksdb_readoptions_t *readopts;
  struct rocksdb_writeoptions_t *writeopts;
  struct rocksdb_cache_t *cacheptr;
  struct rocksdb_env_t *envptr;} *framerd_rocksdb;

typedef struct FD_ROCKSDB {
  FD_CONS_HEADER;
  struct FRAMERD_ROCKSDB rocksdb;} *fd_rocksdb;

FD_EXPORT fd_ptr_type fd_rocksdb_type;

FD_EXPORT int fd_init_rocksdb(void) FD_LIBINIT_FN;

typedef struct FD_ROCKSDB_POOL {
  FD_POOL_FIELDS;
  unsigned int pool_load; time_t pool_mtime;
  unsigned int locked:1;
  struct FD_SLOTCODER slotcodes;
  struct FRAMERD_ROCKSDB rocksdb;} FD_ROCKSDB_POOL;
typedef struct FD_ROCKSDB_POOL *fd_rocksdb_pool;

typedef struct FD_ROCKSDB_INDEX {
  FD_INDEX_FIELDS;
  unsigned int locked:1;
  
  struct FD_SLOTCODER slotcodes;
  struct FD_OIDCODER oidcodes;

  struct FD_HASHTABLE slotids_table;
  struct FRAMERD_ROCKSDB rocksdb;} FD_ROCKSDB_INDEX;
typedef struct FD_ROCKSDB_INDEX *fd_rocksdb_index;

