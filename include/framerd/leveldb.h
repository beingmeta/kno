typedef struct FRAMERD_LEVELDB {
  u8_string path;
  fdtype opts;
  unsigned int closed:1, readonly;
  struct leveldb_t *dbptr;
  struct leveldb_options_t *optionsptr;
  struct leveldb_env_t *envptr;
  struct leveldb_cache_t *cacheptr;} *framerd_leveldb;

typedef struct FD_LEVELDB {
  FD_CONS_HEADER;
  struct FRAMERD_LEVELDB leveldb;} *fd_leveldb;

FD_EXPORT fd_ptr_type fd_leveldb_type;

FD_EXPORT int fd_init_leveldb(void) FD_LIBINIT_FN;

typedef struct FD_LEVELDB_POOL {
  FD_POOL_FIELDS;
  unsigned int pool_load; time_t pool_modtime;
  unsigned int read_only:1, locked:1;
  int pool_n_schemas, pool_max_slotids;
  struct FD_SCHEMA_ENTRY *pool_schemas;
  struct FD_SCHEMA_LOOKUP *pool_schbyval;
  U8_MUTEX_DECL(pool_lock);
  struct FRAMERD_LEVELDB leveldb;} FD_LEVELDB_POOL;
typedef struct FD_LEVELDB_POOL *fd_leveldb_pool;

