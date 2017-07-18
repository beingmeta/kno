typedef struct FRAMERD_LEVELDB {
  u8_string path; lispval opts;
  unsigned int readonly:1;
  enum leveldb_status {
    leveldb_raw = 0,
    leveldb_sketchy,
    leveldb_closed,
    leveldb_opened,
    leveldb_opening,
    leveldb_closing,
    leveldb_error } dbstatus;
  U8_MUTEX_DECL(leveldb_lock);
  struct leveldb_t *dbptr;
  struct leveldb_options_t *optionsptr;
  struct leveldb_readoptions_t *readopts;
  struct leveldb_writeoptions_t *writeopts;
  struct leveldb_cache_t *cacheptr;
  struct leveldb_env_t *envptr;} *framerd_leveldb;

typedef struct FD_LEVELDB {
  FD_CONS_HEADER;
  struct FRAMERD_LEVELDB leveldb;} *fd_leveldb;

FD_EXPORT fd_ptr_type fd_leveldb_type;

FD_EXPORT int fd_init_leveldb(void) FD_LIBINIT_FN;

typedef struct FD_LEVELDB_POOL {
  FD_POOL_FIELDS;
  unsigned int pool_load; time_t pool_mtime;
  unsigned int locked:1;
  lispval *pool_slots; ssize_t n_pool_slots;
  struct FD_HASHTABLE slot_table;
  struct FD_SCHEMA_ENTRY *pool_schemas;
  struct FD_SCHEMA_LOOKUP *pool_schbyval;
  struct FRAMERD_LEVELDB leveldb;} FD_LEVELDB_POOL;
typedef struct FD_LEVELDB_POOL *fd_leveldb_pool;

