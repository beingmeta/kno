typedef struct FD_LEVELDB {
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

typedef struct FD_LEVELDB_CONS {
  FD_CONS_HEADER;
  struct FD_LEVELDB leveldb;} *fd_leveldb;

FD_EXPORT fd_ptr_type fd_leveldb_type;

FD_EXPORT int fd_init_leveldb(void) FD_LIBINIT_FN;

typedef struct FD_LEVELDB_POOL {
  FD_POOL_FIELDS;
  unsigned int pool_load; time_t pool_mtime;
  unsigned int locked:1;
  struct FD_SLOTCODER slotcodes;
  struct FD_LEVELDB leveldb;} FD_LEVELDB_POOL;
typedef struct FD_LEVELDB_POOL *fd_leveldb_pool;

typedef struct FD_LEVELDB_INDEX {
  FD_INDEX_FIELDS;
  unsigned int locked:1;
  
  struct FD_SLOTCODER slotcodes;
  struct FD_OIDCODER oidcodes;

  struct FD_HASHTABLE slotids_table;
  struct FD_LEVELDB leveldb;} FD_LEVELDB_INDEX;
typedef struct FD_LEVELDB_INDEX *fd_leveldb_index;

