typedef struct KNO_LEVELDB {
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
  struct leveldb_env_t *envptr;}  *kno_leveldb;

typedef struct KNO_LEVELDB_CONS {
  KNO_CONS_HEADER;
  struct KNO_LEVELDB leveldb;} *kno_leveldb_cons;

KNO_EXPORT kno_lisp_type kno_leveldb_type;

KNO_EXPORT int kno_init_leveldb(void) KNO_LIBINIT_FN;

typedef struct KNO_LEVELDB_POOL {
  KNO_POOL_FIELDS;
  unsigned int pool_load; time_t pool_mtime;
  unsigned int locked:1;
  struct KNO_SLOTCODER slotcodes;
  struct KNO_LEVELDB leveldb;} KNO_LEVELDB_POOL;
typedef struct KNO_LEVELDB_POOL *kno_leveldb_pool;

typedef struct KNO_LEVELDB_INDEX {
  KNO_INDEX_FIELDS;
  unsigned int locked:1;
  
  struct KNO_SLOTCODER slotcodes;
  struct KNO_OIDCODER oidcodes;

  struct KNO_HASHTABLE slotids_table;
  struct KNO_LEVELDB leveldb;} KNO_LEVELDB_INDEX;
typedef struct KNO_LEVELDB_INDEX *kno_leveldb_index;

