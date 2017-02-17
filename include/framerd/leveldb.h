typedef struct FD_LEVELDB {
  FD_CONS_HEADER;
  u8_string ldb_path;
  fdtype ldb_options;
  int ldb_closed;
  leveldb_options_t *cldb_options;
  leveldb_env_t *cldb_env;
  leveldb_cache_t *cldb_cache;
  leveldb_t *cldb_ptr;} *fd_leveldb;

FD_EXPORT fd_ptr_type fd_leveldb_type;
FD_EXPORT int fd_init_leveldb(void) FD_LIBINIT_FN;
