typedef struct FD_MONGODB {
  FD_CONS_HEADER;
  u8_mutex lock;
  mongo conn; u8_string default_collection;
  u8_string spec;} FD_MONGODB;
typedef struct FD_MONGODB *fd_mongodb;

FD_EXPORT fd_ptr_type fd_mongodb_type;

FD_EXPORT fdtype fd_mongodb_open(fdtype specs,fdtype opts);
FD_EXPORT fdtype fd_mongodb_close(fdtype);

/*
  BSON -> DTYPE mapping

  strings, packets, ints, doubles, true, false, symbols,
  timestamp, uuid (direct),
  slotmaps are objects (unparse-arg/parse-arg),
  BSON arrays are DTYPE choices, BSON_NULL is empty choice,
  DTYPE vectors are converted into arrays of arrays,
  other types are also objects, with a _kind attribute,
  including bignums, rational and complex numbers, quoted
  choices, etc.
  
 */

FD_EXPORT fdtype fd_dtype2bson(fdtype,bson *);
FD_EXPORT fdtype fd_bson2dtype(bson *);

typedef struct FD_MONGO_POOL {
  FD_POOL_FIELDS;
  u8_mutex lock;
  mongo conn;
  mongo_client_t *conn;
  mongo_collection_t *collection;} FD_MONGO_POOL;
typedef struct FD_MONGO_POOL *fd_mongo_pool;

typedef struct FD_MONGO_INDEX {
  FD_INDEX_FIELDS;
  u8_mutex lock;
  mongo_client_t *conn;
  mongo_collection_t *collection;} FD_MONGO_INDEX;
typedef struct FD_MONGO_INDEX *fd_mongo_index;

