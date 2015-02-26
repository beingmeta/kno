#include <bson.h>
#include <mongoc.h>

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

#define FD_MONGODB_SLOTIFY_IN 1
#define FD_MONGODB_SLOTIFY_OUT 2
#define FD_MONGODB_SLOTIFY (FD_MONGODB_SLOTIFY_IN|FD_MONGODB_SLOTIFY_OUT)
#define FD_MONGODB_COLONIZE_IN 4
#define FD_MONGODB_COLONIZE_OUT 8
#define FD_MONGODB_COLONIZE (FD_MONGODB_COLONIZE_IN|FD_MONGODB_COLONIZE_OUT)
#define FD_MONGODB_CHOICEVALS 16
#define FD_MONGODB_DEFAULTS \
  (FD_MONGODB_CHOICEVALS|FD_MONGODB_COLONIZE|FD_MONGODB_SLOTIFY)

FD_EXPORT u8_condition fd_MongoDB_Error, fd_MongoDB_Warning;
FD_EXPORT fd_ptr_type fd_mongo_client, fd_mongo_collection, fd_mongo_cursor;

typedef struct FD_BSON_OUTPUT {
  bson_t *doc; fdtype opts; int flags;} FD_BSON_OUTPUT;
typedef struct FD_BSON_INPUT {
  bson_iter_t *iter; fdtype opts; int flags;} FD_BSON_INPUT;
typedef struct FD_BSON_INPUT *fd_bson_input;

typedef struct FD_MONGODB_CLIENT {
  FD_CONS_HEADER;
  u8_string uri; fdtype opts; int flags;
  mongoc_client_t *client;} FD_MONGODB_CLIENT;
typedef struct FD_MONGODB_CLIENT *fd_mongodb_client;

typedef struct FD_MONGODB_COLLECTION {
  FD_CONS_HEADER;
  fdtype client;
  u8_string uri, dbname, name;
  fdtype opts; int flags, busy; u8_mutex lock;
  mongoc_collection_t *collection;
  mongoc_client_t *cl;} FD_MONGODB_COLLECTION;
typedef struct FD_MONGODB_COLLECTION *fd_mongodb_collection;

typedef struct FD_MONGODB_CURSOR {
  FD_CONS_HEADER;
  fdtype collection, query, opts; int flags;
  mongoc_collection_t *coll; mongoc_client_t *cl;
  bson_t *bsonquery;
  mongoc_cursor_t *cursor;} FD_MONGODB_CURSOR;
typedef struct FD_MONGODB_CURSOR *fd_mongodb_cursor;

FD_EXPORT fdtype fd_bson_write(bson_t *out,int flags,fdtype in);
FD_EXPORT bson_t *fd_dtype2bson(fdtype,int,fdtype);
FD_EXPORT fdtype fd_bson2dtype(bson_t *,int,fdtype);

#if 0
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

#endif
