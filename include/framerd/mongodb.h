#include <bson.h>
#include <mongoc.h>
#include "fddb.h"

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
#define FD_MONGODB_NOBLOCK 32
#define FD_MONGODB_LOGOPS 64
#define FD_MONGODB_DEFAULTS \
  (FD_MONGODB_CHOICEVALS|FD_MONGODB_COLONIZE|FD_MONGODB_SLOTIFY)

FD_EXPORT u8_condition fd_MongoDB_Error, fd_MongoDB_Warning;
FD_EXPORT fd_ptr_type fd_mongoc_server, fd_mongoc_collection, fd_mongoc_cursor;

typedef struct FD_BSON_OUTPUT {
  bson_t *bson_doc;
  fdtype bson_opts, bson_fieldmap;
  int bson_flags;} FD_BSON_OUTPUT;
typedef struct FD_BSON_INPUT {
  bson_iter_t *bson_iter;
  fdtype bson_opts, bson_fieldmap;
  int bson_flags;} FD_BSON_INPUT;
typedef struct FD_BSON_INPUT *fd_bson_input;

typedef struct FD_MONGODB_DATABASE {
  FD_CONS_HEADER;
  u8_string dburi, dbname, dbspec;
  fdtype dbopts;
  int dbflags;
  mongoc_client_pool_t *dbclients;
  mongoc_uri_t *dburi_info;} FD_MONGODB_DATABASE;
typedef struct FD_MONGODB_DATABASE *fd_mongodb_database;

typedef struct FD_MONGODB_COLLECTION {
  FD_CONS_HEADER;
  u8_string collection_name;
  fdtype domain_db;
  fdtype domain_opts;
  int domain_flags;}
  FD_MONGODB_COLLECTION;
typedef struct FD_MONGODB_COLLECTION *fd_mongodb_collection;

typedef struct FD_MONGODB_CURSOR {
  FD_CONS_HEADER;
  fdtype cursor_db, cursor_domain, cursor_query;
  fdtype cursor_opts; int cursor_flags;
  mongoc_client_t *cursor_connection;
  mongoc_collection_t *cursor_collection;
  bson_t *cursor_query_bson;
  bson_t *cursor_fields_bson;
  mongoc_read_prefs_t *cursor_readprefs;
  mongoc_cursor_t *mongoc_cursor;}
  FD_MONGODB_CURSOR;
typedef struct FD_MONGODB_CURSOR *fd_mongodb_cursor;

FD_EXPORT fdtype fd_bson_write(bson_t *out,int flags,fdtype in);
FD_EXPORT bson_t *fd_dtype2bson(fdtype,int,fdtype);
FD_EXPORT fdtype fd_bson2dtype(bson_t *,int,fdtype);
FD_EXPORT fdtype fd_bson_output(struct FD_BSON_OUTPUT,fdtype);
FD_EXPORT int fd_init_mongodb(void);

#define DOMAIN2DB(dom) \
  ((struct FD_MONGODB_DATABASE *) ((dom)->domain_db))
#define CURSOR2DOMAIN(cursor) \
  (struct FD_MONGODB_COLLECTION *) (cursor->cursor_collection);
