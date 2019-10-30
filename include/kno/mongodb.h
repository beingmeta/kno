#define MONGOC_ENABLE_SSL 1

#include <bson.h>
#include <mongoc.h>

#if (!(MONGOC_CHECK_VERSION(1,15,0)))
#define mongoc_uri_get_tls mongoc_uri_get_ssl
#endif

#include "storage.h"

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

#define KNO_MONGODB_SLOTIFY_IN 1
#define KNO_MONGODB_SLOTIFY_OUT 2
#define KNO_MONGODB_SLOTIFY (KNO_MONGODB_SLOTIFY_IN|KNO_MONGODB_SLOTIFY_OUT)
#define KNO_MONGODB_COLONIZE_IN 4
#define KNO_MONGODB_COLONIZE_OUT 8
#define KNO_MONGODB_COLONIZE (KNO_MONGODB_COLONIZE_IN|KNO_MONGODB_COLONIZE_OUT)
#define KNO_MONGODB_CHOICEVALS 16
#define KNO_MONGODB_NOBLOCK 32
#define KNO_MONGODB_LOGOPS 64
#define KNO_MONGODB_DEFAULTS \
  (KNO_MONGODB_CHOICEVALS|KNO_MONGODB_COLONIZE|KNO_MONGODB_SLOTIFY)
#define KNO_MONGODB_VECSLOT  128
#define KNO_MONGODB_SYMSLOT  256
#define KNO_MONGODB_RAWSLOT  512

KNO_EXPORT u8_condition kno_MongoDB_Error, kno_MongoDB_Warning;
KNO_EXPORT kno_lisp_type kno_mongoc_server, kno_mongoc_collection, kno_mongoc_cursor;

typedef struct KNO_BSON_OUTPUT {
  bson_t *bson_doc;
  lispval bson_opts, bson_fieldmap;
  int bson_flags;} KNO_BSON_OUTPUT;
typedef struct KNO_BSON_INPUT {
  bson_iter_t *bson_iter;
  lispval bson_opts, bson_fieldmap;
  int bson_flags;} KNO_BSON_INPUT;
typedef struct KNO_BSON_INPUT *kno_bson_input;

typedef struct KNO_MONGODB_DATABASE {
  KNO_CONS_HEADER;
  u8_string dburi, dbname, dbspec;
  lispval dbopts;
  int dbflags;
  mongoc_client_pool_t *dbclients;
  mongoc_uri_t *dburi_info;} KNO_MONGODB_DATABASE;
typedef struct KNO_MONGODB_DATABASE *kno_mongodb_database;

typedef struct KNO_MONGODB_COLLECTION {
  KNO_CONS_HEADER;
  u8_string collection_name;
  lispval domain_db;
  lispval domain_opts;
  int domain_flags;}
  KNO_MONGODB_COLLECTION;
typedef struct KNO_MONGODB_COLLECTION *kno_mongodb_collection;

typedef struct KNO_MONGODB_CURSOR {
  KNO_CONS_HEADER;
  lispval cursor_db, cursor_domain, cursor_query;
  lispval cursor_opts;
  int cursor_flags, cursor_done;
  mongoc_client_t *cursor_connection;
  mongoc_collection_t *cursor_collection;
  bson_t *cursor_query_bson;
  bson_t *cursor_opts_bson;
  const bson_t *cursor_value_bson;
  mongoc_read_prefs_t *cursor_readprefs;
  mongoc_cursor_t *mongoc_cursor;}
  KNO_MONGODB_CURSOR;
typedef struct KNO_MONGODB_CURSOR *kno_mongodb_cursor;

KNO_EXPORT lispval kno_bson_write(bson_t *out,int flags,lispval in);
KNO_EXPORT bson_t *kno_lisp2bson(lispval,int,lispval);
KNO_EXPORT lispval kno_bson2dtype(bson_t *,int,lispval);
KNO_EXPORT lispval kno_bson_output(struct KNO_BSON_OUTPUT,lispval);
KNO_EXPORT int kno_init_mongodb(void);

#define DOMAIN2DB(dom) \
  ((struct KNO_MONGODB_DATABASE *) ((dom)->domain_db))
#define CURSOR2DOMAIN(cursor) \
  (struct KNO_MONGODB_COLLECTION *) ((cursor)->cursor_domain);
#define CURSOR2COLLECTION(cursor) ((cursor)->cursor_collection)
