#include <sqlite.h>

typedef struct KNO_SQLITE {
  KNO_CONS_HEADER; sqlite *db;} KNO_SQLITE;
typedef struct KNO_SQLITE *kno_sqlite;

KNO_EXPORT kno_lisp_type kno_sqlite_type;

