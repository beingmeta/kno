#include <sqlite.h>

typedef struct FD_SQLITE {
  FD_CONS_HEADER; sqlite *db;} FD_SQLITE;
typedef struct FD_SQLITE *fd_sqlite;

FD_EXPORT fd_ptr_type fd_sqlite_type;

