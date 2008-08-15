/* C Mode */

/* odbc.c
   This implements FramerD bindings to odbc.
   Copyright (C) 2007-2008 beingmeta, inc.
*/

static char versionid[] =
  "$Id: texttools.c 2312 2008-02-23 23:49:10Z haase $";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"
#include <sqlite3.h>

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8digestfns.h>

typedef struct FD_SQLITE {
  FD_CONS_HEADER; sqlite3 *db;} FD_SQLITE;
typedef struct FD_SQLITE *fd_sqlite;

FD_EXPORT fd_ptr_type fd_sqlite_type;

static fd_exception SQLiteError=_("SQLite Error");

static fdtype open_sqlite(fdtype filename,fdtype opts)
{
  sqlite3 *db=NULL;
#if HAVE_SQLITE3_OPEN_V2
  int flags=
    ((FD_VOIDP(opts)) ? (SQLITE_OPEN_READWRITE) :
     (FD_FALSEP(opts)) ? (SQLITE_OPEN_READONLY) :
     ((SQLITE_OPEN_READWRITE)|(SQLITE_OPEN_CREATE)));
#else
  int retval=sqlite3_open(FD_STRDATA(filename),&db);
#endif
  if (retval) {
    u8_string msg=u8_strdup(sqlite3_errmsg(db));
    fd_seterr(SQLiteError,"open_sqlite",msg,filename);
    sqlite3_close(db);
    return FD_ERROR_VALUE;}
  else {
    struct FD_SQLITE *sqlcons=u8_alloc(struct FD_SQLITE);
    FD_INIT_CONS(sqlcons,fd_sqlite_type);
    sqlcons->db=db;
    return FDTYPE_CONS(sqlcons);}
}

static int sqlite_basic_callback(void *rptr,int n_cols,char **vals,char **cols)
{
  fdtype *results=(fdtype *)rptr, result;
  struct FD_KEYVAL *slotvals=u8_alloc_n(n_cols,struct FD_KEYVAL);
  int i=0; while (i<n_cols) {
    slotvals[i].key=fd_intern(cols[i]); 
    slotvals[i].value=fdtype_string(vals[i]);
    i++;}
  result=fd_init_slotmap(NULL,n_cols*2,slotvals);
  FD_ADD_TO_CHOICE(*results,result);
  return 0;
}

static fdtype call_sqlite(fdtype db,fdtype string)
{
  sqlite3 *dbp=FD_GET_CONS(db,fd_sqlite_type,struct FD_SQLITE *)->db;
  fdtype results=FD_EMPTY_CHOICE; char *errmsg;
  int retval=
    sqlite3_exec(dbp,FD_STRDATA(string),sqlite_basic_callback,
		 &results,&errmsg);
  if (retval==SQLITE_OK)
    return results;
  else {
    fd_seterr(SQLiteError,"fdsqlite_call",
	      ((errmsg) ? (u8_fromlibc(errmsg)) : (NULL)),
	      db);
    u8_free(errmsg);
    return FD_ERROR_VALUE;}
}

static int sqlite_init=0;

FD_EXPORT int fd_init_sqlite()
{
  fdtype module;
  if (sqlite_init) return 0;
  module=fd_new_module("SQLITE",0);
  fd_sqlite_type=fd_register_cons_type("SQLITE");
  fd_defn(module,
	  fd_make_cprim2x("SQLITECONN",open_sqlite,1,
			  fd_string_type,FD_VOID,
			  -1,FD_VOID));
  fd_defn(module,
	  fd_make_cprim2x("SQLITECALL",call_sqlite,2,
			  fd_string_type,FD_VOID,
			  fd_sqlite_type,FD_VOID));
  sqlite_init=1;
  return 1;
}
