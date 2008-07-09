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
#include "sqlite.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8digestfns.h>

static fd_exception SQLiteError=_("SQLite Error");

static fdtype open_sqlite(fdtype filename)
{
  char *fname=u8_tolibc(FD_STRDATA(filename)), *errmsg=NULL;
  sqlite *db=sqlite_open(FD_STRDATA(filename),0666,&errmsg);
  if (db) {
    struct FD_SQLITE *sqlcons=u8_alloc(struct FD_SQLITE);
    FD_INIT_CONS(sqlcons,fd_sqlite_type);
    sqlcons->db=db;
    return FDTYPE_CONS(sqlcons);}
  else if (errmsg) {
    u8_string fn=u8_fromlibc(fname); u8_free(fname);
    return fd_err(SQLiteError,"open_sqlite",u8_strdup(errmsg),filename);}
  else {
    u8_string fn=u8_fromlibc(fname); u8_free(fname);
    return fd_err(SQLiteError,"open_sqlite",NULL,filename);}
}

static int fdsqlite_callback(void *rptr,int n_cols,char **vals,char **cols)
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

static fdtype sqlite_call(fdtype db,fdtype string)
{
  sqlite *dbp=FD_GET_CONS(db,fd_sqlite_type,struct FD_SQLITE *)->db;
  fdtype results=FD_EMPTY_CHOICE; char *errmsg;
  int retval=
    sqlite_exec(dbp,FD_STRDATA(string),fdsqlite_callback,
		&results,&errmsg);
  if (retval==SQLITE_OK)
    return results;
  else {
    fd_seterr(sqlite_error_string(retval),"fdsqlite_call",
	      ((errmsg) ? (u8_strdup(errmsg)) : (NULL)),
	      db);
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
	  fd_make_cprim1x("OPEN-SQLITE",open_sqlite,1,fd_string_type,FD_VOID));
  fd_defn(module,
	  fd_make_cprim2x("SQLITE",sqlite_call,2,
			  fd_string_type,FD_VOID,
			  fd_sqlite_type,FD_VOID));
  sqlite_init=1;
  return 1;
}
