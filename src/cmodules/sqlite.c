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
#include "fdb/numbers.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"
#include <sqlite3.h>

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8digestfns.h>

FD_EXPORT int fd_init_sqlite(void) FD_LIBINIT_FN;

typedef struct FD_SQLITE {
  FD_CONS_HEADER; u8_string cid; sqlite3 *db;} FD_SQLITE;
typedef struct FD_SQLITE *fd_sqlite;

fd_ptr_type fd_sqlite_type;

static fd_exception SQLiteError=_("SQLite Error");

static fdtype intern_upcase(u8_output out,u8_string s)
{
  u8_byte *scan=s; int c=u8_sgetc(&s);
  out->u8_outptr=out->u8_outbuf;
  while (c>=0) {
    u8_putc(out,u8_toupper(c));
    c=u8_sgetc(&s);}
  return fd_make_symbol(out->u8_outbuf,out->u8_outptr-out->u8_outbuf);
}

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
    sqlcons->db=db; sqlcons->cid=u8_strdup(FD_STRDATA(filename));
    return FDTYPE_CONS(sqlcons);}
}

static void recycle_sqlitedb(struct FD_CONS *c)
{
  struct FD_SQLITE *dbp=(struct FD_SQLITE *)c;
  u8_free(dbp->cid); sqlite3_close(dbp->db);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static int unparse_sqlitedb(u8_output out,fdtype x)
{
  struct FD_SQLITE *dbp=FD_GET_CONS(x,fd_sqlite_type,struct FD_SQLITE *);
  u8_printf(out,"#<SQLITE %s>",dbp->cid);
  return 1;
}

static int sqlite_basic_callback(void *rptr,int n_cols,char **vals,unsigned char **cols)
{
  fdtype *results=(fdtype *)rptr, result;
  struct FD_KEYVAL *slotvals=u8_alloc_n(n_cols,struct FD_KEYVAL);
  struct U8_OUTPUT out;
  int i=0;
  U8_INIT_OUTPUT(&out,64);
  while (i<n_cols) {
    slotvals[i].key=intern_upcase(&out,cols[i]); 
    slotvals[i].value=fdtype_string(vals[i]);
    i++;}
  result=fd_init_slotmap(NULL,n_cols*2,slotvals);
  FD_ADD_TO_CHOICE(*results,result);
  u8_free(out.u8_outbuf);
  return 0;
}

static fdtype sqlite_values(sqlite3 *db,sqlite3_stmt *stmt,fdtype typeinfo)
{
  fdtype results=FD_EMPTY_CHOICE;
  fdtype _colnames[16], *colnames;
  fdtype _colmaps[16], *colmaps;
  int i=0, n_cols=sqlite3_column_count(stmt), retval;
  struct U8_OUTPUT out;
  if (n_cols==0) return FD_EMPTY_CHOICE;
  if (n_cols>16) {
    colnames=u8_alloc_n(n_cols,fdtype);
    colmaps=u8_alloc_n(n_cols,fdtype);}
  else {
    colnames=_colnames;
    colmaps=_colmaps;}
  U8_INIT_OUTPUT(&out,64);
  while (i<n_cols) {
    fdtype colname;
    colnames[i]=colname=intern_upcase(&out,(u8_string)sqlite3_column_name(stmt,i));
    colmaps[i]=((FD_VOIDP(typeinfo)) ? (FD_VOID) : (fd_get(typeinfo,colname,FD_VOID)));
    i++;}
  while (sqlite3_step(stmt)==SQLITE_ROW) {
    fdtype slotmap;
    struct FD_KEYVAL *kv=u8_alloc_n(n_cols,struct FD_KEYVAL);
    int j=0; while (j<n_cols) {
      int coltype=sqlite3_column_type(stmt,j); fdtype value;
      if (coltype<0) {
	int k=0; while (k<j) {fd_decref(kv[k].value);  k++;}
	u8_free(kv);
	fd_decref(results); u8_free(out.u8_outbuf);
	return FD_ERROR_VALUE;}
      kv[j].key=colnames[j];
      switch (coltype) {
      case SQLITE_INTEGER: {
	long long intval=sqlite3_column_int(stmt,j);
	value=FD_INT2DTYPE(intval); break;}
      case SQLITE_FLOAT:
	value=fd_init_double(NULL,sqlite3_column_double(stmt,j)); break;
      case SQLITE_TEXT:
	value=fdtype_string((unsigned char *)sqlite3_column_text(stmt,j)); break;
      case SQLITE_BLOB: {
	int n_bytes=sqlite3_column_bytes(stmt,j);
	const unsigned char *blob=sqlite3_column_blob(stmt,j);
	unsigned char *bytes=u8_alloc_n(n_bytes,unsigned char);
	memcpy(bytes,blob,n_bytes);
	value=fd_init_packet(NULL,n_bytes,bytes); break;}
      case SQLITE_NULL: default:
	value=FD_EMPTY_CHOICE; break;}
      if (FD_VOIDP(colmaps[j]))
	kv[j].value=value;
      else if (FD_APPLICABLEP(colmaps[j])) {
	kv[j].value=fd_apply(colmaps[j],1,&value);
	fd_decref(value);}
      else if (FD_OIDP(colmaps[j]))
	if (FD_STRINGP(value)) {
	  kv[j].value=fd_parse(FD_STRDATA(value));
	  fd_decref(value);}
	else {
	  FD_OID base=FD_OID_ADDR(colmaps[j]);
	  unsigned int offset=fd_getint(value);
	  if (offset<0) kv[j].value=value;
	  else {
	    kv[j].value=fd_make_oid(base+offset);
	    fd_decref(value);}}
      else if (colmaps[j]==FD_TRUE)
	if (FD_STRINGP(value)) {
	  kv[j].value=fd_parse(FD_STRDATA(value));
	  fd_decref(value);}
	else kv[j].value=value;
      else kv[j].value=value;
      j++;}
    slotmap=fd_init_slotmap(NULL,n_cols,kv);
    FD_ADD_TO_CHOICE(results,slotmap);}
  u8_free(out.u8_outbuf);
  return results;
}

static fdtype sqliteexec(fdtype db,fdtype string,fdtype typeinfo)
{
  sqlite3 *dbp=FD_GET_CONS(db,fd_sqlite_type,struct FD_SQLITE *)->db;
  sqlite3_stmt *stmt;
  fdtype results=FD_EMPTY_CHOICE; const char *errmsg="er, err";
  int retval=
    sqlite3_prepare_v2(dbp,FD_STRDATA(string),FD_STRLEN(string),
		       &stmt,NULL);
  if (retval==SQLITE_OK) {
    fdtype values=sqlite_values(dbp,stmt,typeinfo);
    if (FD_ABORTP(values)) {
      errmsg=sqlite3_errmsg(dbp);
      fd_seterr(SQLiteError,"fdsqlite_call",u8_strdup(errmsg),string);}
    sqlite3_finalize(stmt);
    return values;}
  else {
    errmsg=sqlite3_errmsg(dbp);
    fd_seterr(SQLiteError,"fdsqlite_call",u8_strdup(errmsg),db);
    return FD_ERROR_VALUE;}
}

static int sqlite_init=0;

FD_EXPORT int fd_init_sqlite()
{
  fdtype module;
  if (sqlite_init) return 0;
  module=fd_new_module("SQLITE",0);

  fd_sqlite_type=fd_register_cons_type("SQLITE");
  fd_recyclers[fd_sqlite_type]=recycle_sqlitedb;
  fd_unparsers[fd_sqlite_type]=unparse_sqlitedb;


  fd_defn(module,
	  fd_make_cprim2x("SQLITE/OPEN",open_sqlite,1,
			  fd_string_type,FD_VOID,
			  -1,FD_VOID));
  fd_defn(module,
	  fd_make_cprim3x("SQLITE/EXEC",sqliteexec,2,
			  fd_sqlite_type,FD_VOID,
			  fd_string_type,FD_VOID,
			  -1,FD_VOID));
  sqlite_init=1;

  fd_finish_module(module);

  fd_register_source_file(versionid);

  return 1;
}
