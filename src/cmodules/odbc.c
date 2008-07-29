/* C Mode */

/* odbc.c
   This implements FramerD bindings to odbc.
   Copyright (C) 2007-2008 beingmeta, inc.
*/

static char versionid[] =
  "$Id: texttools.c 2312 2008-02-23 23:49:10Z haase $";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/numbers.h"
#include "fdb/eval.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8digestfns.h>

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

fd_ptr_type fd_odbc_type;
FD_EXPORT void fd_init_odbc(void) FD_LIBINIT_FN;

static fd_exception ODBCError=_("ODBC error");

static u8_string odbc_errstring(SQLHANDLE handle,SQLSMALLINT type)
{
  struct U8_OUTPUT out;
  SQLINTEGER i = 0;
  SQLINTEGER native;
  SQLCHAR state[ 7 ];
  SQLCHAR text[256];
  SQLSMALLINT len;
  SQLRETURN ret=SQLGetDiagRec
    (type, handle, ++i, state, &native, text,sizeof(text),&len);
  U8_INIT_OUTPUT(&out,128);
  while (SQL_SUCCEEDED(ret)) {
    u8_printf(&out,"%s:%ld:%ld:%s\n", state, i, native, text);
    ret = SQLGetDiagRec
      (type, handle, ++i, state, &native, text,sizeof(text),&len);}
  return out.u8_outbuf;
}

typedef struct FD_ODBC {
  FD_CONS_HEADER;
  SQLHENV env;
  SQLHDBC conn;
  u8_string cid;} FD_ODBC;

static SQLHWND sqldialog=0;
static int interactive_dflt=0;

fd_ptr_type fd_odbc_type;

FD_EXPORT fdtype fd_odbc_connect(fdtype spec,int interactive)
{
  struct FD_ODBC *dbp=u8_alloc(struct FD_ODBC);
  int ret=-1, howfar=0;
  if (interactive<0) interactive=interactive_dflt;
  FD_INIT_FRESH_CONS(dbp,fd_odbc_type);
  ret=SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE,&(dbp->env));
  if (SQL_SUCCEEDED(ret)) {
    howfar++;
    SQLSetEnvAttr(dbp->env, SQL_ATTR_ODBC_VERSION,
		  (void *) SQL_OV_ODBC3, 0);
    ret=SQLAllocHandle(SQL_HANDLE_DBC,dbp->env,&(dbp->conn));
    dbp->cid=u8_malloc(512); strcpy(dbp->cid,"uninitialized");
    if (SQL_SUCCEEDED(ret)) {
      howfar++;
      ret=SQLDriverConnect(dbp->conn,sqldialog,
			   FD_STRDATA(spec),FD_STRLEN(spec),
			   dbp->cid,512,NULL,
			   ((interactive==0) ? (SQL_DRIVER_NOPROMPT) :
			    (interactive==1) ? (SQL_DRIVER_COMPLETE_REQUIRED) :
			    (SQL_DRIVER_PROMPT)));
      if (SQL_SUCCEEDED(ret)) {
	return FDTYPE_CONS(dbp);}}}
  if (howfar>1)
    u8_seterr(ODBCError,"fd_odbc_connect",
	      odbc_errstring(dbp->conn,SQL_HANDLE_DBC));
  else if (howfar)
    u8_seterr(ODBCError,"fd_odbc_connect",
	      odbc_errstring(dbp->env,SQL_HANDLE_ENV));
  if (howfar>1) {
    SQLFreeHandle(SQL_HANDLE_DBC,dbp->conn);
    u8_free(dbp->cid);}
  if (howfar) SQLFreeHandle(SQL_HANDLE_ENV,dbp->env);
  u8_free(dbp);
  return FD_ERROR_VALUE;
}

static void recycle_odbconn(struct FD_CONS *c)
{
  struct FD_ODBC *dbp=(struct FD_ODBC *)c;
  SQLFreeHandle(SQL_HANDLE_DBC,dbp->conn);
  SQLFreeHandle(SQL_HANDLE_ENV,dbp->env);
  u8_free(dbp->cid);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static int unparse_odbconn(u8_output out,fdtype x)
{
  struct FD_ODBC *dbp=FD_GET_CONS(x,fd_odbc_type,struct FD_ODBC *);
  u8_printf(out,"#<ODBC %s>",dbp->cid);
  return 1;
}

static fdtype odbconnp(fdtype arg)
{
  if (FD_PRIM_TYPEP(arg,fd_odbc_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype odbconnect(fdtype spec)
{
  return fd_odbc_connect(spec,-1);
}

/* Getting attributes from connections */

static fdtype odbcstringattr(struct FD_ODBC *dbp,int attrid)
{
  u8_byte _buf[64], *buf=_buf; int ret; SQLSMALLINT len;
  ret=SQLGetInfo(dbp->conn,attrid,(SQLPOINTER)buf,sizeof(_buf),&len);
  if (SQL_SUCCEEDED(ret)) {
    if (len<64)
      return fd_init_string(NULL,len,u8_strdup(buf));
    len++; buf=u8_malloc(len);
    ret=SQLGetInfo(dbp->conn,attrid,(SQLPOINTER)&buf,len,NULL);
    if (SQL_SUCCEEDED(ret))
      return fd_init_string(NULL,len,buf);}
  return FD_ERROR_VALUE;
}

static fdtype odbcintattr(struct FD_ODBC *dbp,int attrid)
{
  int len=-1, ret, attrval=0;
  ret=SQLGetInfo(dbp->conn,attrid,(SQLPOINTER)&attrval,0,NULL);
  if (SQL_SUCCEEDED(ret))
    return FD_INT2DTYPE(attrval);
  else return FD_ERROR_VALUE;
}

static fdtype odbcattr(fdtype conn,fdtype attr)
{
  struct FD_ODBC *dbp=FD_GET_CONS(conn,fd_odbc_type,struct FD_ODBC *);
  char *attr_name=FD_SYMBOL_NAME(attr);
  if (strcmp(attr_name,"DBMS")==0)
    return odbcstringattr(dbp,SQL_DBMS_NAME);
  else if (strcmp(attr_name,"VERSION")==0)
    return odbcstringattr(dbp,SQL_DBMS_VER);
  else if (strcmp(attr_name,"GETDATAEXT")==0)
    return odbcstringattr(dbp,SQL_GETDATA_EXTENSIONS);
  else if (strcmp(attr_name,"MAXCONCURRENT")==0)
    return odbcstringattr(dbp,SQL_MAX_CONCURRENT_ACTIVITIES);
  else return FD_FALSE;
}

/* Execution */

static fdtype get_colvalue(SQLHSTMT stmt,int i,int sqltype,int colsize)
{
  switch (sqltype) {
  case SQL_CHAR: case SQL_VARCHAR: {
    u8_byte *data=u8_malloc(colsize+1);
    int ret=SQLGetData(stmt,i+1,SQL_C_CHAR,data,colsize+1,NULL);
    return fd_init_string(NULL,colsize,data);}
  case SQL_INTEGER: case SQL_SMALLINT: {
    long intval;
    int ret=SQLGetData(stmt,i+1,SQL_C_LONG,&intval,0,NULL);
    return FD_INT2DTYPE(intval);}
  case SQL_FLOAT: case SQL_DOUBLE: {
    double dblval;
    int ret=SQLGetData(stmt,i+1,SQL_C_DOUBLE,&dblval,0,NULL);
    return fd_init_double(NULL,dblval);}
  default:
    return FD_VOID;}
}


static fdtype odbc_exec(fdtype conn,fdtype string)
{
  fdtype results=FD_VOID; int ret, i; SQLSMALLINT n_cols;
  struct FD_ODBC *dbp=FD_GET_CONS(conn,fd_odbc_type,struct FD_ODBC *);
  fdtype *colnames; SQLSMALLINT *coltypes; SQLULEN *colsizes;
  SQLHSTMT stmt;
  SQLAllocHandle(SQL_HANDLE_STMT,dbp->conn,&stmt);
  ret=SQLExecDirect(stmt,FD_STRDATA(string),FD_STRLEN(string));
  if (!(SQL_SUCCEEDED(ret))) {}
  ret=SQLNumResultCols(stmt, &n_cols);
  if (!(SQL_SUCCEEDED(ret))) {}
  if (n_cols==0) {
    SQLFreeHandle(SQL_HANDLE_STMT,stmt);
    return FD_VOID;}
  else results=FD_EMPTY_CHOICE;
  colnames=u8_alloc_n(n_cols,fdtype);
  coltypes=u8_alloc_n(n_cols,SQLSMALLINT);
  colsizes=u8_alloc_n(n_cols,SQLULEN);
  i=0; while (i<n_cols) {
    SQLCHAR name[300];
    SQLSMALLINT sqltype;
    SQLULEN colsize;
    SQLSMALLINT sqldigits;
    SQLSMALLINT nullok;
    ret=SQLDescribeCol(stmt,i+1,
		       name,sizeof(name),NULL,
		       &sqltype,&colsize,&sqldigits,&nullok);
    colnames[i]=fd_intern(name);
    coltypes[i]=sqltype;
    colsizes[i]=colsize;
    i++;}
  while (SQL_SUCCEEDED(ret=SQLFetch(stmt))) {
    fdtype slotmap;
    struct FD_KEYVAL *kv=u8_alloc_n(n_cols,struct FD_KEYVAL);
    i=0; while (i<n_cols) {
      kv[i].key=colnames[i];
      kv[i].value=get_colvalue(stmt,i,coltypes[i],colsizes[i]);
      i++;}
    slotmap=fd_init_slotmap(NULL,n_cols,kv);
    FD_ADD_TO_CHOICE(results,slotmap);}
  SQLFreeHandle(SQL_HANDLE_STMT,stmt);
  return results;
}


/* Initialization */

static int odbc_initialized=0;

FD_EXPORT void fd_init_odbc()
{
  fdtype module;
  if (odbc_initialized) return;
  odbc_initialized=1;
  fd_init_fdscheme();

  module=fd_new_module("ODBC",(0));

  fd_odbc_type=fd_register_cons_type("ODBCONN");
  fd_recyclers[fd_odbc_type]=recycle_odbconn;
  fd_unparsers[fd_odbc_type]=unparse_odbconn;

  fd_idefn(module,fd_make_cprim1("ODBCONN?",odbconnp,1));
  fd_idefn(module,fd_make_cprim1("ODBCONNECT",odbconnect,1));
  fd_idefn(module,fd_make_cprim2x
	   ("ODBCATTR",odbcattr,2,
	    fd_odbc_type,FD_VOID,fd_symbol_type,FD_VOID));

  fd_finish_module(module);

  fd_register_source_file(versionid);
}
