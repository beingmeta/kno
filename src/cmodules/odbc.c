/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* odbc.c
   This implements FramerD bindings to odbc.
   Copyright (C) 2007-2019 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"

#include "framerd/extdb.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

static unsigned char *_memdup(const unsigned char *data,int len)
{
  unsigned char *duplicate = u8_alloc_n(len,unsigned char);
  memcpy(duplicate,data,len);
  return duplicate;
}

/* Declarations */

/* As I understand it, ODBC connections are threadsafe
   but statements are no, so we put the lock on statements.
   This is distinct from SQLITE, where connections aren't threadsafe,
   so the lock is on connections. */

static struct FD_EXTDB_HANDLER odbc_handler;

typedef struct FD_ODBC {
  FD_EXTDB_FIELDS;
  SQLHENV env;
  SQLHDBC conn;} FD_ODBC;
typedef struct FD_ODBC *fd_odbc;

typedef struct FD_ODBC_PROC {
  FD_EXTDB_PROC_FIELDS;

  u8_mutex odbc_proc_lock;

  SQLSMALLINT *sqltypes;
  SQLHSTMT stmt;} FD_ODBC_PROC;
typedef struct FD_ODBC_PROC *fd_odbc_proc;

FD_EXPORT int fd_init_odbc(void) FD_LIBINIT_FN;

static u8_condition ODBCError=_("ODBC error");

static u8_string odbc_errstring(SQLHANDLE handle,SQLSMALLINT type)
{
  struct U8_OUTPUT out;
  SQLINTEGER i = 0;
  SQLINTEGER native;
  SQLCHAR state[ 7 ];
  SQLCHAR text[256];
  SQLSMALLINT len;
  SQLRETURN ret = SQLGetDiagRec
    (type, handle, ++i, state, &native, text,sizeof(text),&len);
  U8_INIT_OUTPUT(&out,128);
  while (SQL_SUCCEEDED(ret)) {
    u8_printf(&out,"%s:%ld:%ld:%s\n", state, i, native, text);
    ret = SQLGetDiagRec
      (type, handle, ++i, state, &native, text,sizeof(text),&len);}
  return out.u8_outbuf;
}

static SQLHWND sqldialog = 0;
static int interactive_dflt = 0;

FD_EXPORT lispval fd_odbc_connect(lispval spec,lispval colinfo,int interactive)
{
  struct FD_ODBC *dbp = u8_alloc(struct FD_ODBC);
  int ret = -1, howfar = 0;
  if (interactive<0) interactive = interactive_dflt;
  FD_INIT_FRESH_CONS(dbp,fd_extdb_type);
  ret = SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE,&(dbp->env));
  if (SQL_SUCCEEDED(ret)) {
    char *info;
    howfar++;
    SQLSetEnvAttr(dbp->env, SQL_ATTR_ODBC_VERSION,
                  (void *) SQL_OV_ODBC3, 0);
    ret = SQLAllocHandle(SQL_HANDLE_DBC,dbp->env,&(dbp->conn));
    dbp->extdb_spec = u8_strdup(FD_CSTRING(spec)); dbp->extdb_options = FD_VOID;
    info = u8_malloc(512); strcpy(info,"uninitialized"); dbp->extdb_info = info;
    u8_init_mutex(&(dbp->extdb_proclock));
    dbp->extdb_handler = &odbc_handler;
    if (SQL_SUCCEEDED(ret)) {
      howfar++;
      ret = SQLDriverConnect(dbp->conn,sqldialog,
                           (char *)FD_CSTRING(spec),FD_STRLEN(spec),
                           info,512,NULL,
                           ((interactive==0) ? (SQL_DRIVER_NOPROMPT) :
                            (interactive==1) ? (SQL_DRIVER_COMPLETE_REQUIRED) :
                            (SQL_DRIVER_PROMPT)));
      if (SQL_SUCCEEDED(ret)) {
        dbp->extdb_colinfo = colinfo; fd_incref(colinfo);
        return LISP_CONS(dbp);}}}
  if (howfar>1)
    u8_seterr(ODBCError,"fd_odbc_connect",
              odbc_errstring(dbp->conn,SQL_HANDLE_DBC));
  else if (howfar)
    u8_seterr(ODBCError,"fd_odbc_connect",
              odbc_errstring(dbp->env,SQL_HANDLE_ENV));
  if (howfar>1) {
    SQLFreeHandle(SQL_HANDLE_DBC,dbp->conn);
    u8_free(dbp->extdb_info); u8_free(dbp->extdb_spec);}
  if (howfar) SQLFreeHandle(SQL_HANDLE_ENV,dbp->env);
  u8_free(dbp);
  return FD_ERROR_VALUE;
}

static void recycle_odbconn(struct FD_EXTDB *c)
{
  struct FD_ODBC *dbp = (struct FD_ODBC *)c;
  SQLFreeHandle(SQL_HANDLE_DBC,dbp->conn);
  SQLFreeHandle(SQL_HANDLE_ENV,dbp->env);
  u8_free(dbp->extdb_info); u8_free(dbp->extdb_spec);
}

static lispval odbcopen(lispval spec,lispval colinfo)
{
  return fd_odbc_connect(spec,colinfo,-1);
}

/* ODBCProcs */

static lispval callodbcproc(struct FD_FUNCTION *fn,int n,lispval *args);

static lispval odbcmakeproc
  (struct FD_ODBC *dbp,
   u8_string stmt,int stmt_len,
   lispval colinfo,int n,lispval *args)
{
  int ret = 0, have_stmt = 0;
  SQLSMALLINT n_params, *sqltypes;
  struct FD_ODBC_PROC *dbproc = u8_alloc(struct FD_ODBC_PROC);
  FD_INIT_FRESH_CONS(dbproc,fd_extdb_proc_type);
  ret = SQLAllocHandle(SQL_HANDLE_STMT, dbp->conn, &(dbproc->stmt));
  if (SQL_SUCCEEDED(ret)) have_stmt = 1;
  if (SQL_SUCCEEDED(ret))
    ret = SQLPrepare(dbproc->stmt,(char *)stmt,stmt_len);
  if (SQL_SUCCEEDED(ret))
    ret = SQLNumParams(dbproc->stmt,&n_params);
  if (!(SQL_SUCCEEDED(ret))) {
    if (have_stmt) {
      u8_seterr(ODBCError,"odbcmakeproc",
                odbc_errstring(dbproc->stmt,SQL_HANDLE_STMT));
      SQLFreeHandle(SQL_HANDLE_STMT,dbproc->stmt);
      u8_free(dbproc);}
    else {
      u8_seterr(ODBCError,"odbcmakeproc",
                odbc_errstring(dbp->conn,SQL_HANDLE_DBC));
      u8_free(dbproc);}
    return FD_ERROR_VALUE;}
  dbproc->extdbptr = (lispval)dbp; fd_incref(dbproc->extdbptr);
  dbproc->extdb_handler = &odbc_handler;
  dbproc->fcn_ndcall = 0;
  dbproc->fcn_xcall = 1;
  dbproc->fcn_arity = -1;
  dbproc->fcn_min_arity = dbproc->fcn_n_params = n_params;
  dbproc->fcn_name = dbproc->extdb_qtext=_memdup(stmt,stmt_len+1);
  dbproc->fcn_filename = dbproc->extdb_spec = u8_strdup(dbp->extdb_spec);
  dbproc->sqltypes = sqltypes = u8_alloc_n(n_params,SQLSMALLINT);
  dbproc->fcn_handler.xcalln = callodbcproc;

  u8_init_mutex(&(dbproc->odbc_proc_lock));

  if (FD_VOIDP(colinfo))
    dbproc->extdb_colinfo = fd_incref(dbp->extdb_colinfo);
  else if (FD_VOIDP(dbp->extdb_colinfo))
    dbproc->extdb_colinfo = fd_incref(colinfo);
  else {
    fd_incref(colinfo); fd_incref(dbp->extdb_colinfo);
    dbproc->extdb_colinfo = fd_conspair(colinfo,dbp->extdb_colinfo);}
  {
    int i = 0; lispval *specs = u8_alloc_n(n_params,lispval);
    while (i<n_params)
      if (i<n) {specs[i]=fd_incref(args[i]); i++;}
      else {specs[i]=FD_VOID; i++;}
    dbproc->extdb_paramtypes = specs;}
  {
    int i = 0; while (i<n_params) {
      SQLDescribeParam((dbproc->stmt),i+1,&(sqltypes[i]),NULL,NULL,NULL);
      i++;}}
  fd_register_extdb_proc((struct FD_EXTDB_PROC *) dbproc);
  return LISP_CONS(dbproc);
}

static lispval odbcmakeprochandler
  (struct FD_EXTDB *extdb,
   u8_string stmt,int stmt_len,
   lispval colinfo,int n,lispval *ptypes)
{
  if (extdb->extdb_handler== &odbc_handler)
    return odbcmakeproc((fd_odbc)extdb,stmt,stmt_len,colinfo,n,ptypes);
  else return fd_type_error("ODBC EXTDB","odbcmakeprochandler",(lispval)extdb);
}

static void recycle_odbcproc(struct FD_EXTDB_PROC *c)
{
  struct FD_ODBC_PROC *dbproc = (struct FD_ODBC_PROC *)c;
  fd_release_extdb_proc((struct FD_EXTDB_PROC *) dbproc);
  SQLFreeHandle(SQL_HANDLE_STMT,dbproc->stmt);
  fd_decref(dbproc->extdb_colinfo);
  u8_free(dbproc->extdb_spec); u8_free(dbproc->extdb_qtext);
  u8_free(dbproc->sqltypes); fd_decref(dbproc->extdbptr);
}

/* Getting attributes from connections */

#if 0
static lispval odbcstringattr(struct FD_ODBC *dbp,int attrid)
{
  u8_byte _buf[64], *buf=_buf; int ret; SQLSMALLINT len;
  ret = SQLGetInfo(dbp->conn,attrid,(SQLPOINTER)buf,sizeof(_buf),&len);
  if (SQL_SUCCEEDED(ret)) {
    if (len<64)
      return fd_init_string(NULL,len,u8_strdup(buf));
    len++; buf = u8_malloc(len);
    ret = SQLGetInfo(dbp->conn,attrid,(SQLPOINTER)&buf,len,NULL);
    if (SQL_SUCCEEDED(ret))
      return fd_init_string(NULL,len,buf);}
  return FD_ERROR_VALUE;
}

static lispval odbcintattr(struct FD_ODBC *dbp,int attrid)
{
  int ret, attrval = 0;
  ret = SQLGetInfo(dbp->conn,attrid,(SQLPOINTER)&attrval,0,NULL);
  if (SQL_SUCCEEDED(ret))
    return FD_INT(attrval);
  else return FD_ERROR_VALUE;
}

static lispval odbcattr(lispval conn,lispval attr)
{
  struct FD_ODBC *dbp = fd_consptr(struct FD_ODBC *,conn,fd_extdb_type);
  char *attr_name = FD_SYMBOL_NAME(attr);
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
#endif

/* Execution */

static lispval stmt_error(SQLHSTMT stmt,const u8_string cxt,int free_stmt)
{
  u8_seterr(ODBCError,cxt,odbc_errstring(stmt,SQL_HANDLE_STMT));
  if (free_stmt) {SQLFreeHandle(SQL_HANDLE_STMT,stmt);}
  return FD_ERROR_VALUE;
}

static lispval get_colvalue
  (SQLHSTMT stmt,int i,int sqltype,int colsize,lispval typeinfo)
{
  lispval result = FD_VOID;
  switch (sqltype) {
  case SQL_CHAR: case SQL_VARCHAR: {
    SQLLEN clen; u8_byte *data = u8_malloc(colsize+1);
    int ret = SQLGetData(stmt,i+1,SQL_C_CHAR,data,colsize+1,&clen);
    if (SQL_SUCCEEDED(ret)) {
      if (clen*2<colsize) {
        result = lispval_string(data);
        u8_free(data);}
      else result = fd_lispstring(data);
      break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  case SQL_BIGINT: {
    unsigned long long intval = 0;
    int ret = SQLGetData(stmt,i+1,SQL_C_UBIGINT,&intval,0,NULL);
    if (SQL_SUCCEEDED(ret)) {
      result = FD_INT(intval); break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  case SQL_INTEGER: case SQL_SMALLINT: {
    long intval = 0;
    int ret = SQLGetData(stmt,i+1,SQL_C_LONG,&intval,0,NULL);
    if (SQL_SUCCEEDED(ret)) {
      result = FD_INT(intval); break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  case SQL_FLOAT: case SQL_DOUBLE: {
    double dblval;
    int ret = SQLGetData(stmt,i+1,SQL_C_DOUBLE,&dblval,0,NULL);
    if (SQL_SUCCEEDED(ret)) {
      result = fd_init_double(NULL,dblval); break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  default:
    return FD_VOID;}
  if (FD_VOIDP(typeinfo)) return result;
  else if (FD_OIDP(typeinfo)) {
    FD_OID base = FD_OID_ADDR(typeinfo);
    long long offset=
      ((FD_FIXNUMP(result)) ? (FD_FIX2INT(result)) :
       (FD_BIGINTP(result)) ?
       (fd_bigint_to_long_long((fd_bigint)result)) : (-1));
    if (offset<0) return result;
    else return fd_make_oid(base+offset);}
  else if (FD_APPLICABLEP(typeinfo)) {
    lispval transformed = fd_apply(typeinfo,1,&result);
    fd_decref(result);
    return transformed;}
  else if (FD_TABLEP(typeinfo)) {
    lispval transformed = fd_get(typeinfo,result,FD_EMPTY_CHOICE);
    fd_decref(result);
    return transformed;}
  else return result;
}

static lispval intern_upcase(u8_output out,u8_string s)
{
  int c = u8_sgetc(&s);
  out->u8_write = out->u8_outbuf;
  while (c>=0) {
    u8_putc(out,u8_toupper(c));
    c = u8_sgetc(&s);}
  return fd_make_symbol(out->u8_outbuf,out->u8_write-out->u8_outbuf);
}

static lispval merge_symbol;

static lispval get_stmt_results
  (SQLHSTMT stmt,const u8_string cxt,int free_stmt,lispval typeinfo)
{
  struct U8_OUTPUT out;
  lispval results; int i = 0, ret; SQLSMALLINT n_cols;
  lispval mergefn = fd_getopt(typeinfo,merge_symbol,FD_VOID);
  lispval *colnames, *colinfo; SQLSMALLINT *coltypes; SQLULEN *colsizes;
  if (!((FD_VOIDP(mergefn)) || (FD_TRUEP(mergefn)) ||
        (FD_FALSEP(mergefn)) || (FD_APPLICABLEP(mergefn))))
    return fd_type_error("%MERGE","sqlite_values",mergefn);
  ret = SQLNumResultCols(stmt, &n_cols);
  if (!(SQL_SUCCEEDED(ret))) return stmt_error(stmt,cxt,free_stmt);
  if (n_cols==0) {
    if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
    return FD_VOID;}
  else results = FD_EMPTY_CHOICE;
  colnames = u8_alloc_n(n_cols,lispval);
  colinfo = u8_alloc_n(n_cols,lispval);
  coltypes = u8_alloc_n(n_cols,SQLSMALLINT);
  colsizes = u8_alloc_n(n_cols,SQLULEN);
  /* [TODO] Note that all of this can probably be done up front
     for SQL procs, but we don't do it now. */
  U8_INIT_OUTPUT(&out,64);
  i = 0; while (i<n_cols) {
    SQLCHAR name[300];
    SQLSMALLINT sqltype;
    SQLULEN colsize;
    SQLSMALLINT sqldigits, namelen;
    SQLSMALLINT nullok;
    ret = SQLDescribeCol(stmt,i+1,
                       name,sizeof(name),&namelen,
                       &sqltype,&colsize,&sqldigits,&nullok);
    if (!(SQL_SUCCEEDED(ret))) {
      if (!(FD_VOIDP(typeinfo))) {
        int j = 0; while (j<i) {fd_decref(colinfo[j]); j++;}}
      u8_free(colnames); u8_free(colinfo);
      u8_free(coltypes); u8_free(colsizes);
      return stmt_error(stmt,cxt,free_stmt);}
    colnames[i]=intern_upcase(&out,name);
    colinfo[i]=((FD_VOIDP(typeinfo)) ? (FD_VOID) :
                (fd_get(typeinfo,colnames[i],FD_VOID)));
    coltypes[i]=sqltype;
    colsizes[i]=colsize;
    i++;}
  if ((n_cols==1) && (FD_TRUEP(mergefn)))
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
      lispval value = get_colvalue(stmt,0,coltypes[0],colsizes[0],colinfo[0]);
      if (FD_ABORTP(value)) {
        if (!(FD_VOIDP(typeinfo))) {
          int j = 0; while (j<i) {fd_decref(colinfo[j]); j++;}}
        u8_free(colnames); u8_free(colinfo);
        u8_free(coltypes); u8_free(colsizes);
        if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
        fd_decref(results);
        return FD_ERROR_VALUE;}
      else {FD_ADD_TO_CHOICE(results,value);}}
  else while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
      lispval slotmap;
      struct FD_KEYVAL *kv = u8_alloc_n(n_cols,struct FD_KEYVAL);
      i = 0; while (i<n_cols) {
        lispval value = get_colvalue(stmt,i,coltypes[i],colsizes[i],colinfo[i]);
        if (FD_ABORTP(value)) {
          if (!(FD_VOIDP(typeinfo))) {
            int j = 0; while (j<i) {fd_decref(colinfo[j]); j++;}}
          u8_free(colnames); u8_free(colinfo);
          u8_free(coltypes); u8_free(colsizes);
          if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
          fd_decref(results);
          return FD_ERROR_VALUE;}
        kv[i].kv_key = colnames[i];
        kv[i].kv_val = value;
        i++;}
      if ((FD_VOIDP(mergefn)) || (FD_TRUEP(mergefn)) || (FD_FALSEP(mergefn)))
        slotmap = fd_init_slotmap(NULL,n_cols,kv);
      else {
        lispval tmp_slotmap = fd_init_slotmap(NULL,n_cols,kv);
        slotmap = fd_apply(mergefn,1,&tmp_slotmap);
        fd_decref(tmp_slotmap);}
      FD_ADD_TO_CHOICE(results,slotmap);}
  if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
  if (!(FD_VOIDP(typeinfo))) {
    int j = 0; while (j<n_cols) {fd_decref(colinfo[j]); j++;}}
  return results;
}

static lispval odbcexec(struct FD_ODBC *dbp,lispval string,lispval colinfo)
{
  int ret;
  SQLHSTMT stmt;
  /* No need for locking here, because we have our own statement */
  ret = SQLAllocHandle(SQL_HANDLE_STMT,dbp->conn,&stmt);
  if (!(SQL_SUCCEEDED(ret))) {
    u8_seterr(ODBCError,"odbcexec",NULL);
    return FD_ERROR_VALUE;}
  ret = SQLExecDirect(stmt,(char *)FD_CSTRING(string),FD_STRLEN(string));
  if (FD_VOIDP(colinfo)) colinfo = dbp->extdb_colinfo;
  if (SQL_SUCCEEDED(ret))
    return get_stmt_results(stmt,"odbcexec",1,colinfo);
  else return stmt_error(stmt,"odbcexec",1);
}

static lispval odbcexechandler
  (struct FD_EXTDB *extdb,lispval string,lispval colinfo)
{
  if (extdb->extdb_handler== &odbc_handler)
    return odbcexec((fd_odbc)extdb,string,colinfo);
  else return fd_type_error("ODBC EXTDB","odbcexechandler",(lispval)extdb);
}

static lispval callodbcproc(struct FD_FUNCTION *fn,int n,lispval *args)
{
  struct FD_ODBC_PROC *dbp = (struct FD_ODBC_PROC *)fn;
  int i = 0, ret = -1;
  u8_lock_mutex(&(dbp->odbc_proc_lock));
  while (i<n) {
    lispval arg = args[i]; int dofree = 0;
    if (!(FD_VOIDP(dbp->extdb_paramtypes[i])))
      if (FD_APPLICABLEP(dbp->extdb_paramtypes[i])) {
        arg = fd_apply(dbp->extdb_paramtypes[i],1,&arg);
        if (FD_ABORTP(arg)) {
          u8_unlock_mutex(&(dbp->odbc_proc_lock));
          return arg;}
        else dofree = 1;}
    if (FD_FIXNUMP(arg)) {
      long long intval = FD_FIX2INT(arg);
      SQLBindParameter(dbp->stmt,i+1,
                       SQL_PARAM_INPUT,SQL_C_SLONG,
                       dbp->sqltypes[i],0,0,&intval,0,NULL);}
    else if (FD_FLONUMP(arg)) {
      double floval = FD_FLONUM(arg);
      SQLBindParameter(dbp->stmt,i+1,
                       SQL_PARAM_INPUT,SQL_C_DOUBLE,
                       dbp->sqltypes[i],0,0,&floval,0,NULL);}
    else if (FD_TYPEP(arg,fd_string_type)) {
      SQLBindParameter(dbp->stmt,i+1,
                       SQL_PARAM_INPUT,SQL_C_CHAR,
                       dbp->sqltypes[i],0,0,
                       (char *)FD_CSTRING(arg),
                       FD_STRLEN(arg),
                       NULL);}
    else if (FD_OIDP(arg)) {
      if (FD_OIDP(dbp->extdb_paramtypes[i])) {
        FD_OID addr = FD_OID_ADDR(arg);
        FD_OID base = FD_OID_ADDR(dbp->extdb_paramtypes[i]);
        unsigned long offset = FD_OID_DIFFERENCE(addr,base);
        SQLBindParameter(dbp->stmt,i+1,
                         SQL_PARAM_INPUT,SQL_C_ULONG,
                         dbp->sqltypes[i],0,0,&offset,0,NULL);}
      else {
        FD_OID addr = FD_OID_ADDR(arg);
        SQLBindParameter(dbp->stmt,i+1,
                         SQL_PARAM_INPUT,SQL_C_UBIGINT,
                         dbp->sqltypes[i],0,0,&addr,0,NULL);}}
    if (dofree) fd_decref(arg);
    i++;}
  ret = SQLExecute(dbp->stmt);
  if (SQL_SUCCEEDED(ret)) {
    lispval results = get_stmt_results(dbp->stmt,"odbcexec",0,dbp->extdb_colinfo);
    SQLFreeStmt(dbp->stmt,SQL_CLOSE);
    SQLFreeStmt(dbp->stmt,SQL_RESET_PARAMS);
    u8_unlock_mutex(&(dbp->odbc_proc_lock));
    return results;}
  else {
    u8_unlock_mutex(&(dbp->odbc_proc_lock));
    return stmt_error(dbp->stmt,"odbcexec",0);}
}

/* Initialization */

static int odbc_initialized = 0;

static struct FD_EXTDB_HANDLER odbc_handler=
  {"odbc",NULL,NULL,NULL,NULL};

FD_EXPORT int fd_init_odbc()
{
  lispval module;
  if (odbc_initialized) return 0;
  odbc_initialized = 1;
  fd_init_scheme();

  module = fd_new_cmodule("ODBC",(0),fd_init_odbc);

  odbc_handler.execute = odbcexechandler;
  odbc_handler.makeproc = odbcmakeprochandler;
  odbc_handler.recycle_db = recycle_odbconn;
  odbc_handler.recycle_proc = recycle_odbcproc;

  fd_idefn(module,fd_make_cprim2x("ODBC/OPEN",odbcopen,1,
                                  fd_string_type,FD_VOID,
                                  -1,FD_VOID));

#if 0
  fd_idefn(module,fd_make_cprim2x
           ("ODBC/ATTR",odbcattr,2,
            fd_odbc_type,FD_VOID,fd_symbol_type,FD_VOID));
#endif

  merge_symbol = fd_intern("%MERGE");

  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
