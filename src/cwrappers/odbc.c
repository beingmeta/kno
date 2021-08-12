/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* odbc.c
   This implements Kno bindings to odbc.
   Copyright (C) 2007-2019 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/eval.h"
#include "kno/sequences.h"
#include "kno/texttools.h"
#include "kno/cprims.h"

#include "kno/sqldb.h"

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

static struct KNO_SQLDB_HANDLER odbc_handler;

typedef struct KNO_ODBC {
  KNO_SQLDB_FIELDS;
  SQLHENV env;
  SQLHDBC conn;} KNO_ODBC;
typedef struct KNO_ODBC *kno_odbc;

typedef struct KNO_ODBC_PROC {
  KNO_SQLPROC_FIELDS;

  u8_mutex odbc_proc_lock;

  SQLSMALLINT *sqltypes;
  SQLHSTMT stmt;} KNO_ODBC_PROC;
typedef struct KNO_ODBC_PROC *kno_odbc_proc;

KNO_EXPORT int kno_init_odbc(void) KNO_LIBINIT_FN;

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

KNO_EXPORT lispval kno_odbc_connect(lispval spec,lispval colinfo,int interactive)
{
  struct KNO_ODBC *dbp = u8_alloc(struct KNO_ODBC);
  int ret = -1, howfar = 0;
  if (interactive<0) interactive = interactive_dflt;
  KNO_INIT_FRESH_CONS(dbp,kno_sqldb_type);
  ret = SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE,&(dbp->env));
  if (SQL_SUCCEEDED(ret)) {
    char *info;
    howfar++;
    SQLSetEnvAttr(dbp->env, SQL_ATTR_ODBC_VERSION,
                  (void *) SQL_OV_ODBC3, 0);
    ret = SQLAllocHandle(SQL_HANDLE_DBC,dbp->env,&(dbp->conn));
    dbp->sqldb_spec = u8_strdup(KNO_CSTRING(spec));
    dbp->sqldb_options = KNO_VOID;
    info = u8_malloc(512);
    strcpy(info,"uninitialized");
    dbp->sqldb_info = info;
    u8_init_mutex(&(dbp->sqlproclock));
    dbp->sqldb_handler = &odbc_handler;
    if (SQL_SUCCEEDED(ret)) {
      howfar++;
      ret = SQLDriverConnect(dbp->conn,sqldialog,
                             (char *)KNO_CSTRING(spec),KNO_STRLEN(spec),
                             info,512,NULL,
                             ((interactive==0) ? (SQL_DRIVER_NOPROMPT) :
                              (interactive==1) ? (SQL_DRIVER_COMPLETE_REQUIRED) :
                              (SQL_DRIVER_PROMPT)));
      if (SQL_SUCCEEDED(ret)) {
        dbp->sqldb_colinfo = colinfo;
        kno_incref(colinfo);
        return LISP_CONS(dbp);}}}
  if (howfar>1)
    u8_seterr(ODBCError,"kno_odbc_connect",
              odbc_errstring(dbp->conn,SQL_HANDLE_DBC));
  else if (howfar)
    u8_seterr(ODBCError,"kno_odbc_connect",
              odbc_errstring(dbp->env,SQL_HANDLE_ENV));
  if (howfar>1) {
    SQLFreeHandle(SQL_HANDLE_DBC,dbp->conn);
    u8_free(dbp->sqldb_info); u8_free(dbp->sqldb_spec);}
  if (howfar) SQLFreeHandle(SQL_HANDLE_ENV,dbp->env);
  u8_free(dbp);
  return KNO_ERROR_VALUE;
}

static void recycle_odbconn(struct KNO_SQLDB *c)
{
  struct KNO_ODBC *dbp = (struct KNO_ODBC *)c;
  SQLFreeHandle(SQL_HANDLE_DBC,dbp->conn);
  SQLFreeHandle(SQL_HANDLE_ENV,dbp->env);
}


DEFC_PRIM("odbc/open",odbcopen,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"spec",kno_string_type,KNO_VOID},
	  {"colinfo",kno_any_type,KNO_VOID})
static lispval odbcopen(lispval spec,lispval colinfo)
{
  return kno_odbc_connect(spec,colinfo,-1);
}

/* ODBCProcs */

static lispval callodbcproc(struct KNO_STACK *stack,struct KNO_FUNCTION *fn,
			    int n,kno_argvec args);

static lispval odbcmakeproc
(struct KNO_ODBC *dbp,
 u8_string stmt,int stmt_len,lispval colinfo,
 int n,kno_argvec args)
{
  int ret = 0, have_stmt = 0;
  SQLSMALLINT n_params, *sqltypes;
  struct KNO_ODBC_PROC *dbproc = u8_alloc(struct KNO_ODBC_PROC);
  KNO_INIT_FRESH_CONS(dbproc,kno_sqlproc_type);
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
    return KNO_ERROR_VALUE;}
  dbproc->sqldbptr = (lispval)dbp; kno_incref(dbproc->sqldbptr);
  dbproc->sqldb_handler = &odbc_handler;
#ifdef KNO_CALL_XCALL
  dbproc->fcn_call = KNO_CALL_XCALL | KNO_CALL_NOTAIL;
#else
  dbproc->fcn_call = KNO_FCN_CALL_XCALL | KNO_FCN_CALL_NOTAIL;
#endif
  dbproc->fcn_call_width = dbproc->fcn_arity = -1;
  dbproc->fcn_min_arity = dbproc->fcn_n_params = n_params;
  dbproc->fcn_name = dbproc->sqldb_qtext=_memdup(stmt,stmt_len+1);
  dbproc->fcn_filename = dbproc->sqldb_spec = u8_strdup(dbp->sqldb_spec);
  dbproc->sqltypes = sqltypes = u8_alloc_n(n_params,SQLSMALLINT);
  dbproc->fcn_handler.xcalln = callodbcproc;

  u8_init_mutex(&(dbproc->odbc_proc_lock));

  if (KNO_VOIDP(colinfo))
    dbproc->sqldb_colinfo = kno_incref(dbp->sqldb_colinfo);
  else if (KNO_VOIDP(dbp->sqldb_colinfo))
    dbproc->sqldb_colinfo = kno_incref(colinfo);
  else {
    kno_incref(colinfo); kno_incref(dbp->sqldb_colinfo);
    dbproc->sqldb_colinfo = kno_conspair(colinfo,dbp->sqldb_colinfo);}
  {
    int i = 0; lispval *specs = u8_alloc_n(n_params,lispval);
    while (i<n_params)
      if (i<n) {specs[i]=kno_incref(args[i]); i++;}
      else {specs[i]=KNO_VOID; i++;}
    dbproc->sqldb_paramtypes = specs;}
  {
    int i = 0; while (i<n_params) {
      SQLDescribeParam((dbproc->stmt),i+1,&(sqltypes[i]),NULL,NULL,NULL);
      i++;}}
  kno_register_sqlproc((struct KNO_SQLPROC *) dbproc);
  return LISP_CONS(dbproc);
}

static lispval odbcmakeprochandler
(struct KNO_SQLDB *sqldb,
 u8_string stmt,int stmt_len,lispval colinfo,
 int n,kno_argvec ptypes)
{
  if (sqldb->sqldb_handler== &odbc_handler)
    return odbcmakeproc((kno_odbc)sqldb,stmt,stmt_len,colinfo,n,ptypes);
  else return kno_type_error("ODBC SQLDB","odbcmakeprochandler",(lispval)sqldb);
}

static void recycle_odbcproc(struct KNO_SQLPROC *c)
{
  struct KNO_ODBC_PROC *dbproc = (struct KNO_ODBC_PROC *)c;
  kno_release_sqlproc((struct KNO_SQLPROC *) dbproc);
  SQLFreeHandle(SQL_HANDLE_STMT,dbproc->stmt);
  kno_decref(dbproc->sqldb_colinfo);
  u8_free(dbproc->sqldb_spec);
  u8_free(dbproc->sqldb_qtext);
  u8_free(dbproc->sqltypes);
  kno_decref(dbproc->sqldbptr);
}

/* Getting attributes from connections */

#if 0
static lispval odbcstringattr(struct KNO_ODBC *dbp,int attrid)
{
  u8_byte _buf[64], *buf=_buf; int ret; SQLSMALLINT len;
  ret = SQLGetInfo(dbp->conn,attrid,(SQLPOINTER)buf,sizeof(_buf),&len);
  if (SQL_SUCCEEDED(ret)) {
    if (len<64)
      return kno_init_string(NULL,len,u8_strdup(buf));
    len++; buf = u8_malloc(len);
    ret = SQLGetInfo(dbp->conn,attrid,(SQLPOINTER)&buf,len,NULL);
    if (SQL_SUCCEEDED(ret))
      return kno_init_string(NULL,len,buf);}
  return KNO_ERROR_VALUE;
}

static lispval odbcintattr(struct KNO_ODBC *dbp,int attrid)
{
  int ret, attrval = 0;
  ret = SQLGetInfo(dbp->conn,attrid,(SQLPOINTER)&attrval,0,NULL);
  if (SQL_SUCCEEDED(ret))
    return KNO_INT(attrval);
  else return KNO_ERROR_VALUE;
}

static lispval odbcattr(lispval conn,lispval attr)
{
  struct KNO_ODBC *dbp = kno_consptr(struct KNO_ODBC *,conn,kno_sqldb_type);
  char *attr_name = KNO_SYMBOL_NAME(attr);
  if (strcmp(attr_name,"DBMS")==0)
    return odbcstringattr(dbp,SQL_DBMS_NAME);
  else if (strcmp(attr_name,"VERSION")==0)
    return odbcstringattr(dbp,SQL_DBMS_VER);
  else if (strcmp(attr_name,"GETDATAEXT")==0)
    return odbcstringattr(dbp,SQL_GETDATA_EXTENSIONS);
  else if (strcmp(attr_name,"MAXCONCURRENT")==0)
    return odbcstringattr(dbp,SQL_MAX_CONCURRENT_ACTIVITIES);
  else return KNO_FALSE;
}
#endif

/* Execution */

static lispval stmt_error(SQLHSTMT stmt,const u8_string cxt,int free_stmt)
{
  u8_seterr(ODBCError,cxt,odbc_errstring(stmt,SQL_HANDLE_STMT));
  if (free_stmt) {SQLFreeHandle(SQL_HANDLE_STMT,stmt);}
  return KNO_ERROR_VALUE;
}

static lispval get_colvalue
(SQLHSTMT stmt,int i,int sqltype,int colsize,lispval typeinfo)
{
  lispval result = KNO_VOID;
  switch (sqltype) {
  case SQL_CHAR: case SQL_VARCHAR: {
    SQLLEN clen; u8_byte *data = u8_malloc(colsize+1);
    int ret = SQLGetData(stmt,i+1,SQL_C_CHAR,data,colsize+1,&clen);
    if (SQL_SUCCEEDED(ret)) {
      if (clen*2<colsize) {
        result = kno_mkstring(data);
        u8_free(data);}
      else result = kno_wrapstring(data);
      break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  case SQL_BIGINT: {
    unsigned long long intval = 0;
    int ret = SQLGetData(stmt,i+1,SQL_C_UBIGINT,&intval,0,NULL);
    if (SQL_SUCCEEDED(ret)) {
      result = KNO_INT(intval); break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  case SQL_INTEGER: case SQL_SMALLINT: {
    long intval = 0;
    int ret = SQLGetData(stmt,i+1,SQL_C_LONG,&intval,0,NULL);
    if (SQL_SUCCEEDED(ret)) {
      result = KNO_INT(intval); break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  case SQL_FLOAT: case SQL_DOUBLE: {
    double dblval;
    int ret = SQLGetData(stmt,i+1,SQL_C_DOUBLE,&dblval,0,NULL);
    if (SQL_SUCCEEDED(ret)) {
      result = kno_init_double(NULL,dblval); break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  default:
    return KNO_VOID;}
  if (KNO_VOIDP(typeinfo)) return result;
  else if (KNO_OIDP(typeinfo)) {
    KNO_OID base = KNO_OID_ADDR(typeinfo);
    long long offset=
      ((KNO_FIXNUMP(result)) ? (KNO_FIX2INT(result)) :
       (KNO_BIGINTP(result)) ?
       (kno_bigint_to_long_long((kno_bigint)result)) : (-1));
    if (offset<0) return result;
    else return kno_make_oid(base+offset);}
  else if (KNO_APPLICABLEP(typeinfo)) {
    lispval transformed = kno_apply(typeinfo,1,&result);
    kno_decref(result);
    return transformed;}
  else if (KNO_TABLEP(typeinfo)) {
    lispval transformed = kno_get(typeinfo,result,KNO_EMPTY_CHOICE);
    kno_decref(result);
    return transformed;}
  else return result;
}

static lispval merge_symbol;

static lispval get_stmt_results
(SQLHSTMT stmt,const u8_string cxt,int free_stmt,lispval typeinfo)
{
  struct U8_OUTPUT out;
  lispval results; int i = 0, ret; SQLSMALLINT n_cols;
  lispval mergefn = kno_getopt(typeinfo,merge_symbol,KNO_VOID);
  lispval *colnames, *colinfo; SQLSMALLINT *coltypes; SQLULEN *colsizes;
  if (!((KNO_VOIDP(mergefn)) || (KNO_TRUEP(mergefn)) ||
        (KNO_FALSEP(mergefn)) || (KNO_APPLICABLEP(mergefn))))
    return kno_type_error("%MERGE","sqlite_values",mergefn);
  ret = SQLNumResultCols(stmt, &n_cols);
  if (!(SQL_SUCCEEDED(ret))) return stmt_error(stmt,cxt,free_stmt);
  if (n_cols==0) {
    if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
    return KNO_VOID;}
  else results = KNO_EMPTY_CHOICE;
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
      if (!(KNO_VOIDP(typeinfo))) {
        int j = 0; while (j<i) {kno_decref(colinfo[j]); j++;}}
      u8_free(colnames); u8_free(colinfo);
      u8_free(coltypes); u8_free(colsizes);
      return stmt_error(stmt,cxt,free_stmt);}
    colnames[i]=kno_intern(name);
    colinfo[i]=((KNO_VOIDP(typeinfo)) ? (KNO_VOID) :
                (kno_get(typeinfo,colnames[i],KNO_VOID)));
    coltypes[i]=sqltype;
    colsizes[i]=colsize;
    i++;}
  if ((n_cols==1) && (KNO_TRUEP(mergefn)))
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
      lispval value = get_colvalue(stmt,0,coltypes[0],colsizes[0],colinfo[0]);
      if (KNO_ABORTP(value)) {
        if (!(KNO_VOIDP(typeinfo))) {
          int j = 0; while (j<i) {kno_decref(colinfo[j]); j++;}}
        u8_free(colnames); u8_free(colinfo);
        u8_free(coltypes); u8_free(colsizes);
        if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
        kno_decref(results);
        return KNO_ERROR_VALUE;}
      else {KNO_ADD_TO_CHOICE(results,value);}}
  else while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
      lispval slotmap;
      struct KNO_KEYVAL *kv = u8_alloc_n(n_cols,struct KNO_KEYVAL);
      i = 0; while (i<n_cols) {
        lispval value = get_colvalue(stmt,i,coltypes[i],colsizes[i],colinfo[i]);
        if (KNO_ABORTP(value)) {
          if (!(KNO_VOIDP(typeinfo))) {
            int j = 0; while (j<i) {kno_decref(colinfo[j]); j++;}}
          u8_free(colnames); u8_free(colinfo);
          u8_free(coltypes); u8_free(colsizes);
          if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
          kno_decref(results);
          return KNO_ERROR_VALUE;}
        kv[i].kv_key = colnames[i];
        kv[i].kv_val = value;
        i++;}
      if ((KNO_VOIDP(mergefn)) || (KNO_TRUEP(mergefn)) || (KNO_FALSEP(mergefn)))
        slotmap = kno_init_slotmap(NULL,n_cols,kv);
      else {
        lispval tmp_slotmap = kno_init_slotmap(NULL,n_cols,kv);
        slotmap = kno_apply(mergefn,1,&tmp_slotmap);
        kno_decref(tmp_slotmap);}
      KNO_ADD_TO_CHOICE(results,slotmap);}
  if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
  if (!(KNO_VOIDP(typeinfo))) {
    int j = 0; while (j<n_cols) {kno_decref(colinfo[j]); j++;}}
  return results;
}

static lispval odbcexec(struct KNO_ODBC *dbp,lispval string,lispval colinfo)
{
  int ret;
  SQLHSTMT stmt;
  /* No need for locking here, because we have our own statement */
  ret = SQLAllocHandle(SQL_HANDLE_STMT,dbp->conn,&stmt);
  if (!(SQL_SUCCEEDED(ret))) {
    u8_seterr(ODBCError,"odbcexec",NULL);
    return KNO_ERROR_VALUE;}
  ret = SQLExecDirect(stmt,(char *)KNO_CSTRING(string),KNO_STRLEN(string));
  if (KNO_VOIDP(colinfo)) colinfo = dbp->sqldb_colinfo;
  if (SQL_SUCCEEDED(ret))
    return get_stmt_results(stmt,"odbcexec",1,colinfo);
  else return stmt_error(stmt,"odbcexec",1);
}

static lispval odbcexechandler
(struct KNO_SQLDB *sqldb,lispval string,lispval colinfo)
{
  if (sqldb->sqldb_handler== &odbc_handler)
    return odbcexec((kno_odbc)sqldb,string,colinfo);
  else return kno_type_error("ODBC SQLDB","odbcexechandler",(lispval)sqldb);
}

static lispval callodbcproc(struct KNO_STACK *s,struct KNO_FUNCTION *fn,
			    int n,kno_argvec args)
{
  struct KNO_ODBC_PROC *dbp = (struct KNO_ODBC_PROC *)fn;
  int i = 0, ret = -1;
  u8_lock_mutex(&(dbp->odbc_proc_lock));
  while (i<n) {
    lispval arg = args[i]; int dofree = 0;
    if (!(KNO_VOIDP(dbp->sqldb_paramtypes[i])))
      if (KNO_APPLICABLEP(dbp->sqldb_paramtypes[i])) {
        arg = kno_apply(dbp->sqldb_paramtypes[i],1,&arg);
        if (KNO_ABORTP(arg)) {
          u8_unlock_mutex(&(dbp->odbc_proc_lock));
          return arg;}
        else dofree = 1;}
    if (KNO_FIXNUMP(arg)) {
      long long intval = KNO_FIX2INT(arg);
      SQLBindParameter(dbp->stmt,i+1,
                       SQL_PARAM_INPUT,SQL_C_SLONG,
                       dbp->sqltypes[i],0,0,&intval,0,NULL);}
    else if (KNO_FLONUMP(arg)) {
      double floval = KNO_FLONUM(arg);
      SQLBindParameter(dbp->stmt,i+1,
                       SQL_PARAM_INPUT,SQL_C_DOUBLE,
                       dbp->sqltypes[i],0,0,&floval,0,NULL);}
    else if (KNO_TYPEP(arg,kno_string_type)) {
      SQLBindParameter(dbp->stmt,i+1,
                       SQL_PARAM_INPUT,SQL_C_CHAR,
                       dbp->sqltypes[i],0,0,
                       (char *)KNO_CSTRING(arg),
                       KNO_STRLEN(arg),
                       NULL);}
    else if (KNO_OIDP(arg)) {
      if (KNO_OIDP(dbp->sqldb_paramtypes[i])) {
        KNO_OID addr = KNO_OID_ADDR(arg);
        KNO_OID base = KNO_OID_ADDR(dbp->sqldb_paramtypes[i]);
        unsigned long offset = KNO_OID_DIFFERENCE(addr,base);
        SQLBindParameter(dbp->stmt,i+1,
                         SQL_PARAM_INPUT,SQL_C_ULONG,
                         dbp->sqltypes[i],0,0,&offset,0,NULL);}
      else {
        KNO_OID addr = KNO_OID_ADDR(arg);
        SQLBindParameter(dbp->stmt,i+1,
                         SQL_PARAM_INPUT,SQL_C_UBIGINT,
                         dbp->sqltypes[i],0,0,&addr,0,NULL);}}
    if (dofree) kno_decref(arg);
    i++;}
  ret = SQLExecute(dbp->stmt);
  if (SQL_SUCCEEDED(ret)) {
    lispval results = get_stmt_results(dbp->stmt,"odbcexec",0,dbp->sqldb_colinfo);
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

static struct KNO_SQLDB_HANDLER odbc_handler=
  {"odbc",NULL,NULL,NULL,NULL};

static lispval odbc_module;

KNO_EXPORT int kno_init_odbc()
{
  if (odbc_initialized) return 0;
  odbc_initialized = 1;
  kno_init_scheme();

  odbc_module = kno_new_cmodule("odbc",(0),kno_init_odbc);

  odbc_handler.execute = odbcexechandler;
  odbc_handler.makeproc = odbcmakeprochandler;
  odbc_handler.recycle_db = recycle_odbconn;
  odbc_handler.recycle_proc = recycle_odbcproc;

  link_local_cprims();

  merge_symbol = kno_intern("%merge");

  kno_finish_module(odbc_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}



static void link_local_cprims()
{
  KNO_LINK_CPRIM("odbc/open",odbcopen,2,odbc_module);
}
