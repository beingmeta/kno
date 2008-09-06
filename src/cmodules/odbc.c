/* C Mode */

/* odbc.c
   This implements FramerD bindings to odbc.
   Copyright (C) 2007-2008 beingmeta, inc.
*/

static char versionid[] =
  "$Id:$";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/numbers.h"
#include "fdb/eval.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"

#include "fdb/extdb.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8digestfns.h>

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

static struct FD_EXTDB_HANDLER odbc_handler;

typedef struct FD_ODBC {
  FD_EXTDB_FIELDS;
  SQLHENV env;
  SQLHDBC conn;} FD_ODBC;

typedef struct FD_ODBC_PROC {
  FD_EXTDB_PROC_FIELDS;
  SQLSMALLINT *sqltypes;
  SQLHENV env;
  SQLHDBC conn;
  SQLHSTMT stmt;} FD_ODBC_PROC;

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

static SQLHWND sqldialog=0;
static int interactive_dflt=0;

FD_EXPORT fdtype fd_odbc_connect(fdtype spec,fdtype colinfo,int interactive)
{
  struct FD_ODBC *dbp=u8_alloc(struct FD_ODBC);
  int ret=-1, howfar=0;
  if (interactive<0) interactive=interactive_dflt;
  FD_INIT_FRESH_CONS(dbp,fd_extdb_type);
  ret=SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE,&(dbp->env));
  if (SQL_SUCCEEDED(ret)) {
    howfar++;
    SQLSetEnvAttr(dbp->env, SQL_ATTR_ODBC_VERSION,
		  (void *) SQL_OV_ODBC3, 0);
    ret=SQLAllocHandle(SQL_HANDLE_DBC,dbp->env,&(dbp->conn));
    dbp->spec=u8_strdup(FD_STRDATA(spec));
    dbp->cid=u8_malloc(512); strcpy(dbp->cid,"uninitialized");
    dbp->dbhandler=&odbc_handler;
    if (SQL_SUCCEEDED(ret)) {
      howfar++;
      ret=SQLDriverConnect(dbp->conn,sqldialog,
			   FD_STRDATA(spec),FD_STRLEN(spec),
			   dbp->cid,512,NULL,
			   ((interactive==0) ? (SQL_DRIVER_NOPROMPT) :
			    (interactive==1) ? (SQL_DRIVER_COMPLETE_REQUIRED) :
			    (SQL_DRIVER_PROMPT)));
      if (SQL_SUCCEEDED(ret)) {
	dbp->colinfo=colinfo; fd_incref(colinfo);
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

static fdtype odbcopen(fdtype spec,fdtype colinfo)
{
  return fd_odbc_connect(spec,colinfo,-1);
}

/* ODBCProcs */

static fdtype callodbcproc(struct FD_FUNCTION *fn,int n,fdtype *args);

FD_EXPORT fdtype odbcmakeproc(fdtype spec,fdtype stmt,fdtype colinfo,int n,fdtype *args)
{
  struct FD_ODBC_PROC *dbp=u8_alloc(struct FD_ODBC_PROC);
  int ret=-1, howfar=0, running=1, interactive=interactive_dflt;
  int speclen, consed_colinfo=0;
  u8_string specstring;
  SQLSMALLINT n_params, *sqltypes;
  if (FD_STRINGP(spec)) {
    specstring=FD_STRDATA(spec);
    speclen=FD_STRLEN(spec);}
  else if (FD_PRIM_TYPEP(spec,fd_extdb_type)) {
    struct FD_EXTDB *extdb=(struct FD_EXTDB *)spec;
    if ((extdb->dbhandler)==(&odbc_handler)) {
      specstring=extdb->spec;
      speclen=strlen(specstring);
      if (FD_VOIDP(colinfo)) colinfo=extdb->colinfo;
      else if (FD_VOIDP(extdb->colinfo)) {}
      else {
	fd_incref(colinfo); fd_incref(extdb->colinfo);
	colinfo=fd_init_pair(NULL,colinfo,extdb->colinfo);
	consed_colinfo=1;}}
    else return fd_type_error("ODBCDB","odbcmakproc",spec);}
  else return fd_type_error("ODBCDB","odbcmakproc",spec);
  FD_INIT_FRESH_CONS(dbp,fd_extdb_proc_type);
  while (running) {
    ret=SQLAllocHandle(SQL_HANDLE_ENV,SQL_NULL_HANDLE,&(dbp->env));
    if (!(SQL_SUCCEEDED(ret))) {running=-1; break;} else howfar++; /* 1 */
    ret=SQLSetEnvAttr(dbp->env, SQL_ATTR_ODBC_VERSION,
		      (void *) SQL_OV_ODBC3, 0);
    if (!(SQL_SUCCEEDED(ret))) {running=-1; break;} else howfar++; /* 2 */
    ret=SQLAllocHandle(SQL_HANDLE_DBC,dbp->env,&(dbp->conn));
    if (!(SQL_SUCCEEDED(ret))) {running=-1; break;} else howfar++; /* 3 */
    dbp->cid=u8_malloc(512); strcpy(dbp->cid,"uninitialized");
    ret=SQLDriverConnect(dbp->conn,sqldialog,
			 specstring,speclen,
			 dbp->cid,512,NULL,
			 ((interactive==0) ? (SQL_DRIVER_NOPROMPT) :
			  (interactive==1) ? (SQL_DRIVER_COMPLETE_REQUIRED) :
			  (SQL_DRIVER_PROMPT)));
    if (!(SQL_SUCCEEDED(ret))) {running=-1; break;} else howfar++; /* 4 */
    ret=SQLAllocHandle(SQL_HANDLE_STMT, dbp->conn, &(dbp->stmt));
    if (!(SQL_SUCCEEDED(ret))) {running=-1; break;} else howfar++;
    ret=SQLPrepare(dbp->stmt,FD_STRDATA(stmt),FD_STRLEN(stmt)); /* 5 */
    if (!(SQL_SUCCEEDED(ret))) {running=-1; break;} else howfar++;    
    ret=SQLNumParams(dbp->stmt,&n_params);
    if (!(SQL_SUCCEEDED(ret))) {running=-1; break;} else howfar++; /* 6 */
    dbp->dbhandler=&odbc_handler;
    dbp->ndprim=0; dbp->xprim=1; dbp->min_arity=dbp->n_params=n_params;
    dbp->arity=-1;
    dbp->name=dbp->qtext=u8_strdup(FD_STRDATA(stmt));
    dbp->filename=dbp->spec=u8_strdup(specstring);
    dbp->sqltypes=sqltypes=u8_alloc_n(n_params,SQLSMALLINT);
    dbp->colinfo=colinfo;
    dbp->typeinfo=NULL; dbp->defaults=NULL;
    dbp->handler.xcalln=callodbcproc;
    if (!(consed_colinfo)) fd_incref(colinfo);
    {
      int i=0; fdtype *specs=u8_alloc_n(n_params,fdtype);
      while (i<n_params)
	if (i<n) {specs[i]=fd_incref(args[i]); i++;}
	else {specs[i]=FD_VOID; i++;}
      dbp->paramtypes=specs;}
    {
      int i=0; while (i<n_params) {
	SQLDescribeParam((dbp->stmt),i+1,&(sqltypes[i]),NULL,NULL,NULL);
	i++;}}
    running=0;}
  
  if (running>=0) return FDTYPE_CONS(dbp);
  if (howfar>=5)
    u8_seterr(ODBCError,"fd_odbcproc",odbc_errstring(dbp->stmt,SQL_HANDLE_STMT));
  else if (howfar >= 3)
    u8_seterr(ODBCError,"fd_odbcproc",odbc_errstring(dbp->conn,SQL_HANDLE_DBC));
  else if (howfar>=1)
    u8_seterr(ODBCError,"fd_odbcproc",odbc_errstring(dbp->env,SQL_HANDLE_ENV));
  else u8_seterr(ODBCError,"fd_odbcproc",NULL);
  if (howfar>=5) {
    int j=0; while (j<n_params) {fd_decref(dbp->paramtypes[j]); j++;}
    u8_free(dbp->qtext); fd_decref(dbp->colinfo);
    u8_free(dbp->paramtypes);}
  if (howfar>=4) {
    SQLFreeHandle(SQL_HANDLE_STMT,dbp->stmt);}
  if (howfar>1) {
    SQLFreeHandle(SQL_HANDLE_ENV,dbp->env);}
  return FD_ERROR_VALUE;
}

static void recycle_odbcproc(struct FD_CONS *c)
{
  struct FD_ODBC_PROC *dbp=(struct FD_ODBC_PROC *)c;
  SQLFreeHandle(SQL_HANDLE_STMT,dbp->stmt);
  SQLFreeHandle(SQL_HANDLE_DBC,dbp->conn);
  SQLFreeHandle(SQL_HANDLE_ENV,dbp->env);
  fd_decref(dbp->colinfo);
  u8_free(dbp->spec); u8_free(dbp->cid); u8_free(dbp->qtext);
  u8_free(dbp->sqltypes);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
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
  struct FD_ODBC *dbp=FD_GET_CONS(conn,fd_extdb_type,struct FD_ODBC *);
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

/* static fdtype stmt_error(SQLHSTMT stmt,SQLHDBC dbc,SQLHENV dbenv,const u8_string cxt,int free_stmt) */
static fdtype stmt_error(SQLHSTMT stmt,const u8_string cxt,int free_stmt)
{
  u8_seterr(ODBCError,cxt,odbc_errstring(stmt,SQL_HANDLE_STMT));
  if (free_stmt) {SQLFreeHandle(SQL_HANDLE_STMT,stmt);}
  return FD_ERROR_VALUE;
}

static fdtype get_colvalue(SQLHSTMT stmt,int i,int sqltype,int colsize,fdtype typeinfo)
{
  fdtype result=FD_VOID;
  switch (sqltype) {
  case SQL_CHAR: case SQL_VARCHAR: {
    SQLLEN clen; u8_byte *data=u8_malloc(colsize+1);
    int ret=SQLGetData(stmt,i+1,SQL_C_CHAR,data,colsize+1,&clen);
    if (SQL_SUCCEEDED(ret)) {
      if (clen*2<colsize) {
	result=fdtype_string(data);
	u8_free(data);}
      else result=fd_init_string(NULL,-1,data);
      break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  case SQL_BIGINT: {
    unsigned long long intval=0;
    int ret=SQLGetData(stmt,i+1,SQL_C_UBIGINT,&intval,0,NULL);
    if (SQL_SUCCEEDED(ret)) {
      result=FD_INT2DTYPE(intval); break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  case SQL_INTEGER: case SQL_SMALLINT: {
    long intval=0;
    int ret=SQLGetData(stmt,i+1,SQL_C_LONG,&intval,0,NULL);
    if (SQL_SUCCEEDED(ret)) {
      result=FD_INT2DTYPE(intval); break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  case SQL_FLOAT: case SQL_DOUBLE: {
    double dblval;
    int ret=SQLGetData(stmt,i+1,SQL_C_DOUBLE,&dblval,0,NULL);
    if (SQL_SUCCEEDED(ret)) {
      result=fd_init_double(NULL,dblval); break;}
    else return stmt_error(stmt,"get_colvalue",0);}
  default:
    return FD_VOID;}
  if (FD_VOIDP(typeinfo)) return result;
  else if (FD_OIDP(typeinfo)) {
    FD_OID base=FD_OID_ADDR(typeinfo);
    unsigned long long offset=
      ((FD_FIXNUMP(result)) ? (FD_FIX2INT(result)) :
       (FD_PTR_TYPEP(result,fd_bigint_type)) ?
       (fd_bigint_to_ulong_long((fd_bigint)result)) : (-1));
    if (offset<0) return result;
    else return fd_make_oid(base+offset);}
  else if (FD_APPLICABLEP(typeinfo)) {
    fdtype transformed=fd_apply(typeinfo,1,&result);
    fd_decref(result);
    return transformed;}
  else if (FD_TABLEP(typeinfo)) {
    fdtype transformed=fd_get(typeinfo,result,FD_EMPTY_CHOICE);
    fd_decref(result);
    return transformed;}
  else return result;
}

static fdtype intern_upcase(u8_output out,u8_string s)
{
  u8_byte *scan=s; int c=u8_sgetc(&s);
  out->u8_outptr=out->u8_outbuf;
  while (c>=0) {
    u8_putc(out,u8_toupper(c));
    c=u8_sgetc(&s);}
  return fd_make_symbol(out->u8_outbuf,out->u8_outptr-out->u8_outbuf);
}

static fdtype justvalue_symbol;

static fdtype get_stmt_results(SQLHSTMT stmt,const u8_string cxt,int free_stmt,fdtype typeinfo)
{
  struct U8_OUTPUT out;
  fdtype results; int i=0, ret; SQLSMALLINT n_cols;
  fdtype *colnames, *colinfo; SQLSMALLINT *coltypes; SQLULEN *colsizes;
  ret=SQLNumResultCols(stmt, &n_cols);
  if (!(SQL_SUCCEEDED(ret))) return stmt_error(stmt,cxt,free_stmt);
  if (n_cols==0) {
    if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
    return FD_VOID;}
  else results=FD_EMPTY_CHOICE;
  colnames=u8_alloc_n(n_cols,fdtype);
  colinfo=u8_alloc_n(n_cols,fdtype);
  coltypes=u8_alloc_n(n_cols,SQLSMALLINT);
  colsizes=u8_alloc_n(n_cols,SQLULEN);
  U8_INIT_OUTPUT(&out,64);
  i=0; while (i<n_cols) {
    SQLCHAR name[300];
    SQLSMALLINT sqltype;
    SQLULEN colsize;
    SQLSMALLINT sqldigits, namelen;
    SQLSMALLINT nullok;
    ret=SQLDescribeCol(stmt,i+1,
		       name,sizeof(name),&namelen,
		       &sqltype,&colsize,&sqldigits,&nullok);
    if (!(SQL_SUCCEEDED(ret))) {
      if (!(FD_VOIDP(typeinfo))) {
	int j=0; while (j<i) {fd_decref(colinfo[j]); j++;}}
      u8_free(colnames); u8_free(colinfo); u8_free(coltypes); u8_free(colsizes);
      return stmt_error(stmt,cxt,free_stmt);}
    colnames[i]=intern_upcase(&out,name);
    colinfo[i]=((FD_VOIDP(typeinfo)) ? (FD_VOID) :
		(fd_get(typeinfo,colnames[i],FD_VOID)));
    coltypes[i]=sqltype;
    colsizes[i]=colsize;
    i++;}
  if ((n_cols==1) && (FD_TABLEP(typeinfo)) &&
      (fd_testopt(typeinfo,justvalue_symbol,FD_VOID)))
    while (SQL_SUCCEEDED(ret=SQLFetch(stmt))) {
      fdtype value=get_colvalue(stmt,0,coltypes[0],colsizes[0],colinfo[0]);
      if (FD_ABORTP(value)) {
	if (!(FD_VOIDP(typeinfo))) {
	  int j=0; while (j<i) {fd_decref(colinfo[j]); j++;}}
	u8_free(colnames); u8_free(colinfo); u8_free(coltypes); u8_free(colsizes);
	if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
	fd_decref(results);
	return FD_ERROR_VALUE;}
      else {FD_ADD_TO_CHOICE(results,value);}}
  else while (SQL_SUCCEEDED(ret=SQLFetch(stmt))) {
      fdtype slotmap;
      struct FD_KEYVAL *kv=u8_alloc_n(n_cols,struct FD_KEYVAL);
      i=0; while (i<n_cols) {
	fdtype value=get_colvalue(stmt,i,coltypes[i],colsizes[i],colinfo[i]);
	if (FD_ABORTP(value)) {
	  if (!(FD_VOIDP(typeinfo))) {
	    int j=0; while (j<i) {fd_decref(colinfo[j]); j++;}}
	  u8_free(colnames); u8_free(colinfo); u8_free(coltypes); u8_free(colsizes);
	  if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
	  fd_decref(results);
	  return FD_ERROR_VALUE;}
	kv[i].key=colnames[i];
	kv[i].value=value;
	i++;}
      slotmap=fd_init_slotmap(NULL,n_cols,kv);
      FD_ADD_TO_CHOICE(results,slotmap);}
  if (free_stmt) SQLFreeHandle(SQL_HANDLE_STMT,stmt);
  if (!(FD_VOIDP(typeinfo))) {
    int j=0; while (j<n_cols) {fd_decref(colinfo[j]); j++;}}
  return results;
}

static fdtype odbcexec(fdtype conn,fdtype string,fdtype colinfo)
{
  fdtype results=FD_VOID; int ret, i; SQLSMALLINT n_cols;
  struct FD_ODBC *dbp=FD_GET_CONS(conn,fd_extdb_type,struct FD_ODBC *);
  int stopped=0, howfar=0;
  SQLHSTMT stmt;
  ret=SQLAllocHandle(SQL_HANDLE_STMT,dbp->conn,&stmt);
  if (!(SQL_SUCCEEDED(ret))) {
    u8_seterr(ODBCError,"odbcexec",NULL);
    return FD_ERROR_VALUE;}
  ret=SQLExecDirect(stmt,FD_STRDATA(string),FD_STRLEN(string));
  if (FD_VOIDP(colinfo)) colinfo=dbp->colinfo;
  if (SQL_SUCCEEDED(ret))
    return get_stmt_results(stmt,"odbcexec",1,colinfo);
  else return stmt_error(stmt,"odbcexec",1);
}

static fdtype callodbcproc(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  struct FD_ODBC_PROC *dbp=(struct FD_ODBC_PROC *)fn;
  int i=0, ret=-1;
  while (i<n) {
    fdtype arg=args[i]; int dofree=0;
    if (!(FD_VOIDP(dbp->paramtypes[i])))
      if (FD_APPLICABLEP(dbp->paramtypes[i])) {
	arg=fd_apply(dbp->paramtypes[i],1,&arg);
	if (FD_ABORTP(arg)) return arg;
	else dofree=1;}
    if (FD_PRIM_TYPEP(arg,fd_fixnum_type)) {
      int intval=FD_FIX2INT(arg);
      SQLBindParameter(dbp->stmt,i+1,SQL_PARAM_INPUT,SQL_C_SLONG,dbp->sqltypes[i],0,0,&intval,0,NULL);}
    else if (FD_PRIM_TYPEP(arg,fd_double_type)) {
      double floval=FD_FLONUM(arg);
      SQLBindParameter(dbp->stmt,i+1,SQL_PARAM_INPUT,SQL_C_DOUBLE,dbp->sqltypes[i],0,0,&floval,0,NULL);}
    else if (FD_PRIM_TYPEP(arg,fd_string_type)) {
      SQLBindParameter(dbp->stmt,i+1,SQL_PARAM_INPUT,SQL_C_CHAR,dbp->sqltypes[i],0,0,FD_STRDATA(arg),FD_STRLEN(arg),NULL);}
    else if (FD_OIDP(arg))
      if (FD_OIDP(dbp->paramtypes[i])) {
	FD_OID addr=FD_OID_ADDR(arg);
	FD_OID base=FD_OID_ADDR(dbp->paramtypes[i]);
	unsigned long offset=FD_OID_DIFFERENCE(addr,base);
	SQLBindParameter(dbp->stmt,i+1,SQL_PARAM_INPUT,SQL_C_ULONG,dbp->sqltypes[i],0,0,&addr,0,NULL);}
      else {
	FD_OID addr=FD_OID_ADDR(arg);
	SQLBindParameter(dbp->stmt,i+1,SQL_PARAM_INPUT,SQL_C_UBIGINT,dbp->sqltypes[i],0,0,&addr,0,NULL);}
    if (dofree) fd_decref(arg);
    i++;}
  ret=SQLExecute(dbp->stmt);
  if (SQL_SUCCEEDED(ret))
    return get_stmt_results(dbp->stmt,"odbcexec",0,dbp->colinfo);
  else return stmt_error(dbp->stmt,"odbcexec",0);
}

/* Initialization */

static int odbc_initialized=0;

static struct FD_EXTDB_HANDLER odbc_handler=
  {"odbc",NULL,NULL,NULL,NULL};

FD_EXPORT void fd_init_odbc()
{
  fdtype module;
  if (odbc_initialized) return;
  odbc_initialized=1;
  fd_init_fdscheme();

  module=fd_new_module("ODBC",(0));

  odbc_handler.execute=odbcexec;
  odbc_handler.makeproc=odbcmakeproc;
  odbc_handler.recycle_extdb=recycle_odbconn;
  odbc_handler.recycle_extdb_proc=recycle_odbcproc;

  fd_idefn(module,fd_make_cprim2x("ODBC/OPEN",odbcopen,1,
				  fd_string_type,FD_VOID,
				  -1,FD_VOID));

#if 0
  fd_idefn(module,fd_make_cprim2x
	   ("ODBC/ATTR",odbcattr,2,
	    fd_odbc_type,FD_VOID,fd_symbol_type,FD_VOID));
#endif

  justvalue_symbol=fd_intern("JUSTVALUE");

  fd_finish_module(module);

  fd_register_source_file(versionid);
}
