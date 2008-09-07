/* C Mode */

/* mysql.c
   This implements FramerD bindings to odbc.
   Copyright (C) 2007-2008 beingmeta, inc.
*/

static char versionid[] =
  "$Id:$";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/numbers.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"
#include "fdb/extdb.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8digestfns.h>

#include <mysql/mysql.h>

FD_EXPORT int fd_init_mysql(void) FD_LIBINIT_FN;
static struct FD_EXTDB_HANDLER mysql_handler;
static fdtype callmysqlproc(struct FD_FUNCTION *fn,int n,fdtype *args);

typedef struct FD_MYSQL {
  FD_EXTDB_FIELDS;
#if FD_THREADS_ENABLED
  u8_mutex dblock;
#endif
  MYSQL _db, *db;} FD_MYSQL;
typedef struct FD_MYSQL *fd_mysql;

union BINDBUF { double dbl; void *ptr; int *ival;};

typedef struct FD_MYSQL_PROC {
  FD_EXTDB_PROC_FIELDS;
  int n_fields;
  MYSQL *mysqldb; MYSQL_STMT *stmt;
  MYSQL_BIND *inbind, *outbind;
  union BINDBUF *bindbuf;} FD_MYSQL_PROC;
typedef struct FD_MYSQL_PROC *fd_mysql_proc;

static fd_exception MysqlError=_("MySQL Error");

static fdtype merge_symbol;

static fdtype intern_upcase(u8_output out,u8_string s)
{
  u8_byte *scan=s; int c=u8_sgetc(&s);
  out->u8_outptr=out->u8_outbuf;
  while (c>=0) {
    u8_putc(out,u8_toupper(c));
    c=u8_sgetc(&s);}
  return fd_make_symbol(out->u8_outbuf,out->u8_outptr-out->u8_outbuf);
}

static unsigned char *_memdup(unsigned char *data,int len)
{
  unsigned char *duplicate=u8_alloc_n(len,unsigned char);
  memcpy(duplicate,data,len);
  return duplicate;
}

/* Opening connections */

static fdtype open_mysql
  (fdtype hostname,fdtype dbname,
   fdtype user,fdtype password,
   fdtype port,fdtype options)
{
  MYSQL *db;
  char *host, *username, *passwd, *dbstring, *sockname;
  int portno=0, flags=0;
  struct FD_MYSQL *dbp=u8_alloc(struct FD_MYSQL);
  FD_INIT_FRESH_CONS(dbp,fd_extdb_type);
  dbp->db=mysql_init(&(dbp->_db));
  if ((dbp->db)==NULL) {}
  if (strchr(FD_STRDATA(hostname),'/')==NULL) {
    host=FD_STRDATA(hostname); sockname=NULL;}
  else {
    sockname=FD_STRDATA(hostname); host=NULL;}
  dbstring=((FD_VOIDP(dbname)) ? (NULL) : (FD_STRDATA(dbname)));
  username=((FD_VOIDP(user)) ? (NULL) : (FD_STRDATA(user)));
  passwd=((FD_VOIDP(password)) ? (NULL) : (FD_STRDATA(password)));
  portno=((FD_VOIDP(port)) ? (0) : (fd_getint(port)));
  flags=0;
  db=mysql_real_connect
    (dbp->db,host,user,password,dbstring,sockname,portno,flags);
  if (db==NULL) {} 
  dbp->dbhandler=&mysql_handler;
  dbp->colinfo=colinfo;
  dbp->spec=u8_strdup(FD_STRDATA(hostname));
  dbp->info=u8_strdup(FD_STRDATA(hostname));
  u8_init_mutex(&(dbp->dblock));
  return FDTYPE_CONS(sqlcons);
}

static void recycle_mysqldb(struct FD_EXTDB *c)
{
  struct FD_MYSQL *dbp=(struct FD_MYSQL *)c;
  u8_free(dbp->spec); u8_free(dbp->info);
  mysql_close(dbp->db);
  u8_destroy_mutex(&(dbp->dblock));
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Processing results */

#define IS_BLOBBY(type) \
    ((type==MYSQL_TYPE_BLOB) ||   \
     (type==MYSQL_TYPE_STRING) || \
     (type==MYSQL_TYPE_VAR_STRING))

#define COLBUFS_SIZE 16

static fdtype mysql_values(MYSQL_RES *result,fdtype colinfo,int freeit)
{
  fdtype results=FD_EMPTY_CHOICE;
  fdtype _colnames[COLBUFS_SIZE], *colnames;
  fdtype _colmaps[COLBUFS_SIZE], *colmaps;
  fdtype mergefn=fd_getopt(colinfo,merge_symbol,FD_VOID);
  int i=0, retval;
  int n_cols=mysql_num_fields(result), mergeval=0, need_lengths=0;
  MYSQL_FIELD *fields=mysql_fetch_fields(result);
  MYSQL_ROW *row=NULL;
  struct U8_OUTPUT out;
  if (!((FD_VOIDP(mergefn)) || (FD_TRUEP(mergefn)) ||
	(FD_FALSEP(mergefn)) || (FD_APPLICABLEP(mergefn))))
    return fd_type_error("%MERGE","mysql_values",mergefn);
  if (n_cols==0) return FD_VOID;
  else if (n_cols>COLBUFS_SIZE) {
    colnames=u8_alloc_n(n_cols,fdtype);
    colmaps=u8_alloc_n(n_cols,fdtype);}
  else {
    colnames=_colnames;
    colmaps=_colmaps;}
  U8_INIT_OUTPUT(&out,64);
  while (i<n_cols) {
    fdtype colname;
    enum enum_field_types coltype=fields[i].type;
    colnames[i]=colname=intern_upcase(&out,(u8_string)field[i].name);
    colmaps[i]=(fd_getopt(colinfo,colname,FD_VOID));
    coltypes[i]=type=field[i].type;
    if (IS_BLOBBY(coltype)) need_lengths=1;
    i++;}
  while ((row=mysql_fetch_row(result))) {
    unsigned long *lengths=
      ((need_lengths) ? (mysql_fetch_lengths(result)) : (NULL));
    fdtype slotmap;
    struct FD_KEYVAL *kv=u8_alloc_n(n_cols,struct FD_KEYVAL);
    i=0; while (i<n_cols) {
      fdtype value;
      kv[i].key=colnames[i];
      if (IS_NUM(fields[i].type)) 
	value=fd_parse(row[i]);
      else if (IS_BLOBBY(fields[i].type))
	if ((fields[i].flags)&(IS_BINARY))
	  value=fd_init_packet(NULL,lengths[i],row[i]);
	else value=fd_init_string(NULL,lengths[i],row[i]);
      else value=fd_init_string(NULL,lengths[i],row[i]);
      if (FD_VOIDP(colmaps[i]))
	kv[i].value=value;
      else if (FD_APPLICABLEP(colmaps[i])) {
	kv[i].value=fd_apply(colmaps[i],1,&value);
	fd_decref(value);}
      else if (FD_OIDP(colmaps[i]))
	if (FD_STRINGP(value)) {
	  kv[i].value=fd_parse(FD_STRDATA(value));
	  fd_decref(value);}
	else {
	  FD_OID base=FD_OID_ADDR(colmaps[i]);
	  unsigned int offset=fd_getint(value);
	  if (offset<0) kv[i].value=value;
	  else {
	    kv[i].value=fd_make_oid(base+offset);
	    fd_decref(value);}}
      else if (colmaps[i]==FD_TRUE)
	if (FD_STRINGP(value)) {
	  kv[i].value=fd_parse(FD_STRDATA(value));
	  fd_decref(value);}
	else kv[i].value=value;
      else kv[i].value=value;
      i++;}
    if ((n_cols==1) && (FD_TRUEP(mergefn))) {
      slotmap=kv[0].value;
      u8_free(kv);}
    else if ((FD_VOIDP(mergefn)) ||
	     (FD_FALSEP(mergefn)) ||
	     (FD_TRUEP(mergefn)))
      slotmap=fd_init_slotmap(NULL,n_cols,kv);
    else {
      fdtype tmp_slotmap=fd_init_slotmap(NULL,n_cols,kv);
      slotmap=fd_apply(mergefn,1,&tmp_slotmap);
      fd_decref(tmp_slotmap);}
    FD_ADD_TO_CHOICE(results,slotmap);}
  u8_free(out.u8_outbuf);
  if (n_cols>COLBUFS_SIZE) {
    u8_free(colnames); u8_free(colmaps);}
  fd_decref(mergefn);
  if (freeit) mysql_free_result(result);
  return results;
}

static fdtype mysqlexec(struct FD_MYSQL *dbp,fdtype string,fdtype colinfo)
{
  mysql3 *db=dbp->db;
  MYSQL_RES *result;
  fdtype results;
  int free_colinfo=0;
  int ret=mysql_real_query(db,FD_STRDATA(string),FD_STRLEN(string));
  if (ret) {}
  else result=mysql_use_results(dbp);
  if (FD_VOIDP(colinfo)) colinfo=dbp->colinfo;
  else if (!(FD_VOIDP(dbp->colinfo))) {
    fd_incref(colinfo); fd_incref(dbp->colinfo);
    colinfo=fd_init_pair(colinfo,dbp->colinfo);
    free_colinfo=1;}
  results=mysql_values(result,colinfo,1);
  if (free_colinfo) fd_decref(colinfo);
  return results;
}

static fdtype mysqlexechandler
  (struct FD_EXTDB *extdb,fdtype string,fdtype colinfo)
{
  if (extdb->dbhandler==&mysql_handler)
    return mysqlexec((fd_mysql)extdb,string,colinfo);
  else return fd_type_error("MYSQL EXTDB","mysqlexechandler",(fdtype)extdb);
}

/* MYSQL procs */

static fdtype mysqlmakeproc
  (struct FD_MYSQL *dbp,
   u8_string stmt,int stmt_len,
   fdtype colinfo,int n,fdtype *ptypes)
{
  mysql3 *db=dbp->db;
  u8_string fname;
  int flags=0, consed_colinfo=0, n_params, n_fields, retval;
  struct FD_MYSQL_PROC *dbproc=u8_alloc(struct FD_MYSQL_PROC);
  FD_INIT_FRESH_CONS(dbproc,fd_extdb_proc_type);
  dbproc->stmt=msql_stmt_init(db);
  if (dbproc->stmt)
    retval=mysql_stmt_prepare(dbproc->stmt,stmt,stmt_len);
  else retval=-1;
  if (retval) {
    fdtype dbptr=(fdtype)dbp;
    const char *errmsg=mysql3_errmsg(db);
    fd_seterr(MysqlError,"fdmysql_call",u8_strdup(errmsg),fd_incref(dbptr));
    return FD_ERROR_VALUE;}
  dbproc->dbhandler=&mysql_handler;
  dbproc->db=(fdtype)dbp; fd_incref(dbproc->db);
  dbproc->mysqldb=db;
  dbproc->filename=dbproc->spec=u8_strdup(dbp->spec);
  dbproc->name=dbproc->qtext=_memdup(stmt,stmt_len+1); /* include NUL */
  dbproc->n_params=n_params=mysql_stmt_param_count(dbproc->stmt);
  dbproc->n_fields=n_fields=mysql_stmt_field_count(dbproc->stmt);
  dbproc->ndprim=0; dbproc->xprim=1; dbproc->arity=-1;
  dbproc->min_arity=n_params;
  dbproc->handler.xcalln=callmysqlproc;

  dbproc->inbind=u8_alloc_n(n_params,MYSQL_BIND);
  memset(dbproc->inbind,0,sizeof(MYSQL_BIND)*n_params);
  dbproc->outbind=u8_alloc_n(n_fields,MYSQL_BIND);
  memset(dbproc->outbind,0,sizeof(MYSQL_BIND)*n_fields);
  dbproc->bindbuf=u8_alloc_n((n_fields+n_params),union BINDBUF);
  memset(dbproc->bindbuf,0,sizeof(union BINDBUF)*(n_fields+n_params));

  {
    fdtype *paramtypes=u8_alloc_n(n_params,fdtype);
    int j=0; while (j<n_params) {
      if (j<n) paramtypes[j]=fd_incref(ptypes[j]);
      else paramtypes[j]=FD_VOID;
      j++;}
    dbproc->paramtypes=paramtypes;}
  if (FD_VOIDP(colinfo))
    dbproc->colinfo=fd_incref(dbp->colinfo);
  else if (FD_VOIDP(dbp->colinfo))
    dbproc->colinfo=fd_incref(colinfo);
  else {
    fd_incref(colinfo); fd_incref(dbp->colinfo);
    dbproc->colinfo=fd_init_pair(NULL,colinfo,dbp->colinfo);}
  return FDTYPE_CONS(dbproc);
}

static fdtype mysqlmakeprochandler
  (struct FD_EXTDB *extdb,
   u8_string stmt,int stmt_len,
   fdtype colinfo,int n,fdtype *ptypes)
{
  if (extdb->dbhandler==&mysql_handler)
    return mysqlmakeproc((fd_mysql)extdb,stmt,stmt_len,colinfo,n,ptypes);
  else return fd_type_error("MYSQL EXTDB","mysqlmakeprochandler",(fdtype)extdb);
}

static void recycle_mysqlproc(struct FD_EXTDB_PROC *c)
{
  struct FD_MYSQL_PROC *dbp=(struct FD_MYSQL_PROC *)c;
  mysql_stmt_close(dbp->stmt);
  fd_decref(dbp->colinfo);
  u8_free(dbp->spec); u8_free(dbp->qtext);
  {int j=0, lim=dbp->n_params;; while (j<lim) {
    fd_decref(dbp->paramtypes[j]); j++;}}
  u8_free(dbp->sqltypes); u8_free(dbp->paramtypes);
  fd_decref(dbp->db);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* This is much hairier for MYSQL because you can't have a prepared
   statement just return a result set to process, so we have to process
   the statement results as output bindings instead. */
static fdtype callmysqlproc(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  struct FD_MYSQL_PROC *dbproc=(struct FD_MYSQL_PROC *)fn;
  MYSQL_BIND *params=NULL;
  unsigned int *datalengths=NULL;
  unsigned short *isnull=NULL;
  fdtype values=FD_EMPTY_CHOICE;
  int i=0, ret=-1;
  if (dbproc->n_params) {
    int n_params=dbproc->n_params;
    params=u8_alloc_n(n_params,MYSQL_BIND);
    databuf=u8_alloc_n(n_params,union DATABUF);
    isnull=u8_alloc_n(n_params,unsigned_short);
    datalengths=u8_alloc_n(n_params,unsigned int);
    memset(params,0,n_params*sizeof(MYSQL_BIND));
    memset(databuf,0,n_params*sizeof(union DATABUF));
    memset(databuf,0,n_params*sizeof(unsigned int));
    memset(isnull,0,n_params*sizeof(unsigned short));
    while (i<n) {
      fdtype arg=args[i]; int dofree=0;
      if (!(FD_VOIDP(dbproc->paramtypes[i])))
	if (FD_APPLICABLEP(dbproc->paramtypes[i])) {
	  arg=fd_apply(dbproc->paramtypes[i],1,&arg);
	  if (FD_ABORTP(arg)) return arg;
	  else dofree=1;}
      params[i].length=&(lengths[i]);
      params[i].isnull=&(isnull[i]);
      if (FD_PRIM_TYPEP(arg,fd_fixnum_type)) {
	databuf[i].ival=FD_FIX2INT(arg);
	params[i].buffer_type=;
	params[i].buffer=&(databuf[i].ival);
	params[i].buffer_length=sizeof(int);
	lengths[i]=sizeof(int);}
      else if (FD_PRIM_TYPEP(arg,fd_double_type)) {
	double floval=FD_FLONUM(arg);
	ret=mysql3_bind_double(dbproc->stmt,i+1,floval);}
      else if (FD_PRIM_TYPEP(arg,fd_string_type)) 
	ret=mysql3_bind_text(dbproc->stmt,i+1,FD_STRDATA(arg),FD_STRLEN(arg),MYSQL_TRANSIENT);
    else if (FD_OIDP(arg))
      if (FD_OIDP(dbproc->paramtypes[i])) {
	FD_OID addr=FD_OID_ADDR(arg);
	FD_OID base=FD_OID_ADDR(dbproc->paramtypes[i]);
	unsigned long offset=FD_OID_DIFFERENCE(addr,base);
	ret=mysql3_bind_int(dbproc->stmt,i+1,offset);}
      else {
	FD_OID addr=FD_OID_ADDR(arg);
	ret=mysql3_bind_int64(dbproc->stmt,i+1,addr);}
    if (dofree) fd_decref(arg);
    if (ret) {
      const char *errmsg=mysql3_errmsg(dbproc->mysqldb);
      fd_seterr(MysqlError,"fdmysql_call",u8_strdup(errmsg),fd_incref((fdtype)fn));
      return FD_ERROR_VALUE;}
    i++;}
    mysql_bind_params(dbproc->stmt,params);}
  values=mysql_values(dbproc->mysqldb,dbproc->stmt,dbproc->colinfo);
  mysql3_reset(dbproc->stmt);
  if (FD_ABORTP(values)) {
    const char *errmsg=mysql3_errmsg(dbproc->mysqldb);
    fd_seterr(MysqlError,"fdmysql_call",u8_strdup(errmsg),fd_incref((fdtype)fn));}
  return values;
}

/* Initialization */

static int mysql_init=0;

static struct FD_EXTDB_HANDLER mysql_handler=
  {"mysql",NULL,NULL,NULL,NULL};

FD_EXPORT int fd_init_mysql()
{
  fdtype module;
  if (mysql_init) return 0;

  my_init();
  module=fd_new_module("MYSQL",0);

  mysql_handler.execute=mysqlexechandler;
  mysql_handler.makeproc=mysqlmakeprochandler;
  mysql_handler.recycle_extdb=recycle_mysqldb;
  mysql_handler.recycle_extdb_proc=recycle_mysqlproc;

  fd_register_extdb_handler(&mysql_handler);

  fd_defn(module,
	  fd_make_cprim2x("MYSQL/OPEN",open_mysql,1,
			  fd_string_type,FD_VOID,
			  -1,FD_VOID));
  mysql_init=1;

  merge_symbol=fd_intern("%MERGE");

  fd_finish_module(module);

  fd_register_source_file(versionid);

  return 1;
}
