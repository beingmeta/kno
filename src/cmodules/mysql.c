/* C Mode */

/* mysql.c
   This implements FramerD bindings to odbc.
   Copyright (C) 2007-2008 beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

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

union BINDBUF { double fval; void *ptr; long lval; long long llval;};

typedef struct FD_MYSQL_PROC {
  FD_EXTDB_PROC_FIELDS;
  int n_cols;
  MYSQL *mysqldb; MYSQL_STMT *stmt;
  MYSQL_BIND *inbound, *outbound;
  union BINDBUF *bindbuf;
  fdtype *colnames;} FD_MYSQL_PROC;
typedef struct FD_MYSQL_PROC *fd_mysql_proc;

static fd_exception MySQL_Error=_("MySQL Error");

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

static fdtype merge_colinfo(FD_MYSQL *dbp,fdtype colinfo)
{
  if (FD_VOIDP(colinfo)) return fd_incref(dbp->colinfo);
  else if (FD_VOIDP(dbp->colinfo))
    return fd_incref(colinfo);
  else {
    fd_incref(dbp->colinfo); fd_incref(colinfo);
    return fd_init_pair(NULL,colinfo,dbp->colinfo);}
}

/* Opening connections */

static fdtype open_mysql
  (fdtype hostname,fdtype dbname,
   fdtype colinfo,
   fdtype user,fdtype password,
   fdtype port,fdtype options)
{
  MYSQL *db;
  char *host, *username, *passwd, *dbstring, *sockname;
  int portno=0, flags=0;
  struct FD_MYSQL *dbp=u8_alloc(struct FD_MYSQL);
  FD_INIT_FRESH_CONS(dbp,fd_extdb_type);
  dbp->db=mysql_init(&(dbp->_db));
  mysql_options(&(dbp->_db),MYSQL_READ_DEFAULT_GROUP,u8_appid());
  if ((dbp->db)==NULL) {
    const char *errmsg=mysql_error(&(dbp->_db));
    u8_seterr(MySQL_Error,"open_mysql",u8_strdup(errmsg));
    return FD_ERROR_VALUE;}
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
    (dbp->db,host,username,passwd,dbstring,portno,sockname,flags);
  if (db==NULL) {} 
  dbp->dbhandler=&mysql_handler;
  dbp->colinfo=fd_incref(colinfo);
  dbp->spec=u8_strdup(FD_STRDATA(hostname));
  dbp->info=u8_strdup(FD_STRDATA(hostname));
  u8_init_mutex(&(dbp->dblock));
  return FDTYPE_CONS(dbp);
}

static void recycle_mysqldb(struct FD_EXTDB *c)
{
  struct FD_MYSQL *dbp=(struct FD_MYSQL *)c;
  u8_free(dbp->spec); u8_free(dbp->info);
  mysql_close(dbp->db);
  u8_destroy_mutex(&(dbp->dblock));
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Binding out */

static void outbound_setup(MYSQL_BIND *outval)
{
  switch (outval->buffer_type) {
  case MYSQL_TYPE_TINY: case MYSQL_TYPE_SHORT:
  case MYSQL_TYPE_INT24: case MYSQL_TYPE_LONG:
    outval->buffer_type=MYSQL_TYPE_LONG;
    outval->buffer=u8_malloc(sizeof(unsigned long));
    outval->buffer_length=sizeof(unsigned long);
    outval->length=NULL;
    break;
  case MYSQL_TYPE_LONGLONG: 
    outval->buffer_type=MYSQL_TYPE_LONGLONG;
    outval->buffer=u8_malloc(sizeof(unsigned long long));
    outval->buffer_length=sizeof(unsigned long long);
    outval->length=NULL;
    break;
  case MYSQL_TYPE_FLOAT:  case MYSQL_TYPE_DOUBLE: 
    outval->buffer_type=MYSQL_TYPE_DOUBLE;
    outval->buffer=u8_malloc(sizeof(double));
    outval->buffer_length=sizeof(double);
    outval->length=NULL;
    break;
  case MYSQL_TYPE_STRING: case MYSQL_TYPE_VAR_STRING:
    outval->buffer_type=MYSQL_TYPE_STRING;
    outval->buffer=NULL;
    outval->buffer_length=0;
    outval->length=u8_alloc(unsigned long);
    break;
  case MYSQL_TYPE_LONG_BLOB: case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB: case MYSQL_TYPE_BLOB:
    outval->buffer_type=MYSQL_TYPE_BLOB;
    outval->buffer=NULL;
    outval->buffer_length=0;
    outval->length=u8_alloc(unsigned long);
    break;
  default:
    outval->buffer=NULL;
    outval->buffer_length=0;
    outval->length=NULL;
    break;
  }
}

static fdtype outbound_get(MYSQL_STMT *stmt,MYSQL_BIND *bindings,int column)
{
  MYSQL_BIND *outval=&(bindings[column]);
  switch (outval->buffer_type) {
  case MYSQL_TYPE_LONG:
    if (outval->is_unsigned) {
      unsigned long intval=*((unsigned long *)(outval->buffer));
      return FD_INT2DTYPE(intval);}
    else {
      long intval=*((long *)(outval->buffer));
      return FD_INT2DTYPE(intval);}
  case MYSQL_TYPE_LONGLONG:
    if (outval->is_unsigned) {
      unsigned long long intval=*((unsigned long long *)(outval->buffer));
      return FD_INT2DTYPE(intval);}
    else {
      long intval=*((long long *)(outval->buffer));
      return FD_INT2DTYPE(intval);}
  case MYSQL_TYPE_DOUBLE: {
    double floval=*((double *)(outval->buffer));
    return fd_make_double(floval);}
  case MYSQL_TYPE_STRING: case MYSQL_TYPE_BLOB: {
    fdtype value; int binary=((outval->buffer_type)==MYSQL_TYPE_BLOB);
    int datalen=(*(outval->length)), buflen=datalen+((binary) ? (0) : (1));
    outval->buffer=u8_alloc_n(buflen,unsigned char);
    outval->buffer_length=buflen;
    mysql_stmt_fetch_column(stmt,outval,column,0);
    if (!(binary)) ((unsigned char *)outval->buffer)[datalen]='\0';
    if (binary)
      value=fd_init_packet(NULL,datalen,outval->buffer);
    else value=fd_init_string(NULL,datalen,outval->buffer);
    outval->buffer=NULL;
    outval->buffer_length=0;
    return value;}
  default:
    return FD_FALSE;}
}

/* Getting outputs from a prepared statement */

static fdtype get_stmt_values
  (MYSQL_STMT *stmt,fdtype colinfo,int n_cols,
   fdtype *colnames,MYSQL_BIND *outbound)
{
  fdtype results=FD_EMPTY_CHOICE;
  fdtype mergefn=fd_getopt(colinfo,merge_symbol,FD_VOID);
  fdtype _colmaps[16], *colmaps=
    ((n_cols>16) ? (u8_alloc_n(n_cols,fdtype)) : (_colmaps));
  int i=0, retval=mysql_stmt_fetch(stmt);
  /* Initialize colmaps */
  while (i<n_cols) {
    colmaps[i]=fd_getopt(colinfo,colnames[i],FD_VOID); i++;}
  while ((retval==0) || (retval==MYSQL_DATA_TRUNCATED)) {
    fdtype result;
    struct FD_KEYVAL *kv=u8_alloc_n(n_cols,struct FD_KEYVAL);
    i=0; while (i<n_cols) {
      fdtype value=outbound_get(stmt,outbound,i);
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
      kv[i].key=colnames[i];
      i++;}
    if ((n_cols==1) && (FD_TRUEP(mergefn))) {
      result=kv[0].value;
      u8_free(kv);}
    else if ((FD_VOIDP(mergefn)) ||
	     (FD_FALSEP(mergefn)) ||
	     (FD_TRUEP(mergefn)))
      result=fd_init_slotmap(NULL,n_cols,kv);
    else {
      fdtype tmp_slotmap=fd_init_slotmap(NULL,n_cols,kv);
      result=fd_apply(mergefn,1,&tmp_slotmap);
      fd_decref(tmp_slotmap);}
    FD_ADD_TO_CHOICE(results,result);
    retval=mysql_stmt_fetch(stmt);}

  i=0; while (i<n_cols) {fd_decref(colmaps[i]); i++;}
  if (colmaps!=_colmaps) u8_free(colmaps);
  fd_decref(mergefn);

  if (retval==1) {
    const char *errmsg=mysql_stmt_error(stmt);
    fd_decref(results);
    u8_seterr(MySQL_Error,"get_stmt_values",u8_strdup(errmsg));
    return FD_ERROR_VALUE;}
  else return results;
}

static int init_stmt_results
  (MYSQL_STMT *stmt,MYSQL_BIND **outboundptr,fdtype **colnamesptr)
{
  int n_cols=mysql_stmt_field_count(stmt);
  if (n_cols) {
    fdtype *colnames=u8_alloc_n(n_cols,fdtype);
    MYSQL_BIND *outbound=u8_alloc_n(n_cols,MYSQL_BIND);
    MYSQL_RES *metadata=mysql_stmt_result_metadata(stmt);
    MYSQL_FIELD *fields=((metadata) ? (mysql_fetch_fields(metadata)) : (NULL));
    struct U8_OUTPUT out; u8_byte namebuf[128];
    int i=0;
    U8_INIT_OUTPUT_BUF(&out,128,namebuf);
    if (fields==NULL) {
      const char *errmsg=mysql_stmt_error(stmt);
      u8_free(colnames); u8_free(outbound);
      if (metadata) mysql_free_result(metadata);
      u8_seterr(MySQL_Error,"get_stmt_values",u8_strdup(errmsg));
      return FD_ERROR_VALUE;}
    memset(outbound,0,sizeof(MYSQL_BIND)*n_cols);
    while (i<n_cols) {
      colnames[i]=intern_upcase(&out,fields[i].name);
      if ((fields[i].flags)&(UNSIGNED_FLAG)) outbound[i].is_unsigned=1;
      if (fields[i].type==MYSQL_TYPE_BLOB)
	if ((fields[i].flags)&(BINARY_FLAG))
	  outbound[i].buffer_type=fields[i].type;
	else outbound[i].buffer_type=MYSQL_TYPE_STRING;
      else outbound[i].buffer_type=fields[i].type;
      outbound_setup(&(outbound[i]));
      i++;}
    *colnamesptr=colnames;
    *outboundptr=outbound;
    mysql_stmt_bind_result(stmt,outbound);
    mysql_free_result(metadata); /* Hope this frees FIELDS */
    return n_cols;}
  else {
    *outboundptr=NULL; *colnamesptr=NULL;
    return n_cols;}
}

/* Simple execution */

static fdtype mysqlexec(struct FD_MYSQL *dbp,fdtype string,fdtype colinfo_arg)
{
  MYSQL *db=dbp->db;
  MYSQL_STMT *stmt=mysql_stmt_init(db);
  MYSQL_BIND *outbound=NULL; fdtype *colnames=NULL;
  fdtype colinfo, results;
  int retval, n_cols;
  if (stmt)
    retval=mysql_stmt_prepare(stmt,FD_STRDATA(string),FD_STRLEN(string));
  else retval=-1;
  if (retval==0) n_cols=init_stmt_results(stmt,&outbound,&colnames);
  if (retval==0) retval=mysql_stmt_execute(stmt);
  if (retval) {}
  else if (n_cols==0) return FD_VOID;
  else results=get_stmt_values(stmt,colinfo,n_cols,colnames,outbound);

  int i=0; while (i<n_cols) {
    if (outbound[i].buffer) u8_free(outbound[i].buffer); i++;}

  if (outbound) u8_free(outbound);
  if (colnames) u8_free(colnames);

  if (retval) {
    const char *errmsg=mysql_stmt_error(stmt);
    fd_decref(results);
    u8_seterr(MySQL_Error,"get_stmt_values",u8_strdup(errmsg));
    results=FD_ERROR_VALUE;}

  mysql_stmt_close(stmt);

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
  MYSQL *db=dbp->db;
  u8_string fname;
  int flags=0, consed_colinfo=0, n_params, n_cols, retval;
  struct FD_MYSQL_PROC *dbproc=u8_alloc(struct FD_MYSQL_PROC);
  FD_INIT_FRESH_CONS(dbproc,fd_extdb_proc_type);
  dbproc->stmt=mysql_stmt_init(db);
  if (dbproc->stmt)
    retval=mysql_stmt_prepare(dbproc->stmt,stmt,stmt_len);
  else {
    const char *errmsg=mysql_error(db);
    u8_free(dbproc);
    u8_seterr(MySQL_Error,"mysqlproc",u8_strdup(errmsg));
    return FD_ERROR_VALUE;}

  if (retval) {
    const char *errmsg=mysql_stmt_error(dbproc->stmt);
    u8_free(dbproc);
    u8_seterr(MySQL_Error,"mysqlproc",u8_strdup(errmsg));
    return FD_ERROR_VALUE;}

  dbproc->colinfo=merge_colinfo(dbp,colinfo);

  dbproc->n_cols=n_cols=
    init_stmt_results(dbproc->stmt,&(dbproc->outbound),&(dbproc->colnames));

  dbproc->n_params=n_params=mysql_stmt_param_count(dbproc->stmt);
  dbproc->inbound=u8_alloc_n(n_params,MYSQL_BIND);
  memset(dbproc->inbound,0,sizeof(MYSQL_BIND)*n_params);
  dbproc->bindbuf=u8_alloc_n(n_params,union BINDBUF);

  dbproc->dbhandler=&mysql_handler;
  dbproc->db=(fdtype)dbp; fd_incref(dbproc->db);
  dbproc->mysqldb=db;
  dbproc->filename=dbproc->spec=u8_strdup(dbp->spec);
  dbproc->name=dbproc->qtext=_memdup(stmt,stmt_len+1); /* include NUL */
  dbproc->ndprim=0; dbproc->xprim=1; dbproc->arity=-1;
  dbproc->min_arity=n_params;
  dbproc->handler.xcalln=callmysqlproc;

  {
    fdtype *paramtypes=u8_alloc_n(n_params,fdtype);
    int j=0; while (j<n_params) {
      if (j<n) paramtypes[j]=fd_incref(ptypes[j]);
      else paramtypes[j]=FD_VOID;
      j++;}
    dbproc->paramtypes=paramtypes;}
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
  int i, lim;
  mysql_stmt_close(dbp->stmt);
  fd_decref(dbp->colinfo);

  u8_free(dbp->bindbuf);
  u8_free(dbp->inbound);

  i=0; lim=dbp->n_cols; while (i<lim) {
    if (dbp->outbound[i].buffer) u8_free(dbp->outbound[i].buffer);
    i++;}
  u8_free(dbp->outbound);

  i=0; lim=dbp->n_params; while (i< lim) {
    fd_decref(dbp->paramtypes[i]); i++;}
  u8_free(dbp->paramtypes);
  
  u8_free(dbp->spec); u8_free(dbp->qtext);

  fd_decref(dbp->db);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* This is much hairier for MYSQL because you can't have a prepared
   statement just return a result set to process, so we have to process
   the statement results as output bindings instead. */
static fdtype callmysqlproc(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  fdtype results=FD_EMPTY_CHOICE;
  struct FD_MYSQL_PROC *dbproc=(struct FD_MYSQL_PROC *)fn;
  int n_params=dbproc->n_params, retval=1;
  MYSQL_BIND *inbound=dbproc->inbound;
  union BINDBUF *bindbuf=dbproc->bindbuf;
  fdtype _argbuf[4], *argbuf=
    ((n_params<4) ? (_argbuf) : (u8_alloc_n(n_params,fdtype)));
  fdtype *ptypes=dbproc->paramtypes;
  fdtype values=FD_EMPTY_CHOICE;
  int i=0, ret=-1;
  while (i<n_params) {
    fdtype arg=args[i];
    if (FD_VOIDP(ptypes[i])) argbuf[i]=FD_VOID;
    else if ((FD_OIDP(arg)) && (FD_OIDP(ptypes[i]))) {
      FD_OID addr=FD_OID_ADDR(arg);
      FD_OID base=FD_OID_ADDR(ptypes[i]);
      unsigned long long offset=FD_OID_DIFFERENCE(addr,base);
      argbuf[i]=arg=FD_INT2DTYPE(offset);}
    else if (FD_APPLICABLEP(ptypes[i])) {
      argbuf[i]=arg=fd_apply(ptypes[i],1,&arg);}
    else {}
    if (FD_FIXNUMP(arg)) {
      inbound[i].is_unsigned=0;
      inbound[i].buffer_type=MYSQL_TYPE_LONG;
      inbound[i].buffer=&(bindbuf[i].lval);
      inbound[i].buffer_length=sizeof(int);
      inbound[i].length=NULL;
      bindbuf[i].lval=fd_getint(arg);}
    else if (FD_BIGINTP(arg)) {
      long long lv=fd_bigint_to_long_long((fd_bigint)arg);
      inbound[i].is_unsigned=0;
      inbound[i].buffer_type=MYSQL_TYPE_LONGLONG;
      inbound[i].buffer=&(bindbuf[i].llval);
      inbound[i].buffer_length=sizeof(int);
      inbound[i].length=NULL;
      bindbuf[i].llval=lv;}
    else if (FD_FLONUMP(arg)) {
      inbound[i].buffer_type=MYSQL_TYPE_DOUBLE;
      inbound[i].buffer=&(bindbuf[i].fval);
      inbound[i].buffer_length=sizeof(double);
      inbound[i].length=NULL;
      bindbuf[i].fval=FD_FLONUM(arg);}
    else if (FD_STRINGP(arg)) {
      inbound[i].buffer_type=MYSQL_TYPE_STRING;
      inbound[i].buffer=FD_STRDATA(arg);
      inbound[i].buffer_length=FD_STRLEN(arg);
      inbound[i].length=NULL;}
    else if (FD_PACKETP(arg)) {
      inbound[i].buffer_type=MYSQL_TYPE_BLOB;
      inbound[i].buffer=FD_PACKET_DATA(arg);
      inbound[i].buffer_length=FD_PACKET_LENGTH(arg);
      inbound[i].length=NULL;}
    else {/* Should do some kind of error? */}
    i++;}
  
  /* Bind and execute */
  retval=mysql_stmt_bind_param(dbproc->stmt,inbound);
  if (retval==0) retval=mysql_stmt_execute(dbproc->stmt);

  if (retval) {
    const char *errmsg=mysql_stmt_error(dbproc->stmt);
    u8_seterr(MySQL_Error,"mysqlproc",u8_strdup(errmsg));
    return FD_ERROR_VALUE;}    

  values=get_stmt_values
    (dbproc->stmt,dbproc->colinfo,
     dbproc->n_cols,dbproc->colnames,dbproc->outbound);

  i=0; while (i<n_params) {
    fd_decref(argbuf[i]); i++;}
  if (argbuf!=_argbuf) u8_free(argbuf);

  return values;
}

/* Initialization */

static int mysql_initialized=0;

static struct FD_EXTDB_HANDLER mysql_handler=
  {"mysql",NULL,NULL,NULL,NULL};

FD_EXPORT int fd_init_mysql()
{
  fdtype module;
  if (mysql_initialized) return 0;

  my_init();
  module=fd_new_module("MYSQL",0);

  mysql_handler.execute=mysqlexechandler;
  mysql_handler.makeproc=mysqlmakeprochandler;
  mysql_handler.recycle_extdb=recycle_mysqldb;
  mysql_handler.recycle_extdb_proc=recycle_mysqlproc;
  mysql_handler.recycle_extdb_proc=NULL;

  fd_register_extdb_handler(&mysql_handler);

  fd_defn(module,
	  fd_make_cprim7x("MYSQL/OPEN",open_mysql,1,
			  fd_string_type,FD_VOID,
			  fd_string_type,FD_VOID,
			  -1,FD_VOID,
			  fd_string_type,FD_VOID,
			  fd_string_type,FD_VOID,
			  fd_fixnum_type,FD_VOID,
			  -1,FD_VOID));
  mysql_initialized=1;

  merge_symbol=fd_intern("%MERGE");

  fd_finish_module(module);

  fd_register_source_file(versionid);

  return 1;
}
