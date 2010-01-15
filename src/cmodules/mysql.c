/* -*- Mode: C; character-encoding: utf-8; -*- */

/* mysql.c
   This implements FramerD bindings to mysql.
   Copyright (C) 2007-2010 beingmeta, inc.
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
#include <libu8/u8timefns.h>
#include <libu8/u8crypto.h>

#include <mysql/mysql.h>
#include <mysql/errmsg.h>

extern my_bool my_init(void);

u8_condition ServerReset=_("MYSQL server reset");

#define dupstring(x) ((x==NULL) ? (x) : ((char *)u8_strdup(x)))

#if FD_THREADS_ENABLED
static u8_mutex mysql_connect_lock;
#endif

FD_EXPORT int fd_init_mysql(void) FD_LIBINIT_FN;
static struct FD_EXTDB_HANDLER mysql_handler;
static fdtype callmysqlproc(struct FD_FUNCTION *fn,int n,fdtype *args);

typedef struct FD_MYSQL {
  FD_EXTDB_FIELDS;
  char *hostname, *username, *passwd;
  char *dbstring, *sockname;
#if FD_THREADS_ENABLED
  u8_mutex lock;
#endif
  int portno, flags;
  MYSQL _db, *db;} FD_MYSQL;
typedef struct FD_MYSQL *fd_mysql;

/* This is used as a buffer for inbound bindings */
union BINDBUF { double fval; void *ptr; long lval; long long llval;};

typedef struct FD_MYSQL_PROC {
  FD_EXTDB_PROC_FIELDS;
  struct FD_MYSQL *fdbptr;
#if FD_THREADS_ENABLED
  u8_mutex lock;
#endif
  u8_string stmt_string; size_t stmt_len;
  int n_cols;
  MYSQL *mysqldb; MYSQL_STMT *stmt;
  MYSQL_BIND *inbound, *outbound;
  my_bool *isnull;

  union BINDBUF *bindbuf;
  fdtype *colnames;} FD_MYSQL_PROC;
typedef struct FD_MYSQL_PROC *fd_mysql_proc;

static int reinit_mysqlproc(FD_MYSQL *db,struct FD_MYSQL_PROC *dbproc);

static fd_exception MySQL_Error=_("MySQL Error");
static fd_exception MySQL_NoConvert=_("Can't convert value to SQL");
static fdtype merge_symbol, noempty_symbol;

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

/* Everything but the hostname and dbname is optional.
   In theory, we could have the dbname be optional, but for now we'll
   require it.  */
static fdtype open_mysql
  (fdtype hostname,fdtype dbname,fdtype colinfo,
   fdtype user,fdtype password,
   fdtype port,fdtype options)
{
  MYSQL *db;
  char *host, *username, *passwd, *dbstring, *sockname;
  int portno=0, flags=0;
  struct FD_MYSQL *dbp=u8_alloc(struct FD_MYSQL);
  my_bool reconnect=0;
  /* Initialize the cons (does a memset too) */
  FD_INIT_FRESH_CONS(dbp,fd_extdb_type);
  /* Initialize the MYSQL side of things (after the memset!) */
  dbp->db=mysql_init(&(dbp->_db));
  if ((dbp->db)==NULL) {
    const char *errmsg=mysql_error(&(dbp->_db));
    u8_seterr(MySQL_Error,"open_mysql",u8_strdup(errmsg));
    return FD_ERROR_VALUE;}

  /* If the hostname looks like a filename, we assume it's a Unix domain
     socket. */
  if (strchr(FD_STRDATA(hostname),'/')==NULL) {
    host=FD_STRDATA(hostname); sockname=NULL;}
  else {
    sockname=FD_STRDATA(hostname); host=NULL;}
  /* Process the other arguments */
  dbstring=((FD_VOIDP(dbname)) ? (NULL) : (FD_STRDATA(dbname)));
  username=((FD_VOIDP(user)) ? (NULL) : (FD_STRDATA(user)));
  passwd=((FD_VOIDP(password)) ? (NULL) : (FD_STRDATA(password)));
  portno=((FD_VOIDP(port)) ? (0) : (fd_getint(port)));
  flags=0;

  /* Try to connect */
  u8_lock_mutex(&mysql_connect_lock);
  db=mysql_real_connect
    (dbp->db,host,username,passwd,dbstring,portno,sockname,flags);
  u8_unlock_mutex(&mysql_connect_lock);

  if (db==NULL) {
    const char *errmsg=mysql_error(&(dbp->_db));
    mysql_close(dbp->db);
    u8_free(dbp);
    u8_seterr(MySQL_Error,"open_mysql",u8_strdup(errmsg));
    return FD_ERROR_VALUE;} 

  if (mysql_set_character_set(dbp->db,"utf8")) {
    const char *errmsg=mysql_error(&(dbp->_db));
    u8_seterr(MySQL_Error,"open_mysql",u8_strdup(errmsg));
    mysql_close(dbp->db);
    u8_free(dbp);
    return FD_ERROR_VALUE;}

  if (mysql_options(dbp->db,MYSQL_OPT_RECONNECT,&reconnect)) {
    const char *errmsg=mysql_error(&(dbp->_db));
    u8_seterr(MySQL_Error,"open_mysql",u8_strdup(errmsg));
    mysql_close(dbp->db);
    u8_free(dbp);
    return FD_ERROR_VALUE;}

  /* Initialize the other fields */
  dbp->dbhandler=&mysql_handler;
  dbp->hostname=dupstring(host);
  dbp->username=dupstring(username);
  dbp->passwd=dupstring(passwd);
  dbp->dbstring=dupstring(dbstring);
  dbp->sockname=dupstring(sockname);
  dbp->portno=portno;
  dbp->flags=flags;
  dbp->colinfo=fd_incref(colinfo);
  dbp->spec=u8_strdup(FD_STRDATA(hostname));
  dbp->info=
    u8_mkstring("%s;client %s",
		mysql_get_host_info(db),
		mysql_get_client_info());

  u8_init_mutex(&dbp->proclock);
  u8_init_mutex(&dbp->lock);

  return FDTYPE_CONS(dbp);
}

static int reopen_mysql(struct FD_MYSQL *c)
{
  MYSQL *db;
  u8_log(LOG_WARN,"reopen_mysql",
	 "Reopening MYSQL connection to '%s' (%s)",
	 c->spec,c->info);
  mysql_close(c->db);
  u8_lock_mutex(&mysql_connect_lock);
  db=mysql_real_connect
    (c->db,c->hostname,c->username,c->passwd,
     c->dbstring,c->portno,c->sockname,c->flags);
  u8_unlock_mutex(&mysql_connect_lock);
  if (db==NULL) {
    const char *errmsg=mysql_error(db);
    u8_seterr(MySQL_Error,"reopen_mysql",u8_strdup(errmsg));
    return -1;}
  u8_lock_mutex(&(c->proclock)); {
    int i=0, n=c->n_procs;
    struct FD_MYSQL_PROC **procs=(FD_MYSQL_PROC **)c->procs;
    while (i<n) reinit_mysqlproc(c,procs[i++]);
    u8_unlock_mutex(&(c->proclock)); 
    return 1;}
}

static void recycle_mysqldb(struct FD_EXTDB *c)
{
  struct FD_MYSQL *dbp=(struct FD_MYSQL *)c;
  int n_procs=dbp->n_procs;
  fdtype *toremove=u8_malloc(sizeof(fdtype)*(dbp->n_procs)), *write=toremove;
  int i=0;
  u8_lock_mutex(&(dbp->proclock));
  while (i<n_procs) {
    struct FD_EXTDB_PROC *p=dbp->procs[--i];
    if (FD_CONS_REFCOUNT(p)>1)
      u8_log(LOG_WARN,"freemysqldb",
	     "dangling pointer to extdbproc %s on %s (%s)",
	     p->qtext,dbp->spec,dbp->info);
    *write++=(fdtype)p;}
  u8_unlock_mutex(&(dbp->proclock));

  i=0; while (i<n_procs) {fdtype proc=toremove[i++]; fd_decref(proc);}

  u8_free(dbp->procs);
  fd_decref(dbp->colinfo);
  u8_free(dbp->spec); u8_free(dbp->info);
  u8_destroy_mutex(&(dbp->proclock));
  u8_destroy_mutex(&(dbp->lock));

  mysql_close(dbp->db);

  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Binding out */

/* This sets up outbound bindings for a statement.
   It assumes that outval->buffer_type has been initialized
   from field-specific information. */
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
  case MYSQL_TYPE_TIME: case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_DATE: case MYSQL_TYPE_DATETIME:
    /* outval->buffer_type=outval->buffer_type; */
    outval->buffer=u8_alloc(MYSQL_TIME);
    outval->buffer_length=sizeof(MYSQL_TIME);
    outval->length=NULL;
    break;
  default:
    outval->buffer=NULL;
    outval->buffer_length=0;
    outval->length=NULL;
    break;
  }
}

/* This returns a LISP pointer for a particular output column
   for a statement given prepared bindings.  We use a limited typespace and
   rely on most of the magic happening inside the MySQL client library.
   The main tricky case here is TEXT/BLOB values (see below) which
     we wait to retrieve.
   One potential optimization would be to go ahead and preallocate buffers
     for those columns when their size is known or constrained.  */
static fdtype outbound_get(MYSQL_STMT *stmt,MYSQL_BIND *bindings,
			   my_bool *isnull,int column)
{
  MYSQL_BIND *outval=&(bindings[column]);
  if (isnull[column]) return FD_EMPTY_CHOICE;
  switch (outval->buffer_type) {
  case MYSQL_TYPE_LONG:
    if (outval->is_unsigned) {
      unsigned int intval=*((unsigned int *)(outval->buffer));
      return FD_INT2DTYPE(intval);}
    else {
      int intval=*((int *)(outval->buffer));
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
    /* This is the only tricky case.  When we do the overall fetch,
       the TEXT/BLOCK columns are not retrieved, though their sizes
       are.  We then use mysql_stmt_fetch_column to get the particular
       value once we have consed a buffer to use. */
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
  case MYSQL_TYPE_DATETIME: case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIMESTAMP: {
    struct U8_XTIME xt;
    MYSQL_TIME *mt=(MYSQL_TIME *)outval->buffer;
    memset(&xt,0,sizeof(xt));
    xt.u8_year=mt->year;
    xt.u8_mon=mt->month-1;
    xt.u8_mday=mt->day;
    if ((outval->buffer_type)==MYSQL_TYPE_DATE)
      xt.u8_prec=u8_day;
    else {
      xt.u8_prec=u8_second;
      xt.u8_hour=mt->hour;
      xt.u8_min=mt->minute;
      xt.u8_sec=mt->second;}
    u8_mktime(&xt);
    return fd_make_timestamp(&xt);}
  default:
    return FD_FALSE;}
}

/* Getting outputs from a prepared statement */

/* This gets all the values returned by a statement, using
   prepared buffers for outbound variables and known column names. */
static fdtype get_stmt_values
  (MYSQL_STMT *stmt,fdtype colinfo,int n_cols,
   fdtype *colnames,MYSQL_BIND *outbound,my_bool *isnullbuf)
{
  fdtype results=FD_EMPTY_CHOICE;
  fdtype mergefn=fd_getopt(colinfo,merge_symbol,FD_VOID);
  int noempty=fd_testopt(colinfo,noempty_symbol,FD_VOID);
  fdtype _colmaps[16], *colmaps=
    ((n_cols>16) ? (u8_alloc_n(n_cols,fdtype)) : (_colmaps));
  int i=0, retval=mysql_stmt_fetch(stmt);
  /* Initialize colmaps */
  while (i<n_cols) {
    colmaps[i]=fd_getopt(colinfo,colnames[i],FD_VOID); i++;}
  while ((retval==0) || (retval==MYSQL_DATA_TRUNCATED)) {
    fdtype result;
    struct FD_KEYVAL *kv=u8_alloc_n(n_cols,struct FD_KEYVAL);
    int n_slots=0;
    i=0; while (i<n_cols) {
      fdtype value=outbound_get(stmt,outbound,isnullbuf,i);
      /* Convert outbound variables via colmaps if specified. */
      if (FD_EMPTY_CHOICEP(value)) /* NULL value, don't convert */
	if (noempty) {i++; continue;}
	else kv[n_slots].value=value;
      else if (FD_VOIDP(colmaps[i]))
	kv[n_slots].value=value;
      else if (FD_APPLICABLEP(colmaps[i])) {
	kv[n_slots].value=fd_apply(colmaps[i],1,&value);
	fd_decref(value);}
      else if (FD_OIDP(colmaps[i]))
	if (FD_STRINGP(value)) {
	  kv[n_slots].value=fd_parse(FD_STRDATA(value));
	  fd_decref(value);}
	else {
	  FD_OID base=FD_OID_ADDR(colmaps[i]);
	  unsigned int offset=fd_getint(value);
	  if (offset<0) kv[n_slots].value=value;
	  else {
	    FD_OID baseplus=FD_OID_PLUS(base,offset);
	    kv[n_slots].value=fd_make_oid(baseplus);
	    fd_decref(value);}}
      else if (colmaps[i]==FD_TRUE)
	if (FD_STRINGP(value)) {
	  kv[n_slots].value=fd_parse(FD_STRDATA(value));
	  fd_decref(value);}
	else kv[n_slots].value=value;
      else kv[n_slots].value=value;
      kv[n_slots].key=colnames[i];
      n_slots++;
      i++;}
    /* How to merge values.  Currently, if MERGEFN is #t
       and there's only one column, we just use that directly.
       Otherwise, we apply MERGEFN to the returned slotmap and use
       that value.  With no MERGEFN we just make a slotmap.
       It might be cool to do a schemap rather than a slotmap
       at some point. */
    if (n_slots==0) {
      result=FD_EMPTY_CHOICE;
      u8_free(kv);}
    else if ((n_cols==1) && (FD_TRUEP(mergefn))) {
      result=kv[0].value;
      u8_free(kv);}
    else if ((FD_VOIDP(mergefn)) ||
	     (FD_FALSEP(mergefn)) ||
	     (FD_TRUEP(mergefn)))
      result=fd_init_slotmap(NULL,n_slots,kv);
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

/* This inits the vectors used when converting results from statement
   execution. */
static int init_stmt_results
  (MYSQL_STMT *stmt,MYSQL_BIND **outboundptr,fdtype **colnamesptr,
   my_bool **isnullbuf)
{
  int n_cols=mysql_stmt_field_count(stmt);
  if (n_cols>0) {
    fdtype *colnames=u8_alloc_n(n_cols,fdtype);
    MYSQL_BIND *outbound=u8_alloc_n(n_cols,MYSQL_BIND);
    MYSQL_RES *metadata=mysql_stmt_result_metadata(stmt);
    MYSQL_FIELD *fields=((metadata) ? (mysql_fetch_fields(metadata)) : (NULL));
    my_bool *nullbuf=u8_alloc_n(n_cols,my_bool);
    struct U8_OUTPUT out; u8_byte namebuf[128];
    int i=0;
    U8_INIT_OUTPUT_BUF(&out,128,namebuf);
    if (fields==NULL) {
      const char *errmsg=mysql_stmt_error(stmt);
      u8_free(colnames); u8_free(outbound);
      if (metadata) mysql_free_result(metadata);
      u8_seterr(MySQL_Error,"get_stmt_values",u8_strdup(errmsg));
      return -1;}
    memset(outbound,0,sizeof(MYSQL_BIND)*n_cols);
    while (i<n_cols) {
      colnames[i]=intern_upcase(&out,fields[i].name);
      /* Synchronize the signed and unsigned fields to get error handling. */
      if ((fields[i].flags)&(UNSIGNED_FLAG)) outbound[i].is_unsigned=1;
      /* TEXT fields are confusingly labelled with MYSQL_TYPE_BLOB.
	 We check the BINARY flag and set the corresponding MYSQL_BIND
	 buffer type to MYSQL_TYPE_STRING or MYSQL_TYPE_BLOB as
	 appropriate. */
      if (fields[i].type==MYSQL_TYPE_BLOB)
	if ((fields[i].flags)&(BINARY_FLAG))
	  outbound[i].buffer_type=fields[i].type;
	else outbound[i].buffer_type=MYSQL_TYPE_STRING;
      else outbound[i].buffer_type=fields[i].type;
      outbound_setup(&(outbound[i]));
      outbound[i].is_null=&(nullbuf[i]);
      i++;}
    *colnamesptr=colnames;
    *outboundptr=outbound;
    *isnullbuf=nullbuf;
    mysql_stmt_bind_result(stmt,outbound);
    mysql_free_result(metadata); /* Hope this frees FIELDS */
    return n_cols;}
  else {
    *outboundptr=NULL; *colnamesptr=NULL;
    return n_cols;}
}

/* Simple execution */

/* Straightforward execution of a single string.  We use prepared
   statements anyway (rather than mysql_fetch_row) because it lets us
   use raw C types consistent with prepared statements, rather than
   using the converted strings from mysql_fetch_row.  */
static fdtype mysqlexec(struct FD_MYSQL *dbp,fdtype string,fdtype colinfo_arg)
{
  MYSQL *db=dbp->db;
  MYSQL_STMT *stmt=mysql_stmt_init(db);
  MYSQL_BIND *outbound=NULL; fdtype *colnames=NULL; my_bool *isnullbuf;
  fdtype colinfo=merge_colinfo(dbp,colinfo_arg), results;
  int retval, n_cols; unsigned int mysqlerrno=0;
  if (stmt)
    retval=mysql_stmt_prepare(stmt,FD_STRDATA(string),FD_STRLEN(string));
  else retval=-1;
  if (retval)
    if (stmt) mysqlerrno=mysql_errno(db);
    else mysqlerrno=mysql_stmt_errno(stmt);
  if ((mysqlerrno==CR_SERVER_GONE_ERROR) ||
      (mysqlerrno==CR_SERVER_LOST)) {
    int reconn=reopen_mysql(dbp);
    fd_decref(colinfo);
    if (reconn<0) {
      const char *errmsg=mysql_stmt_error(stmt);
      u8_seterr(MySQL_Error,"mysqlexec",u8_strdup(errmsg));
      return FD_ERROR_VALUE;}
    else return mysqlexec(dbp,string,colinfo_arg);}

  /* We assume that we don't need to check for the server lost condition
     again.  This probably isn't correct.  ??? */
  if (retval==0) n_cols=init_stmt_results(stmt,&outbound,&colnames,&isnullbuf);
  if (retval==0) retval=mysql_stmt_execute(stmt);

  if (n_cols==0) return FD_VOID;
  else {
    results=get_stmt_values(stmt,colinfo,n_cols,colnames,outbound,isnullbuf);
    fd_decref(colinfo);}

  int i=0; while (i<n_cols) {
    if (outbound[i].buffer) u8_free(outbound[i].buffer); i++;}

  if (outbound) u8_free(outbound);
  if (colnames) u8_free(colnames);

  if (retval) {
    const char *errmsg=mysql_stmt_error(stmt);
    fd_decref(results);
    u8_seterr(MySQL_Error,"mysqlexec",u8_strdup(errmsg));
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

  /* Set up the vectors we'll use for statement execution. */
  dbproc->n_cols=n_cols=
    init_stmt_results(dbproc->stmt,&(dbproc->outbound),&(dbproc->colnames),
		      &(dbproc->isnull));
  dbproc->n_params=n_params=mysql_stmt_param_count(dbproc->stmt);
  dbproc->inbound=u8_alloc_n(n_params,MYSQL_BIND);
  memset(dbproc->inbound,0,sizeof(MYSQL_BIND)*n_params);
  dbproc->bindbuf=u8_alloc_n(n_params,union BINDBUF);
  memset(dbproc->bindbuf,0,sizeof(union BINDBUF)*n_params);
  
  /* Set up the other fields */
  
  dbproc->stmt_string=u8_strdup(stmt);
  dbproc->stmt_len=stmt_len;

  dbproc->dbhandler=&mysql_handler;
  dbproc->db=(fdtype)dbp; fd_incref(dbproc->db);
  dbproc->mysqldb=db;
  dbproc->filename=dbproc->spec=u8_strdup(dbp->spec);
  dbproc->name=dbproc->qtext=_memdup(stmt,stmt_len+1); /* include NUL */
  dbproc->ndprim=0; dbproc->xprim=1; dbproc->arity=-1;
  dbproc->min_arity=n_params;
  dbproc->handler.xcalln=callmysqlproc;

  u8_init_mutex(&(dbproc->lock));

  /* Set up the paramtypes which determine how application arguments
     are converted to SQL parameters. */
  {
    fdtype *paramtypes=u8_alloc_n(n_params,fdtype);
    int j=0; while (j<n_params) {
      if (j<n) paramtypes[j]=fd_incref(ptypes[j]);
      else paramtypes[j]=FD_VOID;
      j++;}
    dbproc->paramtypes=paramtypes;}
  dbproc->fdbptr=dbp;
  fd_register_extdb_proc((struct FD_EXTDB_PROC *)dbproc);
  return FDTYPE_CONS(dbproc);
}

static int reinit_mysqlproc(FD_MYSQL *dbp,struct FD_MYSQL_PROC *dbproc)
{
  MYSQL *db=dbp->db;
  int retval=mysql_stmt_close(dbproc->stmt), n_cols, n_params;
  if (retval) {
    const char *errmsg=mysql_stmt_error(dbproc->stmt);
    u8_seterr(MySQL_Error,"reinit_mysqlproc",u8_strdup(errmsg));
    return -1;}
  dbproc->stmt=mysql_stmt_init(db);
  if (dbproc->stmt)
    retval=mysql_stmt_prepare
      (dbproc->stmt,dbproc->stmt_string,dbproc->stmt_len);
  else {
    const char *errmsg=mysql_error(db);
    u8_seterr(MySQL_Error,"mysqlproc",u8_strdup(errmsg));
    return -1;}

  if (retval) {
    const char *errmsg=mysql_stmt_error(dbproc->stmt);
    u8_free(dbproc);
    u8_seterr(MySQL_Error,"mysqlproc",u8_strdup(errmsg));
    return FD_ERROR_VALUE;}

  /* Reinitialize these structures in case there have been schema
     changes. */
  u8_free(dbproc->outbound); dbproc->outbound=NULL;
  u8_free(dbproc->colnames); dbproc->colnames=NULL;
  u8_free(dbproc->isnull); dbproc->isnull=NULL;

  n_cols=
    init_stmt_results(dbproc->stmt,&(dbproc->outbound),&(dbproc->colnames),
		      &(dbproc->isnull));

  /* Check that the number of returned columns has not changed
     (this could happen if there were schema changes) */
  if (n_cols!=dbproc->n_cols) {
    u8_log(LOG_WARN,ServerReset,
	   "The number of columns for query '%s' on %s (%s) has changed",
	   dbproc->qtext,dbp->spec,dbp->info);
    dbproc->n_cols=n_cols;}
  
  /* Check that the number of parameters has not changed
     (?? this might happen if there were schema changes) */
  n_params=mysql_stmt_param_count(dbproc->stmt);
  if (n_params!=dbproc->n_params) {
    u8_log(LOG_WARN,ServerReset,
	   "The number of parmaters for query '%s' on %s (%s) has changed",
    	   dbproc->qtext,dbp->spec,dbp->info);
    dbproc->n_params=n_params;
    u8_free(dbproc->inbound); dbproc->inbound=NULL;
    u8_free(dbproc->bindbuf); dbproc->bindbuf=NULL;
    if (n_params) {
      dbproc->inbound=u8_alloc_n(n_params,MYSQL_BIND);
      memset(dbproc->inbound,0,sizeof(MYSQL_BIND)*n_params);
      dbproc->bindbuf=u8_alloc_n(n_params,union BINDBUF);
      memset(dbproc->bindbuf,0,sizeof(union BINDBUF)*n_params);}}
  
  return 1;

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
  fd_release_extdb_proc(c);
  mysql_stmt_close(dbp->stmt);
  fd_decref(dbp->colinfo);

  u8_free(dbp->bindbuf);
  u8_free(dbp->isnull);
  u8_free(dbp->inbound);

  i=0; lim=dbp->n_cols; while (i<lim) {
    if (dbp->outbound[i].buffer) u8_free(dbp->outbound[i].buffer);
    i++;}
  u8_free(dbp->outbound);

  i=0; lim=dbp->n_params; while (i< lim) {
    fd_decref(dbp->paramtypes[i]); i++;}
  u8_free(dbp->paramtypes);
  
  u8_free(dbp->spec); u8_free(dbp->qtext);
  
  u8_destroy_mutex(&(dbp->lock));
  
  fd_decref(dbp->db);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static fdtype callmysqlproc(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  fdtype results=FD_EMPTY_CHOICE;
  struct FD_MYSQL_PROC *dbproc=(struct FD_MYSQL_PROC *)fn;
  struct FD_MYSQL *dbp=
    FD_GET_CONS(dbproc->db,fd_extdb_type,struct FD_MYSQL *);
  int n_params=dbproc->n_params;
  MYSQL_BIND *inbound=dbproc->inbound;
  union BINDBUF *bindbuf=dbproc->bindbuf;

  MYSQL_TIME *mstimes[4]; int n_mstimes=0;

  /* Argbuf stores objects we consed in the process of
     converting application objects to SQLish values. */
  fdtype _argbuf[4], *argbuf=
    ((n_params<4) ? (_argbuf) : (u8_alloc_n(n_params,fdtype)));
  fdtype *ptypes=dbproc->paramtypes;
  fdtype values=FD_EMPTY_CHOICE;
  int i=0, retval=1;
  unsigned int mysqlerrno;

  /* DB procs are synchronized. */
  u8_lock_mutex(&(dbproc->lock));

  /* Initialize the input parameters from the arguments. */
  while (i<n_params) {
    fdtype arg=args[i];

    /* Use the ptypes to map application arguments into SQL. */
    if (FD_VOIDP(ptypes[i])) argbuf[i]=FD_VOID;
    else if ((FD_OIDP(arg)) && (FD_OIDP(ptypes[i]))) {
      FD_OID addr=FD_OID_ADDR(arg);
      FD_OID base=FD_OID_ADDR(ptypes[i]);
      unsigned long long offset=FD_OID_DIFFERENCE(addr,base);
      argbuf[i]=arg=FD_INT2DTYPE(offset);}
    else if (FD_APPLICABLEP(ptypes[i])) {
      argbuf[i]=arg=fd_apply(ptypes[i],1,&arg);}
    else if (FD_TRUEP(ptypes[i])) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
      fd_unparse(&out,arg);
      argbuf[i]=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
    else argbuf[i]=FD_VOID;

    /* Set this in case it was different for a previous call. */
    inbound[i].is_null=NULL;

    /* Now set up the bindings */
    if (FD_FIXNUMP(arg)) {
      inbound[i].is_unsigned=0;
      inbound[i].buffer_type=MYSQL_TYPE_LONG;
      inbound[i].buffer=&(bindbuf[i].lval);
      inbound[i].buffer_length=sizeof(int);
      inbound[i].length=NULL;
      bindbuf[i].lval=fd_getint(arg);}
    else if (FD_OIDP(arg)) {
      FD_OID addr=FD_OID_ADDR(arg);
      inbound[i].is_unsigned=1;
      inbound[i].buffer_type=MYSQL_TYPE_LONGLONG;
      inbound[i].buffer=&(bindbuf[i].llval);
      inbound[i].buffer_length=sizeof(unsigned long long);
      inbound[i].length=NULL;
      bindbuf[i].llval=addr;}
    else if (FD_BIGINTP(arg)) {
      long long lv=fd_bigint_to_long_long((fd_bigint)arg);
      inbound[i].is_unsigned=0;
      inbound[i].buffer_type=MYSQL_TYPE_LONGLONG;
      inbound[i].buffer=&(bindbuf[i].llval);
      inbound[i].buffer_length=sizeof(long long);
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
      inbound[i].length=&(inbound[i].buffer_length);}
    else if (FD_SYMBOLP(arg)) {
      u8_string pname=FD_SYMBOL_NAME(arg);
      inbound[i].buffer_type=MYSQL_TYPE_STRING;
      inbound[i].buffer=pname;
      inbound[i].buffer_length=strlen(pname);
      inbound[i].length=&(inbound[i].buffer_length);}
    else if (FD_PACKETP(arg)) {
      inbound[i].buffer_type=MYSQL_TYPE_BLOB;
      inbound[i].buffer=FD_PACKET_DATA(arg);
      inbound[i].buffer_length=FD_PACKET_LENGTH(arg);
      inbound[i].length=&(inbound[i].buffer_length);}
    else if (FD_PRIM_TYPEP(arg,fd_timestamp_type)) {
      struct FD_TIMESTAMP *tm=
	FD_GET_CONS(arg,fd_timestamp_type,struct FD_TIMESTAMP *);
      MYSQL_TIME *mt=u8_alloc(MYSQL_TIME);
      struct U8_XTIME *xt=&(tm->xtime), gmxtime; time_t tick;
      if (n_mstimes<4) mstimes[n_mstimes++]=mt;
      if ((xt->u8_tzoff)||(xt->u8_dstoff)) {
	/* If it's not UTC, we need to convert it. */
	tick=xt->u8_tick;
	u8_init_xtime(&gmxtime,tick,xt->u8_prec,xt->u8_nsecs,0,0);
	xt=&gmxtime;}
      inbound[i].buffer=mt;
      inbound[i].buffer_type=
	((xt->u8_prec>u8_day) ?
	 (MYSQL_TYPE_DATETIME) :
	 (MYSQL_TYPE_DATE));
      mt->year=xt->u8_year;
      mt->month=xt->u8_mon+1;
      mt->day=xt->u8_mday;
      mt->hour=xt->u8_hour;
      mt->minute=xt->u8_min;
      mt->second=xt->u8_sec;}
    else if ((FD_TRUEP(arg)) || (FD_FALSEP(arg))) {
      inbound[i].is_unsigned=0;
      inbound[i].buffer_type=MYSQL_TYPE_LONG;
      inbound[i].buffer=&(bindbuf[i].lval);
      inbound[i].buffer_length=sizeof(int);
      inbound[i].length=NULL;
      if (FD_TRUEP(arg)) bindbuf[i].lval=1;
      else bindbuf[i].lval=0;}
    else if ((FD_EMPTY_CHOICEP(arg))|| (FD_EMPTY_QCHOICEP(arg))) {
      my_bool *bp=(my_bool *)&(bindbuf[i].lval);
      inbound[i].is_null=bp;
      inbound[i].buffer=NULL;
      inbound[i].buffer_length=sizeof(int);
      inbound[i].length=NULL;
      *bp=1;}
    /* This catches cases where the conversion process produces an
       error. */
    else if (FD_ABORTP(arg)) {
      int j=0;
      u8_lock_mutex(&(dbproc->lock));
      while (j<n_params) {fd_decref(argbuf[j]); j++;}
      if (argbuf!=_argbuf) u8_free(argbuf);
      return FD_ERROR_VALUE;}
    else if (FD_TRUEP(ptypes[i])) {
      /* If the ptype is #t, try to convert it into a string,
	 and catch it if you have an error. */
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
      if (fd_unparse(&out,arg)<0) {
	fd_seterr(MySQL_NoConvert,"callmysqlproc",
		  u8_strdup(dbproc->qtext),fd_incref(arg));
	u8_lock_mutex(&(dbproc->lock));
	i=0; while (i<n_params) {fd_decref(argbuf[i]); i++;}
	if (argbuf!=_argbuf) u8_free(argbuf);
	return FD_ERROR_VALUE;}
      else {
	u8_string as_string=out.u8_outbuf;
	int stringlen=out.u8_outptr-out.u8_outbuf;
	inbound[i].buffer_type=MYSQL_TYPE_STRING;
	inbound[i].buffer=as_string;
	inbound[i].buffer_length=stringlen;
	inbound[i].length=NULL;
	/* We put the consed string into the argbug as a Lisp
	   string so that we'll free it when we're done. */
	argbuf[i]=fd_init_string(NULL,stringlen,as_string);}}
    else {
      int j;
      /* Finally, if we can't convert the value, we error. */
      fd_seterr(MySQL_NoConvert,"callmysqlproc",
		u8_strdup(dbproc->qtext),fd_incref(arg));
      u8_unlock_mutex(&(dbproc->lock));
      j=0; while (j<n_mstimes) {u8_free(mstimes[j]); j++;}
      j=0; while (j<n_params) {fd_decref(argbuf[j]); j++;}
      if (argbuf!=_argbuf) u8_free(argbuf);
      return FD_ERROR_VALUE;}
    i++;}
  
  /* Bind and execute */
  retval=mysql_stmt_bind_param(dbproc->stmt,inbound);

  /* Lock the connection before executing */
  u8_lock_mutex(&(dbproc->fdbptr->lock));

  if (retval==0) retval=mysql_stmt_execute(dbproc->stmt);

  if (retval) mysqlerrno=mysql_stmt_errno(dbproc->stmt);
  /* Store the result, so you can release the connection lock. */
  else retval=mysql_stmt_store_result(dbproc->stmt);

  if (retval) mysqlerrno=mysql_stmt_errno(dbproc->stmt);

  if ((mysqlerrno==CR_SERVER_GONE_ERROR) ||
      (mysqlerrno==CR_SERVER_LOST)) {
    int reconn=reopen_mysql(dbp), j=0;
    j=0; while (j<n_mstimes) {u8_free(mstimes[j]); j++;}
    j=0; while (j<n_params) {fd_decref(argbuf[j]); j++;}
    if (argbuf!=_argbuf) u8_free(argbuf);
    u8_unlock_mutex(&(dbproc->fdbptr->lock));
    u8_unlock_mutex(&(dbproc->lock));
    if (reconn<0) {
      const char *errmsg=mysql_stmt_error(dbproc->stmt);
      u8_seterr(MySQL_Error,"mysqlexec",u8_strdup(errmsg));
      return FD_ERROR_VALUE;}
    else return callmysqlproc(fn,n,args);}
  else if (retval) {
    const char *errmsg=mysql_stmt_error(dbproc->stmt);
    u8_seterr(MySQL_Error,"mysqlproc",u8_strdup(errmsg));
    i=0; while (i<n_params) {fd_decref(argbuf[i]); i++;}
    if (argbuf!=_argbuf) u8_free(argbuf);
    u8_unlock_mutex(&(dbproc->fdbptr->lock));
    u8_unlock_mutex(&(dbproc->lock));
    return FD_ERROR_VALUE;}    

  /* Unlock the connection */
  u8_unlock_mutex(&(dbproc->fdbptr->lock));

  /* Get any values.  This will handle it's own errors. */
  if (dbproc->n_cols)
    values=get_stmt_values
      (dbproc->stmt,dbproc->colinfo,
       dbproc->n_cols,dbproc->colnames,
       dbproc->outbound,dbproc->isnull);
  else {
    int rows=mysql_stmt_affected_rows(dbproc->stmt);
    values=FD_VOID;}
  
  /* Clean up */
  i=0; while (i<n_mstimes) {u8_free(mstimes[i]); i++;}
  i=0; while (i<n_params) {fd_decref(argbuf[i]); i++;}
  if (argbuf!=_argbuf) u8_free(argbuf);

  u8_unlock_mutex(&(dbproc->lock));

  return values;
}

/* Initialization */

static int mysql_initialized=0;

static struct FD_EXTDB_HANDLER mysql_handler=
  {"mysql",NULL,NULL,NULL,NULL};

static int init_thread_for_mysql()
{
  u8_log(LOG_DEBUG,"MYSQL","Initializing thread for MYSQL");
  mysql_thread_init();
  return 1;
}

static void cleanup_thread_for_mysql()
{
  u8_log(LOG_DEBUG,"MYSQL","Cleaning up thread for MYSQL");
  mysql_thread_end();
}

FD_EXPORT int fd_init_mysql()
{
  fdtype module;
  if (mysql_initialized) return 0;

  my_init();
  u8_register_threadinit(init_thread_for_mysql);
  u8_register_threadexit(cleanup_thread_for_mysql);

  module=fd_new_module("MYSQL",0);

#if FD_THREADS_ENABLED
  u8_init_mutex(&mysql_connect_lock);
#endif

  mysql_handler.execute=mysqlexechandler;
  mysql_handler.makeproc=mysqlmakeprochandler;
  mysql_handler.recycle_extdb=recycle_mysqldb;
  mysql_handler.recycle_extdb_proc=recycle_mysqlproc;

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
  noempty_symbol=fd_intern("%NOEMPTY");

  fd_finish_module(module);

  fd_register_source_file(versionid);

  return 1;
}
