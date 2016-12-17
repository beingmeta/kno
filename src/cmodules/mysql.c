/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* mysql.c
   This implements FramerD bindings to mysql.
   Copyright (C) 2007-2016 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"
#include "framerd/extdb.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>
#include <libu8/u8crypto.h>

#include <mysql/mysql.h>
#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>

extern my_bool my_init(void);

static fdtype sslca_symbol, sslcert_symbol, sslkey_symbol, sslcadir_symbol;
static fdtype sslciphers_symbol, port_symbol, reconnect_symbol;
static fdtype timeout_symbol, connect_timeout_symbol;
static fdtype read_timeout_symbol, write_timeout_symbol, lazy_symbol;

static fdtype boolean_symbol;
u8_condition ServerReset=_("MYSQL server reset");
u8_condition UnusedType=_("MYSQL unused parameter type");

static u8_string dupstring(fdtype x)
{
  if (FD_VOIDP(x)) return NULL;
  else if (FD_STRINGP(x)) 
    return u8_strdup(FD_STRDATA(x));
  else if ((FD_PACKETP(x))||(FD_PRIM_TYPEP(x,fd_secret_type))) {
    const unsigned char *data=FD_PACKET_DATA(x);
    int len=FD_PACKET_LENGTH(x);
    u8_byte *dup=u8_malloc(len+1);
    memcpy(dup,data,len);
    dup[len]='\0';
    return dup;}
  else return NULL;
}

#define NEED_RESTART(err)                               \
  ((err==CR_SERVER_GONE_ERROR) ||                       \
   (err==ER_NORMAL_SHUTDOWN) ||                         \
   (err==ER_SERVER_SHUTDOWN) ||                         \
   (err==ER_SHUTDOWN_COMPLETE) ||                       \
   (err==CR_SERVER_LOST))
#define NEED_RESET(err)                                 \
  ((err==CR_COMMANDS_OUT_OF_SYNC) ||                    \
   (err==CR_NO_PREPARE_STMT)||                          \
   (err==ER_UNKNOWN_STMT_HANDLER)||                     \
   (err==ER_NEED_REPREPARE))
#define RETVAL_OK 0

#ifndef MYSQL_RESTART_SLEEP
#define MYSQL_RESTART_SLEEP 3
#endif
#ifndef MYSQL_RESTART_WAIT
#define MYSQL_RESTART_WAIT 180
#endif

static int restart_sleep=MYSQL_RESTART_SLEEP;
static int restart_wait=MYSQL_RESTART_WAIT;

#if FD_THREADS_ENABLED
static u8_mutex mysql_connect_lock;
#endif

FD_EXPORT int fd_init_mysql(void) FD_LIBINIT_FN;
static struct FD_EXTDB_HANDLER mysql_handler;
static fdtype callmysqlproc(struct FD_FUNCTION *fn,int n,fdtype *args);

typedef struct FD_MYSQL {
  FD_EXTDB_FIELDS;
  const char *hostname, *username, *passwd;
  const char *dbstring, *sockname;
  double startup; double restarted;
  int restarts;
#if FD_THREADS_ENABLED
  u8_mutex fd_lock;
#endif
  /* Tracking this lets us check whether reconnects have happened */
  unsigned long thread_id;
  int portno, flags;
  MYSQL _db, *db;} FD_MYSQL;
typedef struct FD_MYSQL *fd_mysql;

/* This is used as a buffer for inbound bindings */
union BINDBUF { double fval; void *ptr; long lval; long long llval;};

typedef struct FD_MYSQL_PROC {
  FD_EXTDB_PROC_FIELDS;
  struct FD_MYSQL *fdbptr;
#if FD_THREADS_ENABLED
  u8_mutex fd_lock;
#endif
  u8_string stmt_string; size_t stmt_len;
  int n_cols, need_init;
  MYSQL *mysqldb; MYSQL_STMT *stmt;
  MYSQL_BIND *inbound, *outbound;
  my_bool *isnull;
  union BINDBUF *bindbuf;
  fdtype *colnames;} FD_MYSQL_PROC;
typedef struct FD_MYSQL_PROC *fd_mysql_proc;

static int init_mysqlproc(FD_MYSQL *db,struct FD_MYSQL_PROC *dbproc);

static fd_exception MySQL_Error=_("MySQL Error");
static fd_exception MySQL_NoConvert=_("Can't convert value to SQL");
static fdtype merge_symbol, noempty_symbol, sorted_symbol;

static fdtype intern_upcase(u8_output out,u8_string s)
{
  int c=u8_sgetc(&s);
  out->u8_write=out->u8_outbuf;
  while (c>=0) {
    u8_putc(out,u8_toupper(c));
    c=u8_sgetc(&s);}
  return fd_make_symbol(out->u8_outbuf,out->u8_write-out->u8_outbuf);
}

static unsigned char *_memdup(const unsigned char *data,int len)
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
  else if (colinfo==dbp->colinfo)
    return fd_incref(colinfo);
  else if ((FD_PAIRP(colinfo))&&
           ((FD_CDR(colinfo))==(dbp->colinfo)))
    return fd_incref(colinfo);
  else {
    fd_incref(dbp->colinfo); fd_incref(colinfo);
    return fd_conspair(colinfo,dbp->colinfo);}
}

/* Connection operations */

static int setup_connection(struct FD_MYSQL *dbp)
{
  fdtype options=dbp->options; char *option;
  int retval=0;
  int timeout=-1, ctimeout=-1, rtimeout=-1, wtimeout=-1;
  if (!(FD_VOIDP(options))) {
    fdtype port=fd_getopt(options,port_symbol,FD_VOID);
    fdtype sslca=fd_getopt(options,sslca_symbol,FD_VOID);
    fdtype sslcert=fd_getopt(options,sslcert_symbol,FD_VOID);
    fdtype sslkey=fd_getopt(options,sslkey_symbol,FD_VOID);
    fdtype sslcadir=fd_getopt(options,sslcadir_symbol,FD_VOID);
    fdtype sslciphers=fd_getopt(options,sslciphers_symbol,FD_VOID);
    fdtype toval=fd_getopt(options,timeout_symbol,FD_VOID);
    fdtype ctoval=fd_getopt(options,connect_timeout_symbol,FD_VOID);
    fdtype rtoval=fd_getopt(options,read_timeout_symbol,FD_VOID);
    fdtype wtoval=fd_getopt(options,write_timeout_symbol,FD_VOID);
    dbp->portno=((FD_VOIDP(port)) ? (0) : (fd_getint(port)));
    if (FD_FIXNUMP(toval)) timeout=FD_FIX2INT(toval);
    if (FD_FIXNUMP(ctoval)) ctimeout=FD_FIX2INT(ctoval);
    else ctimeout=timeout;
    if (FD_FIXNUMP(rtoval)) rtimeout=FD_FIX2INT(rtoval);
    else rtimeout=timeout;
    if (FD_FIXNUMP(wtoval)) wtimeout=FD_FIX2INT(wtoval);
    else wtimeout=timeout;
    if (!((FD_VOIDP(sslca))&&(FD_VOIDP(sslcert))&&
          (FD_VOIDP(sslkey))&&(FD_VOIDP(sslcadir))&&
          (FD_VOIDP(sslciphers)))) {
      if (!((FD_VOIDP(sslca))||(FD_STRINGP(sslca))))
        return fd_type_error("SSLCA","open_mysql",sslca);
      else if (!((FD_VOIDP(sslcert))||(FD_STRINGP(sslcert))))
        return fd_type_error("SSLCERT","open_mysql",sslcert);
      else if (!((FD_VOIDP(sslkey))||(FD_STRINGP(sslkey))))
        return fd_type_error("SSLKEY","open_mysql",sslkey);
      else if (!((FD_VOIDP(sslcadir))||(FD_STRINGP(sslcadir))))
        return fd_type_error("SSLCADIR","open_mysql",sslcadir);
      else if (!((FD_VOIDP(sslciphers))||(FD_STRINGP(sslciphers))))
        return fd_type_error("SSLCIPHERS","open_mysql",sslciphers);
      else retval=mysql_ssl_set
             (dbp->db,
              ((FD_VOIDP(sslkey))?(NULL):(FD_STRDATA(sslkey))),
              ((FD_VOIDP(sslcert))?(NULL):(FD_STRDATA(sslcert))),
              ((FD_VOIDP(sslca))?(NULL):(FD_STRDATA(sslca))),
              ((FD_VOIDP(sslcadir))?(NULL):(FD_STRDATA(sslcadir))),
              ((FD_VOIDP(sslciphers))?(NULL):(FD_STRDATA(sslciphers))));}}
  if (retval==RETVAL_OK) {
    my_bool reconnect=0;
    retval=mysql_options(dbp->db,MYSQL_OPT_RECONNECT,&reconnect);}
  if (retval==RETVAL_OK) {
    option="charset";
    retval=mysql_options(dbp->db,MYSQL_SET_CHARSET_NAME,"utf8");}
  if ((retval==RETVAL_OK)&&(ctimeout>0)) {
    unsigned int v=ctimeout; option="connect timeout";
    retval=mysql_options(dbp->db,MYSQL_OPT_CONNECT_TIMEOUT,(char *)&v);}
  if ((retval==RETVAL_OK)&&(rtimeout>0)) {
    unsigned int v=rtimeout; option="read timeout";
    retval=mysql_options(dbp->db,MYSQL_OPT_READ_TIMEOUT,(char *)&v);}
  if ((retval==RETVAL_OK)&&(wtimeout>0)) {
    unsigned int v=wtimeout; option="write timeout";
    retval=mysql_options(dbp->db,MYSQL_OPT_WRITE_TIMEOUT,(char *)&v);}

  if (retval!=RETVAL_OK) {
    const char *errmsg=mysql_error(&(dbp->_db)); fd_incref(options);
    fd_seterr(MySQL_Error,"open_mysql/init",u8_strdup(errmsg),options);}
  return retval;
}

static int restart_connection(struct FD_MYSQL *dbp)
{
  MYSQL *db=NULL;
  unsigned int retval=-1, waited=0, errno=0, thread_id=-1;
  while ((db==NULL)&&(waited<restart_wait)) {
    u8_mutex_lock(&mysql_connect_lock); {
      db=mysql_real_connect
        (dbp->db,dbp->hostname,dbp->username,dbp->passwd,
         dbp->dbstring,dbp->portno,dbp->sockname,dbp->flags);
      u8_mutex_unlock(&mysql_connect_lock);}
    if ((db==NULL)&&(mysql_errno(dbp->db)==CR_ALREADY_CONNECTED)) {
      u8_log(LOG_WARN,"mysql/reconnect",
             "Already connected to %s (%s) with id=%d/%d",
             dbp->spec,dbp->info,dbp->thread_id,mysql_thread_id(dbp->db));
      db=dbp->db;}
    else if (db==NULL) {
      sleep(restart_sleep);
      waited=waited+restart_sleep;}
    else {}}
  
  if (!(db)) {
    const char *msg=mysql_error(db); int err=mysql_errno(dbp->db);
    fdtype conn=(fdtype) dbp; fd_incref(conn);
    u8_log(LOG_CRIT,"mysql/reconnect",
           "Failed after %ds to reconnect to MYSQL %s (%s), final error %s (%d)",
           waited,dbp->spec,dbp->info,msg,err);
    fd_seterr(MySQL_Error,"restart_connection",u8_strdup(msg),conn);
    return -1;}
  else {
    int i=0, n=dbp->n_procs;
    struct FD_MYSQL_PROC **procs=(FD_MYSQL_PROC **)dbp->procs;
    u8_log(LOG_WARN,"mysql/reconnect",
           "Took %ds to reconnect to MYSQL %s (%s), thread_id=%d",
           waited,dbp->spec,dbp->info,dbp->thread_id);
    dbp->thread_id=thread_id=mysql_thread_id(db);
    u8_log(LOG_WARN,"myql/reconnect",
           "Reconnect #%d for MYSQL with %s (%s) rv=%d, thread_id=%d",
           dbp->restarts+1,dbp->spec,dbp->info,retval,thread_id);
    
    /* Flag all the sqlprocs (prepared statements) as needing to be
       reinitialized. */
    while (i<n) {
      struct FD_MYSQL_PROC *proc=procs[i++];
      proc->need_init=1; proc->stmt=NULL;}
    
    /* Some connection parameters may be reset, so we set them again */
    setup_connection(dbp);

    dbp->restarts++; dbp->restarted=u8_elapsed_time();
    dbp->thread_id=mysql_thread_id(dbp->db);
    
    u8_log(LOG_WARN,"myql/reconnect",
           "Reconnect #%d MYSQL connection #%d with %s (%s)",
           dbp->restarts,dbp->thread_id,dbp->spec,dbp->info);
    
    return RETVAL_OK;}
}

static int open_connection(struct FD_MYSQL *dbp)
{
  int retval=0;
  MYSQL *db;
  dbp->db=mysql_init(&(dbp->_db));
  if ((dbp->db)==NULL) {
    const char *errmsg=mysql_error(&(dbp->_db));
    u8_seterr(MySQL_Error,"open_mysql/init",u8_strdup(errmsg));
    return -1;}
  if (retval) {
    const char *errmsg=mysql_error(&(dbp->_db));
    u8_seterr(MySQL_Error,"open_mysql",u8_strdup(errmsg));
    mysql_close(dbp->db);
    u8_free(dbp);
    return -1;}

  retval=setup_connection(dbp);

  if (retval) {
    mysql_close(dbp->db);
    u8_free(dbp);
    return -1;}

  u8_mutex_lock(&mysql_connect_lock); {
    db=mysql_real_connect
      (dbp->db,dbp->hostname,dbp->username,dbp->passwd,
       dbp->dbstring,dbp->portno,dbp->sockname,dbp->flags);}
  u8_mutex_unlock(&mysql_connect_lock);

  if (db==NULL) {
    const char *errmsg=mysql_error(&(dbp->_db));
    u8_seterr(MySQL_Error,"open_connection",u8_strdup(errmsg));
    return -1;}
  else dbp->db=db;
  dbp->restarts=0;
  dbp->restarted=0;
  dbp->startup=u8_elapsed_time();
  dbp->thread_id=mysql_thread_id(db);
  u8_mutex_lock(&(dbp->proclock)); {
    int i=0, n=dbp->n_procs;
    struct FD_MYSQL_PROC **procs=(FD_MYSQL_PROC **)dbp->procs;
    while (i<n) procs[i++]->need_init=1;
    u8_mutex_unlock(&(dbp->proclock));
    return 1;}
}

static void recycle_mysqldb(struct FD_EXTDB *c)
{
  struct FD_MYSQL *dbp=(struct FD_MYSQL *)c;
  int n_procs=dbp->n_procs;
  fdtype *toremove=u8_malloc(sizeof(fdtype)*(dbp->n_procs)), *write=toremove;
  int i=0;
  u8_mutex_lock(&(dbp->proclock));
  while (i<n_procs) {
    struct FD_EXTDB_PROC *p=dbp->procs[--i];
    if (FD_CONS_REFCOUNT(p)>1)
      u8_log(LOG_WARN,"freemysqldb",
             "dangling pointer to extdbproc %s on %s (%s)",
             p->qtext,dbp->spec,dbp->info);
    *write++=(fdtype)p;}
  u8_mutex_unlock(&(dbp->proclock));

  i=0; while (i<n_procs) {fdtype proc=toremove[i++]; fd_decref(proc);}

  u8_free(dbp->procs);
  fd_decref(dbp->colinfo); fd_decref(dbp->options);
  u8_free(dbp->spec); u8_free(dbp->info);
  u8_mutex_destroy(&(dbp->proclock));
  u8_mutex_destroy(&(dbp->fd_lock));

  mysql_close(dbp->db);

  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static fdtype refresh_mysqldb(fdtype c,fdtype flags)
{
  struct FD_MYSQL *dbp=(struct FD_MYSQL *)c;

  int retval=mysql_refresh(dbp->db,
                           REFRESH_GRANT|REFRESH_TABLES|REFRESH_HOSTS|
                           REFRESH_STATUS|REFRESH_THREADS);
  if (retval) {
    const char *errmsg=mysql_error(&(dbp->_db));
    return fd_err(MySQL_Error,"mysql/refresh",(u8_string)errmsg,((fdtype)c));}
  else return FD_VOID;
}

/* Opening connections */

/* Everything but the hostname and dbname is optional.
   In theory, we could have the dbname be optional, but for now we'll
   require it.  */
static fdtype open_mysql
  (fdtype hostname,fdtype dbname,fdtype colinfo,
   fdtype user,fdtype password,
   fdtype options)
{
  const char *host, *username, *passwd, *dbstring, *sockname, *spec;
  int portno=0, flags=0, retval;
  struct FD_MYSQL *dbp=NULL;
  if (!((FD_STRINGP(password))||(FD_PRIM_TYPEP(password,fd_secret_type)))) 
    return fd_type_error("string/secret","open_mysql",password);
  else dbp=u8_alloc(struct FD_MYSQL);
  
  /* Initialize the cons (does a memset too) */
  FD_INIT_FRESH_CONS(dbp,fd_extdb_type);
  /* Initialize the MYSQL side of things (after the memset!) */
  /* If the hostname looks like a filename, we assume it's a Unix domain
     socket. */
  if (strchr(FD_STRDATA(hostname),'/')==NULL) {
    host=u8_strdup(FD_STRDATA(hostname)); 
    sockname=NULL;}
  else {
    sockname=u8_strdup(FD_STRDATA(hostname)); 
    host=NULL;}
  /* Process the other arguments */
  spec=dupstring(hostname);
  dbstring=dupstring(dbname);
  username=dupstring(user);
  passwd=dupstring(password);
  flags=CLIENT_REMEMBER_OPTIONS;

  /* Initialize the other fields */
  dbp->dbhandler=&mysql_handler;
  dbp->hostname=host;
  dbp->username=username;
  dbp->passwd=passwd;
  dbp->dbstring=dbstring;
  dbp->sockname=sockname;
  dbp->portno=portno;
  dbp->flags=flags;
  dbp->colinfo=fd_incref(colinfo);
  dbp->spec=spec;
  dbp->options=options; fd_incref(options);

  u8_mutex_init(&dbp->proclock);
  u8_mutex_init(&dbp->fd_lock);

  /* Prep the structure */
  retval=open_connection(dbp);
  if (retval<0) {
    recycle_mysqldb((struct FD_EXTDB *)dbp);
    return FD_ERROR_VALUE;}

  dbp->info=
    u8_mkstring("%s;client %s",
                mysql_get_host_info(dbp->db),
                mysql_get_client_info());
  dbp->startup=u8_elapsed_time();

  return FDTYPE_CONS(dbp);
}

/* Binding for procedures */

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
      return FD_INT(intval);}
    else {
      int intval=*((int *)(outval->buffer));
      return FD_INT(intval);}
  case MYSQL_TYPE_LONGLONG:
    if (outval->is_unsigned) {
      unsigned long long intval=*((unsigned long long *)(outval->buffer));
      return FD_INT(intval);}
    else {
      long intval=*((long long *)(outval->buffer));
      return FD_INT(intval);}
  case MYSQL_TYPE_DOUBLE: {
    double floval=*((double *)(outval->buffer));
    return fd_make_double(floval);}
  case MYSQL_TYPE_STRING: case MYSQL_TYPE_BLOB: {
    /* This is the only tricky case.  When we do the overall fetch,
       the TEXT/BLOCK columns are not retrieved, though their sizes
       are.  We then use mysql_stmt_fetch_column to get the particular
       value once we have consed a buffer to use. */
    fdtype value;
    int binary=((outval->buffer_type)==MYSQL_TYPE_BLOB);
    int datalen=(*(outval->length)), buflen=datalen+((binary) ? (0) : (1));
    outval->buffer=u8_alloc_n(buflen,unsigned char);
    outval->buffer_length=buflen;
    mysql_stmt_fetch_column(stmt,outval,column,0);
    if (!(binary)) ((unsigned char *)outval->buffer)[datalen]='\0';
    if (binary)
      value=fd_make_packet(NULL,datalen,outval->buffer);
    else value=fd_make_string(NULL,datalen,outval->buffer);
    u8_free(outval->buffer);
    outval->buffer=NULL;
    outval->buffer_length=0;
    return value;}
  case MYSQL_TYPE_DATETIME: case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIMESTAMP: {
    struct U8_XTIME xt;
    MYSQL_TIME *mt=(MYSQL_TIME *)outval->buffer;
    if (!((mt->year)||(mt->month)||(mt->day)||
          (mt->hour)||(mt->minute)||(mt->second)))
      return FD_EMPTY_CHOICE;
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
  fdtype sortval=fd_getopt(colinfo,sorted_symbol,FD_VOID);
  int noempty=fd_testopt(colinfo,noempty_symbol,FD_VOID);
  int sorted=(!((FD_FALSEP(sortval))||(FD_VOIDP(sortval))));
  fdtype _colmaps[16], *colmaps=
    ((n_cols>16) ? (u8_alloc_n(n_cols,fdtype)) : (_colmaps));
  int i=0, retval=mysql_stmt_fetch(stmt);
  int result_index=0, n_results;
  if ((retval==0)||(retval==MYSQL_DATA_TRUNCATED)) {
    n_results=((sorted)?(mysql_stmt_num_rows(stmt)):(0));
    if (sorted) results=fd_init_vector(NULL,n_results,NULL);}
  while (i<n_cols) {
    colmaps[i]=fd_getopt(colinfo,colnames[i],FD_VOID); i++;}
  while ((retval==0) || (retval==MYSQL_DATA_TRUNCATED)) {
    /* For each iteration, we have a good value from the database,
       so we don't need to worry about lost connections for this
       loop, but we do need to worry about them afterwards. */
    fdtype result=FD_EMPTY_CHOICE;
    struct FD_KEYVAL *kv=u8_alloc_n(n_cols,struct FD_KEYVAL);
    int n_slots=0;
    i=0; while (i<n_cols) {
      fdtype value=outbound_get(stmt,outbound,isnullbuf,i);
      /* Convert outbound variables via colmaps if specified. */
      if (FD_EMPTY_CHOICEP(value)) /* NULL value, don't convert */
        if (noempty) {i++; continue;}
        else kv[n_slots].fd_value=value;
      else if (FD_VOIDP(colmaps[i]))
        kv[n_slots].fd_value=value;
      else if (FD_APPLICABLEP(colmaps[i])) {
        kv[n_slots].fd_value=fd_apply(colmaps[i],1,&value);
        fd_decref(value);}
      else if (FD_OIDP(colmaps[i])) {
        if (FD_STRINGP(value)) {
          kv[n_slots].fd_value=fd_parse(FD_STRDATA(value));}
        else {
          FD_OID base=FD_OID_ADDR(colmaps[i]);
          int offset=fd_getint(value);
          if (offset<0) kv[n_slots].fd_value=value;
          else if (offset==0) {
            /* Some fields with OIDS use a zero value to indicate no
               value (empty choice), so we handle that here. */
            if (noempty) {i++; continue;}
            else kv[n_slots].fd_value=FD_EMPTY_CHOICE;}
          else {
            FD_OID baseplus=FD_OID_PLUS(base,offset);
            kv[n_slots].fd_value=fd_make_oid(baseplus);}}
        fd_decref(value);}
      else if (colmaps[i]==FD_TRUE)
        if (FD_STRINGP(value)) {
          if (FD_STRLEN(value))
            kv[n_slots].fd_value=fd_parse(FD_STRDATA(value));
          else kv[n_slots].fd_value=FD_EMPTY_CHOICE;
          fd_decref(value);}
        else kv[n_slots].fd_value=value;
      else if (FD_PRIM_TYPEP(colmaps[i],fd_secret_type)) {
        if ((FD_STRINGP(value))||(FD_PACKETP(value)))
          FD_SET_CONS_TYPE(value,fd_secret_type);
        kv[n_slots].fd_value=value;}
      else if (FD_PRIM_TYPEP(colmaps[i],fd_uuid_type))
        if ((FD_PACKETP(value))||(FD_STRINGP(value))) {
          struct FD_UUID *uuid=u8_alloc(struct FD_UUID);
          const unsigned char *data=
            ((FD_PACKETP(value))?
             (FD_PACKET_DATA(value)):
             (FD_STRDATA(value)));
          unsigned char *uuidbytes;
          FD_INIT_CONS(uuid,fd_uuid_type);
          uuidbytes=uuid->fd_uuid16;
          memcpy(uuidbytes,data,16);
          fd_decref(value);
          kv[n_slots].fd_value=FDTYPE_CONS(uuid);}
        else kv[n_slots].fd_value=value;
      else if (FD_EQ(colmaps[i],boolean_symbol)) {
        if (FD_FIXNUMP(value)) {
          int ival=FD_FIX2INT(value);
          if (ival) kv[n_slots].fd_value=FD_TRUE;
          else kv[n_slots].fd_value=FD_FALSE;}
        else {kv[n_slots].fd_value=value;}}
      else kv[n_slots].fd_value=value;
      kv[n_slots].fd_key=colnames[i];
      if (FD_ABORTP(kv[n_slots].fd_value)) {
        result=kv[n_slots].fd_value;
        break;}
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
      if (kv) u8_free(kv);}
    else if (FD_ABORTP(result)) {
      int j=0; while (j<n_slots) {fd_decref(kv[j].fd_value); j++;}
      if (kv) u8_free(kv);}
    else if ((n_cols==1) && (FD_TRUEP(mergefn))) {
      result=kv[0].fd_value;
      u8_free(kv);}
    else if ((FD_SYMBOLP(mergefn))||(FD_OIDP(mergefn))) {
      int j=0; result=FD_EMPTY_CHOICE;
      while (j<n_slots) {
        if (kv[j].fd_key == mergefn) {
          result=kv[j].fd_value; 
          fd_decref(kv[j].fd_key);
          j++;}
        else {
          fd_decref(kv[j].fd_key);
          fd_decref(kv[j].fd_value);
          j++;}}
      u8_free(kv);}
    else if ((FD_VOIDP(mergefn)) ||
             (FD_FALSEP(mergefn)) ||
             (FD_TRUEP(mergefn)))
      result=fd_init_slotmap(NULL,n_slots,kv);
    else if (!(FD_APPLICABLEP(mergefn))) {
      result=fd_type_error("applicable","mysql/get_stmt_values",mergefn);}
    else {
      fdtype tmp_slotmap=fd_init_slotmap(NULL,n_slots,kv);
      result=fd_apply(mergefn,1,&tmp_slotmap);
      fd_decref(tmp_slotmap);}
    if (FD_ABORTP(result)) {
      fd_decref(results); results=result; break;}
    else if (sorted) {
      FD_VECTOR_SET(results,result_index,result);
      result_index++;}
    else {FD_ADD_TO_CHOICE(results,result);}
    retval=mysql_stmt_fetch(stmt);}

  i=0; while (i<n_cols) {fd_decref(colmaps[i]); i++;}
  if (colmaps!=_colmaps) u8_free(colmaps);
  fd_decref(mergefn);
  fd_decref(sortval);

  if (FD_ABORTP(results)) return results;
  else if (retval==MYSQL_NO_DATA) {
    if (sorted) {
      if (FD_VECTORP(results)) return results;
      fd_decref(results);
      return fd_init_vector(NULL,0,NULL);}
    else return results;}
  else if ((retval)&&(retval!=MYSQL_DATA_TRUNCATED)) {
    fd_decref(results); /* Free any partial results */
    /* An FD_EOD return value indicates some kind of MySQL error */
    return FD_EOD;}
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
    memset(outbound,0,n_cols*sizeof(MYSQL_BIND));
    memset(nullbuf,0,n_cols*sizeof(my_bool));
    U8_INIT_STATIC_OUTPUT_BUF(out,128,namebuf);
    if (fields==NULL) {
      const char *errmsg=mysql_stmt_error(stmt);
      if (colnames) u8_free(colnames); 
      if (outbound) u8_free(outbound);
      if (nullbuf) u8_free(nullbuf);
      if (metadata) mysql_free_result(metadata);
      u8_seterr(MySQL_Error,"init_stmt_results",u8_strdup(errmsg));
      return -1;}
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
      else if (fields[i].type==MYSQL_TYPE_STRING)
        if (fields[i].charsetnr==63)
          outbound[i].buffer_type=MYSQL_TYPE_BLOB;
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
    *outboundptr=NULL; *colnamesptr=NULL;  *isnullbuf=NULL;
    return n_cols;}
}

/* Simple execution */

/* Straightforward execution of a single string.  We use prepared
   statements anyway (rather than mysql_fetch_row) because it lets us
   use raw C types consistent with prepared statements, rather than
   using the converted strings from mysql_fetch_row.  */
static fdtype mysqlexec(struct FD_MYSQL *dbp,fdtype string,
                        fdtype colinfo_arg,int reconn)
{
  MYSQL *db=dbp->db;
  MYSQL_BIND *outbound=NULL; fdtype *colnames=NULL; my_bool *isnullbuf=NULL;
  fdtype colinfo=merge_colinfo(dbp,colinfo_arg), results=FD_VOID;
  int retval, n_cols=0, i=0; unsigned int mysqlerrno=0;
  MYSQL_STMT *stmt;
  u8_mutex_lock(&(dbp->fd_lock));
  stmt=mysql_stmt_init(db);
  if (!(stmt)) {
    u8_log(LOG_WARN,"mysqlexec/stmt_init","Call to mysql_stmt_init failed");
    const char *errmsg=(mysql_error(&(dbp->_db)));
    u8_seterr(MySQL_Error,"mysqlexec",u8_strdup(errmsg));
    fd_decref(colinfo);
    u8_mutex_unlock(&(dbp->fd_lock));
    return FD_ERROR_VALUE;}
  retval=mysql_stmt_prepare(stmt,FD_STRDATA(string),FD_STRLEN(string));
  if (retval==RETVAL_OK) {
    n_cols=init_stmt_results(stmt,&outbound,&colnames,&isnullbuf);
    retval=mysql_stmt_execute(stmt);}
  if (retval==RETVAL_OK) retval=mysql_stmt_store_result(stmt);
  if (retval==RETVAL_OK) u8_mutex_unlock(&(dbp->fd_lock));
  if (retval==RETVAL_OK) {
    if (n_cols==0) results=FD_VOID;
    else results=get_stmt_values(stmt,colinfo,n_cols,colnames,
                                 outbound,isnullbuf);}
  /* Clean up */
  i=0; while (i<n_cols) {
    if (outbound[i].buffer) {
      u8_free(outbound[i].buffer);
      outbound[i].buffer=NULL;}
    if (outbound[i].length) {
      u8_free(outbound[i].length);
      outbound[i].length=NULL;}
    i++;}
  if (outbound) u8_free(outbound);
  if (colnames) u8_free(colnames);
  if (isnullbuf) u8_free(isnullbuf);
  fd_decref(colinfo);
  mysql_stmt_close(stmt);

  if (retval) {
    mysqlerrno=mysql_stmt_errno(stmt);
    if ((reconn>0)&&(NEED_RESTART(mysqlerrno))) {
      int rv=restart_connection(dbp);
      if (rv != RETVAL_OK) {
        u8_mutex_unlock(&(dbp->fd_lock));
        fd_decref(results);
        return FD_ERROR_VALUE;}
      u8_mutex_unlock(&(dbp->fd_lock));
      return mysqlexec(dbp,string,colinfo_arg,reconn-1);}
    else {
      const char *errmsg=mysql_stmt_error(stmt);
      u8_seterr(MySQL_Error,"mysqlexec",u8_strdup(errmsg));
      u8_mutex_unlock(&(dbp->fd_lock));
      return FD_ERROR_VALUE;}}
  else return results;
}

static fdtype mysqlexechandler
  (struct FD_EXTDB *extdb,fdtype string,fdtype colinfo)
{
  if (extdb->dbhandler==&mysql_handler)
    return mysqlexec((fd_mysql)extdb,string,colinfo,1);
  else return fd_type_error("MYSQL EXTDB","mysqlexechandler",(fdtype)extdb);
}

/* MYSQL procs */

static int default_lazy_init=0;

static fdtype mysqlmakeproc
  (struct FD_MYSQL *dbp,
   u8_string stmt,int stmt_len,
   fdtype colinfo,int n,fdtype *ptypes)
{
  MYSQL *db=dbp->db; int retval=0;
  struct FD_MYSQL_PROC *dbproc=u8_alloc(struct FD_MYSQL_PROC);
  unsigned int lazy_init=0;
  fdtype lazy_opt=fd_getopt(dbp->options,lazy_symbol,FD_VOID);
  if (FD_VOIDP(lazy_opt))
    lazy_init=default_lazy_init;
  else if (FD_TRUEP(lazy_opt))
    lazy_init=1;
  else lazy_init=0;
  fd_decref(lazy_opt);

  memset(dbproc,0,sizeof(struct FD_MYSQL_PROC));

  FD_INIT_FRESH_CONS(dbproc,fd_extdb_proc_type);

  /* Set up fields for EXTDBPROC */
  dbproc->dbhandler=&mysql_handler;
  dbproc->db=(fdtype)dbp; fd_incref(dbproc->db);
  dbproc->spec=u8_strdup(dbp->spec);
  dbproc->qtext=_memdup(stmt,stmt_len+1); /* include space for NUL */
  colinfo=dbproc->colinfo=merge_colinfo(dbp,colinfo);
  u8_mutex_init(&(dbproc->fd_lock));

  /* Set up MYSQL specific fields */
  dbproc->mysqldb=db;
  dbproc->fdbptr=dbp;
  dbproc->stmt_string=u8_strdup(stmt);
  dbproc->stmt_len=stmt_len;

  /* Set up fields for the function object itself */
  dbproc->filename=dbproc->spec; dbproc->name=dbproc->qtext;
  dbproc->ndcall=0; dbproc->xcall=1; dbproc->arity=-1;
  dbproc->min_arity=0;
  dbproc->handler.xcalln=callmysqlproc;

  /* Register the procedure on the database's list */
  fd_register_extdb_proc((struct FD_EXTDB_PROC *)dbproc);

  /* This indicates that the procedure hasn't been initialized */
  dbproc->n_cols=-1;

  dbproc->n_params=n; {
    fdtype *init_ptypes=u8_alloc_n(n,fdtype);
    int i=0; while (i<n) {
      init_ptypes[i]=fd_incref(ptypes[i]); i++;}
    dbproc->paramtypes=init_ptypes;}

  if (lazy_init) {
    dbproc->stmt=NULL;
    dbproc->need_init=1;}
  else {
    u8_mutex_lock(&(dbp->fd_lock));
    retval=init_mysqlproc(dbp,dbproc);
    u8_mutex_unlock(&(dbp->fd_lock));}
  if (retval<0) return FD_ERROR_VALUE;
  else return FDTYPE_CONS(dbproc);
}

/* This is the handler stored in the method table */
static fdtype mysqlmakeprochandler
  (struct FD_EXTDB *extdb,
   u8_string stmt,int stmt_len,
   fdtype colinfo,int n,fdtype *ptypes)
{
  if (extdb->dbhandler==&mysql_handler)
    return mysqlmakeproc((fd_mysql)extdb,stmt,stmt_len,colinfo,n,ptypes);
  else return fd_type_error("MYSQL EXTDB","mysqlmakeprochandler",(fdtype)extdb);
}

/* Various MYSQLPROC functions */

static int init_mysqlproc(FD_MYSQL *dbp,struct FD_MYSQL_PROC *dbproc)
{
  /* This assumes that both dbp and dpbroc have been locked.  */
  MYSQL *db=dbp->db;
  int retval=0, n_cols=dbproc->n_cols, n_params;
  u8_condition error_phase="init_mysqlproc";
  u8_log(LOG_DEBUG,"MySQLproc/init","%lx: %s",
         (unsigned long long)dbproc,
         dbproc->stmt_string);

  /* Reinitialize these structures in case there have been schema
     changes. */
  if (dbproc->outbound) {
    MYSQL_BIND *outbound=dbproc->outbound;
    int i=0; while (i<n_cols) {
      if (outbound[i].buffer) {
        u8_free(outbound[i].buffer);
        outbound[i].buffer=NULL;}
      if (outbound[i].length) {
        u8_free(outbound[i].length);
        outbound[i].length=NULL;}
      i++;}
    if (outbound) u8_free(outbound);
    dbproc->outbound=NULL;}
  if (dbproc->colnames) {
    u8_free(dbproc->colnames); 
    dbproc->colnames=NULL;}
  if (dbproc->isnull) {
    u8_free(dbproc->isnull);
    dbproc->isnull=NULL;}
  if (dbproc->bindbuf) {
    u8_free(dbproc->bindbuf);
    dbproc->bindbuf=NULL;}

  /* Close any existing statement */
  if (dbproc->stmt) {
    error_phase="init_mysqlproc/close_existing";
    retval=mysql_stmt_close(dbproc->stmt);
    dbproc->stmt=NULL;}

  if (retval==RETVAL_OK) {
    error_phase="init_mysqlproc/stmt_init";
    dbproc->stmt=mysql_stmt_init(db);
    if (dbproc->stmt) {
      error_phase="init_mysqlproc/stmt_prepare";
      retval=mysql_stmt_prepare
        (dbproc->stmt,dbproc->stmt_string,dbproc->stmt_len);}}

  if (retval) {
    int mysqlerrno=mysql_stmt_errno(dbproc->stmt);
    const char *errmsg=mysql_stmt_error(dbproc->stmt);
    u8_log(LOG_WARN,error_phase,"%s: %s",dbproc->stmt_string,errmsg);
    return retval;}

  n_cols=init_stmt_results(dbproc->stmt,
                           &(dbproc->outbound),
                           &(dbproc->colnames),
                           &(dbproc->isnull));

  if (n_cols<0) return -1;

  n_params=mysql_stmt_param_count(dbproc->stmt);

  if (n_params==dbproc->n_params) {}
  else {
    fdtype *init_ptypes=dbproc->paramtypes, *ptypes=u8_alloc_n(n_params,fdtype);
    int i=0, init_n=dbproc->n_params; while ((i<init_n)&&(i<n_params)) {
      ptypes[i]=init_ptypes[i]; i++;}
    while (i<n_params) ptypes[i++]=FD_VOID;
    while (i<init_n) {
      /* We make this a warning rather than an error, because we don't
         need that information.  Note that this should only generate a warning
         the first time that the statement is created. */
      fdtype ptype=init_ptypes[i++];
      if (FD_VOIDP(ptype)) {}
      else u8_log(LOG_WARN,UnusedType,
                  "Parameter type %hq is not used for %s",ptype,dbproc->qtext);
      fd_decref(ptype);}
    dbproc->paramtypes=ptypes; dbproc->n_params=n_params;
    if (init_ptypes) u8_free(init_ptypes);}

  /* Check that the number of returned columns has not changed
     (this could happen if there were schema changes) */
  if ((dbproc->n_cols>=0)&&(n_cols!=dbproc->n_cols)) {
    u8_log(LOG_WARN,ServerReset,
           "The number of columns for query '%s' on %s (%s) has changed",
           dbproc->qtext,dbp->spec,dbp->info);}
  dbproc->n_cols=n_cols;

  /* Check that the number of parameters has not changed
     (this might happen if there were schema changes) */
  dbproc->min_arity=n_params;

  if (n_params) {
    dbproc->inbound=u8_alloc_n(n_params,MYSQL_BIND);
    memset(dbproc->inbound,0,sizeof(MYSQL_BIND)*n_params);
    dbproc->bindbuf=u8_alloc_n(n_params,union BINDBUF);
    memset(dbproc->bindbuf,0,sizeof(union BINDBUF)*n_params);}

  dbproc->need_init=0;

  return RETVAL_OK;
}

static void recycle_mysqlproc(struct FD_EXTDB_PROC *c)
{
  struct FD_MYSQL_PROC *dbproc=(struct FD_MYSQL_PROC *)c;
  int i, lim, rv;
  fd_release_extdb_proc(c);
  if (dbproc->stmt) {
    if ((rv=mysql_stmt_close(dbproc->stmt))) {
      int mysqlerrno=mysql_stmt_errno(dbproc->stmt);
      const char *errmsg=mysql_stmt_error(dbproc->stmt);
      dbproc->stmt=NULL;
      u8_log(LOG_WARN,MySQL_Error,"Error (%d:%d) closing statement %s: %s",
             rv,mysqlerrno,dbproc->stmt_string,errmsg);}}
  fd_decref(dbproc->colinfo);

  if (dbproc->bindbuf) u8_free(dbproc->bindbuf);
  if (dbproc->isnull) u8_free(dbproc->isnull);
  if (dbproc->inbound) u8_free(dbproc->inbound);
  if (dbproc->colnames) u8_free(dbproc->colnames);

  if (dbproc->outbound) {
    i=0; lim=dbproc->n_cols; while (i<lim) {
      if (dbproc->outbound[i].buffer) {
        u8_free(dbproc->outbound[i].buffer);
        dbproc->outbound[i].buffer=NULL;}
      if (dbproc->outbound[i].length) {
        u8_free(dbproc->outbound[i].length);
        dbproc->outbound[i].length=NULL;}
      i++;}
    u8_free(dbproc->outbound);}

  if (dbproc->paramtypes) {
    i=0; lim=dbproc->n_params; while (i< lim) {
      fd_decref(dbproc->paramtypes[i]); i++;}
    u8_free(dbproc->paramtypes);}

  u8_free(dbproc->spec);
  u8_free(dbproc->qtext);
  u8_free(dbproc->stmt_string);

  u8_mutex_destroy(&(dbproc->fd_lock));

  fd_decref(dbproc->db);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Actually calling a MYSQL proc */

static fdtype applymysqlproc(struct FD_FUNCTION *,int,fdtype *,int);

static fdtype callmysqlproc(struct FD_FUNCTION *fn,int n,fdtype *args){
  return applymysqlproc(fn,n,args,7);}

static fdtype applymysqlproc(struct FD_FUNCTION *fn,int n,fdtype *args,
                             int reconn)
{
  struct FD_MYSQL_PROC *dbproc=(struct FD_MYSQL_PROC *)fn;
  struct FD_MYSQL *dbp=
    FD_GET_CONS(dbproc->db,fd_extdb_type,struct FD_MYSQL *);
  int n_params=dbproc->n_params;
  MYSQL_BIND *inbound=dbproc->inbound;
  union BINDBUF *bindbuf=dbproc->bindbuf;
  int retry=0, reterr=0;

  MYSQL_TIME *mstimes[4]; int n_mstimes=0;

  /* Argbuf stores objects we consed in the process of
     converting application objects to SQLish values. */
  fdtype *ptypes=dbproc->paramtypes;
  fdtype values=FD_EMPTY_CHOICE;
  int i=0, n_bound=0;
  /* *retval* tracks the most recent operation and tells whether to keep going.
     The other x*vals* identify the retvals for particular phases, to help
     produce more helpful error messages.
     A ZERO VALUE MEANS OK. */
  int retval=0, bretval=0, eretval=0, sretval=0, iretval=0;
  int proclock=0, dblock=0;
  volatile unsigned int mysqlerrno=0;
  const char *mysqlerrmsg=NULL;
  fdtype _argbuf[4], *argbuf=_argbuf;

  u8_mutex_lock(&(dbproc->fd_lock)); proclock=1;

  u8_log(LOG_DEBUG,"MySQLproc/call","%lx: %s",
         (unsigned long long)dbproc,
         dbproc->stmt_string);

  /* Initialize it if it needs it */
  if (dbproc->need_init) {
    u8_lock_mutex(&(dbp->fd_lock));
    retval=iretval=init_mysqlproc(dbp,dbproc);
    u8_unlock_mutex(&(dbp->fd_lock));}
  if (retval==RETVAL_OK) {
    n_params=dbproc->n_params;
    inbound=dbproc->inbound;
    bindbuf=dbproc->bindbuf;
    ptypes=dbproc->paramtypes;

    /* We check arity here because the procedure may not have been initialized
       (and determined its arity) during the arity checking done by APPLY. */
    if (n!=n_params) {
      u8_unlock_mutex(&(dbproc->fd_lock));
      if (n<n_params)
        return fd_err(fd_TooFewArgs,"fd_dapply",fn->name,FD_VOID);
      else return fd_err(fd_TooManyArgs,"fd_dapply",fn->name,FD_VOID);}

    if (n_params>4) argbuf=u8_alloc_n(n_params,fdtype);
    /* memset(argbuf,0,sizeof(fdtype)*n_params); */

    /* Initialize the input parameters from the arguments.
       None of this accesses the database, so we don't lock it yet.*/
    while (i<n_params) {
      fdtype arg=args[i];
      /* Use the ptypes to map application arguments into SQL. */
      if (FD_VOIDP(ptypes[i])) argbuf[i]=FD_VOID;
      else if ((FD_OIDP(arg)) && (FD_OIDP(ptypes[i]))) {
        FD_OID addr=FD_OID_ADDR(arg);
        FD_OID base=FD_OID_ADDR(ptypes[i]);
        unsigned long long offset=FD_OID_DIFFERENCE(addr,base);
        argbuf[i]=arg=FD_INT(offset);}
      else if (FD_APPLICABLEP(ptypes[i])) {
        argbuf[i]=arg=fd_apply(ptypes[i],1,&arg);}
      else if (FD_TRUEP(ptypes[i])) {
        struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
        fd_unparse(&out,arg);
        argbuf[i]=fd_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
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
        inbound[i].buffer=(u8_byte *)FD_STRDATA(arg);
        inbound[i].buffer_length=FD_STRLEN(arg);
        inbound[i].length=&(inbound[i].buffer_length);}
      else if (FD_SYMBOLP(arg)) {
        u8_string pname=FD_SYMBOL_NAME(arg);
        inbound[i].buffer_type=MYSQL_TYPE_STRING;
        inbound[i].buffer=(u8_byte *)pname;
        inbound[i].buffer_length=strlen(pname);
        inbound[i].length=&(inbound[i].buffer_length);}
      else if (FD_PACKETP(arg)) {
        inbound[i].buffer_type=MYSQL_TYPE_BLOB;
        inbound[i].buffer=(u8_byte *)FD_PACKET_DATA(arg);
        inbound[i].buffer_length=FD_PACKET_LENGTH(arg);
        inbound[i].length=&(inbound[i].buffer_length);}
      else if (FD_PRIM_TYPEP(arg,fd_uuid_type)) {
        struct FD_UUID *uuid=FD_GET_CONS(arg,fd_uuid_type,struct FD_UUID *);
        inbound[i].buffer_type=MYSQL_TYPE_BLOB;
        inbound[i].buffer=&(uuid->fd_uuid16);
        inbound[i].buffer_length=16;
        inbound[i].length=NULL;}
      else if (FD_PRIM_TYPEP(arg,fd_timestamp_type)) {
        struct FD_TIMESTAMP *tm=
          FD_GET_CONS(arg,fd_timestamp_type,struct FD_TIMESTAMP *);
        MYSQL_TIME *mt=u8_alloc(MYSQL_TIME);
        struct U8_XTIME *xt=&(tm->fd_u8xtime), gmxtime; time_t tick;
        memset(mt,0,sizeof(MYSQL_TIME));
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
        while (j<i) {fd_decref(argbuf[j]); j++;}
        if (argbuf!=_argbuf) u8_free(argbuf);
        if (dblock) {u8_mutex_unlock(&(dbp->fd_lock)); dblock=0;}
        if (proclock) {u8_mutex_unlock(&(dbproc->fd_lock)); proclock=0;}
        return FD_ERROR_VALUE;}
      else if (FD_TRUEP(ptypes[i])) {
        /* If the ptype is #t, try to convert it into a string,
           and catch it if you have an error. */
        struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
        if (fd_unparse(&out,arg)<0) {
          int j=0;
          fd_seterr(MySQL_NoConvert,"callmysqlproc",
                    u8_strdup(dbproc->qtext),fd_incref(arg));
          while (j<i) {fd_decref(argbuf[i]); i++;}
          j=0; while (j<n_mstimes) {u8_free(mstimes[j]); j++;}
          if (argbuf!=_argbuf) u8_free(argbuf);
          if (dblock) {u8_mutex_unlock(&(dbp->fd_lock)); dblock=0;}
          if (proclock)  {u8_mutex_unlock(&(dbproc->fd_lock)); proclock=0;}
          return FD_ERROR_VALUE;}
        else {
          u8_byte *as_string=out.u8_outbuf;
          int stringlen=out.u8_write-out.u8_outbuf;
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
        j=0; while (j<n_mstimes) {u8_free(mstimes[j]); j++;}
        j=0; while (j<i) {fd_decref(argbuf[j]); j++;}
        if (argbuf!=_argbuf) u8_free(argbuf);
        if (dblock) {u8_mutex_unlock(&(dbp->fd_lock)); dblock=0;}
        if (proclock)  {u8_mutex_unlock(&(dbproc->fd_lock)); proclock=0;}
        return FD_ERROR_VALUE;}
      i++;}}
  n_bound=i;

  if (dbproc->need_init) {
    /* If there's been a restart while we were binding, act as though
       we've failed and retry the connection at the end. */
    retval=-1; retry=1;}
  else {
    /* Otherwise, tell MYSQL that the parameters are ready. */
    retval=bretval=mysql_stmt_bind_param(dbproc->stmt,inbound);}

  if (retval==RETVAL_OK) {
    /* Lock the connection itself before executing. ??? Why? */
    u8_mutex_lock(&(dbp->fd_lock)); dblock=1;
    if (dbproc->need_init) {
      /* Once more, if there's been a restart before we get the lock,
         set retry=1 and pretend to fail.  Once we have the lock, we
         no longer have to worry about restarts. */
      retval=-1; retry=1;}
    else retval=eretval=mysql_stmt_execute(dbproc->stmt);}

  /* Read all the values at once */
  if (retval==RETVAL_OK)
    retval=sretval=mysql_stmt_store_result(dbproc->stmt);

  if (retval==RETVAL_OK) {
    /* If everything went fine, we release the database lock now. */
    if (dblock) {u8_mutex_unlock(&(dbp->fd_lock)); dblock=0;}}

  /* Now convert the MYSQL results into LISP.  We don't check for
     connection errors because we've stored the whole result locally. */
  if (retval==RETVAL_OK) {
    if (dbproc->n_cols) {
      values=get_stmt_values(dbproc->stmt,
                             dbproc->colinfo,dbproc->n_cols,dbproc->colnames,
                             dbproc->outbound,dbproc->isnull);
      mysql_stmt_free_result(dbproc->stmt);}
    else {
      /* We could possibly do something with this */
      int U8_MAYBE_UNUSED rows=mysql_stmt_affected_rows(dbproc->stmt);
      values=FD_VOID;}}

  if (retval!=RETVAL_OK) {
    /* Log any errors (even ones we're going to handle) */
    mysqlerrno=mysql_stmt_errno(dbproc->stmt);
    mysqlerrmsg=mysql_stmt_error(dbproc->stmt);
    u8_log(LOG_WARN,
           ((sretval)?("callmysqlproc/store"):
            (eretval)?("callmysqlproc/exec"):
            (bretval)?("callmysqlproc/bind"):
            ("callmysqlproc")),
           "MYSQL error '%s' (%d) for %s at %s",
           mysqlerrmsg,mysqlerrno,dbproc->stmt_string,dbp->spec);
    /* mysql_stmt_reset(dbproc->stmt); */

    /* Figure out if we're going to retry */
    if ((reconn>0)&&((retry)||(NEED_RESTART(mysqlerrno))))
      retry=1;
    else if ((reconn>0)&&(NEED_RESET(mysqlerrno))) {
      dbproc->need_init=1; retry=1;}
    else retry=0;
    if ((retval)&&(!(retry))) reterr=1;}

  /* Clean up */
  i=0; while (i<n_mstimes) {u8_free(mstimes[i]); i++;}
  i=0; while (i<n_bound) {
    fdtype arg=argbuf[i++];
    if (arg) fd_decref(arg);}
  if (argbuf!=_argbuf) u8_free(argbuf);

  if (reterr) {
    if (dblock) {u8_mutex_unlock(&(dbp->fd_lock)); dblock=0;}
    if (proclock) {u8_mutex_unlock(&(dbproc->fd_lock)); proclock=0;}
    if (!(bretval==RETVAL_OK))
      u8_seterr(MySQL_Error,"mysqlproc/bind",
                u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
    else if (!(eretval==RETVAL_OK))
      u8_seterr(MySQL_Error,"mysqlproc/exec",
                u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
    else if (!(sretval==RETVAL_OK))
      u8_seterr(MySQL_Error,"mysqlproc/store",
                u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
    else u8_seterr(MySQL_Error,"mysqlproc",
                   u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
    return FD_ERROR_VALUE;}
  else if (retry) {
    if (!(dblock)) {u8_lock_mutex(&(dbp->fd_lock)); dblock=1;}
    if ((dbproc->need_init)&&(!(NEED_RESTART(mysqlerrno)))) {
      retval=init_mysqlproc(dbp,dbproc);
      if (retval!=RETVAL_OK)
        retval=restart_connection(dbp);}
    else retval=restart_connection(dbp);
    if (dblock) {u8_mutex_unlock(&(dbp->fd_lock)); dblock=0;}
    if (proclock) {u8_mutex_unlock(&(dbproc->fd_lock)); proclock=0;}
    if (retval!=RETVAL_OK) {
      mysqlerrno=mysql_errno(dbp->db);
      mysqlerrmsg=mysql_error(dbp->db);
      u8_seterr(MySQL_Error,"mysqlproc/restart",
                u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
      return FD_ERROR_VALUE;}
    else return applymysqlproc(fn,n,args,reconn-1);}
  else {
    if (dblock) {u8_mutex_unlock(&(dbp->fd_lock)); dblock=0;}
    if (proclock) {u8_mutex_unlock(&(dbproc->fd_lock)); proclock=0;}
    return values;}
}

/* Initialization */

static long long int mysql_initialized=0;

static struct FD_EXTDB_HANDLER mysql_handler=
  {"mysql",NULL,NULL,NULL,NULL};

int first_call=1;

static int init_thread_for_mysql()
{
  u8_log(LOG_DEBUG,"MYSQL","Initializing thread for MYSQL");
  if (first_call) {
    first_call=0; my_init();}
  else mysql_thread_init();
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

  u8_register_threadinit(init_thread_for_mysql);
  u8_register_threadexit(cleanup_thread_for_mysql);

  module=fd_new_module("MYSQL",0);

#if FD_THREADS_ENABLED
  u8_mutex_init(&mysql_connect_lock);
#endif

  mysql_handler.execute=mysqlexechandler;
  mysql_handler.makeproc=mysqlmakeprochandler;
  mysql_handler.recycle_extdb=recycle_mysqldb;
  mysql_handler.recycle_extdb_proc=recycle_mysqlproc;

  fd_register_extdb_handler(&mysql_handler);

  fd_defn(module,
          fd_make_cprim6x("MYSQL/OPEN",open_mysql,1,
                          fd_string_type,FD_VOID,
                          fd_string_type,FD_VOID,
                          -1,FD_VOID,
                          fd_string_type,FD_VOID,
                          -1,FD_VOID,
                          -1,FD_VOID));
  fd_defn(module,
          fd_make_cprim2x("MYSQL/REFRESH",refresh_mysqldb,1,
                          fd_extdb_type,FD_VOID,
                          -1,FD_VOID));

  mysql_initialized=u8_millitime();

  boolean_symbol=fd_intern("BOOLEAN");
  merge_symbol=fd_intern("%MERGE");
  noempty_symbol=fd_intern("%NOEMPTY");
  sorted_symbol=fd_intern("%SORTED");

  port_symbol=fd_intern("PORT");
  reconnect_symbol=fd_intern("RECONNECT");
  sslca_symbol=fd_intern("SSLCA");
  sslcert_symbol=fd_intern("SSLCERT");
  sslkey_symbol=fd_intern("SSLKEY");
  sslcadir_symbol=fd_intern("SSLCADIR");
  sslciphers_symbol=fd_intern("SSLCIPHERS");

  timeout_symbol=fd_intern("TIMEOUT");
  connect_timeout_symbol=fd_intern("CONNECT-TIMEOUT");
  read_timeout_symbol=fd_intern("READ-TIMEOUT");
  write_timeout_symbol=fd_intern("WRITE-TIMEOUT");
  lazy_symbol=fd_intern("LAZYPROCS");

  fd_finish_module(module);

  fd_register_config
    ("MYSQL:LAZYPROCS",
     "whether MYSQL should delay initializing dbprocs (statements)",
     fd_boolconfig_get,fd_boolconfig_set,
     &default_lazy_init);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
