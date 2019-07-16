/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* mysql.c
   This implements Kno bindings to mysql.
   Copyright (C) 2007-2019 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/numbers.h"
#include "kno/sequences.h"
#include "kno/texttools.h"
#include "kno/sqldb.h"
#include "kno/cprims.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8timefns.h>
#include <libu8/u8crypto.h>

#include <mysql/mysql.h>
#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>

extern my_bool my_init(void);

static lispval ssl_symbol, sslca_symbol, sslcert_symbol, sslkey_symbol, sslcadir_symbol;
static lispval sslciphers_symbol, port_symbol, reconnect_symbol;
static lispval timeout_symbol, connect_timeout_symbol;
static lispval read_timeout_symbol, write_timeout_symbol, lazy_symbol;

static lispval boolean_symbol;
u8_condition ServerReset=_("MYSQL server reset");
u8_condition UnusedType=_("MYSQL unused parameter type");

static u8_string dupstring(lispval x)
{
  if (KNO_VOIDP(x)) return NULL;
  else if (KNO_STRINGP(x))
    return u8_strdup(KNO_CSTRING(x));
  else if ((KNO_PACKETP(x))||(KNO_TYPEP(x,kno_secret_type))) {
    const unsigned char *data = KNO_PACKET_DATA(x);
    int len = KNO_PACKET_LENGTH(x);
    u8_byte *dup = u8_malloc(len+1);
    memcpy(dup,data,len);
    dup[len]='\0';
    return dup;}
  else return NULL;
}

#define NEED_RESTART(err)                               \
  ((err == CR_SERVER_GONE_ERROR) ||                       \
   (err == ER_NORMAL_SHUTDOWN) ||                         \
   (err == ER_SERVER_SHUTDOWN) ||                         \
   (err == ER_SHUTDOWN_COMPLETE) ||                       \
   (err == CR_SERVER_LOST))
#define NEED_RESET(err)                                 \
  ((err == CR_COMMANDS_OUT_OF_SYNC) ||                    \
   (err == CR_NO_PREPARE_STMT)||                          \
   (err == ER_UNKNOWN_STMT_HANDLER)||                     \
   (err == ER_NEED_REPREPARE))
#define RETVAL_OK 0

#ifndef MYSQL_RESTART_SLEEP
#define MYSQL_RESTART_SLEEP 3
#endif
#ifndef MYSQL_RESTART_WAIT
#define MYSQL_RESTART_WAIT 180
#endif

static int restart_sleep = MYSQL_RESTART_SLEEP;
static int restart_wait = MYSQL_RESTART_WAIT;

static u8_mutex mysql_connect_lock;

KNO_EXPORT int kno_init_mysql(void) KNO_LIBINIT_FN;
static struct KNO_SQLDB_HANDLER mysql_handler;
static lispval callmysqlproc(kno_function fn,int n,lispval *args);

typedef struct KNO_MYSQL {
  KNO_SQLDB_FIELDS;
  const char *mysql_hostname, *mysql_username, *mysql_passwd;
  const char *mysql_dbstring, *mysql_sockname;
  double mysql_startup, mysql_restarted;
  int mysql_restart_count;

  u8_mutex mysql_lock;

  /* Tracking this lets us check whether reconnects have happened */
  unsigned long mysql_thread_id;
  int mysql_portno, mysql_flags;
  MYSQL _mysqldb, *mysqldb;} KNO_MYSQL;
typedef struct KNO_MYSQL *kno_mysql;

/* This is used as a buffer for inbound bindings */
union MYSQL_VALBUF { double fval; void *ptr; long lval; long long llval;};

typedef struct KNO_MYSQL_PROC {
  KNO_SQLPROC_FIELDS;
  struct KNO_MYSQL *extbptr;

  u8_mutex mysqlproc_lock;

  u8_string mysqlproc_string;
  size_t mysqlproc_string_len;
  int mysqlproc_n_cols, mysqlproc_needs_init;
  MYSQL *mysqldb; MYSQL_STMT *mysqlproc_stmt;
  lispval *mysqlproc_colnames;
  union MYSQL_VALBUF *mysqlproc_valbuf;
  my_bool *mysqlproc_isnullbuf;
  MYSQL_BIND *mysqlproc_inbound, *mysqlproc_outbound;} KNO_MYSQL_PROC;
typedef struct KNO_MYSQL_PROC *kno_mysql_proc;

static int init_mysqlproc(KNO_MYSQL *db,struct KNO_MYSQL_PROC *dbproc);

static u8_condition MySQL_Error=_("MySQL Error");
static u8_condition MySQL_NoConvert=_("Can't convert value to SQL");
static lispval merge_symbol, noempty_symbol, sorted_symbol;

static unsigned char *_memdup(const unsigned char *data,int len)
{
  unsigned char *duplicate = u8_alloc_n(len,unsigned char);
  memcpy(duplicate,data,len);
  return duplicate;
}

static lispval merge_colinfo(KNO_MYSQL *dbp,lispval colinfo)
{
  if (KNO_VOIDP(colinfo)) return kno_incref(dbp->sqldb_colinfo);
  else if (KNO_VOIDP(dbp->sqldb_colinfo))
    return kno_incref(colinfo);
  else if (colinfo == dbp->sqldb_colinfo)
    return kno_incref(colinfo);
  else if ((KNO_PAIRP(colinfo))&&
           ((KNO_CDR(colinfo)) == (dbp->sqldb_colinfo)))
    return kno_incref(colinfo);
  else {
    kno_incref(dbp->sqldb_colinfo); kno_incref(colinfo);
    return kno_conspair(colinfo,dbp->sqldb_colinfo);}
}

/* Connection operations */

static int setup_connection(struct KNO_MYSQL *dbp)
{
  int retval = 0;
  lispval options = dbp->sqldb_options;
  int timeout = -1, ctimeout = -1, rtimeout = -1, wtimeout = -1;
#ifdef MYSQL_OPT_SSL_MODE
  int ssl_mode = SSL_MODE_PREFERRED;
#endif
  U8_MAYBE_UNUSED char *option = NULL;
  if (!(KNO_VOIDP(options))) {
    lispval port = kno_getopt(options,port_symbol,KNO_VOID);
    lispval sslca = kno_getopt(options,sslca_symbol,KNO_VOID);
    lispval sslcert = kno_getopt(options,sslcert_symbol,KNO_VOID);
    lispval sslkey = kno_getopt(options,sslkey_symbol,KNO_VOID);
    lispval sslcadir = kno_getopt(options,sslcadir_symbol,KNO_VOID);
    lispval sslciphers = kno_getopt(options,sslciphers_symbol,KNO_VOID);
    lispval toval = kno_getopt(options,timeout_symbol,KNO_VOID);
    lispval ctoval = kno_getopt(options,connect_timeout_symbol,KNO_VOID);
    lispval rtoval = kno_getopt(options,read_timeout_symbol,KNO_VOID);
    lispval wtoval = kno_getopt(options,write_timeout_symbol,KNO_VOID);
    dbp->mysql_portno = ((KNO_VOIDP(port)) ? (0) : (kno_getint(port)));
#ifdef MYSQL_OPT_SSL_MODE
    lispval ssl = kno_getopt(options,ssl_symbol,KNO_VOID);
    if (KNO_FALSEP(ssl))
      ssl_mode=SSL_MODE_DISABLED;
    else if (KNO_TRUEP(ssl))
      ssl_mode=SSL_MODE_REQUIRED;
    else {}
#endif
    if (KNO_FIXNUMP(toval)) timeout = KNO_FIX2INT(toval);
    if (KNO_FIXNUMP(ctoval)) ctimeout = KNO_FIX2INT(ctoval);
    else ctimeout = timeout;
    if (KNO_FIXNUMP(rtoval)) rtimeout = KNO_FIX2INT(rtoval);
    else rtimeout = timeout;
    if (KNO_FIXNUMP(wtoval)) wtimeout = KNO_FIX2INT(wtoval);
    else wtimeout = timeout;
    if (!((KNO_VOIDP(sslca))&&(KNO_VOIDP(sslcert))&&
          (KNO_VOIDP(sslkey))&&(KNO_VOIDP(sslcadir))&&
          (KNO_VOIDP(sslciphers)))) {
      if (!((KNO_VOIDP(sslca))||(KNO_STRINGP(sslca))))
        return kno_type_error("SSLCA","open_mysql",sslca);
      else if (!((KNO_VOIDP(sslcert))||(KNO_STRINGP(sslcert))))
        return kno_type_error("SSLCERT","open_mysql",sslcert);
      else if (!((KNO_VOIDP(sslkey))||(KNO_STRINGP(sslkey))))
        return kno_type_error("SSLKEY","open_mysql",sslkey);
      else if (!((KNO_VOIDP(sslcadir))||(KNO_STRINGP(sslcadir))))
        return kno_type_error("SSLCADIR","open_mysql",sslcadir);
      else if (!((KNO_VOIDP(sslciphers))||(KNO_STRINGP(sslciphers))))
        return kno_type_error("SSLCIPHERS","open_mysql",sslciphers);
      else retval = mysql_ssl_set
             (dbp->mysqldb,
              ((KNO_VOIDP(sslkey))?(NULL):(KNO_CSTRING(sslkey))),
              ((KNO_VOIDP(sslcert))?(NULL):(KNO_CSTRING(sslcert))),
              ((KNO_VOIDP(sslca))?(NULL):(KNO_CSTRING(sslca))),
              ((KNO_VOIDP(sslcadir))?(NULL):(KNO_CSTRING(sslcadir))),
              ((KNO_VOIDP(sslciphers))?(NULL):(KNO_CSTRING(sslciphers))));}}
#ifdef MYSQL_OPT_SSL_MODE
  if (retval == RETVAL_OK) {
    retval = mysql_options(dbp->mysqldb,MYSQL_OPT_SSL_MODE,&ssl_mode);}
#endif
  if (retval == RETVAL_OK) {
    my_bool reconnect = 0;
    retval = mysql_options(dbp->mysqldb,MYSQL_OPT_RECONNECT,&reconnect);}
  if (retval == RETVAL_OK) {
    option="charset";
    retval = mysql_options(dbp->mysqldb,MYSQL_SET_CHARSET_NAME,"utf8");}
  if ((retval == RETVAL_OK)&&(ctimeout>0)) {
    unsigned int v = ctimeout; option="connect timeout";
    retval = mysql_options(dbp->mysqldb,MYSQL_OPT_CONNECT_TIMEOUT,(char *)&v);}
  if ((retval == RETVAL_OK)&&(rtimeout>0)) {
    unsigned int v = rtimeout; option="read timeout";
    retval = mysql_options(dbp->mysqldb,MYSQL_OPT_READ_TIMEOUT,(char *)&v);}
  if ((retval == RETVAL_OK)&&(wtimeout>0)) {
    unsigned int v = wtimeout; option="write timeout";
    retval = mysql_options(dbp->mysqldb,MYSQL_OPT_WRITE_TIMEOUT,(char *)&v);}

  if (retval!=RETVAL_OK) {
    const char *errmsg = mysql_error(&(dbp->_mysqldb)); kno_incref(options);
    kno_seterr(MySQL_Error,"open_mysql/setup_connection",errmsg,options);}
  return retval;
}

static int restart_connection(struct KNO_MYSQL *dbp)
{
  MYSQL *db = NULL;
  unsigned int retval = -1, waited = 0, thread_id = -1;
  while ((db == NULL)&&(waited<restart_wait)) {
    u8_lock_mutex(&mysql_connect_lock); {
      db = mysql_real_connect
        (dbp->mysqldb,dbp->mysql_hostname,
         dbp->mysql_username,dbp->mysql_passwd,dbp->mysql_dbstring,
         dbp->mysql_portno,dbp->mysql_sockname,dbp->mysql_flags);
      u8_unlock_mutex(&mysql_connect_lock);}
    if ((db == NULL)&&(mysql_errno(dbp->mysqldb) == CR_ALREADY_CONNECTED)) {
      u8_log(LOG_WARN,"mysql/restart_connection",
             "Already connected to %s (%s) with id=%d/%d",
             dbp->sqldb_spec,dbp->sqldb_info,
             dbp->mysql_thread_id,
             mysql_thread_id(dbp->mysqldb));
      db = dbp->mysqldb;}
    else if (db == NULL) {
      sleep(restart_sleep);
      waited = waited+restart_sleep;}
    else {}}

  if (!(db)) {
    const char *msg = mysql_error(db); int err = mysql_errno(dbp->mysqldb);
    lispval conn = (lispval) dbp; kno_incref(conn);
    u8_log(LOG_CRIT,"mysql/reconnect",
           "Failed after %ds to reconnect to MYSQL %s (%s), final err %s (%d)",
           waited,dbp->sqldb_spec,dbp->sqldb_info,msg,err);
    kno_seterr(MySQL_Error,"restart_connection",msg,conn);
    return -1;}
  else {
    int i = 0, n = dbp->sqldb_n_procs;
    struct KNO_MYSQL_PROC **procs = (KNO_MYSQL_PROC **)dbp->sqlprocs;
    u8_log(LOG_WARN,"mysql/reconnect",
           "Took %ds to reconnect to MYSQL %s (%s), thread_id=%d",
           waited,dbp->sqldb_spec,dbp->sqldb_info,dbp->mysql_thread_id);
    dbp->mysql_thread_id = thread_id = mysql_thread_id(db);
    u8_log(LOG_WARN,"myql/reconnect",
           "Reconnect #%d for MYSQL with %s (%s) rv=%d, thread_id=%d",
           dbp->mysql_restart_count+1,dbp->sqldb_spec,dbp->sqldb_info,retval,
           thread_id);

    /* Flag all the sqlprocs (prepared statements) as needing to be
       reinitialized. */
    while (i<n) {
      struct KNO_MYSQL_PROC *proc = procs[i++];
      proc->mysqlproc_needs_init = 1;
      proc->mysqlproc_stmt = NULL;}

    /* Some connection parameters may be reset, so we set them again */
    setup_connection(dbp);

    dbp->mysql_restart_count++; dbp->mysql_restarted = u8_elapsed_time();
    dbp->mysql_thread_id = mysql_thread_id(dbp->mysqldb);

    u8_log(LOG_WARN,"myql/reconnect",
           "Reconnect #%d MYSQL connection #%d with %s (%s)",
           dbp->mysql_restart_count,dbp->mysql_thread_id,
           dbp->sqldb_spec,dbp->sqldb_info);

    return RETVAL_OK;}
}

static int open_connection(struct KNO_MYSQL *dbp)
{
  int retval = 0;
  MYSQL *db;
  dbp->mysqldb = mysql_init(&(dbp->_mysqldb));
  if ((dbp->mysqldb) == NULL) {
    const char *errmsg = mysql_error(&(dbp->_mysqldb));
    u8_seterr(MySQL_Error,"mysql/open_connection",u8_strdup(errmsg));
    return -1;}
  if (retval) {
    const char *errmsg = mysql_error(&(dbp->_mysqldb));
    u8_seterr(MySQL_Error,"mysql/open_connection",u8_strdup(errmsg));
    mysql_close(dbp->mysqldb);
    u8_free(dbp);
    return -1;}

  retval = setup_connection(dbp);

  if (retval) {
    mysql_close(dbp->mysqldb);
    u8_free(dbp);
    return -1;}

  u8_lock_mutex(&mysql_connect_lock); {
    db = mysql_real_connect
      (dbp->mysqldb,dbp->mysql_hostname,dbp->mysql_username,dbp->mysql_passwd,
       dbp->mysql_dbstring,dbp->mysql_portno,dbp->mysql_sockname,dbp->mysql_flags);}
  u8_unlock_mutex(&mysql_connect_lock);

  if (db == NULL) {
    const char *errmsg = mysql_error(&(dbp->_mysqldb));
    u8_seterr(MySQL_Error,"mysql/open_connection",u8_strdup(errmsg));
    return -1;}
  else dbp->mysqldb = db;
  dbp->mysql_restart_count = 0;
  dbp->mysql_restarted = 0;
  dbp->mysql_startup = u8_elapsed_time();
  dbp->mysql_thread_id = mysql_thread_id(db);
  u8_lock_mutex(&(dbp->sqlproclock)); {
    int i = 0, n = dbp->sqldb_n_procs;
    struct KNO_MYSQL_PROC **procs = (KNO_MYSQL_PROC **)dbp->sqlprocs;
    while (i<n) procs[i++]->mysqlproc_needs_init = 1;
    u8_unlock_mutex(&(dbp->sqlproclock));
    return 1;}
}

static void recycle_mysqldb(struct KNO_SQLDB *c)
{
  struct KNO_MYSQL *dbp = (struct KNO_MYSQL *)c;
  int n_procs = dbp->sqldb_n_procs;
  lispval *toremove = u8_malloc(LISPVEC_BYTELEN(dbp->sqldb_n_procs)), *write = toremove;
  int i = 0;
  u8_lock_mutex(&(dbp->sqlproclock));
  while (i<n_procs) {
    struct KNO_SQLPROC *p = dbp->sqlprocs[--i];
    if (KNO_CONS_REFCOUNT(p)>1)
      u8_log(LOG_WARN,"freemysqldb",
             "dangling pointer to sqlproc %s on %s (%s)",
             p->sqldb_qtext,dbp->sqldb_spec,dbp->sqldb_info);
    *write++=(lispval)p;}
  u8_unlock_mutex(&(dbp->sqlproclock));
  u8_destroy_mutex(&(dbp->mysql_lock));
  mysql_close(dbp->mysqldb);
}

DEFPRIM2("mysql/refresh",refresh_mysqldb,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
             "`(MYSQL/REFRESH *dbptr* *flags*)` **undocumented**",
             kno_sqldb_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval refresh_mysqldb(lispval c,lispval flags)
{
  struct KNO_MYSQL *dbp = (struct KNO_MYSQL *)c;

  int retval = mysql_refresh(dbp->mysqldb,
                           REFRESH_GRANT|REFRESH_TABLES|REFRESH_HOSTS|
                           REFRESH_STATUS|REFRESH_THREADS);
  if (retval) {
    const char *errmsg = mysql_error(&(dbp->_mysqldb));
    return kno_err(MySQL_Error,"mysql/refresh",(u8_string)errmsg,((lispval)c));}
  else return KNO_VOID;
}

/* Opening connections */

/* Everything but the hostname and dbname is optional.
   In theory, we could have the dbname be optional, but for now we'll
   require it.  */
DEFPRIM6("mysql/open",open_mysql,KNO_MAX_ARGS(6)|KNO_MIN_ARGS(1),
 "`(MYSQL/OPEN *host* *dbname* *colmap* *user* *pass* *opts*)` **undocumented**",
 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID,
 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID,
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval open_mysql
  (lispval hostname,lispval dbname,lispval colinfo,
   lispval user,lispval password,
   lispval options)
{
  const char *host, *username, *passwd, *dbstring, *sockname, *spec;
  int portno = 0, flags = 0, retval;
  struct KNO_MYSQL *dbp = NULL;
  if (!((KNO_STRINGP(password))||(KNO_TYPEP(password,kno_secret_type))))
    return kno_type_error("string/secret","open_mysql",password);
  else dbp = u8_alloc(struct KNO_MYSQL);

  /* Initialize the cons (does a memset too) */
  KNO_INIT_FRESH_CONS(dbp,kno_sqldb_type);
  /* Initialize the MYSQL side of things (after the memset!) */
  /* If the hostname looks like a filename, we assume it's a Unix domain
     socket. */
  if (strchr(KNO_CSTRING(hostname),'/') == NULL) {
    host = u8_strdup(KNO_CSTRING(hostname));
    sockname = NULL;}
  else {
    sockname = u8_strdup(KNO_CSTRING(hostname));
    host = NULL;}
  /* Process the other arguments */
  spec = dupstring(hostname);
  dbstring = dupstring(dbname);
  username = dupstring(user);
  passwd = dupstring(password);
  flags = CLIENT_REMEMBER_OPTIONS;

  /* Initialize the other fields */
  dbp->sqldb_handler = &mysql_handler;
  dbp->mysql_hostname = host;
  dbp->mysql_username = username;
  dbp->mysql_passwd = passwd;
  dbp->mysql_dbstring = dbstring;
  dbp->mysql_sockname = sockname;
  dbp->mysql_portno = portno;
  dbp->mysql_flags = flags;
  dbp->sqldb_colinfo = kno_incref(colinfo);
  dbp->sqldb_spec = spec;
  dbp->sqldb_options = options; kno_incref(options);

  u8_init_mutex(&dbp->sqlproclock);
  u8_init_mutex(&dbp->mysql_lock);

  /* Prep the structure */
  retval = open_connection(dbp);
  if (retval<0) {
    recycle_mysqldb((struct KNO_SQLDB *)dbp);
    return KNO_ERROR_VALUE;}

  dbp->sqldb_info=
    u8_mkstring("%s;client %s",
                mysql_get_host_info(dbp->mysqldb),
                mysql_get_client_info());
  dbp->mysql_startup = u8_elapsed_time();

  return LISP_CONS(dbp);
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
    outval->buffer_type = MYSQL_TYPE_LONG;
    outval->buffer = u8_malloc(sizeof(unsigned long));
    outval->buffer_length = sizeof(unsigned long);
    outval->length = NULL;
    break;
  case MYSQL_TYPE_LONGLONG:
    outval->buffer_type = MYSQL_TYPE_LONGLONG;
    outval->buffer = u8_malloc(sizeof(unsigned long long));
    outval->buffer_length = sizeof(unsigned long long);
    outval->length = NULL;
    break;
  case MYSQL_TYPE_FLOAT:  case MYSQL_TYPE_DOUBLE:
    outval->buffer_type = MYSQL_TYPE_DOUBLE;
    outval->buffer = u8_malloc(sizeof(double));
    outval->buffer_length = sizeof(double);
    outval->length = NULL;
    break;
  case MYSQL_TYPE_STRING: case MYSQL_TYPE_VAR_STRING:
    outval->buffer_type = MYSQL_TYPE_STRING;
    outval->buffer = NULL;
    outval->buffer_length = 0;
    outval->length = u8_alloc(unsigned long);
    break;
  case MYSQL_TYPE_LONG_BLOB: case MYSQL_TYPE_TINY_BLOB:
  case MYSQL_TYPE_MEDIUM_BLOB: case MYSQL_TYPE_BLOB:
    outval->buffer_type = MYSQL_TYPE_BLOB;
    outval->buffer = NULL;
    outval->buffer_length = 0;
    outval->length = u8_alloc(unsigned long);
    break;
  case MYSQL_TYPE_TIME: case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_DATE: case MYSQL_TYPE_DATETIME:
    /* outval->buffer_type = outval->buffer_type; */
    outval->buffer = u8_alloc(MYSQL_TIME);
    outval->buffer_length = sizeof(MYSQL_TIME);
    outval->length = NULL;
    break;
  default:
    outval->buffer = NULL;
    outval->buffer_length = 0;
    outval->length = NULL;
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
static lispval outbound_get(MYSQL_STMT *stmt,MYSQL_BIND *bindings,
                           my_bool *isnull,int column)
{
  MYSQL_BIND *outval = &(bindings[column]);
  if (isnull[column]) return KNO_EMPTY_CHOICE;
  switch (outval->buffer_type) {
  case MYSQL_TYPE_LONG:
    if (outval->is_unsigned) {
      unsigned int intval = *((unsigned int *)(outval->buffer));
      return KNO_INT(intval);}
    else {
      int intval = *((int *)(outval->buffer));
      return KNO_INT(intval);}
  case MYSQL_TYPE_LONGLONG:
    if (outval->is_unsigned) {
      unsigned long long intval = *((unsigned long long *)(outval->buffer));
      return KNO_INT(intval);}
    else {
      long intval = *((long long *)(outval->buffer));
      return KNO_INT(intval);}
  case MYSQL_TYPE_DOUBLE: {
    double floval = *((double *)(outval->buffer));
    return kno_make_double(floval);}
  case MYSQL_TYPE_STRING: case MYSQL_TYPE_BLOB: {
    /* This is the only tricky case.  When we do the overall fetch,
       the TEXT/BLOCK columns are not retrieved, though their sizes
       are.  We then use mysqlproc_stmt_fetch_column to get the particular
       value once we have consed a buffer to use. */
    lispval value;
    int binary = ((outval->buffer_type) == MYSQL_TYPE_BLOB);
    int datalen = (*(outval->length)), buflen = datalen+((binary) ? (0) : (1));
    outval->buffer = u8_alloc_n(buflen,unsigned char);
    outval->buffer_length = buflen;
    mysql_stmt_fetch_column(stmt,outval,column,0);
    if (!(binary)) ((unsigned char *)outval->buffer)[datalen]='\0';
    if (binary)
      value = kno_make_packet(NULL,datalen,outval->buffer);
    else value = kno_make_string(NULL,datalen,outval->buffer);
    u8_free(outval->buffer);
    outval->buffer = NULL;
    outval->buffer_length = 0;
    return value;}
  case MYSQL_TYPE_DATETIME: case MYSQL_TYPE_DATE:
  case MYSQL_TYPE_TIMESTAMP: {
    struct U8_XTIME xt;
    MYSQL_TIME *mt = (MYSQL_TIME *)outval->buffer;
    if (!((mt->year)||(mt->month)||(mt->day)||
          (mt->hour)||(mt->minute)||(mt->second)))
      return KNO_EMPTY_CHOICE;
    memset(&xt,0,sizeof(xt));
    xt.u8_year = mt->year;
    xt.u8_mon = mt->month-1;
    xt.u8_mday = mt->day;
    if ((outval->buffer_type) == MYSQL_TYPE_DATE)
      xt.u8_prec = u8_day;
    else {
      xt.u8_prec = u8_second;
      xt.u8_hour = mt->hour;
      xt.u8_min = mt->minute;
      xt.u8_sec = mt->second;}
    u8_mktime(&xt);
    return kno_make_timestamp(&xt);}
  default:
    return KNO_FALSE;}
}

/* Getting outputs from a prepared statement */

/* This gets all the values returned by a statement, using
   prepared buffers for outbound variables and known column names. */
static lispval get_stmt_values
  (MYSQL_STMT *stmt,lispval colinfo,int n_cols,
   lispval *mysqlproc_colnames,MYSQL_BIND *outbound,my_bool *isnullbuf)
{
  lispval results = KNO_EMPTY_CHOICE;
  lispval mergefn = kno_getopt(colinfo,merge_symbol,KNO_VOID);
  lispval sortval = kno_getopt(colinfo,sorted_symbol,KNO_VOID);
  int noempty = kno_testopt(colinfo,noempty_symbol,KNO_VOID);
  int sorted = (!((KNO_FALSEP(sortval))||(KNO_VOIDP(sortval))));
  lispval _colmaps[16], *colmaps=
    ((n_cols>16) ? (u8_alloc_n(n_cols,lispval)) : (_colmaps));
  int i = 0, retval = mysql_stmt_fetch(stmt);
  int result_index = 0, n_results;
  if ((retval==0)||(retval == MYSQL_DATA_TRUNCATED)) {
    n_results = ((sorted)?(mysql_stmt_num_rows(stmt)):(0));
    if (sorted) results = kno_empty_vector(n_results);}
  while (i<n_cols) {
    colmaps[i]=kno_getopt(colinfo,mysqlproc_colnames[i],KNO_VOID); i++;}
  while ((retval==0) || (retval == MYSQL_DATA_TRUNCATED)) {
    /* For each iteration, we have a good value from the database,
       so we don't need to worry about lost connections for this
       loop, but we do need to worry about them afterwards. */
    lispval result = KNO_EMPTY_CHOICE;
    struct KNO_KEYVAL *kv = u8_alloc_n(n_cols,struct KNO_KEYVAL);
    int n_slots = 0;
    i = 0; while (i<n_cols) {
      lispval value = outbound_get(stmt,outbound,isnullbuf,i);
      /* Convert outbound variables via colmaps if specified. */
      if (KNO_EMPTY_CHOICEP(value)) /* NULL value, don't convert */
        if (noempty) {i++; continue;}
        else kv[n_slots].kv_val = value;
      else if (KNO_VOIDP(colmaps[i]))
        kv[n_slots].kv_val = value;
      else if (KNO_APPLICABLEP(colmaps[i])) {
        kv[n_slots].kv_val = kno_apply(colmaps[i],1,&value);
        kno_decref(value);}
      else if (KNO_OIDP(colmaps[i])) {
        if (KNO_STRINGP(value)) {
          kv[n_slots].kv_val = kno_parse(KNO_CSTRING(value));}
        else {
          KNO_OID base = KNO_OID_ADDR(colmaps[i]);
          int offset = kno_getint(value);
          if (offset<0) kv[n_slots].kv_val = value;
          else if (offset==0) {
            /* Some fields with OIDS use a zero value to indicate no
               value (empty choice), so we handle that here. */
            if (noempty) {i++; continue;}
            else kv[n_slots].kv_val = KNO_EMPTY_CHOICE;}
          else {
            KNO_OID baseplus = KNO_OID_PLUS(base,offset);
            kv[n_slots].kv_val = kno_make_oid(baseplus);}}
        kno_decref(value);}
      else if (colmaps[i]==KNO_TRUE)
        if (KNO_STRINGP(value)) {
          if (KNO_STRLEN(value))
            kv[n_slots].kv_val = kno_parse(KNO_CSTRING(value));
          else kv[n_slots].kv_val = KNO_EMPTY_CHOICE;
          kno_decref(value);}
        else kv[n_slots].kv_val = value;
      else if (KNO_TYPEP(colmaps[i],kno_secret_type)) {
        if ((KNO_STRINGP(value))||(KNO_PACKETP(value)))
          KNO_SET_CONS_TYPE(value,kno_secret_type);
        kv[n_slots].kv_val = value;}
      else if (KNO_TYPEP(colmaps[i],kno_uuid_type))
        if ((KNO_PACKETP(value))||(KNO_STRINGP(value))) {
          struct KNO_UUID *uuid = u8_alloc(struct KNO_UUID);
          const unsigned char *data=
            ((KNO_PACKETP(value))?
             (KNO_PACKET_DATA(value)):
             (KNO_CSTRING(value)));
          unsigned char *uuidbytes;
          KNO_INIT_CONS(uuid,kno_uuid_type);
          uuidbytes = uuid->uuid16;
          memcpy(uuidbytes,data,16);
          kno_decref(value);
          kv[n_slots].kv_val = LISP_CONS(uuid);}
        else kv[n_slots].kv_val = value;
      else if (KNO_EQ(colmaps[i],boolean_symbol)) {
        if (KNO_FIXNUMP(value)) {
          int ival = KNO_FIX2INT(value);
          if (ival) kv[n_slots].kv_val = KNO_TRUE;
          else kv[n_slots].kv_val = KNO_FALSE;}
        else {kv[n_slots].kv_val = value;}}
      else kv[n_slots].kv_val = value;
      kv[n_slots].kv_key = mysqlproc_colnames[i];
      if (KNO_ABORTP(kv[n_slots].kv_val)) {
        result = kv[n_slots].kv_val;
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
      result = KNO_EMPTY_CHOICE;
      if (kv) u8_free(kv);}
    else if (KNO_ABORTP(result)) {
      int j = 0; while (j<n_slots) {kno_decref(kv[j].kv_val); j++;}
      if (kv) u8_free(kv);}
    else if ((n_cols==1) && (KNO_TRUEP(mergefn))) {
      result = kv[0].kv_val;
      u8_free(kv);}
    else if ((KNO_SYMBOLP(mergefn))||(KNO_OIDP(mergefn))) {
      int j = 0; result = KNO_EMPTY_CHOICE;
      while (j<n_slots) {
        if (kv[j].kv_key == mergefn) {
          result = kv[j].kv_val;
          kno_decref(kv[j].kv_key);
          j++;}
        else {
          kno_decref(kv[j].kv_key);
          kno_decref(kv[j].kv_val);
          j++;}}
      u8_free(kv);}
    else if ((KNO_VOIDP(mergefn)) ||
             (KNO_FALSEP(mergefn)) ||
             (KNO_TRUEP(mergefn)))
      result = kno_init_slotmap(NULL,n_slots,kv);
    else if (!(KNO_APPLICABLEP(mergefn))) {
      result = kno_type_error("applicable","mysql/get_stmt_values",mergefn);}
    else {
      lispval tmp_slotmap = kno_init_slotmap(NULL,n_slots,kv);
      result = kno_apply(mergefn,1,&tmp_slotmap);
      kno_decref(tmp_slotmap);}
    if (KNO_ABORTP(result)) {
      kno_decref(results); results = result; break;}
    else if (sorted) {
      KNO_VECTOR_SET(results,result_index,result);
      result_index++;}
    else {KNO_ADD_TO_CHOICE(results,result);}
    retval = mysql_stmt_fetch(stmt);}

  i = 0; while (i<n_cols) {kno_decref(colmaps[i]); i++;}
  if (colmaps!=_colmaps) u8_free(colmaps);
  kno_decref(mergefn);
  kno_decref(sortval);

  if (KNO_ABORTP(results)) return results;
  else if (retval == MYSQL_NO_DATA) {
    if (sorted) {
      if (KNO_VECTORP(results)) return results;
      kno_decref(results);
      return kno_empty_vector(0);}
    else return results;}
  else if ((retval)&&(retval!=MYSQL_DATA_TRUNCATED)) {
    kno_decref(results); /* Free any partial results */
    /* An KNO_EOD return value indicates some kind of MySQL error */
    return KNO_EOD;}
  else return results;
}

/* This inits the vectors used when converting results from statement
   execution. */
static int init_stmt_results
  (MYSQL_STMT *stmt,MYSQL_BIND **outboundptr,lispval **mysqlproc_colnamesptr,
   my_bool **isnullbuf)
{
  int n_cols = mysql_stmt_field_count(stmt);
  if (n_cols>0) {
    lispval *mysqlproc_colnames = u8_alloc_n(n_cols,lispval);
    MYSQL_BIND *outbound = u8_alloc_n(n_cols,MYSQL_BIND);
    MYSQL_RES *metadata = mysql_stmt_result_metadata(stmt);
    MYSQL_FIELD *fields = ((metadata) ? (mysql_fetch_fields(metadata)) : (NULL));
    my_bool *nullbuf = u8_alloc_n(n_cols,my_bool);
    struct U8_OUTPUT out; u8_byte namebuf[128];
    int i = 0;
    memset(outbound,0,n_cols*sizeof(MYSQL_BIND));
    memset(nullbuf,0,n_cols*sizeof(my_bool));
    U8_INIT_STATIC_OUTPUT_BUF(out,128,namebuf);
    if (fields == NULL) {
      const char *errmsg = mysql_stmt_error(stmt);
      if (mysqlproc_colnames) u8_free(mysqlproc_colnames);
      if (outbound) u8_free(outbound);
      if (nullbuf) u8_free(nullbuf);
      if (metadata) mysql_free_result(metadata);
      u8_seterr(MySQL_Error,"mysql/init_stmt_results",u8_strdup(errmsg));
      return -1;}
    while (i<n_cols) {
      mysqlproc_colnames[i]=kno_intern(fields[i].name);
      /* Synchronize the signed and unsigned fields to get error handling. */
      if ((fields[i].flags)&(UNSIGNED_FLAG)) outbound[i].is_unsigned = 1;
      /* TEXT fields are confusingly labelled with MYSQL_TYPE_BLOB.
         We check the BINARY flag and set the corresponding MYSQL_BIND
         buffer type to MYSQL_TYPE_STRING or MYSQL_TYPE_BLOB as
         appropriate. */
      if (fields[i].type == MYSQL_TYPE_BLOB)
        if ((fields[i].flags)&(BINARY_FLAG))
          outbound[i].buffer_type = fields[i].type;
        else outbound[i].buffer_type = MYSQL_TYPE_STRING;
      else if (fields[i].type == MYSQL_TYPE_STRING)
        if (fields[i].charsetnr==63)
          outbound[i].buffer_type = MYSQL_TYPE_BLOB;
        else outbound[i].buffer_type = MYSQL_TYPE_STRING;
      else outbound[i].buffer_type = fields[i].type;
      outbound_setup(&(outbound[i]));
      outbound[i].is_null = &(nullbuf[i]);
      i++;}
    *mysqlproc_colnamesptr = mysqlproc_colnames;
    *outboundptr = outbound;
    *isnullbuf = nullbuf;
    mysql_stmt_bind_result(stmt,outbound);
    mysql_free_result(metadata); /* Hope this frees FIELDS */
    return n_cols;}
  else {
    *outboundptr = NULL;
    *mysqlproc_colnamesptr = NULL;
    *isnullbuf = NULL;
    return n_cols;}
}

/* Simple execution */

/* Straightforward execution of a single string.  We use prepared
   statements anyway (rather than mysql_fetch_row) because it lets us
   use raw C types consistent with prepared statements, rather than
   using the converted strings from mysql_fetch_row.  */
static lispval mysqlexec(struct KNO_MYSQL *dbp,lispval string,
                        lispval colinfo_arg,int reconn)
{
  MYSQL *db = dbp->mysqldb;
  MYSQL_BIND *outbound = NULL;
  lispval *mysqlproc_colnames = NULL;
  my_bool *isnullbuf = NULL;
  lispval colinfo = merge_colinfo(dbp,colinfo_arg), results = KNO_VOID;
  int retval, n_cols = 0, i = 0; unsigned int mysqlerrno = 0;
  MYSQL_STMT *stmt;
  u8_lock_mutex(&(dbp->mysql_lock));
  stmt = mysql_stmt_init(db);
  if (!(stmt)) {
    u8_log(LOG_WARN,"mysqlexec/stmt_init","Call to mysql_stmt_init failed");
    const char *errmsg = (mysql_error(&(dbp->_mysqldb)));
    u8_seterr(MySQL_Error,"mysqlexec",u8_strdup(errmsg));
    kno_decref(colinfo);
    u8_unlock_mutex(&(dbp->mysql_lock));
    return KNO_ERROR_VALUE;}
  retval = mysql_stmt_prepare(stmt,KNO_CSTRING(string),KNO_STRLEN(string));
  if (retval == RETVAL_OK) {
    n_cols = init_stmt_results(stmt,&outbound,&mysqlproc_colnames,&isnullbuf);
    retval = mysql_stmt_execute(stmt);}
  if (retval == RETVAL_OK) retval = mysql_stmt_store_result(stmt);
  if (retval == RETVAL_OK) u8_unlock_mutex(&(dbp->mysql_lock));
  if (retval == RETVAL_OK) {
    if (n_cols==0) results = KNO_VOID;
    else results = get_stmt_values(stmt,colinfo,n_cols,mysqlproc_colnames,
                                 outbound,isnullbuf);}
  /* Clean up */
  i = 0; while (i<n_cols) {
    if (outbound[i].buffer) {
      u8_free(outbound[i].buffer);
      outbound[i].buffer = NULL;}
    if (outbound[i].length) {
      u8_free(outbound[i].length);
      outbound[i].length = NULL;}
    i++;}
  if (outbound) u8_free(outbound);
  if (mysqlproc_colnames) u8_free(mysqlproc_colnames);
  if (isnullbuf) u8_free(isnullbuf);
  kno_decref(colinfo);
  mysql_stmt_close(stmt);

  if (retval) {
    mysqlerrno = mysql_stmt_errno(stmt);
    if ((reconn>0)&&(NEED_RESTART(mysqlerrno))) {
      int rv = restart_connection(dbp);
      if (rv != RETVAL_OK) {
        u8_unlock_mutex(&(dbp->mysql_lock));
        kno_decref(results);
        return KNO_ERROR_VALUE;}
      u8_unlock_mutex(&(dbp->mysql_lock));
      return mysqlexec(dbp,string,colinfo_arg,reconn-1);}
    else {
      const char *errmsg = mysql_stmt_error(stmt);
      u8_seterr(MySQL_Error,"mysqlexec",u8_strdup(errmsg));
      u8_unlock_mutex(&(dbp->mysql_lock));
      return KNO_ERROR_VALUE;}}
  else return results;
}

static lispval mysqlexechandler
  (struct KNO_SQLDB *sqldb,lispval string,lispval colinfo)
{
  if (sqldb->sqldb_handler== &mysql_handler)
    return mysqlexec((kno_mysql)sqldb,string,colinfo,1);
  else return kno_type_error("MYSQL SQLDB","mysqlexechandler",(lispval)sqldb);
}

/* MYSQL procs */

static int default_lazy_init = 0;

static lispval mysqlmakeproc
  (struct KNO_MYSQL *dbp,
   u8_string stmt,int stmt_len,
   lispval colinfo,int n,lispval *ptypes)
{
  MYSQL *db = dbp->mysqldb; int retval = 0;
  struct KNO_MYSQL_PROC *dbproc = u8_alloc(struct KNO_MYSQL_PROC);
  unsigned int lazy_init = 0;
  lispval lazy_opt = kno_getopt(dbp->sqldb_options,lazy_symbol,KNO_VOID);
  if (KNO_VOIDP(lazy_opt))
    lazy_init = default_lazy_init;
  else if (KNO_TRUEP(lazy_opt))
    lazy_init = 1;
  else lazy_init = 0;
  kno_decref(lazy_opt);

  memset(dbproc,0,sizeof(struct KNO_MYSQL_PROC));

  KNO_INIT_FRESH_CONS(dbproc,kno_sqlproc_type);

  /* Set up fields for SQLPROC */
  dbproc->sqldb_handler = &mysql_handler;
  dbproc->sqldbptr = (lispval)dbp; kno_incref(dbproc->sqldbptr);
  dbproc->sqldb_spec = u8_strdup(dbp->sqldb_spec);
  dbproc->sqldb_qtext=_memdup(stmt,stmt_len+1); /* include space for NUL */
  colinfo = dbproc->sqldb_colinfo = merge_colinfo(dbp,colinfo);
  u8_init_mutex(&(dbproc->mysqlproc_lock));

  /* Set up MYSQL specific fields */
  dbproc->mysqldb = db;
  dbproc->extbptr = dbp;
  dbproc->mysqlproc_string = u8_strdup(stmt);
  dbproc->mysqlproc_string_len = stmt_len;

  /* Set up fields for the function object itself */
  dbproc->fcn_filename = dbproc->sqldb_spec;
  dbproc->fcn_name = dbproc->sqldb_qtext;
  dbproc->fcn_ndcall = 0; dbproc->fcn_xcall = 1; dbproc->fcn_arity = -1;
  dbproc->fcn_min_arity = 0;
  dbproc->fcn_handler.xcalln = callmysqlproc;

  /* Register the procedure on the database's list */
  kno_register_sqlproc((struct KNO_SQLPROC *)dbproc);

  /* This indicates that the procedure hasn't been initialized */
  dbproc->mysqlproc_n_cols = -1;

  dbproc->fcn_n_params = n; {
    lispval *init_ptypes = u8_alloc_n(n,lispval);
    int i = 0; while (i<n) {
      init_ptypes[i]=kno_incref(ptypes[i]); i++;}
    dbproc->sqldb_paramtypes = init_ptypes;}

  if (lazy_init) {
    dbproc->mysqlproc_stmt = NULL;
    dbproc->mysqlproc_needs_init = 1;}
  else {
    u8_lock_mutex(&(dbp->mysql_lock));
    retval = init_mysqlproc(dbp,dbproc);
    u8_unlock_mutex(&(dbp->mysql_lock));}
  if (retval<0) return KNO_ERROR_VALUE;
  else return LISP_CONS(dbproc);
}

/* This is the handler stored in the method table */
static lispval mysqlmakeprochandler
  (struct KNO_SQLDB *sqldb,
   u8_string stmt,int stmt_len,
   lispval colinfo,int n,lispval *ptypes)
{
  if (sqldb->sqldb_handler== &mysql_handler)
    return mysqlmakeproc((kno_mysql)sqldb,stmt,stmt_len,colinfo,n,ptypes);
  else return kno_type_error("MYSQL SQLDB","mysqlmakeprochandler",
                            (lispval)sqldb);
}

/* Various MYSQLPROC functions */

static int init_mysqlproc(KNO_MYSQL *dbp,struct KNO_MYSQL_PROC *dbproc)
{
  /* This assumes that both dbp and dpbroc have been locked.  */
  MYSQL *db = dbp->mysqldb;
  int retval = 0, n_cols = dbproc->mysqlproc_n_cols, n_params;
  u8_condition error_phase="init_mysqlproc";
  u8_log(LOG_DEBUG,"MySQLproc/init","%lx: %s",
         KNO_LONGVAL(dbproc),
         dbproc->mysqlproc_string);

  /* Reinitialize these structures in case there have been schema
     changes. */
  if (dbproc->mysqlproc_outbound) {
    MYSQL_BIND *outbound = dbproc->mysqlproc_outbound;
    int i = 0; while (i<n_cols) {
      if (outbound[i].buffer) {
        u8_free(outbound[i].buffer);
        outbound[i].buffer = NULL;}
      if (outbound[i].length) {
        u8_free(outbound[i].length);
        outbound[i].length = NULL;}
      i++;}
    if (outbound) u8_free(outbound);
    dbproc->mysqlproc_outbound = NULL;}
  if (dbproc->mysqlproc_colnames) {
    u8_free(dbproc->mysqlproc_colnames);
    dbproc->mysqlproc_colnames = NULL;}
  if (dbproc->mysqlproc_isnullbuf) {
    u8_free(dbproc->mysqlproc_isnullbuf);
    dbproc->mysqlproc_isnullbuf = NULL;}
  if (dbproc->mysqlproc_valbuf) {
    u8_free(dbproc->mysqlproc_valbuf);
    dbproc->mysqlproc_valbuf = NULL;}

  /* Close any existing statement */
  if (dbproc->mysqlproc_stmt) {
    error_phase="init_mysqlproc/close_existing";
    retval = mysql_stmt_close(dbproc->mysqlproc_stmt);
    dbproc->mysqlproc_stmt = NULL;}

  if (retval == RETVAL_OK) {
    error_phase="init_mysqlproc/stmt_init";
    dbproc->mysqlproc_stmt = mysql_stmt_init(db);
    if (dbproc->mysqlproc_stmt) {
      error_phase="init_mysqlproc/stmt_prepare";
      retval = mysql_stmt_prepare
        (dbproc->mysqlproc_stmt,
         dbproc->mysqlproc_string,
         dbproc->mysqlproc_string_len);}}

  if (retval) {
    const char *errmsg = mysql_stmt_error(dbproc->mysqlproc_stmt);
    u8_log(LOG_WARN,error_phase,"%s: %s",dbproc->mysqlproc_string,errmsg);
    return retval;}

  n_cols = init_stmt_results(dbproc->mysqlproc_stmt,
                           &(dbproc->mysqlproc_outbound),
                           &(dbproc->mysqlproc_colnames),
                           &(dbproc->mysqlproc_isnullbuf));

  if (n_cols<0) return -1;

  n_params = mysql_stmt_param_count(dbproc->mysqlproc_stmt);

  if (n_params == dbproc->fcn_n_params) {}
  else {
    lispval *init_ptypes = dbproc->sqldb_paramtypes;
    lispval *ptypes = u8_alloc_n(n_params,lispval);
    int i = 0, init_n = dbproc->fcn_n_params; while ((i<init_n)&&(i<n_params)) {
      ptypes[i]=init_ptypes[i]; i++;}
    while (i<n_params) ptypes[i++]=KNO_VOID;
    while (i<init_n) {
      /* We make this a warning rather than an error, because we don't
         need that information.  Note that this should only generate a warning
         the first time that the statement is created. */
      lispval ptype = init_ptypes[i++];
      if (KNO_VOIDP(ptype)) {}
      else u8_log(LOG_WARN,UnusedType,"Parameter type %hq is not used for %s",
                  ptype,dbproc->sqldb_qtext);
      kno_decref(ptype);}
    dbproc->sqldb_paramtypes = ptypes; dbproc->fcn_n_params = n_params;
    if (init_ptypes) u8_free(init_ptypes);}

  /* Check that the number of returned columns has not changed
     (this could happen if there were schema changes) */
  if ((dbproc->mysqlproc_n_cols>=0)&&(n_cols!=dbproc->mysqlproc_n_cols)) {
    u8_log(LOG_WARN,ServerReset,
           "The number of columns for query '%s' on %s (%s) has changed",
           dbproc->sqldb_qtext,dbp->sqldb_spec,dbp->sqldb_info);}
  dbproc->mysqlproc_n_cols = n_cols;

  /* Check that the number of parameters has not changed
     (this might happen if there were schema changes) */
  dbproc->fcn_min_arity = n_params;

  if (n_params) {
    dbproc->mysqlproc_inbound = u8_alloc_n(n_params,MYSQL_BIND);
    memset(dbproc->mysqlproc_inbound,0,sizeof(MYSQL_BIND)*n_params);
    dbproc->mysqlproc_valbuf = u8_alloc_n(n_params,union MYSQL_VALBUF);
    memset(dbproc->mysqlproc_valbuf,0,sizeof(union MYSQL_VALBUF)*n_params);}

  dbproc->mysqlproc_needs_init = 0;

  return RETVAL_OK;
}

static void recycle_mysqlproc(struct KNO_SQLPROC *c)
{
  struct KNO_MYSQL_PROC *dbproc = (struct KNO_MYSQL_PROC *)c;
  int i, lim, rv;
  kno_release_sqlproc(c);
  if (dbproc->mysqlproc_stmt) {
    if ((rv = mysql_stmt_close(dbproc->mysqlproc_stmt))) {
      int mysqlerrno = mysql_stmt_errno(dbproc->mysqlproc_stmt);
      const char *errmsg = mysql_stmt_error(dbproc->mysqlproc_stmt);
      dbproc->mysqlproc_stmt = NULL;
      u8_log(LOG_WARN,MySQL_Error,"Error (%d:%d) closing statement %s: %s",
             rv,mysqlerrno,dbproc->mysqlproc_string,errmsg);}}
  kno_decref(dbproc->sqldb_colinfo);

  if (dbproc->mysqlproc_valbuf) u8_free(dbproc->mysqlproc_valbuf);
  if (dbproc->mysqlproc_isnullbuf) u8_free(dbproc->mysqlproc_isnullbuf);
  if (dbproc->mysqlproc_inbound) u8_free(dbproc->mysqlproc_inbound);
  if (dbproc->mysqlproc_colnames) u8_free(dbproc->mysqlproc_colnames);

  if (dbproc->mysqlproc_outbound) {
    i = 0; lim = dbproc->mysqlproc_n_cols; while (i<lim) {
      if (dbproc->mysqlproc_outbound[i].buffer) {
        u8_free(dbproc->mysqlproc_outbound[i].buffer);
        dbproc->mysqlproc_outbound[i].buffer = NULL;}
      if (dbproc->mysqlproc_outbound[i].length) {
        u8_free(dbproc->mysqlproc_outbound[i].length);
        dbproc->mysqlproc_outbound[i].length = NULL;}
      i++;}
    u8_free(dbproc->mysqlproc_outbound);}

  if (dbproc->sqldb_paramtypes) {
    i = 0; lim = dbproc->fcn_n_params; while (i< lim) {
      kno_decref(dbproc->sqldb_paramtypes[i]); i++;}
    u8_free(dbproc->sqldb_paramtypes);}

  u8_free(dbproc->sqldb_spec);
  u8_free(dbproc->sqldb_qtext);
  u8_free(dbproc->mysqlproc_string);

  u8_destroy_mutex(&(dbproc->mysqlproc_lock));

  kno_decref(dbproc->sqldbptr);
}

/* Actually calling a MYSQL proc */

static lispval applymysqlproc(kno_function f,int n,lispval *args,int reconn);

static lispval callmysqlproc(kno_function fn,int n,lispval *args){
  return applymysqlproc(fn,n,args,7);}

static lispval applymysqlproc(kno_function fn,int n,lispval *args,int reconn)
{
  struct KNO_MYSQL_PROC *dbproc = (struct KNO_MYSQL_PROC *)fn;
  struct KNO_MYSQL *dbp=
    KNO_GET_CONS(dbproc->sqldbptr,kno_sqldb_type,struct KNO_MYSQL *);
  int n_params = dbproc->fcn_n_params;
  MYSQL_BIND *inbound = dbproc->mysqlproc_inbound;
  union MYSQL_VALBUF *valbuf = dbproc->mysqlproc_valbuf;
  int retry = 0, reterr = 0;

  MYSQL_TIME *mstimes[4]; int n_mstimes = 0;

  /* Argbuf stores objects we consed in the process of
     converting application objects to SQLish values. */
  lispval *ptypes = dbproc->sqldb_paramtypes;
  lispval values = KNO_EMPTY_CHOICE;
  int i = 0, n_bound = 0;
  /* *retval* tracks the most recent operation and tells whether to keep going.
     The other x*vals* identify the retvals for particular phases, to help
     produce more helpful error messages.
     A ZERO VALUE MEANS OK. */
  int retval = 0, bretval = 0, eretval = 0, sretval = 0, iretval = 0;
  int proclock = 0, dblock = 0;
  volatile unsigned int mysqlerrno = 0;
  const char *mysqlerrmsg = NULL;
  lispval _argbuf[4], *argbuf=_argbuf;

  u8_lock_mutex(&(dbproc->mysqlproc_lock)); proclock = 1;

  u8_log(LOG_DEBUG,"MySQLproc/call","%lx: %s",
         KNO_LONGVAL(dbproc),
         dbproc->mysqlproc_string);

  /* Initialize it if it needs it */
  if (dbproc->mysqlproc_needs_init) {
    u8_lock_mutex(&(dbp->mysql_lock));
    retval = iretval = init_mysqlproc(dbp,dbproc);
    u8_unlock_mutex(&(dbp->mysql_lock));}
  if (retval == RETVAL_OK) {
    n_params = dbproc->fcn_n_params;
    inbound = dbproc->mysqlproc_inbound;
    valbuf = dbproc->mysqlproc_valbuf;
    ptypes = dbproc->sqldb_paramtypes;

    /* We check arity here because the procedure may not have been initialized
       (and determined its arity) during the arity checking done by APPLY. */
    if (n!=n_params) {
      u8_unlock_mutex(&(dbproc->mysqlproc_lock));
      if (n<n_params)
        return kno_err(kno_TooFewArgs,"applymysqlproc",fn->fcn_name,KNO_VOID);
      else return kno_err(kno_TooManyArgs,"applymysqlproc",fn->fcn_name,KNO_VOID);}

    if (n_params>4) argbuf = u8_alloc_n(n_params,lispval);
    /* memset(argbuf,0,LISPVEC_BYTELEN(n_params)); */

    /* Initialize the input parameters from the arguments.
       None of this accesses the database, so we don't lock it yet.*/
    while (i<n_params) {
      lispval arg = args[i];
      /* Use the ptypes to map application arguments into SQL. */
      if (KNO_VOIDP(ptypes[i])) argbuf[i]=KNO_VOID;
      else if ((KNO_OIDP(arg)) && (KNO_OIDP(ptypes[i]))) {
        KNO_OID addr = KNO_OID_ADDR(arg);
        KNO_OID base = KNO_OID_ADDR(ptypes[i]);
        unsigned long long offset = KNO_OID_DIFFERENCE(addr,base);
        argbuf[i]=arg = KNO_INT(offset);}
      else if (KNO_APPLICABLEP(ptypes[i])) {
        argbuf[i]=arg = kno_apply(ptypes[i],1,&arg);}
      else if (KNO_TRUEP(ptypes[i])) {
        struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
        kno_unparse(&out,arg);
        argbuf[i]=kno_init_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
      else argbuf[i]=KNO_VOID;

      /* Set this in case it was different for a previous call. */
      inbound[i].is_null = NULL;

      /* Now set up the bindings */
      if (KNO_FIXNUMP(arg)) {
        inbound[i].is_unsigned = 0;
        inbound[i].buffer_type = MYSQL_TYPE_LONG;
        inbound[i].buffer = &(valbuf[i].lval);
        inbound[i].buffer_length = sizeof(int);
        inbound[i].length = NULL;
        valbuf[i].lval = kno_getint(arg);}
      else if (KNO_OIDP(arg)) {
        KNO_OID addr = KNO_OID_ADDR(arg);
        inbound[i].is_unsigned = 1;
        inbound[i].buffer_type = MYSQL_TYPE_LONGLONG;
        inbound[i].buffer = &(valbuf[i].llval);
        inbound[i].buffer_length = sizeof(unsigned long long);
        inbound[i].length = NULL;
        valbuf[i].llval = addr;}
      else if (KNO_BIGINTP(arg)) {
        long long lv = kno_bigint_to_long_long((kno_bigint)arg);
        inbound[i].is_unsigned = 0;
        inbound[i].buffer_type = MYSQL_TYPE_LONGLONG;
        inbound[i].buffer = &(valbuf[i].llval);
        inbound[i].buffer_length = sizeof(long long);
        inbound[i].length = NULL;
        valbuf[i].llval = lv;}
      else if (KNO_FLONUMP(arg)) {
        inbound[i].buffer_type = MYSQL_TYPE_DOUBLE;
        inbound[i].buffer = &(valbuf[i].fval);
        inbound[i].buffer_length = sizeof(double);
        inbound[i].length = NULL;
        valbuf[i].fval = KNO_FLONUM(arg);}
      else if (KNO_STRINGP(arg)) {
        inbound[i].buffer_type = MYSQL_TYPE_STRING;
        inbound[i].buffer = (u8_byte *)KNO_CSTRING(arg);
        inbound[i].buffer_length = KNO_STRLEN(arg);
        inbound[i].length = &(inbound[i].buffer_length);}
      else if (KNO_SYMBOLP(arg)) {
        u8_string pname = KNO_SYMBOL_NAME(arg);
        inbound[i].buffer_type = MYSQL_TYPE_STRING;
        inbound[i].buffer = (u8_byte *)pname;
        inbound[i].buffer_length = strlen(pname);
        inbound[i].length = &(inbound[i].buffer_length);}
      else if (KNO_PACKETP(arg)) {
        inbound[i].buffer_type = MYSQL_TYPE_BLOB;
        inbound[i].buffer = (u8_byte *)KNO_PACKET_DATA(arg);
        inbound[i].buffer_length = KNO_PACKET_LENGTH(arg);
        inbound[i].length = &(inbound[i].buffer_length);}
      else if (KNO_TYPEP(arg,kno_uuid_type)) {
        struct KNO_UUID *uuid = KNO_CONSPTR(kno_uuid,arg);
        inbound[i].buffer_type = MYSQL_TYPE_BLOB;
        inbound[i].buffer = &(uuid->uuid16);
        inbound[i].buffer_length = 16;
        inbound[i].length = NULL;}
      else if (KNO_TYPEP(arg,kno_timestamp_type)) {
        struct KNO_TIMESTAMP *tm = KNO_CONSPTR(kno_timestamp,arg);
        MYSQL_TIME *mt = u8_alloc(MYSQL_TIME);
        struct U8_XTIME *xt = &(tm->u8xtimeval), gmxtime; time_t tick;
        memset(mt,0,sizeof(MYSQL_TIME));
        if (n_mstimes<4) mstimes[n_mstimes++]=mt;
        if ((xt->u8_tzoff)||(xt->u8_dstoff)) {
          /* If it's not UTC, we need to convert it. */
          tick = xt->u8_tick;
          u8_init_xtime(&gmxtime,tick,xt->u8_prec,xt->u8_nsecs,0,0);
          xt = &gmxtime;}
        inbound[i].buffer = mt;
        inbound[i].buffer_type=
          ((xt->u8_prec>u8_day) ?
           (MYSQL_TYPE_DATETIME) :
           (MYSQL_TYPE_DATE));
        mt->year = xt->u8_year;
        mt->month = xt->u8_mon+1;
        mt->day = xt->u8_mday;
        mt->hour = xt->u8_hour;
        mt->minute = xt->u8_min;
        mt->second = xt->u8_sec;}
      else if ((KNO_TRUEP(arg)) || (KNO_FALSEP(arg))) {
        inbound[i].is_unsigned = 0;
        inbound[i].buffer_type = MYSQL_TYPE_LONG;
        inbound[i].buffer = &(valbuf[i].lval);
        inbound[i].buffer_length = sizeof(int);
        inbound[i].length = NULL;
        if (KNO_TRUEP(arg)) valbuf[i].lval = 1;
        else valbuf[i].lval = 0;}
      else if ((KNO_EMPTY_CHOICEP(arg))|| (KNO_EMPTY_QCHOICEP(arg))) {
        my_bool *bp = (my_bool *)&(valbuf[i].lval);
        inbound[i].is_null = bp;
        inbound[i].buffer = NULL;
        inbound[i].buffer_length = sizeof(int);
        inbound[i].length = NULL;
        *bp = 1;}
      /* This catches cases where the conversion process produces an
         error. */
      else if (KNO_ABORTP(arg)) {
        int j = 0;
        while (j<i) {kno_decref(argbuf[j]); j++;}
        if (argbuf!=_argbuf) u8_free(argbuf);
        if (dblock) {u8_unlock_mutex(&(dbp->mysql_lock)); dblock = 0;}
        if (proclock) {u8_unlock_mutex(&(dbproc->mysqlproc_lock)); proclock = 0;}
        return KNO_ERROR_VALUE;}
      else if (KNO_TRUEP(ptypes[i])) {
        /* If the ptype is #t, try to convert it into a string,
           and catch it if you have an error. */
        struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
        if (kno_unparse(&out,arg)<0) {
          int j = 0;
          kno_seterr(MySQL_NoConvert,"applymysqlproc",
                    dbproc->sqldb_qtext,kno_incref(arg));
          while (j<i) {kno_decref(argbuf[i]); i++;}
          j = 0; while (j<n_mstimes) {u8_free(mstimes[j]); j++;}
          if (argbuf!=_argbuf) u8_free(argbuf);
          if (dblock) {u8_unlock_mutex(&(dbp->mysql_lock)); dblock = 0;}
          if (proclock)  {u8_unlock_mutex(&(dbproc->mysqlproc_lock)); proclock = 0;}
          return KNO_ERROR_VALUE;}
        else {
          u8_byte *as_string = out.u8_outbuf;
          int stringlen = out.u8_write-out.u8_outbuf;
          inbound[i].buffer_type = MYSQL_TYPE_STRING;
          inbound[i].buffer = as_string;
          inbound[i].buffer_length = stringlen;
          inbound[i].length = NULL;
          /* We put the consed string into the argbug as a Lisp
             string so that we'll free it when we're done. */
          argbuf[i]=kno_init_string(NULL,stringlen,as_string);}}
      else {
        int j;
        /* Finally, if we can't convert the value, we error. */
        kno_seterr(MySQL_NoConvert,"applymysqlproc",
                  dbproc->sqldb_qtext,kno_incref(arg));
        j = 0; while (j<n_mstimes) {u8_free(mstimes[j]); j++;}
        j = 0; while (j<i) {kno_decref(argbuf[j]); j++;}
        if (argbuf!=_argbuf) u8_free(argbuf);
        if (dblock) {u8_unlock_mutex(&(dbp->mysql_lock)); dblock = 0;}
        if (proclock)  {u8_unlock_mutex(&(dbproc->mysqlproc_lock)); proclock = 0;}
        return KNO_ERROR_VALUE;}
      i++;}}
  n_bound = i;

  if (dbproc->mysqlproc_needs_init) {
    /* If there's been a restart while we were binding, act as though
       we've failed and retry the connection at the end. */
    retval = -1; retry = 1;}
  else {
    /* Otherwise, tell MYSQL that the parameters are ready. */
    retval = bretval = mysql_stmt_bind_param(dbproc->mysqlproc_stmt,inbound);}

  if (retval == RETVAL_OK) {
    /* Lock the connection itself before executing. ??? Why? */
    u8_lock_mutex(&(dbp->mysql_lock)); dblock = 1;
    if (dbproc->mysqlproc_needs_init) {
      /* Once more, if there's been a restart before we get the lock,
         set retry = 1 and pretend to fail.  Once we have the lock, we
         no longer have to worry about restarts. */
      retval = -1; retry = 1;}
    else retval = eretval = mysql_stmt_execute(dbproc->mysqlproc_stmt);}

  /* Read all the values at once */
  if (retval == RETVAL_OK)
    retval = sretval = mysql_stmt_store_result(dbproc->mysqlproc_stmt);

  if (retval == RETVAL_OK) {
    /* If everything went fine, we release the database lock now. */
    if (dblock) {u8_unlock_mutex(&(dbp->mysql_lock)); dblock = 0;}}

  /* Now convert the MYSQL results into LISP.  We don't check for
     connection errors because we've stored the whole result locally. */
  if (retval == RETVAL_OK) {
    if (dbproc->mysqlproc_n_cols) {
      values = get_stmt_values(dbproc->mysqlproc_stmt,
                             dbproc->sqldb_colinfo,
                             dbproc->mysqlproc_n_cols,
                             dbproc->mysqlproc_colnames,
                             dbproc->mysqlproc_outbound,
                             dbproc->mysqlproc_isnullbuf);
      mysql_stmt_free_result(dbproc->mysqlproc_stmt);}
    else {
      /* We could possibly do something with this */
      int U8_MAYBE_UNUSED rows = mysql_stmt_affected_rows(dbproc->mysqlproc_stmt);
      values = KNO_VOID;}}

  if (retval!=RETVAL_OK) {
    /* Log any errors (even ones we're going to handle) */
    mysqlerrno = mysql_stmt_errno(dbproc->mysqlproc_stmt);
    mysqlerrmsg = mysql_stmt_error(dbproc->mysqlproc_stmt);
    u8_log(LOG_WARN,
           ((sretval)?("applymysqlprocproc/store"):
            (eretval)?("applymysqlprocproc/exec"):
            (bretval)?("applymysqlprocproc/bind"):
            ("applymysqlproc")),
           "MYSQL error '%s' (%d) for %s at %s",
           mysqlerrmsg,mysqlerrno,dbproc->mysqlproc_string,dbp->sqldb_spec);
    /* mysql_stmt_reset(dbproc->mysqlproc_stmt); */

    /* Figure out if we're going to retry */
    if ((reconn>0)&&((retry)||(NEED_RESTART(mysqlerrno))))
      retry = 1;
    else if ((reconn>0)&&(NEED_RESET(mysqlerrno))) {
      dbproc->mysqlproc_needs_init = 1; retry = 1;}
    else retry = 0;
    if ((retval)&&(!(retry))) reterr = 1;}

  /* Clean up */
  i = 0; while (i<n_mstimes) {u8_free(mstimes[i]); i++;}
  i = 0; while (i<n_bound) {
    lispval arg = argbuf[i++];
    if (arg) kno_decref(arg);}
  if (argbuf!=_argbuf) u8_free(argbuf);

  if (reterr) {
    if (dblock) {u8_unlock_mutex(&(dbp->mysql_lock)); dblock = 0;}
    if (proclock) {u8_unlock_mutex(&(dbproc->mysqlproc_lock)); proclock = 0;}
    if (!(bretval == RETVAL_OK))
      u8_seterr(MySQL_Error,"mysqlproc/bind",
                u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
    else if (!(eretval == RETVAL_OK))
      u8_seterr(MySQL_Error,"mysqlproc/exec",
                u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
    else if (!(sretval == RETVAL_OK))
      u8_seterr(MySQL_Error,"mysqlproc/store",
                u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
    else u8_seterr(MySQL_Error,"mysqlproc",
                   u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
    return KNO_ERROR_VALUE;}
  else if (retry) {
    if (!(dblock)) {u8_lock_mutex(&(dbp->mysql_lock)); dblock = 1;}
    if ( (dbproc->mysqlproc_needs_init) && (!(NEED_RESTART(mysqlerrno))) ) {
      retval = init_mysqlproc(dbp,dbproc);
      if (retval!=RETVAL_OK)
        retval = restart_connection(dbp);}
    else retval = restart_connection(dbp);
    if (dblock) {u8_unlock_mutex(&(dbp->mysql_lock)); dblock = 0;}
    if (proclock) {u8_unlock_mutex(&(dbproc->mysqlproc_lock)); proclock = 0;}
    if (retval!=RETVAL_OK) {
      mysqlerrno = mysql_errno(dbp->mysqldb);
      mysqlerrmsg = mysql_error(dbp->mysqldb);
      u8_seterr(MySQL_Error,"mysqlproc/restart",
                u8_mkstring("%s (%d)",mysqlerrmsg,mysqlerrno));
      return KNO_ERROR_VALUE;}
    else return applymysqlproc(fn,n,args,reconn-1);}
  else {
    if (dblock) {
      u8_unlock_mutex(&(dbp->mysql_lock));
      dblock = 0;}
    if (proclock) {
      u8_unlock_mutex(&(dbproc->mysqlproc_lock));
      proclock = 0;}
    return values;}
}

/* Initialization */

static long long int mysql_initialized = 0;

static struct KNO_SQLDB_HANDLER mysql_handler=
  {"mysql",NULL,NULL,NULL,NULL};

int first_call = 1;

static int init_thread_for_mysql()
{
  u8_log(LOG_DEBUG,"MYSQL","Initializing thread for MYSQL");
  if (first_call) {
    first_call = 0; my_init();}
  else mysql_thread_init();
  return 1;
}

static void cleanup_thread_for_mysql()
{
  u8_log(LOG_DEBUG,"MYSQL","Cleaning up thread for MYSQL");
  mysql_thread_end();
}

static lispval mysql_module;

KNO_EXPORT int kno_init_mysql()
{
  if (mysql_initialized) return 0;

  u8_register_threadinit(init_thread_for_mysql);
  u8_register_threadexit(cleanup_thread_for_mysql);

  mysql_module = kno_new_cmodule("mysql",0,kno_init_mysql);

  u8_init_mutex(&mysql_connect_lock);

  mysql_handler.execute = mysqlexechandler;
  mysql_handler.makeproc = mysqlmakeprochandler;
  mysql_handler.recycle_db = recycle_mysqldb;
  mysql_handler.recycle_proc = recycle_mysqlproc;

  kno_register_sqldb_handler(&mysql_handler);

  init_local_cprims();
  
  mysql_initialized = u8_millitime();
  
  boolean_symbol = kno_intern("boolean");
  merge_symbol = kno_intern("%merge");
  noempty_symbol = kno_intern("%noempty");
  sorted_symbol = kno_intern("%sorted");

  port_symbol = kno_intern("port");
  reconnect_symbol = kno_intern("reconnect");
  ssl_symbol = kno_intern("ssl");
  sslca_symbol = kno_intern("sslca");
  sslcert_symbol = kno_intern("sslcert");
  sslkey_symbol = kno_intern("sslkey");
  sslcadir_symbol = kno_intern("sslcadir");
  sslciphers_symbol = kno_intern("sslciphers");

  timeout_symbol = kno_intern("timeout");
  connect_timeout_symbol = kno_intern("connect-timeout");
  read_timeout_symbol = kno_intern("read-timeout");
  write_timeout_symbol = kno_intern("write-timeout");
  lazy_symbol = kno_intern("lazyprocs");

  kno_finish_module(mysql_module);

  kno_register_config
    ("MYSQL:LAZYPROCS",
     "whether MYSQL should delay initializing dbprocs (statements)",
     kno_boolconfig_get,kno_boolconfig_set,
     &default_lazy_init);

  u8_register_source_file(_FILEINFO);

  return 1;
}

static void init_local_cprims()
{
  KNO_LINK_PRIM("mysql/open",open_mysql,6,mysql_module);
  KNO_LINK_PRIM("mysql/refresh",refresh_mysqldb,2,mysql_module);
}
