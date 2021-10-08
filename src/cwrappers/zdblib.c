/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* libzdb.c
   This implements Kno bindings to mariadb.
   Copyright (C) 2007-2019 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
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

#include <zdb.h>

static lispval ssl_symbol, sslca_symbol, sslcert_symbol, sslkey_symbol, sslcadir_symbol;
static lispval sslciphers_symbol, port_symbol, reconnect_symbol;
static lispval timeout_symbol, connect_timeout_symbol;
static lispval read_timeout_symbol, write_timeout_symbol, lazy_symbol;
static lispval merge_symbol, noempty_symbol, sorted_symbol;

static lispval boolean_symbol;
u8_condition ServerReset=_("MYSQL server reset");
u8_condition UnusedType=_("MYSQL unused parameter type");

#ifndef ER_NORMAL_SHUTDOWN
#define ER_NORMAL_SHUTDOWN ER_NORMAL_SERVER_SHUTDOWN
#endif

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

KNO_EXPORT int kno_init_zdb(void) KNO_LIBINIT_FN;
static struct KNO_SQLDB_HANDLER zdb_handler;
static lispval callmysqlproc(kno_stack stack,lispval fn,int n,kno_argvec args);

typedef struct KNO_ZDB {
  KNO_SQLDB_FIELDS;
  double zdb_startup, zdb_restarted;
  ConnectionPool_T zdb_pool;
  URL_T zdb_url;} KNO_MYSQL;
typedef struct KNO_ZDB *kno_zdb;

static struct KNO_SQLDB_HANDLER zdb_handler;

/* This is used as a buffer for inbound bindings */
union MYSQL_VALBUF { double fval; void *ptr; long lval; long long llval;};

typedef struct KNO_ZDB_PROC {
  KNO_SQLPROC_FIELDS;
  int zdbp_n_ready, zdbp_n_running;
  int zdbp_pool_len, zdbp_active_len;
  PreparedStatement_T *zdbp_pool;
  PreparedStatement_T *zdbp_active;
  lispval zdbp_dbptr;
  u8_string zdbp_qtext;
  int zdbp_n_params;
  lispval *zdbp_param_types;
  lispval zdbp_colinfo, zdbp_opts;
} KNO_ZDB_PROC;
typedef struct KNO_ZDB_PROC *kno_zdb_proc;

static unsigned char *_memdup(const unsigned char *data,int len)
{
  unsigned char *duplicate = u8_alloc_n(len,unsigned char);
  memcpy(duplicate,data,len);
  return duplicate;
}

static lispval zdb_error(u8_context caller,u8_string details,lispval obj)
{
  return kno_err("ZDBError",caller,details,obj);
}

/* Connection operations */

DEF_KNOSYM(colinfo);

static lispval open_zdb_connpool(u8_string spec,lispval opts,lispval colinfo)
{
  URL_T url = URL_new(spec);
  if (url==NULL) return zdb_error("open_connection/url",spec,opts);
  /* Init url from opts */
  ConnectionPool_T pool = ConnectionPool_new(url);
  if (pool==NULL) {
    /* free url */
    return zdb_error("open_connection/url",spec,opts);}
  /* Init conn from opts */
  ConnectionPool_start(pool); /* err value? */

  if (KNO_VOIDP(colinfo))
    colinfo=kno_getopt(opts,KNOSYM(colinfo),KNO_FALSE);
  else kno_incref(colinfo);

  struct KNO_ZDB *dbp = u8_alloc(struct KNO_ZDB);
  /* Initialize the cons (does a memset too) */
  KNO_INIT_FRESH_CONS(dbp,kno_sqldb_type);
  /* Process the other arguments */

  /* Initialize the other fields */
  dbp->sqldb_handler = &zdb_handler;
  dbp->sqldb_spec = u8_strdup(spec);
  dbp->sqldb_info = NULL;
  dbp->sqldb_colinfo = colinfo;
  dbp->sqldb_options = kno_incref(opts);
  u8_init_mutex(&dbp->sqlproclock);

  dbp->zdb_url = url;
  dbp->zdb_pool = pool;

  dbp->sqldb_info=u8_strdup(spec);
  /*
    u8_mkstring("%s;client %s",
		mysql_get_host_info(dbp->mysqldb),
		mysql_get_client_info()); */

  dbp->zdb_startup = u8_elapsed_time();
  dbp->zdb_restarted = -1;

  return (lispval) dbp;
}

static void recycle_zdb(struct KNO_SQLDB *c)
{
  struct KNO_ZDB *dbp = (struct KNO_ZDB *)c;
  u8_lock_mutex(&(dbp->sqlproclock));
  ConnectionPool_T pool = dbp->zdb_pool; dbp->zdb_pool=NULL;
  ConnectionPool_stop(dbp->zdb_pool);
  ConnectionPool_free(&(dbp->zdb_pool));
  u8_unlock_mutex(&(dbp->sqlproclock));
}

DEFC_PRIM("zdb/open",zdb_open,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "Opens a ZDB connection pool to the SQL database *spec*.",
	  {"spec",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_FALSE},
	  {"colinfo",kno_any_type,KNO_VOID})
lispval zdb_open(lispval spec,lispval opts,lispval colinfo)
{
  lispval free_spec = KNO_VOID;
  u8_string urlstring;
  if (KNO_STRINGP(spec)) urlstring=KNO_CSTRING(spec);
  else if (KNO_SYMBOLP(spec)) {
    free_spec = kno_config_get(KNO_SYMBOL_NAME(spec));
    if (KNO_STRINGP(free_spec)) urlstring=KNO_CSTRING(free_spec);
    else {
      kno_seterr(kno_TypeError,"zdb_open",KNO_SYMBOL_NAME(spec),free_spec);
      kno_decref(free_spec);
      return KNO_ERROR;}}
  else return kno_type_error("string/config","zdb_open",spec);
  lispval result = open_zdb_connpool(urlstring,opts,colinfo);
  kno_decref(free_spec);
  return result;
}

/* Opening connections */

/* Everything but the hostname and dbname is optional.
   In theory, we could have the dbname be optional, but for now we'll
   require it.  */

#if 0
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
      result = kno_type_error("applicable","mariadb/get_stmt_values",mergefn);}
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
      u8_seterr(MySQL_Error,"mariadb/init_stmt_results",u8_strdup(errmsg));
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
#endif

/* MYSQL procs */

static int default_lazy_init = 0;

#if 0
static lispval mysqlmakeproc
(struct KNO_MYSQL *dbp,
 u8_string stmt,int stmt_len,lispval colinfo,
 int n,kno_argvec ptypes)
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
#ifdef KNO_CALL_XCALL
  dbproc->fcn_call = KNO_CALL_XCALL | KNO_CALL_NOTAIL;
#else
  dbproc->fcn_call = KNO_FCN_CALL_XCALL | KNO_FCN_CALL_NOTAIL;
#endif
  dbproc->fcn_call_width = dbproc->fcn_arity = -1;
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
 lispval colinfo,int n,kno_argvec ptypes)
{
  if (sqldb->sqldb_handler== &mysql_handler)
    return mysqlmakeproc((kno_mysql)sqldb,stmt,stmt_len,colinfo,n,ptypes);
  else return kno_type_error("MYSQL SQLDB","mysqlmakeprochandler",
			     (lispval)sqldb);
}
#endif

/* Various MYSQLPROC functions */

static void recycle_mysqlproc(struct KNO_SQLPROC *c)
{
  struct KNO_ZDB_PROC *dbproc = (struct KNO_ZDB_PROC *)c;
}

/* Initialization */

static long long int zdb_initialized = 0;

static struct KNO_SQLDB_HANDLER zdb_handler=
  {"zdb",NULL,NULL,NULL,NULL};
int first_call = 1;

static lispval zdb_module;

KNO_EXPORT int kno_init_zdblib()
{
  if (zdb_initialized) return 0;

  zdb_module = kno_new_cmodule("zdb",0,kno_init_zdblib);

  zdb_handler.recycle_db = recycle_zdb;

#if 0
  zdb_handler.execute = zdbexechandler;
  zdb_handler.makeproc = zdbmakeprochandler;
  zdb_handler.recycle_proc = recycle_zdbproc;
#endif

#if 0
  kno_register_sqldb_handler(&zdb_handler);
#endif

  link_local_cprims();

  zdb_initialized = u8_millitime();

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

  kno_finish_module(zdb_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("zdb/open",zdb_open,3,zdb_module);
}
