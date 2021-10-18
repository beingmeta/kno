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
#define KNO_DEFINE_GETOPT 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/eval.h"
#include "kno/sequences.h"
#include "kno/storage.h"
#include "kno/texttools.h"
#include "kno/cprims.h"

#include "kno/sql.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include <libpq-fe.h>
#include <math.h>
#include <limits.h>

#include "pqprims.h"

KNO_EXPORT int kno_init_pqprims(void) KNO_LIBINIT_FN;

static lispval pqprims_module;

static int pqprims_initialized = 0;

static struct KNO_SQLDB_HANDLER postgres_handler=
  {"postgres",NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL};

static struct KNO_HASHTABLE _pqtypemaps = { 0 };

/* Declarations */

typedef PGresult *pgresult;

KNO_EXPORT int kno_init_pgql(void) KNO_LIBINIT_FN;

DEF_KNOSYM(connection); DEF_KNOSYM(irritant);
DEF_KNOSYM(primary); DEF_KNOSYM(details); DEF_KNOSYM(hint);
DEF_KNOSYM(iquery); DEF_KNOSYM(position); DEF_KNOSYM(command);
DEF_KNOSYM(schema); DEF_KNOSYM(table); DEF_KNOSYM(column);
DEF_KNOSYM(datatype); DEF_KNOSYM(constraint);
DEF_KNOSYM(sourcefile); DEF_KNOSYM(sourceline); DEF_KNOSYM(sourcefn);
DEF_KNOSYM(errmsg); DEF_KNOSYM(sorted); DEF_KNOSYM(noempty);

static u8_string convert_qmarks(u8_string input)
{
  int len = u8_strlen(input), mark_count = 0;
  struct U8_OUTPUT newq;
  U8_INIT_OUTPUT(&newq,(len+10)*2);
  u8_string scan = input;
  int c = u8_sgetc(&scan), post_punct = 0, post_space = 0;
  while (c>0) {
    if ( (c == '?') && ( (post_punct) || (post_space) ) ) {
      mark_count++;
      u8_printf(&newq,"$%d",mark_count);
      c=u8_sgetc(&scan);}
    else if ( (c == '"') || (c == '\'') ) {
      int saved = c;
      u8_putc(&newq,c);
      c=u8_sgetc(&scan);
      while ( (c>0) && (c != saved) ) {
	u8_putc(&newq,c);
	if (c=='\\') {
	  c=u8_sgetc(&scan);
	  u8_putc(&newq,c);
	  c=u8_sgetc(&scan);}
	else c=u8_sgetc(&scan);}
      if (c>0) {
	u8_putc(&newq,c);
	c=u8_sgetc(&scan);}
      continue;}
    if (c<0) break;
    post_punct = u8_ispunct(c);
    post_space = u8_isspace(c);
    u8_putc(&newq,c);
    c=u8_sgetc(&scan);}
  return newq.u8_outbuf;
}

static int get_postgres_type(lispval x,lispval info)
{
  if ( (KNO_SYMBOLP(info)) || (KNO_CTYPEP(info)) || (KNO_OIDP(info)) ) {
    lispval code = kno_hashtable_get(&_pqtypemaps,info,KNO_VOID);
    if (KNO_FIXNUMP(code)) return KNO_FIX2INT(code);}
  else if ( (KNO_TABLEP(info)) && (kno_testopt(info,KNOSYM_TYPE,KNO_VOID)) ) {
    lispval type = kno_getopt(info,KNOSYM_TYPE,KNO_VOID);
    lispval code = kno_hashtable_get(&_pqtypemaps,type,KNO_VOID);
    if (KNO_FIXNUMP(code)) return KNO_FIX2INT(code);}
  if (KNO_STRINGP(x))
    return PQTYPE_VARCHAR;
  else if (KNO_FLONUMP(x))
    return PQTYPE_FLOAT8;
  else if (KNO_UINTP(x))
    return PQTYPE_INT4;
  else if (KNO_FIXNUMP(x))
    return PQTYPE_INT8;
  else if (KNO_BIGINTP(x))
    return PQTYPE_NUMERIC;
  else return PQTYPE_TEXT;
}

/* Error handling */

static u8_condition PQCError=_("POSTGRES connection error");

static void copy_error_details(lispval details,pgresult result,
			       lispval sym,int fieldcode,
			       int parse)
{
  char *field = PQresultErrorField(result,PG_DIAG_MESSAGE_DETAIL);
  if (field == NULL) return;
  lispval strval = (parse) ? (kno_parse(field)) : (knostring(field));
  u8_exception cxt = u8_current_exception;
  if (KNO_ABORTED(strval)) {
    kno_pop_exceptions(cxt,LOG_DEBUG);
    strval = knostring(field);}
  kno_store(details,sym,strval);
  kno_decref(strval);
}


static void pqresult_error(pqconn conn,pgresult result,
			   u8_string query,lispval irritant)
{
  char *errmsg = (1) ?
    (PQresultVerboseErrorMessage(result,PQERRORS_VERBOSE,PQSHOW_CONTEXT_ERRORS)) :
    (PQresultErrorMessage(result));
  u8_string details = u8_fromlibc(errmsg);
  lispval info = kno_make_slotmap(5,0,NULL);
  if (!(KNO_VOIDP(irritant))) kno_store(info,KNOSYM(irritant),irritant);
  kno_store(info,KNOSYM(connection),(lispval)conn);

  lispval qstring = knostring(query);
  kno_store(info,KNOSYM(command),qstring);
  kno_decref(qstring);

  lispval err = knostring(errmsg);
  kno_store(info,KNOSYM(errmsg),err);
  kno_decref(err);

  copy_error_details(info,result,KNOSYM(primary),PG_DIAG_MESSAGE_DETAIL,0);
  copy_error_details(info,result,KNOSYM(details),PG_DIAG_MESSAGE_PRIMARY,0);
  copy_error_details(info,result,KNOSYM(hint),PG_DIAG_MESSAGE_HINT,0);
  copy_error_details(info,result,KNOSYM(position),PG_DIAG_STATEMENT_POSITION,1);
  copy_error_details(info,result,KNOSYM(iquery),PG_DIAG_INTERNAL_QUERY,0);
  copy_error_details(info,result,KNOSYM(schema),PG_DIAG_SCHEMA_NAME,0);
  copy_error_details(info,result,KNOSYM(table),PG_DIAG_TABLE_NAME,0);
  copy_error_details(info,result,KNOSYM(column),PG_DIAG_COLUMN_NAME,0);
  copy_error_details(info,result,KNOSYM(datatype),PG_DIAG_DATATYPE_NAME,0);
  copy_error_details(info,result,KNOSYM(constraint),PG_DIAG_CONSTRAINT_NAME,0);
  copy_error_details(info,result,KNOSYM(sourcefile),PG_DIAG_SOURCE_FILE,0);
  copy_error_details(info,result,KNOSYM(sourceline),PG_DIAG_SOURCE_LINE,0);
  copy_error_details(info,result,KNOSYM(sourcefn),PG_DIAG_SOURCE_FUNCTION,0);

  kno_seterr(PQCError,"pqresult",details,info);
  u8_free(details);
  kno_decref(info);
}

/* Support functions */

static void recycle_pqconn(pqconn dbptr)
{
  int i = 0, n = dbptr->pqconn_n_params;
  char **keys = dbptr->pqconn_keys;
  char **vals = dbptr->pqconn_vals;
  while (i<n) {
    if (keys[i]) u8_free(keys[i]);
    if (vals[i]) u8_free(vals[i]);
    i++;}
  u8_free(keys); u8_free(vals);
  u8_destroy_mutex(&(dbptr->pqconn_lock));
}

#define PQL_OPEN_MAX_PARAMS 32

static void pqconn_setopt(pqconn conn,const char *key,const char *val)
{
  int i = conn->pqconn_n_params++;
  conn->pqconn_keys[i] = u8_strdup(key);
  conn->pqconn_vals[i] = u8_strdup(val);
}
static void pqconn_useopt(pqconn conn,lispval opts,char *key)
{
  lispval sym = kno_intern(key);
  lispval val = getopt(opts,sym,KNO_VOID);
  if (KNO_VOIDP(val)) return;
  int i = conn->pqconn_n_params++;
  struct U8_OUTPUT _valbuf;
  U8_INIT_STATIC_OUTPUT(_valbuf,64);
  u8_output valbuf = &_valbuf;
  conn->pqconn_keys[i] = u8_strdup(key);
  int first_value = 1;
  KNO_DO_CHOICES(v,val) {
    if (first_value) first_value=0; else u8_putc(valbuf,',');
    if (KNO_STRINGP(v)) u8_puts(valbuf,KNO_CSTRING(v));
    else if (KNO_SYMBOLP(v)) u8_puts(valbuf,KNO_SYMBOL_NAME(v));
    else if (KNO_FIXNUMP(v))
      u8_printf(valbuf,"%d",KNO_FIX2INT(v));
    else u8_printf(valbuf,"%q",v);}
  conn->pqconn_vals[i] = _valbuf.u8_outbuf;
  kno_decref(val);
}

static void recycle_pqconn(pqconn dbptr);

DEF_KNOSYM(application_name); DEF_KNOSYM(appid); DEF_KNOSYM(libpqconn);

static pqconn kno_pq_connect(u8_string spec,lispval opts,lispval colinfo)
{
  if ( (KNO_VOIDP(colinfo)) || (KNO_FALSEP(colinfo)) )
    colinfo=getopt(opts,KNOSYM_COLINFO,KNO_FALSE);
  else kno_incref(colinfo);
  int expand_dbname = 0;
  pqconn db = u8_alloc(struct KNO_PQCONN);
  KNO_INIT_FRESH_CONS(db,kno_sqlconn_type);
  db->sqldb_handler = &postgres_handler;
  db->sqlconn_options = opts; kno_incref(opts);
  db->sqlconn_colinfo = colinfo;

  db->pqconn_keys = u8_alloc_n(PQL_OPEN_MAX_PARAMS,char *);
  db->pqconn_vals = u8_alloc_n(PQL_OPEN_MAX_PARAMS,char *);
  if (spec) {
    pqconn_setopt(db,"dbname",spec);
    expand_dbname=1;}
  else pqconn_useopt(db,opts,"dbname");
  pqconn_useopt(db,opts,"host");
  pqconn_useopt(db,opts,"hostaddr");
  pqconn_useopt(db,opts,"port");

  if (kno_testopt(opts,KNOSYM(application_name),KNO_VOID))
    pqconn_useopt(db,opts,"application_name");
  else if (kno_testopt(opts,KNOSYM(appid),KNO_VOID))
    pqconn_useopt(db,opts,"application_name");
  else pqconn_setopt(db,"application_name",u8_appid());
  pqconn_setopt(db,"client_encoding","utf8");

  pqconn_useopt(db,opts,"user");
  pqconn_useopt(db,opts,"password");
  pqconn_useopt(db,opts,"passfile");

  pqconn_useopt(db,opts,"service");
  pqconn_useopt(db,opts,"options");

  pqconn_useopt(db,opts,"sslmode");
  pqconn_useopt(db,opts,"sslcompression");
  pqconn_useopt(db,opts,"sslcert");
  pqconn_useopt(db,opts,"sslkey");
  pqconn_useopt(db,opts,"sslpassword");
  pqconn_useopt(db,opts,"sslrootcert");
  pqconn_useopt(db,opts,"sslcrl");
  pqconn_useopt(db,opts,"sslcrldir");
  pqconn_useopt(db,opts,"sslsni");

#if 0
  /* Hold off on these */
  pqconn_useopt(db,opts,"channel_binding");
  pqconn_useopt(db,opts,"connect_timeout");
  pqconn_useopt(db,opts,"keepalives");
  pqconn_useopt(db,opts,"keepalives_idle");
  pqconn_useopt(db,opts,"keepalives_interval");
  pqconn_useopt(db,opts,"keepalives_count");
  pqconn_useopt(db,opts,"tcp_user_timeout");
  pqconn_useopt(db,opts,"replication");
  pqconn_useopt(db,opts,"requirepeer");
  pqconn_useopt(db,opts,"ssl_min_protocol_version");
  pqconn_useopt(db,opts,"ssl_max_protocol_version");
  pqconn_useopt(db,opts,"krbsrvname");

  pqconn_useopt(db,opts,"gssencmode");
  pqconn_useopt(db,opts,"gsslib");

  pqconn_useopt(db,opts,"target_session_attrs");
#endif

  PGconn *conn = PQconnectdbParams((const char **)(db->pqconn_keys),
				   (const char **)(db->pqconn_vals),
				   expand_dbname);
  if (conn == NULL) {
    kno_seterr(PQCError,"kno_pq_connect",spec,opts);
    recycle_pqconn(db);
    return NULL;}
  switch (PQstatus(conn)) {
  case CONNECTION_BAD: {
    kno_seterr(PQCError,"kno_pq_connect",PQerrorMessage(conn),opts);
    recycle_pqconn(db);
    PQfinish(conn);
    return NULL;}
  default: {
    if (spec) db->sqlconn_spec = u8_strdup(spec);
    else db->sqlconn_spec = u8_mkstring
	   ("postgres:%s@%s:%s/%s",PQuser(conn),PQhost(conn),PQport(conn),PQdb(conn));
    db->sqlconn_info =
      u8_mkstring("postgres:%s@%s:%s/%s(%s)",
		  PQuser(conn),PQhost(conn),PQport(conn),PQdb(conn),
		  PQoptions(conn));
    db->pqconn = conn;
    db->pqconn_raw = kno_wrap_pointer(conn,-1,NULL,KNOSYM(libpqconn),NULL);}}
  db->sqlconn_bits |= SQLCONN_EXEC_OK;
  KNO_INIT_STATIC_CONS(&(db->pqconn_oids),kno_hashtable_type);
  kno_make_eq_hashtable(&(db->pqconn_oids),128);
  return db;
}

DEFC_PRIM("pq/open",pqopen_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "Opens a POSTGRES database connection",
	  {"spec",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_FALSE},
	  {"colinfo",kno_any_type,KNO_VOID})
static lispval pqopen_prim(lispval spec,lispval opts,lispval colinfo)
{
  int decref_spec = 0, decref_opts = 0;
  u8_string spec_string = NULL;
  if (SYMBOLP(spec)) {
    spec=kno_config_get(KNO_SYMBOL_NAME(spec));
    if (CONSP(spec)) decref_spec=1;}
  if (KNO_STRINGP(spec))
    spec_string=KNO_CSTRING(spec);
  else if (KNO_TABLEP(spec)) {
    if (KNO_FALSEP(opts))
      opts = spec;
    else if (KNO_TABLEP(opts)) {
      opts = kno_make_pair(spec,opts);
      decref_opts=1;}
    else {
      kno_seterr("BadPostgresDBSpec","pqopen_prim",NULL,spec);
      if (decref_spec) kno_decref(spec);
      return KNO_ERROR;}}
  else {
      kno_seterr("BadPostgresDBSpec","pqopen_prim",NULL,spec);
      if (decref_spec) kno_decref(spec);
      return KNO_ERROR;}

  pqconn dbptr = kno_pq_connect(spec_string,opts,colinfo);
  if (decref_spec) kno_decref(spec);
  if (decref_opts) kno_decref(opts);
  if (dbptr)
    return LISPVAL(dbptr);
  else return KNO_ERROR;
}

/* Handling results */

static lispval convert_text_value(pgresult result,char *pqval,
				  Oid type,size_t size,lispval name,
				  lispval request)
{
  struct U8_INPUT _in;
  if ( (request == KNOSYM_STRING) || (request == KNO_STRING_TYPE) )
    return knostring(pqval);
  else if ( (request == KNOSYM_PACKET) || (request == KNO_PACKET_TYPE) ) {
    size_t n_bytes = (size<0) ? (strlen(pqval)) : (size);
    return kno_make_packet(NULL,n_bytes,pqval);}
  else if ( (request == KNOSYM_SECRET) || (request == KNO_SECRET_TYPE) ) {
    size_t n_bytes = (size<0) ? (strlen(pqval)) : (size);
    lispval packet = kno_init_packet(NULL,n_bytes,pqval);
    KNO_SET_CONS_TYPE(packet,kno_secret_type);
    return packet;}
  else if (size<0) {
    U8_INIT_STRING_INPUT(&_in,strlen(pqval),pqval);}
  else {U8_INIT_STRING_INPUT(&_in,size,pqval);}
  switch (type) {
  case PQTYPE_INT8: case PQTYPE_INT4: case PQTYPE_INT2:
    if ( (OIDP(request)) || (request == KNOSYM_OID) ||
	 (request == KNO_OID_TYPE) || (KNO_POOLP(request)) ) {
      unsigned long long long_val;
      if (sscanf(pqval,"%llu",&long_val)==1) {
	if (KNO_IMMEDIATEP(request))
	  return kno_scalar2oid(long_val);
	else if (OIDP(request)) {
	  KNO_OID base = KNO_OID_ADDR(request);
	  KNO_OID result = KNO_OID_PLUS(base,long_val);
	  return kno_make_oid(result);}
	else {
	  kno_pool p = kno_lisp2pool(request);
	  KNO_OID base = p->pool_base;
	  KNO_OID result = KNO_OID_PLUS(base,long_val);
	  return kno_make_oid(result);}}
      else return kno_parser(&_in);}
    else return kno_parser(&_in);
  case PQTYPE_FLOAT4: case PQTYPE_FLOAT8:
    return kno_parser(&_in);
  case PQTYPE_NUMERIC:
    return kno_parser(&_in);
  case PQTYPE_BOOL:
    if (*pqval=='T')
      return KNO_TRUE;
    else return KNO_FALSE;
  case PQTYPE_UUID: {
    U8_UUID uuid;
    u8_parseuuid(pqval,(u8_uuid)&uuid);
    return kno_make_uuid(NULL,(u8_uuid)&uuid);}
  case PQTYPE_VARCHAR: case PQTYPE_CHAR:
  case PQTYPE_TEXT: case PQTYPE_NAME:
    return knostring(pqval);
  case PQTYPE_BYTEA:
    return kno_make_packet(NULL,size,pqval);
  case PQTYPE_TIMESTAMP: {
    int year, mon, mday, hour, minute, secs, nsecs;
    double fsecs;
    int rv = sscanf(pqval,"%d-%d-%d %d:%d:%lf",
		    &year,&mon,&mday,&hour,&minute,&fsecs);
    secs = (int) (floor(fsecs));
    nsecs = (int) round(1000000000*(fsecs-secs));
    lispval base = kno_make_timestamp(NULL);
    struct U8_XTIME *xt = &(((kno_timestamp)base)->u8xtimeval);
    xt->u8_year = year;
    xt->u8_mon = mon-1;
    xt->u8_mday = mday;
    xt->u8_hour = hour;
    xt->u8_min = minute;
    xt->u8_sec = secs;
    xt->u8_nsecs = nsecs;
    time_t tick = u8_mktime(xt);
    return base;}
  case PQTYPE_TIMESTAMPTZ: {
    int year, mon, mday, hour, minute, secs, nsecs, tzoff;
    double fsecs; char sign;
    int rv = sscanf(pqval,"%d-%d-%d %d:%d:%lf%c%d",
		    &year,&mon,&mday,&hour,&minute,&fsecs,
		    &sign,&tzoff);

    secs = (int) (floor(fsecs));
    nsecs = (int) round(1000000000*(fsecs-secs));
    if (sign=='-') tzoff=-tzoff;
    lispval base = kno_make_timestamp(NULL);
    struct U8_XTIME *xt = &(((kno_timestamp)base)->u8xtimeval);
    xt->u8_year = year;
    xt->u8_mon = mon-1;
    xt->u8_mday = mday;
    xt->u8_hour = hour;
    xt->u8_min = minute;
    xt->u8_sec = secs;
    xt->u8_nsecs = nsecs;
    xt->u8_tzoff = tzoff*3600;
    xt->u8_dstoff = 0;
    time_t tick = u8_mktime(xt);
    return base;}
  default:
    return knostring(pqval);}
}

static lispval convert_binary_value(pgresult result,char *pqval,
				    Oid type,int size,
				    lispval name,lispval info)
{
  return KNO_VOID;
}

DEF_KNOSYM(convert); DEF_KNOSYM(extract);

static int get_extract_column(lispval extract_spec,lispval *names,int n_cols);

static lispval convert_results(pgresult r,u8_string query,pqconn conn,
			       lispval opts,lispval colinfo)
{
  int n_rows = PQntuples(r), n_cols = PQnfields(r);
  lispval extract_spec = kno_getopt(opts,KNOSYM(extract),KNO_VOID);
  int noempty = kno_testopt(colinfo,KNOSYM(noempty),KNO_VOID);
  lispval sortval = kno_getopt(opts,KNOSYM(sorted),KNO_VOID);
  int sorted = (!((KNO_FALSEP(sortval))||(KNO_VOIDP(sortval))));
  int no_results = ( (n_rows == 0) || (n_cols == 0) );
  lispval results = KNO_EMPTY;
  if (! no_results ) {
    /* Figure out how we'll convert result values into lisp */
    lispval names[n_cols];
    Oid types[n_cols];
    ssize_t sizes[n_cols];
    lispval reqtype[n_cols], converters[n_cols];
    unsigned char binary[n_cols];
    /* int mods[n_cols]; */
    int i = 0; while (i<n_cols) {
      char *name = PQfname(r,i); names[i] = kno_intern(name);
      lispval sym = names[i] = kno_intern(name);
      if (PQfformat(r,i)) binary[i]=1; else binary[i]=0;
      sizes[i] = PQfsize(r,i);
      /* mods[i] = PQfmod(r,i); */
      types[i] = PQftype(r,i);
      lispval gotinfo = getopt(colinfo,sym,KNO_FALSE);
      if (KNO_APPLICABLEP(gotinfo)) {
	converters[i]=gotinfo;
	reqtype[i]=KNO_FALSE;}
      else if ( (KNO_SYMBOLP(gotinfo)) || (KNO_CTYPEP(gotinfo)) ) {
	converters[i]=KNO_FALSE;
	reqtype[i]=gotinfo;}
      else if (KNO_TABLEP(gotinfo)) {
	reqtype[i] = getopt(gotinfo,KNOSYM_TYPE,KNO_VOID);
	lispval converter = getopt(gotinfo,KNOSYM(convert),KNO_FALSE);
	if (KNO_APPLICABLEP(converter))
	  converters[i]=converter;
	else {
	  kno_decref(converter);
	  converters[i]=KNO_FALSE;}}
      else {
	converters[i]=KNO_FALSE;
	reqtype[i]=KNO_FALSE;}
      i++;}
    int extract_col = -1, extract_n = n_cols;
    if ( (KNO_TRUEP(extract_spec)) && (n_cols==1) ) 
      extract_col=0;
    else extract_col=get_extract_column(extract_spec,names,n_cols);
    if (extract_col>=0) extract_n = 1;
    int row = 0; while (row<n_rows) {
      if (extract_col>=0) {
	int col = extract_col;
	if (!(PQgetisnull(r,row,col))) {
	  char *pqval = PQgetvalue(r,row,col);
	  int size = (sizes[col]<0) ? (PQgetlength(r,row,col)) : (sizes[col]);
	  lispval val = (binary[col]) ?
	    (convert_binary_value(r,pqval,types[col],size,names[col],reqtype[col])) :
	    (convert_text_value(r,pqval,types[col],size,names[col],reqtype[col]));
	  if (sorted) {
	    // TODO: make this CHOICE_PUSH when sorted results
	    KNO_ADD_TO_CHOICE(results,val);}
	  else {KNO_ADD_TO_CHOICE(results,val);}}
	row++; continue;}
      // TODO: Handle ambiguous or vector extract_specs, possibly 'schema option
      lispval frame = kno_make_slotmap(extract_n,0,NULL);
      int col = 0; while (col<n_cols) {
	if (PQgetisnull(r,row,col)) {
	  if (!(noempty)) kno_store(frame,names[col],KNO_EMPTY);
	  col++;
	  continue;}
	char *pqval = PQgetvalue(r,row,col);
	int size = (sizes[col]<0) ? (PQgetlength(r,row,col)) : (sizes[col]);
	lispval val = (binary[col]) ?
	  (convert_binary_value(r,pqval,types[col],size,names[col],reqtype[col])) :
	  (convert_text_value(r,pqval,types[col],size,names[col],reqtype[col]));
	if (!(KNO_VOIDP(val))) {
	  lispval converter = converters[col];
	  if (KNO_FALSEP(converter))
	    kno_store(frame,names[col],val);
	  else {
	    lispval converted = kno_call(NULL,converter,1,&val);
	    if ( (KNO_VOIDP(converted)) || (KNO_EMPTYP(converted)) )
	      kno_store(frame,names[col],val);
	    else if (KNO_ABORTED(converted)) {
	      u8_exception ex = u8_erreify();
	      lispval errobj = kno_wrap_exception(ex);
	      kno_store(frame,names[col],errobj);
	      kno_decref(errobj);
	      u8_free_exception(ex,1);}
	    else kno_store(frame,names[col],converted);
	    kno_decref(converted);}
	  kno_decref(val);}
	col++;}
      KNO_ADD_TO_CHOICE(results,frame);
      row++;}
    if (row>0) {
      kno_decref_elts(reqtype,n_cols);
      kno_decref_elts(converters,n_cols);}}
  if (n_rows==0) {
    if (sorted) return kno_init_vector(NULL,0,NULL);
    else return KNO_EMPTY;}
  else if (sorted)
    return kno_results2vector(results,1);
  else return kno_simplify_choice(results);
}

static int get_extract_column(lispval extract_spec,lispval *names,int n_cols)
{
  if ( (KNO_SYMBOLP(extract_spec)) || (KNO_OIDP(extract_spec)) ) {
    int j = 0; while (j<n_cols) {
      if (names[j]==extract_spec)
	return j+1;
      else j++;}
    return -1;}
  else if (KNO_FIXNUMP(extract_spec)) {
    long long ex_val = KNO_FIX2INT(extract_spec);
    if (ex_val<n_cols) return (int) ex_val;
    else return -1;}
  else return -1;
}

static lispval handle_result(pqconn conn,pgresult result,u8_string q,
			     lispval opts,lispval colinfo)
{
  int waiting = 1;
  while (waiting) {
    switch (PQresultStatus(result)) {
    case PGRES_EMPTY_QUERY: case PGRES_COMMAND_OK:
      return KNO_VOID;
    case PGRES_TUPLES_OK: case PGRES_SINGLE_TUPLE:
      return convert_results(result,q,conn,opts,colinfo);
    case PGRES_COPY_OUT: case PGRES_COPY_IN: case PGRES_COPY_BOTH:
      continue;
    case PGRES_BAD_RESPONSE: {
      pqresult_error(conn,result,q,KNO_VOID);
      return KNO_ERROR;}
    case PGRES_NONFATAL_ERROR: {}
    default:
      pqresult_error(conn,result,q,KNO_VOID);
      return KNO_ERROR;}}
  return kno_err("UnhandledPostgresStatus","postgres_result",
		 PQresStatus(PQresultStatus(result)),
		 (lispval) conn);
}

/* Execute */

static lispval pq_exec_string(pqconn conn,u8_string stmt,lispval req_opts)
{
  u8_lock_mutex(&(conn->pqconn_lock));
  pgresult pgresult = PQexec(conn->pqconn,stmt);
  u8_unlock_mutex(&(conn->pqconn_lock));
  lispval req_colinfo = kno_getopt(req_opts,KNOSYM_COLINFO,KNO_FALSE);
  lispval colinfo = conn->sqlconn_colinfo;
  lispval opts = conn->sqlconn_options;
  int free_opts = 0, free_colinfo = 0;
  colinfo = kno_merge_opts(req_colinfo,colinfo);
  opts = kno_merge_opts(req_opts,opts);
  lispval result = handle_result(conn,pgresult,stmt,opts,colinfo);
  kno_decref(opts);
  kno_decref(colinfo);
  PQclear(pgresult);
  return result;
}

static lispval exec_handler(pqconn conn,lispval stmt,lispval opts)
{
  if (STRINGP(stmt))
    return pq_exec_string(conn,KNO_CSTRING(stmt),opts);
  else return kno_err("NotSQLString","pqprims/exec_handler",conn->sqlconn_spec,stmt);
}

/* Calling */

static const char *convert_parameter(lispval arg,lispval info,
				     Oid *type,int *size,int *binary,
				     const char *freedata[]);

static lispval call_pqproc(kno_stack stack,lispval fn,int n,kno_argvec args)
{
  struct KNO_PQPROC *dbproc = (struct KNO_PQPROC *)fn;
  struct KNO_PQCONN *conn =
    KNO_GET_CONS(dbproc->sqlproc_conn,kno_sqlconn_type,struct KNO_PQCONN *);
  lispval *arginfo = dbproc->sqlproc_paramtypes;
  int n_params = dbproc->sqlproc_n_params;
  u8_string stmt = dbproc->sqlproc_qtext;
  Oid types[n_params];
  const char *values[n_params], *tofree[n_params];
  int sizes[n_params], binary[n_params];
  int i = 0; while (i<n) {
    lispval arg = args[i], info = (arginfo) ? (arginfo[i]) : (KNO_FALSE);
    lispval prepare = (KNO_APPLICABLEP(info)) ? (info) :
      (TABLEP(info)) ? (getopt(info,KNOSYM(convert),KNO_FALSE)) : (KNO_FALSE);
    int free_arg = 0;
    if (KNO_APPLICABLEP(prepare)) {
      arg = kno_call(NULL,prepare,1,&arg);
      if (KNO_CONSP(arg)) free_arg=1;}
    values[i] = convert_parameter(arg,info,&types[i],&sizes[i],&binary[i],
				  &tofree[i]);
    if (free_arg) kno_decref(arg);
    i++;}
  u8_lock_mutex(&(conn->pqconn_lock));
  pgresult pgresult = PQexecParams(conn->pqconn,stmt,n,
				   types,values,sizes,binary,
				   0);
  u8_unlock_mutex(&(conn->pqconn_lock));
  lispval result = handle_result
    (conn,pgresult,stmt,dbproc->sqlproc_options,dbproc->sqlproc_colinfo);
  i=0; while (i<n) { if (tofree[i]) u8_free(tofree[i]); i++;}
  PQclear(pgresult);
  return result;
}

DEF_KNOSYM(baseoid); 

static const char *convert_parameter(lispval arg,lispval info,
				     Oid *type,int *size,int *binary,
				     const char *freedata[])
{
  if ( (KNO_COMPOUNDP(arg)) && (KNO_COMPOUND_LENGTH(arg)>0) ) {
    struct KNO_COMPOUND *compound = (kno_compound) arg;
    arg = compound->compound_0;}
  if (KNO_OIDP(arg)) {
    KNO_OID addr = KNO_OID_ADDR(arg);
    lispval base = (OIDP(info)) ? (info) :
      (TABLEP(info)) ? (kno_getopt(info,KNOSYM(baseoid),KNO_VOID)) :
      (KNO_VOID);
    unsigned long long intval;
    if ( (OIDP(base)) && (KNO_OID_COMPARE(arg,base)>=0) ) {
      intval = KNO_OID_DIFFERENCE(arg,base);
      if (intval>INT_MAX)
	*type = PQTYPE_INT8;
      else *type = PQTYPE_INT4;}
    else {
      intval = KNO_OID2ULL(addr);
      *type = PQTYPE_INT8;}
    u8_string intstring = u8_mkstring("%llu",intval);
    *size=strlen(intstring);
    *freedata=intstring;
    *binary=0;
    return KNO_CSTRING(arg);}
  else if (KNO_STRINGP(arg)) {
    *type=get_postgres_type(arg,info);
    *size=KNO_STRLEN(arg);
    *freedata=NULL;
    *binary=0;
    return KNO_CSTRING(arg);}
  else if (KNO_SYMBOLP(arg)) {
    u8_string pname = KNO_SYMBOL_NAME(arg);
    ssize_t len = strlen(pname);
    *type=PQTYPE_VARCHAR;
    *size=len;
    *binary=0;
    *freedata=NULL;
    return pname;}
  else if (KNO_TYPEP(arg,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *tptr = (kno_timestamp) arg;
    u8_xtime xt = &(tptr->u8xtimeval);
    int tzoff = xt->u8_tzoff+xt->u8_dstoff;
    double fsecs = xt->u8_sec+(((double)xt->u8_nsecs)/1000000000.0);
    char sep; unsigned int use_tzoff;
    if (tzoff) {
      if (tzoff<0) {
	sep='-'; use_tzoff=-tzoff;}
      else { sep='+'; use_tzoff=tzoff;}}
    u8_string strval = (tzoff) ?
      (u8_mkstring("%d-%d-%d %02d:%02d:%f+%d",
		   xt->u8_year,xt->u8_mon+1,xt->u8_mday,
		   xt->u8_hour,xt->u8_min,fsecs,
		   sep,use_tzoff)) :
      (u8_mkstring("%d-%d-%d %02d:%02d:%f",
		   xt->u8_year,xt->u8_mon+1,xt->u8_mday,
		   xt->u8_hour,xt->u8_min,fsecs));
    *type=PQTYPE_VARCHAR;
    *freedata=strval;
    *size=strlen(strval);
    *binary=0;
    return strval;}
  else if (KNO_TYPEP(arg,kno_uuid_type)) {
    struct KNO_UUID *uuid = (kno_uuid) arg;
    u8_string strval = u8_uuidstring(uuid->uuid16,NULL);
    *type=PQTYPE_UUID;
    *size=strlen(strval);
    *freedata=strval;
    *binary=0;
    return strval;}
  else {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
    kno_unparse(&out,arg);
    *type=get_postgres_type(arg,info);
    *size=out.u8_write-out.u8_outbuf;
    *freedata=out.u8_outbuf;
    *binary=0;
    return out.u8_outbuf;}}

/* DBProcs */

static lispval make_pqproc
(struct KNO_PQCONN *dbp,u8_string stmt,lispval options,lispval colinfo,
 int n,kno_argvec arginfo)
{
  struct KNO_PQPROC *dbproc = u8_alloc(struct KNO_PQPROC);
  KNO_INIT_FRESH_CONS(dbproc,kno_sqlproc_type);

  /* Set up fields for SQLPROC */
  dbproc->sqldb_handler = &postgres_handler;
  dbproc->sqlproc_options = kno_merge_opts(options,dbp->sqlconn_options);
  dbproc->sqlproc_colinfo = kno_merge_opts(colinfo,dbp->sqlconn_colinfo);
  int n_params = dbproc->sqlproc_n_params = n;

  dbproc->sqlproc_conn  = (lispval)dbp; kno_incref(dbproc->sqlproc_conn);
  dbproc->sqlproc_spec  = u8_strdup(dbp->sqlconn_spec);
  dbproc->sqlproc_qtext = convert_qmarks(stmt);
#ifdef KNO_CALL_XCALL
  dbproc->fcn_call = KNO_CALL_XCALL | KNO_CALL_NOTAIL;
#else
  dbproc->fcn_call = KNO_FCN_CALL_XCALL | KNO_FCN_CALL_NOTAIL;
#endif
  dbproc->fcn_call_width = dbproc->fcn_arity = -1;
  dbproc->fcn_min_arity = dbproc->sqlproc_n_params = n_params;
  dbproc->fcn_name = u8_strdup(stmt);
  dbproc->fcn_filename = dbproc->sqlproc_spec;
  dbproc->fcn_handler.xcalln = call_pqproc;

  {
    int i = 0; lispval *specs = u8_alloc_n(n_params,lispval);
    while (i<n_params)
      if (i<n) {specs[i]=kno_incref(arginfo[i]); i++;}
      else {specs[i]=KNO_VOID; i++;}
    dbproc->sqlproc_paramtypes = specs;}

  kno_register_sqlproc((struct KNO_SQLPROC *) dbproc);
  return LISP_CONS(dbproc);
}

static lispval make_pqproc_handler
(struct KNO_SQLCONN *sqldb,
 u8_string stmt,lispval options,lispval colinfo,
 int n,kno_argvec arginfo)
{
  if (sqldb->sqldb_handler== &postgres_handler)
    return make_pqproc((kno_pqconn)sqldb,stmt,options,colinfo,n,arginfo);
  else return kno_type_error("Postgres DB","make_pqproc_handler",(lispval)sqldb);
}

static void recycle_pqproc(struct KNO_SQLPROC *c)
{
  struct KNO_PQPROC *dbproc = (struct KNO_PQPROC *)c;
  kno_release_sqlproc(c);
  kno_recycle_sqlproc(c);
}

/* Initialization */

struct TYPEMAP_INIT { u8_string parse; int code, wrap; } typemap_init[]=
  { {"date", PQTYPE_DATE, 0 },
    {"time", PQTYPE_TIME, 0 },
    {"timetz", PQTYPE_TIMETZ, 0},
    {"uuid", PQTYPE_UUID, 0},
    {NULL, -1} };

KNO_EXPORT int kno_init_pqprims()
{
  if (pqprims_initialized) return 0;
  pqprims_initialized = 1;
  kno_init_scheme();

  pqprims_module = kno_new_cmodule("pqprims",(0),kno_init_pqprims);

  kno_init_hashtable((&_pqtypemaps),200,NULL);

  struct TYPEMAP_INIT *scan = typemap_init;
  while (scan->parse) {
    lispval tag = kno_parse(scan->parse);
    lispval code = KNO_INT(scan->code);
    kno_hashtable_store((&_pqtypemaps),tag,code);
    kno_hashtable_store((&_pqtypemaps),tag,code);
    scan++;}

  postgres_handler.execute = (kno_sqlconn_exec)exec_handler;
  postgres_handler.makeproc = make_pqproc_handler;
  postgres_handler.recycle_db = (kno_sqlconn_recycler)recycle_pqconn;
  postgres_handler.recycle_proc = recycle_pqproc;

  kno_register_sqldb_handler(&postgres_handler);

  link_local_cprims();

  kno_finish_module(pqprims_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("pq/open",pqopen_prim,3,pqprims_module);
}
