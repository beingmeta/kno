/* -*- Mode: C; character-encoding: utf-8; -*- */

/* sqlite.c
   This implements FramerD bindings to sqlite3.
   Copyright (C) 2007-2013 beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/numbers.h"
#include "framerd/sequences.h"
#include "framerd/texttools.h"
#include "framerd/extdb.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>

#include <sqlite3.h>
#include <limits.h>
#include <ctype.h>

FD_EXPORT int fd_init_sqlite(void) FD_LIBINIT_FN;
static struct FD_EXTDB_HANDLER sqlite_handler;
static fdtype callsqliteproc(struct FD_FUNCTION *fn,int n,fdtype *args);

typedef struct FD_SQLITE {
  FD_EXTDB_FIELDS;
#if FD_THREADS_ENABLED
  u8_mutex lock;
#endif
  sqlite3 *db;} FD_SQLITE;
typedef struct FD_SQLITE *fd_sqlite;

typedef struct FD_SQLITE_PROC {
  FD_EXTDB_PROC_FIELDS;
  int *sqltypes;
  sqlite3 *sqlitedb; sqlite3_stmt *stmt;} FD_SQLITE_PROC;
typedef struct FD_SQLITE_PROC *fd_sqlite_proc;

static fd_exception SQLiteError=_("SQLite Error");

static fdtype merge_symbol, sorted_symbol;

static fdtype intern_upcase(u8_output out,u8_string s)
{
  int c=u8_sgetc(&s);
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

static int getboolopt(fdtype opts,fdtype sym,int dflt)
{
  fdtype val=fd_getopt(opts,sym,FD_VOID);
  if (FD_VOIDP(val)) return dflt;
  else if (FD_FALSEP(val)) return 0;
  else {
    fd_decref(val);
    return 1;}
}

/* Opening connections */

static fdtype readonly_symbol, create_symbol, sharedcache_symbol, privatecache_symbol, vfs_symbol;
static fdtype execok_symbol;

static int getv2flags(fdtype options,u8_string filename);

static fdtype open_sqlite(fdtype filename,fdtype colinfo,fdtype options)
{
  sqlite3 *db=NULL; int retval;
  fdtype vfs=fd_getopt(options,vfs_symbol,FD_VOID);
  int allow_exec=getboolopt(options,execok_symbol,1);
#if HAVE_SQLITE3_OPEN_V2
  int flags=getv2flags(options,FD_STRDATA(filename));
  if (flags<0) return FD_ERROR_VALUE;
  else retval=sqlite3_open_v2
    (FD_STRDATA(filename),&db,flags,
     ((FD_STRINGP(vfs))?(FD_STRDATA(vfs)):(NULL)));
#else
  int readonly=getboolopt(options,readonly_symbol,0);
  int readcreate=getboolopt(options,create_symbol,0);
  int sharedcache=getboolopt(options,sharedcache_symbol,0);
  int privcache=getboolopt(options,privatecache_symbol,0);

  if (sharedcache)
    u8_log(LOG_WARN,"sqlite_open",
	   "the sqlite3_open_v2 shared cache option is not available");
  if (privcache)
    u8_log(LOG_WARN,"sqlite_open",
	   "the sqlite3_open_v2 private cache option is not available");
  if ((!(readcreate))&&(!(u8_file_existsp(FD_STRDATA(filename))))) {
    u8_seterr(NoSuchFile,"opensqlite",fd_strdup(FD_STRDATA(filename)));
    return FD_ERROR_VALUE;}
  
  if (!(FD_VOIDP(vfs)))
    u8_log(LOG_WARN,"sqlite_open",
	   "the sqlite3_open_v2 vfs methods are not available");
  retval=sqlite3_open(FD_STRDATA(filename),&db);
#endif
  fd_decref(vfs);
  if (retval) {
    u8_string msg=u8_strdup(sqlite3_errmsg(db));
    fd_seterr(SQLiteError,"open_sqlite",msg,filename);
    sqlite3_close(db);
    return FD_ERROR_VALUE;}
  else {
    struct FD_SQLITE *sqlcons=u8_alloc(struct FD_SQLITE);
    FD_INIT_FRESH_CONS(sqlcons,fd_extdb_type);
    sqlcons->dbhandler=&sqlite_handler; sqlcons->db=db; 
    sqlcons->colinfo=colinfo; fd_incref(colinfo);
    sqlcons->options=options; fd_incref(options);
    sqlcons->spec=sqlcons->info=u8_strdup(FD_STRDATA(filename));
    u8_init_mutex(&(sqlcons->lock));
    u8_init_mutex(&(sqlcons->proclock));
    return FDTYPE_CONS(sqlcons);}
}

#if HAVE_SQLITE3_OPEN_V2
static int getv2flags(fdtype options,u8_string filename)
{
  int readonly=getboolopt(options,readonly_symbol,0);
  int readcreate=getboolopt(options,create_symbol,0);
  int sharedcache=getboolopt(options,sharedcache_symbol,0);
  int privcache=getboolopt(options,privatecache_symbol,0);
  int flags=0;
  if (readonly) flags=flags|SQLITE_OPEN_READONLY;
  else if (readcreate)
    flags=flags|SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE;
  else flags=flags|SQLITE_OPEN_READWRITE;
  if (privcache==1) {
#ifdef SQLITE_OPEN_PRIVATECACHE
    flags=flags|SQLITE_OPEN_PRIVATECACHE;
#else
    u8_log(LOG_WARN,"sqlite_open","SQLite private caching is not available");
    return -1;
#endif
  }
  if (sharedcache==1) {
#ifdef SQLITE_OPEN_PRIVATECACHE
    flags=flags|SQLITE_OPEN_SHAREDCACHE;
#else
    u8_log(LOG_WARN,"sqlite_open","SQLite shared caching is not available");
    return -1;
#endif
  }
  if (u8_has_prefix(filename,"file:",1)) {
#if SQLITE_OPEN_URI
    flags=flags|SQLITE_OPEN_URI;
#else
    u8_log(LOG_WARN,"sqlite_open","SQLite file URIs are not available");
    return -1;
#endif
  }

  return flags;
}
#endif

static void recycle_sqlitedb(struct FD_EXTDB *c)
{
  struct FD_SQLITE *dbp=(struct FD_SQLITE *)c;
  if (dbp->info!=dbp->spec) u8_free(dbp->info);
  u8_free(dbp->spec); 
  fd_decref(dbp->options); fd_decref(dbp->colinfo);
  u8_destroy_mutex(&(dbp->lock));
  sqlite3_close(dbp->db);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Convert time string */

static time_t sqlite_time_to_xtime(const char *s,struct U8_XTIME *xtp)
{
  const char *tzstart;
  int stdpos[]={-1,4,7,10,13,16,19,20}, *pos=stdpos;
  int basicpos[]={-1,4,6,8,11,13,15,17};
  int nsecs=0, n_elts, len=strlen(s);
  if (strchr(s,'/')) return (time_t) -1;
  memset(xtp,0,sizeof(struct U8_XTIME));
  if ((len>=11)&&(s[10]==' '))
    /* Assume odd, vaugely human-friendly format that uses space
       rather than T to separate the time */
    n_elts=sscanf(s,"%04u-%02hhu-%02hhu %02hhu:%02hhu:%02hhu.%u",
		  &xtp->u8_year,&xtp->u8_mon,
		  &xtp->u8_mday,&xtp->u8_hour,
		  &xtp->u8_min,&xtp->u8_sec,
		  &nsecs);
  else n_elts=sscanf(s,"%04u-%02hhu-%02hhuT%02hhu:%02hhu:%02hhu.%u",
		     &xtp->u8_year,&xtp->u8_mon,
		     &xtp->u8_mday,&xtp->u8_hour,
		     &xtp->u8_min,&xtp->u8_sec,
		     &nsecs);
  if ((n_elts == 1)&&(len>4)) {
    /* Assume basic format */
    n_elts=sscanf(s,"%04u%02hhu%02hhuT%02hhu%02hhu%02hhu.%u",
		  &xtp->u8_year,&xtp->u8_mon,
		  &xtp->u8_mday,&xtp->u8_hour,
		  &xtp->u8_min,&xtp->u8_sec,
		  &nsecs);
    pos=basicpos;}
  /* Give up if you can't parse anything */
  if (n_elts == 0) return (time_t) -1;
  /* Adjust month */
  xtp->u8_mon--;
  /* Set precision */
  xtp->u8_prec=n_elts;
  if (n_elts <= 6) xtp->u8_nsecs=0;
  if (n_elts == 7) {
    const char *start=s+pos[n_elts], *scan=start; int zeros=0;
    while (*scan == '0') {zeros++; scan++;}
    while (isdigit(*scan)) scan++;
    xtp->u8_nsecs=nsecs*(9-zeros);
    xtp->u8_prec=xtp->u8_prec+((scan-start)/3);
    tzstart=scan;}
  else tzstart=s+pos[n_elts];
  if ((tzstart)&&(*tzstart)) {
    u8_apply_tzspec(xtp,(char *)tzstart);
    xtp->u8_tick=u8_mktime(xtp);}
  else xtp->u8_tick=u8_mklocaltime(xtp);
  xtp->u8_nsecs=0;
  return xtp->u8_tick;
}

/* Processing results */

static fdtype sqlite_values(sqlite3 *db,sqlite3_stmt *stmt,fdtype colinfo)
{
  fdtype results=FD_EMPTY_CHOICE, *resultsv=NULL; int rn=0, rmax=0;
  fdtype _colnames[16], *colnames;
  fdtype _colmaps[16], *colmaps;
  fdtype mergefn=fd_getopt(colinfo,merge_symbol,FD_VOID);
  fdtype sortfn=fd_getopt(colinfo,sorted_symbol,FD_VOID);
  int sorted=(FD_TRUEP(sortfn));
  int i=0, n_cols=sqlite3_column_count(stmt), retval;
  struct U8_OUTPUT out;
  if (!((FD_VOIDP(mergefn)) || (FD_TRUEP(mergefn)) ||
	(FD_FALSEP(mergefn)) || (FD_APPLICABLEP(mergefn))))
    return fd_type_error("%MERGE","sqlite_values",mergefn);
  if (sorted) {
    resultsv=u8_malloc(sizeof(fdtype)*64); rmax=64;}
  if (n_cols==0) {
    int retval=sqlite3_step(stmt);
    if ((retval) && (retval<100))
      return FD_ERROR_VALUE;
    else return FD_EMPTY_CHOICE;}
  if (n_cols>16) {
    colnames=u8_alloc_n(n_cols,fdtype);
    colmaps=u8_alloc_n(n_cols,fdtype);}
  else {
    colnames=_colnames;
    colmaps=_colmaps;}
  U8_INIT_OUTPUT(&out,64);
  /* [TODO] Note that the column name stuff could be cached up front
     for SQL procedures. */
  while (i<n_cols) {
    fdtype colname;
    colnames[i]=colname=
      intern_upcase(&out,(u8_string)sqlite3_column_name(stmt,i));
    colmaps[i]=(fd_getopt(colinfo,colname,FD_VOID));
    i++;}
  while ((retval=sqlite3_step(stmt))==SQLITE_ROW) {
    fdtype result;
    struct FD_KEYVAL *kv=u8_alloc_n(n_cols,struct FD_KEYVAL);
    int j=0; while (j<n_cols) {
      int coltype=sqlite3_column_type(stmt,j); fdtype value;
      if (coltype<0) {
	int k=0; while (k<j) {fd_decref(kv[k].value);  k++;}
	u8_free(kv);
	fd_decref(results); u8_free(out.u8_outbuf);
	return FD_ERROR_VALUE;}
      kv[j].key=colnames[j];
      switch (coltype) {
      case SQLITE_INTEGER: {
	long long intval=sqlite3_column_int(stmt,j);
	const char *decltype=sqlite3_column_decltype(stmt,j);
	if ((strcasecmp(decltype,"DATETIME")==0)||
	    (strcasecmp(decltype,"DATE")==0))
	  value=fd_time2timestamp((time_t)intval);
	else value=FD_INT2DTYPE(intval);
	break;}
      case SQLITE_FLOAT:
	value=fd_init_double(NULL,sqlite3_column_double(stmt,j)); break;
      case SQLITE_TEXT: {
	const char *decltype=sqlite3_column_decltype(stmt,j);
	const char *textval=sqlite3_column_text(stmt,j);
	if ((strcasecmp(decltype,"DATETIME")==0)||
	    (strcasecmp(decltype,"DATE")==0)) {
	  struct U8_XTIME xt; time_t retval=sqlite_time_to_xtime(textval,&xt);
	  if (retval<0) retval=u8_rfc822_to_xtime((u8_string)textval,&xt);
	  if (retval<0) value=fdtype_string((u8_string)textval);
	  else value=fd_make_timestamp(&xt);}
	else value=fdtype_string((unsigned char *)textval);
	break;}
      case SQLITE_BLOB: {
	int n_bytes=sqlite3_column_bytes(stmt,j);
	const unsigned char *blob=sqlite3_column_blob(stmt,j);
	value=fd_make_packet(NULL,n_bytes,(unsigned char *)blob); break;}
      case SQLITE_NULL: default:
	value=FD_EMPTY_CHOICE; break;}
      if (FD_VOIDP(colmaps[j]))
	kv[j].value=value;
      else if (FD_APPLICABLEP(colmaps[j])) {
	kv[j].value=fd_apply(colmaps[j],1,&value);
	fd_decref(value);}
      else if (FD_OIDP(colmaps[j]))
	if (FD_STRINGP(value)) {
	  kv[j].value=fd_parse(FD_STRDATA(value));
	  fd_decref(value);}
	else {
	  FD_OID base=FD_OID_ADDR(colmaps[j]);
	  int offset=fd_getint(value);
	  if (offset<0) kv[j].value=value;
	  else {
	    kv[j].value=fd_make_oid(base+offset);
	    fd_decref(value);}}
      else if (colmaps[j]==FD_TRUE)
	if (FD_STRINGP(value)) {
	  kv[j].value=fd_parse(FD_STRDATA(value));
	  fd_decref(value);}
	else kv[j].value=value;
      else kv[j].value=value;
      j++;}
    if ((n_cols==1) && (FD_TRUEP(mergefn))) {
      result=kv[0].value;
      u8_free(kv);}
    else if ((FD_VOIDP(mergefn)) || (FD_FALSEP(mergefn)) || (FD_TRUEP(mergefn)))
      result=fd_init_slotmap(NULL,n_cols,kv);
    else {
      fdtype tmp_slotmap=fd_init_slotmap(NULL,n_cols,kv);
      result=fd_apply(mergefn,1,&tmp_slotmap);
      fd_decref(tmp_slotmap);}
    if (sorted) {
      if (FD_ABORTP(result)) {}
      else if (rn>=rmax) {
	int new_max=((rmax>=65536)?(rmax+65536):(rmax*2));
	fdtype *newv=u8_realloc(resultsv,sizeof(fdtype)*new_max);
	if (newv==NULL) {
	  int delta=(new_max-rmax)/2;
	  while ((newv==NULL)&&(delta>=1)) {
	    new_max=rmax+delta; delta=delta/2;
	    newv=u8_realloc(resultsv,sizeof(fdtype)*new_max);}
	  if (!(newv)) {
	    fd_decref(result);
	    fd_seterr(fd_OutOfMemory,"sqlite_step",NULL,FD_VOID);
	    result=FD_ERROR_VALUE;}
	  else {resultsv=newv; rmax=new_max;}}}
      resultsv[rn++]=result;}
    else {
      FD_ADD_TO_CHOICE(results,result);}
    if (FD_ABORTP(result)) {
      if (sorted) {
	int k=0; while (k<rn) {
	  fdtype v=resultsv[k++]; fd_decref(v);}
	fd_decref(results);
	u8_free(resultsv);
	results=result;
	break;}
      else {
	fd_decref(results);
	results=result;
	break;}}}
  if (retval!=SQLITE_DONE) {
#if HAVE_SQLITE3_ERRSTR
    const char *msg=sqlite3_errstr(retval);
    fd_seterr(SQLiteError,"sqlite_step3",u8_strdup(msg),FD_VOID);
#endif
    fd_decref(results); if (sorted) {
      int k=0; while (k<rn) {
	fdtype v=resultsv[k++]; fd_decref(v);}
      fd_decref(results);
      u8_free(resultsv);
      resultsv=NULL;}
    results=FD_ERROR_VALUE;}
  u8_free(out.u8_outbuf);
  fd_decref(mergefn);
  fd_decref(sortfn);
  if (FD_ABORTP(results)) return results;
  else if (sorted)
    return fd_init_vector(NULL,rn,resultsv);
  else return results;
}

static fdtype sqliteexec(struct FD_SQLITE *fds,fdtype string,fdtype colinfo)
{
  sqlite3 *dbp=fds->db;
  sqlite3_stmt *stmt;
  const char *errmsg="er, err";
  int retval;
  u8_lock_mutex(&(fds->lock));
#if HAVE_SQLITE3_PREPARE_V2
  retval=
    sqlite3_prepare_v2(dbp,FD_STRDATA(string),FD_STRLEN(string),
		       &stmt,NULL);
#else
  retval=
    sqlite3_prepare(dbp,FD_STRDATA(string),FD_STRLEN(string),&stmt,NULL);
#endif
  if (FD_VOIDP(colinfo)) colinfo=fds->colinfo;
  if (retval==SQLITE_OK) {
    fdtype values=sqlite_values(dbp,stmt,colinfo);
    if (FD_ABORTP(values)) {
      errmsg=sqlite3_errmsg(dbp);
      fd_seterr(SQLiteError,"fdsqlite_call",u8_strdup(errmsg),fd_incref(string));}
    sqlite3_finalize(stmt);
    u8_unlock_mutex(&(fds->lock));
    return values;}
  else {
    fdtype dbptr=(fdtype)fds; fd_incref(dbptr);
    errmsg=sqlite3_errmsg(dbp);
    fd_seterr(SQLiteError,"fdsqlite_call",u8_strdup(errmsg),dbptr);
    u8_unlock_mutex(&(fds->lock));
    return FD_ERROR_VALUE;}
}

static fdtype sqliteexechandler(struct FD_EXTDB *extdb,fdtype string,fdtype colinfo)
{
  if (extdb->dbhandler==&sqlite_handler)
    return sqliteexec((fd_sqlite)extdb,string,colinfo);
  else return fd_type_error("SQLITE EXTDB","sqliteexechandler",(fdtype)extdb);
}

/* SQLITE procs */

static fdtype sqlitemakeproc
  (struct FD_SQLITE *dbp,
   u8_string stmt,int stmt_len,
   fdtype colinfo,int n,fdtype *ptypes)
{
  sqlite3 *db=dbp->db;
  int n_params, retval;
  struct FD_SQLITE_PROC *sqlcons=u8_alloc(struct FD_SQLITE_PROC);
  FD_INIT_FRESH_CONS(sqlcons,fd_extdb_proc_type);
#if HAVE_SQLITE3_PREPARE_V2
  retval=
    sqlite3_prepare_v2(db,stmt,stmt_len+1,&(sqlcons->stmt),NULL);
#else
  retval=
    sqlite3_prepare(db,stmt,stmt_len+1,&(sqlcons->stmt),NULL);
#endif
  if (retval) {
    fdtype dbptr=(fdtype)dbp;
    const char *errmsg=sqlite3_errmsg(db);
    fd_seterr(SQLiteError,"fdsqlite_call",u8_strdup(errmsg),fd_incref(dbptr));
    return FD_ERROR_VALUE;}
  sqlcons->dbhandler=&sqlite_handler;
  sqlcons->db=(fdtype)dbp; fd_incref(sqlcons->db);
  sqlcons->sqlitedb=db;
  sqlcons->filename=sqlcons->spec=u8_strdup(dbp->spec);
  sqlcons->name=sqlcons->qtext=_memdup(stmt,stmt_len+1); /* include NUL */
  sqlcons->n_params=n_params=sqlite3_bind_parameter_count(sqlcons->stmt);
  sqlcons->ndprim=0; sqlcons->xprim=1; sqlcons->arity=-1;
  sqlcons->min_arity=n_params;
  sqlcons->handler.xcalln=callsqliteproc;
  {
    fdtype *paramtypes=u8_alloc_n(n_params,fdtype);
    int j=0; while (j<n_params) {
      if (j<n) paramtypes[j]=fd_incref(ptypes[j]);
      else paramtypes[j]=FD_VOID;
      j++;}
    sqlcons->paramtypes=paramtypes;}
  if (FD_VOIDP(colinfo))
    sqlcons->colinfo=fd_incref(dbp->colinfo);
  else if (FD_VOIDP(dbp->colinfo))
    sqlcons->colinfo=fd_incref(colinfo);
  else {
    fd_incref(colinfo); fd_incref(dbp->colinfo);
    sqlcons->colinfo=fd_init_pair(NULL,colinfo,dbp->colinfo);}
  fd_register_extdb_proc((struct FD_EXTDB_PROC *)sqlcons);
  return FDTYPE_CONS(sqlcons);
}

static fdtype sqlitemakeprochandler
  (struct FD_EXTDB *extdb,
   u8_string stmt,int stmt_len,
   fdtype colinfo,int n,fdtype *ptypes)
{
  if (extdb->dbhandler==&sqlite_handler)
    return sqlitemakeproc((fd_sqlite)extdb,stmt,stmt_len,colinfo,n,ptypes);
  else return fd_type_error("SQLITE EXTDB","sqlitemakeprochandler",(fdtype)extdb);
}

static void recycle_sqliteproc(struct FD_EXTDB_PROC *c)
{
  struct FD_SQLITE_PROC *dbp=(struct FD_SQLITE_PROC *)c;
  fd_release_extdb_proc(c);
  fd_decref(dbp->colinfo); 
  u8_free(dbp->spec); u8_free(dbp->qtext);
  {int j=0, lim=dbp->n_params;; while (j<lim) {
    fd_decref(dbp->paramtypes[j]); j++;}}
  u8_free(dbp->sqltypes); u8_free(dbp->paramtypes);
  sqlite3_finalize(dbp->stmt);
  fd_decref(dbp->db);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static fdtype callsqliteproc(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  struct FD_SQLITE_PROC *dbproc=(struct FD_SQLITE_PROC *)fn;
  /* We use this for the lock */
  struct FD_SQLITE *fds=(struct FD_SQLITE *)(dbproc->db);
  fdtype values=FD_EMPTY_CHOICE;
  int i=0, ret=-1;
  u8_lock_mutex(&(fds->lock));
  while (i<n) {
    fdtype arg=args[i]; int dofree=0;
    if (!(FD_VOIDP(dbproc->paramtypes[i])))
      if (FD_APPLICABLEP(dbproc->paramtypes[i])) {
	arg=fd_apply(dbproc->paramtypes[i],1,&arg);
	if (FD_ABORTP(arg)) {
	  u8_unlock_mutex(&(fds->lock));
	  return arg;}
	else dofree=1;}
    if (FD_PRIM_TYPEP(arg,fd_fixnum_type)) {
      int intval=FD_FIX2INT(arg);
      ret=sqlite3_bind_int(dbproc->stmt,i+1,intval);}
    else if (FD_PRIM_TYPEP(arg,fd_double_type)) {
      double floval=FD_FLONUM(arg);
      ret=sqlite3_bind_double(dbproc->stmt,i+1,floval);}
    else if (FD_PRIM_TYPEP(arg,fd_string_type)) 
      ret=sqlite3_bind_text
	(dbproc->stmt,i+1,FD_STRDATA(arg),FD_STRLEN(arg),SQLITE_TRANSIENT);
    else if (FD_PRIM_TYPEP(arg,fd_packet_type)) 
      ret=sqlite3_bind_blob
	(dbproc->stmt,i+1,FD_PACKET_DATA(arg),FD_PACKET_LENGTH(arg),SQLITE_TRANSIENT);
    else if (FD_OIDP(arg)) {
      if (FD_OIDP(dbproc->paramtypes[i])) {
	FD_OID addr=FD_OID_ADDR(arg);
	FD_OID base=FD_OID_ADDR(dbproc->paramtypes[i]);
	unsigned long offset=FD_OID_DIFFERENCE(addr,base);
	ret=sqlite3_bind_int(dbproc->stmt,i+1,offset);}
      else {
	FD_OID addr=FD_OID_ADDR(arg);
	ret=sqlite3_bind_int64(dbproc->stmt,i+1,addr);}}
    else if (FD_PRIM_TYPEP(arg,fd_timestamp_type)) {
      struct FD_TIMESTAMP *tstamp=FD_GET_CONS(arg,fd_timestamp_type,struct FD_TIMESTAMP *);
      if (tstamp->xtime.u8_tzoff) {
	struct U8_OUTPUT out; u8_byte buf[64]; U8_INIT_FIXED_OUTPUT(&out,64,buf);
	u8_xtime_to_iso8601(&out,&(tstamp->xtime));
	ret=sqlite3_bind_text
	  (dbproc->stmt,i+1,out.u8_outbuf,out.u8_outptr-out.u8_outbuf,SQLITE_TRANSIENT);}
      else {
	time_t tick=tstamp->xtime.u8_tick;
	if (tick>INT_MAX) 
	  ret=sqlite3_bind_int(dbproc->stmt,i+1,(int)tick);
	else ret=sqlite3_bind_int64(dbproc->stmt,i+1,(sqlite_int64)tick);}}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      fd_unparse(&out,arg);
      ret=sqlite3_bind_text
	(dbproc->stmt,i+1,out.u8_outbuf,out.u8_outptr-out.u8_outbuf,SQLITE_TRANSIENT);
      u8_free(out.u8_outbuf);}
    if (dofree) fd_decref(arg);
    if (ret) {
      const char *errmsg=sqlite3_errmsg(dbproc->sqlitedb);
      fd_seterr(SQLiteError,"fdsqlite_call",
		u8_strdup(errmsg),fd_incref((fdtype)fn));
      u8_unlock_mutex(&(fds->lock));
      return FD_ERROR_VALUE;}
    i++;}
  values=sqlite_values(dbproc->sqlitedb,dbproc->stmt,dbproc->colinfo);
  sqlite3_reset(dbproc->stmt);
  u8_unlock_mutex(&(fds->lock));
  if (FD_ABORTP(values)) {
    const char *errmsg=sqlite3_errmsg(dbproc->sqlitedb);
    fd_seterr(SQLiteError,"fdsqlite_call",u8_strdup(errmsg),fd_incref((fdtype)fn));}
  return values;
}

/* Initialization */

static int sqlite_init=0;

static struct FD_EXTDB_HANDLER sqlite_handler=
  {"sqlite",NULL,NULL,NULL,NULL};

FD_EXPORT int fd_init_sqlite()
{
  fdtype module;
  if (sqlite_init) return 0;
  module=fd_new_module("SQLITE",0);

  sqlite_handler.execute=sqliteexechandler;
  sqlite_handler.makeproc=sqlitemakeprochandler;
  sqlite_handler.recycle_extdb=recycle_sqlitedb;
  sqlite_handler.recycle_extdb_proc=recycle_sqliteproc;

  fd_register_extdb_handler(&sqlite_handler);

  fd_defn(module,
	  fd_make_cprim3x("SQLITE/OPEN",open_sqlite,1,
			  fd_string_type,FD_VOID,
			  -1,FD_VOID,-1,FD_VOID));
  sqlite_init=1;

  merge_symbol=fd_intern("%MERGE");
  sorted_symbol=fd_intern("%SORTED");
  readonly_symbol=fd_intern("READONLY");
  create_symbol=fd_intern("CREATE");
  sharedcache_symbol=fd_intern("SHAREDCACHE");
  privatecache_symbol=fd_intern("PRIVATECACHE");
  vfs_symbol=fd_intern("VFS");
  execok_symbol=fd_intern("EXEC/OK");
  
  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}
