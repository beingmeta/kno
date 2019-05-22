/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* sqlite.c
   This implements Kno bindings to sqlite3.
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
#include "kno/extdb.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

#include <sqlite3.h>
#include <limits.h>
#include <ctype.h>
#include <stdio.h>

KNO_EXPORT int kno_init_sqlite(void) KNO_LIBINIT_FN;
static struct KNO_EXTDB_HANDLER sqlite_handler;
static lispval sqlitecallproc(struct KNO_FUNCTION *fn,int n,lispval *args);

typedef struct KNO_SQLITE {
  KNO_EXTDB_FIELDS;
  u8_mutex sqlite_lock;
  u8_string sqlitefile, sqlitevfs;
  sqlite3 *sqlitedb;} KNO_SQLITE;
typedef struct KNO_SQLITE *kno_sqlite;

typedef struct KNO_SQLITE_PROC {
  KNO_EXTDB_PROC_FIELDS;

  u8_mutex sqliteproc_lock;

  int *sqltypes;
  sqlite3 *sqlitedb; sqlite3_stmt *stmt;} KNO_SQLITE_PROC;
typedef struct KNO_SQLITE_PROC *kno_sqlite_proc;

static u8_condition SQLiteError=_("SQLite Error");

static lispval merge_symbol, sorted_symbol;

/* Utility functions */

static unsigned char *_memdup(unsigned char *data,int len)
{
  unsigned char *duplicate = u8_alloc_n(len,unsigned char);
  memcpy(duplicate,data,len);
  return duplicate;
}

static int getboolopt(lispval opts,lispval sym,int dflt)
{
  lispval val = kno_getopt(opts,sym,KNO_VOID);
  if (KNO_VOIDP(val)) return dflt;
  else if (KNO_FALSEP(val)) return 0;
  else {
    kno_decref(val);
    return 1;}
}

/* Prototypes, etc */

static int getv2flags(lispval options,u8_string filename);

static int open_fdsqlite(struct KNO_SQLITE *fdsqlptr);
static void close_fdsqlite(struct KNO_SQLITE *fdptr,int lock);
static lispval merge_colinfo(KNO_SQLITE *dbp,lispval colinfo);

static int open_fdsqliteproc(struct KNO_SQLITE *fdsqlptr,struct KNO_SQLITE_PROC *fdp);
static void close_fdsqliteproc(struct KNO_SQLITE_PROC *dbp);

static lispval sqlite_values(sqlite3 *db,sqlite3_stmt *stmt,lispval colinfo);

#if HAVE_SQLITE3_V2
#define closedb sqlite3_close_v2
#else
#define closedb sqlite3_close
#endif

#if HAVE_SQLITE3_V2
#define newstmt(db,q,len,sptr) sqlite3_prepare_v2(db,q,len,sptr,NULL)
#else
#define newstmt(db,q,len,sptr) sqlite3_prepare(db,q,len,sptr,NULL)
#endif

/* Opening connections */

static lispval readonly_symbol, create_symbol, sharedcache_symbol;
static lispval execok_symbol, privatecache_symbol, vfs_symbol;

static int open_fdsqlite(struct KNO_SQLITE *fdsqlptr)
{
  u8_log(LOG_WARN,"open_fdsqlite",
         "Opening SQLite database %s info=%s, filename=%s",
         fdsqlptr->extdb_spec,fdsqlptr->extdb_info,fdsqlptr->sqlitefile);
  u8_lock_mutex(&(fdsqlptr->sqlite_lock));
  if (fdsqlptr->sqlitedb) {
    u8_unlock_mutex(&(fdsqlptr->sqlite_lock));
    return 0;}
  else {
    sqlite3 *db = NULL; int retval;
#if HAVE_SQLITE3_V2
    int flags = getv2flags(fdsqlptr->extdb_options,fdsqlptr->sqlitefile);
    if (flags<0) return flags;
    retval = sqlite3_open_v2(fdsqlptr->sqlitefile,&db,flags,fdsqlptr->sqlitevfs);
#else
    retval = sqlite3_open(fdsqlptr->sqlitefile,&db);
#endif
    if (retval) {
      u8_string msg = u8_strdup(sqlite3_errmsg(db));
      kno_seterr(SQLiteError,"open_sqlite",msg,
                lispval_string(fdsqlptr->sqlitefile));
      if (db) {closedb(db); fdsqlptr->sqlitedb = NULL;}
      u8_unlock_mutex(&(fdsqlptr->sqlite_lock));
      return -1;}
    fdsqlptr->sqlitedb = db;
    if (fdsqlptr->extdb_n_procs) {
      struct KNO_EXTDB_PROC **scan = fdsqlptr->extdb_procs;
      struct KNO_EXTDB_PROC **limit = scan+fdsqlptr->extdb_n_procs;
      while (scan<limit) {
        struct KNO_EXTDB_PROC *edbp = *scan++;
        struct KNO_SQLITE_PROC *sp = (struct KNO_SQLITE_PROC *)edbp;
        int retval = open_fdsqliteproc(fdsqlptr,sp);
        if (retval)
          u8_log(LOG_CRIT,"sqlite_opendb","Error '%s' updating query '%s'",
                 sqlite3_errmsg(db),sp->extdb_qtext);}}
    u8_unlock_mutex(&(fdsqlptr->sqlite_lock));
    return 1;}
}

static lispval sqlite_open_prim(lispval filename,lispval colinfo,lispval options)
{
  struct KNO_SQLITE *sqlcons; int retval;
  lispval vfs_spec = kno_getopt(options,vfs_symbol,KNO_VOID);
  u8_string vfs = NULL;
  if (KNO_VOIDP(vfs_spec)) {}
  else if (KNO_STRINGP(vfs_spec)) vfs = u8_strdup(KNO_CSTRING(vfs_spec));
  else if (KNO_SYMBOLP(vfs_spec)) vfs = u8_downcase(KNO_SYMBOL_NAME(vfs_spec));
  else return kno_type_error(_("string or symbol"),"sqlite_open_prim/VFS",vfs_spec);

#if HAVE_SQLITE3_V2
  int flags = getv2flags(options,KNO_CSTRING(filename));
  if (flags<0) {
    return KNO_ERROR_VALUE;}
#else
  int readonly = getboolopt(options,readonly_symbol,0);
  int readcreate = getboolopt(options,create_symbol,0);
  int sharedcache = getboolopt(options,sharedcache_symbol,0);
  int privcache = getboolopt(options,privatecache_symbol,0);

  if (sharedcache)
    u8_log(LOG_WARN,"sqlite_open",
           "the sqlite3_open_v2 shared cache option is not available");
  if (privcache)
    u8_log(LOG_WARN,"sqlite_open",
           "the sqlite3_open_v2 private cache option is not available");
  if ((!(readcreate))&&(!(u8_file_existsp(KNO_CSTRING(filename))))) {
    u8_seterr(kno_NoSuchFile,"opensqlite",u8_strdup(KNO_CSTRING(filename)));
    return KNO_ERROR_VALUE;}

  if (vfs)
    u8_log(LOG_WARN,"sqlite_open",
           "the sqlite3_open_v2 vfs methods are not available");
#endif

  sqlcons = u8_alloc(struct KNO_SQLITE);
  KNO_INIT_FRESH_CONS(sqlcons,kno_extdb_type);
  sqlcons->extdb_handler = &sqlite_handler;
  sqlcons->sqlitefile = u8_abspath(KNO_CSTRING(filename),NULL);
  sqlcons->extdb_colinfo = colinfo; kno_incref(colinfo);
  sqlcons->extdb_options = options; kno_incref(options);
  sqlcons->extdb_spec = sqlcons->extdb_info = u8_strdup(KNO_CSTRING(filename));
  sqlcons->sqlitedb = NULL;
  sqlcons->sqlitevfs = vfs;
  u8_init_mutex(&(sqlcons->sqlite_lock));
  u8_init_mutex(&(sqlcons->extdb_proclock));
  retval = open_fdsqlite(sqlcons);
  if (retval<0) {
    lispval lptr = LISP_CONS(sqlcons);
    kno_decref(lptr);
    return KNO_ERROR_VALUE;}
  else return LISP_CONS(sqlcons);
}

static lispval sqlite_reopen_prim(lispval db)
{
  struct KNO_EXTDB *extdb = kno_consptr(struct KNO_EXTDB *,db,kno_extdb_type);
  if (extdb->extdb_handler!= &sqlite_handler)
    return kno_type_error("Not a SQLITE DB","sqlite_close_prim",db);
  else {
    int retval = open_fdsqlite((struct KNO_SQLITE *)extdb);
    if (retval<0) return KNO_ERROR_VALUE;
    else return kno_incref(db);}
}

static void close_fdsqlite(struct KNO_SQLITE *dbp,int lock)
{
  u8_log(LOG_WARN,"Closing SQLITE db %s",dbp->extdb_spec);
  if (lock) u8_lock_mutex(&(dbp->sqlite_lock)); {
    sqlite3 *db = dbp->sqlitedb;
    struct KNO_EXTDB_PROC **scan = dbp->extdb_procs;
    struct KNO_EXTDB_PROC **limit = scan+dbp->extdb_n_procs;
    while (scan<limit)
      close_fdsqliteproc((struct KNO_SQLITE_PROC *)(*scan++));
    closedb(db);
    dbp->sqlitedb = NULL;}
  if (lock) u8_unlock_mutex(&(dbp->sqlite_lock));
}

static lispval sqlite_close_prim(lispval db)
{
  struct KNO_EXTDB *extdb = kno_consptr(struct KNO_EXTDB *,db,kno_extdb_type);
  if (extdb->extdb_handler!= &sqlite_handler)
    return kno_type_error("Not a SQLITE DB","sqlite_close_prim",db);
  close_fdsqlite((struct KNO_SQLITE *)extdb,1);
  return KNO_VOID;
}

static void recycle_fdsqlite(struct KNO_EXTDB *c)
{
  struct KNO_SQLITE *dbp = (struct KNO_SQLITE *)c;
  u8_lock_mutex(&(dbp->sqlite_lock)); {
    close_fdsqlite(dbp,0);
    if (dbp->extdb_info!=dbp->extdb_spec) u8_free(dbp->extdb_info);
    u8_free(dbp->extdb_spec);
    kno_decref(dbp->extdb_options);
    kno_decref(dbp->extdb_colinfo);
    u8_destroy_mutex(&(dbp->sqlite_lock));}
}

#if HAVE_SQLITE3_V2
static int getv2flags(lispval options,u8_string filename)
{
  int readonly = getboolopt(options,readonly_symbol,0);
  int readcreate = getboolopt(options,create_symbol,0);
  int sharedcache = getboolopt(options,sharedcache_symbol,0);
  int privcache = getboolopt(options,privatecache_symbol,0);
  int flags = 0;
  if (readonly) flags = flags|SQLITE_OPEN_READONLY;
  else if (readcreate)
    flags = flags|SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE;
  else flags = flags|SQLITE_OPEN_READWRITE;
  if (privcache==1) {
#ifdef SQLITE_OPEN_PRIVATECACHE
    flags = flags|SQLITE_OPEN_PRIVATECACHE;
#else
    u8_log(LOG_WARN,"sqlite_open","SQLite private caching is not available");
    return -1;
#endif
  }
  if (sharedcache==1) {
#ifdef SQLITE_OPEN_PRIVATECACHE
    flags = flags|SQLITE_OPEN_SHAREDCACHE;
#else
    u8_log(LOG_WARN,"sqlite_open","SQLite shared caching is not available");
    return -1;
#endif
  }
  if (u8_has_prefix(filename,"file:",1)) {
#if SQLITE_OPEN_URI
    flags = flags|SQLITE_OPEN_URI;
#else
    u8_log(LOG_WARN,"sqlite_open","SQLite file URIs are not available");
    return -1;
#endif
  }

  return flags;
}
#endif

/* SQLITE EXEC */

static lispval sqliteexec(struct KNO_SQLITE *fds,lispval string,lispval colinfo)
{
  sqlite3 *dbp = fds->sqlitedb;
  sqlite3_stmt *stmt;
  const char *errmsg="er, err";
  int retval;
  u8_lock_mutex(&(fds->sqlite_lock));
  retval = newstmt(dbp,KNO_CSTRING(string),KNO_STRLEN(string),&stmt);
  if (KNO_VOIDP(colinfo)) colinfo = fds->extdb_colinfo;
  if (retval == SQLITE_OK) {
    lispval values = sqlite_values(dbp,stmt,colinfo);
    if (KNO_ABORTP(values)) {
      errmsg = sqlite3_errmsg(dbp);
      kno_seterr(SQLiteError,"fdsqlite_call",
                errmsg,kno_incref(string));}
    sqlite3_finalize(stmt);
    u8_unlock_mutex(&(fds->sqlite_lock));
    return values;}
  else {
    lispval dbptr = (lispval)fds;
    kno_incref(dbptr);
    errmsg = sqlite3_errmsg(dbp);
    kno_seterr(SQLiteError,"fdsqlite_call",errmsg,dbptr);
    u8_unlock_mutex(&(fds->sqlite_lock));
    return KNO_ERROR_VALUE;}
}

static lispval sqliteexechandler(struct KNO_EXTDB *extdb,lispval string,
                                lispval colinfo)
{
  if (extdb->extdb_handler== &sqlite_handler) {
    struct KNO_SQLITE *sdb = (struct KNO_SQLITE *)extdb;
    u8_lock_mutex(&(sdb->sqlite_lock));
    if (!(sdb->sqlitedb)) {
      u8_unlock_mutex(&(sdb->sqlite_lock));
      if (extdb->extdb_spec!=extdb->extdb_info)
        u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s (%s)",
               extdb->extdb_spec,extdb->extdb_info);
      else u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s",
                  extdb->extdb_spec);
      open_fdsqlite(sdb);
      if (!(sdb->sqlitedb)) return KNO_ERROR_VALUE;}
    return sqliteexec(sdb,string,colinfo);}
  else return kno_type_error("SQLITE EXTDB","sqliteexechandler",(lispval)extdb);
}

/* SQLITE procs */

static lispval sqlitemakeproc
  (struct KNO_SQLITE *dbp,
   u8_string sql,int sql_len,
   lispval colinfo,int n,lispval *ptypes)
{
  sqlite3 *db = dbp->sqlitedb;
  sqlite3_stmt *stmt;
  struct KNO_SQLITE_PROC *sqlcons;
  int n_params, retval;
  if (!(db)) {
    u8_lock_mutex(&(dbp->sqlite_lock));
    if (!(dbp->sqlitedb)) {
      u8_unlock_mutex(&(dbp->sqlite_lock));
      if (dbp->extdb_spec!=dbp->extdb_info)
        u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s (%s)",
               dbp->extdb_spec,dbp->extdb_info);
      else u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s",
                  dbp->extdb_spec);
      open_fdsqlite(dbp);
      if (!(dbp->sqlitedb)) return KNO_ERROR_VALUE;}
    db = dbp->sqlitedb;}
  retval = newstmt(db,sql,sql_len,&stmt);
  if (retval) {
    lispval dbptr = (lispval)dbp;
    const char *errmsg = sqlite3_errmsg(db);
    kno_seterr(SQLiteError,"fdsqlite_call",
              errmsg,kno_incref(dbptr));
    return KNO_ERROR_VALUE;}
  else sqlcons = u8_alloc(struct KNO_SQLITE_PROC);
  KNO_INIT_FRESH_CONS(sqlcons,kno_extdb_proc_type);
  sqlcons->extdb_handler = &sqlite_handler;
  sqlcons->sqlitedb = dbp->sqlitedb;
  sqlcons->extdbptr = (lispval)dbp;
  kno_incref(sqlcons->extdbptr);
  sqlcons->sqlitedb = db; sqlcons->stmt = stmt;
  u8_init_mutex(&(sqlcons->sqliteproc_lock));
  sqlcons->fcn_filename = sqlcons->extdb_spec = u8_strdup(dbp->extdb_spec);
  /* include NUL */
  sqlcons->fcn_name = sqlcons->extdb_qtext=_memdup((u8_byte *)sql,sql_len+1);
  sqlcons->fcn_n_params = n_params = sqlite3_bind_parameter_count(stmt);
  sqlcons->fcn_ndcall = 0; sqlcons->fcn_xcall = 1; sqlcons->fcn_arity = -1;
  sqlcons->fcn_min_arity = n_params;
  sqlcons->fcn_handler.xcalln = sqlitecallproc;
  {
    lispval *paramtypes = u8_alloc_n(n_params,lispval);
    int j = 0; while (j<n_params) {
      if (j<n) paramtypes[j]=kno_incref(ptypes[j]);
      else paramtypes[j]=KNO_VOID;
      j++;}
    sqlcons->extdb_paramtypes = paramtypes;}
  sqlcons->extdb_colinfo = merge_colinfo(dbp,colinfo);
  kno_incref(dbp->extdb_colinfo);
  kno_register_extdb_proc((struct KNO_EXTDB_PROC *)sqlcons);
  return LISP_CONS(sqlcons);
}

static lispval merge_colinfo(KNO_SQLITE *dbp,lispval colinfo)
{
  if (KNO_VOIDP(colinfo)) return kno_incref(dbp->extdb_colinfo);
  else if (KNO_VOIDP(dbp->extdb_colinfo))
    return kno_incref(colinfo);
  else if (colinfo == dbp->extdb_colinfo)
    return kno_incref(colinfo);
  else if ((KNO_PAIRP(colinfo))&&
           ((KNO_CDR(colinfo)) == (dbp->extdb_colinfo)))
    return kno_incref(colinfo);
  else {
    kno_incref(dbp->extdb_colinfo); kno_incref(colinfo);
    return kno_conspair(colinfo,dbp->extdb_colinfo);}
}

static lispval sqlitemakeprochandler
  (struct KNO_EXTDB *extdb,u8_string sql,int sql_len,
   lispval colinfo,int n,lispval *ptypes)
{
  if (extdb->extdb_handler== &sqlite_handler)
    return sqlitemakeproc((kno_sqlite)extdb,sql,sql_len,colinfo,n,ptypes);
  else return kno_type_error
         ("SQLITE EXTDB","sqlitemakeprochandler",(lispval)extdb);
}

static int open_fdsqliteproc(struct KNO_SQLITE *fdsqlptr,struct KNO_SQLITE_PROC *fdp)
{
  sqlite3 *db = fdsqlptr->sqlitedb; u8_string query = fdp->extdb_qtext;
  sqlite3_stmt *stmt;
  int retval = newstmt(db,query,strlen(query),&stmt);
  if (retval)
    return retval;
  else {
    fdp->extdbptr = (lispval)fdsqlptr;
    fdp->sqlitedb = db;
    fdp->stmt = stmt;
    return 0;}
}

static void close_fdsqliteproc(struct KNO_SQLITE_PROC *dbp)
{
  sqlite3_finalize(dbp->stmt); dbp->stmt = NULL;
}

static void recycle_fdsqliteproc(struct KNO_EXTDB_PROC *c)
{
  struct KNO_SQLITE_PROC *dbp = (struct KNO_SQLITE_PROC *)c;
  int retval = kno_release_extdb_proc(c);
  if (retval<0) {
    u8_log(LOG_CRIT,"recycle_fdsqliteproc","Unexpected error during GC");
    kno_clear_errors(1);
    return;}
  close_fdsqliteproc(dbp);
  kno_decref(dbp->extdb_colinfo);
  u8_free(dbp->extdb_spec); u8_free(dbp->extdb_qtext);
  {int j = 0, lim = dbp->fcn_n_params;; while (j<lim) {
    kno_decref(dbp->extdb_paramtypes[j]); j++;}}
  u8_free(dbp->sqltypes); u8_free(dbp->extdb_paramtypes);
  kno_decref(dbp->extdbptr); dbp->extdbptr = KNO_VOID;
}

/* Calling a SQLITE proc */

static lispval sqlitecallproc(struct KNO_FUNCTION *fn,int n,lispval *args)
{
  struct KNO_SQLITE_PROC *dbproc = (struct KNO_SQLITE_PROC *)fn;
  /* We use this for the lock */
  struct KNO_SQLITE *fds = (struct KNO_SQLITE *)(dbproc->sqlitedb);
  lispval values = KNO_EMPTY_CHOICE;
  int i = 0, ret = -1;
  if (!(fds->sqlitedb)) {
    if (fds->extdb_spec!=fds->extdb_info)
      u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s (%s) for '%s'",
             fds->extdb_spec,fds->extdb_info,dbproc->extdb_qtext);
    else u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s for '%s'",
                fds->extdb_spec,dbproc->extdb_qtext);
    open_fdsqlite(fds);
    if (!(fds->sqlitedb)) return KNO_ERROR_VALUE;}
  u8_lock_mutex(&(dbproc->sqliteproc_lock));
  while (i<n) {
    lispval arg = args[i]; int dofree = 0;
    if (!(KNO_VOIDP(dbproc->extdb_paramtypes[i])))
      if (KNO_APPLICABLEP(dbproc->extdb_paramtypes[i])) {
        arg = kno_apply(dbproc->extdb_paramtypes[i],1,&arg);
        if (KNO_ABORTP(arg)) {
          u8_unlock_mutex(&(dbproc->sqliteproc_lock));
          return arg;}
        else dofree = 1;}
    if (KNO_EMPTY_CHOICEP(arg)) {
      ret = sqlite3_bind_null(dbproc->stmt,i+1);}
    else if (KNO_INTP(arg)) {
      int intval = KNO_FIX2INT(arg);
      ret = sqlite3_bind_int(dbproc->stmt,i+1,intval);}
    else if (KNO_FLONUMP(arg)) {
      double floval = KNO_FLONUM(arg);
      ret = sqlite3_bind_double(dbproc->stmt,i+1,floval);}
    else if (KNO_TYPEP(arg,kno_string_type))
      ret = sqlite3_bind_text
        (dbproc->stmt,i+1,KNO_CSTRING(arg),KNO_STRLEN(arg),SQLITE_TRANSIENT);
    else if (KNO_TYPEP(arg,kno_packet_type))
      ret = sqlite3_bind_blob
        (dbproc->stmt,i+1,
         KNO_PACKET_DATA(arg),KNO_PACKET_LENGTH(arg),
         SQLITE_TRANSIENT);
    else if (KNO_OIDP(arg)) {
      if (KNO_OIDP(dbproc->extdb_paramtypes[i])) {
        KNO_OID addr = KNO_OID_ADDR(arg);
        KNO_OID base = KNO_OID_ADDR(dbproc->extdb_paramtypes[i]);
        unsigned long offset = KNO_OID_DIFFERENCE(addr,base);
        ret = sqlite3_bind_int(dbproc->stmt,i+1,offset);}
      else {
        KNO_OID addr = KNO_OID_ADDR(arg);
        ret = sqlite3_bind_int64(dbproc->stmt,i+1,addr);}}
    else if (KNO_TYPEP(arg,kno_timestamp_type)) {
      struct U8_OUTPUT out; u8_byte buf[64]; U8_INIT_FIXED_OUTPUT(&out,64,buf);
      struct KNO_TIMESTAMP *tstamp=
        kno_consptr(struct KNO_TIMESTAMP *,arg,kno_timestamp_type);
      if ((tstamp->u8xtimeval.u8_tzoff)||(tstamp->u8xtimeval.u8_dstoff)) {
        u8_xtime_to_iso8601_x
          (&out,&(tstamp->u8xtimeval),U8_ISO8601_NOZONE|U8_ISO8601_UTC);
        u8_puts(&out," localtime");}
      else u8_xtime_to_iso8601_x
        (&out,&(tstamp->u8xtimeval),U8_ISO8601_NOZONE|U8_ISO8601_UTC);
      ret = sqlite3_bind_text
        (dbproc->stmt,i+1,
         out.u8_outbuf,out.u8_write-out.u8_outbuf,
         SQLITE_TRANSIENT);}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      kno_unparse(&out,arg);
      ret = sqlite3_bind_text
        (dbproc->stmt,i+1,
         out.u8_outbuf,out.u8_write-out.u8_outbuf,
         SQLITE_TRANSIENT);
      u8_free(out.u8_outbuf);}
    if (dofree) kno_decref(arg);
    if (ret) {
      const char *errmsg = sqlite3_errmsg(dbproc->sqlitedb);
      kno_seterr(SQLiteError,"fdsqlite_call",
                errmsg,kno_incref((lispval)fn));
      u8_unlock_mutex(&(dbproc->sqliteproc_lock));
      return KNO_ERROR_VALUE;}
    i++;}
  u8_lock_mutex(&(fds->sqlite_lock));
  values = sqlite_values(dbproc->sqlitedb,dbproc->stmt,dbproc->extdb_colinfo);
  u8_unlock_mutex(&(fds->sqlite_lock));
  sqlite3_reset(dbproc->stmt);
  u8_unlock_mutex(&(dbproc->sqliteproc_lock));
  if (KNO_ABORTP(values)) {
    const char *errmsg = sqlite3_errmsg(dbproc->sqlitedb);
    kno_seterr(SQLiteError,"fdsqlite_call",
              errmsg,kno_incref((lispval)fn));}
  return values;
}

/* Processing results */

static time_t sqlite_time_to_xtime(const char *s,struct U8_XTIME *xtp);

static lispval sqlite_values(sqlite3 *db,sqlite3_stmt *stmt,lispval colinfo)
{
  lispval results = KNO_EMPTY_CHOICE, *resultsv = NULL; int rn = 0, rmax = 0;
  lispval _colnames[16], *colnames;
  lispval _colmaps[16], *colmaps;
  lispval mergefn = kno_getopt(colinfo,merge_symbol,KNO_VOID);
  lispval sortfn = kno_getopt(colinfo,sorted_symbol,KNO_VOID);
  int sorted = (KNO_TRUEP(sortfn));
  int i = 0, n_cols = sqlite3_column_count(stmt), retval;
  struct U8_OUTPUT out;
  if (!((KNO_VOIDP(mergefn)) || (KNO_TRUEP(mergefn)) ||
        (KNO_FALSEP(mergefn)) || (KNO_APPLICABLEP(mergefn))))
    return kno_type_error("%MERGE","sqlite_values",mergefn);
  if (n_cols==0) {
    int retval = sqlite3_step(stmt);
    if ((retval) && (retval<100))
      return KNO_ERROR_VALUE;
    else return KNO_EMPTY_CHOICE;}
  else if (sorted) {
    resultsv = u8_malloc(LISPVEC_BYTELEN(64)); rmax = 64;}
  else {}
  if (n_cols>16) {
    colnames = u8_alloc_n(n_cols,lispval);
    colmaps = u8_alloc_n(n_cols,lispval);}
  else {
    colnames=_colnames;
    colmaps=_colmaps;}
  U8_INIT_OUTPUT(&out,64);
  /* [TODO] Note that the column name stuff could be cached up front
     for SQL procedures. */
  while (i<n_cols) {
    lispval colname;
    colnames[i]=colname=kno_intern((u8_string)sqlite3_column_name(stmt,i));
    colmaps[i]=(kno_getopt(colinfo,colname,KNO_VOID));
    i++;}
  while ((retval = sqlite3_step(stmt)) == SQLITE_ROW) {
    lispval result;
    struct KNO_KEYVAL *kv = u8_alloc_n(n_cols,struct KNO_KEYVAL);
    int j = 0; while (j<n_cols) {
      int coltype = sqlite3_column_type(stmt,j); lispval value;
      if (coltype<0) {
        int k = 0; while (k<j) {kno_decref(kv[k].kv_val);  k++;}
        u8_free(kv);
        kno_decref(results); u8_free(out.u8_outbuf);
        return KNO_ERROR_VALUE;}
      kv[j].kv_key = colnames[j];
      switch (coltype) {
      case SQLITE_INTEGER: {
        long long intval = sqlite3_column_int(stmt,j);
        const char *decltype = sqlite3_column_decltype(stmt,j);
        if ((decltype)&&
            ((strcasecmp(decltype,"DATETIME")==0)||
             (strcasecmp(decltype,"DATE")==0)))
          value = kno_time2timestamp((time_t)intval);
        else value = KNO_INT(intval);
        break;}
      case SQLITE_FLOAT:
        value = kno_init_double(NULL,sqlite3_column_double(stmt,j)); break;
      case SQLITE_TEXT: {
        const char *decltype = sqlite3_column_decltype(stmt,j);
        const char *textval = sqlite3_column_text(stmt,j);
        if ((decltype)&&
            ((strcasecmp(decltype,"DATETIME")==0)||
             (strcasecmp(decltype,"DATE")==0))) {
          struct U8_XTIME xt; time_t retval = sqlite_time_to_xtime(textval,&xt);
          if (retval<0) retval = u8_rfc822_to_xtime((u8_string)textval,&xt);
          if (retval<0) value = lispval_string((u8_string)textval);
          else value = kno_make_timestamp(&xt);}
        else value = lispval_string((unsigned char *)textval);
        break;}
      case SQLITE_BLOB: {
        int n_bytes = sqlite3_column_bytes(stmt,j);
        const unsigned char *blob = sqlite3_column_blob(stmt,j);
        value = kno_make_packet(NULL,n_bytes,(unsigned char *)blob);
        break;}
      case SQLITE_NULL: default:
        value = KNO_EMPTY_CHOICE; break;}
      if (KNO_VOIDP(colmaps[j]))
        kv[j].kv_val = value;
      else if (KNO_APPLICABLEP(colmaps[j])) {
        kv[j].kv_val = kno_apply(colmaps[j],1,&value);
        kno_decref(value);}
      else if (KNO_OIDP(colmaps[j]))
        if (KNO_STRINGP(value)) {
          kv[j].kv_val = kno_parse(KNO_CSTRING(value));
          kno_decref(value);}
        else {
          KNO_OID base = KNO_OID_ADDR(colmaps[j]);
          int offset = kno_getint(value);
          if (offset<0) kv[j].kv_val = value;
          else {
            kv[j].kv_val = kno_make_oid(base+offset);
            kno_decref(value);}}
      else if (colmaps[j]==KNO_TRUE)
        if (KNO_STRINGP(value)) {
          kv[j].kv_val = kno_parse(KNO_CSTRING(value));
          kno_decref(value);}
        else kv[j].kv_val = value;
      else kv[j].kv_val = value;
      j++;}
    if ((n_cols==1) && (KNO_TRUEP(mergefn))) {
      result = kv[0].kv_val;
      u8_free(kv);}
    else if ((KNO_VOIDP(mergefn)) ||
             (KNO_FALSEP(mergefn)) ||
             (KNO_TRUEP(mergefn)))
      result = kno_init_slotmap(NULL,n_cols,kv);
    else {
      lispval tmp_slotmap = kno_init_slotmap(NULL,n_cols,kv);
      result = kno_apply(mergefn,1,&tmp_slotmap);
      kno_decref(tmp_slotmap);}
    if (sorted) {
      if (KNO_ABORTP(result)) {}
      else if (rn>=rmax) {
        int new_max = ((rmax>=65536)?(rmax+65536):(rmax*2));
        lispval *newv = u8_realloc(resultsv,LISPVEC_BYTELEN(new_max));
        if (newv == NULL) {
          int delta = (new_max-rmax)/2;
          while ((newv == NULL)&&(delta>=1)) {
            new_max = rmax+delta; delta = delta/2;
            newv = u8_realloc(resultsv,LISPVEC_BYTELEN(new_max));}
          if (!(newv)) {
            kno_decref(result);
            kno_seterr(kno_OutOfMemory,"sqlite_step",NULL,KNO_VOID);
            result = KNO_ERROR_VALUE;}
          else {resultsv = newv; rmax = new_max;}}}
      resultsv[rn++]=result;}
    else {
      KNO_ADD_TO_CHOICE(results,result);}
    if (KNO_ABORTP(result)) {
      if (sorted) {
        int k = 0; while (k<rn) {
          lispval v = resultsv[k++]; kno_decref(v);}
        kno_decref(results);
        u8_free(resultsv);
        results = result;
        break;}
      else {
        kno_decref(results);
        results = result;
        break;}}}
  if (retval!=SQLITE_DONE) {
#if HAVE_SQLITE3_V2
    const char *msg = sqlite3_errstr(retval);
    kno_seterr(SQLiteError,"sqlite_step3",msg,KNO_VOID);
#endif
    kno_decref(results); if (sorted) {
      int k = 0; while (k<rn) {
        lispval v = resultsv[k++]; kno_decref(v);}
      kno_decref(results);
      u8_free(resultsv);
      resultsv = NULL;}
    results = KNO_ERROR_VALUE;}
  u8_free(out.u8_outbuf);
  kno_decref(mergefn);
  kno_decref(sortfn);
  if (colnames!=_colnames) u8_free(colnames);
  if (colmaps!=_colmaps) u8_free(colmaps);
  if (KNO_ABORTP(results)) return results;
  else if (sorted)
    return kno_wrap_vector(rn,resultsv);
  else return results;
}

/* Converting SQLITE time strings */
static time_t sqlite_time_to_xtime(const char *s,struct U8_XTIME *xtp)
{
  const char *tzstart;
  int stdpos[]={-1,4,7,10,13,16,19,20}, *pos = stdpos;
  int basicpos[]={-1,4,6,8,11,13,15,17};
  int nsecs = 0, n_elts, len = strlen(s);
  if (strchr(s,'/')) return (time_t) -1;
  memset(xtp,0,sizeof(struct U8_XTIME));
  if ((len>=11)&&(s[10]==' '))
    /* Assume odd, vaugely human-friendly format that uses space
       rather than T to separate the time */
    n_elts = sscanf(s,"%04u-%02hhu-%02hhu %02hhu:%02hhu:%02hhu.%u",
                    &xtp->u8_year,&xtp->u8_mon,
                    &xtp->u8_mday,&xtp->u8_hour,
                    &xtp->u8_min,&xtp->u8_sec,
                    &nsecs);
  else n_elts = sscanf(s,"%04u-%02hhu-%02hhuT%02hhu:%02hhu:%02hhu.%u",
                       &xtp->u8_year,&xtp->u8_mon,
                       &xtp->u8_mday,&xtp->u8_hour,
                       &xtp->u8_min,&xtp->u8_sec,
                       &nsecs);
  if ((n_elts == 1)&&(len>4)) {
    /* Assume basic format */
    n_elts = sscanf(s,"%04u%02hhu%02hhuT%02hhu%02hhu%02hhu.%u",
                    &xtp->u8_year,&xtp->u8_mon,
                    &xtp->u8_mday,&xtp->u8_hour,
                    &xtp->u8_min,&xtp->u8_sec,
                    &nsecs);
    pos = basicpos;}
  /* Give up if you can't parse anything */
  if (n_elts == 0) return (time_t) -1;
  /* Adjust month */
  xtp->u8_mon--;
  /* Set precision */
  xtp->u8_prec = n_elts;
  if (n_elts <= 6) xtp->u8_nsecs = 0;
  if (n_elts == 7) {
    const char *start = s+pos[n_elts], *scan = start; int zeros = 0;
    while (*scan == '0') {zeros++; scan++;}
    while (isdigit(*scan)) scan++;
    xtp->u8_nsecs = nsecs*(9-zeros);
    xtp->u8_prec = xtp->u8_prec+((scan-start)/3);
    tzstart = scan;}
  else tzstart = s+pos[n_elts];
  if ((tzstart)&&(*tzstart)) {
    u8_apply_tzspec(xtp,(char *)tzstart);
    xtp->u8_tick = u8_mktime(xtp);}
  else xtp->u8_tick = u8_mklocaltime(xtp);
  xtp->u8_nsecs = 0;
  return xtp->u8_tick;
}

/* Initialization */

static long long int sqlite_init = 0;

static struct KNO_EXTDB_HANDLER sqlite_handler=
  {"sqlite",NULL,NULL,NULL,NULL};

KNO_EXPORT int kno_init_sqlite()
{
  lispval module;
  if (sqlite_init) return 0;
  module = kno_new_cmodule("sqlite",0,kno_init_sqlite);

  sqlite_handler.execute = sqliteexechandler;
  sqlite_handler.makeproc = sqlitemakeprochandler;
  sqlite_handler.recycle_db = recycle_fdsqlite;
  sqlite_handler.recycle_proc = recycle_fdsqliteproc;

  kno_register_extdb_handler(&sqlite_handler);

  kno_defn(module,
          kno_make_cprim3x("SQLITE/OPEN",sqlite_open_prim,1,
                          kno_string_type,KNO_VOID,
                          -1,KNO_VOID,-1,KNO_VOID));
  kno_defn(module,kno_make_cprim1x("SQLITE/REOPEN",sqlite_reopen_prim,1,
                                 kno_extdb_type,KNO_VOID));
  kno_defn(module,kno_make_cprim1x("SQLITE/CLOSE",sqlite_close_prim,1,
                                 kno_extdb_type,KNO_VOID));
  sqlite_init = u8_millitime();

  merge_symbol = kno_intern("%merge");
  sorted_symbol = kno_intern("%sorted");
  readonly_symbol = kno_intern("readonly");
  create_symbol = kno_intern("create");
  sharedcache_symbol = kno_intern("sharedcache");
  privatecache_symbol = kno_intern("privatecache");
  vfs_symbol = kno_intern("vfs");
  execok_symbol = kno_intern("exec/ok");

  kno_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
