/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* sqlite.c
   This implements FramerD bindings to sqlite3.
   Copyright (C) 2007-2018 beingmeta, inc.
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
#include <libu8/u8crypto.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

#include <sqlite3.h>
#include <limits.h>
#include <ctype.h>
#include <stdio.h>

FD_EXPORT int fd_init_sqlite(void) FD_LIBINIT_FN;
static struct FD_EXTDB_HANDLER sqlite_handler;
static lispval sqlitecallproc(struct FD_FUNCTION *fn,int n,lispval *args);

typedef struct FD_SQLITE {
  FD_EXTDB_FIELDS;
  u8_mutex sqlite_lock;
  u8_string sqlitefile, sqlitevfs;
  sqlite3 *sqlitedb;} FD_SQLITE;
typedef struct FD_SQLITE *fd_sqlite;

typedef struct FD_SQLITE_PROC {
  FD_EXTDB_PROC_FIELDS;

  u8_mutex sqliteproc_lock;

  int *sqltypes;
  sqlite3 *sqlitedb; sqlite3_stmt *stmt;} FD_SQLITE_PROC;
typedef struct FD_SQLITE_PROC *fd_sqlite_proc;

static u8_condition SQLiteError=_("SQLite Error");

static lispval merge_symbol, sorted_symbol;

/* Utility functions */

static lispval intern_upcase(u8_output out,u8_string s)
{
  int c = u8_sgetc(&s);
  out->u8_write = out->u8_outbuf;
  while (c>=0) {
    u8_putc(out,u8_toupper(c));
    c = u8_sgetc(&s);}
  return fd_make_symbol(out->u8_outbuf,out->u8_write-out->u8_outbuf);
}

static unsigned char *_memdup(unsigned char *data,int len)
{
  unsigned char *duplicate = u8_alloc_n(len,unsigned char);
  memcpy(duplicate,data,len);
  return duplicate;
}

static int getboolopt(lispval opts,lispval sym,int dflt)
{
  lispval val = fd_getopt(opts,sym,FD_VOID);
  if (FD_VOIDP(val)) return dflt;
  else if (FD_FALSEP(val)) return 0;
  else {
    fd_decref(val);
    return 1;}
}

/* Prototypes, etc */

static int getv2flags(lispval options,u8_string filename);

static int open_fdsqlite(struct FD_SQLITE *fdsqlptr);
static void close_fdsqlite(struct FD_SQLITE *fdptr,int lock);
static lispval merge_colinfo(FD_SQLITE *dbp,lispval colinfo);

static int open_fdsqliteproc(struct FD_SQLITE *fdsqlptr,struct FD_SQLITE_PROC *fdp);
static void close_fdsqliteproc(struct FD_SQLITE_PROC *dbp);

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

static int open_fdsqlite(struct FD_SQLITE *fdsqlptr)
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
      fd_seterr(SQLiteError,"open_sqlite",msg,
                lispval_string(fdsqlptr->sqlitefile));
      if (db) {closedb(db); fdsqlptr->sqlitedb = NULL;}
      u8_unlock_mutex(&(fdsqlptr->sqlite_lock));
      return -1;}
    fdsqlptr->sqlitedb = db;
    if (fdsqlptr->extdb_n_procs) {
      struct FD_EXTDB_PROC **scan = fdsqlptr->extdb_procs;
      struct FD_EXTDB_PROC **limit = scan+fdsqlptr->extdb_n_procs;
      while (scan<limit) {
        struct FD_EXTDB_PROC *edbp = *scan++;
        struct FD_SQLITE_PROC *sp = (struct FD_SQLITE_PROC *)edbp;
        int retval = open_fdsqliteproc(fdsqlptr,sp);
        if (retval)
          u8_log(LOG_CRIT,"sqlite_opendb","Error '%s' updating query '%s'",
                 sqlite3_errmsg(db),sp->extdb_qtext);}}
    u8_unlock_mutex(&(fdsqlptr->sqlite_lock));
    return 1;}
}

static lispval sqlite_open_prim(lispval filename,lispval colinfo,lispval options)
{
  struct FD_SQLITE *sqlcons; int retval;
  lispval vfs_spec = fd_getopt(options,vfs_symbol,FD_VOID);
  u8_string vfs = NULL;
  if (FD_VOIDP(vfs_spec)) {}
  else if (FD_STRINGP(vfs_spec)) vfs = u8_strdup(FD_CSTRING(vfs_spec));
  else if (FD_SYMBOLP(vfs_spec)) vfs = u8_downcase(FD_SYMBOL_NAME(vfs_spec));
  else return fd_type_error(_("string or symbol"),"sqlite_open_prim/VFS",vfs_spec);

#if HAVE_SQLITE3_V2
  int flags = getv2flags(options,FD_CSTRING(filename));
  if (flags<0) {
    return FD_ERROR_VALUE;}
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
  if ((!(readcreate))&&(!(u8_file_existsp(FD_CSTRING(filename))))) {
    u8_seterr(fd_NoSuchFile,"opensqlite",u8_strdup(FD_CSTRING(filename)));
    return FD_ERROR_VALUE;}

  if (vfs)
    u8_log(LOG_WARN,"sqlite_open",
           "the sqlite3_open_v2 vfs methods are not available");
#endif

  sqlcons = u8_alloc(struct FD_SQLITE);
  FD_INIT_FRESH_CONS(sqlcons,fd_extdb_type);
  sqlcons->extdb_handler = &sqlite_handler;
  sqlcons->sqlitefile = u8_abspath(FD_CSTRING(filename),NULL);
  sqlcons->extdb_colinfo = colinfo; fd_incref(colinfo);
  sqlcons->extdb_options = options; fd_incref(options);
  sqlcons->extdb_spec = sqlcons->extdb_info = u8_strdup(FD_CSTRING(filename));
  sqlcons->sqlitedb = NULL;
  sqlcons->sqlitevfs = vfs;
  u8_init_mutex(&(sqlcons->sqlite_lock));
  u8_init_mutex(&(sqlcons->extdb_proclock));
  retval = open_fdsqlite(sqlcons);
  if (retval<0) {
    lispval lptr = LISP_CONS(sqlcons);
    fd_decref(lptr);
    return FD_ERROR_VALUE;}
  else return LISP_CONS(sqlcons);
}

static lispval sqlite_reopen_prim(lispval db)
{
  struct FD_EXTDB *extdb = fd_consptr(struct FD_EXTDB *,db,fd_extdb_type);
  if (extdb->extdb_handler!= &sqlite_handler)
    return fd_type_error("Not a SQLITE DB","sqlite_close_prim",db);
  else {
    int retval = open_fdsqlite((struct FD_SQLITE *)extdb);
    if (retval<0) return FD_ERROR_VALUE;
    else return fd_incref(db);}
}

static void close_fdsqlite(struct FD_SQLITE *dbp,int lock)
{
  u8_log(LOG_WARN,"Closing SQLITE db %s",dbp->extdb_spec);
  if (lock) u8_lock_mutex(&(dbp->sqlite_lock)); {
    sqlite3 *db = dbp->sqlitedb;
    struct FD_EXTDB_PROC **scan = dbp->extdb_procs;
    struct FD_EXTDB_PROC **limit = scan+dbp->extdb_n_procs;
    while (scan<limit)
      close_fdsqliteproc((struct FD_SQLITE_PROC *)(*scan++));
    closedb(db);
    dbp->sqlitedb = NULL;}
  if (lock) u8_unlock_mutex(&(dbp->sqlite_lock));
}

static lispval sqlite_close_prim(lispval db)
{
  struct FD_EXTDB *extdb = fd_consptr(struct FD_EXTDB *,db,fd_extdb_type);
  if (extdb->extdb_handler!= &sqlite_handler)
    return fd_type_error("Not a SQLITE DB","sqlite_close_prim",db);
  close_fdsqlite((struct FD_SQLITE *)extdb,1);
  return FD_VOID;
}

static void recycle_fdsqlite(struct FD_EXTDB *c)
{
  struct FD_SQLITE *dbp = (struct FD_SQLITE *)c;
  u8_lock_mutex(&(dbp->sqlite_lock)); {
    close_fdsqlite(dbp,0);
    if (dbp->extdb_info!=dbp->extdb_spec) u8_free(dbp->extdb_info);
    u8_free(dbp->extdb_spec);
    fd_decref(dbp->extdb_options);
    fd_decref(dbp->extdb_colinfo);
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

static lispval sqliteexec(struct FD_SQLITE *fds,lispval string,lispval colinfo)
{
  sqlite3 *dbp = fds->sqlitedb;
  sqlite3_stmt *stmt;
  const char *errmsg="er, err";
  int retval;
  u8_lock_mutex(&(fds->sqlite_lock));
  retval = newstmt(dbp,FD_CSTRING(string),FD_STRLEN(string),&stmt);
  if (FD_VOIDP(colinfo)) colinfo = fds->extdb_colinfo;
  if (retval == SQLITE_OK) {
    lispval values = sqlite_values(dbp,stmt,colinfo);
    if (FD_ABORTP(values)) {
      errmsg = sqlite3_errmsg(dbp);
      fd_seterr(SQLiteError,"fdsqlite_call",
                errmsg,fd_incref(string));}
    sqlite3_finalize(stmt);
    u8_unlock_mutex(&(fds->sqlite_lock));
    return values;}
  else {
    lispval dbptr = (lispval)fds;
    fd_incref(dbptr);
    errmsg = sqlite3_errmsg(dbp);
    fd_seterr(SQLiteError,"fdsqlite_call",errmsg,dbptr);
    u8_unlock_mutex(&(fds->sqlite_lock));
    return FD_ERROR_VALUE;}
}

static lispval sqliteexechandler(struct FD_EXTDB *extdb,lispval string,
                                lispval colinfo)
{
  if (extdb->extdb_handler== &sqlite_handler) {
    struct FD_SQLITE *sdb = (struct FD_SQLITE *)extdb;
    u8_lock_mutex(&(sdb->sqlite_lock));
    if (!(sdb->sqlitedb)) {
      u8_unlock_mutex(&(sdb->sqlite_lock));
      if (extdb->extdb_spec!=extdb->extdb_info)
        u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s (%s)",
               extdb->extdb_spec,extdb->extdb_info);
      else u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s",
                  extdb->extdb_spec);
      open_fdsqlite(sdb);
      if (!(sdb->sqlitedb)) return FD_ERROR_VALUE;}
    return sqliteexec(sdb,string,colinfo);}
  else return fd_type_error("SQLITE EXTDB","sqliteexechandler",(lispval)extdb);
}

/* SQLITE procs */

static lispval sqlitemakeproc
  (struct FD_SQLITE *dbp,
   u8_string sql,int sql_len,
   lispval colinfo,int n,lispval *ptypes)
{
  sqlite3 *db = dbp->sqlitedb;
  sqlite3_stmt *stmt;
  struct FD_SQLITE_PROC *sqlcons;
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
      if (!(dbp->sqlitedb)) return FD_ERROR_VALUE;}
    db = dbp->sqlitedb;}
  retval = newstmt(db,sql,sql_len,&stmt);
  if (retval) {
    lispval dbptr = (lispval)dbp;
    const char *errmsg = sqlite3_errmsg(db);
    fd_seterr(SQLiteError,"fdsqlite_call",
              errmsg,fd_incref(dbptr));
    return FD_ERROR_VALUE;}
  else sqlcons = u8_alloc(struct FD_SQLITE_PROC);
  FD_INIT_FRESH_CONS(sqlcons,fd_extdb_proc_type);
  sqlcons->extdb_handler = &sqlite_handler;
  sqlcons->sqlitedb = dbp->sqlitedb;
  sqlcons->extdbptr = (lispval)dbp;
  fd_incref(sqlcons->extdbptr);
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
      if (j<n) paramtypes[j]=fd_incref(ptypes[j]);
      else paramtypes[j]=FD_VOID;
      j++;}
    sqlcons->extdb_paramtypes = paramtypes;}
  sqlcons->extdb_colinfo = merge_colinfo(dbp,colinfo);
  fd_incref(dbp->extdb_colinfo);
  fd_register_extdb_proc((struct FD_EXTDB_PROC *)sqlcons);
  return LISP_CONS(sqlcons);
}

static lispval merge_colinfo(FD_SQLITE *dbp,lispval colinfo)
{
  if (FD_VOIDP(colinfo)) return fd_incref(dbp->extdb_colinfo);
  else if (FD_VOIDP(dbp->extdb_colinfo))
    return fd_incref(colinfo);
  else if (colinfo == dbp->extdb_colinfo)
    return fd_incref(colinfo);
  else if ((FD_PAIRP(colinfo))&&
           ((FD_CDR(colinfo)) == (dbp->extdb_colinfo)))
    return fd_incref(colinfo);
  else {
    fd_incref(dbp->extdb_colinfo); fd_incref(colinfo);
    return fd_conspair(colinfo,dbp->extdb_colinfo);}
}

static lispval sqlitemakeprochandler
  (struct FD_EXTDB *extdb,u8_string sql,int sql_len,
   lispval colinfo,int n,lispval *ptypes)
{
  if (extdb->extdb_handler== &sqlite_handler)
    return sqlitemakeproc((fd_sqlite)extdb,sql,sql_len,colinfo,n,ptypes);
  else return fd_type_error
         ("SQLITE EXTDB","sqlitemakeprochandler",(lispval)extdb);
}

static int open_fdsqliteproc(struct FD_SQLITE *fdsqlptr,struct FD_SQLITE_PROC *fdp)
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

static void close_fdsqliteproc(struct FD_SQLITE_PROC *dbp)
{
  sqlite3_finalize(dbp->stmt); dbp->stmt = NULL;
}

static void recycle_fdsqliteproc(struct FD_EXTDB_PROC *c)
{
  struct FD_SQLITE_PROC *dbp = (struct FD_SQLITE_PROC *)c;
  int retval = fd_release_extdb_proc(c);
  if (retval<0) {
    u8_log(LOG_CRIT,"recycle_fdsqliteproc","Unexpected error during GC");
    fd_clear_errors(1);
    return;}
  close_fdsqliteproc(dbp);
  fd_decref(dbp->extdb_colinfo);
  u8_free(dbp->extdb_spec); u8_free(dbp->extdb_qtext);
  {int j = 0, lim = dbp->fcn_n_params;; while (j<lim) {
    fd_decref(dbp->extdb_paramtypes[j]); j++;}}
  u8_free(dbp->sqltypes); u8_free(dbp->extdb_paramtypes);
  fd_decref(dbp->extdbptr); dbp->extdbptr = FD_VOID;
}

/* Calling a SQLITE proc */

static lispval sqlitecallproc(struct FD_FUNCTION *fn,int n,lispval *args)
{
  struct FD_SQLITE_PROC *dbproc = (struct FD_SQLITE_PROC *)fn;
  /* We use this for the lock */
  struct FD_SQLITE *fds = (struct FD_SQLITE *)(dbproc->sqlitedb);
  lispval values = FD_EMPTY_CHOICE;
  int i = 0, ret = -1;
  if (!(fds->sqlitedb)) {
    if (fds->extdb_spec!=fds->extdb_info)
      u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s (%s) for '%s'",
             fds->extdb_spec,fds->extdb_info,dbproc->extdb_qtext);
    else u8_log(LOG_WARN,"SQLITE/REOPEN","Reopening %s for '%s'",
                fds->extdb_spec,dbproc->extdb_qtext);
    open_fdsqlite(fds);
    if (!(fds->sqlitedb)) return FD_ERROR_VALUE;}
  u8_lock_mutex(&(dbproc->sqliteproc_lock));
  while (i<n) {
    lispval arg = args[i]; int dofree = 0;
    if (!(FD_VOIDP(dbproc->extdb_paramtypes[i])))
      if (FD_APPLICABLEP(dbproc->extdb_paramtypes[i])) {
        arg = fd_apply(dbproc->extdb_paramtypes[i],1,&arg);
        if (FD_ABORTP(arg)) {
          u8_unlock_mutex(&(dbproc->sqliteproc_lock));
          return arg;}
        else dofree = 1;}
    if (FD_EMPTY_CHOICEP(arg)) {
      ret = sqlite3_bind_null(dbproc->stmt,i+1);}
    else if (FD_INTP(arg)) {
      int intval = FD_FIX2INT(arg);
      ret = sqlite3_bind_int(dbproc->stmt,i+1,intval);}
    else if (FD_FLONUMP(arg)) {
      double floval = FD_FLONUM(arg);
      ret = sqlite3_bind_double(dbproc->stmt,i+1,floval);}
    else if (FD_TYPEP(arg,fd_string_type))
      ret = sqlite3_bind_text
        (dbproc->stmt,i+1,FD_CSTRING(arg),FD_STRLEN(arg),SQLITE_TRANSIENT);
    else if (FD_TYPEP(arg,fd_packet_type))
      ret = sqlite3_bind_blob
        (dbproc->stmt,i+1,
         FD_PACKET_DATA(arg),FD_PACKET_LENGTH(arg),
         SQLITE_TRANSIENT);
    else if (FD_OIDP(arg)) {
      if (FD_OIDP(dbproc->extdb_paramtypes[i])) {
        FD_OID addr = FD_OID_ADDR(arg);
        FD_OID base = FD_OID_ADDR(dbproc->extdb_paramtypes[i]);
        unsigned long offset = FD_OID_DIFFERENCE(addr,base);
        ret = sqlite3_bind_int(dbproc->stmt,i+1,offset);}
      else {
        FD_OID addr = FD_OID_ADDR(arg);
        ret = sqlite3_bind_int64(dbproc->stmt,i+1,addr);}}
    else if (FD_TYPEP(arg,fd_timestamp_type)) {
      struct U8_OUTPUT out; u8_byte buf[64]; U8_INIT_FIXED_OUTPUT(&out,64,buf);
      struct FD_TIMESTAMP *tstamp=
        fd_consptr(struct FD_TIMESTAMP *,arg,fd_timestamp_type);
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
      fd_unparse(&out,arg);
      ret = sqlite3_bind_text
        (dbproc->stmt,i+1,
         out.u8_outbuf,out.u8_write-out.u8_outbuf,
         SQLITE_TRANSIENT);
      u8_free(out.u8_outbuf);}
    if (dofree) fd_decref(arg);
    if (ret) {
      const char *errmsg = sqlite3_errmsg(dbproc->sqlitedb);
      fd_seterr(SQLiteError,"fdsqlite_call",
                errmsg,fd_incref((lispval)fn));
      u8_unlock_mutex(&(dbproc->sqliteproc_lock));
      return FD_ERROR_VALUE;}
    i++;}
  u8_lock_mutex(&(fds->sqlite_lock));
  values = sqlite_values(dbproc->sqlitedb,dbproc->stmt,dbproc->extdb_colinfo);
  u8_unlock_mutex(&(fds->sqlite_lock));
  sqlite3_reset(dbproc->stmt);
  u8_unlock_mutex(&(dbproc->sqliteproc_lock));
  if (FD_ABORTP(values)) {
    const char *errmsg = sqlite3_errmsg(dbproc->sqlitedb);
    fd_seterr(SQLiteError,"fdsqlite_call",
              errmsg,fd_incref((lispval)fn));}
  return values;
}

/* Processing results */

static time_t sqlite_time_to_xtime(const char *s,struct U8_XTIME *xtp);

static lispval sqlite_values(sqlite3 *db,sqlite3_stmt *stmt,lispval colinfo)
{
  lispval results = FD_EMPTY_CHOICE, *resultsv = NULL; int rn = 0, rmax = 0;
  lispval _colnames[16], *colnames;
  lispval _colmaps[16], *colmaps;
  lispval mergefn = fd_getopt(colinfo,merge_symbol,FD_VOID);
  lispval sortfn = fd_getopt(colinfo,sorted_symbol,FD_VOID);
  int sorted = (FD_TRUEP(sortfn));
  int i = 0, n_cols = sqlite3_column_count(stmt), retval;
  struct U8_OUTPUT out;
  if (!((FD_VOIDP(mergefn)) || (FD_TRUEP(mergefn)) ||
        (FD_FALSEP(mergefn)) || (FD_APPLICABLEP(mergefn))))
    return fd_type_error("%MERGE","sqlite_values",mergefn);
  if (n_cols==0) {
    int retval = sqlite3_step(stmt);
    if ((retval) && (retval<100))
      return FD_ERROR_VALUE;
    else return FD_EMPTY_CHOICE;}
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
    colnames[i]=colname=
      intern_upcase(&out,(u8_string)sqlite3_column_name(stmt,i));
    colmaps[i]=(fd_getopt(colinfo,colname,FD_VOID));
    i++;}
  while ((retval = sqlite3_step(stmt)) == SQLITE_ROW) {
    lispval result;
    struct FD_KEYVAL *kv = u8_alloc_n(n_cols,struct FD_KEYVAL);
    int j = 0; while (j<n_cols) {
      int coltype = sqlite3_column_type(stmt,j); lispval value;
      if (coltype<0) {
        int k = 0; while (k<j) {fd_decref(kv[k].kv_val);  k++;}
        u8_free(kv);
        fd_decref(results); u8_free(out.u8_outbuf);
        return FD_ERROR_VALUE;}
      kv[j].kv_key = colnames[j];
      switch (coltype) {
      case SQLITE_INTEGER: {
        long long intval = sqlite3_column_int(stmt,j);
        const char *decltype = sqlite3_column_decltype(stmt,j);
        if ((decltype)&&
            ((strcasecmp(decltype,"DATETIME")==0)||
             (strcasecmp(decltype,"DATE")==0)))
          value = fd_time2timestamp((time_t)intval);
        else value = FD_INT(intval);
        break;}
      case SQLITE_FLOAT:
        value = fd_init_double(NULL,sqlite3_column_double(stmt,j)); break;
      case SQLITE_TEXT: {
        const char *decltype = sqlite3_column_decltype(stmt,j);
        const char *textval = sqlite3_column_text(stmt,j);
        if ((decltype)&&
            ((strcasecmp(decltype,"DATETIME")==0)||
             (strcasecmp(decltype,"DATE")==0))) {
          struct U8_XTIME xt; time_t retval = sqlite_time_to_xtime(textval,&xt);
          if (retval<0) retval = u8_rfc822_to_xtime((u8_string)textval,&xt);
          if (retval<0) value = lispval_string((u8_string)textval);
          else value = fd_make_timestamp(&xt);}
        else value = lispval_string((unsigned char *)textval);
        break;}
      case SQLITE_BLOB: {
        int n_bytes = sqlite3_column_bytes(stmt,j);
        const unsigned char *blob = sqlite3_column_blob(stmt,j);
        value = fd_make_packet(NULL,n_bytes,(unsigned char *)blob);
        break;}
      case SQLITE_NULL: default:
        value = FD_EMPTY_CHOICE; break;}
      if (FD_VOIDP(colmaps[j]))
        kv[j].kv_val = value;
      else if (FD_APPLICABLEP(colmaps[j])) {
        kv[j].kv_val = fd_apply(colmaps[j],1,&value);
        fd_decref(value);}
      else if (FD_OIDP(colmaps[j]))
        if (FD_STRINGP(value)) {
          kv[j].kv_val = fd_parse(FD_CSTRING(value));
          fd_decref(value);}
        else {
          FD_OID base = FD_OID_ADDR(colmaps[j]);
          int offset = fd_getint(value);
          if (offset<0) kv[j].kv_val = value;
          else {
            kv[j].kv_val = fd_make_oid(base+offset);
            fd_decref(value);}}
      else if (colmaps[j]==FD_TRUE)
        if (FD_STRINGP(value)) {
          kv[j].kv_val = fd_parse(FD_CSTRING(value));
          fd_decref(value);}
        else kv[j].kv_val = value;
      else kv[j].kv_val = value;
      j++;}
    if ((n_cols==1) && (FD_TRUEP(mergefn))) {
      result = kv[0].kv_val;
      u8_free(kv);}
    else if ((FD_VOIDP(mergefn)) ||
             (FD_FALSEP(mergefn)) ||
             (FD_TRUEP(mergefn)))
      result = fd_init_slotmap(NULL,n_cols,kv);
    else {
      lispval tmp_slotmap = fd_init_slotmap(NULL,n_cols,kv);
      result = fd_apply(mergefn,1,&tmp_slotmap);
      fd_decref(tmp_slotmap);}
    if (sorted) {
      if (FD_ABORTP(result)) {}
      else if (rn>=rmax) {
        int new_max = ((rmax>=65536)?(rmax+65536):(rmax*2));
        lispval *newv = u8_realloc(resultsv,LISPVEC_BYTELEN(new_max));
        if (newv == NULL) {
          int delta = (new_max-rmax)/2;
          while ((newv == NULL)&&(delta>=1)) {
            new_max = rmax+delta; delta = delta/2;
            newv = u8_realloc(resultsv,LISPVEC_BYTELEN(new_max));}
          if (!(newv)) {
            fd_decref(result);
            fd_seterr(fd_OutOfMemory,"sqlite_step",NULL,FD_VOID);
            result = FD_ERROR_VALUE;}
          else {resultsv = newv; rmax = new_max;}}}
      resultsv[rn++]=result;}
    else {
      FD_ADD_TO_CHOICE(results,result);}
    if (FD_ABORTP(result)) {
      if (sorted) {
        int k = 0; while (k<rn) {
          lispval v = resultsv[k++]; fd_decref(v);}
        fd_decref(results);
        u8_free(resultsv);
        results = result;
        break;}
      else {
        fd_decref(results);
        results = result;
        break;}}}
  if (retval!=SQLITE_DONE) {
#if HAVE_SQLITE3_V2
    const char *msg = sqlite3_errstr(retval);
    fd_seterr(SQLiteError,"sqlite_step3",msg,FD_VOID);
#endif
    fd_decref(results); if (sorted) {
      int k = 0; while (k<rn) {
        lispval v = resultsv[k++]; fd_decref(v);}
      fd_decref(results);
      u8_free(resultsv);
      resultsv = NULL;}
    results = FD_ERROR_VALUE;}
  u8_free(out.u8_outbuf);
  fd_decref(mergefn);
  fd_decref(sortfn);
  if (colnames!=_colnames) u8_free(colnames);
  if (colmaps!=_colmaps) u8_free(colmaps);
  if (FD_ABORTP(results)) return results;
  else if (sorted)
    return fd_wrap_vector(rn,resultsv);
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

static struct FD_EXTDB_HANDLER sqlite_handler=
  {"sqlite",NULL,NULL,NULL,NULL};

FD_EXPORT int fd_init_sqlite()
{
  lispval module;
  if (sqlite_init) return 0;
  module = fd_new_cmodule("SQLITE",0,fd_init_sqlite);

  sqlite_handler.execute = sqliteexechandler;
  sqlite_handler.makeproc = sqlitemakeprochandler;
  sqlite_handler.recycle_db = recycle_fdsqlite;
  sqlite_handler.recycle_proc = recycle_fdsqliteproc;

  fd_register_extdb_handler(&sqlite_handler);

  fd_defn(module,
          fd_make_cprim3x("SQLITE/OPEN",sqlite_open_prim,1,
                          fd_string_type,FD_VOID,
                          -1,FD_VOID,-1,FD_VOID));
  fd_defn(module,fd_make_cprim1x("SQLITE/REOPEN",sqlite_reopen_prim,1,
                                 fd_extdb_type,FD_VOID));
  fd_defn(module,fd_make_cprim1x("SQLITE/CLOSE",sqlite_close_prim,1,
                                 fd_extdb_type,FD_VOID));
  sqlite_init = u8_millitime();

  merge_symbol = fd_intern("%MERGE");
  sorted_symbol = fd_intern("%SORTED");
  readonly_symbol = fd_intern("READONLY");
  create_symbol = fd_intern("CREATE");
  sharedcache_symbol = fd_intern("SHAREDCACHE");
  privatecache_symbol = fd_intern("PRIVATECACHE");
  vfs_symbol = fd_intern("VFS");
  execok_symbol = fd_intern("EXEC/OK");

  fd_finish_module(module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
