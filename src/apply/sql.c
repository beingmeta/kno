/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

/* The external DB module provides simple access to external SQL
   databases.  There are two Scheme types used by this module:
   SQLDB objects (kno_sqlconn_type) are basically database connections
   implemented by CONSes whose header is identical to "struct KNO_SQLCONN";
   SQLDB procedures (kno_sqlproc_type) are applicable objects which
   correspond to prepared statements for a particular connection.  These
   procedures have (optional) column info consisting of a slotmap which
   maps column names into either OIDs or functions.  The functions are
   used to convert values of said column and the OIDs are used as base values
   to convert integer values to OIDs.  SQLDB procedures also have
   "parameter maps" which are used to process application parameters
   into parameters to be bound to the corresponding statement.

   Implementing a given database bridge consists of defining functions for:
   (a) executing a SQL string on the connection;
   (b) creating an SQLDB proc from the connection;
   (c) recycling this kind of SQLDB connection
   (d) recycling this kind of SQLDB procedure

   The actual access is implemented by loadable modules which generally
   register a handler (though they don't have to) and provides a function
   for opening an SQLDB connection which can then be used for executing
   queries and generating SQLDB procedures.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/apply.h"
#include "kno/sql.h"

#include <libu8/u8printf.h>

lispval KNOSYM_COLINFO = KNO_VOID;

static u8_mutex sqldb_handlers_lock;
static struct KNO_SQLDB_HANDLER *sqldb_handlers[128];
int n_sqldbs = 0;

KNO_EXPORT int kno_register_sqldb_handler(struct KNO_SQLDB_HANDLER *h)
{
  int i = 0, retval = -1;
  u8_lock_mutex(&sqldb_handlers_lock);
  while (i<n_sqldbs)
    if (strcasecmp(h->name,sqldb_handlers[i]->name)==0)
      if (h == sqldb_handlers[i]) {retval = 0; break;}
      else {sqldb_handlers[i]=h; retval = 1; break;}
    else i++;
  if (retval<0) {
    if (n_sqldbs<128) {
      sqldb_handlers[n_sqldbs++]=h;
      retval = 2;}
    else {
      u8_seterr(_("Too many sqldb handlers"),"kno_register_sqldb_handler",NULL);
      retval = -1;}}
  u8_unlock_mutex(&sqldb_handlers_lock);
  return retval;
}

KNO_EXPORT int kno_register_sqlproc(struct KNO_SQLPROC *proc)
{
  struct KNO_SQLCONN *db=
    KNO_GET_CONS(proc->sqlproc_conn,kno_sqlconn_type,struct KNO_SQLCONN *);
  u8_lock_mutex(&(db->sqlconn_procs_lock)); {
    int i = 0, n = db->sqlconn_n_procs;
    struct KNO_SQLPROC **dbprocs = db->sqlconn_procs;
    while (i<n)
      if ((dbprocs[i]) == proc) {
        u8_unlock_mutex(&(db->sqlconn_procs_lock));
        return 0;}
      else i++;
    if (i>=db->sqlconn_procs_len) {
      struct KNO_SQLPROC **newprocs=
        u8_realloc(dbprocs,sizeof(struct KNO_SQLCONN *)*(db->sqlconn_procs_len+32));
      if (newprocs == NULL) {
        u8_unlock_mutex(&(db->sqlconn_procs_lock));
        u8_graberrno("kno_sqldb_register_proc",u8_strdup(db->sqlconn_spec));
        return -1;}
      else {
        db->sqlconn_procs = dbprocs = newprocs;
        db->sqlconn_procs_len = db->sqlconn_procs_len+32;}}
    dbprocs[i]=proc;
    db->sqlconn_n_procs++;}
  u8_unlock_mutex(&(db->sqlconn_procs_lock));
  /* Ensure that these have some kind of value */
  if (proc->sqlproc_options==KNO_NULL) proc->sqlproc_options=KNO_FALSE;
  if (proc->sqlproc_colinfo==KNO_NULL) proc->sqlproc_colinfo=KNO_FALSE;
  return 1;
}

KNO_EXPORT int kno_release_sqlproc(struct KNO_SQLPROC *proc)
{
  struct KNO_SQLCONN *db=
    KNO_GET_CONS(proc->sqlproc_conn,kno_sqlconn_type,struct KNO_SQLCONN *);
  if (!(db)) {
    u8_seterr(_("SQLDB proc without a database"),"kno_release_sqlproc",
              u8_strdup(proc->sqlproc_qtext));
    return -1;}
  u8_lock_mutex(&(db->sqlconn_procs_lock)); {
    int n = db->sqlconn_n_procs, i = n-1;
    struct KNO_SQLPROC **dbprocs = db->sqlconn_procs;
    while (i>=0)
      if ((dbprocs[i]) == proc) {
        memmove(dbprocs+i,dbprocs+i+1,(n-(i+1))*sizeof(struct KNO_SQLPROC *));
        db->sqlconn_n_procs--;
        u8_unlock_mutex(&(db->sqlconn_procs_lock));
        return 1;}
      else i--;}
  u8_unlock_mutex(&(db->sqlconn_procs_lock));
  u8_log(LOG_CRIT,"sqldb_release_proc","Release of unregistered sqldb proc");
  return 0;
}

KNO_EXPORT int kno_recycle_sqlproc(struct KNO_SQLPROC *dbproc)
{
  kno_decref(dbproc->sqlproc_colinfo);
  u8_free(dbproc->sqlproc_spec);
  u8_free(dbproc->sqlproc_qtext);
  if (dbproc->sqlproc_paramtypes) {
      int j = 0, lim = dbproc->sqlproc_n_params;; while (j<lim) {
	kno_decref(dbproc->sqlproc_paramtypes[j]); j++;}
      u8_free(dbproc->sqlproc_paramtypes);}
  lispval conn = dbproc->sqlproc_conn;
  dbproc->sqlproc_conn = KNO_VOID;
  kno_decref(conn);
  return 1;
}

/* SQLDB handlers */

static int unparse_sqldb(u8_output out,lispval x)
{
  struct KNO_SQLCONN *dbp = (struct KNO_SQLCONN *)x;
  u8_printf(out,"#<SQLDB/%s %s>",dbp->sqldb_handler->name,dbp->sqlconn_info);
  return 1;
}

KNO_EXPORT void kno_cleanup_sqlprocs(struct KNO_SQLCONN *dbp)
{
  if (dbp->sqlconn_n_procs) {
    int i = 0, n = dbp->sqlconn_n_procs;
    struct KNO_SQLPROC **procs = dbp->sqlconn_procs;
    while (i < n) {
      struct KNO_SQLPROC *proc = procs[i];
      procs[i] = NULL;
      if (proc) {
        if (KNO_CONS_TYPEOF(proc) == kno_sqlproc_type)
          u8_log(LOG_CRIT,"SQLCleanup","Dangling SQL proc for %q: %q",
                 (lispval)dbp,(lispval)proc);
        else u8_log(LOG_CRIT,"SQLCleanup",
                    "Dangling non SQL proc for %q: %q",
                    (lispval)dbp,(lispval)proc);}
      i++;}}
  if (dbp->sqlconn_procs) {
    u8_free(dbp->sqlconn_procs);
    dbp->sqlconn_procs = NULL;}
  dbp->sqlconn_n_procs = dbp->sqlconn_procs_len = 0;
}

KNO_EXPORT void kno_cleanup_sqlconn(struct KNO_SQLCONN *dbp)
{
  kno_decref(dbp->sqlconn_colinfo);
  kno_decref(dbp->sqlconn_options);
  if ( dbp->sqlconn_spec != dbp->sqlconn_info)
    u8_free(dbp->sqlconn_info);
  u8_free(dbp->sqlconn_spec);
  u8_destroy_mutex(&(dbp->sqlconn_procs_lock));
}

static void recycle_sqldb(struct KNO_RAW_CONS *c)
{
  struct KNO_SQLCONN *dbp = (struct KNO_SQLCONN *)c;
  kno_cleanup_sqlprocs(dbp);
  dbp->sqldb_handler->recycle_db(dbp);
  kno_cleanup_sqlconn(dbp);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

static int unparse_sqlproc(u8_output out,lispval x)
{
  struct KNO_SQLPROC *dbp = (struct KNO_SQLPROC *)x;
  u8_printf(out,"#<DBλ/%s %s: %s>",
            dbp->sqldb_handler->name,dbp->sqlproc_spec,dbp->sqlproc_qtext);
  return 1;
}

static void recycle_sqlproc(struct KNO_RAW_CONS *c)
{
  struct KNO_SQLPROC *dbproc = (struct KNO_SQLPROC *)c;
  if (dbproc->sqldb_handler == NULL)
    u8_log(LOG_WARN,_("recycle failed"),"Bad sqldb proc");
  else if (dbproc->sqldb_handler->recycle_proc) {
    dbproc->sqldb_handler->recycle_proc(dbproc);
    if (dbproc->fcn_attribs) kno_decref(dbproc->fcn_attribs);
    if (dbproc->fcn_moduleid) kno_decref(dbproc->fcn_moduleid);}
  else u8_log(LOG_WARN,_("recycle failed"),
              _("No recycle method for %s database procs"),
              dbproc->sqldb_handler->name);
  kno_decref(dbproc->sqlproc_options);
  kno_decref(dbproc->sqlproc_colinfo);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

static lispval callsqlproc(struct KNO_FUNCTION *xdbproc,int n,lispval *args)
{
  struct KNO_SQLPROC *dbp = (struct KNO_SQLPROC *)xdbproc;
  return dbp->fcn_handler.xcalln((kno_stack)kno_stackptr,(lispval)xdbproc,n,args);
}

/* Initializations */

int sqldb_initialized = 0;

void kno_init_sql_c()
{
  if (sqldb_initialized) return;
  sqldb_initialized = 1;

  KNOSYM_COLINFO = kno_intern("colinfo");

  u8_init_mutex(&sqldb_handlers_lock);

  kno_recyclers[kno_sqlconn_type]=recycle_sqldb;
  kno_unparsers[kno_sqlconn_type]=unparse_sqldb;

  kno_recyclers[kno_sqlproc_type]=recycle_sqlproc;
  kno_unparsers[kno_sqlproc_type]=unparse_sqlproc;
  kno_applyfns[kno_sqlproc_type]=(kno_applyfn)callsqlproc;
  kno_isfunctionp[kno_sqlproc_type]=1;

  u8_register_source_file(_FILEINFO);
}
