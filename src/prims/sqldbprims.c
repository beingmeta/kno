/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* The external DB module provides simple access to external SQL
   databases.  There are two Scheme types used by this module:
    SQLDB objects (kno_sqldb_type) are basically database connections
     implemented by CONSes whose header is identical to "struct KNO_SQLDB";
    SQLDB procedures (kno_sqldb_proc_type) are applicable objects which
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

#define KNO_PROVIDE_FASTEVAL 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"

#include "kno/sqldb.h"

#include <libu8/u8printf.h>

kno_ptr_type kno_sqldb_type, kno_sqldb_proc_type;

static lispval exec_enabled_symbol;

static u8_condition NoMakeProc=
  _("No implementation for prepared SQL statements");

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

KNO_EXPORT int kno_register_sqldb_proc(struct KNO_SQLDB_PROC *proc)
{
  struct KNO_SQLDB *db=
    KNO_GET_CONS(proc->sqldbptr,kno_sqldb_type,struct KNO_SQLDB *);
  u8_lock_mutex(&(db->sqldb_proclock)); {
    int i = 0, n = db->sqldb_n_procs;
    struct KNO_SQLDB_PROC **dbprocs = db->sqldb_procs;
    while (i<n)
      if ((dbprocs[i]) == proc) {
        u8_unlock_mutex(&(db->sqldb_proclock));
        return 0;}
      else i++;
    if (i>=db->sqldb_procslen) {
      struct KNO_SQLDB_PROC **newprocs=
        u8_realloc(dbprocs,sizeof(struct KNO_SQLDB *)*(db->sqldb_procslen+32));
      if (newprocs == NULL) {
        u8_unlock_mutex(&(db->sqldb_proclock));
        u8_graberrno("kno_sqldb_register_proc",u8_strdup(db->sqldb_spec));
        return -1;}
      else {
        db->sqldb_procs = dbprocs = newprocs;
        db->sqldb_procslen = db->sqldb_procslen+32;}}
    dbprocs[i]=proc; db->sqldb_n_procs++;}
  u8_unlock_mutex(&(db->sqldb_proclock));
  return 1;
}

KNO_EXPORT int kno_release_sqldb_proc(struct KNO_SQLDB_PROC *proc)
{
  struct KNO_SQLDB *db=
    KNO_GET_CONS(proc->sqldbptr,kno_sqldb_type,struct KNO_SQLDB *);
  if (!(db)) {
    u8_seterr(_("SQLDB proc without a database"),"kno_release_sqldb_proc",
              u8_strdup(proc->sqldb_qtext));
    return -1;}
  u8_lock_mutex(&(db->sqldb_proclock)); {
    int n = db->sqldb_n_procs, i = n-1;
    struct KNO_SQLDB_PROC **dbprocs = db->sqldb_procs;
    while (i>=0)
      if ((dbprocs[i]) == proc) {
        memmove(dbprocs+i,dbprocs+i+1,(n-(i+1))*sizeof(struct KNO_SQLDB_PROC *));
        db->sqldb_n_procs--;
        u8_unlock_mutex(&(db->sqldb_proclock));
        return 1;}
      else i--;}
  u8_unlock_mutex(&(db->sqldb_proclock));
  u8_log(LOG_CRIT,"sqldb_release_proc","Release of unregistered sqldb proc");
  return 0;
}

/* SQLDB handlers */

static int unparse_sqldb(u8_output out,lispval x)
{
  struct KNO_SQLDB *dbp = (struct KNO_SQLDB *)x;
  u8_printf(out,"#<SQLDB/%s %s>",dbp->sqldb_handler->name,dbp->sqldb_info);
  return 1;
}

static void recycle_sqldb(struct KNO_RAW_CONS *c)
{
  struct KNO_SQLDB *dbp = (struct KNO_SQLDB *)c;
  dbp->sqldb_handler->recycle_db(dbp);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

static int unparse_sqldb_proc(u8_output out,lispval x)
{
  struct KNO_SQLDB_PROC *dbp = (struct KNO_SQLDB_PROC *)x;
  u8_printf(out,"#<DBλ/%s %s: %s>",
            dbp->sqldb_handler->name,dbp->sqldb_spec,dbp->sqldb_qtext);
  return 1;
}

static void recycle_sqldb_proc(struct KNO_RAW_CONS *c)
{
  struct KNO_SQLDB_PROC *dbproc = (struct KNO_SQLDB_PROC *)c;
  if (dbproc->sqldb_handler == NULL)
    u8_log(LOG_WARN,_("recycle failed"),"Bad sqldb proc");
  else if (dbproc->sqldb_handler->recycle_proc) {
    dbproc->sqldb_handler->recycle_proc(dbproc);
    if (dbproc->fcn_typeinfo) u8_free(dbproc->fcn_typeinfo);
    if (dbproc->fcn_defaults) u8_free(dbproc->fcn_defaults);
    if (dbproc->fcn_attribs) kno_decref(dbproc->fcn_attribs);
    if (dbproc->fcn_moduleid) kno_decref(dbproc->fcn_moduleid);}
  else u8_log(LOG_WARN,_("recycle failed"),
              _("No recycle method for %s database procs"),
              dbproc->sqldb_handler->name);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}

static lispval callsqldbproc(struct KNO_FUNCTION *xdbproc,int n,lispval *args)
{
  struct KNO_SQLDB_PROC *dbp = (struct KNO_SQLDB_PROC *)xdbproc;
  return dbp->fcn_handler.xcalln(xdbproc,n,args);
}

/* SQLDB primitives */

static int exec_enabled = 0;

static int check_exec_enabled(lispval opts)
{
  lispval v = kno_getopt(opts,exec_enabled_symbol,VOID);
  if (VOIDP(v)) return 0;
  else if (FALSEP(v)) return 0;
  kno_decref(v);
  return 1;
}

/*
FDPRIM(sqldb_exec,"SQLDB/EXEC",3,KNO_NEEDS_3ARGS,
       "`(sqldb/exec *dbptr* *sql_string* [*colinfo*])` "
       "executes *sql_string* on database *dbptr*, using "
       "*colinfo* to convert arguments and results.",
       kno_sqldb_type,KNO_VOID,kno_string_type,KNO_VOID,-1,KNO_VOID,
       (lispval db,lispval query,lispval colinfo))
*/
static lispval sqldb_exec(lispval db,lispval query,lispval colinfo)
{
  struct KNO_SQLDB *sqldb = KNO_GET_CONS(db,kno_sqldb_type,struct KNO_SQLDB *);
  if ((exec_enabled)||
      ((kno_testopt(sqldb->sqldb_options,exec_enabled_symbol,VOID))&&
       (check_exec_enabled(sqldb->sqldb_options))))
    return sqldb->sqldb_handler->execute(sqldb,query,colinfo);
  else return kno_err(_("Direct SQL execution disabled"),"sqldb_exec",
                     CSTRING(query),db);
}

static lispval sqldb_makeproc(int n,lispval *args)
{
  if (PRED_TRUE
      ((KNO_PRIM_TYPEP(args[0],kno_sqldb_type)) && (STRINGP(args[1])))) {
    struct KNO_SQLDB *sqldb=
      KNO_GET_CONS(args[0],kno_sqldb_type,struct KNO_SQLDB *);
    lispval dbspec = args[0], query = args[1];
    lispval colinfo = ((n>2) ? (args[2]) : (VOID));
    if (sqldb == NULL) return KNO_ERROR;
    else if (!(STRINGP(query)))
      return kno_type_error("string","sqldb_makeproc",query);
    else if ((sqldb->sqldb_handler->makeproc) == NULL)
      return kno_err(NoMakeProc,"sqldb_makeproc",NULL,dbspec);
    else return sqldb->sqldb_handler->makeproc
           (sqldb,CSTRING(query),STRLEN(query),colinfo,
            ((n>3) ? (n-3) : (0)),
            ((n>3)? (args+3) : (NULL)));}
  else if (!(KNO_PRIM_TYPEP(args[0],kno_sqldb_type)))
    return kno_type_error("sqldb","sqldb_makeproc",args[0]);
  else if  (!(STRINGP(args[1])))
    return kno_type_error("string","sqldb_makeproc",args[1]);
  /* Should never be reached  */
  else return VOID;
}

static lispval sqldb_proc_plus(int n,lispval *args)
{
  lispval arg1 = args[0], result = VOID;
  struct KNO_SQLDB_PROC *sqldbproc=
    KNO_GET_CONS(arg1,kno_sqldb_proc_type,struct KNO_SQLDB_PROC *);
  lispval sqldbptr = sqldbproc->sqldbptr;
  struct KNO_SQLDB *sqldb=
    KNO_GET_CONS(sqldbptr,kno_sqldb_type,struct KNO_SQLDB *);
  u8_string base_qtext = sqldbproc->sqldb_qtext, new_qtext=
    u8_string_append(base_qtext," ",CSTRING(args[1]),NULL);
  lispval colinfo = sqldbproc->sqldb_colinfo;
  int n_base_params = sqldbproc->fcn_n_params, n_params = (n-2)+n_base_params;
  lispval *params = ((n_params)?(u8_alloc_n(n_params,lispval)):(NULL));
  lispval *base_params = sqldbproc->sqldb_paramtypes, param_count = 0;
  int i = n-1; while (i>=2) {
    lispval param = args[i--]; kno_incref(param);
    params[param_count++]=param;}
  i = n_base_params-1; while (i>=0) {
    lispval param = base_params[i--]; kno_incref(param);
    params[param_count++]=param;}
  kno_incref(colinfo);
  result = sqldb->sqldb_handler->makeproc
    (sqldb,new_qtext,strlen(new_qtext),colinfo,param_count,params);
  u8_free(new_qtext);
  return result;
}

/* Accessors */

static lispval sqldb_proc_query(lispval sqldb)
{
  struct KNO_SQLDB_PROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqldb_proc_type,struct KNO_SQLDB_PROC *);
  return lispval_string(xdbp->sqldb_qtext);
}

static lispval sqldb_proc_spec(lispval sqldb)
{
  struct KNO_SQLDB_PROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqldb_proc_type,struct KNO_SQLDB_PROC *);
  return lispval_string(xdbp->sqldb_spec);
}

static lispval sqldb_proc_db(lispval sqldb)
{
  struct KNO_SQLDB_PROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqldb_proc_type,struct KNO_SQLDB_PROC *);
  return kno_incref(xdbp->sqldbptr);
}

static lispval sqldb_proc_typemap(lispval sqldb)
{
  struct KNO_SQLDB_PROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqldb_proc_type,struct KNO_SQLDB_PROC *);
  return kno_incref(xdbp->sqldb_colinfo);
}

static lispval sqldb_proc_params(lispval sqldb)
{
  struct KNO_SQLDB_PROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqldb_proc_type,struct KNO_SQLDB_PROC *);
  int n = xdbp->fcn_n_params;
  lispval *paramtypes = xdbp->sqldb_paramtypes;
  lispval result = kno_make_vector(n,NULL);
  int i = 0; while (i<n) {
    lispval param_info = paramtypes[i]; kno_incref(param_info);
    KNO_VECTOR_SET(result,i,param_info);
    i++;}
  return result;
}

/* Initializations */

int sqldb_initialized = 0;

KNO_EXPORT void kno_init_sqldbprims_c()
{
  lispval sqldb_module;
  if (sqldb_initialized) return;
  sqldb_initialized = 1;
  kno_init_scheme();
  sqldb_module = kno_new_cmodule("sqldb",(0),kno_init_sqldbprims_c);
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&sqldb_handlers_lock);

  exec_enabled_symbol = kno_intern("%execok");

  kno_sqldb_type = kno_register_cons_type("SQLDB");
  kno_recyclers[kno_sqldb_type]=recycle_sqldb;
  kno_unparsers[kno_sqldb_type]=unparse_sqldb;

  kno_sqldb_proc_type = kno_register_cons_type("SQLDBPROC");
  kno_recyclers[kno_sqldb_proc_type]=recycle_sqldb_proc;
  kno_unparsers[kno_sqldb_proc_type]=unparse_sqldb_proc;
  kno_applyfns[kno_sqldb_proc_type]=(kno_applyfn)callsqldbproc;
  kno_functionp[kno_sqldb_proc_type]=1;

  kno_idefn3(sqldb_module,"SQLDB/EXEC",sqldb_exec,2,
            "`(SQLDB/EXEC *dbptr* *sql* *colinfo*)`",
            kno_sqldb_type,VOID,
            kno_string_type,VOID,
            -1,VOID);
  kno_idefnN(sqldb_module,"SQLDB/PROC",sqldb_makeproc,2,
            "`(SQLDB/PROC *dbptr* *sql* [*colinfo*] [*paraminfo*...] )`");
  kno_idefnN(sqldb_module,"SQLDB/PROC+",sqldb_proc_plus,2,
            "`(SQLDB/PROC *dbptr* *sql* [*colinfo*] [*paraminfo*...] )`");

  kno_idefn1(sqldb_module,"SQLDB/PROC/QUERY",sqldb_proc_query,1,
            "`(SQLDB/PROC/QUERY *dbproc*)` => sqlstring",
            kno_sqldb_proc_type,VOID);
  kno_idefn1(sqldb_module,"SQLDB/PROC/DB",sqldb_proc_db,1,
            "`(SQLDB/PROC/DB *dbproc*)` => dbptr",
            kno_sqldb_proc_type,VOID);
  kno_idefn1(sqldb_module,"SQLDB/PROC/SPEC",sqldb_proc_spec,1,
            "`(SQLDB/PROC/SPEC *dbproc*)` => dbspecstring",
            kno_sqldb_proc_type,VOID);
  kno_idefn1(sqldb_module,"SQLDB/PROC/PARAMS",sqldb_proc_params,1,
            "`(SQLDB/PROC/PARAMS *dbproc*)` => paraminfo",
            kno_sqldb_proc_type,VOID);
  kno_idefn1(sqldb_module,"SQLDB/PROC/TYPEMAP",sqldb_proc_typemap,1,
            "`(SQLDB/PROC/TYPEMAP *dbproc*)` => colinfo",
            kno_sqldb_proc_type,VOID);

  kno_register_config("SQLEXEC",
                     _("whether direct execution of SQL strings is allowed"),
                     kno_boolconfig_get,kno_boolconfig_set,NULL);

  kno_finish_module(sqldb_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/