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
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/cprims.h"

#include "kno/sql.h"

#include <libu8/u8printf.h>

static lispval exec_enabled_symbol;

static u8_condition NoMakeProc=
  _("No implementation for prepared SQL statements");

/* SQLDB primitives */

static int exec_enabled = 0;

static int check_exec_enabled(struct KNO_SQLCONN *conn)
{
  if ( (conn->sqlconn_bits) & (SQLCONN_EXEC_OK) )
    return 1;
  lispval opts = conn->sqlconn_options;
  lispval v = kno_getopt(opts,exec_enabled_symbol,VOID);
  if (VOIDP(v)) return exec_enabled;
  else if (FALSEP(v)) return 0;
  kno_decref(v);
  return 1;
}

/*
  FDPRIM(sqldb_exec,"SQLDB/EXEC",3,KNO_NEEDS_3ARGS,
  "`(sqldb/exec *dbptr* *sql_string* [*colinfo*])` "
  "executes *sql_string* on database *dbptr*, using "
  "*colinfo* to convert arguments and results.",
  kno_sqlconn_type,KNO_VOID,kno_string_type,KNO_VOID,-1,KNO_VOID,
  (lispval db,lispval query,lispval colinfo))
*/

DEFC_PRIM("sqldb/exec",sqldb_exec,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"db",kno_sqlconn_type,KNO_VOID},
	  {"query",kno_string_type,KNO_VOID},
	  {"colinfo",kno_any_type,KNO_VOID})
static lispval sqldb_exec(lispval db,lispval query,lispval colinfo)
{
  struct KNO_SQLCONN *sqldb = KNO_GET_CONS(db,kno_sqlconn_type,struct KNO_SQLCONN *);
  if ((exec_enabled)||(check_exec_enabled(sqldb)))
    return sqldb->sqldb_handler->execute(sqldb,query,colinfo);
  else return kno_err(_("Direct SQL execution disabled"),"sqldb_exec",
                      CSTRING(query),db);
}

DEFC_PRIMN("sqldb/proc",sqldb_makeproc,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "Creates a procedure object to execute a SQL query on a particular "
	   "connection. This has two basic forms:\n"
	   "* `(sqldb/proc *db* *qstring* *parameter specs...*)\n"
	   "* `(sqldb/proc *db* *options* *qstring* *parameter specs...*)\n"
	   "")
static lispval sqldb_makeproc(int n,kno_argvec args)
{
  if (USUALLY(KNO_PRIM_TYPEP(args[0],kno_sqlconn_type))) {
    int n_params = 0;
    lispval dbspec = args[0], arg1 = args[1], query, options, colinfo;
    struct KNO_SQLCONN *sqldb=
      KNO_GET_CONS(dbspec,kno_sqlconn_type,struct KNO_SQLCONN *);
    if (sqldb == NULL) return KNO_ERROR;
    else if ((sqldb->sqldb_handler->makeproc) == NULL)
      return kno_err(NoMakeProc,"sqldb_makeproc",NULL,dbspec);
    else NO_ELSE;
    if (KNO_STRINGP(arg1)) {
      query=arg1;
      options=KNO_FALSE;
      n_params=n-2;}
    else if ( (KNO_TABLEP(arg1)) && (n>2) && (KNO_STRINGP(args[2])) ) {
      options=arg1;
      query=args[2];
      n_params=n-3;}
    else return kno_type_error("string","sqldb_makeproc",arg1);
    if (kno_testopt(options,KNOSYM_COLINFO,KNO_VOID))
      colinfo = kno_getopt(options,KNOSYM_COLINFO,KNO_FALSE);
    else {
      colinfo = options;
      options = KNO_FALSE;}
    return sqldb->sqldb_handler->makeproc
      (sqldb,CSTRING(query),options,colinfo,
       ((n_params>0) ? n_params : (0)),
       ((n_params>0)? (args+(n-n_params)) : (NULL)));}
  else if (!(KNO_PRIM_TYPEP(args[0],kno_sqlconn_type)))
    return kno_type_error("sqldb","sqldb_makeproc",args[0]);
  /* Should never be reached  */
  else return VOID;
}

#if 0
DEFC_PRIMN("sqldb/proc+",sqlproc_plus,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval sqlproc_plus(int n,kno_argvec args)
{
  lispval arg1 = args[0], result = VOID;
  struct KNO_SQLPROC *sqlproc=
    KNO_GET_CONS(arg1,kno_sqlproc_type,struct KNO_SQLPROC *);
  lispval sqlconn = sqlproc->sqlproc_conn;
  struct KNO_SQLCONN *sqldb=
    KNO_GET_CONS(sqlconn,kno_sqlconn_type,struct KNO_SQLCONN *);
  u8_string base_qtext = sqlproc->sqlproc_qtext, new_qtext=
    u8_string_append(base_qtext," ",CSTRING(args[1]),NULL);
  lispval opts_arg, options, colinfo;
  lispval optionscolinfo = sqlproc->sqlproc_colinfo;
  lispval colinfo = sqlproc->sqlproc_colinfo;
  int n_base_params = sqlproc->sqlproc_n_params, n_params = (n-2)+n_base_params;
  lispval *params = ((n_params)?(u8_alloc_n(n_params,lispval)):(NULL));
  lispval *base_params = sqlproc->sqlproc_paramtypes, param_count = 0;
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
#endif

/* Accessors */

DEFC_PRIM("sqldb/proc/query",sqlproc_query,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "=> sqlstring",
	  {"sqldb",kno_sqlproc_type,KNO_VOID})
static lispval sqlproc_query(lispval sqldb)
{
  struct KNO_SQLPROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqlproc_type,struct KNO_SQLPROC *);
  return kno_mkstring(xdbp->sqlproc_qtext);
}

DEFC_PRIM("sqldb/proc/spec",sqlproc_spec,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "=> dbspecstring",
	  {"sqldb",kno_sqlproc_type,KNO_VOID})
static lispval sqlproc_spec(lispval sqldb)
{
  struct KNO_SQLPROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqlproc_type,struct KNO_SQLPROC *);
  return kno_mkstring(xdbp->sqlproc_spec);
}

DEFC_PRIM("sqldb/proc/db",sqlproc_db,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "=> dbptr",
	  {"sqldb",kno_sqlproc_type,KNO_VOID})
static lispval sqlproc_db(lispval sqldb)
{
  struct KNO_SQLPROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqlproc_type,struct KNO_SQLPROC *);
  return kno_incref(xdbp->sqlproc_conn);
}

DEFC_PRIM("sqldb/proc/typemap",sqlproc_typemap,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "=> colinfo",
	  {"sqldb",kno_sqlproc_type,KNO_VOID})
static lispval sqlproc_typemap(lispval sqldb)
{
  struct KNO_SQLPROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqlproc_type,struct KNO_SQLPROC *);
  return kno_incref(xdbp->sqlproc_colinfo);
}

DEFC_PRIM("sqldb/proc/params",sqlproc_params,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "=> paraminfo",
	  {"sqldb",kno_sqlproc_type,KNO_VOID})
static lispval sqlproc_params(lispval sqldb)
{
  struct KNO_SQLPROC *xdbp = KNO_GET_CONS
    (sqldb,kno_sqlproc_type,struct KNO_SQLPROC *);
  int n = xdbp->sqlproc_n_params;
  lispval *paramtypes = xdbp->sqlproc_paramtypes;
  lispval result = kno_make_vector(n,NULL);
  int i = 0; while (i<n) {
    lispval param_info = paramtypes[i]; kno_incref(param_info);
    KNO_VECTOR_SET(result,i,param_info);
    i++;}
  return result;
}

/* Initializations */

int sqlprims_initialized = 0;

static lispval sqldb_module;

KNO_EXPORT void kno_init_sqlprims_c()
{
  if (sqlprims_initialized) return;
  sqlprims_initialized = 1;
  kno_init_scheme();
  sqldb_module = kno_new_cmodule("sqldb",(0),kno_init_sqlprims_c);

  u8_register_source_file(_FILEINFO);

  exec_enabled_symbol = kno_intern("sqlexec");

  link_local_cprims();
  kno_register_config("SQLEXEC",
                      _("whether direct execution of SQL strings is allowed"),
                      kno_boolconfig_get,kno_boolconfig_set,&exec_enabled);

  kno_finish_cmodule(sqldb_module);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("sqldb/proc/params",sqlproc_params,1,sqldb_module);
  KNO_LINK_CPRIM("sqldb/proc/typemap",sqlproc_typemap,1,sqldb_module);
  KNO_LINK_CPRIM("sqldb/proc/db",sqlproc_db,1,sqldb_module);
  KNO_LINK_CPRIM("sqldb/proc/spec",sqlproc_spec,1,sqldb_module);
  KNO_LINK_CPRIM("sqldb/proc/query",sqlproc_query,1,sqldb_module);
  /* KNO_LINK_CPRIMN("sqldb/proc+",sqlproc_plus,sqldb_module); */
  KNO_LINK_CPRIMN("sqldb/proc",sqldb_makeproc,sqldb_module);
  KNO_LINK_CPRIM("sqldb/exec",sqldb_exec,3,sqldb_module);
}
