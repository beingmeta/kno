/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

/* The external DB module provides simple access to external SQL
   databases.  There are two Scheme types used by this module:
    EXTDB objects (fd_extdb_type) are basically database connections
     implemented by CONSes whose header is identical to "struct FD_EXTDB";
    EXTDB procedures (fd_extdb_proc_type) are applicable objects which
     correspond to prepared statements for a particular connection.  These
     procedures have (optional) column info consisting of a slotmap which
     maps column names into either OIDs or functions.  The functions are
     used to convert values of said column and the OIDs are used as base values
     to convert integer values to OIDs.  EXTDB procedures also have
     "parameter maps" which are used to process application parameters
     into parameters to be bound to the corresponding statement.

    Implementing a given database bridge consists of defining functions for:
     (a) executing a SQL string on the connection;
     (b) creating an EXTDB proc from the connection;
     (c) recycling this kind of EXTDB connection
     (d) recycling this kind of EXTDB procedure

   The actual access is implemented by loadable modules which generally
    register a handler (though they don't have to) and provides a function
    for opening an EXTDB connection which can then be used for executing
    queries and generating EXTDB procedures.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"

#include "framerd/extdb.h"

#include <libu8/u8printf.h>

fd_ptr_type fd_extdb_type, fd_extdb_proc_type;

static lispval exec_enabled_symbol;

static u8_condition NoMakeProc=
  _("No implementation for prepared SQL statements");

static u8_mutex extdb_handlers_lock;
static struct FD_EXTDB_HANDLER *extdb_handlers[128];
int n_extdbs = 0;

FD_EXPORT int fd_register_extdb_handler(struct FD_EXTDB_HANDLER *h)
{
  int i = 0, retval = -1;
  u8_lock_mutex(&extdb_handlers_lock);
  while (i<n_extdbs)
    if (strcasecmp(h->name,extdb_handlers[i]->name)==0)
      if (h == extdb_handlers[i]) {retval = 0; break;}
      else {extdb_handlers[i]=h; retval = 1; break;}
    else i++;
  if (retval<0) {
    if (n_extdbs<128) {
      extdb_handlers[n_extdbs++]=h;
      retval = 2;}
    else {
      u8_seterr(_("Too many extdb handlers"),"fd_register_extdb_handler",NULL);
      retval = -1;}}
  u8_unlock_mutex(&extdb_handlers_lock);
  return retval;
}

FD_EXPORT int fd_register_extdb_proc(struct FD_EXTDB_PROC *proc)
{
  struct FD_EXTDB *db=
    FD_GET_CONS(proc->extdbptr,fd_extdb_type,struct FD_EXTDB *);
  u8_lock_mutex(&(db->extdb_proclock)); {
    int i = 0, n = db->extdb_n_procs;
    struct FD_EXTDB_PROC **dbprocs = db->extdb_procs;
    while (i<n)
      if ((dbprocs[i]) == proc) {
        u8_unlock_mutex(&(db->extdb_proclock));
        return 0;}
      else i++;
    if (i>=db->extdb_procslen) {
      struct FD_EXTDB_PROC **newprocs=
        u8_realloc(dbprocs,sizeof(struct FD_EXTDB *)*(db->extdb_procslen+32));
      if (newprocs == NULL) {
        u8_unlock_mutex(&(db->extdb_proclock));
        u8_graberrno("fd_extdb_register_proc",u8_strdup(db->extdb_spec));
        return -1;}
      else {
        db->extdb_procs = dbprocs = newprocs;
        db->extdb_procslen = db->extdb_procslen+32;}}
    dbprocs[i]=proc; db->extdb_n_procs++;}
  u8_unlock_mutex(&(db->extdb_proclock));
  return 1;
}

FD_EXPORT int fd_release_extdb_proc(struct FD_EXTDB_PROC *proc)
{
  struct FD_EXTDB *db=
    FD_GET_CONS(proc->extdbptr,fd_extdb_type,struct FD_EXTDB *);
  if (!(db)) {
    u8_seterr(_("EXTDB proc without a database"),"fd_release_extdb_proc",
              u8_strdup(proc->extdb_qtext));
    return -1;}
  u8_lock_mutex(&(db->extdb_proclock)); {
    int n = db->extdb_n_procs, i = n-1;
    struct FD_EXTDB_PROC **dbprocs = db->extdb_procs;
    while (i>=0)
      if ((dbprocs[i]) == proc) {
        memmove(dbprocs+i,dbprocs+i+1,(n-(i+1))*sizeof(struct FD_EXTDB_PROC *));
        db->extdb_n_procs--;
        u8_unlock_mutex(&(db->extdb_proclock));
        return 1;}
      else i--;}
  u8_unlock_mutex(&(db->extdb_proclock));
  u8_log(LOG_CRIT,"extdb_release_proc","Release of unregistered extdb proc");
  return 0;
}

/* EXTDB handlers */

static int unparse_extdb(u8_output out,lispval x)
{
  struct FD_EXTDB *dbp = (struct FD_EXTDB *)x;
  u8_printf(out,"#<EXTDB/%s %s>",dbp->extdb_handler->name,dbp->extdb_info);
  return 1;
}

static void recycle_extdb(struct FD_RAW_CONS *c)
{
  struct FD_EXTDB *dbp = (struct FD_EXTDB *)c;
  dbp->extdb_handler->recycle_db(dbp);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

static int unparse_extdb_proc(u8_output out,lispval x)
{
  struct FD_EXTDB_PROC *dbp = (struct FD_EXTDB_PROC *)x;
  u8_printf(out,"#<DBλ/%s %s: %s>",
            dbp->extdb_handler->name,dbp->extdb_spec,dbp->extdb_qtext);
  return 1;
}

static void recycle_extdb_proc(struct FD_RAW_CONS *c)
{
  struct FD_EXTDB_PROC *dbproc = (struct FD_EXTDB_PROC *)c;
  if (dbproc->extdb_handler == NULL)
    u8_log(LOG_WARN,_("recycle failed"),"Bad extdb proc");
  else if (dbproc->extdb_handler->recycle_proc) {
    dbproc->extdb_handler->recycle_proc(dbproc);
    if (dbproc->fcn_typeinfo) u8_free(dbproc->fcn_typeinfo);
    if (dbproc->fcn_defaults) u8_free(dbproc->fcn_defaults);
    if (dbproc->fcn_attribs) fd_decref(dbproc->fcn_attribs);
    if (dbproc->fcn_moduleid) fd_decref(dbproc->fcn_moduleid);}
  else u8_log(LOG_WARN,_("recycle failed"),
              _("No recycle method for %s database procs"),
              dbproc->extdb_handler->name);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

static lispval callextdbproc(struct FD_FUNCTION *xdbproc,int n,lispval *args)
{
  struct FD_EXTDB_PROC *dbp = (struct FD_EXTDB_PROC *)xdbproc;
  return dbp->fcn_handler.xcalln(xdbproc,n,args);
}

/* EXTDB primitives */

static int exec_enabled = 0;

static int check_exec_enabled(lispval opts)
{
  lispval v = fd_getopt(opts,exec_enabled_symbol,VOID);
  if (VOIDP(v)) return 0;
  else if (FALSEP(v)) return 0;
  fd_decref(v);
  return 1;
}

static lispval extdb_exec(lispval db,lispval query,lispval colinfo)
{
  struct FD_EXTDB *extdb = FD_GET_CONS(db,fd_extdb_type,struct FD_EXTDB *);
  if ((exec_enabled)||
      ((fd_testopt(extdb->extdb_options,exec_enabled_symbol,VOID))&&
       (check_exec_enabled(extdb->extdb_options))))
    return extdb->extdb_handler->execute(extdb,query,colinfo);
  else return fd_err(_("Direct SQL execution disabled"),"extdb_exec",
                     CSTRING(query),db);
}

static lispval extdb_makeproc(int n,lispval *args)
{
  if (PRED_TRUE
      ((FD_PRIM_TYPEP(args[0],fd_extdb_type)) && (STRINGP(args[1])))) {
    struct FD_EXTDB *extdb=
      FD_GET_CONS(args[0],fd_extdb_type,struct FD_EXTDB *);
    lispval dbspec = args[0], query = args[1];
    lispval colinfo = ((n>2) ? (args[2]) : (VOID));
    if (extdb == NULL) return FD_ERROR;
    else if (!(STRINGP(query)))
      return fd_type_error("string","extdb_makeproc",query);
    else if ((extdb->extdb_handler->makeproc) == NULL)
      return fd_err(NoMakeProc,"extdb_makeproc",NULL,dbspec);
    else return extdb->extdb_handler->makeproc
           (extdb,CSTRING(query),STRLEN(query),colinfo,
            ((n>3) ? (n-3) : (0)),
            ((n>3)? (args+3) : (NULL)));}
  else if (!(FD_PRIM_TYPEP(args[0],fd_extdb_type)))
    return fd_type_error("extdb","extdb_makeproc",args[0]);
  else if  (!(STRINGP(args[1])))
    return fd_type_error("string","extdb_makeproc",args[1]);
  /* Should never be reached  */
  else return VOID;
}

static lispval extdb_proc_plus(int n,lispval *args)
{
  lispval arg1 = args[0], result = VOID;
  struct FD_EXTDB_PROC *extdbproc=
    FD_GET_CONS(arg1,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  lispval extdbptr = extdbproc->extdbptr;
  struct FD_EXTDB *extdb=
    FD_GET_CONS(extdbptr,fd_extdb_type,struct FD_EXTDB *);
  u8_string base_qtext = extdbproc->extdb_qtext, new_qtext=
    u8_string_append(base_qtext," ",CSTRING(args[1]),NULL);
  lispval colinfo = extdbproc->extdb_colinfo;
  int n_base_params = extdbproc->fcn_n_params, n_params = (n-2)+n_base_params;
  lispval *params = ((n_params)?(u8_alloc_n(n_params,lispval)):(NULL));
  lispval *base_params = extdbproc->extdb_paramtypes, param_count = 0;
  int i = n-1; while (i>=2) {
    lispval param = args[i--]; fd_incref(param);
    params[param_count++]=param;}
  i = n_base_params-1; while (i>=0) {
    lispval param = base_params[i--]; fd_incref(param);
    params[param_count++]=param;}
  fd_incref(colinfo);
  result = extdb->extdb_handler->makeproc
    (extdb,new_qtext,strlen(new_qtext),colinfo,param_count,params);
  u8_free(new_qtext);
  return result;
}

/* Accessors */

static lispval extdb_proc_query(lispval extdb)
{
  struct FD_EXTDB_PROC *xdbp = FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  return lispval_string(xdbp->extdb_qtext);
}

static lispval extdb_proc_spec(lispval extdb)
{
  struct FD_EXTDB_PROC *xdbp = FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  return lispval_string(xdbp->extdb_spec);
}

static lispval extdb_proc_db(lispval extdb)
{
  struct FD_EXTDB_PROC *xdbp = FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  return fd_incref(xdbp->extdbptr);
}

static lispval extdb_proc_typemap(lispval extdb)
{
  struct FD_EXTDB_PROC *xdbp = FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  return fd_incref(xdbp->extdb_colinfo);
}

static lispval extdb_proc_params(lispval extdb)
{
  struct FD_EXTDB_PROC *xdbp = FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  int n = xdbp->fcn_n_params;
  lispval *paramtypes = xdbp->extdb_paramtypes;
  lispval result = fd_make_vector(n,NULL);
  int i = 0; while (i<n) {
    lispval param_info = paramtypes[i]; fd_incref(param_info);
    FD_VECTOR_SET(result,i,param_info);
    i++;}
  return result;
}

/* Initializations */

int extdb_initialized = 0;

FD_EXPORT void fd_init_extdbprims_c()
{
  lispval extdb_module;
  if (extdb_initialized) return;
  extdb_initialized = 1;
  fd_init_scheme();
  extdb_module = fd_new_cmodule("EXTDB",(0),fd_init_extdbprims_c);
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&extdb_handlers_lock);

  exec_enabled_symbol = fd_intern("%EXECOK");

  fd_extdb_type = fd_register_cons_type("EXTDB");
  fd_recyclers[fd_extdb_type]=recycle_extdb;
  fd_unparsers[fd_extdb_type]=unparse_extdb;

  fd_extdb_proc_type = fd_register_cons_type("EXTDBPROC");
  fd_recyclers[fd_extdb_proc_type]=recycle_extdb_proc;
  fd_unparsers[fd_extdb_proc_type]=unparse_extdb_proc;
  fd_applyfns[fd_extdb_proc_type]=(fd_applyfn)callextdbproc;
  fd_functionp[fd_extdb_proc_type]=1;

  fd_idefn3(extdb_module,"EXTDB/EXEC",extdb_exec,2,
            "`(EXTDB/EXEC *dbptr* *sql* *colinfo*)`",
            fd_extdb_type,VOID,
            fd_string_type,VOID,
            -1,VOID);
  fd_idefnN(extdb_module,"EXTDB/PROC",extdb_makeproc,2,
            "`(EXTDB/PROC *dbptr* *sql* [*colinfo*] [*paraminfo*...] )`");
  fd_idefnN(extdb_module,"EXTDB/PROC+",extdb_proc_plus,2,
            "`(EXTDB/PROC *dbptr* *sql* [*colinfo*] [*paraminfo*...] )`");

  fd_idefn1(extdb_module,"EXTDB/PROC/QUERY",extdb_proc_query,1,
            "`(EXTDB/PROC/QUERY *dbproc*)` => sqlstring",
            fd_extdb_proc_type,VOID);
  fd_idefn1(extdb_module,"EXTDB/PROC/DB",extdb_proc_db,1,
            "`(EXTDB/PROC/DB *dbproc*)` => dbptr",
            fd_extdb_proc_type,VOID);
  fd_idefn1(extdb_module,"EXTDB/PROC/SPEC",extdb_proc_spec,1,
            "`(EXTDB/PROC/SPEC *dbproc*)` => dbspecstring",
            fd_extdb_proc_type,VOID);
  fd_idefn1(extdb_module,"EXTDB/PROC/PARAMS",extdb_proc_params,1,
            "`(EXTDB/PROC/PARAMS *dbproc*)` => paraminfo",
            fd_extdb_proc_type,VOID);
  fd_idefn1(extdb_module,"EXTDB/PROC/TYPEMAP",extdb_proc_typemap,1,
            "`(EXTDB/PROC/TYPEMAP *dbproc*)` => colinfo",
            fd_extdb_proc_type,VOID);

  fd_register_config("SQLEXEC",
                     _("whether direct execution of SQL strings is allowed"),
                     fd_boolconfig_get,fd_boolconfig_set,NULL);

  fd_finish_module(extdb_module);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
