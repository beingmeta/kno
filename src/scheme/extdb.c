/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
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

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"

#include "framerd/extdb.h"

#include <libu8/u8printf.h>

fd_ptr_type fd_extdb_type, fd_extdb_proc_type;

static fdtype exec_enabled_symbol;

static fd_exception NoMakeProc=
  _("No implementation for prepared SQL statements");

static struct FD_EXTDB_HANDLER *extdbhandlers[128];
int n_extdbs=0;

#if FD_THREADS_ENABLED
static u8_mutex extdb_handlers_lock;
#endif

FD_EXPORT int fd_register_extdb_handler(struct FD_EXTDB_HANDLER *h)
{
  int i=0;
  u8_lock_mutex(&extdb_handlers_lock);
  while (i<n_extdbs) 
    if (strcasecmp(h->name,extdbhandlers[i]->name)==0)
      if (h==extdbhandlers[i]) return 0;
      else {
	extdbhandlers[i]=h;
	return 1;}
    else i++;
  if (n_extdbs<128) {
    extdbhandlers[n_extdbs++]=h;
    return 2;}
  else {
    u8_seterr(_("Too many extdb handlers"),"fd_register_extdb_handler",NULL);
    return -1;}
}

FD_EXPORT int fd_register_extdb_proc(struct FD_EXTDB_PROC *proc)
{
  struct FD_EXTDB *db=FD_GET_CONS(proc->db,fd_extdb_type,struct FD_EXTDB *);
  u8_lock_mutex(&(db->proclock)); {
    int i=0, n=db->n_procs;
    struct FD_EXTDB_PROC **dbprocs=db->procs;
    while (i<n)
      if ((dbprocs[i])==proc) {
	u8_unlock_mutex(&(db->proclock));
	return 0;}
      else i++;
    if (i>=db->max_procs) {
      struct FD_EXTDB_PROC **newprocs=
	u8_realloc(dbprocs,sizeof(struct FD_EXTDB *)*(db->max_procs+32));
      if (newprocs==NULL) {
	u8_graberr(-1,"fd_extdb_register_proc",u8_strdup(db->spec));
	return -1;}
      else {
	db->procs=dbprocs=newprocs;
	db->max_procs=db->max_procs+32;}}
    dbprocs[i]=proc; db->n_procs++;}
  u8_unlock_mutex(&(db->proclock));
  return 1;
}

FD_EXPORT int fd_release_extdb_proc(struct FD_EXTDB_PROC *proc)
{
  struct FD_EXTDB *db=FD_GET_CONS(proc->db,fd_extdb_type,struct FD_EXTDB *);
  u8_lock_mutex(&(db->proclock)); {
    int n=db->n_procs, i=n-1;
    struct FD_EXTDB_PROC **dbprocs=db->procs;
    while (i>=0)
      if ((dbprocs[i])==proc) {
	memmove(dbprocs+i,dbprocs+i+1,(n-i)*sizeof(struct FD_EXTDB_PROC *));
	db->n_procs--;
	u8_unlock_mutex(&(db->proclock));
	return 1;}
      else i--;}
  u8_unlock_mutex(&(db->proclock));
  u8_log(LOG_CRIT,"extdb_release_proc","Release of unregistered extdb proc");
  return 0;
}

/* EXTDB handlers */

static void recycle_extdb(struct FD_CONS *c)
{
  struct FD_EXTDB *dbp=(struct FD_EXTDB *)c;
  dbp->dbhandler->recycle_extdb(dbp);
}

static int unparse_extdb(u8_output out,fdtype x)
{
  struct FD_EXTDB *dbp=(struct FD_EXTDB *)x;
  u8_printf(out,"#<EXTDB/%s %s>",dbp->dbhandler->name,dbp->info);
  return 1;
}

static void recycle_extdb_proc(struct FD_CONS *c)
{
  struct FD_EXTDB_PROC *dbproc=(struct FD_EXTDB_PROC *)c;
  if (dbproc->dbhandler == NULL)
    u8_log(LOG_WARN,_("recycle failed"),"Bad extdb proc");
  else if (dbproc->dbhandler->recycle_extdb_proc) 
    dbproc->dbhandler->recycle_extdb_proc(dbproc);
  else u8_log(LOG_WARN,_("recycle failed"),
	      _("No recycle method for %s database procs"),
	      dbproc->dbhandler->name);
}

static int unparse_extdb_proc(u8_output out,fdtype x)
{
  struct FD_EXTDB_PROC *dbp=(struct FD_EXTDB_PROC *)x;
  u8_printf(out,"#<DBPROC/%s %s: %s>",dbp->dbhandler->name,dbp->spec,dbp->qtext);
  return 1;
}

static fdtype callextdbproc(struct FD_FUNCTION *xdbproc,int n,fdtype *args)
{
  struct FD_EXTDB_PROC *dbp=(struct FD_EXTDB_PROC *)xdbproc;
  return dbp->handler.xcalln(xdbproc,n,args);
}

/* EXTDB primitives */

static int exec_enabled=0;

static int check_exec_enabled(fdtype opts)
{
  fdtype v=fd_getopt(opts,exec_enabled_symbol,FD_VOID);
  if (FD_VOIDP(v)) return 0;
  else if (FD_FALSEP(v)) return 0;
  fd_decref(v);
  return 1;
}

static fdtype extdb_exec(fdtype db,fdtype query,fdtype colinfo)
{
  struct FD_EXTDB *extdb=FD_GET_CONS(db,fd_extdb_type,struct FD_EXTDB *);
  if ((exec_enabled)||
      ((fd_testopt(extdb->options,exec_enabled,FD_VOID))&&
       (check_exec_enabled(extdb->options))))
    return extdb->dbhandler->execute(extdb,query,colinfo);
  else return fd_err(_("Direct SQL execution disabled"),"extdb_exec",
		     FD_STRDATA(query),db);
}  

static fdtype extdb_makeproc(int n,fdtype *args)
{
  if (FD_EXPECT_TRUE
      ((FD_PRIM_TYPEP(args[0],fd_extdb_type)) && (FD_STRINGP(args[1])))) {
    struct FD_EXTDB *extdb=
      FD_GET_CONS(args[0],fd_extdb_type,struct FD_EXTDB *);
    fdtype dbspec=args[0], query=args[1];
    fdtype colinfo=((n>2) ? (args[2]) : (FD_VOID));
    if (extdb==NULL) return FD_ERROR_VALUE;
    else if (!(FD_STRINGP(query))) 
      return fd_type_error("string","extdb_makeproc",query);
    else if ((extdb->dbhandler->makeproc)==NULL)
      return fd_err(NoMakeProc,"extdb_makeproc",NULL,dbspec);
    else return extdb->dbhandler->makeproc
	   (extdb,FD_STRDATA(query),FD_STRLEN(query),
	    colinfo,((n>3) ? (n-3) : (0)),((n>3)? (args+3) : (NULL)));}
  else if (!(FD_PRIM_TYPEP(args[0],fd_extdb_type)))
    return fd_type_error("extdb","extdb_makeproc",args[0]);
  else if  (!(FD_STRINGP(args[1])))
    return fd_type_error("string","extdb_makeproc",args[1]);
  /* Should never be reached  */
  else return FD_VOID;
}

/* Accessors */

static fdtype extdb_proc_query(fdtype extdb)
{
  struct FD_EXTDB_PROC *xdbp=FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  return fdtype_string(xdbp->qtext);
}

static fdtype extdb_proc_spec(fdtype extdb)
{
  struct FD_EXTDB_PROC *xdbp=FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  return fdtype_string(xdbp->spec);
}

static fdtype extdb_proc_db(fdtype extdb)
{
  struct FD_EXTDB_PROC *xdbp=FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  return fd_incref(xdbp->db);
}

static fdtype extdb_proc_typemap(fdtype extdb)
{
  struct FD_EXTDB_PROC *xdbp=FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  return fd_incref(xdbp->colinfo);
}

static fdtype extdb_proc_params(fdtype extdb)
{
  struct FD_EXTDB_PROC *xdbp=FD_GET_CONS
    (extdb,fd_extdb_proc_type,struct FD_EXTDB_PROC *);
  int n=xdbp->n_params;
  fdtype *paramtypes=xdbp->paramtypes;
  fdtype result=fd_make_vector(n,NULL);
  int i=0; while (i<n) {
    fdtype param_info=paramtypes[i]; fd_incref(param_info);
    FD_VECTOR_SET(result,i,param_info);
    i++;}
  return result;
}

/* Initializations */

int extdb_initialized=0;

FD_EXPORT void fd_init_extdbi_c()
{
  fdtype extdb_module;
  if (extdb_initialized) return;
  extdb_initialized=1;
  fd_init_fdscheme();
  extdb_module=fd_new_module("EXTDB",(0));
  u8_register_source_file(_FILEINFO);

#if FD_THREADS_ENABLED
  fd_init_mutex(&extdb_handlers_lock);
#endif

  exec_enabled_symbol=fd_intern("%EXECOK");

  fd_extdb_type=fd_register_cons_type("EXTDB");
  fd_recyclers[fd_extdb_type]=recycle_extdb;
  fd_unparsers[fd_extdb_type]=unparse_extdb;

  fd_extdb_proc_type=fd_register_cons_type("EXTDBPROC");
  fd_recyclers[fd_extdb_proc_type]=recycle_extdb_proc;
  fd_unparsers[fd_extdb_proc_type]=unparse_extdb_proc;
  fd_applyfns[fd_extdb_proc_type]=(fd_applyfn)callextdbproc;
  fd_functionp[fd_extdb_proc_type]=1;

  fd_idefn(extdb_module,
	   fd_make_cprim3x("EXTDB/EXEC",extdb_exec,2,
			   fd_extdb_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(extdb_module,fd_make_cprimn("EXTDB/PROC",extdb_makeproc,2));

  fd_idefn(extdb_module,fd_make_cprim1x
	   ("EXTDB/PROC/QUERY",extdb_proc_query,1,fd_extdb_proc_type));
  fd_idefn(extdb_module,fd_make_cprim1x
	   ("EXTDB/PROC/DB",extdb_proc_db,1,fd_extdb_proc_type));
  fd_idefn(extdb_module,fd_make_cprim1x
	   ("EXTDB/PROC/SPEC",extdb_proc_spec,1,fd_extdb_proc_type));
  fd_idefn(extdb_module,fd_make_cprim1x
	   ("EXTDB/PROC/PARAMS",extdb_proc_params,1,fd_extdb_proc_type));
  fd_idefn(extdb_module,fd_make_cprim1x
	   ("EXTDB/PROC/TYPEMAP",extdb_proc_typemap,1,fd_extdb_proc_type));

  fd_register_config("SQLEXEC",
		     _("whether direct execution of SQL strings is allowed"),
                     fd_boolconfig_get,fd_boolconfig_set,NULL);
  
  fd_finish_module(extdb_module);
  fd_persist_module(extdb_module);

}

