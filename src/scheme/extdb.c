/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
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

static char versionid[] =
  "$Id: fileprims.c 2888 2008-07-21 15:47:41Z haase $";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"

#include "fdb/extdb.h"

#include <libu8/u8printf.h>

fd_ptr_type fd_extdb_type, fd_extdb_proc_type;

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
  dbproc->dbhandler->recycle_extdb_proc(dbproc);
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

static fdtype extdb_exec(fdtype db,fdtype query,fdtype colinfo)
{
  struct FD_EXTDB *extdb=FD_GET_CONS(db,fd_extdb_type,struct FD_EXTDB *);
  return extdb->dbhandler->execute(extdb,query,colinfo);
}  

static fdtype extdb_makeproc(int n,fdtype *args)
{
  struct FD_EXTDB *extdb=FD_GET_CONS(args[0],fd_extdb_type,struct FD_EXTDB *);
  fdtype dbspec=args[0], query=args[1], colinfo=((n>2) ? (args[2]) : (FD_VOID));
  if (extdb==NULL) return FD_ERROR_VALUE;
  else if (!(FD_STRINGP(query))) 
    return fd_type_error("string","extdb_makeproc",query);
  else if ((extdb->dbhandler->makeproc)==NULL)
    return fd_err(NoMakeProc,"extdb_makeproc",NULL,dbspec);
  else return extdb->dbhandler->makeproc
	 (extdb,FD_STRDATA(query),FD_STRLEN(query),
	  colinfo,((n>3) ? (n-3) : (0)),((n>3)? (args+3) : (NULL)));
}

int extdb_initialized=0;

FD_EXPORT void fd_init_extdbi_c()
{
  fdtype extdb_module;
  if (extdb_initialized) return;
  extdb_initialized=1;
  fd_init_fdscheme();
  extdb_module=fd_new_module("EXTDB",(0));
  fd_register_source_file(versionid);

#if FD_THREADS_ENABLED
  fd_init_mutex(&extdb_handlers_lock);
#endif

  fd_extdb_type=fd_register_cons_type("EXTDB");
  fd_recyclers[fd_extdb_type]=recycle_extdb;
  fd_unparsers[fd_extdb_type]=unparse_extdb;

  fd_extdb_proc_type=fd_register_cons_type("EXTDB");
  fd_recyclers[fd_extdb_proc_type]=recycle_extdb_proc;
  fd_unparsers[fd_extdb_proc_type]=unparse_extdb_proc;
  fd_applyfns[fd_extdb_proc_type]=(fd_applyfn)callextdbproc;

  fd_idefn(extdb_module,
	   fd_make_cprim3x("EXTDB/EXEC",extdb_exec,2,
			   fd_extdb_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(extdb_module,fd_make_cprimn("EXTDB/PROC",extdb_makeproc,2));

  fd_finish_module(extdb_module);
  fd_persist_module(extdb_module);

}
