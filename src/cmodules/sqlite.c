/* C Mode */

/* odbc.c
   This implements FramerD bindings to odbc.
   Copyright (C) 2007-2008 beingmeta, inc.
*/

static char versionid[] =
  "$Id: texttools.c 2312 2008-02-23 23:49:10Z haase $";

#define U8_INLINE_IO 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/numbers.h"
#include "fdb/sequences.h"
#include "fdb/texttools.h"
#include <sqlite3.h>

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8digestfns.h>

FD_EXPORT int fd_init_sqlite(void) FD_LIBINIT_FN;

fd_ptr_type fd_sqlite_type;
typedef struct FD_SQLITE {
  FD_CONS_HEADER; u8_string cid; sqlite3 *db;} FD_SQLITE;
typedef struct FD_SQLITE *fd_sqlite;

fd_ptr_type fd_sqlite_proc_type;
typedef struct FD_SQLITE_PROC {
  FD_FUNCTION_FIELDS;
  sqlite3 *db; sqlite3_stmt *stmt;
  int n_params, *sqltypes;
  fdtype typemap, *argspec;
  u8_string cid, qtext; } FD_SQLITE_PROC;
typedef struct FD_SQLITE_PROC *fd_sqlite_proc;

static fd_exception SQLiteError=_("SQLite Error");

static fdtype intern_upcase(u8_output out,u8_string s)
{
  u8_byte *scan=s; int c=u8_sgetc(&s);
  out->u8_outptr=out->u8_outbuf;
  while (c>=0) {
    u8_putc(out,u8_toupper(c));
    c=u8_sgetc(&s);}
  return fd_make_symbol(out->u8_outbuf,out->u8_outptr-out->u8_outbuf);
}

static fdtype open_sqlite(fdtype filename,fdtype opts)
{
  sqlite3 *db=NULL;
#if HAVE_SQLITE3_OPEN_V2
  int flags=
    ((FD_VOIDP(opts)) ? (SQLITE_OPEN_READWRITE) :
     (FD_FALSEP(opts)) ? (SQLITE_OPEN_READONLY) :
     ((SQLITE_OPEN_READWRITE)|(SQLITE_OPEN_CREATE)));
  int retval=sqlite3_open(FD_STRDATA(filename),&db);
#else
  int retval=sqlite3_open(FD_STRDATA(filename),&db);
#endif
  if (retval) {
    u8_string msg=u8_strdup(sqlite3_errmsg(db));
    fd_seterr(SQLiteError,"open_sqlite",msg,filename);
    sqlite3_close(db);
    return FD_ERROR_VALUE;}
  else {
    struct FD_SQLITE *sqlcons=u8_alloc(struct FD_SQLITE);
    FD_INIT_CONS(sqlcons,fd_sqlite_type);
    sqlcons->db=db; sqlcons->cid=u8_strdup(FD_STRDATA(filename));
    return FDTYPE_CONS(sqlcons);}
}

static void recycle_sqlitedb(struct FD_CONS *c)
{
  struct FD_SQLITE *dbp=(struct FD_SQLITE *)c;
  u8_free(dbp->cid); sqlite3_close(dbp->db);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static int unparse_sqlitedb(u8_output out,fdtype x)
{
  struct FD_SQLITE *dbp=FD_GET_CONS(x,fd_sqlite_type,struct FD_SQLITE *);
  u8_printf(out,"#<SQLITE %s>",dbp->cid);
  return 1;
}

static int sqlite_basic_callback(void *rptr,int n_cols,char **vals,unsigned char **cols)
{
  fdtype *results=(fdtype *)rptr, result;
  struct FD_KEYVAL *slotvals=u8_alloc_n(n_cols,struct FD_KEYVAL);
  struct U8_OUTPUT out;
  int i=0;
  U8_INIT_OUTPUT(&out,64);
  while (i<n_cols) {
    slotvals[i].key=intern_upcase(&out,cols[i]); 
    slotvals[i].value=fdtype_string(vals[i]);
    i++;}
  result=fd_init_slotmap(NULL,n_cols*2,slotvals);
  FD_ADD_TO_CHOICE(*results,result);
  u8_free(out.u8_outbuf);
  return 0;
}

static fdtype sqlite_values(sqlite3 *db,sqlite3_stmt *stmt,fdtype typeinfo)
{
  fdtype results=FD_EMPTY_CHOICE;
  fdtype _colnames[16], *colnames;
  fdtype _colmaps[16], *colmaps;
  int i=0, n_cols=sqlite3_column_count(stmt), retval;
  struct U8_OUTPUT out;
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
  while (i<n_cols) {
    fdtype colname;
    colnames[i]=colname=intern_upcase(&out,(u8_string)sqlite3_column_name(stmt,i));
    colmaps[i]=((FD_VOIDP(typeinfo)) ? (FD_VOID) : (fd_get(typeinfo,colname,FD_VOID)));
    i++;}
  while ((retval=sqlite3_step(stmt))==SQLITE_ROW) {
    fdtype slotmap;
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
	value=FD_INT2DTYPE(intval); break;}
      case SQLITE_FLOAT:
	value=fd_init_double(NULL,sqlite3_column_double(stmt,j)); break;
      case SQLITE_TEXT:
	value=fdtype_string((unsigned char *)sqlite3_column_text(stmt,j)); break;
      case SQLITE_BLOB: {
	int n_bytes=sqlite3_column_bytes(stmt,j);
	const unsigned char *blob=sqlite3_column_blob(stmt,j);
	unsigned char *bytes=u8_alloc_n(n_bytes,unsigned char);
	memcpy(bytes,blob,n_bytes);
	value=fd_init_packet(NULL,n_bytes,bytes); break;}
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
	  unsigned int offset=fd_getint(value);
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
    slotmap=fd_init_slotmap(NULL,n_cols,kv);
    FD_ADD_TO_CHOICE(results,slotmap);}
  u8_free(out.u8_outbuf);
  return results;
}

static fdtype sqliteexec(fdtype db,fdtype string,fdtype typeinfo)
{
  sqlite3 *dbp=FD_GET_CONS(db,fd_sqlite_type,struct FD_SQLITE *)->db;
  sqlite3_stmt *stmt;
  fdtype results=FD_EMPTY_CHOICE; const char *errmsg="er, err";
  int retval=
    sqlite3_prepare_v2(dbp,FD_STRDATA(string),FD_STRLEN(string),
		       &stmt,NULL);
  if (retval==SQLITE_OK) {
    fdtype values=sqlite_values(dbp,stmt,typeinfo);
    if (FD_ABORTP(values)) {
      errmsg=sqlite3_errmsg(dbp);
      fd_seterr(SQLiteError,"fdsqlite_call",u8_strdup(errmsg),string);}
    sqlite3_finalize(stmt);
    return values;}
  else {
    errmsg=sqlite3_errmsg(dbp);
    fd_seterr(SQLiteError,"fdsqlite_call",u8_strdup(errmsg),db);
    return FD_ERROR_VALUE;}
}

/* SQLITE procs */

static fdtype sqliteproc(int n,fdtype *args)
{
  fdtype filename=args[0], query=args[1];
  fdtype typemap=((n>2) ? (args[2]) : (FD_VOID));
  sqlite3 *db=NULL;
#if HAVE_SQLITE3_OPEN_V2
  int flags=(SQLITE_OPEN_READWRITE);
  int retval=sqlite3_open(FD_STRDATA(filename),&db);
#else
  int retval=sqlite3_open(FD_STRDATA(filename),&db);
#endif
  if (retval) {
    u8_string msg=u8_strdup(sqlite3_errmsg(db));
    fd_seterr(SQLiteError,"open_sqlite",msg,filename);
    sqlite3_close(db);
    return FD_ERROR_VALUE;}
  else {
    struct FD_SQLITE_PROC *sqlcons=u8_alloc(struct FD_SQLITE_PROC);
    int retval, n_params;
    FD_INIT_FRESH_CONS(sqlcons,fd_sqlite_proc_type);
    sqlcons->db=db;
    sqlcons->filename=sqlcons->cid=u8_strdup(FD_STRDATA(filename));
    sqlcons->name=sqlcons->qtext=u8_strdup(FD_STRDATA(query));
    retval=sqlite3_prepare_v2(db,FD_STRDATA(query),FD_STRLEN(query),
			      &(sqlcons->stmt),NULL);
    sqlcons->n_params=n_params=sqlite3_bind_parameter_count(sqlcons->stmt);
    sqlcons->ndprim=0; sqlcons->xprim=1; sqlcons->arity=sqlcons->min_arity=n_params;
    {
      fdtype *argspec=u8_alloc_n(n_params,fdtype);
      int j=0; while (j<n_params) {
	if ((j+3)<=n) argspec[j]=fd_incref(args[j+3]);
	else argspec[j]=FD_VOID;
	j++;}
      sqlcons->argspec=argspec;}
    sqlcons->typemap=typemap;
    return FDTYPE_CONS(sqlcons);}
}

static void recycle_sqliteproc(struct FD_CONS *c)
{
  struct FD_SQLITE_PROC *dbp=(struct FD_SQLITE_PROC *)c;
  fd_decref(dbp->typemap);
  u8_free(dbp->cid); u8_free(dbp->qtext);
  {int j=0, lim=dbp->n_params;; while (j<lim) {
    fd_decref(dbp->argspec[j]); j++;}}
  u8_free(dbp->sqltypes); u8_free(dbp->argspec);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

static int unparse_sqliteproc(u8_output out,fdtype x)
{
  struct FD_SQLITE_PROC *dbp=
    FD_GET_CONS(x,fd_sqlite_proc_type,struct FD_SQLITE_PROC *);
  u8_printf(out,"#<SQLITE %s: %s>",dbp->cid,dbp->qtext);
  return 1;
}

static fdtype callsqliteproc(struct FD_FUNCTION *fn,int n,fdtype *args)
{
  struct FD_SQLITE_PROC *dbp=(struct FD_SQLITE_PROC *)fn;
  fdtype values=FD_EMPTY_CHOICE;
  int i=0, ret=-1;
  while (i<n) {
    fdtype arg=args[i]; int dofree=0;
    if (!(FD_VOIDP(dbp->argspec[i])))
      if (FD_APPLICABLEP(dbp->argspec[i])) {
	arg=fd_apply(dbp->argspec[i],1,&arg);
	if (FD_ABORTP(arg)) return arg;
	else dofree=1;}
    if (FD_PRIM_TYPEP(arg,fd_fixnum_type)) {
      int intval=FD_FIX2INT(arg);
      int retval=sqlite3_bind_int(dbp->stmt,i+1,intval);
      if (retval) {
	return FD_ERROR_VALUE;}}
    else if (FD_PRIM_TYPEP(arg,fd_double_type)) {
      double floval=FD_FLONUM(arg);
      sqlite3_bind_double(dbp->stmt,i+1,floval);}
    else if (FD_PRIM_TYPEP(arg,fd_string_type)) {
      sqlite3_bind_text(dbp->stmt,i+1,FD_STRDATA(arg),FD_STRLEN(arg),SQLITE_TRANSIENT);}
    else if (FD_OIDP(arg))
      if (FD_OIDP(dbp->argspec[i])) {
	FD_OID addr=FD_OID_ADDR(arg);
	FD_OID base=FD_OID_ADDR(dbp->argspec[i]);
	unsigned long offset=FD_OID_DIFFERENCE(addr,base);
	sqlite3_bind_int(dbp->stmt,i+1,offset);}
      else {
	FD_OID addr=FD_OID_ADDR(arg);
	sqlite3_bind_int64(dbp->stmt,i+1,addr);}
    if (dofree) fd_decref(arg);
    i++;}
  values=sqlite_values(dbp->db,dbp->stmt,dbp->typemap);
  sqlite3_reset(dbp->stmt);
  if (FD_ABORTP(values)) {
    const char *errmsg=sqlite3_errmsg(dbp->db);
    fd_seterr(SQLiteError,"fdsqlite_call",u8_strdup(errmsg),fd_incref((fdtype)fn));}
  return values;
}

/* Initialization */

static int sqlite_init=0;

FD_EXPORT int fd_init_sqlite()
{
  fdtype module;
  if (sqlite_init) return 0;
  module=fd_new_module("SQLITE",0);

  fd_sqlite_type=fd_register_cons_type("SQLITE");
  fd_recyclers[fd_sqlite_type]=recycle_sqlitedb;
  fd_unparsers[fd_sqlite_type]=unparse_sqlitedb;

  fd_sqlite_proc_type=fd_register_cons_type("SQLITEPROC");
  fd_recyclers[fd_sqlite_proc_type]=recycle_sqliteproc;
  fd_unparsers[fd_sqlite_proc_type]=unparse_sqliteproc;
  fd_applyfns[fd_sqlite_proc_type]=(fd_applyfn)callsqliteproc;

  fd_defn(module,
	  fd_make_cprim2x("SQLITE/OPEN",open_sqlite,1,
			  fd_string_type,FD_VOID,
			  -1,FD_VOID));
  fd_defn(module,
	  fd_make_cprim3x("SQLITE/EXEC",sqliteexec,2,
			  fd_sqlite_type,FD_VOID,
			  fd_string_type,FD_VOID,
			  -1,FD_VOID));
  fd_defn(module,fd_make_cprimn("SQLITE/PROC",sqliteproc,2));

  sqlite_init=1;

  fd_finish_module(module);

  fd_register_source_file(versionid);

  return 1;
}
