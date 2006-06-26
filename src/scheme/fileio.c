/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/frames.h"
#include "fdb/ports.h"
#include "fdb/numbers.h"

#include <libu8/filefns.h>
#include <libu8/stringfns.h>
#include <libu8/xfiles.h>

static U8_XINPUT u8stdin;
static U8_XOUTPUT u8stdout;
static U8_XOUTPUT u8stderr;

static fd_exception fd_ReloadError=_("Module reload error");
static fd_exception NoSuchFile=_("file does not exist");
static fd_exception RemoveFailed=_("File removal failed");

static u8_condition SnapshotSaved=_("Snapshot Saved");
static u8_condition SnapshotRestored=_("Snapshot Restored");

/* Making ports */

static fdtype make_port(U8_INPUT *in,U8_OUTPUT *out)
{
  struct FD_PORT *port=u8_malloc_type(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->in=in; port->out=out;
  return FDTYPE_CONS(port);
}

static u8_output get_output_port(fdtype portarg)
{
  if (FD_VOIDP(portarg))
    return fd_get_default_output();
  else if (FD_PRIM_TYPEP(portarg,fd_port_type)) {
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    return p->out;}
  else return NULL;
}

/* Opening files */

static fdtype open_output_file(fdtype fname,fdtype encid)
{
  u8_string filename=fd_strdata(fname);
  u8_encoding enc; U8_OUTPUT *f;
  if (FD_VOIDP(encid)) enc=NULL;
  else if (FD_STRINGP(encid))
    enc=u8_get_encoding(FD_STRDATA(encid));
  else if (FD_SYMBOLP(encid))
    enc=u8_get_encoding(FD_SYMBOL_NAME(encid));
  else return fd_err(fd_UnknownEncoding,"OPEN-OUTPUT-FILE",NULL,encid);
  f=(u8_output)u8_open_output_file(filename,enc,0,0);
  if (f==NULL)
    return fd_err(fd_CantOpenFile,"OPEN-OUTPUT-FILE",NULL,fname);
  else return make_port(NULL,(u8_output)f);
}

static fdtype open_input_file(fdtype fname,fdtype encid)
{
  u8_string filename=fd_strdata(fname);
  u8_encoding enc; U8_INPUT *f;
  if (FD_VOIDP(encid)) enc=NULL;
  else if (FD_STRINGP(encid))
    enc=u8_get_encoding(FD_STRDATA(encid));
  else if (FD_SYMBOLP(encid))
    enc=u8_get_encoding(FD_SYMBOL_NAME(encid));
  else return fd_err(fd_UnknownEncoding,"OPEN-INPUT_FILE",NULL,encid);
  f=(u8_input)u8_open_input_file(filename,enc,0,0);
  if (f==NULL)
    return fd_err(fd_CantOpenFile,"OPEN-INPUT-FILE",NULL,fname);
  else return make_port((u8_input)f,NULL);
}

/* FILEOUT */

static int printout_helper(U8_OUTPUT *out,fdtype x)
{
  if (FD_EXCEPTIONP(x)) return 0;
  else if (FD_VOIDP(x)) return 1;
  if (out == NULL) out=fd_get_default_output();
  if (FD_STRINGP(x))
    u8_printf(out,"%s",FD_STRDATA(x));
  else u8_printf(out,"%q",x);
  return 1;
}

static fdtype simple_fileout(fdtype expr,fd_lispenv env)
{
  fdtype filename_arg=fd_get_arg(expr,1);
  fdtype filename_val=fd_eval(filename_arg,env);
  fdtype body=fd_get_body(expr,2);
  u8_string filename; U8_OUTPUT *f, *oldf; int doclose;
  if (FD_EXCEPTIONP(filename_val)) return filename_val;
  else if (FD_PRIM_TYPEP(filename_val,fd_port_type)) {
    FD_PORT *port=FD_GET_CONS(filename_val,fd_port_type,FD_PORT *);
    if (port->out) {f=port->out; doclose=0;}
    else {
      fd_decref(filename_val);
      return fd_type_error(_("output port"),"simple_fileout",filename_val);}}
  else if (FD_STRINGP(filename_val)) {
    f=(u8_output)u8_open_output_file(FD_STRDATA(filename_val),NULL,0,0);
    if (f==NULL) {
      fd_decref(filename_val);
      return fd_err(fd_CantOpenFile,"FILEOUT",NULL,filename_val);}
    doclose=1;}
  else {
    fd_decref(filename_val);
    return fd_type_error(_("string"),"simple_fileout",filename_val);}
  oldf=fd_get_default_output();
  fd_set_default_output(f);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    if (printout_helper(f,value)) fd_decref(value);
    else {
      fd_set_default_output(oldf);
      fd_decref(filename_val);
      return value;}
    body=FD_CDR(body);}
  if (oldf) fd_set_default_output(oldf);
  if (doclose) u8_close_output(f);
  else u8_flush(f);
  fd_decref(filename_val);
  return FD_VOID;
}
/* Not really I/O but related structurally and logically */

static fdtype simple_system(fdtype expr,fd_lispenv env)
{
  struct U8_OUTPUT out; int result;
  fdtype body=fd_get_body(expr,1);
  U8_INIT_OUTPUT(&out,256);
  while (FD_PAIRP(body)) {
    fdtype value=fasteval(FD_CAR(body),env);
    body=FD_CDR(body);
    if (FD_EXCEPTIONP(value)) return value;
    else if (FD_VOIDP(value)) continue;
    else if (FD_STRINGP(value))
      u8_printf(&out,"%s",FD_STRDATA(value));
    else u8_printf(&out,"%q",value);
    fd_decref(value);}
  result=system(out.bytes); u8_free(out.bytes);
  return FD_INT2DTYPE(result);
}

static fdtype exit_prim(fdtype arg)
{
  if (FD_FIXNUMP(arg)) exit(FD_FIX2INT(arg));
  else exit(0);
  return FD_VOID;
}

#define FD_IS_SCHEME 1
#define FD_DO_FORK 2

static fdtype exec_helper(int flags,int n,fdtype *args)
{
  if (!(FD_STRINGP(args[0])))
    return fd_type_error("pathname","fdexec_prim",args[0]);
  else {
    char **argv=u8_malloc(sizeof(char *)*n+2), *filename=u8_tolibc(FD_STRDATA(args[0]));
    int i=1, argc=0; pid_t pid;
    if (flags&FD_IS_SCHEME) argv[argc++]=u8_strdup(FD_EXEC);
    argv[argc++]=filename;
    while (i<n)
      if (FD_STRINGP(args[i]))
	argv[argc++]=u8_tolibc(FD_STRDATA(args[i++]));
      else {
	u8_string as_string=fd_dtype2string(args[i++]);
	char *as_libc_string=u8_tolibc(as_string);
	argv[argc++]=as_libc_string; u8_free(as_string);}
    argv[argc++]=NULL;
    if (flags&FD_DO_FORK) 
      if (pid=fork()) {
	i=0; while (i<argc) if (argv[i]) u8_free(argv[i++]); else i++;
	u8_free(argv);
	return FD_INT2DTYPE(pid);}
      else if (flags&FD_IS_SCHEME)
	execvp(FD_EXEC,argv);
      else execvp(filename,argv);
    else if (flags&FD_IS_SCHEME)
      execvp(FD_EXEC,argv);
    else execvp(filename,argv);
    return fd_erreify();}
}

static fdtype exec_prim(int n,fdtype *args)
{
  return exec_helper(0,n,args);
}

static fdtype fdexec_prim(int n,fdtype *args)
{
  return exec_helper(FD_IS_SCHEME,n,args);
}

static fdtype fork_prim(int n,fdtype *args)
{
  return exec_helper(FD_DO_FORK,n,args);
}

static fdtype fdfork_prim(int n,fdtype *args)
{
  return exec_helper((FD_IS_SCHEME|FD_DO_FORK),n,args);
}

/* More file manipulation */

static fdtype remove_file_prim(fdtype arg,fdtype must_exist)
{
  u8_string filename=FD_STRDATA(arg);
  if (u8_file_existsp(filename)) 
    if (u8_removefile(FD_STRDATA(arg))<0) {
      fdtype err=fd_err(RemoveFailed,"remove_file_prim",filename,arg);
      return err;}
    else return FD_TRUE;
  else if (FD_TRUEP(must_exist)) {
    u8_string absolute=u8_abspath(filename,NULL);
    fdtype err=fd_err(NoSuchFile,"remove_file_prim",absolute,arg);
    u8_free(absolute);
    return err;}
  else return FD_FALSE;
}

static fdtype move_file_prim(fdtype from,fdtype to,fdtype must_exist)
{
  u8_string fromname=FD_STRDATA(from);
  if (u8_file_existsp(fromname)) 
    if (u8_movefile(FD_STRDATA(from),FD_STRDATA(to))<0) {
      fdtype err=fd_err(RemoveFailed,"move_file_prim",fromname,to);
      return err;}
    else return FD_TRUE;
  else if (FD_TRUEP(must_exist)) 
    return fd_err(NoSuchFile,"move_file_prim",NULL,from);
  else return FD_FALSE;
}

static fdtype link_file_prim(fdtype from,fdtype to,fdtype must_exist)
{
  u8_string fromname=FD_STRDATA(from);
  if (u8_file_existsp(fromname)) 
    if (u8_linkfile(FD_STRDATA(from),FD_STRDATA(to))<0) {
      fdtype err=fd_err(RemoveFailed,"link_file_prim",fromname,to);
      return err;}
    else return FD_TRUE;
  else if (FD_TRUEP(must_exist)) 
    return fd_err(NoSuchFile,"link_file_prim",NULL,from);
  else return FD_FALSE;
}

/* FILESTRING */

static fdtype filestring_prim(fdtype filename,fdtype enc)
{
  if (FD_VOIDP(enc)) {
    u8_string data=u8_filestring(FD_STRDATA(filename),"UTF-8");
    if (data)
      return fd_init_string(NULL,-1,data);
    else return fd_erreify();}
  else if (FD_STRINGP(enc)) {
    u8_string data=u8_filestring(FD_STRDATA(filename),FD_STRDATA(enc));
    if (data)
      return fd_init_string(NULL,-1,data);
    else return fd_erreify();}
  else return fd_err(fd_UnknownEncoding,"FILESTRING",NULL,enc);
}

static fdtype filedata_prim(fdtype filename)
{
  int len=-1;
  unsigned char *data=u8_filedata(FD_STRDATA(filename),&len);
  if (len>=0) return fd_init_packet(NULL,len,data);
  else return fd_erreify();
}


/* File information */

static fdtype file_existsp(fdtype arg)
{
  if (u8_file_existsp(FD_STRDATA(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype file_readablep(fdtype arg)
{
  if (u8_file_readablep(FD_STRDATA(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype file_writablep(fdtype arg)
{
  if (u8_file_writablep(FD_STRDATA(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype file_directoryp(fdtype arg)
{
  if (u8_directoryp(FD_STRDATA(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype file_basename(fdtype arg)
{
  return fd_init_string(NULL,-1,u8_basename(FD_STRDATA(arg),NULL));
}

static fdtype file_dirname(fdtype arg)
{
  return fd_init_string(NULL,-1,u8_dirname(FD_STRDATA(arg)));
}

static fdtype mkpath_prim(fdtype dirname,fdtype name)
{
  if (FD_SYMBOLP(dirname)) {
    fdtype config_val=fd_config_get(FD_SYMBOL_NAME(dirname));
    if (FD_STRINGP(config_val)) {
      u8_string path=u8_mkpath(FD_STRDATA(config_val),FD_STRDATA(name));
      fd_decref(config_val);
      return fd_init_string(NULL,-1,path);}
    else {
      fd_decref(config_val); 
      return fd_type_error
	(_("pathname or path CONFIG"),"mkpath_prim",dirname);}}
  else if (FD_STRINGP(dirname))
    return fd_init_string
      (NULL,-1,u8_mkpath(FD_STRDATA(dirname),FD_STRDATA(name)));
  else return fd_type_error
	 (_("pathname or path CONFIG"),"mkpath_prim",dirname);
}

/* File time info */

static fdtype make_timestamp(time_t tick)
{
  struct U8_XTIME xt; u8_localtime(&xt,tick);
  return fd_make_timestamp(&xt,NULL);
}

static fdtype file_modtime(fdtype filename)
{
  time_t mtime=u8_file_mtime(FD_STRDATA(filename));
  if (mtime<0) return fd_erreify();
  else return make_timestamp(mtime);
}

static fdtype file_atime(fdtype filename)
{
  time_t mtime=u8_file_atime(FD_STRDATA(filename));
  if (mtime<0) return fd_erreify();
  else return make_timestamp(mtime);
}

static fdtype file_ctime(fdtype filename)
{
  time_t mtime=u8_file_ctime(FD_STRDATA(filename));
  if (mtime<0) return fd_erreify();
  else return make_timestamp(mtime);
}

static fdtype file_mode(fdtype filename)
{
  mode_t mode=u8_file_mode(FD_STRDATA(filename));
  if (mode<0) return fd_erreify();
  else return FD_INT2DTYPE(mode);
}

static fdtype file_owner(fdtype filename)
{
  u8_string name=u8_file_owner(FD_STRDATA(filename));
  if (name) return fd_init_string(NULL,-1,name);
  else return fd_erreify();
}

/* Current directory information */

static fdtype getcwd_prim()
{
  u8_string wd=u8_getcwd();
  if (wd) return fd_init_string(NULL,-1,wd);
  else return fd_erreify();
}

static fdtype setcwd_prim(fdtype dirname)
{
  if (u8_setcwd(FD_STRDATA(dirname))<0)
    return fd_erreify();
  else return FD_VOID;
}

/* Directory listings */

static fdtype getfiles_prim(fdtype dirname,fdtype fullpath)
{
  fdtype results=FD_EMPTY_CHOICE;
  u8_string *contents=
    u8_getfiles(FD_STRDATA(dirname),(!(FD_FALSEP(fullpath)))), *scan=contents;
  while (*scan) {
    fdtype string=fd_init_string(NULL,-1,*scan);
    FD_ADD_TO_CHOICE(results,string);
    scan++;}
  u8_free(contents);
  return results;
}

static fdtype getdirs_prim(fdtype dirname,fdtype fullpath)
{
  fdtype results=FD_EMPTY_CHOICE;
  u8_string *contents=
    u8_getdirs(FD_STRDATA(dirname),(!(FD_FALSEP(fullpath)))), *scan=contents;
  while (*scan) {
    fdtype string=fd_init_string(NULL,-1,*scan);
    FD_ADD_TO_CHOICE(results,string);
    scan++;}
  u8_free(contents);
  return results;
}

/* Reading and writing DTYPEs */

static fdtype write_dtype(fdtype object,fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *out; int bytes;
    out=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_CREATE);
    if (out==NULL) return fd_erreify();
    else bytes=fd_dtswrite_dtype(out,object);
    fd_dtsclose(out,FD_DTSCLOSE_FULL);
    return FD_INT2DTYPE(bytes);}
  else return fd_type_error(_("string"),"write_dtype",filename);
}

static fdtype add_dtype(fdtype object,fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *out; int bytes;
    if (u8_file_existsp(FD_STRDATA(filename))) 
      out=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_MODIFY);
    else out=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_CREATE);
    if (out==NULL) return fd_erreify();
    fd_endpos(out);
    bytes=fd_dtswrite_dtype(out,object);
    fd_dtsclose(out,FD_DTSCLOSE_FULL);
    return FD_INT2DTYPE(bytes);}
  else return fd_type_error(_("string"),"write_dtype",filename);
}

static fdtype read_dtype(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *in; int bytes=0;
    fdtype object=FD_VOID;
    in=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_READ);
    if (in==NULL) return fd_erreify();
    else object=fd_dtsread_dtype(in);
    fd_dtsclose(in,FD_DTSCLOSE_FULL);
    return object;}
  else return fd_type_error(_("string"),"read_dtype",filename);
}

static fdtype read_dtypes(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *in; int bytes=0;
    fdtype results=FD_EMPTY_CHOICE, object=FD_VOID;
    in=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_READ);
    if (in==NULL) return fd_erreify();
    else {
      object=fd_dtsread_dtype(in);
      while (!(FD_EODP(object))) {
	FD_ADD_TO_CHOICE(results,object);
	object=fd_dtsread_dtype(in);}
      fd_dtsclose(in,FD_DTSCLOSE_FULL);
      return results;}}
  else return fd_type_error(_("string"),"read_dtype",filename);
}

/* Getting file sources */

static u8_string file_source_fn(u8_string filename,u8_string encname,u8_string *abspath)
{
  u8_string data=u8_filestring(filename,encname);
  if (data) {
    *abspath=u8_abspath(filename,NULL);
    return data;}
  else return NULL;
}

/* File flush function */

static fdtype flushprim(fdtype portarg)
{
  U8_OUTPUT *out=get_output_port(portarg);
  u8_flush(out);
  if (out->bits&U8_STREAM_OWNS_SOCKET) {
    U8_XOUTPUT *xout=(U8_XOUTPUT *)out;
    fsync(xout->fd);}
  return FD_VOID;
}

static fdtype setbufprim(fdtype portarg,fdtype insize,fdtype outsize)
{
  struct FD_PORT *p=
    FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
  if (FD_FIXNUMP(insize)) {
    U8_INPUT *in=p->in;
    if ((in) && (in->bits&U8_STREAM_OWNS_XBUF)) {
      u8_xinput_setbuf((struct U8_XINPUT *)in,FD_FIX2INT(insize));}}
  
  if (FD_FIXNUMP(outsize)) {
    U8_OUTPUT *out=p->out;
    if ((out) && (out->bits&U8_STREAM_OWNS_XBUF)) {
      u8_xoutput_setbuf((struct U8_XOUTPUT *)out,FD_FIX2INT(outsize));}}
  return FD_VOID;
}

static fdtype getpos_prim(fdtype portarg)
{
  struct FD_PORT *p=
    FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
  off_t result=-1;
  if (p->in)
    result=u8_getpos((struct U8_STREAM *)(p->in));
  else if (p->out)
    result=u8_getpos((struct U8_STREAM *)(p->out));
  else return fd_type_error(_("port"),"getpos_prim",portarg);
  if (result<0)
    return fd_erreify();
  else if (result<FD_MAX_FIXNUM)
    return FD_INT2DTYPE(result);
  else return fd_make_bigint(result);
}

static fdtype endpos_prim(fdtype portarg)
{
  struct FD_PORT *p=
    FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
  off_t result=-1;
  if (p->in)
    result=u8_endpos((struct U8_STREAM *)(p->in));
  else if (p->out)
    result=u8_endpos((struct U8_STREAM *)(p->out));
  else return fd_type_error(_("port"),"getpos_prim",portarg);
  if (result<0)
    return fd_erreify();
  else if (result<FD_MAX_FIXNUM)
    return FD_INT2DTYPE(result);
  else return fd_make_bigint(result);
}

static fdtype file_progress_prim(fdtype portarg)
{
  double result=-1.0;
  struct FD_PORT *p=
    FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
  if (p->in)
    result=u8_getprogress((struct U8_STREAM *)(p->in));
  else if (p->out)
    result=u8_getprogress((struct U8_STREAM *)(p->out));
  else return fd_type_error(_("port"),"getpos_prim",portarg);
  if (result<0)
    return fd_erreify();
  else return fd_init_double(NULL,result);
}

static fdtype setpos_prim(fdtype portarg,fdtype off_arg)
{
  off_t off, result;
  struct FD_PORT *p=
    FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
  if (FD_FIXNUMP(off_arg)) off=FD_FIX2INT(off_arg);
  else if (FD_PRIM_TYPEP(off_arg,fd_bigint_type)) 
#if (_FILE_OFFSET_BITS==64)
    off=(off_t)fd_bigint_to_long_long((fd_bigint)off_arg);
#else
    off=(off_t)fd_bigint_to_long((fd_bigint)off_arg);
#endif
  else return fd_type_error(_("offset"),"setpos_prim",off_arg);
  if (p->in)
    result=u8_setpos((struct U8_STREAM *)(p->in),off);
  else if (p->out)
    result=u8_setpos((struct U8_STREAM *)(p->out),off);
  else return fd_type_error(_("port"),"getpos",portarg);
  if (result<0)
    return fd_erreify();
  else if (result<FD_MAX_FIXNUM)
    return FD_INT2DTYPE(off);
  else return fd_make_bigint(result);
}

/* Module finding */

static fdtype safe_loadpath=FD_EMPTY_LIST;
static fdtype loadpath=FD_EMPTY_LIST;
static void add_load_record(u8_string filename,fd_lispenv env,time_t mtime);
static int load_source_module(u8_string name,int safe)
{
  if (safe==0) {
    FD_DOLIST(elt,loadpath) {
      if (FD_STRINGP(elt)) {
	u8_string module_filename=u8_find_file(name,FD_STRDATA(elt),NULL);
	if (module_filename) {
	  fd_lispenv working_env=fd_working_environment();
	  time_t mtime=u8_file_mtime(module_filename);
	  fdtype load_result=
	    fd_load_source(module_filename,working_env,"auto");
	  if (FD_EXCEPTIONP(load_result)) {
	    u8_free(module_filename);
	    fd_decref((fdtype)working_env);
	    return fd_interr(load_result);}
	  else {
	    fd_decref(load_result);
	    add_load_record(module_filename,working_env,mtime);
	    return 1;}}}}}
  {
    FD_DOLIST(elt,safe_loadpath) {
      if (FD_STRINGP(elt)) {
	u8_string module_filename=u8_find_file(name,FD_STRDATA(elt),NULL);
	if (module_filename) {
	  fd_lispenv working_env=fd_safe_working_environment();
	  time_t mtime=u8_file_mtime(module_filename);
	  fdtype load_result=
	    fd_load_source(module_filename,working_env,"auto");
	  if (FD_EXCEPTIONP(load_result)) {
	    u8_free(module_filename);
	    fd_decref((fdtype)working_env);
	    return fd_interr(load_result);}
	  else {
	    fd_decref(load_result);
	    add_load_record(module_filename,working_env,mtime);
	    return 1;}}}}}
  return 0;
}

static fdtype reload_module(fdtype module)
{
  if (FD_SYMBOLP(module)) {
    u8_string module_name=u8_downcase(FD_SYMBOL_NAME(module));
    int retval=load_source_module(module_name,0);
    u8_free(module_name);
    if (retval) return FD_TRUE; else return FD_FALSE;}
  else return fd_err(fd_TypeError,"reload_module",NULL,module);
}

static fdtype safe_reload_module(fdtype module)
{
  if (FD_SYMBOLP(module)) {
    u8_string module_name=u8_downcase(FD_SYMBOL_NAME(module));
    int retval=load_source_module(module_name,0);
    u8_free(module_name);
    if (retval) return FD_TRUE; else return FD_FALSE;}
  else return fd_err(fd_TypeError,"reload_module",NULL,module);
}

/* Automatic module reloading */

static double reload_interval=-1.0, last_reload=-1.0;

#if FD_THREADS_ENABLED
static u8_mutex load_record_lock;
#endif

struct FD_LOAD_RECORD {
  u8_string filename; time_t mtime; fd_lispenv env;
  struct FD_LOAD_RECORD *next;} *load_records=NULL;

static void add_load_record(u8_string filename,fd_lispenv env,time_t mtime)
{
  struct FD_LOAD_RECORD *scan;
  u8_lock_mutex(&load_record_lock);
  scan=load_records; while (scan)
    if ((strcmp(filename,scan->filename))==0) {
      if (env!=scan->env) {
	fd_decref((fdtype)(scan->env)); scan->env=env;}
      u8_free(filename);
      scan->mtime=mtime;
      u8_unlock_mutex(&load_record_lock);
      return;}
    else scan=scan->next;
  scan=u8_malloc(sizeof(struct FD_LOAD_RECORD));
  scan->filename=filename; scan->env=env; scan->mtime=mtime;
  scan->next=load_records; load_records=scan;
  u8_unlock_mutex(&load_record_lock);
}

FD_EXPORT int fd_update_file_modules(int force)
{
  int n_reloads=0;
  if ((force) ||
      ((reload_interval>=0) &&
       ((u8_elapsed_time()-last_reload)>reload_interval))) {
    struct FD_LOAD_RECORD *scan; double reload_time;
    u8_lock_mutex(&load_record_lock);
    reload_time=u8_elapsed_time();
    scan=load_records; while (scan) {
      time_t mtime=u8_file_mtime(scan->filename);
      if (mtime>scan->mtime) {
	fdtype load_result=
	  fd_load_source(scan->filename,scan->env,"auto");
	if (FD_EXCEPTIONP(load_result)) {
	  fd_seterr(fd_ReloadError,"fd_reload_modules",
		    u8_strdup(scan->filename),load_result);
	  return -1;}
	else {
	  fd_decref(load_result); n_reloads++;
	  scan->mtime=mtime;}}
      scan=scan->next;}
    last_reload=reload_time;
    u8_unlock_mutex(&load_record_lock);}
  return n_reloads;
}

static fdtype update_modules_prim(fdtype flag)
{
  if (fd_update_file_modules((!FD_FALSEP(flag)))<0)
    return fd_erreify();
  else return FD_VOID;
}

static fdtype updatemodules_config_get(fdtype var,void *ignored)
{
  if (reload_interval<0.0) return FD_FALSE;
  else return fd_init_double(NULL,reload_interval);
}
static int updatemodules_config_set(fdtype var,fdtype val,void *ignored)
{
  if (FD_FLONUMP(val)) {
    reload_interval=FD_FLONUM(val);
    return 1;}
  else if (FD_FLONUMP(val)) {
    reload_interval=fd_getint(val);
    return 1;}
  else if (FD_FALSEP(val)) {
    reload_interval=-1.0;
    return 1;}
  else if (FD_TRUEP(val)) {
    reload_interval=0.25;
    return 1;}
  else {
    fd_seterr(fd_TypeError,"updatemodules_config_get",NULL,val);
    return -1;}
}

/* Snapshot save and restore */

/* A snapshot is a set of variable bindings and CONFIG settings which
   can be saved to and restored from a disk file.  */

static fd_exception SnapshotTrouble=_("SNAPSHOT");

static fdtype snapshotvars, snapshotconfig, snapshotfile, configinfo;

FD_EXPORT
/* fd_snapshot:
     Arguments: an environment pointer and a filename (u8_string)
     Returns: an int (<0 on error)
 Saves a snapshot of the environment into the designated file.
*/ 
int fd_snapshot(fd_lispenv env,u8_string filename)
{
  fdtype vars=fd_symeval(snapshotvars,env);
  fdtype configvars=fd_symeval(snapshotconfig,env);
  if ((FD_EMPTY_CHOICEP(vars)) && (FD_EMPTY_CHOICEP(configvars))) {
    u8_message("No snapshot information to save");
    return FD_VOID;}
  else {
    struct FD_DTYPE_STREAM *out; int bytes;
    fdtype slotmap=(fdtype)fd_init_slotmap(NULL,0,NULL,NULL);
    if (FD_VOIDP(vars)) vars=FD_EMPTY_CHOICE;
    if (FD_VOIDP(configvars)) configvars=FD_EMPTY_CHOICE;
    {FD_DO_CHOICES(sym,vars)
       if (FD_SYMBOLP(sym)) {
	 fdtype val=fd_symeval(sym,env);
	 if (FD_VOIDP(val)) 
	   u8_warn(SnapshotTrouble,"The snapshot variable %q is unbound",sym);
	 else fd_add(slotmap,sym,val);
	 fd_decref(val);} 
       else {
	 fd_decref(slotmap);
	 return fd_type_error("symbol","fd_snapshot",sym);}}
    {FD_DO_CHOICES(sym,configvars)
       if (FD_SYMBOLP(sym)) {
	 fdtype val=fd_config_get(FD_SYMBOL_NAME(sym));
	 fdtype config_entry=fd_init_pair(NULL,sym,val);
	 if (FD_VOIDP(val)) 
	   u8_warn(SnapshotTrouble,"The snapshot config %q is not set",
		   sym);
	 else fd_add(slotmap,configinfo,config_entry);
	 fd_decref(val);}
       else {
	 fd_decref(slotmap);
	 return fd_type_error("symbol","fd_snapshot",sym);}}
    out=fd_dtsopen(filename,FD_DTSTREAM_CREATE);
    if (out==NULL) {
      fd_decref(slotmap);
      return -1;}
    else bytes=fd_dtswrite_dtype(out,slotmap);
    fd_dtsclose(out,FD_DTSCLOSE_FULL);
    u8_notify(SnapshotSaved,"Saved snapshot of %d items to %s",
	      FD_SLOTMAP_SIZE(slotmap),filename);
    fd_decref(slotmap); 
    return bytes;}
}

FD_EXPORT
/* fd_snapback:
     Arguments: an environment pointer and a filename (u8_string)
     Returns: an int (<0 on error)
 Restores a snapshot from the designated file into the environment
*/ 
int fd_snapback(fd_lispenv env,u8_string filename)
{
  struct FD_DTYPE_STREAM *in;
  fdtype slotmap; int actions=0;
  in=fd_dtsopen(filename,FD_DTSTREAM_READ);
  if (in==NULL) return -1;
  else slotmap=fd_dtsread_dtype(in);
  if (FD_ABORTP(slotmap)) {
    fd_dtsclose(in,FD_DTSCLOSE_FULL);
    return slotmap;}
  else if (FD_SLOTMAPP(slotmap)) {
    fdtype keys=fd_getkeys(slotmap);
    FD_DO_CHOICES(key,keys) {
      fdtype v=fd_get(slotmap,key,FD_VOID);
      if (FD_EQ(key,configinfo)) {
	FD_DO_CHOICES(config_entry,v)
	  if ((FD_PAIRP(config_entry)) &&
	      (FD_SYMBOLP(FD_CAR(config_entry)))) 
	    if (fd_config_set(FD_SYMBOL_NAME(FD_CAR(config_entry)),
			      FD_CDR(config_entry)) <0) {
	      fd_decref(v); fd_decref(keys); fd_decref(slotmap);
	      return -1;}
	    else actions++;
	  else {
	    fd_seterr(fd_TypeError,"fd_snapback",
		      u8_strdup("saved config entry"),fd_incref(config_entry));
	    fd_decref(v); fd_decref(keys); fd_decref(slotmap);
	    return -1;}}
      else {
	int setval=fd_set_value(key,v,env);
	if (setval==0) setval=fd_bind_value(key,v,env);
	if (setval<0) {
	  fd_decref(v); fd_decref(keys);
	  return -1;}}
      fd_decref(v);}
    fd_decref(keys);}
  else {
    return fd_reterr(fd_TypeError,"fd_snapback", u8_strdup("slotmap"),
		     slotmap);}
  u8_notify(SnapshotRestored,"Restored snapshot of %d items from %s",
	    FD_SLOTMAP_SIZE(slotmap),filename);
  fd_decref(slotmap);
  return actions;
}

static fdtype snapshot_handler(fdtype expr,fd_lispenv env)
{
  fd_lispenv save_env; u8_string save_file; int retval=0;
  fdtype arg1=fd_eval(fd_get_arg(expr,1),env), arg2=fd_eval(fd_get_arg(expr,2),env);
  if (FD_VOIDP(arg1)) {
    fdtype saveto=fd_symeval(snapshotfile,env);
    save_env=env;
    if (FD_STRINGP(saveto))
      save_file=u8_strdup(FD_STRDATA(saveto));
    else save_file=u8_strdup("snapshot");
    fd_decref(saveto);}
  else if (FD_ENVIRONMENTP(arg1)) {
    if (FD_STRINGP(arg2)) save_file=u8_strdup(FD_STRDATA(arg2));
    else save_file=u8_strdup("snapshot");
    save_env=(fd_lispenv)arg1;}
  else if (FD_STRINGP(arg1)) {
    save_file=u8_strdup(FD_STRDATA(arg1));
    if (FD_ENVIRONMENTP(arg2))
      save_env=(fd_lispenv)arg2;
    else save_env=env;}
  else {
    fdtype err=fd_type_error("filename","snapshot_prim",arg1);
    fd_decref(arg1); fd_decref(arg2);
    return err;}
  retval=fd_snapshot(save_env,save_file);
  fd_decref(arg1); fd_decref(arg2); u8_free(save_file);
  if (retval<0) return fd_erreify();
  else return FD_INT2DTYPE(retval);
}

static fdtype snapback_handler(fdtype expr,fd_lispenv env)
{
  fd_lispenv save_env; u8_string save_file; int retval=0;
  fdtype arg1=fd_eval(fd_get_arg(expr,1),env), arg2=fd_eval(fd_get_arg(expr,2),env);
  if (FD_VOIDP(arg1)) {
    fdtype saveto=fd_symeval(snapshotfile,env);
    save_env=env;
    if (FD_STRINGP(saveto))
      save_file=u8_strdup(FD_STRDATA(saveto));
    else save_file=u8_strdup("snapshot");
    fd_decref(saveto);}
  else if (FD_ENVIRONMENTP(arg1)) {
    if (FD_STRINGP(arg2)) save_file=u8_strdup(FD_STRDATA(arg2));
    else save_file=u8_strdup("snapshot");
    save_env=(fd_lispenv)arg1;}
  else if (FD_STRINGP(arg1)) {
    save_file=u8_strdup(FD_STRDATA(arg1));
    if (FD_ENVIRONMENTP(arg2))
      save_env=(fd_lispenv)arg2;
    else save_env=env;}
  else {
    fdtype err=fd_type_error("filename","snapshot_prim",arg1);
    fd_decref(arg1); fd_decref(arg2);
    return err;}
  retval=fd_snapback(save_env,save_file);
  fd_decref(arg1); fd_decref(arg2); u8_free(save_file);
  if (retval<0) return fd_erreify();
  else return FD_INT2DTYPE(retval);
}

/* Load DLL */

static fdtype load_dll(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    char *local=u8_localpath(FD_STRDATA(filename));
    void *module=u8_dynamic_load(local);
    if (module) {
      u8_free(local); return FD_TRUE;}
    else {
      u8_free(local);
      return fd_erreify();}}
  else return FD_VOID;
}

/* The init function */

static int scheme_fileio_initialized=0;

FD_EXPORT void fd_init_fileio_c()
{
  fdtype fileio_module;
  if (scheme_fileio_initialized) return;
  scheme_fileio_initialized=1;
  fd_init_fdscheme();
  fileio_module=fd_new_module("FILEIO",(FD_MODULE_DEFAULT));
  fd_register_source_file(versionid);

#if FD_THREADS_ENABLED
  u8_init_mutex(&load_record_lock);
#endif

  u8_init_xinput(&u8stdin,0,NULL);
  u8_init_xoutput(&u8stdout,1,NULL);
  u8_init_xoutput(&u8stderr,2,NULL);

  fd_set_global_output((u8_output)&u8stdout);

  fd_idefn(fileio_module,
	   fd_make_cprim2("OPEN-OUTPUT-FILE",open_output_file,1));
  fd_idefn(fileio_module,
	   fd_make_cprim2("OPEN-INPUT-FILE",open_input_file,1));
  fd_idefn(fileio_module,fd_make_cprim3x("SETBUF",setbufprim,2,
					 fd_port_type,FD_VOID,
					 -1,FD_FALSE,-1,FD_FALSE));
  

  fd_defspecial(fileio_module,"FILEOUT",simple_fileout);

  fd_defspecial(fileio_module,"SYSTEM",simple_system);

  fd_idefn(fileio_module,fd_make_cprim1("EXIT",exit_prim,0));

  fd_idefn(fileio_module,fd_make_cprimn("EXEC",exec_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FORK",fork_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FDEXEC",fdexec_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FDFORK",fdfork_prim,1));

  fd_idefn(fileio_module,
	   fd_make_cprim2x("REMOVE-FILE",remove_file_prim,1,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim3x("MOVE-FILE",move_file_prim,2,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim3x("LINK-FILE",link_file_prim,2,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));

  fd_idefn(fileio_module,
	   fd_make_cprim2x("FILESTRING",filestring_prim,1,
			   fd_string_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILEDATA",filedata_prim,1,
			   fd_string_type,FD_VOID));

  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILE-EXISTS?",file_existsp,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILE-READABLE?",file_readablep,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILE-WRITABLE?",file_writablep,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILE-DIRECTORY?",file_directoryp,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("DIRNAME",file_dirname,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("BASENAME",file_basename,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim2x("MKPATH",mkpath_prim,2,
			   -1,FD_VOID,fd_string_type,FD_VOID));


  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILE-MODTIME",file_modtime,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILE-ACCESSTIME",file_atime,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILE-CREATIONTIME",file_ctime,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILE-OWNER",file_owner,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("FILE-MODE",file_mode,1,
			   fd_string_type,FD_VOID));

  fd_idefn(fileio_module,
	   fd_make_cprim2x("GETFILES",getfiles_prim,1,
			   fd_string_type,FD_VOID,-1,FD_TRUE));
  fd_idefn(fileio_module,
	   fd_make_cprim2x("GETDIRS",getdirs_prim,1,
			   fd_string_type,FD_VOID,-1,FD_TRUE));

  fd_idefn(fileio_module,fd_make_cprim0("GETCWD",getcwd_prim,0));
  fd_idefn(fileio_module,
	   fd_make_cprim1x("SETCWD",setcwd_prim,1,fd_string_type,FD_VOID));

  fd_idefn(fileio_module,
	   fd_make_ndprim(fd_make_cprim2("DTYPE->FILE",write_dtype,2)));
  fd_idefn(fileio_module,
	   fd_make_ndprim(fd_make_cprim2("DTYPE->FILE+",add_dtype,2)));
  fd_idefn(fileio_module,
	   fd_make_cprim1("FILE->DTYPE",read_dtype,1));
  fd_idefn(fileio_module,
	   fd_make_cprim1("FILE->DTYPES",read_dtypes,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("FLUSH-OUTPUT",flushprim,0));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("GETPOS",getpos_prim,1,fd_port_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("SETPOS",setpos_prim,2,fd_port_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("ENDPOS",endpos_prim,1,fd_port_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("FILE%",file_progress_prim,1,fd_port_type,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim1("LOAD-DLL",load_dll,1));

  fd_init_filedb_c();

  fd_add_module_loader(load_source_module);
  fd_register_config("UPDATEMODULES",
		     updatemodules_config_get,updatemodules_config_set,NULL);
  fd_register_config("LOADPATH",fd_lconfig_get,fd_lconfig_push,&loadpath);
  fd_register_config("SAFELOADPATH",fd_lconfig_get,fd_lconfig_push,&safe_loadpath);

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1("RELOAD-MODULE",safe_reload_module,1));
  fd_idefn(fileio_module,
	   fd_make_cprim1("RELOAD-MODULE",reload_module,1));
  fd_idefn(fileio_module,
	   fd_make_cprim1("UPDATE-MODULES",update_modules_prim,0));

  {
    u8_string path=u8_getenv("FD_LOADPATH");
    fdtype v=((path) ? (fd_init_string(NULL,-1,path)) :
	      (fdtype_string(FD_DEFAULT_LOADPATH)));
    fd_config_set("LOADPATH",v);
    fd_decref(v);}
  {
    u8_string path=u8_getenv("FD_SAFE_LOADPATH");
    fdtype v=((path) ? (fd_init_string(NULL,-1,path)) :
	      (fdtype_string(FD_DEFAULT_SAFE_LOADPATH)));
    fd_config_set("SAFELOADPATH",v);
    fd_decref(v);}

  snapshotvars=fd_intern("%SNAPVARS");
  snapshotconfig=fd_intern("%SNAPCONFIG");
  snapshotfile=fd_intern("%SNAPSHOTFILE");  
  configinfo=fd_intern("%CONFIGINFO");  

  fd_defspecial(fileio_module,"SNAPSHOT",snapshot_handler);
  fd_defspecial(fileio_module,"SNAPBACK",snapback_handler);

  fd_finish_module(fileio_module);

  fd_register_sourcefn(file_source_fn);
}

FD_EXPORT void fd_init_schemeio()
{
  fd_init_fileio_c();
  fd_init_filedb_c();
}



/* The CVS log for this file
   $Log: fileio.c,v $
   Revision 1.44  2006/02/07 03:14:34  haase
   Fixed condition for snapshot restores

   Revision 1.43  2006/02/05 13:53:40  haase
   Added notifications of snapshot/snapback calls

   Revision 1.42  2006/01/28 02:39:57  haase
   Added getpos and setpos primitives

   Revision 1.41  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.40  2006/01/16 02:10:47  haase
   Fixes to snapshots

   Revision 1.39  2006/01/15 21:33:39  haase
   Reorganized session saving into snapshots

   Revision 1.38  2006/01/07 23:46:32  haase
   Moved thread API into libu8

   Revision 1.37  2006/01/07 19:45:08  haase
   Added savesession/restoresession primitives

   Revision 1.36  2006/01/04 18:05:14  haase
   Made FILESTRING default to explicit UTF-8 encoding

   Revision 1.35  2006/01/03 15:50:03  haase
   Added more file manipulation primitives

   Revision 1.34  2006/01/03 15:42:51  haase
   Make file move/etc functions grab their errno

   Revision 1.33  2005/12/30 23:09:50  haase
   Added SETBUF primitive

   Revision 1.32  2005/12/22 14:37:18  haase
   Fix leak in file loading

   Revision 1.31  2005/12/19 19:09:58  haase
   Added REMOVE-FILE primitive

   Revision 1.30  2005/12/17 21:56:15  haase
   Added EXIT primitive

   Revision 1.29  2005/11/29 16:59:22  haase
   Make filestring and filedata primitives pass on non-existent file errors

   Revision 1.28  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.27  2005/08/04 23:24:13  haase
   Added (optional) automatic module updating

   Revision 1.26  2005/07/23 21:38:51  haase
   Renaming file info primitives

   Revision 1.25  2005/07/23 21:35:57  haase
   Added file info primitives

   Revision 1.24  2005/06/23 15:51:19  haase
   Fixed some module GC bugs

   Revision 1.23  2005/06/15 02:46:30  haase
   Added file->dtypes to return multiple dtypes

   Revision 1.22  2005/06/04 21:04:01  haase
   Added DTYPE->FILE+ for appending a DTYPE to a file

   Revision 1.21  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.20  2005/05/16 04:20:22  haase
   Added mime parsing

   Revision 1.19  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.18  2005/05/04 09:42:42  haase
   Added module loading locking stuff

   Revision 1.17  2005/04/28 14:31:28  haase
   Created config handler for SAFELOADPATH, and distinct module for FILEIO

   Revision 1.16  2005/04/28 03:00:07  haase
   Added dirname and basename

   Revision 1.15  2005/04/24 02:09:05  haase
   Don't use realpath until inside the file to get get/load-component to work right

   Revision 1.14  2005/04/21 19:03:26  haase
   Add initialization procedures

   Revision 1.13  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.12  2005/04/14 02:02:06  haase
   Added reload modules primitive

   Revision 1.11  2005/04/06 15:25:22  haase
   Added default loadpath from conf-defines.h

   Revision 1.10  2005/04/04 22:21:52  haase
   Improved integration of error facilities

   Revision 1.9  2005/04/02 16:05:43  haase
   Make LOAD-DLL return errors

   Revision 1.8  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.7  2005/03/29 04:12:36  haase
   Added pool/index making primitives

   Revision 1.6  2005/03/26 20:20:32  haase
   Made flush-output do an fsync for XFILEs

   Revision 1.5  2005/03/26 18:31:41  haase
   Various configuration fixes

   Revision 1.4  2005/03/26 00:16:13  haase
   Made loading facility be generic and moved the rest of file access into fileio.c

   Revision 1.3  2005/03/25 17:48:37  haase
   More fixes for fileio separation

   Revision 1.2  2005/03/25 17:19:17  haase
   Added init of fd_default_output by fileio

   Revision 1.1  2005/03/25 13:25:40  haase
   Seperated out file io functions from generic port functions

*/
