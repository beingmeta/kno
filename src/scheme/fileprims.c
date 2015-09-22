/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/ports.h"
#include "framerd/numbers.h"
#include "framerd/fileprims.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8netfns.h>
#include <libu8/xfiles.h>

#include <stdlib.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#if HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#include <ctype.h>

#if HAVE_SIGNAL_H
#include <signal.h>
#endif

static U8_XINPUT u8stdin;
static U8_XOUTPUT u8stdout;
static U8_XOUTPUT u8stderr;

static fd_exception fd_ReloadError=_("Module reload error");
static fd_exception NoSuchFile=_("file does not exist");
static fd_exception RemoveFailed=_("File removal failed");
static fd_exception LinkFailed=_("File link failed");
static fd_exception OpenFailed=_("File open failed");

static u8_condition StackDumpEvent=_("StackDump");
static u8_condition SnapshotSaved=_("Snapshot Saved");
static u8_condition SnapshotRestored=_("Snapshot Restored");

static int log_reloads=1;

/* Making ports */

static fdtype make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct FD_PORT *port=u8_alloc(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->in=in; port->out=out; port->id=id;
  return FDTYPE_CONS(port);
}

static u8_output get_output_port(fdtype portarg)
{
  if ((FD_VOIDP(portarg))||(portarg==FD_TRUE))
    return u8_current_output;
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    return p->out;}
  else return NULL;
}

/* Opening files */

static fdtype open_output_file(fdtype fname,fdtype encid,fdtype escape_char)
{
  u8_string filename=fd_strdata(fname);
  u8_encoding enc; struct U8_XOUTPUT *f;
  if (FD_VOIDP(encid)) enc=NULL;
  else if (FD_STRINGP(encid))
    enc=u8_get_encoding(FD_STRDATA(encid));
  else if (FD_SYMBOLP(encid))
    enc=u8_get_encoding(FD_SYMBOL_NAME(encid));
  else return fd_err(fd_UnknownEncoding,"OPEN-OUTPUT-FILE",NULL,encid);
  f=u8_open_output_file(filename,enc,0,0);
  if (f==NULL)
    return fd_err(fd_CantOpenFile,"OPEN-OUTPUT-FILE",NULL,fname);
  if (FD_CHARACTERP(escape_char)) {
    int escape=FD_CHAR2CODE(escape_char);
    f->escape=escape;}
  return make_port(NULL,(u8_output)f,u8_strdup(filename));
}

static fdtype extend_output_file(fdtype fname,fdtype encid,fdtype escape_char)
{
  u8_string filename=fd_strdata(fname);
  u8_encoding enc; struct U8_XOUTPUT *f;
  if (FD_VOIDP(encid)) enc=NULL;
  else if (FD_STRINGP(encid))
    enc=u8_get_encoding(FD_STRDATA(encid));
  else if (FD_SYMBOLP(encid))
    enc=u8_get_encoding(FD_SYMBOL_NAME(encid));
  else return fd_err(fd_UnknownEncoding,"EXTEND-OUTPUT-FILE",NULL,encid);
  f=u8_open_output_file(filename,enc,O_APPEND|O_CREAT|O_WRONLY,0);
  if (f==NULL)
    return fd_err(fd_CantOpenFile,"EXTEND-OUTPUT-FILE",NULL,fname);
  if (FD_CHARACTERP(escape_char)) {
    int escape=FD_CHAR2CODE(escape_char);
    f->escape=escape;}
  return make_port(NULL,(u8_output)f,u8_strdup(filename));
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
  else return make_port((u8_input)f,NULL,u8_strdup(filename));
}

static fdtype open_dtype_file(fdtype fname)
{
  u8_string filename=FD_STRDATA(fname);
  struct FD_DTSTREAM *dts=u8_alloc(struct FD_DTSTREAM);
  FD_INIT_CONS(dts,fd_dtstream_type); dts->owns_socket=1;
  if (u8_file_existsp(filename))
    dts->dt_stream=fd_dtsopen(filename,FD_DTSTREAM_MODIFY);
  else dts->dt_stream=fd_dtsopen(filename,FD_DTSTREAM_CREATE);
  if (dts->dt_stream)
    return FDTYPE_CONS(dts);
  else {
    u8_free(dts);
    return FD_ERROR_VALUE;}
}

static fdtype extend_dtype_file(fdtype fname)
{
  u8_string filename=FD_STRDATA(fname);
  struct FD_DTSTREAM *dts=u8_alloc(struct FD_DTSTREAM);
  FD_INIT_CONS(dts,fd_dtstream_type); dts->owns_socket=1;
  if (u8_file_existsp(filename))
    dts->dt_stream=fd_dtsopen(filename,FD_DTSTREAM_MODIFY);
  else dts->dt_stream=fd_dtsopen(filename,FD_DTSTREAM_CREATE);
  if (dts->dt_stream) {
    fd_endpos(dts->dt_stream);
    return FDTYPE_CONS(dts);}
  else {
    u8_free(dts);
    return FD_ERROR_VALUE;}
}

static fdtype writefile_prim(fdtype filename,fdtype object,fdtype enc)
{
  int len=0; const unsigned char *bytes; int free_bytes=0;
  if (FD_STRINGP(object)) {
    bytes=FD_STRDATA(object); len=FD_STRLEN(object);}
  else if (FD_PACKETP(object)) {
    bytes=FD_PACKET_DATA(object); len=FD_PACKET_LENGTH(object);}
  else if ((FD_FALSEP(enc)) || (FD_VOIDP(enc))) {
    struct FD_BYTE_OUTPUT out;
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,object);
    bytes=out.start; len=out.ptr-out.start;
    free_bytes=1;}
  else {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
    fd_unparse(&out,object);
    bytes=out.u8_outbuf; len=out.u8_outptr-out.u8_outbuf;
    free_bytes=1;}
  if ((FD_FALSEP(enc)) || (FD_VOIDP(enc))) {
    FILE *f=u8_fopen(FD_STRDATA(filename),"w");
    size_t off=0, to_write=len;
    if (f==NULL) {
      if (free_bytes) u8_free(bytes);
      return fd_err(OpenFailed,"writefile_prim",NULL,filename);}
    while (to_write>0) {
      ssize_t n_bytes=fwrite(bytes+off,1,to_write,f);
      if (n_bytes<0) {
        u8_graberr(errno,"writefile_prim",u8_strdup(FD_STRDATA(filename)));
        return FD_ERROR_VALUE;}
      else {
        to_write=to_write-n_bytes;
        off=off+n_bytes;}}
    fclose(f);}
  else if ((FD_TRUEP(enc)) || (FD_STRINGP(enc))) {
    struct U8_TEXT_ENCODING *encoding=
      ((FD_STRINGP(enc)) ? (u8_get_encoding(FD_STRDATA(enc))) :
       (u8_get_default_encoding()));
    if (encoding==NULL) {
      if (free_bytes) u8_free(bytes);
      return fd_type_error("encoding","writefile_prim",enc);}
    else {
      U8_XOUTPUT *out=u8_open_output_file(FD_STRDATA(filename),encoding,-1,-1);
      if (out==NULL) {
        if (free_bytes) u8_free(bytes);
        return fd_reterr(OpenFailed,"writefile_prim",NULL,filename);}
      u8_putn((u8_output)out,bytes,len);
      u8_close((u8_stream)out);}}
  else {
    if (free_bytes) u8_free(bytes);
    return fd_type_error("encoding","writefile_prim",enc);}
  if (free_bytes) u8_free(bytes);
  return FD_INT2DTYPE(len);
}

/* FILEOUT */

static int printout_helper(U8_OUTPUT *out,fdtype x)
{
  if (FD_ABORTP(x)) return 0;
  else if (FD_VOIDP(x)) return 1;
  if (out == NULL) out=u8_current_output;
  if (FD_STRINGP(x))
    u8_printf(out,"%s",FD_STRDATA(x));
  else u8_printf(out,"%q",x);
  return 1;
}

static fdtype simple_fileout(fdtype expr,fd_lispenv env)
{
  fdtype filename_arg=fd_get_arg(expr,1);
  fdtype filename_val=fd_eval(filename_arg,env);
  U8_OUTPUT *f, *oldf; int doclose;
  if (FD_ABORTP(filename_val)) return filename_val;
  else if (FD_PORTP(filename_val)) {
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
  oldf=u8_current_output;
  u8_set_default_output(f);
  {FD_DOBODY(ex,expr,2)  {
      fdtype value=fasteval(ex,env);
      if (printout_helper(f,value)) fd_decref(value);
      else {
        u8_set_default_output(oldf);
        fd_decref(filename_val);
        return value;}}}
  if (oldf) u8_set_default_output(oldf);
  if (doclose) u8_close_output(f);
  else u8_flush(f);
  fd_decref(filename_val);
  return FD_VOID;
}

/* Not really I/O but related structurally and logically */

static fdtype simple_system(fdtype expr,fd_lispenv env)
{
  struct U8_OUTPUT out; int result;
  U8_INIT_OUTPUT(&out,256);
  {FD_DOBODY(subexpr,expr,1) {
      fdtype value=fasteval(subexpr,env);
      if (FD_ABORTP(value)) return value;
      else if (FD_VOIDP(value)) continue;
      else if (FD_STRINGP(value))
        u8_printf(&out,"%s",FD_STRDATA(value));
      else u8_printf(&out,"%q",value);
      fd_decref(value);}}
  result=system(out.u8_outbuf); u8_free(out.u8_outbuf);
  return FD_INT2DTYPE(result);
}

static fdtype exit_prim(fdtype arg)
{
  if (FD_FIXNUMP(arg)) exit(FD_FIX2INT(arg));
  else exit(0);
  return FD_VOID;
}

static fdtype ispid_prim(fdtype pid_arg)
{
  pid_t pid=FD_FIX2INT(pid_arg);
  int rv=kill(pid,0);
  if (rv<0) {
    errno=0; return FD_FALSE;}
  else return FD_TRUE;
}

static fdtype pid_kill_prim(fdtype pid_arg,fdtype sig_arg)
{
  pid_t pid=FD_FIX2INT(pid_arg); int sig=FD_FIX2INT(sig_arg);
  int rv=kill(pid,sig);
  if (rv<0) {
    char buf[128]; sprintf(buf,"pid=%ld;sig=%d",(long int)pid,sig);
    u8_graberr(-1,"os_kill_prim",u8_strdup(buf));
    return FD_ERROR_VALUE;}
  else return FD_TRUE;
}

#define FD_IS_SCHEME 1
#define FD_DO_FORK 2
#define FD_DO_WAIT 4
#define FD_DO_LOOKUP 8

static fdtype exec_helper(u8_context caller,int flags,int n,fdtype *args)
{
  if (!(FD_STRINGP(args[0])))
    return fd_type_error("pathname",caller,args[0]);
  else if ((!(flags&(FD_DO_LOOKUP|FD_IS_SCHEME)))&&
           (!(u8_file_existsp(FD_STRDATA(args[0])))))
    return fd_type_error("real file",caller,args[0]);
  else {
    char **argv;
    u8_byte *arg1=(u8_byte *)FD_STRDATA(args[0]);
    u8_byte *filename=NULL, *argcopy=NULL;
    int i=1, argc=0, max_argc=n+2, retval=0; pid_t pid;
    if (strchr(arg1,' ')) {
      const char *scan=arg1; while (scan) {
        const char *brk=strchr(scan,' ');
        if (brk) {max_argc++; scan=brk+1;}
        else break;}}
    else {}
    if ((n>1)&&(FD_SLOTMAPP(args[1]))) {
      fdtype keys=fd_getkeys(args[1]);
      max_argc=max_argc+FD_CHOICE_SIZE(keys);}
    argv=u8_alloc_n(max_argc,char *);
    if (flags&FD_IS_SCHEME) {
      argv[argc++]=filename=((u8_byte *)u8_strdup(FD_EXEC));
      argv[argc++]=(u8_byte *)u8_tolibc(arg1);}
    else if (strchr(arg1,' ')) {
#ifndef FD_EXEC_WRAPPER
      u8_free(argv);
      return fd_err(_("No exec wrapper to handle env args"),
                    caller,NULL,args[0]);
#else
      char *argcopy=u8_tolibc(arg1), *start=argcopy, *scan=strchr(start,' ');
      argv[argc++]=filename=(u8_byte *)u8_strdup(FD_EXEC_WRAPPER);
      while (scan) {
        *scan='\0'; argv[argc++]=start;
        start=scan+1; while (isspace(*start)) start++;
        scan=strchr(start,' ');}
      argv[argc++]=(u8_byte *)u8_tolibc(start);
#endif
    }
    else {
#ifdef FD_EXEC_WRAPPER
      argv[argc++]=filename=(u8_byte *)u8_strdup(FD_EXEC_WRAPPER);
#endif
      if (filename)
        argv[argc++]=u8_tolibc(arg1);
      else argv[argc++]=filename=(u8_byte *)u8_tolibc(arg1);}
    if ((n>1)&&(FD_SLOTMAPP(args[1]))) {
      fdtype params=args[1];
      fdtype keys=fd_getkeys(args[1]);
      FD_DO_CHOICES(key,keys) {
        if ((FD_SYMBOLP(key))||(FD_STRINGP(key))) {
          fdtype value=fd_get(params,key,FD_VOID);
          if (!(FD_VOIDP(value))) {
            struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
            u8_string stringval=NULL;
            if (FD_SYMBOLP(key)) stringval=FD_SYMBOL_NAME(key);
            else stringval=FD_STRDATA(key);
            if (stringval) u8_puts(&out,stringval);
            u8_putc(&out,'=');
            if (FD_STRINGP(value)) u8_puts(&out,FD_STRDATA(value));
            else fd_unparse(&out,value);
            argv[argc++]=out.u8_outbuf;
            fd_decref(value);}}}
        i++;}
    while (i<n) {
      if (FD_STRINGP(args[i]))
        argv[argc++]=u8_tolibc(FD_STRDATA(args[i++]));
      else {
        u8_string as_string=fd_dtype2string(args[i++]);
        char *as_libc_string=u8_tolibc(as_string);
        argv[argc++]=as_libc_string; u8_free(as_string);}}
    argv[argc++]=NULL;
    if ((flags&FD_DO_FORK)&&((pid=(fork())))) {
      i=0; while (i<argc) if (argv[i]) u8_free(argv[i++]); else i++;
      u8_free(argv);
#if HAVE_WAITPID
      if (flags&FD_DO_WAIT) {
        unsigned int retval=-1;
        waitpid(pid,&retval,0);
        return FD_INT2DTYPE(retval);}
#endif
      return FD_INT2DTYPE(pid);}
    if (flags&FD_IS_SCHEME)
      retval=execvp(FD_EXEC,argv);
    else if (flags&FD_DO_LOOKUP)
      retval=execvp(filename,argv);
    else retval=execvp(filename,argv);
    u8_log(LOG_CRIT,caller,"Fork exec failed (%d/%d:%s) with %s %s (%d)",
           retval,errno,strerror(errno),
           filename,argv[0],argc);
    /* We call abort in this case because we've forked but couldn't
       exec and we don't want this FramerD executable to exit normally. */
    if (flags&FD_DO_FORK) {
      u8_graberr(-1,caller,filename);
      return FD_ERROR_VALUE;}
    else {
      fd_clear_errors(1);
      abort();}}
}

static fdtype exec_prim(int n,fdtype *args)
{
  return exec_helper("exec_prim",0,n,args);
}

static fdtype execpath_prim(int n,fdtype *args)
{
  return exec_helper("execpath_prim",FD_DO_LOOKUP,n,args);
}

static fdtype fdexec_prim(int n,fdtype *args)
{
  return exec_helper("fdexec_prim",FD_IS_SCHEME,n,args);
}

static fdtype fork_prim(int n,fdtype *args)
{
  return exec_helper("fork_prim",FD_DO_FORK,n,args);
}

static fdtype forkpath_prim(int n,fdtype *args)
{
  return exec_helper("forkpath_prim",(FD_DO_FORK|FD_DO_LOOKUP),n,args);
}

static fdtype forkwait_prim(int n,fdtype *args)
{
  return exec_helper("forkwait_prim",(FD_DO_FORK|FD_DO_WAIT),n,args);
}

static fdtype forklookupwait_prim(int n,fdtype *args)
{
  return exec_helper("forklookupwait_prim",
                     (FD_DO_FORK|FD_DO_LOOKUP|FD_DO_WAIT),n,args);
}

static fdtype fdforkwait_prim(int n,fdtype *args)
{
  return exec_helper("fdforkwait_prim",
                     (FD_IS_SCHEME|FD_DO_FORK|FD_DO_WAIT),
                     n,args);
}

static fdtype fdfork_prim(int n,fdtype *args)
{
  return exec_helper("fdfork_prim",(FD_IS_SCHEME|FD_DO_FORK),n,args);
}

/* Opening TCP sockets */

static fdtype noblock_symbol, nodelay_symbol;

static fdtype open_socket_prim(fdtype spec,fdtype opts)
{

  u8_socket conn=u8_connect(FD_STRDATA(spec));
  if (conn<0) return FD_ERROR_VALUE;
  else {
    fdtype noblock=fd_getopt(opts,noblock_symbol,FD_FALSE);
    fdtype nodelay=fd_getopt(opts,nodelay_symbol,FD_FALSE);
    u8_xinput in=u8_open_xinput(conn,NULL);
    u8_xoutput out=u8_open_xoutput(conn,NULL);
    if (!(FD_FALSEP(noblock))) u8_set_blocking(conn,0);
    if (!(FD_FALSEP(nodelay))) u8_set_nodelay(conn,1);
    return make_port((u8_input)in,(u8_output)out,u8_strdup(FD_STRDATA(spec)));}
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

static fdtype remove_tree_prim(fdtype arg,fdtype must_exist)
{
  u8_string filename=FD_STRDATA(arg);
  if (u8_directoryp(filename))
    if (u8_rmtree(FD_STRDATA(arg))<0) {
      fdtype err=fd_err(RemoveFailed,"remove_tree_prim",filename,arg);
      return err;}
    else return FD_TRUE;
  else if (FD_TRUEP(must_exist)) {
    u8_string absolute=u8_abspath(filename,NULL);
    fdtype err=fd_err(NoSuchFile,"remove_tree_prim",absolute,arg);
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
      fdtype err=fd_err(LinkFailed,"link_file_prim",fromname,to);
      return err;}
    else return FD_TRUE;
  else if (FD_TRUEP(must_exist))
    return fd_err(NoSuchFile,"link_file_prim",NULL,from);
  else return FD_FALSE;
}

/* FILESTRING */

static fdtype filestring_prim(fdtype filename,fdtype enc)
{
  if ((FD_VOIDP(enc))||(FD_FALSEP(enc))) {
    u8_string data=u8_filestring(FD_STRDATA(filename),"UTF-8");
    if (data)
      return fd_lispstring(data);
    else return FD_ERROR_VALUE;}
  else if (FD_TRUEP(enc)) {
    u8_string data=u8_filestring(FD_STRDATA(filename),"auto");
    if (data) return fd_lispstring(data);
    else return FD_ERROR_VALUE;}
  else if (FD_STRINGP(enc)) {
    u8_string data=u8_filestring(FD_STRDATA(filename),FD_STRDATA(enc));
    if (data)
      return fd_lispstring(data);
    else return FD_ERROR_VALUE;}
  else return fd_err(fd_UnknownEncoding,"FILESTRING",NULL,enc);
}

static fdtype filedata_prim(fdtype filename)
{
  int len=-1;
  unsigned char *data=u8_filedata(FD_STRDATA(filename),&len);
  if (len>=0) return fd_init_packet(NULL,len,data);
  else return FD_ERROR_VALUE;
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

static fdtype file_abspath(fdtype arg,fdtype wd)
{
  u8_string result;
  if (FD_VOIDP(wd))
    result=u8_abspath(FD_STRDATA(arg),NULL);
  else result=u8_abspath(FD_STRDATA(arg),FD_STRDATA(wd));
  if (result) return fd_lispstring(result);
  else return FD_ERROR_VALUE;
}

static fdtype file_realpath(fdtype arg,fdtype wd)
{
  u8_string result;
  if (FD_VOIDP(wd))
    result=u8_realpath(FD_STRDATA(arg),NULL);
  else result=u8_realpath(FD_STRDATA(arg),FD_STRDATA(wd));
  if (result) return fd_lispstring(result);
  else return FD_ERROR_VALUE;
}


static fdtype path_basename(fdtype arg,fdtype suffix)
{
  if ((FD_VOIDP(suffix)) || (FD_FALSEP(suffix)))
    return fd_lispstring(u8_basename(FD_STRDATA(arg),NULL));
  else if (FD_STRINGP(suffix))
    return fd_lispstring(u8_basename(FD_STRDATA(arg),FD_STRDATA(suffix)));
  else return fd_lispstring(u8_basename(FD_STRDATA(arg),"*"));
}

static fdtype path_suffix(fdtype arg,fdtype dflt)
{
  u8_string s=FD_STRDATA(arg);
  u8_string slash=strrchr(s,'/');
  u8_string dot=strrchr(s,'.');
  if ((dot)&&((!(slash))||(slash<dot)))
    return fdtype_string(dot);
  else if (FD_VOIDP(dflt))
    return fdtype_string("");
  else return fd_incref(dflt);
}

static fdtype path_dirname(fdtype arg)
{
  return fd_lispstring(u8_dirname(FD_STRDATA(arg)));
}

static fdtype path_location(fdtype arg)
{
  u8_string path=FD_STRDATA(arg); size_t len=FD_STRLEN(arg);
  u8_string slash=strrchr(path,'/');
  if (slash[1]=='\0') return fd_incref(arg);
  else return fd_substring(path,slash+1);
}

static fdtype mkpath_prim(fdtype dirname,fdtype name)
{
  fdtype config_val=FD_VOID; u8_string dir=NULL, namestring=NULL;
  if (!(FD_STRINGP(name)))
    return fd_type_error(_("string"),"mkuripath_prim",name);
  else namestring=FD_STRDATA(name);
  if (*namestring=='/') return fd_incref(name);
  else if ((FD_STRINGP(dirname))&&(FD_STRLEN(dirname)==0))
    return fd_incref(name);
  else if (FD_STRINGP(dirname)) dir=FD_STRDATA(dirname);
  else if (FD_SYMBOLP(dirname)) {
    config_val=fd_config_get(FD_SYMBOL_NAME(dirname));
    if (FD_STRINGP(config_val)) dir=FD_STRDATA(config_val);
    else {
      fd_decref(config_val);
      return fd_type_error(_("string CONFIG var"),"mkuripath_prim",dirname);}}
  else return fd_type_error
         (_("string or string CONFIG var"),"mkuripath_prim",dirname);
  if (FD_VOIDP(config_val))
    return fd_lispstring(u8_mkpath(dir,namestring));
  else {
    fdtype result=fd_lispstring(u8_mkpath(dir,namestring));
    fd_decref(config_val);
    return result;}
}

/* Getting the runbase for a script file */

static fdtype runfile_prim(fdtype suffix)
{
  return fd_lispstring(fd_runbase_filename(FD_STRDATA(suffix)));
}

/* Making directories */

static fdtype mkdir_prim(fdtype dirname,fdtype mode_arg)
{
  mode_t mode=
    ((FD_FIXNUMP(mode_arg))?((mode_t)(FD_FIX2INT(mode_arg))):((mode_t)0777));
  int retval=u8_mkdir(FD_STRDATA(dirname),mode);
  if (retval<0) {
    u8_condition cond=u8_strerror(errno); errno=0;
    return fd_err(cond,"mkdir_prim",NULL,dirname);}
  else if (retval) {
    /* Force the mode to be set if provided */
    if (FD_FIXNUMP(mode_arg))
      u8_chmod(FD_STRDATA(dirname),((mode_t)(FD_FIX2INT(mode_arg))));
    return FD_TRUE;}
  else return FD_FALSE;
}

static fdtype rmdir_prim(fdtype dirname)
{
  int retval=u8_rmdir(FD_STRDATA(dirname));
  if (retval<0) {
    u8_condition cond=u8_strerror(errno); errno=0;
    return fd_err(cond,"rmdir_prim",NULL,dirname);}
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype mkdirs_prim(fdtype pathname,fdtype mode_arg)
{
  mode_t mode=
    ((FD_FIXNUMP(mode_arg))?((mode_t)(FD_FIX2INT(mode_arg))):((mode_t)0777));
  int retval=u8_mkdirs(FD_STRDATA(pathname),mode);
  if (retval<0) {
    u8_condition cond=u8_strerror(errno); errno=0;
    return fd_err(cond,"mkdirs_prim",NULL,pathname);}
  else if (retval==0) return FD_FALSE;
  else return FD_INT2DTYPE(retval);
}

/* Temporary directories */

static u8_string tempdir_template=NULL;

static char *get_tmpdir()
{
  char *tmpdir=getenv("TMPDIR");
  if (tmpdir) return tmpdir;
  tmpdir=getenv("TMP_DIR");
  if (tmpdir) return tmpdir;
  else return "/tmp";
}

static fdtype temproot_get(fdtype sym,void *ignore)
{
  if (tempdir_template) return fd_lispstring(tempdir_template);
  else return FD_EMPTY_CHOICE;
}

static int temproot_set(fdtype sym,fdtype value,void *ignore)
{
  u8_string template, old_template=tempdir_template;
  if (!(FD_STRINGP(value))) {
    fd_type_error("string","temproot_set",value);
    return -1;}
  if ((FD_STRDATA(value)[0])!='/')
    u8_log(LOG_WARN,"BADTEMPLATE",
           "Setting template to a relative file path");
  template=u8_strdup(FD_STRDATA(value));
  tempdir_template=template;
  if (old_template) u8_free(old_template);
  return 1;
}

static int delete_tempdirs_on_exit=1;
static fdtype tempdirs=FD_EMPTY_CHOICE, keeptemp=FD_EMPTY_CHOICE;
static u8_mutex tempdirs_lock;

static u8_string tempdir_core(fdtype template_arg,int keep)
{
  u8_string tempname;
  u8_string consed=NULL, template=
    ((FD_STRINGP(template_arg))?(FD_STRDATA(template_arg)):(NULL));
  if (FD_SYMBOLP(template_arg)) {
    fdtype ctemp=fd_config_get(FD_STRDATA(template_arg));
    if (FD_STRINGP(ctemp))
      template=consed=u8_strdup(FD_STRDATA(ctemp));
    fd_decref(ctemp);}
  if (!(template)) template=tempdir_template;
  if (!(template)) {
    char *tmpdir=get_tmpdir();
    template=consed=u8_mkpath(tmpdir,"fdtempXXXXXX");}
  /* Unlike mkdtemp, u8_tempdir doesn't overwrite its argument */
  tempname=u8_tempdir(template);
  if (tempname) {
    if (!(keep)) {
      fdtype result=fd_make_string(NULL,-1,tempname);
      fd_lock_mutex(&tempdirs_lock);
      FD_ADD_TO_CHOICE(tempdirs,result);
      fd_unlock_mutex(&tempdirs_lock);}
    if (consed) u8_free(consed);
    return tempname;}
  else {
    u8_condition cond=u8_strerror(errno); errno=0;
    if (consed) u8_free(consed);
    fd_seterr(cond,"tempdir_prim",u8_strdup(template),fd_incref(template_arg));
    return NULL;}
}

static void remove_tempdirs()
{
  int n_files=FD_CHOICE_SIZE(tempdirs);
  int n_keep=FD_CHOICE_SIZE(keeptemp);
  if ((n_files)&&(n_keep))
    u8_log(LOG_NOTICE,"TEMPFILES","Removing %d=%d-%d temporary directories",
           n_files-n_keep,n_files,n_keep);
  else if (n_files)
    u8_log(LOG_NOTICE,"TEMPFILES","Removing %d temporary directories",n_files);
  else return;
  u8_lock_mutex(&tempdirs_lock); {
    fdtype to_remove=fd_difference(tempdirs,keeptemp);
    FD_DO_CHOICES(tmpfile,to_remove) {
      if (FD_STRINGP(tmpfile)) {
        u8_log(LOG_DEBUG,"TEMPFILES","Removing directory %s",
               FD_STRDATA(tmpfile));
        u8_rmtree(FD_STRDATA(tmpfile));}}
    fd_decref(tempdirs); fd_decref(keeptemp); fd_decref(to_remove);
    tempdirs=FD_EMPTY_CHOICE; keeptemp=FD_EMPTY_CHOICE;
    u8_unlock_mutex(&tempdirs_lock);}
}

FD_EXPORT u8_string fd_tempdir(u8_string spec,int keep)
{
  fdtype template_arg=((spec)?(fd_parse_arg(spec)):(FD_VOID));
  u8_string dirname=tempdir_core(template_arg,keep);
  fd_decref(template_arg);
  return dirname;
}

static fdtype tempdir_prim(fdtype template_arg,fdtype keep)
{
  u8_string dirname=tempdir_core(template_arg,(!(FD_FALSEP(keep))));
  if (dirname==NULL) return FD_ERROR_VALUE;
  else return fd_init_string(NULL,-1,dirname);
}

static fdtype tempdir_done_prim(fdtype tempdir,fdtype force_arg)
{
  int force=(!(FD_FALSEP(force_arg))), doit=0;
  u8_string dirname=FD_STRDATA(tempdir);
  u8_lock_mutex(&tempdirs_lock); {
    fdtype cur_tempdirs=tempdirs; fdtype cur_keep=keeptemp;
    if (fd_overlapp(tempdir,cur_tempdirs)) {
      if (fd_overlapp(tempdir,cur_keep)) {
        if (force) {
          u8_log(LOG_WARN,"Forced temp deletion",
                 "Forcing deletion of directory %s, declared for safekeeping",dirname);
          keeptemp=fd_difference(cur_keep,tempdir);
          tempdirs=fd_difference(cur_tempdirs,tempdir);
          fd_decref(cur_keep); fd_decref(cur_tempdirs);
          doit=1;}
        else u8_log(LOG_WARN,_("Keeping temp directory"),
                    "The directory %s is declared for safekeeping, leaving",dirname);}
      else {
        u8_log(LOG_INFO,_("Deleting temp directory"),
               "Explicitly deleting temporary directory %s",dirname);
        tempdirs=fd_difference(cur_tempdirs,tempdir);
        fd_decref(cur_tempdirs);
        doit=1;}}
    else if (!(u8_directoryp(dirname)))
      u8_log(LOG_WARN,_("Weird temp directory"),
             "The temporary directory %s is neither declared or existing",dirname);
    else if (force) {
      u8_log(LOG_WARN,"Forced temp delete",
             "Forcing deletion of the undeclared temporary directory %s",dirname);
      doit=1;}
    else u8_log(LOG_WARN,"Unknown temp directory",
                "The directory %s is not a known temporary directory, leaving",
                FD_STRDATA(tempdir));
    u8_unlock_mutex(&tempdirs_lock);}
  if (doit) {
    int retval=u8_rmtree(dirname);
    if (retval<0) return FD_ERROR_VALUE;
    else if (retval==0) return FD_FALSE;
    else return FD_INT2DTYPE(retval);}
  else return FD_FALSE;
}

static fdtype is_tempdir_prim(fdtype tempdir)
{
  u8_lock_mutex(&tempdirs_lock); {
    fdtype cur_tempdirs=tempdirs; 
    int found=fd_overlapp(tempdir,cur_tempdirs);
    u8_unlock_mutex(&tempdirs_lock); 
    if (found) return FD_TRUE; else return FD_FALSE;}
}

static fdtype tempdirs_get(fdtype sym,void *ignore)
{
  fdtype dirs=tempdirs;
  fd_incref(dirs);
  return dirs;
}
static int tempdirs_add(fdtype sym,fdtype value,void *ignore)
{
  if (!(FD_STRINGP(value))) {
    fd_type_error("string","tempdirs_add",value);
    return -1;}
  else fd_incref(value);
  fd_lock_mutex(&tempdirs_lock);
  FD_ADD_TO_CHOICE(tempdirs,value);
  fd_unlock_mutex(&tempdirs_lock);
  return 1;
}

static fdtype keepdirs_get(fdtype sym,void *ignore)
{
  /* If delete_tempdirs_on_exit is zero, we keep all of the current
     tempdirs, or return true if there aren't any.  Otherwise, we just
     return the stored value. */
  if (delete_tempdirs_on_exit==0)
    return fd_deep_copy(tempdirs);
  else return fd_deep_copy(keeptemp);
}
static int keepdirs_add(fdtype sym,fdtype value,void *ignore)
{
  if (FD_TRUEP(value)) {
    if (delete_tempdirs_on_exit) {
      delete_tempdirs_on_exit=0;
      return 1;}
    else return 0;}
  else if (FD_FALSEP(value)) {
    if (!(delete_tempdirs_on_exit)) {
      delete_tempdirs_on_exit=1;
      return 1;}
    else return 0;}
  else if (!(FD_STRINGP(value))) {
    fd_type_error("string","keepdirs_add",value);
    return -1;}
  else if ((fd_boolstring(FD_STRDATA(value),-1))>=0) {
    int val=fd_boolstring(FD_STRDATA(value),-1);
    if (val) val=0; else val=1;
    if (delete_tempdirs_on_exit==val) return 0;
    else {
      delete_tempdirs_on_exit=val;
      return 1;}}
  else fd_incref(value);
  fd_lock_mutex(&tempdirs_lock);
  FD_ADD_TO_CHOICE(keeptemp,value);
  fd_unlock_mutex(&tempdirs_lock);
  return 1;
}

static fdtype keeptemp_get(fdtype sym,void *ignore)
{
  /* If delete_tempdirs_on_exit is zero, we keep all of the current
     tempdirs, or return true if there aren't any.  Otherwise, we just
     return the stored value. */
  if (delete_tempdirs_on_exit==0) return FD_TRUE;
  else return FD_FALSE;
}
static int keeptemp_set(fdtype sym,fdtype value,void *ignore)
{
  if (FD_TRUEP(value)) {
    if (delete_tempdirs_on_exit) {
      delete_tempdirs_on_exit=0;
      return 1;}
    else return 0;}
  else if (FD_FALSEP(value)) {
    if (!(delete_tempdirs_on_exit)) {
      delete_tempdirs_on_exit=1;
      return 1;}
    else return 0;}
  else if (!(FD_STRINGP(value))) {
    fd_type_error("string","keeptemp_add",value);
    return -1;}
  else if ((fd_boolstring(FD_STRDATA(value),-1))>=0) {
    int val=fd_boolstring(FD_STRDATA(value),-1);
    if (val) val=0; else val=1;
    if (delete_tempdirs_on_exit==val) return 0;
    else {
      delete_tempdirs_on_exit=val;
      return 1;}}
  else {
    fd_type_error("string","keeptemp_set",value);
    return -1;}
  return 1;
}

/* File time info */

static fdtype make_timestamp(time_t tick)
{
  struct U8_XTIME xt; u8_init_xtime(&xt,tick,u8_second,0,0,0);
  return fd_make_timestamp(&xt);
}

static fdtype file_modtime(fdtype filename)
{
  time_t mtime=u8_file_mtime(FD_STRDATA(filename));
  if (mtime<0) return FD_ERROR_VALUE;
  else return make_timestamp(mtime);
}

static fdtype set_file_modtime(fdtype filename,fdtype timestamp)
{
  time_t mtime=
    ((FD_VOIDP(timestamp))?(time(NULL)):
     (FD_FIXNUMP(timestamp))?(FD_FIX2INT(timestamp)):
     (FD_BIGINTP(timestamp))?(fd_getint(timestamp)):
     (FD_PRIM_TYPEP(timestamp,fd_timestamp_type))?
     (((struct FD_TIMESTAMP *)timestamp)->xtime.u8_tick):
     (-1));
  if (mtime<0)
    return fd_type_error("time","set_file_modtime",timestamp);
  else if (u8_set_mtime(FD_STRDATA(filename),mtime)<0)
    return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static fdtype file_atime(fdtype filename)
{
  time_t mtime=u8_file_atime(FD_STRDATA(filename));
  if (mtime<0) return FD_ERROR_VALUE;
  else return make_timestamp(mtime);
}

static fdtype set_file_atime(fdtype filename,fdtype timestamp)
{
  time_t atime=
    ((FD_VOIDP(timestamp))?(time(NULL)):
     (FD_FIXNUMP(timestamp))?(FD_FIX2INT(timestamp)):
     (FD_BIGINTP(timestamp))?(fd_getint(timestamp)):
     (FD_PRIM_TYPEP(timestamp,fd_timestamp_type))?
     (((struct FD_TIMESTAMP *)timestamp)->xtime.u8_tick):
     (-1));
  if (atime<0)
    return fd_type_error("time","set_file_atime",timestamp);
  else if (u8_set_atime(FD_STRDATA(filename),atime)<0)
    return FD_ERROR_VALUE;
  else return FD_TRUE;
}

static fdtype file_ctime(fdtype filename)
{
  time_t mtime=u8_file_ctime(FD_STRDATA(filename));
  if (mtime<0) return FD_ERROR_VALUE;
  else return make_timestamp(mtime);
}

static fdtype file_mode(fdtype filename)
{
  int mode=u8_file_mode(FD_STRDATA(filename));
  if (mode<0) return FD_ERROR_VALUE;
  else return FD_INT2DTYPE(mode);
}

static fdtype file_size(fdtype filename)
{
  ssize_t size=u8_file_size(FD_STRDATA(filename));
  if (size<0) return FD_ERROR_VALUE;
  else if (size<FD_MAX_FIXNUM)
    return FD_INT2DTYPE(size);
  else return fd_make_bigint(size);
}

static fdtype file_owner(fdtype filename)
{
  u8_string name=u8_file_owner(FD_STRDATA(filename));
  if (name) return fd_lispstring(name);
  else return FD_ERROR_VALUE;
}

/* Current directory information */

static fdtype getcwd_prim()
{
  u8_string wd=u8_getcwd();
  if (wd) return fd_lispstring(wd);
  else return FD_ERROR_VALUE;
}

static fdtype setcwd_prim(fdtype dirname)
{
  if (u8_setcwd(FD_STRDATA(dirname))<0)
    return FD_ERROR_VALUE;
  else return FD_VOID;
}

/* Directory listings */

static fdtype getfiles_prim(fdtype dirname,fdtype fullpath)
{
  fdtype results=FD_EMPTY_CHOICE;
  u8_string *contents=
    u8_getfiles(FD_STRDATA(dirname),(!(FD_FALSEP(fullpath)))), *scan=contents;
  if (contents==NULL) return FD_ERROR_VALUE;
  else while (*scan) {
    fdtype string=fd_lispstring(*scan);
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
  if (contents==NULL) return FD_ERROR_VALUE;
  else while (*scan) {
    fdtype string=fd_lispstring(*scan);
    FD_ADD_TO_CHOICE(results,string);
    scan++;}
  u8_free(contents);
  return results;
}

/* Reading and writing DTYPEs */

static fdtype dtype2file(fdtype object,fdtype filename,fdtype bufsiz)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *out; int bytes;
    u8_string temp_name=u8_mkstring("%s.part",FD_STRDATA(filename));
    out=fd_dtsopen(temp_name,FD_DTSTREAM_CREATE);
    if (out==NULL) return FD_ERROR_VALUE;
    if (FD_FIXNUMP(bufsiz))
      fd_dtsbufsize(out,FD_FIX2INT(bufsiz));
    bytes=fd_dtswrite_dtype(out,object);
    if (bytes<0) {
      fd_dtsclose(out,FD_DTSCLOSE_FULL);
      u8_free(temp_name);
      return FD_ERROR_VALUE;}
    fd_dtsclose(out,FD_DTSCLOSE_FULL);
    u8_movefile(temp_name,FD_STRDATA(filename));
    u8_free(temp_name);
    return FD_INT2DTYPE(bytes);}
  else if (FD_PRIM_TYPEP(filename,fd_dtstream_type)) {
    struct FD_DTSTREAM *out=FD_GET_CONS(filename,fd_dtstream_type,struct FD_DTSTREAM *);
    int bytes=fd_dtswrite_dtype(out->dt_stream,object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT2DTYPE(bytes);}
  else return fd_type_error(_("string"),"dtype2file",filename);
}

static fdtype dtype2zipfile(fdtype object,fdtype filename,fdtype bufsiz)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *out; int bytes;
    u8_string temp_name=u8_mkstring("%s.part",FD_STRDATA(filename));
    out=fd_dtsopen(temp_name,FD_DTSTREAM_CREATE);
    if (out==NULL) return FD_ERROR_VALUE;
    if (FD_FIXNUMP(bufsiz))
      fd_dtsbufsize(out,FD_FIX2INT(bufsiz));
    bytes=fd_zwrite_dtype(out,object);
    if (bytes<0) {
      fd_dtsclose(out,FD_DTSCLOSE_FULL);
      u8_free(temp_name);
      return FD_ERROR_VALUE;}
    fd_dtsclose(out,FD_DTSCLOSE_FULL);
    u8_movefile(temp_name,FD_STRDATA(filename));
    u8_free(temp_name);
    return FD_INT2DTYPE(bytes);}
  else if (FD_PRIM_TYPEP(filename,fd_dtstream_type)) {
    struct FD_DTSTREAM *out=FD_GET_CONS(filename,fd_dtstream_type,struct FD_DTSTREAM *);
    int bytes=fd_zwrite_dtype(out->dt_stream,object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT2DTYPE(bytes);}
  else return fd_type_error(_("string"),"dtype2zipfile",filename);
}

static fdtype add_dtype2file(fdtype object,fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *out; int bytes;
    if (u8_file_existsp(FD_STRDATA(filename)))
      out=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_MODIFY);
    else out=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_CREATE);
    if (out==NULL) return FD_ERROR_VALUE;
    fd_endpos(out);
    bytes=fd_dtswrite_dtype(out,object);
    fd_dtsclose(out,FD_DTSCLOSE_FULL);
    return FD_INT2DTYPE(bytes);}
  else if (FD_PRIM_TYPEP(filename,fd_dtstream_type)) {
    struct FD_DTSTREAM *out=FD_GET_CONS(filename,fd_dtstream_type,struct FD_DTSTREAM *);
    int bytes=fd_dtswrite_dtype(out->dt_stream,object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT2DTYPE(bytes);}
  else return fd_type_error(_("string"),"add_dtype2file",filename);
}

static fdtype add_dtype2zipfile(fdtype object,fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *out; int bytes;
    if (u8_file_existsp(FD_STRDATA(filename)))
      out=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_MODIFY);
    else out=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_CREATE);
    if (out==NULL) return FD_ERROR_VALUE;
    fd_endpos(out);
    bytes=fd_dtswrite_dtype(out,object);
    fd_dtsclose(out,FD_DTSCLOSE_FULL);
    return FD_INT2DTYPE(bytes);}
  else if (FD_PRIM_TYPEP(filename,fd_dtstream_type)) {
    struct FD_DTSTREAM *out=FD_GET_CONS(filename,fd_dtstream_type,struct FD_DTSTREAM *);
    int bytes=fd_dtswrite_dtype(out->dt_stream,object);
    if (bytes<0) return FD_ERROR_VALUE;
    else return FD_INT2DTYPE(bytes);}
  else return fd_type_error(_("string"),"add_dtype2zipfile",filename);
}

static fdtype file2dtype(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *in;
    fdtype object=FD_VOID;
    in=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_READ);
    if (in==NULL) return FD_ERROR_VALUE;
    else object=fd_dtsread_dtype(in);
    fd_dtsclose(in,FD_DTSCLOSE_FULL);
    return object;}
  else if (FD_PRIM_TYPEP(filename,fd_dtstream_type)) {
    struct FD_DTSTREAM *in=FD_GET_CONS(filename,fd_dtstream_type,struct FD_DTSTREAM *);
    fdtype object=fd_dtsread_dtype(in->dt_stream);
    if (object == FD_EOD) return FD_EOF;
    else return object;}
  else return fd_type_error(_("string"),"read_dtype",filename);
}

static fdtype zipfile2dtype(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *in;
    fdtype object=FD_VOID;
    in=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_READ);
    if (in==NULL) return FD_ERROR_VALUE;
    else object=fd_zread_dtype(in);
    fd_dtsclose(in,FD_DTSCLOSE_FULL);
    return object;}
  else if (FD_PRIM_TYPEP(filename,fd_dtstream_type)) {
    struct FD_DTSTREAM *in=
      FD_GET_CONS(filename,fd_dtstream_type,struct FD_DTSTREAM *);
    fdtype object=fd_zread_dtype(in->dt_stream);
    if (object == FD_EOD) return FD_EOF;
    else return object;}
  else return fd_type_error(_("string"),"zipfile2dtype",filename);
}

static fdtype file2dtypes(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *in;
    fdtype results=FD_EMPTY_CHOICE, object=FD_VOID;
    in=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_READ);
    if (in==NULL) return FD_ERROR_VALUE;
    else {
      object=fd_dtsread_dtype(in);
      while (!(FD_EODP(object))) {
        FD_ADD_TO_CHOICE(results,object);
        object=fd_dtsread_dtype(in);}
      fd_dtsclose(in,FD_DTSCLOSE_FULL);
      return results;}}
  else return fd_type_error(_("string"),"file2dtypes",filename);
}

static fdtype zipfile2dtypes(fdtype filename)
{
  if (FD_STRINGP(filename)) {
    struct FD_DTYPE_STREAM *in;
    fdtype results=FD_EMPTY_CHOICE, object=FD_VOID;
    in=fd_dtsopen(FD_STRDATA(filename),FD_DTSTREAM_READ);
    if (in==NULL) return FD_ERROR_VALUE;
    else {
      object=fd_zread_dtype(in);
      while (!(FD_EODP(object))) {
        FD_ADD_TO_CHOICE(results,object);
        object=fd_zread_dtype(in);}
      fd_dtsclose(in,FD_DTSCLOSE_FULL);
      return results;}}
  else return fd_type_error(_("string"),"zipfile2dtypes",filename);;
}

/* Getting file sources */

static u8_string file_source_fn(u8_string filename,u8_string encname,u8_string *abspath,time_t *timep)
{
  u8_string data=u8_filestring(filename,encname);
  if (data) {
    if (abspath) *abspath=u8_abspath(filename,NULL);
    if (timep) *timep=u8_file_mtime(filename);
    return data;}
  else return NULL;
}

/* File flush function */

static fdtype close_prim(fdtype portarg)
{
  if (FD_PRIM_TYPEP(portarg,fd_dtstream_type)) {
    struct FD_DTSTREAM *dts=
      FD_GET_CONS(portarg,fd_dtstream_type,struct FD_DTSTREAM *);
    if (dts->dt_stream) {
      fd_dtsclose(dts->dt_stream,1);
      dts->dt_stream=NULL;}
    return FD_VOID;}
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    U8_OUTPUT *out=p->out; U8_INPUT *in=p->in; int closed=-1;
    if (out) {
      u8_flush(out);
      if (out->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
        U8_XOUTPUT *xout=(U8_XOUTPUT *)out;
        if (xout->fd<0) {}
        else {
          closed=xout->fd; fsync(xout->fd); close(xout->fd);
          xout->fd=-1;}}}
    if (in) {
      u8_flush(out);
      if (in->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
        U8_XINPUT *xin=(U8_XINPUT *)in;
        if (xin->fd<0) { /* already closed. warn? */ }
        else if (closed!=xin->fd) {
          close(xin->fd); xin->fd=-1;}}}
    return FD_VOID;}
  else return fd_type_error("port","close_prim",portarg);
}

static fdtype flush_prim(fdtype portarg)
{
  if (FD_PRIM_TYPEP(portarg,fd_dtstream_type)) {
    struct FD_DTSTREAM *dts=
      FD_GET_CONS(portarg,fd_dtstream_type,struct FD_DTSTREAM *);
    fd_dtsflush(dts->dt_stream);
    return FD_VOID;}
  else {
    U8_OUTPUT *out=get_output_port(portarg);
    u8_flush(out);
    if (out->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
      U8_XOUTPUT *xout=(U8_XOUTPUT *)out;
      fsync(xout->fd);}
    return FD_VOID;}
}

static fdtype setbuf_prim(fdtype portarg,fdtype insize,fdtype outsize)
{
  if (FD_PRIM_TYPEP(portarg,fd_dtstream_type)) {
    struct FD_DTSTREAM *dts=FD_GET_CONS(portarg,fd_dtstream_type,struct FD_DTSTREAM *);
    fd_dtsbufsize(dts->dt_stream,FD_FIX2INT(insize));
    return FD_VOID;}
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    if (FD_FIXNUMP(insize)) {
      U8_INPUT *in=p->in;
      if ((in) && (in->u8_streaminfo&U8_STREAM_OWNS_XBUF)) {
        u8_xinput_setbuf((struct U8_XINPUT *)in,FD_FIX2INT(insize));}}

    if (FD_FIXNUMP(outsize)) {
      U8_OUTPUT *out=p->out;
      if ((out) && (out->u8_streaminfo&U8_STREAM_OWNS_XBUF)) {
        u8_xoutput_setbuf((struct U8_XOUTPUT *)out,FD_FIX2INT(outsize));}}
    return FD_VOID;}
  else return fd_type_error("port/stream","setbuf",portarg);
}

static fdtype getpos_prim(fdtype portarg)
{
  if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    fd_off_t result=-1;
    if (p->in)
      result=u8_getpos((struct U8_STREAM *)(p->in));
    else if (p->out)
      result=u8_getpos((struct U8_STREAM *)(p->out));
    else return fd_type_error(_("port"),"getpos_prim",portarg);
    if (result<0)
      return FD_ERROR_VALUE;
    else if (result<FD_MAX_FIXNUM)
      return FD_INT2DTYPE(result);
    else return fd_make_bigint(result);}
  else if (FD_PRIM_TYPEP(portarg,fd_dtstream_type)) {
    fd_dtstream ds=FD_GET_CONS(portarg,fd_dtstream_type,fd_dtstream);
    fd_off_t pos=fd_getpos(ds->dt_stream);
    if (pos<0) return FD_ERROR_VALUE;
    else if (pos<FD_MAX_FIXNUM) return FD_INT2DTYPE(pos);
    else return fd_make_bigint(pos);}
  else return fd_type_error("port or stream","getpos_prim",portarg);
}

static fdtype endpos_prim(fdtype portarg)
{
  if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    fd_off_t result=-1;
    if (p->in)
      result=u8_endpos((struct U8_STREAM *)(p->in));
    else if (p->out)
      result=u8_endpos((struct U8_STREAM *)(p->out));
    else return fd_type_error(_("port"),"getpos_prim",portarg);
    if (result<0)
      return FD_ERROR_VALUE;
    else if (result<FD_MAX_FIXNUM)
      return FD_INT2DTYPE(result);
    else return fd_make_bigint(result);}
  else if (FD_PRIM_TYPEP(portarg,fd_dtstream_type)) {
    fd_dtstream ds=FD_GET_CONS(portarg,fd_dtstream_type,fd_dtstream);
    fd_off_t pos=fd_endpos(ds->dt_stream);
    if (pos<0) return FD_ERROR_VALUE;
    else if (pos<FD_MAX_FIXNUM) return FD_INT2DTYPE(pos);
    else return fd_make_bigint(pos);}
  else return fd_type_error("port or stream","getpos_prim",portarg);
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
    return FD_ERROR_VALUE;
  else return fd_init_double(NULL,result);
}

static fdtype setpos_prim(fdtype portarg,fdtype off_arg)
{
  if (FD_PORTP(portarg)) {
    fd_off_t off, result;
    struct FD_PORT *p=
      FD_GET_CONS(portarg,fd_port_type,struct FD_PORT *);
    if (FD_FIXNUMP(off_arg)) off=FD_FIX2INT(off_arg);
    else if (FD_BIGINTP(off_arg))
#if (_FILE_OFFSET_BITS==64)
      off=(fd_off_t)fd_bigint_to_long_long((fd_bigint)off_arg);
#else
    off=(fd_off_t)fd_bigint_to_long((fd_bigint)off_arg);
#endif
    else return fd_type_error(_("offset"),"setpos_prim",off_arg);
    if (p->in)
      result=u8_setpos((struct U8_STREAM *)(p->in),off);
    else if (p->out)
      result=u8_setpos((struct U8_STREAM *)(p->out),off);
    else return fd_type_error(_("port"),"getpos",portarg);
    if (result<0)
      return FD_ERROR_VALUE;
    else if (result<FD_MAX_FIXNUM)
      return FD_INT2DTYPE(off);
    else return fd_make_bigint(result);}
  else if (FD_PRIM_TYPEP(portarg,fd_dtstream_type)) {
    fd_dtstream ds=FD_GET_CONS(portarg,fd_dtstream_type,fd_dtstream);
    fd_off_t off, result;
    if (FD_FIXNUMP(off_arg)) off=FD_FIX2INT(off_arg);
    else if (FD_BIGINTP(off_arg))
#if (_FILE_OFFSET_BITS==64)
      off=(fd_off_t)fd_bigint_to_long_long((fd_bigint)off_arg);
#else
    off=(fd_off_t)fd_bigint_to_long((fd_bigint)off_arg);
#endif
    else return fd_type_error(_("offset"),"setpos_prim",off_arg);
    result=fd_setpos(ds->dt_stream,off);
    if (result<0) return FD_ERROR_VALUE;
    else if (result<FD_MAX_FIXNUM) return FD_INT2DTYPE(result);
    else return fd_make_bigint(result);}
  else return fd_type_error("port or stream","getpos_prim",portarg);
}

/* Module finding */

static fdtype safe_loadpath=FD_EMPTY_LIST;
static fdtype loadpath=FD_EMPTY_LIST;
static void add_load_record
  (fdtype spec,u8_string filename,fd_lispenv env,time_t mtime);
static fdtype load_source_for_module
  (fdtype spec,u8_string module_filename,int safe);
static u8_string get_module_filename(fdtype spec,int safe);

static int load_source_module(fdtype spec,int safe)
{
  u8_string module_filename=get_module_filename(spec,safe);
  if (module_filename) {
    fdtype load_result=load_source_for_module(spec,module_filename,safe);
    if (FD_ABORTP(load_result)) {
      u8_free(module_filename); fd_decref(load_result);
      return -1;}
    else {
      fdtype module_key=fdtype_string(module_filename);
      fdtype abspath_key=
        fd_lispstring(u8_abspath(module_filename,NULL));
      /* Register the module under its filename too. */
      fd_register_module_x(module_key,load_result,safe);
      fd_register_module_x(abspath_key,load_result,safe);
      fd_decref(module_key);
      fd_decref(abspath_key);
      u8_free(module_filename);
      fd_decref(load_result);
      return 1;}}
  else return 0;
}

static u8_string get_module_filename(fdtype spec,int safe)
{
  if (FD_SYMBOLP(spec)) {
    u8_string name=u8_downcase(FD_SYMBOL_NAME(spec));
    u8_string module_filename=NULL;
    if (safe==0) {
      FD_DOLIST(elt,loadpath) {
        if (FD_STRINGP(elt)) {
          module_filename=u8_find_file(name,FD_STRDATA(elt),NULL);
          if (module_filename) {
            u8_free(name);
            return module_filename;}}}}
    if (module_filename==NULL)  {
      FD_DOLIST(elt,safe_loadpath) {
        if (FD_STRINGP(elt)) {
          module_filename=u8_find_file(name,FD_STRDATA(elt),NULL);
          if (module_filename) {
            u8_free(name);
            return module_filename;}}}}
    u8_free(name);
    return module_filename;}
  else if ((safe==0) &&
           (FD_STRINGP(spec)) &&
           (u8_file_existsp(FD_STRDATA(spec))))
    return u8_strdup(FD_STRDATA(spec));
  else return NULL;
}

static fdtype load_source_for_module
  (fdtype spec,u8_string module_filename,int safe)
{
  fd_lispenv env=
    ((safe) ?
     (fd_safe_working_environment()) :
     (fd_working_environment()));
  time_t mtime=u8_file_mtime(module_filename);
  fdtype load_result=fd_load_source(module_filename,env,"auto");
  if (FD_ABORTP(load_result)) {
    if (FD_HASHTABLEP(env->bindings))
      fd_reset_hashtable((fd_hashtable)(env->bindings),0,1);
    fd_decref((fdtype)env);
    return load_result;}
  add_load_record(spec,module_filename,env,mtime);
  fd_decref(load_result);
  return (fdtype)env;
}

static fdtype reload_module(fdtype module)
{
  if (FD_SYMBOLP(module)) {
    int retval=load_source_module(module,0);
    if (retval) return FD_TRUE; else return FD_FALSE;}
  else return fd_err(fd_TypeError,"reload_module",NULL,module);
}

static fdtype safe_reload_module(fdtype module)
{
  if (FD_SYMBOLP(module)) {
    int retval=load_source_module(module,0);
    if (retval) return FD_TRUE; else return FD_FALSE;}
  else return fd_err(fd_TypeError,"reload_module",NULL,module);
}

/* Automatic module reloading */

static double reload_interval=-1.0, last_reload=-1.0;

#if FD_THREADS_ENABLED
static u8_mutex load_record_lock;
static u8_mutex update_modules_lock;
#endif

struct FD_LOAD_RECORD {
  u8_string filename; fd_lispenv env; fdtype spec;
  time_t mtime; int reloading:1;
  struct FD_LOAD_RECORD *next;} *load_records=NULL;

static void add_load_record
  (fdtype spec,u8_string filename,fd_lispenv env,time_t mtime)
{
  struct FD_LOAD_RECORD *scan;
  fd_lock_mutex(&load_record_lock);
  scan=load_records; while (scan)
    if ((strcmp(filename,scan->filename))==0) {
      if (env!=scan->env) {
        fd_incref((fdtype)env);
        fd_decref((fdtype)(scan->env));
        scan->env=env;}
      scan->mtime=mtime;
      fd_unlock_mutex(&load_record_lock);
      return;}
    else scan=scan->next;
  scan=u8_alloc(struct FD_LOAD_RECORD);
  scan->filename=u8_strdup(filename);
  scan->mtime=mtime; scan->reloading=0;
  scan->env=(fd_lispenv)fd_incref((fdtype)env);
  scan->spec=fd_incref(spec);
  scan->next=load_records; load_records=scan;
  fd_unlock_mutex(&load_record_lock);
}

typedef struct MODULE_RELOAD {
  u8_string filename; fd_lispenv env; fdtype spec;
  struct FD_LOAD_RECORD *record; time_t mtime;
  struct MODULE_RELOAD *next;} RELOAD_MODULE;
typedef struct MODULE_RELOAD *module_reload;

FD_EXPORT int fd_update_file_modules(int force)
{
  module_reload reloads=NULL, rscan;
  int n_reloads=0;
  if ((force) ||
      ((reload_interval>=0) &&
       ((u8_elapsed_time()-last_reload)>reload_interval))) {
    struct FD_LOAD_RECORD *scan; double reload_time;
    fd_lock_mutex(&update_modules_lock);
    fd_lock_mutex(&load_record_lock);
    reload_time=u8_elapsed_time();
    scan=load_records; while (scan) {
      time_t mtime=u8_file_mtime(scan->filename);
      if (mtime>scan->mtime) {
        struct MODULE_RELOAD *toreload=u8_alloc(struct MODULE_RELOAD);
        toreload->filename=scan->filename; toreload->env=scan->env;
        toreload->spec=scan->spec;
        toreload->record=scan; toreload->mtime=-1;
        toreload->next=reloads;
        rscan=reloads=toreload;
        scan->reloading=1;}
      scan=scan->next;}
    fd_unlock_mutex(&load_record_lock);
    rscan=reloads; while (rscan) {
      module_reload this=rscan; fdtype load_result;
      u8_string filename=this->filename;
      fd_lispenv env=this->env; rscan=this->next;
      time_t mtime=u8_file_mtime(this->filename);
      if (log_reloads)
        u8_log(LOG_WARN,"fd_update_file_modules","Reloading %q from %s",
               this->spec,filename);
      load_result=fd_load_source(filename,env,"auto");
      if (FD_ABORTP(load_result)) {
        u8_log(LOG_CRIT,"update_file_modules","Error reloading %q from %s",
               this->spec,filename);
        fd_clear_errors(1);}
      else {
        if (log_reloads)
          u8_log(LOG_WARN,"fd_update_file_modules","Reloaded %q from %s",
                 this->spec,filename);
        fd_decref(load_result); n_reloads++;
        this->mtime=mtime;}}
    fd_lock_mutex(&load_record_lock);
    rscan=reloads; while (rscan) {
      module_reload this=rscan;
      if (this->mtime>0) {
        this->record->mtime=this->mtime;}
      this->record->reloading=0;
      rscan=this->next;
      u8_free(this);}
    fd_unlock_mutex(&load_record_lock);
    last_reload=reload_time;
    fd_unlock_mutex(&update_modules_lock);}
  return n_reloads;
}

FD_EXPORT int fd_update_file_module(u8_string module_filename,int force)
{
  struct FD_LOAD_RECORD *scan;
  time_t mtime=u8_file_mtime(module_filename);
  fd_lock_mutex(&update_modules_lock);
  fd_lock_mutex(&load_record_lock);
  scan=load_records;
  while (scan)
    if (strcmp(scan->filename,module_filename)==0) break;
    else scan=scan->next;
  if ((scan)&&(scan->reloading)) {
    fd_unlock_mutex(&load_record_lock);
    fd_unlock_mutex(&update_modules_lock);
    return 0;}
  else if (!(scan)) {
    fd_unlock_mutex(&load_record_lock);
    fd_unlock_mutex(&update_modules_lock);
    /* Maybe, this should load it. */
    fd_seterr(fd_ReloadError,"fd_update_file_module",
              u8_mkstring(_("The file %s has never been loaded"),
                          module_filename),
              FD_VOID);
    return -1;}
  else if ((!(force))&&(mtime<=scan->mtime)) {
    fd_unlock_mutex(&load_record_lock);
    fd_unlock_mutex(&update_modules_lock);
    return 0;}
  scan->reloading=1;
  fd_unlock_mutex(&load_record_lock);
  if ((force) || (mtime>scan->mtime)) {
    fdtype load_result;
    if (log_reloads)
      u8_log(LOG_WARN,"fd_update_file_module","Reloading %s",scan->filename);
    load_result=fd_load_source(scan->filename,scan->env,"auto");
    if (FD_ABORTP(load_result)) {
      fd_seterr(fd_ReloadError,"fd_reload_modules",
                u8_strdup(scan->filename),load_result);
      fd_lock_mutex(&load_record_lock);
      scan->reloading=0;
      fd_unlock_mutex(&load_record_lock);
      fd_unlock_mutex(&update_modules_lock);
      return -1;}
    else {
      fd_decref(load_result);
      fd_lock_mutex(&load_record_lock);
      scan->mtime=mtime; scan->reloading=0;
      fd_unlock_mutex(&load_record_lock);
      fd_unlock_mutex(&update_modules_lock);
      return 1;}}
  else return 0;
}

static fdtype update_modules_prim(fdtype flag)
{
  if (fd_update_file_modules((!FD_FALSEP(flag)))<0)
    return FD_ERROR_VALUE;
  else return FD_VOID;
}

static fdtype update_module_prim(fdtype spec,fdtype force)
{
  if (FD_FALSEP(force)) {
    u8_string module_filename=get_module_filename(spec,0);
    if (module_filename) {
      int retval=fd_update_file_module(module_filename,0);
      if (retval) return FD_TRUE;
      else return FD_FALSE;}
    else {
      fd_seterr(fd_ReloadError,"update_module_prim",
                u8_strdup(_("Module does not exist")),
                spec);
      return FD_ERROR_VALUE;}}
  else return load_source_module(spec,0);
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
  else if (FD_FIXNUMP(val)) {
    reload_interval=fd_getint(val);
    return 1;}
  else if (FD_FALSEP(val)) {
    reload_interval=-1.0;
    return 1;}
  else if (FD_TRUEP(val)) {
    reload_interval=0.25;
    return 1;}
  else {
    fd_seterr(fd_TypeError,"updatemodules_config_set",NULL,val);
    return -1;}
}

/* Latest source functions */

static fdtype source_symbol;

static fdtype get_entry(fdtype key,fdtype entries)
{
  fdtype entry=FD_EMPTY_CHOICE;
  FD_DO_CHOICES(each,entries)
    if (!(FD_PAIRP(each))) {}
    else if (FDTYPE_EQUAL(key,FD_CAR(each))) {
      entry=each; FD_STOP_DO_CHOICES; break;}
    else {}
  return entry;
}

FD_EXPORT int fd_load_latest(u8_string filename,fd_lispenv env,u8_string base)
{
  if (filename==NULL) {
    int loads=0; fd_lispenv scan=env; fdtype result=FD_VOID;
    while (scan) {
      fdtype sources=fd_get(scan->bindings,source_symbol,FD_EMPTY_CHOICE);
      FD_DO_CHOICES(entry,sources) {
        struct FD_TIMESTAMP *loadstamp=
          FD_GET_CONS(FD_CDR(entry),fd_timestamp_type,struct FD_TIMESTAMP *);
        time_t mod_time=u8_file_mtime(FD_STRDATA(FD_CAR(entry)));
        if (mod_time>loadstamp->xtime.u8_tick) {
          struct FD_PAIR *pair=(struct FD_PAIR *)entry;
          struct FD_TIMESTAMP *tstamp=u8_alloc(struct FD_TIMESTAMP);
          FD_INIT_CONS(tstamp,fd_timestamp_type);
          u8_init_xtime(&(tstamp->xtime),mod_time,u8_second,0,0,0);
          fd_decref(pair->cdr);
          pair->cdr=FDTYPE_CONS(tstamp);
          if (log_reloads)
            u8_log(LOG_WARN,"fd_load_latest","Reloading %s",
                   FD_STRDATA(FD_CAR(entry)));
          result=fd_load_source(FD_STRDATA(FD_CAR(entry)),scan,"auto");
          if (FD_ABORTP(result)) {
            fd_decref(sources);
            return fd_interr(result);}
          else fd_decref(result);
          loads++;}}
      scan=scan->parent;}
    return loads;}
  else {
    u8_string abspath=u8_abspath(filename,base);
    fdtype abspath_dtype=fdtype_string(abspath);
    fdtype sources=fd_get(env->bindings,source_symbol,FD_EMPTY_CHOICE);
    fdtype entry=get_entry(abspath_dtype,sources);
    fdtype result=FD_VOID;
    if (FD_PAIRP(entry))
      if (FD_PTR_TYPEP(FD_CDR(entry),fd_timestamp_type)) {
        struct FD_TIMESTAMP *curstamp=
          FD_GET_CONS(FD_CDR(entry),fd_timestamp_type,struct FD_TIMESTAMP *);
        time_t last_loaded=curstamp->xtime.u8_tick;
        time_t mod_time=u8_file_mtime(FD_STRDATA(abspath_dtype));
        if (mod_time<=last_loaded) return 0;
        else {
          struct FD_PAIR *pair=(struct FD_PAIR *)entry;
          struct FD_TIMESTAMP *tstamp=u8_alloc(struct FD_TIMESTAMP);
          FD_INIT_CONS(tstamp,fd_timestamp_type);
          u8_init_xtime(&(tstamp->xtime),mod_time,u8_second,0,0,0);
          fd_decref(pair->cdr);
          pair->cdr=FDTYPE_CONS(tstamp);}}
      else {
        fd_seterr("Invalid load_latest record","load_latest",abspath,entry);
        fd_decref(sources); fd_decref(abspath_dtype);
        return -1;}
    else {
      time_t mod_time=u8_file_mtime(abspath);
      struct FD_TIMESTAMP *tstamp=u8_alloc(struct FD_TIMESTAMP);
      FD_INIT_CONS(tstamp,fd_timestamp_type);
      u8_init_xtime(&(tstamp->xtime),mod_time,u8_second,0,0,0);
      entry=fd_init_pair(NULL,fd_incref(abspath_dtype),FDTYPE_CONS(tstamp));
      if (FD_EMPTY_CHOICEP(sources)) fd_bind_value(source_symbol,entry,env);
      else fd_add_value(source_symbol,entry,env);}
    if (log_reloads)
      u8_log(LOG_WARN,"fd_load_latest","Reloading %s",abspath);
    result=fd_load_source(abspath,env,"auto");
    u8_free(abspath);
    fd_decref(abspath_dtype);
    fd_decref(sources);
    if (FD_ABORTP(result))
      return fd_interr(result);
    else fd_decref(result);
    return 1;}
}

static fdtype load_latest(fdtype expr,fd_lispenv env)
{
  if (FD_EMPTY_LISTP(FD_CDR(expr))) {
    int loads=fd_load_latest(NULL,env,NULL);
    return FD_INT2DTYPE(loads);}
  else {
    int retval=-1;
    fdtype path_expr=fd_get_arg(expr,1);
    fdtype path=fd_eval(path_expr,env);
    if (!(FD_STRINGP(path)))
      return fd_type_error("pathname","load_latest",path);
    else retval=fd_load_latest(FD_STRDATA(path),env,NULL);
    if (retval<0) return FD_ERROR_VALUE;
    else if (retval) {
      fd_decref(path); return FD_TRUE;}
    else {
      fd_decref(path); return FD_FALSE;}}
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
    fdtype slotmap=(fdtype)fd_empty_slotmap();
    if (FD_VOIDP(vars)) vars=FD_EMPTY_CHOICE;
    if (FD_VOIDP(configvars)) configvars=FD_EMPTY_CHOICE;
    {FD_DO_CHOICES(sym,vars)
       if (FD_SYMBOLP(sym)) {
         fdtype val=fd_symeval(sym,env);
         if (FD_VOIDP(val))
           u8_log(LOG_WARN,SnapshotTrouble,"The snapshot variable %q is unbound",sym);
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
           u8_log(LOG_WARN,SnapshotTrouble,"The snapshot config %q is not set",
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
    u8_log(LOG_INFO,SnapshotSaved,"Saved snapshot of %d items to %s",
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
  u8_log(LOG_INFO,SnapshotRestored,"Restored snapshot of %d items from %s",
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
  if (retval<0) return FD_ERROR_VALUE;
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
  if (retval<0) return FD_ERROR_VALUE;
  else return FD_INT2DTYPE(retval);
}

/* Stackdump configuration */

static u8_string stackdump_filename=NULL;
static FILE *stackdump_file=NULL;

#if FD_THREADS_ENABLED
static u8_mutex stackdump_lock;
#endif

FD_EXPORT void stackdump_dump(u8_string dump)
{
  struct U8_XTIME now;
  struct U8_OUTPUT out;
  u8_now(&now);
  u8_lock_mutex(&stackdump_lock);
  if (stackdump_file==NULL) {
    if (stackdump_filename==NULL) {
      u8_unlock_mutex(&stackdump_lock);
      return;}
    else {
      stackdump_file=fopen(stackdump_filename,"w");
      if (stackdump_file==NULL) {
        u8_log(LOG_CRIT,StackDumpEvent,"Couldn't open stackdump file %s",stackdump_filename);
        u8_free(stackdump_filename);
        stackdump_filename=NULL;
        u8_unlock_mutex(&stackdump_lock);
        return;}
      else {
        u8_log(LOG_NOTIFY,StackDumpEvent,"Opened stackdump file %s",stackdump_filename);}}}
  U8_INIT_OUTPUT(&out,256);
  u8_printf(&out,"pid=%d elapsed=%f (%*liUGt)",getpid(),u8_elapsed_time());
  fprintf(stackdump_file,">>>>>> %s\n",out.u8_outbuf);
  fprintf(stackdump_file,">>>>>> %s\n",u8_sessionid());
  fprintf(stackdump_file,">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
  fputs(dump,stackdump_file);
  fprintf(stackdump_file,"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
  fflush(stackdump_file);
  u8_free(out.u8_outbuf);
  u8_unlock_mutex(&stackdump_lock);
}

static fdtype stackdump_config_get(fdtype var,void *ignored)
{
  if (stackdump_filename)
    return fdtype_string(stackdump_filename);
  else return FD_FALSE;
}
static int stackdump_config_set(fdtype var,fdtype val,void *ignored)
{
  if ((FD_STRINGP(val)) || (FD_FALSEP(val)) || (FD_TRUEP(val))) {
    u8_string filename=((FD_STRINGP(val)) ? (FD_STRDATA(val)) :
                        (FD_TRUEP(val)) ? ((u8_string)"stackdump.log") : (NULL));
    if (stackdump_filename==filename) return 0;
    else if ((stackdump_filename) && (filename) &&
             (strcmp(stackdump_filename,filename)==0))
      return 0;
    u8_lock_mutex(&stackdump_lock);
    if (stackdump_file) {
      fclose(stackdump_file);
      stackdump_file=NULL;
      u8_free(stackdump_filename);
      stackdump_filename=NULL;}
    if (filename) {
      stackdump_filename=u8_strdup(filename);
      fd_dump_backtrace=stackdump_dump;}
    u8_unlock_mutex(&stackdump_lock);
    return 1;}
  else {
    fd_seterr(fd_TypeError,"stackdump_config_set",NULL,val);
    return -1;}
}

/* The init function */

static int scheme_fileio_initialized=0;

FD_EXPORT void fd_init_filedb_c(void);

FD_EXPORT void fd_init_fileio_c()
{
  fdtype fileio_module;
  if (scheme_fileio_initialized) return;
  scheme_fileio_initialized=1;
  fd_init_fdscheme();
  fileio_module=fd_new_module("FILEIO",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);


#if FD_THREADS_ENABLED
  fd_init_mutex(&load_record_lock);
  fd_init_mutex(&update_modules_lock);
  fd_init_mutex(&stackdump_lock);
  fd_init_mutex(&tempdirs_lock);
#endif

  u8_init_xinput(&u8stdin,0,NULL);
  u8_init_xoutput(&u8stdout,1,NULL);
  u8_init_xoutput(&u8stderr,2,NULL);

  u8_set_global_output((u8_output)&u8stdout);

  fd_idefn(fileio_module,
           fd_make_cprim3("OPEN-OUTPUT-FILE",open_output_file,1));
  fd_idefn(fileio_module,
           fd_make_cprim3("EXTEND-OUTPUT-FILE",extend_output_file,1));
  fd_idefn(fileio_module,
           fd_make_cprim2("OPEN-INPUT-FILE",open_input_file,1));
  fd_idefn(fileio_module,fd_make_cprim3x("SETBUF",setbuf_prim,2,
                                         -1,FD_VOID,-1,FD_FALSE,-1,FD_FALSE));

  fd_idefn(fileio_module,
           fd_make_cprim1x("OPEN-DTYPE-FILE",open_dtype_file,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("EXTEND-DTYPE-FILE",extend_dtype_file,1,
                           fd_string_type,FD_VOID));

  fd_idefn(fileio_module,
           fd_make_cprim3x("WRITE-FILE",writefile_prim,2,
                           fd_string_type,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID));

  fd_defspecial(fileio_module,"FILEOUT",simple_fileout);

  fd_defspecial(fileio_module,"SYSTEM",simple_system);

  fd_idefn(fileio_module,fd_make_cprim1("EXIT",exit_prim,0));
  fd_idefn(fileio_module,fd_make_cprimn("EXEC",exec_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("EXEC/PATH",execpath_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FORK",fork_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FORK/PATH",forkpath_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FDEXEC",fdexec_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FDFORK",fdfork_prim,1));
#if HAVE_WAITPID
  fd_idefn(fileio_module,fd_make_cprimn("FORKWAIT",forkwait_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FORKLOOKUPWAIT",forklookupwait_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FDFORKWAIT",fdforkwait_prim,1));
#endif

  fd_idefn(fileio_module,fd_make_cprim1("PID?",ispid_prim,1));
  fd_idefn(fileio_module,fd_make_cprim2("PID/KILL",pid_kill_prim,2));

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
           fd_make_cprim2x("REMOVE-TREE",remove_tree_prim,1,
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
           fd_make_cprim1x("PATH-DIRNAME",path_dirname,1,
                           fd_string_type,FD_VOID));
  fd_defalias(fileio_module,"DIRNAME","PATH-DIRNAME");

  fd_idefn(fileio_module,
           fd_make_cprim2x("PATH-BASENAME",path_basename,1,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID));
  fd_defalias(fileio_module,"BASENAME","PATH-BASENAME");

  fd_idefn(fileio_module,
           fd_make_cprim1x("PATH-LOCATION",path_location,1,fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("PATH-SUFFIX",path_suffix,1,
                           fd_string_type,FD_VOID,-1,FD_VOID));

  fd_idefn(fileio_module,
           fd_make_cprim2x("ABSPATH",file_abspath,1,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("REALPATH",file_realpath,1,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("MKPATH",mkpath_prim,2,
                           -1,FD_VOID,fd_string_type,FD_VOID));

  fd_idefn(fileio_module,
           fd_make_cprim2x("MKDIR",mkdir_prim,1,
                           fd_string_type,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("RMDIR",rmdir_prim,1,fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("MKDIRS",mkdirs_prim,1,
                           fd_string_type,FD_VOID,fd_fixnum_type,FD_VOID));

#if 0
  fd_idefn(fileio_module,
           fd_make_cprim1x("MKTEMP",mktemp_prim,1,fd_string_type,FD_VOID));
#endif
  fd_idefn(fileio_module,
           fd_make_cprim2x("TEMPDIR",tempdir_prim,0,
                           fd_string_type,FD_VOID,
                           -1,FD_FALSE));
  fd_idefn(fileio_module,
           fd_make_cprim1x("TEMPDIR?",is_tempdir_prim,0,
                           fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("TEMPDIR/DONE",tempdir_done_prim,0,
                           fd_string_type,FD_VOID,
                           -1,FD_FALSE));
  fd_idefn(fileio_module,
           fd_make_cprim1x("RUNFILE",runfile_prim,1,
                           fd_string_type,FD_VOID));

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
           fd_make_cprim2x("SET-FILE-MODTIME!",set_file_modtime,1,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("SET-FILE-ATIME!",set_file_atime,1,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID));

  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-OWNER",file_owner,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-MODE",file_mode,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-SIZE",file_size,1,
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
           fd_make_ndprim(fd_make_cprim3("DTYPE->FILE",dtype2file,2)));
  fd_idefn(fileio_module,
           fd_make_ndprim(fd_make_cprim2("DTYPE->FILE+",add_dtype2file,2)));
  fd_idefn(fileio_module,
           fd_make_ndprim(fd_make_cprim3("DTYPE->ZFILE",dtype2zipfile,2)));
  fd_idefn(fileio_module,
           fd_make_ndprim(fd_make_cprim2("DTYPE->ZFILE+",add_dtype2zipfile,2)));
  /* We make these aliases because the output file isn't really a zip
     file, but we don't want to break code which uses the old
     names. */
  fd_defalias(fileio_module,"DTYPE->ZIPFILE","DTYPE->ZFILE");
  fd_defalias(fileio_module,"DTYPE->ZIPFILE+","DTYPE->ZFILE+");
  fd_idefn(fileio_module,
           fd_make_cprim1("FILE->DTYPE",file2dtype,1));
  fd_idefn(fileio_module,
           fd_make_cprim1("FILE->DTYPES",file2dtypes,1));
  fd_idefn(fileio_module,fd_make_cprim1("ZFILE->DTYPE",zipfile2dtype,1));
  fd_idefn(fileio_module,fd_make_cprim1("ZFILE->DTYPES",zipfile2dtypes,1));
  fd_defalias(fileio_module,"ZIPFILE->DTYPE","ZFILE->DTYPE");
  fd_defalias(fileio_module,"ZIPFILE->DTYPES","ZFILE->DTYPES");

  fd_idefn(fd_scheme_module,fd_make_cprim1("FLUSH-OUTPUT",flush_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CLOSE",close_prim,0));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("GETPOS",getpos_prim,1,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SETPOS",setpos_prim,2,-1,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ENDPOS",endpos_prim,1,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("FILE%",file_progress_prim,1,fd_port_type,FD_VOID));

  fd_defspecial(fileio_module,"LOAD-LATEST",load_latest);

  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x
           ("OPEN-SOCKET",open_socket_prim,1,
            fd_string_type,FD_VOID,-1,FD_VOID));

  fd_init_filedb_c();

  fd_add_module_loader(load_source_module);
  fd_register_config
    ("UPDATEMODULES","Modules to update automatically on UPDATEMODULES",
                     updatemodules_config_get,updatemodules_config_set,NULL);
  fd_register_config
    ("LOADPATH","Directories/URIs to search for modules (not sandbox)",
                     fd_lconfig_get,fd_lconfig_push,&loadpath);
  fd_register_config
    ("SAFELOADPATH","Directories/URIs to search for sandbox modules",
     fd_lconfig_get,fd_lconfig_push,&safe_loadpath);

  fd_register_config
    ("TEMPROOT","Template for generating temporary directory names",
     temproot_get,temproot_set,NULL);
  fd_register_config
    ("TEMPDIRS","Declared temporary directories to be deleted on exit",
     tempdirs_get,tempdirs_add,NULL);
  fd_register_config
    ("KEEPDIRS","Temporary directories to not be deleted on exit",
     keepdirs_get,keepdirs_add,NULL);
  fd_register_config
    ("KEEPTEMP","Temporary directories to not be deleted on exit",
     keeptemp_get,keeptemp_set,NULL);
  fd_register_config
    ("LOGRELOADS","Whether or not to log module/file reloads",
     fd_boolconfig_get,fd_boolconfig_set,&log_reloads);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1("RELOAD-MODULE",safe_reload_module,1));
  fd_idefn(fileio_module,
           fd_make_cprim1("RELOAD-MODULE",reload_module,1));
  fd_idefn(fileio_module,
           fd_make_cprim1("UPDATE-MODULES",update_modules_prim,0));
  fd_idefn(fileio_module,
           fd_make_cprim2x("UPDATE-MODULE",update_module_prim,1,
                           -1,FD_VOID,-1,FD_FALSE));

  atexit(remove_tempdirs);

  {
    u8_string path=u8_getenv("FD_LOADPATH");
    fdtype v=((path) ? (fd_lispstring(path)) :
              (fdtype_string(FD_DEFAULT_LOADPATH)));
    fd_config_set("LOADPATH",v);
    fd_decref(v);}
  {
    u8_string path=u8_getenv("FD_SAFE_LOADPATH");
    fdtype v=((path) ? (fd_lispstring(path)) :
              (fdtype_string(FD_DEFAULT_SAFE_LOADPATH)));
    fd_config_set("SAFELOADPATH",v);
    fd_decref(v);}

  source_symbol=fd_intern("%SOURCE");

  snapshotvars=fd_intern("%SNAPVARS");
  snapshotconfig=fd_intern("%SNAPCONFIG");
  snapshotfile=fd_intern("%SNAPSHOTFILE");
  configinfo=fd_intern("%CONFIGINFO");

  noblock_symbol=fd_intern("NOBLOCK");
  nodelay_symbol=fd_intern("NODELAY");

  fd_defspecial(fileio_module,"SNAPSHOT",snapshot_handler);
  fd_defspecial(fileio_module,"SNAPBACK",snapback_handler);

  fd_register_config
    ("STACKDUMP","File to store stackdump information on errors",
     stackdump_config_get,stackdump_config_set,NULL);

  fd_finish_module(fileio_module);

  fd_persist_module(fileio_module);

  fd_register_sourcefn(file_source_fn);
}

FD_EXPORT void fd_init_schemeio()
{
  fd_init_fileio_c();
  fd_init_filedb_c();
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
