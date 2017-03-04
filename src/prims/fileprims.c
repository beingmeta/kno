/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
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
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/ports.h"
#include "framerd/numbers.h"
#include "framerd/fileprims.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8netfns.h>
#include <libu8/u8xfiles.h>

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

#if ((HAVE_SYS_VFS_H)&&(HAVE_STATFS))
#include <sys/vfs.h>
#elif ((HAVE_SYS_FSTAT_H)&&(HAVE_STATFS))
#include <sys/statfs.h>
#endif

static U8_XINPUT u8stdin;
static U8_XOUTPUT u8stdout;
static U8_XOUTPUT u8stderr;

static fd_exception NoSuchFile=_("file does not exist");
static fd_exception RemoveFailed=_("File removal failed");
static fd_exception LinkFailed=_("File link failed");
static fd_exception OpenFailed=_("File open failed");

static u8_condition StackDumpEvent=_("StackDump");
static u8_condition SnapshotSaved=_("Snapshot Saved");
static u8_condition SnapshotRestored=_("Snapshot Restored");

/* Making ports */

static fdtype make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct FD_PORT *port=u8_alloc(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->fd_inport=in;
  port->fd_outport=out;
  port->fd_portid=id;
  return FDTYPE_CONS(port);
}

static u8_output get_output_port(fdtype portarg)
{
  if ((FD_VOIDP(portarg))||(portarg==FD_TRUE))
    return u8_current_output;
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->fd_outport;}
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
    f->u8_xescape=escape;}
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
    f->u8_xescape=escape;}
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

static fdtype writefile_prim(fdtype filename,fdtype object,fdtype enc)
{
  int len=0; const unsigned char *bytes; int free_bytes=0;
  if (FD_STRINGP(object)) {
    bytes=FD_STRDATA(object); len=FD_STRLEN(object);}
  else if (FD_PACKETP(object)) {
    bytes=FD_PACKET_DATA(object); len=FD_PACKET_LENGTH(object);}
  else if ((FD_FALSEP(enc)) || (FD_VOIDP(enc))) {
    struct FD_OUTBUF out;
    FD_INIT_BYTE_OUTBUF(&out,1024);
    fd_write_dtype(&out,object);
    bytes=out.buffer; len=out.bufwrite-out.buffer;
    free_bytes=1;}
  else {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
    fd_unparse(&out,object);
    bytes=out.u8_outbuf; len=out.u8_write-out.u8_outbuf;
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
  return FD_INT(len);
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
    FD_PORT *port=fd_consptr(FD_PORT *,filename_val,fd_port_type);
    if (port->fd_outport) {f=port->fd_outport; doclose=0;}
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
  {fdtype body=fd_get_body(expr,2);
    FD_DOLIST(ex,body)  {
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
  {fdtype string_exprs=fd_get_body(expr,1);
    FD_DOLIST(string_expr,string_exprs) {
      fdtype value=fasteval(string_expr,env);
      if (FD_ABORTP(value)) return value;
      else if (FD_VOIDP(value)) continue;
      else if (FD_STRINGP(value))
        u8_printf(&out,"%s",FD_STRDATA(value));
      else u8_printf(&out,"%q",value);
      fd_decref(value);}}
  result=system(out.u8_outbuf); u8_free(out.u8_outbuf);
  return FD_INT(result);
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
    u8_byte *filename=NULL;
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
        return FD_INT(retval);}
#endif
      return FD_INT(pid);}
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

static fdtype exec_cmd_prim(int n,fdtype *args)
{
  return exec_helper("exec_cmd_prim",FD_DO_LOOKUP,n,args);
}

static fdtype fdexec_prim(int n,fdtype *args)
{
  return exec_helper("fdexec_prim",FD_IS_SCHEME,n,args);
}

static fdtype fork_prim(int n,fdtype *args)
{
  return exec_helper("fork_prim",FD_DO_FORK,n,args);
}

static fdtype fork_cmd_prim(int n,fdtype *args)
{
  return exec_helper("fork_cmd_prim",(FD_DO_FORK|FD_DO_LOOKUP),n,args);
}

static fdtype fork_wait_prim(int n,fdtype *args)
{
  return exec_helper("fork_wait_prim",(FD_DO_FORK|FD_DO_WAIT),n,args);
}

static fdtype fork_cmd_wait_prim(int n,fdtype *args)
{
  return exec_helper("fork_cmd_wait_prim",
                     (FD_DO_FORK|FD_DO_LOOKUP|FD_DO_WAIT),n,args);
}

static fdtype fdfork_wait_prim(int n,fdtype *args)
{
  return exec_helper("fdfork_wait_prim",
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
  if ((u8_file_existsp(filename))||(u8_symlinkp(filename))) {
    if (u8_removefile(FD_STRDATA(arg))<0) {
      fdtype err=fd_err(RemoveFailed,"remove_file_prim",filename,arg);
      return err;}
    else return FD_TRUE;}
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

static fdtype filecontent_prim(fdtype filename)
{
  int len=-1;
  unsigned char *data=u8_filedata(FD_STRDATA(filename),&len);
  if (len>=0) {
    fdtype result;
    if (u8_validp(data)) 
      result=fd_make_string(NULL,len,data);
    else result=fd_make_packet(NULL,len,data);
    u8_free(data);
    return result;}
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

static fdtype file_readlink(fdtype arg,fdtype abs,fdtype err)
{
  u8_string result;
  if (FD_FALSEP(abs))
    result=u8_readlink(FD_STRDATA(arg),0);
  else result=u8_readlink(FD_STRDATA(arg),1);
  if (result) return fd_lispstring(result);
  else if (FD_TRUEP(err)) 
    return FD_ERROR_VALUE;
  else {
    u8_clear_errors(0);
    return FD_FALSE;}
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
  u8_string path=FD_STRDATA(arg);
  u8_string slash=strrchr(path,'/');
  if (slash[1]=='\0') return fd_incref(arg);
  else return fd_substring(path,slash+1);
}

static fdtype mkpath_prim(fdtype dirname,fdtype name)
{
  fdtype config_val=FD_VOID; u8_string dir=NULL, namestring=NULL;
  if (!(FD_STRINGP(name)))
    return fd_type_error(_("string"),"mkpath_prim",name);
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
      return fd_type_error(_("string CONFIG var"),"mkpath_prim",dirname);}}
  else return fd_type_error
         (_("string or string CONFIG var"),"mkpath_prim",dirname);
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
  else return FD_INT(retval);
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
  if (tempdir_template)
    return fdtype_string(tempdir_template);
  else {
    char *tmpdir=get_tmpdir();
    u8_string tmp=u8_mkpath(tmpdir,"fdtempXXXXXX");
    fdtype result=fdtype_string(tmp);
    u8_free(tmp);
    return result;}
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
      u8_lock_mutex(&tempdirs_lock);
      FD_ADD_TO_CHOICE(tempdirs,result);
      u8_unlock_mutex(&tempdirs_lock);}
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
        if (u8_directoryp(FD_STRDATA(tmpfile)))
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
  if ((FD_VOIDP(template_arg))||
      (FD_FALSEP(template_arg))||
      (FD_SYMBOLP(template_arg))||
      (FD_STRINGP(template_arg))) {
    u8_string dirname=tempdir_core(template_arg,(!(FD_FALSEP(keep))));
    if (dirname==NULL) return FD_ERROR_VALUE;
    else return fd_init_string(NULL,-1,dirname);}
  return fd_type_error("tempdir template (string, symbol, or #f)",
                       "tempdir_prim",
                       template_arg);
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
    else return FD_INT(retval);}
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
  u8_lock_mutex(&tempdirs_lock);
  FD_ADD_TO_CHOICE(tempdirs,value);
  u8_unlock_mutex(&tempdirs_lock);
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
  u8_lock_mutex(&tempdirs_lock);
  FD_ADD_TO_CHOICE(keeptemp,value);
  u8_unlock_mutex(&tempdirs_lock);
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
     (FD_TYPEP(timestamp,fd_timestamp_type))?
     (((struct FD_TIMESTAMP *)timestamp)->fd_u8xtime.u8_tick):
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
     (FD_TYPEP(timestamp,fd_timestamp_type))?
     (((struct FD_TIMESTAMP *)timestamp)->fd_u8xtime.u8_tick):
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
  else return FD_INT(mode);
}

static fdtype file_size(fdtype filename)
{
  ssize_t size=u8_file_size(FD_STRDATA(filename));
  if (size<0) return FD_ERROR_VALUE;
  else if (size<FD_MAX_FIXNUM)
    return FD_INT(size);
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

static fdtype getlinks_prim(fdtype dirname,fdtype fullpath)
{
  fdtype results=FD_EMPTY_CHOICE;
  u8_string *contents=
    u8_readdir(FD_STRDATA(dirname),U8_LIST_LINKS,(!(FD_FALSEP(fullpath)))), 
    *scan=contents;
  if (contents==NULL) return FD_ERROR_VALUE;
  else while (*scan) {
    fdtype string=fd_lispstring(*scan);
    FD_ADD_TO_CHOICE(results,string);
    scan++;}
  u8_free(contents);
  return results;
}

static fdtype readdir_prim(fdtype dirname,fdtype fullpath)
{
  fdtype results=FD_EMPTY_CHOICE;
  u8_string *contents=
    u8_readdir(FD_STRDATA(dirname),0,(!(FD_FALSEP(fullpath)))), 
    *scan=contents;
  if (contents==NULL) return FD_ERROR_VALUE;
  else while (*scan) {
    fdtype string=fd_lispstring(*scan);
    FD_ADD_TO_CHOICE(results,string);
    scan++;}
  u8_free(contents);
  return results;
}

/* File flush function */

static fdtype close_prim(fdtype portarg)
{
  if (FD_TYPEP(portarg,fd_stream_type)) {
    struct FD_STREAM *dts=
      fd_consptr(struct FD_STREAM *,portarg,fd_stream_type);
    fd_close_stream(dts,0);
    return FD_VOID;}
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    U8_OUTPUT *out=p->fd_outport; U8_INPUT *in=p->fd_inport; int closed=-1;
    if (out) {
      u8_flush(out);
      if (out->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
        U8_XOUTPUT *xout=(U8_XOUTPUT *)out;
        if (xout->u8_xfd<0) {}
        else {
          closed=xout->u8_xfd; fsync(xout->u8_xfd); close(xout->u8_xfd);
          xout->u8_xfd=-1;}}}
    if (in) {
      u8_flush(out);
      if (in->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
        U8_XINPUT *xin=(U8_XINPUT *)in;
        if (xin->u8_xfd<0) { /* already closed. warn? */ }
        else if (closed!=xin->u8_xfd) {
          close(xin->u8_xfd); xin->u8_xfd=-1;}}}
    return FD_VOID;}
  else return fd_type_error("port","close_prim",portarg);
}

static fdtype flush_prim(fdtype portarg)
{
  if ((FD_VOIDP(portarg))||
      (FD_FALSEP(portarg))||
      (FD_TRUEP(portarg))) {
    u8_flush_xoutput(&u8stdout);
    u8_flush_xoutput(&u8stderr);
    return FD_VOID;}
  else if (FD_TYPEP(portarg,fd_stream_type)) {
    struct FD_STREAM *dts=
      fd_consptr(struct FD_STREAM *,portarg,fd_stream_type);
    fd_flush_stream(dts);
    return FD_VOID;}
  else if (FD_TYPEP(portarg,fd_port_type)) {
    U8_OUTPUT *out=get_output_port(portarg);
    u8_flush(out);
    if (out->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
      U8_XOUTPUT *xout=(U8_XOUTPUT *)out;
      fsync(xout->u8_xfd);}
    return FD_VOID;}
  else return fd_type_error(_("port or stream"),"flush_prim",portarg);
}

static fdtype setbuf_prim(fdtype portarg,fdtype insize,fdtype outsize)
{
  if (FD_TYPEP(portarg,fd_stream_type)) {
    struct FD_STREAM *dts=
      fd_consptr(struct FD_STREAM *,portarg,fd_stream_type);
    fd_stream_setbuf(dts,FD_FIX2INT(insize));
    return FD_VOID;}
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    if (FD_FIXNUMP(insize)) {
      U8_INPUT *in=p->fd_inport;
      if ((in) && (in->u8_streaminfo&U8_STREAM_OWNS_XBUF)) {
        u8_xinput_setbuf((struct U8_XINPUT *)in,FD_FIX2INT(insize));}}

    if (FD_FIXNUMP(outsize)) {
      U8_OUTPUT *out=p->fd_outport;
      if ((out) && (out->u8_streaminfo&U8_STREAM_OWNS_XBUF)) {
        u8_xoutput_setbuf((struct U8_XOUTPUT *)out,FD_FIX2INT(outsize));}}
    return FD_VOID;}
  else return fd_type_error("port/stream","setbuf_prim",portarg);
}

static fdtype getpos_prim(fdtype portarg)
{
  if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    fd_off_t result=-1;
    if (p->fd_inport)
      result=u8_getpos((struct U8_STREAM *)(p->fd_inport));
    else if (p->fd_outport)
      result=u8_getpos((struct U8_STREAM *)(p->fd_outport));
    else return fd_type_error(_("port"),"getpos_prim",portarg);
    if (result<0)
      return FD_ERROR_VALUE;
    else if (result<FD_MAX_FIXNUM)
      return FD_INT(result);
    else return fd_make_bigint(result);}
  else if (FD_TYPEP(portarg,fd_stream_type)) {
    fd_stream ds=fd_consptr(fd_stream,portarg,fd_stream_type);
    fd_off_t pos=fd_getpos(ds);
    if (pos<0) return FD_ERROR_VALUE;
    else if (pos<FD_MAX_FIXNUM) return FD_INT(pos);
    else return fd_make_bigint(pos);}
  else return fd_type_error("port or stream","getpos_prim",portarg);
}

static fdtype endpos_prim(fdtype portarg)
{
  if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    fd_off_t result=-1;
    if (p->fd_inport)
      result=u8_endpos((struct U8_STREAM *)(p->fd_inport));
    else if (p->fd_outport)
      result=u8_endpos((struct U8_STREAM *)(p->fd_outport));
    else return fd_type_error(_("port"),"getpos_prim",portarg);
    if (result<0)
      return FD_ERROR_VALUE;
    else if (result<FD_MAX_FIXNUM)
      return FD_INT(result);
    else return fd_make_bigint(result);}
  else if (FD_TYPEP(portarg,fd_stream_type)) {
    fd_stream ds=fd_consptr(fd_stream,portarg,fd_stream_type);
    fd_off_t pos=fd_endpos(ds);
    if (pos<0) return FD_ERROR_VALUE;
    else if (pos<FD_MAX_FIXNUM) return FD_INT(pos);
    else return fd_make_bigint(pos);}
  else return fd_type_error("port or stream","endpos_prim",portarg);
}

static fdtype file_progress_prim(fdtype portarg)
{
  double result=-1.0;
  struct FD_PORT *p=
    fd_consptr(struct FD_PORT *,portarg,fd_port_type);
  if (p->fd_inport)
    result=u8_getprogress((struct U8_STREAM *)(p->fd_inport));
  else if (p->fd_outport)
    result=u8_getprogress((struct U8_STREAM *)(p->fd_outport));
  else return fd_type_error(_("port"),"file_progress_prim",portarg);
  if (result<0)
    return FD_ERROR_VALUE;
  else return fd_init_double(NULL,result);
}

static fdtype setpos_prim(fdtype portarg,fdtype off_arg)
{
  if (FD_PORTP(portarg)) {
    fd_off_t off, result;
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    if (FD_FIXNUMP(off_arg)) off=FD_FIX2INT(off_arg);
    else if (FD_BIGINTP(off_arg))
#if (_FILE_OFFSET_BITS==64)
      off=(fd_off_t)fd_bigint_to_long_long((fd_bigint)off_arg);
#else
    off=(fd_off_t)fd_bigint_to_long((fd_bigint)off_arg);
#endif
    else return fd_type_error(_("offset"),"setpos_prim",off_arg);
    if (p->fd_inport)
      result=u8_setpos((struct U8_STREAM *)(p->fd_inport),off);
    else if (p->fd_outport)
      result=u8_setpos((struct U8_STREAM *)(p->fd_outport),off);
    else return fd_type_error(_("port"),"setpos_prim",portarg);
    if (result<0)
      return FD_ERROR_VALUE;
    else if (result<FD_MAX_FIXNUM)
      return FD_INT(off);
    else return fd_make_bigint(result);}
  else if (FD_TYPEP(portarg,fd_stream_type)) {
    fd_stream ds=fd_consptr(fd_stream,portarg,fd_stream_type);
    fd_off_t off, result;
    if (FD_FIXNUMP(off_arg)) off=FD_FIX2INT(off_arg);
    else if (FD_BIGINTP(off_arg))
#if (_FILE_OFFSET_BITS==64)
      off=(fd_off_t)fd_bigint_to_long_long((fd_bigint)off_arg);
#else
    off=(fd_off_t)fd_bigint_to_long((fd_bigint)off_arg);
#endif
    else return fd_type_error(_("offset"),"setpos_prim",off_arg);
    result=fd_setpos(ds,off);
    if (result<0) return FD_ERROR_VALUE;
    else if (result<FD_MAX_FIXNUM) return FD_INT(result);
    else return fd_make_bigint(result);}
  else return fd_type_error("port or stream","setpos_prim",portarg);
}

/* File system info */

#if (((HAVE_SYS_STATFS_H)||(HAVE_SYS_VSTAT_H))&&(HAVE_STATFS))
static void statfs_set(fdtype,u8_string,long long int,long long int);

static fdtype fsinfo_prim(fdtype arg)
{
  u8_string path=FD_STRDATA(arg); struct statfs info;
  char *usepath; int retval;
  if (u8_file_existsp(path))
    usepath=u8_tolibc(path);
  else {
    u8_string abs=u8_abspath(path,NULL);
    u8_string dir=u8_dirname(abs);
    usepath=u8_tolibc(dir);
    u8_free(abs); u8_free(dir);}
  retval=statfs(usepath,&info);
  if (((char *)path)!=usepath) u8_free(usepath);
  if (retval) {
    u8_graberr(-1,"fsinfo_prim",u8_strdup(path));
    return FD_ERROR_VALUE;}
  else {
    fdtype result=fd_init_slotmap(NULL,0,NULL);
    statfs_set(result,"BLOCKSIZE",info.f_bsize,1);
    statfs_set(result,"TOTAL-BLOCKS",info.f_blocks,1);
    statfs_set(result,"FREE-BLOCKS",info.f_bfree,1);
    statfs_set(result,"AVAILABLE-BLOCKS",info.f_bavail,1);
    statfs_set(result,"USED-BLOCKS",info.f_blocks-info.f_bavail,1);
    statfs_set(result,"TOTAL",info.f_blocks,info.f_bsize);
    statfs_set(result,"FREE",info.f_bfree,info.f_bsize);
    statfs_set(result,"AVAILABLE",info.f_bavail,info.f_bsize);
    statfs_set(result,"USED",info.f_blocks-info.f_bavail,info.f_bsize);
    statfs_set(result,"FSTYPE",info.f_type,1);
    statfs_set(result,"FSID",info.f_type,1);
    statfs_set(result,"NAMELEN",info.f_namelen,1);
    return result;}
}
static void statfs_set(fdtype r,u8_string name,
                       long long int val,long long int mul)
{
  fdtype slotid=fd_intern(name);
  fdtype lval=FD_INT2DTYPE(val);
  if (mul!=1) {
    fdtype mulval=FD_INT2DTYPE(mul);
    fdtype rval=fd_multiply(lval,mulval);
    fd_decref(mulval); fd_decref(lval);
    lval=rval;}
  fd_store(r,slotid,lval);
  fd_decref(lval);
}
#else
static fdtype fsinfo_prim(fdtype arg)
{
  return fd_err("statfs unavailable","fsinfo_prim",NULL,FD_VOID);
}
#endif

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
    struct FD_STREAM *out; int bytes;
    fdtype slotmap=(fdtype)fd_empty_slotmap();
    if (FD_VOIDP(vars)) vars=FD_EMPTY_CHOICE;
    if (FD_VOIDP(configvars)) configvars=FD_EMPTY_CHOICE;
    {FD_DO_CHOICES(sym,vars)
       if (FD_SYMBOLP(sym)) {
         fdtype val=fd_symeval(sym,env);
         if (FD_VOIDP(val))
           u8_log(LOG_WARN,SnapshotTrouble,
                  "The snapshot variable %q is unbound",sym);
         else fd_add(slotmap,sym,val);
         fd_decref(val);}
       else {
         fd_decref(slotmap);
         return fd_type_error("symbol","fd_snapshot",sym);}}
    {FD_DO_CHOICES(sym,configvars)
       if (FD_SYMBOLP(sym)) {
         fdtype val=fd_config_get(FD_SYMBOL_NAME(sym));
         fdtype config_entry=fd_conspair(sym,val);
         if (FD_VOIDP(val))
           u8_log(LOG_WARN,SnapshotTrouble,
                  "The snapshot config %q is not set",sym);
         else fd_add(slotmap,configinfo,config_entry);
         fd_decref(val);}
       else {
         fd_decref(slotmap);
         return fd_type_error("symbol","fd_snapshot",sym);}}
    out=fd_open_file(filename,FD_FILE_CREATE);
    if (out==NULL) {
      fd_decref(slotmap);
      return -1;}
    else bytes=fd_write_dtype(fd_writebuf(out),slotmap);
    fd_close_stream(out,FD_STREAM_CLOSE_FULL);
    u8_log(LOG_INFO,SnapshotSaved,
           "Saved snapshot of %d items to %s",
           FD_SLOTMAP_NUSED(slotmap),filename);
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
  struct FD_STREAM *in;
  fdtype slotmap; int actions=0;
  in=fd_open_file(filename,FD_FILE_READ);
  if (in==NULL) return -1;
  else slotmap=fd_read_dtype(fd_readbuf(in));
  if (FD_ABORTP(slotmap)) {
    fd_close_stream(in,FD_STREAM_CLOSE_FULL);
    return slotmap;}
  else if (FD_SLOTMAPP(slotmap)) {
    fdtype keys=fd_getkeys(slotmap);
    FD_DO_CHOICES(key,keys) {
      fdtype v=fd_get(slotmap,key,FD_VOID);
      if (FD_EQ(key,configinfo)) {
        FD_DO_CHOICES(config_entry,v)
          if ((FD_PAIRP(config_entry)) &&
              (FD_SYMBOLP(FD_CAR(config_entry))))
            if (fd_set_config(FD_SYMBOL_NAME(FD_CAR(config_entry)),
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
         FD_SLOTMAP_NUSED(slotmap),filename);
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
  else return FD_INT(retval);
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
  else return FD_INT(retval);
}

/* Stackdump configuration */

static u8_string stackdump_filename=NULL;
static FILE *stackdump_file=NULL;
static u8_mutex stackdump_lock;

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

FD_EXPORT void fd_init_driverfns_c(void);
FD_EXPORT void fd_init_loader_c(void);

FD_EXPORT void fd_init_fileio_c()
{
  fdtype fileio_module;
  if (scheme_fileio_initialized) return;
  scheme_fileio_initialized=1;
  fd_init_fdscheme();
  fileio_module=fd_new_module("FILEIO",(FD_MODULE_DEFAULT));
  u8_register_source_file(_FILEINFO);


  u8_init_mutex(&stackdump_lock);
  u8_init_mutex(&tempdirs_lock);

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
  fd_idefn(fileio_module,fd_make_cprim3x("SETBUF!",setbuf_prim,2,
                                         -1,FD_VOID,-1,FD_FALSE,-1,FD_FALSE));
  fd_defalias(fileio_module,"SETBUF","SETBUF!");

  fd_idefn(fileio_module,
           fd_make_cprim3x("WRITE-FILE",writefile_prim,2,
                           fd_string_type,FD_VOID,-1,FD_VOID,
                           -1,FD_VOID));

  fd_defspecial(fileio_module,"FILEOUT",simple_fileout);

  fd_defspecial(fileio_module,"SYSTEM",simple_system);

  fd_idefn(fileio_module,fd_make_cprim1("EXIT",exit_prim,0));
  fd_idefn(fileio_module,fd_make_cprimn("EXEC",exec_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("EXEC/CMD",exec_cmd_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FORK",fork_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FORK/CMD",fork_cmd_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FDEXEC",fdexec_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FDFORK",fdfork_prim,1));
#if HAVE_WAITPID
  fd_idefn(fileio_module,fd_make_cprimn("FORK/WAIT",fork_wait_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FORK/CMD/WAIT",fork_cmd_wait_prim,1));
  fd_idefn(fileio_module,fd_make_cprimn("FDFORK/WAIT",fdfork_wait_prim,1));
#endif

  fd_idefn(fileio_module,fd_make_cprim1("PID?",ispid_prim,1));
  fd_idefn(fileio_module,fd_make_cprim2("PID/KILL!",pid_kill_prim,2));
  fd_defalias(fileio_module,"PID/KILL","PID/KILL!");

  fd_idefn(fileio_module,
           fd_make_cprim2x("REMOVE-FILE!",remove_file_prim,1,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID));
  fd_defalias(fileio_module,"REMOVE-FILE","REMOVE-FILE!");
  fd_idefn(fileio_module,
           fd_make_cprim3x("MOVE-FILE!",move_file_prim,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID));
  fd_defalias(fileio_module,"MOVE-FILE","MOVE-FILE!");
  fd_idefn(fileio_module,
           fd_make_cprim3x("LINK-FILE!",link_file_prim,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID));
  fd_defalias(fileio_module,"LINK-FILE","LINK-FILE!");
  fd_idefn(fileio_module,
           fd_make_cprim2x("REMOVE-TREE!",remove_tree_prim,1,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID));
  fd_defalias(fileio_module,"REMOVE-TREE","REMOVE-TREE!");


  fd_idefn(fileio_module,
           fd_make_cprim2x("FILESTRING",filestring_prim,1,
                           fd_string_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILEDATA",filedata_prim,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILECONTENT",filecontent_prim,1,
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
           fd_make_cprim3x("READLINK",file_readlink,1,
                           fd_string_type,FD_VOID,-1,FD_VOID,-1,FD_VOID));


  fd_idefn(fileio_module,
           fd_make_cprim1x("FSINFO",fsinfo_prim,1,
                           fd_string_type,FD_VOID));
  fd_defalias(fileio_module,"STATFS","FSINFO");

  fd_idefn(fileio_module,
           fd_make_cprim2x("MKDIR",mkdir_prim,1,
                           fd_string_type,FD_VOID,fd_fixnum_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("RMDIR",rmdir_prim,1,fd_string_type,FD_VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("MKDIRS",mkdirs_prim,1,
                           fd_string_type,FD_VOID,fd_fixnum_type,FD_VOID));

  fd_idefn(fileio_module,
           fd_make_cprim2x("TEMPDIR",tempdir_prim,0,
                           -1,FD_VOID,-1,FD_FALSE));
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
  fd_idefn(fileio_module,
           fd_make_cprim2x("GETLINKS",getlinks_prim,1,
                           fd_string_type,FD_VOID,-1,FD_TRUE));
  fd_idefn(fileio_module,
           fd_make_cprim2x("READDIR",readdir_prim,1,
                           fd_string_type,FD_VOID,-1,FD_TRUE));

  fd_idefn(fileio_module,fd_make_cprim0("GETCWD",getcwd_prim,0));
  fd_idefn(fileio_module,
           fd_make_cprim1x("SETCWD",setcwd_prim,1,fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim1("FLUSH-OUTPUT",flush_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CLOSE",close_prim,0));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("GETPOS",getpos_prim,1,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SETPOS!",setpos_prim,2,-1,FD_VOID,-1,FD_VOID));
  fd_defalias(fd_scheme_module,"SETPOS","SETPOS!");
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ENDPOS",endpos_prim,1,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("FILE%",file_progress_prim,1,fd_port_type,FD_VOID));

  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x
           ("OPEN-SOCKET",open_socket_prim,1,
            fd_string_type,FD_VOID,-1,FD_VOID));

  fd_init_driverfns_c();

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

  atexit(remove_tempdirs);

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
}

FD_EXPORT void fd_init_schemeio()
{
  fd_init_fileio_c();
  fd_init_driverfns_c();
  fd_init_loader_c();
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
