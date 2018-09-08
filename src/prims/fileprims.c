/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/apply.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/eval.h"
#include "framerd/ports.h"
#include "framerd/fileprims.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8netfns.h>
#include <libu8/u8xfiles.h>

#define fast_eval(x,env) (_fd_fast_eval(x,env,_stack,0))

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

static u8_condition RemoveFailed=_("File removal failed");
static u8_condition LinkFailed=_("File link failed");
static u8_condition OpenFailed=_("File open failed");

static u8_condition StackDumpEvent=_("StackDump");
static u8_condition SnapshotSaved=_("Snapshot Saved");
static u8_condition SnapshotRestored=_("Snapshot Restored");

/* Making ports */

static lispval make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct FD_PORT *port = u8_alloc(struct FD_PORT);
  FD_INIT_CONS(port,fd_port_type);
  port->port_input = in;
  port->port_output = out;
  port->port_id = id;
  return LISP_CONS(port);
}

static u8_output get_output_port(lispval portarg)
{
  if ((VOIDP(portarg))||(portarg == FD_TRUE))
    return u8_current_output;
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    return p->port_output;}
  else return NULL;
}

/* Opening files */

static lispval open_output_file(lispval fname,lispval opts,lispval escape_char)
{
  struct U8_XOUTPUT *f;
  u8_string filename = fd_strdata(fname);
  lispval encid = FD_VOID;
  u8_encoding enc = NULL;
  int open_flags = O_EXCL|O_CREAT|O_WRONLY;
  if (FD_OPTIONSP(opts)) {
    encid = fd_getopt(opts,FDSYM_ENCODING,FD_VOID);}
  else if ( (FD_SYMBOLP(opts)) || (FD_STRINGP(opts)) )
    encid = opts;
  else NO_ELSE;
  if (VOIDP(encid)) enc = NULL;
  else if (STRINGP(encid))
    enc = u8_get_encoding(CSTRING(encid));
  else if (SYMBOLP(encid))
    enc = u8_get_encoding(SYM_NAME(encid));
  else return fd_err(fd_UnknownEncoding,"OPEN-OUTPUT-FILE",NULL,encid);
  f = u8_open_output_file(filename,enc,open_flags,0);
  if (encid != opts) fd_decref(encid);
  if (f == NULL)
    return fd_err(u8_CantOpenFile,"OPEN-OUTPUT-FILE",NULL,fname);
  if (FD_CHARACTERP(escape_char)) {
    int escape = FD_CHAR2CODE(escape_char);
    f->u8_xescape = escape;}
  return make_port(NULL,(u8_output)f,u8_strdup(filename));
}

static lispval extend_output_file(lispval fname,lispval opts,lispval escape_char)
{
  struct U8_XOUTPUT *f;
  u8_string filename = fd_strdata(fname);
  lispval encid = FD_VOID;
  u8_encoding enc = NULL;
  int open_flags = O_APPEND|O_CREAT|O_WRONLY;
  if (FD_OPTIONSP(opts)) {
    encid = fd_getopt(opts,FDSYM_ENCODING,FD_VOID);}
  else if ( (FD_SYMBOLP(opts)) || (FD_STRINGP(opts)) )
    encid = opts;
  else NO_ELSE;
  if (VOIDP(encid))
    enc = NULL;
  else if (STRINGP(encid))
    enc = u8_get_encoding(CSTRING(encid));
  else if (SYMBOLP(encid))
    enc = u8_get_encoding(SYM_NAME(encid));
  else return fd_err(fd_UnknownEncoding,"EXTEND-OUTPUT-FILE",NULL,encid);
  f = u8_open_output_file(filename,enc,open_flags,0);
  if (f == NULL)
    return fd_err(u8_CantOpenFile,"EXTEND-OUTPUT-FILE",NULL,fname);
  if (FD_CHARACTERP(escape_char)) {
    int escape = FD_CHAR2CODE(escape_char);
    f->u8_xescape = escape;}
  return make_port(NULL,(u8_output)f,u8_strdup(filename));
}

static lispval open_input_file(lispval fname,lispval opts)
{
  struct U8_XINPUT *f;
  u8_string filename = fd_strdata(fname);
  lispval encid = FD_VOID;
  u8_encoding enc = NULL;
  int open_flags = O_RDONLY;
  if (FD_OPTIONSP(opts)) {
    encid = fd_getopt(opts,FDSYM_ENCODING,FD_VOID);}
  else if ( (FD_SYMBOLP(opts)) || (FD_STRINGP(opts)) )
    encid = opts;
  else NO_ELSE;
  if (VOIDP(encid))
    enc = NULL;
  else if (STRINGP(encid))
    enc = u8_get_encoding(CSTRING(encid));
  else if (SYMBOLP(encid))
    enc = u8_get_encoding(SYM_NAME(encid));
  else return fd_err(fd_UnknownEncoding,"OPEN-INPUT_FILE",NULL,encid);
  f = u8_open_input_file(filename,enc,open_flags,0);
  if (f == NULL)
    return fd_err(u8_CantOpenFile,"OPEN-INPUT-FILE",NULL,fname);
  else return make_port((u8_input)f,NULL,u8_strdup(filename));
}

static lispval writefile_prim(lispval filename,lispval object,lispval enc)
{
  int len = 0; const unsigned char *bytes; int free_bytes = 0;
  if (STRINGP(object)) {
    bytes = CSTRING(object);
    len = STRLEN(object);}
  else if (PACKETP(object)) {
    bytes = FD_PACKET_DATA(object);
    len = FD_PACKET_LENGTH(object);}
  else if ((FALSEP(enc)) || (VOIDP(enc))) {
    struct FD_OUTBUF out = { 0 };
    FD_INIT_BYTE_OUTPUT(&out,1024);
    fd_write_dtype(&out,object);
    bytes = out.buffer;
    len = out.bufwrite-out.buffer;
    free_bytes = 1;}
  else {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
    fd_unparse(&out,object);
    bytes = out.u8_outbuf; len = out.u8_write-out.u8_outbuf;
    free_bytes = 1;}
  if ((FALSEP(enc)) || (VOIDP(enc))) {
    FILE *f = u8_fopen(CSTRING(filename),"w");
    size_t off = 0, to_write = len;
    if (f == NULL) {
      if (free_bytes) u8_free(bytes);
      return fd_err(OpenFailed,"writefile_prim",NULL,filename);}
    while (to_write>0) {
      ssize_t n_bytes = fwrite(bytes+off,1,to_write,f);
      if (n_bytes<0) {
        u8_graberr(errno,"writefile_prim",u8_strdup(CSTRING(filename)));
        return FD_ERROR;}
      else {
        to_write = to_write-n_bytes;
        off = off+n_bytes;}}
    fclose(f);}
  else if ((FD_TRUEP(enc)) || (STRINGP(enc))) {
    struct U8_TEXT_ENCODING *encoding=
      ((STRINGP(enc)) ? (u8_get_encoding(CSTRING(enc))) :
       (u8_get_default_encoding()));
    if (encoding == NULL) {
      if (free_bytes) u8_free(bytes);
      return fd_type_error("encoding","writefile_prim",enc);}
    else {
      U8_XOUTPUT *out = u8_open_output_file(CSTRING(filename),encoding,-1,-1);
      if (out == NULL) {
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

static int printout_helper(U8_OUTPUT *out,lispval x)
{
  if (FD_ABORTP(x)) return 0;
  else if (VOIDP(x)) return 1;
  if (out == NULL) out = u8_current_output;
  if (STRINGP(x))
    u8_printf(out,"%s",CSTRING(x));
  else u8_printf(out,"%q",x);
  return 1;
}

static lispval simple_fileout_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval filename_arg = fd_get_arg(expr,1);
  lispval filename_val = fd_eval(filename_arg,env);
  U8_OUTPUT *f, *oldf; int doclose;
  if (FD_ABORTP(filename_val)) return filename_val;
  else if (FD_PORTP(filename_val)) {
    FD_PORT *port = fd_consptr(FD_PORT *,filename_val,fd_port_type);
    if (port->port_output) {
      f = port->port_output;
      doclose = 0;}
    else {
      fd_decref(filename_val);
      return fd_type_error(_("output port"),"simple_fileout",filename_val);}}
  else if (STRINGP(filename_val)) {
    f = (u8_output)u8_open_output_file(CSTRING(filename_val),NULL,0,0);
    if (f == NULL) {
      fd_decref(filename_val);
      return fd_err(u8_CantOpenFile,"FILEOUT",NULL,filename_val);}
    doclose = 1;}
  else {
    fd_decref(filename_val);
    return fd_type_error(_("string"),"simple_fileout",filename_val);}
  oldf = u8_current_output;
  u8_set_default_output(f);
  {lispval body = fd_get_body(expr,2);
    FD_DOLIST(ex,body)  {
      lispval value = fast_eval(ex,env);
      if (printout_helper(f,value)) fd_decref(value);
      else {
        u8_set_default_output(oldf);
        fd_decref(filename_val);
        return value;}}}
  if (oldf) u8_set_default_output(oldf);
  if (doclose) u8_close_output(f);
  else u8_flush(f);
  fd_decref(filename_val);
  return VOID;
}

/* Not really I/O but related structurally and logically */

static lispval simple_system_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  struct U8_OUTPUT out; int result;
  U8_INIT_OUTPUT(&out,256);
  {lispval string_exprs = fd_get_body(expr,1);
    FD_DOLIST(string_expr,string_exprs) {
      lispval value = fast_eval(string_expr,env);
      if (FD_ABORTP(value)) return value;
      else if (VOIDP(value)) continue;
      else if (STRINGP(value))
        u8_printf(&out,"%s",CSTRING(value));
      else u8_printf(&out,"%q",value);
      fd_decref(value);}}
  result = system(out.u8_outbuf); u8_free(out.u8_outbuf);
  return FD_INT(result);
}

/* Opening TCP sockets */

static lispval noblock_symbol, nodelay_symbol;

static lispval open_socket_prim(lispval spec,lispval opts)
{

  u8_socket conn = u8_connect(CSTRING(spec));
  if (conn<0) return FD_ERROR;
  else {
    lispval noblock = fd_getopt(opts,noblock_symbol,FD_FALSE);
    lispval nodelay = fd_getopt(opts,nodelay_symbol,FD_FALSE);
    u8_xinput in = u8_open_xinput(conn,NULL);
    u8_xoutput out = u8_open_xoutput(conn,NULL);
    if ( (in == NULL) || (out == NULL) ) {
      close(conn);
      return FD_ERROR_VALUE;}
    if (!(FALSEP(noblock))) u8_set_blocking(conn,0);
    if (!(FALSEP(nodelay))) u8_set_nodelay(conn,1);
    return make_port((u8_input)in,(u8_output)out,u8_strdup(CSTRING(spec)));}
}

/* More file manipulation */

static lispval remove_file_prim(lispval arg,lispval must_exist)
{
  u8_string filename = CSTRING(arg);
  if ((u8_file_existsp(filename))||(u8_symlinkp(filename))) {
    if (u8_removefile(CSTRING(arg))<0) {
      lispval err = fd_err(RemoveFailed,"remove_file_prim",filename,arg);
      return err;}
    else return FD_TRUE;}
  else if (FD_TRUEP(must_exist)) {
    u8_string absolute = u8_abspath(filename,NULL);
    lispval err = fd_err(fd_NoSuchFile,"remove_file_prim",absolute,arg);
    u8_free(absolute);
    return err;}
  else return FD_FALSE;
}

static lispval remove_tree_prim(lispval arg,lispval must_exist)
{
  u8_string filename = CSTRING(arg);
  if (u8_directoryp(filename))
    if (u8_rmtree(CSTRING(arg))<0) {
      lispval err = fd_err(RemoveFailed,"remove_tree_prim",filename,arg);
      return err;}
    else return FD_TRUE;
  else if (FD_TRUEP(must_exist)) {
    u8_string absolute = u8_abspath(filename,NULL);
    lispval err = fd_err(fd_NoSuchFile,"remove_tree_prim",absolute,arg);
    u8_free(absolute);
    return err;}
  else return FD_FALSE;
}

static lispval move_file_prim(lispval from,lispval to,lispval must_exist)
{
  u8_string fromname = CSTRING(from);
  if (u8_file_existsp(fromname))
    if (u8_movefile(CSTRING(from),CSTRING(to))<0) {
      lispval err = fd_err(RemoveFailed,"move_file_prim",fromname,to);
      return err;}
    else return FD_TRUE;
  else if (FD_TRUEP(must_exist))
    return fd_err(fd_NoSuchFile,"move_file_prim",NULL,from);
  else return FD_FALSE;
}

static lispval link_file_prim(lispval from,lispval to,lispval must_exist)
{
  u8_string fromname = CSTRING(from);
  if (u8_file_existsp(fromname))
    if (u8_linkfile(CSTRING(from),CSTRING(to))<0) {
      lispval err = fd_err(LinkFailed,"link_file_prim",fromname,to);
      return err;}
    else return FD_TRUE;
  else if (FD_TRUEP(must_exist))
    return fd_err(fd_NoSuchFile,"link_file_prim",NULL,from);
  else return FD_FALSE;
}

/* FILESTRING */

static lispval filestring_prim(lispval filename,lispval enc)
{
  if ((VOIDP(enc))||(FALSEP(enc))) {
    u8_string data = u8_filestring(CSTRING(filename),"UTF-8");
    if (data)
      return fd_lispstring(data);
    else return FD_ERROR;}
  else if (FD_TRUEP(enc)) {
    u8_string data = u8_filestring(CSTRING(filename),"auto");
    if (data) return fd_lispstring(data);
    else return FD_ERROR;}
  else if (STRINGP(enc)) {
    u8_string data = u8_filestring(CSTRING(filename),CSTRING(enc));
    if (data)
      return fd_lispstring(data);
    else return FD_ERROR;}
  else return fd_err(fd_UnknownEncoding,"FILESTRING",NULL,enc);
}

static lispval filedata_prim(lispval filename)
{
  int len = -1;
  unsigned char *data = u8_filedata(CSTRING(filename),&len);
  if (len>=0) return fd_init_packet(NULL,len,data);
  else return FD_ERROR;
}

static lispval filecontent_prim(lispval filename)
{
  int len = -1;
  unsigned char *data = u8_filedata(CSTRING(filename),&len);
  if (len>=0) {
    lispval result;
    if (u8_validp(data))
      result = fd_make_string(NULL,len,data);
    else result = fd_make_packet(NULL,len,data);
    u8_free(data);
    return result;}
  else return FD_ERROR;
}

/* File information */

static lispval file_existsp(lispval arg)
{
  if (u8_file_existsp(CSTRING(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval file_regularp(lispval arg)
{
  if (! (u8_file_existsp(CSTRING(arg))) )
    return FD_FALSE;
  else if (u8_directoryp(CSTRING(arg)))
    return FD_FALSE;
  else if (u8_socketp(CSTRING(arg)))
    return FD_FALSE;
  else return FD_TRUE;
}

static lispval file_readablep(lispval arg)
{
  if (u8_file_readablep(CSTRING(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval file_writablep(lispval arg)
{
  if (u8_file_writablep(CSTRING(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval file_directoryp(lispval arg)
{
  if (u8_directoryp(CSTRING(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval file_symlinkp(lispval arg)
{
  if (u8_symlinkp(CSTRING(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval file_socketp(lispval arg)
{
  if (u8_socketp(CSTRING(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval file_abspath(lispval arg,lispval wd)
{
  u8_string result;
  if (VOIDP(wd))
    result = u8_abspath(CSTRING(arg),NULL);
  else result = u8_abspath(CSTRING(arg),CSTRING(wd));
  if (result) return fd_lispstring(result);
  else return FD_ERROR;
}

static lispval file_realpath(lispval arg,lispval wd)
{
  u8_string result;
  if (VOIDP(wd))
    result = u8_realpath(CSTRING(arg),NULL);
  else result = u8_realpath(CSTRING(arg),CSTRING(wd));
  if (result) return fd_lispstring(result);
  else return FD_ERROR;
}

static lispval file_readlink(lispval arg,lispval abs,lispval err)
{
  u8_string result;
  if (FALSEP(abs))
    result = u8_getlink(CSTRING(arg),0);
  else result = u8_getlink(CSTRING(arg),1);
  if (result) return fd_lispstring(result);
  else if (FD_TRUEP(err))
    return FD_ERROR;
  else {
    u8_clear_errors(0);
    return FD_FALSE;}
}

static lispval path_basename(lispval arg,lispval suffix)
{
  if ((VOIDP(suffix)) || (FALSEP(suffix)))
    return fd_lispstring(u8_basename(CSTRING(arg),NULL));
  else if (STRINGP(suffix))
    return fd_lispstring(u8_basename(CSTRING(arg),CSTRING(suffix)));
  else return fd_lispstring(u8_basename(CSTRING(arg),"*"));
}

static lispval path_suffix(lispval arg,lispval dflt)
{
  u8_string s = CSTRING(arg);
  u8_string slash = strrchr(s,'/');
  u8_string dot = strrchr(s,'.');
  if ((dot)&&((!(slash))||(slash<dot)))
    return lispval_string(dot);
  else if (VOIDP(dflt))
    return lispval_string("");
  else return fd_incref(dflt);
}

static lispval path_dirname(lispval arg)
{
  return fd_lispstring(u8_dirname(CSTRING(arg)));
}

static lispval path_location(lispval arg)
{
  u8_string path = CSTRING(arg);
  u8_string slash = strrchr(path,'/');
  if (slash[1]=='\0') return fd_incref(arg);
  else return fd_substring(path,slash+1);
}

static lispval mkpath_prim(lispval dirname,lispval name)
{
  lispval config_val = VOID; u8_string dir = NULL, namestring = NULL;
  if (!(STRINGP(name)))
    return fd_type_error(_("string"),"mkpath_prim",name);
  else namestring = CSTRING(name);
  if (*namestring=='/') return fd_incref(name);
  else if ((STRINGP(dirname))&&(STRLEN(dirname)==0))
    return fd_incref(name);
  else if (STRINGP(dirname)) dir = CSTRING(dirname);
  else if (SYMBOLP(dirname)) {
    config_val = fd_config_get(SYM_NAME(dirname));
    if (STRINGP(config_val)) dir = CSTRING(config_val);
    else {
      fd_decref(config_val);
      return fd_type_error(_("string CONFIG var"),"mkpath_prim",dirname);}}
  else return fd_type_error
         (_("string or string CONFIG var"),"mkpath_prim",dirname);
  if (VOIDP(config_val))
    return fd_lispstring(u8_mkpath(dir,namestring));
  else {
    lispval result = fd_lispstring(u8_mkpath(dir,namestring));
    fd_decref(config_val);
    return result;}
}

/* Getting the runbase for a script file */

static lispval runfile_prim(lispval suffix)
{
  return fd_lispstring(fd_runbase_filename(CSTRING(suffix)));
}

/* Making directories */

static lispval mkdir_prim(lispval dirname,lispval mode_arg)
{
  mode_t mode=
    ((FD_UINTP(mode_arg))?((mode_t)(FIX2INT(mode_arg))):((mode_t)0777));
  int retval = u8_mkdir(CSTRING(dirname),mode);
  if (retval<0) {
    u8_condition cond = u8_strerror(errno); errno = 0;
    return fd_err(cond,"mkdir_prim",NULL,dirname);}
  else if (retval) {
    /* Force the mode to be set if provided */
    if (FD_UINTP(mode_arg))
      u8_chmod(CSTRING(dirname),((mode_t)(FIX2INT(mode_arg))));
    return FD_TRUE;}
  else return FD_FALSE;
}

static lispval rmdir_prim(lispval dirname)
{
  int retval = u8_rmdir(CSTRING(dirname));
  if (retval<0) {
    u8_condition cond = u8_strerror(errno); errno = 0;
    return fd_err(cond,"rmdir_prim",NULL,dirname);}
  else if (retval) return FD_TRUE;
  else return FD_FALSE;
}

static lispval mkdirs_prim(lispval pathname,lispval mode_arg)
{
  mode_t mode=
    ((FD_UINTP(mode_arg))?((mode_t)(FIX2INT(mode_arg))):((mode_t)-1));
  int retval = u8_mkdirs(CSTRING(pathname),mode);
  if (retval<0) {
    u8_condition cond = u8_strerror(errno); errno = 0;
    return fd_err(cond,"mkdirs_prim",NULL,pathname);}
  else if (retval==0) return FD_FALSE;
  else return FD_INT(retval);
}

/* Temporary directories */

static u8_string tempdir_template = NULL;

static char *get_tmpdir()
{
  char *tmpdir = getenv("TMPDIR");
  if (tmpdir) return tmpdir;
  tmpdir = getenv("TMP_DIR");
  if (tmpdir) return tmpdir;
  else return "/tmp";
}

static lispval temproot_get(lispval sym,void *ignore)
{
  if (tempdir_template)
    return lispval_string(tempdir_template);
  else {
    char *tmpdir = get_tmpdir();
    u8_string tmp = u8_mkpath(tmpdir,"fdtempXXXXXX");
    lispval result = lispval_string(tmp);
    u8_free(tmp);
    return result;}
}

static int temproot_set(lispval sym,lispval value,void *ignore)
{
  u8_string template, old_template = tempdir_template;
  if (!(STRINGP(value))) {
    fd_type_error("string","temproot_set",value);
    return -1;}
  if ((CSTRING(value)[0])!='/')
    u8_log(LOG_WARN,"BADTEMPLATE",
           "Setting template to a relative file path");
  template = u8_strdup(CSTRING(value));
  tempdir_template = template;
  if (old_template) u8_free(old_template);
  return 1;
}

static int delete_tempdirs_on_exit = 1;
static lispval tempdirs = EMPTY, keeptemp = EMPTY;
static u8_mutex tempdirs_lock;

static u8_string tempdir_core(lispval template_arg,int keep)
{
  u8_string tempname;
  u8_string consed = NULL, template=
    ((STRINGP(template_arg))?(CSTRING(template_arg)):(NULL));
  if (SYMBOLP(template_arg)) {
    lispval ctemp = fd_config_get(CSTRING(template_arg));
    if (STRINGP(ctemp))
      template = consed = u8_strdup(CSTRING(ctemp));
    fd_decref(ctemp);}
  if (!(template)) template = tempdir_template;
  if (!(template)) {
    char *tmpdir = get_tmpdir();
    template = consed = u8_mkpath(tmpdir,"fdtempXXXXXX");}
  /* Unlike mkdtemp, u8_tempdir doesn't overwrite its argument */
  tempname = u8_tempdir(template);
  if (tempname) {
    if (!(keep)) {
      lispval result = fd_make_string(NULL,-1,tempname);
      u8_lock_mutex(&tempdirs_lock);
      CHOICE_ADD(tempdirs,result);
      u8_unlock_mutex(&tempdirs_lock);}
    if (consed) u8_free(consed);
    return tempname;}
  else {
    u8_condition cond = u8_strerror(errno); errno = 0;
    if (consed) u8_free(consed);
    fd_seterr(cond,"tempdir_prim",template,fd_incref(template_arg));
    return NULL;}
}

static void remove_tempdirs()
{
  int n_files = FD_CHOICE_SIZE(tempdirs);
  int n_keep = FD_CHOICE_SIZE(keeptemp);
  if ((n_files)&&(n_keep))
    u8_log(LOG_NOTICE,"TEMPFILES","Removing %d=%d-%d temporary directories",
           n_files-n_keep,n_files,n_keep);
  else if (n_files)
    u8_log(LOG_NOTICE,"TEMPFILES","Removing %d temporary directories",n_files);
  else return;
  u8_lock_mutex(&tempdirs_lock); {
    lispval to_remove = fd_difference(tempdirs,keeptemp);
    DO_CHOICES(tmpfile,to_remove) {
      if (STRINGP(tmpfile)) {
        u8_log(LOG_DEBUG,"TEMPFILES","Removing directory %s",
               CSTRING(tmpfile));
        if (u8_directoryp(CSTRING(tmpfile)))
          u8_rmtree(CSTRING(tmpfile));}}
    fd_decref(tempdirs); fd_decref(keeptemp); fd_decref(to_remove);
    tempdirs = EMPTY; keeptemp = EMPTY;
    u8_unlock_mutex(&tempdirs_lock);}
}

FD_EXPORT u8_string fd_tempdir(u8_string spec,int keep)
{
  lispval template_arg = ((spec)?(fd_parse_arg(spec)):(VOID));
  u8_string dirname = tempdir_core(template_arg,keep);
  fd_decref(template_arg);
  return dirname;
}

static lispval tempdir_prim(lispval template_arg,lispval keep)
{
  if ((VOIDP(template_arg))||
      (FALSEP(template_arg))||
      (SYMBOLP(template_arg))||
      (STRINGP(template_arg))) {
    u8_string dirname = tempdir_core(template_arg,(!(FALSEP(keep))));
    if (dirname == NULL) return FD_ERROR;
    else return fd_init_string(NULL,-1,dirname);}
  return fd_type_error("tempdir template (string, symbol, or #f)",
                       "tempdir_prim",
                       template_arg);
}

static lispval tempdir_done_prim(lispval tempdir,lispval force_arg)
{
  int force = (!(FALSEP(force_arg))), doit = 0;
  u8_string dirname = CSTRING(tempdir);
  u8_lock_mutex(&tempdirs_lock); {
    lispval cur_tempdirs = tempdirs; lispval cur_keep = keeptemp;
    if (fd_overlapp(tempdir,cur_tempdirs)) {
      if (fd_overlapp(tempdir,cur_keep)) {
        if (force) {
          u8_log(LOG_WARN,"Forced temp deletion",
                 "Forcing deletion of directory %s, declared for safekeeping",dirname);
          keeptemp = fd_difference(cur_keep,tempdir);
          tempdirs = fd_difference(cur_tempdirs,tempdir);
          fd_decref(cur_keep); fd_decref(cur_tempdirs);
          doit = 1;}
        else u8_log(LOG_WARN,_("Keeping temp directory"),
                    "The directory %s is declared for safekeeping, leaving",dirname);}
      else {
        u8_log(LOG_INFO,_("Deleting temp directory"),
               "Explicitly deleting temporary directory %s",dirname);
        tempdirs = fd_difference(cur_tempdirs,tempdir);
        fd_decref(cur_tempdirs);
        doit = 1;}}
    else if (!(u8_directoryp(dirname)))
      u8_log(LOG_WARN,_("Weird temp directory"),
             "The temporary directory %s is neither declared or existing",dirname);
    else if (force) {
      u8_log(LOG_WARN,"Forced temp delete",
             "Forcing deletion of the undeclared temporary directory %s",dirname);
      doit = 1;}
    else u8_log(LOG_WARN,"Unknown temp directory",
                "The directory %s is not a known temporary directory, leaving",
                CSTRING(tempdir));
    u8_unlock_mutex(&tempdirs_lock);}
  if (doit) {
    int retval = u8_rmtree(dirname);
    if (retval<0) return FD_ERROR;
    else if (retval==0) return FD_FALSE;
    else return FD_INT(retval);}
  else return FD_FALSE;
}

static lispval is_tempdir_prim(lispval tempdir)
{
  u8_lock_mutex(&tempdirs_lock); {
    lispval cur_tempdirs = tempdirs;
    int found = fd_overlapp(tempdir,cur_tempdirs);
    u8_unlock_mutex(&tempdirs_lock);
    if (found) return FD_TRUE; else return FD_FALSE;}
}

static lispval tempdirs_get(lispval sym,void *ignore)
{
  lispval dirs = tempdirs;
  fd_incref(dirs);
  return dirs;
}
static int tempdirs_add(lispval sym,lispval value,void *ignore)
{
  if (!(STRINGP(value))) {
    fd_type_error("string","tempdirs_add",value);
    return -1;}
  else fd_incref(value);
  u8_lock_mutex(&tempdirs_lock);
  CHOICE_ADD(tempdirs,value);
  u8_unlock_mutex(&tempdirs_lock);
  return 1;
}

static lispval keepdirs_get(lispval sym,void *ignore)
{
  /* If delete_tempdirs_on_exit is zero, we keep all of the current
     tempdirs, or return true if there aren't any.  Otherwise, we just
     return the stored value. */
  if (delete_tempdirs_on_exit==0)
    return fd_deep_copy(tempdirs);
  else return fd_deep_copy(keeptemp);
}
static int keepdirs_add(lispval sym,lispval value,void *ignore)
{
  if (FD_TRUEP(value)) {
    if (delete_tempdirs_on_exit) {
      delete_tempdirs_on_exit = 0;
      return 1;}
    else return 0;}
  else if (FALSEP(value)) {
    if (!(delete_tempdirs_on_exit)) {
      delete_tempdirs_on_exit = 1;
      return 1;}
    else return 0;}
  else if (!(STRINGP(value))) {
    fd_type_error("string","keepdirs_add",value);
    return -1;}
  else if ((fd_boolstring(CSTRING(value),-1))>=0) {
    int val = fd_boolstring(CSTRING(value),-1);
    if (val) val = 0; else val = 1;
    if (delete_tempdirs_on_exit == val) return 0;
    else {
      delete_tempdirs_on_exit = val;
      return 1;}}
  else fd_incref(value);
  u8_lock_mutex(&tempdirs_lock);
  CHOICE_ADD(keeptemp,value);
  u8_unlock_mutex(&tempdirs_lock);
  return 1;
}

static lispval keeptemp_get(lispval sym,void *ignore)
{
  /* If delete_tempdirs_on_exit is zero, we keep all of the current
     tempdirs, or return true if there aren't any.  Otherwise, we just
     return the stored value. */
  if (delete_tempdirs_on_exit==0) return FD_TRUE;
  else return FD_FALSE;
}
static int keeptemp_set(lispval sym,lispval value,void *ignore)
{
  if (FD_TRUEP(value)) {
    if (delete_tempdirs_on_exit) {
      delete_tempdirs_on_exit = 0;
      return 1;}
    else return 0;}
  else if (FALSEP(value)) {
    if (!(delete_tempdirs_on_exit)) {
      delete_tempdirs_on_exit = 1;
      return 1;}
    else return 0;}
  else if (!(STRINGP(value))) {
    fd_type_error("string","keeptemp_add",value);
    return -1;}
  else if ((fd_boolstring(CSTRING(value),-1))>=0) {
    int val = fd_boolstring(CSTRING(value),-1);
    if (val) val = 0; else val = 1;
    if (delete_tempdirs_on_exit == val) return 0;
    else {
      delete_tempdirs_on_exit = val;
      return 1;}}
  else {
    fd_type_error("string","keeptemp_set",value);
    return -1;}
  return 1;
}

/* File time info */

static lispval make_timestamp(time_t tick)
{
  struct U8_XTIME xt; u8_init_xtime(&xt,tick,u8_second,0,0,0);
  return fd_make_timestamp(&xt);
}

static lispval file_modtime(lispval filename)
{
  time_t mtime = u8_file_mtime(CSTRING(filename));
  if (mtime<0) return FD_ERROR;
  else return make_timestamp(mtime);
}

static lispval set_file_modtime(lispval filename,lispval timestamp)
{
  time_t mtime=
    ((VOIDP(timestamp))?(time(NULL)):
     (FIXNUMP(timestamp))?(FIX2INT(timestamp)):
     (FD_BIGINTP(timestamp))?(fd_getint(timestamp)):
     (TYPEP(timestamp,fd_timestamp_type))?
     (((struct FD_TIMESTAMP *)timestamp)->u8xtimeval.u8_tick):
     (-1));
  if (mtime<0)
    return fd_type_error("time","set_file_modtime",timestamp);
  else if (u8_set_mtime(CSTRING(filename),mtime)<0)
    return FD_ERROR;
  else return FD_TRUE;
}

static lispval file_atime(lispval filename)
{
  time_t mtime = u8_file_atime(CSTRING(filename));
  if (mtime<0) return FD_ERROR;
  else return make_timestamp(mtime);
}

static lispval set_file_atime(lispval filename,lispval timestamp)
{
  time_t atime=
    ((VOIDP(timestamp))?(time(NULL)):
     (FIXNUMP(timestamp))?(FIX2INT(timestamp)):
     (FD_BIGINTP(timestamp))?(fd_getint(timestamp)):
     (TYPEP(timestamp,fd_timestamp_type))?
     (((struct FD_TIMESTAMP *)timestamp)->u8xtimeval.u8_tick):
     (-1));
  if (atime<0)
    return fd_type_error("time","set_file_atime",timestamp);
  else if (u8_set_atime(CSTRING(filename),atime)<0)
    return FD_ERROR;
  else return FD_TRUE;
}

static lispval file_ctime(lispval filename)
{
  time_t mtime = u8_file_ctime(CSTRING(filename));
  if (mtime<0) return FD_ERROR;
  else return make_timestamp(mtime);
}

static lispval file_mode(lispval filename)
{
  int mode = u8_file_mode(CSTRING(filename));
  if (mode<0) return FD_ERROR;
  else return FD_INT(mode);
}

static lispval file_size(lispval filename)
{
  ssize_t size = u8_file_size(CSTRING(filename));
  if (size<0) return FD_ERROR;
  else if (size<FD_MAX_FIXNUM)
    return FD_INT(size);
  else return fd_make_bigint(size);
}

static lispval file_owner(lispval filename)
{
  u8_string name = u8_file_owner(CSTRING(filename));
  if (name) return fd_lispstring(name);
  else return FD_ERROR;
}

static lispval set_file_access_prim(lispval filename,
                                    lispval owner,
                                    lispval group,
                                    lispval mode_arg)
{
  mode_t mode = (FD_FIXNUMP(mode_arg)) ? (FD_FIX2INT(mode_arg)) : -1;
  u8_uid uid; u8_gid gid=-1;
  if (FD_FIXNUMP(owner))
    uid = FD_FIX2INT(owner);
  else if (FD_STRINGP(owner)) {
    uid = u8_getuid(FD_CSTRING(owner));
    if (uid<0) {
      fd_seterr("UnknownUser","set_file_access_prim",
                u8_strdup(FD_CSTRING(owner)),
                VOID);
      return FD_ERROR;}}
  else return fd_type_error("username","set_file_access_prim",owner);
  if (FD_FIXNUMP(group))
    gid = FD_FIX2INT(group);
  else if (FD_STRINGP(group)) {
    gid = u8_getgid(FD_CSTRING(group));
    if (gid<0) {
      fd_seterr("UnknownGroup","set_file_access_prim",
                u8_strdup(FD_CSTRING(group)),
                VOID);
      return FD_ERROR;}}
  else return fd_type_error("groupname","set_file_access_prim",group);
  int rv = u8_set_access_x(FD_CSTRING(filename),uid,gid,mode);
  if (rv < 0)
    return FD_ERROR;
  else return FD_INT(rv);
}

/* Current directory information */

static lispval getcwd_prim()
{
  u8_string wd = u8_getcwd();
  if (wd) return fd_lispstring(wd);
  else return FD_ERROR;
}

static lispval setcwd_prim(lispval dirname)
{
  if (u8_setcwd(CSTRING(dirname))<0)
    return FD_ERROR;
  else return VOID;
}

/* Directory listings */

static lispval getfiles_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_getfiles(CSTRING(dirname),(!(FALSEP(fullpath)))), *scan = contents;
  if (contents == NULL) return FD_ERROR;
  else while (*scan) {
    lispval string = fd_lispstring(*scan);
    CHOICE_ADD(results,string);
    scan++;}
  u8_free(contents);
  return results;
}

static lispval getdirs_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_getdirs(CSTRING(dirname),(!(FALSEP(fullpath)))), *scan = contents;
  if (contents == NULL) return FD_ERROR;
  else while (*scan) {
    lispval string = fd_lispstring(*scan);
    CHOICE_ADD(results,string);
    scan++;}
  u8_free(contents);
  return results;
}

static lispval getlinks_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_readdir(CSTRING(dirname),U8_LIST_LINKS,(!(FALSEP(fullpath)))),
    *scan = contents;
  if (contents == NULL) return FD_ERROR;
  else while (*scan) {
    lispval string = fd_lispstring(*scan);
    CHOICE_ADD(results,string);
    scan++;}
  u8_free(contents);
  return results;
}

static lispval readdir_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_readdir(CSTRING(dirname),0,(!(FALSEP(fullpath)))),
    *scan = contents;
  if (contents == NULL) return FD_ERROR;
  else while (*scan) {
    lispval string = fd_lispstring(*scan);
    CHOICE_ADD(results,string);
    scan++;}
  u8_free(contents);
  return results;
}

/* File flush function */

static lispval close_prim(lispval portarg)
{
  if (TYPEP(portarg,fd_stream_type)) {
    struct FD_STREAM *dts=
      fd_consptr(struct FD_STREAM *,portarg,fd_stream_type);
    fd_close_stream(dts,0);
    return VOID;}
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    U8_OUTPUT *out = p->port_output; U8_INPUT *in = p->port_input; int closed = -1;
    if (out) {
      u8_flush(out);
      if (out->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
        U8_XOUTPUT *xout = (U8_XOUTPUT *)out;
        if (xout->u8_xfd<0) {}
        else {
          closed = xout->u8_xfd; fsync(xout->u8_xfd); close(xout->u8_xfd);
          xout->u8_xfd = -1;}}}
    if (in) {
      u8_flush(out);
      if (in->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
        U8_XINPUT *xin = (U8_XINPUT *)in;
        if (xin->u8_xfd<0) { /* already closed. warn? */ }
        else if (closed!=xin->u8_xfd) {
          close(xin->u8_xfd); xin->u8_xfd = -1;}}}
    return VOID;}
  else return fd_type_error("port","close_prim",portarg);
}

static lispval flush_prim(lispval portarg)
{
  if ((VOIDP(portarg))||
      (FALSEP(portarg))||
      (FD_TRUEP(portarg))) {
    u8_flush_xoutput(&u8stdout);
    u8_flush_xoutput(&u8stderr);
    return VOID;}
  else if (TYPEP(portarg,fd_stream_type)) {
    struct FD_STREAM *dts=
      fd_consptr(struct FD_STREAM *,portarg,fd_stream_type);
    fd_flush_stream(dts);
    return VOID;}
  else if (TYPEP(portarg,fd_port_type)) {
    U8_OUTPUT *out = get_output_port(portarg);
    u8_flush(out);
    if (out->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
      U8_XOUTPUT *xout = (U8_XOUTPUT *)out;
      fsync(xout->u8_xfd);}
    return VOID;}
  else return fd_type_error(_("port or stream"),"flush_prim",portarg);
}

static lispval setbuf_prim(lispval portarg,lispval insize,lispval outsize)
{
  if (TYPEP(portarg,fd_stream_type)) {
    struct FD_STREAM *dts=
      fd_consptr(struct FD_STREAM *,portarg,fd_stream_type);
    fd_setbufsize(dts,FIX2INT(insize));
    return VOID;}
  else if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    if (FIXNUMP(insize)) {
      U8_INPUT *in = p->port_input;
      if ((in) && (in->u8_streaminfo&U8_STREAM_OWNS_XBUF)) {
        u8_xinput_setbuf((struct U8_XINPUT *)in,FIX2INT(insize));}}

    if (FIXNUMP(outsize)) {
      U8_OUTPUT *out = p->port_output;
      if ((out) && (out->u8_streaminfo&U8_STREAM_OWNS_XBUF)) {
        u8_xoutput_setbuf((struct U8_XOUTPUT *)out,FIX2INT(outsize));}}
    return VOID;}
  else return fd_type_error("port/stream","setbuf_prim",portarg);
}

static lispval getpos_prim(lispval portarg)
{
  if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    fd_off_t result = -1;
    if (p->port_input)
      result = u8_getpos((struct U8_STREAM *)(p->port_input));
    else if (p->port_output)
      result = u8_getpos((struct U8_STREAM *)(p->port_output));
    else return fd_type_error(_("port"),"getpos_prim",portarg);
    if (result<0)
      return FD_ERROR;
    else if (result<FD_MAX_FIXNUM)
      return FD_INT(result);
    else return fd_make_bigint(result);}
  else if (TYPEP(portarg,fd_stream_type)) {
    fd_stream ds = fd_consptr(fd_stream,portarg,fd_stream_type);
    fd_off_t pos = fd_getpos(ds);
    if (pos<0) return FD_ERROR;
    else if (pos<FD_MAX_FIXNUM) return FD_INT(pos);
    else return fd_make_bigint(pos);}
  else return fd_type_error("port or stream","getpos_prim",portarg);
}

static lispval endpos_prim(lispval portarg)
{
  if (FD_PORTP(portarg)) {
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    fd_off_t result = -1;
    if (p->port_input)
      result = u8_endpos((struct U8_STREAM *)(p->port_input));
    else if (p->port_output)
      result = u8_endpos((struct U8_STREAM *)(p->port_output));
    else return fd_type_error(_("port"),"getpos_prim",portarg);
    if (result<0)
      return FD_ERROR;
    else if (result<FD_MAX_FIXNUM)
      return FD_INT(result);
    else return fd_make_bigint(result);}
  else if (TYPEP(portarg,fd_stream_type)) {
    fd_stream ds = fd_consptr(fd_stream,portarg,fd_stream_type);
    fd_off_t pos = fd_endpos(ds);
    if (pos<0) return FD_ERROR;
    else if (pos<FD_MAX_FIXNUM) return FD_INT(pos);
    else return fd_make_bigint(pos);}
  else return fd_type_error("port or stream","endpos_prim",portarg);
}

static lispval file_progress_prim(lispval portarg)
{
  double result = -1.0;
  struct FD_PORT *p=
    fd_consptr(struct FD_PORT *,portarg,fd_port_type);
  if (p->port_input)
    result = u8_getprogress((struct U8_STREAM *)(p->port_input));
  else if (p->port_output)
    result = u8_getprogress((struct U8_STREAM *)(p->port_output));
  else return fd_type_error(_("port"),"file_progress_prim",portarg);
  if (result<0)
    return FD_ERROR;
  else return fd_init_double(NULL,result);
}

static lispval setpos_prim(lispval portarg,lispval off_arg)
{
  if (FD_PORTP(portarg)) {
    fd_off_t off, result;
    struct FD_PORT *p=
      fd_consptr(struct FD_PORT *,portarg,fd_port_type);
    if (FIXNUMP(off_arg)) off = FIX2INT(off_arg);
    else if (FD_BIGINTP(off_arg))
#if (_FILE_OFFSET_BITS==64)
      off = (fd_off_t)fd_bigint_to_long_long((fd_bigint)off_arg);
#else
    off = (fd_off_t)fd_bigint_to_long((fd_bigint)off_arg);
#endif
    else return fd_type_error(_("offset"),"setpos_prim",off_arg);
    if (p->port_input)
      result = u8_setpos((struct U8_STREAM *)(p->port_input),off);
    else if (p->port_output)
      result = u8_setpos((struct U8_STREAM *)(p->port_output),off);
    else return fd_type_error(_("port"),"setpos_prim",portarg);
    if (result<0)
      return FD_ERROR;
    else if (result<FD_MAX_FIXNUM)
      return FD_INT(off);
    else return fd_make_bigint(result);}
  else if (TYPEP(portarg,fd_stream_type)) {
    fd_stream ds = fd_consptr(fd_stream,portarg,fd_stream_type);
    fd_off_t off, result;
    if (FIXNUMP(off_arg)) off = FIX2INT(off_arg);
    else if (FD_BIGINTP(off_arg))
#if (_FILE_OFFSET_BITS==64)
      off = (fd_off_t)fd_bigint_to_long_long((fd_bigint)off_arg);
#else
    off = (fd_off_t)fd_bigint_to_long((fd_bigint)off_arg);
#endif
    else return fd_type_error(_("offset"),"setpos_prim",off_arg);
    result = fd_setpos(ds,off);
    if (result<0) return FD_ERROR;
    else if (result<FD_MAX_FIXNUM) return FD_INT(result);
    else return fd_make_bigint(result);}
  else return fd_type_error("port or stream","setpos_prim",portarg);
}

/* File system info */

#if (((HAVE_SYS_STATFS_H)||(HAVE_SYS_VSTAT_H))&&(HAVE_STATFS))
static void statfs_set(lispval,u8_string,long long int,long long int);

static lispval fsinfo_prim(lispval arg)
{
  u8_string path = CSTRING(arg); struct statfs info;
  char *usepath; int retval;
  if (u8_file_existsp(path))
    usepath = u8_tolibc(path);
  else {
    u8_string abs = u8_abspath(path,NULL);
    u8_string dir = u8_dirname(abs);
    usepath = u8_tolibc(dir);
    u8_free(abs); u8_free(dir);}
  retval = statfs(usepath,&info);
  if (((char *)path)!=usepath) u8_free(usepath);
  if (retval) {
    u8_graberrno("fsinfo_prim",u8_strdup(path));
    return FD_ERROR;}
  else {
    lispval result = fd_init_slotmap(NULL,0,NULL);
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
static void statfs_set(lispval r,u8_string name,
                       long long int val,long long int mul)
{
  lispval slotid = fd_intern(name);
  lispval lval = FD_INT2DTYPE(val);
  if (mul!=1) {
    lispval mulval = FD_INT2DTYPE(mul);
    lispval rval = fd_multiply(lval,mulval);
    fd_decref(mulval); fd_decref(lval);
    lval = rval;}
  fd_store(r,slotid,lval);
  fd_decref(lval);
}
#else
static lispval fsinfo_prim(lispval arg)
{
  return fd_err("statfs unavailable","fsinfo_prim",NULL,VOID);
}
#endif

/* Snapshot save and restore */

/* A snapshot is a set of variable bindings and CONFIG settings which
   can be saved to and restored from a disk file.  */

static u8_condition SnapshotTrouble=_("SNAPSHOT");

static lispval snapshotvars, snapshotconfig, snapshotfile, configinfo;

FD_EXPORT
/* fd_snapshot:
     Arguments: an environment pointer and a filename (u8_string)
     Returns: an int (<0 on error)
 Saves a snapshot of the environment into the designated file.
*/
int fd_snapshot(fd_lexenv env,u8_string filename)
{
  lispval vars = fd_symeval(snapshotvars,env);
  lispval configvars = fd_symeval(snapshotconfig,env);
  if ((EMPTYP(vars)) && (EMPTYP(configvars))) {
    u8_message("No snapshot information to save");
    return VOID;}
  else {
    struct FD_STREAM *out; int bytes;
    lispval slotmap = (lispval)fd_empty_slotmap();
    if (VOIDP(vars)) vars = EMPTY;
    if (VOIDP(configvars)) configvars = EMPTY;
    {DO_CHOICES(sym,vars)
       if (SYMBOLP(sym)) {
         lispval val = fd_symeval(sym,env);
         if (VOIDP(val))
           u8_log(LOG_WARN,SnapshotTrouble,
                  "The snapshot variable %q is unbound",sym);
         else fd_add(slotmap,sym,val);
         fd_decref(val);}
       else {
         fd_decref(slotmap);
         return fd_type_error("symbol","fd_snapshot",sym);}}
    {DO_CHOICES(sym,configvars)
       if (SYMBOLP(sym)) {
         lispval val = fd_config_get(SYM_NAME(sym));
         lispval config_entry = fd_conspair(sym,val);
         if (VOIDP(val))
           u8_log(LOG_WARN,SnapshotTrouble,
                  "The snapshot config %q is not set",sym);
         else fd_add(slotmap,configinfo,config_entry);
         fd_decref(val);}
       else {
         fd_decref(slotmap);
         return fd_type_error("symbol","fd_snapshot",sym);}}
    out = fd_open_file(filename,FD_FILE_CREATE);
    if (out == NULL) {
      fd_decref(slotmap);
      return -1;}
    else bytes = fd_write_dtype(fd_writebuf(out),slotmap);
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
int fd_snapback(fd_lexenv env,u8_string filename)
{
  struct FD_STREAM *in;
  lispval slotmap; int actions = 0;
  in = fd_open_file(filename,FD_FILE_READ);
  if (in == NULL) return -1;
  else slotmap = fd_read_dtype(fd_readbuf(in));
  if (FD_ABORTP(slotmap)) {
    fd_close_stream(in,FD_STREAM_CLOSE_FULL);
    return slotmap;}
  else if (SLOTMAPP(slotmap)) {
    lispval keys = fd_getkeys(slotmap);
    DO_CHOICES(key,keys) {
      lispval v = fd_get(slotmap,key,VOID);
      if (FD_EQ(key,configinfo)) {
        DO_CHOICES(config_entry,v)
          if ((PAIRP(config_entry)) &&
              (SYMBOLP(FD_CAR(config_entry))))
            if (fd_set_config(SYM_NAME(FD_CAR(config_entry)),
                              FD_CDR(config_entry)) <0) {
              fd_decref(v); fd_decref(keys); fd_decref(slotmap);
              return -1;}
            else actions++;
          else {
            fd_seterr(fd_TypeError,"fd_snapback",
                      "saved config entry",
                      fd_incref(config_entry));
            fd_decref(v); fd_decref(keys); fd_decref(slotmap);
            return -1;}}
      else {
        int setval = fd_assign_value(key,v,env);
        if (setval==0)
          setval = fd_bind_value(key,v,env);
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

static lispval snapshot_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv save_env; u8_string save_file; int retval = 0;
  lispval arg1 = fd_eval(fd_get_arg(expr,1),env), arg2 = fd_eval(fd_get_arg(expr,2),env);
  if (VOIDP(arg1)) {
    lispval saveto = fd_symeval(snapshotfile,env);
    save_env = env;
    if (STRINGP(saveto))
      save_file = u8_strdup(CSTRING(saveto));
    else save_file = u8_strdup("snapshot");
    fd_decref(saveto);}
  else if (FD_LEXENVP(arg1)) {
    if (STRINGP(arg2)) save_file = u8_strdup(CSTRING(arg2));
    else save_file = u8_strdup("snapshot");
    save_env = (fd_lexenv)arg1;}
  else if (STRINGP(arg1)) {
    save_file = u8_strdup(CSTRING(arg1));
    if (FD_LEXENVP(arg2))
      save_env = (fd_lexenv)arg2;
    else save_env = env;}
  else {
    lispval err = fd_type_error("filename","snapshot_prim",arg1);
    fd_decref(arg1); fd_decref(arg2);
    return err;}
  retval = fd_snapshot(save_env,save_file);
  fd_decref(arg1); fd_decref(arg2); u8_free(save_file);
  if (retval<0) return FD_ERROR;
  else return FD_INT(retval);
}

static lispval snapback_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  fd_lexenv save_env; u8_string save_file; int retval = 0;
  lispval arg1 = fd_eval(fd_get_arg(expr,1),env), arg2 = fd_eval(fd_get_arg(expr,2),env);
  if (VOIDP(arg1)) {
    lispval saveto = fd_symeval(snapshotfile,env);
    save_env = env;
    if (STRINGP(saveto))
      save_file = u8_strdup(CSTRING(saveto));
    else save_file = u8_strdup("snapshot");
    fd_decref(saveto);}
  else if (FD_LEXENVP(arg1)) {
    if (STRINGP(arg2)) save_file = u8_strdup(CSTRING(arg2));
    else save_file = u8_strdup("snapshot");
    save_env = (fd_lexenv)arg1;}
  else if (STRINGP(arg1)) {
    save_file = u8_strdup(CSTRING(arg1));
    if (FD_LEXENVP(arg2))
      save_env = (fd_lexenv)arg2;
    else save_env = env;}
  else {
    lispval err = fd_type_error("filename","snapshot_prim",arg1);
    fd_decref(arg1); fd_decref(arg2);
    return err;}
  retval = fd_snapback(save_env,save_file);
  fd_decref(arg1); fd_decref(arg2); u8_free(save_file);
  if (retval<0) return FD_ERROR;
  else return FD_INT(retval);
}

/* Stackdump configuration */

static u8_string stackdump_filename = NULL;
static FILE *stackdump_file = NULL;
static u8_mutex stackdump_lock;

FD_EXPORT void stackdump_dump(u8_string dump)
{
  struct U8_XTIME now;
  struct U8_OUTPUT out;
  u8_now(&now);
  u8_lock_mutex(&stackdump_lock);
  if (stackdump_file == NULL) {
    if (stackdump_filename == NULL) {
      u8_unlock_mutex(&stackdump_lock);
      return;}
    else {
      stackdump_file = fopen(stackdump_filename,"w");
      if (stackdump_file == NULL) {
        u8_log(LOG_CRIT,StackDumpEvent,"Couldn't open stackdump file %s",stackdump_filename);
        u8_free(stackdump_filename);
        stackdump_filename = NULL;
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

static lispval stackdump_config_get(lispval var,void *ignored)
{
  if (stackdump_filename)
    return lispval_string(stackdump_filename);
  else return FD_FALSE;
}
static int stackdump_config_set(lispval var,lispval val,void *ignored)
{
  if ((STRINGP(val)) || (FALSEP(val)) || (FD_TRUEP(val))) {
    u8_string filename = ((STRINGP(val)) ? (CSTRING(val)) :
                        (FD_TRUEP(val)) ? ((u8_string)"stackdump.log") : (NULL));
    if (stackdump_filename == filename) return 0;
    else if ((stackdump_filename) && (filename) &&
             (strcmp(stackdump_filename,filename)==0))
      return 0;
    u8_lock_mutex(&stackdump_lock);
    if (stackdump_file) {
      fclose(stackdump_file);
      stackdump_file = NULL;
      u8_free(stackdump_filename);
      stackdump_filename = NULL;}
    if (filename) {
      stackdump_filename = u8_strdup(filename);
      /* fd_dump_exception = stackdump_dump; */
}
    u8_unlock_mutex(&stackdump_lock);
    return 1;}
  else {
    fd_seterr(fd_TypeError,"stackdump_config_set",NULL,val);
    return -1;}
}

/* Close libu8 stdin/out/err proxies */

static void close_u8stdio()
{
  u8_close_input((u8_input)&u8stdin);
  u8_close_output((u8_output)&u8stdout);
  u8_close_output((u8_output)&u8stderr);
}

/* The init function */

static int scheme_fileio_initialized = 0;

FD_EXPORT void fd_init_driverfns_c(void);
FD_EXPORT void fd_init_loader_c(void);

FD_EXPORT void fd_init_fileprims_c()
{
  lispval fileio_module;
  if (scheme_fileio_initialized) return;
  scheme_fileio_initialized = 1;
  fd_init_scheme();
  fileio_module =
    fd_new_cmodule("FILEIO",(FD_MODULE_DEFAULT),fd_init_fileprims_c);
  u8_register_source_file(_FILEINFO);


  u8_init_mutex(&stackdump_lock);
  u8_init_mutex(&tempdirs_lock);

  u8_init_xinput(&u8stdin,0,NULL);
  u8_init_xoutput(&u8stdout,1,NULL);
  u8_init_xoutput(&u8stderr,2,NULL);

  atexit(close_u8stdio);

  u8_set_global_output((u8_output)&u8stdout);

  fd_idefn3(fileio_module,"OPEN-OUTPUT-FILE",open_output_file,1,
            "`(open-output-file *filename* [*encoding*] [*escape*])` "
            "returns an output port, writing to the beginning of the "
            "text file *filename* using *encoding*. If *escape* is "
            "specified, it can be the character & or \\, which causes "
            "special characters to be output as either entity escaped or "
            "unicode escaped sequences.",
            fd_string_type,FD_VOID,-1,FD_VOID,fd_character_type,FD_VOID);
  fd_idefn3(fileio_module,"EXTEND-OUTPUT-FILE",extend_output_file,1,
            "`(extend-output-file *filename* [*encoding*] [*escape*])` "
            "returns an output port, writing to the end of the "
            "text file *filename* using *encoding*. If *escape* is "
            "specified, it can be the character & or \\, which causes "
            "special characters to be output as either entity escaped or "
            "unicode escaped sequences.",
            fd_string_type,FD_VOID,-1,FD_VOID,fd_character_type,FD_VOID);
  fd_idefn2(fileio_module,"OPEN-INPUT-FILE",open_input_file,1,
            "`(open-input-file *filename* [*encoding*])` returns an input port "
            "for the text file *filename*, translating from *encoding*. If "
            "*encoding* is not specified, the file is opened as a UTF-8 file.",
            fd_string_type,FD_VOID,-1,FD_VOID);
  fd_idefn3(fileio_module,"SETBUF!",setbuf_prim,2,
            "`(SETBUF! *port/stream* *insize* *outsize*)` sets the "
            "input and output buffer sizes for a port or stream.",
            -1,VOID,-1,FD_FALSE,-1,FD_FALSE);
  fd_defalias(fileio_module,"SETBUF","SETBUF!");

  fd_idefn(fileio_module,
           fd_make_cprim3x("WRITE-FILE",writefile_prim,2,
                           fd_string_type,VOID,-1,VOID,
                           -1,VOID));

  fd_def_evalfn(fileio_module,"FILEOUT","",simple_fileout_evalfn);

  fd_def_evalfn(fileio_module,"SYSTEM","",simple_system_evalfn);

  fd_idefn(fileio_module,
           fd_make_cprim2x("REMOVE-FILE!",remove_file_prim,1,
                           fd_string_type,VOID,
                           -1,VOID));
  fd_defalias(fileio_module,"REMOVE-FILE","REMOVE-FILE!");
  fd_idefn(fileio_module,
           fd_make_cprim3x("MOVE-FILE!",move_file_prim,2,
                           fd_string_type,VOID,
                           fd_string_type,VOID,
                           -1,VOID));
  fd_defalias(fileio_module,"MOVE-FILE","MOVE-FILE!");
  fd_idefn(fileio_module,
           fd_make_cprim3x("LINK-FILE!",link_file_prim,2,
                           fd_string_type,VOID,
                           fd_string_type,VOID,
                           -1,VOID));
  fd_defalias(fileio_module,"LINK-FILE","LINK-FILE!");
  fd_idefn(fileio_module,
           fd_make_cprim2x("REMOVE-TREE!",remove_tree_prim,1,
                           fd_string_type,VOID,
                           -1,VOID));
  fd_defalias(fileio_module,"REMOVE-TREE","REMOVE-TREE!");


  fd_idefn2(fileio_module,"FILESTRING",filestring_prim,1,
            "(FILESTRING *file* [*encoding*]) returns the contents of a text file. "
            "The *encoding*, if provided, specifies the character encoding, which "
            "defaults to UTF-8",
            fd_string_type,VOID,-1,VOID);
  fd_idefn1(fileio_module,"FILEDATA",filedata_prim,1,
            "(FILEDATA *file*) returns the contents of *file* as a packet.",
            fd_string_type,VOID);
  fd_idefn1(fileio_module,"FILECONTENT",filecontent_prim,1,
            "Returns the contents of a named file, trying to be intelligent "
            "about returning a string or packet depending on the probably "
            "file type",
            fd_string_type,VOID);

  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-EXISTS?",file_existsp,1,
                           fd_string_type,VOID));

  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-READABLE?",file_readablep,1,
                           fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-WRITABLE?",file_writablep,1,
                           fd_string_type,VOID));

  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-SYMLINK?",file_symlinkp,1,fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-SOCKET?",file_socketp,1,fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-REGULAR?",file_regularp,1,fd_string_type,VOID));
  fd_defalias(fileio_module,"REGULAR-FILE?","FILE-REGULAR?");

  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-DIRECTORY?",file_directoryp,1,
                           fd_string_type,VOID));
  fd_defalias(fileio_module,"DIRECTORY?","FILE-DIRECTORY?");
  fd_idefn(fileio_module,
           fd_make_cprim1x("PATH-DIRNAME",path_dirname,1,
                           fd_string_type,VOID));
  fd_defalias(fileio_module,"DIRNAME","PATH-DIRNAME");

  fd_idefn(fileio_module,
           fd_make_cprim2x("PATH-BASENAME",path_basename,1,
                           fd_string_type,VOID,
                           -1,VOID));
  fd_defalias(fileio_module,"BASENAME","PATH-BASENAME");

  fd_idefn(fileio_module,
           fd_make_cprim1x("PATH-LOCATION",path_location,1,fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("PATH-SUFFIX",path_suffix,1,
                           fd_string_type,VOID,-1,VOID));

  fd_idefn(fileio_module,
           fd_make_cprim2x("ABSPATH",file_abspath,1,
                           fd_string_type,VOID,
                           fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("REALPATH",file_realpath,1,
                           fd_string_type,VOID,
                           fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("MKPATH",mkpath_prim,2,
                           -1,VOID,fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim3x("READLINK",file_readlink,1,
                           fd_string_type,VOID,-1,VOID,-1,VOID));


  fd_idefn(fileio_module,
           fd_make_cprim1x("FSINFO",fsinfo_prim,1,
                           fd_string_type,VOID));
  fd_defalias(fileio_module,"STATFS","FSINFO");

  fd_idefn(fileio_module,
           fd_make_cprim2x("MKDIR",mkdir_prim,1,
                           fd_string_type,VOID,fd_fixnum_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("RMDIR",rmdir_prim,1,fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("MKDIRS",mkdirs_prim,1,
                           fd_string_type,VOID,fd_fixnum_type,VOID));

  fd_idefn(fileio_module,
           fd_make_cprim2x("TEMPDIR",tempdir_prim,0,
                           -1,VOID,-1,FD_FALSE));
  fd_idefn(fileio_module,
           fd_make_cprim1x("TEMPDIR?",is_tempdir_prim,0,
                           fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("TEMPDIR/DONE",tempdir_done_prim,0,
                           fd_string_type,VOID,
                           -1,FD_FALSE));
  fd_idefn(fileio_module,
           fd_make_cprim1x("RUNFILE",runfile_prim,1,
                           fd_string_type,VOID));

  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-MODTIME",file_modtime,1,
                           fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-ACCESSTIME",file_atime,1,
                           fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-CREATIONTIME",file_ctime,1,
                           fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("SET-FILE-MODTIME!",set_file_modtime,1,
                           fd_string_type,VOID,
                           -1,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim2x("SET-FILE-ATIME!",set_file_atime,1,
                           fd_string_type,VOID,
                           -1,VOID));

  fd_idefn4(fileio_module,"SET-FILE-ACCESS!",
            set_file_access_prim,FD_NEEDS_2_ARGS,
            "(SET-FILE-ACCESS! *file* [*mode*] [*group*] [*owner*]) sets "
            "the mode/group/owner of a file",
            fd_string_type,VOID,fd_fixnum_type,VOID,-1,VOID,-1,VOID);

  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-OWNER",file_owner,1,
                           fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-MODE",file_mode,1,
                           fd_string_type,VOID));
  fd_idefn(fileio_module,
           fd_make_cprim1x("FILE-SIZE",file_size,1,
                           fd_string_type,VOID));

  fd_idefn(fileio_module,
           fd_make_cprim2x("GETFILES",getfiles_prim,1,
                           fd_string_type,VOID,-1,FD_TRUE));
  fd_idefn(fileio_module,
           fd_make_cprim2x("GETDIRS",getdirs_prim,1,
                           fd_string_type,VOID,-1,FD_TRUE));
  fd_idefn(fileio_module,
           fd_make_cprim2x("GETLINKS",getlinks_prim,1,
                           fd_string_type,VOID,-1,FD_TRUE));
  fd_idefn(fileio_module,
           fd_make_cprim2x("READDIR",readdir_prim,1,
                           fd_string_type,VOID,-1,FD_TRUE));

  fd_idefn(fileio_module,fd_make_cprim0("GETCWD",getcwd_prim));
  fd_idefn(fileio_module,
           fd_make_cprim1x("SETCWD",setcwd_prim,1,fd_string_type,VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim1("FLUSH-OUTPUT",flush_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CLOSE",close_prim,0));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("GETPOS",getpos_prim,1,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SETPOS!",setpos_prim,2,-1,VOID,-1,VOID));
  fd_defalias(fd_scheme_module,"SETPOS","SETPOS!");
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ENDPOS",endpos_prim,1,-1,VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("FILE%",file_progress_prim,1,fd_port_type,VOID));

  fd_idefn(fd_xscheme_module,
           fd_make_cprim2x
           ("OPEN-SOCKET",open_socket_prim,1,
            fd_string_type,VOID,-1,VOID));

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

  snapshotvars = fd_intern("%SNAPVARS");
  snapshotconfig = fd_intern("%SNAPCONFIG");
  snapshotfile = fd_intern("%SNAPSHOTFILE");
  configinfo = fd_intern("%CONFIGINFO");

  noblock_symbol = fd_intern("NOBLOCK");
  nodelay_symbol = fd_intern("NODELAY");

  fd_def_evalfn(fileio_module,"SNAPSHOT","",snapshot_evalfn);
  fd_def_evalfn(fileio_module,"SNAPBACK","",snapback_evalfn);

  fd_register_config
    ("STACKDUMP","File to store stackdump information on errors",
     stackdump_config_get,stackdump_config_set,NULL);

  fd_finish_module(fileio_module);
}

FD_EXPORT void fd_init_schemeio()
{
  fd_init_fileprims_c();
  fd_init_driverfns_c();
  fd_init_loader_c();
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
