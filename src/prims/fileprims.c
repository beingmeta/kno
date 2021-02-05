/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define KNO_EVAL_INTERNALS 1 */

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/eval.h"
#include "kno/ports.h"
#include "kno/fileprims.h"
#include "kno/getsource.h"
#include "kno/cprims.h"

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

static u8_condition RemoveFailed=_("File removal failed");
static u8_condition LinkFailed=_("File link failed");
static u8_condition OpenFailed=_("File open failed");

static u8_condition StackDumpEvent=_("StackDump");
static u8_condition SnapshotSaved=_("Snapshot Saved");
static u8_condition SnapshotRestored=_("Snapshot Restored");

static u8_condition FilePrim_Failed=_("File primitive failed");

static lispval fileprim_error(u8_context caller,u8_string f)
{
  if (u8_current_exception)
    return kno_lisp_error(u8_current_exception);
  else return kno_err(FilePrim_Failed,caller,f,KNO_VOID);
}


/* Making ports */

static lispval make_port(U8_INPUT *in,U8_OUTPUT *out,u8_string id)
{
  struct KNO_PORT *port = u8_alloc(struct KNO_PORT);
  KNO_INIT_CONS(port,kno_ioport_type);
  port->port_input = in;
  port->port_output = out;
  port->port_id = id;
  return LISP_CONS(port);
}

static u8_output get_output_port(lispval portarg)
{
  if ((VOIDP(portarg))||(portarg == KNO_TRUE))
    return u8_current_output;
  else if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
    return p->port_output;}
  else return NULL;
}

/* Handling file opts */

DEF_KNOSYM(filemodes); DEF_KNOSYM(permissions);
DEF_KNOSYM(lock); DEF_KNOSYM(escape);
DEF_KNOSYM(write); DEF_KNOSYM(read);
DEF_KNOSYM(append); DEF_KNOSYM(create);
DEF_KNOSYM(exclusive); DEF_KNOSYM(excl);


static int get_file_opts(lispval opts,
			 int *open_flags,int *perm_flags,
			 int *lock_flags,
			 u8_encoding *enc,
			 lispval *escape_char)
{
  int rv = 1;
  u8_encoding encoding = NULL;
  if ( (STRINGP(opts)) && (encoding=u8_get_encoding(CSTRING(opts))) )
    *enc = encoding;
  else if (STRINGP(opts)) {
    u8_string string = KNO_CSTRING(opts);
    if (open_flags) {
      if ( (strchr(string,'r')) && (strchr(string,'w')) )
	*open_flags |= O_RDWR;
      else if (strchr(string,'r'))
	*open_flags |= O_RDONLY;
      else if (strchr(string,'w'))
	*open_flags |= O_WRONLY;
      else {}
      if (strchr(string,'+')) *open_flags = *open_flags | O_APPEND;
      if (strchr(string,'c')) *open_flags = *open_flags | O_CREAT;
      if (strchr(string,'l')) *lock_flags = *lock_flags | 1;}
    if (strchr(string,':')) {
      u8_string enc_name = strchr(string,':')+1;
      encoding = u8_get_encoding(enc_name);}}
  else if (TABLEP(opts)) {
    lispval enc = kno_getopt(opts,KNOSYM_ENCODING,KNO_VOID);
    lispval modes = kno_getopt(opts,KNOSYM(filemodes),KNO_VOID);
    lispval perms = kno_getopt(opts,KNOSYM(permissions),KNO_VOID);
    lispval lock = kno_getopt(opts,KNOSYM(lock),KNO_VOID);
    lispval escape = kno_getopt(opts,KNOSYM(escape),KNO_VOID);
    if (!((KNO_FALSEP(lock))||(KNO_VOIDP(lock)))) *lock_flags |= 1;
    if (KNO_STRINGP(enc)) encoding = u8_get_encoding(KNO_CSTRING(enc));
    else if (KNO_SYMBOLP(enc)) encoding = u8_get_encoding(KNO_SYMBOL_NAME(enc));
    else NO_ELSE;
    if ( (escape_char) && (KNO_VOIDP(*escape_char)) ) {
      if (KNO_CHARACTERP(escape)) *escape_char = escape;
      else if (KNO_STRINGP(escape)) {
	u8_string scan = KNO_CSTRING(escape);
	int ch = u8_sgetc(&scan);
	if ( (ch<0) && (*scan == '\0') )
	  *escape_char = KNO_CODE2CHAR(ch);
	else {
	  kno_seterr("InvalidEscapeArg","get_file_opts",NULL,escape);
	  rv=-1;}}
      else { kno_seterr("InvalidEscapeArg","get_file_opts",NULL,escape);
	rv=-1;}}
    if (open_flags) {
      int r = kno_overlapp(modes,KNOSYM(write));
      int w = kno_overlapp(modes,KNOSYM(read));
      if ( (r) && (w) )
	*open_flags |= O_RDWR;
      else if (r)
	*open_flags |= O_RDONLY;
      else if (w)
	*open_flags |= O_RDONLY;
      else NO_ELSE;
      if (kno_overlapp(modes,KNOSYM(append)))
	*open_flags |= O_APPEND;
      if (kno_overlapp(modes,KNOSYM(create)))
	*open_flags |= O_CREAT;
      if ( (kno_overlapp(modes,KNOSYM(exclusive))) ||
	   (kno_overlapp(modes,KNOSYM(excl))) )
	*open_flags |= O_EXCL;}
    kno_decref(escape);
    kno_decref(lock);
    kno_decref(perms);
    kno_decref(modes);
    kno_decref(enc);}
  else rv = 0;
  if ( (encoding) && (enc) ) *enc=encoding;
  return rv;
}

/* Opening files */

DEFCPRIM("open-output-file",open_output_file,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "returns an output port, writing to the beginning "
	 "of the text file *filename* using *encoding*. If "
	 "*escape* is specified, it can be the character & "
	 "or \\, which causes special characters to be "
	 "output as either entity escaped or unicode "
	 "escaped sequences.",
	 {"fname",kno_string_type,KNO_VOID},
	 {"opts",kno_any_type,KNO_VOID},
	 {"escape_char",kno_character_type,KNO_VOID})
static lispval open_output_file(lispval fname,lispval opts,lispval ec)
{
  struct U8_XOUTPUT *f;
  u8_string filename = kno_strdata(fname);
  u8_encoding enc = NULL;
  int open_flags = O_EXCL|O_CREAT|O_WRONLY;
  int perm_flags = S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH;
  int lock_flags = 0;
  int rv = get_file_opts(opts,&open_flags,&perm_flags,&lock_flags,&enc,&ec);
  if (rv<0) return KNO_ERROR;
  f = u8_open_output_file(filename,enc,open_flags,0);
  if (f == NULL)
    return kno_err("CantWriteFile","OPEN-OUTPUT-FILE",NULL,fname);
  if (KNO_CHARACTERP(ec)) {
    int escape = KNO_CHAR2CODE(ec);
    f->u8_xescape = escape;}
  return make_port(NULL,(u8_output)f,u8_strdup(filename));
}

DEFCPRIM("extend-output-file",extend_output_file,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "returns an output port, writing to the end of the "
	 "text file *filename* using *encoding*. If "
	 "*escape* is specified, it can be the character & "
	 "or \\, which causes special characters to be "
	 "output as either entity escaped or unicode "
	 "escaped sequences.",
	 {"fname",kno_string_type,KNO_VOID},
	 {"opts",kno_any_type,KNO_VOID},
	 {"ec",kno_character_type,KNO_VOID})
static lispval extend_output_file(lispval fname,lispval opts,lispval ec)
{
  struct U8_XOUTPUT *f;
  u8_string filename = kno_strdata(fname);
  u8_encoding enc = NULL;
  int open_flags = O_APPEND|O_CREAT|O_WRONLY;
  int perm_flags = S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH;
  int lock_flags = 0;
  int rv = get_file_opts(opts,&open_flags,&perm_flags,&lock_flags,&enc,&ec);
  if (rv<0) return KNO_ERROR;
  f = u8_open_output_file(filename,enc,open_flags,0);
  if (f == NULL)
    return kno_err(u8_CantOpenFile,"EXTEND-OUTPUT-FILE",NULL,fname);
  if (KNO_CHARACTERP(ec)) {
    int escape = KNO_CHAR2CODE(ec);
    f->u8_xescape = escape;}
  return make_port(NULL,(u8_output)f,u8_strdup(filename));
}

DEFCPRIM("open-input-file",open_input_file,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "returns an input port for the text file "
	 "*filename*, translating from *encoding*. If "
	 "*encoding* is not specified, the file is opened "
	 "as a UTF-8 file.",
	 {"fname",kno_string_type,KNO_VOID},
	 {"opts",kno_any_type,KNO_VOID})
static lispval open_input_file(lispval fname,lispval opts)
{
  struct U8_XINPUT *f;
  u8_string filename = kno_strdata(fname);
  u8_encoding enc = NULL;
  int open_flags = O_RDONLY;
  int lock_flags = 0;
  int rv = get_file_opts(opts,&open_flags,NULL,&lock_flags,&enc,NULL);
  if (rv<0) return KNO_ERROR;
  f = u8_open_input_file(filename,enc,open_flags,0);
  if (f == NULL)
    return kno_err(u8_CantOpenFile,"OPEN-INPUT-FILE",NULL,fname);
  else return make_port((u8_input)f,NULL,u8_strdup(filename));
}

DEFCPRIM("write-file",writefile_prim,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "Writes *object* to *filename*. If *object* is a packet it is "
	 "written directly; if *object* is a string, it is converted to "
	 "the text encoding named *enc*; any other *object* signals an error.",
	 {"filename",kno_string_type,KNO_VOID},
	 {"object",kno_any_type,KNO_VOID},
	 {"enc",kno_any_type,KNO_VOID})
static lispval writefile_prim(lispval filename,lispval object,lispval enc)
{
  int len = 0; const unsigned char *bytes; int free_bytes = 0;
  if (STRINGP(object)) {
    bytes = CSTRING(object);
    len = STRLEN(object);}
  else if (PACKETP(object)) {
    bytes = KNO_PACKET_DATA(object);
    len = KNO_PACKET_LENGTH(object);}
  else {
    struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,1024);
    kno_unparse(&out,object);
    bytes = out.u8_outbuf; len = out.u8_write-out.u8_outbuf;
    free_bytes = 1;}
  if ((FALSEP(enc)) || (VOIDP(enc))) {
    FILE *f = u8_fopen(CSTRING(filename),"w");
    size_t off = 0, to_write = len;
    if (f == NULL) {
      if (free_bytes) u8_free(bytes);
      return kno_err(OpenFailed,"writefile_prim",NULL,filename);}
    while (to_write>0) {
      ssize_t n_bytes = fwrite(bytes+off,1,to_write,f);
      if (n_bytes<0) {
	u8_graberr(errno,"writefile_prim",u8_strdup(CSTRING(filename)));
	return KNO_ERROR;}
      else {
	to_write = to_write-n_bytes;
	off = off+n_bytes;}}
    fclose(f);}
  else if ((KNO_TRUEP(enc)) || (STRINGP(enc))) {
    struct U8_TEXT_ENCODING *encoding=
      ((STRINGP(enc)) ? (u8_get_encoding(CSTRING(enc))) :
       (u8_get_default_encoding()));
    if (encoding == NULL) {
      if (free_bytes) u8_free(bytes);
      return kno_type_error("encoding","writefile_prim",enc);}
    else {
      U8_XOUTPUT *out = u8_open_output_file(CSTRING(filename),encoding,-1,-1);
      if (out == NULL) {
	if (free_bytes) u8_free(bytes);
	return kno_reterr(OpenFailed,"writefile_prim",NULL,filename);}
      u8_putn((u8_output)out,bytes,len);
      u8_close((u8_stream)out);}}
  else {
    if (free_bytes) u8_free(bytes);
    return kno_type_error("encoding","writefile_prim",enc);}
  if (free_bytes) u8_free(bytes);
  return KNO_INT(len);
}

/* FILEOUT */

static int printout_helper(U8_OUTPUT *out,lispval x)
{
  if (KNO_ABORTP(x)) return 0;
  else if (VOIDP(x)) return 1;
  if (out == NULL) out = u8_current_output;
  if (STRINGP(x))
    u8_printf(out,"%s",CSTRING(x));
  else u8_printf(out,"%q",x);
  return 1;
}

static lispval simple_fileout_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval filename_arg = kno_get_arg(expr,1);
  lispval filename_val = kno_eval_arg(filename_arg,env);
  U8_OUTPUT *f, *oldf = NULL; int doclose;
  if (KNO_ABORTP(filename_val)) return filename_val;
  else if (KNO_PORTP(filename_val)) {
    KNO_PORT *port = kno_consptr(KNO_PORT *,filename_val,kno_ioport_type);
    if (port->port_output) {
      f = port->port_output;
      doclose = 0;}
    else {
      kno_decref(filename_val);
      return kno_type_error(_("output port"),"simple_fileout",filename_val);}}
  else if (STRINGP(filename_val)) {
    f = (u8_output)u8_open_output_file(CSTRING(filename_val),NULL,0,0);
    if (f == NULL) {
      kno_decref(filename_val);
      return kno_err(u8_CantOpenFile,"FILEOUT",NULL,filename_val);}
    doclose = 1;}
  else {
    kno_decref(filename_val);
    return kno_type_error(_("string"),"simple_fileout",filename_val);}
  oldf = u8_current_output;
  u8_set_default_output(f);
  {lispval body = kno_get_body(expr,2);
    KNO_DOLIST(ex,body)  {
      lispval value = kno_eval(ex,env,_stack,0);
      if (printout_helper(f,value))
	kno_decref(value);
      else {
	u8_set_default_output(oldf);
	kno_decref(filename_val);
	if (doclose) u8_close_output(f);
	else u8_flush(f);
	return value;}}}
  u8_set_default_output(oldf);
  if (doclose) u8_close_output(f);
  else u8_flush(f);
  kno_decref(filename_val);
  return VOID;
}

/* Not really I/O but related structurally and logically */

static lispval simple_system_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  struct U8_OUTPUT out; int result;
  U8_INIT_OUTPUT(&out,256);
  {lispval string_exprs = kno_get_body(expr,1);
    KNO_DOLIST(string_expr,string_exprs) {
      lispval value = kno_eval(string_expr,env,_stack,0);
      if (KNO_ABORTP(value)) return value;
      else if (VOIDP(value)) continue;
      else if (STRINGP(value))
	u8_printf(&out,"%s",CSTRING(value));
      else u8_printf(&out,"%q",value);
      kno_decref(value);}}
  result = system(out.u8_outbuf); u8_free(out.u8_outbuf);
  return KNO_INT(result);
}

/* Opening TCP sockets */

static lispval noblock_symbol, nodelay_symbol;

DEFCPRIM("open-socket",open_socket_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Opens a TCP socket providing text/io to the "
	 "address specified by *spec*",
	 {"spec",kno_string_type,KNO_VOID},
	 {"opts",kno_any_type,KNO_VOID})
static lispval open_socket_prim(lispval spec,lispval opts)
{

  u8_socket conn = u8_connect(CSTRING(spec));
  if (conn<0) return KNO_ERROR;
  else {
    lispval noblock = kno_getopt(opts,noblock_symbol,KNO_FALSE);
    lispval nodelay = kno_getopt(opts,nodelay_symbol,KNO_FALSE);
    u8_xinput in = u8_open_xinput(conn,NULL);
    u8_xoutput out = u8_open_xoutput(conn,NULL);
    if ( (in == NULL) || (out == NULL) ) {
      close(conn);
      return KNO_ERROR_VALUE;}
    if (!(FALSEP(noblock))) u8_set_blocking(conn,0);
    if (!(FALSEP(nodelay))) u8_set_nodelay(conn,1);
    return make_port((u8_input)in,(u8_output)out,u8_strdup(CSTRING(spec)));}
}

/* More file manipulation */

DEFCPRIM("remove-file!",remove_file_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Removes the file named *name*. If *must_exist* is provided "
	 "and true, this signals an error if a file named *name* "
	 "doesn't exist.",
	 {"filename",kno_string_type,KNO_VOID},
	 {"must_exist",kno_any_type,KNO_VOID})
static lispval remove_file_prim(lispval name,lispval must_exist)
{
  u8_string filename = CSTRING(name);
  if ((u8_file_existsp(filename))||(u8_symlinkp(filename))) {
    if (u8_removefile(CSTRING(name))<0) {
      lispval err = kno_err(RemoveFailed,"remove_file_prim",filename,name);
      return err;}
    else return KNO_TRUE;}
  else if (KNO_TRUEP(must_exist)) {
    u8_string absolute = u8_abspath(filename,NULL);
    lispval err = kno_err(kno_NoSuchFile,"remove_file_prim",absolute,name);
    u8_free(absolute);
    return err;}
  else return KNO_FALSE;
}

DEFCPRIM("remove-tree!",remove_tree_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "Removes the file system tree rooted at *root*. If *must_exist* "
	 "is provided and true, this signals an error if a file named *name* "
	 "doesn't exist.",
	 {"root",kno_string_type,KNO_VOID},
	 {"must_exist",kno_any_type,KNO_VOID})
static lispval remove_tree_prim(lispval root,lispval must_exist)
{
  u8_string filename = CSTRING(root);
  if (u8_directoryp(filename))
    if (u8_rmtree(CSTRING(root))<0) {
      lispval err = kno_err(RemoveFailed,"remove_tree_prim",filename,root);
      return err;}
    else return KNO_TRUE;
  else if (KNO_TRUEP(must_exist)) {
    u8_string absolute = u8_abspath(filename,NULL);
    lispval err = kno_err(kno_NoSuchFile,"remove_tree_prim",absolute,root);
    u8_free(absolute);
    return err;}
  else return KNO_FALSE;
}

DEFCPRIM("move-file!",move_file_prim,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "Moves the file *from* to a new location, *to*.",
	 {"from",kno_string_type,KNO_VOID},
	 {"to",kno_string_type,KNO_VOID},
	 {"must_exist",kno_any_type,KNO_VOID})
static lispval move_file_prim(lispval from,lispval to,lispval must_exist)
{
  u8_string fromname = CSTRING(from);
  if (u8_file_existsp(fromname))
    if (u8_movefile(CSTRING(from),CSTRING(to))<0) {
      lispval err = kno_err(RemoveFailed,"move_file_prim",fromname,to);
      return err;}
    else return KNO_TRUE;
  else if (KNO_TRUEP(must_exist))
    return kno_err(kno_NoSuchFile,"move_file_prim",NULL,from);
  else return KNO_FALSE;
}

DEFCPRIM("link-file!",link_file_prim,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "Creates a symbolic link to *from* located at *to*",
	 {"from",kno_string_type,KNO_VOID},
	 {"to",kno_string_type,KNO_VOID},
	 {"must_exist",kno_any_type,KNO_VOID})
static lispval link_file_prim(lispval from,lispval to,lispval must_exist)
{
  u8_string fromname = CSTRING(from);
  if (u8_file_existsp(fromname))
    if (u8_linkfile(CSTRING(from),CSTRING(to))<0) {
      lispval err = kno_err(LinkFailed,"link_file_prim",fromname,to);
      return err;}
    else return KNO_TRUE;
  else if (KNO_TRUEP(must_exist))
    return kno_err(kno_NoSuchFile,"link_file_prim",NULL,from);
  else return KNO_FALSE;
}

/* FILESTRING */

static u8_string get_filestring(u8_string path,u8_string encname)
{
  if (u8_file_existsp(path))
    return u8_filestring(path,encname);
  else {
    const unsigned char *data = kno_get_source(path,encname,NULL,NULL,NULL);
    if (data == NULL) kno_seterr3(kno_FileNotFound,"filestring",path);
    return data;}
}

DEFCPRIM("filestring",filestring_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(FILESTRING *file* [*encoding*]) "
	 "returns the contents of a text file. The "
	 "*encoding*, if provided, specifies the character "
	 "encoding, which defaults to UTF-8",
	 {"filename",kno_string_type,KNO_VOID},
	 {"enc",kno_any_type,KNO_VOID})
static lispval filestring_prim(lispval filename,lispval enc)
{
  if ((VOIDP(enc))||(FALSEP(enc))) {
    u8_string data = get_filestring(CSTRING(filename),"UTF-8");
    if (data)
      return kno_wrapstring(data);
    else return KNO_ERROR;}
  else if (KNO_TRUEP(enc)) {
    u8_string data = get_filestring(CSTRING(filename),"auto");
    if (data) return kno_wrapstring(data);
    else return KNO_ERROR;}
  else if (STRINGP(enc)) {
    u8_string data = get_filestring(CSTRING(filename),CSTRING(enc));
    if (data)
      return kno_wrapstring(data);
    else return KNO_ERROR;}
  else return kno_err(kno_UnknownEncoding,"FILESTRING",NULL,enc);
}

static u8_string get_filedata(u8_string path,ssize_t *lenp)
{
  if (u8_file_existsp(path))
    return u8_filedata(path,lenp);
  else {
    const unsigned char *data = kno_get_source(path,"bytes",NULL,NULL,lenp);
    if (data == NULL) kno_seterr3(kno_FileNotFound,"filestring",path);
    return data;}
}

DEFCPRIM("filedata",filedata_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(FILEDATA *file*) "
	 "returns the contents of *file* as a packet.",
	 {"filename",kno_string_type,KNO_VOID})
static lispval filedata_prim(lispval filename)
{
  ssize_t len = -1;
  const unsigned char *data = get_filedata(CSTRING(filename),&len);
  if (len>=0) return kno_init_packet(NULL,len,data);
  else return KNO_ERROR;
}

DEFCPRIM("filecontent",filecontent_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the contents of a named file, trying to "
	 "be intelligent about returning a string or packet "
	 "depending on the probably file type",
	 {"filename",kno_string_type,KNO_VOID})
static lispval filecontent_prim(lispval filename)
{
  ssize_t len = -1;
  const unsigned char *data = get_filedata(CSTRING(filename),&len);
  if (len>=0) {
    lispval result;
    if (u8_validp(data))
      result = kno_make_string(NULL,len,data);
    else result = kno_make_packet(NULL,len,data);
    u8_free(data);
    return result;}
  else return KNO_ERROR;
}

/* File information */

DEFCPRIM("file-exists?",file_existsp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval file_existsp(lispval arg)
{
  if (u8_file_existsp(CSTRING(arg)))
    return KNO_TRUE;
  else if ( (strchr(CSTRING(arg),':')) ||
	    (strstr(CSTRING(arg),".zip/")) ) {
    if (kno_probe_source(CSTRING(arg),NULL,NULL,NULL))
      return KNO_TRUE;
    else return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFCPRIM("file-regular?",file_regularp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval file_regularp(lispval arg)
{
  if (! (u8_file_existsp(CSTRING(arg))) )
    return KNO_FALSE;
  else if (u8_directoryp(CSTRING(arg)))
    return KNO_FALSE;
  else if (u8_socketp(CSTRING(arg)))
    return KNO_FALSE;
  else return KNO_TRUE;
}

DEFCPRIM("file-readable?",file_readablep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval file_readablep(lispval arg)
{
  if (u8_file_readablep(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("file-writable?",file_writablep,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval file_writablep(lispval arg)
{
  if (u8_file_writablep(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("file-directory?",file_directoryp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval file_directoryp(lispval arg)
{
  if (u8_directoryp(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("file-symlink?",file_symlinkp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval file_symlinkp(lispval arg)
{
  if (u8_symlinkp(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("file-socket?",file_socketp,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval file_socketp(lispval arg)
{
  if (u8_socketp(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("abspath",file_abspath,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID},
	 {"wd",kno_string_type,KNO_VOID})
static lispval file_abspath(lispval arg,lispval wd)
{
  u8_string result;
  if (VOIDP(wd))
    result = u8_abspath(CSTRING(arg),NULL);
  else result = u8_abspath(CSTRING(arg),CSTRING(wd));
  if (result) return kno_wrapstring(result);
  else return KNO_ERROR;
}

DEFCPRIM("realpath",file_realpath,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID},
	 {"wd",kno_string_type,KNO_VOID})
static lispval file_realpath(lispval arg,lispval wd)
{
  u8_string result;
  if (VOIDP(wd))
    result = u8_realpath(CSTRING(arg),NULL);
  else result = u8_realpath(CSTRING(arg),CSTRING(wd));
  if (result) return kno_wrapstring(result);
  else return KNO_ERROR;
}

DEFCPRIM("readlink",file_readlink,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID},
	 {"abs",kno_any_type,KNO_VOID},
	 {"err",kno_any_type,KNO_VOID})
static lispval file_readlink(lispval arg,lispval abs,lispval err)
{
  u8_string result;
  if (FALSEP(abs))
    result = u8_getlink(CSTRING(arg),0);
  else result = u8_getlink(CSTRING(arg),1);
  if (result) return kno_wrapstring(result);
  else if (KNO_TRUEP(err))
    return KNO_ERROR;
  else {
    u8_clear_errors(0);
    return KNO_FALSE;}
}

DEFCPRIM("path-basename",path_basename,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID},
	 {"suffix",kno_any_type,KNO_VOID})
static lispval path_basename(lispval arg,lispval suffix)
{
  if ((VOIDP(suffix)) || (FALSEP(suffix)))
    return kno_wrapstring(u8_basename(CSTRING(arg),NULL));
  else if (STRINGP(suffix))
    return kno_wrapstring(u8_basename(CSTRING(arg),CSTRING(suffix)));
  else return kno_wrapstring(u8_basename(CSTRING(arg),"*"));
}

DEFCPRIM("path-suffix",path_suffix,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID},
	 {"dflt",kno_any_type,KNO_VOID})
static lispval path_suffix(lispval arg,lispval dflt)
{
  u8_string s = CSTRING(arg);
  u8_string slash = strrchr(s,'/');
  u8_string dot = strrchr(s,'.');
  if ((dot)&&((!(slash))||(slash<dot)))
    return kno_mkstring(dot);
  else if (VOIDP(dflt))
    return kno_mkstring("");
  else return kno_incref(dflt);
}

DEFCPRIM("path-dirname",path_dirname,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval path_dirname(lispval arg)
{
  return kno_wrapstring(u8_dirname(CSTRING(arg)));
}

DEFCPRIM("path-location",path_location,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval path_location(lispval arg)
{
  u8_string path = CSTRING(arg);
  u8_string slash = strrchr(path,'/');
  if (slash[1]=='\0') return kno_incref(arg);
  else return kno_substring(path,slash+1);
}

DEFCPRIM("mkpath",mkpath_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "**undocumented**",
	 {"dirname",kno_any_type,KNO_VOID},
	 {"name",kno_string_type,KNO_VOID})
static lispval mkpath_prim(lispval dirname,lispval name)
{
  lispval config_val = VOID; u8_string dir = NULL, namestring = NULL;
  if (!(STRINGP(name)))
    return kno_type_error(_("string"),"mkpath_prim",name);
  else namestring = CSTRING(name);
  if (*namestring=='/') return kno_incref(name);
  else if ((STRINGP(dirname))&&(STRLEN(dirname)==0))
    return kno_incref(name);
  else if (STRINGP(dirname)) dir = CSTRING(dirname);
  else if (SYMBOLP(dirname)) {
    config_val = kno_config_get(SYM_NAME(dirname));
    if (STRINGP(config_val)) dir = CSTRING(config_val);
    else {
      kno_decref(config_val);
      return kno_type_error(_("string CONFIG var"),"mkpath_prim",dirname);}}
  else return kno_type_error
	 (_("string or string CONFIG var"),"mkpath_prim",dirname);
  if (VOIDP(config_val))
    return kno_wrapstring(u8_mkpath(dir,namestring));
  else {
    lispval result = kno_wrapstring(u8_mkpath(dir,namestring));
    kno_decref(config_val);
    return result;}
}

/* Getting the runbase for a script file */

DEFCPRIM("runfile",runfile_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"suffix",kno_string_type,KNO_VOID})
static lispval runfile_prim(lispval suffix)
{
  return kno_wrapstring(kno_runbase_filename(CSTRING(suffix)));
}

/* Making directories */

DEFCPRIM("mkdir",mkdir_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"dirname",kno_string_type,KNO_VOID},
	 {"mode_arg",kno_fixnum_type,KNO_VOID})
static lispval mkdir_prim(lispval dirname,lispval mode_arg)
{
  mode_t mode=
    ((KNO_UINTP(mode_arg))?((mode_t)(FIX2INT(mode_arg))):((mode_t)0777));
  int retval = u8_mkdir(CSTRING(dirname),mode);
  if (retval<0)
    return KNO_ERROR;
  else if (retval) {
    /* Force the mode to be set if provided */
    if (KNO_UINTP(mode_arg))
      u8_chmod(CSTRING(dirname),((mode_t)(FIX2INT(mode_arg))));
    return KNO_TRUE;}
  else return KNO_FALSE;
}

DEFCPRIM("rmdir",rmdir_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"dirname",kno_string_type,KNO_VOID})
static lispval rmdir_prim(lispval dirname)
{
  int retval = u8_rmdir(CSTRING(dirname));
  if (retval<0) {
    u8_condition cond = u8_strerror(errno); errno = 0;
    return kno_err(cond,"rmdir_prim",NULL,dirname);}
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFCPRIM("mkdirs",mkdirs_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"pathname",kno_string_type,KNO_VOID},
	 {"mode_arg",kno_fixnum_type,KNO_VOID})
static lispval mkdirs_prim(lispval pathname,lispval mode_arg)
{
  mode_t mode=
    ((KNO_UINTP(mode_arg))?((mode_t)(FIX2INT(mode_arg))):((mode_t)-1));
  int retval = u8_mkdirs(CSTRING(pathname),mode);
  if (retval<0) {
    u8_condition cond = u8_strerror(errno); errno = 0;
    return kno_err(cond,"mkdirs_prim",NULL,pathname);}
  else if (retval==0) return KNO_FALSE;
  else return KNO_INT(retval);
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
    return kno_mkstring(tempdir_template);
  else {
    char *tmpdir = get_tmpdir();
    u8_string tmp = u8_mkpath(tmpdir,"knotempXXXXXX");
    lispval result = kno_mkstring(tmp);
    u8_free(tmp);
    return result;}
}

static int temproot_set(lispval sym,lispval value,void *ignore)
{
  u8_string template, old_template = tempdir_template;
  if (!(STRINGP(value))) {
    kno_type_error("string","temproot_set",value);
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
    lispval ctemp = kno_config_get(CSTRING(template_arg));
    if (STRINGP(ctemp))
      template = consed = u8_strdup(CSTRING(ctemp));
    kno_decref(ctemp);}
  if (!(template)) template = tempdir_template;
  if (!(template)) {
    char *tmpdir = get_tmpdir();
    template = consed = u8_mkpath(tmpdir,"knotempXXXXXX");}
  /* Unlike mkdtemp, u8_tempdir doesn't overwrite its argument */
  tempname = u8_tempdir(template);
  if (tempname) {
    if (!(keep)) {
      lispval result = kno_make_string(NULL,-1,tempname);
      u8_lock_mutex(&tempdirs_lock);
      CHOICE_ADD(tempdirs,result);
      u8_unlock_mutex(&tempdirs_lock);}
    if (consed) u8_free(consed);
    return tempname;}
  else {
    u8_condition cond = u8_strerror(errno); errno = 0;
    if (consed) u8_free(consed);
    kno_seterr(cond,"tempdir_prim",template,kno_incref(template_arg));
    return NULL;}
}

static void remove_tempdirs()
{
  int n_files = KNO_CHOICE_SIZE(tempdirs);
  int n_keep = KNO_CHOICE_SIZE(keeptemp);
  if ((n_files)&&(n_keep))
    u8_log(LOG_NOTICE,"TEMPFILES","Removing %d=%d-%d temporary directories",
	   n_files-n_keep,n_files,n_keep);
  else if (n_files)
    u8_log(LOG_NOTICE,"TEMPFILES","Removing %d temporary directories",n_files);
  else return;
  u8_lock_mutex(&tempdirs_lock); {
    lispval to_remove = kno_difference(tempdirs,keeptemp);
    DO_CHOICES(tmpfile,to_remove) {
      if (STRINGP(tmpfile)) {
	u8_log(LOG_DEBUG,"TEMPFILES","Removing directory %s",
	       CSTRING(tmpfile));
	if (u8_directoryp(CSTRING(tmpfile)))
	  u8_rmtree(CSTRING(tmpfile));}}
    kno_decref(tempdirs); kno_decref(keeptemp); kno_decref(to_remove);
    tempdirs = EMPTY; keeptemp = EMPTY;
    u8_unlock_mutex(&tempdirs_lock);}
}

KNO_EXPORT u8_string kno_tempdir(u8_string spec,int keep)
{
  lispval template_arg = ((spec)?(kno_parse_arg(spec)):(VOID));
  u8_string dirname = tempdir_core(template_arg,keep);
  kno_decref(template_arg);
  return dirname;
}

DEFCPRIM("tempdir",tempdir_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0),
	 "**undocumented**",
	 {"template_arg",kno_any_type,KNO_VOID},
	 {"keep",kno_any_type,KNO_FALSE})
static lispval tempdir_prim(lispval template_arg,lispval keep)
{
  if ((VOIDP(template_arg))||
      (FALSEP(template_arg))||
      (SYMBOLP(template_arg))||
      (STRINGP(template_arg))) {
    u8_string dirname = tempdir_core(template_arg,(!(FALSEP(keep))));
    if (dirname == NULL) return KNO_ERROR;
    else return kno_init_string(NULL,-1,dirname);}
  return kno_type_error("tempdir template (string, symbol, or #f)",
			"tempdir_prim",
			template_arg);
}

DEFCPRIM("tempdir/done",tempdir_done_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0),
	 "**undocumented**",
	 {"tempdir",kno_string_type,KNO_VOID},
	 {"force_arg",kno_any_type,KNO_FALSE})
static lispval tempdir_done_prim(lispval tempdir,lispval force_arg)
{
  int force = (!(FALSEP(force_arg))), doit = 0;
  u8_string dirname = CSTRING(tempdir);
  u8_lock_mutex(&tempdirs_lock); {
    lispval cur_tempdirs = tempdirs; lispval cur_keep = keeptemp;
    if (kno_overlapp(tempdir,cur_tempdirs)) {
      if (kno_overlapp(tempdir,cur_keep)) {
	if (force) {
	  u8_log(LOG_WARN,"Forced temp deletion",
		 "Forcing deletion of directory %s, declared for safekeeping",dirname);
	  keeptemp = kno_difference(cur_keep,tempdir);
	  tempdirs = kno_difference(cur_tempdirs,tempdir);
	  kno_decref(cur_keep); kno_decref(cur_tempdirs);
	  doit = 1;}
	else u8_log(LOG_WARN,_("Keeping temp directory"),
		    "The directory %s is declared for safekeeping, leaving",dirname);}
      else {
	u8_log(LOG_INFO,_("Deleting temp directory"),
	       "Explicitly deleting temporary directory %s",dirname);
	tempdirs = kno_difference(cur_tempdirs,tempdir);
	kno_decref(cur_tempdirs);
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
    if (retval<0) return KNO_ERROR;
    else if (retval==0) return KNO_FALSE;
    else return KNO_INT(retval);}
  else return KNO_FALSE;
}

DEFCPRIM("tempdir?",is_tempdir_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "**undocumented**",
	 {"tempdir",kno_string_type,KNO_VOID})
static lispval is_tempdir_prim(lispval tempdir)
{
  u8_lock_mutex(&tempdirs_lock); {
    lispval cur_tempdirs = tempdirs;
    int found = kno_overlapp(tempdir,cur_tempdirs);
    u8_unlock_mutex(&tempdirs_lock);
    if (found) return KNO_TRUE; else return KNO_FALSE;}
}

static lispval tempdirs_get(lispval sym,void *ignore)
{
  lispval dirs = tempdirs;
  kno_incref(dirs);
  return dirs;
}
static int tempdirs_add(lispval sym,lispval value,void *ignore)
{
  if (!(STRINGP(value))) {
    kno_type_error("string","tempdirs_add",value);
    return -1;}
  else kno_incref(value);
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
    return kno_deep_copy(tempdirs);
  else return kno_deep_copy(keeptemp);
}
static int keepdirs_add(lispval sym,lispval value,void *ignore)
{
  if (KNO_TRUEP(value)) {
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
    kno_type_error("string","keepdirs_add",value);
    return -1;}
  else if ((kno_boolstring(CSTRING(value),-1))>=0) {
    int val = kno_boolstring(CSTRING(value),-1);
    if (val) val = 0; else val = 1;
    if (delete_tempdirs_on_exit == val) return 0;
    else {
      delete_tempdirs_on_exit = val;
      return 1;}}
  else kno_incref(value);
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
  if (delete_tempdirs_on_exit==0) return KNO_TRUE;
  else return KNO_FALSE;
}
static int keeptemp_set(lispval sym,lispval value,void *ignore)
{
  if (KNO_TRUEP(value)) {
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
    kno_type_error("string","keeptemp_add",value);
    return -1;}
  else if ((kno_boolstring(CSTRING(value),-1))>=0) {
    int val = kno_boolstring(CSTRING(value),-1);
    if (val) val = 0; else val = 1;
    if (delete_tempdirs_on_exit == val) return 0;
    else {
      delete_tempdirs_on_exit = val;
      return 1;}}
  else {
    kno_type_error("string","keeptemp_set",value);
    return -1;}
  return 1;
}

/* File time info */

static lispval make_timestamp(time_t tick)
{
  struct U8_XTIME xt; u8_init_xtime(&xt,tick,u8_second,0,0,0);
  return kno_make_timestamp(&xt);
}

DEFCPRIM("file-modtime",file_modtime,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"filename",kno_string_type,KNO_VOID})
static lispval file_modtime(lispval filename)
{
  if (u8_file_existsp(CSTRING(filename))) {
    time_t mtime = u8_file_mtime(CSTRING(filename));
    if (mtime<0) return KNO_ERROR;
    else return make_timestamp(mtime);}
  else if ( (strchr(CSTRING(filename),':')) ||
	    (strstr(CSTRING(filename),".zip/")) ) {
    time_t mtime = -1;
    if (kno_probe_source(CSTRING(filename),NULL,&mtime,NULL))
      return make_timestamp(mtime);}
  else NO_ELSE;
  return kno_err(kno_FileNotFound,"file_modtime",
		 CSTRING(filename),
		 filename);
}

DEFCPRIM("set-file-modtime!",set_file_modtime,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"filename",kno_string_type,KNO_VOID},
	 {"timestamp",kno_any_type,KNO_VOID})
static lispval set_file_modtime(lispval filename,lispval timestamp)
{
  time_t mtime=
    ((VOIDP(timestamp))?(time(NULL)):
     (FIXNUMP(timestamp))?(FIX2INT(timestamp)):
     (KNO_BIGINTP(timestamp))?(kno_getint(timestamp)):
     (TYPEP(timestamp,kno_timestamp_type))?
     (((struct KNO_TIMESTAMP *)timestamp)->u8xtimeval.u8_tick):
     (-1));
  if (mtime<0)
    return kno_type_error("time","set_file_modtime",timestamp);
  else if (u8_set_mtime(CSTRING(filename),mtime)<0)
    return KNO_ERROR;
  else return KNO_TRUE;
}

DEFCPRIM("file-accesstime",file_atime,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"filename",kno_string_type,KNO_VOID})
static lispval file_atime(lispval filename)
{
  time_t mtime = u8_file_atime(CSTRING(filename));
  if (mtime<0) return KNO_ERROR;
  else return make_timestamp(mtime);
}

DEFCPRIM("set-file-atime!",set_file_atime,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"filename",kno_string_type,KNO_VOID},
	 {"timestamp",kno_any_type,KNO_VOID})
static lispval set_file_atime(lispval filename,lispval timestamp)
{
  time_t atime=
    ((VOIDP(timestamp))?(time(NULL)):
     (FIXNUMP(timestamp))?(FIX2INT(timestamp)):
     (KNO_BIGINTP(timestamp))?(kno_getint(timestamp)):
     (TYPEP(timestamp,kno_timestamp_type))?
     (((struct KNO_TIMESTAMP *)timestamp)->u8xtimeval.u8_tick):
     (-1));
  if (atime<0)
    return kno_type_error("time","set_file_atime",timestamp);
  else if (u8_set_atime(CSTRING(filename),atime)<0)
    return KNO_ERROR;
  else return KNO_TRUE;
}

DEFCPRIM("file-creationtime",file_ctime,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"filename",kno_string_type,KNO_VOID})
static lispval file_ctime(lispval filename)
{
  time_t mtime = u8_file_ctime(CSTRING(filename));
  if (mtime<0) return KNO_ERROR;
  else return make_timestamp(mtime);
}

DEFCPRIM("file-mode",file_mode,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"filename",kno_string_type,KNO_VOID})
static lispval file_mode(lispval filename)
{
  int mode = u8_file_mode(CSTRING(filename));
  if (mode<0) return KNO_ERROR;
  else return KNO_INT(mode);
}

DEFCPRIM("file-size",file_size,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"filename",kno_string_type,KNO_VOID})
static lispval file_size(lispval filename)
{
  if (u8_file_existsp(KNO_CSTRING(filename))) {
    ssize_t size = u8_file_size(CSTRING(filename));
    if (size<0) return KNO_ERROR;
    else if (size<KNO_MAX_FIXNUM)
      return KNO_INT(size);
    else return kno_make_bigint(size);}
  else if ( (strchr(CSTRING(filename),':')) ||
	    (strstr(CSTRING(filename),".zip/")) ) {
    ssize_t size = -1;
    if (kno_probe_source(CSTRING(filename),NULL,NULL,&size))
      return KNO_INT(size);}
  else NO_ELSE;
  return kno_err(kno_FileNotFound,"file_modtime",
		 CSTRING(filename),
		 filename);
}

DEFCPRIM("file-owner",file_owner,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"filename",kno_string_type,KNO_VOID})
static lispval file_owner(lispval filename)
{
  u8_string name = u8_file_owner(CSTRING(filename));
  if (name) return kno_wrapstring(name);
  else return KNO_ERROR;
}

DEFCPRIM("set-file-access!",set_file_access_prim,
	 KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "(SET-FILE-ACCESS! *file* [*mode*] [*group*] [*owner*]) "
	 "sets the mode/group/owner of a file",
	 {"filename",kno_string_type,KNO_VOID},
	 {"owner",kno_fixnum_type,KNO_VOID},
	 {"group",kno_any_type,KNO_VOID},
	 {"mode_arg",kno_any_type,KNO_VOID})
static lispval set_file_access_prim(lispval filename,
				    lispval owner,
				    lispval group,
				    lispval mode_arg)
{
  mode_t mode = (KNO_FIXNUMP(mode_arg)) ? (KNO_FIX2INT(mode_arg)) : -1;
  u8_uid uid; u8_gid gid=-1;
  if (KNO_FIXNUMP(owner))
    uid = KNO_FIX2INT(owner);
  else if (KNO_STRINGP(owner)) {
    uid = u8_getuid(KNO_CSTRING(owner));
    if (uid<0) {
      kno_seterr("UnknownUser","set_file_access_prim",
		 u8_strdup(KNO_CSTRING(owner)),
		 VOID);
      return KNO_ERROR;}}
  else return kno_type_error("username","set_file_access_prim",owner);
  if (KNO_FIXNUMP(group))
    gid = KNO_FIX2INT(group);
  else if (KNO_STRINGP(group)) {
    gid = u8_getgid(KNO_CSTRING(group));
    if (gid<0) {
      kno_seterr("UnknownGroup","set_file_access_prim",
		 u8_strdup(KNO_CSTRING(group)),
		 VOID);
      return KNO_ERROR;}}
  else return kno_type_error("groupname","set_file_access_prim",group);
  int rv = u8_set_access_x(KNO_CSTRING(filename),uid,gid,mode);
  if (rv < 0)
    return KNO_ERROR;
  else return KNO_INT(rv);
}

/* Current directory information */

DEFCPRIM("getcwd",getcwd_prim,
	 KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	 "**undocumented**")
static lispval getcwd_prim()
{
  u8_string wd = u8_getcwd();
  if (wd) return kno_wrapstring(wd);
  else return KNO_ERROR;
}

DEFCPRIM("setcwd",setcwd_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"dirname",kno_string_type,KNO_VOID})
static lispval setcwd_prim(lispval dirname)
{
  if (u8_setcwd(CSTRING(dirname))<0)
    return KNO_ERROR;
  else return VOID;
}

/* Directory listings */

DEFCPRIM("getfiles",getfiles_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"dirname",kno_string_type,KNO_VOID},
	 {"fullpath",kno_any_type,KNO_TRUE})
static lispval getfiles_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_getfiles(CSTRING(dirname),(!(FALSEP(fullpath)))), *scan = contents;
  if (contents == NULL)
    return fileprim_error("getfiles_prim",KNO_CSTRING(dirname));
  else while (*scan) {
      lispval string = kno_wrapstring(*scan);
      CHOICE_ADD(results,string);
      scan++;}
  u8_free(contents);
  return results;
}

DEFCPRIM("getdirs",getdirs_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"dirname",kno_string_type,KNO_VOID},
	 {"fullpath",kno_any_type,KNO_TRUE})
static lispval getdirs_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_getdirs(CSTRING(dirname),(!(FALSEP(fullpath)))), *scan = contents;
  if (contents == NULL)
    return fileprim_error("getdirs_prim",KNO_CSTRING(dirname));
  else while (*scan) {
      lispval string = kno_wrapstring(*scan);
      CHOICE_ADD(results,string);
      scan++;}
  u8_free(contents);
  return results;
}

DEFCPRIM("getlinks",getlinks_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"dirname",kno_string_type,KNO_VOID},
	 {"fullpath",kno_any_type,KNO_TRUE})
static lispval getlinks_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_readdir(CSTRING(dirname),U8_LIST_LINKS,(!(FALSEP(fullpath)))),
    *scan = contents;
  if (contents == NULL) return KNO_ERROR;
  else while (*scan) {
      lispval string = kno_wrapstring(*scan);
      CHOICE_ADD(results,string);
      scan++;}
  u8_free(contents);
  return results;
}

DEFCPRIM("readdir",readdir_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"dirname",kno_string_type,KNO_VOID},
	 {"fullpath",kno_any_type,KNO_TRUE})
static lispval readdir_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_readdir(CSTRING(dirname),0,(!(FALSEP(fullpath)))),
    *scan = contents;
  if (contents == NULL) return KNO_ERROR;
  else while (*scan) {
      lispval string = kno_wrapstring(*scan);
      CHOICE_ADD(results,string);
      scan++;}
  u8_free(contents);
  return results;
}

/* File flush function */

DEFCPRIM("close",close_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "**undocumented**",
	 {"portarg",kno_any_type,KNO_VOID})
static lispval close_prim(lispval portarg)
{
  if (TYPEP(portarg,kno_stream_type)) {
    struct KNO_STREAM *dts=
      kno_consptr(struct KNO_STREAM *,portarg,kno_stream_type);
    kno_close_stream(dts,0);
    return VOID;}
  else if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
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
  else return kno_type_error("port","close_prim",portarg);
}

DEFCPRIM("flush-output",flush_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "**undocumented**",
	 {"portarg",kno_any_type,KNO_VOID})
static lispval flush_prim(lispval portarg)
{
  if ((VOIDP(portarg))||
      (FALSEP(portarg))||
      (KNO_TRUEP(portarg))) {
    u8_flush_xoutput(&u8stdout);
    u8_flush_xoutput(&u8stderr);
    return VOID;}
  else if (TYPEP(portarg,kno_stream_type)) {
    struct KNO_STREAM *dts=
      kno_consptr(struct KNO_STREAM *,portarg,kno_stream_type);
    kno_flush_stream(dts);
    return VOID;}
  else if (TYPEP(portarg,kno_ioport_type)) {
    U8_OUTPUT *out = get_output_port(portarg);
    u8_flush(out);
    if (out->u8_streaminfo&U8_STREAM_OWNS_SOCKET) {
      U8_XOUTPUT *xout = (U8_XOUTPUT *)out;
      fsync(xout->u8_xfd);}
    return VOID;}
  else return kno_type_error(_("port or stream"),"flush_prim",portarg);
}

DEFCPRIM("setbuf!",setbuf_prim,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "sets the input and output buffer sizes for a port "
	 "or stream.",
	 {"portarg",kno_any_type,KNO_VOID},
	 {"insize",kno_any_type,KNO_FALSE},
	 {"outsize",kno_any_type,KNO_FALSE})
static lispval setbuf_prim(lispval portarg,lispval insize,lispval outsize)
{
  if (TYPEP(portarg,kno_stream_type)) {
    struct KNO_STREAM *dts=
      kno_consptr(struct KNO_STREAM *,portarg,kno_stream_type);
    kno_setbufsize(dts,FIX2INT(insize));
    return VOID;}
  else if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
    if (FIXNUMP(insize)) {
      U8_INPUT *in = p->port_input;
      if ((in) && (in->u8_streaminfo&U8_STREAM_OWNS_XBUF)) {
	u8_xinput_setbuf((struct U8_XINPUT *)in,FIX2INT(insize));}}

    if (FIXNUMP(outsize)) {
      U8_OUTPUT *out = p->port_output;
      if ((out) && (out->u8_streaminfo&U8_STREAM_OWNS_XBUF)) {
	u8_xoutput_setbuf((struct U8_XOUTPUT *)out,FIX2INT(outsize));}}
    return VOID;}
  else return kno_type_error("port/stream","setbuf_prim",portarg);
}

DEFCPRIM("getpos",getpos_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"portarg",kno_any_type,KNO_VOID})
static lispval getpos_prim(lispval portarg)
{
  if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
    kno_off_t result = -1;
    if (p->port_input)
      result = u8_getpos((struct U8_STREAM *)(p->port_input));
    else if (p->port_output)
      result = u8_getpos((struct U8_STREAM *)(p->port_output));
    else return kno_type_error(_("port"),"getpos_prim",portarg);
    if (result<0)
      return KNO_ERROR;
    else if (result<KNO_MAX_FIXNUM)
      return KNO_INT(result);
    else return kno_make_bigint(result);}
  else if (TYPEP(portarg,kno_stream_type)) {
    kno_stream ds = kno_consptr(kno_stream,portarg,kno_stream_type);
    kno_off_t pos = kno_getpos(ds);
    if (pos<0) return KNO_ERROR;
    else if (pos<KNO_MAX_FIXNUM) return KNO_INT(pos);
    else return kno_make_bigint(pos);}
  else return kno_type_error("port or stream","getpos_prim",portarg);
}

DEFCPRIM("endpos",endpos_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"portarg",kno_any_type,KNO_VOID})
static lispval endpos_prim(lispval portarg)
{
  if (KNO_PORTP(portarg)) {
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
    kno_off_t result = -1;
    if (p->port_input)
      result = u8_endpos((struct U8_STREAM *)(p->port_input));
    else if (p->port_output)
      result = u8_endpos((struct U8_STREAM *)(p->port_output));
    else return kno_type_error(_("port"),"getpos_prim",portarg);
    if (result<0)
      return KNO_ERROR;
    else if (result<KNO_MAX_FIXNUM)
      return KNO_INT(result);
    else return kno_make_bigint(result);}
  else if (TYPEP(portarg,kno_stream_type)) {
    kno_stream ds = kno_consptr(kno_stream,portarg,kno_stream_type);
    kno_off_t pos = kno_endpos(ds);
    if (pos<0) return KNO_ERROR;
    else if (pos<KNO_MAX_FIXNUM) return KNO_INT(pos);
    else return kno_make_bigint(pos);}
  else return kno_type_error("port or stream","endpos_prim",portarg);
}

DEFCPRIM("file%",file_progress_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"portarg",kno_ioport_type,KNO_VOID})
static lispval file_progress_prim(lispval portarg)
{
  double result = -1.0;
  struct KNO_PORT *p=
    kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
  if (p->port_input)
    result = u8_getprogress((struct U8_STREAM *)(p->port_input));
  else if (p->port_output)
    result = u8_getprogress((struct U8_STREAM *)(p->port_output));
  else return kno_type_error(_("port"),"file_progress_prim",portarg);
  if (result<0)
    return KNO_ERROR;
  else return kno_init_double(NULL,result);
}

DEFCPRIM("setpos!",setpos_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "**undocumented**",
	 {"portarg",kno_any_type,KNO_VOID},
	 {"off_arg",kno_any_type,KNO_VOID})
static lispval setpos_prim(lispval portarg,lispval off_arg)
{
  if (KNO_PORTP(portarg)) {
    kno_off_t off, result;
    struct KNO_PORT *p=
      kno_consptr(struct KNO_PORT *,portarg,kno_ioport_type);
    if (FIXNUMP(off_arg)) off = FIX2INT(off_arg);
    else if (KNO_BIGINTP(off_arg))
#if (_FILE_OFFSET_BITS==64)
      off = (kno_off_t)kno_bigint_to_long_long((kno_bigint)off_arg);
#else
    off = (kno_off_t)kno_bigint_to_long((kno_bigint)off_arg);
#endif
    else return kno_type_error(_("offset"),"setpos_prim",off_arg);
    if (p->port_input)
      result = u8_setpos((struct U8_STREAM *)(p->port_input),off);
    else if (p->port_output)
      result = u8_setpos((struct U8_STREAM *)(p->port_output),off);
    else return kno_type_error(_("port"),"setpos_prim",portarg);
    if (result<0)
      return KNO_ERROR;
    else if (result<KNO_MAX_FIXNUM)
      return KNO_INT(off);
    else return kno_make_bigint(result);}
  else if (TYPEP(portarg,kno_stream_type)) {
    kno_stream ds = kno_consptr(kno_stream,portarg,kno_stream_type);
    kno_off_t off, result;
    if (FIXNUMP(off_arg)) off = FIX2INT(off_arg);
    else if (KNO_BIGINTP(off_arg))
#if (_FILE_OFFSET_BITS==64)
      off = (kno_off_t)kno_bigint_to_long_long((kno_bigint)off_arg);
#else
    off = (kno_off_t)kno_bigint_to_long((kno_bigint)off_arg);
#endif
    else return kno_type_error(_("offset"),"setpos_prim",off_arg);
    result = kno_setpos(ds,off);
    if (result<0) return KNO_ERROR;
    else if (result<KNO_MAX_FIXNUM) return KNO_INT(result);
    else return kno_make_bigint(result);}
  else return kno_type_error("port or stream","setpos_prim",portarg);
}

/* File system info */

#if (((HAVE_SYS_STATFS_H)||(HAVE_SYS_VSTAT_H))&&(HAVE_STATFS))
static void statfs_set(lispval,u8_string,long long int,long long int);

DEFCPRIM("fsinfo",fsinfo_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
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
    return KNO_ERROR;}
  else {
    lispval result = kno_init_slotmap(NULL,0,NULL);
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
  lispval slotid = kno_intern(name);
  lispval lval = KNO_INT2LISP(val);
  if (mul!=1) {
    lispval mulval = KNO_INT2LISP(mul);
    lispval rval = kno_multiply(lval,mulval);
    kno_decref(mulval); kno_decref(lval);
    lval = rval;}
  kno_store(r,slotid,lval);
  kno_decref(lval);
}
#else

DEFCPRIM("fsinfo",fsinfo_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"arg",kno_string_type,KNO_VOID})
static lispval fsinfo_prim(lispval arg)
{
  return kno_err("statfs unavailable","fsinfo_prim",NULL,VOID);
}
#endif

/* Snapshot save and restore */

/* A snapshot is a set of variable bindings and CONFIG settings which
   can be saved to and restored from a disk file.  */

static u8_condition SnapshotTrouble=_("SNAPSHOT");

static lispval snapshotvars, snapshotconfig, snapshotfile, configinfo;

KNO_EXPORT
/* kno_snapshot:
   Arguments: an environment pointer and a filename (u8_string)
   Returns: an int (<0 on error)
   Saves a snapshot of the environment into the designated file.
*/
int kno_snapshot(kno_lexenv env,u8_string filename)
{
  lispval vars = kno_symeval(snapshotvars,env);
  lispval configvars = kno_symeval(snapshotconfig,env);
  if ((EMPTYP(vars)) && (EMPTYP(configvars))) {
    u8_message("No snapshot information to save");
    return VOID;}
  else {
    struct KNO_STREAM *out; int bytes;
    lispval slotmap = (lispval)kno_empty_slotmap();
    if (VOIDP(vars)) vars = EMPTY;
    if (VOIDP(configvars)) configvars = EMPTY;
    {DO_CHOICES(sym,vars)
	if (SYMBOLP(sym)) {
	  lispval val = kno_symeval(sym,env);
	  if (VOIDP(val))
	    u8_log(LOG_WARN,SnapshotTrouble,
		   "The snapshot variable %q is unbound",sym);
	  else kno_add(slotmap,sym,val);
	  kno_decref(val);}
	else {
	  kno_decref(slotmap);
	  return kno_type_error("symbol","kno_snapshot",sym);}}
    {DO_CHOICES(sym,configvars)
	if (SYMBOLP(sym)) {
	  lispval val = kno_config_get(SYM_NAME(sym));
	  lispval config_entry = kno_conspair(sym,val);
	  if (VOIDP(val))
	    u8_log(LOG_WARN,SnapshotTrouble,
		   "The snapshot config %q is not set",sym);
	  else kno_add(slotmap,configinfo,config_entry);
	  kno_decref(val);}
	else {
	  kno_decref(slotmap);
	  return kno_type_error("symbol","kno_snapshot",sym);}}
    out = kno_open_file(filename,KNO_FILE_CREATE);
    if (out == NULL) {
      kno_decref(slotmap);
      return -1;}
    else bytes = kno_write_dtype(kno_writebuf(out),slotmap);
    kno_close_stream(out,KNO_STREAM_CLOSE_FULL);
    u8_log(LOG_INFO,SnapshotSaved,
	   "Saved snapshot of %d items to %s",
	   KNO_SLOTMAP_NUSED(slotmap),filename);
    kno_decref(slotmap);
    return bytes;}
}

KNO_EXPORT
/* kno_snapback:
   Arguments: an environment pointer and a filename (u8_string)
   Returns: an int (<0 on error)
   Restores a snapshot from the designated file into the environment
*/
int kno_snapback(kno_lexenv env,u8_string filename)
{
  struct KNO_STREAM *in;
  lispval slotmap; int actions = 0;
  in = kno_open_file(filename,KNO_FILE_READ);
  if (in == NULL) return -1;
  else slotmap = kno_read_dtype(kno_readbuf(in));
  if (KNO_ABORTP(slotmap)) {
    kno_close_stream(in,KNO_STREAM_CLOSE_FULL);
    return slotmap;}
  else if (SLOTMAPP(slotmap)) {
    lispval keys = kno_getkeys(slotmap);
    DO_CHOICES(key,keys) {
      lispval v = kno_get(slotmap,key,VOID);
      if (KNO_EQ(key,configinfo)) {
	DO_CHOICES(config_entry,v)
	  if ((PAIRP(config_entry)) &&
	      (SYMBOLP(KNO_CAR(config_entry))))
	    if (kno_set_config(SYM_NAME(KNO_CAR(config_entry)),
			       KNO_CDR(config_entry)) <0) {
	      kno_decref(v); kno_decref(keys); kno_decref(slotmap);
	      return -1;}
	    else actions++;
	  else {
	    kno_seterr(kno_TypeError,"kno_snapback",
		       "saved config entry",
		       kno_incref(config_entry));
	    kno_decref(v); kno_decref(keys); kno_decref(slotmap);
	    return -1;}}
      else {
	int setval = kno_assign_value(key,v,env);
	if (setval==0)
	  setval = kno_bind_value(key,v,env);
	if (setval<0) {
	  kno_decref(v); kno_decref(keys);
	  return -1;}}
      kno_decref(v);}
    kno_decref(keys);}
  else {
    return kno_reterr(kno_TypeError,"kno_snapback", u8_strdup("slotmap"),
		      slotmap);}
  u8_log(LOG_INFO,SnapshotRestored,"Restored snapshot of %d items from %s",
	 KNO_SLOTMAP_NUSED(slotmap),filename);
  kno_decref(slotmap);
  return actions;
}

static lispval snapshot_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  kno_lexenv save_env; u8_string save_file; int retval = 0;
  lispval arg1 = kno_eval_arg(kno_get_arg(expr,1),env), arg2 = kno_eval_arg(kno_get_arg(expr,2),env);
  if (VOIDP(arg1)) {
    lispval saveto = kno_symeval(snapshotfile,env);
    save_env = env;
    if (STRINGP(saveto))
      save_file = u8_strdup(CSTRING(saveto));
    else save_file = u8_strdup("snapshot");
    kno_decref(saveto);}
  else if (KNO_LEXENVP(arg1)) {
    if (STRINGP(arg2)) save_file = u8_strdup(CSTRING(arg2));
    else save_file = u8_strdup("snapshot");
    save_env = (kno_lexenv)arg1;}
  else if (STRINGP(arg1)) {
    save_file = u8_strdup(CSTRING(arg1));
    if (KNO_LEXENVP(arg2))
      save_env = (kno_lexenv)arg2;
    else save_env = env;}
  else {
    lispval err = kno_type_error("filename","snapshot_prim",arg1);
    kno_decref(arg1); kno_decref(arg2);
    return err;}
  retval = kno_snapshot(save_env,save_file);
  kno_decref(arg1); kno_decref(arg2); u8_free(save_file);
  if (retval<0) return KNO_ERROR;
  else return KNO_INT(retval);
}

static lispval snapback_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  kno_lexenv save_env; u8_string save_file; int retval = 0;
  lispval arg1 = kno_eval_arg(kno_get_arg(expr,1),env), arg2 = kno_eval_arg(kno_get_arg(expr,2),env);
  if (VOIDP(arg1)) {
    lispval saveto = kno_symeval(snapshotfile,env);
    save_env = env;
    if (STRINGP(saveto))
      save_file = u8_strdup(CSTRING(saveto));
    else save_file = u8_strdup("snapshot");
    kno_decref(saveto);}
  else if (KNO_LEXENVP(arg1)) {
    if (STRINGP(arg2)) save_file = u8_strdup(CSTRING(arg2));
    else save_file = u8_strdup("snapshot");
    save_env = (kno_lexenv)arg1;}
  else if (STRINGP(arg1)) {
    save_file = u8_strdup(CSTRING(arg1));
    if (KNO_LEXENVP(arg2))
      save_env = (kno_lexenv)arg2;
    else save_env = env;}
  else {
    lispval err = kno_type_error("filename","snapshot_prim",arg1);
    kno_decref(arg1); kno_decref(arg2);
    return err;}
  retval = kno_snapback(save_env,save_file);
  kno_decref(arg1); kno_decref(arg2); u8_free(save_file);
  if (retval<0) return KNO_ERROR;
  else return KNO_INT(retval);
}

/* Stackdump configuration */

static u8_string stackdump_filename = NULL;
static FILE *stackdump_file = NULL;
static u8_mutex stackdump_lock;

KNO_EXPORT void stackdump_dump(u8_string dump)
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
    return kno_mkstring(stackdump_filename);
  else return KNO_FALSE;
}
static int stackdump_config_set(lispval var,lispval val,void *ignored)
{
  if ((STRINGP(val)) || (FALSEP(val)) || (KNO_TRUEP(val))) {
    u8_string filename = ((STRINGP(val)) ? (CSTRING(val)) :
			  (KNO_TRUEP(val)) ? ((u8_string)"stackdump.log") : (NULL));
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
      /* kno_dump_exception = stackdump_dump; */
    }
    u8_unlock_mutex(&stackdump_lock);
    return 1;}
  else {
    kno_seterr(kno_TypeError,"stackdump_config_set",NULL,val);
    return -1;}
}

/* Close libu8 stdin/out/err proxies */

static void close_u8stdio()
{
  u8_close_input((u8_input)&u8stdin);
  u8_close_output((u8_output)&u8stdout);
  u8_close_output((u8_output)&u8stderr);
}

/* Pathstores */

KNO_EXPORT lispval kno_open_zpathstore(u8_string path,lispval opts);

DEFCPRIM("zpathstore",zpathstore_prim,
	 KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "**undocumented**",
	 {"fname",kno_string_type,KNO_VOID},
	 {"opts",kno_any_type,KNO_VOID})
static lispval zpathstore_prim(lispval fname,lispval opts)
{
  return kno_open_zpathstore(KNO_CSTRING(fname),opts);
}

/* The init function */

static int scheme_fileio_initialized = 0;

static lispval fileio_module;

KNO_EXPORT void kno_init_fileprims_c()
{
  if (scheme_fileio_initialized) return;
  scheme_fileio_initialized = 1;
  kno_init_scheme();
  fileio_module = kno_new_cmodule("fileio",(KNO_MODULE_DEFAULT),kno_init_fileprims_c);
  u8_register_source_file(_FILEINFO);

  u8_init_mutex(&stackdump_lock);
  u8_init_mutex(&tempdirs_lock);

  u8_init_xinput(&u8stdin,0,NULL);
  u8_init_xoutput(&u8stdout,1,NULL);
  u8_init_xoutput(&u8stderr,2,NULL);

  atexit(close_u8stdio);

  u8_set_global_output((u8_output)&u8stdout);
  u8_set_global_input((u8_input)&u8stdin);

  link_local_cprims();
  kno_def_xevalfn(fileio_module,"FILEOUT",
		  simple_fileout_evalfn,KNO_EVALFN_NOTAIL,
		  "generates output from "
		  "*args* which is written to the file *filename*. "
		  "Returns VOID");

  kno_def_xevalfn(fileio_module,"SYSTEM",
		  simple_system_evalfn,KNO_EVALFN_NOTAIL,
		  "generates output from "
		  "*args* which is passed as a command to the default shell.");

  kno_register_config
    ("TEMPROOT","Template for generating temporary directory names",
     temproot_get,temproot_set,NULL);
  kno_register_config
    ("TEMPDIRS","Declared temporary directories to be deleted on exit",
     tempdirs_get,tempdirs_add,NULL);
  kno_register_config
    ("KEEPDIRS","Temporary directories to not be deleted on exit",
     keepdirs_get,keepdirs_add,NULL);
  kno_register_config
    ("KEEPTEMP","Temporary directories to not be deleted on exit",
     keeptemp_get,keeptemp_set,NULL);

  atexit(remove_tempdirs);

  snapshotvars = kno_intern("%snapvars");
  snapshotconfig = kno_intern("%snapconfig");
  snapshotfile = kno_intern("%snapshotfile");
  configinfo = kno_intern("%configinfo");

  noblock_symbol = kno_intern("noblock");
  nodelay_symbol = kno_intern("nodelay");

  kno_def_evalfn(fileio_module,"SNAPSHOT",snapshot_evalfn,
		 "*undocumented*");
  kno_def_evalfn(fileio_module,"SNAPBACK",snapback_evalfn,
		 "*undocumented*");

  kno_register_config
    ("STACKDUMP","File to store stackdump information on errors",
     stackdump_config_get,stackdump_config_set,NULL);

  kno_finish_cmodule(fileio_module);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("fsinfo",fsinfo_prim,1,fileio_module);
  KNO_LINK_CPRIM("fsinfo",fsinfo_prim,1,fileio_module);
  KNO_LINK_CPRIM("setpos!",setpos_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("file%",file_progress_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("endpos",endpos_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("getpos",getpos_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("setbuf!",setbuf_prim,3,fileio_module);
  KNO_LINK_CPRIM("flush-output",flush_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("close",close_prim,1,kno_textio_module);
  KNO_LINK_CPRIM("readdir",readdir_prim,2,fileio_module);
  KNO_LINK_CPRIM("getlinks",getlinks_prim,2,fileio_module);
  KNO_LINK_CPRIM("getdirs",getdirs_prim,2,fileio_module);
  KNO_LINK_CPRIM("getfiles",getfiles_prim,2,fileio_module);
  KNO_LINK_CPRIM("setcwd",setcwd_prim,1,fileio_module);
  KNO_LINK_CPRIM("getcwd",getcwd_prim,0,fileio_module);
  KNO_LINK_CPRIM("set-file-access!",set_file_access_prim,4,fileio_module);
  KNO_LINK_CPRIM("file-owner",file_owner,1,fileio_module);
  KNO_LINK_CPRIM("file-size",file_size,1,fileio_module);
  KNO_LINK_CPRIM("file-mode",file_mode,1,fileio_module);
  KNO_LINK_CPRIM("file-creationtime",file_ctime,1,fileio_module);
  KNO_LINK_CPRIM("set-file-atime!",set_file_atime,2,fileio_module);
  KNO_LINK_CPRIM("file-accesstime",file_atime,1,fileio_module);
  KNO_LINK_CPRIM("set-file-modtime!",set_file_modtime,2,fileio_module);
  KNO_LINK_CPRIM("file-modtime",file_modtime,1,fileio_module);
  KNO_LINK_CPRIM("tempdir?",is_tempdir_prim,1,fileio_module);
  KNO_LINK_CPRIM("tempdir/done",tempdir_done_prim,2,fileio_module);
  KNO_LINK_CPRIM("tempdir",tempdir_prim,2,fileio_module);
  KNO_LINK_CPRIM("mkdirs",mkdirs_prim,2,fileio_module);
  KNO_LINK_CPRIM("rmdir",rmdir_prim,1,fileio_module);
  KNO_LINK_CPRIM("mkdir",mkdir_prim,2,fileio_module);
  KNO_LINK_CPRIM("runfile",runfile_prim,1,fileio_module);
  KNO_LINK_CPRIM("mkpath",mkpath_prim,2,fileio_module);
  KNO_LINK_CPRIM("path-location",path_location,1,fileio_module);
  KNO_LINK_CPRIM("path-dirname",path_dirname,1,fileio_module);
  KNO_LINK_CPRIM("path-suffix",path_suffix,2,fileio_module);
  KNO_LINK_CPRIM("path-basename",path_basename,2,fileio_module);
  KNO_LINK_CPRIM("readlink",file_readlink,3,fileio_module);
  KNO_LINK_CPRIM("realpath",file_realpath,2,fileio_module);
  KNO_LINK_CPRIM("abspath",file_abspath,2,fileio_module);
  KNO_LINK_CPRIM("file-socket?",file_socketp,1,fileio_module);
  KNO_LINK_CPRIM("file-symlink?",file_symlinkp,1,fileio_module);
  KNO_LINK_CPRIM("file-directory?",file_directoryp,1,fileio_module);
  KNO_LINK_CPRIM("file-writable?",file_writablep,1,fileio_module);
  KNO_LINK_CPRIM("file-readable?",file_readablep,1,fileio_module);
  KNO_LINK_CPRIM("file-regular?",file_regularp,1,fileio_module);
  KNO_LINK_CPRIM("file-exists?",file_existsp,1,fileio_module);
  KNO_LINK_CPRIM("filecontent",filecontent_prim,1,fileio_module);
  KNO_LINK_CPRIM("filedata",filedata_prim,1,fileio_module);
  KNO_LINK_CPRIM("filestring",filestring_prim,2,fileio_module);
  KNO_LINK_CPRIM("link-file!",link_file_prim,3,fileio_module);
  KNO_LINK_CPRIM("move-file!",move_file_prim,3,fileio_module);
  KNO_LINK_CPRIM("remove-tree!",remove_tree_prim,2,fileio_module);
  KNO_LINK_CPRIM("remove-file!",remove_file_prim,2,fileio_module);
  KNO_LINK_CPRIM("open-socket",open_socket_prim,2,kno_textio_module);
  KNO_LINK_CPRIM("write-file",writefile_prim,3,fileio_module);
  KNO_LINK_CPRIM("open-input-file",open_input_file,2,fileio_module);
  KNO_LINK_CPRIM("extend-output-file",extend_output_file,3,fileio_module);

  KNO_LINK_CPRIM("open-output-file",open_output_file,3,fileio_module);

  KNO_LINK_CPRIM("zpathstore",zpathstore_prim,2,fileio_module);

  KNO_LINK_ALIAS("setbuf",setbuf_prim,fileio_module);
  KNO_LINK_ALIAS("remove-file",remove_file_prim,fileio_module);
  KNO_LINK_ALIAS("move-file",move_file_prim,fileio_module);
  KNO_LINK_ALIAS("link-file",link_file_prim,fileio_module);
  KNO_LINK_ALIAS("remove-tree",remove_tree_prim,fileio_module);
  KNO_LINK_ALIAS("regular-file?",file_regularp,fileio_module);
  KNO_LINK_ALIAS("directory?",file_directoryp,fileio_module);
  KNO_LINK_ALIAS("dirname",path_dirname,fileio_module);
  KNO_LINK_ALIAS("basename",path_basename,fileio_module);
  KNO_LINK_ALIAS("statfs",fsinfo_prim,fileio_module);
  KNO_LINK_ALIAS("setpos",setpos_prim,kno_textio_module);

}
