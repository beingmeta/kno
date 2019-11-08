/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define KNO_PROVIDE_FASTEVAL 1

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
#include "kno/cprims.h"

#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8stringfns.h>
#include <libu8/u8streamio.h>
#include <libu8/u8netfns.h>
#include <libu8/u8xfiles.h>

#define fast_eval(x,env) (_kno_fast_eval(x,env,_stack,0))

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

/* Opening files */

DEFPRIM3("open-output-file",open_output_file,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(open-output-file *filename* [*encoding*] [*escape*])` "
	 "returns an output port, writing to the beginning "
	 "of the text file *filename* using *encoding*. If "
	 "*escape* is specified, it can be the character & "
	 "or \\, which causes special characters to be "
	 "output as either entity escaped or unicode "
	 "escaped sequences.",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_character_type,KNO_VOID);
static lispval open_output_file(lispval fname,lispval opts,lispval escape_char)
{
  struct U8_XOUTPUT *f;
  u8_string filename = kno_strdata(fname);
  lispval encid = KNO_VOID;
  u8_encoding enc = NULL;
  int open_flags = O_EXCL|O_CREAT|O_WRONLY;
  if (KNO_OPTIONSP(opts)) {
    encid = kno_getopt(opts,KNOSYM_ENCODING,KNO_VOID);}
  else if ( (KNO_SYMBOLP(opts)) || (KNO_STRINGP(opts)) )
    encid = opts;
  else NO_ELSE;
  if (VOIDP(encid)) enc = NULL;
  else if (STRINGP(encid))
    enc = u8_get_encoding(CSTRING(encid));
  else if (SYMBOLP(encid))
    enc = u8_get_encoding(SYM_NAME(encid));
  else return kno_err(kno_UnknownEncoding,"OPEN-OUTPUT-FILE",NULL,encid);
  f = u8_open_output_file(filename,enc,open_flags,0);
  if (encid != opts) kno_decref(encid);
  if (f == NULL)
    return kno_err("CantWriteFile","OPEN-OUTPUT-FILE",NULL,fname);
  if (KNO_CHARACTERP(escape_char)) {
    int escape = KNO_CHAR2CODE(escape_char);
    f->u8_xescape = escape;}
  return make_port(NULL,(u8_output)f,u8_strdup(filename));
}

DEFPRIM3("extend-output-file",extend_output_file,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(extend-output-file *filename* [*encoding*] [*escape*])` "
	 "returns an output port, writing to the end of the "
	 "text file *filename* using *encoding*. If "
	 "*escape* is specified, it can be the character & "
	 "or \\, which causes special characters to be "
	 "output as either entity escaped or unicode "
	 "escaped sequences.",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_character_type,KNO_VOID);
static lispval extend_output_file(lispval fname,lispval opts,lispval escape_char)
{
  struct U8_XOUTPUT *f;
  u8_string filename = kno_strdata(fname);
  lispval encid = KNO_VOID;
  u8_encoding enc = NULL;
  int open_flags = O_APPEND|O_CREAT|O_WRONLY;
  if (KNO_OPTIONSP(opts)) {
    encid = kno_getopt(opts,KNOSYM_ENCODING,KNO_VOID);}
  else if ( (KNO_SYMBOLP(opts)) || (KNO_STRINGP(opts)) )
    encid = opts;
  else NO_ELSE;
  if (VOIDP(encid))
    enc = NULL;
  else if (STRINGP(encid))
    enc = u8_get_encoding(CSTRING(encid));
  else if (SYMBOLP(encid))
    enc = u8_get_encoding(SYM_NAME(encid));
  else return kno_err(kno_UnknownEncoding,"EXTEND-OUTPUT-FILE",NULL,encid);
  f = u8_open_output_file(filename,enc,open_flags,0);
  if (f == NULL)
    return kno_err(u8_CantOpenFile,"EXTEND-OUTPUT-FILE",NULL,fname);
  if (KNO_CHARACTERP(escape_char)) {
    int escape = KNO_CHAR2CODE(escape_char);
    f->u8_xescape = escape;}
  return make_port(NULL,(u8_output)f,u8_strdup(filename));
}

DEFPRIM2("open-input-file",open_input_file,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(open-input-file *filename* [*encoding*])` "
	 "returns an input port for the text file "
	 "*filename*, translating from *encoding*. If "
	 "*encoding* is not specified, the file is opened "
	 "as a UTF-8 file.",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval open_input_file(lispval fname,lispval opts)
{
  struct U8_XINPUT *f;
  u8_string filename = kno_strdata(fname);
  lispval encid = KNO_VOID;
  u8_encoding enc = NULL;
  int open_flags = O_RDONLY;
  if (KNO_OPTIONSP(opts)) {
    encid = kno_getopt(opts,KNOSYM_ENCODING,KNO_VOID);}
  else if ( (KNO_SYMBOLP(opts)) || (KNO_STRINGP(opts)) )
    encid = opts;
  else NO_ELSE;
  if (VOIDP(encid))
    enc = NULL;
  else if (STRINGP(encid))
    enc = u8_get_encoding(CSTRING(encid));
  else if (SYMBOLP(encid))
    enc = u8_get_encoding(SYM_NAME(encid));
  else return kno_err(kno_UnknownEncoding,"OPEN-INPUT_FILE",NULL,encid);
  f = u8_open_input_file(filename,enc,open_flags,0);
  if (f == NULL)
    return kno_err(u8_CantOpenFile,"OPEN-INPUT-FILE",NULL,fname);
  else return make_port((u8_input)f,NULL,u8_strdup(filename));
}

DEFPRIM3("write-file",writefile_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(WRITE-FILE *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
static lispval writefile_prim(lispval filename,lispval object,lispval enc)
{
  int len = 0; const unsigned char *bytes; int free_bytes = 0;
  if (STRINGP(object)) {
    bytes = CSTRING(object);
    len = STRLEN(object);}
  else if (PACKETP(object)) {
    bytes = KNO_PACKET_DATA(object);
    len = KNO_PACKET_LENGTH(object);}
  else if ((FALSEP(enc)) || (VOIDP(enc))) {
    struct KNO_OUTBUF out = { 0 };
    KNO_INIT_BYTE_OUTPUT(&out,1024);
    kno_write_dtype(&out,object);
    bytes = out.buffer;
    len = out.bufwrite-out.buffer;
    free_bytes = 1;}
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
  lispval filename_val = kno_eval(filename_arg,env);
  U8_OUTPUT *f, *oldf; int doclose;
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
      lispval value = fast_eval(ex,env);
      if (printout_helper(f,value)) kno_decref(value);
      else {
	u8_set_default_output(oldf);
	kno_decref(filename_val);
	return value;}}}
  if (oldf) u8_set_default_output(oldf);
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
      lispval value = fast_eval(string_expr,env);
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

DEFPRIM2("open-socket",open_socket_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(OPEN-SOCKET *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
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

DEFPRIM2("remove-file!",remove_file_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(REMOVE-FILE! *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval remove_file_prim(lispval arg,lispval must_exist)
{
  u8_string filename = CSTRING(arg);
  if ((u8_file_existsp(filename))||(u8_symlinkp(filename))) {
    if (u8_removefile(CSTRING(arg))<0) {
      lispval err = kno_err(RemoveFailed,"remove_file_prim",filename,arg);
      return err;}
    else return KNO_TRUE;}
  else if (KNO_TRUEP(must_exist)) {
    u8_string absolute = u8_abspath(filename,NULL);
    lispval err = kno_err(kno_NoSuchFile,"remove_file_prim",absolute,arg);
    u8_free(absolute);
    return err;}
  else return KNO_FALSE;
}

DEFPRIM2("remove-tree!",remove_tree_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(REMOVE-TREE! *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval remove_tree_prim(lispval arg,lispval must_exist)
{
  u8_string filename = CSTRING(arg);
  if (u8_directoryp(filename))
    if (u8_rmtree(CSTRING(arg))<0) {
      lispval err = kno_err(RemoveFailed,"remove_tree_prim",filename,arg);
      return err;}
    else return KNO_TRUE;
  else if (KNO_TRUEP(must_exist)) {
    u8_string absolute = u8_abspath(filename,NULL);
    lispval err = kno_err(kno_NoSuchFile,"remove_tree_prim",absolute,arg);
    u8_free(absolute);
    return err;}
  else return KNO_FALSE;
}

DEFPRIM3("move-file!",move_file_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(MOVE-FILE! *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
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

DEFPRIM3("link-file!",link_file_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(LINK-FILE! *arg0* *arg1* [*arg2*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
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

DEFPRIM2("filestring",filestring_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "(FILESTRING *file* [*encoding*]) "
	 "returns the contents of a text file. The "
	 "*encoding*, if provided, specifies the character "
	 "encoding, which defaults to UTF-8",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval filestring_prim(lispval filename,lispval enc)
{
  if ((VOIDP(enc))||(FALSEP(enc))) {
    u8_string data = u8_filestring(CSTRING(filename),"UTF-8");
    if (data)
      return kno_wrapstring(data);
    else return KNO_ERROR;}
  else if (KNO_TRUEP(enc)) {
    u8_string data = u8_filestring(CSTRING(filename),"auto");
    if (data) return kno_wrapstring(data);
    else return KNO_ERROR;}
  else if (STRINGP(enc)) {
    u8_string data = u8_filestring(CSTRING(filename),CSTRING(enc));
    if (data)
      return kno_wrapstring(data);
    else return KNO_ERROR;}
  else return kno_err(kno_UnknownEncoding,"FILESTRING",NULL,enc);
}

DEFPRIM1("filedata",filedata_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "(FILEDATA *file*) "
	 "returns the contents of *file* as a packet.",
	 kno_string_type,KNO_VOID);
static lispval filedata_prim(lispval filename)
{
  int len = -1;
  unsigned char *data = u8_filedata(CSTRING(filename),&len);
  if (len>=0) return kno_init_packet(NULL,len,data);
  else return KNO_ERROR;
}

DEFPRIM1("filecontent",filecontent_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "Returns the contents of a named file, trying to "
	 "be intelligent about returning a string or packet "
	 "depending on the probably file type",
	 kno_string_type,KNO_VOID);
static lispval filecontent_prim(lispval filename)
{
  int len = -1;
  unsigned char *data = u8_filedata(CSTRING(filename),&len);
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

DEFPRIM1("file-exists?",file_existsp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-EXISTS? *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_existsp(lispval arg)
{
  if (u8_file_existsp(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("file-regular?",file_regularp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-REGULAR? *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
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

DEFPRIM1("file-readable?",file_readablep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-READABLE? *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_readablep(lispval arg)
{
  if (u8_file_readablep(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("file-writable?",file_writablep,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-WRITABLE? *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_writablep(lispval arg)
{
  if (u8_file_writablep(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("file-directory?",file_directoryp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-DIRECTORY? *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_directoryp(lispval arg)
{
  if (u8_directoryp(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("file-symlink?",file_symlinkp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-SYMLINK? *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_symlinkp(lispval arg)
{
  if (u8_symlinkp(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM1("file-socket?",file_socketp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-SOCKET? *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_socketp(lispval arg)
{
  if (u8_socketp(CSTRING(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM2("abspath",file_abspath,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(ABSPATH *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);
static lispval file_abspath(lispval arg,lispval wd)
{
  u8_string result;
  if (VOIDP(wd))
    result = u8_abspath(CSTRING(arg),NULL);
  else result = u8_abspath(CSTRING(arg),CSTRING(wd));
  if (result) return kno_wrapstring(result);
  else return KNO_ERROR;
}

DEFPRIM2("realpath",file_realpath,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(REALPATH *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_string_type,KNO_VOID);
static lispval file_realpath(lispval arg,lispval wd)
{
  u8_string result;
  if (VOIDP(wd))
    result = u8_realpath(CSTRING(arg),NULL);
  else result = u8_realpath(CSTRING(arg),CSTRING(wd));
  if (result) return kno_wrapstring(result);
  else return KNO_ERROR;
}

DEFPRIM3("readlink",file_readlink,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(READLINK *arg0* [*arg1*] [*arg2*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID,
	 kno_any_type,KNO_VOID);
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

DEFPRIM2("path-basename",path_basename,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(PATH-BASENAME *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval path_basename(lispval arg,lispval suffix)
{
  if ((VOIDP(suffix)) || (FALSEP(suffix)))
    return kno_wrapstring(u8_basename(CSTRING(arg),NULL));
  else if (STRINGP(suffix))
    return kno_wrapstring(u8_basename(CSTRING(arg),CSTRING(suffix)));
  else return kno_wrapstring(u8_basename(CSTRING(arg),"*"));
}

DEFPRIM2("path-suffix",path_suffix,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(PATH-SUFFIX *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
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

DEFPRIM1("path-dirname",path_dirname,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PATH-DIRNAME *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval path_dirname(lispval arg)
{
  return kno_wrapstring(u8_dirname(CSTRING(arg)));
}

DEFPRIM1("path-location",path_location,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(PATH-LOCATION *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval path_location(lispval arg)
{
  u8_string path = CSTRING(arg);
  u8_string slash = strrchr(path,'/');
  if (slash[1]=='\0') return kno_incref(arg);
  else return kno_substring(path,slash+1);
}

DEFPRIM2("mkpath",mkpath_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(MKPATH *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_string_type,KNO_VOID);
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

DEFPRIM1("runfile",runfile_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(RUNFILE *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval runfile_prim(lispval suffix)
{
  return kno_wrapstring(kno_runbase_filename(CSTRING(suffix)));
}

/* Making directories */

DEFPRIM2("mkdir",mkdir_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(MKDIR *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
static lispval mkdir_prim(lispval dirname,lispval mode_arg)
{
  mode_t mode=
    ((KNO_UINTP(mode_arg))?((mode_t)(FIX2INT(mode_arg))):((mode_t)0777));
  int retval = u8_mkdir(CSTRING(dirname),mode);
  if (retval<0) {
    u8_condition cond = u8_strerror(errno); errno = 0;
    return kno_err(cond,"mkdir_prim",NULL,dirname);}
  else if (retval) {
    /* Force the mode to be set if provided */
    if (KNO_UINTP(mode_arg))
      u8_chmod(CSTRING(dirname),((mode_t)(FIX2INT(mode_arg))));
    return KNO_TRUE;}
  else return KNO_FALSE;
}

DEFPRIM1("rmdir",rmdir_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(RMDIR *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval rmdir_prim(lispval dirname)
{
  int retval = u8_rmdir(CSTRING(dirname));
  if (retval<0) {
    u8_condition cond = u8_strerror(errno); errno = 0;
    return kno_err(cond,"rmdir_prim",NULL,dirname);}
  else if (retval) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFPRIM2("mkdirs",mkdirs_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(MKDIRS *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_fixnum_type,KNO_VOID);
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

DEFPRIM2("tempdir",tempdir_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0),
	 "`(TEMPDIR [*arg0*] [*arg1*])` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE);
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

DEFPRIM2("tempdir/done",tempdir_done_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0),
	 "`(TEMPDIR/DONE [*arg0*] [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_FALSE);
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

DEFPRIM1("tempdir?",is_tempdir_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "`(TEMPDIR? [*arg0*])` **undocumented**",
	 kno_string_type,KNO_VOID);
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

DEFPRIM1("file-modtime",file_modtime,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-MODTIME *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_modtime(lispval filename)
{
  time_t mtime = u8_file_mtime(CSTRING(filename));
  if (mtime<0) return KNO_ERROR;
  else return make_timestamp(mtime);
}

DEFPRIM2("set-file-modtime!",set_file_modtime,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(SET-FILE-MODTIME! *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
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

DEFPRIM1("file-accesstime",file_atime,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-ACCESSTIME *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_atime(lispval filename)
{
  time_t mtime = u8_file_atime(CSTRING(filename));
  if (mtime<0) return KNO_ERROR;
  else return make_timestamp(mtime);
}

DEFPRIM2("set-file-atime!",set_file_atime,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(SET-FILE-ATIME! *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_VOID);
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

DEFPRIM1("file-creationtime",file_ctime,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-CREATIONTIME *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_ctime(lispval filename)
{
  time_t mtime = u8_file_ctime(CSTRING(filename));
  if (mtime<0) return KNO_ERROR;
  else return make_timestamp(mtime);
}

DEFPRIM1("file-mode",file_mode,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-MODE *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_mode(lispval filename)
{
  int mode = u8_file_mode(CSTRING(filename));
  if (mode<0) return KNO_ERROR;
  else return KNO_INT(mode);
}

DEFPRIM1("file-size",file_size,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-SIZE *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_size(lispval filename)
{
  ssize_t size = u8_file_size(CSTRING(filename));
  if (size<0) return KNO_ERROR;
  else if (size<KNO_MAX_FIXNUM)
    return KNO_INT(size);
  else return kno_make_bigint(size);
}

DEFPRIM1("file-owner",file_owner,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE-OWNER *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval file_owner(lispval filename)
{
  u8_string name = u8_file_owner(CSTRING(filename));
  if (name) return kno_wrapstring(name);
  else return KNO_ERROR;
}

DEFPRIM4("set-file-access!",set_file_access_prim,KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	 "(SET-FILE-ACCESS! *file* [*mode*] [*group*] [*owner*]) "
	 "sets the mode/group/owner of a file",
	 kno_string_type,KNO_VOID,kno_fixnum_type,KNO_VOID,
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
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

DEFPRIM("getcwd",getcwd_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
	"`(GETCWD)` **undocumented**");
static lispval getcwd_prim()
{
  u8_string wd = u8_getcwd();
  if (wd) return kno_wrapstring(wd);
  else return KNO_ERROR;
}

DEFPRIM1("setcwd",setcwd_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(SETCWD *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
static lispval setcwd_prim(lispval dirname)
{
  if (u8_setcwd(CSTRING(dirname))<0)
    return KNO_ERROR;
  else return VOID;
}

/* Directory listings */

DEFPRIM2("getfiles",getfiles_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(GETFILES *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_TRUE);
static lispval getfiles_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_getfiles(CSTRING(dirname),(!(FALSEP(fullpath)))), *scan = contents;
  if (contents == NULL) return KNO_ERROR;
  else while (*scan) {
      lispval string = kno_wrapstring(*scan);
      CHOICE_ADD(results,string);
      scan++;}
  u8_free(contents);
  return results;
}

DEFPRIM2("getdirs",getdirs_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(GETDIRS *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_TRUE);
static lispval getdirs_prim(lispval dirname,lispval fullpath)
{
  lispval results = EMPTY;
  u8_string *contents=
    u8_getdirs(CSTRING(dirname),(!(FALSEP(fullpath)))), *scan = contents;
  if (contents == NULL) return KNO_ERROR;
  else while (*scan) {
      lispval string = kno_wrapstring(*scan);
      CHOICE_ADD(results,string);
      scan++;}
  u8_free(contents);
  return results;
}

DEFPRIM2("getlinks",getlinks_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(GETLINKS *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_TRUE);
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

DEFPRIM2("readdir",readdir_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	 "`(READDIR *arg0* [*arg1*])` **undocumented**",
	 kno_string_type,KNO_VOID,kno_any_type,KNO_TRUE);
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

DEFPRIM1("close",close_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "`(CLOSE [*arg0*])` **undocumented**",
	 kno_any_type,KNO_VOID);
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

DEFPRIM1("flush-output",flush_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
	 "`(FLUSH-OUTPUT [*arg0*])` **undocumented**",
	 kno_any_type,KNO_VOID);
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

DEFPRIM3("setbuf!",setbuf_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(SETBUF! *port/stream* *insize* *outsize*)` "
	 "sets the input and output buffer sizes for a port "
	 "or stream.",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_FALSE,
	 kno_any_type,KNO_FALSE);
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

DEFPRIM1("getpos",getpos_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(GETPOS *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
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

DEFPRIM1("endpos",endpos_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(ENDPOS *arg0*)` **undocumented**",
	 kno_any_type,KNO_VOID);
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

DEFPRIM1("file%",file_progress_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FILE% *arg0*)` **undocumented**",
	 kno_ioport_type,KNO_VOID);
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

DEFPRIM2("setpos!",setpos_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	 "`(SETPOS! *arg0* *arg1*)` **undocumented**",
	 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
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

DEFPRIM1("fsinfo",fsinfo_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FSINFO *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
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
DEFPRIM1("fsinfo",fsinfo_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(FSINFO *arg0*)` **undocumented**",
	 kno_string_type,KNO_VOID);
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
  lispval arg1 = kno_eval(kno_get_arg(expr,1),env), arg2 = kno_eval(kno_get_arg(expr,2),env);
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
  lispval arg1 = kno_eval(kno_get_arg(expr,1),env), arg2 = kno_eval(kno_get_arg(expr,2),env);
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

/* The init function */

static int scheme_fileio_initialized = 0;

KNO_EXPORT void kno_init_driverfns_c(void);

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
  kno_def_evalfn(fileio_module,"FILEOUT",simple_fileout_evalfn,
		 "*undocumented*");

  kno_def_evalfn(fileio_module,"SYSTEM",simple_system_evalfn,
		 "*undocumented*");

  kno_init_driverfns_c();

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

  kno_finish_module(fileio_module);
}

KNO_EXPORT void kno_init_schemeio()
{
  kno_init_fileprims_c();
  kno_init_driverfns_c();
}



static void link_local_cprims()
{
  lispval scheme_module = kno_scheme_module;

  KNO_LINK_PRIM("fsinfo",fsinfo_prim,1,fileio_module);
  KNO_LINK_PRIM("fsinfo",fsinfo_prim,1,fileio_module);
  KNO_LINK_PRIM("setpos!",setpos_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("file%",file_progress_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("endpos",endpos_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("getpos",getpos_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("setbuf!",setbuf_prim,3,fileio_module);
  KNO_LINK_PRIM("flush-output",flush_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("close",close_prim,1,kno_scheme_module);
  KNO_LINK_PRIM("readdir",readdir_prim,2,fileio_module);
  KNO_LINK_PRIM("getlinks",getlinks_prim,2,fileio_module);
  KNO_LINK_PRIM("getdirs",getdirs_prim,2,fileio_module);
  KNO_LINK_PRIM("getfiles",getfiles_prim,2,fileio_module);
  KNO_LINK_PRIM("setcwd",setcwd_prim,1,fileio_module);
  KNO_LINK_PRIM("getcwd",getcwd_prim,0,fileio_module);
  KNO_LINK_PRIM("set-file-access!",set_file_access_prim,4,fileio_module);
  KNO_LINK_PRIM("file-owner",file_owner,1,fileio_module);
  KNO_LINK_PRIM("file-size",file_size,1,fileio_module);
  KNO_LINK_PRIM("file-mode",file_mode,1,fileio_module);
  KNO_LINK_PRIM("file-creationtime",file_ctime,1,fileio_module);
  KNO_LINK_PRIM("set-file-atime!",set_file_atime,2,fileio_module);
  KNO_LINK_PRIM("file-accesstime",file_atime,1,fileio_module);
  KNO_LINK_PRIM("set-file-modtime!",set_file_modtime,2,fileio_module);
  KNO_LINK_PRIM("file-modtime",file_modtime,1,fileio_module);
  KNO_LINK_PRIM("tempdir?",is_tempdir_prim,1,fileio_module);
  KNO_LINK_PRIM("tempdir/done",tempdir_done_prim,2,fileio_module);
  KNO_LINK_PRIM("tempdir",tempdir_prim,2,fileio_module);
  KNO_LINK_PRIM("mkdirs",mkdirs_prim,2,fileio_module);
  KNO_LINK_PRIM("rmdir",rmdir_prim,1,fileio_module);
  KNO_LINK_PRIM("mkdir",mkdir_prim,2,fileio_module);
  KNO_LINK_PRIM("runfile",runfile_prim,1,fileio_module);
  KNO_LINK_PRIM("mkpath",mkpath_prim,2,fileio_module);
  KNO_LINK_PRIM("path-location",path_location,1,fileio_module);
  KNO_LINK_PRIM("path-dirname",path_dirname,1,fileio_module);
  KNO_LINK_PRIM("path-suffix",path_suffix,2,fileio_module);
  KNO_LINK_PRIM("path-basename",path_basename,2,fileio_module);
  KNO_LINK_PRIM("readlink",file_readlink,3,fileio_module);
  KNO_LINK_PRIM("realpath",file_realpath,2,fileio_module);
  KNO_LINK_PRIM("abspath",file_abspath,2,fileio_module);
  KNO_LINK_PRIM("file-socket?",file_socketp,1,fileio_module);
  KNO_LINK_PRIM("file-symlink?",file_symlinkp,1,fileio_module);
  KNO_LINK_PRIM("file-directory?",file_directoryp,1,fileio_module);
  KNO_LINK_PRIM("file-writable?",file_writablep,1,fileio_module);
  KNO_LINK_PRIM("file-readable?",file_readablep,1,fileio_module);
  KNO_LINK_PRIM("file-regular?",file_regularp,1,fileio_module);
  KNO_LINK_PRIM("file-exists?",file_existsp,1,fileio_module);
  KNO_LINK_PRIM("filecontent",filecontent_prim,1,fileio_module);
  KNO_LINK_PRIM("filedata",filedata_prim,1,fileio_module);
  KNO_LINK_PRIM("filestring",filestring_prim,2,fileio_module);
  KNO_LINK_PRIM("link-file!",link_file_prim,3,fileio_module);
  KNO_LINK_PRIM("move-file!",move_file_prim,3,fileio_module);
  KNO_LINK_PRIM("remove-tree!",remove_tree_prim,2,fileio_module);
  KNO_LINK_PRIM("remove-file!",remove_file_prim,2,fileio_module);
  KNO_LINK_PRIM("open-socket",open_socket_prim,2,kno_scheme_module);
  KNO_LINK_PRIM("write-file",writefile_prim,3,fileio_module);
  KNO_LINK_PRIM("open-input-file",open_input_file,2,fileio_module);
  KNO_LINK_PRIM("extend-output-file",extend_output_file,3,fileio_module);
  KNO_LINK_PRIM("open-output-file",open_output_file,3,fileio_module);

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
  KNO_LINK_ALIAS("setpos",setpos_prim,scheme_module);

}
