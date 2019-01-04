/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>

#include <sys/types.h>
#include <zip.h>

static u8_condition ZipFileError=_("Zip file error");

FD_EXPORT fd_ptr_type fd_zipfile_type;
fd_ptr_type fd_zipfile_type;
typedef struct FD_ZIPFILE {
  FD_CONS_HEADER;
  u8_string filename; int flags;
  u8_mutex zipfile_lock; int closed;
  struct zip *zip;} FD_ZIPFILE;
typedef struct FD_ZIPFILE *fd_zipfile;

/* Error messages */

static lispval znumerr(u8_context cxt,int zerrno,u8_string path)
{
  u8_byte buf[1024];
  zip_error_to_str(buf,1024,errno,zerrno);
  return fd_err(ZipFileError,cxt,
                u8_mkstring("(%s) %s",path,buf),
                FD_VOID);
}

static lispval ziperr(u8_context cxt,fd_zipfile zf,lispval irritant)
{
  u8_string details=
    u8_mkstring("(%s) %s",zf->filename,zip_strerror(zf->zip));
  fd_seterr(ZipFileError,cxt,details,fd_incref(irritant));
  return FD_ERROR_VALUE;
}

static lispval zfilerr(u8_context cxt,fd_zipfile zf,struct zip_file *zfile,
                      lispval irritant)
{
  u8_string details=
    u8_mkstring("(%s) %s",zf->filename,zip_file_strerror(zfile));
  zip_fclose(zfile);
  return fd_err(ZipFileError,cxt,details,irritant);
}

/* Zip file utilities */

static void recycle_zipfile(struct FD_RAW_CONS *c)
{
  struct FD_ZIPFILE *zf = (struct FD_ZIPFILE *)c;
  if (!(zf->closed)) zip_close(zf->zip);
  zf->closed = 1;
  u8_destroy_mutex(&(zf->zipfile_lock));
  u8_free(zf->filename);
  if (!(FD_STATIC_CONSP(c))) u8_free(c);
}

static int unparse_zipfile(struct U8_OUTPUT *out,lispval x)
{
  struct FD_ZIPFILE *zf = (struct FD_ZIPFILE *)x;
  u8_printf(out,"#<ZIPFILE '%s'%s>",
            zf->filename,((zf->closed)?(" closed"):""));
  return 1;
}

static lispval zipreopen(struct FD_ZIPFILE *zf,int locked)
{
  if (!(zf->closed)) return FD_FALSE;
  else {
    int errflag;
    struct zip *zip;
    if (!(locked)) u8_lock_mutex(&(zf->zipfile_lock));
    if (!(zf->closed)) {
      if (!(locked)) u8_unlock_mutex(&(zf->zipfile_lock));
      return FD_FALSE;}
    else zip = zip_open(zf->filename,zf->flags,&errflag);
    if (!(zip)) {
      lispval errval = znumerr("zipreopen",errflag,zf->filename);
      if (!(locked)) u8_unlock_mutex(&(zf->zipfile_lock));
      return errval;}
    else {
      zf->zip = zip; zf->closed = 0;
      if (!(locked)) u8_unlock_mutex(&(zf->zipfile_lock));
      return FD_TRUE;}}
}

static lispval iszipfile_prim(lispval arg)
{
  if (FD_TYPEP(arg,fd_zipfile_type)) return FD_TRUE;
  else return FD_FALSE;
}

/* Creating/opening zip files */

static lispval zipopen(u8_string path,int zflags,int oflags)
{
  int errflag = 0, flags = zflags|oflags;
  u8_string abspath = u8_abspath(path,NULL);
  if ((!(flags&ZIP_CREATE))&&(!(u8_file_existsp(abspath)))) {
    fd_seterr(fd_FileNotFound,"zipopen",abspath,FD_VOID);
    U8_CLEAR_ERRNO();
    return FD_ERROR_VALUE;}
  else {
    struct zip *zip = zip_open(abspath,flags,&errflag);
    if (zip) {
      struct FD_ZIPFILE *zf = u8_alloc(struct FD_ZIPFILE);
      FD_INIT_FRESH_CONS(zf,fd_zipfile_type);
      u8_init_mutex(&(zf->zipfile_lock));
      zf->filename = abspath; zf->flags = zflags; zf->closed = 0;
      zf->zip = zip;
      U8_CLEAR_ERRNO();
      return LISP_CONS(zf);}
    else {
      U8_CLEAR_ERRNO();
      return znumerr("open_zipfile",errflag,abspath);}}
}
static lispval zipopen_prim(lispval filename,lispval create)
{
  if ((FD_FALSEP(create))||(FD_VOIDP(create)))
    return zipopen(FD_CSTRING(filename),ZIP_CHECKCONS,0);
  else return zipopen(FD_CSTRING(filename),ZIP_CHECKCONS,ZIP_CREATE);
}
static lispval zipmake_prim(lispval filename)
{
  return zipopen(FD_CSTRING(filename),0,ZIP_CREATE|ZIP_EXCL);
}

static lispval zipfilename_prim(lispval zipfile)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  return lispval_string(zf->filename);
}

static lispval close_zipfile(lispval zipfile)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  int retval;
  u8_lock_mutex(&(zf->zipfile_lock));
  if (zf->closed) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return FD_FALSE;}
  else retval = zip_close(zf->zip);
  if (retval) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return ziperr("close_zipfile",zf,zipfile);}
  else {
    zf->closed = 1;
    u8_unlock_mutex(&(zf->zipfile_lock));
    return FD_TRUE;}
}

static lispval zipfile_openp(lispval zipfile)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  if (zf->closed) return FD_FALSE;
  else return FD_TRUE;
}

/* Adding to zip files */

static long long int zipadd
  (struct FD_ZIPFILE *zf,u8_string name,struct zip_source *zsource)
{
  long long int index = zip_name_locate(zf->zip,name,0), retval = -1;
  if (index<0) retval = index = zip_add(zf->zip,name,zsource);
  else retval = zip_replace(zf->zip,index,zsource);
  if (retval<0) return retval;
  else return index;
}

static lispval zipadd_prim(lispval zipfile,lispval filename,lispval value,
                          lispval comment,lispval compress)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  u8_string fname = FD_CSTRING(filename);
  unsigned char *data = NULL; size_t datalen = 0;
  struct zip_source *zsource;
  long long int index = -1;
  if ((fname[0]=='.')&&(fname[1]=='/')) fname = fname+2;
  if (FD_STRINGP(value)) {
    data = u8_strdup(FD_CSTRING(value));
    datalen = FD_STRLEN(value);}
  else if (FD_PACKETP(value)) {
    datalen = FD_PACKET_LENGTH(value);
    data = u8_malloc(datalen);
    memcpy(data,FD_PACKET_DATA(value),datalen);}
  else return fd_type_error("zip source","zipadd_prim",value);
  u8_lock_mutex(&(zf->zipfile_lock));
  if (zf->closed) {
    lispval errval = zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->zipfile_lock));
      return errval;}}
  zsource = zip_source_buffer(zf->zip,data,datalen,1);
  if (!(zsource)) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return ziperr("zipadd/source",zf,(lispval)zf);}
  index = zipadd(zf,fname,zsource);
  if (index<0) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return ziperr("zipadd",zf,(lispval)zf);}
#if (HAVE_ZIP_SET_FILE_COMMENT)
  if (!(FD_FALSEP(comment))) {
    int retval = -1;
    if (FD_STRINGP(comment))
      retval = zip_set_file_comment
        (zf->zip,index,FD_CSTRING(comment),FD_STRLEN(comment));
    else if (FD_PACKETP(comment))
      retval = zip_set_file_comment
        (zf->zip,index,FD_PACKET_DATA(comment),
         FD_PACKET_LENGTH(comment));
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      fd_unparse(&out,comment);
      retval = zip_set_file_comment(zf->zip,index,out.u8_outbuf,
                                  out.u8_write-out.u8_outbuf);}
    if (retval<0) {
      u8_unlock_mutex(&(zf->zipfile_lock));
      return ziperr("zipadd/comment",zf,(lispval)zf);}}
#else
  if (!(FD_FALSEP(comment))) {
    u8_log(LOG_WARNING,"zipadd/comment",
           "available libzip doesn't support comment fields");}
#endif
#if (HAVE_ZIP_SET_FILE_COMPRESSION)
  if (FD_FALSEP(compress)) {
    int retval = zip_set_file_compression(zf->zip,index,ZIP_CM_STORE,0);
    if (retval<0) {
      u8_unlock_mutex(&(zf->zipfile_lock));
      return ziperr("zipadd/nocompresss",zf,(lispval)zf);}}
#else
  if (FD_FALSEP(compress)) {
    u8_log(LOG_WARNING,"zipadd/compress",
           "available libzip doesn't support uncompressed fields");}
#endif
  u8_unlock_mutex(&(zf->zipfile_lock));
  return FD_INT(index);
}

static lispval zipdrop_prim(lispval zipfile,lispval filename)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  u8_string fname = FD_CSTRING(filename);
  int index; int retval;
  if ((fname[0]=='.')&&(fname[1]=='/')) fname = fname+2;
  u8_lock_mutex(&(zf->zipfile_lock));
  if (zf->closed) {
    lispval errval = zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->zipfile_lock));
      return errval;}}
  index = zip_name_locate(zf->zip,fname,0);
  if (index<0) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return FD_FALSE;}
  else retval = zip_delete(zf->zip,index);
  if (retval<0) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return ziperr("zipdrop",zf,(lispval)zf);}
  else {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return FD_TRUE;}
}

static int istext(u8_byte *buf,int size)
{
  u8_byte *scan = buf, *limit = buf+size;
  while (scan<limit)
    if ((*scan)>=0x80)
      return u8_validp(scan);
    else scan++;
  return 1;
}

static lispval zipget_prim(lispval zipfile,lispval filename,lispval isbinary)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  u8_string fname = FD_CSTRING(filename);
  struct zip_stat zstat; int zret;
  struct zip_file *zfile;
  int index;
  if ((fname[0]=='.')&&(fname[1]=='/')) fname = fname+2;
  u8_lock_mutex(&(zf->zipfile_lock));
  if (zf->closed) {
    lispval errval = zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->zipfile_lock));
      return errval;}}
  index = zip_name_locate(zf->zip,fname,0);
  if (index<0) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return FD_FALSE;}
  else if ((zret = zip_stat(zf->zip,fname,0,&zstat))) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return ziperr("zipget_prim/stat",zf,filename);}
  else if ((zfile = zip_fopen(zf->zip,fname,0))) {
    unsigned char *buf = u8_malloc(zstat.size+1);
    int size = zstat.size, block = 0, read = 0, togo = size;
    while ((togo>0)&&((block = zip_fread(zfile,buf+read,togo))>0)) {
      if (block<0) break;
      read = read+block;
      togo = togo-block;}
    if (togo>0) {
      u8_free(buf);
      u8_unlock_mutex(&(zf->zipfile_lock));
      return zfilerr("zipget_prim/fread",zf,zfile,filename);}
    zip_fclose(zfile);
    u8_unlock_mutex(&(zf->zipfile_lock));
    buf[zstat.size]='\0';
    if (FD_VOIDP(isbinary)) {
      if (istext(buf,size))
        return fd_init_string(NULL,size,buf);
      else return fd_init_packet(NULL,size,buf);}
    else if (FD_TRUEP(isbinary))
      return fd_init_packet(NULL,size,buf);
    else return fd_init_string(NULL,size,buf);}
  else {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return ziperr("zipget_prim/fopen",zf,filename);}
}

static lispval zipexists_prim(lispval zipfile,lispval filename)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  u8_string fname = FD_CSTRING(filename); int index;
  if ((fname[0]=='.')&&(fname[1]=='/')) fname = fname+2;
  u8_lock_mutex(&(zf->zipfile_lock));
  if (zf->closed) {
    lispval errval = zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->zipfile_lock));
      return errval;}}
  index = zip_name_locate(zf->zip,fname,0);
  if (index<0) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return FD_FALSE;}
  else {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return FD_TRUE;}
}

static lispval zipmodtime_prim(lispval zipfile,lispval filename)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  u8_string fname = FD_CSTRING(filename);
  struct zip_stat zstat; int zret;
  int index;
  if ((fname[0]=='.')&&(fname[1]=='/')) fname = fname+2;
  u8_lock_mutex(&(zf->zipfile_lock));
  if (zf->closed) {
    lispval errval = zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->zipfile_lock));
      return errval;}}
  index = zip_name_locate(zf->zip,fname,0);
  if (index<0) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return FD_FALSE;}
  else if ((zret = zip_stat(zf->zip,fname,0,&zstat))) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return ziperr("zipmodtime_prim/stat",zf,filename);}
  else {
    time_t modified = zstat.mtime;
    lispval timestamp = fd_time2timestamp(modified);
    u8_unlock_mutex(&(zf->zipfile_lock));
    return timestamp;}
}

static lispval zipgetsize_prim(lispval zipfile,lispval filename)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  u8_string fname = FD_CSTRING(filename);
  struct zip_stat zstat; int zret;
  int index;
  if ((fname[0]=='.')&&(fname[1]=='/')) fname = fname+2;
  u8_lock_mutex(&(zf->zipfile_lock));
  if (zf->closed) {
    lispval errval = zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->zipfile_lock));
      return errval;}}
  index = zip_name_locate(zf->zip,fname,0);
  if (index<0) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return FD_FALSE;}
  else if ((zret = zip_stat(zf->zip,fname,0,&zstat))) {
    u8_unlock_mutex(&(zf->zipfile_lock));
    return ziperr("zipgetsize_prim/stat",zf,filename);}
  else {
    size_t uncompressed_size = zstat.size;
    lispval size = FD_INT2DTYPE(uncompressed_size);
    u8_unlock_mutex(&(zf->zipfile_lock));
    return size;}
}

static lispval zipgetfiles_prim(lispval zipfile)
{
  struct FD_ZIPFILE *zf = fd_consptr(fd_zipfile,zipfile,fd_zipfile_type);
  u8_lock_mutex(&(zf->zipfile_lock));
  if (zf->closed) {
    lispval errval = zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->zipfile_lock));
      return errval;}}
  {
    lispval files = FD_EMPTY_CHOICE;
    int numfiles = zip_get_num_files(zf->zip);
    int i = 0; while (i<numfiles) {
      u8_string name = (u8_string)zip_get_name(zf->zip,i,0);
      if (!(name)) i++;
      else {
        lispval lname = lispval_string(name);
        FD_ADD_TO_CHOICE(files,lname);
        i++;}}
    u8_unlock_mutex(&(zf->zipfile_lock));
    return files;}
}

static lispval zipfeatures_prim()
{
  lispval result = FD_EMPTY_CHOICE;
#if (HAVE_ZIP_SET_FILE_EXTRA)
  FD_ADD_TO_CHOICE(result,fd_intern("EXTRA"));
#endif
#if (HAVE_ZIP_SET_FILE_COMMENT)
  FD_ADD_TO_CHOICE(result,fd_intern("COMMENT"));
#endif
#if (HAVE_ZIP_SET_FILE_COMPRESSION)
  FD_ADD_TO_CHOICE(result,fd_intern("COMPRESSION"));
#endif
  return result;
}

/* Initialization */

FD_EXPORT int fd_init_ziptools(void) FD_LIBINIT_FN;

static long long int ziptools_init = 0;

FD_EXPORT int fd_init_ziptools()
{
  lispval ziptools_module;
  if (ziptools_init) return 0;

  ziptools_init = u8_millitime();
  ziptools_module =
    fd_new_cmodule("ZIPTOOLS",(FD_MODULE_SAFE),fd_init_ziptools);

  fd_zipfile_type = fd_register_cons_type("ZIPFILE");

  fd_unparsers[fd_zipfile_type]=unparse_zipfile;
  fd_recyclers[fd_zipfile_type]=recycle_zipfile;

  fd_idefn(ziptools_module,
           fd_make_cprim1("ZIPFILE?",iszipfile_prim,1));

  fd_idefn(ziptools_module,
           fd_make_cprim2x("ZIP/OPEN",zipopen_prim,1,
                           fd_string_type,FD_VOID,-1,FD_FALSE));
  fd_idefn(ziptools_module,
           fd_make_cprim1x("ZIP/MAKE",zipmake_prim,1,fd_string_type,FD_VOID));

  fd_idefn(ziptools_module,
           fd_make_cprim1x
           ("ZIP/CLOSE!",close_zipfile,1,fd_zipfile_type,FD_VOID));
  fd_defalias(ziptools_module,"ZIP/CLOSE","ZIP/CLOSE!");
  fd_defalias(ziptools_module,"ZIP/COMMIT!","ZIP/CLOSE!");

  fd_idefn(ziptools_module,
           fd_make_cprim1x("ZIP/OPEN?",zipfile_openp,1,fd_zipfile_type,FD_VOID));

  fd_idefn(ziptools_module,
           fd_make_cprim5x("ZIP/ADD!",zipadd_prim,3,
                           fd_zipfile_type,FD_VOID,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID,
                           -1,FD_FALSE,
                           -1,FD_TRUE));

  fd_idefn(ziptools_module,
           fd_make_cprim2x("ZIP/DROP!",zipdrop_prim,2,
                           fd_zipfile_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(ziptools_module,
           fd_make_cprim3x("ZIP/GET",zipget_prim,2,
                           fd_zipfile_type,FD_VOID,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID));

  fd_idefn(ziptools_module,
           fd_make_cprim2x("ZIP/EXISTS?",zipexists_prim,2,
                           fd_zipfile_type,FD_VOID,
                           fd_string_type,FD_VOID));

  fd_idefn(ziptools_module,
           fd_make_cprim2x("ZIP/MODTIME",zipmodtime_prim,2,
                           fd_zipfile_type,FD_VOID,
                           fd_string_type,FD_VOID));

  fd_idefn(ziptools_module,
           fd_make_cprim2x("ZIP/GETSIZE",zipgetsize_prim,2,
                           fd_zipfile_type,FD_VOID,
                           fd_string_type,FD_VOID));

  fd_idefn(ziptools_module,
           fd_make_cprim1x("ZIP/GETFILES",zipgetfiles_prim,1,
                           fd_zipfile_type,FD_VOID));

  fd_idefn(ziptools_module,
    fd_make_cprim0("ZIP/FEATURES",zipfeatures_prim));

  fd_idefn(ziptools_module,
           fd_make_cprim1x("ZIP/FILENAME",zipfilename_prim,1,
                           fd_zipfile_type,FD_VOID));

  fd_finish_module(ziptools_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
