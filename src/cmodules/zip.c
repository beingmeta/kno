/* -*- Mode: C; -*- */

/* Copyright (C) 2007-2010 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id: crypto.c 4774 2010-01-15 14:43:07Z haase $";

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/frames.h"
#include "fdb/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8pathfns.h>

#include <sys/types.h>
#include <zip.h>

static fd_exception ZipFileError=_("Zip file error");

FD_EXPORT fd_ptr_type fd_zipfile_type;
fd_ptr_type fd_zipfile_type;
typedef struct FD_ZIPFILE {
  FD_CONS_HEADER;
  u8_string filename; int flags;
  u8_mutex lock; int closed;
  struct zip *zip;} FD_ZIPFILE;
typedef struct FD_ZIPFILE *fd_zipfile;

/* Error messages */

static fdtype znumerr(u8_context cxt,int zerrno,u8_string path)
{
  u8_byte buf[1024];
  zip_error_to_str(buf,1024,errno,zerrno);
  return fd_err(ZipFileError,cxt,
		u8_mkstring("(%s) %s",path,buf),
		FD_VOID);
}

static fdtype ziperr(u8_context cxt,fd_zipfile zf,fdtype irritant)
{
  u8_string details=
    u8_mkstring("(%s) %s",zf->filename,zip_strerror(zf->zip));
  fd_seterr(ZipFileError,cxt,details,fd_incref(irritant));
  return FD_ERROR_VALUE;
}

static fdtype zfilerr(u8_context cxt,fd_zipfile zf,struct zip_file *zfile,
		      fdtype irritant)
{
  u8_string details=
    u8_mkstring("(%s) %s",zf->filename,zip_file_strerror(zfile));
  zip_fclose(zfile);
  return fd_err(ZipFileError,cxt,details,irritant);
}

/* Zip file utilities */

static void recycle_zipfile(struct FD_CONS *c)
{
  struct FD_ZIPFILE *zf=(struct FD_ZIPFILE *)c;
  if (!(zf->closed)) zip_close(zf->zip);
  zf->closed=1;
  u8_destroy_mutex(&(zf->lock));
  u8_free(zf);
}

static int unparse_zipfile(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_ZIPFILE *zf=(struct FD_ZIPFILE *)x;
  u8_printf(out,"#<ZIPFILE '%s'%s>",
	    zf->filename,((zf->closed)?(" closed"):""));
  return 1;
}

static fdtype zipreopen(struct FD_ZIPFILE *zf,int locked)
{
  if (!(zf->closed)) return FD_FALSE;
  else {
    int errflag;
    struct zip *zip;
    if (!(locked)) u8_lock_mutex(&(zf->lock));
    if (!(zf->closed)) {
      if (!(locked)) u8_unlock_mutex(&(zf->lock));
      return FD_FALSE;}
    else zip=zip_open(zf->filename,zf->flags,&errflag);
    if (!(zip)) {
      fdtype errval=znumerr("zipreopen",errflag,zf->filename);
      if (!(locked)) u8_unlock_mutex(&(zf->lock));
      return errval;}
    else {
      zf->zip=zip; zf->closed=0;
      if (!(locked)) u8_unlock_mutex(&(zf->lock));
      return FD_TRUE;}}
}

/* Creating/opening zip files */

static fdtype zipopen(u8_string path,int zflags)
{
  int errflag=0;
  u8_string abspath=u8_abspath(path,NULL);
  struct zip *zip=zip_open(abspath,zflags,&errflag);
  if (zip) {
    struct FD_ZIPFILE *zf=u8_alloc(struct FD_ZIPFILE);
    FD_INIT_FRESH_CONS(zf,fd_zipfile_type);
    u8_init_mutex(&(zf->lock));
    zf->filename=abspath; zf->flags=zflags; zf->closed=0;
    zf->zip=zip;
    return FDTYPE_CONS(zf);}
  else {
    return znumerr("open_zipfile",errflag,abspath);}
}
static fdtype zipopen_prim(fdtype filename,fdtype create)
{
  if ((FD_FALSEP(create))||(FD_VOIDP(create)))
    return zipopen(FD_STRDATA(filename),ZIP_CHECKCONS);
  else return zipopen(FD_STRDATA(filename),ZIP_CREATE|ZIP_CHECKCONS);
}
static fdtype zipmake_prim(fdtype filename)
{
  return zipopen(FD_STRDATA(filename),ZIP_CREATE|ZIP_EXCL);
}

static fdtype close_zipfile(fdtype zipfile)
{
  struct FD_ZIPFILE *zf=FD_GET_CONS(zipfile,fd_zipfile_type,fd_zipfile);
  int retval;
  u8_lock_mutex(&(zf->lock));
  if (zf->closed) {
    u8_unlock_mutex(&(zf->lock));
    return FD_FALSE;}
  else retval=zip_close(zf->zip);
  if (retval) {
    u8_unlock_mutex(&(zf->lock));
    return ziperr("close_zipfile",zf,zipfile);}
  else {
    zf->closed=1;
    u8_unlock_mutex(&(zf->lock));
    return FD_TRUE;}
}

/* Adding to zip files */

static fdtype zipadd
  (struct FD_ZIPFILE *zf,u8_string name,struct zip_source *zsource)
{
  int index=zip_name_locate(zf->zip,name,0);
  int retval=((index<0)?(zip_add(zf->zip,name,zsource)):
	      (zip_replace(zf->zip,index,zsource)));
  if (retval<0)
    return ziperr("zipadd",zf,(fdtype)zf);
  else if (index<0) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype zipadd_prim(fdtype zipfile,fdtype filename,fdtype value)
{
  struct FD_ZIPFILE *zf=FD_GET_CONS(zipfile,fd_zipfile_type,fd_zipfile);
  unsigned char *data=NULL; size_t datalen=0;
  struct zip_source *zsource;
  if (FD_STRINGP(value)) {
    data=u8_strdup(FD_STRDATA(value));
    datalen=FD_STRLEN(value);}
  else if (FD_PACKETP(value)) {
    datalen=FD_PACKET_LENGTH(value);
    data=u8_malloc(datalen);
    memcpy(data,FD_PACKET_DATA(value),datalen);}
  else return fd_type_error("zip source","zipadd_prim",value);
  u8_lock_mutex(&(zf->lock));
  if (zf->closed) {
    fdtype errval=zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->lock));
      return errval;}}
  zsource=zip_source_buffer(zf->zip,data,datalen,1);
  if (zsource) {
    fdtype v=zipadd(zf,FD_STRDATA(filename),zsource);
    u8_unlock_mutex(&(zf->lock));
    return v;}
  else return ziperr("zipadd/source",zf,value);
}

static fdtype zipdrop_prim(fdtype zipfile,fdtype filename)
{
  struct FD_ZIPFILE *zf=FD_GET_CONS(zipfile,fd_zipfile_type,fd_zipfile);
  int index; int retval;
  u8_lock_mutex(&(zf->lock));
  if (zf->closed) {
    fdtype errval=zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->lock));
      return errval;}}
  index=zip_name_locate(zf->zip,FD_STRDATA(filename),0);
  if (index<0) {
    u8_unlock_mutex(&(zf->lock));
    return FD_FALSE;}
  else retval=zip_delete(zf->zip,index);
  if (retval<0) {
    u8_unlock_mutex(&(zf->lock));
    return ziperr("zipdrop",zf,(fdtype)zf);}
  else {
    u8_unlock_mutex(&(zf->lock));
    return FD_TRUE;}
}

static fdtype zipget_prim(fdtype zipfile,fdtype filename,fdtype stringp)
{
  struct FD_ZIPFILE *zf=FD_GET_CONS(zipfile,fd_zipfile_type,fd_zipfile);
  u8_string fname=FD_STRDATA(filename);
  struct zip_stat zstat; int zret;
  struct zip_file *zfile;
  int index;
  u8_lock_mutex(&(zf->lock));
  if (zf->closed) {
    fdtype errval=zipreopen(zf,1);
    if (FD_ABORTP(errval)) {
      u8_unlock_mutex(&(zf->lock));
      return errval;}}
  index=zip_name_locate(zf->zip,fname,0);
  if (index<0) {
    u8_unlock_mutex(&(zf->lock));
    return FD_FALSE;}
  else if (zret=zip_stat(zf->zip,FD_STRDATA(filename),0,&zstat)) {
    u8_unlock_mutex(&(zf->lock));
    return ziperr("zipget_prim/stat",zf,filename);}
  else if (zfile=zip_fopen(zf->zip,FD_STRDATA(filename),0)) {
    unsigned char *buf=u8_malloc(zstat.size+1);
    int size=zstat.size, block=0, read=0, togo=size;
    while ((togo>0)&&((block=zip_fread(zfile,buf+read,togo))>0)) {
      if (block<0) break;
      read=read+block; togo=togo-block;}
    if (togo>0) {
      u8_free(buf);
      u8_unlock_mutex(&(zf->lock));
      return zfilerr("zipget_prim",zf,zfile,filename);}
    zip_fclose(zfile);
    u8_unlock_mutex(&(zf->lock));
    if (FD_TRUEP(stringp))
      return fd_init_packet(NULL,size,buf);
    else return fd_init_string(NULL,size,buf);}
  else {
    u8_unlock_mutex(&(zf->lock));
    return ziperr("zipget_prim/fopen",zf,filename);}
}

/* Initialization */

FD_EXPORT int fd_init_zip(void) FD_LIBINIT_FN;

static int zip_init=0;

FD_EXPORT int fd_init_zip()
{
  fdtype zip_module;
  if (zip_init) return;
  fd_register_source_file(versionid);
  zip_init=1;
  zip_module=fd_new_module("ZIP",(FD_MODULE_SAFE));
  
  fd_zipfile_type=fd_register_cons_type("ZIPFILE");
  
  fd_unparsers[fd_zipfile_type]=unparse_zipfile;
  fd_recyclers[fd_zipfile_type]=recycle_zipfile;

  fd_idefn(zip_module,
	   fd_make_cprim2x("ZIP/OPEN",zipopen_prim,1,
			   fd_string_type,FD_VOID,-1,FD_FALSE));
  fd_idefn(zip_module,
	   fd_make_cprim1x("ZIP/MAKE",zipmake_prim,1,fd_string_type,FD_VOID));

  fd_idefn(zip_module,
	   fd_make_cprim1x
	   ("ZIP/CLOSE",close_zipfile,1,fd_zipfile_type,FD_VOID));

  fd_idefn(zip_module,
	   fd_make_cprim3x("ZIP/ADD!",zipadd_prim,3,
			   fd_zipfile_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));

  fd_idefn(zip_module,
	   fd_make_cprim2x("ZIP/DROP!",zipdrop_prim,2,
			   fd_zipfile_type,FD_VOID,
			   fd_string_type,FD_VOID));
  fd_idefn(zip_module,
	   fd_make_cprim3x("ZIP/GET",zipget_prim,2,
			   fd_zipfile_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));

  fd_finish_module(zip_module);
  fd_persist_module(zip_module);
}
