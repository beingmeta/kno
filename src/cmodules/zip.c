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

#include <sys/types.h>
#include <zip.h>

static fdtype znumerr(u8_context cxt,int zerrno,fdtype irritant)
{
  u8_byte buf[1024];
  zip_error_to_str(buf,1024,errno,zerrno);
  return fd_err(ZipFileError,cxt,u8_strdup(buf),irritant);
}

static fdtype ziperr(u8_context cxt,fd_zipfile zf,fdtype irritant)
{
  u8_string details=u8_mkstring("(%s) %s",zf->filename,zip_strerror(zf));
  return fd_err(ZipFileError,cxt,details,irritant);
}

static fdtype zfilerr(u8_context cxt,fd_zipfile zf,struct zipfile *zfile,
			 fdtype irritant)
{
  u8_string details=
    u8_mkstring("(%s) %s",zf->filename,zip_file_strerror(zfile));
  zip_fclose(zfile);
  return fd_err(ZipFileError,cxt,details,irritant);
}

/* Creating zip files */

FD_EXPORT fd_ptr_type fd_zipfile_type;
fd_ptr_type fd_zipfile_type;
typedef struct FD_ZIPFILE {
  FD_CONS_HEADER;
  u8_string filename; int flags;
  u8_mutex lock; struct zip *zip;} FD_ZIPFILE;
typedef struct FD_ZIPFILE *fd_zipfile;
static fd_exception ZipFileError=_("Zip file error");

static void recycle_zipfile(struct FD_CONS *c)
{
  struct FD_ZIPFILE *zf=(struct FD_ZIPFILE *)c;
  zip_close(zf->zip);
  u8_destroy_mutex(&(zf->lock));
  u8_free(zf);
}

static int unparse_zipfile(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_ZIPFILE *zf=(struct FD_ZIPFILE *)x;
  u8_printf(out,"#<ZIPFILE '%s'>",zf->filename);
  return 1;
}
static fdtype zipopen(u8_string abspath,int zflags)
{
  int errflag=0;
  struct zip *zip=zip_open(abspath,zflags,&errflag);
  if (zip) {
    struct FD_ZIPFILE *zf=u8_alloc(struct FD_ZIPFILE);
    FD_INIT_FRESH_CONS(zf,fd_zipfile_type);
    u8_init_mutex(&(zf->lock));
    zf->filename=abspath; zf->flags=zflags;
    zf->zip=zip;
    return FDTYPE_CONS(zf);}
  else {
    u8_free(abspath);
    return znumerr("open_zipfile",errflag,filename);}
}
static fdtype zipopen_prim(fdtype filename)
{
  return zipopen(FD_STRDATA(filename),ZIP_CHECKCONS);
}
static fdtype zipmake_prim(fdtype filename)
{
  return zipopen(FD_STRDATA(filename),ZIP_CREATE|ZIP_EXCL);
}
static fdtype zipuse_prim(fdtype filename)
{
  return zipopen(FD_STRDATA(filename),ZIP_CREATE);
}

static fdtype close_zipfile(fdtype zipfile)
{
  struct FD_ZIPFILE *zf=FD_GET_CONS(zipfile,fd_zipfile_type,fd_zipfile);
  int retval=zip_close(zf->zip);
  if (retval) return zipmerr("close_zipfile",zf,zipfile);
  else return FD_TRUE;
}

/* Adding to zip files */

#if 0
static fdtype zipcheckfile(struct zip *z,u8_string name)
{
  u8_string buf[1024];
  u8_byte *start=((name[0]=='/')?(name+1):(name));
  u8_byte sep=strchr(start,'/');
  while (sep) {
    strncpy(buf,start,sep-start); buf[sep-start]='\0';
    
  }    
}
#endif

static fdtype zipadd
  (struct FD_ZIPFILE *zf,u8_string name,struct zip_source *zsource)
{
  int index=zip_name_locate(zf->zip,name,0);
  int retval;
  if (index<0) retval=zip_add(zf->zip,name,zsource);
  else retval=zip_replace(zf->zip,index,zsource);
  if (retval<0) {
    zip_source_free(zsource);
    return ziperr("zipadd",zf,(fdtype)zf);}
  else {
    zip_source_free(zsource);
    return FD_VOID;}
}

static fdtype zipadd_prim(fdtype zipfile,fdtype filename,fdtype value)
{
  struct FD_ZIPFILE *zf=FD_GET_CONS(zipfile,fd_zipfile_type,fd_zipfile);
  struct zip_source *zsource=
    ((FD_STRINGP(value))?
     (zip_source_buffer(zf,FD_STRDATA(value),FD_STRLEN(value))):
     (FD_PACKETP(value))?
     (zip_source_buffer(zf,FD_PACKET_DATA(value),FD_PACKET_LENGTH(value))):
     (NULL));
  if (zsource)
    return zipadd(zipfile,FD_STRDATA(filename),zsource);
  else return fd_type_error("zip source","zipadd_prim",value);
}

static fdtype zipget_prim(fdtype zipfile,fdtype filename,fdtype stringp)
{
  struct FD_ZIPFILE *zf=FD_GET_CONS(zipfile,fd_zipfile_type,fd_zipfile);
  u8_string fname=FD_STRDATA(filename);
  int index=zip_name_locate(zf->zip,fname,0);
  struct zip_stat zstat; int zret;
  if (index<0) return FD_FALSE;
  else if (zret=zip_stat(zf->zip,FD_STRDATA(filename),0,&zstat))
    return ziperr("zipget_prim/stat",zf,filename);
  else {
    struct zip_file *zfile=zip_fopen(zf->zip,FD_STRDATA(filename),0);
    unsigned char *buf=u8_malloc(zstat.size+1);
    int size=zstat.size, block=0, read=0, togo=size;
    while ((togo>0)&&((block=zip_fread(zfile,buf+read,togo))>0)) {
      if (block<0) break;
      read=read+block; togo=togo-block;}
    if (togo>0) {
      u8_free(buf);
      return zfilerr("zipget_prim",zf,zfile,filename);}
    zip_fclose(zipfile);
    if (FD_TRUEP(stringp))
      return fd_init_packet(NULL,size,buf);
    else return fd_init_string(NULL,size,buf);}
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
	   fd_make_cprim1x("ZIP/OPEN",zipopen_prim,1,fd_string_type,FD_VOID));
  fd_idefn(zip_module,
	   fd_make_cprim1x("ZIP/MAKE",zipmake_prim,1,fd_string_type,FD_VOID));
  fd_idefn(zip_module,
	   fd_make_cprim1x("ZIP/USE",zipuse_prim,1,fd_string_type,FD_VOID));
  fd_idefn(zip_module,
	   fd_make_cprim3x("ZIP/ADD!",zipadd_prim,3,
			   fd_zipfile_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(zip_module,
	   fd_make_cprim3x("ZIP/GET",zipget_prim,2,
			   fd_zipfile_type,FD_VOID,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID));

  fd_finish_module(zip_module);
  fd_persist_module(zip_module);
}
