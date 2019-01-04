/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/apply.h"

#include <libu8/u8signals.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>
#include <libu8/libu8io.h>

#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pwd.h>
#include <grp.h>

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>

/* Initialization */

static lispval umask_config_get(lispval var,void *data)
{
  u8_byte buf[256];
  u8_string procfile = u8_sprintf(buf,256,"/proc/self/umask");
#if HAVE_GETUMASK
  int val = getumask();
  return FD_MAKE_FIXNUM(val);
#else
  if (u8_file_existsp(procfile)) {
    u8_string contents = u8_filestring(procfile,NULL);
    int maskval = strtol(contents,NULL,10);
    u8_free(contents);
    return FD_MAKE_FIXNUM(maskval);}
#endif
  int val = umask(0);
  umask(val);
  return val;
}

static int umask_config_set(lispval var,lispval val,void *data)
{
  if (FD_FIXNUMP(val)) {
    long long newval = FD_FIX2INT(val);
    if ( (newval >= 0) && (newval < 512) ) {
      int oldval = umask((mode_t) newval);
      return (oldval != newval);}}
  return fd_type_error("umask","umask_config_set",val);
}

static lispval group_config_get(lispval var,void *data)
{
  int gid = getegid();
  return FD_MAKE_FIXNUM(gid);
}

static int group_config_set(lispval var,lispval val,void *data)
{
  if (FD_FIXNUMP(val)) {
    long long newval = FD_FIX2INT(val);
    if (newval >= 0) {
      int rv = setgid((u8_gid)newval);
      if (rv<0) {
        char buf[32], *str=u8_itoa10(newval,buf);
        u8_graberrno("group_config_set",u8_strdup(str));
        return -1;}
      else return rv;}}
  if (FD_STRINGP(val)) {
    struct group group_info, *info=NULL;
    unsigned char _strbuf[1024], *strbuf=_strbuf;
    size_t malloc_size=0;
    int rv = getgrnam_r(FD_CSTRING(val),&group_info,strbuf,1024,&info);
    while ( (rv < 0) && (errno == ERANGE) ) {
      if (malloc_size)
        strbuf=u8_realloc(strbuf,malloc_size*2);
      else {
        strbuf=u8_malloc(2048);
        malloc_size=2048;}
      errno = 0;
      rv = getgrnam_r(FD_CSTRING(val),&group_info,strbuf,1024,&info);}
    if (rv<0) {
      u8_graberrno("group_config_set",fd_lisp2string(val));
      return -1;}
    else if (info == NULL) {
      fd_seterr("NoSuchGroup","group_config_set",FD_CSTRING(val),FD_VOID);
      return -1;}
    else {
      u8_gid gid = info->gr_gid;
      int rv = setgid(gid);
      if (rv<0) {
        u8_graberrno("group_config_set",
                     u8_mkstring("%s:%d",FD_CSTRING(val),gid));
        return -1;}
      else return gid;}}
  return fd_type_error("group","group_config_set",val);
}

void fd_init_posix_c()
{

  u8_register_source_file(_FILEINFO);

  umask( FD_INIT_UMASK );

  fd_register_config
    ("UMASK",_("The UMASK of the current process"),
     umask_config_get,umask_config_set,NULL);

  fd_register_config
    ("GROUP",_("The default group for the ucrrent process"),
     group_config_get,group_config_set,NULL);
}
/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
