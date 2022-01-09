/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"

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
  return KNO_MAKE_FIXNUM(val);
#else
  if (u8_file_existsp(procfile)) {
    u8_string contents = u8_filestring(procfile,NULL);
    int maskval = strtol(contents,NULL,10);
    u8_free(contents);
    return KNO_MAKE_FIXNUM(maskval);}
#endif
  int val = umask(0);
  umask(val);
  return val;
}

static int umask_config_set(lispval var,lispval val,void *data)
{
  if (KNO_FIXNUMP(val)) {
    long long newval = KNO_FIX2INT(val);
    if ( (newval >= 0) && (newval < 512) ) {
      int oldval = umask((mode_t) newval);
      return (oldval != newval);}}
  return kno_type_error("umask","umask_config_set",val);
}

static lispval group_config_get(lispval var,void *data)
{
  int gid = getegid();
  return KNO_MAKE_FIXNUM(gid);
}

static int group_config_set(lispval var,lispval val,void *data)
{
  if (KNO_FIXNUMP(val)) {
    long long newval = KNO_FIX2INT(val);
    if (newval >= 0) {
      int rv = setgid((u8_gid)newval);
      if (rv<0) {
        char buf[32], *str=u8_itoa10(newval,buf);
        u8_graberrno("group_config_set",u8_strdup(str));
        return -1;}
      else return rv;}}
  if (KNO_STRINGP(val)) {
    struct group group_info, *info=NULL;
    unsigned char _strbuf[1024], *strbuf=_strbuf;
    size_t malloc_size=0;
    int rv = getgrnam_r(KNO_CSTRING(val),&group_info,strbuf,1024,&info);
    while ( (rv < 0) && (errno == ERANGE) ) {
      if (malloc_size)
        strbuf=u8_realloc(strbuf,malloc_size*2);
      else {
        strbuf=u8_malloc(2048);
        malloc_size=2048;}
      errno = 0;
      rv = getgrnam_r(KNO_CSTRING(val),&group_info,strbuf,1024,&info);}
    if (rv<0) {
      u8_graberrno("group_config_set",kno_lisp2string(val));
      return -1;}
    else if (info == NULL)
      return KNO_ERR(-1,"NoSuchGroup","group_config_set",KNO_CSTRING(val),KNO_VOID);
    else {
      u8_gid gid = info->gr_gid;
      int rv = setgid(gid);
      if (rv<0) {
        u8_graberrno("group_config_set",
                     u8_mkstring("%s:%d",KNO_CSTRING(val),gid));
        return -1;}
      else return gid;}}
  return kno_type_error("group","group_config_set",val);
}

void kno_init_posix_c()
{

  u8_register_source_file(_FILEINFO);

  umask( KNO_INIT_UMASK );

  kno_register_config
    ("UMASK",_("The UMASK of the current process"),
     umask_config_get,umask_config_set,NULL);

  kno_register_config
    ("GROUP",_("The default group for the ucrrent process"),
     group_config_get,group_config_set,NULL);
}
