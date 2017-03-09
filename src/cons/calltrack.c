/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_FCNIDS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>
#include <libu8/u8contour.h>
#include <libu8/u8strings.h>

#include <errno.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

#if FD_USE_TLS
u8_tld_key _fd_calltracking_key;
#elif HAVE__THREAD
 __thread int fd_calltracking;
#else
int fd_calltracking;
#endif

static fd_exception NoSuchCalltrackSensor=
  _("designated calltrack sensor does not exist");
static fd_exception TooManyCalltrackSensors=
  _("Too many calltrack sensors");

FD_EXPORT fdtype fd_init_flonum(struct FD_FLONUM *ptr,double flonum);

static int needs_escape(u8_string string)
{
  const u8_byte *scan=string; int c;
  while ((c=u8_sgetc(&scan))>0)
    if (c>=128) return 1;
    else if (u8_isspace(c)) return 1;
    else {}
  return 0;
}

static void out_escaped(FILE *f,u8_string name)
{
  const u8_byte *scan=name;
  while (*scan) {
    int c=u8_sgetc(&scan);
    if (c=='"') fprintf(f,"\\\"");
    else if (c>=128) {
      fprintf(f,"\\u%04x",c);}
    else if (c=='\n') fputs("\\n",f);
    else if (c=='\r') fputs("\\r",f);
    else putc(c,f);}
}

/* Internal profiling support */

#if FD_CALLTRACK_ENABLED
#include <stdio.h>

u8_mutex calltrack_sensor_lock;

static struct FD_CALLTRACK_SENSOR calltrack_sensors[FD_MAX_CALLTRACK_SENSORS];
static int n_calltrack_sensors=0;

FD_EXPORT fd_calltrack_sensor fd_get_calltrack_sensor(u8_string id,int create)
{
  int i=0; u8_lock_mutex(&calltrack_sensor_lock);
  while (i<n_calltrack_sensors)
    if (strcmp(id,calltrack_sensors[i].name)==0) {
      u8_unlock_mutex(&calltrack_sensor_lock);
      return &(calltrack_sensors[i]);}
    else i++;
  if (create==0) {
    u8_unlock_mutex(&calltrack_sensor_lock);
    return NULL;}
  else if (i<FD_MAX_CALLTRACK_SENSORS) {
    calltrack_sensors[i].name=u8_strdup(id);
    calltrack_sensors[i].enabled=0;
    calltrack_sensors[i].intfcn=NULL;
    calltrack_sensors[i].dblfcn=NULL;
    u8_unlock_mutex(&calltrack_sensor_lock);
    return &calltrack_sensors[n_calltrack_sensors++];}
  else {
    fd_seterr(TooManyCalltrackSensors,"fd_get_calltrack_sensor",
              u8_strdup(id),FD_VOID);
    u8_unlock_mutex(&calltrack_sensor_lock);
    return NULL;}
}

FD_EXPORT fdtype fd_calltrack_sensors()
{
  int n=n_calltrack_sensors+1, i=0;
  fdtype *data=u8_alloc_n(n,fdtype);
  data[i++]=fd_intern("TIME");
  while (i<n) {
    data[i]=fd_intern(calltrack_sensors[i-1].name); i++;}
  return fd_init_vector(NULL,n,data);
}

FD_EXPORT fdtype fd_calltrack_sense(int trackall)
{
  int n=n_calltrack_sensors+1, i=0;
  fdtype *data=u8_alloc_n(n,fdtype), *write=data+1;
  data[0]=fd_init_flonum(NULL,u8_elapsed_time());
  while (i<n_calltrack_sensors)
    if ((trackall==0) &&
        (calltrack_sensors[i].enabled==0))
      write[i++]=(FD_FIXNUM_ZERO);
    else if (calltrack_sensors[i].intfcn) {
      long lv=calltrack_sensors[i].intfcn();
      fdtype dv=(fdtype)FD_INT(lv);
      write[i++]=dv;}
    else  if (calltrack_sensors[i].dblfcn) {
      double fv=calltrack_sensors[i].dblfcn();
      fdtype dv=fd_init_flonum(NULL,fv);
      write[i++]=dv;}
    else write[i++]=(FD_FIXNUM_ZERO);
  return fd_init_vector(NULL,n,data);
}

/* Generic calltrack */

#if (FD_USE_TLS)
static u8_tld_key calltrack_log_key;
struct CALLTRACK_DATA {
  FILE *calltrack_output;
  char *calltrack_filename;};

static void free_calltrack_log(void *ptr)
{
  struct CALLTRACK_DATA *data=ptr;
  if (data) {
    fclose(data->calltrack_output);
    u8_free(data->calltrack_filename); 
    u8_free(data);}
}
FD_FASTOP FILE *get_calltrack_logfile()
{
  struct CALLTRACK_DATA *cd=u8_tld_get(calltrack_log_key);
  if (cd)
    return cd->calltrack_output;
  else return NULL;
}
FD_EXPORT int fd_start_calltrack(u8_string filename)
{
  struct CALLTRACK_DATA *cd=u8_tld_get(calltrack_log_key);
  int current=0;
  FILE *output;
  if (cd) {
    current=1;
    fclose(cd->calltrack_output);
    u8_free(cd->calltrack_filename);
    u8_free(cd); cd=NULL;
    u8_tld_set(calltrack_log_key,NULL);}
  if (filename==NULL) {
    if (fd_calltracking) {
      u8_tld_set(_fd_calltracking_key,(void *)0);
      return 1;}
    else return 0;}
  if (*filename=='+')
    output=fopen(filename+1,"a+");
  else output=fopen(filename,"w+");
  if (output) {
    u8_log(LOGWARN,"CalltrackStart","=> %s",filename);
    u8_tld_set(_fd_calltracking_key,(void *)1);
    cd=u8_alloc(struct CALLTRACK_DATA);
    cd->calltrack_output=output;
    cd->calltrack_filename=u8_strdup(filename);
    fprintf(output,":TIME");
    {int i=0; while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled)
        fprintf(output," %s",calltrack_sensors[i++].name);
      else i++;
      fprintf(output,"\n");
      u8_tld_set(calltrack_log_key,cd);
      return current;}}
  else {
    u8_graberr(-1,"fd_start_calltrack",u8_strdup(filename));
    return -1;}
}
#else
static __thread FILE *calltrack_logfile;
static __thread char *calltrack_logfilename;
#define get_calltrack_logfile() (calltrack_logfile)
FD_EXPORT int fd_start_calltrack(u8_string filename)
{
  int retval=0; FILE *output;
  if ((calltrack_logfilename) &&
      (filename) &&
      (strcmp(filename,calltrack_logfilename)==0)) {
    fflush(calltrack_logfile); sync();
    return 0;}
  if (calltrack_logfile) {
    fclose(calltrack_logfile); retval=1;}
  if (filename==NULL) {
    u8_free(calltrack_logfilename);
    calltrack_logfilename=NULL;
    calltrack_logfile=NULL;
    fd_calltracking=0;
    return 1;}
  if (*filename=='+')
    output=fopen(filename+1,"a+");
  else output=fopen(filename,"w");
  if (output) {
    u8_log(LOGWARN,"CalltrackStart","=> %s",filename);
    fd_calltracking=1;
    calltrack_logfilename=u8_strdup(filename);
    calltrack_logfile=output;
    fprintf(output,":TIME");
    {int i=0; while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled)
        fprintf(output," %s",calltrack_sensors[i++].name);
      else i++;}
    fprintf(output,"\n");
    return retval;}
  else {
    u8_graberr(-1,"fd_start_calltrack",u8_strdup(filename));
    return -1;}
}
#endif
static void calltrack_call(u8_string name)
{
  FILE *output=get_calltrack_logfile();
  if (output) {
    double timer=u8_elapsed_time(); int i=0;
    if (needs_escape(name)) {
      fprintf(output,"> \"");
      out_escaped(output,name);
      fprintf(output,"\" %f",timer);}
    else fprintf(output,"> %s %f",name,timer);
    while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled)
        if (calltrack_sensors[i].dblfcn)
          fprintf(output," %f",calltrack_sensors[i++].dblfcn());
        else if (calltrack_sensors[i].intfcn)
          fprintf(output," %ld",calltrack_sensors[i++].intfcn());
        else fprintf(output," 0");
      else i++;
    fprintf(output,"\n");}
}
static void calltrack_return(u8_string name)
{
  FILE *output=get_calltrack_logfile();
  if (output) {
    double timer=u8_elapsed_time(); int i=0;
    if (needs_escape(name)) {
      fprintf(output,"< \"");
      out_escaped(output,name);
      fprintf(output,"\" %f",timer);}
    else fprintf(output,"< %s %f",name,timer);
    while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled)
        if (calltrack_sensors[i].dblfcn)
          fprintf(output," %f",calltrack_sensors[i++].dblfcn());
        else if (calltrack_sensors[i].intfcn)
          fprintf(output," %ld",calltrack_sensors[i++].intfcn());
        else fprintf(output," 0");
      else i++;
    fprintf(output,"\n");}
}
FD_EXPORT
void fd_calltrack_call(u8_string name)
{
  calltrack_call(name);
}

FD_EXPORT
void fd_calltrack_return(u8_string name)
{
  calltrack_return(name);
}

#else
FD_EXPORT const int fd_calltrack_available=1;
#define get_calltrack_logfile() (NULL)
#define calltrack_call(x) (x)
#define calltrack_return(x) (x)
#endif

/* Calltrack configuration */

static int set_calltrack(fdtype ignored,fdtype path_arg,
                         void U8_MAYBE_UNUSED *data)
{
#if FD_CALLTRACK_ENABLED
  if (FD_STRINGP(path_arg))
    return fd_start_calltrack(FD_STRDATA(path_arg));
  else if (FD_FALSEP(path_arg))
    return fd_start_calltrack(NULL);
  else if (FD_TRUEP(path_arg))
    return fd_start_calltrack("+calltrack.log");
  else {
    fd_seterr(fd_TypeError,"config_set_calltrack",
              u8_strdup(_("not a pathname")),fd_incref(path_arg));
    return -1;}
#else
  fd_seterr(fd_ProfilingDisabled,"config_set_calltrack",NULL,FD_VOID);
  return -1;
#endif
}

static fdtype get_calltrack(fdtype ignored,void *lval)
{
#if FD_CALLTRACK_ENABLED
#if FD_USE_TLS
  struct CALLTRACK_DATA *info=u8_tld_get(calltrack_log_key);
  if (info) return fdtype_string(info->calltrack_filename);
  else return FD_EMPTY_CHOICE;
#else
  if (calltrack_logfilename)
    return fdtype_string(calltrack_logfilename);
  else return FD_EMPTY_CHOICE;
#endif
#else
    return FD_EMPTY_CHOICE;
#endif
}

static fdtype calltrack_sense, calltrack_ignore;

static int config_set_calltrack_sensors(fdtype sym,fdtype value,
                                        void U8_MAYBE_UNUSED *data)
{
  u8_string sensor_name; fd_calltrack_sensor sensor;
  if (FD_STRINGP(value)) sensor_name=FD_STRDATA(value);
  else if (FD_SYMBOLP(value)) sensor_name=FD_SYMBOL_NAME(value);
  else {
    fd_seterr(fd_TypeError,"config_set_calltrack_sensor",NULL,value);
    return -1;}
  sensor=fd_get_calltrack_sensor(sensor_name,0);
  if (sensor==NULL) {
    fd_seterr(NoSuchCalltrackSensor,"config_set_calltrack_sensor",NULL,value);
    return -1;}
  if (FD_EQ(sym,calltrack_sense))
    if (sensor->enabled) return 0;
    else {sensor->enabled=1; return 1;}
  else if (FD_EQ(sym,calltrack_ignore))
    if (!(sensor->enabled)) return 0;
    else {sensor->enabled=0; return 1;}
  else {
    fd_seterr(fd_TypeError,"config_set_calltrack_sensor",NULL,value);
    return -1;}
}
static fdtype config_get_calltrack_sensors(fdtype sym,void U8_MAYBE_UNUSED *data)
{
  if (sym==calltrack_sense) {
    fdtype results=FD_EMPTY_CHOICE; int i=0;
    u8_lock_mutex(&calltrack_sensor_lock);
    while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled) {
        fdtype sensorname=fd_make_string(NULL,-1,calltrack_sensors[i].name);
        FD_ADD_TO_CHOICE(results,sensorname);
        i++;}
      else i++;
    u8_unlock_mutex(&calltrack_sensor_lock);
    return fd_simplify_choice(results);}
  else if (sym==calltrack_ignore) {
    fdtype results=FD_EMPTY_CHOICE; int i=0;
    u8_lock_mutex(&calltrack_sensor_lock);
    while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled) i++;
      else {
        fdtype sensorname=fd_make_string(NULL,-1,calltrack_sensors[i].name);
        FD_ADD_TO_CHOICE(results,sensorname);
        i++;}
    u8_unlock_mutex(&calltrack_sensor_lock);
    return fd_simplify_choice(results);}
  else return FD_EMPTY_CHOICE;
}

FD_EXPORT fdtype fd_calltrack_apply(fdtype fp,int n,fdtype *args)
{
  struct FD_FUNCTION *f=FD_DTYPE2FCN(fp);
  fdtype result; u8_byte buf[64]; u8_string name;
  if (f->fcn_name==NULL) {
    sprintf(buf,"FN%lx",(unsigned long int)f); name=buf;}
  else name=f->fcn_name;
  calltrack_call(name);
  result=fd_deterministic_apply((fdtype)f,n,args);
  /* If we don't compile with calltrack, we don't get pointer checking.
     We may want to change this at some point and move the pointer checking
     into the fd_determinstic_apply (fd_dapply_ct/fd_dapply) code. */
  if (!(FD_CHECK_PTR(result)))
    return fd_err(fd_BadPtr,"fd_dapply",f->fcn_name,(fdtype)f);
  else if (FD_TROUBLEP(result))
    if (u8_current_exception==NULL) {
      fd_exception ex=fd_retcode_to_exception(result);
      if (ex) result=fd_err(ex,NULL,NULL,FD_VOID);}
  calltrack_return(name);
  return result;
}

void fd_init_calltrack_c()
{
  u8_register_source_file(_FILEINFO);

  fd_register_config("CALLTRACK",
                     _("File used for calltrack profiling (#f disables calltrack)"),
                     get_calltrack,set_calltrack,NULL);

#if (FD_USE_TLS)
  u8_new_threadkey(&_fd_calltracking_key,NULL);
  u8_new_threadkey(&calltrack_log_key,free_calltrack_log);
#endif

  calltrack_sense=fd_intern("CALLTRACK/SENSE");
  calltrack_ignore=fd_intern("CALLTRACK/IGNORE");

  fd_register_config
    ("CALLTRACK/SENSE",_("Active calltrack sensors"),
     config_get_calltrack_sensors,config_set_calltrack_sensors,NULL);
  fd_register_config
    ("CALLTRACK/IGNORE",_("Ignore calltrack sensors"),
     config_get_calltrack_sensors,config_set_calltrack_sensors,NULL);

  u8_init_mutex(&calltrack_sensor_lock);
}
