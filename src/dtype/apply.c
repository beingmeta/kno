/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_INLINE_PPTRS 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/apply.h"

#include <libu8/u8printf.h>

#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <stdarg.h>

fd_applyfn fd_applyfns[FD_TYPE_MAX];
/* This is set if the type is a CONS with a FUNCTION header */
short fd_functionp[FD_TYPE_MAX];

u8_condition fd_apply_context="APPLY";

fd_exception fd_NotAFunction=_("calling a non function");
fd_exception fd_TooManyArgs=_("too many arguments");
fd_exception fd_TooFewArgs=_("too few arguments");
fd_exception fd_NoDefault=_("no default value for #default argument");
fd_exception fd_ProfilingDisabled=_("profiling not built");

static fd_exception NoSuchCalltrackSensor=
  _("designated calltrack sensor does not exist");
static fd_exception TooManyCalltrackSensors=
  _("Too many calltrack sensors");

FD_EXPORT fdtype fd_init_double(struct FD_DOUBLE *ptr,double flonum);

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

#if FD_THREADS_ENABLED
u8_mutex calltrack_sensor_lock;
#endif

static struct FD_CALLTRACK_SENSOR calltrack_sensors[FD_MAX_CALLTRACK_SENSORS];
static int n_calltrack_sensors=0;

FD_EXPORT fd_calltrack_sensor fd_get_calltrack_sensor(u8_string id,int create)
{
  int i=0; fd_lock_mutex(&calltrack_sensor_lock);
  while (i<n_calltrack_sensors)
    if (strcmp(id,calltrack_sensors[i].name)==0) {
      fd_unlock_mutex(&calltrack_sensor_lock);
      return &(calltrack_sensors[i]);}
    else i++;
  if (create==0) {
    fd_unlock_mutex(&calltrack_sensor_lock);
    return NULL;}
  else if (i<FD_MAX_CALLTRACK_SENSORS) {
    calltrack_sensors[i].name=u8_strdup(id);
    calltrack_sensors[i].enabled=0;
    calltrack_sensors[i].intfcn=NULL;
    calltrack_sensors[i].dblfcn=NULL;
    fd_unlock_mutex(&calltrack_sensor_lock);
    return &calltrack_sensors[n_calltrack_sensors++];}
  else {
    fd_seterr(TooManyCalltrackSensors,"fd_get_calltrack_sensor",
              u8_strdup(id),FD_VOID);
    fd_unlock_mutex(&calltrack_sensor_lock);
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
  data[0]=fd_init_double(NULL,u8_elapsed_time());
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
      fdtype dv=fd_init_double(NULL,fv);
      write[i++]=dv;}
    else write[i++]=(FD_FIXNUM_ZERO);
  return fd_init_vector(NULL,n,data);
}

/* Generic calltrack */

#if (FD_USE_TLS)
static u8_tld_key calltrack_log_key;
struct CALLTRACK_DATA {
  FILE *f; char *filename;};

static void free_calltrack_log(void *ptr)
{
  struct CALLTRACK_DATA *data=ptr;
  if (data) {
    fclose(data->f); u8_free(data->filename); u8_free(data);}
}
FD_FASTOP FILE *get_calltrack_logfile()
{
  struct CALLTRACK_DATA *cd=u8_tld_get(calltrack_log_key);
  if (cd) return cd->f; else return NULL;
}
FD_EXPORT int fd_start_calltrack(u8_string filename)
{
  struct CALLTRACK_DATA *cd=u8_tld_get(calltrack_log_key);
  int current=0;
  FILE *f;
  if (cd) {
    current=1; fclose(cd->f);
    u8_free(cd->filename);
    u8_free(cd); cd=NULL;
    u8_tld_set(calltrack_log_key,NULL);}
  if (filename==NULL) return 1;
  if (*filename=='+')
    f=fopen(filename+1,"a+");
  else f=fopen(filename,"w+");
  if (f) {
    fprintf(f,"# Calltrack start\n");
    cd=u8_alloc(struct CALLTRACK_DATA);
    cd->f=f; cd->filename=u8_strdup(filename);
    fprintf(f,":TIME");
    {int i=0; while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled)
        fprintf(f," %s",calltrack_sensors[i++].name);
      else i++;
    fprintf(f,"\n");
    u8_tld_set(calltrack_log_key,cd);
    return current;}}
  else {
    u8_graberr(-1,"fd_start_calltrack",u8_strdup(filename));
    return -1;}
}
#else
static FD_THREADVAR FILE *calltrack_logfile;
static FD_THREADVAR char *calltrack_logfilename;
#define get_calltrack_logfile() (calltrack_logfile)
FD_EXPORT int fd_start_calltrack(u8_string filename)
{
  int retval=0; FILE *f;
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
    return 1;}
  if (*filename=='+')
    f=fopen(filename+1,"a+");
  else f=fopen(filename,"w");
  if (f) {
    fprintf(f,"# Calltrack start\n");
    calltrack_logfilename=u8_strdup(filename);
    calltrack_logfile=f;
    fprintf(f,":TIME");
    {int i=0; while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled)
        fprintf(f," %s",calltrack_sensors[i++].name);
      else i++;}
    fprintf(f,"\n");
    return retval;}
  else {
    u8_graberr(-1,"fd_start_calltrack",u8_strdup(filename));
    return -1;}
}
#endif
static void calltrack_call(u8_string name)
{
  FILE *f=get_calltrack_logfile();
  if (f) {
    double timer=u8_elapsed_time(); int i=0;
    if (needs_escape(name)) {
      fprintf(f,"> \"");
      out_escaped(f,name);
      fprintf(f,"\" %f",timer);}
    else fprintf(f,"> %s %f",name,timer);
    while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled)
        if (calltrack_sensors[i].dblfcn)
          fprintf(f," %f",calltrack_sensors[i++].dblfcn());
        else if (calltrack_sensors[i].intfcn)
          fprintf(f," %ld",calltrack_sensors[i++].intfcn());
        else fprintf(f," 0");
      else i++;
    fprintf(f,"\n");}
}
static void calltrack_return(u8_string name)
{
  FILE *f=get_calltrack_logfile();
  if (f) {
    double timer=u8_elapsed_time(); int i=0;
    if (needs_escape(name)) {
      fprintf(f,"< \"");
      out_escaped(f,name);
      fprintf(f,"\" %f",timer);}
    else fprintf(f,"< %s %f",name,timer);
    while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled)
        if (calltrack_sensors[i].dblfcn)
          fprintf(f," %f",calltrack_sensors[i++].dblfcn());
        else if (calltrack_sensors[i].intfcn)
          fprintf(f," %ld",calltrack_sensors[i++].intfcn());
        else fprintf(f," 0");
      else i++;
    fprintf(f,"\n");}
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
#define get_calltrack_logfile() (NULL)
#define calltrack_call(x) (x)
#define calltrack_return(x) (x)
#endif

/* Calltrack configuration */

static int set_calltrack(fdtype ignored,fdtype path_arg,void MAYBE_UNUSED *data)
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
  if (info) return fdtype_string(info->filename);
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
                                        void MAYBE_UNUSED *data)
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
static fdtype config_get_calltrack_sensors(fdtype sym,void MAYBE_UNUSED *data)
{
  if (sym==calltrack_sense) {
    fdtype results=FD_EMPTY_CHOICE; int i=0;
    fd_lock_mutex(&calltrack_sensor_lock);
    while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled) {
        fdtype sensorname=fd_make_string(NULL,-1,calltrack_sensors[i].name);
        FD_ADD_TO_CHOICE(results,sensorname);
        i++;}
      else i++;
    fd_unlock_mutex(&calltrack_sensor_lock);
    return fd_simplify_choice(results);}
  else if (sym==calltrack_ignore) {
    fdtype results=FD_EMPTY_CHOICE; int i=0;
    fd_lock_mutex(&calltrack_sensor_lock);
    while (i<n_calltrack_sensors)
      if (calltrack_sensors[i].enabled) i++;
      else {
        fdtype sensorname=fd_make_string(NULL,-1,calltrack_sensors[i].name);
        FD_ADD_TO_CHOICE(results,sensorname);
        i++;}
    fd_unlock_mutex(&calltrack_sensor_lock);
    return fd_simplify_choice(results);}
  else return FD_EMPTY_CHOICE;
}

/* Instrumented apply */

#if FD_CALLTRACK_ENABLED
FD_EXPORT fdtype _fd_dapply_ct(fdtype fp,int n,fdtype *args);
FD_EXPORT fdtype fd_dapply(fdtype fp,int n,fdtype *args)
{
  struct FD_FUNCTION *f=FD_DTYPE2FCN(fp);
  fdtype result; u8_byte buf[64]; u8_string name;
  if (f->name==NULL) {
    sprintf(buf,"FN%lx",(unsigned long int)f); name=buf;}
  else name=f->name;
  calltrack_call(name);
  result=_fd_dapply_ct((fdtype)f,n,args);
  /* If we don't compile with calltrack, we don't get pointer checking.
     We may want to change this at some point and move the pointer checking
     into the FD_DAPPLY (fd_dapply_ct/fd_dapply) code. */
  if (!(FD_CHECK_PTR(result)))
    return fd_err(fd_BadPtr,"fd_dapply",f->name,(fdtype)f);
  else if (FD_TROUBLEP(result))
    if (u8_current_exception==NULL) {
      fd_exception ex=fd_retcode_to_exception(result);
      if (ex) result=fd_err(ex,NULL,NULL,FD_VOID);}
  calltrack_return(name);
  return result;
}
#define FD_DAPPLY _fd_dapply_ct
#else
#define FD_DAPPLY fd_dapply
#endif

/* Calling primitives */

static fdtype dcall0(struct FD_FUNCTION *f)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall0(f);
  else return f->handler.call0();
}
static fdtype dcall1(struct FD_FUNCTION *f,fdtype arg1)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall1(f,arg1);
  else return f->handler.call1(arg1);
}
static fdtype dcall2(struct FD_FUNCTION *f,fdtype arg1,fdtype arg2)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall2(f,arg1,arg2);
  else return f->handler.call2(arg1,arg2);
}
static fdtype dcall3(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall3(f,arg1,arg2,arg3);
  else return f->handler.call3(arg1,arg2,arg3);
}
static fdtype dcall4(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall4(f,arg1,arg2,arg3,arg4);
  else return f->handler.call4(arg1,arg2,arg3,arg4);
}
static fdtype dcall5(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                      fdtype arg5)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall5(f,arg1,arg2,arg3,arg4,arg5);
  else return f->handler.call5(arg1,arg2,arg3,arg4,arg5);
}
static fdtype dcall6(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                      fdtype arg5,fdtype arg6)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall6(f,arg1,arg2,arg3,arg4,arg5,arg6);
  else return f->handler.call6(arg1,arg2,arg3,arg4,arg5,arg6);
}

static fdtype dcall7(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                      fdtype arg5,fdtype arg6,fdtype arg7)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall7(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7);
  else return f->handler.call7(arg1,arg2,arg3,arg4,arg5,arg6,arg7);
}

static fdtype dcall8(struct FD_FUNCTION *f,
                     fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                     fdtype arg5,fdtype arg6,fdtype arg7,fdtype arg8)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall8(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8);
  else return f->handler.call8(arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8);
}

static fdtype dcall9(struct FD_FUNCTION *f,
                     fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                     fdtype arg5,fdtype arg6,fdtype arg7,fdtype arg8,
                     fdtype arg9)
{
  if (FD_EXPECT_FALSE(f->xprim))
    return f->handler.xcall9(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8,arg9);
  else return f->handler.call9(arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8,arg9);
}

/* Generic calling function */

FD_EXPORT fdtype FD_DAPPLY(fdtype fp,int n,fdtype *argvec)
{
  fd_ptr_type ftype=FD_PRIM_TYPE(fp);
  if (FD_PPTRP(fp)) fp=fd_pptr_ref(fp);
  if (fd_functionp[ftype]) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(fp);
    fdtype argbuf[8], *args;
    if (FD_EXPECT_FALSE(f->arity<0)) {
      if (n<(f->min_arity))
        return fd_err(fd_TooFewArgs,"fd_dapply",f->name,FDTYPE_CONS(f));
      else {
        if (FD_EXPECT_FALSE((f->xprim) &&  (f->handler.fnptr==NULL))) {
          int ctype=FD_CONS_TYPE(f);
          return fd_applyfns[ctype]((fdtype)f,n,argvec);}
        else if (f->xprim)
          return f->handler.xcalln((struct FD_FUNCTION *)fp,n,argvec);
        else return f->handler.calln(n,argvec);}}
    /* Fill in the rest of the argvec */
    if (FD_EXPECT_TRUE((n <= f->arity) && (n>=f->min_arity))) {
      if (FD_EXPECT_FALSE(n<f->arity)) {
        /* Fill in defaults */
        int i=0; fdtype *defaults=f->defaults;
        if (f->arity<=8) args=argbuf;
        else args=u8_alloc_n((f->arity),fdtype);
        while (i<n) {
          fdtype a=argvec[i];
          if (a==FD_DEFAULT_VALUE) {
            fdtype d=((defaults)?(defaults[i]):(FD_VOID));
            if (FD_VOIDP(d)) args[i]=a;
            else {
              fd_incref(d); args[i]=d;}}
          else args[i]=a;
          i++;}
        if (defaults)
          while (i<f->arity) {
            fdtype d=defaults[i];
            args[i]=d; fd_incref(d);
            i++;}
        else while (i<f->arity) {args[i]=FD_VOID; i++;}}
      else args=argvec;
      /* Check typeinfo */
      if (FD_EXPECT_FALSE((f->typeinfo!=NULL))) {
        /* Check typeinfo */
        int *typeinfo=f->typeinfo;
        int i=0;
        while (i<n)
          if (typeinfo[i]>=0)
            if (FD_PTR_TYPEP(args[i],typeinfo[i])) i++;
        /* Don't signal errors on unspecified (VOID) args. */
            else if (FD_VOIDP(args[i])) i++;
            else {
              u8_string type_name=
                ((typeinfo[i]<256) ? (fd_type_names[typeinfo[i]]) : (NULL));
              if (type_name)
                return fd_type_error(type_name,f->name,args[i]);
              else return fd_type_error(type_name,f->name,args[i]);}
          else i++;}
      if (FD_EXPECT_FALSE((f->xprim) &&  (f->handler.fnptr==NULL))) {
        int ctype=FD_CONS_TYPE(f);
        if ((args==argbuf) || (args==argvec))
          return fd_applyfns[ctype]((fdtype)f,n,args);
        else {
          fdtype retval=fd_applyfns[ctype]((fdtype)f,n,args);
          u8_free(args);
          return retval;}}
      else switch (f->arity) {
        case 0: return dcall0(f);
        case 1: return dcall1(f,args[0]);
        case 2: return dcall2(f,args[0],args[1]);
        case 3: return dcall3(f,args[0],args[1],args[2]);
        case 4: return dcall4(f,args[0],args[1],args[2],args[3]);
        case 5: return dcall5(f,args[0],args[1],args[2],args[3],args[4]);
        case 6: return dcall6(f,args[0],args[1],args[2],args[3],args[4],args[5]);
        case 7: return dcall7(f,args[0],args[1],args[2],args[3],
                              args[4],args[5],args[6]);
        case 8: return dcall8(f,args[0],args[1],args[2],args[3],
                              args[4],args[5],args[6],args[7]);
        case 9: return dcall9(f,args[0],args[1],args[2],args[3],
                              args[4],args[5],args[6],args[7],args[8]);
        default:
          if ((args==argbuf) || (args==argvec))
            return f->handler.calln(n,args);
          else {
            fdtype retval=f->handler.calln(n,args);
            u8_free(args);
            return retval;}}}
    else {
      fd_exception ex=((n>f->arity) ? (fd_TooManyArgs) : (fd_TooFewArgs));
      return fd_err(ex,"fd_dapply",f->name,FDTYPE_CONS(f));}}
  else if (fd_applyfns[ftype])
    return fd_applyfns[ftype](fp,n,argvec);
  else return fd_type_error("applicable","DAPPLY",fp);
}

/* Calling non-deterministically */
static fdtype ndapply_loop
  (struct FD_FUNCTION *f,fdtype *results,int *typeinfo,
   int i,int n,fdtype *nd_args,fdtype *d_args)
{
  if (i==n) {
    fdtype value=fd_dapply((fdtype)f,n,d_args);
    if (FD_ABORTP(value)) return value;
    else {
      value=fd_finish_call(value);
      if (FD_ABORTP(value)) return value;
      FD_ADD_TO_CHOICE(*results,value);}}
  else if (FD_PTR_TYPEP(nd_args[i],fd_qchoice_type)) {
    fdtype retval;
    d_args[i]=FD_XQCHOICE(nd_args[i])->choice;
    retval=ndapply_loop(f,results,typeinfo,i+1,n,nd_args,d_args);
    if (FD_ABORTP(retval)) return retval;}
  else if ((!(FD_CHOICEP(nd_args[i]))) ||
           ((typeinfo)&&(typeinfo[i]==fd_choice_type))) {
    fdtype retval;
    d_args[i]=nd_args[i];
    retval=ndapply_loop(f,results,typeinfo,i+1,n,nd_args,d_args);
    if (FD_ABORTP(retval)) return retval;}
  else {
    FD_DO_CHOICES(elt,nd_args[i]) {
      fdtype retval; d_args[i]=elt;
      retval=ndapply_loop(f,results,typeinfo,i+1,n,nd_args,d_args);
      if (FD_ABORTP(retval)) return retval;}}
  return FD_VOID;
}

FD_EXPORT fdtype fd_ndapply(fdtype fp,int n,fdtype *args)
{
  struct FD_FUNCTION *f=FD_DTYPE2FCN(fp);
  if ((f->arity < 0) || ((n <= f->arity) && (n>=f->min_arity))) {
    fdtype argbuf[6], *d_args;
    fdtype retval, results=FD_EMPTY_CHOICE;
    /* Initialize the d_args vector */
    if (n>6) d_args=u8_alloc_n(n,fdtype);
    else d_args=argbuf;
    retval=ndapply_loop(f,&results,f->typeinfo,0,n,args,d_args);
    if (FD_ABORTP(retval)) {
      fd_decref(results);
      if (d_args!=argbuf) u8_free(d_args);
      return retval;}
    else {
      if (d_args!=argbuf) u8_free(d_args);
      return fd_simplify_choice(results);}}
  else {
    fd_exception ex=((n>f->arity) ? (fd_TooManyArgs) : (fd_TooFewArgs));
    return fd_err(ex,"fd_ndapply",f->name,FDTYPE_CONS(f));}
}

/* The default apply function */

static int contains_qchoicep(int n,fdtype *args);
static fdtype qchoice_dapply(fdtype fp,int n,fdtype *args);

FD_EXPORT fdtype fd_apply(fdtype fp,int n,fdtype *args)
{
  struct FD_FUNCTION *f=FD_DTYPE2FCN(fp); fdtype result;
  if (f->ndprim)
    if (!(FD_EXPECT_FALSE(contains_qchoicep(n,args))))
      result=fd_dapply((fdtype)f,n,args);
    else result=qchoice_dapply(fp,n,args);
  else {
    int i=0;
    while (i<n)
      if (args[i]==FD_EMPTY_CHOICE) return FD_EMPTY_CHOICE;
      else if (FD_ATOMICP(args[i])) i++;
      else {
        fd_ptr_type argtype=FD_PTR_TYPE(args[i]);
        if ((argtype==fd_choice_type) ||
            (argtype==fd_achoice_type) ||
            (argtype==fd_qchoice_type)) {
          result=fd_ndapply((fdtype)f,n,args);
          return fd_finish_call(result);}
        else i++;}
    result=fd_dapply((fdtype)f,n,args);}
  return fd_finish_call(result);
}

static int contains_qchoicep(int n,fdtype *args)
{
  fdtype *scan=args, *limit=args+n;
  while (scan<limit)
    if (FD_QCHOICEP(*scan)) return 1;
    else scan++;
  return 0;
}

static fdtype qchoice_dapply(fdtype fp,int n,fdtype *args)
{
  fdtype result, _nargs[8], *nargs;
  fdtype *read=args, *limit=read+n, *write;
  if (n<=8) nargs=_nargs;
  else nargs=u8_alloc_n(n,fdtype);
  write=nargs;
  while (read<limit)
    if (FD_QCHOICEP(*read)) {
      struct FD_QCHOICE *qc=(struct FD_QCHOICE *) (*read++);
      *write++=qc->choice;}
    else *write++=*read++;
  result=fd_dapply(fp,n,nargs);
  if (n>8) u8_free(nargs);
  return result;
}

static int unparse_function(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_FUNCTION *fn=
    FD_GET_CONS(x,fd_function_type,struct FD_FUNCTION *);
  char buf[512], name[512], args[512];
  if (fn->filename)
    sprintf(name,_("%s:%s"),fn->name,fn->filename);
  else if (fn->name) sprintf(name,_("%s"),fn->name);
  else sprintf(name,"anon");
  if (fn->arity>=0)
    if (fn->min_arity!=fn->arity)
      sprintf(args,"%d-%d",fn->min_arity,fn->arity);
    else sprintf(args,"%d",fn->arity);
  else if (fn->min_arity>0)
    sprintf(args,"%d+",fn->min_arity);
  else sprintf(args,"0+");
  if (fn->ndprim)
    sprintf(buf,_("#<NDPrimitive %s (%s args)>"),name,args);
  else sprintf(buf,_("#<Primitive %s (%s args)>"),name,args);
  u8_puts(out,buf);
  return 1;
}
static void recycle_function(struct FD_CONS *c)
{
  struct FD_FUNCTION *fn=(struct FD_FUNCTION *)c;
  if (fn->typeinfo) u8_free(fn->typeinfo);
  if (fn->defaults) u8_free(fn->defaults);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Declaring functions */

FD_EXPORT fdtype fd_make_cprimn(u8_string name,fd_cprimn fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=-1; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.calln=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim0(u8_string name,fd_cprim0 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=0; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call0=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim1(u8_string name,fd_cprim1 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=1; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call1=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim2(u8_string name,fd_cprim2 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=2; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call2=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim3(u8_string name,fd_cprim3 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=3; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call3=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim4(u8_string name,fd_cprim4 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=4; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call4=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim5(u8_string name,fd_cprim5 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=5; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call5=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim6(u8_string name,fd_cprim6 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=6; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call6=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim7(u8_string name,fd_cprim7 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=7; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call7=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim8(u8_string name,fd_cprim8 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=8; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call8=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim9(u8_string name,fd_cprim9 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_function_type);
  f->name=name; f->filename=NULL; f->ndprim=0; f->xprim=0; f->filename=NULL;
  f->min_arity=min_arity; f->arity=9; f->typeinfo=NULL; f->defaults=NULL;
  f->handler.call9=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_ndprim(fdtype prim)
{
  struct FD_FUNCTION *f=FD_XFUNCTION(prim);
  f->ndprim=1;
  return prim;
}

/* Providing type info and defaults */

static void init_fn_info(struct FD_FUNCTION *f,va_list args)
{
  int *typeinfo=u8_alloc_n(f->arity,int), i=0;
  fdtype *defaults=u8_alloc_n(f->arity,fdtype);
  while (i < f->arity) {
    int tcode=va_arg(args,int); fdtype dflt=va_arg(args,fdtype);
    typeinfo[i]=tcode; defaults[i]=dflt; i++;}
  f->filename=NULL;
  f->typeinfo=typeinfo; f->defaults=defaults;
}

FD_EXPORT fdtype fd_make_cprim1x
 (u8_string name,fd_cprim1 fn,int min_arity,...)
{
  va_list args;
  struct FD_FUNCTION *f=
    (struct FD_FUNCTION *)fd_make_cprim1(name,fn,min_arity);
  va_start(args,min_arity);
  init_fn_info(f,args);
  va_end(args);
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim2x
  (u8_string name,fd_cprim2 fn,int min_arity,...)
{
  va_list args;
  struct FD_FUNCTION *f=
    (struct FD_FUNCTION *)fd_make_cprim2(name,fn,min_arity);
  va_start(args,min_arity);
  init_fn_info(f,args);
  va_end(args);
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim3x
  (u8_string name,fd_cprim3 fn,int min_arity,...)
{
  va_list args;
  struct FD_FUNCTION *f=
    (struct FD_FUNCTION *)fd_make_cprim3(name,fn,min_arity);
  va_start(args,min_arity);
  init_fn_info(f,args);
  va_end(args);
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim4x
  (u8_string name,fd_cprim4 fn,int min_arity,...)
{
  va_list args;
  struct FD_FUNCTION *f=
    (struct FD_FUNCTION *)fd_make_cprim4(name,fn,min_arity);
  va_start(args,min_arity);
  init_fn_info(f,args);
  va_end(args);
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim5x
   (u8_string name,fd_cprim5 fn,int min_arity,...)
{
  va_list args;
  struct FD_FUNCTION *f=
    (struct FD_FUNCTION *)fd_make_cprim5(name,fn,min_arity);
  va_start(args,min_arity);
  init_fn_info(f,args);
  va_end(args);
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim6x
   (u8_string name,fd_cprim6 fn,int min_arity,...)
{
  va_list args;
  struct FD_FUNCTION *f=
    (struct FD_FUNCTION *)fd_make_cprim6(name,fn,min_arity);
  va_start(args,min_arity);
  init_fn_info(f,args);
  va_end(args);
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim7x
   (u8_string name,fd_cprim7 fn,int min_arity,...)
{
  va_list args;
  struct FD_FUNCTION *f=
    (struct FD_FUNCTION *)fd_make_cprim7(name,fn,min_arity);
  va_start(args,min_arity);
  init_fn_info(f,args);
  va_end(args);
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim8x
   (u8_string name,fd_cprim8 fn,int min_arity,...)
{
  va_list args;
  struct FD_FUNCTION *f=
    (struct FD_FUNCTION *)fd_make_cprim8(name,fn,min_arity);
  va_start(args,min_arity);
  init_fn_info(f,args);
  va_end(args);
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim9x
   (u8_string name,fd_cprim9 fn,int min_arity,...)
{
  va_list args;
  struct FD_FUNCTION *f=
    (struct FD_FUNCTION *)fd_make_cprim9(name,fn,min_arity);
  va_start(args,min_arity);
  init_fn_info(f,args);
  va_end(args);
  return FDTYPE_CONS(f);
}

/* Tail calls */

FD_EXPORT fdtype fd_tail_call(fdtype fcn,int n,fdtype *vec)
{
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)fcn;
  if (FD_EXPECT_FALSE(((f->arity)>=0) && (n>(f->arity)))) {
    fd_seterr(fd_TooManyArgs,"fd_tail_call",u8_mkstring("%d",n),fcn);
    return FD_ERROR_VALUE;}
  else {
    int atomic=1, nd=0;
    struct FD_TAIL_CALL *tc=
      (struct FD_TAIL_CALL *)u8_malloc(sizeof(struct FD_TAIL_CALL)+sizeof(fdtype)*n);
    fdtype *write=&(tc->head), *write_limit=write+(n+1), *read=vec;
    FD_INIT_CONS(tc,fd_tail_call_type); tc->n_elts=n+1; tc->flags=0;
    *write++=fd_incref(fcn);
    while (write<write_limit) {
      fdtype v=*read++;
      if (FD_CONSP(v)) {
        atomic=0;
        if (FD_CHOICEP(v)) nd=1;
        else if (FD_QCHOICEP(v)) nd=1;
        *write++=fd_incref(v);}
      else *write++=v;}
    if (atomic) tc->flags=tc->flags|FD_TAIL_CALL_ATOMIC_ARGS;
    if (nd) tc->flags=tc->flags|FD_TAIL_CALL_ND_ARGS;
    return FDTYPE_CONS(tc);}
}

FD_EXPORT fdtype fd_step_call(fdtype c)
{
  struct FD_TAIL_CALL *tc=
    FD_GET_CONS(c,fd_tail_call_type,struct FD_TAIL_CALL *);
  fdtype result=
    ((tc->flags&FD_TAIL_CALL_ND_ARGS)?
     (fd_apply(tc->head,tc->n_elts-1,(&(tc->head))+1)):
     (fd_dapply(tc->head,tc->n_elts-1,(&(tc->head))+1)));
  fd_decref(c);
  return result;
}

static void recycle_tail_call(struct FD_CONS *c);

FD_EXPORT fdtype _fd_finish_call(fdtype pt)
{
  while (FD_PTR_TYPEP(pt,fd_tail_call_type)) {
    struct FD_TAIL_CALL *tc=
      FD_GET_CONS(pt,fd_tail_call_type,struct FD_TAIL_CALL *);
    fdtype result=((tc->flags&FD_TAIL_CALL_ND_ARGS) ?
                   (fd_apply(tc->head,tc->n_elts-1,(&(tc->head))+1)) :
                   (fd_dapply(tc->head,tc->n_elts-1,(&(tc->head))+1)));
    if (FD_CONS_REFCOUNT(tc)==1)
      recycle_tail_call((struct FD_CONS *)tc);
    else fd_decref(pt);
    pt=result;}
  return pt;
}

static int unparse_tail_call(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_TAIL_CALL *fn=
    FD_GET_CONS(x,fd_tail_call_type,struct FD_TAIL_CALL *);
  u8_printf(out,"#<TAILCALL %q on %d args>",fn->head,fn->n_elts);
  return 1;
}

static void recycle_tail_call(struct FD_CONS *c)
{
  struct FD_TAIL_CALL *tc=(struct FD_TAIL_CALL *)c;
  fdtype *scan=&(tc->head), *limit=scan+tc->n_elts;
  if (!(tc->flags&FD_TAIL_CALL_ATOMIC_ARGS)) {
    while (scan<limit) {fd_decref(*scan); scan++;}}
  /* The head is always incref'd */
  else fd_decref(*scan);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Initializations */

static u8_condition DefnFailed=_("Definition Failed");

FD_EXPORT void fd_defn(fdtype table,fdtype fcn)
{
  struct FD_FUNCTION *f=FD_GET_CONS(fcn,fd_function_type,struct FD_FUNCTION *);
  if (fd_store(table,fd_intern(f->name),fcn)<0)
    u8_raise(DefnFailed,"fd_defn",NULL);
}

FD_EXPORT void fd_idefn(fdtype table,fdtype fcn)
{
  struct FD_FUNCTION *f=FD_GET_CONS(fcn,fd_function_type,struct FD_FUNCTION *);
  if (fd_store(table,fd_intern(f->name),fcn)<0)
    u8_raise(DefnFailed,"fd_defn",NULL);
  fd_decref(fcn);
}

FD_EXPORT void fd_defalias(fdtype table,u8_string to,u8_string from)
{
  fdtype to_symbol=fd_intern(to);
  fdtype from_symbol=fd_intern(from);
  fdtype v=fd_get(table,from_symbol,FD_VOID);
  fd_store(table,to_symbol,v);
  fd_decref(v);
}

FD_EXPORT void fd_defalias2(fdtype table,u8_string to,fdtype src,u8_string from)
{
  fdtype to_symbol=fd_intern(to);
  fdtype from_symbol=fd_intern(from);
  fdtype v=fd_get(src,from_symbol,FD_VOID);
  fd_store(table,to_symbol,v);
  fd_decref(v);
}

FD_EXPORT void fd_init_apply_c()
{
  int i=0; while (i < FD_TYPE_MAX) fd_applyfns[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_functionp[i++]=0;

  u8_register_source_file(_FILEINFO);
  u8_register_source_file(FRAMERD_APPLY_H_INFO);
  fd_register_config("CALLTRACK",_("File used for calltrack profiling (#f disables calltrack)"),
                     get_calltrack,set_calltrack,NULL);

#if ((FD_THREADS_ENABLED) && (FD_USE_TLS))
  u8_new_threadkey(&calltrack_log_key,free_calltrack_log);
#endif

  fd_applyfns[fd_function_type]=(fd_applyfn)fd_dapply;
  fd_functionp[fd_function_type]=1;

  fd_unparsers[fd_function_type]=unparse_function;
  fd_recyclers[fd_function_type]=recycle_function;

  fd_unparsers[fd_tail_call_type]=unparse_tail_call;
  fd_recyclers[fd_tail_call_type]=recycle_tail_call;
  fd_type_names[fd_tail_call_type]="tailcall";

  calltrack_sense=fd_intern("CALLTRACK/SENSE");
  calltrack_ignore=fd_intern("CALLTRACK/IGNORE");
  fd_register_config
    ("CALLTRACK/SENSE",_("Active calltrack sensors"),
     config_get_calltrack_sensors,config_set_calltrack_sensors,NULL);
  fd_register_config
    ("CALLTRACK/IGNORE",_("Ignore calltrack sensors"),
     config_get_calltrack_sensors,config_set_calltrack_sensors,NULL);

#if (FD_THREADS_ENABLED)
  u8_init_mutex(&calltrack_sensor_lock);
#endif

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
