/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_INLINE_PPTRS 1

#include "fdb/dtype.h"
#include "fdb/apply.h"

#include <libu8/u8printf.h>

#include <stdarg.h>

fd_applyfn fd_applyfns[FD_TYPE_MAX];

u8_condition fd_apply_context="APPLY";

fd_exception fd_NotAFunction=_("calling a non function");
fd_exception fd_TooManyArgs=_("too many arguments");
fd_exception fd_TooFewArgs=_("too few arguments");
fd_exception fd_NoDefault=_("no default value for #default argument");
fd_exception fd_ProfilingDisabled=_("profiling not built");

static fd_exception TooManyCalltrackSensors=_("Too many calltrack sensors");

/* Internal profiling support */

#if FD_CALLTRACK_ENABLED
#include <stdio.h>
#ifndef MAX_CALLTRACK_SENSORS
#define MAX_CALLTRACK_SENSORS 64
#endif

#if FD_THREADS_ENABLED
u8_mutex calltrack_sensor_lock;
#endif

static struct FD_CALLTRACK_SENSOR calltrack_sensors[MAX_CALLTRACK_SENSORS];
static int n_calltrack_sensors=0;

FD_EXPORT fd_calltrack_sensor fd_get_calltrack_sensor(u8_string id)
{
  int i=0; fd_lock_mutex(&calltrack_sensor_lock);
  while (i<n_calltrack_sensors) 
    if (strcmp(id,calltrack_sensors[i].name)==0) {
      fd_unlock_mutex(&calltrack_sensor_lock);
      return &(calltrack_sensors[i]);}
    else i++;
  if (i<MAX_CALLTRACK_SENSORS) {
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
FD_EXPORT int fd_start_calltrack(char *filename)
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
FD_EXPORT int fd_start_calltrack(char *filename)
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
    fprintf(f,"> %s %f",name,timer);
    while (i<n_calltrack_sensors) 
      if (calltrack_sensors[i].enabled) 
	if (calltrack_sensors[i].dblfcn)
	  fprintf(f," %f",calltrack_sensors[i++].dblfcn());
	else if (calltrack_sensors[i].intfcn)
	  fprintf(f," %d",calltrack_sensors[i++].intfcn());
	else fprintf(f," 0");
      else i++;
    fprintf(f,"\n");}
}
static void calltrack_return(u8_string name)
{
  FILE *f=get_calltrack_logfile();
  if (f) {
    double timer=u8_elapsed_time(); int i=0;
    fprintf(f,"< %s %f",name,timer);
    while (i<n_calltrack_sensors) 
      if (calltrack_sensors[i].enabled) 
	if (calltrack_sensors[i].dblfcn)
	  fprintf(f," %f",calltrack_sensors[i++].dblfcn());
	else if (calltrack_sensors[i].intfcn)
	  fprintf(f," %d",calltrack_sensors[i++].intfcn());
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
  int retval=-1;
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

/* Instrumented apply */

#if FD_CALLTRACK_ENABLED
FD_EXPORT fdtype _fd_dapply_ct(fdtype fp,int n,fdtype *args);
FD_EXPORT fdtype fd_dapply(fdtype fp,int n,fdtype *args)
{
  struct FD_FUNCTION *f=FD_DTYPE2FCN(fp);
  fdtype result; u8_byte buf[64], *name;
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

/* Generic calling function */

FD_EXPORT fdtype FD_DAPPLY(fdtype fp,int n,fdtype *argvec)
{
  struct FD_FUNCTION *f=FD_DTYPE2FCN(fp);
  fdtype argbuf[8], *args;
  if (FD_EXPECT_FALSE(f->arity<0))
    if (f->xprim) {
      int ctype=FD_CONS_TYPE(f);
      return fd_applyfns[ctype]((fdtype)f,n,argvec);}
    else return f->handler.calln(n,argvec);
  /* Fill in the rest of the argvec */
  if (FD_EXPECT_TRUE((n <= f->arity) && (n>=f->min_arity))) {
    if (FD_EXPECT_FALSE(n<f->arity)) {
      /* Fill in defaults */
      int i=0; fdtype *defaults=f->defaults;
      if (f->arity<=8) args=argbuf;
      else args=u8_alloc_n((f->arity),fdtype);
      while (i<n) {
	args[i]=argvec[i]; i++;}
      if (defaults)
	while (i<f->arity) {args[i]=defaults[i]; i++;}
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
      default:
	if ((args==argbuf) || (args==argvec))
	  return f->handler.calln(n,args);
	else {
	  fdtype retval=f->handler.calln(n,args);
	  u8_free(args);
	  return retval;}}}
  else {
    fd_exception ex=((n>f->arity) ? (fd_TooManyArgs) : (fd_TooFewArgs));
    return fd_err(ex,"fd_dapply",f->name,FDTYPE_CONS(f));}
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

FD_EXPORT fdtype fd_apply(fdtype fp,int n,fdtype *args)
{
  struct FD_FUNCTION *f=FD_DTYPE2FCN(fp); fdtype result;
  if (f->ndprim) result=fd_dapply((fdtype)f,n,args);
  else result=fd_ndapply((fdtype)f,n,args);
  return fd_finish_call(result);
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
    int i=0;
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
  fdtype result=fd_apply(tc->head,tc->n_elts-1,(&(tc->head))+1);
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

FD_EXPORT void fd_init_apply_c()
{
  int i=0; while (i < FD_TYPE_MAX) fd_applyfns[i++]=NULL;

  fd_register_source_file(versionid);
  fd_register_source_file(FDB_APPLY_H_VERSION);
  fd_register_config("CALLTRACK",_("File used for calltrack profiling (non #f enables it)"),
		     get_calltrack,set_calltrack,NULL);

#if ((FD_THREADS_ENABLED) && (FD_USE_TLS))
  u8_new_threadkey(&calltrack_log_key,free_calltrack_log);
#endif

  fd_applyfns[fd_function_type]=(fd_applyfn)fd_dapply;
  fd_unparsers[fd_function_type]=unparse_function;
  fd_recyclers[fd_function_type]=recycle_function;

  fd_unparsers[fd_tail_call_type]=unparse_tail_call;
  fd_recyclers[fd_tail_call_type]=recycle_tail_call;

#if (FD_THREADS_ENABLED)
  u8_init_mutex(&calltrack_sensor_lock);
#endif

}
