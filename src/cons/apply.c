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

fd_applyfn fd_applyfns[FD_TYPE_MAX];
/* This is set if the type is a CONS with a FUNCTION header */
short fd_functionp[FD_TYPE_MAX];

u8_condition fd_apply_context="APPLY";

fd_exception fd_NotAFunction=_("calling a non function");
fd_exception fd_TooManyArgs=_("too many arguments");
fd_exception fd_TooFewArgs=_("too few arguments");
fd_exception fd_NoDefault=_("no default value for #default argument");
fd_exception fd_ProfilingDisabled=_("profiling not built");

const int fd_calltrack_enabled=FD_CALLTRACK_ENABLED;

/* Stack checking */

#if FD_STACKCHECK
#if HAVE_THREAD_STORAGE_CLASS
static __thread ssize_t apply_stack_limit=-1;
static int stackcheck()
{
  if (apply_stack_limit>16384) {
    ssize_t depth=u8_stack_depth();
    if ((u8_stack_direction>0) ?
        (depth>apply_stack_limit) :
        (apply_stack_limit>depth))
      return 0;
    else return 1;}
  else return 1;
}
FD_EXPORT ssize_t fd_stack_limit()
{
  if (apply_stack_limit<0)
    return u8_stack_size;
  return apply_stack_limit;
}
FD_EXPORT ssize_t fd_stack_limit_set(ssize_t limit)
{
  if (limit<65536) {
    char *detailsbuf = u8_malloc(50);
    u8_seterr("StackLimitTooSmall","fd_stack_limit_set",
              u8_write_long_long(limit,detailsbuf,50));
    return -1;}
  else {
    ssize_t oldlimit=apply_stack_limit;
    apply_stack_limit=limit;
    return oldlimit;}
}
#else
static u8_tld_key stack_limit_key;
static int stackcheck()
{
  ssize_t stack_limit=(ssize_t)u8_tld_get(stack_limit_key);
  if (stack_limit>16384) {
    ssize_t depth=u8_stack_depth();
    if ((u8_stack_direction>0) ?
        (depth>stack_limit) :
        (stack_limit>depth))
      return 0;
    else return 1;}
  else return 1;
}
FD_EXPORT ssize_t fd_stack_limit()
{
  ssize_t stack_limit=(ssize_t)u8_tld_get(stack_limit_key);
  if (stack_limit>16384)
    return stack_limit;
  else return u8_stack_size;
}
FD_EXPORT ssize_t fd_stack_limit_set(ssize_t lim)
{
  if ( lim < 65536 ) {
    char *detailsbuf = u8_malloc(50);
    u8_seterr("StackLimitTooSmall","fd_stack_limit_set",
              u8_write_long_long(lim,detailsbuf,64));
    return -1;}
  else {
    ssize_t oldlimit=fd_stack_limit();
    u8_tld_set(stack_limit_key,(void *)lim);
    return oldlimit;}
}
#endif
#else
static int youve_been_warned=0;
#define stackcheck() (1)
FD_EXPORT ssize_t fd_stack_limit()
{
  if (youve_been_warned) return -1;
  u8_log(LOG_WARN,"NoStackChecking",
         "Stack checking is not enabled in this build");
  youve_been_warned=1;
  return -1;
}
FD_EXPORT ssize_t fd_stack_limit_set(ssize_t limit)
{
  u8_log(LOG_WARN,"NoStackChecking",
         "Stack checking is not enabled in this build");
  return -1;
}
#endif

FD_EXPORT int fd_stackcheck()
{
  return stackcheck();
}

/* Calling primitives */

static fdtype dcall0(struct FD_FUNCTION *f)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall0(f);
  else return f->fcn_handler.call0();
}
static fdtype dcall1(struct FD_FUNCTION *f,fdtype arg1)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall1(f,arg1);
  else return f->fcn_handler.call1(arg1);
}
static fdtype dcall2(struct FD_FUNCTION *f,fdtype arg1,fdtype arg2)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall2(f,arg1,arg2);
  else return f->fcn_handler.call2(arg1,arg2);
}
static fdtype dcall3(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall3(f,arg1,arg2,arg3);
  else return f->fcn_handler.call3(arg1,arg2,arg3);
}
static fdtype dcall4(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall4(f,arg1,arg2,arg3,arg4);
  else return f->fcn_handler.call4(arg1,arg2,arg3,arg4);
}
static fdtype dcall5(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                      fdtype arg5)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall5(f,arg1,arg2,arg3,arg4,arg5);
  else return f->fcn_handler.call5(arg1,arg2,arg3,arg4,arg5);
}
static fdtype dcall6(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                      fdtype arg5,fdtype arg6)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall6(f,arg1,arg2,arg3,arg4,arg5,arg6);
  else return f->fcn_handler.call6(arg1,arg2,arg3,arg4,arg5,arg6);
}

static fdtype dcall7(struct FD_FUNCTION *f,
                      fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                      fdtype arg5,fdtype arg6,fdtype arg7)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall7(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7);
  else return f->fcn_handler.call7(arg1,arg2,arg3,arg4,arg5,arg6,arg7);
}

static fdtype dcall8(struct FD_FUNCTION *f,
                     fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                     fdtype arg5,fdtype arg6,fdtype arg7,fdtype arg8)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall8(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8);
  else return f->fcn_handler.call8(arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8);
}

static fdtype dcall9(struct FD_FUNCTION *f,
                     fdtype arg1,fdtype arg2,fdtype arg3,fdtype arg4,
                     fdtype arg5,fdtype arg6,fdtype arg7,fdtype arg8,
                     fdtype arg9)
{
  if (FD_EXPECT_FALSE(f->fcn_xcall))
    return f->fcn_handler.xcall9(f,arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8,arg9);
  else return f->fcn_handler.call9(arg1,arg2,arg3,arg4,arg5,arg6,arg7,arg8,arg9);
}

/* Generic calling function */

FD_FASTOP int check_typeinfo(struct FD_FUNCTION *f,int n,fdtype *args)
{
  /* Check typeinfo */
  int *typeinfo=f->fcn_typeinfo; int i=0;
  if (typeinfo) while (i<n) {
      fdtype arg=args[i]; int argtype=typeinfo[i];
      if (argtype<0) i++;
      else if (FD_TYPEP(arg,argtype)) i++;
      else if ( (FD_VOIDP(arg)) || (FD_DEFAULTP(arg))) i++;
      else {
        u8_string type_name=fd_type2name(argtype);
        fd_seterr(fd_TypeError,type_name,u8dup(f->fcn_name),args[i]);
        return -1;}}
  return 0;
}

FD_FASTOP fdtype *fill_argvec(struct FD_FUNCTION *f,int n,fdtype *argvec,
                              fdtype *argbuf,int argbuf_len)
{
  int arity=f->fcn_arity, min_arity=f->fcn_min_arity;
  fdtype fptr=(fdtype)f;
  if ((min_arity>0) && (n<min_arity)) {
    fd_seterr(fd_TooFewArgs,"fd_dapply",u8dup(f->fcn_name),fd_incref(fptr));
    return NULL;}
  else if ((arity>=0) && (n>arity)) {
    fd_seterr(fd_TooManyArgs,"fd_dapply",f->fcn_name,fd_incref(fptr));
    return NULL;}
  else if ((arity<0)||(arity==n))
    return argvec;
  else {
    fdtype *args=
      (arity>=argbuf_len) ?
      (u8_zalloc_n(arity,fdtype)) :
      (argbuf);
    int i=0; while (i<n) { args[i]=argvec[i]; i++; }
    if (f->fcn_defaults) {
      fdtype *defaults=f->fcn_defaults;
      while (i<arity) {
        fdtype dflt=args[i]=defaults[i];
        fd_incref(dflt);
        i++;}}
    else while (i<arity) args[i++]=FD_VOID;
    return args;}
}

FD_FASTOP fdtype dcall(u8_string fname,fd_function f,int n,fdtype *args)
{
  if (FD_INTERRUPTED()) return FD_ERROR_VALUE;
  else if (f->fcn_handler.fnptr)
    switch (f->fcn_arity) {
    case 0: return dcall0(f);
    case 1: return dcall1(f,args[0]);
    case 2: return dcall2(f,args[0],args[1]);
    case 3: return dcall3(f,args[0],args[1],args[2]);
    case 4: return dcall4(f,args[0],args[1],args[2],args[3]);
    case 5: return dcall5(f,args[0],args[1],args[2],args[3],args[4]);
    case 6:
      return dcall6(f,args[0],args[1],args[2],args[3],args[4],args[5]);
    case 7:
      return dcall7(f,args[0],args[1],args[2],args[3],
                    args[4],args[5],args[6]);
    case 8:
      return dcall8(f,args[0],args[1],args[2],args[3],
                    args[4],args[5],args[6],args[7]);
    case 9:
      return dcall9(f,args[0],args[1],args[2],args[3],
                    args[4],args[5],args[6],args[7],args[8]);
    default:
      if (f->fcn_xcall)
        return f->fcn_handler.calln(n,args);
      else return f->fcn_handler.calln(n,args);}
  else {
    int ctype=FD_CONS_TYPE(f);
    if (fd_applyfns[ctype])
      return fd_applyfns[ctype]((fdtype)f,n,args);
    else return fd_type_error("applicable","dcall",(fdtype)f);}
}

FD_EXPORT fdtype fd_dcall(struct FD_FUNCTION *f,int n,fdtype *args)
{
  return dcall(f->fcn_name,f,n,args);
}

FD_FASTOP fdtype apply_fcn(u8_string name,fd_function f,int n,fdtype *argvec)
{
  fdtype fnptr=(fdtype)f;
  fdtype argbuf[8], *args;
  if (FD_EXPECT_FALSE(n<0))
    return fd_err(_("Negative arg count"),"apply_fcn",name,fnptr);
  else if (f->fcn_arity<0) { /* Is a LEXPR */
    if (n<(f->fcn_min_arity))
      return fd_err(fd_TooFewArgs,"fd_dapply",f->fcn_name,fnptr);
    else if ( (f->fcn_xcall) && (f->fcn_handler.xcalln) )
      return f->fcn_handler.xcalln(f,n,argvec);
    else if (f->fcn_handler.calln)
      return f->fcn_handler.calln(n,argvec);
    else {
      /* There's no explicit method on this function object, so we use
         the method on the type (if there is one) */
      int ctype=FD_CONS_TYPE(f);
      if (fd_applyfns[ctype])
        return fd_applyfns[ctype]((fdtype)f,n,argvec);
      else return fd_err("NotApplicable","apply_fcn",f->fcn_name,fnptr);}}
  else if (n==0)
    return dcall(name,f,n,argvec);
  else if (FD_EXPECT_FALSE(argvec==NULL))
    return fd_err(_("Null argument vector"),"apply_fcn",name,fnptr);
  else args=fill_argvec(f,n,argvec,argbuf,8);
  if (args==NULL)
    return FD_ERROR_VALUE;
  else if (check_typeinfo(f,n,args)<0)
    return FD_ERROR_VALUE;
  else if ((args==argbuf)||(args==argvec))
    return dcall(name,f,n,args);
  else {
    fdtype result=dcall(name,f,n,args);
    u8_free(args);
    return result;}
}

FD_EXPORT fdtype fd_deterministic_apply(fdtype fn,int n,fdtype *argvec)
{
  u8_byte namebuf[60];
  u8_string fname=NULL;
  fd_ptr_type ftype=FD_PRIM_TYPE(fn);
  struct FD_FUNCTION *f=NULL;

  if (ftype==fd_fcnid_type) {
    fn=fd_fcnid_ref(fn);
    ftype=FD_PTR_TYPE(fn);}

  if (fd_functionp[ftype]) {
    f=(struct FD_FUNCTION *)fn;
    if (f->fcn_name)
      fname=f->fcn_name;}
  else if (fd_applyfns[ftype]) {
    sprintf(namebuf,"λ0x%llx",U8_PTR2INT(fn));
    fname=namebuf;}
  else return fd_type_error("applicable","fd_determinstic_apply",fn);

  /* Make the call */
  if (stackcheck()) {
    fdtype result=FD_VOID;
    U8_WITH_CONTOUR(fname,0)
      if (f) result=apply_fcn(fname,f,n,argvec);
      else result=fd_applyfns[ftype](fn,n,argvec);
    U8_ON_EXCEPTION {
      U8_CLEAR_CONTOUR();
      result = FD_ERROR_VALUE;}
    U8_END_EXCEPTION;
    if (errno) {
      u8_string cond=u8_strerror(errno);
      u8_log(LOG_WARN,cond,"Unexpected errno=%d (%s) after %s",
             errno,cond,U8ALT(fname,"primcall"));
      errno=0;}
    return result;}
  else {
    u8_string limit=u8_mkstring("%lld",fd_stack_limit());
    fdtype depth=FD_INT2DTYPE(u8_stack_depth());
    return fd_err(fd_StackOverflow,fname,limit,depth);}
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
  else if (FD_TYPEP(nd_args[i],fd_qchoice_type)) {
    fdtype retval;
    d_args[i]=FD_XQCHOICE(nd_args[i])->fd_choiceval;
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
  fdtype handler=(FD_FCNIDP(fp) ? (fd_fcnid_ref(fp)) : (fp));
  fd_ptr_type fntype=FD_PTR_TYPE(handler);
  if (fd_functionp[fntype]) {
    struct FD_FUNCTION *f=FD_DTYPE2FCN(handler);
    if (f->fcn_arity == 0)
      return fd_dapply(handler,n,args);
    else if ((f->fcn_arity < 0) ?
             (n >= (f->fcn_min_arity)) :
             ((n <= (f->fcn_arity)) && (n >= (f->fcn_min_arity)))) {
      fdtype argbuf[6], *d_args;
      fdtype retval, results=FD_EMPTY_CHOICE;
      /* Initialize the d_args vector */
      if (n>6) d_args=u8_alloc_n(n,fdtype);
      else d_args=argbuf;
      retval=ndapply_loop(f,&results,f->fcn_typeinfo,0,n,args,d_args);
      if (FD_ABORTP(retval)) {
        fd_decref(results);
        if (d_args!=argbuf) u8_free(d_args);
        return retval;}
      else {
        if (d_args!=argbuf) u8_free(d_args);
        return fd_simplify_choice(results);}}
    else {
      fd_exception ex=((n>f->fcn_arity) ? (fd_TooManyArgs) : (fd_TooFewArgs));
      return fd_err(ex,"fd_ndapply",f->fcn_name,FDTYPE_CONS(f));}}
  else if (fd_applyfns[fntype])
    return (fd_applyfns[fntype])(handler,n,args);
  else return fd_type_error("Applicable","fd_ndapply",handler);
}

/* The default apply function */

static int contains_qchoicep(int n,fdtype *args);
static fdtype qchoice_dapply(fdtype fp,int n,fdtype *args);

FD_EXPORT fdtype fd_apply(fdtype fp,int n,fdtype *args)
{
  struct FD_FUNCTION *f=FD_DTYPE2FCN(fp); fdtype result;
  if (f->fcn_ndcall)
    if (!(FD_EXPECT_FALSE(contains_qchoicep(n,args))))
      result=fd_dapply((fdtype)f,n,args);
    else result=qchoice_dapply(fp,n,args);
  else {
    int i=0, qchoice=0;
    while (i<n)
      if (args[i]==FD_EMPTY_CHOICE)
        return FD_EMPTY_CHOICE;
      else if (FD_ATOMICP(args[i])) i++;
      else {
        fd_ptr_type argtype=FD_PTR_TYPE(args[i]);
        if ((argtype==fd_choice_type) ||
            (argtype==fd_achoice_type)) {
          result=fd_ndapply((fdtype)f,n,args);
          return fd_finish_call(result);}
        else if (argtype==fd_qchoice_type) {
          qchoice=1; i++;}
        else i++;}
    if (qchoice)
      result=qchoice_dapply((fdtype)f,n,args);
    else result=fd_dapply((fdtype)f,n,args);}
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
      *write++=qc->fd_choiceval;}
    else *write++=*read++;
  result=fd_dapply(fp,n,nargs);
  if (n>8) u8_free(nargs);
  return result;
}

FD_EXPORT int unparse_primitive(u8_output out,fdtype x)
{
  struct FD_FUNCTION *fcn=(fd_function)x;
  u8_string name=fcn->fcn_name;
  u8_string filename=fcn->fcn_filename;
  u8_byte arity[16]=""; u8_byte codes[16]="";
  if ((filename)&&(filename[0]=='\0'))
    filename=NULL;
  if (name==NULL) name=fcn->fcn_name;
  if (fcn->fcn_ndcall) strcat(codes,"∀");
  if ((fcn->fcn_arity<0)&&(fcn->fcn_min_arity<0))
    strcat(arity,"…");
  else if (fcn->fcn_arity==fcn->fcn_min_arity)
    sprintf(arity,"[%d]",fcn->fcn_min_arity);
  else if (fcn->fcn_arity<0)
    sprintf(arity,"[%d…]",fcn->fcn_min_arity);
  else sprintf(arity,"[%d-%d]",fcn->fcn_min_arity,fcn->fcn_arity);
  if (name)
    u8_printf(out,"#<Φ%s%s%s%s%s%s>",
              codes,name,arity,
              U8OPTSTR(" '",fcn->fcn_filename,"'"));
  else u8_printf(out,"#<Φ%s%s #!0x%llx%s%s%s>",
                 codes,arity,(unsigned long long) fcn,
                 U8OPTSTR("'",fcn->fcn_filename,"'"));
  return 1;
}
static void recycle_primitive(struct FD_RAW_CONS *c)
{
  struct FD_FUNCTION *fn=(struct FD_FUNCTION *)c;
  if (fn->fcn_typeinfo) u8_free(fn->fcn_typeinfo);
  if (fn->fcn_defaults) u8_free(fn->fcn_defaults);
  if (FD_MALLOCD_CONSP(c)) u8_free(c);
}

/* Declaring functions */

FD_EXPORT fdtype fd_make_cprimn(u8_string name,fd_cprimn fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity;
  f->fcn_arity=-1;
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.calln=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim0(u8_string name,fd_cprim0 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=0; 
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call0=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim1(u8_string name,fd_cprim1 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=1; 
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call1=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim2(u8_string name,fd_cprim2 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=2; 
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call2=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim3(u8_string name,fd_cprim3 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=3; 
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call3=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim4(u8_string name,fd_cprim4 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=4; 
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call4=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim5(u8_string name,fd_cprim5 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=5; 
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call5=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim6(u8_string name,fd_cprim6 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=6; 
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call6=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim7(u8_string name,fd_cprim7 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=7; 
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call7=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim8(u8_string name,fd_cprim8 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=8;
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call8=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_cprim9(u8_string name,fd_cprim9 fn,int min_arity)
{
  struct FD_FUNCTION *f=u8_alloc(struct FD_FUNCTION);
  FD_INIT_CONS(f,fd_primfcn_type);
  f->fcn_name=name; f->fcn_filename=NULL; f->fcn_ndcall=0; f->fcn_xcall=0;
  f->fcn_min_arity=min_arity; f->fcn_arity=9;
  f->fcn_typeinfo=NULL;
  f->fcn_defaults=NULL;
  f->fcn_handler.call9=fn;
  return FDTYPE_CONS(f);
}

FD_EXPORT fdtype fd_make_ndprim(fdtype prim)
{
  struct FD_FUNCTION *f=FD_XFUNCTION(prim);
  f->fcn_ndcall=1;
  return prim;
}

/* Providing type info and defaults */

static void init_fn_info(struct FD_FUNCTION *f,va_list args)
{
  int *typeinfo=u8_alloc_n(f->fcn_arity,int), i=0;
  fdtype *defaults=u8_alloc_n(f->fcn_arity,fdtype);
  while (i < f->fcn_arity) {
    int tcode=va_arg(args,int); fdtype dflt=va_arg(args,fdtype);
    typeinfo[i]=tcode; defaults[i]=dflt; i++;}
  f->fcn_filename=NULL;
  f->fcn_typeinfo=typeinfo; f->fcn_defaults=defaults;
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
  if (FD_FCNIDP(fcn)) fcn=fd_fcnid_ref(fcn);
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)fcn;
  if (FD_EXPECT_FALSE(((f->fcn_arity)>=0) && (n>(f->fcn_arity)))) {
    fd_seterr(fd_TooManyArgs,"fd_tail_call",u8_mkstring("%d",n),fcn);
    return FD_ERROR_VALUE;}
  else {
    int atomic=1, nd=0;
    struct FD_TAILCALL *tc=(struct FD_TAILCALL *)
      u8_malloc(sizeof(struct FD_TAILCALL)+sizeof(fdtype)*n);
    fdtype *write=&(tc->tailcall_head), *write_limit=write+(n+1), *read=vec;
    FD_INIT_CONS(tc,fd_tailcall_type);
    tc->tailcall_arity=n+1;
    tc->tailcall_flags=0;
    *write++=fd_incref(fcn);
    while (write<write_limit) {
      fdtype v=*read++;
      if (FD_CONSP(v)) {
        atomic=0;
        if (FD_QCHOICEP(v)) {
          struct FD_QCHOICE *qc=(fd_qchoice)v;
          fdtype cv=qc->fd_choiceval;
          fd_incref(cv);
          *write++=cv;}
        else {
          if (FD_CHOICEP(v)) nd=1;
          *write++=fd_incref(v);}}
      else *write++=v;}
    if (atomic) tc->tailcall_flags=tc->tailcall_flags|FD_TAILCALL_ATOMIC_ARGS;
    if (nd) tc->tailcall_flags=tc->tailcall_flags|FD_TAILCALL_ND_ARGS;
    return FDTYPE_CONS(tc);}
}
FD_EXPORT fdtype fd_void_tail_call(fdtype fcn,int n,fdtype *vec)
{
  if (FD_FCNIDP(fcn)) fcn=fd_fcnid_ref(fcn);
  struct FD_FUNCTION *f=(struct FD_FUNCTION *)fcn;
  if (FD_EXPECT_FALSE(((f->fcn_arity)>=0) && (n>(f->fcn_arity)))) {
    fd_seterr(fd_TooManyArgs,"fd_void_tail_call",u8_mkstring("%d",n),fcn);
    return FD_ERROR_VALUE;}
  else {
    int atomic=1, nd=0;
    struct FD_TAILCALL *tc=(struct FD_TAILCALL *)
      u8_malloc(sizeof(struct FD_TAILCALL)+sizeof(fdtype)*n);
    fdtype *write=&(tc->tailcall_head), *write_limit=write+(n+1), *read=vec;
    FD_INIT_CONS(tc,fd_tailcall_type);
    tc->tailcall_arity=n+1;
    tc->tailcall_flags=FD_TAILCALL_VOID_VALUE;
    *write++=fd_incref(fcn);
    while (write<write_limit) {
      fdtype v=*read++;
      if (FD_CONSP(v)) {
        atomic=0;
        if (FD_QCHOICEP(v)) {
          struct FD_QCHOICE *qc=(fd_qchoice)v;
          fdtype cv=qc->fd_choiceval;
          fd_incref(cv);
          *write++=cv;}
        else {
          if (FD_CHOICEP(v)) nd=1;
          *write++=fd_incref(v);}}
      else *write++=v;}
    if (atomic) tc->tailcall_flags|=FD_TAILCALL_ATOMIC_ARGS;
    if (nd) tc->tailcall_flags|=FD_TAILCALL_ND_ARGS;
    return FDTYPE_CONS(tc);}
}

FD_EXPORT fdtype fd_step_call(fdtype c)
{
  struct FD_TAILCALL *tc=
    fd_consptr(struct FD_TAILCALL *,c,fd_tailcall_type);
  int discard=U8_BITP(tc->tailcall_flags,FD_TAILCALL_VOID_VALUE);
  fdtype result=
    ((tc->tailcall_flags&FD_TAILCALL_ND_ARGS)?
     (fd_apply(tc->tailcall_head,tc->tailcall_arity-1,(&(tc->tailcall_head))+1)):
     (fd_dapply(tc->tailcall_head,tc->tailcall_arity-1,(&(tc->tailcall_head))+1)));
  fd_decref(c);
  if (discard) {
    if (FD_TAILCALLP(result))
      return result;
    fd_decref(result);
    return FD_VOID;}
  else return result;
}

FD_EXPORT fdtype _fd_finish_call(fdtype call)
{
  if (FD_TAILCALLP(call)) {
    fdtype result=FD_VOID;
    while (1) {
      struct FD_TAILCALL *tc=
        fd_consptr(struct FD_TAILCALL *,call,fd_tailcall_type);
      int flags=tc->tailcall_flags;
      int voidval=(U8_BITP(flags,FD_TAILCALL_VOID_VALUE));
      fdtype next=((U8_BITP(flags,FD_TAILCALL_ND_ARGS)) ?
                   (fd_apply(tc->tailcall_head,tc->tailcall_arity-1,
                             (&(tc->tailcall_head))+1)) :
                   (fd_dapply(tc->tailcall_head,tc->tailcall_arity-1,
                              (&(tc->tailcall_head))+1)));
      int finished=(!(FD_TYPEP(next,fd_tailcall_type)));
      fd_decref(call); call=next;
      if (finished) {
        if (voidval) {
          fd_decref(next);
          result=FD_VOID;}
        else result=next;
        break;}}
    return result;}
  else return call;
} 
static int unparse_tail_call(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_TAILCALL *tc=
    fd_consptr(struct FD_TAILCALL *,x,fd_tailcall_type);
  u8_printf(out,"#<TAILCALL %q on %d args>",
            tc->tailcall_head,tc->tailcall_arity);
  return 1;
}

static void recycle_tail_call(struct FD_RAW_CONS *c)
{
  struct FD_TAILCALL *tc=(struct FD_TAILCALL *)c;
  int mallocd=FD_MALLOCD_CONSP(c), n_elts=tc->tailcall_arity;
  fdtype *scan=&(tc->tailcall_head), *limit=scan+n_elts;
  size_t tc_size=sizeof(struct FD_TAILCALL)+(sizeof(fdtype)*(n_elts-1));
  if (!(tc->tailcall_flags&FD_TAILCALL_ATOMIC_ARGS)) {
    while (scan<limit) {fd_decref(*scan); scan++;}}
  /* The head is always incref'd */
  else fd_decref(*scan);
  memset(tc,0,tc_size);
  if (mallocd) u8_free(tc);
}

/* Initializations */

static u8_condition DefnFailed=_("Definition Failed");

FD_EXPORT void fd_defn(fdtype table,fdtype fcn)
{
  struct FD_FUNCTION *f=fd_consptr(struct FD_FUNCTION *,fcn,fd_primfcn_type);
  if (fd_store(table,fd_intern(f->fcn_name),fcn)<0)
    u8_raise(DefnFailed,"fd_defn",NULL);
}

FD_EXPORT void fd_idefn(fdtype table,fdtype fcn)
{
  struct FD_FUNCTION *f=fd_consptr(struct FD_FUNCTION *,fcn,fd_primfcn_type);
  if (fd_store(table,fd_intern(f->fcn_name),fcn)<0)
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

void fd_init_calltrack_c(void);

static fdtype dapply(fdtype f,int n_args,fdtype *argvec)
{
  return fd_dapply(f,n_args,argvec);
}

FD_EXPORT void fd_init_apply_c()
{
  int i=0; while (i < FD_TYPE_MAX) fd_applyfns[i++]=NULL;
  i=0; while (i < FD_TYPE_MAX) fd_functionp[i++]=0;

  fd_functionp[fd_fcnid_type]=1;

  u8_register_source_file(_FILEINFO);
  u8_register_source_file(FRAMERD_APPLY_H_INFO);

#if (FD_USE_TLS)
  u8_new_threadkey(&stack_limit_key,free_calltrack_log);
#endif

  fd_applyfns[fd_primfcn_type]=dapply;
  fd_functionp[fd_primfcn_type]=1;

  fd_unparsers[fd_primfcn_type]=unparse_primitive;
  fd_recyclers[fd_primfcn_type]=recycle_primitive;

  fd_unparsers[fd_tailcall_type]=unparse_tail_call;
  fd_recyclers[fd_tailcall_type]=recycle_tail_call;
  fd_type_names[fd_tailcall_type]="tailcall";

  fd_init_calltrack_c();

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
