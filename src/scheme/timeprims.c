/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"
#include "fdb/fddb.h"
#include "fdb/pools.h"
#include "fdb/indices.h"
#include "fdb/frames.h"
#include "fdb/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8rusage.h>

#include <math.h>
#include <sys/time.h>

fd_exception fd_ImpreciseTimestamp=_("Timestamp too imprecise");
fd_exception fd_MissingFeature=_("OS doesn't support operation");
static fd_exception strftime_error=_("internal strftime error");

static fdtype year_symbol, month_symbol, date_symbol;
static fdtype hours_symbol, minutes_symbol, seconds_symbol;
static fdtype milliseconds_symbol, microseconds_symbol, nanoseconds_symbol;
static fdtype precision_symbol, isostring_symbol, tzoff_symbol;
static fdtype spring_symbol, summer_symbol, autumn_symbol, winter_symbol;
static fdtype season_symbol, gmt_symbol;
static fdtype morning_symbol, afternoon_symbol, evening_symbol, nighttime_symbol;
static fdtype time_of_day_symbol;
static fdtype shortmonth_symbol, longmonth_symbol, shortday_symbol, longday_symbol;
static fdtype hms_symbol, string_symbol, shortstring_symbol;
static fdtype timestring_symbol, datestring_symbol, fullstring_symbol;
static fdtype fullstring_symbol;

static enum u8_timestamp_precision get_precision(fdtype sym)
{
  if (FD_EQ(sym,year_symbol)) return u8_year;
  else if (FD_EQ(sym,month_symbol)) return u8_month;
  else if (FD_EQ(sym,date_symbol)) return u8_day;
  else if (FD_EQ(sym,hours_symbol)) return u8_hour;
  else if (FD_EQ(sym,minutes_symbol)) return u8_minute;
  else if (FD_EQ(sym,seconds_symbol)) return u8_second;
  else if (FD_EQ(sym,milliseconds_symbol)) return u8_millisecond;
  else if (FD_EQ(sym,microseconds_symbol)) return u8_microsecond;
  else if (FD_EQ(sym,nanoseconds_symbol)) return u8_nanosecond;
  else return (enum u8_timestamp_precision) -1;
}

static fdtype gmtimestamp_prim()
{
  struct FD_TIMESTAMP *tm=u8_malloc(sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tm,fd_timestamp_type);
  u8_offtime(&(tm->xtime),time(NULL),0);
  return FDTYPE_CONS(tm);
}

static fdtype timestamp_prim(fdtype arg)
{
  struct FD_TIMESTAMP *tm=u8_malloc(sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tm,fd_timestamp_type);
  if (FD_VOIDP(arg)) {
    u8_now(&(tm->xtime));
    return FDTYPE_CONS(tm);}
  else if (FD_STRINGP(arg)) {
    u8_iso8601_to_xtime(FD_STRDATA(arg),&(tm->xtime));
    return FDTYPE_CONS(tm);}
  else if (FD_SYMBOLP(arg)) {
    enum u8_timestamp_precision prec=get_precision(arg);
    if (((int)prec)<0)
      return fd_type_error("timestamp precision","timestamp_prim",arg);
    u8_now(&(tm->xtime));
    tm->xtime.u8_prec=prec;
    return FDTYPE_CONS(tm);}
  else if (FD_FIXNUMP(arg)) {
    u8_offtime(&(tm->xtime),(time_t)(FD_FIX2INT(arg)),0);
    return FDTYPE_CONS(tm);}
  else if (FD_PTR_TYPEP(arg,fd_bigint_type)) {
    int tv=fd_bigint2int((fd_bigint)arg);
    u8_offtime(&(tm->xtime),(time_t)tv,0);
    return FDTYPE_CONS(tm);}
  else return fd_type_error("timestamp arg","timestamp_prim",arg);
}

static struct FD_TIMESTAMP *get_timestamp(fdtype arg,int *freeit)
{
  if (FD_PTR_TYPEP(arg,fd_timestamp_type)) {
    *freeit=0;
    return FD_GET_CONS(arg,fd_timestamp_type,struct FD_TIMESTAMP *);}
  else if (FD_STRINGP(arg)) {
    struct FD_TIMESTAMP *tm=u8_malloc(sizeof(struct FD_TIMESTAMP));
    u8_iso8601_to_xtime(FD_STRDATA(arg),&(tm->xtime)); *freeit=1;
    return tm;}
  else if (FD_FIXNUMP(arg)) {
    struct FD_TIMESTAMP *tm=u8_malloc(sizeof(struct FD_TIMESTAMP)); 
    u8_now(&(tm->xtime)); *freeit=1;
    u8_xtime_plus(&(tm->xtime),FD_FIX2INT(arg));
    return tm;}
  else {
    fd_set_type_error(fd_TypeError,"timestamp",arg);
    return NULL;}
}

static fdtype timestamp_plus(fdtype arg1,fdtype arg2)
{
  double delta; int free_old=0;
  struct U8_XTIME tmp, *btime;
  struct FD_TIMESTAMP *newtm=u8_malloc(sizeof(struct FD_TIMESTAMP)), *oldtm;
  if (FD_VOIDP(arg2)) {
    if ((FD_FIXNUMP(arg1)) || (FD_FLONUMP(arg1)) || (FD_RATIONALP(arg1)))
      delta=fd_todouble(arg1);
    else return fd_type_error("number","timestamp_plus",arg1);
    u8_now(&tmp); btime=&tmp;}
  else if ((FD_FIXNUMP(arg2)) || (FD_FLONUMP(arg2)) || (FD_RATIONALP(arg2))) {
    delta=fd_todouble(arg2);
    oldtm=get_timestamp(arg1,&free_old);
    btime=&(oldtm->xtime);}
  else return fd_type_error("number","timestamp_plus",arg2);
  /* Init the cons bit field */
  FD_INIT_CONS(newtm,fd_timestamp_type);
  /* Copy the data */
  memcpy(&(newtm->xtime),btime,sizeof(struct U8_XTIME));
  u8_xtime_plus(&(newtm->xtime),delta);
  if (free_old) u8_free(oldtm);
  return FDTYPE_CONS(newtm);
}

static fdtype timestamp_diff(fdtype timestamp1,fdtype timestamp2)
{
  int free1=0, free2=0; 
  struct FD_TIMESTAMP *t1=get_timestamp(timestamp1,&free1);
  struct FD_TIMESTAMP *t2=get_timestamp(timestamp2,&free2);
  double diff;
    if ((t1 == NULL) || (t2 == NULL)) {
      if (free1) u8_free(t1); if (free2) u8_free(t2);
      return fd_erreify();}
  else {
    if (free1) u8_free(t1); if (free2) u8_free(t2);
    return fd_init_double(NULL,u8_xtime_diff(&(t1->xtime),&(t2->xtime)));}
}

static fdtype timestamp_earlier(fdtype timestamp1,fdtype timestamp2)
{
  int free1=0; 
  struct FD_TIMESTAMP *t1=get_timestamp(timestamp1,&free1);
  if (t1==NULL) return fd_erreify();
  else if (FD_VOIDP(timestamp2)) {
    double diff;
    struct U8_XTIME xtime; u8_now(&xtime);
    if (free1) u8_free(t1);
    diff=u8_xtime_diff(&(t1->xtime),&xtime);
    if (diff<0) return FD_TRUE; else return FD_FALSE;}
  else {
    double diff; int free2;
    struct FD_TIMESTAMP *t2=get_timestamp(timestamp2,&free1);
    if (t2 == NULL) {
      if (free1) u8_free(t1); if (free2) u8_free(t2);
      return fd_erreify();}
    else diff=u8_xtime_diff(&(t1->xtime),&(t2->xtime));
    if (diff<0) return FD_TRUE; else return FD_FALSE;}
}

static fdtype timestamp_later(fdtype timestamp1,fdtype timestamp2)
{
  int free1=0; 
  struct FD_TIMESTAMP *t1=get_timestamp(timestamp1,&free1);
  if (t1==NULL) return fd_erreify();
  else if (FD_VOIDP(timestamp2)) {
    double diff;
    struct U8_XTIME xtime; u8_now(&xtime);
    if (free1) u8_free(t1);
    diff=u8_xtime_diff(&(t1->xtime),&xtime);
    if (diff>0) return FD_TRUE; else return FD_FALSE;}
  else {
    double diff; int free2;
    struct FD_TIMESTAMP *t2=get_timestamp(timestamp2,&free1);
    if (t2 == NULL) {
      if (free1) u8_free(t1); if (free2) u8_free(t2);
      return fd_erreify();}
    else diff=u8_xtime_diff(&(t1->xtime),&(t2->xtime));
    if (diff>0) return FD_TRUE; else return FD_FALSE;}
}

/* Lisp access */

static fdtype elapsed_time(fdtype arg)
{
  double elapsed=u8_elapsed_time();
  if (FD_VOIDP(arg))
    return fd_init_double(NULL,elapsed);
  else if (FD_FLONUMP(arg)) {
    double base=FD_FLONUM(arg);
    return fd_init_double(NULL,elapsed-base);}
  else return fd_type_error("double","elapsed_time",arg);
}

/* Timestamps as tables */

static fdtype use_strftime(char *format,struct U8_XTIME *xt)
{
  char *buf=u8_malloc(256);
  int n_bytes=strftime(buf,256,format,&(xt->u8_tptr));
  if (n_bytes<0) {
    u8_free(buf);
    return fd_err(strftime_error,"use_strftime",format,FD_VOID);}
  else return fd_init_string(NULL,n_bytes,buf);
}

static fdtype xtime_get(struct U8_XTIME *xt,fdtype slotid,int reterr)
{
  if (FD_EQ(slotid,year_symbol))
    if (xt->u8_prec>=u8_year)
      return FD_INT2DTYPE(xt->u8_tptr.tm_year+1900);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"year",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,fullstring_symbol)) 
    if (xt->u8_prec>=u8_day) 
      return use_strftime("%A %d %B %Y %r %Z",xt);
    else if (xt->u8_prec==u8_day)
      return use_strftime("%A %d %B %Y %Z",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"mon",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,gmt_symbol))
    if (xt->u8_tzoff==0) 
      return fd_make_timestamp(xt,NULL);
    else {
      struct U8_XTIME asgmt;
      if (xt->u8_prec>u8_second)
	u8_offtime_x(&asgmt,xt->u8_secs,xt->u8_nsecs,0);
      else u8_offtime_x(&asgmt,xt->u8_secs,-1,0);
      asgmt.u8_prec=xt->u8_prec;
      return fd_make_timestamp(&asgmt,NULL);}
  else if (FD_EQ(slotid,month_symbol))
    if (xt->u8_prec>=u8_month)
      return FD_INT2DTYPE(xt->u8_tptr.tm_mon);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"mon",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,shortmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%b",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"monthname",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,longmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%B",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"monthname",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,shortday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%a",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"dayname",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,longday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%A",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"dayname",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,hms_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%T",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"hms",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,timestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%X",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"timestring",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,string_symbol))
    if (xt->u8_prec>=u8_second)
      return use_strftime("%c",xt);
	else if (reterr)
	  return fd_err(fd_ImpreciseTimestamp,"string",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,shortstring_symbol))
    if (xt->u8_prec>=u8_second)
      return use_strftime("%d %b %Y %r",xt);
	else if (reterr)
	  return fd_err(fd_ImpreciseTimestamp,"string",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,datestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%x",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"datestring",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,date_symbol))
    if (xt->u8_prec>=u8_day)
      return FD_INT2DTYPE(xt->u8_tptr.tm_mday);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"mday",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,hours_symbol))
    if (xt->u8_prec>=u8_hour)
      return FD_INT2DTYPE(xt->u8_tptr.tm_hour);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"hour",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,minutes_symbol))
    if (xt->u8_prec>=u8_minute)
      return FD_INT2DTYPE(xt->u8_tptr.tm_min);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"min",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,seconds_symbol))
    if (xt->u8_prec>=u8_second)
      return FD_INT2DTYPE(xt->u8_tptr.tm_sec);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"sec",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,tzoff_symbol))
    return FD_INT2DTYPE(xt->u8_tzoff);
  else if (FD_EQ(slotid,precision_symbol))
    switch (xt->u8_prec) {
    case u8_year: year_symbol;
    case u8_month: month_symbol;
    case u8_day: date_symbol;
    case u8_hour: hours_symbol;
    case u8_minute: minutes_symbol;
    case u8_second: seconds_symbol;
    case u8_millisecond: milliseconds_symbol;
    case u8_microsecond: microseconds_symbol;
    case u8_nanosecond: nanoseconds_symbol;
    default: return FD_EMPTY_CHOICE;}
  else if (FD_EQ(slotid,isostring_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,32);
    u8_xtime_to_iso8601(&out,xt);
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else if (FD_EQ(slotid,season_symbol))
    if (xt->u8_prec>=u8_month) {
      fdtype results=FD_EMPTY_CHOICE;
      int mon=xt->u8_tptr.tm_mon+1;
      if ((mon>=12) || (mon<4)) {
	FD_ADD_TO_CHOICE(results,winter_symbol);}
      if ((mon>=3) && (mon<7)) {
	FD_ADD_TO_CHOICE(results,spring_symbol);}
      if ((mon>5) && (mon<10)) {
	FD_ADD_TO_CHOICE(results,summer_symbol);}
      if ((mon>8) && (mon<12)) {
	FD_ADD_TO_CHOICE(results,autumn_symbol);}
      return results;}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"mon",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,time_of_day_symbol))
    if (xt->u8_prec>=u8_hour) {
      fdtype results=FD_EMPTY_CHOICE;
      int hr=xt->u8_tptr.tm_mon;
      if ((hr<5) || (hr>20)) {
	FD_ADD_TO_CHOICE(results,nighttime_symbol);}
      if ((hr>5) && (hr<=12)) {
	FD_ADD_TO_CHOICE(results,morning_symbol);}
      if ((hr>=12) && (hr<19)) {
	FD_ADD_TO_CHOICE(results,afternoon_symbol);}
      if ((hr>16) && (hr<22)) {
	FD_ADD_TO_CHOICE(results,evening_symbol);}
      return results;}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"mon",NULL,FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (reterr)
    return fd_err(fd_NoSuchKey,"timestamp",NULL,slotid);
  else return FD_EMPTY_CHOICE;
}

static int xtime_set(struct U8_XTIME *xt,fdtype slotid,fdtype value)
{
  if (FD_EQ(slotid,year_symbol))
    if (FD_FIXNUMP(value))
      xt->u8_tptr.tm_year=FD_FIX2INT(value)-1900;
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("year")),value);
  else if (FD_EQ(slotid,month_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>0) && (FD_FIX2INT(value)<13))
      xt->u8_tptr.tm_mon=FD_FIX2INT(value)-1;
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("month")),value);
  else if (FD_EQ(slotid,date_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>0) && (FD_FIX2INT(value)<32))
      xt->u8_tptr.tm_mday=FD_FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("date")),value);
  else if (FD_EQ(slotid,hours_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>0) && (FD_FIX2INT(value)<32))
      xt->u8_tptr.tm_hour=FD_FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("hours")),value);
  else if (FD_EQ(slotid,minutes_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>=0) && (FD_FIX2INT(value)<60))
      xt->u8_tptr.tm_min=FD_FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("minutes")),value);
  else if (FD_EQ(slotid,seconds_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>=0) && (FD_FIX2INT(value)<60))
      xt->u8_tptr.tm_sec=FD_FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("seconds")),value);
  else return -1;
  return 0;
}

static fdtype timestamp_get(fdtype timestamp,fdtype slotid,fdtype dflt)
{
  struct FD_TIMESTAMP *tms=
    FD_GET_CONS(timestamp,fd_timestamp_type,struct FD_TIMESTAMP *);
  if (FD_VOIDP(dflt))
    return xtime_get(&(tms->xtime),slotid,1);
  else {
    fdtype result=xtime_get(&(tms->xtime),slotid,0);
    if (FD_EMPTY_CHOICEP(result)) return dflt;
    else return result;}
}

static fdtype modtime_prim(fdtype slotmap,fdtype base)
{
  fdtype result;
  if (FD_VOIDP(base)) 
    result=timestamp_prim(FD_VOID);
  else if (FD_PTR_TYPEP(base,fd_timestamp_type)) 
    result=fd_deep_copy(base); 
  else result=timestamp_prim(base);
  if (FD_ABORTP(result)) return result;
  else {
    struct U8_XTIME *xt=&((FD_GET_CONS(result,fd_timestamp_type,struct FD_TIMESTAMP *))->xtime);
    fdtype keys=fd_getkeys(slotmap); 
    FD_DO_CHOICES(key,keys) {
      fdtype val=fd_get(slotmap,key,FD_VOID);
      if (xtime_set(xt,key,val)<0) {
	result=fd_erreify(); FD_STOP_DO_CHOICES; break;}
      else {}}
    if (!(FD_ABORTP(result))) u8_mktime(xt);
    return result;}
}

static fdtype timestring()
{
  struct U8_XTIME onstack; struct U8_OUTPUT out;
  u8_localtime(&onstack,time(NULL));
  U8_INIT_OUTPUT(&out,16);
  u8_printf(&out,"%02d:%02d:%02d",
	    onstack.u8_tptr.tm_hour,
	    onstack.u8_tptr.tm_min,
	    onstack.u8_tptr.tm_sec);
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

/* Counting seconds */

static fdtype secs2string(fdtype secs)
{
  struct U8_OUTPUT out;
  if (FD_FIXNUMP(secs)) {
    int seconds=FD_FIX2INT(secs);
    U8_INIT_OUTPUT(&out,64);
    if (seconds>3600*24) {
      int days=seconds/(3600*24);
      if (days==1) u8_puts(&out,"1 day ");
      else u8_printf(&out,"%d days ",days);
      seconds=seconds-days*24*3600;}
    if (seconds>3600) {
      int hours=seconds/3600;
      if (hours==1) u8_puts(&out,"1 hour ");
      else u8_printf(&out,"%d hours ",hours);
      seconds=seconds-hours*3600;}
    if (seconds>60) {
      int minutes=seconds/60;
      if (minutes==1) u8_puts(&out,"1 minute ");
      else u8_printf(&out,"%d minutes ",minutes);
      seconds=seconds-minutes*60;}
    u8_printf(&out,"%d seconds",seconds);}
  else if (FD_FLONUMP(secs)) {
    double seconds=FD_FLONUM(secs);
    U8_INIT_OUTPUT(&out,64);
    if (seconds>3600*24) {
      int days=seconds/(3600*24);
      if (days==1) u8_puts(&out,"1 day ");
      else u8_printf(&out,"%d days ",days);
      seconds=seconds-days*24*3600;}
    if (seconds>3600) {
      int hours=seconds/3600;
      if (hours==1) u8_puts(&out,"1 hour ");
      else u8_printf(&out,"%d hours ",hours);
      seconds=seconds-hours*3600;}
    if (seconds>60) {
      int minutes=seconds/60;
      if (minutes==1) u8_puts(&out,"1 minute ");
      else u8_printf(&out,"%d minutes ",minutes);
      seconds=seconds-minutes*60;}
    if (seconds>1)
      u8_printf(&out,"%g seconds",seconds);
    else if (seconds>0.001)
      u8_printf(&out,"%g ms",seconds*1000);
    else if (seconds>0.000001)
      u8_printf(&out,"%g us",seconds*1000000);
    else u8_printf(&out,"%g ns",seconds*1000000);}
  else return fd_type_error(_("seconds"),"secs2string",secs);
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
}

/* Sleeping */

#if ((HAVE_SLEEP) || (HAVE_NANOSLEEP))
fdtype sleep_prim(fdtype arg)
{
  if (FD_FIXNUMP(arg)) {
    sleep(FD_FIX2INT(arg));
    return FD_TRUE;}
  else if (FD_FLONUMP(arg)) {
#if HAVE_NANOSLEEP
    double interval=FD_FLONUM(arg);
    struct timespec req;
    req.tv_sec=floor(interval);
    req.tv_nsec=1000000000*(interval-req.tv_sec);
    nanosleep(&req,NULL);
    return FD_TRUE;
#else
    return fd_type_error(_("fixnum time interval"),"sleep_prim",arg);
#endif
  }
  else return fd_type_error(_("time interval"),"sleep_prim",arg);
}
#endif

/* RUSAGE */

static fdtype data_symbol, stack_symbol, shared_symbol, resident_symbol, utime_symbol, stime_symbol;

static fdtype rusage_prim(fdtype field)
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return fd_erreify();
  else if (FD_VOIDP(field)) {
    fdtype result=fd_init_slotmap(NULL,0,NULL,NULL);
    fd_add(result,data_symbol,FD_INT2DTYPE(r.ru_idrss));
    fd_add(result,stack_symbol,FD_INT2DTYPE(r.ru_isrss));
    fd_add(result,shared_symbol,FD_INT2DTYPE(r.ru_ixrss));
    fd_add(result,resident_symbol,FD_INT2DTYPE(r.ru_maxrss));
    {
      fdtype tval=fd_init_double(NULL,(r.ru_utime.tv_sec*1000000.0+r.ru_utime.tv_usec*1.0));
      fd_add(result,utime_symbol,tval);
      fd_decref(tval);}
    {
      fdtype tval=fd_init_double(NULL,(r.ru_stime.tv_sec*1000000.0+r.ru_stime.tv_usec*1.0));
      fd_add(result,stime_symbol,tval);
      fd_decref(tval);}
    return result;}
  else if (FD_EQ(field,data_symbol))
    return FD_INT2DTYPE(r.ru_idrss);
  else if (FD_EQ(field,stack_symbol))
    return FD_INT2DTYPE(r.ru_isrss);
  else if (FD_EQ(field,shared_symbol))
    return FD_INT2DTYPE(r.ru_ixrss);
  else if (FD_EQ(field,resident_symbol))
    return FD_INT2DTYPE(r.ru_maxrss);
  else if (FD_EQ(field,utime_symbol))
    return fd_init_double
      (NULL,(r.ru_utime.tv_sec*1000000.0+r.ru_utime.tv_usec*1.0));
  else if (FD_EQ(field,stime_symbol))
    return fd_init_double
      (NULL,(r.ru_utime.tv_sec*1000000.0+r.ru_utime.tv_usec*1.0));
  else return FD_EMPTY_CHOICE;
}

static fdtype memusage_prim()
{
  unsigned long size=u8_memusage();
  return FD_INT2DTYPE(size);
}

/* Initialization */

FD_EXPORT void fd_init_timeprims_c()
{
  fd_register_source_file(versionid);

  tzset();

  fd_tablefns[fd_timestamp_type]=u8_malloc_type(struct FD_TABLEFNS);
  fd_tablefns[fd_timestamp_type]->get=timestamp_get;
  fd_tablefns[fd_timestamp_type]->add=NULL;
  fd_tablefns[fd_timestamp_type]->drop=NULL;
  fd_tablefns[fd_timestamp_type]->store=NULL;
  fd_tablefns[fd_timestamp_type]->test=NULL;
  fd_tablefns[fd_timestamp_type]->keys=NULL;

  year_symbol=fd_intern("YEAR");
  month_symbol=fd_intern("MONTH");
  date_symbol=fd_intern("DATE");
  hours_symbol=fd_intern("HOURS");
  minutes_symbol=fd_intern("MINUTES");
  seconds_symbol=fd_intern("SECONDS");
  isostring_symbol=fd_intern("ISO8601");
  precision_symbol=fd_intern("PRECISION");
  tzoff_symbol=fd_intern("TZOFF");

  time_of_day_symbol=fd_intern("TIME-OF-DAY");
  morning_symbol=fd_intern("MORNING");
  afternoon_symbol=fd_intern("AFTERNOON");
  evening_symbol=fd_intern("EVENING");
  nighttime_symbol=fd_intern("NIGHTTIME");

  season_symbol=fd_intern("SEASON");
  spring_symbol=fd_intern("SPRING");
  summer_symbol=fd_intern("SUMMER");
  autumn_symbol=fd_intern("AUTUMN");
  winter_symbol=fd_intern("WINTER");

  shortmonth_symbol=fd_intern("MONTH-SHORT");
  longmonth_symbol=fd_intern("MONTH-LONG");
  shortday_symbol=fd_intern("WEEKDAY-SHORT");
  longday_symbol=fd_intern("WEEKDAY-LONG");
  hms_symbol=fd_intern("HMS");
  string_symbol=fd_intern("STRING");
  shortstring_symbol=fd_intern("SHORTSTRING");
  timestring_symbol=fd_intern("TIMESTRING");
  datestring_symbol=fd_intern("DATESTRING");
  fullstring_symbol=fd_intern("FULLSTRING");

  gmt_symbol=fd_intern("GMT");

  data_symbol=fd_intern("DATA");
  stack_symbol=fd_intern("STACK");
  shared_symbol=fd_intern("SHARED");
  resident_symbol=fd_intern("RESIDENT");
  utime_symbol=fd_intern("UTIME");
  stime_symbol=fd_intern("STIME");

  fd_idefn(fd_scheme_module,fd_make_cprim0("GMTIMESTAMP",gmtimestamp_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TIMESTAMP",timestamp_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ELAPSED-TIME",elapsed_time,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("TIMESTRING",timestring,0));

  fd_idefn(fd_scheme_module,fd_make_cprim2("MODTIME",modtime_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("TIMESTAMP+",timestamp_plus,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("DIFFTIME",timestamp_diff,2));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2("TIME-EARLIER?",timestamp_earlier,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2("TIME-LATER?",timestamp_later,1));

#if ((HAVE_SLEEP) || (HAVE_NANOSLEEP))
  fd_idefn(fd_scheme_module,fd_make_cprim1("SLEEP",sleep_prim,1));
#endif

  fd_idefn(fd_scheme_module,fd_make_cprim1("SECS->STRING",secs2string,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("RUSAGE",rusage_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("MEMUSAGE",memusage_prim,0));
}


/* The CVS log for this file
   $Log: timeprims.c,v $
   Revision 1.24  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.23  2006/01/05 19:52:38  haase
   Added GMTIMESTAMP primitive

   Revision 1.22  2006/01/05 19:43:54  haase
   Added GMT property for timestamps

   Revision 1.21  2005/11/03 14:07:12  haase
   Renaming and reorganizing in TIMESTAMP.  TIMESTAMP now interprets an integer argument as a time_t and TIMESTAMP+ computes an offset from the current moment when called with one argument

   Revision 1.20  2005/10/31 15:52:20  haase
   Added SLEEP primitive

   Revision 1.19  2005/10/29 19:44:15  haase
   Move RETURN-ERROR to eval.c and defined ONERROR to provide simple error catching

   Revision 1.18  2005/08/29 12:23:08  haase
   Added secs->string

   Revision 1.17  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.16  2005/07/25 19:38:10  haase
   Added more virtual timestamp features

   Revision 1.15  2005/07/24 02:04:57  haase
   Fixes to timeprims time-of-day generation

   Revision 1.14  2005/07/23 22:21:48  haase
   Fixed time-earlier?

   Revision 1.13  2005/07/23 21:12:42  haase
   Added more timestamp properties

   Revision 1.12  2005/07/15 02:09:31  haase
   Renamed some time primitives and added TIME-EARLIER?

   Revision 1.11  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.10  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.9  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.8  2005/03/30 14:48:44  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.7  2005/03/26 19:09:24  haase
   Added TIMESTRING primitive

   Revision 1.6  2005/03/23 02:11:51  haase
   Extended timestamp properties to include season and time of day

   Revision 1.5  2005/03/23 01:43:39  haase
   Moved timestamp structure into fdlisp core

   Revision 1.4  2005/03/18 02:21:13  haase
   Added elapsed-time primitive

   Revision 1.3  2005/03/11 15:12:40  haase
   More timestamp extensions

   Revision 1.2  2005/03/11 14:43:54  haase
   Added more time functions

   Revision 1.1  2005/03/06 18:28:21  haase
   Added timeprims

*/
