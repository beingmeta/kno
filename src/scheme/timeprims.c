/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2011 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

/* #define FD_PROVIDE_FASTEVAL 1 */

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8netfns.h>

#include <ctype.h>
#include <math.h>
#include <sys/time.h>

fd_exception fd_ImpreciseTimestamp=_("Timestamp too imprecise");
fd_exception fd_InvalidTimestamp=_("Invalid timestamp object");
fd_exception fd_MissingFeature=_("OS doesn't support operation");
static fd_exception strftime_error=_("internal strftime error");

static fdtype year_symbol, month_symbol, date_symbol;
static fdtype hours_symbol, minutes_symbol, seconds_symbol;
static fdtype milliseconds_symbol, microseconds_symbol, nanoseconds_symbol;
static fdtype picoseconds_symbol, femtoseconds_symbol;
static fdtype precision_symbol, tzoff_symbol;
static fdtype spring_symbol, summer_symbol, autumn_symbol, winter_symbol;
static fdtype season_symbol, gmt_symbol, timezone_symbol;
static fdtype morning_symbol, afternoon_symbol, evening_symbol, nighttime_symbol;
static fdtype tick_symbol, xtick_symbol, prim_tick_symbol;
static fdtype iso_symbol, isostring_symbol, iso8601_symbol, rfc822_symbol;
static fdtype time_of_day_symbol, dowid_symbol, monthid_symbol;
static fdtype shortmonth_symbol, longmonth_symbol, shortday_symbol, longday_symbol;
static fdtype hms_symbol, dmy_symbol, dm_symbol, my_symbol;
static fdtype string_symbol, shortstring_symbol, fullstring_symbol;
static fdtype timestring_symbol, datestring_symbol;

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

static fdtype timestampp(fdtype arg)
{
  if (FD_PRIM_TYPEP(arg,fd_timestamp_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype timestamp_prim(fdtype arg)
{
  struct FD_TIMESTAMP *tm=u8_alloc(struct FD_TIMESTAMP);
  memset(tm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tm,fd_timestamp_type);
  if (FD_VOIDP(arg)) {
    u8_local_xtime(&(tm->xtime),-1,u8_femtosecond,0);
    return FDTYPE_CONS(tm);}
  else if (FD_STRINGP(arg)) {
    u8_string sdata=FD_STRDATA(arg);
    int c=*sdata;
    if (u8_isdigit(c))
      u8_iso8601_to_xtime(sdata,&(tm->xtime));
    else u8_rfc822_to_xtime(sdata,&(tm->xtime));
    return FDTYPE_CONS(tm);}
  else if (FD_SYMBOLP(arg)) {
    enum u8_timestamp_precision prec=get_precision(arg);
    if (((int)prec)<0)
      return fd_type_error("timestamp precision","timestamp_prim",arg);
    u8_local_xtime(&(tm->xtime),-1,prec,-1);
    return FDTYPE_CONS(tm);}
  else if (FD_FIXNUMP(arg)) {
    u8_local_xtime(&(tm->xtime),(time_t)(FD_FIX2INT(arg)),u8_second,-1);
    return FDTYPE_CONS(tm);}
  else if (FD_PTR_TYPEP(arg,fd_bigint_type)) {
#if (SIZEOF_TIME_T == 8)
    time_t tv=(time_t)fd_bigint_to_long_long((fd_bigint)arg);
#else
    time_t tv=(time_t)fd_bigint_to_long((fd_bigint)arg);
#endif
    u8_local_xtime(&(tm->xtime),tv,u8_second,-1);
    return FDTYPE_CONS(tm);}
  else if (FD_PTR_TYPEP(arg,fd_double_type)) {
    double dv=FD_FLONUM(arg);
    double dsecs=floor(dv), dnsecs=(dv-dsecs)*1000000000;
    unsigned int secs=(unsigned int)dsecs, nsecs=(unsigned int)dnsecs;
    u8_local_xtime(&(tm->xtime),(time_t)secs,u8_second,nsecs);
    return FDTYPE_CONS(tm);}
  else {
    u8_free(tm);
    return fd_type_error("timestamp arg","timestamp_prim",arg);}
}

static fdtype gmtimestamp_prim(fdtype arg)
{
  struct FD_TIMESTAMP *tm=u8_alloc(struct FD_TIMESTAMP);
  memset(tm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tm,fd_timestamp_type);
  if (FD_VOIDP(arg)) {
    u8_init_xtime(&(tm->xtime),-1,u8_femtosecond,0,0,0);
    return FDTYPE_CONS(tm);}
  else if (FD_PRIM_TYPEP(arg,fd_timestamp_type)) {
    struct FD_TIMESTAMP *ftm=FD_GET_CONS(arg,fd_timestamp_type,fd_timestamp);
    if (ftm->xtime.u8_tzoff==0) {
      u8_free(tm); return fd_incref(arg);}
    else {
      time_t tick=ftm->xtime.u8_tick;
      if (ftm->xtime.u8_prec>u8_second)
	u8_init_xtime(&(tm->xtime),tick,ftm->xtime.u8_prec,
		      ftm->xtime.u8_nsecs,0,0);
      else u8_init_xtime(&(tm->xtime),tick,ftm->xtime.u8_prec,0,0,0);
      return FDTYPE_CONS(tm);}}
  else if (FD_STRINGP(arg)) {
    u8_string sdata=FD_STRDATA(arg); int c=*sdata; time_t moment;
    if (u8_isdigit(c))
      u8_iso8601_to_xtime(sdata,&(tm->xtime));
    else u8_rfc822_to_xtime(sdata,&(tm->xtime));
    moment=u8_mktime(&(tm->xtime));
    return FDTYPE_CONS(tm);}
  else if (FD_SYMBOLP(arg)) {
    enum u8_timestamp_precision prec=get_precision(arg);
    if (((int)prec)<0)
      return fd_type_error("timestamp precision","timestamp_prim",arg);
    u8_init_xtime(&(tm->xtime),-1,prec,-1,0,0);
    return FDTYPE_CONS(tm);}
  else if (FD_FIXNUMP(arg)) {
    u8_init_xtime(&(tm->xtime),(time_t)(FD_FIX2INT(arg)),u8_second,-1,0,0);
    return FDTYPE_CONS(tm);}
  else if (FD_PTR_TYPEP(arg,fd_bigint_type)) {
#if (SIZEOF_TIME_T == 8)
    time_t tv=(time_t)fd_bigint_to_long_long((fd_bigint)arg);
#else
    time_t tv=(time_t)fd_bigint_to_long((fd_bigint)arg);
#endif
    u8_init_xtime(&(tm->xtime),tv,u8_second,-1,0,0);
    return FDTYPE_CONS(tm);}
  else if (FD_PTR_TYPEP(arg,fd_double_type)) {
    double dv=FD_FLONUM(arg);
    double dsecs=floor(dv), dnsecs=(dv-dsecs)*1000000000;
    unsigned int secs=(unsigned int)dsecs, nsecs=(unsigned int)dnsecs;
    u8_init_xtime(&(tm->xtime),(time_t)secs,u8_second,nsecs,0,0);
    return FDTYPE_CONS(tm);}
  else {
    u8_free(tm);
    return fd_type_error("timestamp arg","timestamp_prim",arg);}
}

static struct FD_TIMESTAMP *get_timestamp(fdtype arg,int *freeit)
{
  if (FD_PTR_TYPEP(arg,fd_timestamp_type)) {
    *freeit=0;
    return FD_GET_CONS(arg,fd_timestamp_type,struct FD_TIMESTAMP *);}
  else if (FD_STRINGP(arg)) {
    struct FD_TIMESTAMP *tm=u8_alloc(struct FD_TIMESTAMP);
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    u8_iso8601_to_xtime(FD_STRDATA(arg),&(tm->xtime)); *freeit=1;
    return tm;}
  else if (FD_FIXNUMP(arg)) {
    struct FD_TIMESTAMP *tm=u8_alloc(struct FD_TIMESTAMP); 
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    u8_now(&(tm->xtime)); *freeit=1;
    u8_xtime_plus(&(tm->xtime),FD_FIX2INT(arg));
    return tm;}
  else if (FD_VOIDP(arg)) {
    struct FD_TIMESTAMP *tm=u8_alloc(struct FD_TIMESTAMP); 
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    u8_now(&(tm->xtime)); *freeit=1;
    return tm;}
  else {
    fd_set_type_error("timestamp",arg);
    return NULL;}
}

static fdtype timestamp_plus(fdtype arg1,fdtype arg2)
{
  double delta; int free_old=0;
  struct U8_XTIME tmp, *btime;
  struct FD_TIMESTAMP *newtm=u8_alloc(struct FD_TIMESTAMP), *oldtm=NULL;
  memset(newtm,0,sizeof(struct FD_TIMESTAMP));
  if (FD_VOIDP(arg2)) {
    if ((FD_FIXNUMP(arg1)) || (FD_FLONUMP(arg1)) || (FD_RATIONALP(arg1)))
      delta=fd_todouble(arg1);
    else return fd_type_error("number","timestamp_plus",arg1);
    u8_init_xtime(&tmp,-1,u8_femtosecond,-1,0,0);
    btime=&tmp;}
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
  if ((t1 == NULL) || (t2 == NULL)) {
    if (free1) u8_free(t1); if (free2) u8_free(t2);
    return FD_ERROR_VALUE;}
  else if (FD_VOIDP(timestamp2)) {
    double diff=u8_xtime_diff(&(t2->xtime),&(t1->xtime));
    if (free1) u8_free(t1); if (free2) u8_free(t2);
    return fd_init_double(NULL,diff);}
  else {
    double diff=u8_xtime_diff(&(t1->xtime),&(t2->xtime));
    if (free1) u8_free(t1); if (free2) u8_free(t2);
    return fd_init_double(NULL,diff);}
}

static fdtype timestamp_earlier(fdtype timestamp1,fdtype timestamp2)
{
  int free1=0; 
  struct FD_TIMESTAMP *t1=get_timestamp(timestamp1,&free1);
  if (t1==NULL) return FD_ERROR_VALUE;
  else if (FD_VOIDP(timestamp2)) {
    double diff;
    struct U8_XTIME xtime; u8_now(&xtime);
    if (free1) u8_free(t1);
    diff=u8_xtime_diff(&(t1->xtime),&xtime);
    if (diff<0) return FD_TRUE; else return FD_FALSE;}
  else {
    double diff; int free2=0;
    struct FD_TIMESTAMP *t2=get_timestamp(timestamp2,&free2);
    if (t2 == NULL) {
      if (free1) u8_free(t1); if (free2) u8_free(t2);
      return FD_ERROR_VALUE;}
    else diff=u8_xtime_diff(&(t1->xtime),&(t2->xtime));
    if (diff<0) return FD_TRUE; else return FD_FALSE;}
}

static fdtype timestamp_later(fdtype timestamp1,fdtype timestamp2)
{
  int free1=0; 
  struct FD_TIMESTAMP *t1=get_timestamp(timestamp1,&free1);
  if (t1==NULL) return FD_ERROR_VALUE;
  else if (FD_VOIDP(timestamp2)) {
    double diff;
    struct U8_XTIME xtime; u8_now(&xtime);
    if (free1) u8_free(t1);
    diff=u8_xtime_diff(&(t1->xtime),&xtime);
    if (diff>0) return FD_TRUE; else return FD_FALSE;}
  else {
    double diff; int free2=0;
    struct FD_TIMESTAMP *t2=get_timestamp(timestamp2,&free2);
    if (t2 == NULL) {
      if (free1) u8_free(t1); if (free2) u8_free(t2);
      return FD_ERROR_VALUE;}
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

static fdtype dowids[7], monthids[12];
static u8_string month_names[12];
static fdtype xtime_keys=FD_EMPTY_CHOICE;

static fdtype use_strftime(char *format,struct U8_XTIME *xt)
{
  char *buf=u8_malloc(256); struct tm tptr;
  u8_xtime_to_tptr(xt,&tptr);
  int n_bytes=strftime(buf,256,format,&tptr);
  if (n_bytes<0) {
    u8_free(buf);
    return fd_err(strftime_error,"use_strftime",format,FD_VOID);}
  else return fd_init_string(NULL,n_bytes,buf);
}

static fdtype xtime_get(struct U8_XTIME *xt,fdtype slotid,int reterr)
{
  if (FD_EQ(slotid,year_symbol))
    if (xt->u8_prec>=u8_year)
      return FD_INT2DTYPE(xt->u8_year);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,fullstring_symbol)) 
    if (xt->u8_prec>=u8_day) 
      return use_strftime("%A %d %B %Y %r %Z",xt);
    else if (xt->u8_prec==u8_day)
      return use_strftime("%A %d %B %Y %Z",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if ((FD_EQ(slotid,iso_symbol)) ||
	   (FD_EQ(slotid,isostring_symbol)) ||
	   (FD_EQ(slotid,iso8601_symbol))) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_iso8601(&out,xt);
    return fd_stream2string(&out);}
  else if (FD_EQ(slotid,rfc822_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_rfc822(&out,xt);
    return fd_stream2string(&out);}
  else if (FD_EQ(slotid,gmt_symbol))
    if (xt->u8_tzoff==0) 
      return fd_make_timestamp(xt);
    else {
      struct U8_XTIME asgmt;
      u8_init_xtime(&asgmt,xt->u8_tick,xt->u8_prec,xt->u8_nsecs,0,0);
      return fd_make_timestamp(&asgmt);}
  else if (FD_EQ(slotid,month_symbol))
    if (xt->u8_prec>=u8_month)
      return FD_BYTE2DTYPE(xt->u8_mon);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,monthid_symbol))
    if (xt->u8_prec>=u8_month)
      if (xt->u8_mon<12)
	return monthids[xt->u8_mon];
      else if (reterr)
	return fd_err(fd_InvalidTimestamp,"xtime_get",
		      FD_SYMBOL_NAME(slotid),FD_VOID);
      else return FD_EMPTY_CHOICE;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,shortmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%b",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,longmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%B",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,dowid_symbol))
    if (xt->u8_prec>u8_month)
      if (xt->u8_wday<7)
	return dowids[xt->u8_wday];
      else if (reterr)
	return fd_err(fd_InvalidTimestamp,"xtime_get",
		      FD_SYMBOL_NAME(slotid),FD_VOID);
      else return FD_EMPTY_CHOICE;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,shortday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%a",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,longday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%A",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,hms_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%T",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,timestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%X",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,string_symbol))
    if (xt->u8_prec>=u8_second)
      return use_strftime("%c",xt);
	else if (reterr)
	  return fd_err(fd_ImpreciseTimestamp,"xtime_get",
			FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,shortstring_symbol))
    if (xt->u8_prec>=u8_second)
      return use_strftime("%d %b %Y %r",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,datestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%x",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,dmy_symbol))
    if (xt->u8_prec>=u8_day) {
      if (xt->u8_mon<12) {
	char buf[64];
	sprintf(buf,"%d%s%04d",xt->u8_mday,
		month_names[xt->u8_mon],xt->u8_year);
	return fd_make_string(NULL,-1,buf);}
      else if (reterr)
	return fd_err(fd_InvalidTimestamp,"xtime_get",
		      FD_SYMBOL_NAME(slotid),FD_VOID);
      else return FD_EMPTY_CHOICE;}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,dm_symbol))
    if (xt->u8_prec>=u8_day)
      if (xt->u8_mon<12) {
	char buf[64];
	sprintf(buf,"%d%s",xt->u8_mday,month_names[xt->u8_mon]);
	return fd_make_string(NULL,-1,buf);}
      else if (reterr)
	return fd_err(fd_InvalidTimestamp,"xtime_get",
		      FD_SYMBOL_NAME(slotid),FD_VOID);
      else return FD_EMPTY_CHOICE;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,my_symbol))
    if (xt->u8_prec>=u8_month)
      if (xt->u8_mon<12) {
	char buf[64];
	sprintf(buf,"%s%d",month_names[xt->u8_mon],xt->u8_year);
	return fd_make_string(NULL,-1,buf);}
      else if (reterr)
	return fd_err(fd_InvalidTimestamp,"xtime_get",
		      FD_SYMBOL_NAME(slotid),FD_VOID);
      else return FD_EMPTY_CHOICE;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,date_symbol))
    if (xt->u8_prec>=u8_day)
      return FD_BYTE2DTYPE(xt->u8_mday);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,hours_symbol))
    if (xt->u8_prec>=u8_hour)
      return FD_BYTE2DTYPE(xt->u8_hour);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,minutes_symbol))
    if (xt->u8_prec>=u8_minute)
      return FD_BYTE2DTYPE(xt->u8_min);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,seconds_symbol))
    if (xt->u8_prec>=u8_second)
      return FD_BYTE2DTYPE(xt->u8_sec);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,tzoff_symbol))
    return FD_SHORT2DTYPE(xt->u8_tzoff);
  else if (FD_EQ(slotid,tick_symbol))
    if (xt->u8_prec>=u8_second) {
      time_t tick=xt->u8_tick;
      return FD_INT2DTYPE((long)tick);}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,prim_tick_symbol)) {
    time_t tick=xt->u8_tick;
    return FD_INT2DTYPE((long)tick);}
  else if (FD_EQ(slotid,xtick_symbol))
    if (xt->u8_prec>=u8_second) {
      double dsecs=(double)(xt->u8_tick), dnsecs=(double)(xt->u8_nsecs);
      return fd_init_double(NULL,dsecs+(dnsecs/1000000000.0));}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if ((FD_EQ(slotid,nanoseconds_symbol)) ||
	   (FD_EQ(slotid,microseconds_symbol)) ||
	   (FD_EQ(slotid,milliseconds_symbol)))
    if (xt->u8_prec>=u8_second) {
      unsigned int nsecs=xt->u8_nsecs;
      if (FD_EQ(slotid,nanoseconds_symbol))
	return FD_INT2DTYPE(nsecs);
      else {
	unsigned int reduce=
	  ((FD_EQ(slotid,microseconds_symbol)) ? (1000) :(1000000));
	unsigned int half_reduce=reduce/2;
	unsigned int retval=((nsecs/reduce)+((nsecs%reduce)>=half_reduce));
	return FD_INT2DTYPE(retval);}}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,precision_symbol))
    switch (xt->u8_prec) {
    case u8_year: return year_symbol;
    case u8_month: return month_symbol;
    case u8_day: return date_symbol;
    case u8_hour: return hours_symbol;
    case u8_minute: return minutes_symbol;
    case u8_second: return seconds_symbol;
    case u8_millisecond: return milliseconds_symbol;
    case u8_microsecond: return microseconds_symbol;
    case u8_nanosecond: return nanoseconds_symbol;
    default: return FD_EMPTY_CHOICE;}
  else if (FD_EQ(slotid,season_symbol))
    if (xt->u8_prec>=u8_month) {
      fdtype results=FD_EMPTY_CHOICE;
      int mon=xt->u8_mon+1;
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
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,time_of_day_symbol))
    if (xt->u8_prec>=u8_hour) {
      fdtype results=FD_EMPTY_CHOICE;
      int hr=xt->u8_hour;
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
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (reterr)
    return fd_err(fd_NoSuchKey,"timestamp",NULL,slotid);
  else return FD_EMPTY_CHOICE;
}

static int xtime_set(struct U8_XTIME *xt,fdtype slotid,fdtype value)
{
  if (FD_EQ(slotid,year_symbol))
    if (FD_FIXNUMP(value))
      xt->u8_year=FD_FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("year")),value);
  else if (FD_EQ(slotid,month_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>0) && (FD_FIX2INT(value)<13))
      xt->u8_mon=FD_FIX2INT(value)-1;
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("month")),value);
  else if (FD_EQ(slotid,date_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>0) && (FD_FIX2INT(value)<32))
      xt->u8_mday=FD_FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("date")),value);
  else if (FD_EQ(slotid,hours_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>=0) && (FD_FIX2INT(value)<32))
      xt->u8_hour=FD_FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("hours")),value);
  else if (FD_EQ(slotid,minutes_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>=0) && (FD_FIX2INT(value)<60))
      xt->u8_min=FD_FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("minutes")),value);
  else if (FD_EQ(slotid,seconds_symbol))
    if ((FD_FIXNUMP(value)) &&
	(FD_FIX2INT(value)>=0) && (FD_FIX2INT(value)<60))
      xt->u8_sec=FD_FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("seconds")),value);
  else if (FD_EQ(slotid,timezone_symbol)) {
    if (FD_STRINGP(value)) {
      int tz=u8_parse_tzspec(FD_STRDATA(value),xt->u8_tzoff);
      xt->u8_tzoff=tz;}
    else if (FD_FIXNUMP(value)) {
      int offset=0;
      if ((FD_FIX2INT(value)>=-12) && (FD_FIX2INT(value)<=12))
	offset=3600*FD_FIX2INT(value);
      else offset=FD_FIX2INT(value);
      xt->u8_tzoff=offset;}
    else return fd_reterr(fd_TypeError,"xtime_set",
			  u8_strdup(_("seconds")),value);}
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

static fdtype timestamp_getkeys(fdtype timestamp)
{
  /* This could be clever about precision, but currently it isn't */
  return fd_incref(xtime_keys);
}

static fdtype modtime_prim(fdtype slotmap,fdtype base,fdtype togmt)
{
  fdtype result;
  if (FD_VOIDP(base)) 
    result=timestamp_prim(FD_VOID);
  else if (FD_PTR_TYPEP(base,fd_timestamp_type)) 
    result=fd_deep_copy(base); 
  else result=timestamp_prim(base);
  if (FD_ABORTP(result)) return result;
  else {
    struct U8_XTIME *xt=
      &((FD_GET_CONS(result,fd_timestamp_type,struct FD_TIMESTAMP *))->xtime);
    int tzoff=xt->u8_tzoff;
    fdtype keys=fd_getkeys(slotmap); 
    FD_DO_CHOICES(key,keys) {
      fdtype val=fd_get(slotmap,key,FD_VOID);
      if (xtime_set(xt,key,val)<0) {
	result=FD_ERROR_VALUE; FD_STOP_DO_CHOICES; break;}
      else {}}
    if (FD_ABORTP(result)) return result;
    else if (FD_FALSEP(togmt)) {
      time_t moment=u8_mktime(xt);
      u8_init_xtime(xt,moment,xt->u8_prec,xt->u8_nsecs,tzoff,0);
      return result;}
    else {
      time_t moment=u8_mktime(xt);
      u8_init_xtime(xt,moment,xt->u8_prec,xt->u8_nsecs,0,0);
      return result;}}
}      

/* Miscellanous time utilities */

static fdtype timestring()
{
  struct U8_XTIME onstack; struct U8_OUTPUT out;
  u8_local_xtime(&onstack,-1,u8_second,0);
  U8_INIT_OUTPUT(&out,16);
  u8_printf(&out,"%02d:%02d:%02d",
	    onstack.u8_hour,
	    onstack.u8_min,
	    onstack.u8_sec);
  return fd_stream2string(&out);
}

static fdtype time_prim()
{
  time_t now=time(NULL);
  if (now<0) {
    u8_graberr(-1,"time_prim",NULL);
    return FD_ERROR_VALUE;}
  else return FD_INT2DTYPE(now);
}

static fdtype millitime_prim()
{
  long long now=u8_millitime();
  if (now<0) {
    u8_graberr(-1,"millitime_prim",NULL);
    return FD_ERROR_VALUE;}
  else return FD_INT2DTYPE(now);
}

static fdtype microtime_prim()
{
  long long now=u8_microtime();
  if (now<0) {
    u8_graberr(-1,"microtime_prim",NULL);
    return FD_ERROR_VALUE;}
  else return FD_INT2DTYPE(now);
}

/* Counting seconds */

static fdtype secs2string(fdtype secs,fdtype prec_arg)
{
  struct U8_OUTPUT out;
  int precision=((FD_FIXNUMP(prec_arg)) ? (FD_FIX2INT(prec_arg)) :
		 (FD_FALSEP(prec_arg)) ? (-1) : (0));
  int elts=0;
  double seconds;
  int years, months, weeks, days, hours, minutes, need_comma=0;
  if (FD_FIXNUMP(secs)) 
    seconds=(double)FD_FIX2INT(secs);
  else if (FD_FLONUMP(secs))
    seconds=FD_FLONUM(secs);
  else return fd_type_error(_("seconds"),"secs2string",secs);
  U8_INIT_OUTPUT(&out,64);
  if (seconds<0) {
    u8_printf(&out,"negative "); seconds=-seconds;}
  years=(int)floor(seconds/(365*24*3600));
  seconds=seconds-years*(365*24*3600);
  months=(int)floor(seconds/(30*24*3600));
  seconds=seconds-months*(30*24*3600);
  weeks=(int)floor(seconds/(7*24*3600));
  seconds=seconds-weeks*(7*24*3600);
  days=(int)floor(seconds/(24*3600));
  seconds=seconds-days*(3600*24);
  hours=(int)floor(seconds/(3600));
  seconds=seconds-hours*(3600);
  minutes=floor(seconds/60);
  seconds=seconds-minutes*60;

  if ((precision>0) && (elts>=precision)) {}
  else if (years>0) {
    if (elts>0) u8_puts(&out,", ");
    if (years>1) {
      u8_printf(&out,_("%d years"),years);}
    else if (years==1) {
      u8_printf(&out,_("one year"));}
    else {}
    elts++;}

  if ((precision>0) && (elts>=precision)) {}
  else if (months>0) {
    if (elts>0) u8_puts(&out,", ");
    if (months>1) {
      u8_printf(&out,_("%d months"),months);}
    else if (months==1) {
      u8_printf(&out,_("one month"));}
    else {}
    elts++;}

  if ((precision>0) && (elts>=precision)) {}
  else if (weeks>0) {
    if (elts>0) u8_puts(&out,", ");
    if (weeks>1) 
      u8_printf(&out,_("%d weeks"),weeks);
    else if (weeks==1) 
      u8_printf(&out,_("one week"));
    else {}
    elts++;}

  if ((precision>0) && (elts>=precision)) {}
  else if (days>0) {
    if (elts>0) u8_puts(&out,", ");
    if (days>1) 
      u8_printf(&out,_("%d days"),days);
    else if (days==1) 
      u8_printf(&out,_("one day"));
    else {}
    elts++;}

  if ((precision>0) && (elts>=precision)) {}
  else if (hours>0) {
    if (elts>0) u8_puts(&out,", ");
    if (hours>1) 
      u8_printf(&out,_("%d hours"),hours);
    else if (hours==1) 
      u8_printf(&out,_("one hour"));
    else {}
    elts++;}

  if ((precision>0) && (elts>=precision)) {}
  else if (minutes>0) {
    if (elts>0) u8_puts(&out,", ");
    if (minutes>1) 
      u8_printf(&out,_("%d minutes"),minutes);
    else if (minutes==1) 
      u8_printf(&out,_("one minute"));
    else {}
    elts++;}

  if ((precision>0) && (elts>=precision)) {}
  else if (seconds==0) {}
  else if ((precision==0) && (elts>0) &&
	   (FD_FLONUMP(secs)) && (seconds>1)) {
    /* This is the case where we round the seconds */
    if (elts>0) u8_puts(&out,", ");
    u8_printf(&out,_("%d seconds"),(int)floor(seconds));}
  else if (FD_FLONUMP(secs)) {
    if (elts>0) u8_puts(&out,", ");
    u8_printf(&out,_("%f seconds"),seconds);}
  else {
    if (elts>0) u8_puts(&out,", ");
    u8_printf(&out,_("%d seconds"),(int)floor(seconds));}

  return fd_stream2string(&out);
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

/* GETENV primitive */

static fdtype getenv_prim(fdtype var)
{
  u8_string enval=u8_getenv(FD_STRDATA(var));
  if (enval==NULL) return FD_FALSE;
  else return fd_lispstring(enval);
}

/* Getting the current hostname */

/* There's not a good justification for putting this here other
   than that it has to do with getting stuff from the environment. */
static fdtype hostname_prim()
{
  return fd_lispstring(u8_gethostname());
}

/* There's not a good justification for putting this here other
   than that it has to do with getting stuff from the environment. */
static fdtype hostaddrs_prim(fdtype hostname)
{
  int addr_len=-1; unsigned int type;
  char **addrs=u8_lookup_host(FD_STRDATA(hostname),&addr_len,&type);
  fdtype results=FD_EMPTY_CHOICE;
  int i=0;
  if (addrs==NULL) return results;
  else while (addrs[i]) {
    unsigned char *addr=addrs[i++]; fdtype string;
    struct U8_OUTPUT out; int j=0; U8_INIT_OUTPUT(&out,16);
    while (j<addr_len) {
      u8_printf(&out,((j>0)?(".%d"):("%d")),(int)addr[j]);
      j++;}
    string=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
    FD_ADD_TO_CHOICE(results,string);}
  u8_free(addrs);
  return results;
}

/* RUSAGE */

static fdtype data_symbol, stack_symbol, shared_symbol, resident_symbol;
static fdtype utime_symbol, stime_symbol, cpusage_symbol, clock_symbol;

static fdtype rusage_prim(fdtype field)
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return FD_ERROR_VALUE;
  else if (FD_VOIDP(field)) {
    fdtype result=fd_init_slotmap(NULL,0,NULL);
    fd_add(result,data_symbol,FD_INT2DTYPE(r.ru_idrss));
    fd_add(result,stack_symbol,FD_INT2DTYPE(r.ru_isrss));
    fd_add(result,shared_symbol,FD_INT2DTYPE(r.ru_ixrss));
    fd_add(result,resident_symbol,FD_INT2DTYPE(r.ru_maxrss));
    {
      double elapsed=u8_elapsed_time();
      fdtype tval=fd_init_double(NULL,elapsed);
      fd_add(result,clock_symbol,tval);
      fd_decref(tval);}
    {
      fdtype tval=fd_make_double(u8_dbltime(r.ru_utime));
      fd_add(result,utime_symbol,tval);
      fd_decref(tval);}
    {
      fdtype tval=fd_make_double(u8_dbltime(r.ru_stime));
      fd_add(result,stime_symbol,tval);
      fd_decref(tval);}
    return result;}
  else if (FD_EQ(field,data_symbol))
    return FD_INT2DTYPE(r.ru_idrss);
  else if (FD_EQ(field,cpusage_symbol)) {
    double elapsed=u8_elapsed_time()*1000000.0;
    double stime=u8_dbltime(r.ru_stime);
    double utime=u8_dbltime(r.ru_utime);
    double cpusage=(stime+utime)*100.0/elapsed;
    return fd_init_double(NULL,cpusage);}
  else if (FD_EQ(field,stack_symbol))
    return FD_INT2DTYPE(r.ru_isrss);
  else if (FD_EQ(field,shared_symbol))
    return FD_INT2DTYPE(r.ru_ixrss);
  else if (FD_EQ(field,resident_symbol))
    return FD_INT2DTYPE(r.ru_maxrss);
  else if (FD_EQ(field,utime_symbol))
    return fd_make_double(u8_dbltime(r.ru_utime));
  else if (FD_EQ(field,stime_symbol))
    return fd_make_double(u8_dbltime(r.ru_stime));
  else return FD_EMPTY_CHOICE;
}

static fdtype memusage_prim()
{
  unsigned long size=u8_memusage();
  return FD_INT2DTYPE(size);
}

static fdtype usertime_prim()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return FD_ERROR_VALUE;
  else return fd_init_double
	 (NULL,(r.ru_utime.tv_sec*1000000.0+r.ru_utime.tv_usec*1.0));
}

static fdtype systime_prim()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return FD_ERROR_VALUE;
  else return fd_init_double
	 (NULL,(r.ru_stime.tv_sec*1000000.0+r.ru_stime.tv_usec*1.0));
}

static fdtype cpusage_prim(fdtype arg)
{
  if (FD_VOIDP(arg))
    return rusage_prim(cpusage_symbol);
  else {
    struct rusage r;
    memset(&r,0,sizeof(r));
    if (u8_getrusage(RUSAGE_SELF,&r)<0) 
      return FD_ERROR_VALUE;
    else {
      fdtype prelapsed=fd_get(arg,clock_symbol,FD_VOID);
      fdtype prestime=fd_get(arg,stime_symbol,FD_VOID);
      fdtype preutime=fd_get(arg,utime_symbol,FD_VOID);
      if ((FD_PRIM_TYPEP(prelapsed,fd_double_type)) &&
	  (FD_PRIM_TYPEP(prestime,fd_double_type)) &&
	  (FD_PRIM_TYPEP(preutime,fd_double_type))) {
	double elapsed=
	  (u8_elapsed_time()-FD_FLONUM(prelapsed))*1000000.0;
	double stime=(u8_dbltime(r.ru_stime)-FD_FLONUM(prestime));
	double utime=u8_dbltime(r.ru_utime)-FD_FLONUM(preutime);
	double cpusage=(stime+utime)*100.0/elapsed;
	return fd_init_double(NULL,cpusage);}
      else return fd_type_error(_("rusage"),"getcpusage",arg);}}
}

/* Initialization */

static void init_id_tables()
{
  dowids[0]=fd_intern("SUN");
  dowids[1]=fd_intern("MON");
  dowids[2]=fd_intern("TUE");
  dowids[3]=fd_intern("WED");
  dowids[4]=fd_intern("THU");
  dowids[5]=fd_intern("FRI");
  dowids[6]=fd_intern("SAT");
  monthids[0]=fd_intern("JAN");
  monthids[1]=fd_intern("FEB");
  monthids[2]=fd_intern("MAR");
  monthids[3]=fd_intern("APR");
  monthids[4]=fd_intern("MAY");
  monthids[5]=fd_intern("JUN");
  monthids[6]=fd_intern("JUL");
  monthids[7]=fd_intern("AUG");
  monthids[8]=fd_intern("SEP");
  monthids[9]=fd_intern("OCT");
  monthids[10]=fd_intern("NOV");
  monthids[11]=fd_intern("DEC");
  month_names[0]="Jan";
  month_names[1]="Feb";
  month_names[2]="Mar";
  month_names[3]="Apr";
  month_names[4]="May";
  month_names[5]="Jun";
  month_names[6]="Jul";
  month_names[7]="Aug";
  month_names[8]="Sep";
  month_names[9]="Oct";
  month_names[10]="Nov";
  month_names[11]="Dec";
}

/* CALLTRACK SENSORS */

/* See src/dtype/apply.c for a description of calltrack, which is a
   profiling utility for higher level programs.
   These functions allow the tracking of various RUSAGE fields
   over program execution.
*/

#if FD_CALLTRACK_ENABLED
static double utime_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return 0.0;
  else return r.ru_utime.tv_sec*1000000.0+r.ru_utime.tv_usec*1.0;
}
static double stime_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return 0.0;
  else return r.ru_stime.tv_sec*1000000.0+r.ru_stime.tv_usec*1.0;
}
#if HAVE_STRUCT_RUSAGE_RU_INBLOCK
static long inblock_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return 0;
  else return r.ru_inblock;
}
static long outblock_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return 0;
  else return r.ru_oublock;
}
#endif
#if HAVE_STRUCT_RUSAGE_RU_MAJFLT
static long majflt_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return 0;
  else return r.ru_majflt;
}
static long nswaps_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return 0;
  else return r.ru_nswap;
}
#endif
#if HAVE_STRUCT_RUSAGE_RU_NVCSW
static long cxtswitch_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return 0;
  else return r.ru_nvcsw+r.ru_nivcsw;
}
static long vcxtswitch_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return 0;
  else return r.ru_nvcsw;
}
static long ivcxtswitch_sensor()
{
  struct rusage r;
  memset(&r,0,sizeof(r));
  if (u8_getrusage(RUSAGE_SELF,&r)<0) 
    return 0;
  else return r.ru_nivcsw;
}
#endif
#endif

/* CALLTRACK INTERFACE */

/* This is the Scheme API for accessing CALLTRACK */

static fdtype calltrack_sensors()
{
#if FD_CALLTRACK_ENABLED
  return fd_calltrack_sensors();
#else
  return fd_init_vector(NULL,0,NULL);
#endif
}

static fdtype calltrack_sense(fdtype all)
{
#if FD_CALLTRACK_ENABLED
  if (FD_FALSEP(all)) 
    return fd_calltrack_sense(0);
  else return fd_calltrack_sense(1);
#else
  return fd_init_vector(NULL,0,NULL);
#endif
}

/* UUID functions */

static fdtype uuidp_prim(fdtype x)
{
  if (FD_PRIM_TYPEP(x,fd_uuid_type)) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype getuuid_prim(fdtype nodeid,fdtype tptr)
{
  struct U8_XTIME *xt=NULL;
  long long id=-1;
  if ((FD_VOIDP(tptr))&&(FD_STRINGP(nodeid))) {
    /* Assume it's a UUID string, so parse it. */
      struct FD_UUID *uuid=u8_alloc(struct FD_UUID);
      u8_string start=FD_STRDATA(nodeid);
      FD_INIT_CONS(uuid,fd_uuid_type);
      if ((start[0]==':')&&(start[1]=='#')&&
	  (start[2]=='U')&&(isxdigit(start[3])))
	start=start+3;
      else if ((start[0]=='#')&&(start[1]=='U')&&(isxdigit(start[2])))
	start=start+2;
      else if ((start[0]=='U')&&(isxdigit(start[1])))
	start=start+1;
      else if (isxdigit(start[0])) {}
      else return fd_type_error("UUID string","getuuid_prim",nodeid);
      u8_parseuuid(start,(u8_uuid)&(uuid->uuid));
      return FDTYPE_CONS(uuid);}
  else if ((FD_VOIDP(tptr))&&(FD_PACKETP(nodeid)))
    if (FD_PACKET_LENGTH(nodeid)==16) {
      struct FD_UUID *uuid=u8_alloc(struct FD_UUID);
      unsigned char *data=FD_PACKET_DATA(nodeid);
      FD_INIT_CONS(uuid,fd_uuid_type);
      memcpy(&(uuid->uuid),data,16);
      return FDTYPE_CONS(uuid);}
    else return fd_type_error("UUID (16-byte packet)","getuuid_prim",nodeid);
  else if ((FD_VOIDP(tptr))&&(FD_PRIM_TYPEP(nodeid,fd_uuid_type)))
    return fd_incref(nodeid);
  else if ((FD_VOIDP(tptr))&&(FD_PRIM_TYPEP(nodeid,fd_timestamp_type))) {
    fdtype tmp=tptr; tptr=nodeid; nodeid=tmp;}
  if ((FD_VOIDP(tptr))&&(FD_VOIDP(nodeid)))
    return fd_fresh_uuid(NULL);
  if (FD_PRIM_TYPEP(tptr,fd_timestamp_type)) {
    struct FD_TIMESTAMP *tstamp=
      FD_GET_CONS(tptr,fd_timestamp_type,struct FD_TIMESTAMP *);
    xt=&(tstamp->xtime);}
  if (FD_FIXNUMP(nodeid))
    id=((long long)(FD_FIX2INT(nodeid)));
  else if (FD_BIGINTP(nodeid))
    id=fd_bigint2int64((fd_bigint)nodeid);
  else if (FD_VOIDP(nodeid)) id=-1;
  else return fd_type_error("node id","getuuid_prim",nodeid);
  return fd_cons_uuid(NULL,xt,id,-1);
}

static fdtype uuidtime_prim(fdtype uuid_arg)
{
  struct FD_UUID *uuid=FD_GET_CONS(uuid_arg,fd_uuid_type,struct FD_UUID *);
  struct FD_TIMESTAMP *tstamp=u8_alloc(struct FD_TIMESTAMP);
  FD_INIT_CONS(tstamp,fd_timestamp_type);
  if (u8_uuid_xtime(uuid->uuid,&(tstamp->xtime)))
    return FDTYPE_CONS(tstamp);
  else {
    u8_free(tstamp);
    return fd_type_error("time-based UUID","uuidtime_prim",uuid_arg);}
}

static fdtype uuidnode_prim(fdtype uuid_arg)
{
  struct FD_UUID *uuid=FD_GET_CONS(uuid_arg,fd_uuid_type,struct FD_UUID *);
  long long id= u8_uuid_nodeid(uuid->uuid);
  if (id<0)
    return fd_type_error("time-based UUID","uuidnode_prim",uuid_arg);
  else return FD_INT2DTYPE(id);
}

static fdtype uuidstring_prim(fdtype uuid_arg)
{
  struct FD_UUID *uuid=FD_GET_CONS(uuid_arg,fd_uuid_type,struct FD_UUID *);
  return fd_init_string(NULL,36,u8_uuidstring(uuid->uuid,NULL));
}

static fdtype uuidpacket_prim(fdtype uuid_arg)
{
  struct FD_UUID *uuid=FD_GET_CONS(uuid_arg,fd_uuid_type,struct FD_UUID *);
  return fd_make_packet(NULL,16,uuid->uuid);
}

/* Initialization */

FD_EXPORT void fd_init_timeprims_c()
{
  fd_register_source_file(versionid);

  tzset();

  init_id_tables();

  fd_tablefns[fd_timestamp_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_timestamp_type]->get=timestamp_get;
  fd_tablefns[fd_timestamp_type]->add=NULL;
  fd_tablefns[fd_timestamp_type]->drop=NULL;
  fd_tablefns[fd_timestamp_type]->store=NULL;
  fd_tablefns[fd_timestamp_type]->test=NULL;
  fd_tablefns[fd_timestamp_type]->keys=timestamp_getkeys;

  year_symbol=fd_intern("YEAR");
  FD_ADD_TO_CHOICE(xtime_keys,year_symbol);
  month_symbol=fd_intern("MONTH");
  FD_ADD_TO_CHOICE(xtime_keys,month_symbol);
  date_symbol=fd_intern("DATE");
  FD_ADD_TO_CHOICE(xtime_keys,date_symbol);
  hours_symbol=fd_intern("HOURS");
  FD_ADD_TO_CHOICE(xtime_keys,hours_symbol);
  minutes_symbol=fd_intern("MINUTES");
  FD_ADD_TO_CHOICE(xtime_keys,minutes_symbol);
  seconds_symbol=fd_intern("SECONDS");
  FD_ADD_TO_CHOICE(xtime_keys,seconds_symbol);
  precision_symbol=fd_intern("PRECISION");
  FD_ADD_TO_CHOICE(xtime_keys,precision_symbol);
  tzoff_symbol=fd_intern("TZOFF");
  FD_ADD_TO_CHOICE(xtime_keys,tzoff_symbol);
  
  milliseconds_symbol=fd_intern("MILLISECONDS");
  FD_ADD_TO_CHOICE(xtime_keys,milliseconds_symbol);
  microseconds_symbol=fd_intern("MICROSECONDS");
  FD_ADD_TO_CHOICE(xtime_keys,microseconds_symbol);
  nanoseconds_symbol=fd_intern("NANOSECONDS");
  FD_ADD_TO_CHOICE(xtime_keys,nanoseconds_symbol);
  picoseconds_symbol=fd_intern("PICOSECONDS");
  FD_ADD_TO_CHOICE(xtime_keys,picoseconds_symbol);
  femtoseconds_symbol=fd_intern("FEMTOSECONDS");
  FD_ADD_TO_CHOICE(xtime_keys,femtoseconds_symbol);
  
  tick_symbol=fd_intern("TICK");
  FD_ADD_TO_CHOICE(xtime_keys,tick_symbol);
  prim_tick_symbol=fd_intern("%TICK");
  FD_ADD_TO_CHOICE(xtime_keys,prim_tick_symbol);
  xtick_symbol=fd_intern("XTICK");
  FD_ADD_TO_CHOICE(xtime_keys,xtick_symbol);
  iso_symbol=fd_intern("ISO");
  FD_ADD_TO_CHOICE(xtime_keys,iso_symbol);
  isostring_symbol=fd_intern("ISOSTRING");
  FD_ADD_TO_CHOICE(xtime_keys,isostring_symbol);
  iso8601_symbol=fd_intern("ISO8601");
  FD_ADD_TO_CHOICE(xtime_keys,iso8601_symbol);
  rfc822_symbol=fd_intern("RFC822");
  FD_ADD_TO_CHOICE(xtime_keys,rfc822_symbol);
  
  time_of_day_symbol=fd_intern("TIME-OF-DAY");
  FD_ADD_TO_CHOICE(xtime_keys,time_of_day_symbol);
  morning_symbol=fd_intern("MORNING");
  afternoon_symbol=fd_intern("AFTERNOON");
  evening_symbol=fd_intern("EVENING");
  nighttime_symbol=fd_intern("NIGHTTIME");

  season_symbol=fd_intern("SEASON");
  FD_ADD_TO_CHOICE(xtime_keys,season_symbol);
  spring_symbol=fd_intern("SPRING");
  summer_symbol=fd_intern("SUMMER");
  autumn_symbol=fd_intern("AUTUMN");
  winter_symbol=fd_intern("WINTER");

  shortmonth_symbol=fd_intern("MONTH-SHORT");
  FD_ADD_TO_CHOICE(xtime_keys,shortmonth_symbol);
  longmonth_symbol=fd_intern("MONTH-LONG");
  FD_ADD_TO_CHOICE(xtime_keys,longmonth_symbol);
  shortday_symbol=fd_intern("WEEKDAY-SHORT");
  FD_ADD_TO_CHOICE(xtime_keys,shortday_symbol);
  longday_symbol=fd_intern("WEEKDAY-LONG");
  FD_ADD_TO_CHOICE(xtime_keys,longday_symbol);
  hms_symbol=fd_intern("HMS");
  FD_ADD_TO_CHOICE(xtime_keys,hms_symbol);
  dmy_symbol=fd_intern("DMY");
  FD_ADD_TO_CHOICE(xtime_keys,dmy_symbol);
  dm_symbol=fd_intern("DM");
  FD_ADD_TO_CHOICE(xtime_keys,dm_symbol);
  my_symbol=fd_intern("MY");
  FD_ADD_TO_CHOICE(xtime_keys,my_symbol);
  string_symbol=fd_intern("STRING");
  FD_ADD_TO_CHOICE(xtime_keys,string_symbol);
  shortstring_symbol=fd_intern("SHORTSTRING");
  FD_ADD_TO_CHOICE(xtime_keys,shortstring_symbol);
  timestring_symbol=fd_intern("TIMESTRING");
  FD_ADD_TO_CHOICE(xtime_keys,timestring_symbol);
  datestring_symbol=fd_intern("DATESTRING");
  FD_ADD_TO_CHOICE(xtime_keys,datestring_symbol);
  fullstring_symbol=fd_intern("FULLSTRING");
  FD_ADD_TO_CHOICE(xtime_keys,fullstring_symbol);
  
  dowid_symbol=fd_intern("DOWID");
  FD_ADD_TO_CHOICE(xtime_keys,dowid_symbol);
  monthid_symbol=fd_intern("MONTHID");
  FD_ADD_TO_CHOICE(xtime_keys,monthid_symbol);
  
  timezone_symbol=fd_intern("TIMEZONE");
  FD_ADD_TO_CHOICE(xtime_keys,timezone_symbol);
  
  gmt_symbol=fd_intern("GMT");

  data_symbol=fd_intern("DATA");
  stack_symbol=fd_intern("STACK");
  shared_symbol=fd_intern("SHARED");
  resident_symbol=fd_intern("RESIDENT");
  utime_symbol=fd_intern("UTIME");
  stime_symbol=fd_intern("STIME");
  clock_symbol=fd_intern("CLOCK");
  cpusage_symbol=fd_intern("CPUSAGE");

  fd_idefn(fd_scheme_module,fd_make_cprim1("TIMESTAMP?",timestampp,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("GMTIMESTAMP",gmtimestamp_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TIMESTAMP",timestamp_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ELAPSED-TIME",elapsed_time,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("TIMESTRING",timestring,0));

  fd_idefn(fd_scheme_module,fd_make_cprim3("MODTIME",modtime_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("TIMESTAMP+",timestamp_plus,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("DIFFTIME",timestamp_diff,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2("PAST-TIME?",timestamp_earlier,1));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2("FUTURE-TIME?",timestamp_later,1));
  fd_defalias(fd_scheme_module,"TIME-EARLIER?","PAST-TIME?");
  fd_defalias(fd_scheme_module,"TIME-LATER?","FUTURE-TIME?");

#if ((HAVE_SLEEP) || (HAVE_NANOSLEEP))
  fd_idefn(fd_scheme_module,fd_make_cprim1("SLEEP",sleep_prim,1));
#endif

  fd_idefn(fd_scheme_module,fd_make_cprim0("TIME",time_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("MILLITIME",millitime_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("MICROTIME",microtime_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("UUID?",uuidp_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim2("GETUUID",getuuid_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("UUID-TIME",uuidtime_prim,1,
					    fd_uuid_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("UUID-NODE",uuidnode_prim,1,
					    fd_uuid_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("UUID->STRING",uuidstring_prim,1,
					    fd_uuid_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("UUID->PACKET",uuidpacket_prim,1,
					    fd_uuid_type,FD_VOID));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("SECS->STRING",secs2string,1,
			   -1,FD_VOID,-1,FD_FALSE));

  fd_idefn(fd_scheme_module,fd_make_cprim0("GETHOSTNAME",hostname_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("HOSTADDRS",hostaddrs_prim,0));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("GETENV",getenv_prim,1,
			   fd_string_type,FD_VOID));
  
  fd_idefn(fd_scheme_module,fd_make_cprim1("RUSAGE",rusage_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("MEMUSAGE",memusage_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("USERTIME",usertime_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("SYSTIME",systime_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CPUSAGE",cpusage_prim,0));

  fd_idefn(fd_scheme_module,fd_make_cprim0("CT/SENSORS",calltrack_sensors,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CT/SENSE",calltrack_sense,0));

  /* Initialize utime and stime sensors */
#if FD_CALLTRACK_ENABLED
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("UTIME",1);
    cts->enabled=0; cts->dblfcn=utime_sensor;}
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("STIME",1);
    cts->enabled=0; cts->dblfcn=stime_sensor;}
#if HAVE_STRUCT_RUSAGE_RU_INBLOCK
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("INBLOCK",1);
    cts->enabled=0; cts->intfcn=inblock_sensor;}
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("OUTBLOCK",1);
    cts->enabled=0; cts->intfcn=outblock_sensor;}
#endif
#if HAVE_STRUCT_RUSAGE_RU_MAJFLT
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("MAJFLT",1);
    cts->enabled=0; cts->intfcn=majflt_sensor;}
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("NSWAPS",1);
    cts->enabled=0; cts->intfcn=nswaps_sensor;}
#endif
#if HAVE_STRUCT_RUSAGE_RU_NVCSW
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("SWITCHES",1);
    cts->enabled=0; cts->intfcn=cxtswitch_sensor;}
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("VSWITCHES",1);
    cts->enabled=0; cts->intfcn=vcxtswitch_sensor;}
  {
    fd_calltrack_sensor cts=fd_get_calltrack_sensor("IVSWITCHES",1);
    cts->enabled=0; cts->intfcn=ivcxtswitch_sensor;}
#endif
#endif

}


