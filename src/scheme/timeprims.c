/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2007 beingmeta, inc.
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
static fdtype tick_symbol, xtick_symbol;
static fdtype iso_symbol, isostring_symbol, iso8601_symbol, rfc822_symbol;
static fdtype time_of_day_symbol, dowid_symbol, monthid_symbol;
static fdtype shortmonth_symbol, longmonth_symbol, shortday_symbol, longday_symbol;
static fdtype hms_symbol, dmy_symbol, dm_symbol, string_symbol, shortstring_symbol;
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
    int tv=fd_bigint2int((fd_bigint)arg);
    u8_local_xtime(&(tm->xtime),(time_t)tv,u8_second,-1);
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
    u8_init_xtime(&(tm->xtime),-1,u8_femtosecond,0,0);
    return FDTYPE_CONS(tm);}
  else if (FD_STRINGP(arg)) {
    u8_string sdata=FD_STRDATA(arg); int c=*sdata; time_t moment;
    if (u8_isdigit(c))
      u8_iso8601_to_xtime(sdata,&(tm->xtime));
    else u8_rfc822_to_xtime(sdata,&(tm->xtime));
    moment=u8_mktime(&(tm->xtime));
    u8_offtime(&(tm->xtime),moment,0);
    return FDTYPE_CONS(tm);}
  else if (FD_SYMBOLP(arg)) {
    enum u8_timestamp_precision prec=get_precision(arg);
    if (((int)prec)<0)
      return fd_type_error("timestamp precision","timestamp_prim",arg);
    u8_init_xtime(&(tm->xtime),-1,prec,-1,0);
    return FDTYPE_CONS(tm);}
  else if (FD_FIXNUMP(arg)) {
    u8_init_xtime(&(tm->xtime),(time_t)(FD_FIX2INT(arg)),u8_second,-1,0);
    return FDTYPE_CONS(tm);}
  else if (FD_PTR_TYPEP(arg,fd_bigint_type)) {
    int tv=fd_bigint2int((fd_bigint)arg);
    u8_init_xtime(&(tm->xtime),(time_t)tv,u8_second,-1,0);
    return FDTYPE_CONS(tm);}
  else if (FD_PTR_TYPEP(arg,fd_double_type)) {
    double dv=FD_FLONUM(arg);
    double dsecs=floor(dv), dnsecs=(dv-dsecs)*1000000000;
    unsigned int secs=(unsigned int)dsecs, nsecs=(unsigned int)dnsecs;
    u8_init_xtime(&(tm->xtime),(time_t)secs,u8_second,nsecs,0);
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
  struct FD_TIMESTAMP *newtm=u8_alloc(struct FD_TIMESTAMP), *oldtm;
  memset(newtm,0,sizeof(struct FD_TIMESTAMP));
  if (FD_VOIDP(arg2)) {
    if ((FD_FIXNUMP(arg1)) || (FD_FLONUMP(arg1)) || (FD_RATIONALP(arg1)))
      delta=fd_todouble(arg1);
    else return fd_type_error("number","timestamp_plus",arg1);
    u8_init_xtime(&tmp,-1,u8_femtosecond,-1,0);
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
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,fullstring_symbol)) 
    if (xt->u8_prec>=u8_day) 
      return use_strftime("%A %d %B %Y %r %Z",xt);
    else if (xt->u8_prec==u8_day)
      return use_strftime("%A %d %B %Y %Z",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if ((FD_EQ(slotid,iso_symbol)) ||
	   (FD_EQ(slotid,isostring_symbol)) ||
	   (FD_EQ(slotid,iso8601_symbol))) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_iso8601(&out,xt);
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else if (FD_EQ(slotid,rfc822_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_rfc822(&out,xt);
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else if (FD_EQ(slotid,gmt_symbol))
    if (xt->u8_tzoff==0) 
      return fd_make_timestamp(xt);
    else {
      struct U8_XTIME asgmt;
      u8_init_xtime(&asgmt,xt->u8_secs,xt->u8_prec,xt->u8_nsecs,0);
      return fd_make_timestamp(&asgmt);}
  else if (FD_EQ(slotid,month_symbol))
    if (xt->u8_prec>=u8_month)
      return FD_INT2DTYPE(xt->u8_tptr.tm_mon);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,monthid_symbol))
    if (xt->u8_prec>=u8_month)
      if ((xt->u8_tptr.tm_mon>=0) &&
	  (xt->u8_tptr.tm_mon<12))
	return monthids[xt->u8_tptr.tm_mon];
      else if (reterr)
	return fd_err(fd_InvalidTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
      else return FD_EMPTY_CHOICE;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,shortmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%b",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,longmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%B",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,dowid_symbol))
    if (xt->u8_prec>u8_month)
      if ((xt->u8_tptr.tm_wday>=0) && (xt->u8_tptr.tm_wday<7))
	return dowids[xt->u8_tptr.tm_wday];
      else if (reterr)
	return fd_err(fd_InvalidTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
      else return FD_EMPTY_CHOICE;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,shortday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%a",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,longday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%A",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,hms_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%T",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,timestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%X",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,string_symbol))
    if (xt->u8_prec>=u8_second)
      return use_strftime("%c",xt);
	else if (reterr)
	  return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,shortstring_symbol))
    if (xt->u8_prec>=u8_second)
      return use_strftime("%d %b %Y %r",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,datestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%x",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,dmy_symbol))
    if (xt->u8_prec>=u8_hour) {
      if ((xt->u8_tptr.tm_mon>=0) && (xt->u8_tptr.tm_mon<11)) {
	char buf[64];
	sprintf(buf,"%d%s%04d",xt->u8_tptr.tm_mday,
		month_names[xt->u8_tptr.tm_mon],xt->u8_tptr.tm_year+1900);
	return fd_init_string(NULL,-1,u8_strdup(buf));}
      else if (reterr)
	return fd_err(fd_InvalidTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
      else return FD_EMPTY_CHOICE;}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,dm_symbol))
    if (xt->u8_prec>=u8_hour)
      if ((xt->u8_tptr.tm_mon>=0) && (xt->u8_tptr.tm_mon<11)) {
	char buf[64];
	sprintf(buf,"%d%s",xt->u8_tptr.tm_mday,month_names[xt->u8_tptr.tm_mon]);
	return fd_init_string(NULL,-1,u8_strdup(buf));}
      else if (reterr)
	return fd_err(fd_InvalidTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
      else return FD_EMPTY_CHOICE;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,date_symbol))
    if (xt->u8_prec>=u8_day)
      return FD_INT2DTYPE(xt->u8_tptr.tm_mday);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,hours_symbol))
    if (xt->u8_prec>=u8_hour)
      return FD_INT2DTYPE(xt->u8_tptr.tm_hour);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,minutes_symbol))
    if (xt->u8_prec>=u8_minute)
      return FD_INT2DTYPE(xt->u8_tptr.tm_min);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,seconds_symbol))
    if (xt->u8_prec>=u8_second)
      return FD_INT2DTYPE(xt->u8_tptr.tm_sec);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,tzoff_symbol))
    return FD_INT2DTYPE(xt->u8_tzoff);
  else if (FD_EQ(slotid,tick_symbol))
    if (xt->u8_prec>=u8_second) {
      time_t tick=xt->u8_secs;
      return FD_INT2DTYPE((long)tick);}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
		    FD_SYMBOL_NAME(slotid),FD_VOID);
    else return FD_EMPTY_CHOICE;
  else if (FD_EQ(slotid,xtick_symbol))
    if (xt->u8_prec>=u8_second) {
      double dsecs=(double)(xt->u8_secs), dnsecs=(double)(xt->u8_nsecs);
      return fd_init_double(NULL,dsecs+(dnsecs/1000000000.0));}
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
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
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
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",FD_SYMBOL_NAME(slotid),FD_VOID);
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
	(FD_FIX2INT(value)>=0) && (FD_FIX2INT(value)<32))
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
  else if (FD_EQ(slotid,timezone_symbol))
    if (FD_STRINGP(value)) {
      int tz=u8_parse_tzspec(FD_STRDATA(value),xt->u8_tzoff);
#if HAVE_TM_GMTOFF
      xt->u8_tptr.tm_gmtoff=tz;
      xt->u8_tptr.tm_zone=NULL;
#else
      xt->u8_tzoff=tz;
#endif
    }
    else if (FD_FIXNUMP(value)) {
      int offset=0;
      if ((FD_FIX2INT(value)>=-12) && (FD_FIX2INT(value)<=12))
	offset=3600*FD_FIX2INT(value);
      else offset=FD_FIX2INT(value);
#if HAVE_TM_GMTOFF
      xt->u8_tptr.tm_gmtoff=offset;
#else
      xt->u8_tzoff=offset;
#endif
    }
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("seconds")),value);
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
    fdtype keys=fd_getkeys(slotmap); 
    FD_DO_CHOICES(key,keys) {
      fdtype val=fd_get(slotmap,key,FD_VOID);
      if (xtime_set(xt,key,val)<0) {
	result=FD_ERROR_VALUE; FD_STOP_DO_CHOICES; break;}
      else {}}
    if (FD_ABORTP(result)) return result;
    else if (FD_FALSEP(togmt)) {
      u8_mktime(xt);
      return result;}
    else {
      time_t moment=u8_mktime(xt);
      u8_init_xtime(xt,moment,xt->u8_prec,xt->u8_nsecs,0);
      return result;}}
}      

/* Miscellanous time utilities */

static fdtype timestring()
{
  struct U8_XTIME onstack; struct U8_OUTPUT out;
  u8_local_xtime(&onstack,-1,u8_second,0);
  U8_INIT_OUTPUT(&out,16);
  u8_printf(&out,"%02d:%02d:%02d",
	    onstack.u8_tptr.tm_hour,
	    onstack.u8_tptr.tm_min,
	    onstack.u8_tptr.tm_sec);
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
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

static fdtype secs2string(fdtype secs,fdtype inexact_arg)
{
  struct U8_OUTPUT out;
  int inexact=(!(FD_FALSEP(inexact_arg)));
  double seconds;
  int weeks, days, hours, minutes, need_comma=0;
  if (FD_FIXNUMP(secs)) 
    seconds=(double)FD_FIX2INT(secs);
  else if (FD_FLONUMP(secs))
    seconds=FD_FLONUM(secs);
  else return fd_type_error(_("seconds"),"secs2string",secs);
  U8_INIT_OUTPUT(&out,64);
  if (seconds<0) {
    u8_printf(&out,"negative "); seconds=-seconds;}
  weeks=(int)floor(seconds/(3600*24*7));
  seconds=seconds-weeks*(3600*24*7);
  days=(int)floor(seconds/(3600*24));
  seconds=seconds-days*(3600*24);
  hours=(int)floor(seconds/(3600));
  seconds=seconds-hours*(3600);
  minutes=floor(seconds/60);
  seconds=seconds-minutes*60;

  if (weeks>1) {
    u8_printf(&out,_("%d weeks"),weeks);
    need_comma=1;}
  else if (weeks==1) {
    u8_printf(&out,_("one week"));
    need_comma=1;}

  if (days>1) {
    if (need_comma) u8_puts(&out,", ");
    u8_printf(&out,_("%d days"),days);
    need_comma=1;}
  else if (days==1) {
    if (need_comma) u8_puts(&out,", ");
    u8_printf(&out,_("one day"));
    need_comma=1;}

  if ((inexact) && (weeks>0))
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);

  if (hours>1) {
    if (need_comma) u8_puts(&out,", ");
    u8_printf(&out,_("%d hours"),hours);
    need_comma=1;}
  else if (hours==1) {
    if (need_comma) u8_puts(&out,", ");
    u8_printf(&out,_("one hour"));
    need_comma=1;}

  if ((inexact) && (days>0))
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);

  if (minutes>1) {
    if (need_comma) u8_puts(&out,", ");
    u8_printf(&out,_("%d minutes"),minutes);
    need_comma=1;}
  else if (minutes==1) {
    if (need_comma) u8_puts(&out,", ");
    u8_printf(&out,_("one minute"));
    need_comma=1;}

  if ((inexact) && (hours>0))
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);

  if (seconds==0) {}
  else if ((inexact) && (FD_FLONUMP(secs)) && (seconds>1)) {
    if (need_comma) u8_puts(&out,", ");
    u8_printf(&out,_("%d seconds"),(int)floor(seconds));}
  else if (FD_FLONUMP(secs)) {
    if (need_comma) u8_puts(&out,", ");
    u8_printf(&out,_("%f seconds"),seconds);}
  else {
    if (need_comma) u8_puts(&out,", ");
    u8_printf(&out,_("%d seconds"),(int)floor(seconds));}
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
  fd_tablefns[fd_timestamp_type]->keys=NULL;

  year_symbol=fd_intern("YEAR");
  month_symbol=fd_intern("MONTH");
  date_symbol=fd_intern("DATE");
  hours_symbol=fd_intern("HOURS");
  minutes_symbol=fd_intern("MINUTES");
  seconds_symbol=fd_intern("SECONDS");
  precision_symbol=fd_intern("PRECISION");
  tzoff_symbol=fd_intern("TZOFF");

  milliseconds_symbol=fd_intern("MILLISECONDS");
  microseconds_symbol=fd_intern("MICROSECONDS");
  nanoseconds_symbol=fd_intern("NANOSECONDS");
  picoseconds_symbol=fd_intern("PICOSECONDS");
  femtoseconds_symbol=fd_intern("FEMTOSECONDS");

  tick_symbol=fd_intern("TICK");
  xtick_symbol=fd_intern("XTICK");
  iso_symbol=fd_intern("ISO");
  isostring_symbol=fd_intern("ISOSTRING");
  iso8601_symbol=fd_intern("ISO8601");
  rfc822_symbol=fd_intern("RFC822");

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
  dmy_symbol=fd_intern("DMY");
  dm_symbol=fd_intern("DM");
  string_symbol=fd_intern("STRING");
  shortstring_symbol=fd_intern("SHORTSTRING");
  timestring_symbol=fd_intern("TIMESTRING");
  datestring_symbol=fd_intern("DATESTRING");
  fullstring_symbol=fd_intern("FULLSTRING");

  dowid_symbol=fd_intern("DOWID");
  monthid_symbol=fd_intern("MONTHID");

  timezone_symbol=fd_intern("TIMEZONE");

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

  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("SECS->STRING",secs2string,1,
			   -1,FD_VOID,-1,FD_FALSE));

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


