/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define FD_PROVIDE_FASTEVAL 1 */

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8netfns.h>

#include <ctype.h>
#include <math.h>
#include <sys/time.h>

#if ((HAVE_SYS_UTSNAME_H)&&(HAVE_UNAME))
#include <sys/utsname.h>
#endif

u8_condition fd_ImpreciseTimestamp=_("Timestamp too imprecise");
u8_condition fd_InvalidTimestamp=_("Invalid timestamp object");
static u8_condition strftime_error=_("internal strftime error");

static lispval year_symbol, month_symbol, date_symbol;
static lispval hours_symbol, minutes_symbol, seconds_symbol;
static lispval milliseconds_symbol, microseconds_symbol, nanoseconds_symbol;
static lispval picoseconds_symbol, femtoseconds_symbol;

static lispval years_symbol, months_symbol, day_symbol, days_symbol;
static lispval hour_symbol, minute_symbol, second_symbol;
static lispval millisecond_symbol, microsecond_symbol, nanosecond_symbol;
static lispval picosecond_symbol, femtosecond_symbol;

static lispval precision_symbol, tzoff_symbol, dstoff_symbol, gmtoff_symbol;
static lispval spring_symbol, summer_symbol, autumn_symbol, winter_symbol;
static lispval season_symbol, gmt_symbol, timezone_symbol;
static lispval morning_symbol, afternoon_symbol;
static lispval  evening_symbol, nighttime_symbol;
static lispval tick_symbol, xtick_symbol, prim_tick_symbol;
static lispval iso_symbol, isostring_symbol, iso8601_symbol;
static lispval isodate_symbol, isobasic_symbol, isobasicdate_symbol;
static lispval rfc822_symbol, rfc822date_symbol, rfc822x_symbol;
static lispval localstring_symbol;
static lispval time_of_day_symbol, dowid_symbol, monthid_symbol;
static lispval shortmonth_symbol, longmonth_symbol;
static lispval  shortday_symbol, longday_symbol;
static lispval hms_symbol, dmy_symbol, dm_symbol, my_symbol;
static lispval shortstring_symbol, short_symbol;
static lispval string_symbol, fullstring_symbol;
static lispval timestring_symbol, datestring_symbol;

static int get_precision(lispval sym)
{
  if ( (FD_EQ(sym,year_symbol)) || (FD_EQ(sym,years_symbol)) )
    return (int) u8_year;
  else if ( (FD_EQ(sym,month_symbol)) || (FD_EQ(sym,months_symbol)) )
    return (int) u8_month;
  else if ( (FD_EQ(sym,date_symbol)) || (FD_EQ(sym,day_symbol)) ||
            (FD_EQ(sym,days_symbol)) )
    return (int) u8_day;
  else if ( (FD_EQ(sym,hours_symbol)) || (FD_EQ(sym,hour_symbol)) )
    return (int) u8_hour;
  else if ( (FD_EQ(sym,minutes_symbol)) || (FD_EQ(sym,minute_symbol)) )
    return (int ) u8_minute;
  else if ( (FD_EQ(sym,seconds_symbol)) || (FD_EQ(sym,second_symbol)) )
    return (int) u8_second;
  else if ( (FD_EQ(sym,milliseconds_symbol)) || (FD_EQ(sym,millisecond_symbol)) )
    return (int) u8_millisecond;
  else if ( (FD_EQ(sym,microseconds_symbol)) || (FD_EQ(sym,microsecond_symbol)) )
    return (int) u8_microsecond;
  else if ( (FD_EQ(sym,nanoseconds_symbol)) || (FD_EQ(sym,nanosecond_symbol)) )
    return (int) u8_nanosecond;
  else if ( (FD_EQ(sym,picoseconds_symbol)) || (FD_EQ(sym,picosecond_symbol)) )
    return (int) u8_picosecond;
  else if ( (FD_EQ(sym,femtoseconds_symbol)) || (FD_EQ(sym,femtosecond_symbol)) )
    return (int) u8_femtosecond;
  else return -1;
}

static lispval timestampp(lispval arg)
{
  if (TYPEP(arg,fd_timestamp_type))
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval timestamp_prim(lispval arg)
{
  struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
  memset(tm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tm,fd_timestamp_type);
  if (VOIDP(arg)) {
    u8_local_xtime(&(tm->u8xtimeval),-1,u8_nanosecond,0);
    return LISP_CONS(tm);}
  else if (STRINGP(arg)) {
    u8_string sdata = CSTRING(arg);
    int c = *sdata;
    if (u8_isdigit(c))
      u8_iso8601_to_xtime(sdata,&(tm->u8xtimeval));
    else u8_rfc822_to_xtime(sdata,&(tm->u8xtimeval));
    return LISP_CONS(tm);}
  else if (SYMBOLP(arg)) {
    int prec_val = get_precision(arg);
    enum u8_timestamp_precision prec;
    if (prec_val<0)
      return fd_type_error("timestamp precision","timestamp_prim",arg);
    else prec=prec_val;
    u8_local_xtime(&(tm->u8xtimeval),-1,prec,-1);
    return LISP_CONS(tm);}
  else if (FIXNUMP(arg)) {
    u8_local_xtime(&(tm->u8xtimeval),(time_t)(FIX2INT(arg)),u8_second,-1);
    return LISP_CONS(tm);}
  else if (TYPEP(arg,fd_timestamp_type)) {
    struct FD_TIMESTAMP *fdt = (struct FD_TIMESTAMP *)arg;
    u8_local_xtime(&(tm->u8xtimeval),
                   fdt->u8xtimeval.u8_tick,fdt->u8xtimeval.u8_prec,
                   fdt->u8xtimeval.u8_nsecs);
    return LISP_CONS(tm);}
  else if (FD_BIGINTP(arg)) {
#if (SIZEOF_TIME_T == 8)
    time_t tv = (time_t)fd_bigint_to_long_long((fd_bigint)arg);
#else
    time_t tv = (time_t)fd_bigint_to_long((fd_bigint)arg);
#endif
    u8_local_xtime(&(tm->u8xtimeval),tv,u8_second,-1);
    return LISP_CONS(tm);}
  else if (FD_FLONUMP(arg)) {
    double dv = FD_FLONUM(arg);
    double dsecs = floor(dv), dnsecs = (dv-dsecs)*1000000000;
    unsigned int secs = (unsigned int)dsecs, nsecs = (unsigned int)dnsecs;
    u8_local_xtime(&(tm->u8xtimeval),(time_t)secs,u8_second,nsecs);
    return LISP_CONS(tm);}
  else {
    u8_free(tm);
    return fd_type_error("timestamp arg","timestamp_prim",arg);}
}

static lispval gmtimestamp_prim(lispval arg)
{
  struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
  memset(tm,0,sizeof(struct FD_TIMESTAMP));
  FD_INIT_CONS(tm,fd_timestamp_type);
  if (VOIDP(arg)) {
    u8_init_xtime(&(tm->u8xtimeval),-1,u8_nanosecond,0,0,0);
    return LISP_CONS(tm);}
  else if (TYPEP(arg,fd_timestamp_type)) {
    struct FD_TIMESTAMP *ftm = fd_consptr(fd_timestamp,arg,fd_timestamp_type);
    if ((ftm->u8xtimeval.u8_tzoff==0)&&(ftm->u8xtimeval.u8_dstoff==0)) {
      u8_free(tm); return fd_incref(arg);}
    else {
      time_t tick = ftm->u8xtimeval.u8_tick;
      if (ftm->u8xtimeval.u8_prec>u8_second)
        u8_init_xtime(&(tm->u8xtimeval),tick,ftm->u8xtimeval.u8_prec,
                      ftm->u8xtimeval.u8_nsecs,0,0);
      else u8_init_xtime(&(tm->u8xtimeval),tick,ftm->u8xtimeval.u8_prec,0,0,0);
      return LISP_CONS(tm);}}
  else if (STRINGP(arg)) {
    u8_string sdata = CSTRING(arg);
    int c = *sdata; time_t moment;
    if (u8_isdigit(c))
      u8_iso8601_to_xtime(sdata,&(tm->u8xtimeval));
    else u8_rfc822_to_xtime(sdata,&(tm->u8xtimeval));
    moment = u8_mktime(&(tm->u8xtimeval));
    if (moment<0) {
      u8_free(tm); return FD_ERROR;}
    if ((tm->u8xtimeval.u8_tzoff!=0)||(tm->u8xtimeval.u8_dstoff!=0))
      u8_init_xtime(&(tm->u8xtimeval),moment,tm->u8xtimeval.u8_prec,
                    tm->u8xtimeval.u8_nsecs,0,0);
    return LISP_CONS(tm);}
  else if (SYMBOLP(arg)) {
    int prec_val = get_precision(arg);
    enum u8_timestamp_precision prec;
    if (prec_val<0)
      return fd_type_error("timestamp precision","timestamp_prim",arg);
    else prec = prec_val;
    u8_init_xtime(&(tm->u8xtimeval),-1,prec,-1,0,0);
    return LISP_CONS(tm);}
  else if (FIXNUMP(arg)) {
    u8_init_xtime(&(tm->u8xtimeval),(time_t)(FIX2INT(arg)),u8_second,-1,0,0);
    return LISP_CONS(tm);}
  else if (FD_BIGINTP(arg)) {
#if (SIZEOF_TIME_T == 8)
    time_t tv = (time_t)fd_bigint_to_long_long((fd_bigint)arg);
#else
    time_t tv = (time_t)fd_bigint_to_long((fd_bigint)arg);
#endif
    u8_init_xtime(&(tm->u8xtimeval),tv,u8_second,-1,0,0);
    return LISP_CONS(tm);}
  else if (FD_FLONUMP(arg)) {
    double dv = FD_FLONUM(arg);
    double dsecs = floor(dv), dnsecs = (dv-dsecs)*1000000000;
    unsigned int secs = (unsigned int)dsecs, nsecs = (unsigned int)dnsecs;
    u8_init_xtime
      (&(tm->u8xtimeval),(time_t)secs,u8_second,nsecs,0,0);
    return LISP_CONS(tm);}
  else {
    u8_free(tm);
    return fd_type_error("timestamp arg","timestamp_prim",arg);}
}

static struct FD_TIMESTAMP *get_timestamp(lispval arg,int *freeit)
{
  if (TYPEP(arg,fd_timestamp_type)) {
    *freeit = 0;
    return fd_consptr(struct FD_TIMESTAMP *,arg,fd_timestamp_type);}
  else if (STRINGP(arg)) {
    struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    u8_iso8601_to_xtime(CSTRING(arg),&(tm->u8xtimeval)); *freeit = 1;
    return tm;}
  else if ((FIXNUMP(arg))||
           ((FD_BIGINTP(arg))&&
            (fd_modest_bigintp((fd_bigint)arg)))) {
    struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
    long long int tick;
    if (FIXNUMP(arg)) tick = FIX2INT(arg);
    else tick = fd_bigint_to_long_long((fd_bigint)arg);
    memset(tm,0,sizeof(struct FD_TIMESTAMP)); *freeit = 1;
    if (tick<31536000L) {
      u8_now(&(tm->u8xtimeval));
      u8_xtime_plus(&(tm->u8xtimeval),FIX2INT(arg));}
    else u8_init_xtime(&(tm->u8xtimeval),tick,u8_second,0,0,0);
    return tm;}
  else if (VOIDP(arg)) {
    struct FD_TIMESTAMP *tm = u8_alloc(struct FD_TIMESTAMP);
    memset(tm,0,sizeof(struct FD_TIMESTAMP));
    u8_now(&(tm->u8xtimeval)); *freeit = 1;
    return tm;}
  else {
    fd_set_type_error("timestamp",arg); *freeit = 0;
    return NULL;}
}

static lispval timestamp_plus_helper(lispval arg1,lispval arg2,int neg)
{
  double delta; int free_old = 0;
  struct U8_XTIME tmp, *btime;
  struct FD_TIMESTAMP *newtm = u8_alloc(struct FD_TIMESTAMP), *oldtm = NULL;
  memset(newtm,0,sizeof(struct FD_TIMESTAMP));
  if (VOIDP(arg2)) {
    if ((FIXNUMP(arg1)) || (FD_FLONUMP(arg1)) || (FD_RATIONALP(arg1)))
      delta = fd_todouble(arg1);
    else {
      u8_free(newtm);
      return fd_type_error("number","timestamp_plus",arg1);}
    u8_init_xtime(&tmp,-1,u8_nanosecond,-1,0,0);
    btime = &tmp;}
  else if ((FIXNUMP(arg2)) || (FD_FLONUMP(arg2)) || (FD_RATIONALP(arg2))) {
    delta = fd_todouble(arg2);
    oldtm = get_timestamp(arg1,&free_old);
    btime = &(oldtm->u8xtimeval);}
  else return fd_type_error("number","timestamp_plus",arg2);
  if (neg) delta = -delta;
  /* Init the cons bit field */
  FD_INIT_CONS(newtm,fd_timestamp_type);
  /* Copy the data */
  memcpy(&(newtm->u8xtimeval),btime,sizeof(struct U8_XTIME));
  u8_xtime_plus(&(newtm->u8xtimeval),delta);
  if (free_old) u8_free(oldtm);
  return LISP_CONS(newtm);
}

static lispval timestamp_plus(lispval arg1,lispval arg2)
{
  return timestamp_plus_helper(arg1,arg2,0);
}

static lispval timestamp_minus(lispval arg1,lispval arg2)
{
  return timestamp_plus_helper(arg1,arg2,1);
}

static lispval timestamp_diff(lispval timestamp1,lispval timestamp2)
{
  if ((FD_FLONUMP(timestamp1))&&(VOIDP(timestamp2))) {
    double then = FD_FLONUM(timestamp1);
    double now = u8_elapsed_time();
    double diff = now-then;
    return fd_make_flonum(diff);}
  else if ((FD_FLONUMP(timestamp1))&&(FD_FLONUMP(timestamp2))) {
    double t1 = FD_FLONUM(timestamp1);
    double t2 = FD_FLONUM(timestamp2);
    double diff = t1-t2;
    return fd_make_flonum(diff);}
  else {
    int free1 = 0, free2 = 0;
    struct FD_TIMESTAMP *t1 = get_timestamp(timestamp1,&free1);
    struct FD_TIMESTAMP *t2 = get_timestamp(timestamp2,&free2);
    if ((t1 == NULL) || (t2 == NULL)) {
      if (free1) u8_free(t1); if (free2) u8_free(t2);
      return FD_ERROR;}
    else {
      double diff = u8_xtime_diff(&(t1->u8xtimeval),&(t2->u8xtimeval));
      if (free1) u8_free(t1);
      if (free2) u8_free(t2);
      return fd_init_double(NULL,diff);}}
}

static lispval time_until(lispval arg)
{
  return timestamp_diff(arg,VOID);
}
static lispval time_since(lispval arg)
{
  return timestamp_diff(VOID,arg);
}

static lispval timestamp_greater(lispval timestamp1,lispval timestamp2)
{
  int free1 = 0;
  struct FD_TIMESTAMP *t1 = get_timestamp(timestamp1,&free1);
  if (t1==NULL)
    return FD_ERROR;
  else if (VOIDP(timestamp2)) {
    double diff;
    struct U8_XTIME xtime; u8_now(&xtime);
    diff = u8_xtime_diff(&xtime,&(t1->u8xtimeval));
    if (free1) u8_free(t1);
    if (diff>0)
      return FD_TRUE;
    else return FD_FALSE;}
  else {
    double diff; int free2 = 0;
    struct FD_TIMESTAMP *t2 = get_timestamp(timestamp2,&free2);
    if (t2 == NULL) {
      if (free1) u8_free(t1);
      if (free2) u8_free(t2);
      return FD_ERROR;}
    else diff = u8_xtime_diff(&(t1->u8xtimeval),&(t2->u8xtimeval));
    if (diff>0)
      return FD_TRUE;
    else return FD_FALSE;}
}

static lispval timestamp_lesser(lispval timestamp1,lispval timestamp2)
{
  int free1 = 0;
  struct FD_TIMESTAMP *t1 = get_timestamp(timestamp1,&free1);
  if (t1==NULL)
    return FD_ERROR;
  else if (VOIDP(timestamp2)) {
    double diff;
    struct U8_XTIME xtime; u8_now(&xtime);
    diff = u8_xtime_diff(&xtime,&(t1->u8xtimeval));
    if (free1) u8_free(t1);
    if (diff<0)
      return FD_TRUE;
    else return FD_FALSE;}
  else {
    double diff; int free2 = 0;
    struct FD_TIMESTAMP *t2 = get_timestamp(timestamp2,&free2);
    if (t2 == NULL) {
      if (free1) u8_free(t1);
      if (free2) u8_free(t2);
      return FD_ERROR;}
    else diff = u8_xtime_diff(&(t1->u8xtimeval),&(t2->u8xtimeval));
    if (diff<0)
      return FD_TRUE;
    else return FD_FALSE;}
}

FD_EXPORT
int fd_cmp_now(lispval timestamp,double thresh)
{
  int free_t = 0;
  struct FD_TIMESTAMP *t = get_timestamp(timestamp,&free_t);
  if (t == NULL)
    return FD_ERROR;
  else {
    double diff; struct U8_XTIME now; u8_now(&now);
    diff = u8_xtime_diff((&(t->u8xtimeval)),&now);
    if (free_t) u8_free(t);
    if (diff > thresh)
      return 1;
    else if (diff < (-thresh))
      return -1;
    else return 0;}
}

static lispval futurep(lispval timestamp,lispval thresh_arg)
{
  int thresh = (FD_FLONUMP(thresh_arg)) ?
    (FD_FLONUM(thresh_arg)) :
    (0.0);
  if (fd_cmp_now(timestamp,thresh)>0)
    return FD_TRUE;
  else return FD_FALSE;
}

static lispval pastp(lispval timestamp,lispval thresh_arg)
{
  int thresh = (FD_FLONUMP(thresh_arg)) ?
    (FD_FLONUM(thresh_arg)) :
    (0.0);
  if (fd_cmp_now(timestamp,thresh)<0)
    return FD_TRUE;
  else return FD_FALSE;
}

/* Lisp access */

static lispval elapsed_time(lispval arg)
{
  double elapsed = u8_elapsed_time();
  if (VOIDP(arg))
    return fd_init_double(NULL,elapsed);
  else if (FD_FLONUMP(arg)) {
    double base = FD_FLONUM(arg);
    return fd_init_double(NULL,elapsed-base);}
  else if (FD_FIXNUMP(arg)) {
    long long intval = FD_FIX2INT(arg);
    double base = (double) intval;
    return fd_init_double(NULL,elapsed-base);}
  else return fd_type_error("double","elapsed_time",arg);
}

/* Timestamps as tables */

static lispval dowids[7], monthids[12];
static u8_string month_names[12];
static lispval xtime_keys = EMPTY;

static lispval use_strftime(char *format,struct U8_XTIME *xt)
{
  char *buf = u8_malloc(256); struct tm tptr;
  u8_xtime_to_tptr(xt,&tptr);
  int n_bytes = strftime(buf,256,format,&tptr);
  if (n_bytes<0) {
    u8_free(buf);
    return fd_err(strftime_error,"use_strftime",format,VOID);}
  else return fd_init_string(NULL,n_bytes,buf);
}

static int tzvalueok(lispval value,int *off,u8_context caller)
{
  if (FIXNUMP(value)) {
    long long fixval = FIX2INT(value);
    if ((fixval<(48*3600))&&(fixval>(-48*3600))) {
      if ((fixval>=0)&&(fixval<48)) *off = fixval*3600;
      else if ((fixval<=0)&&(fixval>=(-48))) *off = fixval*3600;
      else *off = fixval;
      return 1;}}
  else if (FD_FLONUMP(value)) {
    double floatval = FD_FLONUM(value);
    int fixval = (int) floatval;
    if ((fixval<(48*3600))&&(fixval>(-48*3600))) {
      *off = fixval;
      return 1;}}
  fd_seterr(fd_TypeError,caller,"invalid timezone offset",value);
  return 0;
}

static lispval xtime_get(struct U8_XTIME *xt,lispval slotid,int reterr)
{
  if (FD_EQ(slotid,year_symbol))
    if (xt->u8_prec>=u8_year)
      return FD_INT(xt->u8_year);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,fullstring_symbol))
    if (xt->u8_prec>=u8_day)
      return use_strftime("%A %d %B %Y %r %Z",xt);
    else if (xt->u8_prec == u8_day)
      return use_strftime("%A %d %B %Y %Z",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if ((FD_EQ(slotid,iso_symbol)) ||
           (FD_EQ(slotid,isostring_symbol)) ||
           (FD_EQ(slotid,iso8601_symbol))) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_iso8601(&out,xt);
    return fd_stream2string(&out);}
  else if (FD_EQ(slotid,isodate_symbol)) {
    struct U8_XTIME newt;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    memcpy(&newt,xt,sizeof(struct U8_XTIME));
    u8_set_xtime_precision(&newt,u8_day);
    u8_xtime_to_iso8601(&out,&newt);
    return fd_stream2string(&out);}
  else if (FD_EQ(slotid,isobasic_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_iso8601_x(&out,xt,U8_ISO8601_BASIC);
    return fd_stream2string(&out);}
  else if (FD_EQ(slotid,isobasicdate_symbol)) {
    struct U8_XTIME newt;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    memcpy(&newt,xt,sizeof(struct U8_XTIME));
    u8_set_xtime_precision(&newt,u8_day);
    u8_xtime_to_iso8601_x(&out,&newt,U8_ISO8601_BASIC);
    return fd_stream2string(&out);}
  else if (FD_EQ(slotid,rfc822_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_rfc822(&out,xt);
    return fd_stream2string(&out);}
  else if (FD_EQ(slotid,rfc822date_symbol)) {
    struct U8_XTIME newt;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    memcpy(&newt,xt,sizeof(struct U8_XTIME));
    u8_set_xtime_precision(&newt,u8_day);
    u8_xtime_to_rfc822(&out,&newt);
    return fd_stream2string(&out);}
  else if (FD_EQ(slotid,rfc822x_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_rfc822_x(&out,xt,-1,0);
    return fd_stream2string(&out);}
  else if (FD_EQ(slotid,localstring_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_rfc822_x(&out,xt,1,U8_RFC822_NOZONE);
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
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,monthid_symbol))
    if (xt->u8_prec>=u8_month)
      if (xt->u8_mon<12)
        return monthids[xt->u8_mon];
      else if (reterr)
        return fd_err(fd_InvalidTimestamp,"xtime_get",
                      SYM_NAME(slotid),VOID);
      else return EMPTY;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,shortmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%b",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,longmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%B",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,dowid_symbol))
    if (xt->u8_prec>u8_month)
      if (xt->u8_wday<7)
        return dowids[xt->u8_wday];
      else if (reterr)
        return fd_err(fd_InvalidTimestamp,"xtime_get",
                      SYM_NAME(slotid),VOID);
      else return EMPTY;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,shortday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%a",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,longday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%A",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,hms_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%T",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,timestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%X",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,string_symbol))
    if (xt->u8_prec>=u8_second)
      return use_strftime("%c",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,shortstring_symbol)) {
    u8_byte buf[128]; struct U8_OUTPUT out;
    U8_INIT_FIXED_OUTPUT(&out,128,buf);
    if (xt->u8_prec<u8_hour)
      u8_printf(&out,"%d%s%d",
                xt->u8_mday,month_names[xt->u8_mon],xt->u8_year);
    else u8_printf
           (&out,"%d%s%d %02d:%02d:%02d%s",
            xt->u8_mday,month_names[xt->u8_mon],xt->u8_year,
            ((xt->u8_hour>12)?((xt->u8_hour)%12):(xt->u8_hour)),
            xt->u8_min,xt->u8_sec,
            ((xt->u8_hour>=12)?("PM"):("AM")));
    return fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  else if (FD_EQ(slotid,short_symbol)) {
    u8_byte buf[128]; struct U8_OUTPUT out;
    U8_INIT_FIXED_OUTPUT(&out,128,buf);
    if (xt->u8_prec<u8_hour)
      u8_printf(&out,"%d%s%d",
                xt->u8_mday,month_names[xt->u8_mon],xt->u8_year);
    else u8_printf
           (&out,"%d%s%d %02d:%02d%s",
            xt->u8_mday,month_names[xt->u8_mon],xt->u8_year,
            ((xt->u8_hour>12)?((xt->u8_hour)%12):(xt->u8_hour)),
            xt->u8_min,((xt->u8_hour>=12)?("PM"):("AM")));
    return fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  else if (FD_EQ(slotid,datestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%x",xt);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,dmy_symbol))
    if (xt->u8_prec>=u8_day) {
      if (xt->u8_mon<12) {
        char buf[64];
        sprintf(buf,"%d%s%04d",xt->u8_mday,
                month_names[xt->u8_mon],xt->u8_year);
        return fd_make_string(NULL,-1,buf);}
      else if (reterr)
        return fd_err(fd_InvalidTimestamp,"xtime_get",
                      SYM_NAME(slotid),VOID);
      else return EMPTY;}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,dm_symbol))
    if (xt->u8_prec>=u8_day)
      if (xt->u8_mon<12) {
        char buf[64];
        sprintf(buf,"%d%s",xt->u8_mday,month_names[xt->u8_mon]);
        return fd_make_string(NULL,-1,buf);}
      else if (reterr)
        return fd_err(fd_InvalidTimestamp,"xtime_get",
                      SYM_NAME(slotid),VOID);
      else return EMPTY;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,my_symbol))
    if (xt->u8_prec>=u8_month)
      if (xt->u8_mon<12) {
        char buf[64];
        sprintf(buf,"%s%d",month_names[xt->u8_mon],xt->u8_year);
        return fd_make_string(NULL,-1,buf);}
      else if (reterr)
        return fd_err(fd_InvalidTimestamp,"xtime_get",
                      SYM_NAME(slotid),VOID);
      else return EMPTY;
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,date_symbol))
    if (xt->u8_prec>=u8_day)
      return FD_BYTE2DTYPE(xt->u8_mday);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,hours_symbol))
    if (xt->u8_prec>=u8_hour)
      return FD_BYTE2DTYPE(xt->u8_hour);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,minutes_symbol))
    if (xt->u8_prec>=u8_minute)
      return FD_BYTE2DTYPE(xt->u8_min);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,seconds_symbol))
    if (xt->u8_prec>=u8_second)
      return FD_BYTE2DTYPE(xt->u8_sec);
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,tzoff_symbol))
    return FD_SHORT2DTYPE(xt->u8_tzoff);
  else if (FD_EQ(slotid,dstoff_symbol))
    return FD_SHORT2DTYPE(xt->u8_dstoff);
  else if (FD_EQ(slotid,gmtoff_symbol))
    return FD_SHORT2DTYPE((xt->u8_tzoff+xt->u8_dstoff));
  else if (FD_EQ(slotid,tick_symbol))
    if (xt->u8_prec>=u8_second) {
      time_t tick = xt->u8_tick;
      return FD_INT((long)tick);}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,prim_tick_symbol)) {
    time_t tick = xt->u8_tick;
    return FD_INT((long)tick);}
  else if (FD_EQ(slotid,xtick_symbol))
    if (xt->u8_prec>=u8_second) {
      double dsecs = (double)(xt->u8_tick), dnsecs = (double)(xt->u8_nsecs);
      return fd_init_double(NULL,dsecs+(dnsecs/1000000000.0));}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if ((FD_EQ(slotid,nanoseconds_symbol)) ||
           (FD_EQ(slotid,microseconds_symbol)) ||
           (FD_EQ(slotid,milliseconds_symbol)))
    if (xt->u8_prec>=u8_second) {
      unsigned int nsecs = xt->u8_nsecs;
      if (FD_EQ(slotid,nanoseconds_symbol))
        return FD_INT(nsecs);
      else {
        unsigned int reduce=
          ((FD_EQ(slotid,microseconds_symbol)) ? (1000) :(1000000));
        unsigned int half_reduce = reduce/2;
        unsigned int retval = ((nsecs/reduce)+((nsecs%reduce)>=half_reduce));
        return FD_INT(retval);}}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
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
    default: return EMPTY;}
  else if (FD_EQ(slotid,season_symbol))
    if (xt->u8_prec>=u8_month) {
      lispval results = EMPTY;
      int mon = xt->u8_mon+1;
      if ((mon>=12) || (mon<4)) {
        CHOICE_ADD(results,winter_symbol);}
      if ((mon>=3) && (mon<6)) {
        CHOICE_ADD(results,spring_symbol);}
      if ((mon>5) && (mon<10)) {
        CHOICE_ADD(results,summer_symbol);}
      if ((mon>8) && (mon<12)) {
        CHOICE_ADD(results,autumn_symbol);}
      return results;}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (FD_EQ(slotid,time_of_day_symbol))
    if (xt->u8_prec>=u8_hour) {
      lispval results = EMPTY;
      int hr = xt->u8_hour;
      if ((hr<5) || (hr>20)) {
        CHOICE_ADD(results,nighttime_symbol);}
      if ((hr>=5) && (hr<12)) {
        CHOICE_ADD(results,morning_symbol);}
      if ((hr>=12) && (hr<19)) {
        CHOICE_ADD(results,afternoon_symbol);}
      if ((hr>16) && (hr<22)) {
        CHOICE_ADD(results,evening_symbol);}
      return results;}
    else if (reterr)
      return fd_err(fd_ImpreciseTimestamp,"xtime_get",
                    SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (reterr)
    return fd_err(fd_NoSuchKey,"timestamp",NULL,slotid);
  else return EMPTY;
}

static int xtime_set(struct U8_XTIME *xt,lispval slotid,lispval value)
{
  time_t tick = xt->u8_tick; int rv = -1;
  if (FD_EQ(slotid,year_symbol))
    if (FD_UINTP(value))
      xt->u8_year = FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("year")),value);
  else if (FD_EQ(slotid,month_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>0) && (FIX2INT(value)<13))
      xt->u8_mon = FIX2INT(value)-1;
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("month")),value);
  else if (FD_EQ(slotid,date_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>0) && (FIX2INT(value)<32))
      xt->u8_mday = FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("date")),value);
  else if (FD_EQ(slotid,hours_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>=0) && (FIX2INT(value)<32))
      xt->u8_hour = FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("hours")),value);
  else if (FD_EQ(slotid,minutes_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>=0) && (FIX2INT(value)<60))
      xt->u8_min = FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("minutes")),value);
  else if (FD_EQ(slotid,seconds_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>=0) && (FIX2INT(value)<60))
      xt->u8_sec = FIX2INT(value);
    else return fd_reterr(fd_TypeError,"xtime_set",u8_strdup(_("seconds")),value);
  else if (FD_EQ(slotid,gmtoff_symbol)) {
    int gmtoff; if (tzvalueok(value,&gmtoff,"xtime_set/gmtoff")) {
      u8_tmprec prec = xt->u8_prec;
      time_t tick = xt->u8_tick; int nsecs = xt->u8_nsecs;
      int dstoff = xt->u8_dstoff;
      u8_init_xtime(xt,tick,prec,nsecs,gmtoff-dstoff,dstoff);
      return 0;}
    else return -1;}
  else if (FD_EQ(slotid,dstoff_symbol)) {
    int dstoff; if (tzvalueok(value,&dstoff,"xtime_set/dstoff")) {
      u8_tmprec prec = xt->u8_prec;
      time_t tick = xt->u8_tick; int nsecs = xt->u8_nsecs;
      int gmtoff = xt->u8_tzoff+xt->u8_dstoff;
      u8_init_xtime(xt,tick,prec,nsecs,gmtoff-dstoff,dstoff);
      return 0;}
    else return -1;}
  else if (FD_EQ(slotid,tzoff_symbol)) {
    int tzoff; if (tzvalueok(value,&tzoff,"xtime_set/tzoff")) {
      u8_tmprec prec = xt->u8_prec; int dstoff = xt->u8_dstoff;
      time_t tick = xt->u8_tick; int nsecs = xt->u8_nsecs;
      u8_init_xtime(xt,tick,prec,nsecs,tzoff,dstoff);
      return 0;}
    else return -1;}
  else if (FD_EQ(slotid,precision_symbol)) {
    int prec_val = get_precision(value);
    enum u8_timestamp_precision precision;
    if (prec_val<0)
      return fd_reterr(fd_TypeError,"xtime_set",_("precision"),value);
    else precision = prec_val;
    xt->u8_prec=precision;
    return 0;}
  else if (FD_EQ(slotid,timezone_symbol)) {
    if (STRINGP(value)) {
      u8_apply_tzspec(xt,CSTRING(value));
      return 0;}
    else return fd_reterr(fd_TypeError,"xtime_set",
                          u8_strdup(_("timezone string")),value);}
  rv = u8_mktime(xt);
  if (rv<0) return rv;
  else if (xt->u8_tick == tick) return 0;
  else return 1;
}

static lispval timestamp_get(lispval timestamp,lispval slotid,lispval dflt)
{
  struct FD_TIMESTAMP *tms=
    fd_consptr(struct FD_TIMESTAMP *,timestamp,fd_timestamp_type);
  if (VOIDP(dflt))
    return xtime_get(&(tms->u8xtimeval),slotid,1);
  else {
    lispval result = xtime_get(&(tms->u8xtimeval),slotid,0);
    if (EMPTYP(result)) return dflt;
    else return result;}
}

static int timestamp_store(lispval timestamp,lispval slotid,lispval val)
{
  struct FD_TIMESTAMP *tms=
    fd_consptr(struct FD_TIMESTAMP *,timestamp,fd_timestamp_type);
  return xtime_set(&(tms->u8xtimeval),slotid,val);
}

static lispval timestamp_getkeys(lispval timestamp)
{
  /* This could be clever about precision, but currently it isn't */
  return fd_incref(xtime_keys);
}

static lispval modtime_prim(lispval slotmap,lispval base,lispval togmt)
{
  lispval result;
  if (!(TABLEP(slotmap)))
    return fd_type_error("table","modtime_prim",slotmap);
  else if (VOIDP(base))
    result = timestamp_prim(VOID);
  else if (TYPEP(base,fd_timestamp_type))
    result = fd_deep_copy(base);
  else result = timestamp_prim(base);
  if (FD_ABORTP(result))
    return result;
  else {
    struct U8_XTIME *xt=
      &((fd_consptr(struct FD_TIMESTAMP *,result,fd_timestamp_type))->u8xtimeval);
    int tzoff = xt->u8_tzoff;
    lispval keys = fd_getkeys(slotmap);
    DO_CHOICES(key,keys) {
      lispval val = fd_get(slotmap,key,VOID);
      if (xtime_set(xt,key,val)<0) {
        result = FD_ERROR;
        FD_STOP_DO_CHOICES;
        break;}
      else {}}
    if (FD_ABORTP(result)) return result;
    else if (FALSEP(togmt)) {
      time_t moment = u8_mktime(xt);
      u8_init_xtime(xt,moment,xt->u8_prec,xt->u8_nsecs,tzoff,0);
      return result;}
    else {
      time_t moment = u8_mktime(xt);
      u8_init_xtime(xt,moment,xt->u8_prec,xt->u8_nsecs,0,0);
      return result;}}
}

static lispval mktime_lexpr(int n,lispval *args)
{
  lispval base; struct U8_XTIME *xt; int scan = 0;
  if (n%2) {
    lispval spec = args[0]; scan = 1;
    if (TYPEP(spec,fd_timestamp_type))
      base = fd_deep_copy(spec);
    else if ((FIXNUMP(spec))||(FD_BIGINTP(spec))) {
      time_t moment = (time_t)
        ((FIXNUMP(spec))?(FIX2INT(spec)):
         (fd_bigint_to_long_long((fd_bigint)spec)));
      base = fd_time2timestamp(moment);}
    else return fd_type_error(_("time base"),"mktime_lexpr",spec);}
  else base = fd_make_timestamp(NULL);
  xt = &(((struct FD_TIMESTAMP *)(base))->u8xtimeval);
  while (scan<n) {
    int rv = xtime_set(xt,args[scan],args[scan+1]);
    if (rv<0) {fd_decref(base); return FD_ERROR;}
    else scan = scan+2;}
  return (lispval)base;
}

/* Miscellanous time utilities */

static lispval timestring()
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

static lispval time_prim()
{
  time_t now = time(NULL);
  if (now<0) {
    u8_graberrno("time_prim",NULL);
    return FD_ERROR;}
  else return FD_INT(now);
}

static lispval millitime_prim()
{
  long long now = u8_millitime();
  if (now<0) {
    u8_graberrno("millitime_prim",NULL);
    return FD_ERROR;}
  else return FD_INT(now);
}

static lispval microtime_prim()
{
  long long now = u8_microtime();
  if (now<0) {
    u8_graberrno("microtime_prim",NULL);
    return FD_ERROR;}
  else return FD_INT(now);
}

static lispval now_macro(lispval expr,fd_lexenv env,fd_stack ptr)
{
  lispval field = fd_get_arg(expr,1);
  lispval now = fd_make_timestamp(NULL);
  lispval v = fd_get(now,field,FD_VOID);
  fd_decref(now);
  if ( (FD_VOIDP(v)) || (FD_EMPTYP(v)) )
    return FD_FALSE;
  else return v;
}

/* Counting seconds */

static lispval secs2string(lispval secs,lispval prec_arg)
{
  struct U8_OUTPUT out;
  double precision =
    ((FD_FLONUMP(prec_arg)) ? (FD_FLONUM(prec_arg)) :
     (FD_UINTP(prec_arg)) ? (FIX2INT(prec_arg)) :
     (FALSEP(prec_arg)) ? (-1) :
     (0));
  int elts = 0, done = 0;
  double seconds, reduce;
  int years, months, weeks, days, hours, minutes;
  if (FIXNUMP(secs))
    seconds = (double)FIX2INT(secs);
  else if (FD_FLONUMP(secs))
    seconds = FD_FLONUM(secs);
  else return fd_type_error(_("seconds"),"secs2string",secs);
  U8_INIT_OUTPUT(&out,64);
  if (seconds<0) {
    u8_printf(&out,"negative "); reduce = -seconds;}
  else if (seconds==0) {
    u8_free(out.u8_outbuf);
    return lispval_string("0 seconds");}
  else reduce = seconds;
  years = (int)floor(reduce/(365*24*3600));
  reduce = reduce-years*(365*24*3600);
  months = (int)floor(reduce/(30*24*3600));
  reduce = reduce-months*(30*24*3600);
  weeks = (int)floor(reduce/(7*24*3600));
  reduce = reduce-weeks*(7*24*3600);
  days = (int)floor(reduce/(24*3600));
  reduce = reduce-days*(3600*24);
  hours = (int)floor(reduce/(3600));
  reduce = reduce-hours*(3600);
  minutes = floor(reduce/60);
  reduce = reduce-minutes*60;

  if (years>0) {
    if (elts>0) u8_puts(&out,", ");
    if (years>1) {
      u8_printf(&out,_("%d years"),years);}
    else if (years==1) {
      u8_printf(&out,_("one year"));}
    else {}
    elts++;}

  if (done) {}
  else if (months>0) {
    if (elts>0) u8_puts(&out,", ");
    if (months>1) {
      u8_printf(&out,_("%d months"),months);}
    else if (months==1) {
      u8_printf(&out,_("one month"));}
    else {}
    if (precision >= 3600*24*300 ) done=1;
    elts++;}

  if (done) {}
  else if (weeks>0) {
    if (elts>0) u8_puts(&out,", ");
    if (weeks>1)
      u8_printf(&out,_("%d weeks"),weeks);
    else if (weeks==1)
      u8_printf(&out,_("one week"));
    else {}
    if (precision >= 3600*24*31 ) done=1;
    elts++;}

  if (done) {}
  else if (days>0) {
    if (elts>0) u8_puts(&out,", ");
    if (days>1)
      u8_printf(&out,_("%d days"),days);
    else if (days==1)
      u8_printf(&out,_("one day"));
    else {}
    if (precision >= 3600*24 ) done=1;
    elts++;}

  if (done) {}
  else if (hours>0) {
    if (elts>0) u8_puts(&out,", ");
    if (hours>1)
      u8_printf(&out,_("%d hours"),hours);
    else if (hours==1)
      u8_printf(&out,_("one hour"));
    else {}
    if (precision >= 3600 ) done=1;
    elts++;}

  if (done) {}
  else if (minutes>0) {
    if (elts>0) u8_puts(&out,", ");
    if (minutes>1)
      u8_printf(&out,_("%d minutes"),minutes);
    else if (minutes==1)
      u8_printf(&out,_("one minute"));
    else {}
    if (precision >= 60 ) done=1;
    elts++;}

  if (done) {}
  else if (reduce==0) {}
  else if (seconds==0) {}
  else if (precision<0) {
    if (elts>0) u8_puts(&out,", ");
    u8_printf(&out,_("%f seconds"),reduce);}
  else if (!(FD_FLONUMP(secs)))  {
    if (elts>0) u8_puts(&out,", ");
    u8_printf(&out,_("%d seconds"),(int)floor(reduce));}
  else if (precision<1) {
    if (elts>0) u8_puts(&out,", ");
    if ( (elts==0) && (reduce<1) )
      u8_printf(&out,_("%f seconds"),reduce);
    else if (precision < 0.0001)
      u8_printf(&out,_("%f seconds"),reduce);
    else if (precision < 0.001)
      u8_printf(&out,_("%.3f seconds"),reduce);
    else if (precision < 0.01)
      u8_printf(&out,_("%.2f seconds"),reduce);
    else u8_printf(&out,_("%.1f seconds"),reduce);}
  else {
    if (elts>0) u8_puts(&out,", ");
    u8_printf(&out,_("%d seconds"),(int)round(reduce));}
  return fd_stream2string(&out);
}

static lispval secs2short(lispval secs)
{
  struct U8_OUTPUT out;
  double seconds, reduce;
  int days, hours, minutes;
  if (FIXNUMP(secs))
    seconds = (double)FIX2INT(secs);
  else if (FD_FLONUMP(secs))
    seconds = FD_FLONUM(secs);
  else return fd_type_error(_("seconds"),"secs2string",secs);
  U8_INIT_OUTPUT(&out,64);
  if (seconds<0) {
    u8_printf(&out,"negative "); reduce = -seconds;}
  else if (seconds==0) {
    u8_free(out.u8_outbuf);
    return lispval_string("0 seconds");}
  else reduce = seconds;
  days = (int)floor(reduce/(24*3600));
  reduce = reduce-days*(3600*24);
  hours = (int)floor(reduce/(3600));
  reduce = reduce-hours*(3600);
  minutes = floor(reduce/60);
  reduce = reduce-minutes*60;

  if (days>0) u8_printf(&out,"%dd-");
  if ((days==0)&&(hours==0)&&(minutes==0))
    if (reduce>=10)
      u8_printf(&out,"%.2d:%0.2d:%f",hours,minutes,reduce);
    else u8_printf(&out,"%.2d:%0.2d:0%f",hours,minutes,reduce);
  else u8_printf(&out,"%.2d:%0.2d:%0.2d",hours,minutes,(int)floor(reduce));
  return fd_stream2string(&out);
}

/* Sleeping */

#if ((HAVE_SLEEP) || (HAVE_NANOSLEEP))
lispval sleep_prim(lispval arg)
{
  if (FIXNUMP(arg)) {
    long long ival = FIX2INT(arg);
    if (ival<0)
      return fd_type_error(_("positive fixnum time interval"),"sleep_prim",arg);
    sleep(ival);
    return FD_TRUE;}
  else if (FD_FLONUMP(arg)) {
#if HAVE_NANOSLEEP
    double interval = FD_FLONUM(arg);
    struct timespec req;
    if (interval<0)
      return fd_type_error(_("positive time interval"),"sleep_prim",arg);
    req.tv_sec = floor(interval);
    req.tv_nsec = 1000000000*(interval-req.tv_sec);
    nanosleep(&req,NULL);
    return FD_TRUE;
#else
    double floval = FD_FLONUM(arg);
    double secs = ceil(floval);
    int ival = (int)secs;
    if (ival<0)
      return fd_type_error(_("positive time interval"),"sleep_prim",arg);
    if (ival!=(int)floval)
      u8_log(LOG_WARN,"UnsupportedSleepPrecision",
             "This system doesnt' have fine-grained sleep precision");
    sleep(ival);
    return FD_TRUE;
#endif
  }
  else return fd_type_error(_("time interval"),"sleep_prim",arg);
}
#endif

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

/* UUID functions */

static lispval uuidp_prim(lispval x)
{
  if (TYPEP(x,fd_uuid_type)) return FD_TRUE;
  else return FD_FALSE;
}

static lispval getuuid_prim(lispval nodeid,lispval tptr)
{
  struct U8_XTIME *xt = NULL;
  long long id = -1;
  if ((VOIDP(tptr))&&(STRINGP(nodeid))) {
    /* Assume it's a UUID string, so parse it. */
      struct FD_UUID *uuid = u8_alloc(struct FD_UUID);
      u8_string start = CSTRING(nodeid);
      FD_INIT_CONS(uuid,fd_uuid_type);
      if ((start[0]==':')&&(start[1]=='#')&&
          (start[2]=='U')&&(isxdigit(start[3])))
        start = start+3;
      else if ((start[0]=='#')&&(start[1]=='U')&&(isxdigit(start[2])))
        start = start+2;
      else if ((start[0]=='U')&&(isxdigit(start[1])))
        start = start+1;
      else if (isxdigit(start[0])) {}
      else return fd_type_error("UUID string","getuuid_prim",nodeid);
      u8_parseuuid(start,(u8_uuid)&(uuid->uuid16));
      return LISP_CONS(uuid);}
  else if ((VOIDP(tptr))&&(PACKETP(nodeid)))
    if (FD_PACKET_LENGTH(nodeid)==16) {
      struct FD_UUID *uuid = u8_alloc(struct FD_UUID);
      const unsigned char *data = FD_PACKET_DATA(nodeid);
      FD_INIT_CONS(uuid,fd_uuid_type);
      memcpy(&(uuid->uuid16),data,16);
      return LISP_CONS(uuid);}
    else return fd_type_error("UUID (16-byte packet)","getuuid_prim",nodeid);
  else if ((VOIDP(tptr))&&(TYPEP(nodeid,fd_uuid_type)))
    return fd_incref(nodeid);
  else if ((VOIDP(tptr))&&(TYPEP(nodeid,fd_timestamp_type))) {
    lispval tmp = tptr; tptr = nodeid; nodeid = tmp;}
  if ((VOIDP(tptr))&&(VOIDP(nodeid)))
    return fd_fresh_uuid(NULL);
  if (TYPEP(tptr,fd_timestamp_type)) {
    struct FD_TIMESTAMP *tstamp=
      fd_consptr(struct FD_TIMESTAMP *,tptr,fd_timestamp_type);
    xt = &(tstamp->u8xtimeval);}
  if (FIXNUMP(nodeid))
    id = ((long long)(FIX2INT(nodeid)));
  else if (FD_BIGINTP(nodeid))
    id = fd_bigint2int64((fd_bigint)nodeid);
  else if (VOIDP(nodeid)) id = -1;
  else return fd_type_error("node id","getuuid_prim",nodeid);
  return fd_cons_uuid(NULL,xt,id,-1);
}

static lispval uuidtime_prim(lispval uuid_arg)
{
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,uuid_arg,fd_uuid_type);
  struct FD_TIMESTAMP *tstamp = u8_alloc(struct FD_TIMESTAMP);
  FD_INIT_CONS(tstamp,fd_timestamp_type);
  if (u8_uuid_xtime(uuid->uuid16,&(tstamp->u8xtimeval)))
    return LISP_CONS(tstamp);
  else {
    u8_free(tstamp);
    return fd_type_error("time-based UUID","uuidtime_prim",uuid_arg);}
}

static lispval uuidnode_prim(lispval uuid_arg)
{
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,uuid_arg,fd_uuid_type);
  long long id = u8_uuid_nodeid(uuid->uuid16);
  if (id<0)
    return fd_type_error("time-based UUID","uuidnode_prim",uuid_arg);
  else return FD_INT(id);
}

static lispval uuidstring_prim(lispval uuid_arg)
{
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,uuid_arg,fd_uuid_type);
  return fd_init_string(NULL,36,u8_uuidstring(uuid->uuid16,NULL));
}

static lispval uuidpacket_prim(lispval uuid_arg)
{
  struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,uuid_arg,fd_uuid_type);
  return fd_make_packet(NULL,16,uuid->uuid16);
}

/* Initialization */

FD_EXPORT void fd_init_timeprims_c()
{
  u8_register_source_file(_FILEINFO);

  tzset();

  init_id_tables();

  fd_tablefns[fd_timestamp_type]=u8_alloc(struct FD_TABLEFNS);
  fd_tablefns[fd_timestamp_type]->get = timestamp_get;
  fd_tablefns[fd_timestamp_type]->add = NULL;
  fd_tablefns[fd_timestamp_type]->drop = NULL;
  fd_tablefns[fd_timestamp_type]->store = timestamp_store;
  fd_tablefns[fd_timestamp_type]->test = NULL;
  fd_tablefns[fd_timestamp_type]->keys = timestamp_getkeys;

  year_symbol = fd_intern("YEAR");
  CHOICE_ADD(xtime_keys,year_symbol);
  month_symbol = fd_intern("MONTH");
  CHOICE_ADD(xtime_keys,month_symbol);
  date_symbol = fd_intern("DATE");
  CHOICE_ADD(xtime_keys,date_symbol);
  hours_symbol = fd_intern("HOURS");
  CHOICE_ADD(xtime_keys,hours_symbol);
  minutes_symbol = fd_intern("MINUTES");
  CHOICE_ADD(xtime_keys,minutes_symbol);
  seconds_symbol = fd_intern("SECONDS");
  CHOICE_ADD(xtime_keys,seconds_symbol);
  precision_symbol = fd_intern("PRECISION");
  CHOICE_ADD(xtime_keys,precision_symbol);
  tzoff_symbol = fd_intern("TZOFF");
  CHOICE_ADD(xtime_keys,tzoff_symbol);
  dstoff_symbol = fd_intern("DSTOFF");
  CHOICE_ADD(xtime_keys,dstoff_symbol);
  gmtoff_symbol = fd_intern("GMTOFF");
  CHOICE_ADD(xtime_keys,gmtoff_symbol);

  milliseconds_symbol = fd_intern("MILLISECONDS");
  CHOICE_ADD(xtime_keys,milliseconds_symbol);
  microseconds_symbol = fd_intern("MICROSECONDS");
  CHOICE_ADD(xtime_keys,microseconds_symbol);
  nanoseconds_symbol = fd_intern("NANOSECONDS");
  CHOICE_ADD(xtime_keys,nanoseconds_symbol);
  picoseconds_symbol = fd_intern("PICOSECONDS");
  CHOICE_ADD(xtime_keys,picoseconds_symbol);
  femtoseconds_symbol = fd_intern("FEMTOSECONDS");
  CHOICE_ADD(xtime_keys,femtoseconds_symbol);

  tick_symbol = fd_intern("TICK");
  CHOICE_ADD(xtime_keys,tick_symbol);
  prim_tick_symbol = fd_intern("%TICK");
  CHOICE_ADD(xtime_keys,prim_tick_symbol);
  xtick_symbol = fd_intern("XTICK");
  CHOICE_ADD(xtime_keys,xtick_symbol);
  iso_symbol = fd_intern("ISO");
  CHOICE_ADD(xtime_keys,iso_symbol);
  isodate_symbol = fd_intern("ISODATE");
  CHOICE_ADD(xtime_keys,isodate_symbol);
  isobasic_symbol = fd_intern("ISOBASIC");
  CHOICE_ADD(xtime_keys,isobasic_symbol);
  isobasicdate_symbol = fd_intern("ISOBASICDATE");
  CHOICE_ADD(xtime_keys,isobasicdate_symbol);
  isostring_symbol = fd_intern("ISOSTRING");
  CHOICE_ADD(xtime_keys,isostring_symbol);
  iso8601_symbol = fd_intern("ISO8601");
  CHOICE_ADD(xtime_keys,iso8601_symbol);
  rfc822_symbol = fd_intern("RFC822");
  CHOICE_ADD(xtime_keys,rfc822_symbol);
  rfc822date_symbol = fd_intern("RFC822DATE");
  CHOICE_ADD(xtime_keys,rfc822_symbol);
  rfc822x_symbol = fd_intern("RFC822X");
  CHOICE_ADD(xtime_keys,rfc822x_symbol);
  localstring_symbol = fd_intern("LOCALSTRING");
  CHOICE_ADD(xtime_keys,localstring_symbol);

  time_of_day_symbol = fd_intern("TIME-OF-DAY");
  CHOICE_ADD(xtime_keys,time_of_day_symbol);
  morning_symbol = fd_intern("MORNING");
  afternoon_symbol = fd_intern("AFTERNOON");
  evening_symbol = fd_intern("EVENING");
  nighttime_symbol = fd_intern("NIGHTTIME");

  season_symbol = fd_intern("SEASON");
  CHOICE_ADD(xtime_keys,season_symbol);
  spring_symbol = fd_intern("SPRING");
  summer_symbol = fd_intern("SUMMER");
  autumn_symbol = fd_intern("AUTUMN");
  winter_symbol = fd_intern("WINTER");

  shortmonth_symbol = fd_intern("MONTH-SHORT");
  CHOICE_ADD(xtime_keys,shortmonth_symbol);
  longmonth_symbol = fd_intern("MONTH-LONG");
  CHOICE_ADD(xtime_keys,longmonth_symbol);
  shortday_symbol = fd_intern("WEEKDAY-SHORT");
  CHOICE_ADD(xtime_keys,shortday_symbol);
  longday_symbol = fd_intern("WEEKDAY-LONG");
  CHOICE_ADD(xtime_keys,longday_symbol);
  hms_symbol = fd_intern("HMS");
  CHOICE_ADD(xtime_keys,hms_symbol);
  dmy_symbol = fd_intern("DMY");
  CHOICE_ADD(xtime_keys,dmy_symbol);
  dm_symbol = fd_intern("DM");
  CHOICE_ADD(xtime_keys,dm_symbol);
  my_symbol = fd_intern("MY");
  CHOICE_ADD(xtime_keys,my_symbol);
  string_symbol = fd_intern("STRING");
  CHOICE_ADD(xtime_keys,string_symbol);
  shortstring_symbol = fd_intern("SHORTSTRING");
  CHOICE_ADD(xtime_keys,shortstring_symbol);
  short_symbol = fd_intern("SHORT");
  CHOICE_ADD(xtime_keys,short_symbol);
  timestring_symbol = fd_intern("TIMESTRING");
  CHOICE_ADD(xtime_keys,timestring_symbol);
  datestring_symbol = fd_intern("DATESTRING");
  CHOICE_ADD(xtime_keys,datestring_symbol);
  fullstring_symbol = fd_intern("FULLSTRING");
  CHOICE_ADD(xtime_keys,fullstring_symbol);

  dowid_symbol = fd_intern("DOWID");
  CHOICE_ADD(xtime_keys,dowid_symbol);
  monthid_symbol = fd_intern("MONTHID");
  CHOICE_ADD(xtime_keys,monthid_symbol);

  timezone_symbol = fd_intern("TIMEZONE");
  CHOICE_ADD(xtime_keys,timezone_symbol);

  years_symbol=fd_intern("YEARS");
  months_symbol=fd_intern("MONTHS");
  day_symbol=fd_intern("DAY");
  days_symbol=fd_intern("DAYS");
  hour_symbol=fd_intern("HOUR");
  minute_symbol=fd_intern("MINUTE");
  second_symbol=fd_intern("SECOND");
  millisecond_symbol=fd_intern("MILLISECOND");
  microsecond_symbol=fd_intern("MICROSECOND");
  nanosecond_symbol=fd_intern("NANOSECOND");
  picosecond_symbol=fd_intern("PICOSECOND");
  femtosecond_symbol=fd_intern("FEMTOSECOND");

  gmt_symbol = fd_intern("GMT");

  fd_idefn(fd_scheme_module,fd_make_cprim1("TIMESTAMP?",timestampp,1));

  fd_idefn(fd_scheme_module,fd_make_cprim1("GMTIMESTAMP",gmtimestamp_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TIMESTAMP",timestamp_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1("ELAPSED-TIME",elapsed_time,0));
  fd_idefn(fd_scheme_module,fd_make_cprim0("TIMESTRING",timestring));

  fd_idefn(fd_scheme_module,fd_make_cprim3("MODTIME",modtime_prim,1));

  fd_idefn(fd_scheme_module,fd_make_cprim2("TIMESTAMP+",timestamp_plus,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("TIMESTAMP-",timestamp_minus,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("DIFFTIME",timestamp_diff,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("TIME>?",timestamp_greater,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2("TIME<?",timestamp_lesser,1));
  fd_defalias(fd_scheme_module,"TIME-EARLIER?","TIME<?");
  fd_defalias(fd_scheme_module,"TIME-LATER?","TIME>?");
  fd_defalias(fd_scheme_module,"TIME+","TIMESTAMP+");
  fd_defalias(fd_scheme_module,"TIME-","TIMESTAMP-");
  fd_idefn(fd_scheme_module,fd_make_cprim1("TIME-UNTIL",time_until,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("TIME-SINCE",time_since,1));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("FUTURE?",futurep,1,-1,VOID,fd_flonum_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim2x
           ("PAST?",pastp,1,-1,VOID,fd_flonum_type,VOID));

  fd_def_evalfn(fd_scheme_module,"#NOW",
                "#:NOW:YEAR\n evaluates to a field of the current time",
                now_macro);

  fd_idefn(fd_scheme_module,fd_make_cprimn("MKTIME",mktime_lexpr,0));

#if ((HAVE_SLEEP) || (HAVE_NANOSLEEP))
  fd_idefn(fd_scheme_module,fd_make_cprim1("SLEEP",sleep_prim,1));
#endif

  fd_idefn(fd_scheme_module,fd_make_cprim0("TIME",time_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("MILLITIME",millitime_prim));
  fd_idefn(fd_scheme_module,fd_make_cprim0("MICROTIME",microtime_prim));

  fd_idefn(fd_scheme_module,fd_make_cprim1("UUID?",uuidp_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim2("GETUUID",getuuid_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("UUID-TIME",uuidtime_prim,1,
                                            fd_uuid_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("UUID-NODE",uuidnode_prim,1,
                                            fd_uuid_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("UUID->STRING",uuidstring_prim,1,
                                            fd_uuid_type,VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1x("UUID->PACKET",uuidpacket_prim,1,
                                            fd_uuid_type,VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("SECS->STRING",secs2string,1,
                           -1,VOID,-1,FD_INT(3)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("SECS->SHORT",secs2short,1,-1,VOID));

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
