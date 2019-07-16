/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

/* #define KNO_PROVIDE_FASTEVAL 1 */

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"

#include <libu8/libu8io.h>
#include <libu8/u8timefns.h>
#include <libu8/u8rusage.h>
#include <libu8/u8netfns.h>

#include <ctype.h>
#include <math.h>
#include <sys/time.h>

#if ((HAVE_SYS_UTSNAME_H)&&(HAVE_UNAME))
#include <sys/utsname.h>
#include <kno/cprims.h>

#endif

u8_condition kno_ImpreciseTimestamp=_("Timestamp too imprecise");
u8_condition kno_InvalidTimestamp=_("Invalid timestamp object");
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
static lispval localstring_symbol, utcstring_symbol;
static lispval time_of_day_symbol, dowid_symbol, monthid_symbol;
static lispval shortmonth_symbol, longmonth_symbol;
static lispval  shortday_symbol, longday_symbol;
static lispval hms_symbol, dmy_symbol, dm_symbol, my_symbol;
static lispval shortstring_symbol, short_symbol;
static lispval string_symbol, fullstring_symbol;
static lispval timestring_symbol, datestring_symbol;

static int get_precision(lispval sym)
{
  if ( (KNO_EQ(sym,year_symbol)) || (KNO_EQ(sym,years_symbol)) )
    return (int) u8_year;
  else if ( (KNO_EQ(sym,month_symbol)) || (KNO_EQ(sym,months_symbol)) )
    return (int) u8_month;
  else if ( (KNO_EQ(sym,date_symbol)) || (KNO_EQ(sym,day_symbol)) ||
            (KNO_EQ(sym,days_symbol)) )
    return (int) u8_day;
  else if ( (KNO_EQ(sym,hours_symbol)) || (KNO_EQ(sym,hour_symbol)) )
    return (int) u8_hour;
  else if ( (KNO_EQ(sym,minutes_symbol)) || (KNO_EQ(sym,minute_symbol)) )
    return (int ) u8_minute;
  else if ( (KNO_EQ(sym,seconds_symbol)) || (KNO_EQ(sym,second_symbol)) )
    return (int) u8_second;
  else if ( (KNO_EQ(sym,milliseconds_symbol)) || (KNO_EQ(sym,millisecond_symbol)) )
    return (int) u8_millisecond;
  else if ( (KNO_EQ(sym,microseconds_symbol)) || (KNO_EQ(sym,microsecond_symbol)) )
    return (int) u8_microsecond;
  else if ( (KNO_EQ(sym,nanoseconds_symbol)) || (KNO_EQ(sym,nanosecond_symbol)) )
    return (int) u8_nanosecond;
  else if ( (KNO_EQ(sym,picoseconds_symbol)) || (KNO_EQ(sym,picosecond_symbol)) )
    return (int) u8_picosecond;
  else if ( (KNO_EQ(sym,femtoseconds_symbol)) || (KNO_EQ(sym,femtosecond_symbol)) )
    return (int) u8_femtosecond;
  else return -1;
}

KNO_DEFPRIM1("timestamp?",timestampp,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(TIMESTAMP? *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval timestampp(lispval arg)
{
  if (TYPEP(arg,kno_timestamp_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DEFPRIM1("timestamp",timestamp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
 "`(TIMESTAMP [*arg0*])` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval timestamp_prim(lispval arg)
{
  struct KNO_TIMESTAMP *tm = u8_alloc(struct KNO_TIMESTAMP);
  memset(tm,0,sizeof(struct KNO_TIMESTAMP));
  KNO_INIT_CONS(tm,kno_timestamp_type);
  if (VOIDP(arg)) {
    u8_local_xtime(&(tm->u8xtimeval),-1,u8_nanosecond,0);
    return LISP_CONS(tm);}
  else if (STRINGP(arg)) {
    time_t tick;
    u8_string sdata = CSTRING(arg);
    int c = *sdata;
    if (u8_isdigit(c))
      tick=u8_iso8601_to_xtime(sdata,&(tm->u8xtimeval));
    else tick=u8_rfc822_to_xtime(sdata,&(tm->u8xtimeval));
    if (tick>=0)
      return LISP_CONS(tm);
    else {
      u8_free(tm);
      return kno_type_error("timestamp arg","timestamp_prim",arg);}}
  else if (SYMBOLP(arg)) {
    int prec_val = get_precision(arg);
    enum u8_timestamp_precision prec;
    if (prec_val<0)
      return kno_type_error("timestamp precision","timestamp_prim",arg);
    else prec=prec_val;
    u8_local_xtime(&(tm->u8xtimeval),-1,prec,-1);
    return LISP_CONS(tm);}
  else if (FIXNUMP(arg)) {
    u8_local_xtime(&(tm->u8xtimeval),(time_t)(FIX2INT(arg)),u8_second,-1);
    return LISP_CONS(tm);}
  else if (TYPEP(arg,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *fdt = (struct KNO_TIMESTAMP *)arg;
    u8_local_xtime(&(tm->u8xtimeval),
                   fdt->u8xtimeval.u8_tick,fdt->u8xtimeval.u8_prec,
                   fdt->u8xtimeval.u8_nsecs);
    return LISP_CONS(tm);}
  else if (KNO_BIGINTP(arg)) {
#if (SIZEOF_TIME_T == 8)
    time_t tv = (time_t)kno_bigint_to_long_long((kno_bigint)arg);
#else
    time_t tv = (time_t)kno_bigint_to_long((kno_bigint)arg);
#endif
    u8_local_xtime(&(tm->u8xtimeval),tv,u8_second,-1);
    return LISP_CONS(tm);}
  else if (KNO_FLONUMP(arg)) {
    double dv = KNO_FLONUM(arg);
    double dsecs = floor(dv), dnsecs = (dv-dsecs)*1000000000;
    unsigned int secs = (unsigned int)dsecs, nsecs = (unsigned int)dnsecs;
    u8_local_xtime(&(tm->u8xtimeval),(time_t)secs,u8_second,nsecs);
    return LISP_CONS(tm);}
  else {
    u8_free(tm);
    return kno_type_error("timestamp arg","timestamp_prim",arg);}
}

KNO_DEFPRIM1("gmtimestamp",gmtimestamp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
 "`(GMTIMESTAMP [*arg0*])` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval gmtimestamp_prim(lispval arg)
{
  struct KNO_TIMESTAMP *tm = u8_alloc(struct KNO_TIMESTAMP);
  memset(tm,0,sizeof(struct KNO_TIMESTAMP));
  KNO_INIT_CONS(tm,kno_timestamp_type);
  if (VOIDP(arg)) {
    u8_init_xtime(&(tm->u8xtimeval),-1,u8_nanosecond,0,0,0);
    return LISP_CONS(tm);}
  else if (TYPEP(arg,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *ftm = kno_consptr(kno_timestamp,arg,kno_timestamp_type);
    if ((ftm->u8xtimeval.u8_tzoff==0)&&(ftm->u8xtimeval.u8_dstoff==0)) {
      u8_free(tm); return kno_incref(arg);}
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
      moment=u8_iso8601_to_xtime(sdata,&(tm->u8xtimeval));
    else moment=u8_rfc822_to_xtime(sdata,&(tm->u8xtimeval));
    if (moment >= 0) moment = u8_mktime(&(tm->u8xtimeval));
    if (moment<0) {
      u8_free(tm);
      return kno_type_error("timestamp arg","timestamp_prim",arg);}
    if ((tm->u8xtimeval.u8_tzoff!=0)||(tm->u8xtimeval.u8_dstoff!=0))
      u8_init_xtime(&(tm->u8xtimeval),moment,tm->u8xtimeval.u8_prec,
                    tm->u8xtimeval.u8_nsecs,0,0);
    return LISP_CONS(tm);}
  else if (SYMBOLP(arg)) {
    int prec_val = get_precision(arg);
    enum u8_timestamp_precision prec;
    if (prec_val<0)
      return kno_type_error("timestamp precision","timestamp_prim",arg);
    else prec = prec_val;
    u8_init_xtime(&(tm->u8xtimeval),-1,prec,-1,0,0);
    return LISP_CONS(tm);}
  else if (FIXNUMP(arg)) {
    u8_init_xtime(&(tm->u8xtimeval),(time_t)(FIX2INT(arg)),u8_second,-1,0,0);
    return LISP_CONS(tm);}
  else if (KNO_BIGINTP(arg)) {
#if (SIZEOF_TIME_T == 8)
    time_t tv = (time_t)kno_bigint_to_long_long((kno_bigint)arg);
#else
    time_t tv = (time_t)kno_bigint_to_long((kno_bigint)arg);
#endif
    u8_init_xtime(&(tm->u8xtimeval),tv,u8_second,-1,0,0);
    return LISP_CONS(tm);}
  else if (KNO_FLONUMP(arg)) {
    double dv = KNO_FLONUM(arg);
    double dsecs = floor(dv), dnsecs = (dv-dsecs)*1000000000;
    unsigned int secs = (unsigned int)dsecs, nsecs = (unsigned int)dnsecs;
    u8_init_xtime
      (&(tm->u8xtimeval),(time_t)secs,u8_second,nsecs,0,0);
    return LISP_CONS(tm);}
  else {
    u8_free(tm);
    return kno_type_error("timestamp arg","timestamp_prim",arg);}
}

static struct KNO_TIMESTAMP *get_timestamp(lispval arg,int *freeit)
{
  *freeit = 0;
  if (TYPEP(arg,kno_timestamp_type)) {
    return kno_consptr(struct KNO_TIMESTAMP *,arg,kno_timestamp_type);}
  else if (STRINGP(arg)) {
    struct KNO_TIMESTAMP *tm = u8_alloc(struct KNO_TIMESTAMP);
    memset(tm,0,sizeof(struct KNO_TIMESTAMP));
    time_t moment =
      u8_iso8601_to_xtime(CSTRING(arg),&(tm->u8xtimeval));
    if (moment<0) {
      kno_set_type_error("timestamp",arg);
      u8_free(tm);
      return NULL;}
    else {
      *freeit = 1;
      return tm;}}
  else if ((FIXNUMP(arg))||
           ((KNO_BIGINTP(arg))&&
            (kno_modest_bigintp((kno_bigint)arg)))) {
    struct KNO_TIMESTAMP *tm = u8_alloc(struct KNO_TIMESTAMP);
    long long int tick; time_t moment;
    if (FIXNUMP(arg))
      tick = FIX2INT(arg);
    else tick = kno_bigint_to_long_long((kno_bigint)arg);
    KNO_INIT_FRESH_CONS(tm,kno_timestamp_type); *freeit = 1;
    if (tick<31536000L) {
      moment = u8_now(&(tm->u8xtimeval));
      if (moment > 0)
        u8_xtime_plus(&(tm->u8xtimeval),FIX2INT(arg));
      else {
        u8_free(tm); *freeit=0;
        return NULL;}}
    else if (u8_init_xtime(&(tm->u8xtimeval),tick,u8_second,0,0,0) == NULL) {
      u8_free(tm); *freeit=0;
      return NULL;}
    return tm;}
  else if (VOIDP(arg)) {
    struct KNO_TIMESTAMP *tm = u8_alloc(struct KNO_TIMESTAMP);
    memset(tm,0,sizeof(struct KNO_TIMESTAMP));
    time_t moment = u8_now(&(tm->u8xtimeval));
    if (moment < 0) {
      u8_free(tm);
      return NULL;}
    *freeit=1;
    return tm;}
  else {
    kno_set_type_error("timestamp",arg);
    return NULL;}
}

static lispval timestamp_plus_helper(lispval arg1,lispval arg2,int neg)
{
  double delta; int free_old = 0;
  struct U8_XTIME tmp, *btime;
  struct KNO_TIMESTAMP *newtm = u8_alloc(struct KNO_TIMESTAMP), *oldtm = NULL;
  memset(newtm,0,sizeof(struct KNO_TIMESTAMP));
  if (VOIDP(arg2)) {
    if ((FIXNUMP(arg1)) || (KNO_FLONUMP(arg1)) || (KNO_RATIONALP(arg1)))
      delta = kno_todouble(arg1);
    else {
      u8_free(newtm);
      return kno_type_error("number","timestamp_plus",arg1);}
    u8_init_xtime(&tmp,-1,u8_nanosecond,-1,0,0);
    btime = &tmp;}
  else if ((FIXNUMP(arg2)) || (KNO_FLONUMP(arg2)) || (KNO_RATIONALP(arg2))) {
    delta = kno_todouble(arg2);
    oldtm = get_timestamp(arg1,&free_old);
    btime = &(oldtm->u8xtimeval);}
  else return kno_type_error("number","timestamp_plus",arg2);
  if (neg) delta = -delta;
  /* Init the cons bit field */
  KNO_INIT_CONS(newtm,kno_timestamp_type);
  /* Copy the data */
  memcpy(&(newtm->u8xtimeval),btime,sizeof(struct U8_XTIME));
  u8_xtime_plus(&(newtm->u8xtimeval),delta);
  if (free_old) u8_free(oldtm);
  return LISP_CONS(newtm);
}

KNO_DEFPRIM2("timestamp+",timestamp_plus,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(TIMESTAMP+ *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval timestamp_plus(lispval arg1,lispval arg2)
{
  return timestamp_plus_helper(arg1,arg2,0);
}

KNO_DEFPRIM2("timestamp-",timestamp_minus,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(TIMESTAMP- *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval timestamp_minus(lispval arg1,lispval arg2)
{
  return timestamp_plus_helper(arg1,arg2,1);
}

KNO_DEFPRIM2("difftime",timestamp_diff,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(DIFFTIME *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval timestamp_diff(lispval timestamp1,lispval timestamp2)
{
  if ((KNO_FLONUMP(timestamp1))&&(VOIDP(timestamp2))) {
    double then = KNO_FLONUM(timestamp1);
    double now = u8_elapsed_time();
    double diff = now-then;
    return kno_make_flonum(diff);}
  else if ((KNO_FLONUMP(timestamp1))&&(KNO_FLONUMP(timestamp2))) {
    double t1 = KNO_FLONUM(timestamp1);
    double t2 = KNO_FLONUM(timestamp2);
    double diff = t1-t2;
    return kno_make_flonum(diff);}
  else {
    int free1 = 0, free2 = 0;
    struct KNO_TIMESTAMP *t1 = get_timestamp(timestamp1,&free1);
    struct KNO_TIMESTAMP *t2 = get_timestamp(timestamp2,&free2);
    if ((t1 == NULL) || (t2 == NULL)) {
      if (free1) u8_free(t1);
      if (free2) u8_free(t2);
      return KNO_ERROR;}
    else {
      double diff = u8_xtime_diff(&(t1->u8xtimeval),&(t2->u8xtimeval));
      if (free1) u8_free(t1);
      if (free2) u8_free(t2);
      return kno_init_double(NULL,diff);}}
}

KNO_DEFPRIM1("time-until",time_until,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(TIME-UNTIL *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval time_until(lispval arg)
{
  return timestamp_diff(arg,VOID);
}
KNO_DEFPRIM1("time-since",time_since,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(TIME-SINCE *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval time_since(lispval arg)
{
  return timestamp_diff(VOID,arg);
}

KNO_DEFPRIM2("time>?",timestamp_greater,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(TIME>? *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval timestamp_greater(lispval timestamp1,lispval timestamp2)
{
  int free1 = 0;
  struct KNO_TIMESTAMP *t1 = get_timestamp(timestamp1,&free1);
  if (t1==NULL)
    return KNO_ERROR;
  else if (VOIDP(timestamp2)) {
    double diff;
    struct U8_XTIME xtime; u8_now(&xtime);
    diff = u8_xtime_diff(&(t1->u8xtimeval),(&xtime));
    if (free1) u8_free(t1);
    if (diff>0)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else {
    double diff; int free2 = 0, err = 0;
    struct KNO_TIMESTAMP *t2 = get_timestamp(timestamp2,&free2);
    if (t2)
      diff = u8_xtime_diff(&(t1->u8xtimeval),&(t2->u8xtimeval));
    else err = 1;
    if (free1) u8_free(t1);
    if (free2) u8_free(t2);
    if (err)
      return KNO_ERROR;
    else if (diff>0)
      return KNO_TRUE;
    else return KNO_FALSE;}
}

KNO_DEFPRIM2("time<?",timestamp_lesser,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(TIME<? *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval timestamp_lesser(lispval timestamp1,lispval timestamp2)
{
  int free1 = 0;
  struct KNO_TIMESTAMP *t1 = get_timestamp(timestamp1,&free1);
  if (t1==NULL)
    return KNO_ERROR;
  else if (VOIDP(timestamp2)) {
    double diff;
    struct U8_XTIME xtime; u8_now(&xtime);
    diff = u8_xtime_diff(&(t1->u8xtimeval),&xtime);
    if (free1) u8_free(t1);
    if (diff<0)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else {
    double diff; int free2 = 0, err = 0;
    struct KNO_TIMESTAMP *t2 = get_timestamp(timestamp2,&free2);
    if (t2)
      diff = u8_xtime_diff(&(t1->u8xtimeval),&(t2->u8xtimeval));
    else err = 1;
    if (free1) u8_free(t1);
    if (free2) u8_free(t2);
    if (err)
      return KNO_ERROR;
    else if (diff<0)
      return KNO_TRUE;
    else return KNO_FALSE;}
}

KNO_EXPORT
int kno_cmp_now(lispval timestamp,double thresh)
{
  int free_t = 0;
  struct KNO_TIMESTAMP *t = get_timestamp(timestamp,&free_t);
  if (t == NULL)
    return KNO_ERROR;
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

KNO_DEFPRIM2("future?",futurep,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(FUTURE? *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_flonum_type,KNO_VOID);
static lispval futurep(lispval timestamp,lispval thresh_arg)
{
  int thresh = (KNO_FLONUMP(thresh_arg)) ?
    (KNO_FLONUM(thresh_arg)) :
    (0.0);
  if (kno_cmp_now(timestamp,thresh)>0)
    return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DEFPRIM2("past?",pastp,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(PAST? *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_flonum_type,KNO_VOID);
static lispval pastp(lispval timestamp,lispval thresh_arg)
{
  int thresh = (KNO_FLONUMP(thresh_arg)) ?
    (KNO_FLONUM(thresh_arg)) :
    (0.0);
  if (kno_cmp_now(timestamp,thresh)<0)
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* Lisp access */

KNO_DEFPRIM1("elapsed-time",elapsed_time,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
 "`(ELAPSED-TIME [*arg0*])` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval elapsed_time(lispval arg)
{
  double elapsed = u8_elapsed_time();
  if (VOIDP(arg))
    return kno_init_double(NULL,elapsed);
  else if (KNO_FLONUMP(arg)) {
    double base = KNO_FLONUM(arg);
    return kno_init_double(NULL,elapsed-base);}
  else if (KNO_FIXNUMP(arg)) {
    long long intval = KNO_FIX2INT(arg);
    double base = (double) intval;
    return kno_init_double(NULL,elapsed-base);}
  else return kno_type_error("double","elapsed_time",arg);
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
    return kno_err(strftime_error,"use_strftime",format,VOID);}
  else return kno_init_string(NULL,n_bytes,buf);
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
  else if (KNO_FLONUMP(value)) {
    double floatval = KNO_FLONUM(value);
    int fixval = (int) floatval;
    if ((fixval<(48*3600))&&(fixval>(-48*3600))) {
      *off = fixval;
      return 1;}}
  kno_seterr(kno_TypeError,caller,"invalid timezone offset",value);
  return 0;
}

static lispval xtime_get(struct U8_XTIME *xt,lispval slotid,int reterr)
{
  if (KNO_EQ(slotid,year_symbol))
    if (xt->u8_prec>=u8_year)
      return KNO_INT(xt->u8_year);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,fullstring_symbol))
    if (xt->u8_prec>=u8_day)
      return use_strftime("%A %d %B %Y %r %z",xt);
    else if (xt->u8_prec == u8_day)
      return use_strftime("%A %d %B %Y %z",xt);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if ((KNO_EQ(slotid,iso_symbol)) ||
           (KNO_EQ(slotid,isostring_symbol)) ||
           (KNO_EQ(slotid,iso8601_symbol))) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_iso8601(&out,xt);
    return kno_stream2string(&out);}
  else if (KNO_EQ(slotid,isodate_symbol)) {
    struct U8_XTIME newt;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    memcpy(&newt,xt,sizeof(struct U8_XTIME));
    u8_set_xtime_precision(&newt,u8_day);
    u8_xtime_to_iso8601(&out,&newt);
    return kno_stream2string(&out);}
  else if (KNO_EQ(slotid,isobasic_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_iso8601_x(&out,xt,U8_ISO8601_BASIC);
    return kno_stream2string(&out);}
  else if (KNO_EQ(slotid,isobasicdate_symbol)) {
    struct U8_XTIME newt;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    memcpy(&newt,xt,sizeof(struct U8_XTIME));
    u8_set_xtime_precision(&newt,u8_day);
    u8_xtime_to_iso8601_x(&out,&newt,U8_ISO8601_BASIC);
    return kno_stream2string(&out);}
  else if (KNO_EQ(slotid,rfc822_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_rfc822(&out,xt);
    return kno_stream2string(&out);}
  else if (KNO_EQ(slotid,rfc822date_symbol)) {
    struct U8_XTIME newt;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    memcpy(&newt,xt,sizeof(struct U8_XTIME));
    u8_set_xtime_precision(&newt,u8_day);
    u8_xtime_to_rfc822(&out,&newt);
    return kno_stream2string(&out);}
  else if (KNO_EQ(slotid,rfc822x_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_rfc822_x(&out,xt,-1,0);
    return kno_stream2string(&out);}
  else if (KNO_EQ(slotid,localstring_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_rfc822_x(&out,xt,1,U8_RFC822_NOZONE);
    return kno_stream2string(&out);}
  else if (KNO_EQ(slotid,utcstring_symbol)) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,128);
    u8_xtime_to_rfc822_x(&out,xt,0,U8_RFC822_NOZONE);
    return kno_stream2string(&out);}
  else if (KNO_EQ(slotid,gmt_symbol))
    if (xt->u8_tzoff==0)
      return kno_make_timestamp(xt);
    else {
      struct U8_XTIME asgmt;
      u8_init_xtime(&asgmt,xt->u8_tick,xt->u8_prec,xt->u8_nsecs,0,0);
      return kno_make_timestamp(&asgmt);}
  else if (KNO_EQ(slotid,month_symbol))
    if (xt->u8_prec>=u8_month)
      return KNO_BYTE2DTYPE(xt->u8_mon);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,monthid_symbol))
    if (xt->u8_prec>=u8_month)
      if (xt->u8_mon<12)
        return monthids[xt->u8_mon];
      else if (reterr)
        return kno_err(kno_InvalidTimestamp,"xtime_get",
                       SYM_NAME(slotid),VOID);
      else return EMPTY;
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,shortmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%b",xt);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,longmonth_symbol))
    if (xt->u8_prec>=u8_month)
      return use_strftime("%B",xt);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,dowid_symbol))
    if (xt->u8_prec>u8_month)
      if (xt->u8_wday<7)
        return dowids[xt->u8_wday];
      else if (reterr)
        return kno_err(kno_InvalidTimestamp,"xtime_get",
                       SYM_NAME(slotid),VOID);
      else return EMPTY;
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,shortday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%a",xt);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,longday_symbol))
    if (xt->u8_prec>u8_month)
      return use_strftime("%A",xt);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,hms_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%T",xt);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,timestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%X",xt);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,string_symbol))
    if (xt->u8_prec>=u8_second)
      return use_strftime("%c",xt);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,shortstring_symbol)) {
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
    return kno_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  else if (KNO_EQ(slotid,short_symbol)) {
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
    return kno_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);}
  else if (KNO_EQ(slotid,datestring_symbol))
    if (xt->u8_prec>=u8_hour)
      return use_strftime("%x",xt);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,dmy_symbol))
    if (xt->u8_prec>=u8_day) {
      if (xt->u8_mon<12) {
        char buf[64];
        sprintf(buf,"%d%s%04d",xt->u8_mday,
                month_names[xt->u8_mon],xt->u8_year);
        return kno_make_string(NULL,-1,buf);}
      else if (reterr)
        return kno_err(kno_InvalidTimestamp,"xtime_get",
                       SYM_NAME(slotid),VOID);
      else return EMPTY;}
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,dm_symbol))
    if (xt->u8_prec>=u8_day)
      if (xt->u8_mon<12) {
        char buf[64];
        sprintf(buf,"%d%s",xt->u8_mday,month_names[xt->u8_mon]);
        return kno_make_string(NULL,-1,buf);}
      else if (reterr)
        return kno_err(kno_InvalidTimestamp,"xtime_get",
                       SYM_NAME(slotid),VOID);
      else return EMPTY;
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,my_symbol))
    if (xt->u8_prec>=u8_month)
      if (xt->u8_mon<12) {
        char buf[64];
        sprintf(buf,"%s%d",month_names[xt->u8_mon],xt->u8_year);
        return kno_make_string(NULL,-1,buf);}
      else if (reterr)
        return kno_err(kno_InvalidTimestamp,"xtime_get",
                       SYM_NAME(slotid),VOID);
      else return EMPTY;
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,date_symbol))
    if (xt->u8_prec>=u8_day)
      return KNO_BYTE2DTYPE(xt->u8_mday);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,hours_symbol))
    if (xt->u8_prec>=u8_hour)
      return KNO_BYTE2DTYPE(xt->u8_hour);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,minutes_symbol))
    if (xt->u8_prec>=u8_minute)
      return KNO_BYTE2DTYPE(xt->u8_min);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,seconds_symbol))
    if (xt->u8_prec>=u8_second)
      return KNO_BYTE2DTYPE(xt->u8_sec);
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,tzoff_symbol))
    return KNO_SHORT2DTYPE(xt->u8_tzoff);
  else if (KNO_EQ(slotid,dstoff_symbol))
    return KNO_SHORT2DTYPE(xt->u8_dstoff);
  else if (KNO_EQ(slotid,gmtoff_symbol))
    return KNO_SHORT2DTYPE((xt->u8_tzoff+xt->u8_dstoff));
  else if (KNO_EQ(slotid,tick_symbol))
    if (xt->u8_prec>=u8_second) {
      time_t tick = xt->u8_tick;
      return KNO_INT((long)tick);}
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,prim_tick_symbol)) {
    time_t tick = xt->u8_tick;
    return KNO_INT((long)tick);}
  else if (KNO_EQ(slotid,xtick_symbol))
    if (xt->u8_prec>=u8_second) {
      double dsecs = (double)(xt->u8_tick), dnsecs = (double)(xt->u8_nsecs);
      return kno_init_double(NULL,dsecs+(dnsecs/1000000000.0));}
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if ((KNO_EQ(slotid,nanoseconds_symbol)) ||
           (KNO_EQ(slotid,microseconds_symbol)) ||
           (KNO_EQ(slotid,milliseconds_symbol)))
    if (xt->u8_prec>=u8_second) {
      unsigned int nsecs = xt->u8_nsecs;
      if (KNO_EQ(slotid,nanoseconds_symbol))
        return KNO_INT(nsecs);
      else {
        unsigned int reduce=
          ((KNO_EQ(slotid,microseconds_symbol)) ? (1000) :(1000000));
        unsigned int half_reduce = reduce/2;
        unsigned int retval = ((nsecs/reduce)+((nsecs%reduce)>=half_reduce));
        return KNO_INT(retval);}}
    else if (reterr)
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,precision_symbol))
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
  else if (KNO_EQ(slotid,season_symbol))
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
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (KNO_EQ(slotid,time_of_day_symbol))
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
      return kno_err(kno_ImpreciseTimestamp,"xtime_get",
                     SYM_NAME(slotid),VOID);
    else return EMPTY;
  else if (reterr)
    return kno_err(kno_NoSuchKey,"timestamp",NULL,slotid);
  else return EMPTY;
}

static int xtime_set(struct U8_XTIME *xt,lispval slotid,lispval value)
{
  time_t tick = xt->u8_tick; int rv = -1;
  if (KNO_EQ(slotid,year_symbol))
    if (KNO_UINTP(value))
      xt->u8_year = FIX2INT(value);
    else return kno_reterr(kno_TypeError,"xtime_set",u8_strdup(_("year")),value);
  else if (KNO_EQ(slotid,month_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>0) && (FIX2INT(value)<13))
      xt->u8_mon = FIX2INT(value)-1;
    else return kno_reterr(kno_TypeError,"xtime_set",u8_strdup(_("month")),value);
  else if (KNO_EQ(slotid,date_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>0) && (FIX2INT(value)<32))
      xt->u8_mday = FIX2INT(value);
    else return kno_reterr(kno_TypeError,"xtime_set",u8_strdup(_("date")),value);
  else if (KNO_EQ(slotid,hours_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>=0) && (FIX2INT(value)<32))
      xt->u8_hour = FIX2INT(value);
    else return kno_reterr(kno_TypeError,"xtime_set",u8_strdup(_("hours")),value);
  else if (KNO_EQ(slotid,minutes_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>=0) && (FIX2INT(value)<60))
      xt->u8_min = FIX2INT(value);
    else return kno_reterr(kno_TypeError,"xtime_set",u8_strdup(_("minutes")),value);
  else if (KNO_EQ(slotid,seconds_symbol))
    if ((FIXNUMP(value)) &&
        (FIX2INT(value)>=0) && (FIX2INT(value)<60))
      xt->u8_sec = FIX2INT(value);
    else return kno_reterr(kno_TypeError,"xtime_set",u8_strdup(_("seconds")),value);
  else if (KNO_EQ(slotid,gmtoff_symbol)) {
    int gmtoff; if (tzvalueok(value,&gmtoff,"xtime_set/gmtoff")) {
      u8_tmprec prec = xt->u8_prec;
      time_t tick = xt->u8_tick; int nsecs = xt->u8_nsecs;
      int dstoff = xt->u8_dstoff;
      u8_init_xtime(xt,tick,prec,nsecs,gmtoff-dstoff,dstoff);
      return 0;}
    else return -1;}
  else if (KNO_EQ(slotid,dstoff_symbol)) {
    int dstoff; if (tzvalueok(value,&dstoff,"xtime_set/dstoff")) {
      u8_tmprec prec = xt->u8_prec;
      time_t tick = xt->u8_tick; int nsecs = xt->u8_nsecs;
      int gmtoff = xt->u8_tzoff+xt->u8_dstoff;
      u8_init_xtime(xt,tick,prec,nsecs,gmtoff-dstoff,dstoff);
      return 0;}
    else return -1;}
  else if (KNO_EQ(slotid,tzoff_symbol)) {
    int tzoff; if (tzvalueok(value,&tzoff,"xtime_set/tzoff")) {
      u8_tmprec prec = xt->u8_prec; int dstoff = xt->u8_dstoff;
      time_t tick = xt->u8_tick; int nsecs = xt->u8_nsecs;
      u8_init_xtime(xt,tick,prec,nsecs,tzoff,dstoff);
      return 0;}
    else return -1;}
  else if (KNO_EQ(slotid,precision_symbol)) {
    int prec_val = get_precision(value);
    enum u8_timestamp_precision precision;
    if (prec_val<0)
      return kno_reterr(kno_TypeError,"xtime_set",_("precision"),value);
    else precision = prec_val;
    xt->u8_prec=precision;
    return 0;}
  else if (KNO_EQ(slotid,timezone_symbol)) {
    if (STRINGP(value)) {
      u8_apply_tzspec(xt,CSTRING(value));
      return 0;}
    else return kno_reterr(kno_TypeError,"xtime_set",
                           u8_strdup(_("timezone string")),value);}
  rv = u8_mktime(xt);
  if (rv<0) return rv;
  else if (xt->u8_tick == tick) return 0;
  else return 1;
}

static lispval timestamp_get(lispval timestamp,lispval slotid,lispval dflt)
{
  struct KNO_TIMESTAMP *tms=
    kno_consptr(struct KNO_TIMESTAMP *,timestamp,kno_timestamp_type);
  if (VOIDP(dflt))
    return xtime_get(&(tms->u8xtimeval),slotid,1);
  else {
    lispval result = xtime_get(&(tms->u8xtimeval),slotid,0);
    if (EMPTYP(result)) return dflt;
    else return result;}
}

static int timestamp_store(lispval timestamp,lispval slotid,lispval val)
{
  struct KNO_TIMESTAMP *tms=
    kno_consptr(struct KNO_TIMESTAMP *,timestamp,kno_timestamp_type);
  return xtime_set(&(tms->u8xtimeval),slotid,val);
}

static lispval timestamp_getkeys(lispval timestamp)
{
  /* This could be clever about precision, but currently it isn't */
  return kno_incref(xtime_keys);
}

KNO_DEFPRIM3("modtime",modtime_prim,KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
 "`(MODTIME *arg0* [*arg1*] [*arg2*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID,
 kno_any_type,KNO_VOID);
static lispval modtime_prim(lispval slotmap,lispval base,lispval togmt)
{
  lispval result;
  if (!(TABLEP(slotmap)))
    return kno_type_error("table","modtime_prim",slotmap);
  else if (VOIDP(base))
    result = timestamp_prim(VOID);
  else if (TYPEP(base,kno_timestamp_type))
    result = kno_deep_copy(base);
  else result = timestamp_prim(base);
  if (KNO_ABORTP(result))
    return result;
  else {
    struct U8_XTIME *xt=
      &((kno_consptr(struct KNO_TIMESTAMP *,result,kno_timestamp_type))->u8xtimeval);
    int tzoff = xt->u8_tzoff;
    lispval keys = kno_getkeys(slotmap);
    DO_CHOICES(key,keys) {
      lispval val = kno_get(slotmap,key,VOID);
      if (xtime_set(xt,key,val)<0) {
        result = KNO_ERROR;
        KNO_STOP_DO_CHOICES;
        break;}
      else {}}
    if (KNO_ABORTP(result)) return result;
    else if (FALSEP(togmt)) {
      time_t moment = u8_mktime(xt);
      u8_init_xtime(xt,moment,xt->u8_prec,xt->u8_nsecs,tzoff,0);
      return result;}
    else {
      time_t moment = u8_mktime(xt);
      u8_init_xtime(xt,moment,xt->u8_prec,xt->u8_nsecs,0,0);
      return result;}}
}

KNO_DEFPRIM("mktime",mktime_lexpr,KNO_VAR_ARGS|KNO_MIN_ARGS(0),
 "`(MKTIME *args...*)` **undocumented**");
static lispval mktime_lexpr(int n,lispval *args)
{
  lispval base; struct U8_XTIME *xt; int scan = 0;
  if (n%2) {
    lispval spec = args[0]; scan = 1;
    if (TYPEP(spec,kno_timestamp_type))
      base = kno_deep_copy(spec);
    else if ((FIXNUMP(spec))||(KNO_BIGINTP(spec))) {
      time_t moment = (time_t)
        ((FIXNUMP(spec))?(FIX2INT(spec)):
         (kno_bigint_to_long_long((kno_bigint)spec)));
      base = kno_time2timestamp(moment);}
    else return kno_type_error(_("time base"),"mktime_lexpr",spec);}
  else base = kno_make_timestamp(NULL);
  xt = &(((struct KNO_TIMESTAMP *)(base))->u8xtimeval);
  while (scan<n) {
    int rv = xtime_set(xt,args[scan],args[scan+1]);
    if (rv<0) {kno_decref(base); return KNO_ERROR;}
    else scan = scan+2;}
  return (lispval)base;
}

/* Miscellanous time utilities */

KNO_DEFPRIM("timestring",timestring,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
 "`(TIMESTRING)` **undocumented**");
static lispval timestring()
{
  struct U8_XTIME onstack; struct U8_OUTPUT out;
  u8_local_xtime(&onstack,-1,u8_second,0);
  U8_INIT_OUTPUT(&out,16);
  u8_printf(&out,"%02d:%02d:%02d",
            onstack.u8_hour,
            onstack.u8_min,
            onstack.u8_sec);
  return kno_stream2string(&out);
}

KNO_DEFPRIM("time",time_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
 "`(TIME)` **undocumented**");
static lispval time_prim()
{
  time_t now = time(NULL);
  if (now<0) {
    u8_graberrno("time_prim",NULL);
    return KNO_ERROR;}
  else return KNO_INT(now);
}

KNO_DEFPRIM("millitime",millitime_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
 "`(MILLITIME)` **undocumented**");
static lispval millitime_prim()
{
  long long now = u8_millitime();
  if (now<0) {
    u8_graberrno("millitime_prim",NULL);
    return KNO_ERROR;}
  else return KNO_INT(now);
}

KNO_DEFPRIM("microtime",microtime_prim,KNO_MAX_ARGS(0)|KNO_MIN_ARGS(0),
 "`(MICROTIME)` **undocumented**");
static lispval microtime_prim()
{
  long long now = u8_microtime();
  if (now<0) {
    u8_graberrno("microtime_prim",NULL);
    return KNO_ERROR;}
  else return KNO_INT(now);
}

static lispval now_macro(lispval expr,kno_lexenv env,kno_stack ptr)
{
  lispval field = kno_get_arg(expr,1);
  lispval now = kno_make_timestamp(NULL);
  lispval v = (FALSEP(field)) ? (kno_incref(now)) :
    (kno_get(now,field,KNO_VOID));
  kno_decref(now);
  if ( (KNO_VOIDP(v)) || (KNO_EMPTYP(v)) )
    return KNO_FALSE;
  else return v;
}

/* Counting seconds */

KNO_DEFPRIM2("secs->string",secs2string,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
 "`(SECS->STRING *arg0* [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_INT(3));
static lispval secs2string(lispval secs,lispval prec_arg)
{
  struct U8_OUTPUT out;
  double precision =
    ((KNO_FLONUMP(prec_arg)) ? (KNO_FLONUM(prec_arg)) :
     (KNO_UINTP(prec_arg)) ? (FIX2INT(prec_arg)) :
     (FALSEP(prec_arg)) ? (-1) :
     (0));
  int elts = 0, done = 0;
  double seconds, reduce;
  int years, months, weeks, days, hours, minutes;
  if (FIXNUMP(secs))
    seconds = (double)FIX2INT(secs);
  else if (KNO_FLONUMP(secs))
    seconds = KNO_FLONUM(secs);
  else return kno_type_error(_("seconds"),"secs2string",secs);
  U8_INIT_OUTPUT(&out,64);
  if (seconds<0) {
    u8_printf(&out,"negative "); reduce = -seconds;}
  else if (seconds==0) {
    u8_free(out.u8_outbuf);
    return kno_mkstring("0 seconds");}
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
  else if (!(KNO_FLONUMP(secs)))  {
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
  return kno_stream2string(&out);
}

KNO_DEFPRIM1("secs->short",secs2short,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(SECS->SHORT *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval secs2short(lispval secs)
{
  struct U8_OUTPUT out;
  double seconds, reduce;
  int days, hours, minutes;
  if (FIXNUMP(secs))
    seconds = (double)FIX2INT(secs);
  else if (KNO_FLONUMP(secs))
    seconds = KNO_FLONUM(secs);
  else return kno_type_error(_("seconds"),"secs2string",secs);
  U8_INIT_OUTPUT(&out,64);
  if (seconds<0) {
    u8_printf(&out,"negative "); reduce = -seconds;}
  else if (seconds==0) {
    u8_free(out.u8_outbuf);
    return kno_mkstring("0 seconds");}
  else reduce = seconds;
  days = (int) (floor(reduce/(24*3600)));
  reduce = reduce-days*(3600*24);
  hours = (int) (floor(reduce/(3600)));
  reduce = reduce-hours*(3600);
  minutes = (int) (floor(reduce/60));
  reduce = reduce-minutes*60;

  if (days>0) u8_printf(&out,"%dd-",days);
  if ((days==0)&&(hours==0)&&(minutes==0))
    if (reduce>=10)
      u8_printf(&out,"%.2d:%0.2d:%f",hours,minutes,reduce);
    else u8_printf(&out,"%.2d:%0.2d:0%f",hours,minutes,reduce);
  else u8_printf(&out,"%.2d:%0.2d:%0.2d",hours,minutes,(int)floor(reduce));
  return kno_stream2string(&out);
}

/* Sleeping */

#if ((HAVE_SLEEP) || (HAVE_NANOSLEEP))
KNO_DEFPRIM1("sleep",sleep_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(SLEEP *arg0*)` **undocumented**",
 kno_any_type,KNO_VOID);
lispval sleep_prim(lispval arg)
{
  if (FIXNUMP(arg)) {
    long long ival = FIX2INT(arg);
    if (ival<0)
      return kno_type_error(_("positive fixnum time interval"),"sleep_prim",arg);
    sleep(ival);
    return KNO_TRUE;}
  else if (KNO_FLONUMP(arg)) {
#if HAVE_NANOSLEEP
    double interval = KNO_FLONUM(arg);
    struct timespec req;
    if (interval<0)
      return kno_type_error(_("positive time interval"),"sleep_prim",arg);
    req.tv_sec = floor(interval);
    req.tv_nsec = 1000000000*(interval-req.tv_sec);
    nanosleep(&req,NULL);
    return KNO_TRUE;
#else
    double floval = KNO_FLONUM(arg);
    double secs = ceil(floval);
    int ival = (int)secs;
    if (ival<0)
      return kno_type_error(_("positive time interval"),"sleep_prim",arg);
    if (ival!=(int)floval)
      u8_log(LOG_WARN,"UnsupportedSleepPrecision",
             "This system doesnt' have fine-grained sleep precision");
    sleep(ival);
    return KNO_TRUE;
#endif
  }
  else return kno_type_error(_("time interval"),"sleep_prim",arg);
}
#endif

/* Initialization */

static void init_id_tables()
{
  dowids[0]=kno_intern("sun");
  dowids[1]=kno_intern("mon");
  dowids[2]=kno_intern("tue");
  dowids[3]=kno_intern("wed");
  dowids[4]=kno_intern("thu");
  dowids[5]=kno_intern("fri");
  dowids[6]=kno_intern("sat");
  monthids[0]=kno_intern("jan");
  monthids[1]=kno_intern("feb");
  monthids[2]=kno_intern("mar");
  monthids[3]=kno_intern("apr");
  monthids[4]=kno_intern("may");
  monthids[5]=kno_intern("jun");
  monthids[6]=kno_intern("jul");
  monthids[7]=kno_intern("aug");
  monthids[8]=kno_intern("sep");
  monthids[9]=kno_intern("oct");
  monthids[10]=kno_intern("nov");
  monthids[11]=kno_intern("dec");
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

KNO_DEFPRIM1("uuid?",uuidp_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(0),
 "`(UUID? [*arg0*])` **undocumented**",
 kno_any_type,KNO_VOID);
static lispval uuidp_prim(lispval x)
{
  if (TYPEP(x,kno_uuid_type)) return KNO_TRUE;
  else return KNO_FALSE;
}

KNO_DEFPRIM2("getuuid",getuuid_prim,KNO_MAX_ARGS(2)|KNO_MIN_ARGS(0),
 "`(GETUUID [*arg0*] [*arg1*])` **undocumented**",
 kno_any_type,KNO_VOID,kno_any_type,KNO_VOID);
static lispval getuuid_prim(lispval nodeid,lispval tptr)
{
  struct U8_XTIME *xt = NULL;
  long long id = -1;
  if ((VOIDP(tptr))&&(STRINGP(nodeid))) {
    /* Assume it's a UUID string, so parse it. */
    struct KNO_UUID *uuid = u8_alloc(struct KNO_UUID);
    u8_string start = CSTRING(nodeid);
    KNO_INIT_CONS(uuid,kno_uuid_type);
    if ((start[0]==':')&&(start[1]=='#')&&
        (start[2]=='U')&&(isxdigit(start[3])))
      start = start+3;
    else if ((start[0]=='#')&&(start[1]=='U')&&(isxdigit(start[2])))
      start = start+2;
    else if ((start[0]=='U')&&(isxdigit(start[1])))
      start = start+1;
    else if (isxdigit(start[0])) {}
    else return kno_type_error("UUID string","getuuid_prim",nodeid);
    u8_parseuuid(start,(u8_uuid)&(uuid->uuid16));
    return LISP_CONS(uuid);}
  else if ((VOIDP(tptr))&&(PACKETP(nodeid)))
    if (KNO_PACKET_LENGTH(nodeid)==16) {
      struct KNO_UUID *uuid = u8_alloc(struct KNO_UUID);
      const unsigned char *data = KNO_PACKET_DATA(nodeid);
      KNO_INIT_CONS(uuid,kno_uuid_type);
      memcpy(&(uuid->uuid16),data,16);
      return LISP_CONS(uuid);}
    else return kno_type_error("UUID (16-byte packet)","getuuid_prim",nodeid);
  else if ((VOIDP(tptr))&&(TYPEP(nodeid,kno_uuid_type)))
    return kno_incref(nodeid);
  else if ((VOIDP(tptr))&&(TYPEP(nodeid,kno_timestamp_type))) {
    lispval tmp = tptr; tptr = nodeid; nodeid = tmp;}
  if ((VOIDP(tptr))&&(VOIDP(nodeid)))
    return kno_fresh_uuid(NULL);
  if (TYPEP(tptr,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *tstamp=
      kno_consptr(struct KNO_TIMESTAMP *,tptr,kno_timestamp_type);
    xt = &(tstamp->u8xtimeval);}
  if (FIXNUMP(nodeid))
    id = ((long long)(FIX2INT(nodeid)));
  else if (KNO_BIGINTP(nodeid))
    id = kno_bigint2int64((kno_bigint)nodeid);
  else if (VOIDP(nodeid)) id = -1;
  else return kno_type_error("node id","getuuid_prim",nodeid);
  return kno_cons_uuid(NULL,xt,id,-1);
}

KNO_DEFPRIM1("uuid-time",uuidtime_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(UUID-TIME *arg0*)` **undocumented**",
 kno_uuid_type,KNO_VOID);
static lispval uuidtime_prim(lispval uuid_arg)
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,uuid_arg,kno_uuid_type);
  struct KNO_TIMESTAMP *tstamp = u8_alloc(struct KNO_TIMESTAMP);
  KNO_INIT_CONS(tstamp,kno_timestamp_type);
  if (u8_uuid_xtime(uuid->uuid16,&(tstamp->u8xtimeval)))
    return LISP_CONS(tstamp);
  else {
    u8_free(tstamp);
    return kno_type_error("time-based UUID","uuidtime_prim",uuid_arg);}
}

KNO_DEFPRIM1("uuid-node",uuidnode_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(UUID-NODE *arg0*)` **undocumented**",
 kno_uuid_type,KNO_VOID);
static lispval uuidnode_prim(lispval uuid_arg)
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,uuid_arg,kno_uuid_type);
  long long id = u8_uuid_nodeid(uuid->uuid16);
  if (id<0)
    return kno_type_error("time-based UUID","uuidnode_prim",uuid_arg);
  else return KNO_INT(id);
}

KNO_DEFPRIM1("uuid->string",uuidstring_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(UUID->STRING *arg0*)` **undocumented**",
 kno_uuid_type,KNO_VOID);
static lispval uuidstring_prim(lispval uuid_arg)
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,uuid_arg,kno_uuid_type);
  return kno_init_string(NULL,36,u8_uuidstring(uuid->uuid16,NULL));
}

KNO_DEFPRIM1("uuid->packet",uuidpacket_prim,KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
 "`(UUID->PACKET *arg0*)` **undocumented**",
 kno_uuid_type,KNO_VOID);
static lispval uuidpacket_prim(lispval uuid_arg)
{
  struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,uuid_arg,kno_uuid_type);
  return kno_make_packet(NULL,16,uuid->uuid16);
}

/* Initialization */

KNO_EXPORT void kno_init_timeprims_c()
{
  u8_register_source_file(_FILEINFO);

  tzset();

  init_id_tables();

  kno_tablefns[kno_timestamp_type]=u8_zalloc(struct KNO_TABLEFNS);
  kno_tablefns[kno_timestamp_type]->get = timestamp_get;
  kno_tablefns[kno_timestamp_type]->add = NULL;
  kno_tablefns[kno_timestamp_type]->drop = NULL;
  kno_tablefns[kno_timestamp_type]->store = timestamp_store;
  kno_tablefns[kno_timestamp_type]->test = NULL;
  kno_tablefns[kno_timestamp_type]->keys = timestamp_getkeys;

  year_symbol = kno_intern("year");
  CHOICE_ADD(xtime_keys,year_symbol);
  month_symbol = kno_intern("month");
  CHOICE_ADD(xtime_keys,month_symbol);
  date_symbol = kno_intern("date");
  CHOICE_ADD(xtime_keys,date_symbol);
  hours_symbol = kno_intern("hours");
  CHOICE_ADD(xtime_keys,hours_symbol);
  minutes_symbol = kno_intern("minutes");
  CHOICE_ADD(xtime_keys,minutes_symbol);
  seconds_symbol = kno_intern("seconds");
  CHOICE_ADD(xtime_keys,seconds_symbol);
  precision_symbol = kno_intern("precision");
  CHOICE_ADD(xtime_keys,precision_symbol);
  tzoff_symbol = kno_intern("tzoff");
  CHOICE_ADD(xtime_keys,tzoff_symbol);
  dstoff_symbol = kno_intern("dstoff");
  CHOICE_ADD(xtime_keys,dstoff_symbol);
  gmtoff_symbol = kno_intern("gmtoff");
  CHOICE_ADD(xtime_keys,gmtoff_symbol);

  milliseconds_symbol = kno_intern("milliseconds");
  CHOICE_ADD(xtime_keys,milliseconds_symbol);
  microseconds_symbol = kno_intern("microseconds");
  CHOICE_ADD(xtime_keys,microseconds_symbol);
  nanoseconds_symbol = kno_intern("nanoseconds");
  CHOICE_ADD(xtime_keys,nanoseconds_symbol);
  picoseconds_symbol = kno_intern("picoseconds");
  CHOICE_ADD(xtime_keys,picoseconds_symbol);
  femtoseconds_symbol = kno_intern("femtoseconds");
  CHOICE_ADD(xtime_keys,femtoseconds_symbol);

  tick_symbol = kno_intern("tick");
  CHOICE_ADD(xtime_keys,tick_symbol);
  prim_tick_symbol = kno_intern("%tick");
  CHOICE_ADD(xtime_keys,prim_tick_symbol);
  xtick_symbol = kno_intern("xtick");
  CHOICE_ADD(xtime_keys,xtick_symbol);
  iso_symbol = kno_intern("iso");
  CHOICE_ADD(xtime_keys,iso_symbol);
  isodate_symbol = kno_intern("isodate");
  CHOICE_ADD(xtime_keys,isodate_symbol);
  isobasic_symbol = kno_intern("isobasic");
  CHOICE_ADD(xtime_keys,isobasic_symbol);
  isobasicdate_symbol = kno_intern("isobasicdate");
  CHOICE_ADD(xtime_keys,isobasicdate_symbol);
  isostring_symbol = kno_intern("isostring");
  CHOICE_ADD(xtime_keys,isostring_symbol);
  iso8601_symbol = kno_intern("iso8601");
  CHOICE_ADD(xtime_keys,iso8601_symbol);
  rfc822_symbol = kno_intern("rfc822");
  CHOICE_ADD(xtime_keys,rfc822_symbol);
  rfc822date_symbol = kno_intern("rfc822date");
  CHOICE_ADD(xtime_keys,rfc822_symbol);
  rfc822x_symbol = kno_intern("rfc822x");
  CHOICE_ADD(xtime_keys,rfc822x_symbol);
  localstring_symbol = kno_intern("localstring");
  CHOICE_ADD(xtime_keys,localstring_symbol);
  utcstring_symbol = kno_intern("utcstring");
  CHOICE_ADD(xtime_keys,utcstring_symbol);

  time_of_day_symbol = kno_intern("time-of-day");
  CHOICE_ADD(xtime_keys,time_of_day_symbol);
  morning_symbol = kno_intern("morning");
  afternoon_symbol = kno_intern("afternoon");
  evening_symbol = kno_intern("evening");
  nighttime_symbol = kno_intern("nighttime");

  season_symbol = kno_intern("season");
  CHOICE_ADD(xtime_keys,season_symbol);
  spring_symbol = kno_intern("spring");
  summer_symbol = kno_intern("summer");
  autumn_symbol = kno_intern("autumn");
  winter_symbol = kno_intern("winter");

  shortmonth_symbol = kno_intern("month-short");
  CHOICE_ADD(xtime_keys,shortmonth_symbol);
  longmonth_symbol = kno_intern("month-long");
  CHOICE_ADD(xtime_keys,longmonth_symbol);
  shortday_symbol = kno_intern("weekday-short");
  CHOICE_ADD(xtime_keys,shortday_symbol);
  longday_symbol = kno_intern("weekday-long");
  CHOICE_ADD(xtime_keys,longday_symbol);
  hms_symbol = kno_intern("hms");
  CHOICE_ADD(xtime_keys,hms_symbol);
  dmy_symbol = kno_intern("dmy");
  CHOICE_ADD(xtime_keys,dmy_symbol);
  dm_symbol = kno_intern("dm");
  CHOICE_ADD(xtime_keys,dm_symbol);
  my_symbol = kno_intern("my");
  CHOICE_ADD(xtime_keys,my_symbol);
  string_symbol = kno_intern("string");
  CHOICE_ADD(xtime_keys,string_symbol);
  shortstring_symbol = kno_intern("shortstring");
  CHOICE_ADD(xtime_keys,shortstring_symbol);
  short_symbol = kno_intern("short");
  CHOICE_ADD(xtime_keys,short_symbol);
  timestring_symbol = kno_intern("timestring");
  CHOICE_ADD(xtime_keys,timestring_symbol);
  datestring_symbol = kno_intern("datestring");
  CHOICE_ADD(xtime_keys,datestring_symbol);
  fullstring_symbol = kno_intern("fullstring");
  CHOICE_ADD(xtime_keys,fullstring_symbol);

  dowid_symbol = kno_intern("dowid");
  CHOICE_ADD(xtime_keys,dowid_symbol);
  monthid_symbol = kno_intern("monthid");
  CHOICE_ADD(xtime_keys,monthid_symbol);

  timezone_symbol = kno_intern("timezone");
  CHOICE_ADD(xtime_keys,timezone_symbol);

  years_symbol=kno_intern("years");
  months_symbol=kno_intern("months");
  day_symbol=kno_intern("day");
  days_symbol=kno_intern("days");
  hour_symbol=kno_intern("hour");
  minute_symbol=kno_intern("minute");
  second_symbol=kno_intern("second");
  millisecond_symbol=kno_intern("millisecond");
  microsecond_symbol=kno_intern("microsecond");
  nanosecond_symbol=kno_intern("nanosecond");
  picosecond_symbol=kno_intern("picosecond");
  femtosecond_symbol=kno_intern("femtosecond");

  gmt_symbol = kno_intern("gmt");

  init_local_cprims();

  kno_def_evalfn(kno_sys_module,"#NOW",
                 "#:NOW:YEAR\n evaluates to a field of the current time",
                 now_macro);

#if 0 /* ((HAVE_SLEEP) || (HAVE_NANOSLEEP)) */
  kno_idefn(kno_sys_module,kno_make_cprim1("SLEEP",sleep_prim,1));
#endif
}


static void init_local_cprims()
{
  KNO_LINK_PRIM("uuid->packet",uuidpacket_prim,1,kno_sys_module);
  KNO_LINK_PRIM("uuid->string",uuidstring_prim,1,kno_sys_module);
  KNO_LINK_PRIM("uuid-node",uuidnode_prim,1,kno_sys_module);
  KNO_LINK_PRIM("uuid-time",uuidtime_prim,1,kno_sys_module);
  KNO_LINK_PRIM("getuuid",getuuid_prim,2,kno_sys_module);
  KNO_LINK_PRIM("uuid?",uuidp_prim,1,kno_sys_module);
  KNO_LINK_PRIM("sleep",sleep_prim,1,kno_sys_module);
  KNO_LINK_PRIM("secs->short",secs2short,1,kno_sys_module);
  KNO_LINK_PRIM("secs->string",secs2string,2,kno_sys_module);
  KNO_LINK_PRIM("microtime",microtime_prim,0,kno_sys_module);
  KNO_LINK_PRIM("millitime",millitime_prim,0,kno_sys_module);
  KNO_LINK_PRIM("time",time_prim,0,kno_sys_module);
  KNO_LINK_PRIM("timestring",timestring,0,kno_sys_module);
  KNO_LINK_VARARGS("mktime",mktime_lexpr,kno_sys_module);
  KNO_LINK_PRIM("modtime",modtime_prim,3,kno_sys_module);
  KNO_LINK_PRIM("elapsed-time",elapsed_time,1,kno_sys_module);
  KNO_LINK_PRIM("past?",pastp,2,kno_sys_module);
  KNO_LINK_PRIM("future?",futurep,2,kno_sys_module);
  KNO_LINK_PRIM("time<?",timestamp_lesser,2,kno_sys_module);
  KNO_LINK_PRIM("time>?",timestamp_greater,2,kno_sys_module);
  KNO_LINK_PRIM("time-since",time_since,1,kno_sys_module);
  KNO_LINK_PRIM("time-until",time_until,1,kno_sys_module);
  KNO_LINK_PRIM("difftime",timestamp_diff,2,kno_sys_module);
  KNO_LINK_PRIM("timestamp-",timestamp_minus,2,kno_sys_module);
  KNO_LINK_PRIM("timestamp+",timestamp_plus,2,kno_sys_module);
  KNO_LINK_PRIM("gmtimestamp",gmtimestamp_prim,1,kno_sys_module);
  KNO_LINK_PRIM("timestamp",timestamp_prim,1,kno_sys_module);
  KNO_LINK_PRIM("timestamp?",timestampp,1,kno_sys_module);

  KNO_LINK_ALIAS("time-earlier?",timestamp_lesser,kno_sys_module);
  KNO_LINK_ALIAS("time-later?",timestamp_greater,kno_sys_module);
  KNO_LINK_ALIAS("time+",timestamp_plus,kno_sys_module);
  KNO_LINK_ALIAS("time-",timestamp_minus,kno_sys_module);
}
