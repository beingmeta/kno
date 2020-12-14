/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2020 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/storage.h"
#include "kno/pools.h"
#include "kno/indexes.h"
#include "kno/frames.h"
#include "kno/numbers.h"
#include "kno/knoregex.h"
#include "kno/cprims.h"

#include <libu8/libu8io.h>

#include <sys/types.h>


u8_condition kno_RegexBadOp=_("Invalid Regex operation");


DEFCPRIM("regex",make_regex,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	 "`(REGEX *arg0* [*arg1*] [*arg2*])` "
	 "**undocumented**",
	 {"pat",kno_string_type,KNO_VOID},
	 {"nocase",kno_any_type,KNO_FALSE},
	 {"matchnl",kno_any_type,KNO_FALSE})
static lispval make_regex(lispval pat,lispval nocase,lispval matchnl)
{
  int cflags = REG_EXTENDED;
  if (!(FALSEP(nocase))) cflags = cflags|REG_ICASE;
  if (!(FALSEP(matchnl))) cflags = cflags|REG_NEWLINE;
  return kno_make_regex(CSTRING(pat),cflags);
}


DEFCPRIM("regex?",regexp_prim,
	 KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	 "`(REGEX? *arg0*)` "
	 "**undocumented**",
	 {"x",kno_any_type,KNO_VOID})
static lispval regexp_prim(lispval x)
{
  if (TYPEP(x,kno_regex_type))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static lispval getcharoff(u8_string s,int byteoff)
{
  int charoff = u8_charoffset(s,byteoff);
  return KNO_INT(charoff);
}

static lispval regex_searchop(enum KNO_REGEX_OP op,
			      lispval pat,lispval string,
			      int eflags)
{
  struct KNO_REGEX *ptr = kno_consptr(struct KNO_REGEX *,pat,kno_regex_type);
  regmatch_t results[1];
  int retval, len = STRLEN(string);
  u8_string s = CSTRING(string);
  /* Convert numeric eflags value to correct flags field */
  if (eflags==1)
    eflags = REG_NOTBOL;
  else if (eflags==2)
    eflags = REG_NOTEOL;
  else if (eflags==3)
    eflags = REG_NOTEOL|REG_NOTBOL;
  else eflags = 0;
  u8_lock_mutex(&(ptr->rx_lock));
  retval = regexec(&(ptr->rxcompiled),CSTRING(string),1,results,eflags);
  if (retval == REG_NOMATCH) {
    u8_unlock_mutex(&(ptr->rx_lock));
    U8_CLEAR_ERRNO();
    return KNO_FALSE;}
  else if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->rxcompiled),buf,512);
    u8_unlock_mutex(&(ptr->rx_lock));
    if (errno) u8_graberrno("regex_search",NULL);
    return kno_err(kno_RegexError,"regex_search",u8_strdup(buf),VOID);}
  else {
    U8_CLEAR_ERRNO();
    u8_unlock_mutex(&(ptr->rx_lock));}
  if (results[0].rm_so<0)
    return KNO_FALSE;
  else switch (op) {
    case rx_search:
      return getcharoff(s,results[0].rm_so);
    case rx_exactmatch:
      if ((results[0].rm_so==0)&&(results[0].rm_eo == len))
	return KNO_TRUE;
      else return KNO_FALSE;
    case rx_matchlen:
      if (results[0].rm_so==0)
	return getcharoff(s,results[0].rm_eo);
      else return KNO_FALSE;
    case rx_matchstring:
      return kno_extract_string
	(NULL,CSTRING(string)+results[0].rm_so,
	 CSTRING(string)+results[0].rm_eo);
    case rx_matchspan:
      return kno_conspair(getcharoff(s,results[0].rm_so),
			  getcharoff(s,results[0].rm_eo));
    default: return KNO_FALSE;}
}

KNO_EXPORT ssize_t kno_regex_op(enum KNO_REGEX_OP op,lispval pat,
				u8_string s,size_t len,
				int eflags)
{
  if ( ! (KNO_TYPEP(pat,kno_regex_type)) ) {
    kno_seterr(kno_TypeError,"kno_regex_op","regex",pat);
    return -2;}
  else NO_ELSE;
  struct KNO_REGEX *ptr = kno_consptr(struct KNO_REGEX *,pat,kno_regex_type);
  regmatch_t results[1] = { { 0 } };
  int retval;
  if (len < 0) len = strlen(s);
  u8_lock_mutex(&(ptr->rx_lock));
  retval = regexec(&(ptr->rxcompiled),s,1,results,eflags);
  if (retval == REG_NOMATCH) {
    u8_unlock_mutex(&(ptr->rx_lock));
    U8_CLEAR_ERRNO();
    return -1;}
  else if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->rxcompiled),buf,512);
    u8_unlock_mutex(&(ptr->rx_lock));
    if (errno) u8_graberrno("regex_search",NULL);
    kno_seterr(kno_RegexError,"kno_regex_op",buf,VOID);
    return -2;}
  else {
    u8_unlock_mutex(&(ptr->rx_lock));
    U8_CLEAR_ERRNO();}
  u8_string op_string = NULL;
  if (results[0].rm_so<0)
    return -1;
  else switch (op) {
    case rx_search:
      return results[0].rm_so;
    case rx_exactmatch:
      if ((results[0].rm_so==0)&&(results[0].rm_eo == len))
	return 1;
      else return 0;
    case rx_matchlen:
      if ((results[0].rm_so==0)&&
	  (results[0].rm_eo<=len))
	return results[0].rm_eo;
      else return -1;
    case rx_matchstring:
      op_string = "rx_matchstring"; break;
    case rx_matchspan:
      op_string = "rx_matchspan"; break;
    default: {
      op_string = "badop"; break;}}
  kno_seterr(kno_RegexBadOp,"kno_regex_op",op_string,pat);
  return -2;
}

KNO_EXPORT int kno_regex_test(lispval pat,u8_string s,ssize_t len)
{
  if (len<0) len = strlen(s);
  if (kno_regex_op(rx_search,pat,s,len,0)>=0)
    return 1;
  else return 0;
}
KNO_EXPORT off_t kno_regex_search(lispval pat,u8_string s,ssize_t len)
{
  if (len<0) len = strlen(s);
  return kno_regex_op(rx_search,pat,s,len,0);
}
KNO_EXPORT ssize_t kno_regex_match(lispval pat,u8_string s,ssize_t len)
{
  if (len<0) len = strlen(s);
  return kno_regex_op(rx_exactmatch,pat,s,len,0);
}
KNO_EXPORT ssize_t kno_regex_matchlen(lispval pat,u8_string s,ssize_t len)
{
  if (len<0) len = strlen(s);
  return kno_regex_op(rx_matchlen,pat,s,len,0);
}


DEFCPRIM("regex/search",regex_search,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(REGEX/SEARCH *arg0* *arg1* [*arg2*])` "
	 "**undocumented**",
	 {"pat",kno_regex_type,KNO_VOID},
	 {"string",kno_string_type,KNO_VOID},
	 {"ef",kno_fixnum_type,KNO_INT(0)})
static lispval regex_search(lispval pat,lispval string,lispval ef)
{
  if (KNO_UINTP(ef))
    return regex_searchop(rx_search,pat,string,FIX2INT(ef));
  else return kno_type_error("unsigned int","regex_search/flags",ef);
}

DEFCPRIM("regex/matchlen",regex_matchlen,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(REGEX/MATCHLEN *arg0* *arg1* [*arg2*])` "
	 "**undocumented**",
	 {"pat",kno_regex_type,KNO_VOID},
	 {"string",kno_string_type,KNO_VOID},
	 {"ef",kno_fixnum_type,KNO_INT(0)})
static lispval regex_matchlen(lispval pat,lispval string,lispval ef)
{
  if (KNO_UINTP(ef))
    return regex_searchop(rx_matchlen,pat,string,FIX2INT(ef));
  else return kno_type_error("unsigned int","regex_matchlen/flags",ef);
}

DEFCPRIM("regex/match",regex_exactmatch,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(REGEX/MATCH *arg0* *arg1* [*arg2*])` "
	 "**undocumented**",
	 {"pat",kno_regex_type,KNO_VOID},
	 {"string",kno_string_type,KNO_VOID},
	 {"ef",kno_fixnum_type,KNO_INT(0)})
static lispval regex_exactmatch(lispval pat,lispval string,lispval ef)
{
  if (KNO_UINTP(ef))
    return regex_searchop(rx_exactmatch,pat,string,FIX2INT(ef));
  else return kno_type_error("unsigned int","regex_exactmatch/flags",ef);
}

DEFCPRIM("regex/matchstring",regex_matchstring,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(REGEX/MATCHSTRING *arg0* *arg1* [*arg2*])` "
	 "**undocumented**",
	 {"pat",kno_regex_type,KNO_VOID},
	 {"string",kno_string_type,KNO_VOID},
	 {"ef",kno_fixnum_type,KNO_INT(0)})
static lispval regex_matchstring(lispval pat,lispval string,lispval ef)
{
  if (KNO_UINTP(ef))
    return regex_searchop(rx_matchstring,pat,string,FIX2INT(ef));
  else return kno_type_error("unsigned int","regex_matchstring/flags",ef);
}

DEFCPRIM("regex/matchspan",regex_matchspan,
	 KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	 "`(REGEX/MATCHSPAN *arg0* *arg1* [*arg2*])` "
	 "**undocumented**",
	 {"pat",kno_regex_type,KNO_VOID},
	 {"string",kno_string_type,KNO_VOID},
	 {"ef",kno_fixnum_type,KNO_INT(0)})
static lispval regex_matchspan(lispval pat,lispval string,lispval ef)
{
  if (KNO_UINTP(ef))
    return regex_searchop(rx_matchspan,pat,string,FIX2INT(ef));
  else return kno_type_error("unsigned int","regex_matchspan/flags",ef);
}

/* Initialization */

static int regex_init = 0;

static lispval regex_module;

KNO_EXPORT int kno_init_regex_c()
{
  if (regex_init) return 0;

  regex_init = 1;
  regex_module = kno_new_cmodule("regex",0,kno_init_regex_c);

  link_local_cprims();

  kno_finish_module(regex_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("regex/matchspan",regex_matchspan,3,regex_module);
  KNO_LINK_CPRIM("regex/matchstring",regex_matchstring,3,regex_module);
  KNO_LINK_CPRIM("regex/match",regex_exactmatch,3,regex_module);
  KNO_LINK_CPRIM("regex/matchlen",regex_matchlen,3,regex_module);
  KNO_LINK_CPRIM("regex/search",regex_search,3,regex_module);
  KNO_LINK_CPRIM("regex?",regexp_prim,1,regex_module);
  KNO_LINK_CPRIM("regex",make_regex,3,regex_module);
}
