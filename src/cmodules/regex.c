/* -*- Mode: C; -*- */

/* Copyright (C) 2007-2012 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/fddb.h"
#include "framerd/pools.h"
#include "framerd/indices.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"

#include <libu8/libu8io.h>

#include <sys/types.h>
#include <regex.h>

fd_exception fd_RegexError=_("Regular expression error");

typedef struct FD_REGEX {
  FD_CONS_HEADER;
  u8_string src; int flags;
  u8_mutex lock; int active;
  regex_t compiled;} FD_REGEX;
typedef struct FD_REGEX *fd_regex;

FD_EXPORT fd_ptr_type fd_regex_type;
fd_ptr_type fd_regex_type;

static fdtype make_regex(fdtype pat,fdtype nocase,fdtype matchnl)
{
  struct FD_REGEX *ptr=u8_alloc(struct FD_REGEX);
  int retval, cflags=REG_EXTENDED;
  u8_string src=u8_strdup(FD_STRDATA(pat));
  FD_INIT_FRESH_CONS(ptr,fd_regex_type);
  if (!(FD_FALSEP(nocase))) cflags=cflags|REG_ICASE;
  if (!(FD_FALSEP(matchnl))) cflags=cflags|REG_NEWLINE;
  retval=regcomp(&(ptr->compiled),src,cflags);
  if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->compiled),buf,512);
    u8_free(ptr);
    return fd_err(fd_RegexError,"fd_make_regex",u8_strdup(buf),FD_VOID);}
  else {
    ptr->flags=cflags; ptr->src=src;
    u8_init_mutex(&(ptr->lock)); ptr->active=1;
    return FDTYPE_CONS(ptr);}
}

static void recycle_regex(struct FD_CONS *c)
{
  struct FD_REGEX *rx=(struct FD_REGEX *)c;
  regfree(&(rx->compiled));
  u8_destroy_mutex(&(rx->lock));
  u8_free(rx);
}

static int unparse_regex(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_REGEX *rx=(struct FD_REGEX *)x;
  u8_printf(out,"#<REGEX /%s/%s%s%s%s>",rx->src,
	    (((rx->flags)&REG_EXTENDED)?"e":""),
	    (((rx->flags)&REG_ICASE)?"c":""),
	    (((rx->flags)&REG_ICASE)?"l":""),
	    (((rx->flags)&REG_NOSUB)?"s":""));
  return 1;
}

enum SEARCHOP {
  rx_search, rx_zeromatch, rx_matchlen, rx_exactmatch, rx_matchpair,
  rx_matchstring};

static fdtype getcharoff(u8_string s,int byteoff)
{
  int charoff=u8_charoffset(s,byteoff);
  return FD_INT2DTYPE(charoff);
}

static fdtype regex_searchop(enum SEARCHOP op,fdtype pat,fdtype string)
{
  struct FD_REGEX *ptr=FD_GET_CONS(pat,fd_regex_type,struct FD_REGEX *);
  regmatch_t results[1];
  int retval, len=FD_STRLEN(string);
  u8_string s=FD_STRDATA(string);
  retval=regexec(&(ptr->compiled),FD_STRDATA(string),1,results,0);
  u8_lock_mutex(&(ptr->lock));
  if (retval==REG_NOMATCH) return FD_FALSE;
  else if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->compiled),buf,512);
    u8_unlock_mutex(&(ptr->lock));
    return fd_err(fd_RegexError,"regex_search",u8_strdup(buf),FD_VOID);}
  else u8_unlock_mutex(&(ptr->lock));
  if (results[0].rm_so<0) return FD_FALSE;
  else switch (op) {
    case rx_search: return getcharoff(s,results[0].rm_so);;
    case rx_exactmatch:
      if ((results[0].rm_so==0)&&(results[0].rm_eo==len))
	return FD_TRUE;
      else return FD_FALSE;
    case rx_matchlen:
      if (results[0].rm_so==0)
	return getcharoff(s,results[0].rm_eo);
      else return FD_FALSE;
    case rx_matchstring:
      return fd_extract_string
	(NULL,FD_STRDATA(string)+results[0].rm_so,
	 FD_STRDATA(string)+results[0].rm_eo);
    case rx_matchpair:
      return fd_init_pair
	(NULL,getcharoff(s,results[0].rm_so),
	 getcharoff(s,results[0].rm_eo));
    default: return FD_FALSE;}
}

static fdtype regex_search(fdtype pat,fdtype string)
{
  return regex_searchop(rx_search,pat,string);
}
static fdtype regex_matchlen(fdtype pat,fdtype string)
{
  return regex_searchop(rx_matchlen,pat,string);
}
static fdtype regex_exactmatch(fdtype pat,fdtype string)
{
  return regex_searchop(rx_exactmatch,pat,string);
}
static fdtype regex_matchstring(fdtype pat,fdtype string)
{
  return regex_searchop(rx_matchstring,pat,string);
}
static fdtype regex_matchpair(fdtype pat,fdtype string)
{
  return regex_searchop(rx_matchstring,pat,string);
}

/* Initialization */

FD_EXPORT int fd_init_regex(void) FD_LIBINIT_FN;

static int regex_init=0;

FD_EXPORT int fd_init_regex()
{
  fdtype regex_module;
  if (regex_init) return 0;
  fd_register_source_file(_FILEINFO);
  regex_init=1;
  regex_module=fd_new_module("REGEX",(FD_MODULE_SAFE));
  
  fd_regex_type=fd_register_cons_type("REGEX");
  
  fd_unparsers[fd_regex_type]=unparse_regex;
  fd_recyclers[fd_regex_type]=recycle_regex;

  fd_idefn(regex_module,
	   fd_make_cprim3x("REGEX",make_regex,1,
			   fd_string_type,FD_VOID,-1,FD_FALSE,-1,FD_FALSE));
  fd_idefn(regex_module,
	   fd_make_cprim2x("REGEX/SEARCH",regex_search,2,
			   fd_regex_type,FD_VOID,fd_string_type,FD_VOID));
  fd_idefn(regex_module,
	   fd_make_cprim2x("REGEX/MATCH",regex_exactmatch,2,
			   fd_regex_type,FD_VOID,fd_string_type,FD_VOID));
  fd_idefn(regex_module,
	   fd_make_cprim2x("REGEX/MATCHLEN",regex_matchlen,2,
			   fd_regex_type,FD_VOID,fd_string_type,FD_VOID));
  fd_idefn(regex_module,
	   fd_make_cprim2x("REGEX/MATCHSTRING",regex_matchstring,2,
			   fd_regex_type,FD_VOID,fd_string_type,FD_VOID));
  fd_idefn(regex_module,
	   fd_make_cprim2x("REGEX/MATCHPAIR",regex_matchpair,2,
			   fd_regex_type,FD_VOID,fd_string_type,FD_VOID));

  fd_finish_module(regex_module);
  fd_persist_module(regex_module);
  return 1;
}

