/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2007-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/storage.h"
#include "framerd/pools.h"
#include "framerd/indexes.h"
#include "framerd/frames.h"
#include "framerd/numbers.h"
#include "framerd/fdregex.h"

#include <libu8/libu8io.h>

#include <sys/types.h>

u8_condition fd_RegexBadOp=_("Invalid Regex operation");

static lispval make_regex(lispval pat,lispval nocase,lispval matchnl)
{
  struct FD_REGEX *ptr = u8_alloc(struct FD_REGEX);
  int retval, cflags = REG_EXTENDED;
  u8_string src = u8_strdup(CSTRING(pat));
  FD_INIT_FRESH_CONS(ptr,fd_regex_type);
  if (!(FALSEP(nocase))) cflags = cflags|REG_ICASE;
  if (!(FALSEP(matchnl))) cflags = cflags|REG_NEWLINE;
  retval = regcomp(&(ptr->rxcompiled),src,cflags);
  if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->rxcompiled),buf,512);
    u8_free(ptr);
    return fd_err(fd_RegexError,"fd_make_regex",u8_strdup(buf),VOID);}
  else {
    ptr->rxflags = cflags; ptr->rxsrc = src;
    u8_init_mutex(&(ptr->rx_lock)); ptr->rxactive = 1;
    return LISP_CONS(ptr);}
}

static lispval regexp_prim(lispval x)
{
  if (TYPEP(x,fd_regex_type)) return FD_TRUE;
  else return FD_FALSE;
}

static lispval getcharoff(u8_string s,int byteoff)
{
  int charoff = u8_charoffset(s,byteoff);
  return FD_INT(charoff);
}

static lispval regex_searchop(enum FD_REGEX_OP op,
                              lispval pat,lispval string,
                              int eflags)
{
  struct FD_REGEX *ptr = fd_consptr(struct FD_REGEX *,pat,fd_regex_type);
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
    return FD_FALSE;}
  else if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->rxcompiled),buf,512);
    u8_unlock_mutex(&(ptr->rx_lock));
    return fd_err(fd_RegexError,"regex_search",u8_strdup(buf),VOID);}
  else u8_unlock_mutex(&(ptr->rx_lock));
  if (results[0].rm_so<0)
    return FD_FALSE;
  else switch (op) {
    case rx_search:
      return getcharoff(s,results[0].rm_so);
    case rx_exactmatch:
      if ((results[0].rm_so==0)&&(results[0].rm_eo == len))
        return FD_TRUE;
      else return FD_FALSE;
    case rx_matchlen:
      if (results[0].rm_so==0)
        return getcharoff(s,results[0].rm_eo);
      else return FD_FALSE;
    case rx_matchstring:
      return fd_extract_string
        (NULL,CSTRING(string)+results[0].rm_so,
         CSTRING(string)+results[0].rm_eo);
    case rx_matchpair:
      return fd_conspair(getcharoff(s,results[0].rm_so),
                         getcharoff(s,results[0].rm_eo));
    default: return FD_FALSE;}
}

FD_EXPORT ssize_t fd_regex_op(enum FD_REGEX_OP op,lispval pat,
                              u8_string s,size_t len,
                              int eflags)
{
  struct FD_REGEX *ptr = fd_consptr(struct FD_REGEX *,pat,fd_regex_type);
  regmatch_t results[1];
  int retval;
  /* Convert numeric eflags value to correct flags field */
  if (eflags==1)
    eflags = REG_NOTBOL;
  else if (eflags==2)
    eflags = REG_NOTEOL;
  else if (eflags==3)
    eflags = REG_NOTEOL|REG_NOTBOL;
  else eflags = 0;
  u8_lock_mutex(&(ptr->rx_lock));
  retval = regexec(&(ptr->rxcompiled),s,1,results,eflags);
  if (retval == REG_NOMATCH) {
    u8_unlock_mutex(&(ptr->rx_lock));
    return -1;}
  else if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->rxcompiled),buf,512);
    u8_unlock_mutex(&(ptr->rx_lock));
    fd_seterr(fd_RegexError,"fd_regex_op",buf,VOID);
    return -2;}
  else u8_unlock_mutex(&(ptr->rx_lock));
  if (results[0].rm_so<0) return -1;
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
    case rx_matchstring: {
      fd_seterr(fd_RegexBadOp,"fd_regex_op","rx_matchstring",pat);
      fd_incref(pat);
      return -2;}
    case rx_matchpair: {
      fd_seterr(fd_RegexBadOp,"fd_regex_op","rx_matchpair",pat);
      fd_incref(pat);
      return -2;}
    default: {
      fd_seterr(fd_RegexBadOp,"fd_regex_op","badop",pat);
      fd_incref(pat);
      return -2;}}
}

FD_EXPORT int fd_regex_test(lispval pat,u8_string s,ssize_t len)
 {
   if (len<0) len = strlen(s);
   if (fd_regex_op(rx_search,pat,s,len,0)>=0)
     return 1;
   else return 0;
}
FD_EXPORT off_t fd_regex_search(lispval pat,u8_string s,ssize_t len)
{
  if (len<0) len = strlen(s);
  return fd_regex_op(rx_search,pat,s,len,0);
}
FD_EXPORT ssize_t fd_regex_match(lispval pat,u8_string s,ssize_t len)
{
  if (len<0) len = strlen(s);
  return fd_regex_op(rx_exactmatch,pat,s,len,0);
}
FD_EXPORT ssize_t fd_regex_matchlen(lispval pat,u8_string s,ssize_t len)
{
  if (len<0) len = strlen(s);
  return fd_regex_op(rx_matchlen,pat,s,len,0);
}

static lispval regex_search(lispval pat,lispval string,lispval ef)
{
  if (FD_UINTP(ef))
    return regex_searchop(rx_search,pat,string,FIX2INT(ef));
  else return fd_type_error("unsigned int","regex_search",ef);
}
static lispval regex_matchlen(lispval pat,lispval string,lispval ef)
{
  if (FD_UINTP(ef))
    return regex_searchop(rx_matchlen,pat,string,FIX2INT(ef));
  else return fd_type_error("unsigned int","regex_matchlen",ef);
}
static lispval regex_exactmatch(lispval pat,lispval string,lispval ef)
{
  if (FD_UINTP(ef))
    return regex_searchop(rx_exactmatch,pat,string,FIX2INT(ef));
  else return fd_type_error("unsigned int","regex_exactmatch",ef);
}
static lispval regex_matchstring(lispval pat,lispval string,lispval ef)
{
  if (FD_UINTP(ef))
    return regex_searchop(rx_matchstring,pat,string,FIX2INT(ef));
  else return fd_type_error("unsigned int","regex_matchstring",ef);
}
static lispval regex_matchpair(lispval pat,lispval string,lispval ef)
{
  if (FD_UINTP(ef))
    return regex_searchop(rx_matchstring,pat,string,FIX2INT(ef));
  else return fd_type_error("unsigned int","regex_matchpair",ef);
}

/* Initialization */

static int regex_init = 0;

FD_EXPORT int fd_init_regex_c()
{
  lispval regex_module;
  if (regex_init) return 0;

  regex_init = 1;
  regex_module = fd_new_cmodule("REGEX",(FD_MODULE_SAFE),fd_init_regex_c);

  fd_idefn(regex_module,
           fd_make_cprim3x("REGEX",make_regex,1,
                           fd_string_type,VOID,-1,FD_FALSE,-1,FD_FALSE));
  fd_idefn(regex_module,fd_make_cprim1("REGEX?",regexp_prim,1));

  fd_idefn(regex_module,
           fd_make_cprim3x("REGEX/SEARCH",regex_search,2,
                           fd_regex_type,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_FIXZERO));
  fd_idefn(regex_module,
           fd_make_cprim3x("REGEX/MATCH",regex_exactmatch,2,
                           fd_regex_type,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_FIXZERO));
  fd_idefn(regex_module,
           fd_make_cprim3x("REGEX/MATCHLEN",regex_matchlen,2,
                           fd_regex_type,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_FIXZERO));
  fd_idefn(regex_module,
           fd_make_cprim3x("REGEX/MATCHSTRING",regex_matchstring,2,
                           fd_regex_type,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_FIXZERO));
  fd_idefn(regex_module,
           fd_make_cprim3x("REGEX/MATCHPAIR",regex_matchpair,2,
                           fd_regex_type,VOID,fd_string_type,VOID,
                           fd_fixnum_type,FD_FIXZERO));

  fd_finish_module(regex_module);

  u8_register_source_file(_FILEINFO);

  return 1;
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
