/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2013 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define FD_PROVIDE_FASTEVAL 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/sequences.h"

#include <libu8/libu8.h>
#include <libu8/u8convert.h>

/* Character functions */

static fdtype char2integer(fdtype arg)
{
  return FD_INT2DTYPE(FD_CHAR2CODE(arg));
}

static fdtype integer2char(fdtype arg)
{
  return FD_CODE2CHAR(fd_getint(arg));
}

static fdtype char_alphabeticp(fdtype arg)
{
  if (u8_isalpha(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype char_numericp(fdtype arg)
{
  if (u8_isdigit(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype char_whitespacep(fdtype arg)
{
  if (u8_isspace(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype char_upper_casep(fdtype arg)
{
  if (u8_isupper(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype char_lower_casep(fdtype arg)
{
  if (u8_islower(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype char_alphanumericp(fdtype arg)
{
  if (u8_isalnum(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

static fdtype char_punctuationp(fdtype arg)
{
  if (u8_ispunct(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

/* String predicates */

static fdtype asciip(fdtype string)
{
  u8_byte *scan=FD_STRDATA(string);
  u8_byte *limit=scan+FD_STRLEN(string);
  while (scan<limit)
    if (*scan>=0x80) return FD_FALSE;
    else scan++;
  return FD_TRUE;
}

static fdtype latin1p(fdtype string)
{
  u8_byte *scan=FD_STRDATA(string);
  u8_byte *limit=scan+FD_STRLEN(string);
  int c=u8_sgetc(&scan);
  while (scan<limit) {
    if (c>0x100) return FD_FALSE;
    else c=u8_sgetc(&scan);}
  return FD_TRUE;
}

static fdtype lowercasep(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c;
    while ((c=u8_sgetc(&scan))>=0) {
      if (u8_isupper(c)) return FD_FALSE;}
    return FD_TRUE;}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    if (u8_islower(c)) return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("string or character","lowercasep",string);
}

static fdtype uppercasep(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c;
    while ((c=u8_sgetc(&scan))>=0) {
      if (u8_islower(c)) return FD_FALSE;}
    return FD_TRUE;}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    if (u8_isupper(c)) return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("string or character","uppercasep",string);
}

static fdtype capitalizedp(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c=u8_sgetc(&scan);
    if (u8_isupper(c)) return FD_TRUE; else return FD_FALSE;}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    if (u8_isupper(c)) return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("string or character","capitalizedp",string);
}

static fdtype some_capitalizedp(fdtype string,fdtype window_arg)
{
  int window=FD_FIX2INT(window_arg);
  u8_byte *scan=FD_STRDATA(string); int c=u8_sgetc(&scan), i=0;
  if (c<0) return FD_FALSE;
  else if (window<=0)
    while (c>0) {
      if (u8_isupper(c)) return FD_TRUE;
      c=u8_sgetc(&scan);}
  else while ((c>0) && (i<window)) {
      if (u8_isupper(c)) return FD_TRUE;
      c=u8_sgetc(&scan); i++;}
  return FD_FALSE;
}

static fdtype string_compoundp(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string);
    if (strchr(scan,' ')) return FD_TRUE;
    else {
      u8_byte *lim=scan+FD_STRLEN(string);
      int c=u8_sgetc(&scan);
      while ((c>=0) && (scan<lim))
        if (u8_isspace(c)) return FD_TRUE;
        else c=u8_sgetc(&scan);
      return FD_FALSE;}}
  else return FD_FALSE;
}

static fdtype string_phrase_length(fdtype string)
{
  int len=0;
  u8_byte *scan=FD_STRDATA(string);
  u8_byte *lim=scan+FD_STRLEN(string);
  int c=u8_sgetc(&scan);
  if (u8_isspace(c)) while ((u8_isspace(c)) && (c>=0) && (scan<lim)) {
      c=u8_sgetc(&scan);}
  while ((c>=0) && (scan<lim))
    if (u8_isspace(c)) {
      len++; while ((u8_isspace(c)) && (c>=0) && (scan<lim)) {
        c=u8_sgetc(&scan);}
      continue;}
    else c=u8_sgetc(&scan);
  return FD_INT2DTYPE(len+1);
}

static fdtype empty_stringp(fdtype string,fdtype count_vspace_arg)
{
  int count_vspace=(!(FD_FALSEP(count_vspace_arg)));
  if (!(FD_STRINGP(string))) return FD_FALSE;
  else if (FD_STRLEN(string)==0) return FD_TRUE;
  else {
    u8_byte *scan=FD_STRDATA(string), *lim=scan+FD_STRLEN(string);
    while (scan<lim) {
      int c=u8_sgetc(&scan);
      if ((count_vspace)&&
          ((c=='\n')||(c=='\r')||(c=='\f')||(c=='\v')))
        return FD_FALSE;
      else if (!(u8_isspace(c))) return FD_FALSE;
      else {}}
    return FD_TRUE;}
}

/* String conversions */

static fdtype downcase(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      int lc=u8_tolower(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_tolower(c));}
  else if (FD_SYMBOLP(string)) {
    u8_byte *scan=FD_SYMBOL_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      int lc=u8_tolower(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else return fd_type_error(_("string, symbol, or character"),"downcase",string);

}
static fdtype char_downcase(fdtype ch)
{
  int c=FD_CHARCODE(ch);
  return FD_CODE2CHAR(u8_tolower(c));
}

static fdtype upcase(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      int lc=u8_toupper(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_toupper(c));}
  else if (FD_SYMBOLP(string)) {
    u8_byte *scan=FD_SYMBOL_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      int lc=u8_toupper(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else return fd_type_error(_("string or character"),"upcase",string);
}
static fdtype char_upcase(fdtype ch)
{
  int c=FD_CHARCODE(ch);
  return FD_CODE2CHAR(u8_toupper(c));
}

static fdtype capitalize(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c;
    struct U8_OUTPUT out; int word_start=1;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      int oc=((word_start) ? (u8_toupper(c)) : (u8_tolower(c)));
      u8_putc(&out,oc); word_start=(u8_isspace(c));}
    return fd_stream2string(&out);}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_toupper(c));}
  else return fd_type_error(_("string or character"),"capitalize",string);
}

static fdtype capitalize1(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c=u8_sgetc(&scan);
    if (u8_isupper(c)) return fd_incref(string);
    else {
      struct U8_OUTPUT out;
      U8_INIT_OUTPUT(&out,FD_STRLEN(string)+4);
      u8_putc(&out,u8_toupper(c));
      u8_puts(&out,scan);
      return fd_stream2string(&out);}}
  else return fd_type_error(_("string or character"),"capitalize1",string);
}

static u8_string skip_space(u8_string s)
{
  u8_string last=s, scan=s; int c=u8_sgetc(&scan);
  while (u8_isspace(c)) {last=scan; c=u8_sgetc(&scan);}
  return last;
}

static fdtype string_stdcap(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_string str=FD_STRDATA(string), scan=str;
    int fc=u8_sgetc(&scan), c=fc, n_caps=0, nospace=1, at_break=1;
    while (c>=0) {
      if ((at_break)&&(u8_isupper(c))) {
        c=u8_sgetc(&scan);
        if (!(u8_isupper(c))) n_caps++;}
      if (u8_isspace(c)) {nospace=0; at_break=1;}
      else if (u8_ispunct(c)) at_break=1;
      else at_break=0;
      c=u8_sgetc(&scan);}
    if (nospace) return fd_incref(string);
    else if (n_caps==0) return fd_incref(string);
    else {
      struct U8_OUTPUT out;
      u8_string scan=str, prev=str;
      U8_INIT_OUTPUT(&out,FD_STRLEN(string));
      c=u8_sgetc(&scan);
      while (c>=0) {
        if (u8_ispunct(c)) {
          u8_putc(&out,c); prev=scan; c=u8_sgetc(&scan);}
        else if (u8_isspace(c)) {
          while (u8_isspace(c)) {
            prev=scan; c=u8_sgetc(&scan);}
          if ((*scan)&&(out.u8_outptr>out.u8_outbuf))
            u8_putc(&out,' ');}
        else {
          u8_string w_start=prev, w1=scan;
          int fc=c, weird_caps=0;
          prev=scan; c=u8_sgetc(&scan);
          while (!((c<0)||(u8_isspace(c))||(u8_ispunct(c)))) {
            if (u8_isupper(c)) weird_caps=1;
            prev=scan; c=u8_sgetc(&scan);}
          if ((weird_caps)||(u8_isupper(fc)))
            u8_putn(&out,w_start,prev-w_start);
          else {
            u8_putc(&out,u8_toupper(fc));
            u8_putn(&out,w1,prev-w1);}}}
      return fd_stream2string(&out);}}
  else return fd_type_error(_("string"),"string_stdcap",string);
}

static fdtype string_downcase1(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c=u8_sgetc(&scan);
    if (u8_islower(c)) return fd_incref(string);
    else {
      struct U8_OUTPUT out;
      U8_INIT_OUTPUT(&out,FD_STRLEN(string)+4);
      u8_putc(&out,u8_tolower(c));
      u8_puts(&out,scan);
      return fd_stream2string(&out);}}
  else return fd_type_error(_("string or character"),"capitalize1",string);
}

static fdtype string_stdspace(fdtype string,fdtype keep_vertical_arg)
{
  int keep_vertical=FD_TRUEP(keep_vertical_arg);
  u8_byte *scan=FD_STRDATA(string); int c, white=1;
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT(&out,64);
  while ((c=u8_sgetc(&scan))>=0) {
    if ((keep_vertical) && ((c=='\n') && (*scan=='\n'))) {
      u8_putc(&out,c); u8_putc(&out,c); scan++; white=1;}
    else if ((keep_vertical) &&
             ((c==0xB6) || (c==0x0700) ||(c==0x10FB) ||
              (c==0x1368) || (c==0x2029))) {
      u8_putc(&out,c); white=1;}
    else if (u8_isspace(c))
      if (white) {}
      else {u8_putc(&out,' '); white=1;}
    else {white=0; u8_putc(&out,c);}}
  if (out.u8_outptr==out.u8_outbuf) {
    u8_free(out.u8_outbuf);
    return fdtype_string("");}
  else if (white) {out.u8_outptr[-1]='\0'; out.u8_outptr--;}
  return fd_stream2string(&out);
}

static fdtype string_stdstring(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c, white=1;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      if (u8_isspace(c))
        if (white) {}
        else {u8_putc(&out,' '); white=1;}
      else if (u8_ismodifier(c)) white=0;
      else {
        int bc=u8_base_char(c);
        bc=u8_tolower(bc); white=0;
        u8_putc(&out,bc);}}
    if (out.u8_outptr==out.u8_outbuf) {
      u8_free(out.u8_outbuf);
      return fdtype_string("");}
    else if (white) {out.u8_outptr[-1]='\0'; out.u8_outptr--;}
    return fd_stream2string(&out);}
  else return fd_type_error("string","string_stdstring",string);
}

static fdtype string_basestring(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      int bc=u8_base_char(c);
      u8_putc(&out,bc);}
    if (out.u8_outptr==out.u8_outbuf) {
      u8_free(out.u8_outbuf);
      return fdtype_string("");}
    return fd_stream2string(&out);}
  else return fd_type_error("string","string_basestring",string);
}

static fdtype string_startword(fdtype string)
{
  u8_string scan=FD_STRDATA(string), start=scan, last=scan;
  int c=u8_sgetc(&scan);
  if (u8_isspace(c)) {
    start=scan;
    while ((c>0)&&(u8_isspace(c))) c=u8_sgetc(&scan);
    last=scan;}
  while (c>=0) {
    if (u8_isspace(c))
      return fd_extract_string(NULL,start,last);
    else {
      last=scan; c=u8_sgetc(&scan);}}
  return fd_incref(string);
}

/* String comparison */

static int string_compare(u8_string s1,u8_string s2)
{
  u8_string scan1=s1, scan2=s2;
  int c1=u8_sgetc(&scan1), c2=u8_sgetc(&scan2);
  while ((c1==c2) && (c1>0)) {
    c1=u8_sgetc(&scan1); c2=u8_sgetc(&scan2);}
  if (c1==c2) return 0;
  else if (c1<0) return -1;
  else if (c2<0) return 1;
  else if (c1<c2) return -1;
  else return 1;
}

static int string_compare_ci(u8_string s1,u8_string s2)
{
  u8_string scan1=s1, scan2=s2;
  int c1=u8_sgetc(&scan1), c2=u8_sgetc(&scan2);
  c1=u8_tolower(c1); c2=u8_tolower(c2);
  while ((c1==c2) && (c1>0)) {
    c1=u8_sgetc(&scan1); c2=u8_sgetc(&scan2);
    c1=u8_tolower(c1); c2=u8_tolower(c2);}
  if (c1==c2) return 0;
  else if (c1<0) return -1;
  else if (c2<0) return 1;
  else if (c1<c2) return -1;
  else return 1;
}

static fdtype string_eq(fdtype string1,fdtype string2)
{
  if (string_compare(FD_STRDATA(string1),FD_STRDATA(string2))==0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_eq(fdtype ch1,fdtype ch2)
{
  if (FD_CHARCODE(ch1)==FD_CHARCODE(ch2))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype string_ci_eq(fdtype string1,fdtype string2)
{
  if (string_compare_ci(FD_STRDATA(string1),FD_STRDATA(string2))==0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_ci_eq(fdtype ch1,fdtype ch2)
{
  if (u8_tolower(FD_CHARCODE(ch1))==u8_tolower(FD_CHARCODE(ch2)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype string_lt(fdtype string1,fdtype string2)
{
  if (string_compare(FD_STRDATA(string1),FD_STRDATA(string2))<0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_lt(fdtype ch1,fdtype ch2)
{
  if (FD_CHARCODE(ch1)<FD_CHARCODE(ch2))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype string_ci_lt(fdtype string1,fdtype string2)
{
  if (string_compare_ci(FD_STRDATA(string1),FD_STRDATA(string2))<0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_ci_lt(fdtype ch1,fdtype ch2)
{
  if (u8_tolower(FD_CHARCODE(ch1))<u8_tolower(FD_CHARCODE(ch2)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype string_lte(fdtype string1,fdtype string2)
{
  if (string_compare(FD_STRDATA(string1),FD_STRDATA(string2))<=0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_lte(fdtype ch1,fdtype ch2)
{
  if (FD_CHARCODE(ch1)<=FD_CHARCODE(ch2))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype string_ci_lte(fdtype string1,fdtype string2)
{
  if (string_compare_ci(FD_STRDATA(string1),FD_STRDATA(string2))<=0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_ci_lte(fdtype ch1,fdtype ch2)
{
  if (u8_tolower(FD_CHARCODE(ch1))<=u8_tolower(FD_CHARCODE(ch2)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype string_gt(fdtype string1,fdtype string2)
{
  if (string_compare(FD_STRDATA(string1),FD_STRDATA(string2))>0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_gt(fdtype ch1,fdtype ch2)
{
  if (FD_CHARCODE(ch1)>FD_CHARCODE(ch2))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype string_ci_gt(fdtype string1,fdtype string2)
{
  if (string_compare_ci(FD_STRDATA(string1),FD_STRDATA(string2))>0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_ci_gt(fdtype ch1,fdtype ch2)
{
  if (u8_tolower(FD_CHARCODE(ch1))>u8_tolower(FD_CHARCODE(ch2)))
    return FD_TRUE;
  else return FD_FALSE;
}

static fdtype string_gte(fdtype string1,fdtype string2)
{
  if (string_compare(FD_STRDATA(string1),FD_STRDATA(string2))>=0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_gte(fdtype ch1,fdtype ch2)
{
  if (FD_CHARCODE(ch1)>=FD_CHARCODE(ch2))
    return FD_TRUE;
  else return FD_FALSE;
}


static fdtype string_ci_gte(fdtype string1,fdtype string2)
{
  if (string_compare_ci(FD_STRDATA(string1),FD_STRDATA(string2))>=0)
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype char_ci_gte(fdtype ch1,fdtype ch2)
{
  if (u8_tolower(FD_CHARCODE(ch1))>=u8_tolower(FD_CHARCODE(ch2)))
    return FD_TRUE;
  else return FD_FALSE;
}

/* String building */

static fdtype string_append(int n,fdtype *args)
{
  int i=0;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
  while (i<n)
    if (FD_STRINGP(args[i])) {
      u8_puts(&out,FD_STRDATA(args[i])); i++;}
    else {
      u8_free(out.u8_outbuf);
      return fd_type_error("string","string_append",args[i]);}
  return fd_stream2string(&out);
}

static fdtype string_prim(int n,fdtype *args)
{
  int i=0;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
  while (i<n)
    if (FD_CHARACTERP(args[i])) {
      u8_putc(&out,FD_CHARCODE(args[i])); i++;}
    else {
      u8_free(out.u8_outbuf);
      return fd_type_error("character","string_prim",args[i]);}
  return fd_stream2string(&out);
}

static fdtype makestring(fdtype len,fdtype character)
{
  struct U8_OUTPUT out;
  if (fd_getint(len)==0)
    return fd_init_string(NULL,0,NULL);
  else {
    int i=0, n=fd_getint(len), ch=FD_CHAR2CODE(character);
    U8_INIT_OUTPUT(&out,n);
    while (i<n) {u8_putc(&out,ch); i++;}
    return fd_stream2string(&out);}
}

/* Trigrams and Bigrams */

static int get_stdchar(u8_byte **in)
{
  int c;
  while ((c=u8_sgetc(in))>=0)
    if (u8_ismodifier(c)) c=u8_sgetc(in);
    else {
      c=u8_base_char(c); c=u8_tolower(c);
      return c;}
  return c;
}

static fdtype string_trigrams(fdtype string)
{
  U8_OUTPUT out; u8_byte buf[64];
  u8_byte *in=FD_STRDATA(string);
  int c1=' ', c2=' ', c3=' ', c;
  fdtype trigram, trigrams=FD_EMPTY_CHOICE;
  U8_INIT_FIXED_OUTPUT(&out,64,buf);
  if (FD_STRINGP(string)) {
    while ((c=get_stdchar(&in))>=0) {
      c1=c2; c2=c3; c3=c; out.u8_outptr=out.u8_outbuf;
      u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
      trigram=fd_make_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
      FD_ADD_TO_CHOICE(trigrams,trigram);}
    c1=c2; c2=c3; c3=' '; out.u8_outptr=out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram=fd_make_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
    FD_ADD_TO_CHOICE(trigrams,trigram);
    c1=c2; c2=c3; c3=' '; out.u8_outptr=out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram=fd_make_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
    FD_ADD_TO_CHOICE(trigrams,trigram);
    return trigrams;}
  else return fd_type_error(_("string"),"string_trigrams",string);
}

static fdtype string_bigrams(fdtype string)
{
  U8_OUTPUT out; u8_byte buf[64];
  u8_byte *in=FD_STRDATA(string);
  int c1=' ', c2=' ', c;
  fdtype bigram, bigrams=FD_EMPTY_CHOICE;
  U8_INIT_FIXED_OUTPUT(&out,64,buf);
  if (FD_STRINGP(string)) {
    while ((c=get_stdchar(&in))>=0) {
      c1=c2; c2=c; out.u8_outptr=out.u8_outbuf;
      u8_putc(&out,c1); u8_putc(&out,c2);
      bigram=fd_make_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
      FD_ADD_TO_CHOICE(bigrams,bigram);}
    c1=c2; c2=' '; out.u8_outptr=out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2);
    bigram=fd_make_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
    FD_ADD_TO_CHOICE(bigrams,bigram);
    return bigrams;}
  else return fd_type_error(_("string"),"string_bigrams",string);
}

/* String predicates */

static int getnonstring(fdtype choice)
{
  FD_DO_CHOICES(x,choice) {
    if (!(FD_STRINGP(x))) {
      FD_STOP_DO_CHOICES;
      return x;}
    else {}}
  return FD_VOID;
}

static fdtype has_suffix_test(fdtype string,fdtype suffix)
{
  int string_len=FD_STRING_LENGTH(string);
  int suffix_len=FD_STRING_LENGTH(suffix);
  if (suffix_len>string_len) return FD_FALSE;
  else {
    u8_string string_data=FD_STRING_DATA(string);
    u8_string suffix_data=FD_STRING_DATA(suffix);
    if (strncmp(string_data+(string_len-suffix_len),
                suffix_data,
                suffix_len) == 0)
      return FD_TRUE;
    else return FD_FALSE;}
}
static fdtype has_suffix(fdtype string,fdtype suffix)
{
  fdtype notstring;
  if (FD_QCHOICEP(suffix)) suffix=(FD_XQCHOICE(suffix))->choice;
  if ((FD_STRINGP(string))&&(FD_STRINGP(suffix)))
    return has_suffix_test(string,suffix);
  if ((FD_EMPTY_CHOICEP(string))||(FD_EMPTY_CHOICEP(suffix)))
    return FD_FALSE;
  notstring=getnonstring(string);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_suffix/input",notstring);
  else notstring=getnonstring(suffix);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_suffix/suffix",notstring);
  if ((FD_CHOICEP(string))&&(FD_CHOICEP(suffix))) {
    fdtype result=FD_FALSE;
    FD_DO_CHOICES(s,string) {
      FD_DO_CHOICES(sx,suffix) {
        result=has_suffix_test(s,sx);
        if (FD_TRUEP(result)) {
          FD_STOP_DO_CHOICES; break;}}
      if (FD_TRUEP(result)) {
        FD_STOP_DO_CHOICES; break;}}
    return result;}
  else if (FD_CHOICEP(string)) {
    FD_DO_CHOICES(s,string) {
      fdtype result=has_suffix_test(s,suffix);
      if (FD_TRUEP(result)) {
        FD_STOP_DO_CHOICES;
        return result;}}
    return FD_FALSE;}
  else if (FD_CHOICEP(suffix)) {
    FD_DO_CHOICES(sx,suffix) {
      fdtype result=has_suffix_test(string,sx);
      if (FD_TRUEP(result)) {
        FD_STOP_DO_CHOICES;
        return result;}}
    return FD_FALSE;}
  else return FD_FALSE;
}
static fdtype is_suffix(fdtype suffix,fdtype string) {
  return has_suffix(string,suffix); }

static fdtype has_prefix_test(fdtype string,fdtype prefix)
{
  int string_len=FD_STRING_LENGTH(string);
  int prefix_len=FD_STRING_LENGTH(prefix);
  if (prefix_len>string_len) return FD_FALSE;
  else {
    u8_string string_data=FD_STRING_DATA(string);
    u8_string prefix_data=FD_STRING_DATA(prefix);
    if (strncmp(string_data,prefix_data,prefix_len) == 0)
      return FD_TRUE;
    else return FD_FALSE;}
}
static fdtype has_prefix(fdtype string,fdtype prefix)
{
  fdtype notstring;
  if (FD_QCHOICEP(prefix)) prefix=(FD_XQCHOICE(prefix))->choice;
  if ((FD_STRINGP(string))&&(FD_STRINGP(prefix)))
    return has_prefix_test(string,prefix);
  if ((FD_EMPTY_CHOICEP(string))||(FD_EMPTY_CHOICEP(prefix)))
    return FD_FALSE;
  notstring=getnonstring(string);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_prefix/input",notstring);
  else notstring=getnonstring(prefix);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_prefix/prefix",notstring);
  if ((FD_CHOICEP(string))&&(FD_CHOICEP(prefix))) {
    fdtype result=FD_FALSE;
    FD_DO_CHOICES(s,string) {
      FD_DO_CHOICES(p,prefix) {
        result=has_prefix_test(s,p);
        if (FD_TRUEP(result)) {
          FD_STOP_DO_CHOICES; break;}}
      if (FD_TRUEP(result)) {
        FD_STOP_DO_CHOICES; break;}}
    return result;}
  else if (FD_CHOICEP(string)) {
    FD_DO_CHOICES(s,string) {
      fdtype result=has_prefix_test(s,prefix);
      if (FD_TRUEP(result)) {
        FD_STOP_DO_CHOICES;
        return result;}}
    return FD_FALSE;}
  else if (FD_CHOICEP(prefix)) {
    FD_DO_CHOICES(p,prefix) {
      fdtype result=has_prefix_test(string,p);
      if (FD_TRUEP(result)) {
        FD_STOP_DO_CHOICES;
        return result;}}
    return FD_FALSE;}
  else return FD_FALSE;
}
static fdtype is_prefix(fdtype prefix,fdtype string) {
  return has_prefix(string,prefix); }

/* YES/NO */

static int strmatch(u8_string s,fdtype lval,int ignorecase)
{
  u8_string sval=NULL;
  if (FD_STRINGP(lval)) sval=FD_STRDATA(lval);
  else if (FD_SYMBOLP(lval)) sval=FD_SYMBOL_NAME(lval);
  else {}
  if (sval==NULL) return 0;
  else if (ignorecase)
    return (strcasecmp(s,sval)==0);
  else return (strcmp(s,sval)==0);
}

static int check_yesp(u8_string arg,fdtype strings,int ignorecase)
{
  if ((FD_STRINGP(strings))||(FD_SYMBOLP(strings)))
    return strmatch(arg,strings,ignorecase);
  else if (FD_VECTORP(strings)) {
    fdtype *v=FD_VECTOR_DATA(strings);
    int i=0, lim=FD_VECTOR_LENGTH(strings);
    while (i<lim) {
      fdtype s=FD_VECTOR_REF(v,i); i++;
      if (strmatch(arg,s,ignorecase))
        return 1;}
    return 0;}
  else if (FD_PAIRP(strings)) {
    FD_DOLIST(s,strings) {
      if (strmatch(arg,s,ignorecase)) return 1;}
    return 0;}
  else if ((FD_CHOICEP(strings))||(FD_ACHOICEP(strings))) {
    FD_DO_CHOICES(s,strings) {
      if (strmatch(arg,s,ignorecase)) {
        FD_STOP_DO_CHOICES;
        return 1;}}
    return 0;}
  else {}
  return 0;
}

static fdtype yesp_prim(fdtype arg,fdtype dflt,fdtype yes,fdtype no)
{
  u8_string string_arg; int ignorecase=0;
  if (FD_STRINGP(arg)) string_arg=FD_STRDATA(arg);
  else if (FD_SYMBOLP(arg)) {
    string_arg=FD_SYMBOL_NAME(arg); ignorecase=1;}
  else return fd_type_error("string or symbol","yesp_prim",arg);
  if ((!(FD_VOIDP(yes)))&&(check_yesp(string_arg,yes,ignorecase)))
    return FD_TRUE;
  else if ((!(FD_VOIDP(no)))&&(check_yesp(string_arg,no,ignorecase)))
    return FD_FALSE;
  else return fd_boolstring(FD_STRDATA(arg),((FD_FALSEP(dflt))?(0):(1)));
}

/* Conversion */

static fdtype entity_escape;

static fdtype string2packet(fdtype string,fdtype encoding,fdtype escape)
{
  char *data; int n_bytes; u8_byte *scan, *limit;
  struct U8_TEXT_ENCODING *enc;
  if (FD_VOIDP(encoding)) {
    int n_bytes=FD_STRLEN(string);
    return fd_make_packet(NULL,n_bytes,FD_STRDATA(string));}
  else if (FD_STRINGP(encoding))
    enc=u8_get_encoding(FD_STRDATA(encoding));
  else if (FD_SYMBOLP(encoding))
    enc=u8_get_encoding(FD_SYMBOL_NAME(encoding));
  else return fd_type_error(_("text encoding"),"string2packet",encoding);
  scan=FD_STRDATA(string); limit=scan+FD_STRLEN(string);
  if (FD_EQ(escape,entity_escape))
    data=u8_localize(enc,&scan,limit,'&',0,NULL,&n_bytes);
  else if ((FD_FALSEP(escape)) || (FD_VOIDP(escape)))
    data=u8_localize(enc,&scan,limit,0,0,NULL,&n_bytes);
  else data=u8_localize(enc,&scan,limit,'\\',0,NULL,&n_bytes);
  if (data) {
    fdtype result=fd_make_packet(NULL,n_bytes,data);
    u8_free(data);
    return result;}
  else return FD_ERROR_VALUE;
}

static fdtype x2secret_prim(fdtype arg)
{
  if (FD_PRIM_TYPEP(arg,fd_secret_type))
    return fd_incref(arg);
  else if (FD_PRIM_TYPEP(arg,fd_packet_type)) {
    fdtype result=fd_make_packet
      (NULL,FD_PACKET_LENGTH(arg),FD_PACKET_DATA(arg));
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
  else if (FD_STRINGP(arg)) {
    fdtype result=fd_make_packet(NULL,FD_STRLEN(arg),FD_STRDATA(arg));
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
  else return fd_type_error("string/packet","x2secret_prim",arg);
}

static fdtype packet2string(fdtype packet,fdtype encoding)
{
  if (FD_PACKET_LENGTH(packet)==0)
    return fdtype_string("");
  else {
    struct U8_OUTPUT out;
    u8_byte *scan=FD_PACKET_DATA(packet), *limit=scan+FD_PACKET_LENGTH(packet);
    U8_INIT_OUTPUT(&out,2*FD_PACKET_LENGTH(packet));
    struct U8_TEXT_ENCODING *enc;
    if (FD_VOIDP(encoding))
      enc=u8_get_encoding("UTF-8");
    else if (FD_STRINGP(encoding))
      enc=u8_get_encoding(FD_STRDATA(encoding));
    else if (FD_SYMBOLP(encoding))
      enc=u8_get_encoding(FD_SYMBOL_NAME(encoding));
    else return fd_type_error(_("text encoding"),"packet2string",encoding);
    if (u8_convert(enc,0,&out,&scan,limit)<0) {
      u8_free(out.u8_outbuf); return FD_ERROR_VALUE;}
    else return fd_stream2string(&out);}
}

static fdtype string_byte_length(fdtype string)
{
  int len=FD_STRLEN(string);
  return FD_INT2DTYPE(len);
}

/* Fixing embedded NULs */

static fdtype fixnuls(fdtype string)
{
  struct FD_STRING *ss=FD_GET_CONS(string,fd_string_type,fd_string);
  if (strlen(ss->bytes)<ss->length) {
    /* Handle embedded NUL */
    struct U8_OUTPUT out;
    u8_byte *scan=ss->bytes, *limit=scan+ss->length;
    U8_INIT_OUTPUT(&out,ss->length+8);
    while (scan<limit) {
      if (*scan)
        u8_putc(&out,u8_sgetc(&scan));
      else u8_putc(&out,0);}
    return fd_stream2string(&out);}
  else return fd_incref(string);
}

/* Simple string subst */

static fdtype string_subst_prim(fdtype string,fdtype substring,fdtype with)
{
  if (FD_STRLEN(string)==0) return fd_incref(string);
  else if (strstr(FD_STRDATA(string),FD_STRDATA(substring))==NULL)
    return fd_incref(string);
  else {
    u8_string original=FD_STRDATA(string);
    u8_string search=FD_STRDATA(substring);
    u8_string replace=FD_STRDATA(with);
    int searchlen=FD_STRING_LENGTH(substring);
    int startlen=((FD_STRLEN(with)<=FD_STRLEN(substring))?
                  (FD_STRLEN(string)+17):(FD_STRLEN(string)*2));
    u8_string point=strstr(original,search);
    if (point) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,startlen);
      u8_string last=original; while (point) {
        u8_putn(&out,last,point-last); u8_puts(&out,replace);
        last=point+searchlen; point=strstr(last,search);}
      u8_puts(&out,last);
      return fd_stream2string(&out);}
    else return fd_incref(string);}
}

static fdtype string_subst_star(int n,fdtype *args)
{
  fdtype base=args[0]; int i=1;
  if ((n%2)==0)
    return fd_err(fd_SyntaxError,"string_subst_star",NULL,FD_VOID);
  else while (i<n)
    if (FD_EXPECT_FALSE(!(FD_STRINGP(args[i]))))
      return fd_type_error(_("string"),"string_subst_star",args[i]);
    else i++;
  /* In case we return it. */
  fd_incref(base); i=1;
  while (i<n) {
    fdtype replace=args[i];
    fdtype with=args[i+1];
    fdtype result=string_subst_prim(base,replace,with);
    i=i+2; fd_decref(base); base=result;}
  return base;
}

static fdtype trim_spaces(fdtype string)
{
  u8_byte *start=FD_STRDATA(string), *end=start+FD_STRLEN(string);
  u8_byte *trim_start=start, *trim_end=end;
  u8_byte *scan=trim_start;
  while (scan<end) {
    int c=u8_sgetc(&scan);
    if (u8_isspace(c)) trim_start=scan;
    else break;}
  scan=trim_end-1;
  while (scan>=trim_start) {
    u8_byte *cstart=scan; int c;
    while ((cstart>=trim_start) &&
           ((*cstart)>=0x80) &&
           ((*cstart)<0xC0)) cstart--;
    if (cstart<trim_start) break;
    scan=cstart;
    c=u8_sgetc(&scan);
    if (u8_isspace(c)) {
      trim_end=cstart;
      scan=cstart-1;}
    else break;}
  if ((trim_start==start) && (trim_end==end))
    return fd_incref(string);
  else return fd_extract_string(NULL,trim_start,trim_end);
}

/* Glomming */

static fdtype glom_lexpr(int n,fdtype *args)
{
  unsigned char *result_data, *write;
  int i=0, sumlen=0; fd_ptr_type result_type=0;
  unsigned char **strings, *stringsbuf[16];
  int *lengths, lengthsbuf[16];
  unsigned char *consed, consedbuf[16];
  if (n>16) {
    strings=u8_malloc(sizeof(unsigned char *)*n);
    lengths=u8_malloc(sizeof(int)*n);
    consed=u8_malloc(sizeof(unsigned char)*n);}
  else {
    strings=stringsbuf;
    lengths=lengthsbuf;
    consed=consedbuf;}
  memset(strings,0,sizeof(unsigned char *)*n);
  memset(lengths,0,sizeof(int)*n);
  memset(consed,0,sizeof(unsigned char)*n);
  while (i<n)
    if (FD_STRINGP(args[i])) {
      sumlen=sumlen+FD_STRLEN(args[i]);
      strings[i]=FD_STRDATA(args[i]);
      lengths[i]=FD_STRLEN(args[i]);
      if (result_type==0) result_type=fd_string_type;
      consed[i++]=0;}
    else if (FD_PACKETP(args[i])) {
      sumlen=sumlen+FD_PACKET_LENGTH(args[i]);
      strings[i]=FD_PACKET_DATA(args[i]);
      lengths[i]=FD_PACKET_LENGTH(args[i]);
      if (FD_PRIM_TYPEP(args[i],fd_secret_type))
        result_type=fd_secret_type;
      else if (result_type!=fd_secret_type)
        result_type=fd_packet_type;
      consed[i++]=0;}
    else if ((FD_FALSEP(args[i]))||(FD_EMPTY_CHOICEP(args[i]))||
             (FD_VOIDP(args[i]))) {
      if (result_type==0) result_type=fd_string_type;
      strings[i]=NULL; lengths[i]=0; consed[i++]=0;}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
      if (result_type==0) result_type=fd_string_type;
      fd_unparse(&out,args[i]);
      sumlen=sumlen+(out.u8_outptr-out.u8_outbuf);
      strings[i]=out.u8_outbuf;
      lengths[i]=out.u8_outptr-out.u8_outbuf;
      consed[i++]=1;}
  write=result_data=u8_malloc(sumlen+1);
  i=0; while (i<n) {
    if (!(strings[i])) {i++; continue;}
    memcpy(write,strings[i],lengths[i]);
    write=write+lengths[i];
    i++;}
  *write='\0';
  i=0; while (i<n) {
    if (consed[i]) u8_free(strings[i]); i++;}
  if (n>16) {
    u8_free(strings); u8_free(lengths); u8_free(consed);}
  if (result_type==fd_string_type)
    return fd_init_string(NULL,sumlen,result_data);
  else if (result_type==fd_packet_type)
    return fd_init_packet(NULL,sumlen,result_data);
  else {
    fdtype result=fd_init_packet(NULL,sumlen,result_data);
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
}

/* Text if */

static fdtype textif_handler(fdtype expr,fd_lispenv env)
{
  fdtype test_expr=fd_get_arg(expr,1), test_val=FD_VOID;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_SyntaxError,"textif_handler",NULL,FD_VOID);
  else test_val=fd_eval(test_expr,env);
  if (FD_ABORTP(test_val)) return test_val;
  else if ((FD_FALSEP(test_val))||(FD_EMPTY_CHOICEP(test_val)))
    return fd_make_string(NULL,0,"");
  else {
    fdtype body=fd_get_body(expr,2), len=fd_seq_length(body);
    fd_decref(test_val); 
    if (len==0) return fd_make_string(NULL,0,NULL);
    else if (len==1) {
      fdtype text=fd_eval(fd_get_arg(body,0),env);
      if (FD_STRINGP(text)) return fd_incref(text);
      else if ((FD_FALSEP(text))||(FD_EMPTY_CHOICEP(text)))
        return fd_make_string(NULL,0,"");
      else {
        u8_string as_string=fd_dtype2string(text);
        return fd_init_string(NULL,-1,as_string);}}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      if (FD_PAIRP(body)) {
        FD_DOLIST(text_expr,body) {
          fdtype text=fd_eval(text_expr,env);
          if (FD_ABORTP(text))
            return fd_err("Bad text clause","textif_handler",
                          out.u8_outbuf,text_expr);
          else if (FD_STRINGP(text))
            u8_putn(&out,FD_STRDATA(text),FD_STRLEN(text));
          else fd_unparse(&out,text);
          fd_decref(text); text=FD_VOID;}}
      else {
        int i=0; while (i<len) {
          fdtype text_expr=fd_get_arg(body,i++);
          fdtype text=fd_eval(text_expr,env);
          if (FD_ABORTP(text))
            return fd_err("Bad text clause","textif_handler",
                          out.u8_outbuf,text_expr);
          else if (FD_STRINGP(text))
            u8_putn(&out,FD_STRDATA(text),FD_STRLEN(text));
          else fd_unparse(&out,text);
          fd_decref(text); text=FD_VOID;}}
      return fd_block_string(out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
    }
  }
}

/* Initialization */

FD_EXPORT void fd_init_strings_c()
{
  u8_register_source_file(_FILEINFO);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("ASCII?",asciip,1,fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("LATIN1?",latin1p,1,fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("LOWERCASE?",lowercasep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("DOWNCASE",downcase,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR-DOWNCASE",char_downcase,1,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("UPPERCASE?",uppercasep,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("UPCASE",upcase,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR-UPCASE",char_upcase,1,
                           fd_character_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING=?",string_eq,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING-CI=?",string_ci_eq,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING<?",string_lt,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING-CI<?",string_ci_lt,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING>?",string_gt,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING-CI>?",string_ci_gt,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING>=?",string_gte,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING-CI>=?",string_ci_gte,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING<=?",string_lte,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STRING-CI<=?",string_ci_lte,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR=?",char_eq,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR-CI=?",char_ci_eq,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR<?",char_lt,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR-CI<?",char_ci_lt,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR>?",char_gt,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR-CI>?",char_ci_gt,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR>=?",char_gte,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR-CI>=?",char_ci_gte,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR<=?",char_lte,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("CHAR-CI<=?",char_ci_lte,2,
                           fd_character_type,FD_VOID,
                           fd_character_type,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprim1("CAPITALIZED?",capitalizedp,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x
           ("SOMECAP?",some_capitalizedp,1,
            fd_string_type,FD_VOID,
            fd_fixnum_type,FD_INT2DTYPE(-1)));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CAPITALIZE",capitalize,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CAPITALIZE1",capitalize1,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("DOWNCASE1",string_downcase1,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("EMPTY-STRING?",
                           empty_stringp,1,-1,FD_VOID,-1,FD_FALSE));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND-STRING?",string_compoundp,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("PHRASE-LENGTH",string_phrase_length,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("COMPOUND?",string_compoundp,1,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("STDSPACE",string_stdspace,1,
                           fd_string_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("STDCAP",string_stdcap,1,fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("STDSTRING",string_stdstring,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("BASESTRING",string_basestring,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("STARTWORD",string_startword,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("TRIGRAMS",string_trigrams,1,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("BIGRAMS",string_bigrams,1,
                           fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR->INTEGER",char2integer,1,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("INTEGER->CHAR",integer2char,1,
                           fd_fixnum_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR-ALPHABETIC?",char_alphabeticp,1,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR-NUMERIC?",char_numericp,1,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR-ALPHANUMERIC?",char_alphanumericp,1,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR-PUNCTUATION?",char_punctuationp,1,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR-WHITESPACE?",char_whitespacep,1,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR-UPPER-CASE?",char_upper_casep,1,
                           fd_character_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("CHAR-LOWER-CASE?",char_lower_casep,1,
                           fd_character_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprim2x("HAS-PREFIX",has_prefix,2,
                            -1,FD_VOID,-1,FD_VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("IS-PREFIX",is_prefix,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprim2x("HAS-SUFFIX",has_suffix,2,
                            -1,FD_VOID,-1,FD_VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("IS-SUFFIX",is_suffix,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim4x("YES?",yesp_prim,1,
                           -1,FD_VOID,-1,FD_FALSE,
                           -1,FD_EMPTY_CHOICE,
                           -1,FD_EMPTY_CHOICE));

  fd_idefn(fd_scheme_module,fd_make_cprimn("STRING-APPEND",string_append,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("STRING",string_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("MAKE-STRING",makestring,1,
                                            fd_fixnum_type,FD_VOID,
                                            fd_character_type,FD_CODE2CHAR(32)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("STRING-SUBST",string_subst_prim,3,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprimn("STRING-SUBST*",string_subst_star,3));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("TRIM-SPACES",trim_spaces,1,fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprimn("GLOM",glom_lexpr,1));
  fd_defspecial(fd_scheme_module,"TEXTIF",textif_handler);

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("BYTE-LENGTH",string_byte_length,1,
                           fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("STRING->PACKET",string2packet,1,
                           fd_string_type,FD_VOID,
                           -1,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("PACKET->STRING",packet2string,1,
                           fd_packet_type,FD_VOID,
                           -1,FD_VOID));
  fd_idefn(fd_scheme_module,fd_make_cprim1("->SECRET",x2secret_prim,1));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("FIXNULS",fixnuls,1,fd_string_type,FD_VOID));

  entity_escape=fd_intern("ENTITIES");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
