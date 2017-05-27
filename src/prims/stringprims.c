/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
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

static u8_string StrSearchKey=_("string search/key");

/* Character functions */

static fdtype char2integer(fdtype arg)
{
  return FD_INT(FD_CHAR2CODE(arg));
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
  const u8_byte *scan = FD_STRDATA(string);
  const u8_byte *limit = scan+FD_STRLEN(string);
  while (scan<limit)
    if (*scan>=0x80) return FD_FALSE;
    else scan++;
  return FD_TRUE;
}

static fdtype latin1p(fdtype string)
{
  const u8_byte *scan = FD_STRDATA(string);
  const u8_byte *limit = scan+FD_STRLEN(string);
  int c = u8_sgetc(&scan);
  while (scan<limit) {
    if (c>0x100) return FD_FALSE;
    else c = u8_sgetc(&scan);}
  return FD_TRUE;
}

static fdtype lowercasep(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string); int c;
    while ((c = u8_sgetc(&scan))>=0) {
      if (u8_isupper(c)) return FD_FALSE;}
    return FD_TRUE;}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    if (u8_islower(c)) return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("string or character","lowercasep",string);
}

static fdtype uppercasep(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string); int c;
    while ((c = u8_sgetc(&scan))>=0) {
      if (u8_islower(c)) return FD_FALSE;}
    return FD_TRUE;}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    if (u8_isupper(c)) return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("string or character","uppercasep",string);
}

static fdtype capitalizedp(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string); int c = u8_sgetc(&scan);
    if (u8_isupper(c)) return FD_TRUE; else return FD_FALSE;}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    if (u8_isupper(c)) return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("string or character","capitalizedp",string);
}

static fdtype some_capitalizedp(fdtype string,fdtype window_arg)
{
  if (!(FD_UINTP(window_arg)))
    return fd_type_error("uint","some_capitalizedp",window_arg);
  int window = FD_FIX2INT(window_arg);
  const u8_byte *scan = FD_STRDATA(string);
  int c = u8_sgetc(&scan), i = 0;
  if (c<0) return FD_FALSE;
  else if (window<=0)
    while (c>0) {
      if (u8_isupper(c)) return FD_TRUE;
      c = u8_sgetc(&scan);}
  else while ((c>0) && (i<window)) {
      if (u8_isupper(c)) return FD_TRUE;
      c = u8_sgetc(&scan); i++;}
  return FD_FALSE;
}

static fdtype string_compoundp(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string);
    if (strchr(scan,' ')) return FD_TRUE;
    else {
      const u8_byte *lim = scan+FD_STRLEN(string);
      int c = u8_sgetc(&scan);
      while ((c>=0) && (scan<lim))
        if (u8_isspace(c)) return FD_TRUE;
        else c = u8_sgetc(&scan);
      return FD_FALSE;}}
  else return FD_FALSE;
}

static fdtype string_phrase_length(fdtype string)
{
  int len = 0;
  const u8_byte *scan = FD_STRDATA(string);
  const u8_byte *lim = scan+FD_STRLEN(string);
  int c = u8_sgetc(&scan);
  if (u8_isspace(c)) while ((u8_isspace(c)) && (c>=0) && (scan<lim)) {
      c = u8_sgetc(&scan);}
  while ((c>=0) && (scan<lim))
    if (u8_isspace(c)) {
      len++; while ((u8_isspace(c)) && (c>=0) && (scan<lim)) {
        c = u8_sgetc(&scan);}
      continue;}
    else c = u8_sgetc(&scan);
  return FD_INT(len+1);
}

static fdtype empty_stringp(fdtype string,fdtype count_vspace_arg,
                            fdtype count_nbsp_arg)
{
  int count_vspace = (!(FD_FALSEP(count_vspace_arg)));
  int count_nbsp = (!(FD_FALSEP(count_nbsp_arg)));
  if (!(FD_STRINGP(string))) return FD_FALSE;
  else if (FD_STRLEN(string)==0) return FD_TRUE;
  else {
    const u8_byte *scan = FD_STRDATA(string), *lim = scan+FD_STRLEN(string);
    while (scan<lim) {
      int c = u8_sgetc(&scan);
      if ((count_vspace)&&
          ((c=='\n')||(c=='\r')||(c=='\f')||(c=='\v')))
        return FD_FALSE;
      else if ((count_nbsp)&&(c==0x00a0))
        return FD_FALSE;
      else if (!(u8_isspace(c))) return FD_FALSE;
      else {}}
    return FD_TRUE;}
}

/* String conversions */

static fdtype downcase(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_tolower(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_tolower(c));}
  else if (FD_SYMBOLP(string)) {
    const u8_byte *scan = FD_SYMBOL_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_tolower(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else return fd_type_error
         (_("string, symbol, or character"),"downcase",string);

}
static fdtype char_downcase(fdtype ch)
{
  int c = FD_CHARCODE(ch);
  return FD_CODE2CHAR(u8_tolower(c));
}

static fdtype upcase(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_toupper(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_toupper(c));}
  else if (FD_SYMBOLP(string)) {
    const u8_byte *scan = FD_SYMBOL_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_toupper(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else return fd_type_error(_("string or character"),"upcase",string);
}
static fdtype char_upcase(fdtype ch)
{
  int c = FD_CHARCODE(ch);
  return FD_CODE2CHAR(u8_toupper(c));
}

static fdtype capitalize(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string); int c;
    struct U8_OUTPUT out; int word_start = 1;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int oc = ((word_start) ? (u8_toupper(c)) : (u8_tolower(c)));
      u8_putc(&out,oc); word_start = (u8_isspace(c));}
    return fd_stream2string(&out);}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_toupper(c));}
  else return fd_type_error(_("string or character"),"capitalize",string);
}

static fdtype capitalize1(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string); int c = u8_sgetc(&scan);
    if (u8_isupper(c)) return fd_incref(string);
    else {
      struct U8_OUTPUT out;
      U8_INIT_OUTPUT(&out,FD_STRLEN(string)+4);
      u8_putc(&out,u8_toupper(c));
      u8_puts(&out,scan);
      return fd_stream2string(&out);}}
  else return fd_type_error(_("string or character"),"capitalize1",string);
}

static fdtype string_stdcap(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_string str = FD_STRDATA(string), scan = str;
    int fc = u8_sgetc(&scan), c = fc, n_caps = 0, nospace = 1, at_break = 1;
    while (c>=0) {
      if ((at_break)&&(u8_isupper(c))) {
        c = u8_sgetc(&scan);
        if (!(u8_isupper(c))) n_caps++;}
      if (u8_isspace(c)) {nospace = 0; at_break = 1;}
      else if (u8_ispunct(c)) at_break = 1;
      else at_break = 0;
      c = u8_sgetc(&scan);}
    if (nospace) return fd_incref(string);
    else if (n_caps==0) return fd_incref(string);
    else {
      struct U8_OUTPUT out;
      u8_string scan = str, prev = str;
      U8_INIT_OUTPUT(&out,FD_STRLEN(string));
      c = u8_sgetc(&scan);
      while (c>=0) {
        if (u8_ispunct(c)) {
          u8_putc(&out,c); prev = scan; c = u8_sgetc(&scan);}
        else if (u8_isspace(c)) {
          while (u8_isspace(c)) {
            prev = scan; c = u8_sgetc(&scan);}
          if ((*scan)&&(out.u8_write>out.u8_outbuf))
            u8_putc(&out,' ');}
        else {
          u8_string w_start = prev, w1 = scan;
          int fc = c, weird_caps = 0;
          prev = scan; c = u8_sgetc(&scan);
          while (!((c<0)||(u8_isspace(c))||(u8_ispunct(c)))) {
            if (u8_isupper(c)) weird_caps = 1;
            prev = scan; c = u8_sgetc(&scan);}
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
    const u8_byte *scan = FD_STRDATA(string); int c = u8_sgetc(&scan);
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
  int keep_vertical = FD_TRUEP(keep_vertical_arg);
  const u8_byte *scan = FD_STRDATA(string); int c, white = 1;
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT(&out,64);
  while ((c = u8_sgetc(&scan))>=0) {
    if ((keep_vertical) && ((c=='\n') && (*scan=='\n'))) {
      u8_putc(&out,c); u8_putc(&out,c); scan++; white = 1;}
    else if ((keep_vertical) &&
             ((c==0xB6) || (c==0x0700) ||(c==0x10FB) ||
              (c==0x1368) || (c==0x2029))) {
      u8_putc(&out,c); white = 1;}
    else if (u8_isspace(c))
      if (white) {}
      else {u8_putc(&out,' '); white = 1;}
    else {white = 0; u8_putc(&out,c);}}
  if (out.u8_write == out.u8_outbuf) {
    u8_free(out.u8_outbuf);
    return fdtype_string("");}
  else if (white) {out.u8_write[-1]='\0'; out.u8_write--;}
  return fd_stream2string(&out);
}

static fdtype string_stdstring(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string); int c, white = 1;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      if (u8_isspace(c))
        if (white) {}
        else {u8_putc(&out,' '); white = 1;}
      else if (u8_ismodifier(c)) white = 0;
      else {
        int bc = u8_base_char(c);
        bc = u8_tolower(bc); white = 0;
        u8_putc(&out,bc);}}
    if (out.u8_write == out.u8_outbuf) {
      u8_free(out.u8_outbuf);
      return fdtype_string("");}
    else if (white) {out.u8_write[-1]='\0'; out.u8_write--;}
    return fd_stream2string(&out);}
  else return fd_type_error("string","string_stdstring",string);
}

static fdtype string_basestring(fdtype string)
{
  if (FD_STRINGP(string)) {
    const u8_byte *scan = FD_STRDATA(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int bc = u8_base_char(c);
      u8_putc(&out,bc);}
    if (out.u8_write == out.u8_outbuf) {
      u8_free(out.u8_outbuf);
      return fdtype_string("");}
    return fd_stream2string(&out);}
  else return fd_type_error("string","string_basestring",string);
}

static fdtype string_startword(fdtype string)
{
  u8_string scan = FD_STRDATA(string), start = scan, last = scan;
  int c = u8_sgetc(&scan);
  if (u8_isspace(c)) {
    start = scan;
    while ((c>0)&&(u8_isspace(c))) c = u8_sgetc(&scan);
    last = scan;}
  while (c>=0) {
    if (u8_isspace(c))
      return fd_substring(start,last);
    else {
      last = scan; c = u8_sgetc(&scan);}}
  return fd_incref(string);
}

/* UTF8 related primitives */

static fdtype utf8p_prim(fdtype packet)
{
  if (u8_validp(FD_PACKET_DATA(packet)))
    return FD_TRUE;
  else return FD_FALSE;
}
static fdtype utf8string_prim(fdtype packet)
{
  if (u8_validp(FD_PACKET_DATA(packet)))
    return fd_make_string(NULL,FD_PACKET_LENGTH(packet),FD_PACKET_DATA(packet));
  else return fd_incref(packet);
}

/* String comparison */

static int string_compare(u8_string s1,u8_string s2)
{
  u8_string scan1 = s1, scan2 = s2;
  int c1 = u8_sgetc(&scan1), c2 = u8_sgetc(&scan2);
  while ((c1==c2) && (c1>0)) {
    c1 = u8_sgetc(&scan1); c2 = u8_sgetc(&scan2);}
  if (c1==c2) return 0;
  else if (c1<0) return -1;
  else if (c2<0) return 1;
  else if (c1<c2) return -1;
  else return 1;
}

static int string_compare_ci(u8_string s1,u8_string s2)
{
  u8_string scan1 = s1, scan2 = s2;
  int c1 = u8_sgetc(&scan1), c2 = u8_sgetc(&scan2);
  c1 = u8_tolower(c1); c2 = u8_tolower(c2);
  while ((c1==c2) && (c1>0)) {
    c1 = u8_sgetc(&scan1); c2 = u8_sgetc(&scan2);
    c1 = u8_tolower(c1); c2 = u8_tolower(c2);}
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
  if (FD_CHARCODE(ch1) == FD_CHARCODE(ch2))
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
  if (u8_tolower(FD_CHARCODE(ch1)) == u8_tolower(FD_CHARCODE(ch2)))
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
  int i = 0;
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
  int i = 0;
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
    int i = 0, n = fd_getint(len), ch = FD_CHAR2CODE(character);
    U8_INIT_OUTPUT(&out,n);
    while (i<n) {u8_putc(&out,ch); i++;}
    return fd_stream2string(&out);}
}

/* Trigrams and Bigrams */

static int get_stdchar(const u8_byte **in)
{
  int c;
  while ((c = u8_sgetc(in))>=0)
    if (u8_ismodifier(c)) c = u8_sgetc(in);
    else {
      c = u8_base_char(c); c = u8_tolower(c);
      return c;}
  return c;
}

static fdtype string_trigrams(fdtype string)
{
  U8_OUTPUT out; u8_byte buf[64];
  const u8_byte *in = FD_STRDATA(string);
  int c1=' ', c2=' ', c3=' ', c;
  fdtype trigram, trigrams = FD_EMPTY_CHOICE;
  U8_INIT_FIXED_OUTPUT(&out,64,buf);
  if (FD_STRINGP(string)) {
    while ((c = get_stdchar(&in))>=0) {
      c1 = c2; c2 = c3; c3 = c; out.u8_write = out.u8_outbuf;
      u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
      trigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
      FD_ADD_TO_CHOICE(trigrams,trigram);}
    c1 = c2; c2 = c3; c3=' '; out.u8_write = out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    FD_ADD_TO_CHOICE(trigrams,trigram);
    c1 = c2; c2 = c3; c3=' '; out.u8_write = out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    FD_ADD_TO_CHOICE(trigrams,trigram);
    return trigrams;}
  else return fd_type_error(_("string"),"string_trigrams",string);
}

static fdtype string_bigrams(fdtype string)
{
  U8_OUTPUT out; u8_byte buf[64];
  const u8_byte *in = FD_STRDATA(string);
  int c1=' ', c2=' ', c;
  fdtype bigram, bigrams = FD_EMPTY_CHOICE;
  U8_INIT_FIXED_OUTPUT(&out,64,buf);
  if (FD_STRINGP(string)) {
    while ((c = get_stdchar(&in))>=0) {
      c1 = c2; c2 = c; out.u8_write = out.u8_outbuf;
      u8_putc(&out,c1); u8_putc(&out,c2);
      bigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
      FD_ADD_TO_CHOICE(bigrams,bigram);}
    c1 = c2; c2=' '; out.u8_write = out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2);
    bigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    FD_ADD_TO_CHOICE(bigrams,bigram);
    return bigrams;}
  else return fd_type_error(_("string"),"string_bigrams",string);
}

/* String predicates */

static fdtype getnonstring(fdtype choice)
{
  FD_DO_CHOICES(x,choice) {
    if (!(FD_STRINGP(x))) {
      FD_STOP_DO_CHOICES;
      return x;}
    else {}}
  return FD_VOID;
}

static int has_suffix_test(fdtype string,fdtype suffix)
{
  int string_len = FD_STRING_LENGTH(string);
  int suffix_len = FD_STRING_LENGTH(suffix);
  if (suffix_len>string_len) return 0;
  else {
    u8_string string_data = FD_STRING_DATA(string);
    u8_string suffix_data = FD_STRING_DATA(suffix);
    if (strncmp(string_data+(string_len-suffix_len),
                suffix_data,
                suffix_len) == 0)
      return 1;
    else return 0;}
}
static fdtype has_suffix(fdtype string,fdtype suffix)
{
  fdtype notstring;
  if (FD_QCHOICEP(suffix)) suffix = (FD_XQCHOICE(suffix))->qchoiceval;
  if ((FD_STRINGP(string))&&(FD_STRINGP(suffix))) {
    if (has_suffix_test(string,suffix))
      return FD_TRUE;
    else return FD_FALSE;}
  if ((FD_EMPTY_CHOICEP(string))||(FD_EMPTY_CHOICEP(suffix)))
    return FD_FALSE;
  notstring = getnonstring(string);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_suffix/input",notstring);
  else notstring = getnonstring(suffix);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_suffix/suffix",notstring);
  if ((FD_CHOICEP(string))&&(FD_CHOICEP(suffix))) {
    int matched = 0;
    FD_DO_CHOICES(s,string) {
      FD_DO_CHOICES(sx,suffix) {
        matched = has_suffix_test(s,sx);
        if (matched) {FD_STOP_DO_CHOICES; break;}}
      if (matched) {FD_STOP_DO_CHOICES; break;}}
    if (matched)
      return FD_TRUE;
    else return FD_FALSE;}
  else if (FD_CHOICEP(string)) {
    FD_DO_CHOICES(s,string) {
      if (has_suffix_test(s,suffix)) {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else if (FD_CHOICEP(suffix)) {
    FD_DO_CHOICES(sx,suffix) {
      if (has_suffix_test(string,sx)) {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else return FD_FALSE;
}
static fdtype is_suffix(fdtype suffix,fdtype string) {
  return has_suffix(string,suffix); }

static fdtype strip_suffix(fdtype string,fdtype suffix)
{
  fdtype notstring;
  if (FD_QCHOICEP(suffix)) suffix = (FD_XQCHOICE(suffix))->qchoiceval;
  if ((FD_STRINGP(string))&&(FD_STRINGP(suffix))) {
    if (has_suffix_test(string,suffix)) {
      int sufflen = FD_STRLEN(suffix), len = FD_STRLEN(string);
      return fd_substring(FD_STRDATA(string),
                          FD_STRDATA(string)+(len-sufflen));}
    else return fd_incref(string);}
  if ((FD_EMPTY_CHOICEP(string))||(FD_EMPTY_CHOICEP(suffix)))
    return fd_incref(string);
  notstring = getnonstring(string);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_suffix/input",notstring);
  else notstring = getnonstring(suffix);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_suffix/suffix",notstring);
  else {
    fdtype result = FD_EMPTY_CHOICE;
    FD_DO_CHOICES(s,string) {
      int max_sufflen = -1;
      FD_DO_CHOICES(sx,suffix) {
        if (has_suffix_test(s,sx)) {
          int sufflen = FD_STRLEN(sx);
          if (sufflen>max_sufflen) max_sufflen = sufflen;}}
      if (max_sufflen<0) {
        fd_incref(s);
        FD_ADD_TO_CHOICE(result,s);}
      else {
        int len = FD_STRLEN(s);
        fdtype stripped = fd_extract_string
          (NULL,FD_STRDATA(s),FD_STRDATA(s)+(len-max_sufflen));
        FD_ADD_TO_CHOICE(result,stripped);}}
    return result;}
}

static int has_prefix_test(fdtype string,fdtype prefix)
{
  int string_len = FD_STRING_LENGTH(string);
  int prefix_len = FD_STRING_LENGTH(prefix);
  if (prefix_len>string_len) return 0;
  else {
    u8_string string_data = FD_STRING_DATA(string);
    u8_string prefix_data = FD_STRING_DATA(prefix);
    if (strncmp(string_data,prefix_data,prefix_len) == 0)
      return 1;
    else return 0;}
}
static fdtype has_prefix(fdtype string,fdtype prefix)
{
  fdtype notstring;
  if (FD_QCHOICEP(prefix)) prefix = (FD_XQCHOICE(prefix))->qchoiceval;
  if ((FD_STRINGP(string))&&(FD_STRINGP(prefix))) {
    if (has_prefix_test(string,prefix)) return FD_TRUE;
    else return FD_FALSE;}
  if ((FD_EMPTY_CHOICEP(string))||(FD_EMPTY_CHOICEP(prefix)))
    return FD_FALSE;
  notstring = getnonstring(string);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_prefix/input",notstring);
  else notstring = getnonstring(prefix);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_prefix/prefix",notstring);
  if ((FD_CHOICEP(string))&&(FD_CHOICEP(prefix))) {
    int matched = 0;
    FD_DO_CHOICES(s,string) {
      FD_DO_CHOICES(p,prefix) {
        matched = has_prefix_test(s,p);
        if (matched) {
          FD_STOP_DO_CHOICES; break;}}
      if (matched) {
        FD_STOP_DO_CHOICES; break;}}
    if (matched) return FD_TRUE; else return FD_FALSE;}
  else if (FD_CHOICEP(string)) {
    FD_DO_CHOICES(s,string) {
      if (has_prefix_test(s,prefix)) {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else if (FD_CHOICEP(prefix)) {
    FD_DO_CHOICES(p,prefix) {
      if (has_prefix_test(string,p)) {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else return FD_FALSE;
}
static fdtype is_prefix(fdtype prefix,fdtype string) {
  return has_prefix(string,prefix); }

static fdtype strip_prefix(fdtype string,fdtype prefix)
{
  fdtype notstring;
  if (FD_QCHOICEP(prefix)) prefix = (FD_XQCHOICE(prefix))->qchoiceval;
  if ((FD_STRINGP(string))&&(FD_STRINGP(prefix))) {
    if (has_prefix_test(string,prefix)) {}
    else return fd_incref(string);}
  if ((FD_EMPTY_CHOICEP(string))||(FD_EMPTY_CHOICEP(prefix)))
    return fd_incref(string);
  notstring = getnonstring(string);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_prefix/input",notstring);
  else notstring = getnonstring(prefix);
  if (!(FD_VOIDP(notstring)))
    return fd_type_error("string","has_prefix/prefix",notstring);
  else {
    fdtype result = FD_EMPTY_CHOICE;
   FD_DO_CHOICES(s,string) {
      int max_prelen = -1;
      FD_DO_CHOICES(px,prefix) {
        if (has_prefix_test(s,px)) {
          int prelen = FD_STRLEN(px);
          if (prelen>max_prelen) max_prelen = prelen;}}
      if (max_prelen<0) {
        fd_incref(s);
        FD_ADD_TO_CHOICE(result,s);}
      else {
        int len = FD_STRLEN(s);
        fdtype stripped = fd_extract_string
          (NULL,FD_STRDATA(s)+max_prelen,FD_STRDATA(s)+len);
        FD_ADD_TO_CHOICE(result,stripped);}}
    return result;}
}

/* YES/NO */

static int strmatch(u8_string s,fdtype lval,int ignorecase)
{
  u8_string sval = NULL;
  if (FD_STRINGP(lval)) sval = FD_STRDATA(lval);
  else if (FD_SYMBOLP(lval)) sval = FD_SYMBOL_NAME(lval);
  else {}
  if (sval == NULL) return 0;
  else if (ignorecase)
    return (strcasecmp(s,sval)==0);
  else return (strcmp(s,sval)==0);
}

static int check_yesp(u8_string arg,fdtype strings,int ignorecase)
{
  if ((FD_STRINGP(strings))||(FD_SYMBOLP(strings)))
    return strmatch(arg,strings,ignorecase);
  else if (FD_VECTORP(strings)) {
    fdtype *v = FD_VECTOR_DATA(strings);
    int i = 0, lim = FD_VECTOR_LENGTH(strings);
    while (i<lim) {
      fdtype s = FD_VECTOR_REF(v,i); i++;
      if (strmatch(arg,s,ignorecase))
        return 1;}
    return 0;}
  else if (FD_PAIRP(strings)) {
    FD_DOLIST(s,strings) {
      if (strmatch(arg,s,ignorecase)) return 1;}
    return 0;}
  else if ((FD_CHOICEP(strings))||(FD_PRECHOICEP(strings))) {
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
  u8_string string_arg; int ignorecase = 0;
  if (FD_STRINGP(arg)) string_arg = FD_STRDATA(arg);
  else if (FD_SYMBOLP(arg)) {
    string_arg = FD_SYMBOL_NAME(arg); ignorecase = 1;}
  else return fd_type_error("string or symbol","yesp_prim",arg);
  if ((!(FD_VOIDP(yes)))&&(check_yesp(string_arg,yes,ignorecase)))
    return FD_TRUE;
  else if ((!(FD_VOIDP(no)))&&(check_yesp(string_arg,no,ignorecase)))
    return FD_FALSE;
  else return fd_boolstring(FD_STRDATA(arg),((FD_FALSEP(dflt))?(0):(1)));
}

/* STRSEARCH */

static fdtype strmatchp_prim(fdtype pat,fdtype string,fdtype ef)
{
  if (FD_CHOICEP(string)) {
    FD_DO_CHOICES(str,string) {
      fdtype r = strmatchp_prim(pat,str,ef);
      if (FD_ABORTP(r)) {
        FD_STOP_DO_CHOICES;
        return r;}
      else if (FD_TRUEP(r)) {
        FD_STOP_DO_CHOICES;
        return r;}}
    return FD_FALSE;}
  else if (!(FD_UINTP(ef)))
    return fd_type_error("uint","strmatchp_prim",ef);
  else {
    if (FD_TYPEP(pat,fd_regex_type)) {
      int off = fd_regex_op(rx_search,pat,
                          FD_STRDATA(string),FD_STRLEN(string),
                          FD_FIX2INT(ef));
      if (off>=0) return FD_TRUE;
      else return FD_FALSE;}
    else if (FD_STRINGP(pat)) {
      u8_string start = strstr(FD_STRDATA(string),FD_STRDATA(pat));
      if (start) return FD_TRUE;
      else return FD_FALSE;}
    else if (FD_CHOICEP(pat)) {
      FD_DO_CHOICES(p,pat) {
        if (FD_TYPEP(pat,fd_regex_type)) {
          int off = fd_regex_op(rx_search,p,
                              FD_STRDATA(string),FD_STRLEN(string),
                              FD_FIX2INT(ef));
          if (off>=0) {
            FD_STOP_DO_CHOICES;
            return FD_TRUE;}}
        else if (FD_STRINGP(pat)) {
          u8_string start = strstr(FD_STRDATA(string),FD_STRDATA(pat));
          if (start) {
            FD_STOP_DO_CHOICES;
            return FD_TRUE;}}
        else {
          FD_STOP_DO_CHOICES;
          return fd_type_error(StrSearchKey,"strmatchp_prim",p);}}
      return FD_FALSE;}
    else return fd_type_error(StrSearchKey,"strmatchp_prim",pat);}
}

/* Conversion */

static fdtype entity_escape;

static fdtype string2packet(fdtype string,fdtype encoding,fdtype escape)
{
  char *data; int n_bytes;
  const u8_byte *scan, *limit;
  struct U8_TEXT_ENCODING *enc;
  if (FD_VOIDP(encoding)) {
    int n_bytes = FD_STRLEN(string);
    return fd_make_packet(NULL,n_bytes,FD_STRDATA(string));}
  else if (FD_STRINGP(encoding))
    enc = u8_get_encoding(FD_STRDATA(encoding));
  else if (FD_SYMBOLP(encoding))
    enc = u8_get_encoding(FD_SYMBOL_NAME(encoding));
  else return fd_type_error(_("text encoding"),"string2packet",encoding);
  scan = FD_STRDATA(string); limit = scan+FD_STRLEN(string);
  if (FD_EQ(escape,entity_escape))
    data = u8_localize(enc,&scan,limit,'&',0,NULL,&n_bytes);
  else if ((FD_FALSEP(escape)) || (FD_VOIDP(escape)))
    data = u8_localize(enc,&scan,limit,0,0,NULL,&n_bytes);
  else data = u8_localize(enc,&scan,limit,'\\',0,NULL,&n_bytes);
  if (data) {
    fdtype result = fd_make_packet(NULL,n_bytes,data);
    u8_free(data);
    return result;}
  else return FD_ERROR_VALUE;
}

static fdtype x2secret_prim(fdtype arg)
{
  if (FD_TYPEP(arg,fd_secret_type))
    return fd_incref(arg);
  else if (FD_TYPEP(arg,fd_packet_type)) {
    fdtype result = fd_make_packet
      (NULL,FD_PACKET_LENGTH(arg),FD_PACKET_DATA(arg));
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
  else if (FD_STRINGP(arg)) {
    fdtype result = fd_make_packet(NULL,FD_STRLEN(arg),FD_STRDATA(arg));
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
    const u8_byte *scan = FD_PACKET_DATA(packet);
    const u8_byte *limit = scan+FD_PACKET_LENGTH(packet);
    U8_INIT_OUTPUT(&out,2*FD_PACKET_LENGTH(packet));
    struct U8_TEXT_ENCODING *enc;
    if (FD_VOIDP(encoding))
      enc = u8_get_encoding("UTF-8");
    else if (FD_STRINGP(encoding))
      enc = u8_get_encoding(FD_STRDATA(encoding));
    else if (FD_SYMBOLP(encoding))
      enc = u8_get_encoding(FD_SYMBOL_NAME(encoding));
    else return fd_type_error(_("text encoding"),"packet2string",encoding);
    if (u8_convert(enc,0,&out,&scan,limit)<0) {
      u8_free(out.u8_outbuf); return FD_ERROR_VALUE;}
    else return fd_stream2string(&out);}
}

static fdtype string_byte_length(fdtype string)
{
  int len = FD_STRLEN(string);
  return FD_INT(len);
}

/* Fixing embedded NULs */

static fdtype fixnuls(fdtype string)
{
  struct FD_STRING *ss = fd_consptr(fd_string,string,fd_string_type);
  if (strlen(ss->fd_bytes)<ss->fd_bytelen) {
    /* Handle embedded NUL */
    struct U8_OUTPUT out;
    const u8_byte *scan = ss->fd_bytes, *limit = scan+ss->fd_bytelen;
    U8_INIT_OUTPUT(&out,ss->fd_bytelen+8);
    while (scan<limit) {
      if (*scan)
        u8_putc(&out,u8_sgetc(&scan));
      else u8_putc(&out,0);}
    return fd_stream2string(&out);}
  else return fd_incref(string);
}

/* Simple string subst */

static u8_string strsearch(u8_string string,fdtype pat,
                           size_t len,size_t *matchlenp)
{
  if (FD_STRINGP(pat)) {
    *matchlenp = len;
    return strstr(string,FD_STRDATA(pat));}
  else if (FD_TYPEP(pat,fd_regex_type)) {
    int off = fd_regex_op(rx_search,pat,string,len,0);
    if (off<0) return NULL;
    else {
      int matchlen = fd_regex_op(rx_matchlen,pat,string+off,len-off,0);
      if (matchlen<0) {
        u8_string start = string+off, next = start; u8_sgetc(&next);
        /* Not sure when this would happen, but catch it anyway. If
           rx_matchlen fails on a result from rx_search, assume the 
           match is just one character long, so you don't loop. 
           Use sgetc to get the length of the utf-8 encoded character
           at the point. */
        *matchlenp = next-start;
        return string+off;}
      else {
        *matchlenp = matchlen;
        return string+off;}}}
  /* Never reached */
  else return NULL;
}

static fdtype string_subst_prim(fdtype string,fdtype pat,fdtype with)
{
  if (FD_STRLEN(string)==0) return fd_incref(string);
  else if (!((FD_STRINGP(pat))||(FD_TYPEP(pat,fd_regex_type))))
    return fd_type_error("string or regex","string_subst_prim",pat);
  else if ((FD_STRINGP(pat))&&
           (strstr(FD_STRDATA(string),FD_STRDATA(pat)) == NULL))
    return fd_incref(string);
  else {
    u8_string original = FD_STRDATA(string);
    u8_string replace = FD_STRDATA(with);
    size_t patlen = (FD_STRINGP(pat))?(FD_STRING_LENGTH(pat)):(-1);
    size_t startlen = FD_STRLEN(string)*2;
    size_t matchlen = -1;
    u8_string point = strsearch(original,pat,patlen,&matchlen);
    if (point) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,startlen);
      u8_string last = original; while (point) {
        u8_putn(&out,last,point-last); u8_puts(&out,replace);
        last = point+matchlen; 
        point = strsearch(last,pat,patlen,&matchlen);}
      u8_puts(&out,last);
      return fd_stream2string(&out);}
    else return fd_incref(string);}
}

static fdtype string_subst_star(int n,fdtype *args)
{
  fdtype base = args[0]; int i = 1;
  if ((n%2)==0)
    return fd_err(fd_SyntaxError,"string_subst_star",NULL,FD_VOID);
  else while (i<n) {
      if (FD_EXPECT_FALSE
          (!((FD_STRINGP(args[i]))||
             (FD_TYPEP(args[i],fd_regex_type)))))
        return fd_type_error(_("string"),"string_subst_star",args[i]);
      else i++;}
  /* In case we return it. */
  fd_incref(base); i = 1;
  while (i<n) {
    fdtype replace = args[i];
    fdtype with = args[i+1];
    fdtype result = string_subst_prim(base,replace,with);
    i = i+2; fd_decref(base); base = result;}
  return base;
}

static fdtype trim_spaces(fdtype string)
{
  const u8_byte *start = FD_STRDATA(string), *end = start+FD_STRLEN(string);
  const u8_byte *trim_start = start, *trim_end = end;
  const u8_byte *scan = trim_start;
  while (scan<end) {
    int c = u8_sgetc(&scan);
    if (u8_isspace(c)) trim_start = scan;
    else break;}
  scan = trim_end-1;
  while (scan>=trim_start) {
    const u8_byte *cstart = scan; int c;
    while ((cstart>=trim_start) &&
           ((*cstart)>=0x80) &&
           ((*cstart)<0xC0)) cstart--;
    if (cstart<trim_start) break;
    scan = cstart;
    c = u8_sgetc(&scan);
    if (u8_isspace(c)) {
      trim_end = cstart;
      scan = cstart-1;}
    else break;}
  if ((trim_start == start) && (trim_end == end))
    return fd_incref(string);
  else return fd_substring(trim_start,trim_end);
}

/* Glomming */

static fdtype glom_lexpr(int n,fdtype *args)
{
  unsigned char *result_data, *write;
  int i = 0, sumlen = 0; fd_ptr_type result_type = 0;
  const unsigned char **strings, *stringsbuf[16];
  int *lengths, lengthsbuf[16];
  unsigned char *consed, consedbuf[16];
  if (n>16) {
    strings = u8_malloc(sizeof(unsigned char *)*n);
    lengths = u8_malloc(sizeof(int)*n);
    consed = u8_malloc(sizeof(unsigned char)*n);}
  else {
    strings = stringsbuf;
    lengths = lengthsbuf;
    consed = consedbuf;}
  memset(strings,0,sizeof(unsigned char *)*n);
  memset(lengths,0,sizeof(int)*n);
  memset(consed,0,sizeof(unsigned char)*n);
  while (i<n)
    if (FD_STRINGP(args[i])) {
      sumlen = sumlen+FD_STRLEN(args[i]);
      strings[i]=FD_STRDATA(args[i]);
      lengths[i]=FD_STRLEN(args[i]);
      if (result_type==0) result_type = fd_string_type;
      consed[i++]=0;}
    else if (FD_PACKETP(args[i])) {
      sumlen = sumlen+FD_PACKET_LENGTH(args[i]);
      strings[i]=FD_PACKET_DATA(args[i]);
      lengths[i]=FD_PACKET_LENGTH(args[i]);
      if (FD_TYPEP(args[i],fd_secret_type))
        result_type = fd_secret_type;
      else if (result_type!=fd_secret_type)
        result_type = fd_packet_type;
      consed[i++]=0;}
    else if ((FD_FALSEP(args[i]))||(FD_EMPTY_CHOICEP(args[i]))||
             (FD_VOIDP(args[i]))) {
      if (result_type==0) result_type = fd_string_type;
      strings[i]=NULL; lengths[i]=0; consed[i++]=0;}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
      if (result_type==0) result_type = fd_string_type;
      fd_unparse(&out,args[i]);
      sumlen = sumlen+(out.u8_write-out.u8_outbuf);
      strings[i]=out.u8_outbuf;
      lengths[i]=out.u8_write-out.u8_outbuf;
      consed[i++]=1;}
  write = result_data = u8_malloc(sumlen+1);
  i = 0; while (i<n) {
    if (!(strings[i])) {i++; continue;}
    memcpy(write,strings[i],lengths[i]);
    write = write+lengths[i];
    i++;}
  *write='\0';
  i = 0; while (i<n) {
    if (consed[i]) u8_free(strings[i]); i++;}
  if (n>16) {
    u8_free(strings); u8_free(lengths); u8_free(consed);}
  if (result_type == fd_string_type)
    return fd_init_string(NULL,sumlen,result_data);
  else if (result_type == fd_packet_type)
    return fd_init_packet(NULL,sumlen,result_data);
  else {
    fdtype result = fd_init_packet(NULL,sumlen,result_data);
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
}

/* Text if */

static fdtype textif_evalfn(fdtype expr,fd_lexenv env,fd_stack _stack)
{
  fdtype test_expr = fd_get_arg(expr,1), test_val = FD_VOID;
  if (FD_VOIDP(test_expr))
    return fd_err(fd_SyntaxError,"textif_evalfn",NULL,FD_VOID);
  else test_val = fd_eval(test_expr,env);
  if (FD_ABORTED(test_val)) return test_val;
  else if ((FD_FALSEP(test_val))||(FD_EMPTY_CHOICEP(test_val)))
    return fd_make_string(NULL,0,"");
  else {
    fdtype body = fd_get_body(expr,2), len = fd_seq_length(body);
    fd_decref(test_val); 
    if (len==0) return fd_make_string(NULL,0,NULL);
    else if (len==1) {
      fdtype text = fd_eval(fd_get_arg(body,0),env);
      if (FD_STRINGP(text)) return fd_incref(text);
      else if ((FD_FALSEP(text))||(FD_EMPTY_CHOICEP(text)))
        return fd_make_string(NULL,0,"");
      else {
        u8_string as_string = fd_dtype2string(text);
        return fd_init_string(NULL,-1,as_string);}}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      if (FD_PAIRP(body)) {
        FD_DOLIST(text_expr,body) {
          fdtype text = fd_eval(text_expr,env);
          if (FD_ABORTED(text))
            return fd_err("Bad text clause","textif_evalfn",
                          out.u8_outbuf,text_expr);
          else if (FD_STRINGP(text))
            u8_putn(&out,FD_STRDATA(text),FD_STRLEN(text));
          else fd_unparse(&out,text);
          fd_decref(text); text = FD_VOID;}}
      else {
        int i = 0; while (i<len) {
          fdtype text_expr = fd_get_arg(body,i++);
          fdtype text = fd_eval(text_expr,env);
          if (FD_ABORTED(text))
            return fd_err("Bad text clause","textif_evalfn",
                          out.u8_outbuf,text_expr);
          else if (FD_STRINGP(text))
            u8_putn(&out,FD_STRDATA(text),FD_STRLEN(text));
          else fd_unparse(&out,text);
          fd_decref(text); text = FD_VOID;}}
      return fd_block_string(out.u8_write-out.u8_outbuf,out.u8_outbuf);
    }
  }
}

/* Initialization */

FD_EXPORT void fd_init_stringprims_c()
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
            fd_fixnum_type,FD_INT(-1)));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CAPITALIZE",capitalize,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("CAPITALIZE1",capitalize1,1));
  fd_idefn(fd_scheme_module,fd_make_cprim1("DOWNCASE1",string_downcase1,1));

  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("UTF8?",utf8p_prim,1,fd_packet_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("UTF8STRING",utf8string_prim,1,fd_packet_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_cprim3x("EMPTY-STRING?",empty_stringp,1,
                           -1,FD_VOID,-1,FD_FALSE,-1,FD_FALSE));
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
           (fd_make_cprim2x("STRIP-PREFIX",strip_prefix,2,
                            -1,FD_VOID,-1,FD_VOID)));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprim2x("HAS-SUFFIX",has_suffix,2,
                            -1,FD_VOID,-1,FD_VOID)));
  fd_idefn(fd_scheme_module,
           fd_make_cprim2x("IS-SUFFIX",is_suffix,2,
                           fd_string_type,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprim2x("STRIP-SUFFIX",strip_suffix,2,
                            -1,FD_VOID,-1,FD_VOID)));

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
                           fd_string_type,FD_VOID,-1,FD_VOID,
                           fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
           fd_make_cprimn("STRING-SUBST*",string_subst_star,3));
  fd_idefn(fd_scheme_module,
           fd_make_cprim1x("TRIM-SPACES",trim_spaces,1,fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,
           fd_make_ndprim
           (fd_make_cprim3x("STRMATCH?",strmatchp_prim,2,
                            fd_string_type,FD_VOID,-1,FD_VOID,
                            fd_fixnum_type,FD_FIXZERO)));


  fd_idefn(fd_scheme_module,fd_make_cprimn("GLOM",glom_lexpr,1));
  fd_defspecial(fd_scheme_module,"TEXTIF",textif_evalfn);

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

  entity_escape = fd_intern("ENTITIES");

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
