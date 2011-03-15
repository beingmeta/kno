/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2011 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"

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

static fdtype empty_stringp(fdtype string)
{
  if (!(FD_STRINGP(string))) return FD_FALSE;
  else if (FD_STRLEN(string)==0) return FD_TRUE;
  else {
    u8_byte *scan=FD_STRDATA(string), *lim=scan+FD_STRLEN(string);
    int c=u8_sgetc(&scan);
    if (!(u8_isspace(c))) return FD_FALSE;
    while ((c>=0) && (scan<lim))
      if (u8_isspace(c)) c=u8_sgetc(&scan);
      else return FD_FALSE;
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
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_tolower(c));}
  else if (FD_SYMBOLP(string)) {
    u8_byte *scan=FD_SYMBOL_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      int lc=u8_tolower(c); u8_putc(&out,lc);}
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
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
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_toupper(c));}
  else if (FD_SYMBOLP(string)) {
    u8_byte *scan=FD_SYMBOL_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      int lc=u8_toupper(c); u8_putc(&out,lc);}
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
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
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
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
      return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}}
  else return fd_type_error(_("string or character"),"capitalize1",string);
}

static fdtype downcase1(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c=u8_sgetc(&scan);
    if (u8_islower(c)) return fd_incref(string);
    else {
      struct U8_OUTPUT out;
      U8_INIT_OUTPUT(&out,FD_STRLEN(string)+4);
      u8_putc(&out,u8_tolower(c));
      u8_puts(&out,scan);
      return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}}
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
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
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
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
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
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else return fd_type_error("string","string_basestring",string);
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
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
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
  return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);
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
    return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
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
      trigram=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,u8_strdup(out.u8_outbuf));
      FD_ADD_TO_CHOICE(trigrams,trigram);}
    c1=c2; c2=c3; c3=' '; out.u8_outptr=out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,u8_strdup(out.u8_outbuf));
    FD_ADD_TO_CHOICE(trigrams,trigram);
    c1=c2; c2=c3; c3=' '; out.u8_outptr=out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,u8_strdup(out.u8_outbuf));
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
      bigram=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,u8_strdup(out.u8_outbuf));
      FD_ADD_TO_CHOICE(bigrams,bigram);}
    c1=c2; c2=' '; out.u8_outptr=out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2);
    bigram=fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,u8_strdup(out.u8_outbuf));
    FD_ADD_TO_CHOICE(bigrams,bigram);
    return bigrams;}
  else return fd_type_error(_("string"),"string_bigrams",string);
}

/* String predicates */

static fdtype has_suffix(fdtype string,fdtype suffix)
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
static fdtype is_suffix(fdtype suffix,fdtype string) { return has_suffix(string,suffix); }

static fdtype has_prefix(fdtype string,fdtype prefix)
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
static fdtype is_prefix(fdtype prefix,fdtype string) { return has_prefix(string,prefix); }

/* Conversion */

static fdtype entity_escape;

static fdtype string2packet(fdtype string,fdtype encoding,fdtype escape)
{
  char *data; int n_bytes; u8_byte *scan, *limit;
  struct U8_TEXT_ENCODING *enc;
  if (FD_VOIDP(encoding)) {
    int n_bytes=FD_STRLEN(string);
    u8_byte *data=u8_malloc(n_bytes);
    strncpy(data,FD_STRDATA(string),n_bytes);
    return fd_init_packet(NULL,n_bytes,data);}
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
  if (data)
    return fd_init_packet(NULL,n_bytes,data);
  else return FD_ERROR_VALUE;
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
    else return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
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
    return fd_init_string
      (NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
  else return fd_incref(string);
}

/* Simple string subst */

static fdtype string_subst_prim(fdtype string,fdtype substring,fdtype with)
{
  if (FD_STRLEN(string)==0) return fd_incref(string);
  else {
    u8_string original=FD_STRDATA(string);
    u8_string search=FD_STRDATA(substring);
    u8_string replace=FD_STRDATA(with);
    int searchlen=FD_STRING_LENGTH(substring);
    u8_string point=strstr(original,search);
    if (point) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,2*FD_STRLEN(string));
      u8_string last=original; while (point) {
	u8_putn(&out,last,point-last); u8_puts(&out,replace);
	last=point+searchlen; point=strstr(last,search);}
      u8_puts(&out,last);
      return fd_init_string(NULL,out.u8_outptr-out.u8_outbuf,out.u8_outbuf);}
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

/* Initialization */

FD_EXPORT void fd_init_strings_c()
{
  fd_register_source_file(versionid);

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
  fd_idefn(fd_scheme_module,fd_make_cprim1("DOWNCASE1",downcase1,1));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("EMPTY-STRING?",empty_stringp,1,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("COMPOUND-STRING?",string_compoundp,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("COMPOUND?",string_compoundp,1,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("STDSPACE",string_stdspace,1,
			   fd_string_type,FD_VOID,-1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("STDSTRING",string_stdstring,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("BASESTRING",string_basestring,1,
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
	   fd_make_cprim2x("HAS-PREFIX",has_prefix,2,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("IS-PREFIX",is_prefix,2,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("HAS-SUFFIX",has_suffix,2,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("IS-SUFFIX",is_suffix,2,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID));

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



  fd_idefn(fd_scheme_module,
	   fd_make_cprim3x("STRING->PACKET",string2packet,1,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("PACKET->STRING",packet2string,1,
			   fd_packet_type,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("FIXNULS",fixnuls,1,fd_string_type,FD_VOID));

  entity_escape=fd_intern("ENTITIES");

}
