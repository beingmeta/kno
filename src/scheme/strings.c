/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.
*/

static char versionid[] =
  "$Id$";

#define FD_PROVIDE_FASTEVAL 1

#include "fdb/dtype.h"
#include "fdb/eval.h"

#include <libu8/u8.h>
#include <libu8/convert.h>

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
  else return fd_type_error("string or character","lowercasep",string);
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
  else return fd_type_error("string or character","lowercasep",string);
}

static fdtype string_compoundp(fdtype string)
{
  u8_byte *scan=FD_STRDATA(string);
  if (strchr(scan,' ')) return FD_TRUE;
  else return FD_FALSE;
}

/* String conversions */

static fdtype downcase(fdtype string)
{
  if (FD_STRINGP(string)) {
    u8_byte *scan=FD_STRDATA(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c=u8_sgetc(&scan))>=0) {
      int lc=u8_tolower(c); u8_sputc(&out,lc);}
    return fd_init_string(NULL,out.point-out.bytes,out.bytes);}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_tolower(c));}
  else return fd_type_error(_("string or character"),"downcase",string);
    
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
      int lc=u8_toupper(c); u8_sputc(&out,lc);}
    return fd_init_string(NULL,out.point-out.bytes,out.bytes);}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_toupper(c));}
  else return fd_type_error(_("string or character"),"downcase",string);
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
      u8_sputc(&out,oc); word_start=(u8_isspace(c));}
    return fd_init_string(NULL,out.point-out.bytes,out.bytes);}
  else if (FD_CHARACTERP(string)) {
    int c=FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_toupper(c));}
  else return fd_type_error(_("string or character"),"downcase",string);
}

static fdtype string_stdspace(fdtype string)
{
  u8_byte *scan=FD_STRDATA(string); int c, white=1;
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT(&out,64);
  while ((c=u8_sgetc(&scan))>=0) {
    if (u8_isspace(c))
      if (white) {}
      else {u8_sputc(&out,' '); white=1;}
    else {white=0; u8_sputc(&out,c);}}
  if (out.point==out.bytes) {
    u8_free(out.bytes);
    return fdtype_string("");}
  else if (white) {out.point[-1]='\0'; out.point--;}
  return fd_init_string(NULL,out.point-out.bytes,out.bytes);
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
	else {u8_sputc(&out,' '); white=1;}
      else if (u8_ismodifier(c)) white=0;
      else {
	int bc=u8_base_char(c);
	bc=u8_tolower(bc); white=0;
	u8_sputc(&out,bc);}}
    if (out.point==out.bytes) {
      u8_free(out.bytes);
      return fdtype_string("");}
    else if (white) {out.point[-1]='\0'; out.point--;}
    return fd_init_string(NULL,out.point-out.bytes,out.bytes);}
  else return fd_type_error("string","string_stdstring",string);
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
      u8_free(out.bytes);
      return fd_type_error("string","string_append",args[i]);}
  return fd_init_string(NULL,out.point-out.bytes,out.bytes);
}

static fdtype string_prim(int n,fdtype *args)
{
  int i=0;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
  while (i<n)
    if (FD_CHARACTERP(args[i])) {
      u8_putc(&out,FD_CHARCODE(args[i])); i++;}
    else {
      u8_free(out.bytes);
      return fd_type_error("character","string_prim",args[i]);}
  return fd_init_string(NULL,out.point-out.bytes,out.bytes);
}

static fdtype makestring(fdtype len,fdtype character)
{
  struct U8_OUTPUT out;
  if (fd_getint(len)==0) 
    return fd_init_string(NULL,0,NULL);
  else {
    int i=0, n=fd_getint(len), ch=FD_CHAR2CODE(character);
    U8_INIT_OUTPUT(&out,n);
    while (i<n) {u8_sputc(&out,ch); i++;}
    return fd_init_string(NULL,out.point-out.bytes,out.bytes);}
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
      c1=c2; c2=c3; c3=c; out.point=out.bytes;
      u8_sputc(&out,c1); u8_sputc(&out,c2); u8_sputc(&out,c3);
      trigram=fd_init_string(NULL,out.point-out.bytes,u8_strdup(out.bytes));
      FD_ADD_TO_CHOICE(trigrams,trigram);}
    c1=c2; c2=c3; c3=' '; out.point=out.bytes;
    u8_sputc(&out,c1); u8_sputc(&out,c2); u8_sputc(&out,c3);
    trigram=fd_init_string(NULL,out.point-out.bytes,u8_strdup(out.bytes));
    FD_ADD_TO_CHOICE(trigrams,trigram);
    c1=c2; c2=c3; c3=' '; out.point=out.bytes;
    u8_sputc(&out,c1); u8_sputc(&out,c2); u8_sputc(&out,c3);
    trigram=fd_init_string(NULL,out.point-out.bytes,u8_strdup(out.bytes));
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
      c1=c2; c2=c; out.point=out.bytes;
      u8_sputc(&out,c1); u8_sputc(&out,c2);
      bigram=fd_init_string(NULL,out.point-out.bytes,u8_strdup(out.bytes));
      FD_ADD_TO_CHOICE(bigrams,bigram);}
    c1=c2; c2=' '; out.point=out.bytes;
    u8_sputc(&out,c1); u8_sputc(&out,c2);
    bigram=fd_init_string(NULL,out.point-out.bytes,u8_strdup(out.bytes));
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
  else return fd_erreify();
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
      u8_free(out.bytes); return fd_erreify();}
    else return fd_init_string(NULL,out.point-out.bytes,out.bytes);}
}

/* Initialization */

FD_EXPORT void fd_init_strings_c()
{
  fd_register_source_file(versionid);

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
  fd_idefn(fd_scheme_module,fd_make_cprim1("CAPITALIZE",capitalize,1));

  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("COMPOUND?",string_compoundp,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("STDSPACE",string_stdspace,1,
			   fd_string_type,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim1x("STDSTRING",string_stdstring,1,
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
	   fd_make_cprim2x("HAS-SUFFIX",has_suffix,2,
			   fd_string_type,FD_VOID,
			   fd_string_type,FD_VOID));

  fd_idefn(fd_scheme_module,fd_make_cprimn("STRING-APPEND",string_append,0));
  fd_idefn(fd_scheme_module,fd_make_cprimn("STRING",string_prim,0));
  fd_idefn(fd_scheme_module,fd_make_cprim2x("MAKE-STRING",makestring,1,
					    fd_fixnum_type,FD_VOID,
					    fd_character_type,FD_CODE2CHAR(32)));


  fd_idefn(fd_scheme_module,
	   fd_make_cprim3x("STRING->PACKET",string2packet,1,
			   fd_string_type,FD_VOID,
			   -1,FD_VOID,
			   -1,FD_VOID));
  fd_idefn(fd_scheme_module,
	   fd_make_cprim2x("PACKET->STRING",packet2string,1,
			   fd_packet_type,FD_VOID,
			   -1,FD_VOID));

  entity_escape=fd_intern("ENTITIES");

}


/* The CVS log for this file
   $Log: strings.c,v $
   Revision 1.33  2006/02/08 16:53:27  haase
   Fixed empty string bugs

   Revision 1.32  2006/02/03 19:03:36  haase
   Added string/packet functions

   Revision 1.31  2006/01/31 13:47:24  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.30  2006/01/26 14:44:33  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.29  2006/01/01 21:21:05  haase
   Made case functions be generic

   Revision 1.28  2005/12/17 05:55:41  haase
   Added more r4rs functions

   Revision 1.27  2005/10/29 23:51:29  haase
   Added lexicographic and case-insensitive string comparison functions

   Revision 1.26  2005/09/11 19:44:27  haase
   Made capitalize work on compounds

   Revision 1.25  2005/09/04 21:03:10  haase
   Added COMPOUND? string primitive

   Revision 1.24  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.23  2005/07/12 03:12:45  haase
   Added CAPITALIZE primitive

   Revision 1.22  2005/06/08 01:50:43  haase
   Added char-punctuation?

   Revision 1.21  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.20  2005/05/12 22:11:03  haase
   Added character predicates

   Revision 1.19  2005/05/10 18:43:35  haase
   Added context argument to fd_type_error

   Revision 1.18  2005/04/29 04:04:59  haase
   Fixes to space normalization to also trim trailing and leading pspaces

   Revision 1.17  2005/04/25 23:03:53  haase
   Fixed bug in stdstring which only kept the first whitespace break

   Revision 1.16  2005/04/14 16:21:53  haase
   Fix integer/char conversions

   Revision 1.15  2005/04/12 13:27:06  haase
   Made empty lists count as sequences and fixed some static details fd_err bugs

   Revision 1.14  2005/04/11 22:06:59  haase
   Fixed bugs in stdspace and stdstring

   Revision 1.13  2005/04/10 01:34:33  haase
   Added uppercase?/lowercase?/capitalized? primitives

   Revision 1.12  2005/03/06 02:02:52  haase
   Defined BIGRAMS and TRIGRAMS primitives

   Revision 1.11  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.10  2005/03/05 05:58:27  haase
   Various message changes for better initialization

   Revision 1.9  2005/02/21 22:44:12  haase
   Changed log entry

   Revision 1.8  2005/02/21 22:13:08  haase
   Added readable record printing

   Revision 1.7  2005/02/20 14:47:12  haase
   Added has-prefix and has-suffix primitives

   Revision 1.6  2005/02/19 16:21:30  haase
   Added type declarations and left type checking to evaluator, also added char->integer and integer->char

   Revision 1.5  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.4  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
