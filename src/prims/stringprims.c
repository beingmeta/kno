/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/sequences.h"
#include "kno/cprims.h"

#include <libu8/libu8.h>
#include <libu8/u8convert.h>

static u8_string StrSearchKey=_("string search/key");

/* Character functions */

DEFC_PRIM("char->integer",char2integer,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_character_type,KNO_VOID})
static lispval char2integer(lispval arg)
{
  return KNO_INT(KNO_CHAR2CODE(arg));
}

DEFC_PRIM("integer->char",integer2char,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_fixnum_type,KNO_VOID})
static lispval integer2char(lispval arg)
{
  return KNO_CODE2CHAR(kno_getint(arg));
}

DEFC_PRIM("char-alphabetic?",char_alphabeticp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_character_type,KNO_VOID})
static lispval char_alphabeticp(lispval arg)
{
  if (u8_isalpha(KNO_CHAR2CODE(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("char-numeric?",char_numericp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_character_type,KNO_VOID})
static lispval char_numericp(lispval arg)
{
  if (u8_isdigit(KNO_CHAR2CODE(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("char-whitespace?",char_whitespacep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_character_type,KNO_VOID})
static lispval char_whitespacep(lispval arg)
{
  if (u8_isspace(KNO_CHAR2CODE(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("char-upper-case?",char_upper_casep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_character_type,KNO_VOID})
static lispval char_upper_casep(lispval arg)
{
  if (u8_isupper(KNO_CHAR2CODE(arg)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("char-lower-case?",char_lower_casep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_character_type,KNO_VOID})
static lispval char_lower_casep(lispval arg)
{
  if (u8_islower(KNO_CHAR2CODE(arg))) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("char-alphanumeric?",char_alphanumericp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_character_type,KNO_VOID})
static lispval char_alphanumericp(lispval arg)
{
  if (u8_isalnum(KNO_CHAR2CODE(arg))) return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("char-punctuation?",char_punctuationp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_character_type,KNO_VOID})
static lispval char_punctuationp(lispval arg)
{
  if (u8_ispunct(KNO_CHAR2CODE(arg))) return KNO_TRUE;
  else return KNO_FALSE;
}

/* String predicates */

DEFC_PRIM("ascii?",asciip,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"string",kno_string_type,KNO_VOID})
static lispval asciip(lispval string)
{
  const u8_byte *scan = CSTRING(string);
  const u8_byte *limit = scan+STRLEN(string);
  while (scan<limit)
    if (*scan>=0x80) return KNO_FALSE;
    else scan++;
  return KNO_TRUE;
}

DEFC_PRIM("latin1?",latin1p,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"string",kno_string_type,KNO_VOID})
static lispval latin1p(lispval string)
{
  const u8_byte *scan = CSTRING(string);
  const u8_byte *limit = scan+STRLEN(string);
  int c = u8_sgetc(&scan);
  while (scan<limit) {
    if (c>0x100) return KNO_FALSE;
    else c = u8_sgetc(&scan);}
  return KNO_TRUE;
}

DEFC_PRIM("lowercase?",lowercasep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *textarg* (a string or character) "
	  "is lowercase.",
	  {"string",kno_any_type,KNO_VOID})
static lispval lowercasep(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    while ((c = u8_sgetc(&scan))>=0) {
      if (u8_isupper(c)) return KNO_FALSE;}
    return KNO_TRUE;}
  else if (KNO_CHARACTERP(string)) {
    int c = KNO_CHARCODE(string);
    if (u8_islower(c)) return KNO_TRUE;
    else return KNO_FALSE;}
  else return kno_type_error("string or character","lowercasep",string);
}

DEFC_PRIM("uppercase?",uppercasep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *textarg* (a string or character) "
	  "is uppercase.",
	  {"string",kno_any_type,KNO_VOID})
static lispval uppercasep(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    while ((c = u8_sgetc(&scan))>=0) {
      if (u8_islower(c)) return KNO_FALSE;}
    return KNO_TRUE;}
  else if (KNO_CHARACTERP(string)) {
    int c = KNO_CHARCODE(string);
    if (u8_isupper(c)) return KNO_TRUE;
    else return KNO_FALSE;}
  else return kno_type_error("string or character","uppercasep",string);
}

DEFC_PRIM("capitalized?",capitalizedp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns true if *textarg* (a string or character) "
	  "is capitalized.",
	  {"string",kno_any_type,KNO_VOID})
static lispval capitalizedp(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c = u8_sgetc(&scan);
    if (u8_isupper(c)) return KNO_TRUE; else return KNO_FALSE;}
  else if (KNO_CHARACTERP(string)) {
    int c = KNO_CHARCODE(string);
    if (u8_isupper(c)) return KNO_TRUE;
    else return KNO_FALSE;}
  else return kno_type_error("string or character","capitalizedp",string);
}

DEFC_PRIM("somecap?",some_capitalizedp,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "returns true if *string* contains any uppercase "
	  "characters. If *window* is provided, it limits "
	  "the number of characters searched",
	  {"string",kno_string_type,KNO_VOID},
	  {"window_arg",kno_fixnum_type,KNO_VOID})
static lispval some_capitalizedp(lispval string,lispval window_arg)
{
  int window;
  if (KNO_VOIDP(window_arg))
    window = -1;
  else if (!(KNO_UINTP(window_arg)))
    return kno_type_error("uint","some_capitalizedp",window_arg);
  else window = KNO_FIX2INT(window_arg);
  const u8_byte *scan = CSTRING(string);
  int c = u8_sgetc(&scan), i = 0;
  if (c<0)
    return KNO_FALSE;
  else if (window<=0)
    while (c>0) {
      if (u8_isupper(c))
	return KNO_TRUE;
      c = u8_sgetc(&scan);}
  else while ((c>0) && (i<window)) {
      if (u8_isupper(c))
	return KNO_TRUE;
      c = u8_sgetc(&scan); i++;}
  return KNO_FALSE;
}

DEFC_PRIM("empty-string?",empty_stringp,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "returns #t if *string* is empty.",
	  {"string",kno_any_type,KNO_VOID},
	  {"count_vspace_arg",kno_any_type,KNO_FALSE},
	  {"count_nbsp_arg",kno_any_type,KNO_FALSE})
static lispval empty_stringp(lispval string,lispval count_vspace_arg,
			     lispval count_nbsp_arg)
{
  int count_vspace = (!(FALSEP(count_vspace_arg)));
  int count_nbsp = (!(FALSEP(count_nbsp_arg)));
  if (!(STRINGP(string))) return KNO_FALSE;
  else if (STRLEN(string)==0) return KNO_TRUE;
  else {
    const u8_byte *scan = CSTRING(string), *lim = scan+STRLEN(string);
    while (scan<lim) {
      int c = u8_sgetc(&scan);
      if ((count_vspace)&&
	  ((c=='\n')||(c=='\r')||(c=='\f')||(c=='\v')))
	return KNO_FALSE;
      else if ((count_nbsp)&&(c==0x00a0))
	return KNO_FALSE;
      else if (!(u8_isspace(c))) return KNO_FALSE;
      else {}}
    return KNO_TRUE;}
}

DEFC_PRIM("compound-string?",string_compoundp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "contains more than one word",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_compoundp(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string);
    if (strchr(scan,' '))
      return KNO_TRUE;
    else {
      const u8_byte *lim = scan+STRLEN(string);
      int c = u8_sgetc(&scan);
      while ((c>=0) && (scan<lim))
	if (u8_isspace(c)) return KNO_TRUE;
	else c = u8_sgetc(&scan);
      return KNO_FALSE;}}
  else return KNO_FALSE;
}

DEFC_PRIM("multiline-string?",string_multilinep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "contains more than one line",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_multilinep(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string);
    if (strchr(scan,'\n'))
      return KNO_TRUE;
    else {
      const u8_byte *lim = scan+STRLEN(string);
      int c = u8_sgetc(&scan);
      while ((c>=0) && (scan<lim))
	if (u8_isvspace(c))
	  return KNO_TRUE;
	else c = u8_sgetc(&scan);
      return KNO_FALSE;}}
  else return KNO_FALSE;
}

DEFC_PRIM("phrase-length",string_phrase_length,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of whitespace-separated tokens "
	  "in *string*",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_phrase_length(lispval string)
{
  int len = 0;
  const u8_byte *scan = CSTRING(string);
  const u8_byte *lim = scan+STRLEN(string);
  int c = u8_sgetc(&scan);
  if (u8_isspace(c)) while ((u8_isspace(c)) && (c>=0) && (scan<lim)) {
      c = u8_sgetc(&scan);}
  while ((c>=0) && (scan<lim))
    if (u8_isspace(c)) {
      len++; while ((u8_isspace(c)) && (c>=0) && (scan<lim)) {
	c = u8_sgetc(&scan);}
      continue;}
    else c = u8_sgetc(&scan);
  return KNO_INT(len+1);
}

/* String conversions */

DEFC_PRIM("downcase",downcase,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a lowercase version of *textarg* (a "
	  "string or character)",
	  {"string",kno_any_type,KNO_VOID})
static lispval downcase(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_tolower(c);
      u8_putc(&out,lc);}
    return kno_stream2string(&out);}
  else if (KNO_CHARACTERP(string)) {
    int c = KNO_CHARCODE(string);
    return KNO_CODE2CHAR(u8_tolower(c));}
  else if (SYMBOLP(string)) {
    const u8_byte *scan = SYM_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_tolower(c); u8_putc(&out,lc);}
    return kno_stream2string(&out);}
  else return kno_type_error
	 (_("string, symbol, or character"),"downcase",string);

}

DEFC_PRIM("char-downcase",char_downcase,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the lower case version of *char* if there "
	  "is one, otherwise returns *char*",
	  {"ch",kno_any_type,KNO_VOID})
static lispval char_downcase(lispval ch)
{
  int c = KNO_CHARCODE(ch);
  return KNO_CODE2CHAR(u8_tolower(c));
}

DEFC_PRIM("upcase",upcase,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns an uppercase version of *textarg* (a "
	  "string or character)",
	  {"string",kno_any_type,KNO_VOID})
static lispval upcase(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_toupper(c); u8_putc(&out,lc);}
    return kno_stream2string(&out);}
  else if (KNO_CHARACTERP(string)) {
    int c = KNO_CHARCODE(string);
    return KNO_CODE2CHAR(u8_toupper(c));}
  else if (SYMBOLP(string)) {
    const u8_byte *scan = SYM_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_toupper(c); u8_putc(&out,lc);}
    return kno_stream2string(&out);}
  else return kno_type_error(_("string or character"),"upcase",string);
}

DEFC_PRIM("char-upcase",char_upcase,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the upper case version of *char* if there "
	  "is one, otherwise returns *char*",
	  {"ch",kno_any_type,KNO_VOID})
static lispval char_upcase(lispval ch)
{
  int c = KNO_CHARCODE(ch);
  return KNO_CODE2CHAR(u8_toupper(c));
}

DEFC_PRIM("capitalize",capitalize,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a capitalized version of *textarg* (a "
	  "string or character). For a string, the initial "
	  "letters of all the word tokens are converted to "
	  "uppercase (if possible)",
	  {"string",kno_any_type,KNO_VOID})
static lispval capitalize(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    struct U8_OUTPUT out; int word_start = 1;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int oc = ((word_start) ? (u8_toupper(c)) : (u8_tolower(c)));
      u8_putc(&out,oc); word_start = (u8_isspace(c));}
    return kno_stream2string(&out);}
  else if (KNO_CHARACTERP(string)) {
    int c = KNO_CHARCODE(string);
    return KNO_CODE2CHAR(u8_toupper(c));}
  else return kno_type_error(_("string or character"),"capitalize",string);
}

DEFC_PRIM("capitalize1",capitalize1,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a capitalized version of *textarg* (a "
	  "string or character). For a string, the initial "
	  "letter of the first word token is converted to "
	  "uppercase (if possible)",
	  {"string",kno_any_type,KNO_VOID})
static lispval capitalize1(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c = u8_sgetc(&scan);
    if (u8_isupper(c)) return kno_incref(string);
    else {
      struct U8_OUTPUT out;
      U8_INIT_OUTPUT(&out,STRLEN(string)+4);
      u8_putc(&out,u8_toupper(c));
      u8_puts(&out,scan);
      return kno_stream2string(&out);}}
  else return kno_type_error(_("string or character"),"capitalize1",string);
}

DEFC_PRIM("stdcap",string_stdcap,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "normalizes the capitalization of *string*",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_stdcap(lispval string)
{
  if (STRINGP(string)) {
    u8_string str = CSTRING(string), scan = str;
    int fc = u8_sgetc(&scan), c = fc, n_caps = 0, nospace = 1, at_break = 1;
    while (c>=0) {
      if ((at_break)&&(u8_isupper(c))) {
	c = u8_sgetc(&scan);
	if (!(u8_isupper(c))) n_caps++;}
      if (u8_isspace(c)) {nospace = 0; at_break = 1;}
      else if (u8_ispunct(c)) at_break = 1;
      else at_break = 0;
      c = u8_sgetc(&scan);}
    if (nospace) return kno_incref(string);
    else if (n_caps==0) return kno_incref(string);
    else {
      struct U8_OUTPUT out;
      u8_string scan = str, prev = str;
      U8_INIT_OUTPUT(&out,STRLEN(string));
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
      return kno_stream2string(&out);}}
  else return kno_type_error(_("string"),"string_stdcap",string);
}

DEFC_PRIM("stdspace",string_stdspace,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "normalizes the whitespace of *string*. "
	  "*keepvspace*, if true, retains pairs of newlines.",
	  {"string",kno_string_type,KNO_VOID},
	  {"keep_vertical_arg",kno_any_type,KNO_VOID})
static lispval string_stdspace(lispval string,lispval keep_vertical_arg)
{
  int keep_vertical = KNO_TRUEP(keep_vertical_arg);
  const u8_byte *scan = CSTRING(string); int c, white = 1;
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
    return kno_mkstring("");}
  else if (white) {out.u8_write[-1]='\0'; out.u8_write--;}
  return kno_stream2string(&out);
}

DEFC_PRIM("stdstring",string_stdstring,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "normalizes the whitespace and capitalization of "
	  "*string*, also removing diacritical marks and "
	  "other modifiers.",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_stdstring(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c, white = 1;
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
      return kno_mkstring("");}
    else if (white) {out.u8_write[-1]='\0'; out.u8_write--;}
    return kno_stream2string(&out);}
  else return kno_type_error("string","string_stdstring",string);
}

DEFC_PRIM("stdobj",stdobj,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "if *arg* is a string, this normalizes it for "
	  "whitespace and capitalization, otherwise *arg* is "
	  "simply returned.",
	  {"string",kno_any_type,KNO_VOID})
static lispval stdobj(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c, white = 1;
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
      return kno_mkstring("");}
    else if (white) {out.u8_write[-1]='\0'; out.u8_write--;}
    return kno_stream2string(&out);}
  else return kno_incref(string);
}

DEFC_PRIM("basestring",string_basestring,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "removes diacritical marks, etc from *string*.",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_basestring(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int bc = u8_base_char(c);
      u8_putc(&out,bc);}
    if (out.u8_write == out.u8_outbuf) {
      u8_free(out.u8_outbuf);
      return kno_mkstring("");}
    return kno_stream2string(&out);}
  else return kno_type_error("string","string_basestring",string);
}

DEFC_PRIM("startword",string_startword,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the first word for *string*",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_startword(lispval string)
{
  u8_string scan = CSTRING(string), start = scan, last = scan;
  int c = u8_sgetc(&scan);
  if (u8_isspace(c)) {
    start = scan;
    while ((c>0)&&(u8_isspace(c))) c = u8_sgetc(&scan);
    last = scan;}
  while (c>=0) {
    if (u8_isspace(c))
      return kno_substring(start,last);
    else {
      last = scan; c = u8_sgetc(&scan);}}
  return kno_incref(string);
}

DEFC_PRIM("indent-text",indent_text_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "returns a copy of *text* indented by *indent* "
	  "(either a string or a positive fixnum.",
	  {"text",kno_string_type,KNO_VOID},
	  {"indent_arg",kno_any_type,KNO_VOID})
static lispval indent_text_prim(lispval text,lispval indent_arg)
{
  u8_string indent =
    (KNO_STRINGP(indent_arg)) ? (KNO_CSTRING(indent_arg)) : (NULL);
  int indent_len =
    (KNO_FIXNUMP(indent_arg)) ? (KNO_FIX2INT(indent_arg)) : (-1);
  if (indent) {}
  else if (indent_len<0)
    return kno_err("InvalidIndent","indent_text",NULL,indent_arg);
  else if (indent_len==0)
    return kno_incref(text);
  else NO_ELSE;
  int buflen = (indent_len>0) ? (indent_len+1) : (1);
  u8_byte indentbuf[buflen];
  if (indent == NULL) {
    int i = 0; while (i<indent_len) {
      indentbuf[i++]=' ';}
    indentbuf[i]='\0';
    indent=indentbuf;}
  u8_string indented = u8_indent_text(KNO_CSTRING(text),indent);
  return kno_init_string(NULL,-1,indented);
}

DEFC_PRIM("symbolize",symbolize_cprim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "upcases and interns *arg* if it is a string, "
	  "returns *arg* if it is already a symbol, or "
	  "signals an error otherwise",
	  {"arg",kno_any_type,KNO_VOID})
static lispval symbolize_cprim(lispval arg)
{
  if (KNO_SYMBOLP(arg))
    return arg;
  else if (KNO_STRINGP(arg))
    return kno_getsym(KNO_CSTRING(arg));
  else return kno_err("NotAString","symbolize_cprim",NULL,arg);
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

DEFC_PRIM("trigrams",string_trigrams,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns all the bigrams (letter triples) in "
	  "*string* as a choice. This strips modifiers and "
	  "diactricial marks.",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_trigrams(lispval string)
{
  U8_OUTPUT out; u8_byte buf[64];
  const u8_byte *in = CSTRING(string);
  int c1=' ', c2=' ', c3=' ', c;
  lispval trigram, trigrams = EMPTY;
  U8_INIT_FIXED_OUTPUT(&out,64,buf);
  if (STRINGP(string)) {
    while ((c = get_stdchar(&in))>=0) {
      c1 = c2; c2 = c3; c3 = c; out.u8_write = out.u8_outbuf;
      u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
      trigram = kno_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
      CHOICE_ADD(trigrams,trigram);}
    c1 = c2; c2 = c3; c3=' '; out.u8_write = out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram = kno_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    CHOICE_ADD(trigrams,trigram);
    c1 = c2; c2 = c3; c3=' '; out.u8_write = out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram = kno_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    CHOICE_ADD(trigrams,trigram);
    return trigrams;}
  else return kno_type_error(_("string"),"string_trigrams",string);
}

DEFC_PRIM("bigrams",string_bigrams,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns all the bigrams (letter pairs) in "
	  "*string* as a choice. This strips modifiers and "
	  "diactricial marks.",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_bigrams(lispval string)
{
  U8_OUTPUT out; u8_byte buf[64];
  const u8_byte *in = CSTRING(string);
  int c1=' ', c2=' ', c;
  lispval bigram, bigrams = EMPTY;
  U8_INIT_FIXED_OUTPUT(&out,64,buf);
  if (STRINGP(string)) {
    while ((c = get_stdchar(&in))>=0) {
      c1 = c2; c2 = c; out.u8_write = out.u8_outbuf;
      u8_putc(&out,c1); u8_putc(&out,c2);
      bigram = kno_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
      CHOICE_ADD(bigrams,bigram);}
    c1 = c2; c2=' '; out.u8_write = out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2);
    bigram = kno_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    CHOICE_ADD(bigrams,bigram);
    return bigrams;}
  else return kno_type_error(_("string"),"string_bigrams",string);
}

/* UTF8 related primitives */

DEFC_PRIM("utf8?",utf8p_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns #t if *packet* contains a well-formed "
	  "UTF-8 string.",
	  {"packet",kno_packet_type,KNO_VOID})
static lispval utf8p_prim(lispval packet)
{
  if (u8_validp(KNO_PACKET_DATA(packet)))
    return KNO_TRUE;
  else return KNO_FALSE;
}

DEFC_PRIM("utf8string",utf8string_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "converts *packet* to a string if it contains a "
	  "well-formed UTF-8 string, or simply returns it "
	  "otherwise.",
	  {"packet",kno_packet_type,KNO_VOID})
static lispval utf8string_prim(lispval packet)
{
  if (u8_validp(KNO_PACKET_DATA(packet)))
    return kno_make_string(NULL,KNO_PACKET_LENGTH(packet),KNO_PACKET_DATA(packet));
  else return kno_incref(packet);
}

DEFC_PRIM("byte-length",string_byte_length,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns the number of bytes in the UTF-8 "
	  "representation of *string*",
	  {"string",kno_string_type,KNO_VOID})
static lispval string_byte_length(lispval string)
{
  int len = STRLEN(string);
  return KNO_INT(len);
}

/* Fixing embedded NULs */

DEFC_PRIM("fixnuls",fixnuls_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "replaces any null bytes in the utf-8 "
	  "representation of string with a 'not quite "
	  "kosher' multi-byte representation.",
	  {"string",kno_string_type,KNO_VOID})
static lispval fixnuls_prim(lispval string)
{
  struct KNO_STRING *ss = kno_consptr(kno_string,string,kno_string_type);
  if (strlen(ss->str_bytes)<ss->str_bytelen) {
    /* Handle embedded NUL */
    struct U8_OUTPUT out;
    const u8_byte *scan = ss->str_bytes, *limit = scan+ss->str_bytelen;
    U8_INIT_OUTPUT(&out,ss->str_bytelen+8);
    while (scan<limit) {
      if (*scan)
	u8_putc(&out,u8_sgetc(&scan));
      else u8_putc(&out,0);}
    return kno_stream2string(&out);}
  else return kno_incref(string);
}

/* String comparison */

enum STRCMP { str_lt, str_lte, str_eq, str_neq, str_gte, str_gt };

static int docmp(enum STRCMP pred,int rv)
{
  switch (pred) {
  case str_lt:
    return (rv<0);
  case str_lte:
    return (rv<=0);
  case str_eq:
    return (rv==0);
  case str_neq:
    return (rv!=0);
  case str_gte:
    return (rv>=0);
  case str_gt:
    return (rv>0);
  default:
    return -1;
  }
}

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
static lispval compare_strings(enum STRCMP need,int n,kno_argvec stringvals)
{
  int i = 1;
  lispval base_arg = stringvals[0];
  if (!(KNO_STRINGP(base_arg)))
    return kno_type_error("string","compare_strings",base_arg);
  u8_string base = KNO_CSTRING(base_arg);
  while (i < n) {
    lispval arg = stringvals[i++];
    if (!(KNO_STRINGP(arg)))
      return kno_type_error("string","compare_strings",arg);
    u8_string str = KNO_CSTRING(arg);
    int cmp = string_compare(base,str);
    if (! (docmp(need,cmp)) )
      return KNO_FALSE;
    base = str;}
  return KNO_TRUE;
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
  else return KNO_TRUE;
}

static lispval compare_strings_ci(enum STRCMP need,int n,kno_argvec stringvals)
{
  int i = 1;
  lispval base_arg = stringvals[0];
  if (!(KNO_STRINGP(base_arg)))
    return kno_type_error("string","compare_strings",base_arg);
  u8_string base = KNO_CSTRING(base_arg);
  while (i < n) {
    lispval arg = stringvals[i++];
    if (!(KNO_STRINGP(arg)))
      return kno_type_error("string","compare_strings",arg);
    u8_string str = KNO_CSTRING(arg);
    int cmp = string_compare_ci(base,str);
    if (! (docmp(need,cmp)) )
      return KNO_FALSE;
    base = str;}
  return KNO_TRUE;
}

DEFC_PRIMN("string=?",string_eq,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_eq(int n,kno_argvec args) {
  return compare_strings(str_eq,n,args); }

DEFC_PRIMN("string-ci=?",string_ci_eq,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_ci_eq(int n,kno_argvec args) {
  return compare_strings_ci(str_eq,n,args); }

DEFC_PRIMN("string!=?",string_ne,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_ne(int n,kno_argvec args) {
  return compare_strings(str_neq,n,args); }

DEFC_PRIMN("string-ci!=?",string_ci_ne,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_ci_ne(int n,kno_argvec args) {
  return compare_strings_ci(str_neq,n,args); }

DEFC_PRIMN("string<?",string_lt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_lt(int n,kno_argvec args) {
  return compare_strings(str_lt,n,args); }

DEFC_PRIMN("string-ci<?",string_ci_lt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_ci_lt(int n,kno_argvec args) {
  return compare_strings_ci(str_lt,n,args); }

DEFC_PRIMN("string<=?",string_lte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_lte(int n,kno_argvec args) {
  return compare_strings(str_lte,n,args); }

DEFC_PRIMN("string-ci<=?",string_ci_lte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_ci_lte(int n, kno_argvec args) {
  return compare_strings_ci(str_lte,n,args); }

DEFC_PRIMN("string>=?",string_gte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_gte(int n,kno_argvec args) {
  return compare_strings(str_gte,n,args); }

DEFC_PRIMN("string-ci>=?",string_ci_gte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_ci_gte(int n,kno_argvec args) {
  return compare_strings_ci(str_gte,n,args); }

DEFC_PRIMN("string>?",string_gt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_gt(int n,kno_argvec args) {
  return compare_strings(str_gt,n,args); }

DEFC_PRIMN("string-ci>?",string_ci_gt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval string_ci_gt(int n,kno_argvec args) {
  return compare_strings_ci(str_gt,n,args); }

/* Character comparisons */

static lispval compare_chars(enum STRCMP need,int n,kno_argvec charvals)
{
  int i = 1;
  lispval base_arg = charvals[0];
  long long charcode;
  if (KNO_CHARACTERP(base_arg))
    charcode = KNO_CHARCODE(base_arg);
  else if (KNO_FIXNUMP(base_arg))
    charcode = KNO_FIX2INT(base_arg);
  else return kno_type_error("character/codepoint","compare_chars",base_arg);
  if (charcode < 0)
    return kno_type_error("character/codepoint","compare_chars",base_arg);
  while (i < n) {
    lispval arg = charvals[i++]; long long code;
    if (KNO_CHARACTERP(arg))
      code = KNO_CHARCODE(arg);
    else if (KNO_FIXNUMP(arg))
      code = KNO_FIX2INT(arg);
    else return kno_type_error("character/codepoint","compare_chars",arg);
    if (code < 0)
      return kno_type_error("character/codepoint","compare_chars",arg);
    int cmp = (charcode < code) ? (-1) : (charcode == code) ? (0) : (1);
    if (! (docmp(need,cmp)) )
      return KNO_FALSE;
    charcode = code;}
  return KNO_TRUE;
}

static lispval compare_chars_ci(enum STRCMP need,int n,kno_argvec charvals)
{
  int i = 1;
  lispval base_arg = charvals[0];
  long long charcode;
  if (KNO_CHARACTERP(base_arg))
    charcode = KNO_CHARCODE(base_arg);
  else if (KNO_FIXNUMP(base_arg))
    charcode = KNO_FIX2INT(base_arg);
  else return kno_type_error("character/codepoint","compare_chars",base_arg);
  if (charcode < 0)
    return kno_type_error("character/codepoint","compare_chars",base_arg);
  else charcode = u8_tolower(charcode);
  while (i < n) {
    lispval arg = charvals[i++]; long long code;
    if (KNO_CHARACTERP(arg))
      code = KNO_CHARCODE(arg);
    else if (KNO_FIXNUMP(arg))
      code = KNO_FIX2INT(arg);
    else return kno_type_error("character/codepoint","compare_chars",arg);
    if (code < 0)
      return kno_type_error("character/codepoint","compare_chars",arg);
    else code = u8_tolower(code);
    int cmp = (charcode < code) ? (-1) : (charcode == code) ? (0) : (1);
    if (! (docmp(need,cmp)) )
      return KNO_FALSE;
    charcode = code;}
  return KNO_TRUE;
}

DEFC_PRIMN("char=?",char_eq,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_eq(int n,kno_argvec args) {
  return compare_chars(str_eq,n,args); }

DEFC_PRIMN("char-ci=?",char_ci_eq,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_ci_eq(int n,kno_argvec args) {
  return compare_chars_ci(str_eq,n,args); }

DEFC_PRIMN("char!=?",char_ne,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_ne(int n,kno_argvec args) {
  return compare_chars(str_neq,n,args); }

DEFC_PRIMN("char-ci!=?",char_ci_ne,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_ci_ne(int n,kno_argvec args) {
  return compare_chars_ci(str_neq,n,args); }

DEFC_PRIMN("char<?",char_lt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_lt(int n,kno_argvec args) {
  return compare_chars(str_lt,n,args); }

DEFC_PRIMN("char-ci<?",char_ci_lt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_ci_lt(int n,kno_argvec args) {
  return compare_chars_ci(str_lt,n,args); }

DEFC_PRIMN("char<=?",char_lte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_lte(int n,kno_argvec args) {
  return compare_chars(str_lte,n,args); }

DEFC_PRIMN("char-ci<=?",char_ci_lte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_ci_lte(int n, kno_argvec args) {
  return compare_chars_ci(str_lte,n,args); }

DEFC_PRIMN("char>=?",char_gte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_gte(int n,kno_argvec args) {
  return compare_chars(str_gte,n,args); }

DEFC_PRIMN("char-ci>=?",char_ci_gte,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_ci_gte(int n,kno_argvec args) {
  return compare_chars_ci(str_gte,n,args); }

DEFC_PRIMN("char>?",char_gt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_gt(int n,kno_argvec args) {
  return compare_chars(str_gt,n,args); }

DEFC_PRIMN("char-ci>?",char_ci_gt,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval char_ci_gt(int n,kno_argvec args) {
  return compare_chars_ci(str_gt,n,args); }

/* String building */

DEFC_PRIMN("string-append",string_append_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(-1),
	   "creates a new string by appending together each "
	   "of *strings*.")
static lispval string_append_prim(int n,kno_argvec args)
{
  int i = 0;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
  while (i<n)
    if (STRINGP(args[i])) {
      u8_puts(&out,CSTRING(args[i])); i++;}
    else {
      u8_free(out.u8_outbuf);
      return kno_type_error("string","string_append",args[i]);}
  return kno_stream2string(&out);
}

DEFC_PRIMN("string",string_prim,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(-1),
	   "creates a new string by appending together "
	   "strings or characters.")
static lispval string_prim(int n,kno_argvec args)
{
  int i = 0;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
  while (i<n)
    if (KNO_CHARACTERP(args[i])) {
      u8_putc(&out,KNO_CHARCODE(args[i])); i++;}
    else {
      u8_free(out.u8_outbuf);
      return kno_type_error("character","string_prim",args[i]);}
  return kno_stream2string(&out);
}

DEFC_PRIM("make-string",makestring_prim,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "creates a new string of *len* characters, "
	  "initialied to *initchar* which defaults to #\\Space",
	  {"len",kno_fixnum_type,KNO_VOID},
	  {"character",kno_character_type,KNO_VOID})
static lispval makestring_prim(lispval len,lispval character)
{
  struct U8_OUTPUT out;
  if (kno_getint(len)==0)
    return kno_init_string(NULL,0,NULL);
  else {
    int i = 0, n = kno_getint(len);
    int ch = (VOIDP(character)) ? (' ') : (KNO_CHAR2CODE(character));
    U8_INIT_OUTPUT(&out,n);
    while (i<n) {u8_putc(&out,ch); i++;}
    return kno_stream2string(&out);}
}

/* String predicates */

static lispval getnonstring(lispval choice)
{
  DO_CHOICES(x,choice) {
    if (!(STRINGP(x))) {
      KNO_STOP_DO_CHOICES;
      return x;}
    else {}}
  return VOID;
}

static int has_suffix_test(lispval string,lispval suffix)
{
  int string_len = KNO_STRING_LENGTH(string);
  int suffix_len = KNO_STRING_LENGTH(suffix);
  if (suffix_len>string_len) return 0;
  else {
    u8_string string_data = KNO_STRING_DATA(string);
    u8_string suffix_data = KNO_STRING_DATA(suffix);
    if (strncmp(string_data+(string_len-suffix_len),
		suffix_data,
		suffix_len) == 0)
      return 1;
    else return 0;}
}

DEFC_PRIM("has-suffix",has_suffix,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "returns true if *string* starts with any of "
	  "*suffixes* (also strings).",
	  {"string",kno_any_type,KNO_VOID},
	  {"suffix",kno_any_type,KNO_VOID})
static lispval has_suffix(lispval string,lispval suffix)
{
  lispval notstring;
  if (QCHOICEP(suffix)) suffix = (KNO_XQCHOICE(suffix))->qchoiceval;
  if ((STRINGP(string))&&(STRINGP(suffix))) {
    if (has_suffix_test(string,suffix))
      return KNO_TRUE;
    else return KNO_FALSE;}
  if ((EMPTYP(string))||(EMPTYP(suffix)))
    return KNO_FALSE;
  notstring = getnonstring(string);
  if (!(VOIDP(notstring)))
    return kno_type_error("string","has_suffix/input",notstring);
  else notstring = getnonstring(suffix);
  if (!(VOIDP(notstring)))
    return kno_type_error("string","has_suffix/suffix",notstring);
  if ((CHOICEP(string))&&(CHOICEP(suffix))) {
    int matched = 0;
    DO_CHOICES(s,string) {
      DO_CHOICES(sx,suffix) {
	matched = has_suffix_test(s,sx);
	if (matched) {KNO_STOP_DO_CHOICES; break;}}
      if (matched) {KNO_STOP_DO_CHOICES; break;}}
    if (matched)
      return KNO_TRUE;
    else return KNO_FALSE;}
  else if (CHOICEP(string)) {
    DO_CHOICES(s,string) {
      if (has_suffix_test(s,suffix)) {
	KNO_STOP_DO_CHOICES;
	return KNO_TRUE;}}
    return KNO_FALSE;}
  else if (CHOICEP(suffix)) {
    DO_CHOICES(sx,suffix) {
      if (has_suffix_test(string,sx)) {
	KNO_STOP_DO_CHOICES;
	return KNO_TRUE;}}
    return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("is-suffix",is_suffix,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "returns true if *string* starts with *suffix*.",
	  {"suffix",kno_string_type,KNO_VOID},
	  {"string",kno_string_type,KNO_VOID})
static lispval is_suffix(lispval suffix,lispval string) {
  return has_suffix(string,suffix); }

DEFC_PRIM("strip-suffix",strip_suffix,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "removes the longest of *suffixes* from the end of "
	  "*string*, if any.",
	  {"string",kno_any_type,KNO_VOID},
	  {"suffix",kno_any_type,KNO_VOID})
static lispval strip_suffix(lispval string,lispval suffix)
{
  lispval notstring;
  if (QCHOICEP(suffix)) suffix = (KNO_XQCHOICE(suffix))->qchoiceval;
  if ((STRINGP(string))&&(STRINGP(suffix))) {
    if (has_suffix_test(string,suffix)) {
      int sufflen = STRLEN(suffix), len = STRLEN(string);
      return kno_substring(CSTRING(string),
			   CSTRING(string)+(len-sufflen));}
    else return kno_incref(string);}
  if ((EMPTYP(string))||(EMPTYP(suffix)))
    return kno_incref(string);
  notstring = getnonstring(string);
  if (!(VOIDP(notstring)))
    return kno_type_error("string","has_suffix/input",notstring);
  else notstring = getnonstring(suffix);
  if (!(VOIDP(notstring)))
    return kno_type_error("string","has_suffix/suffix",notstring);
  else {
    lispval result = EMPTY;
    DO_CHOICES(s,string) {
      int max_sufflen = -1;
      DO_CHOICES(sx,suffix) {
	if (has_suffix_test(s,sx)) {
	  int sufflen = STRLEN(sx);
	  if (sufflen>max_sufflen) max_sufflen = sufflen;}}
      if (max_sufflen<0) {
	kno_incref(s);
	CHOICE_ADD(result,s);}
      else {
	int len = STRLEN(s);
	lispval stripped = kno_extract_string
	  (NULL,CSTRING(s),CSTRING(s)+(len-max_sufflen));
	CHOICE_ADD(result,stripped);}}
    return result;}
}

static int has_prefix_test(lispval string,lispval prefix)
{
  int string_len = KNO_STRING_LENGTH(string);
  int prefix_len = KNO_STRING_LENGTH(prefix);
  if (prefix_len>string_len) return 0;
  else {
    u8_string string_data = KNO_STRING_DATA(string);
    u8_string prefix_data = KNO_STRING_DATA(prefix);
    if (strncmp(string_data,prefix_data,prefix_len) == 0)
      return 1;
    else return 0;}
}

DEFC_PRIM("has-prefix",has_prefix,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "returns true if *string* starts with any of "
	  "*prefixes* (also strings).",
	  {"string",kno_any_type,KNO_VOID},
	  {"prefix",kno_any_type,KNO_VOID})
static lispval has_prefix(lispval string,lispval prefix)
{
  lispval notstring;
  if (QCHOICEP(prefix)) prefix = (KNO_XQCHOICE(prefix))->qchoiceval;
  if ((STRINGP(string))&&(STRINGP(prefix))) {
    if (has_prefix_test(string,prefix)) return KNO_TRUE;
    else return KNO_FALSE;}
  if ((EMPTYP(string))||(EMPTYP(prefix)))
    return KNO_FALSE;
  notstring = getnonstring(string);
  if (!(VOIDP(notstring)))
    return kno_type_error("string","has_prefix/input",notstring);
  else notstring = getnonstring(prefix);
  if (!(VOIDP(notstring)))
    return kno_type_error("string","has_prefix/prefix",notstring);
  if ((CHOICEP(string))&&(CHOICEP(prefix))) {
    int matched = 0;
    DO_CHOICES(s,string) {
      DO_CHOICES(p,prefix) {
	matched = has_prefix_test(s,p);
	if (matched) {
	  KNO_STOP_DO_CHOICES; break;}}
      if (matched) {
	KNO_STOP_DO_CHOICES; break;}}
    if (matched) return KNO_TRUE; else return KNO_FALSE;}
  else if (CHOICEP(string)) {
    DO_CHOICES(s,string) {
      if (has_prefix_test(s,prefix)) {
	KNO_STOP_DO_CHOICES;
	return KNO_TRUE;}}
    return KNO_FALSE;}
  else if (CHOICEP(prefix)) {
    DO_CHOICES(p,prefix) {
      if (has_prefix_test(string,p)) {
	KNO_STOP_DO_CHOICES;
	return KNO_TRUE;}}
    return KNO_FALSE;}
  else return KNO_FALSE;
}

DEFC_PRIM("is-prefix",is_prefix,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2),
	  "returns true if *string* starts with *prefix*.",
	  {"prefix",kno_string_type,KNO_VOID},
	  {"string",kno_string_type,KNO_VOID})
static lispval is_prefix(lispval prefix,lispval string) {
  return has_prefix(string,prefix); }

DEFC_PRIM("strip-prefix",strip_prefix,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "removes the longest of *suffixes* from the "
	  "beginning of *string*, if any.",
	  {"string",kno_any_type,KNO_VOID},
	  {"prefix",kno_any_type,KNO_VOID})
static lispval strip_prefix(lispval string,lispval prefix)
{
  lispval notstring;
  if (QCHOICEP(prefix)) prefix = (KNO_XQCHOICE(prefix))->qchoiceval;
  if ((STRINGP(string))&&(STRINGP(prefix))) {
    if (has_prefix_test(string,prefix)) {}
    else return kno_incref(string);}
  if ((EMPTYP(string))||(EMPTYP(prefix)))
    return kno_incref(string);
  notstring = getnonstring(string);
  if (!(VOIDP(notstring)))
    return kno_type_error("string","has_prefix/input",notstring);
  else notstring = getnonstring(prefix);
  if (!(VOIDP(notstring)))
    return kno_type_error("string","has_prefix/prefix",notstring);
  else {
    lispval result = EMPTY;
    DO_CHOICES(s,string) {
      int max_prelen = -1;
      DO_CHOICES(px,prefix) {
	if (has_prefix_test(s,px)) {
	  int prelen = STRLEN(px);
	  if (prelen>max_prelen) max_prelen = prelen;}}
      if (max_prelen<0) {
	kno_incref(s);
	CHOICE_ADD(result,s);}
      else {
	int len = STRLEN(s);
	lispval stripped = kno_extract_string
	  (NULL,CSTRING(s)+max_prelen,CSTRING(s)+len);
	CHOICE_ADD(result,stripped);}}
    return result;}
}

/* YES/NO */

static int strmatch(u8_string s,lispval lval,int ignorecase)
{
  u8_string sval = NULL;
  if (STRINGP(lval)) sval = CSTRING(lval);
  else if (SYMBOLP(lval)) sval = SYM_NAME(lval);
  else {}
  if (sval == NULL) return 0;
  else if (ignorecase)
    return (strcasecmp(s,sval)==0);
  else return (strcmp(s,sval)==0);
}

static int check_yesp(u8_string arg,lispval strings,int ignorecase)
{
  if ((STRINGP(strings))||(SYMBOLP(strings)))
    return strmatch(arg,strings,ignorecase);
  else if (VECTORP(strings)) {
    lispval *v = VEC_DATA(strings);
    int i = 0, lim = VEC_LEN(strings);
    while (i<lim) {
      lispval s = VEC_REF(v,i); i++;
      if (strmatch(arg,s,ignorecase))
	return 1;}
    return 0;}
  else if (PAIRP(strings)) {
    KNO_DOLIST(s,strings) {
      if (strmatch(arg,s,ignorecase)) return 1;}
    return 0;}
  else if (CHOICEP(strings)) {
    DO_CHOICES(s,strings) {
      if (strmatch(arg,s,ignorecase)) {
	KNO_STOP_DO_CHOICES;
	return 1;}}
    return 0;}
  else {}
  return 0;
}

DEFC_PRIM("yes?",yesp_prim,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(1),
	  "returns true if *string* is recognized as an "
	  "affirmative natural langauge response.",
	  {"arg",kno_any_type,KNO_VOID},
	  {"dflt",kno_any_type,KNO_FALSE},
	  {"yes",kno_any_type,KNO_VOID},
	  {"no",kno_any_type,KNO_VOID})
static lispval yesp_prim(lispval arg,lispval dflt,lispval yes,lispval no)
{
  u8_string string_arg; int ignorecase = 0;
  if (STRINGP(arg)) string_arg = CSTRING(arg);
  else if (SYMBOLP(arg)) {
    string_arg = SYM_NAME(arg); ignorecase = 1;}
  else return kno_type_error("string or symbol","yesp_prim",arg);
  if ((!(VOIDP(yes)))&&(check_yesp(string_arg,yes,ignorecase)))
    return KNO_TRUE;
  else if ((!(VOIDP(no)))&&(check_yesp(string_arg,no,ignorecase)))
    return KNO_FALSE;
  else return kno_boolstring(CSTRING(arg),((FALSEP(dflt))?(0):(1)));
}

/* STRSEARCH */

DEFC_PRIM("strmatch?",strmatchp_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "return true if any of *patterns* (a choice of "
	  "strings or regexes) matches the substring of "
	  "*string* starting at *pos*.",
	  {"pat",kno_string_type,KNO_VOID},
	  {"string",kno_any_type,KNO_VOID},
	  {"ef",kno_fixnum_type,KNO_VOID})
static lispval strmatchp_prim(lispval pat,lispval string,lispval ef)
{
  if (CHOICEP(string)) {
    DO_CHOICES(str,string) {
      lispval r = strmatchp_prim(pat,str,ef);
      if (KNO_ABORTP(r)) {
	KNO_STOP_DO_CHOICES;
	return r;}
      else if (KNO_TRUEP(r)) {
	KNO_STOP_DO_CHOICES;
	return r;}}
    return KNO_FALSE;}
  else if (!(KNO_UINTP(ef)))
    return kno_type_error("uint","strmatchp_prim",ef);
  else {
    if (TYPEP(pat,kno_regex_type)) {
      int off = kno_regex_op(rx_search,pat,
			     CSTRING(string),STRLEN(string),
			     FIX2INT(ef));
      if (off>=0) return KNO_TRUE;
      else return KNO_FALSE;}
    else if (STRINGP(pat)) {
      u8_string start = strstr(CSTRING(string),CSTRING(pat));
      if (start) return KNO_TRUE;
      else return KNO_FALSE;}
    else if (CHOICEP(pat)) {
      DO_CHOICES(p,pat) {
	if (TYPEP(pat,kno_regex_type)) {
	  int off = kno_regex_op(rx_search,p,
				 CSTRING(string),STRLEN(string),
				 FIX2INT(ef));
	  if (off>=0) {
	    KNO_STOP_DO_CHOICES;
	    return KNO_TRUE;}}
	else if (STRINGP(pat)) {
	  u8_string start = strstr(CSTRING(string),CSTRING(pat));
	  if (start) {
	    KNO_STOP_DO_CHOICES;
	    return KNO_TRUE;}}
	else {
	  KNO_STOP_DO_CHOICES;
	  return kno_type_error(StrSearchKey,"strmatchp_prim",p);}}
      return KNO_FALSE;}
    else return kno_type_error(StrSearchKey,"strmatchp_prim",pat);}
}

/* Conversion */

static lispval entity_escape;

DEFC_PRIM("string->packet",string2packet,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "converts *string* into a packet based on "
	  "*encoding*. Characters which cannot be converted "
	  "are escaped based on the *escape* arg. If "
	  "*escape* is '**entities**, HTML escapes are used, "
	  "if *escape* is #f (the default), non-represented "
	  "characters are ignored, and otherwise \\u escapes "
	  "are emitted. *encoding* is either a string or "
	  "symbol naming the encoding scheme.",
	  {"string",kno_string_type,KNO_VOID},
	  {"encoding",kno_any_type,KNO_VOID},
	  {"escape",kno_any_type,KNO_VOID})
static lispval string2packet(lispval string,lispval encoding,lispval escape)
{
  char *data; ssize_t n_bytes;
  const u8_byte *scan, *limit;
  struct U8_TEXT_ENCODING *enc;
  if ( (VOIDP(encoding)) || (FALSEP(encoding)) ) {
    int n_bytes = STRLEN(string);
    return kno_make_packet(NULL,n_bytes,CSTRING(string));}
  else if (STRINGP(encoding))
    enc = u8_get_encoding(CSTRING(encoding));
  else if (SYMBOLP(encoding))
    enc = u8_get_encoding(SYM_NAME(encoding));
  else return kno_type_error(_("text encoding"),"string2packet",encoding);
  scan = CSTRING(string); limit = scan+STRLEN(string);
  if (KNO_EQ(escape,entity_escape))
    data = u8_localize(enc,&scan,limit,'&',0,NULL,&n_bytes);
  else if ((FALSEP(escape)) || (VOIDP(escape)))
    data = u8_localize(enc,&scan,limit,0,0,NULL,&n_bytes);
  else data = u8_localize(enc,&scan,limit,'\\',0,NULL,&n_bytes);
  if (data) {
    lispval result = kno_make_packet(NULL,n_bytes,data);
    u8_free(data);
    return result;}
  else return KNO_ERROR;
}

DEFC_PRIM("->secret",x2secret_prim,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "returns a *secret packet* from *object* whose "
	  "printed representation does not disclose it's "
	  "value but which can be used as arguments to many "
	  "string combination functions. If *object* is a "
	  "packet, it's bytes are used directly, if *object* "
	  "is a string, it's UTF-8 representation is used. "
	  "Otherwise, the object is unparsed into a string.",
	  {"arg",kno_any_type,KNO_VOID})
static lispval x2secret_prim(lispval arg)
{
  if (TYPEP(arg,kno_secret_type))
    return kno_incref(arg);
  else if (TYPEP(arg,kno_packet_type)) {
    lispval result = kno_make_packet
      (NULL,KNO_PACKET_LENGTH(arg),KNO_PACKET_DATA(arg));
    KNO_SET_CONS_TYPE(result,kno_secret_type);
    return result;}
  else if (STRINGP(arg)) {
    lispval result = kno_make_packet(NULL,STRLEN(arg),CSTRING(arg));
    KNO_SET_CONS_TYPE(result,kno_secret_type);
    return result;}
  else return kno_type_error("string/packet","x2secret_prim",arg);
}

DEFC_PRIM("packet->string",packet2string,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "converts the bytes of *packet* into a string "
	  "based on the named *encoding* which can be a "
	  "string or symbol and defaults to UTF-8",
	  {"packet",kno_packet_type,KNO_VOID},
	  {"encoding",kno_any_type,KNO_FALSE})
static lispval packet2string(lispval packet,lispval encoding)
{
  if (KNO_PACKET_LENGTH(packet)==0)
    return kno_mkstring("");
  else {
    struct U8_OUTPUT out;
    const u8_byte *scan = KNO_PACKET_DATA(packet);
    const u8_byte *limit = scan+KNO_PACKET_LENGTH(packet);
    U8_INIT_OUTPUT(&out,2*KNO_PACKET_LENGTH(packet));
    struct U8_TEXT_ENCODING *enc;
    if ( (VOIDP(encoding)) || (FALSEP(encoding)) )
      enc = u8_get_encoding("UTF-8");
    else if (STRINGP(encoding))
      enc = u8_get_encoding(CSTRING(encoding));
    else if (SYMBOLP(encoding))
      enc = u8_get_encoding(SYM_NAME(encoding));
    else return kno_type_error(_("text encoding"),"packet2string",encoding);
    if (u8_convert(enc,0,&out,&scan,limit)<0) {
      u8_free(out.u8_outbuf);
      return KNO_ERROR;}
    else return kno_stream2string(&out);}
}

/* Simple string subst */

static u8_string strsearch(u8_string string,lispval pat,
			   size_t len,size_t *matchlenp)
{
  if (STRINGP(pat)) {
    *matchlenp = len;
    return strstr(string,CSTRING(pat));}
  else if (TYPEP(pat,kno_regex_type)) {
    int off = kno_regex_op(rx_search,pat,string,len,0);
    if (off<0) return NULL;
    else {
      int matchlen = kno_regex_op(rx_matchlen,pat,string+off,len-off,0);
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

DEFC_PRIM("string-subst",string_subst_prim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(3),
	  "replaces substrings of *string* which match "
	  "*pattern* with *replacement*. *pattern* can be a "
	  "string or a regex and arguments substitutions are "
	  "not available.",
	  {"string",kno_string_type,KNO_VOID},
	  {"pat",kno_any_type,KNO_VOID},
	  {"with",kno_string_type,KNO_VOID})
static lispval string_subst_prim(lispval string,lispval pat,lispval with)
{
  if (STRLEN(string)==0) return kno_incref(string);
  else if (!((STRINGP(pat))||(TYPEP(pat,kno_regex_type))))
    return kno_type_error("string or regex","string_subst_prim",pat);
  else if ((STRINGP(pat))&&
	   (strstr(CSTRING(string),CSTRING(pat)) == NULL))
    return kno_incref(string);
  else {
    u8_string original = CSTRING(string);
    u8_string replace = CSTRING(with);
    size_t patlen = (STRINGP(pat))?(KNO_STRING_LENGTH(pat)):(-1);
    size_t startlen = STRLEN(string)*2;
    size_t matchlen = -1;
    u8_string point = strsearch(original,pat,patlen,&matchlen);
    if (point) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,startlen);
      u8_string last = original; while (point) {
	u8_putn(&out,last,point-last); u8_puts(&out,replace);
	last = point+matchlen;
	point = strsearch(last,pat,patlen,&matchlen);}
      u8_puts(&out,last);
      return kno_stream2string(&out);}
    else return kno_incref(string);}
}

DEFC_PRIMN("string-subst*",string_subst_star,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(3),
	   "replaces substrings matching each *pattern* with "
	   "the corresponding *subst* starting from left to "
	   "right.")
static lispval string_subst_star(int n,kno_argvec args)
{
  lispval base = args[0]; int i = 1;
  if ((n%2)==0)
    return kno_err(kno_SyntaxError,"string_subst_star",NULL,VOID);
  else while (i<n) {
      if (RARELY
	  (!((STRINGP(args[i]))||
	     (TYPEP(args[i],kno_regex_type)))))
	return kno_type_error(_("string"),"string_subst_star",args[i]);
      else i++;}
  /* In case we return it. */
  kno_incref(base); i = 1;
  while (i<n) {
    lispval replace = args[i];
    lispval with = args[i+1];
    lispval result = string_subst_prim(base,replace,with);
    i = i+2; kno_decref(base); base = result;}
  return base;
}

DEFC_PRIM("trim-spaces",trim_spaces,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "removes leading and trailing whitespace from "
	  "*string*.",
	  {"string",kno_string_type,KNO_VOID})
static lispval trim_spaces(lispval string)
{
  const u8_byte *start = CSTRING(string), *end = start+STRLEN(string);
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
    return kno_incref(string);
  else return kno_substring(trim_start,trim_end);
}

/* Glomming */

DEFC_PRIMN("glom",glom_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(-1),
	   "returns a string concatenating either the text of "
	   "*objects* (if they are strings) or the text of "
	   "their printed representations (if they are not). "
	   "If any of the arguments are choices, `GLOM` "
	   "returns a choice of object combinations.")
static lispval glom_lexpr(int n,kno_argvec args)
{
  if (n == 0) return kno_make_string(NULL,0,NULL);
  unsigned char *result_data, *write;
  int i = 0, sumlen = 0; kno_lisp_type result_type = 0;
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
    if (STRINGP(args[i])) {
      sumlen = sumlen+STRLEN(args[i]);
      strings[i]=CSTRING(args[i]);
      lengths[i]=STRLEN(args[i]);
      if (result_type==0) result_type = kno_string_type;
      consed[i++]=0;}
    else if (PACKETP(args[i])) {
      sumlen = sumlen+KNO_PACKET_LENGTH(args[i]);
      strings[i]=KNO_PACKET_DATA(args[i]);
      lengths[i]=KNO_PACKET_LENGTH(args[i]);
      if (TYPEP(args[i],kno_secret_type))
	result_type = kno_secret_type;
      else if (result_type!=kno_secret_type)
	result_type = kno_packet_type;
      consed[i++]=0;}
    else if ((FALSEP(args[i]))||(EMPTYP(args[i]))||
	     (VOIDP(args[i]))) {
      if (result_type==0) result_type = kno_string_type;
      strings[i]=NULL; lengths[i]=0; consed[i++]=0;}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,64);
      if (result_type==0) result_type = kno_string_type;
      kno_unparse(&out,args[i]);
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
  if (result_type == kno_string_type)
    return kno_init_string(NULL,sumlen,result_data);
  else if (result_type == kno_packet_type)
    return kno_init_packet(NULL,sumlen,result_data);
  else {
    lispval result = kno_init_packet(NULL,sumlen,result_data);
    KNO_SET_CONS_TYPE(result,kno_secret_type);
    return result;}
}

/* Text if */

static lispval textif_evalfn(lispval expr,kno_lexenv env,kno_stack _stack)
{
  lispval test_expr = kno_get_arg(expr,1), test_val = VOID;
  if (VOIDP(test_expr))
    return kno_err(kno_SyntaxError,"textif_evalfn",NULL,VOID);
  else test_val = kno_eval(test_expr,env,_stack);
  if (KNO_ABORTED(test_val)) return test_val;
  else if ((FALSEP(test_val))||(EMPTYP(test_val)))
    return kno_make_string(NULL,0,"");
  else {
    lispval body = kno_get_body(expr,2), len = kno_seq_length(body);
    kno_decref(test_val);
    if (len==0) return kno_make_string(NULL,0,NULL);
    else if (len==1) {
      lispval text = kno_eval(kno_get_arg(body,0),env,_stack);
      if (STRINGP(text)) return kno_incref(text);
      else if ((FALSEP(text))||(EMPTYP(text)))
	return kno_make_string(NULL,0,"");
      else {
	u8_string as_string = kno_lisp2string(text);
	return kno_init_string(NULL,-1,as_string);}}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      if (PAIRP(body)) {
	KNO_DOLIST(text_expr,body) {
	  lispval text = kno_eval(text_expr,env,_stack);
	  if (KNO_ABORTED(text))
	    return kno_err("Bad text clause","textif_evalfn",
			   out.u8_outbuf,text_expr);
	  else if (STRINGP(text))
	    u8_putn(&out,CSTRING(text),STRLEN(text));
	  else kno_unparse(&out,text);
	  kno_decref(text); text = VOID;}}
      else {
	int i = 0; while (i<len) {
	  lispval text_expr = kno_get_arg(body,i++);
	  lispval text = kno_eval(text_expr,env,_stack);
	  if (KNO_ABORTED(text))
	    return kno_err("Bad text clause","textif_evalfn",
			   out.u8_outbuf,text_expr);
	  else if (STRINGP(text))
	    u8_putn(&out,CSTRING(text),STRLEN(text));
	  else kno_unparse(&out,text);
	  kno_decref(text); text = VOID;}}
      return kno_block_string(out.u8_write-out.u8_outbuf,out.u8_outbuf);
    }
  }
}

/* Initialization */

KNO_EXPORT void kno_init_stringprims_c()
{
  u8_register_source_file(_FILEINFO);

  link_local_cprims();

  kno_def_evalfn(kno_scheme_module,"TEXTIF",textif_evalfn,
		 "*undocumented*");

  entity_escape = kno_intern("entities");
}

static void link_local_cprims()
{
  KNO_LINK_CPRIMN("glom",glom_lexpr,kno_scheme_module);
  KNO_LINK_CPRIM("trim-spaces",trim_spaces,1,kno_scheme_module);
  KNO_LINK_CPRIM("string-subst",string_subst_prim,3,kno_scheme_module);
  KNO_LINK_CPRIMN("string-subst*",string_subst_star,kno_scheme_module);
  KNO_LINK_CPRIM("packet->string",packet2string,2,kno_scheme_module);
  KNO_LINK_CPRIM("->secret",x2secret_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("string->packet",string2packet,3,kno_scheme_module);
  KNO_LINK_CPRIM("strmatch?",strmatchp_prim,3,kno_scheme_module);
  KNO_LINK_CPRIM("yes?",yesp_prim,4,kno_scheme_module);
  KNO_LINK_CPRIM("strip-prefix",strip_prefix,2,kno_scheme_module);
  KNO_LINK_CPRIM("is-prefix",is_prefix,2,kno_scheme_module);
  KNO_LINK_CPRIM("has-prefix",has_prefix,2,kno_scheme_module);
  KNO_LINK_CPRIM("strip-suffix",strip_suffix,2,kno_scheme_module);
  KNO_LINK_CPRIM("is-suffix",is_suffix,2,kno_scheme_module);
  KNO_LINK_CPRIM("has-suffix",has_suffix,2,kno_scheme_module);
  KNO_LINK_CPRIM("make-string",makestring_prim,2,kno_scheme_module);
  KNO_LINK_CPRIM("indent-text",indent_text_prim,2,kno_scheme_module);
  KNO_LINK_CPRIMN("string",string_prim,kno_scheme_module);
  KNO_LINK_CPRIMN("string-append",string_append_prim,kno_scheme_module);
  KNO_LINK_CPRIMN("char-ci>?",char_ci_gt,kno_scheme_module);
  KNO_LINK_CPRIMN("char>?",char_gt,kno_scheme_module);
  KNO_LINK_CPRIMN("char-ci>=?",char_ci_gte,kno_scheme_module);
  KNO_LINK_CPRIMN("char>=?",char_gte,kno_scheme_module);
  KNO_LINK_CPRIMN("char-ci<=?",char_ci_lte,kno_scheme_module);
  KNO_LINK_CPRIMN("char<=?",char_lte,kno_scheme_module);
  KNO_LINK_CPRIMN("char-ci<?",char_ci_lt,kno_scheme_module);
  KNO_LINK_CPRIMN("char<?",char_lt,kno_scheme_module);
  KNO_LINK_CPRIMN("char-ci!=?",char_ci_ne,kno_scheme_module);
  KNO_LINK_CPRIMN("char!=?",char_ne,kno_scheme_module);
  KNO_LINK_CPRIMN("char-ci=?",char_ci_eq,kno_scheme_module);
  KNO_LINK_CPRIMN("char=?",char_eq,kno_scheme_module);
  KNO_LINK_CPRIMN("string-ci>?",string_ci_gt,kno_scheme_module);
  KNO_LINK_CPRIMN("string>?",string_gt,kno_scheme_module);
  KNO_LINK_CPRIMN("string-ci>=?",string_ci_gte,kno_scheme_module);
  KNO_LINK_CPRIMN("string>=?",string_gte,kno_scheme_module);
  KNO_LINK_CPRIMN("string-ci<=?",string_ci_lte,kno_scheme_module);
  KNO_LINK_CPRIMN("string<=?",string_lte,kno_scheme_module);
  KNO_LINK_CPRIMN("string-ci<?",string_ci_lt,kno_scheme_module);
  KNO_LINK_CPRIMN("string<?",string_lt,kno_scheme_module);
  KNO_LINK_CPRIMN("string-ci!=?",string_ci_ne,kno_scheme_module);
  KNO_LINK_CPRIMN("string!=?",string_ne,kno_scheme_module);
  KNO_LINK_CPRIMN("string-ci=?",string_ci_eq,kno_scheme_module);
  KNO_LINK_CPRIMN("string=?",string_eq,kno_scheme_module);
  KNO_LINK_CPRIM("fixnuls",fixnuls_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("byte-length",string_byte_length,1,kno_scheme_module);
  KNO_LINK_CPRIM("utf8string",utf8string_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("utf8?",utf8p_prim,1,kno_scheme_module);
  KNO_LINK_CPRIM("bigrams",string_bigrams,1,kno_scheme_module);
  KNO_LINK_CPRIM("trigrams",string_trigrams,1,kno_scheme_module);
  KNO_LINK_CPRIM("symbolize",symbolize_cprim,1,kno_scheme_module);
  KNO_LINK_CPRIM("startword",string_startword,1,kno_scheme_module);
  KNO_LINK_CPRIM("basestring",string_basestring,1,kno_scheme_module);
  KNO_LINK_CPRIM("stdobj",stdobj,1,kno_scheme_module);
  KNO_LINK_CPRIM("stdstring",string_stdstring,1,kno_scheme_module);
  KNO_LINK_CPRIM("stdspace",string_stdspace,2,kno_scheme_module);
  KNO_LINK_CPRIM("stdcap",string_stdcap,1,kno_scheme_module);
  KNO_LINK_CPRIM("capitalize1",capitalize1,1,kno_scheme_module);
  KNO_LINK_CPRIM("capitalize",capitalize,1,kno_scheme_module);
  KNO_LINK_CPRIM("char-upcase",char_upcase,1,kno_scheme_module);
  KNO_LINK_CPRIM("upcase",upcase,1,kno_scheme_module);
  KNO_LINK_CPRIM("char-downcase",char_downcase,1,kno_scheme_module);
  KNO_LINK_CPRIM("downcase",downcase,1,kno_scheme_module);
  KNO_LINK_CPRIM("phrase-length",string_phrase_length,1,kno_scheme_module);
  KNO_LINK_CPRIM("multiline-string?",string_multilinep,1,kno_scheme_module);
  KNO_LINK_CPRIM("compound-string?",string_compoundp,1,kno_scheme_module);
  KNO_LINK_CPRIM("empty-string?",empty_stringp,3,kno_scheme_module);
  KNO_LINK_CPRIM("somecap?",some_capitalizedp,2,kno_scheme_module);
  KNO_LINK_CPRIM("capitalized?",capitalizedp,1,kno_scheme_module);
  KNO_LINK_CPRIM("uppercase?",uppercasep,1,kno_scheme_module);
  KNO_LINK_CPRIM("lowercase?",lowercasep,1,kno_scheme_module);
  KNO_LINK_CPRIM("latin1?",latin1p,1,kno_scheme_module);
  KNO_LINK_CPRIM("ascii?",asciip,1,kno_scheme_module);
  KNO_LINK_CPRIM("char-punctuation?",char_punctuationp,1,kno_scheme_module);
  KNO_LINK_CPRIM("char-alphanumeric?",char_alphanumericp,1,kno_scheme_module);
  KNO_LINK_CPRIM("char-lower-case?",char_lower_casep,1,kno_scheme_module);
  KNO_LINK_CPRIM("char-upper-case?",char_upper_casep,1,kno_scheme_module);
  KNO_LINK_CPRIM("char-whitespace?",char_whitespacep,1,kno_scheme_module);
  KNO_LINK_CPRIM("char-numeric?",char_numericp,1,kno_scheme_module);
  KNO_LINK_CPRIM("char-alphabetic?",char_alphabeticp,1,kno_scheme_module);
  KNO_LINK_CPRIM("integer->char",integer2char,1,kno_scheme_module);
  KNO_LINK_CPRIM("char->integer",char2integer,1,kno_scheme_module);
}
