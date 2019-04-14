/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
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
#include "framerd/cprims.h"

#include <libu8/libu8.h>
#include <libu8/u8convert.h>

static u8_string StrSearchKey=_("string search/key");

/* Character functions */

DCLPRIM1("CHAR->INTEGER",char2integer,MAX_ARGS(1),
         "`(CHAR->INTEGER *char*)`",
         fd_character_type,FD_VOID)
static lispval char2integer(lispval arg)
{
  return FD_INT(FD_CHAR2CODE(arg));
}

DCLPRIM1("INTEGER->CHAR",integer2char,MAX_ARGS(1),
         "`(CHAR->INTEGER *char*)`",
         fd_fixnum_type,FD_VOID)
static lispval integer2char(lispval arg)
{
  return FD_CODE2CHAR(fd_getint(arg));
}

DCLPRIM1("CHAR-ALPHABETIC?",char_alphabeticp,MAX_ARGS(1),
         "`(CHAR-ALPHABETIC? *char*)`",
         fd_character_type,FD_VOID)
static lispval char_alphabeticp(lispval arg)
{
  if (u8_isalpha(FD_CHAR2CODE(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

DCLPRIM1("CHAR-NUMERIC?",char_numericp,MAX_ARGS(1),
         "`(CHAR-NUMERIC? *char*)`",
         fd_character_type,FD_VOID)
static lispval char_numericp(lispval arg)
{
  if (u8_isdigit(FD_CHAR2CODE(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

DCLPRIM1("CHAR-WHITESPACE?",char_whitespacep,MAX_ARGS(1),
         "`(CHAR-WHITESPACE? *char*)`",
         fd_character_type,FD_VOID)
static lispval char_whitespacep(lispval arg)
{
  if (u8_isspace(FD_CHAR2CODE(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

DCLPRIM1("CHAR-UPPER-CASE?",char_upper_casep,MAX_ARGS(1),
         "`(CHAR-UPPER-CASE? *char*)`",
         fd_character_type,FD_VOID)
static lispval char_upper_casep(lispval arg)
{
  if (u8_isupper(FD_CHAR2CODE(arg)))
    return FD_TRUE;
  else return FD_FALSE;
}

DCLPRIM1("CHAR-LOWER-CASE?",char_lower_casep,MAX_ARGS(1),
         "`(CHAR-LOWER-CASE? *char*)`",
         fd_character_type,FD_VOID)
static lispval char_lower_casep(lispval arg)
{
  if (u8_islower(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

DCLPRIM1("CHAR-ALPHANUMERIC?",char_alphanumericp,MAX_ARGS(1),
         "`(CHAR-ALPHANUMERIC? *char*)`",
         fd_character_type,FD_VOID)
static lispval char_alphanumericp(lispval arg)
{
  if (u8_isalnum(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

DCLPRIM1("CHAR-PUNCTUATION?",char_punctuationp,MAX_ARGS(1),
         "`(CHAR-PUNCTUATION? *char*)`",
         fd_character_type,FD_VOID)
static lispval char_punctuationp(lispval arg)
{
  if (u8_ispunct(FD_CHAR2CODE(arg))) return FD_TRUE;
  else return FD_FALSE;
}

/* String predicates */

DCLPRIM1("ASCII?",asciip,MAX_ARGS(1),
         "`(ASCII? *string*)`",
         fd_string_type,FD_VOID)
static lispval asciip(lispval string)
{
  const u8_byte *scan = CSTRING(string);
  const u8_byte *limit = scan+STRLEN(string);
  while (scan<limit)
    if (*scan>=0x80) return FD_FALSE;
    else scan++;
  return FD_TRUE;
}

DCLPRIM1("LATIN1?",latin1p,MAX_ARGS(1),
         "`(LATIN1? *string*)`",
         fd_string_type,FD_VOID)
static lispval latin1p(lispval string)
{
  const u8_byte *scan = CSTRING(string);
  const u8_byte *limit = scan+STRLEN(string);
  int c = u8_sgetc(&scan);
  while (scan<limit) {
    if (c>0x100) return FD_FALSE;
    else c = u8_sgetc(&scan);}
  return FD_TRUE;
}

DCLPRIM("LOWERCASE?",lowercasep,MAX_ARGS(1),
        "`(LOWERCASE? *textarg*)` returns true if *textarg* "
        "(a string or character) is lowercase.")
static lispval lowercasep(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    while ((c = u8_sgetc(&scan))>=0) {
      if (u8_isupper(c)) return FD_FALSE;}
    return FD_TRUE;}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    if (u8_islower(c)) return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("string or character","lowercasep",string);
}

DCLPRIM("UPPERCASE?",uppercasep,MAX_ARGS(1),
        "`(UPPERCASE? *textarg*)` returns true if *textarg* "
        "(a string or character) is uppercase.")
static lispval uppercasep(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    while ((c = u8_sgetc(&scan))>=0) {
      if (u8_islower(c)) return FD_FALSE;}
    return FD_TRUE;}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    if (u8_isupper(c)) return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("string or character","uppercasep",string);
}

DCLPRIM("CAPITALIZED?",capitalizedp,MAX_ARGS(1),
        "`(CAPITALIZED? *textarg*)` returns true if *textarg* "
        "(a string or character) is capitalized.")
static lispval capitalizedp(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c = u8_sgetc(&scan);
    if (u8_isupper(c)) return FD_TRUE; else return FD_FALSE;}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    if (u8_isupper(c)) return FD_TRUE;
    else return FD_FALSE;}
  else return fd_type_error("string or character","capitalizedp",string);
}

DCLPRIM2("SOMECAP?",some_capitalizedp,MAX_ARGS(2)|MIN_ARGS(1),
         "`(SOMECAP? *string* *window*)` returns true if *string* "
         "contains any uppercase characters. If *window* is provided, "
         "it limits the number of characters searched",
         fd_string_type,FD_VOID,fd_fixnum_type,FD_VOID)
static lispval some_capitalizedp(lispval string,lispval window_arg)
{
  int window;
  if (FD_VOIDP(window_arg))
    window = -1;
  else if (!(FD_UINTP(window_arg)))
    return fd_type_error("uint","some_capitalizedp",window_arg);
  else window = FD_FIX2INT(window_arg);
  const u8_byte *scan = CSTRING(string);
  int c = u8_sgetc(&scan), i = 0;
  if (c<0)
    return FD_FALSE;
  else if (window<=0)
    while (c>0) {
      if (u8_isupper(c))
        return FD_TRUE;
      c = u8_sgetc(&scan);}
  else while ((c>0) && (i<window)) {
      if (u8_isupper(c))
        return FD_TRUE;
      c = u8_sgetc(&scan); i++;}
  return FD_FALSE;
}

DCLPRIM3("EMPTY-STRING?",empty_stringp,MIN_ARGS(1),
         "`(EMPTY-STRING? *string*)` returns #t if *string* "
         "is empty.",
         -1,FD_VOID,-1,FD_FALSE,-1,FD_FALSE)
static lispval empty_stringp(lispval string,lispval count_vspace_arg,
                            lispval count_nbsp_arg)
{
  int count_vspace = (!(FALSEP(count_vspace_arg)));
  int count_nbsp = (!(FALSEP(count_nbsp_arg)));
  if (!(STRINGP(string))) return FD_FALSE;
  else if (STRLEN(string)==0) return FD_TRUE;
  else {
    const u8_byte *scan = CSTRING(string), *lim = scan+STRLEN(string);
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

DCLPRIM1("COMPOUND-STRING?",string_compoundp,MAX_ARGS(1),
         "`(COMPOUND-STRING? *string*)` contains more than one word",
         fd_string_type,FD_VOID)
static lispval string_compoundp(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string);
    if (strchr(scan,' '))
      return FD_TRUE;
    else {
      const u8_byte *lim = scan+STRLEN(string);
      int c = u8_sgetc(&scan);
      while ((c>=0) && (scan<lim))
        if (u8_isspace(c)) return FD_TRUE;
        else c = u8_sgetc(&scan);
      return FD_FALSE;}}
  else return FD_FALSE;
}

DCLPRIM1("MULTILINE-STRING?",string_multilinep,MAX_ARGS(1),
         "`(MULTILINE-STRING? *string*)` contains more than one line",
         fd_string_type,FD_VOID)
static lispval string_multilinep(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string);
    if (strchr(scan,'\n'))
      return FD_TRUE;
    else {
      const u8_byte *lim = scan+STRLEN(string);
      int c = u8_sgetc(&scan);
      while ((c>=0) && (scan<lim))
        if (u8_isvspace(c))
          return FD_TRUE;
        else c = u8_sgetc(&scan);
      return FD_FALSE;}}
  else return FD_FALSE;
}

DCLPRIM1("PHRASE-LENGTH",string_phrase_length,MAX_ARGS(1),
         "`(PHRASE-LENGTH *string*)` returns the number of "
         "whitespace-separated tokens in *string*",
         fd_string_type,FD_VOID)
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
  return FD_INT(len+1);
}

/* String conversions */

DCLPRIM1("DOWNCASE",downcase,MAX_ARGS(1),
         "`(DOWNCASE *textarg*)` returns a lowercase "
         "version of *textarg* (a string or character)",
         -1,FD_VOID)
static lispval downcase(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_tolower(c);
      u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_tolower(c));}
  else if (SYMBOLP(string)) {
    const u8_byte *scan = SYM_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_tolower(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else return fd_type_error
         (_("string, symbol, or character"),"downcase",string);

}
DCLPRIM1("CHAR-DOWNCASE",char_downcase,MAX_ARGS(1),
         "`(CHAR-DOWNCASE *char*)` returns the lower case "
         "version of *char* if there is one, otherwise "
         "returns *char*",
         -1,FD_VOID)
static lispval char_downcase(lispval ch)
{
  int c = FD_CHARCODE(ch);
  return FD_CODE2CHAR(u8_tolower(c));
}

DCLPRIM1("UPCASE",upcase,MAX_ARGS(1),
         "`(UPCASE *textarg*)` returns an uppercase "
         "version of *textarg* (a string or character)",
         -1,FD_VOID)
static lispval upcase(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_toupper(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else if (FD_CHARACTERP(string)) {
    int c = FD_CHARCODE(string);
    return FD_CODE2CHAR(u8_toupper(c));}
  else if (SYMBOLP(string)) {
    const u8_byte *scan = SYM_NAME(string); int c;
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,64);
    while ((c = u8_sgetc(&scan))>=0) {
      int lc = u8_toupper(c); u8_putc(&out,lc);}
    return fd_stream2string(&out);}
  else return fd_type_error(_("string or character"),"upcase",string);
}
DCLPRIM1("CHAR-UPCASE",char_upcase,MAX_ARGS(1),
         "`(CHAR-UPCASE *char*)` returns the upper case "
         "version of *char* if there is one, otherwise "
         "returns *char*",
         -1,FD_VOID)
static lispval char_upcase(lispval ch)
{
  int c = FD_CHARCODE(ch);
  return FD_CODE2CHAR(u8_toupper(c));
}

DCLPRIM1("CAPITALIZE",capitalize,MAX_ARGS(1),
         "`(CAPITALIZE *textarg*)` returns a capitalized "
         "version of *textarg* (a string or character). "
         "For a string, the initial letters of all the word "
         "tokens are converted to uppercase (if possible)",
         -1,FD_VOID)
static lispval capitalize(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c;
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

DCLPRIM1("CAPITALIZE1",capitalize1,MAX_ARGS(1),
         "`(CAPITALIZE1 *textarg*)` returns a capitalized "
         "version of *textarg* (a string or character). "
         "For a string, the initial letter of the first word "
         "token is converted to uppercase (if possible)",
         -1,FD_VOID)
static lispval capitalize1(lispval string)
{
  if (STRINGP(string)) {
    const u8_byte *scan = CSTRING(string); int c = u8_sgetc(&scan);
    if (u8_isupper(c)) return fd_incref(string);
    else {
      struct U8_OUTPUT out;
      U8_INIT_OUTPUT(&out,STRLEN(string)+4);
      u8_putc(&out,u8_toupper(c));
      u8_puts(&out,scan);
      return fd_stream2string(&out);}}
  else return fd_type_error(_("string or character"),"capitalize1",string);
}

DCLPRIM1("STDCAP",string_stdcap,MAX_ARGS(1),
         "`(STDCAP *string*)` normalizes the capitalization "
         "of *string*",
         fd_string_type,FD_VOID)
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
    if (nospace) return fd_incref(string);
    else if (n_caps==0) return fd_incref(string);
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
      return fd_stream2string(&out);}}
  else return fd_type_error(_("string"),"string_stdcap",string);
}

DCLPRIM2("STDSPACE",string_stdspace,MAX_ARGS(2)|MIN_ARGS(1),
         "`(STDSPACE *string* *keepvspace*)` normalizes the whitespace "
         "of *string*. *keepvspace*, if true, retains pairs of newlines.",
         fd_string_type,FD_VOID,-1,FD_VOID)
static lispval string_stdspace(lispval string,lispval keep_vertical_arg)
{
  int keep_vertical = FD_TRUEP(keep_vertical_arg);
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
    return lispval_string("");}
  else if (white) {out.u8_write[-1]='\0'; out.u8_write--;}
  return fd_stream2string(&out);
}

DCLPRIM1("STDSTRING",string_stdstring,MAX_ARGS(1),
         "`(STDSTRING *string*)` normalizes the whitespace and capitalization "
         "of *string*, also removing diacritical marks and other modifiers.",
         fd_string_type,FD_VOID)
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
      return lispval_string("");}
    else if (white) {out.u8_write[-1]='\0'; out.u8_write--;}
    return fd_stream2string(&out);}
  else return fd_type_error("string","string_stdstring",string);
}

DCLPRIM1("STDOBJ",stdobj,MAX_ARGS(1),
         "`(STDOBJ *arg*)` if *arg* is a string, this normalizes "
         "its whitespace and capitalization, otherwise *arg* is "
         "simply returned.",
         -1,FD_VOID)
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
      return lispval_string("");}
    else if (white) {out.u8_write[-1]='\0'; out.u8_write--;}
    return fd_stream2string(&out);}
  else return fd_incref(string);
}

DCLPRIM1("BASESTRING",string_basestring,MAX_ARGS(1),
         "`(BASESTRING *string*)` removes diacritical marks, etc "
         "from *string*.",
         fd_string_type,FD_VOID)
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
      return lispval_string("");}
    return fd_stream2string(&out);}
  else return fd_type_error("string","string_basestring",string);
}

DCLPRIM1("STARTWORD",string_startword,MAX_ARGS(1),
         "`(STARTWORD *string*)` returns the first word for *string*",
         fd_string_type,FD_VOID)
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
      return fd_substring(start,last);
    else {
      last = scan; c = u8_sgetc(&scan);}}
  return fd_incref(string);
}

DCLPRIM("SYMBOLIZE",symbolize_cprim,MAX_ARGS(1),
        "`(SYMBOLIZE *arg*)` upcases and interns *arg* "
        "if it is a string, returns *arg* if it is already "
        "a symbol, or signals an error otherwise")
static lispval symbolize_cprim(lispval arg)
{
  if (FD_SYMBOLP(arg))
    return arg;
  else if (FD_STRINGP(arg))
    return fd_symbolize(FD_CSTRING(arg));
  else return fd_err("NotAString","symbolize_cprim",NULL,arg);
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

DCLPRIM1("TRIGRAMS",string_trigrams,MAX_ARGS(1),
         "`(TRIGRAMS *string*)` returns all the bigrams (letter triples) "
         "in *string* as a choice. This strips modifiers and diactricial "
         "marks.",
         fd_string_type,FD_VOID)
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
      trigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
      CHOICE_ADD(trigrams,trigram);}
    c1 = c2; c2 = c3; c3=' '; out.u8_write = out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    CHOICE_ADD(trigrams,trigram);
    c1 = c2; c2 = c3; c3=' '; out.u8_write = out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2); u8_putc(&out,c3);
    trigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    CHOICE_ADD(trigrams,trigram);
    return trigrams;}
  else return fd_type_error(_("string"),"string_trigrams",string);
}

DCLPRIM1("BIGRAMS",string_bigrams,MAX_ARGS(1),
         "`(BIGRAMS *string*)` returns all the bigrams (letter pairs) "
         "in *string* as a choice. This strips modifiers and diactricial "
         "marks.",
         fd_string_type,FD_VOID)
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
      bigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
      CHOICE_ADD(bigrams,bigram);}
    c1 = c2; c2=' '; out.u8_write = out.u8_outbuf;
    u8_putc(&out,c1); u8_putc(&out,c2);
    bigram = fd_make_string(NULL,out.u8_write-out.u8_outbuf,out.u8_outbuf);
    CHOICE_ADD(bigrams,bigram);
    return bigrams;}
  else return fd_type_error(_("string"),"string_bigrams",string);
}

/* UTF8 related primitives */

DCLPRIM1("UTF8?",utf8p_prim,MAX_ARGS(1),
         "`(UTF8? *packet*)` returns #t if *packet* "
         "contains a well-formed UTF-8 string.",
         fd_packet_type,FD_VOID)
static lispval utf8p_prim(lispval packet)
{
  if (u8_validp(FD_PACKET_DATA(packet)))
    return FD_TRUE;
  else return FD_FALSE;
}
DCLPRIM1("UTF8STRING",utf8string_prim,MAX_ARGS(1),
         "`(UTF8STRING *packet*)` converts *packet* to a string "
         "if it contains a well-formed UTF-8 string, or simply "
         "returns it otherwise.",
         fd_packet_type,FD_VOID)
static lispval utf8string_prim(lispval packet)
{
  if (u8_validp(FD_PACKET_DATA(packet)))
    return fd_make_string(NULL,FD_PACKET_LENGTH(packet),FD_PACKET_DATA(packet));
  else return fd_incref(packet);
}

DCLPRIM1("BYTE-LENGTH",string_byte_length,MAX_ARGS(1),
         "`(BYTE-LENGTH *string*)` returns the number of bytes "
         "in the UTF-8 representation of *string*",
         fd_string_type,FD_VOID)
static lispval string_byte_length(lispval string)
{
  int len = STRLEN(string);
  return FD_INT(len);
}

/* Fixing embedded NULs */

DCLPRIM1("FIXNULS",fixnuls_prim,MAX_ARGS(1),
         "`(FIXNULS *string*)` replaces any null bytes in the "
         "utf-8 representation of string with a 'not quite kosher' "
         "multi-byte representation.",
         fd_string_type,FD_VOID)
static lispval fixnuls_prim(lispval string)
{
  struct FD_STRING *ss = fd_consptr(fd_string,string,fd_string_type);
  if (strlen(ss->str_bytes)<ss->str_bytelen) {
    /* Handle embedded NUL */
    struct U8_OUTPUT out;
    const u8_byte *scan = ss->str_bytes, *limit = scan+ss->str_bytelen;
    U8_INIT_OUTPUT(&out,ss->str_bytelen+8);
    while (scan<limit) {
      if (*scan)
        u8_putc(&out,u8_sgetc(&scan));
      else u8_putc(&out,0);}
    return fd_stream2string(&out);}
  else return fd_incref(string);
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
static lispval compare_strings(enum STRCMP need,int n,lispval *stringvals)
{
  int i = 1;
  lispval base_arg = stringvals[0];
  if (!(FD_STRINGP(base_arg)))
    return fd_type_error("string","compare_strings",base_arg);
  u8_string base = FD_CSTRING(base_arg);
  while (i < n) {
    lispval arg = stringvals[i++];
    if (!(FD_STRINGP(arg)))
      return fd_type_error("string","compare_strings",arg);
    u8_string str = FD_CSTRING(arg);
    int cmp = string_compare(base,str);
    if (! (docmp(need,cmp)) )
      return FD_FALSE;
    base = str;}
  return FD_TRUE;
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
  else return FD_TRUE;
}

static lispval compare_strings_ci(enum STRCMP need,int n,lispval *stringvals)
{
  int i = 1;
  lispval base_arg = stringvals[0];
  if (!(FD_STRINGP(base_arg)))
    return fd_type_error("string","compare_strings",base_arg);
  u8_string base = FD_CSTRING(base_arg);
  while (i < n) {
    lispval arg = stringvals[i++];
    if (!(FD_STRINGP(arg)))
      return fd_type_error("string","compare_strings",arg);
    u8_string str = FD_CSTRING(arg);
    int cmp = string_compare_ci(base,str);
    if (! (docmp(need,cmp)) )
      return FD_FALSE;
    base = str;}
  return FD_TRUE;
}

DCLPRIM("STRING=?",string_eq,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(string=? *strings...*)`")
static lispval string_eq(int n,lispval *args) {
  return compare_strings(str_eq,n,args); }

DCLPRIM("STRING-CI=?",string_ci_eq,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING-CI=? *strings...*)`")
static lispval string_ci_eq(int n,lispval *args) {
  return compare_strings_ci(str_eq,n,args); }

DCLPRIM("STRING!=?",string_ne,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING!=? *strings...*)`")
static lispval string_ne(int n,lispval *args) {
  return compare_strings(str_neq,n,args); }

DCLPRIM("STRING-CI!=?",string_ci_ne,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING-CI!=? *strings...*)`")
static lispval string_ci_ne(int n,lispval *args) {
  return compare_strings_ci(str_neq,n,args); }

DCLPRIM("STRING<?",string_lt,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING<? *strings...*)`")
static lispval string_lt(int n,lispval *args) {
  return compare_strings(str_lt,n,args); }

DCLPRIM("STRING-CI<?",string_ci_lt,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING-CI<? *strings...*)`")
static lispval string_ci_lt(int n,lispval *args) {
  return compare_strings_ci(str_lt,n,args); }

DCLPRIM("STRING<=?",string_lte,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING<=? *strings...*)`")
static lispval string_lte(int n,lispval *args) {
  return compare_strings(str_lte,n,args); }

DCLPRIM("STRING-CI<=?",string_ci_lte,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING-CI<=? *strings...*)`")
static lispval string_ci_lte(int n, lispval *args) {
  return compare_strings_ci(str_lte,n,args); }

DCLPRIM("STRING>=?",string_gte,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING>=? *strings...*)`")
static lispval string_gte(int n,lispval *args) {
  return compare_strings(str_gte,n,args); }

DCLPRIM("STRING-CI>=?",string_ci_gte,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING-CI>=? *strings...*)`")
static lispval string_ci_gte(int n,lispval *args) {
  return compare_strings_ci(str_gte,n,args); }

DCLPRIM("STRING>?",string_gt,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING>? *strings...*)`")
static lispval string_gt(int n,lispval *args) {
  return compare_strings(str_gt,n,args); }

DCLPRIM("STRING-CI>?",string_ci_gt,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(STRING-CI>? *strings...*)`")
static lispval string_ci_gt(int n,lispval *args) {
  return compare_strings_ci(str_gt,n,args); }

/* Character comparisons */

static lispval compare_chars(enum STRCMP need,int n,lispval *charvals)
{
  int i = 1;
  lispval base_arg = charvals[0];
  long long charcode;
  if (FD_CHARACTERP(base_arg))
    charcode = FD_CHARCODE(base_arg);
  else if (FD_FIXNUMP(base_arg))
    charcode = FD_FIX2INT(base_arg);
  else return fd_type_error("character/codepoint","compare_chars",base_arg);
  if (charcode < 0)
    return fd_type_error("character/codepoint","compare_chars",base_arg);
  while (i < n) {
    lispval arg = charvals[i++]; long long code;
    if (FD_CHARACTERP(arg))
      code = FD_CHARCODE(arg);
    else if (FD_FIXNUMP(arg))
      code = FD_FIX2INT(arg);
    else return fd_type_error("character/codepoint","compare_chars",arg);
    if (code < 0)
      return fd_type_error("character/codepoint","compare_chars",arg);
    int cmp = (charcode < code) ? (-1) : (charcode == code) ? (0) : (1);
    if (! (docmp(need,cmp)) )
      return FD_FALSE;
    charcode = code;}
  return FD_TRUE;
}

static lispval compare_chars_ci(enum STRCMP need,int n,lispval *charvals)
{
  int i = 1;
  lispval base_arg = charvals[0];
  long long charcode;
  if (FD_CHARACTERP(base_arg))
    charcode = FD_CHARCODE(base_arg);
  else if (FD_FIXNUMP(base_arg))
    charcode = FD_FIX2INT(base_arg);
  else return fd_type_error("character/codepoint","compare_chars",base_arg);
  if (charcode < 0)
    return fd_type_error("character/codepoint","compare_chars",base_arg);
  else charcode = u8_tolower(charcode);
  while (i < n) {
    lispval arg = charvals[i++]; long long code;
    if (FD_CHARACTERP(arg))
      code = FD_CHARCODE(arg);
    else if (FD_FIXNUMP(arg))
      code = FD_FIX2INT(arg);
    else return fd_type_error("character/codepoint","compare_chars",arg);
    if (code < 0)
      return fd_type_error("character/codepoint","compare_chars",arg);
    else code = u8_tolower(code);
    int cmp = (charcode < code) ? (-1) : (charcode == code) ? (0) : (1);
    if (! (docmp(need,cmp)) )
      return FD_FALSE;
    charcode = code;}
  return FD_TRUE;
}

DCLPRIM("CHAR=?",char_eq,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(char=? *chars...*)`")
static lispval char_eq(int n,lispval *args) {
  return compare_chars(str_eq,n,args); }

DCLPRIM("CHAR-CI=?",char_ci_eq,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR-CI=? *chars...*)`")
static lispval char_ci_eq(int n,lispval *args) {
  return compare_chars_ci(str_eq,n,args); }

DCLPRIM("CHAR!=?",char_ne,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR!=? *chars...*)`")
static lispval char_ne(int n,lispval *args) {
  return compare_chars(str_neq,n,args); }

DCLPRIM("CHAR-CI!=?",char_ci_ne,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR-CI!=? *chars...*)`")
static lispval char_ci_ne(int n,lispval *args) {
  return compare_chars_ci(str_neq,n,args); }

DCLPRIM("CHAR<?",char_lt,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR<? *chars...*)`")
static lispval char_lt(int n,lispval *args) {
  return compare_chars(str_lt,n,args); }

DCLPRIM("CHAR-CI<?",char_ci_lt,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR-CI<? *chars...*)`")
static lispval char_ci_lt(int n,lispval *args) {
  return compare_chars_ci(str_lt,n,args); }

DCLPRIM("CHAR<=?",char_lte,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR<=? *chars...*)`")
static lispval char_lte(int n,lispval *args) {
  return compare_chars(str_lte,n,args); }

DCLPRIM("CHAR-CI<=?",char_ci_lte,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR-CI<=? *chars...*)`")
static lispval char_ci_lte(int n, lispval *args) {
  return compare_chars_ci(str_lte,n,args); }

DCLPRIM("CHAR>=?",char_gte,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR>=? *chars...*)`")
static lispval char_gte(int n,lispval *args) {
  return compare_chars(str_gte,n,args); }

DCLPRIM("CHAR-CI>=?",char_ci_gte,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR-CI>=? *chars...*)`")
static lispval char_ci_gte(int n,lispval *args) {
  return compare_chars_ci(str_gte,n,args); }

DCLPRIM("CHAR>?",char_gt,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR>? *chars...*)`")
static lispval char_gt(int n,lispval *args) {
  return compare_chars(str_gt,n,args); }

DCLPRIM("CHAR-CI>?",char_ci_gt,MIN_ARGS(2)|FD_VAR_ARGS,
        "`(CHAR-CI>? *chars...*)`")
static lispval char_ci_gt(int n,lispval *args) {
  return compare_chars_ci(str_gt,n,args); }

/* String building */

DCLPRIM("STRING-APPEND",string_append_prim,FD_VAR_ARGS,
        "`(STRING-APPEND *strings...*)` creates a new string "
        "by appending together each of *strings*.")
static lispval string_append_prim(int n,lispval *args)
{
  int i = 0;
  U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
  while (i<n)
    if (STRINGP(args[i])) {
      u8_puts(&out,CSTRING(args[i])); i++;}
    else {
      u8_free(out.u8_outbuf);
      return fd_type_error("string","string_append",args[i]);}
  return fd_stream2string(&out);
}

DCLPRIM("STRING",string_prim,FD_VAR_ARGS,
        "`(STRING *strings|chars...*)` creates a new string "
        "by appending together strings or characters.")
static lispval string_prim(int n,lispval *args)
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

DCLPRIM2("MAKE-STRING",makestring_prim,MAX_ARGS(2)|MIN_ARGS(1),
         "`(MAKE-STRING *len* [*initchar*])` creates a new string "
         "of *len* characters, initialied to *initchar* which defaults "
         "to #\\Space",
         fd_fixnum_type,VOID,fd_character_type,FD_VOID)
static lispval makestring_prim(lispval len,lispval character)
{
  struct U8_OUTPUT out;
  if (fd_getint(len)==0)
    return fd_init_string(NULL,0,NULL);
  else {
    int i = 0, n = fd_getint(len);
    int ch = (VOIDP(character)) ? (' ') : (FD_CHAR2CODE(character));
    U8_INIT_OUTPUT(&out,n);
    while (i<n) {u8_putc(&out,ch); i++;}
    return fd_stream2string(&out);}
}

/* String predicates */

static lispval getnonstring(lispval choice)
{
  DO_CHOICES(x,choice) {
    if (!(STRINGP(x))) {
      FD_STOP_DO_CHOICES;
      return x;}
    else {}}
  return VOID;
}

static int has_suffix_test(lispval string,lispval suffix)
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
DCLPRIM2("HAS-SUFFIX",has_suffix,MAX_ARGS(2)|MIN_ARGS(2)|NDCALL,
         "`(HAS-SUFFIX *string* *suffixes*)` returns true if *string* "
         "starts with any of *suffixes* (also strings).",
         -1,FD_VOID,-1,FD_VOID)
static lispval has_suffix(lispval string,lispval suffix)
{
  lispval notstring;
  if (QCHOICEP(suffix)) suffix = (FD_XQCHOICE(suffix))->qchoiceval;
  if ((STRINGP(string))&&(STRINGP(suffix))) {
    if (has_suffix_test(string,suffix))
      return FD_TRUE;
    else return FD_FALSE;}
  if ((EMPTYP(string))||(EMPTYP(suffix)))
    return FD_FALSE;
  notstring = getnonstring(string);
  if (!(VOIDP(notstring)))
    return fd_type_error("string","has_suffix/input",notstring);
  else notstring = getnonstring(suffix);
  if (!(VOIDP(notstring)))
    return fd_type_error("string","has_suffix/suffix",notstring);
  if ((CHOICEP(string))&&(CHOICEP(suffix))) {
    int matched = 0;
    DO_CHOICES(s,string) {
      DO_CHOICES(sx,suffix) {
        matched = has_suffix_test(s,sx);
        if (matched) {FD_STOP_DO_CHOICES; break;}}
      if (matched) {FD_STOP_DO_CHOICES; break;}}
    if (matched)
      return FD_TRUE;
    else return FD_FALSE;}
  else if (CHOICEP(string)) {
    DO_CHOICES(s,string) {
      if (has_suffix_test(s,suffix)) {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else if (CHOICEP(suffix)) {
    DO_CHOICES(sx,suffix) {
      if (has_suffix_test(string,sx)) {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else return FD_FALSE;
}
DCLPRIM2("IS-SUFFIX",is_suffix,MAX_ARGS(2)|MIN_ARGS(2),
         "`(IS-SUFFIX *suffix* *string*)` returns true if *string* "
         "starts with *suffix*.",
         fd_string_type,FD_VOID,fd_string_type,FD_VOID)
static lispval is_suffix(lispval suffix,lispval string) {
  return has_suffix(string,suffix); }

DCLPRIM2("STRIP-SUFFIX",strip_suffix,MAX_ARGS(2)|MIN_ARGS(2)|NDCALL,
         "`(STRIP-SUFFIX *string* *suffixes*)` removes the longest of "
         "*suffixes* from the end of *string*, if any.",
         -1,FD_VOID,-1,FD_VOID)
static lispval strip_suffix(lispval string,lispval suffix)
{
  lispval notstring;
  if (QCHOICEP(suffix)) suffix = (FD_XQCHOICE(suffix))->qchoiceval;
  if ((STRINGP(string))&&(STRINGP(suffix))) {
    if (has_suffix_test(string,suffix)) {
      int sufflen = STRLEN(suffix), len = STRLEN(string);
      return fd_substring(CSTRING(string),
                          CSTRING(string)+(len-sufflen));}
    else return fd_incref(string);}
  if ((EMPTYP(string))||(EMPTYP(suffix)))
    return fd_incref(string);
  notstring = getnonstring(string);
  if (!(VOIDP(notstring)))
    return fd_type_error("string","has_suffix/input",notstring);
  else notstring = getnonstring(suffix);
  if (!(VOIDP(notstring)))
    return fd_type_error("string","has_suffix/suffix",notstring);
  else {
    lispval result = EMPTY;
    DO_CHOICES(s,string) {
      int max_sufflen = -1;
      DO_CHOICES(sx,suffix) {
        if (has_suffix_test(s,sx)) {
          int sufflen = STRLEN(sx);
          if (sufflen>max_sufflen) max_sufflen = sufflen;}}
      if (max_sufflen<0) {
        fd_incref(s);
        CHOICE_ADD(result,s);}
      else {
        int len = STRLEN(s);
        lispval stripped = fd_extract_string
          (NULL,CSTRING(s),CSTRING(s)+(len-max_sufflen));
        CHOICE_ADD(result,stripped);}}
    return result;}
}

static int has_prefix_test(lispval string,lispval prefix)
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

DCLPRIM2("HAS-PREFIX",has_prefix,MAX_ARGS(2)|MIN_ARGS(2)|NDCALL,
         "`(HAS-PREFIX *string* *prefixes*)` returns true if *string* "
         "starts with any of *prefixes* (also strings).",
         -1,FD_VOID,-1,FD_VOID)
static lispval has_prefix(lispval string,lispval prefix)
{
  lispval notstring;
  if (QCHOICEP(prefix)) prefix = (FD_XQCHOICE(prefix))->qchoiceval;
  if ((STRINGP(string))&&(STRINGP(prefix))) {
    if (has_prefix_test(string,prefix)) return FD_TRUE;
    else return FD_FALSE;}
  if ((EMPTYP(string))||(EMPTYP(prefix)))
    return FD_FALSE;
  notstring = getnonstring(string);
  if (!(VOIDP(notstring)))
    return fd_type_error("string","has_prefix/input",notstring);
  else notstring = getnonstring(prefix);
  if (!(VOIDP(notstring)))
    return fd_type_error("string","has_prefix/prefix",notstring);
  if ((CHOICEP(string))&&(CHOICEP(prefix))) {
    int matched = 0;
    DO_CHOICES(s,string) {
      DO_CHOICES(p,prefix) {
        matched = has_prefix_test(s,p);
        if (matched) {
          FD_STOP_DO_CHOICES; break;}}
      if (matched) {
        FD_STOP_DO_CHOICES; break;}}
    if (matched) return FD_TRUE; else return FD_FALSE;}
  else if (CHOICEP(string)) {
    DO_CHOICES(s,string) {
      if (has_prefix_test(s,prefix)) {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else if (CHOICEP(prefix)) {
    DO_CHOICES(p,prefix) {
      if (has_prefix_test(string,p)) {
        FD_STOP_DO_CHOICES;
        return FD_TRUE;}}
    return FD_FALSE;}
  else return FD_FALSE;
}
DCLPRIM2("IS-PREFIX",is_prefix,MAX_ARGS(2)|MIN_ARGS(2),
         "`(IS-PREFIX *prefix* *string*)` returns true if *string* "
         "starts with *prefix*.",
         fd_string_type,FD_VOID,fd_string_type,FD_VOID)
static lispval is_prefix(lispval prefix,lispval string) {
  return has_prefix(string,prefix); }

DCLPRIM2("STRIP-PREFIX",strip_prefix,MAX_ARGS(2)|MIN_ARGS(2)|NDCALL,
         "`(STRIP-PREFIX *string* *suffixes*)` removes the longest of "
         "*suffixes* from the beginning of *string*, if any.",
         -1,FD_VOID,-1,FD_VOID)
static lispval strip_prefix(lispval string,lispval prefix)
{
  lispval notstring;
  if (QCHOICEP(prefix)) prefix = (FD_XQCHOICE(prefix))->qchoiceval;
  if ((STRINGP(string))&&(STRINGP(prefix))) {
    if (has_prefix_test(string,prefix)) {}
    else return fd_incref(string);}
  if ((EMPTYP(string))||(EMPTYP(prefix)))
    return fd_incref(string);
  notstring = getnonstring(string);
  if (!(VOIDP(notstring)))
    return fd_type_error("string","has_prefix/input",notstring);
  else notstring = getnonstring(prefix);
  if (!(VOIDP(notstring)))
    return fd_type_error("string","has_prefix/prefix",notstring);
  else {
    lispval result = EMPTY;
   DO_CHOICES(s,string) {
      int max_prelen = -1;
      DO_CHOICES(px,prefix) {
        if (has_prefix_test(s,px)) {
          int prelen = STRLEN(px);
          if (prelen>max_prelen) max_prelen = prelen;}}
      if (max_prelen<0) {
        fd_incref(s);
        CHOICE_ADD(result,s);}
      else {
        int len = STRLEN(s);
        lispval stripped = fd_extract_string
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
    FD_DOLIST(s,strings) {
      if (strmatch(arg,s,ignorecase)) return 1;}
    return 0;}
  else if ((CHOICEP(strings))||(PRECHOICEP(strings))) {
    DO_CHOICES(s,strings) {
      if (strmatch(arg,s,ignorecase)) {
        FD_STOP_DO_CHOICES;
        return 1;}}
    return 0;}
  else {}
  return 0;
}

DCLPRIM4("YES?",yesp_prim,MAX_ARGS(4)|MIN_ARGS(1),
         "`(YES? *string* [*default*] [*yesses*] [*nos*])` "
         "returns true if *string* is recognized as "
         "an affirmative natural langauge response.",
         -1,VOID,-1,FD_FALSE,
         -1,EMPTY,-1,EMPTY)
static lispval yesp_prim(lispval arg,lispval dflt,lispval yes,lispval no)
{
  u8_string string_arg; int ignorecase = 0;
  if (STRINGP(arg)) string_arg = CSTRING(arg);
  else if (SYMBOLP(arg)) {
    string_arg = SYM_NAME(arg); ignorecase = 1;}
  else return fd_type_error("string or symbol","yesp_prim",arg);
  if ((!(VOIDP(yes)))&&(check_yesp(string_arg,yes,ignorecase)))
    return FD_TRUE;
  else if ((!(VOIDP(no)))&&(check_yesp(string_arg,no,ignorecase)))
    return FD_FALSE;
  else return fd_boolstring(CSTRING(arg),((FALSEP(dflt))?(0):(1)));
}

/* STRSEARCH */

DCLPRIM3("STRMATCH?",strmatchp_prim,MIN_ARGS(2)|MAX_ARGS(3)|NDCALL,
         "`(STRMATCH? *string* *patterns* *pos*)` return true "
         "if any of *patterns* (a choice of strings or regexes) "
         "matches the substring of *string* starting at *pos*.",
         fd_string_type,VOID,-1,VOID,fd_fixnum_type,VOID)
static lispval strmatchp_prim(lispval pat,lispval string,lispval ef)
{
  if (CHOICEP(string)) {
    DO_CHOICES(str,string) {
      lispval r = strmatchp_prim(pat,str,ef);
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
    if (TYPEP(pat,fd_regex_type)) {
      int off = fd_regex_op(rx_search,pat,
                            CSTRING(string),STRLEN(string),
                            FIX2INT(ef));
      if (off>=0) return FD_TRUE;
      else return FD_FALSE;}
    else if (STRINGP(pat)) {
      u8_string start = strstr(CSTRING(string),CSTRING(pat));
      if (start) return FD_TRUE;
      else return FD_FALSE;}
    else if (CHOICEP(pat)) {
      DO_CHOICES(p,pat) {
        if (TYPEP(pat,fd_regex_type)) {
          int off = fd_regex_op(rx_search,p,
                                CSTRING(string),STRLEN(string),
                                FIX2INT(ef));
          if (off>=0) {
            FD_STOP_DO_CHOICES;
            return FD_TRUE;}}
        else if (STRINGP(pat)) {
          u8_string start = strstr(CSTRING(string),CSTRING(pat));
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

static lispval entity_escape;

DCLPRIM3("STRING->PACKET",string2packet,MAX_ARGS(3)|MIN_ARGS(1),
         "`(STRING->PACKET *string* [*encoding*] [*escape*])` converts "
         "*string* into a packet based on *encoding*. Characters which "
         "cannot be converted are escaped based on the *escape* arg. "
         "If *escape* is '**entities**, HTML escapes are used, if "
         "*escape* is #f (the default), non-represented characters "
         "are ignored, and otherwise \\u escapes are emitted. *encoding* "
         "is either a string or symbol naming the encoding scheme.",
         fd_string_type,FD_VOID,-1,FD_VOID,-1,FD_VOID)
static lispval string2packet(lispval string,lispval encoding,lispval escape)
{
  char *data; int n_bytes;
  const u8_byte *scan, *limit;
  struct U8_TEXT_ENCODING *enc;
  if ( (VOIDP(encoding)) || (FALSEP(encoding)) ) {
    int n_bytes = STRLEN(string);
    return fd_make_packet(NULL,n_bytes,CSTRING(string));}
  else if (STRINGP(encoding))
    enc = u8_get_encoding(CSTRING(encoding));
  else if (SYMBOLP(encoding))
    enc = u8_get_encoding(SYM_NAME(encoding));
  else return fd_type_error(_("text encoding"),"string2packet",encoding);
  scan = CSTRING(string); limit = scan+STRLEN(string);
  if (FD_EQ(escape,entity_escape))
    data = u8_localize(enc,&scan,limit,'&',0,NULL,&n_bytes);
  else if ((FALSEP(escape)) || (VOIDP(escape)))
    data = u8_localize(enc,&scan,limit,0,0,NULL,&n_bytes);
  else data = u8_localize(enc,&scan,limit,'\\',0,NULL,&n_bytes);
  if (data) {
    lispval result = fd_make_packet(NULL,n_bytes,data);
    u8_free(data);
    return result;}
  else return FD_ERROR;
}

DCLPRIM("->SECRET",x2secret_prim,MAX_ARGS(1),
        "`(->SECRET *object*)` returns a *secret packet* from "
        "*object* whose printed representation does not disclose it's "
        "value but which can be used as arguments to many string "
        "combination functions. If *object* is a packet, it's bytes "
        "are used directly, if *object* is a string, it's UTF-8 "
        "representation is used. Otherwise, the object is unparsed "
        "into a string.")
static lispval x2secret_prim(lispval arg)
{
  if (TYPEP(arg,fd_secret_type))
    return fd_incref(arg);
  else if (TYPEP(arg,fd_packet_type)) {
    lispval result = fd_make_packet
      (NULL,FD_PACKET_LENGTH(arg),FD_PACKET_DATA(arg));
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
  else if (STRINGP(arg)) {
    lispval result = fd_make_packet(NULL,STRLEN(arg),CSTRING(arg));
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
  else return fd_type_error("string/packet","x2secret_prim",arg);
}

DCLPRIM2("PACKET->STRING",packet2string,MAX_ARGS(2)|MIN_ARGS(1),
         "`(PACKET->STRING *packet* [*encoding*])` converts the "
         "bytes of *packet* into a string based on the named "
         "*encoding* which can be a string or symbol and defaults "
         "to UTF-8",
         fd_packet_type,FD_VOID,-1,FD_FALSE)
static lispval packet2string(lispval packet,lispval encoding)
{
  if (FD_PACKET_LENGTH(packet)==0)
    return lispval_string("");
  else {
    struct U8_OUTPUT out;
    const u8_byte *scan = FD_PACKET_DATA(packet);
    const u8_byte *limit = scan+FD_PACKET_LENGTH(packet);
    U8_INIT_OUTPUT(&out,2*FD_PACKET_LENGTH(packet));
    struct U8_TEXT_ENCODING *enc;
    if ( (VOIDP(encoding)) || (FALSEP(encoding)) )
      enc = u8_get_encoding("UTF-8");
    else if (STRINGP(encoding))
      enc = u8_get_encoding(CSTRING(encoding));
    else if (SYMBOLP(encoding))
      enc = u8_get_encoding(SYM_NAME(encoding));
    else return fd_type_error(_("text encoding"),"packet2string",encoding);
    if (u8_convert(enc,0,&out,&scan,limit)<0) {
      u8_free(out.u8_outbuf);
      return FD_ERROR;}
    else return fd_stream2string(&out);}
}

/* Simple string subst */

static u8_string strsearch(u8_string string,lispval pat,
                           size_t len,size_t *matchlenp)
{
  if (STRINGP(pat)) {
    *matchlenp = len;
    return strstr(string,CSTRING(pat));}
  else if (TYPEP(pat,fd_regex_type)) {
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

DCLPRIM3("STRING-SUBST",string_subst_prim,MAX_ARGS(3),
         "`(STRING-SUBST *string* *pat* *replace*)` "
         "replaces substrings of *string* which match *pattern* "
         "with *replacement*. *pattern* can be a string or "
         "a regex and arguments substitutions are not available.",
         fd_string_type,FD_VOID,-1,FD_VOID,fd_string_type,FD_VOID)
static lispval string_subst_prim(lispval string,lispval pat,lispval with)
{
  if (STRLEN(string)==0) return fd_incref(string);
  else if (!((STRINGP(pat))||(TYPEP(pat,fd_regex_type))))
    return fd_type_error("string or regex","string_subst_prim",pat);
  else if ((STRINGP(pat))&&
           (strstr(CSTRING(string),CSTRING(pat)) == NULL))
    return fd_incref(string);
  else {
    u8_string original = CSTRING(string);
    u8_string replace = CSTRING(with);
    size_t patlen = (STRINGP(pat))?(FD_STRING_LENGTH(pat)):(-1);
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
      return fd_stream2string(&out);}
    else return fd_incref(string);}
}

DCLPRIM("STRING-SUBST*",string_subst_star,MIN_ARGS(3)|FD_VAR_ARGS,
        "`(STRING-SUBST* *string* [*pattern* *subst*]...)` replaces "
        "substrings matching each *pattern* with the corresponding *subst* "
        "starting from left to right.")
static lispval string_subst_star(int n,lispval *args)
{
  lispval base = args[0]; int i = 1;
  if ((n%2)==0)
    return fd_err(fd_SyntaxError,"string_subst_star",NULL,VOID);
  else while (i<n) {
      if (PRED_FALSE
          (!((STRINGP(args[i]))||
             (TYPEP(args[i],fd_regex_type)))))
        return fd_type_error(_("string"),"string_subst_star",args[i]);
      else i++;}
  /* In case we return it. */
  fd_incref(base); i = 1;
  while (i<n) {
    lispval replace = args[i];
    lispval with = args[i+1];
    lispval result = string_subst_prim(base,replace,with);
    i = i+2; fd_decref(base); base = result;}
  return base;
}

DCLPRIM1("TRIM-SPACES",trim_spaces,MAX_ARGS(1),
         "`(TRIM-SPACES *string*)` removes leading and trailing "
         "whitespace from *string*.",
         fd_string_type,FD_VOID)
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
    return fd_incref(string);
  else return fd_substring(trim_start,trim_end);
}

/* Glomming */

DCLPRIM("GLOM",glom_lexpr,MAX_ARGS(0)|FD_VAR_ARGS,
        "`(GLOM *objects...*)` returns a string concatenating "
        "either the text of *objects* (if they are strings) or the "
        "text of their printed representations (if they are not). "
        "If any of the arguments are choices, `GLOM` returns a choice "
        "of object combinations.")
static lispval glom_lexpr(int n,lispval *args)
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
    if (STRINGP(args[i])) {
      sumlen = sumlen+STRLEN(args[i]);
      strings[i]=CSTRING(args[i]);
      lengths[i]=STRLEN(args[i]);
      if (result_type==0) result_type = fd_string_type;
      consed[i++]=0;}
    else if (PACKETP(args[i])) {
      sumlen = sumlen+FD_PACKET_LENGTH(args[i]);
      strings[i]=FD_PACKET_DATA(args[i]);
      lengths[i]=FD_PACKET_LENGTH(args[i]);
      if (TYPEP(args[i],fd_secret_type))
        result_type = fd_secret_type;
      else if (result_type!=fd_secret_type)
        result_type = fd_packet_type;
      consed[i++]=0;}
    else if ((FALSEP(args[i]))||(EMPTYP(args[i]))||
             (VOIDP(args[i]))) {
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
    lispval result = fd_init_packet(NULL,sumlen,result_data);
    FD_SET_CONS_TYPE(result,fd_secret_type);
    return result;}
}

/* Text if */

static lispval textif_evalfn(lispval expr,fd_lexenv env,fd_stack _stack)
{
  lispval test_expr = fd_get_arg(expr,1), test_val = VOID;
  if (VOIDP(test_expr))
    return fd_err(fd_SyntaxError,"textif_evalfn",NULL,VOID);
  else test_val = fd_eval(test_expr,env);
  if (FD_ABORTED(test_val)) return test_val;
  else if ((FALSEP(test_val))||(EMPTYP(test_val)))
    return fd_make_string(NULL,0,"");
  else {
    lispval body = fd_get_body(expr,2), len = fd_seq_length(body);
    fd_decref(test_val);
    if (len==0) return fd_make_string(NULL,0,NULL);
    else if (len==1) {
      lispval text = fd_eval(fd_get_arg(body,0),env);
      if (STRINGP(text)) return fd_incref(text);
      else if ((FALSEP(text))||(EMPTYP(text)))
        return fd_make_string(NULL,0,"");
      else {
        u8_string as_string = fd_lisp2string(text);
        return fd_init_string(NULL,-1,as_string);}}
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,128);
      if (PAIRP(body)) {
        FD_DOLIST(text_expr,body) {
          lispval text = fd_eval(text_expr,env);
          if (FD_ABORTED(text))
            return fd_err("Bad text clause","textif_evalfn",
                          out.u8_outbuf,text_expr);
          else if (STRINGP(text))
            u8_putn(&out,CSTRING(text),STRLEN(text));
          else fd_unparse(&out,text);
          fd_decref(text); text = VOID;}}
      else {
        int i = 0; while (i<len) {
          lispval text_expr = fd_get_arg(body,i++);
          lispval text = fd_eval(text_expr,env);
          if (FD_ABORTED(text))
            return fd_err("Bad text clause","textif_evalfn",
                          out.u8_outbuf,text_expr);
          else if (STRINGP(text))
            u8_putn(&out,CSTRING(text),STRLEN(text));
          else fd_unparse(&out,text);
          fd_decref(text); text = VOID;}}
      return fd_block_string(out.u8_write-out.u8_outbuf,out.u8_outbuf);
    }
  }
}

/* Initialization */

FD_EXPORT void fd_init_stringprims_c()
{
  u8_register_source_file(_FILEINFO);

  DECL_PRIM(asciip,1,fd_scheme_module);
  DECL_PRIM(latin1p,1,fd_scheme_module);
  DECL_PRIM(lowercasep,1,fd_scheme_module);
  DECL_PRIM(uppercasep,1,fd_scheme_module);

  DECL_PRIM(utf8p_prim,1,fd_scheme_module);
  DECL_PRIM(utf8string_prim,1,fd_scheme_module);
  DECL_PRIM(string_byte_length,1,fd_scheme_module);
  DECL_PRIM(fixnuls_prim,1,fd_scheme_module);

  DECL_PRIM(char2integer,1,fd_scheme_module);
  DECL_PRIM(integer2char,1,fd_scheme_module);
  DECL_PRIM(char_alphabeticp,1,fd_scheme_module);
  DECL_PRIM(char_numericp,1,fd_scheme_module);
  DECL_PRIM(char_alphanumericp,1,fd_scheme_module);
  DECL_PRIM(char_punctuationp,1,fd_scheme_module);
  DECL_PRIM(char_whitespacep,1,fd_scheme_module);
  DECL_PRIM(char_upper_casep,1,fd_scheme_module);
  DECL_PRIM(char_lower_casep,1,fd_scheme_module);

  DECL_PRIM(char_downcase,1,fd_scheme_module);
  DECL_PRIM(downcase,1,fd_scheme_module);
  DECL_PRIM(char_upcase,1,fd_scheme_module);
  DECL_PRIM(upcase,1,fd_scheme_module);

  DECL_PRIM_N(string_eq,fd_scheme_module);
  DECL_PRIM_N(string_ci_eq,fd_scheme_module);
  DECL_PRIM_N(string_ne,fd_scheme_module);
  DECL_PRIM_N(string_ci_ne,fd_scheme_module);
  DECL_PRIM_N(string_lt,fd_scheme_module);
  DECL_PRIM_N(string_ci_lt,fd_scheme_module);
  DECL_PRIM_N(string_gt,fd_scheme_module);
  DECL_PRIM_N(string_ci_gt,fd_scheme_module);
  DECL_PRIM_N(string_gte,fd_scheme_module);
  DECL_PRIM_N(string_ci_gte,fd_scheme_module);
  DECL_PRIM_N(string_lte,fd_scheme_module);
  DECL_PRIM_N(string_ci_lte,fd_scheme_module);

  DECL_PRIM_N(char_eq,fd_scheme_module);
  DECL_PRIM_N(char_ci_eq,fd_scheme_module);
  DECL_PRIM_N(char_ne,fd_scheme_module);
  DECL_PRIM_N(char_ci_ne,fd_scheme_module);
  DECL_PRIM_N(char_lt,fd_scheme_module);
  DECL_PRIM_N(char_ci_lt,fd_scheme_module);
  DECL_PRIM_N(char_gt,fd_scheme_module);
  DECL_PRIM_N(char_ci_gt,fd_scheme_module);
  DECL_PRIM_N(char_gte,fd_scheme_module);
  DECL_PRIM_N(char_ci_gte,fd_scheme_module);
  DECL_PRIM_N(char_lte,fd_scheme_module);
  DECL_PRIM_N(char_ci_lte,fd_scheme_module);

  DECL_PRIM(capitalizedp,1,fd_scheme_module);
  DECL_PRIM(capitalize,1,fd_scheme_module);
  DECL_PRIM(capitalize1,1,fd_scheme_module);
  DECL_PRIM(symbolize_cprim,1,fd_scheme_module);

  DECL_PRIM(some_capitalizedp,2,fd_scheme_module);

  DECL_PRIM(empty_stringp,3,fd_scheme_module);
  DECL_PRIM(string_compoundp,1,fd_scheme_module);
  DECL_PRIM(string_multilinep,1,fd_scheme_module);
  DECL_PRIM(string_phrase_length,1,fd_scheme_module);

  DECL_PRIM(string_stdspace,2,fd_scheme_module);
  DECL_PRIM(string_stdcap,1,fd_scheme_module);
  DECL_PRIM(string_stdstring,1,fd_scheme_module);
  DECL_PRIM(string_basestring,1,fd_scheme_module);
  DECL_PRIM(string_startword,1,fd_scheme_module);
  DECL_PRIM(string_trigrams,1,fd_scheme_module);
  DECL_PRIM(string_bigrams,1,fd_scheme_module);

  DECL_PRIM(yesp_prim,4,fd_scheme_module);

  DECL_PRIM(stdobj,1,fd_scheme_module);

  DECL_PRIM(has_prefix,2,fd_scheme_module);
  DECL_PRIM(is_prefix,2,fd_scheme_module);
  DECL_PRIM(strip_prefix,2,fd_scheme_module);

  DECL_PRIM(has_suffix,2,fd_scheme_module);
  DECL_PRIM(is_suffix,2,fd_scheme_module);
  DECL_PRIM(strip_suffix,2,fd_scheme_module);

  DECL_PRIM_N(string_append_prim,fd_scheme_module);
  DECL_PRIM_N(string_prim,fd_scheme_module);
  DECL_PRIM(makestring_prim,2,fd_scheme_module);

  DECL_PRIM(string_subst_prim,3,fd_scheme_module);
  DECL_PRIM(trim_spaces,1,fd_scheme_module);
  DECL_PRIM(strmatchp_prim,3,fd_scheme_module);
  DECL_PRIM_N(string_subst_star,fd_scheme_module);

  DECL_PRIM_N(glom_lexpr,fd_scheme_module);

  DECL_PRIM(string2packet,3,fd_scheme_module);
  DECL_PRIM(packet2string,2,fd_scheme_module);
  DECL_PRIM(x2secret_prim,1,fd_scheme_module);
  DECL_PRIM(x2secret_prim,1,fd_scheme_module);

  fd_def_evalfn(fd_scheme_module,"TEXTIF","",textif_evalfn);

  entity_escape = fd_intern("ENTITIES");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
