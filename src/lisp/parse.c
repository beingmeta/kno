/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.

   This file implements the core parser and printer (unparser) functionality.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/ports.h"

#include <libu8/u8printf.h>
#include <libu8/u8streamio.h>
#include <libu8/u8convert.h>
#include <libu8/u8crypto.h>

#include <ctype.h>
#include <errno.h>
/* We include this for sscanf, but we're not using the FILE functions */
#include <stdio.h>

#include <stdarg.h>

/* TODO: Add general parse_error function which provides input context
   of some sort, where possible. */

/* Common macros, functions and declarations */

#define PARSE_ERRORP(x) ((x == FD_EOX) || (x == FD_PARSE_ERROR) || (x == FD_OOM))
#define PARSE_ABORTP(x)                                                 \
  (FD_EXPECT_FALSE(((FD_TYPEP(x,fd_constant_type)) &&                   \
                    (FD_GET_IMMEDIATE(x,fd_constant_type)>6) &&         \
                    (FD_GET_IMMEDIATE(x,fd_constant_type)<16))))


#define odigitp(c) ((c>='0')&&(c<='8'))
#define spacecharp(c) ((c>0) && (c<128) && (isspace(c)))
#define atombreakp(c) \
  ((c<=0) || ((c<128) && ((isspace(c)) || (strchr("{}()[]#\"',`",c)))))

u8_condition fd_BadEscapeSequence=_("Invalid escape sequence");
u8_condition fd_InvalidConstant=_("Invalid constant reference");
u8_condition fd_InvalidCharacterConstant=_("Invalid character constant");
u8_condition fd_BadAtom=_("Bad atomic expression");
u8_condition fd_NoPointerExpressions=_("no pointer expressions allowed");
u8_condition fd_BadPointerRef=_("bad pointer reference");
u8_condition fd_UnexpectedEOF=_("Unexpected EOF in LISP expression");
u8_condition fd_ParseError=_("LISP expression parse error");
u8_condition fd_InvalidHexCharacter=_("Invalid hex character");
u8_condition fd_InvalidBase64Character=_("Invalid base64 character");
u8_condition fd_MissingCloseQuote=_("Unclosed double quotation mark (\")");
u8_condition fd_MissingOpenQuote=_("Missing open quotation mark (\")");
u8_condition fd_ParseArgError=_("External LISP argument parse error");
u8_condition fd_CantParseRecord=_("Can't parse record object");
u8_condition fd_MismatchedClose=_("Expression open/close mismatch");
u8_condition fd_UnterminatedBlockComment=_("Unterminated block (#|..|#) comment");

int fd_interpret_pointers = 1;

static lispval quote_symbol, histref_symbol, comment_symbol;
static lispval quasiquote_symbol, unquote_symbol, unquotestar_symbol;
static lispval opaque_tag;

static int skip_whitespace(u8_input s)
{
  int c = u8_getc(s);
  if (c<-1) return c;
  while (1) {
    while ((c>0) && (spacecharp(c))) c = u8_getc(s);
    if (c==';') {
      while ((c>=0) && (c != '\n')) c = u8_getc(s);
      if (c<0) return -1;}
    else if ((c=='#')&&(u8_peekc(s)<0)) {
      u8_ungetc(s,c);
      return c;}
    else if ((c=='#') && (u8_probec(s)=='|')) {
      int bar = 0; c = u8_getc(s);
      /* Read block comment */
      while ((c = u8_getc(s))>=0)
        if (c=='|') bar = 1;
        else if ((bar) && (c=='#')) break;
        else bar = 0;
      if (c=='#') c = u8_getc(s);}
    else break;}
  if (c<0) return c;
  u8_ungetc(s,c);
  return c;
}

FD_EXPORT int fd_skip_whitespace(u8_input s)
{
  return skip_whitespace(s);
}

/* Tables */

static u8_string character_constant_names[]={
  "SPACE","NEWLINE","RETURN",
  "TAB","VTAB","VERTICALTAB",
  "PAGE","BACKSPACE",
  "ATTENTION","BELL","DING",
  "SLASH","BACKSLASH","SEMICOLON",
  "OPENPAREN","CLOSEPAREN",
  "OPENBRACE","CLOSEBRACE",
  "OPENBRACKET","CLOSEBRACKET",
  NULL};
static lispval character_constants[]={
  FD_CODE2CHAR(' '),FD_CODE2CHAR('\n'),FD_CODE2CHAR('\r'),
  FD_CODE2CHAR('\t'),FD_CODE2CHAR('\v'),FD_CODE2CHAR('\v'),
  FD_CODE2CHAR('\f'),FD_CODE2CHAR('\b'),
  FD_CODE2CHAR('\a'),FD_CODE2CHAR('\a'),FD_CODE2CHAR('\a'),
  FD_CODE2CHAR('/'),FD_CODE2CHAR('\\'),FD_CODE2CHAR(';'),
  FD_CODE2CHAR('('),FD_CODE2CHAR(')'),
  FD_CODE2CHAR('{'),FD_CODE2CHAR('}'),
  FD_CODE2CHAR('['),FD_CODE2CHAR(']'),
  0};

/* Parsing */

static int parse_unicode_escape(u8_string arg)
{
  u8_string s;
  if (arg[0] == '\\') s = arg+1; else s = arg;
  if (s[0] == 'u') {
    if ((strlen(s)==5) &&
        (isxdigit(s[1])) && (isxdigit(s[2])) &&
        (isxdigit(s[3])) && (isxdigit(s[4]))) {
      int code = -1;
      if (sscanf(s+1,"%4x",&code)<1) code = -1;
      return code;}
    else if (s[1]=='{') {
      int code = -1;
      if (sscanf(s+1,"{%x}",&code)<1) code = -1;
      return code;}
    else {
      fd_seterr3(fd_BadEscapeSequence,"parse_unicode_escape",s);
      return -1;}}
  else if (s[0] == 'U')
    if ((strlen(s)==9) &&
        (isxdigit(s[1])) && (isxdigit(s[2])) &&
        (isxdigit(s[3])) && (isxdigit(s[4])) &&
        (isxdigit(s[5])) && (isxdigit(s[6])) &&
        (isxdigit(s[7])) && (isxdigit(s[8]))) {
      int code = -1;
      if (sscanf(s+1,"%8x",&code)<1) code = -1;
      return code;}
    else if (s[1]=='{') {
      int code = -1;
      if (sscanf(s+1,"{%x}",&code)<1) code = -1;
      return code;}
    else {
      fd_seterr3(fd_BadEscapeSequence,"parse_unicode_escape",s);
      return -1;}
  else {
    fd_seterr3(fd_BadEscapeSequence,"parse_unicode_escape",s);
    return -1;}
}

/* This reads an escape sequence inside of a string.
   It interprets the standard C escape sequences and also
   handles unicode escapes. */
static int read_escape(u8_input in)
{
  int c;
  switch (c = u8_getc(in)) {
  case 't': return '\t';
  case 'n': return '\n';
  case 'r': return '\r';
  case 'f': return '\f';
  case 'b': return '\b';
  case 'v': return '\v';
  case 'a': return '\a';
  case '?': return '\?';
  case '|': return '|';
  case '"': return '"';
  case '\\': return '\\';
  case '\'': return '\'';
  case 'x': {
    u8_byte buf[16]; int len;
    u8_string parsed = u8_gets_x(buf,16,in,";",&len);
    if (parsed) {
      int code = -1;
      if (sscanf(buf,"%x",&code)<1) code = -1;
      return code;}
    else {
      fd_seterr3(fd_BadEscapeSequence,"parse_unicode_escape",NULL);
      return -1;}}
  case 'u': {
    char buf[16]; int nc = u8_probec(in), len;
    if (nc=='{') {
      buf[0]='\\'; buf[1]='u';
      u8_gets_x(buf+2,13,in,"}",&len);}
    else {buf[0]='\\'; buf[1]='u'; u8_getn(buf+2,4,in);}
    return parse_unicode_escape(buf);}
  case 'U': {
    char buf[16];
    buf[0]='\\'; buf[1]='U'; u8_getn(buf+2,8,in);
    return parse_unicode_escape(buf);}
  case '0': case '1': case '2': case '3': {
    char buf[16]; int code = -1;
    buf[0]='\\'; buf[1]=c; u8_getn(buf+2,2,in); buf[5]='\0';
    if (strlen(buf)==4) {
      if (sscanf(buf+1,"%o",&code)<1) code = -1;
      if (code<0)
        fd_seterr3(fd_BadEscapeSequence,"read_escape",buf);
      return code;}
    else return -1;}
  case '&': {
    int code = u8_get_entity(in);
    if (code<0)
      fd_seterr3(fd_BadEscapeSequence,"read_escape",NULL);
    return code;}
  default:
    return c;
  }
}

FD_EXPORT int fd_read_escape(u8_input in)
{
  return read_escape(in);
}

/* Atom parsing */

static struct FD_KEYVAL *hashnames=NULL;
static int n_hashnames=0, hashnames_len=0;
static u8_rwlock hashnames_lock;

#ifndef MAX_HASHNAMES
#define MAX_HASHNAMES 7654321
#endif

static lispval lookup_hashname(u8_string s,int len,int lock)
{
  if (len<0) len=strlen(s);
  struct FD_STRING _string; unsigned char buf[len+1];
  strcpy(buf,s); buf[len]='\0';
  lispval string=fd_init_string(&_string,len,buf);
  FD_MAKE_STATIC(string);
  if (lock) u8_read_lock(&hashnames_lock);
  struct FD_KEYVAL *kv=fd_sortvec_get(string,hashnames,n_hashnames);
  if (lock) u8_rw_unlock(&hashnames_lock);
  if (kv)
    return kv->kv_val;
  else return FD_NULL;
}

FD_EXPORT
int fd_add_hashname(u8_string s,lispval value)
{
  u8_write_lock(&hashnames_lock);
  lispval cur=lookup_hashname(s,-1,0);
  if (cur!=FD_NULL) {
    u8_rw_unlock(&hashnames_lock);
    if (value==cur) return 0;
    else return -1;}
  else {
    u8_string d=u8_upcase(s);
    lispval string=lispval_string(d);
    struct FD_KEYVAL *added=
      fd_sortvec_insert(string,&hashnames,
                        &n_hashnames,&hashnames_len,MAX_HASHNAMES,
                        1);
    if (added)
      added->kv_val=value;
    if ( (added) && (added->kv_key != string ) ) {
      if (!(FD_EQUALP(value,added->kv_val)))
        u8_log(LOG_WARN,"ConstantConflict",
               "Conflicting values for constant #%s: #!0x%llx and #!0x%llx",
               d,added->kv_val,value);
      fd_decref(string);}
    u8_rw_unlock(&hashnames_lock);
    u8_free(d);
    return 1;}
}

FD_EXPORT
lispval fd_lookup_hashname(u8_string s)
{
  return lookup_hashname(s,-1,1);
}

static int copy_atom(u8_input s,u8_output a)
{
  int c = u8_getc(s), vbar = 0;
  if (c=='|') {vbar = 1; c = u8_getc(s);}
  while ((c>=0) && ((vbar) || (!(atombreakp(c))))) {
    if (c == '|') if (vbar) vbar = 0; else vbar = 1;
    else if (c == '\\') {
      int realc;
      realc = read_escape(s);
      u8_putc(a,realc);}
    else if (vbar) u8_putc(a,c);
    else {
      int upper = u8_toupper(c);
      u8_putc(a,upper);}
    c = u8_getc(s);}
  if (c>=0) u8_ungetc(s,c);
  return c;
}

lispval fd_parse_atom(u8_string start,int len)
{
  /* fprintf(stderr,"fd_parse_atom %d: %s\n",len,start); */
  if (PRED_FALSE(len==0)) return FD_EOX;
  else if ((start[0]=='#')&&(start[1]=='U')) { /* It's a UUID */
    struct FD_UUID *uuid = u8_alloc(struct FD_UUID);
    FD_INIT_CONS(uuid,fd_uuid_type);
    if (u8_parseuuid(start+2,(u8_uuid)&(uuid->uuid16)))
      return LISP_CONS(uuid);
    else {
      fd_seterr3("Invalid UUID","fd_parse_atom",start);
      return FD_PARSE_ERROR;}}
  else if ((start[0]=='#')&&(start[1]=='T')&&(isdigit(start[2]))) {
    struct U8_XTIME xt;
    int retval = u8_iso8601_to_xtime(start+2,&xt);
    if (retval<0) {
      fd_seterr("Invalid timestamp","fd_parse_atom",start,VOID);
      return FD_PARSE_ERROR;}
    else return fd_make_timestamp(&xt);}
  else if ((start[0]=='#')&&(start[1]=='!')&&(isxdigit(start[2]))) {
    if (fd_interpret_pointers) {
      unsigned long long pval;
      if (sscanf(start+2,"%llx",&pval)!=1)
        return fd_err
          (fd_BadPointerRef,"fd_parse_atom",u8_strdup(start),VOID);
      else if (FD_CHECK_PTR(pval))
        return fd_incref((lispval)pval);
      else return fd_err
             (fd_BadPointerRef,"fd_parse_atom",u8_strdup(start),VOID);}
    else return fd_err
           (fd_NoPointerExpressions,"fd_parse_atom",
            u8_strdup(start),VOID);}
  else if (start[0]=='#') { /* Look it up */
    lispval value = lookup_hashname(start,-1,1);
    if (value != FD_NULL) return value;
    /* This is where we would handle history refs */
    /* Number syntaxes */
    if (strchr("XxOoBbEeIiDd",start[1])) {
      lispval result=_fd_parse_number(start,-1);
      if (!(FALSEP(result))) return result;}
    fd_seterr3(fd_InvalidConstant,"fd_parse_atom",start);
    return FD_PARSE_ERROR;}
  else {
    lispval result;
    /* More numbers */
    if ((isdigit(start[0])) || (start[0]=='+') ||
        (start[0]=='-') || (start[0]=='.')) {
      result=_fd_parse_number(start,-1);
      if (!(FALSEP(result))) return result;}
    /* Otherwise, it's a symbol */
    return fd_make_symbol(start,len);}
}

/* Parsing characters */

static lispval parse_character(U8_INPUT *in)
{
  char buf[128];
  struct U8_OUTPUT tmpbuf;
  int c, n_chars = 0;
  /* First, copy an entire atom. */
  U8_INIT_STATIC_OUTPUT_BUF(tmpbuf,128,buf);
  c = u8_getc(in);
  if ((c=='&')&&(!(atombreakp(*(in->u8_read))))) {
    int code = u8_get_entity(in);
    if (code<0)
      if (atombreakp(*(in->u8_read)))
        return FD_CODE2CHAR('&');
      else {
        fd_seterr3(fd_BadEscapeSequence,"read_escape",NULL);
        return FD_PARSE_ERROR;}
    else return FD_CODE2CHAR(code);}
  if ((c<128) && (ispunct(c))) {
    return FD_CODE2CHAR(c);}
  while ((c>=0) && (!(atombreakp(c)))) {
    n_chars++; u8_putc(&tmpbuf,c); c = u8_getc(in);}
  if (n_chars==0) return FD_CODE2CHAR(c);
  else u8_ungetc(in,c);
  if (n_chars==1) {
    const u8_byte *scan = buf; int c = u8_sgetc(&scan);
    return FD_CODE2CHAR(c);}
  else if ((tmpbuf.u8_outbuf[0]=='u') || (tmpbuf.u8_outbuf[0]=='U'))
    c = parse_unicode_escape(tmpbuf.u8_outbuf);
  else c = -1;
  if (c>=0) return FD_CODE2CHAR(c);
  else if ((c = u8_entity2code(tmpbuf.u8_outbuf))>=0)
    return FD_CODE2CHAR(c);
  else {
    int i = 0; while (character_constant_names[i])
      if (strcasecmp(buf,character_constant_names[i]) == 0)
        return character_constants[i];
      else i++;
    fd_seterr3(fd_InvalidCharacterConstant,"parse_character",tmpbuf.u8_outbuf);
    return FD_PARSE_ERROR;}
}

/* OID parsing */

static lispval (*oid_parser)(u8_string start,int len) = NULL;

FD_EXPORT
/* fd_set_oid_parser:
     Arguments: a function which takes a UTF8 string and an integer length
     Returns: void
  This sets the default parser used for OIDs.
*/
void fd_set_oid_parser(lispval (*parsefn)(u8_string start,int len))
  {
  oid_parser = parsefn;
}

typedef unsigned long long ull;

static lispval default_parse_oid(u8_string start,int len)
{
  u8_byte buf[64];
  FD_OID oid = FD_NULL_OID_INIT;
  unsigned int hi, lo;
  if (len>64) {
    fd_seterr("BadOIDReference","default_parse_oid",start,VOID);
    return FD_PARSE_ERROR;}
  strncpy(buf,start,len); buf[len]='\0';
  if (strchr(buf,'/')) {
    int items = sscanf(buf,"@%x/%x",&hi,&lo);
    if (items!=2) {
      fd_seterr("BadOIDReference","default_parse_oid",start,VOID);
      return FD_PARSE_ERROR;}}
  else {
    unsigned long long addr;
    int items = sscanf(buf,"@%llx",&addr);
    if (items!=1) {
      fd_seterr("BadOIDReference","default_parse_oid",start,VOID);
      return FD_PARSE_ERROR;}
    hi = ((addr>>32)&((ull)0xFFFFFFFF));
    lo = (addr&((ull)0xFFFFFFFF));}
  FD_SET_OID_HI(oid,hi); FD_SET_OID_LO(oid,lo);
  return fd_make_oid(oid);
}

FD_EXPORT lispval fd_parse_oid_addr(u8_string string,int len)
{
  return default_parse_oid(string,len);
}

static int copy_string(u8_input s,u8_output a);

/* This is the function called from the main parser loop. */
static lispval parse_oid(U8_INPUT *in)
{
  struct U8_OUTPUT tmpbuf; char buf[128]; int c; lispval result;
  U8_INIT_STATIC_OUTPUT_BUF(tmpbuf,128,buf);
  /* First, copy the data into a buffer.
     The buffer will almost never grow, but it might
     if we have a really long prefix id. */
  c = copy_atom(in,&tmpbuf);
  if ((c=='"')&&((tmpbuf.u8_write-tmpbuf.u8_outbuf)==2)&&
      (buf[0]=='@')&&(ispunct(buf[1]))&&
      (strchr("(){}[]<>",buf[1]) == NULL)) {
    copy_string(in,&tmpbuf); c='@';}
  if (tmpbuf.u8_write<=tmpbuf.u8_outbuf)
    return FD_EOX;
  else if (oid_parser)
    result = oid_parser(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
  else result = default_parse_oid(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
  if (FD_ABORTP(result))
      return result;
  if (strchr("({\"",c)) {
    /* If an object starts immediately after the OID (no whitespace)
       it is the OID's label, so we read it and discard it. */
    lispval label = fd_parser(in);
    fd_decref(label);}
  if (tmpbuf.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpbuf.u8_outbuf);
  return result;
}

/* String and packet parsing */

static lispval parse_string(U8_INPUT *in)
{
  lispval result = VOID; u8_byte buf[256];
  struct U8_OUTPUT out; int c = u8_getc(in);
  U8_INIT_OUTPUT_X(&out,256,buf,0);
  while ((c = u8_getc(in))>=0)
    if (c == '"') break;
    else if (c == '\\') {
      int nextc = u8_getc(in);
      if (nextc=='\n') {
        while (u8_isspace(nextc)) nextc = u8_getc(in);
        u8_putc(&out,nextc);
        continue;}
      else u8_ungetc(in,nextc);
      c = read_escape(in);
      if (c<0) {
        if ((out.u8_streaminfo)&(U8_STREAM_OWNS_BUF))
          u8_free(out.u8_outbuf);
        return FD_PARSE_ERROR;}
      u8_putc(&out,c);}
    else u8_putc(&out,c);
  result = fd_make_string(NULL,u8_outlen(&out),u8_outstring(&out));
  u8_close_output(&out);
  return result;
}

static lispval make_regex(u8_string src_arg,u8_string opts);

static lispval parse_regex(U8_INPUT *in)
{
  lispval result; struct U8_OUTPUT src; u8_byte buf[128];
  u8_byte opts[16]="", *optwrite = opts;
  int c = u8_getc(in); U8_INIT_OUTPUT_BUF(&src,128,buf);
  while (c>=0) {
    if (c=='\\') {
      c = u8_getc(in);
      switch (c) {
      case 'n': u8_putc(&src,'\n'); break;
      case 't': u8_putc(&src,'\t'); break;
      case 'r': u8_putc(&src,'\r'); break;
      case 'a': u8_putc(&src,'\a'); break;
      case 'f': u8_putc(&src,'\f'); break;
      case 'b': u8_putc(&src,'\b'); break;
      case 'v': u8_putc(&src,'\v'); break;
      case '\\': u8_putc(&src,'\\'); break;
      case 'u': case 'U': {
        char buf[9];
        int i=0, n = (c='u') ? (4) : (8);
        while (i<n) {
          int nc=u8_getc(in);
          if (!(isxdigit(nc))) {
            u8_seterr("Invalid escape in Regex","parse_regex",
                      u8_strdup(src.u8_outbuf));
            u8_close((u8_stream)&src);
            return FD_ERROR;}
          buf[i++]=nc;}
        buf[i]='\0';
        u8_putc(&src,atol(buf));
        break;}
      default:
        u8_putc(&src,'\\');
        if (c>0) u8_putc(&src,c);}}
    else if (c!='/') u8_putc(&src,c);
    else {
      int mc = u8_getc(in);
      while ((mc<128)&&(u8_isalpha(mc))) {
        if (strchr("eils",mc)) *optwrite++=(char)mc;
        else {
          fd_seterr(fd_ParseError,"parse_regex",src.u8_outbuf,VOID);
          return FD_PARSE_ERROR;}
        mc = u8_getc(in);}
      u8_ungetc(in,mc);
      *optwrite++='\0';
      result = make_regex(src.u8_outbuf,opts);
      u8_close((u8_stream)&src);
      return result;}
    c = u8_getc(in);}
  return FD_EOF;
}

static lispval make_regex(u8_string src_arg,u8_string opts)
{
  struct FD_REGEX *ptr = u8_alloc(struct FD_REGEX);
  int retval, cflags = REG_EXTENDED;
  u8_string src = u8_strdup(src_arg);
  FD_INIT_FRESH_CONS(ptr,fd_regex_type);
  if (strchr(opts,'i')) cflags |= REG_ICASE;
  else if (strchr(opts,'c')) cflags &= ~REG_ICASE;
  else if (strchr(opts,'m')) cflags |= REG_NEWLINE;
  else {}
  retval = regcomp(&(ptr->rxcompiled),src,cflags);
  if (retval) {
    u8_byte buf[512];
    regerror(retval,&(ptr->rxcompiled),buf,512);
    u8_free(ptr);
    return fd_err(fd_RegexError,"parse_regex",u8_strdup(buf),VOID);}
  else {
    U8_CLEAR_ERRNO();
    ptr->rxflags = cflags; ptr->rxsrc = src;
    u8_init_mutex(&(ptr->rx_lock)); ptr->rxactive = 1;
    return LISP_CONS(ptr);}
}

static lispval parse_opaque(U8_INPUT *in)
{
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,500);
  int c = u8_getc(in), opaque=0;
  if (c=='<') { c=u8_getc(in); opaque=1;}
  while (c>0) {
    if (c == '\\') c=u8_getc(in);
    else if (c == '>') break;
    else NO_ELSE;
    u8_putc(&out,c);
    c=u8_getc(in);}
  if (c<0) {
    u8_log(LOG_WARN,"BadOpaqueExpr",
           "No missing close char in: #<<%s",
           out.u8_outbuf);}
  else if (opaque) {
    int nextc = u8_getc(in);
    if (nextc != '>') {
      u8_ungetc(in,nextc);
      u8_log(LOG_WARN,"BadOpaqueExpr",
             "Missing close in: #<<%s>%c",
             out.u8_outbuf,nextc);}}
  else NO_ELSE;
  lispval string = fd_stream2string(&out);
  return fd_init_compound(NULL,opaque_tag,0,1,string);
}

/* Packet parsing functions */

static lispval parse_text_packet(U8_INPUT *in)
{
  char *data = u8_malloc(128);
  int max = 128, len = 0, c = u8_getc(in);
  while ((c>=0) && (c<128) && (c!='"')) {
    if (len>=max) {
      data = u8_realloc(data,max+128);
      max = max+128;}
    if (c == '\\') {
      char obuf[8];
      obuf[0]=c = u8_getc(in);
      if (obuf[0]=='\n') {
        while (u8_isspace(c)) c = u8_getc(in);
        continue;}
      else switch (c) {
        case '\\':
          data[len++]='\\'; break;
        case 'a':
          data[len++]='\a'; break;
        case 'f': case 'l':
          data[len++]='\f'; break;
        case 'h': case 'b':
          data[len++]='\b'; break;
        case 'n':
          data[len++]='\n'; break;
        case 't':
          data[len++]='\t'; break;
        case 'r':
          data[len++]='\r'; break;
        case 'z':
          data[len++]=26; break;
        case '#':
          data[len++]='#'; break;
        case 'x': {
          int c1 = u8_getc(in), c2 = u8_getc(in);
          if ((c1<0)||(c2<0)) {
            u8_free(data);
            return FD_EOX;}
          if ((isxdigit(c1))&&(isxdigit(c2))) {
            char xbuf[4];
            xbuf[0]=(char) c1; xbuf[1]=(char) c2; xbuf[2]='\0';
            c = strtol(xbuf,NULL,16);
            data[len++]=c;
            break;}
          else {
            struct U8_OUTPUT tmpout; u8_byte buf[16];
            U8_INIT_FIXED_OUTPUT(&tmpout,16,buf);
            u8_putc(&tmpout,'\\');
            u8_putc(&tmpout,c1);
            u8_putc(&tmpout,c2);
            u8_seterr(_("Bad hex escape"),"parse_text_packet",
                      u8_strdup(tmpout.u8_outbuf));
            u8_free(data);
            return FD_PARSE_ERROR;}}
        case '0': case '1': case '2': case '3': {
          int i = 1; while ((i<3)&&((c = u8_getc(in))>=0)&&(odigitp(c))) {
            obuf[i++]=c;}
          obuf[i]='\0';
          if (c<0) {
            u8_free(data);
            return FD_EOX;}
          else if (!(odigitp(c))) {
            u8_free(data);
            u8_seterr(_("Bad octal escape"),"parse_text_packet",
                      u8_strdup(obuf));
            return FD_PARSE_ERROR;}
          else {
            c = strtol(obuf,NULL,8);
            if (c<256) {
              data[len++]=c;
              break;}
            else {
              u8_free(data);
              u8_seterr(_("Bad octal escape"),"parse_text_packet",
                        u8_strdup(obuf));
              return FD_PARSE_ERROR;}}}
        default:
          data[len++]=c;}}
    else data[len++]=c;
    c = u8_getc(in);}
  if (c=='"') {
    lispval packet = fd_bytes2packet(NULL,len,data);
    u8_free(data);
    return packet;}
  else {
    u8_free(data);
    if (c<0) return FD_EOX;
    else {
      u8_seterr(fd_MissingCloseQuote,"parse_text_packet",NULL);
      return FD_PARSE_ERROR;}}
}

static lispval parse_hex_packet(U8_INPUT *in)
{
  char *data = u8_malloc(128);
  int max = 128, len = 0, c = u8_getc(in);
  while (isxdigit(c)) {
    int nc = u8_getc(in), byte = 0; char xbuf[3];
    if (!(isxdigit(nc))) {c = nc; break;}
    if (len>=max) {
      data = u8_realloc(data,max+128);
      max = max+128;}
    xbuf[0]=c; xbuf[1]=nc; xbuf[2]='\0';
    byte = strtol(xbuf,NULL,16);
    data[len++]=byte;
    c = u8_getc(in);}
  if (c=='"') {
    lispval result = fd_bytes2packet(NULL,len,data);
    u8_free(data);
    return result;}
  else if (c<0) {
    u8_free(data);
    return FD_EOX;}
  else {
    u8_byte buf[16]; struct U8_OUTPUT tmpout;
    U8_INIT_FIXED_OUTPUT(&tmpout,16,buf);
    u8_putc(&tmpout,c);
    u8_seterr(fd_InvalidHexCharacter,"parse_hex_packet",
              u8_strdup(tmpout.u8_outbuf));
    u8_free(data);
    return FD_PARSE_ERROR;}
}

static lispval parse_base64_packet(U8_INPUT *in)
{
  char *data = u8_malloc(128);
  int max = 128, len = 0, c = u8_getc(in);
  while ((c>=0)&&(c!='"')) {
    if (len>=max) {
      data = u8_realloc(data,max+128);
      max = max+128;}
    if (!((isalnum(c))||
          (isspace(c))||(u8_isspace(c))||
          (c=='+')||(c=='/')||(c=='='))) {
      u8_byte buf[16]; struct U8_OUTPUT tmpout;
      U8_INIT_FIXED_OUTPUT(&tmpout,16,buf);
      u8_putc(&tmpout,c);
      u8_seterr(fd_InvalidBase64Character,"parse_base64_packet",
                u8_strdup(tmpout.u8_outbuf));
      u8_free(data);
      return FD_PARSE_ERROR;}
    data[len++]=c;
    c = u8_getc(in);}
  if (c<0) {
    u8_free(data);
    return FD_EOX;}
  else if (c=='"') {
    int n_bytes = 0;
    unsigned char *bytes = u8_read_base64(data,data+len,&n_bytes);
    if (bytes) {
      lispval result = fd_make_packet(NULL,n_bytes,bytes);
      u8_free(bytes); u8_free(data);
      return result;}
    else {
      u8_free(data);
      u8_seterr(fd_ParseError,"parse_base64_packet",NULL);
      return FD_PARSE_ERROR;}}
  else {
    u8_free(data);
    u8_seterr(fd_MissingCloseQuote,"parse_base64_packet",NULL);
    return FD_PARSE_ERROR;}
}

static lispval parse_packet(U8_INPUT *in,int nextc)
{
  if (nextc<0) return FD_EOF;
  else if (nextc=='"')
    return parse_text_packet(in);
  else if ((nextc=='X')||(nextc=='x')) {
    int nc = u8_getc(in);
    if (nc=='"') return parse_hex_packet(in);
    else {
      u8_seterr(fd_MissingOpenQuote,"parse_packet",NULL);
      return FD_PARSE_ERROR;}}
  else if (nextc=='@') {
    int nc = u8_getc(in);
    if (nc=='"') return parse_base64_packet(in);
    else {
      u8_seterr(fd_MissingOpenQuote,"parse_packet",NULL);
      return FD_PARSE_ERROR;}}
  else {
    u8_seterr(fd_ParseError,"parse_packet",NULL);
    return FD_PARSE_ERROR;}
}

static int copy_string(u8_input s,u8_output a)
{
  int c = u8_getc(s);
  if (c!='"') return c;
  u8_putc(a,c);
  while ((c = u8_getc(s))>=0) {
      if (c == '"') {
        u8_putc(a,c);
        return c;}
      else if (c == '\\') {
        int nextc = u8_getc(s);
        if (nextc=='\n') {
          while (u8_isspace(nextc)) nextc = u8_getc(s);
          u8_putc(a,nextc);
          continue;}
        else u8_ungetc(s,nextc);
        c = read_escape(s);
        if (c<0) return c;
        u8_putc(a,c);}
      else u8_putc(a,c);}
  return c;
}

/* Compound object parsing */

#define iscloser(c) (((c)==')') || ((c)==']') || ((c)=='}'))

static lispval *parse_vec(u8_input in,char end_char,int *size)
{
  lispval *elts = u8_alloc_n(32,lispval);
  unsigned int n_elts = 0, max_elts = 32;
  int ch = skip_whitespace(in);
  while ((ch>=0) && (ch != end_char) && (!(iscloser(ch)))) {
    lispval elt = fd_parser(in);
    if (PARSE_ABORTP(elt)) {
      int i = 0; while (i < n_elts) {
        fd_decref(elts[i]); i++;}
      u8_free(elts);
      if (elt == FD_EOX) *size = -1;
      else if (elt == FD_PARSE_ERROR) *size = -2;
      else *size = -3;
      if (PARSE_ABORTP(elt))
        fd_interr(elt);
      return NULL;}
    else if (n_elts == max_elts) {
      lispval *new_elts = u8_realloc_n(elts,max_elts*2,lispval);
      if (new_elts) {elts = new_elts; max_elts = max_elts*2;}
      else {
        int i = 0; while (i < n_elts) {fd_decref(elts[i]); i++;}
        u8_free(elts); *size = n_elts;
        return NULL;}}
    elts[n_elts++]=elt;
    ch = skip_whitespace(in);}
  if (ch == end_char) {
    *size = n_elts;
    ch = u8_getc(in); /* Skip the end char */
    if (n_elts) return elts;
    else {
      u8_free(elts);
      return NULL;}}
  else {
    int i = 0; while (i < n_elts) {fd_decref(elts[i]); i++;}
    u8_free(elts);
    if (ch<0) {
      *size = ch;
      fd_seterr2(fd_UnexpectedEOF,"parse_vec");}
    else {
      *size = -1;
      fd_seterr(fd_MismatchedClose,"parse_vec",NULL,FD_CODE2CHAR(end_char));}
    return NULL;}
}

static lispval parse_list(U8_INPUT *in)
{
  /* This starts parsing the list after a '(' has been read. */
  int ch = skip_whitespace(in); lispval head = VOID;
  if (ch<0)
    if (ch== -1) return FD_EOX;
    else return FD_PARSE_ERROR;
  else if (ch == ')') {
    /* The empty list case */
    u8_getc(in); return NIL;}
  else if (ch == ']') {
    fd_seterr(fd_MismatchedClose,"parse_list",NULL,head);
    return FD_PARSE_ERROR;}
  else {
    /* This is where we build the list.  We recur in the CAR direction and
       iterate in the CDR direction to avoid growing the stack. */
    struct FD_PAIR *scan;
    lispval car = fd_parser(in), head;
    if (PARSE_ABORTP(car))
      return car;
    else {
      scan = u8_alloc(struct FD_PAIR);
      FD_INIT_CONS(scan,fd_pair_type);
      if (scan == NULL) {fd_decref(car); return FD_OOM;}
      else head = fd_init_pair(scan,car,NIL);}
    ch = skip_whitespace(in);
    while ((ch>=0) && (ch != ')')) {
      /* After starting with the head, we iterate until we get to
         the closing paren, except for the dotted pair exit clause. */
      lispval list_elt; struct FD_PAIR *new_pair;
      if (ch == '.') {
        int nextch = u8_getc(in), probed = u8_probec(in);
        if (u8_isspace(probed)) break;
        else u8_ungetc(in,nextch);}
      list_elt = fd_parser(in);
      if (PARSE_ABORTP(list_elt)) {
        fd_decref(head); return list_elt;}
      new_pair = u8_alloc(struct FD_PAIR);
      if (new_pair) {
        scan->cdr = fd_init_pair(new_pair,list_elt,NIL);
        scan = new_pair;}
      else {
        fd_decref(head); fd_decref(list_elt);
        return FD_OOM;}
      ch = skip_whitespace(in);}
    if (ch<0) {
      fd_decref(head);
      if (ch== -1) return FD_EOX;
      else return FD_PARSE_ERROR;}
    else if (ch == ')') {
      u8_getc(in);
      return head;}
    else {
      lispval tail;
      tail = fd_parser(in);
      if (PARSE_ABORTP(tail)) {
        fd_decref(head);
        return tail;}
      skip_whitespace(in); ch = u8_getc(in);
      if (ch == ')') {scan->cdr = tail; return head;}
      fd_decref(head); fd_decref(tail);
      return FD_PARSE_ERROR;}}
}

static lispval parse_bracket_list(U8_INPUT *in)
{
  /* This starts parsing the list after a '(' has been read. */
  int ch = skip_whitespace(in); lispval head = VOID;
  if (ch<0)
    if (ch== -1) return FD_EOX;
    else return FD_PARSE_ERROR;
  else if (ch == ']') {
    /* The empty list case */
    u8_getc(in); return NIL;}
  else if (ch == ')') {
    fd_seterr(fd_MismatchedClose,"parse_bracket_list",NULL,head);
    return FD_PARSE_ERROR;}
  else {
    /* This is where we build the list.  We recur in the CAR direction and
       iterate in the CDR direction to avoid growing the stack. */
    struct FD_PAIR *scan;
    lispval car = fd_parser(in), head;
    if (PARSE_ABORTP(car))
      return car;
    else {
      scan = u8_alloc(struct FD_PAIR);
      FD_INIT_CONS(scan,fd_pair_type);
      if (scan == NULL) {fd_decref(car); return FD_OOM;}
      else head = fd_init_pair(scan,car,NIL);}
    ch = skip_whitespace(in);
    while ((ch>=0) && (ch != ']')) {
      /* After starting with the head, we iterate until we get to
         the closing paren, except for the dotted pair exit clause. */
      lispval list_elt; struct FD_PAIR *new_pair;
      if (ch == '.') {
        int nextch = u8_getc(in), probed = u8_probec(in);
        if (u8_isspace(probed)) break;
        else u8_ungetc(in,nextch);}
      list_elt = fd_parser(in);
      if (PARSE_ABORTP(list_elt)) {
        fd_decref(head);
        return list_elt;}
      new_pair = u8_alloc(struct FD_PAIR);
      if (new_pair) {
        scan->cdr = fd_init_pair(new_pair,list_elt,NIL);
        scan = new_pair;}
      else {
        fd_decref(head); fd_decref(list_elt);
        return FD_OOM;}
      ch = skip_whitespace(in);}
    if (ch<0) {
      fd_decref(head);
      if (ch== -1) return FD_EOX;
      else return FD_PARSE_ERROR;}
    else if (ch == ']') {
      u8_getc(in);
      return head;}
    else {
      lispval tail;
      tail = fd_parser(in);
      if (PARSE_ABORTP(tail)) {
        fd_decref(head);
        return tail;}
      skip_whitespace(in); ch = u8_getc(in);
      if (ch == ')') {scan->cdr = tail; return head;}
      fd_decref(head); fd_decref(tail);
      return FD_PARSE_ERROR;}}
}

static lispval parse_vector(U8_INPUT *in)
{
  int n_elts = -2;
  lispval *elts = parse_vec(in,')',&n_elts);
  if (n_elts>=0)
    return fd_init_vector(u8_alloc(struct FD_VECTOR),n_elts,elts);
  else return FD_PARSE_ERROR;
}

static lispval parse_code(U8_INPUT *in)
{
  int n_elts = -2;
  lispval *elts = parse_vec(in,')',&n_elts);
  if (n_elts>=0)
    return fd_init_code(u8_alloc(struct FD_VECTOR),n_elts,elts);
  else return FD_PARSE_ERROR;
}

static lispval parse_slotmap(U8_INPUT *in)
{
  int n_elts = -2;
  lispval *elts = parse_vec(in,']',&n_elts);
  if (PRED_FALSE(n_elts<0)) return FD_PARSE_ERROR;
  else if (n_elts>7)
    return fd_init_slotmap(NULL,n_elts/2,(struct FD_KEYVAL *)elts);
  else {
    lispval result =
      fd_make_slotmap(n_elts/2,n_elts/2,(struct FD_KEYVAL *)elts);
    u8_free(elts);
    return result;}
}

static lispval parse_choice(U8_INPUT *in)
{
  int ch = skip_whitespace(in);
  if (ch == '}') {
    u8_getc(in); return EMPTY;}
  else if (ch < 0)
    if (ch== -1) return FD_EOX; else return FD_PARSE_ERROR;
  else {
    int n_elts = -2; lispval *elts = parse_vec(in,'}',&n_elts);
    if (n_elts==0) return EMPTY;
    else if (elts == NULL)
      return FD_PARSE_ERROR;
    else if (n_elts==1) {
      lispval v = elts[0]; u8_free(elts);
      return v;}
    else if ((elts) && (n_elts>0)) {
      struct FD_CHOICE *ch = fd_alloc_choice(n_elts);
      lispval result = fd_init_choice(ch,n_elts,elts,FD_CHOICE_DOSORT);
      if (FD_XCHOICE_SIZE(ch)==1) {
        result = FD_XCHOICE_DATA(ch)[0];
        u8_big_free(ch);}
      u8_free(elts);
      return result;}
    else return FD_PARSE_ERROR;}
}

static lispval parse_qchoice(U8_INPUT *in)
{
  int n_elts = -2;
  lispval *elts = parse_vec(in,'}',&n_elts);
  if (n_elts==0)
    return fd_init_qchoice(u8_alloc(struct FD_QCHOICE),
                           EMPTY);
  else if (n_elts==1) {
    lispval result = elts[0]; u8_free(elts);
    return result;}
  else if (n_elts>1) {
    struct FD_CHOICE *xch = fd_alloc_choice(n_elts);
    lispval choice = fd_init_choice(xch,n_elts,elts,FD_CHOICE_DOSORT);
    if (FD_XCHOICE_SIZE(xch)==1) {
      lispval result = FD_XCHOICE_DATA(xch)[0];
      u8_free(elts); return result;}
    u8_free(elts);
    return fd_init_qchoice(u8_alloc(struct FD_QCHOICE),choice);}
  else if (n_elts== -1) return FD_EOX;
  else return FD_PARSE_ERROR;
}

/* Record parsing */

static lispval recreate_record(int n,lispval *v)
{
  int i = 0;
  struct FD_COMPOUND_TYPEINFO *entry = fd_lookup_compound(v[0]);
  if ((entry) && (entry->compound_parser)) {
    lispval result = entry->compound_parser(n,v,entry);
    if (!(VOIDP(result))) {
      while (i<n) {fd_decref(v[i]); i++;}
      if (v) u8_free(v);
      return result;}}
  {
    struct FD_COMPOUND *c=
      u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*LISPVAL_LEN);
    lispval *data = &(c->compound_0); fd_init_compound(c,v[0],0,0);
    c->compound_length = n-1;
    i = 1; while (i<n) {data[i-1]=v[i]; i++;}
    if (v) u8_free(v);
    return LISP_CONS(c);}
}

static lispval parse_record(U8_INPUT *in)
{
  int n_elts;
  lispval *elts = parse_vec(in,')',&n_elts);
  if (n_elts>0)
    return recreate_record(n_elts,elts);
  else if (n_elts==0)
    return fd_err(fd_CantParseRecord,"parse_record","empty record",VOID);
  else if (n_elts== -1) return FD_EOX;
  else return FD_PARSE_ERROR;
}

/* The main parser procedure */

static lispval parse_atom(u8_input in,int ch1,int ch2);

FD_EXPORT
/* fd_parser:
     Arguments: a U8 input stream and a memory pool
     Returns: a lisp object

     Parses a textual object representation from a stream into a lisp object.
*/
lispval fd_parser(u8_input in)
{
  int inchar = skip_whitespace(in);
  if (inchar<0) {
    if (inchar== -1) return FD_EOX;
    else return FD_PARSE_ERROR;}
  else switch (inchar) {
  case ')': case ']': case '}': {
    u8_string details=u8_get_input_context(in,32,32,">!<");
    u8_getc(in); /* Consume the character */
    return fd_err(fd_ParseError,"unexpected terminator",
                  details,FD_CODE2CHAR(inchar));}
  case '"': return parse_string(in);
  case '@': return parse_oid(in);
  case '(':
    /* Skip the open paren and parse the list */
    u8_getc(in); return parse_list(in);
  case '[':
    /* Skip the open paren and parse the list */
    u8_getc(in); return parse_bracket_list(in);
  case '{':
    /* Skip the open brace and parse the choice */
    u8_getc(in); return parse_choice(in);
  case '\'': {
    lispval content;
    u8_getc(in); /* Skip the quote mark */
    content = fd_parser(in);
    if (PARSE_ABORTP(content))
      return content;
    else return fd_make_list(2,quote_symbol,content);}
  case '`': {
    lispval content;
    u8_getc(in); /* Skip the quote mark */
    content = fd_parser(in);
    if (PARSE_ABORTP(content))
      return content;
    else return fd_make_list(2,quasiquote_symbol,content);}
  case ',': {
    lispval content; int c = u8_getc(in); c = u8_getc(in);
    /* Skip the quote mark and check for an atsign. */
    if (c != '@') u8_ungetc(in,c);
    content = fd_parser(in);
    if (PARSE_ABORTP(content))
      return content;
    else if (c == '@')
      return fd_make_list(2,unquotestar_symbol,content);
    else return fd_make_list(2,unquote_symbol,content);}
  case '|': { /* Escaped symbol */
    struct U8_OUTPUT tmpbuf; char buf[128];
    lispval result; U8_MAYBE_UNUSED int c;
    U8_INIT_STATIC_OUTPUT_BUF(tmpbuf,128,buf);
    c = copy_atom(in,&tmpbuf);
    result = fd_make_symbol(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
    if (tmpbuf.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpbuf.u8_outbuf);
    return result;}
  case '#': {
    /* Absorb the # and set ch to the next character, dispatching on
       that */
    int ch = u8_getc(in); ch = u8_getc(in);
    switch (ch) {
    case '(': return parse_vector(in);
    case '~': {
      ch = u8_getc(in); if (ch<0) return FD_EOX;
      if (ch!='(') return fd_err(fd_ParseError,"fd_parser",NULL,VOID);
      return parse_code(in);}
    case '{': return parse_qchoice(in);
    case '[': return parse_slotmap(in);
    case '|': {
      int bar = 0;
      /* Skip block #|..|# comment */
      while ((ch = u8_getc(in))>=0)
        if (ch=='|') bar = 1;
        else if ((bar) && (ch=='#')) break;
        else bar = 0;
      if (ch<0) {
        u8_seterr(fd_UnterminatedBlockComment,"fd_parser",NULL);
        return FD_PARSE_ERROR;}
      else return fd_parser(in);}
    case '*': {
      int nextc = u8_getc(in);
      lispval result = parse_packet(in,nextc);
      if (PACKETP(result)) {
        FD_SET_CONS_TYPE(result,fd_secret_type);}
      return result;}
    case 'X': case 'x': case '@': case '"':
      return parse_packet(in,ch);
    case '/': return parse_regex(in);
    case '<': return parse_opaque(in);
    case ';': {
      lispval content = fd_parser(in);
      if (PARSE_ABORTP(content))
        return content;
      else return fd_conspair(comment_symbol,
                              fd_conspair(content,NIL));}
    case '%': {
      int c = u8_getc(in);
      if (c=='(') return parse_record(in);
      else return FD_PARSE_ERROR;}
    case '\\': return parse_character(in);
    case '#': return fd_make_list(2,histref_symbol,fd_parser(in));
    case 'U': return parse_atom(in,inchar,ch); /* UUID */
    case 'T': return parse_atom(in,inchar,ch); /* TIMESTAMP */
    case '!': return parse_atom(in,inchar,ch); /* pointer reference */
    default:
      if (u8_ispunct(ch)) {
        /* This introduced a hash-punct sequence which is used
           for other kinds of character macros. */
        u8_byte buf[16]; struct U8_OUTPUT out; int nch;
        lispval punct_code, punct_code_arg;
        U8_INIT_OUTPUT_X(&out,16,buf,U8_FIXED_STREAM);
        u8_putc(&out,'#'); u8_putc(&out,ch); nch = u8_getc(in);
        while ((u8_ispunct(nch))&&
               ((nch>128)||(strchr("\"([{",nch) == NULL))) {
          u8_putc(&out,nch);
          if ((out.u8_write-out.u8_outbuf)>11) {
            fd_seterr(fd_ParseError,"fd_parser","invalid hash # prefix",
                      fd_stream2string(&out));
            return FD_PARSE_ERROR;}
          else nch = u8_getc(in);}
        u8_ungetc(in,nch);
        punct_code = fd_intern(buf);
        punct_code_arg = fd_parser(in);
        return fd_make_list(2,punct_code,punct_code_arg);}
      else {
        /* In this case, it's just an atom */
        u8_ungetc(in,ch);
        return parse_atom(in,'#',-1);}}}
  default:
    return parse_atom(in,-1,-1);}
}

static lispval parse_atom(u8_input in,int ch1,int ch2)
{
  /* Parse an atom, i.e. a printed representation which doesn't
     contain any special spaces or other special characters */
  struct U8_OUTPUT tmpbuf; char buf[128];
  lispval result; U8_MAYBE_UNUSED int c;
  U8_INIT_STATIC_OUTPUT_BUF(tmpbuf,128,buf);
  if (ch1>=0) u8_putc(&tmpbuf,ch1);
  if (ch2>=0) u8_putc(&tmpbuf,ch2);
  c = copy_atom(in,&tmpbuf);
  if (tmpbuf.u8_write == tmpbuf.u8_outbuf) result = FD_EOX;
  else if (ch1 == '|')
    result = fd_make_symbol(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
  else result = fd_parse_atom(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
  if (tmpbuf.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpbuf.u8_outbuf);
  return result;
}

FD_EXPORT
/* fd_parse_expr:
     Arguments: a U8 input stream
     Returns: a lisp object

     This returns FD_EOF if there is nothing to read.
     It is distinct from fd_parser which returns FD_EOX
      (an error) if there is nothing to read.
*/
lispval fd_parse_expr(u8_input in)
{
  int inchar = skip_whitespace(in);
  if (inchar<0)
    return FD_EOF;
  // TODO: When this returns an error, add details from in
  else return fd_parser(in);
}

FD_EXPORT
/* fd_parser:
     Arguments: a string
     Returns: a lisp object

Parses a textual object representation into a lisp object. */
lispval fd_parse(u8_string s)
{
  struct U8_INPUT stream;
  U8_INIT_STRING_INPUT((&stream),-1,s);
  return fd_parser(&stream);
}

#if 0
FD_EXPORT
/* fd_parse_arg:
     Arguments: a string
     Returns: a lisp object

     Parses a textual object representation into a lisp object.  This is
     designed for command line arguments or other external contexts
     (e.g. Windows registry entries).  The idea is to be able to easily
     pass strings (without embedded double quotes) while still allowing
     arbitrary expressions.  If the string starts with a parser-significant
     character, the parser is called on it.  If the string starts with a ':',
     the parser is called on the rest of the string (so you can refer to the
     symbol FOO as ":foo").  If the string starts with a backslash, a lisp string
     is created from the rest of the string.  Otherwise, a lisp string is
     just created from the string.
*/
lispval fd_parse_arg(u8_string arg)
{
  if (*arg=='\0') return lispval_string(arg);
  else if (*arg == ':')
    if (arg[1]=='\0') return lispval_string(arg);
    else {
      lispval val = fd_parse(arg+1);
      if (PARSE_ABORTP(val)) {
        u8_log(LOG_WARN,fd_ParseArgError,"Bad colon spec arg '%s'",arg);
        fd_clear_errors(1);
        return lispval_string(arg);}
      else return val;}
  else if (*arg == '\\') return lispval_string(arg+1);
  else if ((isdigit(arg[0])) ||
           ((strchr("+-.",arg[0])) && (isdigit(arg[1]))) ||
           ((arg[0]=='#') && (strchr("OoXxDdBbIiEe",arg[1])))) {
    lispval num = fd_string2number(arg,-1);
    if (NUMBERP(num)) return num;
    else return lispval_string(arg);}
  else if (arg[0] == '\'')
    return fd_symbolize(arg+1);
  else if (strchr("@{#(\"|",arg[0])) {
    lispval result;
    struct U8_INPUT stream;
    U8_INIT_STRING_INPUT((&stream),-1,arg);
    result = fd_parser(&stream);
    if (PARSE_ABORTP(result)) {
      fd_clear_errors(1);
      return lispval_string(arg);}
    else if (fd_skip_whitespace(&stream)>0) {
      /* If there's more than one object, take the whole arg as a string */
      fd_decref(result);
      return lispval_string(arg);}
    else return result;}
  else return lispval_string(arg);
}
#endif

FD_EXPORT
/* fd_parse_arg:
     Arguments: a string
     Returns: a lisp object

     Parses a textual object representation into a lisp object.  This is
     designed for command line arguments or other external contexts
     (e.g. Windows registry entries).  The idea is to be able to easily
     pass strings (without embedded double quotes) while still allowing
     arbitrary expressions.  If the string starts with a parser-significant
     character, the parser is called on it.  If the string starts with a ':',
     the parser is called on the rest of the string (so you can refer to the
     symbol FOO as ":foo").  If the string starts with a backslash, a lisp string
     is created from the rest of the string.  Otherwise, a lisp string is
     just created from the string.
*/
lispval fd_read_arg(u8_input in)
{
  int c=u8_probec(in);
  if (c<0)
    return lispval_string("");
  else if ((c==':') || (c=='\'')) {
    c=u8_getc(in);
    int nextc=u8_probec(in);
    if (nextc<0) {
      char buf[2]="a"; buf[0]=c;
      return lispval_string(buf);}
    lispval val=fd_parser(in);
    if (PARSE_ABORTP(val)) {
      fd_clear_errors(1);
      return FD_FALSE;}
    else return val;}
  else if (c == '\\') {
    lispval val=lispval_string(in->u8_read+1);
    in->u8_read=in->u8_inlim;
    return val;}
  else if (strchr("@{#(\"|0123456789",c)) {
    u8_string start=in->u8_read;
    lispval result=fd_parser(in);
    if (PARSE_ABORTP(result)) {
      fd_clear_errors(1);
      if ( (start>=in->u8_inbuf) && (start<=in->u8_read) )
        return lispval_string(start);
      else return EMPTY;}
    else return result;}
  else return lispval_string(in->u8_read);
}

FD_EXPORT
/* fd_parse_arg:
     Arguments: a string
     Returns: a lisp object

     Parses a textual object representation into a lisp object.  This is
     designed for command line arguments or other external contexts
     (e.g. Windows registry entries).  The idea is to be able to easily
     pass strings (without embedded double quotes) while still allowing
     arbitrary expressions.  If the string starts with a parser-significant
     character, the parser is called on it.  If the string starts with a ':',
     the parser is called on the rest of the string (so you can refer to the
     symbol FOO as ":foo").  If the string starts with a backslash, a lisp string
     is created from the rest of the string.  Otherwise, a lisp string is
     just created from the string.
*/
lispval fd_parse_arg(u8_string arg)
{
  if ( (*arg=='\0') ||
       ( ((*arg == ':') || (*arg == '\'')) &&
         (arg[1]=='\0') ) )
    return lispval_string(arg);
  else if (*arg == '\\')
    return lispval_string(arg+1);
  else {
    lispval result;
    struct U8_INPUT stream;
    U8_INIT_STRING_INPUT((&stream),-1,arg);
    result = fd_read_arg(&stream);
    if (PARSE_ABORTP(result)) {
      fd_clear_errors(1);
      return lispval_string(arg);}
    else if (fd_skip_whitespace(&stream)>0) {
      /* If there's more than one object, take the whole arg as a string */
      fd_decref(result);
      return lispval_string(arg);}
    else return result;}
}

/* Initializations */

FD_EXPORT void fd_init_parse_c()
{
  u8_register_source_file(_FILEINFO);

  u8_init_rwlock(&hashnames_lock);

  quote_symbol = fd_intern("QUOTE");
  quasiquote_symbol = fd_intern("QUASIQUOTE");
  unquote_symbol = fd_intern("UNQUOTE");
  unquotestar_symbol = fd_intern("UNQUOTE*");
  histref_symbol = fd_intern("%HISTREF");
  comment_symbol = fd_intern("COMMENT");
  opaque_tag = fd_intern("%OPAQUE");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
