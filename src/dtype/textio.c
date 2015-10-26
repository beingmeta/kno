/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2015 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright
   and a valuable trade secret of beingmeta, inc.

   This file implements the core parser and printer (unparser) functionality.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1
#include "framerd/fdsource.h"
#include "framerd/dtype.h"

#include <libu8/u8printf.h>
#include <libu8/u8streamio.h>
#include <libu8/u8crypto.h>

#include <ctype.h>
#include <errno.h>
/* We include this for sscanf, but we're not using the FILE functions */
#include <stdio.h>

#include <stdarg.h>

/* Common macros, functions and declarations */

#define PARSE_ERRORP(x) ((x==FD_EOX) || (x==FD_PARSE_ERROR) || (x==FD_OOM))

#define isoctaldigit(c) ((c<'8') && (isdigit(c)))
#define spacecharp(c) ((c>0) && (c<128) && (isspace(c)))
#define atombreakp(c) \
  ((c<=0) || ((c<128) && ((isspace(c)) || (strchr("{}()[]#\"',`",c)))))

fd_exception fd_CantOpenFile=_("Can't open file");
fd_exception fd_FileNotFound=_("File not found");
fd_exception fd_BadEscapeSequence=_("Invalid escape sequence");
fd_exception fd_InvalidConstant=_("Invalid constant reference");
fd_exception fd_InvalidCharacterConstant=_("Invalid character constant");
fd_exception fd_BadAtom=_("Bad atomic expression");
fd_exception fd_NoPointerExpressions=_("no pointer expressions allowed");
fd_exception fd_BadPointerRef=_("bad pointer reference");
fd_exception fd_UnexpectedEOF=_("Unexpected EOF in LISP expression");
fd_exception fd_ParseError=_("LISP expression parse error");
fd_exception fd_ParseArgError=_("External LISP argument parse error");
fd_exception fd_CantUnparse=_("LISP expression unparse error");
fd_exception fd_CantParseRecord=_("Can't parse record object");
fd_exception fd_MismatchedClose=_("Expression open/close mismatch");

int fd_unparse_maxelts=0;
int fd_unparse_maxchars=0;
int fd_unparse_hexpacket=0;

int fd_interpret_pointers=1;

int (*fd_unparse_error)(U8_OUTPUT *,fdtype x,u8_string details)=NULL;

static fdtype quote_symbol, histref_symbol, comment_symbol;
static fdtype quasiquote_symbol, unquote_symbol, unquotestar_symbol;

static int skip_whitespace(u8_input s)
{
  int c=u8_getc(s);
  if (c<-1) return c;
  while (1) {
    while ((c>0) && (spacecharp(c))) c=u8_getc(s);
    if (c==';') {
      while ((c>=0) && (c != '\n')) c=u8_getc(s);
      if (c<0) return -1;}
    else if ((c=='#') && (u8_probec(s)=='|')) {
      int bar=0; c=u8_getc(s);
      while ((c=u8_getc(s))>=0)
        if (c=='|') bar=1;
        else if ((bar) && (c=='#')) break;
        else bar=0;
      if (c=='#') c=u8_getc(s);}
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
static fdtype character_constants[]={
  FD_CODE2CHAR(' '),FD_CODE2CHAR('\n'),FD_CODE2CHAR('\r'),
  FD_CODE2CHAR('\t'),FD_CODE2CHAR('\v'),FD_CODE2CHAR('\v'),
  FD_CODE2CHAR('\f'),FD_CODE2CHAR('\b'),
  FD_CODE2CHAR('\a'),FD_CODE2CHAR('\a'),FD_CODE2CHAR('\a'),
  FD_CODE2CHAR('/'),FD_CODE2CHAR('\\'),FD_CODE2CHAR(';'),
  FD_CODE2CHAR('('),FD_CODE2CHAR(')'),
  FD_CODE2CHAR('{'),FD_CODE2CHAR('}'),
  FD_CODE2CHAR('['),FD_CODE2CHAR(']'),
  0};

/* Unparsing */

static int emit_symbol_name(U8_OUTPUT *out,u8_string name)
{
  const u8_byte *scan=name;
  int c=u8_sgetc(&scan), needs_protection=0;
  while (c>=0)
    if ((atombreakp(c)) ||
        ((u8_islower(c)) && ((u8_toupper(c))!=c))) {
      needs_protection=1; break;}
    else c=u8_sgetc(&scan);
  if (needs_protection==0) u8_puts(out,name);
  else {
    const u8_byte *start=name, *scan=start;
    u8_putc(out,'|');
    while (*scan)
      if ((*scan == '\\') || (*scan == '|')) {
        u8_putn(out,start,scan-start);
        u8_putc(out,'\\'); u8_putc(out,*scan);
        scan++; start=scan;}
      else if (iscntrl(*scan)) {
        char buf[32];
        u8_putn(out,start,scan-start);
        sprintf(buf,"\\%03o",*scan); u8_puts(out,buf);
        scan++; start=scan;}
      else scan++;
    u8_puts(out,start);
    u8_putc(out,'|');}
  return 1;
}

static void output_ellipsis(U8_OUTPUT *out,int n,u8_string unit)
{
  if (n==0) return;
  u8_putc(out,0x00b7);
  u8_putc(out,0x00b7);
  u8_putc(out,0x00b7);
  if (n>=0) {
    if (unit) u8_printf(out,"%d%s",n,unit);
    else u8_printf(out,"%d",n);}
  u8_putc(out,0x00b7);
  u8_putc(out,0x00b7);
  u8_putc(out,0x00b7);

}

static int unparse_string(U8_OUTPUT *out,fdtype x)
{
  struct FD_STRING *s=(struct FD_STRING *)x; int n_chars=0;
  u8_string scan=s->bytes, limit=s->bytes+s->length;
  u8_putc(out,'"'); while (scan < limit) {
    u8_string chunk=scan;
    while ((scan < limit) &&
           (*scan != '"') && (*scan != '\\') &&
           (!((*scan<0x80)&&(iscntrl(*scan))))) {
      scan++; n_chars++;
      if ((fd_unparse_maxchars>0) && (n_chars>=fd_unparse_maxchars)) {
        u8_putn(out,chunk,scan-chunk); u8_putc(out,' ');
        output_ellipsis(out,u8_strlen(scan),"chars");
        return u8_putc(out,'"');}}
    u8_putn(out,chunk,scan-chunk);
    if (scan < limit) {
      int c=*scan++;
      switch (c) {
      case '"': u8_puts(out,"\\\""); break;
      case '\\': u8_puts(out,"\\\\"); break;
      case '\n': u8_puts(out,"\\n"); break;
      case '\t': u8_puts(out,"\\t"); break;
      case '\r': u8_puts(out,"\\r"); break;
      case '\v': u8_puts(out,"\\v"); break;
      case '\a': u8_puts(out,"\\a"); break;
      case '\f': u8_puts(out,"\\f"); break;
      default:
        if (iscntrl(c)) {
          char buf[32]; sprintf(buf,"\\%03o",c);
          u8_puts(out,buf);}
        else u8_putc(out,c);}}}
  /* We don't check for an error value until we get here, which means that
     many of the calls above might have failed, however this shouldn't
     be a functional problem. */
  return u8_putc(out,'"');
}

static int unparse_packet(U8_OUTPUT *out,fdtype x)
{
  struct FD_STRING *s=(struct FD_STRING *)x;
  const unsigned char *bytes=s->bytes;
  int i=0, len=s->length;
  if (fd_unparse_hexpacket) {
    u8_puts(out,"#x\"");
    while (i<len) {
      int byte=bytes[i++]; char buf[16];
      if ((fd_unparse_maxchars>0) && (i>=fd_unparse_maxchars)) {
        u8_putc(out,' '); output_ellipsis(out,len-i,"bytes");
        return u8_putc(out,'"');}
      sprintf(buf,"%02x",byte);
      u8_puts(out,buf);}
    return u8_puts(out,"\"");}
  else {
    u8_puts(out,"#\"");
    if (bytes[0]=='!') {u8_puts(out,"\\!"); i++;}
    while (i < len) {
      int byte=bytes[i++]; char buf[16];
      if ((fd_unparse_maxchars>0) && (i>=fd_unparse_maxchars)) {
        u8_putc(out,' '); output_ellipsis(out,len-i,"bytes");
        return u8_putc(out,'"');}
      if ((byte>=0x7f) || (byte<0x20) ||
          (byte == '\\') || (byte == '"')) {
        sprintf(buf,"\\%02x",byte);
        u8_puts(out,buf);}
      else {
        buf[0]=byte; buf[1]='\0';
        u8_puts(out,buf);}}
    /* We don't check for an error value until we get here, which means that
       many of the calls above might have failed, however this shouldn't
       be a functional problem. */
    return u8_puts(out,"\"");}
}


static int unparse_secret(U8_OUTPUT *out,fdtype x)
{
  struct FD_STRING *s=(struct FD_STRING *)x;
  const unsigned char *bytes=s->bytes; int i=0, len=s->length;
  unsigned char hashbuf[16], *hash;
  u8_printf(out,"#*\"%d:",len);
  hash=u8_md5(bytes,len,hashbuf);
  while (i<16) {u8_printf(out,"%02x",hash[i]); i++;}
  /* We don't check for an error value until we get here, which means that
     many of the calls above might have failed, however this shouldn't
     be a functional problem. */
  return u8_puts(out,"\"");
}

static int unparse_pair(U8_OUTPUT *out,fdtype x)
{
  fdtype car=FD_CAR(x);
  if ((FD_SYMBOLP(car)) && (FD_PAIRP(FD_CDR(x))) &&
      ((FD_CDR(FD_CDR(x)))==FD_EMPTY_LIST)) {
    int false_alarm=0;
    if (car==quote_symbol) u8_puts(out,"'");
    else if (car==quasiquote_symbol) u8_puts(out,"`");
    else if (car==unquote_symbol) u8_puts(out,",");
    else if (car==unquotestar_symbol) u8_puts(out,",@");
    else if (car==comment_symbol) u8_puts(out,"#;");
    else false_alarm=1;
    if (false_alarm==0)
      return fd_unparse(out,FD_CAR(FD_CDR(x)));}
  {
    fdtype scan=x; int len=0, ellipsis_start=-1;
    u8_puts(out,"(");
    fd_unparse(out,FD_CAR(scan)); len++;
    scan=FD_CDR(scan);
    while (FD_PTR_TYPE(scan) == fd_pair_type)
      if ((fd_unparse_maxelts>0) && (len>=fd_unparse_maxelts)) {
        if (len==fd_unparse_maxelts) ellipsis_start=len;
        scan=FD_CDR(scan); len++;}
      else {
        u8_puts(out," ");
        fd_unparse(out,FD_CAR(scan)); len++;
        scan=FD_CDR(scan);}
    if (ellipsis_start>0) {
      u8_puts(out," ");
      output_ellipsis(out,len-ellipsis_start,"elts");}
    if (FD_EMPTY_LISTP(scan)) return u8_puts(out,")");
    else {
      u8_puts(out," . ");
      fd_unparse(out,scan);
      return u8_puts(out,")");}
  }
}

static int unparse_vector(U8_OUTPUT *out,fdtype x)
{
  struct FD_VECTOR *v=(struct FD_VECTOR *) x;
  int i=0, len=v->length;
  u8_puts(out,"#(");
  while (i < len) {
    if ((fd_unparse_maxelts>0) && (i>=fd_unparse_maxelts)) {
      u8_puts(out," "); output_ellipsis(out,len-i,"elts");
      return u8_puts(out,")");}
    if (i>0) u8_puts(out," "); fd_unparse(out,v->data[i]);
    i++;}
  return u8_puts(out,")");
}

static int unparse_rail(U8_OUTPUT *out,fdtype x)
{
  struct FD_VECTOR *v=(struct FD_VECTOR *) x;
  int i=0, len=v->length; fdtype *data=v->data;
  u8_puts(out,"#~(");
  while (i < len) {
    if ((fd_unparse_maxelts>0) && (i>=fd_unparse_maxelts)) {
      u8_puts(out," "); output_ellipsis(out,len-i,"elts");
      return u8_puts(out,")");}
    if (i>0) u8_puts(out," ");
    fd_unparse(out,data[i]);
    i++;}
  return u8_puts(out,")");
}

static int unparse_choice(U8_OUTPUT *out,fdtype x)
{
  struct FD_CHOICE *v=(struct FD_CHOICE *) x;
  const fdtype *data=FD_XCHOICE_DATA(v);
  int i=0, len=FD_XCHOICE_SIZE(v);
  u8_puts(out,"{");
  while (i < len) {
    if (i>0) u8_puts(out," ");
    if ((fd_unparse_maxelts>0) && (i>=fd_unparse_maxelts)) {
      output_ellipsis(out,len-i,"elts");
      return u8_puts(out,"}");}
    else fd_unparse(out,data[i]);
    i++;}
  return u8_puts(out,"}");
}

FD_EXPORT
/* fd_unparse:
     Arguments: a U8 output stream and a lisp object
     Returns: void
  Emits a printed representation of the object to the stream.
*/
int fd_unparse(u8_output out,fdtype x)
{
  switch (FD_PTR_MANIFEST_TYPE(x)) {
  case fd_oid_ptr_type:  /* output OID */
    if (fd_unparsers[fd_oid_type])
      return fd_unparsers[fd_oid_type](out,x);
    else {
      FD_OID addr=FD_OID_ADDR(x); char buf[128];
      unsigned int hi=FD_OID_HI(addr), lo=FD_OID_LO(addr);
      sprintf(buf,"@%x/%x",hi,lo);
      return u8_puts(out,buf);}
  case fd_fixnum_ptr_type: { /* output fixnum */
    int val=FD_FIX2INT(x); char buf[128]; sprintf(buf,"%d",val);
    return u8_puts(out,buf);}
  case fd_immediate_type: { /* output constant */
    fd_ptr_type itype=FD_IMMEDIATE_TYPE(x);
    int data=FD_GET_IMMEDIATE(x,itype);
    if (itype == fd_symbol_type)
      return emit_symbol_name(out,FD_SYMBOL_NAME(x));
    else if (itype == fd_character_type) { /* Output unicode character */
      int c=data; char buf[32];
      if ((c<0x80) && (isalnum(c))) /*  || (u8_isalnum(c)) */
        sprintf(buf,"#\\%c",c);
      else sprintf(buf,"#\\u%04x",c);
      return u8_puts(out,buf);}
    else if (itype == fd_constant_type)
      if ((data<256)&&(fd_constant_names[data]))
        return u8_puts(out,fd_constant_names[data]);
      else {
        char buf[24]; sprintf(buf,"#!%lx",(unsigned long)x);
        return u8_puts(out,buf);}
    else if (fd_unparsers[itype])
      return fd_unparsers[itype](out,x);
    else {
      char buf[24]; sprintf(buf,"#!%lx",(unsigned long)x);
      return u8_puts(out,buf);}}
  case fd_cons_ptr_type: {/* output cons */
    struct FD_CONS *cons=FD_CONS_DATA(x);
    fd_ptr_type ct=FD_CONS_TYPE(cons);
    if ((FD_VALID_TYPEP(ct)) && (fd_unparsers[ct]) && (fd_unparsers[ct](out,x)))
      return 1;
    else if (fd_unparse_error)
      return fd_unparse_error(out,x,_("no handler"));
    else {
      char buf[128]; int retval;
      sprintf(buf,"#!%lx",(unsigned long)x);
      retval=u8_puts(out,buf);
      sprintf(buf,"#!%lx (type=%d)",(unsigned long)x,ct);
      u8_log(LOG_WARN,fd_CantUnparse,buf);
      return retval;}}
  default:
    return 1;
  }
}

FD_EXPORT
/* fd_dtype2string:
     Arguments: a lisp object
     Returns: a UTF-8 encoding string

     Returns a textual encoding of its argument.
*/
u8_string fd_dtype2string(fdtype x)
{
  struct U8_OUTPUT out;
  U8_INIT_OUTPUT(&out,1024);
  fd_unparse(&out,x);
  return out.u8_outbuf;
}

/* Parsing */

static int parse_unicode_escape(u8_string arg)
{
  u8_string s;
  if (arg[0] == '\\') s=arg+1; else s=arg;
  if (s[0] == 'u') {
    if ((strlen(s)==5) &&
        (isxdigit(s[1])) && (isxdigit(s[2])) &&
        (isxdigit(s[3])) && (isxdigit(s[4]))) {
      int code=-1;
      if (sscanf(s+1,"%4x",&code)<1) code=-1;
      return code;}
    else if (s[1]=='{') {
      int code=-1;
      if (sscanf(s+1,"{%x}",&code)<1) code=-1;
      return code;}
    else {
      fd_seterr3(fd_BadEscapeSequence,"parse_unicode_escape",u8_strdup(s));
      return -1;}}
  else if (s[0] == 'U')
    if ((strlen(s)==9) &&
        (isxdigit(s[1])) && (isxdigit(s[2])) &&
        (isxdigit(s[3])) && (isxdigit(s[4])) &&
        (isxdigit(s[5])) && (isxdigit(s[6])) &&
        (isxdigit(s[7])) && (isxdigit(s[8]))) {
      int code=-1;
      if (sscanf(s+1,"%8x",&code)<1) code=-1;
      return code;}
    else if (s[1]=='{') {
      int code=-1;
      if (sscanf(s+1,"{%x}",&code)<1) code=-1;
      return code;}
    else {
      fd_seterr3(fd_BadEscapeSequence,"parse_unicode_escape",u8_strdup(s));
      return -1;}
  else {
    fd_seterr3(fd_BadEscapeSequence,"parse_unicode_escape",u8_strdup(s));
    return -1;}
}

/* This reads an escape sequence inside of a string.
   It interprets the standard C escape sequences and also
   handles unicode escapes. */
static int read_escape(u8_input in)
{
  int c;
  switch (c=u8_getc(in)) {
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
    u8_string parsed=u8_gets_x(buf,16,in,";",&len);
    if (parsed) {
      int code=-1;
      if (sscanf(buf,"%x",&code)<1) code=-1;
      return code;}
    else {
      fd_seterr3(fd_BadEscapeSequence,"parse_unicode_escape",NULL);
      return -1;}}
  case 'u': {
    char buf[16]; int nc=u8_probec(in), len;
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
    char buf[16]; int code=-1;
    buf[0]='\\'; buf[1]=c; u8_getn(buf+2,2,in); buf[5]='\0';
    if (strlen(buf)==4) {
      if (sscanf(buf+1,"%o",&code)<1) code=-1;
      if (code<0)
        fd_seterr3(fd_BadEscapeSequence,"read_escape",u8_strdup(buf));
      return code;}
    else return -1;}
  case '&': {
    int code=u8_get_entity(in);
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

static u8_string constant_names[]={
  "#T","#F","#TRUE","#FALSE","#VOID","#EOF","#EOD","#EOX","#DFLT","#DEFAULT",
  NULL};
static fdtype constant_values[]={
  FD_TRUE,FD_FALSE,FD_TRUE,FD_FALSE,FD_VOID,FD_EOF,FD_EOD,FD_EOX,
  FD_DEFAULT_VALUE,FD_DEFAULT_VALUE,
  0};

static int copy_atom(u8_input s,u8_output a)
{
  int c=u8_getc(s), vbar=0;
  if (c=='|') {vbar=1; c=u8_getc(s);}
  while ((c>=0) && ((vbar) || (!(atombreakp(c))))) {
    if (c == '|') if (vbar) vbar=0; else vbar=1;
    else if (c == '\\') {
      int realc;
      realc=read_escape(s);
      u8_putc(a,realc);}
    else if (vbar) u8_putc(a,c);
    else {
      int upper=u8_toupper(c);
      u8_putc(a,upper);}
    c=u8_getc(s);}
  if (c>=0) u8_ungetc(s,c);
  return c;
}

fdtype fd_parse_atom(u8_string start,int len)
{
  /* fprintf(stderr,"fd_parse_atom %d: %s\n",len,start); */
  if (FD_EXPECT_FALSE(len==0)) return FD_EOX;
  else if ((start[0]=='#')&&(start[1]=='U')) { /* It's a UUID */
    struct FD_UUID *uuid=u8_alloc(struct FD_UUID);
    FD_INIT_CONS(uuid,fd_uuid_type);
    if (u8_parseuuid(start+2,(u8_uuid)&(uuid->uuid)))
      return FDTYPE_CONS(uuid);
    else {
      fd_seterr3("Invalid UUID","fd_parse_atom",u8_strdup(start));
      return FD_PARSE_ERROR;}}
  else if ((start[0]=='#')&&(start[1]=='T')&&(isdigit(start[2]))) {
    struct U8_XTIME xt;
    int retval=u8_iso8601_to_xtime(start+2,&xt);
    if (retval<0) {
      fd_seterr("Invalid timestamp","fd_parse_atom",u8_strdup(start),FD_VOID);
      return FD_PARSE_ERROR;}
    else return fd_make_timestamp(&xt);}
  else if ((start[0]=='#')&&(start[1]=='!')&&(isxdigit(start[2]))) {
    if (fd_interpret_pointers) {
      unsigned long pval;
      if (sscanf(start+2,"%lx",&pval)!=1)
        return fd_err
          (fd_BadPointerRef,"fd_parse_atom",u8_strdup(start),FD_VOID);
      else if (FD_CHECK_PTR(pval))
        return fd_incref((fdtype)pval);
      else return fd_err
             (fd_BadPointerRef,"fd_parse_atom",u8_strdup(start),FD_VOID);}
    else return fd_err
           (fd_NoPointerExpressions,"fd_parse_atom",
            u8_strdup(start),FD_VOID);}
  else if (start[0]=='#') { /* It's a constant */
    int i=0; while (constant_names[i])
               if (strcmp(start,constant_names[i]) == 0)
                 return constant_values[i];
               else i++;
    i=0; while (i<256)
           if ((fd_constant_names[i])&&
               (strcasecmp(start,fd_constant_names[i])==0))
             return FD_CONSTANT(i);
           else i++;
    if (strchr("XxOoBbEeIiDd",start[1])) {
      fdtype result=_fd_parse_number(start,-1);
      if (!(FD_FALSEP(result))) return result;}
    fd_seterr3(fd_InvalidConstant,"fd_parse_atom",u8_strdup(start));
    return FD_PARSE_ERROR;}
  else {
    fdtype result;
    if ((isdigit(start[0])) || (start[0]=='+') ||
        (start[0]=='-') || (start[0]=='.')) {
      result=_fd_parse_number(start,-1);
      if (!(FD_FALSEP(result))) return result;}
    return fd_make_symbol(start,len);}
}

/* Parsing characters */

static fdtype parse_character(U8_INPUT *in)
{
  char buf[128];
  struct U8_OUTPUT tmpbuf;
  int c, n_chars=0;
  /* First, copy an entire atom. */
  U8_INIT_STATIC_OUTPUT_BUF(tmpbuf,128,buf);
  c=u8_getc(in);
  if ((c=='&')&&(!(atombreakp(*(in->u8_inptr))))) {
    int code=u8_get_entity(in);
    if (code<0)
      if (atombreakp(*(in->u8_inptr)))
        return FD_CODE2CHAR('&');
      else {
        fd_seterr3(fd_BadEscapeSequence,"read_escape",NULL);
        return FD_PARSE_ERROR;}
    else return FD_CODE2CHAR(code);}
  if ((c<128) && (ispunct(c))) {
    return FD_CODE2CHAR(c);}
  while ((c>=0) && (!(atombreakp(c)))) {
    n_chars++; u8_putc(&tmpbuf,c); c=u8_getc(in);}
  if (n_chars==0) return FD_CODE2CHAR(c);
  else u8_ungetc(in,c);
  if (n_chars==1) {
    const u8_byte *scan=buf; int c=u8_sgetc(&scan);
    return FD_CODE2CHAR(c);}
  else if ((tmpbuf.u8_outbuf[0]=='u') || (tmpbuf.u8_outbuf[0]=='U'))
    c=parse_unicode_escape(tmpbuf.u8_outbuf);
  else c=-1;
  if (c>=0) return FD_CODE2CHAR(c);
  else if ((c=u8_entity2code(tmpbuf.u8_outbuf))>=0)
    return FD_CODE2CHAR(c);
  else {
    int i=0; while (character_constant_names[i])
      if (strcasecmp(buf,character_constant_names[i]) == 0)
        return character_constants[i];
      else i++;
    fd_seterr3(fd_InvalidCharacterConstant,
               "parse_character",u8_strdup(tmpbuf.u8_outbuf));
    return FD_PARSE_ERROR;}
}

/* OID parsing */

static fdtype (*oid_parser)(u8_string start,int len)=NULL;

FD_EXPORT
/* fd_set_oid_parser:
     Arguments: a function which takes a UTF8 string and an integer length
     Returns: void
  This sets the default parser used for OIDs.
*/
void fd_set_oid_parser(fdtype (*parsefn)(u8_string start,int len))
  {
  oid_parser=parsefn;
}

typedef unsigned long long ull;

static fdtype default_parse_oid(u8_string start,int len)
{
  u8_byte _buf[64], *buf=_buf;
  FD_OID oid=FD_NULL_OID_INIT;
  unsigned int hi, lo, c=start[len];
  if (len>64) buf=u8_malloc(len+1);
  strncpy(buf,start,len);
  if ((strchr(buf,'/'))>0) {
    int items=sscanf(buf,"@%x/%x",&hi,&lo);
    if (items!=2) {
      if (buf!=_buf) u8_free(buf);
      return FD_PARSE_ERROR;}}
  else {
    unsigned long long addr;
    int items=sscanf(buf,"@%llx",&addr);
    if (items!=1) {
      if (buf!=_buf) u8_free(buf);
      return FD_PARSE_ERROR;}
    hi=((addr>>32)&((ull)0xFFFFFFFF));
    lo=(addr&((ull)0xFFFFFFFF));}
  FD_SET_OID_HI(oid,hi); FD_SET_OID_LO(oid,lo);
  return fd_make_oid(oid);
}

FD_EXPORT fdtype fd_parse_oid_addr(u8_string string,int len)
{
  return default_parse_oid(string,len);
}

static int copy_string(u8_input s,u8_output a);

/* This is the function called from the main parser loop. */
static fdtype parse_oid(U8_INPUT *in)
{
  struct U8_OUTPUT tmpbuf; char buf[128]; int c; fdtype result;
  U8_INIT_STATIC_OUTPUT_BUF(tmpbuf,128,buf);
  /* First, copy the data into a buffer.
     The buffer will almost never grow, but it might
     if we have a really long prefix id. */
  c=copy_atom(in,&tmpbuf);
  if ((c=='"')&&((tmpbuf.u8_outptr-tmpbuf.u8_outbuf)==2)&&
      (buf[0]=='@')&&(ispunct(buf[1]))&&
      (strchr("(){}[]<>",buf[1])==NULL)) {
    copy_string(in,&tmpbuf); c='@';}
  if (tmpbuf.u8_outptr<=tmpbuf.u8_outbuf)
    return FD_EOX;
  else if (oid_parser)
    result=oid_parser(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
  else result=default_parse_oid(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
  if (FD_ABORTP(result)) return result;
  if (strchr("({\"",c)) {
    /* If an object starts immediately after the OID (no whitespace)
       it is the OID's label, so we read it and discard it. */
    fdtype label=fd_parser(in);
    fd_decref(label);}
  if (tmpbuf.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpbuf.u8_outbuf);
  return result;
}

/* String and packet parsing */

static fdtype parse_string(U8_INPUT *in)
{
  fdtype result=FD_VOID; u8_byte buf[256];
  struct U8_OUTPUT out; int c=u8_getc(in);
  U8_INIT_OUTPUT_X(&out,256,buf,0);
  while ((c=u8_getc(in))>=0)
    if (c == '"') break;
    else if (c == '\\') {
      int nextc=u8_getc(in);
      if (nextc=='\n') {
        while (u8_isspace(nextc)) nextc=u8_getc(in);
        u8_putc(&out,nextc);
        continue;}
      else u8_ungetc(in,nextc);
      c=read_escape(in);
      if (c<0) {
        if ((out.u8_streaminfo)&(U8_STREAM_OWNS_BUF))
          u8_free(out.u8_outbuf);
        return FD_PARSE_ERROR;}
      u8_putc(&out,c);}
    else u8_putc(&out,c);
  result=fd_make_string(NULL,u8_outlen(&out),u8_outstring(&out));
  u8_close_output(&out);
  return result;
}

static fdtype parse_packet(U8_INPUT *in)
{
  char *data=u8_malloc(128);
  int max=128, len=0, c=u8_getc(in);
  while ((c>=0) && (c<128) && (c!='"')) {
    if (len>=max) {
      data=u8_realloc(data,max+128);
      max=max+128;}
    if (c == '\\') {
      char obuf[4];
      obuf[0]=c=u8_getc(in);
      if (obuf[0]=='\n') {
        while (u8_isspace(c)) c=u8_getc(in);
        continue;}
      else if (obuf[0]=='\\') {
        data[len++]='\\';
        continue;}
      else if (c=='n') {
        data[len++]='\n';
        continue;}
      else if (c=='t') {
        data[len++]='\t';
        continue;}
      else if (c=='#') {
        data[len++]='#';
        continue;}
      obuf[1]=c=u8_getc(in);
      obuf[2]='\0';
      if (c<0) {
        u8_free(data);
        return FD_EOX;}
      else if ((isxdigit(obuf[0])) && (isxdigit(obuf[1]))) {
        c=strtol(obuf,NULL,16);
        if (c<256) data[len++]=c; else break;}
      else break;}
    else data[len++]=c;
    c=u8_getc(in);}
  if (c=='"')
    return fd_bytes2packet(NULL,len,data);
  else {
    u8_free(data);
    if (c<0) return FD_EOX;
    else return FD_PARSE_ERROR;}
}

fdtype (*fd_regex_parser)(u8_string src,u8_string opts)=NULL;

static fdtype parse_regex(U8_INPUT *in)
{
  fdtype result; struct U8_OUTPUT src; u8_byte buf[128];
  u8_byte opts[16], *optwrite=opts;
  int c=u8_getc(in); U8_INIT_OUTPUT_BUF(&src,128,buf);
  while (c>=0) {
    if (c!='/') u8_putc(&src,c);
    else {
      int mc=u8_getc(in);
      while ((mc<128)&&(u8_isalpha(mc))) {
        if (strchr("eils",mc)) *optwrite++=(char)mc;
        else {
          fd_seterr(fd_ParseError,"parse_regex",src.u8_outbuf,FD_VOID);
          return FD_PARSE_ERROR;}
        mc=u8_getc(in);}
      u8_ungetc(in,mc);
      *optwrite++='\0';
      if (fd_regex_parser)
        result=fd_regex_parser(src.u8_outbuf,opts);
      else result=fd_make_nvector(3,fd_intern("NOREGEX"),
                                  fdtype_string(src.u8_outbuf),
                                  fdtype_string(opts));
      u8_close((u8_stream)&src);
      return result;}
    c=u8_getc(in);}
  return FD_EOF;
}

static fdtype parse_hex_packet(U8_INPUT *in)
{
  char *data=u8_malloc(128);
  int max=128, len=0, c=u8_getc(in);
  while (isxdigit(c)) {
    int nc=u8_getc(in), byte=0; char xbuf[3];
    if (!(isxdigit(nc))) break;
    if (len>=max) {
      data=u8_realloc(data,max+128);
      max=max+128;}
    xbuf[0]=c; xbuf[1]=nc; xbuf[2]='\0';
    byte=strtol(xbuf,NULL,16);
    data[len++]=byte;
    c=u8_getc(in);}
  if (c=='"')
    return fd_bytes2packet(NULL,len,data);
  u8_free(data);
  if (c<0) return FD_EOX;
  else return FD_PARSE_ERROR;
}

static int copy_string(u8_input s,u8_output a)
{
  int c=u8_getc(s);
  if (c!='"') return c;
  u8_putc(a,c);
  while ((c=u8_getc(s))>=0) {
      if (c == '"') {
        u8_putc(a,c);
        return c;}
      else if (c == '\\') {
        int nextc=u8_getc(s);
        if (nextc=='\n') {
          while (u8_isspace(nextc)) nextc=u8_getc(s);
          u8_putc(a,nextc);
          continue;}
        else u8_ungetc(s,nextc);
        c=read_escape(s);
        if (c<0) return c;
        u8_putc(a,c);}
      else u8_putc(a,c);}
  return c;
}

/* Compound object parsing */

#define iscloser(c) (((c)==')') || ((c)==']') || ((c)=='}'))

static fdtype *parse_vec(u8_input in,char end_char,int *size)
{
  fdtype *elts=u8_alloc_n(32,fdtype);
  unsigned int n_elts=0, max_elts=32;
  int ch=skip_whitespace(in);
  while ((ch>=0) && (ch != end_char) && (!(iscloser(ch)))) {
    fdtype elt=fd_parser(in);
    if (FD_ABORTP(elt)) {
      int i=0; while (i < n_elts) {
        fd_decref(elts[i]); i++;}
      u8_free(elts);
      if (elt == FD_EOX) *size=-1;
      else if (elt == FD_PARSE_ERROR) *size=-2;
      else *size=-3;
      if (FD_ABORTP(elt)) fd_interr(elt);
      return NULL;}
    else if (n_elts == max_elts) {
      fdtype *new_elts=u8_realloc_n(elts,max_elts*2,fdtype);
      if (new_elts) {elts=new_elts; max_elts=max_elts*2;}
      else {
        int i=0; while (i < n_elts) {fd_decref(elts[i]); i++;}
        u8_free(elts); *size=n_elts;
        return NULL;}}
    elts[n_elts++]=elt;
    ch=skip_whitespace(in);}
  if (ch == end_char) {
    *size=n_elts;
    ch=u8_getc(in); /* Skip the end char */
    if (n_elts) return elts;
    else {
      u8_free(elts);
      return NULL;}}
  else {
    int i=0; while (i < n_elts) {fd_decref(elts[i]); i++;}
    u8_free(elts);
    if (ch<0) {
      *size=ch;
      fd_seterr(fd_UnexpectedEOF,"parse_vec",NULL,FD_VOID);}
    else {
      *size=-1;
      fd_seterr(fd_MismatchedClose,"parse_vec",NULL,FD_CODE2CHAR(end_char));}
    return NULL;}
}

static fdtype parse_list(U8_INPUT *in)
{
  /* This starts parsing the list after a '(' has been read. */
  int ch=skip_whitespace(in); fdtype head=FD_VOID;
  if (ch<0)
    if (ch==-1) return FD_EOX;
    else return FD_PARSE_ERROR;
  else if (ch == ')') {
    /* The empty list case */
    u8_getc(in); return FD_EMPTY_LIST;}
  else if (ch == ']') {
    fd_seterr(fd_MismatchedClose,"parse_list",NULL,head);
    return FD_PARSE_ERROR;}
  else {
    /* This is where we build the list.  We recur in the CAR direction and
       iterate in the CDR direction to avoid growing the stack. */
    struct FD_PAIR *scan;
    fdtype car=fd_parser(in), head;
    if (FD_ABORTP(car))
      return car;
    else {
      scan=u8_alloc(struct FD_PAIR);
      FD_INIT_CONS(scan,fd_pair_type);
      if (scan == NULL) {fd_decref(car); return FD_OOM;}
      else head=fd_init_pair(scan,car,FD_EMPTY_LIST);}
    ch=skip_whitespace(in);
    while ((ch>=0) && (ch != ')')) {
      /* After starting with the head, we iterate until we get to
         the closing paren, except for the dotted pair exit clause. */
      fdtype list_elt; struct FD_PAIR *new_pair;
      if (ch == '.') {
        int nextch=u8_getc(in), probed=u8_probec(in);
        if (u8_isspace(probed)) break;
        else u8_ungetc(in,nextch);}
      list_elt=fd_parser(in);
      if (FD_ABORTP(list_elt)) {
        fd_decref(head); return list_elt;}
      new_pair=u8_alloc(struct FD_PAIR);
      if (new_pair) {
        scan->cdr=fd_init_pair(new_pair,list_elt,FD_EMPTY_LIST);
        scan=new_pair;}
      else {
        fd_decref(head); fd_decref(list_elt);
        return FD_OOM;}
      ch=skip_whitespace(in);}
    if (ch<0) {
      fd_decref(head);
      if (ch==-1) return FD_EOX;
      else return FD_PARSE_ERROR;}
    else if (ch == ')') {
      u8_getc(in);
      return head;}
    else {
      fdtype tail;
      tail=fd_parser(in);
      if (FD_ABORTP(tail)) {
        fd_decref(head); return tail;}
      skip_whitespace(in); ch=u8_getc(in);
      if (ch == ')') {scan->cdr=tail; return head;}
      fd_decref(head); fd_decref(tail);
      return FD_PARSE_ERROR;}}
}

static fdtype parse_bracket_list(U8_INPUT *in)
{
  /* This starts parsing the list after a '(' has been read. */
  int ch=skip_whitespace(in); fdtype head=FD_VOID;
  if (ch<0)
    if (ch==-1) return FD_EOX;
    else return FD_PARSE_ERROR;
  else if (ch == ']') {
    /* The empty list case */
    u8_getc(in); return FD_EMPTY_LIST;}
  else if (ch == ')') {
    fd_seterr(fd_MismatchedClose,"parse_bracket_list",NULL,head);
    return FD_PARSE_ERROR;}
  else {
    /* This is where we build the list.  We recur in the CAR direction and
       iterate in the CDR direction to avoid growing the stack. */
    struct FD_PAIR *scan;
    fdtype car=fd_parser(in), head;
    if (FD_ABORTP(car))
      return car;
    else {
      scan=u8_alloc(struct FD_PAIR);
      FD_INIT_CONS(scan,fd_pair_type);
      if (scan == NULL) {fd_decref(car); return FD_OOM;}
      else head=fd_init_pair(scan,car,FD_EMPTY_LIST);}
    ch=skip_whitespace(in);
    while ((ch>=0) && (ch != ']')) {
      /* After starting with the head, we iterate until we get to
         the closing paren, except for the dotted pair exit clause. */
      fdtype list_elt; struct FD_PAIR *new_pair;
      if (ch == '.') {
        int nextch=u8_getc(in), probed=u8_probec(in);
        if (u8_isspace(probed)) break;
        else u8_ungetc(in,nextch);}
      list_elt=fd_parser(in);
      if (FD_ABORTP(list_elt)) {
        fd_decref(head); return list_elt;}
      new_pair=u8_alloc(struct FD_PAIR);
      if (new_pair) {
        scan->cdr=fd_init_pair(new_pair,list_elt,FD_EMPTY_LIST);
        scan=new_pair;}
      else {
        fd_decref(head); fd_decref(list_elt);
        return FD_OOM;}
      ch=skip_whitespace(in);}
    if (ch<0) {
      fd_decref(head);
      if (ch==-1) return FD_EOX;
      else return FD_PARSE_ERROR;}
    else if (ch == ']') {
      u8_getc(in);
      return head;}
    else {
      fdtype tail;
      tail=fd_parser(in);
      if (FD_ABORTP(tail)) {
        fd_decref(head); return tail;}
      skip_whitespace(in); ch=u8_getc(in);
      if (ch == ')') {scan->cdr=tail; return head;}
      fd_decref(head); fd_decref(tail);
      return FD_PARSE_ERROR;}}
}

static fdtype parse_vector(U8_INPUT *in)
{
  int n_elts=-2;
  fdtype *elts=parse_vec(in,')',&n_elts);
  if (n_elts>=0)
    return fd_init_vector(u8_alloc(struct FD_VECTOR),n_elts,elts);
  else return FD_PARSE_ERROR;
}

static fdtype parse_rail(U8_INPUT *in)
{
  int n_elts=-2;
  fdtype *elts=parse_vec(in,')',&n_elts);
  if (n_elts>=0)
    return fd_init_rail(u8_alloc(struct FD_VECTOR),n_elts,elts);
  else return FD_PARSE_ERROR;
}

static fdtype parse_slotmap(U8_INPUT *in)
{
  int n_elts=-2;
  fdtype *elts=parse_vec(in,']',&n_elts);
  if (FD_EXPECT_FALSE(n_elts<0)) return FD_PARSE_ERROR;
  else if (n_elts>7)
    return fd_init_slotmap(NULL,n_elts/2,(struct FD_KEYVAL *)elts);
  else {
    fdtype result=fd_make_slotmap(n_elts/2,n_elts/2,(struct FD_KEYVAL *)elts);
    u8_free(elts);
    return result;}
}

static fdtype parse_choice(U8_INPUT *in)
{
  int ch=skip_whitespace(in);
  if (ch == '}') {
    u8_getc(in); return FD_EMPTY_CHOICE;}
  else if (ch < 0)
    if (ch==-1) return FD_EOX; else return FD_PARSE_ERROR;
  else {
    int n_elts=-2; fdtype *elts=parse_vec(in,'}',&n_elts);
    if (n_elts==0) return FD_EMPTY_CHOICE;
    else if (elts==NULL)
      return FD_PARSE_ERROR;
    else if (n_elts==1) {
      fdtype v=elts[0]; u8_free(elts);
      return v;}
    else if ((elts) && (n_elts>0)) {
      struct FD_CHOICE *ch=fd_alloc_choice(n_elts);
      fdtype result=fd_init_choice(ch,n_elts,elts,FD_CHOICE_DOSORT);
      if (FD_XCHOICE_SIZE(ch)==1) {
        result=FD_XCHOICE_DATA(ch)[0];
        u8_free(ch);}
      u8_free(elts);
      return result;}
    else return FD_PARSE_ERROR;}
}

static fdtype parse_qchoice(U8_INPUT *in)
{
  int n_elts=-2;
  fdtype *elts=parse_vec(in,'}',&n_elts);
  if (n_elts==0)
    return fd_init_qchoice(u8_alloc(struct FD_QCHOICE),
                           FD_EMPTY_CHOICE);
  else if (n_elts==1) {
    fdtype result=elts[0]; u8_free(elts);
    return result;}
  else if (n_elts>1) {
    struct FD_CHOICE *xch=fd_alloc_choice(n_elts);
    fdtype choice=fd_init_choice(xch,n_elts,elts,FD_CHOICE_DOSORT);
    if (FD_XCHOICE_SIZE(xch)==1) {
      fdtype result=FD_XCHOICE_DATA(xch)[0];
      u8_free(elts); return result;}
    u8_free(elts);
    return fd_init_qchoice(u8_alloc(struct FD_QCHOICE),choice);}
  else if (n_elts==-1) return FD_EOX;
  else return FD_PARSE_ERROR;
}

/* Record parsing */

static fdtype recreate_record(int n,fdtype *v)
{
  int i=0;
  struct FD_COMPOUND_ENTRY *entry=fd_lookup_compound(v[0]);
  if ((entry) && (entry->parser)) {
    fdtype result=entry->parser(n,v,entry);
    if (!(FD_VOIDP(result))) {
      while (i<n) {fd_decref(v[i]); i++;}
      if (v) u8_free(v);
      return result;}}
  {
    struct FD_COMPOUND *c=
      u8_malloc(sizeof(struct FD_COMPOUND)+(n-1)*sizeof(fdtype));
    fdtype *data=&(c->elt0); fd_init_compound(c,v[0],0,0);
    c->n_elts=n-1;
    i=1; while (i<n) {data[i-1]=v[i]; i++;}
    if (v) u8_free(v);
    return FDTYPE_CONS(c);}
}

static fdtype parse_record(U8_INPUT *in)
{
  int n_elts;
  fdtype *elts=parse_vec(in,')',&n_elts);
  if (n_elts>0)
    return recreate_record(n_elts,elts);
  else if (n_elts==0)
    return fd_err(fd_CantParseRecord,"parse_record","empty record",FD_VOID);
  else if (n_elts==-1) return FD_EOX;
  else return FD_PARSE_ERROR;
}

static fdtype get_compound_tag(fdtype tag)
{
  if (FD_COMPOUND_TYPEP(tag,fd_compound_descriptor_type)) {
    struct FD_COMPOUND *c=FD_XCOMPOUND(tag);
    return fd_incref(c->elt0);}
  else return tag;
}

static int unparse_compound(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_COMPOUND *xc=FD_GET_CONS(x,fd_compound_type,struct FD_COMPOUND *);
  fdtype tag=get_compound_tag(xc->tag);
  struct FD_COMPOUND_ENTRY *entry=fd_lookup_compound(tag);
  if ((entry) && (entry->unparser)) {
    int retval=entry->unparser(out,x,entry);
    if (retval<0) return retval;
    else if (retval) return retval;}
  {
    fdtype *data=&(xc->elt0);
    int i=0, n=xc->n_elts;
    if ((entry)&&(entry->core_slots>0)&&(entry->core_slots<n))
      n=entry->core_slots;
    u8_printf(out,"#%%(%q",xc->tag);
    while (i<n) {
      u8_printf(out," %q",data[i]); i++;}
    u8_printf(out,")");
    return 1;}
}

/* The main parser procedure */

static fdtype parse_atom(u8_input in,int ch1,int ch2);

FD_EXPORT
/* fd_parser:
     Arguments: a U8 input stream and a memory pool
     Returns: a lisp object

     Parses a textual object representation from a stream into a lisp object.
*/
fdtype fd_parser(u8_input in)
{
  int inchar=skip_whitespace(in);
  if (inchar<0) {
    if (inchar==-1) return FD_EOX;
    else return FD_PARSE_ERROR;}
  switch (inchar) {
  case ')': case ']': case '}': {
    u8_getc(in); /* Consume the character */
    return fd_err(fd_ParseError,"unexpected terminator",
                  u8_strndup(in->u8_inptr,17),
                  FD_CODE2CHAR(inchar));}
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
    fdtype content;
    u8_getc(in); /* Skip the quote mark */
    content=fd_parser(in);
    if (FD_ABORTP(content)) return content;
    else return fd_make_list(2,quote_symbol,content);}
  case '`': {
    fdtype content;
    u8_getc(in); /* Skip the quote mark */
    content=fd_parser(in);
    if (FD_ABORTP(content)) return content;
    else return fd_make_list(2,quasiquote_symbol,content);}
  case ',': {
    fdtype content; int c=u8_getc(in); c=u8_getc(in);
    /* Skip the quote mark and check for an atsign. */
    if (c != '@') u8_ungetc(in,c);
    content=fd_parser(in);
    if (FD_ABORTP(content)) return content;
    else if (c == '@')
      return fd_make_list(2,unquotestar_symbol,content);
    else return fd_make_list(2,unquote_symbol,content);}
  case '|': { /* Escaped symbol */
    struct U8_OUTPUT tmpbuf; char buf[128];
    fdtype result; MAYBE_UNUSED int c;
    U8_INIT_STATIC_OUTPUT_BUF(tmpbuf,128,buf);
    c=copy_atom(in,&tmpbuf);
    result=fd_make_symbol(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
    if (tmpbuf.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpbuf.u8_outbuf);
    return result;}
  case '#': {
    /* Absorb the # and set ch to the next character, dispatching on
       that */
    int ch=u8_getc(in); ch=u8_getc(in);
    switch (ch) {
    case '(': return parse_vector(in);
    case '~': {
      ch=u8_getc(in);
      if (ch!='(') return fd_err(fd_ParseError,"fd_parser",NULL,FD_VOID);
      return parse_rail(in);}
    case '{': return parse_qchoice(in);
    case '[': return parse_slotmap(in);
    case '*': {
      int nextc=u8_getc(in); int hex=0; fdtype result;
      if (nextc=='X') {hex=1; nextc=u8_getc(in);}
      if (nextc!='"') return fd_err(fd_ParseError,"fd_parser",NULL,FD_VOID);
      if (hex)
        result=parse_hex_packet(in);
      else result=parse_packet(in);
      FD_SET_CONS_TYPE(result,fd_secret_type);
      return result;}
    case 'X': {
      int nextc=u8_getc(in); fdtype result;
      if (nextc!='"') {
        fd_seterr(fd_ParseError,"fd_parser","invalid hex packet",
                  FD_VOID);
        return FD_PARSE_ERROR;}
      result=parse_hex_packet(in);
      return result;}
    case '"': return parse_packet(in);
    case '/': return parse_regex(in);
    case '<':
      return fd_err(fd_ParseError,"fd_parser",NULL,FD_VOID);
    case ';': {
      fdtype content=fd_parser(in);
      if (FD_ABORTP(content)) return content;
      else return fd_conspair(comment_symbol,
                              fd_conspair(content,FD_EMPTY_LIST));}
    case '%': {
      int c=u8_getc(in);
      if (c=='(') return parse_record(in);
      else return FD_PARSE_ERROR;}
    case '\\': return parse_character(in);
    case '#': return fd_make_list(2,histref_symbol,fd_parser(in));
    case 'U': return parse_atom(in,inchar,ch); /* UUID */
    case 'T': return parse_atom(in,inchar,ch); /* TIMESTAMP */
    case '!': return parse_atom(in,inchar,ch); /* pointer reference */
    default:
      if (u8_ispunct(ch)) {
        u8_byte buf[16]; struct U8_OUTPUT out; int nch;
        U8_INIT_OUTPUT_X(&out,16,buf,U8_FIXED_STREAM);
        u8_putc(&out,'#'); u8_putc(&out,ch); nch=u8_getc(in);
        while (u8_ispunct(nch)) {
          u8_putc(&out,nch);
          if ((out.u8_outptr-out.u8_outbuf)>11) {
            fd_seterr(fd_ParseError,"fd_parser","invalid hash # prefix",
                      fd_stream2string(&out));
            return FD_PARSE_ERROR;}
          else nch=u8_getc(in);}
        u8_ungetc(in,nch);
        return fd_make_list(2,fd_intern(buf),fd_parser(in));}
      else {
        u8_ungetc(in,ch);
        return parse_atom(in,'#',-1);}}}
  default:
    return parse_atom(in,-1,-1);}
}

static fdtype parse_atom(u8_input in,int ch1,int ch2)
{
  /* Parse an atom, i.e. a printed representation which doesn't
     contain any special spaces or other special characters */
  struct U8_OUTPUT tmpbuf; char buf[128];
  fdtype result; MAYBE_UNUSED int c;
  U8_INIT_STATIC_OUTPUT_BUF(tmpbuf,128,buf);
  if (ch1>=0) u8_putc(&tmpbuf,ch1);
  if (ch2>=0) u8_putc(&tmpbuf,ch2);
  c=copy_atom(in,&tmpbuf);
  if (tmpbuf.u8_outptr==tmpbuf.u8_outbuf) result=FD_EOX;
  else if (ch1 == '|')
    result=fd_make_symbol(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
  else result=fd_parse_atom(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
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
fdtype fd_parse_expr(u8_input in)
{
  int inchar=skip_whitespace(in);
  if (inchar<0)
    return FD_EOF;
  else return fd_parser(in);
}

FD_EXPORT
/* fd_parser:
     Arguments: a string
     Returns: a lisp object

Parses a textual object representation into a lisp object. */
fdtype fd_parse(u8_string s)
{
  struct U8_INPUT stream;
  U8_INIT_STRING_INPUT((&stream),-1,s);
  return fd_parser(&stream);
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
fdtype fd_parse_arg(u8_string arg)
{
  if (*arg=='\0') return fdtype_string(arg);
  else if (*arg == ':')
    if (arg[1]=='\0') return fdtype_string(arg);
    else {
      fdtype val=fd_parse(arg+1);
      if (FD_ABORTP(val)) {
        u8_log(LOG_WARN,fd_ParseArgError,"Bad colon spec arg '%s'",arg);
        fd_clear_errors(1);
        return fdtype_string(arg);}
      else return val;}
  else if (*arg == '\\') return fdtype_string(arg+1);
  else if ((isdigit(arg[0])) ||
           ((strchr("+-.",arg[0])) && (isdigit(arg[1]))) ||
           ((arg[0]=='#') && (strchr("OoXxDdBbIiEe",arg[1])))) {
    fdtype num=fd_string2number(arg,-1);
    if (FD_NUMBERP(num)) return num;
    else return fdtype_string(arg);}
  else if (strchr("@{#(\"|",arg[0])) {
    fdtype result;
    struct U8_INPUT stream;
    U8_INIT_STRING_INPUT((&stream),-1,arg);
    result=fd_parser(&stream);
    if (fd_skip_whitespace(&stream)>0) {
      fd_decref(result);
      return fdtype_string(arg);}
    else return result;}
  else return fdtype_string(arg);
}

FD_EXPORT
/* fd_unparse_arg:
     Arguments: a lisp object
     Returns: a utf-8 string

     Generates a string representation from a lisp object, trying
     to make the representation as natural as possible but allowing
     it to be reversed by fd_parse_arg
*/
u8_string fd_unparse_arg(fdtype arg)
{
  struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,32);
  if (FD_STRINGP(arg)) {
    u8_string string=FD_STRDATA(arg);
    if ((strchr("@{#(\"",string[0])) || (isdigit(string[0])))
      u8_putc(&out,'\\');
    u8_puts(&out,string);}
  else if (FD_OIDP(arg)) {
    FD_OID addr=FD_OID_ADDR(arg);
    u8_printf(&out,"@%x/%x",FD_OID_HI(addr),FD_OID_LO(addr));}
  else if (FD_NUMBERP(arg)) fd_unparse(&out,arg);
  else {
    u8_putc(&out,':'); fd_unparse(&out,arg);}
  return out.u8_outbuf;
}

/* U8_PRINTF extensions */

static u8_string lisp_printf_handler
  (struct U8_OUTPUT *s,char *cmd,u8_byte *buf,int bufsiz,va_list *args)
{
  fdtype value=va_arg(*args,fdtype);
  int taciturn=(strchr(cmd,'h')!=NULL), retval;
  int already=(s->u8_streaminfo)&(U8_STREAM_TACITURN);
  if (taciturn) s->u8_streaminfo=(s->u8_streaminfo)|U8_STREAM_TACITURN;
  else s->u8_streaminfo=(s->u8_streaminfo)&(~U8_STREAM_TACITURN);
  retval=fd_unparse(s,value);
  if (already)
    s->u8_streaminfo=s->u8_streaminfo|U8_STREAM_TACITURN;
  else s->u8_streaminfo=s->u8_streaminfo&(~U8_STREAM_TACITURN);
  if (retval<0) fd_clear_errors(1);
  if (strchr(cmd,'-')) fd_decref(value);
  return NULL;
}

FD_EXPORT void fd_init_textio_c()
{
  u8_register_source_file(_FILEINFO);

  u8_printf_handlers['q']=lisp_printf_handler;

  fd_unparsers[fd_compound_type]=unparse_compound;
  fd_unparsers[fd_string_type]=unparse_string;
  fd_unparsers[fd_packet_type]=unparse_packet;
  fd_unparsers[fd_secret_type]=unparse_secret;
  fd_unparsers[fd_vector_type]=unparse_vector;
  fd_unparsers[fd_rail_type]=unparse_rail;
  fd_unparsers[fd_pair_type]=unparse_pair;
  fd_unparsers[fd_choice_type]=unparse_choice;

  quote_symbol=fd_intern("QUOTE");
  quasiquote_symbol=fd_intern("QUASIQUOTE");
  unquote_symbol=fd_intern("UNQUOTE");
  unquotestar_symbol=fd_intern("UNQUOTE*");
  histref_symbol=fd_intern("%HISTREF");
  comment_symbol=fd_intern("COMMENT");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "if test -f ../../makefile; then cd ../..; make debug; fi;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
