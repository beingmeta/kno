/* -*- Mode: C; -*- */

/* Copyright (C) 2004-2006 beingmeta, inc.
   This file is part of beingmeta's FDB platform and is copyright 
   and a valuable trade secret of beingmeta, inc.

   This file implements the core parser and printer (unparser) functionality.
*/

static char versionid[] =
  "$Id$";

#define U8_INLINE_IO 1
#include "fdb/dtype.h"

#include <ctype.h>
#include <errno.h>
/* We include this for sscanf, but we're not using the FILE functions */
#include <stdio.h>

/* Common macros, functions and declarations */

#define PARSE_ERRORP(x) ((x==FD_EOX) || (x==FD_PARSE_ERROR) || (x==FD_OOM))

#define isoctaldigit(c) ((c<'8') && (isdigit(c)))
#define spacecharp(c) ((c>0) && (c<128) && (isspace(c)))
#define atombreakp(c) \
  ((c<=0) || ((c<128) && ((isspace(c)) || (strchr("{}()[]#\"',`",c)))))

fd_exception fd_CantOpenFile=_("Can't open file");
fd_exception fd_CantFindFile=_("Can't find file");
fd_exception fd_BadEscapeSequence=_("Invalid escape sequence");
fd_exception fd_InvalidConstant=_("Invalid constant reference");
fd_exception fd_InvalidCharacterConstant=_("Invalid character constant");
fd_exception fd_BadAtom=_("Bad atomic expression");
fd_exception fd_NoPointerExpressions=_("no pointer expressions allowed");
fd_exception fd_BadPointerRef=_("bad pointer reference");
fd_exception fd_ParseError=_("LISP expression parse error");
fd_exception fd_CantUnparse=_("LISP expression unparse error");
fd_exception fd_CantParseRecord=_("Can't parse record object");

int fd_unparse_maxelts=0;
int fd_unparse_maxchars=0;

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

static int n_const_names=17;
static u8_string const_names[]={
  "#?","#f","#t","{}","()","#eof","#eod","#eox",
  "#baddtype","#badparse","#oom","#typeerror","#rangeerror",
  "#exception","#error","#lockholder","#unbound",NULL};
  
static int n_character_constants=14;
static u8_string character_constant_names[]={
  "SPACE","NEWLINE","RETURN",
  "TAB","VTAB","VERTICALTAB",
  "PAGE","BACKSPACE",
  "ATTENTION","BELL","DING",
  "SLASH","BACKSLASH","SEMICOLON",
  NULL};
static fdtype character_constants[]={
  FD_CODE2CHAR(' '),FD_CODE2CHAR('\n'),FD_CODE2CHAR('\r'),
  FD_CODE2CHAR('\t'),FD_CODE2CHAR('\v'),FD_CODE2CHAR('\v'),
  FD_CODE2CHAR('\f'),FD_CODE2CHAR('\b'),
  FD_CODE2CHAR('\a'),FD_CODE2CHAR('\a'),FD_CODE2CHAR('\a'),
  FD_CODE2CHAR('/'),FD_CODE2CHAR('\\'),FD_CODE2CHAR(';'),
  0};

/* Unparsing */

static int emit_symbol_name(U8_OUTPUT *out,u8_string name)
{
  u8_byte *scan=name;
  int c=u8_sgetc(&scan), needs_protection=0;
  while (c>=0) 
    if ((atombreakp(c)) ||
	((u8_islower(c)) && ((u8_toupper(c))!=c))) {
      needs_protection=1; break;}
    else c=u8_sgetc(&scan);
  if (needs_protection==0) u8_puts(out,name);
  else {
    u8_byte *start=name, *scan=start;
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
  if (n>=0)
    if (unit) u8_printf(out,"%d%s",n,unit);
    else u8_printf(out,"%d",n);
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
	   (!(iscntrl(*scan)))) {
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
  unsigned char *bytes=s->bytes; int i=0, len=s->length;
  u8_puts(out,"#\"");
  while (i < len) {
    int byte=bytes[i++]; char buf[16]; 
    if ((fd_unparse_maxchars>0) && (i>=fd_unparse_maxchars)) {
      u8_putc(out,' '); output_ellipsis(out,len-i,"bytes");
      return u8_putc(out,'"');}
    if ((byte>0x80) || (byte<0x20) ||
	(byte == '\\') || (byte == '"')) {
      sprintf(buf,"\\%02x",byte);
      u8_puts(out,buf);}
    else {
      buf[0]=byte; buf[1]='\0';
      u8_puts(out,buf);}}
  /* We don't check for an error value until we get here, which means that
     many of the calls above might have failed, however this shouldn't
     be a functional problem. */
  return u8_puts(out,"\"");
}

static int unparse_compound(U8_OUTPUT *out,fdtype x)
{
  struct FD_COMPOUND *c=(struct FD_COMPOUND *)x;
  u8_printf(out,"#<%q %q>",c->tag,c->data);
  return 1;
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
      if (data<n_const_names)
	return u8_puts(out,const_names[data]);
      else {
	char buf[24]; sprintf(buf,"#!%x",x);
	return u8_puts(out,buf);}
    else if (fd_unparsers[itype])
      return fd_unparsers[itype](out,x);
    else {
      char buf[24]; sprintf(buf,"#!%x",x);
      return u8_puts(out,buf);}}
  case fd_cons_ptr_type: {/* output cons */
    struct FD_CONS *cons=FD_CONS_DATA(x);
    fd_ptr_type ct=FD_CONS_TYPE(cons);
    if ((ct < FD_TYPE_MAX) && (fd_unparsers[ct]) && (fd_unparsers[ct](out,x)))
      return 1;
    else if (fd_unparse_error) 
      return fd_unparse_error(out,x,_("no handler"));
    else {
      char buf[128]; int retval;
      sprintf(buf,"#!%lx",x);
      retval=u8_puts(out,buf);
      sprintf(buf,"#!%lx (type=%d)",x,ct);
      u8_warn(fd_CantUnparse,buf);
      return retval;}}
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
  if (s[0] == 'u')
    if ((strlen(s)==5) &&
	(isxdigit(s[1])) && (isxdigit(s[2])) &&
	(isxdigit(s[3])) && (isxdigit(s[4]))) {
      int code=-1;
      if (sscanf(s+1,"%4x",&code)<1) code=-1;
      return code;}
    else {
      fd_seterr3(fd_BadEscapeSequence,"parse_unicode_escape",u8_strdup(s));
      return -1;}
  else if (s[0] == 'U') 
    if ((strlen(s)==9) &&
	(isxdigit(s[1])) && (isxdigit(s[2])) &&
	(isxdigit(s[3])) && (isxdigit(s[4])) &&
	(isxdigit(s[5])) && (isxdigit(s[6])) &&
	(isxdigit(s[7])) && (isxdigit(s[8]))) {
      int code=-1;
      if (sscanf(s+1,"%8x",&code)<1) code=-1;
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
  case 'u': {
    char buf[16]; int code=-1;
    buf[0]='\\'; buf[1]='u'; u8_getn(buf+2,4,in);
    return parse_unicode_escape(buf);}
  case 'U': {
    char buf[16]; int code=-1;
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

/* Atom parsing */

static int n_constants=6;
static u8_string constant_names[]={
  "#T","#F","#TRUE","#FALSE","#VOID","#EOF",
  NULL};
static fdtype constant_values[]={
  FD_TRUE,FD_FALSE,FD_TRUE,FD_FALSE,FD_VOID,FD_EOF,
  0};

static int skip_atom(u8_input s)
{
  int c=u8_getc(s), vbar=0;
  while ((c>=0) && ((vbar) || (!(atombreakp(c))))) {
    if (c == '|') if (vbar) vbar=0; else vbar=1;
    else if (c == '\\') c=u8_getc(s);
    c=u8_getc(s);}
  if (c<-1) return c;
  u8_ungetc(s,c);
  return c;
}
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
  char buf[64]; int i=0;
  /* fprintf(stderr,"fd_parse_atom %d: %s\n",len,start); */
  if (FD_EXPECT_FALSE(len==0)) return FD_EOX;
  else if (start[0]=='#') { /* It's a constant */
    int i=0; while (constant_names[i])
      if (strcmp(start,constant_names[i]) == 0)
	return constant_values[i];
      else i++;
    if (start[1] == '!')
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
	      u8_strdup(start),FD_VOID);
    fd_seterr3(fd_InvalidConstant,"fd_parse_atom",u8_strdup(start));
    return FD_PARSE_ERROR;}
  else {
    fdtype result;
    if ((isdigit(start[0])) || (start[0]=='+') ||
	(start[0]=='-') || (start[0]=='.')) {
      result=_fd_parse_number(start);
      if (!(FD_VOIDP(result))) return result;}
    return fd_make_symbol(start,len);}
}

/* Parsing characters */

static fdtype parse_character(U8_INPUT *in)
{
  char buf[128]; u8_byte *scan;
  struct U8_OUTPUT tmpbuf;
  int c, n_chars=0;
  /* First, copy an entire atom. */
  U8_INIT_OUTPUT_BUF(&tmpbuf,128,buf);
  c=u8_getc(in);
  if (c=='&') {
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
    u8_byte *scan=buf; int c=u8_sgetc(&scan);
    return FD_CODE2CHAR(c);}
  else if ((tmpbuf.u8_outbuf[0]=='u') || (tmpbuf.u8_outbuf[0]=='U')) 
    c=parse_unicode_escape(tmpbuf.u8_outbuf);
  else c=-1;
  if (c>=0)
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

static fdtype default_parse_oid(u8_string start,int len)
{
  FD_OID oid; unsigned int hi, lo, c=start[len];
  start[len]='\0'; sscanf(start,"@%x/%x",&hi,&lo); start[len]=c;
  FD_SET_OID_HI(oid,hi); FD_SET_OID_LO(oid,lo);
  return fd_make_oid(oid);
}

/* This is the function called from the main parser loop. */
static fdtype parse_oid(U8_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{ 
  struct U8_OUTPUT tmpbuf; char buf[128]; int c; fdtype result;
  U8_INIT_OUTPUT_BUF(&tmpbuf,128,buf);
  /* First, copy the data into a buffer.
     The buffer will almost never grow, but it might
     if we have a really long prefix id. */
  c=copy_atom(in,&tmpbuf);
  if (tmpbuf.u8_outptr<tmpbuf.u8_outbuf)
    return FD_EOX;
  else if (oid_parser)
    result=oid_parser(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
  else result=default_parse_oid(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
  if (strchr("({\"",c)) {
    /* If an object starts immediately after the OID (no whitespace)
       it is the OID's label, so we read it and discard it. */
    fdtype label=fd_parser(in,p);
    fd_decref(label);}
  if (tmpbuf.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpbuf.u8_outbuf);
  return result;
}

/* String and packet parsing */

static fdtype parse_string(U8_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
    struct U8_OUTPUT out; int c=u8_getc(in);
    U8_INIT_OUTPUT(&out,16);
    while ((c=u8_getc(in))>=0)
      if (c == '"') break;
      else if (c == '\\') {
	c=read_escape(in);
	if (c<0) {
	  u8_pfree(out.mpool,out.u8_outbuf);
	  return FD_PARSE_ERROR;}
	u8_putc(&out,c);}
      else u8_putc(&out,c);
    if (p)
      return fd_init_string(u8_pmalloc(p,sizeof(struct FD_STRING)),
			    u8_outlen(&out),u8_outstring(&out));
    else return fd_init_string(NULL,u8_outlen(&out),u8_outstring(&out));
}

static fdtype parse_packet(U8_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
  char *data=u8_pmalloc(p,128);
  int max=128, len=0, c=u8_getc(in);
  while ((c>=0) && (c<128) &&
	 ((isalnum(c)) || (c == '\\') || (c == ' '))) {
    if (len>=max) {
      data=u8_prealloc(p,data,max+128);
      max=max+128;}
    if (c == '\\') {
      char obuf[4];
      obuf[0]=c=u8_getc(in);
      obuf[1]=c=u8_getc(in);
      obuf[2]='\0';
      if (c<0) return FD_EOX;
      else if ((isxdigit(obuf[0])) && (isxdigit(obuf[1]))) {
	c=strtol(obuf,NULL,16);
	if (c<256) data[len++]=c; else break;}
      else break;}
    else data[len++]=c;
    c=u8_getc(in);}
  if (c<0) return FD_EOX;
  else if (c=='"')
    if (p) {
      struct FD_STRING *packetp=u8_pmalloc(p,sizeof(struct FD_STRING));
      fdtype packet=fd_init_packet(packetp,len,data);
      if (FD_ABORTP(packet)) {
	u8_pfree_x(p,data,max);
	u8_pfree_x(p,packetp,sizeof(struct FD_STRING));}
      return packet;}
    else return fd_init_packet(NULL,len,data);
  else return FD_PARSE_ERROR;
}

/* Compound object parsing */

static fdtype *parse_vec
  (FD_MEMORY_POOL_TYPE *p,u8_input in,char end_char,int *size)
{
  fdtype *elts=u8_malloc(sizeof(fdtype)*32);
  unsigned int n_elts=0, max_elts=32;
  int ch=skip_whitespace(in);
  while ((ch>=0) && (ch != end_char)) {
    fdtype elt=fd_parser(in,p);
    if (FD_ABORTP(elt)) {
      int i=0; while (i < n_elts) {
	fd_decref(elts[i]); i++;}
      u8_pfree_x(p,elts,sizeof(fdtype)*max_elts); 
      if (elt == FD_EOX) *size=-1;
      else if (elt == FD_PARSE_ERROR) *size=-2;
      else *size=-3;
      if (FD_ABORTP(elt)) fd_interr(elt);
      return NULL;}
    else if (n_elts == max_elts) {
      fdtype *new_elts=u8_realloc(elts,sizeof(fdtype)*max_elts*2);
      if (new_elts) {elts=new_elts; max_elts=max_elts*2;}
      else {
	int i=0; while (i < n_elts) {fd_decref(elts[i]); i++;}
	u8_pfree_x(p,elts,sizeof(fdtype)*max_elts); *size=n_elts;
	return NULL;}}
    elts[n_elts++]=elt;
    ch=skip_whitespace(in);}
  if (ch == end_char) {
    *size=n_elts;
    ch=u8_getc(in); /* Skip the end char */
    if (n_elts) return elts;
    else {
      u8_pfree_x(p,elts,sizeof(fdtype)*max_elts); 
      return NULL;}}
  else {
    int i=0; while (i < n_elts) {fd_decref(elts[i]); i++;}
    u8_pfree_x(p,elts,sizeof(fdtype)*max_elts); *size=ch;
    return NULL;}
}

static fdtype parse_list(U8_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
  /* This starts parsing the list after a '(' has been read. */
  int ch=skip_whitespace(in); fdtype head;
  if (ch<0)
    if (ch==-1) return FD_EOX;
    else return FD_PARSE_ERROR;
  else if (ch == ')') {
    /* The empty list case */
    u8_getc(in); return FD_EMPTY_LIST;}
  else {
    /* This is where we build the list.  We recur in the CAR direction and
       iterate in the CDR direction to avoid growing the stack. */
    struct FD_PAIR *scan;
    fdtype car=fd_parser(in,p), head;
    if (FD_ABORTP(car))
      return car;
    else {
      scan=u8_pmalloc_type(p,struct FD_PAIR);
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
      list_elt=fd_parser(in,p);
      if (FD_ABORTP(list_elt)) {
	fd_decref(head); return list_elt;}
      new_pair=u8_pmalloc_type(p,struct FD_PAIR);
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
      tail=fd_parser(in,p);
      if (FD_ABORTP(tail)) {
	fd_decref(head); return tail;}
      skip_whitespace(in); ch=u8_getc(in);
      if (ch == ')') {scan->cdr=tail; return head;}
      fd_decref(head); fd_decref(tail);
      return FD_PARSE_ERROR;}}
}

static fdtype parse_vector(U8_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
  int n_elts;
  fdtype *elts=parse_vec(p,in,')',&n_elts);
  if (n_elts>=0) 
    return fd_init_vector(u8_pmalloc_type(p,struct FD_VECTOR),n_elts,elts);
  else if (n_elts==-1) return FD_EOX;
  else return FD_PARSE_ERROR;
}

static fdtype parse_slotmap(U8_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
  int n_elts;
  fdtype *elts=parse_vec(p,in,']',&n_elts);
  if (n_elts>=0) 
    return fd_init_slotmap(u8_pmalloc_type(p,struct FD_SLOTMAP),n_elts/2,
			   (struct FD_KEYVAL *)elts,p);
  else if (n_elts==-1) return FD_EOX;
  else return FD_PARSE_ERROR;
}

static fdtype parse_choice(U8_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
  int ch=skip_whitespace(in);
  if (ch == '}') {
    u8_getc(in); return FD_EMPTY_CHOICE;}
  else if (ch < 0) 
    if (ch==-1) return FD_EOX; else return FD_PARSE_ERROR;
  else {
    int n_elts; fdtype *elts=parse_vec(p,in,'}',&n_elts);
    if (n_elts==0) return FD_EMPTY_CHOICE;
    else if (n_elts==1) {
      fdtype v=elts[0]; u8_free(elts);
      return v;}
    else if (n_elts>0) {
      struct FD_CHOICE *ch=fd_palloc_choice(p,n_elts);
      fdtype result=fd_init_choice(ch,n_elts,elts,FD_CHOICE_DOSORT);
      if (FD_XCHOICE_SIZE(ch)==1) {
	result=FD_XCHOICE_DATA(ch)[0];
	u8_pfree_x(p,ch,sizeof(struct FD_CHOICE));}
      u8_free(elts);
      return result;}
    else if (n_elts == -1) return FD_EOX;
    else return FD_PARSE_ERROR;}
}

static fdtype parse_qchoice(U8_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
  int n_elts;
  fdtype *elts=parse_vec(p,in,'}',&n_elts);
  if (n_elts==0)
    return fd_init_qchoice(u8_pmalloc_type(p,struct FD_QCHOICE),
			   FD_EMPTY_CHOICE);
  else if (n_elts==1) return elts[0];
  else if (n_elts>1) {
    struct FD_CHOICE *xch=fd_palloc_choice(p,n_elts);
    fdtype choice=fd_init_choice(xch,n_elts,elts,FD_CHOICE_DOSORT);
    if (FD_XCHOICE_SIZE(xch)==1) {
      fdtype result=FD_XCHOICE_DATA(xch)[0];
      u8_free(elts); return result;}
    u8_free(elts);
    return fd_init_qchoice(u8_pmalloc_type(p,struct FD_QCHOICE),choice);}
  else if (n_elts==-1) return FD_EOX;
  else return FD_PARSE_ERROR;
}

/* Record parsing */

static fdtype recreate_record(FD_MEMORY_POOL_TYPE *p,int n,fdtype *v)
{
  int i=0;
  struct FD_COMPOUND_ENTRY *entry=fd_lookup_compound(v[0]);
  if ((entry) && (entry->parser)) {
    fdtype result=entry->parser(p,n,v);
    while (i<n) {fd_decref(v[i]); i++;}
    return result;}
  if (n==2) {
    fdtype compound=
      fd_init_compound(u8_pmalloc(p,sizeof(struct FD_COMPOUND)),
		       v[0],v[1]);
    u8_free(v);
    return compound;}
  else if (n>2)
    u8_warn(fd_CantParseRecord,"record %q %q %q",v[0],v[1],v[2]);
  else if (n> 1)
    u8_warn(fd_CantParseRecord,"record %q",v[0],v[1]);
  else u8_warn(fd_CantParseRecord,"record %q",v[0]);
  return fd_err(fd_CantParseRecord,"fd_recreate_record",NULL,v[0]);
}

static fdtype parse_record(U8_INPUT *in,FD_MEMORY_POOL_TYPE *p)
{
  int n_elts;
  fdtype *elts=parse_vec(p,in,'>',&n_elts);
  if (n_elts>0)
    return recreate_record(p,n_elts,elts);
  else if (n_elts==0)
    return fd_err(fd_CantParseRecord,"parse_record","empty record",FD_VOID);
  else if (n_elts==-1) return FD_EOX;
  else return FD_PARSE_ERROR;
}

/* The main parser procedure */

FD_EXPORT
/* fd_parser:
     Arguments: a U8 input stream and a memory pool
     Returns: a lisp object

     Parses a textual object representation from a stream into a lisp object.
*/
fdtype fd_parser(u8_input in,FD_MEMORY_POOL_TYPE *p)
{
  int inchar=skip_whitespace(in);
  if (inchar<0)
    if (inchar==-1) return FD_EOX;
    else return FD_PARSE_ERROR;
  switch (inchar) {
  case ')': case ']': case '}': {
    u8_getc(in); /* Consume the character */
    return fd_err(fd_ParseError,"unexpected terminator",
		  u8_strndup(in->u8_inptr,17),
		  FD_CODE2CHAR(inchar));}
  case '"': return parse_string(in,p);
  case '@': return parse_oid(in,p);
  case '(': 
    /* Skip the open paren and parse the list */
    u8_getc(in); return parse_list(in,p);
  case '{':
    /* Skip the open brace and parse the choice */
    u8_getc(in); return parse_choice(in,p);
  case '\'': {
    fdtype content;
    u8_getc(in); /* Skip the quote mark */
    content=fd_parser(in,p);
    if (FD_ABORTP(content)) return content;
    else return fd_make_list(2,quote_symbol,content);}
  case '`': {
    fdtype content;
    u8_getc(in); /* Skip the quote mark */
    content=fd_parser(in,p);
    if (FD_ABORTP(content)) return content;
    else return fd_make_list(2,quasiquote_symbol,content);}
  case ',': {
    fdtype content; int c=u8_getc(in); c=u8_getc(in);
    /* Skip the quote mark and check for an atsign. */
    if (c != '@') u8_ungetc(in,c);
    content=fd_parser(in,p);
    if (FD_ABORTP(content)) return content;
    else if (c == '@')
      return fd_make_list(2,unquotestar_symbol,content);
    else return fd_make_list(2,unquote_symbol,content);}
  case '#': {
    int ch=u8_getc(in); ch=u8_getc(in);
    switch (ch) {
    case '(': return parse_vector(in,p);
    case '{': return parse_qchoice(in,p);
    case '[': return parse_slotmap(in,p);
    case '"': return parse_packet(in,p);
    case '<': return parse_record(in,p);
    case '#':
      return fd_make_list(2,histref_symbol,fd_parser(in,p));
    case '\\': return parse_character(in);
    default: u8_ungetc(in,ch);}}
  default: { /* Parse an atom */
    struct U8_OUTPUT tmpbuf; char buf[128]; int c; fdtype result;
    U8_INIT_OUTPUT_BUF(&tmpbuf,128,buf);
    if (inchar == '#') u8_putc(&tmpbuf,'#');
    c=copy_atom(in,&tmpbuf);
    if (tmpbuf.u8_outptr==tmpbuf.u8_outbuf) result=FD_EOX;
    else if (inchar == '|')
      result=fd_make_symbol(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
    else result=fd_parse_atom(u8_outstring(&tmpbuf),u8_outlen(&tmpbuf));
    if (tmpbuf.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmpbuf.u8_outbuf);
    return result;}
  }
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
  return fd_parser(&stream,NULL);
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
  else if ((strchr("@{#(",arg[0])) || (isdigit(arg[0])))
    return fd_parse(arg);
  else if ((strchr("+-.",arg[0])) && (isdigit(arg[1])))
    return fd_parse(arg);
  else if (*arg == ':') return fd_parse(arg+1);
  else if (*arg == '\\') return fdtype_string(arg+1);
  else return fdtype_string(arg);
}

/* U8_PRINTF extensions */

static u8_string lisp_printf_handler
  (u8_output s,char *cmd,u8_byte *buf,int bufsiz,va_list *args)
{
  fd_unparse(s,va_arg(*args,fdtype));
  return NULL;
}

FD_EXPORT fd_init_textio_c()
{
  fd_register_source_file(versionid);

  u8_printf_handlers['q']=lisp_printf_handler;

  fd_unparsers[fd_string_type]=unparse_string;
  fd_unparsers[fd_packet_type]=unparse_packet;
  fd_unparsers[fd_vector_type]=unparse_vector;
  fd_unparsers[fd_pair_type]=unparse_pair;
  fd_unparsers[fd_choice_type]=unparse_choice;
  fd_unparsers[fd_compound_type]=unparse_compound;

  quote_symbol=fd_intern("QUOTE");
  quasiquote_symbol=fd_intern("QUASIQUOTE");
  unquote_symbol=fd_intern("UNQUOTE");
  unquotestar_symbol=fd_intern("UNQUOTE*");
  histref_symbol=fd_intern("%HISTREF");
  comment_symbol=fd_intern("COMMENT");
}


/* The CVS log for this file
   $Log: textio.c,v $
   Revision 1.93  2006/02/13 18:37:14  haase
   Fixes to text i/o

   Revision 1.92  2006/02/10 14:24:14  haase
   Added fd_unparse_maxelts and fd_unparse_maxchars to control printing

   Revision 1.91  2006/02/03 19:25:42  haase
   Fixed handling of #\&

   Revision 1.90  2006/02/03 19:09:07  haase
   Made more characters printable in packets

   Revision 1.89  2006/01/31 13:47:24  haase
   Changed fd_str[n]dup into u8_str[n]dup

   Revision 1.88  2006/01/26 14:44:32  haase
   Fixed copyright dates and removed dangling EFRAMERD references

   Revision 1.87  2006/01/25 18:47:20  haase
   Added XHTML entity parsing to string and character syntax

   Revision 1.86  2006/01/07 03:43:16  haase
   Fixes to choice mergesort implementation

   Revision 1.85  2006/01/03 16:59:51  haase
   Fixed dramatic typo in minor cleanup of choice parsing

   Revision 1.84  2006/01/03 15:42:51  haase
   Simplified code paths

   Revision 1.83  2005/12/28 23:03:29  haase
   Made choices be direct blocks of elements, including various fixes, simplifications, and more detailed documentation.

   Revision 1.82  2005/12/26 18:19:44  haase
   Reorganized and documented lisp pointers and conses

   Revision 1.81  2005/12/21 19:04:06  haase
   Removed leak in compound parsing

   Revision 1.80  2005/12/20 20:43:58  haase
   Added default record parsing and unparsing

   Revision 1.79  2005/12/17 15:44:30  haase
   Fixes to distinguish terminal flonums from dotted ints in lists

   Revision 1.78  2005/09/05 12:39:33  haase
   Output all non-ascii character objects as unicode codepoints

   Revision 1.77  2005/08/10 06:34:09  haase
   Changed module name to fdb, moving header file as well

   Revision 1.76  2005/07/16 17:53:28  haase
   Made unexpected terminators be consumed

   Revision 1.75  2005/06/14 19:07:45  haase
   Fixed leftover ungetc in symbol parsing

   Revision 1.74  2005/06/14 19:03:05  haase
   Fixed character parsing bug which didn't put terminating characters back

   Revision 1.73  2005/06/04 01:25:52  haase
   Fixed READ to return the EOF object and made it a non-error

   Revision 1.72  2005/05/27 13:47:23  haase
   Added display for compounds

   Revision 1.71  2005/05/18 19:25:20  haase
   Fixes to header ordering to make off_t defaults be pervasive

   Revision 1.70  2005/04/29 18:48:54  haase
   Fixed character parsing bug

   Revision 1.69  2005/04/24 01:53:07  haase
   Removed redundant of terminator check

   Revision 1.68  2005/04/21 19:05:14  haase
   Fix parser to recognize errors sooner and avoid falling through cases in the parser

   Revision 1.67  2005/04/17 16:43:45  haase
   Move unexpected terminator test

   Revision 1.66  2005/04/17 12:37:39  haase
   Improve error reporting for misterminated expressions

   Revision 1.65  2005/04/16 16:55:11  haase
   Fixed fd_parse_arg of empty string

   Revision 1.64  2005/04/15 14:37:35  haase
   Made all malloc calls go to libu8

   Revision 1.63  2005/04/13 15:01:52  haase
   Fixed bugs in error passing behaviour

   Revision 1.62  2005/04/11 17:54:51  haase
   Fixes to character parsing to handle punctuation characters

   Revision 1.61  2005/04/11 00:38:30  haase
   Have parse_vec abort on exceptions

   Revision 1.60  2005/03/30 15:30:00  haase
   Made calls to new seterr do appropriate strdups

   Revision 1.59  2005/03/30 14:48:43  haase
   Extended error reporting to distinguish context discrimination (a const string) from details (malloc'd)

   Revision 1.58  2005/03/22 19:06:59  haase
   Made quasiquote work

   Revision 1.57  2005/03/19 02:50:15  haase
   Rearranging core pointer types

   Revision 1.56  2005/03/18 01:57:00  haase
   Exception fixes

   Revision 1.55  2005/03/17 03:59:30  haase
   Fixed empty vector GC bug

   Revision 1.54  2005/03/16 22:22:46  haase
   Added bignums

   Revision 1.53  2005/03/14 05:49:31  haase
   Updated comments and internal documentation

   Revision 1.52  2005/03/05 21:07:39  haase
   Numerous i18n updates

   Revision 1.51  2005/03/05 18:19:18  haase
   More i18n modifications

   Revision 1.50  2005/03/04 04:08:33  haase
   Fixes for minor libu8 changes

   Revision 1.49  2005/02/25 20:13:22  haase
   Fixed incref and decref references for double evaluation

   Revision 1.48  2005/02/25 16:03:42  haase
   Fix to bug introduced in list printing

   Revision 1.47  2005/02/23 22:49:27  haase
   Created generalized compound registry and made compound dtypes and #< reading use it

   Revision 1.46  2005/02/21 22:36:11  haase
   Fixes to record reading implementation

   Revision 1.45  2005/02/21 22:13:08  haase
   Added readable record printing

   Revision 1.44  2005/02/21 21:18:02  haase
   Fixed handling of comments, integrating into skip_whitespace

   Revision 1.43  2005/02/19 19:30:17  haase
   Fix OID prefixes to be case-insensitive

   Revision 1.42  2005/02/19 19:08:49  haase
   NULL terminated the array of constants

   Revision 1.41  2005/02/19 16:28:51  haase
   Major reorganization including increased modularization for
   smaller functions and easier debugging.  Separated out character
   object parsing from atom parsing, various simplifications of parsing
   routines to avoid redundant buffer copying.

   Revision 1.40  2005/02/15 22:39:04  haase
   More extensions to port handling functions, including GETLINE procedure

   Revision 1.39  2005/02/15 14:51:38  haase
   Added declarations to inline some U8 stream ops

   Revision 1.38  2005/02/15 13:34:32  haase
   Updated fd_parser to use input streams rather than just strings

   Revision 1.37  2005/02/15 03:03:40  haase
   Updated to use the new libu8

   Revision 1.36  2005/02/11 04:44:32  haase
   indentation changes

   Revision 1.35  2005/02/11 02:51:14  haase
   Added in-file CVS logs

*/
