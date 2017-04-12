/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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

/* Common macros, functions and declarations */

#define PARSE_ERRORP(x) ((x==FD_EOX) || (x==FD_PARSE_ERROR) || (x==FD_OOM))

#define odigitp(c) ((c>='0')&&(c<='8'))
#define spacecharp(c) ((c>0) && (c<128) && (isspace(c)))
#define atombreakp(c) \
  ((c<=0) || ((c<128) && ((isspace(c)) || (strchr("{}()[]#\"',`",c)))))

fd_exception fd_CantUnparse=_("LISP expression unparse error");

int fd_unparse_maxelts=0;
int fd_unparse_maxchars=0;
int fd_packet_outfmt=-1;

int (*fd_unparse_error)(U8_OUTPUT *,fdtype x,u8_string details)=NULL;

static fdtype quote_symbol, histref_symbol, comment_symbol;
static fdtype quasiquote_symbol, unquote_symbol, unquotestar_symbol;

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
  u8_string scan=s->fd_bytes, limit=s->fd_bytes+s->fd_bytelen;
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

static int get_packet_base(const unsigned char *bytes,int len)
{
  if ((fd_packet_outfmt==16)||
      (fd_packet_outfmt==64)||
      (fd_packet_outfmt==8))
    return fd_packet_outfmt;
  else {
    const unsigned char *scan=bytes, *limit=bytes+len; int weird=0;
    while (scan<limit) {
      int c=*scan++;
      if ((c>=128)||(iscntrl(c))) weird++;}
    if ((weird*4)>len) return 16;
    else return 8;}
}

FD_EXPORT int fd_unparse_packet
   (U8_OUTPUT *out,const unsigned char *bytes,size_t len,int base)
{
  int i=0;
  if (base==16) {
    u8_puts(out,"#X\"");
    while (i<len) {
      int byte=bytes[i++]; char buf[16];
      if ((fd_unparse_maxchars>0) && (i>=fd_unparse_maxchars)) {
        u8_putc(out,' '); output_ellipsis(out,len-i,"bytes");
        return u8_putc(out,'"');}
      sprintf(buf,"%02x",byte);
      u8_puts(out,buf);}
    return u8_putc(out,'"');}
  else if (base==64) {
    int n_chars=0;
    char *b64=u8_write_base64(bytes,len,&n_chars);
    u8_puts(out,"#@\"");
    if ((fd_unparse_maxchars>0) && (n_chars>=fd_unparse_maxchars)) {
      u8_putn(out,b64,fd_unparse_maxchars);
      output_ellipsis(out,len-fd_unparse_maxchars,"bytes");}
    else u8_putn(out,b64,n_chars);
    u8_free(b64);
    return u8_putc(out,'"');}
  else {
    u8_puts(out,"#\"");
    while (i < len) {
      int byte=bytes[i++]; char buf[16];
      if ((fd_unparse_maxchars>0) && (i>=fd_unparse_maxchars)) {
        u8_putc(out,' '); output_ellipsis(out,len-i,"bytes");
        return u8_putc(out,'"');}
      if (byte == '"') u8_puts(out,"\\\"");
      else if (byte == '\\') u8_puts(out,"\\\\");
      else if (byte == '\n') u8_puts(out,"\\n");
      else if (byte == '\r') u8_puts(out,"\\r");
      else if (byte == '\t') u8_puts(out,"\\t");
      else if ((byte>=0x7f) || (byte<0x20)) {
        sprintf(buf,"\\%03o",byte);
        u8_puts(out,buf);}
      else {
        buf[0]=byte; buf[1]='\0';
        u8_puts(out,buf);}}
    /* We don't check for an error value until we get here, which means that
       many of the calls above might have failed, however this shouldn't
       be a functional problem. */
    return u8_puts(out,"\"");}
}

static int unparse_packet(U8_OUTPUT *out,fdtype x)
{
  struct FD_STRING *s=(struct FD_STRING *)x;
  const unsigned char *bytes=s->fd_bytes;
  int len=s->fd_bytelen, base=get_packet_base(bytes,len);
  return fd_unparse_packet(out,bytes,len,base);
}

static int unparse_secret(U8_OUTPUT *out,fdtype x)
{
  struct FD_STRING *s=(struct FD_STRING *)x;
  const unsigned char *bytes=s->fd_bytes; int i=0, len=s->fd_bytelen;
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
  int i=0, len=v->fdvec_length;
  u8_puts(out,"#(");
  while (i < len) {
    if ((fd_unparse_maxelts>0) && (i>=fd_unparse_maxelts)) {
      u8_puts(out," "); output_ellipsis(out,len-i,"elts");
      return u8_puts(out,")");}
    if (i>0) u8_puts(out," "); fd_unparse(out,v->fdvec_elts[i]);
    i++;}
  return u8_puts(out,")");
}

static int unparse_rail(U8_OUTPUT *out,fdtype x)
{
  struct FD_VECTOR *v=(struct FD_VECTOR *) x;
  int i=0, len=v->fdvec_length; fdtype *data=v->fdvec_elts;
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
     Returns: int
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
    long long val=FD_FIX2INT(x);
    char buf[128]; sprintf(buf,"%lld",val);
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
  case fd_cons_ptr_type:
    if (x==FD_NULL)
      return u8_puts(out,"#null");
    else {/* output cons */
      struct FD_CONS *cons=FD_CONS_DATA(x);
      fd_ptr_type ct=FD_CONS_TYPE(cons);
      if ((FD_VALID_TYPECODEP(ct)) && (fd_unparsers[ct])) {
        int uv=fd_unparsers[ct](out,x);
        if (uv<0) {
          char buf[128];
          sprintf(buf,"#!%lx (type=0x%x)",(unsigned long)x,ct);
          u8_log(LOG_WARN,fd_CantUnparse,
                 "fd_unparse handler failed for CONS %s",buf);
          u8_clear_errors(1);
          return uv;}
        else return 1;}
      if (fd_unparse_error)
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

FD_EXPORT
/* fd_dtype2buf:
     Arguments: a lisp object
     Arguments: a length in bytes
     Arguments: a (possibly NULL) buffer
     Returns: a UTF-8 encoding string

     Writes a text representation of the object into a fixed length
     string.

*/
u8_string fd_dtype2buf(fdtype x,size_t n,u8_byte *buf)
{
  struct U8_OUTPUT out;
  if (buf==NULL) buf=u8_malloc(n+7);
  U8_INIT_FIXED_OUTPUT(&out,n,buf);
  fd_unparse(&out,x);
  return out.u8_outbuf;
}

static fdtype get_compound_tag(fdtype tag)
{
  if (FD_COMPOUND_TYPEP(tag,fd_compound_descriptor_type)) {
    struct FD_COMPOUND *c=FD_XCOMPOUND(tag);
    return fd_incref(c->compound_typetag);}
  else return tag;
}

static int unparse_compound(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_COMPOUND *xc=fd_consptr(struct FD_COMPOUND *,x,fd_compound_type);
  fdtype tag=get_compound_tag(xc->compound_typetag);
  struct FD_COMPOUND_TYPEINFO *entry=fd_lookup_compound(tag);
  if ((entry) && (entry->fd_compound_unparser)) {
    int retval=entry->fd_compound_unparser(out,x,entry);
    if (retval<0) return retval;
    else if (retval) return retval;}
  {
    fdtype *data=&(xc->compound_0);
    int i=0, n=xc->fd_n_elts;
    if ((entry)&&(entry->fd_compound_corelen>0)&&(entry->fd_compound_corelen<n))
      n=entry->fd_compound_corelen;
    u8_printf(out,"#%%(%q",xc->compound_typetag);
    while (i<n) {
      fdtype elt=data[i++];
      if (0) { /* (FD_PACKETP(elt)) */
        struct FD_STRING *packet=fd_consptr(fd_string,elt,fd_packet_type);
        const unsigned char *bytes=packet->fd_bytes; 
        int n_bytes=packet->fd_bytelen;
        u8_puts(out," ");
        fd_unparse_packet(out,bytes,n_bytes,16);}
      else u8_printf(out," %q",elt);}
    u8_printf(out,")");
    return 1;}
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

/* Custom unparsers */

static int unparse_uuid(u8_output out,fdtype x)
{
  struct FD_UUID *uuid=fd_consptr(struct FD_UUID *,x,fd_uuid_type);
  char buf[37]; u8_uuidstring((u8_uuid)(&(uuid->fd_uuid16)),buf);
  u8_printf(out,"#U%s",buf);
  return 1;
}

static int unparse_exception(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_EXCEPTION_OBJECT *xo=
    fd_consptr(struct FD_EXCEPTION_OBJECT *,x,fd_error_type);
  u8_exception ex=xo->fdex_u8ex;
  if (ex==NULL)
    u8_printf(out,"#<!OLDEXCEPTION>");
  else {
    u8_printf(out,"#<!EXCEPTION ");
    fd_sum_exception(out,xo->fdex_u8ex);
    u8_printf(out,"!>");}
  return 1;
}

static int unparse_regex(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_REGEX *rx=(struct FD_REGEX *)x;
  u8_printf(out,"#/%s/%s%s%s%s",rx->fd_rxsrc,
            (((rx->fd_rxflags)&REG_EXTENDED)?"e":""),
            (((rx->fd_rxflags)&REG_ICASE)?"i":""),
            (((rx->fd_rxflags)&REG_ICASE)?"l":""),
            (((rx->fd_rxflags)&REG_NOSUB)?"s":""));
  return 1;
}

static int unparse_mystery(u8_output out,fdtype x)
{
  struct FD_MYSTERY_DTYPE *d=
    fd_consptr(struct FD_MYSTERY_DTYPE *,x,fd_mystery_type);
  char buf[128];
  if (d->myst_dtcode&0x80)
    sprintf(buf,_("#<MysteryVector 0x%x/0x%x %d elements>"),
            d->myst_dtpackage,d->myst_dtcode,d->myst_dtsize);
  else sprintf(buf,_("#<MysteryPacket 0x%x/0x%x %d bytes>"),
               d->myst_dtpackage,d->myst_dtcode,d->myst_dtsize);
  u8_puts(out,buf);
  return 1;
}

static int unparse_rawptr(struct U8_OUTPUT *out,fdtype x)
{
  struct FD_RAWPTR *rawptr=(struct FD_RAWPTR *)x;
  if ((rawptr->typestring)&&(rawptr->idstring))
    u8_printf(out,"#<RAW '%s' 0x%llx (%s)>",
              rawptr->typestring,
              U8_PTR2INT(rawptr->ptrval),
              rawptr->idstring);
  else if (rawptr->typestring)
    u8_printf(out,"#<RAW '%s' 0x%llx>",
              rawptr->typestring,
              U8_PTR2INT(rawptr->ptrval));
  else if (rawptr->idstring)
    u8_printf(out,"#<RAW 0x%llx (%s)>",
              U8_PTR2INT(rawptr->ptrval),
              rawptr->idstring);
  else u8_printf(out,"#<RAW 0x%llx>",
                 U8_PTR2INT(rawptr->ptrval));
  return 1;
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

FD_EXPORT void fd_init_unparse_c()
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

  if (fd_unparsers[fd_error_type]==NULL)
    fd_unparsers[fd_error_type]=unparse_exception;

  if (fd_unparsers[fd_mystery_type]==NULL)
    fd_unparsers[fd_mystery_type]=unparse_mystery;

  fd_unparsers[fd_uuid_type]=unparse_uuid;
  fd_unparsers[fd_regex_type]=unparse_regex;

  quote_symbol=fd_intern("QUOTE");
  quasiquote_symbol=fd_intern("QUASIQUOTE");
  unquote_symbol=fd_intern("UNQUOTE");
  unquotestar_symbol=fd_intern("UNQUOTE*");
  histref_symbol=fd_intern("%HISTREF");
  comment_symbol=fd_intern("COMMENT");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
