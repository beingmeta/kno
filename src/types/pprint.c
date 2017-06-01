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

/* Pretty printing */

static fdtype quote_symbol, comment_symbol;
static fdtype unquote_symbol, quasiquote_symbol, unquote_star_symbol;

static int output_keyval(u8_output out,fdtype key,fdtype val,
                         int col,int maxcol);

#define PPRINT_ATOMICP(x)                     \
  (!((FD_PAIRP(x)) || (FD_VECTORP(x)) ||      \
     (FD_SCHEMAPP(x)) || (FD_SLOTMAPP(x)) ||  \
     (FD_CHOICEP(x)) || (FD_PRECHOICEP(x)) || \
     (FD_QCHOICEP(x)) || (FD_COMPOUNDP(x))))

/* Prototypes */

FD_EXPORT
int fd_pprint_x(u8_output out,fdtype x,u8_string prefix,
                int indent,int col,int maxcol,
                fd_pprintfn fn,void *data);

FD_EXPORT
int fd_pprint_table(u8_output out,fdtype x,
                    const fdtype *keys,size_t n_keys,
                    u8_string prefix,int indent,
                    int col,int maxcol,
                    fd_pprintfn fn,void *data);

/* The default function */

FD_EXPORT
int fd_pprint(u8_output out,fdtype x,u8_string prefix,
              int indent,int col,int maxcol)
{
  return fd_pprint_x(out,x,prefix,indent,col,maxcol,NULL,NULL);
}


/* The real function */

static int do_indent(u8_output out,u8_string prefix,int indent,
                     int startoff)
{
  u8_byte *start;
  if (startoff>=0) {
    out->u8_write = out->u8_outbuf+startoff;
    out->u8_outbuf[startoff]='\0';}
  u8_putc(out,'\n');
  start=out->u8_write;
  if (prefix) u8_puts(out,prefix);
  int i=0; while (i<indent) { u8_putc(out,' '); i++;}
  return out->u8_write-start;
}

static void do_reset(u8_output out,int startoff)
{
  out->u8_write=out->u8_outbuf+startoff;
}

FD_EXPORT
int fd_pprint_x(u8_output out,fdtype x,u8_string prefix,
                int indent,int col,int maxcol,
                fd_pprintfn fn,void *data)
{
  if (fn) {
    int newcol = fn(out,x,prefix,indent,col,maxcol,data);
    if (newcol>=0) return newcol;}
  size_t prefix_len= (prefix) ? (strlen(prefix)) : (0);
  if ( ( col > (indent+prefix_len) ) &&
       ( (FD_SLOTMAPP(x)) || (FD_SCHEMAPP(x)) ) )
    col=do_indent(out,prefix,indent,-1);
  int startoff = out->u8_write-out->u8_outbuf, n_chars;
  if ((col>(prefix_len+indent))) u8_putc(out,' ');
  fd_unparse(out,x);
  /* We call u8_strlen because we're counting chars, not bytes */
  n_chars = u8_strlen(out->u8_outbuf+startoff);
  /* Accept the flat printed value if either: */
  if ( (col+n_chars) < maxcol)
    /* It fits */
    return col+n_chars;
  else if (/* it's going to be on one line anyway and */
           (PPRINT_ATOMICP(x)) &&
           /* it wouldn't fit on its own line, and */
           ( ((prefix_len+indent+n_chars)>=maxcol) &&
             /* we're at (or near) the beginning of this line */
             (col<=(prefix_len+indent+2))))
    return col+n_chars;
  else if (PPRINT_ATOMICP(x)) {
    int new_col=do_indent(out,prefix,indent,startoff);
    fd_unparse(out,x);
    return new_col+n_chars;}
  else if ( (n_chars<5) && ( (col+n_chars) < (maxcol+5)))
    /* It's short and just a 'tiny bit' over */
    return col+n_chars;
  else if (col>(prefix_len+indent))
    col=do_indent(out,prefix,indent,startoff);
  else do_reset(out,startoff);
  /* Handle quote, quasiquote and friends */
  if ((FD_PAIRP(x)) && (FD_SYMBOLP(FD_CAR(x))) &&
      (FD_PAIRP(FD_CDR(x))) &&
      (FD_EMPTY_LISTP(FD_CDR(FD_CDR(x))))) {
    fdtype car = FD_CAR(x);
    if (FD_EQ(car,quote_symbol)) {
      u8_putc(out,'\''); indent++; x = FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,unquote_symbol)) {
      indent++; u8_putc(out,','); x = FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,quasiquote_symbol)) {
      indent++; u8_putc(out,'`'); x = FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,unquote_star_symbol)) {
      indent++; indent++; u8_puts(out,",@"); x = FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,comment_symbol)) {
      u8_puts(out,"#;"); indent = indent+2; x = FD_CAR(FD_CDR(x));}}
  /* Special compound printers for different types. */
  if (FD_PAIRP(x)) {
    fdtype car = FD_CAR(x), scan = x;
    if (FD_SYMBOLP(car)) indent = indent+2;
    u8_putc(out,'('); col++; indent++;
    while (FD_PAIRP(scan)) {
      col = fd_pprint_x(out,FD_CAR(scan),prefix,
                        indent,col,maxcol,
                        fn,data);
      scan = FD_CDR(scan);}
    if (FD_EMPTY_LISTP(scan)) {
      u8_putc(out,')');  return col+1;}
    else {
      startoff = out->u8_write-out->u8_outbuf;
      u8_puts(out," . ");
      fd_unparse(out,scan);
      u8_putc(out,')');
      n_chars = u8_strlen(out->u8_outbuf+startoff);
      if (col+n_chars>maxcol) {
        int i = indent;
        out->u8_write = out->u8_outbuf+startoff;
        out->u8_outbuf[startoff]='\0';
        u8_putc(out,'\n');
        if (prefix) u8_puts(out,prefix);
        while (i>0) {u8_putc(out,' '); i--;}
        u8_puts(out,". ");
        col = indent+2+((prefix) ? (u8_strlen(prefix)) : (0));
        col = fd_pprint_x(out,scan,prefix,indent+2,col,maxcol,fn,data);
        u8_putc(out,')');
        return col+1;}
      else return col+n_chars;}}
  else if (FD_VECTORP(x)) {
    int len = FD_VECTOR_LENGTH(x);
    if (len==0) {
      u8_puts(out,"#()");
      return col+3;}
    else {
      int eltno = 0;
      u8_puts(out,"#("); col=col+2;
      while (eltno<len) {
        col = fd_pprint_x(out,FD_VECTOR_REF(x,eltno),prefix,
                          indent+2,col,maxcol,fn,data);
        eltno++;}
      u8_putc(out,')');
      return col+1;}}
  else if (FD_QCHOICEP(x)) {
    struct FD_QCHOICE *qc = FD_XQCHOICE(x);
    int first_value = 1;
    if (FD_EMPTY_CHOICEP(qc->qchoiceval)) {
      u8_puts(out,"#{}");
      return col+3;}
    else {
      FD_DO_CHOICES(elt,qc->qchoiceval)
        if (first_value) {
          u8_puts(out,"#{");
          first_value = 0;
          col = fd_pprint_x(out,elt,prefix,indent+2,col+2,maxcol,fn,data);}
        else col = fd_pprint_x(out,elt,prefix,indent+2,col,maxcol,fn,data);
      u8_putc(out,'}');
      return col+1;}}
  else if (FD_CHOICEP(x)) {
    int first_value = 1;
    FD_DO_CHOICES(elt,x)
      if (first_value) {
        u8_putc(out,'{'); first_value = 0;
        col = fd_pprint_x(out,elt,prefix,indent+1,col+1,maxcol,fn,data);}
      else col = fd_pprint_x(out,elt,prefix,indent+1,col,maxcol,fn,data);
    u8_putc(out,'}');
    return col+1;}
  else if ( (FD_SLOTMAPP(x)) || (FD_SCHEMAPP(x)) ) {
    fdtype keys=fd_getkeys(x);
    if (FD_PRECHOICEP(keys)) keys=fd_simplify_choice(keys);
    if (FD_EMPTY_CHOICEP(keys)) {
      if (col>(prefix_len+indent)) {
        u8_puts(out," #[]");
        return col+4;}
      else {
        u8_puts(out,"#[]");
        return col+3;}}
    if (col>(prefix_len+indent))
      col=do_indent(out,prefix,indent,-1);
    u8_puts(out,"#[");
    if (!(FD_CHOICEP(keys)))
      col=fd_pprint_table(out,x,&keys,1,prefix,
                          indent+2,col+2,maxcol,
                          fn,data);
    else col=fd_pprint_table
           (out,x,FD_CHOICE_DATA(keys),FD_CHOICE_SIZE(keys),
            prefix,indent+2,col+2,maxcol,
            fn,data);
    u8_putc(out,']');
    return col+1;}
  else {
    int startoff = out->u8_write-out->u8_outbuf;
    fd_unparse(out,x);
    n_chars = u8_strlen(out->u8_outbuf+startoff);
    return prefix_len+indent+n_chars;}
}

FD_EXPORT
int fd_pprint_table(u8_output out,fdtype x,
                    const fdtype *keys,size_t n_keys,
                    u8_string prefix,int indent,int col,int maxcol,
                    fd_pprintfn fn,void *data)
{
  if (n_keys==0)
    return col;
  const fdtype *scan=keys, *limit=scan+n_keys;
  size_t prefix_len = (prefix) ? (strlen(prefix)) : (0);
  int count=0;
  while (scan<limit) {
    fdtype key = *scan++;
    fdtype val = fd_get(x,key,FD_VOID);
    if (FD_VOIDP(val)) continue;
    else if (count) {
      int i = indent;
      u8_putc(out,'\n'); col=0;
      if (prefix) { u8_puts(out,prefix); col+=prefix_len; }
      while (i>0) { u8_putc(out,' '); col++; i--; }}
    else {}
    int newcol = output_keyval(out,key,val,col,maxcol);
    if (newcol>=0) {
      /* Key + value fit on one line */
      col=newcol;
      count++;
      continue;}
    else count++;
    if (FD_SYMBOLP(key)) {
      u8_puts(out,FD_SYMBOL_NAME(key));
      col=col+strlen(FD_SYMBOL_NAME(key));}
    else if (FD_STRINGP(key)) {
      u8_putc(out,'"');
      u8_puts(out,FD_STRDATA(key));
      u8_putc(out,'"');
      col=col+2+strlen(FD_STRDATA(key));}
    else {
      struct U8_OUTPUT tmp; u8_byte tmpbuf[512];
      U8_INIT_OUTPUT_BUF(&tmp,512,tmpbuf);
      fd_unparse(&tmp,key);
      u8_puts(out,tmp.u8_outbuf);
      col=col+(tmp.u8_write-tmp.u8_outbuf);
      u8_close_output(&tmp);}
    /* Output value */
    struct U8_OUTPUT tmp; u8_byte tmpbuf[512];
    U8_INIT_OUTPUT_BUF(&tmp,512,tmpbuf);
    fd_unparse(&tmp,val);
    size_t len=tmp.u8_write-tmp.u8_outbuf;
    /* Output the prefix, indent, etc */
    int value_indent = indent+2, i=0;
    u8_putc(out,'\n'); col=0;
    if (prefix) { u8_puts(out,prefix); col+=prefix_len; }
    while (i<value_indent) {u8_putc(out,' '); i++;}
    col += value_indent;
    if ((col+len) < maxcol) {
      u8_putn(out,tmp.u8_outbuf,len);
      u8_close_output(&tmp);
      col += len;
      continue;}
    u8_close_output(&tmp);
    col=fd_pprint_x(out,val,prefix,value_indent,col,maxcol,fn,data);
    fd_decref(val);}
  return col;
}

static int output_keyval(u8_output out,fdtype key,fdtype val,
                         int col,int maxcol)
{
  ssize_t len = 0;
  if (FD_STRINGP(key))
    len = len+FD_STRLEN(key)+3;
  else if (FD_SYMBOLP(key)) {
    u8_string pname = FD_SYMBOL_NAME(key);
    len = len+strlen(pname)+1;}
  else if (FD_CONSP(key)) return -1;
  if (FD_STRINGP(val))
    len = len+FD_STRLEN(val)+2;
  else if (FD_SYMBOLP(val)) {
    u8_string pname = FD_SYMBOL_NAME(val);
    len = len+strlen(pname);}
  else {}
  if ((col+len)>maxcol)
    return -1;
  else len=0;
  /* Try it out */
  struct U8_OUTPUT kvout;
  u8_byte kvbuf[256];
  U8_INIT_STATIC_OUTPUT_BUF(kvout,256,kvbuf);
  fd_unparse(&kvout,key);
  u8_putc(&kvout,' ');
  fd_unparse(&kvout,val);
  len=kvout.u8_write-kvout.u8_outbuf;
  if ((kvout.u8_streaminfo&U8_STREAM_OVERFLOW)||((col+(len))>maxcol))
    len=-1;
  else u8_putn(out,kvout.u8_outbuf,len);
  u8_close_output(&kvout);
  if (len>=0)
    return col+len;
  else return -1;
}

/* printf handler for pprint */

static u8_string lisp_pprintf_handler
  (u8_output out,char *cmd,u8_byte *buf,int bufsiz,va_list *args)
{
  struct U8_OUTPUT tmpout;
  int width = 80; fdtype value;
  if (strchr(cmd,'*'))
    width = va_arg(*args,int);
  else {
    width = strtol(cmd,NULL,10);
    U8_CLEAR_ERRNO();}
  value = va_arg(*args,fdtype);
  U8_INIT_OUTPUT(&tmpout,512);
  fd_pprint(&tmpout,value,NULL,0,0,width);
  u8_puts(out,tmpout.u8_outbuf);
  u8_free(tmpout.u8_outbuf);
  if (strchr(cmd,'-')) fd_decref(value);
  return NULL;
}

FD_EXPORT void fd_init_pprint_c()
{
  u8_register_source_file(_FILEINFO);

  u8_printf_handlers['Q']=lisp_pprintf_handler;

  quote_symbol = fd_intern("QUOTE");
  quasiquote_symbol = fd_intern("QUASIQUOTE");
  unquote_symbol = fd_intern("UNQUOTE");
  unquote_star_symbol = fd_intern("UNQUOTE*");
  comment_symbol = fd_intern("COMMENT");
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debug;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
