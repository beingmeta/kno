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
                         int col,int maxcol,int first_kv);

#define PPRINT_ATOMICP(x) \
  (!((FD_PAIRP(x)) || (FD_VECTORP(x)) || (FD_SLOTMAPP(x)) || \
     (FD_CHOICEP(x)) || (FD_PRECHOICEP(x)) || (FD_QCHOICEP(x))))

FD_EXPORT
int fd_pprint(u8_output out,fdtype x,u8_string prefix,
              int indent,int col,int maxcol,int is_initial)
{
  int startoff = out->u8_write-out->u8_outbuf, n_chars;
  if (is_initial==0) u8_putc(out,' ');
  fd_unparse(out,x); n_chars = u8_strlen(out->u8_outbuf+startoff);
  /* If we're not going to descend, and it all fits, just return the
     new column position. */
  if ((n_chars<5)||
      ((PPRINT_ATOMICP(x)) && ((is_initial) || (col+n_chars<maxcol))))
    return col+n_chars;
  /* Otherwise, reset the stream pointer. */
  out->u8_write = out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
  /* Newline and indent if you're non-initial and ran out of space. */
  if ((is_initial==0) && (col+n_chars>=maxcol)) {
    int i = indent; u8_putc(out,'\n');
    if (prefix) u8_puts(out,prefix);
    while (i>0) {u8_putc(out,' '); i--;}
    col = indent+((prefix) ? (u8_strlen(prefix)) : (0));
    startoff = out->u8_write-out->u8_outbuf;}
  else if (is_initial==0) u8_putc(out,' ');
  /* Handle quote, quasiquote and friends */
  if ((FD_PAIRP(x)) && (FD_SYMBOLP(FD_CAR(x))) &&
      (FD_PAIRP(FD_CDR(x))) && (FD_EMPTY_LISTP(FD_CDR(FD_CDR(x))))) {
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
    fdtype car = FD_CAR(x), scan = x; int first = 1;
    if (FD_SYMBOLP(car)) indent = indent+2;
    u8_putc(out,'('); col++; indent++;
    while (FD_PAIRP(scan)) {
      col = fd_pprint(out,FD_CAR(scan),prefix,indent,col,maxcol,first);
      first = 0; scan = FD_CDR(scan);}
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
        col = fd_pprint(out,scan,prefix,indent+2,col,maxcol,1);
        u8_putc(out,')');
	return col+1;}
      else return col+n_chars;}}
  else if (FD_VECTORP(x)) {
    int len = FD_VECTOR_LENGTH(x);
    if (len==0) {u8_puts(out,"#()"); return col+3;}
    else {
      int eltno = 0;
      u8_puts(out,"#("); col = col+2;
      while (eltno<len) {
        col = fd_pprint(out,FD_VECTOR_REF(x,eltno),prefix,
                      indent+2,col,maxcol,(eltno==0));
        eltno++;}
      u8_putc(out,')'); return col+1;}}
  else if (FD_QCHOICEP(x)) {
    struct FD_QCHOICE *qc = FD_XQCHOICE(x);
    int first_value = 1;
    if (FD_EMPTY_CHOICEP(qc->qchoiceval)) {
      u8_puts(out,"#{}");
      return col+3;}
    else {
      FD_DO_CHOICES(elt,qc->qchoiceval)
        if (first_value) {
          u8_puts(out,"#{"); col++; first_value = 0;
          col = fd_pprint(out,elt,prefix,indent+2,col,maxcol,1);}
        else col = fd_pprint(out,elt,prefix,indent+2,col,maxcol,0);
      u8_putc(out,'}');
      return col+1;}}
  else if (FD_CHOICEP(x)) {
    int first_value = 1;
    FD_DO_CHOICES(elt,x)
      if (first_value) {
        u8_putc(out,'{'); col++; first_value = 0;
        col = fd_pprint(out,elt,prefix,indent+1,col,maxcol,1);}
      else col = fd_pprint(out,elt,prefix,indent+1,col,maxcol,0);
    u8_putc(out,'}'); return col+1;}
  else if (FD_SLOTMAPP(x)) {
    struct FD_SLOTMAP *sm = FD_XSLOTMAP(x);
    struct FD_KEYVAL *scan, *limit;
    int slotmap_size, first_kv = 1;
    slotmap_size = FD_XSLOTMAP_NUSED(sm);
    if (slotmap_size==0) {
      if (is_initial) {
        u8_puts(out,"#[]"); return col+3;}
      else {u8_puts(out," #[]"); return col+4;}}
    fd_read_lock_table(sm);
    scan = sm->sm_keyvals; limit = sm->sm_keyvals+slotmap_size;
    u8_puts(out,"#["); col = col+2;
    while (scan<limit) {
      fdtype key = scan->kv_key, val = scan->kv_val;
      int newcol = output_keyval(out,key,val,col,maxcol,first_kv);
      if (newcol>=0) {
        col = newcol; scan++; first_kv = 0;
        continue;}
      else {
        int i = indent; u8_putc(out,'\n');
        if (prefix) u8_puts(out,prefix);
        while (i>0) {u8_putc(out,' '); i--;}
        col = indent+((prefix) ? (u8_strlen(prefix)) : (0));}
      col = fd_pprint(out,scan->kv_key,prefix,indent+2,col,maxcol,first_kv);
      u8_putc(out,' '); col++;
      col = fd_pprint(out,scan->kv_val,prefix,indent+4,col,maxcol,0);
      first_kv = 0;
      scan++;}
    u8_puts(out,"]");
    fd_unlock_table(sm);
    return col+1;}
  else {
    int startoff = out->u8_write-out->u8_outbuf;
    fd_unparse(out,x); n_chars = u8_strlen(out->u8_outbuf+startoff);
    return indent+n_chars;}
}

FD_EXPORT
int fd_xpprint(u8_output out,fdtype x,u8_string prefix,
               int indent,int col,int maxcol,int is_initial,
               fd_pprintfn fn,void *data)
{
  int startoff = out->u8_write-out->u8_outbuf, n_chars;
  if (fn) {
    int newcol = fn(out,x,prefix,indent,col,maxcol,is_initial,data);
    if (newcol>=0) return newcol;}
  if (is_initial==0) u8_putc(out,' ');
  fd_unparse(out,x); n_chars = u8_strlen(out->u8_outbuf+startoff);
  /* If we're not going to descend, and it all fits, just return the
     new column position. */
  if ((PPRINT_ATOMICP(x)) && ((is_initial) || (col+n_chars<maxcol)))
    return col+n_chars;
  /* Otherwise, reset the stream pointer. */
  out->u8_write = out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
  /* Newline and indent if you're non-initial and ran out of space. */
  if ((is_initial==0) && (col+n_chars>=maxcol)) {
    int i = indent; u8_putc(out,'\n');
    if (prefix) u8_puts(out,prefix);
    while (i>0) {u8_putc(out,' '); i--;}
    col = indent+((prefix) ? (u8_strlen(prefix)) : (0));
    startoff = out->u8_write-out->u8_outbuf;}
  else if (is_initial==0) u8_putc(out,' ');
  /* Handle quote, quasiquote and friends */
  if ((FD_PAIRP(x)) && (FD_SYMBOLP(FD_CAR(x))) &&
      (FD_PAIRP(FD_CDR(x))) && (FD_EMPTY_LISTP(FD_CDR(FD_CDR(x))))) {
    fdtype car = FD_CAR(x);
    if (FD_EQ(car,quote_symbol)) {
      u8_putc(out,'\''); indent++; x = FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,unquote_symbol)) {
      indent++; u8_putc(out,','); x = FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,quasiquote_symbol)) {
      indent++; u8_putc(out,'`'); x = FD_CAR(FD_CDR(x));}
    else if (FD_EQ(car,unquote_star_symbol)) {
      indent++; indent++; u8_puts(out,",@"); x = FD_CAR(FD_CDR(x));}}
  /* Special compound printers for different types. */
  if (FD_PAIRP(x)) {
    fdtype car = FD_CAR(x), scan = x; int first = 1;
    if (FD_SYMBOLP(car)) indent = indent+2;
    u8_putc(out,'('); col++; indent++;
    while (FD_PAIRP(scan)) {
      col = fd_xpprint(out,FD_CAR(scan),prefix,indent,col,maxcol,first,fn,data);
      first = 0; scan = FD_CDR(scan);}
    if (FD_EMPTY_LISTP(scan)) {
      u8_putc(out,')');  return col+1;}
    else {
      startoff = out->u8_write-out->u8_outbuf; 
      u8_puts(out," . "); fd_unparse(out,scan);
      n_chars = u8_strlen(out->u8_outbuf+startoff);
      if (col+n_chars>maxcol) {
        int i = indent;
        out->u8_write = out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
        u8_putc(out,'\n');
        if (prefix) u8_puts(out,prefix);
        while (i>0) {u8_putc(out,' '); i--;}
        u8_puts(out,". ");
        col = indent+2+((prefix) ? (u8_strlen(prefix)) : (0));
        col = fd_xpprint(out,scan,prefix,indent+2,col,maxcol,1,fn,data);
        u8_putc(out,')'); return col+1;}
      else return col+n_chars;}}
  else if (FD_VECTORP(x)) {
    int len = FD_VECTOR_LENGTH(x);
    if (len==0) {u8_puts(out,"#()"); return col+3;}
    else {
      int eltno = 0;
      u8_puts(out,"#("); col = col+2;
      while (eltno<len) {
        col = fd_xpprint(out,FD_VECTOR_REF(x,eltno),prefix,indent+2,
                       col,maxcol,(eltno==0),fn,data);
        eltno++;}
      u8_putc(out,')'); return col+1;}}
  else if (FD_QCHOICEP(x)) {
    struct FD_QCHOICE *qc = FD_XQCHOICE(x);
    int first_value = 1;
    FD_DO_CHOICES(elt,qc->qchoiceval)
      if (first_value) {
        u8_puts(out,"#{"); col++; first_value = 0;
        col = fd_xpprint(out,elt,prefix,indent+2,col,maxcol,1,fn,data);}
      else col = fd_xpprint(out,elt,prefix,indent+2,col,maxcol,0,fn,data);
    u8_putc(out,'}'); return col+1;}
  else if (FD_CHOICEP(x)) {
    int first_value = 1;
    FD_DO_CHOICES(elt,x)
      if (first_value) {
        u8_putc(out,'{'); col++; first_value = 0;
        col = fd_xpprint(out,elt,prefix,indent+1,col,maxcol,1,fn,data);}
      else col = fd_xpprint(out,elt,prefix,indent+1,col,maxcol,0,fn,data);
    u8_putc(out,'}'); return col+1;}
  else if (FD_SLOTMAPP(x)) {
    struct FD_SLOTMAP *sm = FD_XSLOTMAP(x);
    struct FD_KEYVAL *scan, *limit;
    int slotmap_size, first_pair = 1;
    fd_read_lock_table(sm);
    slotmap_size = FD_XSLOTMAP_NUSED(sm);
    if (slotmap_size==0) fd_unlock_table(sm);
    if (slotmap_size==0) {
      if (is_initial) {
        u8_puts(out," #[]"); return 3;}
      else {u8_puts(out," #[]"); return 4;}}
    scan = sm->sm_keyvals; limit = sm->sm_keyvals+slotmap_size;
    u8_puts(out,"#["); col = col+2;
    while (scan<limit) {
      col = fd_xpprint(out,scan->kv_key,prefix,
                     indent+2,col,maxcol,first_pair,fn,data);
      col = fd_xpprint(out,scan->kv_val,
                     prefix,indent+4,col,maxcol,0,fn,data);
      first_pair = 0;
      scan++;}
    u8_puts(out,"]");
    fd_unlock_table(sm);
    return col+1;}
  else {
    int startoff = out->u8_write-out->u8_outbuf;
    fd_unparse(out,x); n_chars = u8_strlen(out->u8_outbuf+startoff);
    return indent+n_chars;}
}

static int output_keyval(u8_output out,fdtype key,fdtype val,
                         int col,int maxcol,int first_kv)
{
  int len = 0;
  if (FD_STRINGP(key)) len = len+FD_STRLEN(key)+3;
  else if (FD_SYMBOLP(key)) {
    u8_string pname = FD_SYMBOL_NAME(key);
    len = len+strlen(pname)+1;}
  else if (FD_CONSP(key)) return -1;
  else {}
  if (FD_STRINGP(val)) len = len+FD_STRLEN(val)+3;
  else if (FD_SYMBOLP(val)) {
    u8_string pname = FD_SYMBOL_NAME(val);
    len = len+strlen(pname)+1;}
  else {}
  if ((col+len)>maxcol) return -1;
  else {
    struct U8_OUTPUT kvout; u8_byte kvbuf[128];
    U8_INIT_FIXED_OUTPUT(&kvout,128,kvbuf);
    if (!(first_kv)) u8_putc(&kvout,' ');
    fd_unparse(&kvout,key);
    u8_putc(&kvout,' ');
    fd_unparse(&kvout,val);
    if ((kvout.u8_streaminfo&U8_STREAM_OVERFLOW)||
        ((col+(kvout.u8_write-kvout.u8_outbuf))>maxcol))
      return -1;
    else u8_putn(out,kvout.u8_outbuf,kvout.u8_write-kvout.u8_outbuf);
    len = len+kvout.u8_write-kvout.u8_outbuf;}
  return len;
}

/* Focused pprinting */

struct FOCUS_STRUCT {fdtype focus; u8_string prefix, suffix;};

static int focus_pprint(u8_output out,fdtype x,u8_string prefix,
                        int indent,int col,int maxcol,int is_initial,void *data)
{
  struct FOCUS_STRUCT *fs = (struct FOCUS_STRUCT *) data;
  if (FD_EQ(x,fs->focus)) {
    int startoff = out->u8_write-out->u8_outbuf, n_chars;
    if (is_initial==0) u8_putc(out,' ');
    fd_unparse(out,x); n_chars = u8_strlen(out->u8_outbuf+startoff);
    out->u8_write = out->u8_outbuf+startoff; out->u8_outbuf[startoff]='\0';
    if (col+n_chars>=maxcol) {
      int i = indent; u8_putc(out,'\n');
      if (prefix) u8_puts(out,prefix);
      while (i>0) {u8_putc(out,' '); i--;}
      col = indent+((prefix) ? (u8_strlen(prefix)) : (0));}
    else if (is_initial==0) {u8_putc(out,' '); col++;}
    if (fs->prefix) u8_puts(out,fs->prefix);
    col = fd_pprint(out,x,prefix,indent,col,maxcol,1);
    if (fs->suffix) u8_puts(out,fs->suffix);
    return col;}
  else return -1;
}

FD_EXPORT
void fd_pprint_focus(U8_OUTPUT *out,fdtype entry,fdtype focus,u8_string prefix,
                     int indent,int width,u8_string focus_prefix,
                     u8_string focus_suffix)
{
  struct FOCUS_STRUCT fs;
  fs.focus = focus;
  fs.prefix = focus_prefix;
  fs.suffix = focus_suffix;
  fd_xpprint(out,entry,prefix,indent,indent,width,1,
             focus_pprint,(void *)&fs);
}

FD_EXPORT void fd_init_pprint_c()
{
  u8_register_source_file(_FILEINFO);

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
