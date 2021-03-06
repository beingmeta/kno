/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

   This file implements the core parser and printer (unparser) functionality.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/pprint.h"
#include "kno/ports.h"

#include <libu8/u8printf.h>
#include <libu8/u8streamio.h>
#include <libu8/u8convert.h>
#include <libu8/u8crypto.h>

#include <ctype.h>
#include <errno.h>

/* -1 = unlimited, 0 means use default */
int pprint_maxcol     = 80;
int pprint_fudge      =  5;
int pprint_maxchars   = -1;
int pprint_maxdepth   = -1;
int pprint_maxbytes   = -1;
int pprint_maxelts    = -1;
int pprint_maxkeys    = -1;
int pprint_list_max   = -1;
int pprint_vector_max = -1;
int pprint_choice_max = -1;
u8_string pprint_margin = NULL;
lispval pprint_default_rules = KNO_VOID;

#define pprint_max(prop,ppcxt)                  \
  ( ( (ppcxt) == NULL) ?                        \
    (pprint_ ## prop) :                         \
    ( (ppcxt -> pp_ ## prop) ?                  \
      (ppcxt -> pp_ ## prop) :                  \
      (pprint_ ## prop) ) )

#define pprint_max3(prop,dprop,ppcxt)           \
  ( ( (ppcxt) == NULL ) ?                       \
    ( (pprint_ ## prop) ?                       \
      (pprint_ ## prop) :                       \
      (pprint_ ## dprop) ) :                    \
    ( (ppcxt -> pp_ ## prop) ?                  \
      (ppcxt -> pp_ ## prop) :                  \
      (ppcxt -> pp_ ## dprop) ?                 \
      (ppcxt -> pp_ ## dprop) :                 \
      (pprint_ ## prop) ?                       \
      (pprint_ ## prop) :                       \
      (pprint_ ## dprop) ) )

#define OVERFLOWP(val,lim) ( ((lim) > 0) && ((val) >= (lim)) )

static int do_indent(u8_output out,u8_string margin,int indent,int startoff);
static void do_reset(u8_output out,int startoff);
static lispval get_pprint_rule(lispval car,pprint_context ppcxt);

/* Pretty printing */

static lispval comment_symbol;
static lispval unquote_symbol, quasiquote_symbol, unquote_star_symbol;

static int output_keyval(u8_output out,lispval key,lispval val,
                         int col,pprint_context ppcxt);
static int unparse(u8_output out,lispval obj,pprint_context ppcxt);

#define PPRINT_ATOMICP(x)               \
  (!((PAIRP(x)) || (VECTORP(x)) ||      \
     (SCHEMAPP(x)) || (SLOTMAPP(x)) ||  \
     (CHOICEP(x)) || (PRECHOICEP(x)) ||         \
     (QCHOICEP(x)) || (KNO_COMPOUNDP(x))))

/* The real function */

KNO_EXPORT
int kno_pprinter(u8_output out,lispval x,int indent,int col,int depth,
                 kno_pprintfn customfn,void *customdata,
                 pprint_context ppcxt)
{
  int maxcol = pprint_max(maxcol,ppcxt);
  int maxdepth = pprint_max(maxdepth,ppcxt);
  int fudge  = pprint_max(fudge,ppcxt);
  u8_string margin = (ppcxt)? (ppcxt->pp_margin) : (pprint_margin);
  size_t margin_len = (ppcxt) ? (ppcxt->pp_margin_len) :
    (margin) ? (strlen(margin)) : (0);

  if (OVERFLOWP(depth,maxdepth)) {} else {}
  if (customfn) {
    int newcol = customfn(out,x,indent,col,depth,ppcxt,customdata);
    if (newcol>=0) return newcol;}
  if ( ( col > (indent+margin_len) ) &&
       ( (SLOTMAPP(x)) || (SCHEMAPP(x)) ) )
    col=do_indent(out,margin,indent,-1);
  int startoff = out->u8_write-out->u8_outbuf, n_chars;
  unparse(out,x,ppcxt);
  /* We call u8_strlen because we're counting chars, not bytes */
  n_chars = u8_strlen(out->u8_outbuf+startoff);
  /* Accept the flat printed value if either: */
  if ( (col+n_chars) < maxcol) /* it fits */
    return col+n_chars;
  else if /* or it's going to be on one line anyway and */
    ((PPRINT_ATOMICP(x)) &&
     /* it wouldn't fit on its own line, and */
     ( ((margin_len+indent+n_chars)>=maxcol) &&
       /* we're at (or near) the beginning of this line */
       (col<=(margin_len+indent+3))))
    return col+n_chars;
  else if (PPRINT_ATOMICP(x)) {
    int new_col=do_indent(out,margin,indent,startoff);
    unparse(out,x,ppcxt);
    return new_col+n_chars;}
  else if ( (n_chars<fudge) && ( (col+n_chars) < (maxcol+5)))
    /* It's short and just a 'tiny bit' (5 chars) over */
    return col+n_chars;
  else if (col>(margin_len+indent))
    col=do_indent(out,margin,indent,startoff);
  else do_reset(out,startoff);
  /* Handle quote, quasiquote and friends */
  if ((PAIRP(x)) && (SYMBOLP(KNO_CAR(x))) &&
      (PAIRP(KNO_CDR(x))) &&
      (NILP(KNO_CDR(KNO_CDR(x))))) {
    lispval car = KNO_CAR(x);
    if (KNO_EQ(car,KNOSYM_QUOTE)) {
      u8_putc(out,'\''); indent++; x = KNO_CAR(KNO_CDR(x));}
    else if (KNO_EQ(car,unquote_symbol)) {
      indent++; u8_putc(out,','); x = KNO_CAR(KNO_CDR(x));}
    else if (KNO_EQ(car,quasiquote_symbol)) {
      indent++; u8_putc(out,'`'); x = KNO_CAR(KNO_CDR(x));}
    else if (KNO_EQ(car,unquote_star_symbol)) {
      indent++; indent++; u8_puts(out,",@"); x = KNO_CAR(KNO_CDR(x));}
    else if (KNO_EQ(car,comment_symbol)) {
      u8_puts(out,"#;"); indent = indent+2; x = KNO_CAR(KNO_CDR(x));}}
  /* Special compound printers for different types. */
  if (PAIRP(x)) {
    lispval car = KNO_CAR(x), scan = x;
    lispval rule = get_pprint_rule(car,ppcxt);
    int list_max = pprint_max3(list_max,maxelts,ppcxt);
    int n_elts = 0;
    u8_putc(out,'('); col++; indent++;
    int base_col = col;
    while (PAIRP(scan)) {
      if (OVERFLOWP(n_elts,list_max)) {
        lispval probe_cdr = KNO_CDR(scan);
        if (NILP(probe_cdr)) {
          /* If there's just one more element and list_max is > 3,
             display it anyway. */}
        else {
          U8_STATIC_OUTPUT(ellipsis,80);
          int remaining=0; while (KNO_PAIRP(scan)) {
            remaining++; scan=KNO_CDR(scan);}
          u8_printf(&ellipsis,"#|…%s list with %d more elements…|#",
                    remaining,((NILP(scan)) ? ("normal") : ("improper")));
          size_t ellipsis_len = ellipsis.u8_write-ellipsis.u8_outbuf;
          if ( ( (col+ellipsis_len) > maxcol ) &&
               ( col > (indent+margin_len)) )
            col=do_indent(out,margin,indent,-1);
          u8_putn(out,ellipsis.u8_outbuf,ellipsis_len);
          col=col+ellipsis_len;
          scan=NIL;
          break;}}
      lispval head = KNO_CAR(scan);
      col = kno_pprinter(out,head,indent,col,depth,
                         customfn,customdata,
                         ppcxt);
      scan = KNO_CDR(scan);
      if (n_elts == 0) {
        if ( (KNO_SYMBOLP(head)) &&
             ( (KNO_TRUEP(rule)) || ( (col-base_col) < 5) ) )
          indent=indent+(col-base_col)+1;
        else if ( (KNO_UINTP(rule)) && (rule < (KNO_INT(17)) ) ) {
          int rel_indent = KNO_FIX2INT(rule);
          if (rel_indent > (col-base_col)+1)
            indent = indent + (col-base_col) + 1;
          else indent = indent + rel_indent;}
        else {}}
      n_elts++;
      if ( (PAIRP(scan)) && (col>base_col)) { u8_putc(out,' '); col++; }}
    if (NILP(scan)) {
      u8_putc(out,')');
      return col+1;}
    else {
      startoff = out->u8_write-out->u8_outbuf;
      u8_puts(out," . ");
      unparse(out,scan,ppcxt);
      u8_putc(out,')');
      n_chars = u8_strlen(out->u8_outbuf+startoff);
      if (col+n_chars>maxcol) {
        int i = indent;
        out->u8_write = out->u8_outbuf+startoff;
        out->u8_outbuf[startoff]='\0';
        u8_putc(out,'\n');
        if (margin) u8_puts(out,margin);
        while (i>0) {u8_putc(out,' '); i--;}
        u8_puts(out,". ");
        col = indent+2+((margin) ? (u8_strlen(margin)) : (0));
        col = kno_pprinter(out,scan,indent+2,col,depth+1,
                           customfn,customdata,
                           ppcxt);
        u8_putc(out,')');
        return col+1;}
      else return col+n_chars;}}
  else if (VECTORP(x)) {
    int len = VEC_LEN(x);
    int vec_max = pprint_max3(vector_max,maxelts,ppcxt);
    if (len==0) {
      u8_puts(out,"#()");
      return col+3;}
    else {
      int eltno = 0, base_col = col = col+2;
      u8_puts(out,"#(");
      while (eltno<len) {
        if (OVERFLOWP(eltno,vec_max)) {
          if ( (eltno+1 == len) && (vec_max>3) ) {
            /* If there's one more item and the vector is 'short',
               just display it */}
          else {
            U8_STATIC_OUTPUT(ellipsis,80);
            int remaining=len-eltno;
            u8_printf(&ellipsis,"#|…%d more elements…|#",remaining);
            size_t ellipsis_len = ellipsis.u8_write-ellipsis.u8_outbuf;
            if ( ( (col+ellipsis_len) > maxcol ) &&
                 ( col > (indent+margin_len)) )
              col=do_indent(out,margin,indent,-1);
            u8_putn(out,ellipsis.u8_outbuf,ellipsis_len);
            col=col+ellipsis_len;
            break;}}
        if (col > base_col) {
          u8_putc(out,' '); col++;}
        col = kno_pprinter(out,VEC_REF(x,eltno),indent+2,col,depth+1,
                           customfn,customdata,
                           ppcxt);
        eltno++;}
      u8_putc(out,')');
      return col+1;}}
  else if (KNO_COMPOUND_VECTORP(x)) {
    struct KNO_COMPOUND *comp = (kno_compound) x;
    int len = KNO_COMPOUND_VECLEN(x);
    int vec_max = pprint_max3(vector_max,maxelts,ppcxt);
    U8_SUB_STREAM(tmp,200,out);
    tmp.u8_streaminfo|=U8_STREAM_HUMANE;
    u8_printf(tmpout,"#%%(%q)",comp->typetag);
    size_t head_len = tmp.u8_write-tmp.u8_outbuf;
    if (len==0) {
      u8_putn(out,tmp.u8_outbuf,head_len);
      if (tmp.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmp.u8_outbuf);
      return col+head_len;}
    else {
      int eltno = 0, base_col = col+4;
      u8_putn(out,tmp.u8_outbuf,head_len-1); /* head_len-1 skips the close paren */
      if (tmp.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(tmp.u8_outbuf);
      col=col+head_len;
      while (eltno<len) {
        if (OVERFLOWP(eltno,vec_max)) {
          if ( (eltno+1 == len) && (vec_max>3) ) {
            /* If there's one more item and the vector is 'short',
               just display it */}
          else {
            U8_STATIC_OUTPUT(ellipsis,80);
            int remaining=len-eltno;
            u8_printf(&ellipsis,"#|…%d more elements…|#",remaining);
            size_t ellipsis_len = ellipsis.u8_write-ellipsis.u8_outbuf;
            if ( ( (col+ellipsis_len) > maxcol ) &&
                 ( col > (indent+margin_len)) )
              col=do_indent(out,margin,indent,-1);
            u8_putn(out,ellipsis.u8_outbuf,ellipsis_len);
            col=col+ellipsis_len;
            break;}}
        if (col > base_col) {
          u8_putc(out,' '); col++;}
        col = kno_pprinter(out,KNO_XCOMPOUND_VECREF(x,eltno),indent+4,col,depth+1,
                           customfn,customdata,
                           ppcxt);
        eltno++;
        if (eltno < len) {
          u8_putc(out,' '); col++;}}
      u8_putc(out,')');
      return col+1;}}
  else if (QCHOICEP(x)) {
    struct KNO_QCHOICE *qc = KNO_XQCHOICE(x);
    int n_elts=0, choice_max = pprint_max3(choice_max,maxelts,ppcxt);
    if (EMPTYP(qc->qchoiceval)) {
      u8_puts(out,"#{}");
      return col+3;}
    else {
      int base_col = col = col+2;
      u8_puts(out,"#{");
      DO_CHOICES(elt,qc->qchoiceval) {
        if ( (n_elts > 0) && (col > base_col) ) {
          u8_putc(out,' '); col++;}
        if (OVERFLOWP(n_elts,choice_max)) {}
        else col = kno_pprinter(out,elt,indent+2,col+2,depth+1,
                                customfn,customdata,
                                ppcxt);
        n_elts++;}
      u8_putc(out,'}');
      return col+1;}}
  else if (KNO_CHOICEP(x)) {
    int choice_max = pprint_max3(choice_max,maxelts,ppcxt);
    int n_elts = 0, n_choices=KNO_CHOICE_SIZE(x);
    int base_col = col = col+1;
    u8_putc(out,'{');
    DO_CHOICES(elt,x) {
      if (col > base_col) u8_putc(out,' ');
      if (OVERFLOWP(n_elts,choice_max)) {
        if (n_elts+1 == n_choices)  {
          /* If there's one more item and the vector is 'short',
             just display it */}
        else {
          U8_STATIC_OUTPUT(ellipsis,80);
          int remaining=n_choices-n_elts;
          u8_printf(&ellipsis,"#|…%d more choices…|#",remaining);
          size_t ellipsis_len = ellipsis.u8_write-ellipsis.u8_outbuf;
          if ( ( (col+ellipsis_len) > maxcol ) &&
               ( col > (indent+margin_len)) )
            col=do_indent(out,margin,indent,-1);
          u8_putn(out,ellipsis.u8_outbuf,ellipsis_len);
          col=col+ellipsis_len;
          break;}}
      col = kno_pprinter(out,elt,indent+1,col+1,depth+1,
                         customfn,customdata,
                         ppcxt);
      n_elts++;}
    u8_putc(out,'}');
    return col+1;}
  else if (KNO_PRECHOICEP(x)) {
    lispval simple = kno_make_simple_choice(x);
    int rv = kno_pprinter(out,simple,indent,col,depth,
                          customfn,customdata,
                          ppcxt);
    kno_decref(simple);
    return rv;}
  else if ( (SLOTMAPP(x)) || (SCHEMAPP(x)) ) {
    lispval keys=kno_getkeys(x); int off = 0;
    if (PRECHOICEP(keys)) keys=kno_simplify_choice(keys);
    if (EMPTYP(keys)) {
      int at_start = ! (col>(margin_len+indent));
      if (at_start) off++;
      if (! (KNO_SCHEMAPP(x)) ) off++;
      u8_printf(out,"%s%s[]",
                (at_start) ? (" ") : (""),
                (KNO_SCHEMAPP(x)) ? ("") : ("#"));
      return col+off+2;}
    if (col>(margin_len+indent))
      col=do_indent(out,margin,indent,-1);
    if (KNO_SCHEMAPP(x)) {
      off = 1; u8_putc(out,'[');}
    else {
      off = 2; u8_puts(out,"#[");}
    if (!(CHOICEP(keys)))
      col=kno_pprint_table(out,x,&keys,1,
                           indent+off,col+off,depth+1,
                           customfn,customdata,
                           ppcxt);
    else col=kno_pprint_table
           (out,x,KNO_CHOICE_DATA(keys),KNO_CHOICE_SIZE(keys),
            indent+off,col+off,depth+1,
            customfn,customdata,
            ppcxt);
    u8_putc(out,']');
    kno_decref(keys);
    return col+1;}
  else {
    int startoff = out->u8_write-out->u8_outbuf;
    unparse(out,x,ppcxt);
    n_chars = u8_strlen(out->u8_outbuf+startoff);
    return margin_len+indent+n_chars;}
}

/* Static support */

static int do_indent(u8_output out,u8_string margin,int indent,
                     int startoff)
{
  u8_byte *start;
  if (startoff>=0) {
    out->u8_write = out->u8_outbuf+startoff;
    out->u8_outbuf[startoff]='\0';}
  u8_putc(out,'\n');
  start=out->u8_write;
  if (margin) u8_puts(out,margin);
  int i=0; while (i<indent) { u8_putc(out,' '); i++;}
  return out->u8_write-start;
}

static void do_reset(u8_output out,int startoff)
{
  out->u8_write=out->u8_outbuf+startoff;
}

static lispval get_pprint_rule(lispval car,pprint_context ppcxt)
{
  lispval rule=VOID;
  if (ppcxt->pp_rules)
    rule = kno_get(ppcxt->pp_rules,car,KNO_VOID);
  else if (pprint_default_rules)
    rule = kno_get(pprint_default_rules,car,KNO_VOID);
  else {}
  if (KNO_FIXNUMP(rule)) {}
  else if (KNO_SYMBOLP(rule)) {
    lispval new_rule= (ppcxt->pp_rules) ?
      (kno_get(ppcxt->pp_rules,rule,KNO_VOID)) :
      (KNO_VOID);
    if (KNO_AGNOSTICP(new_rule))
      new_rule=kno_get(pprint_default_rules,rule,KNO_VOID);
    if (KNO_AGNOSTICP(new_rule)) {
      kno_decref(rule); rule=new_rule;}
    else kno_decref(new_rule);}
  else if (KNO_TRUEP(rule)) {}
  else if ( (KNO_SYMBOLP(car)) ||
            (KNO_FCNIDP(car)) ||
            (KNO_TYPEP(car,kno_cprim_type)) ||
            (KNO_TYPEP(car,kno_lambda_type)) ||
            (KNO_TYPEP(car,kno_evalfn_type)) )
    rule=KNO_INT2FIX(3);
  else rule=KNO_FIXZERO;
  return rule;
}

/* Flat unparsing */

static int escape_char(u8_output out,int c)
{
  switch (c) {
  case '"': u8_puts(out,"\\\""); return 2;
  case '\\': u8_puts(out,"\\\\"); return 2;
  case '\n': u8_puts(out,"\\n"); return 2;
  case '\t': u8_puts(out,"\\t"); return 2;
  case '\r': u8_puts(out,"\\r"); return 2;
  default:
    if (iscntrl(c)) {
      char buf[32]; sprintf(buf,"\\%03o",c);
      u8_puts(out,buf);
      return 4;}
    else return u8_putc(out,c);}
}

static int unparse(u8_output out,lispval obj,pprint_context ppcxt)
{
  if (STRINGP(obj)) {
    size_t len=KNO_STRLEN(obj);
    int max_chars = pprint_max(maxchars,ppcxt);
    int n_bytes = 2; /* Open and close '"' */
    if (max_chars >= 0) {
      int n_chars=0;
      u8_string scan = CSTRING(obj), limit = scan+len;
      u8_putc(out,'"');
      while ( (scan < limit) && (n_chars < max_chars) ) {
        u8_string chunk = scan;
        while ((scan < limit) &&
               (n_chars < max_chars) &&
               (*scan != '"') && (*scan != '\\') &&
               (!(iscntrl(*scan)))) {
          n_chars++;
          u8_sgetc(&scan);}
        n_bytes += scan-chunk;
        u8_putn(out,chunk,scan-chunk);
        if (*scan) {
          n_bytes += escape_char(out,*scan);
          n_chars++;
          scan++;}}
      if (scan==limit) {
        u8_putc(out,'"');
        return n_bytes+1;}
      else {
        int total_chars=n_chars;
        while (scan<limit) {total_chars++; u8_sgetc(&scan);}
        U8_FIXED_OUTPUT(tmp,256);
	tmp.u8_streaminfo|=U8_STREAM_HUMANE;
        u8_printf(tmpout,"… %d/%d chars …\"",
                  total_chars-n_chars,total_chars);
        u8_putn(out,tmp.u8_outbuf,tmp.u8_write-tmp.u8_outbuf);
        n_bytes += (tmp.u8_write-tmp.u8_outbuf);
        return n_bytes;}}
    else return kno_unparse(out,obj);}
  else if (PACKETP(obj)) {
    if (KNO_SECRETP(obj))
      return kno_unparse(out,obj);
    else {
      struct KNO_STRING *s = (kno_string) obj;
      int max_bytes = pprint_max3(maxbytes,maxchars,ppcxt);
      const unsigned char *bytes = s->str_bytes;
      int i = 0, len = s->str_bytelen;
      if ( ( max_bytes > 0 ) && ( len > max_bytes ) ) {
        unsigned char hashbuf[16], *hash;
        u8_printf(out,"#~\"%d:",len);
        hash = u8_md5(bytes,len,hashbuf);
        while (i<16) {u8_printf(out,"%02x",hash[i]); i++;}
        return u8_puts(out,"\"");}
      else return kno_unparse(out,obj);}}
  else return kno_unparse(out,obj);
}

/* Printing tables */

KNO_EXPORT
int kno_pprint_table(u8_output out,lispval x,
                     const lispval *keys,size_t n_keys,
                     int indent,int col,int depth,
                     kno_pprintfn customfn,void *customdata,
                     pprint_context ppcxt)
{
  if (n_keys==0) return col;
  const lispval *scan=keys, *limit=scan+n_keys;
  int maxcol = pprint_max(maxcol,ppcxt);
  int maxkeys = pprint_max(maxkeys,ppcxt);
  u8_string margin = (ppcxt)? (ppcxt->pp_margin) : (pprint_margin);
  size_t margin_len = (ppcxt) ? (ppcxt->pp_margin_len) :
    (margin) ? (strlen(margin)) : (0);
  int count=0;
  while (scan<limit) {
    lispval key = *scan++;
    lispval val = kno_get(x,key,VOID);
    if (VOIDP(val)) continue;
    else if (count) {
      int i = indent;
      u8_putc(out,'\n'); col=0;
      if (margin) { u8_puts(out,margin); col+=margin_len; }
      while (i>0) { u8_putc(out,' '); col++; i--; }}
    else {}
    if (OVERFLOWP(count,maxkeys)) {
      U8_STATIC_OUTPUT(ellipsis,80);
      int remaining=limit-scan;
      u8_printf(&ellipsis,"#|…%d more keys…|#",remaining);
      size_t ellipsis_len = ellipsis.u8_write-ellipsis.u8_outbuf;
      if ( ( (col+ellipsis_len) > maxcol ) &&
           ( col > (indent+margin_len)) )
        col=do_indent(out,margin,indent,-1);
      u8_putn(out,ellipsis.u8_outbuf,ellipsis_len);
      col=col+ellipsis_len;
      break;}
    int newcol = output_keyval(out,key,val,col,ppcxt);
    if (newcol>=0) {
      /* Key + value fit on one line */
      col=newcol;
      count++;
      kno_decref(val);
      continue;}
    else count++;
    if (SYMBOLP(key)) {
      u8_string pname = SYM_NAME(key);
      if (*pname=='\0') {
	u8_puts(out,"||");
	col=col+2;}
      else {
	u8_puts(out,pname);
	col=col+strlen(pname);}}
    else if (STRINGP(key)) {
      u8_putc(out,'"');
      u8_puts(out,CSTRING(key));
      u8_putc(out,'"');
      col=col+2+strlen(CSTRING(key));}
    else {
      U8_SUB_STREAM(tmp,512,out);
      tmp.u8_streaminfo|=U8_STREAM_HUMANE;
      unparse(&tmp,key,ppcxt);
      u8_puts(out,tmp.u8_outbuf);
      col=col+(tmp.u8_write-tmp.u8_outbuf);
      u8_close_output(&tmp);}
    /* Output value */
    U8_SUB_STREAM(tmp,512,out);
    tmp.u8_streaminfo|=U8_STREAM_HUMANE;
    unparse(&tmp,val,ppcxt);
    size_t len=tmp.u8_write-tmp.u8_outbuf;
    /* Output the margin, indent, etc */
    int value_indent = indent+2, i=0;
    u8_putc(out,'\n'); col=0;
    if (margin) { u8_puts(out,margin); col+=margin_len; }
    while (i<value_indent) {u8_putc(out,' '); i++;}
    col += value_indent;
    if ((col+len) < maxcol) {
      u8_putn(out,tmp.u8_outbuf,len);
      u8_close_output(&tmp);
      col += len;
      kno_decref(val);
      continue;}
    u8_close_output(&tmp);
    col=kno_pprinter(out,val,value_indent,col,depth+1,
                     customfn,customdata,
                     ppcxt);
    kno_decref(val);}
  return col;
}

static int output_keyval(u8_output out,
                         lispval key,lispval val,
                         int col,pprint_context ppcxt)
{
  ssize_t len = 0;
  int maxcol = ppcxt->pp_maxcol;
  if (STRINGP(key))
    len = len+STRLEN(key)+3;
  else if (SYMBOLP(key)) {
    u8_string pname = SYM_NAME(key);
    if (*pname) len += strlen(pname)+1;
    else len += 2;}
  else if (CONSP(key)) return -1;
  if (STRINGP(val))
    len = len+STRLEN(val)+2;
  else if (SYMBOLP(val)) {
    u8_string pname = SYM_NAME(val);
    if (*pname) len += strlen(pname)+1;
    else len += 2;}
  else {}
  if ((col+len)>maxcol)
    return -1;
  else len=0;
  /* Try it out */
  U8_SUB_STREAM(kv,512,out);
  kv.u8_streaminfo |= U8_HUMAN_OUTPUT;
  unparse(kvout,key,ppcxt);
  u8_putc(kvout,' ');
  unparse(kvout,val,ppcxt);
  len=kv.u8_write-kv.u8_outbuf;
  if ((kv.u8_streaminfo&U8_STREAM_OVERFLOW)||((col+(len))>maxcol))
    len=-1;
  else u8_putn(out,kv.u8_outbuf,len);
  u8_close_output(kvout);
  if (len>=0)
    return col+len;
  else return -1;
}

/* Wrappers */

KNO_EXPORT
int kno_pprint(u8_output out,lispval x,u8_string margin,
               int indent,int col,int maxcol)
{
  return kno_pprint_x(out,x,margin,indent,col,maxcol,NULL,NULL);
}

KNO_EXPORT
int kno_pprint_x(u8_output out,lispval x,u8_string margin,
                 int indent,int col,int maxcol,
                 kno_pprintfn customfn,void *customdata)
{
  struct PPRINT_CONTEXT ppcxt={0};
  ppcxt.pp_margin = margin;
  ppcxt.pp_margin_len = (margin) ? (strlen(margin)) : (0);
  ppcxt.pp_maxcol = maxcol;
  return kno_pprinter(out,x,indent,col,0,customfn,customdata,&ppcxt);
}

/* printf handler for pprint */

static u8_string lisp_pprintf_handler
(u8_output out,char *cmd,u8_byte *buf,int bufsiz,va_list *args)
{
  struct U8_OUTPUT tmpout;
  int width = 80; lispval value;
  if (strchr(cmd,'*'))
    width = va_arg(*args,int);
  else {
    width = strtol(cmd,NULL,10);
    U8_CLEAR_ERRNO();}
  value = va_arg(*args,lispval);
  U8_INIT_OUTPUT_BUF(&tmpout,bufsiz,buf);
  tmpout.u8_streaminfo |=
    (out->u8_streaminfo&(U8_SUB_STREAM_MASK)) |
    U8_HUMAN_OUTPUT;
  kno_pprint(&tmpout,value,NULL,0,0,width);
  u8_puts(out,tmpout.u8_outbuf);
  u8_close_output(&tmpout);
  if (strchr(cmd,'-')) kno_decref(value);
  return NULL;
}

KNO_EXPORT void kno_init_pprint_c()
{
  u8_register_source_file(_FILEINFO);

  u8_printf_handlers['Q']=lisp_pprintf_handler;

  quasiquote_symbol = kno_intern("quasiquote");
  unquote_symbol = kno_intern("unquote");
  unquote_star_symbol = kno_intern("unquote*");
  comment_symbol = kno_intern("comment");

  pprint_default_rules = kno_make_hashtable(NULL,200);
}

