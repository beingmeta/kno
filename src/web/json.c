/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/eval.h"
#include "framerd/tables.h"
#include "framerd/ports.h"
#include "framerd/fdweb.h"

#include <libu8/u8xfiles.h>

#include <ctype.h>
#define FD_JSON_ANYKEY    2  /* Allow compound table keys */
#define FD_JSON_IDKEY     4  /* Allow raw identifiers as table keys */
#define FD_JSON_SYMBOLIZE 8  /* Convert table keys to symbols (and vice versa) */
#define FD_JSON_COLONIZE 16  /* Use (assume) colon prefixes for LISPY objects */
#define FD_JSON_TICKS    32  /* Show time as unix time */
#define FD_JSON_TICKLETS 64  /* Show time as nanosecond-precision unix time */
#define FD_JSON_VERBOSE 128 /* Emit conversion warnings, etc */
#define FD_JSON_STRICT  256 /* Emit conversion warnings, etc */
#define FD_JSON_DEFAULTS (FD_JSON_SYMBOLIZE)

#define FD_JSON_MAXFLAGS 256

static u8_condition JSON_Error="JSON Parsing Error";

static int readc(U8_INPUT *in)
{
  int c = u8_getc(in);
  if (c=='\\') {
    u8_ungetc(in,c);
    return fd_read_escape(in);}
  else return c;
}

static int skip_whitespace(U8_INPUT *in)
{
  int c = u8_getc(in);
  while ((c>0) && (u8_isspace(c))) c = u8_getc(in);
  if (c>0) u8_ungetc(in,c);
  return c;
}

static lispval parse_error(u8_output out,lispval result,int report)
{
  fd_clear_errors(report);
  fd_decref(result);
  return fd_make_string(NULL,out->u8_write-out->u8_outbuf,
                        out->u8_outbuf);
}

static lispval convert_value(lispval fn,lispval val,int free,int warn)
{
  if (FD_ABORTP(val)) return val;
  else if (FD_ABORTP(fn)) return val;
  else if (VECTORP(fn)) {
    lispval eltfn = VEC_REF(fn,0);
    if (VECTORP(val)) {
      lispval results = EMPTY;
      lispval *elts = VEC_DATA(val);
      int i = 0, lim = VEC_LEN(val);
      while (i<lim) {
        lispval cval = convert_value(eltfn,elts[i],0,warn);
        if (FD_ABORTP(cval)) {
          fd_clear_errors(warn); fd_incref(elts[i]);
          CHOICE_ADD(results,elts[i]);}
        else if (VOIDP(cval)) {
          fd_incref(elts[i]);
          CHOICE_ADD(results,elts[i]);}
        else {
          CHOICE_ADD(results,cval);}
        i++;}
      fd_decref(val);
      return results;}
    else return convert_value(eltfn,val,1,warn);}
  else if (FD_APPLICABLEP(fn)) {
    lispval converted = fd_apply(fn,1,&val);
    if (VOIDP(converted)) return val;
    else if (FD_ABORTP(converted)) {
      fd_clear_errors(warn);
      return val;}
    else {
      fd_decref(val);
      return converted;}}
  else if ((FD_TRUEP(fn))&&(STRINGP(val))) {
    lispval parsed = fd_parse(CSTRING(val));
    if (FD_ABORTP(parsed)) {
      fd_clear_errors(warn);
      return val;}
    else {
      fd_decref(val); return parsed;}}
  else return val;
}

static lispval json_parse(U8_INPUT *in,int flags,lispval fieldmap);
static lispval json_vector(U8_INPUT *in,int flags,lispval fieldmap);
static lispval json_table(U8_INPUT *in,int flags,lispval fieldmap);
static lispval json_string(U8_INPUT *in,int flags);
static lispval json_atom(U8_INPUT *in,int flags);

static lispval json_parse(U8_INPUT *in,int flags,lispval fieldmap)
{
  int c = skip_whitespace(in);
  if (c=='[') return json_vector(in,flags,fieldmap);
  else if (c=='{') return json_table(in,flags,fieldmap);
  else if (c=='"') return json_string(in,flags);
  else if (c=='\'') {
    if ( (flags) & (FD_JSON_STRICT) )
      return fd_err("BadJSON","json_parse",
                    "unexpected ' in strict mode",
                    FD_VOID);
    else return json_string(in,flags);}
  else return json_atom(in,0);
}

static lispval json_atom(U8_INPUT *in,int flags)
{
  lispval result;
  struct U8_OUTPUT out; u8_byte _buf[256]; int c = readc(in);
  U8_INIT_STATIC_OUTPUT_BUF(out,256,_buf);
  while ((u8_isalnum(c)) || (c=='-') || (c=='_') || (c=='+') || (c=='.')) {
    u8_putc(&out,c); c = readc(in);}
  if (c>=0) u8_ungetc(in,c);
  if ((strcmp(out.u8_outbuf,"true")==0)) result = FD_TRUE;
  else if ((strcmp(out.u8_outbuf,"false")==0)) result = FD_FALSE;
  else if  ((strcmp(out.u8_outbuf,"null")==0)) result = EMPTY;
  else result = fd_parse(out.u8_outbuf);
  if (FD_ABORTP(result))
    result = parse_error(&out,result,flags&FD_JSON_VERBOSE);
  if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
  return result;
}

static lispval json_string(U8_INPUT *in,int flags)
{
  struct U8_OUTPUT out;
  int terminator = readc(in); /* Save '"/\'' */
  int c = u8_getc(in);
  int raw_string = 0;
  U8_INIT_OUTPUT(&out,32);
  if ( (c=='\\') && (flags&FD_JSON_COLONIZE) ) {
    c = u8_getc(in);
    if (c==':') raw_string = 1;
    u8_putc(&out,c);
    c = u8_getc(in);}
  while (c>=0) {
    if (c == terminator) break;
    else if (c=='\\') {
      c = fd_read_escape(in);
      u8_putc(&out,c);
      c = u8_getc(in);
      continue;}
    u8_putc(&out,c);
    c = u8_getc(in);}
  if (raw_string)
    return fd_stream2string(&out);
  else if ((flags&FD_JSON_COLONIZE)&&(out.u8_outbuf[0]==':')) {
    lispval result = fd_parse(out.u8_outbuf+1);
    if (FD_ABORTP(result))
      result = parse_error(&out,result,flags&FD_JSON_VERBOSE);
    if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
    return result;}
  else return fd_stream2string(&out);
}

static lispval json_intern(U8_INPUT *in,int flags)
{
  struct U8_OUTPUT out; int c = readc(in); /* Skip '"' */
  int good_symbol = 1;
  c = u8_getc(in);
  U8_INIT_OUTPUT(&out,64);
  while (c>=0) {
    if (c=='"') break;
    else if (c=='\\') {
      c = fd_read_escape(in);
      u8_putc(&out,c); c = u8_getc(in);
      continue;}
    else if ((u8_isspace(c))||(c=='/')||(c=='.')) good_symbol = 0;
    u8_putc(&out,c);
    c = u8_getc(in);}
  if (out.u8_outbuf[0]==':') {
    lispval result = fd_parse(out.u8_outbuf+1);
    if (FD_ABORTP(result))
      result = parse_error(&out,result,flags&FD_JSON_VERBOSE);
    if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
    return result;}
  else {
    lispval result = (((good_symbol)&&(out.u8_write-out.u8_outbuf))?
                   (fd_parse(out.u8_outbuf)):
                   (fd_stream_string(&out)));
    if (FD_ABORTP(result))
      result = parse_error(&out,result,flags&FD_JSON_VERBOSE);
    if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
    return result;}
}

static lispval json_key(U8_INPUT *in,int flags,lispval fieldmap)
{
  int c = skip_whitespace(in);
  if (c=='"')
    if (flags&FD_JSON_SYMBOLIZE)
      return json_intern(in,flags);
    else if (VOIDP(fieldmap))
      return json_string(in,flags);
    else {
      lispval stringkey = json_string(in,flags);
      lispval mapped = fd_get(fieldmap,stringkey,VOID);
      if (VOIDP(mapped)) return stringkey;
      else {
        fd_decref(stringkey);
        return mapped;}}
  else if (((c=='{')||(c=='['))&&(!(flags&FD_JSON_ANYKEY))) {
    fd_seterr("Invalid JSON key","json_key",NULL,VOID);
    return FD_PARSE_ERROR;}
  else if ((!(flags&FD_JSON_IDKEY))&&(!((u8_isdigit(c))||(c=='+')||(c=='-')))) {
    fd_seterr("Invalid JSON key","json_key",NULL,VOID);
    return FD_PARSE_ERROR;}
  else {
    lispval result = json_atom(in,flags);
    if ((FIXNUMP(result))||(FD_BIGINTP(result))||(FD_FLONUMP(result)))
      return result;
    else if ((SYMBOLP(result))&&(flags&FD_JSON_IDKEY))
      return result;
    else if (flags&FD_JSON_ANYKEY)
      return result;
    else {
      fd_decref(result);
      fd_seterr("Invalid JSON key","json_key",NULL,VOID);
      return FD_PARSE_ERROR;}}
}

static lispval json_vector(U8_INPUT *in,int flags,lispval fieldmap)
{
  int n_elts = 0, max_elts = 16, c, i;
  unsigned int good_pos = in->u8_read-in->u8_inbuf;
  lispval *elts;
  if (u8_getc(in)!='[') return FD_ERROR;
  else elts = u8_alloc_n(16,lispval);
  c = skip_whitespace(in);
  while (c>=0) {
    good_pos = in->u8_read-in->u8_inbuf;
    if (c==']') {
      u8_getc(in); /* Absorb ] */
      return fd_wrap_vector(n_elts,elts);}
    else if (c==',') {
      c = u8_getc(in); c = skip_whitespace(in);}
    else {
      lispval elt;
      if (n_elts == max_elts)  {
        int new_max = max_elts*2;
        lispval *newelts = u8_realloc(elts,LISPVEC_BYTELEN(new_max));
        if (newelts) {elts = newelts; max_elts = new_max;}
        else {
          u8_seterr(fd_MallocFailed,"json_vector",NULL);
          break;}}
      elts[n_elts++]=elt = json_parse(in,flags,fieldmap);
      if (FD_ABORTP(elt)) break;
      c = skip_whitespace(in);}}
  i = 0; while (i<n_elts) {fd_decref(elts[i]); i++;}
  return fd_err(JSON_Error,"json_vector",
                u8_strdup(in->u8_inbuf+good_pos),
                VOID);
}

static lispval json_table(U8_INPUT *in,int flags,lispval fieldmap)
{
  int n_elts = 0, max_elts = 16, c, i;
  unsigned int good_pos = in->u8_read-in->u8_inbuf;
  struct FD_KEYVAL *kv;
  if (u8_getc(in)!='{') return FD_ERROR;
  else kv = u8_alloc_n(16,struct FD_KEYVAL);
  c = skip_whitespace(in);
  while (c>=0) {
    good_pos = in->u8_read-in->u8_inbuf;
    if (c=='}') {
      u8_getc(in); /* Absorb ] */
      return fd_init_slotmap(NULL,n_elts,kv);}
    else if (c==',') {
      c = u8_getc(in); c = skip_whitespace(in);}
    else {
      if (n_elts == max_elts)  {
        int new_max = max_elts*2;
        struct FD_KEYVAL *newelts=
          u8_realloc(kv,FD_KEYVAL_LEN*new_max);
        if (newelts) {kv = newelts; max_elts = new_max;}
        else {
          u8_seterr(fd_MallocFailed,"json_table",NULL);
          break;}}
      kv[n_elts].kv_key = json_key(in,flags,fieldmap);
      if (FD_ABORTP(kv[n_elts].kv_key)) break;
      c = skip_whitespace(in);
      if (c==':') c = u8_getc(in);
      else return FD_EOD;
      if ((VOIDP(fieldmap))||(CONSP(kv[n_elts].kv_key)))
        kv[n_elts].kv_val = json_parse(in,flags,fieldmap);
      else {
        lispval handler = fd_get(fieldmap,kv[n_elts].kv_key,VOID);
        if (VOIDP(handler))
          kv[n_elts].kv_val = json_parse(in,flags,fieldmap);
        else
          kv[n_elts].kv_val =
            convert_value(handler,json_parse(in,flags,fieldmap),
                          1,(flags&FD_JSON_VERBOSE));}
      if (FD_ABORTP(kv[n_elts].kv_val)) break;
      n_elts++; c = skip_whitespace(in);}}
  i = 0; while (i<n_elts) {
    fd_decref(kv[i].kv_key); fd_decref(kv[i].kv_val); i++;}
  u8_free(kv);
  return fd_err(JSON_Error,"json_table",in->u8_inbuf+good_pos,VOID);
}

static lispval symbolize_symbol, colonize_symbol, rawids_symbol;
static lispval ticks_symbol, ticklets_symbol, verbose_symbol, strict_symbol;

static int get_json_flags(lispval flags_arg)
{
  if (FALSEP(flags_arg))
    return 0;
  else if (FIXNUMP(flags_arg)) {
    long long val=FIX2INT(flags_arg);
    if ((val>0) && (val<FD_JSON_MAXFLAGS))
      return (unsigned int) val;
    else return FD_JSON_DEFAULTS;}
  else if (FD_TRUEP(flags_arg))
    return FD_JSON_COLONIZE|FD_JSON_SYMBOLIZE;
  else if (flags_arg == FD_DEFAULT_VALUE)
    return FD_JSON_DEFAULTS;
  else if ((PAIRP(flags_arg))||(TABLEP(flags_arg))) {
    int flags=0;
    if (!(fd_testopt(flags_arg,symbolize_symbol,FD_FALSE)))
      flags |= FD_JSON_SYMBOLIZE;
    if (fd_testopt(flags_arg,colonize_symbol,VOID))
      flags |= FD_JSON_COLONIZE;
    if (!(fd_testopt(flags_arg,rawids_symbol,VOID)))
      flags |= FD_JSON_IDKEY;
    if (fd_testopt(flags_arg,ticks_symbol,VOID))
      flags |= FD_JSON_TICKS;
    if (fd_testopt(flags_arg,ticklets_symbol,VOID))
      flags |= FD_JSON_TICKLETS;
    if (fd_testopt(flags_arg,verbose_symbol,VOID))
      flags |= FD_JSON_VERBOSE;
    if (fd_testopt(flags_arg,strict_symbol,VOID))
      flags |= FD_JSON_STRICT;
    return flags;}
  else if (PRECHOICEP(flags_arg)) {
    lispval choice=fd_make_simple_choice(flags_arg);
    int rv=get_json_flags(choice);
    fd_decref(choice);
    return rv;}
  else if ( (CHOICEP(flags_arg)) || (SYMBOLP(flags_arg)) ) {
    int flags=0;
    if (fd_overlapp(symbolize_symbol,flags_arg))
      flags |= FD_JSON_SYMBOLIZE;
    if (fd_overlapp(colonize_symbol,flags_arg))
      flags |= FD_JSON_COLONIZE;
    if (fd_overlapp(rawids_symbol,flags_arg))
      flags |= FD_JSON_IDKEY;
    if (fd_overlapp(ticks_symbol,flags_arg))
      flags |= FD_JSON_TICKS;
    if (fd_overlapp(ticklets_symbol,flags_arg))
      flags |= FD_JSON_TICKLETS|FD_JSON_TICKS;
    if (fd_overlapp(verbose_symbol,flags_arg))
      flags |= FD_JSON_VERBOSE;
    return flags;}
  else return FD_JSON_DEFAULTS;
}

static lispval jsonparseprim(lispval in,lispval flags_arg,lispval fieldmap)
{
  unsigned int flags = get_json_flags(flags_arg);
  if (FD_PORTP(in)) {
    struct FD_PORT *p = fd_consptr(struct FD_PORT *,in,fd_port_type);
    U8_INPUT *in = p->port_input;
    return json_parse(in,flags,fieldmap);}
  else if (STRINGP(in)) {
    struct U8_INPUT inport;
    U8_INIT_STRING_INPUT(&inport,STRLEN(in),CSTRING(in));
    return json_parse(&inport,flags,fieldmap);}
  else if (PACKETP(in)) {
    struct U8_INPUT inport;
    U8_INIT_STRING_INPUT(&inport,FD_PACKET_LENGTH(in),FD_PACKET_DATA(in));
    return json_parse(&inport,flags,fieldmap);}
  else return fd_type_error("string or stream","jsonparse",in);
}

/* JSON output */

static void json_unparse(u8_output out,lispval x,int flags,lispval oidfn,lispval slotfn,lispval miscfn);

static void json_escape(u8_output out,u8_string s)
{
  u8_string start = s, scan = start; int c; char buf[16];
  while ((c = (*scan))) {
    if ((c>=128)||(c=='"')||(c=='\\')||(iscntrl(c))) {
      if (scan>start) u8_putn(out,start,scan-start);
      u8_putc(out,'\\');
      switch (c) {
      case '\n': u8_putc(out,'n'); break;
      case '\b': u8_putc(out,'b'); break;
      case '\f': u8_putc(out,'f'); break;
      case '\r': u8_putc(out,'r'); break;
      case '\t': u8_putc(out,'t'); break;
      case '"': u8_putc(out,'"'); break;
      case '\\': u8_putc(out,'\\'); break;
      default:
        if (c>=128) {
          long uc = u8_sgetc(&scan);
          sprintf(buf,"u%04x",(unsigned int)uc);
          u8_puts(out,buf);
          start = scan; continue;}
        else {
          sprintf(buf,"u%04x",c);
          u8_puts(out,buf);
          break;}}
      scan++; start = scan;}
    else scan++;}
  if (scan>start) u8_putn(out,start,scan-start);
}

static void json_lower(u8_output out,u8_string s)
{
  const u8_byte *scan = s;
  int c = u8_sgetc(&scan);
  while (c>=0) {
    int lc = u8_tolower(c);
    if (lc>=128) u8_putc(out,lc);
    else switch (lc) {
      case '\n': u8_puts(out,"\\n"); break;
      case '\r': u8_puts(out,"\\r"); break;
      case '\t': u8_puts(out,"\\t"); break;
      case '\\': u8_puts(out,"\\\\"); break;
      case '"': u8_puts(out,"\\\""); break;
      default: u8_putc(out,lc);}
    c = u8_sgetc(&scan);}
}

static int json_slotval(u8_output out,lispval key,lispval value,int flags,
                        lispval slotfn,lispval oidfn,lispval miscfn)
{
  if (VOIDP(value)) return 0;
  else {
    lispval slotname = ((VOIDP(slotfn))?(VOID):(fd_apply(slotfn,1,&key)));
    if (VOIDP(slotname))
      if ((SYMBOLP(key))&&(flags&FD_JSON_SYMBOLIZE)) {
        u8_string pname = SYM_NAME(key);
        if (!(flags&FD_JSON_IDKEY)) u8_putc(out,'"');
        json_lower(out,pname);
        if (!(flags&FD_JSON_IDKEY)) u8_putc(out,'"');}
      else json_unparse(out,key,flags,oidfn,slotfn,miscfn);
    else if (STRINGP(slotname)) {
      u8_putc(out,'"');
      json_escape(out,CSTRING(slotname)); fd_decref(slotname);
      u8_putc(out,'"');}
    else if (SYMBOLP(slotname)) {
      json_escape(out,SYM_NAME(slotname));}
    else return 0;
    u8_puts(out,": ");
    json_unparse(out,value,flags,oidfn,slotfn,miscfn);
    return 1;}
}

static void json_unparse(u8_output out,lispval x,int flags,lispval slotfn,
                         lispval oidfn,lispval miscfn)
{
  if (FIXNUMP(x))
    u8_printf(out,"%lld",FIX2INT(x));
  else if (OIDP(x)) {
    lispval oidval = ((VOIDP(oidfn))?(VOID):
                   (fd_finish_call(fd_dapply(oidfn,1,&x))));
    if (VOIDP(oidval)) {
      FD_OID addr = FD_OID_ADDR(x);
      if (flags)
        u8_printf(out,"\":@%x/%x\"",FD_OID_HI(addr),FD_OID_LO(addr));
      else u8_printf(out,"\"@%x/%x\"",FD_OID_HI(addr),FD_OID_LO(addr));}
    else json_unparse(out,oidval,flags,slotfn,oidfn,miscfn);
    fd_decref(oidval);}
  else if ((SYMBOLP(x))&&(VOIDP(miscfn))) {
    u8_string pname = SYM_NAME(x);
    u8_puts(out,"\":");
    json_escape(out,pname);
    u8_puts(out,"\"");}
  else if (STRINGP(x)) {
    u8_string pstring = CSTRING(x);
    u8_puts(out,"\"");
    if (((flags)&(FD_JSON_COLONIZE))&&(pstring[0]==':')) {
      u8_putc(out,'\\'); u8_putc(out,'\\');}
    json_escape(out,pstring);
    u8_puts(out,"\"");}
  else if ((FD_BIGINTP(x))||(FD_FLONUMP(x)))
    fd_unparse(out,x);
  else if (VECTORP(x)) {
    int i = 0; int lim = VEC_LEN(x);
    if (lim==0) u8_putc(out,'[');
    else while (i<lim) {
        if (i>0) u8_putc(out,','); else u8_putc(out,'[');
        json_unparse(out,VEC_REF(x,i),flags,slotfn,oidfn,miscfn);
        i++;}
    u8_putc(out,']');}
  else if ((CHOICEP(x))||(PRECHOICEP(x))) {
    int elt_count = 0; DO_CHOICES(e,x) {
      if (elt_count>0) u8_putc(out,',');
      else u8_puts(out,"[");
      json_unparse(out,e,flags,slotfn,oidfn,miscfn);
      elt_count++;}
    u8_putc(out,']');}
  else if (TYPEP(x,fd_timestamp_type)) {
    struct FD_TIMESTAMP *tm=
      fd_consptr(struct FD_TIMESTAMP *,x,fd_timestamp_type);
    if (flags&FD_JSON_TICKS)
      if (tm->u8xtimeval.u8_tick<0)  u8_puts(out,"-1"); /* Invalid time */
      else if (flags&FD_JSON_TICKLETS) {
        double dtick = ((unsigned long long)tm->u8xtimeval.u8_tick)+
          (tm->u8xtimeval.u8_nsecs)*0.000000001;
        u8_printf(out,"%f",dtick);}
      else {
        unsigned long long llval = 
          (unsigned long long)(tm->u8xtimeval.u8_tick);
        u8_printf(out,"%llu",llval);}
    else if (flags&FD_JSON_COLONIZE)
      u8_printf(out,"\":#T%iSXGt\"",&(tm->u8xtimeval));
    else if (tm->u8xtimeval.u8_tick<0)  u8_puts(out,"\"invalid time\""); /* Invalid time */
    else u8_printf(out,"\"%iSXGt\"",&(tm->u8xtimeval));}
  else if (TYPEP(x,fd_uuid_type)) {
    struct FD_UUID *uuid = fd_consptr(struct FD_UUID *,x,fd_uuid_type);
    char buf[64]; u8_uuidstring((u8_uuid)(&(uuid->uuid16)),buf);
    if ((flags)&(FD_JSON_COLONIZE))
      u8_printf(out,"\":#U%s\"",buf);
    else u8_printf(out,"\"%s\"",buf);}
  else if (TABLEP(x)) {
    lispval keys = fd_getkeys(x);
    if (EMPTYP(keys)) u8_puts(out,"{}");
    else {
      int elt_count = 0; DO_CHOICES(key,keys) {
        lispval value = fd_get(x,key,VOID);
        if (!(VOIDP(value))) {
          if (elt_count>0) u8_putc(out,',');
          else u8_puts(out,"{");
          if (json_slotval(out,key,value,flags,slotfn,oidfn,miscfn)) elt_count++;
          fd_decref(value);}}
      u8_puts(out,"}");}}
  else if (EMPTYP(x)) u8_puts(out,"[]");
  else if (FD_TRUEP(x)) u8_puts(out,"true");
  else if (FALSEP(x)) u8_puts(out,"false");
  else {
    u8_byte buf[256]; struct U8_OUTPUT tmpout;
    lispval tval = ((VOIDP(miscfn))?(VOID):
                 (fd_finish_call(fd_dapply(miscfn,1,&x))));
    U8_INIT_STATIC_OUTPUT_BUF(tmpout,256,buf);
    tmpout.u8_streaminfo |= U8_STREAM_VERBOSE;
    if (VOIDP(tval))
      fd_unparse(&tmpout,x);
    else fd_unparse(&tmpout,tval);
    fd_decref(tval);
    if (flags) u8_puts(out,"\":"); else u8_putc(out,'"');
    json_escape(out,tmpout.u8_outbuf);
    u8_putc(out,'"');
    u8_close_output(&tmpout);}
}

static lispval jsonoutput(lispval x,lispval flags_arg,
                         lispval slotfn,lispval oidfn,lispval miscfn)
{
  u8_output out = u8_current_output;
  int flags = get_json_flags(flags_arg);
  if ((flags<0)||(flags>=FD_JSON_MAXFLAGS))
    return fd_type_error("fixnum/flags","jsonoutput",flags_arg);
  json_unparse(out,x,flags,slotfn,oidfn,miscfn);
  if (out == u8_global_output) u8_flush(out);
  return VOID;
}

static lispval jsonstring(lispval x,lispval flags_arg,lispval slotfn,
                         lispval oidfn,lispval miscfn)
{
  struct U8_OUTPUT tmpout;
  int flags = get_json_flags(flags_arg);
  if ((flags<0)||(flags>=FD_JSON_MAXFLAGS))
    return fd_type_error("fixnum/flags","jsonoutput",flags_arg);
  U8_INIT_OUTPUT(&tmpout,128);
  json_unparse(&tmpout,x,flags,slotfn,oidfn,miscfn);
  return fd_stream2string(&tmpout);
}

/* Module initialization */

FD_EXPORT void fd_init_json_c()
{
  lispval module = fd_new_module("FDWEB",(FD_MODULE_SAFE));
  lispval unsafe_module = fd_new_module("FDWEB",0);

  fd_idefn3(module,"JSONPARSE",jsonparseprim,1,
            "(JSONPARSE *string*) Parse the JSON in *string* into a LISP object",
            -1,VOID,
            -1,FD_INT(FD_JSON_DEFAULTS),
            -1,VOID);
  fd_idefn3(unsafe_module,"JSONPARSE",jsonparseprim,1,
            "(JSONPARSE *string*) Parse the JSON in *string* into a LISP object",
            -1,VOID,
            -1,FD_INT(FD_JSON_DEFAULTS),
            -1,VOID);

  fd_idefn5(module,"->JSON",jsonstring,1,
            "(->JSON *obj* ...) returns a JSON string for the lisp object *obj*",
            -1,VOID,-1,FD_INT(FD_JSON_DEFAULTS),
            -1,VOID,-1,VOID,
            -1,VOID);
  fd_idefn5(unsafe_module,"->JSON",jsonstring,1,
            "(->JSON *obj* ...) returns a JSON string for the lisp object *obj*",
            -1,VOID,-1,FD_INT(FD_JSON_DEFAULTS),
            -1,VOID,-1,VOID,
            -1,VOID);

  fd_idefn5(module,"JSONOUTPUT",jsonoutput,1,
            "Outputs a JSON representation to the standard output",
            -1,VOID,-1,FD_INT(FD_JSON_DEFAULTS),
            -1,VOID,-1,VOID,-1,VOID);
  fd_idefn5(unsafe_module,"JSONOUTPUT",jsonoutput,1,
            "Outputs a JSON representation to the standard output",
            -1,VOID,-1,FD_INT(FD_JSON_DEFAULTS),
            -1,VOID,-1,VOID,-1,VOID);

  symbolize_symbol=fd_intern("SYMBOLIZE");
  colonize_symbol=fd_intern("COLONIZE");
  rawids_symbol=fd_intern("RAWIDS");
  ticks_symbol=fd_intern("TICKS");
  ticklets_symbol=fd_intern("TICKLETS");
  verbose_symbol=fd_intern("VERBOSE");
  strict_symbol=fd_intern("STRICT");

  u8_register_source_file(_FILEINFO);
}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
