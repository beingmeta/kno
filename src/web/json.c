/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/eval.h"
#include "kno/tables.h"
#include "kno/ports.h"
#include "kno/webtools.h"
#include "kno/cprims.h"

#include <libu8/u8xfiles.h>

#include <ctype.h>

#define KNO_JSON_ANYKEY    2  /* Allow compound table keys */
#define KNO_JSON_IDKEY     4  /* Allow raw identifiers as table keys */
#define KNO_JSON_SLOTIDS   8  /* Convert table keys to symbols (and vice versa) */
#define KNO_JSON_COLONIZE 16  /* Use (assume) colon prefixes for LISPY objects */
#define KNO_JSON_TICKS    32  /* Show time as unix time */
#define KNO_JSON_TICKLETS 64  /* Show time as nanosecond-precision unix time */
#define KNO_JSON_VERBOSE 128 /* Emit conversion warnings, etc */
#define KNO_JSON_STRICT  256 /* Emit conversion warnings, etc */
#define KNO_JSON_DEFAULTS (KNO_JSON_SLOTIDS)

#define KNO_JSON_MAXFLAGS 256

static lispval json_slotid(u8_input in);

static u8_condition JSON_Error="JSON Parsing Error";

static int readc(U8_INPUT *in)
{
  int c = u8_getc(in);
  if (c=='\\') {
    u8_ungetc(in,c);
    return kno_read_escape(in);}
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
  kno_clear_errors(report);
  kno_decref(result);
  return kno_make_string(NULL,out->u8_write-out->u8_outbuf,
			 out->u8_outbuf);
}

static lispval convert_value(lispval fn,lispval val,int free,int warn)
{
  if (KNO_ABORTP(val)) return val;
  else if (KNO_ABORTP(fn)) return val;
  else if (VECTORP(fn)) {
    lispval eltfn = VEC_REF(fn,0);
    if (VECTORP(val)) {
      lispval results = EMPTY;
      lispval *elts = VEC_DATA(val);
      int i = 0, lim = VEC_LEN(val);
      while (i<lim) {
	lispval cval = convert_value(eltfn,elts[i],0,warn);
	if (KNO_ABORTP(cval)) {
	  kno_clear_errors(warn); kno_incref(elts[i]);
	  CHOICE_ADD(results,elts[i]);}
	else if (VOIDP(cval)) {
	  kno_incref(elts[i]);
	  CHOICE_ADD(results,elts[i]);}
	else {
	  CHOICE_ADD(results,cval);}
	i++;}
      kno_decref(val);
      return results;}
    else return convert_value(eltfn,val,1,warn);}
  else if (KNO_APPLICABLEP(fn)) {
    lispval converted = kno_apply(fn,1,&val);
    if (VOIDP(converted)) return val;
    else if (KNO_ABORTP(converted)) {
      kno_clear_errors(warn);
      return val;}
    else {
      kno_decref(val);
      return converted;}}
  else if ((KNO_TRUEP(fn))&&(STRINGP(val))) {
    lispval parsed = kno_parse(CSTRING(val));
    if (KNO_ABORTP(parsed)) {
      kno_clear_errors(warn);
      return val;}
    else {
      kno_decref(val); return parsed;}}
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
    if ( (flags) & (KNO_JSON_STRICT) )
      return kno_err("BadJSON","json_parse",
		     "unexpected ' in strict mode",
		     KNO_VOID);
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
  if ((strcmp(out.u8_outbuf,"true")==0)) result = KNO_TRUE;
  else if ((strcmp(out.u8_outbuf,"false")==0)) result = KNO_FALSE;
  else if  ((strcmp(out.u8_outbuf,"null")==0)) result = EMPTY;
  else result = kno_parse(out.u8_outbuf);
  if (KNO_ABORTP(result))
    result = parse_error(&out,result,flags&KNO_JSON_VERBOSE);
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
  if ( (c=='\\') && (flags&KNO_JSON_COLONIZE) ) {
    c = u8_getc(in);
    if (c==':') raw_string = 1;
    u8_putc(&out,c);
    c = u8_getc(in);}
  while (c>=0) {
    if (c == terminator) break;
    else if (c=='\\') {
      c = kno_read_escape(in);
      u8_putc(&out,c);
      c = u8_getc(in);
      continue;}
    u8_putc(&out,c);
    c = u8_getc(in);}
  if (raw_string)
    return kno_stream2string(&out);
  else if ((flags&KNO_JSON_COLONIZE)&&(out.u8_outbuf[0]==':')) {
    lispval result = kno_parse(out.u8_outbuf+1);
    if (KNO_ABORTP(result))
      result = parse_error(&out,result,flags&KNO_JSON_VERBOSE);
    if (out.u8_streaminfo&U8_STREAM_OWNS_BUF) u8_free(out.u8_outbuf);
    return result;}
  else return kno_stream2string(&out);
}

static lispval json_key(U8_INPUT *in,int flags,lispval fieldmap)
{
  int c = skip_whitespace(in);
  if ( (c=='\'') && ( (flags) & (KNO_JSON_STRICT) ) )
    return kno_err("BadJSON","json_parse",
		   "unexpected ' in strict mode",
		   KNO_VOID);
  else if ( (c=='"') || (c=='\'') )
    if (flags&KNO_JSON_SLOTIDS)
      return json_slotid(in);
    else if (VOIDP(fieldmap))
      return json_string(in,flags);
    else {
      lispval stringkey = json_string(in,flags);
      lispval mapped = kno_get(fieldmap,stringkey,VOID);
      if (VOIDP(mapped)) return stringkey;
      else {
	kno_decref(stringkey);
	return mapped;}}
  else if (((c=='{')||(c=='['))&&(!(flags&KNO_JSON_ANYKEY))) {
    kno_seterr("Invalid JSON key","json_key",NULL,VOID);
    return KNO_PARSE_ERROR;}
  else if ((!(flags&KNO_JSON_IDKEY))&&(!((u8_isdigit(c))||(c=='+')||(c=='-')))) {
    kno_seterr("Invalid JSON key","json_key",NULL,VOID);
    return KNO_PARSE_ERROR;}
  else {
    lispval result = json_atom(in,flags);
    if ((FIXNUMP(result))||(KNO_BIGINTP(result))||(KNO_FLONUMP(result)))
      return result;
    else if ((SYMBOLP(result))&&(flags&KNO_JSON_IDKEY))
      return result;
    else if (flags&KNO_JSON_ANYKEY)
      return result;
    else {
      kno_decref(result);
      kno_seterr("Invalid JSON key","json_key",NULL,VOID);
      return KNO_PARSE_ERROR;}}
}

static lispval json_vector(U8_INPUT *in,int flags,lispval fieldmap)
{
  int n_elts = 0, max_elts = 16, c, i;
  unsigned int good_pos = in->u8_read-in->u8_inbuf;
  lispval *elts;
  if (u8_getc(in)!='[') return KNO_ERROR;
  else elts = u8_alloc_n(16,lispval);
  c = skip_whitespace(in);
  while (c>=0) {
    good_pos = in->u8_read-in->u8_inbuf;
    if (c==']') {
      u8_getc(in); /* Absorb ] */
      return kno_wrap_vector(n_elts,elts);}
    else if (c==',') {
      c = u8_getc(in); c = skip_whitespace(in);}
    else {
      lispval elt;
      if (n_elts == max_elts)  {
	int new_max = max_elts*2;
	lispval *newelts = u8_realloc(elts,LISPVEC_BYTELEN(new_max));
	if (newelts) {elts = newelts; max_elts = new_max;}
	else {
	  u8_seterr(kno_MallocFailed,"json_vector",NULL);
	  break;}}
      elts[n_elts++]=elt = json_parse(in,flags,fieldmap);
      if (KNO_ABORTP(elt)) break;
      c = skip_whitespace(in);}}
  i = 0; while (i<n_elts) {kno_decref(elts[i]); i++;}
  return kno_err(JSON_Error,"json_vector",
		 u8_strdup(in->u8_inbuf+good_pos),
		 VOID);
}

static lispval json_table(U8_INPUT *in,int flags,lispval fieldmap)
{
  int n_elts = 0, max_elts = 16, c, i;
  unsigned int good_pos = in->u8_read-in->u8_inbuf;
  struct KNO_KEYVAL *kv;
  if (u8_getc(in)!='{') return KNO_ERROR;
  else kv = u8_alloc_n(16,struct KNO_KEYVAL);
  c = skip_whitespace(in);
  while (c>=0) {
    good_pos = in->u8_read-in->u8_inbuf;
    if (c=='}') {
      u8_getc(in); /* Absorb ] */
      return kno_init_slotmap(NULL,n_elts,kv);}
    else if (c==',') {
      c = u8_getc(in); c = skip_whitespace(in);}
    else {
      if (n_elts == max_elts)  {
	int new_max = max_elts*2;
	struct KNO_KEYVAL *newelts=
	  u8_realloc(kv,KNO_KEYVAL_LEN*new_max);
	if (newelts) {kv = newelts; max_elts = new_max;}
	else {
	  u8_seterr(kno_MallocFailed,"json_table",NULL);
	  break;}}
      kv[n_elts].kv_key = json_key(in,flags,fieldmap);
      if (KNO_ABORTP(kv[n_elts].kv_key)) break;
      c = skip_whitespace(in);
      if (c==':') c = u8_getc(in);
      else return KNO_EOD;
      if ((VOIDP(fieldmap))||(CONSP(kv[n_elts].kv_key)))
	kv[n_elts].kv_val = json_parse(in,flags,fieldmap);
      else {
	lispval handler = kno_get(fieldmap,kv[n_elts].kv_key,VOID);
	if (VOIDP(handler))
	  kv[n_elts].kv_val = json_parse(in,flags,fieldmap);
	else
	  kv[n_elts].kv_val =
	    convert_value(handler,json_parse(in,flags,fieldmap),
			  1,(flags&KNO_JSON_VERBOSE));}
      if (KNO_ABORTP(kv[n_elts].kv_val)) break;
      n_elts++; c = skip_whitespace(in);}}
  i = 0; while (i<n_elts) {
    kno_decref(kv[i].kv_key); kno_decref(kv[i].kv_val); i++;}
  u8_free(kv);
  return kno_err(JSON_Error,"json_table",in->u8_inbuf+good_pos,VOID);
}

static lispval slotids_symbol, symbolize_symbol, colonize_symbol, rawids_symbol;
static lispval ticks_symbol, ticklets_symbol, verbose_symbol, strict_symbol;

static int get_json_flags(lispval flags_arg)
{
  if (FALSEP(flags_arg))
    return 0;
  else if (FIXNUMP(flags_arg)) {
    long long val=FIX2INT(flags_arg);
    if ((val>=0) && (val<KNO_JSON_MAXFLAGS))
      return (unsigned int) val;
    else return KNO_JSON_DEFAULTS;}
  else if (KNO_TRUEP(flags_arg))
    return KNO_JSON_COLONIZE|KNO_JSON_SLOTIDS;
  else if (flags_arg == KNO_DEFAULT_VALUE)
    return KNO_JSON_DEFAULTS;
  else if ((PAIRP(flags_arg))||(TABLEP(flags_arg))) {
    int flags=0;
    if (!((kno_testopt(flags_arg,slotids_symbol,KNO_FALSE))|
	  (kno_testopt(flags_arg,symbolize_symbol,KNO_FALSE))))
      flags |= KNO_JSON_SLOTIDS;
    if (kno_testopt(flags_arg,colonize_symbol,VOID))
      flags |= KNO_JSON_COLONIZE;
    if (!(kno_testopt(flags_arg,rawids_symbol,VOID)))
      flags |= KNO_JSON_IDKEY;
    if (kno_testopt(flags_arg,ticks_symbol,VOID))
      flags |= KNO_JSON_TICKS;
    if (kno_testopt(flags_arg,ticklets_symbol,VOID))
      flags |= KNO_JSON_TICKLETS;
    if (kno_testopt(flags_arg,verbose_symbol,VOID))
      flags |= KNO_JSON_VERBOSE;
    if (kno_testopt(flags_arg,strict_symbol,VOID))
      flags |= KNO_JSON_STRICT;
    return flags;}
  else if (PRECHOICEP(flags_arg)) {
    lispval choice=kno_make_simple_choice(flags_arg);
    int rv=get_json_flags(choice);
    kno_decref(choice);
    return rv;}
  else if ( (CHOICEP(flags_arg)) || (SYMBOLP(flags_arg)) ) {
    int flags=0;
    if ((kno_overlapp(slotids_symbol,flags_arg)) ||
	(kno_overlapp(symbolize_symbol,flags_arg)))
      flags |= KNO_JSON_SLOTIDS;
    if (kno_overlapp(colonize_symbol,flags_arg))
      flags |= KNO_JSON_COLONIZE;
    if (kno_overlapp(rawids_symbol,flags_arg))
      flags |= KNO_JSON_IDKEY;
    if (kno_overlapp(ticks_symbol,flags_arg))
      flags |= KNO_JSON_TICKS;
    if (kno_overlapp(ticklets_symbol,flags_arg))
      flags |= KNO_JSON_TICKLETS|KNO_JSON_TICKS;
    if (kno_overlapp(verbose_symbol,flags_arg))
      flags |= KNO_JSON_VERBOSE;
    return flags;}
  else return KNO_JSON_DEFAULTS;
}

DEFC_PRIM("jsonparse",jsonparseprim,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "(JSONPARSE *string* [*flags*] [*fieldmap*]) "
	  "parses the JSON in *string* into a LISP object. "
	  "The additional arguments control the conversion "
	  "of JSON to lisp tables. *fieldmap* has a set of "
	  "string/symbol translations or associates field "
	  "names with conversion functions. *flags* handles "
	  "the conversion to and from symbols. Flags is an "
	  "opts structure and SLOTIDS causes table keys "
	  "without spaces to be converted into symbols, "
	  "while colonize applies argstring processing to "
	  "values, parsing pairs, etc if possible and "
	  "obeying :expr when it doesn't generate an error.",
	  {"in",kno_any_type,KNO_VOID},
	  {"flags_arg",kno_any_type,KNO_INT(8)},
	  {"fieldmap",kno_any_type,KNO_VOID})
static lispval jsonparseprim(lispval in,lispval flags_arg,lispval fieldmap)
{
  unsigned int flags = get_json_flags(flags_arg);
  if (KNO_PORTP(in)) {
    struct KNO_PORT *p = kno_consptr(struct KNO_PORT *,in,kno_ioport_type);
    U8_INPUT *in = p->port_input;
    return json_parse(in,flags,fieldmap);}
  else if (STRINGP(in)) {
    struct U8_INPUT inport;
    U8_INIT_STRING_INPUT(&inport,STRLEN(in),CSTRING(in));
    return json_parse(&inport,flags,fieldmap);}
  else if (PACKETP(in)) {
    struct U8_INPUT inport;
    U8_INIT_STRING_INPUT(&inport,KNO_PACKET_LENGTH(in),KNO_PACKET_DATA(in));
    return json_parse(&inport,flags,fieldmap);}
  else return kno_type_error("string or stream","jsonparse",in);
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
    lispval slotname = ((VOIDP(slotfn))?(VOID):(kno_apply(slotfn,1,&key)));
    if (VOIDP(slotname))
      if ((SYMBOLP(key))&&(flags&KNO_JSON_SLOTIDS)) {
	u8_string pname = SYM_NAME(key);
	if (!(flags&KNO_JSON_IDKEY)) u8_putc(out,'"');
	json_lower(out,pname);
	if (!(flags&KNO_JSON_IDKEY)) u8_putc(out,'"');}
      else json_unparse(out,key,flags,oidfn,slotfn,miscfn);
    else if (STRINGP(slotname)) {
      u8_putc(out,'"');
      json_escape(out,CSTRING(slotname)); kno_decref(slotname);
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
    lispval oidval = ( (VOIDP(oidfn))? (VOID) : (kno_dapply(oidfn,1,&x)) );
    if (VOIDP(oidval)) {
      KNO_OID addr = KNO_OID_ADDR(x);
      if (flags)
	u8_printf(out,"\":@%x/%x\"",KNO_OID_HI(addr),KNO_OID_LO(addr));
      else u8_printf(out,"\"@%x/%x\"",KNO_OID_HI(addr),KNO_OID_LO(addr));}
    else json_unparse(out,oidval,flags,slotfn,oidfn,miscfn);
    kno_decref(oidval);}
  else if ((SYMBOLP(x))&&(VOIDP(miscfn))) {
    u8_string pname = SYM_NAME(x);
    u8_puts(out,"\":");
    json_escape(out,pname);
    u8_puts(out,"\"");}
  else if (STRINGP(x)) {
    u8_string pstring = CSTRING(x);
    u8_puts(out,"\"");
    if (((flags)&(KNO_JSON_COLONIZE))&&(pstring[0]==':')) {
      u8_putc(out,'\\'); u8_putc(out,'\\');}
    json_escape(out,pstring);
    u8_puts(out,"\"");}
  else if ((KNO_BIGINTP(x))||(KNO_FLONUMP(x)))
    kno_unparse(out,x);
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
  else if (TYPEP(x,kno_timestamp_type)) {
    struct KNO_TIMESTAMP *tm=
      kno_consptr(struct KNO_TIMESTAMP *,x,kno_timestamp_type);
    if (flags&KNO_JSON_TICKS)
      if (tm->u8xtimeval.u8_tick<0)  u8_puts(out,"-1"); /* Invalid time */
      else if (flags&KNO_JSON_TICKLETS) {
	double dtick = ((unsigned long long)tm->u8xtimeval.u8_tick)+
	  (tm->u8xtimeval.u8_nsecs)*0.000000001;
	u8_printf(out,"%f",dtick);}
      else {
	unsigned long long llval =
	  (unsigned long long)(tm->u8xtimeval.u8_tick);
	u8_printf(out,"%llu",llval);}
    else if (flags&KNO_JSON_COLONIZE)
      u8_printf(out,"\":#T%iSXGt\"",&(tm->u8xtimeval));
    else if (tm->u8xtimeval.u8_tick<0)  u8_puts(out,"\"invalid time\""); /* Invalid time */
    else u8_printf(out,"\"%iSXGt\"",&(tm->u8xtimeval));}
  else if (TYPEP(x,kno_uuid_type)) {
    struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,x,kno_uuid_type);
    char buf[64]; u8_uuidstring((u8_uuid)(&(uuid->uuid16)),buf);
    if ((flags)&(KNO_JSON_COLONIZE))
      u8_printf(out,"\":#U%s\"",buf);
    else u8_printf(out,"\"%s\"",buf);}
  else if (TABLEP(x)) {
    lispval keys = kno_getkeys(x);
    if (EMPTYP(keys)) u8_puts(out,"{}");
    else {
      int elt_count = 0; DO_CHOICES(key,keys) {
	lispval value = kno_get(x,key,VOID);
	if (!(VOIDP(value))) {
	  if (elt_count>0) u8_putc(out,',');
	  else u8_puts(out,"{");
	  if (json_slotval(out,key,value,flags,slotfn,oidfn,miscfn)) elt_count++;
	  kno_decref(value);}}
      u8_puts(out,"}");}}
  else if (EMPTYP(x)) u8_puts(out,"[]");
  else if (KNO_TRUEP(x)) u8_puts(out,"true");
  else if (FALSEP(x)) u8_puts(out,"false");
  else {
    u8_byte buf[256]; struct U8_OUTPUT tmpout;
    lispval tval = ( (VOIDP(miscfn)) ? (VOID) : kno_dapply(miscfn,1,&x) );
    U8_INIT_STATIC_OUTPUT_BUF(tmpout,256,buf);
    tmpout.u8_streaminfo |= U8_STREAM_VERBOSE;
    if (VOIDP(tval))
      kno_unparse(&tmpout,x);
    else kno_unparse(&tmpout,tval);
    kno_decref(tval);
    if (flags) u8_puts(out,"\":"); else u8_putc(out,'"');
    json_escape(out,tmpout.u8_outbuf);
    u8_putc(out,'"');
    u8_close_output(&tmpout);}
}

DEFC_PRIM("jsonoutput",jsonoutput,
	  KNO_MAX_ARGS(5)|KNO_MIN_ARGS(1),
	  "Outputs a JSON representation to the standard "
	  "output",
	  {"x",kno_any_type,KNO_VOID},
	  {"flags_arg",kno_any_type,KNO_INT(8)},
	  {"slotfn",kno_any_type,KNO_VOID},
	  {"oidfn",kno_any_type,KNO_VOID},
	  {"miscfn",kno_any_type,KNO_VOID})
static lispval jsonoutput(lispval x,lispval flags_arg,
			  lispval slotfn,lispval oidfn,lispval miscfn)
{
  u8_output out = u8_current_output;
  int flags = get_json_flags(flags_arg);
  if ((flags<0)||(flags>=KNO_JSON_MAXFLAGS))
    return kno_type_error("fixnum/flags","jsonoutput",flags_arg);
  json_unparse(out,x,flags,slotfn,oidfn,miscfn);
  if (out == u8_global_output) u8_flush(out);
  return VOID;
}

DEFC_PRIM("->json",jsonstring,
	  KNO_MAX_ARGS(5)|KNO_MIN_ARGS(1),
	  "(->JSON *obj* ...) "
	  "returns a JSON string for the lisp object *obj*",
	  {"x",kno_any_type,KNO_VOID},
	  {"flags_arg",kno_any_type,KNO_INT(8)},
	  {"slotfn",kno_any_type,KNO_VOID},
	  {"oidfn",kno_any_type,KNO_VOID},
	  {"miscfn",kno_any_type,KNO_VOID})
static lispval jsonstring(lispval x,lispval flags_arg,lispval slotfn,
			  lispval oidfn,lispval miscfn)
{
  struct U8_OUTPUT tmpout;
  int flags = get_json_flags(flags_arg);
  if ((flags<0)||(flags>=KNO_JSON_MAXFLAGS))
    return kno_type_error("fixnum/flags","jsonoutput",flags_arg);
  U8_INIT_OUTPUT(&tmpout,128);
  json_unparse(&tmpout,x,flags,slotfn,oidfn,miscfn);
  return kno_stream2string(&tmpout);
}

static lispval json_slotid(u8_input in)
{
  int c = u8_getc(in);
  if (RARELY(c<0)) return KNO_EOF;
  int delim = ( (c == '\'') || (c == '"') ) ? (c) : (-1);
  int make_symbol = 1;
  U8_STATIC_OUTPUT(all,120);
  if (delim > 0) c = u8_getc(in); /* Skip the delimiter */
  if (c == '\\')
    /* Handle backslash by not interpreting the next char */
    c = u8_getc(in);
  else if ( (c<0x80) && (strchr("@\\:'#{[",c)) ) {
    /* For these things, call the lisp parser */
    lispval val = kno_parser(in);
    if (delim<0) return val;
    int nextc = skip_whitespace(in);
    if (nextc != delim) {
      kno_seterr("UnmatchedDelimiter","json_slotid",
		 all.u8_outbuf,KNO_VOID);
      u8_close_output(allout);
      return KNO_ERROR_VALUE;}
    else return val;}
  else {}
  while (c >= 0) {
    if (c == delim) break;
    else if ((delim<0) && (c == ':')) break;
    else if (c == '\\') c = u8_getc(in);
    if ( (make_symbol) && ( (u8_isspace(c)) || (u8_isctrl(c)) ) )
      make_symbol = 0;
    u8_putc(allout,c);
    c = u8_getc(in);}
  lispval result = (make_symbol) ? (kno_getsym(all.u8_outbuf)) :
    (kno_mkstring(all.u8_outbuf));
  u8_close_output(allout);
  return result;
}

/* Module initialization */

static lispval webtools_module;

KNO_EXPORT void kno_init_json_c()
{
  lispval module = kno_new_module("WEBTOOLS",0);
  webtools_module = module;

  link_local_cprims();

  slotids_symbol=kno_intern("slotids");
  symbolize_symbol=kno_intern("symbolize");
  colonize_symbol=kno_intern("colonize");
  rawids_symbol=kno_intern("rawids");
  ticks_symbol=kno_intern("ticks");
  ticklets_symbol=kno_intern("ticklets");
  verbose_symbol=kno_intern("verbose");
  strict_symbol=kno_intern("strict");

  u8_register_source_file(_FILEINFO);
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("->json",jsonstring,5,webtools_module);
  KNO_LINK_CPRIM("jsonoutput",jsonoutput,5,webtools_module);
  KNO_LINK_CPRIM("jsonparse",jsonparseprim,3,webtools_module);
}
