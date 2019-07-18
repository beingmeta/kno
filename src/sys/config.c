/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2019 beingmeta, inc.
   This file is part of beingmeta's Kno platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"

#include <libu8/u8signals.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <signal.h>
#include <sys/types.h>
#include <pwd.h>

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#if KNO_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

u8_condition kno_ConfigError=_("Configuration error");
u8_condition kno_ReadOnlyConfig=_("Read-only config setting");

int kno_trace_config = 0;

static int support_config_c_init_done = 0;

/* Configuration handling */

struct KNO_CONFIG_HANDLER *config_handlers = NULL;

static lispval configuration_table;
static lispval path_macro, env_macro, config_macro, now_macro;
static lispval glom_macro, source_macro;

static u8_mutex config_lookup_lock;
static u8_mutex config_register_lock;

static struct KNO_CONFIG_FINDER *config_lookupfns = NULL;

/* Low level functions */

static lispval config_intern(u8_string start)
{
  /* Config settings are normalized to uppercase and punctuation and
     whitespace, other than : and / are converted to '_'. */
  U8_OUTPUT nameout; u8_byte buf[64];
  const u8_byte *scan = start;
  U8_INIT_STATIC_OUTPUT_BUF(nameout,64,buf);
  while (*scan) {
    int c = u8_sgetc(&scan);
    if ((c == '/')||(c==':'))
      u8_putc(&nameout,c);
    else if (u8_isctrl(c)) {}
    else if ( (u8_ispunct(c)) || (u8_isspace(c)) )
      u8_putc(&nameout,'_');
    else if (u8_isupper(c))
      u8_putc(&nameout,c);
    else u8_putc(&nameout,u8_toupper(c));}
  if (nameout.u8_streaminfo&U8_STREAM_OWNS_BUF) {
    lispval symbol=
      kno_make_symbol(nameout.u8_outbuf,nameout.u8_write-nameout.u8_outbuf);
    u8_close((u8_stream)&nameout);
    return symbol;}
  else return kno_make_symbol
         (nameout.u8_outbuf,nameout.u8_write-nameout.u8_outbuf);
}

static lispval config_get(lispval symbol)
{
  lispval probe = kno_get(configuration_table,symbol,VOID);
  /* This lookups configuration information using various methods */
  if (VOIDP(probe)) {
    lispval value = VOID;
    struct KNO_CONFIG_FINDER *scan = config_lookupfns;
    while (scan) {
      value = scan->config_lookup(symbol,scan->config_lookup_data);
      if (VOIDP(value))
        scan = scan->next_lookup;
      else break;}
    kno_store(configuration_table,symbol,value);
    return value;}
  else return probe;
}

int set_config(lispval symbol,lispval val)
{
  lispval current = kno_get(configuration_table,symbol,VOID);
  if (VOIDP(current)) {
    if (PAIRP(val)) {
      lispval pairpair = kno_make_pair(val,NIL);
      int rv = kno_store(configuration_table,symbol,pairpair);
      kno_decref(pairpair);
      return rv;}
    else return kno_store(configuration_table,symbol,val);}
  else if (PAIRP(current)) {
    lispval pairpair = kno_make_pair(val,current);
    int rv = kno_store(configuration_table,symbol,pairpair);
    kno_decref(pairpair);
    return rv;}
  else if (KNO_EQUAL(current,val)) return 0;
  else {
    lispval cdr = kno_make_pair(current,NIL);
    lispval pairpair = kno_make_pair(val,cdr);
    int rv = kno_store(configuration_table,symbol,pairpair);
    kno_decref(cdr);
    kno_decref(pairpair);
    return rv;}
}

/* API functions */

KNO_EXPORT lispval kno_config_get(u8_string var)
{
  lispval symbol = config_intern(var);
  struct KNO_CONFIG_HANDLER *scan = config_handlers;
  while (scan)
    if (KNO_EQ(scan->configname,symbol)) {
      lispval val;
      val = scan->config_get_method(symbol,scan->configdata);
      return val;}
    else scan = scan->config_next;
  return config_get(symbol);
}

KNO_EXPORT int kno_set_config_sym(lispval symbol,lispval val)
{
  int retval = 0;
  struct KNO_CONFIG_HANDLER *scan = config_handlers;
  while (scan)
    if (KNO_EQ(scan->configname,symbol)) {
      scan->configflags |= KNO_CONFIG_ALREADY_MODIFIED;
      if (kno_trace_config)
        u8_log(LOG_WARN,"ConfigSet",
               "Using handler to configure %s with %q",
               SYM_NAME(symbol),val);
      retval = scan->config_set_method(symbol,val,scan->configdata);
      if (retval<0) {
        u8_string errsum = kno_errstring(NULL);
        u8_log(LOG_WARN,kno_ConfigError,"Config handler error %q=%q: %s",
               symbol,val,errsum);
        if (errsum) u8_free(errsum);}
      break;}
    else scan = scan->config_next;
  if ((!(scan))&&(kno_trace_config))
    u8_log(LOG_WARN,"ConfigSet","Configuring %s with %q",
           SYM_NAME(symbol),val);
  if (retval>=0) set_config(symbol,val);
  return retval;
}

KNO_EXPORT int kno_set_config(u8_string var,lispval val)
{
  lispval symbol = config_intern(var);
  return kno_set_config_sym(symbol,val);
}

KNO_EXPORT int kno_default_config_sym(lispval symbol,lispval val)
{
  int retval = 1;
  struct KNO_CONFIG_HANDLER *scan = config_handlers;
  while (scan)
    if (KNO_EQ(scan->configname,symbol)) {
      if ( (scan->configflags) & (KNO_CONFIG_ALREADY_MODIFIED) )
        return 0;
      scan->configflags |= KNO_CONFIG_ALREADY_MODIFIED;
      retval = scan->config_set_method(symbol,val,scan->configdata);
      if (kno_trace_config)
        u8_log(LOG_WARN,"ConfigSet",
               "Using handler to configure default %s with %q",
               SYM_NAME(symbol),val);
      break;}
    else scan = scan->config_next;
  if (kno_test(configuration_table,symbol,VOID)) return 0;
  else {
    if ((!(scan))&&(kno_trace_config))
      u8_log(LOG_WARN,"ConfigSet","Configuring %s with %q",
             SYM_NAME(symbol),val);
    retval = set_config(symbol,val);}
  if (retval<0) {
    u8_string errsum = kno_errstring(NULL);
    u8_log(LOG_WARN,kno_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
    u8_free(errsum);}
  return retval;
}

KNO_EXPORT int kno_default_config(u8_string var,lispval val)
{
  lispval symbol = config_intern(var);
  return kno_default_config_sym(symbol,val);
}

KNO_EXPORT int kno_set_config_consed(u8_string var,lispval val)
{
  int retval = kno_set_config(var,val);
  if (retval<0) return retval;
  kno_decref(val);
  return retval;
}

/* Registering new configuration handlers */

KNO_EXPORT int kno_register_config_x
(u8_string var,u8_string doc,
 lispval (*getfn)(lispval,void *),
 int (*setfn)(lispval,lispval,void *),
 void *data,
 int flags,
 int (*reuse)(struct KNO_CONFIG_HANDLER *scan))
{
  lispval symbol = config_intern(var);
  lispval current = config_get(symbol);
  int retval = 0;
  kno_config_getfn old_getfn = NULL;
  kno_config_setfn old_setfn = NULL;
  void *old_configdata = NULL;
  u8_string old_configdoc = NULL;
  struct KNO_CONFIG_HANDLER *scan;
  u8_lock_mutex(&config_register_lock);
  scan = config_handlers;
  while (scan)
    if (KNO_EQ(scan->configname,symbol)) {
      if (reuse) reuse(scan);
      if (doc) {
        /* We don't override a real docstring with a NULL docstring.
           Possibly not the right thing. */
        old_configdoc = scan->configdoc;
        scan->configdoc = u8_strdup(doc);}
      old_getfn = scan->config_get_method;
      old_setfn = scan->config_set_method;
      old_configdata = scan->configdata;
      scan->config_get_method = getfn;
      scan->config_set_method = setfn;
      scan->configdata = data;
      if (flags>0) scan->configflags = flags;
      break;}
    else scan = scan->config_next;
  if (scan == NULL) {
    scan = u8_alloc(struct KNO_CONFIG_HANDLER);
    scan->configname = symbol;
    if (doc) scan->configdoc = u8_strdup(doc);
    else scan->configdoc = NULL;
    if (flags > 0)
      scan->configflags = flags;
    else scan->configflags = 0;
    scan->config_get_method = getfn;
    scan->config_set_method = setfn;
    scan->configdata = data;
    scan->config_next = config_handlers;
    config_handlers = scan;}
  struct KNO_CONFIG_HANDLER *config_entry = scan;
  u8_unlock_mutex(&config_register_lock);
  if (KNO_ABORTP(current)) {
    kno_clear_errors(1);
    retval = -1;}
  else if (VOIDP(current)) {}
  else if ( (PAIRP(current)) &&
            (!(config_entry->configflags&KNO_CONFIG_SINGLE_VALUE)) ) {
    /* There have been multiple configuration specifications,
       so run them all backwards. */
    int n = 0;
    lispval scan = current; while (PAIRP(scan)) {scan = KNO_CDR(scan); n++;}
    lispval *vals = u8_alloc_n(n,lispval), *write = vals;
    {KNO_DOLIST(cv,current) *write++=cv;}
    while (n>0) {
      n = n-1;
      if (retval<0) {
        u8_free(vals);
        kno_decref(current);
        if (config_entry->configdoc) u8_free(config_entry->configdoc);
        config_entry->configdoc = old_configdoc;
        config_entry->configdata = old_configdata;
        config_entry->config_get_method = old_getfn;
        config_entry->config_set_method = old_setfn;
        return retval;}
      else retval = setfn(symbol,vals[n],data);}
    u8_free(vals);}
  else if (KNO_PAIRP(current)) {
    lispval last = KNO_CAR(current), scan = KNO_CDR(current);
    while (PAIRP(scan)) {
      last = KNO_CAR(scan);
      scan = KNO_CDR(scan); }
    retval = setfn(symbol,last,data);}
  else retval = setfn(symbol,current,data);
  if (old_configdoc) u8_free(old_configdoc);
  scan->configflags |= KNO_CONFIG_ALREADY_MODIFIED;
  kno_decref(current);
  return retval;
}

KNO_EXPORT int kno_register_config
(u8_string var,u8_string doc,
 lispval (*getfn)(lispval,void *),
 int (*setfn)(lispval,lispval,void *),
 void *data)
{
  return kno_register_config_x(var,doc,getfn,setfn,data,0,NULL);
}

KNO_EXPORT lispval kno_all_configs(int with_docs)
{
  lispval results = EMPTY;
  struct KNO_CONFIG_HANDLER *scan;
  u8_lock_mutex(&config_register_lock); {
    scan = config_handlers;
    while (scan) {
      lispval var = scan->configname;
      if (with_docs) {
        lispval doc = ((scan->configdoc)?
                       (knostring(scan->configdoc)):
                       (NIL));
        lispval pair = kno_conspair(var,doc); kno_incref(var);
        CHOICE_ADD(results,pair);}
      else {kno_incref(var); CHOICE_ADD(results,var);}
      scan = scan->config_next;}}
  u8_unlock_mutex(&config_register_lock);
  return results;
}

/* Config lookup methods */

KNO_EXPORT
void kno_register_config_lookup(lispval (*fn)(lispval,void *),void *ldata)
{
  struct KNO_CONFIG_FINDER *entry=
    u8_alloc(struct KNO_CONFIG_FINDER);
  u8_lock_mutex(&config_lookup_lock);
  entry->config_lookup = fn;
  entry->config_lookup_data = ldata;
  entry->next_lookup = config_lookupfns;
  config_lookupfns = entry;
  u8_unlock_mutex(&config_lookup_lock);
}

/* Environment config lookup */

static lispval getenv_config_lookup(lispval symbol,void *ignored)
{
  U8_OUTPUT out;
  char *getenv_result;
  u8_string u8result;
  lispval result;
  U8_INIT_OUTPUT(&out,32);
  u8_printf(&out,"KNO_%s",SYM_NAME(symbol));
  getenv_result = getenv(out.u8_outbuf);
  if (getenv_result == NULL) {
    u8_free(out.u8_outbuf); return VOID;}
  u8result = u8_fromlibc(getenv_result);
  result = kno_parse_arg(u8result);
  u8_free(out.u8_outbuf); u8_free(u8result);
  return result;
}

/* File-based configuration */

#if KNO_FILECONFIG_ENABLED
static u8_string configdata_path = NULL;

static lispval file_config_lookup(lispval symbol,void *pathdata)
{
  u8_string path=
    ((pathdata == NULL) ?
     ((configdata_path) ? (configdata_path) : ((u8_string)KNO_CONFIG_FILE_PATH)) :
     ((u8_string)pathdata));
  u8_string filename = u8_find_file(SYM_NAME(symbol),path,u8_file_readablep);
  if (filename) {
    int n_bytes; lispval result;
    unsigned char *content = u8_filedata(filename,&n_bytes);
    if (content[0]==0) {
      struct KNO_INBUF in = { 0 };
      in.buffer = in.bufread = content+1; in.buflim = in.buffer+n_bytes;
      in.buf_fillfn = NULL;
      result = kno_read_dtype(&in);}
    else {
      /* Zap any trailing newlines */
      if ((n_bytes>1) && (content[n_bytes-1]=='\n'))
        content[n_bytes-1]='\0';
      if ((n_bytes>2) && (content[n_bytes-2]=='\r'))
        content[n_bytes-2]='\0';
      result = kno_parse_arg(content);}
    u8_free(filename);
    u8_free(content);
    return result;}
  else return VOID;
}

#endif

/* Lots of different ways to specify configuration */

/* This takes a string of the form var = value */
KNO_EXPORT int kno_config_assignment(u8_string assignment)
{
  u8_byte *equals;
  if ((equals = (strchr(assignment,'=')))) {
    int sep = -1;
    u8_byte *semi = strchr(assignment,';');
    u8_byte *comma = strchr(assignment,',');
    if ( (comma) && (semi) ) {
      if (comma>semi) sep=';';
      else sep = ',';}
    else if (semi) sep=';';
    else if (comma) sep=',';
    else sep = -1;
    if (sep>0) {
      ssize_t len = strlen(assignment);
      int count = 0, rv = 0;
      u8_byte copied[len+1], *start = copied, *scan;
      strncpy(copied,assignment,len+1);
      while ((scan=strchr(start,sep))) {
        if ( (scan>start) && (scan[-1] == '\\') ) {
          scan = strchr(scan+1,sep);
          continue;}
        *scan = '\0';
        int rv = kno_config_assignment(start);
        if (rv<0) {
          u8_seterr("BadConfig","kno_config_assigment",u8_strdup(start));
          return rv;}
        start=scan+1;
        count++;}
      if (*start) {
        rv = kno_config_assignment(start);
        if (rv>=0) count++;}
      if (rv<0)
        return -(count+1);
      else return count;}
    else {
      u8_byte _namebuf[64], *namebuf;
      int namelen = equals-assignment, retval;
      lispval value = kno_parse_arg(equals+1);
      if (KNO_ABORTP(value))
        return kno_interr(value);
      else if (KNO_PAIRP(value)) {
        lispval interpreted = kno_interpret_value(value);
        kno_decref(value);
        value = interpreted;}
      if (namelen+1>64)
        namebuf = u8_malloc(namelen+1);
      else namebuf=_namebuf;
      strncpy(namebuf,assignment,namelen); namebuf[namelen]='\0';
      retval = kno_set_config_consed(namebuf,value);
      if (namebuf!=_namebuf) u8_free(namebuf);
      return retval;}}
  else return -1;
}

/* This takes a string of the form var = value */
KNO_EXPORT int kno_default_config_assignment(u8_string assignment)
{
  u8_byte *equals;
  if ((equals = (strchr(assignment,'=')))) {
    u8_byte _namebuf[64], *namebuf;
    int namelen = equals-assignment, retval = 0;
    lispval value = kno_parse_arg(equals+1);
    if (KNO_ABORTP(value))
      return kno_interr(value);
    else if (KNO_PAIRP(value)) {
      lispval interpreted = kno_interpret_value(value);
      kno_decref(value);
      value = interpreted;}
    if (namelen+1>64)
      namebuf = u8_malloc(namelen+1);
    else namebuf=_namebuf;
    strncpy(namebuf,assignment,namelen); namebuf[namelen]='\0';
    if (!(kno_test(configuration_table,config_intern(namebuf),VOID)))
      retval = kno_set_config_consed(namebuf,value);
    if (namebuf!=_namebuf) u8_free(namebuf);
    return retval;}
  else return -1;
}

#define EXPR_STARTS_WITH(expr,sym)  \
  ( (KNO_PAIRP(expr)) &&             \
    (KNO_PAIRP(KNO_CDR(expr))) &&    \
    ( (KNO_CAR(expr)) == (sym) ) )

KNO_EXPORT lispval kno_interpret_value(lispval expr)
{
  if ( (KNO_PAIRP(expr)) &&
       (KNO_PAIRP(KNO_CDR(expr))) &&
       (KNO_CDDR(expr) == KNO_NIL) ) {
    lispval head = KNO_CAR(expr);
    lispval arg = KNO_CADR(expr);
    if (head == path_macro) {
      if ( (KNO_STRINGP(arg)) ) {
        u8_string fullpath = (u8_abspath(KNO_CSTRING(arg),NULL));
        return kno_init_string(NULL,-1,fullpath);}
      else return kno_incref(arg);}
    else if (head == source_macro) {
      u8_string sourcepath = kno_sourcebase();
      if (!(sourcepath))
        return kno_incref(arg);
      else if (KNO_FALSEP(arg))
        return kno_make_string(NULL,-1,sourcepath);
      else if (KNO_STRINGP(arg)) {
        u8_string str = kno_get_component(KNO_CSTRING(arg));
        return kno_wrapstring(str);}
      else return kno_incref(expr);}
    else if (head == config_macro) {
      if (KNO_SYMBOLP(arg)) {
        lispval v = kno_config_get(KNO_SYMBOL_NAME(arg));
        if (KNO_VOIDP(v))
          return expr;
        else return v;}
      else return kno_incref(expr);}
    else if (head == env_macro) {
      u8_string varname; int free_varname = 0;
      if (KNO_SYMBOLP(arg)) {
        varname = u8_upcase(KNO_SYMBOL_NAME(arg));
        free_varname = 1;}
      else if (KNO_STRINGP(arg))
        varname = KNO_CSTRING(arg);
      else return kno_incref(expr);
      u8_string strval = u8_getenv(varname);
      if (free_varname) u8_free(varname);
      if (strval) {
        lispval val = knostring(strval);
        u8_free(strval);
        return val;}
      else return KNO_FALSE;}
    else if (head == now_macro) {
      if (KNO_SYMBOLP(arg)) {
        lispval now = kno_make_timestamp(NULL);
        lispval v = kno_get(now,arg,KNO_VOID);
        kno_decref(now);
        if (KNO_VOIDP(v))
          return expr;
        else return v;}
      else return kno_make_timestamp(NULL);}
    else return kno_incref(expr);}
  else return kno_incref(expr);
}

/* This reads a config file.  It consists of a series of entries, each of which is
   either a list (var value) or an assignment var = value.
   Both # and ; are comment characters at the beginning of lines. */
static int read_config(U8_INPUT *in,int dflt)
{
  int c, count = 0;
  u8_string buf;
  while ((c = u8_getc(in))>=0)
    if (c == '#') {
      buf = u8_gets(in); u8_free(buf);}
    else if (c == ';') {
      buf = u8_gets(in); u8_free(buf);}
    else if (c == '(') {
      lispval entry;
      u8_ungetc(in,c);
      entry = kno_parser(in);
      if (KNO_ABORTP(entry))
        return kno_interr(entry);
      else if ((PAIRP(entry)) &&
               (SYMBOLP(KNO_CAR(entry))) &&
               (PAIRP(KNO_CDR(entry)))) {
        lispval val = kno_interpret_value(KNO_CADR(entry));
        int rv = (dflt) ?
          (kno_default_config(SYM_NAME(KNO_CAR(entry)),val)<0) :
          (kno_set_config(SYM_NAME(KNO_CAR(entry)),val)<0);
        if (rv < 0) {
          kno_decref(val);
          return KNO_ERR(-1,kno_ConfigError,"kno_read_config",NULL,entry);}
        if (rv) count++;
        kno_decref(entry);
        kno_decref(val);}
      else return KNO_ERR(-1,kno_ConfigError,"kno_read_config",NULL,entry);}
    else if ((u8_isspace(c)) || (u8_isctrl(c))) {}
    else {
      u8_ungetc(in,c);
      buf = u8_gets(in);
      int rv = (dflt) ?
        (kno_default_config_assignment(buf)<0) :
        (kno_config_assignment(buf)<0);
      if (rv<0)
        return kno_reterr(kno_ConfigError,"kno_read_config",buf,VOID);
      else if (rv)
        count++;
      else NO_ELSE;
      u8_free(buf);}
  return count;
}

/* This reads a config file.  It consists of a series of entries, each of which is
   either a list (var value) or an assignment var = value.
   Both # and ; are comment characters */
KNO_EXPORT int kno_read_config(U8_INPUT *in)
{
  return read_config(in,0);
}

/* This reads a config file.  It consists of a series of entries, each of which is
   either a list (var value) or an assignment var = value.
   Both # and ; are comment characters */
KNO_EXPORT int kno_read_default_config(U8_INPUT *in)
{
  return read_config(in,1);
}

/* Utility configuration functions */

/* This set method just returns an error */
KNO_EXPORT int kno_readonly_config_set(lispval ignored,lispval v,void *vptr)
{
  if (SYMBOLP(v))
    return kno_reterr(kno_ReadOnlyConfig,"kno_set_config",
                      SYM_NAME(v),VOID);
  else if (STRINGP(v))
    return kno_reterr(kno_ReadOnlyConfig,"kno_set_config",
                      CSTRING(v),VOID);
  else return kno_reterr(kno_ReadOnlyConfig,"kno_set_config",NULL,VOID);
}

/* For configuration variables which are just integer constants */
KNO_EXPORT lispval kno_constconfig_get(lispval ignored,void *llval)
{
  kno_ptrval int_val = (kno_ptrval) llval;
  return KNO_INT(int_val);
}

/* For configuration variables which get/set dtype value. */
KNO_EXPORT lispval kno_lconfig_get(lispval ignored,void *lispp)
{
  lispval *val = (lispval *)lispp;
  return kno_incref(*val);
}
KNO_EXPORT int kno_lconfig_set(lispval ignored,lispval v,void *lispp)
{
  lispval *val = (lispval *)lispp, cur = *val;
  kno_decref(cur); kno_incref(v);
  *val = v;
  return 1;
}
KNO_EXPORT int kno_symconfig_set(lispval ignored,lispval v,void *lispp)
{
  lispval *val = (lispval *)lispp;
  lispval sym = VOID;
  if (KNO_SYMBOLP(v))
    sym = v;
  else if (KNO_STRINGP(v)) {
    u8_string lower = u8_downcase(CSTRING(v));
    sym = kno_intern(lower);
    u8_free(lower);}
  else {}
  if (KNO_VOIDP(sym)) {
    kno_type_error("string or symbol","kno_symconfig_set",v);
    return -1;}
  else {
    *val = sym;
    return 1;}
}
KNO_EXPORT int kno_lconfig_add(lispval ignored,lispval v,void *lispp)
{
  lispval *val = (lispval *)lispp;
  CHOICE_ADD(*val,v);
  return 1;
}
KNO_EXPORT int kno_lconfig_push(lispval ignored,lispval v,void *lispp)
{
  lispval *val = (lispval *)lispp;
  *val = kno_conspair(kno_incref(v),*val);
  return 1;
}

/* For configuration variables which get/set strings. */
KNO_EXPORT lispval kno_sconfig_get(lispval ignored,void *vptr)
{
  u8_string *ptr = vptr;
  if (*ptr) return kno_mkstring(*ptr);
  else return EMPTY;
}
KNO_EXPORT int kno_sconfig_set(lispval ignored,lispval v,void *vptr)
{
  u8_string *ptr = vptr;
  if (STRINGP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = u8_strdup(CSTRING(v));
    return 1;}
  else return kno_reterr(kno_TypeError,"kno_sconfig_set",u8_strdup(_("string")),v);
}
KNO_EXPORT int kno_realpath_config_set(lispval confvar,lispval v,void *vptr)
{
  u8_string *ptr = vptr;
  if (STRINGP(v)) {
    u8_string s = CSTRING(v);
    /* We could allow alternates here, e.g. path1;path2;path3 */
    if (!(u8_file_existsp(s))) {
      u8_log(LOG_ERR,"BadConfigPath",
             "The path %s, configured for %q, does not exist",
             s,confvar);
      s = NULL;}
    else {}
    if (s) {
      if (*ptr) u8_free(*ptr);
      *ptr = u8_strdup(s);
      return 1;}
    else return 0;}
  else return kno_reterr(kno_TypeError,"kno_sconfig_set",u8_strdup(_("string")),v);
}
KNO_EXPORT int kno_realdir_config_set(lispval confvar,lispval v,void *vptr)
{
  u8_string *ptr = vptr;
  if (STRINGP(v)) {
    u8_string s = CSTRING(v);
    /* We could allow alternates here, e.g. path1;path2;path3 */
    if (!(u8_directoryp(s))) {
      u8_log(LOG_ERR,"BadConfigPath",
             "The directory %s, configured for %q, does not exist",
             s,confvar);
      s = NULL;}
    else {}
    if (s) {
      if (*ptr) u8_free(*ptr);
      *ptr = u8_strdup(s);
      return 1;}
    else return 0;}
  else return kno_reterr(kno_TypeError,"kno_sconfig_set",u8_strdup(_("string")),v);
}


/* For configuration variables which get/set ints. */
KNO_EXPORT lispval kno_intconfig_get(lispval ignored,void *vptr)
{
  int *ptr = vptr;
  return KNO_INT(*ptr);
}
KNO_EXPORT int kno_intconfig_set(lispval ignored,lispval v,void *vptr)
{
  int *ptr = vptr;
  if (KNO_INTP(v)) {
    *ptr = FIX2INT(v);
    return 1;}
  return kno_reterr(kno_TypeError,"kno_intconfig_set",
                    u8_strdup(_("small fixnum")),v);
}

/* For configuration variables which get/set ints. */
KNO_EXPORT lispval kno_longconfig_get(lispval ignored,void *vptr)
{
  long long *ptr = vptr;
  return KNO_INT(*ptr);
}
KNO_EXPORT int kno_longconfig_set(lispval ignored,lispval v,void *vptr)
{
  long long *ptr = vptr;
  if (FIXNUMP(v)) {
    *ptr = FIX2INT(v);
    return 1;}
  else return kno_reterr(kno_TypeError,"kno_longconfig_set",
                         u8_strdup(_("fixnum")),v);
}

/* For configuration variables which get/set ints. */
KNO_EXPORT lispval kno_sizeconfig_get(lispval ignored,void *vptr)
{
  ssize_t *ptr = vptr;
  ssize_t sz = *ptr;
  return KNO_INT(sz);
}
KNO_EXPORT int kno_sizeconfig_set(lispval ignored,lispval v,void *vptr)
{
  ssize_t *ptr = vptr;
  if (FIXNUMP(v)) {
    if ((*ptr) == (FIX2INT(v))) return 0;
    *ptr = FIX2INT(v);
    return 1;}
  else if (KNO_BIGINTP(v)) {
    struct KNO_BIGINT *bi = (kno_bigint)v;
    if (kno_bigint_fits_in_word_p(bi,8,1)) {
      long long ullv = kno_bigint_to_long_long(bi);
      if ((*ptr) == ((ssize_t)ullv)) return 0;
      *ptr = (ssize_t)ullv;
      return 1;}
    else return kno_reterr
           (kno_RangeError,"kno_sizeconfig_set",
            u8_strdup(_("size_t sized value")),v);}
  else return kno_reterr
         (kno_TypeError,"kno_sizeconfig_set",
          u8_strdup(_("size_t sized value")),v);
}

/* Double config methods */
KNO_EXPORT lispval kno_dblconfig_get(lispval ignored,void *vptr)
{
  double *ptr = vptr;
  if (*ptr) return kno_init_double(NULL,*ptr);
  else return KNO_FALSE;
}
KNO_EXPORT int kno_dblconfig_set(lispval var,lispval v,void *vptr)
{
  double *ptr = vptr;
  if (FALSEP(v)) {
    *ptr = 0.0; return 1;}
  else if (KNO_FLONUMP(v)) {
    *ptr = KNO_FLONUM(v);}
  else if (FIXNUMP(v)) {
    long long intval = FIX2INT(v);
    double dblval = (double)intval;
    *ptr = dblval;}
  else return kno_reterr(kno_TypeError,"kno_dblconfig_set",
                         SYM_NAME(var),v);
  return 1;
}

/* Boolean configuration handlers */

static int false_stringp(u8_string string);
static int true_stringp(u8_string string);

KNO_EXPORT lispval kno_boolconfig_get(lispval ignored,void *vptr)
{
  int *ptr = vptr;
  if (*ptr) return KNO_TRUE; else return KNO_FALSE;
}
KNO_EXPORT int kno_boolconfig_set(lispval var,lispval v,void *vptr)
{
  int *ptr = vptr;
  if (FALSEP(v)) {
    *ptr = 0; return 1;}
  else if (FIXNUMP(v)) {
    /* Strictly speaking, this isn't exactly right, but it's not uncommon
       to have int-valued config variables where 1 is a default setting
       but others are possible. */
    if (FIX2INT(v)<=0)
      *ptr = 0;
    else if (FIX2INT(v)<INT_MAX)
      *ptr = FIX2INT(v);
    else *ptr = INT_MAX;
    return 1;}
  else if ((STRINGP(v)) && (false_stringp(CSTRING(v)))) {
    *ptr = 0; return 1;}
  else if ((STRINGP(v)) && (true_stringp(CSTRING(v)))) {
    *ptr = 1; return 1;}
  else if (STRINGP(v))
    return KNO_ERR(-1,kno_TypeError,"kno_boolconfig_set",KNO_XSYMBOL_NAME(var),v);
  else {*ptr = 1; return 1;}
}

/* Someday, these should be configurable. */
static u8_string false_strings[]={
  "no","false","off","n","f" "#f","#false","nope",
  "0","disable","non","nei","nein","not","never",NULL};

static int false_stringp(u8_string string)
{
  u8_string *scan = false_strings;
  while (*scan)
    if (strcasecmp(string,*scan)==0) return 1;
    else scan++;
  return 0;
}

static u8_string true_strings[]={
  "yes","true","on","y","t","#t","#true","1","enable","ok",
  "oui","yah","yeah","yep","sure","hai",NULL};

static int true_stringp(u8_string string)
{
  u8_string *scan = true_strings;
  while (*scan)
    if (strcasecmp(string,*scan)==0) return 1;
    else scan++;
  return 0;
}

KNO_EXPORT int kno_boolstring(u8_string string,int dflt)
{
  u8_string *scan = true_strings;
  while (*scan)
    if (strcasecmp(string,*scan)==0) return 1;
    else scan++;
  scan = false_strings;
  while (*scan)
    if (strcasecmp(string,*scan)==0) return 0;
    else scan++;
  return dflt;
}

/* Version info */

static lispval knoversion_config_get(lispval var,void *data)
{
  return kno_mkstring(KNO_VERSION);
}
static lispval knorevision_config_get(lispval var,void *data)
{
  return kno_mkstring(KNO_REVISION);
}
static lispval knobranch_config_get(lispval var,void *data)
{
#ifdef KNO_BRANCH
  return kno_mkstring(KNO_BRANCH);
#else
  return kno_mkstring("unknown_branch");
#endif
}
static lispval knomajor_config_get(lispval var,void *data)
{
  return KNO_INT(KNO_MAJOR_VERSION);
}
static lispval u8version_config_get(lispval var,void *data)
{
  return kno_mkstring(u8_getversion());
}
static lispval u8revision_config_get(lispval var,void *data)
{
  return kno_mkstring(u8_getrevision());
}
static lispval u8major_config_get(lispval var,void *data)
{
  return KNO_INT(u8_getmajorversion());
}

/* LOGLEVEL */

static int loglevelconfig_set(lispval var,lispval val,void *data)
{
  if (KNO_INTP(val)) {
    int *valp = (int *)data;
    *valp = FIX2INT(val);
    return 1;}
  else if ((STRINGP(val)) || (SYMBOLP(val))) {
    u8_string *scan = u8_loglevels; int loglevel = -1;
    u8_string level_name;
    if (STRINGP(val)) level_name = CSTRING(val);
    else level_name = SYM_NAME(val);
    while (*scan)
      if (strcasecmp(*scan,level_name)==0) {
        loglevel = scan-u8_loglevels; break;}
      else scan++;
    if (loglevel>=0) {
      int *valp = (int *)data; *valp = loglevel;
      return 1;}
    else return KNO_ERR(-1,kno_TypeError,"loglevelconfig_set",KNO_XSYMBOL_NAME(var),val);}
  else {
    kno_seterr(kno_TypeError,"loglevelconfig_set",KNO_XSYMBOL_NAME(var),val);
    return -1;}
}

KNO_EXPORT int kno_loglevelconfig_set(lispval var,lispval val,void *data)
{
  return loglevelconfig_set(var,val,data);
}

static lispval cwd_config_get(lispval var,void *data)
{
  u8_string wd = u8_getcwd();
  if (wd) return kno_wrapstring(wd);
  else return KNO_ERROR;
}

static int cwd_config_set(lispval var,lispval val,void *data)
{
  if (KNO_STRINGP(val)) {
    if (u8_setcwd(CSTRING(val))<0)
      return KNO_ERROR;
    else return 1;}
  else return kno_type_error("string","cwd_config_set",val);
}


/* Talbe configs */

static void tblconfig_error(lispval var,lispval val)
{
  u8_string details =
    (KNO_SYMBOLP(var)) ? (KNO_SYMBOL_NAME(var)) :
    (KNO_STRINGP(var)) ? (CSTRING(var)) :
    (U8STR("oddconfig"));
  kno_seterr("ConfigFailed","kno_tblconfig_set",
             u8_strdup(details),val);

}

KNO_EXPORT int kno_tblconfig_set(lispval var,lispval config_val,void *data)
{
  lispval *lptr = (lispval *) data;
  lispval table = *lptr;
  int retval=0, err=0;
  KNO_DO_CHOICES(entry,config_val) {
    if (KNO_PAIRP(entry)) {
      lispval key = KNO_CAR(entry), value;
      if (KNO_PAIRP(KNO_CDR(entry)))
        value=KNO_CADR(entry);
      else value=KNO_CDR(entry);
      int rv=kno_store(table,key,value);
      if (rv<0) {
        tblconfig_error(var,value);
        KNO_STOP_DO_CHOICES;
        return -1;}
      else retval++;}
    else if (KNO_TABLEP(entry)) {
      lispval keys=kno_getkeys(entry);
      KNO_DO_CHOICES(key,keys) {
        lispval value = kno_get(entry,key,KNO_VOID);
        if (KNO_ABORTP(value)) {
          KNO_STOP_DO_CHOICES;
          tblconfig_error(var,KNO_VOID);
          err=1;
          break;}
        else if (KNO_AGNOSTICP(value)) {}
        else {
          int rv=kno_store(table,key,value);
          if (rv<0) {
            KNO_STOP_DO_CHOICES;
            tblconfig_error(var,value);
            break;}}
        kno_decref(value);}
      kno_decref(keys);
      if (err) {
        KNO_STOP_DO_CHOICES;
        break;}}
    else {
      KNO_STOP_DO_CHOICES;
      tblconfig_error(var,entry);
      err=1;
      break;}}
  if (err)
    return -1;
  else return retval;
}

KNO_EXPORT lispval kno_tblconfig_get(lispval var,void *data)
{
  lispval *lptr = (lispval *) data;
  lispval table = *lptr;
  return kno_incref(table);
}

/* Miscellaneous configs */

static lispval elapsed_config_get(lispval var,void *data)
{
  double e = u8_elapsed_time();
  return kno_make_flonum(e);
}

static lispval hostname_config_get(lispval var,void *data)
{
  u8_string hostname = u8_gethostname();
  return kno_wrapstring(hostname);
}

/* Initialization */

void kno_init_config_c()
{
  if (support_config_c_init_done)
    return;
  else support_config_c_init_done=1;

  u8_register_source_file(_FILEINFO);

  configuration_table = kno_make_hashtable(NULL,16);

  u8_init_mutex(&config_lookup_lock);
  u8_init_mutex(&config_register_lock);

  path_macro = kno_intern("#path");
  glom_macro = kno_intern("#glom");
  config_macro = kno_intern("#config");
  now_macro = kno_intern("#now");
  env_macro = kno_intern("#env");
  source_macro = kno_intern("#source");

  kno_register_config
    ("KNOVERSION",_("Get the Kno version string"),
     knoversion_config_get,kno_readonly_config_set,NULL);
  kno_register_config
    ("KNOREVISION",_("Get the Kno revision identifier"),
     knorevision_config_get,kno_readonly_config_set,NULL);
  kno_register_config
    ("KNOBRANCH",_("Get the Kno branch name"),
     knobranch_config_get,kno_readonly_config_set,NULL);
  kno_register_config
    ("KNOMAJOR",_("Get the Kno major version number"),
     knomajor_config_get,kno_readonly_config_set,NULL);
  kno_register_config
    ("U8VERSION",_("Get the libu8 version string"),
     u8version_config_get,kno_readonly_config_set,NULL);
  kno_register_config
    ("U8REVISION",_("Get the libu8 revision identifier"),
     u8revision_config_get,kno_readonly_config_set,NULL);
  kno_register_config
    ("U8MAJOR",_("Get the libu8 major version number"),
     u8major_config_get,kno_readonly_config_set,NULL);

#if KNO_FILECONFIG_ENABLED
  kno_register_config
    ("CONFIGDATA",_("Directory for looking up config entries"),
     kno_sconfig_get,kno_sconfig_set,&configdata_path);
  kno_register_config_lookup(file_config_lookup,NULL);
#endif

  kno_register_config_lookup(getenv_config_lookup,NULL);

  kno_register_config
    ("CWD",_("Get/set the current working directory"),
     cwd_config_get,cwd_config_set,NULL);

  kno_register_config
    ("DIRECTORY",_("Get/set the current working directory"),
     cwd_config_get,cwd_config_set,NULL);

  kno_register_config
    ("ELAPSED",_("Get the time elapsed since this process started"),
     elapsed_config_get,NULL,NULL);
  kno_register_config
    ("HOSTNAME",_("Get hostname of the computer running KNO"),
     hostname_config_get,NULL,NULL);

  kno_register_config("FIXMAX","The maximum fixnum value",
                      kno_lconfig_get,kno_readonly_config_set,
                      &kno_max_fixnum);
  kno_register_config("MAXFIX","The maximum fixnum value",
                      kno_lconfig_get,kno_readonly_config_set,
                      &kno_max_fixnum);
  kno_register_config("FIXMIN","The minimum fixnum value",
                      kno_lconfig_get,kno_readonly_config_set,
                      &kno_min_fixnum);
  kno_register_config("MINFIX","The minimum fixnum value",
                      kno_lconfig_get,kno_readonly_config_set,
                      &kno_min_fixnum);

  kno_register_config("UINT_MAX",
                      "Maximum value for an underlying unsigned INT",
                      kno_constconfig_get,kno_readonly_config_set,
                      (void *) UINT_MAX);
  kno_register_config("INT_MAX",
                      "Maximum value for an underlying unsigned INT",
                      kno_constconfig_get,kno_readonly_config_set,
                      (void *) INT_MAX);
  kno_register_config("INT_MIN",
                      "Maximum value for an underlying unsigned INT",
                      kno_constconfig_get,kno_readonly_config_set,
                      (void *) INT_MIN);

  kno_register_config("DTYPES:FIXCASE","Normalize symbol case",
                      kno_boolconfig_get,kno_boolconfig_set,
                      &kno_dtype_fixcase);

  kno_register_config
    ("TRACECONFIG",_("whether to trace configuration"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_trace_config);

}

