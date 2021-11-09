/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2020 beingmeta, inc.
   Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/numbers.h"
#include "kno/apply.h"
#include "kno/getsource.h"

#include <libu8/u8signals.h>
#include <libu8/u8pathfns.h>
#include <libu8/u8filefns.h>
#include <libu8/u8printf.h>
#include <libu8/u8logging.h>

#include <signal.h>
#include <sys/types.h>
#include <pwd.h>
#include <ctype.h>

#include <libu8/libu8.h>
#include <libu8/u8netfns.h>
#if KNO_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

u8_condition kno_ConfigError=_("Configuration error");
u8_condition kno_ReadOnlyConfig=_("Read-only config setting");

u8_condition LoadConfig=_("Loading config");

int kno_trace_config_load = 0;
int kno_trace_config = 0;

static int support_config_c_init_done = 0;

/* Configuration handling */

struct KNO_CONFIG_HANDLER *config_handlers = NULL;

static struct KNO_HASHTABLE *configuration_table;
static struct KNO_HASHTABLE *configuration_defaults;
static lispval path_macro, env_macro, config_macro, now_macro;
static lispval source_macro, glom_macro, string_macro;

static u8_mutex config_lookup_lock;

static struct KNO_CONFIG_FINDER *config_lookupfns = NULL;

static u8_mutex config_register_lock;
static int n_config_handlers = 0;
int kno_configs_initialized = 0;

static u8_string knox_path = KNO_EXEC;

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
    if ((c == '/')||(c==':')||(c=='\\'))
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
  lispval probe = kno_hashtable_get(configuration_table,symbol,VOID);
  /* This lookups configuration information using various methods */
  if ( (VOIDP(probe)) && (kno_configs_initialized) ) {
    /* We only do a search/lookup when configs have been initialized */
    lispval value = KNO_VOID;
    struct KNO_CONFIG_FINDER *scan = config_lookupfns;
    while (scan) {
      value = scan->config_lookup(symbol,scan->config_lookup_data);
      if (VOIDP(value))
        scan = scan->next_lookup;
      else break;}
    if (!(VOIDP(value)))
      kno_hashtable_store(configuration_defaults,symbol,value);
    else value = kno_hashtable_get(configuration_defaults,symbol,KNO_VOID);
    if (!(KNO_VOIDP(value))) kno_hashtable_store(configuration_table,symbol,value);
    return value;}
  else return probe;
}

int record_config(lispval symbol,lispval val)
{
  lispval current = kno_hashtable_get(configuration_table,symbol,VOID);
  if (VOIDP(current)) {
    if (PAIRP(val)) {
      lispval pairpair = kno_make_pair(val,NIL);
      int rv = kno_hashtable_store(configuration_table,symbol,pairpair);
      kno_decref(pairpair);
      return rv;}
    else return kno_hashtable_store(configuration_table,symbol,val);}
  else if (PAIRP(current)) {
    lispval pairpair = kno_make_pair(val,current);
    int rv = kno_hashtable_store(configuration_table,symbol,pairpair);
    kno_decref(pairpair);
    return rv;}
  else if (KNO_EQUAL(current,val)) return 0;
  else {
    lispval cdr = kno_make_pair(current,NIL);
    lispval pairpair = kno_make_pair(val,cdr);
    int rv = kno_hashtable_store(configuration_table,symbol,pairpair);
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

int set_config(lispval symbol,lispval val)
{
  int retval = 0;
  struct KNO_CONFIG_HANDLER *scan = config_handlers;
  while (scan)
    if (KNO_EQ(scan->configname,symbol)) {
      scan->configflags |= KNO_CONFIG_MODIFIED;
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
  if (retval>=0) record_config(symbol,val);
  return retval;
}

KNO_EXPORT int kno_set_config(u8_string var,lispval val)
{
  lispval symbol = config_intern(var);
  return set_config(symbol,val);
}

KNO_EXPORT int kno_handle_config(lispval symbol,lispval val)
{
  return set_config(symbol,val);
}

KNO_EXPORT int kno_set_config_consed(u8_string var,lispval val)
{
  lispval symbol = config_intern(var);
  int retval = set_config(symbol,val);
  if (retval<0) return retval;
  kno_decref(val);
  return retval;
}

static int init_config_val(struct KNO_CONFIG_HANDLER *entry,lispval current,int err)
{
  /* This initializes a config entry using a specified value. */
  if ( (entry->configflags) & KNO_CONFIG_MODIFIED ) return 0;
  else if (KNO_ABORTP(current)) {
    kno_clear_errors(1);
    return -1;}
  if (VOIDP(current)) return 0;
  entry->configflags |= KNO_CONFIG_MODIFIED;
  lispval name =entry->configname;
  void *usedata = entry->configdata;
  if ( (PAIRP(current)) &&
       (!(entry->configflags&KNO_CONFIG_SINGLE_VALUE)) ) {
    /* There have been multiple configuration specifications,
       so run them all backwards. */
    int n = 0;
    lispval scan = current; while (PAIRP(scan)) {scan = KNO_CDR(scan); n++;}
    lispval *vals = u8_alloc_n(n,lispval), *write = vals;
    int count = 0;
    {KNO_DOLIST(cv,current) *write++=cv;}
    n--; while (n>=0) {
      int rv = (entry->config_set_method) ?
	(entry->config_set_method(name,vals[n],usedata)) :
	(0);
      if (rv<0) {
	if (err) { u8_free(vals); return rv; }
	u8_exception ex = u8_pop_exception();
	u8_log(LOGERR,"ConfigError","%q=%q (%s)",
	       entry->configname,vals[n],
	       kno_errstring(ex));
	u8_free_exception(ex,0);}
      else count += rv;
      n--;}
    u8_free(vals);
    return count;}
  else if (KNO_PAIRP(current)) {
    /* KNO_CONFIG_SINGLE_VALUE, so just use the latest */
    lispval last = KNO_CAR(current), scan = KNO_CDR(current);
    while (PAIRP(scan)) {
      last = KNO_CAR(scan);
      scan = KNO_CDR(scan); }
    return entry->config_set_method(name,last,usedata);}
  else if (entry->config_set_method)
    return entry->config_set_method(name,current,usedata);
  else return 0;
}

/* Default configs */

KNO_EXPORT int set_default_config(lispval var,lispval val)
{
  int retval = 0;
  lispval symbol = config_intern(KNO_SYMBOL_NAME(var));
  if (kno_hashtable_probe(configuration_table,symbol)) {
    if (kno_trace_config)
      u8_log(LOG_INFO,"ConfigDefault/ignored",
	     "Config %q already set, ignoring default %q",
	     var,val);
    return 0;}
  else if (!(kno_configs_initialized)) {
    /* If configs haven't finished initialization, just store it in
       the default configs table */
    if (!(kno_hashtable_test(configuration_table,symbol,KNO_VOID)) )
      kno_hashtable_store(configuration_defaults,symbol,val);
    return 1;}
  else {
    struct KNO_CONFIG_HANDLER *scan = config_handlers;
    while (scan) {
      if (KNO_EQ(scan->configname,symbol)) {
	if ( (scan->configflags) & (KNO_CONFIG_MODIFIED) )
	  return 0;
	scan->configflags |= KNO_CONFIG_MODIFIED;
	if (scan->config_set_method)
	  retval = scan->config_set_method(symbol,val,scan->configdata);
	if (kno_trace_config)
	  u8_log(LOG_WARN,"ConfigSet",
		 "Using handler to configure default %s with %q",
		 SYM_NAME(symbol),val);
	break;}
      else scan = scan->config_next;}
    if (retval<0) {
      u8_string errsum = kno_errstring(NULL);
      u8_log(LOG_WARN,kno_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
      u8_free(errsum);}
    else if (retval == 0) {
      if (kno_trace_config)
	u8_log(LOG_WARN,"ConfigSet","Saving config %s with %q",
	       SYM_NAME(symbol),val);
      retval = record_config(symbol,val);}
    else retval = record_config(symbol,val);
    return retval;}
}

KNO_EXPORT int kno_set_default_config(u8_string var,lispval val)
{
  lispval symbol = config_intern(var);
  return kno_handle_default_config(symbol,val);
}

KNO_EXPORT int kno_handle_default_config(lispval var,lispval val)
{
  return set_default_config(var,val);
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
    n_config_handlers++;
    config_handlers = scan;}
  struct KNO_CONFIG_HANDLER *config_entry = scan;
  u8_unlock_mutex(&config_register_lock);
  retval = init_config_val(config_entry,current,1);
  if (old_configdoc) u8_free(old_configdoc);
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

KNO_EXPORT int kno_init_configs()
{
  if (kno_configs_initialized) return 0;
  u8_lock_mutex(&config_register_lock);
  if (kno_configs_initialized) {
    u8_unlock_mutex(&config_register_lock);
    return 0;}
  int n_config_handlers = 0;
  struct KNO_CONFIG_HANDLER *scan = config_handlers;
  while (scan) { n_config_handlers++; scan=scan->config_next; }
  struct KNO_CONFIG_HANDLER *handlers[n_config_handlers];
  scan = config_handlers;
  int config_i = 0, run_count = 0;
  while (scan) {
    if (! ( ( scan->configflags) & (KNO_CONFIG_MODIFIED) ) )
      handlers[config_i++]=scan;
    scan = scan->config_next;}
  kno_configs_initialized=1;
  u8_unlock_mutex(&config_register_lock);
  int i = 0; while (i<config_i) {
    struct KNO_CONFIG_HANDLER *h = handlers[i++];
    lispval cur = config_get(h->configname);
    if (KNO_VOIDP(cur)) continue;
    int rv = init_config_val(h,cur,0);
    kno_decref(cur);
    run_count += rv;}
  return run_count;
}

/* Config lookup methods */

KNO_EXPORT
void kno_register_config_lookup(lispval (*fn)(lispval,void *),void *ldata)
{
  struct KNO_CONFIG_FINDER *entry = u8_alloc(struct KNO_CONFIG_FINDER);
  u8_lock_mutex(&config_lookup_lock);
  struct KNO_CONFIG_FINDER *scan = config_lookupfns;
  struct KNO_CONFIG_FINDER *prev = NULL;
  while (scan) {
    if ( (scan->config_lookup == fn) &&
	 (scan->config_lookup_data == ldata) ) {
      /* Reorder config lookups */
      if (prev) {
	struct KNO_CONFIG_FINDER *next = scan->next_lookup;
	prev->next_lookup = next;
	scan->next_lookup = config_lookupfns;
	config_lookupfns = scan;}
      u8_lock_mutex(&config_lookup_lock);
      return;}
    else {
      prev = scan;;
      scan = scan->next_lookup;}}
  entry->config_lookup = fn;
  entry->config_lookup_data = ldata;
  entry->next_lookup = config_lookupfns;
  config_lookupfns = entry;
  u8_unlock_mutex(&config_lookup_lock);
}

/* Environment config lookup */

static int envconfig_enabled = 1;

static lispval getenv_config_lookup(lispval symbol,void *ignored)
{
  if (! (envconfig_enabled) ) return VOID;
  U8_STATIC_OUTPUT(out,32);
  char *getenv_result;
  u8_string u8result;
  lispval result;
  u8_printf(&out,"KNO_%s",SYM_NAME(symbol));
  getenv_result = getenv(out.u8_outbuf);
  if ( (getenv_result == NULL) || (getenv_result[0] == '\0') ) {
    u8_close_output(&out);
    return VOID;}
  u8result = u8_fromlibc(getenv_result);
  result = kno_parse_arg(u8result);
  if (KNO_PAIRP(result)) {
    lispval interpreted = kno_interpret_config(result);
    kno_decref(result);
    result=interpreted;}
  u8_close_output(&out);
  u8_free(u8result);
  return result;
}

/* File-based configuration */

#if KNO_FILECONFIG_ENABLED
static u8_string configdata_path = NULL;

static lispval file_config_lookup(lispval symbol,void *pathdata)
{
  u8_string path = ((pathdata) ? (pathdata) : (configdata_path));
  u8_string filename =
    u8_find_file(SYM_NAME(symbol),path,u8_file_readablep);
  if (filename) {
    ssize_t n_bytes; lispval results = KNO_EMPTY;
    unsigned char *content = u8_filedata(filename,&n_bytes);
    if (content[0]==0) {
      struct KNO_INBUF in = { 0 };
      in.buffer = in.bufread = content+1;
      in.buflim = in.buffer+n_bytes;
      in.buf_fillfn = NULL;
      lispval result = kno_read_dtype(&in);
      while (in.bufread < in.buflim) {
	if (ABORTED(result)) {
	  kno_decref(results);
	  results = result;
	  break;}
	KNO_ADD_TO_CHOICE(results,result);
	result = kno_read_dtype(&in);}
      KNO_ADD_TO_CHOICE(results,result);}
    else {
      /* Zap any trailing newlines */
      int last_byte = n_bytes-1;
      while ( (content[last_byte]<128) && (isspace(content[last_byte])) ) {
	content[last_byte]='\0'; last_byte--;}
      n_bytes = last_byte+1;
      struct U8_INPUT instream;
      U8_INIT_STRING_INPUT(&instream,n_bytes,content);
      lispval result = kno_read_arg(&instream);
      while (1) {
	if (KNO_ABORTED(result)) {
	  kno_decref(results);
	  results=result;
	  break;}
	else {KNO_ADD_TO_CHOICE(results,result);}
	int c = kno_skip_whitespace(&instream);
	if (c < 0) break; else u8_ungetc(&instream,c);
	result = kno_read_arg(&instream);
	if (result == KNO_EOF) break;}}
    u8_free(filename);
    u8_free(content);
    return kno_simplify_choice(results);}
  else return VOID;
}

static lispval get_config_sources(lispval var,void *data)
{
  lispval paths = KNO_EMPTY_LIST;
  u8_lock_mutex(&config_lookup_lock);
  struct KNO_CONFIG_FINDER *scan = config_lookupfns;
  while (scan) {
    if ( (scan->config_lookup == file_config_lookup) &&
	 (scan->config_lookup_data) ) {
      u8_string s = (u8_string) (scan->config_lookup_data);
      paths = kno_init_pair(NULL,knostring(s),paths);}
    scan = scan->next_lookup;}
  u8_unlock_mutex(&config_lookup_lock);
  lispval in_search_order = kno_reverse_list(paths);
  kno_decref(paths);
  return in_search_order;
}

static int add_config_source(lispval var,lispval val,void *data)
{
  if (KNO_STRINGP(val)) {
    u8_string copy = u8_strdup(KNO_CSTRING(val));
    kno_register_config_lookup(file_config_lookup,(void *)copy);
    return 1;}
  else {
    kno_seterr("NotAString","add_config_dir",NULL,val);
    return -1;}
}

#endif

/* Lots of different ways to specify configuration */

static int do_config_assignment(u8_string assignment)
{
  u8_byte _namebuf[64], *namebuf;
  u8_string equals = strchr(assignment,'=');
  if (equals == NULL) {
    u8_log(LOG_ERR,"InvalidConfig","Couldn't handle %s",assignment);
    return 0;}
  int namelen = equals-assignment, retval;
  lispval value = kno_parse_arg(equals+1);
  if (KNO_ABORTP(value))
    return kno_interr(value);
  else if (KNO_PAIRP(value)) {
    lispval interpreted = kno_interpret_config(value);
    kno_decref(value);
    value = interpreted;}
  if (namelen+1>64)
    namebuf = u8_malloc(namelen+1);
  else namebuf=_namebuf;
  strncpy(namebuf,assignment,namelen); namebuf[namelen]='\0';
  retval = kno_set_config_consed(namebuf,value);
  if (namebuf!=_namebuf) u8_free(namebuf);
  return retval;
}

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
      memcpy(copied,assignment,len+1);
      while ((scan=strchr(start,sep))) {
	if ( (scan>start) && (scan[-1] == '\\') ) {
          scan = strchr(scan+1,sep);
	  if (scan == NULL) break;
          continue;}
	if (scan) *scan = '\0';
	if ( scan == start) break;
        int rv = kno_config_assignment(start);
        if (rv<0) {
          u8_seterr("BadConfig","kno_config_assigment",u8_strdup(start));
          return rv;}
	if (scan == NULL) break;
        start=scan+1;
        count++;}
      if (*start) {
        rv = do_config_assignment(start);
        if (rv>=0) count++;}
      if (rv<0)
        return -(count+1);
      else return count;}
    else return do_config_assignment(assignment);}
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
      lispval interpreted = kno_interpret_config(value);
      kno_decref(value);
      value = interpreted;}
    if (namelen+1>64)
      namebuf = u8_malloc(namelen+1);
    else namebuf=_namebuf;
    strncpy(namebuf,assignment,namelen); namebuf[namelen]='\0';
    if (!(kno_hashtable_test(configuration_table,config_intern(namebuf),VOID)))
      retval = kno_set_config_consed(namebuf,value);
    if (namebuf!=_namebuf) u8_free(namebuf);
    return retval;}
  else return -1;
}

/* config-time read macros */

KNO_EXPORT lispval kno_interpret_config(lispval expr)
{
  if ( (KNO_PAIRP(expr)) &&
       (KNO_SYMBOLP(KNO_CAR(expr))) &&
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
      else if ( (KNO_PAIRP(arg)) && (KNO_SYMBOLP(KNO_CAR(arg))) ) {
	lispval v = kno_config_get(KNO_SYMBOL_NAME(KNO_CAR(arg)));
	if (!(KNO_VOIDP(v)))
	  return v;
	else if (KNO_PAIRP(KNO_CDR(v))) {
	  lispval dflt = KNO_CADR(v);
	  return kno_incref(dflt);}
	else return kno_incref(expr);}
      else return kno_incref(expr);}
    else if (head == env_macro) {
      u8_string varname; int free_varname = 0;
      if (KNO_SYMBOLP(arg)) {
        varname = u8_upcase(KNO_SYMBOL_NAME(arg));
        free_varname = 1;}
      else if (KNO_STRINGP(arg))
        varname = KNO_CSTRING(arg);
      else if ( (KNO_PAIRP(arg)) && (KNO_SYMBOLP(KNO_CAR(arg))) ) {
	varname = KNO_SYMBOL_NAME(KNO_CAR(arg));
	free_varname = 1;}
      else if ( (KNO_PAIRP(arg)) && (KNO_STRINGP(KNO_CAR(arg))) )
	varname = KNO_CSTRING(KNO_CAR(arg));
      else return kno_incref(expr);
      u8_string strval = u8_getenv(varname);
      if (free_varname) u8_free(varname);
      if (strval) {
        lispval val = knostring(strval);
        u8_free(strval);
        return val;}
      else if ( (KNO_PAIRP(arg)) && (KNO_PAIRP(KNO_CDR(arg))) ) {
	lispval dflt = KNO_CADR(arg);
	return kno_incref(dflt);}
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
    else if ( (head == glom_macro) || (head == string_macro) ) {
      if (KNO_PAIRP(arg)) {
	U8_STATIC_OUTPUT(string,128);
	lispval scan = arg; while (KNO_PAIRP(scan)) {
	  lispval car = KNO_CAR(scan); scan = KNO_CDR(scan);
	  lispval elt = (KNO_PAIRP(car)) ? (kno_interpret_config(car)) :
	    (kno_incref(car));
	  if (KNO_STRINGP(elt))
	    u8_putn(stringout,KNO_CSTRING(elt),KNO_STRLEN(elt));
	  else kno_unparse(stringout,elt);
	  kno_decref(elt);}
	if (scan != KNO_EMPTY_LIST) {
	  u8_log(LOGWARN,"BadConfigValue","Tail=%q in %q",
		 scan,arg);}
	lispval result = kno_stream2string(stringout);
	u8_close_output(stringout);
	return result;}
      else return kno_incref(expr);}
    else return kno_incref(expr);}
  else return kno_incref(expr);
}

int skip_space(u8_input in)
{
  int c = u8_getc(in);
  while ((c>=0) && (u8_isspace(c))) c = u8_getc(in);
  return c;
}

/* This reads a config file.  It consists of a series of entries, each of which is
   either a list (var value) or an assignment var = value.
   Both # and ; are comment characters at the beginning of lines. */
static int read_config(U8_INPUT *in,int dflt)
{
  int c, count = 0;
  while ((c = skip_space(in))>=0) {
    u8_string buf = NULL;
    if ( (c == '#') || (c == ';') )
      buf = u8_gets(in);
    else {
      int doparse = 0;
      if ( (c == '(') || (c == '[') ) doparse=1;
      else {
	int nextc = u8_probec(in);
	if ( (c == '/') && (nextc == '/') )
	  buf = u8_gets(in);
	else if ( (c == '#') && (nextc == '[') )
	  doparse=1;
	else NO_ELSE;}
      if (doparse) {
	u8_ungetc(in,c);
	lispval entry = kno_parser(in);
	if (KNO_ABORTP(entry))
	  return kno_interr(entry);
	else if ((PAIRP(entry)) &&
		 (SYMBOLP(KNO_CAR(entry))) &&
		 (PAIRP(KNO_CDR(entry)))) {
	  lispval val = kno_interpret_config(KNO_CADR(entry));
	  int rv = (dflt) ?
	    (kno_set_default_config(SYM_NAME(KNO_CAR(entry)),val)<0) :
	    (kno_set_config(SYM_NAME(KNO_CAR(entry)),val)<0);
	  if (rv < 0) {
	    kno_decref(val);
	    if (buf) u8_free(buf);
	    return KNO_ERR(-1,kno_ConfigError,"kno_read_config",NULL,entry);}
	  if (rv) count++;
	  kno_decref(entry);
	  kno_decref(val);}
	else if ( (KNO_SLOTMAPP(entry)) || (KNO_SCHEMAPP(entry)) ) {
	  lispval keys = kno_getkeys(entry);
	  KNO_DO_CHOICES(key,keys) {
	    u8_string conf_name =
	      (KNO_SYMBOLP(key)) ? (KNO_SYMBOL_NAME(key)) :
	      (KNO_STRINGP(key)) ? (KNO_CSTRING(key)) :
	      (NULL);
	    if (conf_name) {
	      lispval val = kno_get(entry,key,KNO_VOID);
	      if (! ( (KNO_VOIDP(val)) || (KNO_DEFAULTP(val)) || (KNO_EMPTYP(val)) ) ) {
		KNO_DO_CHOICES(v,val) {
		  int rv = (dflt) ?
		    (kno_set_default_config(conf_name,v)) :
		    (kno_set_config(conf_name,v));
		  if (rv) count++;}
		kno_decref(val);}}}
	  kno_decref(keys);
	  kno_decref(entry);}
	else {
	  if (buf) u8_free(buf);
	  return KNO_ERR(-1,kno_ConfigError,"kno_read_config",NULL,entry);}}
      else {
	u8_ungetc(in,c);
	buf = u8_gets(in);
	int rv = (dflt) ?
	  (kno_default_config_assignment(buf)<0) :
	  (kno_config_assignment(buf)<0);
	if (rv<0) {
	  if (buf) u8_free(buf);
	  return kno_reterr(kno_ConfigError,"kno_read_config",buf,VOID);}
	else if (rv)
	  count++;
	else NO_ELSE;}
      if (buf) u8_free(buf);}}
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
KNO_EXPORT lispval kno_intboolconfig_get(lispval ignored,void *vptr)
{
  int *ptr = vptr; int v = *ptr;
  if (v<0)
    return KNO_FALSE;
  else return KNO_INT(v);
}
KNO_EXPORT int kno_intboolconfig_set(lispval ignored,lispval v,void *vptr)
{
  int *ptr = vptr;
  if (KNO_FALSEP(v)) {
    *ptr = -1;
    return 1;}
  else if (KNO_FIXNUMP(v)) {
    long long iv = FIX2INT(v);
    if (iv < 0) {
      *ptr = -1;
      return 1;}
    else if (iv < INT_MAX) {
      *ptr = iv;
      return 1;}}
  else NO_ELSE;
  return kno_reterr
    (kno_TypeError,"kno_uintconfig_set",
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
    u8_log(LOGWARN,"CWD","Setting current directory to %s",KNO_CSTRING(val));
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

/* Feature configuration */

static lispval features_config_get(lispval var,void *data)
{
  return kno_getkeys(kno_runtime_features);
}

static int features_config_set(lispval var,lispval val,void *data)
{
  if (KNO_PAIRP(val))
    return kno_store(kno_runtime_features,KNO_CAR(val),KNO_CDR(val));
  else return kno_store(kno_runtime_features,val,KNO_TRUE);
}

/* Loading configs */

static u8_string get_config_path(u8_string spec)
{
  if (*spec == '/')
    return u8_strdup(spec);
  else if (((u8_string)(strstr(spec,"file:"))) == spec)
    return u8_strdup(spec+7);
  else if (strchr(spec,':')) /* It's got a schema, assume it's absolute */
    return u8_strdup(spec);
  else {
    u8_string sourcebase = kno_sourcebase();
    if (sourcebase) {
      u8_string full = u8_mkpath(spec,sourcebase);
      if (kno_probe_source(full,NULL,NULL,NULL))
	return full;
      else u8_free(full);}
    u8_string abspath = u8_abspath(spec,NULL);
    if (u8_file_existsp(abspath))
      return abspath;
    else {
      u8_free(abspath);
      return NULL;}}
}

KNO_EXPORT int kno_load_config(u8_string sourceid)
{
  struct U8_INPUT stream; int retval;
  u8_string sourcebase = NULL, outer_sourcebase = NULL;
  u8_string fullpath = get_config_path(sourceid);
  if (fullpath == NULL)
    return KNO_ERR(-1,"MissingConfig","kno_load_config",sourceid,VOID);
  u8_string content = kno_get_source(fullpath,NULL,&sourcebase,NULL,NULL);
  u8_free(fullpath);
  if (content == NULL)
    return -1;
  else if (sourcebase) {
    outer_sourcebase = kno_bind_sourcebase(sourcebase);}
  else outer_sourcebase = NULL;
  U8_INIT_STRING_INPUT((&stream),-1,content);
  if ( (kno_trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,LoadConfig,
	   "Loading config %s (%d bytes)",sourcebase,u8_strlen(content));
  retval = kno_read_config(&stream);
  if ( (kno_trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,LoadConfig,"Loaded config %s",sourcebase);
  if ( (sourcebase) || (outer_sourcebase) ) {
    kno_restore_sourcebase(outer_sourcebase);
    if (sourcebase) u8_free(sourcebase);}
  u8_free(content);
  return retval;
}

KNO_EXPORT int kno_load_default_config(u8_string sourceid)
{
  struct U8_INPUT stream; int retval;
  u8_string sourcebase = NULL, outer_sourcebase = NULL;
  u8_string fullpath = get_config_path(sourceid);
  if (fullpath == NULL)
    return KNO_ERR(-1,"MissingConfig","kno_load_default_config",sourceid,VOID);
  u8_string content = kno_get_source(fullpath,NULL,&sourcebase,NULL,NULL);
  u8_free(fullpath);
  if (content == NULL)
    return -1;
  else if (sourcebase) {
    outer_sourcebase = kno_bind_sourcebase(sourcebase);}
  else outer_sourcebase = NULL;
  U8_INIT_STRING_INPUT((&stream),-1,content);
  if ( (kno_trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,LoadConfig,
	   "Loading default config %s (%d bytes)",sourcebase,u8_strlen(content));
  retval = kno_read_default_config(&stream);
  if ( (kno_trace_config_load) || (kno_trace_config) )
    u8_log(LOG_WARN,LoadConfig,"Loaded default config %s",sourcebase);
  if ( (sourcebase) || (outer_sourcebase) ) {
    kno_restore_sourcebase(outer_sourcebase);
    if (sourcebase) u8_free(sourcebase);}
  u8_free(content);
  return retval;
}

/* Config config */

static u8_mutex config_file_lock;

static KNO_CONFIG_RECORD *config_records = NULL, *config_stack = NULL;

static lispval get_config_files(lispval var,void U8_MAYBE_UNUSED *data)
{
  struct KNO_CONFIG_RECORD *scan; lispval result = NIL;
  u8_lock_mutex(&config_file_lock);
  scan = config_records; while (scan) {
    result = kno_conspair(kno_mkstring(scan->config_filename),result);
    scan = scan->loaded_after;}
  u8_unlock_mutex(&config_file_lock);
  return result;
}

static int add_config_file_helper(lispval var,lispval val,
				  void U8_MAYBE_UNUSED *data,
				  int isopt,int isdflt)
{
  if (!(STRINGP(val))) return -1;
  else if (STRLEN(val)==0) return 0;
  else {
    int retval;
    struct KNO_CONFIG_RECORD on_stack, *scan, *newrec;
    u8_string root = kno_sourcebase();
    u8_string pathname = u8_abspath(CSTRING(val),root);
    u8_lock_mutex(&config_file_lock);
    scan = config_stack; while (scan) {
      if (strcmp(scan->config_filename,pathname)==0) {
	u8_unlock_mutex(&config_file_lock);
	if ( (kno_trace_config) || (kno_trace_config_load) )
	  u8_log(LOGINFO,LoadConfig,
		 "Skipping redundant reload of %s from the config %q",
		 KNO_CSTRING(val),var);
	u8_free(pathname);
	return 0;}
      else scan = scan->loaded_after;}
    if ( (kno_trace_config) || (kno_trace_config_load) )
      u8_log(LOGWARN,LoadConfig,
	     "Loading the config file %s in response to a %q directive",
	     KNO_CSTRING(val),var);
    memset(&on_stack,0,sizeof(struct KNO_CONFIG_RECORD));
    on_stack.config_filename = pathname;
    on_stack.loaded_after = config_stack;
    config_stack = &on_stack;
    u8_unlock_mutex(&config_file_lock);
    if (isdflt)
      retval = kno_load_default_config(pathname);
    else retval = kno_load_config(pathname);
    u8_lock_mutex(&config_file_lock);
    if (retval<0) {
      if (isopt) {
	u8_free(pathname); config_stack = on_stack.loaded_after;
	u8_unlock_mutex(&config_file_lock);
	u8_pop_exception();
	return 0;}
      u8_free(pathname); config_stack = on_stack.loaded_after;
      u8_unlock_mutex(&config_file_lock);
      return retval;}
    newrec = u8_alloc(struct KNO_CONFIG_RECORD);
    newrec->config_filename = pathname;
    newrec->loaded_after = config_records;
    config_records = newrec;
    config_stack = on_stack.loaded_after;
    u8_unlock_mutex(&config_file_lock);
    return retval;}
}

static int add_config_file(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,0,0);
}

static int add_opt_config_file(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,1,0);
}

static int add_default_config_file(lispval var,lispval val,void U8_MAYBE_UNUSED *data)
{
  return add_config_file_helper(var,val,data,1,1);
}

/* Initialization */

void kno_init_configs_c()
{
  if (support_config_c_init_done)
    return;
  else support_config_c_init_done=1;

  u8_register_source_file(_FILEINFO);

  if (configdata_path == NULL)
    configdata_path = kno_syspath(KNO_CONFIG_FILE_PATH);

  configuration_table = (kno_hashtable) kno_make_hashtable(NULL,72);
  configuration_defaults = (kno_hashtable) kno_make_hashtable(NULL,72);

  u8_init_mutex(&config_lookup_lock);
  u8_init_mutex(&config_register_lock);
  u8_init_mutex(&config_file_lock);

  path_macro = kno_intern("#path");
  config_macro = kno_intern("#config");
  now_macro = kno_intern("#now");
  env_macro = kno_intern("#env");
  source_macro = kno_intern("#source");
  glom_macro = kno_intern("#glom");
  string_macro = kno_intern("#string");

  if ( (knox_path) && (strcmp(knox_path,KNO_EXEC)==0) )
    knox_path=kno_syspath(KNO_EXEC);

  kno_register_config
    ("KNOVERSION",_("Get the Kno version string"),
     knoversion_config_get,kno_readonly_config_set,NULL);
  kno_register_config
    ("KNOVERSION",_("Get the Kno version identifier"),
     knoversion_config_get,kno_readonly_config_set,NULL);
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
    ("U8VERSION",_("Get the libu8 version identifier"),
     u8version_config_get,kno_readonly_config_set,NULL);
  kno_register_config
    ("U8MAJOR",_("Get the libu8 major version number"),
     u8major_config_get,kno_readonly_config_set,NULL);

  kno_register_config
    ("LOGLEVEL",_("default loglevel (from libu8) for the session"),
     kno_intconfig_get,loglevelconfig_set,&u8_loglevel);

  kno_register_config("SYSROOT","Sets the KNO_PATH ",
		      kno_sconfig_get,NULL,&kno_sysroot);

#if KNO_FILECONFIG_ENABLED
#if KNO_FILECONFIG_DEFAULTS
  kno_register_config_lookup(file_config_lookup,NULL);
  kno_register_config
    ("CONFIGDATA",_("Default directory for looking up config entries"),
     kno_sconfig_get,kno_sconfig_set,&configdata_path);
#endif
  kno_register_config
    ("CONFIGSRC",
     _("Directory/path (cumulative) for looking up config settings"),
     get_config_sources,add_config_source,NULL);
#endif

  if (! (getenv("KNO_DISABLE_ENVCONFIG")) )
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

  kno_register_config("ENVCONFIG",
		      "Check the POSIX environment for configuration information",
                      kno_boolconfig_get,kno_boolconfig_set,
                      &envconfig_enabled);

  kno_register_config
    ("KNOX",_("Get the Kno executable path"),
     kno_sconfig_get,kno_sconfig_set,&knox_path);


  kno_register_config("FEATURES",
		      "Defined runtime features in this session",
		      features_config_get,features_config_set,
		      NULL);

  kno_register_config("BIGCHOICE",
		      "When to use a merge sort (bigsort) to sort choices",
		      kno_sizeconfig_get,kno_sizeconfig_set,
		      &kno_bigchoice_threshold);
  kno_register_config("BIGSORT:BLOCKSIZE",
		      "Size of chunk size for bigsort (merge sort) of choices",
		      kno_sizeconfig_get,kno_sizeconfig_set,
		      &kno_bigsort_blocksize);


  kno_register_config
    ("TRACECONFIG",_("whether to trace configuration"),
     kno_boolconfig_get,kno_boolconfig_set,&kno_trace_config);

  kno_register_config("CONFIG","Add a CONFIG file/URI to process",
		      get_config_files,add_config_file,NULL);
  kno_register_config("DEFAULTS","Add a CONFIG file/URI to process as defaults",
		      get_config_files,add_default_config_file,NULL);
  kno_register_config("OPTCONFIG","Add an optional CONFIG file/URI to process",
		      get_config_files,add_opt_config_file,NULL);
  kno_register_config("TRACELOADCONFIG","Trace config file loading",
		      kno_boolconfig_get,kno_boolconfig_set,&kno_trace_config_load);
}


