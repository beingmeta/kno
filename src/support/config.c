/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2018 beingmeta, inc.
   This file is part of beingmeta's FramerD platform and is copyright
   and a valuable trade secret of beingmeta, inc.
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

#include "framerd/fdsource.h"
#include "framerd/dtype.h"
#include "framerd/numbers.h"
#include "framerd/apply.h"

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
#if FD_FILECONFIG_ENABLED
#include <libu8/u8filefns.h>
#include <libu8/libu8io.h>
#endif

u8_condition fd_ConfigError=_("Configuration error");
u8_condition fd_ReadOnlyConfig=_("Read-only config setting");

int fd_trace_config = 0;

static int support_config_c_init_done = 0;

/* Configuration handling */

struct FD_CONFIG_HANDLER *config_handlers = NULL;

static lispval configuration_table;
static lispval path_macro, env_macro, config_macro, now_macro;
static lispval glom_macro;

static u8_mutex config_lookup_lock;
static u8_mutex config_register_lock;

static struct FD_CONFIG_FINDER *config_lookupfns = NULL;

static lispval config_intern(u8_string start)
{
  U8_OUTPUT nameout; u8_byte buf[64];
  const u8_byte *scan = start;
  U8_INIT_STATIC_OUTPUT_BUF(nameout,64,buf);
  while (*scan) {
    int c = u8_sgetc(&scan);
    if ((c == '/')||(c==':')) u8_putc(&nameout,c);
    else if (u8_ispunct(c)) {}
    else if (u8_isupper(c)) u8_putc(&nameout,c);
    else u8_putc(&nameout,u8_toupper(c));}
  if (nameout.u8_streaminfo&U8_STREAM_OWNS_BUF) {
    lispval symbol=
      fd_make_symbol(nameout.u8_outbuf,nameout.u8_write-nameout.u8_outbuf);
    u8_close((u8_stream)&nameout);
    return symbol;}
  else return fd_make_symbol
         (nameout.u8_outbuf,nameout.u8_write-nameout.u8_outbuf);
}

FD_EXPORT
void fd_register_config_lookup(lispval (*fn)(lispval,void *),void *ldata)
{
  struct FD_CONFIG_FINDER *entry=
    u8_alloc(struct FD_CONFIG_FINDER);
  u8_lock_mutex(&config_lookup_lock);
  entry->config_lookup = fn;
  entry->config_lookup_data = ldata;
  entry->next_lookup = config_lookupfns;
  config_lookupfns = entry;
  u8_unlock_mutex(&config_lookup_lock);
}

static lispval config_get(u8_string var)
{
  lispval symbol = config_intern(var);
  lispval probe = fd_get(configuration_table,symbol,VOID);
  /* This lookups configuration information using various methods */
  if (VOIDP(probe)) {
    lispval value = VOID;
    struct FD_CONFIG_FINDER *scan = config_lookupfns;
    while (scan) {
      value = scan->config_lookup(symbol,scan->config_lookup_data);
      if (VOIDP(value))
        scan = scan->next_lookup;
      else break;}
    fd_store(configuration_table,symbol,value);
    return value;}
  else return probe;
}

static lispval getenv_config_lookup(lispval symbol,void *ignored)
{
  U8_OUTPUT out;
  char *getenv_result;
  u8_string u8result;
  lispval result;
  U8_INIT_OUTPUT(&out,32);
  u8_printf(&out,"FD_%s",SYM_NAME(symbol));
  getenv_result = getenv(out.u8_outbuf);
  if (getenv_result == NULL) {
    u8_free(out.u8_outbuf); return VOID;}
  u8result = u8_fromlibc(getenv_result);
  result = fd_parse_arg(u8result);
  u8_free(out.u8_outbuf); u8_free(u8result);
  return result;
}

int set_config(u8_string var,lispval val)
{
  lispval symbol = config_intern(var);
  lispval current = fd_get(configuration_table,symbol,VOID);
  if (VOIDP(current)) {
    if (PAIRP(val)) {
      lispval pairpair = fd_make_pair(val,NIL);
      int rv = fd_store(configuration_table,symbol,pairpair);
      fd_decref(pairpair);
      return rv;}
    else return fd_store(configuration_table,symbol,val);}
  else if (PAIRP(current)) {
    lispval pairpair = fd_make_pair(val,current);
    int rv = fd_store(configuration_table,symbol,pairpair);
    fd_decref(pairpair);
    return rv;}
  else if (FD_EQUAL(current,val)) return 0;
  else {
    lispval cdr = fd_make_pair(current,NIL);
    lispval pairpair = fd_make_pair(val,cdr);
    int rv = fd_store(configuration_table,symbol,pairpair);
    fd_decref(cdr); fd_decref(pairpair);
    return rv;}
}

/* File-based configuration */

#if FD_FILECONFIG_ENABLED
static u8_string configdata_path = NULL;

static lispval file_config_lookup(lispval symbol,void *pathdata)
{
  u8_string path=
    ((pathdata == NULL) ?
     ((configdata_path) ? (configdata_path) : ((u8_string)FD_CONFIG_FILE_PATH)) :
     ((u8_string)pathdata));
  u8_string filename = u8_find_file(SYM_NAME(symbol),path,u8_file_readablep);
  if (filename) {
    int n_bytes; lispval result;
    unsigned char *content = u8_filedata(filename,&n_bytes);
    if (content[0]==0) {
      struct FD_INBUF in = { 0 };
      in.buffer = in.bufread = content+1; in.buflim = in.buffer+n_bytes;
      in.buf_fillfn = NULL;
      result = fd_read_dtype(&in);}
    else {
      /* Zap any trailing newlines */
      if ((n_bytes>1) && (content[n_bytes-1]=='\n'))
        content[n_bytes-1]='\0';
      if ((n_bytes>2) && (content[n_bytes-2]=='\r'))
        content[n_bytes-2]='\0';
      result = fd_parse_arg(content);}
    u8_free(filename);
    u8_free(content);
    return result;}
  else return VOID;
}

#endif

/* API functions */

FD_EXPORT lispval fd_config_get(u8_string var)
{
  lispval symbol = config_intern(var);
  struct FD_CONFIG_HANDLER *scan = config_handlers;
  while (scan)
    if (FD_EQ(scan->configname,symbol)) {
      lispval val;
      val = scan->config_get_method(symbol,scan->configdata);
      return val;}
    else scan = scan->config_next;
  return config_get(var);
}

FD_EXPORT int fd_set_config(u8_string var,lispval val)
{
  lispval symbol = config_intern(var); int retval = 0;
  struct FD_CONFIG_HANDLER *scan = config_handlers;
  while (scan)
    if (FD_EQ(scan->configname,symbol)) {
      scan->configflags = scan->configflags|FD_CONFIG_ALREADY_MODIFIED;
      retval = scan->config_set_method(symbol,val,scan->configdata);
      if (fd_trace_config)
        u8_log(LOG_WARN,"ConfigSet",
               "Using handler to configure %s (%s) with %q",
               var,SYM_NAME(symbol),val);
      break;}
    else scan = scan->config_next;
  if ((!(scan))&&(fd_trace_config))
    u8_log(LOG_WARN,"ConfigSet","Configuring %s (%s) with %q",
           var,SYM_NAME(symbol),val);
  set_config(var,val);
  if (retval<0) {
    u8_string errsum = fd_errstring(NULL);
    u8_log(LOG_WARN,fd_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
    u8_free(errsum);}
  return retval;
}

FD_EXPORT int fd_default_config(u8_string var,lispval val)
{
  lispval symbol = config_intern(var); int retval = 1;
  struct FD_CONFIG_HANDLER *scan = config_handlers;
  while (scan)
    if (FD_EQ(scan->configname,symbol)) {
      if ((scan->configflags)&(FD_CONFIG_ALREADY_MODIFIED)) return 0;
      scan->configflags = scan->configflags|FD_CONFIG_ALREADY_MODIFIED;
      retval = scan->config_set_method(symbol,val,scan->configdata);
      if (fd_trace_config)
        u8_log(LOG_WARN,"ConfigSet",
               "Using handler to configure default %s (%s) with %q",
               var,SYM_NAME(symbol),val);
      break;}
    else scan = scan->config_next;
  if (fd_test(configuration_table,symbol,VOID)) return 0;
  else {
    if ((!(scan))&&(fd_trace_config))
      u8_log(LOG_WARN,"ConfigSet","Configuring %s (%s) with %q",
             var,SYM_NAME(symbol),val);
    retval = set_config(var,val);}
  if (retval<0) {
    u8_string errsum = fd_errstring(NULL);
    u8_log(LOG_WARN,fd_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
    u8_free(errsum);}
  return retval;
}

FD_EXPORT int fd_set_config_consed(u8_string var,lispval val)
{
  int retval = fd_set_config(var,val);
  if (retval<0) return retval;
  fd_decref(val);
  return retval;
}

/* Registering new configuration values */

FD_EXPORT int fd_register_config_x
  (u8_string var,u8_string doc,
   lispval (*getfn)(lispval,void *),
   int (*setfn)(lispval,lispval,void *),
   void *data,int (*reuse)(struct FD_CONFIG_HANDLER *scan))
{
  lispval symbol = config_intern(var), current = config_get(var);
  int retval = 0;
  struct FD_CONFIG_HANDLER *scan;
  u8_lock_mutex(&config_register_lock);
  scan = config_handlers;
  while (scan)
    if (FD_EQ(scan->configname,symbol)) {
      if (reuse) reuse(scan);
      if (doc) {
        /* We don't override a real doc with a NULL doc.
           Possibly not the right thing. */
        if (scan->configdoc) u8_free(scan->configdoc);
        scan->configdoc = u8_strdup(doc);}
      scan->config_get_method = getfn;
      scan->config_set_method = setfn;
      scan->configdata = data;
      break;}
    else scan = scan->config_next;
  if (scan == NULL) {
    current = config_get(var);
    scan = u8_alloc(struct FD_CONFIG_HANDLER);
    scan->configname = symbol;
    if (doc) scan->configdoc = u8_strdup(doc); else scan->configdoc = NULL;
    scan->configflags = 0;
    scan->config_get_method = getfn;
    scan->config_set_method = setfn;
    scan->configdata = data;
    scan->config_next = config_handlers;
    config_handlers = scan;}
  u8_unlock_mutex(&config_register_lock);
  if (FD_ABORTP(current)) {
    fd_clear_errors(1);
    retval = -1;}
  else if (VOIDP(current)) {}
  else if (PAIRP(current)) {
    /* There have been multiple configuration specifications,
       so run them all backwards. */
    int n = 0; lispval *vals, *write;
    {lispval scan = current; while (PAIRP(scan)) {scan = FD_CDR(scan); n++;}}
    vals = u8_alloc_n(n,lispval); write = vals;
    {FD_DOLIST(cv,current) *write++=cv;}
    while (n>0) {
      n = n-1;
      if (retval<0) {
        u8_free(vals); fd_decref(current);
        return retval;}
      else retval = setfn(symbol,vals[n],data);}
    fd_decref(current); u8_free(vals);
    return retval;}
  else retval = setfn(symbol,current,data);
  fd_decref(current);
  return retval;
}

FD_EXPORT int fd_register_config
  (u8_string var,u8_string doc,
   lispval (*getfn)(lispval,void *),
   int (*setfn)(lispval,lispval,void *),
   void *data)
{
  return fd_register_config_x(var,doc,getfn,setfn,data,NULL);
}

FD_EXPORT lispval fd_all_configs(int with_docs)
{
  lispval results = EMPTY;
  struct FD_CONFIG_HANDLER *scan;
  u8_lock_mutex(&config_register_lock); {
    scan = config_handlers;
    while (scan) {
      lispval var = scan->configname;
      if (with_docs) {
        lispval doc = ((scan->configdoc)?
                    (fdstring(scan->configdoc)):
                    (NIL));
        lispval pair = fd_conspair(var,doc); fd_incref(var);
        CHOICE_ADD(results,pair);}
      else {fd_incref(var); CHOICE_ADD(results,var);}
      scan = scan->config_next;}}
  u8_unlock_mutex(&config_register_lock);
  return results;
}

/* Lots of different ways to specify configuration */

/* This takes a string of the form var = value */
FD_EXPORT int fd_config_assignment(u8_string assignment)
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
        int rv = fd_config_assignment(start);
        if (rv<0) {
          u8_seterr("BadConfig","fd_config_assigment",u8_strdup(start));
          return rv;}
        start=scan+1;
        count++;}
      if (*start) {
        rv = fd_config_assignment(start);
        if (rv>=0) count++;}
      if (rv<0)
        return -(count+1);
      else return count;}
    else {
      u8_byte _namebuf[64], *namebuf;
      int namelen = equals-assignment, retval;
      lispval value = fd_parse_arg(equals+1);
      if (FD_ABORTP(value))
        return fd_interr(value);
      else if (FD_PAIRP(value)) {
        lispval interpreted = fd_interpret_value(value);
        fd_decref(value);
        value = interpreted;}
      if (namelen+1>64)
        namebuf = u8_malloc(namelen+1);
      else namebuf=_namebuf;
      strncpy(namebuf,assignment,namelen); namebuf[namelen]='\0';
      retval = fd_set_config_consed(namebuf,value);
      if (namebuf!=_namebuf) u8_free(namebuf);
      return retval;}}
  else return -1;
}

/* This takes a string of the form var = value */
FD_EXPORT int fd_default_config_assignment(u8_string assignment)
{
  u8_byte *equals;
  if ((equals = (strchr(assignment,'=')))) {
    u8_byte _namebuf[64], *namebuf;
    int namelen = equals-assignment, retval = 0;
    lispval value = fd_parse_arg(equals+1);
    if (FD_ABORTP(value))
      return fd_interr(value);
    else if (FD_PAIRP(value)) {
      lispval interpreted = fd_interpret_value(value);
      fd_decref(value);
      value = interpreted;}
    if (namelen+1>64)
      namebuf = u8_malloc(namelen+1);
    else namebuf=_namebuf;
    strncpy(namebuf,assignment,namelen); namebuf[namelen]='\0';
    if (!(fd_test(configuration_table,config_intern(namebuf),VOID)))
      retval = fd_set_config_consed(namebuf,value);
    if (namebuf!=_namebuf) u8_free(namebuf);
    return retval;}
  else return -1;
}

#define EXPR_STARTS_WITH(expr,sym) \
  ( (FD_PAIRP(expr)) &&            \
    (FD_PAIRP(FD_CDR(expr))) &&    \
    ( (FD_CAR(expr)) == (sym) ) )

FD_EXPORT lispval fd_interpret_value(lispval expr)
{
  if ( (FD_PAIRP(expr)) &&
       (FD_PAIRP(FD_CDR(expr))) &&
       (FD_CDDR(expr) == FD_NIL) ) {
    lispval head = FD_CAR(expr);
    lispval arg = FD_CADR(expr);
    if (head == path_macro) {
      if ( (FD_STRINGP(arg)) ) {
        u8_string fullpath = (fd_sourcebase()) ?
          (fd_get_component(FD_CSTRING(arg))) :
          (u8_abspath(FD_CSTRING(arg),NULL));
        return fd_init_string(NULL,-1,fullpath);}
      else return fd_incref(arg);}
    else if (head == config_macro) {
      if (FD_SYMBOLP(arg)) {
        lispval v = fd_config_get(FD_SYMBOL_NAME(arg));
        if (FD_VOIDP(v))
          return expr;
        else return v;}}
    else if (head == env_macro) {
      if ( (FD_SYMBOLP(arg)) || (FD_STRINGP(arg)) ) {
        u8_string strval = (FD_SYMBOLP(arg)) ?
          (u8_getenv(FD_SYMBOL_NAME(arg))) :
          (u8_getenv(FD_CSTRING(arg)));
        if (strval) {
          lispval val = fd_lispstring(strval);
          u8_free(strval);
          return val;}
        else return FD_FALSE;}
      else return expr;}
    else if (head == now_macro) {
      if (FD_SYMBOLP(arg)) {
        lispval now = fd_make_timestamp(NULL);
        lispval v = fd_get(now,arg,FD_VOID);
        fd_decref(now);
        if (FD_VOIDP(v))
          return expr;
        else return v;}}
    else NO_ELSE;
    return fd_incref(expr);}
  else return fd_incref(expr);
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
      entry = fd_parser(in);
      if (FD_ABORTP(entry))
        return fd_interr(entry);
      else if ((PAIRP(entry)) &&
               (SYMBOLP(FD_CAR(entry))) &&
               (PAIRP(FD_CDR(entry)))) {
        lispval val = fd_interpret_value(FD_CADR(entry));
        int rv = (dflt) ?
          (fd_default_config(SYM_NAME(FD_CAR(entry)),val)<0) :
          (fd_set_config(SYM_NAME(FD_CAR(entry)),val)<0);
        if (rv < 0) {
          fd_seterr(fd_ConfigError,"fd_read_config",NULL,entry);
          fd_decref(val);
          return -1;}
        if (rv) count++;
        fd_decref(entry);
        fd_decref(val);}
      else {
        fd_seterr(fd_ConfigError,"fd_read_config",NULL,entry);
        return -1;}}
    else if ((u8_isspace(c)) || (u8_isctrl(c))) {}
    else {
      u8_ungetc(in,c);
      buf = u8_gets(in);
      int rv = (dflt) ?
        (fd_default_config_assignment(buf)<0) :
        (fd_config_assignment(buf)<0);
      if (rv<0)
        return fd_reterr(fd_ConfigError,"fd_read_config",buf,VOID);
      else if (rv)
        count++;
      else NO_ELSE;
      u8_free(buf);}
  return count;
}

/* This reads a config file.  It consists of a series of entries, each of which is
   either a list (var value) or an assignment var = value.
   Both # and ; are comment characters */
FD_EXPORT int fd_read_config(U8_INPUT *in)
{
  return read_config(in,0);
}

/* This reads a config file.  It consists of a series of entries, each of which is
   either a list (var value) or an assignment var = value.
   Both # and ; are comment characters */
FD_EXPORT int fd_read_default_config(U8_INPUT *in)
{
  return read_config(in,1);
}

/* Utility configuration functions */

/* This set method just returns an error */
FD_EXPORT int fd_readonly_config_set(lispval ignored,lispval v,void *vptr)
{
  if (SYMBOLP(v))
    return fd_reterr(fd_ReadOnlyConfig,"fd_set_config",
                     SYM_NAME(v),VOID);
  else if (STRINGP(v))
    return fd_reterr(fd_ReadOnlyConfig,"fd_set_config",
                     CSTRING(v),VOID);
  else return fd_reterr(fd_ReadOnlyConfig,"fd_set_config",NULL,VOID);
}

/* For configuration variables which are just integer constants */
FD_EXPORT lispval fd_constconfig_get(lispval ignored,void *llval)
{
  long long int_val = (long long) llval;
  return FD_INT(int_val);
}

/* For configuration variables which get/set dtype value. */
FD_EXPORT lispval fd_lconfig_get(lispval ignored,void *lispp)
{
  lispval *val = (lispval *)lispp;
  return fd_incref(*val);
}
FD_EXPORT int fd_lconfig_set(lispval ignored,lispval v,void *lispp)
{
  lispval *val = (lispval *)lispp, cur = *val;
  fd_decref(cur); fd_incref(v);
  *val = v;
  return 1;
}
FD_EXPORT int fd_symconfig_set(lispval ignored,lispval v,void *lispp)
{
  lispval *val = (lispval *)lispp;
  lispval sym = VOID;
  if (FD_SYMBOLP(v))
    sym = v;
  else if (FD_STRINGP(v)) {
    u8_string upper = u8_upcase(CSTRING(v));
    sym = fd_intern(upper);
    u8_free(upper);}
  else {}
  if (FD_VOIDP(sym)) {
    fd_type_error("string or symbol","fd_symconfig_set",v);
    return -1;}
  else {
    *val = sym;
    return 1;}
}
FD_EXPORT int fd_lconfig_add(lispval ignored,lispval v,void *lispp)
{
  lispval *val = (lispval *)lispp;
  CHOICE_ADD(*val,v);
  return 1;
}
FD_EXPORT int fd_lconfig_push(lispval ignored,lispval v,void *lispp)
{
  lispval *val = (lispval *)lispp;
  *val = fd_conspair(fd_incref(v),*val);
  return 1;
}

/* For configuration variables which get/set strings. */
FD_EXPORT lispval fd_sconfig_get(lispval ignored,void *vptr)
{
  u8_string *ptr = vptr;
  if (*ptr) return lispval_string(*ptr);
  else return EMPTY;
}
FD_EXPORT int fd_sconfig_set(lispval ignored,lispval v,void *vptr)
{
  u8_string *ptr = vptr;
  if (STRINGP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr = u8_strdup(CSTRING(v));
    return 1;}
  else return fd_reterr(fd_TypeError,"fd_sconfig_set",u8_strdup(_("string")),v);
}
FD_EXPORT int fd_realpath_config_set(lispval confvar,lispval v,void *vptr)
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
  else return fd_reterr(fd_TypeError,"fd_sconfig_set",u8_strdup(_("string")),v);
}
FD_EXPORT int fd_realdir_config_set(lispval confvar,lispval v,void *vptr)
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
  else return fd_reterr(fd_TypeError,"fd_sconfig_set",u8_strdup(_("string")),v);
}


/* For configuration variables which get/set ints. */
FD_EXPORT lispval fd_intconfig_get(lispval ignored,void *vptr)
{
  int *ptr = vptr;
  return FD_INT(*ptr);
}
FD_EXPORT int fd_intconfig_set(lispval ignored,lispval v,void *vptr)
{
  int *ptr = vptr;
  if (FD_INTP(v)) {
    *ptr = FIX2INT(v);
    return 1;}
  return fd_reterr(fd_TypeError,"fd_intconfig_set",
                   u8_strdup(_("small fixnum")),v);
}

/* For configuration variables which get/set ints. */
FD_EXPORT lispval fd_longconfig_get(lispval ignored,void *vptr)
{
  long long *ptr = vptr;
  return FD_INT(*ptr);
}
FD_EXPORT int fd_longconfig_set(lispval ignored,lispval v,void *vptr)
{
  long long *ptr = vptr;
  if (FIXNUMP(v)) {
    *ptr = FIX2INT(v);
    return 1;}
  else return fd_reterr(fd_TypeError,"fd_longconfig_set",
                        u8_strdup(_("fixnum")),v);
}

/* For configuration variables which get/set ints. */
FD_EXPORT lispval fd_sizeconfig_get(lispval ignored,void *vptr)
{
  ssize_t *ptr = vptr;
  ssize_t sz = *ptr;
  return FD_INT(sz);
}
FD_EXPORT int fd_sizeconfig_set(lispval ignored,lispval v,void *vptr)
{
  ssize_t *ptr = vptr;
  if (FIXNUMP(v)) {
    if ((*ptr) == (FIX2INT(v))) return 0;
    *ptr = FIX2INT(v);
    return 1;}
  else if (FD_BIGINTP(v)) {
    struct FD_BIGINT *bi = (fd_bigint)v;
    if (fd_bigint_fits_in_word_p(bi,8,1)) {
      long long ullv = fd_bigint_to_long_long(bi);
      if ((*ptr) == ((ssize_t)ullv)) return 0;
      *ptr = (ssize_t)ullv;
      return 1;}
    else return fd_reterr
           (fd_RangeError,"fd_sizeconfig_set",
            u8_strdup(_("size_t sized value")),v);}
  else return fd_reterr
         (fd_TypeError,"fd_sizeconfig_set",
          u8_strdup(_("size_t sized value")),v);
}

/* Double config methods */
FD_EXPORT lispval fd_dblconfig_get(lispval ignored,void *vptr)
{
  double *ptr = vptr;
  if (*ptr) return fd_init_double(NULL,*ptr);
  else return FD_FALSE;
}
FD_EXPORT int fd_dblconfig_set(lispval var,lispval v,void *vptr)
{
  double *ptr = vptr;
  if (FALSEP(v)) {
    *ptr = 0.0; return 1;}
  else if (FD_FLONUMP(v)) {
    *ptr = FD_FLONUM(v);}
  else if (FIXNUMP(v)) {
    long long intval = FIX2INT(v);
    double dblval = (double)intval;
    *ptr = dblval;}
  else return fd_reterr(fd_TypeError,"fd_dblconfig_set",
                        SYM_NAME(var),v);
  return 1;
}

/* Boolean configuration handlers */

static int false_stringp(u8_string string);
static int true_stringp(u8_string string);

FD_EXPORT lispval fd_boolconfig_get(lispval ignored,void *vptr)
{
  int *ptr = vptr;
  if (*ptr) return FD_TRUE; else return FD_FALSE;
}
FD_EXPORT int fd_boolconfig_set(lispval var,lispval v,void *vptr)
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
  else if (STRINGP(v)) {
    fd_seterr(fd_TypeError,"fd_boolconfig_set",FD_XSYMBOL_NAME(var),v);
    return -1;}
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

FD_EXPORT int fd_boolstring(u8_string string,int dflt)
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

static lispval fdversion_config_get(lispval var,void *data)
{
  return lispval_string(FD_VERSION);
}
static lispval fdrevision_config_get(lispval var,void *data)
{
  return lispval_string(FRAMERD_REVISION);
}
static lispval fdbranch_config_get(lispval var,void *data)
{
#ifdef FD_BRANCH
  return lispval_string(FD_BRANCH);
#else
  return lispval_string("unknown_branch");
#endif
}
static lispval fdmajor_config_get(lispval var,void *data)
{
  return FD_INT(FD_MAJOR_VERSION);
}
static lispval u8version_config_get(lispval var,void *data)
{
  return lispval_string(u8_getversion());
}
static lispval u8revision_config_get(lispval var,void *data)
{
  return lispval_string(u8_getrevision());
}
static lispval u8major_config_get(lispval var,void *data)
{
  return FD_INT(u8_getmajorversion());
}

/* LOGLEVEL */

static int loglevelconfig_set(lispval var,lispval val,void *data)
{
  if (FD_INTP(val)) {
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
    else {
      fd_seterr(fd_TypeError,"loglevelconfig_set",FD_XSYMBOL_NAME(var),val);
      return -1;}}
  else {
    fd_seterr(fd_TypeError,"loglevelconfig_set",FD_XSYMBOL_NAME(var),val);
    return -1;}
}

FD_EXPORT int fd_loglevelconfig_set(lispval var,lispval val,void *data)
{
  return loglevelconfig_set(var,val,data);
}

static lispval cwd_config_get(lispval var,void *data)
{
  u8_string wd = u8_getcwd();
  if (wd) return fd_lispstring(wd);
  else return FD_ERROR;
}

static int cwd_config_set(lispval var,lispval val,void *data)
{
  if (FD_STRINGP(val)) {
    if (u8_setcwd(CSTRING(val))<0)
      return FD_ERROR;
    else return 1;}
  else return fd_type_error("string","cwd_config_set",val);
}


/* Talbe configs */

static void tblconfig_error(lispval var,lispval val)
{
  u8_string details =
    (FD_SYMBOLP(var)) ? (FD_SYMBOL_NAME(var)) :
    (FD_STRINGP(var)) ? (CSTRING(var)) :
    (U8STR("oddconfig"));
  fd_seterr("ConfigFailed","fd_tblconfig_set",
            u8_strdup(details),val);

}

FD_EXPORT int fd_tblconfig_set(lispval var,lispval config_val,void *data)
{
  lispval *lptr = (lispval *) data;
  lispval table = *lptr;
  int retval=0, err=0;
  FD_DO_CHOICES(entry,config_val) {
    if (FD_PAIRP(entry)) {
      lispval key = FD_CAR(entry), value;
      if (FD_PAIRP(FD_CDR(entry)))
        value=FD_CADR(entry);
      else value=FD_CDR(entry);
      int rv=fd_store(table,key,value);
      if (rv<0) {
        tblconfig_error(var,value);
        FD_STOP_DO_CHOICES;
        return -1;}
      else retval++;}
    else if (FD_TABLEP(entry)) {
      lispval keys=fd_getkeys(entry);
      FD_DO_CHOICES(key,keys) {
        lispval value = fd_get(entry,key,FD_VOID);
        if (FD_ABORTP(value)) {
          FD_STOP_DO_CHOICES;
          tblconfig_error(var,FD_VOID);
          err=1;
          break;}
        else if (FD_AGNOSTICP(value)) {}
        else {
          int rv=fd_store(table,key,value);
          if (rv<0) {
            FD_STOP_DO_CHOICES;
            tblconfig_error(var,value);
            break;}}
        fd_decref(value);}
      fd_decref(keys);
      if (err) {
        FD_STOP_DO_CHOICES;
        break;}}
    else {
      FD_STOP_DO_CHOICES;
      tblconfig_error(var,entry);
      err=1;
      break;}}
  if (err)
    return -1;
  else return retval;
}

FD_EXPORT lispval fd_tblconfig_get(lispval var,void *data)
{
  lispval *lptr = (lispval *) data;
  lispval table = *lptr;
  return fd_incref(table);
}

/* Initialization */

void fd_init_config_c()
{
  if (support_config_c_init_done)
    return;
  else support_config_c_init_done=1;

  u8_register_source_file(_FILEINFO);

  configuration_table = fd_make_hashtable(NULL,16);

  u8_init_mutex(&config_lookup_lock);
  u8_init_mutex(&config_register_lock);

  path_macro = fd_intern("#PATH");
  glom_macro = fd_intern("#GLOM");
  config_macro = fd_intern("#CONFIG");
  now_macro = fd_intern("#NOW");
  env_macro = fd_intern("#ENV");

  fd_register_config
    ("FDVERSION",_("Get the FramerD version string"),
     fdversion_config_get,fd_readonly_config_set,NULL);
  fd_register_config
    ("FDREVISION",_("Get the FramerD revision identifier"),
     fdrevision_config_get,fd_readonly_config_set,NULL);
  fd_register_config
    ("FDBRANCH",_("Get the FramerD branch name"),
     fdbranch_config_get,fd_readonly_config_set,NULL);
  fd_register_config
    ("FDMAJOR",_("Get the FramerD major version number"),
     fdmajor_config_get,fd_readonly_config_set,NULL);
  fd_register_config
    ("U8VERSION",_("Get the libu8 version string"),
     u8version_config_get,fd_readonly_config_set,NULL);
  fd_register_config
    ("U8REVISION",_("Get the libu8 revision identifier"),
     u8revision_config_get,fd_readonly_config_set,NULL);
  fd_register_config
    ("U8MAJOR",_("Get the libu8 major version number"),
     u8major_config_get,fd_readonly_config_set,NULL);

#if FD_FILECONFIG_ENABLED
  fd_register_config
    ("CONFIGDATA",_("Directory for looking up config entries"),
     fd_sconfig_get,fd_sconfig_set,&configdata_path);
  fd_register_config_lookup(file_config_lookup,NULL);
#endif

  fd_register_config_lookup(getenv_config_lookup,NULL);

  fd_register_config
    ("CWD",_("Get/set the current working directory"),
     cwd_config_get,cwd_config_set,NULL);

  fd_register_config("FIXMAX","The maximum fixnum value",
                     fd_lconfig_get,fd_readonly_config_set,
                     &fd_max_fixnum);
  fd_register_config("MAXFIX","The maximum fixnum value",
                     fd_lconfig_get,fd_readonly_config_set,
                     &fd_max_fixnum);
  fd_register_config("FIXMIN","The minimum fixnum value",
                     fd_lconfig_get,fd_readonly_config_set,
                     &fd_min_fixnum);
  fd_register_config("MINFIX","The minimum fixnum value",
                     fd_lconfig_get,fd_readonly_config_set,
                     &fd_min_fixnum);

  fd_register_config("UINT_MAX",
                     "Maximum value for an underlying unsigned INT",
                     fd_constconfig_get,fd_readonly_config_set,
                     (void *) UINT_MAX);
  fd_register_config("INT_MAX",
                     "Maximum value for an underlying unsigned INT",
                     fd_constconfig_get,fd_readonly_config_set,
                     (void *) INT_MAX);
  fd_register_config("INT_MIN",
                     "Maximum value for an underlying unsigned INT",
                     fd_constconfig_get,fd_readonly_config_set,
                     (void *) INT_MIN);

  fd_register_config
    ("TRACECONFIG",_("whether to trace configuration"),
     fd_boolconfig_get,fd_boolconfig_set,&fd_trace_config);

}

/* Emacs local variables
   ;;;  Local variables: ***
   ;;;  compile-command: "make -C ../.. debugging;" ***
   ;;;  indent-tabs-mode: nil ***
   ;;;  End: ***
*/
