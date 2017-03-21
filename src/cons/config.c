/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* Copyright (C) 2004-2017 beingmeta, inc.
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

fd_exception fd_ConfigError=_("Configuration error");
fd_exception fd_ReadOnlyConfig=_("Read-only config setting");

static int trace_config=0;

/* Configuration handling */

struct FD_CONFIG_HANDLER *config_handlers=NULL;

static fdtype configuration_table;

static u8_mutex config_lookup_lock;
static u8_mutex config_register_lock;

static struct FD_CONFIG_FINDER *config_lookupfns=NULL;

static fdtype config_intern(u8_string start)
{
  U8_OUTPUT nameout; u8_byte buf[64];
  const u8_byte *scan=start;
  U8_INIT_STATIC_OUTPUT_BUF(nameout,64,buf);
  while (*scan) {
    int c=u8_sgetc(&scan);
    if ((c == '/')||(c==':')) u8_putc(&nameout,c);
    else if (u8_ispunct(c)) {}
    else if (u8_isupper(c)) u8_putc(&nameout,c);
    else u8_putc(&nameout,u8_toupper(c));}
  if (nameout.u8_streaminfo&U8_STREAM_OWNS_BUF) {
    fdtype symbol=
      fd_make_symbol(nameout.u8_outbuf,nameout.u8_write-nameout.u8_outbuf);
    u8_close((u8_stream)&nameout);
    return symbol;}
  else return fd_make_symbol
         (nameout.u8_outbuf,nameout.u8_write-nameout.u8_outbuf);
}

FD_EXPORT
void fd_register_config_lookup(fdtype (*fn)(fdtype,void *),void *ldata)
{
  struct FD_CONFIG_FINDER *entry=
    u8_alloc(struct FD_CONFIG_FINDER);
  u8_lock_mutex(&config_lookup_lock);
  entry->fdcfg_lookup=fn;
  entry->fdcfg_lookup_data=ldata;
  entry->fd_next_finder=config_lookupfns;
  config_lookupfns=entry;
  u8_unlock_mutex(&config_lookup_lock);
}

static fdtype config_get(u8_string var)
{
  fdtype symbol=config_intern(var);
  fdtype probe=fd_get(configuration_table,symbol,FD_VOID);
  /* This lookups configuration information using various methods */
  if (FD_VOIDP(probe)) {
    fdtype value=FD_VOID;
    struct FD_CONFIG_FINDER *scan=config_lookupfns;
    while (scan) {
      value=scan->fdcfg_lookup(symbol,scan->fdcfg_lookup_data);
      if (FD_VOIDP(value)) scan=scan->fd_next_finder; else break;}
    fd_store(configuration_table,symbol,value);
    return value;}
  else return probe;
}

static fdtype getenv_config_lookup(fdtype symbol,void *ignored)
{
  U8_OUTPUT out;
  char *getenv_result;
  u8_string u8result;
  fdtype result;
  U8_INIT_OUTPUT(&out,32);
  u8_printf(&out,"FD_%s",FD_SYMBOL_NAME(symbol));
  getenv_result=getenv(out.u8_outbuf);
  if (getenv_result==NULL) {
    u8_free(out.u8_outbuf); return FD_VOID;}
  u8result=u8_fromlibc(getenv_result);
  result=fd_parse_arg(u8result);
  u8_free(out.u8_outbuf); u8_free(u8result);
  return result;
}

int set_config(u8_string var,fdtype val)
{
  fdtype symbol=config_intern(var);
  fdtype current=fd_get(configuration_table,symbol,FD_VOID);
  if (FD_VOIDP(current)) {
    if (FD_PAIRP(val)) {
      fdtype pairpair=fd_make_pair(val,FD_EMPTY_LIST);
      int rv=fd_store(configuration_table,symbol,pairpair);
      fd_decref(pairpair);
      return rv;}
    else return fd_store(configuration_table,symbol,val);}
  else if (FD_PAIRP(current)) {
    fdtype pairpair=fd_make_pair(val,current);
    int rv=fd_store(configuration_table,symbol,pairpair);
    fd_decref(pairpair);
    return rv;}
  else if (FD_EQUAL(current,val)) return 0;
  else {
    fdtype cdr=fd_make_pair(current,FD_EMPTY_LIST);
    fdtype pairpair=fd_make_pair(val,cdr);
    int rv=fd_store(configuration_table,symbol,pairpair);
    fd_decref(cdr); fd_decref(pairpair);
    return rv;}
}

/* File-based configuration */

#if FD_FILECONFIG_ENABLED
static u8_string configdata_path=NULL;

static fdtype file_config_lookup(fdtype symbol,void *pathdata)
{
  u8_string path=
    ((pathdata==NULL) ?
     ((configdata_path) ? (configdata_path) : ((u8_string)FD_CONFIG_FILE_PATH)) :
     ((u8_string)pathdata));
  u8_string filename=u8_find_file(FD_SYMBOL_NAME(symbol),path,NULL);
  if (filename) {
    int n_bytes; fdtype result;
    unsigned char *content=u8_filedata(filename,&n_bytes);
    if (content[0]==0) {
      struct FD_INBUF in;
      in.buffer=in.bufread=content+1; in.buflim=in.buffer+n_bytes;
      in.buf_fillfn=NULL;
      result=fd_read_dtype(&in);}
    else {
      /* Zap any trailing newlines */
      if ((n_bytes>1) && (content[n_bytes-1]=='\n'))
        content[n_bytes-1]='\0';
      if ((n_bytes>2) && (content[n_bytes-2]=='\r'))
        content[n_bytes-2]='\0';
      result=fd_parse_arg(content);}
    u8_free(filename);
    u8_free(content);
    return result;}
  else return FD_VOID;
}

#endif

/* API functions */

FD_EXPORT fdtype fd_config_get(u8_string var)
{
  fdtype symbol=config_intern(var);
  struct FD_CONFIG_HANDLER *scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->fd_configname,symbol)) {
      fdtype val;
      val=scan->fd_config_get_method(symbol,scan->fd_configdata);
      return val;}
    else scan=scan->fd_nextconfig;
  return config_get(var);
}

FD_EXPORT int fd_set_config(u8_string var,fdtype val)
{
  fdtype symbol=config_intern(var); int retval=0;
  struct FD_CONFIG_HANDLER *scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->fd_configname,symbol)) {
      scan->fd_configflags=scan->fd_configflags|FD_CONFIG_ALREADY_MODIFIED;
      retval=scan->fd_config_set_method(symbol,val,scan->fd_configdata);
      if (trace_config)
        u8_log(LOG_WARN,"ConfigSet",
               "Using handler to configure %s (%s) with %q",
               var,FD_SYMBOL_NAME(symbol),val);
      break;}
    else scan=scan->fd_nextconfig;
  if ((!(scan))&&(trace_config))
    u8_log(LOG_WARN,"ConfigSet","Configuring %s (%s) with %q",
           var,FD_SYMBOL_NAME(symbol),val);
  set_config(var,val);
  if (retval<0) {
    u8_string errsum=fd_errstring(NULL);
    u8_log(LOG_WARN,fd_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
    u8_free(errsum);}
  return retval;
}

FD_EXPORT int fd_default_config(u8_string var,fdtype val)
{
  fdtype symbol=config_intern(var); int retval=1;
  struct FD_CONFIG_HANDLER *scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->fd_configname,symbol)) {
      if ((scan->fd_configflags)&(FD_CONFIG_ALREADY_MODIFIED)) return 0;
      scan->fd_configflags=scan->fd_configflags|FD_CONFIG_ALREADY_MODIFIED;
      retval=scan->fd_config_set_method(symbol,val,scan->fd_configdata);
      if (trace_config)
        u8_log(LOG_WARN,"ConfigSet",
               "Using handler to configure default %s (%s) with %q",
               var,FD_SYMBOL_NAME(symbol),val);
      break;}
    else scan=scan->fd_nextconfig;
  if (fd_test(configuration_table,symbol,FD_VOID)) return 0;
  else {
    if ((!(scan))&&(trace_config))
      u8_log(LOG_WARN,"ConfigSet","Configuring %s (%s) with %q",
             var,FD_SYMBOL_NAME(symbol),val);
    retval=set_config(var,val);}
  if (retval<0) {
    u8_string errsum=fd_errstring(NULL);
    u8_log(LOG_WARN,fd_ConfigError,"Config error %q=%q: %s",symbol,val,errsum);
    u8_free(errsum);}
  return retval;
}

FD_EXPORT int fd_set_config_consed(u8_string var,fdtype val)
{
  int retval=fd_set_config(var,val);
  if (retval<0) return retval;
  fd_decref(val);
  return retval;
}

/* Registering new configuration values */

FD_EXPORT int fd_register_config_x
  (u8_string var,u8_string doc,
   fdtype (*getfn)(fdtype,void *),
   int (*setfn)(fdtype,fdtype,void *),
   void *data,int (*reuse)(struct FD_CONFIG_HANDLER *scan))
{
  fdtype symbol=config_intern(var), current=config_get(var);
  int retval=0;
  struct FD_CONFIG_HANDLER *scan;
  u8_lock_mutex(&config_register_lock);
  scan=config_handlers;
  while (scan)
    if (FD_EQ(scan->fd_configname,symbol)) {
      if (reuse) reuse(scan);
      if (doc) {
        /* We don't override a real doc with a NULL doc.
           Possibly not the right thing. */
        if (scan->fd_configdoc) u8_free(scan->fd_configdoc);
        scan->fd_configdoc=u8_strdup(doc);}
      scan->fd_config_get_method=getfn;
      scan->fd_config_set_method=setfn;
      scan->fd_configdata=data;
      break;}
    else scan=scan->fd_nextconfig;
  if (scan==NULL) {
    current=config_get(var);
    scan=u8_alloc(struct FD_CONFIG_HANDLER);
    scan->fd_configname=symbol;
    if (doc) scan->fd_configdoc=u8_strdup(doc); else scan->fd_configdoc=NULL;
    scan->fd_configflags=0;
    scan->fd_config_get_method=getfn;
    scan->fd_config_set_method=setfn;
    scan->fd_configdata=data;
    scan->fd_nextconfig=config_handlers;
    config_handlers=scan;}
  u8_unlock_mutex(&config_register_lock);
  if (FD_ABORTP(current)) {
    fd_clear_errors(1);
    retval=-1;}
  else if (FD_VOIDP(current)) {}
  else if (FD_PAIRP(current)) {
    /* There have been multiple configuration specifications,
       so run them all backwards. */
    int n=0; fdtype *vals, *write;
    {fdtype scan=current; while (FD_PAIRP(scan)) {scan=FD_CDR(scan); n++;}}
    vals=u8_alloc_n(n,fdtype); write=vals;
    {FD_DOLIST(cv,current) *write++=cv;}
    while (n>0) {
      n=n-1;
      if (retval<0) {
        u8_free(vals); fd_decref(current);
        return retval;}
      else retval=setfn(symbol,vals[n],data);}
    fd_decref(current); u8_free(vals);
    return retval;}
  else retval=setfn(symbol,current,data);
  fd_decref(current);
  return retval;
}

FD_EXPORT int fd_register_config
  (u8_string var,u8_string doc,
   fdtype (*getfn)(fdtype,void *),
   int (*setfn)(fdtype,fdtype,void *),
   void *data)
{
  return fd_register_config_x(var,doc,getfn,setfn,data,NULL);
}

FD_EXPORT fdtype fd_all_configs(int with_docs)
{
  fdtype results=FD_EMPTY_CHOICE;
  struct FD_CONFIG_HANDLER *scan;
  u8_lock_mutex(&config_register_lock); {
    scan=config_handlers;
    while (scan) {
      fdtype var=scan->fd_configname;
      if (with_docs) {
        fdtype doc=((scan->fd_configdoc)?
                    (fdstring(scan->fd_configdoc)):
                    (FD_EMPTY_LIST));
        fdtype pair=fd_conspair(var,doc); fd_incref(var);
        FD_ADD_TO_CHOICE(results,pair);}
      else {fd_incref(var); FD_ADD_TO_CHOICE(results,var);}
      scan=scan->fd_nextconfig;}}
  u8_unlock_mutex(&config_register_lock);
  return results;
}

/* Lots of different ways to specify configuration */

/* This takes a string of the form var=value */
FD_EXPORT int fd_config_assignment(u8_string assignment)
{
  u8_byte *equals;
  if ((equals=(strchr(assignment,'=')))) {
    u8_byte _namebuf[64], *namebuf;
    int namelen=equals-assignment, retval;
    fdtype value=fd_parse_arg(equals+1);
    if (FD_ABORTP(value))
      return fd_interr(value);
    if (namelen+1>64)
      namebuf=u8_malloc(namelen+1);
    else namebuf=_namebuf;
    strncpy(namebuf,assignment,namelen); namebuf[namelen]='\0';
    retval=fd_set_config_consed(namebuf,value);
    if (namebuf!=_namebuf) u8_free(namebuf);
    return retval;}
  else return -1;
}

/* This takes a string of the form var=value */
FD_EXPORT int fd_default_config_assignment(u8_string assignment)
{
  u8_byte *equals;
  if ((equals=(strchr(assignment,'=')))) {
    u8_byte _namebuf[64], *namebuf;
    int namelen=equals-assignment, retval=0;
    fdtype value=fd_parse_arg(equals+1);
    if (FD_ABORTP(value))
      return fd_interr(value);
    if (namelen+1>64)
      namebuf=u8_malloc(namelen+1);
    else namebuf=_namebuf;
    strncpy(namebuf,assignment,namelen); namebuf[namelen]='\0';
    if (!(fd_test(configuration_table,config_intern(namebuf),FD_VOID)))
      retval=fd_set_config_consed(namebuf,value);
    if (namebuf!=_namebuf) u8_free(namebuf);
    return retval;}
  else return -1;
}

/* This reads a config file.  It consists of a series of entries, each of which is
   either a list (var value) or an assignment var=value.
   Both # and ; are comment characters */
FD_EXPORT int fd_read_config(U8_INPUT *in)
{
  int c,n =0; u8_string buf;
  while ((c=u8_getc(in))>=0)
    if (c == '#') {
      buf=u8_gets(in); u8_free(buf);}
    else if (c == ';') {
      buf=u8_gets(in); u8_free(buf);}
    else if (c == '(') {
      fdtype entry;
      u8_ungetc(in,c);
      entry=fd_parser(in);
      if (FD_ABORTP(entry))
        return fd_interr(entry);
      else if ((FD_PAIRP(entry)) &&
               (FD_SYMBOLP(FD_CAR(entry))) &&
               (FD_PAIRP(FD_CDR(entry)))) {
        if (fd_set_config(FD_SYMBOL_NAME(FD_CAR(entry)),(FD_CADR(entry)))<0) {
          fd_seterr(fd_ConfigError,"fd_read_config",NULL,entry);
          return -1;}
        fd_decref(entry);
        n++;}
      else {
        fd_seterr(fd_ConfigError,"fd_read_config",NULL,entry);
        return -1;}}
    else if ((u8_isspace(c)) || (u8_isctrl(c))) {}
    else {
      u8_ungetc(in,c);
      buf=u8_gets(in);
      if (fd_config_assignment(buf)<0)
        return fd_reterr(fd_ConfigError,"fd_read_config",buf,FD_VOID);
      else n++;
      u8_free(buf);}
  return n;
}

/* This reads a config file.  It consists of a series of entries, each of which is
   either a list (var value) or an assignment var=value.
   Both # and ; are comment characters */
FD_EXPORT int fd_read_default_config(U8_INPUT *in)
{
  int c, n=0, count=0; u8_string buf;
  while ((c=u8_getc(in))>=0)
    if (c == '#') {
      buf=u8_gets(in); u8_free(buf);}
    else if (c == ';') {
      buf=u8_gets(in); u8_free(buf);}
    else if (c == '(') {
      fdtype entry;
      u8_ungetc(in,c);
      entry=fd_parser(in);
      if (FD_ABORTP(entry))
        return fd_interr(entry);
      else if ((FD_PAIRP(entry)) &&
               (FD_SYMBOLP(FD_CAR(entry))) &&
               (FD_PAIRP(FD_CDR(entry)))) {
        if (fd_default_config(FD_SYMBOL_NAME(FD_CAR(entry)),(FD_CADR(entry)))<0) {
          fd_seterr(fd_ConfigError,"fd_read_config",NULL,entry);
          return -1;}
        else {n++; count++;}
        fd_decref(entry);}
      else {
        fd_seterr(fd_ConfigError,"fd_read_config",NULL,entry);
        return -1;}}
    else if ((u8_isspace(c)) || (u8_isctrl(c))) {}
    else {
      u8_ungetc(in,c);
      buf=u8_gets(in);
      if (fd_default_config_assignment(buf)<0)
        return fd_reterr(fd_ConfigError,"fd_read_config",buf,FD_VOID);
      else {count++; n++;}
      u8_free(buf);}
  return count;
}

/* Utility configuration functions */

/* This set method just returns an error */
FD_EXPORT int fd_readonly_config_set(fdtype ignored,fdtype v,void *vptr)
{
  if (FD_SYMBOLP(v))
    return fd_reterr(fd_ReadOnlyConfig,"fd_set_config",
                     FD_SYMBOL_NAME(v),FD_VOID);
  else if (FD_STRINGP(v))
    return fd_reterr(fd_ReadOnlyConfig,"fd_set_config",
                     FD_STRDATA(v),FD_VOID);
  else return fd_reterr(fd_ReadOnlyConfig,"fd_set_config",NULL,FD_VOID);
}

/* For configuration variables which get/set dtype value. */
FD_EXPORT fdtype fd_lconfig_get(fdtype ignored,void *lispp)
{
  fdtype *val=(fdtype *)lispp;
  return fd_incref(*val);
}
FD_EXPORT int fd_lconfig_set(fdtype ignored,fdtype v,void *lispp)
{
  fdtype *val=(fdtype *)lispp, cur=*val;
  fd_decref(cur); fd_incref(v);
  *val=v;
  return 1;
}
FD_EXPORT int fd_lconfig_add(fdtype ignored,fdtype v,void *lispp)
{
  fdtype *val=(fdtype *)lispp;
  FD_ADD_TO_CHOICE(*val,v);
  return 1;
}
FD_EXPORT int fd_lconfig_push(fdtype ignored,fdtype v,void *lispp)
{
  fdtype *val=(fdtype *)lispp;
  *val=fd_conspair(fd_incref(v),*val);
  return 1;
}

/* For configuration variables which get/set strings. */
FD_EXPORT fdtype fd_sconfig_get(fdtype ignored,void *vptr)
{
  u8_string *ptr=vptr;
  if (*ptr) return fdtype_string(*ptr);
  else return FD_EMPTY_CHOICE;
}
FD_EXPORT int fd_sconfig_set(fdtype ignored,fdtype v,void *vptr)
{
  u8_string *ptr=vptr;
  if (FD_STRINGP(v)) {
    if (*ptr) u8_free(*ptr);
    *ptr=u8_strdup(FD_STRDATA(v));
    return 1;}
  else return fd_reterr(fd_TypeError,"fd_sconfig_set",u8_strdup(_("string")),v);
}

/* For configuration variables which get/set ints. */
FD_EXPORT fdtype fd_intconfig_get(fdtype ignored,void *vptr)
{
  int *ptr=vptr;
  return FD_INT(*ptr);
}
FD_EXPORT int fd_intconfig_set(fdtype ignored,fdtype v,void *vptr)
{
  int *ptr=vptr;
  if (FD_INTP(v)) {
    *ptr=FD_FIX2INT(v);
    return 1;}
  return fd_reterr(fd_TypeError,"fd_intconfig_set",
		   u8_strdup(_("small fixnum")),v);
}

/* For configuration variables which get/set ints. */
FD_EXPORT fdtype fd_longconfig_get(fdtype ignored,void *vptr)
{
  long long *ptr=vptr;
  return FD_INT(*ptr);
}
FD_EXPORT int fd_longconfig_set(fdtype ignored,fdtype v,void *vptr)
{
  long long *ptr=vptr;
  if (FD_FIXNUMP(v)) {
    *ptr=FD_FIX2INT(v);
    return 1;}
  else return fd_reterr(fd_TypeError,"fd_longconfig_set",
			u8_strdup(_("fixnum")),v);
}

/* For configuration variables which get/set ints. */
FD_EXPORT fdtype fd_sizeconfig_get(fdtype ignored,void *vptr)
{
  ssize_t *ptr=vptr;
  ssize_t sz=*ptr;
  return FD_INT(sz);
}
FD_EXPORT int fd_sizeconfig_set(fdtype ignored,fdtype v,void *vptr)
{
  ssize_t *ptr=vptr;
  if (FD_FIXNUMP(v)) {
    if ((*ptr)==(FD_FIX2INT(v))) return 0;
    *ptr=FD_FIX2INT(v);
    return 1;}
  else if (FD_BIGINTP(v)) {
    struct FD_BIGINT *bi=(fd_bigint)v;
    if (fd_bigint_fits_in_word_p(bi,8,1)) {
      long long ullv=fd_bigint_to_long_long(bi);
      if ((*ptr)==((ssize_t)ullv)) return 0;
      *ptr=(ssize_t)ullv;
      return 1;}
    else return fd_reterr
           (fd_RangeError,"fd_sizeconfig_set",
            u8_strdup(_("size_t sized value")),v);}
  else return fd_reterr
         (fd_TypeError,"fd_sizeconfig_set",
          u8_strdup(_("size_t sized value")),v);
}

/* Double config methods */
FD_EXPORT fdtype fd_dblconfig_get(fdtype ignored,void *vptr)
{
  double *ptr=vptr;
  if (*ptr) return fd_init_double(NULL,*ptr);
  else return FD_FALSE;
}
FD_EXPORT int fd_dblconfig_set(fdtype var,fdtype v,void *vptr)
{
  double *ptr=vptr;
  if (FD_FALSEP(v)) {
    *ptr=0.0; return 1;}
  else if (FD_FLONUMP(v)) {
    *ptr=FD_FLONUM(v);}
  else if (FD_FIXNUMP(v)) {
    long long intval=FD_FIX2INT(v);
    double dblval=(double)intval;
    *ptr=dblval;}
  else return fd_reterr(fd_TypeError,"fd_dblconfig_set",
                        FD_SYMBOL_NAME(var),v);
  return 1;
}

/* Boolean configuration handlers */

static int false_stringp(u8_string string);
static int true_stringp(u8_string string);

FD_EXPORT fdtype fd_boolconfig_get(fdtype ignored,void *vptr)
{
  int *ptr=vptr;
  if (*ptr) return FD_TRUE; else return FD_FALSE;
}
FD_EXPORT int fd_boolconfig_set(fdtype var,fdtype v,void *vptr)
{
  int *ptr=vptr;
  if (FD_FALSEP(v)) {
    *ptr=0; return 1;}
  else if (FD_FIXNUMP(v)) {
    /* Strictly speaking, this isn't exactly right, but it's not uncommon
       to have int-valued config variables where 1 is a default setting
       but others are possible. */
    if (FD_FIX2INT(v)<=0)
      *ptr=0;
    else if (FD_FIX2INT(v)<INT_MAX)
      *ptr=FD_FIX2INT(v);
    else *ptr=INT_MAX;
    return 1;}
  else if ((FD_STRINGP(v)) && (false_stringp(FD_STRDATA(v)))) {
    *ptr=0; return 1;}
  else if ((FD_STRINGP(v)) && (true_stringp(FD_STRDATA(v)))) {
    *ptr=1; return 1;}
  else if (FD_STRINGP(v)) {
    fd_seterr(fd_TypeError,"fd_boolconfig_set",
              u8_strdup(FD_XSYMBOL_NAME(var)),fd_incref(v));
    return -1;}
  else {*ptr=1; return 1;}
}

/* Someday, these should be configurable. */
static u8_string false_strings[]={
  "no","false","off","n","f" "#f","#false","nope",
  "0","disable","non","nei","nein","not","never",NULL};

static int false_stringp(u8_string string)
{
  u8_string *scan=false_strings;
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
  u8_string *scan=true_strings;
  while (*scan)
    if (strcasecmp(string,*scan)==0) return 1;
    else scan++;
  return 0;
}

FD_EXPORT int fd_boolstring(u8_string string,int dflt)
{
  u8_string *scan=true_strings;
  while (*scan)
    if (strcasecmp(string,*scan)==0) return 1;
    else scan++;
  scan=false_strings;
  while (*scan)
    if (strcasecmp(string,*scan)==0) return 0;
    else scan++;
  return dflt;
}

/* Version info */

static fdtype fdversion_config_get(fdtype var,void *data)
{
  return fdtype_string(FD_VERSION);
}
static fdtype fdrevision_config_get(fdtype var,void *data)
{
  return fdtype_string(FRAMERD_REVISION);
}
static fdtype fdmajor_config_get(fdtype var,void *data)
{
  return FD_INT(FD_MAJOR_VERSION);
}
static fdtype u8version_config_get(fdtype var,void *data)
{
  return fdtype_string(u8_getversion());
}
static fdtype u8revision_config_get(fdtype var,void *data)
{
  return fdtype_string(u8_getrevision());
}
static fdtype u8major_config_get(fdtype var,void *data)
{
  return FD_INT(u8_getmajorversion());
}

/* LOGLEVEL */

static int loglevelconfig_set(fdtype var,fdtype val,void *data)
{
  if (FD_INTP(val)) {
    int *valp=(int *)data;
    *valp=FD_FIX2INT(val);
    return 1;}
  else if ((FD_STRINGP(val)) || (FD_SYMBOLP(val))) {
    u8_string *scan=u8_loglevels; int loglevel=-1;
    u8_string level_name;
    if (FD_STRINGP(val)) level_name=FD_STRDATA(val);
    else level_name=FD_SYMBOL_NAME(val);
    while (*scan)
      if (strcasecmp(*scan,level_name)==0) {
        loglevel=scan-u8_loglevels; break;}
      else scan++;
    if (loglevel>=0) {
      int *valp=(int *)data; *valp=loglevel;
      return 1;}
    else {
      fd_seterr(fd_TypeError,"config_setloglevel",
                u8_strdup(FD_XSYMBOL_NAME(var)),
                fd_incref(val));
      return -1;}}
  else {
    fd_seterr(fd_TypeError,"config_setloglevel",
              u8_strdup(FD_XSYMBOL_NAME(var)),
              fd_incref(val));
    return -1;}
}

FD_EXPORT int fd_loglevelconfig_set(fdtype var,fdtype val,void *data)
{
  return loglevelconfig_set(var,val,data);
}

void fd_init_config_c()
{
  u8_register_source_file(_FILEINFO);

  configuration_table=fd_make_hashtable(NULL,16);

  u8_init_mutex(&config_lookup_lock);
  u8_init_mutex(&config_register_lock);

  fd_register_config_lookup(getenv_config_lookup,NULL);
 
  fd_register_config
    ("FDVERSION",_("Get the FramerD version string"),
     fdversion_config_get,fd_readonly_config_set,NULL);
  fd_register_config
    ("FDREVISION",_("Get the FramerD revision identifier"),
     fdrevision_config_get,fd_readonly_config_set,NULL);
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

  fd_register_config
    ("TRACECONFIG",_("whether to trace configuration"),
     fd_boolconfig_get,fd_boolconfig_set,&trace_config);

}
